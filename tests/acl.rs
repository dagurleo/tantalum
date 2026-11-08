//! ACLs: users, their rules, and what they may run, over several connections.
//! The scenarios and Redis 7.2.4's replies are recorded by `tools/acl-diff.py
//! --rust`, which compares them with Tantalum's (and more: replies that vary,
//! like `ACL LOG`'s times, it compares in part).

mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use common::{MODES, Mode, read_reply, req, start};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

enum Step {
    /// On connection N, this command, and Redis' reply (`<closed>` for none).
    Run(usize, &'static [&'static [u8]], &'static [u8]),
    /// On connection N, a command whose reply varies (Redis' is compared in
    /// part by the tool): run, not checked.
    Send(usize, &'static [&'static [u8]]),
    /// Users, the log and the data back to the start, connections closed.
    Reset,
    Sleep(u64),
}

#[rustfmt::skip]
const STEPS: &[Step] = &[
// BEGIN STEPS
    // Who's who.
    Step::Run(0, &[b"ACL", b"WHOAMI"], b"$7\r\ndefault\r\n"),
    Step::Run(0, &[b"ACL", b"USERS"], b"*1\r\n$7\r\ndefault\r\n"),
    Step::Run(0, &[b"ACL", b"LIST"], b"*1\r\n$51\r\nuser default on nopass sanitize-payload ~* &* +@all\r\n"),
    Step::Send(0, &[b"ACL", b"CAT"]),
    Step::Send(0, &[b"ACL", b"CAT", b"read"]),
    Step::Send(0, &[b"ACL", b"CAT", b"dangerous"]),
    Step::Send(0, &[b"ACL", b"CAT", b"scripting"]),
    Step::Run(0, &[b"ACL", b"CAT", b"NON_EXISTS"], b"-ERR Unknown category 'NON_EXISTS'\r\n"),
    Step::Run(0, &[b"ACL", b"CAT", b"a", b"b"], b"-ERR unknown subcommand or wrong number of arguments for 'CAT'. Try ACL HELP.\r\n"),
    Step::Run(0, &[b"ACL", b"HELP"], b"*29\r\n+ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:\r\n+CAT [<category>]\r\n+    List all commands that belong to <category>, or all command categories\r\n+    when no category is specified.\r\n+DELUSER <username> [<username> ...]\r\n+    Delete a list of users.\r\n+DRYRUN <username> <command> [<arg> ...]\r\n+    Returns whether the user can execute the given command without executing the command.\r\n+GETUSER <username>\r\n+    Get the user's details.\r\n+GENPASS [<bits>]\r\n+    Generate a secure 256-bit user password. The optional `bits` argument can\r\n+    be used to specify a different size.\r\n+LIST\r\n+    Show users details in config file format.\r\n+LOAD\r\n+    Reload users from the ACL file.\r\n+LOG [<count> | RESET]\r\n+    Show the ACL log entries.\r\n+SAVE\r\n+    Save the current config to the ACL file.\r\n+SETUSER <username> <attribute> [<attribute> ...]\r\n+    Create or modify a user with the specified attributes.\r\n+USERS\r\n+    List all the registered usernames.\r\n+WHOAMI\r\n+    Return the current connection username.\r\n+HELP\r\n+    Print this help.\r\n"),
    Step::Run(0, &[b"ACL", b"NOPE"], b"-ERR unknown subcommand 'NOPE'. Try ACL HELP.\r\n"),
    Step::Run(0, &[b"ACL", b"GETUSER"], b"-ERR wrong number of arguments for 'acl|getuser' command\r\n"),
    Step::Run(0, &[b"ACL", b"WHOAMI", b"extra"], b"-ERR wrong number of arguments for 'acl|whoami' command\r\n"),
    Step::Run(0, &[b"ACL", b"GETUSER", b"default"], b"*12\r\n$5\r\nflags\r\n*3\r\n$2\r\non\r\n$6\r\nnopass\r\n$16\r\nsanitize-payload\r\n$9\r\npasswords\r\n*0\r\n$8\r\ncommands\r\n$5\r\n+@all\r\n$4\r\nkeys\r\n$2\r\n~*\r\n$8\r\nchannels\r\n$2\r\n&*\r\n$9\r\nselectors\r\n*0\r\n"),
    Step::Run(0, &[b"ACL", b"GETUSER", b"nobody"], b"$-1\r\n"),
    Step::Send(1, &[b"HELLO", b"3"]),
    Step::Run(1, &[b"ACL", b"GETUSER", b"default"], b"%6\r\n$5\r\nflags\r\n~3\r\n$2\r\non\r\n$6\r\nnopass\r\n$16\r\nsanitize-payload\r\n$9\r\npasswords\r\n*0\r\n$8\r\ncommands\r\n$5\r\n+@all\r\n$4\r\nkeys\r\n$2\r\n~*\r\n$8\r\nchannels\r\n$2\r\n&*\r\n$9\r\nselectors\r\n*0\r\n"),
    Step::Run(1, &[b"ACL", b"GETUSER", b"nobody"], b"_\r\n"),
    // Users and their rules.
    Step::Run(0, &[b"ACL", b"SETUSER", b"alice"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"LIST"], b"*2\r\n$51\r\nuser alice off sanitize-payload resetchannels -@all\r\n$51\r\nuser default on nopass sanitize-payload ~* &* +@all\r\n"),
    Step::Run(0, &[b"ACL", b"GETUSER", b"alice"], b"*12\r\n$5\r\nflags\r\n*2\r\n$3\r\noff\r\n$16\r\nsanitize-payload\r\n$9\r\npasswords\r\n*0\r\n$8\r\ncommands\r\n$5\r\n-@all\r\n$4\r\nkeys\r\n$0\r\n\r\n$8\r\nchannels\r\n$0\r\n\r\n$9\r\nselectors\r\n*0\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"alice", b"on", b">p1", b">p2", b"~cached:*", b"%R~read:*", b"%W~write:*", b"&news.*", b"+get", b"+set", b"-@all", b"+@hash"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"GETUSER", b"alice"], b"*12\r\n$5\r\nflags\r\n*2\r\n$2\r\non\r\n$16\r\nsanitize-payload\r\n$9\r\npasswords\r\n*2\r\n$64\r\nf64551fcd6f07823cb87971cfb91446425da18286b3ab1ef935e0cbd7a69f68a\r\n$64\r\n3946ca64ff78d93ca61090a437cbb6b3d2ca0d488f5f9ccf3059608368b27693\r\n$8\r\ncommands\r\n$12\r\n-@all +@hash\r\n$4\r\nkeys\r\n$30\r\n~cached:* %R~read:* %W~write:*\r\n$8\r\nchannels\r\n$7\r\n&news.*\r\n$9\r\nselectors\r\n*0\r\n"),
    Step::Run(1, &[b"ACL", b"GETUSER", b"alice"], b"%6\r\n$5\r\nflags\r\n~2\r\n$2\r\non\r\n$16\r\nsanitize-payload\r\n$9\r\npasswords\r\n*2\r\n$64\r\nf64551fcd6f07823cb87971cfb91446425da18286b3ab1ef935e0cbd7a69f68a\r\n$64\r\n3946ca64ff78d93ca61090a437cbb6b3d2ca0d488f5f9ccf3059608368b27693\r\n$8\r\ncommands\r\n$12\r\n-@all +@hash\r\n$4\r\nkeys\r\n$30\r\n~cached:* %R~read:* %W~write:*\r\n$8\r\nchannels\r\n$7\r\n&news.*\r\n$9\r\nselectors\r\n*0\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"alice", b"<p1", b"resetkeys", b"allkeys"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"GETUSER", b"alice"], b"*12\r\n$5\r\nflags\r\n*2\r\n$2\r\non\r\n$16\r\nsanitize-payload\r\n$9\r\npasswords\r\n*1\r\n$64\r\n3946ca64ff78d93ca61090a437cbb6b3d2ca0d488f5f9ccf3059608368b27693\r\n$8\r\ncommands\r\n$12\r\n-@all +@hash\r\n$4\r\nkeys\r\n$2\r\n~*\r\n$8\r\nchannels\r\n$7\r\n&news.*\r\n$9\r\nselectors\r\n*0\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"alice", b"(+get", b"~x*)", b"(+set", b"%W~y*", b"&chan)"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"GETUSER", b"alice"], b"*12\r\n$5\r\nflags\r\n*2\r\n$2\r\non\r\n$16\r\nsanitize-payload\r\n$9\r\npasswords\r\n*1\r\n$64\r\n3946ca64ff78d93ca61090a437cbb6b3d2ca0d488f5f9ccf3059608368b27693\r\n$8\r\ncommands\r\n$12\r\n-@all +@hash\r\n$4\r\nkeys\r\n$2\r\n~*\r\n$8\r\nchannels\r\n$7\r\n&news.*\r\n$9\r\nselectors\r\n*2\r\n*6\r\n$8\r\ncommands\r\n$10\r\n-@all +get\r\n$4\r\nkeys\r\n$3\r\n~x*\r\n$8\r\nchannels\r\n$0\r\n\r\n*6\r\n$8\r\ncommands\r\n$10\r\n-@all +set\r\n$4\r\nkeys\r\n$5\r\n%W~y*\r\n$8\r\nchannels\r\n$5\r\n&chan\r\n"),
    Step::Run(1, &[b"ACL", b"GETUSER", b"alice"], b"%6\r\n$5\r\nflags\r\n~2\r\n$2\r\non\r\n$16\r\nsanitize-payload\r\n$9\r\npasswords\r\n*1\r\n$64\r\n3946ca64ff78d93ca61090a437cbb6b3d2ca0d488f5f9ccf3059608368b27693\r\n$8\r\ncommands\r\n$12\r\n-@all +@hash\r\n$4\r\nkeys\r\n$2\r\n~*\r\n$8\r\nchannels\r\n$7\r\n&news.*\r\n$9\r\nselectors\r\n*2\r\n%3\r\n$8\r\ncommands\r\n$10\r\n-@all +get\r\n$4\r\nkeys\r\n$3\r\n~x*\r\n$8\r\nchannels\r\n$0\r\n\r\n%3\r\n$8\r\ncommands\r\n$10\r\n-@all +set\r\n$4\r\nkeys\r\n$5\r\n%W~y*\r\n$8\r\nchannels\r\n$5\r\n&chan\r\n"),
    Step::Run(0, &[b"ACL", b"LIST"], b"*2\r\n$204\r\nuser alice on sanitize-payload #3946ca64ff78d93ca61090a437cbb6b3d2ca0d488f5f9ccf3059608368b27693 ~* resetchannels &news.* -@all +@hash (~x* resetchannels -@all +get) (%W~y* resetchannels &chan -@all +set)\r\n$51\r\nuser default on nopass sanitize-payload ~* &* +@all\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"alice", b"clearselectors"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"LIST"], b"*2\r\n$134\r\nuser alice on sanitize-payload #3946ca64ff78d93ca61090a437cbb6b3d2ca0d488f5f9ccf3059608368b27693 ~* resetchannels &news.* -@all +@hash\r\n$51\r\nuser default on nopass sanitize-payload ~* &* +@all\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"alice", b"reset"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"LIST"], b"*2\r\n$51\r\nuser alice off sanitize-payload resetchannels -@all\r\n$51\r\nuser default on nopass sanitize-payload ~* &* +@all\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"bad name"], b"-ERR Usernames can't contain spaces or null characters\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"bad", b"+nosuch"], b"-ERR Error in ACL SETUSER modifier '+nosuch': Unknown command or category name in ACL\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"bad", b"+@nosuch"], b"-ERR Error in ACL SETUSER modifier '+@nosuch': Unknown command or category name in ACL\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"bad", b"~a", b"allkeys", b"~b"], b"-ERR Error in ACL SETUSER modifier '~b': Adding a pattern after the * pattern (or the 'allkeys' flag) is not valid and does not have any effect. Try 'resetkeys' to start with an empty list of patterns\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"bad", b"allchannels", b"&x"], b"-ERR Error in ACL SETUSER modifier '&x': Adding a pattern after the * pattern (or the 'allchannels' flag) is not valid and does not have any effect. Try 'resetchannels' to start with an empty list of channels\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"bad", b"<nope"], b"-ERR Error in ACL SETUSER modifier '<nope': The password you are trying to remove from the user does not exist\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"bad", b"#abc"], b"-ERR Error in ACL SETUSER modifier '#abc': The password hash must be exactly 64 characters and contain only lowercase hexadecimal characters\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"bad", b"#2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"bad", b"!2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"bad", b"(+get", b"~x"], b"-ERR Unmatched parenthesis in acl selector starting at '(+get'.\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"bad", b"(", b"+get", b"~x", b")"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"bad", b"(+get", b"~x)"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"bad", b"(+nosuch)"], b"-ERR Error in ACL SETUSER modifier '(+nosuch)': Unknown command or category name in ACL\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"bad", b"+config|get|x"], b"-ERR Error in ACL SETUSER modifier '+config|get|x': Allowing first-arg of a subcommand is not supported\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"bad", b"+get|"], b"-ERR Error in ACL SETUSER modifier '+get|': Syntax error\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"bad", b"-select|0"], b"-ERR Error in ACL SETUSER modifier '-select|0': Unknown command or category name in ACL\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"bad", b"+select|0", b"+debug|object"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"bad", b"%X~a"], b"-ERR Error in ACL SETUSER modifier '%X~a': Syntax error\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"bad", b"%R"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"bad", b"skip-sanitize-payload"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"LIST"], b"*3\r\n$51\r\nuser alice off sanitize-payload resetchannels -@all\r\n$142\r\nuser bad off skip-sanitize-payload %R~ resetchannels -@all +select|0 +debug|object (~x resetchannels -@all +get) (~x resetchannels -@all +get)\r\n$51\r\nuser default on nopass sanitize-payload ~* &* +@all\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"cmds", b"+@all", b"-@hash", b"-@slow", b"+hget"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"GETUSER", b"cmds"], b"*12\r\n$5\r\nflags\r\n*2\r\n$3\r\noff\r\n$16\r\nsanitize-payload\r\n$9\r\npasswords\r\n*0\r\n$8\r\ncommands\r\n$25\r\n+@all -@hash -@slow +hget\r\n$4\r\nkeys\r\n$0\r\n\r\n$8\r\nchannels\r\n$0\r\n\r\n$9\r\nselectors\r\n*0\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"cmds", b"-@hash"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"cmds", b"+@hash"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"GETUSER", b"cmds"], b"*12\r\n$5\r\nflags\r\n*2\r\n$3\r\noff\r\n$16\r\nsanitize-payload\r\n$9\r\npasswords\r\n*0\r\n$8\r\ncommands\r\n$25\r\n+@all -@slow +hget +@hash\r\n$4\r\nkeys\r\n$0\r\n\r\n$8\r\nchannels\r\n$0\r\n\r\n$9\r\nselectors\r\n*0\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"cmds", b"-@all", b"+client|list", b"+client|list", b"+config|get", b"+config", b"+acl|list", b"-acl"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"GETUSER", b"cmds"], b"*12\r\n$5\r\nflags\r\n*2\r\n$3\r\noff\r\n$16\r\nsanitize-payload\r\n$9\r\npasswords\r\n*0\r\n$8\r\ncommands\r\n$31\r\n-@all +client|list +config -acl\r\n$4\r\nkeys\r\n$0\r\n\r\n$8\r\nchannels\r\n$0\r\n\r\n$9\r\nselectors\r\n*0\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"cmds", b"-@all", b"+config", b"+config|get", b"-config|set", b"+config"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"GETUSER", b"cmds"], b"*12\r\n$5\r\nflags\r\n*2\r\n$3\r\noff\r\n$16\r\nsanitize-payload\r\n$9\r\npasswords\r\n*0\r\n$8\r\ncommands\r\n$13\r\n-@all +config\r\n$4\r\nkeys\r\n$0\r\n\r\n$8\r\nchannels\r\n$0\r\n\r\n$9\r\nselectors\r\n*0\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"cmds", b"+@all", b"-client|kill", b"+client"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"GETUSER", b"cmds"], b"*12\r\n$5\r\nflags\r\n*2\r\n$3\r\noff\r\n$16\r\nsanitize-payload\r\n$9\r\npasswords\r\n*0\r\n$8\r\ncommands\r\n$13\r\n+@all +client\r\n$4\r\nkeys\r\n$0\r\n\r\n$8\r\nchannels\r\n$0\r\n\r\n$9\r\nselectors\r\n*0\r\n"),
    Step::Run(0, &[b"ACL", b"USERS"], b"*4\r\n$5\r\nalice\r\n$3\r\nbad\r\n$4\r\ncmds\r\n$7\r\ndefault\r\n"),
    Step::Run(0, &[b"ACL", b"DELUSER", b"default"], b"-ERR The 'default' user cannot be removed\r\n"),
    Step::Run(0, &[b"ACL", b"DELUSER", b"nobody"], b":0\r\n"),
    Step::Run(0, &[b"ACL", b"DELUSER", b"bad", b"cmds", b"nobody"], b":2\r\n"),
    Step::Run(0, &[b"ACL", b"USERS"], b"*2\r\n$5\r\nalice\r\n$7\r\ndefault\r\n"),
    // Logging in.
    Step::Run(0, &[b"AUTH", b"whatever"], b"-ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?\r\n"),
    Step::Run(0, &[b"AUTH", b"a", b"b", b"c"], b"-ERR syntax error\r\n"),
    Step::Run(0, &[b"AUTH", b"default", b"anything"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"bob", b"on", b">secret", b"~*", b"&*", b"+@all"], b"+OK\r\n"),
    Step::Run(1, &[b"AUTH", b"bob", b"wrong"], b"-WRONGPASS invalid username-password pair or user is disabled.\r\n"),
    Step::Run(1, &[b"AUTH", b"bob", b"secret"], b"+OK\r\n"),
    Step::Run(1, &[b"ACL", b"WHOAMI"], b"$3\r\nbob\r\n"),
    Step::Send(1, &[b"CLIENT", b"INFO"]),
    Step::Run(2, &[b"AUTH", b"nobody", b"x"], b"-WRONGPASS invalid username-password pair or user is disabled.\r\n"),
    Step::Run(2, &[b"HELLO", b"3", b"AUTH", b"bob", b"wrong"], b"-WRONGPASS invalid username-password pair or user is disabled.\r\n"),
    Step::Send(2, &[b"HELLO", b"2", b"AUTH", b"bob", b"secret", b"SETNAME", b"bobconn"]),
    Step::Send(2, &[b"CLIENT", b"INFO"]),
    Step::Run(2, &[b"HELLO", b"2", b"AUTH", b"bob", b"secret", b"SETNAME", b"bad name"], b"-ERR Client names cannot contain spaces, newlines or special characters.\r\n"),
    Step::Run(2, &[b"RESET"], b"+RESET\r\n"),
    Step::Run(2, &[b"ACL", b"WHOAMI"], b"$7\r\ndefault\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"bob", b"off"], b"+OK\r\n"),
    Step::Run(3, &[b"AUTH", b"bob", b"secret"], b"-WRONGPASS invalid username-password pair or user is disabled.\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"bob", b"on", b"nopass"], b"+OK\r\n"),
    Step::Run(3, &[b"AUTH", b"bob", b"anything"], b"+OK\r\n"),
    Step::Run(3, &[b"ACL", b"WHOAMI"], b"$3\r\nbob\r\n"),
    Step::Send(0, &[b"INFO", b"stats"]),
    // What a user may run.
    Step::Reset,
    Step::Run(0, &[b"ACL", b"SETUSER", b"u", b"on", b"nopass", b"-@all", b"+get", b"+set", b"+acl|whoami", b"~app:*", b"%R~ro:*", b"%W~wo:*"], b"+OK\r\n"),
    Step::Run(1, &[b"AUTH", b"u", b"x"], b"+OK\r\n"),
    Step::Run(1, &[b"GET", b"app:1"], b"$-1\r\n"),
    Step::Run(1, &[b"GET", b"other"], b"-NOPERM No permissions to access a key\r\n"),
    Step::Run(1, &[b"GET", b"ro:1"], b"$-1\r\n"),
    Step::Run(1, &[b"GET", b"wo:1"], b"-NOPERM No permissions to access a key\r\n"),
    Step::Run(1, &[b"SET", b"wo:1", b"v"], b"+OK\r\n"),
    Step::Run(1, &[b"SET", b"ro:1", b"v"], b"-NOPERM No permissions to access a key\r\n"),
    Step::Run(1, &[b"SET", b"wo:1", b"v", b"GET"], b"-NOPERM No permissions to access a key\r\n"),
    Step::Run(1, &[b"PING"], b"-NOPERM User u has no permissions to run the 'ping' command\r\n"),
    Step::Run(1, &[b"DEL", b"app:1"], b"-NOPERM User u has no permissions to run the 'del' command\r\n"),
    Step::Run(1, &[b"ACL", b"WHOAMI"], b"$1\r\nu\r\n"),
    Step::Run(1, &[b"ACL", b"USERS"], b"-NOPERM User u has no permissions to run the 'acl|users' command\r\n"),
    Step::Run(1, &[b"AUTH", b"default", b"x"], b"+OK\r\n"),
    Step::Run(1, &[b"GET", b"nothere", b"x"], b"-ERR wrong number of arguments for 'get' command\r\n"),
    Step::Run(1, &[b"FOO"], b"-ERR unknown command 'FOO', with args beginning with: \r\n"),
    Step::Run(1, &[b"CONFIG", b"GET"], b"-ERR wrong number of arguments for 'config|get' command\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"u", b"+mset", b"+copy", b"+exists", b"+client|id", b"+select|0"], b"+OK\r\n"),
    Step::Run(1, &[b"MSET", b"app:1", b"a", b"wo:2", b"b", b"ro:3", b"c"], b"+OK\r\n"),
    Step::Run(1, &[b"COPY", b"ro:1", b"wo:9"], b":0\r\n"),
    Step::Run(1, &[b"COPY", b"wo:1", b"ro:9"], b":1\r\n"),
    Step::Run(1, &[b"EXISTS", b"ro:1", b"wo:1", b"app:1"], b":2\r\n"),
    Step::Run(1, &[b"EXISTS", b"zzz"], b":0\r\n"),
    Step::Send(1, &[b"CLIENT", b"ID"]),
    Step::Send(1, &[b"CLIENT", b"INFO"]),
    Step::Run(1, &[b"SELECT", b"0"], b"+OK\r\n"),
    Step::Run(1, &[b"SELECT", b"1"], b"+OK\r\n"),
    Step::Run(1, &[b"CLIENT", b"KILL", b"ID", b"999"], b":0\r\n"),
    Step::Run(1, &[b"CLIENT", b"NOPE"], b"-ERR unknown subcommand 'NOPE'. Try CLIENT HELP.\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"u", b"+@all", b"-client|kill"], b"+OK\r\n"),
    Step::Run(1, &[b"CLIENT", b"KILL", b"ID", b"999"], b":0\r\n"),
    Step::Run(1, &[b"OBJECT", b"ENCODING", b"app:1"], b"$-1\r\n"),
    Step::Run(1, &[b"OBJECT", b"ENCODING", b"other"], b"$-1\r\n"),
    Step::Run(1, &[b"PUBLISH", b"chan", b"m"], b":0\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"u", b"&news.*", b"&lit"], b"+OK\r\n"),
    Step::Run(1, &[b"PUBLISH", b"news.1", b"m"], b":0\r\n"),
    Step::Run(1, &[b"PUBLISH", b"lit", b"m"], b":0\r\n"),
    Step::Run(1, &[b"PUBLISH", b"other", b"m"], b":0\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"u", b"allchannels"], b"+OK\r\n"),
    Step::Run(1, &[b"PUBLISH", b"other", b"m"], b":0\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"u", b"resetchannels"], b"+OK\r\n"),
    Step::Run(1, &[b"PUBLISH", b"other", b"m"], b":0\r\n"),
    // Selectors.
    Step::Reset,
    Step::Run(0, &[b"ACL", b"SETUSER", b"s", b"on", b"nopass", b"+get", b"~a*", b"(+set", b"~b*)", b"(+del", b"%R~c*)"], b"+OK\r\n"),
    Step::Run(1, &[b"AUTH", b"s", b"x"], b"+OK\r\n"),
    Step::Run(1, &[b"GET", b"a1"], b"$-1\r\n"),
    Step::Run(1, &[b"SET", b"b1", b"v"], b"+OK\r\n"),
    Step::Run(1, &[b"GET", b"b1"], b"-NOPERM No permissions to access a key\r\n"),
    Step::Run(1, &[b"SET", b"a1", b"v"], b"-NOPERM No permissions to access a key\r\n"),
    Step::Run(1, &[b"DEL", b"c1"], b"-NOPERM No permissions to access a key\r\n"),
    Step::Run(1, &[b"DEL", b"a1"], b"-NOPERM No permissions to access a key\r\n"),
    Step::Run(1, &[b"HGET", b"a1", b"f"], b"-NOPERM User s has no permissions to run the 'hget' command\r\n"),
    Step::Run(1, &[b"DRYRUN"], b"-ERR unknown command 'DRYRUN', with args beginning with: \r\n"),
    // The log.
    Step::Reset,
    Step::Run(0, &[b"ACL", b"SETUSER", b"l", b"on", b">pw", b"-@all", b"+get", b"~k*"], b"+OK\r\n"),
    Step::Run(1, &[b"AUTH", b"l", b"wrong"], b"-WRONGPASS invalid username-password pair or user is disabled.\r\n"),
    Step::Run(1, &[b"AUTH", b"l", b"pw"], b"+OK\r\n"),
    Step::Run(1, &[b"GET", b"k1"], b"$-1\r\n"),
    Step::Run(1, &[b"GET", b"other"], b"-NOPERM No permissions to access a key\r\n"),
    Step::Run(1, &[b"GET", b"other"], b"-NOPERM No permissions to access a key\r\n"),
    Step::Run(1, &[b"SET", b"k1", b"v"], b"-NOPERM User l has no permissions to run the 'set' command\r\n"),
    Step::Run(1, &[b"PUBLISH", b"c", b"m"], b"-NOPERM User l has no permissions to run the 'publish' command\r\n"),
    Step::Send(0, &[b"ACL", b"LOG"]),
    Step::Send(0, &[b"ACL", b"LOG", b"1"]),
    Step::Run(0, &[b"ACL", b"LOG", b"0"], b"*0\r\n"),
    Step::Run(0, &[b"ACL", b"LOG", b"-5"], b"*0\r\n"),
    Step::Run(0, &[b"ACL", b"LOG", b"nope"], b"-ERR value is not an integer or out of range\r\n"),
    Step::Run(0, &[b"ACL", b"LOG", b"a", b"b"], b"-ERR unknown subcommand or wrong number of arguments for 'LOG'. Try ACL HELP.\r\n"),
    Step::Run(0, &[b"ACL", b"LOG", b"RESET"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"LOG"], b"*0\r\n"),
    Step::Send(0, &[b"INFO", b"stats"]),
    // Transactions.
    Step::Reset,
    Step::Run(0, &[b"ACL", b"SETUSER", b"t", b"on", b"nopass", b"+@all", b"-set", b"~*"], b"+OK\r\n"),
    Step::Run(1, &[b"AUTH", b"t", b"x"], b"+OK\r\n"),
    Step::Run(1, &[b"MULTI"], b"+OK\r\n"),
    Step::Run(1, &[b"GET", b"k"], b"+QUEUED\r\n"),
    Step::Run(1, &[b"SET", b"k", b"v"], b"-NOPERM User t has no permissions to run the 'set' command\r\n"),
    Step::Run(1, &[b"EXEC"], b"-EXECABORT Transaction discarded because of previous errors.\r\n"),
    Step::Run(1, &[b"MULTI"], b"+OK\r\n"),
    Step::Run(1, &[b"GET", b"k"], b"+QUEUED\r\n"),
    Step::Run(1, &[b"INCR", b"k"], b"+QUEUED\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"t", b"-incr"], b"+OK\r\n"),
    Step::Run(1, &[b"EXEC"], b"*2\r\n$-1\r\n-NOPERM ACLs rules changed between the moment the transaction was accumulated and the EXEC call. This command is no longer allowed for the following reason: no permission to execute the command or subcommand\r\n"),
    Step::Send(0, &[b"ACL", b"LOG"]),
    // Scripts.
    Step::Reset,
    Step::Run(0, &[b"ACL", b"SETUSER", b"sc", b"on", b"nopass", b"+@all", b"-incr", b"~k*"], b"+OK\r\n"),
    Step::Run(1, &[b"AUTH", b"sc", b"x"], b"+OK\r\n"),
    Step::Run(1, &[b"EVAL", b"return redis.call('incr','k')", b"0"], b"-ERR ACL failure in script: User sc has no permissions to run the 'incr' command script: 1f71dc1cd0b838440b29a02b9ecb8480e01af28a, on @user_script:1.\r\n"),
    Step::Run(1, &[b"EVAL", b"return redis.pcall('incr','k')", b"0"], b"-ERR ACL failure in script: User sc has no permissions to run the 'incr' command\r\n"),
    Step::Run(1, &[b"EVAL", b"return redis.call('get','other')", b"0"], b"-ERR ACL failure in script: No permissions to access a key script: 733db9816e9c67c4cc15572d4e80f4ebb13b5cdc, on @user_script:1.\r\n"),
    Step::Run(1, &[b"EVAL", b"return redis.call('get','k')", b"0"], b"$-1\r\n"),
    Step::Run(1, &[b"EVAL", b"return redis.acl_check_cmd('incr','k')", b"0"], b"$-1\r\n"),
    Step::Run(1, &[b"EVAL", b"return redis.acl_check_cmd('get','k')", b"0"], b":1\r\n"),
    Step::Run(1, &[b"EVAL", b"return redis.acl_check_cmd('get','other')", b"0"], b"$-1\r\n"),
    Step::Run(1, &[b"EVAL", b"return redis.acl_check_cmd('nosuch')", b"0"], b"-ERR Invalid command passed to redis.acl_check_cmd() script: d2491587bab5149c93be0dce4426f7a06ba9e1a1, on @user_script:1.\r\n"),
    Step::Run(1, &[b"EVAL", b"return 1", b"1", b"other"], b"-NOPERM No permissions to access a key\r\n"),
    Step::Run(1, &[b"EVAL", b"return 1", b"1", b"k1"], b":1\r\n"),
    Step::Send(0, &[b"ACL", b"LOG"]),
    // Dry runs.
    Step::Reset,
    Step::Run(0, &[b"ACL", b"SETUSER", b"d", b"on", b"nopass", b"+get", b"~k*", b"&c"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"d", b"GET", b"k"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"d", b"GET", b"other"], b"$51\r\nUser d has no permissions to access the 'other' key\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"d", b"SET", b"k", b"v"], b"$50\r\nUser d has no permissions to run the 'set' command\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"d", b"PUBLISH", b"x", b"m"], b"$54\r\nUser d has no permissions to run the 'publish' command\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"d", b"GET"], b"-ERR wrong number of arguments for 'get' command\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"d", b"NOSUCH", b"x"], b"-ERR Command 'NOSUCH' not found\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"nobody", b"GET", b"k"], b"-ERR User 'nobody' not found\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"d", b"CONFIG", b"GET", b"x"], b"$57\r\nUser d has no permissions to run the 'config|get' command\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"d", b"CONFIG"], b"-ERR wrong number of arguments for 'config' command\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"d", b"CONFIG", b"NOPE"], b"-ERR Command 'CONFIG' not found\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"default", b"SET", b"k", b"v"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"command-test", b"+@all", b"%R~read*", b"%W~write*", b"%RW~rw*"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"SORT", b"read", b"STORE", b"write"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"SORT", b"read", b"STORE", b"read"], b"$61\r\nUser command-test has no permissions to access the 'read' key\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"SORT", b"write", b"STORE", b"write"], b"$62\r\nUser command-test has no permissions to access the 'write' key\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"EVAL", b"", b"1", b"rw1"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"EVAL", b"", b"1", b"read"], b"$61\r\nUser command-test has no permissions to access the 'read' key\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"EVAL_RO", b"", b"1", b"rw1"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"EVAL_RO", b"", b"1", b"read"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"EVAL", b"", b"0", b"read"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"EVAL", b"", b"-1", b"read"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"EVAL", b"", b"3", b"rw", b"rw"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"EVAL", b"", b"3", b"rw", b"read"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"GEORADIUS", b"read", b"longitude", b"latitude", b"radius", b"M", b"STOREDIST", b"write"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"GEORADIUS", b"read", b"longitude", b"latitude", b"radius", b"M"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"GEORADIUS", b"read1", b"longitude", b"latitude", b"radius", b"M", b"STOREDIST", b"read2"], b"$62\r\nUser command-test has no permissions to access the 'read2' key\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"GEORADIUS", b"write1", b"longitude", b"latitude", b"radius", b"M", b"STOREDIST", b"write2"], b"$63\r\nUser command-test has no permissions to access the 'write1' key\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"GEORADIUS", b"read", b"longitude", b"latitude", b"radius", b"M", b"STORE", b"write"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"XREAD", b"STREAMS", b"read1", b"read2", b"0", b"0"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"XREAD", b"STREAMS", b"read1", b"write2", b"0", b"0"], b"$63\r\nUser command-test has no permissions to access the 'write2' key\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"rw1", b"0"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"read1", b"0"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"LMPOP", b"2", b"read1", b"rw1", b"LEFT"], b"$62\r\nUser command-test has no permissions to access the 'read1' key\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"LMPOP", b"2", b"rw1", b"rw2", b"LEFT"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"ZUNIONSTORE", b"write1", b"2", b"read1", b"read2"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"ZUNIONSTORE", b"read1", b"2", b"read1", b"read2"], b"$62\r\nUser command-test has no permissions to access the 'read1' key\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"BITFIELD", b"read1", b"GET", b"u8", b"0"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"BITFIELD", b"read1", b"SET", b"u8", b"0", b"1"], b"$62\r\nUser command-test has no permissions to access the 'read1' key\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"BITFIELD", b"rw1", b"SET", b"u8", b"0", b"1"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"SET", b"write1", b"v"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"SET", b"write1", b"v", b"GET"], b"$63\r\nUser command-test has no permissions to access the 'write1' key\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"OBJECT", b"ENCODING", b"read1"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"OBJECT", b"ENCODING", b"write1"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"XINFO", b"STREAM", b"write1"], b"$63\r\nUser command-test has no permissions to access the 'write1' key\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"MEMORY", b"USAGE", b"write1"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"FUNCTION", b"LIST"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"command-test", b"-@scripting"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"FCALL_RO", b"f", b"0"], b"$66\r\nUser command-test has no permissions to run the 'fcall_ro' command\r\n"),
    Step::Run(0, &[b"ACL", b"DRYRUN", b"command-test", b"EVAL", b"", b"0"], b"$62\r\nUser command-test has no permissions to run the 'eval' command\r\n"),
    // Deleting users closes their connections.
    Step::Reset,
    Step::Run(0, &[b"ACL", b"SETUSER", b"gone", b"on", b"nopass", b"+@all", b"~*", b"&*"], b"+OK\r\n"),
    Step::Run(1, &[b"AUTH", b"gone", b"x"], b"+OK\r\n"),
    Step::Run(2, &[b"AUTH", b"gone", b"x"], b"+OK\r\n"),
    Step::Run(2, &[b"PING"], b"+PONG\r\n"),
    Step::Run(0, &[b"ACL", b"DELUSER", b"gone"], b":1\r\n"),
    Step::Sleep(100),
    Step::Run(1, &[b"PING"], b"<closed>"),
    Step::Run(2, &[b"PING"], b"<closed>"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"selfdel", b"on", b"nopass", b"+@all", b"~*", b"&*"], b"+OK\r\n"),
    Step::Run(3, &[b"AUTH", b"selfdel", b"x"], b"+OK\r\n"),
    Step::Run(3, &[b"ACL", b"DELUSER", b"selfdel"], b":1\r\n"),
    Step::Run(3, &[b"PING"], b"<closed>"),
    // Losing a channel closes a subscriber.
    Step::Reset,
    Step::Run(0, &[b"ACL", b"SETUSER", b"sub", b"on", b"nopass", b"+@all", b"~*", b"&news.*", b"&sport"], b"+OK\r\n"),
    Step::Run(1, &[b"AUTH", b"sub", b"x"], b"+OK\r\n"),
    Step::Run(1, &[b"SUBSCRIBE", b"news.1", b"sport"], b"*3\r\n$9\r\nsubscribe\r\n$6\r\nnews.1\r\n:1\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$9\r\nsubscribe\r\n$5\r\nsport\r\n:2\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"sub", b"&extra"], b"+OK\r\n"),
    Step::Run(0, &[b"PUBLISH", b"sport", b"hello"], b":1\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$5\r\nsport\r\n$5\r\nhello\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"sub", b"resetchannels", b"&sport"], b"+OK\r\n"),
    Step::Sleep(100),
    Step::Run(1, &[b"PING"], b"<closed>"),
    // SORT's patterns read other keys.
    Step::Reset,
    Step::Run(0, &[b"ACL", b"SETUSER", b"so", b"on", b"nopass", b"+@all", b"~list*"], b"+OK\r\n"),
    Step::Run(0, &[b"RPUSH", b"list", b"3", b"1", b"2"], b":3\r\n"),
    Step::Run(1, &[b"AUTH", b"so", b"x"], b"+OK\r\n"),
    Step::Run(1, &[b"SORT", b"list"], b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"),
    Step::Run(1, &[b"SORT", b"list", b"BY", b"nosort"], b"*3\r\n$1\r\n3\r\n$1\r\n1\r\n$1\r\n2\r\n"),
    Step::Run(1, &[b"SORT", b"list", b"BY", b"w_*"], b"-ERR BY option of SORT denied due to insufficient ACL permissions.\r\n"),
    Step::Run(1, &[b"SORT", b"list", b"GET", b"#"], b"-ERR GET option of SORT denied due to insufficient ACL permissions.\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"so", b"%R~*"], b"+OK\r\n"),
    Step::Run(1, &[b"SORT", b"list", b"BY", b"w_*", b"GET", b"#"], b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"),
    // Passwords.
    Step::Reset,
    Step::Run(0, &[b"ACL", b"GENPASS", b"4097"], b"-ERR ACL GENPASS argument must be the number of bits for the output password, a positive number up to 4096\r\n"),
    Step::Run(0, &[b"ACL", b"GENPASS", b"0"], b"-ERR ACL GENPASS argument must be the number of bits for the output password, a positive number up to 4096\r\n"),
    Step::Run(0, &[b"ACL", b"GENPASS", b"nope"], b"-ERR value is not an integer or out of range\r\n"),
    Step::Send(0, &[b"ACL", b"GENPASS"]),
    Step::Send(0, &[b"ACL", b"GENPASS", b"1"]),
    Step::Send(0, &[b"ACL", b"GENPASS", b"5"]),
    Step::Send(0, &[b"ACL", b"GENPASS", b"4096"]),
    Step::Run(0, &[b"CONFIG", b"SET", b"requirepass", b"hunter2"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"GETUSER", b"default"], b"*12\r\n$5\r\nflags\r\n*2\r\n$2\r\non\r\n$16\r\nsanitize-payload\r\n$9\r\npasswords\r\n*1\r\n$64\r\nf52fbd32b2b3b86ff88ef6c490628285f482af15ddcb29541f94bcf526a3f6c7\r\n$8\r\ncommands\r\n$5\r\n+@all\r\n$4\r\nkeys\r\n$2\r\n~*\r\n$8\r\nchannels\r\n$2\r\n&*\r\n$9\r\nselectors\r\n*0\r\n"),
    Step::Run(1, &[b"PING"], b"-NOAUTH Authentication required.\r\n"),
    Step::Run(1, &[b"AUTH", b"hunter2"], b"+OK\r\n"),
    Step::Run(1, &[b"PING"], b"+PONG\r\n"),
    Step::Run(0, &[b"CONFIG", b"SET", b"requirepass", b""], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"GETUSER", b"default"], b"*12\r\n$5\r\nflags\r\n*3\r\n$2\r\non\r\n$6\r\nnopass\r\n$16\r\nsanitize-payload\r\n$9\r\npasswords\r\n*0\r\n$8\r\ncommands\r\n$5\r\n+@all\r\n$4\r\nkeys\r\n$2\r\n~*\r\n$8\r\nchannels\r\n$2\r\n&*\r\n$9\r\nselectors\r\n*0\r\n"),
    Step::Run(0, &[b"CLIENT", b"KILL", b"USER", b"nobody"], b"-ERR No such user 'nobody'\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"k", b"on", b"nopass", b"+@all"], b"+OK\r\n"),
    Step::Run(1, &[b"AUTH", b"k", b"x"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"KILL", b"USER", b"k"], b":1\r\n"),
    Step::Sleep(100),
    Step::Run(1, &[b"PING"], b"<closed>"),
    // The default user, restricted.
    Step::Reset,
    Step::Run(0, &[b"ACL", b"SETUSER", b"admin", b"on", b">adminpw", b"~*", b"&*", b"+@all"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"default", b"-flushall"], b"+OK\r\n"),
    Step::Run(0, &[b"FLUSHALL"], b"-NOPERM User default has no permissions to run the 'flushall' command\r\n"),
    Step::Run(1, &[b"FLUSHALL"], b"-NOPERM User default has no permissions to run the 'flushall' command\r\n"),
    Step::Run(1, &[b"GET", b"k"], b"$-1\r\n"),
    Step::Run(1, &[b"AUTH", b"admin", b"adminpw"], b"+OK\r\n"),
    Step::Run(1, &[b"FLUSHALL"], b"+OK\r\n"),
    Step::Run(0, &[b"ACL", b"SETUSER", b"default", b"off"], b"+OK\r\n"),
    Step::Run(2, &[b"PING"], b"-NOAUTH Authentication required.\r\n"),
    Step::Run(2, &[b"AUTH", b"admin", b"adminpw"], b"+OK\r\n"),
    Step::Run(2, &[b"ACL", b"SETUSER", b"default", b"on", b"+flushall"], b"+OK\r\n"),
    Step::Run(2, &[b"PING"], b"+PONG\r\n"),
// END STEPS
];

struct Connections {
    addr: SocketAddr,
    open: HashMap<usize, BufReader<TcpStream>>,
}

impl Connections {
    async fn call(&mut self, n: usize, args: &[&[u8]]) -> Vec<u8> {
        if !self.open.contains_key(&n) {
            // (A server may still be coming up.)
            let mut stream = None;
            for _ in 0..100 {
                if let Ok(s) = TcpStream::connect(self.addr).await {
                    stream = Some(s);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let stream = stream.expect("the server comes up");
            stream.set_nodelay(true).unwrap();
            self.open.insert(n, BufReader::new(stream));
        }
        let conn = self.open.get_mut(&n).expect("open");
        let read_only = args == [b"READ"];
        if !read_only && conn.get_mut().write_all(&req(args)).await.is_err() {
            self.open.remove(&n);
            return b"<closed>".to_vec();
        }
        // A connection the server closed: nothing more to read.
        let closed = match tokio::time::timeout(Duration::from_secs(10), conn.fill_buf()).await {
            Ok(Ok(buf)) => buf.is_empty(),
            Ok(Err(_)) => true,
            Err(_) => panic!("no reply to {args:?}"),
        };
        if closed {
            self.open.remove(&n);
            return b"<closed>".to_vec();
        }
        read_reply(conn).await
    }

    async fn reset(&mut self) {
        self.open.clear();
        let users = self.call(0, &[b"ACL", b"USERS"]).await;
        let text = String::from_utf8(users).unwrap();
        for name in text
            .split("\r\n")
            .filter(|l| !l.starts_with(['*', '$']) && !l.is_empty())
        {
            if name != "default" {
                self.call(0, &[b"ACL", b"DELUSER", name.as_bytes()]).await;
            }
        }
        for cmd in [
            &[
                &b"ACL"[..],
                b"SETUSER",
                b"default",
                b"reset",
                b"on",
                b"nopass",
                b"~*",
                b"&*",
                b"+@all",
            ][..],
            &[b"CONFIG", b"SET", b"requirepass", b""],
            &[b"ACL", b"LOG", b"RESET"],
            &[b"CONFIG", b"RESETSTAT"],
            &[b"FLUSHALL"],
        ] {
            self.call(0, cmd).await;
        }
    }
}

fn shown(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

async fn replay(mode: Mode) {
    let (addr, _ctx) = start(mode).await;
    let mut conns = Connections {
        addr,
        open: HashMap::new(),
    };
    for (i, step) in STEPS.iter().enumerate() {
        match step {
            Step::Run(n, args, expected) => {
                let reply = conns.call(*n, args).await;
                let command: Vec<String> = args.iter().map(|a| shown(a)).collect();
                assert_eq!(
                    shown(&reply),
                    shown(expected),
                    "step {i}, {mode:?}, on connection {n}: {command:?}"
                );
            }
            Step::Send(n, args) => {
                conns.call(*n, args).await;
            }
            Step::Reset => conns.reset().await,
            Step::Sleep(ms) => tokio::time::sleep(Duration::from_millis(*ms)).await,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn users_and_their_rules_work_as_in_redis() {
    for mode in MODES {
        replay(mode).await;
    }
}
