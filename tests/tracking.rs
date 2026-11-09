//! Client-side caching: `CLIENT TRACKING`, `CACHING`, `GETREDIR` and
//! `TRACKINGINFO`. The scenarios, and Redis 7.2.4's replies, are recorded by
//! `tools/tracking-diff.py --rust`. Here, besides, a client caches what it
//! reads while other connections write, and is never left with a stale value.

mod common;

use common::{Client, MODES, Mode, Step, replay_steps_expiring, start};

#[rustfmt::skip]
const STEPS: &[Step] = &[
// BEGIN STEPS
    // Errors, in Redis' order.
    Step::Run(0, &[b"CLIENT", b"TRACKING"], b"-ERR wrong number of arguments for 'client|tracking' command\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"maybe"], b"-ERR syntax error\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"nonsense"], b"-ERR syntax error\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"REDIRECT"], b"-ERR syntax error\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"REDIRECT", b"abc"], b"-ERR value is not an integer or out of range\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"REDIRECT", b"0"], b"-ERR The client ID you want redirect to does not exist\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"REDIRECT", b"-5"], b"-ERR The client ID you want redirect to does not exist\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"REDIRECT", b"999999"], b"-ERR The client ID you want redirect to does not exist\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"REDIRECT", b"999999", b"REDIRECT", b"999998"], b"-ERR The client ID you want redirect to does not exist\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"PREFIX", b"a"], b"-ERR PREFIX option requires BCAST mode to be enabled\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"PREFIX"], b"-ERR syntax error\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"BCAST", b"OPTIN"], b"-ERR OPTIN and OPTOUT are not compatible with BCAST\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"BCAST", b"OPTOUT"], b"-ERR OPTIN and OPTOUT are not compatible with BCAST\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"OPTIN", b"OPTOUT"], b"-ERR You can't specify both OPTIN mode and OPTOUT mode\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"BCAST", b"PREFIX", b"FOOBAR", b"PREFIX", b"FOO"], b"-ERR Prefix 'FOOBAR' overlaps with another provided prefix 'FOO'. Prefixes for a single client must not overlap.\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"BCAST", b"PREFIX", b"FOO", b"PREFIX", b"FOOBAR"], b"-ERR Prefix 'FOO' overlaps with another provided prefix 'FOOBAR'. Prefixes for a single client must not overlap.\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"BCAST", b"PREFIX", b"FOO", b"PREFIX", b"FOO"], b"-ERR Prefix 'FOO' overlaps with another provided prefix 'FOO'. Prefixes for a single client must not overlap.\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"BCAST", b"PREFIX", b"", b"PREFIX", b"a"], b"-ERR Prefix '' overlaps with another provided prefix 'a'. Prefixes for a single client must not overlap.\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"maybe", b"BCAST"], b"-ERR syntax error\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"off", b"nonsense"], b"-ERR syntax error\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"off", b"PREFIX", b"a"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"off", b"BCAST", b"OPTIN", b"OPTOUT"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKINGINFO"], b"*6\r\n$5\r\nflags\r\n*1\r\n$3\r\noff\r\n$8\r\nredirect\r\n:-1\r\n$8\r\nprefixes\r\n*0\r\n"),
    Step::Run(0, &[b"CLIENT", b"CACHING"], b"-ERR wrong number of arguments for 'client|caching' command\r\n"),
    Step::Run(0, &[b"CLIENT", b"CACHING", b"yes"], b"-ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled\r\n"),
    Step::Run(0, &[b"CLIENT", b"CACHING", b"yes", b"extra"], b"-ERR wrong number of arguments for 'client|caching' command\r\n"),
    Step::Run(0, &[b"CLIENT", b"GETREDIR"], b":-1\r\n"),
    Step::Run(0, &[b"CLIENT", b"GETREDIR", b"x"], b"-ERR wrong number of arguments for 'client|getredir' command\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKINGINFO", b"x"], b"-ERR wrong number of arguments for 'client|trackinginfo' command\r\n"),
    Step::Send(0, &[b"CLIENT", b"INFO"]),
    Step::Send(0, &[b"INFO"]),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"GETREDIR"], b":0\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKINGINFO"], b"*6\r\n$5\r\nflags\r\n*1\r\n$2\r\non\r\n$8\r\nredirect\r\n:0\r\n$8\r\nprefixes\r\n*0\r\n"),
    Step::Send(0, &[b"CLIENT", b"INFO"]),
    Step::Send(0, &[b"INFO"]),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"BCAST"], b"-ERR You can't switch BCAST mode on/off before disabling tracking for this client, and then re-enabling it with a different mode.\r\n"),
    Step::Run(0, &[b"CLIENT", b"CACHING", b"yes"], b"-ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.\r\n"),
    Step::Run(0, &[b"CLIENT", b"CACHING", b"no"], b"-ERR CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"OPTIN"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"OPTOUT"], b"-ERR You can't switch OPTIN/OPTOUT mode before disabling tracking for this client, and then re-enabling it with a different mode.\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKINGINFO"], b"*6\r\n$5\r\nflags\r\n*2\r\n$2\r\non\r\n$5\r\noptin\r\n$8\r\nredirect\r\n:0\r\n$8\r\nprefixes\r\n*0\r\n"),
    Step::Run(0, &[b"CLIENT", b"CACHING", b"no"], b"-ERR CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.\r\n"),
    Step::Run(0, &[b"CLIENT", b"CACHING", b"maybe"], b"-ERR syntax error\r\n"),
    Step::Run(0, &[b"CLIENT", b"CACHING", b"yes"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKINGINFO"], b"*6\r\n$5\r\nflags\r\n*3\r\n$2\r\non\r\n$5\r\noptin\r\n$11\r\ncaching-yes\r\n$8\r\nredirect\r\n:0\r\n$8\r\nprefixes\r\n*0\r\n"),
    Step::Run(0, &[b"PING"], b"+PONG\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKINGINFO"], b"*6\r\n$5\r\nflags\r\n*2\r\n$2\r\non\r\n$5\r\noptin\r\n$8\r\nredirect\r\n:0\r\n$8\r\nprefixes\r\n*0\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"off"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"OPTOUT"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"CACHING", b"no"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKINGINFO"], b"*6\r\n$5\r\nflags\r\n*3\r\n$2\r\non\r\n$6\r\noptout\r\n$10\r\ncaching-no\r\n$8\r\nredirect\r\n:0\r\n$8\r\nprefixes\r\n*0\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"NOLOOP"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKINGINFO"], b"*6\r\n$5\r\nflags\r\n*2\r\n$2\r\non\r\n$6\r\nnoloop\r\n$8\r\nredirect\r\n:0\r\n$8\r\nprefixes\r\n*0\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"off"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"BCAST", b"PREFIX", b"foo", b"PREFIX", b"bar"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKINGINFO"], b"*6\r\n$5\r\nflags\r\n*2\r\n$2\r\non\r\n$5\r\nbcast\r\n$8\r\nredirect\r\n:0\r\n$8\r\nprefixes\r\n*2\r\n$3\r\nbar\r\n$3\r\nfoo\r\n"),
    Step::Send(0, &[b"CLIENT", b"INFO"]),
    Step::Send(0, &[b"INFO"]),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"BCAST", b"PREFIX", b"fo"], b"-ERR Prefix 'fo' overlaps with an existing prefix 'foo'. Prefixes for a single client must not overlap.\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"BCAST", b"PREFIX", b"barb"], b"-ERR Prefix 'barb' overlaps with an existing prefix 'bar'. Prefixes for a single client must not overlap.\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"BCAST", b"PREFIX", b"baz"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKINGINFO"], b"*6\r\n$5\r\nflags\r\n*2\r\n$2\r\non\r\n$5\r\nbcast\r\n$8\r\nredirect\r\n:0\r\n$8\r\nprefixes\r\n*3\r\n$3\r\nbar\r\n$3\r\nbaz\r\n$3\r\nfoo\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"BCAST"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKINGINFO"], b"*6\r\n$5\r\nflags\r\n*2\r\n$2\r\non\r\n$5\r\nbcast\r\n$8\r\nredirect\r\n:0\r\n$8\r\nprefixes\r\n*4\r\n$0\r\n\r\n$3\r\nbar\r\n$3\r\nbaz\r\n$3\r\nfoo\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on"], b"-ERR You can't switch BCAST mode on/off before disabling tracking for this client, and then re-enabling it with a different mode.\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"off"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKINGINFO"], b"*6\r\n$5\r\nflags\r\n*1\r\n$3\r\noff\r\n$8\r\nredirect\r\n:-1\r\n$8\r\nprefixes\r\n*0\r\n"),
    Step::Run(0, &[b"CLIENT", b"GETREDIR"], b":-1\r\n"),
    Step::Send(0, &[b"INFO"]),
    Step::Send(5, &[b"HELLO", b"3"]),
    Step::Run(5, &[b"CLIENT", b"TRACKINGINFO"], b"%3\r\n$5\r\nflags\r\n~1\r\n$3\r\noff\r\n$8\r\nredirect\r\n:-1\r\n$8\r\nprefixes\r\n*0\r\n"),
    Step::Run(5, &[b"CLIENT", b"TRACKING", b"on", b"BCAST", b"PREFIX", b"x"], b"+OK\r\n"),
    Step::Run(5, &[b"CLIENT", b"TRACKINGINFO"], b"%3\r\n$5\r\nflags\r\n~2\r\n$2\r\non\r\n$5\r\nbcast\r\n$8\r\nredirect\r\n:0\r\n$8\r\nprefixes\r\n*1\r\n$1\r\nx\r\n"),
    Step::Run(5, &[b"CLIENT", b"TRACKING", b"off"], b"+OK\r\n"),
    Step::Reset,
    // Invalidations redirected to a RESP2 connection subscribed to __redis__:invalidate.
    Step::Send(1, &[b"CLIENT", b"ID"]),
    Step::Run(1, &[b"SUBSCRIBE", b"__redis__:invalidate"], b"*3\r\n$9\r\nsubscribe\r\n$20\r\n__redis__:invalidate\r\n:1\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"REDIRECT", b"{id:1}"], b"+OK\r\n"),
    Step::Send(0, &[b"CLIENT", b"GETREDIR"]),
    Step::Send(0, &[b"CLIENT", b"INFO"]),
    Step::Send(0, &[b"CLIENT", b"TRACKINGINFO"]),
    Step::Run(0, &[b"SET", b"a", b"1"], b"+OK\r\n"),
    Step::Run(0, &[b"SET", b"b", b"1"], b"+OK\r\n"),
    Step::Run(0, &[b"GET", b"a"], b"$1\r\n1\r\n"),
    Step::Run(0, &[b"INCR", b"b"], b":2\r\n"),
    Step::Run(0, &[b"INCR", b"a"], b":2\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\na\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    // Read once, heard of once.
    Step::Run(0, &[b"INCR", b"a"], b":3\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Run(0, &[b"GET", b"a"], b"$1\r\n3\r\n"),
    Step::Run(0, &[b"GET", b"a"], b"$1\r\n3\r\n"),
    Step::Run(2, &[b"SET", b"a", b"5"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\na\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    // Every key a command reads: MGET, missing ones too, and a read that fails.
    Step::Run(0, &[b"MGET", b"a", b"b", b"c", b"d"], b"*4\r\n$1\r\n5\r\n$1\r\n2\r\n$-1\r\n$-1\r\n"),
    Step::Send(0, &[b"INFO"]),
    Step::Run(2, &[b"MSET", b"a", b"1", b"b", b"2", b"c", b"3"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\na\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\nb\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\nc\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Run(0, &[b"LLEN", b"a"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    Step::Run(2, &[b"SET", b"a", b"2"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\na\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Send(0, &[b"INFO"]),
    Step::Run(0, &[b"GET", b"d"], b"$-1\r\n"),
    Step::Run(2, &[b"DEL", b"d"], b":0\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Run(2, &[b"SET", b"d", b"1"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\nd\r\n"),
    Step::Run(0, &[b"LPUSH", b"l", b"x"], b":1\r\n"),
    Step::Run(0, &[b"LLEN", b"l"], b":1\r\n"),
    Step::Run(0, &[b"LRANGE", b"l", b"0", b"-1"], b"*1\r\n$1\r\nx\r\n"),
    Step::Run(2, &[b"RPUSH", b"l", b"y"], b":2\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\nl\r\n"),
    Step::Run(0, &[b"GET", b"l"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    Step::Run(2, &[b"DEL", b"l"], b":1\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\nl\r\n"),
    // Keys written but not read aren't tracked; nor are a write's own.
    Step::Run(0, &[b"SET", b"w", b"1"], b"+OK\r\n"),
    Step::Run(2, &[b"SET", b"w", b"2"], b"+OK\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Run(0, &[b"INCR", b"w"], b":3\r\n"),
    Step::Run(2, &[b"SET", b"w", b"3"], b"+OK\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    // Reads of every kind.
    Step::Run(0, &[b"HSET", b"h", b"f", b"v"], b":1\r\n"),
    Step::Run(0, &[b"HGET", b"h", b"f"], b"$1\r\nv\r\n"),
    Step::Run(2, &[b"HSET", b"h", b"g", b"w"], b":1\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\nh\r\n"),
    Step::Run(0, &[b"ZADD", b"z", b"1", b"m"], b":1\r\n"),
    Step::Run(0, &[b"ZSCORE", b"z", b"m"], b"$1\r\n1\r\n"),
    Step::Run(2, &[b"ZINCRBY", b"z", b"1", b"m"], b"$1\r\n2\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\nz\r\n"),
    Step::Run(0, &[b"SADD", b"s", b"m"], b":1\r\n"),
    Step::Run(0, &[b"SISMEMBER", b"s", b"m"], b":1\r\n"),
    Step::Run(2, &[b"SREM", b"s", b"m"], b":1\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\ns\r\n"),
    Step::Run(0, &[b"XADD", b"x", b"1-1", b"f", b"v"], b"$3\r\n1-1\r\n"),
    Step::Run(0, &[b"XRANGE", b"x", b"-", b"+"], b"*1\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    Step::Run(2, &[b"XADD", b"x", b"2-1", b"f", b"v"], b"$3\r\n2-1\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\nx\r\n"),
    Step::Run(0, &[b"SET", b"bits", b"1"], b"+OK\r\n"),
    Step::Run(0, &[b"GETBIT", b"bits", b"1"], b":0\r\n"),
    Step::Run(2, &[b"SETBIT", b"bits", b"1", b"0"], b":0\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Run(2, &[b"SETBIT", b"bits", b"1", b"1"], b":0\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$4\r\nbits\r\n"),
    Step::Run(0, &[b"PFADD", b"hll", b"a"], b":1\r\n"),
    Step::Run(0, &[b"PFCOUNT", b"hll"], b":1\r\n"),
    Step::Run(2, &[b"PFADD", b"hll", b"b"], b":1\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$3\r\nhll\r\n"),
    Step::Run(0, &[b"SET", b"o", b"v"], b"+OK\r\n"),
    Step::Run(0, &[b"OBJECT", b"ENCODING", b"o"], b"$6\r\nembstr\r\n"),
    Step::Run(2, &[b"SET", b"o", b"12"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\no\r\n"),
    Step::Run(0, &[b"EXISTS", b"o"], b":1\r\n"),
    Step::Run(2, &[b"DEL", b"o"], b":1\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\no\r\n"),
    Step::Run(0, &[b"TYPE", b"o"], b"+none\r\n"),
    Step::Run(2, &[b"SET", b"o", b"1"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\no\r\n"),
    Step::Run(0, &[b"STRLEN", b"o"], b":1\r\n"),
    Step::Run(2, &[b"EXPIRE", b"o", b"100"], b":1\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\no\r\n"),
    Step::Run(0, &[b"TTL", b"o"], b":100\r\n"),
    Step::Run(2, &[b"PERSIST", b"o"], b":1\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\no\r\n"),
    Step::Run(0, &[b"SET", b"o2", b"2"], b"+OK\r\n"),
    Step::Run(0, &[b"GET", b"o2"], b"$1\r\n2\r\n"),
    Step::Run(2, &[b"RENAME", b"o2", b"o3"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$2\r\no2\r\n"),
    Step::Run(0, &[b"GET", b"o3"], b"$1\r\n2\r\n"),
    Step::Run(2, &[b"MOVE", b"o3", b"1"], b":1\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$2\r\no3\r\n"),
    // A change made by the tracking client itself is heard of too.
    Step::Run(0, &[b"GET", b"a"], b"$1\r\n2\r\n"),
    Step::Run(0, &[b"SET", b"a", b"9"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\na\r\n"),
    Step::Run(0, &[b"GET", b"a"], b"$1\r\n9\r\n"),
    Step::Run(0, &[b"DEL", b"a"], b":1\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\na\r\n"),
    // A flush: everyone hears a null, and nothing is tracked after.
    Step::Run(0, &[b"GET", b"a"], b"$-1\r\n"),
    Step::Run(0, &[b"GET", b"b"], b"$1\r\n2\r\n"),
    Step::Send(0, &[b"INFO"]),
    Step::Run(2, &[b"FLUSHALL"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n$-1\r\n"),
    Step::Send(0, &[b"INFO"]),
    Step::Run(2, &[b"SET", b"a", b"1"], b"+OK\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Run(0, &[b"GET", b"a"], b"$1\r\n1\r\n"),
    Step::Run(2, &[b"FLUSHDB"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n$-1\r\n"),
    Step::Run(0, &[b"GET", b"a"], b"$-1\r\n"),
    Step::Run(2, &[b"SELECT", b"1"], b"+OK\r\n"),
    Step::Run(2, &[b"FLUSHDB"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n$-1\r\n"),
    Step::Run(2, &[b"SELECT", b"0"], b"+OK\r\n"),
    Step::Run(2, &[b"SET", b"a", b"1"], b"+OK\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Run(0, &[b"GET", b"a"], b"$1\r\n1\r\n"),
    Step::Run(0, &[b"FLUSHALL", b"ASYNC"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n$-1\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"off"], b"+OK\r\n"),
    Step::Send(0, &[b"CLIENT", b"INFO"]),
    Step::Send(0, &[b"INFO"]),
    Step::Run(2, &[b"SET", b"a", b"2"], b"+OK\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Reset,
    // NOLOOP: what the client changes itself isn't sent.
    Step::Send(1, &[b"CLIENT", b"ID"]),
    Step::Run(1, &[b"SUBSCRIBE", b"__redis__:invalidate"], b"*3\r\n$9\r\nsubscribe\r\n$20\r\n__redis__:invalidate\r\n:1\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"REDIRECT", b"{id:1}", b"NOLOOP"], b"+OK\r\n"),
    Step::Run(0, &[b"MGET", b"k1", b"k2", b"k3"], b"*3\r\n$-1\r\n$-1\r\n$-1\r\n"),
    Step::Run(2, &[b"SET", b"k1", b"1"], b"+OK\r\n"),
    Step::Run(0, &[b"SET", b"k2", b"1"], b"+OK\r\n"),
    Step::Run(2, &[b"SET", b"k3", b"1"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$2\r\nk1\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$2\r\nk3\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Run(0, &[b"GET", b"k2"], b"$1\r\n1\r\n"),
    Step::Run(2, &[b"SET", b"k2", b"2"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$2\r\nk2\r\n"),
    Step::Send(0, &[b"CLIENT", b"TRACKINGINFO"]),
    Step::Reset,
    // OPTIN and OPTOUT.
    Step::Send(1, &[b"CLIENT", b"ID"]),
    Step::Run(1, &[b"SUBSCRIBE", b"__redis__:invalidate"], b"*3\r\n$9\r\nsubscribe\r\n$20\r\n__redis__:invalidate\r\n:1\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"OPTIN", b"REDIRECT", b"{id:1}"], b"+OK\r\n"),
    Step::Run(2, &[b"SET", b"k1", b"1"], b"+OK\r\n"),
    Step::Run(0, &[b"GET", b"k1"], b"$1\r\n1\r\n"),
    Step::Run(2, &[b"SET", b"k1", b"2"], b"+OK\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Run(0, &[b"CLIENT", b"CACHING", b"yes"], b"+OK\r\n"),
    Step::Run(0, &[b"GET", b"k1"], b"$1\r\n2\r\n"),
    Step::Run(2, &[b"SET", b"k1", b"3"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$2\r\nk1\r\n"),
    Step::Run(0, &[b"CLIENT", b"CACHING", b"yes"], b"+OK\r\n"),
    Step::Run(0, &[b"PING"], b"+PONG\r\n"),
    Step::Run(0, &[b"GET", b"k1"], b"$1\r\n3\r\n"),
    Step::Run(2, &[b"SET", b"k1", b"4"], b"+OK\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Run(0, &[b"CLIENT", b"CACHING", b"yes"], b"+OK\r\n"),
    Step::Send(0, &[b"CLIENT", b"ID"]),
    Step::Run(0, &[b"GET", b"k1"], b"$1\r\n4\r\n"),
    Step::Run(2, &[b"SET", b"k1", b"5"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$2\r\nk1\r\n"),
    Step::Run(0, &[b"CLIENT", b"CACHING", b"yes"], b"+OK\r\n"),
    Step::Run(0, &[b"MULTI"], b"+OK\r\n"),
    Step::Run(0, &[b"GET", b"k1"], b"+QUEUED\r\n"),
    Step::Run(0, &[b"GET", b"k2"], b"+QUEUED\r\n"),
    Step::Run(0, &[b"EXEC"], b"*2\r\n$1\r\n5\r\n$-1\r\n"),
    Step::Run(2, &[b"MSET", b"k1", b"6", b"k2", b"6"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$2\r\nk1\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$2\r\nk2\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"off"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"OPTOUT", b"REDIRECT", b"{id:1}"], b"+OK\r\n"),
    Step::Run(0, &[b"GET", b"k1"], b"$1\r\n6\r\n"),
    Step::Run(2, &[b"SET", b"k1", b"7"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$2\r\nk1\r\n"),
    Step::Run(0, &[b"CLIENT", b"CACHING", b"no"], b"+OK\r\n"),
    Step::Run(0, &[b"GET", b"k1"], b"$1\r\n7\r\n"),
    Step::Run(2, &[b"SET", b"k1", b"8"], b"+OK\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Run(0, &[b"GET", b"k1"], b"$1\r\n8\r\n"),
    Step::Run(2, &[b"SET", b"k1", b"9"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$2\r\nk1\r\n"),
    Step::Reset,
    // BCAST: every key, or those of some prefixes, a message a prefix.
    Step::Send(1, &[b"CLIENT", b"ID"]),
    Step::Run(1, &[b"SUBSCRIBE", b"__redis__:invalidate"], b"*3\r\n$9\r\nsubscribe\r\n$20\r\n__redis__:invalidate\r\n:1\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"BCAST", b"REDIRECT", b"{id:1}"], b"+OK\r\n"),
    Step::Send(0, &[b"CLIENT", b"INFO"]),
    Step::Send(0, &[b"INFO"]),
    Step::Run(2, &[b"MSET", b"a", b"1", b"b", b"2", b"c", b"3"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"),
    Step::Run(2, &[b"SET", b"d", b"1"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\nd\r\n"),
    Step::Run(2, &[b"SELECT", b"1"], b"+OK\r\n"),
    Step::Run(2, &[b"SET", b"e", b"1"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\ne\r\n"),
    Step::Run(2, &[b"SELECT", b"0"], b"+OK\r\n"),
    Step::Run(0, &[b"SET", b"mine", b"1"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$4\r\nmine\r\n"),
    Step::Run(0, &[b"GET", b"a"], b"$1\r\n1\r\n"),
    Step::Run(2, &[b"GET", b"a"], b"$1\r\n1\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"off"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"BCAST", b"REDIRECT", b"{id:1}", b"PREFIX", b"a:", b"PREFIX", b"b:"], b"+OK\r\n"),
    Step::Send(0, &[b"INFO"]),
    Step::Run(0, &[b"MULTI"], b"+OK\r\n"),
    Step::Run(0, &[b"INCR", b"a:1"], b"+QUEUED\r\n"),
    Step::Run(0, &[b"INCR", b"a:2"], b"+QUEUED\r\n"),
    Step::Run(0, &[b"INCR", b"b:1"], b"+QUEUED\r\n"),
    Step::Run(0, &[b"INCR", b"b:2"], b"+QUEUED\r\n"),
    Step::Run(0, &[b"INCR", b"c:1"], b"+QUEUED\r\n"),
    Step::Run(0, &[b"EXEC"], b"*5\r\n:1\r\n:1\r\n:1\r\n:1\r\n:1\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*2\r\n$3\r\na:1\r\n$3\r\na:2\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*2\r\n$3\r\nb:1\r\n$3\r\nb:2\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"BCAST", b"REDIRECT", b"{id:1}", b"PREFIX", b"c:"], b"+OK\r\n"),
    Step::Run(0, &[b"INCR", b"c:1234"], b":1\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$6\r\nc:1234\r\n"),
    Step::Send(0, &[b"CLIENT", b"TRACKINGINFO"]),
    Step::Send(0, &[b"INFO"]),
    Step::Run(2, &[b"MSET", b"a:9", b"1", b"b:9", b"1", b"c:9", b"1", b"d:9", b"1"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$3\r\na:9\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$3\r\nb:9\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$3\r\nc:9\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Run(3, &[b"CLIENT", b"TRACKING", b"on", b"BCAST", b"REDIRECT", b"{id:1}", b"PREFIX", b"a"], b"+OK\r\n"),
    Step::Send(0, &[b"INFO"]),
    Step::Run(2, &[b"SET", b"a:x", b"1"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$3\r\na:x\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$3\r\na:x\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Run(3, &[b"CLIENT", b"TRACKING", b"off"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"off"], b"+OK\r\n"),
    Step::Send(0, &[b"INFO"]),
    Step::Run(2, &[b"SET", b"a:x", b"2"], b"+OK\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Reset,
    // NOLOOP in BCAST mode.
    Step::Send(1, &[b"CLIENT", b"ID"]),
    Step::Run(1, &[b"SUBSCRIBE", b"__redis__:invalidate"], b"*3\r\n$9\r\nsubscribe\r\n$20\r\n__redis__:invalidate\r\n:1\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"BCAST", b"REDIRECT", b"{id:1}", b"NOLOOP"], b"+OK\r\n"),
    Step::Run(2, &[b"SET", b"otherkey1", b"1"], b"+OK\r\n"),
    Step::Run(0, &[b"SET", b"loopkey", b"1"], b"+OK\r\n"),
    Step::Run(2, &[b"SET", b"otherkey2", b"1"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$9\r\notherkey1\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$9\r\notherkey2\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Run(0, &[b"MSET", b"loopkey", b"2", b"mine", b"3"], b"+OK\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    // A key changed by the client and then by another is sent.
    Step::Run(0, &[b"MULTI"], b"+OK\r\n"),
    Step::Run(0, &[b"SET", b"k", b"1"], b"+QUEUED\r\n"),
    Step::Run(0, &[b"SET", b"k2", b"1"], b"+QUEUED\r\n"),
    Step::Run(0, &[b"EXEC"], b"*2\r\n+OK\r\n+OK\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Reset,
    // Expiry, in the background and on reading.
    Step::Send(1, &[b"CLIENT", b"ID"]),
    Step::Run(1, &[b"SUBSCRIBE", b"__redis__:invalidate"], b"*3\r\n$9\r\nsubscribe\r\n$20\r\n__redis__:invalidate\r\n:1\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"BCAST", b"REDIRECT", b"{id:1}", b"NOLOOP"], b"+OK\r\n"),
    Step::Run(0, &[b"SET", b"mykey", b"myval", b"px", b"1"], b"+OK\r\n"),
    Step::Run(0, &[b"SET", b"mykeyotherkey", b"myval"], b"+OK\r\n"),
    Step::Sleep(1000),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$5\r\nmykey\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Run(0, &[b"DEBUG", b"SET-ACTIVE-EXPIRE", b"0"], b"+OK\r\n"),
    Step::Run(0, &[b"MULTI"], b"+OK\r\n"),
    Step::Run(0, &[b"SET", b"mykey2", b"myval", b"px", b"1"], b"+QUEUED\r\n"),
    Step::Run(0, &[b"SET", b"mykeyotherkey2", b"myval"], b"+QUEUED\r\n"),
    Step::Run(0, &[b"DEBUG", b"SLEEP", b"0.1"], b"+QUEUED\r\n"),
    Step::Run(0, &[b"GET", b"mykey2"], b"+QUEUED\r\n"),
    Step::Run(0, &[b"EXEC"], b"*4\r\n+OK\r\n+OK\r\n+OK\r\n$5\r\nmyval\r\n"),
    Step::Run(0, &[b"GET", b"mykey2"], b"$-1\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$6\r\nmykey2\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"off"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"REDIRECT", b"{id:1}"], b"+OK\r\n"),
    Step::Run(0, &[b"SET", b"e1", b"v", b"px", b"100"], b"+OK\r\n"),
    Step::Run(0, &[b"GET", b"e1"], b"$1\r\nv\r\n"),
    Step::Sleep(200),
    Step::Run(0, &[b"GET", b"e1"], b"$-1\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$2\r\ne1\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Run(2, &[b"SET", b"e1", b"v"], b"+OK\r\n"),
    Step::Run(1, &[b"PING"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$2\r\ne1\r\n"),
    Step::Run(0, &[b"GET", b"e1"], b"$1\r\nv\r\n"),
    Step::Run(2, &[b"SET", b"e1", b"w"], b"+OK\r\n"),
    Step::Run(1, &[b"READ"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Run(0, &[b"SET", b"e2", b"v", b"px", b"100"], b"+OK\r\n"),
    Step::Run(0, &[b"GET", b"e2"], b"$1\r\nv\r\n"),
    Step::Sleep(200),
    Step::Run(2, &[b"GET", b"e2"], b"$-1\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$2\r\ne1\r\n"),
    Step::Run(0, &[b"DEBUG", b"SET-ACTIVE-EXPIRE", b"1"], b"+OK\r\n"),
    Step::Run(0, &[b"SET", b"e3", b"v", b"px", b"100"], b"+OK\r\n"),
    Step::Run(0, &[b"GET", b"e3"], b"$1\r\nv\r\n"),
    Step::Sleep(1000),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$2\r\ne2\r\n"),
    Step::Run(1, &[b"PING"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$2\r\ne3\r\n"),
    Step::Reset,
    // RESP3: invalidations come on the same connection, after the reply.
    Step::Send(0, &[b"HELLO", b"3"]),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on"], b"+OK\r\n"),
    Step::Run(2, &[b"SET", b"key1", b"1"], b"+OK\r\n"),
    Step::Run(0, &[b"GET", b"key1"], b"$1\r\n1\r\n"),
    Step::Run(2, &[b"SET", b"key1", b"2"], b"+OK\r\n"),
    Step::Run(0, &[b"READ"], b">2\r\n$10\r\ninvalidate\r\n*1\r\n$4\r\nkey1\r\n"),
    Step::Run(0, &[b"PING"], b"+PONG\r\n"),
    Step::Run(0, &[b"HSET", b"myhash", b"f", b"1"], b":1\r\n"),
    Step::Run(0, &[b"HGET", b"myhash", b"f"], b"$1\r\n1\r\n"),
    Step::Run(0, &[b"HDEL", b"myhash", b"f"], b":1\r\n"),
    Step::Run(0, &[b"READ"], b">2\r\n$10\r\ninvalidate\r\n*1\r\n$6\r\nmyhash\r\n"),
    Step::Run(0, &[b"MSET", b"x", b"1", b"y", b"2"], b"+OK\r\n"),
    Step::Run(0, &[b"GET", b"x"], b"$1\r\n1\r\n"),
    Step::Run(0, &[b"GET", b"y"], b"$1\r\n2\r\n"),
    Step::Run(0, &[b"MULTI"], b"+OK\r\n"),
    Step::Run(0, &[b"INCR", b"x"], b"+QUEUED\r\n"),
    Step::Run(0, &[b"GET", b"y"], b"+QUEUED\r\n"),
    Step::Run(0, &[b"EXEC"], b"*2\r\n:2\r\n$1\r\n2\r\n"),
    Step::Run(0, &[b"READ"], b">2\r\n$10\r\ninvalidate\r\n*1\r\n$1\r\nx\r\n"),
    Step::Run(0, &[b"PING"], b"+PONG\r\n"),
    Step::Run(0, &[b"MGET", b"x", b"y"], b"*2\r\n$1\r\n2\r\n$1\r\n2\r\n"),
    Step::Run(0, &[b"DEBUG", b"SET-ACTIVE-EXPIRE", b"0"], b"+OK\r\n"),
    Step::Run(0, &[b"PEXPIRE", b"y", b"100"], b":1\r\n"),
    Step::Sleep(150),
    Step::Run(0, &[b"MGET", b"x", b"y"], b">2\r\n$10\r\ninvalidate\r\n*1\r\n$1\r\ny\r\n"),
    Step::Run(0, &[b"READ"], b"*2\r\n$1\r\n2\r\n_\r\n"),
    Step::Run(0, &[b"DEBUG", b"SET-ACTIVE-EXPIRE", b"1"], b"+OK\r\n"),
    Step::Run(0, &[b"SET", b"a", b"1"], b"+OK\r\n"),
    Step::Run(0, &[b"GET", b"a"], b"$1\r\n1\r\n"),
    Step::Run(0, &[b"MULTI"], b"+OK\r\n"),
    Step::Run(0, &[b"FLUSHDB"], b"+QUEUED\r\n"),
    Step::Run(0, &[b"EXEC"], b"*1\r\n+OK\r\n"),
    Step::Run(0, &[b"READ"], b">2\r\n$10\r\ninvalidate\r\n_\r\n"),
    Step::Run(0, &[b"GET", b"a"], b"_\r\n"),
    Step::Run(0, &[b"FLUSHALL"], b"+OK\r\n"),
    Step::Run(0, &[b"READ"], b">2\r\n$10\r\ninvalidate\r\n_\r\n"),
    Step::Run(0, &[b"PING"], b"+PONG\r\n"),
    // Read again, heard of again.
    Step::Run(0, &[b"SET", b"p1", b"1"], b"+OK\r\n"),
    Step::Run(0, &[b"SET", b"p2", b"1"], b"+OK\r\n"),
    Step::Run(0, &[b"MGET", b"p1", b"p2"], b"*2\r\n$1\r\n1\r\n$1\r\n1\r\n"),
    Step::Run(2, &[b"SET", b"p1", b"2"], b"+OK\r\n"),
    Step::Run(0, &[b"READ"], b">2\r\n$10\r\ninvalidate\r\n*1\r\n$2\r\np1\r\n"),
    Step::Run(0, &[b"GET", b"p1"], b"$1\r\n2\r\n"),
    Step::Run(2, &[b"SET", b"p1", b"3"], b"+OK\r\n"),
    Step::Run(0, &[b"READ"], b">2\r\n$10\r\ninvalidate\r\n*1\r\n$2\r\np1\r\n"),
    Step::Run(0, &[b"PING"], b"+PONG\r\n"),
    // After switching to BCAST mode, nothing for keys read before.
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"off"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on"], b"+OK\r\n"),
    Step::Run(2, &[b"SET", b"key1", b"1"], b"+OK\r\n"),
    Step::Run(0, &[b"GET", b"key1"], b"$1\r\n1\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"off"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"BCAST"], b"+OK\r\n"),
    Step::Run(2, &[b"INCR", b"key1"], b":2\r\n"),
    Step::Run(0, &[b"READ"], b">2\r\n$10\r\ninvalidate\r\n*1\r\n$4\r\nkey1\r\n"),
    Step::Run(0, &[b"PING"], b"+PONG\r\n"),
    // RESP2 again: nothing to hear without a redirection.
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"off"], b"+OK\r\n"),
    Step::Send(0, &[b"HELLO", b"2"]),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on"], b"+OK\r\n"),
    Step::Run(0, &[b"GET", b"key1"], b"$1\r\n2\r\n"),
    Step::Run(2, &[b"SET", b"key1", b"3"], b"+OK\r\n"),
    Step::Run(0, &[b"PING"], b"+PONG\r\n"),
    // Back to RESP3: what was read meanwhile is heard of.
    Step::Send(0, &[b"HELLO", b"3"]),
    Step::Run(2, &[b"SET", b"key1", b"4"], b"+OK\r\n"),
    Step::Run(0, &[b"GET", b"key1"], b"$1\r\n4\r\n"),
    Step::Run(2, &[b"SET", b"key1", b"5"], b"+OK\r\n"),
    Step::Run(0, &[b"READ"], b">2\r\n$10\r\ninvalidate\r\n*1\r\n$4\r\nkey1\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"off"], b"+OK\r\n"),
    Step::Reset,
    // Redirected to RESP3 connections: one subscribed, one not.
    Step::Send(1, &[b"HELLO", b"3"]),
    Step::Send(1, &[b"CLIENT", b"ID"]),
    Step::Run(1, &[b"SUBSCRIBE", b"__redis__:invalidate"], b">3\r\n$9\r\nsubscribe\r\n$20\r\n__redis__:invalidate\r\n:1\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"REDIRECT", b"{id:1}"], b"+OK\r\n"),
    Step::Run(2, &[b"SET", b"key1", b"1"], b"+OK\r\n"),
    Step::Run(0, &[b"GET", b"key1"], b"$1\r\n1\r\n"),
    Step::Run(2, &[b"INCR", b"key1"], b":2\r\n"),
    Step::Run(1, &[b"READ"], b">2\r\n$10\r\ninvalidate\r\n*1\r\n$4\r\nkey1\r\n"),
    Step::Send(4, &[b"HELLO", b"3"]),
    Step::Send(4, &[b"CLIENT", b"ID"]),
    Step::Run(3, &[b"CLIENT", b"TRACKING", b"on", b"REDIRECT", b"{id:4}"], b"+OK\r\n"),
    Step::Run(4, &[b"PING"], b"+PONG\r\n"),
    Step::Run(3, &[b"GET", b"key1"], b"$1\r\n2\r\n"),
    Step::Run(2, &[b"SET", b"key1", b"2"], b"+OK\r\n"),
    Step::Run(4, &[b"READ"], b">2\r\n$10\r\ninvalidate\r\n*1\r\n$4\r\nkey1\r\n"),
    Step::Run(4, &[b"SEND", b"CLIENT", b"REPLY", b"OFF"], b"<sent>"),
    Step::Run(3, &[b"GET", b"key1"], b"$1\r\n2\r\n"),
    Step::Run(2, &[b"SET", b"key1", b"3"], b"+OK\r\n"),
    Step::Run(4, &[b"READ"], b">2\r\n$10\r\ninvalidate\r\n*1\r\n$4\r\nkey1\r\n"),
    Step::Run(4, &[b"CLIENT", b"REPLY", b"ON"], b"+OK\r\n"),
    Step::Run(4, &[b"PING"], b"+PONG\r\n"),
    // The redirected-to connection goes: RESP3 clients hear it's broken.
    Step::Run(1, &[b"UNSUBSCRIBE", b"__redis__:invalidate"], b">3\r\n$11\r\nunsubscribe\r\n$20\r\n__redis__:invalidate\r\n:0\r\n"),
    Step::Run(1, &[b"QUIT"], b"+OK\r\n"),
    Step::Send(0, &[b"HELLO", b"3"]),
    Step::Run(0, &[b"GET", b"key1"], b"$1\r\n3\r\n"),
    Step::Run(2, &[b"SET", b"key1", b"4"], b"+OK\r\n"),
    Step::Send(0, &[b"READ"]),
    Step::Send(0, &[b"CLIENT", b"TRACKINGINFO"]),
    Step::Send(0, &[b"CLIENT", b"INFO"]),
    Step::Run(0, &[b"GET", b"key1"], b"$1\r\n4\r\n"),
    Step::Run(2, &[b"SET", b"key1", b"5"], b"+OK\r\n"),
    Step::Send(0, &[b"READ"]),
    Step::Run(0, &[b"PING"], b"+PONG\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKINGINFO"], b"%3\r\n$5\r\nflags\r\n~1\r\n$2\r\non\r\n$8\r\nredirect\r\n:0\r\n$8\r\nprefixes\r\n*0\r\n"),
    Step::Reset,
    // Different clients redirect to the same connection.
    Step::Send(1, &[b"CLIENT", b"ID"]),
    Step::Run(1, &[b"SUBSCRIBE", b"__redis__:invalidate"], b"*3\r\n$9\r\nsubscribe\r\n$20\r\n__redis__:invalidate\r\n:1\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"REDIRECT", b"{id:1}"], b"+OK\r\n"),
    Step::Run(3, &[b"CLIENT", b"TRACKING", b"on", b"REDIRECT", b"{id:1}"], b"+OK\r\n"),
    Step::Run(2, &[b"MSET", b"key1", b"1", b"key2", b"1"], b"+OK\r\n"),
    Step::Run(0, &[b"GET", b"key1"], b"$1\r\n1\r\n"),
    Step::Run(3, &[b"GET", b"key2"], b"$1\r\n1\r\n"),
    Step::Run(2, &[b"INCR", b"key1"], b":2\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$4\r\nkey1\r\n"),
    Step::Run(2, &[b"INCR", b"key2"], b":2\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$4\r\nkey2\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    // And both hear of a key both read, once each.
    Step::Run(0, &[b"GET", b"key1"], b"$1\r\n2\r\n"),
    Step::Run(3, &[b"GET", b"key1"], b"$1\r\n2\r\n"),
    Step::Run(2, &[b"INCR", b"key1"], b":3\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$4\r\nkey1\r\n"),
    Step::Run(1, &[b"READ"], b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$4\r\nkey1\r\n"),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Send(0, &[b"INFO"]),
    Step::Reset,
    // Scripts: only what they read is tracked.
    Step::Send(0, &[b"HELLO", b"3"]),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on"], b"+OK\r\n"),
    Step::Run(2, &[b"MSET", b"key2", b"1", b"key1", b"1"], b"+OK\r\n"),
    Step::Run(0, &[b"EVAL", b"redis.call('set', 'key3', 'bar')", b"2", b"key1", b"key2"], b"_\r\n"),
    Step::Run(2, &[b"MSET", b"key2", b"2", b"key1", b"2"], b"+OK\r\n"),
    Step::Run(0, &[b"PING"], b"+PONG\r\n"),
    Step::Run(0, &[b"EVAL", b"redis.call('get', 'key2')", b"2", b"key1", b"key2"], b"_\r\n"),
    Step::Run(2, &[b"MSET", b"key2", b"2", b"key3", b"2"], b"+OK\r\n"),
    Step::Run(0, &[b"READ"], b">2\r\n$10\r\ninvalidate\r\n*1\r\n$4\r\nkey2\r\n"),
    Step::Run(0, &[b"PING"], b"+PONG\r\n"),
    Step::Run(0, &[b"EVAL_RO", b"redis.call('ping')", b"2", b"key1", b"key2"], b"_\r\n"),
    Step::Run(2, &[b"MSET", b"key2", b"2", b"key1", b"2"], b"+OK\r\n"),
    Step::Run(0, &[b"PING"], b"+PONG\r\n"),
    Step::Run(0, &[b"EVAL_RO", b"redis.call('get', 'key2')", b"2", b"key1", b"key2"], b"_\r\n"),
    Step::Run(2, &[b"MSET", b"key2", b"2", b"key3", b"2"], b"+OK\r\n"),
    Step::Run(0, &[b"READ"], b">2\r\n$10\r\ninvalidate\r\n*1\r\n$4\r\nkey2\r\n"),
    Step::Run(0, &[b"PING"], b"+PONG\r\n"),
    // A script that reads and then writes the key.
    Step::Run(0, &[b"EVAL", b"redis.call('get', KEYS[1]) return redis.call('set', KEYS[1], 'x')", b"1", b"key4"], b"+OK\r\n"),
    Step::Run(0, &[b"READ"], b">2\r\n$10\r\ninvalidate\r\n*1\r\n$4\r\nkey4\r\n"),
    Step::Run(0, &[b"PING"], b"+PONG\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"off"], b"+OK\r\n"),
    Step::Run(3, &[b"CLIENT", b"TRACKING", b"on", b"NOLOOP"], b"+OK\r\n"),
    Step::Send(3, &[b"HELLO", b"3"]),
    Step::Run(3, &[b"SET", b"key1", b"1"], b"+OK\r\n"),
    Step::Run(3, &[b"GET", b"key1"], b"$1\r\n1\r\n"),
    Step::Run(3, &[b"EVAL", b"return redis.call('set', 'key1', '2')", b"1", b"key1"], b"+OK\r\n"),
    Step::Run(3, &[b"GET", b"key1"], b"$1\r\n2\r\n"),
    Step::Run(3, &[b"CLIENT", b"TRACKING", b"off"], b"+OK\r\n"),
    Step::Reset,
    // The table's size limit.
    Step::Send(1, &[b"CLIENT", b"ID"]),
    Step::Run(1, &[b"SUBSCRIBE", b"__redis__:invalidate"], b"*3\r\n$9\r\nsubscribe\r\n$20\r\n__redis__:invalidate\r\n:1\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"REDIRECT", b"{id:1}", b"NOLOOP"], b"+OK\r\n"),
    Step::Run(0, &[b"MSET", b"key1", b"1", b"key2", b"2"], b"+OK\r\n"),
    Step::Run(0, &[b"MGET", b"key1", b"key2"], b"*2\r\n$1\r\n1\r\n$1\r\n2\r\n"),
    Step::Send(0, &[b"INFO"]),
    Step::Run(0, &[b"CONFIG", b"SET", b"tracking-table-max-keys", b"1"], b"+OK\r\n"),
    Step::Send(1, &[b"READ"]),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Send(0, &[b"INFO"]),
    Step::Run(0, &[b"MGET", b"k3", b"k4", b"k5"], b"*3\r\n$-1\r\n$-1\r\n$-1\r\n"),
    Step::Send(1, &[b"READ"]),
    Step::Send(1, &[b"READ"]),
    Step::Send(1, &[b"READ"]),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Send(0, &[b"INFO"]),
    Step::Run(0, &[b"CONFIG", b"SET", b"tracking-table-max-keys", b"0"], b"+OK\r\n"),
    Step::Run(0, &[b"MGET", b"k6", b"k7", b"k8"], b"*3\r\n$-1\r\n$-1\r\n$-1\r\n"),
    Step::Send(0, &[b"INFO"]),
    Step::Run(1, &[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    Step::Run(0, &[b"CONFIG", b"SET", b"tracking-table-max-keys", b"1000000"], b"+OK\r\n"),
    Step::Reset,
    // RESET ends tracking.
    Step::Send(0, &[b"HELLO", b"3"]),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"BCAST", b"PREFIX", b"p"], b"+OK\r\n"),
    Step::Send(0, &[b"INFO"]),
    Step::Run(0, &[b"RESET"], b"+RESET\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKINGINFO"], b"*6\r\n$5\r\nflags\r\n*1\r\n$3\r\noff\r\n$8\r\nredirect\r\n:-1\r\n$8\r\nprefixes\r\n*0\r\n"),
    Step::Send(0, &[b"CLIENT", b"INFO"]),
    Step::Send(0, &[b"INFO"]),
    Step::Run(2, &[b"SET", b"p1", b"1"], b"+OK\r\n"),
    Step::Run(0, &[b"PING"], b"+PONG\r\n"),
    Step::Reset,
    // A blocked command that's served changes a key its client tracks.
    Step::Run(2, &[b"RPUSH", b"list2", b"a"], b":1\r\n"),
    Step::Send(3, &[b"HELLO", b"3"]),
    Step::Run(3, &[b"CLIENT", b"TRACKING", b"on"], b"+OK\r\n"),
    Step::Run(3, &[b"LRANGE", b"list2", b"0", b"-1"], b"*1\r\n$1\r\na\r\n"),
    Step::Run(3, &[b"SEND", b"BLMOVE", b"list1", b"list2", b"left", b"left", b"0"], b"<sent>"),
    Step::Sleep(100),
    Step::Run(2, &[b"RPUSH", b"list1", b"foo"], b":1\r\n"),
    Step::Run(3, &[b"READ"], b"$3\r\nfoo\r\n"),
    Step::Run(3, &[b"READ"], b">2\r\n$10\r\ninvalidate\r\n*1\r\n$5\r\nlist2\r\n"),
    Step::Run(3, &[b"PING"], b"+PONG\r\n"),
    Step::Reset,
    // Coverage, as Redis' tests have it.
    Step::Send(1, &[b"CLIENT", b"ID"]),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"on", b"OPTIN", b"REDIRECT", b"{id:1}"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"CACHING", b"yes"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKING", b"off"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"REPLY", b"on"], b"+OK\r\n"),
    Step::Run(0, &[b"CLIENT", b"TRACKINGINFO"], b"*6\r\n$5\r\nflags\r\n*1\r\n$3\r\noff\r\n$8\r\nredirect\r\n:-1\r\n$8\r\nprefixes\r\n*0\r\n"),
    Step::Run(0, &[b"CLIENT", b"GETREDIR"], b":-1\r\n"),
// END STEPS
];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tracking_works_as_in_redis() {
    for mode in MODES {
        replay_steps_expiring(
            STEPS,
            &[&[b"CONFIG", b"SET", b"tracking-table-max-keys", b"1000000"]],
            mode,
        )
        .await;
    }
}

/// A RESP3 client that caches each key it reads until it hears it changed.
struct Caching {
    client: Client,
    cache: std::collections::HashMap<String, String>,
}

impl Caching {
    /// Reads replies until a non-push one, dropping what pushes say changed.
    async fn reply(&mut self) -> String {
        loop {
            let reply = self.client.reply().await;
            if let Some(rest) = reply.strip_prefix(">2\r\n$10\r\ninvalidate\r\n") {
                if rest.starts_with('_') {
                    self.cache.clear();
                } else {
                    // `*N\r\n$len\r\nkey\r\n...`: keys every other line from the third.
                    for key in rest.split("\r\n").skip(2).step_by(2) {
                        self.cache.remove(key);
                    }
                }
                continue;
            }
            return reply;
        }
    }

    async fn get(&mut self, key: &str) -> String {
        if let Some(value) = self.cache.get(key) {
            return value.clone();
        }
        self.client.send(&["GET", key]).await;
        let value = self.reply().await;
        self.cache.insert(key.to_string(), value.clone());
        value
    }
}

/// However reads and writes on other threads fall, once the writers stop and
/// the client has heard all there is, what it has cached is what's there.
async fn never_stale(mode: Mode) {
    let (addr, _ctx) = start(mode).await;
    let keys = ["k0", "k1", "k2"];
    let mut reader = Caching {
        client: Client::connect(addr).await,
        cache: Default::default(),
    };
    reader.client.call(&["HELLO", "3"]).await;
    assert_eq!(
        reader.client.call(&["CLIENT", "TRACKING", "on"]).await,
        "+OK\r\n"
    );
    let writers: Vec<_> = (0..2)
        .map(|w| {
            tokio::spawn(async move {
                let mut writer = Client::connect(addr).await;
                for i in 0..2000 {
                    let key = ["k0", "k1", "k2"][(i + w) % 3];
                    if i % 7 == 0 {
                        writer.call(&["DEL", key]).await;
                    } else if i % 11 == 0 {
                        writer.call(&["SET", key, "x", "PX", "1"]).await;
                    } else {
                        writer.call(&["INCR", key]).await;
                    }
                }
            })
        })
        .collect();
    let mut reads = 0;
    while !writers.iter().all(|w| w.is_finished()) {
        for key in keys {
            reader.get(key).await;
            reads += 1;
        }
        // Hear what's come.
        reader.client.send(&["PING"]).await;
        assert_eq!(reader.reply().await, "+PONG\r\n");
    }
    for writer in writers {
        writer.await.unwrap();
    }
    // Keys set to expire have, and are gone once read, which is heard of.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let mut check = Client::connect(addr).await;
    check.call(&["HELLO", "3"]).await;
    let mut there = std::collections::HashMap::new();
    for key in keys {
        there.insert(key, check.call(&["GET", key]).await);
    }
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    reader.client.send(&["PING"]).await;
    assert_eq!(reader.reply().await, "+PONG\r\n");
    for key in keys {
        if let Some(cached) = reader.cache.get(key).cloned() {
            let now = there[key].clone();
            assert_eq!(
                cached, now,
                "{mode:?}: {key} cached stale after {reads} reads"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cached_value_is_never_stale() {
    for mode in MODES {
        never_stale(mode).await;
    }
}
