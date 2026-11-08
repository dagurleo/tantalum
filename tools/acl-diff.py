#!/usr/bin/env python3
"""Runs the same ACL scenarios on Redis and on Tantalum, over several
connections each, and shows where their replies differ, byte for byte.

    tools/acl-diff.py [--redis 6390] [--tantalum 6391] [--rust]

Start both on spare ports with no password, Redis with an aclfile so that ACL
SAVE and LOAD work there too:

    touch /tmp/r.acl /tmp/t.acl
    redis-server --port 6390 --save '' --appendonly no --aclfile /tmp/r.acl &
    target/release/tantalum --port 6391 --aclfile /tmp/t.acl &

Each scenario line is a command, split like a shell line (\\xNN escapes give
bytes), optionally after:

    @N        run it on connection N (0 by default; each is opened when first used)
    ?sorted   compare an array's elements in any order (ACL CAT's, whose order is
              Redis' hash table's)
    ?len      compare only the reply's kind and length (ACL GENPASS)
    ?log      ACL LOG, without the times, and with only the client-info fields that
              don't vary (cmd, user, name, db, multi, resp)
    ?client   CLIENT INFO or LIST, with only those fields too
    ?info     INFO, with only its acl_access_denied lines
    ?ints     every integer masked (client ids, which Redis' own clients move on)

and lines `!reset` (users, log and data back to the start, connections closed)
and `!sleep MS`. A connection the server closed reads as <closed>. With --rust
it prints the scenario and Redis' replies as test steps for tests/acl.rs
(the lines compared in part are run, unchecked). Commands Redis has and Tantalum doesn't (FCALL)
ACL DRYRUN judges by the command rules alone, not knowing their keys, so none
of those with keys are here.
"""

import argparse
import re
import shlex
import socket
import sys
import time

SCENARIOS = r"""
# Who's who.
ACL WHOAMI
ACL USERS
ACL LIST
?sorted ACL CAT
?sorted ACL CAT read
?sorted ACL CAT dangerous
?sorted ACL CAT scripting
ACL CAT NON_EXISTS
ACL CAT a b
ACL HELP
ACL NOPE
ACL GETUSER
ACL WHOAMI extra
ACL GETUSER default
ACL GETUSER nobody
@1 ?ints HELLO 3
@1 ACL GETUSER default
@1 ACL GETUSER nobody

# Users and their rules.
ACL SETUSER alice
ACL LIST
ACL GETUSER alice
ACL SETUSER alice on >p1 >p2 ~cached:* %R~read:* %W~write:* &news.* +get +set -@all +@hash
ACL GETUSER alice
@1 ACL GETUSER alice
ACL SETUSER alice <p1 resetkeys allkeys
ACL GETUSER alice
ACL SETUSER alice (+get ~x*) (+set %W~y* &chan)
ACL GETUSER alice
@1 ACL GETUSER alice
ACL LIST
ACL SETUSER alice clearselectors
ACL LIST
ACL SETUSER alice reset
ACL LIST
ACL SETUSER "bad name"
ACL SETUSER bad +nosuch
ACL SETUSER bad +@nosuch
ACL SETUSER bad ~a allkeys ~b
ACL SETUSER bad allchannels &x
ACL SETUSER bad <nope
ACL SETUSER bad #abc
ACL SETUSER bad #2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b
ACL SETUSER bad !2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b
ACL SETUSER bad (+get ~x
ACL SETUSER bad ( +get ~x )
ACL SETUSER bad "(+get" "~x)"
ACL SETUSER bad (+nosuch)
ACL SETUSER bad +config|get|x
ACL SETUSER bad +get|
ACL SETUSER bad -select|0
ACL SETUSER bad +select|0 +debug|object
ACL SETUSER bad %X~a
ACL SETUSER bad %R
ACL SETUSER bad skip-sanitize-payload
ACL LIST
ACL SETUSER cmds +@all -@hash -@slow +hget
ACL GETUSER cmds
ACL SETUSER cmds -@hash
ACL SETUSER cmds +@hash
ACL GETUSER cmds
ACL SETUSER cmds -@all +client|list +client|list +config|get +config +acl|list -acl
ACL GETUSER cmds
ACL SETUSER cmds -@all +config +config|get -config|set +config
ACL GETUSER cmds
ACL SETUSER cmds +@all -client|kill +client
ACL GETUSER cmds
ACL USERS
ACL DELUSER default
ACL DELUSER nobody
ACL DELUSER bad cmds nobody
ACL USERS

# Logging in.
AUTH whatever
AUTH a b c
AUTH default anything
ACL SETUSER bob on >secret ~* &* +@all
@1 AUTH bob wrong
@1 AUTH bob secret
@1 ACL WHOAMI
@1 ?client CLIENT INFO
@2 AUTH nobody x
@2 HELLO 3 AUTH bob wrong
@2 ?ints HELLO 2 AUTH bob secret SETNAME bobconn
@2 ?client CLIENT INFO
@2 HELLO 2 AUTH bob secret SETNAME "bad name"
@2 RESET
@2 ACL WHOAMI
ACL SETUSER bob off
@3 AUTH bob secret
ACL SETUSER bob on nopass
@3 AUTH bob anything
@3 ACL WHOAMI
?info INFO stats

# What a user may run.
!reset
ACL SETUSER u on nopass -@all +get +set +acl|whoami ~app:* %R~ro:* %W~wo:*
@1 AUTH u x
@1 GET app:1
@1 GET other
@1 GET ro:1
@1 GET wo:1
@1 SET wo:1 v
@1 SET ro:1 v
@1 SET wo:1 v GET
@1 PING
@1 DEL app:1
@1 ACL WHOAMI
@1 ACL USERS
@1 AUTH default x
@1 GET nothere x
@1 FOO
@1 CONFIG GET
ACL SETUSER u +mset +copy +exists +client|id +select|0
@1 MSET app:1 a wo:2 b ro:3 c
@1 COPY ro:1 wo:9
@1 COPY wo:1 ro:9
@1 EXISTS ro:1 wo:1 app:1
@1 EXISTS zzz
@1 ?ints CLIENT ID
@1 ?client CLIENT INFO
@1 SELECT 0
@1 SELECT 1
@1 CLIENT KILL ID 999
@1 CLIENT NOPE
ACL SETUSER u +@all -client|kill
@1 CLIENT KILL ID 999
@1 OBJECT ENCODING app:1
@1 OBJECT ENCODING other
@1 PUBLISH chan m
ACL SETUSER u &news.* &lit
@1 PUBLISH news.1 m
@1 PUBLISH lit m
@1 PUBLISH other m
ACL SETUSER u allchannels
@1 PUBLISH other m
ACL SETUSER u resetchannels
@1 PUBLISH other m

# Selectors.
!reset
ACL SETUSER s on nopass +get ~a* (+set ~b*) (+del %R~c*)
@1 AUTH s x
@1 GET a1
@1 SET b1 v
@1 GET b1
@1 SET a1 v
@1 DEL c1
@1 DEL a1
@1 HGET a1 f
@1 DRYRUN

# The log.
!reset
ACL SETUSER l on >pw -@all +get ~k*
@1 AUTH l wrong
@1 AUTH l pw
@1 GET k1
@1 GET other
@1 GET other
@1 SET k1 v
@1 PUBLISH c m
?log ACL LOG
?log ACL LOG 1
ACL LOG 0
ACL LOG -5
ACL LOG nope
ACL LOG a b
ACL LOG RESET
ACL LOG
?info INFO stats

# Transactions.
!reset
ACL SETUSER t on nopass +@all -set ~*
@1 AUTH t x
@1 MULTI
@1 GET k
@1 SET k v
@1 EXEC
@1 MULTI
@1 GET k
@1 INCR k
ACL SETUSER t -incr
@1 EXEC
?log ACL LOG

# Scripts.
!reset
ACL SETUSER sc on nopass +@all -incr ~k*
@1 AUTH sc x
@1 EVAL "return redis.call('incr','k')" 0
@1 EVAL "return redis.pcall('incr','k')" 0
@1 EVAL "return redis.call('get','other')" 0
@1 EVAL "return redis.call('get','k')" 0
@1 EVAL "return redis.acl_check_cmd('incr','k')" 0
@1 EVAL "return redis.acl_check_cmd('get','k')" 0
@1 EVAL "return redis.acl_check_cmd('get','other')" 0
@1 EVAL "return redis.acl_check_cmd('nosuch')" 0
@1 EVAL "return 1" 1 other
@1 EVAL "return 1" 1 k1
?log ACL LOG

# Dry runs.
!reset
ACL SETUSER d on nopass +get ~k* &c
ACL DRYRUN d GET k
ACL DRYRUN d GET other
ACL DRYRUN d SET k v
ACL DRYRUN d PUBLISH x m
ACL DRYRUN d GET
ACL DRYRUN d NOSUCH x
ACL DRYRUN nobody GET k
ACL DRYRUN d CONFIG GET x
ACL DRYRUN d CONFIG
ACL DRYRUN d CONFIG NOPE
ACL DRYRUN default SET k v
ACL SETUSER command-test +@all %R~read* %W~write* %RW~rw*
ACL DRYRUN command-test SORT read STORE write
ACL DRYRUN command-test SORT read STORE read
ACL DRYRUN command-test SORT write STORE write
ACL DRYRUN command-test EVAL "" 1 rw1
ACL DRYRUN command-test EVAL "" 1 read
ACL DRYRUN command-test EVAL_RO "" 1 rw1
ACL DRYRUN command-test EVAL_RO "" 1 read
ACL DRYRUN command-test EVAL "" 0 read
ACL DRYRUN command-test EVAL "" -1 read
ACL DRYRUN command-test EVAL "" 3 rw rw
ACL DRYRUN command-test EVAL "" 3 rw read
ACL DRYRUN command-test GEORADIUS read longitude latitude radius M STOREDIST write
ACL DRYRUN command-test GEORADIUS read longitude latitude radius M
ACL DRYRUN command-test GEORADIUS read1 longitude latitude radius M STOREDIST read2
ACL DRYRUN command-test GEORADIUS write1 longitude latitude radius M STOREDIST write2
ACL DRYRUN command-test GEORADIUS read longitude latitude radius M STORE write
ACL DRYRUN command-test XREAD STREAMS read1 read2 0 0
ACL DRYRUN command-test XREAD STREAMS read1 write2 0 0
ACL DRYRUN command-test XREADGROUP GROUP g c STREAMS rw1 0
ACL DRYRUN command-test XREADGROUP GROUP g c STREAMS read1 0
ACL DRYRUN command-test LMPOP 2 read1 rw1 LEFT
ACL DRYRUN command-test LMPOP 2 rw1 rw2 LEFT
ACL DRYRUN command-test ZUNIONSTORE write1 2 read1 read2
ACL DRYRUN command-test ZUNIONSTORE read1 2 read1 read2
ACL DRYRUN command-test BITFIELD read1 GET u8 0
ACL DRYRUN command-test BITFIELD read1 SET u8 0 1
ACL DRYRUN command-test BITFIELD rw1 SET u8 0 1
ACL DRYRUN command-test SET write1 v
ACL DRYRUN command-test SET write1 v GET
ACL DRYRUN command-test OBJECT ENCODING read1
ACL DRYRUN command-test OBJECT ENCODING write1
ACL DRYRUN command-test XINFO STREAM write1
ACL DRYRUN command-test MEMORY USAGE write1
ACL DRYRUN command-test FUNCTION LIST
ACL SETUSER command-test -@scripting
ACL DRYRUN command-test FCALL_RO f 0
ACL DRYRUN command-test EVAL "" 0

# Deleting users closes their connections.
!reset
ACL SETUSER gone on nopass +@all ~* &*
@1 AUTH gone x
@2 AUTH gone x
@2 PING
ACL DELUSER gone
!sleep 100
@1 PING
@2 PING
ACL SETUSER selfdel on nopass +@all ~* &*
@3 AUTH selfdel x
@3 ACL DELUSER selfdel
@3 PING

# Losing a channel closes a subscriber.
!reset
ACL SETUSER sub on nopass +@all ~* &news.* &sport
@1 AUTH sub x
@1 SUBSCRIBE news.1 sport
@1 READ
ACL SETUSER sub &extra
PUBLISH sport hello
@1 READ
ACL SETUSER sub resetchannels &sport
!sleep 100
@1 PING

# SORT's patterns read other keys.
!reset
ACL SETUSER so on nopass +@all ~list*
RPUSH list 3 1 2
@1 AUTH so x
@1 SORT list
@1 SORT list BY nosort
@1 SORT list BY w_*
@1 SORT list GET #
ACL SETUSER so %R~*
@1 SORT list BY w_* GET #

# Passwords.
!reset
ACL GENPASS 4097
ACL GENPASS 0
ACL GENPASS nope
?len ACL GENPASS
?len ACL GENPASS 1
?len ACL GENPASS 5
?len ACL GENPASS 4096
CONFIG SET requirepass hunter2
ACL GETUSER default
@1 PING
@1 AUTH hunter2
@1 PING
CONFIG SET requirepass ""
ACL GETUSER default
CLIENT KILL USER nobody
ACL SETUSER k on nopass +@all
@1 AUTH k x
CLIENT KILL USER k
!sleep 100
@1 PING

# The default user, restricted.
!reset
ACL SETUSER admin on >adminpw ~* &* +@all
ACL SETUSER default -flushall
FLUSHALL
@1 FLUSHALL
@1 GET k
@1 AUTH admin adminpw
@1 FLUSHALL
ACL SETUSER default off
@2 PING
@2 AUTH admin adminpw
@2 ACL SETUSER default on +flushall
@2 PING
"""


def read_reply(f):
    line = f.readline()
    if not line:
        raise EOFError
    kind, rest = line[:1], line[1:-2]
    if kind in b"$=!":
        n = int(rest)
        return line + (f.read(n + 2) if n >= 0 else b"")
    if kind in b"*~>":
        n = int(rest)
        return line + b"".join(read_reply(f) for _ in range(max(n, 0)))
    if kind in b"%|":
        return line + b"".join(read_reply(f) for _ in range(2 * int(rest)))
    return line


def encode(args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


def parse_args(line):
    return [a.encode("latin-1").decode("unicode_escape").encode("latin-1") for a in shlex.split(line)]


class Server:
    def __init__(self, port):
        self.port = port
        self.conns = {}

    def conn(self, n):
        if n not in self.conns:
            s = socket.create_connection(("127.0.0.1", self.port))
            s.settimeout(5)
            self.conns[n] = (s, s.makefile("rb"))
        return self.conns[n]

    def call(self, n, args, read_only=False):
        try:
            s, f = self.conn(n)
            if not read_only:
                s.sendall(encode(args))
            return read_reply(f)
        except (EOFError, ConnectionError, OSError):
            self.drop(n)
            return b"<closed>"

    def drop(self, n):
        if n in self.conns:
            try:
                self.conns[n][0].close()
            except OSError:
                pass
            del self.conns[n]

    def reset(self):
        for n in list(self.conns):
            self.drop(n)
        users = self.call(0, [b"ACL", b"USERS"])
        for name in re.findall(rb"\$\d+\r\n([^\r]*)\r\n", users):
            if name != b"default":
                self.call(0, [b"ACL", b"DELUSER", name])
        for cmd in (
            [b"ACL", b"SETUSER", b"default", b"reset", b"on", b"nopass", b"~*", b"&*", b"+@all"],
            [b"CONFIG", b"SET", b"requirepass", b""],
            [b"ACL", b"LOG", b"RESET"],
            [b"CONFIG", b"RESETSTAT"],
            [b"FLUSHALL"],
        ):
            self.call(0, cmd)


def items(reply):
    """The top-level elements of an array or map reply, raw."""
    f = __import__("io").BytesIO(reply)
    head = f.readline()
    n = int(head[1:-2])
    if head[:1] == b"%":
        n *= 2
    return head, [read_reply(f) for _ in range(max(n, 0))]


KEPT_CLIENT_FIELDS = ("name", "db", "multi", "cmd", "user", "resp", "flags")


def mask_client(text):
    fields = dict(f.split("=", 1) for f in text.split() if "=" in f)
    return " ".join(f"{k}={fields.get(k, '?')}" for k in KEPT_CLIENT_FIELDS)


def masked(mode, reply):
    if reply == b"<closed>" or reply[:1] in b"-!":
        return reply
    if mode == "sorted":
        head, elements = items(reply)
        return head + b"".join(sorted(elements))
    if mode == "ints":
        return re.sub(rb":\d+\r\n", b":<n>\r\n", reply)
    if mode == "len":
        return reply[:1] + b"%d" % len(reply)
    if mode == "info":
        text = reply.decode("latin-1")
        return "\n".join(l for l in text.split("\r\n") if l.startswith("acl_access")).encode()
    if mode == "client":
        text = reply.decode("latin-1").split("\r\n", 1)[-1]
        return "\n".join(mask_client(l) for l in text.splitlines() if l.startswith("id=")).encode()
    if mode == "log":
        head, entries = items(reply)
        out = [head]
        for entry in entries:
            ehead, fields = items(entry)
            parts = [ehead]
            for i in range(0, len(fields), 2):
                name = fields[i]
                value = fields[i + 1]
                if b"age-seconds" in name or b"timestamp" in name:
                    value = b"<time>"
                elif b"client-info" in name:
                    value = mask_client(value.split(b"\r\n")[1].decode("latin-1")).encode()
                parts += [name, value]
            out.append(b"".join(parts))
        return b"".join(out)
    return reply


def rust_bytes(b):
    out = ""
    for c in b:
        ch = chr(c)
        if ch in "\\\"":
            out += "\\" + ch
        elif ch == "\r":
            out += "\\r"
        elif ch == "\n":
            out += "\\n"
        elif 0x20 <= c < 0x7F:
            out += ch
        else:
            out += f"\\x{c:02x}"
    return f'b"{out}"'


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--redis", type=int, default=6390)
    p.add_argument("--tantalum", type=int, default=6391)
    p.add_argument("--rust", action="store_true")
    a = p.parse_args()
    servers = {"redis": Server(a.redis), "tantalum": Server(a.tantalum)}
    for name, server in servers.items():
        version = server.call(0, [b"INFO", b"server"])
        found = re.search(rb"(redis_version|tantalum_version):([^\r]*)", version)
        print(f"# {name}: {found.group(0).decode() if found else '?'}", file=sys.stderr)
        server.reset()
    compared = differ = 0
    for raw in SCENARIOS.strip().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            if a.rust and line.startswith("#"):
                print(f"    // {line[1:].strip()}")
            continue
        if line == "!reset":
            for server in servers.values():
                server.reset()
            if a.rust:
                print("    Step::Reset,")
            continue
        if line.startswith("!sleep"):
            time.sleep(int(line.split()[1]) / 1000)
            if a.rust:
                print(f"    Step::Sleep({line.split()[1]}),")
            continue
        conn, mode = 0, None
        while line[0] in "@?":
            word, _, line = line.partition(" ")
            if word[0] == "@":
                conn = int(word[1:])
            else:
                mode = word[1:]
        args = parse_args(line)
        read_only = args == [b"READ"]
        replies = {
            name: masked(mode, server.call(conn, args, read_only)) for name, server in servers.items()
        }
        compared += 1
        same = replies["redis"] == replies["tantalum"]
        if a.rust:
            shown = ", ".join(rust_bytes(x) for x in args)
            if mode is None:
                mark = "" if same else "  // DIFFERS"
                print(f"    Step::Run({conn}, &[{shown}], {rust_bytes(replies['redis'])}),{mark}")
            else:
                print(f"    Step::Send({conn}, &[{shown}]),")
            continue
        if not same:
            differ += 1
            print(f"@{conn} {raw.strip()}")
            print(f"  redis:    {replies['redis']!r}")
            print(f"  tantalum: {replies['tantalum']!r}")
    if not a.rust:
        print(f"{compared} compared, {differ} differ")


main()
