#!/usr/bin/env python3
"""Runs the same client-side caching scenarios (`CLIENT TRACKING` and the
rest) on Redis and on Tantalum and shows where their replies differ, byte for
byte: tools/acl-diff.py's runner, with more.

    tools/tracking-diff.py [--redis 6390] [--tantalum 6391] [--rust]

Start both on spare ports with no password:

    redis-server --port 6390 --save '' --appendonly no &
    target/release/tantalum --port 6391 &

`READ` reads a connection's next message without sending anything: the
invalidations it's pushed. With --rust it prints the scenarios and Redis'
replies as test steps for tests/tracking.rs. Besides acl-diff.py's lines and
masks:

    {id:N}      in a command, connection N's id, as its last `CLIENT ID` replied
    ?flags      CLIENT INFO or LIST: only each line's flags= and redir= (an id
                read as <id>)
    ?tracking   INFO: only the tracking_ lines
    ?keys       an invalidation's keys only counted (which keys a full table drops
                is random)
"""

import argparse
import importlib.util
import os
import re
import sys
import time

spec = importlib.util.spec_from_file_location(
    "acl_diff", os.path.join(os.path.dirname(os.path.abspath(__file__)), "acl-diff.py")
)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)

SCENARIOS = r"""
# Errors, in Redis' order.
CLIENT TRACKING
CLIENT TRACKING maybe
CLIENT TRACKING on nonsense
CLIENT TRACKING on REDIRECT
CLIENT TRACKING on REDIRECT abc
CLIENT TRACKING on REDIRECT 0
CLIENT TRACKING on REDIRECT -5
CLIENT TRACKING on REDIRECT 999999
CLIENT TRACKING on REDIRECT 999999 REDIRECT 999998
CLIENT TRACKING on PREFIX a
CLIENT TRACKING on PREFIX
CLIENT TRACKING on BCAST OPTIN
CLIENT TRACKING on BCAST OPTOUT
CLIENT TRACKING on OPTIN OPTOUT
CLIENT TRACKING on BCAST PREFIX FOOBAR PREFIX FOO
CLIENT TRACKING on BCAST PREFIX FOO PREFIX FOOBAR
CLIENT TRACKING on BCAST PREFIX FOO PREFIX FOO
CLIENT TRACKING on BCAST PREFIX "" PREFIX a
CLIENT TRACKING maybe BCAST
CLIENT TRACKING off nonsense
CLIENT TRACKING off PREFIX a
CLIENT TRACKING off BCAST OPTIN OPTOUT
CLIENT TRACKINGINFO
CLIENT CACHING
CLIENT CACHING yes
CLIENT CACHING yes extra
CLIENT GETREDIR
CLIENT GETREDIR x
CLIENT TRACKINGINFO x
?flags CLIENT INFO
?tracking INFO
CLIENT TRACKING on
CLIENT GETREDIR
CLIENT TRACKINGINFO
?flags CLIENT INFO
?tracking INFO
CLIENT TRACKING on BCAST
CLIENT CACHING yes
CLIENT CACHING no
CLIENT TRACKING on OPTIN
CLIENT TRACKING on OPTOUT
CLIENT TRACKINGINFO
CLIENT CACHING no
CLIENT CACHING maybe
CLIENT CACHING yes
CLIENT TRACKINGINFO
PING
CLIENT TRACKINGINFO
CLIENT TRACKING off
CLIENT TRACKING on OPTOUT
CLIENT CACHING no
CLIENT TRACKINGINFO
CLIENT TRACKING on NOLOOP
CLIENT TRACKINGINFO
CLIENT TRACKING off
CLIENT TRACKING on BCAST PREFIX foo PREFIX bar
CLIENT TRACKINGINFO
?flags CLIENT INFO
?tracking INFO
CLIENT TRACKING on BCAST PREFIX fo
CLIENT TRACKING on BCAST PREFIX barb
CLIENT TRACKING on BCAST PREFIX baz
CLIENT TRACKINGINFO
CLIENT TRACKING on BCAST
CLIENT TRACKINGINFO
CLIENT TRACKING on
CLIENT TRACKING off
CLIENT TRACKINGINFO
CLIENT GETREDIR
?tracking INFO
@5 ?ints HELLO 3
@5 CLIENT TRACKINGINFO
@5 CLIENT TRACKING on BCAST PREFIX x
@5 CLIENT TRACKINGINFO
@5 CLIENT TRACKING off
!reset

# Invalidations redirected to a RESP2 connection subscribed to __redis__:invalidate.
@1 ?ints CLIENT ID
@1 SUBSCRIBE __redis__:invalidate
CLIENT TRACKING on REDIRECT {id:1}
?ints CLIENT GETREDIR
?flags CLIENT INFO
?ints CLIENT TRACKINGINFO
SET a 1
SET b 1
GET a
INCR b
INCR a
@1 READ
@1 PING
# Read once, heard of once.
INCR a
@1 PING
GET a
GET a
@2 SET a 5
@1 READ
@1 PING
# Every key a command reads: MGET, missing ones too, and a read that fails.
MGET a b c d
?tracking INFO
@2 MSET a 1 b 2 c 3
@1 READ
@1 READ
@1 READ
@1 PING
LLEN a
@2 SET a 2
@1 READ
@1 PING
?tracking INFO
GET d
@2 DEL d
@1 PING
@2 SET d 1
@1 READ
LPUSH l x
LLEN l
LRANGE l 0 -1
@2 RPUSH l y
@1 READ
GET l
@2 DEL l
@1 READ
# Keys written but not read aren't tracked; nor are a write's own.
SET w 1
@2 SET w 2
@1 PING
INCR w
@2 SET w 3
@1 PING
# Reads of every kind.
HSET h f v
HGET h f
@2 HSET h g w
@1 READ
ZADD z 1 m
ZSCORE z m
@2 ZINCRBY z 1 m
@1 READ
SADD s m
SISMEMBER s m
@2 SREM s m
@1 READ
XADD x 1-1 f v
XRANGE x - +
@2 XADD x 2-1 f v
@1 READ
SET bits 1
GETBIT bits 1
@2 SETBIT bits 1 0
@1 PING
@2 SETBIT bits 1 1
@1 READ
PFADD hll a
PFCOUNT hll
@2 PFADD hll b
@1 READ
SET o v
OBJECT ENCODING o
@2 SET o 12
@1 READ
EXISTS o
@2 DEL o
@1 READ
TYPE o
@2 SET o 1
@1 READ
STRLEN o
@2 EXPIRE o 100
@1 READ
TTL o
@2 PERSIST o
@1 READ
SET o2 2
GET o2
@2 RENAME o2 o3
@1 READ
GET o3
@2 MOVE o3 1
@1 READ
# A change made by the tracking client itself is heard of too.
GET a
SET a 9
@1 READ
GET a
DEL a
@1 READ
# A flush: everyone hears a null, and nothing is tracked after.
GET a
GET b
?tracking INFO
@2 FLUSHALL
@1 READ
?tracking INFO
@2 SET a 1
@1 PING
GET a
@2 FLUSHDB
@1 READ
GET a
@2 SELECT 1
@2 FLUSHDB
@1 READ
@2 SELECT 0
@2 SET a 1
@1 PING
GET a
FLUSHALL ASYNC
@1 READ
CLIENT TRACKING off
?flags CLIENT INFO
?tracking INFO
@2 SET a 2
@1 PING
!reset

# NOLOOP: what the client changes itself isn't sent.
@1 ?ints CLIENT ID
@1 SUBSCRIBE __redis__:invalidate
CLIENT TRACKING on REDIRECT {id:1} NOLOOP
MGET k1 k2 k3
@2 SET k1 1
SET k2 1
@2 SET k3 1
@1 READ
@1 READ
@1 PING
GET k2
@2 SET k2 2
@1 READ
?ints CLIENT TRACKINGINFO
!reset

# OPTIN and OPTOUT.
@1 ?ints CLIENT ID
@1 SUBSCRIBE __redis__:invalidate
CLIENT TRACKING on OPTIN REDIRECT {id:1}
@2 SET k1 1
GET k1
@2 SET k1 2
@1 PING
CLIENT CACHING yes
GET k1
@2 SET k1 3
@1 READ
CLIENT CACHING yes
PING
GET k1
@2 SET k1 4
@1 PING
CLIENT CACHING yes
?ints CLIENT ID
GET k1
@2 SET k1 5
@1 READ
CLIENT CACHING yes
MULTI
GET k1
GET k2
EXEC
@2 MSET k1 6 k2 6
@1 READ
@1 READ
@1 PING
CLIENT TRACKING off
CLIENT TRACKING on OPTOUT REDIRECT {id:1}
GET k1
@2 SET k1 7
@1 READ
CLIENT CACHING no
GET k1
@2 SET k1 8
@1 PING
GET k1
@2 SET k1 9
@1 READ
!reset

# BCAST: every key, or those of some prefixes, a message a prefix.
@1 ?ints CLIENT ID
@1 SUBSCRIBE __redis__:invalidate
CLIENT TRACKING on BCAST REDIRECT {id:1}
?flags CLIENT INFO
?tracking INFO
@2 MSET a 1 b 2 c 3
@1 READ
@2 SET d 1
@1 READ
@2 SELECT 1
@2 SET e 1
@1 READ
@2 SELECT 0
SET mine 1
@1 READ
GET a
@2 GET a
@1 PING
CLIENT TRACKING off
CLIENT TRACKING on BCAST REDIRECT {id:1} PREFIX a: PREFIX b:
?tracking INFO
MULTI
INCR a:1
INCR a:2
INCR b:1
INCR b:2
INCR c:1
EXEC
@1 READ
@1 READ
@1 PING
CLIENT TRACKING on BCAST REDIRECT {id:1} PREFIX c:
INCR c:1234
@1 READ
?ints CLIENT TRACKINGINFO
?tracking INFO
@2 MSET a:9 1 b:9 1 c:9 1 d:9 1
@1 READ
@1 READ
@1 READ
@1 PING
@3 CLIENT TRACKING on BCAST REDIRECT {id:1} PREFIX a
?tracking INFO
@2 SET a:x 1
@1 READ
@1 READ
@1 PING
@3 CLIENT TRACKING off
CLIENT TRACKING off
?tracking INFO
@2 SET a:x 2
@1 PING
!reset

# NOLOOP in BCAST mode.
@1 ?ints CLIENT ID
@1 SUBSCRIBE __redis__:invalidate
CLIENT TRACKING on BCAST REDIRECT {id:1} NOLOOP
@2 SET otherkey1 1
SET loopkey 1
@2 SET otherkey2 1
@1 READ
@1 READ
@1 PING
MSET loopkey 2 mine 3
@1 PING
# A key changed by the client and then by another is sent.
MULTI
SET k 1
SET k2 1
EXEC
@1 PING
!reset

# Expiry, in the background and on reading.
@1 ?ints CLIENT ID
@1 SUBSCRIBE __redis__:invalidate
CLIENT TRACKING on BCAST REDIRECT {id:1} NOLOOP
SET mykey myval px 1
SET mykeyotherkey myval
!sleep 1000
@1 READ
@1 PING
DEBUG SET-ACTIVE-EXPIRE 0
MULTI
SET mykey2 myval px 1
SET mykeyotherkey2 myval
DEBUG SLEEP 0.1
GET mykey2
EXEC
GET mykey2
@1 READ
@1 PING
CLIENT TRACKING off
CLIENT TRACKING on REDIRECT {id:1}
SET e1 v px 100
GET e1
!sleep 200
GET e1
@1 READ
@1 PING
@2 SET e1 v
@1 PING
GET e1
@2 SET e1 w
@1 READ
SET e2 v px 100
GET e2
!sleep 200
@2 GET e2
@1 READ
DEBUG SET-ACTIVE-EXPIRE 1
SET e3 v px 100
GET e3
!sleep 1000
@1 READ
@1 PING
!reset

# RESP3: invalidations come on the same connection, after the reply.
@0 ?ints HELLO 3
CLIENT TRACKING on
@2 SET key1 1
GET key1
@2 SET key1 2
READ
PING
HSET myhash f 1
HGET myhash f
HDEL myhash f
READ
MSET x 1 y 2
GET x
GET y
MULTI
INCR x
GET y
EXEC
READ
PING
MGET x y
DEBUG SET-ACTIVE-EXPIRE 0
PEXPIRE y 100
!sleep 150
MGET x y
READ
DEBUG SET-ACTIVE-EXPIRE 1
SET a 1
GET a
MULTI
FLUSHDB
EXEC
READ
GET a
FLUSHALL
READ
PING
# Read again, heard of again.
SET p1 1
SET p2 1
MGET p1 p2
@2 SET p1 2
READ
GET p1
@2 SET p1 3
READ
PING
# After switching to BCAST mode, nothing for keys read before.
CLIENT TRACKING off
CLIENT TRACKING on
@2 SET key1 1
GET key1
CLIENT TRACKING off
CLIENT TRACKING on BCAST
@2 INCR key1
READ
PING
# RESP2 again: nothing to hear without a redirection.
CLIENT TRACKING off
?ints HELLO 2
CLIENT TRACKING on
GET key1
@2 SET key1 3
PING
# Back to RESP3: what was read meanwhile is heard of.
?ints HELLO 3
@2 SET key1 4
GET key1
@2 SET key1 5
READ
CLIENT TRACKING off
!reset

# Redirected to RESP3 connections: one subscribed, one not.
@1 ?ints HELLO 3
@1 ?ints CLIENT ID
@1 SUBSCRIBE __redis__:invalidate
CLIENT TRACKING on REDIRECT {id:1}
@2 SET key1 1
GET key1
@2 INCR key1
@1 READ
@4 ?ints HELLO 3
@4 ?ints CLIENT ID
@3 CLIENT TRACKING on REDIRECT {id:4}
@4 PING
@3 GET key1
@2 SET key1 2
@4 READ
@4 SEND CLIENT REPLY OFF
@3 GET key1
@2 SET key1 3
@4 READ
@4 CLIENT REPLY ON
@4 PING
# The redirected-to connection goes: RESP3 clients hear it's broken.
@1 UNSUBSCRIBE __redis__:invalidate
@1 QUIT
?ints HELLO 3
GET key1
@2 SET key1 4
?ints READ
?ints CLIENT TRACKINGINFO
?flags CLIENT INFO
GET key1
@2 SET key1 5
?ints READ
PING
CLIENT TRACKING on
CLIENT TRACKINGINFO
!reset

# Different clients redirect to the same connection.
@1 ?ints CLIENT ID
@1 SUBSCRIBE __redis__:invalidate
CLIENT TRACKING on REDIRECT {id:1}
@3 CLIENT TRACKING on REDIRECT {id:1}
@2 MSET key1 1 key2 1
GET key1
@3 GET key2
@2 INCR key1
@1 READ
@2 INCR key2
@1 READ
@1 PING
# And both hear of a key both read, once each.
GET key1
@3 GET key1
@2 INCR key1
@1 READ
@1 READ
@1 PING
?tracking INFO
!reset

# Scripts: only what they read is tracked.
?ints HELLO 3
CLIENT TRACKING on
@2 MSET key2 1 key1 1
EVAL "redis.call('set', 'key3', 'bar')" 2 key1 key2
@2 MSET key2 2 key1 2
PING
EVAL "redis.call('get', 'key2')" 2 key1 key2
@2 MSET key2 2 key3 2
READ
PING
EVAL_RO "redis.call('ping')" 2 key1 key2
@2 MSET key2 2 key1 2
PING
EVAL_RO "redis.call('get', 'key2')" 2 key1 key2
@2 MSET key2 2 key3 2
READ
PING
# A script that reads and then writes the key.
EVAL "redis.call('get', KEYS[1]) return redis.call('set', KEYS[1], 'x')" 1 key4
READ
PING
CLIENT TRACKING off
@3 CLIENT TRACKING on NOLOOP
@3 ?ints HELLO 3
@3 SET key1 1
@3 GET key1
@3 EVAL "return redis.call('set', 'key1', '2')" 1 key1
@3 GET key1
@3 CLIENT TRACKING off
!reset

# The table's size limit.
@1 ?ints CLIENT ID
@1 SUBSCRIBE __redis__:invalidate
CLIENT TRACKING on REDIRECT {id:1} NOLOOP
MSET key1 1 key2 2
MGET key1 key2
?tracking INFO
CONFIG SET tracking-table-max-keys 1
?keys @1 READ
@1 PING
?tracking INFO
MGET k3 k4 k5
?keys @1 READ
?keys @1 READ
?keys @1 READ
@1 PING
?tracking INFO
CONFIG SET tracking-table-max-keys 0
MGET k6 k7 k8
?tracking INFO
@1 PING
CONFIG SET tracking-table-max-keys 1000000
!reset

# RESET ends tracking.
?ints HELLO 3
CLIENT TRACKING on BCAST PREFIX p
?tracking INFO
RESET
CLIENT TRACKINGINFO
?flags CLIENT INFO
?tracking INFO
@2 SET p1 1
PING
!reset

# A blocked command that's served changes a key its client tracks.
@2 RPUSH list2 a
@3 ?ints HELLO 3
@3 CLIENT TRACKING on
@3 LRANGE list2 0 -1
@3 SEND BLMOVE list1 list2 left left 0
!sleep 100
@2 RPUSH list1 foo
@3 READ
@3 READ
@3 PING
!reset

# Coverage, as Redis' tests have it.
@1 ?ints CLIENT ID
CLIENT TRACKING on OPTIN REDIRECT {id:1}
CLIENT CACHING yes
CLIENT TRACKING off
CLIENT REPLY on
CLIENT TRACKINGINFO
CLIENT GETREDIR
"""


def masked(mode, reply):
    if mode == "flags":
        text = reply.decode("latin-1")
        lines = []
        for line in re.split(r"\r\n|\n", text):
            flags = re.search(r"\bflags=(\S*)", line)
            redir = re.search(r"\bredir=(-?\d+)", line)
            if flags and redir:
                r = redir.group(1)
                lines.append(f"flags={flags.group(1)} redir={r if r in ('-1', '0') else '<id>'}")
        return "\n".join(lines).encode()
    if mode == "keys":
        head, _, rest = reply.partition(b"invalidate\r\n")
        return head + b"invalidate " + rest.split(b"\r\n", 1)[0]
    if mode == "tracking":
        return b"\n".join(re.findall(rb"tracking_[a-z_]+:\d+", reply))
    return runner.masked(mode, reply)


class Server(runner.Server):
    def __init__(self, port):
        super().__init__(port)
        self.ids = {}

    def drop(self, n):
        self.ids.pop(n, None)
        super().drop(n)

    def call(self, n, args, read_only=False):
        # SEND: sends without reading the reply (a blocking command's).
        if args[:1] == [b"SEND"]:
            s, _ = self.conn(n)
            s.sendall(runner.encode(args[1:]))
            return b"<sent>"
        args = [
            self.ids.get(int(m.group(1)), b"?") if (m := re.fullmatch(rb"\{id:(\d+)\}", a)) else a
            for a in args
        ]
        reply = super().call(n, args, read_only)
        if [a.upper() for a in args] == [b"CLIENT", b"ID"] and reply.startswith(b":"):
            self.ids[n] = reply[1:-2]
        return reply


def main():
    runner.Server.reset_extra = [[b"CONFIG", b"SET", b"tracking-table-max-keys", b"1000000"]]
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
        args = runner.parse_args(line)
        read_only = args == [b"READ"]
        replies = {
            name: masked(mode, server.call(conn, args, read_only)) for name, server in servers.items()
        }
        compared += 1
        same = replies["redis"] == replies["tantalum"]
        if a.rust:
            shown = ", ".join(runner.rust_bytes(x) for x in args)
            if mode is None:
                mark = "" if same else "  // DIFFERS"
                print(f"    Step::Run({conn}, &[{shown}], {runner.rust_bytes(replies['redis'])}),{mark}")
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


if __name__ == "__main__":
    main()
