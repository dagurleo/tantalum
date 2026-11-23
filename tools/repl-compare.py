#!/usr/bin/env python3
"""Compares the data on two servers, key by key, in every database: for a
master and its replica (Redis or Tantalum, either way round).

    tools/repl-compare.py MASTER_PORT REPLICA_PORT [--dbs 16]

Each key's type, its value (read whole, by type: sets and hashes sorted, so
order doesn't count) and whether it has a TTL (and that it's within 2 s).
Prints what differs; exits 1 if anything does.
"""
import argparse
import socket
import sys


class Conn:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port))
        self.f = self.sock.makefile("rb")

    def call(self, *args):
        out = b"*%d\r\n" % len(args)
        for a in args:
            a = a if isinstance(a, bytes) else str(a).encode()
            out += b"$%d\r\n%s\r\n" % (len(a), a)
        self.sock.sendall(out)
        return self.read()

    def read(self):
        line = self.f.readline()
        kind, rest = line[:1], line[1:-2]
        if kind == b"$":
            n = int(rest)
            return None if n < 0 else self.f.read(n + 2)[:-2]
        if kind == b"*":
            n = int(rest)
            return None if n < 0 else [self.read() for _ in range(n)]
        if kind == b":":
            return int(rest)
        if kind == b"-":
            return Exception(rest.decode())
        return rest


def value(c, key, kind):
    if kind == b"string":
        return c.call("GET", key)
    if kind == b"list":
        return c.call("LRANGE", key, 0, -1)
    if kind == b"set":
        return sorted(c.call("SMEMBERS", key))
    if kind == b"zset":
        return c.call("ZRANGE", key, 0, -1, "WITHSCORES")
    if kind == b"hash":
        flat = c.call("HGETALL", key)
        return sorted(zip(flat[::2], flat[1::2]))
    if kind == b"stream":
        entries = c.call("XRANGE", key, "-", "+")
        # Without entries-read and lag, which Redis doesn't propagate either
        # (a replica keeps what it had at its full sync).
        groups = [g[:8] for g in c.call("XINFO", "GROUPS", key)]
        pending = []
        for g in groups:
            name = g[1]
            pending.append((name, c.call("XPENDING", key, name, "-", "+", 1000)))
        info = c.call("XINFO", "STREAM", key)
        return entries, groups, pending, info[info.index(b"last-generated-id") + 1]
    return kind


def keys(c):
    found, cursor = [], b"0"
    while True:
        cursor, batch = c.call("SCAN", cursor, "COUNT", 1000)
        found += batch
        if cursor == b"0":
            return set(found)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("master", type=int)
    p.add_argument("replica", type=int)
    p.add_argument("--dbs", type=int, default=16)
    a = p.parse_args()
    m, r = Conn(a.master), Conn(a.replica)
    differ = 0
    total = 0
    shown = 0

    def say(text):
        nonlocal shown
        shown += 1
        if shown <= 20:
            print(text)
        elif shown == 21:
            print("...")
    for db in range(a.dbs):
        m.call("SELECT", db)
        r.call("SELECT", db)
        mk, rk = keys(m), keys(r)
        for key in sorted(mk ^ rk):
            say(f"db {db} {key!r}: only on the {'master' if key in mk else 'replica'}")
            differ += 1
        for key in sorted(mk & rk):
            total += 1
            kind = m.call("TYPE", key)
            if r.call("TYPE", key) != kind:
                say(f"db {db} {key!r}: types differ")
                differ += 1
                continue
            mv, rv = value(m, key, kind), value(r, key, kind)
            mt, rt = m.call("PTTL", key), r.call("PTTL", key)
            if mv != rv:
                say(f"db {db} {key!r}: values differ\n  master  {mv!r}\n  replica {rv!r}")
                differ += 1
            elif (mt < 0) != (rt < 0) or abs(mt - rt) > 2000:
                say(f"db {db} {key!r}: TTLs differ: {mt} and {rt}")
                differ += 1
    mf, rf = m.call("FUNCTION", "LIST"), r.call("FUNCTION", "LIST")
    if sorted(map(repr, mf)) != sorted(map(repr, rf)):
        say("function libraries differ")
        differ += 1
    print(f"{total} keys compared, {differ} differ")
    sys.exit(1 if differ else 0)


if __name__ == "__main__":
    main()
