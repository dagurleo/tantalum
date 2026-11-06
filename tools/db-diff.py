#!/usr/bin/env python3
"""Numbered databases and CLIENT KILL against a real Redis: the same commands to
both, comparing replies byte for byte. First every error and edge by hand, then
random work across databases, in seeds.

    tools/db-diff.py [--redis 7390] [--tantalum 7391] [--seeds 1,2,3] [--steps 4000]

Start the servers yourself on spare ports:

    redis-server --port 7390 --save '' --appendonly no &
    target/release/tantalum --port 7391 &
"""

import argparse
import random
import socket
import sys


class Conn:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port))
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.f = self.sock.makefile("rb")

    def send(self, args):
        out = b"*%d\r\n" % len(args)
        for a in args:
            a = a if isinstance(a, bytes) else str(a).encode()
            out += b"$%d\r\n%s\r\n" % (len(a), a)
        self.sock.sendall(out)

    def read(self):
        line = self.f.readline()
        if not line:
            return b"(closed)"
        kind, rest = line[:1], line[1:-2]
        if kind in b"$=!":
            n = int(rest)
            return line + (self.f.read(n + 2) if n >= 0 else b"")
        if kind in b"*~>":
            n = int(rest)
            return line + b"".join(self.read() for _ in range(max(n, 0)))
        if kind in b"%|":
            return line + b"".join(self.read() for _ in range(2 * int(rest)))
        return line

    def call(self, *args):
        self.send(args)
        return self.read()


class Both:
    """The same connection to both servers."""

    def __init__(self, ports, stats):
        self.conns = [Conn(p) for p in ports]
        self.stats = stats

    def call(self, *args, sort=False, same=True):
        replies = [c.call(*args) for c in self.conns]
        if sort:
            replies = [normalize(r) for r in replies]
        self.stats["commands"] += 1
        if same and replies[0] != replies[1]:
            self.stats["differences"] += 1
            if self.stats["differences"] <= 40:
                shown = " ".join(a.decode("latin-1") if isinstance(a, bytes) else str(a) for a in args)
                print(f"DIFF {shown[:100]}\n  redis:    {replies[0][:160]!r}\n  tantalum: {replies[1][:160]!r}")
        return replies[0]


def normalize(reply):
    """A flat array's elements in sorted order, for replies in no set order."""
    lines = reply.split(b"\r\n")
    if lines[0].startswith(b"*"):
        items = [lines[i] + b"\r\n" + lines[i + 1] for i in range(1, len(lines) - 1, 2)]
        return lines[0] + b"\r\n" + b"\r\n".join(sorted(items))
    return reply


def by_hand(c, c2):
    # SELECT
    for arg in ["0", "15", "16", "-1", "abc", "1.5", "", "99999999999", "2147483648", "-2147483649", "00", "+1", " 1"]:
        c.call("SELECT", arg)
    c.call("SELECT")
    c.call("SELECT", "1", "2")
    c.call("SELECT", "0")
    # MOVE
    c.call("FLUSHALL")
    c.call("SET", "m", "v")
    for arg in ["0", "16", "-1", "abc", "", "1.5", "99999999999"]:
        c.call("MOVE", "m", arg)
    c.call("MOVE", "m")
    c.call("MOVE", "missing", "1")
    c.call("MOVE", "m", "1")
    c.call("EXISTS", "m")
    c.call("SET", "m", "v2")
    c.call("MOVE", "m", "1")
    c.call("GET", "m")
    c.call("SELECT", "1")
    c.call("GET", "m")
    c.call("SELECT", "0")
    c.call("SET", "t", "v", "EX", "1000")
    c.call("MOVE", "t", "2")
    c.call("SELECT", "2")
    c.call("TTL", "t")
    c.call("SELECT", "0")
    # COPY
    c.call("FLUSHALL")
    c.call("SET", "a", "v")
    for opts in [["DB", "0"], ["DB", "16"], ["DB", "-1"], ["DB", "abc"], ["DB"], ["REPLACE", "DB"], ["DB", "1", "DB"], ["NOPE"],
                 ["DB", "1"], ["DB", "1"], ["DB", "1", "REPLACE"], ["REPLACE", "DB", "1"], ["db", "2"], ["DB", "99999999999"]]:
        c.call("COPY", "a", "a", *opts)
    c.call("COPY", "a", "b", "DB", "0")
    c.call("COPY", "missing", "b", "DB", "3")
    # SWAPDB
    c.call("FLUSHALL")
    c.call("SET", "s", "zero")
    for args in [["0", "0"], ["0", "16"], ["16", "0"], ["-1", "0"], ["0", "-1"], ["a", "0"], ["0", "a"], ["a", "b"], ["1.5", "0"],
                 ["99999999999", "0"], ["0", "99999999999"], ["", "0"], ["0", "1"]]:
        c.call("SWAPDB", *args)
    c.call("SWAPDB", "0")
    c.call("GET", "s")
    c.call("SELECT", "1")
    c.call("GET", "s")
    c.call("DBSIZE")
    c.call("SELECT", "0")
    c.call("DBSIZE")
    # FLUSHDB, FLUSHALL
    for name in ["FLUSHDB", "FLUSHALL"]:
        for args in [[], ["ASYNC"], ["SYNC"], ["async"], ["NOPE"], ["ASYNC", "SYNC"]]:
            c.call(name, *args)
    # In a transaction, and it stays.
    c.call("FLUSHALL")
    c.call("MULTI")
    c.call("SELECT", "16")
    c.call("SET", "x", "1")
    c.call("EXEC")
    c.call("GET", "x")
    c.call("MULTI")
    c.call("SELECT", "abc")
    c.call("DISCARD")
    c.call("MULTI")
    c.call("SELECT", "3")
    c.call("SET", "x", "three")
    c.call("MOVE", "x", "4")
    c.call("MOVE", "x", "3")
    c.call("SWAPDB", "3", "5")
    c.call("EXEC")
    c.call("DBSIZE")
    c.call("SELECT", "4")
    c.call("GET", "x")
    c.call("SELECT", "5")
    c.call("GET", "x")
    c.call("SELECT", "0")
    # Scripts.
    c.call("FLUSHALL")
    for script in [
        "return redis.call('SELECT', 1)",
        "return redis.pcall('SELECT', 16)",
        "return redis.pcall('SELECT', 'abc')",
        "redis.call('SELECT', 2) return redis.call('SET', KEYS[1], 'two')",
        "redis.call('SELECT', 2) return redis.call('GET', KEYS[1])",
        "return redis.call('GET', KEYS[1])",
        "redis.call('SELECT', 2) return redis.call('MOVE', KEYS[1], 3)",
        "return redis.call('SWAPDB', 0, 3)",
        "return redis.call('DBSIZE')",
    ]:
        c.call("EVAL", script, "1", "k")
    c.call("DBSIZE")
    c.call("GET", "k")
    # What scripts may not do, and a transaction's kill.
    for script in [
        "return redis.pcall('CLIENT', 'KILL', 'ID', '999999')",
        "return redis.pcall('CLIENT', 'ID')",
        "return redis.pcall('COPY', KEYS[1], KEYS[1], 'DB', 9)",
        "return redis.pcall('MOVE', KEYS[1], 16)",
        "return redis.pcall('SWAPDB', 0, 16)",
        "return redis.pcall('FLUSHDB')",
    ]:
        c.call("EVAL", script, "1", "k")
    c.call("MULTI")
    c.call("CLIENT", "KILL", "ID", "999999")
    c.call("EXEC")
    # WATCH across them.
    c.call("FLUSHALL")
    c2.call("SELECT", "0")
    for steps in [
        # (where it watches, where the other writes, whether it's the same key)
        ("1", "1"),
        ("1", "0"),
        ("0", "1"),
    ]:
        c.call("SELECT", steps[0])
        c.call("WATCH", "w")
        c2.call("SELECT", steps[1])
        c2.call("SET", "w", "x")
        c.call("MULTI")
        c.call("PING")
        c.call("EXEC")
    c.call("SELECT", "0")
    c.call("WATCH", "w")
    c2.call("SELECT", "1")
    c2.call("MOVE", "w", "0")
    c.call("MULTI")
    c.call("PING")
    c.call("EXEC")
    c.call("FLUSHALL")
    c.call("SET", "w", "1")
    c.call("WATCH", "w")
    c2.call("SWAPDB", "0", "1")
    c.call("MULTI")
    c.call("PING")
    c.call("EXEC")
    # Odd cases of SWAPDB and watching.
    c.call("FLUSHALL")
    c.call("WATCH", "absent")
    c2.call("SWAPDB", "0", "1")
    c.call("MULTI")
    c.call("PING")
    c.call("EXEC")
    # RESET goes back to 0.
    c.call("SELECT", "4")
    c.call("SET", "r", "1")
    c.call("RESET")
    c.call("EXISTS", "r")
    # INFO keyspace's lines, whatever else differs.
    c.call("FLUSHALL")
    c.call("CONFIG", "GET", "databases")
    # CLIENT KILL's replies.
    for args in [[], ["ID"], ["ID", "0"], ["ID", "-1"], ["ID", "x"], ["ID", "999999"], ["ID", "999999", "TYPE"],
                 ["TYPE", "nope"], ["TYPE", "master"], ["TYPE", "replica"], ["TYPE", "slave"], ["TYPE", "pubsub"],
                 ["USER", "nobody"], ["USER", "default", "ID", "999999"], ["SKIPME", "maybe"], ["SKIPME", "yes", "ID", "999999"],
                 ["SKIPME", "no", "ID", "999999"], ["ADDR", "1.2.3.4:5"], ["LADDR", "1.2.3.4:5"], ["NOPE", "1"],
                 ["MAXAGE", "100000"], ["MAXAGE", "x"], ["1.2.3.4:5"], [""]]:
        c.call("CLIENT", "KILL", *args)
    c.call("CLIENT", "LIST", "TYPE", "pubsub", same=False)


def keyspace_lines(conn):
    info = conn.call("INFO", "keyspace")
    lines = [l for l in info.split(b"\r\n") if l.startswith(b"db")]
    return [l.split(b",avg_ttl")[0] for l in lines]


def random_work(c, c2, stats, seed, steps, ports, extra):
    rng = random.Random(seed)
    keys = [f"k{i}" for i in range(12)]
    current = 0
    c.call("FLUSHALL")
    for _ in range(steps):
        conn = c if rng.random() < 0.8 else c2
        key, other = rng.choice(keys), rng.choice(keys)
        db = rng.randrange(0, 6)
        roll = rng.randrange(0, 100)
        if roll < 12:
            conn.call("SELECT", db)
        elif roll < 28:
            conn.call("SET", key, rng.randrange(1000))
        elif roll < 36:
            conn.call("GET", key)
        elif roll < 40:
            conn.call("DEL", key)
        elif roll < 44:
            conn.call("RPUSH", "l" + key, rng.randrange(10))
        elif roll < 48:
            conn.call("LRANGE", "l" + key, 0, -1)
        elif roll < 52:
            conn.call("HSET", "h" + key, "f", rng.randrange(10))
        elif roll < 55:
            conn.call("HGETALL", "h" + key)
        elif roll < 62:
            conn.call("MOVE", rng.choice([key, "l" + key, "h" + key]), db)
        elif roll < 68:
            opts = ["DB", db] + (["REPLACE"] if rng.random() < 0.3 else [])
            conn.call("COPY", key, other, *opts)
        elif roll < 71:
            conn.call("SWAPDB", db, rng.randrange(0, 6))
        elif roll < 73:
            conn.call("FLUSHDB") if rng.random() < 0.8 else conn.call("FLUSHALL")
        elif roll < 77:
            conn.call("DBSIZE")
        elif roll < 80:
            conn.call("KEYS", "*", sort=True)
        elif roll < 83:
            conn.call("EXISTS", key, other)
        elif roll < 86:
            conn.call("RENAME", key, other)
        elif roll < 89:
            conn.call("TYPE", rng.choice([key, "l" + key, "h" + key]))
        elif roll < 92:
            conn.call("MULTI")
            conn.call("SET", key, "t")
            conn.call("SELECT", db)
            conn.call("GET", key)
            conn.call("MOVE", key, rng.randrange(0, 6))
            conn.call("EXEC")
        elif roll < 95:
            script = f"redis.call('SELECT', {db}) redis.call('INCR', KEYS[1]) return redis.call('DBSIZE')"
            conn.call("EVAL", script, "1", "n" + key)
        elif roll < 97:
            conn.call("WATCH", key)
            other_conn = c2 if conn is c else c
            if rng.random() < 0.5:
                other_conn.call("SET", key, "w")
            conn.call("MULTI")
            conn.call("DBSIZE")
            conn.call("EXEC")
        else:
            lines = [keyspace_lines(x) for x in conn.conns]
            stats["commands"] += 1
            if lines[0] != lines[1]:
                stats["differences"] += 1
                print(f"DIFF INFO keyspace, seed {seed}\n  redis:    {lines[0]}\n  tantalum: {lines[1]}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis", type=int, default=7390)
    parser.add_argument("--tantalum", type=int, default=7391)
    parser.add_argument("--seeds", default="1,2,3,7")
    parser.add_argument("--steps", type=int, default=4000)
    opts = parser.parse_args()
    ports = [opts.redis, opts.tantalum]
    stats = {"commands": 0, "differences": 0}
    c, c2 = Both(ports, stats), Both(ports, stats)
    by_hand(c, c2)
    print(f"by hand: {stats['commands']} commands, {stats['differences']} differences")
    for seed in opts.seeds.split(","):
        c, c2 = Both(ports, stats), Both(ports, stats)
        random_work(c, c2, stats, int(seed), opts.steps, ports, None)
        print(f"seed {seed}: {stats['commands']} commands so far, {stats['differences']} differences")
    sys.exit(1 if stats["differences"] else 0)


if __name__ == "__main__":
    main()
