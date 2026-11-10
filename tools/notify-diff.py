#!/usr/bin/env python3
"""Keyspace notifications on Redis and on Tantalum: the same commands on both,
with a client subscribed to every event, and where the events differ.

    tools/notify-diff.py [--redis 6390] [--tantalum 6391] [--seeds 1,2,3]
                         [--steps 2000] [--events KEA]

Start both on spare ports with no password and DEBUG on:

    redis-server --port 6390 --save '' --appendonly no --enable-debug-command yes &
    target/release/tantalum --port 6391 --enable-debug-command yes &

Scripted cases come first, then random commands on a few keys of each type
(with MULTI/EXEC, scripts, a blocked client and keys expiring as they're
read). After each command the writer publishes a token, and everything the
subscriber heard before it is the command's. Replies are compared too, as a
check that both hold the same data; the events are what's reported.
"""

import argparse
import random
import signal
import socket
import sys
import time


class Conn:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port))
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""

    def send(self, *args):
        out = b"*%d\r\n" % len(args)
        for a in args:
            a = a if isinstance(a, bytes) else str(a).encode()
            out += b"$%d\r\n%s\r\n" % (len(a), a)
        self.s.sendall(out)

    def _line(self):
        while b"\r\n" not in self.buf:
            chunk = self.s.recv(65536)
            if not chunk:
                raise EOFError("closed")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _bytes(self, n):
        while len(self.buf) < n + 2:
            chunk = self.s.recv(65536)
            if not chunk:
                raise EOFError("closed")
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n + 2:]
        return data

    def read(self):
        line = self._line()
        kind, rest = line[:1], line[1:]
        if kind in (b"+", b"-", b":", b",", b"_", b"#", b"("):
            return line
        if kind in (b"$", b"="):
            n = int(rest)
            return None if n < 0 else self._bytes(n)
        if kind in (b"*", b">", b"~", b"%"):
            n = int(rest)
            if n < 0:
                return None
            if kind == b"%":
                n *= 2
            return [self.read() for _ in range(n)]
        raise ValueError(line)

    def call(self, *args):
        self.send(*args)
        return self.read()


class Side:
    def __init__(self, port, events):
        self.w = Conn(port)
        self.sub = Conn(port)
        self.w.call("FLUSHALL")
        self.w.call("DEBUG", "SET-ACTIVE-EXPIRE", "0")
        self.w.call("CONFIG", "SET", "notify-keyspace-events", events)
        self.sub.call("PSUBSCRIBE", "*")
        self.token = 0

    def run(self, args, sync=True):
        reply = self.w.call(*args)
        return reply, (self.heard() if sync else [])

    def heard(self):
        self.token += 1
        token = str(self.token).encode()
        self.w.call("PUBLISH", "__sync__", token)
        got = []
        while True:
            msg = self.sub.read()
            if msg[0] == b"pmessage" and msg[2] == b"__sync__":
                if msg[3] == token:
                    return got
                continue
            got.append(msg[2] + b" " + msg[3])


SCRIPTED = [
    [["SET", "k", "v"], ["SET", "k", "v", "EX", "100"], ["SET", "k", "w", "NX"], ["SET", "k", "w", "XX", "GET"]],
    [["SETEX", "k", "10", "v"], ["PSETEX", "k", "10000", "v"], ["SETNX", "k", "v"], ["GETSET", "k", "x"]],
    [["MSET", "a", "1", "b", "2", "a", "3"], ["MSETNX", "a", "1", "c", "2"], ["MSETNX", "d", "1", "e", "2"]],
    [["INCR", "n"], ["DECRBY", "n", "5"], ["INCRBYFLOAT", "n", "1.5"], ["APPEND", "n", "x"], ["INCR", "n"]],
    [["SETRANGE", "s", "0", ""], ["SETRANGE", "s", "3", "ab"], ["SETRANGE", "s", "0", ""], ["SETBIT", "s", "0", "1"], ["SETBIT", "s", "0", "1"]],
    [["BITFIELD", "bf", "GET", "u8", "0"], ["BITFIELD", "bf", "SET", "u8", "0", "1"], ["BITFIELD", "bf", "SET", "u8", "0", "1"], ["BITFIELD", "bf", "OVERFLOW", "FAIL", "INCRBY", "u8", "0", "300"]],
    [["SET", "x", "a"], ["SET", "y", "b"], ["BITOP", "AND", "z", "x", "y"], ["BITOP", "AND", "z", "nope", "nope2"], ["BITOP", "OR", "z", "nope"]],
    [["SET", "k", "v"], ["GETEX", "k"], ["GETEX", "k", "EX", "10"], ["GETEX", "k", "PERSIST"], ["GETEX", "k", "PERSIST"], ["GETEX", "k", "PXAT", "1"], ["GETDEL", "k"]],
    [["SET", "k", "v"], ["EXPIRE", "k", "100"], ["EXPIRE", "k", "50", "GT"], ["EXPIRE", "k", "50", "LT"], ["PERSIST", "k"], ["PERSIST", "k"], ["PEXPIREAT", "k", "1"], ["EXPIRE", "nope", "1"]],
    [["SET", "a", "1"], ["RENAME", "a", "b"], ["RENAME", "b", "b"], ["SET", "c", "1"], ["RENAMENX", "b", "c"], ["RENAME", "b", "c"], ["RENAME", "nope", "x"]],
    [["SET", "a", "1"], ["COPY", "a", "b"], ["COPY", "a", "b"], ["COPY", "a", "b", "REPLACE"], ["COPY", "a", "b", "DB", "2"], ["MOVE", "a", "3"], ["MOVE", "b", "2"]],
    [["SET", "a", "1"], ["DEL", "a", "a", "nope"], ["SET", "a", "1"], ["UNLINK", "a"], ["DEL", "nope"]],
    [["RPUSH", "l", "a", "b", "c"], ["LPUSHX", "l2", "a"], ["LPUSHX", "l", "z"], ["LINSERT", "l", "BEFORE", "b", "x"], ["LINSERT", "l", "BEFORE", "q", "x"], ["LSET", "l", "0", "y"], ["LREM", "l", "0", "nope"], ["LREM", "l", "1", "y"]],
    [["RPUSH", "l", "a", "b", "c"], ["LTRIM", "l", "0", "-1"], ["LTRIM", "l", "0", "0"], ["LTRIM", "nope", "0", "1"], ["LTRIM", "l", "1", "0"]],
    [["RPUSH", "l", "a", "b", "c"], ["LPOP", "l", "0"], ["LPOP", "l"], ["RPOP", "l", "5"], ["RPOP", "l"]],
    [["RPUSH", "l", "a", "b"], ["LMOVE", "l", "m", "LEFT", "RIGHT"], ["LMOVE", "m", "m", "LEFT", "LEFT"], ["RPOPLPUSH", "l", "m"], ["RPOPLPUSH", "l", "m"]],
    [["RPUSH", "l", "a", "b"], ["RPUSH", "l2", "c"], ["LMPOP", "2", "nope", "l", "LEFT", "COUNT", "5"], ["LMPOP", "2", "l", "l2", "RIGHT"]],
    [["SADD", "s", "a", "b"], ["SADD", "s", "a"], ["SREM", "s", "nope"], ["SREM", "s", "a"], ["SMOVE", "s", "s", "b"], ["SMOVE", "s", "t", "b"], ["SMOVE", "s", "t", "b"]],
    [["SADD", "s", "a"], ["SADD", "t", "a"], ["SMOVE", "s", "t", "a"]],
    [["SADD", "s", "a", "b"], ["SADD", "t", "b"], ["SINTERSTORE", "d", "s", "t"], ["SINTERSTORE", "d", "s", "t"], ["SDIFFSTORE", "d", "t", "s"], ["SDIFFSTORE", "d", "t", "s"], ["SUNIONSTORE", "d", "nope"]],
    [["SADD", "s", "a", "b", "c"], ["SPOP", "s", "0"], ["SPOP", "s", "5"], ["SPOP", "s"]],
    [["ZADD", "z", "1", "a", "2", "b"], ["ZADD", "z", "1", "a"], ["ZADD", "z", "XX", "CH", "5", "a"], ["ZADD", "z", "NX", "5", "a"], ["ZADD", "z", "INCR", "1", "a"], ["ZADD", "z", "GT", "INCR", "-1", "a"], ["ZINCRBY", "z", "0", "a"], ["ZINCRBY", "z", "1", "a"]],
    [["ZADD", "z", "1", "a", "2", "b", "3", "c"], ["ZREM", "z", "nope"], ["ZREM", "z", "a"], ["ZPOPMIN", "z"], ["ZPOPMAX", "z", "0"], ["ZPOPMAX", "z", "5"]],
    [["ZADD", "z", "1", "a", "2", "b", "3", "c"], ["ZREMRANGEBYSCORE", "z", "10", "20"], ["ZREMRANGEBYSCORE", "z", "1", "1"], ["ZREMRANGEBYLEX", "z", "-", "+"]],
    [["ZADD", "z", "1", "a", "2", "b"], ["ZADD", "y", "1", "b"], ["ZUNIONSTORE", "d", "2", "z", "y"], ["ZINTERSTORE", "d", "2", "z", "y"], ["ZDIFFSTORE", "d", "2", "y", "z"], ["ZRANGESTORE", "e", "z", "0", "0"], ["ZRANGESTORE", "e", "z", "5", "6"]],
    [["ZADD", "z", "1", "a"], ["ZADD", "y", "2", "b"], ["ZMPOP", "2", "nope", "z", "MIN"], ["ZMPOP", "1", "y", "MAX", "COUNT", "3"]],
    [["HSET", "h", "f", "v"], ["HSET", "h", "f", "v"], ["HSETNX", "h", "f", "v"], ["HSETNX", "h", "g", "v"], ["HMSET", "h", "a", "1"], ["HINCRBY", "h", "n", "1"], ["HINCRBYFLOAT", "h", "n", "1.5"], ["HDEL", "h", "nope"], ["HDEL", "h", "f", "g", "a", "n"]],
    [["XADD", "x", "1-0", "f", "v"], ["XADD", "x", "MAXLEN", "1", "2-0", "f", "v"], ["XADD", "x", "MAXLEN", "5", "3-0", "f", "v"], ["XADD", "nope", "NOMKSTREAM", "1-0", "f", "v"], ["XDEL", "x", "9-9"], ["XDEL", "x", "2-0"], ["XTRIM", "x", "MAXLEN", "5"], ["XTRIM", "x", "MAXLEN", "0"], ["XSETID", "x", "10-0"]],
    [["XGROUP", "CREATE", "x", "g", "$", "MKSTREAM"], ["XGROUP", "CREATE", "x", "g", "$"], ["XGROUP", "CREATECONSUMER", "x", "g", "c"], ["XGROUP", "CREATECONSUMER", "x", "g", "c"], ["XADD", "x", "1-0", "f", "v"], ["XREADGROUP", "GROUP", "g", "d", "COUNT", "1", "STREAMS", "x", ">"], ["XREADGROUP", "GROUP", "g", "d", "STREAMS", "x", ">"], ["XCLAIM", "x", "g", "e", "0", "1-0"], ["XAUTOCLAIM", "x", "g", "f", "0", "0-0"], ["XGROUP", "SETID", "x", "g", "0"], ["XGROUP", "DELCONSUMER", "x", "g", "nope"], ["XGROUP", "DELCONSUMER", "x", "g", "c"], ["XGROUP", "DESTROY", "x", "g"], ["XGROUP", "DESTROY", "x", "g"]],
    [["PFADD", "p", "a"], ["PFADD", "p", "a"], ["PFADD", "q"], ["PFADD", "q"], ["PFMERGE", "r", "p"], ["PFMERGE", "r"], ["PFCOUNT", "p", "q"]],
    [["GEOADD", "g", "1", "1", "a"], ["GEOADD", "g", "1", "1", "a"], ["GEOSEARCHSTORE", "d", "g", "FROMLONLAT", "1", "1", "BYRADIUS", "10", "km"], ["GEOSEARCHSTORE", "d", "g", "FROMLONLAT", "50", "50", "BYRADIUS", "1", "km"], ["GEORADIUS", "g", "1", "1", "10", "km", "STORE", "e"], ["GEORADIUS", "g", "1", "1", "10", "km", "STOREDIST", "e"]],
    [["RPUSH", "l", "3", "1", "2"], ["SORT", "l", "STORE", "d"], ["SORT", "nope", "STORE", "d"], ["SORT", "l"]],
    [["SET", "k", "v"], ["DUMP", "k"]],
    [["SET", "k", "v"], ["PEXPIRE", "k", "1"], ["SLEEP"], ["GET", "k"], ["SET", "k", "v", "PX", "1"], ["SLEEP"], ["SET", "k", "w"], ["RPUSH", "l", "a"], ["PEXPIRE", "l", "1"], ["SLEEP"], ["RPUSH", "l", "b"]],
    [["MULTI"], ["SET", "a", "1"], ["RPUSH", "l", "a"], ["DEL", "a"], ["EXEC"]],
    [["EVAL", "redis.call('set', KEYS[1], 'v'); redis.call('rpush', KEYS[2], 'a'); redis.call('del', KEYS[1]); return 1", "2", "a", "l"]],
    [["RPUSH", "l", "a"], ["BLOCKED", "BLPOP", "q", "0"], ["RPUSH", "q", "a", "b"], ["UNBLOCKED"]],
    [["BLOCKED", "BLMOVE", "q", "r", "LEFT", "RIGHT", "0"], ["LPUSH", "q", "a"], ["UNBLOCKED"]],
    [["BLOCKED", "BZPOPMIN", "q", "0"], ["ZADD", "q", "1", "a"], ["UNBLOCKED"]],
    [["FLUSHALL"], ["SET", "a", "1"], ["SWAPDB", "0", "1"], ["FLUSHDB"]],
]


def random_command(rng, n):
    """One random command on a few keys per type (and any key, for type errors)."""
    pick = rng.choice
    s, l, t, z, h = (pick([f"{p}{i}" for i in range(3)]) for p in "sltzh")
    x = pick(["x0", "x1"])
    anyk = pick(["s0", "s1", "l0", "t0", "z0", "h0", "x0", "nope"])
    v = pick(["a", "b", "c", "1", "2", "10", ""])
    num = str(rng.randint(-2, 5))
    choices = [
        ["SET", s, v], ["SET", s, v, "EX", "100"], ["SET", s, v, "NX"], ["SET", s, v, "XX"], ["SET", s, v, "GET"],
        ["SET", s, v, "KEEPTTL"], ["SETNX", s, v], ["GETSET", s, v], ["GETDEL", s], ["GETEX", s, "EX", "100"],
        ["GETEX", s, "PERSIST"], ["MSET", s, v, pick(["s0", "s1"]), v], ["MSETNX", s, v, "s2", v], ["INCR", s],
        ["INCRBY", s, num], ["INCRBYFLOAT", s, "1.5"], ["APPEND", s, v], ["SETRANGE", s, num.lstrip("-"), v],
        ["SETBIT", s, "3", pick(["0", "1"])], ["BITFIELD", s, "SET", "u4", "0", "3"], ["BITOP", "OR", s, "s0", "s1"],
        ["PFADD", "p" + s, v], ["PFMERGE", "p" + s, "ps0"],
        ["DEL", anyk, s], ["UNLINK", anyk], ["EXPIRE", anyk, "100"], ["EXPIRE", anyk, "-1"], ["PERSIST", anyk],
        ["EXPIRE", anyk, "50", pick(["NX", "XX", "GT", "LT"])], ["RENAME", anyk, pick(["s0", "l1", "z2"])],
        ["RENAMENX", anyk, pick(["s0", "l1", "z2"])], ["COPY", anyk, pick(["s1", "t2"])],
        ["COPY", anyk, pick(["s1", "t2"]), "REPLACE"], ["COPY", anyk, "k", "DB", "1"], ["SORT", l, "STORE", pick(["l2", "s0"])],
        ["EXISTS", anyk], ["TYPE", anyk], ["GET", s],
        ["LPUSH", l, v], ["RPUSH", l, v, v], ["LPUSHX", l, v], ["RPUSHX", l, v], ["LPOP", l], ["RPOP", l, "2"],
        ["LINSERT", l, pick(["BEFORE", "AFTER"]), v, "x"], ["LSET", l, "0", v], ["LREM", l, "0", v],
        ["LTRIM", l, "0", num], ["LMOVE", l, pick(["l0", "l1"]), pick(["LEFT", "RIGHT"]), pick(["LEFT", "RIGHT"])],
        ["RPOPLPUSH", l, pick(["l0", "l2"])], ["LMPOP", "2", l, "l1", pick(["LEFT", "RIGHT"]), "COUNT", "2"],
        ["SADD", t, v, v], ["SREM", t, v], ["SPOP", t, "10"], ["SMOVE", t, pick(["t0", "t1"]), v],
        ["SINTERSTORE", t, "t0", "t1"], ["SUNIONSTORE", t, "t1", "t2"], ["SDIFFSTORE", t, "t2", "t0"],
        ["ZADD", z, num, v], ["ZADD", z, "INCR", num, v], ["ZADD", z, pick(["NX", "XX", "GT", "LT"]), num, v],
        ["ZINCRBY", z, num, v], ["ZREM", z, v], ["ZPOPMIN", z], ["ZPOPMAX", z, "2"], ["ZREMRANGEBYRANK", z, "0", "0"],
        ["ZREMRANGEBYSCORE", z, "0", num], ["ZUNIONSTORE", z, "2", "z0", "z1"], ["ZINTERSTORE", z, "2", "z1", "z2"],
        ["ZRANGESTORE", z, "z0", "0", num], ["ZMPOP", "2", z, "z1", pick(["MIN", "MAX"])],
        ["HSET", h, v, v], ["HSETNX", h, v, v], ["HINCRBY", h, "n", num], ["HINCRBYFLOAT", h, "f", "0.5"], ["HDEL", h, v, "n"],
        ["XADD", x, f"{n}-0", "f", v], ["XADD", x, "MAXLEN", "2", f"{n}-1", "f", v], ["XTRIM", x, "MAXLEN", "1"],
        ["XDEL", x, f"{max(n - 3, 0)}-0"], ["XGROUP", "CREATE", x, "g", "0", "MKSTREAM"],
        ["XGROUP", "CREATECONSUMER", x, "g", pick(["c", "d"])], ["XGROUP", "DELCONSUMER", x, "g", pick(["c", "d"])],
        ["XGROUP", "DESTROY", x, "g"], ["XREADGROUP", "GROUP", "g", pick(["c", "d", "e"]), "COUNT", "1", "STREAMS", x, ">"],
        ["XAUTOCLAIM", x, "g", pick(["c", "f"]), "0", "0-0", "COUNT", "1"],
        ["PEXPIRE", anyk, "1"], ["SET", s, v, "PX", "1"],
    ]
    return pick(choices)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis", type=int, default=6390)
    ap.add_argument("--tantalum", type=int, default=6391)
    ap.add_argument("--seeds", default="1,2,3")
    ap.add_argument("--steps", type=int, default=2000)
    ap.add_argument("--events", default="KEA")
    a = ap.parse_args()
    shown = 0
    current = [""]
    differ = 0
    total = 0
    replies_differ = 0

    def compare(sides, cases, label):
        nonlocal shown, differ, total, replies_differ
        blocked = [None, None]
        queued = False
        history = []
        for args in cases:
            current[0] = f"{label}: {' '.join(map(str, args))}"
            signal.alarm(20)
            if args == ["SLEEP"]:
                time.sleep(0.005)
                continue
            if args[0] == "BLOCKED":
                for i, side in enumerate(sides):
                    blocked[i] = Conn(side.port)
                    blocked[i].send(*args[1:])
                time.sleep(0.05)
                continue
            if args == ["UNBLOCKED"]:
                got = []
                for i, side in enumerate(sides):
                    blocked[i].read()
                    got.append(side.heard())
                results = [(None, g) for g in got]
            else:
                name = str(args[0]).upper()
                if name == "MULTI":
                    queued = True
                # Nothing is heard in a transaction until EXEC (the token would be queued).
                sync = not queued or name in ("EXEC", "DISCARD")
                if name in ("EXEC", "DISCARD"):
                    queued = False
                results = [side.run(args, sync) for side in sides]
            total += 1
            (rr, re_), (tr, te) = results
            history.append(args)
            # SPOP's members come in any order.
            if str(args[0]).upper() == "SPOP" and isinstance(rr, list) and isinstance(tr, list):
                rr, tr = sorted(rr), sorted(tr)
            if rr != tr and not str(args[0]).upper().startswith("DUMP"):
                replies_differ += 1
                if replies_differ <= 3:
                    print(f"{label}: replies first differ at {' '.join(map(str, args))}: "
                          f"redis {rr!r} tantalum {tr!r}; before it:")
                    for h in history[-12:-1]:
                        print(f"    {' '.join(map(str, h))}")
            if re_ != te:
                differ += 1
                if shown < 40:
                    shown += 1
                    print(f"{label}: {' '.join(map(str, args))}")
                    print(f"  redis:    {[e.decode(errors='replace') for e in re_]}")
                    print(f"  tantalum: {[e.decode(errors='replace') for e in te]}")
                    if rr != tr:
                        print(f"  replies: redis {rr!r} tantalum {tr!r}")

    signal.signal(signal.SIGALRM, lambda *_: sys.exit(f"stuck at {current[0]}"))
    for i, case in enumerate(SCRIPTED):
        sides = [Side(a.redis, a.events), Side(a.tantalum, a.events)]
        sides[0].port, sides[1].port = a.redis, a.tantalum
        compare(sides, case, f"case {i}")
    scripted = total
    for seed in [int(s) for s in a.seeds.split(",")]:
        rng = random.Random(seed)
        sides = [Side(a.redis, a.events), Side(a.tantalum, a.events)]
        sides[0].port, sides[1].port = a.redis, a.tantalum
        steps = []
        for n in range(1, a.steps + 1):
            r = rng.random()
            if r < 0.02:
                block = [random_command(rng, n), random_command(rng, n)]
                steps += [["MULTI"], *block, ["EXEC"]]
                # A key given a millisecond to live has gone by the next command.
                if any(c[-1] == "1" and c[0] in ("PEXPIRE", "SET") for c in block):
                    steps.append(["SLEEP"])
            elif r < 0.03:
                key = rng.choice(["l0", "l1"])
                steps.append(["EVAL", "redis.call('rpush', KEYS[1], 'a'); return redis.call('lpop', KEYS[1])", "1", key])
            else:
                command = random_command(rng, n)
                steps.append(command)
                if command[-1] == "1" and command[0] in ("PEXPIRE", "SET"):
                    steps.append(["SLEEP"])
        compare(sides, steps, f"seed {seed}")
    print(f"events {a.events}: {scripted} scripted and {total - scripted} random commands, "
          f"{differ} with different events, {replies_differ} with different replies")
    sys.exit(1 if differ else 0)


if __name__ == "__main__":
    main()
