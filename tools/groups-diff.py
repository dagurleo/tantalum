#!/usr/bin/env python3
"""Consumer groups: the same commands to Redis (on 6390) and Tantalum (on 6391),
replies compared, the times in them within a tolerance. Every argument and key
error in both protocols, then a random run.

    tools/groups-diff.py [SEED] [STEPS] [--only-fuzz] [--record FILE]

--record writes what has no time in its reply for tests/groups.rs, as
tests/data/groups-redis.txt was: seed 7, 1200 steps.
"""
import random, socket, sys, time, zlib

REDIS, TANTALUM = 6390, 6391
SEED = int(sys.argv[1]) if len(sys.argv) > 1 else 1
OPS = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
TOLERANCE_MS = 1500


class Conn:
    def __init__(self, port, resp3=False):
        self.s = socket.create_connection(("127.0.0.1", port))
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""
        self.raw = b""
        if resp3:
            self.call("HELLO", "3")

    def _fill(self):
        data = self.s.recv(1 << 20)
        if not data:
            raise EOFError("closed")
        self.buf += data

    def _line(self):
        while b"\r\n" not in self.buf:
            self._fill()
        line, self.buf = self.buf.split(b"\r\n", 1)
        self.raw += line + b"\r\n"
        return line

    def _exact(self, n):
        while len(self.buf) < n + 2:
            self._fill()
        data, self.buf = self.buf[:n], self.buf[n + 2:]
        self.raw += data + b"\r\n"
        return data

    def reply(self):
        self.raw = b""
        return self._reply()

    def _reply(self):
        line = self._line()
        t, rest = line[:1], line[1:]
        if t in b"+-":
            return (t.decode(), rest)
        if t == b":":
            return int(rest)
        if t == b"_":
            return ("null",)
        if t == b"$":
            n = int(rest)
            return ("null-bulk",) if n < 0 else self._exact(n)
        if t == b"*":
            n = int(rest)
            return ("null-array",) if n < 0 else [self._reply() for _ in range(n)]
        if t == b"%":
            return ("map", [(self._reply(), self._reply()) for _ in range(int(rest))])
        raise ValueError(line)

    def call(self, *args):
        args = [a if isinstance(a, bytes) else str(a).encode() for a in args]
        out = b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n%s\r\n" % (len(a), a) for a in args)
        self.s.sendall(out)
        return self.reply()


TIME_FIELDS = {b"seen-time", b"active-time", b"idle", b"inactive"}


def same(a, b, cmd):
    """Whether two replies match, with the times in them near each other."""
    name = cmd[0].upper()
    sub = cmd[1].upper() if len(cmd) > 1 and isinstance(cmd[1], str) else ""
    if name == "XPENDING" and isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        if a and isinstance(a[0], list) and len(a[0]) == 4:
            for x, y in zip(a, b):
                if not (isinstance(y, list) and len(y) == 4):
                    return False
                if x[:2] != y[:2] or x[3] != y[3] or abs(x[2] - y[2]) > TOLERANCE_MS:
                    return False
            return True
    if name == "XINFO":
        return same_info(a, b)
    return a == b


def near(x, y):
    if isinstance(x, int) and isinstance(y, int):
        if x == -1 or y == -1:
            return x == y
        return abs(x - y) <= TOLERANCE_MS
    return x == y


def same_info(a, b):
    if isinstance(a, tuple) and a and a[0] == "map" and isinstance(b, tuple) and b and b[0] == "map":
        a, b = flat(a), flat(b)
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return False
        i = 0
        while i < len(a):
            if a[i] != b[i] and not isinstance(a[i], (list, tuple)):
                return False
            if isinstance(a[i], bytes) and a[i] in TIME_FIELDS and i + 1 < len(a):
                if not near(a[i + 1], b[i + 1]):
                    return False
                i += 2
                continue
            if a[i] == b"pending" and i + 1 < len(a) and isinstance(a[i + 1], list):
                # Pending entries: [id, consumer, time, count] or [id, time, count].
                x, y = a[i + 1], b[i + 1]
                if not isinstance(y, list) or len(x) != len(y):
                    return False
                for p, q in zip(x, y):
                    t = len(p) - 2
                    if len(p) != len(q) or p[:t] != q[:t] or p[t + 1] != q[t + 1] or not near(p[t], q[t]):
                        return False
                i += 2
                continue
            if not same_info(a[i], b[i]):
                return False
            i += 1
        return True
    return a == b


def flat(m):
    out = []
    for k, v in m[1]:
        out += [k, v]
    return out


def cases_of_errors():
    # Each argument error in Redis' order: a stream `s` with a group `g` exists.
    return [
        ["XGROUP"], ["XGROUP", "FOO"], ["XGROUP", "HELP"], ["XGROUP", "HELP", "x"], ["xgroup", "help"],
        ["XGROUP", "CREATE"], ["XGROUP", "CREATE", "s"], ["XGROUP", "CREATE", "s", "g2"],
        ["XGROUP", "CREATE", "nokey", "g", "$"], ["XGROUP", "CREATE", "s", "g", "$"],
        ["XGROUP", "CREATE", "s", "g3", "bad"], ["XGROUP", "CREATE", "s", "g3", "$", "FOO"],
        ["XGROUP", "CREATE", "s", "g3", "$", "ENTRIESREAD"], ["XGROUP", "CREATE", "s", "g3", "$", "ENTRIESREAD", "x"],
        ["XGROUP", "CREATE", "s", "g3", "$", "ENTRIESREAD", "-2"],
        ["XGROUP", "CREATE", "s", "g4", "$", "MKSTREAM", "MKSTREAM", "MKSTREAM", "MKSTREAM"],
        ["XGROUP", "CREATE", "str", "g", "$"], ["XGROUP", "CREATE", "str", "g", "$", "MKSTREAM"],
        ["XGROUP", "CREATE", "newkey", "g", "bad", "MKSTREAM"], ["EXISTS", "newkey"],
        ["XGROUP", "CREATE", "newkey", "g", "$", "MKSTREAM", "MKSTREAM", "MKSTREAM", "MKSTREAM"], ["EXISTS", "newkey"],
        ["XGROUP", "SETID", "s", "g"], ["XGROUP", "SETID", "s", "nog", "$"], ["XGROUP", "SETID", "nokey", "g", "$"],
        ["XGROUP", "SETID", "s", "g", "bad"], ["XGROUP", "SETID", "s", "g", "$", "MKSTREAM"],
        ["XGROUP", "SETID", "s", "g", "$", "ENTRIESREAD", "1", "ENTRIESREAD", "2"],
        ["XGROUP", "SETID", "s", "g", "-"], ["XGROUP", "SETID", "s", "g", "+"], ["XINFO", "GROUPS", "s"],
        ["XGROUP", "SETID", "s", "g", "(1"], ["XGROUP", "SETID", "s", "g", "0"],
        ["XGROUP", "DESTROY", "s"], ["XGROUP", "DESTROY", "s", "g", "x"], ["XGROUP", "DESTROY", "nokey", "g"],
        ["XGROUP", "DESTROY", "s", "nog"], ["XGROUP", "DESTROY", "str", "g"],
        ["XGROUP", "CREATECONSUMER", "s", "g"], ["XGROUP", "CREATECONSUMER", "s", "g", "c", "x"],
        ["XGROUP", "CREATECONSUMER", "s", "nog", "c"], ["XGROUP", "CREATECONSUMER", "nokey", "g", "c"],
        ["XGROUP", "CREATECONSUMER", "s", "g", "c"], ["XGROUP", "CREATECONSUMER", "s", "g", "c"],
        ["XGROUP", "DELCONSUMER", "s", "g"], ["XGROUP", "DELCONSUMER", "s", "nog", "c"],
        ["XGROUP", "DELCONSUMER", "s", "g", "nobody"], ["XGROUP", "DELCONSUMER", "s", "g", "c"],
        ["XGROUP", "DELCONSUMER", "nokey", "g", "c"],
        ["XGROUP", "CREATE", b"s", b"new\r\nline", "$"], ["XGROUP", "SETID", "s", b"no\r\ngroup\x00cut", "$"],
        ["XREADGROUP"], ["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s"],
        ["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">", "x"],
        ["XREADGROUP", "GROUP", "g", "STREAMS", "s", ">", "x", "y"],
        ["XREADGROUP", "COUNT", "1", "STREAMS", "s", ">", "x"], ["XREADGROUP", "FOO", "g", "c", "STREAMS", "s", ">"],
        ["XREADGROUP", "GROUP", "g", "c", "COUNT", "x", "STREAMS", "s", ">"],
        ["XREADGROUP", "GROUP", "g", "c", "BLOCK", "x", "STREAMS", "s", ">"],
        ["XREADGROUP", "GROUP", "g", "c", "BLOCK", "-1", "STREAMS", "s", ">"],
        ["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", "$"], ["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", "bad"],
        ["XREADGROUP", "GROUP", "nog", "c", "STREAMS", "s", ">"], ["XREADGROUP", "GROUP", "g", "c", "STREAMS", "nokey", ">"],
        ["XREADGROUP", "GROUP", "g", "c", "STREAMS", "str", ">"],
        ["XREADGROUP", "GROUP", b"n\x00g", b"c", "STREAMS", b"k\x00y", ">"],
        ["XREADGROUP", "GROUP", "g", "c", "NOACK", "NOACK", "COUNT", "-5", "STREAMS", "s", "0"],
        ["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", "nokey", ">", ">"],
        ["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", "str", ">", ">"],
        ["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", "s", "bad", ">"],
        ["XREAD", "GROUP", "g", "c", "STREAMS", "s", ">"], ["XREAD", "NOACK", "STREAMS", "s", ">"], ["XREAD", "STREAMS", "s", ">"],
        ["XACK"], ["XACK", "s", "g"], ["XACK", "nokey", "g", "bad"], ["XACK", "s", "nog", "bad"], ["XACK", "s", "g", "bad"],
        ["XACK", "s", "g", "1-1", "bad"], ["XACK", "str", "g", "1-1"], ["XACK", "s", "g", "1-1", "1-1"],
        ["XPENDING"], ["XPENDING", "s"], ["XPENDING", "s", "g", "x"], ["XPENDING", "s", "g", "x", "y"],
        ["XPENDING", "s", "g", "-", "+", "x"], ["XPENDING", "s", "g", "bad", "+", "1"], ["XPENDING", "s", "g", "-", "bad", "1"],
        ["XPENDING", "s", "g", "(18446744073709551615-18446744073709551615", "+", "1"],
        ["XPENDING", "s", "g", "-", "(0-0", "1"], ["XPENDING", "s", "g", "IDLE", "x", "-", "+", "1"],
        ["XPENDING", "s", "g", "IDLE", "1", "-", "+"], ["XPENDING", "s", "g", "IDLE", "1", "-"],
        ["XPENDING", "s", "g", "-", "+", "-5"], ["XPENDING", "s", "g", "-", "+", "1", "c", "extra"],
        ["XPENDING", "s", "g", "IDLE", "1", "-", "+", "1", "c", "extra"],
        ["XPENDING", "s", "g", "IDLE", "1", "-", "+", "1", "c", "extra", "more"],
        ["XPENDING", "nokey", "g"], ["XPENDING", "s", "nog"], ["XPENDING", "str", "g"], ["XPENDING", "nokey", "g", "-", "+", "1"],
        ["XPENDING", "nokey", "g", "bad", "+", "1"], ["XPENDING", "s", "g", "5", "3", "10"],
        ["XCLAIM"], ["XCLAIM", "s", "g", "c", "0"], ["XCLAIM", "nokey", "g", "c", "x", "bad"], ["XCLAIM", "s", "nog", "c", "x", "bad"],
        ["XCLAIM", "str", "g", "c", "0", "1-1"], ["XCLAIM", "s", "g", "c", "x", "1-1"], ["XCLAIM", "s", "g", "c", "0", "bad"],
        ["XCLAIM", "s", "g", "c", "0", "1-1", "bad"], ["XCLAIM", "s", "g", "c", "0", "1-1", "IDLE"], ["XCLAIM", "s", "g", "c", "0", "1-1", "IDLE", "x"],
        ["XCLAIM", "s", "g", "c", "0", "1-1", "TIME", "x"], ["XCLAIM", "s", "g", "c", "0", "1-1", "RETRYCOUNT", "x"],
        ["XCLAIM", "s", "g", "c", "0", "1-1", "LASTID", "bad"], ["XCLAIM", "s", "g", "c", "0", "1-1", "JUSTID", "2-2"],
        ["XCLAIM", "s", "g", "c", "0", "FORCE"], ["XCLAIM", "s", "g", "c", "0", b"op\x00tion"],
        ["XAUTOCLAIM"], ["XAUTOCLAIM", "s", "g", "c", "0"], ["XAUTOCLAIM", "s", "g", "c", "x", "0"], ["XAUTOCLAIM", "s", "g", "c", "0", "bad"],
        ["XAUTOCLAIM", "s", "g", "c", "0", "(18446744073709551615-18446744073709551615"], ["XAUTOCLAIM", "s", "g", "c", "0", "0", "COUNT"],
        ["XAUTOCLAIM", "s", "g", "c", "0", "0", "COUNT", "0"], ["XAUTOCLAIM", "s", "g", "c", "0", "0", "COUNT", "x"], ["XAUTOCLAIM", "s", "g", "c", "0", "0", "COUNT", "-1"],
        ["XAUTOCLAIM", "s", "g", "c", "0", "0", "COUNT", "9223372036854775807"], ["XAUTOCLAIM", "s", "g", "c", "0", "0", "COUNT", "576460752303423488"],
        ["XAUTOCLAIM", "s", "g", "c", "0", "0", "COUNT", "576460752303423487"],
        ["XAUTOCLAIM", "s", "g", "c", "0", "0", "FOO"], ["XAUTOCLAIM", "nokey", "g", "c", "0", "0"], ["XAUTOCLAIM", "s", "nog", "c", "0", "0"], ["XAUTOCLAIM", "str", "g", "c", "0", "0"],
        ["XAUTOCLAIM", "nokey", "g", "c", "x", "0"], ["XAUTOCLAIM", "s", "g", "c", "0", "-"],  ["XAUTOCLAIM", "s", "g", "c", "0", "+"],
        ["XINFO"], ["XINFO", "FOO"], ["XINFO", "HELP"], ["XINFO", "HELP", "x"], ["xinfo", "help"], ["XINFO", "STREAM"], ["XINFO", "GROUPS"], ["XINFO", "CONSUMERS", "s"],
        ["XINFO", "GROUPS", "s", "x"], ["XINFO", "CONSUMERS", "s", "g", "x"], ["XINFO", "GROUPS", "nokey"], ["XINFO", "GROUPS", "str"], ["XINFO", "STREAM", "nokey"], ["XINFO", "STREAM", "str"],
        ["XINFO", "CONSUMERS", "nokey", "g"], ["XINFO", "CONSUMERS", "s", "nog"], ["XINFO", "CONSUMERS", "str", "g"],
        ["XINFO", "STREAM", "s", "FOO"], ["XINFO", "STREAM", "s", "FULL", "x"], ["XINFO", "STREAM", "s", "FULL", "FOO", "1"], ["XINFO", "STREAM", "s", "FULL", "COUNT", "x"],
        ["XINFO", "STREAM", "s", "FULL", "COUNT", "1", "x"], ["XINFO", "STREAM", "s", "FULL", "COUNT", "-1"],
        ["XINFO", "STREAM", "s", "FULL", "COUNT", "0"], ["xinfo", "stream", "s", "full", "count", "1"],
        ["XINFO", "STREAM", "emptied"], ["XINFO", "STREAM", "emptied", "FULL"], ["XINFO", "GROUPS", "emptied"],
        ["XSETID"], ["XSETID", "s"], ["XSETID", "s", "bad"], ["XSETID", "nokey", "1-1"], ["XSETID", "str", "1-1"],
        ["XSETID", "s", "1-1", "FOO"], ["XSETID", "s", "1-1", "ENTRIESADDED"], ["XSETID", "s", "1-1", "ENTRIESADDED", "x"], ["XSETID", "s", "1-1", "ENTRIESADDED", "-1"],
        ["XSETID", "s", "1-1", "MAXDELETEDID"], ["XSETID", "s", "1-1", "MAXDELETEDID", "bad"], ["XSETID", "s", "1-1", "MAXDELETEDID", "2-1"], ["XSETID", "nokey", "1-1", "MAXDELETEDID", "2-1"],
        ["XSETID", "s", "1-1"], ["XSETID", "s", "9999-1", "ENTRIESADDED", "0"], ["XSETID", "s", "9999-1", "ENTRIESADDED", "100000"],
        ["XSETID", "emptied", "1-0"], ["XSETID", "emptied", "1-0", "MAXDELETEDID", "1-0", "ENTRIESADDED", "0"],
        ["XINFO", "STREAM", "emptied", "FULL"],
    ]


def encode(arg):
    text = arg.decode() if isinstance(arg, bytes) else str(arg)
    if len(text) > 1 and len(set(text)) == 1:
        return f"~{text[0]}{len(text)}"
    return text.replace("\r", "\\r").replace("\n", "\\n").replace("\0", "\\0")


def holds_times(cmd, reply):
    """Whether a reply has a time in it, so it can't be recorded."""
    if isinstance(reply, tuple) and reply[0] == "-":
        return False
    word = str(cmd[0]).upper()
    if word == "XPENDING" and len(cmd) > 3:
        return bool(reply)
    if word == "XINFO":
        sub = str(cmd[1]).upper()
        if sub == "CONSUMERS":
            return bool(reply)
        if sub == "STREAM":
            return any(str(c).upper() == "FULL" for c in cmd)
    return False


class Run:
    def __init__(self):
        self.record = None
        self.recorded_resp3 = False
        self.conns = [(Conn(REDIS), Conn(TANTALUM)), (Conn(REDIS, True), Conn(TANTALUM, True))]
        self.n = 0
        self.bad = 0
        self.seen = {}
        self.deep = {}

    def both(self, *cmd, resp3=False):
        r, t = self.conns[1 if resp3 else 0]
        a, b = r.call(*cmd), t.call(*cmd)
        self.n += 1
        if self.record and not holds_times(cmd, a):
            if resp3 != self.recorded_resp3:
                self.record.write(f"-\tHELLO\t{3 if resp3 else 2}\n")
                self.recorded_resp3 = resp3
            line = "\t".join(encode(c) for c in cmd)
            self.record.write(f"{zlib.crc32(r.raw):08x}:{len(r.raw)}\t{line}\n")
        word = str(cmd[0]).upper()
        kind = "err" if isinstance(a, tuple) and a[0] == "-" else "ok"
        self.seen[(word, kind)] = self.seen.get((word, kind), 0) + 1
        def note(name):
            self.deep[name] = self.deep.get(name, 0) + 1
        if word == "XREADGROUP" and kind == "ok" and a not in (("null-array",), ("null",)):
            note("read-served")
        if word == "XAUTOCLAIM" and kind == "ok":
            if a[1]:
                note("autoclaim-claimed")
            if a[2]:
                note("autoclaim-deleted")
            if a[0] != b"0-0":
                note("autoclaim-cursor")
        if word == "XCLAIM" and kind == "ok" and a:
            note("claim-claimed")
        if word == "XPENDING" and kind == "ok" and isinstance(a, list) and a:
            note("pending-rows")
        if word == "XACK" and isinstance(a, int) and a > 0:
            note("acked")
        if word == "XINFO" and len(cmd) > 1 and str(cmd[1]) == "GROUPS" and kind == "ok" and a:
            text = repr(a)
            note("groups-info")
            if "null" in text:
                note("lag-or-read-null")
        if word == "XINFO" and len(cmd) > 2 and str(cmd[1]) == "STREAM" and kind == "ok":
            items = flat(a) if isinstance(a, tuple) else a
            if isinstance(items, list) and b"radix-tree-keys" in items:
                keys = items[items.index(b"radix-tree-keys") + 1]
                self.deep["max-chunks"] = max(self.deep.get("max-chunks", 0), keys)
        if not same(a, b, [c.decode("latin1") if isinstance(c, bytes) else str(c) for c in cmd]):
            self.bad += 1
            if self.bad <= 25:
                print("DIFF", cmd, "resp3" if resp3 else "")
                print("  redis   ", repr(a)[:900])
                print("  tantalum", repr(b)[:900])
        return a


def static_cases(run):
    run.both("FLUSHALL")
    run.both("SET", "str", "x")
    run.both("XADD", "s", "1-1", "f", "v")
    run.both("XADD", "s", "2-1", "f", "v")
    run.both("XGROUP", "CREATE", "s", "g", "0")
    run.both("XADD", "emptied", "1-1", "f", "v")
    run.both("XDEL", "emptied", "1-1")
    for resp3 in (False, True):
        for case in cases_of_errors():
            run.both(*case, resp3=resp3)
        run.both("XGROUP", "DESTROY", "newkey", "g")
        run.both("DEL", "newkey")
        run.both("XGROUP", "DESTROY", "s", "g3")
        run.both("XGROUP", "DESTROY", "s", "g4")
        run.both("XGROUP", "DESTROY", "s", b"new\r\nline")
        run.both("XGROUP", "SETID", "s", "g", "0")


def fuzz(run, rng):
    run.both("FLUSHALL")
    keys = ["a", "b", "c"]
    groups = ["g1"] * 6 + ["g2", "g\xff"]
    for k in keys:
        for name in ("g1", "g2"):
            run.both("XGROUP", "CREATE", k, name, "0", "MKSTREAM")
    consumers = ["alice", "bob", "carol", "d"]
    clock = {k: 1000 for k in keys}
    added = {k: [] for k in keys}

    def some_id(k):
        if added[k] and rng.random() < 0.8:
            return rng.choice(added[k])
        return f"{rng.randrange(900, clock[k] + 50)}-{rng.randrange(3)}"

    def add(k, big=False):
        clock[k] += rng.randrange(1, 4)
        id = f"{clock[k]}-{rng.randrange(3)}"
        n = rng.randrange(1, 4)
        size = rng.choice([1, 8, 40, 200]) if not big else rng.choice([1000, 3000, 5000])
        fields = []
        for i in range(n):
            fields += [f"f{i}" if rng.random() < 0.8 else f"x{rng.randrange(9)}", "v" * rng.randrange(1, size + 1)]
        r = run.both("XADD", k, id, *fields)
        if isinstance(r, bytes):
            added[k].append(r.decode())

    for step in range(OPS):
        k = rng.choice(keys)
        g = rng.choice(groups)
        c = rng.choice(consumers)
        resp3 = rng.random() < 0.25
        roll = rng.randrange(100)
        if roll < 22:
            for _ in range(rng.choice([1, 1, 1, 2, 5, 40, 130])):
                add(k, rng.random() < 0.03)
        elif roll < 30:
            start = rng.choice(["$", "0", "0-0", some_id(k)])
            opts = []
            if rng.random() < 0.5:
                opts.append("MKSTREAM")
            if rng.random() < 0.2:
                opts += ["ENTRIESREAD", rng.choice([-1, 0, 3, 100])]
            run.both("XGROUP", "CREATE", k, g, start, *opts, resp3=resp3)
        elif roll < 33:
            opts = ["ENTRIESREAD", rng.choice([-1, 0, 3, 100])] if rng.random() < 0.3 else []
            run.both("XGROUP", "SETID", k, g, rng.choice(["$", "0", some_id(k), "-", "+"]), *opts)
        elif roll < 34:
            if rng.random() < 0.3:
                run.both("XGROUP", "DESTROY", k, g)
        elif roll < 36:
            run.both("XGROUP", "CREATECONSUMER", k, g, c)
        elif roll < 37:
            run.both("XGROUP", "DELCONSUMER", k, g, c)
        elif roll < 55:
            opts = []
            if rng.random() < 0.6:
                opts += ["COUNT", rng.choice([0, 1, 2, 5, 50, -1])]
            if rng.random() < 0.15:
                opts.append("NOACK")
            many = rng.random() < 0.2
            ks = rng.sample(keys, 2) if many else [k]
            ids = [rng.choice([">", ">", ">", "0", "0-0", some_id(x)]) for x in ks]
            run.both("XREADGROUP", "GROUP", g, c, *opts, "STREAMS", *ks, *ids, resp3=resp3)
        elif roll < 62:
            ids = [some_id(k) for _ in range(rng.choice([1, 1, 2, 5]))]
            run.both("XACK", k, g, *ids, resp3=resp3)
        elif roll < 68:
            if rng.random() < 0.4:
                run.both("XPENDING", k, g, resp3=resp3)
            else:
                opts = []
                if rng.random() < 0.3:
                    opts += ["IDLE", rng.choice([3600000, -1])]
                start = rng.choice(["-", some_id(k), "(" + some_id(k)])
                end = rng.choice(["+", some_id(k), "(" + some_id(k)])
                who = [c] if rng.random() < 0.4 else []
                run.both("XPENDING", k, g, *opts, start, end, rng.choice([0, 1, 3, 100, -1]), *who, resp3=resp3)
        elif roll < 75:
            ids = [some_id(k) for _ in range(rng.choice([1, 1, 2, 5]))]
            opts = []
            if rng.random() < 0.25:
                opts += ["IDLE", rng.choice([0, 7200000, 99999999999999])]
            if rng.random() < 0.15:
                opts += ["TIME", rng.choice([1000, 99999999999999, -5])]
            if rng.random() < 0.25:
                opts += ["RETRYCOUNT", rng.choice([0, 5])]
            if rng.random() < 0.2:
                opts.append("FORCE")
            if rng.random() < 0.3:
                opts.append("JUSTID")
            if rng.random() < 0.1:
                opts += ["LASTID", some_id(k)]
            run.both("XCLAIM", k, g, c, rng.choice([0, 0, 3600000, -1]), *ids, *opts, resp3=resp3)
        elif roll < 80:
            opts = []
            if rng.random() < 0.6:
                opts += ["COUNT", rng.choice([1, 2, 5, 100])]
            if rng.random() < 0.3:
                opts.append("JUSTID")
            start = rng.choice(["0", "0-0", "-", some_id(k), "(" + some_id(k)])
            run.both("XAUTOCLAIM", k, g, c, rng.choice([0, 0, 3600000, -1]), start, *opts, resp3=resp3)
        elif roll < 88:
            pick = rng.randrange(5)
            if pick == 0:
                run.both("XINFO", "STREAM", k, resp3=resp3)
            elif pick == 1:
                run.both("XINFO", "STREAM", k, "FULL", resp3=resp3)
            elif pick == 2:
                run.both("XINFO", "STREAM", k, "FULL", "COUNT", rng.choice([0, 1, 3]), resp3=resp3)
            elif pick == 3:
                run.both("XINFO", "GROUPS", k, resp3=resp3)
            else:
                run.both("XINFO", "CONSUMERS", k, g, resp3=resp3)
        elif roll < 92:
            ids = [some_id(k) for _ in range(rng.choice([1, 1, 2, 8]))]
            run.both("XDEL", k, *ids)
            added[k] = [i for i in added[k] if i not in ids]
        elif roll < 94:
            how = rng.choice(["MAXLEN", "MINID"])
            arg = rng.randrange(0, 200) if how == "MAXLEN" else some_id(k)
            approx = ["~"] if rng.random() < 0.5 else []
            run.both("XTRIM", k, how, *approx, arg)
        elif roll < 96:
            opts = []
            if rng.random() < 0.3:
                opts += ["ENTRIESADDED", rng.choice([0, 1000, 100000])]
            if rng.random() < 0.3:
                opts += ["MAXDELETEDID", some_id(k)]
            r = run.both("XSETID", k, rng.choice([f"{clock[k] + 100}-0", some_id(k)]), *opts)
            clock[k] += 101
        elif roll < 97:
            if rng.random() < 0.2:
                other = rng.choice(keys)
                run.both("COPY", k, other, "REPLACE")
                clock[other] = max(clock[other], clock[k])
        elif roll < 98:
            if rng.random() < 0.2:
                other = rng.choice(keys)
                run.both("RENAME", k, other)
                clock[other] = max(clock[other], clock[k])
        elif roll < 99:
            if rng.random() < 0.2:
                run.both("DEL", k)
                for name in ("g1", "g2"):
                    run.both("XGROUP", "CREATE", k, name, "0", "MKSTREAM")
        elif rng.random() < 0.2:
            run.both("SET", k, "text")
            run.both("XADD", k, "1-1", "f", "v")
            run.both("DEL", k)
            for name in ("g1", "g2"):
                run.both("XGROUP", "CREATE", k, name, "0", "MKSTREAM")
        if step % 400 == 0:
            for x in keys:
                for grp in groups:
                    run.both("XINFO", "CONSUMERS", x, grp)
                run.both("XINFO", "STREAM", x, "FULL", "COUNT", 0)


def main():
    run = Run()
    if "--record" in sys.argv:
        run.record = open(sys.argv[sys.argv.index("--record") + 1], "w")
    if "--only-fuzz" not in sys.argv:
        static_cases(run)
        print(f"static done: {run.n} commands, {run.bad} differences", flush=True)
    rng = random.Random(SEED)
    fuzz(run, rng)
    if run.record:
        run.record.close()
    print(f"seed {SEED}: {run.n} commands, {run.bad} differences")
    print("coverage:", " ".join(f"{w}:{k}={n}" for (w, k), n in sorted(run.seen.items())))
    print("deep:", " ".join(f"{k}={n}" for k, n in sorted(run.deep.items())))
    sys.exit(1 if run.bad else 0)


main()
