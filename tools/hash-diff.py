#!/usr/bin/env python3
"""Hashes of every size: the same commands to Redis (on 6390) and Tantalum (on 6391).
Replies must match exactly while Redis keeps a hash packed, as it then replies in the
order added. Once it has a hash table, its order is its own, so the same fields and
values must come back in any order. Random replies are checked to be from the hash.

    tools/hash-diff.py [SEED] [STEPS] [--record FILE]

--record writes what's the same on any run for tests/hash_sizes.rs, as
tests/data/hashes-redis.txt was: seed 7, 2500 steps.
"""
import random, socket, sys, zlib

REDIS, TANTALUM = 6390, 6391
SEED = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 1
STEPS = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].isdigit() else 8000


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

    def _reply(self):
        line = self._line()
        t, rest = line[:1], line[1:]
        if t in b"+-":
            return (t.decode(), rest)
        if t == b":":
            return int(rest)
        if t == b",":
            return ("double", rest)
        if t == b"_":
            return None
        if t == b"$":
            n = int(rest)
            return None if n < 0 else self._exact(n)
        if t == b"*":
            n = int(rest)
            return None if n < 0 else [self._reply() for _ in range(n)]
        if t == b"%":
            return ("map", [(self._reply(), self._reply()) for _ in range(int(rest))])
        raise ValueError(line)

    def call(self, *args):
        self.raw = b""
        args = [a if isinstance(a, bytes) else str(a).encode() for a in args]
        self.s.sendall(b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n%s\r\n" % (len(a), a) for a in args))
        return self._reply()


def encode(arg):
    text = arg.decode("latin1") if isinstance(arg, bytes) else str(arg)
    if len(text) > 1 and len(set(text)) == 1:
        return f"~{text[0]}{len(text)}"
    return text


def as_pairs(reply):
    if isinstance(reply, tuple) and reply[0] == "map":
        return sorted(reply[1])
    return sorted(zip(reply[::2], reply[1::2]))


class Run:
    def __init__(self):
        self.conns = {False: (Conn(REDIS), Conn(TANTALUM)), True: (Conn(REDIS, True), Conn(TANTALUM, True))}
        self.n = self.bad = 0
        self.seen = {}
        self.deep = {}
        self.record = None
        self.recorded_resp3 = False

    def note(self, name):
        self.deep[name] = self.deep.get(name, 0) + 1

    def fail(self, cmd, a, b, resp3):
        self.bad += 1
        if self.bad <= 20:
            print("DIFF", [encode(c)[:40] for c in cmd], "resp3" if resp3 else "")
            print("  redis   ", repr(a)[:500])
            print("  tantalum", repr(b)[:500])

    def both(self, *cmd, resp3=False):
        """The two replies, each compared as `compare` says; exact unless told."""
        r, t = self.conns[resp3]
        a, b = r.call(*cmd), t.call(*cmd)
        self.n += 1
        word = str(cmd[0]).upper()
        kind = "err" if isinstance(a, tuple) and a[0] == "-" else "ok"
        self.seen[(word, kind)] = self.seen.get((word, kind), 0) + 1
        return a, b, r.raw

    def write_record(self, cmd, raw, resp3):
        if not self.record:
            return
        if resp3 != self.recorded_resp3:
            self.record.write(f"-\tHELLO\t{3 if resp3 else 2}\n")
            self.recorded_resp3 = resp3
        line = "\t".join(encode(c) for c in cmd)
        self.record.write(f"{zlib.crc32(raw):08x}:{len(raw)}\t{line}\n")

    def exact(self, *cmd, resp3=False):
        a, b, raw = self.both(*cmd, resp3=resp3)
        if a != b:
            self.fail(cmd, a, b, resp3)
        self.write_record(cmd, raw, resp3)
        return a

    def setup(self, *cmd):
        """To both, not compared, but recorded: a replay has to get there too."""
        r, t = self.conns[False]
        r.call(*cmd)
        t.call(*cmd)
        self.n += 1
        if self.record:
            if self.recorded_resp3:
                self.record.write("-\tHELLO\t2\n")
                self.recorded_resp3 = False
            self.record.write("-\t" + "\t".join(encode(c) for c in cmd) + "\n")

    def encoding(self, key):
        return self.conns[False][0].call("OBJECT", "ENCODING", key)

    def whole(self, *cmd, key, shape, resp3=False):
        """HGETALL, HKEYS, HVALS: exact while Redis has a listpack, else as a set."""
        packed = self.encoding(key) == b"listpack"
        a, b, raw = self.both(*cmd, resp3=resp3)
        if packed:
            self.note("whole-in-order")
            if a != b:
                self.fail(cmd, a, b, resp3)
            self.write_record(cmd, raw, resp3)
        else:
            self.note("whole-any-order")
            same = shape(a) == shape(b) if not isinstance(a, tuple) or a[0] == "map" else a == b
            if not same:
                self.fail(cmd, a, b, resp3)


def fuzz(run, rng):
    run.exact("FLUSHALL")
    # "ordered" stays within Redis' limits, so it replies in the order added, while
    # past 8 KB it's big here: where a big hash must keep that order, removals too.
    keys = ["small", "mid", "big", "ordered", "ordered"]
    # How many fields each tends to, across each limit, and how long its values get.
    room = {"small": 20, "mid": 700, "big": 3000, "ordered": 480}

    def field(key):
        return f"f{rng.randrange(room[key])}"

    def value(key):
        if key == "ordered":
            return "z" * rng.randrange(0, 65)
        pick = rng.randrange(100)
        if pick < 70:
            return "v" * rng.randrange(0, 12)
        if pick < 92:
            return "w" * rng.randrange(40, 70)
        if pick < 99:
            return "x" * rng.randrange(100, 400)
        return "y" * rng.randrange(3000, 9000)

    def number():
        return str(rng.randrange(-1000, 1000))

    for step in range(STEPS):
        key = rng.choice(keys)
        resp3 = rng.random() < 0.2
        roll = rng.randrange(100)
        if roll < 30:
            pairs = []
            for _ in range(rng.choice([1, 1, 1, 2, 5, 20])):
                pairs += [field(key), value(key)]
            run.exact("HSET", key, *pairs, resp3=resp3)
        elif roll < 34:
            run.exact("HMSET", key, field(key), value(key), resp3=resp3)
        elif roll < 38:
            run.exact("HSETNX", key, field(key), value(key), resp3=resp3)
        elif roll < 44:
            run.exact("HDEL", key, *[field(key) for _ in range(rng.choice([1, 1, 3, 10]))], resp3=resp3)
        elif roll < 56:
            run.exact("HGET", key, field(key), resp3=resp3)
        elif roll < 60:
            run.exact("HMGET", key, *[field(key) for _ in range(rng.choice([1, 3, 12]))], resp3=resp3)
        elif roll < 65:
            f = "n" + str(rng.randrange(8))
            run.exact("HINCRBY", key, f, number(), resp3=resp3)
        elif roll < 67:
            f = "m" + str(rng.randrange(4))
            run.exact("HINCRBYFLOAT", key, f, f"{rng.randrange(1, 100)}.5", resp3=resp3)
        elif roll < 72:
            run.exact("HEXISTS", key, field(key), resp3=resp3)
        elif roll < 75:
            run.exact("HSTRLEN", key, field(key), resp3=resp3)
        elif roll < 79:
            run.exact("HLEN", key, resp3=resp3)
            run.exact("TYPE", key)
        elif roll < 83:
            run.whole("HGETALL", key, key=key, shape=as_pairs, resp3=resp3)
        elif roll < 85:
            run.whole("HKEYS", key, key=key, shape=lambda r: sorted(r) if isinstance(r, list) else r, resp3=resp3)
        elif roll < 87:
            run.whole("HVALS", key, key=key, shape=lambda r: sorted(r) if isinstance(r, list) else r, resp3=resp3)
        elif roll < 91:
            scan(run, key, resp3, rng)
        elif roll < 94:
            sample(run, key, resp3, rng)
        elif roll < 95:
            # What a hash is held as, while it's only grown: a shrunk one's past isn't
            # known here, as in the README's gaps.
            pass
        elif roll < 97:
            other = "copy-of-" + key
            run.exact("COPY", key, other, "REPLACE")
            run.exact("HLEN", other)
            run.exact("DEL", other)
        elif roll < 98 and rng.random() < 0.2:
            run.exact("RENAME", key, key + "-moved")
            run.exact("RENAME", key + "-moved", key)
        elif rng.random() < 0.05:
            run.exact("DEL", key)
        if step % 400 == 0:
            for k in dict.fromkeys(keys):
                run.exact("HLEN", k)
                run.whole("HGETALL", k, key=k, shape=as_pairs)
                encoding_when_built_up(run, k)
    for k in dict.fromkeys(keys):
        n = run.exact("HLEN", k)
        run.deep[f"final-{k}"] = n


def encoding_when_built_up(run, key):
    # A copy built from what the key holds now has no past on either side.
    r = run.conns[False][0]
    pairs = r.call("HGETALL", key)
    built = "built-" + key
    run.setup("DEL", built)
    for i in range(0, len(pairs), 200):
        run.setup("HSET", built, *pairs[i:i + 200])
    if pairs:
        # Not recorded: Redis' order past its limits decides which go in first, which
        # doesn't change what they come to, but a replay's values can't be known here.
        a, b, _ = run.both("OBJECT", "ENCODING", built)
        if a != b:
            run.fail(["OBJECT", "ENCODING", built], a, b, False)
    run.setup("DEL", built)


def scan(run, key, resp3, rng):
    """A whole HSCAN on each side: the same fields and values, each at least once."""
    r, t = run.conns[resp3]
    packed = run.encoding(key) == b"listpack"
    count = rng.choice([1, 10, 100, 1000])
    # (Not NOVALUES, which Redis has from 7.4 and this one, 7.2, refuses.)
    novalues = False
    args = ["COUNT", count]

    def everything(conn):
        cursor, found, rounds = b"0", [], 0
        while True:
            reply = conn.call("HSCAN", key, cursor, *args)
            run.n += 1
            if isinstance(reply, tuple):
                return reply
            cursor, page = reply
            found += page
            rounds += 1
            if cursor == b"0":
                return found, rounds

    a, b = everything(r), everything(t)
    if isinstance(a, tuple) and a[0] == "-" or isinstance(b, tuple) and b[0] == "-":
        if a != b:
            run.fail(["HSCAN", key, *args], a, b, resp3)
        return
    if packed:
        run.note("scan-whole")
        if a != b or a[1] != 1:
            run.fail(["HSCAN", key, *args], a, b, resp3)
        return
    run.note("scan-paged")
    per = 1 if novalues else 2
    shape = lambda found: set(tuple(found[i:i + per]) for i in range(0, len(found), per))
    if shape(a[0]) != shape(b[0]):
        run.fail(["HSCAN", key, *args], len(a[0]), len(b[0]), resp3)


def sample(run, key, resp3, rng):
    """HRANDFIELD is chance: right count, fields from the hash, distinct if asked."""
    r, t = run.conns[False]
    pairs = r.call("HGETALL", key)
    if isinstance(pairs, tuple):
        return
    fields = dict(zip(pairs[::2], pairs[1::2]))
    count = rng.choice([1, 3, 50, 2000, -1, -7, -50])
    a, b = r.call("HRANDFIELD", key, count), t.call("HRANDFIELD", key, count)
    run.n += 2
    if len(a) != len(b) or any(f not in fields for f in b):
        run.fail(["HRANDFIELD", key, count], a[:5], b[:5], False)
    if count > 0 and len(set(b)) != len(b):
        run.fail(["HRANDFIELD distinct", key, count], len(b), len(set(b)), False)
    w = t.call("HRANDFIELD", key, count, "WITHVALUES")
    run.n += 1
    if len(w) != len(a) * 2 or any(fields.get(w[i]) != w[i + 1] for i in range(0, len(w), 2)):
        run.fail(["HRANDFIELD WITHVALUES", key, count], len(a), len(w), False)
    one = t.call("HRANDFIELD", key)
    run.n += 1
    if (one is None) != (not fields) or (one is not None and one not in fields):
        run.fail(["HRANDFIELD", key], None, one, False)
    run.note("sampled")


def limits(run):
    """Each way across the limits, step by step, order and encoding included."""
    run.exact("FLUSHALL")
    for n in (511, 512, 513):
        key = f"count{n}"
        for i in range(n):
            run.exact("HSET", key, f"f{i}", "v")
        run.exact("OBJECT", "ENCODING", key)
        run.whole("HGETALL", key, key=key, shape=as_pairs)
        run.exact("HLEN", key)
    # Past 8 KB here, and still a listpack in Redis: in order, removals too.
    key = "long-but-packed"
    for i in range(300):
        run.exact("HSET", key, f"field{i:03}", "z" * 60)
    run.exact("OBJECT", "ENCODING", key)
    run.whole("HGETALL", key, key=key, shape=as_pairs)
    for i in range(0, 300, 7):
        run.exact("HDEL", key, f"field{i:03}")
    run.whole("HGETALL", key, key=key, shape=as_pairs)
    run.whole("HKEYS", key, key=key, shape=sorted)
    run.exact("HSET", key, "added-last", "1")
    run.whole("HGETALL", key, key=key, shape=as_pairs)
    run.exact("HSCAN", key, "0", "COUNT", "5")
    # A value crossing 64 bytes, and one crossing 8 KB alone.
    run.exact("HSET", "crossing", "a", "1", "b", "2")
    run.exact("OBJECT", "ENCODING", "crossing")
    run.exact("HSET", "crossing", "a", "v" * 64)
    run.exact("OBJECT", "ENCODING", "crossing")
    run.exact("HSET", "crossing", "a", "v" * 65)
    run.exact("OBJECT", "ENCODING", "crossing")
    run.exact("HSET", "crossing", "huge", "h" * 20000)
    run.exact("HGET", "crossing", "huge")
    run.exact("HSTRLEN", "crossing", "huge")
    run.exact("HDEL", "crossing", "huge", "a", "b")
    run.exact("EXISTS", "crossing")
    # Errors and wrong types against a big hash; a TTL kept through the change.
    for i in range(600):
        run.exact("HSET", "large", f"f{i}", "v")
    run.exact("HSET", "large", "text", "abc")
    run.exact("HINCRBY", "large", "text", "1")
    run.exact("HINCRBYFLOAT", "large", "text", "1.5")
    run.exact("HSET", "large", "nearly", "9223372036854775807")
    run.exact("HINCRBY", "large", "nearly", "1")
    run.exact("HINCRBY", "large", "f0", "x")
    run.exact("GET", "large")
    run.exact("LPUSH", "large", "x")
    run.exact("TYPE", "large")
    run.exact("SET", "text", "x")
    run.exact("HGET", "text", "f")
    run.exact("HSET", "text", "f", "v")
    run.exact("HSCAN", "large", "bad")
    run.exact("HRANDFIELD", "large", "x")
    run.exact("HSET", "with-ttl", "a", "1")
    run.exact("EXPIRE", "with-ttl", "1000")
    for i in range(600):
        run.exact("HSET", "with-ttl", f"f{i}", "v")
    a = run.conns[False][0].call("TTL", "with-ttl")
    b = run.conns[False][1].call("TTL", "with-ttl")
    if abs(a - b) > 2 or b <= 0:
        run.fail(["TTL with-ttl"], a, b, False)
    for i in range(600):
        run.exact("HDEL", "large", f"f{i}")
    run.exact("HLEN", "large")
    run.exact("HDEL", "large", "text", "nearly")
    run.exact("EXISTS", "large")
    run.exact("SORT", "nothing", "BY", "large->f")
    # SORT reads fields from hashes of either size.
    for i in range(3):
        run.exact("RPUSH", "ids", str(i))
        run.exact("HSET", f"small:{i}", "w", str(9 - i))
        for j in range(600):
            run.setup("HSET", f"wide:{i}", f"f{j}", "v")
        run.exact("HSET", f"wide:{i}", "w", str(i))
    run.exact("SORT", "ids", "BY", "small:*->w", "GET", "small:*->w")
    run.exact("SORT", "ids", "BY", "wide:*->w", "GET", "wide:*->w")


def main():
    run = Run()
    if "--record" in sys.argv:
        run.record = open(sys.argv[sys.argv.index("--record") + 1], "w")
    limits(run)
    print(f"limits done: {run.n} commands, {run.bad} differences", flush=True)
    fuzz(run, random.Random(SEED))
    if run.record:
        run.record.close()
    print(f"seed {SEED}: {run.n} commands, {run.bad} differences")
    print("coverage:", " ".join(f"{w}:{k}={n}" for (w, k), n in sorted(run.seen.items())))
    print("deep:", " ".join(f"{k}={n}" for k, n in sorted(run.deep.items())))
    sys.exit(1 if run.bad else 0)


main()
