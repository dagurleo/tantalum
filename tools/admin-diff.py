#!/usr/bin/env python3
"""The admin commands against a real Redis: the same commands to both, comparing replies byte
for byte, and for MONITOR and SLOWLOG, what they show apart from times and ports.

    tools/admin-diff.py [--redis 7390] [--tantalum 7391]

Start them yourself, with DEBUG allowed on both:

    redis-server --port 7390 --save '' --appendonly no --enable-debug-command yes &
    target/release/tantalum --port 7391 --enable-debug-command yes &
"""

import argparse
import re
import socket
import sys
import time


class Conn:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port))
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.settimeout(10)
        self.f = self.sock.makefile("rb")

    def send(self, *args):
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
            return line + b"".join(self.read() for _ in range(max(int(rest), 0)))
        if kind in b"%|":
            return line + b"".join(self.read() for _ in range(2 * int(rest)))
        return line

    def call(self, *args):
        self.send(*args)
        return self.read()


class Both:
    def __init__(self, ports, stats):
        self.conns = [Conn(p) for p in ports]
        self.stats = stats

    def compare(self, what, a, b):
        self.stats["compared"] += 1
        if a != b:
            self.stats["differences"] += 1
            print(f"DIFF {what[:100]}\n  redis:    {a[:900]!r}\n  tantalum: {b[:900]!r}")

    def call(self, *args):
        replies = [c.call(*args) for c in self.conns]
        shown = " ".join(a.decode("latin-1") if isinstance(a, bytes) else str(a) for a in args)
        self.compare(shown, replies[0], replies[1])
        return replies[0]

    def both(self, *args):
        """Runs it on both, without comparing."""
        return [c.call(*args) for c in self.conns]

    def entry(self, *args):
        """A `COMMAND INFO` or `DOCS` reply: the order of subcommands is chance in Redis."""
        replies = [dump(in_order(parse(c.call(*args)))) for c in self.conns]
        self.compare(" ".join(args), replies[0], replies[1])

    def shape(self, what, *args):
        """Compares a reply apart from its numbers."""
        replies = [re.sub(rb"\d+(\.\d+)?", b"N", c.call(*args)) for c in self.conns]
        self.compare(what, replies[0], replies[1])


def parse(raw):
    """A reply as a tree: a scalar's whole text, or a header's and what it holds."""
    def node(at):
        end = raw.index(b"\r\n", at) + 2
        line = raw[at:end]
        kind, rest = line[:1], line[1:-2]
        if kind in b"$=!":
            n = int(rest)
            if n < 0:
                return line, end
            return raw[at : end + n + 2], end + n + 2
        if kind in b"*~>%|":
            count = max(int(rest), 0) * (2 if kind in b"%|" else 1)
            items = []
            for _ in range(count):
                item, end = node(end)
                items.append(item)
            return [line, items], end
        return line, end
    return node(0)[0]


def dump(tree):
    if isinstance(tree, bytes):
        return tree
    return tree[0] + b"".join(dump(item) for item in tree[1])


def in_order(tree):
    """Sorts every list of subcommands in a `COMMAND INFO` or `DOCS` reply."""
    if isinstance(tree, bytes):
        return tree
    head, items = tree[0], [in_order(item) for item in tree[1]]
    # DOCS: a map's `subcommands`, whose value is a map by name.
    for i in range(len(items) - 1):
        if items[i] in (b"$11\r\nsubcommands\r\n",) and isinstance(items[i + 1], list):
            inner = items[i + 1]
            pairs = sorted(zip(inner[1][::2], inner[1][1::2]), key=lambda p: dump(p[0]))
            inner[1] = [x for p in pairs for x in p]
    # INFO: an entry's tenth item, the entries under it.
    if len(items) == 10 and isinstance(items[1], bytes) and items[1].startswith(b":") and isinstance(items[9], list):
        items[9][1].sort(key=lambda entry: dump(entry[1][0]) if isinstance(entry, list) else b"")
    return [head, items]


FIXED = [
    # SLOWLOG
    ["SLOWLOG", "GET", "-2"], ["SLOWLOG", "GET", "x"], ["SLOWLOG", "GET", "1", "2"], ["SLOWLOG", "LEN", "1"],
    ["SLOWLOG", "RESET", "1"], ["SLOWLOG", "NOPE"], ["SLOWLOG"], ["SLOWLOG", "RESET"],
    # MEMORY
    ["MEMORY", "NOPE"], ["MEMORY"], ["MEMORY", "USAGE"], ["MEMORY", "USAGE", "nokey"], ["MEMORY", "USAGE", "nokey", "SAMPLES", "x"],
    ["MEMORY", "USAGE", "nokey", "NOPE", "1"], ["MEMORY", "USAGE", "nokey", "SAMPLES", "-1"], ["MEMORY", "USAGE", "nokey", "SAMPLES", "1"],
    ["MEMORY", "PURGE"], ["MEMORY", "PURGE", "x"], ["MEMORY", "DOCTOR", "x"],
    # WAIT
    ["WAIT"], ["WAIT", "0"], ["WAIT", "0", "0"], ["WAIT", "x", "0"], ["WAIT", "0", "x"], ["WAIT", "0", "-1"], ["WAIT", "1", "10"], ["WAIT", "-1", "0"],
    ["WAITAOF", "0", "0", "0"], ["WAITAOF", "1", "0", "0"], ["WAITAOF", "2", "0", "0"], ["WAITAOF", "-1", "0", "0"], ["WAITAOF", "0", "1", "10"],
    ["WAITAOF", "x", "0", "0"], ["WAITAOF", "0", "x", "0"], ["WAITAOF", "0", "-1", "0"], ["WAITAOF", "0", "0", "x"], ["WAITAOF", "0", "0", "-1"],
    # Roles.
    ["ROLE", "x"], ["REPLICAOF", "NO", "ONE"], ["SLAVEOF", "no", "one"], ["REPLICAOF", "NO"],
    # MODULE, LATENCY
    ["MODULE"], ["MODULE", "LIST"], ["MODULE", "LIST", "x"], ["MODULE", "NOPE"],
    ["LATENCY"], ["LATENCY", "LATEST"], ["LATENCY", "HISTORY", "x"], ["LATENCY", "RESET"], ["LATENCY", "RESET", "x"], ["LATENCY", "GRAPH", "x"],
    ["LATENCY", "DOCTOR"], ["LATENCY", "NOPE"],
    # CLIENT
    ["CLIENT", "NO-EVICT", "ON"], ["CLIENT", "NO-EVICT", "OFF"], ["CLIENT", "NO-EVICT", "x"], ["CLIENT", "NO-EVICT"],
    ["CLIENT", "NO-TOUCH", "ON"], ["CLIENT", "NO-TOUCH", "OFF"], ["CLIENT", "NO-TOUCH", "x"], ["CLIENT", "NO-TOUCH"],
    ["CLIENT", "UNBLOCK", "999999"], ["CLIENT", "UNBLOCK", "x"], ["CLIENT", "UNBLOCK", "999999", "NOPE"], ["CLIENT", "UNBLOCK", "999999", "TIMEOUT"],
    ["CLIENT", "UNBLOCK", "999999", "ERROR"], ["CLIENT", "UNBLOCK"], ["CLIENT", "UNBLOCK", "1", "2", "3"],
    ["CLIENT", "PAUSE"], ["CLIENT", "PAUSE", "x"], ["CLIENT", "PAUSE", "-1"], ["CLIENT", "PAUSE", "1", "NOPE"], ["CLIENT", "PAUSE", "1", "2", "3"],
    ["CLIENT", "UNPAUSE"], ["CLIENT", "UNPAUSE", "x"], ["CLIENT", "REPLY", "NOPE"], ["CLIENT", "REPLY"], ["CLIENT", "GETREDIR"], ["CLIENT", "GETREDIR", "x"],
    ["CLIENT", "TRACKINGINFO"], ["CLIENT", "TRACKINGINFO", "x"],
    # CONFIG
    ["CONFIG"], ["CONFIG", "GET"], ["CONFIG", "SET"], ["CONFIG", "SET", "hz"], ["CONFIG", "SET", "nope", "1"], ["CONFIG", "SET", "hz", "x"], ["CONFIG", "SET", "hz", "10"],
    ["CONFIG", "SET", "databases", "3"], ["CONFIG", "SET", "maxmemory", "x"], ["CONFIG", "SET", "maxmemory-policy", "x"], ["CONFIG", "SET", "appendonly", "x"],
    ["CONFIG", "SET", "hz", "10", "hz", "11"], ["CONFIG", "SET", "hz", "10", "nope", "1"], ["CONFIG", "SET", "HZ", "10"], ["CONFIG", "REWRITE"], ["CONFIG", "REWRITE", "x"],
    ["CONFIG", "RESETSTAT"], ["CONFIG", "RESETSTAT", "x"], ["CONFIG", "NOPE"], ["CONFIG", "SET", "maxmemory", "1mb"], ["CONFIG", "GET", "maxmemory"], ["CONFIG", "SET", "maxmemory", "2KB"],
    ["CONFIG", "GET", "maxmemory"], ["CONFIG", "SET", "maxmemory", "3k"], ["CONFIG", "GET", "maxmemory"], ["CONFIG", "SET", "maxmemory", "0"],
    ["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"], ["CONFIG", "GET", "maxmemory-policy"], ["CONFIG", "SET", "maxmemory-policy", "NOEVICTION"], ["CONFIG", "GET", "maxmemory-policy"],
    ["CONFIG", "SET", "timeout", "100"], ["CONFIG", "GET", "timeout"], ["CONFIG", "SET", "timeout", "0"], ["CONFIG", "SET", "tcp-keepalive", "60"], ["CONFIG", "GET", "tcp-keepalive"],
    ["CONFIG", "SET", "tcp-keepalive", "300"], ["CONFIG", "SET", "slowlog-max-len", "64"], ["CONFIG", "GET", "slowlog-max-len"], ["CONFIG", "SET", "slowlog-max-len", "128"],
    ["CONFIG", "SET", "slowlog-log-slower-than", "20000"], ["CONFIG", "GET", "slowlog-log-slower-than"], ["CONFIG", "SET", "slowlog-log-slower-than", "10000"],
    ["CONFIG", "SET", "hash-max-listpack-entries", "100"], ["CONFIG", "GET", "hash-max-listpack-entries"], ["CONFIG", "GET", "hash-max-ziplist-entries"],
    ["CONFIG", "SET", "hash-max-ziplist-entries", "128"], ["CONFIG", "GET", "hash-max-listpack-entries"], ["CONFIG", "SET", "loglevel", "x"], ["CONFIG", "SET", "loglevel", "verbose"],
    ["CONFIG", "GET", "loglevel"], ["CONFIG", "SET", "loglevel", "notice"], ["CONFIG", "GET", "nope"], ["CONFIG", "GET", "nope*"], ["CONFIG", "GET", "loglevel", "loglevel"],
    ["CONFIG", "HELP", "x"], ["SLOWLOG", "HELP", "x"], ["MEMORY", "HELP", "x"], ["MODULE", "HELP", "x"], ["LATENCY", "HELP", "x"],
    ["CONFIG", "SET", "maxmemory-policy", "allkeys-lfu"], ["CONFIG", "GET", "maxmemory-policy"], ["CONFIG", "SET", "maxmemory-policy", "VOLATILE-TTL"],
    ["CONFIG", "GET", "maxmemory-policy"], ["CONFIG", "SET", "maxmemory-policy", "allkeys-random"], ["CONFIG", "SET", "maxmemory-policy", "noeviction"],
    ["CONFIG", "SET", "latency-tracking-info-percentiles", "0.0 50.0 100.0"], ["CONFIG", "GET", "latency-tracking-info-percentiles"],
    ["CONFIG", "SET", "latency-tracking-info-percentiles", "10 a"], ["CONFIG", "SET", "latency-tracking-info-percentiles", "101"],
    ["CONFIG", "SET", "latency-tracking-info-percentiles", "1e1 99.900"], ["CONFIG", "GET", "latency-tracking-info-percentiles"],
    ["CONFIG", "SET", "latency-tracking-info-percentiles", ""], ["CONFIG", "GET", "latency-tracking-info-percentiles"],
    ["CONFIG", "SET", "latency-tracking-info-percentiles", "50 99 99.9"], ["CONFIG", "SET", "latency-tracking", "no"], ["CONFIG", "SET", "latency-tracking", "yes"],
    ["LATENCY", "HISTOGRAM", "nope"], ["OBJECT", "FREQ", "nope"],
    # COMMAND
    ["COMMAND", "GETKEYS"], ["COMMAND", "GETKEYS", "GET"], ["COMMAND", "GETKEYS", "PING"], ["COMMAND", "GETKEYS", "NOPE", "x"], ["COMMAND", "GETKEYS", "GET", "k"],
    ["COMMAND", "GETKEYS", "MSET", "a", "1", "b", "2"], ["COMMAND", "GETKEYS", "EVAL", "x", "0"], ["COMMAND", "GETKEYS", "EVAL", "x", "1", "k"], ["COMMAND", "GETKEYS", "EVAL", "x", "2", "k"],
    ["COMMAND", "GETKEYS", "EVAL", "x", "y"], ["COMMAND", "GETKEYSANDFLAGS"], ["COMMAND", "GETKEYSANDFLAGS", "SET", "k", "v"], ["COMMAND", "GETKEYSANDFLAGS", "SET", "k", "v", "GET"],
    ["COMMAND", "GETKEYSANDFLAGS", "GET", "k"], ["COMMAND", "GETKEYSANDFLAGS", "LMOVE", "a", "b", "LEFT", "RIGHT"], ["COMMAND", "GETKEYS", "SORT", "a", "STORE", "b"], ["COMMAND", "GETKEYS", "SORT", "a"],
    ["COMMAND", "GETKEYS", "SORT", "a", "BY", "x", "GET", "y", "LIMIT", "0", "1", "STORE", "b"], ["COMMAND", "GETKEYS", "XREAD", "STREAMS", "a", "b", "0", "0"], ["COMMAND", "GETKEYS", "XREAD", "COUNT", "1", "STREAMS", "a", "0"],
    ["COMMAND", "GETKEYS", "XREADGROUP", "GROUP", "g", "c", "STREAMS", "a", ">"], ["COMMAND", "GETKEYS", "ZUNION", "2", "a", "b"], ["COMMAND", "GETKEYS", "ZUNIONSTORE", "d", "2", "a", "b"],
    ["COMMAND", "GETKEYS", "ZUNION", "0", "a"], ["COMMAND", "GETKEYS", "OBJECT", "ENCODING", "k"], ["COMMAND", "GETKEYS", "OBJECT", "NOPE", "k"], ["COMMAND", "GETKEYS", "XGROUP", "CREATE", "k", "g", "$"],
    ["COMMAND", "GETKEYS", "LMPOP", "2", "a", "b", "LEFT"], ["COMMAND", "GETKEYS", "BLPOP", "a", "b", "0"], ["COMMAND", "GETKEYS", "DEL", "a", "b"], ["COMMAND", "GETKEYS", "SINTERCARD", "2", "a", "b"],
    ["COMMAND", "GETKEYS", "EVALSHA", "x", "1", "k"], ["COMMAND", "GETKEYS", "BITFIELD", "k", "GET", "u8", "0"], ["COMMAND", "GETKEYSANDFLAGS", "BITFIELD", "k", "GET", "u8", "0"], ["COMMAND", "GETKEYSANDFLAGS", "BITFIELD", "k", "SET", "u8", "0", "1"], ["COMMAND", "GETKEYSANDFLAGS", "BITFIELD", "k", "OVERFLOW", "SAT", "GET", "u8", "0"], ["COMMAND", "GETKEYSANDFLAGS", "BITFIELD", "k", "NOPE"], ["COMMAND", "GETKEYSANDFLAGS", "BITFIELD", "k", "OVERFLOW"], ["COMMAND", "GETKEYSANDFLAGS", "BITFIELD", "k"],
    ["COMMAND", "GETKEYS", "SET", "k"], ["COMMAND", "GETKEYS", "CLIENT", "ID"], ["COMMAND", "GETKEYS", "CLIENT", "NOPE"],
    ["COMMAND", "LIST", "FILTERBY", "PATTERN", "lol*"], ["COMMAND", "LIST", "FILTERBY", "MODULE", "x"], ["COMMAND", "LIST", "NOPE"], ["COMMAND", "LIST", "FILTERBY", "NOPE", "x"], ["COMMAND", "LIST", "FILTERBY", "ACLCAT"],
    ["COMMAND", "COUNT", "x"], ["COMMAND", "INFO", "nope"], ["COMMAND", "DOCS", "nope"], ["COMMAND", "NOPE"],
    # DEBUG
    ["DEBUG", "SLEEP", "0"], ["DEBUG", "NOPE"], ["DEBUG", "OBJECT", "none"], ["DEBUG", "ERROR", "LOADING fake message"], ["DEBUG", "ERROR", "two\r\nlines"], ["DEBUG", "ERROR", "x", "y"],
    # Settings that act, and what they refuse.
    ["CONFIG", "SET", "maxmemory-clients", "10%"], ["CONFIG", "GET", "maxmemory-clients"], ["CONFIG", "SET", "maxmemory-clients", "101%"], ["CONFIG", "SET", "maxmemory-clients", "0%"], ["CONFIG", "GET", "maxmemory-clients"], ["CONFIG", "SET", "maxmemory-clients", "-1%"],
    ["CONFIG", "SET", "maxmemory-clients", "1mb"], ["CONFIG", "GET", "maxmemory-clients"], ["CONFIG", "SET", "maxmemory-clients", "0"], ["CONFIG", "SET", "shutdown-on-sigint", "now force"], ["CONFIG", "SET", "shutdown-on-sigint", "nope"], ["CONFIG", "SET", "shutdown-on-sigint", "default"],
    ["LATENCY", "HELP"],
]


def monitor_lines(raw):
    out = []
    for line in raw.split(b"\r\n"):
        if line:
            line = re.sub(rb"^\+\d+\.\d+ \[(\d+) [^\]]*:\d+\]", rb"[\1 client]", line)
            line = re.sub(rb"^\+\d+\.\d+ \[(\d+) lua\]", rb"[\1 lua]", line)
            out.append(line)
    return out


def monitor(ports, stats):
    monitors = [Conn(p) for p in ports]
    for m in monitors:
        assert m.call("MONITOR") == b"+OK\r\n"
    c = Both(ports, stats)
    time.sleep(0.05)
    c.both("PING")  # looks from its next batch
    time.sleep(0.05)
    for m in monitors:
        m.sock.settimeout(0.5)
        m.sock.recv(65536)
    commands = [
        ["SET", "k", b'a b\n"\\\xff\x00\x07\x08\t\r'], ["GET", "k"], ["get", "k"], ["AUTH", "x"], ["AUTH", "u", "p"], ["HELLO", "3", "AUTH", "u", "p"], ["HELLO", "2"], ["HELLO", "2", "SETNAME", "x"],
        ["SELECT", "3"], ["SET", "k", "3"], ["MOVE", "k", "4"], ["SWAPDB", "3", "4"], ["SELECT", "0"], ["MULTI"], ["SET", "k", "m"], ["GET", "k"], ["EXEC"], ["MULTI"], ["SET", "k", "m"], ["DISCARD"],
        ["EVAL", "return redis.call('GET', KEYS[1])", "1", "k"], ["EVAL", "redis.call('SELECT', 2) return redis.call('SET', KEYS[1], 'x')", "1", "k"], ["EVAL", "return 1", "0"],
        ["MULTI"], ["EVALSHA", "ffffffffffffffffffffffffffffffffffffffff", "0"], ["EVAL", "return redis.call('PING')", "0"], ["CONFIG", "GET", "hz"], ["CLIENT", "ID"], ["SET", "k", "1"], ["EXEC"], ["EVALSHA", "ffffffffffffffffffffffffffffffffffffffff", "0"], ["EVAL", "x", "x"], ["EVAL", "x"],
        ["SCRIPT", "LOAD", "return 1"], ["EVALSHA", "e0e1f9fabfc9d4800c877a703b823ac0578ff8db", "0"], ["EVAL_RO", "return 1", "0"],
        ["CONFIG", "GET", "hz"], ["CONFIG", "SET", "hz", "10"], ["CLIENT", "ID"], ["CLIENT", "LIST"], ["CLIENT", "INFO"], ["CLIENT", "SETNAME", "x"], ["CLIENT", "GETNAME"], ["CLIENT", "KILL", "ID", "999999"],
        ["CLIENT", "NO-EVICT", "OFF"], ["CLIENT", "UNBLOCK", "999999"], ["CLIENT", "PAUSE", "0"], ["CLIENT", "UNPAUSE"], ["CLIENT", "GETREDIR"],
        ["DEBUG", "OBJECT", "k"], ["INFO", "server"], ["PING"], ["NOPE"], ["GET"], ["SLOWLOG", "LEN"], ["MEMORY", "PURGE"], ["MEMORY", "USAGE", "k"], ["LATENCY", "LATEST"], ["COMMAND", "COUNT"], ["OBJECT", "ENCODING", "k"], ["ROLE"],
        ["WAIT", "0", "0"], ["DBSIZE"], ["FLUSHDB"], ["SAVE"], ["BGSAVE"], ["LASTSAVE"], ["SUBSCRIBE", "x"], ["UNSUBSCRIBE", "x"], ["PUBLISH", "x", "y"], ["RESET"],
    ]
    for args in commands:
        c.both(*args)
    time.sleep(0.2)
    heard = []
    for m in monitors:
        data = b""
        try:
            while True:
                chunk = m.sock.recv(65536)
                if not chunk:
                    break
                data += chunk
        except OSError:
            pass
        heard.append(monitor_lines(data))
    for i in range(max(len(heard[0]), len(heard[1]))):
        a = heard[0][i] if i < len(heard[0]) else b"(nothing)"
        b = heard[1][i] if i < len(heard[1]) else b"(nothing)"
        c.compare(f"MONITOR line {i}", a, b)


def slowlog_entries(raw):
    """Each entry's arguments and name, as raw text: id, time, duration and port taken out."""
    return re.sub(rb":\d+\r\n", b":N\r\n", re.sub(rb"\$\d+\r\n127\.0\.0\.1:\d+\r\n", b"PEER\r\n", raw))


def settings(ports, stats):
    """What every setting says to a bad value, and that it stays what it was."""
    c = Both(ports, stats)
    parse_pairs = lambda raw: dict(zip(*[iter(raw.split(b"\r\n")[2::2])] * 2))
    before = parse_pairs(c.both("CONFIG", "GET", "*")[0])
    # Each a server's own, or would lock this connection out or stop it.
    own = (b"requirepass", b"port", b"bind", b"dir", b"appendonly", b"save", b"maxclients", b"notify-keyspace-events")
    # Settings that do nothing here, and whose syntax of their own isn't checked.
    unread = (b"client-output-buffer-limit", b"latency-tracking-info-percentiles", b"locale-collate", b"oom-score-adj-values", b"proc-title-template", b"maxmemory-clients")
    for name in sorted(n for n in before if n not in own + unread):
        for value in ("nope", "-1", "-99999999999999999", "99999999999999999999999", "2147483648", "1000000", ""):
            replies = c.both("CONFIG", "SET", name, value)
            c.compare(f"CONFIG SET {name.decode()} {value}", replies[0], replies[1])
            if replies[0].startswith(b"+") or replies[1].startswith(b"+"):
                got = c.both("CONFIG", "GET", name)
                c.compare(f"CONFIG GET {name.decode()} after {value}", got[0], got[1])
                c.both("CONFIG", "SET", name, before[name])


def streams(ports, stats):
    """How `~` trims with nodes of other sizes."""
    c = Both(ports, stats)

    def fill(key, n):
        return [c.both("XADD", key, f"{i + 1}-0", "a", str(i))[0].decode().split("\r\n")[1] for i in range(n)]

    for node in (1, 2, 3, 5, 100, 0):
        c.call("CONFIG", "SET", "stream-node-max-entries", node)
        for n, target, limit in [(5, 2, None), (5, 0, None), (5, 0, 1), (10, 1, 3), (10, 4, 0), (7, 3, None), (250, 10, None), (250, 10, 50), (250, 10, 0)]:
            c.both("DEL", "s")
            fill("s", n)
            args = ["XTRIM", "s", "MAXLEN", "~", target] + (["LIMIT", limit] if limit is not None else [])
            c.call(*args)
            c.call("XLEN", "s")
            c.call("XRANGE", "s", "-", "+", "COUNT", "1")
        c.both("DEL", "s")
        ids = fill("s", 5)
        c.call("XTRIM", "s", "MINID", "~", ids[3])
        c.call("XLEN", "s")
        c.both("DEL", "s")
        ids = fill("s", 5)
        c.call("XTRIM", "s", "MINID", "~", ids[3], "LIMIT", 1)
        c.call("XLEN", "s")
        c.both("DEL", "s")
        ids = fill("s", 10)
        c.both("XDEL", "s", ids[0], ids[1])
        c.call("XTRIM", "s", "MAXLEN", "~", 3)
        c.call("XLEN", "s")
        c.both("DEL", "s")
        fill("s", 5)
        c.call("XADD", "s", "MAXLEN", "~", 2, "99-0", "a", "z")
        c.call("XLEN", "s")
    c.call("CONFIG", "SET", "stream-node-max-entries", 100)
    for size in (64, 4096, 0):
        c.call("CONFIG", "SET", "stream-node-max-bytes", size)
        for value in ("x", "x" * 300, "x" * 5000):
            c.both("DEL", "s")
            for i in range(40):
                c.both("XADD", "s", f"{i + 1}-0", "a", value)
            c.call("XTRIM", "s", "MAXLEN", "~", 1)
            c.call("XLEN", "s")
    c.call("CONFIG", "SET", "stream-node-max-bytes", 4096)
    c.both("DEL", "s")


def latency(ports, stats):
    """The latency monitor, and `MEMORY STATS`' names and kinds."""
    c = Both(ports, stats)
    c.both("SET", "a", "1")
    c.shape("MEMORY STATS", "MEMORY", "STATS")
    r3 = Both(ports, stats)
    r3.both("HELLO", "3")
    r3.shape("MEMORY STATS, RESP3", "MEMORY", "STATS")
    c.call("LATENCY", "DOCTOR")
    c.call("CONFIG", "SET", "latency-monitor-threshold", "50")
    c.call("LATENCY", "DOCTOR")
    c.call("LATENCY", "GRAPH", "command")
    # A slow command, as one wouldn't be on a fast machine: one sample, then more.
    c.both("DEBUG", "SLEEP", "0.2")
    c.shape("LATENCY LATEST", "LATENCY", "LATEST")
    c.shape("LATENCY HISTORY", "LATENCY", "HISTORY", "command")
    c.call("LATENCY", "HISTORY", "nope")
    c.shape("LATENCY GRAPH", "LATENCY", "GRAPH", "command")
    r3.shape("LATENCY GRAPH, RESP3", "LATENCY", "GRAPH", "command")
    c.shape("LATENCY DOCTOR", "LATENCY", "DOCTOR")
    c.both("CONFIG", "SET", "slowlog-log-slower-than", "-1")
    c.shape("LATENCY DOCTOR, no slow log", "LATENCY", "DOCTOR")
    c.both("CONFIG", "SET", "slowlog-log-slower-than", "100000")
    c.shape("LATENCY DOCTOR, slow log above", "LATENCY", "DOCTOR")
    c.both("CONFIG", "SET", "slowlog-log-slower-than", "10000")
    # Inside EXEC, and EXEC itself.
    c.both("LATENCY", "RESET")
    c.both("MULTI")
    c.both("DEBUG", "SLEEP", "0.1")
    c.both("EXEC")
    c.shape("LATENCY LATEST, after EXEC", "LATENCY", "LATEST")
    c.call("LATENCY", "RESET", "nope")
    c.call("LATENCY", "RESET", "command", "nope")
    c.call("LATENCY", "LATEST")
    c.call("LATENCY", "RESET")
    c.call("CONFIG", "SET", "latency-monitor-threshold", "0")
    c.both("DEBUG", "SLEEP", "0.1")
    c.call("LATENCY", "LATEST")
    # One setting, then what it turned on, in one go.
    c.both("CONFIG", "SET", "latency-monitor-threshold", "1")
    c.both("LATENCY", "RESET")
    c.both("CONFIG", "SET", "latency-monitor-threshold", "0")
    for conn in c.conns:
        conn.send("CONFIG", "SET", "latency-monitor-threshold", "1")
        conn.send("DEBUG", "SLEEP", "0.01")
        conn.send("LATENCY", "HISTORY", "command")
    replies = [[re.sub(rb"\d+", b"N", conn.read()) for _ in range(3)] for conn in c.conns]
    c.compare("a pipeline that turns the monitor on", b"".join(replies[0]), b"".join(replies[1]))
    c.both("CONFIG", "SET", "latency-monitor-threshold", "0")
    c.both("LATENCY", "RESET")


def slowlog(ports, stats):
    c = Both(ports, stats)
    # The command that turns it on is in it.
    c.both("SLOWLOG", "RESET")
    c.call("CONFIG", "SET", "slowlog-log-slower-than", "0")
    c.both("SET", "first", "1")
    logs = c.both("SLOWLOG", "GET", "-1")
    c.compare("SLOWLOG after turning it on", slowlog_entries(logs[0]), slowlog_entries(logs[1]))
    c.call("CONFIG", "SET", "slowlog-log-slower-than", "10000")
    c.both("SLOWLOG", "RESET")
    for conn in c.conns:
        conn.send("CONFIG", "SET", "slowlog-log-slower-than", "0")
        conn.send("SET", "second", "1")
        conn.send("SLOWLOG", "LEN")
    replies = [[conn.read() for _ in range(3)] for conn in c.conns]
    c.compare("a pipeline that turns it on", b"".join(replies[0]), b"".join(replies[1]))
    c.both("PING")
    c.both("SLOWLOG", "RESET")
    # What's secret.
    for args in [["AUTH", "x"], ["AUTH", "u", "p"], ["AUTH", "a", "b", "c"], ["HELLO", "2", "AUTH", "u", "p"], ["HELLO", "2", "SETNAME", "AUTH", "AUTH", "u", "p"], ["HELLO", "2", "AUTH", "u"], ["HELLO", "2"],
                 ["CONFIG", "SET", "masteruser", "sekret"], ["CONFIG", "SET", "masterauth", "sekret"], ["CONFIG", "SET", "MASTERAUTH", "sekret"], ["CONFIG", "SET", "nope", "sekret"], ["CONFIG", "SET", "hz", "x", "masterauth", "sekret"],
                 ["CONFIG", "SET", "masterauth", "sekret", "nope", "1"], ["CONFIG", "SET", "masteruser", ""], ["CONFIG", "SET", "masterauth", ""],
                 ["EVAL", "redis.call('SET', KEYS[1], '1') redis.call('SELECT', 3) redis.call('GET', KEYS[1]) return 1", "1", "k"], ["MULTI"], ["SET", "k", "1"], ["GET", "k"], ["EXEC"], ["BLPOP", "nolist", "0.01"], ["RPUSH", "alist", "x"], ["BLPOP", "alist", "0"], ["CONFIG", "SET", "hz"], ["CONFIG", "NOPE"], ["GET"]]:
        c.both(*args)
    logs = c.both("SLOWLOG", "GET", "-1")
    c.compare("SLOWLOG of secrets, scripts and EXEC", slowlog_entries(logs[0]), slowlog_entries(logs[1]))
    c.both("SLOWLOG", "RESET")
    for args in [["SET", "long", "x" * 200], ["AUTH", "x"], ["MULTI"], ["SET", "k", "1"], ["EXEC"], ["MSET", *(["a", "b"] * 20)],
                 ["CLIENT", "SETNAME", "who"], ["GET", "k"], ["HELLO", "2"], ["CONFIG", "GET", "hz"], ["EVAL", "return redis.call('GET', KEYS[1])", "1", "k"], ["NOPE"]]:
        c.both(*args)
    logs = c.both("SLOWLOG", "GET", "-1")
    c.compare("SLOWLOG GET -1", slowlog_entries(logs[0]), slowlog_entries(logs[1]))
    c.call("CONFIG", "SET", "slowlog-log-slower-than", "-1")
    c.both("SLOWLOG", "RESET")
    c.both("PING")
    c.call("SLOWLOG", "LEN")
    c.call("CONFIG", "SET", "slowlog-log-slower-than", "10000")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis", type=int, default=7390)
    parser.add_argument("--tantalum", type=int, default=7391)
    opts = parser.parse_args()
    ports = [opts.redis, opts.tantalum]
    stats = {"compared": 0, "differences": 0}
    c = Both(ports, stats)
    for args in FIXED:
        c.call(*args)
    print(f"by hand: {stats['compared']} compared, {stats['differences']} differences")
    # Every command's entry, in both protocols, and every setting's default.
    names = sorted(line for line in c.both("COMMAND", "LIST")[1].decode().split("\r\n")[2::2] if line)
    r3 = Both(ports, stats)
    r3.both("HELLO", "3")
    for name in names:
        c.entry("COMMAND", "INFO", name)
        c.entry("COMMAND", "DOCS", name)
        r3.entry("COMMAND", "INFO", name)
        r3.entry("COMMAND", "DOCS", name)
        c.call("COMMAND", "GETKEYS", name)
    print(f"{len(names)} commands' entries: {stats['compared']} compared, {stats['differences']} differences")
    defaults = c.both("CONFIG", "GET", "*")
    parse = lambda raw: dict(zip(*[iter(raw.split(b"\r\n")[2::2])] * 2))
    redis, tantalum = parse(defaults[0]), parse(defaults[1])
    for name in sorted(set(redis) | set(tantalum)):
        # Where each was started differently, or that's about the machine.
        if name in (b"port", b"bind", b"dir", b"save", b"appendonly", b"enable-debug-command", b"maxclients", b"io-threads", b"oom-score-adj-values"):
            continue
        c.compare(f"CONFIG GET {name.decode()}", redis.get(name, b"(absent)"), tantalum.get(name, b"(absent)"))
    print(f"{len(redis)} settings: {stats['compared']} compared, {stats['differences']} differences")
    settings(ports, stats)
    print(f"Bad values: {stats['compared']} compared, {stats['differences']} differences")
    streams(ports, stats)
    print(f"Streams: {stats['compared']} compared, {stats['differences']} differences")
    latency(ports, stats)
    print(f"LATENCY: {stats['compared']} compared, {stats['differences']} differences")
    monitor(ports, stats)
    print(f"MONITOR: {stats['compared']} compared, {stats['differences']} differences")
    slowlog(ports, stats)
    print(f"SLOWLOG: {stats['compared']} compared, {stats['differences']} differences")
    sys.exit(1 if stats["differences"] else 0)


if __name__ == "__main__":
    main()
