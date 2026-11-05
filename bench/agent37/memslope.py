#!/usr/bin/env python3
"""Real memory per stream entry and per pending entry: RSS and used_memory at each
million, small batches so client buffers stay small. The slope from the first
checkpoint to the last is what each one costs; the rest is a fixed cost.

    memslope.py NAME=PORT=PID ...       (MILLIONS, VALUE, CONSUMERS in the environment)

Each server fresh and alone, on Linux (it reads /proc). Results: results-groups.txt.
"""
import os, socket, sys

MILLIONS = int(os.environ.get("MILLIONS", "5"))
VALUE = b"v" * int(os.environ.get("VALUE", "32"))
STEP = 1_000_000
BATCH = 500


class Server:
    def __init__(self, name, port, pid):
        self.name, self.pid = name, pid
        self.s = socket.create_connection(("127.0.0.1", port))
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def exact(self, n):
        left = n
        buf = bytearray(min(n, 1 << 20))
        while left:
            got = self.s.recv_into(buf, min(left, len(buf)))
            if not got:
                raise EOFError
            left -= got

    def line(self):
        out = b""
        while not out.endswith(b"\r\n"):
            out += self.s.recv(1)
        return out

    def call(self, *args):
        self.s.sendall(cmd(*args))
        head = self.line()
        if head[:1] == b"$":
            n = int(head[1:])
            if n < 0:
                return None
            data = b""
            while len(data) < n + 2:
                data += self.s.recv(n + 2 - len(data))
            return data[:-2]
        return head.strip()

    def used(self):
        for line in self.call(b"INFO", b"memory").decode("latin1").splitlines():
            if line.startswith("used_memory:"):
                return int(line.split(":")[1])
        raise ValueError

    def rss(self):
        with open(f"/proc/{self.pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
        raise ValueError


def cmd(*args):
    return b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n%s\r\n" % (len(a), a) for a in args)


def fill(s, start, n):
    for at in range(start, start + n, BATCH):
        ids = [b"%d-0" % i for i in range(at, at + BATCH)]
        s.s.sendall(b"".join(cmd(b"XADD", b"s", i, b"f", VALUE) for i in ids))
        s.exact(sum(len(i) + len(str(len(i))) + 5 for i in ids))


CONSUMERS = int(os.environ.get("CONSUMERS", "1"))
READS = [cmd(b"XREADGROUP", b"GROUP", b"g", b"c%d" % c, b"COUNT", b"%d" % BATCH, b"STREAMS", b"s", b">") for c in range(CONSUMERS)]
HEAD = b"*1\r\n*2\r\n$1\r\ns\r\n*%d\r\n" % BATCH
ENTRY = len(b"*2\r\n\r\n*2\r\n$1\r\nf\r\n$%d\r\n\r\n" % len(VALUE)) + len(VALUE)


def read(s, start, n):
    for at in range(start, start + n, BATCH):
        total = len(HEAD)
        for i in range(at, at + BATCH):
            digits = len(str(i)) + 2  # "-0"
            total += ENTRY + 1 + len(str(digits)) + 2 + digits
        s.s.sendall(READS[(at // BATCH) % CONSUMERS])
        s.exact(total)


def slope(points):
    return (points[-1] - points[0]) / ((len(points) - 1) * STEP)


def run(s):
    s.call(b"XGROUP", b"CREATE", b"s", b"g", b"0", b"MKSTREAM")
    base = (s.used(), s.rss())
    used, rss = [], []
    for m in range(MILLIONS):
        fill(s, 1 + m * STEP, STEP)
        used.append(s.used())
        rss.append(s.rss())
    last = (used[-1], rss[-1])
    entries_only = os.environ.get("ONLY_ENTRIES")
    print(f"{s.name}: entries, slope per entry: used_memory {slope(used):.1f} B, RSS {slope(rss):.1f} B; fixed RSS {(rss[0] - base[1]) / 1e6 - slope(rss):.1f} MB; "
          f"RSS at each million, MB: {' '.join(f'{r / 1e6:.0f}' for r in rss)}", flush=True)
    if entries_only:
        return
    used, rss = [last[0]], [last[1]]
    for m in range(MILLIONS):
        read(s, 1 + m * STEP, STEP)
        used.append(s.used())
        rss.append(s.rss())
    print(f"{s.name}: pending, slope per entry: used_memory {slope(used):.1f} B, RSS {slope(rss):.1f} B; "
          f"RSS at each million, MB: {' '.join(f'{r / 1e6:.0f}' for r in rss)}", flush=True)


for spec in sys.argv[1:]:
    name, port, pid = spec.split("=")
    run(Server(name, int(port), int(pid)))
print("JOB-DONE")
