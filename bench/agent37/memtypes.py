#!/usr/bin/env python3
"""Real memory per item for each data type, with the same method as memslope.py:
RSS and used_memory at checkpoints, small batches. Prints the slope and the total.

    memtypes.py KIND NAME PORT PID

KIND is one of WORKLOADS below; a fresh server for each run, on Linux (it reads
/proc).
"""
import socket, sys

BATCH = 500
STEP = 1_000_000
KIND, NAME, PORT, PID = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])


def cmd(*args):
    return b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n%s\r\n" % (len(a), a) for a in args)


value = b"v" * 32


def member(i):
    return b"m%031d" % i


def field(i):
    return b"f%015d" % i


WORKLOADS = {
    "string": (3, lambda i: cmd(b"SET", b"key:%d" % i, value)),
    "list": (3, lambda i: cmd(b"RPUSH", b"l", value)),
    "set": (3, lambda i: cmd(b"SADD", b"s", member(i))),
    "zset": (3, lambda i: cmd(b"ZADD", b"z", b"%d" % i, member(i))),
    "hash": (3, lambda i: cmd(b"HSET", b"h", field(i), value)),
    # Many small ones: packed, in the store's own pages, not big collections.
    "small-hash": (3, lambda i: cmd(b"HSET", b"h:%d" % (i // 10), b"f%d" % (i % 10), value)),
    "small-list": (3, lambda i: cmd(b"RPUSH", b"l:%d" % (i // 10), value)),
    "small-set": (3, lambda i: cmd(b"SADD", b"s:%d" % (i // 10), member(i))),
    "small-zset": (3, lambda i: cmd(b"ZADD", b"z:%d" % (i // 10), b"%d" % i, member(i))),
}
steps, make = WORKLOADS[KIND]

s = socket.create_connection(("127.0.0.1", PORT))
s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)


def replies(n):
    left = n
    while left:
        data = s.recv(1 << 20)
        if not data:
            raise EOFError
        left -= data.count(b"\n")


def used():
    s.sendall(cmd(b"INFO", b"memory"))
    head = b""
    while b"\r\n" not in head:
        head += s.recv(1)
    n = int(head[1:head.index(b"\r")])
    data = head[head.index(b"\n") + 1:]
    while len(data) < n + 2:
        data += s.recv(1 << 16)
    for line in data.decode("latin1").splitlines():
        if line.startswith("used_memory:"):
            return int(line.split(":")[1])
    raise ValueError


def rss():
    with open(f"/proc/{PID}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    raise ValueError


base = (used(), rss())
points = []
for step in range(steps):
    for at in range(step * STEP, (step + 1) * STEP, BATCH):
        s.sendall(b"".join(make(i) for i in range(at, at + BATCH)))
        replies(BATCH)
    points.append((used(), rss()))
total = steps * STEP
slope = lambda k: (points[-1][k] - points[0][k]) / ((steps - 1) * STEP)
print(
    f"{KIND:<11} {NAME:<9} used_memory/item {(points[-1][0] - base[0]) / total:6.1f}  RSS/item {(points[-1][1] - base[1]) / total:6.1f}"
    f"   slope: used {slope(0):6.1f}  RSS {slope(1):6.1f}",
    flush=True,
)
