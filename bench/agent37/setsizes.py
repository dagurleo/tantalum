#!/usr/bin/env python3
"""A big set or hash at several sizes, settled: a hash table's cost rises and falls
as it doubles, so one size could flatter anyone. Also the longest a batch took, which
is the longest it stopped to grow. A fresh server for each, on Linux (it reads /proc).
setsizes.py NAME PORT PID [hash]. Results: results-sets.txt, results-hashes.txt."""
import socket, sys, time

NAME, PORT, PID = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
HASH = sys.argv[4:] == ["hash"]
BATCH = 500
SIZES = [500_000, 1_000_000, 2_000_000, 3_000_000, 4_000_000, 6_000_000, 8_000_000]


def cmd(*args):
    return b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n%s\r\n" % (len(a), a) for a in args)


s = socket.create_connection(("127.0.0.1", PORT))
s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)


def replies(n):
    left = n
    while left:
        data = s.recv(1 << 20)
        if not data:
            raise EOFError
        left -= data.count(b"\n")


def rss():
    with open(f"/proc/{PID}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    raise ValueError


base = rss()
at = 0
row = []
worst = 0.0
for size in SIZES:
    while at < size:
        started = time.monotonic()
        if HASH:
            s.sendall(b"".join(cmd(b"HSET", b"h", b"f%015d" % i, b"v" * 32) for i in range(at, at + BATCH)))
        else:
            s.sendall(b"".join(cmd(b"SADD", b"s", b"m%031d" % i) for i in range(at, at + BATCH)))
        replies(BATCH)
        worst = max(worst, time.monotonic() - started)
        at += BATCH
    time.sleep(3)
    row.append(f"{size // 1_000_000 if size >= 1_000_000 else size / 1e6:g}M {(rss() - base) / size:5.1f}")
print(f"{NAME:<10} " + "  ".join(row) + f"   longest batch {worst * 1000:.0f} ms", flush=True)
