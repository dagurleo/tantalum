#!/usr/bin/env python3
"""The longest one client waits while one big thing fills and then empties: how long a
server stops to grow or to tidy up. Batches of 500; the longest batches, where they were,
and the usual batch for comparison. A fresh server for each kind.

    stops.py NAME PORT KIND [SIZE]     (KIND: string hash set zset list stream)

Results: results-stops.txt.
"""
import socket, statistics, sys, time

NAME, PORT, KIND = sys.argv[1], int(sys.argv[2]), sys.argv[3]
SIZE = int(sys.argv[4]) if len(sys.argv) > 4 else 8_000_000
BATCH = 500
VALUE = b"v" * 32


def cmd(*args):
    return b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n%s\r\n" % (len(a), a) for a in args)


FILL = {
    "string": lambda i: cmd(b"SET", b"k%015d" % i, VALUE),
    "hash": lambda i: cmd(b"HSET", b"h", b"f%015d" % i, VALUE),
    "set": lambda i: cmd(b"SADD", b"s", b"m%031d" % i),
    "zset": lambda i: cmd(b"ZADD", b"z", b"%d" % (i * 2654435761 % 4294967296), b"m%031d" % i),
    "list": lambda i: cmd(b"RPUSH", b"l", VALUE),
    "stream": lambda i: cmd(b"XADD", b"x", b"%d-0" % (i + 1), b"f", VALUE),
}
EMPTY = {
    "hash": lambda i: cmd(b"HDEL", b"h", b"f%015d" % i),
    "set": lambda i: cmd(b"SREM", b"s", b"m%031d" % i),
    "zset": lambda i: cmd(b"ZREM", b"z", b"m%031d" % i),
}
LINES = 2 if KIND == "stream" else 1

s = socket.create_connection(("127.0.0.1", PORT))
s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)


def replies(n):
    left = n
    while left:
        data = s.recv(1 << 20)
        if not data:
            raise EOFError
        left -= data.count(b"\n")


def run(stage, make, indexes):
    times = []
    started = time.monotonic()
    for at in range(0, len(indexes), BATCH):
        payload = b"".join(make(i) for i in indexes[at : at + BATCH])
        t = time.monotonic()
        s.sendall(payload)
        replies(len(indexes[at : at + BATCH]) * LINES)
        times.append(time.monotonic() - t)
    top = sorted(range(len(times)), key=times.__getitem__, reverse=True)[:4]
    print(
        f"{NAME:<10} {KIND:<7} {stage:<6} longest "
        + " ".join(f"{times[t] * 1000:.1f}@{t * BATCH / 1e6:.2f}M" for t in top)
        + f", over 5 ms: {sum(t > 0.005 for t in times)}, usual {statistics.median(times) * 1000:.2f} ms, "
        f"all {time.monotonic() - started:.1f} s",
        flush=True,
    )


run("fill", FILL[KIND], range(SIZE))
if KIND in EMPTY:
    # Three in four, spread over everything, so no part of it empties on its own.
    run("empty", EMPTY[KIND], [i * 2654435761 % SIZE for i in range(SIZE * 3 // 4)])
