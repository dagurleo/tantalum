#!/usr/bin/env python3
"""Sends the same commands to Redis and to Tantalum and shows where their
raw replies differ, byte for byte.

    tools/redis-diff.py [commands file] [--redis port] [--tantalum port] [--rust]

Each line of the file (default: stdin) is one command, split like a shell
line, so quote arguments with spaces; \\xNN escapes give binary bytes, and
lines starting with # are comments. Both servers are flushed first. With
--rust it prints each command and Redis' reply as a Rust test case
(Tantalum's where they differ, marked) instead. Start the servers yourself
on spare ports, for instance:

    redis-server --port 6390 --save '' --appendonly no &
    target/release/tantalum --port 6391 &
"""

import argparse
import shlex
import socket
import sys


def read_reply(f):
    """Reads one whole RESP reply and returns its raw bytes."""
    line = f.readline()
    if not line:
        raise EOFError("connection closed")
    kind, rest = line[:1], line[1:-2]
    if kind in b"$=!":
        n = int(rest)
        return line + (f.read(n + 2) if n >= 0 else b"")
    if kind in b"*~>":
        n = int(rest)
        return line + b"".join(read_reply(f) for _ in range(max(n, 0)))
    if kind == b"%" or kind == b"|":
        return line + b"".join(read_reply(f) for _ in range(2 * int(rest)))
    return line


def encode(args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


def parse(line):
    return [a.encode("latin-1").decode("unicode_escape").encode("latin-1") for a in shlex.split(line)]


def rust_bytes(b):
    """A Rust byte string literal."""
    out = ""
    for c in b:
        ch = chr(c)
        if ch == "\\" or ch == '"':
            out += "\\" + ch
        elif ch == "\r":
            out += "\\r"
        elif ch == "\n":
            out += "\\n"
        elif 0x20 <= c < 0x7F:
            out += ch
        else:
            out += f"\\x{c:02x}"
    return f'b"{out}"'


def main():
    p = argparse.ArgumentParser()
    p.add_argument("file", nargs="?")
    p.add_argument("--redis", type=int, default=6390)
    p.add_argument("--tantalum", type=int, default=6391)
    p.add_argument("--rust", action="store_true")
    a = p.parse_args()
    lines = open(a.file).read().splitlines() if a.file else sys.stdin.read().splitlines()
    conns = {}
    for name, port in (("redis", a.redis), ("tantalum", a.tantalum)):
        s = socket.create_connection(("127.0.0.1", port))
        conns[name] = (s, s.makefile("rb"))
    for s, f in conns.values():
        s.sendall(encode([b"FLUSHALL"]))
        read_reply(f)
    differences = 0
    for line in lines:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        args = parse(line)
        replies = {}
        for name, (s, f) in conns.items():
            s.sendall(encode(args))
            replies[name] = read_reply(f)
        if a.rust:
            same = replies["redis"] == replies["tantalum"]
            if not same:
                print(f"    // Differs from Redis, which replies {rust_bytes(replies['redis'])}")
            print(f"    (&[{', '.join(rust_bytes(x) for x in args)}], {rust_bytes(replies['tantalum'])}),")
            continue
        if replies["redis"] != replies["tantalum"]:
            differences += 1
            print(f"DIFF  {line}")
            print(f"  redis:    {replies['redis']!r}")
            print(f"  tantalum: {replies['tantalum']!r}")
    if a.rust:
        return
    total = sum(1 for l in lines if l.strip() and not l.lstrip().startswith("#"))
    print(f"{total} commands, {differences} differences")
    sys.exit(1 if differences else 0)


if __name__ == "__main__":
    main()
