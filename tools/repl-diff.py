#!/usr/bin/env python3
"""Runs the same commands on Redis and on Tantalum, each watched by a replica,
and shows where what they send their replicas differs, byte for byte.

    tools/repl-diff.py [cases file] [--redis port] [--tantalum port] [--only N]
                       [--show]

Each case in the file (default: stdin) is a block of command lines, split like
a shell line (quote arguments with spaces; \\xNN escapes give binary bytes),
ended by a blank line; lines starting with # are comments. A line starting
with `!` runs without being watched: setup, whose propagation isn't compared.
A line `sleep MS` waits (`!sleep MS` before the replica attaches). Before each case both servers are flushed and a fresh
replica attaches (`SYNC`, as Redis' own tests' `attach_to_replication_stream`
does); what each server sends it after the case's commands, up to a marker
`SET` sent last, is compared. Unix times in milliseconds or seconds near now
(within a day), and stream IDs made from them, are shown as <ms>, <s> and
<id>, as they can't match. --show prints Redis' stream for every case.

Start the servers yourself on spare ports, for instance:

    redis-server --port 6390 --save '' --appendonly no &
    target/release/tantalum --port 6391 &
"""

import argparse
import re
import shlex
import socket
import sys
import time

MARKER = b"__repl_diff_marker__"


class Conn:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port))
        self.sock.settimeout(5)
        self.f = self.sock.makefile("rb")

    def send(self, data):
        self.sock.sendall(data)

    def close(self):
        self.f.close()
        self.sock.close()


def read_reply(f):
    """One whole RESP reply's raw bytes."""
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
    if kind in (b"%", b"|"):
        return line + b"".join(read_reply(f) for _ in range(2 * int(rest)))
    return line


def read_command(f):
    """One command of a replication stream, as a list of its arguments."""
    line = f.readline()
    if not line:
        raise EOFError("connection closed")
    if line == b"\n":  # a newline the master sends while it prepares the RDB
        return None
    if not line.startswith(b"*"):
        return [line.rstrip(b"\r\n")]  # inline
    n = int(line[1:-2])
    args = []
    for _ in range(n):
        header = f.readline()
        size = int(header[1:-2])
        args.append(f.read(size + 2)[:-2])
    return args


def encode(args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


def parse(line):
    return [a.encode("latin-1").decode("unicode_escape").encode("latin-1") for a in shlex.split(line)]


def attach(port):
    """A replica of the server on `port`, past its RDB: reads commands next."""
    replica = Conn(port)
    replica.send(b"SYNC\r\n")
    while True:
        line = replica.f.readline()
        if not line:
            raise EOFError("closed during SYNC")
        if line == b"\n":
            continue
        if line.startswith(b"$EOF:"):
            mark = line[5:-2]
            data = b""
            while not data.endswith(mark):
                data += replica.f.read(1)
            return replica
        if line.startswith(b"$"):
            replica.f.read(int(line[1:-2]))
            return replica
        raise ValueError("unexpected reply to SYNC: %r" % line)


def run_case(port, case):
    client = Conn(port)
    for cmd in (b"FLUSHALL", b"FUNCTION FLUSH", b"SCRIPT FLUSH"):
        client.send(cmd + b"\r\n")
        read_reply(client.f)
    replies = []
    replica = None
    for unwatched, line in case:
        if not unwatched and replica is None:
            replica = attach(port)
        if line.startswith("sleep "):
            time.sleep(int(line.split()[1]) / 1000)
            continue
        client.send(encode(parse(line)))
        replies.append(read_reply(client.f))
    if replica is None:
        replica = attach(port)
    client.send(encode([b"SET", MARKER, b"1"]))
    read_reply(client.f)
    stream = []
    while True:
        cmd = read_command(replica.f)
        if cmd is None:
            continue
        if cmd == [b"SET", MARKER, b"1"]:
            break
        if [a.upper() for a in cmd[:1]] == [b"PING"]:
            continue
        stream.append(cmd)
    # The marker's SELECT, if it's the first command in database 0.
    if stream and stream[-1][:1] == [b"SELECT"] and stream[-1][1:] == [b"0"]:
        stream.pop()
    replica.close()
    client.close()
    return replies, stream


NOW_MS = re.compile(rb"^\d{13}$")
NOW_S = re.compile(rb"^\d{10}$")
NOW_ID = re.compile(rb"^(\d{13})-(\d+)$")


def normalize(arg):
    now = time.time()
    if NOW_MS.match(arg) and abs(int(arg) / 1000 - now) < 86400:
        return b"<ms>"
    if NOW_S.match(arg) and abs(int(arg) - now) < 86400:
        return b"<s>"
    m = NOW_ID.match(arg)
    if m and abs(int(m.group(1)) / 1000 - now) < 86400:
        return b"<id>-" + m.group(2)
    return arg


def show(stream):
    return "\n".join(
        "    " + " ".join(repr(a)[2:-1] if a else '""' for a in cmd) for cmd in stream
    ) or "    (nothing)"


def read_cases(text):
    cases, case = [], []
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("#"):
            continue
        if not line:
            if case:
                cases.append(case)
            case = []
            continue
        unwatched = line.startswith("!")
        case.append((unwatched, line[1:].strip() if unwatched else line))
    if case:
        cases.append(case)
    return cases


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("file", nargs="?")
    p.add_argument("--redis", type=int, default=6390)
    p.add_argument("--tantalum", type=int, default=6391)
    p.add_argument("--only", type=int, help="run only case N (from 1)")
    p.add_argument("--show", action="store_true", help="print Redis' stream for every case")
    p.add_argument("--redis-only", action="store_true", help="don't run Tantalum")
    a = p.parse_args()
    text = open(a.file).read() if a.file else sys.stdin.read()
    cases = read_cases(text)
    differ = 0
    for n, case in enumerate(cases, 1):
        if a.only and n != a.only:
            continue
        title = " / ".join(line for unwatched, line in case if not unwatched)
        r_replies, r_stream = run_case(a.redis, case)
        r_norm = [[normalize(x) for x in c] for c in r_stream]
        if a.redis_only:
            print(f"#{n} {title}\n{show(r_norm)}")
            continue
        t_replies, t_stream = run_case(a.tantalum, case)
        t_norm = [[normalize(x) for x in c] for c in t_stream]
        norm_replies = lambda replies: [re.sub(rb"\d{13}-\d+", b"<id>", r) for r in replies]
        r_replies, t_replies = norm_replies(r_replies), norm_replies(t_replies)
        same = r_norm == t_norm and r_replies == t_replies
        if not same:
            differ += 1
            print(f"#{n} DIFFERS: {title}")
            if r_replies != t_replies:
                print(f"  replies: Redis {r_replies!r}\n           Tantalum {t_replies!r}")
            if r_norm != t_norm:
                print("  Redis:\n" + show(r_norm) + "\n  Tantalum:\n" + show(t_norm))
        elif a.show:
            print(f"#{n} same: {title}\n{show(r_norm)}")
    if not a.redis_only:
        print(f"{len(cases) if not a.only else 1} cases, {differ} differ")
    sys.exit(1 if differ else 0)


if __name__ == "__main__":
    main()
