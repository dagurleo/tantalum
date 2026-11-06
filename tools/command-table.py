#!/usr/bin/env python3
"""Generates what Tantalum says about its commands and settings from a real Redis, so
that `COMMAND` and `CONFIG GET` answer as it does (Redis 7.2.4's are BSD licensed).

    tools/command-table.py --port 7390 --names names.txt --out src/

names.txt has a command name a line. Writes:

    command_table.resp   each named command: its name, COMMAND INFO's entry and COMMAND
                         DOCS' map, RESP3, one after another (read by src/command_table.rs)
    config_defaults.txt  CONFIG GET *, name then value, tab between, value escaped as \\n
    config_kinds.txt     how each setting's value reads, from what it says to bad ones

Start the server with no options: the defaults are what's being read.
"""
import argparse
import os
import re
import socket


class Conn:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port))
        self.f = self.sock.makefile("rb")

    def send(self, *args):
        out = b"*%d\r\n" % len(args)
        for a in args:
            a = a if isinstance(a, bytes) else str(a).encode()
            out += b"$%d\r\n%s\r\n" % (len(a), a)
        self.sock.sendall(out)

    def read(self):
        line = self.f.readline()
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


def parse_pairs(raw):
    """A RESP3 map of bulk strings, as a list of pairs."""
    lines = raw.split(b"\r\n")
    n = int(lines[0][1:])
    pairs, at = [], 0
    # Walk by lengths: values may hold line ends.
    body = raw[raw.index(b"\r\n") + 2 :]

    def bulk():
        nonlocal at
        end = body.index(b"\r\n", at)
        size = int(body[at + 1 : end])
        value = body[end + 2 : end + 2 + size]
        at = end + 2 + size + 2
        return value

    for _ in range(n):
        name = bulk()
        pairs.append((name, bulk()))
    return pairs


def setting_kinds(conn, pairs):
    """Each setting's kind and bounds or choices, from what Redis says when it refuses a value."""
    lines = []
    for name, default in pairs:
        name = name.decode()
        # It would lock this connection out.
        if name == "requirepass":
            lines.append(f"{name}\ttext")
            continue
        said = conn.call("CONFIG", "SET", name, "nope-value")
        kind, accepted = "text", said.startswith(b"+")
        text = said.decode("latin-1")
        bounds = lambda t: re.search(r"between (-?\d+) and (-?\d+) inclusive", t)
        if "can't set immutable" in text:
            kind = "immutable"
        elif "can't set protected" in text:
            kind = "protected"
        elif "must be 'yes' or 'no'" in text:
            kind = "bool"
        elif "couldn't be parsed into an integer" in text:
            low = conn.call("CONFIG", "SET", name, "-99999999999999999").decode("latin-1")
            found = bounds(low)
            kind = f"int\t{found.group(1)}\t{found.group(2)}"
        elif "must be a memory or percent value" in text:
            kind = "percent"
            accepted = True
        elif "must be a memory value" in text:
            high = conn.call("CONFIG", "SET", name, "99999999999999999999999").decode("latin-1")
            found = bounds(high)
            kind = f"memory\t{found.group(1)}\t{found.group(2)}" if found else f"memory\t0\t{2**64 - 1}"
            accepted = found is None
        elif "must be one of the following: " in text:
            kind = "oneof\t" + text.split("must be one of the following: ")[1].strip()
        if accepted:
            conn.call("CONFIG", "SET", name, default)
        lines.append(f"{name}\t{kind}")
    return lines


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=7390)
    parser.add_argument("--names", required=True)
    parser.add_argument("--out", required=True)
    opts = parser.parse_args()
    names = [line.strip() for line in open(opts.names) if line.strip()]
    conn = Conn(opts.port)
    assert conn.call("HELLO", "3").startswith(b"%")
    version = [l for l in conn.call("INFO", "server").split(b"\r\n") if l.startswith(b"redis_version:")]
    print("from", version[0].decode())
    missing = []
    with open(os.path.join(opts.out, "command_table.resp"), "wb") as table:
        for name in names:
            info = conn.call("COMMAND", "INFO", name)
            # An array of one entry, or of one null.
            entry = info[info.index(b"\r\n") + 2 :]
            docs = conn.call("COMMAND", "DOCS", name)
            if entry.startswith(b"_"):
                missing.append(name)
                continue
            # The map's one value, past its header and the name.
            at = docs.index(b"\r\n") + 2
            at = docs.index(b"\r\n", at) + 2
            at = docs.index(b"\r\n", at) + 2
            table.write(b"$%d\r\n%s\r\n" % (len(name), name.encode()))
            table.write(entry)
            table.write(docs[at:])
    print("not in this Redis:", missing)
    pairs = parse_pairs(conn.call("CONFIG", "GET", "*"))
    pairs.sort()
    with open(os.path.join(opts.out, "config_defaults.txt"), "wb") as out:
        for name, value in pairs:
            out.write(name + b"\t" + value.replace(b"\\", b"\\\\").replace(b"\n", b"\\n") + b"\n")
    with open(os.path.join(opts.out, "config_kinds.txt"), "w") as out:
        out.write("\n".join(setting_kinds(conn, pairs)) + "\n")
    print(len(names) - len(missing), "commands,", len(pairs), "settings")


if __name__ == "__main__":
    main()
