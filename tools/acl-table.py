#!/usr/bin/env python3
"""Writes src/acl_commands.txt: every command and subcommand of Redis 7.2.4
with its ACL categories, from Redis' own src/commands.def, so that ACL rules
name the commands Redis has (those Tantalum lacks too, as an ACL file copied
from a Redis names them) and categories hold what they hold in Redis.

    curl -fsSLO https://raw.githubusercontent.com/redis/redis/7.2.4/src/commands.def
    tools/acl-table.py commands.def > src/acl_commands.txt

Categories are the declared ones plus those Redis adds from the flags
(server.c's setImplicitACLCategories). Commands only Sentinel has are left out,
as Redis leaves them out when it isn't one.
"""

import re
import sys

CATEGORIES = [
    "keyspace", "read", "write", "set", "sortedset", "list", "hash", "string",
    "bitmap", "hyperloglog", "geo", "stream", "pubsub", "admin", "fast", "slow",
    "blocking", "dangerous", "connection", "transaction", "scripting",
]


def split_args(text):
    """Top-level comma-separated arguments of a C call, strings kept whole."""
    args, depth, current, i = [], 0, [], 0
    while i < len(text):
        c = text[i]
        if c == '"':
            j = i + 1
            while text[j] != '"':
                j += 2 if text[j] == "\\" else 1
            current.append(text[i : j + 1])
            i = j + 1
            continue
        if c in "({":
            depth += 1
        elif c in ")}":
            depth -= 1
        if c == "," and depth == 0:
            args.append("".join(current).strip())
            current = []
        else:
            current.append(c)
        i += 1
    args.append("".join(current).strip())
    return args


def make_cmd(line):
    """The fields of a MAKE_CMD(...) entry, and its subcommands table if any."""
    start = line.index("MAKE_CMD(") + len("MAKE_CMD(")
    depth, i = 1, start
    while depth:
        if line[i] == '"':
            i += 1
            while line[i] != '"':
                i += 2 if line[i] == "\\" else 1
        elif line[i] == "(":
            depth += 1
        elif line[i] == ")":
            depth -= 1
        i += 1
    fields = split_args(line[start : i - 1])
    sub = re.search(r"\.subcommands=(\w+)", line[i:])
    return fields, sub.group(1) if sub else None


def categories(flags, acl):
    names = set()
    for part in acl.split("|"):
        part = part.strip()
        if part.startswith("ACL_CATEGORY_"):
            names.add(part[len("ACL_CATEGORY_") :].lower())
    flags = {f.strip() for f in flags.split("|")}
    if "CMD_WRITE" in flags:
        names.add("write")
    if "CMD_READONLY" in flags and "scripting" not in names:
        names.add("read")
    if "CMD_ADMIN" in flags:
        names |= {"admin", "dangerous"}
    if "CMD_PUBSUB" in flags:
        names.add("pubsub")
    if "CMD_FAST" in flags:
        names.add("fast")
    if "CMD_BLOCKING" in flags:
        names.add("blocking")
    if "fast" not in names:
        names.add("slow")
    unknown = names - set(CATEGORIES)
    assert not unknown, unknown
    return [c for c in CATEGORIES if c in names], flags


def main():
    text = open(sys.argv[1]).read()
    tables, table = {}, None
    for line in text.splitlines():
        m = re.match(r"struct COMMAND_STRUCT (\w+)\[\] = \{", line)
        if m:
            table = tables.setdefault(m.group(1), [])
            continue
        if line.startswith("{MAKE_CMD(") and table is not None:
            table.append(make_cmd(line))
    rows = []

    def add(fields, parent):
        name = fields[0].strip('"')
        cats, flags = categories(fields[15], fields[16])
        if "CMD_ONLY_SENTINEL" in flags:
            return False
        full = f"{parent}|{name}" if parent else name
        extra = " no_auth" if "CMD_NO_AUTH" in flags else ""
        rows.append(f"{full} {','.join(cats)}{extra}")
        return True

    for fields, sub in tables["redisCommandTable"]:
        if add(fields, None) and sub:
            for subfields, _ in tables[sub]:
                add(subfields, fields[0].strip('"'))
    print("# Redis 7.2.4's commands and subcommands and their ACL categories, made by")
    print("# tools/acl-table.py from Redis' src/commands.def. A line: name, categories,")
    print("# and no_auth for those that run before AUTH.")
    for row in sorted(rows):
        print(row)


main()
