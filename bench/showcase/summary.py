#!/usr/bin/env python3
"""Turns bench/showcase/run.sh's output into tables: python3 summary.py results.txt

Prints each example's median ops/s and p99 per server, how much more than Redis
and Valkey Tantalum did, memory, and anything that makes a number not count: a
failed check, or a server sending a different number of bytes per command, which
would mean it isn't answering the same thing.
"""
import re
import statistics
import sys
from collections import defaultdict

SERVERS = ["tantalum", "tantalum-uring", "redis", "valkey"]
BYTES_MAY_DIFFER = 0.03  # Keys and values are random: small differences are expected.


def main(path):
    ops = defaultdict(list)
    p99 = defaultdict(list)
    bytes_per_op = defaultdict(list)
    memory = {}
    problems = []
    order = []

    with open(path, errors="replace") as lines:
        for line in lines:
            line = line.rstrip("\n")
            if line.startswith("# ") and not line.startswith("# example"):
                print(line)
            if line.startswith("check ") and not line.startswith("check same"):
                problems.append(line)
            found = re.match(
                r"(\w+) round \d+ ([\w-]+) (P\d+) ops (\d+) misses (\S+) p50 (\S+) p99 (\S+) kb_per_op (\S+)",
                line,
            )
            if found:
                example, server, pipe = found.group(1, 2, 3)
                key = (example, pipe, server)
                ops[key].append(int(found.group(4)))
                p99[key].append(float(found.group(7)))
                bytes_per_op[key].append(float(found.group(8)))
                misses = found.group(5)
                if misses not in ("---", "0.00") and float(misses) > 0:
                    problems.append(f"misses on {example} {server} {pipe}: {misses}")
                if (example, pipe) not in order:
                    order.append((example, pipe))
            found = re.match(r"memory (\w+) ([\w-]+) rss_mb (\S+) bytes_per_item (\S+)", line)
            if found:
                memory[found.group(1, 2)] = (float(found.group(3)), float(found.group(4)))

    def median(table, example, pipe, server):
        values = table.get((example, pipe, server))
        return statistics.median(values) if values else None

    print("\n| Example | Pipeline | Tantalum | io_uring | Redis | Valkey | vs Redis | vs Valkey | p99 ms: Tantalum, Redis, Valkey |")
    print("|---|---|---|---|---|---|---|---|---|")
    for example, pipe in order:
        row = {s: median(ops, example, pipe, s) for s in SERVERS}
        tail = [median(p99, example, pipe, s) for s in ("tantalum", "redis", "valkey")]
        ahead = lambda other: f"{(row['tantalum'] / row[other] - 1) * 100:+.1f}%" if row["tantalum"] and row[other] else "?"
        cells = " | ".join(f"{row[s]:,.0f}" if row[s] else "?" for s in SERVERS)
        tails = ", ".join(f"{t:.2f}" if t is not None else "?" for t in tail)
        print(f"| {example} | {pipe} | {cells} | {ahead('redis')} | {ahead('valkey')} | {tails} |")

        sizes = {s: median(bytes_per_op, example, pipe, s) for s in SERVERS}
        known = [v for v in sizes.values() if v]
        if known and (max(known) - min(known)) / max(known) > BYTES_MAY_DIFFER:
            problems.append(f"bytes per command differ on {example} {pipe}: {sizes}")

    print("\n| Example | Tantalum MB | Redis MB | Valkey MB | Bytes an item: Tantalum, Redis, Valkey |")
    print("|---|---|---|---|---|")
    for example in dict.fromkeys(e for e, _ in memory):
        rss = [memory.get((example, s), (0, 0)) for s in ("tantalum", "redis", "valkey")]
        print(f"| {example} | {rss[0][0]:,.0f} | {rss[1][0]:,.0f} | {rss[2][0]:,.0f} | {rss[0][1]:.1f}, {rss[1][1]:.1f}, {rss[2][1]:.1f} |")

    print(f"\n{len(problems)} problems")
    for problem in problems:
        print(" ", problem)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
