#!/usr/bin/env python3
"""categorize.py OUTDIR [--examples]: the lost tests of compare.py, by what they need.

Rules go by the failure's text first, then the file and the test's name; the
first that matches wins. Tests that never ran count under what stopped their
file (its note), or "file stopped early".
"""
import collections
import json
import os
import re
import sys

out = sys.argv[1]
examples = "--examples" in sys.argv

# (category, regex on "file | test | why")
RULES = [
    ("keyspace notifications", r"notify-keyspace-events|Keyspace notifications"),
    ("replication (SYNC, REPLICAOF, WAIT, PSYNC)", r"SYNC'|REPLCONF|replication isn't there|replicaof|slaveof|integration/(replication|psync|block-repl|failover)|unit/wait|repl_|--replica|good replicas|min-replicas|master_link|role:slave"),
    ("CONFIG SET appendonly at runtime / AOF files", r"argument 'appendonly'|integration/aof|appendonlydir|aof-multi-part|aofrw|AOFRW|loadaof|aof_rewrite|bgrewriteaof|BGREWRITEAOF"),
    ("RDB files: SAVE/BGSAVE, DEBUG RELOAD, loading .rdb", r"snapshots are off|'reload'|'RELOAD'|DEBUG RELOAD|integration/rdb|corrupt-dump|convert-zip|\.rdb|rdb-key-save-delay|key-load-delay|rdb_|bgsave|BGSAVE|dbfilename"),
    ("OBJECT ENCODING and encoding settings", r"listpack|quicklist|skiplist|hashtable|intset|'int' to match|encoding:|Expected 'raw'|_encoding|assert_encoding|max-listpack|max-intset|quicklist-packed"),
    ("DEBUG subcommands (DIGEST, PROTOCOL, ...)", r"for 'digest|for 'protocol|for 'replybuffer|'quicklist-packed-threshold'|Try DEBUG HELP|debug sleep|DEBUG SLEEP|debug change-repl-id|stringmatch-len|set-disable-deny-scripts|jmap|for 'object'"),
    ("busy scripts: SCRIPT/FUNCTION KILL, timeouts", r"Timedout|script timeout|function kill|SCRIPT KILL|FUNCTION KILL|busy|BUSY"),
    ("client pause counted as blocked", r"unit/pause"),
    ("client eviction, output limits, query buffers", r"client-eviction|obuf-limits|querybuf|qbuf|client-output-buffer|maxmemory-clients|tot-mem|omem"),
    ("OOM checks (queued commands, scripts)", r"OOM|allow-oom|use-memory|deny oom"),
    ("logs, crash reports, startup and shutdown", r"integration/logging|unit/shutdown|integration/shutdown|verify_log|crash|log file|SIGTERM|shutdown-on|unit/oom-score-adj|oom-score|integration/dismiss-mem|unit/networking|bind|port"),
    ("memory: maxmemory, lazyfree, memefficiency", r"unit/maxmemory|lazyfree|memefficiency|used_memory|mem_fragmentation|defrag"),
    ("blocked clients count / blocking XREADGROUP", r"blocked clients|blocked_clients"),
    ("OBJECT IDLETIME, LRU and LFU values", r"idletime|lru|LRU|LFU|freq"),
    ("WATCH with expired keys", r"stale key|WATCH will consider touched expired"),
    ("INFO and CLIENT LIST fields", r"rejected_calls|number_of_cached_scripts|CLIENT LIST|CLIENT INFO|keyspace|keys=|dirty|expired_keys|errorstat|lazyfree"),
    ("MULTI left open by an earlier failure", r"calls can not be nested|WATCH inside MULTI"),
    ("COMMAND LIST and HELP output", r"COMMAND LIST|HELP commands|HELP should not"),
    ("slow log and latency events", r"SLOWLOG|expire-cycle|LATENCY"),
    ("protocol parsing", r"unit/protocol"),
    ("ACL extras", r"unit/acl"),
    ("TLS", r"unit/tls"),
]


def load(name):
    path = os.path.join(out, name)
    return json.load(open(path)) if os.path.exists(path) and os.path.getsize(path) else {}


for mode in ["external", "spawn"]:
    red, tan = load(f"redis-{mode}.json"), load(f"tantalum-{mode}.json")
    cats = collections.defaultdict(list)
    for f in sorted(red):
        r = {}
        for name, status, *_ in red[f]["tests"]:
            r.setdefault(name, status)
        t, why = {}, {}
        for name, status, *rest in tan.get(f, {}).get("tests", []):
            t.setdefault(name, status)
            why.setdefault(name, rest[0] if rest else "")
        note = " ".join(tan.get(f, {}).get("notes", []))
        for n, s in r.items():
            if s != "ok" or t.get(n) == "ok":
                continue
            text = f"{f} | {n} | {why.get(n) or ('NOT RUN ' + note)}"
            cat = next((c for c, rx in RULES if re.search(rx, text)), None)
            if cat is None:
                cat = "file stopped early, other" if n not in t else "other"
            cats[cat].append(text)
    total = sum(len(v) for v in cats.values())
    print(f"== {mode}: {total} lost")
    for cat, items in sorted(cats.items(), key=lambda kv: -len(kv[1])):
        files = collections.Counter(i.split(" | ")[0] for i in items)
        top = ", ".join(f"{f.split('/')[-1]} {c}" for f, c in files.most_common(4))
        print(f"  {len(items):4}  {cat}  ({top})")
        if examples and cat in ("other", "file stopped early, other"):
            for i in items[:60]:
                print(f"          {i[:200]}")
