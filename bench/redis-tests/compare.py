#!/usr/bin/env python3
"""compare.py OUTDIR: Redis' own tests, Tantalum against Redis 7.2.4, in each mode.

A test is lost when it passed on Redis and didn't on Tantalum (failed, hung,
aborted its file, or never ran because its file stopped first). Prints per mode
the totals, then per file what was lost, with the first line of why.
"""
import collections
import json
import os
import sys

out = sys.argv[1]
detail = "--detail" in sys.argv


def load(name):
    path = os.path.join(out, name)
    return json.load(open(path)) if os.path.exists(path) and os.path.getsize(path) else {}


for mode in ["external", "spawn"]:
    red, tan = load(f"redis-{mode}.json"), load(f"tantalum-{mode}.json")
    if not red:
        continue
    totals = collections.Counter()
    per_file = []
    for f in sorted(red):
        r = {}
        for name, status, *_ in red[f]["tests"]:
            r.setdefault(name, status)
        t = {}
        why = {}
        for name, status, *rest in tan.get(f, {}).get("tests", []):
            t.setdefault(name, status)
            why.setdefault(name, (rest[0] if rest else "")[:160])
        ok_r = [n for n, s in r.items() if s == "ok"]
        lost = [n for n in ok_r if t.get(n) != "ok"]
        gained = [n for n, s in t.items() if s == "ok" and r.get(n) != "ok"]
        totals["redis ok"] += len(ok_r)
        totals["tantalum ok"] += sum(1 for n in ok_r if t.get(n) == "ok")
        totals["lost"] += len(lost)
        totals["not run"] += sum(1 for n in lost if n not in t)
        totals["gained"] += len(gained)
        notes = tan.get(f, {}).get("notes", [])
        if lost or notes:
            per_file.append((f, len(ok_r), len(lost), lost, why, t, notes))
    print(f"== {mode}: Redis passed {totals['redis ok']}, Tantalum {totals['tantalum ok']} of those; "
          f"lost {totals['lost']} ({totals['not run']} never ran), Tantalum alone passed {totals['gained']}")
    for f, ok, nlost, lost, why, t, notes in sorted(per_file, key=lambda x: -x[2]):
        print(f"  {f}: lost {nlost} of {ok}" + (f"  [{notes[0][:150]}]" if notes else ""))
        if detail:
            for n in lost:
                print(f"      {t.get(n, 'not run'):9} {n[:90]} | {why.get(n, '')[:140]}")
