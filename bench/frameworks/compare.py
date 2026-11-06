#!/usr/bin/env python3
"""Compares two runs of a suite, test by test.

    compare.py CONTROL_DIR RUN_DIR [--names]

Reads JUnit XML (pytest, PHPUnit) and minitest's -v lines. A test seen several
times (PHPUnit's data sets aside) counts as passed only if all of its runs passed.
"""
import pathlib
import re
import sys
import xml.etree.ElementTree as ET

LINE = re.compile(r"^(.+#.+) = \d+\.\d+ s = ([.FES])$")


def read(root):
    results = {}

    def seen(name, status):
        before = results.get(name)
        if before in (None, "skip") or status == "fail":
            results[name] = status

    for path in sorted(pathlib.Path(root).rglob("*.xml")):
        prefix = path.parent.name + "/" if path.parent != pathlib.Path(root) else ""
        try:
            tree = ET.parse(path)
        except ET.ParseError:
            seen(f"{prefix}{path.name} (no results: the run died)", "fail")
            continue
        for case in tree.iter("testcase"):
            name = f"{prefix}{case.get('classname') or case.get('class')}::{case.get('name')}"
            if case.find("failure") is not None or case.find("error") is not None:
                seen(name, "fail")
            elif case.find("skipped") is not None:
                seen(name, "skip")
            else:
                seen(name, "pass")
    for path in sorted(pathlib.Path(root).glob("*.txt")):
        for line in path.read_text(errors="replace").splitlines():
            m = LINE.match(line)
            if m:
                status = {".": "pass", "S": "skip"}.get(m.group(2), "fail")
                seen(f"{path.stem}/{m.group(1)}", status)
    return results


def counts(results):
    values = list(results.values())
    return f"{values.count('pass')} passed, {values.count('fail')} failed, {values.count('skip')} skipped"


control, run = read(sys.argv[1]), read(sys.argv[2])
print(f"control: {counts(control)}")
print(f"run:     {counts(run)}")
names = sorted(set(control) | set(run))
lost = [n for n in names if control.get(n) == "pass" and run.get(n) == "fail"]
gained = [n for n in names if control.get(n) == "fail" and run.get(n) == "pass"]
both = [n for n in names if control.get(n) == "fail" and run.get(n) == "fail"]
missing = [n for n in names if control.get(n) == "pass" and n not in run]
extra_skip = [n for n in names if control.get(n) == "pass" and run.get(n) == "skip"]
print(f"pass on control, fail on run: {len(lost)}")
print(f"fail on control, pass on run: {len(gained)}")
print(f"fail on both: {len(both)}")
print(f"pass on control, never ran on run: {len(missing)}")
print(f"pass on control, skipped on run: {len(extra_skip)}")
if "--names" in sys.argv:
    for title, group in (("lost", lost), ("gained", gained), ("both", both), ("missing", missing), ("skipped", extra_skip)):
        for name in group:
            print(f"{title}: {name}")
