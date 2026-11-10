#!/usr/bin/env python3
"""rt.py MODE SERVER OUT.json WORKERS FILE...: runs Redis 7.2.4's own tests, one
file at a time, on WORKERS copies of the test tree ($RT_WORK/SERVER-MODE-N, which
run.sh makes). The binaries are $TANTALUM_BIN and $REDIS_BIN.

MODE external: the harness talks to one server we start per file (fresh each
attempt), as Redis' CI "external server" job does. MODE spawn: the harness starts
its own servers from src/redis-server (real Redis, or the Tantalum wrapper).

--durable turns errors inside a test into failures. A test that hangs, or an
error outside --durable's reach, ends the run: that test is recorded and skipped,
and the file runs again, up to 12 times. Every test's first result is kept.
"""
import json
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time

MODE, SERVER, OUT, WORKERS = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
FILES = sys.argv[5:]
ANSI = re.compile(r"\x1b\[[0-9;]*m")
WORK = os.environ.get("RT_WORK", "/root/rt")
SERVERS = {
    "tantalum": [os.environ.get("TANTALUM_BIN", "/root/tantalum-rt"), "--enable-debug-command",
                 "yes", "--threads", "2"],
    "redis": [os.environ.get("REDIS_BIN", "/root/redis-724"), "--save", "", "--appendonly", "no", "--enable-debug-command",
              "yes", "--enable-protected-configs", "yes", "--enable-module-command", "yes"],
}
results, lock = {}, threading.Lock()


def start_external(port, logpath):
    log = open(logpath, "ab")
    p = subprocess.Popen(SERVERS[SERVER] + ["--port", str(port)], stdout=log, stderr=log,
                         start_new_session=True)
    for _ in range(200):
        try:
            socket.create_connection(("127.0.0.1", port), timeout=1).close()
            return p
        except OSError:
            time.sleep(0.05)
    return p


def stop(p):
    if p is None:
        return
    try:
        os.killpg(p.pid, signal.SIGKILL)
    except OSError:
        pass
    p.wait()


def run_file(w, f):
    tree = f"{WORK}/{SERVER}-{MODE}-{w}"
    port = 7600 + w * 10 + (0 if SERVER == "tantalum" else 5)
    base = 22000 + w * 700 + (0 if SERVER == "tantalum" else 5000)
    tests, order, skips, notes = {}, [], [], []
    started = time.time()
    for attempt in range(12):
        if time.time() - started > 1800:
            notes.append(f"gave up after 30 minutes, {len(tests)} tests")
            break
        cmd = ["tclsh", "tests/test_helper.tcl", "--single", f, "--clients", "1", "--durable",
               "--timeout", "180", "--baseport", str(base), "--portcount", "600"]
        srv = None
        if MODE == "external":
            srv = start_external(port, f"{WORK}/logs/{SERVER}-{w}.log")
            cmd += ["--host", "127.0.0.1", "--port", str(port)]
        for s in skips:
            cmd += ["--skiptest", s]
        p = subprocess.Popen(cmd, cwd=tree, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             start_new_session=True)
        try:
            out = p.communicate(timeout=2400)[0].decode(errors="replace")
        except subprocess.TimeoutExpired:
            stop(p)
            out = (p.stdout.read() or b"").decode(errors="replace") + "\n[TIMEOUT]: whole file\n"
        stop(p)
        stop(srv)
        lines = [ANSI.sub("", l) for l in out.splitlines()]
        current, done, broke = None, False, None
        for i, l in enumerate(lines):
            m = re.match(r"^\[(ok|err|skip|ignore|testing|exception|TIMEOUT)\]: (.*)$", l)
            if re.match(r"^\[\d+/\d+ done\]: ", l):
                done = True
            if not m:
                continue
            kind, rest = m.groups()
            if kind == "testing":
                current = rest
                continue
            if kind == "ok":
                name = re.sub(r" \(\d+ ms\)$", "", rest)
                status, why = "ok", ""
            elif kind == "err":
                name = re.sub(r" in tests/\S+\.tcl$", "", rest)
                status, why = "err", " | ".join(lines[i + 1:i + 3])[:300]
            elif kind == "ignore":
                name, _, why = rest.partition(": ")
                status = "ignore"
            elif kind == "skip":
                continue
            else:
                broke = (kind, " | ".join([rest] + lines[i + 1:i + 4])[:400])
                break
            if name not in tests:
                tests[name] = [status, why]
                order.append(name)
            if name == current:
                current = None
        if broke is None and not done:
            broke = ("ended", " | ".join(lines[-4:])[:400])
        if broke is None:
            break
        kind, why = broke
        if current and current not in tests:
            tests[current] = [kind.lower(), why]
            order.append(current)
            skips.append(current)
            continue
        notes.append(f"stopped outside a test ({kind}) after {len(tests)} tests: {why}")
        break
    with lock:
        results[f] = {"tests": [[n] + tests[n] for n in order], "attempts": attempt + 1,
                      "seconds": round(time.time() - started), "notes": notes}
        json.dump(results, open(OUT, "w"), indent=0)
        ok = sum(1 for n in order if tests[n][0] == "ok")
        print(f"{SERVER} {MODE} {f}: {ok}/{len(order)} ok, {attempt + 1} runs, "
              f"{round(time.time() - started)} s {'; ' + notes[0][:120] if notes else ''}", flush=True)


queue = list(FILES)


def worker(w):
    while True:
        with lock:
            if not queue:
                return
            f = queue.pop(0)
        run_file(w, f)


threads = [threading.Thread(target=worker, args=(w,)) for w in range(WORKERS)]
for t in threads:
    t.start()
for t in threads:
    t.join()
print("ALL-DONE", flush=True)
