#!/usr/bin/env python3
"""Stands in for src/redis-server in Redis' test tree, so its harness can start
Tantalum the way it starts Redis: `redis-server CONFIG [--name value...]`.

Settings Tantalum takes as flags become flags; the rest are sent with CONFIG SET
(ACL SETUSER for `user` lines) once it listens. Then it prints the two lines the
harness waits for, with the pid the harness knows: this process execs Tantalum,
and a forked helper does the rest. What couldn't be applied is printed too, as
"# wrapper:" lines in the server's stdout log.
"""
import os
import shlex
import socket
import sys
import time

TANTALUM = os.environ.get("TANTALUM_BIN", "/root/tantalum-rt")
FLAGS = {
    "port": "--port", "maxmemory": "--maxmemory", "maxmemory-policy": "--maxmemory-policy",
    "requirepass": "--requirepass", "aclfile": "--aclfile", "maxclients": "--maxclients",
    "timeout": "--timeout", "tcp-keepalive": "--tcp-keepalive", "databases": "--databases",
    "enable-debug-command": "--enable-debug-command",
}
IGNORED = {"daemonize", "pidfile", "logfile", "always-show-logo", "unixsocket", "dir",
           "dbfilename", "save", "appendonly", "appendfsync", "appenddirname", "bind",
           "enable-protected-configs", "enable-module-command", "supervised"}


def words(line):
    try:
        parts = shlex.split(line)
    except ValueError:
        parts = line.split()
    return ["" if p in ("{}", "''", '""') else p for p in parts]


def main():
    args = sys.argv[1:]
    if args in (["--version"], ["-v"]):
        os.execv(TANTALUM, [TANTALUM, "--version"])
    lines = []
    if args and not args[0].startswith("--"):
        with open(args.pop(0)) as f:
            for raw in f:
                raw = raw.strip()
                if raw and not raw.startswith("#"):
                    lines.append(words(raw))
    i = 0
    while i < len(args):
        if args[i].startswith("--"):
            name, vals = args[i][2:], []
            i += 1
            while i < len(args) and not args[i].startswith("--"):
                vals.append(args[i])
                i += 1
            lines.append([name] + vals)
        else:
            i += 1
    conf, users, later = {}, [], []
    for parts in lines:
        if not parts:
            continue
        name = parts[0].lower()
        if name == "user":
            users.append(parts[1:])
        else:
            conf[name] = parts[1:]
            later = [l for l in later if l[0] != name] + [[name] + parts[1:]]
    port = int((conf.get("port") or ["6379"])[0])
    d = (conf.get("dir") or ["."])[0] or "."
    flags = ["--threads", os.environ.get("TANTALUM_THREADS", "2")]
    for name, flag in FLAGS.items():
        if name in conf and conf[name]:
            flags += [flag, " ".join(conf[name])]
    bind = (conf.get("bind") or ["127.0.0.1"])[0] or "127.0.0.1"
    flags += ["--bind", "127.0.0.1" if bind in ("*", "0.0.0.0", "::", "-::*") else bind.lstrip("-")]
    snap = os.path.join(d, (conf.get("dbfilename") or ["dump.rdb"])[0])
    flags += ["--snapshot", snap]
    save = " ".join(conf.get("save") or [""]).strip()
    flags += ["--snapshot-interval", "0" if save in ("", '""', "''") else "60"]
    if (conf.get("appendonly") or ["no"])[0] == "yes":
        flags += ["--aof", os.path.join(d, (conf.get("appenddirname") or ["appendonlydir"])[0])]
        if (conf.get("appendfsync") or ["everysec"])[0] == "no":
            flags += ["--aof-fsync", "no"]
    password = " ".join(conf.get("requirepass") or []) or None
    pid = os.getpid()
    sys.stdout.flush()
    if os.fork() == 0:
        helper(pid, port, password, later, users, flags)
        os._exit(0)
    os.execv(TANTALUM, [TANTALUM] + flags)


def send(sock, *parts):
    out = b"*%d\r\n" % len(parts)
    for p in parts:
        p = p.encode() if isinstance(p, str) else p
        out += b"$%d\r\n%s\r\n" % (len(p), p)
    sock.sendall(out)
    data = b""
    while not data.endswith(b"\r\n"):
        chunk = sock.recv(65536)
        if not chunk:
            break
        data += chunk
    return data.decode(errors="replace").strip()


def alive(pid):
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().split(") ", 1)[1][0] not in "ZX"
    except OSError:
        return False


def helper(pid, port, password, later, users, flags):
    log = []
    sock = None
    for _ in range(400):
        if not alive(pid):
            break
        try:
            sock = socket.create_connection(("127.0.0.1", port), timeout=5)
            break
        except OSError:
            time.sleep(0.025)
    if sock is not None:
        if password:
            send(sock, "AUTH", password)
        for parts in later:
            name = parts[0]
            if name in FLAGS or name in IGNORED:
                continue
            reply = send(sock, "CONFIG", "SET", name, " ".join(parts[1:]))
            if not reply.startswith("+OK"):
                log.append(f"# wrapper: CONFIG SET {name} {' '.join(parts[1:])}: {reply}")
        for u in users:
            reply = send(sock, "ACL", "SETUSER", *u)
            if not reply.startswith("+OK"):
                log.append(f"# wrapper: ACL SETUSER {' '.join(u)}: {reply}")
        sock.close()
    else:
        log.append(f"# wrapper: Tantalum didn't start: {' '.join(flags)}")
    stamp = time.strftime("%d %b %Y %H:%M:%S.000")
    out = "\n".join(log + [f"                 PID: {pid}",
                           f"{pid}:M {stamp} * Server initialized",
                           f"{pid}:M {stamp} * Ready to accept connections tcp"]) + "\n"
    sys.stdout.write(out)
    sys.stdout.flush()


main()
