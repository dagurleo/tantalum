#!/usr/bin/env python3
"""Consumer groups with several clients: blocked consumers, WATCH, MULTI and scripts.
The same scenario on Redis (on 6390) and Tantalum (on 6391), every reply compared,
and whether a client is still waiting.

    tools/groups-blocking-diff.py [SCENARIO ...]
"""
import select, socket, sys, time

REDIS, TANTALUM = 6390, 6391


class Conn:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port))
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""

    def send(self, *args):
        args = [a if isinstance(a, bytes) else str(a).encode() for a in args]
        self.s.sendall(b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n%s\r\n" % (len(a), a) for a in args))

    def _fill(self, timeout):
        ready, _, _ = select.select([self.s], [], [], timeout)
        if not ready:
            raise TimeoutError
        data = self.s.recv(1 << 20)
        if not data:
            raise EOFError
        self.buf += data

    def _line(self, timeout):
        while b"\r\n" not in self.buf:
            self._fill(timeout)
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def reply(self, timeout=3.0):
        line = self._line(timeout)
        t, rest = line[:1], line[1:]
        if t in b"+-:":
            return line
        if t == b"_":
            return line
        if t == b"$":
            n = int(rest)
            if n < 0:
                return line
            while len(self.buf) < n + 2:
                self._fill(timeout)
            data, self.buf = self.buf[:n], self.buf[n + 2:]
            return data
        if t in b"*~":
            n = int(rest)
            return line if n < 0 else [self.reply(timeout) for _ in range(n)]
        if t == b"%":
            return ("map", [(self.reply(timeout), self.reply(timeout)) for _ in range(int(rest))])
        raise ValueError(line)

    def call(self, *args):
        self.send(*args)
        return self.reply()

    def silent(self, wait=0.15):
        """Whether no reply comes for a moment."""
        try:
            return ("got", self.reply(wait))
        except TimeoutError:
            return "still blocked"


def on(port, scenario):
    log = []
    conns = [Conn(port) for _ in range(4)]
    conns[0].call("FLUSHALL")
    scenario(conns, log)
    return log


def blocked(args=(), key="s", group="g", consumer="c"):
    return ["XREADGROUP", "GROUP", group, consumer, *args, "BLOCK", 0, "STREAMS", key, ">"]


def setup(a):
    a.call("XGROUP", "CREATE", "s", "g", "$", "MKSTREAM")


def scenario_served(c, log):
    a, b, *_ = c
    setup(a)
    a.send(*blocked())
    log.append(a.silent())
    log.append(b.call("XADD", "s", "1-1", "f", "v"))
    log.append(a.reply())
    log.append(b.call("XPENDING", "s", "g"))


def trigger(*cmd, before=()):
    def scenario(c, log):
        a, b, *_ = c
        setup(a)
        for prep in before:
            log.append(b.call(*prep))
        a.send(*blocked())
        log.append(a.silent())
        log.append(b.call(*cmd))
        log.append(a.silent(1.0))
        log.append(b.call("EXISTS", "s"))
    return scenario


def scenario_expire(c, log):
    a, b, *_ = c
    setup(a)
    a.send(*blocked())
    log.append(a.silent())
    log.append(b.call("PEXPIRE", "s", 50))
    # Nothing else runs: active expiry has to wake it.
    log.append(a.reply(5.0))


def scenario_timeout(c, log):
    a, b, *_ = c
    setup(a)
    a.send("XREADGROUP", "GROUP", "g", "c", "BLOCK", 100, "STREAMS", "s", ">")
    log.append(a.reply(3.0))
    log.append(b.call("XINFO", "GROUPS", "s"))


def scenario_delconsumer(c, log):
    a, b, *_ = c
    setup(a)
    a.send(*blocked())
    log.append(a.silent())
    log.append(b.call("XGROUP", "DELCONSUMER", "s", "g", "c"))
    log.append(a.silent())
    log.append(b.call("XADD", "s", "1-1", "f", "v"))
    log.append(a.reply())
    log.append(b.call("XPENDING", "s", "g"))


def scenario_two_consumers(c, log):
    a, b, d, _ = c
    setup(a)
    a.send(*blocked(consumer="c1"))
    log.append(a.silent())
    d.send(*blocked(consumer="c2"))
    log.append(d.silent())
    log.append(b.call("XADD", "s", "1-1", "f", "v"))
    log.append(a.reply())
    log.append(d.silent())
    log.append(b.call("XADD", "s", "2-1", "f", "v"))
    log.append(d.reply())
    log.append(b.call("XPENDING", "s", "g"))


def scenario_noack_and_count(c, log):
    a, b, *_ = c
    setup(a)
    a.send(*blocked(args=("COUNT", 1, "NOACK")))
    log.append(a.silent())
    log.append(b.call("MULTI"))
    log.append(b.call("XADD", "s", "1-1", "f", "v"))
    log.append(b.call("XADD", "s", "2-1", "f", "v"))
    log.append(b.call("EXEC"))
    log.append(a.reply())
    log.append(b.call("XPENDING", "s", "g"))
    log.append(b.call("XINFO", "GROUPS", "s"))


def scenario_many_streams(c, log):
    a, b, *_ = c
    setup(a)
    a.call("XGROUP", "CREATE", "s2", "g", "$", "MKSTREAM")
    a.send("XREADGROUP", "GROUP", "g", "c", "BLOCK", 0, "STREAMS", "s", "s2", ">", ">")
    log.append(a.silent())
    log.append(b.call("XADD", "s2", "1-1", "f", "v"))
    log.append(a.reply())
    log.append(b.call("XINFO", "GROUPS", "s2"))
    log.append(b.call("XINFO", "GROUPS", "s"))
    a.send("XREADGROUP", "GROUP", "g", "c", "BLOCK", 0, "STREAMS", "s", "s2", ">", ">")
    log.append(a.silent())
    log.append(b.call("DEL", "s"))
    log.append(a.silent(1.0))


def scenario_added_and_gone(c, log):
    a, b, *_ = c
    setup(a)
    a.send(*blocked())
    log.append(a.silent())
    for cmd in (["MULTI"], ["XADD", "s", "1-1", "f", "v"], ["XDEL", "s", "1-1"], ["EXEC"]):
        log.append(b.call(*cmd))
    log.append(a.silent())
    log.append(b.call("XADD", "s", "2-1", "f", "v"))
    log.append(a.reply())


def scenario_recreated(c, log):
    a, b, *_ = c
    setup(a)
    a.send(*blocked())
    log.append(a.silent())
    for cmd in (["MULTI"], ["DEL", "s"], ["XGROUP", "CREATE", "s", "g", "0", "MKSTREAM"], ["EXEC"]):
        log.append(b.call(*cmd))
    log.append(a.silent())
    log.append(b.call("XADD", "s", "2-1", "f", "v"))
    log.append(a.reply())


def scenario_in_multi(c, log):
    a, b, *_ = c
    setup(a)
    for cmd in (["MULTI"], blocked(), ["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", "0"], ["EXEC"]):
        log.append(a.call(*cmd))
    log.append(a.call("XINFO", "GROUPS", "s"))


def watched(*cmd, entry=True, group=True, before=()):
    def scenario(c, log):
        a, b, *_ = c
        for prep in (["XADD", "s", "1-1", "f", "v"], ["XADD", "s", "2-1", "f", "v"], ["XGROUP", "CREATE", "s", "g", "0"],
                     ["XREADGROUP", "GROUP", "g", "c", "COUNT", 1, "STREAMS", "s", ">"]):
            log.append(a.call(*prep))
        log.append(a.call("WATCH", "s"))
        log.append(b.call(*cmd))
        log.append(a.call("MULTI"))
        log.append(a.call("PING"))
        log.append(a.call("EXEC"))
    return scenario


def scenario_scripts(c, log):
    a, b, *_ = c
    setup(a)
    a.call("XADD", "s", "1-1", "f", "v")
    a.call("XADD", "s", "2-1", "f", "v")
    for script, keys in [
        ("return redis.call('XREADGROUP','GROUP','g','c','BLOCK',0,'STREAMS',KEYS[1],'>')", ["s"]),
        ("return redis.call('XREADGROUP','GROUP','g','c','COUNT',1,'STREAMS',KEYS[1],'>')", ["s"]),
        ("return redis.call('XACK',KEYS[1],'g','1-1')", ["s"]),
        ("return redis.call('XPENDING',KEYS[1],'g')", ["s"]),
        ("return redis.call('XINFO','GROUPS',KEYS[1])", ["s"]),
        ("return redis.call('XGROUP','CREATE',KEYS[1],'g','0')", ["s"]),
        ("return redis.pcall('XGROUP','CREATE',KEYS[1],'g','0')", ["s"]),
        ("return redis.call('XAUTOCLAIM',KEYS[1],'g','z',0,'0')", ["s"]),
        ("return redis.call('XSETID',KEYS[1],'9-9')", ["s"]),
    ]:
        log.append(a.call("EVAL", script, len(keys), *keys))
    log.append(a.call("EVAL_RO", "return redis.call('XACK',KEYS[1],'g','1-1')", 1, "s"))
    log.append(a.call("EVAL_RO", "return redis.call('XPENDING',KEYS[1],'g')", 1, "s"))
    log.append(a.call("OBJECT", "ENCODING", "s"))
    log.append(a.call("TYPE", "s"))


def scenario_resp3(c, log):
    a, b, *_ = c
    a.call("HELLO", 3)
    setup(a)
    a.send(*blocked())
    log.append(a.silent())
    log.append(b.call("XADD", "s", "1-1", "f", "v"))
    log.append(a.reply())
    a.send("XREADGROUP", "GROUP", "g", "c", "BLOCK", 100, "STREAMS", "s", ">")
    log.append(a.reply())
    a.send(*blocked())
    log.append(a.silent())
    log.append(b.call("XGROUP", "DESTROY", "s", "g"))
    log.append(a.reply())


def scenario_copy_keeps_groups(c, log):
    a, b, *_ = c
    setup(a)
    a.call("XADD", "s", "1-1", "f", "v")
    a.call("XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">")
    log.append(a.call("COPY", "s", "t"))
    log.append(a.call("XACK", "s", "g", "1-1"))
    log.append(a.call("XPENDING", "t", "g"))
    log.append(a.call("XPENDING", "s", "g"))
    log.append(a.call("RENAME", "t", "u"))
    log.append(a.call("XPENDING", "u", "g"))


SCENARIOS = {
    "served by XADD": scenario_served,
    "group destroyed": trigger("XGROUP", "DESTROY", "s", "g"),
    "another group destroyed": trigger("XGROUP", "CREATE", "s", "other", "$") ,
    "key deleted": trigger("DEL", "s"),
    "key unlinked": trigger("UNLINK", "s"),
    "overwritten by SET": trigger("SET", "s", "x"),
    "renamed away": trigger("RENAME", "s", "elsewhere"),
    "renamed over": trigger("RENAME", "t", "s", before=(["XADD", "t", "5-1", "f", "v"],)),
    "renamed over, same group": trigger("RENAME", "t", "s", before=(["XGROUP", "CREATE", "t", "g", "$", "MKSTREAM"],)),
    "copied over": trigger("COPY", "t", "s", "REPLACE", before=(["XADD", "t", "5-1", "f", "v"],)),
    "flushed": trigger("FLUSHALL"),
    "flushed db": trigger("FLUSHDB"),
    "group moved back": trigger("XGROUP", "SETID", "s", "g", "0", before=(["XADD", "s", "1-1", "f", "v"], ["XGROUP", "SETID", "s", "g", "$"])),
    "XACK leaves it": trigger("XACK", "s", "g", "1-1"),
    "XSETID leaves it": trigger("XSETID", "s", "5-5"),
    "XTRIM leaves it": trigger("XTRIM", "s", "MAXLEN", 0),
    "expired": scenario_expire,
    "timed out": scenario_timeout,
    "consumer deleted meanwhile": scenario_delconsumer,
    "two consumers": scenario_two_consumers,
    "NOACK and COUNT": scenario_noack_and_count,
    "many streams": scenario_many_streams,
    "added and gone": scenario_added_and_gone,
    "deleted and made again": scenario_recreated,
    "in MULTI": scenario_in_multi,
    "scripts": scenario_scripts,
    "RESP3": scenario_resp3,
    "COPY and RENAME keep groups": scenario_copy_keeps_groups,
    "WATCH: XADD": watched("XADD", "s", "3-1", "f", "v"),
    "WATCH: XDEL": watched("XDEL", "s", "1-1"),
    "WATCH: XTRIM": watched("XTRIM", "s", "MAXLEN", 1),
    "WATCH: DEL": watched("DEL", "s"),
    "WATCH: XACK": watched("XACK", "s", "g", "1-1"),
    "WATCH: XREADGROUP": watched("XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"),
    "WATCH: XREADGROUP new consumer": watched("XREADGROUP", "GROUP", "g", "c2", "COUNT", 0, "STREAMS", "s", "0"),
    "WATCH: XCLAIM": watched("XCLAIM", "s", "g", "z", 0, "1-1"),
    "WATCH: XAUTOCLAIM": watched("XAUTOCLAIM", "s", "g", "z", 0, "0"),
    "WATCH: XSETID": watched("XSETID", "s", "9-9"),
    "WATCH: XGROUP CREATE": watched("XGROUP", "CREATE", "s", "g2", "0"),
    "WATCH: XGROUP DESTROY": watched("XGROUP", "DESTROY", "s", "g"),
    "WATCH: XGROUP SETID": watched("XGROUP", "SETID", "s", "g", "0"),
    "WATCH: XGROUP CREATECONSUMER": watched("XGROUP", "CREATECONSUMER", "s", "g", "n"),
    "WATCH: XGROUP DELCONSUMER": watched("XGROUP", "DELCONSUMER", "s", "g", "c"),
    "WATCH: XGROUP CREATE MKSTREAM": watched("XGROUP", "CREATE", "other", "g", "0", "MKSTREAM"),
}


def main():
    only = sys.argv[1:] or SCENARIOS
    bad = 0
    for name in only:
        scenario = SCENARIOS[name]
        try:
            redis = on(REDIS, scenario)
        except Exception as e:
            redis = f"raised {e!r}"
        try:
            tantalum = on(TANTALUM, scenario)
        except Exception as e:
            tantalum = f"raised {e!r}"
        if isinstance(redis, str) or isinstance(tantalum, str):
            bad += 1
            print(f"RAISED  {name}\n  redis    {redis!r}\n  tantalum {tantalum!r}")
        elif redis == tantalum:
            print(f"same  {name}  {repr(redis)[:230]}")
        else:
            bad += 1
            print(f"DIFF  {name}\n  redis    {redis!r}\n  tantalum {tantalum!r}")
    print(f"{len(only)} scenarios, {bad} differences")
    sys.exit(1 if bad else 0)


main()
