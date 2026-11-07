#!/usr/bin/env python3
"""HyperLogLog and geo commands against a real Redis: the same commands to both,
comparing replies byte for byte, in both protocols, and what they stored. First
every error and edge by hand, then random work, in seeds.

    tools/geo-hll-diff.py [--redis 7390] [--tantalum 7391] [--seeds 1,2,3] [--steps 4000]

Start the servers yourself on spare ports:

    redis-server --port 7390 --save '' --appendonly no &
    target/release/tantalum --port 7391 &
"""

import argparse
import random
import socket
import sys


class Conn:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port))
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.f = self.sock.makefile("rb")

    def send(self, args):
        out = b"*%d\r\n" % len(args)
        for a in args:
            a = a if isinstance(a, bytes) else str(a).encode()
            out += b"$%d\r\n%s\r\n" % (len(a), a)
        self.sock.sendall(out)

    def read(self):
        line = self.f.readline()
        if not line:
            return b"(closed)"
        kind, rest = line[:1], line[1:-2]
        if kind in b"$=!":
            n = int(rest)
            return line + (self.f.read(n + 2) if n >= 0 else b"")
        if kind in b"*~>":
            n = int(rest)
            return line + b"".join(self.read() for _ in range(max(n, 0)))
        if kind in b"%|":
            return line + b"".join(self.read() for _ in range(2 * int(rest)))
        return line

    def call(self, *args):
        self.send(args)
        return self.read()


class Both:
    def __init__(self, ports):
        self.redis = Conn(ports[0])
        self.tantalum = Conn(ports[1])
        self.compared = 0
        self.differences = 0
        self.by_command = {}

    def call(self, *args):
        a = self.redis.call(*args)
        b = self.tantalum.call(*args)
        self.compared += 1
        if a != b:
            self.differences += 1
            # All of them, shortened, but only a few of each command.
            self.by_command[args[0]] = self.by_command.get(args[0], 0) + 1
            if self.by_command[args[0]] <= 6:
                shown = [x if isinstance(x, (str, int, float)) else repr(x)[:60] for x in args]
                print(f"DIFF {shown}\n   redis    {a[:300]!r}\n   tantalum {b[:300]!r}")
        return a

    def resp(self, version):
        self.redis.call("HELLO", version)
        self.tantalum.call("HELLO", version)


HLL_BY_HAND = [
    ("PFADD", "h"),
    ("GET", "h"),
    ("PFADD", "h"),
    ("PFCOUNT", "h"),
    ("PFADD", "h", "a"),
    ("GET", "h"),
    ("PFADD", "h", "a"),
    ("PFADD", "h", "b", "c", "a"),
    ("PFCOUNT", "h"),
    ("GET", "h"),
    ("PFCOUNT", "h"),
    ("PFCOUNT", "nothing"),
    ("PFCOUNT", "nothing", "either"),
    ("PFCOUNT", "h", "nothing"),
    ("PFDEBUG", "ENCODING", "h"),
    ("PFDEBUG", "encoding", "h"),
    ("PFDEBUG", "DECODE", "h"),
    ("PFDEBUG", "NOPE", "h"),
    ("PFDEBUG", "ENCODING", "nothing"),
    ("PFDEBUG", "ENCODING"),
    ("PFDEBUG", "ENCODING", "h", "more"),
    ("PFDEBUG", "GETREG", "h"),
    ("PFDEBUG", "DECODE", "h"),
    ("PFDEBUG", "ENCODING", "h"),
    ("PFDEBUG", "TODENSE", "h"),
    ("STRLEN", "h"),
    ("PFADD", "h2", "x", "y"),
    ("PFMERGE", "out", "h", "h2"),
    ("PFDEBUG", "ENCODING", "out"),
    ("GET", "out"),
    ("PFCOUNT", "out"),
    ("PFMERGE", "only"),
    ("EXISTS", "only"),
    ("GET", "only"),
    ("PFMERGE", "only2", "nothing"),
    ("GET", "only2"),
    ("PFCOUNT", "h", "h2"),
    ("SET", "str", "hello"),
    ("PFADD", "str", "a"),
    ("PFCOUNT", "str"),
    ("PFCOUNT", "str", "h"),
    ("PFMERGE", "str", "h"),
    ("PFMERGE", "h", "str"),
    ("PFDEBUG", "ENCODING", "str"),
    ("SET", "num", "12345678901234567890"),
    ("PFADD", "num", "a"),
    ("LPUSH", "list", "a"),
    ("PFADD", "list", "a"),
    ("PFCOUNT", "list"),
    ("PFCOUNT", "list", "h"),
    ("PFMERGE", "list", "h"),
    ("PFMERGE", "h", "list"),
    ("PFDEBUG", "ENCODING", "list"),
    ("PFADD",),
    ("PFCOUNT",),
    ("PFMERGE",),
    # TTLs stay, as it's the same value changing.
    ("PFADD", "ttl", "a"),
    ("EXPIRE", "ttl", "100"),
    ("PFADD", "ttl", "b"),
    ("PFCOUNT", "ttl"),
    ("TTL", "ttl"),
    ("PFMERGE", "ttl", "h"),
    ("TTL", "ttl"),
    # Corrupt ones.
    ("SET", "bad", b"HYLL"),
    ("PFADD", "bad", "a"),
    ("SET", "bad", b"HYLL\x02" + b"\0" * 20),
    ("PFADD", "bad", "a"),
    ("PFCOUNT", "bad"),
    ("SET", "bad", b"HYLL\x00" + b"\0" * 20),
    ("PFADD", "bad", "a"),
    ("PFCOUNT", "bad"),
    ("DEL", "bad"),
    ("PFADD", "bad", "a", "b", "c"),
    ("APPEND", "bad", "hello"),
    ("PFCOUNT", "bad"),
    ("PFADD", "bad", "d"),
    ("PFMERGE", "m", "bad"),
    ("PFCOUNT", "bad", "h"),
    ("PFDEBUG", "GETREG", "bad"),
    ("PFDEBUG", "DECODE", "bad"),
    ("PFDEBUG", "TODENSE", "bad"),
    # The limit.
    ("CONFIG", "GET", "hll-sparse-max-bytes"),
    ("CONFIG", "SET", "hll-sparse-max-bytes", "30"),
    ("DEL", "small"),
    ("PFADD", "small", "1", "2", "3"),
    ("PFDEBUG", "ENCODING", "small"),
    ("PFADD", "small", "4", "5", "6", "7", "8", "9", "10", "11", "12"),
    ("PFDEBUG", "ENCODING", "small"),
    ("GET", "small"),
    ("CONFIG", "SET", "hll-sparse-max-bytes", "3000"),
    ("MULTI",),
    ("PFADD", "tx", "a"),
    ("PFCOUNT", "tx"),
    ("EXEC",),
    ("EVAL", "return redis.call('PFADD', KEYS[1], 'a', 'b')", "1", "lua"),
    ("EVAL", "return redis.call('PFCOUNT', KEYS[1])", "1", "lua"),
    ("EVAL_RO", "return redis.call('PFCOUNT', KEYS[1])", "1", "lua"),
    ("COMMAND", "GETKEYS", "PFCOUNT", "a", "b"),
    ("COMMAND", "GETKEYS", "PFMERGE", "a", "b"),
    ("COMMAND", "GETKEYS", "PFDEBUG", "ENCODING", "a"),
]

GEO_BY_HAND = [
    ("GEOADD", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"),
    ("ZRANGE", "Sicily", "0", "-1", "WITHSCORES"),
    ("GEOPOS", "Sicily", "Palermo", "Catania", "NonExisting"),
    ("GEOPOS", "nothing", "a"),
    ("GEOPOS", "Sicily"),
    ("GEOHASH", "Sicily", "Palermo", "Catania", "NonExisting"),
    ("GEOHASH", "nothing", "a"),
    ("GEODIST", "Sicily", "Palermo", "Catania"),
    ("GEODIST", "Sicily", "Palermo", "Catania", "km"),
    ("GEODIST", "Sicily", "Palermo", "Catania", "MI"),
    ("GEODIST", "Sicily", "Palermo", "Catania", "ft"),
    ("GEODIST", "Sicily", "Palermo", "Palermo"),
    ("GEODIST", "Sicily", "Palermo", "Catania", "parsec"),
    ("GEODIST", "Sicily", "Palermo", "Catania", "km", "more"),
    ("GEODIST", "Sicily", "Foo", "Bar"),
    ("GEODIST", "nothing", "Foo", "Bar"),
    ("GEORADIUS", "Sicily", "15", "37", "200", "km"),
    ("GEORADIUS", "Sicily", "15", "37", "200", "km", "WITHDIST"),
    ("GEORADIUS", "Sicily", "15", "37", "200", "km", "WITHCOORD"),
    ("GEORADIUS", "Sicily", "15", "37", "200", "km", "WITHHASH"),
    ("GEORADIUS", "Sicily", "15", "37", "200", "km", "WITHDIST", "WITHCOORD", "WITHHASH"),
    ("GEORADIUS", "Sicily", "15", "37", "200", "km", "ASC"),
    ("GEORADIUS", "Sicily", "15", "37", "200", "km", "DESC"),
    ("GEORADIUS", "Sicily", "15", "37", "200", "km", "COUNT", "1"),
    ("GEORADIUS", "Sicily", "15", "37", "200", "km", "COUNT", "1", "DESC"),
    ("GEORADIUS", "Sicily", "15", "37", "200", "km", "COUNT", "1", "ANY"),
    ("GEORADIUS", "Sicily", "15", "37", "200", "km", "ANY"),
    ("GEORADIUS", "Sicily", "15", "37", "200", "km", "COUNT", "0"),
    ("GEORADIUS", "Sicily", "15", "37", "200", "km", "COUNT", "-1"),
    ("GEORADIUS", "Sicily", "15", "37", "200", "km", "COUNT", "x"),
    ("GEORADIUS", "Sicily", "15", "37", "200", "km", "COUNT"),
    ("GEORADIUS", "Sicily", "15", "37", "200", "km", "NOPE"),
    ("GEORADIUS", "Sicily", "15", "37", "200", "parsec"),
    ("GEORADIUS", "Sicily", "15", "37", "-1", "km"),
    ("GEORADIUS", "Sicily", "15", "37", "x", "km"),
    ("GEORADIUS", "Sicily", "x", "37", "200", "km"),
    ("GEORADIUS", "Sicily", "15", "x", "200", "km"),
    ("GEORADIUS", "Sicily", "181", "37", "200", "km"),
    ("GEORADIUS", "Sicily", "15", "86", "200", "km"),
    ("GEORADIUS", "Sicily", "inf", "37", "200", "km"),
    ("GEORADIUS", "Sicily", "15", "37", "0", "km"),
    ("GEORADIUS", "Sicily", "15", "37", "inf", "km"),
    ("GEORADIUS", "Sicily", "15", "37", "200", "km", "STORE", "out"),
    ("ZRANGE", "out", "0", "-1", "WITHSCORES"),
    ("GEORADIUS", "Sicily", "15", "37", "200", "km", "STOREDIST", "out"),
    ("ZRANGE", "out", "0", "-1", "WITHSCORES"),
    ("GEORADIUS", "Sicily", "15", "37", "200", "km", "STORE"),
    ("GEORADIUS", "Sicily", "15", "37", "200", "km", "STORE", "out", "WITHDIST"),
    ("GEORADIUS", "Sicily", "15", "37", "1", "m", "STORE", "out"),
    ("EXISTS", "out"),
    ("SET", "out", "a string"),
    ("EXPIRE", "out", "100"),
    ("GEORADIUS", "Sicily", "15", "37", "200", "km", "STORE", "out"),
    ("TYPE", "out"),
    ("TTL", "out"),
    ("GEORADIUS", "nothing", "15", "37", "200", "km"),
    ("GEORADIUS", "nothing", "15", "37", "200", "km", "STORE", "out"),
    ("EXISTS", "out"),
    ("GEORADIUS", "nothing", "x", "37", "200", "km"),
    ("GEORADIUS", "nothing", "15", "37", "200", "km", "NOPE"),
    ("GEORADIUS_RO", "Sicily", "15", "37", "200", "km", "WITHDIST"),
    ("GEORADIUS_RO", "Sicily", "15", "37", "200", "km", "STORE", "out"),
    ("GEORADIUS_RO", "Sicily", "15", "37", "200", "km", "STOREDIST", "out"),
    ("GEORADIUSBYMEMBER", "Sicily", "Palermo", "200", "km"),
    ("GEORADIUSBYMEMBER", "Sicily", "Palermo", "200", "km", "WITHDIST", "ASC"),
    ("GEORADIUSBYMEMBER", "Sicily", "Palermo", "200", "km", "STORE", "out"),
    ("ZRANGE", "out", "0", "-1", "WITHSCORES"),
    ("GEORADIUSBYMEMBER", "Sicily", "NonExisting", "200", "km"),
    ("GEORADIUSBYMEMBER", "nothing", "a", "200", "km"),
    ("GEORADIUSBYMEMBER", "nothing", "a", "x", "parsec"),
    ("GEORADIUSBYMEMBER", "nothing", "a", "x", "parsec", "STORE", "out"),
    ("GEORADIUSBYMEMBER_RO", "Sicily", "Palermo", "200", "km"),
    ("GEORADIUSBYMEMBER_RO", "Sicily", "Palermo", "200", "km", "STORE", "out"),
    ("GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC"),
    ("GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYBOX", "400", "400", "km", "ASC", "WITHCOORD", "WITHDIST"),
    ("GEOSEARCH", "Sicily", "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km", "WITHHASH"),
    ("GEOSEARCH", "Sicily", "FROMMEMBER", "NonExisting", "BYRADIUS", "200", "km"),
    ("GEOSEARCH", "Sicily", "FROMMEMBER", "Palermo", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"),
    ("GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km"),
    ("GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "BYBOX", "1", "1", "km"),
    ("GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYBOX", "1", "1", "km", "BYRADIUS", "200", "km"),
    ("GEOSEARCH", "Sicily", "BYRADIUS", "200", "km"),
    ("geosearch", "Sicily", "BYRADIUS", "200", "km"),
    ("GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37"),
    ("GeoSearch", "Sicily", "FROMLONLAT", "15", "37"),
    ("GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYBOX", "x", "1", "km"),
    ("GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYBOX", "1", "x", "km"),
    ("GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYBOX", "-1", "1", "km"),
    ("GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYBOX", "1", "1", "parsec"),
    ("GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "STORE", "out"),
    ("GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "STOREDIST"),
    ("GEOSEARCH", "Sicily", "FROMLONLAT", "15"),
    ("GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "200"),
    ("GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYBOX", "200", "200"),
    ("GEOSEARCH", "nothing", "FROMMEMBER", "a", "BYRADIUS", "200", "km"),
    ("GEOSEARCH", "nothing", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"),
    ("GEOSEARCH", "nothing", "FROMLONLAT", "x", "37", "BYRADIUS", "200", "km"),
    ("GEOSEARCH", "nothing", "BYRADIUS", "200", "km"),
    ("GEOSEARCHSTORE", "out", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"),
    ("ZRANGE", "out", "0", "-1", "WITHSCORES"),
    ("GEOSEARCHSTORE", "out", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "STOREDIST"),
    ("ZRANGE", "out", "0", "-1", "WITHSCORES"),
    ("GEOSEARCHSTORE", "out", "Sicily", "FROMLONLAT", "15", "37", "BYBOX", "400", "400", "km", "COUNT", "1"),
    ("ZRANGE", "out", "0", "-1", "WITHSCORES"),
    ("GEOSEARCHSTORE", "out", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "WITHDIST"),
    ("GEOSEARCHSTORE", "out", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "STORE", "x"),
    ("GEOSEARCHSTORE", "out", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "1", "m"),
    ("EXISTS", "out"),
    ("GEOSEARCHSTORE", "out", "nothing", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"),
    ("GEOSEARCHSTORE", "Sicily2", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"),
    ("GEOSEARCHSTORE", "Sicily2", "Sicily2", "FROMLONLAT", "15", "37", "BYRADIUS", "160", "km"),
    ("ZRANGE", "Sicily2", "0", "-1", "WITHSCORES"),
    # GEOADD's options and errors.
    ("GEOADD", "g", "NX", "1", "2", "a"),
    ("GEOADD", "g", "NX", "3", "4", "a"),
    ("GEOPOS", "g", "a"),
    ("GEOADD", "g", "XX", "3", "4", "a"),
    ("GEOPOS", "g", "a"),
    ("GEOADD", "g", "XX", "3", "4", "b"),
    ("GEOADD", "g", "CH", "5", "6", "a", "7", "8", "c"),
    ("GEOADD", "g", "XX", "CH", "5", "7", "a"),
    ("GEOADD", "g", "ch", "xx", "5", "7", "a"),
    ("GEOADD", "g", "NX", "XX", "1", "2", "z"),
    ("GEOADD", "g", "GT", "1", "2", "z"),
    ("GEOADD", "g", "LT", "1", "2", "z"),
    ("GEOADD", "g", "INCR", "1", "2", "z"),
    ("GEOADD", "g", "1", "2"),
    ("GEOADD", "g", "1", "2", "a", "b"),
    ("GEOADD", "g", "NX", "CH", "NX"),
    ("GEOADD", "g", "x", "2", "a"),
    ("GEOADD", "g", "1", "x", "a"),
    ("GEOADD", "g", "181", "2", "a"),
    ("GEOADD", "g", "1", "85.06", "a"),
    ("GEOADD", "g", "-181", "2", "a"),
    ("GEOADD", "g", "1", "-85.06", "a"),
    ("GEOADD", "g", "1", "2", "ok", "181", "2", "a"),
    ("GEOPOS", "g", "ok"),
    ("GEOADD", "g", "180", "85.05112878", "corner", "-180", "-85.05112878", "other"),
    ("GEOPOS", "g", "corner", "other"),
    ("GEOHASH", "g", "corner", "other"),
    ("ZSCORE", "g", "corner"),
    ("ZSCORE", "g", "other"),
    ("GEOADD", "g", "0", "0", "zero"),
    ("GEOPOS", "g", "zero"),
    ("GEOHASH", "g", "zero"),
    ("GEOADD", "g", " 1", "2", "a"),
    ("GEOADD", "g", "1 ", "2", "a"),
    ("GEOADD", "g", "0x10", "2", "hex"),
    ("GEOADD", "g", "1e1", "2", "exp"),
    ("GEOPOS", "g", "hex", "exp"),
    # Wrong kinds.
    ("SET", "str", "hello"),
    ("GEOADD", "str", "1", "2", "a"),
    ("GEOPOS", "str", "a"),
    ("GEOHASH", "str", "a"),
    ("GEODIST", "str", "a", "b"),
    ("GEODIST", "str", "a", "b", "parsec"),
    ("GEORADIUS", "str", "15", "37", "200", "km"),
    ("GEORADIUS", "str", "x", "37", "200", "km"),
    ("GEORADIUSBYMEMBER", "str", "a", "200", "km"),
    ("GEOSEARCH", "str", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"),
    ("GEOSEARCH", "str", "NOPE"),
    ("GEOSEARCHSTORE", "out", "str", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"),
    # A sorted set whose scores aren't positions.
    ("ZADD", "notgeo", "-1", "neg", "0", "zero", "1", "one", "1e300", "huge", "1.5", "half", "inf", "top", "-inf", "bottom"),
    ("GEOPOS", "notgeo", "neg", "zero", "one", "huge", "half", "top", "bottom"),
    ("GEOHASH", "notgeo", "neg", "zero", "one", "huge", "half", "top", "bottom"),
    ("GEODIST", "notgeo", "neg", "zero"),
    ("GEODIST", "notgeo", "one", "huge"),
    # Arity.
    ("GEOADD", "g", "1", "2"),
    ("GEOHASH",),
    ("GEOPOS",),
    ("GEODIST", "g", "a"),
    ("GEORADIUS", "g", "1", "2", "3"),
    ("GEORADIUSBYMEMBER", "g", "a", "3"),
    ("GEOSEARCH", "g", "FROMLONLAT", "1", "2", "BYRADIUS"),
    ("GEOSEARCHSTORE", "o", "g", "FROMLONLAT", "1", "2", "BYRADIUS"),
    # In transactions and scripts.
    ("MULTI",),
    ("GEOADD", "tx", "1", "2", "a"),
    ("GEOPOS", "tx", "a"),
    ("GEOSEARCHSTORE", "tx2", "tx", "FROMLONLAT", "1", "2", "BYRADIUS", "1", "km"),
    ("EXEC",),
    ("EVAL", "return redis.call('GEOADD', KEYS[1], 1, 2, 'a')", "1", "lua2"),
    ("EVAL", "return redis.call('GEOPOS', KEYS[1], 'a')", "1", "lua2"),
    ("EVAL", "return redis.call('GEODIST', KEYS[1], 'a', 'a')", "1", "lua2"),
    ("EVAL", "return redis.call('GEORADIUS', KEYS[1], 1, 2, 1, 'km', 'WITHDIST', 'WITHCOORD', 'WITHHASH')", "1", "lua2"),
    ("EVAL_RO", "return redis.call('GEORADIUS', KEYS[1], 1, 2, 1, 'km', 'STORE', 'x')", "1", "lua2"),
    ("EVAL_RO", "return redis.call('GEORADIUS_RO', KEYS[1], 1, 2, 1, 'km')", "1", "lua2"),
    # What COMMAND says of their keys.
    ("COMMAND", "GETKEYS", "GEOADD", "k", "1", "2", "a"),
    ("COMMAND", "GETKEYS", "GEORADIUS", "k", "1", "2", "3", "km"),
    ("COMMAND", "GETKEYS", "GEORADIUS", "k", "1", "2", "3", "km", "STORE", "o"),
    ("COMMAND", "GETKEYS", "GEORADIUS", "k", "1", "2", "3", "km", "STOREDIST", "o"),
    ("COMMAND", "GETKEYS", "GEORADIUSBYMEMBER", "k", "m", "3", "km", "STORE", "o"),
    ("COMMAND", "GETKEYS", "GEOSEARCHSTORE", "o", "k", "FROMLONLAT", "1", "2", "BYRADIUS", "1", "km"),
]

# Replies with doubles, which is how scores print.
SCORES = [
    ("0"), ("-0"), ("0.0"), ("-0.0"), ("1"), ("-1"), ("0.1"), ("-0.1"), ("1.5"), ("100"), ("1e3"),
    ("12345.678"), ("15194.382059851243"), ("-15194.382059851243"), ("99999.123456789"),
    ("100000.5"), ("1234567.1234567"), ("0.0009088329744167203"), ("0.00001"), ("0.000001"),
    ("0.0000001"), ("1.5e-7"), ("-1.5e-7"), ("1e-10"), ("1e-300"), ("5e-324"),
    ("9007199254740991"), ("9007199254740992"), ("9007199254740993"), ("1e15"), ("1e16"),
    ("1e17"), ("1e18"), ("4611686018427387904"), ("4611686018427387905"), ("4611686018427388928"),
    ("-4611686018427387904"), ("-4611686018427388928"), ("1152921504606846976"),
    ("123456789012345678"), ("1e19"), ("1e20"), ("1e21"), ("1e22"), ("1.5e22"), ("1e100"),
    ("1e300"), ("1.7976931348623157e308"), ("-1.7976931348623157e308"), ("3.141592653589793"),
    ("2.718281828459045"), ("0.30000000000000004"), ("1.0000000000000002"), ("inf"), ("-inf"),
    ("1234.5"), ("12345.5"), ("123456.5"), ("1234567.5"), ("12345678.5"), ("123456789.5"),
    ("1234567890.5"), ("12345678901.5"), ("123456789012.5"), ("1234567890123.5"),
    ("1000.0000001"), ("10000.0000001"), ("1000.00000001"), ("100000.000001"),
]


def scores(c):
    for i, score in enumerate(SCORES):
        c.call("ZADD", "scores", score, f"m{i}")
        c.call("ZSCORE", "scores", f"m{i}")
    c.call("ZRANGE", "scores", 0, -1, "WITHSCORES")
    c.call("ZINCRBY", "scores", "0.1", "m0")
    c.call("ZINCRBY", "scores", "12345.678", "m6")
    c.call("ZADD", "scores", "INCR", "1e-7", "m0")
    c.call("ZMSCORE", "scores", "m0", "m6", "m7")
    c.call("EVAL", "return redis.call('ZSCORE', KEYS[1], 'm12')", "1", "scores")
    c.call("EVAL", "return tostring(redis.call('ZINCRBY', KEYS[1], 0.5, 'm12'))", "1", "scores")
    # Too big for a listpack, which turns -0 into 0, as a skiplist doesn't.
    c.call("ZADD", "big", *[x for i in range(200) for x in (i, f"filler{i}")])
    c.call("ZADD", "big", "-0", "negative zero", "0", "zero")
    c.call("ZSCORE", "big", "negative zero")
    c.call("ZSCORE", "big", "zero")
    c.call("ZRANGE", "big", 0, 4, "WITHSCORES")
    c.call("ZADD", "small", "-0", "negative zero")
    c.call("ZADD", "small", "INCR", "-0", "made by adding")
    c.call("ZRANGE", "small", 0, -1, "WITHSCORES")
    c.call("ZINCRBY", "small", "-0", "negative zero")
    c.call("ZINCRBY", "big", "-0", "negative zero")


def by_hand(c):
    for version in (2, 3):
        c.resp(version)
        c.call("FLUSHALL")
        for args in HLL_BY_HAND + GEO_BY_HAND:
            c.call(*args)
        scores(c)
    c.resp(2)


def hll_work(c, rng, steps):
    keys = [f"h{i}" for i in range(6)]
    corrupt = False
    for _ in range(steps):
        key = rng.choice(keys)
        roll = rng.random()
        if roll < 0.55:
            n = rng.choice([1, 1, 2, 5, 40, 400, 3000])
            base = rng.randrange(1 << 40)
            c.call("PFADD", key, *[f"e{base + i}" for i in range(n)])
        elif roll < 0.72:
            c.call("PFCOUNT", key)
        elif roll < 0.8:
            c.call("PFCOUNT", *rng.sample(keys, rng.randint(1, 4)))
        elif roll < 0.86:
            c.call("PFMERGE", key, *rng.sample(keys, rng.randint(0, 3)))
        elif roll < 0.9:
            c.call("PFDEBUG", "ENCODING", key)
        elif roll < 0.93:
            if b"sparse" in c.call("PFDEBUG", "ENCODING", key):
                c.call("PFDEBUG", "DECODE", key)
        elif roll < 0.95:
            c.call("PFDEBUG", "TODENSE", key)
        elif roll < 0.97:
            c.call("PFDEBUG", "GETREG", key)
        elif roll < 0.99:
            c.call("DEL", key)
        else:
            c.call("CONFIG", "SET", "hll-sparse-max-bytes", rng.choice([30, 100, 1000, 3000, 20000]))
        c.call("GET", key)
    c.call("CONFIG", "SET", "hll-sparse-max-bytes", 3000)
    return corrupt


def hll_corruption(c, rng, rounds):
    """Random damage: both must survive it, and say the same. But when a damaged
    string ends in half an opcode, Redis reads the rest from past the string's end,
    where what's left depends on its past, so what a later `PFADD` leaves in the
    string's last byte may differ. Differences are counted apart, and each round
    starts anew."""
    allowed = 0
    for _ in range(rounds):
        c.call("DEL", "bad", "good")
        n = rng.choice([1, 3, 10, 100, 1000])
        base = rng.randrange(1 << 40)
        c.call("PFADD", "bad", *[f"e{base + i}" for i in range(n)])
        c.call("PFADD", "good", "x", "y")
        for _ in range(rng.randint(1, 3)):
            length = int(c.call("STRLEN", "bad")[1:-2])
            at = rng.randrange(16, max(length, 17))
            c.call("SETRANGE", "bad", at, bytes([rng.randrange(256)]))
        if rng.random() < 0.2:
            c.call("APPEND", "bad", b"\x7f")
        for args in (
            ("PFCOUNT", "bad"),
            ("PFCOUNT", "bad", "good"),
            ("PFMERGE", "m", "good", "bad"),
            ("PFDEBUG", "ENCODING", "bad"),
            ("PFDEBUG", "DECODE", "bad"),
        ):
            c.call(*args)
        c.call("PFADD", "bad", f"e{base + n}", f"e{base + n + 1}")
        got = c.redis.call("GET", "bad"), c.tantalum.call("GET", "bad")
        if got[0] != got[1]:
            allowed += 1
            if got[0][:-3] != got[1][:-3]:
                print(f"DIFF more than the last byte\n   redis    {got[0]!r}\n   tantalum {got[1]!r}")
                c.differences += 1
        c.compared += 1
    print(f"corrupt: {allowed} rounds whose last byte differs, as above")


def random_position(rng, centers):
    roll = rng.random()
    if roll < 0.15:
        return rng.uniform(-180, 180), rng.uniform(-85.05, 85.05)
    x, y = rng.choice(centers)
    spread = rng.choice([0.0001, 0.01, 0.2, 2, 15])
    return (
        max(-180, min(180, x + rng.uniform(-spread, spread))),
        max(-85.05, min(85.05, y + rng.uniform(-spread, spread))),
    )


def coordinate(rng, value):
    return f"{value:.{rng.choice([1, 3, 6, 9])}f}"


def geo_search_options(rng):
    options = []
    for word in ("WITHDIST", "WITHHASH", "WITHCOORD"):
        if rng.random() < 0.3:
            options.append(word)
    sort = rng.random()
    if sort < 0.3:
        options.append("ASC")
    elif sort < 0.6:
        options.append("DESC")
    if rng.random() < 0.4:
        options += ["COUNT", rng.choice([1, 2, 3, 5, 8, 20, 50, 300])]
        if rng.random() < 0.3:
            options.append("ANY")
    rng.shuffle(options)
    return options


def geo_work(c, rng, steps):
    centers = [
        (rng.uniform(-179, 179), rng.uniform(-84, 84)) for _ in range(3)
    ] + [(13.4, 38.1), (179.9, 0.0), (-179.9, 0.0), (0.0, 0.0), (0.0, 84.9), (0.0, -84.9)]
    keys = ["g1", "g2", "g3"]
    members = [f"m{i}" for i in range(400)]
    units = ["m", "km", "ft", "mi", "KM", "Mi"]
    radii = [0, 0.5, 1, 10, 100, 1000, 5000, 20000, 100000, 1e7, 3e7, 1e9]
    same_place = (12.5, 41.9)
    for _ in range(steps):
        key = rng.choice(keys)
        x, y = random_position(rng, centers)
        roll = rng.random()
        if roll < 0.28:
            # Sometimes many at the same place: ties decide what COUNT keeps.
            args = []
            for _ in range(rng.choice([1, 1, 3, 10, 80])):
                px, py = same_place if rng.random() < 0.3 else random_position(rng, centers)
                args += [coordinate(rng, px), coordinate(rng, py), rng.choice(members)]
            options = rng.choice([[], [], [], ["CH"], ["NX"], ["XX"], ["XX", "CH"]])
            c.call("GEOADD", key, *options, *args)
        elif roll < 0.33:
            c.call("GEOPOS", key, *rng.sample(members, rng.randint(1, 5)))
        elif roll < 0.38:
            c.call("GEOHASH", key, *rng.sample(members, rng.randint(1, 5)))
        elif roll < 0.43:
            c.call("GEODIST", key, rng.choice(members), rng.choice(members), rng.choice(units))
        elif roll < 0.53:
            name = rng.choice(["GEORADIUS", "GEORADIUS_RO"])
            c.call(name, key, coordinate(rng, x), coordinate(rng, y), rng.choice(radii), rng.choice(units), *geo_search_options(rng))
        elif roll < 0.63:
            name = rng.choice(["GEORADIUSBYMEMBER", "GEORADIUSBYMEMBER_RO"])
            c.call(name, key, rng.choice(members), rng.choice(radii), rng.choice(units), *geo_search_options(rng))
        elif roll < 0.8:
            start = (
                ["FROMMEMBER", rng.choice(members)]
                if rng.random() < 0.4
                else ["FROMLONLAT", coordinate(rng, x), coordinate(rng, y)]
            )
            shape = (
                ["BYRADIUS", rng.choice(radii), rng.choice(units)]
                if rng.random() < 0.5
                else ["BYBOX", rng.choice(radii), rng.choice(radii), rng.choice(units)]
            )
            c.call("GEOSEARCH", key, *start, *shape, *geo_search_options(rng))
        elif roll < 0.92:
            dest = rng.choice(["out1", "out2", key])
            start = ["FROMLONLAT", coordinate(rng, x), coordinate(rng, y)]
            shape = (
                ["BYRADIUS", rng.choice(radii), rng.choice(units)]
                if rng.random() < 0.5
                else ["BYBOX", rng.choice(radii), rng.choice(radii), rng.choice(units)]
            )
            options = [o for o in geo_search_options(rng) if not str(o).startswith("WITH")]
            if rng.random() < 0.4:
                options.append("STOREDIST")
            # (COUNT's number was kept, even with its word's neighbours gone.)
            c.call("GEOSEARCHSTORE", dest, key, *start, *shape, *options)
            c.call("ZRANGE", dest, 0, -1, "WITHSCORES")
        elif roll < 0.97:
            dest = rng.choice(["out1", "out2"])
            word = rng.choice(["STORE", "STOREDIST"])
            options = [o for o in geo_search_options(rng) if not str(o).startswith("WITH")]
            c.call("GEORADIUS", key, coordinate(rng, x), coordinate(rng, y), rng.choice(radii), rng.choice(units), word, dest, *options)
            c.call("ZRANGE", dest, 0, -1, "WITHSCORES")
        elif roll < 0.99:
            c.call("ZREM", key, *rng.sample(members, rng.randint(1, 40)))
        else:
            c.call("DEL", key)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis", type=int, default=7390)
    parser.add_argument("--tantalum", type=int, default=7391)
    parser.add_argument("--seeds", default="1,2,3")
    parser.add_argument("--steps", type=int, default=4000)
    opts = parser.parse_args()
    c = Both([opts.redis, opts.tantalum])
    by_hand(c)
    print(f"by hand: {c.compared} compared, {c.differences} differences", flush=True)
    for seed in opts.seeds.split(","):
        rng = random.Random(int(seed))
        for version in (2, 3):
            c.resp(version)
            c.call("FLUSHALL")
            geo_work(c, rng, opts.steps)
            print(f"seed {seed} geo RESP{version}: {c.compared} compared, {c.differences} differences", flush=True)
            c.call("FLUSHALL")
            hll_work(c, rng, opts.steps // 4)
            print(f"seed {seed} hll RESP{version}: {c.compared} compared, {c.differences} differences", flush=True)
        c.resp(2)
        c.call("FLUSHALL")
        hll_corruption(c, rng, 300)
        print(f"seed {seed} corrupt: {c.compared} compared, {c.differences} differences", flush=True)
    sys.exit(1 if c.differences else 0)


if __name__ == "__main__":
    main()
