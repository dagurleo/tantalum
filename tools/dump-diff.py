#!/usr/bin/env python3
"""Runs the same DUMP, RESTORE and MIGRATE scenarios on Redis and on Tantalum
and shows where their replies differ, byte for byte: tools/acl-diff.py's
runner, with more.

    tools/dump-diff.py [--redis 6390] [--tantalum 6391] [--old 6392] [--rust]

Start Redis 7.2 and Tantalum on spare ports with no password, and for the
lines below that need one, a Redis 6.2 (--old), whose payloads hold the
encodings Redis 7.2 no longer writes (ziplists, quicklists of them, the first
stream format):

    redis-server --port 6390 --save '' --appendonly no &
    target/release/tantalum --port 6391 &
    docker run -d --network host redis:6.2 redis-server --port 6392 --save '' &

With --rust it prints the scenarios that need only one server and Redis'
replies as test steps for tests/dump.rs (the old Redis isn't needed then).
Besides acl-diff.py's lines and masks:

    {other}     in a command, the other server's port: MIGRATE from Redis lands in
                Tantalum and from Tantalum in Redis, so that each reads what the
                other wrote. Send it to a database the other doesn't use (1), as
                both run it.
    ?ttl        integers above 0 read as <positive> (PTTL after a restore)
    ?pairs      a map or flat array of pairs in any order (HGETALL of a big hash)
    ?cached     INFO's migrate_cached_sockets line alone
    !old CMD    run on the old Redis, not compared
    !import K.. DUMP each key on the old Redis, then RESTORE it under the same name
                with REPLACE on both, and compare those replies
"""

import argparse
import importlib.util
import os
import re
import sys
import time

spec = importlib.util.spec_from_file_location(
    "acl_diff", os.path.join(os.path.dirname(os.path.abspath(__file__)), "acl-diff.py")
)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)

# Lua that makes collections too big for the small encodings.
FILL = r'''"for i=1,tonumber(ARGV[2]) do redis.call(ARGV[1], KEYS[1], unpack(ARGV[3] == 'z' and {i * 1.5, 'member:' .. i} or ARGV[3] == 'h' and {'field:' .. i, 'value:' .. i} or ARGV[3] == 'i' and {i} or {'element:' .. i})) end return redis.call('type', KEYS[1])"'''

# What runs on one server, which --rust writes as test steps (tests/dump.rs).
SINGLE = r"""
# DUMP: every type, in each encoding Redis 7.2 holds it in, byte for byte.
DUMP nosuch
@1 ?ints HELLO 3
@1 DUMP nosuch
SET s hello
DUMP s
SET n 12345
DUMP n
SET n2 -1
DUMP n2
SET n3 -2147483648
DUMP n3
SET n4 12345678901
DUMP n4
SET n5 0123
DUMP n5
SET e ""
DUMP e
SET long "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
DUMP long
SET short_lzf "abcabcabcabcabcabcabcabc"
DUMP short_lzf
SET f 3.14
DUMP f
SETRANGE sr 5 x
DUMP sr
INCR counter
DUMP counter
APPEND app abc
DUMP app
RPUSH l a b c 1 2 -3 1000 100000 -100000 10000000000 -10000000000 9223372036854775807 -9223372036854775808 18446744073709551616 007 " 1" 1.5 ""
DUMP l
EVAL """ + FILL + r""" 1 lbig rpush 2000 l
DUMP lbig
EVAL "redis.call('rpush', KEYS[1], 'a', string.rep('x', 9000), 'b') return 1" 1 lplain
DUMP lplain
LPUSH lp x y z
DUMP lp
SADD si 1 2 3 -5 100000
DUMP si
SADD si64 1 9999999999 -9999999999
DUMP si64
SADD ss a b c 1
DUMP ss
EVAL """ + FILL + r""" 1 sbig sadd 200 m
?len DUMP sbig
EVAL """ + FILL + r""" 1 sibig sadd 600 i
?len DUMP sibig
ZADD z 1 a 2.5 b -3 c 1e100 d inf e -inf f 0 g 1.0000000000000002 h -0 i 3.0e-5 j 123456789012 k
DUMP z
EVAL """ + FILL + r""" 1 zbig zadd 200 z
DUMP zbig
HSET h f v n 1 big 99999999999 neg -7
DUMP h
EVAL """ + FILL + r""" 1 hbig hset 200 h
?len DUMP hbig
XADD x 1-1 a 1 b 2
XADD x 1-2 a 3 b 4
XADD x 2-0 c 5
XADD x 2-1 a 9 b 9
XADD x 3-0 a 1
DUMP x
EVAL "for i=1,250 do redis.call('xadd', KEYS[1], i .. '-' .. (i % 3), 'field', i, 'other', 'value') end return 1" 1 xbig
?len DUMP xbig
XADD xempty 1-1 a 1
XDEL xempty 1-1
DUMP xempty
XGROUP CREATE x g 0
XGROUP CREATE x g2 $
XREADGROUP GROUP g c1 COUNT 2 STREAMS x >
XREADGROUP GROUP g c2 COUNT 1 STREAMS x >
?len DUMP x
EVAL "return redis.call('dump', KEYS[1])" 1 s
MULTI
DUMP s
DUMP nosuch
EXEC
DUMP
DUMP a b

# RESTORE: what it says, in Redis' order.
RESTORE
RESTORE k
RESTORE k 0
RESTORE k 0 bad
RESTORE s 0 bad
RESTORE k -1 bad
RESTORE k x bad
RESTORE k 0 bad REPLACE
RESTORE k 0 bad IDLETIME
RESTORE k 0 bad IDLETIME -1
RESTORE k 0 bad IDLETIME x
RESTORE k 0 bad FREQ 256
RESTORE k 0 bad FREQ -1
RESTORE k 0 bad FREQ x
RESTORE k 0 bad IDLETIME 1 FREQ 1
RESTORE k 0 bad FREQ 1 IDLETIME 1
RESTORE k 0 bad ABSTTL
RESTORE k 0 bad BOGUS
RESTORE s x bad
RESTORE s -1 bad REPLACE
RESTORE k 0 {rdb:000568656c6c6f:11}
GET k
RESTORE k 0 {rdb:000568656c6c6f:11}
RESTORE k 0 "\x00\x05world\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00" REPLACE
RESTORE k 0 {rdb:000568656c6c6f:12} REPLACE
RESTORE k 0 "\x00\x05hello\x0b" REPLACE
RESTORE k 0 "" REPLACE
RESTORE k 0 {rdb:000577686174:9} REPLACE
GET k
RESTORE k 0 {rdb:000577686174:0} REPLACE
RESTORE k 0 {rdb:630568656c6c6f:11} REPLACE
RESTORE k 0 {rdb:000968656c6c6f:11} REPLACE
GET k
RESTORE k 0 {rdb:000f68656c6c6f:11} REPLACE
STRLEN k
RESTORE k 0 {rdb:001068656c6c6f:11} REPLACE
RESTORE k 0 {rdb:00:11} REPLACE
RESTORE k 0 {rdb::11} REPLACE
RESTORE k 0 {rdb:00c0:11} REPLACE
RESTORE k 0 {rdb:00c07b:11} REPLACE
GET k
RESTORE k 0 {rdb:00c1ff7f:11} REPLACE
GET k
RESTORE k 0 {rdb:00c2ffffff7f:11} REPLACE
GET k
RESTORE k 0 {rdb:00c3050a616263646566:11} REPLACE
RESTORE k 0 {rdb:00c4:11} REPLACE
RESTORE k 0 {rdb:0e00:11} REPLACE
RESTORE k 0 {rdb:1200:11} REPLACE
RESTORE k 0 {rdb:0200:11} REPLACE
RESTORE k 0 {rdb:0b0e0200000003000000010002000300:11} REPLACE
SMEMBERS k
OBJECT ENCODING k
RESTORE k 0 {rdb:0b0e0200000003000000010002000300ff:11} REPLACE
RESTORE k 0 {rdb:0b0e0300000003000000010002000300:11} REPLACE
RESTORE k 0 {rdb:0b0a020000000000000001000200:11} REPLACE
RESTORE k 0 {rdb:0907010166010076ff:11} REPLACE
HGETALL k
OBJECT ENCODING k
RESTORE k 0 {rdb:090c02016601007601660100 77ff:11} REPLACE
RESTORE k 0 {rdb:090e 02 0166 01 00 76 0167 01 02 77 abab ff:11} REPLACE
HGETALL k
RESTORE k 0 {rdb:0907020166010076ff:11} REPLACE
RESTORE k 0 {rdb:09020000:11} REPLACE
RESTORE k 0 {rdb:020201610161:11} REPLACE
RESTORE k 0 {rdb:020201610162:11} REPLACE
?sorted SMEMBERS k
RESTORE k 0 {rdb:0401016601 76:11} REPLACE
HGETALL k
RESTORE k 0 {rdb:04020166017601660177:11} REPLACE
RESTORE k 0 {rdb:030101610331 2e35:11} REPLACE
ZRANGE k 0 -1 WITHSCORES
RESTORE k 0 {rdb:03010161fd:11} REPLACE
RESTORE k 0 {rdb:03020161fe0162ff:11} REPLACE
ZRANGE k 0 -1 WITHSCORES
RESTORE k 0 {rdb:0501 0161 000000000000f87f:11} REPLACE
RESTORE k 0 {rdb:0502 0161 000000000000f83f 0161 000000000000f03f:11} REPLACE
RESTORE k 0 {rdb:0501 0161 000000000000f83f:11} REPLACE
ZRANGE k 0 -1 WITHSCORES
RESTORE k 0 {rdb:01020161 0162:11} REPLACE
LRANGE k 0 -1
OBJECT ENCODING k
RESTORE k 0 {rdb:0a11110000000d0000000200000161030162ff:11} REPLACE
LRANGE k 0 -1
RESTORE k 0 {rdb:0a11110000000d0000000300000161030162ff:11} REPLACE
RESTORE k 0 {rdb:0e01 11110000000d0000000200000161030162ff:11} REPLACE
LRANGE k 0 -1
RESTORE k 0 {rdb:1202 01 056161616161 02 0d 0d000000 0200 816102 816202 ff:11} REPLACE
LRANGE k 0 -1
RESTORE k 0 {rdb:12010309 0d0d00000002008161028162 02ff:11} REPLACE
RESTORE k 0 {rdb:14 0d 0d000000 0200 816102 816202 ff:11} REPLACE
?sorted SMEMBERS k
RESTORE k 0 {rdb:10 0d 0d000000 0200 816102 816202 ff:11} REPLACE
HGETALL k
RESTORE k 0 {rdb:11 0c 0c000000 0200 816102 0201 ff:11} REPLACE
ZRANGE k 0 -1 WITHSCORES
RESTORE k 0 "\x0b\x0e\x02\x00\x00\x00\x03\x00\x00\x00\x01\x00\x02\x00\x03\x00\x0b\x00\xcc\xdd\xe1\x91\x90\xf1\xa4\x92" REPLACE ABSTTL IDLETIME 5
RESTORE k 0 "\x0b\x0e\x02\x00\x00\x00\x03\x00\x00\x00\x01\x00\x02\x00\x03\x00\x0b\x00\xcc\xdd\xe1\x91\x90\xf1\xa4\x92" REPLACE FREQ 100 replace absttl
TYPE k
RESTORE kt 100000 "\x00\x05hello\x0b\x00\n\xadb\x05\x98\xab\xc9\x83"
?ttl PTTL kt
RESTORE kt2 99999999999999 "\x00\x05hello\x0b\x00\n\xadb\x05\x98\xab\xc9\x83" ABSTTL
?ttl PTTL kt2
RESTORE kt3 1 "\x00\x05hello\x0b\x00\n\xadb\x05\x98\xab\xc9\x83" ABSTTL
EXISTS kt3
SET gone soon
RESTORE gone 1 "\x00\x05hello\x0b\x00\n\xadb\x05\x98\xab\xc9\x83" ABSTTL REPLACE
EXISTS gone
RESTORE kt4 0 "\x00\x05hello\x0b\x00\n\xadb\x05\x98\xab\xc9\x83"
TTL kt4
RESTORE khuge 9223372036854775807 {rdb:000568656c6c6f:11}
EXISTS khuge
RESTORE khuge2 9223372036854775807 {rdb:000568656c6c6f:11} ABSTTL
?ttl PTTL khuge2
EVAL "return redis.call('restore', KEYS[1], 0, ARGV[1])" 1 klua "\x00\x05hello\x0b\x00\n\xadb\x05\x98\xab\xc9\x83"
GET klua
MULTI
RESTORE kmulti 0 "\x00\x05hello\x0b\x00\n\xadb\x05\x98\xab\xc9\x83"
RESTORE kmulti 0 bad
EXEC
GET kmulti

# MIGRATE: what it says.
MIGRATE
MIGRATE 127.0.0.1 1 s 0
MIGRATE 127.0.0.1 1 nosuch 0 1000
MIGRATE 127.0.0.1 1 s 0 1000
MIGRATE 127.0.0.1 1 s 0 1000 BOGUS
MIGRATE 127.0.0.1 1 s 0 1000 AUTH
MIGRATE 127.0.0.1 1 s 0 1000 AUTH2 u
MIGRATE 127.0.0.1 1 s 0 1000 KEYS a
MIGRATE 127.0.0.1 1 s x 1000
MIGRATE 127.0.0.1 1 s 0 x
MIGRATE 127.0.0.1 1 s x x BOGUS
MIGRATE 127.0.0.1 1 "" 0 1000 KEYS nosuch other
MIGRATE 127.0.0.1 1 "" 0 1000
MIGRATE 127.0.0.1 1 "" 0 1000 KEYS
COMMAND GETKEYS MIGRATE 127.0.0.1 1 k 0 100
COMMAND GETKEYS MIGRATE 127.0.0.1 1 "" 0 100 KEYS a b
COMMAND GETKEYS MIGRATE 127.0.0.1 1 "" 0 100 AUTH KEYS KEYS a
COMMAND GETKEYS MIGRATE 127.0.0.1 1 "" 0 100 AUTH2 KEYS KEYS KEYS a
COMMAND GETKEYS MIGRATE 127.0.0.1 1 k 0 100 KEYS a
COMMAND GETKEYS MIGRATE 127.0.0.1 1 "" 0 100 COPY REPLACE
COMMAND GETKEYS DUMP k
COMMAND GETKEYS RESTORE k 0 x
COMMAND INFO dump restore migrate

# Users.
ACL SETUSER du on nopass +@all ~a* -dump
@3 AUTH du x
@3 MIGRATE 127.0.0.1 1 "" 0 100 KEYS a1 b1
@3 MIGRATE 127.0.0.1 1 "" 0 100 KEYS a1 a2
@3 MIGRATE 127.0.0.1 1 b1 0 100
@3 DUMP a1
@3 RESTORE b 0 x
@3 RESTORE a 0 x
ACL DRYRUN du MIGRATE 127.0.0.1 1 "" 0 100 KEYS a1 b1
ACL DRYRUN du RESTORE a 0 x
ACL DRYRUN du DUMP a
ACL SETUSER ro on nopass +@all %R~*
@4 AUTH ro x
@4 DUMP s
@4 RESTORE r 0 x
@4 MIGRATE 127.0.0.1 1 s 0 100
?log ACL LOG
"""

# What needs both: MIGRATE to the other, payloads from Redis 6.2.
CROSS = r"""
# Again, for both: the values, then each sent to the other.
!reset
DUMP nosuch
@1 ?ints HELLO 3
@1 DUMP nosuch
SET s hello
DUMP s
SET n 12345
DUMP n
SET n2 -1
DUMP n2
SET n3 -2147483648
DUMP n3
SET n4 12345678901
DUMP n4
SET n5 0123
DUMP n5
SET e ""
DUMP e
SET long "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
DUMP long
SET short_lzf "abcabcabcabcabcabcabcabc"
DUMP short_lzf
SET f 3.14
DUMP f
SETRANGE sr 5 x
DUMP sr
INCR counter
DUMP counter
APPEND app abc
DUMP app
RPUSH l a b c 1 2 -3 1000 100000 -100000 10000000000 -10000000000 9223372036854775807 -9223372036854775808 18446744073709551616 007 " 1" 1.5 ""
DUMP l
EVAL """ + FILL + r""" 1 lbig rpush 2000 l
DUMP lbig
EVAL "redis.call('rpush', KEYS[1], 'a', string.rep('x', 9000), 'b') return 1" 1 lplain
DUMP lplain
LPUSH lp x y z
DUMP lp
SADD si 1 2 3 -5 100000
DUMP si
SADD si64 1 9999999999 -9999999999
DUMP si64
SADD ss a b c 1
DUMP ss
EVAL """ + FILL + r""" 1 sbig sadd 200 m
?len DUMP sbig
EVAL """ + FILL + r""" 1 sibig sadd 600 i
?len DUMP sibig
ZADD z 1 a 2.5 b -3 c 1e100 d inf e -inf f 0 g 1.0000000000000002 h -0 i 3.0e-5 j 123456789012 k
DUMP z
EVAL """ + FILL + r""" 1 zbig zadd 200 z
DUMP zbig
HSET h f v n 1 big 99999999999 neg -7
DUMP h
EVAL """ + FILL + r""" 1 hbig hset 200 h
?len DUMP hbig
XADD x 1-1 a 1 b 2
XADD x 1-2 a 3 b 4
XADD x 2-0 c 5
XADD x 2-1 a 9 b 9
XADD x 3-0 a 1
DUMP x
EVAL "for i=1,250 do redis.call('xadd', KEYS[1], i .. '-' .. (i % 3), 'field', i, 'other', 'value') end return 1" 1 xbig
?len DUMP xbig
XADD xempty 1-1 a 1
XDEL xempty 1-1
DUMP xempty
XGROUP CREATE x g 0
XGROUP CREATE x g2 $
XREADGROUP GROUP g c1 COUNT 2 STREAMS x >
XREADGROUP GROUP g c2 COUNT 1 STREAMS x >
?len DUMP x
EVAL "return redis.call('dump', KEYS[1])" 1 s
MULTI
DUMP s
DUMP nosuch
EXEC
DUMP
DUMP a b

# Each value DUMPed here and RESTOREd there, both ways: MIGRATE each to the other's
# database 1, then compare what each got from the other.
MIGRATE 127.0.0.1 {other} "" 1 5000 COPY KEYS s n n2 n3 n4 n5 e long short_lzf f sr counter app l lbig lplain lp si si64 ss sbig sibig z zbig h hbig x xbig xempty
@2 SELECT 1
@2 DBSIZE
@2 GET s
@2 GET n
@2 GET n3
@2 GET n4
@2 GET n5
@2 GET e
@2 GET long
@2 GET f
@2 GET sr
@2 GET app
@2 LRANGE l 0 -1
@2 LRANGE lbig 0 -1
@2 LRANGE lplain 0 -1
@2 LRANGE lp 0 -1
@2 ?sorted SMEMBERS si
@2 ?sorted SMEMBERS si64
@2 ?sorted SMEMBERS ss
@2 ?sorted SMEMBERS sbig
@2 ?sorted SMEMBERS sibig
@2 ZRANGE z 0 -1 WITHSCORES
@2 ZRANGE zbig 0 -1 WITHSCORES
@2 HGETALL h
@2 ?pairs HGETALL hbig
@2 XRANGE x - +
@2 XRANGE xbig - +
@2 XINFO GROUPS x
@2 ?ints XINFO CONSUMERS x g
@2 ?ints XPENDING x g - + 10
@2 ?ints XINFO STREAM x FULL
@2 XINFO STREAM xbig
@2 XINFO STREAM xempty
@2 DUMP s
@2 DUMP n
@2 DUMP long
@2 DUMP l
@2 DUMP lbig
@2 DUMP lplain
@2 DUMP si
@2 DUMP si64
@2 DUMP ss
@2 DUMP z
@2 DUMP zbig
@2 DUMP h
@2 DUMP xempty
@2 ?len DUMP x
@2 ?len DUMP xbig
@2 OBJECT ENCODING l
@2 OBJECT ENCODING lbig
@2 OBJECT ENCODING si
@2 OBJECT ENCODING ss
@2 OBJECT ENCODING sbig
@2 OBJECT ENCODING z
@2 OBJECT ENCODING zbig
@2 OBJECT ENCODING h
@2 OBJECT ENCODING hbig
@2 ?len XADD x * after restore
@2 ?len XREADGROUP GROUP g c1 COUNT 10 STREAMS x >
@2 XADD xempty 1-1 a 1
@2 XADD xempty 2-0 a 1

# Streams with deleted entries, trimmed, with deleted and moved group entries.
XADD xd 1-1 a 1
XADD xd 1-2 a 2
XADD xd 1-3 b 3
XADD xd 1-4 a 4 c 5
XDEL xd 1-1 1-3
XGROUP CREATE xd g 0 ENTRIESREAD 1
XREADGROUP GROUP g c1 STREAMS xd >
XACK xd g 1-2
XCLAIM xd g c2 0 1-4
XGROUP CREATECONSUMER xd g c3
EVAL "for i=1,300 do redis.call('xadd', KEYS[1], i .. '-1', 'f', i) end redis.call('xtrim', KEYS[1], 'MAXLEN', '~', 150) redis.call('xdel', KEYS[1], '200-1', '201-1') return redis.call('xlen', KEYS[1])" 1 xt
MIGRATE 127.0.0.1 {other} "" 1 5000 COPY KEYS xd xt
@2 XRANGE xd - +
@2 XINFO GROUPS xd
@2 ?ints XINFO CONSUMERS xd g
@2 ?ints XPENDING xd g - + 10
@2 XINFO STREAM xt
@2 XRANGE xt - + COUNT 3
@2 XLEN xt
@2 ?len XADD xd * new 1
@2 XADD xt 100-1 too small

# MIGRATE to the other. (And to ports that need the C library's view: 0 fails at once on
# macOS and later on Linux, so Redis' reply depends on where it runs.)
MIGRATE 127.0.0.1 nonsense s 0 1000
MIGRATE 127.0.0.1 99999 s 0 1000
MIGRATE 127.0.0.1 {other} s 1 1000 COPY
MIGRATE 127.0.0.1 {other} s 1 1000 COPY REPLACE
MIGRATE 127.0.0.1 {other} s 1 1000 AUTH pw
MIGRATE 127.0.0.1 {other} s 1 1000 AUTH2 nobody pw
MIGRATE 127.0.0.1 {other} s 99 1000
MIGRATE 127.0.0.1 {other} s -1 1000
EXISTS s
MIGRATE 127.0.0.1 {other} s 1 0 REPLACE
EXISTS s
MIGRATE 127.0.0.1 {other} "" 1 1000 KEYS n n2 nosuch
EXISTS n n2
SET n 1
MIGRATE 127.0.0.1 {other} "" 1 1000 KEYS n n3
EXISTS n n3
@2 GET n
@2 GET n3
SET ttl v PX 100000
MIGRATE 127.0.0.1 {other} ttl 1 1000
@2 ?ttl PTTL ttl
MULTI
SET inmulti 1
MIGRATE 127.0.0.1 {other} inmulti 1 1000
EXEC
@2 GET inmulti
EVAL "return redis.call('migrate', '127.0.0.1', ARGV[1], KEYS[1], 1, 1000, 'COPY')" 1 l {other}
?cached INFO stats
!sleep 12500
?cached INFO stats
MIGRATE 127.0.0.1 {other} s 1 1000 REPLACE COPY
?cached INFO stats

# Payloads Redis 6.2 writes: ziplists, quicklists of them, zipmaps' successors,
# the first stream format.
!reset
!old FLUSHALL
!old SET ostr hello
!old SET onum 42
!old RPUSH ol a b 1 2 -300 70000
!old EVAL "for i=1,500 do redis.call('rpush', KEYS[1], 'element-' .. i) end" 1 olbig
!old HSET oh f v n 1
!old ZADD oz 1 a 2.5 b -1 c
!old EVAL "for i=1,200 do redis.call('zadd', KEYS[1], i, 'm' .. i) end" 1 ozbig
!old EVAL "for i=1,200 do redis.call('hset', KEYS[1], 'f' .. i, i) end" 1 ohbig
!old SADD os 1 2 3
!old SADD os2 a b c
!old XADD ox 1-1 a 1 b 2
!old XADD ox 1-2 a 3 b 4
!old XADD ox 2-0 c 5
!old XDEL ox 1-2
!old XGROUP CREATE ox g 0
!old XREADGROUP GROUP g c1 COUNT 1 STREAMS ox >
!import ostr onum ol olbig oh oz ozbig ohbig os os2 ox
GET ostr
GET onum
LRANGE ol 0 -1
LLEN olbig
LRANGE olbig 495 -1
HGETALL oh
ZRANGE oz 0 -1 WITHSCORES
ZRANGE ozbig 0 -1 WITHSCORES
?pairs HGETALL ohbig
?sorted SMEMBERS os
?sorted SMEMBERS os2
XRANGE ox - +
XINFO GROUPS ox
?ints XINFO STREAM ox FULL
DUMP ostr
DUMP onum
DUMP ol
DUMP oh
DUMP oz
DUMP ozbig
DUMP os
DUMP os2
OBJECT ENCODING ol
OBJECT ENCODING oh
OBJECT ENCODING oz
OBJECT ENCODING os2
"""

SCENARIOS = SINGLE + "\n" + CROSS


def crc64(data):
    """Redis' CRC-64 (Jones, reflected, no final xor)."""
    crc = 0
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ (0x95AC9329AC4BC9B5 if crc & 1 else 0)
    return crc


assert crc64(b"123456789") == 0xE9C6D914C4B8D9CA


def payloads(args):
    """Each {rdb:HEX:VERSION} argument as a payload: the bytes (spaces allowed),
    the RDB version and the checksum."""
    out = []
    for arg in args:
        found = re.fullmatch(rb"\{rdb:([0-9a-f ]*):(\d+)\}", arg)
        if found:
            body = bytes.fromhex(found.group(1).decode().replace(" ", ""))
            body += int(found.group(2)).to_bytes(2, "little")
            arg = body + crc64(body).to_bytes(8, "little")
        out.append(arg)
    return out


def masked(mode, reply):
    if reply == b"<closed>" or reply[:1] in b"-!":
        return reply
    if mode == "ttl":
        return re.sub(rb":[1-9]\d*\r\n", b":<positive>\r\n", reply)
    if mode == "cached":
        return b"".join(re.findall(rb"migrate_cached_sockets:\d+", reply))
    if mode == "pairs":
        head, elements = runner.items(reply)
        pairs = sorted(elements[i] + elements[i + 1] for i in range(0, len(elements), 2))
        return head + b"".join(pairs)
    return runner.masked(mode, reply)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--redis", type=int, default=6390)
    p.add_argument("--tantalum", type=int, default=6391)
    p.add_argument("--old", type=int, default=6392)
    p.add_argument("--rust", action="store_true")
    a = p.parse_args()
    scenarios = SINGLE if a.rust else SCENARIOS
    servers = {"redis": runner.Server(a.redis), "tantalum": runner.Server(a.tantalum)}
    other = {"redis": a.tantalum, "tantalum": a.redis}
    old = runner.Server(a.old)
    for name, server in [*servers.items(), ("old", old)]:
        version = server.call(0, [b"INFO", b"server"])
        found = re.search(rb"(redis_version|tantalum_version):([^\r]*)", version)
        print(f"# {name}: {found.group(0).decode() if found else '?'}", file=sys.stderr)
    for server in servers.values():
        server.reset()
    compared = differ = 0

    def compare(raw, conn, mode, replies, args=None):
        nonlocal compared, differ
        replies = {name: masked(mode, r) for name, r in replies.items()}
        compared += 1
        same = replies["redis"] == replies["tantalum"]
        if a.rust:
            shown = ", ".join(runner.rust_bytes(x) for x in args)
            if mode is None:
                mark = "" if same else "  // DIFFERS"
                print(f"    Step::Run({conn}, &[{shown}], {runner.rust_bytes(replies['redis'])}),{mark}")
            else:
                print(f"    Step::Send({conn}, &[{shown}]),")
            return
        if not same:
            differ += 1
            print(f"@{conn} {raw.strip()[:300]}")
            print(f"  redis:    {replies['redis'][:600]!r}")
            print(f"  tantalum: {replies['tantalum'][:600]!r}")

    for raw in scenarios.strip().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            if a.rust and line.startswith("#"):
                print(f"    // {line[1:].strip()}")
            continue
        if line == "!reset":
            if a.rust:
                print("    Step::Reset,")
            for server in servers.values():
                server.reset()
                # Database 1, where the other's MIGRATEs land.
                server.call(0, [b"SELECT", b"1"])
                server.call(0, [b"FLUSHDB"])
                server.drop(0)
            continue
        if line.startswith("!sleep "):
            time.sleep(int(line.split()[1]) / 1000)
            continue
        if line.startswith("!old "):
            old.call(0, runner.parse_args(line[5:]))
            continue
        if line.startswith("!import "):
            for key in line.split()[1:]:
                payload = old.call(0, [b"DUMP", key.encode()])
                body = payload[payload.index(b"\r\n") + 2 : -2]
                args = [b"RESTORE", key.encode(), b"0", body, b"REPLACE"]
                compare(f"RESTORE {key} (from Redis 6.2)", 0, None,
                        {n: s.call(0, args) for n, s in servers.items()})
            continue
        conn, mode = 0, None
        while line[0] in "@?":
            word, _, line = line.partition(" ")
            if word[0] == "@":
                conn = int(word[1:])
            else:
                mode = word[1:]
        replies = {}
        for name, server in servers.items():
            args = payloads(runner.parse_args(line.replace("{other}", str(other[name]))))
            replies[name] = server.call(conn, args)
        compare(raw, conn, mode, replies, args)
    if not a.rust:
        print(f"{compared} compared, {differ} differ")


if __name__ == "__main__":
    main()
