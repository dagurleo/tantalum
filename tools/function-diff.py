#!/usr/bin/env python3
"""Runs the same function scenarios (FUNCTION, FCALL) on Redis and on Tantalum
and shows where their replies differ, byte for byte: tools/acl-diff.py's
runner, with these scenarios, and FUNCTION FLUSH at each !reset.

    tools/function-diff.py [--redis 6390] [--tantalum 6391] [--rust]

Start both on spare ports with no password. Library code is written on one line,
with \\n for its newlines. FUNCTION LIST of more than one library is compared in
any order (?functions), as Redis lists libraries and functions in its hash
tables' order, which changes at each start.
"""

import importlib.util
import os

spec = importlib.util.spec_from_file_location(
    "acl_diff", os.path.join(os.path.dirname(os.path.abspath(__file__)), "acl-diff.py")
)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)

LIB = r'''"#!lua name=lib\nredis.register_function('f', function(keys, args) return redis.call('set', keys[1], args[1]) end)\nredis.register_function{function_name='ro', callback=function(keys) return redis.call('get', keys[1]) end, flags={'no-writes'}, description='reads a key'}"'''

SCENARIOS = r"""
# Loading.
?functions FUNCTION LIST
FUNCTION STATS
FUNCTION LOAD """ + LIB + r"""
?functions FUNCTION LIST
?functions FUNCTION LIST WITHCODE
FUNCTION STATS
FUNCTION LOAD """ + LIB + r"""
FUNCTION LOAD REPLACE """ + LIB + r"""
FUNCTION LOAD replace """ + LIB + r"""
FUNCTION LOAD FOO """ + LIB + r"""
FUNCTION LOAD
FUNCTION LOAD REPLACE
FUNCTION LOAD "redis.register_function('g', function() return 1 end)"
FUNCTION LOAD "#!lua name=lib2"
FUNCTION LOAD "#!lua\nredis.register_function('g', function() return 1 end)"
FUNCTION LOAD "#! name=lib2\nredis.register_function('g', function() return 1 end)"
FUNCTION LOAD "#!bad name=lib2\nredis.register_function('g', function() return 1 end)"
FUNCTION LOAD "#!lua name=lib2 foo=bar\nredis.register_function('g', function() return 1 end)"
FUNCTION LOAD "#!lua name=a name=b\nredis.register_function('g', function() return 1 end)"
FUNCTION LOAD "#!lua name=bad-name\nredis.register_function('g', function() return 1 end)"
FUNCTION LOAD "#!lua name=\nredis.register_function('g', function() return 1 end)"
FUNCTION LOAD "#!LUA name=\"quoted\"\nredis.register_function('quoted_f', function() return 1 end)"
FUNCTION LOAD "#!lua name=lib2\nbad script"
FUNCTION LOAD "#!lua name=lib2\nreturn 1"
FUNCTION LOAD "#!lua name=lib2\na = 1"
FUNCTION LOAD "#!lua name=lib2\nreturn math.random()"
FUNCTION LOAD "#!lua name=lib2\nreturn redis.call('ping')"
FUNCTION LOAD "#!lua name=lib2\nreturn redis.setresp(3)"
FUNCTION LOAD "#!lua name=lib2\nmt = getmetatable(_G)"
FUNCTION LOAD "#!lua name=lib2\nredis.register_function()"
FUNCTION LOAD "#!lua name=lib2\nredis.register_function('g')"
FUNCTION LOAD "#!lua name=lib2\nredis.register_function('g', function() return 1 end, 'x')"
FUNCTION LOAD "#!lua name=lib2\nredis.register_function(nil, function() return 1 end)"
FUNCTION LOAD "#!lua name=lib2\nredis.register_function('g', 'not a function')"
FUNCTION LOAD "#!lua name=lib2\nredis.register_function('', function() return 1 end)"
FUNCTION LOAD "#!lua name=lib2\nredis.register_function('a-b', function() return 1 end)"
FUNCTION LOAD "#!lua name=lib2\nredis.register_function('g', function() return 1 end)\nredis.register_function('g', function() return 2 end)"
FUNCTION LOAD "#!lua name=lib2\nredis.register_function('F', function() return 1 end)"
FUNCTION LOAD "#!lua name=lib2\nredis.register_function{function_name='g'}"
FUNCTION LOAD "#!lua name=lib2\nredis.register_function{callback=function() return 1 end}"
FUNCTION LOAD "#!lua name=lib2\nredis.register_function{function_name=1, callback=function() return 1 end}"
FUNCTION LOAD "#!lua name=lib2\nredis.register_function{function_name='g', callback='x'}"
FUNCTION LOAD "#!lua name=lib2\nredis.register_function{function_name='g', callback=function() return 1 end, description=1}"
FUNCTION LOAD "#!lua name=lib2\nredis.register_function{function_name='g', callback=function() return 1 end, description={}}"
FUNCTION LOAD "#!lua name=lib2\nredis.register_function{function_name='g', callback=function() return 1 end, flags='no-writes'}"
FUNCTION LOAD "#!lua name=lib2\nredis.register_function{function_name='g', callback=function() return 1 end, flags={'bad-flag'}}"
FUNCTION LOAD "#!lua name=lib2\nredis.register_function{function_name='g', callback=function() return 1 end, flags={1}}"
FUNCTION LOAD "#!lua name=lib2\nredis.register_function{function_name='g', callback=function() return 1 end, other=1}"
FUNCTION LOAD "#!lua name=lib2\nredis.register_function{'g', function() return 1 end}"
FUNCTION LOAD "#!lua name=lib2\nredis.register_function{function_name='g', callback=function() return 1 end, flags={'NO-WRITES', 'allow-oom', 'allow-stale', 'no-cluster', 'allow-cross-slot-keys'}}"
FUNCTION LOAD "#!lua name=lib2\nlocal lib = redis\nlib.register_function('mal1', function() lib.redis = redis return {ok='OK'} end)\nlib.register_function('mal2', function() lib.register_function('x', function() end) end)\nredis.register_function('rt', function() redis.register_function('y', function() return 2 end) return 1 end)"
?functions FUNCTION LIST
?functions FUNCTION LIST WITHCODE LIBRARYNAME lib2
?functions FUNCTION LIST LIBRARYNAME LIB*
?functions FUNCTION LIST LIBRARYNAME nomatch*
FUNCTION LIST bad_argument
FUNCTION LIST LIBRARYNAME
FUNCTION LIST WITHCODE WITHCODE
FUNCTION LIST WITHCODE LIBRARYNAME a LIBRARYNAME b
@1 ?ints HELLO 3
@1 ?functions FUNCTION LIST
@1 ?functions FUNCTION LIST LIBRARYNAME lib
@1 FUNCTION STATS
FUNCTION STATS

# Calling.
FCALL f 1 k v
FCALL F 1 k v2
FCALL_RO ro 1 k
FCALL_RO f 1 k v
FCALL nosuch 0
FCALL f x k
FCALL f 3 k
FCALL f -1 k
FCALL f
FCALL_RO ro
FCALL g 0
FCALL mal1 0
FCALL mal2 0
FCALL rt 0
FCALL_RO g 0
FUNCTION LOAD REPLACE "#!lua name=lib3\nredis.register_function('kinds', function(keys, args) return {1, 'two', {3}, redis.status_reply('OK'), false, 3.7} end)\nredis.register_function('err', function() return redis.error_reply('MY bad') end)\nredis.register_function('raise', function() error('boom') end)\nredis.register_function('raise_t', function() error({err='CUSTOM thing'}) end)\nredis.register_function('bad_call', function() return redis.call('nosuch') end)\nredis.register_function('pbad', function() return redis.pcall('incr', 'k') end)\nredis.register_function('global', function() return nosuch_global end)\nredis.register_function('resp3', function() redis.setresp(3) return redis.call('hgetall', 'h') end)\nredis.register_function('args', function(keys, args) return {#keys, #args, keys[1], args[1]} end)\nredis.register_function{function_name='nw', callback=function() return redis.call('set', 'x', 1) end, flags={'no-writes'}}\nredis.register_function('ver', function() return redis.REDIS_VERSION end)\nredis.register_function('trick', function() local mt = getmetatable(_G) local g = mt.__index g['redis'] = 1 end)"
FCALL kinds 0
@1 FCALL kinds 0
FCALL err 0
FCALL raise 0
FCALL raise_t 0
FCALL bad_call 0
FCALL pbad 0
FCALL global 0
HSET h a 1
FCALL resp3 0
@1 FCALL resp3 0
FCALL args 2 a b c d
FCALL nw 0
FCALL ver 0
FCALL trick 0
EVAL "return redis.call('fcall', 'ver', 0)" 0
MULTI
FCALL ver 0
?functions FUNCTION LIST LIBRARYNAME lib3x
EXEC
COMMAND GETKEYS FCALL f 2 a b c
COMMAND GETKEYS FCALL_RO f 0 a

# Deleting, dumping and restoring.
FUNCTION DELETE nosuch
FUNCTION DELETE lib3
FUNCTION DELETE lib3
FCALL kinds 0
FUNCTION DELETE
FUNCTION DELETE a b
!reset
FUNCTION LOAD """ + LIB + r"""
FUNCTION DUMP
FUNCTION LOAD "#!lua name=other\nredis.register_function('o', function() return 'other' end)"
?len FUNCTION DUMP
FUNCTION RESTORE nonsense
FUNCTION RESTORE "\xf5\x02ab\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00"
FUNCTION RESTORE "\x0b\x00\x9a\xf4\xeb\x1e\xa6\xc1\x8e\x8b"
FUNCTION RESTORE x BAD
FUNCTION RESTORE x FLUSH extra
FUNCTION FLUSH bad
FUNCTION FLUSH SYNC extra
FUNCTION FLUSH ASYNC
?functions FUNCTION LIST
FUNCTION FLUSH
FUNCTION FLUSH sync
FUNCTION LOAD "#!lua name=lib1\nlocal function add1(a) return a + 1 end\nredis.register_function('f1', function() return add1(1) end)\nredis.register_function('f2', function() return add1(2) end)\nredis.register_function('f3', function() return add1(3) end)"
FUNCTION DUMP
FUNCTION FLUSH

# Kill and stats with nothing running; help.
FUNCTION KILL
FUNCTION STATS
FUNCTION KILL x
FUNCTION STATS x
FUNCTION HELP
FUNCTION HELP x
FUNCTION
FUNCTION NOPE
FUNCTION DUMP x

# Users.
FUNCTION LOAD "#!lua name=u\nredis.register_function('uf', function(keys) return redis.call('get', keys[1]) end)"
ACL SETUSER fu on nopass +@all -@scripting ~*
@2 AUTH fu x
@2 FCALL uf 1 k
@2 ?functions FUNCTION LIST
ACL SETUSER fu2 on nopass +fcall ~a*
@3 AUTH fu2 x
@3 FCALL uf 1 abc
@3 FCALL uf 1 xyz
ACL DRYRUN fu2 FCALL uf 1 abc
ACL DRYRUN fu2 FCALL uf 1 xyz
ACL DRYRUN fu2 FCALL_RO uf 0
?log ACL LOG
"""

if __name__ == "__main__":
    runner.main(SCENARIOS, reset_extra=["FUNCTION FLUSH"])
