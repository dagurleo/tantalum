//! Lua scripting: replies recorded from Redis 7.2.4 for 312 commands on one
//! connection (return values, errors and where they happened, the sandbox,
//! the `redis` API, RESP3 inside scripts, the libraries, shebangs and the
//! script cache), and under concurrent clients in every I/O mode:
//! atomicity, `SCRIPT KILL`, scripts inside `EXEC`, and the clients a
//! script's writes wake.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common::{Client, MODES, Mode, start};

#[rustfmt::skip]
const SCRIPT: &[(&[&[u8]], &[u8])] = &[
    (&[b"FLUSHALL"], b"+OK\r\n"),
    (&[b"SCRIPT", b"FLUSH"], b"+OK\r\n"),
    (&[b"EVAL", b"return 1", b"0"], b":1\r\n"),
    (&[b"EVAL", b"return -7", b"0"], b":-7\r\n"),
    (&[b"EVAL", b"return 3.99", b"0"], b":3\r\n"),
    (&[b"EVAL", b"return -3.99", b"0"], b":-3\r\n"),
    (&[b"EVAL", b"return 1e15", b"0"], b":1000000000000000\r\n"),
    (&[b"EVAL", b"return 0.5", b"0"], b":0\r\n"),
    (&[b"EVAL", b"return -0.0", b"0"], b":0\r\n"),
    (&[b"EVAL", b"return 'hello'", b"0"], b"$5\r\nhello\r\n"),
    (&[b"EVAL", b"return ''", b"0"], b"$0\r\n\r\n"),
    (&[b"EVAL", b"return '\\0a\\0'", b"0"], b"$3\r\n\x00a\x00\r\n"),
    (&[b"EVAL", b"return true", b"0"], b":1\r\n"),
    (&[b"EVAL", b"return false", b"0"], b"$-1\r\n"),
    (&[b"EVAL", b"return nil", b"0"], b"$-1\r\n"),
    (&[b"EVAL", b"return {}", b"0"], b"*0\r\n"),
    (&[b"EVAL", b"return {1,2,3}", b"0"], b"*3\r\n:1\r\n:2\r\n:3\r\n"),
    (&[b"EVAL", b"return {1,nil,3}", b"0"], b"*1\r\n:1\r\n"),
    (&[b"EVAL", b"return {1,{2,{3,{4}}},'x',false,true}", b"0"], b"*5\r\n:1\r\n*2\r\n:2\r\n*2\r\n:3\r\n*1\r\n:4\r\n$1\r\nx\r\n$-1\r\n:1\r\n"),
    (&[b"EVAL", b"return {ok='fine'}", b"0"], b"+fine\r\n"),
    (&[b"EVAL", b"return {ok='a\\r\\nb'}", b"0"], b"+a  b\r\n"),
    (&[b"EVAL", b"return {ok=''}", b"0"], b"+\r\n"),
    (&[b"EVAL", b"return {err='My Error'}", b"0"], b"-My Error\r\n"),
    (&[b"EVAL", b"return {err='X'}", b"0"], b"-X\r\n"),
    (&[b"EVAL", b"return {err=''}", b"0"], b"-\r\n"),
    (&[b"EVAL", b"return {err='ERR with\\nnewline'}", b"0"], b"-ERR with newline\r\n"),
    (&[b"EVAL", b"return redis.error_reply('bad thing')", b"0"], b"-bad thing\r\n"),
    (&[b"EVAL", b"return redis.error_reply('-CODE msg')", b"0"], b"-CODE msg\r\n"),
    (&[b"EVAL", b"return redis.error_reply('NOSPACE')", b"0"], b"-ERR NOSPACE\r\n"),
    (&[b"EVAL", b"return redis.error_reply(1)", b"0"], b"-ERR wrong number or type of arguments\r\n"),
    (&[b"EVAL", b"return redis.error_reply()", b"0"], b"-ERR wrong number or type of arguments\r\n"),
    (&[b"EVAL", b"return redis.status_reply('SOMEOK')", b"0"], b"+SOMEOK\r\n"),
    (&[b"EVAL", b"return redis.status_reply(5)", b"0"], b"-ERR wrong number or type of arguments\r\n"),
    (&[b"EVAL", b"return {double=3.5}", b"0"], b"$3\r\n3.5\r\n"),
    (&[b"EVAL", b"return {double=0}", b"0"], b"$1\r\n0\r\n"),
    (&[b"EVAL", b"return {double=1/0}", b"0"], b"$3\r\ninf\r\n"),
    (&[b"EVAL", b"return {double=-1/0}", b"0"], b"$4\r\n-inf\r\n"),
    (&[b"EVAL", b"return {double=1e300}", b"0"], b"$6\r\n1e+300\r\n"),
    (&[b"EVAL", b"return {double=0.1}", b"0"], b"$3\r\n0.1\r\n"),
    (&[b"EVAL", b"return {double=-0.0}", b"0"], b"$2\r\n-0\r\n"),
    (&[b"EVAL", b"return {double=12345678901234567}", b"0"], b"$17\r\n12345678901234568\r\n"),
    (&[b"EVAL", b"return {big_number='123456789012345678901234567890'}", b"0"], b"$30\r\n123456789012345678901234567890\r\n"),
    (&[b"EVAL", b"return {verbatim_string={format='txt', string='hello'}}", b"0"], b"$5\r\nhello\r\n"),
    (&[b"EVAL", b"return {verbatim_string={format='markdown', string='hi'}}", b"0"], b"$2\r\nhi\r\n"),
    (&[b"EVAL", b"return {verbatim_string={format='x', string='hi'}}", b"0"], b"$2\r\nhi\r\n"),
    (&[b"EVAL", b"return {map={a=1}}", b"0"], b"*2\r\n$1\r\na\r\n:1\r\n"),
    (&[b"EVAL", b"return {set={a=true}}", b"0"], b"*1\r\n$1\r\na\r\n"),
    (&[b"EVAL", b"return {map={}}", b"0"], b"*0\r\n"),
    (&[b"EVAL", b"return {set={}}", b"0"], b"*0\r\n"),
    (&[b"EVAL", b"return {1, {ok='x'}, {err='y'}}", b"0"], b"*3\r\n:1\r\n+x\r\n-y\r\n"),
    (&[b"EVAL", b"return {err=5}", b"0"], b"*0\r\n"),
    (&[b"EVAL", b"return {ok=5}", b"0"], b"*0\r\n"),
    (&[b"EVAL", b"return function() end", b"0"], b"$-1\r\n"),
    (&[b"EVAL", b"return coroutine.create(function() end)", b"0"], b"$-1\r\n"),
    (&[b"EVAL", b"return KEYS", b"2", b"a", b"b", b"c"], b"*2\r\n$1\r\na\r\n$1\r\nb\r\n"),
    (&[b"EVAL", b"return ARGV", b"1", b"a", b"b", b"c"], b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n"),
    (&[b"EVAL", b"return {#KEYS, #ARGV}", b"0", b"x"], b"*2\r\n:0\r\n:1\r\n"),
    (&[b"EVAL", b"return 1"], b"-ERR wrong number of arguments for 'eval' command\r\n"),
    (&[b"EVAL", b"return 1", b"x"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"EVAL", b"return 1", b"3", b"a"], b"-ERR Number of keys can't be greater than number of args\r\n"),
    (&[b"EVAL", b"return 1", b"-1"], b"-ERR Number of keys can't be negative\r\n"),
    (&[b"EVAL", b"return 1", b" 1"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"EVAL", b"return 1", b"1.0", b"a"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"EVAL", b"return KEYS[1]", b"01", b"a"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"EVAL", b"return redis.call('set', KEYS[1], 'v')", b"1", b"s"], b"+OK\r\n"),
    (&[b"EVAL", b"return redis.call('get', KEYS[1])", b"1", b"s"], b"$1\r\nv\r\n"),
    (&[b"EVAL", b"return redis.call('get', 'nokey')", b"0"], b"$-1\r\n"),
    (&[b"EVAL", b"return redis.call('incr', 'n')", b"0"], b":1\r\n"),
    (&[b"EVAL", b"return redis.call('set', 'num', 3.14159)", b"0"], b"+OK\r\n"),
    (&[b"EVAL", b"return redis.call('get', 'num')", b"0"], b"$7\r\n3.14159\r\n"),
    (&[b"EVAL", b"redis.call('set', 'big', 1e20) return redis.call('get', 'big')", b"0"], b"$5\r\n1e+20\r\n"),
    (&[b"EVAL", b"redis.call('set', 'neg', -0.0) return redis.call('get', 'neg')", b"0"], b"$2\r\n-0\r\n"),
    (&[b"EVAL", b"redis.call('set', 'third', 1/3) return redis.call('get', 'third')", b"0"], b"$18\r\n0.3333333333333333\r\n"),
    (&[b"EVAL", b"redis.call('set', 'int', 100) return redis.call('get', 'int')", b"0"], b"$3\r\n100\r\n"),
    (&[b"EVAL", b"return redis.call('rpush', 'l', 'a', 'b', 'c')", b"0"], b":3\r\n"),
    (&[b"EVAL", b"return redis.call('lrange', 'l', 0, -1)", b"0"], b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"),
    (&[b"EVAL", b"return redis.call('hset', 'h', 'f1', 'v1', 'f2', 'v2')", b"0"], b":2\r\n"),
    (&[b"EVAL", b"return redis.call('hgetall', 'h')", b"0"], b"*4\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n"),
    (&[b"EVAL", b"return redis.call('sadd', 'st', 'x', 'y')", b"0"], b":2\r\n"),
    (&[b"EVAL", b"return redis.call('zadd', 'z', 1.5, 'a', 2, 'b')", b"0"], b":2\r\n"),
    (&[b"EVAL", b"return redis.call('zrange', 'z', 0, -1, 'WITHSCORES')", b"0"], b"*4\r\n$1\r\na\r\n$3\r\n1.5\r\n$1\r\nb\r\n$1\r\n2\r\n"),
    (&[b"EVAL", b"return redis.call('zscore', 'z', 'a')", b"0"], b"$3\r\n1.5\r\n"),
    (&[b"EVAL", b"return redis.call('type', 'z')", b"0"], b"+zset\r\n"),
    (&[b"EVAL", b"return redis.call('ping')", b"0"], b"+PONG\r\n"),
    (&[b"EVAL", b"return redis.call('exists', 'z', 'h', 'nope')", b"0"], b":2\r\n"),
    (&[b"EVAL", b"return redis.call('get', 'l')", b"0"], b"-WRONGTYPE Operation against a key holding the wrong kind of value script: e471b73f1ef44774987ab00bdf51f21fd9f7974a, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.pcall('get', 'l')", b"0"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"EVAL", b"local r = redis.pcall('get', 'l') return r['err']", b"0"], b"$65\r\nWRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"EVAL", b"return redis.pcall('nosuchcmd')", b"0"], b"-ERR Unknown Redis command called from script\r\n"),
    (&[b"EVAL", b"return redis.call('nosuchcmd')", b"0"], b"-ERR Unknown Redis command called from script script: 4f5958446b28593dea988ad0a5603cbd6962dd95, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.call('get')", b"0"], b"-ERR Wrong number of args calling Redis command from script script: f187dee77f607f7cff00bbabc9df38507582c78d, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.pcall('get')", b"0"], b"-ERR Wrong number of args calling Redis command from script\r\n"),
    (&[b"EVAL", b"return redis.call()", b"0"], b"-ERR Please specify at least one argument for this redis lib call script: 0a907e1429221a4d85516cab7fd219a82a9439d8, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.pcall()", b"0"], b"-ERR Please specify at least one argument for this redis lib call\r\n"),
    (&[b"EVAL", b"return redis.call('get', {})", b"0"], b"-ERR Lua redis lib command arguments must be strings or integers script: 4368e88f98d93af9caf358186e5fbf273374ee91, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.pcall('get', nil)", b"0"], b"-ERR Lua redis lib command arguments must be strings or integers\r\n"),
    (&[b"EVAL", b"return redis.call('set', 'x', true)", b"0"], b"-ERR Lua redis lib command arguments must be strings or integers script: f90c094bf83beb44b4a2fc71a2b37f56cdb0ff15, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.call('multi')", b"0"], b"-ERR This Redis command is not allowed from script script: c329a1a294be6bf95b44144f7502610661ae925a, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.call('exec')", b"0"], b"-ERR This Redis command is not allowed from script script: cfd06730c41ba24d30327b6d49d1ebf96a001298, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.call('subscribe', 'c')", b"0"], b"-ERR This Redis command is not allowed from script script: ef3d3b3db03eea01b69a3675b5e27984671b35c3, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.call('eval', 'return 1', 0)", b"0"], b"-ERR This Redis command is not allowed from script script: 310efa61ec0f2edf6c2e5b4007966dd555e75546, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.call('script', 'load', 'return 1')", b"0"], b"-ERR This Redis command is not allowed from script script: a490477d6bc12575fb844107684e6cd178d473fe, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.call('script', 'help')", b"0"], b"*17\r\n+SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:\r\n+DEBUG (YES|SYNC|NO)\r\n+    Set the debug mode for subsequent scripts executed.\r\n+EXISTS <sha1> [<sha1> ...]\r\n+    Return information about the existence of the scripts in the script cache.\r\n+FLUSH [ASYNC|SYNC]\r\n+    Flush the Lua scripts cache. Very dangerous on replicas.\r\n+    When called without the optional mode argument, the behavior is determined by the\r\n+    lazyfree-lazy-user-flush configuration directive. Valid modes are:\r\n+    * ASYNC: Asynchronously flush the scripts cache.\r\n+    * SYNC: Synchronously flush the scripts cache.\r\n+KILL\r\n+    Kill the currently executing Lua script.\r\n+LOAD <script>\r\n+    Load a script into the scripts cache without executing it.\r\n+HELP\r\n+    Print this help.\r\n"),
    (&[b"EVAL", b"return redis.call('client', 'getname')", b"0"], b"-ERR This Redis command is not allowed from script script: 378c1ad2b97ae88db40b83fb272999bef09f7559, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.call('config', 'get', 'maxmemory')", b"0"], b"-ERR This Redis command is not allowed from script script: 0980669769efeb6079a5a3c93e7ca4e95eedfa17, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.call('hello', 3)", b"0"], b"-ERR This Redis command is not allowed from script script: 9072650c185571d79d3f6ecc75c089d07fc0c292, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.call('auth', 'x')", b"0"], b"-ERR This Redis command is not allowed from script script: 7f1217694d43dd4cdc3ff084133cc4d0682328ab, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.call('watch', 'x')", b"0"], b"-ERR This Redis command is not allowed from script script: 2145a9e9d92db8f07774fabbaa3d4c5e443be426, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.call('quit')", b"0"], b"-ERR This Redis command is not allowed from script script: 594184995799820e8ea15ff5ed6e55342c8fed42, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.call('blpop', 'emptylist', 0)", b"0"], b"$-1\r\n"),
    (&[b"EVAL", b"return redis.call('brpoplpush', 'emptylist', 'dst', 0)", b"0"], b"$-1\r\n"),
    (&[b"EVAL", b"return redis.call('blmove', 'emptylist', 'dst', 'LEFT', 'RIGHT', 0)", b"0"], b"$-1\r\n"),
    (&[b"EVAL", b"return redis.call('bzpopmin', 'emptyz', 0)", b"0"], b"$-1\r\n"),
    (&[b"EVAL", b"return redis.call('xread', 'BLOCK', 0, 'STREAMS', 'nostream', '$')", b"0"], b"-ERR xread command is not allowed with BLOCK option from scripts script: 64075e709930467bee987abeef7fa6b5569dd63a, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.call('blpop', 'l', 0)", b"0"], b"*2\r\n$1\r\nl\r\n$1\r\na\r\n"),
    (&[b"EVAL", b"return redis.call('publish', 'ch', 'msg')", b"0"], b":0\r\n"),
    (&[b"EVAL", b"local t = redis.call('time') return #t", b"0"], b":2\r\n"),
    (&[b"EVAL", b"return redis.call('dbsize')", b"0"], b":11\r\n"),
    (&[b"EVAL", b"return redis.call('xadd', 'xs', '1-1', 'f', 'v')", b"0"], b"$3\r\n1-1\r\n"),
    (&[b"EVAL", b"return redis.call('xrange', 'xs', '-', '+')", b"0"], b"*1\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"EVAL", b"return redis.call('GET', 'S')", b"0"], b"$-1\r\n"),
    (&[b"EVAL", b"return redis.call('gEt', 's')", b"0"], b"$1\r\nv\r\n"),
    (&[b"EVAL", b"error('boom')", b"0"], b"-ERR user_script:1: boom script: 82903a0434f1503e152f89c03c9acd881a0e8150, on @user_script:1.\r\n"),
    (&[b"EVAL", b"error('boom', 0)", b"0"], b"-ERR boom script: 90724e16396e5864c1184910ba6d7440461cee4f, on @user_script:1.\r\n"),
    (&[b"EVAL", b"error({err='TABLEERR x'})", b"0"], b"-TABLEERR x script: cf9d383af29399ff37b748146b58325e9f724df1, on @user_script:1.\r\n"),
    (&[b"EVAL", b"error()", b"0"], b"-ERR nil script: aca90d914519fdb9825352bdc0befb59f7c8510f, on @user_script:1.\r\n"),
    (&[b"EVAL", b"error(nil)", b"0"], b"-ERR nil script: c402aa49952c86c38f35fb311d54977d08f3bf01, on @user_script:1.\r\n"),
    (&[b"EVAL", b"error(42)", b"0"], b"-ERR user_script:1: 42 script: acc7142a53d840846449a0a8c6055f5a674809fd, on @user_script:1.\r\n"),
    (&[b"EVAL", b"local function f() error('deep') end\nlocal function g() f() end\ng()", b"0"], b"-ERR user_script:1: deep script: fcfe7b39a4bd241fc3b57a447bdf7decec15e500, on @user_script:1.\r\n"),
    (&[b"EVAL", b"\n\nlocal x = nil + 1", b"0"], b"-ERR user_script:3: attempt to perform arithmetic on a nil value script: e2d68346255c148997029726b0e1a01d7b77715d, on @user_script:3.\r\n"),
    (&[b"EVAL", b"return 1 +", b"0"], b"-ERR Error compiling script (new function): user_script:1: unexpected symbol near '<eof>'\r\n"),
    (&[b"EVAL", b"return )", b"0"], b"-ERR Error compiling script (new function): user_script:1: unexpected symbol near ')'\r\n"),
    (&[b"EVAL", b"local t = {} t.x.y = 1", b"0"], b"-ERR user_script:1: attempt to index field 'x' (a nil value) script: a3e688812e3164b24d5a4a070bf45fd27d50fc57, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return #nil", b"0"], b"-ERR user_script:1: attempt to get length of a nil value script: 6d6abd31d8721234b1ee1a830316ae36ef2693e2, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return nosuchfn()", b"0"], b"-ERR user_script:1: Script attempted to access nonexistent global variable 'nosuchfn' script: a7fdd0a69dc75bd6f371d0af88f18f7fce637289, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return string.nosuch()", b"0"], b"-ERR user_script:1: attempt to call field 'nosuch' (a nil value) script: 09ae656737bc83d7f22230828020994274fcc76e, on @user_script:1.\r\n"),
    (&[b"EVAL", b"x = 5", b"0"], b"-ERR user_script:1: Attempt to modify a readonly table script: 818a330663e3c3e78469660421595218e5ab5c48, on @user_script:1.\r\n"),
    (&[b"EVAL", b"_G.x = 5", b"0"], b"-ERR user_script:1: Attempt to modify a readonly table script: 62fffb1eb74fcbfa7ad753cd71b8bad98e9cd136, on @user_script:1.\r\n"),
    (&[b"EVAL", b"rawset(_G, 'x', 1)", b"0"], b"-ERR Attempt to modify a readonly table script: e2c706419da183d6229a1230c3be09cc32cdeee0, on @user_script:1.\r\n"),
    (&[b"EVAL", b"setmetatable(_G, {})", b"0"], b"-ERR Attempt to modify a readonly table script: 0bf0a51fbb051c942113de0105d3f8e18cdcddb5, on @user_script:1.\r\n"),
    (&[b"EVAL", b"string.foo = 1", b"0"], b"-ERR user_script:1: Attempt to modify a readonly table script: 46f9bf166bc7e1ab2ed3b9de7b99c4e1e7dc1f7d, on @user_script:1.\r\n"),
    (&[b"EVAL", b"redis.call = nil", b"0"], b"-ERR user_script:1: Attempt to modify a readonly table script: 2786c35ed73d3860851cf52e0b5dfbbc0bdb4803, on @user_script:1.\r\n"),
    (&[b"EVAL", b"cjson.encode = nil", b"0"], b"-ERR user_script:1: Attempt to modify a readonly table script: ca8772b89bfaafed79b4e978e804ff876bb3abc5, on @user_script:1.\r\n"),
    (&[b"EVAL", b"math.pi = 3", b"0"], b"-ERR user_script:1: Attempt to modify a readonly table script: 29d944a5e7490072c60ab9cdf9bf7c8d2c97058c, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return print", b"0"], b"-ERR user_script:1: Script attempted to access nonexistent global variable 'print' script: df4c752a7558004676bbee87e38b92e0b9056d68, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return loadfile", b"0"], b"-ERR user_script:1: Script attempted to access nonexistent global variable 'loadfile' script: a08fbe72c95f67027cc9b6349f5d335b598397b7, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return dofile", b"0"], b"-ERR user_script:1: Script attempted to access nonexistent global variable 'dofile' script: 0c5f629ecf4a281cada464398cd571c2f6828b27, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return debug", b"0"], b"-ERR user_script:1: Script attempted to access nonexistent global variable 'debug' script: 7f0eed9eaa523ec9e1ff69159f45a399473d9771, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return os", b"0"], b"-ERR user_script:1: Script attempted to access nonexistent global variable 'os' script: f03b5ad6e7d58786b6a5e879efbea4a4408f5c2f, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return io", b"0"], b"-ERR user_script:1: Script attempted to access nonexistent global variable 'io' script: badfe324d30f60abd2dbecf4d4b0f915038384e2, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return require", b"0"], b"-ERR user_script:1: Script attempted to access nonexistent global variable 'require' script: 68c9d8918a98cebaec1948aff703e96c57af9fdd, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return package", b"0"], b"-ERR user_script:1: Script attempted to access nonexistent global variable 'package' script: bfab18646c3a65e87365badef4807726c27a0041, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return module", b"0"], b"-ERR user_script:1: Script attempted to access nonexistent global variable 'module' script: 2803774b6b78613fcfa3f4ce44070f589e937542, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return type(loadstring)", b"0"], b"$8\r\nfunction\r\n"),
    (&[b"EVAL", b"return loadstring('return 7')()", b"0"], b":7\r\n"),
    (&[b"EVAL", b"return {pcall(error, 'x')}", b"0"], b"*2\r\n$-1\r\n$1\r\nx\r\n"),
    (&[b"EVAL", b"return {pcall(function() redis.call('nosuch') end)}", b"0"], b"*2\r\n$-1\r\n$44\r\nERR Unknown Redis command called from script\r\n"),
    (&[b"EVAL", b"return {pcall(function() error({err='E1 m'}) end)}", b"0"], b"*2\r\n$-1\r\n$4\r\nE1 m\r\n"),
    (&[b"EVAL", b"return {xpcall(function() error('x') end, function(e) return 'handled: ' .. e end)}", b"0"], b"*2\r\n$-1\r\n$25\r\nhandled: user_script:1: x\r\n"),
    (&[b"EVAL", b"local ok, e = pcall(redis.call, 'get', 'l') return {tostring(ok), e}", b"0"], b"*2\r\n$5\r\nfalse\r\n$65\r\nWRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"EVAL", b"return redis.call('get', 'l')", b"0"], b"-WRONGTYPE Operation against a key holding the wrong kind of value script: e471b73f1ef44774987ab00bdf51f21fd9f7974a, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return {err='first'} -- comment", b"0"], b"-first\r\n"),
    (&[b"EVAL", b"return redis.sha1hex('')", b"0"], b"$40\r\nda39a3ee5e6b4b0d3255bfef95601890afd80709\r\n"),
    (&[b"EVAL", b"return redis.sha1hex('abc')", b"0"], b"$40\r\na9993e364706816aba3e25717850c26c9cd0d89d\r\n"),
    (&[b"EVAL", b"return redis.sha1hex()", b"0"], b"-ERR wrong number of arguments script: 3c7ce947ae74a835cc575b6ee87fb27503cb7ba4, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.sha1hex(123)", b"0"], b"$40\r\n40bd001563085fc35165329ea1ff5c5ecbdbbeef\r\n"),
    (&[b"EVAL", b"return redis.REDIS_VERSION", b"0"], b"$5\r\n7.2.4\r\n"),
    (&[b"EVAL", b"return redis.REDIS_VERSION_NUM", b"0"], b":459268\r\n"),
    (&[b"EVAL", b"return redis.log(redis.LOG_DEBUG, 'x')", b"0"], b"$-1\r\n"),
    (&[b"EVAL", b"return redis.log(redis.LOG_WARNING)", b"0"], b"-ERR redis.log() requires two arguments or more. script: 224c2b6c4d4a245fb6c4394854d25a2ca448909c, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.log('a', 'b')", b"0"], b"-ERR First argument must be a number (log level). script: a18a496257a6e37fb1d228e7a552fc022cf3a674, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.log(9, 'b')", b"0"], b"-ERR Invalid debug level. script: ab74876ad3951d09018229eb510209f72c4cdde4, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return {redis.LOG_DEBUG, redis.LOG_VERBOSE, redis.LOG_NOTICE, redis.LOG_WARNING}", b"0"], b"*4\r\n:0\r\n:1\r\n:2\r\n:3\r\n"),
    (&[b"EVAL", b"return {redis.REPL_NONE, redis.REPL_AOF, redis.REPL_SLAVE, redis.REPL_REPLICA, redis.REPL_ALL}", b"0"], b"*5\r\n:0\r\n:1\r\n:2\r\n:2\r\n:3\r\n"),
    (&[b"EVAL", b"return redis.set_repl(redis.REPL_ALL)", b"0"], b"$-1\r\n"),
    (&[b"EVAL", b"return redis.set_repl(8)", b"0"], b"-ERR Invalid replication flags. Use REPL_AOF, REPL_REPLICA, REPL_ALL or REPL_NONE. script: 186ef0ab02df6866cf837bcb124a0cff989e3d09, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.set_repl()", b"0"], b"-ERR redis.set_repl() requires one argument. script: 9504533b41060c143a92eb2f8878e77d33b6dc33, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.replicate_commands()", b"0"], b":1\r\n"),
    (&[b"EVAL", b"return redis.breakpoint()", b"0"], b"$-1\r\n"),
    (&[b"EVAL", b"return redis.debug('x')", b"0"], b"$-1\r\n"),
    (&[b"EVAL", b"return redis.setresp(4)", b"0"], b"-ERR RESP version must be 2 or 3. script: 921c0a48021d97dfd5884aeea41875972c7804d9, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.setresp()", b"0"], b"-ERR redis.setresp() requires one argument. script: 76da64e28014e7c9ad76db33c3918c53d81a3d7e, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.acl_check_cmd('get', 'x')", b"0"], b":1\r\n"),
    (&[b"EVAL", b"return redis.acl_check_cmd('nosuch')", b"0"], b"-ERR Invalid command passed to redis.acl_check_cmd() script: d2491587bab5149c93be0dce4426f7a06ba9e1a1, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return redis.acl_check_cmd()", b"0"], b"-ERR Please specify at least one argument for this redis lib call script: 1124f33ad66e7866b4e0c1499a4c20fb5eb9c180, on @user_script:1.\r\n"),
    (&[b"EVAL", b"local n = 0 for k, v in pairs(redis) do n = n + 1 end return n", b"0"], b":23\r\n"),
    (&[b"EVAL", b"local t = {} for k, v in pairs(redis) do t[#t+1] = k end table.sort(t) return t", b"0"], b"*23\r\n$9\r\nLOG_DEBUG\r\n$10\r\nLOG_NOTICE\r\n$11\r\nLOG_VERBOSE\r\n$11\r\nLOG_WARNING\r\n$13\r\nREDIS_VERSION\r\n$17\r\nREDIS_VERSION_NUM\r\n$8\r\nREPL_ALL\r\n$8\r\nREPL_AOF\r\n$9\r\nREPL_NONE\r\n$12\r\nREPL_REPLICA\r\n$10\r\nREPL_SLAVE\r\n$13\r\nacl_check_cmd\r\n$10\r\nbreakpoint\r\n$4\r\ncall\r\n$5\r\ndebug\r\n$11\r\nerror_reply\r\n$3\r\nlog\r\n$5\r\npcall\r\n$18\r\nreplicate_commands\r\n$8\r\nset_repl\r\n$7\r\nsetresp\r\n$7\r\nsha1hex\r\n$12\r\nstatus_reply\r\n"),
    (&[b"EVAL", b"local t = {} for k, v in pairs(_G) do t[#t+1] = k end table.sort(t) return t", b"0"], b"*38\r\n$4\r\nARGV\r\n$4\r\nKEYS\r\n$2\r\n_G\r\n$8\r\n_VERSION\r\n$21\r\n__redis__err__handler\r\n$6\r\nassert\r\n$3\r\nbit\r\n$5\r\ncjson\r\n$8\r\ncmsgpack\r\n$14\r\ncollectgarbage\r\n$9\r\ncoroutine\r\n$5\r\nerror\r\n$6\r\ngcinfo\r\n$7\r\ngetfenv\r\n$12\r\ngetmetatable\r\n$6\r\nipairs\r\n$4\r\nload\r\n$10\r\nloadstring\r\n$4\r\nmath\r\n$8\r\nnewproxy\r\n$4\r\nnext\r\n$5\r\npairs\r\n$5\r\npcall\r\n$8\r\nrawequal\r\n$6\r\nrawget\r\n$6\r\nrawset\r\n$5\r\nredis\r\n$6\r\nselect\r\n$7\r\nsetfenv\r\n$12\r\nsetmetatable\r\n$6\r\nstring\r\n$6\r\nstruct\r\n$5\r\ntable\r\n$8\r\ntonumber\r\n$8\r\ntostring\r\n$4\r\ntype\r\n$6\r\nunpack\r\n$6\r\nxpcall\r\n"),
    (&[b"EVAL", b"local t = {} for k, v in pairs(math) do t[#t+1] = k end table.sort(t) return t", b"0"], b"*31\r\n$3\r\nabs\r\n$4\r\nacos\r\n$4\r\nasin\r\n$4\r\natan\r\n$5\r\natan2\r\n$4\r\nceil\r\n$3\r\ncos\r\n$4\r\ncosh\r\n$3\r\ndeg\r\n$3\r\nexp\r\n$5\r\nfloor\r\n$4\r\nfmod\r\n$5\r\nfrexp\r\n$4\r\nhuge\r\n$5\r\nldexp\r\n$3\r\nlog\r\n$5\r\nlog10\r\n$3\r\nmax\r\n$3\r\nmin\r\n$3\r\nmod\r\n$4\r\nmodf\r\n$2\r\npi\r\n$3\r\npow\r\n$3\r\nrad\r\n$6\r\nrandom\r\n$10\r\nrandomseed\r\n$3\r\nsin\r\n$4\r\nsinh\r\n$4\r\nsqrt\r\n$3\r\ntan\r\n$4\r\ntanh\r\n"),
    (&[b"EVAL", b"local t = {} for k, v in pairs(string) do t[#t+1] = k end table.sort(t) return t", b"0"], b"*15\r\n$4\r\nbyte\r\n$4\r\nchar\r\n$4\r\ndump\r\n$4\r\nfind\r\n$6\r\nformat\r\n$5\r\ngfind\r\n$6\r\ngmatch\r\n$4\r\ngsub\r\n$3\r\nlen\r\n$5\r\nlower\r\n$5\r\nmatch\r\n$3\r\nrep\r\n$7\r\nreverse\r\n$3\r\nsub\r\n$5\r\nupper\r\n"),
    (&[b"EVAL", b"redis.setresp(3) return redis.call('hgetall', 'h')", b"0"], b"*4\r\n$2\r\nf2\r\n$2\r\nv2\r\n$2\r\nf1\r\n$2\r\nv1\r\n"),
    (&[b"EVAL", b"redis.setresp(3) local r = redis.call('hgetall', 'h') return r['map']['f1']", b"0"], b"$2\r\nv1\r\n"),
    (&[b"EVAL", b"redis.setresp(3) return redis.call('smembers', 'st')", b"0"], b"*2\r\n$1\r\ny\r\n$1\r\nx\r\n"),
    (&[b"EVAL", b"redis.setresp(3) return redis.call('zscore', 'z', 'a')", b"0"], b"$3\r\n1.5\r\n"),
    (&[b"EVAL", b"redis.setresp(3) local r = redis.call('zscore', 'z', 'a') return tostring(r['double'])", b"0"], b"$3\r\n1.5\r\n"),
    (&[b"EVAL", b"redis.setresp(3) return redis.call('get', 'nokey')", b"0"], b"$-1\r\n"),
    (&[b"EVAL", b"redis.setresp(3) return type(redis.call('get', 'nokey'))", b"0"], b"$3\r\nnil\r\n"),
    (&[b"EVAL", b"redis.setresp(3) return true", b"0"], b":1\r\n"),
    (&[b"EVAL", b"redis.setresp(3) return false", b"0"], b":0\r\n"),
    (&[b"EVAL", b"redis.setresp(3) return redis.call('zrange', 'z', 0, -1, 'WITHSCORES')", b"0"], b"*2\r\n*2\r\n$1\r\na\r\n$3\r\n1.5\r\n*2\r\n$1\r\nb\r\n$1\r\n2\r\n"),
    (&[b"EVAL", b"redis.setresp(3) return redis.call('lpop', 'nolist')", b"0"], b"$-1\r\n"),
    (&[b"EVAL", b"redis.setresp(3) return redis.call('ping')", b"0"], b"+PONG\r\n"),
    (&[b"EVAL", b"redis.setresp(2) return redis.call('hgetall', 'h')", b"0"], b"*4\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n"),
    (&[b"EVAL", b"return cjson.encode({1, 2, 'three', {four=4}})", b"0"], b"$24\r\n[1,2,\"three\",{\"four\":4}]\r\n"),
    (&[b"EVAL", b"return cjson.encode({a=1.5, b=true, c=cjson.null})", b"0"], b"$27\r\n{\"a\":1.5,\"c\":null,\"b\":true}\r\n"),
    (&[b"EVAL", b"return cjson.encode(1/3)", b"0"], b"$16\r\n0.33333333333333\r\n"),
    (&[b"EVAL", b"return cjson.encode(1e100)", b"0"], b"$6\r\n1e+100\r\n"),
    (&[b"EVAL", b"return cjson.encode('a/b\"c\\n')", b"0"], b"$11\r\n\"a\\/b\\\"c\\n\"\r\n"),
    (&[b"EVAL", b"return cjson.encode({})", b"0"], b"$2\r\n{}\r\n"),
    (&[b"EVAL", b"return cjson.decode('[1,2')", b"0"], b"-ERR user_script:1: Expected comma or array end but found T_END at character 5 script: c107001b5ba2ff860ee2557f64da37c4767ef4bd, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return cjson.encode(cjson.decode('[1.5e3]'))", b"0"], b"$6\r\n[1500]\r\n"),
    (&[b"EVAL", b"return cmsgpack.pack({1, 2, 3})", b"0"], b"$4\r\n\x93\x01\x02\x03\r\n"),
    (&[b"EVAL", b"return cmsgpack.pack({a=1})", b"0"], b"$4\r\n\x81\xa1a\x01\r\n"),
    (&[b"EVAL", b"return {cmsgpack.unpack(cmsgpack.pack(1, 'two', {3}))}", b"0"], b"*3\r\n:1\r\n$3\r\ntwo\r\n*1\r\n:3\r\n"),
    (&[b"EVAL", b"return cmsgpack.pack(1.5, -1, 2^40, 'x')", b"0"], b"$17\r\n\xca?\xc0\x00\x00\xff\xcf\x00\x00\x01\x00\x00\x00\x00\x00\xa1x\r\n"),
    (&[b"EVAL", b"return {cmsgpack.unpack('\\x93\\x01\\x02\\x03')}", b"0"], b"*12\r\n:120\r\n:57\r\n:51\r\n:120\r\n:48\r\n:49\r\n:120\r\n:48\r\n:50\r\n:120\r\n:48\r\n:51\r\n"),
    (&[b"EVAL", b"return struct.pack('>I2', 258)", b"0"], b"$2\r\n\x01\x02\r\n"),
    (&[b"EVAL", b"return {struct.unpack('>I2', '\\1\\2')}", b"0"], b"*2\r\n:258\r\n:3\r\n"),
    (&[b"EVAL", b"return bit.band(12, 10)", b"0"], b":8\r\n"),
    (&[b"EVAL", b"return bit.tohex(255)", b"0"], b"$8\r\n000000ff\r\n"),
    (&[b"EVAL", b"return bit.lshift(1, 40)", b"0"], b":256\r\n"),
    (&[b"EVAL", b"return string.format('%5.2f|%d|%s|%q', 3.14159, 42, 'x', 'a\"b')", b"0"], b"$17\r\n 3.14|42|x|\"a\\\"b\"\r\n"),
    (&[b"EVAL", b"return tostring(1/3)", b"0"], b"$16\r\n0.33333333333333\r\n"),
    (&[b"EVAL", b"return tostring(1e15)", b"0"], b"$5\r\n1e+15\r\n"),
    (&[b"EVAL", b"return tostring(2^53)", b"0"], b"$18\r\n9.007199254741e+15\r\n"),
    (&[b"EVAL", b"return tostring(0/0) .. tostring(-1/0)", b"0"], b"$7\r\nnan-inf\r\n"),
    (&[b"EVAL", b"return tonumber('0x10')", b"0"], b":16\r\n"),
    (&[b"EVAL", b"return table.concat({1, 2, 3}, ',')", b"0"], b"$5\r\n1,2,3\r\n"),
    (&[b"EVAL", b"return #'abc' .. string.rep('ab', 3)", b"0"], b"$7\r\n3ababab\r\n"),
    (&[b"EVAL", b"return {string.byte('ABC', 1, 3)}", b"0"], b"*3\r\n:65\r\n:66\r\n:67\r\n"),
    (&[b"EVAL", b"return string.gsub('hello world', 'o', '0')", b"0"], b"$11\r\nhell0 w0rld\r\n"),
    (&[b"EVAL", b"return _VERSION", b"0"], b"$7\r\nLua 5.1\r\n"),
    (&[b"EVAL", b"return type(newproxy)", b"0"], b"$8\r\nfunction\r\n"),
    (&[b"EVAL", b"return gcinfo() > 0", b"0"], b":1\r\n"),
    (&[b"EVAL", b"return {math.floor(3.7), math.max(1, 5, 3), math.huge > 0}", b"0"], b"*3\r\n:3\r\n:5\r\n:1\r\n"),
    (&[b"EVAL", b"math.randomseed(42) return {math.random(100), math.random(100), math.random(1, 6)}", b"0"], b"*3\r\n:75\r\n:35\r\n:1\r\n"),
    (&[b"EVAL", b"return math.random(0)", b"0"], b"-ERR user_script:1: bad argument #1 to 'random' (interval is empty) script: 008bcb60f86ee36d26c67b641b46674578d9cba0, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return math.random(5, 1)", b"0"], b"-ERR user_script:1: bad argument #2 to 'random' (interval is empty) script: 1d8aa02a60cda8bed17d3a4d709a3e8508a63268, on @user_script:1.\r\n"),
    (&[b"EVAL", b"return math.random(1, 2, 3)", b"0"], b"-ERR user_script:1: wrong number of arguments script: 07d2fd85ff1e5c39f9bd7d9163eddb0db34f2a7a, on @user_script:1.\r\n"),
    (&[b"EVAL", b"math.randomseed(1) return tostring(math.random())", b"0"], b"$17\r\n0.041630344484761\r\n"),
    (&[b"EVAL", b"return unpack({1, 2, 3})", b"0"], b":1\r\n"),
    (&[b"EVAL", b"return select('#', 1, 2, 3)", b"0"], b":3\r\n"),
    (&[b"EVAL", b"return getmetatable('').__index == string", b"0"], b":1\r\n"),
    (&[b"EVAL", b"local co = coroutine.wrap(function() coroutine.yield(1) end) return co()", b"0"], b":1\r\n"),
    (&[b"EVAL", b"return collectgarbage('count') > 0", b"0"], b":1\r\n"),
    (&[b"EVAL", b"return tostring(setmetatable({}, {__tostring = function() return 'custom' end}))", b"0"], b"$6\r\ncustom\r\n"),
    (&[b"EVAL", b"#!lua\nreturn 1", b"0"], b":1\r\n"),
    (&[b"EVAL", b"#!lua flags=no-writes\nreturn redis.call('get', 's')", b"0"], b"$1\r\nv\r\n"),
    (&[b"EVAL", b"#!lua flags=no-writes\nreturn redis.call('set', 's', 1)", b"0"], b"-ERR Write commands are not allowed from read-only scripts. script: 395feeea16413e08ec9b066b6d5634c40964a5c8, on @user_script:2.\r\n"),
    (&[b"EVAL", b"#!lua flags=no-writes,allow-oom\nreturn 2", b"0"], b":2\r\n"),
    (&[b"EVAL", b"#!lua flags=bogus\nreturn 1", b"0"], b"-ERR Unexpected flag in script shebang: bogus\r\n"),
    (&[b"EVAL", b"#!lua x=y\nreturn 1", b"0"], b"-ERR Unknown lua shebang option: x=y\r\n"),
    (&[b"EVAL", b"#!python\nreturn 1", b"0"], b"-ERR Unexpected engine in script shebang: #!python\r\n"),
    (&[b"EVAL", b"#!lua", b"0"], b"-ERR Invalid script shebang\r\n"),
    (&[b"EVAL", b"#!lua flags=\nreturn 3", b"0"], b":3\r\n"),
    (&[b"EVAL", b"#!lua\nerror('line 2')", b"0"], b"-ERR user_script:2: line 2 script: 75dc4a505429a92ebf98441c58ee62ceb5fdced8, on @user_script:2.\r\n"),
    (&[b"EVAL", b"#!lua flags=allow-cross-slot-keys,no-cluster,allow-stale\nreturn 4", b"0"], b":4\r\n"),
    (&[b"EVAL_RO", b"return redis.call('get', 's')", b"0"], b"$1\r\nv\r\n"),
    (&[b"EVAL_RO", b"return redis.call('set', 's', 2)", b"0"], b"-ERR Write commands are not allowed from read-only scripts. script: d671d98888ba8598f782c79aeacb0e43b4c70d2c, on @user_script:1.\r\n"),
    (&[b"EVAL_RO", b"return redis.call('publish', 'ch', 'm')", b"0"], b"-ERR Write commands are not allowed from read-only scripts. script: 1b529a3c145ed594072c3c2219b2f093339e63cc, on @user_script:1.\r\n"),
    (&[b"EVAL_RO", b"#!lua\nreturn 1", b"0"], b"-ERR Can not execute a script with write flag using *_ro command.\r\n"),
    (&[b"EVAL_RO", b"#!lua flags=no-writes\nreturn 1", b"0"], b":1\r\n"),
    (&[b"EVAL_RO", b"return redis.pcall('del', 's')", b"0"], b"-ERR Write commands are not allowed from read-only scripts.\r\n"),
    (&[b"SCRIPT", b"LOAD", b"return 'loaded'"], b"$40\r\nb534286061d4b9e4026607613b95c06c06015ae8\r\n"),
    (&[b"EVALSHA", b"a0f6c3c1b8b9c0bbcde2b4e1e0c0b8e9a4c4b1a5", b"0"], b"-NOSCRIPT No matching script. Please use EVAL.\r\n"),
    (&[b"SCRIPT", b"LOAD", b"return 'x'"], b"$40\r\n573cd020e2fc941d149285df8b681959190edd09\r\n"),
    (&[b"EVALSHA", b"e0e1f9fabfc9d4800c877a703b823ac0578ff8db", b"0"], b":1\r\n"),
    (&[b"EVALSHA", b"E0E1F9FABFC9D4800C877A703B823AC0578FF8DB", b"0"], b":1\r\n"),
    (&[b"EVALSHA", b"abc", b"0"], b"-NOSCRIPT No matching script. Please use EVAL.\r\n"),
    (&[b"EVALSHA", b"e0e1f9fabfc9d4800c877a703b823ac0578ff8db", b"x"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"EVALSHA", b"e0e1f9fabfc9d4800c877a703b823ac0578ff8db", b"5"], b"-ERR Number of keys can't be greater than number of args\r\n"),
    (&[b"EVALSHA_RO", b"e0e1f9fabfc9d4800c877a703b823ac0578ff8db", b"0"], b":1\r\n"),
    (&[b"SCRIPT", b"EXISTS", b"e0e1f9fabfc9d4800c877a703b823ac0578ff8db", b"nope", b"E0E1F9FABFC9D4800C877A703B823AC0578FF8DB"], b"*3\r\n:1\r\n:0\r\n:1\r\n"),
    (&[b"SCRIPT", b"EXISTS"], b"-ERR wrong number of arguments for 'script|exists' command\r\n"),
    (&[b"SCRIPT", b"LOAD"], b"-ERR wrong number of arguments for 'script|load' command\r\n"),
    (&[b"SCRIPT", b"LOAD", b"return +"], b"-ERR Error compiling script (new function): user_script:1: unexpected symbol near '+'\r\n"),
    (&[b"SCRIPT", b"LOAD", b"#!lua flags=bad\nreturn 1"], b"-ERR Unexpected flag in script shebang: bad\r\n"),
    (&[b"SCRIPT", b"LOAD", b"return 'x'", b"y"], b"-ERR wrong number of arguments for 'script|load' command\r\n"),
    (&[b"SCRIPT", b"KILL"], b"-NOTBUSY No scripts in execution right now.\r\n"),
    (&[b"SCRIPT", b"KILL", b"x"], b"-ERR wrong number of arguments for 'script|kill' command\r\n"),
    (&[b"SCRIPT", b"HELP"], b"*17\r\n+SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:\r\n+DEBUG (YES|SYNC|NO)\r\n+    Set the debug mode for subsequent scripts executed.\r\n+EXISTS <sha1> [<sha1> ...]\r\n+    Return information about the existence of the scripts in the script cache.\r\n+FLUSH [ASYNC|SYNC]\r\n+    Flush the Lua scripts cache. Very dangerous on replicas.\r\n+    When called without the optional mode argument, the behavior is determined by the\r\n+    lazyfree-lazy-user-flush configuration directive. Valid modes are:\r\n+    * ASYNC: Asynchronously flush the scripts cache.\r\n+    * SYNC: Synchronously flush the scripts cache.\r\n+KILL\r\n+    Kill the currently executing Lua script.\r\n+LOAD <script>\r\n+    Load a script into the scripts cache without executing it.\r\n+HELP\r\n+    Print this help.\r\n"),
    (&[b"SCRIPT", b"help", b"x"], b"-ERR wrong number of arguments for 'script|help' command\r\n"),
    (&[b"SCRIPT", b"FLUSH", b"BOGUS"], b"-ERR SCRIPT FLUSH only support SYNC|ASYNC option\r\n"),
    (&[b"SCRIPT", b"FLUSH", b"SYNC", b"x"], b"-ERR SCRIPT FLUSH only support SYNC|ASYNC option\r\n"),
    (&[b"SCRIPT", b"BOGUS"], b"-ERR unknown subcommand 'BOGUS'. Try SCRIPT HELP.\r\n"),
    (&[b"SCRIPT"], b"-ERR wrong number of arguments for 'script' command\r\n"),
    (&[b"SCRIPT", b"DEBUG", b"NO"], b"+OK\r\n"),
    (&[b"SCRIPT", b"DEBUG", b"MAYBE"], b"-ERR Use SCRIPT DEBUG YES/SYNC/NO\r\n"),
    (&[b"SCRIPT", b"FLUSH", b"ASYNC"], b"+OK\r\n"),
    (&[b"SCRIPT", b"EXISTS", b"e0e1f9fabfc9d4800c877a703b823ac0578ff8db"], b"*1\r\n:0\r\n"),
    (&[b"EVALSHA", b"e0e1f9fabfc9d4800c877a703b823ac0578ff8db", b"0"], b"-NOSCRIPT No matching script. Please use EVAL.\r\n"),
    (&[b"SCRIPT", b"FLUSH"], b"+OK\r\n"),
    (&[b"MULTI"], b"+OK\r\n"),
    (&[b"EVAL", b"return redis.call('incr', 'tx')", b"0"], b"+QUEUED\r\n"),
    (&[b"EVALSHA", b"nope", b"0"], b"+QUEUED\r\n"),
    (&[b"EVAL", b"return 1", b"x"], b"+QUEUED\r\n"),
    (&[b"EXEC"], b"*3\r\n:1\r\n-NOSCRIPT No matching script. Please use EVAL.\r\n-ERR value is not an integer or out of range\r\n"),
    (&[b"MULTI"], b"+OK\r\n"),
    (&[b"EVAL", b"redis.call('set', 'txk', 'a') return redis.call('get', 'txk')", b"0"], b"+QUEUED\r\n"),
    (&[b"EXEC"], b"*1\r\n$1\r\na\r\n"),
    (&[b"EVAL", b"local s = '' for i = 1, 100 do s = s .. i end return #s", b"0"], b":192\r\n"),
    (&[b"EVAL", b"local t = {} for i = 1, 1000 do t[i] = i end return #t", b"0"], b":1000\r\n"),
    (&[b"EVAL", b"local t = {} for i = 1, 300 do t[i] = redis.call('incr', 'counter') end return t[300]", b"0"], b":300\r\n"),
    (&[b"EVAL", b"return redis.call('get', 'counter')", b"0"], b"$3\r\n300\r\n"),
    (&[b"EVAL", b"return {1, 2, 3, n=4}", b"0"], b"*3\r\n:1\r\n:2\r\n:3\r\n"),
    (&[b"EVAL", b"return {[1]=1, [2]=2, [4]=4}", b"0"], b"*2\r\n:1\r\n:2\r\n"),
    (&[b"EVAL", b"return {a=1}", b"0"], b"*0\r\n"),
    (&[b"EVAL", b"return {0.5, 1.5, -2.5}", b"0"], b"*3\r\n:0\r\n:1\r\n:-2\r\n"),
    (&[b"EVAL", b"return 9007199254740993", b"0"], b":9007199254740992\r\n"),
    (&[b"EVAL", b"return 2^62", b"0"], b":4611686018427387904\r\n"),
    (&[b"EVAL", b"return -2^62", b"0"], b":-4611686018427387904\r\n"),
    (&[b"EVAL", b"return redis.call('set', 'k\\0z', 'v\\0z')", b"0"], b"+OK\r\n"),
    (&[b"EVAL", b"return redis.call('get', 'k\\0z')", b"0"], b"$3\r\nv\x00z\r\n"),
    (&[b"EVAL", b"return string.char(255, 0, 128)", b"0"], b"$3\r\n\xff\x00\x80\r\n"),
];

#[tokio::test]
async fn replies_match_redis() {
    common::replay(SCRIPT).await;
}

/// Scripts move amounts between two counters while other scripts read
/// both: no reader may see one moved without the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scripts_are_atomic() {
    const MOVE: &str = "redis.call('decrby', KEYS[1], ARGV[1]) \
                        redis.call('incrby', KEYS[2], ARGV[1])";
    const READ: &str = "return {redis.call('get', KEYS[1]), redis.call('get', KEYS[2])}";
    for mode in MODES {
        let (addr, _ctx) = start(mode).await;
        let mut setup = Client::connect(addr).await;
        setup.call(&["MSET", "a", "1000", "b", "1000"]).await;
        let stop = Arc::new(AtomicBool::new(false));
        let mut writers = Vec::new();
        for _ in 0..4 {
            writers.push(tokio::spawn(async move {
                let mut c = Client::connect(addr).await;
                for _ in 0..300 {
                    c.call(&["EVAL", MOVE, "2", "a", "b", "3"]).await;
                }
            }));
        }
        let mut readers = Vec::new();
        for _ in 0..2 {
            let stop = stop.clone();
            readers.push(tokio::spawn(async move {
                let mut c = Client::connect(addr).await;
                let mut reads = 0;
                while !stop.load(Ordering::Relaxed) {
                    let reply = c.call(&["EVAL", READ, "2", "a", "b"]).await;
                    let sum: i64 = reply
                        .split("\r\n")
                        .filter(|l| !l.is_empty() && !l.starts_with(['*', '$']))
                        .map(|l| l.parse::<i64>().unwrap())
                        .sum();
                    assert_eq!(sum, 2000, "{mode:?}: {reply:?}");
                    reads += 1;
                }
                reads
            }));
        }
        for w in writers {
            w.await.unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        for r in readers {
            assert!(r.await.unwrap() > 0);
        }
        let left = (1000 - 4 * 300 * 3).to_string();
        assert_eq!(
            setup.call(&["GET", "a"]).await,
            format!("${}\r\n{left}\r\n", left.len()),
            "{mode:?}"
        );
    }
}

/// A read-modify-write script run by many clients at once loses nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_scripts_lose_no_updates() {
    const BUMP: &str = "local v = tonumber(redis.call('get', KEYS[1]) or '0') \
                        redis.call('set', KEYS[1], v + 1) return v + 1";
    for mode in MODES {
        let (addr, _ctx) = start(mode).await;
        let mut clients = Vec::new();
        for _ in 0..6 {
            clients.push(tokio::spawn(async move {
                let mut c = Client::connect(addr).await;
                for _ in 0..200 {
                    c.call(&["EVAL", BUMP, "1", "n"]).await;
                }
            }));
        }
        for c in clients {
            c.await.unwrap();
        }
        let mut c = Client::connect(addr).await;
        assert_eq!(c.call(&["GET", "n"]).await, "$4\r\n1200\r\n", "{mode:?}");
    }
}

/// `SCRIPT KILL` from another connection stops a busy script, but not one
/// that wrote. The killer must be served on another thread than the busy
/// script: per-thread I/O deals connections out in turn, so the second one
/// is. (The shared runtime may hold it behind the busy script, and io_uring
/// threads take connections as the kernel spreads them.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn script_kill_stops_a_busy_script() {
    for mode in [Mode::PerThread] {
        let (addr, _ctx) = start(mode).await;
        let mut busy = Client::connect(addr).await;
        let mut admin = Client::connect(addr).await;
        assert_eq!(
            admin.call(&["SCRIPT", "KILL"]).await,
            "-NOTBUSY No scripts in execution right now.\r\n"
        );
        busy.send(&["EVAL", "while true do end", "0"]).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(admin.call(&["SCRIPT", "KILL"]).await, "+OK\r\n", "{mode:?}");
        assert_eq!(
            busy.reply().await,
            "-ERR Script killed by user with SCRIPT KILL... script: \
             694a5fe1ddb97a4c6a1bf299d9537c7d3d0f84e7, on @user_script:1.\r\n",
            "{mode:?}"
        );
        assert_eq!(busy.call(&["PING"]).await, "+PONG\r\n");
        // Once it wrote, it runs to the end.
        let writes_then_spins = "redis.call('set', 'w', 1) \
            local t = redis.call('time') local start = t[1] * 1000000 + t[2] \
            repeat t = redis.call('time') until t[1] * 1000000 + t[2] - start > 500000 \
            return 'done'";
        busy.send(&["EVAL", writes_then_spins, "0"]).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            admin
                .call(&["SCRIPT", "KILL"])
                .await
                .starts_with("-UNKILLABLE Sorry the script already executed write commands"),
            "{mode:?}"
        );
        assert_eq!(busy.reply().await, "$4\r\ndone\r\n");
    }
}

/// Scripts inside `EXEC` (which holds the whole store already) while other
/// clients run scripts: nothing deadlocks, and every increment lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scripts_inside_transactions() {
    const INCR: &str = "return redis.call('incr', KEYS[1])";
    for mode in MODES {
        let (addr, _ctx) = start(mode).await;
        let mut clients = Vec::new();
        for i in 0..6 {
            clients.push(tokio::spawn(async move {
                let mut c = Client::connect(addr).await;
                for _ in 0..100 {
                    if i % 2 == 0 {
                        c.call(&["EVAL", INCR, "1", "m"]).await;
                    } else {
                        c.call(&["MULTI"]).await;
                        c.call(&["EVAL", INCR, "1", "m"]).await;
                        c.call(&["INCR", "m"]).await;
                        let exec = c.call(&["EXEC"]).await;
                        assert!(exec.starts_with("*2\r\n:"), "{exec:?}");
                    }
                }
            }));
        }
        for c in clients {
            tokio::time::timeout(Duration::from_secs(30), c)
                .await
                .expect("deadlocked")
                .unwrap();
        }
        let mut c = Client::connect(addr).await;
        assert_eq!(c.call(&["GET", "m"]).await, "$3\r\n900\r\n", "{mode:?}");
    }
}

/// What a script writes wakes blocked clients and reaches subscribers, as a
/// client's commands would.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scripts_wake_clients_and_publish() {
    for mode in MODES {
        let (addr, _ctx) = start(mode).await;
        let mut waiter = Client::connect(addr).await;
        let mut listener = Client::connect(addr).await;
        let mut script = Client::connect(addr).await;
        waiter.send(&["BLPOP", "q", "0"]).await;
        listener.call(&["SUBSCRIBE", "ch"]).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.has_reply().await);
        let pushed = "redis.call('rpush', KEYS[1], 'x') \
                      return redis.call('publish', 'ch', 'from a script')";
        assert_eq!(script.call(&["EVAL", pushed, "1", "q"]).await, ":1\r\n");
        assert_eq!(
            waiter.reply().await,
            "*2\r\n$1\r\nq\r\n$1\r\nx\r\n",
            "{mode:?}"
        );
        assert_eq!(
            listener.reply().await,
            "*3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$13\r\nfrom a script\r\n"
        );
    }
}
