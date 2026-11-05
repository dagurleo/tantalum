//! String, generic and hash commands added in 0.20.0, over the wire, against
//! replies recorded from a real Redis (7.2.4 on x86-64, with
//! `tools/redis-diff.py --rust`), errors included.

mod common;

use std::time::Duration;

use common::{Mode, check, connect, connect_buffered, replay, req, start};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// Commands and Redis' replies, in order, on one connection.
#[rustfmt::skip]
const SCRIPT: &[(&[&[u8]], &[u8])] = &[
    (&[b"RANDOMKEY"], b"$-1\r\n"),
    (&[b"SET", b"only", b"v"], b"+OK\r\n"),
    (&[b"RANDOMKEY"], b"$4\r\nonly\r\n"),
    (&[b"RANDOMKEY", b"x"], b"-ERR wrong number of arguments for 'randomkey' command\r\n"),
    (&[b"FLUSHALL"], b"+OK\r\n"),
    (&[b"SET", b"s", b"hello"], b"+OK\r\n"),
    (&[b"STRLEN", b"s"], b":5\r\n"),
    (&[b"STRLEN", b"missing"], b":0\r\n"),
    (&[b"RPUSH", b"l", b"a"], b":1\r\n"),
    (&[b"STRLEN", b"l"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SET", b"n", b"12345"], b"+OK\r\n"),
    (&[b"STRLEN", b"n"], b":5\r\n"),
    (&[b"SET", b"e", b""], b"+OK\r\n"),
    (&[b"STRLEN", b"e"], b":0\r\n"),
    (&[b"STRLEN"], b"-ERR wrong number of arguments for 'strlen' command\r\n"),
    (&[b"STRLEN", b"a", b"b"], b"-ERR wrong number of arguments for 'strlen' command\r\n"),
    (&[b"APPEND", b"ap", b"hello"], b":5\r\n"),
    (&[b"APPEND", b"ap", b" world"], b":11\r\n"),
    (&[b"GET", b"ap"], b"$11\r\nhello world\r\n"),
    (&[b"APPEND", b"l", b"x"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SET", b"ttl", b"v", b"EX", b"100"], b"+OK\r\n"),
    (&[b"APPEND", b"ttl", b"more"], b":5\r\n"),
    (&[b"TTL", b"ttl"], b":100\r\n"),
    (&[b"GET", b"ttl"], b"$5\r\nvmore\r\n"),
    (&[b"APPEND", b"empty", b""], b":0\r\n"),
    (&[b"EXISTS", b"empty"], b":1\r\n"),
    (&[b"STRLEN", b"empty"], b":0\r\n"),
    (&[b"TYPE", b"empty"], b"+string\r\n"),
    (&[b"APPEND"], b"-ERR wrong number of arguments for 'append' command\r\n"),
    (&[b"APPEND", b"a"], b"-ERR wrong number of arguments for 'append' command\r\n"),
    (&[b"APPEND", b"a", b"b", b"c"], b"-ERR wrong number of arguments for 'append' command\r\n"),
    (&[b"SET", b"num", b"10"], b"+OK\r\n"),
    (&[b"APPEND", b"num", b"5"], b":3\r\n"),
    (&[b"GET", b"num"], b"$3\r\n105\r\n"),
    (&[b"INCR", b"num"], b":106\r\n"),
    (&[b"SET", b"g", b"This is a string"], b"+OK\r\n"),
    (&[b"GETRANGE", b"g", b"0", b"3"], b"$4\r\nThis\r\n"),
    (&[b"GETRANGE", b"g", b"-3", b"-1"], b"$3\r\ning\r\n"),
    (&[b"GETRANGE", b"g", b"0", b"-1"], b"$16\r\nThis is a string\r\n"),
    (&[b"GETRANGE", b"g", b"10", b"100"], b"$6\r\nstring\r\n"),
    (&[b"GETRANGE", b"g", b"5", b"3"], b"$0\r\n\r\n"),
    (&[b"GETRANGE", b"g", b"-100", b"-50"], b"$1\r\nT\r\n"),
    (&[b"GETRANGE", b"g", b"-1", b"-100"], b"$0\r\n\r\n"),
    (&[b"GETRANGE", b"g", b"-5", b"-100"], b"$0\r\n\r\n"),
    (&[b"GETRANGE", b"g", b"100", b"200"], b"$0\r\n\r\n"),
    (&[b"GETRANGE", b"g", b"0", b"0"], b"$1\r\nT\r\n"),
    (&[b"GETRANGE", b"g", b"-1", b"-1"], b"$1\r\ng\r\n"),
    (&[b"GETRANGE", b"g", b"16", b"16"], b"$0\r\n\r\n"),
    (&[b"GETRANGE", b"g", b"15", b"15"], b"$1\r\ng\r\n"),
    (&[b"GETRANGE", b"g", b"-16", b"-16"], b"$1\r\nT\r\n"),
    (&[b"GETRANGE", b"g", b"-17", b"-17"], b"$1\r\nT\r\n"),
    (&[b"GETRANGE", b"g", b"-17", b"0"], b"$1\r\nT\r\n"),
    (&[b"GETRANGE", b"missing", b"0", b"-1"], b"$0\r\n\r\n"),
    (&[b"GETRANGE", b"l", b"0", b"1"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"GETRANGE", b"g", b"a", b"1"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"GETRANGE", b"g", b"1", b"b"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"GETRANGE", b"g", b"1.5", b"2"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"GETRANGE", b"g", b"0", b"9223372036854775807"], b"$16\r\nThis is a string\r\n"),
    (&[b"GETRANGE", b"g", b"9223372036854775807", b"9223372036854775807"], b"$0\r\n\r\n"),
    (&[b"GETRANGE", b"g", b"-9223372036854775808", b"-1"], b"$16\r\nThis is a string\r\n"),
    (&[b"GETRANGE", b"g", b"-9223372036854775808", b"-9223372036854775808"], b"$1\r\nT\r\n"),
    (&[b"GETRANGE", b"g", b"9223372036854775808", b"1"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"GETRANGE", b"g"], b"-ERR wrong number of arguments for 'getrange' command\r\n"),
    (&[b"GETRANGE", b"g", b"0"], b"-ERR wrong number of arguments for 'getrange' command\r\n"),
    (&[b"GETRANGE", b"g", b"0", b"1", b"2"], b"-ERR wrong number of arguments for 'getrange' command\r\n"),
    (&[b"SET", b"emp", b""], b"+OK\r\n"),
    (&[b"GETRANGE", b"emp", b"0", b"-1"], b"$0\r\n\r\n"),
    (&[b"GETRANGE", b"emp", b"0", b"0"], b"$0\r\n\r\n"),
    (&[b"GETRANGE", b"emp", b"-1", b"-1"], b"$0\r\n\r\n"),
    (&[b"SET", b"gn", b"12345"], b"+OK\r\n"),
    (&[b"GETRANGE", b"gn", b"1", b"2"], b"$2\r\n23\r\n"),
    (&[b"SUBSTR", b"g", b"0", b"3"], b"$4\r\nThis\r\n"),
    (&[b"SUBSTR", b"g", b"-3", b"-1"], b"$3\r\ning\r\n"),
    (&[b"SUBSTR", b"missing", b"0", b"1"], b"$0\r\n\r\n"),
    (&[b"SUBSTR", b"l", b"0", b"1"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SUBSTR", b"g", b"0"], b"-ERR wrong number of arguments for 'substr' command\r\n"),
    (&[b"SET", b"r", b"Hello World"], b"+OK\r\n"),
    (&[b"SETRANGE", b"r", b"6", b"Redis"], b":11\r\n"),
    (&[b"GET", b"r"], b"$11\r\nHello Redis\r\n"),
    (&[b"SETRANGE", b"nr", b"5", b"Redis"], b":10\r\n"),
    (&[b"GET", b"nr"], b"$10\r\n\x00\x00\x00\x00\x00Redis\r\n"),
    (&[b"SETRANGE", b"nr", b"0", b""], b":10\r\n"),
    (&[b"SETRANGE", b"gone", b"0", b""], b":0\r\n"),
    (&[b"EXISTS", b"gone"], b":0\r\n"),
    (&[b"SETRANGE", b"gone2", b"100", b""], b":0\r\n"),
    (&[b"EXISTS", b"gone2"], b":0\r\n"),
    (&[b"SETRANGE", b"r", b"100", b""], b":11\r\n"),
    (&[b"STRLEN", b"r"], b":11\r\n"),
    (&[b"SETRANGE", b"r", b"-1", b"x"], b"-ERR offset is out of range\r\n"),
    (&[b"SETRANGE", b"r", b"-1", b""], b"-ERR offset is out of range\r\n"),
    (&[b"SETRANGE", b"r", b"a", b"x"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"SETRANGE", b"r", b"1.5", b"x"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"SETRANGE", b"l", b"0", b"x"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SETRANGE", b"l", b"0", b""], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SETRANGE", b"r", b"536870912", b"x"], b"-ERR string exceeds maximum allowed size (proto-max-bulk-len)\r\n"),
    (&[b"SETRANGE", b"r", b"536870911", b"xx"], b"-ERR string exceeds maximum allowed size (proto-max-bulk-len)\r\n"),
    (&[b"SETRANGE", b"fresh", b"536870912", b"x"], b"-ERR string exceeds maximum allowed size (proto-max-bulk-len)\r\n"),
    (&[b"SETRANGE", b"r", b"9223372036854775807", b"x"], b"-ERR string exceeds maximum allowed size (proto-max-bulk-len)\r\n"),
    (&[b"SETRANGE", b"r", b"9223372036854775808", b"x"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"SETRANGE", b"ttl", b"0", b"x"], b":5\r\n"),
    (&[b"TTL", b"ttl"], b":100\r\n"),
    (&[b"GET", b"ttl"], b"$5\r\nxmore\r\n"),
    (&[b"SETRANGE", b"r", b"2", b""], b":11\r\n"),
    (&[b"SETRANGE", b"r"], b"-ERR wrong number of arguments for 'setrange' command\r\n"),
    (&[b"SETRANGE", b"r", b"0"], b"-ERR wrong number of arguments for 'setrange' command\r\n"),
    (&[b"SETRANGE", b"gn", b"1", b"xx"], b":5\r\n"),
    (&[b"GET", b"gn"], b"$5\r\n1xx45\r\n"),
    (&[b"SETRANGE", b"r", b"20", b"\x00\xffbin"], b":25\r\n"),
    (&[b"GET", b"r"], b"$25\r\nHello Redis\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xffbin\r\n"),
    (&[b"GETSET", b"gs", b"a"], b"$-1\r\n"),
    (&[b"GETSET", b"gs", b"b"], b"$1\r\na\r\n"),
    (&[b"GET", b"gs"], b"$1\r\nb\r\n"),
    (&[b"SET", b"gt", b"v", b"EX", b"100"], b"+OK\r\n"),
    (&[b"GETSET", b"gt", b"w"], b"$1\r\nv\r\n"),
    (&[b"TTL", b"gt"], b":-1\r\n"),
    (&[b"GETSET", b"l", b"x"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"LLEN", b"l"], b":1\r\n"),
    (&[b"GETSET", b"gs"], b"-ERR wrong number of arguments for 'getset' command\r\n"),
    (&[b"GETSET", b"gs", b"a", b"b"], b"-ERR wrong number of arguments for 'getset' command\r\n"),
    (&[b"MSETNX", b"a1", b"1", b"a2", b"2"], b":1\r\n"),
    (&[b"MGET", b"a1", b"a2"], b"*2\r\n$1\r\n1\r\n$1\r\n2\r\n"),
    (&[b"MSETNX", b"a1", b"9", b"a3", b"3"], b":0\r\n"),
    (&[b"EXISTS", b"a3"], b":0\r\n"),
    (&[b"GET", b"a1"], b"$1\r\n1\r\n"),
    (&[b"MSETNX", b"a4", b"1", b"a4", b"2"], b":1\r\n"),
    (&[b"GET", b"a4"], b"$1\r\n2\r\n"),
    (&[b"MSETNX", b"a5"], b"-ERR wrong number of arguments for 'msetnx' command\r\n"),
    (&[b"MSETNX", b"a5", b"1", b"a6"], b"-ERR wrong number of arguments for 'msetnx' command\r\n"),
    (&[b"MSETNX"], b"-ERR wrong number of arguments for 'msetnx' command\r\n"),
    (&[b"MSETNX", b"l", b"1"], b":0\r\n"),
    (&[b"LLEN", b"l"], b":1\r\n"),
    (&[b"SET", b"mt", b"v", b"EX", b"100"], b"+OK\r\n"),
    (&[b"MSETNX", b"mt", b"x"], b":0\r\n"),
    (&[b"TTL", b"mt"], b":100\r\n"),
    (&[b"HSET", b"one", b"f", b"v"], b":1\r\n"),
    (&[b"HRANDFIELD", b"one"], b"$1\r\nf\r\n"),
    (&[b"HRANDFIELD", b"one", b"1"], b"*1\r\n$1\r\nf\r\n"),
    (&[b"HRANDFIELD", b"one", b"1", b"WITHVALUES"], b"*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"HRANDFIELD", b"one", b"-3"], b"*3\r\n$1\r\nf\r\n$1\r\nf\r\n$1\r\nf\r\n"),
    (&[b"HRANDFIELD", b"one", b"-2", b"WITHVALUES"], b"*4\r\n$1\r\nf\r\n$1\r\nv\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"HRANDFIELD", b"one", b"5"], b"*1\r\n$1\r\nf\r\n"),
    (&[b"HRANDFIELD", b"one", b"5", b"WITHVALUES"], b"*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"HRANDFIELD", b"one", b"0"], b"*0\r\n"),
    (&[b"HRANDFIELD", b"one", b"0", b"WITHVALUES"], b"*0\r\n"),
    (&[b"HRANDFIELD", b"missing"], b"$-1\r\n"),
    (&[b"HRANDFIELD", b"missing", b"5"], b"*0\r\n"),
    (&[b"HRANDFIELD", b"missing", b"5", b"WITHVALUES"], b"*0\r\n"),
    (&[b"HRANDFIELD", b"s"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"HRANDFIELD", b"s", b"1"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"HRANDFIELD", b"one", b"a"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"HRANDFIELD", b"one", b"1", b"x"], b"-ERR syntax error\r\n"),
    (&[b"HRANDFIELD", b"one", b"1", b"WITHVALUES", b"x"], b"-ERR syntax error\r\n"),
    (&[b"HRANDFIELD", b"one", b"9223372036854775807", b"WITHVALUES"], b"-ERR value is out of range\r\n"),
    (&[b"HRANDFIELD", b"one", b"-9223372036854775807", b"WITHVALUES"], b"-ERR value is out of range\r\n"),
    (&[b"HRANDFIELD", b"one", b"-9223372036854775808"], b"-ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807\r\n"),
    (&[b"HRANDFIELD", b"one", b"-9223372036854775808", b"WITHVALUES"], b"-ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807\r\n"),
    (&[b"HRANDFIELD", b"one", b"1.5"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"HRANDFIELD"], b"-ERR wrong number of arguments for 'hrandfield' command\r\n"),
    (&[b"HRANDFIELD", b"one"], b"$1\r\nf\r\n"),
    (&[b"HRANDFIELD", b"missing"], b"$-1\r\n"),
    (&[b"HRANDFIELD", b"one", b"1"], b"*1\r\n$1\r\nf\r\n"),
    (&[b"HRANDFIELD", b"one", b"1", b"WITHVALUES"], b"*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"HRANDFIELD", b"one", b"-2", b"WITHVALUES"], b"*4\r\n$1\r\nf\r\n$1\r\nv\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"HRANDFIELD", b"one", b"0", b"WITHVALUES"], b"*0\r\n"),
    (&[b"SET", b"k1", b"ohmytext"], b"+OK\r\n"),
    (&[b"SET", b"k2", b"mynewtext"], b"+OK\r\n"),
    (&[b"LCS", b"k1", b"k2"], b"$6\r\nmytext\r\n"),
    (&[b"LCS", b"k1", b"k2", b"LEN"], b":6\r\n"),
    (&[b"LCS", b"k1", b"k2", b"IDX"], b"*4\r\n$7\r\nmatches\r\n*2\r\n*2\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n*2\r\n*2\r\n:2\r\n:3\r\n*2\r\n:0\r\n:1\r\n$3\r\nlen\r\n:6\r\n"),
    (&[b"LCS", b"k1", b"k2", b"IDX", b"MINMATCHLEN", b"4"], b"*4\r\n$7\r\nmatches\r\n*1\r\n*2\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n$3\r\nlen\r\n:6\r\n"),
    (&[b"LCS", b"k1", b"k2", b"IDX", b"WITHMATCHLEN"], b"*4\r\n$7\r\nmatches\r\n*2\r\n*3\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n:4\r\n*3\r\n*2\r\n:2\r\n:3\r\n*2\r\n:0\r\n:1\r\n:2\r\n$3\r\nlen\r\n:6\r\n"),
    (&[b"LCS", b"k1", b"k2", b"IDX", b"MINMATCHLEN", b"4", b"WITHMATCHLEN"], b"*4\r\n$7\r\nmatches\r\n*1\r\n*3\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n:4\r\n$3\r\nlen\r\n:6\r\n"),
    (&[b"LCS", b"k1", b"k2", b"IDX", b"MINMATCHLEN", b"-1"], b"*4\r\n$7\r\nmatches\r\n*2\r\n*2\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n*2\r\n*2\r\n:2\r\n:3\r\n*2\r\n:0\r\n:1\r\n$3\r\nlen\r\n:6\r\n"),
    (&[b"LCS", b"k1", b"k2", b"idx", b"withmatchlen", b"minmatchlen", b"2"], b"*4\r\n$7\r\nmatches\r\n*2\r\n*3\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n:4\r\n*3\r\n*2\r\n:2\r\n:3\r\n*2\r\n:0\r\n:1\r\n:2\r\n$3\r\nlen\r\n:6\r\n"),
    (&[b"LCS", b"k1", b"k2", b"LEN", b"IDX"], b"-ERR If you want both the length and indexes, please just use IDX.\r\n"),
    (&[b"LCS", b"k1", b"k2", b"LEN", b"MINMATCHLEN", b"3"], b":6\r\n"),
    (&[b"LCS", b"k1", b"k2", b"WITHMATCHLEN"], b"$6\r\nmytext\r\n"),
    (&[b"LCS", b"k1", b"missing"], b"$0\r\n\r\n"),
    (&[b"LCS", b"k1", b"missing", b"LEN"], b":0\r\n"),
    (&[b"LCS", b"k1", b"missing", b"IDX"], b"*4\r\n$7\r\nmatches\r\n*0\r\n$3\r\nlen\r\n:0\r\n"),
    (&[b"LCS", b"missing", b"missing2"], b"$0\r\n\r\n"),
    (&[b"LCS", b"l", b"k1"], b"-ERR The specified keys must contain string values\r\n"),
    (&[b"LCS", b"k1", b"l"], b"-ERR The specified keys must contain string values\r\n"),
    (&[b"LCS", b"k1", b"k2", b"foo"], b"-ERR syntax error\r\n"),
    (&[b"LCS", b"k1", b"k2", b"MINMATCHLEN"], b"-ERR syntax error\r\n"),
    (&[b"LCS", b"k1", b"k2", b"MINMATCHLEN", b"x"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"LCS", b"k1", b"k1", b"IDX"], b"*4\r\n$7\r\nmatches\r\n*1\r\n*2\r\n*2\r\n:0\r\n:7\r\n*2\r\n:0\r\n:7\r\n$3\r\nlen\r\n:8\r\n"),
    (&[b"LCS", b"k1"], b"-ERR wrong number of arguments for 'lcs' command\r\n"),
    (&[b"SET", b"k3", b"aXbXcXd"], b"+OK\r\n"),
    (&[b"SET", b"k4", b"YaYbYcYd"], b"+OK\r\n"),
    (&[b"LCS", b"k3", b"k4", b"IDX", b"WITHMATCHLEN"], b"*4\r\n$7\r\nmatches\r\n*4\r\n*3\r\n*2\r\n:6\r\n:6\r\n*2\r\n:7\r\n:7\r\n:1\r\n*3\r\n*2\r\n:4\r\n:4\r\n*2\r\n:5\r\n:5\r\n:1\r\n*3\r\n*2\r\n:2\r\n:2\r\n*2\r\n:3\r\n:3\r\n:1\r\n*3\r\n*2\r\n:0\r\n:0\r\n*2\r\n:1\r\n:1\r\n:1\r\n$3\r\nlen\r\n:4\r\n"),
    (&[b"LCS", b"k4", b"k3", b"IDX"], b"*4\r\n$7\r\nmatches\r\n*4\r\n*2\r\n*2\r\n:7\r\n:7\r\n*2\r\n:6\r\n:6\r\n*2\r\n*2\r\n:5\r\n:5\r\n*2\r\n:4\r\n:4\r\n*2\r\n*2\r\n:3\r\n:3\r\n*2\r\n:2\r\n:2\r\n*2\r\n*2\r\n:1\r\n:1\r\n*2\r\n:0\r\n:0\r\n$3\r\nlen\r\n:4\r\n"),
    (&[b"LCS", b"k1", b"k2", b"IDX", b"WITHMATCHLEN"], b"*4\r\n$7\r\nmatches\r\n*2\r\n*3\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n:4\r\n*3\r\n*2\r\n:2\r\n:3\r\n*2\r\n:0\r\n:1\r\n:2\r\n$3\r\nlen\r\n:6\r\n"),
    (&[b"LCS", b"k1", b"k2", b"LEN"], b":6\r\n"),
    (&[b"TOUCH", b"s", b"g", b"missing"], b":2\r\n"),
    (&[b"TOUCH", b"s", b"s"], b":2\r\n"),
    (&[b"TOUCH", b"missing"], b":0\r\n"),
    (&[b"TOUCH"], b"-ERR wrong number of arguments for 'touch' command\r\n"),
    (&[b"TOUCH", b"l"], b":1\r\n"),
    (&[b"SET", b"src", b"v", b"EX", b"100"], b"+OK\r\n"),
    (&[b"COPY", b"src", b"dst"], b":1\r\n"),
    (&[b"GET", b"dst"], b"$1\r\nv\r\n"),
    (&[b"TTL", b"dst"], b":100\r\n"),
    (&[b"COPY", b"src", b"dst"], b":0\r\n"),
    (&[b"COPY", b"src", b"dst", b"REPLACE"], b":1\r\n"),
    (&[b"COPY", b"src", b"dst", b"replace"], b":1\r\n"),
    (&[b"COPY", b"src", b"src"], b"-ERR source and destination objects are the same\r\n"),
    (&[b"COPY", b"src", b"src", b"REPLACE"], b"-ERR source and destination objects are the same\r\n"),
    (&[b"COPY", b"src", b"src", b"DB", b"0"], b"-ERR source and destination objects are the same\r\n"),
    (&[b"COPY", b"missing", b"d"], b":0\r\n"),
    (&[b"COPY", b"missing", b"dst", b"REPLACE"], b":0\r\n"),
    (&[b"GET", b"dst"], b"$1\r\nv\r\n"),
    (&[b"COPY", b"src", b"d0", b"DB", b"0"], b":1\r\n"),
    (&[b"GET", b"d0"], b"$1\r\nv\r\n"),
    (&[b"COPY", b"src", b"d3", b"DB", b"x"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"COPY", b"src", b"d4", b"FOO"], b"-ERR syntax error\r\n"),
    (&[b"COPY", b"src", b"d5", b"DB"], b"-ERR syntax error\r\n"),
    (&[b"COPY", b"src", b"d6", b"DB", b"0", b"REPLACE"], b":1\r\n"),
    (&[b"COPY", b"src"], b"-ERR wrong number of arguments for 'copy' command\r\n"),
    (&[b"COPY", b"src", b"d7", b"REPLACE", b"DB", b"0"], b":1\r\n"),
    (&[b"SET", b"plain", b"p"], b"+OK\r\n"),
    (&[b"COPY", b"plain", b"dst", b"REPLACE"], b":1\r\n"),
    (&[b"TTL", b"dst"], b":-1\r\n"),
    (&[b"GET", b"dst"], b"$1\r\np\r\n"),
    (&[b"HSET", b"h1", b"a", b"1", b"b", b"2"], b":2\r\n"),
    (&[b"COPY", b"h1", b"h2"], b":1\r\n"),
    (&[b"HGETALL", b"h2"], b"*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n"),
    (&[b"HSET", b"h2", b"c", b"3"], b":1\r\n"),
    (&[b"HLEN", b"h1"], b":2\r\n"),
    (&[b"RPUSH", b"l1", b"a", b"b", b"c"], b":3\r\n"),
    (&[b"COPY", b"l1", b"l2"], b":1\r\n"),
    (&[b"LRANGE", b"l2", b"0", b"-1"], b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"),
    (&[b"RPUSH", b"l2", b"d"], b":4\r\n"),
    (&[b"LLEN", b"l1"], b":3\r\n"),
    (&[b"SADD", b"st1", b"a"], b":1\r\n"),
    (&[b"COPY", b"st1", b"st2"], b":1\r\n"),
    (&[b"SMEMBERS", b"st2"], b"*1\r\n$1\r\na\r\n"),
    (&[b"ZADD", b"z1", b"1", b"a", b"2", b"b"], b":2\r\n"),
    (&[b"COPY", b"z1", b"z2"], b":1\r\n"),
    (&[b"ZRANGE", b"z2", b"0", b"-1", b"WITHSCORES"], b"*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n"),
    (&[b"ZADD", b"z2", b"3", b"c"], b":1\r\n"),
    (&[b"ZCARD", b"z1"], b":2\r\n"),
    (&[b"XADD", b"x1", b"1-1", b"f", b"v"], b"$3\r\n1-1\r\n"),
    (&[b"COPY", b"x1", b"x2"], b":1\r\n"),
    (&[b"XRANGE", b"x2", b"-", b"+"], b"*1\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"XADD", b"x2", b"2-1", b"f", b"v"], b"$3\r\n2-1\r\n"),
    (&[b"XLEN", b"x1"], b":1\r\n"),
    (&[b"COPY", b"l1", b"plain"], b":0\r\n"),
    (&[b"COPY", b"l1", b"plain", b"REPLACE"], b":1\r\n"),
    (&[b"TYPE", b"plain"], b"+list\r\n"),
    (&[b"LRANGE", b"plain", b"0", b"-1"], b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"),
    (&[b"COPY", b"plain", b"h1", b"REPLACE"], b":1\r\n"),
    (&[b"TYPE", b"h1"], b"+list\r\n"),
    (&[b"SET", b"oi", b"123"], b"+OK\r\n"),
    (&[b"OBJECT", b"ENCODING", b"oi"], b"$3\r\nint\r\n"),
    (&[b"SET", b"oim", b"-123"], b"+OK\r\n"),
    (&[b"OBJECT", b"ENCODING", b"oim"], b"$3\r\nint\r\n"),
    (&[b"SET", b"oi0", b"007"], b"+OK\r\n"),
    (&[b"OBJECT", b"ENCODING", b"oi0"], b"$6\r\nembstr\r\n"),
    (&[b"SET", b"oil", b"12345678901234567890"], b"+OK\r\n"),
    (&[b"OBJECT", b"ENCODING", b"oil"], b"$6\r\nembstr\r\n"),
    (&[b"SET", b"oi2", b"9223372036854775807"], b"+OK\r\n"),
    (&[b"OBJECT", b"ENCODING", b"oi2"], b"$3\r\nint\r\n"),
    (&[b"SET", b"oe", b"abc"], b"+OK\r\n"),
    (&[b"OBJECT", b"ENCODING", b"oe"], b"$6\r\nembstr\r\n"),
    (&[b"OBJECT", b"ENCODING", b"missing"], b"$-1\r\n"),
    (&[b"OBJECT", b"REFCOUNT", b"missing"], b"$-1\r\n"),
    (&[b"OBJECT", b"FREQ", b"missing"], b"$-1\r\n"),
    (&[b"OBJECT", b"IDLETIME", b"missing"], b"$-1\r\n"),
    (&[b"OBJECT", b"REFCOUNT", b"oi"], b":2147483647\r\n"),
    (&[b"OBJECT", b"REFCOUNT", b"oe"], b":1\r\n"),
    (&[b"SET", b"large", b"10000"], b"+OK\r\n"),
    (&[b"OBJECT", b"REFCOUNT", b"large"], b":1\r\n"),
    (&[b"OBJECT", b"FREQ", b"oi"], b"-ERR An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.\r\n"),
    (&[b"OBJECT", b"HELP"], b"*15\r\n+OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:\r\n+ENCODING <key>\r\n+    Return the kind of internal representation used in order to store the value\r\n+    associated with a <key>.\r\n+FREQ <key>\r\n+    Return the access frequency index of the <key>. The returned integer is\r\n+    proportional to the logarithm of the recent access frequency of the key.\r\n+IDLETIME <key>\r\n+    Return the idle time of the <key>, that is the approximated number of\r\n+    seconds elapsed since the last access to the key.\r\n+REFCOUNT <key>\r\n+    Return the number of references of the value associated with the specified\r\n+    <key>.\r\n+HELP\r\n+    Print this help.\r\n"),
    (&[b"OBJECT", b"help"], b"*15\r\n+OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:\r\n+ENCODING <key>\r\n+    Return the kind of internal representation used in order to store the value\r\n+    associated with a <key>.\r\n+FREQ <key>\r\n+    Return the access frequency index of the <key>. The returned integer is\r\n+    proportional to the logarithm of the recent access frequency of the key.\r\n+IDLETIME <key>\r\n+    Return the idle time of the <key>, that is the approximated number of\r\n+    seconds elapsed since the last access to the key.\r\n+REFCOUNT <key>\r\n+    Return the number of references of the value associated with the specified\r\n+    <key>.\r\n+HELP\r\n+    Print this help.\r\n"),
    (&[b"OBJECT", b"HELP", b"x"], b"-ERR wrong number of arguments for 'object|help' command\r\n"),
    (&[b"OBJECT", b"FOO"], b"-ERR unknown subcommand 'FOO'. Try OBJECT HELP.\r\n"),
    (&[b"OBJECT", b"FOO", b"bar"], b"-ERR unknown subcommand 'FOO'. Try OBJECT HELP.\r\n"),
    (&[b"OBJECT"], b"-ERR wrong number of arguments for 'object' command\r\n"),
    (&[b"OBJECT", b"ENCODING"], b"-ERR wrong number of arguments for 'object|encoding' command\r\n"),
    (&[b"OBJECT", b"ENCODING", b"a", b"b"], b"-ERR wrong number of arguments for 'object|encoding' command\r\n"),
    (&[b"OBJECT", b"REFCOUNT"], b"-ERR wrong number of arguments for 'object|refcount' command\r\n"),
    (&[b"OBJECT", b"idletime"], b"-ERR wrong number of arguments for 'object|idletime' command\r\n"),
    (&[b"OBJECT", b"encoding", b"oi"], b"$3\r\nint\r\n"),
    (&[b"RPUSH", b"sl", b"3", b"1", b"2", b"10"], b":4\r\n"),
    (&[b"SORT", b"sl"], b"*4\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$2\r\n10\r\n"),
    (&[b"SORT", b"sl", b"DESC"], b"*4\r\n$2\r\n10\r\n$1\r\n3\r\n$1\r\n2\r\n$1\r\n1\r\n"),
    (&[b"SORT", b"sl", b"ASC"], b"*4\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$2\r\n10\r\n"),
    (&[b"SORT", b"sl", b"LIMIT", b"1", b"2"], b"*2\r\n$1\r\n2\r\n$1\r\n3\r\n"),
    (&[b"SORT", b"sl", b"LIMIT", b"0", b"0"], b"*0\r\n"),
    (&[b"SORT", b"sl", b"LIMIT", b"10", b"2"], b"*0\r\n"),
    (&[b"SORT", b"sl", b"LIMIT", b"-1", b"2"], b"*2\r\n$1\r\n1\r\n$1\r\n2\r\n"),
    (&[b"SORT", b"sl", b"LIMIT", b"1", b"-1"], b"*3\r\n$1\r\n2\r\n$1\r\n3\r\n$2\r\n10\r\n"),
    (&[b"SORT", b"sl", b"LIMIT", b"1", b"100"], b"*3\r\n$1\r\n2\r\n$1\r\n3\r\n$2\r\n10\r\n"),
    (&[b"SORT", b"sl", b"DESC", b"LIMIT", b"1", b"2"], b"*2\r\n$1\r\n3\r\n$1\r\n2\r\n"),
    (&[b"SORT", b"sl", b"ALPHA"], b"*4\r\n$1\r\n1\r\n$2\r\n10\r\n$1\r\n2\r\n$1\r\n3\r\n"),
    (&[b"SORT", b"sl", b"ALPHA", b"DESC"], b"*4\r\n$1\r\n3\r\n$1\r\n2\r\n$2\r\n10\r\n$1\r\n1\r\n"),
    (&[b"SORT", b"sl", b"LIMIT", b"1"], b"-ERR syntax error\r\n"),
    (&[b"SORT", b"sl", b"LIMIT", b"a", b"1"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"SORT", b"sl", b"LIMIT", b"1", b"b"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"SORT", b"sl", b"FOO"], b"-ERR syntax error\r\n"),
    (&[b"SORT", b"sl", b"BY"], b"-ERR syntax error\r\n"),
    (&[b"SORT", b"sl", b"GET"], b"-ERR syntax error\r\n"),
    (&[b"SORT", b"sl", b"STORE"], b"-ERR syntax error\r\n"),
    (&[b"SORT", b"missing"], b"*0\r\n"),
    (&[b"SORT", b"missing", b"STORE", b"sd"], b":0\r\n"),
    (&[b"SORT", b"missing", b"ALPHA", b"DESC", b"LIMIT", b"0", b"5"], b"*0\r\n"),
    (&[b"SORT", b"s"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SORT", b"s", b"STORE", b"sd"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SORT"], b"-ERR wrong number of arguments for 'sort' command\r\n"),
    (&[b"RPUSH", b"sa", b"b", b"a", b"c"], b":3\r\n"),
    (&[b"SORT", b"sa"], b"-ERR One or more scores can't be converted into double\r\n"),
    (&[b"SORT", b"sa", b"ALPHA"], b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"),
    (&[b"SORT", b"sa", b"ALPHA", b"DESC"], b"*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n"),
    (&[b"SORT", b"sa", b"BY", b"nosort"], b"*3\r\n$1\r\nb\r\n$1\r\na\r\n$1\r\nc\r\n"),
    (&[b"SORT", b"sa", b"BY", b"nosort", b"DESC"], b"*3\r\n$1\r\nc\r\n$1\r\na\r\n$1\r\nb\r\n"),
    (&[b"SORT", b"sa", b"BY", b"nosort", b"LIMIT", b"1", b"1"], b"*1\r\n$1\r\na\r\n"),
    (&[b"SORT", b"sa", b"BY", b"nosort", b"DESC", b"LIMIT", b"1", b"1"], b"*1\r\n$1\r\na\r\n"),
    (&[b"SORT", b"sa", b"BY", b"nosort", b"LIMIT", b"5", b"1"], b"*0\r\n"),
    (&[b"SORT", b"sa", b"BY", b"nosort", b"LIMIT", b"0", b"0"], b"*0\r\n"),
    (&[b"RPUSH", b"sf", b"1.5", b"-2", b"3e2", b" 4"], b":4\r\n"),
    (&[b"SORT", b"sf"], b"*4\r\n$2\r\n-2\r\n$3\r\n1.5\r\n$2\r\n 4\r\n$3\r\n3e2\r\n"),
    (&[b"RPUSH", b"sb", b"1", b"inf"], b":2\r\n"),
    (&[b"SORT", b"sb"], b"*2\r\n$1\r\n1\r\n$3\r\ninf\r\n"),
    (&[b"RPUSH", b"sb2", b"1", b""], b":2\r\n"),
    (&[b"SORT", b"sb2"], b"*2\r\n$0\r\n\r\n$1\r\n1\r\n"),
    (&[b"RPUSH", b"sb3", b"1", b"nan"], b":2\r\n"),
    (&[b"SORT", b"sb3"], b"-ERR One or more scores can't be converted into double\r\n"),
    (&[b"RPUSH", b"se", b"a", b"a"], b":2\r\n"),
    (&[b"SORT", b"se", b"ALPHA"], b"*2\r\n$1\r\na\r\n$1\r\na\r\n"),
    (&[b"SADD", b"sset", b"3", b"1", b"2"], b":3\r\n"),
    (&[b"SORT", b"sset"], b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"),
    (&[b"SORT", b"sset", b"DESC"], b"*3\r\n$1\r\n3\r\n$1\r\n2\r\n$1\r\n1\r\n"),
    (&[b"SORT", b"sset", b"LIMIT", b"1", b"1"], b"*1\r\n$1\r\n2\r\n"),
    (&[b"SORT", b"sset", b"BY", b"nosort", b"STORE", b"sd"], b":3\r\n"),
    (&[b"LRANGE", b"sd", b"0", b"-1"], b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"),
    (&[b"SADD", b"ss2", b"b", b"a", b"c"], b":3\r\n"),
    (&[b"SORT", b"ss2", b"ALPHA"], b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"),
    (&[b"SORT", b"ss2", b"BY", b"nosort", b"STORE", b"sd"], b":3\r\n"),
    (&[b"LRANGE", b"sd", b"0", b"-1"], b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"),
    (&[b"ZADD", b"sz", b"1", b"c", b"2", b"b", b"3", b"a"], b":3\r\n"),
    (&[b"SORT", b"sz", b"ALPHA"], b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"),
    (&[b"SORT", b"sz", b"BY", b"nosort"], b"*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n"),
    (&[b"SORT", b"sz", b"BY", b"nosort", b"DESC"], b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"),
    (&[b"SORT", b"sz", b"BY", b"nosort", b"LIMIT", b"1", b"1"], b"*1\r\n$1\r\nb\r\n"),
    (&[b"SORT", b"sz", b"BY", b"nosort", b"DESC", b"LIMIT", b"1", b"1"], b"*1\r\n$1\r\nb\r\n"),
    (&[b"SORT", b"sz", b"BY", b"nosort", b"LIMIT", b"0", b"10"], b"*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n"),
    (&[b"SORT", b"sz", b"ALPHA", b"DESC", b"LIMIT", b"0", b"2"], b"*2\r\n$1\r\nc\r\n$1\r\nb\r\n"),
    (&[b"SET", b"w_1", b"30"], b"+OK\r\n"),
    (&[b"SET", b"w_2", b"20"], b"+OK\r\n"),
    (&[b"SET", b"w_3", b"10"], b"+OK\r\n"),
    (&[b"SET", b"o_1", b"one"], b"+OK\r\n"),
    (&[b"SET", b"o_2", b"two"], b"+OK\r\n"),
    (&[b"HSET", b"hw_1", b"w", b"3", b"o", b"h1"], b":2\r\n"),
    (&[b"HSET", b"hw_2", b"w", b"2", b"o", b"h2"], b":2\r\n"),
    (&[b"HSET", b"hw_3", b"w", b"1", b"o", b"h3"], b":2\r\n"),
    (&[b"RPUSH", b"ids", b"1", b"2", b"3"], b":3\r\n"),
    (&[b"SORT", b"ids", b"BY", b"w_*"], b"*3\r\n$1\r\n3\r\n$1\r\n2\r\n$1\r\n1\r\n"),
    (&[b"SORT", b"ids", b"BY", b"w_*", b"DESC"], b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"),
    (&[b"SORT", b"ids", b"BY", b"w_*", b"LIMIT", b"0", b"1"], b"*1\r\n$1\r\n3\r\n"),
    (&[b"SORT", b"ids", b"BY", b"hw_*->w"], b"*3\r\n$1\r\n3\r\n$1\r\n2\r\n$1\r\n1\r\n"),
    (&[b"SORT", b"ids", b"BY", b"hw_*->missing"], b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"),
    (&[b"SORT", b"ids", b"BY", b"hw_*->"], b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"),
    (&[b"SORT", b"ids", b"BY", b"none_*"], b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"),
    (&[b"SORT", b"ids", b"BY", b"none_*", b"ALPHA"], b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"),
    (&[b"SORT", b"ids", b"BY", b"w_*", b"ALPHA"], b"*3\r\n$1\r\n3\r\n$1\r\n2\r\n$1\r\n1\r\n"),
    (&[b"SORT", b"ids", b"BY", b"w_*", b"ALPHA", b"DESC"], b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"),
    (&[b"SORT", b"ids", b"GET", b"o_*"], b"*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$-1\r\n"),
    (&[b"SORT", b"ids", b"GET", b"#", b"GET", b"o_*"], b"*6\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n3\r\n$-1\r\n"),
    (&[b"SORT", b"ids", b"GET", b"#", b"GET", b"o_*", b"GET", b"hw_*->o"], b"*9\r\n$1\r\n1\r\n$3\r\none\r\n$2\r\nh1\r\n$1\r\n2\r\n$3\r\ntwo\r\n$2\r\nh2\r\n$1\r\n3\r\n$-1\r\n$2\r\nh3\r\n"),
    (&[b"SORT", b"ids", b"GET", b"missing_*"], b"*3\r\n$-1\r\n$-1\r\n$-1\r\n"),
    (&[b"SORT", b"ids", b"GET", b"nostar"], b"*3\r\n$-1\r\n$-1\r\n$-1\r\n"),
    (&[b"SORT", b"ids", b"GET", b"hw_*->nofield"], b"*3\r\n$-1\r\n$-1\r\n$-1\r\n"),
    (&[b"SORT", b"ids", b"GET", b"w_*->x"], b"*3\r\n$-1\r\n$-1\r\n$-1\r\n"),
    (&[b"SORT", b"ids", b"BY", b"nosort", b"GET", b"o_*"], b"*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$-1\r\n"),
    (&[b"SORT", b"ids", b"BY", b"nosort", b"GET", b"#", b"DESC"], b"*3\r\n$1\r\n3\r\n$1\r\n2\r\n$1\r\n1\r\n"),
    (&[b"SORT", b"ids", b"BY", b"w_*", b"GET", b"o_*", b"STORE", b"sd"], b":3\r\n"),
    (&[b"LRANGE", b"sd", b"0", b"-1"], b"*3\r\n$0\r\n\r\n$3\r\ntwo\r\n$3\r\none\r\n"),
    (&[b"SORT", b"ids", b"GET", b"missing_*", b"STORE", b"sd"], b":3\r\n"),
    (&[b"LRANGE", b"sd", b"0", b"-1"], b"*3\r\n$0\r\n\r\n$0\r\n\r\n$0\r\n\r\n"),
    (&[b"LLEN", b"sd"], b":3\r\n"),
    (&[b"SORT", b"ids", b"STORE", b"sd"], b":3\r\n"),
    (&[b"TYPE", b"sd"], b"+list\r\n"),
    (&[b"LRANGE", b"sd", b"0", b"-1"], b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"),
    (&[b"SET", b"sd2", b"v", b"EX", b"100"], b"+OK\r\n"),
    (&[b"SORT", b"ids", b"STORE", b"sd2"], b":3\r\n"),
    (&[b"TTL", b"sd2"], b":-1\r\n"),
    (&[b"TYPE", b"sd2"], b"+list\r\n"),
    (&[b"SORT", b"missing", b"STORE", b"sd2"], b":0\r\n"),
    (&[b"EXISTS", b"sd2"], b":0\r\n"),
    (&[b"SORT", b"ids", b"LIMIT", b"5", b"5", b"STORE", b"sd3"], b":0\r\n"),
    (&[b"EXISTS", b"sd3"], b":0\r\n"),
    (&[b"SET", b"sv", b"oldvalue"], b"+OK\r\n"),
    (&[b"SORT", b"ids", b"LIMIT", b"5", b"5", b"STORE", b"sv"], b":0\r\n"),
    (&[b"EXISTS", b"sv"], b":0\r\n"),
    (&[b"SORT_RO", b"ids"], b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"),
    (&[b"SORT_RO", b"ids", b"DESC"], b"*3\r\n$1\r\n3\r\n$1\r\n2\r\n$1\r\n1\r\n"),
    (&[b"SORT_RO", b"ids", b"STORE", b"sd"], b"-ERR syntax error\r\n"),
    (&[b"SORT_RO", b"ids", b"GET", b"o_*"], b"*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$-1\r\n"),
    (&[b"SORT_RO", b"s"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SORT_RO", b"missing"], b"*0\r\n"),
    (&[b"SORT_RO"], b"-ERR wrong number of arguments for 'sort_ro' command\r\n"),
    (&[b"SORT", b"ids", b"GET", b"missing_*"], b"*3\r\n$-1\r\n$-1\r\n$-1\r\n"),
    (&[b"SORT", b"ids", b"BY", b"w_*", b"GET", b"#", b"GET", b"o_*", b"LIMIT", b"1", b"1", b"DESC"], b"*2\r\n$1\r\n2\r\n$3\r\ntwo\r\n"),
    (&[b"EVAL", b"return redis.call('SORT', KEYS[1], 'BY', 'nosort')", b"1", b"ss2"], b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"),
    (&[b"EVAL", b"return redis.call('SORT', KEYS[1], 'BY', 'nosort', 'DESC')", b"1", b"ss2"], b"*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n"),
    (&[b"SET", b"e44", b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"], b"+OK\r\n"),
    (&[b"OBJECT", b"ENCODING", b"e44"], b"$6\r\nembstr\r\n"),
    (&[b"SET", b"e45", b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"], b"+OK\r\n"),
    (&[b"OBJECT", b"ENCODING", b"e45"], b"$3\r\nraw\r\n"),
    (&[b"HSET", b"hs", b"a", b"b"], b":1\r\n"),
    (&[b"OBJECT", b"ENCODING", b"hs"], b"$8\r\nlistpack\r\n"),
    (&[b"HSET", b"hv", b"a", b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"], b":1\r\n"),
    (&[b"OBJECT", b"ENCODING", b"hv"], b"$8\r\nlistpack\r\n"),
    (&[b"HSET", b"hv2", b"a", b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"], b":1\r\n"),
    (&[b"OBJECT", b"ENCODING", b"hv2"], b"$9\r\nhashtable\r\n"),
    (&[b"HSET", b"hk", b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", b"a"], b":1\r\n"),
    (&[b"OBJECT", b"ENCODING", b"hk"], b"$9\r\nhashtable\r\n"),
    (&[b"OBJECT", b"ENCODING", b"h128"], b"$-1\r\n"),
    (&[b"OBJECT", b"ENCODING", b"h129"], b"$-1\r\n"),
    (&[b"OBJECT", b"ENCODING", b"h512"], b"$-1\r\n"),
    (&[b"OBJECT", b"ENCODING", b"h513"], b"$-1\r\n"),
    (&[b"RPUSH", b"ls", b"a"], b":1\r\n"),
    (&[b"OBJECT", b"ENCODING", b"ls"], b"$8\r\nlistpack\r\n"),
    (&[b"OBJECT", b"ENCODING", b"lbig"], b"$-1\r\n"),
    (&[b"SADD", b"si", b"1", b"2", b"3"], b":3\r\n"),
    (&[b"OBJECT", b"ENCODING", b"si"], b"$6\r\nintset\r\n"),
    (&[b"SADD", b"ss", b"a", b"b"], b":2\r\n"),
    (&[b"OBJECT", b"ENCODING", b"ss"], b"$8\r\nlistpack\r\n"),
    (&[b"SADD", b"sm2", b"1", b"a"], b":2\r\n"),
    (&[b"OBJECT", b"ENCODING", b"sm2"], b"$8\r\nlistpack\r\n"),
    (&[b"OBJECT", b"ENCODING", b"s512"], b"$-1\r\n"),
    (&[b"OBJECT", b"ENCODING", b"s513"], b"$-1\r\n"),
    (&[b"OBJECT", b"ENCODING", b"sa128"], b"$-1\r\n"),
    (&[b"OBJECT", b"ENCODING", b"sa129"], b"$-1\r\n"),
    (&[b"SADD", b"sv", b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"], b":1\r\n"),
    (&[b"OBJECT", b"ENCODING", b"sv"], b"$8\r\nlistpack\r\n"),
    (&[b"SADD", b"sv2", b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"], b":1\r\n"),
    (&[b"OBJECT", b"ENCODING", b"sv2"], b"$9\r\nhashtable\r\n"),
    (&[b"ZADD", b"zs", b"1", b"a"], b":1\r\n"),
    (&[b"OBJECT", b"ENCODING", b"zs"], b"$8\r\nlistpack\r\n"),
    (&[b"ZADD", b"zv", b"1", b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"], b":1\r\n"),
    (&[b"OBJECT", b"ENCODING", b"zv"], b"$8\r\nlistpack\r\n"),
    (&[b"ZADD", b"zv2", b"1", b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"], b":1\r\n"),
    (&[b"OBJECT", b"ENCODING", b"zv2"], b"$8\r\nskiplist\r\n"),
    (&[b"OBJECT", b"ENCODING", b"z128"], b"$-1\r\n"),
    (&[b"OBJECT", b"ENCODING", b"z129"], b"$-1\r\n"),
    (&[b"XADD", b"xs", b"1-1", b"f", b"v"], b"$3\r\n1-1\r\n"),
    (&[b"OBJECT", b"ENCODING", b"xs"], b"$6\r\nstream\r\n"),
    (&[b"COPY", b"s513", b"s513c"], b":0\r\n"),
    (&[b"SCARD", b"s513c"], b":0\r\n"),
    (&[b"COPY", b"z129", b"z129c"], b":0\r\n"),
    (&[b"ZCARD", b"z129c"], b":0\r\n"),
    (&[b"ZSCORE", b"z129c", b"m100"], b"$-1\r\n"),
    (&[b"COPY", b"lbig", b"lbigc"], b":0\r\n"),
    (&[b"LLEN", b"lbigc"], b":0\r\n"),
    (&[b"RPUSH", b"sb4", b"1", b"1e999"], b":2\r\n"),
    (&[b"SORT", b"sb4"], b"-ERR One or more scores can't be converted into double\r\n"),
];

/// The same, for what depends on C's `long double`: Redis, and Tantalum, come
/// out differently where it isn't 80 bits.
#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
#[rustfmt::skip]
const FLOATS: &[(&[&[u8]], &[u8])] = &[
    (&[b"SET", b"s", b"hello"], b"+OK\r\n"),
    (&[b"RPUSH", b"l", b"a"], b":1\r\n"),
    (&[b"SET", b"f", b"10.50"], b"+OK\r\n"),
    (&[b"INCRBYFLOAT", b"f", b"0.1"], b"$4\r\n10.6\r\n"),
    (&[b"GET", b"f"], b"$4\r\n10.6\r\n"),
    (&[b"INCRBYFLOAT", b"f", b"-5"], b"$3\r\n5.6\r\n"),
    (&[b"INCRBYFLOAT", b"f", b"2.0e2"], b"$21\r\n205.60000000000000001\r\n"),
    (&[b"INCRBYFLOAT", b"nf", b"1.5"], b"$3\r\n1.5\r\n"),
    (&[b"INCRBYFLOAT", b"nf", b"abc"], b"-ERR value is not a valid float\r\n"),
    (&[b"INCRBYFLOAT", b"nf", b""], b"-ERR value is not a valid float\r\n"),
    (&[b"SET", b"bad", b"abc"], b"+OK\r\n"),
    (&[b"INCRBYFLOAT", b"bad", b"1"], b"-ERR value is not a valid float\r\n"),
    (&[b"INCRBYFLOAT", b"l", b"1"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"INCRBYFLOAT", b"inf1", b"inf"], b"-ERR increment would produce NaN or Infinity\r\n"),
    (&[b"INCRBYFLOAT", b"inf1", b"-inf"], b"-ERR increment would produce NaN or Infinity\r\n"),
    (&[b"INCRBYFLOAT", b"inf1", b"nan"], b"-ERR value is not a valid float\r\n"),
    (&[b"EXISTS", b"inf1"], b":0\r\n"),
    (&[b"INCRBYFLOAT", b"f", b"1e5000"], b"-ERR value is not a valid float\r\n"),
    (&[b"INCRBYFLOAT", b"f", b"-1e5000"], b"-ERR value is not a valid float\r\n"),
    (&[b"INCRBYFLOAT", b"f", b"1e-5000"], b"-ERR value is not a valid float\r\n"),
    (&[b"INCRBYFLOAT", b"zero", b"0"], b"$1\r\n0\r\n"),
    (&[b"INCRBYFLOAT", b"zero", b"-0"], b"$1\r\n0\r\n"),
    (&[b"INCRBYFLOAT", b"neg", b"-1"], b"$2\r\n-1\r\n"),
    (&[b"INCRBYFLOAT", b"neg", b"1"], b"$1\r\n0\r\n"),
    (&[b"SET", b"ft", b"1", b"EX", b"100"], b"+OK\r\n"),
    (&[b"INCRBYFLOAT", b"ft", b"1"], b"$1\r\n2\r\n"),
    (&[b"TTL", b"ft"], b":100\r\n"),
    (&[b"INCRBYFLOAT", b"f", b" 1"], b"-ERR value is not a valid float\r\n"),
    (&[b"INCRBYFLOAT", b"f", b"1 "], b"-ERR value is not a valid float\r\n"),
    (&[b"INCRBYFLOAT", b"hx", b"0x10"], b"$2\r\n16\r\n"),
    (&[b"INCRBYFLOAT", b"sm", b"0.1"], b"$3\r\n0.1\r\n"),
    (&[b"INCRBYFLOAT", b"sm", b"0.2"], b"$3\r\n0.3\r\n"),
    (&[b"INCRBYFLOAT", b"sm", b"3.0e-20"], b"$3\r\n0.3\r\n"),
    (&[b"INCRBYFLOAT", b"big", b"1e30"], b"$31\r\n1000000000000000000024696061952\r\n"),
    (&[b"INCRBYFLOAT", b"big", b"1"], b"$31\r\n1000000000000000000024696061952\r\n"),
    (&[b"SET", b"s4", b"3.0e3"], b"+OK\r\n"),
    (&[b"INCRBYFLOAT", b"s4", b"2.0e2"], b"$4\r\n3200\r\n"),
    (&[b"INCRBYFLOAT", b"i3", b"1"], b"$1\r\n1\r\n"),
    (&[b"TYPE", b"i3"], b"+string\r\n"),
    (&[b"INCR", b"i3"], b":2\r\n"),
    (&[b"INCRBYFLOAT", b"i4", b"1.5"], b"$3\r\n1.5\r\n"),
    (&[b"INCR", b"i4"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"INCRBYFLOAT", b"f"], b"-ERR wrong number of arguments for 'incrbyfloat' command\r\n"),
    (&[b"INCRBYFLOAT", b"f", b"1", b"2"], b"-ERR wrong number of arguments for 'incrbyfloat' command\r\n"),
    (&[b"SET", b"sp", b" 1"], b"+OK\r\n"),
    (&[b"INCRBYFLOAT", b"sp", b"1"], b"-ERR value is not a valid float\r\n"),
    (&[b"HSET", b"hf", b"a", b"10.50", b"b", b"x", b"n", b"5"], b":3\r\n"),
    (&[b"HINCRBYFLOAT", b"hf", b"a", b"0.1"], b"$4\r\n10.6\r\n"),
    (&[b"HGET", b"hf", b"a"], b"$4\r\n10.6\r\n"),
    (&[b"HINCRBYFLOAT", b"hf", b"new", b"5"], b"$1\r\n5\r\n"),
    (&[b"HINCRBYFLOAT", b"hf", b"n", b"0.5"], b"$3\r\n5.5\r\n"),
    (&[b"HINCRBYFLOAT", b"hf", b"b", b"1"], b"-ERR hash value is not a float\r\n"),
    (&[b"HINCRBYFLOAT", b"hf", b"a", b"x"], b"-ERR value is not a valid float\r\n"),
    (&[b"HINCRBYFLOAT", b"hf", b"a", b""], b"-ERR value is not a valid float\r\n"),
    (&[b"HINCRBYFLOAT", b"hf", b"a", b"inf"], b"-ERR value is NaN or Infinity\r\n"),
    (&[b"HINCRBYFLOAT", b"hf", b"a", b"nan"], b"-ERR value is not a valid float\r\n"),
    (&[b"HINCRBYFLOAT", b"hf", b"a", b"1e5000"], b"-ERR value is not a valid float\r\n"),
    (&[b"HINCRBYFLOAT", b"nh", b"f", b"1"], b"$1\r\n1\r\n"),
    (&[b"HGETALL", b"nh"], b"*2\r\n$1\r\nf\r\n$1\r\n1\r\n"),
    (&[b"HINCRBYFLOAT", b"s", b"f", b"1"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"HINCRBYFLOAT", b"hf", b"a"], b"-ERR wrong number of arguments for 'hincrbyfloat' command\r\n"),
    (&[b"HINCRBYFLOAT", b"hf", b"a", b"1", b"2"], b"-ERR wrong number of arguments for 'hincrbyfloat' command\r\n"),
    (&[b"HSET", b"hf2", b"a", b"1e4900"], b":1\r\n"),
    (&[b"HINCRBYFLOAT", b"hf2", b"a", b"1e4932"], b"$4933\r\n1000000000000000000006018949387963891323837774254533114773785041352533019486608722407533327779433142773666525563666277682061860906440460336623416164931097542108653623739947237824273607755646282077337746770628281083461244709457643633409272844851420914666765408339856497738306756454137037618440476182982865954054046320908299555942321302503320602080070208203623921079151881460203745014774931620490030394998719312614654064081373493248877120892455186383753439197870978041502797716880193955403107721911638646256939484197293128160329482360996995779489224859264034146949087194820794222433687849284561987230134006305414553626323614864479565079390052761980633463640128979696615362545507608303960553830866145592644372289102918602696741767741297153998359263135897985849697638678310586803720024647593066044070323761747439375151949994191528876172274034561238465689530914002496208547488452249219821049217690359246594326188537200846025476936856707726986425804304053688317572380292690880454677852210458790733177421098965346383845533016192965653377533421097553773202105452559589360227732550843545717469715211293029277100649951579279463212875228768700827274629951331584994203243708412190948315614798641721054158397351395747758548328754270534225721361640356227816080094892433366902417454242382629232277544026609640255325557814321580325340352330660617642326696140257239552549121071985189443846777623334881929189773707008403466595204838733621633273383631944856989484138444978386656058755310848833689791896484998847550364264104800800578245511762594691161545730891319744107953897993101008217601269310629890308357893793344340072837782716410520818640099771094401772024731084835860689936533035726854175396195351768096519310944631445923638053214575993378468775292865871435742249777979822775014324475845899615096380079899099538453121043851821349178371736640702645304284254885437611441690360616466368202194307769462209963387690446576691969551571041941288302648912827345194286271122230347823246073498285153708579500062656222714954113329293953346495615947784091175883554988117167387650681525781361864450364992605079129770604793291076559668041153558254765774288831330658676910296506538343982699076824147706077401185501337053676875440863113630172067902223837838525802181406003942606507609826320015193807633035586661539150425605940691768637845815434184585255915074081317250319572700370579877977177644345205933505850374212779064837146242765034714569464306865784829936445754893825213995710137567750342556762329137973387913047300554841737506800154984198686923965286147079273203561004647365286049876790788288301130050437207489067798842078443360167772245834103629301041345252122743437501935254370757049878526838283810705343010153283985958550741661374001212323172562230037290597675029278865489570268803618001520280748588481858632332145422181524500970533738858652881625738158955840469334830106366243262210429210258173978186522208306814207038831590332622155464159216193005478809313772661406449321989529908990407599717265354887081027622412165764793106246309910984757869627982700885055259900973567128498437782671492610351088858271463387495833021819318548705558606395846070390500323204636339383655528123657438115225035433947543876805317781492691295772410403746456774175666884503175504415270824258750305653788473882671038340481278188648013647450190431469737277008003041204693859153544058059108384516541261521127088116402349679151317227395183628490922161336429757386698401928682323657056924758648793848799822907606679071280964393541325129231682456669123450140879120137931548652537601508656209183486928384515842973760163601756230697826779237275078471824258664382133759876151537532598429521999758431018087001817485300919479583171049967781322202936864648780744025361452947117776701129210049872555316467204668470989130203733391349501708713181244068921199587314823974568234174792429139799088587794666528769986602820375968045445209219875596678877912103336949894142378173648692130854177898020194629670311101994938157038765957014792056173739623614871557317714453597336202068774935086409386414223375141765601758268418764114651053088281009248246110195084364468926837572992870110411414878770616397817334529367403238061699632089293543118185299881280463481800696201966723511922556078483839325067415562633057448408746541808590342228476964416681055875561781810005936328191844815915658719683522845979901018335050006246392027657149580332319739660376707180429166280658258434773362764563277134220525779869893832824866422909928484939374964071386248088592408142723646533801882861736205992781965360090000802821408763787641023437653119263757280212773694655213507075717622521160227187157631635583682130089364682543007292042009976150135189415923413244100006421082817394988300265734422677548100741829682842668334472314928702968616204691717366798642916146473018363370523427203416579250811577900985980957404830019403600361860520985937973506956557739754741783354728589350876379371163141086538418113791886622720\r\n"),
    (&[b"HGET", b"hf2", b"a"], b"$4933\r\n1000000000000000000006018949387963891323837774254533114773785041352533019486608722407533327779433142773666525563666277682061860906440460336623416164931097542108653623739947237824273607755646282077337746770628281083461244709457643633409272844851420914666765408339856497738306756454137037618440476182982865954054046320908299555942321302503320602080070208203623921079151881460203745014774931620490030394998719312614654064081373493248877120892455186383753439197870978041502797716880193955403107721911638646256939484197293128160329482360996995779489224859264034146949087194820794222433687849284561987230134006305414553626323614864479565079390052761980633463640128979696615362545507608303960553830866145592644372289102918602696741767741297153998359263135897985849697638678310586803720024647593066044070323761747439375151949994191528876172274034561238465689530914002496208547488452249219821049217690359246594326188537200846025476936856707726986425804304053688317572380292690880454677852210458790733177421098965346383845533016192965653377533421097553773202105452559589360227732550843545717469715211293029277100649951579279463212875228768700827274629951331584994203243708412190948315614798641721054158397351395747758548328754270534225721361640356227816080094892433366902417454242382629232277544026609640255325557814321580325340352330660617642326696140257239552549121071985189443846777623334881929189773707008403466595204838733621633273383631944856989484138444978386656058755310848833689791896484998847550364264104800800578245511762594691161545730891319744107953897993101008217601269310629890308357893793344340072837782716410520818640099771094401772024731084835860689936533035726854175396195351768096519310944631445923638053214575993378468775292865871435742249777979822775014324475845899615096380079899099538453121043851821349178371736640702645304284254885437611441690360616466368202194307769462209963387690446576691969551571041941288302648912827345194286271122230347823246073498285153708579500062656222714954113329293953346495615947784091175883554988117167387650681525781361864450364992605079129770604793291076559668041153558254765774288831330658676910296506538343982699076824147706077401185501337053676875440863113630172067902223837838525802181406003942606507609826320015193807633035586661539150425605940691768637845815434184585255915074081317250319572700370579877977177644345205933505850374212779064837146242765034714569464306865784829936445754893825213995710137567750342556762329137973387913047300554841737506800154984198686923965286147079273203561004647365286049876790788288301130050437207489067798842078443360167772245834103629301041345252122743437501935254370757049878526838283810705343010153283985958550741661374001212323172562230037290597675029278865489570268803618001520280748588481858632332145422181524500970533738858652881625738158955840469334830106366243262210429210258173978186522208306814207038831590332622155464159216193005478809313772661406449321989529908990407599717265354887081027622412165764793106246309910984757869627982700885055259900973567128498437782671492610351088858271463387495833021819318548705558606395846070390500323204636339383655528123657438115225035433947543876805317781492691295772410403746456774175666884503175504415270824258750305653788473882671038340481278188648013647450190431469737277008003041204693859153544058059108384516541261521127088116402349679151317227395183628490922161336429757386698401928682323657056924758648793848799822907606679071280964393541325129231682456669123450140879120137931548652537601508656209183486928384515842973760163601756230697826779237275078471824258664382133759876151537532598429521999758431018087001817485300919479583171049967781322202936864648780744025361452947117776701129210049872555316467204668470989130203733391349501708713181244068921199587314823974568234174792429139799088587794666528769986602820375968045445209219875596678877912103336949894142378173648692130854177898020194629670311101994938157038765957014792056173739623614871557317714453597336202068774935086409386414223375141765601758268418764114651053088281009248246110195084364468926837572992870110411414878770616397817334529367403238061699632089293543118185299881280463481800696201966723511922556078483839325067415562633057448408746541808590342228476964416681055875561781810005936328191844815915658719683522845979901018335050006246392027657149580332319739660376707180429166280658258434773362764563277134220525779869893832824866422909928484939374964071386248088592408142723646533801882861736205992781965360090000802821408763787641023437653119263757280212773694655213507075717622521160227187157631635583682130089364682543007292042009976150135189415923413244100006421082817394988300265734422677548100741829682842668334472314928702968616204691717366798642916146473018363370523427203416579250811577900985980957404830019403600361860520985937973506956557739754741783354728589350876379371163141086538418113791886622720\r\n"),
];

#[tokio::test]
async fn replies_match_redis() {
    replay(SCRIPT).await;
}

#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
#[tokio::test]
async fn float_replies_match_redis() {
    replay(FLOATS).await;
}

/// What comes out the same whatever a `long double` is.
#[tokio::test]
async fn floats_add_up_anywhere() {
    replay(&[
        (&[b"SET", b"f", b"10.5"], b"+OK\r\n"),
        (&[b"INCRBYFLOAT", b"f", b"0.25"], b"$5\r\n10.75\r\n"),
        (&[b"INCRBYFLOAT", b"f", b"-10.75"], b"$1\r\n0\r\n"),
        (
            &[b"INCRBYFLOAT", b"f", b"abc"],
            b"-ERR value is not a valid float\r\n",
        ),
        (
            &[b"INCRBYFLOAT", b"f", b"inf"],
            b"-ERR increment would produce NaN or Infinity\r\n",
        ),
        (&[b"HSET", b"h", b"a", b"1.5"], b":1\r\n"),
        (&[b"HINCRBYFLOAT", b"h", b"a", b"1"], b"$3\r\n2.5\r\n"),
        (
            &[b"HINCRBYFLOAT", b"h", b"a", b"inf"],
            b"-ERR value is NaN or Infinity\r\n",
        ),
    ])
    .await;
}

#[tokio::test]
async fn resp3_replies() {
    let mut client = connect_buffered().await;
    let mut ask = async |args: &[&[u8]]| {
        client.stream.get_mut().write_all(&req(args)).await.unwrap();
        client.raw_reply().await
    };
    ask(&[b"HELLO", b"3"]).await;
    ask(&[b"HSET", b"one", b"f", b"v"]).await;
    ask(&[b"SET", b"k1", b"ohmytext"]).await;
    ask(&[b"SET", b"k2", b"mynewtext"]).await;
    ask(&[b"RPUSH", b"ids", b"1"]).await;
    assert_eq!(ask(&[b"HRANDFIELD", b"missing"]).await, b"_\r\n");
    assert_eq!(ask(&[b"RANDOMKEY", b"x"]).await[0], b'-');
    assert_eq!(
        ask(&[b"HRANDFIELD", b"one", b"1"]).await,
        b"*1\r\n$1\r\nf\r\n"
    );
    assert_eq!(
        ask(&[b"HRANDFIELD", b"one", b"1", b"WITHVALUES"]).await,
        b"*1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"
    );
    assert_eq!(
        ask(&[b"HRANDFIELD", b"one", b"-2", b"WITHVALUES"]).await,
        b"*2\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"
    );
    assert_eq!(
        ask(&[b"SORT", b"ids", b"GET", b"none_*"]).await,
        b"*1\r\n_\r\n"
    );
    assert_eq!(ask(&[b"OBJECT", b"ENCODING", b"missing"]).await, b"_\r\n");
    assert_eq!(ask(&[b"GETSET", b"new", b"v"]).await, b"_\r\n");
    assert_eq!(
        ask(&[b"LCS", b"k1", b"k2", b"IDX", b"WITHMATCHLEN"]).await,
        b"%2\r\n$7\r\nmatches\r\n*2\r\n*3\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n:4\r\n\
          *3\r\n*2\r\n:2\r\n:3\r\n*2\r\n:0\r\n:1\r\n:2\r\n$3\r\nlen\r\n:6\r\n"
    );
}

/// `OBJECT ENCODING` at Redis' limits (checked against it), by what's held now.
#[tokio::test]
async fn encodings_change_where_redis_does() {
    let mut c = connect().await;
    let mut run = async |args: Vec<Vec<u8>>| {
        let args: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
        c.write_all(&req(&args)).await.unwrap();
        let mut reply = vec![0; 1024];
        let n = tokio::io::AsyncReadExt::read(&mut c, &mut reply)
            .await
            .unwrap();
        reply.truncate(n);
        reply
    };
    let words =
        |line: String| -> Vec<Vec<u8>> { line.split(' ').map(|w| w.as_bytes().to_vec()).collect() };
    let cases: Vec<(String, &str)> = vec![
        (format!("SET e44 {}", "x".repeat(44)), "embstr"),
        (format!("SET e45 {}", "x".repeat(45)), "raw"),
        ("SET i 9223372036854775807".into(), "int"),
        ("SET o 007".into(), "embstr"),
        (format!("HSET hv a {}", "x".repeat(64)), "listpack"),
        (format!("HSET hw a {}", "x".repeat(65)), "hashtable"),
        (format!("SADD sv {}", "x".repeat(64)), "listpack"),
        (format!("SADD sw {}", "x".repeat(65)), "hashtable"),
        (format!("ZADD zv 1 {}", "x".repeat(64)), "listpack"),
        (format!("ZADD zw 1 {}", "x".repeat(65)), "skiplist"),
        ("SADD si 1 2 3".into(), "intset"),
        ("SADD sm 1 a".into(), "listpack"),
        ("RPUSH l a".into(), "listpack"),
        (
            format!("RPUSH lb {}", vec!["x".repeat(200); 100].join(" ")),
            "quicklist",
        ),
        ("XADD xs 1-1 f v".into(), "stream"),
    ];
    let many = |prefix: &str, n: usize, each: &dyn Fn(usize) -> String| {
        let parts: Vec<String> = (0..n).map(each).collect();
        format!("{prefix} {}", parts.join(" "))
    };
    let mut cases = cases;
    cases.push((many("HSET h512", 512, &|i| format!("f{i} v")), "listpack"));
    cases.push((many("HSET h513", 513, &|i| format!("f{i} v")), "hashtable"));
    cases.push((many("SADD s512", 512, &|i| i.to_string()), "intset"));
    cases.push((many("SADD s513", 513, &|i| i.to_string()), "hashtable"));
    cases.push((many("SADD a128", 128, &|i| format!("m{i}")), "listpack"));
    cases.push((many("SADD a129", 129, &|i| format!("m{i}")), "hashtable"));
    cases.push((many("ZADD z128", 128, &|i| format!("{i} m{i}")), "listpack"));
    cases.push((many("ZADD z129", 129, &|i| format!("{i} m{i}")), "skiplist"));
    for (line, expected) in cases {
        let args = words(line);
        let key = args[1].clone();
        run(args).await;
        let reply = run(vec![b"OBJECT".to_vec(), b"ENCODING".to_vec(), key.clone()]).await;
        let want = format!("${}\r\n{expected}\r\n", expected.len());
        assert_eq!(
            String::from_utf8_lossy(&reply),
            want,
            "{}",
            String::from_utf8_lossy(&key)
        );
    }
}

/// Copies of big collections are their own: changing one leaves the other. A
/// key too long to pack with its value works too.
#[tokio::test]
async fn copies_are_independent() {
    let mut client = connect_buffered().await;
    let mut ask = async |args: &[&[u8]]| {
        client.stream.get_mut().write_all(&req(args)).await.unwrap();
        client.raw_reply().await
    };
    let names: Vec<String> = (0..1000).map(|i| format!("m{i}")).collect();
    let value = [b'x'; 200];

    let mut command: Vec<&[u8]> = vec![b"RPUSH", b"l"];
    command.extend(std::iter::repeat_n(&value[..], 200));
    ask(&command).await;
    assert_eq!(ask(&[b"COPY", b"l", b"l2"]).await, b":1\r\n");
    ask(&[b"RPOP", b"l2", b"150"]).await;
    assert_eq!(ask(&[b"LLEN", b"l"]).await, b":200\r\n");
    assert_eq!(ask(&[b"LLEN", b"l2"]).await, b":50\r\n");

    let mut command: Vec<&[u8]> = vec![b"SADD", b"s"];
    command.extend(names.iter().map(|m| m.as_bytes()));
    ask(&command).await;
    assert_eq!(ask(&[b"COPY", b"s", b"s2"]).await, b":1\r\n");
    assert_eq!(ask(&[b"SREM", b"s2", b"m0"]).await, b":1\r\n");
    assert_eq!(ask(&[b"SISMEMBER", b"s", b"m0"]).await, b":1\r\n");
    assert_eq!(ask(&[b"SCARD", b"s2"]).await, b":999\r\n");

    let mut command: Vec<&[u8]> = vec![b"ZADD", b"z"];
    for name in &names {
        command.push(b"1");
        command.push(name.as_bytes());
    }
    ask(&command).await;
    assert_eq!(ask(&[b"COPY", b"z", b"z2"]).await, b":1\r\n");
    assert_eq!(ask(&[b"ZREM", b"z2", b"m0"]).await, b":1\r\n");
    assert_eq!(ask(&[b"ZSCORE", b"z", b"m0"]).await, b"$1\r\n1\r\n");
    assert_eq!(ask(&[b"ZCARD", b"z2"]).await, b":999\r\n");

    ask(&[b"XADD", b"x", b"1-1", b"f", b"v"]).await;
    assert_eq!(ask(&[b"COPY", b"x", b"x2"]).await, b":1\r\n");
    ask(&[b"XADD", b"x2", b"2-1", b"f", b"v"]).await;
    assert_eq!(ask(&[b"XLEN", b"x"]).await, b":1\r\n");
    assert_eq!(ask(&[b"XLEN", b"x2"]).await, b":2\r\n");

    ask(&[b"HSET", b"h", b"a", b"1"]).await;
    assert_eq!(ask(&[b"COPY", b"h", b"h2"]).await, b":1\r\n");
    ask(&[b"HSET", b"h2", b"b", b"2"]).await;
    assert_eq!(ask(&[b"HLEN", b"h"]).await, b":1\r\n");

    // A value and a key too long to fit together in the store's segments.
    let long_key = vec![b'k'; 17_000];
    ask(&[b"RPUSH", b"short", b"a", b"b"]).await;
    assert_eq!(ask(&[b"COPY", b"short", &long_key]).await, b":1\r\n");
    assert_eq!(ask(&[b"RPOP", &long_key]).await, b"$1\r\nb\r\n");
    assert_eq!(ask(&[b"LLEN", b"short"]).await, b":2\r\n");
}

/// A client blocked on the destination hears of `COPY` and `SORT STORE`.
#[tokio::test]
async fn copy_and_sort_store_wake_blocked_clients() {
    for mode in [Mode::PerThread, Mode::Shared] {
        let (addr, _ctx) = start(mode).await;
        let mut writer = TcpStream::connect(addr).await.unwrap();
        check(
            &mut writer,
            &req(&[b"RPUSH", b"src", b"b", b"a"]),
            b":2\r\n",
        )
        .await;
        for (destination, command) in [
            (&b"d1"[..], req(&[b"COPY", b"src", b"d1"])),
            (
                &b"d2"[..],
                req(&[b"SORT", b"src", b"ALPHA", b"STORE", b"d2"]),
            ),
        ] {
            let mut waiter = TcpStream::connect(addr).await.unwrap();
            waiter
                .write_all(&req(&[b"BLPOP", destination, b"5"]))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
            writer.write_all(&command).await.unwrap();
            let first = if destination == b"d1" { b"b" } else { b"a" };
            let expected = [
                b"*2\r\n$2\r\n".as_slice(),
                destination,
                b"\r\n$1\r\n",
                first,
                b"\r\n",
            ]
            .concat();
            let mut got = vec![0; expected.len()];
            tokio::time::timeout(
                Duration::from_secs(5),
                tokio::io::AsyncReadExt::read_exact(&mut waiter, &mut got),
            )
            .await
            .expect("the blocked client hears")
            .unwrap();
            assert_eq!(got, expected);
            let mut reply = [0; 4];
            tokio::io::AsyncReadExt::read_exact(&mut writer, &mut reply)
                .await
                .unwrap();
            assert_eq!(
                &reply,
                if destination == b"d1" {
                    b":1\r\n"
                } else {
                    b":2\r\n"
                }
            );
        }
    }
}

/// Changing a watched key, by any of the new commands, fails the transaction.
#[tokio::test]
async fn new_writes_touch_watched_keys() {
    let commands: [&[&[u8]]; 6] = [
        &[b"APPEND", b"k", b"x"],
        &[b"SETRANGE", b"k", b"0", b"x"],
        &[b"GETSET", b"k", b"x"],
        &[b"INCRBYFLOAT", b"n", b"1"],
        &[b"COPY", b"o", b"k", b"REPLACE"],
        &[b"SORT", b"l", b"ALPHA", b"STORE", b"k"],
    ];
    for command in commands {
        let (addr, _ctx) = start(Mode::Shared).await;
        let mut a = TcpStream::connect(addr).await.unwrap();
        let mut b = TcpStream::connect(addr).await.unwrap();
        check(&mut a, &req(&[b"SET", b"k", b"v"]), b"+OK\r\n").await;
        check(&mut a, &req(&[b"SET", b"n", b"1"]), b"+OK\r\n").await;
        check(&mut a, &req(&[b"SET", b"o", b"v"]), b"+OK\r\n").await;
        check(&mut a, &req(&[b"RPUSH", b"l", b"x"]), b":1\r\n").await;
        let key = if command[0] == b"INCRBYFLOAT" {
            &b"n"[..]
        } else {
            b"k"
        };
        check(&mut a, &req(&[b"WATCH", key]), b"+OK\r\n").await;
        b.write_all(&req(command)).await.unwrap();
        let mut reply = [0; 1];
        tokio::io::AsyncReadExt::read_exact(&mut b, &mut reply)
            .await
            .unwrap();
        check(&mut a, &req(&[b"MULTI"]), b"+OK\r\n").await;
        check(&mut a, &req(&[b"PING"]), b"+QUEUED\r\n").await;
        check(&mut a, &req(&[b"EXEC"]), b"*-1\r\n").await;
    }
}

/// Over a store of many keys, `RANDOMKEY` finds them all in time.
#[tokio::test]
async fn randomkey_reaches_every_key() {
    let mut client = connect_buffered().await;
    let mut ask = async |args: &[&[u8]]| {
        client.stream.get_mut().write_all(&req(args)).await.unwrap();
        client.raw_reply().await
    };
    for i in 0..8 {
        ask(&[b"SET", format!("k{i}").as_bytes(), b"v"]).await;
    }
    let mut seen = std::collections::HashSet::new();
    for _ in 0..2000 {
        seen.insert(ask(&[b"RANDOMKEY"]).await);
    }
    assert_eq!(seen.len(), 8, "{seen:?}");
}
