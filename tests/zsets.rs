//! Sorted set commands: replies recorded from Redis 7.2.4
//! (`tools/redis-diff.py --rust`), and a model test across packed and big
//! sorted sets.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use common::Value;
use tantalum::context::{Context, Settings};
use tantalum::store::Store;

#[rustfmt::skip]
const SCRIPT: &[(&[&[u8]], &[u8])] = &[
    (&[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"], b":3\r\n"),
    (&[b"ZADD", b"z", b"1.5", b"b"], b":0\r\n"),
    (&[b"ZADD", b"z", b"CH", b"1.5", b"b", b"9", b"c", b"10", b"d"], b":2\r\n"),
    (&[b"ZADD", b"z", b"NX", b"0", b"a", b"11", b"e"], b":1\r\n"),
    (&[b"ZADD", b"z", b"XX", b"0", b"a", b"12", b"f"], b":0\r\n"),
    (&[b"ZADD", b"z", b"GT", b"0.5", b"a"], b":0\r\n"),
    (&[b"ZADD", b"z", b"GT", b"5", b"a"], b":0\r\n"),
    (&[b"ZADD", b"z", b"LT", b"10", b"a"], b":0\r\n"),
    (&[b"ZADD", b"z", b"LT", b"1", b"a"], b":0\r\n"),
    (&[b"ZADD", b"z", b"XX", b"GT", b"CH", b"20", b"a"], b":1\r\n"),
    (&[b"ZADD", b"z", b"INCR", b"1", b"a"], b"$2\r\n21\r\n"),
    (&[b"ZADD", b"z", b"INCR", b"NX", b"1", b"a"], b"$-1\r\n"),
    (&[b"ZADD", b"z", b"INCR", b"XX", b"1", b"newm"], b"$-1\r\n"),
    (&[b"ZADD", b"z", b"INCR", b"GT", b"-100", b"a"], b"$-1\r\n"),
    (&[b"ZADD", b"z", b"NX", b"XX", b"1", b"a"], b"-ERR XX and NX options at the same time are not compatible\r\n"),
    (&[b"ZADD", b"z", b"GT", b"LT", b"1", b"a"], b"-ERR GT, LT, and/or NX options at the same time are not compatible\r\n"),
    (&[b"ZADD", b"z", b"NX", b"GT", b"1", b"a"], b"-ERR GT, LT, and/or NX options at the same time are not compatible\r\n"),
    (&[b"ZADD", b"z", b"INCR", b"1", b"a", b"2", b"b"], b"-ERR INCR option supports a single increment-element pair\r\n"),
    (&[b"ZADD", b"z", b"1"], b"-ERR wrong number of arguments for 'zadd' command\r\n"),
    (&[b"ZADD", b"z"], b"-ERR wrong number of arguments for 'zadd' command\r\n"),
    (&[b"ZADD", b"z", b"1", b"a", b"2"], b"-ERR syntax error\r\n"),
    (&[b"ZADD", b"z", b"x", b"a"], b"-ERR value is not a valid float\r\n"),
    (&[b"ZADD", b"z", b"1", b"a", b"x", b"b"], b"-ERR value is not a valid float\r\n"),
    (&[b"ZADD", b"z", b"nan", b"a"], b"-ERR value is not a valid float\r\n"),
    (&[b"ZADD", b"z", b"inf", b"a"], b":0\r\n"),
    (&[b"ZADD", b"z", b"INCR", b"-inf", b"a"], b"-ERR resulting score is not a number (NaN)\r\n"),
    (&[b"ZSCORE", b"z", b"a"], b"$3\r\ninf\r\n"),
    (&[b"ZADD", b"z", b"-inf", b"a"], b":0\r\n"),
    (&[b"ZADD", b"nokey", b"XX", b"1", b"a"], b":0\r\n"),
    (&[b"EXISTS", b"nokey"], b":0\r\n"),
    (&[b"ZADD", b"nokey", b"XX", b"INCR", b"1", b"a"], b"$-1\r\n"),
    (&[b"ZINCRBY", b"z", b"2.5", b"b"], b"$1\r\n4\r\n"),
    (&[b"ZINCRBY", b"z", b"1", b"nomember"], b"$1\r\n1\r\n"),
    (&[b"ZINCRBY", b"z", b"x", b"b"], b"-ERR value is not a valid float\r\n"),
    (&[b"ZINCRBY", b"nokey2", b"3", b"m"], b"$1\r\n3\r\n"),
    (&[b"ZINCRBY", b"z", b"nan", b"b"], b"-ERR value is not a valid float\r\n"),
    (&[b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES"], b"*12\r\n$1\r\na\r\n$4\r\n-inf\r\n$8\r\nnomember\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n4\r\n$1\r\nc\r\n$1\r\n9\r\n$1\r\nd\r\n$2\r\n10\r\n$1\r\ne\r\n$2\r\n11\r\n"),
    (&[b"ZCARD", b"z"], b":6\r\n"),
    (&[b"ZCARD", b"nokey"], b":0\r\n"),
    (&[b"ZSCORE", b"z", b"b"], b"$1\r\n4\r\n"),
    (&[b"ZSCORE", b"z", b"nope"], b"$-1\r\n"),
    (&[b"ZSCORE", b"nokey", b"a"], b"$-1\r\n"),
    (&[b"ZMSCORE", b"z", b"a", b"b", b"nope"], b"*3\r\n$4\r\n-inf\r\n$1\r\n4\r\n$-1\r\n"),
    (&[b"ZMSCORE", b"nokey", b"a"], b"*1\r\n$-1\r\n"),
    (&[b"ZRANK", b"z", b"b"], b":2\r\n"),
    (&[b"ZREVRANK", b"z", b"b"], b":3\r\n"),
    (&[b"ZRANK", b"z", b"nope"], b"$-1\r\n"),
    (&[b"ZRANK", b"z", b"b", b"WITHSCORE"], b"*2\r\n:2\r\n$1\r\n4\r\n"),
    (&[b"ZREVRANK", b"z", b"b", b"WITHSCORE"], b"*2\r\n:3\r\n$1\r\n4\r\n"),
    (&[b"ZRANK", b"z", b"nope", b"WITHSCORE"], b"*-1\r\n"),
    (&[b"ZRANK", b"nokey", b"a", b"WITHSCORE"], b"*-1\r\n"),
    (&[b"ZRANK", b"z", b"b", b"BAD"], b"-ERR syntax error\r\n"),
    (&[b"ZRANK", b"z", b"b", b"WITHSCORE", b"x"], b"-ERR wrong number of arguments for 'zrank' command\r\n"),
    (&[b"ZRANGE", b"z", b"0", b"-1"], b"*6\r\n$1\r\na\r\n$8\r\nnomember\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n"),
    (&[b"ZRANGE", b"z", b"1", b"2", b"WITHSCORES"], b"*4\r\n$8\r\nnomember\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n4\r\n"),
    (&[b"ZRANGE", b"z", b"-2", b"-1"], b"*2\r\n$1\r\nd\r\n$1\r\ne\r\n"),
    (&[b"ZRANGE", b"z", b"5", b"1"], b"*0\r\n"),
    (&[b"ZRANGE", b"z", b"0", b"-1", b"REV"], b"*6\r\n$1\r\ne\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n$8\r\nnomember\r\n$1\r\na\r\n"),
    (&[b"ZRANGE", b"z", b"0", b"1", b"REV", b"WITHSCORES"], b"*4\r\n$1\r\ne\r\n$2\r\n11\r\n$1\r\nd\r\n$2\r\n10\r\n"),
    (&[b"ZRANGE", b"z", b"x", b"1"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"ZRANGE", b"z", b"0", b"1", b"LIMIT", b"0", b"1"], b"-ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX\r\n"),
    (&[b"ZREVRANGE", b"z", b"0", b"-1", b"WITHSCORES"], b"*12\r\n$1\r\ne\r\n$2\r\n11\r\n$1\r\nd\r\n$2\r\n10\r\n$1\r\nc\r\n$1\r\n9\r\n$1\r\nb\r\n$1\r\n4\r\n$8\r\nnomember\r\n$1\r\n1\r\n$1\r\na\r\n$4\r\n-inf\r\n"),
    (&[b"ZREVRANGE", b"z", b"0", b"0", b"REV"], b"-ERR syntax error\r\n"),
    (&[b"ZRANGE", b"nokey", b"0", b"-1"], b"*0\r\n"),
    (&[b"DEL", b"s"], b":0\r\n"),
    (&[b"ZADD", b"s", b"1", b"a", b"2", b"b", b"3", b"c", b"4", b"d", b"5", b"e"], b":5\r\n"),
    (&[b"ZRANGE", b"s", b"2", b"4", b"BYSCORE"], b"*3\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n"),
    (&[b"ZRANGE", b"s", b"(2", b"4", b"BYSCORE", b"WITHSCORES"], b"*4\r\n$1\r\nc\r\n$1\r\n3\r\n$1\r\nd\r\n$1\r\n4\r\n"),
    (&[b"ZRANGE", b"s", b"2", b"(4", b"BYSCORE"], b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n"),
    (&[b"ZRANGE", b"s", b"-inf", b"+inf", b"BYSCORE", b"LIMIT", b"1", b"2"], b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n"),
    (&[b"ZRANGE", b"s", b"-inf", b"+inf", b"BYSCORE", b"LIMIT", b"-1", b"2"], b"*0\r\n"),
    (&[b"ZRANGE", b"s", b"-inf", b"+inf", b"BYSCORE", b"LIMIT", b"1", b"-1"], b"*4\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n"),
    (&[b"ZRANGE", b"s", b"-inf", b"+inf", b"BYSCORE", b"LIMIT", b"10", b"2"], b"*0\r\n"),
    (&[b"ZRANGE", b"s", b"4", b"2", b"BYSCORE", b"REV"], b"*3\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n"),
    (&[b"ZRANGE", b"s", b"+inf", b"-inf", b"BYSCORE", b"REV", b"LIMIT", b"1", b"2", b"WITHSCORES"], b"*4\r\n$1\r\nd\r\n$1\r\n4\r\n$1\r\nc\r\n$1\r\n3\r\n"),
    (&[b"ZRANGE", b"s", b"4", b"2", b"BYSCORE"], b"*0\r\n"),
    (&[b"ZRANGE", b"s", b"x", b"2", b"BYSCORE"], b"-ERR min or max is not a float\r\n"),
    (&[b"ZRANGE", b"s", b"nan", b"2", b"BYSCORE"], b"-ERR min or max is not a float\r\n"),
    (&[b"ZRANGE", b"s", b" 1", b"2", b"BYSCORE"], b"*2\r\n$1\r\na\r\n$1\r\nb\r\n"),
    (&[b"ZRANGE", b"s", b"(", b"3", b"BYSCORE"], b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"),
    (&[b"ZRANGEBYSCORE", b"s", b"2", b"4"], b"*3\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n"),
    (&[b"ZRANGEBYSCORE", b"s", b"2", b"4", b"WITHSCORES", b"LIMIT", b"0", b"2"], b"*4\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n"),
    (&[b"ZRANGEBYSCORE", b"s", b"2", b"4", b"REV"], b"-ERR syntax error\r\n"),
    (&[b"ZRANGEBYSCORE", b"s", b"2", b"4", b"BYLEX"], b"-ERR syntax error\r\n"),
    (&[b"ZREVRANGEBYSCORE", b"s", b"4", b"2"], b"*3\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n"),
    (&[b"ZREVRANGEBYSCORE", b"s", b"4", b"2", b"WITHSCORES", b"LIMIT", b"1", b"1"], b"*2\r\n$1\r\nc\r\n$1\r\n3\r\n"),
    (&[b"ZREVRANGEBYSCORE", b"s", b"2", b"4"], b"*0\r\n"),
    (&[b"ZCOUNT", b"s", b"2", b"4"], b":3\r\n"),
    (&[b"ZCOUNT", b"s", b"(2", b"(4"], b":1\r\n"),
    (&[b"ZCOUNT", b"s", b"-inf", b"+inf"], b":5\r\n"),
    (&[b"ZCOUNT", b"s", b"x", b"1"], b"-ERR min or max is not a float\r\n"),
    (&[b"ZCOUNT", b"nokey", b"1", b"2"], b":0\r\n"),
    (&[b"ZCOUNT", b"s", b"3", b"3"], b":1\r\n"),
    (&[b"ZCOUNT", b"s", b"(3", b"3"], b":0\r\n"),
    (&[b"DEL", b"l"], b":0\r\n"),
    (&[b"ZADD", b"l", b"0", b"a", b"0", b"b", b"0", b"c", b"0", b"d", b"0", b"e"], b":5\r\n"),
    (&[b"ZRANGE", b"l", b"[b", b"[d", b"BYLEX"], b"*3\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n"),
    (&[b"ZRANGE", b"l", b"(b", b"[d", b"BYLEX"], b"*2\r\n$1\r\nc\r\n$1\r\nd\r\n"),
    (&[b"ZRANGE", b"l", b"-", b"+", b"BYLEX", b"LIMIT", b"1", b"2"], b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n"),
    (&[b"ZRANGE", b"l", b"[d", b"[b", b"BYLEX", b"REV"], b"*3\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n"),
    (&[b"ZRANGE", b"l", b"[b", b"[d", b"BYLEX", b"WITHSCORES"], b"-ERR syntax error, WITHSCORES not supported in combination with BYLEX\r\n"),
    (&[b"ZRANGE", b"l", b"b", b"d", b"BYLEX"], b"-ERR min or max not valid string range item\r\n"),
    (&[b"ZRANGE", b"l", b"[b", b"++", b"BYLEX"], b"-ERR min or max not valid string range item\r\n"),
    (&[b"ZRANGEBYLEX", b"l", b"-", b"(c"], b"*2\r\n$1\r\na\r\n$1\r\nb\r\n"),
    (&[b"ZRANGEBYLEX", b"l", b"[c", b"+"], b"*3\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n"),
    (&[b"ZREVRANGEBYLEX", b"l", b"+", b"-"], b"*5\r\n$1\r\ne\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n"),
    (&[b"ZREVRANGEBYLEX", b"l", b"(d", b"[a", b"LIMIT", b"0", b"2"], b"*2\r\n$1\r\nc\r\n$1\r\nb\r\n"),
    (&[b"ZLEXCOUNT", b"l", b"[b", b"[d"], b":3\r\n"),
    (&[b"ZLEXCOUNT", b"l", b"-", b"+"], b":5\r\n"),
    (&[b"ZLEXCOUNT", b"l", b"x", b"y"], b"-ERR min or max not valid string range item\r\n"),
    (&[b"ZLEXCOUNT", b"l", b"+", b"-"], b":0\r\n"),
    (&[b"ZLEXCOUNT", b"nokey", b"-", b"+"], b":0\r\n"),
    (&[b"ZREM", b"s", b"a", b"nope", b"e"], b":2\r\n"),
    (&[b"ZREM", b"nokey", b"a"], b":0\r\n"),
    (&[b"ZRANGE", b"s", b"0", b"-1"], b"*3\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n"),
    (&[b"ZREMRANGEBYRANK", b"s", b"0", b"0"], b":1\r\n"),
    (&[b"ZREMRANGEBYRANK", b"s", b"x", b"1"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"ZREMRANGEBYSCORE", b"s", b"(3", b"+inf"], b":1\r\n"),
    (&[b"ZREMRANGEBYSCORE", b"s", b"x", b"1"], b"-ERR min or max is not a float\r\n"),
    (&[b"ZRANGE", b"s", b"0", b"-1", b"WITHSCORES"], b"*2\r\n$1\r\nc\r\n$1\r\n3\r\n"),
    (&[b"ZREMRANGEBYLEX", b"l", b"[b", b"(d"], b":2\r\n"),
    (&[b"ZREMRANGEBYLEX", b"l", b"x", b"y"], b"-ERR min or max not valid string range item\r\n"),
    (&[b"ZRANGE", b"l", b"0", b"-1"], b"*3\r\n$1\r\na\r\n$1\r\nd\r\n$1\r\ne\r\n"),
    (&[b"ZREMRANGEBYRANK", b"nokey", b"0", b"1"], b":0\r\n"),
    (&[b"ZREM", b"s", b"c"], b":1\r\n"),
    (&[b"EXISTS", b"s"], b":0\r\n"),
    (&[b"DEL", b"p"], b":0\r\n"),
    (&[b"ZADD", b"p", b"1", b"a", b"2", b"b", b"3", b"c", b"4", b"d"], b":4\r\n"),
    (&[b"ZPOPMIN", b"p"], b"*2\r\n$1\r\na\r\n$1\r\n1\r\n"),
    (&[b"ZPOPMAX", b"p"], b"*2\r\n$1\r\nd\r\n$1\r\n4\r\n"),
    (&[b"ZPOPMIN", b"p", b"0"], b"*0\r\n"),
    (&[b"ZPOPMIN", b"p", b"5"], b"*4\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n"),
    (&[b"EXISTS", b"p"], b":0\r\n"),
    (&[b"ZPOPMIN", b"nokey"], b"*0\r\n"),
    (&[b"ZPOPMIN", b"nokey", b"2"], b"*0\r\n"),
    (&[b"ZPOPMIN", b"p", b"-1"], b"-ERR value is out of range, must be positive\r\n"),
    (&[b"ZPOPMIN", b"p", b"x"], b"-ERR value is out of range, must be positive\r\n"),
    (&[b"ZPOPMIN", b"p", b"1", b"2"], b"-ERR syntax error\r\n"),
    (&[b"ZADD", b"p1", b"1", b"a", b"2", b"b"], b":2\r\n"),
    (&[b"ZADD", b"p2", b"5", b"x"], b":1\r\n"),
    (&[b"ZMPOP", b"2", b"nokey", b"p1", b"MIN"], b"*2\r\n$2\r\np1\r\n*1\r\n*2\r\n$1\r\na\r\n$1\r\n1\r\n"),
    (&[b"ZMPOP", b"2", b"p1", b"p2", b"MAX", b"COUNT", b"5"], b"*2\r\n$2\r\np1\r\n*1\r\n*2\r\n$1\r\nb\r\n$1\r\n2\r\n"),
    (&[b"ZMPOP", b"1", b"p2", b"min", b"count", b"1"], b"*2\r\n$2\r\np2\r\n*1\r\n*2\r\n$1\r\nx\r\n$1\r\n5\r\n"),
    (&[b"ZMPOP", b"1", b"p2", b"MIN"], b"*-1\r\n"),
    (&[b"ZMPOP", b"0", b"p1", b"MIN"], b"-ERR numkeys should be greater than 0\r\n"),
    (&[b"ZMPOP", b"x", b"p1", b"MIN"], b"-ERR numkeys should be greater than 0\r\n"),
    (&[b"ZMPOP", b"2", b"p1", b"MIN"], b"-ERR syntax error\r\n"),
    (&[b"ZMPOP", b"1", b"p1", b"UP"], b"-ERR syntax error\r\n"),
    (&[b"ZMPOP", b"1", b"p1", b"MIN", b"COUNT", b"0"], b"-ERR count should be greater than 0\r\n"),
    (&[b"ZMPOP", b"1", b"p1", b"MIN", b"COUNT", b"x"], b"-ERR count should be greater than 0\r\n"),
    (&[b"ZMPOP", b"1", b"p1", b"MIN", b"COUNT", b"1", b"COUNT", b"2"], b"-ERR syntax error\r\n"),
    (&[b"ZADD", b"r", b"3", b"c", b"1", b"a", b"2", b"b"], b":3\r\n"),
    (&[b"ZRANDMEMBER", b"r", b"10"], b"*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n"),
    (&[b"ZRANDMEMBER", b"r", b"3", b"WITHSCORES"], b"*6\r\n$1\r\nc\r\n$1\r\n3\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\na\r\n$1\r\n1\r\n"),
    (&[b"ZRANDMEMBER", b"r", b"0"], b"*0\r\n"),
    (&[b"ZRANDMEMBER", b"r", b"x"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"ZRANDMEMBER", b"r", b"1", b"BAD"], b"-ERR syntax error\r\n"),
    (&[b"ZRANDMEMBER", b"r", b"1", b"WITHSCORES", b"x"], b"-ERR syntax error\r\n"),
    (&[b"ZRANDMEMBER", b"r", b"9223372036854775807", b"WITHSCORES"], b"-ERR value is out of range\r\n"),
    (&[b"ZRANDMEMBER", b"r", b"-9223372036854775808"], b"-ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807\r\n"),
    (&[b"ZRANDMEMBER", b"nokey"], b"$-1\r\n"),
    (&[b"ZRANDMEMBER", b"nokey", b"3"], b"*0\r\n"),
    (&[b"ZSCAN", b"r", b"0"], b"*2\r\n$1\r\n0\r\n*6\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n"),
    (&[b"ZSCAN", b"r", b"0", b"MATCH", b"[ab]"], b"*2\r\n$1\r\n0\r\n*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n"),
    (&[b"ZSCAN", b"r", b"0", b"COUNT", b"1"], b"*2\r\n$1\r\n0\r\n*6\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n"),
    (&[b"ZSCAN", b"r", b"abc"], b"-ERR invalid cursor\r\n"),
    (&[b"ZSCAN", b"nokey", b"0"], b"*2\r\n$1\r\n0\r\n*0\r\n"),
    // Redis 7.2 has no NOVALUES (a syntax error); 7.4, which added it, says this.
    (&[b"ZSCAN", b"r", b"0", b"NOVALUES"], b"-ERR NOVALUES option can only be used in HSCAN\r\n"),
    (&[b"DEL", b"u1", b"u2", b"u3"], b":0\r\n"),
    (&[b"ZADD", b"u1", b"1", b"a", b"2", b"b", b"3", b"c"], b":3\r\n"),
    (&[b"ZADD", b"u2", b"10", b"b", b"20", b"c", b"30", b"d"], b":3\r\n"),
    (&[b"SADD", b"u3", b"c", b"d", b"e"], b":3\r\n"),
    (&[b"ZUNION", b"2", b"u1", b"u2"], b"*4\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n"),
    (&[b"ZUNION", b"2", b"u1", b"u2", b"WITHSCORES"], b"*8\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$2\r\n12\r\n$1\r\nc\r\n$2\r\n23\r\n$1\r\nd\r\n$2\r\n30\r\n"),
    (&[b"ZUNION", b"2", b"u1", b"u2", b"WEIGHTS", b"2", b"0.5", b"WITHSCORES"], b"*8\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n9\r\n$1\r\nd\r\n$2\r\n15\r\n$1\r\nc\r\n$2\r\n16\r\n"),
    (&[b"ZUNION", b"2", b"u1", b"u2", b"AGGREGATE", b"MIN", b"WITHSCORES"], b"*8\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n$1\r\nd\r\n$2\r\n30\r\n"),
    (&[b"ZUNION", b"2", b"u1", b"u2", b"AGGREGATE", b"MAX", b"WITHSCORES"], b"*8\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$2\r\n10\r\n$1\r\nc\r\n$2\r\n20\r\n$1\r\nd\r\n$2\r\n30\r\n"),
    (&[b"ZUNION", b"3", b"u1", b"u2", b"u3", b"WITHSCORES"], b"*10\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\ne\r\n$1\r\n1\r\n$1\r\nb\r\n$2\r\n12\r\n$1\r\nc\r\n$2\r\n24\r\n$1\r\nd\r\n$2\r\n31\r\n"),
    (&[b"ZINTER", b"2", b"u1", b"u2", b"WITHSCORES"], b"*4\r\n$1\r\nb\r\n$2\r\n12\r\n$1\r\nc\r\n$2\r\n23\r\n"),
    (&[b"ZINTER", b"3", b"u1", b"u2", b"u3", b"WITHSCORES"], b"*2\r\n$1\r\nc\r\n$2\r\n24\r\n"),
    (&[b"ZINTER", b"2", b"u1", b"nokey"], b"*0\r\n"),
    (&[b"ZDIFF", b"2", b"u1", b"u2", b"WITHSCORES"], b"*2\r\n$1\r\na\r\n$1\r\n1\r\n"),
    (&[b"ZDIFF", b"2", b"u2", b"u3", b"WITHSCORES"], b"*2\r\n$1\r\nb\r\n$2\r\n10\r\n"),
    (&[b"ZDIFF", b"2", b"u1", b"u2", b"WEIGHTS", b"1", b"2"], b"-ERR syntax error\r\n"),
    (&[b"ZUNION", b"0", b"u1"], b"-ERR at least 1 input key is needed for 'zunion' command\r\n"),
    (&[b"ZUNION", b"x", b"u1"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"ZUNION", b"3", b"u1", b"u2"], b"-ERR syntax error\r\n"),
    (&[b"ZUNION", b"2", b"u1", b"u2", b"WEIGHTS", b"1"], b"-ERR syntax error\r\n"),
    (&[b"ZUNION", b"2", b"u1", b"u2", b"WEIGHTS", b"1", b"x"], b"-ERR weight value is not a float\r\n"),
    (&[b"ZUNION", b"2", b"u1", b"u2", b"AGGREGATE", b"AVG"], b"-ERR syntax error\r\n"),
    (&[b"ZUNION", b"2", b"u1", b"u2", b"BAD"], b"-ERR syntax error\r\n"),
    (&[b"ZUNIONSTORE", b"d", b"2", b"u1", b"u2", b"WITHSCORES"], b"-ERR syntax error\r\n"),
    (&[b"ZUNIONSTORE", b"d", b"2", b"u1", b"u2", b"WEIGHTS", b"1", b"2"], b":4\r\n"),
    (&[b"ZRANGE", b"d", b"0", b"-1", b"WITHSCORES"], b"*8\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$2\r\n22\r\n$1\r\nc\r\n$2\r\n43\r\n$1\r\nd\r\n$2\r\n60\r\n"),
    (&[b"ZINTERSTORE", b"d", b"2", b"u1", b"nokey"], b":0\r\n"),
    (&[b"EXISTS", b"d"], b":0\r\n"),
    (&[b"ZDIFFSTORE", b"d", b"2", b"u2", b"u1"], b":1\r\n"),
    (&[b"ZRANGE", b"d", b"0", b"-1", b"WITHSCORES"], b"*2\r\n$1\r\nd\r\n$2\r\n30\r\n"),
    (&[b"ZINTERCARD", b"2", b"u1", b"u2"], b":2\r\n"),
    (&[b"ZINTERCARD", b"2", b"u1", b"u2", b"LIMIT", b"1"], b":1\r\n"),
    (&[b"ZINTERCARD", b"3", b"u1", b"u2", b"u3"], b":1\r\n"),
    (&[b"ZINTERCARD", b"0", b"u1"], b"-ERR at least 1 input key is needed for 'zintercard' command\r\n"),
    (&[b"ZINTERCARD", b"2", b"u1", b"u2", b"LIMIT", b"-1"], b"-ERR LIMIT can't be negative\r\n"),
    (&[b"ZADD", b"inf1", b"inf", b"a"], b":1\r\n"),
    (&[b"ZADD", b"inf2", b"-inf", b"a"], b":1\r\n"),
    (&[b"ZUNION", b"2", b"inf1", b"inf2", b"WITHSCORES"], b"*2\r\n$1\r\na\r\n$1\r\n0\r\n"),
    (&[b"ZUNION", b"2", b"inf1", b"inf2", b"WEIGHTS", b"0", b"1", b"WITHSCORES"], b"*2\r\n$1\r\na\r\n$4\r\n-inf\r\n"),
    (&[b"SET", b"str", b"v"], b"+OK\r\n"),
    (&[b"ZUNION", b"2", b"u1", b"str"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"ZUNION", b"2", b"u1", b"str", b"BAD"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"ZRANGESTORE", b"dst", b"u2", b"0", b"1"], b":2\r\n"),
    (&[b"ZRANGE", b"dst", b"0", b"-1", b"WITHSCORES"], b"*4\r\n$1\r\nb\r\n$2\r\n10\r\n$1\r\nc\r\n$2\r\n20\r\n"),
    (&[b"ZRANGESTORE", b"dst", b"u2", b"(10", b"+inf", b"BYSCORE", b"LIMIT", b"0", b"1"], b":1\r\n"),
    (&[b"ZRANGE", b"dst", b"0", b"-1", b"WITHSCORES"], b"*2\r\n$1\r\nc\r\n$2\r\n20\r\n"),
    (&[b"ZRANGESTORE", b"dst", b"u2", b"0", b"-1", b"WITHSCORES"], b"-ERR syntax error\r\n"),
    (&[b"ZRANGESTORE", b"dst", b"nokey", b"0", b"-1"], b":0\r\n"),
    (&[b"EXISTS", b"dst"], b":0\r\n"),
    (&[b"ZADD", b"str", b"1", b"a"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"ZSCORE", b"str", b"a"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"ZRANGE", b"str", b"0", b"-1"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"ZRANK", b"str", b"a"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"ZREM", b"str", b"a"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"ZCARD", b"str"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"ZPOPMIN", b"str"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"ZMPOP", b"1", b"str", b"MIN"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"ZINCRBY", b"str", b"1", b"a"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"ZCOUNT", b"str", b"1", b"2"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"ZRANDMEMBER", b"str"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SADD", b"u1", b"a"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"GET", b"u1"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"TYPE", b"u1"], b"+zset\r\n"),
    (&[b"ZADD", b"f", b"0.1", b"a", b"1e21", b"b", b"1e-7", b"c", b"-0", b"d", b"123456789012345678", b"e", b"1.7976931348623157e308", b"f", b"0x10", b"g"], b":7\r\n"),
    (&[b"ZRANGE", b"f", b"0", b"-1", b"WITHSCORES"], b"*14\r\n$1\r\nd\r\n$1\r\n0\r\n$1\r\nc\r\n$4\r\n1e-7\r\n$1\r\na\r\n$3\r\n0.1\r\n$1\r\ng\r\n$2\r\n16\r\n$1\r\ne\r\n$18\r\n123456789012345680\r\n$1\r\nb\r\n$5\r\n1e+21\r\n$1\r\nf\r\n$23\r\n1.7976931348623157e+308\r\n"),
    (&[b"ZSCORE", b"f", b"c"], b"$4\r\n1e-7\r\n"),
    (&[b"ZINCRBY", b"f", b"0.2", b"a"], b"$19\r\n0.30000000000000004\r\n"),
    (&[b"ZADD", b"t", b"1", b"a"], b":1\r\n"),
    (&[b"EXPIRE", b"t", b"100"], b":1\r\n"),
    (&[b"ZADD", b"t", b"2", b"b"], b":1\r\n"),
    (&[b"ZREM", b"t", b"a"], b":1\r\n"),
    (&[b"TTL", b"t"], b":100\r\n"),
    (&[b"DBSIZE"], b":13\r\n"),
    // Redis reports its own client id here.
    (&[b"HELLO", b"3"], b"%7\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.2.4\r\n$5\r\nproto\r\n:3\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n"),
    (&[b"ZRANGE", b"u2", b"0", b"-1", b"WITHSCORES"], b"*3\r\n*2\r\n$1\r\nb\r\n,10\r\n*2\r\n$1\r\nc\r\n,20\r\n*2\r\n$1\r\nd\r\n,30\r\n"),
    (&[b"ZRANGE", b"u2", b"0", b"-1"], b"*3\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n"),
    (&[b"ZSCORE", b"u2", b"b"], b",10\r\n"),
    (&[b"ZSCORE", b"u2", b"nope"], b"_\r\n"),
    (&[b"ZMSCORE", b"u2", b"b", b"nope"], b"*2\r\n,10\r\n_\r\n"),
    (&[b"ZINCRBY", b"u2", b"1", b"b"], b",11\r\n"),
    (&[b"ZADD", b"u2", b"INCR", b"1", b"b"], b",12\r\n"),
    (&[b"ZADD", b"u2", b"NX", b"INCR", b"1", b"b"], b"_\r\n"),
    (&[b"ZRANK", b"u2", b"b", b"WITHSCORE"], b"*2\r\n:0\r\n,12\r\n"),
    (&[b"ZRANK", b"u2", b"nope", b"WITHSCORE"], b"_\r\n"),
    (&[b"ZPOPMIN", b"u2"], b"*2\r\n$1\r\nb\r\n,12\r\n"),
    (&[b"ZPOPMIN", b"u2", b"1"], b"*1\r\n*2\r\n$1\r\nc\r\n,20\r\n"),
    (&[b"ZPOPMAX", b"nokey", b"2"], b"*0\r\n"),
    (&[b"ZMPOP", b"1", b"u2", b"MAX"], b"*2\r\n$2\r\nu2\r\n*1\r\n*2\r\n$1\r\nd\r\n,30\r\n"),
    (&[b"ZMPOP", b"1", b"nokey", b"MAX"], b"_\r\n"),
    (&[b"ZRANDMEMBER", b"r", b"3", b"WITHSCORES"], b"*3\r\n*2\r\n$1\r\nc\r\n,3\r\n*2\r\n$1\r\nb\r\n,2\r\n*2\r\n$1\r\na\r\n,1\r\n"),
    (&[b"ZUNION", b"2", b"u1", b"u2", b"WITHSCORES"], b"*3\r\n*2\r\n$1\r\na\r\n,1\r\n*2\r\n$1\r\nb\r\n,2\r\n*2\r\n$1\r\nc\r\n,3\r\n"),
    (&[b"ZSCAN", b"r", b"0"], b"*2\r\n$1\r\n0\r\n*6\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n"),
    (&[b"ZRANGEBYSCORE", b"r", b"-inf", b"+inf", b"WITHSCORES"], b"*3\r\n*2\r\n$1\r\na\r\n,1\r\n*2\r\n$1\r\nb\r\n,2\r\n*2\r\n$1\r\nc\r\n,3\r\n"),
];

#[tokio::test]
async fn replies_match_redis() {
    common::replay(SCRIPT).await;
}

/// A sorted set's entries, by score then member.
fn sorted(model: &HashMap<Vec<u8>, f64>) -> Vec<(f64, Vec<u8>)> {
    let mut all: Vec<(f64, Vec<u8>)> = model.iter().map(|(m, s)| (*s, m.clone())).collect();
    all.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap().then_with(|| a.1.cmp(&b.1)));
    all
}

fn score(value: &Value) -> f64 {
    match value {
        Value::Bulk(b) => std::str::from_utf8(b).unwrap().parse().unwrap(),
        other => panic!("not a score: {other:?}"),
    }
}

/// Members and scores from a flat WITHSCORES reply.
fn pairs(value: &Value) -> Vec<(f64, Vec<u8>)> {
    let Value::Array(items) = value else {
        panic!("not an array: {value:?}")
    };
    items
        .chunks(2)
        .map(|pair| match &pair[0] {
            Value::Bulk(m) => (score(&pair[1]), m.clone()),
            other => panic!("not a member: {other:?}"),
        })
        .collect()
}

/// Random sorted set commands through `cmd::execute` against a model, with
/// sets growing past the packed limits and back.
#[test]
fn matches_a_model() {
    let ctx = Context::new(Arc::new(Store::new(4)), Settings::default());
    let client = ctx.connect(-1).unwrap();
    let mut session = client.session();
    let mut model: HashMap<Vec<u8>, HashMap<Vec<u8>, f64>> = HashMap::new();
    let mut rng = 0x9E37_79B9_7F4A_7C15u64;
    let mut below = move |n: u64| {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng % n.max(1)
    };
    let mut biggest = 0;
    for step in 0..10_000 {
        biggest = model
            .values()
            .map(HashMap::len)
            .max()
            .unwrap_or(0)
            .max(biggest);
        let key = format!("z{}", below(3)).into_bytes();
        let other = format!("z{}", below(3)).into_bytes();
        let count = if below(15) == 0 {
            150
        } else {
            1 + below(3) as usize
        };
        let members: Vec<Vec<u8>> = (0..count)
            .map(|_| match below(30) {
                0 => vec![b'L'; 70],
                _ => format!("m{}", below(400)).into_bytes(),
            })
            .collect();
        let scores: Vec<f64> = (0..count)
            .map(|_| (below(80) as f64 - 20.0) / 4.0)
            .collect();
        let mut run =
            |args: Vec<&[u8]>| common::parse(&common::execute(&ctx, &mut session, 1, &args));
        let label = format!("step {step}");
        let set = model.get(&key).cloned().unwrap_or_default();
        let put = |model: &mut HashMap<Vec<u8>, HashMap<Vec<u8>, f64>>,
                   key: &[u8],
                   set: HashMap<Vec<u8>, f64>| {
            if set.is_empty() {
                model.remove(key);
            } else {
                model.insert(key.to_vec(), set);
            }
        };
        match below(12) {
            0..3 => {
                let flag = [&b""[..], b"NX", b"XX", b"GT", b"LT"][below(5) as usize];
                let texts: Vec<String> = scores.iter().map(|s| s.to_string()).collect();
                let mut args = vec![&b"ZADD"[..], &key];
                if !flag.is_empty() {
                    args.push(flag);
                }
                args.push(b"CH");
                for (s, m) in texts.iter().zip(&members) {
                    args.extend([s.as_bytes(), &m[..]]);
                }
                let mut set = set;
                let mut changed = 0;
                if !(flag == b"XX" && set.is_empty() && !model.contains_key(&key)) {
                    for (s, m) in scores.iter().zip(&members) {
                        match set.get(m).copied() {
                            Some(cur) => {
                                let apply = match flag {
                                    b"NX" => false,
                                    b"GT" => *s > cur,
                                    b"LT" => *s < cur,
                                    _ => true,
                                };
                                if apply && *s != cur {
                                    set.insert(m.clone(), *s);
                                    changed += 1;
                                }
                            }
                            None if flag == b"XX" => {}
                            None => {
                                set.insert(m.clone(), *s);
                                changed += 1;
                            }
                        }
                    }
                }
                assert_eq!(run(args).int(), changed, "{label}");
                put(&mut model, &key, set);
            }
            3 => {
                let by = (below(5) as f64 - 2.0) / 2.0;
                let got = run(vec![
                    b"ZINCRBY",
                    &key,
                    by.to_string().as_bytes(),
                    &members[0],
                ]);
                let mut set = set;
                let new = set.get(&members[0]).copied().unwrap_or(0.0) + by;
                set.insert(members[0].clone(), new);
                assert_eq!(score(&got), new, "{label}");
                put(&mut model, &key, set);
            }
            4 => {
                let mut args = vec![&b"ZREM"[..], &key];
                args.extend(members.iter().map(Vec::as_slice));
                let mut set = set;
                let gone: std::collections::HashSet<&Vec<u8>> = members.iter().collect();
                let removed = gone.iter().filter(|m| set.remove(**m).is_some()).count();
                assert_eq!(run(args).int(), removed as i64, "{label}");
                put(&mut model, &key, set);
            }
            5 => {
                let all = sorted(&set);
                let m = &members[0];
                let rank = all.iter().position(|(_, x)| x == m);
                let got = run(vec![b"ZRANK", &key, m]);
                match rank {
                    Some(r) => assert_eq!(got.int(), r as i64, "{label}"),
                    None => assert_eq!(got, Value::Nil, "{label}"),
                }
                let got = run(vec![b"ZREVRANK", &key, m]);
                match rank {
                    Some(r) => assert_eq!(got.int(), (all.len() - 1 - r) as i64, "{label}"),
                    None => assert_eq!(got, Value::Nil, "{label}"),
                }
            }
            6 => {
                let (start, stop) = (below(10) as i64 - 4, below(10) as i64 - 3);
                let rev = below(2) == 0;
                let mut all = sorted(&set);
                if rev {
                    all.reverse();
                }
                let len = all.len() as i64;
                let s = if start < 0 {
                    (len + start).max(0)
                } else {
                    start
                };
                let e = if stop < 0 {
                    len + stop
                } else {
                    stop.min(len - 1)
                };
                let expected: Vec<(f64, Vec<u8>)> = if s > e || s >= len {
                    Vec::new()
                } else {
                    all[s as usize..=e as usize].to_vec()
                };
                let cmd: &[u8] = if rev { b"ZREVRANGE" } else { b"ZRANGE" };
                let got = run(vec![
                    cmd,
                    &key,
                    start.to_string().as_bytes(),
                    stop.to_string().as_bytes(),
                    b"WITHSCORES",
                ]);
                assert_eq!(pairs(&got), expected, "{label}");
            }
            7 => {
                let (min, max) = (
                    (below(40) as f64 - 20.0) / 4.0,
                    (below(40) as f64 - 10.0) / 4.0,
                );
                let (offset, n) = (below(4) as i64, below(6) as i64 - 1);
                let all = sorted(&set);
                let within: Vec<(f64, Vec<u8>)> = all
                    .iter()
                    .filter(|(s, _)| *s > min && *s <= max)
                    .cloned()
                    .collect();
                let expected: Vec<(f64, Vec<u8>)> = within
                    .iter()
                    .skip(offset as usize)
                    .take(if n < 0 { usize::MAX } else { n as usize })
                    .cloned()
                    .collect();
                let got = run(vec![
                    b"ZRANGEBYSCORE",
                    &key,
                    format!("({min}").as_bytes(),
                    max.to_string().as_bytes(),
                    b"WITHSCORES",
                    b"LIMIT",
                    offset.to_string().as_bytes(),
                    n.to_string().as_bytes(),
                ]);
                assert_eq!(pairs(&got), expected, "{label}");
                let count = run(vec![
                    b"ZCOUNT",
                    &key,
                    format!("({min}").as_bytes(),
                    max.to_string().as_bytes(),
                ]);
                assert_eq!(count.int(), within.len() as i64, "{label}");
            }
            8 => {
                let n = below(4);
                let max = below(2) == 0;
                let mut all = sorted(&set);
                if max {
                    all.reverse();
                }
                let popped: Vec<(f64, Vec<u8>)> = all.into_iter().take(n as usize).collect();
                let got = run(vec![
                    if max { b"ZPOPMAX" } else { b"ZPOPMIN" },
                    &key,
                    n.to_string().as_bytes(),
                ]);
                assert_eq!(pairs(&got), popped, "{label}");
                let mut set = set;
                for (_, m) in &popped {
                    set.remove(m);
                }
                put(&mut model, &key, set);
            }
            9 => {
                let (start, stop) = (below(4) as i64, below(6) as i64 - 2);
                let all = sorted(&set);
                let len = all.len() as i64;
                let s = start.min(len);
                let e = if stop < 0 {
                    len + stop
                } else {
                    stop.min(len - 1)
                };
                let gone: Vec<(f64, Vec<u8>)> = if s > e || s >= len {
                    Vec::new()
                } else {
                    all[s as usize..=e as usize].to_vec()
                };
                let got = run(vec![
                    b"ZREMRANGEBYRANK",
                    &key,
                    start.to_string().as_bytes(),
                    stop.to_string().as_bytes(),
                ]);
                assert_eq!(got.int(), gone.len() as i64, "{label}");
                let mut set = set;
                for (_, m) in &gone {
                    set.remove(m);
                }
                put(&mut model, &key, set);
            }
            10 => {
                let b = model.get(&other).cloned().unwrap_or_default();
                let mut union = set.clone();
                for (m, s) in &b {
                    *union.entry(m.clone()).or_insert(0.0) +=
                        if set.contains_key(m) { *s } else { 0.0 };
                    if !set.contains_key(m) {
                        union.insert(m.clone(), *s);
                    }
                }
                let dst = b"zdst".to_vec();
                let got = run(vec![b"ZUNIONSTORE", &dst, b"2", &key, &other]);
                assert_eq!(got.int(), union.len() as i64, "{label}");
                let stored = run(vec![b"ZRANGE", &dst, b"0", b"-1", b"WITHSCORES"]);
                assert_eq!(pairs(&stored), sorted(&union), "{label}");
                put(&mut model, &dst, union);
            }
            _ => {
                assert_eq!(run(vec![b"ZCARD", &key]).int(), set.len() as i64, "{label}");
                let got = run(vec![b"ZSCORE", &key, &members[0]]);
                match set.get(&members[0]) {
                    Some(s) => assert_eq!(score(&got), *s, "{label}"),
                    None => assert_eq!(got, Value::Nil, "{label}"),
                }
            }
        }
    }
    assert!(biggest > 200, "no big sorted set: {biggest}");
}
