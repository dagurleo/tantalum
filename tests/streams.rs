//! Streams: replies recorded from Redis 7.2.4 (`tools/redis-diff.py`
//! style, on one connection), a recorded run of 2,200 random commands
//! (`tests/data/streams-random.txt`: each command with the CRC-32 and
//! length of Redis' reply), and blocking XREAD in every I/O mode.

mod common;

use std::time::Duration;

use common::{Client, MODES, start};
use tantalum::context::Context;

#[rustfmt::skip]
const SCRIPT: &[(&[&[u8]], &[u8])] = &[
    (&[b"FLUSHALL"], b"+OK\r\n"),
    (&[b"XADD", b"s", b"1-1", b"f", b"v"], b"$3\r\n1-1\r\n"),
    (&[b"XADD", b"s", b"1-1", b"f", b"v"], b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"),
    (&[b"XADD", b"s", b"1-0", b"f", b"v"], b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"),
    (&[b"XADD", b"s", b"0-0", b"f", b"v"], b"-ERR The ID specified in XADD must be greater than 0-0\r\n"),
    (&[b"XADD", b"s", b"1-2", b"f", b"v", b"g", b"w"], b"$3\r\n1-2\r\n"),
    (&[b"XADD", b"s", b"2", b"f", b"v"], b"$3\r\n2-0\r\n"),
    (&[b"XADD", b"s", b"2-*", b"f", b"v"], b"$3\r\n2-1\r\n"),
    (&[b"XADD", b"s", b"3-*", b"f", b"v"], b"$3\r\n3-0\r\n"),
    (&[b"XADD", b"s", b"abc", b"f", b"v"], b"-ERR Invalid stream ID specified as stream command argument\r\n"),
    (&[b"XADD", b"s", b"1-x", b"f", b"v"], b"-ERR Invalid stream ID specified as stream command argument\r\n"),
    (&[b"XADD", b"s", b"-1", b"f", b"v"], b"-ERR Invalid stream ID specified as stream command argument\r\n"),
    (&[b"XADD", b"s", b"18446744073709551615-18446744073709551615", b"f", b"v"], b"$41\r\n18446744073709551615-18446744073709551615\r\n"),
    (&[b"XADD", b"s", b"18446744073709551615-*", b"f", b"v"], b"-ERR The stream has exhausted the last possible ID, unable to add more items\r\n"),
    (&[b"XADD", b"s", b"18446744073709551616", b"f", b"v"], b"-ERR Invalid stream ID specified as stream command argument\r\n"),
    (&[b"XADD", b"s", b"5-1", b"f"], b"-ERR wrong number of arguments for 'xadd' command\r\n"),
    (&[b"XADD", b"s", b"5-1"], b"-ERR wrong number of arguments for 'xadd' command\r\n"),
    (&[b"XADD", b"s", b"NOMKSTREAM", b"6-1", b"f", b"v"], b"-ERR The stream has exhausted the last possible ID, unable to add more items\r\n"),
    (&[b"XADD", b"nope", b"NOMKSTREAM", b"*", b"f", b"v"], b"$-1\r\n"),
    (&[b"EXISTS", b"nope"], b":0\r\n"),
    (&[b"XADD", b"s2", b"MAXLEN", b"2", b"1-1", b"a", b"1"], b"$3\r\n1-1\r\n"),
    (&[b"XADD", b"s2", b"MAXLEN", b"2", b"2-1", b"a", b"2"], b"$3\r\n2-1\r\n"),
    (&[b"XADD", b"s2", b"MAXLEN", b"2", b"3-1", b"a", b"3"], b"$3\r\n3-1\r\n"),
    (&[b"XRANGE", b"s2", b"-", b"+"], b"*2\r\n*2\r\n$3\r\n2-1\r\n*2\r\n$1\r\na\r\n$1\r\n2\r\n*2\r\n$3\r\n3-1\r\n*2\r\n$1\r\na\r\n$1\r\n3\r\n"),
    (&[b"XADD", b"s2", b"MAXLEN", b"=", b"2", b"4-1", b"a", b"4"], b"$3\r\n4-1\r\n"),
    (&[b"XADD", b"s2", b"MAXLEN", b"~", b"2", b"5-1", b"a", b"5"], b"$3\r\n5-1\r\n"),
    (&[b"XLEN", b"s2"], b":3\r\n"),
    (&[b"XADD", b"s2", b"MINID", b"4", b"6-1", b"a", b"6"], b"$3\r\n6-1\r\n"),
    (&[b"XRANGE", b"s2", b"-", b"+"], b"*3\r\n*2\r\n$3\r\n4-1\r\n*2\r\n$1\r\na\r\n$1\r\n4\r\n*2\r\n$3\r\n5-1\r\n*2\r\n$1\r\na\r\n$1\r\n5\r\n*2\r\n$3\r\n6-1\r\n*2\r\n$1\r\na\r\n$1\r\n6\r\n"),
    (&[b"XADD", b"s2", b"MAXLEN", b"-1", b"7-1", b"a", b"7"], b"-ERR The MAXLEN argument must be >= 0.\r\n"),
    (&[b"XADD", b"s2", b"MAXLEN", b"x", b"7-1", b"a", b"7"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"XADD", b"s2", b"MAXLEN", b"1", b"LIMIT", b"5", b"7-1", b"a", b"7"], b"-ERR syntax error, LIMIT cannot be used without the special ~ option\r\n"),
    (&[b"XADD", b"s2", b"MAXLEN", b"~", b"1", b"LIMIT", b"5", b"8-1", b"a", b"8"], b"$3\r\n8-1\r\n"),
    (&[b"XADD", b"s2", b"MAXLEN", b"~", b"1", b"LIMIT", b"-1", b"9-1", b"a", b"8"], b"-ERR The LIMIT argument must be >= 0.\r\n"),
    (&[b"XADD", b"s2", b"MINID", b"x", b"9-1", b"a", b"9"], b"-ERR Invalid stream ID specified as stream command argument\r\n"),
    (&[b"XADD", b"s2", b"MAXLEN", b"1", b"MINID", b"1", b"9-1", b"a", b"9"], b"-ERR syntax error, MAXLEN and MINID options at the same time are not compatible\r\n"),
    (&[b"XADD", b"s2", b"MAXLEN", b"0", b"10-1", b"a", b"10"], b"$4\r\n10-1\r\n"),
    (&[b"XLEN", b"s2"], b":0\r\n"),
    (&[b"EXISTS", b"s2"], b":1\r\n"),
    (&[b"XRANGE", b"s2", b"-", b"+"], b"*0\r\n"),
    (&[b"XADD", b"s2", b"9-1", b"a", b"1"], b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"),
    (&[b"XLEN", b"nope"], b":0\r\n"),
    (&[b"XLEN", b"s"], b":6\r\n"),
    (&[b"XRANGE", b"s", b"-", b"+"], b"*6\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n1-2\r\n*4\r\n$1\r\nf\r\n$1\r\nv\r\n$1\r\ng\r\n$1\r\nw\r\n*2\r\n$3\r\n2-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n2-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n3-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$41\r\n18446744073709551615-18446744073709551615\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"XRANGE", b"s", b"-", b"+", b"COUNT", b"2"], b"*2\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n1-2\r\n*4\r\n$1\r\nf\r\n$1\r\nv\r\n$1\r\ng\r\n$1\r\nw\r\n"),
    (&[b"XRANGE", b"s", b"-", b"+", b"COUNT", b"0"], b"*-1\r\n"),
    (&[b"XRANGE", b"s", b"-", b"+", b"COUNT", b"-1"], b"*-1\r\n"),
    (&[b"XRANGE", b"s", b"-", b"+", b"COUNT", b"x"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"XRANGE", b"s", b"1", b"1"], b"*2\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n1-2\r\n*4\r\n$1\r\nf\r\n$1\r\nv\r\n$1\r\ng\r\n$1\r\nw\r\n"),
    (&[b"XRANGE", b"s", b"1-2", b"+"], b"*5\r\n*2\r\n$3\r\n1-2\r\n*4\r\n$1\r\nf\r\n$1\r\nv\r\n$1\r\ng\r\n$1\r\nw\r\n*2\r\n$3\r\n2-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n2-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n3-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$41\r\n18446744073709551615-18446744073709551615\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"XRANGE", b"s", b"(1-1", b"+"], b"*5\r\n*2\r\n$3\r\n1-2\r\n*4\r\n$1\r\nf\r\n$1\r\nv\r\n$1\r\ng\r\n$1\r\nw\r\n*2\r\n$3\r\n2-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n2-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n3-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$41\r\n18446744073709551615-18446744073709551615\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"XRANGE", b"s", b"-", b"(2-0"], b"*2\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n1-2\r\n*4\r\n$1\r\nf\r\n$1\r\nv\r\n$1\r\ng\r\n$1\r\nw\r\n"),
    (&[b"XRANGE", b"s", b"(-", b"+"], b"-ERR Invalid stream ID specified as stream command argument\r\n"),
    (&[b"XRANGE", b"s", b"-", b"(+"], b"-ERR Invalid stream ID specified as stream command argument\r\n"),
    (&[b"XRANGE", b"s", b"3", b"1"], b"*0\r\n"),
    (&[b"XRANGE", b"s", b"x", b"+"], b"-ERR Invalid stream ID specified as stream command argument\r\n"),
    (&[b"XRANGE", b"s", b"-", b"+", b"COUNT"], b"-ERR syntax error\r\n"),
    (&[b"XRANGE", b"s", b"-", b"+", b"FOO", b"1"], b"-ERR syntax error\r\n"),
    (&[b"XRANGE", b"nope", b"-", b"+"], b"*0\r\n"),
    (&[b"XREVRANGE", b"s", b"+", b"-"], b"*6\r\n*2\r\n$41\r\n18446744073709551615-18446744073709551615\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n3-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n2-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n2-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n1-2\r\n*4\r\n$1\r\nf\r\n$1\r\nv\r\n$1\r\ng\r\n$1\r\nw\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"XREVRANGE", b"s", b"+", b"-", b"COUNT", b"1"], b"*1\r\n*2\r\n$41\r\n18446744073709551615-18446744073709551615\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"XREVRANGE", b"s", b"2", b"1"], b"*4\r\n*2\r\n$3\r\n2-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n2-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n1-2\r\n*4\r\n$1\r\nf\r\n$1\r\nv\r\n$1\r\ng\r\n$1\r\nw\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"XREVRANGE", b"s", b"(2-1", b"-"], b"*3\r\n*2\r\n$3\r\n2-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n1-2\r\n*4\r\n$1\r\nf\r\n$1\r\nv\r\n$1\r\ng\r\n$1\r\nw\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"XDEL", b"s", b"1-1", b"99-1"], b":1\r\n"),
    (&[b"XDEL", b"s", b"x"], b"-ERR Invalid stream ID specified as stream command argument\r\n"),
    (&[b"XDEL", b"nope", b"1-1"], b":0\r\n"),
    (&[b"XRANGE", b"s", b"-", b"+"], b"*5\r\n*2\r\n$3\r\n1-2\r\n*4\r\n$1\r\nf\r\n$1\r\nv\r\n$1\r\ng\r\n$1\r\nw\r\n*2\r\n$3\r\n2-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n2-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n3-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$41\r\n18446744073709551615-18446744073709551615\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"XTRIM", b"s", b"MAXLEN", b"2"], b":3\r\n"),
    (&[b"XTRIM", b"s", b"MAXLEN", b"2"], b":0\r\n"),
    (&[b"XTRIM", b"s", b"MINID", b"2-1"], b":0\r\n"),
    (&[b"XTRIM", b"s", b"MINID", b"=", b"99"], b":1\r\n"),
    (&[b"XLEN", b"s"], b":1\r\n"),
    (&[b"EXISTS", b"s"], b":1\r\n"),
    (&[b"XTRIM", b"s"], b"-ERR wrong number of arguments for 'xtrim' command\r\n"),
    (&[b"XTRIM", b"s", b"FOO", b"1"], b"-ERR syntax error\r\n"),
    (&[b"XTRIM", b"nope", b"MAXLEN", b"1"], b":0\r\n"),
    (&[b"XTRIM", b"s", b"MAXLEN", b"~", b"0", b"LIMIT", b"0"], b":1\r\n"),
    (&[b"SET", b"str", b"x"], b"+OK\r\n"),
    (&[b"XADD", b"str", b"*", b"f", b"v"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"XLEN", b"str"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"XRANGE", b"str", b"-", b"+"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"XREAD", b"STREAMS", b"str", b"0"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"TYPE", b"s"], b"+stream\r\n"),
    (&[b"XADD", b"z", b"5-0", b"f", b"v"], b"$3\r\n5-0\r\n"),
    (&[b"XREAD", b"STREAMS", b"s", b"0"], b"*-1\r\n"),
    (&[b"XREAD", b"STREAMS", b"s", b"z", b"0", b"0"], b"*1\r\n*2\r\n$1\r\nz\r\n*1\r\n*2\r\n$3\r\n5-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"XREAD", b"COUNT", b"1", b"STREAMS", b"s", b"z", b"0", b"0"], b"*1\r\n*2\r\n$1\r\nz\r\n*1\r\n*2\r\n$3\r\n5-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"XREAD", b"STREAMS", b"s", b"$"], b"*-1\r\n"),
    (&[b"XREAD", b"STREAMS", b"s"], b"-ERR wrong number of arguments for 'xread' command\r\n"),
    (&[b"XREAD", b"STREAMS", b"s", b"z", b"0"], b"-ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.\r\n"),
    (&[b"XREAD", b"COUNT", b"x", b"STREAMS", b"s", b"0"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"XREAD", b"COUNT", b"-1", b"STREAMS", b"s", b"0"], b"*-1\r\n"),
    (&[b"XREAD", b"BLOCK", b"x", b"STREAMS", b"s", b"0"], b"-ERR timeout is not an integer or out of range\r\n"),
    (&[b"XREAD", b"BLOCK", b"-1", b"STREAMS", b"s", b"0"], b"-ERR timeout is negative\r\n"),
    (&[b"XREAD", b"BLOCK", b"10", b"STREAMS", b"s", b"$"], b"*-1\r\n"),
    (&[b"XREAD", b"BLOCK", b"10", b"STREAMS", b"nope", b"0"], b"*-1\r\n"),
    (&[b"XREAD", b"BLOCK", b"10", b"STREAMS", b"nope", b"$"], b"*-1\r\n"),
    (&[b"XREAD", b"FOO", b"STREAMS", b"s", b"0"], b"-ERR syntax error\r\n"),
    (&[b"XREAD", b"STREAMS", b"nope", b"0"], b"*-1\r\n"),
    (&[b"XREAD", b"STREAMS", b"s", b"+"], b"-ERR Invalid stream ID specified as stream command argument\r\n"),
    (&[b"XREAD", b"STREAMS", b"s", b"x"], b"-ERR Invalid stream ID specified as stream command argument\r\n"),
    (&[b"XREAD", b"COUNT", b"1", b"COUNT", b"2", b"STREAMS", b"s", b"0"], b"*-1\r\n"),
    (&[b"XREAD", b"STREAMS", b"s", b"99-0"], b"*-1\r\n"),
    (&[b"HELLO", b"3"], b"%7\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.2.4\r\n$5\r\nproto\r\n:3\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n"),
    (&[b"XRANGE", b"s", b"-", b"+"], b"*0\r\n"),
    (&[b"XREAD", b"STREAMS", b"s", b"0"], b"_\r\n"),
    (&[b"XREAD", b"STREAMS", b"nope", b"0"], b"_\r\n"),
    (&[b"XREAD", b"BLOCK", b"10", b"STREAMS", b"nope", b"0"], b"_\r\n"),
    (&[b"XADD", b"s", b"100-1", b"f", b"v"], b"-ERR The stream has exhausted the last possible ID, unable to add more items\r\n"),
    (&[b"XRANGE", b"s", b"100", b"100"], b"*0\r\n"),
    (&[b"FLUSHALL"], b"+OK\r\n"),
    (&[b"XADD", b"s", b"*", b"f", b"v", b"g"], b"-ERR wrong number of arguments for 'xadd' command\r\n"),
    (&[b"XADD", b"s", b"01-1", b"f", b"v"], b"$3\r\n1-1\r\n"),
    (&[b"XADD", b"s", b"2-01", b"f", b"v"], b"$3\r\n2-1\r\n"),
    (&[b"XADD", b"s", b"+1", b"f", b"v"], b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"),
    (&[b"XADD", b"s", b" 1", b"f", b"v"], b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"),
    (&[b"XADD", b"s", b"3-", b"f", b"v"], b"-ERR Invalid stream ID specified as stream command argument\r\n"),
    (&[b"XADD", b"s", b"-3", b"f", b"v"], b"-ERR Invalid stream ID specified as stream command argument\r\n"),
    (&[b"XADD", b"s", b"4-1", b"f", b"v"], b"$3\r\n4-1\r\n"),
    (&[b"XADD", b"s", b"5-1", b"F", b"V"], b"$3\r\n5-1\r\n"),
    (&[b"XRANGE", b"s", b"01", b"+"], b"*4\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n2-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n4-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n5-1\r\n*2\r\n$1\r\nF\r\n$1\r\nV\r\n"),
    (&[b"XRANGE", b"s", b"+", b"-"], b"*0\r\n"),
    (&[b"XRANGE", b"s", b"4", b"4-1"], b"*1\r\n*2\r\n$3\r\n4-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"XRANGE", b"s", b"4-0", b"4"], b"*1\r\n*2\r\n$3\r\n4-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"XDEL", b"s", b"4-1", b"5-1"], b":2\r\n"),
    (&[b"XLEN", b"s"], b":2\r\n"),
    (&[b"XADD", b"s", b"5-1", b"f", b"v"], b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"),
    (&[b"XADD", b"s", b"6-1", b"f", b"v"], b"$3\r\n6-1\r\n"),
    (&[b"HELLO", b"3"], b"%7\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.2.4\r\n$5\r\nproto\r\n:3\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n"),
    (&[b"XRANGE", b"s", b"-", b"+"], b"*3\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n2-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n6-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"XREAD", b"STREAMS", b"s", b"0"], b"%1\r\n$1\r\ns\r\n*3\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n2-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n6-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"XREVRANGE", b"s", b"+", b"-", b"COUNT", b"1"], b"*1\r\n*2\r\n$3\r\n6-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"HELLO", b"2"], b"*14\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.2.4\r\n$5\r\nproto\r\n:2\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n"),
    (&[b"MULTI"], b"+OK\r\n"),
    (&[b"XREAD", b"BLOCK", b"0", b"STREAMS", b"s", b"$"], b"+QUEUED\r\n"),
    (&[b"XREAD", b"BLOCK", b"0", b"STREAMS", b"s", b"0"], b"+QUEUED\r\n"),
    (&[b"EXEC"], b"*2\r\n*-1\r\n*1\r\n*2\r\n$1\r\ns\r\n*3\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n2-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n6-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"XREAD", b"COUNT", b"0", b"STREAMS", b"s", b"0"], b"*1\r\n*2\r\n$1\r\ns\r\n*3\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n2-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n6-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"XREAD", b"STREAMS", b"s", b"5"], b"*1\r\n*2\r\n$1\r\ns\r\n*1\r\n*2\r\n$3\r\n6-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"XREAD", b"STREAMS", b"s", b"5-1"], b"*1\r\n*2\r\n$1\r\ns\r\n*1\r\n*2\r\n$3\r\n6-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"XREAD", b"STREAMS", b"s", b"18446744073709551615-18446744073709551615"], b"*-1\r\n"),
    (&[b"XREAD", b"STREAMS", b"s", b"18446744073709551616-0"], b"-ERR Invalid stream ID specified as stream command argument\r\n"),
    (&[b"XREAD", b"STREAMS", b"s", b"-"], b"-ERR Invalid stream ID specified as stream command argument\r\n"),
];

#[tokio::test]
async fn replies_match_redis() {
    common::replay(SCRIPT).await;
}

/// The random run: XADD with every kind of ID and trimming (`~` included,
/// whose result depends on where Redis' nodes end), XDEL, XTRIM, XLEN,
/// XRANGE, XREVRANGE and XREAD, over values from empty to 4 KB, in RESP2
/// and RESP3. An argument `~cN` stands for N times `c`.
#[tokio::test]
async fn random_commands_match_redis() {
    let fixture = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/data/streams-random.txt"
    ))
    .unwrap();
    let mut c = common::connect_buffered().await;
    for (n, line) in fixture.lines().enumerate() {
        let mut parts = line.split('\t');
        let expected = parts.next().unwrap();
        let args: Vec<Vec<u8>> = parts
            .map(|a| match a.strip_prefix('~') {
                Some(run) if run.len() > 1 => {
                    let (ch, count) = run.split_at(1);
                    ch.repeat(count.parse().unwrap()).into_bytes()
                }
                _ => a.as_bytes().to_vec(),
            })
            .collect();
        let refs: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
        c.send_raw(&common::req(&refs)).await;
        let reply = c.raw_reply().await;
        if expected != "-" {
            let got = format!("{:08x}:{}", crc32fast::hash(&reply), reply.len());
            assert_eq!(
                got,
                expected,
                "line {}: {}",
                n + 1,
                &line[..line.len().min(120)]
            );
        }
    }
}

async fn blocked(ctx: &Context, n: usize) {
    for _ in 0..1000 {
        if ctx.store.blocked_clients() == n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("{} blocked, not {n}", ctx.store.blocked_clients());
}

fn entry(id: &str, pairs: &[&str]) -> String {
    let mut s = format!("*2\r\n${}\r\n{id}\r\n*{}\r\n", id.len(), pairs.len());
    for p in pairs {
        s += &format!("${}\r\n{p}\r\n", p.len());
    }
    s
}

/// XREAD's reply for entries of one stream (RESP2).
fn read_reply(key: &str, entries: &[String]) -> String {
    format!(
        "*1\r\n*2\r\n${}\r\n{key}\r\n*{}\r\n{}",
        key.len(),
        entries.len(),
        entries.concat()
    )
}

/// As Redis 7.2.4 replied to each case (checked with the same clients).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_readers_are_served_as_in_redis() {
    for mode in MODES {
        let (addr, ctx) = start(mode).await;
        let mut admin = Client::connect(addr).await;
        let mut a = Client::connect(addr).await;
        let mut b = Client::connect(addr).await;

        // Everyone waiting on `$` gets the new entry: reading takes nothing.
        admin.call(&["XADD", "s", "1-1", "f", "old"]).await;
        a.send(&["XREAD", "BLOCK", "0", "STREAMS", "s", "$"]).await;
        blocked(&ctx, 1).await;
        b.send(&["XREAD", "BLOCK", "0", "STREAMS", "s", "$"]).await;
        blocked(&ctx, 2).await;
        admin.call(&["XADD", "s", "2-1", "f", "new"]).await;
        let new = read_reply("s", &[entry("2-1", &["f", "new"])]);
        assert_eq!(a.reply().await, new, "{mode:?}");
        assert_eq!(b.reply().await, new);
        blocked(&ctx, 0).await;

        // A stream that doesn't exist yet; one waited for past an ID.
        a.send(&["XREAD", "BLOCK", "0", "STREAMS", "nope", "0"])
            .await;
        blocked(&ctx, 1).await;
        admin.call(&["XADD", "nope", "5-0", "x", "y"]).await;
        assert_eq!(
            a.reply().await,
            read_reply("nope", &[entry("5-0", &["x", "y"])])
        );
        a.send(&["XREAD", "BLOCK", "0", "STREAMS", "s", "100-0"])
            .await;
        blocked(&ctx, 1).await;
        admin.call(&["XADD", "s", "50-0", "f", "v"]).await;
        assert!(!a.has_reply().await);
        admin.call(&["XADD", "s", "101-0", "f", "w"]).await;
        assert_eq!(
            a.reply().await,
            read_reply("s", &[entry("101-0", &["f", "w"])])
        );

        // A transaction's entries come together.
        a.send(&["XREAD", "COUNT", "10", "BLOCK", "0", "STREAMS", "s", "$"])
            .await;
        blocked(&ctx, 1).await;
        admin.call(&["MULTI"]).await;
        admin.call(&["XADD", "s", "200-0", "f", "a"]).await;
        admin.call(&["XADD", "s", "201-0", "f", "b"]).await;
        admin.call(&["EXEC"]).await;
        assert_eq!(
            a.reply().await,
            read_reply(
                "s",
                &[entry("200-0", &["f", "a"]), entry("201-0", &["f", "b"])]
            )
        );

        // Deleting it doesn't end the wait; another kind doesn't serve it.
        a.send(&["XREAD", "BLOCK", "0", "STREAMS", "k", "$"]).await;
        blocked(&ctx, 1).await;
        admin.call(&["RPUSH", "k", "x"]).await;
        assert!(!a.has_reply().await);
        admin.call(&["DEL", "k"]).await;
        admin.call(&["XADD", "k", "1-0", "f", "v"]).await;
        assert_eq!(
            a.reply().await,
            read_reply("k", &[entry("1-0", &["f", "v"])])
        );

        // Timeouts, and RESP3.
        assert_eq!(
            a.call(&["XREAD", "BLOCK", "50", "STREAMS", "s", "$"]).await,
            "*-1\r\n"
        );
        a.call(&["HELLO", "3"]).await;
        assert_eq!(
            a.call(&["XREAD", "BLOCK", "50", "STREAMS", "s", "$"]).await,
            "_\r\n"
        );
        a.send(&["XREAD", "BLOCK", "0", "STREAMS", "s", "$"]).await;
        blocked(&ctx, 1).await;
        admin.call(&["XADD", "s", "300-0", "f", "w"]).await;
        assert_eq!(
            a.reply().await,
            format!("%1\r\n$1\r\ns\r\n*1\r\n{}", entry("300-0", &["f", "w"]))
        );
        blocked(&ctx, 0).await;
    }
}
