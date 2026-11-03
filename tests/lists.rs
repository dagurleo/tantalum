//! List commands: replies recorded from Redis 7.2.4 (`tools/redis-diff.py
//! --rust`), and a model test across packed and big lists.

mod common;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use common::Value;
use tantalum::context::{Context, Settings};
use tantalum::store::Store;

#[rustfmt::skip]
const SCRIPT: &[(&[&[u8]], &[u8])] = &[
    (&[b"LPUSH", b"l", b"c", b"b", b"a"], b":3\r\n"),
    (&[b"RPUSH", b"l", b"d", b"e"], b":5\r\n"),
    (&[b"LRANGE", b"l", b"0", b"-1"], b"*5\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n"),
    (&[b"LRANGE", b"l", b"1", b"2"], b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n"),
    (&[b"LRANGE", b"l", b"-2", b"-1"], b"*2\r\n$1\r\nd\r\n$1\r\ne\r\n"),
    (&[b"LRANGE", b"l", b"-100", b"100"], b"*5\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n"),
    (&[b"LRANGE", b"l", b"3", b"1"], b"*0\r\n"),
    (&[b"LRANGE", b"l", b"10", b"20"], b"*0\r\n"),
    (&[b"LRANGE", b"l", b"x", b"1"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"LRANGE", b"nokey", b"0", b"-1"], b"*0\r\n"),
    (&[b"LRANGE", b"nokey", b"x", b"1"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"LLEN", b"l"], b":5\r\n"),
    (&[b"LLEN", b"nokey"], b":0\r\n"),
    (&[b"LINDEX", b"l", b"0"], b"$1\r\na\r\n"),
    (&[b"LINDEX", b"l", b"-1"], b"$1\r\ne\r\n"),
    (&[b"LINDEX", b"l", b"5"], b"$-1\r\n"),
    (&[b"LINDEX", b"l", b"-6"], b"$-1\r\n"),
    (&[b"LINDEX", b"l", b"x"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"LINDEX", b"nokey", b"x"], b"$-1\r\n"),
    (&[b"LINDEX", b"nokey", b"0"], b"$-1\r\n"),
    (&[b"LPUSHX", b"nokey", b"a"], b":0\r\n"),
    (&[b"RPUSHX", b"nokey", b"a"], b":0\r\n"),
    (&[b"EXISTS", b"nokey"], b":0\r\n"),
    (&[b"LPUSHX", b"l", b"z"], b":6\r\n"),
    (&[b"RPUSHX", b"l", b"y", b"z"], b":8\r\n"),
    (&[b"LRANGE", b"l", b"0", b"-1"], b"*8\r\n$1\r\nz\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n$1\r\ny\r\n$1\r\nz\r\n"),
    (&[b"LPUSH"], b"-ERR wrong number of arguments for 'lpush' command\r\n"),
    (&[b"LPUSH", b"l"], b"-ERR wrong number of arguments for 'lpush' command\r\n"),
    (&[b"RPUSH", b"l"], b"-ERR wrong number of arguments for 'rpush' command\r\n"),
    (&[b"LPOP", b"l"], b"$1\r\nz\r\n"),
    (&[b"RPOP", b"l"], b"$1\r\nz\r\n"),
    (&[b"LPOP", b"l", b"2"], b"*2\r\n$1\r\na\r\n$1\r\nb\r\n"),
    (&[b"RPOP", b"l", b"2"], b"*2\r\n$1\r\ny\r\n$1\r\ne\r\n"),
    (&[b"LPOP", b"l", b"0"], b"*0\r\n"),
    (&[b"LPOP", b"nokey"], b"$-1\r\n"),
    (&[b"LPOP", b"nokey", b"2"], b"*-1\r\n"),
    (&[b"LPOP", b"nokey", b"0"], b"*-1\r\n"),
    (&[b"LPOP", b"l", b"-1"], b"-ERR value is out of range, must be positive\r\n"),
    (&[b"LPOP", b"l", b"x"], b"-ERR value is out of range, must be positive\r\n"),
    (&[b"LPOP", b"l", b"1", b"2"], b"-ERR wrong number of arguments for 'lpop' command\r\n"),
    (&[b"RPOP", b"l", b"100"], b"*2\r\n$1\r\nd\r\n$1\r\nc\r\n"),
    (&[b"EXISTS", b"l"], b":0\r\n"),
    (&[b"RPUSH", b"s", b"a", b"b", b"c", b"b", b"a"], b":5\r\n"),
    (&[b"LSET", b"s", b"0", b"A"], b"+OK\r\n"),
    (&[b"LSET", b"s", b"-1", b"Z"], b"+OK\r\n"),
    (&[b"LSET", b"s", b"5", b"x"], b"-ERR index out of range\r\n"),
    (&[b"LSET", b"s", b"-6", b"x"], b"-ERR index out of range\r\n"),
    (&[b"LSET", b"s", b"x", b"y"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"LSET", b"nokey", b"0", b"x"], b"-ERR no such key\r\n"),
    (&[b"LSET", b"nokey", b"x", b"x"], b"-ERR no such key\r\n"),
    (&[b"LRANGE", b"s", b"0", b"-1"], b"*5\r\n$1\r\nA\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\nZ\r\n"),
    (&[b"LINSERT", b"s", b"BEFORE", b"b", b"X"], b":6\r\n"),
    (&[b"LINSERT", b"s", b"AFTER", b"b", b"Y"], b":7\r\n"),
    (&[b"LINSERT", b"s", b"after", b"Z", b"end"], b":8\r\n"),
    (&[b"LINSERT", b"s", b"before", b"A", b"start"], b":9\r\n"),
    (&[b"LINSERT", b"s", b"BEFORE", b"nope", b"x"], b":-1\r\n"),
    (&[b"LINSERT", b"s", b"MIDDLE", b"b", b"x"], b"-ERR syntax error\r\n"),
    (&[b"LINSERT", b"nokey", b"BEFORE", b"a", b"x"], b":0\r\n"),
    (&[b"LINSERT", b"nokey", b"MIDDLE", b"a", b"x"], b"-ERR syntax error\r\n"),
    (&[b"LRANGE", b"s", b"0", b"-1"], b"*9\r\n$5\r\nstart\r\n$1\r\nA\r\n$1\r\nX\r\n$1\r\nb\r\n$1\r\nY\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\nZ\r\n$3\r\nend\r\n"),
    (&[b"LREM", b"s", b"1", b"b"], b":1\r\n"),
    (&[b"LREM", b"s", b"-1", b"b"], b":1\r\n"),
    (&[b"LRANGE", b"s", b"0", b"-1"], b"*7\r\n$5\r\nstart\r\n$1\r\nA\r\n$1\r\nX\r\n$1\r\nY\r\n$1\r\nc\r\n$1\r\nZ\r\n$3\r\nend\r\n"),
    (&[b"RPUSH", b"r", b"a", b"b", b"a", b"b", b"a", b"b"], b":6\r\n"),
    (&[b"LREM", b"r", b"0", b"a"], b":3\r\n"),
    (&[b"LRANGE", b"r", b"0", b"-1"], b"*3\r\n$1\r\nb\r\n$1\r\nb\r\n$1\r\nb\r\n"),
    (&[b"LREM", b"r", b"1", b"zzz"], b":0\r\n"),
    (&[b"LREM", b"r", b"x", b"a"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"LREM", b"nokey", b"1", b"a"], b":0\r\n"),
    (&[b"LREM", b"nokey", b"x", b"a"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"RPUSH", b"t", b"1", b"2", b"3", b"4", b"5", b"6"], b":6\r\n"),
    (&[b"LTRIM", b"t", b"1", b"-2"], b"+OK\r\n"),
    (&[b"LRANGE", b"t", b"0", b"-1"], b"*4\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n"),
    (&[b"LTRIM", b"t", b"-100", b"100"], b"+OK\r\n"),
    (&[b"LRANGE", b"t", b"0", b"-1"], b"*4\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n"),
    (&[b"LTRIM", b"t", b"5", b"1"], b"+OK\r\n"),
    (&[b"EXISTS", b"t"], b":0\r\n"),
    (&[b"LTRIM", b"nokey", b"0", b"1"], b"+OK\r\n"),
    (&[b"LTRIM", b"t", b"x", b"1"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"RPUSH", b"p", b"a", b"b", b"c", b"a", b"b", b"c", b"a"], b":7\r\n"),
    (&[b"LPOS", b"p", b"a"], b":0\r\n"),
    (&[b"LPOS", b"p", b"c"], b":2\r\n"),
    (&[b"LPOS", b"p", b"z"], b"$-1\r\n"),
    (&[b"LPOS", b"p", b"a", b"RANK", b"2"], b":3\r\n"),
    (&[b"LPOS", b"p", b"a", b"RANK", b"-1"], b":6\r\n"),
    (&[b"LPOS", b"p", b"a", b"RANK", b"-2"], b":3\r\n"),
    (&[b"LPOS", b"p", b"a", b"COUNT", b"0"], b"*3\r\n:0\r\n:3\r\n:6\r\n"),
    (&[b"LPOS", b"p", b"a", b"COUNT", b"2"], b"*2\r\n:0\r\n:3\r\n"),
    (&[b"LPOS", b"p", b"a", b"RANK", b"2", b"COUNT", b"0"], b"*2\r\n:3\r\n:6\r\n"),
    (&[b"LPOS", b"p", b"a", b"RANK", b"-1", b"COUNT", b"2"], b"*2\r\n:6\r\n:3\r\n"),
    (&[b"LPOS", b"p", b"c", b"MAXLEN", b"2"], b"$-1\r\n"),
    (&[b"LPOS", b"p", b"c", b"MAXLEN", b"3"], b":2\r\n"),
    (&[b"LPOS", b"p", b"a", b"COUNT", b"0", b"MAXLEN", b"4"], b"*2\r\n:0\r\n:3\r\n"),
    (&[b"LPOS", b"p", b"a", b"RANK", b"0"], b"-ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list\r\n"),
    (&[b"LPOS", b"p", b"a", b"RANK", b"x"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"LPOS", b"p", b"a", b"COUNT", b"-1"], b"-ERR COUNT can't be negative\r\n"),
    (&[b"LPOS", b"p", b"a", b"COUNT", b"x"], b"-ERR COUNT can't be negative\r\n"),
    (&[b"LPOS", b"p", b"a", b"MAXLEN", b"-1"], b"-ERR MAXLEN can't be negative\r\n"),
    (&[b"LPOS", b"p", b"a", b"MAXLEN", b"x"], b"-ERR MAXLEN can't be negative\r\n"),
    (&[b"LPOS", b"p", b"a", b"RANK", b"-9223372036854775808"], b"-ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807\r\n"),
    (&[b"LPOS", b"p", b"a", b"BAD", b"1"], b"-ERR syntax error\r\n"),
    (&[b"LPOS", b"p", b"a", b"RANK"], b"-ERR syntax error\r\n"),
    (&[b"LPOS", b"p", b"a", b"RANK", b"5"], b"$-1\r\n"),
    (&[b"LPOS", b"p", b"a", b"RANK", b"5", b"COUNT", b"1"], b"*0\r\n"),
    (&[b"LPOS", b"nokey", b"a"], b"$-1\r\n"),
    (&[b"LPOS", b"nokey", b"a", b"COUNT", b"1"], b"*0\r\n"),
    (&[b"RPUSH", b"m1", b"a", b"b", b"c"], b":3\r\n"),
    (&[b"LMOVE", b"m1", b"m2", b"LEFT", b"RIGHT"], b"$1\r\na\r\n"),
    (&[b"LMOVE", b"m1", b"m2", b"RIGHT", b"LEFT"], b"$1\r\nc\r\n"),
    (&[b"LRANGE", b"m1", b"0", b"-1"], b"*1\r\n$1\r\nb\r\n"),
    (&[b"LRANGE", b"m2", b"0", b"-1"], b"*2\r\n$1\r\nc\r\n$1\r\na\r\n"),
    (&[b"LMOVE", b"m1", b"m1", b"LEFT", b"RIGHT"], b"$1\r\nb\r\n"),
    (&[b"LRANGE", b"m1", b"0", b"-1"], b"*1\r\n$1\r\nb\r\n"),
    (&[b"LMOVE", b"m1", b"m2", b"left", b"right"], b"$1\r\nb\r\n"),
    (&[b"LMOVE", b"m1", b"m2", b"UP", b"LEFT"], b"-ERR syntax error\r\n"),
    (&[b"LMOVE", b"m1", b"m2", b"LEFT", b"DOWN"], b"-ERR syntax error\r\n"),
    (&[b"LMOVE", b"nokey", b"m2", b"LEFT", b"LEFT"], b"$-1\r\n"),
    (&[b"EXISTS", b"m1"], b":0\r\n"),
    (&[b"RPOPLPUSH", b"m2", b"m3"], b"$1\r\nb\r\n"),
    (&[b"RPOPLPUSH", b"m2", b"m3"], b"$1\r\na\r\n"),
    (&[b"RPOPLPUSH", b"m2", b"m3"], b"$1\r\nc\r\n"),
    (&[b"RPOPLPUSH", b"m2", b"m3"], b"$-1\r\n"),
    (&[b"LRANGE", b"m3", b"0", b"-1"], b"*3\r\n$1\r\nc\r\n$1\r\na\r\n$1\r\nb\r\n"),
    (&[b"EXISTS", b"m2"], b":0\r\n"),
    (&[b"SET", b"str", b"v"], b"+OK\r\n"),
    (&[b"LMOVE", b"str", b"m3", b"LEFT", b"LEFT"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"LMOVE", b"m3", b"str", b"LEFT", b"LEFT"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"RPOPLPUSH", b"m3", b"str"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"LRANGE", b"m3", b"0", b"-1"], b"*3\r\n$1\r\nc\r\n$1\r\na\r\n$1\r\nb\r\n"),
    (&[b"RPUSH", b"one", b"x"], b":1\r\n"),
    (&[b"RPOPLPUSH", b"one", b"one"], b"$1\r\nx\r\n"),
    (&[b"LRANGE", b"one", b"0", b"-1"], b"*1\r\n$1\r\nx\r\n"),
    (&[b"RPUSH", b"q1", b"a", b"b", b"c"], b":3\r\n"),
    (&[b"RPUSH", b"q2", b"x", b"y"], b":2\r\n"),
    (&[b"LMPOP", b"2", b"nokey", b"q1", b"LEFT"], b"*2\r\n$2\r\nq1\r\n*1\r\n$1\r\na\r\n"),
    (&[b"LMPOP", b"2", b"nokey", b"q1", b"RIGHT", b"COUNT", b"5"], b"*2\r\n$2\r\nq1\r\n*2\r\n$1\r\nc\r\n$1\r\nb\r\n"),
    (&[b"LMPOP", b"2", b"q1", b"q2", b"LEFT", b"COUNT", b"1"], b"*2\r\n$2\r\nq2\r\n*1\r\n$1\r\nx\r\n"),
    (&[b"LMPOP", b"1", b"q2", b"left", b"count", b"10"], b"*2\r\n$2\r\nq2\r\n*1\r\n$1\r\ny\r\n"),
    (&[b"LMPOP", b"2", b"nokey", b"nokey2", b"LEFT"], b"*-1\r\n"),
    (&[b"LMPOP", b"0", b"q1", b"LEFT"], b"-ERR numkeys should be greater than 0\r\n"),
    (&[b"LMPOP", b"x", b"q1", b"LEFT"], b"-ERR numkeys should be greater than 0\r\n"),
    (&[b"LMPOP", b"2", b"q1", b"LEFT"], b"-ERR syntax error\r\n"),
    (&[b"LMPOP", b"1", b"q1", b"UP"], b"-ERR syntax error\r\n"),
    (&[b"LMPOP", b"1", b"q1", b"LEFT", b"COUNT", b"0"], b"-ERR count should be greater than 0\r\n"),
    (&[b"LMPOP", b"1", b"q1", b"LEFT", b"COUNT", b"x"], b"-ERR count should be greater than 0\r\n"),
    (&[b"LMPOP", b"1", b"q1", b"LEFT", b"COUNT", b"1", b"COUNT", b"2"], b"-ERR syntax error\r\n"),
    (&[b"LMPOP", b"1", b"q1", b"LEFT", b"EXTRA"], b"-ERR syntax error\r\n"),
    (&[b"LMPOP", b"2", b"str", b"q1", b"LEFT"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"LMPOP", b"1", b"nokey", b"LEFT", b"COUNT", b"-1"], b"-ERR count should be greater than 0\r\n"),
    (&[b"LPUSH", b"str", b"a"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"RPUSH", b"str", b"a"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"LPOP", b"str"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"RPOP", b"str", b"2"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"LLEN", b"str"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"LRANGE", b"str", b"0", b"1"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"LINDEX", b"str", b"0"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"LSET", b"str", b"0", b"a"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"LINSERT", b"str", b"BEFORE", b"a", b"b"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"LREM", b"str", b"0", b"a"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"LTRIM", b"str", b"0", b"1"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"LPOS", b"str", b"a"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"LPUSHX", b"str", b"a"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"GET", b"q1"], b"$-1\r\n"),
    (&[b"SADD", b"q1", b"a"], b":1\r\n"),
    (&[b"TYPE", b"m3"], b"+list\r\n"),
    (&[b"RPUSH", b"ttl", b"a", b"b"], b":2\r\n"),
    (&[b"EXPIRE", b"ttl", b"100"], b":1\r\n"),
    (&[b"LPUSH", b"ttl", b"z"], b":3\r\n"),
    (&[b"LPOP", b"ttl"], b"$1\r\nz\r\n"),
    (&[b"LSET", b"ttl", b"0", b"q"], b"+OK\r\n"),
    (&[b"TTL", b"ttl"], b":100\r\n"),
    (&[b"DBSIZE"], b":8\r\n"),
    // Redis reports its own client id here.
    (&[b"HELLO", b"3"], b"%7\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.2.4\r\n$5\r\nproto\r\n:3\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n"),
    (&[b"LPOP", b"nokey", b"2"], b"_\r\n"),
    (&[b"LPOP", b"ttl", b"0"], b"*0\r\n"),
    (&[b"LMPOP", b"1", b"nokey", b"LEFT"], b"_\r\n"),
    (&[b"LPOS", b"ttl", b"q", b"COUNT", b"0"], b"*1\r\n:0\r\n"),
    (&[b"LPOS", b"ttl", b"zz"], b"_\r\n"),
    (&[b"LRANGE", b"ttl", b"0", b"-1"], b"*2\r\n$1\r\nq\r\n$1\r\nb\r\n"),
    (&[b"LMPOP", b"1", b"ttl", b"RIGHT"], b"*2\r\n$3\r\nttl\r\n*1\r\n$1\r\nb\r\n"),
];

#[tokio::test]
async fn replies_match_redis() {
    common::replay(SCRIPT).await;
}

/// Random list commands through `cmd::execute` against a model, with
/// lists growing past the packed limits (and big elements) and back.
#[test]
fn matches_a_model() {
    let ctx = Context::new(Arc::new(Store::new(4)), Settings::default());
    let client = ctx.connect(-1).unwrap();
    let mut session = client.session();
    let mut model: HashMap<Vec<u8>, VecDeque<Vec<u8>>> = HashMap::new();
    let mut rng = 0x2545_F491_4F6C_DD1Du64;
    let mut below = move |n: u64| {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng % n.max(1)
    };
    let mut biggest = 0;
    for step in 0..12_000 {
        biggest = model
            .values()
            .map(VecDeque::len)
            .max()
            .unwrap_or(0)
            .max(biggest);
        let key = format!("l{}", below(3)).into_bytes();
        let other = format!("l{}", below(3)).into_bytes();
        let count = if below(15) == 0 {
            150
        } else {
            1 + below(3) as usize
        };
        let elements: Vec<Vec<u8>> = (0..count)
            .map(|_| match below(40) {
                0 => vec![b'B'; 2_000 + below(6_000) as usize],
                _ => format!("e{}", below(20)).into_bytes(),
            })
            .collect();
        let mut run =
            |args: Vec<&[u8]>| common::parse(&common::execute(&ctx, &mut session, 1, &args));
        let label = format!("step {step}");
        let list = model.get(&key).cloned().unwrap_or_default();
        let len = list.len() as i64;
        let set = |model: &mut HashMap<Vec<u8>, VecDeque<Vec<u8>>>,
                   key: &[u8],
                   list: VecDeque<Vec<u8>>| {
            if list.is_empty() {
                model.remove(key);
            } else {
                model.insert(key.to_vec(), list);
            }
        };
        match below(14) {
            0..3 => {
                let left = below(2) == 0;
                let mut args = vec![if left { &b"LPUSH"[..] } else { b"RPUSH" }, &key];
                args.extend(elements.iter().map(Vec::as_slice));
                let mut list = list;
                for e in &elements {
                    if left {
                        list.push_front(e.clone())
                    } else {
                        list.push_back(e.clone())
                    }
                }
                assert_eq!(run(args).int(), list.len() as i64, "{label}");
                set(&mut model, &key, list);
            }
            3 | 4 => {
                let n = below(4);
                let left = below(2) == 0;
                let reply = run(vec![
                    if left { b"LPOP" } else { b"RPOP" },
                    &key,
                    n.to_string().as_bytes(),
                ]);
                let mut list = list;
                let popped: Vec<Vec<u8>> = (0..n)
                    .filter_map(|_| {
                        if left {
                            list.pop_front()
                        } else {
                            list.pop_back()
                        }
                    })
                    .collect();
                if model.contains_key(&key) {
                    assert_eq!(reply.bulks(), popped, "{label}");
                } else {
                    assert_eq!(reply, Value::Nil, "{label}");
                }
                set(&mut model, &key, list);
            }
            5 => {
                let (start, stop) = (below(10) as i64 - 5, below(10) as i64 - 3);
                let got = run(vec![
                    b"LRANGE",
                    &key,
                    start.to_string().as_bytes(),
                    stop.to_string().as_bytes(),
                ]);
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
                let expected: Vec<Vec<u8>> = if s > e || s >= len {
                    Vec::new()
                } else {
                    list.iter()
                        .skip(s as usize)
                        .take((e - s + 1) as usize)
                        .cloned()
                        .collect()
                };
                assert_eq!(got.bulks(), expected, "{label}");
            }
            6 => {
                let i = below(12) as i64 - 6;
                let at = if i < 0 { len + i } else { i };
                let got = run(vec![b"LINDEX", &key, i.to_string().as_bytes()]);
                match list.get(at as usize).filter(|_| at >= 0) {
                    Some(e) => assert_eq!(got, Value::Bulk(e.clone()), "{label}"),
                    None => assert_eq!(got, Value::Nil, "{label}"),
                }
            }
            7 if len > 0 => {
                let i = below(len as u64) as usize;
                assert_eq!(
                    run(vec![b"LSET", &key, i.to_string().as_bytes(), &elements[0]]),
                    Value::Simple("OK".into()),
                    "{label}"
                );
                let mut list = list;
                list[i] = elements[0].clone();
                set(&mut model, &key, list);
            }
            8 => {
                let pivot = format!("e{}", below(20)).into_bytes();
                let after = below(2) == 0;
                let got = run(vec![
                    b"LINSERT",
                    &key,
                    if after { b"AFTER" } else { b"BEFORE" },
                    &pivot,
                    &elements[0],
                ])
                .int();
                let mut list = list;
                let expected = if !model.contains_key(&key) {
                    0
                } else if let Some(at) = list.iter().position(|e| *e == pivot) {
                    list.insert(at + usize::from(after), elements[0].clone());
                    list.len() as i64
                } else {
                    -1
                };
                assert_eq!(got, expected, "{label}");
                set(&mut model, &key, list);
            }
            9 => {
                let value = format!("e{}", below(20)).into_bytes();
                let count = below(5) as i64 - 2;
                let got = run(vec![b"LREM", &key, count.to_string().as_bytes(), &value]).int();
                let mut positions: Vec<usize> =
                    (0..list.len()).filter(|&i| list[i] == value).collect();
                if count < 0 {
                    positions.reverse();
                }
                if count != 0 {
                    positions.truncate(count.unsigned_abs() as usize);
                }
                positions.sort_unstable();
                let mut list = list;
                for &i in positions.iter().rev() {
                    list.remove(i);
                }
                assert_eq!(got, positions.len() as i64, "{label}");
                set(&mut model, &key, list);
            }
            10 => {
                let (start, stop) = (below(6) as i64 - 2, below(400) as i64 - 3);
                run(vec![
                    b"LTRIM",
                    &key,
                    start.to_string().as_bytes(),
                    stop.to_string().as_bytes(),
                ]);
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
                let list: VecDeque<Vec<u8>> = if s > e || s >= len {
                    VecDeque::new()
                } else {
                    list.iter()
                        .skip(s as usize)
                        .take((e - s + 1) as usize)
                        .cloned()
                        .collect()
                };
                set(&mut model, &key, list);
            }
            11 => {
                let (from, to) = (below(2) == 0, below(2) == 0);
                let got = run(vec![
                    b"LMOVE",
                    &key,
                    &other,
                    if from { b"LEFT" } else { b"RIGHT" },
                    if to { b"LEFT" } else { b"RIGHT" },
                ]);
                let mut list = list;
                match if from {
                    list.pop_front()
                } else {
                    list.pop_back()
                } {
                    None => assert_eq!(got, Value::Nil, "{label}"),
                    Some(e) => {
                        assert_eq!(got, Value::Bulk(e.clone()), "{label}");
                        set(&mut model, &key, list);
                        let mut dst = model.get(&other).cloned().unwrap_or_default();
                        if to {
                            dst.push_front(e)
                        } else {
                            dst.push_back(e)
                        }
                        set(&mut model, &other, dst);
                    }
                }
            }
            12 => {
                let value = format!("e{}", below(20)).into_bytes();
                let got = run(vec![b"LPOS", &key, &value, b"COUNT", b"0"]);
                let expected: Vec<Value> = (0..list.len())
                    .filter(|&i| list[i] == value)
                    .map(|i| Value::Int(i as i64))
                    .collect();
                assert_eq!(got, Value::Array(expected), "{label}");
            }
            _ => {
                assert_eq!(run(vec![b"LLEN", &key]).int(), len, "{label}");
                let all = run(vec![b"LRANGE", &key, b"0", b"-1"]).bulks();
                assert_eq!(all, Vec::from(list), "{label}");
            }
        }
    }
    assert!(biggest > 300, "no big list: {biggest}");
}
