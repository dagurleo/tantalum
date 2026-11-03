//! Set commands: replies recorded from Redis 7.2.4 (`tools/redis-diff.py
//! --rust`), and what a byte comparison can't check: big sets, whose order
//! is each server's own, and random members.

mod common;

#[rustfmt::skip]
const SCRIPT: &[(&[&[u8]], &[u8])] = &[
    (&[b"SADD", b"ints", b"30", b"10", b"20", b"10"], b":3\r\n"),
    (&[b"SMEMBERS", b"ints"], b"*3\r\n$2\r\n10\r\n$2\r\n20\r\n$2\r\n30\r\n"),
    (&[b"SCARD", b"ints"], b":3\r\n"),
    (&[b"SISMEMBER", b"ints", b"10"], b":1\r\n"),
    (&[b"SISMEMBER", b"ints", b"11"], b":0\r\n"),
    (&[b"SISMEMBER", b"ints", b"010"], b":0\r\n"),
    (&[b"SMISMEMBER", b"ints", b"10", b"11", b"30"], b"*3\r\n:1\r\n:0\r\n:1\r\n"),
    (&[b"SADD", b"ints", b"-5", b"007", b"+3"], b":3\r\n"),
    (&[b"SMEMBERS", b"ints"], b"*6\r\n$2\r\n-5\r\n$2\r\n10\r\n$2\r\n20\r\n$2\r\n30\r\n$3\r\n007\r\n$2\r\n+3\r\n"),
    (&[b"SADD", b"ints", b"x"], b":1\r\n"),
    (&[b"SMEMBERS", b"ints"], b"*7\r\n$2\r\n-5\r\n$2\r\n10\r\n$2\r\n20\r\n$2\r\n30\r\n$3\r\n007\r\n$2\r\n+3\r\n$1\r\nx\r\n"),
    (&[b"SREM", b"ints", b"10", b"nope", b"x"], b":2\r\n"),
    (&[b"SMEMBERS", b"ints"], b"*5\r\n$2\r\n-5\r\n$2\r\n20\r\n$2\r\n30\r\n$3\r\n007\r\n$2\r\n+3\r\n"),
    (&[b"SADD", b"strs", b"c", b"b", b"a", b"10", b"2"], b":5\r\n"),
    (&[b"SMEMBERS", b"strs"], b"*5\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n$2\r\n10\r\n$1\r\n2\r\n"),
    (&[b"SADD", b"strs", b"a"], b":0\r\n"),
    (&[b"SREM", b"strs", b"b"], b":1\r\n"),
    (&[b"SMEMBERS", b"strs"], b"*4\r\n$1\r\nc\r\n$1\r\na\r\n$2\r\n10\r\n$1\r\n2\r\n"),
    (&[b"TYPE", b"strs"], b"+set\r\n"),
    (&[b"SCARD", b"nokey"], b":0\r\n"),
    (&[b"SMEMBERS", b"nokey"], b"*0\r\n"),
    (&[b"SISMEMBER", b"nokey", b"a"], b":0\r\n"),
    (&[b"SMISMEMBER", b"nokey", b"a", b"b"], b"*2\r\n:0\r\n:0\r\n"),
    (&[b"SREM", b"nokey", b"a"], b":0\r\n"),
    (&[b"SREM", b"strs", b"c", b"a", b"10", b"2"], b":4\r\n"),
    (&[b"EXISTS", b"strs"], b":0\r\n"),
    (&[b"SADD", b"s"], b"-ERR wrong number of arguments for 'sadd' command\r\n"),
    (&[b"SADD"], b"-ERR wrong number of arguments for 'sadd' command\r\n"),
    (&[b"SREM", b"s"], b"-ERR wrong number of arguments for 'srem' command\r\n"),
    (&[b"SISMEMBER", b"s"], b"-ERR wrong number of arguments for 'sismember' command\r\n"),
    (&[b"SMISMEMBER", b"s"], b"-ERR wrong number of arguments for 'smismember' command\r\n"),
    (&[b"SCARD"], b"-ERR wrong number of arguments for 'scard' command\r\n"),
    (&[b"SMEMBERS"], b"-ERR wrong number of arguments for 'smembers' command\r\n"),
    (&[b"SET", b"str", b"v"], b"+OK\r\n"),
    (&[b"SADD", b"str", b"a"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SREM", b"str", b"a"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SISMEMBER", b"str", b"a"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SMISMEMBER", b"str", b"a"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SMEMBERS", b"str"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SCARD", b"str"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SPOP", b"str"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SRANDMEMBER", b"str"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SMOVE", b"str", b"s", b"a"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SINTER", b"str"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SUNION", b"str"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SDIFF", b"str"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SSCAN", b"str", b"0"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"HSET", b"h", b"f", b"v"], b":1\r\n"),
    (&[b"SADD", b"h", b"a"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"GET", b"ints"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"HGET", b"ints", b"f"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"TYPE", b"ints"], b"+set\r\n"),
    (&[b"SADD", b"p", b"3", b"1", b"2"], b":3\r\n"),
    (&[b"SRANDMEMBER", b"p", b"10"], b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"),
    (&[b"SRANDMEMBER", b"p", b"3"], b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"),
    (&[b"SRANDMEMBER", b"p", b"0"], b"*0\r\n"),
    (&[b"SRANDMEMBER", b"p", b"abc"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"SRANDMEMBER", b"p", b"-9223372036854775808"], b"-ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807\r\n"),
    (&[b"SRANDMEMBER", b"p", b"1", b"2"], b"-ERR syntax error\r\n"),
    (&[b"SRANDMEMBER", b"nokey"], b"$-1\r\n"),
    (&[b"SRANDMEMBER", b"nokey", b"5"], b"*0\r\n"),
    (&[b"SRANDMEMBER", b"nokey", b"-5"], b"*0\r\n"),
    (&[b"SPOP", b"p", b"5"], b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"),
    (&[b"EXISTS", b"p"], b":0\r\n"),
    (&[b"SADD", b"q", b"3", b"1", b"x"], b":3\r\n"),
    (&[b"SPOP", b"q", b"3"], b"*3\r\n$1\r\n1\r\n$1\r\n3\r\n$1\r\nx\r\n"),
    (&[b"SPOP", b"nokey"], b"$-1\r\n"),
    (&[b"SPOP", b"nokey", b"3"], b"*0\r\n"),
    (&[b"SPOP", b"q", b"-1"], b"-ERR value is out of range, must be positive\r\n"),
    (&[b"SPOP", b"q", b"abc"], b"-ERR value is out of range, must be positive\r\n"),
    (&[b"SPOP", b"q", b"1", b"2"], b"-ERR syntax error\r\n"),
    (&[b"SADD", b"r", b"a"], b":1\r\n"),
    (&[b"SPOP", b"r", b"0"], b"*0\r\n"),
    (&[b"SPOP", b"r"], b"$1\r\na\r\n"),
    (&[b"EXISTS", b"r"], b":0\r\n"),
    (&[b"SADD", b"lp", b"3", b"1", b"x"], b":3\r\n"),
    (&[b"SRANDMEMBER", b"lp", b"3"], b"*3\r\n$1\r\n1\r\n$1\r\n3\r\n$1\r\nx\r\n"),
    (&[b"SADD", b"lp2", b"x", b"3", b"1"], b":3\r\n"),
    (&[b"SRANDMEMBER", b"lp2", b"3"], b"*3\r\n$1\r\nx\r\n$1\r\n3\r\n$1\r\n1\r\n"),
    (&[b"SADD", b"m1", b"a", b"b"], b":2\r\n"),
    (&[b"SADD", b"m2", b"c"], b":1\r\n"),
    (&[b"SMOVE", b"m1", b"m2", b"a"], b":1\r\n"),
    (&[b"SMEMBERS", b"m1"], b"*1\r\n$1\r\nb\r\n"),
    (&[b"SMEMBERS", b"m2"], b"*2\r\n$1\r\nc\r\n$1\r\na\r\n"),
    (&[b"SMOVE", b"m1", b"m2", b"nope"], b":0\r\n"),
    (&[b"SMOVE", b"m1", b"m1", b"b"], b":1\r\n"),
    (&[b"SMOVE", b"m1", b"m1", b"z"], b":0\r\n"),
    (&[b"SMOVE", b"nokey", b"m2", b"a"], b":0\r\n"),
    (&[b"SMOVE", b"m1", b"str", b"b"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SMOVE", b"str", b"m1", b"b"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SMOVE", b"m1", b"m3", b"b"], b":1\r\n"),
    (&[b"EXISTS", b"m1"], b":0\r\n"),
    (&[b"SMEMBERS", b"m3"], b"*1\r\n$1\r\nb\r\n"),
    (&[b"SADD", b"a1", b"1", b"2", b"3", b"4"], b":4\r\n"),
    (&[b"SADD", b"a2", b"3", b"4", b"5"], b":3\r\n"),
    (&[b"SADD", b"a3", b"x", b"4", b"3"], b":3\r\n"),
    (&[b"SINTER", b"a1", b"a2"], b"*2\r\n$1\r\n3\r\n$1\r\n4\r\n"),
    (&[b"SINTER", b"a1", b"a2", b"a3"], b"*2\r\n$1\r\n3\r\n$1\r\n4\r\n"),
    (&[b"SINTER", b"a3", b"a1"], b"*2\r\n$1\r\n4\r\n$1\r\n3\r\n"),
    (&[b"SINTER", b"a1", b"nokey"], b"*0\r\n"),
    (&[b"SINTER", b"a1"], b"*4\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n"),
    (&[b"SUNION", b"a1", b"a2"], b"*5\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n"),
    (&[b"SUNION", b"a3", b"a1"], b"*5\r\n$1\r\nx\r\n$1\r\n4\r\n$1\r\n3\r\n$1\r\n1\r\n$1\r\n2\r\n"),
    (&[b"SUNION", b"a1", b"nokey", b"a2"], b"*5\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n"),
    (&[b"SDIFF", b"a1", b"a2"], b"*2\r\n$1\r\n1\r\n$1\r\n2\r\n"),
    (&[b"SDIFF", b"a3", b"a1"], b"*1\r\n$1\r\nx\r\n"),
    (&[b"SDIFF", b"nokey", b"a1"], b"*0\r\n"),
    (&[b"SDIFF", b"a1", b"nokey"], b"*4\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n"),
    (&[b"SINTERSTORE", b"d", b"a1", b"a2"], b":2\r\n"),
    (&[b"SMEMBERS", b"d"], b"*2\r\n$1\r\n3\r\n$1\r\n4\r\n"),
    (&[b"SINTERSTORE", b"d", b"a1", b"nokey"], b":0\r\n"),
    (&[b"EXISTS", b"d"], b":0\r\n"),
    (&[b"SUNIONSTORE", b"d", b"a3", b"a1"], b":5\r\n"),
    (&[b"SMEMBERS", b"d"], b"*5\r\n$1\r\nx\r\n$1\r\n4\r\n$1\r\n3\r\n$1\r\n1\r\n$1\r\n2\r\n"),
    (&[b"SDIFFSTORE", b"d", b"a1", b"a2"], b":2\r\n"),
    (&[b"SMEMBERS", b"d"], b"*2\r\n$1\r\n1\r\n$1\r\n2\r\n"),
    (&[b"SDIFFSTORE", b"d", b"a1", b"a1"], b":0\r\n"),
    (&[b"EXISTS", b"d"], b":0\r\n"),
    (&[b"SET", b"dstr", b"v"], b"+OK\r\n"),
    (&[b"SUNIONSTORE", b"dstr", b"a1"], b":4\r\n"),
    (&[b"TYPE", b"dstr"], b"+set\r\n"),
    (&[b"SINTERCARD", b"2", b"a1", b"a2"], b":2\r\n"),
    (&[b"SINTERCARD", b"2", b"a1", b"a2", b"LIMIT", b"1"], b":1\r\n"),
    (&[b"SINTERCARD", b"2", b"a1", b"a2", b"LIMIT", b"0"], b":2\r\n"),
    (&[b"SINTERCARD", b"1", b"a1"], b":4\r\n"),
    (&[b"SINTERCARD", b"0", b"a1"], b"-ERR numkeys should be greater than 0\r\n"),
    (&[b"SINTERCARD", b"x", b"a1"], b"-ERR numkeys should be greater than 0\r\n"),
    (&[b"SINTERCARD", b"3", b"a1", b"a2"], b"-ERR Number of keys can't be greater than number of args\r\n"),
    (&[b"SINTERCARD", b"2", b"a1", b"a2", b"LIMIT", b"-1"], b"-ERR LIMIT can't be negative\r\n"),
    (&[b"SINTERCARD", b"2", b"a1", b"a2", b"LIMIT", b"x"], b"-ERR LIMIT can't be negative\r\n"),
    (&[b"SINTERCARD", b"2", b"a1", b"a2", b"LIMIT"], b"-ERR syntax error\r\n"),
    (&[b"SINTERCARD", b"2", b"a1", b"a2", b"BAD", b"1"], b"-ERR syntax error\r\n"),
    (&[b"SINTERCARD", b"2", b"a1", b"str"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SSCAN", b"a1", b"0"], b"*2\r\n$1\r\n0\r\n*4\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n"),
    (&[b"SSCAN", b"a3", b"0"], b"*2\r\n$1\r\n0\r\n*3\r\n$1\r\nx\r\n$1\r\n4\r\n$1\r\n3\r\n"),
    (&[b"SSCAN", b"a1", b"0", b"MATCH", b"1*"], b"*2\r\n$1\r\n0\r\n*1\r\n$1\r\n1\r\n"),
    (&[b"SSCAN", b"a3", b"0", b"MATCH", b"x"], b"*2\r\n$1\r\n0\r\n*1\r\n$1\r\nx\r\n"),
    (&[b"SSCAN", b"a1", b"0", b"COUNT", b"1"], b"*2\r\n$1\r\n0\r\n*4\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n"),
    // Redis 7.2 has no NOVALUES (a syntax error); 7.4, which added it, says this.
    (&[b"SSCAN", b"a1", b"0", b"NOVALUES"], b"-ERR NOVALUES option can only be used in HSCAN\r\n"),
    (&[b"SSCAN", b"a1", b"0", b"TYPE", b"string"], b"-ERR syntax error\r\n"),
    (&[b"SSCAN", b"a1", b"abc"], b"-ERR invalid cursor\r\n"),
    (&[b"SSCAN", b"nokey", b"0"], b"*2\r\n$1\r\n0\r\n*0\r\n"),
    (&[b"SSCAN", b"a1", b"0", b"COUNT", b"0"], b"-ERR syntax error\r\n"),
    (&[b"SADD", b"t", b"a"], b":1\r\n"),
    (&[b"EXPIRE", b"t", b"100"], b":1\r\n"),
    (&[b"SADD", b"t", b"b"], b":1\r\n"),
    (&[b"SREM", b"t", b"a"], b":1\r\n"),
    (&[b"TTL", b"t"], b":100\r\n"),
    (&[b"DBSIZE"], b":12\r\n"),
    // Redis reports its own client id here.
    (&[b"HELLO", b"3"], b"%7\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.2.4\r\n$5\r\nproto\r\n:3\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n"),
    (&[b"SMEMBERS", b"a1"], b"~4\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n"),
    (&[b"SINTER", b"a1", b"a2"], b"~2\r\n$1\r\n3\r\n$1\r\n4\r\n"),
    (&[b"SUNION", b"a1", b"a2"], b"~5\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n"),
    (&[b"SDIFF", b"a1", b"a2"], b"~2\r\n$1\r\n1\r\n$1\r\n2\r\n"),
    (&[b"SRANDMEMBER", b"a1", b"10"], b"*4\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n"),
    (&[b"SRANDMEMBER", b"a1", b"0"], b"*0\r\n"),
    (&[b"SMEMBERS", b"nokey"], b"~0\r\n"),
    (&[b"SPOP", b"nokey", b"2"], b"~0\r\n"),
    (&[b"SPOP", b"a2", b"10"], b"~3\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n"),
    (&[b"SMISMEMBER", b"a1", b"1", b"9"], b"*2\r\n:1\r\n:0\r\n"),
];

#[tokio::test]
async fn replies_match_redis() {
    common::replay(SCRIPT).await;
}

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use common::Value;
use tantalum::context::{Context, Settings};
use tantalum::store::Store;

/// Random set commands through `cmd::execute` against a model, at every
/// size: packed integers, packed strings, and big sets past both limits,
/// with members coming and going across them.
#[test]
fn matches_a_model() {
    let ctx = Context::new(Arc::new(Store::new(4)), Settings::default());
    let client = ctx.connect(-1).unwrap();
    let mut session = client.session();
    let mut model: HashMap<Vec<u8>, BTreeSet<Vec<u8>>> = HashMap::new();
    let mut rng = 0x9E37_79B9_7F4A_7C15u64;
    let mut below = move |n: u64| {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng % n
    };
    for step in 0..8_000 {
        let key = format!("s{}", below(4)).into_bytes();
        let other = format!("s{}", below(4)).into_bytes();
        let count = if below(20) == 0 {
            200
        } else {
            1 + below(4) as usize
        };
        // Integers, short strings, and now and then a long member.
        let members: Vec<Vec<u8>> = (0..count)
            .map(|_| match below(10) {
                0..5 => below(700).to_string().into_bytes(),
                5..9 => format!("m{}", below(300)).into_bytes(),
                _ => vec![b'L'; 70 + below(3) as usize],
            })
            .collect();
        let mut run =
            |args: Vec<&[u8]>| common::parse(&common::execute(&ctx, &mut session, 1, &args));
        let set = model.get(&key).cloned().unwrap_or_default();
        let label = format!("step {step}");
        match below(12) {
            0..4 => {
                let mut args = vec![&b"SADD"[..], &key];
                args.extend(members.iter().map(Vec::as_slice));
                let added = members
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>()
                    .difference(&set)
                    .count();
                assert_eq!(run(args).int(), added as i64, "{label}");
                model.entry(key.clone()).or_default().extend(members);
            }
            4 | 5 => {
                let mut args = vec![&b"SREM"[..], &key];
                args.extend(members.iter().map(Vec::as_slice));
                let gone: BTreeSet<_> = members.iter().cloned().collect();
                assert_eq!(
                    run(args).int(),
                    gone.intersection(&set).count() as i64,
                    "{label}"
                );
                let left: BTreeSet<_> = set.difference(&gone).cloned().collect();
                if left.is_empty() {
                    model.remove(&key);
                } else {
                    model.insert(key.clone(), left);
                }
            }
            6 => {
                let got: BTreeSet<_> = run(vec![b"SMEMBERS", &key]).bulks().into_iter().collect();
                assert_eq!(got, set, "{label}");
                assert_eq!(run(vec![b"SCARD", &key]).int(), set.len() as i64, "{label}");
                let found = run(vec![b"SISMEMBER", &key, &members[0]]).int();
                assert_eq!(found == 1, set.contains(&members[0]), "{label}");
            }
            7 => {
                let n = below(5);
                let popped = run(vec![b"SPOP", &key, n.to_string().as_bytes()]).bulks();
                assert_eq!(popped.len(), (n as usize).min(set.len()), "{label}");
                let popped: BTreeSet<_> = popped.into_iter().collect();
                assert!(popped.is_subset(&set), "{label}");
                let left: BTreeSet<_> = set.difference(&popped).cloned().collect();
                if left.is_empty() {
                    model.remove(&key);
                } else {
                    model.insert(key.clone(), left);
                }
            }
            8 => {
                let n = 1 + below(5);
                let picked = run(vec![b"SRANDMEMBER", &key, n.to_string().as_bytes()]).bulks();
                let distinct: BTreeSet<_> = picked.iter().cloned().collect();
                assert_eq!(picked.len(), (n as usize).min(set.len()), "{label}");
                assert_eq!(distinct.len(), picked.len(), "{label}: repeats");
                assert!(distinct.is_subset(&set), "{label}");
            }
            9 => {
                let m = &members[0];
                let moved = run(vec![b"SMOVE", &key, &other, m]).int();
                assert_eq!(moved == 1, set.contains(m), "{label}");
                if moved == 1 && key != other {
                    let from = model.get_mut(&key).unwrap();
                    from.remove(m);
                    if from.is_empty() {
                        model.remove(&key);
                    }
                    model.entry(other.clone()).or_default().insert(m.clone());
                }
            }
            10 => {
                let (op, store) = [
                    (&b"SINTER"[..], &b"SINTERSTORE"[..]),
                    (b"SUNION", b"SUNIONSTORE"),
                    (b"SDIFF", b"SDIFFSTORE"),
                ][below(3) as usize];
                let b = model.get(&other).cloned().unwrap_or_default();
                let expected: BTreeSet<Vec<u8>> = match op {
                    b"SINTER" => set.intersection(&b).cloned().collect(),
                    b"SUNION" => set.union(&b).cloned().collect(),
                    _ => set.difference(&b).cloned().collect(),
                };
                let got: BTreeSet<_> = run(vec![op, &key, &other]).bulks().into_iter().collect();
                assert_eq!(got, expected, "{label}");
                let dst = b"dst".to_vec();
                assert_eq!(
                    run(vec![store, &dst, &key, &other]).int(),
                    expected.len() as i64,
                    "{label}"
                );
                if expected.is_empty() {
                    model.remove(&dst);
                } else {
                    model.insert(dst, expected);
                }
            }
            _ => {
                // A whole SSCAN, small steps.
                let mut seen = BTreeSet::new();
                let mut cursor = b"0".to_vec();
                loop {
                    let Value::Array(reply) = run(vec![b"SSCAN", &key, &cursor, b"COUNT", b"7"])
                    else {
                        panic!("{label}: not an array");
                    };
                    let Value::Bulk(next) = &reply[0] else {
                        panic!()
                    };
                    seen.extend(reply[1].bulks());
                    if next == b"0" {
                        break;
                    }
                    cursor = next.clone();
                }
                assert_eq!(seen, set, "{label}");
            }
        }
    }
    // Big ones were made, and some shrank back.
    assert!(model.values().any(|s| s.len() > 128));
}
