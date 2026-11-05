//! Random commands, valid and not, run straight through `cmd::execute`:
//! none may panic, and every reply must be well-formed RESP. Cheap enough
//! for every test run; `fuzz/` has the coverage-guided version.

use std::sync::Arc;

use tantalum::cmd::{self, Args, Reply};
use tantalum::context::{Context, Settings};
use tantalum::store::{Config, EvictionPolicy, Store};

/// Deterministic xorshift, so a failure reproduces.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn pick<'a>(&mut self, items: &[&'a [u8]]) -> &'a [u8] {
        items[self.below(items.len())]
    }
}

const COMMANDS: &[&[u8]] = &[
    b"GET",
    b"SET",
    b"MGET",
    b"MSET",
    b"DEL",
    b"UNLINK",
    b"EXISTS",
    b"EXPIRE",
    b"PEXPIRE",
    b"EXPIREAT",
    b"PEXPIREAT",
    b"TTL",
    b"PTTL",
    b"EXPIRETIME",
    b"PEXPIRETIME",
    b"PERSIST",
    b"SETBIT",
    b"GETBIT",
    b"BITCOUNT",
    b"BITPOS",
    b"BITOP",
    b"BITFIELD",
    b"BITFIELD_RO",
    b"STRLEN",
    b"APPEND",
    b"GETRANGE",
    b"SUBSTR",
    b"SETRANGE",
    b"GETSET",
    b"INCRBYFLOAT",
    b"MSETNX",
    b"LCS",
    b"TOUCH",
    b"RANDOMKEY",
    b"COPY",
    b"OBJECT",
    b"SORT",
    b"SORT_RO",
    b"HINCRBYFLOAT",
    b"HRANDFIELD",
    b"INCR",
    b"DECR",
    b"INCRBY",
    b"DECRBY",
    b"SETEX",
    b"PSETEX",
    b"SETNX",
    b"GETDEL",
    b"GETEX",
    b"PING",
    b"ECHO",
    b"SELECT",
    b"DBSIZE",
    b"INFO",
    b"AUTH",
    b"HELLO",
    b"CLIENT",
    b"COMMAND",
    b"CONFIG",
    b"FLUSHALL",
    b"FLUSHDB",
    b"TYPE",
    b"HSET",
    b"HSETNX",
    b"HMSET",
    b"HGET",
    b"HMGET",
    b"HGETALL",
    b"HDEL",
    b"HEXISTS",
    b"HLEN",
    b"HKEYS",
    b"HVALS",
    b"HSTRLEN",
    b"HINCRBY",
    b"HSCAN",
    b"SCAN",
    b"KEYS",
    b"RENAME",
    b"RENAMENX",
    b"SADD",
    b"SREM",
    b"SISMEMBER",
    b"SMISMEMBER",
    b"SMEMBERS",
    b"SCARD",
    b"SPOP",
    b"SRANDMEMBER",
    b"SMOVE",
    b"SINTER",
    b"SINTERSTORE",
    b"SINTERCARD",
    b"SUNION",
    b"SUNIONSTORE",
    b"SDIFF",
    b"SDIFFSTORE",
    b"SSCAN",
    b"LPUSH",
    b"RPUSH",
    b"LPUSHX",
    b"RPUSHX",
    b"LPOP",
    b"RPOP",
    b"LLEN",
    b"LRANGE",
    b"LINDEX",
    b"LSET",
    b"LINSERT",
    b"LREM",
    b"LTRIM",
    b"LPOS",
    b"LMOVE",
    b"RPOPLPUSH",
    b"LMPOP",
    b"BLPOP",
    b"BRPOP",
    b"BLMOVE",
    b"BRPOPLPUSH",
    b"BLMPOP",
    b"ZADD",
    b"ZINCRBY",
    b"ZREM",
    b"ZCARD",
    b"ZSCORE",
    b"ZMSCORE",
    b"ZRANK",
    b"ZREVRANK",
    b"ZRANGE",
    b"ZRANGESTORE",
    b"ZREVRANGE",
    b"ZRANGEBYSCORE",
    b"ZREVRANGEBYSCORE",
    b"ZRANGEBYLEX",
    b"ZREVRANGEBYLEX",
    b"ZCOUNT",
    b"ZLEXCOUNT",
    b"ZREMRANGEBYRANK",
    b"ZREMRANGEBYSCORE",
    b"ZREMRANGEBYLEX",
    b"ZPOPMIN",
    b"ZPOPMAX",
    b"ZMPOP",
    b"BZPOPMIN",
    b"BZPOPMAX",
    b"BZMPOP",
    b"SUBSCRIBE",
    b"UNSUBSCRIBE",
    b"PSUBSCRIBE",
    b"PUNSUBSCRIBE",
    b"SSUBSCRIBE",
    b"SUNSUBSCRIBE",
    b"PUBLISH",
    b"SPUBLISH",
    b"PUBSUB",
    b"RESET",
    b"MULTI",
    b"EXEC",
    b"DISCARD",
    b"WATCH",
    b"UNWATCH",
    b"TIME",
    b"EVAL",
    b"EVALSHA",
    b"EVAL_RO",
    b"SCRIPT",
    b"XADD",
    b"XLEN",
    b"XRANGE",
    b"XREVRANGE",
    b"XDEL",
    b"XTRIM",
    b"XREAD",
    b"XREADGROUP",
    b"XGROUP",
    b"XACK",
    b"XPENDING",
    b"XCLAIM",
    b"XAUTOCLAIM",
    b"XINFO",
    b"XSETID",
    b"ZRANDMEMBER",
    b"ZSCAN",
    b"ZUNION",
    b"ZINTER",
    b"ZDIFF",
    b"ZUNIONSTORE",
    b"ZINTERSTORE",
    b"ZDIFFSTORE",
    b"ZINTERCARD",
    b"hget",
    b"get",
    b"sEt",
    b"NOPE",
];

const WORDS: &[&[u8]] = &[
    b"k",
    b"k1",
    b"k2",
    b"counter",
    b"return 1",
    b"return redis.call('get', KEYS[1])",
    b"redis.call('set', KEYS[1], 'v') return redis.pcall('incr', KEYS[1])",
    b"return {1, 'a', false, {ok='x'}, {err='E'}, {double=1.5}, {map={a=1}}}",
    b"error('x')",
    b"return redis.call('blpop', KEYS[1], 0)",
    b"redis.setresp(3) return redis.call('hgetall', KEYS[1])",
    b"return cjson.encode({ARGV[1], cmsgpack.unpack(cmsgpack.pack({1, 2}))})",
    b"LOAD",
    b"EXISTS",
    b"FLUSH",
    b"KILL",
    b"e0e1f9fabfc9d4800c877a703b823ac0578ff8db",
    b"",
    b"0",
    b"1",
    b"-1",
    b"2",
    b"3",
    b"10",
    b"100",
    b"99999999999",
    b"9223372036854775807",
    b"-9223372036854775808",
    b"9223372036854775808",
    b"1.5",
    b"abc",
    b" 1",
    b"EX",
    b"PX",
    b"EXAT",
    b"PXAT",
    b"NX",
    b"XX",
    b"GT",
    b"LT",
    b"GET",
    b"KEEPTTL",
    b"PERSIST",
    b"ASYNC",
    b"SYNC",
    b"AUTH",
    b"SETNAME",
    b"default",
    b"GETNAME",
    b"ID",
    b"SETINFO",
    b"LIB-NAME",
    b"LIB-VER",
    b"COUNT",
    b"INFO",
    b"LIST",
    b"DOCS",
    b"MATCH",
    b"LIMIT",
    b"LEFT",
    b"RIGHT",
    b"BEFORE",
    b"AFTER",
    b"RANK",
    b"MAXLEN",
    b"WITHSCORES",
    b"BYSCORE",
    b"BYLEX",
    b"REV",
    b"INCR",
    b"CH",
    b"WEIGHTS",
    b"AGGREGATE",
    b"MIN",
    b"MAX",
    b"inf",
    b"-inf",
    b"(1",
    b"[a",
    b"+",
    b"-",
    b"0.5",
    b"-5",
    b"TYPE",
    b"string",
    b"hash",
    b"4294967296",
    b"NOVALUES",
    b"f",
    b"k*",
    b"[a-z]",
    b"\\",
    b"maxmemory*",
    b"*",
    b"?",
    b"name with space",
    b"\r\n",
    b"\0\xff",
    b"pw",
    b"CHANNELS",
    b"NUMSUB",
    b"NUMPAT",
    b"SHARDCHANNELS",
    b"SHARDNUMSUB",
];

/// A well-formed blocking command, or a write that may serve one, on a
/// few keys: random arguments rarely line those up.
fn blocking_or_serving(rng: &mut Rng) -> Vec<Vec<u8>> {
    const KEYS: &[&[u8]] = &[b"k", b"k1", b"k2"];
    let choice = rng.below(38);
    let mut key = || rng.pick(KEYS).to_vec();
    let words = |w: &[&str]| w.iter().map(|w| w.as_bytes().to_vec()).collect::<Vec<_>>();
    let mut args = match choice {
        0 => vec![b"BLPOP".to_vec(), key(), key()],
        1 => vec![b"BRPOP".to_vec(), key()],
        2 => [
            vec![b"BLMOVE".to_vec(), key(), key()],
            words(&["LEFT", "RIGHT"]),
        ]
        .concat(),
        3 => vec![b"BRPOPLPUSH".to_vec(), key(), key()],
        4 => [
            words(&["BLMPOP", "0", "2"]),
            vec![key(), key()],
            words(&["LEFT", "COUNT", "2"]),
        ]
        .concat(),
        5 => vec![b"BZPOPMIN".to_vec(), key(), key()],
        6 => [words(&["BZMPOP", "0", "1"]), vec![key()], words(&["MAX"])].concat(),
        7 => [vec![b"RPUSH".to_vec(), key()], words(&["a", "b", "c"])].concat(),
        8 => [vec![b"LPUSH".to_vec(), key()], words(&["x"])].concat(),
        9 => [vec![b"ZADD".to_vec(), key()], words(&["1", "m", "2", "n"])].concat(),
        10 => [
            vec![b"LMOVE".to_vec(), key(), key()],
            words(&["LEFT", "LEFT"]),
        ]
        .concat(),
        11 => vec![b"RENAME".to_vec(), key(), key()],
        12 => vec![b"SET".to_vec(), key(), b"v".to_vec()],
        13 => vec![b"SUBSCRIBE".to_vec(), key(), key()],
        14 => vec![b"PSUBSCRIBE".to_vec(), b"k*".to_vec()],
        15 => vec![b"SSUBSCRIBE".to_vec(), key()],
        16 => vec![b"PUBLISH".to_vec(), key(), b"m".to_vec()],
        17 => vec![b"SPUBLISH".to_vec(), key(), b"m".to_vec()],
        18 => vec![b"UNSUBSCRIBE".to_vec(), key()],
        19 => [vec![b"XADD".to_vec(), key()], words(&["*", "f", "v"])].concat(),
        20 => [
            vec![b"XADD".to_vec(), key()],
            words(&["MAXLEN", "~", "3", "*", "a", "1", "b", "2"]),
        ]
        .concat(),
        21 => [
            words(&["XREAD", "BLOCK", "0", "STREAMS"]),
            vec![key(), b"$".to_vec()],
        ]
        .concat(),
        22 => [vec![b"XTRIM".to_vec(), key()], words(&["MAXLEN", "1"])].concat(),
        23 => [vec![b"XDEL".to_vec(), key()], words(&["0-1"])].concat(),
        24 => [
            words(&["EVAL", "redis.call('rpush', KEYS[1], ARGV[1]) return redis.call('publish', KEYS[2], ARGV[1])", "2"]),
            vec![key(), key(), b"x".to_vec()],
        ]
        .concat(),
        25 => [
            words(&["EVAL", "redis.call('xadd', KEYS[1], '*', 'f', 'v') return redis.call('zadd', KEYS[2], 1, ARGV[1])", "2"]),
            vec![key(), key(), b"m".to_vec()],
        ]
        .concat(),
        26 => [
            words(&["XGROUP", "CREATE"]),
            vec![key()],
            words(&["g", "0", "MKSTREAM"]),
        ]
        .concat(),
        27 => [
            words(&["XREADGROUP", "GROUP", "g", "c", "BLOCK", "0", "STREAMS"]),
            vec![key(), b">".to_vec()],
        ]
        .concat(),
        28 => [
            words(&["XREADGROUP", "GROUP", "g", "d", "COUNT", "1", "STREAMS"]),
            vec![key(), key(), b">".to_vec(), b"0".to_vec()],
        ]
        .concat(),
        29 => [words(&["XGROUP", "DESTROY"]), vec![key()], words(&["g"])].concat(),
        30 => [vec![b"XACK".to_vec(), key()], words(&["g", "0-1"])].concat(),
        31 => [
            vec![b"XAUTOCLAIM".to_vec(), key()],
            words(&["g", "e", "0", "0"]),
        ]
        .concat(),
        32 => [words(&["XINFO", "STREAM"]), vec![key()], words(&["FULL"])].concat(),
        33 => [
            words(&["XGROUP", "DELCONSUMER"]),
            vec![key()],
            words(&["g", "c"]),
        ]
        .concat(),
        // A hash past its packed limits, to change in place, and fields to find.
        34 => [
            vec![b"HSET".to_vec(), key(), b"long".to_vec()],
            vec![vec![b'l'; 9_000]],
        ]
        .concat(),
        35 => [vec![b"HDEL".to_vec(), key()], words(&["long", "a"])].concat(),
        36 => [
            words(&["HSCAN"]),
            vec![key()],
            words(&["0", "COUNT", "2"]),
        ]
        .concat(),
        _ => vec![b"DEL".to_vec(), key()],
    };
    // The blocking ones take a timeout last, except the *MPOPs: first.
    match args[0].as_slice() {
        b"BLMPOP" | b"BZMPOP" => {}
        b"BLPOP" | b"BRPOP" | b"BLMOVE" | b"BRPOPLPUSH" | b"BZPOPMIN" => {
            args.push(if rng.below(4) == 0 {
                b"0.01".to_vec()
            } else {
                b"0".to_vec()
            })
        }
        _ => {}
    }
    args
}

fn random_arg(rng: &mut Rng) -> Vec<u8> {
    match rng.below(10) {
        0 => (0..rng.below(40)).map(|_| rng.next() as u8).collect(),
        1 => vec![b'v'; rng.below(3) * 10_000], // some values big enough to be external
        2 => rng.next().to_string().into_bytes(),
        _ => rng.pick(WORDS).to_vec(),
    }
}

/// Checks `out` holds complete RESP replies, and returns how many.
fn count_replies(out: &[u8]) -> usize {
    let mut pos = 0;
    let mut replies = 0;
    let mut pending = 0usize;
    while pos < out.len() {
        let end = pos
            + out[pos..]
                .windows(2)
                .position(|w| w == b"\r\n")
                .expect("reply line without CRLF");
        let line = &out[pos..end];
        pos = end + 2;
        let len = || {
            std::str::from_utf8(&line[1..])
                .unwrap()
                .parse::<i64>()
                .unwrap()
        };
        match line[0] {
            b'+' | b'-' | b':' | b'_' | b',' => {}
            b'$' | b'=' if len() >= 0 => {
                pos += len() as usize + 2;
                assert_eq!(&out[pos - 2..pos], b"\r\n", "bulk without CRLF");
            }
            b'$' => {}
            b'*' | b'~' => pending += len().max(0) as usize,
            b'%' => pending += 2 * len().max(0) as usize,
            other => panic!("bad reply type {:?}", other as char),
        }
        if pending > 0 {
            pending -= 1;
        } else {
            replies += 1;
        }
    }
    assert!(pos == out.len() && pending == 0, "truncated reply");
    replies
}

#[test]
fn random_commands_never_panic() {
    for (seed, settings) in [
        (1u64, Settings::default()),
        (
            2,
            Settings {
                password: Some(b"pw".to_vec()),
                ..Settings::default()
            },
        ),
        (3, Settings::default()),
    ] {
        let store = Store::with_config(Config {
            shards: 4,
            maxmemory: 2 << 20,
            policy: if seed == 3 {
                EvictionPolicy::NoEviction
            } else {
                EvictionPolicy::AllKeysLru
            },
            ..Config::default()
        });
        let ctx = Context::new(Arc::new(store), settings);
        let client = ctx.connect(-1).unwrap();
        let mut session = client.session();
        // Subscribing happens on a second client, which the first one's
        // PUBLISHes reach: subscribed, the first could run little else.
        let listener_client = ctx.connect(-1).unwrap();
        let mut listener = listener_client.session();
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let mut now = 1;
        let mut out = Vec::new();
        let mut blocked = Vec::new();
        for step in 0..100_000 {
            // Transactions end soon, so most commands run.
            let args = if session.in_transaction() && rng.below(4) == 0 {
                vec![if rng.below(4) == 0 {
                    b"DISCARD".to_vec()
                } else {
                    b"EXEC".to_vec()
                }]
            } else if rng.below(6) == 0 {
                blocking_or_serving(&mut rng)
            } else {
                let mut args = vec![rng.pick(COMMANDS).to_vec()];
                for _ in 0..rng.below(7) {
                    args.push(random_arg(&mut rng));
                }
                args
            };
            let mut buf = Vec::new();
            let mut ranges = Vec::new();
            for arg in &args {
                ranges.push(buf.len()..buf.len() + arg.len());
                buf.extend_from_slice(arg);
            }
            out.clear();
            let subscribing = [
                &b"SUBSCRIBE"[..],
                b"UNSUBSCRIBE",
                b"PSUBSCRIBE",
                b"PUNSUBSCRIBE",
                b"SSUBSCRIBE",
                b"SUNSUBSCRIBE",
                b"RESET",
            ]
            .contains(&args[0].as_slice());
            let session = if subscribing || rng.below(20) == 0 {
                &mut listener
            } else {
                &mut session
            };
            // A subscriber's output also holds messages that came before
            // the command, and (un)subscribing replies once per channel.
            let several = subscribing || session.is_subscribed();
            match cmd::execute(&ctx, session, now, &Args::new(&buf, &ranges), &mut out) {
                Reply::Bulk(value) => {
                    if !several {
                        assert_eq!(count_replies(&out), 0, "step {step}");
                    }
                    assert!(!value.as_bytes().is_empty());
                }
                Reply::Written | Reply::Close if several => {
                    assert!(count_replies(&out) >= 1, "step {step}: {args:?}");
                }
                Reply::Written | Reply::Close => {
                    assert_eq!(count_replies(&out), 1, "step {step}: {args:?}");
                }
                // Blocked: nothing written yet. It waits while later
                // commands run, which may serve it.
                Reply::Block(b) => {
                    assert!(several || out.is_empty(), "step {step}");
                    blocked.push(b);
                }
            }
            // Now and then one is done waiting: its one reply (served, or
            // timed out), or its client leaves.
            if !blocked.is_empty() && rng.below(32) == 0 {
                let b: Box<tantalum::cmd::Blocked> = blocked.swap_remove(rng.below(blocked.len()));
                if rng.below(2) == 0 {
                    let mut reply = Vec::new();
                    b.finish(&mut reply);
                    assert_eq!(count_replies(&reply), 1, "step {step}");
                }
            }
            now += rng.below(50) as u64;
        }
    }
}
