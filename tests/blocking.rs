//! Blocking commands (BLPOP and the rest): replies recorded from Redis
//! 7.2.4 (`tools/redis-diff.py --rust`), including timeouts, and clients
//! blocking and being served over real connections in every I/O mode. The
//! multi-client cases were checked against Redis 7.2.4 too.

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{Client, MODES, start};
use tantalum::context::Context;
use tokio::io::AsyncReadExt;

#[rustfmt::skip]
const SCRIPT: &[(&[&[u8]], &[u8])] = &[
    (&[b"BLPOP"], b"-ERR wrong number of arguments for 'blpop' command\r\n"),
    (&[b"BLPOP", b"k"], b"-ERR wrong number of arguments for 'blpop' command\r\n"),
    (&[b"BRPOP", b"k"], b"-ERR wrong number of arguments for 'brpop' command\r\n"),
    (&[b"BLMOVE", b"a", b"b", b"left"], b"-ERR wrong number of arguments for 'blmove' command\r\n"),
    (&[b"BLMOVE", b"a", b"b", b"left", b"right", b"0", b"x"], b"-ERR wrong number of arguments for 'blmove' command\r\n"),
    (&[b"BRPOPLPUSH", b"a", b"b"], b"-ERR wrong number of arguments for 'brpoplpush' command\r\n"),
    (&[b"BLMPOP", b"0", b"1"], b"-ERR wrong number of arguments for 'blmpop' command\r\n"),
    (&[b"BZPOPMIN", b"k"], b"-ERR wrong number of arguments for 'bzpopmin' command\r\n"),
    (&[b"BZPOPMAX"], b"-ERR wrong number of arguments for 'bzpopmax' command\r\n"),
    (&[b"BZMPOP", b"0", b"1", b"k"], b"-ERR wrong number of arguments for 'bzmpop' command\r\n"),
    (&[b"BLPOP", b"k", b"abc"], b"-ERR timeout is not a float or out of range\r\n"),
    (&[b"BLPOP", b"k", b"-1"], b"-ERR timeout is negative\r\n"),
    (&[b"BLPOP", b"k", b"inf"], b"-ERR timeout is out of range\r\n"),
    (&[b"BLPOP", b"k", b"-inf"], b"-ERR timeout is negative\r\n"),
    (&[b"BLPOP", b"k", b"+inf"], b"-ERR timeout is out of range\r\n"),
    (&[b"BLPOP", b"k", b"1e300"], b"-ERR timeout is out of range\r\n"),
    (&[b"BLPOP", b"k", b"nan"], b"-ERR timeout is not a float or out of range\r\n"),
    (&[b"BLPOP", b"k", b" 1"], b"-ERR timeout is not a float or out of range\r\n"),
    (&[b"BLPOP", b"k", b"1 "], b"-ERR timeout is not a float or out of range\r\n"),
    (&[b"BLPOP", b"k", b""], b"-ERR timeout is not a float or out of range\r\n"),
    (&[b"BLPOP", b"k", b"9223372036854775807"], b"-ERR timeout is out of range\r\n"),
    (&[b"BLPOP", b"k", b"9223372036854775.807"], b"-ERR timeout is out of range\r\n"),
    (&[b"BLPOP", b"k", b"9223372036854775"], b"-ERR timeout is out of range\r\n"),
    (&[b"BLPOP", b"k", b"1e-400"], b"*-1\r\n"),
    (&[b"BLPOP", b"k", b"0.001"], b"*-1\r\n"),
    (&[b"BLPOP", b"k", b"-0.0001x"], b"-ERR timeout is not a float or out of range\r\n"),
    (&[b"BRPOP", b"k", b"0.01"], b"*-1\r\n"),
    (&[b"RPUSH", b"q", b"a", b"b", b"c"], b":3\r\n"),
    (&[b"RPUSH", b"h", b"x", b"y"], b":2\r\n"),
    (&[b"BLPOP", b"h", b"0x10"], b"*2\r\n$1\r\nh\r\n$1\r\nx\r\n"),
    (&[b"BLPOP", b"h", b"0x1p-2"], b"*2\r\n$1\r\nh\r\n$1\r\ny\r\n"),
    (&[b"BLPOP", b"none", b"q", b"0"], b"*2\r\n$1\r\nq\r\n$1\r\na\r\n"),
    (&[b"BRPOP", b"none", b"q", b"0"], b"*2\r\n$1\r\nq\r\n$1\r\nc\r\n"),
    (&[b"SET", b"s", b"v"], b"+OK\r\n"),
    (&[b"BLPOP", b"s", b"q", b"0"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"BLPOP", b"q", b"s", b"0"], b"*2\r\n$1\r\nq\r\n$1\r\nb\r\n"),
    (&[b"BRPOP", b"none", b"s", b"0"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"BLPOP", b"none", b"0.01"], b"*-1\r\n"),
    (&[b"RPUSH", b"q", b"z"], b":1\r\n"),
    (&[b"BLPOP", b"q", b"q", b"0"], b"*2\r\n$1\r\nq\r\n$1\r\nz\r\n"),
    (&[b"LRANGE", b"q", b"0", b"-1"], b"*0\r\n"),
    (&[b"RPUSH", b"src", b"1", b"2", b"3"], b":3\r\n"),
    (&[b"BLMOVE", b"src", b"dst", b"left", b"right", b"0"], b"$1\r\n1\r\n"),
    (&[b"BLMOVE", b"src", b"dst", b"up", b"right", b"0"], b"-ERR syntax error\r\n"),
    (&[b"BLMOVE", b"src", b"dst", b"left", b"down", b"0"], b"-ERR syntax error\r\n"),
    (&[b"BLMOVE", b"src", b"dst", b"left", b"right", b"x"], b"-ERR timeout is not a float or out of range\r\n"),
    (&[b"BLMOVE", b"src", b"dst", b"left", b"right", b"-1"], b"-ERR timeout is negative\r\n"),
    (&[b"BLMOVE", b"src", b"s", b"left", b"right", b"0"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"BLMOVE", b"s", b"dst", b"left", b"right", b"0"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"BLMOVE", b"empty", b"dst", b"left", b"right", b"0.01"], b"*-1\r\n"),
    (&[b"BLMOVE", b"empty", b"s", b"left", b"right", b"0.01"], b"*-1\r\n"),
    (&[b"BLMOVE", b"s", b"empty", b"left", b"right", b"0.01"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"BRPOPLPUSH", b"src", b"dst", b"0"], b"$1\r\n3\r\n"),
    (&[b"BRPOPLPUSH", b"src", b"dst", b"x"], b"-ERR timeout is not a float or out of range\r\n"),
    (&[b"BRPOPLPUSH", b"empty", b"dst", b"0.01"], b"*-1\r\n"),
    (&[b"BRPOPLPUSH", b"src", b"s", b"0"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"BRPOPLPUSH", b"src", b"src", b"0"], b"$1\r\n2\r\n"),
    (&[b"LRANGE", b"src", b"0", b"-1"], b"*1\r\n$1\r\n2\r\n"),
    (&[b"LRANGE", b"dst", b"0", b"-1"], b"*2\r\n$1\r\n3\r\n$1\r\n1\r\n"),
    (&[b"RPUSH", b"m1", b"a", b"b", b"c", b"d", b"e"], b":5\r\n"),
    (&[b"BLMPOP", b"0", b"1", b"m1", b"left"], b"*2\r\n$2\r\nm1\r\n*1\r\n$1\r\na\r\n"),
    (&[b"BLMPOP", b"0", b"1", b"m1", b"right", b"count", b"2"], b"*2\r\n$2\r\nm1\r\n*2\r\n$1\r\ne\r\n$1\r\nd\r\n"),
    (&[b"BLMPOP", b"0", b"2", b"none", b"m1", b"left", b"count", b"10"], b"*2\r\n$2\r\nm1\r\n*2\r\n$1\r\nb\r\n$1\r\nc\r\n"),
    (&[b"BLMPOP", b"0", b"0", b"m1", b"left"], b"-ERR numkeys should be greater than 0\r\n"),
    (&[b"BLMPOP", b"0", b"-1", b"m1", b"left"], b"-ERR numkeys should be greater than 0\r\n"),
    (&[b"BLMPOP", b"0", b"x", b"m1", b"left"], b"-ERR numkeys should be greater than 0\r\n"),
    (&[b"BLMPOP", b"0", b"5", b"m1", b"left"], b"-ERR syntax error\r\n"),
    (&[b"BLMPOP", b"0", b"1", b"m1", b"middle"], b"-ERR syntax error\r\n"),
    (&[b"BLMPOP", b"0", b"1", b"m1", b"left", b"count", b"0"], b"-ERR count should be greater than 0\r\n"),
    (&[b"BLMPOP", b"0", b"1", b"m1", b"left", b"count", b"-1"], b"-ERR count should be greater than 0\r\n"),
    (&[b"BLMPOP", b"0", b"1", b"m1", b"left", b"count", b"x"], b"-ERR count should be greater than 0\r\n"),
    (&[b"BLMPOP", b"0", b"1", b"m1", b"left", b"count", b"1", b"count", b"2"], b"-ERR syntax error\r\n"),
    (&[b"BLMPOP", b"0", b"1", b"m1", b"left", b"foo"], b"-ERR syntax error\r\n"),
    (&[b"BLMPOP", b"0", b"1", b"m1", b"left", b"count"], b"-ERR syntax error\r\n"),
    (&[b"BLMPOP", b"x", b"1", b"m1", b"left"], b"-ERR timeout is not a float or out of range\r\n"),
    (&[b"BLMPOP", b"-1", b"1", b"m1", b"left"], b"-ERR timeout is negative\r\n"),
    (&[b"BLMPOP", b"x", b"0", b"m1", b"left"], b"-ERR numkeys should be greater than 0\r\n"),
    (&[b"BLMPOP", b"x", b"1", b"m1", b"middle"], b"-ERR syntax error\r\n"),
    (&[b"BLMPOP", b"0.01", b"1", b"none", b"left"], b"*-1\r\n"),
    (&[b"BLMPOP", b"0.01", b"1", b"s", b"left"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"BLMPOP", b"0.01", b"2", b"none", b"s", b"left"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"], b":3\r\n"),
    (&[b"ZADD", b"z2", b"1.5", b"x"], b":1\r\n"),
    (&[b"BZPOPMIN", b"z", b"0"], b"*3\r\n$1\r\nz\r\n$1\r\na\r\n$1\r\n1\r\n"),
    (&[b"BZPOPMAX", b"z", b"0"], b"*3\r\n$1\r\nz\r\n$1\r\nc\r\n$1\r\n3\r\n"),
    (&[b"BZPOPMIN", b"none", b"z2", b"0"], b"*3\r\n$2\r\nz2\r\n$1\r\nx\r\n$3\r\n1.5\r\n"),
    (&[b"BZPOPMIN", b"z", b"abc"], b"-ERR timeout is not a float or out of range\r\n"),
    (&[b"BZPOPMIN", b"z", b"-1"], b"-ERR timeout is negative\r\n"),
    (&[b"BZPOPMIN", b"s", b"z", b"0"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"BZPOPMIN", b"dst", b"0"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"BZPOPMIN", b"none", b"0.01"], b"*-1\r\n"),
    (&[b"BZPOPMAX", b"none", b"0.01"], b"*-1\r\n"),
    (&[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"], b":2\r\n"),
    (&[b"BZMPOP", b"0", b"1", b"z", b"min"], b"*2\r\n$1\r\nz\r\n*1\r\n*2\r\n$1\r\na\r\n$1\r\n1\r\n"),
    (&[b"BZMPOP", b"0", b"1", b"z", b"max", b"count", b"5"], b"*2\r\n$1\r\nz\r\n*2\r\n*2\r\n$1\r\nc\r\n$1\r\n3\r\n*2\r\n$1\r\nb\r\n$1\r\n2\r\n"),
    (&[b"BZMPOP", b"0.01", b"1", b"none", b"min"], b"*-1\r\n"),
    (&[b"BZMPOP", b"0", b"0", b"z", b"min"], b"-ERR numkeys should be greater than 0\r\n"),
    (&[b"BZMPOP", b"0", b"1", b"z", b"mid"], b"-ERR syntax error\r\n"),
    (&[b"BZMPOP", b"0", b"1", b"z", b"min", b"count", b"0"], b"-ERR count should be greater than 0\r\n"),
    (&[b"BZMPOP", b"0", b"1", b"z", b"min", b"count", b"1", b"count", b"1"], b"-ERR syntax error\r\n"),
    (&[b"BZMPOP", b"x", b"1", b"z", b"min"], b"-ERR timeout is not a float or out of range\r\n"),
    (&[b"BZMPOP", b"0", b"1", b"s", b"min"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"BZMPOP", b"0", b"1", b"dst", b"max"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"HELLO", b"3"], b"%7\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.2.4\r\n$5\r\nproto\r\n:3\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n"),
    (&[b"RPUSH", b"r", b"a"], b":1\r\n"),
    (&[b"ZADD", b"zr", b"2.5", b"m", b"3", b"n"], b":2\r\n"),
    (&[b"BLPOP", b"r", b"0"], b"*2\r\n$1\r\nr\r\n$1\r\na\r\n"),
    (&[b"BLPOP", b"none", b"0.01"], b"_\r\n"),
    (&[b"BRPOPLPUSH", b"none", b"dst", b"0.01"], b"_\r\n"),
    (&[b"BLMOVE", b"none", b"dst", b"left", b"left", b"0.01"], b"_\r\n"),
    (&[b"BLMPOP", b"0.01", b"1", b"none", b"left"], b"_\r\n"),
    (&[b"BZPOPMIN", b"zr", b"0"], b"*3\r\n$2\r\nzr\r\n$1\r\nm\r\n,2.5\r\n"),
    (&[b"BZPOPMIN", b"none", b"0.01"], b"_\r\n"),
    (&[b"BZMPOP", b"0", b"1", b"zr", b"max"], b"*2\r\n$2\r\nzr\r\n*1\r\n*2\r\n$1\r\nn\r\n,3\r\n"),
    (&[b"BZMPOP", b"0.01", b"1", b"none", b"max"], b"_\r\n"),
];

#[tokio::test]
async fn replies_match_redis() {
    common::replay(SCRIPT).await;
}

/// Waits until `n` clients are blocked.
async fn blocked(ctx: &Context, n: usize) {
    let start = Instant::now();
    while ctx.store.blocked_clients() != n {
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "{} blocked, not {n}",
            ctx.store.blocked_clients()
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

fn pop(key: &str, element: &str) -> String {
    format!(
        "*2\r\n${}\r\n{key}\r\n${}\r\n{element}\r\n",
        key.len(),
        element.len()
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn waiters_are_served_first_come_first_served() {
    for mode in MODES {
        let (addr, ctx) = start(mode).await;
        let mut admin = Client::connect(addr).await;
        let mut waiters = Vec::new();
        for i in 0..3 {
            let mut c = Client::connect(addr).await;
            c.send(&["BLPOP", "q", "0"]).await;
            blocked(&ctx, i + 1).await;
            waiters.push(c);
        }
        assert_eq!(admin.call(&["RPUSH", "q", "a", "b"]).await, ":2\r\n");
        assert_eq!(waiters[0].reply().await, pop("q", "a"), "{mode:?}");
        assert_eq!(waiters[1].reply().await, pop("q", "b"), "{mode:?}");
        assert!(!waiters[2].has_reply().await, "{mode:?}");
        // Served right after the push: nothing is left for anyone else.
        assert_eq!(admin.call(&["LLEN", "q"]).await, ":0\r\n");
        assert_eq!(admin.call(&["RPUSH", "q", "c"]).await, ":1\r\n");
        assert_eq!(waiters[2].reply().await, pop("q", "c"), "{mode:?}");
        blocked(&ctx, 0).await;

        // One push of three serves two waiters, in the list's order.
        for (i, w) in waiters.iter_mut().take(2).enumerate() {
            w.send(&["BLPOP", "q", "0"]).await;
            blocked(&ctx, i + 1).await;
        }
        assert_eq!(admin.call(&["LPUSH", "q", "a", "b", "c"]).await, ":3\r\n");
        assert_eq!(waiters[0].reply().await, pop("q", "c"));
        assert_eq!(waiters[1].reply().await, pop("q", "b"));
        assert_eq!(
            admin.call(&["LRANGE", "q", "0", "-1"]).await,
            "*1\r\n$1\r\na\r\n"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn what_serves_a_waiter() {
    for mode in MODES {
        let (addr, ctx) = start(mode).await;
        let mut admin = Client::connect(addr).await;
        let mut c = Client::connect(addr).await;
        let mut d = Client::connect(addr).await;

        // Any of its keys.
        c.send(&["BRPOP", "k1", "k2", "0"]).await;
        blocked(&ctx, 1).await;
        admin.call(&["RPUSH", "k2", "x"]).await;
        assert_eq!(c.reply().await, pop("k2", "x"), "{mode:?}");

        // A chain: BLMOVE's push serves the client blocked on its
        // destination, all before the next command.
        c.send(&["BLMOVE", "src", "dst", "RIGHT", "LEFT", "0"])
            .await;
        blocked(&ctx, 1).await;
        d.send(&["BLPOP", "dst", "0"]).await;
        blocked(&ctx, 2).await;
        admin.call(&["RPUSH", "src", "v"]).await;
        assert_eq!(c.reply().await, "$1\r\nv\r\n");
        assert_eq!(d.reply().await, pop("dst", "v"));
        assert_eq!(admin.call(&["EXISTS", "src", "dst"]).await, ":0\r\n");

        // Only data of its kind; a key of another kind leaves it waiting.
        c.send(&["BLPOP", "k", "0"]).await;
        blocked(&ctx, 1).await;
        d.send(&["BZPOPMIN", "k", "0"]).await;
        blocked(&ctx, 2).await;
        admin.call(&["SET", "k", "v"]).await;
        admin.call(&["DEL", "k"]).await;
        admin.call(&["SADD", "k", "m"]).await;
        admin.call(&["DEL", "k"]).await;
        assert!(!c.has_reply().await && !d.has_reply().await);
        admin.call(&["ZADD", "k", "2", "m"]).await;
        assert_eq!(d.reply().await, "*3\r\n$1\r\nk\r\n$1\r\nm\r\n$1\r\n2\r\n");
        assert!(!c.has_reply().await);
        admin.call(&["RPUSH", "k", "x"]).await;
        assert_eq!(c.reply().await, pop("k", "x"));

        // RENAME, LMOVE and ZUNIONSTORE create keys too.
        c.send(&["BLPOP", "renamed", "0"]).await;
        blocked(&ctx, 1).await;
        admin.call(&["RPUSH", "tmp", "r"]).await;
        admin.call(&["RENAME", "tmp", "renamed"]).await;
        assert_eq!(c.reply().await, pop("renamed", "r"));
        c.send(&["BLPOP", "moved", "0"]).await;
        blocked(&ctx, 1).await;
        admin.call(&["RPUSH", "from", "m", "n"]).await;
        admin
            .call(&["LMOVE", "from", "moved", "LEFT", "LEFT"])
            .await;
        assert_eq!(c.reply().await, pop("moved", "m"));
        c.send(&["BZPOPMAX", "union", "0"]).await;
        blocked(&ctx, 1).await;
        admin.call(&["ZADD", "zin", "3", "u"]).await;
        admin.call(&["ZUNIONSTORE", "union", "1", "zin"]).await;
        assert_eq!(
            c.reply().await,
            "*3\r\n$5\r\nunion\r\n$1\r\nu\r\n$1\r\n3\r\n"
        );

        // Run again as Redis 7.2 does, its other keys can give an error.
        c.send(&["BLPOP", "s", "l", "0"]).await;
        blocked(&ctx, 1).await;
        admin.call(&["SET", "s", "string"]).await;
        admin.call(&["RPUSH", "l", "kept"]).await;
        assert_eq!(
            c.reply().await,
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
        assert_eq!(admin.call(&["LLEN", "l"]).await, ":1\r\n");

        // Counts and RESP3.
        admin.call(&["FLUSHALL"]).await;
        c.call(&["HELLO", "3"]).await;
        c.send(&["BZMPOP", "0", "2", "z1", "z2", "MIN", "COUNT", "5"])
            .await;
        blocked(&ctx, 1).await;
        admin.call(&["ZADD", "z2", "1.5", "x", "2", "y"]).await;
        assert_eq!(
            c.reply().await,
            "*2\r\n$2\r\nz2\r\n*2\r\n*2\r\n$1\r\nx\r\n,1.5\r\n*2\r\n$1\r\ny\r\n,2\r\n"
        );
        c.send(&["BLMPOP", "0", "1", "l", "RIGHT", "COUNT", "2"])
            .await;
        blocked(&ctx, 1).await;
        admin.call(&["RPUSH", "l", "a", "b", "c"]).await;
        assert_eq!(
            c.reply().await,
            "*2\r\n$1\r\nl\r\n*2\r\n$1\r\nc\r\n$1\r\nb\r\n"
        );
        // FLUSHALL doesn't forget who waits.
        c.send(&["BLPOP", "q", "0"]).await;
        blocked(&ctx, 1).await;
        admin.call(&["FLUSHALL"]).await;
        admin.call(&["RPUSH", "q", "x"]).await;
        assert_eq!(c.reply().await, pop("q", "x"));
        blocked(&ctx, 0).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipelining_around_a_blocked_command() {
    for mode in MODES {
        let (addr, ctx) = start(mode).await;
        let mut admin = Client::connect(addr).await;
        let mut c = Client::connect(addr).await;
        // What comes before is answered; what comes after waits.
        let mut batch = common::req(&[b"PING", b"before"]);
        batch.extend(common::req(&[b"BLPOP", b"q", b"0"]));
        batch.extend(common::req(&[b"PING", b"after"]));
        c.send_raw(&batch).await;
        assert_eq!(c.reply().await, "$6\r\nbefore\r\n", "{mode:?}");
        blocked(&ctx, 1).await;
        assert!(!c.has_reply().await);
        // And so does what's sent while it's blocked.
        c.send(&["RPUSH", "mine", "1"]).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(admin.call(&["EXISTS", "mine"]).await, ":0\r\n");
        // A writer's next command sees the waiter served.
        let mut batch = common::req(&[b"RPUSH", b"q", b"x", b"y"]);
        batch.extend(common::req(&[b"LRANGE", b"q", b"0", b"-1"]));
        admin.send_raw(&batch).await;
        assert_eq!(admin.reply().await, ":2\r\n");
        assert_eq!(admin.reply().await, "*1\r\n$1\r\ny\r\n");
        assert_eq!(c.reply().await, pop("q", "x"));
        assert_eq!(c.reply().await, "$5\r\nafter\r\n");
        assert_eq!(c.reply().await, ":1\r\n");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeouts_and_clients_that_leave() {
    for mode in MODES {
        let (addr, ctx) = start(mode).await;
        let mut admin = Client::connect(addr).await;
        let mut c = Client::connect(addr).await;
        let mut d = Client::connect(addr).await;
        let started = Instant::now();
        c.send(&["BLPOP", "q", "0.1"]).await;
        d.send(&["BRPOPLPUSH", "q", "dst", "0"]).await;
        assert_eq!(c.reply().await, "*-1\r\n", "{mode:?}");
        assert!(started.elapsed() >= Duration::from_millis(100));
        blocked(&ctx, 1).await;
        // Timing out doesn't disturb the others.
        admin.call(&["RPUSH", "q", "x"]).await;
        assert_eq!(d.reply().await, "$1\r\nx\r\n");
        c.call(&["HELLO", "3"]).await;
        assert_eq!(c.call(&["BZPOPMIN", "z", "0.01"]).await, "_\r\n");

        // A client that leaves stops waiting, and takes nothing.
        c.send(&["BLPOP", "q", "0"]).await;
        blocked(&ctx, 1).await;
        drop(c);
        blocked(&ctx, 0).await;
        admin.call(&["RPUSH", "q", "x"]).await;
        assert_eq!(admin.call(&["LLEN", "q"]).await, ":1\r\n");
        assert!(
            admin
                .call(&["INFO"])
                .await
                .contains("blocked_clients:0\r\n"),
            "{mode:?}"
        );

        // Shutdown closes blocked connections.
        d.send(&["BLPOP", "nothing", "0"]).await;
        blocked(&ctx, 1).await;
        ctx.shutdown();
        let mut rest = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), d.stream.read_to_end(&mut rest))
            .await
            .expect("still open")
            .unwrap();
        assert!(rest.is_empty());
        blocked(&ctx, 0).await;
    }
}

/// Producers push numbered items while consumers pop them with every
/// blocking command; each item must arrive exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_producers_and_consumers() {
    const PRODUCERS: usize = 4;
    const PER_PRODUCER: usize = 400;
    const CONSUMERS: usize = 8;
    for mode in MODES {
        let (addr, ctx) = start(mode).await;
        let mut consumers = Vec::new();
        for i in 0..CONSUMERS {
            consumers.push(tokio::spawn(async move {
                let mut c = Client::connect(addr).await;
                let mut got = Vec::new();
                loop {
                    let reply = match i % 4 {
                        0 => c.call(&["BLPOP", "q1", "q2", "1"]).await,
                        1 => c.call(&["BRPOP", "q2", "q1", "1"]).await,
                        2 => {
                            c.call(&["BLMPOP", "1", "2", "q1", "q2", "LEFT", "COUNT", "3"])
                                .await
                        }
                        _ => {
                            // Through a list of its own, as a reliable queue would.
                            let mine = format!("mine{i}");
                            let r = c.call(&["BLMOVE", "q1", &mine, "LEFT", "RIGHT", "1"]).await;
                            if r.starts_with('$') {
                                assert_eq!(c.call(&["LPOP", &mine]).await, r);
                            }
                            r
                        }
                    };
                    if reply == "*-1\r\n" {
                        return got;
                    }
                    // (An internal error here once came with an item lost:
                    // a writer that gave a waiter back served it an error
                    // after another writer had claimed it.)
                    assert!(reply.contains("item"), "consumer {i} got {reply:?}");
                    got.extend(
                        reply
                            .split("\r\n")
                            .filter(|s| s.starts_with("item"))
                            .map(str::to_owned),
                    );
                }
            }));
        }
        let mut producers = Vec::new();
        for p in 0..PRODUCERS {
            producers.push(tokio::spawn(async move {
                let mut c = Client::connect(addr).await;
                for n in 0..PER_PRODUCER {
                    let key = if n % 2 == 0 { "q1" } else { "q2" };
                    let r = c.call(&["RPUSH", key, &format!("item{p}-{n}")]).await;
                    assert!(r.starts_with(':'), "RPUSH replied {r:?}");
                    if n % 50 == 0 {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                }
            }));
        }
        for p in producers {
            p.await.unwrap();
        }
        let mut seen = HashSet::new();
        for c in consumers {
            for item in c.await.unwrap() {
                assert!(seen.insert(item.clone()), "{item} twice ({mode:?})");
            }
        }
        let mut admin = Client::connect(addr).await;
        if seen.len() != PRODUCERS * PER_PRODUCER {
            let keys = admin.call(&["KEYS", "*"]).await;
            panic!(
                "{mode:?}: {} of {} items reached consumers; keys left: {keys:?}",
                seen.len(),
                PRODUCERS * PER_PRODUCER
            );
        }
        assert_eq!(admin.call(&["DBSIZE"]).await, ":0\r\n");
        blocked(&ctx, 0).await;
    }
}

/// Consumers blocked for ever on two keys, racing non-blocking poppers for
/// the same data: a consumer that missed a wakeup would wait with data
/// there, and never get its stop sentinel.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_wakeup_is_lost_to_a_race() {
    use std::sync::atomic::{AtomicBool, Ordering};
    const ITEMS: usize = 3000;
    const CONSUMERS: usize = 6;
    for mode in MODES {
        let (addr, ctx) = start(mode).await;
        let stop = Arc::new(AtomicBool::new(false));
        let mut consumers = Vec::new();
        for _ in 0..CONSUMERS {
            consumers.push(tokio::spawn(async move {
                let mut c = Client::connect(addr).await;
                let mut got = Vec::new();
                loop {
                    let reply = c.call(&["BLPOP", "q1", "q2", "0"]).await;
                    let item = reply.rsplit("\r\n").nth(1).unwrap().to_owned();
                    if item == "stop" {
                        return got;
                    }
                    got.push(item);
                }
            }));
        }
        let mut stealers = Vec::new();
        for i in 0..2 {
            let stop = stop.clone();
            stealers.push(tokio::spawn(async move {
                let mut c = Client::connect(addr).await;
                let mut got = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    let reply = c.call(&[["LPOP", "RPOP"][i], ["q1", "q2"][i]]).await;
                    if reply != "$-1\r\n" {
                        got.push(reply.split("\r\n").nth(1).unwrap().to_owned());
                    }
                }
                got
            }));
        }
        let mut producer = Client::connect(addr).await;
        for n in 0..ITEMS {
            producer
                .call(&["RPUSH", ["q1", "q2"][n % 2], &format!("item{n}")])
                .await;
        }
        stop.store(true, Ordering::Relaxed);
        let mut seen = HashSet::new();
        for s in stealers {
            for item in s.await.unwrap() {
                assert!(seen.insert(item), "twice ({mode:?})");
            }
        }
        for _ in 0..CONSUMERS {
            producer.call(&["RPUSH", "q2", "stop"]).await;
        }
        for c in consumers {
            let got = tokio::time::timeout(Duration::from_secs(10), c)
                .await
                .unwrap_or_else(|_| panic!("a consumer missed its wakeup ({mode:?})"))
                .unwrap();
            for item in got {
                assert!(seen.insert(item), "twice ({mode:?})");
            }
        }
        assert_eq!(seen.len(), ITEMS, "{mode:?}");
        assert_eq!(producer.call(&["DBSIZE"]).await, ":0\r\n");
        blocked(&ctx, 0).await;
    }
}

/// Whether `client`'s socket holds unread bytes right now, without waiting.
fn has_bytes_now(client: &Client) -> bool {
    if !client.stream.buffer().is_empty() {
        return true;
    }
    let mut byte = [std::mem::MaybeUninit::<u8>::uninit()];
    socket2::SockRef::from(client.stream.get_ref())
        .peek(&mut byte)
        .is_ok_and(|n| n > 0)
}

/// As from Redis, a client a write wakes gets its reply before the writer
/// gets the write's (BullMQ's waiting workers count on it), with the two
/// connections on different threads, and when the write is a script's or
/// a transaction's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_woken_client_hears_before_the_writer() {
    for mode in MODES {
        let (addr, ctx) = start(mode).await;
        // Connections are dealt to the two threads in turn.
        let mut waiter = Client::connect(addr).await;
        let mut writer = Client::connect(addr).await;
        for round in 0..60 {
            waiter.send(&["BLPOP", "q", "0"]).await;
            blocked(&ctx, 1).await;
            match round % 3 {
                0 => assert_eq!(writer.call(&["RPUSH", "q", "x"]).await, ":1\r\n"),
                1 => assert_eq!(
                    writer
                        .call(&["EVAL", "return redis.call('rpush', KEYS[1], 'x')", "1", "q"])
                        .await,
                    ":1\r\n"
                ),
                _ => {
                    writer.send(&["MULTI"]).await;
                    writer.send(&["RPUSH", "q", "x"]).await;
                    writer.send(&["EXEC"]).await;
                    for expected in ["+OK\r\n", "+QUEUED\r\n", "*1\r\n:1\r\n"] {
                        assert_eq!(writer.reply().await, expected);
                    }
                }
            }
            assert!(has_bytes_now(&waiter), "{mode:?}, round {round}");
            assert_eq!(waiter.reply().await, pop("q", "x"));
            // The reply can come before its connection is done with the
            // wait (the writer sent it), so the next BLPOP must not be
            // taken as blocked by this one's count.
            blocked(&ctx, 0).await;
        }
    }
}
