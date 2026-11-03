//! Transactions: replies recorded from Redis 7.2.4 on one connection, which
//! writes count as changing a watched key (each case run against Redis),
//! and atomicity and optimistic locking under concurrent clients, in every
//! I/O mode.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common::{Client, MODES, start};

#[rustfmt::skip]
const SCRIPT: &[(&[&[u8]], &[u8])] = &[
    (&[b"EXEC"], b"-ERR EXEC without MULTI\r\n"),
    (&[b"DISCARD"], b"-ERR DISCARD without MULTI\r\n"),
    (&[b"UNWATCH"], b"+OK\r\n"),
    (&[b"WATCH"], b"-ERR wrong number of arguments for 'watch' command\r\n"),
    (&[b"TIME", b"x"], b"-ERR wrong number of arguments for 'time' command\r\n"),
    (&[b"MULTI"], b"+OK\r\n"),
    (&[b"MULTI"], b"-ERR MULTI calls can not be nested\r\n"),
    (&[b"SET", b"k", b"v"], b"+QUEUED\r\n"),
    (&[b"INCR", b"k"], b"+QUEUED\r\n"),
    (&[b"GET", b"k"], b"+QUEUED\r\n"),
    (&[b"WATCH", b"x"], b"-ERR WATCH inside MULTI is not allowed\r\n"),
    (&[b"EXEC"], b"*3\r\n+OK\r\n-ERR value is not an integer or out of range\r\n$1\r\nv\r\n"),
    (&[b"MULTI"], b"+OK\r\n"),
    (&[b"SET", b"k", b"1"], b"+QUEUED\r\n"),
    (&[b"NOSUCH"], b"-ERR unknown command 'NOSUCH', with args beginning with: \r\n"),
    (&[b"SET", b"k"], b"-ERR wrong number of arguments for 'set' command\r\n"),
    (&[b"GET", b"k"], b"+QUEUED\r\n"),
    (&[b"EXEC"], b"-EXECABORT Transaction discarded because of previous errors.\r\n"),
    (&[b"GET", b"k"], b"$1\r\nv\r\n"),
    (&[b"MULTI"], b"+OK\r\n"),
    (&[b"SAVE"], b"-ERR Command not allowed inside a transaction\r\n"),
    (&[b"SET", b"k2", b"x"], b"+QUEUED\r\n"),
    (&[b"EXEC"], b"-EXECABORT Transaction discarded because of previous errors.\r\n"),
    (&[b"MULTI"], b"+OK\r\n"),
    (&[b"EXEC"], b"*0\r\n"),
    (&[b"MULTI"], b"+OK\r\n"),
    (&[b"DISCARD"], b"+OK\r\n"),
    (&[b"DISCARD"], b"-ERR DISCARD without MULTI\r\n"),
    (&[b"MULTI"], b"+OK\r\n"),
    (&[b"BLPOP", b"q", b"0"], b"+QUEUED\r\n"),
    (&[b"BLMOVE", b"q", b"d", b"LEFT", b"LEFT", b"0"], b"+QUEUED\r\n"),
    (&[b"BRPOPLPUSH", b"q", b"d", b"0"], b"+QUEUED\r\n"),
    (&[b"BZPOPMIN", b"z", b"0"], b"+QUEUED\r\n"),
    (&[b"BLMPOP", b"0", b"1", b"q", b"LEFT"], b"+QUEUED\r\n"),
    (&[b"BZMPOP", b"0", b"1", b"z", b"MIN"], b"+QUEUED\r\n"),
    (&[b"RPUSH", b"q", b"a", b"b"], b"+QUEUED\r\n"),
    (&[b"BLPOP", b"q", b"0"], b"+QUEUED\r\n"),
    (&[b"EXEC"], b"*8\r\n*-1\r\n$-1\r\n$-1\r\n*-1\r\n*-1\r\n*-1\r\n:2\r\n*2\r\n$1\r\nq\r\n$1\r\na\r\n"),
    (&[b"MULTI"], b"+OK\r\n"),
    (&[b"INCR", b"k"], b"+QUEUED\r\n"),
    (&[b"LPUSH", b"k", b"x"], b"+QUEUED\r\n"),
    (&[b"DEL", b"k"], b"+QUEUED\r\n"),
    (&[b"EXEC"], b"*3\r\n-ERR value is not an integer or out of range\r\n-WRONGTYPE Operation against a key holding the wrong kind of value\r\n:1\r\n"),
    (&[b"SET", b"w", b"1"], b"+OK\r\n"),
    (&[b"WATCH", b"w"], b"+OK\r\n"),
    (&[b"MULTI"], b"+OK\r\n"),
    (&[b"INCR", b"w"], b"+QUEUED\r\n"),
    (&[b"EXEC"], b"*1\r\n:2\r\n"),
    (&[b"WATCH", b"w"], b"+OK\r\n"),
    (&[b"SET", b"w", b"5"], b"+OK\r\n"),
    (&[b"MULTI"], b"+OK\r\n"),
    (&[b"GET", b"w"], b"+QUEUED\r\n"),
    (&[b"EXEC"], b"*-1\r\n"),
    (&[b"WATCH", b"w"], b"+OK\r\n"),
    (&[b"UNWATCH"], b"+OK\r\n"),
    (&[b"SET", b"w", b"7"], b"+OK\r\n"),
    (&[b"MULTI"], b"+OK\r\n"),
    (&[b"GET", b"w"], b"+QUEUED\r\n"),
    (&[b"EXEC"], b"*1\r\n$1\r\n7\r\n"),
    (&[b"WATCH", b"nokey"], b"+OK\r\n"),
    (&[b"MULTI"], b"+OK\r\n"),
    (&[b"GET", b"nokey"], b"+QUEUED\r\n"),
    (&[b"EXEC"], b"*1\r\n$-1\r\n"),
    (&[b"MULTI"], b"+OK\r\n"),
    (&[b"PING"], b"+QUEUED\r\n"),
    (&[b"ECHO", b"x"], b"+QUEUED\r\n"),
    (&[b"RESET"], b"+RESET\r\n"),
    (&[b"MULTI"], b"+OK\r\n"),
    (&[b"EXEC"], b"*0\r\n"),
    (&[b"HELLO", b"3"], b"%7\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.2.4\r\n$5\r\nproto\r\n:3\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n"),
    (&[b"MULTI"], b"+OK\r\n"),
    (&[b"SET", b"k", b"v"], b"+QUEUED\r\n"),
    (&[b"GET", b"nope"], b"+QUEUED\r\n"),
    (&[b"HGETALL", b"nope"], b"+QUEUED\r\n"),
    (&[b"LPOP", b"nope", b"2"], b"+QUEUED\r\n"),
    (&[b"EXEC"], b"*4\r\n+OK\r\n_\r\n%0\r\n_\r\n"),
    (&[b"WATCH", b"w"], b"+OK\r\n"),
    (&[b"SET", b"w", b"9"], b"+OK\r\n"),
    (&[b"MULTI"], b"+OK\r\n"),
    (&[b"GET", b"w"], b"+QUEUED\r\n"),
    (&[b"EXEC"], b"_\r\n"),
    (&[b"MULTI"], b"+OK\r\n"),
    (&[b"BLPOP", b"q", b"0"], b"+QUEUED\r\n"),
    (&[b"BLMOVE", b"q", b"d", b"LEFT", b"LEFT", b"0"], b"+QUEUED\r\n"),
    (&[b"EXEC"], b"*2\r\n*2\r\n$1\r\nq\r\n$1\r\nb\r\n_\r\n"),
    (&[b"HELLO", b"2"], b"*14\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.2.4\r\n$5\r\nproto\r\n:2\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n"),
    (&[b"MULTI"], b"+OK\r\n"),
    (&[b"HELLO", b"3"], b"+QUEUED\r\n"),
    (&[b"CLIENT", b"SETNAME", b"n"], b"+QUEUED\r\n"),
    (&[b"EXEC"], b"*2\r\n%7\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.2.4\r\n$5\r\nproto\r\n:3\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n+OK\r\n"),
    (&[b"CLIENT", b"GETNAME"], b"$1\r\nn\r\n"),
];

#[tokio::test]
async fn replies_match_redis() {
    common::replay(SCRIPT).await;
}

/// Setup, a command another client runs after `WATCH k`, and whether that
/// makes `EXEC` fail, as in Redis 7.2.4 (`-` for no setup).
#[rustfmt::skip]
const WATCH_CASES: &[(&str, &str, bool)] = &[
    ("SET k v", "SET k v", true),
    ("SET k v", "SET k v NX", false),
    ("SET k v", "SET k w XX", true),
    ("-", "SET k v XX", false),
    ("SET k v", "SET k v GET", true),
    ("SET k 1", "INCR k", true),
    ("SET k 1", "INCRBY k 0", true),
    ("SET k 1", "DECRBY k 2", true),
    ("SET k v", "GETDEL k", true),
    ("-", "GETDEL k", false),
    ("SET k v", "GETEX k", false),
    ("SET k v", "GETEX k EX 100", true),
    ("SET k v", "GETEX k PERSIST", false),
    ("SET k v", "DEL k", true),
    ("-", "DEL k", false),
    ("-", "UNLINK k", false),
    ("SET k v", "EXPIRE k 100", true),
    ("SET k v EX 100", "EXPIRE k 50 GT", false),
    ("SET k v EX 100", "EXPIRE k 200 GT", true),
    ("SET k v", "EXPIRE k 100 XX", false),
    ("SET k v", "EXPIRE k 0", true),
    ("SET k v", "PERSIST k", false),
    ("SET k v EX 100", "PERSIST k", true),
    ("SET k v", "PEXPIREAT k 1", true),
    ("SET k v", "SETEX k 10 w", true),
    ("-", "SETNX k v", true),
    ("SET k v", "SETNX k v", false),
    ("-", "MSET k v k2 w", true),
    ("SET k v", "RENAME k k2", true),
    ("SET k2 v", "RENAME k2 k", true),
    ("SET k v", "RENAMENX k k2", true),
    ("SET k2 v", "RENAMENX k2 k", true),
    ("HSET k f v", "HSET k f v", true),
    ("HSET k f v", "HSET k f w", true),
    ("HSET k f v", "HSETNX k f w", false),
    ("HSET k f v", "HSETNX k g w", true),
    ("HSET k f v", "HDEL k g", false),
    ("HSET k f v", "HDEL k f", true),
    ("HSET k f 1", "HINCRBY k f 0", true),
    ("-", "HSET k f v", true),
    ("LPUSH k a", "LPUSH k b", true),
    ("-", "LPUSHX k b", false),
    ("LPUSH k a", "RPUSHX k b", true),
    ("LPUSH k a b", "LPOP k", true),
    ("-", "LPOP k", false),
    ("LPUSH k a", "LPOP k 0", false),
    ("LPUSH k a b", "RPOP k", true),
    ("LPUSH k a b", "LSET k 0 z", true),
    ("LPUSH k a b", "LINSERT k BEFORE a z", true),
    ("LPUSH k a b", "LINSERT k BEFORE nope z", false),
    ("LPUSH k a b", "LREM k 0 nope", false),
    ("LPUSH k a b", "LREM k 0 a", true),
    ("LPUSH k a b", "LTRIM k 0 -1", true),
    ("LPUSH k a b", "LTRIM k 0 0", true),
    ("LPUSH k a", "LMOVE k k2 LEFT LEFT", true),
    ("LPUSH k2 a", "LMOVE k2 k LEFT LEFT", true),
    ("LPUSH k a", "LMOVE k k LEFT RIGHT", true),
    ("LPUSH k a b", "LMPOP 1 k LEFT", true),
    ("LPUSH k a", "BLPOP k 1", true),
    ("LPUSH k2 a", "BLMOVE k2 k LEFT LEFT 1", true),
    ("SADD k a", "SADD k a", false),
    ("SADD k a", "SADD k b", true),
    ("SADD k a", "SREM k b", false),
    ("SADD k a", "SREM k a", true),
    ("SADD k a b", "SPOP k", true),
    ("-", "SPOP k", false),
    ("SADD k a", "SMOVE k k2 a", true),
    ("SADD k a", "SMOVE k k2 nope", false),
    ("SADD k2 a", "SMOVE k2 k a", true),
    ("SADD k2 a", "SINTERSTORE k k2", true),
    ("SADD k a", "SINTERSTORE k nope", true),
    ("-", "SINTERSTORE k nope", false),
    ("SADD k2 a", "SUNIONSTORE k k2", true),
    ("SADD k2 a", "SDIFFSTORE k k2", true),
    ("ZADD k 1 a", "ZADD k 1 a", false),
    ("ZADD k 1 a", "ZADD k 2 a", true),
    ("ZADD k 1 a", "ZADD k NX 2 a", false),
    ("ZADD k 1 a", "ZADD k XX 2 b", false),
    ("ZADD k 1 a", "ZINCRBY k 0 a", false),
    ("ZADD k 1 a", "ZREM k b", false),
    ("ZADD k 1 a", "ZREM k a", true),
    ("ZADD k 1 a", "ZREMRANGEBYSCORE k 5 6", false),
    ("ZADD k 1 a", "ZREMRANGEBYSCORE k 0 6", true),
    ("ZADD k 1 a", "ZREMRANGEBYRANK k 5 6", false),
    ("ZADD k 1 a", "ZREMRANGEBYLEX k [x [y", false),
    ("ZADD k 1 a 2 b", "ZPOPMIN k", true),
    ("-", "ZPOPMIN k", false),
    ("ZADD k 1 a", "ZPOPMAX k 0", false),
    ("ZADD k 1 a", "ZMPOP 1 k MIN", true),
    ("ZADD k2 1 a", "ZUNIONSTORE k 1 k2", true),
    ("ZADD k2 1 a", "ZINTERSTORE k 1 k2", true),
    ("ZADD k2 1 a", "ZDIFFSTORE k 1 k2", true),
    ("ZADD k 1 a", "ZUNIONSTORE k 1 nope", true),
    ("-", "ZUNIONSTORE k 1 nope", false),
    ("ZADD k2 1 a", "ZRANGESTORE k k2 0 -1", true),
    ("ZADD k 1 a", "ZRANGESTORE k nope 0 -1", true),
    ("ZADD k 1 a", "BZPOPMIN k 1", true),
    ("SET k v", "FLUSHALL", true),
    ("-", "FLUSHALL", false),
    ("SET k2 v", "FLUSHDB", false),
    ("SET k v", "GET k", false),
];

fn words(s: &str) -> Vec<&str> {
    s.split_whitespace().collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn what_changes_a_watched_key() {
    for mode in MODES {
        let (addr, _ctx) = start(mode).await;
        let mut a = Client::connect(addr).await;
        let mut b = Client::connect(addr).await;
        for &(setup, command, changes) in WATCH_CASES {
            a.call(&["FLUSHALL"]).await;
            if setup != "-" {
                a.call(&words(setup)).await;
            }
            a.call(&["WATCH", "k"]).await;
            b.call(&words(command)).await;
            a.call(&["MULTI"]).await;
            a.call(&["PING"]).await;
            let exec = a.call(&["EXEC"]).await;
            let aborted = exec == "*-1\r\n";
            assert_eq!(aborted, changes, "{setup} | {command} ({mode:?}): {exec:?}");
        }
        // Expiring counts too, unless it had expired already.
        a.call(&["SET", "k", "v", "PX", "30"]).await;
        a.call(&["WATCH", "k"]).await;
        tokio::time::sleep(Duration::from_millis(60)).await;
        a.call(&["MULTI"]).await;
        assert_eq!(a.call(&["EXEC"]).await, "*-1\r\n");
        a.call(&["SET", "k", "v", "PX", "1"]).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        a.call(&["WATCH", "k"]).await;
        a.call(&["GET", "k"]).await; // removes it
        a.call(&["MULTI"]).await;
        assert_eq!(a.call(&["EXEC"]).await, "*0\r\n");
    }
}

/// Transfers between two counters in transactions, while readers read both
/// in transactions too: no reader may see one moved without the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transactions_are_atomic() {
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
                    c.send_raw(
                        &[
                            common::req(&[b"MULTI"]),
                            common::req(&[b"DECRBY", b"a", b"3"]),
                            common::req(&[b"INCRBY", b"b", b"3"]),
                            common::req(&[b"EXEC"]),
                        ]
                        .concat(),
                    )
                    .await;
                    for _ in 0..4 {
                        c.reply().await;
                    }
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
                    c.send_raw(
                        &[
                            common::req(&[b"MULTI"]),
                            common::req(&[b"GET", b"a"]),
                            common::req(&[b"GET", b"b"]),
                            common::req(&[b"EXEC"]),
                        ]
                        .concat(),
                    )
                    .await;
                    c.reply().await;
                    c.reply().await;
                    c.reply().await;
                    let exec = c.reply().await;
                    let values: Vec<i64> = exec
                        .split("\r\n")
                        .filter(|l| !l.is_empty() && !l.starts_with(['*', '$']))
                        .map(|l| l.parse().unwrap())
                        .collect();
                    assert_eq!(values.iter().sum::<i64>(), 2000, "{exec:?}");
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

/// Clients incrementing one counter with WATCH, GET, MULTI, SET, EXEC,
/// retrying when EXEC fails: none of their increments may be lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn optimistic_locking_loses_nothing() {
    const CLIENTS: usize = 6;
    const EACH: usize = 100;
    for mode in MODES {
        let (addr, _ctx) = start(mode).await;
        let mut clients = Vec::new();
        for _ in 0..CLIENTS {
            clients.push(tokio::spawn(async move {
                let mut c = Client::connect(addr).await;
                let mut retries = 0;
                for _ in 0..EACH {
                    loop {
                        c.call(&["WATCH", "counter"]).await;
                        let value = c.call(&["GET", "counter"]).await;
                        let n: i64 = value
                            .split("\r\n")
                            .nth(1)
                            .map_or(0, |v| v.parse().unwrap_or(0));
                        c.call(&["MULTI"]).await;
                        c.call(&["SET", "counter", &(n + 1).to_string()]).await;
                        if c.call(&["EXEC"]).await != "*-1\r\n" {
                            break;
                        }
                        retries += 1;
                    }
                }
                retries
            }));
        }
        let mut retries = 0;
        for c in clients {
            retries += c.await.unwrap();
        }
        let mut c = Client::connect(addr).await;
        let total = (CLIENTS * EACH).to_string();
        assert_eq!(
            c.call(&["GET", "counter"]).await,
            format!("${}\r\n{total}\r\n", total.len()),
            "{mode:?} after {retries} retries"
        );
    }
}
