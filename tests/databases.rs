//! Numbered databases (`SELECT`, `MOVE`, `SWAPDB`...) and `CLIENT KILL`, end to
//! end. Replies are as Redis 7.2.4 gives them (see `tools/db-diff.py`).

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{Client, MODES, Mode, execute, start};
use tantalum::cmd;
use tantalum::context::{Context, Settings};
use tantalum::store::{Config, EvictionPolicy, Store};
use tokio::io::AsyncReadExt;

/// Whether the server closes this connection within a few seconds.
async fn closes(c: &mut Client) -> bool {
    let mut buf = [0; 64];
    let read = c.stream.get_mut().read(&mut buf);
    matches!(
        tokio::time::timeout(Duration::from_secs(5), read).await,
        Ok(Ok(0) | Err(_))
    )
}

#[tokio::test]
async fn databases_hold_their_own_keys() {
    let (addr, _ctx) = start(Mode::PerThread).await;
    let mut c = Client::connect(addr).await;
    assert_eq!(c.call(&["SET", "k", "zero"]).await, "+OK\r\n");
    assert_eq!(c.call(&["SELECT", "1"]).await, "+OK\r\n");
    assert_eq!(c.call(&["GET", "k"]).await, "$-1\r\n");
    assert_eq!(c.call(&["EXISTS", "k"]).await, ":0\r\n");
    assert_eq!(c.call(&["DBSIZE"]).await, ":0\r\n");
    assert_eq!(c.call(&["KEYS", "*"]).await, "*0\r\n");
    assert_eq!(c.call(&["RANDOMKEY"]).await, "$-1\r\n");
    assert_eq!(c.call(&["SCAN", "0"]).await, "*2\r\n$1\r\n0\r\n*0\r\n");
    assert_eq!(c.call(&["SET", "k", "one"]).await, "+OK\r\n");
    assert_eq!(c.call(&["GET", "k"]).await, "$3\r\none\r\n");
    assert_eq!(c.call(&["KEYS", "*"]).await, "*1\r\n$1\r\nk\r\n");
    assert_eq!(c.call(&["RANDOMKEY"]).await, "$1\r\nk\r\n");
    assert_eq!(c.call(&["DEL", "k"]).await, ":1\r\n");
    assert_eq!(c.call(&["SELECT", "0"]).await, "+OK\r\n");
    assert_eq!(c.call(&["GET", "k"]).await, "$4\r\nzero\r\n");

    // Every type, and a connection keeps its own choice.
    let mut other = Client::connect(addr).await;
    assert_eq!(other.call(&["SELECT", "15"]).await, "+OK\r\n");
    assert_eq!(other.call(&["HSET", "h", "f", "v"]).await, ":1\r\n");
    assert_eq!(other.call(&["RPUSH", "l", "a"]).await, ":1\r\n");
    assert_eq!(other.call(&["SADD", "s", "a"]).await, ":1\r\n");
    assert_eq!(other.call(&["ZADD", "z", "1", "a"]).await, ":1\r\n");
    assert_eq!(
        other.call(&["XADD", "x", "1-1", "f", "v"]).await,
        "$3\r\n1-1\r\n"
    );
    assert_eq!(other.call(&["DBSIZE"]).await, ":5\r\n");
    assert_eq!(other.call(&["TYPE", "h"]).await, "+hash\r\n");
    assert_eq!(c.call(&["DBSIZE"]).await, ":1\r\n");
    assert_eq!(c.call(&["TYPE", "h"]).await, "+none\r\n");
    assert_eq!(other.call(&["RENAME", "h", "h2"]).await, "+OK\r\n");
    assert_eq!(other.call(&["HGET", "h2", "f"]).await, "$1\r\nv\r\n");
}

#[tokio::test]
async fn select_reads_its_index_as_redis_does() {
    let (addr, _ctx) = start(Mode::PerThread).await;
    let mut c = Client::connect(addr).await;
    let not_integer = "-ERR value is not an integer or out of range\r\n";
    assert_eq!(c.call(&["SELECT", "abc"]).await, not_integer);
    assert_eq!(c.call(&["SELECT", "1.5"]).await, not_integer);
    let out_of_range = "-ERR DB index is out of range\r\n";
    assert_eq!(c.call(&["SELECT", "-1"]).await, out_of_range);
    assert_eq!(c.call(&["SELECT", "16"]).await, out_of_range);
    assert_eq!(
        c.call(&["SELECT", "99999999999"]).await,
        "-ERR value is out of range, value must between -2147483648 and 2147483647\r\n"
    );
    assert_eq!(c.call(&["SELECT", "15"]).await, "+OK\r\n");
}

#[tokio::test]
async fn flushdb_empties_one_and_flushall_every_one() {
    let (addr, ctx) = start(Mode::PerThread).await;
    let mut c = Client::connect(addr).await;
    for db in ["0", "1", "2"] {
        c.call(&["SELECT", db]).await;
        c.call(&["SET", "k", db]).await;
    }
    assert_eq!(c.call(&["FLUSHDB"]).await, "+OK\r\n");
    assert_eq!(c.call(&["DBSIZE"]).await, ":0\r\n");
    c.call(&["SELECT", "1"]).await;
    assert_eq!(c.call(&["GET", "k"]).await, "$1\r\n1\r\n");
    assert_eq!(c.call(&["FLUSHALL"]).await, "+OK\r\n");
    assert_eq!(c.call(&["DBSIZE"]).await, ":0\r\n");
    c.call(&["SELECT", "0"]).await;
    assert_eq!(c.call(&["DBSIZE"]).await, ":0\r\n");
    assert_eq!(ctx.store.stats().keys, 0);
}

#[tokio::test]
async fn move_takes_the_value_and_ttl_along() {
    let (addr, _ctx) = start(Mode::PerThread).await;
    let mut c = Client::connect(addr).await;
    c.call(&["SET", "k", "v", "EX", "100"]).await;
    assert_eq!(c.call(&["MOVE", "k", "3"]).await, ":1\r\n");
    assert_eq!(c.call(&["GET", "k"]).await, "$-1\r\n");
    c.call(&["SELECT", "3"]).await;
    assert_eq!(c.call(&["GET", "k"]).await, "$1\r\nv\r\n");
    let ttl: i64 = c.call(&["TTL", "k"]).await[1..].trim().parse().unwrap();
    assert!((1..=100).contains(&ttl), "{ttl}");
    // Not over one that's there, and nothing to move.
    c.call(&["SELECT", "0"]).await;
    c.call(&["SET", "k", "here"]).await;
    c.call(&["SELECT", "3"]).await;
    assert_eq!(c.call(&["MOVE", "k", "0"]).await, ":0\r\n");
    assert_eq!(c.call(&["GET", "k"]).await, "$1\r\nv\r\n");
    assert_eq!(c.call(&["MOVE", "none", "0"]).await, ":0\r\n");
    // What it replies to bad targets.
    assert_eq!(
        c.call(&["MOVE", "k", "3"]).await,
        "-ERR source and destination objects are the same\r\n"
    );
    assert_eq!(
        c.call(&["MOVE", "k", "16"]).await,
        "-ERR DB index is out of range\r\n"
    );
    assert_eq!(
        c.call(&["MOVE", "k", "x"]).await,
        "-ERR value is not an integer or out of range\r\n"
    );
    // A big collection.
    let fields: Vec<String> = (0..2_000)
        .flat_map(|i| [format!("f{i}"), "v".into()])
        .collect();
    let mut hset = vec!["HSET", "big"];
    hset.extend(fields.iter().map(String::as_str));
    assert_eq!(c.call(&hset).await, ":2000\r\n");
    assert_eq!(c.call(&["MOVE", "big", "4"]).await, ":1\r\n");
    assert_eq!(c.call(&["EXISTS", "big"]).await, ":0\r\n");
    c.call(&["SELECT", "4"]).await;
    assert_eq!(c.call(&["HLEN", "big"]).await, ":2000\r\n");
    assert_eq!(c.call(&["HGET", "big", "f1999"]).await, "$1\r\nv\r\n");
}

#[tokio::test]
async fn copy_goes_to_another_database() {
    let (addr, _ctx) = start(Mode::PerThread).await;
    let mut c = Client::connect(addr).await;
    c.call(&["SET", "a", "v", "EX", "100"]).await;
    assert_eq!(
        c.call(&["COPY", "a", "a"]).await,
        "-ERR source and destination objects are the same\r\n"
    );
    assert_eq!(
        c.call(&["COPY", "a", "a", "DB", "0"]).await,
        "-ERR source and destination objects are the same\r\n"
    );
    assert_eq!(c.call(&["COPY", "a", "a", "DB", "1"]).await, ":1\r\n");
    assert_eq!(c.call(&["COPY", "a", "a", "DB", "1"]).await, ":0\r\n");
    assert_eq!(
        c.call(&["COPY", "a", "a", "DB", "1", "REPLACE"]).await,
        ":1\r\n"
    );
    assert_eq!(c.call(&["COPY", "a", "b", "DB", "2"]).await, ":1\r\n");
    assert_eq!(
        c.call(&["COPY", "a", "b", "DB", "16"]).await,
        "-ERR DB index is out of range\r\n"
    );
    assert_eq!(c.call(&["GET", "a"]).await, "$1\r\nv\r\n");
    assert_eq!(c.call(&["SELECT", "2"]).await, "+OK\r\n");
    assert_eq!(c.call(&["GET", "b"]).await, "$1\r\nv\r\n");
    assert_ne!(c.call(&["TTL", "b"]).await, ":-1\r\n");
}

#[tokio::test]
async fn swapdb_swaps_what_they_hold() {
    let (addr, ctx) = start(Mode::PerThread).await;
    let mut c = Client::connect(addr).await;
    c.call(&["SET", "k", "zero"]).await;
    c.call(&["SET", "only-in-0", "x"]).await;
    c.call(&["SELECT", "1"]).await;
    c.call(&["SET", "k", "one"]).await;
    let used = ctx.store.stats().used_memory;
    assert_eq!(c.call(&["SWAPDB", "0", "1"]).await, "+OK\r\n");
    assert_eq!(c.call(&["GET", "k"]).await, "$4\r\nzero\r\n");
    assert_eq!(c.call(&["DBSIZE"]).await, ":2\r\n");
    c.call(&["SELECT", "0"]).await;
    assert_eq!(c.call(&["GET", "k"]).await, "$3\r\none\r\n");
    assert_eq!(c.call(&["DBSIZE"]).await, ":1\r\n");
    assert_eq!(ctx.store.stats().used_memory, used);
    // With one never used, and with itself.
    assert_eq!(c.call(&["SWAPDB", "0", "9"]).await, "+OK\r\n");
    assert_eq!(c.call(&["DBSIZE"]).await, ":0\r\n");
    assert_eq!(c.call(&["SWAPDB", "9", "9"]).await, "+OK\r\n");
    c.call(&["SELECT", "9"]).await;
    assert_eq!(c.call(&["GET", "k"]).await, "$3\r\none\r\n");
    assert_eq!(
        c.call(&["SWAPDB", "x", "1"]).await,
        "-ERR invalid first DB index\r\n"
    );
    assert_eq!(
        c.call(&["SWAPDB", "1", "x"]).await,
        "-ERR invalid second DB index\r\n"
    );
    assert_eq!(
        c.call(&["SWAPDB", "16", "1"]).await,
        "-ERR DB index is out of range\r\n"
    );
}

#[tokio::test]
async fn swapdb_serves_and_tells_those_waiting_on_its_keys() {
    for mode in MODES {
        let (addr, _ctx) = start(mode).await;
        let mut a = Client::connect(addr).await;
        let mut b = Client::connect(addr).await;
        a.call(&["SELECT", "1"]).await;
        a.send(&["BLPOP", "l", "0"]).await;
        assert!(!a.has_reply().await);
        // In the other database, so far.
        b.call(&["RPUSH", "l", "x"]).await;
        assert!(!a.has_reply().await);
        assert_eq!(b.call(&["SWAPDB", "0", "1"]).await, "+OK\r\n");
        assert_eq!(a.reply().await, "*2\r\n$1\r\nl\r\n$1\r\nx\r\n");

        // Someone watching a key the swap changes.
        b.call(&["SET", "w", "1"]).await;
        a.call(&["SELECT", "0"]).await;
        a.call(&["WATCH", "w"]).await;
        a.call(&["MULTI"]).await;
        a.call(&["PING"]).await;
        assert_eq!(b.call(&["SWAPDB", "0", "2"]).await, "+OK\r\n");
        assert_eq!(a.call(&["EXEC"]).await, "*-1\r\n", "{mode:?}");
    }
}

#[tokio::test]
async fn transactions_can_select() {
    let (addr, _ctx) = start(Mode::PerThread).await;
    let mut c = Client::connect(addr).await;
    c.call(&["MULTI"]).await;
    assert_eq!(c.call(&["SET", "a", "zero"]).await, "+QUEUED\r\n");
    assert_eq!(c.call(&["SELECT", "1"]).await, "+QUEUED\r\n");
    assert_eq!(c.call(&["SET", "a", "one"]).await, "+QUEUED\r\n");
    assert_eq!(c.call(&["EXEC"]).await, "*3\r\n+OK\r\n+OK\r\n+OK\r\n");
    // It stays selected, as in Redis.
    assert_eq!(c.call(&["GET", "a"]).await, "$3\r\none\r\n");
    c.call(&["SELECT", "0"]).await;
    assert_eq!(c.call(&["GET", "a"]).await, "$4\r\nzero\r\n");
    // But not if discarded: nothing ran.
    c.call(&["MULTI"]).await;
    c.call(&["SELECT", "5"]).await;
    c.call(&["DISCARD"]).await;
    assert_eq!(c.call(&["GET", "a"]).await, "$4\r\nzero\r\n");
}

#[tokio::test]
async fn a_watch_is_of_a_key_in_a_database() {
    let (addr, _ctx) = start(Mode::PerThread).await;
    let mut c = Client::connect(addr).await;
    let mut other = Client::connect(addr).await;
    c.call(&["SELECT", "1"]).await;
    c.call(&["WATCH", "k"]).await;
    // The same name in another database is another key.
    other.call(&["SET", "k", "0"]).await;
    c.call(&["MULTI"]).await;
    c.call(&["PING"]).await;
    assert_eq!(c.call(&["EXEC"]).await, "*1\r\n+PONG\r\n");
    c.call(&["WATCH", "k"]).await;
    other.call(&["SELECT", "1"]).await;
    other.call(&["SET", "k", "1"]).await;
    c.call(&["MULTI"]).await;
    c.call(&["PING"]).await;
    assert_eq!(c.call(&["EXEC"]).await, "*-1\r\n");
    // Watched in two, either counts, wherever the connection is by then.
    c.call(&["WATCH", "x"]).await;
    c.call(&["SELECT", "2"]).await;
    c.call(&["WATCH", "x"]).await;
    other.call(&["SET", "x", "1"]).await;
    c.call(&["MULTI"]).await;
    c.call(&["PING"]).await;
    assert_eq!(c.call(&["EXEC"]).await, "*-1\r\n");
    // FLUSHDB tells those watching what it removes.
    other.call(&["SET", "y", "1"]).await;
    c.call(&["SELECT", "1"]).await;
    c.call(&["WATCH", "y"]).await;
    other.call(&["FLUSHDB"]).await;
    c.call(&["MULTI"]).await;
    c.call(&["PING"]).await;
    assert_eq!(c.call(&["EXEC"]).await, "*-1\r\n");
}

#[tokio::test]
async fn a_scripts_select_is_its_own() {
    let (addr, _ctx) = start(Mode::PerThread).await;
    let mut c = Client::connect(addr).await;
    c.call(&["SELECT", "3"]).await;
    c.call(&["SET", "k", "three"]).await;
    // It starts in the caller's database.
    assert_eq!(
        c.call(&["EVAL", "return redis.call('GET', KEYS[1])", "1", "k"])
            .await,
        "$5\r\nthree\r\n"
    );
    let script = "redis.call('SELECT', 2) redis.call('SET', KEYS[1], 'two') return redis.call('GET', KEYS[1])";
    assert_eq!(c.call(&["EVAL", script, "1", "k"]).await, "$3\r\ntwo\r\n");
    assert_eq!(c.call(&["GET", "k"]).await, "$5\r\nthree\r\n");
    c.call(&["SELECT", "2"]).await;
    assert_eq!(c.call(&["GET", "k"]).await, "$3\r\ntwo\r\n");
}

#[tokio::test]
async fn blocking_is_in_a_database() {
    for mode in MODES {
        let (addr, _ctx) = start(mode).await;
        let mut a = Client::connect(addr).await;
        let mut b = Client::connect(addr).await;
        a.call(&["SELECT", "1"]).await;
        a.send(&["BLPOP", "k", "0"]).await;
        assert!(!a.has_reply().await);
        assert_eq!(b.call(&["RPUSH", "k", "zero"]).await, ":1\r\n");
        assert!(
            !a.has_reply().await,
            "{mode:?}: served from another database"
        );
        b.call(&["SELECT", "1"]).await;
        assert_eq!(b.call(&["RPUSH", "k", "one"]).await, ":1\r\n");
        assert_eq!(
            a.reply().await,
            "*2\r\n$1\r\nk\r\n$3\r\none\r\n",
            "{mode:?}"
        );
        b.call(&["SELECT", "0"]).await;
        assert_eq!(b.call(&["LLEN", "k"]).await, ":1\r\n");
        // MOVE gives a key to those waiting there.
        a.send(&["BLPOP", "k", "0"]).await;
        assert!(!a.has_reply().await);
        assert_eq!(b.call(&["MOVE", "k", "1"]).await, ":1\r\n");
        assert_eq!(
            a.reply().await,
            "*2\r\n$1\r\nk\r\n$4\r\nzero\r\n",
            "{mode:?}"
        );
    }
}

#[tokio::test]
async fn info_config_and_client_show_them() {
    let (addr, _ctx) = start(Mode::PerThread).await;
    let mut c = Client::connect(addr).await;
    let mut other = Client::connect(addr).await;
    c.call(&["SET", "a", "1"]).await;
    c.call(&["SELECT", "2"]).await;
    c.call(&["SET", "a", "1", "EX", "100"]).await;
    c.call(&["SET", "b", "1"]).await;
    let info = c.call(&["INFO"]).await;
    assert!(
        info.contains("db0:keys=1,expires=0,avg_ttl=0\r\n"),
        "{info}"
    );
    assert!(
        info.contains("db2:keys=2,expires=1,avg_ttl=0\r\n"),
        "{info}"
    );
    assert!(!info.contains("db1:"), "{info}");
    // What Sidekiq's dashboard shows.
    for field in [
        "uptime_in_days:0\r\n",
        "used_memory_human:",
        "used_memory_peak:",
        "used_memory_peak_human:",
        "connected_clients:",
        "redis_version:",
    ] {
        assert!(info.contains(field), "{field} in {info}");
    }
    assert_eq!(
        c.call(&["CONFIG", "GET", "databases"]).await,
        "*2\r\n$9\r\ndatabases\r\n$2\r\n16\r\n"
    );
    assert!(c.call(&["CLIENT", "INFO"]).await.contains(" db=2 "));
    // Others', too.
    assert!(other.call(&["CLIENT", "LIST"]).await.contains(" db=2 "));
    assert!(other.call(&["CLIENT", "INFO"]).await.contains(" db=0 "));
    // RESET goes back to 0.
    assert_eq!(c.call(&["RESET"]).await, "+RESET\r\n");
    assert_eq!(c.call(&["DBSIZE"]).await, ":1\r\n");
    assert!(c.call(&["CLIENT", "INFO"]).await.contains(" db=0 "));
}

#[test]
fn fewer_databases_when_asked() {
    let ctx = Context::new(
        Arc::new(Store::with_config(Config {
            databases: 2,
            ..Config::default()
        })),
        Settings::default(),
    );
    let mut session = ctx.connect(-1).unwrap().session();
    let mut run = |args: &[&[u8]]| execute(&ctx, &mut session, 1, args);
    assert_eq!(run(&[b"SELECT", b"1"]), b"+OK\r\n");
    assert_eq!(
        run(&[b"SELECT", b"2"]),
        b"-ERR DB index is out of range\r\n"
    );
    assert_eq!(
        run(&[b"CONFIG", b"GET", b"databases"]),
        b"*2\r\n$9\r\ndatabases\r\n$1\r\n2\r\n"
    );
}

/// A context limited to `limit` bytes, and the time.
fn limited(limit: usize, policy: EvictionPolicy) -> (Context, u64) {
    let store = Store::with_config(Config {
        shards: 4,
        maxmemory: limit,
        policy,
        ..Config::default()
    });
    let now = store.now();
    (Context::new(Arc::new(store), Settings::default()), now)
}

#[test]
fn eviction_takes_from_every_database() {
    const LIMIT: usize = 4 << 20;
    let (ctx, now) = limited(LIMIT, EvictionPolicy::AllKeysLru);
    let mut session = ctx.connect(-1).unwrap().session();
    let value = vec![b'v'; 1_000];
    let fill = |session: &mut _, n: usize, name: &str| {
        for i in 0..n {
            let key = format!("{name}{i}");
            assert_eq!(
                execute(&ctx, session, now, &[b"SET", key.as_bytes(), &value]),
                b"+OK\r\n"
            );
        }
    };
    // Database 0 about full, and then room is wanted in another.
    fill(&mut session, 3_500, "a");
    let at_first = ctx.store.db(0).key_counts().0;
    execute(&ctx, &mut session, now, &[b"SELECT", b"1"]);
    fill(&mut session, 3_500, "b");
    let (zero, one) = (
        ctx.store.db(0).key_counts().0,
        ctx.store.db(1).key_counts().0,
    );
    assert!(zero < at_first, "database 0 kept all {zero} of its keys");
    assert!(one > 0);
    assert!(
        ctx.store.used_memory() < LIMIT + LIMIT / 8,
        "{} over the limit",
        ctx.store.used_memory()
    );
}

#[test]
fn the_limit_counts_every_database() {
    const LIMIT: usize = 2 << 20;
    let (ctx, now) = limited(LIMIT, EvictionPolicy::NoEviction);
    let mut session = ctx.connect(-1).unwrap().session();
    let value = vec![b'v'; 1_000];
    let mut i = 0;
    loop {
        let key = format!("a{i}");
        let reply = execute(&ctx, &mut session, now, &[b"SET", key.as_bytes(), &value]);
        i += 1;
        if reply.starts_with(b"-OOM") {
            break;
        }
        assert!(i < 100_000, "never full");
    }
    execute(&ctx, &mut session, now, &[b"SELECT", b"1"]);
    assert!(
        execute(&ctx, &mut session, now, &[b"SET", b"b", &value]).starts_with(b"-OOM"),
        "another database doesn't get a limit of its own"
    );
    execute(&ctx, &mut session, now, &[b"SELECT", b"0"]);
    execute(&ctx, &mut session, now, &[b"FLUSHALL"]);
    execute(&ctx, &mut session, now, &[b"SELECT", b"1"]);
    assert_eq!(
        execute(&ctx, &mut session, now, &[b"SET", b"b", &value]),
        b"+OK\r\n"
    );
}

#[tokio::test]
async fn keys_expire_in_every_database() {
    let ctx = Arc::new(Context::new(Arc::new(Store::new(4)), Settings::default()));
    std::mem::forget(cmd::spawn_expirer(&ctx, Duration::from_millis(5)));
    let now = ctx.store.now();
    let mut session = ctx.connect(-1).unwrap().session();
    execute(&ctx, &mut session, now, &[b"SELECT", b"7"]);
    execute(&ctx, &mut session, now, &[b"SET", b"k", b"v", b"PX", b"1"]);
    for _ in 0..1_000 {
        if ctx.store.stats().expired_keys == 1 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("the key was never swept");
}

#[tokio::test]
async fn client_kill_closes_who_it_picks() {
    for mode in MODES {
        let (addr, _ctx) = start(mode).await;
        let mut me = Client::connect(addr).await;
        // By id.
        let mut a = Client::connect(addr).await;
        let id = a.call(&["CLIENT", "ID"]).await;
        let id = id[1..].trim().to_string();
        assert_eq!(me.call(&["CLIENT", "KILL", "ID", &id]).await, ":1\r\n");
        assert!(closes(&mut a).await, "{mode:?}: by id");
        assert_eq!(me.call(&["CLIENT", "KILL", "ID", &id]).await, ":0\r\n");

        // By address, the old way, which says OK.
        let mut b = Client::connect(addr).await;
        assert_eq!(b.call(&["PING"]).await, "+PONG\r\n");
        let local = b.stream.get_ref().local_addr().unwrap().to_string();
        assert_eq!(me.call(&["CLIENT", "KILL", &local]).await, "+OK\r\n");
        assert!(closes(&mut b).await, "{mode:?}: by address");
        assert_eq!(
            me.call(&["CLIENT", "KILL", &local]).await,
            "-ERR No such client\r\n"
        );

        // By the new way's address.
        let mut c = Client::connect(addr).await;
        assert_eq!(c.call(&["PING"]).await, "+PONG\r\n");
        let local = c.stream.get_ref().local_addr().unwrap().to_string();
        assert_eq!(me.call(&["CLIENT", "KILL", "ADDR", &local]).await, ":1\r\n");
        assert!(closes(&mut c).await, "{mode:?}: by ADDR");

        // Subscribers, as Action Cable's tests have it: not the others.
        let mut sub = Client::connect(addr).await;
        let mut normal = Client::connect(addr).await;
        assert_eq!(normal.call(&["PING"]).await, "+PONG\r\n");
        sub.call(&["SUBSCRIBE", "ch"]).await;
        assert_eq!(
            me.call(&["CLIENT", "KILL", "TYPE", "pubsub"]).await,
            ":1\r\n"
        );
        assert!(closes(&mut sub).await, "{mode:?}: subscribers");
        assert_eq!(normal.call(&["PING"]).await, "+PONG\r\n");

        // No longer one once it unsubscribed.
        let mut was = Client::connect(addr).await;
        was.call(&["SUBSCRIBE", "ch"]).await;
        was.call(&["UNSUBSCRIBE", "ch"]).await;
        assert_eq!(
            me.call(&["CLIENT", "KILL", "TYPE", "pubsub"]).await,
            ":0\r\n"
        );
        assert_eq!(was.call(&["PING"]).await, "+PONG\r\n");

        // Everyone normal but the caller.
        assert_eq!(
            me.call(&["CLIENT", "KILL", "TYPE", "normal"]).await,
            ":2\r\n"
        );
        assert!(closes(&mut normal).await);
        assert_eq!(me.call(&["PING"]).await, "+PONG\r\n");
        assert_eq!(
            me.call(&["CLIENT", "KILL", "TYPE", "master"]).await,
            ":0\r\n"
        );

        // A client blocked in a command goes too.
        let mut blocked = Client::connect(addr).await;
        blocked.send(&["BLPOP", "nothing", "0"]).await;
        assert!(!blocked.has_reply().await);
        assert_eq!(
            me.call(&["CLIENT", "KILL", "TYPE", "normal"]).await,
            ":1\r\n"
        );
        assert!(closes(&mut blocked).await, "{mode:?}: blocked");

        // Itself, hearing first.
        let my_id = me.call(&["CLIENT", "ID"]).await;
        let my_id = my_id[1..].trim().to_string();
        assert_eq!(
            me.call(&["CLIENT", "KILL", "ID", &my_id, "SKIPME", "no"])
                .await,
            ":1\r\n"
        );
        assert!(closes(&mut me).await, "{mode:?}: itself");
    }
}

#[tokio::test]
async fn client_kill_reads_its_options_as_redis_does() {
    let (addr, _ctx) = start(Mode::PerThread).await;
    let mut c = Client::connect(addr).await;
    let syntax = "-ERR syntax error\r\n";
    assert_eq!(
        c.call(&["CLIENT", "KILL"]).await,
        "-ERR wrong number of arguments for 'client|kill' command\r\n"
    );
    assert_eq!(
        c.call(&["CLIENT", "KILL", "ID"]).await,
        "-ERR No such client\r\n"
    );
    assert_eq!(c.call(&["CLIENT", "KILL", "ID", "1", "TYPE"]).await, syntax);
    assert_eq!(c.call(&["CLIENT", "KILL", "NOPE", "1"]).await, syntax);
    assert_eq!(c.call(&["CLIENT", "KILL", "SKIPME", "maybe"]).await, syntax);
    assert_eq!(
        c.call(&["CLIENT", "KILL", "ID", "0"]).await,
        "-ERR client-id should be greater than 0\r\n"
    );
    assert_eq!(
        c.call(&["CLIENT", "KILL", "ID", "x"]).await,
        "-ERR client-id should be greater than 0\r\n"
    );
    assert_eq!(
        c.call(&["CLIENT", "KILL", "TYPE", "nope"]).await,
        "-ERR Unknown client type 'nope'\r\n"
    );
    assert_eq!(
        c.call(&["CLIENT", "KILL", "USER", "nobody"]).await,
        "-ERR No such user 'nobody'\r\n"
    );
    assert_eq!(
        c.call(&["CLIENT", "KILL", "USER", "default"]).await,
        ":0\r\n"
    );
}
