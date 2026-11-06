//! What operators and tools ask a server: `MONITOR`, `SLOWLOG`, `CONFIG`, `CLIENT PAUSE`
//! and the rest. Replies are as Redis 7.2.4 gives them (see `tools/admin-diff.py`).

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{Client, MODES, Mode, start};
use tantalum::context::{Context, DebugCommand, Settings};
use tantalum::server;
use tantalum::store::Store;
use tokio::io::AsyncReadExt;

/// What's come for a monitor within a moment, with times and ports taken out.
async fn heard(c: &mut Client) -> Vec<String> {
    let mut text = Vec::new();
    let mut buf = [0; 4096];
    loop {
        let read = c.stream.get_mut().read(&mut buf);
        match tokio::time::timeout(Duration::from_millis(200), read).await {
            Ok(Ok(n)) if n > 0 => text.extend_from_slice(&buf[..n]),
            _ => break,
        }
    }
    String::from_utf8_lossy(&text)
        .split("\r\n")
        .filter(|line| !line.is_empty())
        .map(|line| {
            let (_, rest) = line.split_once(" [").expect("a time first");
            let (who, args) = rest.split_once("] ").expect("who it was");
            let (db, addr) = who.split_once(' ').expect("the database");
            let addr = if addr == "lua" { "lua" } else { "client" };
            format!("[{db} {addr}] {args}")
        })
        .collect()
}

#[tokio::test]
async fn monitor_hears_what_redis_shows() {
    for mode in MODES {
        let (addr, _ctx) = start(mode).await;
        let mut m = Client::connect(addr).await;
        let mut c = Client::connect(addr).await;
        assert_eq!(m.call(&["MONITOR"]).await, "+OK\r\n");
        // Connections look once a batch.
        c.call(&["PING"]).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        heard(&mut m).await;

        c.call(&["SET", "k", "a b\n\"\\\u{7f}"]).await;
        c.call(&["GET", "k"]).await;
        c.call(&["AUTH", "x"]).await;
        c.call(&["AUTH", "u", "p"]).await;
        c.call(&["HELLO", "3", "AUTH", "u", "p"]).await;
        c.call(&["HELLO", "2"]).await;
        c.call(&["SELECT", "3"]).await;
        c.call(&["SET", "k", "3"]).await;
        c.call(&["SELECT", "0"]).await;
        c.call(&["MULTI"]).await;
        c.call(&["SET", "k", "m"]).await;
        c.call(&["GET", "k"]).await;
        c.call(&["EXEC"]).await;
        c.call(&["EVAL", "return redis.call('GET', KEYS[1])", "1", "k"])
            .await;
        // Not these: admin, and what never ran.
        c.call(&["CONFIG", "GET", "hz"]).await;
        c.call(&["CLIENT", "LIST"]).await;
        c.call(&["SLOWLOG", "LEN"]).await;
        c.call(&["NOPE"]).await;
        c.call(&["GET"]).await;
        // But these.
        c.call(&["CLIENT", "ID"]).await;
        c.call(&["PING"]).await;
        let lines = heard(&mut m).await;
        let wanted: Vec<String> = [
            r#"[0 client] "SET" "k" "a b\n\"\\\x7f""#,
            r#"[0 client] "GET" "k""#,
            r#"[0 client] "AUTH" "(redacted)""#,
            r#"[0 client] "AUTH" "(redacted)" "(redacted)""#,
            r#"[0 client] "HELLO" "3" "AUTH" "(redacted)" "(redacted)""#,
            r#"[0 client] "HELLO" "2""#,
            r#"[3 client] "SELECT" "3""#,
            r#"[3 client] "SET" "k" "3""#,
            r#"[0 client] "SELECT" "0""#,
            r#"[0 client] "MULTI""#,
            r#"[0 client] "SET" "k" "m""#,
            r#"[0 client] "GET" "k""#,
            r#"[0 client] "EXEC""#,
            r#"[0 client] "EVAL" "return redis.call('GET', KEYS[1])" "1" "k""#,
            r#"[0 lua] "GET" "k""#,
            r#"[0 client] "CLIENT" "ID""#,
            r#"[0 client] "PING""#,
        ]
        .iter()
        .map(|line| line.to_string())
        .collect();
        assert_eq!(lines, wanted, "{mode:?}");
    }
}

#[tokio::test]
async fn monitor_is_refused_where_redis_refuses_it_and_ends() {
    let (addr, ctx) = start(Mode::PerThread).await;
    let mut c = Client::connect(addr).await;
    c.call(&["MULTI"]).await;
    assert_eq!(
        c.call(&["MONITOR"]).await,
        "-ERR Command not allowed inside a transaction\r\n"
    );
    c.call(&["DISCARD"]).await;
    assert!(
        c.call(&["EVAL", "return redis.pcall('MONITOR')", "0"])
            .await
            .contains("not allowed from script")
    );
    // It ends with RESET, and with the connection: then commands take the fast path again.
    let mut m = Client::connect(addr).await;
    m.call(&["MONITOR"]).await;
    assert!(c.call(&["CLIENT", "LIST"]).await.contains("flags=O"));
    m.send(&["RESET"]).await;
    // (It hears of its own, as Redis' does.)
    while m.reply().await != "+RESET\r\n" {}
    assert!(m.call(&["CLIENT", "INFO"]).await.contains("flags=N"));
    drop(m);
    c.call(&["PING"]).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    c.call(&["PING"]).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    // The next command notices nobody's left to hear, and the one after takes the
    // fast path: nothing here but that it still answers.
    assert_eq!(c.call(&["PING"]).await, "+PONG\r\n");
    assert_eq!(ctx.store.blocked_clients(), 0);
}

/// Waits for a command that's been sent to come back, and how long it took.
async fn timed_reply(c: &mut Client) -> (String, Duration) {
    let started = Instant::now();
    let reply = c.reply().await;
    (reply, started.elapsed())
}

#[tokio::test]
async fn pause_holds_back_what_redis_holds_back() {
    for mode in MODES {
        let (addr, _ctx) = start(mode).await;
        let mut a = Client::connect(addr).await;
        let mut b = Client::connect(addr).await;
        b.call(&["PING"]).await;
        // WRITE: reads go on, and what writes or may waits.
        assert_eq!(
            a.call(&["CLIENT", "PAUSE", "300", "WRITE"]).await,
            "+OK\r\n"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
        b.send(&["GET", "x"]).await;
        let (reply, took) = timed_reply(&mut b).await;
        assert_eq!(reply, "$-1\r\n");
        assert!(
            took < Duration::from_millis(200),
            "{mode:?}: GET took {took:?}"
        );
        for (command, wanted) in [
            (&["SET", "x", "1"][..], "+OK\r\n"),
            (&["PUBLISH", "c", "m"][..], ":0\r\n"),
            (&["EVAL", "return 1", "0"][..], ":1\r\n"),
        ] {
            a.call(&["CLIENT", "PAUSE", "300", "WRITE"]).await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            b.send(command).await;
            let (reply, took) = timed_reply(&mut b).await;
            assert_eq!(reply, wanted, "{mode:?}: {command:?}");
            assert!(
                took >= Duration::from_millis(200),
                "{mode:?}: {command:?} took {took:?}"
            );
        }
        // UNPAUSE ends it, while it's only writes.
        a.call(&["CLIENT", "PAUSE", "10000", "WRITE"]).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        b.send(&["SET", "y", "1"]).await;
        assert_eq!(a.call(&["CLIENT", "UNPAUSE"]).await, "+OK\r\n");
        let (reply, took) = timed_reply(&mut b).await;
        assert_eq!(reply, "+OK\r\n");
        assert!(
            took < Duration::from_millis(1000),
            "{mode:?}: took {took:?}"
        );
        // ALL: everything, even UNPAUSE from another client, until the time's up.
        a.call(&["CLIENT", "PAUSE", "300", "ALL"]).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        b.send(&["CLIENT", "UNPAUSE"]).await;
        let (reply, took) = timed_reply(&mut b).await;
        assert_eq!(reply, "+OK\r\n");
        assert!(
            took >= Duration::from_millis(200),
            "{mode:?}: UNPAUSE took {took:?}"
        );
        // A transaction with a write waits at its write, then at EXEC.
        a.call(&["CLIENT", "PAUSE", "200", "WRITE"]).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        b.call(&["MULTI"]).await;
        b.call(&["SET", "z", "1"]).await;
        assert_eq!(b.call(&["EXEC"]).await, "*1\r\n+OK\r\n");
    }
}

#[tokio::test]
async fn pause_reads_its_arguments_as_redis_does() {
    let (addr, _ctx) = start(Mode::PerThread).await;
    let mut c = Client::connect(addr).await;
    assert_eq!(
        c.call(&["CLIENT", "PAUSE", "x"]).await,
        "-ERR timeout is not an integer or out of range\r\n"
    );
    assert_eq!(
        c.call(&["CLIENT", "PAUSE", "-1"]).await,
        "-ERR timeout is negative\r\n"
    );
    assert_eq!(
        c.call(&["CLIENT", "PAUSE", "1", "NOPE"]).await,
        "-ERR CLIENT PAUSE mode must be WRITE or ALL\r\n"
    );
}

#[tokio::test]
async fn unblock_ends_a_blocking_command() {
    for mode in MODES {
        let (addr, _ctx) = start(mode).await;
        let mut a = Client::connect(addr).await;
        let mut b = Client::connect(addr).await;
        let id = b.call(&["CLIENT", "ID"]).await;
        let id = id[1..].trim().to_string();
        assert_eq!(a.call(&["CLIENT", "UNBLOCK", &id]).await, ":0\r\n");
        b.send(&["BLPOP", "nothing", "0"]).await;
        assert!(!b.has_reply().await);
        assert_eq!(a.call(&["CLIENT", "UNBLOCK", &id]).await, ":1\r\n");
        assert_eq!(b.reply().await, "*-1\r\n", "{mode:?}");
        b.send(&["BLPOP", "nothing", "0"]).await;
        assert!(!b.has_reply().await);
        assert_eq!(a.call(&["CLIENT", "UNBLOCK", &id, "ERROR"]).await, ":1\r\n");
        assert_eq!(
            b.reply().await,
            "-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n",
            "{mode:?}"
        );
        // WAIT with nobody to hear from can be ended too.
        b.send(&["WAIT", "1", "0"]).await;
        assert!(!b.has_reply().await);
        assert_eq!(a.call(&["CLIENT", "UNBLOCK", &id]).await, ":1\r\n");
        assert_eq!(b.reply().await, ":0\r\n", "{mode:?}");
        assert_eq!(
            a.call(&["CLIENT", "UNBLOCK", &id, "NOPE"]).await,
            "-ERR CLIENT UNBLOCK reason should be TIMEOUT or ERROR\r\n"
        );
        assert_eq!(
            a.call(&["CLIENT", "UNBLOCK", "x"]).await,
            "-ERR value is not an integer or out of range\r\n"
        );
    }
}

#[tokio::test]
async fn client_reply_silences_as_asked() {
    let (addr, _ctx) = start(Mode::PerThread).await;
    let mut c = Client::connect(addr).await;
    c.send(&["CLIENT", "REPLY", "SKIP"]).await;
    c.send(&["SET", "a", "1"]).await;
    assert_eq!(c.call(&["GET", "a"]).await, "$1\r\n1\r\n");
    c.send(&["CLIENT", "REPLY", "OFF"]).await;
    c.send(&["SET", "a", "2"]).await;
    c.send(&["GET", "a"]).await;
    assert_eq!(c.call(&["CLIENT", "REPLY", "ON"]).await, "+OK\r\n");
    assert_eq!(c.call(&["GET", "a"]).await, "$1\r\n2\r\n");
    assert_eq!(
        c.call(&["CLIENT", "REPLY", "NOPE"]).await,
        "-ERR syntax error\r\n"
    );
}

#[tokio::test]
async fn small_client_subcommands() {
    let (addr, _ctx) = start(Mode::PerThread).await;
    let mut c = Client::connect(addr).await;
    assert_eq!(c.call(&["CLIENT", "NO-EVICT", "ON"]).await, "+OK\r\n");
    assert_eq!(
        c.call(&["CLIENT", "NO-EVICT", "x"]).await,
        "-ERR syntax error\r\n"
    );
    assert_eq!(c.call(&["CLIENT", "NO-TOUCH", "ON"]).await, "+OK\r\n");
    assert_eq!(
        c.call(&["CLIENT", "NO-TOUCH", "x"]).await,
        "-ERR syntax error\r\n"
    );
    let info = c.call(&["CLIENT", "INFO"]).await;
    assert!(info.contains("flags=eT "), "{info}");
    assert_eq!(c.call(&["CLIENT", "GETREDIR"]).await, ":-1\r\n");
    assert_eq!(
        c.call(&["CLIENT", "TRACKINGINFO"]).await,
        "*6\r\n$5\r\nflags\r\n*1\r\n$3\r\noff\r\n$8\r\nredirect\r\n:-1\r\n$8\r\nprefixes\r\n*0\r\n"
    );
    // A subscriber's cmd is known to others, which is what redis-py's tests go by.
    let mut s = Client::connect(addr).await;
    s.call(&["SUBSCRIBE", "ch"]).await;
    let list = c.call(&["CLIENT", "LIST"]).await;
    assert!(list.contains("cmd=subscribe"), "{list}");
}

#[tokio::test]
async fn slowlog_keeps_what_it_should() {
    let (addr, _ctx) = start(Mode::PerThread).await;
    let mut c = Client::connect(addr).await;
    assert_eq!(c.call(&["SLOWLOG", "LEN"]).await, ":0\r\n");
    assert_eq!(c.call(&["SLOWLOG", "GET"]).await, "*0\r\n");
    assert_eq!(
        c.call(&["CONFIG", "SET", "slowlog-log-slower-than", "0"])
            .await,
        "+OK\r\n"
    );
    // From the next batch on.
    c.call(&["SLOWLOG", "RESET"]).await;
    c.call(&["SLOWLOG", "RESET"]).await;
    let long = "x".repeat(200);
    c.call(&["SET", "long", &long]).await;
    let many: Vec<String> = (0..40).map(|i| format!("a{i}")).collect();
    let mut mset = vec!["MGET"];
    mset.extend(many.iter().map(String::as_str));
    c.call(&mset).await;
    c.call(&["CLIENT", "SETNAME", "who"]).await;
    c.call(&["GET", "long"]).await;
    let log = c.call(&["SLOWLOG", "GET", "4"]).await;
    assert!(log.starts_with("*4\r\n"), "{log}");
    // Newest first, and who it was.
    assert!(log.contains("$3\r\nGET\r\n$4\r\nlong\r\n"), "{log}");
    assert!(log.contains("$3\r\nwho\r\n"), "{log}");
    // Long arguments and long commands are cut as Redis cuts them.
    assert!(log.contains("... (72 more bytes)"), "{log}");
    assert!(log.contains("... (10 more arguments)"), "{log}");

    assert_eq!(
        c.call(&["CONFIG", "SET", "slowlog-max-len", "1"]).await,
        "+OK\r\n"
    );
    assert_eq!(c.call(&["SLOWLOG", "LEN"]).await, ":1\r\n");
    assert_eq!(
        c.call(&["CONFIG", "SET", "slowlog-log-slower-than", "-1"])
            .await,
        "+OK\r\n"
    );
    c.call(&["SLOWLOG", "RESET"]).await;
    c.call(&["PING"]).await;
    assert_eq!(c.call(&["SLOWLOG", "LEN"]).await, ":0\r\n");
    for (command, reply) in [
        (
            &["SLOWLOG", "GET", "-2"][..],
            "-ERR count should be greater than or equal to -1\r\n",
        ),
        (
            &["SLOWLOG", "GET", "x"][..],
            "-ERR count should be greater than or equal to -1\r\n",
        ),
        (
            &["SLOWLOG", "GET", "1", "2"][..],
            "-ERR unknown subcommand or wrong number of arguments for 'GET'. Try SLOWLOG HELP.\r\n",
        ),
        (
            &["SLOWLOG", "LEN", "1"][..],
            "-ERR wrong number of arguments for 'slowlog|len' command\r\n",
        ),
        (
            &["SLOWLOG", "RESET", "1"][..],
            "-ERR wrong number of arguments for 'slowlog|reset' command\r\n",
        ),
        (
            &["SLOWLOG", "NOPE"][..],
            "-ERR unknown subcommand 'NOPE'. Try SLOWLOG HELP.\r\n",
        ),
        (
            &["SLOWLOG"][..],
            "-ERR wrong number of arguments for 'slowlog' command\r\n",
        ),
    ] {
        assert_eq!(c.call(command).await, reply, "{command:?}");
    }
}

#[tokio::test]
async fn the_default_threshold_catches_a_slow_command_by_the_ticking_clock() {
    let ctx = Arc::new(Context::new(
        Arc::new(Store::new(16)),
        Settings {
            debug_command: DebugCommand::Yes,
            ..Settings::default()
        },
    ));
    std::mem::forget(tantalum::cmd::spawn_expirer(
        &ctx,
        Duration::from_millis(50),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(server::serve(listener, ctx.clone()));
    let mut c = Client::connect(addr).await;
    c.call(&["PING"]).await;
    // Under the threshold, and over it.
    c.call(&["DEBUG", "SLEEP", "0.001"]).await;
    assert_eq!(c.call(&["SLOWLOG", "LEN"]).await, ":0\r\n");
    c.call(&["DEBUG", "SLEEP", "0.05"]).await;
    assert_eq!(c.call(&["SLOWLOG", "LEN"]).await, ":1\r\n");
    let log = c.call(&["SLOWLOG", "GET"]).await;
    assert!(log.contains("$5\r\nDEBUG\r\n$5\r\nSLEEP\r\n"), "{log}");
}

#[tokio::test]
async fn the_clock_sleeps_when_nothing_runs_and_still_times() {
    let ctx = Arc::new(Context::new(
        Arc::new(Store::new(16)),
        Settings {
            debug_command: DebugCommand::Yes,
            ..Settings::default()
        },
    ));
    std::mem::forget(tantalum::cmd::spawn_expirer(
        &ctx,
        Duration::from_millis(50),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(server::serve(listener, ctx.clone()));
    let mut c = Client::connect(addr).await;
    let micros_of_last = async |c: &mut Client| -> u64 {
        let log = c.call(&["SLOWLOG", "GET", "1"]).await;
        let mut numbers = log.split("\r\n").filter_map(|l| l.strip_prefix(':'));
        numbers.nth(2).expect("a duration").parse().unwrap()
    };
    // Long enough for the clock to go to sleep. The first command after it counts.
    c.call(&["PING"]).await;
    tokio::time::sleep(Duration::from_millis(1300)).await;
    c.call(&["DEBUG", "SLEEP", "0.05"]).await;
    assert_eq!(c.call(&["SLOWLOG", "LEN"]).await, ":1\r\n");
    let micros = micros_of_last(&mut c).await;
    assert!((35_000..200_000).contains(&micros), "{micros}");
    // And one running as it goes to sleep is timed in full.
    c.call(&["DEBUG", "SLEEP", "1.5"]).await;
    assert_eq!(c.call(&["SLOWLOG", "LEN"]).await, ":2\r\n");
    let log = c.call(&["SLOWLOG", "GET", "1"]).await;
    assert!(log.contains("1.5"), "{log}");
    let micros = micros_of_last(&mut c).await;
    assert!((1_400_000..2_500_000).contains(&micros), "{micros}");
    // Woken again, it's as it was: nothing short is logged.
    tokio::time::sleep(Duration::from_millis(1300)).await;
    c.call(&["SLOWLOG", "RESET"]).await;
    for _ in 0..50 {
        c.call(&["CONFIG", "GET", "hz"]).await;
    }
    assert_eq!(c.call(&["SLOWLOG", "LEN"]).await, ":0\r\n");
}

#[tokio::test]
async fn config_reads_and_sets() {
    let (addr, ctx) = start(Mode::PerThread).await;
    let mut c = Client::connect(addr).await;
    // Redis' whole table.
    let all = c.call(&["CONFIG", "GET", "*"]).await;
    assert!(all.starts_with("*"), "{all}");
    for name in [
        "loglevel",
        "hash-max-listpack-entries",
        "stream-node-max-entries",
        "slowlog-log-slower-than",
        "timeout",
    ] {
        assert!(all.contains(name), "{name}");
    }
    assert_eq!(
        c.call(&["CONFIG", "GET", "loglevel"]).await,
        "*2\r\n$8\r\nloglevel\r\n$6\r\nnotice\r\n"
    );
    for (command, reply) in [
        (
            &["CONFIG", "SET", "nope", "1"][..],
            "-ERR Unknown option or number of arguments for CONFIG SET - 'nope'\r\n",
        ),
        (
            &["CONFIG", "SET", "hz"][..],
            "-ERR wrong number of arguments for 'config|set' command\r\n",
        ),
        (
            &["CONFIG", "SET", "hz", "x"][..],
            "-ERR CONFIG SET failed (possibly related to argument 'hz') - argument couldn't be parsed into an integer\r\n",
        ),
        (
            &["CONFIG", "SET", "databases", "3"][..],
            "-ERR CONFIG SET failed (possibly related to argument 'databases') - can't set immutable config\r\n",
        ),
        (
            &["CONFIG", "SET", "maxmemory", "x"][..],
            "-ERR CONFIG SET failed (possibly related to argument 'maxmemory') - argument must be a memory value\r\n",
        ),
        (
            &["CONFIG", "SET", "appendonly", "x"][..],
            "-ERR CONFIG SET failed (possibly related to argument 'appendonly') - argument must be 'yes' or 'no'\r\n",
        ),
        (
            &["CONFIG", "SET", "hz", "10", "hz", "11"][..],
            "-ERR CONFIG SET failed (possibly related to argument 'hz') - duplicate parameter\r\n",
        ),
        (
            &["CONFIG", "REWRITE"][..],
            "-ERR The server is running without a config file\r\n",
        ),
        (
            &["CONFIG", "NOPE"][..],
            "-ERR unknown subcommand 'NOPE'. Try CONFIG HELP.\r\n",
        ),
    ] {
        assert_eq!(c.call(command).await, reply, "{command:?}");
    }
    // Remembered, under both its names.
    assert_eq!(
        c.call(&["CONFIG", "SET", "hash-max-listpack-entries", "7"])
            .await,
        "+OK\r\n"
    );
    assert_eq!(
        c.call(&["CONFIG", "GET", "hash-max-ziplist-entries"]).await,
        "*2\r\n$24\r\nhash-max-ziplist-entries\r\n$1\r\n7\r\n"
    );
    // All or nothing.
    assert!(
        c.call(&["CONFIG", "SET", "hz", "20", "nope", "1"])
            .await
            .starts_with("-ERR Unknown")
    );
    assert_eq!(
        c.call(&["CONFIG", "GET", "hz"]).await,
        "*2\r\n$2\r\nhz\r\n$2\r\n10\r\n"
    );
    // What acts: memory, as units.
    assert_eq!(
        c.call(&["CONFIG", "SET", "maxmemory", "10mb"]).await,
        "+OK\r\n"
    );
    assert_eq!(
        c.call(&["CONFIG", "GET", "maxmemory"]).await,
        "*2\r\n$9\r\nmaxmemory\r\n$8\r\n10485760\r\n"
    );
    assert_eq!(ctx.store.maxmemory(), 10 << 20);
    assert_eq!(
        c.call(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"])
            .await,
        "+OK\r\n"
    );
    assert_eq!(
        c.call(&["CONFIG", "GET", "maxmemory-policy"]).await,
        "*2\r\n$16\r\nmaxmemory-policy\r\n$11\r\nallkeys-lru\r\n"
    );
    // Those that can't be done say so.
    assert!(
        c.call(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lfu"])
            .await
            .contains("noeviction, allkeys-lru and volatile-lru")
    );
    assert!(
        c.call(&["CONFIG", "SET", "maxmemory-policy", "x"])
            .await
            .contains("must be one of the following: volatile-lru")
    );
    assert!(
        c.call(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
            .await
            .contains("keyspace notifications")
    );
    assert_eq!(
        c.call(&["CONFIG", "SET", "notify-keyspace-events", ""])
            .await,
        "+OK\r\n"
    );
    assert!(
        c.call(&["CONFIG", "SET", "appendonly", "yes"])
            .await
            .contains("--aof")
    );
    assert_eq!(
        c.call(&["CONFIG", "SET", "appendonly", "no"]).await,
        "+OK\r\n"
    );
    // RESETSTAT.
    c.call(&["GET", "nope"]).await;
    assert_eq!(c.call(&["CONFIG", "RESETSTAT"]).await, "+OK\r\n");
    let info = c.call(&["INFO", "stats"]).await;
    assert!(info.contains("keyspace_misses:0\r\n"), "{info}");
}

#[tokio::test]
async fn maxmemory_set_at_runtime_holds() {
    let (addr, ctx) = start(Mode::PerThread).await;
    let mut c = Client::connect(addr).await;
    let value = "v".repeat(1000);
    assert_eq!(
        c.call(&["CONFIG", "SET", "maxmemory", "1mb"]).await,
        "+OK\r\n"
    );
    let mut refused = false;
    for i in 0..5_000 {
        if c.call(&["SET", &format!("k{i}"), &value])
            .await
            .starts_with("-OOM")
        {
            refused = true;
            break;
        }
    }
    assert!(refused, "noeviction, the default, refuses");
    assert_eq!(
        c.call(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"])
            .await,
        "+OK\r\n"
    );
    for i in 0..5_000 {
        assert_eq!(c.call(&["SET", &format!("k{i}"), &value]).await, "+OK\r\n");
    }
    assert!(ctx.store.stats().evicted_keys > 0);
    assert_eq!(
        c.call(&["CONFIG", "SET", "maxmemory", "0"]).await,
        "+OK\r\n"
    );
    assert_eq!(c.call(&["SET", "after", &value]).await, "+OK\r\n");
}

#[tokio::test]
async fn requirepass_set_at_runtime() {
    let (addr, _ctx) = start(Mode::PerThread).await;
    let mut c = Client::connect(addr).await;
    assert_eq!(
        c.call(&["CONFIG", "SET", "requirepass", "secret"]).await,
        "+OK\r\n"
    );
    // Those already in stay in; others need it.
    assert_eq!(c.call(&["PING"]).await, "+PONG\r\n");
    let mut other = Client::connect(addr).await;
    assert_eq!(
        other.call(&["PING"]).await,
        "-NOAUTH Authentication required.\r\n"
    );
    assert_eq!(
        other.call(&["AUTH", "wrong"]).await,
        "-WRONGPASS invalid username-password pair or user is disabled.\r\n"
    );
    assert_eq!(other.call(&["AUTH", "secret"]).await, "+OK\r\n");
    assert_eq!(other.call(&["PING"]).await, "+PONG\r\n");
    assert_eq!(
        other.call(&["CONFIG", "GET", "requirepass"]).await,
        "*2\r\n$11\r\nrequirepass\r\n$6\r\nsecret\r\n"
    );
    assert_eq!(
        c.call(&["CONFIG", "SET", "requirepass", ""]).await,
        "+OK\r\n"
    );
    let mut third = Client::connect(addr).await;
    assert_eq!(third.call(&["PING"]).await, "+PONG\r\n");
}

#[tokio::test]
async fn command_answers_with_what_redis_says() {
    let (addr, _ctx) = start(Mode::PerThread).await;
    let mut c = Client::connect(addr).await;
    let info = c.call(&["COMMAND", "INFO", "get"]).await;
    assert!(info.contains("+@read\r\n+@string\r\n+@fast\r\n"), "{info}");
    assert!(info.contains("begin_search"), "{info}");
    for (command, reply) in [
        (
            &["COMMAND", "GETKEYS"][..],
            "-ERR wrong number of arguments for 'command|getkeys' command\r\n",
        ),
        (
            &["COMMAND", "GETKEYS", "GET"][..],
            "-ERR Invalid number of arguments specified for command\r\n",
        ),
        (
            &["COMMAND", "GETKEYS", "PING"][..],
            "-ERR The command has no key arguments\r\n",
        ),
        (
            &["COMMAND", "GETKEYS", "NOPE", "x"][..],
            "-ERR Invalid command specified\r\n",
        ),
        (
            &["COMMAND", "GETKEYS", "MSET", "a", "1", "b", "2"][..],
            "*2\r\n$1\r\na\r\n$1\r\nb\r\n",
        ),
        (&["COMMAND", "GETKEYS", "EVAL", "x", "0"][..], "*0\r\n"),
        (
            &["COMMAND", "GETKEYS", "EVAL", "x", "1", "k"][..],
            "*1\r\n$1\r\nk\r\n",
        ),
        (
            &["COMMAND", "GETKEYSANDFLAGS", "SET", "k", "v"][..],
            "*1\r\n*2\r\n$1\r\nk\r\n*2\r\n+OW\r\n+update\r\n",
        ),
        (
            &["COMMAND", "GETKEYS", "SORT", "a", "STORE", "b"][..],
            "*2\r\n$1\r\na\r\n$1\r\nb\r\n",
        ),
        (
            &["COMMAND", "GETKEYS", "XREAD", "STREAMS", "a", "b", "0", "0"][..],
            "*2\r\n$1\r\na\r\n$1\r\nb\r\n",
        ),
        (
            &["COMMAND", "GETKEYS", "ZUNION", "2", "a", "b"][..],
            "*2\r\n$1\r\na\r\n$1\r\nb\r\n",
        ),
        (
            &["COMMAND", "GETKEYS", "OBJECT", "ENCODING", "k"][..],
            "*1\r\n$1\r\nk\r\n",
        ),
        (
            &["COMMAND", "LIST", "FILTERBY", "PATTERN", "lol*"][..],
            "*1\r\n$6\r\nlolwut\r\n",
        ),
        (
            &["COMMAND", "LIST", "FILTERBY", "MODULE", "x"][..],
            "*0\r\n",
        ),
        (&["COMMAND", "LIST", "NOPE"][..], "-ERR syntax error\r\n"),
        (
            &["COMMAND", "COUNT", "x"][..],
            "-ERR wrong number of arguments for 'command|count' command\r\n",
        ),
    ] {
        assert_eq!(c.call(command).await, reply, "{command:?}");
    }
    let docs = c.call(&["COMMAND", "DOCS", "get"]).await;
    assert!(
        docs.contains("Returns the string value of a key."),
        "{docs}"
    );
    let count: usize = c.call(&["COMMAND", "COUNT"]).await[1..]
        .trim()
        .parse()
        .unwrap();
    assert!(
        c.call(&["COMMAND", "DOCS"])
            .await
            .starts_with(&format!("*{}\r\n", count * 2))
    );
    assert!(
        c.call(&["COMMAND", "LIST", "FILTERBY", "ACLCAT", "stream"])
            .await
            .contains("xadd")
    );
    // RESP3: sets and maps.
    c.call(&["HELLO", "3"]).await;
    assert!(
        c.call(&["COMMAND", "INFO", "get"])
            .await
            .contains("~3\r\n+@read")
    );
    assert!(
        c.call(&["COMMAND", "DOCS", "get"])
            .await
            .starts_with("%1\r\n")
    );
}

#[tokio::test]
async fn the_small_ones() {
    let (addr, _ctx) = start(Mode::PerThread).await;
    let mut c = Client::connect(addr).await;
    for (command, reply) in [
        (&["ROLE"][..], "*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n"),
        (&["REPLICAOF", "NO", "ONE"][..], "+OK\r\n"),
        (&["SLAVEOF", "NO", "ONE"][..], "+OK\r\n"),
        (&["MODULE", "LIST"][..], "*0\r\n"),
        (
            &["MODULE", "NOPE"][..],
            "-ERR unknown subcommand 'NOPE'. Try MODULE HELP.\r\n",
        ),
        (&["LATENCY", "LATEST"][..], "*0\r\n"),
        (&["LATENCY", "HISTORY", "x"][..], "*0\r\n"),
        (&["LATENCY", "RESET"][..], ":0\r\n"),
        (
            &["LATENCY", "GRAPH", "x"][..],
            "-ERR No samples available for event 'x'\r\n",
        ),
        (
            &["LATENCY", "NOPE"][..],
            "-ERR unknown subcommand 'NOPE'. Try LATENCY HELP.\r\n",
        ),
        (&["WAIT", "0", "0"][..], ":0\r\n"),
        (
            &["WAIT", "x", "0"][..],
            "-ERR value is not an integer or out of range\r\n",
        ),
        (
            &["WAIT", "0", "x"][..],
            "-ERR timeout is not an integer or out of range\r\n",
        ),
        (&["WAIT", "0", "-1"][..], "-ERR timeout is negative\r\n"),
        (&["WAIT", "1", "10"][..], ":0\r\n"),
        (&["WAITAOF", "0", "0", "0"][..], "*2\r\n:0\r\n:0\r\n"),
        (
            &["WAITAOF", "1", "0", "0"][..],
            "-ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.\r\n",
        ),
        (
            &["WAITAOF", "2", "0", "0"][..],
            "-ERR value is out of range, value must between 0 and 1\r\n",
        ),
        (&["WAITAOF", "0", "1", "10"][..], "*2\r\n:0\r\n:0\r\n"),
        (
            &["MEMORY", "NOPE"][..],
            "-ERR unknown subcommand 'NOPE'. Try MEMORY HELP.\r\n",
        ),
        (&["MEMORY", "USAGE", "nokey"][..], "$-1\r\n"),
        (&["MEMORY", "PURGE"][..], "+OK\r\n"),
        (
            &["DEBUG", "POPULATE", "1"][..],
            "-ERR DEBUG command not allowed. If the enable-debug-command option is set to \"local\", you can run it from a local connection, otherwise you need to set this option in the configuration file, and then restart the server.\r\n",
        ),
    ] {
        assert_eq!(c.call(command).await, reply, "{command:?}");
    }
    assert!(c.call(&["LOLWUT"]).await.contains("Redis ver. 7.2.4"));
    assert!(
        c.call(&["REPLICAOF", "127.0.0.1", "6379"])
            .await
            .starts_with("-ERR")
    );
    c.call(&["SET", "k", "v"]).await;
    let size: i64 = c.call(&["MEMORY", "USAGE", "k"]).await[1..]
        .trim()
        .parse()
        .unwrap();
    assert!((1..200).contains(&size), "{size}");
    assert_eq!(
        c.call(&["MEMORY", "USAGE", "k", "SAMPLES", "x"]).await,
        "-ERR value is not an integer or out of range\r\n"
    );
    assert_eq!(
        c.call(&["MEMORY", "USAGE", "k", "NOPE", "1"]).await,
        "-ERR syntax error\r\n"
    );
    assert_eq!(
        c.call(&["MEMORY", "USAGE", "k", "SAMPLES", "-1"]).await,
        "-ERR syntax error\r\n"
    );
    let stats = c.call(&["MEMORY", "STATS"]).await;
    assert!(
        stats.contains("total.allocated") && stats.contains("db.0"),
        "{stats}"
    );
    assert!(c.call(&["MEMORY", "DOCTOR"]).await.starts_with("$"));
    assert!(c.call(&["MEMORY", "MALLOC-STATS"]).await.starts_with("$"));
    // A big one counts what it holds.
    c.call(&["RPUSH", "big", &"x".repeat(100_000)]).await;
    let size: i64 = c.call(&["MEMORY", "USAGE", "big"]).await[1..]
        .trim()
        .parse()
        .unwrap();
    assert!(size > 100_000, "{size}");
}

#[tokio::test]
async fn logs_keep_secrets() {
    for mode in MODES {
        let (addr, _ctx) = start(mode).await;
        let mut c = Client::connect(addr).await;
        let mut m = Client::connect(addr).await;
        c.call(&["CONFIG", "SET", "slowlog-log-slower-than", "0"])
            .await;
        c.call(&["AUTH", "sekret"]).await;
        c.call(&["AUTH", "user", "sekret"]).await;
        c.call(&["HELLO", "2", "AUTH", "user", "sekret"]).await;
        c.call(&["CONFIG", "SET", "masterauth", "sekret"]).await;
        c.call(&["CONFIG", "SET", "hz", "x", "masteruser", "sekret"])
            .await;
        let log = c.call(&["SLOWLOG", "GET", "-1"]).await;
        assert!(!log.contains("sekret"), "{log}");
        assert!(log.contains("(redacted)"), "{log}");
        assert_eq!(m.call(&["MONITOR"]).await, "+OK\r\n");
        c.call(&["PING"]).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        heard(&mut m).await;
        c.call(&["AUTH", "user", "sekret"]).await;
        let lines = heard(&mut m).await;
        assert_eq!(
            lines,
            ["[0 client] \"AUTH\" \"(redacted)\" \"(redacted)\""],
            "{mode:?}"
        );
        // A script's heard of inside `EXEC` too, even one that isn't found (ioredis' tests
        // count on it), and an admin command isn't.
        c.call(&["MULTI"]).await;
        c.call(&["EVALSHA", "ffffffffffffffffffffffffffffffffffffffff", "0"])
            .await;
        c.call(&["CONFIG", "GET", "hz"]).await;
        c.call(&["EVAL", "return redis.call('PING')", "0"]).await;
        c.call(&["EXEC"]).await;
        let lines = heard(&mut m).await;
        assert_eq!(
            lines,
            [
                "[0 client] \"MULTI\"",
                "[0 client] \"EVALSHA\" \"ffffffffffffffffffffffffffffffffffffffff\" \"0\"",
                "[0 client] \"EVAL\" \"return redis.call('PING')\" \"0\"",
                "[0 lua] \"PING\"",
                "[0 client] \"EXEC\"",
            ],
            "{mode:?}"
        );
    }
}

#[tokio::test]
async fn slowlog_has_what_scripts_and_transactions_run() {
    let (addr, _ctx) = start(Mode::Shared).await;
    let mut c = Client::connect(addr).await;
    let names = |log: &str| -> Vec<String> {
        log.split("\r\n")
            .filter(|line| ["EVAL", "GET", "SET", "EXEC", "MULTI", "CONFIG"].contains(line))
            .map(str::to_string)
            .collect()
    };
    c.call(&["CLIENT", "SETNAME", "who"]).await;
    // In the same batch as what turned it on, and that's in the log itself.
    c.send(&["CONFIG", "SET", "slowlog-log-slower-than", "0"])
        .await;
    c.send(&["SET", "k", "1"]).await;
    c.send(&["SLOWLOG", "LEN"]).await;
    assert_eq!(c.reply().await, "+OK\r\n");
    assert_eq!(c.reply().await, "+OK\r\n");
    assert_eq!(c.reply().await, ":2\r\n");
    c.call(&["SLOWLOG", "RESET"]).await;
    c.call(&["EVAL", "return redis.call('GET', KEYS[1])", "1", "k"])
        .await;
    let log = c.call(&["SLOWLOG", "GET", "2"]).await;
    assert_eq!(names(&log), ["EVAL", "GET"], "{log}");
    // The script's own has no one to name.
    assert!(log.contains("$3\r\nwho\r\n"), "{log}");
    assert!(log.contains("$0\r\n\r\n$0\r\n\r\n"), "{log}");
    c.call(&["SLOWLOG", "RESET"]).await;
    c.call(&["MULTI"]).await;
    c.call(&["SET", "k", "2"]).await;
    c.call(&["GET", "k"]).await;
    c.call(&["EXEC"]).await;
    let log = c.call(&["SLOWLOG", "GET", "4"]).await;
    assert_eq!(names(&log), ["GET", "SET", "MULTI"], "{log}");
    // What was refused never ran, and what blocked did.
    c.call(&["SLOWLOG", "RESET"]).await;
    c.call(&["CONFIG", "SET", "hz"]).await;
    c.call(&["CONFIG", "NOPE"]).await;
    c.call(&["XINFO", "NOPE"]).await;
    assert_eq!(c.call(&["SLOWLOG", "LEN"]).await, ":1\r\n");
    c.call(&["BLPOP", "nothing", "0.01"]).await;
    let log = c.call(&["SLOWLOG", "GET", "1"]).await;
    assert!(log.contains("BLPOP"), "{log}");
}

#[tokio::test]
async fn bad_settings_are_refused_as_redis_refuses_them() {
    let (addr, _ctx) = start(Mode::Shared).await;
    let mut c = Client::connect(addr).await;
    let failed = |name: &str, why: &str| {
        format!("-ERR CONFIG SET failed (possibly related to argument '{name}') - {why}\r\n")
    };
    for (name, value, why) in [
        (
            "hz",
            "-1",
            "argument must be between 0 and 2147483647 inclusive",
        ),
        ("hz", "nope", "argument couldn't be parsed into an integer"),
        (
            "maxmemory-samples",
            "0",
            "argument must be between 1 and 2147483647 inclusive",
        ),
        (
            "proto-max-bulk-len",
            "1",
            "argument must be between 1048576 and 9223372036854775807 inclusive",
        ),
        (
            "repl-backlog-size",
            "0",
            "argument must be between 1 and 9223372036854775807 inclusive",
        ),
        ("maxmemory", "-1", "argument must be a memory value"),
        (
            "maxmemory-clients",
            "101%",
            "percentage argument must be less or equal to 100",
        ),
        (
            "maxmemory-clients",
            "x",
            "argument must be a memory or percent value",
        ),
        ("dbfilename", "x", "can't set protected config"),
        ("dir", "/", "can't set protected config"),
        ("databases", "1", "can't set immutable config"),
        (
            "appendfsync",
            "x",
            "argument(s) must be one of the following: everysec, always, no",
        ),
        (
            "shutdown-on-sigint",
            "x",
            "argument(s) must be one of the following: default, save, nosave, now, force",
        ),
        ("protected-mode", "x", "argument must be 'yes' or 'no'"),
    ] {
        let reply = c.call(&["CONFIG", "SET", name, value]).await;
        assert_eq!(reply, failed(name, why), "{name} {value}");
    }
    for (name, value, kept) in [
        ("hz", "1000000", "500"),
        ("hz", "0", "1"),
        ("maxmemory-clients", "10%", "10%"),
        ("maxmemory-clients", "1mb", "1048576"),
        ("shutdown-on-sigint", "now force", "now force"),
        (
            "maxmemory",
            "99999999999999999999999",
            "18446744073709551615",
        ),
        ("maxmemory", "", "0"),
    ] {
        assert_eq!(
            c.call(&["CONFIG", "SET", name, value]).await,
            "+OK\r\n",
            "{name} {value}"
        );
        let got = c.call(&["CONFIG", "GET", name]).await;
        let want = format!(
            "*2\r\n${}\r\n{name}\r\n${}\r\n{kept}\r\n",
            name.len(),
            kept.len()
        );
        assert_eq!(got, want);
    }
}

#[tokio::test]
async fn latency_monitor_keeps_spikes() {
    let ctx = Arc::new(Context::new(
        Arc::new(Store::new(16)),
        Settings {
            debug_command: DebugCommand::Yes,
            ..Settings::default()
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(server::serve(listener, ctx));
    let mut c = Client::connect(addr).await;

    assert!(
        c.call(&["LATENCY", "DOCTOR"])
            .await
            .contains("Latency monitoring is disabled")
    );
    // Off: nothing's kept.
    c.call(&["DEBUG", "SLEEP", "0.03"]).await;
    assert_eq!(c.call(&["LATENCY", "LATEST"]).await, "*0\r\n");
    // In the same batch as what turned it on.
    c.send(&["CONFIG", "SET", "latency-monitor-threshold", "20"])
        .await;
    c.send(&["DEBUG", "SLEEP", "0.03"]).await;
    c.send(&["LATENCY", "LATEST"]).await;
    assert_eq!(c.reply().await, "+OK\r\n");
    assert_eq!(c.reply().await, "+OK\r\n");
    let latest = c.reply().await;
    assert!(
        latest.starts_with("*1\r\n*4\r\n$7\r\ncommand\r\n:"),
        "{latest}"
    );
    assert!(
        c.call(&["LATENCY", "GRAPH", "command"])
            .await
            .contains("command - high ")
    );
    assert!(
        c.call(&["LATENCY", "DOCTOR"])
            .await
            .contains("1. command: 1 latency spikes")
    );
    assert!(
        c.call(&["LATENCY", "HISTORY", "command"])
            .await
            .starts_with("*1\r\n*2\r\n:")
    );
    assert_eq!(
        c.call(&["LATENCY", "RESET", "command", "nope"]).await,
        ":1\r\n"
    );
    assert_eq!(c.call(&["LATENCY", "LATEST"]).await, "*0\r\n");
    assert_eq!(
        c.call(&["LATENCY", "GRAPH", "command"]).await,
        "-ERR No samples available for event 'command'\r\n"
    );
    assert_eq!(
        c.call(&["LATENCY", "HISTORY"]).await,
        "-ERR wrong number of arguments for 'latency|history' command\r\n"
    );
    assert_eq!(
        c.call(&["CONFIG", "SET", "latency-monitor-threshold", "-1"])
            .await,
        "-ERR CONFIG SET failed (possibly related to argument 'latency-monitor-threshold') - argument must be between 0 and 9223372036854775807 inclusive\r\n"
    );
}

#[tokio::test]
async fn trims_go_by_the_stream_node_settings() {
    let (addr, _ctx) = start(Mode::Shared).await;
    let mut c = Client::connect(addr).await;
    // What a Redis 7.2.4 does with each.
    for (entries, count, target, limit, trimmed, left) in [
        (1, 5, "2", None, 3, 2),
        (1, 5, "0", Some("1"), 1, 4),
        (1, 250, "10", None, 100, 150),
        (2, 5, "2", None, 2, 3),
        (2, 5, "0", Some("1"), 0, 5),
        (2, 250, "10", None, 200, 50),
        (3, 250, "10", None, 240, 10),
        (3, 250, "10", Some("50"), 48, 202),
        (5, 7, "3", None, 0, 7),
        (5, 10, "4", Some("0"), 5, 5),
        (100, 250, "10", None, 200, 50),
        (100, 250, "10", Some("50"), 0, 250),
    ] {
        c.call(&[
            "CONFIG",
            "SET",
            "stream-node-max-entries",
            &entries.to_string(),
        ])
        .await;
        c.call(&["DEL", "s"]).await;
        for _ in 0..count {
            c.call(&["XADD", "s", "*", "a", "b"]).await;
        }
        let mut args = vec!["XTRIM", "s", "MAXLEN", "~", target];
        if let Some(limit) = limit {
            args.extend(["LIMIT", limit]);
        }
        let what = format!("{entries} {count} {target} {limit:?}");
        assert_eq!(c.call(&args).await, format!(":{trimmed}\r\n"), "{what}");
        assert_eq!(
            c.call(&["XLEN", "s"]).await,
            format!(":{left}\r\n"),
            "{what}"
        );
    }
    c.call(&["CONFIG", "SET", "stream-node-max-entries", "100"])
        .await;
    c.call(&["CONFIG", "SET", "stream-node-max-bytes", "64"])
        .await;
    c.call(&["DEL", "s"]).await;
    for _ in 0..20 {
        c.call(&["XADD", "s", "*", "a", "1"]).await;
    }
    let trimmed = c.call(&["XTRIM", "s", "MAXLEN", "~", "1"]).await;
    assert_ne!(trimmed, ":0\r\n", "nodes of a few entries each");
}

#[tokio::test]
async fn memory_stats_has_what_redis_has() {
    let (addr, _ctx) = start(Mode::Shared).await;
    let mut c = Client::connect(addr).await;
    c.call(&["SET", "k", "v"]).await;
    let stats = c.call(&["MEMORY", "STATS"]).await;
    for name in [
        "peak.allocated",
        "total.allocated",
        "startup.allocated",
        "replication.backlog",
        "clients.slaves",
        "clients.normal",
        "cluster.links",
        "aof.buffer",
        "lua.caches",
        "functions.caches",
        "db.0",
        "overhead.hashtable.main",
        "overhead.total",
        "keys.count",
        "keys.bytes-per-key",
        "dataset.bytes",
        "dataset.percentage",
        "peak.percentage",
        "allocator.allocated",
        "allocator.active",
        "allocator.resident",
        "allocator-fragmentation.ratio",
        "allocator-fragmentation.bytes",
        "allocator-rss.ratio",
        "allocator-rss.bytes",
        "rss-overhead.ratio",
        "rss-overhead.bytes",
        "fragmentation",
        "fragmentation.bytes",
    ] {
        assert!(
            stats.contains(&format!("\r\n{name}\r\n")),
            "{name}: {stats}"
        );
    }
}

#[tokio::test]
async fn debug_when_it_is_allowed() {
    let ctx = Arc::new(Context::new(
        Arc::new(Store::new(16)),
        Settings {
            debug_command: DebugCommand::Local,
            ..Settings::default()
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(server::serve(listener, ctx));
    let mut c = Client::connect(addr).await;
    assert_eq!(c.call(&["DEBUG", "POPULATE", "100"]).await, "+OK\r\n");
    assert_eq!(c.call(&["DBSIZE"]).await, ":100\r\n");
    assert_eq!(c.call(&["GET", "key:7"]).await, "$7\r\nvalue:7\r\n");
    assert_eq!(
        c.call(&["DEBUG", "POPULATE", "2", "p", "12"]).await,
        "+OK\r\n"
    );
    assert_eq!(c.call(&["STRLEN", "p:1"]).await, ":12\r\n");
    // What's there stays.
    c.call(&["SET", "key:1", "mine"]).await;
    c.call(&["DEBUG", "POPULATE", "100"]).await;
    assert_eq!(c.call(&["GET", "key:1"]).await, "$4\r\nmine\r\n");
    assert!(
        c.call(&["DEBUG", "OBJECT", "key:1"])
            .await
            .starts_with("+Value at:")
    );
    assert_eq!(
        c.call(&["DEBUG", "OBJECT", "none"]).await,
        "-ERR no such key\r\n"
    );
    assert_eq!(c.call(&["DEBUG", "SLEEP", "0"]).await, "+OK\r\n");
    assert_eq!(
        c.call(&["DEBUG", "NOPE"]).await,
        "-ERR unknown subcommand or wrong number of arguments for 'NOPE'. Try DEBUG HELP.\r\n"
    );
}
