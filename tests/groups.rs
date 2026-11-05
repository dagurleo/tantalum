//! Stream consumer groups: replies recorded from Redis 7.2.4 and how blocked
//! `XREADGROUP`s, `WATCH` and times behave, checked against it with the same
//! clients.

mod common;

use std::time::Duration;

use common::{Client, MODES, start};
use tantalum::cmd;
use tantalum::context::Context;

/// `tests/data/groups-redis.txt`: every argument and key error in both
/// protocols, then a random run over several groups, consumers and chunks,
/// with `XDEL`, `XTRIM`, `XSETID`, `COPY` and `RENAME` among them, of what has
/// no time in its reply. Each line has the CRC-32 and length of Redis' reply
/// (`-`: any), then the arguments: `~cN` stands for N times `c`, and `\r`, `\n`
/// and `\0` for those bytes.
#[tokio::test]
async fn replies_match_redis() {
    let fixture = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/data/groups-redis.txt"
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
                _ => a
                    .replace("\\r", "\r")
                    .replace("\\n", "\n")
                    .replace("\\0", "\0")
                    .into_bytes(),
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
                "line {}: {}\n{}",
                n + 1,
                &line[..line.len().min(120)],
                String::from_utf8_lossy(&reply)
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

const WAIT: &[&str] = &[
    "XREADGROUP",
    "GROUP",
    "g",
    "c",
    "BLOCK",
    "0",
    "STREAMS",
    "s",
    ">",
];
const NO_GROUP: &str =
    "-NOGROUP No such key 's' or consumer group 'g' in XREADGROUP with GROUP option\r\n";
const ONE_ENTRY: &str =
    "*1\r\n*2\r\n$1\r\ns\r\n*1\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n";

/// As Redis 7.2.4 replied to each case (checked with the same clients).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_consumers_hear_as_in_redis() {
    for mode in MODES {
        let (addr, ctx) = start(mode).await;
        // `SET`'s fast path doesn't look for ready keys: this does, as it does
        // in a server (see `blocking.rs`).
        std::mem::forget(cmd::spawn_expirer(&ctx, Duration::from_millis(5)));
        let mut admin = Client::connect(addr).await;
        let mut a = Client::connect(addr).await;
        let mut b = Client::connect(addr).await;
        let setup = ["XGROUP", "CREATE", "s", "g", "$", "MKSTREAM"];
        admin.call(&setup).await;

        // A new entry goes to the one who waited longest, and is pending.
        a.send(WAIT).await;
        blocked(&ctx, 1).await;
        let other = [
            "XREADGROUP",
            "GROUP",
            "g",
            "d",
            "BLOCK",
            "0",
            "STREAMS",
            "s",
            ">",
        ];
        b.send(&other).await;
        blocked(&ctx, 2).await;
        admin.call(&["XADD", "s", "1-1", "f", "v"]).await;
        assert_eq!(a.reply().await, ONE_ENTRY, "{mode:?}");
        blocked(&ctx, 1).await;
        assert!(
            admin.call(&["XPENDING", "s", "g"]).await.contains("1-1"),
            "{mode:?}"
        );

        // What leaves the group where it was leaves the other waiting.
        admin.call(&["XACK", "s", "g", "1-1"]).await;
        admin.call(&["XSETID", "s", "9-9"]).await;
        admin
            .call(&["XGROUP", "CREATECONSUMER", "s", "g", "e"])
            .await;
        admin.call(&["XGROUP", "DELCONSUMER", "s", "g", "d"]).await;
        admin.call(&["XGROUP", "CREATE", "s", "other", "$"]).await;
        admin.call(&["XTRIM", "s", "MAXLEN", "0"]).await;
        blocked(&ctx, 1).await;
        assert!(!b.has_reply().await, "{mode:?}");

        // Its group going gets it an error, however that happens.
        admin.call(&["XGROUP", "DESTROY", "s", "g"]).await;
        assert_eq!(b.reply().await, NO_GROUP, "{mode:?}");
        blocked(&ctx, 0).await;

        for (gone, expected) in [
            (vec!["DEL", "s"], NO_GROUP),
            (vec!["UNLINK", "s"], NO_GROUP),
            (vec!["FLUSHALL"], NO_GROUP),
            (vec!["RENAME", "s", "t"], NO_GROUP),
            (vec!["RENAME", "t", "s"], NO_GROUP),
            (
                vec!["SET", "s", "text"],
                "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
            ),
        ] {
            admin.call(&["DEL", "s"]).await;
            admin.call(&["XADD", "t", "5-1", "f", "v"]).await;
            admin.call(&setup).await;
            a.send(WAIT).await;
            blocked(&ctx, 1).await;
            admin.call(&gone).await;
            assert_eq!(a.reply().await, expected, "{mode:?} {gone:?}");
            blocked(&ctx, 0).await;
            admin.call(&["DEL", "t"]).await;
        }

        // Made again with its group in one go, it just goes on waiting.
        admin.call(&["DEL", "s"]).await;
        admin.call(&setup).await;
        a.send(WAIT).await;
        blocked(&ctx, 1).await;
        admin.call(&["MULTI"]).await;
        admin.call(&["DEL", "s"]).await;
        admin
            .call(&["XGROUP", "CREATE", "s", "g", "0", "MKSTREAM"])
            .await;
        admin.call(&["EXEC"]).await;
        blocked(&ctx, 1).await;
        assert!(!a.has_reply().await, "{mode:?}");
        admin.call(&["XADD", "s", "1-1", "f", "v"]).await;
        assert_eq!(a.reply().await, ONE_ENTRY, "{mode:?}");
        blocked(&ctx, 0).await;

        // Timed out: nothing, but the consumer stays.
        a.send(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "late",
            "BLOCK",
            "20",
            "STREAMS",
            "s",
            ">",
        ])
        .await;
        assert_eq!(a.reply().await, "*-1\r\n", "{mode:?}");
        assert!(
            admin
                .call(&["XINFO", "CONSUMERS", "s", "g"])
                .await
                .contains("late"),
            "{mode:?}"
        );

        // In MULTI it can't wait.
        admin.call(&["MULTI"]).await;
        admin.call(WAIT).await;
        assert_eq!(admin.call(&["EXEC"]).await, "*1\r\n*-1\r\n", "{mode:?}");
    }
}

/// Nothing else runs: the thread that expires keys has to wake it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_blocked_consumer_hears_when_its_key_expires() {
    for mode in MODES {
        let (addr, ctx) = start(mode).await;
        std::mem::forget(cmd::spawn_expirer(&ctx, Duration::from_millis(5)));
        let mut admin = Client::connect(addr).await;
        let mut a = Client::connect(addr).await;
        admin
            .call(&["XGROUP", "CREATE", "s", "g", "$", "MKSTREAM"])
            .await;
        a.send(WAIT).await;
        blocked(&ctx, 1).await;
        admin.call(&["PEXPIRE", "s", "20"]).await;
        assert_eq!(a.reply().await, NO_GROUP, "{mode:?}");
        blocked(&ctx, 0).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resp3_blocked_consumers() {
    let (addr, ctx) = start(common::Mode::Shared).await;
    let mut admin = Client::connect(addr).await;
    let mut a = Client::connect(addr).await;
    a.call(&["HELLO", "3"]).await;
    admin
        .call(&["XGROUP", "CREATE", "s", "g", "$", "MKSTREAM"])
        .await;
    a.send(WAIT).await;
    blocked(&ctx, 1).await;
    admin.call(&["XADD", "s", "1-1", "f", "v"]).await;
    assert_eq!(
        a.reply().await,
        "%1\r\n$1\r\ns\r\n*1\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"
    );
    a.send(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "c",
        "BLOCK",
        "20",
        "STREAMS",
        "s",
        ">",
    ])
    .await;
    assert_eq!(a.reply().await, "_\r\n");
}

/// Whether `EXEC` runs after `command`, which comes from another client.
async fn exec_runs_after(addr: std::net::SocketAddr, command: &[&str]) -> bool {
    let mut a = Client::connect(addr).await;
    let mut b = Client::connect(addr).await;
    a.call(&["FLUSHALL"]).await;
    a.call(&["XADD", "s", "1-1", "f", "v"]).await;
    a.call(&["XADD", "s", "2-1", "f", "v"]).await;
    a.call(&["XGROUP", "CREATE", "s", "g", "0"]).await;
    a.call(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "c",
        "COUNT",
        "1",
        "STREAMS",
        "s",
        ">",
    ])
    .await;
    a.call(&["WATCH", "s"]).await;
    b.call(command).await;
    a.call(&["MULTI"]).await;
    a.call(&["PING"]).await;
    a.call(&["EXEC"]).await != "*-1\r\n"
}

/// As in Redis, what happens to a stream's groups isn't a change to it.
#[tokio::test]
async fn watch_sees_entries_not_groups() {
    let (addr, _ctx) = start(common::Mode::Shared).await;
    for changes in [
        &["XADD", "s", "3-1", "f", "v"][..],
        &["XDEL", "s", "1-1"],
        &["XTRIM", "s", "MAXLEN", "1"],
        &["DEL", "s"],
    ] {
        assert!(!exec_runs_after(addr, changes).await, "{changes:?}");
    }
    for unseen in [
        &["XACK", "s", "g", "1-1"][..],
        &["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"],
        &["XREADGROUP", "GROUP", "g", "new", "STREAMS", "s", "0"],
        &["XCLAIM", "s", "g", "z", "0", "1-1"],
        &["XAUTOCLAIM", "s", "g", "z", "0", "0"],
        &["XSETID", "s", "9-9"],
        &["XGROUP", "CREATE", "s", "g2", "0"],
        &["XGROUP", "DESTROY", "s", "g"],
        &["XGROUP", "SETID", "s", "g", "0"],
        &["XGROUP", "CREATECONSUMER", "s", "g", "n"],
        &["XGROUP", "DELCONSUMER", "s", "g", "c"],
    ] {
        assert!(exec_runs_after(addr, unseen).await, "{unseen:?}");
    }
}

/// The integers after `name` in a reply, in order.
fn after(reply: &str, name: &str) -> Vec<i64> {
    let needle = format!("${}\r\n{name}\r\n:", name.len());
    reply
        .match_indices(&needle)
        .map(|(at, _)| {
            let rest = &reply[at + needle.len()..];
            rest[..rest.find('\r').unwrap()].parse().unwrap()
        })
        .collect()
}

fn unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// What the recorded replies can't hold: the times.
#[tokio::test]
async fn times_are_as_redis_reports_them() {
    let (addr, _ctx) = start(common::Mode::Shared).await;
    let mut c = Client::connect(addr).await;
    c.call(&["XADD", "s", "1-1", "f", "v"]).await;
    c.call(&["XADD", "s", "2-1", "f", "v"]).await;
    c.call(&["XGROUP", "CREATE", "s", "g", "0"]).await;
    c.call(&["XGROUP", "CREATECONSUMER", "s", "g", "idle"])
        .await;

    let before = unix_ms();
    c.call(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"])
        .await;
    let row = c.call(&["XPENDING", "s", "g", "-", "+", "1"]).await;
    let idle: i64 = row.split("\r\n").nth(6).unwrap()[1..].parse().unwrap();
    assert!((0..1_000).contains(&idle), "{row}");
    assert!(row.ends_with(":1\r\n"), "delivered once: {row}");

    // Never active: -1.
    let info = c.call(&["XINFO", "CONSUMERS", "s", "g"]).await;
    let inactive = after(&info, "inactive");
    assert!((0..1_000).contains(&inactive[0]), "{info}");
    assert_eq!(inactive[1], -1, "{info}");
    assert!(after(&info, "idle").iter().all(|i| (0..1_000).contains(i)));

    // A delivery time set, in the past: the idle time and the filter go by it.
    let hour = "3600000";
    c.call(&["XCLAIM", "s", "g", "c", "0", "1-1", "IDLE", hour, "JUSTID"])
        .await;
    let old = c
        .call(&["XPENDING", "s", "g", "IDLE", hour, "-", "+", "10"])
        .await;
    assert!(old.starts_with("*1\r\n") && old.contains("1-1"), "{old}");
    let idle: i64 = old.split("\r\n").nth(6).unwrap()[1..].parse().unwrap();
    assert!((3_600_000..3_601_000).contains(&idle), "{old}");
    // Not idle long enough to take; claiming it makes it not idle any more.
    let taken = c
        .call(&["XAUTOCLAIM", "s", "g", "d", hour, "0", "JUSTID"])
        .await;
    assert!(taken.contains("1-1"), "{taken}");
    let nothing = c
        .call(&["XAUTOCLAIM", "s", "g", "e", hour, "0", "JUSTID"])
        .await;
    assert!(!nothing.contains("1-1"), "{nothing}");

    // A time from the future is taken as now, and one given stays.
    c.call(&[
        "XCLAIM", "s", "g", "c", "0", "2-1", "TIME", "1234", "JUSTID",
    ])
    .await;
    let full = c.call(&["XINFO", "STREAM", "s", "FULL"]).await;
    assert!(full.contains(":1234\r\n"), "{full}");
    let seen = after(&full, "seen-time");
    assert!(
        seen.iter().all(|&t| t >= before - 1_000 && t <= unix_ms()),
        "{full}"
    );
    let active = after(&full, "active-time");
    assert!(active.contains(&-1), "{full}");

    // XINFO's first and last entries, with those groups around.
    let brief = c.call(&["XINFO", "STREAM", "s"]).await;
    assert_eq!(after(&brief, "groups"), [1], "{brief}");
    assert_eq!(after(&brief, "radix-tree-keys"), [1], "{brief}");
    assert_eq!(after(&brief, "radix-tree-nodes"), [2], "{brief}");
}

/// Consumers that take turns, as a worker pool does: everything goes to one of
/// them once, and is gone when acknowledged. In every I/O mode, with many
/// threads' clients on one stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pool_of_consumers_gets_everything_once() {
    for mode in MODES {
        let (addr, ctx) = start(mode).await;
        let mut admin = Client::connect(addr).await;
        admin
            .call(&["XGROUP", "CREATE", "s", "g", "$", "MKSTREAM"])
            .await;
        let total = 2_000;
        let mut workers = Vec::new();
        for worker in 0..4 {
            let name = format!("worker{worker}");
            workers.push(tokio::spawn(async move {
                let mut c = Client::connect(addr).await;
                let mut seen = Vec::new();
                loop {
                    let reply = c
                        .call(&[
                            "XREADGROUP",
                            "GROUP",
                            "g",
                            &name,
                            "COUNT",
                            "7",
                            "BLOCK",
                            "100",
                            "STREAMS",
                            "s",
                            ">",
                        ])
                        .await;
                    if reply == "*-1\r\n" {
                        return seen;
                    }
                    let ids: Vec<String> = reply
                        .split("\r\n")
                        .filter(|part| part.contains('-') && !part.starts_with('*'))
                        .map(str::to_string)
                        .collect();
                    for id in ids {
                        let n = c.call(&["XACK", "s", "g", &id]).await;
                        assert_eq!(n, ":1\r\n", "acknowledged once: {id}");
                        seen.push(id);
                    }
                }
            }));
        }
        for i in 1..=total {
            admin
                .call(&["XADD", "s", &format!("{i}-0"), "f", "v"])
                .await;
        }
        let mut all = Vec::new();
        for worker in workers {
            all.extend(worker.await.unwrap());
        }
        all.sort_by_key(|id| id.trim_end_matches("-0").parse::<u64>().unwrap());
        let expected: Vec<String> = (1..=total).map(|i| format!("{i}-0")).collect();
        assert_eq!(all, expected, "{mode:?}");
        let summary = admin.call(&["XPENDING", "s", "g"]).await;
        assert!(summary.starts_with("*4\r\n:0\r\n"), "{mode:?} {summary}");
        blocked(&ctx, 0).await;
    }
}
