//! Keyspace notifications: Redis 7.2.4's own tests of them (unit/pubsub), and
//! the events of each kind of command, blocked clients, transactions and
//! scripts, as Redis sends them (checked against Redis 7.2.4 by
//! tools/notify-diff.py).

mod common;

use std::time::Duration;

use common::{Client, MODES, Mode, start_with};
use tantalum::context::{DebugCommand, Settings};

/// A server with background expiry, and a client subscribed to every event.
async fn server(mode: Mode, events: &str) -> (Client, Client) {
    let settings = Settings {
        debug_command: DebugCommand::Yes,
        ..Settings::default()
    };
    let (addr, ctx) = start_with(mode, settings).await;
    std::mem::forget(tantalum::cmd::spawn_expirer(
        &ctx,
        Duration::from_millis(100),
    ));
    let mut r = Client::connect(addr).await;
    assert_eq!(
        r.call(&["CONFIG", "SET", "notify-keyspace-events", events])
            .await,
        "+OK\r\n"
    );
    let mut sub = Client::connect(addr).await;
    assert_eq!(
        sub.call(&["PSUBSCRIBE", "*"]).await,
        "*3\r\n$10\r\npsubscribe\r\n$1\r\n*\r\n:1\r\n"
    );
    (r, sub)
}

/// The next message as `channel payload`.
async fn heard(sub: &mut Client) -> String {
    let reply = sub.reply().await;
    let parts: Vec<&str> = reply.split("\r\n").collect();
    // *4 $8 pmessage $1 * $n channel $n payload
    assert_eq!(parts[2], "pmessage", "{reply:?}");
    format!("{} {}", parts[6], parts[8])
}

/// Hears these messages, in order, and nothing more.
async fn hears(sub: &mut Client, expected: &[&str], what: &str) {
    for want in expected {
        assert_eq!(heard(sub).await, *want, "{what}");
    }
    assert!(!sub.has_reply().await, "{what}: more than {expected:?}");
}

/// Runs each command, checking it didn't fail.
async fn run(r: &mut Client, commands: &[&[&str]]) {
    for command in commands {
        let reply = r.call(command).await;
        assert!(!reply.starts_with('-'), "{command:?}: {reply}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_own_tests_pass() {
    for mode in MODES {
        let what = format!("{mode:?}");
        // We receive keyspace notifications, keyevent ones, or both.
        let (mut r, mut sub) = server(mode, "KA").await;
        run(&mut r, &[&["SET", "foo", "bar"]]).await;
        hears(&mut sub, &["__keyspace@0__:foo set"], &what).await;
        run(
            &mut r,
            &[
                &["CONFIG", "SET", "notify-keyspace-events", "EA"],
                &["SET", "foo", "bar"],
            ],
        )
        .await;
        hears(&mut sub, &["__keyevent@0__:set foo"], &what).await;
        run(
            &mut r,
            &[
                &["CONFIG", "SET", "notify-keyspace-events", "KEA"],
                &["SET", "foo", "bar"],
            ],
        )
        .await;
        hears(
            &mut sub,
            &["__keyspace@0__:foo set", "__keyevent@0__:set foo"],
            &what,
        )
        .await;
        // We are able to mask events.
        run(
            &mut r,
            &[
                &["CONFIG", "SET", "notify-keyspace-events", "KEl"],
                &["DEL", "mylist"],
                &["SET", "foo", "bar"],
                &["LPUSH", "mylist", "a"],
            ],
        )
        .await;
        hears(
            &mut sub,
            &["__keyspace@0__:mylist lpush", "__keyevent@0__:lpush mylist"],
            &what,
        )
        .await;
        // General events.
        run(
            &mut r,
            &[
                &["CONFIG", "SET", "notify-keyspace-events", "KEg"],
                &["SET", "foo", "bar"],
                &["EXPIRE", "foo", "1"],
                &["DEL", "foo"],
            ],
        )
        .await;
        hears(
            &mut sub,
            &[
                "__keyspace@0__:foo expire",
                "__keyevent@0__:expire foo",
                "__keyspace@0__:foo del",
                "__keyevent@0__:del foo",
            ],
            &what,
        )
        .await;
        // List events.
        run(
            &mut r,
            &[
                &["CONFIG", "SET", "notify-keyspace-events", "KEl"],
                &["DEL", "mylist"],
                &["LPUSH", "mylist", "a"],
                &["RPUSH", "mylist", "a"],
                &["RPOP", "mylist"],
            ],
        )
        .await;
        hears(
            &mut sub,
            &[
                "__keyspace@0__:mylist lpush",
                "__keyevent@0__:lpush mylist",
                "__keyspace@0__:mylist rpush",
                "__keyevent@0__:rpush mylist",
                "__keyspace@0__:mylist rpop",
                "__keyevent@0__:rpop mylist",
            ],
            &what,
        )
        .await;
        // Set events.
        run(
            &mut r,
            &[
                &["CONFIG", "SET", "notify-keyspace-events", "Ks"],
                &["DEL", "myset"],
                &["SADD", "myset", "a", "b", "c", "d"],
                &["SREM", "myset", "x"],
                &["SADD", "myset", "x", "y", "z"],
                &["SREM", "myset", "x"],
            ],
        )
        .await;
        hears(
            &mut sub,
            &[
                "__keyspace@0__:myset sadd",
                "__keyspace@0__:myset sadd",
                "__keyspace@0__:myset srem",
            ],
            &what,
        )
        .await;
        // Sorted set events.
        run(
            &mut r,
            &[
                &["CONFIG", "SET", "notify-keyspace-events", "Kz"],
                &["DEL", "myzset"],
                &["ZADD", "myzset", "1", "a", "2", "b"],
                &["ZREM", "myzset", "x"],
                &["ZADD", "myzset", "3", "x", "4", "y", "5", "z"],
                &["ZREM", "myzset", "x"],
            ],
        )
        .await;
        hears(
            &mut sub,
            &[
                "__keyspace@0__:myzset zadd",
                "__keyspace@0__:myzset zadd",
                "__keyspace@0__:myzset zrem",
            ],
            &what,
        )
        .await;
        // Hash events.
        run(
            &mut r,
            &[
                &["CONFIG", "SET", "notify-keyspace-events", "Kh"],
                &["DEL", "myhash"],
                &["HMSET", "myhash", "yes", "1", "no", "0"],
                &["HINCRBY", "myhash", "yes", "10"],
            ],
        )
        .await;
        hears(
            &mut sub,
            &[
                "__keyspace@0__:myhash hset",
                "__keyspace@0__:myhash hincrby",
            ],
            &what,
        )
        .await;
        // Stream events.
        run(
            &mut r,
            &[
                &["CONFIG", "SET", "notify-keyspace-events", "Kt"],
                &["DEL", "mystream"],
                &["XGROUP", "CREATE", "mystream", "mygroup", "$", "MKSTREAM"],
                &["XGROUP", "CREATECONSUMER", "mystream", "mygroup", "Bob"],
                &["XADD", "mystream", "1", "field1", "A"],
                &[
                    "XREADGROUP",
                    "GROUP",
                    "mygroup",
                    "Alice",
                    "STREAMS",
                    "mystream",
                    ">",
                ],
                &["XCLAIM", "mystream", "mygroup", "Mike", "0", "1-0", "FORCE"],
                // Not heard: Lee isn't there.
                &["XGROUP", "DELCONSUMER", "mystream", "mygroup", "Lee"],
                // Not heard: Bob is there.
                &["XAUTOCLAIM", "mystream", "mygroup", "Bob", "0", "1-0"],
                &["XGROUP", "DELCONSUMER", "mystream", "mygroup", "Bob"],
            ],
        )
        .await;
        hears(
            &mut sub,
            &[
                "__keyspace@0__:mystream xgroup-create",
                "__keyspace@0__:mystream xgroup-createconsumer",
                "__keyspace@0__:mystream xadd",
                "__keyspace@0__:mystream xgroup-createconsumer",
                "__keyspace@0__:mystream xgroup-createconsumer",
                "__keyspace@0__:mystream xgroup-delconsumer",
            ],
            &what,
        )
        .await;
        // Expired events, as the key's read and in the background.
        run(
            &mut r,
            &[
                &["CONFIG", "SET", "notify-keyspace-events", "Ex"],
                &["DEL", "foo"],
                &["PSETEX", "foo", "100", "1"],
            ],
        )
        .await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        r.call(&["EXISTS", "foo"]).await;
        hears(&mut sub, &["__keyevent@0__:expired foo"], &what).await;
        run(&mut r, &[&["PSETEX", "foo", "100", "1"]]).await;
        assert_eq!(
            heard(&mut sub).await,
            "__keyevent@0__:expired foo",
            "{what}"
        );
        // Evicted events. Redis' test sets maxmemory 1 with one key; here the
        // segment being written isn't evicted from, so the test fills one.
        run(
            &mut r,
            &[
                &["CONFIG", "SET", "notify-keyspace-events", "Ee"],
                &["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"],
                &["FLUSHDB"],
            ],
        )
        .await;
        let info = r.call(&["INFO", "memory"]).await;
        let used: usize = info
            .split("\r\n")
            .find_map(|l| l.strip_prefix("used_memory:"))
            .unwrap()
            .parse()
            .unwrap();
        let limit = (used + 300_000).to_string();
        run(&mut r, &[&["CONFIG", "SET", "maxmemory", &limit]]).await;
        let value = "v".repeat(1000);
        for i in 0..2000 {
            r.call(&["SET", &format!("key:{i}"), &value]).await;
        }
        let mut evicted = Vec::new();
        while sub.has_reply().await {
            evicted.push(heard(&mut sub).await);
        }
        assert!(evicted.len() > 100, "{what}: {} evicted", evicted.len());
        for event in &evicted {
            let key = event.strip_prefix("__keyevent@0__:evicted ").unwrap();
            assert_eq!(r.call(&["EXISTS", key]).await, ":0\r\n", "{what}: {key}");
        }
        run(
            &mut r,
            &[
                &["CONFIG", "SET", "maxmemory", "0"],
                &["CONFIG", "SET", "maxmemory-policy", "noeviction"],
            ],
        )
        .await;
        // CONFIG GET and SET of the flags.
        for (set, get) in [
            ("gKE", "gKE"),
            ("$lshzxeKE", "$lshzxeKE"),
            ("KA", "AK"),
            ("EA", "AE"),
        ] {
            r.call(&["CONFIG", "SET", "notify-keyspace-events", set])
                .await;
            assert_eq!(
                r.call(&["CONFIG", "GET", "notify-keyspace-events"]).await,
                format!(
                    "*2\r\n$22\r\nnotify-keyspace-events\r\n${}\r\n{get}\r\n",
                    get.len()
                ),
                "{what}"
            );
        }
        // New keys.
        run(
            &mut r,
            &[
                &["CONFIG", "SET", "notify-keyspace-events", "En"],
                &["SET", "foo", "bar"],
                &["SET", "foo", "baz"],
                &["SET", "bar", "bar"],
            ],
        )
        .await;
        hears(
            &mut sub,
            &["__keyevent@0__:new foo", "__keyevent@0__:new bar"],
            &what,
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn letters_redis_does_not_know_are_refused() {
    let (mut r, _sub) = server(Mode::PerThread, "").await;
    assert_eq!(
        r.call(&["CONFIG", "SET", "notify-keyspace-events", "KEw"])
            .await,
        "-ERR CONFIG SET failed (possibly related to argument 'notify-keyspace-events') - \
         Invalid event class character. Use 'Ag$lshzxeKEtmdn'.\r\n"
    );
    assert_eq!(
        r.call(&["CONFIG", "GET", "notify-keyspace-events"]).await,
        "*2\r\n$22\r\nnotify-keyspace-events\r\n$0\r\n\r\n"
    );
}

/// What (un)subscribing says is a push, sent even with replies off or
/// skipped, as in Redis 7.2 (unit/pubsub's first keyspace tests, and
/// "PubSub messages with CLIENT REPLY OFF").
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribing_is_heard_with_replies_off() {
    for mode in MODES {
        let what = format!("{mode:?}");
        let (mut r, _sub) = server(mode, "KA").await;
        let mut quiet = Client::connect(r.stream.get_ref().peer_addr().unwrap()).await;
        quiet.send(&["CLIENT", "REPLY", "OFF"]).await;
        quiet.send(&["PSUBSCRIBE", "*"]).await;
        assert_eq!(
            quiet.reply().await,
            "*3\r\n$10\r\npsubscribe\r\n$1\r\n*\r\n:1\r\n",
            "{what}"
        );
        run(&mut r, &[&["SET", "foo", "bar"]]).await;
        assert_eq!(heard(&mut quiet).await, "__keyspace@0__:foo set", "{what}");
        // RESP3, OFF and then SKIP: the pushes come, the replies don't.
        let mut three = Client::connect(r.stream.get_ref().peer_addr().unwrap()).await;
        three.call(&["HELLO", "3"]).await;
        three.send(&["CLIENT", "REPLY", "OFF"]).await;
        three.send(&["SUBSCRIBE", "channel"]).await;
        assert_eq!(
            three.reply().await,
            ">3\r\n$9\r\nsubscribe\r\n$7\r\nchannel\r\n:1\r\n",
            "{what}"
        );
        three.send(&["PING"]).await;
        three.send(&["CLIENT", "REPLY", "SKIP"]).await;
        three.send(&["UNSUBSCRIBE", "channel"]).await;
        assert_eq!(
            three.reply().await,
            ">3\r\n$11\r\nunsubscribe\r\n$7\r\nchannel\r\n:0\r\n",
            "{what}"
        );
        assert!(!three.has_reply().await, "{what}");
    }
}

/// Redis shows `KEAn` as `AKE` (`A` hides the `n`), but keeps it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_hidden_by_all_still_acts() {
    let (mut r, mut sub) = server(Mode::PerThread, "KEAn").await;
    assert_eq!(
        r.call(&["CONFIG", "GET", "notify-keyspace-events"]).await,
        "*2\r\n$22\r\nnotify-keyspace-events\r\n$3\r\nAKE\r\n"
    );
    run(&mut r, &[&["SET", "k", "v"]]).await;
    hears(
        &mut sub,
        &[
            "__keyspace@0__:k new",
            "__keyevent@0__:new k",
            "__keyspace@0__:k set",
            "__keyevent@0__:set k",
        ],
        "KEAn",
    )
    .await;
}

/// `keymiss` for the keys writes read, before their own events.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_that_read_hear_keymiss() {
    let (mut r, mut sub) = server(Mode::PerThread, "Egm").await;
    run(
        &mut r,
        &[
            &["SET", "d", "x"],
            &["SUNIONSTORE", "d", "nope"],
            &["GETDEL", "gone"],
        ],
    )
    .await;
    hears(
        &mut sub,
        &[
            "__keyevent@0__:keymiss nope",
            "__keyevent@0__:del d",
            "__keyevent@0__:keymiss gone",
        ],
        "Egm",
    )
    .await;
}

/// What each kind of command sends, with `KEA` (keyspace channels only shown).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commands_send_redis_events() {
    let cases: &[(&[&[&str]], &[&str])] = &[
        (&[&["SET", "k", "v", "EX", "100"]], &["k set", "k expire"]),
        (&[&["SET", "k", "v", "NX"]], &[]),
        (
            &[&["MSET", "a", "1", "b", "2", "a", "3"]],
            &["a set", "b set", "a set"],
        ),
        (
            &[
                &["INCR", "n"],
                &["INCRBYFLOAT", "n", "1.5"],
                &["APPEND", "n", "x"],
            ],
            &["n incrby", "n incrbyfloat", "n append"],
        ),
        (
            &[
                &["SETRANGE", "s", "0", ""],
                &["SETRANGE", "s", "2", "ab"],
                &["SETBIT", "s", "0", "1"],
            ],
            &["s setrange", "s setbit"],
        ),
        (
            &[
                &["GETEX", "k"],
                &["GETEX", "k", "EX", "10"],
                &["GETEX", "k", "PERSIST"],
                &["GETDEL", "k"],
            ],
            &["k expire", "k persist", "k del"],
        ),
        (
            &[
                &["EXPIRE", "a", "100"],
                &["PERSIST", "a"],
                &["EXPIRE", "a", "-1"],
            ],
            &["a expire", "a persist", "a del"],
        ),
        (
            &[
                &["RENAME", "b", "c"],
                &["COPY", "c", "d"],
                &["MOVE", "d", "1"],
            ],
            &["b rename_from", "c rename_to", "d copy_to", "d move_from"],
        ),
        (
            &[&["DEL", "b", "b", "nothing"], &["UNLINK", "n"]],
            &["b del", "n del"],
        ),
        (
            &[
                &["RPUSH", "l", "a", "b"],
                &["LMOVE", "l", "m", "LEFT", "RIGHT"],
                &["RPOPLPUSH", "l", "m"],
            ],
            &["l rpush", "m rpush", "l lpop", "m lpush", "l rpop", "l del"],
        ),
        (
            &[
                &["RPUSH", "m", "b", "a"],
                &["LINSERT", "m", "BEFORE", "zz", "x"],
                &["LSET", "m", "0", "y"],
                &["LTRIM", "m", "0", "10"],
                &["LREM", "m", "0", "y"],
                &["LTRIM", "m", "1", "0"],
            ],
            &["m rpush", "m lset", "m ltrim", "m lrem", "m ltrim", "m del"],
        ),
        (
            &[
                &["SADD", "s1", "a", "b"],
                &["SADD", "s1", "a"],
                &["SMOVE", "s1", "s2", "a"],
                &["SMOVE", "s1", "s2", "b"],
            ],
            &[
                "s1 sadd", "s1 srem", "s2 sadd", "s1 srem", "s1 del", "s2 sadd",
            ],
        ),
        (
            &[
                &["SADD", "s2", "a", "b"],
                &["SINTERSTORE", "s3", "s2", "nothing"],
                &["SUNIONSTORE", "s3", "s2"],
                &["SINTERSTORE", "s3", "nothing"],
            ],
            &["s2 sadd", "s3 sunionstore", "s3 del"],
        ),
        (
            &[&["SADD", "s2", "a", "b"], &["SPOP", "s2", "5"]],
            &["s2 sadd", "s2 spop", "s2 del"],
        ),
        (
            &[
                &["ZADD", "z", "1", "a", "2", "b"],
                &["ZADD", "z", "1", "a"],
                &["ZADD", "z", "INCR", "1", "a"],
                &["ZINCRBY", "z", "1", "b"],
                &["ZPOPMIN", "z"],
                &["ZREMRANGEBYRANK", "z", "0", "-1"],
            ],
            &[
                "z zadd",
                "z zincr",
                "z zincr",
                "z zpopmin",
                "z zremrangebyrank",
                "z del",
            ],
        ),
        (
            &[
                &["HSET", "h", "f", "v"],
                &["HSET", "h", "f", "v"],
                &["HSETNX", "h", "f", "w"],
                &["HINCRBYFLOAT", "h", "g", "1"],
                &["HDEL", "h", "f", "g"],
            ],
            &["h hset", "h hset", "h hincrbyfloat", "h hdel", "h del"],
        ),
        (
            &[
                &["XADD", "x", "1-0", "f", "v"],
                &["XADD", "x", "MAXLEN", "1", "2-0", "f", "v"],
                &["XADD", "x", "MAXLEN", "5", "3-0", "f", "v"],
                &["XDEL", "x", "2-0"],
                &["XTRIM", "x", "MAXLEN", "0"],
                &["XSETID", "x", "9-0"],
            ],
            &[
                "x xadd", "x xadd", "x xtrim", "x xadd", "x xdel", "x xtrim", "x xsetid",
            ],
        ),
        (
            &[
                &["PFADD", "p", "a"],
                &["PFADD", "p", "a"],
                &["PFMERGE", "p"],
            ],
            &["p pfadd", "p pfadd"],
        ),
        (
            &[
                &["GEOADD", "g", "1", "1", "a"],
                &[
                    "GEOSEARCHSTORE",
                    "g2",
                    "g",
                    "FROMLONLAT",
                    "1",
                    "1",
                    "BYRADIUS",
                    "10",
                    "km",
                ],
            ],
            &["g zadd", "g2 geosearchstore"],
        ),
        (
            &[
                &["RPUSH", "sorted", "2", "1"],
                &["SORT", "sorted", "STORE", "out"],
            ],
            &["sorted rpush", "out sortstore"],
        ),
    ];
    for (commands, expected) in cases {
        let (mut r, mut sub) = server(Mode::PerThread, "KA").await;
        run(
            &mut r,
            &[
                &["SET", "a", "1"],
                &["SET", "b", "2"],
                &["SET", "k", "v"],
                &["SET", "n", "1"],
            ],
        )
        .await;
        for _ in 0..4 {
            heard(&mut sub).await;
        }
        for command in *commands {
            r.call(command).await;
        }
        let expected: Vec<String> = expected
            .iter()
            .map(|e| {
                let (key, event) = e.split_once(' ').unwrap();
                format!("__keyspace@0__:{key} {event}")
            })
            .collect();
        let mut got = Vec::new();
        while sub.has_reply().await {
            got.push(heard(&mut sub).await);
        }
        let got: Vec<String> = got
            .into_iter()
            .filter(|g| g.starts_with("__keyspace@0__"))
            .collect();
        assert_eq!(got, expected, "{commands:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_clients_transactions_and_scripts_send_theirs() {
    for mode in MODES {
        let what = format!("{mode:?}");
        let (mut r, mut sub) = server(mode, "Kl").await;
        // A blocked client served: the push, then its pop.
        let mut blocked = Client::connect(r.stream.get_ref().peer_addr().unwrap()).await;
        blocked.send(&["BLPOP", "q", "0"]).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        run(&mut r, &[&["RPUSH", "q", "a"]]).await;
        assert_eq!(blocked.reply().await, "*2\r\n$1\r\nq\r\n$1\r\na\r\n");
        hears(
            &mut sub,
            &["__keyspace@0__:q rpush", "__keyspace@0__:q lpop"],
            &what,
        )
        .await;
        // In EXEC and in a script, each command's own.
        run(&mut r, &[&["MULTI"]]).await;
        assert_eq!(r.call(&["RPUSH", "q", "a", "b"]).await, "+QUEUED\r\n");
        assert_eq!(r.call(&["LPOP", "q", "2"]).await, "+QUEUED\r\n");
        run(&mut r, &[&["EXEC"]]).await;
        hears(
            &mut sub,
            &["__keyspace@0__:q rpush", "__keyspace@0__:q lpop"],
            &what,
        )
        .await;
        run(
            &mut r,
            &[&[
                "EVAL",
                "redis.call('rpush', KEYS[1], 'a'); redis.call('lpop', KEYS[1]); return 1",
                "1",
                "q",
            ]],
        )
        .await;
        hears(
            &mut sub,
            &["__keyspace@0__:q rpush", "__keyspace@0__:q lpop"],
            &what,
        )
        .await;
        // Off: nothing more.
        run(
            &mut r,
            &[
                &["CONFIG", "SET", "notify-keyspace-events", ""],
                &["RPUSH", "q", "a"],
            ],
        )
        .await;
        assert!(!sub.has_reply().await, "{what}");
    }
}
