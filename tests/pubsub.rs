//! Pub/sub: replies recorded from Redis 7.2.4 on one connection (each
//! command's whole output, so pushes a command causes come with it), and
//! messages between connections in every I/O mode.

mod common;

use std::time::{Duration, Instant};

use common::{Client, MODES, start};
use tantalum::context::Context;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[rustfmt::skip]
const SCRIPT: &[(&[&[u8]], &[u8])] = &[
    (&[b"SUBSCRIBE", b"c1", b"c2"], b"*3\r\n$9\r\nsubscribe\r\n$2\r\nc1\r\n:1\r\n*3\r\n$9\r\nsubscribe\r\n$2\r\nc2\r\n:2\r\n"),
    (&[b"SUBSCRIBE", b"c1"], b"*3\r\n$9\r\nsubscribe\r\n$2\r\nc1\r\n:2\r\n"),
    (&[b"PSUBSCRIBE", b"c*"], b"*3\r\n$10\r\npsubscribe\r\n$2\r\nc*\r\n:3\r\n"),
    (&[b"SSUBSCRIBE", b"s1"], b"*3\r\n$10\r\nssubscribe\r\n$2\r\ns1\r\n:1\r\n"),
    (&[b"PING"], b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"),
    (&[b"PING", b"hi"], b"*2\r\n$4\r\npong\r\n$2\r\nhi\r\n"),
    (&[b"PING", b"a", b"b"], b"-ERR wrong number of arguments for 'ping' command\r\n"),
    (&[b"GET", b"k"], b"-ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n"),
    (&[b"CLIENT", b"SETNAME", b"x"], b"-ERR Can't execute 'client|setname': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n"),
    (&[b"CLIENT", b"FOO"], b"-ERR unknown subcommand 'FOO'. Try CLIENT HELP.\r\n"),
    (&[b"CONFIG", b"GET", b"maxmemory"], b"-ERR Can't execute 'config|get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n"),
    (&[b"COMMAND", b"COUNT"], b"-ERR Can't execute 'command|count': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n"),
    (&[b"NOSUCH"], b"-ERR unknown command 'NOSUCH', with args beginning with: \r\n"),
    (&[b"GET"], b"-ERR wrong number of arguments for 'get' command\r\n"),
    (&[b"PUBLISH", b"c1", b"m"], b"-ERR Can't execute 'publish': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n"),
    (&[b"HELLO", b"3"], b"-ERR Can't execute 'hello': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n"),
    (&[b"UNSUBSCRIBE", b"c2", b"zz"], b"*3\r\n$11\r\nunsubscribe\r\n$2\r\nc2\r\n:2\r\n*3\r\n$11\r\nunsubscribe\r\n$2\r\nzz\r\n:2\r\n"),
    (&[b"PUNSUBSCRIBE"], b"*3\r\n$12\r\npunsubscribe\r\n$2\r\nc*\r\n:1\r\n"),
    (&[b"PUNSUBSCRIBE"], b"*3\r\n$12\r\npunsubscribe\r\n$-1\r\n:1\r\n"),
    (&[b"SUNSUBSCRIBE"], b"*3\r\n$12\r\nsunsubscribe\r\n$2\r\ns1\r\n:0\r\n"),
    (&[b"UNSUBSCRIBE"], b"*3\r\n$11\r\nunsubscribe\r\n$2\r\nc1\r\n:0\r\n"),
    (&[b"UNSUBSCRIBE"], b"*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n"),
    (&[b"GET", b"k"], b"$-1\r\n"),
    (&[b"PUBLISH", b"c1", b"nobody"], b":0\r\n"),
    (&[b"SPUBLISH", b"s1", b"nobody"], b":0\r\n"),
    (&[b"PUBSUB", b"NUMPAT"], b":0\r\n"),
    (&[b"PUBSUB", b"NUMSUB", b"c1"], b"*2\r\n$2\r\nc1\r\n:0\r\n"),
    (&[b"PUBSUB", b"CHANNELS"], b"*0\r\n"),
    (&[b"PUBSUB", b"SHARDCHANNELS"], b"*0\r\n"),
    (&[b"PUBSUB", b"SHARDNUMSUB", b"s1"], b"*2\r\n$2\r\ns1\r\n:0\r\n"),
    (&[b"PUBSUB"], b"-ERR wrong number of arguments for 'pubsub' command\r\n"),
    (&[b"PUBSUB", b"foo"], b"-ERR unknown subcommand 'foo'. Try PUBSUB HELP.\r\n"),
    (&[b"PUBSUB", b"NUMPAT", b"x"], b"-ERR wrong number of arguments for 'pubsub|numpat' command\r\n"),
    (&[b"PUBSUB", b"CHANNELS", b"a", b"b"], b"-ERR unknown subcommand or wrong number of arguments for 'CHANNELS'. Try PUBSUB HELP.\r\n"),
    (&[b"pubsub", b"shardchannels", b"a", b"b"], b"-ERR unknown subcommand or wrong number of arguments for 'shardchannels'. Try PUBSUB HELP.\r\n"),
    (&[b"PUBSUB", b"HELP"], b"*14\r\n+PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:\r\n+CHANNELS [<pattern>]\r\n+    Return the currently active channels matching a <pattern> (default: '*').\r\n+NUMPAT\r\n+    Return number of subscriptions to patterns.\r\n+NUMSUB [<channel> ...]\r\n+    Return the number of subscribers for the specified channels, excluding\r\n+    pattern subscriptions(default: no channels).\r\n+SHARDCHANNELS [<pattern>]\r\n+    Return the currently active shard level channels matching a <pattern> (default: '*').\r\n+SHARDNUMSUB [<shardchannel> ...]\r\n+    Return the number of subscribers for the specified shard level channel(s)\r\n+HELP\r\n+    Print this help.\r\n"),
    (&[b"PUBSUB", b"HELP", b"x"], b"-ERR wrong number of arguments for 'pubsub|help' command\r\n"),
    (&[b"PUBLISH", b"a"], b"-ERR wrong number of arguments for 'publish' command\r\n"),
    (&[b"SUBSCRIBE"], b"-ERR wrong number of arguments for 'subscribe' command\r\n"),
    (&[b"UNSUBSCRIBE", b"a", b"b"], b"*3\r\n$11\r\nunsubscribe\r\n$1\r\na\r\n:0\r\n*3\r\n$11\r\nunsubscribe\r\n$1\r\nb\r\n:0\r\n"),
    (&[b"PUNSUBSCRIBE", b"a"], b"*3\r\n$12\r\npunsubscribe\r\n$1\r\na\r\n:0\r\n"),
    (&[b"HELLO", b"3"], b"%7\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.2.4\r\n$5\r\nproto\r\n:3\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n"),
    (&[b"SUBSCRIBE", b"c1"], b">3\r\n$9\r\nsubscribe\r\n$2\r\nc1\r\n:1\r\n"),
    (&[b"PUBLISH", b"c1", b"self"], b":1\r\n>3\r\n$7\r\nmessage\r\n$2\r\nc1\r\n$4\r\nself\r\n"),
    (&[b"PSUBSCRIBE", b"*"], b">3\r\n$10\r\npsubscribe\r\n$1\r\n*\r\n:2\r\n"),
    (&[b"PUBLISH", b"c1", b"both"], b":2\r\n>3\r\n$7\r\nmessage\r\n$2\r\nc1\r\n$4\r\nboth\r\n>4\r\n$8\r\npmessage\r\n$1\r\n*\r\n$2\r\nc1\r\n$4\r\nboth\r\n"),
    (&[b"PUBSUB", b"NUMSUB", b"c1", b"c2"], b"*4\r\n$2\r\nc1\r\n:1\r\n$2\r\nc2\r\n:0\r\n"),
    (&[b"PUBSUB", b"CHANNELS", b"c*"], b"*1\r\n$2\r\nc1\r\n"),
    (&[b"PUBSUB", b"CHANNELS", b"z*"], b"*0\r\n"),
    (&[b"PUBSUB", b"NUMPAT"], b":1\r\n"),
    (&[b"GET", b"k"], b"_\r\n"),
    (&[b"SET", b"k", b"v"], b"+OK\r\n"),
    (&[b"GET", b"k"], b"$1\r\nv\r\n"),
    (&[b"PING"], b"+PONG\r\n"),
    (&[b"PING", b"hi"], b"$2\r\nhi\r\n"),
    (&[b"SSUBSCRIBE", b"s1", b"s2"], b">3\r\n$10\r\nssubscribe\r\n$2\r\ns1\r\n:1\r\n>3\r\n$10\r\nssubscribe\r\n$2\r\ns2\r\n:2\r\n"),
    (&[b"SPUBLISH", b"s1", b"me"], b":1\r\n>3\r\n$8\r\nsmessage\r\n$2\r\ns1\r\n$2\r\nme\r\n"),
    (&[b"SUNSUBSCRIBE", b"s9"], b">3\r\n$12\r\nsunsubscribe\r\n$2\r\ns9\r\n:2\r\n"),
    (&[b"HELLO", b"2"], b"*14\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.2.4\r\n$5\r\nproto\r\n:2\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n"),
    (&[b"PUBLISH", b"c1", b"x"], b"-ERR Can't execute 'publish': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n"),
    (&[b"HELLO", b"3"], b"-ERR Can't execute 'hello': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n"),
    (&[b"RESET"], b"+RESET\r\n"),
    (&[b"GET", b"k"], b"$1\r\nv\r\n"),
    (&[b"PING"], b"+PONG\r\n"),
    (&[b"UNSUBSCRIBE"], b"*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n"),
    (&[b"PUNSUBSCRIBE"], b"*3\r\n$12\r\npunsubscribe\r\n$-1\r\n:0\r\n"),
    (&[b"SUNSUBSCRIBE"], b"*3\r\n$12\r\nsunsubscribe\r\n$-1\r\n:0\r\n"),
];

#[tokio::test]
async fn replies_match_redis() {
    common::replay(SCRIPT).await;
}

fn bulk(s: &str) -> String {
    format!("${}\r\n{s}\r\n", s.len())
}

fn message(channel: &str, payload: &str) -> String {
    format!("*3\r\n$7\r\nmessage\r\n{}{}", bulk(channel), bulk(payload))
}

/// Waits until `condition` holds.
async fn until(what: &str, condition: impl Fn() -> bool) {
    let start = Instant::now();
    while !condition() {
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "timed out waiting until {what}"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

fn channels(ctx: &Context) -> usize {
    ctx.pubsub.channel_count(false) + ctx.pubsub.channel_count(true) + ctx.pubsub.pattern_count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messages_reach_every_subscriber() {
    for mode in MODES {
        let (addr, _ctx) = start(mode).await;
        let mut a = Client::connect(addr).await;
        let mut b = Client::connect(addr).await;
        let mut s = Client::connect(addr).await;
        let mut publisher = Client::connect(addr).await;
        a.call(&["SUBSCRIBE", "news"]).await;
        b.call(&["PSUBSCRIBE", "n*"]).await;
        s.call(&["SSUBSCRIBE", "news"]).await;
        assert_eq!(
            publisher.call(&["PUBLISH", "news", "hi"]).await,
            ":2\r\n",
            "{mode:?}"
        );
        assert_eq!(a.reply().await, message("news", "hi"));
        assert_eq!(
            b.reply().await,
            format!(
                "*4\r\n$8\r\npmessage\r\n{}{}{}",
                bulk("n*"),
                bulk("news"),
                bulk("hi")
            )
        );
        // Shard channels are a namespace of their own.
        assert!(!s.has_reply().await);
        assert_eq!(publisher.call(&["SPUBLISH", "news", "s"]).await, ":1\r\n");
        assert_eq!(
            s.reply().await,
            format!("*3\r\n$8\r\nsmessage\r\n{}{}", bulk("news"), bulk("s"))
        );
        assert!(!a.has_reply().await && !b.has_reply().await);
        assert_eq!(publisher.call(&["PUBLISH", "other", "x"]).await, ":0\r\n");

        // Several publishers to many subscribers: every message reaches every
        // subscriber, each publisher's in order.
        const SUBSCRIBERS: usize = 8;
        const PUBLISHERS: usize = 3;
        const MESSAGES: usize = 300;
        let mut subscribers = Vec::new();
        for _ in 0..SUBSCRIBERS {
            let mut c = Client::connect(addr).await;
            c.call(&["SUBSCRIBE", "fan"]).await;
            subscribers.push(tokio::spawn(async move {
                let mut got = Vec::new();
                for _ in 0..PUBLISHERS * MESSAGES {
                    let reply = c.reply().await;
                    got.push(reply.rsplit("\r\n").nth(1).unwrap().to_owned());
                }
                got
            }));
        }
        let mut publishers = Vec::new();
        for p in 0..PUBLISHERS {
            publishers.push(tokio::spawn(async move {
                let mut c = Client::connect(addr).await;
                for n in 0..MESSAGES {
                    let reply = c.call(&["PUBLISH", "fan", &format!("{p}-{n}")]).await;
                    assert_eq!(reply, format!(":{SUBSCRIBERS}\r\n"));
                }
            }));
        }
        for p in publishers {
            p.await.unwrap();
        }
        for s in subscribers {
            let got = s.await.unwrap();
            for p in 0..PUBLISHERS {
                let theirs: Vec<usize> = got
                    .iter()
                    .filter_map(|m| m.strip_prefix(&format!("{p}-")))
                    .map(|n| n.parse().unwrap())
                    .collect();
                assert_eq!(theirs, (0..MESSAGES).collect::<Vec<_>>(), "{mode:?}");
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribers_come_and_go() {
    for mode in MODES {
        let (addr, ctx) = start(mode).await;
        let mut a = Client::connect(addr).await;
        let mut publisher = Client::connect(addr).await;
        a.send(&["SUBSCRIBE", "x", "y"]).await;
        a.reply().await;
        a.reply().await;
        a.send(&["PSUBSCRIBE", "p*"]).await;
        a.reply().await;
        assert_eq!(
            a.call(&["UNSUBSCRIBE", "x"]).await,
            "*3\r\n$11\r\nunsubscribe\r\n$1\r\nx\r\n:2\r\n"
        );
        assert_eq!(publisher.call(&["PUBLISH", "x", "gone"]).await, ":0\r\n");
        assert_eq!(publisher.call(&["PUBLISH", "y", "here"]).await, ":1\r\n");
        assert_eq!(a.reply().await, message("y", "here"));
        assert_eq!(channels(&ctx), 2, "{mode:?}");
        // A client that leaves is unsubscribed from everything.
        drop(a);
        until("the client's subscriptions are gone", || {
            channels(&ctx) == 0
        })
        .await;
        assert_eq!(
            publisher.call(&["PUBSUB", "NUMSUB", "y"]).await,
            "*2\r\n$1\r\ny\r\n:0\r\n"
        );
        assert!(
            publisher
                .call(&["INFO", "stats"])
                .await
                .contains("pubsub_channels:0\r\n")
        );
        // So is one that resets.
        let mut c = Client::connect(addr).await;
        c.call(&["SUBSCRIBE", "z"]).await;
        assert_eq!(c.call(&["RESET"]).await, "+RESET\r\n");
        assert_eq!(channels(&ctx), 0);
        assert_eq!(c.call(&["GET", "k"]).await, "$-1\r\n");
        // Shutdown closes subscribers.
        c.call(&["SUBSCRIBE", "z"]).await;
        ctx.shutdown();
        let mut rest = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), c.stream.read_to_end(&mut rest))
            .await
            .expect("still open")
            .unwrap();
        assert!(rest.is_empty());
        until("shutdown unsubscribed it", || channels(&ctx) == 0).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resp3_subscribers_run_commands_too() {
    for mode in MODES {
        let (addr, ctx) = start(mode).await;
        let mut c = Client::connect(addr).await;
        let mut publisher = Client::connect(addr).await;
        c.call(&["HELLO", "3"]).await;
        assert_eq!(
            c.call(&["SUBSCRIBE", "ch"]).await,
            ">3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:1\r\n"
        );
        assert_eq!(c.call(&["SET", "k", "v"]).await, "+OK\r\n");
        publisher.call(&["PUBLISH", "ch", "m1"]).await;
        let push = format!(">3\r\n$7\r\nmessage\r\n{}{}", bulk("ch"), bulk("m1"));
        assert_eq!(c.reply().await, push, "{mode:?}");
        assert_eq!(c.call(&["GET", "k"]).await, "$1\r\nv\r\n");
        // Messages still come while a command blocks.
        c.send(&["BLPOP", "q", "0"]).await;
        until("it blocks", || ctx.store.blocked_clients() == 1).await;
        publisher.call(&["PUBLISH", "ch", "m2"]).await;
        let push = format!(">3\r\n$7\r\nmessage\r\n{}{}", bulk("ch"), bulk("m2"));
        assert_eq!(c.reply().await, push, "{mode:?}");
        publisher.call(&["RPUSH", "q", "x"]).await;
        assert_eq!(c.reply().await, "*2\r\n$1\r\nq\r\n$1\r\nx\r\n");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscriber_that_doesnt_read_is_disconnected() {
    for mode in MODES {
        let (addr, ctx) = start(mode).await;
        let mut publisher = Client::connect(addr).await; // waits until it's up
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_recv_buffer_size(4096).unwrap();
        let mut slow = socket.connect(addr).await.unwrap();
        slow.write_all(&common::req(&[b"SUBSCRIBE", b"slow"]))
            .await
            .unwrap();
        until("it subscribes", || ctx.pubsub.channel_count(false) == 1).await;
        let big = "x".repeat(1 << 20);
        for _ in 0..40 {
            publisher.call(&["PUBLISH", "slow", &big]).await;
        }
        // Past 32 MB unread it's cut off, though it never read a byte.
        until("it's unsubscribed", || channels(&ctx) == 0).await;
        let mut rest = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), slow.read_to_end(&mut rest))
            .await
            .expect("still open")
            .ok();
        until("its buffers are given back", || {
            ctx.store.stats().client_bytes < 1 << 20
        })
        .await;
        assert_eq!(publisher.call(&["PING"]).await, "+PONG\r\n", "{mode:?}");
    }
}
