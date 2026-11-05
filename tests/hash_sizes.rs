//! Hashes of every size: replies recorded from Redis 7.2.4, and what a recording
//! can't hold, as Redis' order past its limits is its own.

mod common;

use common::{Client, MODES, start};

/// `tests/data/hashes-redis.txt`: each way across the limits, step by step, then a
/// random run over hashes that stay small, that stay packed in Redis while they're big
/// here, and that go past every limit (`tools/hash-diff.py`), of what's the same on any
/// run. Each line has the CRC-32 and length of Redis' reply (`-`: any), then the
/// arguments: `~cN` stands for N times `c`.
#[tokio::test]
async fn replies_match_redis() {
    let fixture = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/data/hashes-redis.txt"
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
                _ => a.as_bytes().to_vec(),
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
                String::from_utf8_lossy(&reply[..reply.len().min(300)])
            );
        }
    }
}

/// The bulk strings in a reply, in order.
fn bulks(reply: &str) -> Vec<String> {
    let mut lines = reply.split("\r\n");
    let mut out = Vec::new();
    while let Some(line) = lines.next() {
        if line.starts_with('$') {
            out.push(lines.next().unwrap().to_string());
        }
    }
    out
}

/// A big hash, in every I/O mode: one big by its count, one by its bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn big_hashes_work_as_small_ones_do() {
    for mode in MODES {
        let (addr, _ctx) = start(mode).await;
        let mut c = Client::connect(addr).await;
        for i in 0..2_000 {
            let (field, value) = (format!("f{i}"), format!("v{i}"));
            assert_eq!(c.call(&["HSET", "many", &field, &value]).await, ":1\r\n");
        }
        let long = "l".repeat(20_000);
        c.call(&["HSET", "long", "a", &long, "b", "2"]).await;
        assert_eq!(c.call(&["HLEN", "many"]).await, ":2000\r\n", "{mode:?}");
        assert_eq!(
            c.call(&["HGET", "many", "f1999"]).await,
            "$5\r\nv1999\r\n",
            "{mode:?}"
        );
        assert_eq!(c.call(&["HSTRLEN", "long", "a"]).await, ":20000\r\n");
        // Overwritten, shorter, longer, counted up.
        c.call(&["HSET", "many", "f0", "x"]).await;
        assert_eq!(c.call(&["HGET", "many", "f0"]).await, "$1\r\nx\r\n");
        let longer = "y".repeat(500);
        assert_eq!(c.call(&["HSET", "many", "f0", &longer]).await, ":0\r\n");
        assert_eq!(c.call(&["HSTRLEN", "many", "f0"]).await, ":500\r\n");
        assert_eq!(c.call(&["HINCRBY", "many", "n", "5"]).await, ":5\r\n");
        assert_eq!(c.call(&["HINCRBY", "many", "n", "-7"]).await, ":-2\r\n");
        assert_eq!(c.call(&["HSETNX", "many", "n", "9"]).await, ":0\r\n");
        // All there, once each, however they're asked for.
        let all = bulks(&c.call(&["HGETALL", "many"]).await);
        assert_eq!(all.len(), 2 * 2_001);
        let mut keys = bulks(&c.call(&["HKEYS", "many"]).await);
        keys.sort();
        keys.dedup();
        assert_eq!(keys.len(), 2_001);
        let mut scanned = Vec::new();
        let mut cursor = "0".to_string();
        loop {
            let reply = c.call(&["HSCAN", "many", &cursor, "COUNT", "100"]).await;
            let mut page = bulks(&reply);
            cursor = page.remove(0);
            scanned.extend(page.chunks(2).map(|pair| pair[0].clone()));
            if cursor == "0" {
                break;
            }
        }
        scanned.sort();
        scanned.dedup();
        assert_eq!(scanned, keys, "{mode:?}");
        // By chance, but from the hash and distinct.
        let some = bulks(&c.call(&["HRANDFIELD", "many", "50"]).await);
        assert_eq!(some.len(), 50);
        assert!(some.iter().all(|field| keys.contains(field)));
        let mut distinct = some.clone();
        distinct.sort();
        distinct.dedup();
        assert_eq!(distinct.len(), 50);
        // A TTL, carried over and kept, and a rename and a copy.
        c.call(&["HSET", "timed", "a", "1"]).await;
        c.call(&["EXPIRE", "timed", "1000"]).await;
        for i in 0..600 {
            c.call(&["HSET", "timed", &format!("f{i}"), "v"]).await;
        }
        let ttl = c.call(&["TTL", "timed"]).await;
        assert!(ttl.starts_with(":99") || ttl.starts_with(":100"), "{ttl}");
        c.call(&["RENAME", "many", "moved"]).await;
        c.call(&["COPY", "moved", "copied"]).await;
        c.call(&["HSET", "moved", "only-here", "1"]).await;
        assert_eq!(c.call(&["HLEN", "copied"]).await, ":2001\r\n");
        assert_eq!(c.call(&["HLEN", "moved"]).await, ":2002\r\n");
        // Emptied, it's gone.
        for i in 0..2_000 {
            c.call(&["HDEL", "copied", &format!("f{i}")]).await;
        }
        assert_eq!(c.call(&["HDEL", "copied", "n"]).await, ":1\r\n");
        assert_eq!(c.call(&["EXISTS", "copied"]).await, ":0\r\n");
        // Another kind's command on it.
        assert!(c.call(&["GET", "moved"]).await.starts_with("-WRONGTYPE"));
    }
}

/// One command setting many fields of a new hash, which took time in proportion to
/// their square. Well under a second here; that way, many minutes.
#[tokio::test]
async fn one_hset_of_many_fields() {
    const FIELDS: usize = 200_000;
    let names: Vec<String> = (0..FIELDS).map(|i| format!("f{i}")).collect();
    let mut args: Vec<&[u8]> = vec![b"HSET", b"h"];
    for name in &names {
        args.extend([name.as_bytes(), &b"v"[..]]);
    }
    let mut c = common::connect_buffered().await;
    c.send_raw(&common::req(&args)).await;
    let reply = tokio::time::timeout(std::time::Duration::from_secs(60), c.raw_reply())
        .await
        .expect("minutes, for what should take moments");
    assert_eq!(reply, format!(":{FIELDS}\r\n").into_bytes());
    c.send_raw(&common::req(&[b"HGET", b"h", b"f199999"])).await;
    assert_eq!(c.raw_reply().await, b"$1\r\nv\r\n");
}

/// What used to take time in proportion to the hash for each write. A few seconds in
/// a debug build; at O(n) a write, this many would take minutes.
#[tokio::test]
async fn writes_to_a_big_hash_dont_slow_down_as_it_grows() {
    let mut c = common::connect_buffered().await;
    let mut request = Vec::new();
    for batch in 0..100 {
        request.clear();
        for i in 0..1_000 {
            let field = format!("field:{batch}:{i}");
            request.extend_from_slice(&common::req(&[
                b"HSET",
                b"h",
                field.as_bytes(),
                b"some value",
            ]));
        }
        c.send_raw(&request).await;
        for _ in 0..1_000 {
            assert_eq!(c.raw_reply().await, b":1\r\n");
        }
    }
    c.send_raw(&common::req(&[b"HLEN", b"h"])).await;
    assert_eq!(c.raw_reply().await, b":100000\r\n");
}
