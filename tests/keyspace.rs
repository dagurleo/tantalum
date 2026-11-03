//! `SCAN`, `KEYS` and `RENAME`: replies recorded from a real Redis (7.0.11,
//! with `tools/redis-diff.py --rust`) where they're comparable (none or one
//! key, since key order and cursors are each server's own), and the
//! guarantees `SCAN` makes everywhere else.

mod common;

use std::collections::HashSet;
use std::sync::Arc;

use tantalum::context::{Context, Settings};
use tantalum::store::Store;

#[rustfmt::skip]
const SCRIPT: &[(&[&[u8]], &[u8])] = &[
    (&[b"KEYS", b"*"], b"*0\r\n"),
    (&[b"KEYS", b"a*"], b"*0\r\n"),
    (&[b"SCAN", b"0"], b"*2\r\n$1\r\n0\r\n*0\r\n"),
    (&[b"SCAN", b"0", b"COUNT", b"100"], b"*2\r\n$1\r\n0\r\n*0\r\n"),
    (&[b"SCAN", b"0", b"MATCH", b"*"], b"*2\r\n$1\r\n0\r\n*0\r\n"),
    (&[b"SCAN"], b"-ERR wrong number of arguments for 'scan' command\r\n"),
    (&[b"SCAN", b"abc"], b"-ERR invalid cursor\r\n"),
    (&[b"SCAN", b"-"], b"-ERR invalid cursor\r\n"),
    (&[b"SCAN", b" 1"], b"-ERR invalid cursor\r\n"),
    (&[b"SCAN", b"0", b"COUNT", b"0"], b"-ERR syntax error\r\n"),
    (&[b"SCAN", b"0", b"COUNT", b"-5"], b"-ERR syntax error\r\n"),
    (&[b"SCAN", b"0", b"COUNT", b"x"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"SCAN", b"0", b"COUNT"], b"-ERR syntax error\r\n"),
    (&[b"SCAN", b"0", b"MATCH"], b"-ERR syntax error\r\n"),
    (&[b"SCAN", b"0", b"TYPE"], b"-ERR syntax error\r\n"),
    (&[b"SCAN", b"0", b"BADOPT"], b"-ERR syntax error\r\n"),
    // Redis 7.0 says syntax error; 7.4, which added NOVALUES, says this.
    (&[b"SCAN", b"0", b"NOVALUES"], b"-ERR NOVALUES option can only be used in HSCAN\r\n"),
    (&[b"KEYS"], b"-ERR wrong number of arguments for 'keys' command\r\n"),
    (&[b"KEYS", b"a", b"b"], b"-ERR wrong number of arguments for 'keys' command\r\n"),
    (&[b"RENAME"], b"-ERR wrong number of arguments for 'rename' command\r\n"),
    (&[b"RENAME", b"a"], b"-ERR wrong number of arguments for 'rename' command\r\n"),
    (&[b"RENAME", b"a", b"b", b"c"], b"-ERR wrong number of arguments for 'rename' command\r\n"),
    (&[b"RENAMENX", b"a"], b"-ERR wrong number of arguments for 'renamenx' command\r\n"),
    (&[b"RENAME", b"nokey", b"other"], b"-ERR no such key\r\n"),
    (&[b"RENAMENX", b"nokey", b"other"], b"-ERR no such key\r\n"),
    (&[b"RENAME", b"nokey", b"nokey"], b"-ERR no such key\r\n"),
    (&[b"RENAMENX", b"nokey", b"nokey"], b"-ERR no such key\r\n"),
    (&[b"SET", b"only", b"1"], b"+OK\r\n"),
    (&[b"KEYS", b"*"], b"*1\r\n$4\r\nonly\r\n"),
    (&[b"KEYS", b"o*"], b"*1\r\n$4\r\nonly\r\n"),
    (&[b"KEYS", b"O*"], b"*0\r\n"),
    (&[b"KEYS", b"on?y"], b"*1\r\n$4\r\nonly\r\n"),
    (&[b"KEYS", b"[o]nly"], b"*1\r\n$4\r\nonly\r\n"),
    (&[b"KEYS", b"x*"], b"*0\r\n"),
    (&[b"KEYS", b""], b"*0\r\n"),
    (&[b"SCAN", b"0"], b"*2\r\n$1\r\n0\r\n*1\r\n$4\r\nonly\r\n"),
    (&[b"SCAN", b"0", b"COUNT", b"1000"], b"*2\r\n$1\r\n0\r\n*1\r\n$4\r\nonly\r\n"),
    (&[b"SCAN", b"0", b"MATCH", b"on*"], b"*2\r\n$1\r\n0\r\n*1\r\n$4\r\nonly\r\n"),
    (&[b"SCAN", b"0", b"MATCH", b"nope*"], b"*2\r\n$1\r\n0\r\n*0\r\n"),
    (&[b"SCAN", b"0", b"TYPE", b"string"], b"*2\r\n$1\r\n0\r\n*1\r\n$4\r\nonly\r\n"),
    (&[b"SCAN", b"0", b"TYPE", b"STRING"], b"*2\r\n$1\r\n0\r\n*1\r\n$4\r\nonly\r\n"),
    (&[b"SCAN", b"0", b"TYPE", b"hash"], b"*2\r\n$1\r\n0\r\n*0\r\n"),
    (&[b"SCAN", b"0", b"TYPE", b"list"], b"*2\r\n$1\r\n0\r\n*0\r\n"),
    (&[b"SCAN", b"0", b"TYPE", b"nonsense"], b"*2\r\n$1\r\n0\r\n*0\r\n"),
    (&[b"SCAN", b"0", b"MATCH", b"o*", b"TYPE", b"string", b"COUNT", b"5"], b"*2\r\n$1\r\n0\r\n*1\r\n$4\r\nonly\r\n"),
    (&[b"SCAN", b"0", b"COUNT", b"5", b"COUNT", b"7"], b"*2\r\n$1\r\n0\r\n*1\r\n$4\r\nonly\r\n"),
    (&[b"SCAN", b"", b"COUNT", b"100"], b"*2\r\n$1\r\n0\r\n*1\r\n$4\r\nonly\r\n"),
    (&[b"SET", b"a", b"hello"], b"+OK\r\n"),
    (&[b"RENAME", b"a", b"b"], b"+OK\r\n"),
    (&[b"GET", b"a"], b"$-1\r\n"),
    (&[b"GET", b"b"], b"$5\r\nhello\r\n"),
    (&[b"EXISTS", b"a"], b":0\r\n"),
    (&[b"RENAME", b"b", b"b"], b"+OK\r\n"),
    (&[b"GET", b"b"], b"$5\r\nhello\r\n"),
    (&[b"RENAMENX", b"b", b"b"], b":0\r\n"),
    (&[b"SET", b"c", b"other"], b"+OK\r\n"),
    (&[b"RENAMENX", b"b", b"c"], b":0\r\n"),
    (&[b"GET", b"b"], b"$5\r\nhello\r\n"),
    (&[b"GET", b"c"], b"$5\r\nother\r\n"),
    (&[b"RENAME", b"b", b"c"], b"+OK\r\n"),
    (&[b"GET", b"c"], b"$5\r\nhello\r\n"),
    (&[b"EXISTS", b"b"], b":0\r\n"),
    (&[b"SET", b"t", b"v", b"EX", b"100"], b"+OK\r\n"),
    (&[b"RENAME", b"t", b"t2"], b"+OK\r\n"),
    (&[b"TTL", b"t2"], b":100\r\n"),
    (&[b"TTL", b"t"], b":-2\r\n"),
    (&[b"PERSIST", b"t2"], b":1\r\n"),
    (&[b"RENAME", b"t2", b"t3"], b"+OK\r\n"),
    (&[b"TTL", b"t3"], b":-1\r\n"),
    (&[b"HSET", b"h", b"f", b"v"], b":1\r\n"),
    (&[b"RENAME", b"h", b"h2"], b"+OK\r\n"),
    (&[b"TYPE", b"h2"], b"+hash\r\n"),
    (&[b"HGETALL", b"h2"], b"*2\r\n$1\r\nf\r\n$1\r\nv\r\n"),
    (&[b"SET", b"s", b"str"], b"+OK\r\n"),
    (&[b"RENAME", b"s", b"h2"], b"+OK\r\n"),
    (&[b"TYPE", b"h2"], b"+string\r\n"),
    (&[b"GET", b"h2"], b"$3\r\nstr\r\n"),
    (&[b"HSET", b"h3", b"a", b"1"], b":1\r\n"),
    (&[b"RENAME", b"h2", b"h3"], b"+OK\r\n"),
    (&[b"TYPE", b"h3"], b"+string\r\n"),
    (&[b"GET", b"h3"], b"$3\r\nstr\r\n"),
    (&[b"RENAMENX", b"h3", b"fresh"], b":1\r\n"),
    (&[b"GET", b"fresh"], b"$3\r\nstr\r\n"),
    (&[b"EXISTS", b"h3"], b":0\r\n"),
    (&[b"SET", b"\x00\r\n", b"bin"], b"+OK\r\n"),
    (&[b"RENAME", b"\x00\r\n", b"\xff"], b"+OK\r\n"),
    (&[b"GET", b"\xff"], b"$3\r\nbin\r\n"),
    (&[b"DBSIZE"], b":5\r\n"),
    (&[b"FLUSHALL"], b"+OK\r\n"),
    (&[b"SET", b"k1", b"v"], b"+OK\r\n"),
    (&[b"KEYS", b"k1"], b"*1\r\n$2\r\nk1\r\n"),
    (&[b"SCAN", b"0", b"MATCH", b"k1"], b"*2\r\n$1\r\n0\r\n*1\r\n$2\r\nk1\r\n"),
];

#[tokio::test]
async fn replies_match_redis() {
    common::replay(SCRIPT).await;
}

/// Parses a `SCAN` reply into its cursor and keys.
fn scan_reply(reply: &[u8]) -> (u64, Vec<Vec<u8>>) {
    let mut lines = Lines(reply);
    assert_eq!(lines.line(), b"*2");
    let cursor = String::from_utf8(lines.bulk()).unwrap().parse().unwrap();
    let n: usize = std::str::from_utf8(&lines.line()[1..])
        .unwrap()
        .parse()
        .unwrap();
    let keys = (0..n).map(|_| lines.bulk()).collect();
    assert!(lines.0.is_empty(), "trailing bytes");
    (cursor, keys)
}

fn array_reply(reply: &[u8]) -> Vec<Vec<u8>> {
    let mut lines = Lines(reply);
    let n: usize = std::str::from_utf8(&lines.line()[1..])
        .unwrap()
        .parse()
        .unwrap();
    (0..n).map(|_| lines.bulk()).collect()
}

struct Lines<'a>(&'a [u8]);

impl Lines<'_> {
    fn line(&mut self) -> Vec<u8> {
        let end = self.0.windows(2).position(|w| w == b"\r\n").unwrap();
        let line = self.0[..end].to_vec();
        self.0 = &self.0[end + 2..];
        line
    }

    fn bulk(&mut self) -> Vec<u8> {
        let header = self.line();
        let len: usize = std::str::from_utf8(&header[1..]).unwrap().parse().unwrap();
        let bytes = self.0[..len].to_vec();
        self.0 = &self.0[len + 2..];
        bytes
    }
}

/// Whole scans through the command, with every option, agree with `KEYS`
/// and with what was written, and cursors stay small enough for clients
/// that read them as JavaScript numbers (node-redis 4).
#[test]
fn scans_and_keys_agree() {
    let ctx = Context::new(Arc::new(Store::new(16)), Settings::default());
    let client = ctx.connect(-1).unwrap();
    let mut session = client.session();
    let mut run = |args: &[&[u8]]| common::execute(&ctx, &mut session, 1, args);
    let mut strings = HashSet::new();
    let mut hashes = HashSet::new();
    for i in 0..20_000u32 {
        let key = format!("{}:{i}", ["user", "session", "x"][i as usize % 3]).into_bytes();
        if i % 5 == 0 {
            run(&[b"HSET", &key, b"f", b"v"]);
            hashes.insert(key);
        } else {
            let value = if i % 997 == 0 {
                vec![b'v'; 30_000]
            } else {
                b"v".to_vec()
            };
            run(&[b"SET", &key, &value]);
            strings.insert(key);
        }
    }
    let all: HashSet<_> = strings.union(&hashes).cloned().collect();
    let keys = array_reply(&run(&[b"KEYS", b"*"]));
    assert_eq!(keys.len(), all.len(), "KEYS returned a key twice");
    assert_eq!(keys.into_iter().collect::<HashSet<_>>(), all);
    let users: HashSet<_> = all
        .iter()
        .filter(|k| k.starts_with(b"user:"))
        .cloned()
        .collect();
    let keys = array_reply(&run(&[b"KEYS", b"user:*"]));
    assert_eq!(keys.into_iter().collect::<HashSet<_>>(), users);

    for (options, expected) in [
        (vec![], &all),
        (vec![&b"MATCH"[..], b"user:*"], &users),
        (vec![b"TYPE", b"hash"], &hashes),
        (vec![b"TYPE", b"STRING"], &strings),
    ] {
        for count in [&b"10"[..], b"777", b"100000"] {
            let mut seen = Vec::new();
            let mut cursor = 0u64;
            let mut calls = 0;
            loop {
                let cursor_arg = cursor.to_string();
                let mut args = vec![&b"SCAN"[..], cursor_arg.as_bytes(), b"COUNT", count];
                args.extend(options.iter().copied());
                let (next, keys) = scan_reply(&run(&args));
                seen.extend(keys);
                assert!(next < 1 << 53, "cursor {next}");
                calls += 1;
                cursor = next;
                if cursor == 0 {
                    break;
                }
            }
            let unique: HashSet<_> = seen.iter().cloned().collect();
            assert_eq!(unique.len(), seen.len(), "a key twice");
            assert_eq!(&unique, expected);
            // COUNT is roughly how many keys each call looks at.
            if count == b"10" {
                assert!(calls >= 20_000 / 10 / 2, "only {calls} calls");
            }
        }
    }
}
