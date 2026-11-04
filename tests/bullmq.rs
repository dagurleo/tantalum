//! BullMQ's scripts natively and in Lua give the same replies and leave the
//! store the same: a random queue workload (jobs added with BullMQ's own
//! `addStandardJob`, taken with `moveToActive`, completed or failed with
//! `moveToFinished`, with the queue paused, capped, rate limited, delayed
//! jobs coming due, priorities, parents, wrong locks and missing jobs)
//! replayed on two stores at the same times, one running the scripts
//! natively, compared reply by reply and key by key after every step. And
//! the native versions do run: most calls don't go back to Lua.

mod common;

use std::sync::Arc;

use tantalum::context::{Context, Session, Settings};
use tantalum::store::{Config, Store};

fn script(name: &str) -> Vec<u8> {
    std::fs::read(format!(
        "{}/tests/data/bullmq/{name}.lua",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap()
}

/// Minimal msgpack, as BullMQ's msgpackr writes options.
#[derive(Clone)]
enum M {
    Nil,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    /// Raw bytes, as a msgpack string (not UTF-8 necessarily).
    Bytes(Vec<u8>),
    Map(Vec<(&'static str, M)>),
    /// A map with keys of any kind.
    Obj(Vec<(M, M)>),
    Arr(Vec<M>),
}

impl M {
    fn pack(&self, out: &mut Vec<u8>) {
        match self {
            M::Nil => out.push(0xc0),
            M::Bool(b) => out.push(if *b { 0xc3 } else { 0xc2 }),
            M::Int(n) if (0..128).contains(n) => out.push(*n as u8),
            M::Int(n) if (-32..0).contains(n) => out.push(*n as i8 as u8),
            M::Int(n) if (0..65536).contains(n) => {
                out.push(0xcd);
                out.extend_from_slice(&(*n as u16).to_be_bytes());
            }
            M::Int(n) if *n >= 0 => {
                out.push(0xce);
                out.extend_from_slice(&(*n as u32).to_be_bytes());
            }
            M::Int(n) => {
                out.push(0xd3);
                out.extend_from_slice(&n.to_be_bytes());
            }
            M::Float(f) => {
                out.push(0xcb);
                out.extend_from_slice(&f.to_bits().to_be_bytes());
            }
            M::Str(s) => M::Bytes(s.as_bytes().to_vec()).pack(out),
            M::Bytes(s) => {
                let len = s.len();
                if len < 32 {
                    out.push(0xa0 | len as u8);
                } else if len < 256 {
                    out.push(0xd9);
                    out.push(len as u8);
                } else {
                    out.push(0xda);
                    out.extend_from_slice(&(len as u16).to_be_bytes());
                }
                out.extend_from_slice(s);
            }
            M::Map(entries) => {
                let entries = entries.iter().map(|(k, v)| (s(k), v.clone())).collect();
                M::Obj(entries).pack(out);
            }
            M::Obj(entries) => {
                if entries.len() < 16 {
                    out.push(0x80 | entries.len() as u8);
                } else {
                    out.push(0xde);
                    out.extend_from_slice(&(entries.len() as u16).to_be_bytes());
                }
                for (k, v) in entries {
                    k.pack(out);
                    v.pack(out);
                }
            }
            M::Arr(items) => {
                if items.len() < 16 {
                    out.push(0x90 | items.len() as u8);
                } else {
                    out.push(0xdc);
                    out.extend_from_slice(&(items.len() as u16).to_be_bytes());
                }
                items.iter().for_each(|i| i.pack(out));
            }
        }
    }

    fn bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.pack(&mut out);
        out
    }
}

fn s(x: &str) -> M {
    M::Str(x.to_string())
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n.max(1)
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

/// Option names BullMQ uses, and some short ones, so keys collide in Lua's
/// tables.
const OPTION_NAMES: &[&str] = &[
    "attempts",
    "delay",
    "priority",
    "lifo",
    "removeOnComplete",
    "removeOnFail",
    "jobId",
    "backoff",
    "repeat",
    "timestamp",
    "stackTraceLimit",
    "keepLogs",
    "sizeLimit",
    "fpof",
    "cpof",
    "idof",
    "rdof",
    "tm",
    "omc",
    "kl",
    "rjk",
    "telemetry",
    "prevMillis",
    "type",
    "count",
    "age",
    "a",
    "b",
    "c",
    "ab",
    "ba",
    "x1",
    "1x",
];

fn random_string(rng: &mut Rng, max: u64) -> Vec<u8> {
    // Letters, and what cjson escapes, and UTF-8.
    const BYTES: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCXYZ0123456789 _-:.\"/\\\n\t\x01\x7f";
    let mut out = Vec::new();
    for _ in 0..rng.below(max + 1) {
        if rng.chance(5) {
            out.extend_from_slice("é".as_bytes());
        } else {
            out.push(BYTES[rng.below(BYTES.len() as u64) as usize]);
        }
    }
    out
}

fn random_key(rng: &mut Rng) -> Vec<u8> {
    if rng.chance(60) {
        OPTION_NAMES[rng.below(OPTION_NAMES.len() as u64) as usize]
            .as_bytes()
            .to_vec()
    } else {
        random_string(rng, 10)
    }
}

fn random_value(rng: &mut Rng, depth: u32) -> M {
    match rng.below(if depth > 2 { 8 } else { 10 }) {
        0 => M::Nil,
        1 => M::Bool(rng.chance(50)),
        2 => M::Int(rng.below(100) as i64),
        3 => M::Int(match rng.below(4) {
            0 => -(rng.below(1000) as i64),
            1 => rng.below(1 << 40) as i64,
            2 => 1_780_000_000_000 + rng.below(1000) as i64,
            _ => 1 << 53,
        }),
        4 => M::Float(match rng.below(7) {
            0 => rng.below(1000) as f64 / 8.0,
            1 => 0.1 * rng.below(100) as f64,
            2 => 1e15 * (1 + rng.below(9)) as f64,
            3 => -(rng.below(100) as f64) / 3.0,
            4 => 1e-7 * rng.below(100) as f64,
            5 => 123_456_789.123_456_79,
            _ if rng.chance(10) => f64::NAN,
            _ => -0.0,
        }),
        5..=7 => M::Bytes(random_string(rng, 12)),
        8 => random_object(rng, depth + 1, 6),
        _ => M::Arr(
            (0..rng.below(6))
                .map(|_| {
                    if rng.chance(20) {
                        M::Nil
                    } else {
                        random_value(rng, depth + 1)
                    }
                })
                .collect(),
        ),
    }
}

/// A map of up to `max` entries: names that collide, repeated keys, nil
/// values, now and then a number for a key.
fn random_object(rng: &mut Rng, depth: u32, max: u64) -> M {
    let mut entries: Vec<(M, M)> = Vec::new();
    for _ in 0..rng.below(max + 1) {
        let key = if !entries.is_empty() && rng.chance(8) {
            entries[rng.below(entries.len() as u64) as usize].0.clone()
        } else if rng.chance(2) {
            M::Int(rng.below(5) as i64)
        } else {
            M::Bytes(random_key(rng))
        };
        entries.push((key, random_value(rng, depth)));
    }
    M::Obj(entries)
}

/// Two stores, one running BullMQ's scripts natively.
struct Twin {
    stores: [(&'static Context, Session); 2],
    now: u64,
}

impl Twin {
    fn new() -> Twin {
        let side = |native: bool| {
            let settings = Settings {
                native_scripts: native,
                ..Settings::default()
            };
            // The same Unix time on both, so XADD's IDs are the same.
            let store = Store::with_config(Config {
                shards: 16,
                unix_base: Some(0),
                ..Config::default()
            });
            let ctx: &'static Context =
                Box::leak(Box::new(Context::new(Arc::new(store), settings)));
            let session = Box::leak(Box::new(ctx.connect(-1).unwrap())).session();
            (ctx, session)
        };
        let mut twin = Twin {
            stores: [side(false), side(true)],
            now: 1_780_000_000_000,
        };
        for name in ["addStandardJob-9", "moveToActive-11", "moveToFinished-14"] {
            twin.run(&[b"SCRIPT", b"LOAD", &script(name)]);
        }
        twin
    }

    /// Runs a command on both, which must reply the same.
    fn run(&mut self, args: &[&[u8]]) -> Vec<u8> {
        let now = self.now;
        let [(a, sa), (b, sb)] = &mut self.stores;
        let lua = common::execute(a, sa, now, args);
        let native = common::execute(b, sb, now, args);
        assert_eq!(
            String::from_utf8_lossy(&native),
            String::from_utf8_lossy(&lua),
            "{:?}",
            args.iter()
                .map(|a| String::from_utf8_lossy(a))
                .collect::<Vec<_>>()
        );
        lua
    }

    /// Every key, its type, contents and TTL, as one transcript per store,
    /// which must be the same.
    fn compare(&mut self) {
        let now = self.now;
        let transcripts: Vec<String> = self
            .stores
            .iter_mut()
            .map(|(ctx, session)| {
                let mut run = |args: &[&[u8]]| common::execute(ctx, session, now, args);
                let keys = common::parse(&run(&[b"KEYS", b"*"])).bulks();
                let mut keys: Vec<Vec<u8>> = keys;
                keys.sort();
                let mut out = String::new();
                for key in keys {
                    let kind = run(&[b"TYPE", &key]);
                    let contents = match kind.as_slice() {
                        b"+string\r\n" => run(&[b"GET", &key]),
                        b"+hash\r\n" => run(&[b"HGETALL", &key]),
                        b"+list\r\n" => run(&[b"LRANGE", &key, b"0", b"-1"]),
                        b"+set\r\n" => {
                            let mut members = common::parse(&run(&[b"SMEMBERS", &key])).bulks();
                            members.sort();
                            format!("{members:?}").into_bytes()
                        }
                        b"+zset\r\n" => run(&[b"ZRANGE", &key, b"0", b"-1", b"WITHSCORES"]),
                        // Entries without their IDs, which come from the
                        // wall clock (XADD *), not the stores' time.
                        b"+stream\r\n" => match common::parse(&run(&[b"XRANGE", &key, b"-", b"+"]))
                        {
                            common::Value::Array(entries) => {
                                let fields: Vec<_> = entries
                                    .into_iter()
                                    .map(|e| match e {
                                        common::Value::Array(mut parts) => parts.pop(),
                                        _ => None,
                                    })
                                    .collect();
                                format!("{fields:?}").into_bytes()
                            }
                            other => format!("{other:?}").into_bytes(),
                        },
                        _ => b"?".to_vec(),
                    };
                    let ttl = run(&[b"PTTL", &key]);
                    out.push_str(&format!(
                        "{} {} {} {}\n",
                        String::from_utf8_lossy(&key),
                        String::from_utf8_lossy(&kind).trim(),
                        String::from_utf8_lossy(&contents),
                        String::from_utf8_lossy(&ttl).trim()
                    ));
                }
                out
            })
            .collect();
        if transcripts[0] != transcripts[1] {
            let (lua, native) = (&transcripts[0], &transcripts[1]);
            for (l, n) in lua.lines().zip(native.lines()) {
                assert_eq!(n, l, "stores differ");
            }
            assert_eq!(native.lines().count(), lua.lines().count(), "stores differ");
        }
    }

    fn native_runs(&self) -> (u64, u64) {
        self.stores[1].0.scripting_stats()
    }
}

const Q: &str = "bull:q:";

fn k(name: &str) -> Vec<u8> {
    format!("{Q}{name}").into_bytes()
}

fn sha(name: &str) -> Vec<u8> {
    let digest = sha1_hex(&script(name));
    digest.into_bytes()
}

fn sha1_hex(data: &[u8]) -> String {
    // The server's own SHA-1, through SCRIPT LOAD's reply.
    let ctx = Context::new(Arc::new(Store::new(1)), Settings::default());
    let mut session = ctx.connect(-1).unwrap().session();
    let reply = common::execute(&ctx, &mut session, 1, &[b"SCRIPT", b"LOAD", data]);
    String::from_utf8_lossy(&reply[5..45]).into_owned()
}

/// A worker's view: jobs it holds, with their lock tokens.
struct Workload {
    rng: Rng,
    held: Vec<(Vec<u8>, String)>,
    tokens: u64,
    add: Vec<u8>,
    to_active: Vec<u8>,
    to_finished: Vec<u8>,
    /// Options as a typical app sets them, rather than every kind.
    typical: bool,
}

impl Workload {
    fn add_job(&mut self, twin: &mut Twin) {
        // Custom ids, some repeated; untypically, ones whose keys are the
        // queue's own.
        let custom = match self.rng.below(20) {
            0 | 1 => format!("custom{}", self.rng.below(20)),
            2 if !self.typical => [
                "meta", "wait", "id", "events", "active", "marker", "7", "a/b\"c",
            ][self.rng.below(8) as usize]
                .to_string(),
            _ => String::new(),
        };
        let untypical = |rng: &mut Rng, percent: u64, value: M| {
            if !self.typical && rng.chance(percent) {
                value
            } else {
                M::Nil
            }
        };
        let name = if !self.typical && self.rng.chance(5) {
            M::Int(42)
        } else {
            s("job")
        };
        let parent = untypical(&mut self.rng, 3, s("bull:parent:1"));
        let dependencies = untypical(&mut self.rng, 5, s("bull:parent:1:dependencies"));
        let repeat_key = if self.rng.chance(5) {
            s("repeat:abc")
        } else {
            M::Nil
        };
        let args = M::Arr(vec![
            s(Q),
            s(&custom),
            name,
            M::Int(twin.now as i64),
            parent,
            dependencies,
            M::Nil,
            repeat_key,
            M::Nil,
            M::Nil,
        ])
        .bytes();
        let opts = if self.typical || self.rng.chance(50) {
            // As BullMQ's Queue.add sends them.
            let mut opts = vec![("attempts", M::Int(1 + self.rng.below(3) as i64))];
            if self.rng.chance(20) {
                opts.push(("lifo", M::Bool(true)));
            }
            if self.rng.chance(30) {
                opts.push((
                    "backoff",
                    M::Map(vec![("type", s("exponential")), ("delay", M::Int(1000))]),
                ));
            }
            match self.rng.below(3) {
                0 => opts.push(("removeOnComplete", M::Bool(true))),
                1 => opts.push(("removeOnComplete", M::Map(vec![("count", M::Int(100))]))),
                _ => {}
            }
            if self.rng.chance(20) {
                opts.push(("delay", M::Int(0)));
            }
            if !self.typical && self.rng.chance(5) {
                opts.push(("de", M::Map(vec![("id", s("dedup"))])));
            }
            M::Map(opts)
        } else {
            random_object(&mut self.rng, 0, 12)
        }
        .bytes();
        let keys: Vec<Vec<u8>> = [
            "wait",
            "paused",
            "meta",
            "id",
            "completed",
            "delayed",
            "active",
            "events",
            "marker",
        ]
        .iter()
        .map(|n| k(n))
        .collect();
        let mut call: Vec<&[u8]> = vec![b"EVALSHA", &self.add, b"9"];
        call.extend(keys.iter().map(Vec::as_slice));
        call.extend([&args[..], b"{\"x\":1}", &opts]);
        twin.run(&call);
    }

    fn worker_opts(&mut self, token: &str) -> M {
        let mut opts = vec![
            ("token", s(token)),
            ("lockDuration", M::Int(30_000)),
            ("name", if self.rng.chance(30) { s("w1") } else { M::Nil }),
        ];
        opts.push((
            "limiter",
            if !self.typical && self.rng.chance(5) {
                M::Map(vec![("max", M::Int(5)), ("duration", M::Int(1000))])
            } else {
                M::Nil
            },
        ));
        M::Map(opts)
    }

    fn token(&mut self) -> String {
        self.tokens += 1;
        if self.rng.chance(5) {
            "0".to_string()
        } else {
            format!("tok-{}", self.tokens)
        }
    }

    /// Notes the job a reply hands over, if any.
    fn took(&mut self, reply: &[u8], token: String) {
        let value = common::parse(reply);
        if let common::Value::Array(items) = value
            && let Some(common::Value::Bulk(id)) = items.get(1)
        {
            self.held.push((id.clone(), token));
        }
    }

    fn take(&mut self, twin: &mut Twin) {
        let token = self.token();
        let opts = self.worker_opts(&token).bytes();
        let keys: Vec<Vec<u8>> = [
            "wait",
            "active",
            "prioritized",
            "events",
            "stalled",
            "limiter",
            "delayed",
            "paused",
            "meta",
            "pc",
            "marker",
        ]
        .iter()
        .map(|n| k(n))
        .collect();
        let ts = twin.now.to_string();
        let mut call: Vec<&[u8]> = vec![b"EVALSHA", &self.to_active, b"11"];
        call.extend(keys.iter().map(Vec::as_slice));
        call.extend([Q.as_bytes(), ts.as_bytes(), &opts]);
        let reply = twin.run(&call);
        self.took(&reply, token);
    }

    fn finish(&mut self, twin: &mut Twin) {
        // Mostly a held job; sometimes one that isn't (missing, not active).
        let (id, mut token) = if !self.held.is_empty() && !self.rng.chance(5) {
            let i = self.rng.below(self.held.len() as u64) as usize;
            self.held.swap_remove(i)
        } else {
            (
                format!("{}", 1 + self.rng.below(40)).into_bytes(),
                "tok-x".to_string(),
            )
        };
        if self.rng.chance(4) {
            token = "tok-wrong".to_string();
        }
        let failed = self.rng.chance(25);
        let target = if failed { "failed" } else { "completed" };
        let count = match self.rng.below(if self.typical { 8 } else { 10 }) {
            0..=3 => M::Int(0),
            4..=7 => M::Int(-1),
            8 => M::Int(3),
            _ => M::Nil,
        };
        let mut keep = vec![("count", count)];
        if !self.typical && self.rng.chance(3) {
            keep.push(("age", M::Int(60)));
        }
        let opts = M::Map(vec![
            ("token", s(&token)),
            ("name", M::Nil),
            ("keepJobs", M::Map(keep)),
            ("limiter", M::Nil),
            ("lockDuration", M::Int(30_000)),
            (
                "attempts",
                if !self.typical && self.rng.chance(10) {
                    M::Nil
                } else {
                    M::Int(1 + self.rng.below(3) as i64)
                },
            ),
            (
                "maxMetricsSize",
                if !self.typical && self.rng.chance(4) {
                    M::Int(100)
                } else {
                    s("")
                },
            ),
            ("fpof", M::Bool(false)),
            ("cpof", M::Bool(false)),
            ("idof", M::Bool(false)),
            ("rdof", M::Bool(false)),
        ])
        .bytes();
        let fields = match self.rng.below(10) {
            0 => Some(M::Arr(vec![s("stacktrace"), s("[\"boom\"]")]).bytes()),
            1 => Some(M::Arr(vec![s("progress"), M::Int(50)]).bytes()),
            2 if !self.typical => Some(Vec::new()),
            _ => None,
        };
        let job_key = [Q.as_bytes(), &id].concat();
        let mut keys: Vec<Vec<u8>> = [
            "wait",
            "active",
            "prioritized",
            "events",
            "stalled",
            "limiter",
            "delayed",
            "paused",
            "meta",
            "pc",
        ]
        .iter()
        .map(|n| k(n))
        .collect();
        keys.push(k(target));
        keys.push(job_key);
        keys.push(k(&format!("metrics:{target}")));
        keys.push(k("marker"));
        let ts = twin.now.to_string();
        let fetch = if self.rng.chance(80) { "1" } else { "0" };
        let value = if failed { "boom" } else { "\"ok\"" };
        let field = if failed {
            "failedReason"
        } else {
            "returnvalue"
        };
        let mut call: Vec<&[u8]> = vec![b"EVALSHA", &self.to_finished, b"14"];
        call.extend(keys.iter().map(Vec::as_slice));
        call.extend([
            &id[..],
            ts.as_bytes(),
            field.as_bytes(),
            value.as_bytes(),
            target.as_bytes(),
            fetch.as_bytes(),
            Q.as_bytes(),
            &opts,
        ]);
        if let Some(fields) = &fields {
            call.push(fields);
        }
        let reply = twin.run(&call);
        // A job fetched next is locked with the finishing call's token.
        self.took(&reply, token);
    }
}

/// Nudges the queue's state toward the paths that go back to Lua, or to
/// errors, and back.
fn disturb(rng: &mut Rng, twin: &mut Twin, workload: &Workload) {
    let meta = k("meta");
    match rng.below(16) {
        0 => {
            twin.run(&[b"HSET", &meta, b"paused", b"1"]);
        }
        1 => {
            twin.run(&[b"HDEL", &meta, b"paused"]);
        }
        2 => {
            let n = (1 + rng.below(4)).to_string();
            twin.run(&[b"HSET", &meta, b"concurrency", n.as_bytes()]);
        }
        3 => {
            twin.run(&[b"HDEL", &meta, b"concurrency", b"max", b"duration"]);
        }
        4 => {
            twin.run(&[b"HSET", &meta, b"max", b"3", b"duration", b"1000"]);
        }
        5 => {
            // A delayed job, due now or later.
            let id = format!("d{}", rng.below(5));
            let at = twin.now as i64 + if rng.chance(50) { -10 } else { 60_000 };
            let score = (at * 4096).to_string();
            twin.run(&[
                b"HSET",
                &[Q.as_bytes(), id.as_bytes()].concat(),
                b"name",
                b"delayed",
                b"data",
                b"{}",
            ]);
            twin.run(&[b"ZADD", &k("delayed"), score.as_bytes(), id.as_bytes()]);
        }
        6 => {
            twin.run(&[b"DEL", &k("delayed")]);
        }
        7 => {
            let id = format!("p{}", rng.below(5));
            twin.run(&[
                b"HSET",
                &[Q.as_bytes(), id.as_bytes()].concat(),
                b"name",
                b"prio",
                b"priority",
                b"2",
            ]);
            twin.run(&[b"ZADD", &k("prioritized"), b"8589934593", id.as_bytes()]);
        }
        8 if rng.chance(30) => {
            // A legacy marker next in line.
            twin.run(&[b"RPUSH", &k("wait"), b"0:0"]);
        }
        9 => {
            if let Some((id, _)) = workload.held.first() {
                let job = [Q.as_bytes(), id].concat();
                twin.run(&[b"HSET", &job, b"parentKey", b"bull:parent:1"]);
            }
        }
        10 => {
            if let Some((id, _)) = workload.held.last() {
                let lock = [Q.as_bytes(), id, b":lock"].concat();
                if rng.chance(50) {
                    twin.run(&[b"DEL", &lock]);
                } else {
                    twin.run(&[b"SET", &lock, b"someone-else"]);
                }
            }
        }
        11 => {
            if let Some((id, _)) = workload.held.first() {
                let deps = [Q.as_bytes(), id, b":dependencies"].concat();
                twin.run(&[b"SADD", &deps, b"bull:child:1"]);
            }
        }
        12 => {
            twin.run(&[b"HSET", &meta, b"opts.maxLenEvents", b"20"]);
        }
        13 => {
            if let Some((id, _)) = workload.held.first() {
                twin.run(&[b"LREM", &k("active"), b"0", id]);
            }
        }
        _ => {}
    }
}

/// Runs a random workload on both, with `disturb` percent of steps
/// changing the queue's state; returns (native runs, calls sent to Lua).
fn workload(seed: u64, steps: usize, disturb_percent: u64, typical: bool) -> (u64, u64) {
    let mut twin = Twin::new();
    let mut workload = Workload {
        rng: Rng(seed),
        held: Vec::new(),
        tokens: 0,
        add: sha("addStandardJob-9"),
        to_active: sha("moveToActive-11"),
        to_finished: sha("moveToFinished-14"),
        typical,
    };
    let mut disturbance = Rng(seed.rotate_left(17) | 1);
    for step in 0..steps {
        if disturbance.chance(disturb_percent) {
            disturb(&mut disturbance, &mut twin, &workload);
        } else {
            match workload.rng.below(9) {
                0..=3 => workload.add_job(&mut twin),
                4..=5 => workload.take(&mut twin),
                _ => workload.finish(&mut twin),
            }
        }
        twin.now += workload.rng.below(50);
        if step % 10 == 0 {
            twin.compare();
        }
    }
    twin.compare();
    twin.native_runs()
}

/// Whether the native add leaves options to Lua: keys that aren't strings,
/// numbers cjson won't write, deduplication, a delay or priority that isn't
/// a string or number (the last value a key was given counts, as in Lua).
fn options_for_lua(opts: &M) -> bool {
    fn unencodable(m: &M) -> bool {
        match m {
            M::Float(f) => !f.is_finite(),
            // Any key's type counts, but only the values keys end up with:
            // cjson never sees one a later entry overwrote.
            M::Obj(entries) => {
                entries.iter().any(|(k, _)| !matches!(k, M::Bytes(_)))
                    || entries.iter().enumerate().any(|(i, (k, v))| {
                        let overwritten = entries[i + 1..].iter().any(
                            |(later, _)| matches!((k, later), (M::Bytes(a), M::Bytes(b)) if a == b),
                        );
                        !overwritten && unencodable(v)
                    })
            }
            M::Map(entries) => entries.iter().any(|(_, v)| unencodable(v)),
            M::Arr(items) => items.iter().any(unencodable),
            _ => false,
        }
    }
    let M::Obj(entries) = opts else {
        return true;
    };
    let get = |name: &str| {
        entries
            .iter()
            .rev()
            .find(|(k, _)| matches!(k, M::Bytes(b) if b == name.as_bytes()))
            .map_or(M::Nil, |(_, v)| v.clone())
    };
    let truthy = |m: &M| !matches!(m, M::Nil | M::Bool(false));
    let argument = |m: &M| !truthy(m) || matches!(m, M::Int(_) | M::Float(_) | M::Bytes(_));
    unencodable(opts)
        || truthy(&get("de"))
        || !argument(&get("delay"))
        || !argument(&get("priority"))
}

fn describe(m: &M) -> String {
    match m {
        M::Nil => "nil".into(),
        M::Bool(b) => b.to_string(),
        M::Int(n) => n.to_string(),
        M::Float(f) => format!("{f:?}"),
        M::Str(s) => format!("{s:?}"),
        M::Bytes(b) => format!("{:?}", String::from_utf8_lossy(b)),
        M::Map(e) => format!(
            "{{{}}}",
            e.iter()
                .map(|(k, v)| format!("{k:?}: {}", describe(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        M::Obj(e) => format!(
            "{{{}}}",
            e.iter()
                .map(|(k, v)| format!("{}: {}", describe(k), describe(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        M::Arr(i) => format!(
            "[{}]",
            i.iter().map(describe).collect::<Vec<_>>().join(", ")
        ),
    }
}

/// Job options are stored as Lua's cjson writes them, whatever their keys
/// (colliding, repeated, set to nil, tables growing), values and nesting.
#[test]
fn job_options_are_stored_as_lua_writes_them() {
    let mut twin = Twin::new();
    let mut rng = Rng(0x243F_6A88_85A3_08D3);
    let add = sha("addStandardJob-9");
    let keys: Vec<Vec<u8>> = [
        "wait",
        "paused",
        "meta",
        "id",
        "completed",
        "delayed",
        "active",
        "events",
        "marker",
    ]
    .iter()
    .map(|n| k(n))
    .collect();
    let args = M::Arr(vec![s(Q), s(""), s("job"), M::Int(twin.now as i64)]).bytes();
    for step in 0..4000 {
        let options = random_object(&mut rng, 0, 40);
        let opts = options.bytes();
        let mut call: Vec<&[u8]> = vec![b"EVALSHA", &add, b"9"];
        call.extend(keys.iter().map(Vec::as_slice));
        call.extend([&args[..], b"{}", &opts]);
        let (native_before, _) = twin.native_runs();
        let reply = twin.run(&call);
        let (native_after, _) = twin.native_runs();
        // Native exactly when it should be.
        assert_eq!(
            native_after - native_before,
            u64::from(!options_for_lua(&options)),
            "step {step}: {} for {}",
            String::from_utf8_lossy(&reply),
            describe(&options)
        );
        if let common::Value::Bulk(id) = common::parse(&reply) {
            twin.run(&[b"HGET", &[Q.as_bytes(), &id].concat(), b"opts"]);
        }
        if step % 200 == 0 {
            twin.compare();
        }
    }
    twin.compare();
    let (native, lua) = twin.native_runs();
    assert!(native > 1500, "native runs: {native}, back to Lua: {lua}");
}

#[test]
fn native_scripts_match_lua() {
    for seed in [
        0x2545_F491_4F6C_DD1D,
        0x9E37_79B9_7F4A_7C15,
        0xD1B5_4A32_D192_ED03,
    ] {
        let (native, lua) = workload(seed, 3_000, 12, false);
        assert!(native > 300, "native runs: {native}, back to Lua: {lua}");
        assert!(lua > 50, "the Lua paths ran too: {lua}");
    }
}

/// A queue as apps run it (default options, wrong tokens and missing jobs
/// included): every call runs natively.
#[test]
fn a_steady_queue_runs_natively() {
    let (native, lua) = workload(0x5851_F42D_4C95_7F2D, 3_000, 0, true);
    assert!(native > 1_000, "native runs: {native}");
    assert_eq!(lua, 0, "calls sent back to Lua");
}

/// One queue's keys, in addStandardJob's, moveToActive's or
/// moveToFinished's order.
fn queue_keys(queue: &str, names: &[&str]) -> Vec<Vec<u8>> {
    names
        .iter()
        .map(|n| format!("bull:{queue}:{n}").into_bytes())
        .collect()
}

/// Native scripts on many threads at once, holding only their shards,
/// with Lua scripts and transactions (which hold every shard) and plain
/// commands on the same keys meanwhile: nothing deadlocks, and every job
/// added is taken and finished exactly once.
fn threads_share_queues(config: Config, evicting: bool) {
    let ctx: &'static Context = Box::leak(Box::new(Context::new(
        Arc::new(Store::with_config(config)),
        Settings::default(),
    )));
    let mut session = ctx.connect(-1).unwrap().session();
    for name in ["addStandardJob-9", "moveToActive-11", "moveToFinished-14"] {
        common::execute(ctx, &mut session, 1, &[b"SCRIPT", b"LOAD", &script(name)]);
    }
    const THREADS: u64 = 8;
    const STEPS: u64 = 1500;
    let queues = ["t0", "t1", "t2"];
    let finished = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let added = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let watchdog = {
        let done = done.clone();
        std::thread::spawn(move || {
            for _ in 0..1200 {
                if done.load(std::sync::atomic::Ordering::Acquire) {
                    return;
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            eprintln!("threads still running after 120 s: a deadlock?");
            std::process::abort();
        })
    };
    let work = |t: u64, ctx: &'static Context| {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ ((t + 1) * 0x1000_0001));
        let mut session = ctx.connect(-1).unwrap().session();
        let (add, take, finish) = (
            sha("addStandardJob-9"),
            sha("moveToActive-11"),
            sha("moveToFinished-14"),
        );
        let mut held: Vec<(String, Vec<u8>)> = Vec::new();
        let run = |session: &mut Session, args: &[&[u8]]| {
            common::execute(ctx, session, ctx.store.now(), args)
        };
        let token = format!("tok-{t}");
        for step in 0..STEPS {
            let queue = queues[rng.below(queues.len() as u64) as usize];
            let prefix = format!("bull:{queue}:");
            let now = (1_780_000_000_000 + step).to_string();
            match rng.below(10) {
                0..=3 => {
                    let keys = queue_keys(
                        queue,
                        &[
                            "wait",
                            "paused",
                            "meta",
                            "id",
                            "completed",
                            "delayed",
                            "active",
                            "events",
                            "marker",
                        ],
                    );
                    let args = M::Arr(vec![s(&prefix), s(""), s("job"), M::Int(1)]).bytes();
                    let opts = M::Map(vec![("attempts", M::Int(1))]).bytes();
                    let mut call: Vec<&[u8]> = vec![b"EVALSHA", &add, b"9"];
                    call.extend(keys.iter().map(Vec::as_slice));
                    call.extend([&args[..], b"{}", &opts]);
                    let reply = run(&mut session, &call);
                    assert!(
                        reply.starts_with(b"$"),
                        "add: {}",
                        String::from_utf8_lossy(&reply)
                    );
                    added.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                4..=6 => {
                    let keys = queue_keys(
                        queue,
                        &[
                            "wait",
                            "active",
                            "prioritized",
                            "events",
                            "stalled",
                            "limiter",
                            "delayed",
                            "paused",
                            "meta",
                            "pc",
                            "marker",
                        ],
                    );
                    let opts = M::Map(vec![
                        ("token", s(&token)),
                        ("lockDuration", M::Int(30_000)),
                        ("name", M::Nil),
                        ("limiter", M::Nil),
                    ])
                    .bytes();
                    let mut call: Vec<&[u8]> = vec![b"EVALSHA", &take, b"11"];
                    call.extend(keys.iter().map(Vec::as_slice));
                    call.extend([prefix.as_bytes(), now.as_bytes(), &opts]);
                    let reply = run(&mut session, &call);
                    if let common::Value::Array(items) = common::parse(&reply)
                        && let Some(common::Value::Bulk(id)) = items.get(1)
                    {
                        held.push((queue.to_string(), id.clone()));
                    }
                }
                7 | 8 if !held.is_empty() => {
                    let (queue, id) = held.swap_remove(rng.below(held.len() as u64) as usize);
                    let prefix = format!("bull:{queue}:");
                    let mut keys = queue_keys(
                        &queue,
                        &[
                            "wait",
                            "active",
                            "prioritized",
                            "events",
                            "stalled",
                            "limiter",
                            "delayed",
                            "paused",
                            "meta",
                            "pc",
                            "completed",
                        ],
                    );
                    keys.push([prefix.as_bytes(), &id].concat());
                    keys.extend(queue_keys(&queue, &["metrics:completed", "marker"]));
                    let opts = M::Map(vec![
                        ("token", s(&token)),
                        ("keepJobs", M::Map(vec![("count", M::Int(0))])),
                        ("lockDuration", M::Int(30_000)),
                        ("attempts", M::Int(1)),
                        ("maxMetricsSize", s("")),
                        ("name", M::Nil),
                        ("limiter", M::Nil),
                    ])
                    .bytes();
                    // Fetching the next job too, now and then.
                    let fetch: &[u8] = if rng.chance(50) { b"1" } else { b"0" };
                    let mut call: Vec<&[u8]> = vec![b"EVALSHA", &finish, b"14"];
                    call.extend(keys.iter().map(Vec::as_slice));
                    call.extend([
                        &id[..],
                        now.as_bytes(),
                        b"returnvalue",
                        b"\"ok\"",
                        b"completed",
                        fetch,
                        prefix.as_bytes(),
                        &opts,
                    ]);
                    let reply = run(&mut session, &call);
                    match common::parse(&reply) {
                        common::Value::Int(0) => {}
                        common::Value::Array(items) => {
                            if let Some(common::Value::Bulk(next)) = items.get(1) {
                                held.push((queue.clone(), next.clone()));
                            }
                        }
                        // Evicted keys (the job's lock, say): BullMQ's codes
                        // for what's missing.
                        common::Value::Int(_) if evicting => {}
                        _ => panic!("finish: {}", String::from_utf8_lossy(&reply)),
                    }
                    finished.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                7 | 8 => {}
                _ => match rng.below(3) {
                    // A Lua script reading the queue: every shard.
                    0 => {
                        let wait = format!("bull:{queue}:wait");
                        let reply = run(
                            &mut session,
                            &[
                                b"EVAL",
                                b"return redis.call('LLEN', KEYS[1])",
                                b"1",
                                wait.as_bytes(),
                            ],
                        );
                        assert!(reply.starts_with(b":"));
                    }
                    // A transaction, every shard too.
                    1 => {
                        let meta = format!("bull:{queue}:meta");
                        run(&mut session, &[b"MULTI"]);
                        run(
                            &mut session,
                            &[b"HGET", meta.as_bytes(), b"opts.maxLenEvents"],
                        );
                        run(&mut session, &[b"INCR", format!("tx:{t}").as_bytes()]);
                        let reply = run(&mut session, &[b"EXEC"]);
                        assert!(
                            reply.starts_with(b"*2"),
                            "{}",
                            String::from_utf8_lossy(&reply)
                        );
                    }
                    // Plain commands on the queue's keys.
                    _ => {
                        let events = format!("bull:{queue}:events");
                        run(&mut session, &[b"XLEN", events.as_bytes()]);
                        let value = if evicting {
                            vec![b'v'; 4096]
                        } else {
                            b"v".to_vec()
                        };
                        let key = format!("k:{t}:{step}");
                        run(&mut session, &[b"SET", key.as_bytes(), &value]);
                    }
                },
            }
        }
        held
    };
    let mut left: Vec<(String, Vec<u8>)> = Vec::new();
    std::thread::scope(|scope| {
        let work = &work;
        let handles: Vec<_> = (0..THREADS)
            .map(|t| scope.spawn(move || work(t, ctx)))
            .collect();
        for h in handles {
            left.extend(h.join().expect("a thread panicked"));
        }
    });
    done.store(true, std::sync::atomic::Ordering::Release);
    watchdog.join().unwrap();
    let (native, lua) = ctx.scripting_stats();
    if evicting {
        // Any key can go, the queues' too: only that it evicted, and that
        // the native scripts ran meanwhile.
        let info = String::from_utf8(common::execute(ctx, &mut session, 1, &[b"INFO"])).unwrap();
        let evicted: u64 = info
            .lines()
            .find_map(|l| l.strip_prefix("evicted_keys:"))
            .and_then(|n| n.trim().parse().ok())
            .unwrap();
        assert!(evicted > 100, "evicted {evicted}");
        assert!(native > 1000, "native runs: {native}, back to Lua: {lua}");
        return;
    }
    // Jobs still held, and jobs still waiting: each exactly once.
    let mut ids = std::collections::HashSet::new();
    for (queue, id) in &left {
        assert!(
            ids.insert((queue.clone(), id.clone())),
            "{queue} {id:?} held twice"
        );
    }
    let mut waiting = 0;
    for queue in queues {
        for list in ["wait", "active"] {
            let key = format!("bull:{queue}:{list}");
            let items = common::parse(&common::execute(
                ctx,
                &mut session,
                1,
                &[b"LRANGE", key.as_bytes(), b"0", b"-1"],
            ))
            .bulks();
            for id in items {
                if list == "wait" {
                    waiting += 1;
                    assert!(
                        ids.insert((queue.to_string(), id.clone())),
                        "{queue} {id:?} waiting and held"
                    );
                }
            }
        }
    }
    let added = added.load(std::sync::atomic::Ordering::Relaxed);
    let finished = finished.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        finished + left.len() as u64 + waiting,
        added,
        "every job finished, held or waiting"
    );
    assert!(
        native >= added + finished,
        "native runs: {native}, back to Lua: {lua}"
    );
}

#[test]
fn threads_share_queues_holding_only_their_shards() {
    threads_share_queues(
        Config {
            shards: 64,
            ..Config::default()
        },
        false,
    );
}

/// The same, near a memory limit: writes evict, and a native script's
/// thread may only try the shards it doesn't hold.
#[test]
fn native_scripts_evict_without_waiting() {
    threads_share_queues(
        Config {
            shards: 64,
            maxmemory: 2 << 20,
            policy: tantalum::store::EvictionPolicy::AllKeysLru,
            ..Config::default()
        },
        true,
    );
}
