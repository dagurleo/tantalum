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
    Str(String),
    Map(Vec<(&'static str, M)>),
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
            M::Str(s) => {
                let len = s.len();
                if len < 32 {
                    out.push(0xa0 | len as u8);
                } else {
                    out.push(0xd9);
                    out.push(len as u8);
                }
                out.extend_from_slice(s.as_bytes());
            }
            M::Map(entries) => {
                out.push(0x80 | entries.len() as u8);
                for (k, v) in entries {
                    M::Str(k.to_string()).pack(out);
                    v.pack(out);
                }
            }
            M::Arr(items) => {
                out.push(0x90 | items.len() as u8);
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
        let custom = if self.rng.chance(10) {
            format!("custom{}", self.rng.below(20))
        } else {
            String::new()
        };
        let args = M::Arr(vec![
            s(Q),
            s(&custom),
            s("job"),
            M::Int(twin.now as i64),
            M::Nil,
            M::Nil,
            M::Nil,
            M::Nil,
            M::Nil,
            M::Nil,
        ])
        .bytes();
        let mut opts = vec![("attempts", M::Int(1 + self.rng.below(3) as i64))];
        if self.rng.chance(20) {
            opts.push(("lifo", M::Bool(true)));
        }
        let opts = M::Map(opts).bytes();
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
