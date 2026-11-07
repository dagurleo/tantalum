//! The append-only log: a random workload on a store with the log on, then
//! a new store started from the log, must hold exactly the same data (every
//! key, type, value, stream ID and expiry time), across rewrites, evictions,
//! scripts, transactions and blocked commands served; and a log the server
//! stopped in the middle of writing loads what it had whole.

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tantalum::aof::{self, Fsync, Options};
use tantalum::cmd::{self, Args, Reply};
use tantalum::context::{Context, Session, Settings};
use tantalum::store::{Config, EvictionPolicy, Store};

/// A directory for one test, removed when the test ends. A test that fails
/// leaves it, to look at.
struct TempDir(PathBuf);

impl std::ops::Deref for TempDir {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

fn temp_dir(name: &str) -> TempDir {
    let dir = std::env::temp_dir().join(format!("tantalum-aof-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    TempDir(dir)
}

fn options(dir: &Path) -> Options {
    Options {
        dir: dir.to_path_buf(),
        fsync: Fsync::EverySec,
        rewrite_percentage: 100,
        // Rewrites only when a test asks.
        rewrite_min_size: u64::MAX,
    }
}

fn store(config: Config) -> Arc<Store> {
    Arc::new(Store::with_config(Config {
        unix_clock: true,
        shards: 16,
        ..config
    }))
}

/// A server's context with the log in `dir` on (loading what's there).
fn open(dir: &Path, config: Config) -> (&'static Context, Arc<aof::Aof>, aof::Started) {
    let ctx: &'static Context =
        Box::leak(Box::new(Context::new(store(config), Settings::default())));
    let (log, started) = aof::start(ctx, options(dir)).expect("the log loads");
    ctx.set_aof(log.clone());
    (ctx, log, started)
}

fn run(ctx: &Context, session: &mut Session, args: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut ranges = Vec::new();
    for arg in args {
        ranges.push(buf.len()..buf.len() + arg.len());
        buf.extend_from_slice(arg);
    }
    let mut out = Vec::new();
    match cmd::execute(
        ctx,
        session,
        ctx.store.now(),
        &Args::new(&buf, &ranges),
        &mut out,
    ) {
        Reply::Bulk(value) => tantalum::resp::write_bulk(&mut out, value.as_bytes()),
        Reply::Written | Reply::Close | Reply::Block(_) | Reply::Paused => {}
    }
    out
}

/// Everything in the store, as text: keys sorted, with type, contents and
/// absolute expiry. Keys that expire before `cutoff` (Unix ms) are left
/// out: they may be gone by the next dump.
fn dump(ctx: &Context, cutoff: i64) -> String {
    let mut session = ctx.connect(-1).unwrap().session();
    let mut keys = common::parse(&run(ctx, &mut session, &[b"KEYS", b"*"])).bulks();
    keys.sort();
    let mut out = String::new();
    for key in keys {
        let expires = run(ctx, &mut session, &[b"PEXPIRETIME", &key]);
        let at: i64 = String::from_utf8_lossy(&expires[1..expires.len() - 2])
            .parse()
            .unwrap();
        // -2: gone since KEYS.
        if at == -2 || (0..cutoff).contains(&at) {
            continue;
        }
        let kind = run(ctx, &mut session, &[b"TYPE", &key]);
        let contents = match kind.as_slice() {
            b"+string\r\n" => run(ctx, &mut session, &[b"GET", &key]),
            b"+hash\r\n" => {
                let mut pairs = common::parse(&run(ctx, &mut session, &[b"HGETALL", &key])).bulks();
                let mut fields: Vec<_> = pairs.chunks_mut(2).map(|p| p.to_vec()).collect();
                fields.sort();
                format!("{fields:?}").into_bytes()
            }
            b"+list\r\n" => run(ctx, &mut session, &[b"LRANGE", &key, b"0", b"-1"]),
            b"+set\r\n" => {
                let mut members =
                    common::parse(&run(ctx, &mut session, &[b"SMEMBERS", &key])).bulks();
                members.sort();
                format!("{members:?}").into_bytes()
            }
            b"+zset\r\n" => run(
                ctx,
                &mut session,
                &[b"ZRANGE", &key, b"0", b"-1", b"WITHSCORES"],
            ),
            b"+stream\r\n" => {
                let mut stream = run(ctx, &mut session, &[b"XRANGE", &key, b"-", b"+"]);
                let info = run(
                    ctx,
                    &mut session,
                    &[b"XINFO", b"STREAM", &key, b"FULL", b"COUNT", b"0"],
                );
                stream.extend_from_slice(&without_seen_times(&info));
                stream
            }
            other => other.to_vec(),
        };
        out.push_str(&format!(
            "{} {} {} {}\n",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&kind).trim(),
            String::from_utf8_lossy(&contents),
            String::from_utf8_lossy(&expires).trim()
        ));
    }
    out
}

/// `XINFO STREAM FULL`'s reply without the times consumers were last seen.
fn without_seen_times(info: &[u8]) -> Vec<u8> {
    const SEEN: &[u8] = b"$9\r\nseen-time\r\n:";
    let mut out = Vec::with_capacity(info.len());
    let mut rest = info;
    while let Some(at) = rest.windows(SEEN.len()).position(|w| w == SEEN) {
        out.extend_from_slice(&rest[..at]);
        rest = &rest[at + SEEN.len()..];
        let end = rest.iter().position(|&b| b == b'\r').unwrap();
        rest = &rest[end..];
    }
    out.extend_from_slice(rest);
    out
}

/// A cutoff for [`dump`]s compared with each other: a few seconds ahead.
fn cutoff() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    now.as_millis() as i64 + 5_000
}

/// Waits for the writer thread, and syncs what's left.
fn close(log: &aof::Aof) {
    log.stop();
    log.flush().unwrap();
    std::thread::sleep(Duration::from_millis(30));
}

fn assert_same(a: &str, b: &str) {
    if a == b {
        return;
    }
    // By key: what's missing, extra or different.
    let by_key = |text: &str| -> std::collections::BTreeMap<String, String> {
        let mut map = std::collections::BTreeMap::new();
        let mut current: Option<String> = None;
        for line in text.split('\n') {
            if let Some(key) = line
                .split(' ')
                .next()
                .filter(|k| !k.is_empty() && line.contains(" +"))
                && !line.starts_with(':')
                && !line.starts_with('$')
                && !line.starts_with('*')
            {
                current = Some(key.to_string());
                map.insert(key.to_string(), line.to_string());
            } else if let Some(key) = &current {
                let entry = map.get_mut(key).unwrap();
                entry.push('|');
                entry.push_str(line);
            }
        }
        map
    };
    let (a, b) = (by_key(a), by_key(b));
    // A key's line, cut short if long.
    let short = |line: &String| -> String {
        let cut: String = line.chars().take(300).collect();
        match cut.len() < line.len() {
            true => format!("{cut}... ({} bytes)", line.len()),
            false => cut,
        }
    };
    let mut report = String::new();
    for (key, line) in &a {
        match b.get(key) {
            None => report += &format!("missing after loading: {}\n", short(line)),
            Some(other) if other != line => {
                report += &format!("differs: {}\n   now: {}\n", short(line), short(other));
            }
            _ => {}
        }
    }
    for (key, line) in &b {
        if !a.contains_key(key) {
            report += &format!("extra after loading: {}\n", short(line));
        }
    }
    panic!("the store started from the log differs:\n{report}");
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n.max(1)
    }
}

/// A random write (or a few), on a small set of keys so they meet.
// Boxed as `Reply::Block` gives them.
#[allow(clippy::vec_box)]
fn step(
    ctx: &Context,
    session: &mut Session,
    rng: &mut Rng,
    blocked: &mut Vec<Box<tantalum::blocking::Blocked>>,
) {
    let k = |rng: &mut Rng, kind: &str| format!("{kind}{}", rng.below(6));
    let n = |rng: &mut Rng, max: u64| rng.below(max).to_string();
    let s = k(rng, "s");
    let s2 = k(rng, "s");
    let h = k(rng, "h");
    let l = k(rng, "l");
    let l2 = k(rng, "l");
    let st = k(rng, "set");
    let st2 = k(rng, "set");
    let z = k(rng, "z");
    let x = k(rng, "x");
    let v = n(rng, 1000);
    let ms = (1 + rng.below(60)).to_string();
    let at = (ctx.store.now() + rng.below(80)).to_string();
    let g = format!("g{}", rng.below(2));
    let who = format!("c{}", rng.below(3));
    let group_read = |rest: &[&str]| -> Vec<Vec<String>> {
        let mut command: Vec<String> = ["XREADGROUP", "GROUP", &g, &who]
            .iter()
            .map(|a| a.to_string())
            .collect();
        command.extend(rest.iter().map(|a| a.to_string()));
        vec![command]
    };
    let words = |parts: &[&str]| -> Vec<Vec<String>> {
        vec![parts.iter().map(|a| a.to_string()).collect()]
    };
    let commands: Vec<Vec<String>> = match rng.below(71) {
        // Past a packed hash's limits by bytes, then changed and shrunk in place.
        69 => words(&[
            "HSET",
            &h,
            &format!("long{}", rng.below(8)),
            &"l".repeat(3_000),
        ]),
        70 => words(&["HDEL", &h, &format!("long{}", rng.below(8))]),
        // A consumer blocked on a stream, served by the entry that follows
        // (run again by the writer, and logged then); or still waiting when it
        // gives up, which leaves only the consumer it made.
        67 | 68 => {
            let served = rng.below(2) == 0;
            // Nobody else's, or a later command would make it again anyway.
            let who = format!("once{}", rng.below(1_000_000));
            let args: Vec<&[u8]> = vec![
                b"XREADGROUP",
                b"GROUP",
                g.as_bytes(),
                who.as_bytes(),
                b"BLOCK",
                b"0",
                b"STREAMS",
                x.as_bytes(),
                b">",
            ];
            let mut buf = Vec::new();
            let mut ranges = Vec::new();
            for arg in args {
                ranges.push(buf.len()..buf.len() + arg.len());
                buf.extend_from_slice(arg);
            }
            let mut waiter = ctx.connect(-1).unwrap().session();
            let mut out = Vec::new();
            if let Reply::Block(b) = cmd::execute(
                ctx,
                &mut waiter,
                ctx.store.now(),
                &Args::new(&buf, &ranges),
                &mut out,
            ) {
                blocked.push(b);
            }
            if served {
                words(&["XADD", &x, "*", "f", &v])
            } else {
                Vec::new()
            }
        }
        53 => words(&["XGROUP", "CREATE", &x, &g, "0", "MKSTREAM"]),
        54 => words(&["XGROUP", "SETID", &x, &g, "$"]),
        55 if rng.below(10) == 0 => words(&["XGROUP", "DESTROY", &x, &g]),
        56 => words(&["XGROUP", "CREATECONSUMER", &x, &g, &who]),
        57 if rng.below(10) == 0 => words(&["XGROUP", "DELCONSUMER", &x, &g, &who]),
        58 | 59 => group_read(&["COUNT", "2", "STREAMS", &x, ">"]),
        60 => group_read(&["NOACK", "COUNT", "1", "STREAMS", &x, ">"]),
        61 => group_read(&["STREAMS", &x, "0"]),
        // Nothing to read, most times: a consumer it makes must last anyway.
        62 => {
            let (a, b) = (k(rng, "x"), k(rng, "x"));
            let mut read = group_read(&["STREAMS", &a, &b, ">", ">"]);
            // Nobody else's, or a later command would make it again anyway.
            read[0][3] = format!("once{}", rng.below(1_000_000));
            read
        }
        63 => {
            let id = pending_id(ctx, session, &x, &g);
            words(&["XACK", &x, &g, &id])
        }
        64 => {
            let id = pending_id(ctx, session, &x, &g);
            words(&[
                "XCLAIM",
                &x,
                &g,
                &who,
                "0",
                &id,
                "IDLE",
                "5000",
                "RETRYCOUNT",
                "3",
            ])
        }
        65 => words(&["XAUTOCLAIM", &x, &g, &who, "0", "0", "COUNT", "2"]),
        66 => words(&["XSETID", &x, "99999999999999-0", "ENTRIESADDED", "5000"]),
        0 => vec![vec!["SET".into(), s, v]],
        1 => vec![vec!["SET".into(), s, v, "PX".into(), ms]],
        2 => vec![vec!["SET".into(), s, v, "PXAT".into(), at]],
        3 => vec![vec!["SET".into(), s, v, "NX".into(), "GET".into()]],
        4 => vec![vec!["SET".into(), s, v, "XX".into(), "KEEPTTL".into()]],
        5 => vec![vec!["INCR".into(), s]],
        6 => vec![vec!["APPEND".into(), s, v]],
        7 => vec![vec!["GETDEL".into(), s]],
        8 => vec![vec!["GETEX".into(), s, "PX".into(), ms]],
        9 => vec![vec!["EXPIRE".into(), s, "1".into()]],
        10 => vec![vec!["PEXPIRE".into(), k(rng, "l"), ms]],
        11 => vec![vec!["PERSIST".into(), s]],
        12 => vec![vec!["RENAME".into(), s, s2]],
        13 => vec![vec!["MSET".into(), s, v.clone(), s2, v]],
        14 => vec![vec!["DEL".into(), s, k(rng, "h")]],
        15 => vec![vec!["HSET".into(), h, n(rng, 5), v]],
        16 => vec![vec!["HINCRBY".into(), h, n(rng, 5), "3".into()]],
        17 => vec![vec!["HDEL".into(), h, n(rng, 5)]],
        18 => vec![vec!["RPUSH".into(), l, v.clone(), v]],
        19 => vec![vec!["LPOP".into(), l]],
        20 => vec![vec!["LMOVE".into(), l, l2, "LEFT".into(), "RIGHT".into()]],
        21 => vec![vec!["LTRIM".into(), l, "0".into(), "3".into()]],
        22 => vec![vec!["SADD".into(), st, v, n(rng, 9), n(rng, 9)]],
        23 => vec![vec!["SPOP".into(), st]],
        24 => vec![vec!["SPOP".into(), st, "2".into()]],
        25 => vec![vec!["SMOVE".into(), st, st2, n(rng, 9)]],
        26 => vec![vec!["SINTERSTORE".into(), st, st2, k(rng, "set")]],
        27 => vec![vec!["ZADD".into(), z, n(rng, 50), n(rng, 9)]],
        28 => vec![vec!["ZINCRBY".into(), z, "2".into(), n(rng, 9)]],
        29 => vec![vec!["ZPOPMIN".into(), z]],
        30 => vec![vec![
            "ZUNIONSTORE".into(),
            z,
            "2".into(),
            k(rng, "z"),
            k(rng, "z"),
        ]],
        31 => vec![vec!["XADD".into(), x, "*".into(), "f".into(), v]],
        32 => vec![vec![
            "XADD".into(),
            x,
            "MAXLEN".into(),
            "~".into(),
            "5".into(),
            "*".into(),
            "f".into(),
            v,
        ]],
        33 => vec![vec!["XTRIM".into(), x, "MAXLEN".into(), "3".into()]],
        34 => vec![
            vec!["MULTI".into()],
            vec!["INCR".into(), s.clone()],
            vec!["SPOP".into(), st],
            vec!["RPUSH".into(), l, v],
            vec!["EXEC".into()],
        ],
        35 => vec![vec![
            "EVAL".into(),
            "redis.call('INCR', KEYS[1]) redis.call('SPOP', KEYS[2]) \
             redis.call('SET', KEYS[3], 'x', 'PX', ARGV[1]) \
             return redis.call('XADD', KEYS[4], '*', 'a', 'b')"
                .into(),
            "4".into(),
            s,
            st,
            s2,
            x,
            ms,
        ]],
        49 => vec![vec!["SETBIT".into(), s, n(rng, 200), n(rng, 2)]],
        50 => vec![vec![
            "BITFIELD".into(),
            s,
            "INCRBY".into(),
            "u8".into(),
            "#1".into(),
            n(rng, 300),
        ]],
        51 => vec![vec!["BITOP".into(), "XOR".into(), s, s2.clone(), s2]],
        52 => vec![vec!["BITOP".into(), "NOT".into(), s, s2]],
        40 => vec![vec!["APPEND".into(), s, v]],
        41 => vec![vec!["SETRANGE".into(), s, n(rng, 8), v]],
        42 => vec![vec!["GETSET".into(), s, v]],
        43 => vec![vec!["INCRBYFLOAT".into(), s, "0.5".into()]],
        44 => vec![vec!["MSETNX".into(), s, v, s2, ms]],
        45 => vec![vec!["COPY".into(), s, s2]],
        46 => vec![vec!["COPY".into(), l, l2, "REPLACE".into()]],
        47 => vec![vec!["SORT".into(), l, "ALPHA".into(), "STORE".into(), l2]],
        48 => vec![vec!["HINCRBYFLOAT".into(), h, n(rng, 5), "0.25".into()]],
        36 => {
            // A client blocked on a list, served by the push that follows.
            let mut buf = Vec::new();
            let mut ranges = Vec::new();
            for arg in [&b"BLPOP"[..], l.as_bytes(), b"0"] {
                ranges.push(buf.len()..buf.len() + arg.len());
                buf.extend_from_slice(arg);
            }
            let mut waiter = ctx.connect(-1).unwrap().session();
            let mut out = Vec::new();
            if let Reply::Block(b) = cmd::execute(
                ctx,
                &mut waiter,
                ctx.store.now(),
                &Args::new(&buf, &ranges),
                &mut out,
            ) {
                blocked.push(b);
            }
            vec![vec!["RPUSH".into(), l, v]]
        }
        37 if rng.below(100) == 0 => vec![vec!["FLUSHALL".into()]],
        38 => vec![vec!["SET".into(), k(rng, "z"), v]],
        _ => vec![vec!["ZREMRANGEBYSCORE".into(), z, "0".into(), "10".into()]],
    };
    for command in commands {
        let args: Vec<&[u8]> = command.iter().map(|a| a.as_bytes()).collect();
        run(ctx, session, &args);
    }
    // Served by now, or given up on.
    blocked.clear();
}

/// The first ID pending in group `g` of stream `x`, or any ID.
fn pending_id(ctx: &Context, session: &mut Session, x: &str, g: &str) -> String {
    let reply = run(ctx, session, &[b"XPENDING", x.as_bytes(), g.as_bytes()]);
    match common::parse(&reply) {
        common::Value::Array(items) => match items.get(1) {
            Some(common::Value::Bulk(id)) => String::from_utf8_lossy(id).into_owned(),
            _ => "1-1".to_string(),
        },
        _ => "1-1".to_string(),
    }
}

/// Runs `steps` random writes on a store with the log in `dir` (rewriting
/// at `rewrites`), then checks a store started from the log matches it.
fn round_trip(dir: &Path, seed: u64, steps: u64, rewrites: &[u64], config: Config) -> String {
    let (ctx, log, started) = open(dir, config.clone());
    assert!(started.created);
    let mut session = ctx.connect(-1).unwrap().session();
    let mut rng = Rng(seed);
    let mut blocked = Vec::new();
    for i in 0..steps {
        step(ctx, &mut session, &mut rng, &mut blocked);
        if rewrites.contains(&i) {
            assert!(log.rewrite());
        }
        if i % 150 == 0 {
            // Some TTLs run out.
            std::thread::sleep(Duration::from_millis(15));
        }
    }
    drop(blocked);
    let cutoff = cutoff();
    let before = dump(ctx, cutoff);
    close(&log);
    let (again, log2, started) = open(dir, config);
    assert!(!started.created);
    assert_eq!(started.failed, 0, "logged commands failed when run again");
    assert_same(&before, &dump(again, cutoff));
    close(&log2);
    before
}

#[test]
fn a_store_comes_back_from_its_log() {
    let dir = temp_dir("round-trip");
    let data = round_trip(&dir, 0x2545_F491_4F6C_DD1D, 3000, &[], Config::default());
    assert!(
        data.lines().count() > 20,
        "a workload that leaves data: {data}"
    );
}

#[test]
fn rewrites_keep_everything() {
    let dir = temp_dir("rewrites");
    round_trip(
        &dir,
        0x9E37_79B9_7F4A_7C15,
        3000,
        &[700, 701, 1900],
        Config::default(),
    );
    // Only the last base and log are left.
    let mut names: Vec<_> = std::fs::read_dir(&*dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    assert_eq!(names.len(), 3, "{names:?}");
}

#[test]
fn evictions_are_logged() {
    let dir = temp_dir("evictions");
    let config = Config {
        maxmemory: 300 << 10,
        policy: EvictionPolicy::AllKeysLru,
        ..Config::default()
    };
    let (ctx, log, _) = open(&dir, config.clone());
    let mut session = ctx.connect(-1).unwrap().session();
    let value = vec![b'v'; 1000];
    for i in 0..2000 {
        let key = format!("k{i}");
        if i % 4 == 0 {
            // Its pairs can evict each other.
            let keys: Vec<String> = (0..8).map(|j| format!("{key}.{j}")).collect();
            let mut args: Vec<&[u8]> = vec![b"MSET"];
            for key in &keys {
                args.extend([key.as_bytes(), &value]);
            }
            assert_eq!(run(ctx, &mut session, &args), b"+OK\r\n");
        } else {
            run(ctx, &mut session, &[b"SET", key.as_bytes(), &value]);
        }
    }
    let cutoff = cutoff();
    let before = dump(ctx, cutoff);
    let info = String::from_utf8(run(ctx, &mut session, &[b"INFO"])).unwrap();
    assert!(!info.contains("evicted_keys:0\r"), "it evicted");
    close(&log);
    let (again, log2, started) = open(&dir, config);
    assert_eq!(started.failed, 0);
    assert_same(&before, &dump(again, cutoff));
    close(&log2);
}

#[test]
fn an_mset_out_of_memory_logs_the_pairs_it_set() {
    let dir = temp_dir("mset-oom");
    let config = Config {
        maxmemory: 200 << 10,
        policy: EvictionPolicy::NoEviction,
        ..Config::default()
    };
    let (ctx, log, _) = open(&dir, config.clone());
    let mut session = ctx.connect(-1).unwrap().session();
    let value = vec![b'v'; 1000];
    let keys: Vec<String> = (0..1000).map(|i| format!("k{i}")).collect();
    let mut args: Vec<&[u8]> = vec![b"MSET"];
    for key in &keys {
        args.extend([key.as_bytes(), &value]);
    }
    let reply = run(ctx, &mut session, &args);
    assert!(
        reply.starts_with(b"-OOM"),
        "{}",
        String::from_utf8_lossy(&reply)
    );
    let before = dump(ctx, 0);
    assert!(before.lines().count() > 10, "set some first");
    close(&log);
    let (again, log2, started) = open(&dir, config);
    assert_eq!(started.failed, 0);
    assert_same(&before, &dump(again, 0));
    close(&log2);
}

fn log_file(dir: &Path) -> PathBuf {
    let manifest = std::fs::read_to_string(dir.join("tantalum.aof.manifest")).unwrap();
    let name = manifest
        .lines()
        .filter_map(|l| l.strip_prefix("incr "))
        .next_back()
        .unwrap();
    dir.join(name)
}

/// Writes a few commands, appends `extra` to the log (a torn or damaged
/// record), and starts from it.
fn torn(name: &str, extra: &[u8]) -> (String, aof::Started) {
    let dir = temp_dir(name);
    let (ctx, log, _) = open(&dir, Config::default());
    let mut session = ctx.connect(-1).unwrap().session();
    run(ctx, &mut session, &[b"SET", b"a", b"1"]);
    run(ctx, &mut session, &[b"RPUSH", b"l", b"x", b"y"]);
    close(&log);
    let path = log_file(&dir);
    let mut data = std::fs::read(&path).unwrap();
    data.extend_from_slice(extra);
    std::fs::write(&path, &data).unwrap();
    let (again, log2, started) = open(&dir, Config::default());
    let mut session = again.connect(-1).unwrap().session();
    // It goes on logging after what it kept.
    run(again, &mut session, &[b"INCR", b"a"]);
    let data = dump(again, 0);
    close(&log2);
    let (third, log3, more) = open(&dir, Config::default());
    assert_eq!(more.truncated, 0);
    assert_same(&data, &dump(third, 0));
    close(&log3);
    (data, started)
}

#[test]
fn a_torn_last_record_is_dropped() {
    let (data, started) = torn("torn-record", b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$5\r\nhel");
    assert!(started.truncated > 0);
    assert!(data.contains("a +string $1\r\n2\r\n"), "{data}");
    assert!(!data.contains("\nb +"), "{data}");
}

#[test]
fn a_torn_group_is_dropped_whole() {
    let (data, started) = torn(
        "torn-group",
        b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n*2\r\n$4\r\nINCR\r\n$1\r\na\r\n",
    );
    assert!(started.truncated > 0);
    assert!(!data.contains("\nb +"), "{data}");
    assert!(data.contains("a +string $1\r\n2\r\n"), "{data}");
}

#[test]
fn damage_inside_the_log_stops_the_load() {
    let dir = temp_dir("damaged");
    let (ctx, log, _) = open(&dir, Config::default());
    let mut session = ctx.connect(-1).unwrap().session();
    run(ctx, &mut session, &[b"SET", b"a", b"1"]);
    run(ctx, &mut session, &[b"SET", b"b", b"2"]);
    close(&log);
    let path = log_file(&dir);
    let mut data = std::fs::read(&path).unwrap();
    let at = data.len() / 2;
    data[at..at + 4].copy_from_slice(b"\x00\x00\x00\x00");
    std::fs::write(&path, &data).unwrap();
    let ctx = Box::leak(Box::new(Context::new(
        store(Config::default()),
        Settings::default(),
    )));
    assert!(aof::start(ctx, options(&dir)).is_err());
}

/// Where a rewrite has started a new log and not yet written its base: the
/// manifest names the old base and both logs, and all of them load.
#[test]
fn several_logs_load_in_order() {
    let dir = temp_dir("several");
    let (ctx, log, _) = open(&dir, Config::default());
    let mut session = ctx.connect(-1).unwrap().session();
    run(ctx, &mut session, &[b"SET", b"a", b"1"]);
    run(ctx, &mut session, &[b"RPUSH", b"l", b"x"]);
    close(&log);
    // A second log, as a rewrite starts one.
    let mut second = b"TANTALUM AOF 1\r\n".to_vec();
    for command in [&[&b"INCR"[..], b"a"][..], &[b"RPUSH", b"l", b"y"]] {
        second.extend_from_slice(format!("*{}\r\n", command.len()).as_bytes());
        for arg in command {
            second.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            second.extend_from_slice(arg);
            second.extend_from_slice(b"\r\n");
        }
    }
    std::fs::write(dir.join("tantalum.incr.9.aof"), &second).unwrap();
    let manifest = std::fs::read_to_string(dir.join("tantalum.aof.manifest")).unwrap();
    std::fs::write(
        dir.join("tantalum.aof.manifest"),
        format!("{manifest}incr tantalum.incr.9.aof\n"),
    )
    .unwrap();
    let (again, log2, started) = open(&dir, Config::default());
    assert_eq!(started.commands, 4);
    let mut session = again.connect(-1).unwrap().session();
    assert_eq!(run(again, &mut session, &[b"GET", b"a"]), b"$1\r\n2\r\n");
    assert_eq!(
        run(again, &mut session, &[b"LRANGE", b"l", b"0", b"-1"]),
        b"*2\r\n$1\r\nx\r\n$1\r\ny\r\n"
    );
    close(&log2);
}

/// A server process, killed (SIGKILL) when dropped.
struct Server(std::process::Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The server binary with the log in `dir`, ready for connections.
fn server(dir: &Path) -> (Server, std::net::TcpStream) {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let child = std::process::Command::new(env!("CARGO_BIN_EXE_tantalum"))
        .args([
            "--bind",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--threads",
            "1",
        ])
        .arg("--aof")
        .arg(dir)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let child = Server(child);
    for _ in 0..200 {
        if let Ok(stream) = std::net::TcpStream::connect(("127.0.0.1", port)) {
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            return (child, stream);
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("the server didn't start");
}

/// Sends `args` and checks the reply is `expected`.
fn call(stream: &mut std::net::TcpStream, args: &[&str], expected: &str) {
    use std::io::{Read, Write};
    let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
    stream.write_all(&common::req(&args)).unwrap();
    let mut reply = vec![0; expected.len()];
    stream.read_exact(&mut reply).unwrap();
    assert_eq!(String::from_utf8_lossy(&reply), expected, "{args:?}");
}

#[test]
fn a_killed_server_comes_back() {
    let dir = temp_dir("killed");
    let (server1, mut c) = server(&dir);
    call(&mut c, &["SET", "a", "1"], "+OK\r\n");
    call(&mut c, &["RPUSH", "l", "x", "y", "z"], ":3\r\n");
    call(&mut c, &["SET", "t", "v", "PX", "600000"], "+OK\r\n");
    call(&mut c, &["XADD", "s", "5-1", "f", "v"], "$3\r\n5-1\r\n");
    call(&mut c, &["MULTI"], "+OK\r\n");
    call(&mut c, &["INCR", "a"], "+QUEUED\r\n");
    call(&mut c, &["LPOP", "l"], "+QUEUED\r\n");
    call(&mut c, &["EXEC"], "*2\r\n:2\r\n$1\r\nx\r\n");
    call(
        &mut c,
        &[
            "EVAL",
            "return redis.call('SADD', KEYS[1], 'm')",
            "1",
            "set",
        ],
        ":1\r\n",
    );
    // In the file (the writer writes every 10 ms), not yet synced.
    std::thread::sleep(Duration::from_millis(300));
    drop(server1); // SIGKILL

    let (server2, mut c) = server(&dir);
    call(&mut c, &["GET", "a"], "$1\r\n2\r\n");
    call(
        &mut c,
        &["LRANGE", "l", "0", "-1"],
        "*2\r\n$1\r\ny\r\n$1\r\nz\r\n",
    );
    call(
        &mut c,
        &["XADD", "s", "5-1", "f", "v"],
        "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n",
    );
    call(&mut c, &["SISMEMBER", "set", "m"], ":1\r\n");
    call(&mut c, &["EXPIRE", "t", "1", "NX"], ":0\r\n"); // has a TTL
    call(&mut c, &["PERSIST", "t"], ":1\r\n");
    std::thread::sleep(Duration::from_millis(300));
    drop(server2);

    // And the commands of the second run too.
    let (_server3, mut c) = server(&dir);
    call(&mut c, &["TTL", "t"], ":-1\r\n");
    call(&mut c, &["GET", "a"], "$1\r\n2\r\n");
}

#[test]
fn writes_wait_for_a_log_that_cant_be_written() {
    let dir = temp_dir("full");
    let (ctx, log, _) = open(&dir, Config::default());
    let mut c = ctx.connect(-1).unwrap().session();
    let status = |log: &aof::Aof, want: &str| {
        let want = format!("aof_last_write_status:{want}\r\n");
        for _ in 0..500 {
            if log.info().contains(&want) {
                return;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        panic!("no {want}");
    };
    run(ctx, &mut c, &[b"SET", b"a", b"1"]);
    log.fail_writes(Some(28)); // ENOSPC
    // Taken, as the failure isn't known yet: written when it can be.
    assert_eq!(run(ctx, &mut c, &[b"SET", b"b", b"2"]), b"+OK\r\n");
    status(&log, "err");
    let misconf = "-MISCONF Errors writing to the AOF file: No space left on device\r\n";
    let text = |reply: Vec<u8>| String::from_utf8(reply).unwrap();
    assert_eq!(text(run(ctx, &mut c, &[b"SET", b"c", b"3"])), misconf);
    assert_eq!(text(run(ctx, &mut c, &[b"PING"])), misconf);
    assert_eq!(text(run(ctx, &mut c, &[b"GET", b"a"])), "$1\r\n1\r\n");
    assert_eq!(text(run(ctx, &mut c, &[b"DBSIZE"])), ":2\r\n");
    // In a transaction: refused while queueing, or at EXEC.
    run(ctx, &mut c, &[b"MULTI"]);
    assert_eq!(text(run(ctx, &mut c, &[b"INCR", b"a"])), misconf);
    assert_eq!(
        text(run(ctx, &mut c, &[b"EXEC"])),
        "-EXECABORT Transaction discarded because of previous errors.\r\n"
    );
    log.fail_writes(None);
    status(&log, "ok");
    run(ctx, &mut c, &[b"MULTI"]);
    run(ctx, &mut c, &[b"INCR", b"a"]);
    log.fail_writes(Some(28));
    let mut other = ctx.connect(-1).unwrap().session();
    run(ctx, &mut other, &[b"SET", b"x", b"1"]);
    status(&log, "err");
    assert_eq!(
        text(run(ctx, &mut c, &[b"EXEC"])),
        "-EXECABORT Transaction discarded because of: MISCONF Errors writing to the AOF file: \
         No space left on device\r\n"
    );
    assert_eq!(
        text(run(ctx, &mut c, &[b"EXEC"])),
        "-ERR EXEC without MULTI\r\n"
    );
    // Scripts: a write fails as a command would; one declared writing
    // doesn't run; one declared not writing does.
    let script = text(run(
        ctx,
        &mut c,
        &[b"EVAL", b"return redis.call('SET', 'e', '1')", b"0"],
    ));
    assert!(
        script.starts_with(
            "-MISCONF Errors writing to the AOF file: No space left on device script: "
        ),
        "{script}"
    );
    assert_eq!(
        text(run(ctx, &mut c, &[b"EVAL", b"#!lua\nreturn 1", b"0"])),
        misconf
    );
    assert_eq!(
        text(run(
            ctx,
            &mut c,
            &[
                b"EVAL",
                b"#!lua flags=no-writes\nreturn redis.call('GET', 'a')",
                b"0"
            ]
        )),
        "$1\r\n1\r\n"
    );
    log.fail_writes(None);
    status(&log, "ok");
    assert_eq!(run(ctx, &mut c, &[b"SET", b"f", b"6"]), b"+OK\r\n");
    let before = dump(ctx, 0);
    assert!(before.contains("b +string $1\r\n2\r\n"), "{before}");
    assert!(before.contains("x +string"), "{before}");
    assert!(
        !before.contains("\nc +") && !before.contains("\ne +"),
        "{before}"
    );
    close(&log);
    let (again, log2, started) = open(&dir, Config::default());
    assert_eq!((started.failed, started.truncated), (0, 0));
    assert_same(&before, &dump(again, 0));
    close(&log2);
}

/// `args` run on a new connection, which blocks: kept until dropped.
fn blocked(ctx: &Context, args: &[&[u8]]) -> Option<Box<tantalum::blocking::Blocked>> {
    let mut buf = Vec::new();
    let mut ranges = Vec::new();
    for arg in args {
        ranges.push(buf.len()..buf.len() + arg.len());
        buf.extend_from_slice(arg);
    }
    let mut waiter = ctx.connect(-1).unwrap().session();
    match cmd::execute(
        ctx,
        &mut waiter,
        ctx.store.now(),
        &Args::new(&buf, &ranges),
        &mut Vec::new(),
    ) {
        Reply::Block(b) => Some(b),
        _ => None,
    }
}

#[test]
fn blocked_clients_served_after_a_read_are_logged() {
    // A read that finds a key ready serves its blocked clients too: their
    // pops must be logged, in order, as the writer's are.
    let dir = temp_dir("served-by-reads");
    let (ctx, log, _) = open(&dir, Config::default());
    let done = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        for _ in 0..3 {
            scope.spawn(|| {
                let mut c = ctx.connect(-1).unwrap().session();
                while !done.load(std::sync::atomic::Ordering::Relaxed) {
                    run(ctx, &mut c, &[b"LLEN", b"q"]);
                }
            });
        }
        let mut c = ctx.connect(-1).unwrap().session();
        let mut waiting = Vec::new();
        for i in 0..5000 {
            waiting.extend(blocked(ctx, &[b"BLPOP", b"q", b"0"]));
            run(ctx, &mut c, &[b"RPUSH", b"q", i.to_string().as_bytes()]);
            if i % 100 == 0 {
                waiting.clear();
            }
        }
        done.store(true, std::sync::atomic::Ordering::Relaxed);
    });
    let before = dump(ctx, 0);
    close(&log);
    let (again, log2, started) = open(&dir, Config::default());
    assert_eq!(started.failed, 0);
    assert_same(&before, &dump(again, 0));
    close(&log2);
}

/// `INCRBYFLOAT` and `HINCRBYFLOAT` are logged as what they came to, so a
/// machine that rounds differently gets the same.
#[test]
fn float_increments_are_logged_as_what_they_set() {
    let dir = temp_dir("floats");
    let (ctx, log, _) = open(&dir, Config::default());
    let mut session = ctx.connect(-1).unwrap().session();
    run(ctx, &mut session, &[b"INCRBYFLOAT", b"f", b"1.5"]);
    run(ctx, &mut session, &[b"HINCRBYFLOAT", b"h", b"a", b"2.5"]);
    // A plain sort changes nothing, so it isn't logged.
    run(ctx, &mut session, &[b"RPUSH", b"l", b"1"]);
    run(ctx, &mut session, &[b"SORT", b"l"]);
    close(&log);
    let text = String::from_utf8_lossy(&std::fs::read(log_file(&dir)).unwrap()).into_owned();
    assert!(
        text.contains("$3\r\nSET\r\n$1\r\nf\r\n$3\r\n1.5\r\n$7\r\nKEEPTTL\r\n"),
        "{text}"
    );
    assert!(
        text.contains("$4\r\nHSET\r\n$1\r\nh\r\n$1\r\na\r\n$3\r\n2.5\r\n"),
        "{text}"
    );
    assert!(!text.contains("INCRBYFLOAT"), "{text}");
    assert!(!text.contains("SORT"), "{text}");
}

/// What's at `key` in database `db`.
fn get_in(ctx: &Context, db: &[u8], key: &[u8]) -> Vec<u8> {
    let mut session = ctx.connect(-1).unwrap().session();
    run(ctx, &mut session, &[b"SELECT", db]);
    run(ctx, &mut session, &[b"GET", key])
}

#[test]
fn numbered_databases_come_back_from_the_log() {
    let dir = temp_dir("databases");
    let (ctx, log, _) = open(&dir.0, Config::default());
    let mut s = ctx.connect(-1).unwrap().session();
    run(ctx, &mut s, &[b"SET", b"k", b"zero"]);
    run(ctx, &mut s, &[b"SELECT", b"2"]);
    run(ctx, &mut s, &[b"SET", b"k", b"two"]);
    // A transaction that goes elsewhere, and a script.
    run(ctx, &mut s, &[b"MULTI"]);
    run(ctx, &mut s, &[b"SET", b"a", b"still two"]);
    run(ctx, &mut s, &[b"SELECT", b"3"]);
    run(ctx, &mut s, &[b"SET", b"k", b"three"]);
    run(ctx, &mut s, &[b"EXEC"]);
    run(
        ctx,
        &mut s,
        &[
            b"EVAL",
            b"redis.call('SELECT', 4) return redis.call('SET', KEYS[1], 'four')",
            b"1",
            b"k",
        ],
    );
    // Still 3, here and in the log.
    run(ctx, &mut s, &[b"SET", b"b", b"three"]);
    run(ctx, &mut s, &[b"MOVE", b"b", b"5"]);
    run(ctx, &mut s, &[b"COPY", b"k", b"c", b"DB", b"6"]);
    run(ctx, &mut s, &[b"SELECT", b"7"]);
    run(ctx, &mut s, &[b"SET", b"k", b"seven"]);
    run(ctx, &mut s, &[b"SELECT", b"8"]);
    run(ctx, &mut s, &[b"SET", b"k", b"eight"]);
    run(ctx, &mut s, &[b"SWAPDB", b"7", b"8"]);
    run(ctx, &mut s, &[b"SELECT", b"9"]);
    run(ctx, &mut s, &[b"SET", b"gone", b"1"]);
    run(ctx, &mut s, &[b"FLUSHDB"]);
    close(&log);

    let check = |ctx: &Context| {
        assert_eq!(get_in(ctx, b"0", b"k"), b"$4\r\nzero\r\n");
        assert_eq!(get_in(ctx, b"2", b"k"), b"$3\r\ntwo\r\n");
        assert_eq!(get_in(ctx, b"2", b"a"), b"$9\r\nstill two\r\n");
        assert_eq!(get_in(ctx, b"3", b"k"), b"$5\r\nthree\r\n");
        assert_eq!(get_in(ctx, b"4", b"k"), b"$4\r\nfour\r\n");
        assert_eq!(get_in(ctx, b"3", b"b"), b"$-1\r\n");
        assert_eq!(get_in(ctx, b"5", b"b"), b"$5\r\nthree\r\n");
        assert_eq!(get_in(ctx, b"6", b"c"), b"$5\r\nthree\r\n");
        assert_eq!(get_in(ctx, b"7", b"k"), b"$5\r\neight\r\n");
        assert_eq!(get_in(ctx, b"8", b"k"), b"$5\r\nseven\r\n");
        assert_eq!(get_in(ctx, b"9", b"gone"), b"$-1\r\n");
    };
    let (ctx, log, started) = open(&dir.0, Config::default());
    assert_eq!(started.failed, 0);
    check(ctx);

    // The log left off in database 9: writes go on where they belong.
    let mut s = ctx.connect(-1).unwrap().session();
    run(ctx, &mut s, &[b"SET", b"after", b"zero"]);
    close(&log);
    let (ctx, log, started) = open(&dir.0, Config::default());
    assert_eq!(started.failed, 0);
    check(ctx);
    assert_eq!(get_in(ctx, b"0", b"after"), b"$4\r\nzero\r\n");
    assert_eq!(get_in(ctx, b"9", b"after"), b"$-1\r\n");

    // Through a rewrite, starting a new log in another database.
    let mut s = ctx.connect(-1).unwrap().session();
    run(ctx, &mut s, &[b"SELECT", b"10"]);
    run(ctx, &mut s, &[b"SET", b"before", b"ten"]);
    assert!(log.rewrite());
    run(ctx, &mut s, &[b"SET", b"since", b"ten"]);
    run(ctx, &mut s, &[b"SELECT", b"0"]);
    run(ctx, &mut s, &[b"SET", b"since", b"zero"]);
    close(&log);
    let (ctx, log, started) = open(&dir.0, Config::default());
    assert_eq!(started.failed, 0);
    check(ctx);
    assert_eq!(get_in(ctx, b"10", b"before"), b"$3\r\nten\r\n");
    assert_eq!(get_in(ctx, b"10", b"since"), b"$3\r\nten\r\n");
    assert_eq!(get_in(ctx, b"0", b"since"), b"$4\r\nzero\r\n");
    log.stop();
}

#[test]
fn using_only_database_0_logs_no_select() {
    let dir = temp_dir("database-0");
    let (ctx, log, _) = open(&dir.0, Config::default());
    let mut s = ctx.connect(-1).unwrap().session();
    run(ctx, &mut s, &[b"SELECT", b"0"]);
    run(ctx, &mut s, &[b"SET", b"k", b"zero"]);
    close(&log);
    for entry in std::fs::read_dir(&dir.0).unwrap() {
        let data = std::fs::read(entry.unwrap().path()).unwrap();
        assert!(
            !data.windows(6).any(|w| w == b"SELECT"),
            "a SELECT in the log"
        );
    }
}

#[test]
fn a_blocked_client_is_logged_in_its_database() {
    let dir = temp_dir("database-blocked");
    let (ctx, log, _) = open(&dir.0, Config::default());
    let mut s = ctx.connect(-1).unwrap().session();
    run(ctx, &mut s, &[b"SELECT", b"2"]);
    run(ctx, &mut s, &[b"RPUSH", b"l", b"a", b"b"]);
    // BLPOP that finds data, in database 2, after one in database 0.
    run(ctx, &mut s, &[b"BLPOP", b"l", b"0"]);
    run(ctx, &mut s, &[b"SELECT", b"0"]);
    run(ctx, &mut s, &[b"RPUSH", b"l", b"z"]);
    close(&log);
    let (ctx, log, started) = open(&dir.0, Config::default());
    assert_eq!(started.failed, 0);
    let mut s = ctx.connect(-1).unwrap().session();
    assert_eq!(
        run(ctx, &mut s, &[b"LRANGE", b"l", b"0", b"-1"]),
        b"*1\r\n$1\r\nz\r\n"
    );
    run(ctx, &mut s, &[b"SELECT", b"2"]);
    assert_eq!(
        run(ctx, &mut s, &[b"LRANGE", b"l", b"0", b"-1"]),
        b"*1\r\n$1\r\nb\r\n"
    );
    log.stop();
}

#[test]
fn evictions_are_logged_in_their_databases() {
    let dir = temp_dir("database-evictions");
    let config = Config {
        maxmemory: 400 << 10,
        policy: EvictionPolicy::AllKeysLru,
        ..Config::default()
    };
    let (ctx, log, _) = open(&dir, config.clone());
    let mut session = ctx.connect(-1).unwrap().session();
    let value = vec![b'v'; 1000];
    // Filling one database evicts from the others too.
    for db in [b"0", b"1", b"2"] {
        run(ctx, &mut session, &[b"SELECT", db]);
        for i in 0..600 {
            run(
                ctx,
                &mut session,
                &[b"SET", format!("k{i}").as_bytes(), &value],
            );
        }
    }
    let keys = |ctx: &Context| -> Vec<Vec<Vec<u8>>> {
        let mut session = ctx.connect(-1).unwrap().session();
        [b"0", b"1", b"2"]
            .iter()
            .map(|db| {
                run(ctx, &mut session, &[b"SELECT", &db[..]]);
                let mut keys = common::parse(&run(ctx, &mut session, &[b"KEYS", b"*"])).bulks();
                keys.sort();
                keys
            })
            .collect()
    };
    let before = keys(ctx);
    let info = String::from_utf8(run(ctx, &mut session, &[b"INFO"])).unwrap();
    assert!(!info.contains("evicted_keys:0\r"), "it evicted");
    close(&log);
    let (again, log2, started) = open(&dir, config);
    assert_eq!(started.failed, 0);
    assert_eq!(before, keys(again));
    close(&log2);
}

#[test]
fn hyperloglogs_and_geo_come_back() {
    let dir = temp_dir("geo-hll");
    let (server1, mut c) = server(&dir);
    call(&mut c, &["PFADD", "h", "a", "b", "c"], ":1\r\n");
    call(&mut c, &["PFCOUNT", "h"], ":3\r\n");
    call(&mut c, &["PFADD", "h2", "d"], ":1\r\n");
    call(&mut c, &["PFMERGE", "m", "h", "h2"], "+OK\r\n");
    call(
        &mut c,
        &[
            "GEOADD",
            "g",
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania",
        ],
        ":2\r\n",
    );
    // A search changes nothing, so it isn't logged.
    call(
        &mut c,
        &["GEORADIUS", "g", "15", "37", "200", "km", "COUNT", "1"],
        "*1\r\n$7\r\nCatania\r\n",
    );
    call(
        &mut c,
        &["GEORADIUS", "g", "15", "37", "100", "km", "STORE", "stored"],
        ":1\r\n",
    );
    call(
        &mut c,
        &[
            "GEOSEARCHSTORE",
            "near",
            "g",
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "100",
            "km",
        ],
        ":1\r\n",
    );
    std::thread::sleep(Duration::from_millis(300));
    drop(server1); // SIGKILL
    let log = std::fs::read(log_file(&dir)).unwrap();
    let times = |word: &[u8]| log.windows(word.len()).filter(|w| *w == word).count();
    assert_eq!(times(b"COUNT"), 0, "a search without STORE was logged");
    assert_eq!(times(b"GEOSEARCHSTORE"), 1);
    assert_eq!(times(b"STORE"), 2, "with GEOSEARCHSTORE's");

    let (_server2, mut c) = server(&dir);
    call(&mut c, &["PFCOUNT", "h"], ":3\r\n");
    call(&mut c, &["PFCOUNT", "m"], ":4\r\n");
    call(
        &mut c,
        &["GEODIST", "g", "Palermo", "Catania", "km"],
        "$8\r\n166.2742\r\n",
    );
    call(
        &mut c,
        &["ZRANGE", "stored", "0", "-1"],
        "*1\r\n$7\r\nCatania\r\n",
    );
    call(
        &mut c,
        &["ZRANGE", "near", "0", "-1"],
        "*1\r\n$7\r\nCatania\r\n",
    );
}
