//! In-process benchmark for `tools/speed-guard.sh`: commands run through the
//! server's own parsing, prefetching and execution (`server::Batch`,
//! `server::run`), without sockets, so it measures Tantalum's code rather
//! than the kernel's. Prints one JSON line:
//!
//! - `memory_per_key`: accounted bytes per key after the fill (exact, so any
//!   change shows);
//! - `<workload>`: nanoseconds per command, one number per run;
//! - `scan_ns_per_key`: a whole `Store::scan` of the string fill, 100 keys
//!   a step, per key;
//! - `hash_memory_per_field`: the same for a fill of hashes, per field.
//!
//! The string workloads run first and alone, as they did before hashes
//! existed; the hashes come after them, then a stream's consumer group
//! (`group_p16_ns`: `XADD`, `XREADGROUP` and `XACK` in turn) and one big hash
//! (`big_hash_p16_ns`: 1 `HSET` for every 10 `HGET`s).
//!
//!   bench [--keys N] [--ops N] [--runs N]

use std::cell::Cell;
use std::sync::Arc;
use std::time::Instant;

use tantalum::context::{Context, Settings};
use tantalum::server::{self, Batch};
use tantalum::store::{Config, Store};

/// Deterministic xorshift, so every run asks for the same keys.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % n as u64) as usize
    }
}

fn push_command(out: &mut Vec<u8>, args: &[&[u8]]) {
    out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
}

/// Requests of `pipeline` commands each: 1 SET for every 10 GETs, over
/// random keys of the fill, as memtier's 1:10 runs do.
fn requests(keys: usize, ops: usize, pipeline: usize, seed: u64) -> Vec<Vec<u8>> {
    let mut rng = Rng(seed);
    let value = [b'v'; 32];
    (0..ops / pipeline)
        .map(|_| {
            let mut buf = Vec::new();
            for _ in 0..pipeline {
                let key = format!("key:{}", rng.below(keys));
                if rng.below(11) == 0 {
                    push_command(&mut buf, &[b"SET", key.as_bytes(), &value]);
                } else {
                    push_command(&mut buf, &[b"GET", key.as_bytes()]);
                }
            }
            buf
        })
        .collect()
}

/// The hash fill: `--keys / 10` hashes of [`FIELDS`] fields of 16 bytes,
/// like small objects (a user, a session) kept as hashes.
const FIELDS: usize = 10;

/// Requests of `pipeline` commands each: 1 HSET of one field for every 10
/// HGETs, over random fields of random hashes of the fill.
fn hash_requests(hashes: usize, ops: usize, pipeline: usize, seed: u64) -> Vec<Vec<u8>> {
    let mut rng = Rng(seed);
    let value = [b'h'; 16];
    (0..ops / pipeline)
        .map(|_| {
            let mut buf = Vec::new();
            for _ in 0..pipeline {
                let key = format!("user:{}", rng.below(hashes));
                let field = format!("field{}", rng.below(FIELDS));
                if rng.below(11) == 0 {
                    push_command(
                        &mut buf,
                        &[b"HSET", key.as_bytes(), field.as_bytes(), &value],
                    );
                } else {
                    push_command(&mut buf, &[b"HGET", key.as_bytes(), field.as_bytes()]);
                }
            }
            buf
        })
        .collect()
}

/// Requests of `pipeline` commands each, a worker's life over and over: an entry
/// comes, is read through a consumer group and acknowledged. IDs go on from
/// `next`, as each run adds to the same stream.
fn group_requests(next: &Cell<u64>, ops: usize, pipeline: usize) -> Vec<Vec<u8>> {
    let value = [b'g'; 16];
    let mut step = 0;
    (0..ops / pipeline)
        .map(|_| {
            let mut buf = Vec::new();
            for _ in 0..pipeline {
                let id = format!("{}-0", next.get());
                match step {
                    0 => push_command(&mut buf, &[b"XADD", b"s", id.as_bytes(), b"f", &value]),
                    1 => push_command(
                        &mut buf,
                        &[
                            b"XREADGROUP",
                            b"GROUP",
                            b"g",
                            b"c",
                            b"COUNT",
                            b"1",
                            b"STREAMS",
                            b"s",
                            b">",
                        ],
                    ),
                    _ => {
                        push_command(&mut buf, &[b"XACK", b"s", b"g", id.as_bytes()]);
                        next.set(next.get() + 1);
                    }
                }
                step = (step + 1) % 3;
            }
            buf
        })
        .collect()
}

/// Requests of `pipeline` commands each on one big hash: 1 HSET for every 10 HGETs,
/// over random fields of its `fields`.
fn big_hash_requests(fields: usize, ops: usize, pipeline: usize, seed: u64) -> Vec<Vec<u8>> {
    let mut rng = Rng(seed);
    let value = [b'b'; 16];
    (0..ops / pipeline)
        .map(|_| {
            let mut buf = Vec::new();
            for _ in 0..pipeline {
                let field = format!("field:{}", rng.below(fields));
                if rng.below(11) == 0 {
                    push_command(&mut buf, &[b"HSET", b"big", field.as_bytes(), &value]);
                } else {
                    push_command(&mut buf, &[b"HGET", b"big", field.as_bytes()]);
                }
            }
            buf
        })
        .collect()
}

/// Runs `f` with a fresh store and a function that runs every command in a
/// buffer as a connection does with one read, writing replies to `out`.
fn with_server<R>(f: impl FnOnce(&Store, &mut dyn FnMut(&[u8], &mut Vec<u8>)) -> R) -> R {
    // A fixed hash seed lays the keys out the same way every run, so memory
    // per key compares exactly between builds.
    let store = Arc::new(Store::with_config(Config {
        shards: 256,
        fixed_seed: true,
        ..Config::default()
    }));
    let ctx = Context::new(store.clone(), Settings::default());
    let client = ctx.connect(-1).expect("a client");
    let mut session = client.session();
    let mut batch = Batch::default();
    let mut serve = |buf: &[u8], out: &mut Vec<u8>| {
        let now = store.now();
        let mut pos = 0;
        while pos < buf.len() {
            batch.parse(buf, pos, &store).expect("valid requests");
            for i in 0..batch.len() {
                batch.prefetch(i, &store);
                let args = batch.command(i, buf);
                if let Some(tantalum::cmd::Reply::Bulk(value)) =
                    server::run(&ctx, &mut session, now, &args, out)
                {
                    out.extend_from_slice(value.as_bytes());
                }
                pos = batch.end(i);
            }
            out.clear();
        }
    };
    f(&store, &mut serve)
}

/// Nanoseconds per command of each run, as a JSON field.
fn time(
    name: &str,
    runs: usize,
    serve: &mut dyn FnMut(&[u8], &mut Vec<u8>),
    requests: impl Fn(u64) -> Vec<Vec<u8>>,
    per_request: usize,
) -> String {
    let mut out = Vec::new();
    let mut times = Vec::new();
    for run in 0..runs {
        let reqs = requests(0x9E37_79B9_7F4A_7C15 ^ run as u64);
        let started = Instant::now();
        for req in &reqs {
            serve(req, &mut out);
        }
        let ns = started.elapsed().as_nanos() as f64 / (reqs.len() * per_request) as f64;
        times.push(format!("{ns:.1}"));
    }
    format!("\"{name}\":[{}]", times.join(","))
}

/// Sends `commands` in batches of 64, as a client filling the store would.
fn fill(serve: &mut dyn FnMut(&[u8], &mut Vec<u8>), commands: impl Iterator<Item = Vec<u8>>) {
    let (mut buf, mut out) = (Vec::new(), Vec::new());
    for (i, command) in commands.enumerate() {
        buf.extend_from_slice(&command);
        if (i + 1) % 64 == 0 {
            serve(&buf, &mut out);
            buf.clear();
        }
    }
    serve(&buf, &mut out);
}

fn command(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    push_command(&mut out, args);
    out
}

fn main() {
    let mut keys = 1_000_000;
    let mut ops = 2_000_000;
    let mut runs = 3;
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value: usize = args.next().and_then(|v| v.parse().ok()).expect("a number");
        match flag.as_str() {
            "--keys" => keys = value,
            "--ops" => ops = value,
            "--runs" => runs = value,
            _ => panic!("unknown option {flag}"),
        }
    }
    let mut results = Vec::new();

    with_server(|store, serve| {
        let value = [b'v'; 32];
        fill(
            serve,
            (0..keys).map(|i| command(&[b"SET", format!("key:{i}").as_bytes(), &value])),
        );
        let memory_per_key = store.stats().used_memory as f64 / keys as f64;
        results.push(format!("\"memory_per_key\":{memory_per_key:.3}"));
        for (name, pipeline) in [("p1_ns", 1), ("p16_ns", 16)] {
            let reqs = |seed| requests(keys, ops, pipeline, seed);
            results.push(time(name, runs, serve, reqs, pipeline));
        }
        let mut times = Vec::new();
        for _ in 0..runs {
            let (mut cursor, mut seen, mut bytes) = (0, 0usize, 0usize);
            let started = Instant::now();
            loop {
                cursor = store.scan(cursor, 100, store.now(), |key, _| {
                    seen += 1;
                    bytes += key.len();
                });
                if cursor == 0 {
                    break;
                }
            }
            let ns = started.elapsed().as_nanos() as f64 / seen as f64;
            assert!(seen >= keys && bytes > 0, "the scan missed keys");
            times.push(format!("{ns:.1}"));
        }
        results.push(format!("\"scan_ns_per_key\":[{}]", times.join(",")));
    });

    // Hashes in a store of their own, so the string workloads run as they
    // did before hashes existed.
    with_server(|store, serve| {
        let hashes = (keys / 10).max(1);
        let value = [b'h'; 16];
        let names: Vec<String> = (0..FIELDS).map(|j| format!("field{j}")).collect();
        fill(
            serve,
            (0..hashes).map(|i| {
                let key = format!("user:{i}");
                let mut args: Vec<&[u8]> = vec![b"HSET", key.as_bytes()];
                for name in &names {
                    args.extend([name.as_bytes(), &value]);
                }
                command(&args)
            }),
        );
        let per_field = store.stats().used_memory as f64 / (hashes * FIELDS) as f64;
        results.push(format!("\"hash_memory_per_field\":{per_field:.3}"));
        let reqs = |seed| hash_requests(hashes, ops, 16, seed);
        results.push(time("hash_p16_ns", runs, serve, reqs, 16));
    });

    // Consumer groups, likewise.
    with_server(|_, serve| {
        fill(
            serve,
            std::iter::once(command(&[
                b"XGROUP",
                b"CREATE",
                b"s",
                b"g",
                b"0",
                b"MKSTREAM",
            ])),
        );
        let next = Cell::new(1);
        let reqs = |_seed| group_requests(&next, ops, 16);
        results.push(time("group_p16_ns", runs, serve, reqs, 16));
    });

    // One big hash, likewise.
    with_server(|_, serve| {
        let fields = keys.min(1_000_000);
        let value = [b'b'; 16];
        fill(
            serve,
            (0..fields).map(|i| {
                let field = format!("field:{i}");
                command(&[b"HSET", b"big", field.as_bytes(), &value])
            }),
        );
        let reqs = |seed| big_hash_requests(fields, ops, 16, seed);
        results.push(time("big_hash_p16_ns", runs, serve, reqs, 16));
    });
    println!("{{{}}}", results.join(","));
}
