//! In-process benchmark for `tools/speed-guard.sh`: commands run through the
//! server's own parsing, prefetching and execution (`server::Batch`,
//! `server::run`), without sockets, so it measures Tantalum's code rather
//! than the kernel's. Prints one JSON line:
//!
//! - `memory_per_key`: accounted bytes per key after the fill (exact, so any
//!   change shows);
//! - `<workload>`: nanoseconds per command, one number per run.
//!
//!   bench [--keys N] [--ops N] [--runs N]

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
    let mut out = Vec::new();

    // Runs every command in `buf` as a connection does with one read.
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

    let value = [b'v'; 32];
    let mut fill = Vec::new();
    for i in 0..keys {
        push_command(&mut fill, &[b"SET", format!("key:{i}").as_bytes(), &value]);
        if (i + 1) % 64 == 0 || i + 1 == keys {
            serve(&fill, &mut out);
            fill.clear();
        }
    }
    let memory_per_key = store.stats().used_memory as f64 / keys as f64;

    let mut results = Vec::new();
    for (name, pipeline) in [("p1_ns", 1), ("p16_ns", 16)] {
        let mut times = Vec::new();
        for run in 0..runs {
            let reqs = requests(keys, ops, pipeline, 0x9E37_79B9_7F4A_7C15 ^ run as u64);
            let started = Instant::now();
            for req in &reqs {
                serve(req, &mut out);
            }
            let ns = started.elapsed().as_nanos() as f64 / (reqs.len() * pipeline) as f64;
            times.push(format!("{ns:.1}"));
        }
        results.push(format!("\"{name}\":[{}]", times.join(",")));
    }
    println!(
        "{{\"memory_per_key\":{memory_per_key:.3},{}}}",
        results.join(",")
    );
}
