//! Measures store memory per key, without the network layer:
//!
//!   cargo run --release --example memory -- [keys] [shards] [value-size]

use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use tantalum::store::{Condition, Expiry, Store};

/// mimalloc, plus a count of live requested bytes.
struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LIVE.fetch_add(layout.size(), Relaxed);
        unsafe { mimalloc::MiMalloc.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Relaxed);
        unsafe { mimalloc::MiMalloc.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn rss_bytes() -> usize {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .expect("run ps");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse::<usize>()
        .expect("parse rss")
        * 1024
}

fn main() {
    let mut args = std::env::args()
        .skip(1)
        .map(|a| a.parse::<usize>().expect("numeric argument"));
    let keys = args.next().unwrap_or(1_000_000);
    let shards = args.next().unwrap_or(1024);
    let value = vec![b'x'; args.next().unwrap_or(32)];

    let store = Store::new(shards);
    let (rss0, live0) = (rss_bytes(), LIVE.load(Relaxed));
    for i in 0..keys {
        let key = format!("key:{i}");
        store
            .set(
                key.as_bytes(),
                &value,
                Condition::Always,
                Expiry::Never,
                1,
                false,
            )
            .expect("unlimited store");
    }
    let (rss1, live1) = (rss_bytes(), LIVE.load(Relaxed));

    let per_key = |bytes: usize| bytes as f64 / keys as f64;
    let stats = store.stats();
    println!("{keys} keys, {shards} shards, {}-byte values", value.len());
    println!(
        "  rss          {:6.1} bytes/key (total rss {:.1} MB)",
        per_key(rss1 - rss0),
        rss1 as f64 / 1e6
    );
    println!("  requested    {:6.1} bytes/key", per_key(live1 - live0));
    println!(
        "  hash tables  {:6.1} bytes/key",
        per_key(stats.table_bytes)
    );
    println!(
        "  segments     {:6.1} bytes/key ({:.1} live)",
        per_key(stats.segment_bytes),
        per_key(stats.live_bytes)
    );
}
