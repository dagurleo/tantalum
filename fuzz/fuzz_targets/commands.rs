//! Raw bytes as a connection's input: parsed like the server does, and
//! every command run against a small store with a memory limit. Nothing may
//! panic, and the parser must always make progress.
//!
//! cargo +nightly fuzz run commands -- -max_total_time=600

#![no_main]

use std::sync::Arc;

use libfuzzer_sys::fuzz_target;
use tantalum::cmd::{self, Args};
use tantalum::context::{Context, Settings};
use tantalum::resp;
use tantalum::store::{Config, EvictionPolicy, Store};

fuzz_target!(|data: &[u8]| {
    let store = Store::with_config(Config {
        shards: 2,
        maxmemory: 1 << 20,
        policy: EvictionPolicy::AllKeysLru,
    });
    let ctx = Context::new(Arc::new(store), Settings::default());
    let client = ctx.connect(-1).unwrap();
    let mut session = client.session();
    let mut ranges = Vec::new();
    let mut out = Vec::new();
    let mut pos = 0;
    let mut now = 1;
    while let Ok(Some(next)) = resp::parse_command(data, pos, &mut ranges) {
        assert!(next > pos, "parser made no progress");
        if !ranges.is_empty() {
            out.clear();
            let _ = cmd::execute(&ctx, &mut session, now, &Args::new(data, &ranges), &mut out);
        }
        pos = next;
        now += 3;
    }
});
