//! Raw bytes as a connection's input: parsed like the server does, and
//! every command run against a small store with a memory limit. Nothing may
//! panic, and the parser must always make progress. Blocking commands that
//! block stay blocked for a few more commands, which may serve them.
//!
//! cargo +nightly fuzz run commands corpus/commands seeds/commands -- -max_total_time=600

#![no_main]

use std::sync::Arc;

use libfuzzer_sys::fuzz_target;
use tantalum::cmd::{self, Args, Reply};
use tantalum::context::{Context, Settings};
use tantalum::resp;
use tantalum::store::{Config, EvictionPolicy, Store};

fuzz_target!(|data: &[u8]| {
    let store = Store::with_config(Config {
        shards: 2,
        maxmemory: 1 << 20,
        policy: EvictionPolicy::AllKeysLru,
        ..Config::default()
    });
    let ctx = Context::new(Arc::new(store), Settings::default());
    let client = ctx.connect(-1).unwrap();
    let mut session = client.session();
    let mut ranges = Vec::new();
    let mut out = Vec::new();
    let mut pos = 0;
    let mut now = 1;
    let mut blocked = std::collections::VecDeque::new();
    while let Ok(Some(next)) = resp::parse_command(data, pos, &mut ranges) {
        assert!(next > pos, "parser made no progress");
        if !ranges.is_empty() {
            out.clear();
            let reply = cmd::execute(&ctx, &mut session, now, &Args::new(data, &ranges), &mut out);
            if let Reply::Block(b) = reply {
                blocked.push_back(b);
                if blocked.len() > 8 {
                    blocked.pop_front().unwrap().finish(&mut out);
                }
            }
        }
        pos = next;
        now += 3;
    }
    drop(blocked);
    assert_eq!(ctx.store.blocked_clients(), 0);
});
