//! Raw bytes as a connection's input: parsed like the server does, and
//! every command run against a small store with a memory limit. Nothing may
//! panic, and the parser must always make progress. Blocking commands that
//! block stay blocked for a few more commands, which may serve them.
//! Subscribing commands run on a second client, which the first one's
//! PUBLISHes reach (subscribed, the first could run little else).
//! `MIGRATE` doesn't run: it would connect wherever the input says, a server
//! on this machine included. (`RESTORE`'s readers have a test of their own
//! for damaged payloads, which seldom pass the checksum here.)
//!
//! cargo +nightly fuzz run commands corpus/commands seeds/commands -- -max_total_time=600

#![no_main]

use std::sync::{Arc, Mutex};

use libfuzzer_sys::fuzz_target;
use tantalum::cmd::{self, Args, Reply};
use tantalum::context::{Context, Settings};
use tantalum::resp;
use tantalum::store::{Config, EvictionPolicy, Store};

/// Stores with numbered databases. A store and those keep each other alive, for
/// as long as the server runs (see `Store`), which here is a store per input: this
/// keeps them where the leak check sees they aren't lost, so that it can go on
/// finding what is.
static WITH_DATABASES: Mutex<Vec<Arc<Store>>> = Mutex::new(Vec::new());

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
    let listener_client = ctx.connect(-1).unwrap();
    let mut listener = listener_client.session();
    let mut ranges = Vec::new();
    let mut out = Vec::new();
    let mut pos = 0;
    let mut now = 1;
    let mut blocked = std::collections::VecDeque::new();
    while let Ok(Some(next)) = resp::parse_command(data, pos, &mut ranges) {
        assert!(next > pos, "parser made no progress");
        if !ranges.is_empty() {
            out.clear();
            let args = Args::new(data, &ranges);
            let name = args.get(0);
            if name.eq_ignore_ascii_case(b"MIGRATE") {
                pos = next;
                continue;
            }
            let subscribing = name.len() >= 9
                && name[name.len() - 9..].eq_ignore_ascii_case(b"SUBSCRIBE")
                || name.eq_ignore_ascii_case(b"RESET");
            let session = if subscribing { &mut listener } else { &mut session };
            let reply = cmd::execute(&ctx, session, now, &args, &mut out);
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
    drop(listener);
    assert_eq!(ctx.pubsub.channel_count(false) + ctx.pubsub.pattern_count(), 0);
    if ctx.store.live().count() > 1 {
        for db in ctx.store.live() {
            db.clear();
        }
        WITH_DATABASES.lock().unwrap().push(ctx.store.clone());
    }
});
