//! A connection's transaction state: the commands `MULTI` queued for
//! `EXEC`, and the keys `WATCH` watches.
//!
//! `EXEC` runs its commands with every shard of the store locked
//! (`Store::exclusive`), so no other client's command runs meanwhile, as in
//! Redis. A watched key's change sets the watcher's flag, under the key's
//! shard lock: `EXEC` checks it holding every shard, so no change can slip
//! in between the check and the commands.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

use crate::cmd::Args;
use crate::store::{Millis, Store};

/// Commands queued by `MULTI`, back to back.
#[derive(Default)]
pub(crate) struct Queue {
    buf: Vec<u8>,
    ranges: Vec<Range<usize>>,
    /// Each command's arguments, as a range of `ranges`.
    commands: Vec<Range<usize>>,
    /// A command was refused while queueing: `EXEC` refuses them all.
    pub(crate) refused: bool,
}

impl Queue {
    pub(crate) fn push(&mut self, args: &Args<'_>) {
        let first = self.ranges.len();
        for i in 0..args.len() {
            let arg = args.get(i);
            self.ranges.push(self.buf.len()..self.buf.len() + arg.len());
            self.buf.extend_from_slice(arg);
        }
        self.commands.push(first..self.ranges.len());
    }

    pub(crate) fn len(&self) -> usize {
        self.commands.len()
    }

    /// Queued command `i`.
    pub(crate) fn command(&self, i: usize) -> Args<'_> {
        Args::new(&self.buf, &self.ranges[self.commands[i].clone()])
    }
}

/// The keys a connection watches. Dropping it stops watching them.
pub(crate) struct Watching {
    store: Arc<Store>,
    /// Set when a watched key changes.
    flag: Arc<AtomicBool>,
    /// Each key, and whether it had expired already when watched.
    keys: Vec<(Box<[u8]>, bool)>,
}

impl Watching {
    pub(crate) fn new(store: &Arc<Store>) -> Self {
        Watching {
            store: store.clone(),
            flag: Arc::default(),
            keys: Vec::new(),
        }
    }

    pub(crate) fn add(&mut self, key: &[u8], now: Millis) {
        if self.keys.iter().any(|(k, _)| **k == *key) {
            return;
        }
        let expired = self.store.watch(self.store.key(key), &self.flag, now);
        self.keys.push((key.into(), expired));
    }

    /// Whether a watched key changed, or expired since it was watched. For
    /// `EXEC`, holding every shard.
    pub(crate) fn changed(&self, now: Millis) -> bool {
        self.flag.load(Relaxed)
            || self
                .keys
                .iter()
                .any(|(key, expired)| !expired && self.store.has_expired(self.store.key(key), now))
    }
}

impl Drop for Watching {
    fn drop(&mut self) {
        for (key, _) in &self.keys {
            self.store.unwatch(self.store.key(key), &self.flag);
        }
    }
}
