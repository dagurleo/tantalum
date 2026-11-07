//! A connection's transaction state: the commands `MULTI` queued for
//! `EXEC`, and the keys `WATCH` watches.
//!
//! `EXEC` runs its commands as a transaction (`Store::exclusive`), so it's as if
//! no other client's command runs meanwhile, as in Redis. A watched key's change
//! sets the watcher's flag, under the key's shard lock: `EXEC` checks it holding
//! the shards of every watched key, so no change can slip in between the check
//! and the commands.

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
    /// Database 0.
    store: Arc<Store>,
    /// Set when a watched key changes.
    flag: Arc<AtomicBool>,
    /// Each key, its database, and whether it had expired already when
    /// watched.
    keys: Vec<(usize, Box<[u8]>, bool)>,
}

impl Watching {
    pub(crate) fn new(store: &Arc<Store>) -> Self {
        Watching {
            store: store.clone(),
            flag: Arc::default(),
            keys: Vec::new(),
        }
    }

    pub(crate) fn add(&mut self, db: usize, key: &[u8], now: Millis) {
        if self.keys.iter().any(|(d, k, _)| *d == db && **k == *key) {
            return;
        }
        let store = self.store.db(db);
        let expired = store.watch(store.key(key), &self.flag, now);
        self.keys.push((db, key.into(), expired));
    }

    /// Whether a watched key changed, or expired since it was watched. For
    /// `EXEC`, in its transaction: looking at each key holds its shard from then
    /// on, so the flag is read last.
    pub(crate) fn changed(&self, now: Millis) -> bool {
        let mut expired_since = false;
        for (db, key, expired) in &self.keys {
            let store = self.store.db(*db);
            expired_since |= store.has_expired(store.key(key), now) && !expired;
        }
        expired_since || self.flag.load(Relaxed)
    }
}

impl Drop for Watching {
    fn drop(&mut self) {
        for (db, key, _) in &self.keys {
            let store = self.store.db(*db);
            store.unwatch(store.key(key), &self.flag);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{Condition, Expiry};

    fn set(store: &Store, value: &[u8]) {
        store
            .set(
                b"watched",
                value,
                Condition::Always,
                Expiry::Never,
                1,
                false,
            )
            .unwrap();
    }

    /// `EXEC` hasn't reached the key yet when it checks, and a change right after
    /// the check would be lost under what it then writes: it must wait.
    #[test]
    fn no_change_slips_in_after_the_check() {
        let store = Arc::new(Store::new(4));
        set(&store, b"first");
        let mut watching = Watching::new(&store);
        watching.add(0, b"watched", 1);
        let writer = store.exclusive(|| {
            assert!(!watching.changed(1));
            let store = store.clone();
            let writer = std::thread::spawn(move || set(&store, b"theirs"));
            std::thread::sleep(std::time::Duration::from_millis(50));
            assert!(!writer.is_finished(), "it changed after the check");
            writer
        });
        writer.join().unwrap();
        assert!(watching.flag.load(Relaxed));
        assert!(store.exclusive(|| watching.changed(1)));
    }
}
