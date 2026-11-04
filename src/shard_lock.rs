//! A shard's lock, which the thread holding every shard for a transaction
//! (`Store::exclusive`, for `EXEC`) can take again while it runs the
//! transaction's commands: they use the store as always, and nothing else
//! runs meanwhile, as in Redis.
//!
//! Outside transactions, taking a lock costs one read more than the mutex
//! does: of a counter only transactions write, so it stays in every core's
//! cache.

use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use parking_lot::{Mutex, MutexGuard};

/// Threads holding every shard for a transaction right now.
static EXCLUSIVE: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static MARK: u8 = const { 0 };
}

/// This thread, as a number no other running thread has.
fn this_thread() -> usize {
    MARK.with(|mark| mark as *const u8 as usize)
}

pub(crate) struct ShardLock<T> {
    mutex: Mutex<T>,
    /// The thread holding it for a transaction, or 0.
    owner: AtomicUsize,
}

pub(crate) enum ShardGuard<'a, T> {
    Locked(MutexGuard<'a, T>),
    /// Taken again by the thread holding it for a transaction.
    Held(&'a mut T),
}

impl<T> ShardLock<T> {
    pub(crate) fn new(value: T) -> Self {
        ShardLock {
            mutex: Mutex::new(value),
            owner: AtomicUsize::new(0),
        }
    }

    pub(crate) fn lock(&self) -> ShardGuard<'_, T> {
        if EXCLUSIVE.load(Relaxed) != 0 && self.owner.load(Relaxed) == this_thread() {
            // SAFETY: this thread holds the mutex (see `hold_all`, whose
            // guard is never dereferenced) and uses the data only through
            // guards like this one, one at a time per shard (no store code
            // locks a shard it already holds).
            return ShardGuard::Held(unsafe { &mut *self.mutex.data_ptr() });
        }
        ShardGuard::Locked(self.mutex.lock())
    }

    /// Whether this thread holds it for a transaction (see [`hold_all`]).
    pub(crate) fn held_here(&self) -> bool {
        EXCLUSIVE.load(Relaxed) != 0 && self.owner.load(Relaxed) == this_thread()
    }

    pub(crate) fn try_lock(&self) -> Option<ShardGuard<'_, T>> {
        self.mutex.try_lock().map(ShardGuard::Locked)
    }
}

impl<T> Deref for ShardGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        match self {
            ShardGuard::Locked(guard) => guard,
            ShardGuard::Held(data) => data,
        }
    }
}

impl<T> DerefMut for ShardGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        match self {
            ShardGuard::Locked(guard) => guard,
            ShardGuard::Held(data) => data,
        }
    }
}

/// Every lock given, held by this thread until this drops.
pub(crate) struct Holding<'a, T> {
    locks: Vec<(&'a ShardLock<T>, MutexGuard<'a, T>)>,
}

/// Takes `locks` in the order given (shard order, as everything that takes
/// two does, so this can't deadlock) and holds them for this thread.
pub(crate) fn hold_all<'a, T>(locks: impl Iterator<Item = &'a ShardLock<T>>) -> Holding<'a, T> {
    EXCLUSIVE.fetch_add(1, Relaxed);
    let me = this_thread();
    let locks: Vec<_> = locks
        .map(|lock| {
            let guard = lock.mutex.lock();
            lock.owner.store(me, Relaxed);
            (lock, guard)
        })
        .collect();
    Holding { locks }
}

impl<T> Drop for Holding<'_, T> {
    fn drop(&mut self) {
        for (lock, _) in &self.locks {
            lock.owner.store(0, Relaxed);
        }
        self.locks.clear(); // unlocks
        EXCLUSIVE.fetch_sub(1, Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_holder_can_lock_again_and_others_wait() {
        let locks: Vec<ShardLock<u32>> = (0..4).map(ShardLock::new).collect();
        let locks = &*Box::leak(locks.into_boxed_slice());
        let holding = hold_all(locks.iter());
        *locks[2].lock() += 10;
        assert_eq!(*locks[2].lock(), 12);
        assert!(locks[1].try_lock().is_none());
        let other = std::thread::spawn(move || *locks[2].lock());
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(!other.is_finished(), "another thread must wait");
        drop(holding);
        assert_eq!(other.join().unwrap(), 12);
        assert!(locks[1].try_lock().is_some());
    }
}
