//! A shard's lock, which the thread holding every shard for a transaction
//! (`Store::exclusive`, for `EXEC` and Lua scripts), or the shards a native
//! script uses ([`Partial`]), can take again while it runs the commands:
//! they use the store as always, and nothing else touches those shards
//! meanwhile, as in Redis.
//!
//! Outside these, taking a lock costs one read more than the mutex does: of
//! a counter only they write, so it stays in every core's cache.
//!
//! Shards are always taken in ascending order, or tried without waiting,
//! so holders can't deadlock. A native script must say which shards it
//! needs before it writes (the next one may be below those it holds, and
//! then it starts over); locking one it didn't is a bug, which panics
//! rather than risk waiting for ever.

use std::cell::Cell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use parking_lot::{Mutex, MutexGuard};

/// Threads holding shards for a transaction or script right now.
static EXCLUSIVE: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static MARK: u8 = const { 0 };
    /// This thread holds every shard ([`hold_all`]), how many times over.
    static ALL: Cell<usize> = const { Cell::new(0) };
    /// This thread holds some shards for a native script ([`Partial`]).
    static PARTIAL: Cell<bool> = const { Cell::new(false) };
}

/// Whether this thread holds every shard (`EXEC`, a Lua script).
pub(crate) fn holding_all() -> bool {
    ALL.with(Cell::get) > 0
}

/// Whether this thread holds some shards for a native script: then other
/// shards may only be tried ([`ShardLock::try_lock_here`]).
pub(crate) fn holding_some() -> bool {
    PARTIAL.with(Cell::get)
}

#[cold]
#[inline(never)]
fn unplanned() -> ! {
    panic!("a native script used a shard it didn't hold");
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

/// A shard locked: its data, and the lock to release when this drops (none if
/// this thread holds it for a transaction). Two pointers, so that it comes back
/// in registers, with nothing to decide to reach the data: most commands take
/// two of these.
pub(crate) struct ShardGuard<'a, T> {
    data: &'a mut T,
    #[allow(dead_code)]
    lock: Option<MutexGuard<'a, T>>,
}

impl<T> ShardLock<T> {
    pub(crate) fn new(value: T) -> Self {
        ShardLock {
            mutex: Mutex::new(value),
            owner: AtomicUsize::new(0),
        }
    }

    /// Its data, for a guard that releases `lock`, if any.
    #[inline]
    fn guard<'a>(&'a self, lock: Option<MutexGuard<'a, T>>) -> ShardGuard<'a, T> {
        ShardGuard {
            // SAFETY: the caller holds what guards the data: this lock, or the
            // one standing for it (`lock_for`). It's only reached through guards
            // like this one, one at a time per shard (no store code locks a
            // shard it already holds, and `hold_all`'s and `Partial`'s guards
            // are never dereferenced).
            data: unsafe { &mut *self.mutex.data_ptr() },
            lock,
        }
    }

    #[inline]
    pub(crate) fn lock(&self) -> ShardGuard<'_, T> {
        self.lock_for(self)
    }

    /// Whether this thread holds it for a transaction (see [`hold_all`]).
    pub(crate) fn held_here(&self) -> bool {
        EXCLUSIVE.load(Relaxed) != 0 && self.owner.load(Relaxed) == this_thread()
    }

    #[inline]
    pub(crate) fn try_lock(&self) -> Option<ShardGuard<'_, T>> {
        self.try_lock_for(self)
    }

    /// [`try_lock`](Self::try_lock), or this thread's own if it holds it.
    pub(crate) fn try_lock_here(&self) -> Option<ShardGuard<'_, T>> {
        self.try_lock_here_for(self)
    }

    /// [`lock`](Self::lock), for the data of `data`, whose own lock nothing
    /// takes: this lock stands for it, so that what holds this holds that
    /// too (a numbered database's shard, under database 0's: see `Store`).
    #[inline]
    pub(crate) fn lock_for<'a>(&'a self, data: &'a ShardLock<T>) -> ShardGuard<'a, T> {
        if EXCLUSIVE.load(Relaxed) != 0 {
            if self.owner.load(Relaxed) == this_thread() {
                return data.guard(None);
            }
            if holding_some() {
                unplanned();
            }
        }
        data.guard(Some(self.mutex.lock()))
    }

    /// [`try_lock`](Self::try_lock), as [`lock_for`](Self::lock_for) does.
    #[inline]
    pub(crate) fn try_lock_for<'a>(&'a self, data: &'a ShardLock<T>) -> Option<ShardGuard<'a, T>> {
        Some(data.guard(Some(self.mutex.try_lock()?)))
    }

    /// [`try_lock_here`](Self::try_lock_here), as [`lock_for`](Self::lock_for) does.
    pub(crate) fn try_lock_here_for<'a>(
        &'a self,
        data: &'a ShardLock<T>,
    ) -> Option<ShardGuard<'a, T>> {
        if self.held_here() {
            return Some(data.guard(None));
        }
        self.try_lock_for(data)
    }
}

impl<T> Deref for ShardGuard<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        self.data
    }
}

impl<T> DerefMut for ShardGuard<'_, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        self.data
    }
}

/// Every lock given, held by this thread until this drops.
pub(crate) struct Holding<'a, T> {
    locks: Vec<(&'a ShardLock<T>, MutexGuard<'a, T>)>,
}

/// Takes `locks` in the order given (shard order, as everything that takes
/// two does, so this can't deadlock) and holds them for this thread.
pub(crate) fn hold_all<'a, T>(locks: impl Iterator<Item = &'a ShardLock<T>>) -> Holding<'a, T> {
    debug_assert!(!holding_some(), "a native script took every shard");
    EXCLUSIVE.fetch_add(1, Relaxed);
    ALL.with(|all| all.set(all.get() + 1));
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
        ALL.with(|all| all.set(all.get() - 1));
        EXCLUSIVE.fetch_sub(1, Relaxed);
    }
}

/// Some shards, held by this thread for a native script until this drops:
/// taken in ascending order, or tried. By index, ascending.
pub(crate) struct Partial<'a, T> {
    locks: Vec<(usize, &'a ShardLock<T>, MutexGuard<'a, T>)>,
}

impl<'a, T> Partial<'a, T> {
    /// Holds nothing yet.
    pub(crate) fn new() -> Self {
        debug_assert!(!holding_some() && !holding_all());
        EXCLUSIVE.fetch_add(1, Relaxed);
        PARTIAL.with(|p| p.set(true));
        Partial { locks: Vec::new() }
    }

    /// Takes shard `index` (`lock`): waiting for it if it's above every one
    /// held, else only if it's free. False if it wasn't: then holding it
    /// with the rest means starting over, all in order.
    pub(crate) fn take(&mut self, index: usize, lock: &'a ShardLock<T>) -> bool {
        let at = match self.locks.binary_search_by_key(&index, |(i, ..)| *i) {
            Ok(_) => return true,
            Err(at) => at,
        };
        let guard = if at == self.locks.len() {
            lock.mutex.lock()
        } else {
            match lock.mutex.try_lock() {
                Some(guard) => guard,
                None => return false,
            }
        };
        lock.owner.store(this_thread(), Relaxed);
        self.locks.insert(at, (index, lock, guard));
        true
    }

    /// The shards held, ascending.
    #[cfg(test)]
    fn indices(&self) -> Vec<usize> {
        self.locks.iter().map(|(i, ..)| *i).collect()
    }
}

impl<T> Drop for Partial<'_, T> {
    fn drop(&mut self) {
        for (_, lock, _) in &self.locks {
            lock.owner.store(0, Relaxed);
        }
        self.locks.clear(); // unlocks
        PARTIAL.with(|p| p.set(false));
        EXCLUSIVE.fetch_sub(1, Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn some_shards_are_taken_in_order_or_tried() {
        let locks: Vec<ShardLock<u32>> = (0..4).map(ShardLock::new).collect();
        let locks = &*Box::leak(locks.into_boxed_slice());
        // Another thread holds shard 1 meanwhile.
        let (held, release) = (std::sync::Barrier::new(2), std::sync::Barrier::new(2));
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let _guard = locks[1].mutex.lock();
                held.wait();
                release.wait();
            });
            held.wait();
            let mut partial = Partial::new();
            assert!(partial.take(2, &locks[2]));
            assert!(
                partial.take(3, &locks[3]),
                "above every one held: waits, gets it"
            );
            assert!(!partial.take(1, &locks[1]), "below, and taken: start over");
            assert!(partial.take(0, &locks[0]), "below, and free: taken");
            assert_eq!(partial.indices(), [0, 2, 3]);
            *locks[2].lock() += 10;
            assert_eq!(*locks[2].lock(), 12);
            assert!(locks[2].try_lock_here().is_some());
            assert!(locks[1].try_lock_here().is_none());
            let unplanned =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(locks[1].lock())));
            assert!(unplanned.is_err(), "an unheld shard must not be waited for");
            drop(partial);
            assert!(!holding_some());
            release.wait();
        });
        assert!(locks[2].try_lock().is_some());
    }

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
