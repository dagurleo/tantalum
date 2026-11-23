//! A shard's lock, and how a thread holds shards across many commands:
//!
//! - A transaction ([`transaction`], through `Store::exclusive`: `EXEC`, Lua
//!   scripts, commands on several keys) takes each shard the first time it
//!   touches it, whatever the order, and keeps it to the end. What it has read
//!   can't change and what it has written can't be seen until it finishes, so
//!   it's as if nothing else ran meanwhile, as in Redis, while commands on other
//!   shards carry on. Only one runs at a time.
//! - A native script ([`Partial`]) says which shards it needs before it writes,
//!   and starts over if it can't have them. Using one it didn't ask for is a
//!   bug, which panics rather than risk waiting for ever.
//!
//! Commands they run use the store as always: taking a lock this thread holds
//! this way costs nothing. Outside these, taking a lock costs one read more than
//! the mutex does: of a counter only they write, so it stays in every core's
//! cache.
//!
//! No deadlocks, because only a transaction waits for as long as it takes
//! while holding a shard. Anything else holding one takes the next in ascending
//! order, waits [`BRIEFLY`] at most, and otherwise lets go of what it holds and
//! starts over. Debug builds check this: waiting for a shard while holding
//! another panics.

use std::cell::{Cell, RefCell};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Duration;

use parking_lot::{Mutex, MutexGuard};

/// How long a thread holding a shard waits for another before it lets go: a
/// transaction may hold that one and be waiting for this one.
const BRIEFLY: Duration = Duration::from_millis(1);

/// Threads holding shards across commands right now.
static EXCLUSIVE: AtomicUsize = AtomicUsize::new(0);

/// Shards that owe a copy of themselves as they were at a point in time (a
/// replica's full sync: see `Store::request_copies`): while there are any,
/// each guard lets its data see to that before anything can change it.
static COPIES_OWED: AtomicUsize = AtomicUsize::new(0);

/// What a shard's data does whenever it's locked, while copies are owed.
pub(crate) trait OnLock {
    fn on_lock(&mut self);
}

/// Out of the way of every lock's path: while copies are owed.
#[cold]
#[inline(never)]
fn owing<T: OnLock>(data: &mut T) {
    data.on_lock();
}

/// A shard now owes a copy (see [`COPIES_OWED`]).
pub(crate) fn copy_owed() {
    COPIES_OWED.fetch_add(1, Relaxed);
}

/// A shard made the copy it owed.
pub(crate) fn copy_made() {
    COPIES_OWED.fetch_sub(1, Relaxed);
}

/// What a thread holds shards across commands for.
#[derive(Clone, Copy, PartialEq)]
enum Holding {
    Nothing,
    Transaction,
    NativeScript,
}

/// A shard a transaction took, whatever its lock's data is.
struct Taken {
    lock: *const (),
    release: unsafe fn(*const ()),
}

thread_local! {
    static MARK: u8 = const { 0 };
    static HOLDING: Cell<Holding> = const { Cell::new(Holding::Nothing) };
    /// What this thread's transaction has taken. Kept between transactions, so
    /// that they don't allocate.
    static TAKEN: RefCell<Vec<Taken>> = const { RefCell::new(Vec::new()) };
}

#[cfg(debug_assertions)]
thread_local! {
    /// Shards this thread has locked for a single command.
    static LOCKED: Cell<usize> = const { Cell::new(0) };
}

/// Whether this thread is running a transaction ([`transaction`]).
pub(crate) fn in_transaction() -> bool {
    HOLDING.get() == Holding::Transaction
}

/// Whether this thread holds shards for a native script ([`Partial`]).
pub(crate) fn holding_some() -> bool {
    HOLDING.get() == Holding::NativeScript
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
    /// The thread holding it across commands, or 0.
    owner: AtomicUsize,
}

/// A shard locked: its data, and the lock to release when this drops (none if
/// this thread holds it across commands). Two pointers, so that it comes back
/// in registers, with nothing to decide to reach the data: most commands take
/// two of these.
pub(crate) struct ShardGuard<'a, T> {
    data: &'a mut T,
    #[allow(dead_code)]
    lock: Option<MutexGuard<'a, T>>,
}

impl<T: OnLock> ShardLock<T> {
    pub(crate) fn new(value: T) -> Self {
        ShardLock {
            mutex: Mutex::new(value),
            owner: AtomicUsize::new(0),
        }
    }

    /// Its data, for a guard that releases `lock`, if any.
    #[inline]
    fn guard<'a>(&'a self, lock: Option<MutexGuard<'a, T>>) -> ShardGuard<'a, T> {
        #[cfg(debug_assertions)]
        if lock.is_some() {
            LOCKED.set(LOCKED.get() + 1);
        }
        // SAFETY: the caller holds what guards the data: this lock, or the
        // one standing for it (`lock_for`). It's only reached through guards
        // like this one, one at a time per shard (no store code locks a
        // shard it already holds, and `Partial`'s guards are never
        // dereferenced).
        let data = unsafe { &mut *self.mutex.data_ptr() };
        // Every way to a shard's data comes through here, so a copy it owes
        // is made before anything can change it.
        if COPIES_OWED.load(Relaxed) != 0 {
            owing(data);
        }
        ShardGuard { data, lock }
    }

    #[inline]
    pub(crate) fn lock(&self) -> ShardGuard<'_, T> {
        self.lock_for(self)
    }

    /// Whether this thread holds it across commands.
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

    /// For a thread holding shards across commands: `data`, by this lock. Out
    /// of the way of the path every command takes.
    #[cold]
    #[inline(never)]
    fn across_commands<'a>(&'a self, data: &'a ShardLock<T>) -> Option<ShardGuard<'a, T>> {
        if self.owner.load(Relaxed) == this_thread() {
            return Some(data.guard(None));
        }
        match HOLDING.get() {
            Holding::Nothing => None,
            Holding::Transaction => Some(self.take_for_transaction(data)),
            Holding::NativeScript => unplanned(),
        }
    }

    /// Takes it until this thread's transaction ends.
    fn take_for_transaction<'a>(&'a self, data: &'a ShardLock<T>) -> ShardGuard<'a, T> {
        std::mem::forget(self.mutex.lock());
        self.owner.store(this_thread(), Relaxed);
        TAKEN.with_borrow_mut(|taken| {
            taken.push(Taken {
                lock: (self as *const Self).cast(),
                release: release::<T>,
            });
        });
        data.guard(None)
    }

    /// [`lock`](Self::lock), for the data of `data`, whose own lock nothing
    /// takes: this lock stands for it, so that what holds this holds that
    /// too (a numbered database's shard, under database 0's: see `Store`).
    #[inline]
    pub(crate) fn lock_for<'a>(&'a self, data: &'a ShardLock<T>) -> ShardGuard<'a, T> {
        if EXCLUSIVE.load(Relaxed) != 0
            && let Some(guard) = self.across_commands(data)
        {
            return guard;
        }
        #[cfg(debug_assertions)]
        assert_eq!(LOCKED.get(), 0, "waiting for a shard while holding another");
        data.guard(Some(self.mutex.lock()))
    }

    /// [`lock_for`](Self::lock_for), for a thread holding the shards below this
    /// one for a command: `None` after waiting [`BRIEFLY`], and then it must let
    /// go of them and start over.
    #[inline]
    pub(crate) fn lock_next_for<'a>(&'a self, data: &'a ShardLock<T>) -> Option<ShardGuard<'a, T>> {
        if EXCLUSIVE.load(Relaxed) != 0
            && let Some(guard) = self.across_commands(data)
        {
            return Some(guard);
        }
        Some(data.guard(Some(self.mutex.try_lock_for(BRIEFLY)?)))
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

/// Lets go of a lock a transaction took.
///
/// # Safety
///
/// `lock` is that lock, a `ShardLock<T>`, its guard forgotten by this thread.
unsafe fn release<T>(lock: *const ()) {
    // SAFETY: as the caller says, and the store outlives its transactions.
    unsafe {
        let lock = &*lock.cast::<ShardLock<T>>();
        lock.owner.store(0, Relaxed);
        lock.mutex.force_unlock(stateless());
    }
}

/// The value of a type with nothing in it, as a mutex's guard state is.
fn stateless<G>() -> G {
    const { assert!(size_of::<G>() == 0) };
    // SAFETY: it has no bytes to be wrong.
    unsafe { std::mem::zeroed() }
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

#[cfg(debug_assertions)]
impl<T> Drop for ShardGuard<'_, T> {
    fn drop(&mut self) {
        if self.lock.is_some() {
            LOCKED.set(LOCKED.get() - 1);
        }
    }
}

/// A transaction running on this thread, until this drops.
pub(crate) struct Transaction<'a> {
    _turn: MutexGuard<'a, ()>,
}

/// Starts a transaction, waiting for its `turn`, a lock all of a store's
/// transactions share. Must end on this thread, before the locks it takes go
/// away.
pub(crate) fn transaction(turn: &Mutex<()>) -> Transaction<'_> {
    debug_assert!(HOLDING.get() == Holding::Nothing, "already holding shards");
    let turn = turn.lock();
    EXCLUSIVE.fetch_add(1, Relaxed);
    HOLDING.set(Holding::Transaction);
    Transaction { _turn: turn }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        TAKEN.with_borrow_mut(|taken| {
            for shard in taken.drain(..) {
                // SAFETY: `take_for_transaction` made this, and took the lock.
                unsafe { (shard.release)(shard.lock) };
            }
        });
        HOLDING.set(Holding::Nothing);
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
        debug_assert!(HOLDING.get() == Holding::Nothing, "already holding shards");
        EXCLUSIVE.fetch_add(1, Relaxed);
        HOLDING.set(Holding::NativeScript);
        Partial { locks: Vec::new() }
    }

    /// Takes shard `index` (`lock`): waiting for it, [`BRIEFLY`], if it's above
    /// every one held, else only if it's free. False if it wasn't had: then
    /// holding it with the rest means starting over, all in order.
    pub(crate) fn take(&mut self, index: usize, lock: &'a ShardLock<T>) -> bool {
        let at = match self.locks.binary_search_by_key(&index, |(i, ..)| *i) {
            Ok(_) => return true,
            Err(at) => at,
        };
        let guard = if at == self.locks.len() {
            lock.mutex.try_lock_for(BRIEFLY)
        } else {
            lock.mutex.try_lock()
        };
        let Some(guard) = guard else {
            return false;
        };
        lock.owner.store(this_thread(), Relaxed);
        self.locks.insert(at, (index, lock, guard));
        true
    }

    /// Lets go of every shard held.
    pub(crate) fn let_go(&mut self) {
        for (_, lock, _) in &self.locks {
            lock.owner.store(0, Relaxed);
        }
        self.locks.clear(); // unlocks
    }

    /// The shards held, ascending.
    #[cfg(test)]
    fn indices(&self) -> Vec<usize> {
        self.locks.iter().map(|(i, ..)| *i).collect()
    }
}

impl<T> Drop for Partial<'_, T> {
    fn drop(&mut self) {
        self.let_go();
        HOLDING.set(Holding::Nothing);
        EXCLUSIVE.fetch_sub(1, Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl OnLock for u32 {
        fn on_lock(&mut self) {}
    }

    fn leaked_locks() -> &'static [ShardLock<u32>] {
        Box::leak(
            (0..4)
                .map(ShardLock::new)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        )
    }

    #[test]
    fn some_shards_are_taken_in_order_or_tried() {
        let locks = leaked_locks();
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
            assert!(partial.take(3, &locks[3]), "above every one held, and free");
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
    fn a_native_script_waits_only_briefly_for_a_shard_above() {
        let locks = leaked_locks();
        let _taken = locks[3].mutex.lock();
        let mut partial = Partial::new();
        assert!(partial.take(1, &locks[1]));
        assert!(!partial.take(3, &locks[3]), "taken: start over");
        partial.let_go();
        assert!(partial.indices().is_empty());
        assert!(locks[1].mutex.try_lock().is_some());
    }

    #[test]
    fn a_transaction_holds_what_it_touched_and_nothing_else() {
        let locks = leaked_locks();
        let turn = Mutex::new(());
        let transaction = transaction(&turn);
        *locks[2].lock() += 10;
        assert_eq!(*locks[2].lock(), 12, "takes it again, at no cost");
        assert!(locks[2].mutex.is_locked(), "still held between commands");
        assert!(!locks[1].mutex.is_locked(), "not touched, not held");
        let other = std::thread::spawn(move || *locks[2].lock());
        std::thread::sleep(Duration::from_millis(50));
        assert!(!other.is_finished(), "another thread must wait");
        assert_eq!(
            std::thread::spawn(move || *locks[1].lock()).join().unwrap(),
            1,
            "a shard it didn't touch is free"
        );
        drop(transaction);
        assert_eq!(other.join().unwrap(), 12);
        assert!(!locks[2].mutex.is_locked());
        assert!(!in_transaction());
        assert!(turn.try_lock().is_some());
    }

    #[test]
    fn transactions_take_turns() {
        let turn = Mutex::new(());
        let first = transaction(&turn);
        assert!(turn.try_lock().is_none());
        drop(first);
        assert!(turn.try_lock().is_some());
    }

    /// A transaction takes shards in any order, so it can want the one a command
    /// holds while the command wants one it has: the command gives way.
    #[test]
    fn a_command_on_two_shards_gives_way_to_a_transaction() {
        let locks = leaked_locks();
        let (locked_low, took_high) = (std::sync::Barrier::new(2), std::sync::Barrier::new(2));
        std::thread::scope(|scope| {
            let command = scope.spawn(|| {
                let mut gave_way = 0;
                let mut low = locks[1].lock();
                locked_low.wait();
                took_high.wait();
                loop {
                    if let Some(mut high) = locks[2].lock_next_for(&locks[2]) {
                        *low += 100;
                        *high += 100;
                        return gave_way;
                    }
                    gave_way += 1;
                    drop(low);
                    low = locks[1].lock();
                }
            });
            locked_low.wait();
            let turn = Mutex::new(());
            let transaction = transaction(&turn);
            *locks[2].lock() += 10;
            took_high.wait();
            // Below the one it holds, and the command has it.
            *locks[1].lock() += 10;
            drop(transaction);
            assert!(command.join().unwrap() > 0, "the command let go");
        });
        assert_eq!(*locks[1].lock(), 111);
        assert_eq!(*locks[2].lock(), 112);
    }
}
