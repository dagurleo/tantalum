//! Sharded in-memory key/value store with millisecond TTLs and eviction.
//!
//! Each shard is one mutex around a hash table and a set of segments:
//!
//! - Entries are packed back to back into append-only *segments*
//!   (`segment.rs`), 4 KB to 256 KB each and sized to the shard. There is no
//!   heap allocation per key, so no allocator header, no size-class rounding
//!   and no cross-thread fragmentation. Segment memory comes from a pool of OS
//!   pages (`pages.rs`) and only becomes resident as it's written, so memory is
//!   accounted as written pages: a segment's unused tail costs nothing.
//! - The hash table (`table.rs`, bucketized cuckoo hashing) holds 8-byte
//!   slots: 32 bits of the key's hash plus a 32-bit location (segment id and
//!   offset). Comparing the stored hash first means a lookup reads almost no
//!   non-matching entries, and the table can resize without reading any keys.
//!   It runs 75–94% full, ~84% on average.
//! - Overwrites, deletes and expiry only mark the old entry dead. A segment
//!   left entirely dead is freed at once; when dead bytes pass a quarter of
//!   the shard, the deadest segment is compacted (live entries are copied to
//!   the active segment and the old one is freed). New deadlines and integers
//!   of the same length are written in place instead (see `segment.rs`).
//! - Eviction is GCLOCK at segment granularity: reads bump a 2-bit usage
//!   counter in the entry's flags byte. Evicting looks at the shard's oldest
//!   segment: if at least a quarter of it is reclaimable (dead, expired or
//!   unused since its last pass), it's compacted, moving used entries
//!   (counter decremented) to the young end and dropping the rest. Otherwise
//!   it's mostly hot, so it's aged in place instead: counters are decremented
//!   without copying and the segment goes to the back of the line. Hot keys
//!   are never forced out, compaction always frees at least a quarter of a
//!   segment, and a segment can only be aged 3 times before it qualifies.
//! - Entries over `INLINE_MAX` keep key and value in their own refcounted
//!   allocation instead, so compaction never copies big values and readers
//!   can share them without a copy.
//! - Expiry is lazy (checked on access) plus a background sweep that only
//!   scans segments whose earliest deadline has passed.
//! - Memory is accounted per shard under its lock and folded into a global
//!   counter in batches, so writers don't all contend on one atomic. The
//!   batch size scales with `maxmemory`, bounding the global error to ~1.6%.
//! - Eviction picks between the writer's shard and a random one, whichever
//!   holds more ("power of two choices"), which keeps shards evenly filled.
//!   It holds one shard lock at a time and never while the writer holds its
//!   own, so it can't deadlock.

use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use crate::blocking::Waiter;
use crate::collection::{self, Collection};
use crate::entry::KeyValue;
use crate::pages;
pub use crate::segment::Kind;
use crate::segment::{self, Body, Header, MAX_USAGE, Payload, Segment};
use crate::shard_lock::{self, ShardGuard, ShardLock};
use crate::table::{self, Table};

/// Milliseconds on the store's monotonic clock. Starts at 1; 0 means "no expiry".
pub type Millis = u64;

pub const NO_EXPIRY: Millis = 0;

/// Latest representable deadline.
pub const MAX_DEADLINE: Millis = i64::MAX as u64;

/// Values up to this size are copied out while holding the shard lock;
/// larger ones are shared and copied (or written to the socket) afterwards.
pub const COPY_UNDER_LOCK_MAX: usize = 16 * 1024;

/// Entries up to this size (header included) are packed into segments.
/// Larger ones get their own allocation. It stays within
/// `COPY_UNDER_LOCK_MAX`, so packed values are always copied out under the
/// lock, and a segment (at least 8 entries' worth) never exceeds `MAX_SEGMENT`.
pub const INLINE_MAX: usize = 16 * 1024;

/// A location is a segment id in the high bits and a byte offset below.
const OFFSET_BITS: u32 = 18;
const MAX_SEGMENT: usize = 1 << OFFSET_BITS; // 256 KiB
const MIN_SEGMENT: usize = 4 * 1024;
const MAX_SEGMENT_IDS: usize = 1 << (32 - OFFSET_BITS);
const _: () = assert!(MIN_SEGMENT == pages::POOL_MIN && MAX_SEGMENT == pages::POOL_MAX);
const _: () = assert!(INLINE_MAX <= COPY_UNDER_LOCK_MAX && INLINE_MAX * 8 <= MAX_SEGMENT * 4);

/// Accounting granularity for segment memory: an OS page.
const PAGE: usize = 4096;

/// Bytes of RAM a segment with `len` bytes written occupies.
fn resident(len: usize) -> usize {
    len.next_multiple_of(PAGE)
}

/// Bytes of segments the expiry sweep may scan per shard per cycle.
const EXPIRE_SCAN_BYTES: usize = 32 * 1024;

/// Upper bound on a shard's unreported memory delta.
const MAX_FLUSH_BYTES: usize = 64 * 1024;

/// Consecutive fruitless two-choice eviction visits before falling back to
/// sweeping every shard in turn.
const EVICT_TRIES: usize = 64;

/// How many lookups ahead of the running one [`Store::prefetch_ahead`]
/// prefetches buckets, and then entries.
const PREFETCH_AHEAD: usize = 8;
const PREFETCH_ENTRY_AHEAD: usize = 4;

/// The most room left for a big string that has grown to grow further, as Redis
/// leaves up to 1 MB (under that, as much as it holds).
const GROWING_STRING_ROOM: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Condition {
    Always,
    /// `NX`: only set if the key does not exist.
    IfAbsent,
    /// `XX`: only set if the key already exists.
    IfPresent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expiry {
    Never,
    /// Absolute deadline on the store clock, at most [`MAX_DEADLINE`].
    At(Millis),
    /// `KEEPTTL`: keep the existing key's deadline.
    Keep,
}

/// What to do when a write would exceed `maxmemory`. Names match Redis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EvictionPolicy {
    /// Reject writes with an OOM error.
    #[default]
    NoEviction,
    /// Evict the least recently used keys (approximately).
    AllKeysLru,
    /// Like `AllKeysLru`, but only keys with a TTL are candidates.
    VolatileLru,
    /// Evict the least often used keys: by the same use counter as the LRU
    /// policies, which counts up to three uses and loses one each time
    /// eviction passes, so it keeps what's used often as well as lately.
    AllKeysLfu,
    /// Like `AllKeysLfu`, but only keys with a TTL.
    VolatileLfu,
    /// Evict keys regardless of use: a segment picked at random gives up all of its keys.
    AllKeysRandom,
    /// Like `AllKeysRandom`, but only keys with a TTL.
    VolatileRandom,
    /// Evict the keys with a TTL nearest their deadline: of the oldest segment
    /// holding some, the half with the soonest.
    VolatileTtl,
}

/// How a policy picks what goes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Choice {
    /// By the use counter: the LRU and LFU policies.
    Use,
    Random,
    /// Soonest deadline first.
    Soonest,
}

impl EvictionPolicy {
    const ALL: [Self; 8] = [
        Self::NoEviction,
        Self::AllKeysLru,
        Self::VolatileLru,
        Self::AllKeysLfu,
        Self::VolatileLfu,
        Self::AllKeysRandom,
        Self::VolatileRandom,
        Self::VolatileTtl,
    ];

    fn from_u8(n: u8) -> Self {
        Self::ALL
            .get(usize::from(n))
            .copied()
            .unwrap_or(Self::NoEviction)
    }

    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|policy| policy.name().eq_ignore_ascii_case(name))
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::NoEviction => "noeviction",
            Self::AllKeysLru => "allkeys-lru",
            Self::VolatileLru => "volatile-lru",
            Self::AllKeysLfu => "allkeys-lfu",
            Self::VolatileLfu => "volatile-lfu",
            Self::AllKeysRandom => "allkeys-random",
            Self::VolatileRandom => "volatile-random",
            Self::VolatileTtl => "volatile-ttl",
        }
    }

    /// An LFU policy: `OBJECT FREQ` answers only under one, as in Redis.
    pub fn is_lfu(self) -> bool {
        matches!(self, Self::AllKeysLfu | Self::VolatileLfu)
    }

    /// An LRU or LFU policy, which goes by the use counter.
    pub fn counts_uses(self) -> bool {
        self.choice() == Some(Choice::Use)
    }

    /// Only keys with a TTL may go.
    fn volatile_only(self) -> bool {
        matches!(
            self,
            Self::VolatileLru | Self::VolatileLfu | Self::VolatileRandom | Self::VolatileTtl
        )
    }

    /// How it picks what goes; `None` for `noeviction`.
    fn choice(self) -> Option<Choice> {
        match self {
            Self::NoEviction => None,
            Self::AllKeysLru | Self::VolatileLru | Self::AllKeysLfu | Self::VolatileLfu => {
                Some(Choice::Use)
            }
            Self::AllKeysRandom | Self::VolatileRandom => Some(Choice::Random),
            Self::VolatileTtl => Some(Choice::Soonest),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Config {
    /// Rounded up to a power of two. 0 picks a count from `maxmemory`.
    pub shards: usize,
    /// Limit on accounted memory in bytes; 0 means unlimited.
    pub maxmemory: usize,
    pub policy: EvictionPolicy,
    /// Hash keys with a fixed seed instead of a random one, so a benchmark
    /// lays its keys out the same way every run (memory per key is then
    /// exact). Never in production: a random seed stops clients from picking
    /// keys that collide on purpose.
    #[doc(hidden)]
    pub fixed_seed: bool,
    /// Tests only: Unix time in milliseconds at store-clock time 0, in
    /// place of the real one, so time-based IDs are the same every run.
    #[doc(hidden)]
    pub unix_base: Option<u64>,
    /// Run the store clock on Unix milliseconds rather than from 1, so
    /// commands can be run again at the times they first ran at (the
    /// append-only log replays its commands so: see `aof.rs`).
    pub unix_clock: bool,
    /// How many numbered databases there are (`SELECT`); 0 means 16, as in
    /// Redis.
    pub databases: usize,
}

/// What a key and a value of this size take, packed.
pub(crate) fn entry_size(key_len: usize, value_len: usize) -> usize {
    KeyValue::size_for(key_len, value_len)
}

/// A write was refused because it would exceed `maxmemory`.
#[derive(Debug, PartialEq, Eq)]
pub struct OutOfMemory;

/// A value handed out of the store, valid after the shard lock is released.
#[derive(Clone)]
pub struct SharedValue(Shared);

#[derive(Clone)]
enum Shared {
    /// Shares a big value's allocation.
    External(KeyValue),
    /// A copy of a small value.
    Owned(Box<[u8]>),
}

impl SharedValue {
    pub fn as_bytes(&self) -> &[u8] {
        match &self.0 {
            Shared::External(kv) => kv.value(),
            Shared::Owned(bytes) => bytes,
        }
    }
}

/// A big collection and its key, kept by its shard (for `EXTERNAL` entries
/// of collection kinds).
#[derive(Clone)]
struct Object {
    key: Box<[u8]>,
    value: Collection,
}

/// What a removed entry held outside its segment, freed once the shard lock
/// is released.
enum Freed {
    Value(KeyValue),
    Object(#[allow(dead_code)] Box<Object>),
}

/// Borrowed view of a live value, valid while the shard lock is held.
pub struct ValueRef<'a>(Borrowed<'a>, Kind);

enum Borrowed<'a> {
    Inline(&'a [u8]),
    External(&'a KeyValue),
    Object(&'a Collection),
}

/// A value as collection commands read it: bytes (a string, a hash, or a
/// packed collection), or a big collection.
pub enum View<'a> {
    Bytes(&'a [u8]),
    Object(&'a Collection),
}

impl ValueRef<'_> {
    /// What the value holds: commands for one kind reply `WRONGTYPE` to others.
    pub fn kind(&self) -> Kind {
        self.1
    }

    /// The bytes of a string or hash, or of a packed collection. Big
    /// collections have none: check the kind first, or use [`ValueRef::view`].
    pub fn as_bytes(&self) -> &[u8] {
        match self.0 {
            Borrowed::Inline(bytes) => bytes,
            Borrowed::External(kv) => kv.value(),
            Borrowed::Object(_) => panic!("a big collection has no bytes"),
        }
    }

    pub fn view(&self) -> View<'_> {
        match self.0 {
            Borrowed::Inline(bytes) => View::Bytes(bytes),
            Borrowed::External(kv) => View::Bytes(kv.value()),
            Borrowed::Object(collection) => View::Object(collection),
        }
    }

    /// Takes the value out from under the lock: shares big values, copies
    /// small ones. Not for big collections.
    pub fn share(&self) -> SharedValue {
        SharedValue(match self.0 {
            Borrowed::Inline(bytes) => Shared::Owned(bytes.into()),
            Borrowed::External(kv) => Shared::External(kv.clone()),
            Borrowed::Object(_) => panic!("a big collection can't be shared"),
        })
    }
}

/// The `want` smallest slot hashes from `from` on, and where they live, for
/// [`ShardInner::scan`].
struct Smallest {
    heap: BinaryHeap<(u32, u32)>,
    from: u32,
    want: usize,
    /// The largest hash kept once there are `want`: nothing above it can be.
    threshold: u32,
    /// The smallest hash left out at or below the threshold: if it ties with
    /// the last one kept, those keys come too.
    tie: Option<u32>,
}

impl Smallest {
    /// For a table of `len` slots in use, at most.
    fn new(from: u32, want: usize, len: usize) -> Self {
        Smallest {
            heap: BinaryHeap::with_capacity(want.min(len)),
            from,
            want,
            threshold: u32::MAX,
            tie: None,
        }
    }

    /// Hashes in `from..=from + span` may be kept.
    fn span(&self) -> u32 {
        self.threshold.wrapping_sub(self.from)
    }

    fn is_full(&self) -> bool {
        self.heap.len() >= self.want
    }

    fn last(&self) -> Option<u32> {
        self.heap.peek().map(|&(hash, _)| hash)
    }

    fn offer(&mut self, slot: &table::Slot) {
        let hash = slot.hash;
        if slot.loc == table::EMPTY || hash < self.from || hash > self.threshold {
            return;
        }
        if !self.is_full() {
            self.heap.push((hash, slot.loc));
            if self.is_full() {
                self.threshold = self.last().expect("full");
            }
            return;
        }
        let mut top = self.heap.peek_mut().expect("full");
        let out = if hash < top.0 {
            std::mem::replace(&mut *top, (hash, slot.loc)).0
        } else {
            hash
        };
        drop(top);
        self.tie = Some(self.tie.map_or(out, |tie| tie.min(out)));
        self.threshold = self.last().expect("full");
    }
}

/// What [`Store::rename`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Renamed {
    Done,
    /// There's no source key.
    NoSuchKey,
    /// With `only_if_absent`: the destination exists (or is the source).
    Exists,
}

/// Whether two byte strings are equal, at once if they're the same bytes
/// (a shared big value).
fn same_bytes(a: &[u8], b: &[u8]) -> bool {
    (a.as_ptr() == b.as_ptr() && a.len() == b.len()) || a == b
}

pub struct SetOutcome {
    /// Whether the value was written (false when NX/XX prevented it).
    pub applied: bool,
    /// The previous live value, if requested and there was one.
    pub previous: Option<SharedValue>,
    /// With `want_previous`, the key held another kind than a string, so
    /// nothing was written (Redis' `WRONGTYPE`).
    pub wrong_type: bool,
    /// Big values this write displaced, freed when the outcome is dropped
    /// (after the shard lock is released).
    _garbage: Vec<Freed>,
}

/// A live key, as [`Store::update_key`] shows it.
pub struct Current<'a> {
    pub value: ValueRef<'a>,
    /// `Never` or `At`.
    pub expiry: Expiry,
    /// Its use counter, which eviction goes by: 0 to [`MAX_USAGE`].
    pub uses: u8,
}

/// A live key, as [`Store::update_collection`] shows it.
pub struct Entry<'a> {
    pub kind: Kind,
    /// `Never` or `At`.
    pub expiry: Expiry,
    pub value: EntryValue<'a>,
}

pub enum EntryValue<'a> {
    /// A string, a hash, or a packed collection.
    Bytes(&'a [u8]),
    /// A big collection, to change in place if need be.
    Object(&'a mut Collection),
}

fn expiry_of(header: &Header) -> Expiry {
    match header.deadline {
        NO_EXPIRY => Expiry::Never,
        deadline => Expiry::At(deadline),
    }
}

/// Why [`Store::apply`] couldn't write.
enum Retry {
    /// `maxmemory` needs this many bytes freed first.
    NeedRoom(usize),
    /// The shard is out of segment ids (practically unreachable).
    OutOfIds,
}

/// Two keys with their shards locked, from [`Store::update_pair`].
pub struct Pair<'a> {
    store: &'a Store,
    low: &'a mut ShardInner,
    high: Option<&'a mut ShardInner>,
    low_index: usize,
    /// Each key, its slot hash and its shard.
    keys: [(&'a [u8], u32, usize); 2],
    now: Millis,
    garbage: Vec<Freed>,
    /// Whether memory changed in the low and the high shard.
    changed: [bool; 2],
    out_of_ids: bool,
}

impl Pair<'_> {
    /// Shows `f` key `which` (0 or 1, in the order given) as
    /// [`Store::update_collection`] does, and applies its update at once.
    pub fn update<R>(
        &mut self,
        which: usize,
        mut f: impl FnMut(Option<Entry<'_>>) -> (Update, R),
    ) -> R {
        let (key, hash, shard) = self.keys[which];
        let side = usize::from(shard != self.low_index);
        let inner: &mut ShardInner = match self.high.as_deref_mut() {
            Some(high) if side == 1 => high,
            _ => self.low,
        };
        let (existing, removed) = inner.lookup_live(hash, key, self.now, &mut self.garbage);
        let (update, result) =
            inner.with_entry(existing.as_ref().map(|(loc, h)| (*loc, h)), &mut f);
        let absent = existing.is_none();
        match self.store.apply(
            inner,
            hash,
            key,
            existing,
            update,
            self.now,
            false,
            &mut self.garbage,
        ) {
            Ok(applied) => {
                self.changed[side] |= applied || removed;
                if applied && absent && !inner.waiting.is_empty() {
                    self.store.created(inner, key);
                }
            }
            Err(_) => self.out_of_ids = true,
        }
        result
    }
}

/// What [`Store::update_key`] does to the key.
#[derive(Debug, Clone)]
pub enum Update {
    /// Leaves it as it is, and doesn't count as using it.
    Peek,
    /// Leaves it as it is but counts as using it, like a read.
    Touch,
    /// Removes it.
    Delete,
    /// Gives it a new expiry: `Never` removes its TTL, a deadline already
    /// passed removes the key, and `Keep` changes nothing.
    Expire(Expiry),
    /// Sets its value to this integer in decimal, keeping its TTL, or
    /// creates it without one.
    SetInt(i64),
    /// Sets its value to this, of this kind, keeping its TTL, or creates it
    /// without one.
    Replace(Kind, Vec<u8>),
    /// Sets its value to this string, kept apart with some room, keeping its TTL
    /// or creating it without one: for one [`Store::edit_string_room`] will go on
    /// changing where it lies, as its length changes a little at a time.
    ReplaceWithRoom(Vec<u8>),
    /// Sets its value to this big collection, keeping its TTL, or creates it
    /// without one.
    Object(Collection),
    /// Its big collection was changed in place.
    Changed,
    /// It was, in a way clients watching the key don't see, as in Redis: a
    /// stream's consumer groups. (A snapshot does.)
    ChangedUnseen,
}

fn join(id: usize, offset: usize) -> u32 {
    debug_assert!(id < MAX_SEGMENT_IDS && offset < MAX_SEGMENT);
    let loc = ((id as u32) << OFFSET_BITS) | offset as u32;
    debug_assert_ne!(loc, table::EMPTY);
    loc
}

/// A key and its hash, so that a command hashes its key once however many
/// steps (prefetches, then the command itself) look it up.
#[derive(Clone, Copy)]
pub struct Key<'a> {
    bytes: &'a [u8],
    hash: KeyHash,
}

impl Key<'_> {
    pub fn hash(&self) -> KeyHash {
        self.hash
    }
}

/// A key's hash under the store's (randomly seeded) hasher.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyHash(u64);

fn split(loc: u32) -> (usize, usize) {
    (
        (loc >> OFFSET_BITS) as usize,
        (loc & (MAX_SEGMENT as u32 - 1)) as usize,
    )
}

// The same hash as foldhash's `RandomState`, but it can also take a fixed
// seed (`Config::fixed_seed`).
type Hasher = foldhash::fast::SeedableRandomState;

/// The low 32 bits of a key's hash; the shard index comes from the high bits.
fn slot_hash(hasher: &Hasher, key: &[u8]) -> u32 {
    hasher.hash_one(key) as u32
}

/// The middle deadline of a segment's live keys with a TTL, if it has any.
fn middle_due(seg: &Segment) -> Option<Millis> {
    let mut due: Vec<Millis> = seg
        .entries()
        .filter(|(_, header)| !header.is_dead() && header.has_ttl())
        .map(|(_, header)| header.deadline)
        .collect();
    let middle = due.len().checked_sub(1)? / 2;
    Some(*due.select_nth_unstable(middle).1)
}

/// Whether eviction may remove this entry at all (volatile-lru spares keys
/// without a TTL).
fn evictable(volatile_only: bool, header: &Header) -> bool {
    !volatile_only || header.has_ttl()
}

#[derive(Clone, Copy)]
enum Compaction {
    /// Keep every live entry; just reclaim dead space.
    Defrag,
    /// Drop the evictable entries `rule` picks; keep the rest.
    Evict { volatile_only: bool, rule: Rule },
}

/// Which evictable entries a compaction drops.
#[derive(Clone, Copy)]
enum Rule {
    /// Those unused since their last pass (the others lose a use).
    Unused,
    All,
    /// Those due by this deadline.
    DueBy(Millis),
}

/// What eviction asks of a shard: the policy's choice, and a random number for it.
#[derive(Clone, Copy)]
struct Eviction {
    volatile_only: bool,
    choice: Choice,
    pick: u64,
}

// 128-byte alignment keeps each shard's lock on its own cache line (pair), so
// threads hammering neighboring shards don't false-share.
#[repr(align(128))]
struct Shard {
    inner: ShardLock<ShardInner>,
    /// Copies of the shard's footprint and TTL key count, readable without the
    /// lock, used to steer eviction toward shards with the most to give.
    footprint: AtomicUsize,
    with_ttl: AtomicUsize,
    /// The table's [`Table::raw_parts`], so [`Store::prefetch`] can find a
    /// key's buckets without the lock. Updated with the footprint, which
    /// changes whenever the table is reallocated.
    table_base: AtomicUsize,
    table_buckets: AtomicUsize,
}

#[derive(Default)]
struct ShardInner {
    table: Table,
    /// Indexed by segment id; `None` for free ids.
    segments: Vec<Option<Segment>>,
    free_ids: Vec<u32>,
    /// Full segments, oldest first: the eviction order.
    sealed: VecDeque<u32>,
    /// The segment new entries are appended to.
    active: Option<usize>,
    /// Entries too big to pack, indexed by their external index.
    externals: Vec<Option<KeyValue>>,
    free_externals: Vec<u32>,
    /// Big collections, indexed the same way by their entries.
    objects: Vec<Option<Object>>,
    free_objects: Vec<u32>,
    mem: Mem,
    /// Footprint as of the last `sync_usage`, of which `unflushed` hasn't
    /// been added to the global counter yet.
    reported: usize,
    unflushed: isize,
    /// Next segment id the expiry sweep looks at.
    sweep_cursor: usize,
    /// Clients blocked on keys of this shard, by key, first come first.
    waiting: HashMap<Box<[u8]>, VecDeque<Arc<Waiter>>, foldhash::fast::RandomState>,
    /// What hears of this shard's keys changing.
    watched: Notices,
}

/// Where shards that owe a copy of themselves put it (a replica's full sync:
/// [`Store::request_copies`]), by the address of their data: kept apart, so
/// that a shard's own layout doesn't change, and read only while copies are
/// owed (`OnLock`).
static OWED: Mutex<Vec<(usize, Arc<CopySlot>)>> = Mutex::new(Vec::new());

/// A shard's copy, owed and then made ([`Store::request_copies`]).
#[derive(Default)]
pub(crate) struct CopySlot(Mutex<Option<ShardCopy>>);

impl crate::shard_lock::OnLock for ShardInner {
    /// Makes the copy this shard owes, if it owes one, before anything can
    /// change it.
    fn on_lock(&mut self) {
        let me = self as *const ShardInner as usize;
        let slot = {
            let mut owed = OWED.lock();
            let Some(at) = owed.iter().position(|(shard, _)| *shard == me) else {
                return;
            };
            owed.swap_remove(at).1
        };
        // Unless the sync that asked has gone.
        if Arc::strong_count(&slot) > 1 {
            *slot.0.lock() = Some(self.copy_out());
        }
        crate::shard_lock::copy_made();
    }
}

impl ShardInner {
    /// Its data as it is now ([`ShardCopy`]).
    fn copy_out(&self) -> ShardCopy {
        ShardCopy {
            segments: self
                .segments
                .iter()
                .map(|seg| seg.as_ref().map(|seg| seg.buf[..].to_vec()))
                .collect(),
            externals: self.externals.clone(),
            objects: self.objects.clone(),
        }
    }
}

/// What hears of a shard's keys changing: clients watching them (`WATCH`),
/// clients blocked in `XREADGROUP`, and clients caching them (`CLIENT
/// TRACKING`). A change checks one flag, `any`, and costs nothing more while
/// nothing listens.
#[derive(Default)]
struct Notices {
    by_key: HashMap<Box<[u8]>, Vec<Watcher>, foldhash::fast::RandomState>,
    /// While a client tracks in `BCAST` mode: database 0, which queues every
    /// change for the prefixes to hear of after the command.
    broadcast: Option<Arc<Store>>,
    /// While keyspace notifications are on: this shard's database, whose
    /// changes go into the thread's log for them (see `notify.rs`).
    events: Option<u16>,
    /// While replicas may follow (`repl/propagate.rs`): this shard's database,
    /// whose changes count toward what the command propagates, and whose
    /// expirations and evictions propagate as `DEL`s.
    repl: Option<u16>,
    /// Whether any of those has anything.
    any: bool,
}

/// How a key changed, for keyspace notifications.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Change {
    /// Written over, or changed in place.
    Written,
    /// Made: there was no key.
    Created,
    Removed,
    Expired,
    Evicted,
}

/// What hears of one key changing.
enum Watcher {
    /// A client watching it: its flag is set when the key changes.
    Watch {
        flag: Arc<AtomicBool>,
        /// The key had expired already when it was watched, so its removal
        /// isn't a change (as in Redis).
        expired: bool,
        /// Not a client watching but one blocked in `XREADGROUP`, whose group
        /// goes when the key does: a change queues the key for it, so that this
        /// costs other commands only the check for watchers they already make.
        waking: Option<Arc<Store>>,
    },
    /// A client that read it and may cache it: it hears of the next change
    /// (and of no more, unless it reads the key again).
    Track(crate::tracking::Tracked),
}

impl Notices {
    fn update(&mut self) {
        self.any = !self.by_key.is_empty()
            || self.broadcast.is_some()
            || self.events.is_some()
            || self.repl.is_some();
    }

    /// After `key` changed (written, removed, expired or evicted): tells what
    /// hears of it. An expiry isn't a change to watchers that found it expired
    /// already.
    #[inline]
    fn changed(&mut self, key: &[u8], how: Change) {
        if self.any {
            self.mark(key, how);
        }
    }

    #[cold]
    fn mark(&mut self, key: &[u8], how: Change) {
        if let Some(db) = self.repl {
            crate::repl::propagate::changed(db, key, how);
        }
        if let Some(db) = self.events {
            crate::notify::log(db, key, how);
        }
        let by_expiry = how == Change::Expired;
        if let Some(base) = &self.broadcast {
            base.queue_broadcast(key, crate::tracking::modifier(by_expiry));
        }
        let Some(watchers) = self.by_key.get_mut(key) else {
            return;
        };
        let counts = tracking_counts(watchers);
        watchers.retain_mut(|watcher| match watcher {
            Watcher::Watch {
                flag,
                expired,
                waking,
            } => {
                if !(by_expiry && *expired) {
                    flag.store(true, Relaxed);
                    if let Some(store) = waking {
                        store.queue_ready(key);
                    }
                }
                true
            }
            Watcher::Track(entry) => entry.changed(key),
        });
        if let Some(counts) = counts
            && !watchers.iter().any(Watcher::is_track)
        {
            counts.key_gone();
        }
        if watchers.is_empty() {
            self.by_key.remove(key);
            self.update();
        }
    }

    /// For `SWAPDB` and `FLUSHDB`: sets the flags of the clients watching
    /// `key`, as a change would, but tells no one else.
    fn touch(&self, key: &[u8]) {
        for watcher in self.by_key.get(key).into_iter().flatten() {
            if let Watcher::Watch { flag, waking, .. } = watcher {
                flag.store(true, Relaxed);
                if let Some(store) = waking {
                    store.queue_ready(key);
                }
            }
        }
    }

    /// Stops the tracking of `key`, telling those that tracked it: for
    /// `tracking-table-max-keys`. Whether it was tracked.
    fn untrack(&mut self, key: &[u8]) -> bool {
        let Some(watchers) = self.by_key.get_mut(key) else {
            return false;
        };
        let Some(counts) = tracking_counts(watchers) else {
            return false;
        };
        watchers.retain(|watcher| match watcher {
            Watcher::Track(entry) => {
                entry.evicted(key);
                false
            }
            Watcher::Watch { .. } => true,
        });
        counts.key_gone();
        if watchers.is_empty() {
            self.by_key.remove(key);
            self.update();
        }
        true
    }

    /// Drops every tracking entry, telling no one: after a flush, which tells
    /// every tracking client at once.
    fn forget_tracked(&mut self) {
        self.by_key.retain(|_, watchers| {
            if let Some(counts) = tracking_counts(watchers) {
                watchers.retain(|watcher| !watcher.is_track());
                counts.key_gone();
            }
            !watchers.is_empty()
        });
        if self.by_key.is_empty() {
            self.by_key = HashMap::default();
        }
        self.update();
    }
}

impl Watcher {
    fn is_track(&self) -> bool {
        matches!(self, Watcher::Track(_))
    }
}

/// The tracking table's counts, if any of a key's watchers tracks it.
fn tracking_counts(watchers: &[Watcher]) -> Option<Arc<crate::tracking::Counts>> {
    watchers.iter().find_map(|watcher| match watcher {
        Watcher::Track(entry) => Some(entry.counts().clone()),
        Watcher::Watch { .. } => None,
    })
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
struct Mem {
    /// Sum of segment allocations (address space, mostly not resident).
    segment_capacity: usize,
    /// Sum of segments' written pages: what they cost in RAM.
    segment_resident: usize,
    /// Sum of bytes written into segments, live or dead.
    segment_used: usize,
    /// Sum of live entry sizes.
    live: usize,
    /// Sum of external allocations.
    external: usize,
    /// Sum of big collections' sizes.
    objects: usize,
    with_ttl: usize,
    expired: u64,
    evicted: u64,
    /// Changes made by commands (writes, deletes, TTL changes), so a
    /// snapshot can tell whether anything changed since the last one.
    writes: u64,
}

impl ShardInner {
    fn segment(&self, id: usize) -> &Segment {
        self.segments[id].as_ref().expect("live segment")
    }

    fn segment_mut(&mut self, id: usize) -> &mut Segment {
        self.segments[id].as_mut().expect("live segment")
    }

    /// Key bytes of the entry described by `header` in `buf`.
    fn entry_key<'a>(
        buf: &'a [u8],
        header: &Header,
        externals: &'a [Option<KeyValue>],
        objects: &'a [Option<Object>],
    ) -> &'a [u8] {
        match &header.body {
            Body::Inline { key, .. } => &buf[key.clone()],
            Body::External(i) if header.kind().is_collection() => {
                &objects[*i as usize].as_ref().expect("live object").key
            }
            Body::External(i) => externals[*i as usize]
                .as_ref()
                .expect("live external")
                .key(),
        }
    }

    /// Finds the live or expired (not yet removed) entry for `key`.
    fn lookup(&self, hash: u32, key: &[u8]) -> Option<(u32, Header)> {
        let mut found = None;
        self.table.find(hash, |slot| {
            let (id, offset) = split(slot.loc);
            let buf = &self.segment(id).buf;
            let header = segment::parse(buf, offset);
            if Self::entry_key(buf, &header, &self.externals, &self.objects) != key {
                return false;
            }
            found = Some((slot.loc, header));
            true
        });
        found
    }

    /// [`ShardInner::lookup`], removing the entry if it has expired (its
    /// memory into `garbage`). Also returns whether it removed one.
    fn lookup_live(
        &mut self,
        hash: u32,
        key: &[u8],
        now: Millis,
        garbage: &mut Vec<Freed>,
    ) -> (Option<(u32, Header)>, bool) {
        let existing = self.lookup(hash, key);
        if let Some((loc, header)) = &existing
            && header.is_expired(now)
        {
            if self.keeps_expired(key) {
                return (None, false);
            }
            self.watched.changed(key, Change::Expired);
            self.unlink(hash, *loc);
            garbage.extend(self.kill(*loc));
            self.mem.expired += 1;
            return (None, true);
        }
        (existing, false)
    }

    /// Whether an expired key a lookup finds must stay: while replicating
    /// (see [`Store::set_replicating`]), unless this thread is in a write's
    /// turn, which propagates its removal as a `DEL` in order. A read that
    /// leaves it sees no key; the background sweep or a write removes it.
    #[cold]
    fn keeps_expired(&self, key: &[u8]) -> bool {
        match self.watched.repl {
            Some(db) if !crate::aof::logging() => {
                crate::repl::propagate::expired_read(db, key);
                true
            }
            _ => false,
        }
    }

    /// Calls `f` with the entry as [`Store::update_collection`] shows it,
    /// counting any change in size of a big collection it changes.
    fn with_entry<R>(
        &mut self,
        existing: Option<(u32, &Header)>,
        f: &mut impl FnMut(Option<Entry<'_>>) -> (Update, R),
    ) -> (Update, R) {
        let Some((loc, header)) = existing else {
            return f(None);
        };
        let (kind, expiry) = (header.kind(), expiry_of(header));
        if kind.is_collection()
            && let Body::External(i) = header.body
        {
            let object = &mut self.objects[i as usize]
                .as_mut()
                .expect("live object")
                .value;
            let before = object.allocation_size();
            let result = f(Some(Entry {
                kind,
                expiry,
                value: EntryValue::Object(object),
            }));
            let after = self.objects[i as usize]
                .as_ref()
                .expect("live object")
                .value
                .allocation_size();
            self.mem.objects = self.mem.objects + after - before;
            return result;
        }
        let value = match self.value_ref(loc, header).0 {
            Borrowed::Inline(bytes) => bytes,
            Borrowed::External(kv) => kv.value(),
            Borrowed::Object(_) => unreachable!("objects are collections"),
        };
        f(Some(Entry {
            kind,
            expiry,
            value: EntryValue::Bytes(value),
        }))
    }

    fn value_ref(&self, loc: u32, header: &Header) -> ValueRef<'_> {
        let value = match &header.body {
            Body::Inline { value, .. } => {
                Borrowed::Inline(&self.segment(split(loc).0).buf[value.clone()])
            }
            Body::External(i) if header.kind().is_collection() => Borrowed::Object(
                &self.objects[*i as usize]
                    .as_ref()
                    .expect("live object")
                    .value,
            ),
            Body::External(i) => {
                Borrowed::External(self.externals[*i as usize].as_ref().expect("live external"))
            }
        };
        ValueRef(value, header.kind())
    }

    /// Bumps an entry's usage counter.
    fn touch(&mut self, loc: u32, header: &Header) {
        let usage = header.usage();
        if usage < MAX_USAGE {
            let (id, offset) = split(loc);
            self.segment_mut(id).buf[offset] = segment::with_usage(header.flags, usage + 1);
        }
    }

    /// Points the slot at `old` to `new`.
    fn relink(&mut self, hash: u32, old: u32, new: u32) -> bool {
        self.table.replace_loc(hash, old, new)
    }

    fn unlink(&mut self, hash: u32, loc: u32) -> bool {
        self.table.remove(hash, loc)
    }

    /// Size for a new segment: a sixteenth of the live data, so a shard has
    /// about that many segments, but at least 8 entries of this size.
    fn next_segment_size(&self, entry_len: usize) -> usize {
        (self.mem.live / 16)
            .max(entry_len * 8)
            .clamp(MIN_SEGMENT, MAX_SEGMENT)
            .next_power_of_two()
            .min(MAX_SEGMENT)
    }

    /// The active segment if it has `len` bytes free, or a fresh one.
    /// Returns `None` only if the shard has run out of segment ids.
    fn active_with_room(&mut self, len: usize) -> Option<usize> {
        if let Some(id) = self.active {
            if self.segment(id).room() >= len {
                return Some(id);
            }
            self.active = None;
            if self.segment(id).live == 0 {
                self.release(id);
            } else {
                self.sealed.push_back(id as u32);
            }
        }
        let id = match self.free_ids.pop() {
            Some(id) => id as usize,
            None if self.segments.len() < MAX_SEGMENT_IDS => {
                self.segments.push(None);
                self.segments.len() - 1
            }
            None => return None,
        };
        let size = self.next_segment_size(len);
        self.segments[id] = Some(Segment::new(size));
        self.mem.segment_capacity += size;
        self.active = Some(id);
        Some(id)
    }

    /// Frees a segment with no live entries.
    fn release(&mut self, id: usize) {
        let seg = self.segments[id].take().expect("live segment");
        debug_assert_eq!(seg.live, 0);
        self.mem.segment_capacity -= seg.capacity();
        self.mem.segment_resident -= resident(seg.buf.len());
        self.mem.segment_used -= seg.buf.len();
        if let Some(pos) = self.sealed.iter().position(|&s| s as usize == id) {
            self.sealed.remove(pos);
        }
        if self.active == Some(id) {
            self.active = None;
        }
        self.free_ids.push(id as u32);
    }

    fn count_appended(&mut self, id: usize, len: usize, deadline: Millis) {
        let seg = self.segment_mut(id);
        let written = seg.buf.len();
        let resident_growth = resident(written) - resident(written - len);
        seg.live += len;
        if deadline != NO_EXPIRY {
            seg.ttl_live += 1;
            seg.min_deadline = match seg.min_deadline {
                NO_EXPIRY => deadline,
                min => min.min(deadline),
            };
            self.mem.with_ttl += 1;
        }
        self.mem.live += len;
        self.mem.segment_used += len;
        self.mem.segment_resident += resident_growth;
    }

    fn append(
        &mut self,
        kind: Kind,
        usage: u8,
        payload: &Payload<'_>,
        deadline: Millis,
    ) -> Option<u32> {
        let len = segment::encoded_len(payload, deadline);
        let id = self.active_with_room(len)?;
        let buf = &mut self.segment_mut(id).buf;
        let offset = buf.len();
        segment::encode(buf, kind, usage, payload, deadline);
        debug_assert_eq!(buf.len() - offset, len);
        self.count_appended(id, len, deadline);
        Some(join(id, offset))
    }

    /// Copies an already encoded entry, with new flags, to the active segment.
    fn append_raw(&mut self, entry: &[u8], flags: u8, deadline: Millis) -> Option<u32> {
        let id = self.active_with_room(entry.len())?;
        let buf = &mut self.segment_mut(id).buf;
        let offset = buf.len();
        buf.extend_from_slice(entry);
        buf[offset] = flags;
        self.count_appended(id, entry.len(), deadline);
        Some(join(id, offset))
    }

    fn store_external(&mut self, kv: KeyValue) -> u32 {
        self.mem.external += kv.allocation_size();
        match self.free_externals.pop() {
            Some(index) => {
                self.externals[index as usize] = Some(kv);
                index
            }
            None => {
                self.externals.push(Some(kv));
                (self.externals.len() - 1) as u32
            }
        }
    }

    fn take_external(&mut self, index: u32) -> KeyValue {
        let kv = self.externals[index as usize]
            .take()
            .expect("live external");
        self.mem.external -= kv.allocation_size();
        self.free_externals.push(index);
        kv
    }

    fn store_object(&mut self, key: &[u8], value: Collection) -> u32 {
        self.mem.objects += value.allocation_size();
        let object = Some(Object {
            key: key.into(),
            value,
        });
        match self.free_objects.pop() {
            Some(index) => {
                self.objects[index as usize] = object;
                index
            }
            None => {
                self.objects.push(object);
                (self.objects.len() - 1) as u32
            }
        }
    }

    fn take_object(&mut self, index: u32) -> Object {
        let object = self.objects[index as usize].take().expect("live object");
        self.mem.objects -= object.value.allocation_size();
        self.free_objects.push(index);
        object
    }

    /// Puts an object taken with `take_object` back at `index`.
    fn put_object(&mut self, index: u32, key: &[u8], value: Collection) {
        self.mem.objects += value.allocation_size();
        self.objects[index as usize] = Some(Object {
            key: key.into(),
            value,
        });
        self.free_objects.retain(|&free| free != index);
    }

    /// What an `EXTERNAL` entry of `kind` holds outside its segment, taken
    /// out to be freed.
    fn take_outside(&mut self, kind: Kind, index: u32) -> Freed {
        if kind.is_collection() {
            Freed::Object(Box::new(self.take_object(index)))
        } else {
            Freed::Value(self.take_external(index))
        }
    }

    /// The index `store_external` will hand out next.
    fn next_external_index(&self) -> u32 {
        self.free_externals
            .last()
            .copied()
            .unwrap_or(self.externals.len() as u32)
    }

    /// Marks the (unlinked) entry at `loc` dead, freeing its segment if that
    /// was the last live entry. Returns its external allocation, if any, for
    /// the caller to free outside the lock or hand out.
    fn kill(&mut self, loc: u32) -> Option<Freed> {
        let header = self.mark_dead(loc);
        match header.body {
            Body::External(index) => Some(self.take_outside(header.kind(), index)),
            Body::Inline { .. } => None,
        }
    }

    /// [`ShardInner::kill`], except that an external allocation stays, for
    /// a newer entry that refers to it.
    fn mark_dead(&mut self, loc: u32) -> Header {
        let (id, offset) = split(loc);
        let seg = self.segment_mut(id);
        let header = segment::parse(&seg.buf, offset);
        debug_assert!(!header.is_dead());
        seg.buf[offset] |= segment::DEAD;
        seg.live -= header.len;
        if header.has_ttl() {
            seg.ttl_live -= 1;
            if seg.ttl_live == 0 {
                seg.min_deadline = NO_EXPIRY;
            }
        }
        let empty = seg.live == 0;
        self.mem.live -= header.len;
        if header.has_ttl() {
            self.mem.with_ttl -= 1;
        }
        if empty && self.active != Some(id) {
            self.release(id);
        }
        header
    }

    /// Points the key at `hash` to its new entry `new`: instead of `old`,
    /// which is killed (returning its external allocation, if any), or as a
    /// new key.
    fn replace(&mut self, hash: u32, old: Option<u32>, new: u32) -> Option<Freed> {
        match old {
            Some(old) => {
                let linked = self.relink(hash, old, new);
                debug_assert!(linked, "live entry without a slot");
                self.kill(old)
            }
            None => {
                self.table.insert(hash, new);
                None
            }
        }
    }

    /// Gives the live entry at `loc` a new deadline (`NO_EXPIRY` for none).
    /// Done in place when it has a deadline field wide enough; otherwise
    /// the entry is copied with the new deadline.
    fn set_deadline(&mut self, hash: u32, key: &[u8], loc: u32, header: &Header, deadline: Millis) {
        if header.deadline == deadline {
            return;
        }
        let (id, offset) = split(loc);
        let seg = self.segments[id].as_mut().expect("live segment");
        if let Some(field) = segment::deadline_field(&seg.buf, offset)
            && segment::write_deadline(&mut seg.buf[field], deadline)
        {
            match (header.has_ttl(), deadline != NO_EXPIRY) {
                (false, true) => {
                    seg.ttl_live += 1;
                    self.mem.with_ttl += 1;
                }
                (true, false) => {
                    seg.ttl_live -= 1;
                    if seg.ttl_live == 0 {
                        seg.min_deadline = NO_EXPIRY;
                    }
                    self.mem.with_ttl -= 1;
                }
                _ => {}
            }
            if deadline != NO_EXPIRY {
                seg.min_deadline = match seg.min_deadline {
                    NO_EXPIRY => deadline,
                    min => min.min(deadline),
                };
            }
            return;
        }
        let value;
        let payload = match &header.body {
            Body::Inline { value: range, .. } => {
                value = seg.buf[range.clone()].to_vec();
                Payload::Inline { key, value: &value }
            }
            // The new entry takes over the allocation.
            Body::External(index) => Payload::External(*index),
        };
        // Out of segment ids (practically unreachable): keep the old deadline.
        if let Some(new) = self.append(header.kind(), header.usage(), &payload, deadline) {
            let linked = self.relink(hash, loc, new);
            debug_assert!(linked, "live entry without a slot");
            self.mark_dead(loc);
        }
    }

    /// Moves the live entries of segment `id` to the active segment (or drops
    /// them, depending on `mode`) and frees it.
    fn compact(
        &mut self,
        id: usize,
        mode: Compaction,
        now: Millis,
        hasher: &Hasher,
        garbage: &mut Vec<Freed>,
    ) -> usize {
        if self.active == Some(id) {
            self.active = None;
        }
        if let Some(pos) = self.sealed.iter().position(|&s| s as usize == id) {
            self.sealed.remove(pos);
        }
        // Take it out so entries can be read while others are appended. Its id
        // isn't freed until the end, so no new entry can reuse its locations.
        let seg = self.segments[id].take().expect("live segment");
        let mut dropped = 0;
        self.mem.segment_capacity -= seg.capacity();
        self.mem.segment_resident -= resident(seg.buf.len());
        self.mem.segment_used -= seg.buf.len();
        self.mem.live -= seg.live;
        self.mem.with_ttl -= seg.ttl_live;
        for (offset, header) in seg.entries() {
            if header.is_dead() {
                continue;
            }
            let old = join(id, offset);
            let hash = slot_hash(
                hasher,
                Self::entry_key(&seg.buf, &header, &self.externals, &self.objects),
            );
            let expired = header.is_expired(now);
            let usage = header.usage();
            let keep = !expired
                && match mode {
                    Compaction::Defrag => true,
                    Compaction::Evict {
                        volatile_only,
                        rule,
                    } => {
                        !evictable(volatile_only, &header)
                            || match rule {
                                Rule::Unused => usage > 0,
                                Rule::All => false,
                                Rule::DueBy(cutoff) => header.deadline > cutoff,
                            }
                    }
                };
            if keep {
                let usage = match mode {
                    Compaction::Evict {
                        rule: Rule::Unused, ..
                    } => usage.saturating_sub(1),
                    Compaction::Evict { .. } | Compaction::Defrag => usage,
                };
                let bytes = &seg.buf[offset..offset + header.len];
                let flags = segment::with_usage(header.flags, usage);
                if let Some(new) = self.append_raw(bytes, flags, header.deadline) {
                    let linked = self.relink(hash, old, new);
                    debug_assert!(linked, "live entry without a slot");
                    continue;
                }
                // Out of segment ids (practically unreachable): drop it instead.
            }
            let key = Self::entry_key(&seg.buf, &header, &self.externals, &self.objects);
            if !expired {
                // Running the log's commands again doesn't evict the same
                // keys: it says which went.
                crate::aof::evicted(key);
            }
            self.watched.changed(
                key,
                if expired {
                    Change::Expired
                } else {
                    Change::Evicted
                },
            );
            let unlinked = self.unlink(hash, old);
            debug_assert!(unlinked, "live entry without a slot");
            if let Body::External(index) = header.body {
                garbage.push(self.take_outside(header.kind(), index));
            }
            if expired {
                self.mem.expired += 1;
            } else {
                self.mem.evicted += 1;
            }
            dropped += header.len;
        }
        self.free_ids.push(id as u32);
        dropped
    }

    /// Makes eviction progress in this shard. By use (the LRU and LFU
    /// policies), it compacts the oldest suitable segment if enough of it is
    /// reclaimable, otherwise ages it in place. In `thorough` mode it keeps
    /// going through every segment (the active one included) until one
    /// compacts. Returns the bytes reclaimed, 0 if none were (everything looked
    /// at was aged, or nothing was suitable).
    fn evict_segment(
        &mut self,
        how: Eviction,
        thorough: bool,
        now: Millis,
        hasher: &Hasher,
        garbage: &mut Vec<Freed>,
    ) -> usize {
        let volatile_only = how.volatile_only;
        if how.choice != Choice::Use {
            return self.evict_unaged(how, thorough, now, hasher, garbage);
        }
        let suitable = |seg: &Segment| seg.live > 0 && (!volatile_only || seg.ttl_live > 0);
        let rounds = if thorough { self.sealed.len() + 1 } else { 1 };
        for round in 0..rounds {
            // Aged segments move to the back, so this walks oldest to newest.
            let victim = self
                .sealed
                .iter()
                .map(|&id| id as usize)
                .find(|&id| suitable(self.segment(id)))
                .or_else(|| {
                    let last_round = round + 1 == rounds;
                    self.active
                        .filter(|&id| thorough && last_round && suitable(self.segment(id)))
                });
            let Some(id) = victim else {
                return 0;
            };
            let seg = self.segment(id);
            let mut reclaimable = seg.buf.len() - seg.live;
            for (_, header) in seg.entries() {
                let unused = header.is_expired(now) || header.usage() == 0;
                if !header.is_dead() && unused && evictable(volatile_only, &header) {
                    reclaimable += header.len;
                }
            }
            if reclaimable * 4 >= seg.buf.len() {
                let rule = Rule::Unused;
                self.compact(
                    id,
                    Compaction::Evict {
                        volatile_only,
                        rule,
                    },
                    now,
                    hasher,
                    garbage,
                );
                return reclaimable;
            }
            self.age(id, volatile_only);
        }
        0
    }

    /// [`ShardInner::evict_segment`] for the random and `volatile-ttl`
    /// policies, which age nothing, so whichever segment they pick gives up
    /// keys: a random one all of its evictable keys, the oldest with TTL keys
    /// the half of them nearest their deadline. The active segment is a
    /// candidate only when `thorough`.
    fn evict_unaged(
        &mut self,
        how: Eviction,
        thorough: bool,
        now: Millis,
        hasher: &Hasher,
        garbage: &mut Vec<Freed>,
    ) -> usize {
        let volatile_only = how.volatile_only;
        let suitable = |seg: &Segment| seg.live > 0 && (!volatile_only || seg.ttl_live > 0);
        // Fresh keys may be due soonest, so `volatile-ttl` looks at the active one too.
        let active = self
            .active
            .filter(|_| thorough || how.choice == Choice::Soonest);
        let ids: Vec<usize> = self
            .sealed
            .iter()
            .map(|&id| id as usize)
            .chain(active)
            .filter(|&id| suitable(self.segment(id)))
            .collect();
        if ids.is_empty() {
            return 0;
        }
        let pick = |n: u64| ids[(n % ids.len() as u64) as usize];
        let (id, rule) = match how.choice {
            Choice::Soonest => {
                // Of the oldest, the newest and two at random, the one whose
                // TTL keys are due soonest, by the middle one; its half due
                // soonest goes.
                let looks = [
                    ids[0],
                    ids[ids.len() - 1],
                    pick(how.pick),
                    pick(how.pick >> 24),
                ];
                let Some((due, id)) = looks
                    .into_iter()
                    .filter_map(|id| Some((middle_due(self.segment(id))?, id)))
                    .min()
                else {
                    return 0;
                };
                (id, Rule::DueBy(due))
            }
            Choice::Random | Choice::Use => (pick(how.pick), Rule::All),
        };
        let seg = self.segment(id);
        let mut reclaimable = seg.buf.len() - seg.live;
        for (_, header) in seg.entries() {
            let chosen = match rule {
                Rule::DueBy(cutoff) => header.deadline <= cutoff,
                Rule::All | Rule::Unused => true,
            };
            let goes = header.is_expired(now) || (chosen && evictable(volatile_only, &header));
            if !header.is_dead() && goes {
                reclaimable += header.len;
            }
        }
        self.compact(
            id,
            Compaction::Evict {
                volatile_only,
                rule,
            },
            now,
            hasher,
            garbage,
        );
        reclaimable
    }

    /// Decrements the usage counters of segment `id` in place and moves it to
    /// the back of the eviction order.
    fn age(&mut self, id: usize, volatile_only: bool) {
        let seg = self.segment_mut(id);
        let mut offset = 0;
        while offset < seg.buf.len() {
            let header = segment::parse(&seg.buf, offset);
            let usage = header.usage();
            if !header.is_dead() && usage > 0 && evictable(volatile_only, &header) {
                seg.buf[offset] = segment::with_usage(header.flags, usage - 1);
            }
            offset += header.len;
        }
        if let Some(pos) = self.sealed.iter().position(|&s| s as usize == id) {
            self.sealed.remove(pos);
            self.sealed.push_back(id as u32);
        }
    }

    /// Compacts the deadest sealed segment once dead bytes pass a quarter of
    /// the shard's segment memory, or a tenth when the store is near its
    /// memory limit (`eager`): there memory is scarcer than the CPU spent
    /// copying.
    fn maybe_defrag(
        &mut self,
        eager: bool,
        now: Millis,
        hasher: &Hasher,
        garbage: &mut Vec<Freed>,
    ) {
        let divisor = if eager { 10 } else { 4 };
        let dead = self.mem.segment_used - self.mem.live;
        if dead < MIN_SEGMENT || dead * divisor < self.mem.segment_resident {
            return;
        }
        let dead_bytes = |seg: &Segment| seg.buf.len() - seg.live;
        let victim = self
            .sealed
            .iter()
            .map(|&id| id as usize)
            .max_by_key(|&id| dead_bytes(self.segment(id)));
        if let Some(id) = victim {
            let seg = self.segment(id);
            if dead_bytes(seg) * divisor >= seg.buf.len() {
                self.compact(id, Compaction::Defrag, now, hasher, garbage);
            }
        }
    }

    /// Removes expired entries from segments whose earliest deadline has
    /// passed, scanning at most `EXPIRE_SCAN_BYTES`. Returns how many.
    fn sweep_expired(&mut self, now: Millis, hasher: &Hasher, garbage: &mut Vec<Freed>) -> usize {
        let count = self.segments.len();
        let mut budget = EXPIRE_SCAN_BYTES;
        let mut removed = 0;
        for _ in 0..count {
            if budget == 0 {
                break;
            }
            let id = self.sweep_cursor % count;
            self.sweep_cursor = id + 1;
            let Some(seg) = self.segments[id].as_ref() else {
                continue;
            };
            if seg.ttl_live == 0 || seg.min_deadline > now {
                continue;
            }
            budget = budget.saturating_sub(seg.buf.len());
            // Collect first: removing entries can free the segment.
            let mut due = Vec::new();
            let mut next = NO_EXPIRY;
            for (offset, header) in seg.entries() {
                if header.is_dead() || !header.has_ttl() {
                    continue;
                }
                if header.deadline <= now {
                    let key = Self::entry_key(&seg.buf, &header, &self.externals, &self.objects);
                    self.watched.changed(key, Change::Expired);
                    due.push((offset, slot_hash(hasher, key)));
                } else if next == NO_EXPIRY || header.deadline < next {
                    next = header.deadline;
                }
            }
            self.segment_mut(id).min_deadline = next;
            for (offset, hash) in due {
                let loc = join(id, offset);
                let unlinked = self.unlink(hash, loc);
                debug_assert!(unlinked, "live entry without a slot");
                garbage.extend(self.kill(loc));
                self.mem.expired += 1;
                removed += 1;
            }
        }
        removed
    }

    /// Gives memory back after a mass expiry or eviction: frees an emptied
    /// active segment (it was sized for the old data; the next write gets a
    /// right-sized one) and shrinks a mostly empty table.
    fn shrink_if_sparse(&mut self) {
        if let Some(id) = self.active
            && self.segment(id).live == 0
        {
            self.release(id);
        }
        self.table.shrink_if_sparse();
    }

    /// One step of [`Store::scan`] in this shard: the keys with the `want`
    /// smallest slot hashes from `from` on (more on a tie: every key with
    /// the last of those hashes), expired ones counted but not passed to
    /// `f`. Returns how many it counted and the hash to go on from, or
    /// `None` if none are left.
    fn scan(
        &self,
        from: u32,
        want: usize,
        now: Millis,
        f: &mut impl FnMut(&[u8], Kind),
    ) -> (usize, Option<u32>) {
        let mut picked = Smallest::new(from, want, self.table.len());
        // A bucket at a time, checked in one vectorizable pass for a slot
        // that could be picked; once there are `want`, that's rare.
        for bucket in self.table.buckets() {
            let span = picked.span();
            let mut any = false;
            for slot in bucket {
                any |= (slot.hash.wrapping_sub(from) <= span) & (slot.loc != table::EMPTY);
            }
            if any {
                bucket.iter().for_each(|slot| picked.offer(slot));
            }
        }
        self.table
            .overflow()
            .iter()
            .for_each(|slot| picked.offer(slot));
        let Some(last) = picked.last() else {
            return (0, None);
        };
        // A full set may have left keys out; if not, the next step finds
        // none and moves on.
        let more = picked.is_full();
        let tie = picked.tie == Some(last);
        let mut taken = picked.heap.into_vec();
        if tie {
            for slot in self.table.iter().filter(|slot| slot.hash == last) {
                if !taken.contains(&(slot.hash, slot.loc)) {
                    taken.push((slot.hash, slot.loc));
                }
            }
        }
        for &(_, loc) in &taken {
            let (id, offset) = split(loc);
            let buf = &self.segment(id).buf;
            let header = segment::parse(buf, offset);
            if !header.is_expired(now) {
                f(
                    Self::entry_key(buf, &header, &self.externals, &self.objects),
                    header.kind(),
                );
            }
        }
        let next = if more { last.checked_add(1) } else { None };
        (taken.len(), next)
    }

    /// Bytes this shard is responsible for.
    fn footprint(&self) -> usize {
        let small_vecs = (self.free_ids.capacity()
            + self.free_externals.capacity()
            + self.free_objects.capacity()
            + self.sealed.capacity())
            * size_of::<u32>()
            + self.segments.capacity() * size_of::<Option<Segment>>()
            + self.externals.capacity() * size_of::<Option<KeyValue>>()
            + self.objects.capacity() * size_of::<Option<Object>>();
        self.mem.segment_resident
            + self.mem.external
            + self.mem.objects
            + pages::reserved(self.table.allocation_size())
            + small_vecs
    }
}

thread_local! {
    /// Writes on this thread go past `maxmemory` (a function flagged `allow-oom`
    /// is running): looked at only when a write doesn't fit.
    static ALLOW_OOM: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Runs `f` with writes allowed past `maxmemory` on this thread.
pub(crate) fn allowing_oom<R>(f: impl FnOnce() -> R) -> R {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            ALLOW_OOM.set(self.0);
        }
    }
    let _reset = Reset(ALLOW_OOM.replace(true));
    f()
}

pub struct Store {
    shards: Box<[Shard]>,
    shard_mask: usize,
    hasher: Hasher,
    epoch: Instant,
    /// The store clock at `epoch`: 1, or the Unix time then.
    clock_start: u64,
    /// Unix time in milliseconds at store-clock time 0.
    unix_base: u64,
    /// Loading data (replaying the append-only log): writes aren't held to
    /// `maxmemory`, as the evictions they made are in the log.
    loading: AtomicBool,
    /// `CONFIG SET` can change these two, in every database.
    maxmemory: AtomicUsize,
    policy: AtomicU8,
    /// Sum of all shards' flushed footprints.
    used: AtomicI64,
    /// Connection buffers, counted against `maxmemory` too (see
    /// [`Store::add_client_memory`]).
    client_memory: AtomicI64,
    flush_bytes: AtomicUsize,
    ready: Ready,
    /// What transactions take turns by (see [`Store::exclusive`]), in database 0.
    transactions: Mutex<()>,
    /// Which numbered database this is (`SELECT`).
    index: u32,
    /// Whether some client tracks in `BCAST` mode (in database 0).
    broadcasting: Mutex<bool>,
    /// Database 0, to any other. Each database is a store of its own but for
    /// the shard locks, which all use database 0's so that what holds a shard
    /// holds that shard of every one: `EXEC`, scripts and `MOVE` hold what
    /// they touch wherever it is, and what's in other databases costs database 0
    /// nothing. Database 0 also counts for all of them: memory, keys ready for
    /// blocked clients, and whether it's loading. (This and `others` keep
    /// each other alive, which is for as long as the server runs.)
    base: Option<Arc<Store>>,
    /// The other databases, to database 0, made when first used.
    others: Box<[OnceLock<Arc<Store>>]>,
    /// In database 0: keyspace notifications are on, so every change is
    /// logged for them (see `set_notifying`).
    notifying: AtomicBool,
    /// In database 0: replicas may follow this server, or it follows a master
    /// (see `set_replicating`). Expired keys are then removed only by writes,
    /// which propagate the removal in order: a read that finds one leaves it.
    replicating: AtomicBool,
    /// Function libraries' code (`FUNCTION LOAD`), in database 0, kept with the
    /// data for snapshots and the log, with a version that goes up as it
    /// changes. `FLUSHALL` leaves it, as Redis does.
    functions: Mutex<(u64, Vec<Arc<[u8]>>)>,
}

/// `n` empty shards; changes in them are queued for `broadcast` (database 0) if
/// some client tracks in `BCAST` mode, and logged as database `events`'s
/// while keyspace notifications are on.
fn new_shards(
    n: usize,
    broadcast: Option<&Arc<Store>>,
    events: Option<u16>,
    repl: Option<u16>,
) -> Box<[Shard]> {
    (0..n)
        .map(|_| Shard {
            inner: ShardLock::new(ShardInner {
                watched: Notices {
                    broadcast: broadcast.cloned(),
                    events,
                    repl,
                    any: broadcast.is_some() || events.is_some() || repl.is_some(),
                    ..Notices::default()
                },
                ..ShardInner::default()
            }),
            footprint: AtomicUsize::new(0),
            with_ttl: AtomicUsize::new(0),
            table_base: AtomicUsize::new(0),
            table_buckets: AtomicUsize::new(0),
        })
        .collect()
}

/// The shards a native script holds while it runs (`Store::script_shards`).
pub(crate) struct ScriptShards<'a>(Shards<'a>);

enum Shards<'a> {
    /// This thread runs a transaction, which holds what it uses as it uses it.
    Transaction,
    Planned {
        store: &'a Store,
        held: shard_lock::Partial<'a, ShardInner>,
        /// Every shard asked for, held or not.
        wanted: Vec<usize>,
    },
}

impl ScriptShards<'_> {
    /// Holds the shards of `keys` too. False if one couldn't be taken
    /// without waiting long for it while holding others: the script must start
    /// over (it can't have written yet) holding [`wanted`](Self::wanted).
    pub(crate) fn hold(&mut self, keys: &[&[u8]]) -> bool {
        let Shards::Planned {
            store,
            held,
            wanted,
        } = &mut self.0
        else {
            return true;
        };
        let mut ok = true;
        for key in keys {
            let (_, index) = store.locate(store.key(key).hash);
            if !wanted.contains(&index) {
                wanted.push(index);
            }
            if ok && !held.take(index, &store.shards[index].inner) {
                ok = false;
            }
        }
        ok
    }

    /// Every shard asked for.
    pub(crate) fn wanted(&self) -> &[usize] {
        match &self.0 {
            Shards::Transaction => &[],
            Shards::Planned { wanted, .. } => wanted,
        }
    }
}

/// Keys created while clients were blocked on them, to serve those
/// clients (see `blocking.rs`). On its own cache line: commands read
/// `pending` after running.
#[derive(Default)]
#[repr(align(128))]
struct Ready {
    pending: AtomicBool,
    keys: Mutex<ReadyQueue>,
    /// Clients blocked right now.
    blocked: AtomicI64,
    /// And what each is blocked in, for `CLIENT UNBLOCK`.
    waiters: Mutex<HashMap<u64, Arc<Waiter>>>,
}

/// What commands see to when they end, under one lock so that `pending`
/// can't be cleared while either has something.
#[derive(Default)]
struct ReadyQueue {
    /// Keys created while clients were blocked on them, each with its database.
    keys: Vec<(u32, Box<[u8]>)>,
    /// Keys changed while some client tracks in `BCAST` mode, each with who
    /// changed it (see `tracking::modifier`).
    broadcast: Vec<(Box<[u8]>, u64)>,
}

impl Store {
    /// Creates an unlimited store with `shards` shards.
    pub fn new(shards: usize) -> Self {
        Self::with_config(Config {
            shards,
            ..Config::default()
        })
    }

    pub fn with_config(config: Config) -> Self {
        let unix_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);
        let n = match config.shards {
            // Plenty for spreading locks across threads. More shards would
            // mean smaller tables, and tables under ~18 KB can't be mapped
            // from the OS cheaply (see `pages.rs`), so their old copies would
            // linger after every resize. With a memory limit, each shard
            // should also get a few MB so its segments aren't tiny.
            0 if config.maxmemory > 0 => (config.maxmemory >> 22).clamp(16, 256),
            0 => 256,
            n => n,
        }
        .clamp(1, 1 << 16)
        .next_power_of_two();
        // Each shard may hold back up to `flush_bytes`, so with a limit the
        // global counter is within maxmemory / 64 of the truth.
        let flush_bytes = match config.maxmemory {
            0 => MAX_FLUSH_BYTES,
            limit => (limit / n / 64).min(MAX_FLUSH_BYTES),
        };
        let databases = match config.databases {
            0 => 16,
            n => n,
        };
        Store {
            shards: new_shards(n, None, None, None),
            shard_mask: n - 1,
            hasher: if config.fixed_seed {
                Hasher::fixed()
            } else {
                Hasher::random()
            },
            epoch: Instant::now(),
            // `now()` starts at 1, or at the Unix time.
            clock_start: if config.unix_clock {
                unix_now.max(1)
            } else {
                1
            },
            unix_base: match config.unix_base {
                Some(base) => base,
                None if config.unix_clock => 0,
                None => unix_now.saturating_sub(1),
            },
            loading: AtomicBool::new(false),
            maxmemory: AtomicUsize::new(config.maxmemory),
            policy: AtomicU8::new(config.policy as u8),
            used: AtomicI64::new(0),
            client_memory: AtomicI64::new(0),
            flush_bytes: AtomicUsize::new(flush_bytes),
            ready: Ready::default(),
            transactions: Mutex::new(()),
            functions: Mutex::new((0, Vec::new())),
            index: 0,
            broadcasting: Mutex::new(false),
            base: None,
            others: (1..databases).map(|_| OnceLock::new()).collect(),
            notifying: AtomicBool::new(false),
            replicating: AtomicBool::new(false),
        }
    }

    /// A new memory limit, in every database. What it takes to get under it is
    /// evicted as writes come, or they're refused, by the policy.
    pub(crate) fn set_maxmemory(&self, limit: usize) {
        for db in self.live() {
            db.maxmemory.store(limit, Relaxed);
            let flush = match limit {
                0 => MAX_FLUSH_BYTES,
                limit => (limit / db.shards.len() / 64).min(MAX_FLUSH_BYTES),
            };
            db.flush_bytes.store(flush, Relaxed);
        }
    }

    /// A new eviction policy, in every database.
    pub(crate) fn set_policy(&self, policy: EvictionPolicy) {
        for db in self.live() {
            db.policy.store(policy as u8, Relaxed);
        }
    }

    /// Forgets how many keys expired and were evicted, for `CONFIG RESETSTAT`.
    pub(crate) fn reset_stats(&self) {
        for db in self.live() {
            for shard in db.shards.iter() {
                let mut inner = db.lock(shard);
                inner.mem.expired = 0;
                inner.mem.evicted = 0;
            }
        }
    }

    /// Notes what a client is blocked in.
    pub(crate) fn track_waiter(&self, waiter: &Arc<Waiter>) {
        self.base()
            .ready
            .waiters
            .lock()
            .insert(waiter.client(), waiter.clone());
    }

    pub(crate) fn untrack_waiter(&self, client: u64) {
        self.base().ready.waiters.lock().remove(&client);
    }

    /// What client `id` is blocked in, if anything.
    pub(crate) fn waiter_of(&self, id: u64) -> Option<Arc<Waiter>> {
        self.base().ready.waiters.lock().get(&id).cloned()
    }

    /// Database 0, for what it counts for all of them (see `base`).
    #[inline]
    fn base(&self) -> &Store {
        self.base.as_deref().unwrap_or(self)
    }

    /// How many databases there are.
    pub fn databases(&self) -> usize {
        self.base().others.len() + 1
    }

    /// Which database this is.
    pub fn index(&self) -> usize {
        self.index as usize
    }

    /// Database `index`, of database 0: made if it hasn't been used yet.
    pub fn db(self: &Arc<Self>, index: usize) -> &Arc<Store> {
        if index == 0 {
            return self;
        }
        if let Some(db) = self.others[index - 1].get() {
            return db;
        }
        // Held until the new database is in `others`, so that `set_broadcast`
        // and `set_notifying` either see it or come before it.
        let broadcasting = self.broadcasting.lock();
        let events = self.notifying.load(Relaxed).then_some(index as u16);
        let repl = self.replicating.load(Relaxed).then_some(index as u16);
        self.others[index - 1].get_or_init(|| {
            Arc::new(Store {
                shards: new_shards(
                    self.shards.len(),
                    (*broadcasting).then_some(self),
                    events,
                    repl,
                ),
                shard_mask: self.shard_mask,
                hasher: self.hasher.clone(),
                epoch: self.epoch,
                clock_start: self.clock_start,
                unix_base: self.unix_base,
                loading: AtomicBool::new(false),
                maxmemory: AtomicUsize::new(self.maxmemory()),
                policy: AtomicU8::new(self.policy() as u8),
                used: AtomicI64::new(0),
                client_memory: AtomicI64::new(0),
                flush_bytes: AtomicUsize::new(self.flush_bytes.load(Relaxed)),
                ready: Ready::default(),
                transactions: Mutex::new(()),
                functions: Mutex::new((0, Vec::new())),
                index: index as u32,
                broadcasting: Mutex::new(false),
                base: Some(self.clone()),
                others: Box::default(),
                notifying: AtomicBool::new(false),
                replicating: AtomicBool::new(false),
            })
        })
    }

    /// The databases made so far, database 0 first.
    pub fn live(&self) -> impl Iterator<Item = &Store> {
        let base = self.base();
        std::iter::once(base).chain(base.others.iter().filter_map(|db| db.get().map(|db| &**db)))
    }

    /// Locks `shard`, one of this database's.
    #[inline]
    fn lock<'a>(&'a self, shard: &'a Shard) -> ShardGuard<'a, ShardInner> {
        match &self.base {
            None => shard.inner.lock(),
            Some(base) => base.same_shard(self, shard).inner.lock_for(&shard.inner),
        }
    }

    /// Locks shards `a` and `b` of this database, lower first: the lower, and
    /// the higher if they're not the same one.
    fn lock_two(
        &self,
        a: usize,
        b: usize,
    ) -> (
        ShardGuard<'_, ShardInner>,
        Option<ShardGuard<'_, ShardInner>>,
    ) {
        if a == b {
            return (self.lock(&self.shards[a]), None);
        }
        let (low, high) = (&self.shards[a.min(b)], &self.shards[a.max(b)]);
        loop {
            let first = self.lock(low);
            let second = match &self.base {
                None => high.inner.lock_next_for(&high.inner),
                Some(base) => base.same_shard(self, high).inner.lock_next_for(&high.inner),
            };
            if second.is_some() {
                return (first, second);
            }
            // A transaction holding the higher may be waiting for the lower.
            drop(first);
            std::thread::yield_now();
        }
    }

    /// Tries to lock `shard`, one of this database's, without waiting.
    #[inline]
    fn try_lock<'a>(&'a self, shard: &'a Shard) -> Option<ShardGuard<'a, ShardInner>> {
        match &self.base {
            None => shard.inner.try_lock(),
            Some(base) => base
                .same_shard(self, shard)
                .inner
                .try_lock_for(&shard.inner),
        }
    }

    /// [`try_lock`](Self::try_lock), or this thread's own if it holds it.
    fn try_lock_here<'a>(&'a self, shard: &'a Shard) -> Option<ShardGuard<'a, ShardInner>> {
        match &self.base {
            None => shard.inner.try_lock_here(),
            Some(base) => base
                .same_shard(self, shard)
                .inner
                .try_lock_here_for(&shard.inner),
        }
    }

    /// This database's shard in the place `shard` has in `db`.
    fn same_shard(&self, db: &Store, shard: &Shard) -> &Shard {
        let at = shard as *const Shard as usize - db.shards.as_ptr() as usize;
        &self.shards[at / std::mem::size_of::<Shard>()]
    }

    /// Swaps what this database holds with what `other` does, for `SWAPDB`:
    /// clients blocked on or watching a key stay with its database's number,
    /// and hear if it now has something else.
    pub(crate) fn swap_with(&self, other: &Store) {
        self.exclusive(|| {
            for i in 0..self.shards.len() {
                let mut a = self.lock(&self.shards[i]);
                let mut b = other.lock(&other.shards[i]);
                let (a, b) = (&mut *a, &mut *b);
                for (one, another) in [(&*a, &*b), (&*b, &*a)] {
                    for key in one.watched.by_key.keys() {
                        let hash = slot_hash(&self.hasher, key);
                        if one.lookup(hash, key).is_some() || another.lookup(hash, key).is_some() {
                            one.watched.touch(key);
                        }
                    }
                }
                std::mem::swap(a, b);
                std::mem::swap(&mut a.waiting, &mut b.waiting);
                std::mem::swap(&mut a.watched, &mut b.watched);
                self.sync_usage(&self.shards[i], a, true);
                other.sync_usage(&other.shards[i], b, true);
                for (db, inner) in [(self, &*a), (other, &*b)] {
                    for key in inner.waiting.keys() {
                        if inner.lookup(slot_hash(&self.hasher, key), key).is_some() {
                            db.queue_ready(key);
                        }
                    }
                }
            }
        });
    }

    /// Current time on the store clock.
    pub fn now(&self) -> Millis {
        self.epoch.elapsed().as_millis() as Millis + self.clock_start
    }

    /// While loading, writes aren't held to `maxmemory` (see `loading`).
    pub(crate) fn set_loading(&self, loading: bool) {
        self.base().loading.store(loading, Relaxed);
    }

    /// Unix time in milliseconds at store-clock time `now`, for `XADD`'s
    /// automatic IDs: the time a command runs at, as Redis' snapshot of it,
    /// so a script's `XADD`s (its time stands still) share a millisecond.
    pub fn unix_ms(&self, now: Millis) -> u64 {
        self.unix_base.saturating_add(now)
    }

    /// Converts a Unix timestamp in milliseconds to a store-clock deadline,
    /// by the clock's Unix time (as `XADD` reads it), so a command run again
    /// at the time it first ran gets the same deadline. Timestamps in the
    /// past map to `now`, which is already expired.
    pub fn deadline_from_unix_ms(&self, now: Millis, unix_ms: i64) -> Millis {
        let deadline = unix_ms.saturating_sub(self.unix_base as i64);
        (deadline.max(now as i64) as Millis).min(MAX_DEADLINE)
    }

    /// Accounted memory in bytes (approximate; see module docs), connection
    /// buffers included.
    pub fn used_memory(&self) -> usize {
        let base = self.base();
        (base.used.load(Relaxed) + base.client_memory.load(Relaxed)).max(0) as usize
    }

    /// Counts memory the connections use (their buffers) as used, so that
    /// eviction and the limit leave room for it, as Redis does.
    pub fn add_client_memory(&self, delta: isize) {
        self.base().client_memory.fetch_add(delta as i64, Relaxed);
    }

    /// Whether usage is within 10% of `maxmemory`.
    fn near_limit(&self) -> bool {
        self.maxmemory() != 0 && self.used_memory() * 10 >= self.maxmemory() * 9
    }

    /// `bytes` as a key, hashed.
    pub fn key<'a>(&self, bytes: &'a [u8]) -> Key<'a> {
        Key {
            bytes,
            hash: KeyHash(self.hasher.hash_one(bytes)),
        }
    }

    /// The key for `bytes` with a hash computed earlier by [`Store::key`].
    pub fn key_with_hash<'a>(&self, bytes: &'a [u8], hash: KeyHash) -> Key<'a> {
        debug_assert_eq!(hash, self.key(bytes).hash, "hash of another key");
        Key { bytes, hash }
    }

    /// The key's slot hash and shard. The shard comes from the hash's high
    /// 32 bits and the slot hash is the low 32, so they're independent.
    fn locate(&self, hash: KeyHash) -> (u32, usize) {
        (hash.0 as u32, (hash.0 >> 32) as usize & self.shard_mask)
    }

    /// Folds the shard's footprint change into its pending delta, flushing to
    /// the global counter when it grows past the batch size (or if `force`).
    fn sync_usage(&self, shard: &Shard, inner: &mut ShardInner, force: bool) {
        let footprint = inner.footprint();
        shard.footprint.store(footprint, Relaxed);
        shard.with_ttl.store(inner.mem.with_ttl, Relaxed);
        let (base, buckets) = inner.table.raw_parts();
        shard.table_base.store(base, Relaxed);
        shard.table_buckets.store(buckets, Relaxed);
        inner.unflushed += footprint as isize - inner.reported as isize;
        inner.reported = footprint;
        if inner.unflushed != 0
            && (force || inner.unflushed.unsigned_abs() > self.flush_bytes.load(Relaxed))
        {
            self.base().used.fetch_add(inner.unflushed as i64, Relaxed);
            inner.unflushed = 0;
        }
    }

    /// Starts loading the memory a lookup of `key` will read first (its
    /// shard's lock and its two buckets), so that running other commands
    /// meanwhile overlaps the cache misses instead of waiting on each in turn.
    pub fn prefetch(&self, key: KeyHash) {
        let (hash, index) = self.locate(key);
        let shard = &self.shards[index];
        table::prefetch_line(std::ptr::from_ref(shard) as usize);
        table::prefetch(
            shard.table_base.load(Relaxed),
            shard.table_buckets.load(Relaxed),
            hash,
        );
    }

    /// The step after [`Store::prefetch`], a few commands later: reads the
    /// key's buckets (cached by then) under the lock and prefetches the
    /// entries they point to, the next miss a lookup would wait on. Skips a
    /// shard another thread holds rather than wait for it.
    pub fn prefetch_entry(&self, key: KeyHash) {
        let (hash, index) = self.locate(key);
        let Some(inner) = self.try_lock(&self.shards[index]) else {
            return;
        };
        inner.table.find(hash, |slot| {
            let (id, offset) = split(slot.loc);
            if let Some(Some(segment)) = inner.segments.get(id) {
                // An entry may straddle two cache lines.
                let addr = segment.buf.as_ptr() as usize + offset;
                table::prefetch_line(addr);
                table::prefetch_line(addr + 63);
            }
            false
        });
    }

    /// Prefetches for a run of lookups, before lookup `i` (counting from 0)
    /// runs: [`Store::prefetch`] `PREFETCH_AHEAD` lookups ahead and
    /// [`Store::prefetch_entry`] `PREFETCH_ENTRY_AHEAD` ahead, once those
    /// buckets have had time to arrive. `key_at(j)` is lookup `j`'s key, or
    /// `None` past the end (or to skip prefetching).
    pub fn prefetch_ahead(&self, i: usize, key_at: impl Fn(usize) -> Option<KeyHash>) {
        if i == 0 {
            for j in 0..PREFETCH_AHEAD {
                if let Some(key) = key_at(j) {
                    self.prefetch(key);
                }
            }
            for j in 0..PREFETCH_ENTRY_AHEAD {
                if let Some(key) = key_at(j) {
                    self.prefetch_entry(key);
                }
            }
        }
        if let Some(key) = key_at(i + PREFETCH_AHEAD) {
            self.prefetch(key);
        }
        if let Some(key) = key_at(i + PREFETCH_ENTRY_AHEAD) {
            self.prefetch_entry(key);
        }
    }

    /// Looks up `key` and passes the value to `f`.
    ///
    /// `f` runs while the shard lock is held: keep it short. Values larger than
    /// [`COPY_UNDER_LOCK_MAX`] should be taken with [`ValueRef::share`].
    pub fn get_with<R>(
        &self,
        key: &[u8],
        now: Millis,
        f: impl FnOnce(Option<ValueRef<'_>>) -> R,
    ) -> R {
        self.get_key(self.key(key), now, f)
    }

    /// [`Store::get_with`] for a key hashed already.
    pub fn get_key<R>(
        &self,
        key: Key<'_>,
        now: Millis,
        f: impl FnOnce(Option<ValueRef<'_>>) -> R,
    ) -> R {
        let (hash, index) = self.locate(key.hash);
        let key = key.bytes;
        let shard = &self.shards[index];
        let expired;
        let result = {
            let mut guard = self.lock(shard);
            let inner = &mut *guard;
            let Some((loc, header)) = inner.lookup(hash, key) else {
                return f(None);
            };
            if !header.is_expired(now) {
                inner.touch(loc, &header);
                return f(Some(inner.value_ref(loc, &header)));
            }
            if inner.keeps_expired(key) {
                return f(None);
            }
            inner.watched.changed(key, Change::Expired);
            inner.unlink(hash, loc);
            expired = inner.kill(loc);
            inner.mem.expired += 1;
            self.sync_usage(shard, inner, false);
            f(None)
        };
        drop(expired);
        result
    }

    /// Writes `key = value` subject to `cond`, with the given expiry. With
    /// `want_previous`, the outcome carries the old value (for `SET ... GET`).
    ///
    /// Fails if the write would exceed `maxmemory` and the eviction policy
    /// can't free enough room.
    pub fn set(
        &self,
        key: &[u8],
        value: &[u8],
        cond: Condition,
        expiry: Expiry,
        now: Millis,
        want_previous: bool,
    ) -> Result<SetOutcome, OutOfMemory> {
        self.set_key(self.key(key), value, cond, expiry, now, want_previous)
    }

    /// [`Store::set`] for a key hashed already.
    #[inline]
    pub fn set_key(
        &self,
        key: Key<'_>,
        value: &[u8],
        cond: Condition,
        expiry: Expiry,
        now: Millis,
        want_previous: bool,
    ) -> Result<SetOutcome, OutOfMemory> {
        self.set_kind(key, Kind::String, value, cond, expiry, now, want_previous)
    }

    /// Writes a value of any kind as it is, replacing whatever the key held:
    /// for loading snapshots.
    pub fn restore(
        &self,
        key: &[u8],
        kind: Kind,
        value: &[u8],
        expiry: Expiry,
        now: Millis,
    ) -> Result<(), OutOfMemory> {
        // (A key too long to pack it with makes even a small one big, as a write does.)
        let too_long = key.len() + value.len() + 20 > INLINE_MAX;
        if kind.is_collection() && (too_long || !collection::fits_compact(kind, value)) {
            let key = self.key(key);
            let mut big = Some(Collection::from_compact(kind, value));
            self.update_collection(key, now, true, |_| {
                (Update::Object(big.take().expect("written once")), ())
            })?;
            if let Expiry::At(deadline) = expiry {
                self.update_key(key, now, |_| (Update::Expire(Expiry::At(deadline)), ()))?;
            }
            return Ok(());
        }
        self.set_kind(
            self.key(key),
            kind,
            value,
            Condition::Always,
            expiry,
            now,
            false,
        )
        .map(drop)
    }

    #[allow(clippy::too_many_arguments)]
    fn set_kind(
        &self,
        key: Key<'_>,
        kind: Kind,
        value: &[u8],
        cond: Condition,
        expiry: Expiry,
        now: Millis,
        want_previous: bool,
    ) -> Result<SetOutcome, OutOfMemory> {
        let (hash, index) = self.locate(key.hash);
        let key = key.bytes;
        if self.maxmemory() != 0
            && KeyValue::size_for(key.len(), value.len()) > self.maxmemory()
            && !ALLOW_OOM.get()
        {
            // Can never fit; don't flush the cache trying.
            return Err(OutOfMemory);
        }
        // Big entries get their own allocation, made before taking the lock.
        // (The 20 bytes cover the worst-case header.)
        let mut external =
            (key.len() + value.len() + 20 > INLINE_MAX).then(|| KeyValue::new(key, value));
        let external_size = external.as_ref().map_or(0, KeyValue::allocation_size);
        let shard = &self.shards[index];
        let mut attempts = 0;
        loop {
            let mut guard = self.lock(shard);
            let inner = &mut *guard;
            let existing = inner.lookup(hash, key);
            let live = existing
                .as_ref()
                .filter(|(_, header)| !header.is_expired(now))
                .cloned();
            // `SET ... GET` on another kind fails, as in Redis, and sets nothing.
            if want_previous && live.as_ref().is_some_and(|(_, h)| h.kind() != Kind::String) {
                return Ok(SetOutcome {
                    applied: false,
                    previous: None,
                    wrong_type: true,
                    _garbage: Vec::new(),
                });
            }
            if (cond == Condition::IfAbsent && live.is_some())
                || (cond == Condition::IfPresent && live.is_none())
            {
                // An expired key goes as the write looks at it, as in Redis.
                let mut garbage = Vec::new();
                if live.is_none()
                    && let Some((loc, _)) = existing
                {
                    inner.watched.changed(key, Change::Expired);
                    inner.unlink(hash, loc);
                    garbage.extend(inner.kill(loc));
                    inner.mem.expired += 1;
                    self.sync_usage(shard, inner, false);
                }
                let previous = live.and_then(|(loc, header)| {
                    inner.touch(loc, &header);
                    want_previous.then(|| inner.value_ref(loc, &header).share())
                });
                return Ok(SetOutcome {
                    applied: false,
                    previous,
                    wrong_type: false,
                    _garbage: garbage,
                });
            }
            let deadline = match expiry {
                Expiry::Never => NO_EXPIRY,
                Expiry::At(t) => t,
                Expiry::Keep => live
                    .as_ref()
                    .map_or(NO_EXPIRY, |(_, header)| header.deadline),
            };
            // Overwriting a key counts as using it.
            let usage = live
                .as_ref()
                .map_or(0, |(_, header)| (header.usage() + 1).min(MAX_USAGE));
            let entry_len = match &external {
                Some(_) => {
                    segment::encoded_len(&Payload::External(inner.next_external_index()), deadline)
                }
                None => segment::encoded_len(&Payload::Inline { key, value }, deadline),
            };

            // Make room first if this write needs memory the limit doesn't
            // have. Never evict while holding our own shard's lock: drop it,
            // evict, and retry. After two tries, write anyway (bounded
            // overshoot) rather than fail under heavy concurrent writes.
            if self.maxmemory() != 0 && attempts < 2 {
                // Segment memory is counted as written pages, so the entry
                // itself is what this write adds (a new segment's untouched
                // capacity is free).
                let needed = external_size + entry_len;
                if self.used_memory() + needed > self.maxmemory()
                    && !self.base().loading.load(Relaxed)
                {
                    drop(guard);
                    attempts += 1;
                    if !self.make_room(index, hash, needed, now) {
                        return Err(OutOfMemory);
                    }
                    continue;
                }
            }

            let mut garbage = Vec::new();
            let mut previous = None;
            if want_previous
                && let Some((loc, header)) = &live
                && let Body::Inline { .. } = header.body
            {
                // Copy small values now; big ones are handed over below.
                previous = Some(inner.value_ref(*loc, header).share());
            }
            let payload = match external.take() {
                Some(kv) => Payload::External(inner.store_external(kv)),
                None => Payload::Inline { key, value },
            };
            let Some(new) = inner.append(kind, usage, &payload, deadline) else {
                // Out of segment ids: practically unreachable (4 GB per shard).
                if let Payload::External(index) = payload {
                    garbage.push(Freed::Value(inner.take_external(index)));
                }
                self.sync_usage(shard, inner, false);
                drop(guard);
                drop(garbage);
                return Err(OutOfMemory);
            };
            match inner.replace(hash, existing.as_ref().map(|(old, _)| *old), new) {
                Some(Freed::Value(kv)) if want_previous && live.is_some() => {
                    previous = Some(SharedValue(Shared::External(kv)));
                }
                freed => garbage.extend(freed),
            }
            if existing.is_some() && live.is_none() {
                inner.mem.expired += 1;
                inner.watched.changed(key, Change::Expired);
            }
            inner.mem.writes += 1;
            inner.watched.changed(
                key,
                if live.is_some() {
                    Change::Written
                } else {
                    Change::Created
                },
            );
            inner.maybe_defrag(self.near_limit(), now, &self.hasher, &mut garbage);
            self.sync_usage(shard, inner, false);
            return Ok(SetOutcome {
                applied: true,
                previous,
                wrong_type: false,
                _garbage: garbage,
            });
        }
    }

    /// Shows `f` the key (`None` if it doesn't exist) and applies the
    /// [`Update`] it returns, all under the shard lock, so read-modify-write
    /// commands like `INCR`, `GETDEL` or `EXPIRE ... GT` are atomic. Keep `f`
    /// short.
    ///
    /// Only writing a value may need memory (when it changes length). Like
    /// [`Store::set`], it fails if `maxmemory` can't make room, and `f` runs
    /// again after room was made.
    pub fn update_key<R>(
        &self,
        key: Key<'_>,
        now: Millis,
        mut f: impl FnMut(Option<Current<'_>>) -> (Update, R),
    ) -> Result<R, OutOfMemory> {
        self.update_with(key, now, false, |inner, existing| {
            f(existing.map(|(loc, header)| Current {
                value: inner.value_ref(loc, header),
                expiry: expiry_of(header),
                uses: header.usage(),
            }))
        })
    }

    /// [`Store::update_key`] for lists, sets and sorted sets, whose big
    /// forms `f` may change in place (returning [`Update::Changed`]); a
    /// packed one comes as bytes. With `grows` (commands that may add data,
    /// Redis' `denyoom`), it refuses with `OutOfMemory` when memory is over
    /// `maxmemory` and eviction can't make room first. As then nothing has
    /// changed yet, `f` doesn't run again after that.
    pub fn update_collection<R>(
        &self,
        key: Key<'_>,
        now: Millis,
        grows: bool,
        mut f: impl FnMut(Option<Entry<'_>>) -> (Update, R),
    ) -> Result<R, OutOfMemory> {
        self.update_with(key, now, grows, |inner, existing| {
            inner.with_entry(existing, &mut f)
        })
    }

    /// Runs `f` with both keys' shards locked, for commands that change two
    /// keys at once, such as `SMOVE`: `f` reads and changes each through
    /// [`Pair::update`]. The locks are taken in shard order, as `rename`
    /// takes them, so this can't deadlock. `grows` as in
    /// [`Store::update_collection`].
    pub fn update_pair<R>(
        &self,
        a: Key<'_>,
        b: Key<'_>,
        now: Millis,
        grows: bool,
        f: impl FnOnce(&mut Pair<'_>) -> R,
    ) -> Result<R, OutOfMemory> {
        let (hash_a, ia) = self.locate(a.hash);
        let (hash_b, ib) = self.locate(b.hash);
        if grows && !self.room_for(ia, hash_a, a.bytes.len() + b.bytes.len(), now) {
            return Err(OutOfMemory);
        }
        let (mut low, mut high) = self.lock_two(ia, ib);
        let mut pair = Pair {
            store: self,
            low: &mut low,
            high: high.as_deref_mut(),
            low_index: ia.min(ib),
            keys: [(a.bytes, hash_a, ia), (b.bytes, hash_b, ib)],
            now,
            garbage: Vec::new(),
            changed: [false; 2],
            out_of_ids: false,
        };
        let result = f(&mut pair);
        let Pair {
            mut garbage,
            changed,
            out_of_ids,
            ..
        } = pair;
        let eager = self.near_limit();
        if changed[0] {
            low.maybe_defrag(eager, now, &self.hasher, &mut garbage);
            self.sync_usage(&self.shards[ia.min(ib)], &mut low, false);
        }
        if let Some(high) = high.as_deref_mut()
            && changed[1]
        {
            high.maybe_defrag(eager, now, &self.hasher, &mut garbage);
            self.sync_usage(&self.shards[ia.max(ib)], high, false);
        }
        drop((low, high));
        drop(garbage); // frees values outside the locks
        if out_of_ids {
            return Err(OutOfMemory);
        }
        Ok(result)
    }

    /// For commands that may add data: whether there's room under
    /// `maxmemory` for about `incoming` more bytes, evicting if the policy
    /// allows.
    fn room_for(&self, home: usize, seed: u32, incoming: usize, now: Millis) -> bool {
        let incoming = incoming + 64;
        self.maxmemory() == 0
            || self.used_memory() + incoming <= self.maxmemory()
            || self.base().loading.load(Relaxed)
            || self.make_room(home, seed, incoming, now)
    }

    /// The single-key write loop: looks the key up, lets `view` decide the
    /// update, applies it, and evicts and retries if a write needs room.
    fn update_with<R>(
        &self,
        key: Key<'_>,
        now: Millis,
        grows: bool,
        mut view: impl FnMut(&mut ShardInner, Option<(u32, &Header)>) -> (Update, R),
    ) -> Result<R, OutOfMemory> {
        let (hash, index) = self.locate(key.hash);
        let key = key.bytes;
        let shard = &self.shards[index];
        if grows && !self.room_for(index, hash, key.len(), now) {
            return Err(OutOfMemory);
        }
        let mut attempts = 0;
        loop {
            let mut garbage = Vec::new();
            let mut guard = self.lock(shard);
            let inner = &mut *guard;
            let (existing, mut changed) = inner.lookup_live(hash, key, now, &mut garbage);
            let (update, result) = view(inner, existing.as_ref().map(|(loc, h)| (*loc, h)));
            let absent = existing.is_none();
            match self.apply(
                inner,
                hash,
                key,
                existing,
                update,
                now,
                attempts < 2,
                &mut garbage,
            ) {
                Ok(applied) => {
                    changed |= applied;
                    if applied && absent && !inner.waiting.is_empty() {
                        self.created(inner, key);
                    }
                }
                Err(Retry::NeedRoom(needed)) => {
                    if changed {
                        self.sync_usage(shard, inner, false);
                    }
                    drop(guard);
                    drop(garbage);
                    attempts += 1;
                    if !self.make_room(index, hash, needed, now) {
                        return Err(OutOfMemory);
                    }
                    continue;
                }
                Err(Retry::OutOfIds) => {
                    self.sync_usage(shard, inner, false);
                    drop(guard);
                    drop(garbage);
                    return Err(OutOfMemory);
                }
            }
            if changed {
                inner.maybe_defrag(self.near_limit(), now, &self.hasher, &mut garbage);
                self.sync_usage(shard, inner, false);
            }
            drop(guard);
            drop(garbage); // frees values outside the lock
            return Ok(result);
        }
    }

    /// Changes a string where it lies, if it can: the key holds one, nothing else
    /// holds a reference to it now, and it has room for the length `new_len`
    /// gives (from its length, or `None` not to try): a small one only keeps its
    /// length. `edit` gets its bytes, new ones zero, and its old length, and
    /// returns its result and whether the string changed. Otherwise this does
    /// nothing and returns `None`, for the caller's [`Store::update_key`]: a
    /// reader holding a reference keeps what it has.
    pub(crate) fn edit_string<R>(
        &self,
        key: Key<'_>,
        now: Millis,
        new_len: impl FnOnce(usize) -> Option<usize>,
        edit: impl FnOnce(&mut [u8], usize) -> (R, bool),
    ) -> Option<R> {
        let (hash, index) = self.locate(key.hash);
        let key = key.bytes;
        let shard = &self.shards[index];
        let mut garbage = Vec::new();
        let mut guard = self.lock(shard);
        let inner = &mut *guard;
        let (existing, expired) = inner.lookup_live(hash, key, now, &mut garbage);
        let result = existing.and_then(|(loc, header)| {
            if header.kind() != Kind::String {
                return None;
            }
            let (bytes, old) = match &header.body {
                Body::External(i) => {
                    let value = inner.externals[*i as usize]
                        .as_mut()
                        .expect("live external");
                    let old = value.value().len();
                    (value.edit(new_len(old)?)?, old)
                }
                Body::Inline { value: range, .. } => {
                    let old = range.len();
                    if new_len(old)? != old {
                        return None;
                    }
                    let segment = inner.segment_mut(split(loc).0);
                    (&mut segment.buf[range.clone()], old)
                }
            };
            let (result, changed) = edit(bytes, old);
            inner.touch(loc, &header);
            if changed {
                inner.mem.writes += 1;
                inner.watched.changed(key, Change::Written);
            }
            Some(result)
        });
        if expired {
            self.sync_usage(shard, inner, false);
        }
        drop(guard);
        drop(garbage); // frees values outside the lock
        result
    }

    /// [`edit_string`](Self::edit_string) for a change of length too: `edit` gets
    /// its bytes, then `extra` zeroes if it's kept apart (`Update::ReplaceWithRoom`)
    /// and has the room for them, and its length, and returns its result, its new
    /// length, which only a string with the zeroes may change, and whether it
    /// changed. `None` if there's no such string, for the caller's
    /// [`Store::update_key`].
    pub(crate) fn edit_string_room<R>(
        &self,
        key: Key<'_>,
        now: Millis,
        extra: usize,
        edit: impl FnOnce(&mut [u8], usize) -> (R, usize, bool),
    ) -> Option<R> {
        let (hash, index) = self.locate(key.hash);
        let key = key.bytes;
        let shard = &self.shards[index];
        let mut garbage = Vec::new();
        let mut guard = self.lock(shard);
        let inner = &mut *guard;
        let (existing, expired) = inner.lookup_live(hash, key, now, &mut garbage);
        let result = existing.and_then(|(loc, header)| {
            if header.kind() != Kind::String {
                return None;
            }
            let (result, changed) = match &header.body {
                Body::External(i) => {
                    let value = inner.externals[*i as usize]
                        .as_mut()
                        .expect("live external");
                    let old = value.value().len();
                    let room = if old + extra <= value.value_capacity() {
                        extra
                    } else {
                        0
                    };
                    let (result, len, changed) = edit(value.edit(old + room)?, old);
                    value.shorten(len);
                    (result, changed)
                }
                Body::Inline { value: range, .. } => {
                    let old = range.len();
                    let segment = inner.segment_mut(split(loc).0);
                    let (result, len, changed) = edit(&mut segment.buf[range.clone()], old);
                    debug_assert_eq!(len, old);
                    (result, changed)
                }
            };
            inner.touch(loc, &header);
            if changed {
                inner.mem.writes += 1;
                inner.watched.changed(key, Change::Written);
            }
            Some(result)
        });
        if expired {
            self.sync_usage(shard, inner, false);
        }
        drop(guard);
        drop(garbage); // frees values outside the lock
        result
    }

    #[cfg(test)]
    fn has_ttl_for_test(&self, key: Key<'_>, now: Millis) -> bool {
        self.update_key(key, now, |current| {
            (
                Update::Peek,
                current.is_some_and(|c| matches!(c.expiry, Expiry::At(_))),
            )
        })
        .unwrap()
    }

    /// Takes away a key's expiry: what a command's new value does to what the key
    /// held, as Redis' `setKey`, where the value is written without touching it.
    pub(crate) fn forget_expiry(&self, key: Key<'_>, now: Millis) {
        let _ = self.update_key(key, now, |_| (Update::Expire(Expiry::Never), ()));
    }

    /// Applies `update` to the key's live entry (`existing`) under its
    /// shard's lock. Returns whether memory changed. A write that needs
    /// room under `maxmemory` returns [`Retry::NeedRoom`] instead, if
    /// `may_evict`, having changed nothing.
    #[allow(clippy::too_many_arguments)]
    fn apply(
        &self,
        inner: &mut ShardInner,
        hash: u32,
        key: &[u8],
        existing: Option<(u32, Header)>,
        update: Update,
        now: Millis,
        may_evict: bool,
        garbage: &mut Vec<Freed>,
    ) -> Result<bool, Retry> {
        let mut digits = itoa::Buffer::new();
        let collection = match (update, &existing) {
            (Update::SetInt(n), _) => {
                let value = digits.format(n).as_bytes();
                return self.write_bytes(
                    inner,
                    hash,
                    key,
                    &existing,
                    Kind::String,
                    value,
                    false,
                    may_evict,
                    garbage,
                );
            }
            (Update::ReplaceWithRoom(value), _) => {
                return self.write_bytes(
                    inner,
                    hash,
                    key,
                    &existing,
                    Kind::String,
                    &value,
                    true,
                    may_evict,
                    garbage,
                );
            }
            (Update::Replace(kind, value), _)
                if !kind.is_collection() || key.len() + value.len() + 20 <= INLINE_MAX =>
            {
                return self.write_bytes(
                    inner, hash, key, &existing, kind, &value, false, may_evict, garbage,
                );
            }
            // A packed collection with a key too big to pack it with.
            (Update::Replace(kind, value), _) => Collection::from_compact(kind, &value),
            (Update::Object(collection), _) => collection,
            (Update::Peek, _) | (_, None) => return Ok(false),
            (Update::Touch, Some((loc, header))) => {
                inner.touch(*loc, header);
                return Ok(false);
            }
            (Update::Changed, Some((loc, header))) => {
                // `with_entry` already counted its new size.
                inner.touch(*loc, header);
                inner.mem.writes += 1;
                inner.watched.changed(key, Change::Written);
                return Ok(true);
            }
            (Update::ChangedUnseen, Some((loc, header))) => {
                inner.touch(*loc, header);
                inner.mem.writes += 1;
                // Not a change watchers see, but one replicas must.
                if inner.watched.repl.is_some() {
                    crate::repl::propagate::changed_unseen();
                }
                return Ok(true);
            }
            (Update::Delete, Some((loc, _))) => {
                inner.unlink(hash, *loc);
                garbage.extend(inner.kill(*loc));
                inner.mem.writes += 1;
                inner.watched.changed(key, Change::Removed);
                return Ok(true);
            }
            (Update::Expire(expiry), Some((loc, header))) => {
                let deadline = match expiry {
                    Expiry::Keep => header.deadline,
                    Expiry::Never => NO_EXPIRY,
                    Expiry::At(deadline) => deadline,
                };
                // (A replica's master says when a key goes: one it set to
                // a time passed already, by the time it got here, stays.)
                let how = if deadline != NO_EXPIRY
                    && deadline <= now
                    && !crate::segment::expiry_suspended()
                {
                    inner.unlink(hash, *loc);
                    garbage.extend(inner.kill(*loc));
                    Change::Removed
                } else {
                    inner.set_deadline(hash, key, *loc, header, deadline);
                    Change::Written
                };
                inner.mem.writes += 1;
                inner.watched.changed(key, how);
                return Ok(true);
            }
        };
        // A big collection: an entry pointing at it, keeping the TTL.
        let (old, deadline, usage) = match &existing {
            Some((loc, header)) => (
                Some(*loc),
                header.deadline,
                (header.usage() + 1).min(MAX_USAGE),
            ),
            None => (None, NO_EXPIRY, 0),
        };
        let how = if existing.is_some() {
            Change::Written
        } else {
            Change::Created
        };
        let kind = collection.kind();
        let index = inner.store_object(key, collection);
        let Some(new) = inner.append(kind, usage, &Payload::External(index), deadline) else {
            garbage.push(Freed::Object(Box::new(inner.take_object(index))));
            return Err(Retry::OutOfIds);
        };
        garbage.extend(inner.replace(hash, old, new));
        inner.mem.writes += 1;
        inner.watched.changed(key, how);
        Ok(true)
    }

    /// Writes `value` of `kind` as the key's value: in place when it's as
    /// long as the old one and of the same kind, otherwise as a new entry.
    #[allow(clippy::too_many_arguments)]
    fn write_bytes(
        &self,
        inner: &mut ShardInner,
        hash: u32,
        key: &[u8],
        existing: &Option<(u32, Header)>,
        kind: Kind,
        value: &[u8],
        apart: bool,
        may_evict: bool,
        garbage: &mut Vec<Freed>,
    ) -> Result<bool, Retry> {
        if !apart
            && let Some((loc, header)) = existing
            && let Body::Inline { value: range, .. } = &header.body
            && range.len() == value.len()
            && header.kind() == kind
        {
            inner.segment_mut(split(*loc).0).buf[range.clone()].copy_from_slice(value);
            inner.touch(*loc, header);
            inner.mem.writes += 1;
            inner.watched.changed(key, Change::Written);
            return Ok(false);
        }
        let (old, deadline, usage) = match existing {
            Some((loc, header)) => (
                Some(*loc),
                header.deadline,
                (header.usage() + 1).min(MAX_USAGE),
            ),
            None => (None, NO_EXPIRY, 0),
        };
        // Asked for, or only a huge key makes this external.
        let external = (apart || key.len() + value.len() + 20 > INLINE_MAX).then(|| {
            // A big string growing, as `APPEND` and `SETBIT` do, is likely to go
            // on: with room for it, `edit_string` can do that without copying it.
            let grown = existing.as_ref().is_some_and(|(loc, header)| {
                kind == Kind::String
                    && header.kind() == Kind::String
                    && matches!(header.body, Body::External(_))
                    && inner.value_ref(*loc, header).as_bytes().len() < value.len()
            });
            let room = if apart {
                (value.len() / 8).clamp(32, 300)
            } else if grown {
                value.len().min(GROWING_STRING_ROOM)
            } else {
                0
            };
            KeyValue::with_capacity(key, value, value.len() + room)
        });
        let needed = match &external {
            Some(kv) => {
                kv.allocation_size()
                    + segment::encoded_len(
                        &Payload::External(inner.next_external_index()),
                        deadline,
                    )
            }
            None => segment::encoded_len(&Payload::Inline { key, value }, deadline),
        };
        if may_evict
            && self.maxmemory() != 0
            && self.used_memory() + needed > self.maxmemory()
            && !self.base().loading.load(Relaxed)
        {
            return Err(Retry::NeedRoom(needed));
        }
        let payload = match external {
            Some(kv) => Payload::External(inner.store_external(kv)),
            None => Payload::Inline { key, value },
        };
        let Some(new) = inner.append(kind, usage, &payload, deadline) else {
            // Out of segment ids: practically unreachable.
            if let Payload::External(index) = payload {
                garbage.push(Freed::Value(inner.take_external(index)));
            }
            return Err(Retry::OutOfIds);
        };
        let how = if old.is_some() {
            Change::Written
        } else {
            Change::Created
        };
        garbage.extend(inner.replace(hash, old, new));
        inner.mem.writes += 1;
        inner.watched.changed(key, how);
        Ok(true)
    }

    /// Removes every key. Not atomic across shards: writes running meanwhile
    /// may survive it.
    pub fn clear(&self) {
        for shard in self.shards.iter() {
            let old = {
                let mut guard = self.lock(shard);
                let inner = &mut *guard;
                // What this shard has added to the global counter so far.
                let flushed = inner.reported as i64 - inner.unflushed as i64;
                let mut empty = ShardInner::default();
                empty.mem.expired = inner.mem.expired;
                empty.mem.evicted = inner.mem.evicted;
                empty.mem.writes = inner.mem.writes + 1;
                empty.waiting = std::mem::take(&mut inner.waiting);
                // Watched keys that existed change. (Tracking clients hear of the
                // flush on their own: see `Tracking::flushed`.)
                for key in inner.watched.by_key.keys() {
                    if inner.lookup(slot_hash(&self.hasher, key), key).is_some() {
                        inner.watched.touch(key);
                    }
                }
                empty.watched = std::mem::take(&mut inner.watched);
                let old = std::mem::replace(inner, empty);
                self.base().used.fetch_sub(flushed, Relaxed);
                self.sync_usage(shard, inner, true);
                old
            };
            drop(old); // frees its memory outside the lock
        }
    }

    /// After a write created `key` under its shard's lock: queues it as
    /// ready if clients are blocked on it.
    fn created(&self, inner: &ShardInner, key: &[u8]) {
        if inner.waiting.contains_key(key) {
            self.queue_ready(key);
        }
    }

    /// Queues `key` for the clients blocked on it, once.
    fn queue_ready(&self, key: &[u8]) {
        let ready = &self.base().ready;
        let mut queue = ready.keys.lock();
        if !queue
            .keys
            .iter()
            .any(|(db, queued)| *db == self.index && **queued == *key)
        {
            queue.keys.push((self.index, key.into()));
        }
        ready.pending.store(true, Relaxed);
    }

    /// Queues a change to `key` for the clients tracking in `BCAST` mode, made
    /// by `by`.
    fn queue_broadcast(&self, key: &[u8], by: u64) {
        let ready = &self.base().ready;
        ready.keys.lock().broadcast.push((key.into(), by));
        ready.pending.store(true, Relaxed);
    }

    /// The changes queued for the clients tracking in `BCAST` mode.
    pub(crate) fn take_broadcast(&self) -> Vec<(Box<[u8]>, u64)> {
        let ready = &self.base().ready;
        let mut queue = ready.keys.lock();
        if queue.keys.is_empty() {
            ready.pending.store(false, Relaxed);
        }
        std::mem::take(&mut queue.broadcast)
    }

    /// Whether some client tracks in `BCAST` mode.
    pub(crate) fn broadcasting(&self) -> bool {
        *self.base().broadcasting.lock()
    }

    /// Some client tracks in `BCAST` mode, or none does any more: every
    /// change in every database is queued for them meanwhile (one made in a
    /// database made later too).
    pub(crate) fn set_broadcast(self: &Arc<Self>, on: bool) {
        let base = self.base_arc();
        let mut broadcasting = base.broadcasting.lock();
        *broadcasting = on;
        for db in base.live() {
            for shard in db.shards.iter() {
                let mut inner = db.lock(shard);
                inner.watched.broadcast = on.then(|| base.clone());
                inner.watched.update();
            }
        }
    }

    /// Keyspace notifications came on or went off: while on, every change in
    /// every database goes into its thread's log for them (`notify::log`).
    pub(crate) fn set_notifying(&self, on: bool) {
        let base = self.base();
        let _creating = base.broadcasting.lock();
        base.notifying.store(on, Relaxed);
        for db in base.live() {
            let events = on.then_some(db.index() as u16);
            for shard in db.shards.iter() {
                let mut inner = db.lock(shard);
                inner.watched.events = events;
                inner.watched.update();
            }
        }
    }

    /// Replicas may follow, or this server follows a master: every change is
    /// counted for what commands propagate, expirations and evictions
    /// propagate as `DEL`s, and reads leave expired keys for writes to remove
    /// (see `repl/propagate.rs`). Off again only when that stops.
    pub(crate) fn set_replicating(&self, on: bool) {
        let base = self.base();
        let _creating = base.broadcasting.lock();
        base.replicating.store(on, Relaxed);
        for db in base.live() {
            let repl = on.then_some(db.index() as u16);
            for shard in db.shards.iter() {
                let mut inner = db.lock(shard);
                inner.watched.repl = repl;
                inner.watched.update();
            }
        }
    }

    /// Database 0, as an `Arc`.
    fn base_arc(self: &Arc<Self>) -> Arc<Store> {
        match &self.base {
            None => self.clone(),
            Some(base) => base.clone(),
        }
    }

    /// `CLIENT TRACKING`: `tracker` hears of `key`'s next change, read by its
    /// command `since`.
    pub(crate) fn track(&self, key: &[u8], tracker: &Arc<crate::tracking::Tracker>, since: u64) {
        let key = self.key(key);
        let (_, index) = self.locate(key.hash);
        let mut inner = self.lock(&self.shards[index]);
        let watched = &mut inner.watched;
        let watchers = match watched.by_key.get_mut(key.bytes) {
            Some(watchers) => watchers,
            None => watched.by_key.entry(key.bytes.into()).or_default(),
        };
        let mut tracked = false;
        for watcher in watchers.iter_mut() {
            if let Watcher::Track(entry) = watcher {
                if entry.is_of(tracker) {
                    entry.renew(since);
                    return;
                }
                tracked = true;
            }
        }
        let entry = crate::tracking::Tracked::new(tracker, since);
        if !tracked {
            entry.counts().key_added();
        }
        watchers.push(Watcher::Track(entry));
        watched.update();
    }

    /// For `tracking-table-max-keys`: stops tracking keys, from shard `start`
    /// on, until `done`. Whether it's done.
    pub(crate) fn untrack_until(&self, start: usize, done: &dyn Fn() -> bool) -> bool {
        let n = self.shards.len();
        for i in 0..n {
            let mut inner = self.lock(&self.shards[(start + i) % n]);
            if !inner.watched.any {
                continue;
            }
            let keys: Vec<Box<[u8]>> = inner
                .watched
                .by_key
                .iter()
                .filter(|(_, watchers)| watchers.iter().any(Watcher::is_track))
                .map(|(key, _)| key.clone())
                .collect();
            for key in keys {
                if done() {
                    return true;
                }
                inner.watched.untrack(&key);
            }
        }
        done()
    }

    /// After a flush: tracks no key (see `Tracking::flushed`).
    pub(crate) fn forget_tracked(&self) {
        for shard in self.shards.iter() {
            let mut inner = self.lock(shard);
            if inner.watched.any {
                inner.watched.forget_tracked();
            }
        }
    }

    /// After a write to `key` that may serve clients blocked on it without
    /// creating it (`XADD`): queues it if some are.
    pub(crate) fn wake_waiters(&self, key: Key<'_>) {
        if self.blocked_clients() == 0 {
            return;
        }
        let (_, index) = self.locate(key.hash);
        let inner = self.lock(&self.shards[index]);
        self.created(&inner, key.bytes);
    }

    /// Registers `waiter` as blocked on `key`, after those already there.
    pub(crate) fn wait_on(&self, key: Key<'_>, waiter: &Arc<Waiter>) {
        let (_, index) = self.locate(key.hash);
        let mut inner = self.lock(&self.shards[index]);
        inner
            .waiting
            .entry(key.bytes.into())
            .or_default()
            .push_back(waiter.clone());
    }

    /// For a client blocked on `key` that wakes when it changes in any way:
    /// [`Store::unwatch`] with `flag` ends it.
    pub(crate) fn wake_on_change(self: &Arc<Self>, key: Key<'_>, flag: &Arc<AtomicBool>) {
        let (_, index) = self.locate(key.hash);
        let mut inner = self.lock(&self.shards[index]);
        inner
            .watched
            .by_key
            .entry(key.bytes.into())
            .or_default()
            .push(Watcher::Watch {
                flag: flag.clone(),
                expired: false,
                waking: Some(self.clone()),
            });
        inner.watched.update();
    }

    /// Deregisters `waiter` from `key`.
    pub(crate) fn stop_waiting(&self, key: Key<'_>, waiter: &Arc<Waiter>) {
        let (_, index) = self.locate(key.hash);
        let mut inner = self.lock(&self.shards[index]);
        if let Some(waiters) = inner.waiting.get_mut(key.bytes) {
            waiters.retain(|w| !Arc::ptr_eq(w, waiter));
            if waiters.is_empty() {
                inner.waiting.remove(key.bytes);
                if inner.waiting.is_empty() {
                    inner.waiting = HashMap::default();
                }
            }
        }
    }

    /// Whether keys were created that clients are blocked on: cheap, for
    /// every command.
    pub(crate) fn has_ready(&self) -> bool {
        self.ready.pending.load(Relaxed)
    }

    /// Takes a key that was created while clients were blocked on it, and
    /// its database. (Of database 0, as is [`has_ready`](Self::has_ready).)
    pub(crate) fn take_ready(&self) -> Option<(usize, Box<[u8]>)> {
        if !self.has_ready() {
            return None;
        }
        let mut queue = self.ready.keys.lock();
        let key = queue.keys.pop();
        if queue.keys.is_empty() && queue.broadcast.is_empty() {
            self.ready.pending.store(false, Relaxed);
        }
        key.map(|(db, key)| (db as usize, key))
    }

    /// The first client blocked on `key` that it now has data for, or that
    /// wants to hear of any change, claimed (see [`Waiter::claim`]) for the
    /// caller to serve. (Skipping those in `tried`.)
    pub(crate) fn claim_waiter(
        &self,
        key: Key<'_>,
        now: Millis,
        tried: &[Arc<Waiter>],
    ) -> Option<Arc<Waiter>> {
        self.update_with(key, now, false, |inner, existing| {
            let kind = existing.map(|(_, header)| header.kind());
            let claimed = inner.waiting.get(key.bytes).and_then(|waiters| {
                waiters
                    .iter()
                    .find(|w| {
                        (Some(w.kind()) == kind || w.wakes_on_change())
                            && !tried.iter().any(|t| Arc::ptr_eq(t, w))
                            && w.claim()
                    })
                    .cloned()
            });
            (Update::Peek, claimed)
        })
        .ok()
        .flatten()
    }

    /// `WATCH`: sets `flag` when `key` changes from now on. Returns whether
    /// the key had expired already.
    pub(crate) fn watch(&self, key: Key<'_>, flag: &Arc<AtomicBool>, now: Millis) -> bool {
        let (hash, index) = self.locate(key.hash);
        let mut inner = self.lock(&self.shards[index]);
        let expired = inner
            .lookup(hash, key.bytes)
            .is_some_and(|(_, header)| header.is_expired(now));
        inner
            .watched
            .by_key
            .entry(key.bytes.into())
            .or_default()
            .push(Watcher::Watch {
                flag: flag.clone(),
                expired,
                waking: None,
            });
        inner.watched.update();
        expired
    }

    pub(crate) fn unwatch(&self, key: Key<'_>, flag: &Arc<AtomicBool>) {
        let (_, index) = self.locate(key.hash);
        let mut inner = self.lock(&self.shards[index]);
        let watched = &mut inner.watched;
        if let Some(watchers) = watched.by_key.get_mut(key.bytes) {
            watchers
                .retain(|w| !matches!(w, Watcher::Watch { flag: f, .. } if Arc::ptr_eq(f, flag)));
            if watchers.is_empty() {
                watched.by_key.remove(key.bytes);
                if watched.by_key.is_empty() {
                    watched.by_key = HashMap::default();
                }
                watched.update();
            }
        }
    }

    /// Whether `key` is there but expired by `now` (not removed yet).
    pub(crate) fn has_expired(&self, key: Key<'_>, now: Millis) -> bool {
        let (hash, index) = self.locate(key.hash);
        let inner = self.lock(&self.shards[index]);
        inner
            .lookup(hash, key.bytes)
            .is_some_and(|(_, header)| header.is_expired(now))
    }

    /// Counts clients blocking (`delta` 1) or done (-1), for `INFO`.
    pub(crate) fn count_blocked(&self, delta: i64) {
        self.base().ready.blocked.fetch_add(delta, Relaxed);
    }

    /// Clients blocked right now.
    pub fn blocked_clients(&self) -> usize {
        self.base().ready.blocked.load(Relaxed).max(0) as usize
    }

    /// Clients really waiting now, for tests to go by. `blocked_clients` counts one from
    /// before its command's last look at its keys, as a write must know to wake it, so a
    /// write right then can still be the answer to that look.
    pub fn clients_waiting(&self) -> usize {
        self.base()
            .ready
            .waiters
            .lock()
            .values()
            .filter(|waiter| waiter.is_waiting())
            .count()
    }

    /// The shards a native script runs holding: those it uses as it uses them
    /// inside a transaction (`EXEC`), else none yet but those in `start`, taken
    /// in order. The script asks for the rest ([`ScriptShards::hold`]) before it
    /// uses their keys.
    pub(crate) fn script_shards(&self, start: &[usize]) -> ScriptShards<'_> {
        let store = self.base();
        if shard_lock::in_transaction() {
            return ScriptShards(Shards::Transaction);
        }
        let mut held = shard_lock::Partial::new();
        let mut wanted = start.to_vec();
        wanted.sort_unstable();
        wanted.dedup();
        // Ascending, so each is waited for, but not for long.
        while !wanted.iter().all(|&i| held.take(i, &store.shards[i].inner)) {
            held.let_go();
            std::thread::yield_now();
        }
        ScriptShards(Shards::Planned {
            store,
            held,
            wanted,
        })
    }

    /// Runs `f` as a transaction, using the store as always: `EXEC`, scripts,
    /// and commands on several keys. It's as if nothing else ran meanwhile: each
    /// shard it touches is this thread's until it ends (`src/shard_lock.rs`),
    /// and they run one at a time. Commands on other shards carry on.
    pub(crate) fn exclusive<R>(&self, f: impl FnOnce() -> R) -> R {
        // In one already (a script run by `EXEC`).
        if shard_lock::in_transaction() {
            return f();
        }
        let _transaction = shard_lock::transaction(&self.base().transactions);
        f()
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// One step of `SCAN`: calls `f` with the key and kind of about `count`
    /// keys and returns the cursor for the next step, 0 when every key has
    /// been seen. Keys go in order of their hash within each shard: a cursor
    /// is a shard (high 32 bits) and the slot hash to go on from (low 32).
    /// A key's hash never changes, so a whole scan returns every key that
    /// existed throughout exactly once, whatever was written, moved or
    /// resized meanwhile, and any cursor is safe to pass. Each step reads
    /// its shard's whole table (8 bytes per key) under the lock, so for big
    /// shards it takes at least a 1024th of the shard, which bounds a whole
    /// scan to 1024 reads of each table.
    pub fn scan(
        &self,
        cursor: u64,
        count: usize,
        now: Millis,
        mut f: impl FnMut(&[u8], Kind),
    ) -> u64 {
        let mut shard = (cursor >> 32) as usize;
        let mut from = cursor as u32;
        let mut left = count.max(1);
        while shard < self.shards.len() {
            let inner = self.lock(&self.shards[shard]);
            let want = left.max(inner.table.len() / 1024);
            let (taken, next) = inner.scan(from, want, now, &mut f);
            drop(inner);
            if let Some(next) = next {
                return (shard as u64) << 32 | u64::from(next);
            }
            // That was the rest of this shard.
            shard += 1;
            from = 0;
            left = left.saturating_sub(taken);
            if left == 0 {
                break;
            }
        }
        if shard < self.shards.len() {
            (shard as u64) << 32
        } else {
            0
        }
    }

    /// `RENAME` (and with `only_if_absent`, `RENAMENX`): gives `from`'s
    /// value, kind, TTL and usage to `to`, replacing whatever `to` held, and
    /// removes `from`, atomically. It holds both shards' locks, taken in
    /// shard order, which nothing else does, so it can't deadlock. When the
    /// entry will be too big to pack under its new name, its allocation is
    /// made before locking and used if the source hasn't changed meanwhile.
    /// Like Redis' `RENAME`, it doesn't check `maxmemory`.
    pub fn rename(
        &self,
        from: Key<'_>,
        to: Key<'_>,
        only_if_absent: bool,
        now: Millis,
    ) -> Result<Renamed, OutOfMemory> {
        let (from_hash, a) = self.locate(from.hash);
        let (to_hash, b) = self.locate(to.hash);
        // A big entry's new allocation, and the value it was copied from.
        let mut prepared: Option<(SharedValue, KeyValue)> = None;
        let mut attempts = 0;
        loop {
            let mut garbage = Vec::new();
            let (mut low, mut high) = self.lock_two(a, b);
            let (src, mut dst): (&mut ShardInner, Option<&mut ShardInner>) =
                match high.as_deref_mut() {
                    None => (&mut *low, None),
                    Some(high) if a < b => (&mut *low, Some(high)),
                    Some(high) => (high, Some(&mut *low)),
                };
            // The destination's shard, which may be the source's.
            macro_rules! target {
                () => {
                    match dst.as_deref_mut() {
                        Some(dst) => dst,
                        None => &mut *src,
                    }
                };
            }
            let Some((loc, header)) = src.lookup(from_hash, from.bytes) else {
                return Ok(Renamed::NoSuchKey);
            };
            if header.is_expired(now) {
                src.watched.changed(from.bytes, Change::Expired);
                src.unlink(from_hash, loc);
                garbage.extend(src.kill(loc));
                src.mem.expired += 1;
                self.sync_usage(&self.shards[a], src, false);
                drop((low, high));
                return Ok(Renamed::NoSuchKey);
            }
            if from.bytes == to.bytes {
                return Ok(if only_if_absent {
                    Renamed::Exists
                } else {
                    Renamed::Done
                });
            }
            let existing = target!().lookup(to_hash, to.bytes);
            let live = existing.as_ref().filter(|(_, h)| !h.is_expired(now));
            if only_if_absent && live.is_some() {
                return Ok(Renamed::Exists);
            }
            // A big collection moves as it is: its object goes to the
            // destination's shard, under the new name.
            let moved = header.kind().is_collection() && matches!(header.body, Body::External(_));
            let new = if let (true, Body::External(i)) = (moved, &header.body) {
                let object = src.take_object(*i);
                let target = target!();
                let index = target.store_object(to.bytes, object.value);
                let entry = Payload::External(index);
                match target.append(header.kind(), header.usage(), &entry, header.deadline) {
                    Some(new) => new,
                    None => {
                        // Out of segment ids (practically unreachable): put it back.
                        let object = target!().take_object(index);
                        src.put_object(*i, from.bytes, object.value);
                        return Err(OutOfMemory);
                    }
                }
            } else {
                // The value, out from under the source's segment, which the
                // destination's appends may share.
                let value = src.value_ref(loc, &header).share();
                let too_big = to.bytes.len() + value.as_bytes().len() + 20 > INLINE_MAX;
                let payload = if too_big && header.kind().is_collection() {
                    // A packed collection whose new key is too long to pack
                    // it with becomes a big one (it's at most a few KB).
                    let big = Collection::from_compact(header.kind(), value.as_bytes());
                    Payload::External(target!().store_object(to.bytes, big))
                } else if too_big {
                    let kv = match prepared.take() {
                        Some((copied, kv)) if same_bytes(copied.as_bytes(), value.as_bytes()) => kv,
                        _ if attempts < 2 => {
                            // Copy it without the locks, then check again.
                            drop((low, high));
                            attempts += 1;
                            let kv = KeyValue::new(to.bytes, value.as_bytes());
                            prepared = Some((value, kv));
                            continue;
                        }
                        _ => KeyValue::new(to.bytes, value.as_bytes()),
                    };
                    Payload::External(target!().store_external(kv))
                } else {
                    Payload::Inline {
                        key: to.bytes,
                        value: value.as_bytes(),
                    }
                };
                let appended =
                    target!().append(header.kind(), header.usage(), &payload, header.deadline);
                let Some(new) = appended else {
                    // Out of segment ids: practically unreachable (4 GB per shard).
                    if let Payload::External(index) = payload {
                        garbage.push(target!().take_outside(header.kind(), index));
                    }
                    return Err(OutOfMemory);
                };
                new
            };
            let old = existing.as_ref().map(|(old, _)| *old);
            let expired = existing.is_some() && live.is_none();
            let target = target!();
            garbage.extend(target.replace(to_hash, old, new));
            if expired {
                target.mem.expired += 1;
                target.watched.changed(to.bytes, Change::Expired);
            }
            target.mem.writes += 1;
            target.watched.changed(
                to.bytes,
                if live.is_some() {
                    Change::Written
                } else {
                    Change::Created
                },
            );
            // Like Redis, as if deleted and added again: clients blocked on
            // the destination may be served.
            if !target.waiting.is_empty() {
                self.created(target, to.bytes);
            }
            src.unlink(from_hash, loc);
            if moved {
                src.mark_dead(loc); // its object went with it
            } else {
                garbage.extend(src.kill(loc));
            }
            src.mem.writes += 1;
            src.watched.changed(from.bytes, Change::Removed);
            let eager = self.near_limit();
            src.maybe_defrag(eager, now, &self.hasher, &mut garbage);
            self.sync_usage(&self.shards[a], src, false);
            if let Some(dst) = dst {
                dst.maybe_defrag(eager, now, &self.hasher, &mut garbage);
                self.sync_usage(&self.shards[b], dst, false);
            }
            drop((low, high));
            drop(garbage); // frees values outside the locks
            return Ok(Renamed::Done);
        }
    }

    /// Calls `f` with the key, kind, value and deadline (`NO_EXPIRY` for
    /// none) of every live entry in shard `index`, holding its lock
    /// throughout: keep `f` quick, such as copying into a buffer.
    pub fn for_each_live(
        &self,
        index: usize,
        now: Millis,
        mut f: impl FnMut(&[u8], Kind, &[u8], Millis),
    ) {
        let inner = self.lock(&self.shards[index]);
        for seg in inner.segments.iter().flatten() {
            for (_, header) in seg.entries() {
                if header.is_dead() || header.is_expired(now) {
                    continue;
                }
                let packed;
                let key =
                    ShardInner::entry_key(&seg.buf, &header, &inner.externals, &inner.objects);
                let value = match &header.body {
                    Body::Inline { value, .. } => &seg.buf[value.clone()],
                    Body::External(i) if header.kind().is_collection() => {
                        packed = inner.objects[*i as usize]
                            .as_ref()
                            .expect("live object")
                            .value
                            .to_compact();
                        &packed
                    }
                    Body::External(i) => inner.externals[*i as usize]
                        .as_ref()
                        .expect("live external")
                        .value(),
                };
                f(key, header.kind(), value, header.deadline);
            }
        }
    }

    /// Calls `f` with the key and kind of every live entry in shard
    /// `index`, under its lock.
    pub fn for_each_key(&self, index: usize, now: Millis, mut f: impl FnMut(&[u8], Kind)) {
        let inner = self.lock(&self.shards[index]);
        for seg in inner.segments.iter().flatten() {
            for (_, header) in seg.entries() {
                if !header.is_dead() && !header.is_expired(now) {
                    let key =
                        ShardInner::entry_key(&seg.buf, &header, &inner.externals, &inner.objects);
                    f(key, header.kind());
                }
            }
        }
    }

    /// How many changes commands have made so far (see `Mem::writes`), with
    /// each change to the function libraries.
    pub fn changes(&self) -> u64 {
        let data: u64 = self
            .live()
            .flat_map(|db| db.shards.iter().map(move |s| db.lock(s).mem.writes))
            .sum();
        data + self.base().functions.lock().0
    }

    /// The function libraries' code, and its version.
    pub(crate) fn function_codes(&self) -> (u64, Vec<Arc<[u8]>>) {
        let functions = self.base().functions.lock();
        (functions.0, functions.1.clone())
    }

    /// Replaces the function libraries' code; returns its new version.
    pub(crate) fn set_function_codes(&self, codes: Vec<Arc<[u8]>>) -> u64 {
        let mut functions = self.base().functions.lock();
        functions.0 += 1;
        functions.1 = codes;
        functions.0
    }

    pub fn maxmemory(&self) -> usize {
        self.maxmemory.load(Relaxed)
    }

    pub fn policy(&self) -> EvictionPolicy {
        EvictionPolicy::from_u8(self.policy.load(Relaxed))
    }

    /// A deadline on the store clock as a Unix timestamp in milliseconds.
    pub fn deadline_to_unix_ms(&self, _now: Millis, deadline: Millis) -> i64 {
        (self.unix_base as i64).saturating_add(deadline as i64)
    }

    /// Evicts until `incoming` more bytes fit under `maxmemory`. Holds at most
    /// one shard lock at a time. `seed` randomizes which shards are visited.
    ///
    /// Shards are picked by two random choices (see module docs). If that
    /// keeps reclaiming nothing (say, every segment is hot and just gets
    /// aged), every shard is swept in turn, each visit going through all of
    /// its segments. Each sweep ages every counter once, so after
    /// `MAX_USAGE + 1` sweeps something must be reclaimable; if not, there's
    /// nothing evictable at all (e.g. no TTL keys for volatile-lru).
    fn make_room(&self, home: usize, seed: u32, incoming: usize, now: Millis) -> bool {
        // A function flagged `allow-oom` writes past the limit, as in Redis.
        if ALLOW_OOM.get() {
            return true;
        }
        let made = self.evict_for(home, seed, incoming, now);
        // Evictions are logged in the database of the one they came from.
        crate::aof::select(self.index());
        made
    }

    fn evict_for(&self, home: usize, seed: u32, incoming: usize, now: Millis) -> bool {
        let policy = self.policy();
        let Some(choice) = policy.choice() else {
            return false;
        };
        let volatile_only = policy.volatile_only();
        // How much a shard has to give: bytes, or for the volatile policies, TTL keys.
        let weight = |(db, i): (&Store, usize)| {
            let shard = &db.shards[i];
            if volatile_only {
                shard.with_ttl.load(Relaxed)
            } else {
                shard.footprint.load(Relaxed)
            }
        };
        let needed = (self.used_memory() + incoming).saturating_sub(self.maxmemory());
        let mut freed = 0;
        let mut rng = (u64::from(seed) << 1) | 1;
        let mut picks = rng.rotate_left(29) ^ 0x9e37_79b9_7f4a_7c15;
        // xorshift64: a cheap, good-enough source of shard choices.
        // Every database gives, as in Redis, whichever one needs the room.
        let dbs: Vec<&Store> = self.live().collect();
        let places = dbs.len() * self.shards.len();
        let mut random_place = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            (rng >> 32) as usize % places
        };
        let at = |place: usize| (dbs[place / self.shards.len()], place % self.shards.len());
        let mut fruitless = 0;
        let mut sweep: Option<usize> = None;
        while freed < needed {
            let (db, index) = match &mut sweep {
                Some(next) => {
                    let place = *next;
                    *next = (place + 1) % places;
                    at(place)
                }
                None => {
                    // Start with the writer's shard as one choice, but stop
                    // favoring it once a visit comes up empty.
                    let a = if fruitless == 0 {
                        (self, home)
                    } else {
                        at(random_place())
                    };
                    let b = at(random_place());
                    if weight(b) > weight(a) { b } else { a }
                }
            };
            crate::aof::select(db.index());
            let shard = &db.shards[index];
            let mut garbage = Vec::new();
            // A native script's thread holds only its own shards: it takes
            // others only if they're free, so as never to wait while holding.
            // Nor does a transaction keep a shard just for evicting from it.
            let guard = if shard_lock::holding_some() || shard_lock::in_transaction() {
                db.try_lock_here(shard)
            } else {
                Some(db.lock(shard))
            };
            let (reclaimed, released) = match guard {
                Some(mut guard) => {
                    let inner = &mut *guard;
                    let before = inner.footprint();
                    // A second xorshift stream, for the random policies' segment.
                    picks ^= picks << 13;
                    picks ^= picks >> 7;
                    picks ^= picks << 17;
                    let how = Eviction {
                        volatile_only,
                        choice,
                        pick: picks >> 16,
                    };
                    let reclaimed =
                        inner.evict_segment(how, sweep.is_some(), now, &self.hasher, &mut garbage);
                    inner.shrink_if_sparse();
                    let after = inner.footprint();
                    db.sync_usage(shard, inner, true);
                    (reclaimed, before.saturating_sub(after))
                }
                None => (0, 0),
            };
            drop(garbage); // frees values outside the lock
            // Survivors can land in a freshly allocated segment, so a visit
            // may reclaim entries yet not shrink the footprint yet; it's
            // still progress.
            freed += released;
            fruitless = if reclaimed > 0 { 0 } else { fruitless + 1 };
            match sweep {
                Some(_) if fruitless > places * (usize::from(MAX_USAGE) + 1) => {
                    return false;
                }
                None if fruitless > EVICT_TRIES.min(self.shards.len()) => {
                    sweep = Some(random_place());
                    fruitless = 0;
                }
                _ => {}
            }
        }
        true
    }

    /// Removes expired keys and compacts dead space in every shard. Returns
    /// how many keys were removed.
    pub fn expire_cycle(&self, now: Millis) -> usize {
        let mut removed = 0;
        for shard in self.shards.iter() {
            let mut garbage = Vec::new();
            {
                let mut guard = self.lock(shard);
                let inner = &mut *guard;
                removed += inner.sweep_expired(now, &self.hasher, &mut garbage);
                inner.maybe_defrag(self.near_limit(), now, &self.hasher, &mut garbage);
                inner.shrink_if_sparse();
                self.sync_usage(shard, inner, false);
            }
            drop(garbage); // frees values outside the lock
        }
        removed
    }

    /// Number of keys, including expired keys not yet swept.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| self.lock(s).table.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Exact figures, unlike [`Store::used_memory`]: it locks every shard.
    pub fn stats(&self) -> Stats {
        let client_bytes = self.base().client_memory.load(Relaxed).max(0) as usize;
        let mut stats = Stats {
            maxmemory: self.maxmemory(),
            policy: self.policy(),
            shards: self.shards.len(),
            used_memory: client_bytes,
            client_bytes,
            ..Stats::default()
        };
        for db in self.live() {
            for shard in db.shards.iter() {
                let inner = db.lock(shard);
                stats.used_memory += inner.footprint();
                stats.keys += inner.table.len();
                stats.keys_with_ttl += inner.mem.with_ttl;
                stats.table_bytes += pages::reserved(inner.table.allocation_size());
                stats.segment_bytes += inner.mem.segment_resident;
                stats.live_bytes += inner.mem.live;
                stats.external_bytes += inner.mem.external;
                stats.expired_keys += inner.mem.expired;
                stats.evicted_keys += inner.mem.evicted;
            }
        }
        stats
    }

    /// What this database's table of keys takes.
    pub fn table_bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| pages::reserved(self.lock(shard).table.allocation_size()))
            .sum()
    }

    /// This database's keys, and how many of them have a TTL.
    /// Asks every shard for a copy of its data as it is now, to read later
    /// without its lock: its segments' bytes as they are, the values too big
    /// for them (shared, by count: they're replaced, never changed), and its
    /// big collections (cloned). For a replica's full sync, while no write
    /// runs: each shard makes its copy the next time anything locks it, so
    /// before any write changes it ([`Store::take_copy`] locks it to). This
    /// only marks them. Also returns how many keys there are, and with a TTL.
    pub(crate) fn request_copies(&self) -> (usize, usize, Vec<Arc<CopySlot>>) {
        let (mut keys, mut with_ttl) = (0, 0);
        let slots = self
            .shards
            .iter()
            .map(|shard| {
                let inner = self.lock(shard);
                keys += inner.table.len();
                with_ttl += inner.mem.with_ttl;
                let slot = Arc::new(CopySlot::default());
                OWED.lock()
                    .push((&*inner as *const ShardInner as usize, slot.clone()));
                crate::shard_lock::copy_owed();
                slot
            })
            .collect();
        (keys, with_ttl, slots)
    }

    /// The copy shard `index` made into `slot` ([`Store::request_copies`]),
    /// made now if nothing has locked it since.
    pub(crate) fn take_copy(&self, index: usize, slot: &CopySlot) -> ShardCopy {
        if slot.0.lock().is_none() {
            drop(self.lock(&self.shards[index]));
        }
        slot.0
            .lock()
            .take()
            .expect("a shard makes the copy it owes when locked")
    }

    pub fn key_counts(&self) -> (usize, usize) {
        self.shards.iter().fold((0, 0), |(keys, with_ttl), shard| {
            let inner = self.lock(shard);
            (keys + inner.table.len(), with_ttl + inner.mem.with_ttl)
        })
    }
}

#[derive(Debug, Default)]
pub struct Stats {
    pub keys: usize,
    pub keys_with_ttl: usize,
    /// Accounted bytes: segments, external values, hash tables and
    /// connection buffers.
    pub used_memory: usize,
    pub maxmemory: usize,
    pub policy: EvictionPolicy,
    pub shards: usize,
    /// Bytes allocated for the hash tables themselves.
    pub table_bytes: usize,
    /// RAM used by segments (their written pages), and how much of that is
    /// live entries.
    pub segment_bytes: usize,
    pub live_bytes: usize,
    /// Bytes allocated for entries too big to pack.
    pub external_bytes: usize,
    /// Connection buffers.
    pub client_bytes: usize,
    pub expired_keys: u64,
    pub evicted_keys: u64,
}

#[cfg(test)]
mod tests;

/// A shard's data as it was ([`Store::copy_shard`]).
pub(crate) struct ShardCopy {
    segments: Vec<Option<Vec<u8>>>,
    externals: Vec<Option<KeyValue>>,
    objects: Vec<Option<Object>>,
}

impl ShardCopy {
    /// Calls `f` with each live entry's key, value and deadline, as
    /// [`Store::for_each_live_ref`] does with the shard (skipping what had
    /// expired at `now`, unless it's 0).
    pub(crate) fn for_each_live_ref(
        &self,
        now: Millis,
        mut f: impl FnMut(&[u8], ValueRef<'_>, Millis),
    ) {
        for buf in self.segments.iter().flatten() {
            let mut offset = 0;
            while offset < buf.len() {
                let header = segment::parse(buf, offset);
                offset += header.len;
                if header.is_dead() || (now != 0 && header.is_expired(now)) {
                    continue;
                }
                let key = ShardInner::entry_key(buf, &header, &self.externals, &self.objects);
                let value = match &header.body {
                    Body::Inline { value, .. } => Borrowed::Inline(&buf[value.clone()]),
                    Body::External(i) if header.kind().is_collection() => Borrowed::Object(
                        &self.objects[*i as usize]
                            .as_ref()
                            .expect("live object")
                            .value,
                    ),
                    Body::External(i) => Borrowed::External(
                        self.externals[*i as usize].as_ref().expect("live external"),
                    ),
                };
                f(key, ValueRef(value, header.kind()), header.deadline);
            }
        }
    }
}
