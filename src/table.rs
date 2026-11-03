//! The per-shard hash table: bucketized cuckoo hashing over 8-byte slots.
//!
//! Each key can live in one of two buckets of 8 slots (64 bytes, a cache line),
//! both picked from its 32-bit slot hash, so a lookup reads at most two buckets.
//! When an insert finds both full, it moves existing slots to their other
//! bucket, found by a short breadth-first search, which lets the table run ~94%
//! full. It grows by 25% at a time (in whole pages), so it averages ~84% full. A doubling table
//! averages ~65%, and since all shards double together, every one was 48% full
//! at 1M keys. Deletes just clear a slot, so there are no tombstones.
//!
//! Bucket indexes come from the stored hash by fast range reduction (Lemire),
//! so the table can be any size and a resize never reads keys. A slot that
//! can't be placed even after growing (17+ keys sharing a 32-bit hash; with a
//! randomly seeded hash, practically never) goes to a small overflow list, so
//! inserts can't loop forever.

use allocator_api2::vec::Vec as PagedVec;

use crate::pages::{self, PageAlloc};

/// A key's 32-bit hash and where its entry lives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct Slot {
    pub hash: u32,
    pub loc: u32,
}

/// Marks a free slot. No real entry can start here: it's the last byte of a
/// full-size segment, and entries are at least 3 bytes.
pub const EMPTY: u32 = u32::MAX;
const VACANT: Slot = Slot {
    hash: 0,
    loc: EMPTY,
};

pub const WAYS: usize = 8;

/// One cache line of slots, aligned so a bucket never straddles two.
#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct Bucket([Slot; WAYS]);

const VACANT_BUCKET: Bucket = Bucket([VACANT; WAYS]);

impl std::ops::Deref for Bucket {
    type Target = [Slot; WAYS];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Bucket {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'a> IntoIterator for &'a Bucket {
    type Item = &'a Slot;
    type IntoIter = std::slice::Iter<'a, Slot>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// Grow beyond 15/16 full.
const MAX_LOAD_NUM: usize = 15;
const MAX_LOAD_DEN: usize = 16;
/// Buckets the displacement search may visit per insert.
const SEARCH_LIMIT: usize = 256;

/// `h` scaled to `0..n` without division.
fn range(h: u32, n: usize) -> usize {
    ((u64::from(h) * n as u64) >> 32) as usize
}

/// A bijective mix (from Chris Wellons' hash-prospector) for the second
/// bucket choice.
fn mix(mut h: u32) -> u32 {
    h ^= h >> 16;
    h = h.wrapping_mul(0x7feb_352d);
    h ^= h >> 15;
    h = h.wrapping_mul(0x846c_a68b);
    h ^ (h >> 16)
}

fn buckets_of(hash: u32, n: usize) -> [usize; 2] {
    [range(hash, n), range(mix(hash), n)]
}

/// Asks the CPU to start loading the buckets `hash` can be in, for a table
/// whose [`Table::raw_parts`] were `(base, n)` at some point. It reads
/// nothing, so parts that went stale since only waste the hint.
pub fn prefetch(base: usize, n: usize, hash: u32) {
    if n > 0 {
        for b in buckets_of(hash, n) {
            prefetch_line(base.wrapping_add(b * size_of::<Bucket>()));
        }
    }
}

/// A prefetch hint for the cache line at `addr`, which needn't be valid:
/// prefetches never fault and aren't memory accesses.
pub fn prefetch_line(addr: usize) {
    #[cfg(target_arch = "aarch64")]
    // SAFETY: PRFM only hints the cache; it can't fault or change memory.
    unsafe {
        std::arch::asm!("prfm pldl1keep, [{0}]", in(reg) addr, options(nostack, preserves_flags, readonly));
    }
    #[cfg(target_arch = "x86_64")]
    // SAFETY: as above, for PREFETCHT0 (SSE is always there on x86_64).
    unsafe {
        use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
        _mm_prefetch::<_MM_HINT_T0>(addr as *const i8);
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    let _ = addr;
}

fn free_way(bucket: &Bucket) -> Option<usize> {
    bucket.iter().position(|slot| slot.loc == EMPTY)
}

pub struct Table {
    buckets: PagedVec<Bucket, PageAlloc>,
    overflow: Vec<Slot>,
    len: usize,
}

impl Default for Table {
    fn default() -> Self {
        Table {
            buckets: PagedVec::new_in(PageAlloc),
            overflow: Vec::new(),
            len: 0,
        }
    }
}

impl Table {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Slots in the bucket array.
    pub fn capacity(&self) -> usize {
        self.buckets.len() * WAYS
    }

    /// The bucket array's address and length, for [`prefetch`] by threads
    /// that can't read the table itself. `(0, 0)` while it has no buckets.
    pub fn raw_parts(&self) -> (usize, usize) {
        match self.buckets.len() {
            0 => (0, 0),
            n => (self.buckets.as_ptr() as usize, n),
        }
    }

    /// Bytes allocated, for memory accounting.
    pub fn allocation_size(&self) -> usize {
        self.buckets.capacity() * size_of::<Bucket>() + self.overflow.capacity() * size_of::<Slot>()
    }

    /// Every bucket's slots, in use or not (`loc == EMPTY`), for tight loops
    /// over the whole table. The rest are in [`Table::overflow`].
    pub fn buckets(&self) -> impl Iterator<Item = &[Slot; WAYS]> {
        self.buckets.iter().map(|bucket| &bucket.0)
    }

    /// Slots that didn't fit a bucket (practically never any).
    pub fn overflow(&self) -> &[Slot] {
        &self.overflow
    }

    /// Every slot in use, in no particular order.
    pub fn iter(&self) -> impl Iterator<Item = &Slot> {
        self.buckets
            .iter()
            .flatten()
            .filter(|slot| slot.loc != EMPTY)
            .chain(&self.overflow)
    }

    /// The slot with this hash for which `eq` holds.
    pub fn find(&self, hash: u32, mut eq: impl FnMut(&Slot) -> bool) -> Option<&Slot> {
        let n = self.buckets.len();
        if n > 0 {
            for b in buckets_of(hash, n) {
                for slot in &self.buckets[b] {
                    if slot.hash == hash && slot.loc != EMPTY && eq(slot) {
                        return Some(slot);
                    }
                }
            }
        }
        self.overflow
            .iter()
            .find(|slot| slot.hash == hash && eq(slot))
    }

    /// Where the slot for `(hash, loc)` is: a bucket and way, or an overflow index.
    fn position(&self, hash: u32, loc: u32) -> Option<Position> {
        let n = self.buckets.len();
        if n > 0 {
            for b in buckets_of(hash, n) {
                if let Some(way) = self.buckets[b].iter().position(|slot| slot.loc == loc) {
                    return Some(Position::Bucket(b, way));
                }
            }
        }
        self.overflow
            .iter()
            .position(|slot| slot.loc == loc)
            .map(Position::Overflow)
    }

    /// Points the slot for `(hash, old)` at `new`.
    pub fn replace_loc(&mut self, hash: u32, old: u32, new: u32) -> bool {
        match self.position(hash, old) {
            Some(Position::Bucket(b, way)) => self.buckets[b][way].loc = new,
            Some(Position::Overflow(i)) => self.overflow[i].loc = new,
            None => return false,
        }
        true
    }

    pub fn remove(&mut self, hash: u32, loc: u32) -> bool {
        match self.position(hash, loc) {
            Some(Position::Bucket(b, way)) => self.buckets[b][way] = VACANT,
            Some(Position::Overflow(i)) => {
                self.overflow.swap_remove(i);
            }
            None => return false,
        }
        self.len -= 1;
        true
    }

    /// Adds a slot. The caller guarantees no slot has this `loc` yet.
    pub fn insert(&mut self, hash: u32, loc: u32) {
        debug_assert_ne!(loc, EMPTY);
        let slot = Slot { hash, loc };
        if (self.len + 1) * MAX_LOAD_DEN > self.capacity() * MAX_LOAD_NUM {
            self.grow();
        }
        // A failed placement in a half-empty table means a cluster of equal
        // hashes, which growing can't fix (they keep sharing two buckets):
        // only grow for a fuller table, and otherwise overflow.
        let placed = self.place(slot)
            || ((self.len + 1) * 2 > self.capacity() && {
                self.grow();
                self.place(slot)
            });
        if !placed {
            self.overflow.push(slot);
        }
        self.len += 1;
    }

    /// Frees a table that's empty and shrinks one that's under a quarter full.
    pub fn shrink_if_sparse(&mut self) {
        if self.is_empty() && self.allocation_size() > 0 {
            *self = Table::default();
        } else if self.capacity() > 1024 && self.len * 4 < self.capacity() {
            // Rebuild at about 70% full.
            self.rebuild((self.len * 10 / 7).div_ceil(WAYS));
        }
    }

    fn grow(&mut self) {
        let n = self.buckets.len();
        self.rebuild((n + n / 4).max(n + 1).max(2));
    }

    /// Moves every slot into a fresh array of about `n` buckets.
    fn rebuild(&mut self, n: usize) {
        // Arrays of a page or more come out at a whole number of pages, so
        // they're always mapped from the OS (see `pages.rs`). Otherwise
        // 25% growth steps would leave a trail of freed mid-size arrays stuck
        // in the global allocator (~4 MB extra at 200k keys).
        let per_page = (pages::page_size() / size_of::<Bucket>()).max(1);
        let n = if n >= per_page {
            n.next_multiple_of(per_page)
        } else {
            n
        };
        let mut buckets = PagedVec::with_capacity_in(n, PageAlloc);
        buckets.resize(n, VACANT_BUCKET);
        let old_buckets = std::mem::replace(&mut self.buckets, buckets);
        let old_overflow = std::mem::take(&mut self.overflow);
        for slot in old_buckets
            .iter()
            .flatten()
            .filter(|slot| slot.loc != EMPTY)
            .chain(&old_overflow)
        {
            if !self.place(*slot) {
                self.overflow.push(*slot);
            }
        }
    }

    /// Puts `slot` in one of its buckets, moving others along if both are
    /// full. Returns false if no room turned up within the search limit.
    fn place(&mut self, slot: Slot) -> bool {
        let n = self.buckets.len();
        if n == 0 {
            return false;
        }
        let roots = buckets_of(slot.hash, n);
        for b in roots {
            if let Some(way) = free_way(&self.buckets[b]) {
                self.buckets[b][way] = slot;
                return true;
            }
        }

        // Breadth-first search for a chain of moves ending in a free way.
        let mut nodes = [Node {
            bucket: 0,
            parent: ROOT,
            way: 0,
        }; SEARCH_LIMIT];
        let mut count = 0;
        for b in roots {
            if count == 0 || nodes[0].bucket != b {
                nodes[count] = Node {
                    bucket: b,
                    parent: ROOT,
                    way: 0,
                };
                count += 1;
            }
        }
        let mut head = 0;
        while head < count {
            let node = nodes[head];
            for way in 0..WAYS {
                let moved = self.buckets[node.bucket][way];
                let [x, y] = buckets_of(moved.hash, n);
                let other = if x == node.bucket { y } else { x };
                if other == node.bucket || on_path(&nodes, head, other) {
                    continue;
                }
                if let Some(free) = free_way(&self.buckets[other]) {
                    // Shift slots along the chain, from the free end back to
                    // a root, then put the new slot in the root's freed way.
                    let (mut to_bucket, mut to_way) = (other, free);
                    let (mut at, mut from_way) = (head, way);
                    loop {
                        let from_bucket = nodes[at].bucket;
                        self.buckets[to_bucket][to_way] = self.buckets[from_bucket][from_way];
                        (to_bucket, to_way) = (from_bucket, from_way);
                        if nodes[at].parent == ROOT {
                            break;
                        }
                        from_way = nodes[at].way;
                        at = nodes[at].parent;
                    }
                    self.buckets[to_bucket][to_way] = slot;
                    return true;
                }
                if count < SEARCH_LIMIT {
                    nodes[count] = Node {
                        bucket: other,
                        parent: head,
                        way,
                    };
                    count += 1;
                }
            }
            head += 1;
        }
        false
    }
}

/// A step of the displacement search: a bucket reached by moving the slot at
/// `way` of the parent node's bucket into it (that slot's other bucket).
#[derive(Clone, Copy)]
struct Node {
    bucket: usize,
    parent: usize,
    way: usize,
}

/// `Node::parent` of the search's starting buckets.
const ROOT: usize = usize::MAX;

/// Whether `bucket` is `nodes[at]` or one of its ancestors. A chain must not
/// pass through a bucket twice, or shifting along it would move a slot twice.
fn on_path(nodes: &[Node], mut at: usize, bucket: usize) -> bool {
    loop {
        if nodes[at].bucket == bucket {
            return true;
        }
        match nodes[at].parent {
            ROOT => return false,
            parent => at = parent,
        }
    }
}

enum Position {
    Bucket(usize, usize),
    Overflow(usize),
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    fn check(table: &Table, model: &HashMap<u32, u32>) {
        assert_eq!(table.len(), model.len());
        assert_eq!(table.iter().count(), model.len());
        for (&loc, &hash) in model {
            assert_eq!(table.find(hash, |s| s.loc == loc).map(|s| s.loc), Some(loc));
        }
    }

    #[test]
    fn matches_a_model_through_growth_and_shrinking() {
        let mut table = Table::default();
        // (loc, hash) of every slot, so removals can pick at random in O(1).
        let mut live: Vec<(u32, u32)> = Vec::new();
        let mut rng = Rng(0x2545_F491_4F6C_DD1D);
        let mut next_loc = 0u32;
        let model = |live: &[(u32, u32)]| live.iter().copied().collect::<HashMap<u32, u32>>();
        for step in 0..300_000u32 {
            let pick = |rng: &mut Rng, len: usize| (rng.next() % len as u64) as usize;
            match rng.next() % 10 {
                // Mostly inserts while growing, then mostly removes.
                0..6 if step < 200_000 => {
                    let hash = rng.next() as u32;
                    table.insert(hash, next_loc);
                    live.push((next_loc, hash));
                    next_loc += 1;
                }
                6 if !live.is_empty() => {
                    let i = pick(&mut rng, live.len());
                    let (loc, hash) = live[i];
                    assert!(table.replace_loc(hash, loc, next_loc));
                    live[i] = (next_loc, hash);
                    next_loc += 1;
                }
                _ if !live.is_empty() => {
                    let (loc, hash) = live.swap_remove(pick(&mut rng, live.len()));
                    assert!(table.remove(hash, loc));
                }
                _ => {}
            }
            if step % 997 == 0 {
                table.shrink_if_sparse();
            }
            if step % 50_000 == 0 {
                check(&table, &model(&live));
            }
        }
        check(&table, &model(&live));
        table.shrink_if_sparse();
        check(&table, &model(&live));
        assert!(!table.remove(1, 123_456_789));
    }

    #[test]
    fn stays_well_filled() {
        let mut table = Table::default();
        let mut rng = Rng(7);
        let mut lowest = 1.0f64;
        for loc in 0..500_000u32 {
            table.insert(rng.next() as u32, loc);
            // Past ~6k buckets, rounding sizes up to whole pages adds at most
            // a few percent even with 16 KB pages.
            if loc > 50_000 {
                lowest = lowest.min(table.len() as f64 / table.capacity() as f64);
            }
        }
        // Growing by 25% at 15/16 full stays near 75%.
        assert!(lowest > 0.71, "load fell to {lowest:.2}");
        assert!(table.overflow.is_empty());
    }

    #[test]
    fn identical_hashes_overflow_instead_of_looping() {
        let mut table = Table::default();
        let mut model = HashMap::new();
        for loc in 0..100u32 {
            table.insert(42, loc);
            model.insert(loc, 42);
        }
        // Two buckets hold 16; the rest must have gone somewhere findable,
        // without the table growing to compensate.
        assert!(!table.overflow.is_empty());
        assert!(
            table.capacity() <= 400,
            "grew to {} slots",
            table.capacity()
        );
        check(&table, &model);
        for loc in 0..100u32 {
            assert!(table.remove(42, loc));
        }
        assert!(table.is_empty());
    }

    #[test]
    fn empty_table_frees_its_memory() {
        let mut table = Table::default();
        assert_eq!(table.allocation_size(), 0);
        assert!(table.find(1, |_| true).is_none());
        for loc in 0..1000 {
            table.insert(loc * 7919, loc);
        }
        for loc in 0..1000 {
            assert!(table.remove(loc * 7919, loc));
        }
        table.shrink_if_sparse();
        assert_eq!(table.allocation_size(), 0);
    }
}
