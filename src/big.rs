//! What big hashes, sets and sorted sets are made of, so that they never stop for
//! long. One buffer and one table would have to grow all at once: copying the
//! buffers and, for the table, reading every field again to hash it, 614 ms at 8M
//! fields under the shard's lock. So everything here comes in small pieces, each
//! grown or rewritten on its own:
//!
//! - [`Records`]: the bytes, in chunks that stay where they are;
//! - [`Paged`]: a list, in pages that stay where they are;
//! - [`Index`]: a hash table of positions in the list, made of small tables that
//!   each split in two when full.
//!
//! These cost a few more loads per command, all of them of small arrays that stay
//! in cache, next to the misses a big collection's own memory takes anyway.

use hashbrown::HashTable;

use crate::table::prefetch_line;

const PAGE_BITS: usize = 16;
const PAGE: usize = 1 << PAGE_BITS;

/// What [`Paged::iter`] returns.
pub type PagedIter<'a, T> = std::iter::Flatten<std::slice::Iter<'a, Vec<T>>>;

/// A list in pages of 64K items. Pushing never moves what's there once the first
/// page is full, and a page is given back as soon as it's empty.
#[derive(Clone)]
pub struct Paged<T> {
    pages: Vec<Vec<T>>,
    len: usize,
}

impl<T> Default for Paged<T> {
    fn default() -> Self {
        Paged {
            pages: Vec::new(),
            len: 0,
        }
    }
}

impl<T: Copy> Paged<T> {
    pub fn with_capacity(n: usize) -> Self {
        let mut list = Paged::default();
        if n > 0 {
            list.pages.push(Vec::with_capacity(n.min(PAGE)));
        }
        list
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn get(&self, i: usize) -> T {
        self.pages[i >> PAGE_BITS][i & (PAGE - 1)]
    }

    #[inline]
    pub fn set(&mut self, i: usize, value: T) {
        self.pages[i >> PAGE_BITS][i & (PAGE - 1)] = value;
    }

    #[inline]
    pub fn push(&mut self, value: T) {
        let page = self.len >> PAGE_BITS;
        if page == self.pages.len() {
            // The first grows from nothing, as most collections stay within it.
            self.pages.push(if page == 0 {
                Vec::new()
            } else {
                Vec::with_capacity(PAGE)
            });
        }
        self.pages[page].push(value);
        self.len += 1;
    }

    /// Takes the last item out, and puts it at `i` if that isn't where it was.
    pub fn swap_remove(&mut self, i: usize) {
        let last = self.pages.last_mut().expect("not empty");
        let value = last.pop().expect("not empty");
        if last.is_empty() {
            self.pages.pop();
        }
        self.len -= 1;
        if i < self.len {
            self.set(i, value);
        }
        self.give_back_spare();
    }

    /// Takes out the item at `i`, moving those after it up: only within one page.
    pub fn remove_in_order(&mut self, i: usize) {
        assert_eq!(self.pages.len(), 1, "one page");
        self.pages[0].remove(i);
        self.len -= 1;
        if self.len == 0 {
            self.pages.clear();
        }
        self.give_back_spare();
    }

    /// The first page never goes while it's the only one, so it shrinks instead, at
    /// most a page's worth to copy.
    fn give_back_spare(&mut self) {
        if let [page] = &mut self.pages[..]
            && page.capacity() > 64
            && page.len() * 4 < page.capacity()
        {
            page.shrink_to(page.len() * 2);
        }
    }

    /// Starts loading item `i`, which is there.
    #[inline]
    fn prefetch(&self, i: usize) {
        let page = self.pages[i >> PAGE_BITS].as_ptr() as usize;
        prefetch_line(page + (i & (PAGE - 1)) * size_of::<T>());
    }

    pub fn iter(&self) -> PagedIter<'_, T> {
        self.pages.iter().flatten()
    }

    /// Memory it takes: full pages, and all of the last one, as it will fill.
    pub fn allocation_size(&self) -> usize {
        let spare = self.pages.last().map_or(0, |p| p.capacity() - p.len());
        (self.len + spare) * size_of::<T>()
    }
}

const CHUNK_BITS: usize = 22;
const CHUNK: usize = 1 << CHUNK_BITS;
/// How many positions a write that leaves a hole goes through to tidy up, once holes
/// are over half: a quarter of a million writes see to a million positions.
const TIDY_STEP: usize = 4;

/// The address of a record in [`Records`], as a list keeps it.
pub trait Address: Copy {
    fn new(at: usize) -> Self;
    fn get(self) -> usize;
}

#[derive(Clone, Default)]
struct Chunk {
    bytes: Vec<u8>,
    /// Bytes here that nothing refers to.
    holes: usize,
}

/// Records of bytes in chunks of up to 4 MB, each record whole in one chunk (one
/// over a chunk's size gets its own), addressed by chunk number and place in it.
/// What the owner stops using is a hole; a chunk of nothing but holes is given
/// back, and its number used again.
///
/// A chunk can't be read for its records, since a hole doesn't say how long it is,
/// so tidying goes through the owner's list: once over half of all is holes, each
/// write that makes one takes a few more places in the list, and moves the record
/// there to the end if it's in a chunk with more holes than not.
#[derive(Clone)]
pub struct Records {
    /// Never empty.
    chunks: Vec<Chunk>,
    /// The chunk being written to.
    tail: u32,
    /// Numbers of chunks given back.
    free: Vec<u32>,
    /// Bytes in chunks, holes included.
    written: usize,
    holes: usize,
    /// Where tidying goes on from, in the owner's list.
    tidied_to: u32,
}

impl Default for Records {
    fn default() -> Self {
        Records::with_capacity(0)
    }
}

impl Records {
    pub fn with_capacity(bytes: usize) -> Self {
        Records {
            chunks: vec![Chunk {
                bytes: Vec::with_capacity(bytes.min(CHUNK)),
                holes: 0,
            }],
            tail: 0,
            free: Vec::new(),
            written: 0,
            holes: 0,
            tidied_to: 0,
        }
    }

    /// From the record at `address` to the end of its chunk.
    #[inline]
    pub fn at(&self, address: usize) -> &[u8] {
        &self.chunks[address >> CHUNK_BITS].bytes[address & (CHUNK - 1)..]
    }

    /// Starts loading the record at `address`.
    #[inline]
    fn prefetch(&self, address: usize) {
        let chunk = self.chunks[address >> CHUNK_BITS].bytes.as_ptr() as usize;
        prefetch_line(chunk + (address & (CHUNK - 1)));
    }

    /// The same, to change a record where it is.
    #[inline]
    pub fn at_mut(&mut self, address: usize) -> &mut [u8] {
        &mut self.chunks[address >> CHUNK_BITS].bytes[address & (CHUNK - 1)..]
    }

    /// The address of a new record of `size` bytes, at least one, and where to write
    /// it, without going past that size.
    #[inline]
    pub fn append(&mut self, size: usize) -> (usize, &mut Vec<u8>) {
        let tail = &self.chunks[self.tail as usize].bytes;
        let c = if tail.capacity() - tail.len() >= size {
            self.tail as usize
        } else {
            self.room_for(size)
        };
        self.written += size;
        let bytes = &mut self.chunks[c].bytes;
        ((c << CHUNK_BITS) | bytes.len(), bytes)
    }

    /// The chunk to write `size` more bytes to, there being no room at the end.
    #[cold]
    fn room_for(&mut self, size: usize) -> usize {
        if size > CHUNK {
            return self.new_chunk(size);
        }
        let tail = &mut self.chunks[self.tail as usize].bytes;
        let wanted = tail.len() + size;
        if wanted > CHUNK {
            return self.next_tail(CHUNK);
        }
        // Small collections stay small: up to a chunk, it doubles.
        let capacity = wanted.max(tail.capacity() * 2).min(CHUNK);
        tail.reserve_exact(capacity - tail.len());
        self.tail as usize
    }

    fn new_chunk(&mut self, capacity: usize) -> usize {
        let chunk = Chunk {
            bytes: Vec::with_capacity(capacity),
            holes: 0,
        };
        match self.free.pop() {
            Some(c) => {
                self.chunks[c as usize] = chunk;
                c as usize
            }
            None => {
                self.chunks.push(chunk);
                self.chunks.len() - 1
            }
        }
    }

    fn next_tail(&mut self, capacity: usize) -> usize {
        let last = self.tail as usize;
        let tail = self.new_chunk(capacity);
        self.tail = tail as u32;
        if self.chunks[last].holes == self.chunks[last].bytes.len() {
            self.give_back(last);
        }
        tail
    }

    /// Nothing refers to these `size` bytes any more, from `address`.
    #[inline]
    pub fn hole(&mut self, address: usize, size: usize) {
        let c = address >> CHUNK_BITS;
        self.holes += size;
        let chunk = &mut self.chunks[c];
        chunk.holes += size;
        if chunk.holes == chunk.bytes.len() && c != self.tail as usize {
            self.give_back(c);
        }
    }

    fn give_back(&mut self, c: usize) {
        let chunk = std::mem::take(&mut self.chunks[c]);
        self.written -= chunk.bytes.len();
        self.holes -= chunk.holes;
        self.free.push(c as u32);
    }

    /// Takes the next step of tidying if it's due; `size_of` tells a record's length
    /// from its bytes. The list's order doesn't change, only where its records are.
    #[inline]
    pub fn tidy<A: Address>(&mut self, list: &mut Paged<A>, size_of: impl Fn(&[u8]) -> usize) {
        if self.holes > 4096 && self.holes * 2 > self.written {
            self.tidy_step(list, size_of);
        }
    }

    #[cold]
    fn tidy_step<A: Address>(&mut self, list: &mut Paged<A>, size_of: impl Fn(&[u8]) -> usize) {
        let tail = &self.chunks[self.tail as usize];
        if tail.holes * 2 > tail.bytes.len() {
            // Its records are to move out too, so they need somewhere to go.
            let used = tail.bytes.len() - tail.holes;
            self.next_tail(used.min(CHUNK));
        }
        let from = self.tidied_to as usize;
        if from >= list.len() {
            self.tidied_to = 0;
            return;
        }
        let to = (from + TIDY_STEP).min(list.len());
        // In parallel, so that a step costs about one miss rather than eight.
        for i in from..to {
            let address = list.get(i).get();
            if self.worth_moving(address) {
                self.prefetch(address);
            }
        }
        for i in from..to {
            if let Some(moved) = self.moved_out(list.get(i).get(), &size_of) {
                list.set(i, A::new(moved));
            }
        }
        self.tidied_to = to as u32;
    }

    /// Whether the record at `address` is in a chunk worth emptying.
    #[inline]
    fn worth_moving(&self, address: usize) -> bool {
        let c = address >> CHUNK_BITS;
        let chunk = &self.chunks[c];
        c != self.tail as usize && chunk.holes * 2 > chunk.bytes.len()
    }

    /// Where the record at `address` is now, if it was in a chunk worth emptying.
    fn moved_out(&mut self, address: usize, size_of: impl Fn(&[u8]) -> usize) -> Option<usize> {
        if !self.worth_moving(address) {
            return None;
        }
        let c = address >> CHUNK_BITS;
        let from = address & (CHUNK - 1);
        let size = size_of(&self.chunks[c].bytes[from..]);
        let old = std::mem::take(&mut self.chunks[c].bytes);
        let (moved, bytes) = self.append(size);
        bytes.extend_from_slice(&old[from..from + size]);
        self.chunks[c].bytes = old;
        self.hole(address, size);
        Some(moved)
    }

    /// Bytes in use.
    pub fn used(&self) -> usize {
        self.written - self.holes
    }

    /// Memory it takes: what's been written, as the rest of a chunk is memory never
    /// touched, and the room left at the end.
    pub fn allocation_size(&self) -> usize {
        let tail = &self.chunks[self.tail as usize].bytes;
        self.written + (tail.capacity() - tail.len())
    }

    #[cfg(test)]
    pub fn holes(&self) -> usize {
        self.holes
    }

    #[cfg(test)]
    pub fn written(&self) -> usize {
        self.written
    }

    /// Checks the totals, against the size of the records in use.
    #[cfg(test)]
    pub fn check(&self, used: usize) {
        assert_eq!(self.used(), used, "bytes in use");
        let written: usize = self.chunks.iter().map(|c| c.bytes.len()).sum();
        let holes: usize = self.chunks.iter().map(|c| c.holes).sum();
        assert_eq!(self.written, written, "bytes written");
        assert_eq!(self.holes, holes, "holes");
        for (c, chunk) in self.chunks.iter().enumerate() {
            assert!(chunk.holes <= chunk.bytes.len());
            if c != self.tail as usize && !self.free.contains(&(c as u32)) {
                assert!(
                    chunk.holes < chunk.bytes.len(),
                    "a chunk of nothing but holes"
                );
            }
        }
    }
}

/// The buckets of a table at its largest.
const BUCKETS: usize = 1 << 14;
/// What those hold, at their 7/8: past this a table splits, which is about 1 ms.
const SPLIT_AT: usize = BUCKETS / 8 * 7;
/// How far ahead a split starts loading records: each iteration takes about 30 ns once
/// they're in cache, and a miss about 100.
const AHEAD: usize = 8;
/// Hash bits from here up pick a table, below the seven hashbrown keeps at the top
/// and above the few a small table uses from the bottom.
const PICK_SHIFT: u32 = 32;
const MAX_DEPTH: u32 = 24;

/// What an [`Index`] needs to know of the positions in it.
pub trait Positions {
    /// The hash of what's at a position.
    fn hash(&self, position: u32) -> u64;
    /// Starts loading what `hash` will read first.
    fn prefetch_first(&self, _position: u32) {}
    /// Starts loading what `hash` will read next, what the first led to being in cache.
    fn prefetch_second(&self, _position: u32) {}
}

/// Positions in a list of addresses of records.
pub struct Listed<'a, A, H> {
    pub records: &'a Records,
    pub list: &'a Paged<A>,
    /// The hash of the record at the front of these bytes.
    pub hash_of: H,
}

impl<A: Address, H: Fn(&[u8]) -> u64> Positions for Listed<'_, A, H> {
    #[inline]
    fn hash(&self, position: u32) -> u64 {
        (self.hash_of)(self.records.at(self.list.get(position as usize).get()))
    }

    #[inline]
    fn prefetch_first(&self, position: u32) {
        self.list.prefetch(position as usize);
    }

    #[inline]
    fn prefetch_second(&self, position: u32) {
        self.records
            .prefetch(self.list.get(position as usize).get());
    }
}

/// A hash table of positions, in tables of up to [`SPLIT_AT`] that each split in two
/// when full, which is extendible hashing: `tables` holds them, and `directory` says
/// which one a hash's low pick bits go to, several places sharing one that hasn't
/// split as often as others.
///
/// Fields hash evenly, so tables fill alike and split around the same size of the
/// whole, within a few percent: memory rises as one table's doubling would, and
/// that work comes a table at a time. Emptying, a table shrinks on its own; tables
/// don't merge again.
#[derive(Clone)]
pub struct Index {
    directory: Vec<u32>,
    tables: Vec<HashTable<u32>>,
    /// All the tables' buckets, kept as they change so the size is O(1) to tell.
    buckets: usize,
}

impl Index {
    /// Room for `n` without splitting, the tables' sizes left a little apart.
    pub fn with_capacity(n: usize) -> Self {
        if n <= SPLIT_AT {
            let table = HashTable::with_capacity(n);
            return Index {
                directory: vec![0],
                buckets: table.num_buckets(),
                tables: vec![table],
            };
        }
        let count = (n / (SPLIT_AT * 3 / 4) + 1).next_power_of_two();
        let tables: Vec<_> = (0..count)
            .map(|_| HashTable::with_capacity(SPLIT_AT))
            .collect();
        Index {
            directory: (0..count as u32).collect(),
            buckets: tables.iter().map(HashTable::num_buckets).sum(),
            tables,
        }
    }

    #[inline]
    fn table_of(&self, hash: u64) -> usize {
        let pick = (hash >> PICK_SHIFT) as usize & (self.directory.len() - 1);
        self.directory[pick] as usize
    }

    #[inline]
    pub fn find(&self, hash: u64, mut eq: impl FnMut(u32) -> bool) -> Option<u32> {
        self.tables[self.table_of(hash)]
            .find(hash, |&p| eq(p))
            .copied()
    }

    /// Adds a position not there yet.
    #[inline]
    pub fn insert(&mut self, hash: u64, position: u32, positions: &impl Positions) {
        let mut t = self.table_of(hash);
        if self.tables[t].len() == self.tables[t].capacity() {
            t = self.make_room(t, hash, positions);
        }
        self.tables[t].insert_unique(hash, position, |&p| positions.hash(p));
    }

    /// Table `t`, which `hash` goes to, is full: returns the table it goes to now.
    #[cold]
    #[inline(never)]
    fn make_room(&mut self, t: usize, hash: u64, positions: &impl Positions) -> usize {
        let table = &self.tables[t];
        // At its largest it splits, unless it's what removals left behind that fills it:
        // then this writes it again where it is.
        if table.num_buckets() >= BUCKETS && table.len() * 2 >= SPLIT_AT {
            let depth = self.depth_of(t);
            if depth < MAX_DEPTH {
                self.split(t, depth, hash, positions);
                return self.table_of(hash);
            }
        }
        let table = &mut self.tables[t];
        let buckets = table.num_buckets();
        table.reserve(1, |&p| positions.hash(p));
        self.buckets += table.num_buckets() - buckets;
        t
    }

    /// How many hash bits pick table `t`, which goes by the places that say it.
    fn depth_of(&self, t: usize) -> u32 {
        let places = self.directory.iter().filter(|&&x| x as usize == t).count();
        (self.directory.len() / places).trailing_zeros()
    }

    /// Splits table `t`, of this depth, which `hash` goes to, by the next bit of the hash.
    /// Hashing means reading what's at each position, nearly all of it not in cache,
    /// in no order a CPU could guess, so this tells it what's coming.
    fn split(&mut self, t: usize, depth: u32, hash: u64, positions: &impl Positions) {
        if 1usize << depth == self.directory.len() {
            self.directory.extend_from_within(..);
        }
        let bit = 1u64 << (PICK_SHIFT + depth);
        let all: Vec<u32> = self.tables[t].iter().copied().collect();
        let mut stay = HashTable::with_capacity(SPLIT_AT);
        let mut leave = HashTable::with_capacity(SPLIT_AT);
        for (i, &position) in all.iter().enumerate() {
            if let Some(&ahead) = all.get(i + 2 * AHEAD) {
                positions.prefetch_first(ahead);
            }
            if let Some(&ahead) = all.get(i + AHEAD) {
                positions.prefetch_second(ahead);
            }
            let h = positions.hash(position);
            let to: &mut HashTable<u32> = if h & bit == 0 { &mut stay } else { &mut leave };
            to.insert_unique(h, position, |&p| positions.hash(p));
        }
        self.buckets =
            self.buckets + stay.num_buckets() + leave.num_buckets() - self.tables[t].num_buckets();
        self.tables[t] = stay;
        let new = self.tables.len() as u32;
        self.tables.push(leave);
        let low = (hash >> PICK_SHIFT) as usize & ((1usize << depth) - 1);
        let mut place = low | 1usize << depth;
        while place < self.directory.len() {
            self.directory[place] = new;
            place += 1usize << (depth + 1);
        }
    }

    /// Takes `position` out, which is there and has this hash. None of those left may
    /// have moved yet.
    #[inline]
    pub fn remove(&mut self, hash: u64, position: u32, positions: &impl Positions) {
        self.take(hash, |p| p == position, positions)
            .expect("indexed");
    }

    /// Takes out the position `eq` says, if any, and returns it. None of those left may
    /// have moved yet.
    #[inline]
    pub fn take(
        &mut self,
        hash: u64,
        mut eq: impl FnMut(u32) -> bool,
        positions: &impl Positions,
    ) -> Option<u32> {
        let t = self.table_of(hash);
        let table = &mut self.tables[t];
        let (position, _) = table.find_entry(hash, |&p| eq(p)).ok()?.remove();
        let buckets = table.num_buckets();
        if buckets > 16 && table.len() * 16 < buckets * 3 {
            table.shrink_to(table.len() * 2, |&p| positions.hash(p));
            self.buckets -= buckets - table.num_buckets();
        }
        Some(position)
    }

    /// Changes `old`, which is there and has this hash, to `new`.
    #[inline]
    pub fn replace(&mut self, hash: u64, old: u32, new: u32) {
        let t = self.table_of(hash);
        *self.tables[t]
            .find_mut(hash, |&p| p == old)
            .expect("indexed") = new;
    }

    pub fn positions_mut(&mut self) -> impl Iterator<Item = &mut u32> {
        self.tables.iter_mut().flat_map(HashTable::iter_mut)
    }

    /// Memory it takes: a position and a control byte per bucket.
    pub fn allocation_size(&self) -> usize {
        self.buckets * 5
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.tables.iter().map(HashTable::len).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl<F: Fn(u32) -> u64> Positions for F {
        fn hash(&self, position: u32) -> u64 {
            self(position)
        }
    }

    fn hash_of(position: u32) -> u64 {
        // As a good hash's bits would be: splitmix64.
        let mut z = u64::from(position).wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// The point of it all: however many there are, adding one reads at most a
    /// table's worth again, which is what a stop takes.
    #[test]
    fn adding_never_reads_more_than_a_table() {
        use std::cell::Cell;
        let reads = Cell::new(0usize);
        let mut index = Index::with_capacity(0);
        let mut most = 0;
        let n = 300_000u32;
        for position in 0..n {
            reads.set(0);
            index.insert(hash_of(position), position, &|p| {
                reads.set(reads.get() + 1);
                hash_of(p)
            });
            most = most.max(reads.get());
        }
        assert!(most <= SPLIT_AT, "{most} read for one insert");
        assert!(index.tables.len() > 8, "it split");
        assert_eq!(index.len(), n as usize);
        for position in 0..n {
            assert_eq!(
                index.find(hash_of(position), |p| p == position),
                Some(position)
            );
        }
        // What's there is what's counted.
        let buckets: usize = index.tables.iter().map(HashTable::num_buckets).sum();
        assert_eq!(index.buckets, buckets);
        // Each table's where the directory says, every place that says it.
        for (place, &t) in index.directory.iter().enumerate() {
            let depth = index.depth_of(t as usize);
            assert_eq!(index.directory[place & ((1 << depth) - 1)], t);
        }
    }

    /// Tables fill alike, so no size stops much more often than the rest.
    #[test]
    fn tables_fill_alike() {
        let mut index = Index::with_capacity(0);
        let n = 500_000u32;
        for position in 0..n {
            index.insert(hash_of(position), position, &hash_of);
        }
        let lens: Vec<_> = index.tables.iter().map(HashTable::len).collect();
        let (least, most) = (lens.iter().min().unwrap(), lens.iter().max().unwrap());
        assert!(most - least < most / 4, "from {least} to {most}");
    }

    #[test]
    fn built_to_size_it_needs_no_split() {
        for n in [0, 1, SPLIT_AT, SPLIT_AT + 1, 100_000] {
            let mut index = Index::with_capacity(n);
            let tables = index.tables.len();
            let buckets = index.buckets;
            for position in 0..n as u32 {
                index.insert(hash_of(position), position, &|_| panic!("no rehash"));
            }
            assert_eq!(index.tables.len(), tables);
            assert_eq!(index.buckets, buckets);
        }
    }

    #[test]
    fn removing_shrinks_the_tables_and_finds_the_rest() {
        let mut index = Index::with_capacity(0);
        let n = 100_000u32;
        for position in 0..n {
            index.insert(hash_of(position), position, &hash_of);
        }
        let full = index.allocation_size();
        for position in 0..n - 1000 {
            index.remove(hash_of(position), position, &hash_of);
        }
        assert!(index.allocation_size() < full / 4);
        let buckets: usize = index.tables.iter().map(HashTable::num_buckets).sum();
        assert_eq!(index.buckets, buckets);
        for position in n - 1000..n {
            assert_eq!(
                index.find(hash_of(position), |p| p == position),
                Some(position)
            );
        }
    }

    #[test]
    fn a_paged_list_matches_a_vec() {
        let mut list = Paged::with_capacity(10);
        let mut model = Vec::new();
        let mut rng = 0x1234_5678_9ABC_DEF1u64;
        let mut below = move |n: u64| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng % n
        };
        for size in [50, 40_000, 1_000, 70_000, 0] {
            while model.len() != size {
                if model.len() < size {
                    let value = below(1 << 40);
                    list.push(value);
                    model.push(value);
                } else {
                    let i = below(model.len() as u64) as usize;
                    list.swap_remove(i);
                    model.swap_remove(i);
                }
                assert_eq!(list.len(), model.len());
            }
            assert!(list.iter().copied().eq(model.iter().copied()));
            for (i, value) in model.iter().enumerate() {
                assert_eq!(list.get(i), *value);
            }
            assert_eq!(list.pages.len(), size.div_ceil(PAGE));
        }
        // And it gives back what it no longer needs.
        for i in 0..PAGE as u64 {
            list.push(i);
        }
        let full = list.allocation_size();
        while list.len() > 100 {
            list.swap_remove(0);
        }
        assert!(list.allocation_size() < full / 4);
        list.remove_in_order(0);
        assert_eq!(list.get(0), list.get(0));
        assert_eq!(list.len(), 99);
    }

    #[derive(Clone, Copy)]
    struct At(usize);

    impl Address for At {
        fn new(at: usize) -> Self {
            At(at)
        }
        fn get(self) -> usize {
            self.0
        }
    }

    /// Records here are a byte for the length (in 100s) and then that many.
    fn put(records: &mut Records, list: &mut Paged<At>, fill: u8, hundreds: u8) {
        let size = 1 + usize::from(hundreds) * 100;
        let (at, bytes) = records.append(size);
        bytes.push(hundreds);
        bytes.resize(bytes.len() + size - 1, fill);
        list.push(At(at));
    }

    fn size_of_record(bytes: &[u8]) -> usize {
        1 + usize::from(bytes[0]) * 100
    }

    fn check(records: &Records, list: &Paged<At>, fills: &[u8]) {
        let mut used = 0;
        for (i, fill) in fills.iter().enumerate() {
            let at = records.at(list.get(i).0);
            let size = size_of_record(at);
            assert!(at[1..size].iter().all(|b| b == fill));
            used += size;
        }
        records.check(used);
    }

    /// Tidying moves what's left out of sparse chunks, a step at a time, reads the
    /// same all along, and gives back what it empties.
    #[test]
    fn records_tidy_up_a_step_at_a_time() {
        let mut records = Records::default();
        let mut list = Paged::default();
        let mut fills = Vec::new();
        let n = 100_000;
        for i in 0..n {
            let fill = (i % 251) as u8;
            put(&mut records, &mut list, fill, 8);
            fills.push(fill);
        }
        assert!(records.chunks.len() > 5);
        let full = records.allocation_size();
        let mut i = 0;
        while list.len() > n / 10 {
            // Spread over everything, so that no chunk empties on its own.
            i = (i + 7919) % list.len();
            let gone = records.at(list.get(i).0);
            let size = size_of_record(gone);
            records.hole(list.get(i).0, size);
            list.swap_remove(i);
            fills.swap_remove(i);
            let before = (records.written, records.tidied_to);
            records.tidy(&mut list, size_of_record);
            // No more work per write than the step takes.
            assert!(
                records.written <= before.0 + TIDY_STEP * size
                    && (records.tidied_to == 0
                        || records.tidied_to as usize <= before.1 as usize + TIDY_STEP),
            );
            if list.len() % 20_000 == 0 {
                check(&records, &list, &fills);
            }
        }
        check(&records, &list, &fills);
        // At most about twice what's in use, and a chunk either way.
        assert!(
            records.written < 2 * records.used() + 2 * CHUNK,
            "{} written, {} used",
            records.written,
            records.used()
        );
        assert!(records.allocation_size() < full / 3);
    }

    #[test]
    fn records_over_a_chunk_get_their_own() {
        let mut records = Records::default();
        let mut list = Paged::default();
        let small = 1 + 100;
        put(&mut records, &mut list, 1, 1);
        // Over a chunk, in a test that needn't take much: as if by length.
        let huge = CHUNK + 5;
        let (at, bytes) = records.append(huge);
        assert_eq!(at & (CHUNK - 1), 0);
        bytes.resize(huge, 9);
        put(&mut records, &mut list, 2, 1);
        assert_eq!(records.at(at).len(), huge);
        assert_eq!(records.at(list.get(0).0)[1], 1);
        assert_eq!(records.at(list.get(1).0)[1], 2);
        assert_eq!(records.written(), huge + 2 * small);
        // Its chunk goes as soon as it's a hole, and the number is used again.
        records.hole(at, huge);
        assert_eq!(records.holes(), 0);
        assert_eq!(records.written(), 2 * small);
        let (again, bytes) = records.append(huge);
        bytes.resize(huge, 8);
        assert_eq!(again, at);
    }

    /// A chunk's end is never crossed, and a copy reads the same and can go on.
    #[test]
    fn records_stay_whole_and_copies_go_on() {
        let mut records = Records::default();
        let mut list = Paged::default();
        let mut fills = Vec::new();
        for i in 0..30_000u32 {
            let fill = (i % 200) as u8;
            put(&mut records, &mut list, fill, (i % 7) as u8);
            fills.push(fill);
        }
        check(&records, &list, &fills);
        let mut copy = records.clone();
        let mut copied_list = list.clone();
        put(&mut copy, &mut copied_list, 3, 2);
        fills.push(3);
        check(&copy, &copied_list, &fills);
    }
}
