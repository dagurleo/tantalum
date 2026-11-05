//! An ordered map from stream IDs, for a consumer group's pending entries and
//! each consumer's: sorted leaves of up to [`LEAF_ENTRIES`] under a tree of where
//! they start. Entries that come in order, as deliveries do, fill each leaf, where
//! a B-tree's nodes stay half empty. Entries that don't come in order split a leaf
//! in two, and leaves left nearly empty join a neighbour, so a map stays dense
//! whichever way it's used. Going in at the end and out at the start, as they
//! mostly do, entries go straight to their leaf and move nothing in it.

use std::collections::{BTreeMap, btree_map};
use std::fmt;
use std::ops::Bound;
use std::slice;

use crate::stream::StreamId;

/// How many entries a leaf holds. A leaf's IDs then land on a size the allocator
/// has blocks for (1,024 bytes), with nothing to spare.
const LEAF_ENTRIES: usize = 64;

/// About what a leaf takes in the tree over it: its 72 bytes in a node, nodes a
/// little over half full, and the nodes above those.
const LEAF_OVERHEAD: usize = 160;

/// Its entries are what's from `start` on, sorted: those before it have been
/// removed, so that removing from the start moves nothing.
#[derive(Clone)]
struct Leaf<V> {
    ids: Vec<StreamId>,
    values: Vec<V>,
    start: usize,
}

impl<V: Copy> Leaf<V> {
    fn with_capacity(capacity: usize) -> Self {
        Leaf {
            ids: Vec::with_capacity(capacity),
            values: Vec::with_capacity(capacity),
            start: 0,
        }
    }

    fn bytes(&self) -> usize {
        LEAF_OVERHEAD
            + self.ids.capacity() * size_of::<StreamId>()
            // (A zero-sized value's vector says its capacity is the largest there is.)
            + self.values.capacity() * size_of::<V>()
    }

    fn len(&self) -> usize {
        self.ids.len() - self.start
    }

    fn ids(&self) -> &[StreamId] {
        &self.ids[self.start..]
    }

    fn values(&self) -> &[V] {
        &self.values[self.start..]
    }

    /// Where `id` is, or would go. New IDs are mostly the largest yet.
    fn find(&self, id: StreamId) -> Result<usize, usize> {
        match self.ids.last() {
            Some(&last) if id > last => Err(self.len()),
            _ => self.ids().binary_search(&id),
        }
    }

    /// Moves its entries to the start of its vectors.
    fn compact(&mut self) {
        if self.start > 0 {
            self.ids.drain(..self.start);
            self.values.drain(..self.start);
            self.start = 0;
        }
    }

    fn insert(&mut self, at: usize, id: StreamId, value: V) {
        if self.start > 0 {
            if at == 0 {
                self.start -= 1;
                self.ids[self.start] = id;
                self.values[self.start] = value;
                return;
            }
            if self.ids.len() == self.ids.capacity() {
                self.compact();
            }
        }
        self.ids.insert(self.start + at, id);
        self.values.insert(self.start + at, value);
    }

    /// Moves whichever entries are fewer: those before it or those after.
    fn remove(&mut self, at: usize) -> V {
        let from = self.start;
        let value = self.values[from + at];
        if at * 2 < self.len() {
            self.ids.copy_within(from..from + at, from + 1);
            self.values.copy_within(from..from + at, from + 1);
            self.start += 1;
        } else {
            self.ids.remove(from + at);
            self.values.remove(from + at);
        }
        if self.start == self.ids.len() {
            self.ids.clear();
            self.values.clear();
            self.start = 0;
        }
        value
    }

    /// Gives back what entries removed left spare. Only by a lot: an allocator
    /// may well keep a block that's asked to shrink by half or less.
    fn fit(&mut self) {
        let capacity = self.ids.capacity();
        if capacity > 8 && self.len() * 4 <= capacity {
            self.compact();
            self.ids.shrink_to(capacity / 4);
            self.values.shrink_to(capacity / 4);
        }
    }
}

/// See the module's description.
pub struct IdMap<V> {
    /// Each leaf under an ID no larger than its first, and larger than all
    /// the previous leaf's. Leaves aren't empty, but for an only leaf, which
    /// stays for a map that goes from empty to one entry and back all the time.
    leaves: BTreeMap<StreamId, Leaf<V>>,
    len: usize,
    bytes: usize,
}

impl<V> Default for IdMap<V> {
    fn default() -> Self {
        IdMap {
            leaves: BTreeMap::new(),
            len: 0,
            bytes: 0,
        }
    }
}

impl<V: Copy> Clone for IdMap<V> {
    fn clone(&self) -> Self {
        // Copies have no spare room, so they take less.
        let leaves: BTreeMap<_, _> = self.leaves.clone();
        let bytes = leaves.values().map(Leaf::bytes).sum();
        IdMap {
            leaves,
            len: self.len,
            bytes,
        }
    }
}

impl<V: Copy + fmt::Debug> fmt::Debug for IdMap<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<V: Copy> IdMap<V> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// What it has allocated.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn get(&self, id: StreamId) -> Option<&V> {
        let (_, leaf) = self.leaf_of(id)?;
        let at = leaf.ids().binary_search(&id).ok()?;
        Some(&leaf.values()[at])
    }

    pub fn get_mut(&mut self, id: StreamId) -> Option<&mut V> {
        // Entries still pending are mostly the oldest, in the first leaf.
        let (_, first) = self.leaves.first_key_value()?;
        let leaf = if first.ids.last().is_some_and(|&last| id <= last) {
            self.leaves.first_entry()?.into_mut()
        } else {
            self.leaves.range_mut(..=id).next_back()?.1
        };
        let at = leaf.ids().binary_search(&id).ok()?;
        Some(&mut leaf.values[leaf.start + at])
    }

    pub fn first(&self) -> Option<StreamId> {
        let (_, leaf) = self.leaves.first_key_value()?;
        leaf.ids().first().copied()
    }

    pub fn last(&self) -> Option<StreamId> {
        let (_, leaf) = self.leaves.last_key_value()?;
        leaf.ids().last().copied()
    }

    /// The value it had, if any.
    pub fn insert(&mut self, id: StreamId, value: V) -> Option<V> {
        // New IDs mostly go in the last leaf, which takes no comparing to find.
        let leaf = match self.leaves.last_entry() {
            Some(last) if *last.key() <= id => last.into_mut(),
            _ => match self.leaves.range_mut(..=id).next_back() {
                Some((_, leaf)) => leaf,
                None => return self.insert_below_all(id, value),
            },
        };
        let at = match leaf.find(id) {
            Ok(at) => {
                let slot = leaf.start + at;
                return Some(std::mem::replace(&mut leaf.values[slot], value));
            }
            Err(at) => at,
        };
        self.len += 1;
        let before = leaf.bytes();
        if leaf.len() < LEAF_ENTRIES {
            leaf.insert(at, id, value);
            self.bytes = self.bytes + leaf.bytes() - before;
        } else if at == LEAF_ENTRIES {
            // After all of a full leaf: a new leaf, for those to come after it.
            let mut new = Leaf::with_capacity(LEAF_ENTRIES);
            new.insert(0, id, value);
            self.bytes += new.bytes();
            self.leaves.insert(id, new);
        } else {
            let half = leaf.start + LEAF_ENTRIES / 2;
            let mut upper = Leaf {
                ids: leaf.ids.split_off(half),
                values: leaf.values.split_off(half),
                start: 0,
            };
            if at < LEAF_ENTRIES / 2 {
                leaf.insert(at, id, value);
            } else {
                upper.insert(at - LEAF_ENTRIES / 2, id, value);
            }
            self.bytes = self.bytes + leaf.bytes() + upper.bytes() - before;
            self.leaves.insert(upper.ids[0], upper);
        }
        None
    }

    /// An ID below where every leaf starts: the first leaf starts there now.
    #[cold]
    fn insert_below_all(&mut self, id: StreamId, value: V) -> Option<V> {
        let leaf = match self.leaves.pop_first() {
            Some((_, leaf)) => leaf,
            None => {
                let leaf = Leaf::with_capacity(0);
                self.bytes += leaf.bytes();
                leaf
            }
        };
        self.leaves.insert(id, leaf);
        self.insert(id, value)
    }

    /// The value it had, if any.
    pub fn remove(&mut self, id: StreamId) -> Option<V> {
        let only_leaf = self.leaves.len() == 1;
        // Entries mostly go from the first leaf, which takes no searching to find.
        let (key, leaf) = match self.leaves.first_entry() {
            Some(first) if first.get().ids.last().is_some_and(|&last| id <= last) => {
                (*first.key(), first.into_mut())
            }
            _ => {
                let (&key, leaf) = self.leaves.range_mut(..=id).next_back()?;
                (key, leaf)
            }
        };
        // Mostly the first it has, or already gone: neither takes a search.
        let at = match leaf.ids().first() {
            Some(&first) if id < first => return None,
            Some(&first) if id == first => 0,
            _ => leaf.ids().binary_search(&id).ok()?,
        };
        let before = leaf.bytes();
        let value = leaf.remove(at);
        self.len -= 1;
        let left = leaf.len();
        if left == 0 && !only_leaf {
            self.bytes -= before;
            self.leaves.remove(&key);
            return Some(value);
        }
        leaf.fit();
        self.bytes = self.bytes + leaf.bytes() - before;
        // Not at every entry from here on, or removing entries would look next
        // door every time for a neighbour that's too full.
        if matches!(left, 1 | 16) {
            self.join_neighbour(key, left);
        }
        Some(value)
    }

    /// Joins the leaf at `key`, of `len` entries, to the leaf before or after
    /// it, if it's small enough for the two to leave room in the one.
    fn join_neighbour(&mut self, key: StreamId, len: usize) {
        let room = LEAF_ENTRIES / 2;
        let previous = self.leaves.range(..key).next_back();
        if let Some((&previous, leaf)) = previous
            && leaf.len() + len <= room
        {
            self.join(previous, key);
            return;
        }
        let next = self
            .leaves
            .range((Bound::Excluded(key), Bound::Unbounded))
            .next();
        if let Some((&next, leaf)) = next
            && leaf.len() + len <= room
        {
            self.join(key, next);
        }
    }

    /// Moves the leaf at `upper` to the end of the one at `lower`.
    fn join(&mut self, lower: StreamId, upper: StreamId) {
        let Some(upper) = self.leaves.remove(&upper) else {
            return;
        };
        self.bytes -= upper.bytes();
        if let Some(lower) = self.leaves.get_mut(&lower) {
            let before = lower.bytes();
            lower.compact();
            lower.ids.extend_from_slice(upper.ids());
            lower.values.extend_from_slice(upper.values());
            self.bytes = self.bytes + lower.bytes() - before;
        }
    }

    /// The leaf that has or would have `id`, or the first if it's below them all.
    /// Entries still pending are mostly the oldest, in the first leaf.
    fn leaf_of(&self, id: StreamId) -> Option<(&StreamId, &Leaf<V>)> {
        let first = self.leaves.first_key_value()?;
        if first.1.ids.last().is_some_and(|&last| id <= last) {
            return Some(first);
        }
        self.leaves.range(..=id).next_back().or(Some(first))
    }

    /// The first entry from `start` on: [`IdMap::range`], for just that.
    pub fn first_from(&self, start: StreamId) -> Option<(StreamId, &V)> {
        let (&key, leaf) = self.leaf_of(start)?;
        let at = leaf.ids().partition_point(|&id| id < start);
        if at < leaf.len() {
            return Some((leaf.ids()[at], &leaf.values()[at]));
        }
        let (_, next) = self
            .leaves
            .range((Bound::Excluded(key), Bound::Unbounded))
            .next()?;
        Some((*next.ids().first()?, next.values().first()?))
    }

    /// The entries from `start` to `end`, both included, in order.
    pub fn range(&self, start: StreamId, end: StreamId) -> Range<'_, V> {
        let (key, ids, values) = match self.leaf_of(start) {
            Some((&key, leaf)) => {
                let skip = leaf.ids().partition_point(|&id| id < start);
                (key, &leaf.ids()[skip..], &leaf.values()[skip..])
            }
            None => (StreamId::MIN, &[][..], &[][..]),
        };
        Range {
            map: self,
            key,
            rest: None,
            ids: ids.iter(),
            values: values.iter(),
            end,
        }
    }

    pub fn iter(&self) -> Range<'_, V> {
        self.range(StreamId::MIN, StreamId::MAX)
    }

    #[cfg(test)]
    pub fn check(&self) {
        let mut previous = None;
        let mut len = 0;
        let mut bytes = 0;
        for (&key, leaf) in &self.leaves {
            assert_eq!(leaf.ids.len(), leaf.values.len());
            assert!(leaf.start <= leaf.ids.len());
            assert!(leaf.len() <= LEAF_ENTRIES);
            assert!(leaf.len() > 0 || self.leaves.len() == 1, "an empty leaf");
            if let Some(&first) = leaf.ids().first() {
                assert!(key <= first, "a leaf under {key:?} starts at {first:?}");
            }
            for &id in leaf.ids() {
                assert!(previous.is_none_or(|p| id > p), "IDs in order");
                previous = Some(id);
            }
            len += leaf.len();
            bytes += leaf.bytes();
        }
        assert_eq!(len, self.len, "its length");
        assert_eq!(bytes, self.bytes, "its bytes");
    }
}

/// See [`IdMap::range`].
pub struct Range<'a, V> {
    map: &'a IdMap<V>,
    /// Where the first leaf is: what comes after it is looked up only if the
    /// entries wanted go on that far, as they mostly don't.
    key: StreamId,
    rest: Option<btree_map::Range<'a, StreamId, Leaf<V>>>,
    ids: slice::Iter<'a, StreamId>,
    values: slice::Iter<'a, V>,
    end: StreamId,
}

impl<'a, V: Copy> Iterator for Range<'a, V> {
    type Item = (StreamId, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let (Some(&id), Some(value)) = (self.ids.next(), self.values.next()) {
                return (id <= self.end).then_some((id, value));
            }
            let (map, key) = (self.map, self.key);
            let rest = self
                .rest
                .get_or_insert_with(|| map.leaves.range((Bound::Excluded(key), Bound::Unbounded)));
            let (_, leaf) = rest.next()?;
            self.ids = leaf.ids().iter();
            self.values = leaf.values().iter();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn id(ms: u64) -> StreamId {
        StreamId { ms, seq: 0 }
    }

    /// In order, as deliveries come: every leaf full but the last.
    #[test]
    fn entries_in_order_fill_their_leaves() {
        let mut map = IdMap::default();
        for ms in 1..=64_000 {
            assert_eq!(map.insert(id(ms), ms), None);
        }
        map.check();
        assert_eq!(map.leaves.len(), 1_000);
        // What each leaf could hold is what it has.
        let per_entry = map.bytes() / map.len();
        assert!(per_entry <= 16 + 8 + 3, "{per_entry} bytes an entry");
    }

    /// Whatever goes on, as a B-tree does.
    #[test]
    fn matches_a_btree() {
        let mut rng = 0x2545_F491_4F6C_DD1Du64;
        let mut below = |n: u64| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng % n
        };
        for span in [40, 500, 5_000, 200_000] {
            let mut map = IdMap::default();
            let mut model = BTreeMap::new();
            // What's being added and removed moves along, as deliveries and
            // acknowledgements do; some runs go down instead, some at random.
            let mut at = span / 2;
            for step in 0..60_000u64 {
                let wanted = match step / 10_000 % 3 {
                    0 => {
                        at = (at + 1) % span;
                        at
                    }
                    1 => span - 1 - at.min(span - 1),
                    _ => below(span),
                };
                match below(10) {
                    0..=4 => {
                        assert_eq!(map.insert(id(wanted), step), model.insert(id(wanted), step));
                    }
                    5..=7 => {
                        let gone = below(span);
                        assert_eq!(map.remove(id(gone)), model.remove(&id(gone)));
                    }
                    8 => {
                        let found = below(span);
                        assert_eq!(map.get(id(found)), model.get(&id(found)));
                        if let Some(value) = map.get_mut(id(found)) {
                            *value += 1;
                            *model.get_mut(&id(found)).unwrap() += 1;
                        }
                    }
                    _ => {
                        let (a, b) = (below(span), below(span));
                        let (a, b) = (a.min(b), a.max(b));
                        let got: Vec<_> = map.range(id(a), id(b)).map(|(k, v)| (k, *v)).collect();
                        let want: Vec<_> =
                            model.range(id(a)..=id(b)).map(|(k, v)| (*k, *v)).collect();
                        assert_eq!(got, want);
                        let from = map.first_from(id(a)).map(|(k, v)| (k, *v));
                        let want = model.range(id(a)..).next().map(|(k, v)| (*k, *v));
                        assert_eq!(from, want);
                    }
                }
                assert_eq!(map.len(), model.len());
                assert_eq!(map.first(), model.keys().next().copied());
                assert_eq!(map.last(), model.keys().next_back().copied());
                if step % 500 == 0 {
                    map.check();
                }
            }
            map.check();
            assert!(
                map.iter()
                    .map(|(k, v)| (k, *v))
                    .eq(model.iter().map(|(k, v)| (*k, *v)))
            );
        }
    }

    /// Removing nearly everything, in any order, leaves little behind.
    #[test]
    fn leaves_join_up_as_entries_go() {
        for step in [1, 2, 7] {
            let mut map = IdMap::default();
            for ms in 0..64_000u64 {
                map.insert(id(ms), ms);
            }
            // All but one in a hundred, going along it, then back along it.
            for pass in [0, 1] {
                let mut ids: Vec<u64> = (0..64_000).filter(|ms| ms % 100 != 0).collect();
                if pass == 1 {
                    ids.reverse();
                }
                for (i, &ms) in ids.iter().enumerate() {
                    if (i as u64).is_multiple_of(step) || pass == 1 {
                        map.remove(id(ms));
                    }
                }
            }
            map.check();
            assert_eq!(map.len(), 640);
            assert!(map.leaves.len() <= 64, "{} leaves", map.leaves.len());
            let per_entry = map.bytes() / map.len();
            assert!(per_entry <= 120, "{per_entry} bytes an entry");
        }
    }

    /// A map that keeps going from nothing to one entry keeps its leaf.
    #[test]
    fn an_only_leaf_stays() {
        let mut map = IdMap::default();
        assert_eq!(map.first(), None);
        assert_eq!(map.last(), None);
        for ms in 1..100 {
            map.insert(id(ms), ());
            assert_eq!(map.first(), Some(id(ms)));
            map.remove(id(ms));
            assert!(map.is_empty());
            assert_eq!(map.first(), None);
            assert_eq!(map.last(), None);
            assert_eq!(map.iter().count(), 0);
            assert_eq!(map.leaves.len(), 1);
            map.check();
        }
        // Below where it started.
        map.insert(id(1), ());
        map.check();
        assert_eq!(map.first(), Some(id(1)));
    }

    #[test]
    fn copies_keep_their_books() {
        let mut map = IdMap::default();
        for ms in 0..1_000 {
            map.insert(id(ms * 2), ms);
        }
        for ms in 0..300 {
            map.remove(id(ms * 6));
        }
        let copy = map.clone();
        copy.check();
        assert!(copy.bytes() <= map.bytes());
        assert!(copy.iter().eq(map.iter()));
    }
}
