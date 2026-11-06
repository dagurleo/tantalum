//! Hashes: a key's fields and values, kept in its entry's value as
//!
//! ```text
//! field count, then per field:  field length, field, value length, value
//! ```
//!
//! with counts and lengths as LEB128 varints (one byte below 128), and the
//! fields in the order they were added, the order Redis' small hashes
//! (listpacks) reply in. Reads scan the fields and every change writes the
//! whole value again: cheap for the small hashes caches hold, but O(n) per
//! command for big ones. So past [`PACKED_FIELDS`] or [`PACKED_BYTES`] a hash
//! is a [`BigHash`], which its shard keeps as an object and changes in place, and
//! written to a snapshot in the format above, so the format needn't know.

use std::collections::HashMap;
use std::hash::BuildHasher;

use crate::big::{Address, Index, Listed, Paged, PagedIter, Records};

/// Past this many fields a hash isn't packed: Redis' `hash-max-listpack-entries`,
/// up to which it replies in the order added, so a [`BigHash`] does too.
pub const PACKED_FIELDS: usize = 512;
/// Nor past this many bytes, which each write would copy. Redis has no such limit,
/// since its limit on a value's length makes it unnecessary; here a hash with a few
/// long values stays packed, as it costs less, until they add up.
pub const PACKED_BYTES: usize = 8 * 1024;
/// A longer field or value is why Redis would have a hash table
/// (`hash-max-listpack-value`), which `OBJECT ENCODING` and `HSCAN` tell.
const LISTPACK_VALUE: usize = 64;

/// Whether a packed hash, valid, may stay packed.
pub fn fits_packed(bytes: &[u8]) -> bool {
    bytes.len() <= PACKED_BYTES && len(bytes) <= PACKED_FIELDS
}

fn put_varint(out: &mut Vec<u8>, mut n: usize) {
    while n >= 0x80 {
        out.push(n as u8 | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

/// Writes a varint over all of `out`, which is as long as it takes.
fn put_varint_in(out: &mut [u8], mut n: usize) {
    for byte in out {
        *byte = if n >= 0x80 { n as u8 | 0x80 } else { n as u8 };
        n >>= 7;
    }
}

fn varint_len(mut n: usize) -> usize {
    let mut len = 1;
    while n >= 0x80 {
        n >>= 7;
        len += 1;
    }
    len
}

/// Reads a varint from the front of `bytes`, advancing past it.
#[inline]
fn take_varint(bytes: &mut &[u8]) -> Option<usize> {
    // Nearly every length is under 128: one byte.
    let (&first, rest) = bytes.split_first()?;
    if first < 0x80 {
        *bytes = rest;
        return Some(usize::from(first));
    }
    take_long_varint(bytes)
}

#[cold]
fn take_long_varint(bytes: &mut &[u8]) -> Option<usize> {
    let mut n = 0usize;
    for shift in (0..usize::BITS).step_by(7) {
        let (&byte, rest) = bytes.split_first()?;
        *bytes = rest;
        n |= usize::from(byte & 0x7f).checked_shl(shift)?;
        if byte < 0x80 {
            return Some(n);
        }
    }
    None
}

#[inline]
fn take_bytes<'a>(bytes: &mut &'a [u8]) -> Option<&'a [u8]> {
    let len = take_varint(bytes)?;
    if len > bytes.len() {
        return None;
    }
    let (taken, rest) = bytes.split_at(len);
    *bytes = rest;
    Some(taken)
}

/// How many fields the hash has.
pub fn len(mut bytes: &[u8]) -> usize {
    take_varint(&mut bytes).expect("a hash")
}

/// Its fields and values, in order.
pub fn iter(mut bytes: &[u8]) -> Pairs<'_> {
    let left = take_varint(&mut bytes).expect("a hash");
    Pairs { bytes, left }
}

pub struct Pairs<'a> {
    bytes: &'a [u8],
    left: usize,
}

impl<'a> Iterator for Pairs<'a> {
    type Item = (&'a [u8], &'a [u8]);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let field = take_bytes(&mut self.bytes).expect("a field");
        let value = take_bytes(&mut self.bytes).expect("a value");
        Some((field, value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left, Some(self.left))
    }
}

impl ExactSizeIterator for Pairs<'_> {}

/// The value of `field`, if the hash has it.
pub fn get<'a>(bytes: &'a [u8], field: &[u8]) -> Option<&'a [u8]> {
    iter(bytes).find(|(f, _)| same(f, field)).map(|(_, v)| v)
}

/// `a == b`, without a call to `memcmp` for the short fields hashes mostly
/// have: two overlapping loads from each cover up to 16 bytes.
#[inline]
fn same(a: &[u8], b: &[u8]) -> bool {
    let n = a.len();
    if n != b.len() {
        return false;
    }
    let u32_at = |s: &[u8], i: usize| u32::from_ne_bytes(s[i..i + 4].try_into().unwrap());
    let u64_at = |s: &[u8], i: usize| u64::from_ne_bytes(s[i..i + 8].try_into().unwrap());
    match n {
        0 => true,
        1..4 => a[0] == b[0] && a[n / 2] == b[n / 2] && a[n - 1] == b[n - 1],
        4..8 => u32_at(a, 0) == u32_at(b, 0) && u32_at(a, n - 4) == u32_at(b, n - 4),
        8..=16 => u64_at(a, 0) == u64_at(b, 0) && u64_at(a, n - 8) == u64_at(b, n - 8),
        _ => a == b,
    }
}

/// Whether `bytes` is a hash as this module writes them: at least one
/// field, no field twice, nothing left over. For values read from files.
pub fn is_valid(mut bytes: &[u8]) -> bool {
    let Some(count) = take_varint(&mut bytes) else {
        return false;
    };
    // Each field takes at least two bytes, so a bad count can't make this
    // allocate much.
    if count == 0 || count > bytes.len() / 2 {
        return false;
    }
    let mut seen = std::collections::HashSet::with_capacity(count);
    for _ in 0..count {
        let (Some(field), Some(_)) = (take_bytes(&mut bytes), take_bytes(&mut bytes)) else {
            return false;
        };
        if !seen.insert(field) {
            return false;
        }
    }
    bytes.is_empty()
}

/// Above this many field comparisons, an edit indexes the fields first.
const SCAN_LIMIT: usize = 512;

/// A hash being changed: its fields borrowed from the old value and the
/// command's arguments, written out again by [`Edit::encode`].
pub struct Edit<'a> {
    /// Deleted fields stay as `None`, so positions in `index` hold.
    pairs: Vec<Option<(&'a [u8], &'a [u8])>>,
    live: usize,
    index: Option<HashMap<&'a [u8], usize>>,
    changed: bool,
}

impl<'a> Edit<'a> {
    /// Starts from `old` (`None` for a new hash), expecting about `lookups`
    /// field lookups.
    pub fn new(old: Option<&'a [u8]>, lookups: usize) -> Self {
        let pairs: Vec<_> = old.map_or_else(Vec::new, |old| iter(old).map(Some).collect());
        // Those fields and what the lookups may add, or one `HSET` of many fields
        // makes a new hash in O(n^2).
        let fields = pairs.len().saturating_add(lookups);
        let index = (fields.saturating_mul(lookups) > SCAN_LIMIT).then(|| {
            pairs
                .iter()
                .enumerate()
                .map(|(i, pair)| (pair.expect("live").0, i))
                .collect()
        });
        Edit {
            live: pairs.len(),
            pairs,
            index,
            changed: false,
        }
    }

    fn position(&self, field: &[u8]) -> Option<usize> {
        match &self.index {
            Some(index) => index.get(field).copied(),
            None => self
                .pairs
                .iter()
                .position(|pair| pair.is_some_and(|(f, _)| same(f, field))),
        }
    }

    pub fn get(&self, field: &[u8]) -> Option<&'a [u8]> {
        self.position(field)
            .and_then(|i| self.pairs[i])
            .map(|(_, v)| v)
    }

    /// Sets `field` to `value`. Returns whether the field is new.
    #[inline]
    pub fn set(&mut self, field: &'a [u8], value: &'a [u8]) -> bool {
        self.changed = true;
        match self.position(field) {
            Some(i) => {
                self.pairs[i] = Some((field, value));
                false
            }
            None => {
                if let Some(index) = &mut self.index {
                    index.insert(field, self.pairs.len());
                }
                self.pairs.push(Some((field, value)));
                self.live += 1;
                true
            }
        }
    }

    /// Removes `field`. Returns whether it was there.
    pub fn delete(&mut self, field: &[u8]) -> bool {
        let Some(i) = self.position(field) else {
            return false;
        };
        if let Some(index) = &mut self.index {
            index.remove(field);
        }
        self.pairs[i] = None;
        self.live -= 1;
        self.changed = true;
        true
    }

    pub fn is_empty(&self) -> bool {
        self.live == 0
    }

    /// Whether anything was set or deleted (setting a field to the value it
    /// had counts, as in Redis).
    pub fn changed(&self) -> bool {
        self.changed
    }

    pub fn encode(&self) -> Vec<u8> {
        let pairs = || self.pairs.iter().flatten();
        let size = varint_len(self.live)
            + pairs()
                .map(|(f, v)| varint_len(f.len()) + f.len() + varint_len(v.len()) + v.len())
                .sum::<usize>();
        let mut out = Vec::with_capacity(size);
        put_varint(&mut out, self.live);
        for (field, value) in pairs() {
            put_varint(&mut out, field.len());
            out.extend_from_slice(field);
            put_varint(&mut out, value.len());
            out.extend_from_slice(value);
        }
        debug_assert_eq!(out.len(), size);
        out
    }
}

/// Where a field's record starts: 5 bytes, as there can be many, and 4 would
/// stop a hash at 4 GB.
#[derive(Clone, Copy)]
#[repr(C, packed)]
struct Offset {
    low: u32,
    high: u8,
}

impl Address for Offset {
    #[inline]
    fn new(at: usize) -> Self {
        Offset {
            low: at as u32,
            high: u8::try_from(at >> 32).expect("a hash under 1 TB"),
        }
    }

    #[inline]
    fn get(self) -> usize {
        usize::from(self.high) << 32 | self.low as usize
    }
}

fn put_record(out: &mut Vec<u8>, field: &[u8], value: &[u8]) {
    put_varint(out, field.len());
    out.extend_from_slice(field);
    put_varint(out, value.len());
    out.extend_from_slice(value);
}

fn record_len(field: &[u8], value: &[u8]) -> usize {
    varint_len(field.len()) + field.len() + varint_len(value.len()) + value.len()
}

#[inline]
fn field_at(records: &Records, offset: Offset) -> &[u8] {
    take_bytes(&mut records.at(offset.get())).expect("a field")
}

#[inline]
fn record_at(records: &Records, offset: Offset) -> (&[u8], &[u8]) {
    let mut at = records.at(offset.get());
    let field = take_bytes(&mut at).expect("a field");
    let value = take_bytes(&mut at).expect("a value");
    (field, value)
}

/// A record's length, from its bytes.
#[inline]
fn size_of_record(bytes: &[u8]) -> usize {
    let mut at = bytes;
    take_bytes(&mut at).expect("a field");
    take_bytes(&mut at).expect("a value");
    bytes.len() - at.len()
}

/// What an [`Index`] needs to hash the field at a position.
#[inline]
fn listed<'a>(
    records: &'a Records,
    offsets: &'a Paged<Offset>,
    hasher: &'a foldhash::fast::FixedState,
) -> Listed<'a, Offset, impl Fn(&[u8]) -> u64 + 'a> {
    Listed {
        records,
        list: offsets,
        hash_of: move |mut bytes: &[u8]| hasher.hash_one(take_bytes(&mut bytes).expect("a field")),
    }
}

/// A big hash, kept compact: its fields and values back to back in [`Records`], each
/// field next to its value so that finding one has the other in hand; their addresses
/// in a list, whose order is the hash's (so a random field, or a position to scan
/// from, is O(1)); and an [`Index`] of positions in that list. None of them stops to
/// grow (see [`crate::big`]). A value is changed where it is if it fits, else written
/// at the end.
///
/// In the list, a removed field's place goes to the last one, as `swap_remove` does,
/// unless Redis might still have the hash packed: within [`PACKED_FIELDS`] the rest
/// move up, as they do in a listpack, so that replies stay in the order added.
#[derive(Clone)]
pub struct BigHash {
    records: Records,
    offsets: Paged<Offset>,
    index: Index,
    hasher: foldhash::fast::FixedState,
}

impl BigHash {
    fn with_capacity(fields: usize, bytes: usize) -> Self {
        BigHash {
            records: Records::with_capacity(bytes),
            offsets: Paged::with_capacity(fields),
            index: Index::with_capacity(fields),
            hasher: foldhash::fast::FixedState::with_seed(crate::set::random_seed()),
        }
    }

    fn hash(&self, field: &[u8]) -> u64 {
        self.hasher.hash_one(field)
    }

    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.len() == 0
    }

    fn find(&self, hash: u64, field: &[u8]) -> Option<usize> {
        let (records, offsets) = (&self.records, &self.offsets);
        self.index
            .find(hash, |p| {
                same(field_at(records, offsets.get(p as usize)), field)
            })
            .map(|p| p as usize)
    }

    pub fn get(&self, field: &[u8]) -> Option<&[u8]> {
        // The record once: finding the field has its value in hand.
        let (records, offsets) = (&self.records, &self.offsets);
        let mut value = None;
        self.index.find(self.hash(field), |p| {
            let (candidate, its_value) = record_at(records, offsets.get(p as usize));
            let found = same(candidate, field);
            if found {
                value = Some(its_value);
            }
            found
        });
        value
    }

    /// The field and value at position `i`.
    pub fn get_index(&self, i: usize) -> (&[u8], &[u8]) {
        record_at(&self.records, self.offsets.get(i))
    }

    pub fn iter(&self) -> BigPairs<'_> {
        BigPairs {
            records: &self.records,
            offsets: self.offsets.iter(),
        }
    }

    /// Sets `field` to `value`. Returns whether the field is new.
    pub fn insert(&mut self, field: &[u8], value: &[u8]) -> bool {
        let hash = self.hash(field);
        if let Some(position) = self.find(hash, field) {
            self.replace_value(position, field, value);
            return false;
        }
        let position = u32::try_from(self.offsets.len()).expect("under 4 billion fields");
        let (at, bytes) = self.records.append(record_len(field, value));
        put_record(bytes, field, value);
        self.offsets.push(Offset::new(at));
        self.index.insert(
            hash,
            position,
            &listed(&self.records, &self.offsets, &self.hasher),
        );
        true
    }

    fn replace_value(&mut self, position: usize, field: &[u8], value: &[u8]) {
        let offset = self.offsets.get(position);
        let old_len = record_at(&self.records, offset).1.len();
        if value.len() <= old_len && varint_len(value.len()) == varint_len(old_len) {
            // It fits, and nothing reads past a record's own end.
            let length_at = varint_len(field.len()) + field.len();
            let value_at = length_at + varint_len(value.len());
            let record = self.records.at_mut(offset.get());
            put_varint_in(&mut record[length_at..value_at], value.len());
            record[value_at..value_at + value.len()].copy_from_slice(value);
            if value.len() < old_len {
                self.records.hole(offset.get(), old_len - value.len());
                self.records.tidy(&mut self.offsets, size_of_record);
            }
            return;
        }
        let old_size = varint_len(field.len()) + field.len() + varint_len(old_len) + old_len;
        let (at, bytes) = self.records.append(record_len(field, value));
        put_record(bytes, field, value);
        self.offsets.set(position, Offset::new(at));
        self.records.hole(offset.get(), old_size);
        self.records.tidy(&mut self.offsets, size_of_record);
    }

    /// Removes `field`. Returns whether it was there.
    pub fn remove(&mut self, field: &[u8]) -> bool {
        let hash = self.hash(field);
        let (records, offsets) = (&self.records, &self.offsets);
        let Some(i) = self.index.take(
            hash,
            |p| same(field_at(records, offsets.get(p as usize)), field),
            &listed(records, offsets, &self.hasher),
        ) else {
            return false;
        };
        let i = i as usize;
        let offset = self.offsets.get(i);
        let size = size_of_record(self.records.at(offset.get()));
        self.records.hole(offset.get(), size);
        let last = self.offsets.len() - 1;
        if self.offsets.len() <= PACKED_FIELDS {
            self.offsets.remove_in_order(i);
            for position in self.index.positions_mut() {
                if *position as usize > i {
                    *position -= 1;
                }
            }
        } else {
            if i != last {
                let moved = self.hash(field_at(&self.records, self.offsets.get(last)));
                self.index.replace(moved, last as u32, i as u32);
            }
            self.offsets.swap_remove(i);
        }
        self.records.tidy(&mut self.offsets, size_of_record);
        true
    }

    /// Memory it takes.
    pub fn allocation_size(&self) -> usize {
        self.records.allocation_size()
            + self.offsets.allocation_size()
            + self.index.allocation_size()
    }

    /// Its contents as a packed hash, however long.
    pub fn to_packed(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.records.used() + 10);
        put_varint(&mut out, self.len());
        for (field, value) in self.iter() {
            put_record(&mut out, field, value);
        }
        out
    }

    /// The big form of a packed hash, valid.
    pub fn from_packed(bytes: &[u8]) -> Self {
        let mut big = BigHash::with_capacity(len(bytes), bytes.len());
        for (field, value) in iter(bytes) {
            big.insert(field, value);
        }
        big
    }

    #[cfg(test)]
    fn check(&self) {
        assert_eq!(self.index.len(), self.offsets.len());
        let mut used = 0;
        for (i, (field, value)) in self.iter().enumerate() {
            assert_eq!(
                self.find(self.hash(field), field),
                Some(i),
                "a field's position"
            );
            used += record_len(field, value);
        }
        self.records.check(used);
    }
}

/// See [`BigHash::iter`].
pub struct BigPairs<'a> {
    records: &'a Records,
    offsets: PagedIter<'a, Offset>,
}

impl<'a> Iterator for BigPairs<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.offsets.next()?;
        Some(record_at(self.records, *offset))
    }
}

/// A hash being read, in either form.
#[derive(Clone, Copy)]
pub enum HashView<'a> {
    Packed(&'a [u8]),
    Big(&'a BigHash),
}

impl<'a> HashView<'a> {
    pub fn len(self) -> usize {
        match self {
            HashView::Packed(bytes) => len(bytes),
            HashView::Big(hash) => hash.len(),
        }
    }

    pub fn get(self, field: &[u8]) -> Option<&'a [u8]> {
        match self {
            HashView::Packed(bytes) => get(bytes, field),
            HashView::Big(hash) => hash.get(field),
        }
    }

    pub fn iter(self) -> ViewPairs<'a> {
        match self {
            HashView::Packed(bytes) => ViewPairs::Packed(iter(bytes)),
            HashView::Big(hash) => ViewPairs::Big(hash.iter()),
        }
    }

    /// Whether Redis would keep what it holds now as a listpack, built up from
    /// nothing.
    pub fn fits_listpack(self) -> bool {
        self.len() <= PACKED_FIELDS
            && self
                .iter()
                .all(|(f, v)| f.len() <= LISTPACK_VALUE && v.len() <= LISTPACK_VALUE)
    }
}

/// See [`HashView::iter`].
pub enum ViewPairs<'a> {
    Packed(Pairs<'a>),
    Big(BigPairs<'a>),
}

impl<'a> Iterator for ViewPairs<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ViewPairs::Packed(pairs) => pairs.next(),
            ViewPairs::Big(pairs) => pairs.next(),
        }
    }
}

/// A hash being changed, in either form: a packed one is written out again
/// afterwards, a big one is changed as this goes.
pub enum HashEdit<'e, 'a> {
    /// (By reference: moving an [`Edit`] around costs the small hashes' writes.)
    Packed(&'e mut Edit<'a>),
    Big {
        hash: &'e mut BigHash,
        changed: bool,
    },
}

impl<'a> HashEdit<'_, 'a> {
    #[inline]
    pub fn get(&self, field: &[u8]) -> Option<&[u8]> {
        match self {
            HashEdit::Packed(edit) => edit.get(field),
            HashEdit::Big { hash, .. } => hash.get(field),
        }
    }

    /// Sets `field` to `value`. Returns whether the field is new.
    #[inline]
    pub fn set(&mut self, field: &'a [u8], value: &'a [u8]) -> bool {
        match self {
            HashEdit::Packed(edit) => edit.set(field, value),
            HashEdit::Big { hash, changed } => {
                *changed = true;
                hash.insert(field, value)
            }
        }
    }

    /// Removes `field`. Returns whether it was there.
    #[inline]
    pub fn delete(&mut self, field: &[u8]) -> bool {
        match self {
            HashEdit::Packed(edit) => edit.delete(field),
            HashEdit::Big { hash, changed } => {
                let removed = hash.remove(field);
                *changed |= removed;
                removed
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Whatever goes on, it holds what a plain list does, in its order for as long
    /// as Redis would keep that order, and comes back from its bytes.
    #[test]
    fn a_big_hash_matches_a_model() {
        let mut rng = 0x9E37_79B9_7F4A_7C15u64;
        let mut below = move |n: u64| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng % n
        };
        for (fields, longest) in [(40, 20), (900, 30), (900, 200), (5_000, 2_000)] {
            let mut hash = BigHash::with_capacity(0, 0);
            let mut model: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            // Once past the limit, order is no longer promised, for good.
            let mut ordered = true;
            for step in 0..40_000u64 {
                let field = format!("field:{}", below(fields)).into_bytes();
                let found = model.iter().position(|(f, _)| *f == field);
                match below(10) {
                    0..=4 => {
                        let value = vec![b'a' + below(26) as u8; below(longest) as usize];
                        assert_eq!(hash.insert(&field, &value), found.is_none());
                        match found {
                            Some(i) => model[i].1 = value,
                            None => model.push((field.clone(), value)),
                        }
                    }
                    5..=7 => {
                        assert_eq!(hash.remove(&field), found.is_some());
                        if let Some(i) = found {
                            model.remove(i);
                        }
                    }
                    _ => assert_eq!(hash.get(&field), found.map(|i| &model[i].1[..])),
                }
                assert_eq!(hash.len(), model.len());
                ordered &= model.len() <= PACKED_FIELDS;
                if step % 2_000 == 0 {
                    hash.check();
                    if ordered {
                        assert!(
                            hash.iter()
                                .map(|(f, v)| (f.to_vec(), v.to_vec()))
                                .eq(model.iter().cloned())
                        );
                    } else {
                        let mut got: Vec<_> =
                            hash.iter().map(|(f, v)| (f.to_vec(), v.to_vec())).collect();
                        let mut want = model.clone();
                        got.sort();
                        want.sort();
                        assert_eq!(got, want);
                    }
                }
            }
            hash.check();
            if !model.is_empty() {
                let packed = hash.to_packed();
                assert!(is_valid(&packed));
                let back = BigHash::from_packed(&packed);
                back.check();
                assert!(back.iter().eq(hash.iter()));
                assert_eq!(back.to_packed(), packed);
            }
        }
    }

    /// Past the limit a removal is O(1); within it the rest stay in order.
    #[test]
    fn removal_keeps_order_within_the_limit() {
        let mut hash = BigHash::with_capacity(0, 0);
        let name = |i: usize| format!("f{i}").into_bytes();
        for i in 0..PACKED_FIELDS + 1 {
            hash.insert(&name(i), b"v");
        }
        // Over the limit: the last takes its place.
        assert!(hash.remove(&name(0)));
        assert_eq!(hash.get_index(0).0, name(PACKED_FIELDS));
        // At it: the rest move up.
        assert!(hash.remove(&name(1)));
        assert_eq!(hash.get_index(0).0, name(PACKED_FIELDS));
        assert_eq!(hash.get_index(1).0, name(2));
        hash.check();
    }

    #[test]
    fn values_change_where_they_are_if_they_fit() {
        let mut hash = BigHash::with_capacity(0, 0);
        hash.insert(b"a", b"12345");
        hash.insert(b"b", b"x");
        let bytes = hash.records.written();
        // Shorter or the same: no more written, and the rest stay readable.
        hash.insert(b"a", b"123");
        assert_eq!(hash.records.written(), bytes);
        assert_eq!(hash.records.holes(), 2);
        assert_eq!(hash.get(b"a"), Some(&b"123"[..]));
        assert_eq!(hash.get(b"b"), Some(&b"x"[..]));
        hash.check();
        // Longer: at the end.
        hash.insert(b"a", b"123456789");
        assert!(hash.records.written() > bytes);
        assert_eq!(hash.get(b"a"), Some(&b"123456789"[..]));
        hash.check();
        // A length taking another byte to write can't stay, though it's shorter.
        let long = vec![b'l'; 200];
        hash.insert(b"c", &long);
        hash.insert(b"c", &[b's'; 100]);
        assert_eq!(hash.get(b"c"), Some(&[b's'; 100][..]));
        hash.check();
    }

    #[test]
    fn holes_are_taken_back() {
        let mut hash = BigHash::with_capacity(0, 0);
        let value = [b'v'; 100];
        for i in 0..2_000 {
            hash.insert(format!("f{i}").as_bytes(), &value);
        }
        let full = hash.allocation_size();
        for i in 0..1_900 {
            hash.remove(format!("f{i}").as_bytes());
        }
        hash.check();
        let written = hash.records.written();
        assert!(written < 30_000, "{written} bytes");
        assert!(hash.allocation_size() < full / 2);
        for i in 1_900..2_000 {
            assert_eq!(hash.get(format!("f{i}").as_bytes()), Some(&value[..]));
        }
    }

    /// A value over a chunk's size has one of its own, in every way a value can change.
    #[test]
    fn values_of_many_megabytes() {
        let mut hash = BigHash::with_capacity(0, 0);
        let huge = vec![b'h'; 5 << 20];
        hash.insert(b"small", b"v");
        hash.insert(b"huge", &huge);
        hash.insert(b"other", &huge);
        assert_eq!(hash.get(b"huge"), Some(&huge[..]));
        assert_eq!(hash.get(b"small"), Some(&b"v"[..]));
        hash.check();
        // Shorter, where it is.
        hash.insert(b"huge", &huge[..huge.len() - 100]);
        assert_eq!(hash.get(b"huge"), Some(&huge[..huge.len() - 100]));
        hash.check();
        // Longer, somewhere else: what it was goes back.
        let more = vec![b'm'; 6 << 20];
        hash.insert(b"huge", &more);
        assert_eq!(hash.get(b"huge"), Some(&more[..]));
        assert_eq!(hash.get(b"other"), Some(&huge[..]));
        hash.check();
        assert!(hash.allocation_size() < (12 << 20));
        assert!(hash.remove(b"other"));
        assert!(hash.allocation_size() < (7 << 20));
        assert_eq!(hash.get(b"huge"), Some(&more[..]));
        hash.check();
    }

    #[test]
    fn offsets_go_past_4_gb() {
        let at = (5usize << 32) + 123;
        assert_eq!(Offset::new(at).get(), at);
        assert_eq!(size_of::<Offset>(), 5);
    }

    #[test]
    fn stays_packed_up_to_its_limits() {
        let fields = |n: usize, value: &[u8]| {
            let names: Vec<_> = (0..n).map(|i| format!("f{i}").into_bytes()).collect();
            let mut edit = Edit::new(None, 0);
            for name in &names {
                edit.set(name, value);
            }
            edit.encode()
        };
        assert!(fits_packed(&fields(PACKED_FIELDS, b"v")));
        assert!(!fits_packed(&fields(PACKED_FIELDS + 1, b"v")));
        assert!(fits_packed(&fields(1, &vec![b'v'; PACKED_BYTES - 16])));
        assert!(!fits_packed(&fields(1, &vec![b'v'; PACKED_BYTES])));
        let long = vec![b'v'; 65];
        assert!(HashView::Packed(&fields(10, &[b'v'; 64])).fits_listpack());
        assert!(!HashView::Packed(&fields(10, &long)).fits_listpack());
    }

    fn build(pairs: &[(&[u8], &[u8])]) -> Vec<u8> {
        let mut edit = Edit::new(None, pairs.len());
        for &(f, v) in pairs {
            edit.set(f, v);
        }
        edit.encode()
    }

    #[test]
    fn encodes_compactly_in_order() {
        let bytes = build(&[(b"name", b"tantalum"), (b"n", b"73")]);
        assert_eq!(bytes, b"\x02\x04name\x08tantalum\x01n\x0273");
        assert_eq!(len(&bytes), 2);
        let pairs: Vec<_> = iter(&bytes).collect();
        assert_eq!(pairs, [(&b"name"[..], &b"tantalum"[..]), (b"n", b"73")]);
        assert_eq!(get(&bytes, b"n"), Some(&b"73"[..]));
        assert_eq!(get(&bytes, b"x"), None);
        assert!(is_valid(&bytes));
    }

    #[test]
    fn long_lengths_take_more_bytes() {
        let value = vec![b'v'; 300];
        let bytes = build(&[(b"", &value)]);
        assert_eq!(&bytes[..4], b"\x01\x00\xac\x02");
        assert_eq!(get(&bytes, b""), Some(&value[..]));
        assert!(is_valid(&bytes));
    }

    #[test]
    fn edits_keep_order_and_drop_deleted_fields() {
        let old = build(&[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]);
        for lookups in [1, 1000] {
            let mut edit = Edit::new(Some(&old), lookups);
            assert!(edit.index.is_some() == (lookups == 1000));
            assert!(!edit.set(b"b", b"20"));
            assert!(edit.delete(b"a"));
            assert!(!edit.delete(b"a"));
            assert!(edit.set(b"d", b"4"));
            assert!(edit.set(b"a", b"5"));
            assert_eq!(edit.get(b"b"), Some(&b"20"[..]));
            assert_eq!(edit.get(b"x"), None);
            let new = edit.encode();
            let pairs: Vec<_> = iter(&new).collect();
            assert_eq!(
                pairs,
                [
                    (&b"b"[..], &b"20"[..]),
                    (b"c", b"3"),
                    (b"d", b"4"),
                    (b"a", b"5")
                ]
            );
            let mut edit = Edit::new(Some(&new), lookups);
            for field in [&b"a"[..], b"b", b"c", b"d"] {
                assert!(edit.delete(field));
            }
            assert!(edit.is_empty());
        }
    }

    #[test]
    fn compares_like_eq() {
        let words: Vec<Vec<u8>> = (0..=20)
            .flat_map(|len| {
                let base: Vec<u8> = (0..len as u8).collect();
                // Each byte changed in turn, so every position is checked.
                let changed = (0..len).map(move |i| {
                    let mut w: Vec<u8> = (0..len as u8).collect();
                    w[i] ^= 0x80;
                    w
                });
                std::iter::once(base).chain(changed)
            })
            .collect();
        for a in &words {
            for b in &words {
                assert_eq!(same(a, b), a == b, "{a:?} {b:?}");
            }
        }
    }

    #[test]
    fn rejects_damaged_values() {
        let good = build(&[(b"a", b"1"), (b"b", b"2")]);
        assert!(is_valid(&good));
        for bad in [
            &b""[..],
            b"\x00",                                         // empty
            &good[..good.len() - 1],                         // cut short
            b"\x02\x01a\x011\x01a\x012",                     // a field twice
            b"\x01\x01a\x011x",                              // something left over
            b"\x01\x05a\x011",                               // a length past the end
            b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01", // a count too big
        ] {
            assert!(!is_valid(bad), "{bad:?}");
        }
    }
}
