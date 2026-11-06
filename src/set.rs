//! Sets. Small ones are packed in their entry, the way Redis packs small
//! sets in intsets and listpacks, and reply in the same order:
//!
//! ```text
//! integers:  0x01, count, then each member as a zigzag varint, ascending
//! others:    0x00, count, then each member's length and bytes, in the order added
//! ```
//!
//! A set of integers stays packed up to [`MAX_INTS`] members, any other up
//! to [`MAX_MEMBERS`] members of up to [`MAX_MEMBER_LEN`] bytes, and changes
//! rewrite it. Past that it's a [`BigSet`], kept by its shard and changed in
//! place, with members in an order of positions, so a random member (`SPOP`,
//! `SRANDMEMBER`) is O(1).

use std::borrow::Cow;

use std::hash::BuildHasher;

use crate::big::{Address, Index, Listed, Paged, Records};
use crate::resp;

/// Redis' `set-max-intset-entries`.
pub const MAX_INTS: usize = 512;
/// Redis' `set-max-listpack-entries` and `set-max-listpack-value`.
pub const MAX_MEMBERS: usize = 128;
pub const MAX_MEMBER_LEN: usize = 64;

const STRINGS: u8 = 0;
const INTS: u8 = 1;

fn put_varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        out.push(n as u8 | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

fn take_varint(bytes: &mut &[u8]) -> Option<u64> {
    let mut n = 0u64;
    for shift in (0..64).step_by(7) {
        let (&byte, rest) = bytes.split_first()?;
        *bytes = rest;
        n |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return Some(n);
        }
    }
    None
}

fn zigzag(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

fn unzigzag(n: u64) -> i64 {
    (n >> 1) as i64 ^ -((n & 1) as i64)
}

/// The member as an integer, if it's one Redis would keep in an intset:
/// written exactly as the integer prints.
fn as_int(member: &[u8]) -> Option<i64> {
    resp::parse_int(member)
}

/// A member, as stored: bytes, or an integer of a packed integer set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Member<'a> {
    Bytes(&'a [u8]),
    Int(i64),
}

impl Member<'_> {
    /// Calls `f` with the member's bytes (an integer's decimal digits).
    pub fn with_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        match *self {
            Member::Bytes(bytes) => f(bytes),
            Member::Int(n) => f(itoa::Buffer::new().format(n).as_bytes()),
        }
    }
}

/// A packed set, read in place.
#[derive(Clone, Copy)]
pub struct Compact<'a> {
    ints: bool,
    len: usize,
    body: &'a [u8],
}

impl<'a> Compact<'a> {
    /// `bytes` must be a packed set (as [`is_valid`] checks).
    pub fn new(mut bytes: &'a [u8]) -> Self {
        let (&tag, rest) = bytes.split_first().expect("a packed set");
        bytes = rest;
        let len = take_varint(&mut bytes).expect("a packed set") as usize;
        Compact {
            ints: tag == INTS,
            len,
            body: bytes,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn iter(&self) -> CompactIter<'a> {
        CompactIter {
            ints: self.ints,
            left: self.len,
            body: self.body,
        }
    }

    pub fn contains(&self, member: &[u8]) -> bool {
        if self.ints {
            let Some(n) = as_int(member) else {
                return false;
            };
            // Sorted: stop once past it.
            self.iter()
                .map(|m| match m {
                    Member::Int(i) => i,
                    Member::Bytes(_) => unreachable!("an integer set"),
                })
                .take_while(|&i| i <= n)
                .any(|i| i == n)
        } else {
            self.iter().any(|m| m == Member::Bytes(member))
        }
    }
}

pub struct CompactIter<'a> {
    ints: bool,
    left: usize,
    body: &'a [u8],
}

impl<'a> Iterator for CompactIter<'a> {
    type Item = Member<'a>;

    fn next(&mut self) -> Option<Member<'a>> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let n = take_varint(&mut self.body).expect("a packed set");
        if self.ints {
            return Some(Member::Int(unzigzag(n)));
        }
        let (member, rest) = self.body.split_at(n as usize);
        self.body = rest;
        Some(Member::Bytes(member))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left, Some(self.left))
    }
}

impl ExactSizeIterator for CompactIter<'_> {}

/// Whether `bytes` is a packed set as this module writes them, for values
/// read from files: members ascending if integers, distinct otherwise.
pub fn is_valid(mut bytes: &[u8]) -> bool {
    let Some((&tag, rest)) = bytes.split_first() else {
        return false;
    };
    bytes = rest;
    let Some(len) = take_varint(&mut bytes) else {
        return false;
    };
    if len == 0 || len as usize > bytes.len() || tag > INTS {
        return false;
    }
    if tag == INTS {
        let mut last = None;
        for _ in 0..len {
            let Some(n) = take_varint(&mut bytes).map(unzigzag) else {
                return false;
            };
            if last.is_some_and(|last| last >= n) {
                return false;
            }
            last = Some(n);
        }
    } else {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..len {
            let Some(n) = take_varint(&mut bytes) else {
                return false;
            };
            if n as usize > bytes.len() {
                return false;
            }
            let (member, rest) = bytes.split_at(n as usize);
            bytes = rest;
            if !seen.insert(member) {
                return false;
            }
        }
    }
    bytes.is_empty()
}

/// Whether a packed set is within the limits for staying packed.
pub fn fits_compact(bytes: &[u8]) -> bool {
    let set = Compact::new(bytes);
    if set.ints {
        set.len() <= MAX_INTS
    } else {
        set.len() <= MAX_MEMBERS
            && set
                .iter()
                .all(|m| m.with_bytes(|b| b.len() <= MAX_MEMBER_LEN))
    }
}

/// A big set, kept compact: its members in [`Records`], each a varint length and
/// bytes; their addresses in a list, whose order is the set's (so a random member, or
/// a position to scan from, is O(1)); and an [`Index`] of positions in that list.
/// None of them stops to grow (see [`crate::big`]). About 18 bytes per member besides
/// its own (Redis 7.2 takes about 29 for short ones), plus growth room. Removing a
/// member moves the last one into its place, as `swap_remove` does, and leaves its
/// bytes as a hole.
#[derive(Clone)]
pub struct BigSet {
    records: Records,
    offsets: Paged<u32>,
    index: Index,
    hasher: foldhash::fast::FixedState,
}

impl Default for BigSet {
    fn default() -> Self {
        BigSet::with_capacity(0)
    }
}

impl Address for u32 {
    #[inline]
    fn new(at: usize) -> Self {
        u32::try_from(at).expect("a big set under 4 GB")
    }

    #[inline]
    fn get(self) -> usize {
        self as usize
    }
}

#[inline]
fn read_member(records: &Records, offset: u32) -> &[u8] {
    let mut at = records.at(offset as usize);
    let len = take_varint(&mut at).expect("a member") as usize;
    &at[..len]
}

/// What an [`Index`] needs to hash the member at a position.
#[inline]
fn listed<'a>(
    records: &'a Records,
    offsets: &'a Paged<u32>,
    hasher: &'a foldhash::fast::FixedState,
) -> Listed<'a, u32, impl Fn(&[u8]) -> u64 + 'a> {
    Listed {
        records,
        list: offsets,
        hash_of: move |mut bytes: &[u8]| {
            let len = take_varint(&mut bytes).expect("a member") as usize;
            hasher.hash_one(&bytes[..len])
        },
    }
}

/// A record's length, from its bytes.
fn size_of_record(bytes: &[u8]) -> usize {
    let mut at = bytes;
    let len = take_varint(&mut at).expect("a member") as usize;
    bytes.len() - at.len() + len
}

impl BigSet {
    pub fn with_capacity(n: usize) -> Self {
        BigSet {
            records: Records::default(),
            offsets: Paged::with_capacity(n),
            index: Index::with_capacity(n),
            hasher: foldhash::fast::FixedState::with_seed(random_seed()),
        }
    }

    fn hash(&self, member: &[u8]) -> u64 {
        self.hasher.hash_one(member)
    }

    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.len() == 0
    }

    /// The member at position `i`.
    pub fn get_index(&self, i: usize) -> &[u8] {
        read_member(&self.records, self.offsets.get(i))
    }

    fn find(&self, hash: u64, member: &[u8]) -> Option<usize> {
        let (records, offsets) = (&self.records, &self.offsets);
        self.index
            .find(hash, |i| {
                read_member(records, offsets.get(i as usize)) == member
            })
            .map(|i| i as usize)
    }

    /// Where `member` is in the set's order.
    pub fn position(&self, member: &[u8]) -> Option<usize> {
        self.find(self.hash(member), member)
    }

    pub fn contains(&self, member: &[u8]) -> bool {
        self.position(member).is_some()
    }

    /// Adds `member`; returns whether it's new.
    pub fn insert(&mut self, member: &[u8]) -> bool {
        let hash = self.hash(member);
        if self.find(hash, member).is_some() {
            return false;
        }
        let (at, bytes) = self.records.append(varint_len(member.len()) + member.len());
        put_varint(bytes, member.len() as u64);
        bytes.extend_from_slice(member);
        self.offsets.push(u32::new(at));
        let position = (self.offsets.len() - 1) as u32;
        self.index.insert(
            hash,
            position,
            &listed(&self.records, &self.offsets, &self.hasher),
        );
        true
    }

    /// Removes `member`; returns whether it was there.
    pub fn remove(&mut self, member: &[u8]) -> bool {
        let hash = self.hash(member);
        let (records, offsets) = (&self.records, &self.offsets);
        let found = self.index.take(
            hash,
            |i| read_member(records, offsets.get(i as usize)) == member,
            &listed(records, offsets, &self.hasher),
        );
        let Some(i) = found else {
            return false;
        };
        self.forget(i as usize, varint_len(member.len()) + member.len());
        true
    }

    /// Removes the member at position `i`, moving the last one there.
    pub fn remove_at(&mut self, i: usize) {
        let member = self.get_index(i);
        let (hash, size) = (self.hash(member), varint_len(member.len()) + member.len());
        self.index.remove(
            hash,
            i as u32,
            &listed(&self.records, &self.offsets, &self.hasher),
        );
        self.forget(i, size);
    }

    /// The same, returning the member.
    pub fn swap_remove_index(&mut self, i: usize) -> Vec<u8> {
        let member = self.get_index(i).to_vec();
        self.remove_at(i);
        member
    }

    /// Lets go of position `i`, of `size` bytes, which the index no longer has, moving
    /// the last one there.
    fn forget(&mut self, i: usize, size: usize) {
        self.records.hole(self.offsets.get(i) as usize, size);
        let last = self.offsets.len() - 1;
        if i != last {
            // The last member takes position `i`.
            let moved = self.hash(self.get_index(last));
            self.index.replace(moved, last as u32, i as u32);
        }
        self.offsets.swap_remove(i);
        self.records.tidy(&mut self.offsets, size_of_record);
    }

    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.offsets
            .iter()
            .map(|&offset| read_member(&self.records, offset))
    }

    /// Memory it takes.
    pub fn allocation_size(&self) -> usize {
        self.records.allocation_size()
            + self.offsets.allocation_size()
            + self.index.allocation_size()
    }

    pub fn to_compact(&self) -> Vec<u8> {
        let mut out = vec![STRINGS];
        put_varint(&mut out, self.len() as u64);
        for member in self.iter() {
            put_varint(&mut out, member.len() as u64);
            out.extend_from_slice(member);
        }
        out
    }

    pub fn from_compact(bytes: &[u8]) -> Self {
        let set = Compact::new(bytes);
        let mut big = BigSet::with_capacity(set.len());
        for member in set.iter() {
            member.with_bytes(|m| big.insert(m));
        }
        big
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

/// A random seed for a big set's hash (so members can't be chosen to
/// collide), from the process's random state.
pub(crate) fn random_seed() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    foldhash::fast::RandomState::default().hash_one(COUNTER.fetch_add(1, Relaxed))
}

/// A set being read: packed or big.
#[derive(Clone, Copy)]
pub enum SetView<'a> {
    Compact(Compact<'a>),
    Big(&'a BigSet),
}

impl<'a> SetView<'a> {
    pub fn len(&self) -> usize {
        match self {
            SetView::Compact(set) => set.len(),
            SetView::Big(set) => set.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn contains(&self, member: &[u8]) -> bool {
        match self {
            SetView::Compact(set) => set.contains(member),
            SetView::Big(set) => set.contains(member),
        }
    }

    /// Calls `f` with each member's bytes, in reply order.
    pub fn for_each(&self, mut f: impl FnMut(&[u8])) {
        match self {
            SetView::Compact(set) => set.iter().for_each(|m| m.with_bytes(&mut f)),
            SetView::Big(set) => set.iter().for_each(f),
        }
    }

    /// The member at `i` (`0..len`), for random picks: O(1) when big.
    pub fn get(&self, i: usize) -> Member<'a> {
        match self {
            SetView::Compact(set) => set.iter().nth(i).expect("in range"),
            SetView::Big(set) => Member::Bytes(set.get_index(i)),
        }
    }
}

/// A set being changed: a small one decoded from its entry (members
/// borrowed where they can be), one grown past the packing limits, or a
/// big one in place.
pub enum SetMut<'a> {
    Small {
        /// `Some` while every member is an integer: kept ascending.
        ints: Option<Vec<i64>>,
        members: Vec<Cow<'a, [u8]>>,
        changed: bool,
    },
    Grown(BigSet),
    Big {
        set: &'a mut BigSet,
        changed: bool,
    },
}

impl<'a> SetMut<'a> {
    /// A new set, packed the way Redis would make it for `first` and about
    /// `size_hint` members.
    pub fn new(first: &[u8], size_hint: usize) -> Self {
        if as_int(first).is_some() && size_hint <= MAX_INTS {
            SetMut::Small {
                ints: Some(Vec::new()),
                members: Vec::new(),
                changed: false,
            }
        } else if size_hint <= MAX_MEMBERS {
            SetMut::Small {
                ints: None,
                members: Vec::new(),
                changed: false,
            }
        } else {
            SetMut::Grown(BigSet::with_capacity(size_hint))
        }
    }

    /// An empty integer set, as Redis starts the sets it builds results in.
    pub fn results() -> Self {
        SetMut::Small {
            ints: Some(Vec::new()),
            members: Vec::new(),
            changed: false,
        }
    }

    pub fn from_compact(set: Compact<'a>) -> Self {
        if set.ints {
            let ints = set
                .iter()
                .map(|m| match m {
                    Member::Int(n) => n,
                    Member::Bytes(_) => unreachable!("an integer set"),
                })
                .collect();
            SetMut::Small {
                ints: Some(ints),
                members: Vec::new(),
                changed: false,
            }
        } else {
            let members = set
                .iter()
                .map(|m| match m {
                    Member::Bytes(b) => Cow::Borrowed(b),
                    Member::Int(_) => unreachable!("a set of strings"),
                })
                .collect();
            SetMut::Small {
                ints: None,
                members,
                changed: false,
            }
        }
    }

    pub fn big(set: &'a mut BigSet) -> Self {
        SetMut::Big {
            set,
            changed: false,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            SetMut::Small {
                ints: Some(ints), ..
            } => ints.len(),
            SetMut::Small { members, .. } => members.len(),
            SetMut::Grown(set) => set.len(),
            SetMut::Big { set, .. } => set.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Converts to a big set now if about `size_hint` members wouldn't fit
    /// packed, as Redis does before adding many.
    pub fn reserve(&mut self, size_hint: usize) {
        let limit = match self {
            SetMut::Small { ints: Some(_), .. } => MAX_INTS,
            SetMut::Small { .. } => MAX_MEMBERS,
            _ => return,
        };
        if size_hint > limit {
            self.grow();
        }
    }

    fn grow(&mut self) {
        let SetMut::Small { ints, members, .. } = self else {
            return;
        };
        let mut big = BigSet::with_capacity(members.len().max(ints.as_ref().map_or(0, Vec::len)));
        match ints {
            Some(ints) => ints
                .iter()
                .for_each(|n| _ = big.insert(itoa::Buffer::new().format(*n).as_bytes())),
            None => members.iter().for_each(|m| _ = big.insert(m)),
        }
        *self = SetMut::Grown(big);
    }

    pub fn contains(&self, member: &[u8]) -> bool {
        match self {
            SetMut::Small {
                ints: Some(ints), ..
            } => as_int(member).is_some_and(|n| ints.binary_search(&n).is_ok()),
            SetMut::Small { members, .. } => members.iter().any(|m| m[..] == *member),
            SetMut::Grown(set) => set.contains(member),
            SetMut::Big { set, .. } => set.contains(member),
        }
    }

    /// Adds `member`; returns whether it's new.
    pub fn insert(&mut self, member: &'a [u8]) -> bool {
        match self {
            SetMut::Small {
                ints: Some(ints),
                changed,
                ..
            } => {
                if let Some(n) = as_int(member) {
                    let Err(at) = ints.binary_search(&n) else {
                        return false;
                    };
                    ints.insert(at, n);
                    *changed = true;
                    if ints.len() > MAX_INTS {
                        self.grow();
                    }
                    return true;
                }
                // The first member that isn't an integer: to strings, in
                // order, if it all still fits.
                if ints.len() < MAX_MEMBERS && member.len() <= MAX_MEMBER_LEN {
                    let mut members: Vec<Cow<'a, [u8]>> = ints
                        .iter()
                        .map(|n| Cow::Owned(itoa::Buffer::new().format(*n).as_bytes().to_vec()))
                        .collect();
                    members.push(Cow::Borrowed(member));
                    *self = SetMut::Small {
                        ints: None,
                        members,
                        changed: true,
                    };
                    return true;
                }
                self.grow();
                self.insert(member)
            }
            SetMut::Small {
                ints: None,
                members,
                changed,
            } => {
                if members.iter().any(|m| m[..] == *member) {
                    return false;
                }
                if members.len() < MAX_MEMBERS && member.len() <= MAX_MEMBER_LEN {
                    members.push(Cow::Borrowed(member));
                    *changed = true;
                    return true;
                }
                self.grow();
                self.insert(member)
            }
            SetMut::Grown(set) => set.insert(member),
            SetMut::Big { set, changed } => {
                let added = set.insert(member);
                *changed |= added;
                added
            }
        }
    }

    /// Removes `member`; returns whether it was there.
    pub fn remove(&mut self, member: &[u8]) -> bool {
        match self {
            SetMut::Small {
                ints: Some(ints),
                changed,
                ..
            } => {
                let Some(Ok(at)) = as_int(member).map(|n| ints.binary_search(&n)) else {
                    return false;
                };
                ints.remove(at);
                *changed = true;
                true
            }
            SetMut::Small {
                members, changed, ..
            } => {
                let Some(at) = members.iter().position(|m| m[..] == *member) else {
                    return false;
                };
                members.remove(at);
                *changed = true;
                true
            }
            SetMut::Grown(set) => set.remove(member),
            SetMut::Big { set, changed } => {
                let removed = set.remove(member);
                *changed |= removed;
                removed
            }
        }
    }

    /// Removes and returns the member at `i` (`0..len`).
    pub fn remove_index(&mut self, i: usize) -> Vec<u8> {
        match self {
            SetMut::Small {
                ints: Some(ints),
                changed,
                ..
            } => {
                *changed = true;
                itoa::Buffer::new()
                    .format(ints.remove(i))
                    .as_bytes()
                    .to_vec()
            }
            SetMut::Small {
                members, changed, ..
            } => {
                *changed = true;
                members.remove(i).into_owned()
            }
            SetMut::Grown(set) => set.swap_remove_index(i),
            SetMut::Big { set, changed } => {
                *changed = true;
                set.swap_remove_index(i)
            }
        }
    }

    /// Calls `f` with each member's bytes, in reply order.
    pub fn for_each(&self, mut f: impl FnMut(&[u8])) {
        match self {
            SetMut::Small {
                ints: Some(ints), ..
            } => ints
                .iter()
                .for_each(|n| f(itoa::Buffer::new().format(*n).as_bytes())),
            SetMut::Small { members, .. } => members.iter().for_each(|m| f(m)),
            SetMut::Grown(set) => set.iter().for_each(f),
            SetMut::Big { set, .. } => set.iter().for_each(f),
        }
    }

    /// How to store the result: packed, as a new big set, changed in place,
    /// or deleted when empty. `None` if nothing changed.
    pub fn finish(self) -> Option<Finished> {
        if self.is_empty() {
            let changed = match &self {
                SetMut::Small { changed, .. } | SetMut::Big { changed, .. } => *changed,
                SetMut::Grown(_) => true,
            };
            return changed.then_some(Finished::Delete);
        }
        match self {
            SetMut::Small { changed: false, .. } | SetMut::Big { changed: false, .. } => None,
            SetMut::Small {
                ints: Some(ints), ..
            } => {
                let mut out = vec![INTS];
                put_varint(&mut out, ints.len() as u64);
                for n in ints {
                    put_varint(&mut out, zigzag(n));
                }
                Some(Finished::Packed(out))
            }
            SetMut::Small { members, .. } => {
                let mut out = vec![STRINGS];
                put_varint(&mut out, members.len() as u64);
                for m in members {
                    put_varint(&mut out, m.len() as u64);
                    out.extend_from_slice(&m);
                }
                Some(Finished::Packed(out))
            }
            SetMut::Grown(set) => Some(Finished::Grown(set)),
            SetMut::Big { .. } => Some(Finished::Changed),
        }
    }
}

/// What a changed set becomes, from [`SetMut::finish`].
pub enum Finished {
    Packed(Vec<u8>),
    Grown(BigSet),
    Changed,
    Delete,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packed(set: SetMut<'_>) -> Vec<u8> {
        match set.finish() {
            Some(Finished::Packed(bytes)) => bytes,
            _ => panic!("not packed"),
        }
    }

    fn members(bytes: &[u8]) -> Vec<Vec<u8>> {
        Compact::new(bytes)
            .iter()
            .map(|m| m.with_bytes(<[u8]>::to_vec))
            .collect()
    }

    #[test]
    fn integers_stay_sorted_and_compact() {
        let mut set = SetMut::new(b"30", 3);
        for m in [&b"30"[..], b"-5", b"10", b"10"] {
            set.insert(m);
        }
        let bytes = packed(set);
        // Tag, count, then zigzag varints: -5 is 9, 10 is 20, 30 is 60.
        assert_eq!(bytes, [INTS, 3, 9, 20, 60]);
        assert_eq!(members(&bytes), [&b"-5"[..], b"10", b"30"]);
        assert!(is_valid(&bytes) && fits_compact(&bytes));
        let set = Compact::new(&bytes);
        assert!(set.contains(b"10") && !set.contains(b"11") && !set.contains(b"010"));
    }

    #[test]
    fn a_string_turns_integers_into_strings_in_order() {
        let bytes = packed({
            let mut set = SetMut::new(b"3", 2);
            set.insert(b"3");
            set.insert(b"1");
            set
        });
        let mut set = SetMut::from_compact(Compact::new(&bytes));
        assert!(set.insert(b"x"));
        assert!(!set.insert(b"1"));
        assert!(set.insert(b"2"));
        let bytes = packed(set);
        assert_eq!(members(&bytes), [&b"1"[..], b"3", b"x", b"2"]);
        assert!(is_valid(&bytes));
    }

    #[test]
    fn grows_past_the_limits_like_redis() {
        // Too many strings, a long one, too many integers, a big hint.
        let mut set = SetMut::new(b"a", 1);
        let names: Vec<Vec<u8>> = (0..=MAX_MEMBERS)
            .map(|i| format!("m{i}").into_bytes())
            .collect();
        for name in &names {
            set.insert(name);
        }
        assert!(matches!(set.finish(), Some(Finished::Grown(s)) if s.len() == MAX_MEMBERS + 1));
        let long = vec![b'x'; MAX_MEMBER_LEN + 1];
        let mut set = SetMut::new(b"a", 1);
        set.insert(b"a");
        set.insert(&long);
        assert!(matches!(set.finish(), Some(Finished::Grown(_))));
        let ints: Vec<Vec<u8>> = (0..=MAX_INTS).map(|i| i.to_string().into_bytes()).collect();
        let mut set = SetMut::new(b"0", 1);
        for n in &ints[..MAX_INTS] {
            set.insert(n);
        }
        assert!(matches!(&set, SetMut::Small { .. }));
        set.insert(&ints[MAX_INTS]);
        assert!(matches!(set.finish(), Some(Finished::Grown(s)) if s.len() == MAX_INTS + 1));
        let mut set = SetMut::new(b"a", 1);
        set.reserve(MAX_MEMBERS + 1);
        assert!(matches!(set, SetMut::Grown(_)));
    }

    #[test]
    fn removing_and_finishing() {
        let bytes = packed({
            let mut set = SetMut::new(b"a", 2);
            set.insert(b"a");
            set.insert(b"b");
            set
        });
        let mut set = SetMut::from_compact(Compact::new(&bytes));
        assert!(!set.remove(b"z"));
        assert!(set.finish().is_none(), "unchanged");
        let mut set = SetMut::from_compact(Compact::new(&bytes));
        assert!(set.remove(b"a") && set.remove(b"b"));
        assert!(matches!(set.finish(), Some(Finished::Delete)));
    }

    #[test]
    fn big_sets_count_their_memory_and_round_trip() {
        let mut big = BigSet::default();
        assert!(big.insert(b"hello") && !big.insert(b"hello"));
        let before = big.allocation_size();
        big.insert(&[7; 100]);
        assert!(big.allocation_size() > before);
        assert!(big.remove(b"hello") && !big.remove(b"hello"));
        let again = BigSet::from_compact(&big.to_compact());
        assert_eq!(
            again.iter().collect::<Vec<_>>(),
            big.iter().collect::<Vec<_>>()
        );
    }

    /// Against a model, through removals that move members and compactions
    /// that rewrite the buffer.
    #[test]
    fn big_sets_match_a_model() {
        let mut big = BigSet::default();
        let mut model = std::collections::HashSet::new();
        let mut rng = 0x9E37_79B9_7F4A_7C15u64;
        for step in 0..200_000u64 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let member = format!("member-{}", rng % 5_000).into_bytes();
            match rng % 7 {
                0..3 => assert_eq!(big.insert(&member), model.insert(member), "step {step}"),
                3 | 4 => assert_eq!(big.remove(&member), model.remove(&member), "step {step}"),
                5 if !big.is_empty() => {
                    let i = (rng >> 8) as usize % big.len();
                    let gone = big.swap_remove_index(i);
                    assert!(model.remove(&gone), "step {step}");
                }
                _ => assert_eq!(
                    big.contains(&member),
                    model.contains(&member),
                    "step {step}"
                ),
            }
            assert_eq!(big.len(), model.len());
        }
        let members: std::collections::HashSet<Vec<u8>> = big.iter().map(<[u8]>::to_vec).collect();
        assert_eq!(members, model);
        for i in 0..big.len() {
            assert_eq!(big.position(big.get_index(i)), Some(i));
        }
    }

    #[test]
    fn rejects_damaged_values() {
        for bad in [
            &b""[..],
            b"\x00\x00",           // empty
            b"\x01\x02\x14\x14",   // integers not ascending
            b"\x00\x02\x01a\x01a", // a member twice
            b"\x00\x01\x05a",      // length past the end
            b"\x02\x01\x00",       // unknown tag
            b"\x00\x01\x01ax",     // left over
        ] {
            assert!(!is_valid(bad), "{bad:?}");
        }
    }
}
