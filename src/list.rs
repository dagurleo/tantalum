//! Lists. A small one is packed in its entry:
//!
//! ```text
//! count, then each element head first: its length (a varint), its bytes,
//! and its length again, as a varint written backwards
//! ```
//!
//! The second length lets the list be read from the tail, as Redis'
//! listpacks are, so the last elements are as cheap as the first. It's
//! packed up to [`MAX_PACKED_LEN`] elements in [`MAX_PACKED_BYTES`]. A bigger one is
//! a [`BigList`] its shard keeps: chunks of up to [`CHUNK_MAX`] bytes in the
//! same format (like Redis' quicklist of listpacks), so pushing and popping
//! at either end touch one chunk, and finding an index walks chunk counts.
//! Writes to a packed list decode it into one chunk (a copy), change that,
//! and pack it again if it still fits.

use std::collections::VecDeque;

/// Packed lists stay within these.
pub const MAX_PACKED_LEN: usize = 128;
pub const MAX_PACKED_BYTES: usize = 4096;
/// A chunk grows to this many bytes before a new one starts (one element
/// may make it bigger).
pub const CHUNK_MAX: usize = 8192;

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

fn varint_len(mut n: usize) -> usize {
    let mut len = 1;
    while n >= 0x80 {
        n >>= 7;
        len += 1;
    }
    len
}

fn encoded_len(element: &[u8]) -> usize {
    2 * varint_len(element.len()) + element.len()
}

fn encode(out: &mut Vec<u8>, element: &[u8]) {
    put_varint(out, element.len() as u64);
    out.extend_from_slice(element);
    put_backward_varint(out, element.len());
}

/// A varint whose bytes are reversed, so it reads from its end backwards.
fn put_backward_varint(out: &mut Vec<u8>, n: usize) {
    let start = out.len();
    put_varint(out, n as u64);
    out[start..].reverse();
}

/// Reads the backwards varint that ends at `end`: the number and where it
/// starts.
fn take_backward_varint(bytes: &[u8], end: usize) -> Option<(usize, usize)> {
    let mut n = 0usize;
    let mut at = end;
    for shift in (0..64).step_by(7) {
        at = at.checked_sub(1)?;
        let byte = bytes[at];
        n |= usize::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return Some((n, at));
        }
    }
    None
}

/// The element starting at `start`: its bytes' range and where it ends.
fn element_at(bytes: &[u8], start: usize) -> (std::ops::Range<usize>, usize) {
    let mut rest = &bytes[start..];
    let len = take_varint(&mut rest).expect("an element") as usize;
    let data = bytes.len() - rest.len();
    (data..data + len, data + len + varint_len(len))
}

/// The element ending at `end`: its bytes' range and where it starts.
fn element_before(bytes: &[u8], end: usize) -> (std::ops::Range<usize>, usize) {
    let (len, back) = take_backward_varint(bytes, end).expect("an element");
    let data = back - len;
    (data..back, data - varint_len(len))
}

/// The elements of a chunk body (or a packed list after its count), from
/// the head, or from the tail backwards.
#[derive(Clone)]
pub struct Elements<'a> {
    bytes: &'a [u8],
    start: usize,
    end: usize,
}

impl<'a> Iterator for Elements<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        if self.start >= self.end {
            return None;
        }
        let (data, next) = element_at(self.bytes, self.start);
        self.start = next;
        Some(&self.bytes[data])
    }
}

impl DoubleEndedIterator for Elements<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.start >= self.end {
            return None;
        }
        let (data, before) = element_before(self.bytes, self.end);
        self.end = before;
        Some(&self.bytes[data])
    }
}

fn elements(bytes: &[u8]) -> Elements<'_> {
    Elements {
        bytes,
        start: 0,
        end: bytes.len(),
    }
}

/// The byte range of element `i` of `count` in a chunk body, walking from
/// whichever end is nearer.
fn span(bytes: &[u8], i: usize, count: usize) -> std::ops::Range<usize> {
    if i < count / 2 {
        let mut at = 0;
        for _ in 0..i {
            at = element_at(bytes, at).1;
        }
        at..element_at(bytes, at).1
    } else {
        let mut end = bytes.len();
        for _ in 0..count - 1 - i {
            end = element_before(bytes, end).1;
        }
        element_before(bytes, end).1..end
    }
}

/// A packed list, read in place.
#[derive(Clone, Copy)]
pub struct Packed<'a> {
    len: usize,
    body: &'a [u8],
}

impl<'a> Packed<'a> {
    pub fn new(mut bytes: &'a [u8]) -> Self {
        let len = take_varint(&mut bytes).expect("a packed list") as usize;
        Packed { len, body: bytes }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn iter(&self) -> Elements<'a> {
        elements(self.body)
    }
}

/// Whether `bytes` is a packed list as this module writes them.
pub fn is_valid(mut bytes: &[u8]) -> bool {
    let Some(len) = take_varint(&mut bytes) else {
        return false;
    };
    if len == 0 || len as usize > bytes.len() {
        return false;
    }
    for _ in 0..len {
        let Some(n) = take_varint(&mut bytes) else {
            return false;
        };
        let n = n as usize;
        let back = varint_len(n);
        if n + back > bytes.len() {
            return false;
        }
        let mut expected = Vec::new();
        put_backward_varint(&mut expected, n);
        if bytes[n..n + back] != expected[..] {
            return false;
        }
        bytes = &bytes[n + back..];
    }
    bytes.is_empty()
}

pub fn fits_packed(bytes: &[u8]) -> bool {
    bytes.len() <= MAX_PACKED_BYTES && Packed::new(bytes).len() <= MAX_PACKED_LEN
}

#[derive(Clone, Default)]
struct Chunk {
    /// Elements, each a varint length and bytes.
    bytes: Vec<u8>,
    count: usize,
}

impl Chunk {
    fn span(&self, i: usize) -> std::ops::Range<usize> {
        span(&self.bytes, i, self.count)
    }

    fn get(&self, i: usize) -> &[u8] {
        let start = self.span(i).start;
        &self.bytes[element_at(&self.bytes, start).0]
    }
}

/// A big list: chunks of packed elements.
#[derive(Clone, Default)]
pub struct BigList {
    chunks: VecDeque<Chunk>,
    len: usize,
}

impl BigList {
    /// A list of the elements of a packed list (one chunk, a copy).
    pub fn from_packed(bytes: &[u8]) -> Self {
        let packed = Packed::new(bytes);
        let mut list = BigList::default();
        if packed.len() > 0 {
            list.chunks.push_back(Chunk {
                bytes: packed.body.to_vec(),
                count: packed.len(),
            });
            list.len = packed.len();
        }
        list.split_big_chunks();
        list
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Whether it would fit packed.
    pub fn fits_packed(&self) -> bool {
        self.len <= MAX_PACKED_LEN && self.packed_len() <= MAX_PACKED_BYTES
    }

    fn packed_len(&self) -> usize {
        varint_len(self.len) + self.chunks.iter().map(|c| c.bytes.len()).sum::<usize>()
    }

    /// In the packed format, however long, for snapshots and small lists.
    pub fn to_packed(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.packed_len());
        put_varint(&mut out, self.len as u64);
        for chunk in &self.chunks {
            out.extend_from_slice(&chunk.bytes);
        }
        out
    }

    /// Splits chunks past `CHUNK_MAX` (with more than one element) in two,
    /// until none are.
    fn split_big_chunks(&mut self) {
        let mut c = 0;
        while c < self.chunks.len() {
            let chunk = &self.chunks[c];
            if chunk.bytes.len() <= CHUNK_MAX || chunk.count < 2 {
                c += 1;
                continue;
            }
            // Split after the first element that ends past half.
            let half = chunk.bytes.len() / 2;
            let (mut at, mut n) = (0usize, 0usize);
            while n < chunk.count - 1 {
                at = element_at(&chunk.bytes, at).1;
                n += 1;
                if at >= half {
                    break;
                }
            }
            let chunk = &mut self.chunks[c];
            let tail = chunk.bytes.split_off(at);
            let tail_count = chunk.count - n;
            chunk.count = n;
            chunk.bytes.shrink_to_fit();
            self.chunks.insert(
                c + 1,
                Chunk {
                    bytes: tail,
                    count: tail_count,
                },
            );
        }
    }

    /// Drops chunk `c` if empty, or merges it into a neighbor if both are
    /// small, so chunks don't wither into many tiny ones.
    fn tidy(&mut self, c: usize) {
        if c >= self.chunks.len() {
            return;
        }
        if self.chunks[c].count == 0 {
            self.chunks.remove(c);
            return;
        }
        let small = |chunk: &Chunk| chunk.bytes.len() < CHUNK_MAX / 4;
        if !small(&self.chunks[c]) {
            return;
        }
        for (a, b) in [(c.wrapping_sub(1), c), (c, c + 1)] {
            if b < self.chunks.len()
                && a < self.chunks.len()
                && small(&self.chunks[a])
                && self.chunks[a].bytes.len() + self.chunks[b].bytes.len() <= CHUNK_MAX / 2
            {
                let next = self.chunks.remove(b).expect("a chunk");
                let into = &mut self.chunks[a];
                into.bytes.extend_from_slice(&next.bytes);
                into.count += next.count;
                return;
            }
        }
    }

    pub fn push_front(&mut self, element: &[u8]) {
        let size = encoded_len(element);
        match self.chunks.front_mut() {
            Some(chunk) if chunk.bytes.len() + size <= CHUNK_MAX => {
                let mut encoded = Vec::with_capacity(size);
                encode(&mut encoded, element);
                chunk.bytes.splice(0..0, encoded);
                chunk.count += 1;
            }
            _ => {
                let mut bytes = Vec::with_capacity(size.max(256));
                encode(&mut bytes, element);
                self.chunks.push_front(Chunk { bytes, count: 1 });
            }
        }
        self.len += 1;
    }

    pub fn push_back(&mut self, element: &[u8]) {
        let size = encoded_len(element);
        match self.chunks.back_mut() {
            Some(chunk) if chunk.bytes.len() + size <= CHUNK_MAX => {
                encode(&mut chunk.bytes, element);
                chunk.count += 1;
            }
            _ => {
                let mut bytes = Vec::with_capacity(size.max(256));
                encode(&mut bytes, element);
                self.chunks.push_back(Chunk { bytes, count: 1 });
            }
        }
        self.len += 1;
    }

    pub fn pop_front(&mut self) -> Option<Vec<u8>> {
        self.remove(0)
    }

    pub fn pop_back(&mut self) -> Option<Vec<u8>> {
        self.len.checked_sub(1).and_then(|last| self.remove(last))
    }

    /// The chunk holding element `i` and its index there.
    fn locate(&self, mut i: usize) -> Option<(usize, usize)> {
        if i >= self.len {
            return None;
        }
        // From whichever end is nearer.
        if i < self.len / 2 {
            for (c, chunk) in self.chunks.iter().enumerate() {
                if i < chunk.count {
                    return Some((c, i));
                }
                i -= chunk.count;
            }
        } else {
            let mut from_end = self.len - 1 - i;
            for (c, chunk) in self.chunks.iter().enumerate().rev() {
                if from_end < chunk.count {
                    return Some((c, chunk.count - 1 - from_end));
                }
                from_end -= chunk.count;
            }
        }
        unreachable!("counts add up to len")
    }

    pub fn get(&self, i: usize) -> Option<&[u8]> {
        let (c, at) = self.locate(i)?;
        Some(self.chunks[c].get(at))
    }

    /// Sets element `i`; false if out of range.
    pub fn set(&mut self, i: usize, element: &[u8]) -> bool {
        let Some((c, at)) = self.locate(i) else {
            return false;
        };
        let chunk = &mut self.chunks[c];
        let range = chunk.span(at);
        let mut encoded = Vec::with_capacity(encoded_len(element));
        encode(&mut encoded, element);
        chunk.bytes.splice(range, encoded);
        self.split_big_chunks();
        true
    }

    /// Inserts `element` so it becomes element `i` (`i <= len`).
    pub fn insert(&mut self, i: usize, element: &[u8]) {
        if i == 0 {
            return self.push_front(element);
        }
        if i == self.len {
            return self.push_back(element);
        }
        let (c, at) = self.locate(i).expect("in range");
        let chunk = &mut self.chunks[c];
        let offset = chunk.span(at).start;
        let mut encoded = Vec::with_capacity(encoded_len(element));
        encode(&mut encoded, element);
        chunk.bytes.splice(offset..offset, encoded);
        chunk.count += 1;
        self.len += 1;
        self.split_big_chunks();
    }

    /// Removes and returns element `i`.
    pub fn remove(&mut self, i: usize) -> Option<Vec<u8>> {
        let (c, at) = self.locate(i)?;
        let chunk = &mut self.chunks[c];
        let range = chunk.span(at);
        let element = chunk.bytes[element_at(&chunk.bytes, range.start).0].to_vec();
        chunk.bytes.drain(range);
        chunk.count -= 1;
        self.len -= 1;
        self.tidy(c);
        Some(element)
    }

    /// Keeps elements `start..end` only (`start <= end <= len`).
    pub fn trim(&mut self, start: usize, end: usize) {
        let mut kept = BigList::default();
        for element in self.iter().skip(start).take(end - start) {
            kept.push_back(element);
        }
        *self = kept;
    }

    /// Removes up to `count` elements equal to `value` (all if 0), from
    /// the head, or from the tail if `from_tail`. Returns how many.
    pub fn remove_matching(&mut self, value: &[u8], count: usize, from_tail: bool) -> usize {
        let mut positions: Vec<usize> = self
            .iter()
            .enumerate()
            .filter(|(_, e)| *e == value)
            .map(|(i, _)| i)
            .collect();
        if from_tail {
            positions.reverse();
        }
        if count > 0 {
            positions.truncate(count);
        }
        let removed = positions.len();
        if removed == 0 {
            return 0;
        }
        let gone: std::collections::HashSet<usize> = positions.into_iter().collect();
        let mut kept = BigList::default();
        for (i, element) in self.iter().enumerate() {
            if !gone.contains(&i) {
                kept.push_back(element);
            }
        }
        *self = kept;
        removed
    }

    /// Elements from `start` on, head to tail.
    pub fn iter_from(&self, start: usize) -> impl Iterator<Item = &[u8]> {
        let (first, at) = self.locate(start).unwrap_or((self.chunks.len(), 0));
        // Where element `start` begins in its chunk, found from the nearer end.
        let offset = self
            .chunks
            .get(first)
            .map_or(0, |chunk| chunk.span(at).start);
        self.chunks
            .range(first..)
            .enumerate()
            .flat_map(move |(n, chunk)| Elements {
                bytes: &chunk.bytes,
                start: if n == 0 { offset } else { 0 },
                end: chunk.bytes.len(),
            })
    }

    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.chunks.iter().flat_map(|chunk| elements(&chunk.bytes))
    }

    /// Elements tail to head.
    pub fn iter_rev(&self) -> impl Iterator<Item = &[u8]> {
        self.chunks
            .iter()
            .rev()
            .flat_map(|chunk| elements(&chunk.bytes).rev())
    }

    pub fn allocation_size(&self) -> usize {
        self.chunks.capacity() * size_of::<Chunk>()
            + self
                .chunks
                .iter()
                .map(|c| c.bytes.capacity())
                .sum::<usize>()
    }
}

/// A list being read: packed or big.
#[derive(Clone, Copy)]
pub enum ListView<'a> {
    Packed(Packed<'a>),
    Big(&'a BigList),
}

impl<'a> ListView<'a> {
    pub fn len(&self) -> usize {
        match self {
            ListView::Packed(list) => list.len(),
            ListView::Big(list) => list.len(),
        }
    }

    pub fn get(&self, i: usize) -> Option<&'a [u8]> {
        match *self {
            ListView::Packed(list) => list.iter().nth(i),
            ListView::Big(list) => list.get(i),
        }
    }

    /// Calls `f` with elements `start..end`, head to tail.
    pub fn for_range(&self, start: usize, end: usize, mut f: impl FnMut(&[u8])) {
        match *self {
            ListView::Packed(list) => list.iter().skip(start).take(end - start).for_each(f),
            ListView::Big(list) => list.iter_from(start).take(end - start).for_each(&mut f),
        }
    }

    /// Calls `f` with each element and its index, from the head, or from
    /// the tail if `rev`, until it returns false.
    pub fn search(&self, rev: bool, mut f: impl FnMut(usize, &[u8]) -> bool) {
        let len = self.len();
        match (*self, rev) {
            (ListView::Packed(list), false) => {
                for (i, e) in list.iter().enumerate() {
                    if !f(i, e) {
                        return;
                    }
                }
            }
            (ListView::Packed(list), true) => {
                for (n, e) in list.iter().rev().enumerate() {
                    if !f(len - 1 - n, e) {
                        return;
                    }
                }
            }
            (ListView::Big(list), false) => {
                for (i, e) in list.iter().enumerate() {
                    if !f(i, e) {
                        return;
                    }
                }
            }
            (ListView::Big(list), true) => {
                for (n, e) in list.iter_rev().enumerate() {
                    if !f(len - 1 - n, e) {
                        return;
                    }
                }
            }
        }
    }
}

/// A list being changed: a packed one decoded into a [`BigList`] (which
/// is stored packed again if it still fits), or a big one in place.
pub enum ListMut<'a> {
    Owned {
        list: BigList,
        changed: bool,
    },
    Big {
        list: &'a mut BigList,
        changed: bool,
    },
}

/// What a changed list becomes, from [`ListMut::finish`].
pub enum Finished {
    Packed(Vec<u8>),
    Grown(BigList),
    Changed,
    Delete,
}

impl<'a> ListMut<'a> {
    pub fn new() -> Self {
        ListMut::Owned {
            list: BigList::default(),
            changed: false,
        }
    }

    pub fn from_packed(bytes: &[u8]) -> Self {
        ListMut::Owned {
            list: BigList::from_packed(bytes),
            changed: false,
        }
    }

    pub fn big(list: &'a mut BigList) -> Self {
        ListMut::Big {
            list,
            changed: false,
        }
    }

    /// The list, to read.
    pub fn list(&self) -> &BigList {
        match self {
            ListMut::Owned { list, .. } => list,
            ListMut::Big { list, .. } => list,
        }
    }

    /// The list, to change (counted as changed).
    pub fn edit(&mut self) -> &mut BigList {
        match self {
            ListMut::Owned { list, changed } => {
                *changed = true;
                list
            }
            ListMut::Big { list, changed } => {
                *changed = true;
                list
            }
        }
    }

    pub fn len(&self) -> usize {
        self.list().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn finish(self) -> Option<Finished> {
        match self {
            ListMut::Owned { changed: false, .. } | ListMut::Big { changed: false, .. } => None,
            ListMut::Owned { list, .. } if list.is_empty() => Some(Finished::Delete),
            ListMut::Owned { list, .. } if list.fits_packed() => {
                Some(Finished::Packed(list.to_packed()))
            }
            ListMut::Owned { list, .. } => Some(Finished::Grown(list)),
            ListMut::Big { list, .. } if list.is_empty() => Some(Finished::Delete),
            ListMut::Big { .. } => Some(Finished::Changed),
        }
    }
}

impl Default for ListMut<'_> {
    fn default() -> Self {
        ListMut::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all(list: &BigList) -> Vec<Vec<u8>> {
        list.iter().map(<[u8]>::to_vec).collect()
    }

    #[test]
    fn packed_lists_round_trip() {
        let mut list = BigList::default();
        for e in [&b"b"[..], b"c"] {
            list.push_back(e);
        }
        list.push_front(b"a");
        let bytes = list.to_packed();
        assert_eq!(bytes, b"\x03\x01a\x01\x01b\x01\x01c\x01");
        assert!(is_valid(&bytes) && fits_packed(&bytes));
        let again = BigList::from_packed(&bytes);
        assert_eq!(all(&again), [&b"a"[..], b"b", b"c"]);
        let packed = Packed::new(&bytes);
        assert_eq!(packed.iter().collect::<Vec<_>>(), [&b"a"[..], b"b", b"c"]);
        for bad in [
            &b""[..],
            b"\x00",
            b"\x02\x01a\x01",
            b"\x01\x05a\x05",
            b"\x01\x01a\x01b",
            b"\x01\x01a\x02",
        ] {
            assert!(!is_valid(bad), "{bad:?}");
        }
    }

    /// Against a `VecDeque`, through every operation, with chunks splitting
    /// and merging.
    #[test]
    fn matches_a_model() {
        let mut list = BigList::default();
        let mut model: VecDeque<Vec<u8>> = VecDeque::new();
        let mut rng = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move |n: u64| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng % n.max(1)
        };
        for step in 0..60_000u64 {
            let len = model.len() as u64;
            let element = match next(20) {
                0 => vec![b'x'; 3_000 + next(10_000) as usize],
                _ => format!("e{}", next(50)).into_bytes(),
            };
            match next(12) {
                0 | 1 => {
                    list.push_front(&element);
                    model.push_front(element);
                }
                2 | 3 => {
                    list.push_back(&element);
                    model.push_back(element);
                }
                4 => assert_eq!(list.pop_front(), model.pop_front(), "step {step}"),
                5 => assert_eq!(list.pop_back(), model.pop_back(), "step {step}"),
                6 => {
                    let i = next(len + 1) as usize;
                    assert_eq!(list.get(i), model.get(i).map(Vec::as_slice), "step {step}");
                }
                7 if len > 0 => {
                    let i = next(len) as usize;
                    assert!(list.set(i, &element));
                    model[i] = element;
                }
                8 => {
                    let i = next(len + 1) as usize;
                    list.insert(i, &element);
                    model.insert(i, element);
                }
                9 if len > 0 => {
                    let i = next(len) as usize;
                    assert_eq!(list.remove(i), model.remove(i), "step {step}");
                }
                10 if len > 10 => {
                    let start = next(len / 4) as usize;
                    let end = len as usize - next(len / 4) as usize;
                    list.trim(start, end);
                    model = model.into_iter().skip(start).take(end - start).collect();
                }
                11 => {
                    let count = next(3) as usize;
                    let from_tail = next(2) == 0;
                    let value = format!("e{}", next(50)).into_bytes();
                    let expected = {
                        let mut positions: Vec<usize> =
                            (0..model.len()).filter(|&i| model[i] == value).collect();
                        if from_tail {
                            positions.reverse();
                        }
                        if count > 0 {
                            positions.truncate(count);
                        }
                        let mut sorted = positions.clone();
                        sorted.sort_unstable();
                        for &i in sorted.iter().rev() {
                            model.remove(i);
                        }
                        positions.len()
                    };
                    assert_eq!(
                        list.remove_matching(&value, count, from_tail),
                        expected,
                        "step {step}"
                    );
                }
                _ => {}
            }
            assert_eq!(list.len(), model.len(), "step {step}");
            assert_eq!(
                list.chunks.iter().map(|c| c.count).sum::<usize>(),
                list.len()
            );
            if step % 1_000 == 0 {
                assert_eq!(all(&list), Vec::from(model.clone()), "step {step}");
                let rev: Vec<Vec<u8>> = list.iter_rev().map(<[u8]>::to_vec).collect();
                let mut expected = Vec::from(model.clone());
                expected.reverse();
                assert_eq!(rev, expected);
                let start = next(len + 1) as usize;
                let from: Vec<Vec<u8>> = list.iter_from(start).map(<[u8]>::to_vec).collect();
                assert_eq!(from, model.iter().skip(start).cloned().collect::<Vec<_>>());
                for chunk in &list.chunks {
                    assert!(chunk.count > 0, "an empty chunk");
                    assert!(
                        chunk.bytes.len() <= CHUNK_MAX || chunk.count == 1,
                        "an oversized chunk"
                    );
                }
            }
        }
    }
}
