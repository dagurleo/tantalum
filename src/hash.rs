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
//! command for big ones.

use std::collections::HashMap;

fn put_varint(out: &mut Vec<u8>, mut n: usize) {
    while n >= 0x80 {
        out.push(n as u8 | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
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
        let index = (pairs.len().saturating_mul(lookups) > SCAN_LIMIT).then(|| {
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

#[cfg(test)]
mod tests {
    use super::*;

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
