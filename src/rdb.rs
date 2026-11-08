//! Redis' RDB format, for payloads Redis and Tantalum hand each other
//! (`DUMP` and `RESTORE`, `FUNCTION DUMP` and `FUNCTION RESTORE`): lengths,
//! strings with their integer and LZF encodings (Redis' own LZF,
//! `vendor/lzf`), values of every type in the encodings Redis writes and
//! reads (listpacks, ziplists, intsets, quicklists, stream nodes), and the
//! footer of a dump: the RDB version and a CRC-64 (Jones) of what's before.

use std::collections::BTreeMap;
use std::ffi::{c_uint, c_void};

use crate::stream::{BigStream, Group, NodeLimits, StreamId, UNKNOWN_READS};

/// The RDB version Redis 7.2 writes.
pub(crate) const VERSION: u16 = 11;
/// A function library: its code follows, as a string.
pub(crate) const OPCODE_FUNCTION2: u8 = 245;
/// Libraries in the format before Redis 7.0's release.
pub(crate) const OPCODE_FUNCTION_PRE_GA: u8 = 246;

const LEN_6BIT: u8 = 0;
const LEN_14BIT: u8 = 1;
const LEN_32BIT: u8 = 0x80;
const LEN_64BIT: u8 = 0x81;
const ENCODED: u8 = 3;
const ENC_INT8: u8 = 0;
const ENC_INT16: u8 = 1;
const ENC_INT32: u8 = 2;
const ENC_LZF: u8 = 3;

unsafe extern "C" {
    fn lzf_compress(
        in_data: *const c_void,
        in_len: c_uint,
        out: *mut c_void,
        out_len: c_uint,
    ) -> c_uint;
    fn lzf_decompress(
        in_data: *const c_void,
        in_len: c_uint,
        out: *mut c_void,
        out_len: c_uint,
    ) -> c_uint;
}

/// Redis' `rdbSaveLen`.
pub(crate) fn put_len(out: &mut Vec<u8>, len: u64) {
    if len < 1 << 6 {
        out.push(len as u8 | LEN_6BIT << 6);
    } else if len < 1 << 14 {
        out.push((len >> 8) as u8 | LEN_14BIT << 6);
        out.push(len as u8);
    } else if len <= u64::from(u32::MAX) {
        out.push(LEN_32BIT);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    } else {
        out.push(LEN_64BIT);
        out.extend_from_slice(&len.to_be_bytes());
    }
}

/// Redis' `rdbSaveRawString`: as an integer if it's one in canonical form and
/// fits 32 bits, LZF-compressed if that saves at least four bytes (over 20
/// bytes, as Redis tries it), or as it is.
pub(crate) fn put_string(out: &mut Vec<u8>, s: &[u8]) {
    if s.len() <= 11
        && let Some(n) = canonical_int(s)
    {
        if let Ok(n) = i8::try_from(n) {
            out.extend_from_slice(&[ENCODED << 6 | ENC_INT8, n as u8]);
            return;
        }
        if let Ok(n) = i16::try_from(n) {
            out.push(ENCODED << 6 | ENC_INT16);
            out.extend_from_slice(&n.to_le_bytes());
            return;
        }
        if let Ok(n) = i32::try_from(n) {
            out.push(ENCODED << 6 | ENC_INT32);
            out.extend_from_slice(&n.to_le_bytes());
            return;
        }
    }
    if s.len() > 20
        && s.len() <= c_uint::MAX as usize
        && let Some(compressed) = compress(s)
    {
        out.push(ENCODED << 6 | ENC_LZF);
        put_len(out, compressed.len() as u64);
        put_len(out, s.len() as u64);
        out.extend_from_slice(&compressed);
        return;
    }
    put_len(out, s.len() as u64);
    out.extend_from_slice(s);
}

/// `s` as Redis' `string2ll` reads it: digits with an optional `-`, no
/// leading zeros (but `0`), within 64 bits.
fn canonical_int(s: &[u8]) -> Option<i64> {
    let n: i64 = std::str::from_utf8(s).ok()?.parse().ok()?;
    (itoa::Buffer::new().format(n).as_bytes() == s).then_some(n)
}

/// LZF, if it saves at least four bytes (Redis' `rdbSaveLzfStringObject`).
fn compress(s: &[u8]) -> Option<Vec<u8>> {
    if s.len() <= 4 {
        return None;
    }
    let room = s.len() - 4;
    let mut out = vec![0u8; room + 1];
    // SAFETY: both buffers are as long as said; LZF writes at most `room` bytes.
    let n = unsafe {
        lzf_compress(
            s.as_ptr().cast(),
            s.len() as c_uint,
            out.as_mut_ptr().cast(),
            room as c_uint,
        )
    };
    if n == 0 {
        return None;
    }
    out.truncate(n as usize);
    Some(out)
}

/// Reads RDB data from a slice.
pub(crate) struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Reader { data, pos: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    pub(crate) fn byte(&mut self) -> Option<u8> {
        let b = *self.data.get(self.pos)?;
        self.pos += 1;
        Some(b)
    }

    pub(crate) fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let bytes = self.data.get(self.pos..self.pos.checked_add(n)?)?;
        self.pos += n;
        Some(bytes)
    }

    /// Redis' `rdbLoadLenByRef`: a length, or an encoding (`true`).
    fn len_or_encoding(&mut self) -> Option<(u64, bool)> {
        let first = self.byte()?;
        match first >> 6 {
            ENCODED => Some((u64::from(first & 0x3f), true)),
            LEN_6BIT => Some((u64::from(first & 0x3f), false)),
            LEN_14BIT => Some((
                u64::from(first & 0x3f) << 8 | u64::from(self.byte()?),
                false,
            )),
            _ => match first {
                LEN_32BIT => {
                    let b = self.take(4)?;
                    Some((u64::from(u32::from_be_bytes(b.try_into().ok()?)), false))
                }
                LEN_64BIT => {
                    let b = self.take(8)?;
                    Some((u64::from_be_bytes(b.try_into().ok()?), false))
                }
                _ => None,
            },
        }
    }

    pub(crate) fn len(&mut self) -> Option<u64> {
        match self.len_or_encoding()? {
            (len, false) => Some(len),
            (_, true) => None,
        }
    }

    /// Redis' `rdbGenericLoadStringObject`.
    pub(crate) fn string(&mut self) -> Option<Vec<u8>> {
        let (len, encoded) = self.len_or_encoding()?;
        if !encoded {
            return Some(self.take(usize::try_from(len).ok()?)?.to_vec());
        }
        let int = |n: i64| itoa::Buffer::new().format(n).as_bytes().to_vec();
        match len as u8 {
            ENC_INT8 => Some(int(i64::from(self.byte()? as i8))),
            ENC_INT16 => Some(int(i64::from(i16::from_le_bytes(
                self.take(2)?.try_into().ok()?,
            )))),
            ENC_INT32 => Some(int(i64::from(i32::from_le_bytes(
                self.take(4)?.try_into().ok()?,
            )))),
            ENC_LZF => {
                let compressed_len = usize::try_from(self.len()?).ok()?;
                let len = usize::try_from(self.len()?).ok()?;
                let compressed = self.take(compressed_len)?;
                // (LZF makes at most 264 bytes of 3, so no more can be there.)
                if len > crate::resp::MAX_BULK_LEN
                    || compressed_len > c_uint::MAX as usize
                    || len > compressed_len.saturating_mul(88).saturating_add(8)
                {
                    return None;
                }
                let mut out = vec![0u8; len];
                // SAFETY: both buffers are as long as said.
                let n = unsafe {
                    lzf_decompress(
                        compressed.as_ptr().cast(),
                        compressed_len as c_uint,
                        out.as_mut_ptr().cast(),
                        len as c_uint,
                    )
                };
                (n as usize == len).then_some(out)
            }
            _ => None,
        }
    }
}

/// Object types (Redis' `RDB_TYPE_*`) this reads, and the ones it writes.
pub(crate) const TYPE_STRING: u8 = 0;
const TYPE_LIST: u8 = 1;
const TYPE_SET: u8 = 2;
const TYPE_ZSET: u8 = 3;
const TYPE_HASH: u8 = 4;
const TYPE_ZSET_2: u8 = 5;
const TYPE_HASH_ZIPMAP: u8 = 9;
const TYPE_LIST_ZIPLIST: u8 = 10;
const TYPE_SET_INTSET: u8 = 11;
const TYPE_ZSET_ZIPLIST: u8 = 12;
const TYPE_HASH_ZIPLIST: u8 = 13;
const TYPE_LIST_QUICKLIST: u8 = 14;
const TYPE_HASH_LISTPACK: u8 = 16;
const TYPE_ZSET_LISTPACK: u8 = 17;
const TYPE_LIST_QUICKLIST_2: u8 = 18;
const TYPE_SET_LISTPACK: u8 = 20;
const TYPE_STREAM_LISTPACKS: u8 = 15;
const TYPE_STREAM_LISTPACKS_2: u8 = 19;
const TYPE_STREAM_LISTPACKS_3: u8 = 21;

/// A stream entry's flags in its node's listpack.
const ENTRY_DELETED: i64 = 1;
const ENTRY_SAME_FIELDS: i64 = 2;

/// A quicklist node holding one element as it is, or a listpack of them.
const NODE_PLAIN: u64 = 1;
const NODE_PACKED: u64 = 2;

/// An element of a listpack or ziplist: bytes, or an integer.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Element {
    Bytes(Vec<u8>),
    Int(i64),
}

impl Element {
    pub(crate) fn into_bytes(self) -> Vec<u8> {
        match self {
            Element::Bytes(b) => b,
            Element::Int(n) => itoa::Buffer::new().format(n).as_bytes().to_vec(),
        }
    }
}

/// Redis' listpacks, built an entry at a time.
pub(crate) struct Listpack {
    bytes: Vec<u8>,
    count: usize,
}

impl Listpack {
    pub(crate) fn new() -> Self {
        Listpack {
            bytes: vec![0; 6],
            count: 0,
        }
    }

    /// An entry: an integer if it's one in canonical form, as Redis stores it.
    pub(crate) fn push(&mut self, s: &[u8]) {
        match canonical_int(s) {
            Some(n) => self.push_int(n),
            None => self.push_bytes(s),
        }
    }

    pub(crate) fn push_int(&mut self, v: i64) {
        let start = self.bytes.len();
        let out = &mut self.bytes;
        if (0..=127).contains(&v) {
            out.push(v as u8);
        } else if (-4096..=4095).contains(&v) {
            let u = if v < 0 { (1 << 13) + v } else { v } as u64;
            out.extend_from_slice(&[(u >> 8) as u8 | 0xc0, u as u8]);
        } else if (-32768..=32767).contains(&v) {
            out.push(0xf1);
            out.extend_from_slice(&(v as i16).to_le_bytes());
        } else if (-8_388_608..=8_388_607).contains(&v) {
            out.push(0xf2);
            out.extend_from_slice(&(v as i32).to_le_bytes()[..3]);
        } else if (-2_147_483_648..=2_147_483_647).contains(&v) {
            out.push(0xf3);
            out.extend_from_slice(&(v as i32).to_le_bytes());
        } else {
            out.push(0xf4);
            out.extend_from_slice(&v.to_le_bytes());
        }
        self.finish_entry(start);
    }

    pub(crate) fn push_bytes(&mut self, s: &[u8]) {
        let start = self.bytes.len();
        let out = &mut self.bytes;
        let len = s.len();
        if len < 64 {
            out.push(len as u8 | 0x80);
        } else if len < 4096 {
            out.extend_from_slice(&[(len >> 8) as u8 | 0xe0, len as u8]);
        } else {
            out.push(0xf0);
            out.extend_from_slice(&(len as u32).to_le_bytes());
        }
        out.extend_from_slice(s);
        self.finish_entry(start);
    }

    /// The entry's length, written backwards after it (Redis' `lpEncodeBacklen`).
    fn finish_entry(&mut self, start: usize) {
        let l = (self.bytes.len() - start) as u64;
        let out = &mut self.bytes;
        if l <= 127 {
            out.push(l as u8);
        } else if l < 16383 {
            out.extend_from_slice(&[(l >> 7) as u8, (l & 127) as u8 | 128]);
        } else if l < 2_097_151 {
            out.extend_from_slice(&[
                (l >> 14) as u8,
                ((l >> 7) & 127) as u8 | 128,
                (l & 127) as u8 | 128,
            ]);
        } else if l < 268_435_455 {
            out.extend_from_slice(&[
                (l >> 21) as u8,
                ((l >> 14) & 127) as u8 | 128,
                ((l >> 7) & 127) as u8 | 128,
                (l & 127) as u8 | 128,
            ]);
        } else {
            out.extend_from_slice(&[
                (l >> 28) as u8,
                ((l >> 21) & 127) as u8 | 128,
                ((l >> 14) & 127) as u8 | 128,
                ((l >> 7) & 127) as u8 | 128,
                (l & 127) as u8 | 128,
            ]);
        }
        self.count += 1;
    }

    /// Its bytes, the end marker included: Redis' `lpBytes`.
    pub(crate) fn len(&self) -> usize {
        self.bytes.len() + 1
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub(crate) fn finish(mut self) -> Vec<u8> {
        self.bytes.push(0xff);
        let total = self.bytes.len() as u32;
        self.bytes[..4].copy_from_slice(&total.to_le_bytes());
        let count = self.count.min(u16::MAX as usize) as u16;
        self.bytes[4..6].copy_from_slice(&count.to_le_bytes());
        self.bytes
    }
}

/// A listpack's entries, or `None` if it isn't one (Redis' listpack.c, read with
/// every bound checked).
pub(crate) fn listpack_entries(lp: &[u8]) -> Option<Vec<Element>> {
    if lp.len() < 7 || u32::from_le_bytes(lp[..4].try_into().ok()?) as usize != lp.len() {
        return None;
    }
    if *lp.last()? != 0xff {
        return None;
    }
    let mut at = 6;
    let mut out = Vec::new();
    let end = lp.len() - 1;
    let get = |at: usize, n: usize| lp.get(at..at.checked_add(n)?).filter(|_| at + n <= end);
    while at < end {
        let b = lp[at];
        let (element, len) = if b & 0x80 == 0 {
            (Element::Int(i64::from(b)), 1)
        } else if b & 0xc0 == 0x80 {
            let n = usize::from(b & 0x3f);
            (Element::Bytes(get(at + 1, n)?.to_vec()), 1 + n)
        } else if b & 0xe0 == 0xc0 {
            let u = (u64::from(b & 0x1f) << 8) | u64::from(*get(at + 1, 1)?.first()?);
            let v = if u >= 1 << 12 {
                u as i64 - (1 << 13)
            } else {
                u as i64
            };
            (Element::Int(v), 2)
        } else if b & 0xf0 == 0xe0 {
            let n = (usize::from(b & 0x0f) << 8) | usize::from(*get(at + 1, 1)?.first()?);
            (Element::Bytes(get(at + 2, n)?.to_vec()), 2 + n)
        } else {
            match b {
                0xf0 => {
                    let n = u32::from_le_bytes(get(at + 1, 4)?.try_into().ok()?) as usize;
                    (Element::Bytes(get(at + 5, n)?.to_vec()), 5 + n)
                }
                0xf1 => (
                    Element::Int(i64::from(i16::from_le_bytes(
                        get(at + 1, 2)?.try_into().ok()?,
                    ))),
                    3,
                ),
                0xf2 => {
                    let b3 = get(at + 1, 3)?;
                    let v = i32::from_le_bytes([b3[0], b3[1], b3[2], 0]) << 8 >> 8;
                    (Element::Int(i64::from(v)), 4)
                }
                0xf3 => (
                    Element::Int(i64::from(i32::from_le_bytes(
                        get(at + 1, 4)?.try_into().ok()?,
                    ))),
                    5,
                ),
                0xf4 => (
                    Element::Int(i64::from_le_bytes(get(at + 1, 8)?.try_into().ok()?)),
                    9,
                ),
                _ => return None,
            }
        };
        at += len;
        // The length again, backwards.
        let backlen = if len <= 127 {
            1
        } else if len < 16383 {
            2
        } else if len < 2_097_151 {
            3
        } else if len < 268_435_455 {
            4
        } else {
            5
        };
        // It says how long the entry was, seven bits a byte from the last.
        let mut said = 0;
        for (i, &b) in get(at, backlen)?.iter().rev().enumerate() {
            said |= usize::from(b & 127) << (7 * i);
            if (b & 128 == 0) != (i == backlen - 1) {
                return None;
            }
        }
        if said != len {
            return None;
        }
        at += backlen;
        out.push(element);
    }
    let count = u16::from_le_bytes([lp[4], lp[5]]);
    (count == u16::MAX || usize::from(count) == out.len()).then_some(out)
}

/// A ziplist's entries (Redis 6's encoding, which older payloads have), or
/// `None` if it isn't one.
pub(crate) fn ziplist_entries(zl: &[u8]) -> Option<Vec<Element>> {
    if zl.len() < 11 || u32::from_le_bytes(zl[..4].try_into().ok()?) as usize != zl.len() {
        return None;
    }
    let end = zl.len() - 1;
    if zl[end] != 0xff {
        return None;
    }
    let get = |at: usize, n: usize| zl.get(at..at.checked_add(n)?).filter(|_| at + n <= end);
    let mut at = 10;
    let mut out = Vec::new();
    let (mut previous, mut last) = (0, 10);
    while at < end {
        // The previous entry's length, which has to be right.
        let start = at;
        let said = match *get(at, 1)?.first()? {
            n @ 0..254 => {
                at += 1;
                usize::from(n)
            }
            254 => {
                let n = u32::from_le_bytes(get(at + 1, 4)?.try_into().ok()?) as usize;
                at += 5;
                n
            }
            _ => return None,
        };
        if said != previous {
            return None;
        }
        let b = *get(at, 1)?.first()?;
        let (element, len) = match b >> 6 {
            0 => {
                let n = usize::from(b & 0x3f);
                (Element::Bytes(get(at + 1, n)?.to_vec()), 1 + n)
            }
            1 => {
                let n = (usize::from(b & 0x3f) << 8) | usize::from(*get(at + 1, 1)?.first()?);
                (Element::Bytes(get(at + 2, n)?.to_vec()), 2 + n)
            }
            2 => {
                let n = u32::from_be_bytes(get(at + 1, 4)?.try_into().ok()?) as usize;
                (Element::Bytes(get(at + 5, n)?.to_vec()), 5 + n)
            }
            _ => match b {
                0xc0 => (
                    Element::Int(i64::from(i16::from_le_bytes(
                        get(at + 1, 2)?.try_into().ok()?,
                    ))),
                    3,
                ),
                0xd0 => (
                    Element::Int(i64::from(i32::from_le_bytes(
                        get(at + 1, 4)?.try_into().ok()?,
                    ))),
                    5,
                ),
                0xe0 => (
                    Element::Int(i64::from_le_bytes(get(at + 1, 8)?.try_into().ok()?)),
                    9,
                ),
                0xf0 => {
                    let b3 = get(at + 1, 3)?;
                    let v = i32::from_le_bytes([b3[0], b3[1], b3[2], 0]) << 8 >> 8;
                    (Element::Int(i64::from(v)), 4)
                }
                0xfe => (Element::Int(i64::from(*get(at + 1, 1)?.first()? as i8)), 2),
                0xf1..=0xfd => (Element::Int(i64::from(b & 0x0f) - 1), 1),
                _ => return None,
            },
        };
        at += len;
        previous = at - start;
        last = start;
        out.push(element);
    }
    // The header's offset of the last entry, and count (unless it's too many to say).
    let tail = u32::from_le_bytes(zl[4..8].try_into().ok()?) as usize;
    let count = u16::from_le_bytes([zl[8], zl[9]]);
    (tail == last && (count == u16::MAX || usize::from(count) == out.len())).then_some(out)
}

/// A zipmap's fields and values (Redis 2.4's small hashes), or `None` if it
/// isn't one, empty included (Redis' `zipmapValidateIntegrity`, deep).
fn zipmap_pairs(zm: &[u8]) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
    // A length: a byte, or 254 and four more (little endian).
    let len = |pos: &mut usize| -> Option<usize> {
        let first = *zm.get(*pos)?;
        if first < 254 {
            *pos += 1;
            return Some(usize::from(first));
        }
        let bytes = zm.get(*pos + 1..*pos + 5)?;
        *pos += 5;
        usize::try_from(u32::from_le_bytes(bytes.try_into().ok()?)).ok()
    };
    if zm.len() < 2 {
        return None;
    }
    let mut pos = 1;
    let mut pairs = Vec::new();
    while *zm.get(pos)? != 255 {
        let field_len = len(&mut pos)?;
        let field = zm.get(pos..pos.checked_add(field_len)?)?.to_vec();
        pos += field_len;
        let value_len = len(&mut pos)?;
        let free = usize::from(*zm.get(pos)?);
        pos += 1;
        let value = zm.get(pos..pos.checked_add(value_len)?)?.to_vec();
        pos = pos.checked_add(value_len)?.checked_add(free)?;
        pairs.push((field, value));
    }
    let count_ok = zm[0] == 254 || usize::from(zm[0]) == pairs.len();
    (!pairs.is_empty() && count_ok).then_some(pairs)
}

/// Whether no two items have the same name: Redis refuses a payload with
/// repeats in the formats it reads into a hash table.
fn unique<T>(items: &[T], name: impl Fn(&T) -> &[u8]) -> bool {
    let mut seen = std::collections::HashSet::with_capacity(items.len());
    items.iter().all(|item| seen.insert(name(item)))
}

/// An intset's integers, or `None` if it isn't one.
fn intset_members(blob: &[u8]) -> Option<Vec<i64>> {
    if blob.len() < 8 {
        return None;
    }
    let width = u32::from_le_bytes(blob[..4].try_into().ok()?) as usize;
    let count = u32::from_le_bytes(blob[4..8].try_into().ok()?) as usize;
    if !matches!(width, 2 | 4 | 8) || blob.len() != 8 + width.checked_mul(count)? {
        return None;
    }
    Some(
        blob[8..]
            .chunks_exact(width)
            .map(|c| match width {
                2 => i64::from(i16::from_le_bytes([c[0], c[1]])),
                4 => i64::from(i32::from_le_bytes([c[0], c[1], c[2], c[3]])),
                _ => i64::from_le_bytes(c.try_into().expect("8 bytes")),
            })
            .collect(),
    )
}

/// An intset of these, sorted (Redis' layout: the narrowest width that fits them all).
fn intset(mut members: Vec<i64>) -> Vec<u8> {
    members.sort_unstable();
    let width: u32 = if members.iter().all(|&m| i16::try_from(m).is_ok()) {
        2
    } else if members.iter().all(|&m| i32::try_from(m).is_ok()) {
        4
    } else {
        8
    };
    let mut out = Vec::with_capacity(8 + members.len() * width as usize);
    out.extend_from_slice(&width.to_le_bytes());
    out.extend_from_slice(&(members.len() as u32).to_le_bytes());
    for m in members {
        match width {
            2 => out.extend_from_slice(&(m as i16).to_le_bytes()),
            4 => out.extend_from_slice(&(m as i32).to_le_bytes()),
            _ => out.extend_from_slice(&m.to_le_bytes()),
        }
    }
    out
}

/// A sorted set's score as Redis' listpack keeps it: an integer if it's a whole
/// one in `long long`'s safe range (`double2ll`), otherwise as `d2string` prints it.
fn push_score(lp: &mut Listpack, score: f64) {
    const SAFE: f64 = 4_611_686_018_427_387_903.0;
    if (-SAFE..=SAFE).contains(&score) && score.fract() == 0.0 {
        lp.push_int(score as i64);
    } else {
        let mut text = Vec::new();
        crate::zset::format_score(score, &mut text);
        lp.push_bytes(&text);
    }
}

/// How a set is written: the encoding Redis would have it in.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum SetEncoding {
    Intset,
    Listpack,
    Hashtable,
}

/// The payload of `DUMP` for a string (Redis' `RDB_TYPE_STRING`).
pub(crate) fn dump_string(out: &mut Vec<u8>, value: &[u8]) {
    out.push(TYPE_STRING);
    put_string(out, value);
}

/// A list as Redis 7.2 writes one: a quicklist of listpacks (one, if it would
/// be a listpack), split where Redis splits a list it was given an element at
/// a time at its end, with `list-max-listpack-size -2`. It's a listpack until
/// one more would take it past 8 KB, and that's its first node; after that, a
/// node takes another while it stays 8 bytes short of 8 KB. (Lists made
/// otherwise have their nodes elsewhere in Redis, as it made them.)
pub(crate) fn dump_list<'a>(out: &mut Vec<u8>, elements: impl Iterator<Item = &'a [u8]>) {
    const NODE_BYTES: usize = 8192;
    let mut nodes = Vec::new();
    let mut node = Listpack::new();
    for element in elements {
        let slack = if nodes.is_empty() { 0 } else { 8 };
        if !node.is_empty() && node.len() + element.len() + slack > NODE_BYTES {
            nodes.push(std::mem::replace(&mut node, Listpack::new()).finish());
        }
        node.push(element);
    }
    if !node.is_empty() {
        nodes.push(node.finish());
    }
    out.push(TYPE_LIST_QUICKLIST_2);
    put_len(out, nodes.len() as u64);
    for node in nodes {
        put_len(out, NODE_PACKED);
        put_string(out, &node);
    }
}

pub(crate) fn dump_set<'a>(
    out: &mut Vec<u8>,
    members: impl Iterator<Item = &'a [u8]>,
    len: usize,
    encoding: SetEncoding,
) {
    match encoding {
        SetEncoding::Intset => {
            out.push(TYPE_SET_INTSET);
            let ints = members.filter_map(canonical_int).collect();
            put_string(out, &intset(ints));
        }
        SetEncoding::Listpack => {
            out.push(TYPE_SET_LISTPACK);
            let mut lp = Listpack::new();
            members.for_each(|m| lp.push(m));
            put_string(out, &lp.finish());
        }
        SetEncoding::Hashtable => {
            out.push(TYPE_SET);
            put_len(out, len as u64);
            members.for_each(|m| put_string(out, m));
        }
    }
}

/// A sorted set, its members in order (ascending): a listpack, or Redis'
/// `ZSET_2`, from the greatest down, with binary scores.
pub(crate) fn dump_zset(out: &mut Vec<u8>, members: &[(f64, &[u8])], listpack: bool) {
    if listpack {
        out.push(TYPE_ZSET_LISTPACK);
        let mut lp = Listpack::new();
        for &(score, member) in members {
            lp.push(member);
            push_score(&mut lp, score);
        }
        put_string(out, &lp.finish());
    } else {
        out.push(TYPE_ZSET_2);
        put_len(out, members.len() as u64);
        for &(score, member) in members.iter().rev() {
            put_string(out, member);
            out.extend_from_slice(&score.to_le_bytes());
        }
    }
}

pub(crate) fn dump_hash<'a>(
    out: &mut Vec<u8>,
    pairs: impl Iterator<Item = (&'a [u8], &'a [u8])>,
    len: usize,
    listpack: bool,
) {
    if listpack {
        out.push(TYPE_HASH_LISTPACK);
        let mut lp = Listpack::new();
        for (field, value) in pairs {
            lp.push(field);
            lp.push(value);
        }
        put_string(out, &lp.finish());
    } else {
        out.push(TYPE_HASH);
        put_len(out, len as u64);
        for (field, value) in pairs {
            put_string(out, field);
            put_string(out, value);
        }
    }
}

/// A value read from a payload.
#[derive(Debug, PartialEq)]
pub(crate) enum Object {
    String(Vec<u8>),
    List(Vec<Vec<u8>>),
    Set(Vec<Vec<u8>>),
    ZSet(Vec<(Vec<u8>, f64)>),
    Hash(Vec<(Vec<u8>, Vec<u8>)>),
    /// As [`BigStream::to_compact`] has it.
    Stream(Vec<u8>),
}

/// Redis' `rdbLoadDoubleValue`: a length, then the number as text (or 253-255
/// for NaN and the infinities).
fn text_double(reader: &mut Reader<'_>) -> Option<f64> {
    match reader.byte()? {
        255 => Some(f64::NEG_INFINITY),
        254 => Some(f64::INFINITY),
        253 => Some(f64::NAN),
        n => {
            let text = std::str::from_utf8(reader.take(usize::from(n))?).ok()?;
            crate::zset::parse_score(text.as_bytes()).or_else(|| text.trim().parse().ok())
        }
    }
}

/// An element as a score, as Redis reads a listpack's.
fn element_score(e: Element) -> Option<f64> {
    match e {
        Element::Int(n) => Some(n as f64),
        Element::Bytes(b) => crate::zset::parse_score(&b),
    }
}

/// Redis' `rdbLoadObject`, for every type a payload from Redis 2.6 to 7.2 can
/// hold but streams and modules: the value, or `None` for a payload that isn't
/// one, or is empty (as Redis refuses).
pub(crate) fn load_object(kind: u8, reader: &mut Reader<'_>, limits: NodeLimits) -> Option<Object> {
    let pairs = |elements: Vec<Element>| -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        if !elements.len().is_multiple_of(2) {
            return None;
        }
        let mut it = elements.into_iter();
        let mut pairs = Vec::new();
        while let (Some(a), Some(b)) = (it.next(), it.next()) {
            pairs.push((a.into_bytes(), b.into_bytes()));
        }
        Some(pairs)
    };
    let zset_pairs = |elements: Vec<Element>| -> Option<Vec<(Vec<u8>, f64)>> {
        if !elements.len().is_multiple_of(2) {
            return None;
        }
        let mut it = elements.into_iter();
        let mut out = Vec::new();
        while let (Some(member), Some(score)) = (it.next(), it.next()) {
            out.push((member.into_bytes(), element_score(score)?));
        }
        Some(out)
    };
    let len = |reader: &mut Reader<'_>| usize::try_from(reader.len()?).ok();
    let object = match kind {
        TYPE_STRING => Object::String(reader.string()?),
        TYPE_LIST | TYPE_SET => {
            let n = len(reader)?;
            let mut items = Vec::with_capacity(n.min(1 << 16));
            for _ in 0..n {
                items.push(reader.string()?);
            }
            if kind == TYPE_LIST {
                Object::List(items)
            } else if unique(&items, |m| m) {
                Object::Set(items)
            } else {
                return None;
            }
        }
        TYPE_ZSET | TYPE_ZSET_2 => {
            let n = len(reader)?;
            let mut members = Vec::with_capacity(n.min(1 << 16));
            for _ in 0..n {
                let member = reader.string()?;
                let score = if kind == TYPE_ZSET_2 {
                    f64::from_le_bytes(reader.take(8)?.try_into().ok()?)
                } else {
                    text_double(reader)?
                };
                if score.is_nan() {
                    return None;
                }
                members.push((member, score));
            }
            if !unique(&members, |(m, _)| m) {
                return None;
            }
            Object::ZSet(members)
        }
        TYPE_HASH => {
            let n = len(reader)?;
            let mut pairs = Vec::with_capacity(n.min(1 << 16));
            for _ in 0..n {
                let field = reader.string()?;
                pairs.push((field, reader.string()?));
            }
            // Redis looks for repeats only in one too big for a listpack (by
            // default), and keeps them in a small one: here the first is kept.
            if pairs.len() > 128 && !unique(&pairs, |(f, _)| f) {
                return None;
            }
            let mut seen = std::collections::HashSet::new();
            pairs.retain(|(f, _)| seen.insert(f.clone()));
            Object::Hash(pairs)
        }
        TYPE_HASH_ZIPMAP => {
            let pairs = zipmap_pairs(&reader.string()?)?;
            if !unique(&pairs, |(f, _)| f) {
                return None;
            }
            Object::Hash(pairs)
        }
        TYPE_SET_INTSET => {
            let blob = reader.string()?;
            let members = intset_members(&blob)?;
            Object::Set(
                members
                    .into_iter()
                    .map(|n| Element::Int(n).into_bytes())
                    .collect(),
            )
        }
        TYPE_SET_LISTPACK => {
            let elements = listpack_entries(&reader.string()?)?;
            Object::Set(elements.into_iter().map(Element::into_bytes).collect())
        }
        TYPE_LIST_ZIPLIST => {
            let elements = ziplist_entries(&reader.string()?)?;
            Object::List(elements.into_iter().map(Element::into_bytes).collect())
        }
        TYPE_LIST_QUICKLIST | TYPE_LIST_QUICKLIST_2 => {
            let nodes = len(reader)?;
            let mut items = Vec::new();
            for _ in 0..nodes {
                let container = if kind == TYPE_LIST_QUICKLIST_2 {
                    reader.len()?
                } else {
                    NODE_PACKED
                };
                let blob = reader.string()?;
                match container {
                    NODE_PLAIN => items.push(blob),
                    NODE_PACKED => {
                        let elements = if kind == TYPE_LIST_QUICKLIST_2 {
                            listpack_entries(&blob)?
                        } else {
                            ziplist_entries(&blob)?
                        };
                        // (Redis skips an empty node.)
                        items.extend(elements.into_iter().map(Element::into_bytes));
                    }
                    _ => return None,
                }
            }
            Object::List(items)
        }
        TYPE_ZSET_ZIPLIST | TYPE_ZSET_LISTPACK => {
            let blob = reader.string()?;
            let elements = if kind == TYPE_ZSET_LISTPACK {
                listpack_entries(&blob)?
            } else {
                ziplist_entries(&blob)?
            };
            Object::ZSet(zset_pairs(elements)?)
        }
        TYPE_HASH_ZIPLIST | TYPE_HASH_LISTPACK => {
            let blob = reader.string()?;
            let elements = if kind == TYPE_HASH_LISTPACK {
                listpack_entries(&blob)?
            } else {
                ziplist_entries(&blob)?
            };
            Object::Hash(pairs(elements)?)
        }
        TYPE_STREAM_LISTPACKS | TYPE_STREAM_LISTPACKS_2 | TYPE_STREAM_LISTPACKS_3 => {
            Object::Stream(load_stream(kind, reader, limits)?)
        }
        _ => return None,
    };
    let empty = match &object {
        Object::String(_) | Object::Stream(_) => false,
        Object::List(v) | Object::Set(v) => v.is_empty(),
        Object::ZSet(v) => v.is_empty(),
        Object::Hash(v) => v.is_empty(),
    };
    (!empty).then_some(object)
}

/// CRC-64 with the Jones polynomial, reflected, as Redis' `crc64`.
/// A stream ID as Redis' radix trees key them: 128 bits, big endian.
fn put_stream_id(out: &mut Vec<u8>, id: StreamId) {
    out.extend_from_slice(&id.ms.to_be_bytes());
    out.extend_from_slice(&id.seq.to_be_bytes());
}

fn stream_id(raw: &[u8]) -> Option<StreamId> {
    let (ms, seq) = (raw.get(..8)?, raw.get(8..16)?);
    Some(StreamId {
        ms: u64::from_be_bytes(ms.try_into().ok()?),
        seq: u64::from_be_bytes(seq.try_into().ok()?),
    })
}

/// Redis' `rdbSaveMillisecondTime`.
fn put_millis(out: &mut Vec<u8>, ms: i64) {
    out.extend_from_slice(&ms.to_le_bytes());
}

fn millis(reader: &mut Reader<'_>) -> Option<i64> {
    Some(i64::from_le_bytes(reader.take(8)?.try_into().ok()?))
}

/// A stream, as `RDB_TYPE_STREAM_LISTPACKS_3`: each chunk as the listpack node
/// Redis has for it (but for entries Redis would keep there marked deleted,
/// which are gone here), its IDs, and its groups with their consumers and
/// pending entries.
pub(crate) fn dump_stream(out: &mut Vec<u8>, stream: &BigStream) {
    out.push(TYPE_STREAM_LISTPACKS_3);
    put_len(out, stream.chunk_count() as u64);
    for node in stream.nodes() {
        let master = node.master();
        let mut key = Vec::with_capacity(16);
        put_stream_id(&mut key, master);
        put_string(out, &key);
        let mut lp = Listpack::new();
        lp.push_int(i64::from(node.live()));
        lp.push_int(0);
        let fields: Vec<&[u8]> = node.fields().collect();
        lp.push_int(fields.len() as i64);
        for field in fields {
            lp.push(field);
        }
        lp.push_int(0);
        for (entry, same) in node.entries() {
            lp.push_int(if same { ENTRY_SAME_FIELDS } else { 0 });
            lp.push_int(entry.id.ms.wrapping_sub(master.ms) as i64);
            lp.push_int(entry.id.seq.wrapping_sub(master.seq) as i64);
            if !same {
                lp.push_int(entry.count as i64);
            }
            for (field, value) in entry.pairs() {
                if !same {
                    lp.push(field);
                }
                lp.push(value);
            }
            // How many entries it took, so it can be walked back.
            let count = entry.count as i64;
            lp.push_int(if same { count + 3 } else { 2 * count + 4 });
        }
        put_string(out, &lp.finish());
    }
    put_len(out, stream.len() as u64);
    for id in [stream.last_id(), stream.first_id(), stream.max_deleted()] {
        put_len(out, id.ms);
        put_len(out, id.seq);
    }
    put_len(out, stream.entries_added());
    put_len(out, stream.groups.len() as u64);
    for (name, group) in &stream.groups {
        put_string(out, name);
        put_len(out, group.last_id.ms);
        put_len(out, group.last_id.seq);
        put_len(out, group.entries_read as u64);
        put_len(out, group.pending().len() as u64);
        for (id, pending) in group.pending().iter() {
            let (time, count) = (pending.delivery_time, pending.delivery_count);
            put_stream_id(out, id);
            put_millis(out, time);
            put_len(out, count as u64);
        }
        put_len(out, group.consumer_count() as u64);
        for consumer in group.consumers() {
            put_string(out, consumer.name());
            put_millis(out, consumer.seen_time);
            put_millis(out, consumer.active_time);
            put_len(out, consumer.pending().len() as u64);
            for (id, ()) in consumer.pending().iter() {
                put_stream_id(out, id);
            }
        }
    }
}

/// An element that's a number, as Redis' `lpGetIntegerIfValid` reads it.
fn next_int(elements: &mut impl Iterator<Item = Element>) -> Option<i64> {
    match elements.next()? {
        Element::Int(n) => Some(n),
        Element::Bytes(b) => canonical_int(&b),
    }
}

fn next_bytes(elements: &mut impl Iterator<Item = Element>) -> Option<Vec<u8>> {
    elements.next().map(Element::into_bytes)
}

/// A stream from `RDB_TYPE_STREAM_LISTPACKS` (Redis 5 and 6), `_2` (7.0) or
/// `_3` (7.2), as [`BigStream::to_compact`]'s bytes: its entries added again in
/// order, so that they fall into chunks as this server's limits say, and those
/// marked deleted left out. What Redis would load but couldn't then use (IDs
/// out of order, a length that isn't what's there, a pending entry no consumer
/// has) isn't a stream here.
fn load_stream(kind: u8, reader: &mut Reader<'_>, limits: NodeLimits) -> Option<Vec<u8>> {
    let mut stream = BigStream::default();
    let mut previous = StreamId::MIN;
    for _ in 0..reader.len()? {
        let key = reader.string()?;
        if key.len() != 16 {
            return None;
        }
        let master = stream_id(&key)?;
        let elements = listpack_entries(&reader.string()?)?;
        let mut it = elements.into_iter();
        let count = next_int(&mut it)?;
        let deleted = next_int(&mut it)?;
        let nfields = usize::try_from(next_int(&mut it)?).ok()?;
        let mut fields = Vec::with_capacity(nfields.min(1024));
        for _ in 0..nfields {
            fields.push(next_bytes(&mut it)?);
        }
        if next_int(&mut it)? != 0 || count < 0 || deleted < 0 {
            return None;
        }
        for _ in 0..count.checked_add(deleted)? {
            let flags = next_int(&mut it)?;
            let id = StreamId {
                ms: master.ms.wrapping_add(next_int(&mut it)? as u64),
                seq: master.seq.wrapping_add(next_int(&mut it)? as u64),
            };
            let same = flags & ENTRY_SAME_FIELDS != 0;
            let n = if same {
                nfields
            } else {
                usize::try_from(next_int(&mut it)?).ok()?
            };
            let mut pairs = Vec::with_capacity(n.min(1024));
            if same {
                for field in &fields {
                    pairs.push((field.clone(), next_bytes(&mut it)?));
                }
            } else {
                for _ in 0..n {
                    let field = next_bytes(&mut it)?;
                    pairs.push((field, next_bytes(&mut it)?));
                }
            }
            let expected = if same { n as i64 + 3 } else { 2 * n as i64 + 4 };
            if next_int(&mut it)? != expected {
                return None;
            }
            if flags & ENTRY_DELETED != 0 {
                continue;
            }
            if id <= previous || pairs.is_empty() {
                return None;
            }
            previous = id;
            let pairs: Vec<(&[u8], &[u8])> = pairs
                .iter()
                .map(|(f, v)| (f.as_slice(), v.as_slice()))
                .collect();
            stream.add(id, &pairs, limits);
        }
        if it.next().is_some() {
            return None;
        }
    }
    let length = reader.len()?;
    let last_id = StreamId {
        ms: reader.len()?,
        seq: reader.len()?,
    };
    let (max_deleted, entries_added) = if kind == TYPE_STREAM_LISTPACKS {
        (StreamId::MIN, length)
    } else {
        // The first ID, which is the first entry's.
        reader.len()?;
        reader.len()?;
        let max_deleted = StreamId {
            ms: reader.len()?,
            seq: reader.len()?,
        };
        (max_deleted, reader.len()?)
    };
    if length != stream.len() as u64 || last_id < previous {
        return None;
    }
    stream.set_ids(last_id, Some(entries_added), max_deleted);
    for _ in 0..reader.len()? {
        let name = reader.string()?;
        let last = StreamId {
            ms: reader.len()?,
            seq: reader.len()?,
        };
        let entries_read = if kind == TYPE_STREAM_LISTPACKS {
            stream.entries_up_to(last).unwrap_or(UNKNOWN_READS)
        } else {
            reader.len()? as i64
        };
        if entries_read < UNKNOWN_READS {
            return None;
        }
        let mut group = Group::new(last, entries_read);
        // Each pending entry, then whose it is from the consumers' lists.
        let mut pending = BTreeMap::new();
        for _ in 0..reader.len()? {
            let id = stream_id(reader.take(16)?)?;
            let time = millis(reader)?;
            let count = reader.len()? as i64;
            if pending.insert(id, (time, count, None)).is_some() {
                return None;
            }
        }
        for _ in 0..reader.len()? {
            let name = reader.string()?;
            let seen = millis(reader)?;
            let active = if kind == TYPE_STREAM_LISTPACKS_3 {
                millis(reader)?
            } else {
                seen
            };
            let (consumer, created) = group.consumer(&name, seen);
            // (-1 is never: below that isn't a time.)
            if !created || active < -1 {
                return None;
            }
            group.consumer_mut(consumer).active_time = active;
            for _ in 0..reader.len()? {
                let id = stream_id(reader.take(16)?)?;
                let (_, _, owner) = pending.get_mut(&id)?;
                if owner.replace(consumer).is_some() {
                    return None;
                }
            }
        }
        for (id, (time, count, owner)) in pending {
            group.assign(id, owner?, time, count);
        }
        if stream.groups.insert(name.into(), group).is_some() {
            return None;
        }
    }
    Some(stream.to_compact())
}

pub(crate) fn crc64(mut crc: u64, data: &[u8]) -> u64 {
    const POLY: u64 = 0x95ac_9329_ac4b_c9b5;
    const TABLE: [u64; 256] = {
        let mut table = [0u64; 256];
        let mut i = 0;
        while i < 256 {
            let mut c = i as u64;
            let mut bit = 0;
            while bit < 8 {
                c = if c & 1 != 0 { (c >> 1) ^ POLY } else { c >> 1 };
                bit += 1;
            }
            table[i] = c;
            i += 1;
        }
        table
    };
    for &b in data {
        crc = TABLE[((crc ^ u64::from(b)) & 0xff) as usize] ^ (crc >> 8);
    }
    crc
}

/// Ends a dump payload: the RDB version and the CRC-64 of everything.
pub(crate) fn finish_payload(out: &mut Vec<u8>) {
    out.extend_from_slice(&VERSION.to_le_bytes());
    let crc = crc64(0, out);
    out.extend_from_slice(&crc.to_le_bytes());
}

/// Redis' `verifyDumpPayload`: the body, if the version is one this reads
/// and the checksum matches.
pub(crate) fn payload_body(payload: &[u8]) -> Option<&[u8]> {
    if payload.len() < 10 {
        return None;
    }
    let footer = &payload[payload.len() - 10..];
    if u16::from_le_bytes([footer[0], footer[1]]) > VERSION {
        return None;
    }
    let crc = crc64(0, &payload[..payload.len() - 8]);
    (crc.to_le_bytes() == footer[2..]).then_some(&payload[..payload.len() - 10])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc64_is_redis() {
        // Redis' crc64 test vector.
        assert_eq!(crc64(0, b"123456789"), 0xe9c6_d914_c4b8_d9ca);
    }

    #[test]
    fn strings_come_back() {
        let long =
            b"#!lua name=lib\nredis.register_function('f', function() return 1 end) -- ".repeat(5);
        for s in [
            &b""[..],
            b"0",
            b"-1",
            b"127",
            b"128",
            b"-32768",
            b"40000",
            b"-0",
            b"007",
            b"abc",
            &long,
        ] {
            let mut out = Vec::new();
            put_string(&mut out, s);
            let mut reader = Reader::new(&out);
            assert_eq!(reader.string().as_deref(), Some(s), "{s:?}");
            assert_eq!(reader.remaining(), 0);
        }
        // Integers and repetitive text are encoded.
        let mut out = Vec::new();
        put_string(&mut out, b"127");
        assert_eq!(out, [0xc0, 127]);
        out.clear();
        put_string(&mut out, &long);
        assert_eq!(out[0], 0xc3);
        assert!(out.len() < long.len());
    }

    fn payload(build: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
        let mut out = Vec::new();
        build(&mut out);
        finish_payload(&mut out);
        out
    }

    fn load(payload: &[u8]) -> Option<Object> {
        payload_body(payload)?;
        let mut reader = Reader::new(payload);
        let kind = reader.byte()?;
        load_object(kind, &mut reader, NodeLimits::default())
    }

    fn owned(items: &[&[u8]]) -> Vec<Vec<u8>> {
        items.iter().map(|i| i.to_vec()).collect()
    }

    #[test]
    fn values_come_back() {
        let small: &[&[u8]] = &[b"a", b"1", b"-300", b"", b"9223372036854775807", b"007"];
        let p = payload(|out| dump_list(out, small.iter().copied()));
        assert_eq!(load(&p), Some(Object::List(owned(small))));
        // Long enough for several nodes, and one too big for any.
        let mut many: Vec<Vec<u8>> = (0..3000)
            .map(|i| format!("element:{i}").into_bytes())
            .collect();
        many.insert(1500, vec![b'x'; 10_000]);
        let p = payload(|out| dump_list(out, many.iter().map(Vec::as_slice)));
        assert_eq!(p[0], TYPE_LIST_QUICKLIST_2);
        assert_eq!(load(&p), Some(Object::List(many)));

        for (members, encoding) in [
            (
                &[&b"-3"[..], b"1", b"2", b"70000", b"9999999999"][..],
                SetEncoding::Intset,
            ),
            (&[&b"a"[..], b"b", b"1"][..], SetEncoding::Listpack),
            (&[&b"a"[..], b"b", b"1"][..], SetEncoding::Hashtable),
        ] {
            let p = payload(|out| dump_set(out, members.iter().copied(), members.len(), encoding));
            assert_eq!(load(&p), Some(Object::Set(owned(members))));
        }

        let scores = [
            (-1.5, &b"a"[..]),
            (0.0, b"b"),
            (3.0, b"c"),
            (1e100, b"d"),
            (f64::INFINITY, b"e"),
        ];
        let expected: Vec<(Vec<u8>, f64)> = scores.iter().map(|&(s, m)| (m.to_vec(), s)).collect();
        let p = payload(|out| dump_zset(out, &scores, true));
        assert_eq!(load(&p), Some(Object::ZSet(expected.clone())));
        let p = payload(|out| dump_zset(out, &scores, false));
        let mut reversed = expected;
        reversed.reverse();
        assert_eq!(load(&p), Some(Object::ZSet(reversed)));

        let pairs = [(&b"f"[..], &b"v"[..]), (b"n", b"12"), (b"", b"")];
        let expected: Vec<(Vec<u8>, Vec<u8>)> = pairs
            .iter()
            .map(|&(f, v)| (f.to_vec(), v.to_vec()))
            .collect();
        for listpack in [true, false] {
            let p = payload(|out| dump_hash(out, pairs.iter().copied(), pairs.len(), listpack));
            assert_eq!(load(&p), Some(Object::Hash(expected.clone())));
        }

        let p = payload(|out| dump_string(out, &b"abc".repeat(100)));
        assert_eq!(load(&p), Some(Object::String(b"abc".repeat(100))));
    }

    /// A stream with what it can hold: entries with their master's fields and
    /// without, groups with consumers and pending entries.
    fn sample_stream() -> BigStream {
        let mut stream = BigStream::default();
        let limits = NodeLimits::default();
        for i in 1..=250u64 {
            let value = i.to_string();
            let id = StreamId {
                ms: i * 10,
                seq: i % 3,
            };
            if i % 7 == 0 {
                stream.add(id, &[(b"other", value.as_bytes())], limits);
            } else {
                stream.add(id, &[(b"field", value.as_bytes()), (b"x", b"y")], limits);
            }
        }
        stream.set_ids(
            StreamId { ms: 9999, seq: 5 },
            Some(300),
            StreamId { ms: 5, seq: 0 },
        );
        let mut group = Group::new(StreamId { ms: 50, seq: 2 }, 5);
        let (alice, _) = group.consumer(b"alice", 1000);
        let (bob, _) = group.consumer(b"bob", 2000);
        group.consumer_mut(bob).active_time = 2500;
        group.assign(StreamId { ms: 10, seq: 1 }, alice, 1100, 1);
        group.assign(StreamId { ms: 20, seq: 2 }, bob, 2100, 3);
        stream.groups.insert(b"g".to_vec().into(), group);
        stream.groups.insert(
            b"empty".to_vec().into(),
            Group::new(StreamId::MIN, UNKNOWN_READS),
        );
        stream
    }

    #[test]
    fn streams_come_back() {
        let stream = sample_stream();
        assert!(stream.chunk_count() > 1);
        let p = payload(|out| dump_stream(out, &stream));
        assert_eq!(p[0], TYPE_STREAM_LISTPACKS_3);
        // The same, chunks and all, as it was made with the same limits.
        assert_eq!(load(&p), Some(Object::Stream(stream.to_compact())));
        let empty = BigStream::default();
        let p = payload(|out| dump_stream(out, &empty));
        assert_eq!(load(&p), Some(Object::Stream(empty.to_compact())));
    }

    #[test]
    fn zipmaps_are_read() {
        // f => v, g => w with two bytes free after it.
        let zm = [
            2, 1, b'f', 1, 0, b'v', 1, b'g', 1, 2, b'w', 0xab, 0xab, 0xff,
        ];
        let p = payload(|out| {
            out.push(TYPE_HASH_ZIPMAP);
            put_string(out, &zm);
        });
        let expected = vec![
            (b"f".to_vec(), b"v".to_vec()),
            (b"g".to_vec(), b"w".to_vec()),
        ];
        assert_eq!(load(&p), Some(Object::Hash(expected)));
        // (Repeats are for the caller to refuse.)
        assert_eq!(
            zipmap_pairs(&[2, 1, b'f', 1, 0, b'v', 1, b'f', 1, 0, b'w', 0xff]).map(|p| p.len()),
            Some(2)
        );
        assert_eq!(zipmap_pairs(&[0, 0xff]), None);
        assert_eq!(zipmap_pairs(&[3, 1, b'f', 1, 0, b'v', 0xff]), None);
    }

    /// Damaged payloads of every kind are refused or read, never a panic, and
    /// what's read can be stored: the checks `RESTORE` relies on.
    #[test]
    fn damaged_payloads_are_safe() {
        let stream = sample_stream();
        let mut many: Vec<Vec<u8>> = (0..400).map(|i| format!("e{i}").into_bytes()).collect();
        many.push(b"-12345".to_vec());
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..20)
            .map(|i| (format!("f{i}").into_bytes(), i.to_string().into_bytes()))
            .collect();
        let scores: Vec<(f64, Vec<u8>)> =
            (0..20).map(|i| (i as f64 / 3.0, vec![b'm', i])).collect();
        let scores: Vec<(f64, &[u8])> = scores.iter().map(|(s, m)| (*s, m.as_slice())).collect();
        let samples = [
            payload(|out| dump_string(out, &b"hello world ".repeat(10))),
            payload(|out| dump_list(out, many.iter().map(Vec::as_slice))),
            payload(|out| {
                dump_set(
                    out,
                    [&b"1"[..], b"2", b"300000"].into_iter(),
                    3,
                    SetEncoding::Intset,
                )
            }),
            payload(|out| {
                dump_set(
                    out,
                    many.iter().map(Vec::as_slice),
                    many.len(),
                    SetEncoding::Listpack,
                )
            }),
            payload(|out| {
                dump_set(
                    out,
                    many.iter().map(Vec::as_slice),
                    many.len(),
                    SetEncoding::Hashtable,
                )
            }),
            payload(|out| dump_zset(out, &scores, true)),
            payload(|out| dump_zset(out, &scores, false)),
            payload(|out| {
                dump_hash(
                    out,
                    pairs.iter().map(|(f, v)| (f.as_slice(), v.as_slice())),
                    20,
                    true,
                )
            }),
            payload(|out| {
                dump_hash(
                    out,
                    pairs.iter().map(|(f, v)| (f.as_slice(), v.as_slice())),
                    20,
                    false,
                )
            }),
            payload(|out| dump_stream(out, &stream)),
        ];
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut random = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut read = 0;
        for sample in &samples {
            assert!(load(sample).is_some());
            for _ in 0..3000 {
                let mut damaged = sample[..sample.len() - 10].to_vec();
                for _ in 0..1 + random() % 3 {
                    let at = (random() as usize) % damaged.len();
                    match random() % 4 {
                        0 => damaged[at] = random() as u8,
                        1 => damaged[at] ^= 1 << (random() % 8),
                        2 => damaged.truncate(at.max(1)),
                        _ => damaged.insert(at, random() as u8),
                    }
                }
                finish_payload(&mut damaged);
                if let Some(object) = load(&damaged) {
                    read += 1;
                    if let Object::Stream(compact) = &object {
                        assert!(BigStream::from_compact(compact).is_some(), "{damaged:x?}");
                    }
                }
            }
        }
        // Some damage leaves a payload that's still one (a changed byte of a string).
        assert!(read > 0);
    }

    #[test]
    fn payloads_check_their_footer() {
        let mut payload = vec![OPCODE_FUNCTION2];
        put_string(&mut payload, b"code");
        finish_payload(&mut payload);
        assert_eq!(payload_body(&payload), Some(&payload[..6]));
        let mut damaged = payload.clone();
        damaged[1] ^= 1;
        assert_eq!(payload_body(&damaged), None);
        assert_eq!(payload_body(b"short"), None);
    }
}
