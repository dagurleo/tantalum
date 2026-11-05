//! Append-only segments that pack many entries into one allocation.
//!
//! Entry layout, byte-aligned with no padding:
//!
//! ```text
//! inline:   [flags] [key len] [value len] [deadline]? [key bytes] [value bytes]
//! external: [flags] [deadline]? [external index]
//! ```
//!
//! Lengths, deadlines and indexes are LEB128 varints, so a typical small entry
//! has 3 bytes of overhead. The deadline is present only when `HAS_TTL` is
//! set. External entries keep key and value in their own refcounted
//! allocation (`entry.rs`), referenced by an index into the shard's table of
//! externals; they're for entries too big to pack.
//!
//! After an entry is written, its size never changes. Its flags byte does (the
//! dead bit and usage counter), and so may its deadline and value, but only
//! in place: a new deadline is padded to the old field's width (0 there means
//! the TTL was removed), and a new value must be the same length. Anything
//! else is a new entry, so a segment is effectively an append-only log.

use std::ops::Range;

use crate::pages::SegBuf;
use crate::store::Millis;

pub const DEAD: u8 = 1;
pub const HAS_TTL: u8 = 1 << 1;
pub const EXTERNAL: u8 = 1 << 2;
const USAGE_SHIFT: u32 = 3;
const USAGE_MASK: u8 = 0b11 << USAGE_SHIFT;
/// Usage counters saturate here (2 bits).
pub const MAX_USAGE: u8 = 3;
/// The value's data type, in the top three bits. Strings are 0, so their
/// entries are what they were before there were other types.
const KIND_SHIFT: u32 = 5;
const KIND_MASK: u8 = 0b111 << KIND_SHIFT;

/// What an entry's value holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    String = 0,
    /// Fields and values, encoded by `hash.rs`.
    Hash = 1,
    List = 2,
    /// Packed by `set.rs`, or with `EXTERNAL`, a big set (`collection.rs`).
    Set = 3,
    ZSet = 4,
    /// Always an object (`stream.rs`).
    Stream = 5,
}

impl Kind {
    /// The kind in a flags byte: one table load, as every read asks.
    #[inline]
    pub fn of(flags: u8) -> Kind {
        const KINDS: [Kind; 8] = [
            Kind::String,
            Kind::Hash,
            Kind::List,
            Kind::Set,
            Kind::ZSet,
            Kind::Stream,
            // Never written.
            Kind::String,
            Kind::String,
        ];
        KINDS[usize::from((flags & KIND_MASK) >> KIND_SHIFT)]
    }

    /// The name `TYPE` replies with.
    pub fn name(self) -> &'static str {
        match self {
            Kind::String => "string",
            Kind::Hash => "hash",
            Kind::List => "list",
            Kind::Set => "set",
            Kind::ZSet => "zset",
            Kind::Stream => "stream",
        }
    }

    /// Hashes, lists, sets, sorted sets and streams: when too big to stay
    /// packed (streams always), an `EXTERNAL` entry of these kinds is an object
    /// (`collection.rs`) rather than bytes in an `entry.rs` allocation.
    pub fn is_collection(self) -> bool {
        matches!(
            self,
            Kind::Hash | Kind::List | Kind::Set | Kind::ZSet | Kind::Stream
        )
    }
}

pub fn usage(flags: u8) -> u8 {
    (flags & USAGE_MASK) >> USAGE_SHIFT
}

pub fn with_usage(flags: u8, usage: u8) -> u8 {
    (flags & !USAGE_MASK) | (usage.min(MAX_USAGE) << USAGE_SHIFT)
}

pub fn varint_len(mut n: u64) -> usize {
    let mut len = 1;
    while n >= 0x80 {
        n >>= 7;
        len += 1;
    }
    len
}

/// Where entries are encoded to: a segment, or a `Vec` in tests.
pub trait Sink {
    fn push(&mut self, byte: u8);
    fn extend_from_slice(&mut self, bytes: &[u8]);
}

impl Sink for Vec<u8> {
    fn push(&mut self, byte: u8) {
        Vec::push(self, byte);
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) {
        Vec::extend_from_slice(self, bytes);
    }
}

impl Sink for SegBuf {
    fn push(&mut self, byte: u8) {
        SegBuf::push(self, byte);
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) {
        SegBuf::extend_from_slice(self, bytes);
    }
}

fn put_varint(out: &mut impl Sink, mut n: u64) {
    while n >= 0x80 {
        out.push(n as u8 | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

/// Decodes a varint at `*pos`. Segment contents are written only by us, so
/// malformed input is a bug and panics via the slice bounds check.
fn get_varint(buf: &[u8], pos: &mut usize) -> u64 {
    let mut n = 0u64;
    let mut shift = 0;
    loop {
        let byte = buf[*pos];
        *pos += 1;
        n |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return n;
        }
        shift += 7;
    }
}

/// What an entry stores, for encoding.
pub enum Payload<'a> {
    Inline { key: &'a [u8], value: &'a [u8] },
    External(u32),
}

/// Encoded size of an entry. `deadline` 0 means no TTL.
pub fn encoded_len(payload: &Payload<'_>, deadline: Millis) -> usize {
    let ttl = if deadline == 0 {
        0
    } else {
        varint_len(deadline)
    };
    1 + ttl
        + match payload {
            Payload::Inline { key, value } => {
                varint_len(key.len() as u64)
                    + varint_len(value.len() as u64)
                    + key.len()
                    + value.len()
            }
            Payload::External(index) => varint_len(u64::from(*index)),
        }
}

/// Appends an encoded entry to `out`.
pub fn encode(out: &mut impl Sink, kind: Kind, usage: u8, payload: &Payload<'_>, deadline: Millis) {
    let mut flags = with_usage((kind as u8) << KIND_SHIFT, usage);
    if deadline != 0 {
        flags |= HAS_TTL;
    }
    if matches!(payload, Payload::External(_)) {
        flags |= EXTERNAL;
    }
    out.push(flags);
    match payload {
        Payload::Inline { key, value } => {
            put_varint(out, key.len() as u64);
            put_varint(out, value.len() as u64);
            if deadline != 0 {
                put_varint(out, deadline);
            }
            out.extend_from_slice(key);
            out.extend_from_slice(value);
        }
        Payload::External(index) => {
            if deadline != 0 {
                put_varint(out, deadline);
            }
            put_varint(out, u64::from(*index));
        }
    }
}

/// A decoded entry header. Ranges are relative to the segment buffer.
#[derive(Debug, Clone)]
pub struct Header {
    pub flags: u8,
    /// 0 when the entry has no TTL.
    pub deadline: Millis,
    pub body: Body,
    /// Total encoded size of the entry.
    pub len: usize,
}

#[derive(Debug, Clone)]
pub enum Body {
    Inline {
        key: Range<usize>,
        value: Range<usize>,
    },
    External(u32),
}

impl Header {
    pub fn is_dead(&self) -> bool {
        self.flags & DEAD != 0
    }

    pub fn has_ttl(&self) -> bool {
        self.deadline != 0
    }

    pub fn is_expired(&self, now: Millis) -> bool {
        self.deadline != 0 && self.deadline <= now
    }

    pub fn usage(&self) -> u8 {
        usage(self.flags)
    }

    pub fn kind(&self) -> Kind {
        Kind::of(self.flags)
    }
}

/// Decodes the entry starting at `start`.
pub fn parse(buf: &[u8], start: usize) -> Header {
    let flags = buf[start];
    let mut pos = start + 1;
    let ttl = flags & HAS_TTL != 0;
    if flags & EXTERNAL != 0 {
        let deadline = if ttl { get_varint(buf, &mut pos) } else { 0 };
        let index = get_varint(buf, &mut pos) as u32;
        Header {
            flags,
            deadline,
            body: Body::External(index),
            len: pos - start,
        }
    } else {
        let key_len = get_varint(buf, &mut pos) as usize;
        let value_len = get_varint(buf, &mut pos) as usize;
        let deadline = if ttl { get_varint(buf, &mut pos) } else { 0 };
        let key = pos..pos + key_len;
        let value = key.end..key.end + value_len;
        let len = value.end - start;
        Header {
            flags,
            deadline,
            body: Body::Inline { key, value },
            len,
        }
    }
}

/// Where the entry at `start` keeps its deadline, if it has the field.
pub fn deadline_field(buf: &[u8], start: usize) -> Option<Range<usize>> {
    let flags = buf[start];
    if flags & HAS_TTL == 0 {
        return None;
    }
    let mut pos = start + 1;
    if flags & EXTERNAL == 0 {
        get_varint(buf, &mut pos); // key length
        get_varint(buf, &mut pos); // value length
    }
    let from = pos;
    get_varint(buf, &mut pos);
    Some(from..pos)
}

/// Overwrites a deadline field with `deadline` (0 for none), padded to the
/// field's width: LEB128 allows extra continuation bytes. Returns false,
/// changing nothing, if it needs more bytes than the field has.
pub fn write_deadline(field: &mut [u8], deadline: Millis) -> bool {
    if varint_len(deadline) > field.len() {
        return false;
    }
    let (last, rest) = field.split_last_mut().expect("a varint has a byte");
    let mut n = deadline;
    for byte in rest {
        *byte = n as u8 | 0x80;
        n >>= 7;
    }
    *last = n as u8;
    true
}

/// A fixed-capacity buffer of entries, appended to until full.
pub struct Segment {
    /// Fixed capacity, so offsets stay valid.
    pub buf: SegBuf,
    /// Bytes of entries that are still live (not marked dead).
    pub live: usize,
    /// Live entries with a TTL.
    pub ttl_live: usize,
    /// No later than the earliest deadline among live entries; 0 if none.
    pub min_deadline: Millis,
}

impl Segment {
    pub fn new(capacity: usize) -> Self {
        Segment {
            buf: SegBuf::with_capacity(capacity),
            live: 0,
            ttl_live: 0,
            min_deadline: 0,
        }
    }

    pub fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    pub fn room(&self) -> usize {
        self.buf.room()
    }

    /// All entries, live and dead, with their offsets.
    pub fn entries(&self) -> impl Iterator<Item = (usize, Header)> + '_ {
        let mut offset = 0;
        std::iter::from_fn(move || {
            if offset >= self.buf.len() {
                return None;
            }
            let header = parse(&self.buf, offset);
            let at = offset;
            offset += header.len;
            Some((at, header))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints() {
        for n in [
            0,
            1,
            127,
            128,
            300,
            16_383,
            16_384,
            u32::MAX as u64,
            u64::MAX,
        ] {
            let mut buf = Vec::new();
            put_varint(&mut buf, n);
            assert_eq!(buf.len(), varint_len(n), "{n}");
            let mut pos = 0;
            assert_eq!(get_varint(&buf, &mut pos), n);
            assert_eq!(pos, buf.len());
        }
    }

    #[test]
    fn entries_roundtrip() {
        let mut seg = Segment::new(1024);
        let cases: [(Payload<'_>, Millis, u8); 4] = [
            (
                Payload::Inline {
                    key: b"key",
                    value: b"value",
                },
                0,
                0,
            ),
            (
                Payload::Inline {
                    key: b"",
                    value: b"",
                },
                12_345_678,
                3,
            ),
            (Payload::External(7), 0, 1),
            (Payload::External(300), 99, 2),
        ];
        let mut expected_offsets = Vec::new();
        for (payload, deadline, usage) in &cases {
            expected_offsets.push(seg.buf.len());
            let before = seg.buf.len();
            encode(&mut seg.buf, Kind::String, *usage, payload, *deadline);
            assert_eq!(seg.buf.len() - before, encoded_len(payload, *deadline));
        }
        let parsed: Vec<_> = seg.entries().collect();
        assert_eq!(parsed.len(), cases.len());
        for ((offset, header), ((payload, deadline, usage), expected)) in
            parsed.iter().zip(cases.iter().zip(&expected_offsets))
        {
            assert_eq!(offset, expected);
            assert_eq!(header.deadline, *deadline);
            assert_eq!(header.usage(), *usage);
            assert!(!header.is_dead());
            match (payload, &header.body) {
                (Payload::Inline { key, value }, Body::Inline { key: k, value: v }) => {
                    assert_eq!(&seg.buf[k.clone()], *key);
                    assert_eq!(&seg.buf[v.clone()], *value);
                }
                (Payload::External(i), Body::External(j)) => assert_eq!(i, j),
                _ => panic!("kind mismatch"),
            }
        }
    }

    #[test]
    fn small_entries_are_compact() {
        let payload = Payload::Inline {
            key: &[b'k'; 15],
            value: &[b'v'; 32],
        };
        assert_eq!(encoded_len(&payload, 0), 50);
        // A TTL costs the deadline's varint: 5 bytes for ~a year of uptime.
        assert_eq!(encoded_len(&payload, 30_000_000_000), 55);
    }

    #[test]
    fn deadlines_rewrite_in_place() {
        for (old, new, fits) in [
            (1_000_000, 5, true),
            (1_000_000, 2_000_000, true),
            (1_000_000, 0, true),
            (100, 1_000_000, false),
            (u64::MAX, u64::MAX - 1, true),
        ] {
            for payload in [
                Payload::Inline {
                    key: b"key",
                    value: b"value",
                },
                Payload::External(300),
            ] {
                let mut buf = Vec::new();
                encode(&mut buf, Kind::String, 2, &payload, old);
                let before = parse(&buf, 0);
                let field = deadline_field(&buf, 0).unwrap();
                assert_eq!(field.len(), varint_len(old));
                assert_eq!(write_deadline(&mut buf[field], new), fits);
                let after = parse(&buf, 0);
                assert_eq!(after.deadline, if fits { new } else { old });
                assert_eq!(after.len, before.len);
                assert_eq!(after.usage(), 2);
                assert_eq!(after.has_ttl(), !fits || new != 0);
                match (after.body, before.body) {
                    (Body::Inline { key, value }, Body::Inline { key: k, value: v }) => {
                        assert_eq!((key, value), (k, v));
                    }
                    (Body::External(i), Body::External(j)) => assert_eq!(i, j),
                    _ => panic!("kind changed"),
                }
            }
        }
        let mut buf = Vec::new();
        encode(&mut buf, Kind::String, 0, &Payload::External(1), 0);
        assert_eq!(deadline_field(&buf, 0), None);
    }

    #[test]
    fn kinds_survive_usage_updates() {
        let mut buf = Vec::new();
        encode(
            &mut buf,
            Kind::Hash,
            1,
            &Payload::Inline {
                key: b"k",
                value: b"v",
            },
            0,
        );
        assert_eq!(parse(&buf, 0).kind(), Kind::Hash);
        buf[0] = with_usage(buf[0], 3);
        assert_eq!(parse(&buf, 0).kind(), Kind::Hash);
        assert_eq!(parse(&buf, 0).usage(), 3);
        // A string's flags byte is unchanged by kinds existing.
        let mut plain = Vec::new();
        encode(
            &mut plain,
            Kind::String,
            2,
            &Payload::Inline {
                key: b"k",
                value: b"v",
            },
            0,
        );
        assert_eq!(plain[0], with_usage(0, 2));
    }

    #[test]
    fn flags_updates() {
        let flags = with_usage(HAS_TTL, 2);
        assert_eq!(usage(flags), 2);
        assert_eq!(flags & HAS_TTL, HAS_TTL);
        assert_eq!(usage(with_usage(flags, 9)), MAX_USAGE);
        assert_eq!(usage(flags | DEAD), 2);
    }
}
