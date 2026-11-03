//! Streams: entries of fields and values under increasing IDs (`XADD`,
//! `XRANGE`, `XREAD` ...), always kept as an object by the shard.
//!
//! Entries live in chunks, as Redis keeps them in listpack nodes, and a new
//! chunk starts exactly where Redis would start a new node: after 100
//! entries, or when its listpack would pass 4 KB (the defaults of
//! `stream-node-max-entries` and `stream-node-max-bytes`). Each chunk
//! counts the bytes Redis' listpack would take, by Redis' encoding rules, so
//! trimming with `~`, which removes whole nodes, removes what Redis does.
//! The entries themselves are stored more compactly: varint IDs relative to
//! the chunk's first, varint lengths, and field names left out when they
//! are the chunk's first entry's (as Redis' `SAMEFIELDS` does).
//!
//! Like Redis, a chunk counts entries removed from it (`XDEL`, trimming
//! inside it) towards its 100, and goes when its last entry does (by
//! `XDEL`). A stream stays when it's emptied, keeping its last ID.

use std::collections::VecDeque;

/// Redis' defaults for `stream-node-max-entries` and `-max-bytes`.
const NODE_MAX_ENTRIES: usize = 100;
const NODE_MAX_BYTES: usize = 4096;
/// A whole entry in a chunk takes its fields from the chunk's first.
const SAME_FIELDS: u8 = 1;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const MIN: StreamId = StreamId { ms: 0, seq: 0 };
    pub const MAX: StreamId = StreamId {
        ms: u64::MAX,
        seq: u64::MAX,
    };

    /// The next ID, if there is one.
    pub fn next(self) -> Option<StreamId> {
        match self {
            StreamId::MAX => None,
            StreamId { ms, seq: u64::MAX } => Some(StreamId { ms: ms + 1, seq: 0 }),
            StreamId { ms, seq } => Some(StreamId { ms, seq: seq + 1 }),
        }
    }

    /// The previous ID, if there is one.
    pub fn previous(self) -> Option<StreamId> {
        match self {
            StreamId::MIN => None,
            StreamId { ms, seq: 0 } => Some(StreamId {
                ms: ms - 1,
                seq: u64::MAX,
            }),
            StreamId { ms, seq } => Some(StreamId { ms, seq: seq - 1 }),
        }
    }

    /// As Redis writes it: `ms-seq`.
    pub fn write(self, out: &mut Vec<u8>) {
        let mut buf = itoa::Buffer::new();
        out.extend_from_slice(buf.format(self.ms).as_bytes());
        out.push(b'-');
        out.extend_from_slice(buf.format(self.seq).as_bytes());
    }
}

/// Redis' `string2ll`: an integer with no sign but `-`, no leading zeros
/// or spaces, and nothing after it.
fn strict_i64(s: &[u8]) -> Option<i64> {
    if s.is_empty() || s.len() >= 21 {
        return None;
    }
    if s == b"0" {
        return Some(0);
    }
    let (negative, digits) = match s.split_first() {
        Some((b'-', rest)) if !rest.is_empty() => (true, rest),
        _ => (false, s),
    };
    if !matches!(digits[0], b'1'..=b'9') || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let mut v: u64 = 0;
    for &d in digits {
        v = v.checked_mul(10)?.checked_add(u64::from(d - b'0'))?;
    }
    if negative {
        (v <= 1 << 63).then(|| (v as i64).wrapping_neg())
    } else {
        i64::try_from(v).ok()
    }
}

/// Redis' `string2ull`: `string2ll` if that works (and isn't negative),
/// otherwise C's `strtoull` over the whole string, which also takes leading
/// space, a sign (`-` wraps around) and leading zeros.
fn string2ull(s: &[u8]) -> Option<u64> {
    if let Some(v) = strict_i64(s) {
        return u64::try_from(v).ok();
    }
    let mut i = 0;
    while i < s.len() && matches!(s[i], b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r') {
        i += 1;
    }
    let negative = s.get(i) == Some(&b'-');
    if matches!(s.get(i), Some(b'-' | b'+')) {
        i += 1;
    }
    let digits = &s[i..];
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let mut v: u64 = 0;
    for &d in digits {
        v = v.checked_mul(10)?.checked_add(u64::from(d - b'0'))?;
    }
    Some(if negative { v.wrapping_neg() } else { v })
}

/// How an ID argument is read.
#[derive(Clone, Copy)]
pub enum IdForm {
    /// `-` and `+` mean the smallest and largest IDs.
    Interval,
    /// No `-` or `+`.
    Strict,
}

/// An ID argument, as Redis' `streamGenericParseIDOrReply` reads it: `ms`
/// (with `missing_seq` as its sequence) or `ms-seq`. With `auto_seq`, `ms-*`
/// is taken too, returning `None` as the sequence.
pub fn parse_id(
    arg: &[u8],
    missing_seq: u64,
    form: IdForm,
    auto_seq: bool,
) -> Option<(u64, Option<u64>)> {
    // Redis reads it as a C string: up to a NUL byte.
    let arg = &arg[..arg.iter().position(|&b| b == 0).unwrap_or(arg.len())];
    if arg.len() > 127 {
        return None;
    }
    match (arg, form) {
        (b"-" | b"+", IdForm::Strict) => return None,
        (b"-", IdForm::Interval) => return Some((0, Some(0))),
        (b"+", IdForm::Interval) => return Some((u64::MAX, Some(u64::MAX))),
        _ => {}
    }
    match arg.iter().position(|&b| b == b'-') {
        None => Some((string2ull(arg)?, Some(missing_seq))),
        Some(dash) => {
            let ms = string2ull(&arg[..dash])?;
            let seq = &arg[dash + 1..];
            if auto_seq && seq == b"*" {
                Some((ms, None))
            } else {
                Some((ms, Some(string2ull(seq)?)))
            }
        }
    }
}

/// A whole ID argument (not `ms-*`).
pub fn parse_whole_id(arg: &[u8], missing_seq: u64, form: IdForm) -> Option<StreamId> {
    let (ms, seq) = parse_id(arg, missing_seq, form, false)?;
    Some(StreamId {
        ms,
        seq: seq.expect("not auto"),
    })
}

/// Bytes Redis' listpack takes for an element of `enclen` bytes: those,
/// and its length written backwards.
fn lp_entry(enclen: usize) -> usize {
    enclen
        + match enclen {
            0..=127 => 1,
            128..=16382 => 2,
            16383..=2097150 => 3,
            2097151..=268435454 => 4,
            _ => 5,
        }
}

fn lp_int(v: i64) -> usize {
    lp_entry(match v {
        0..=127 => 1,
        -4096..=4095 => 2,
        -32768..=32767 => 3,
        -8388608..=8388607 => 4,
        -2147483648..=2147483647 => 5,
        _ => 9,
    })
}

/// A string element: as an integer if it reads as one.
fn lp_str(s: &[u8]) -> usize {
    if let Some(v) = strict_i64(s) {
        return lp_int(v);
    }
    let len = s.len();
    lp_entry(match len {
        0..=63 => 1 + len,
        64..=4095 => 2 + len,
        _ => 5 + len,
    })
}

fn put_varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        out.push(n as u8 | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

fn get_varint(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let mut n = 0u64;
    let mut shift = 0;
    loop {
        let b = *buf.get(*pos)?;
        *pos += 1;
        if shift > 63 {
            return None;
        }
        n |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Some(n);
        }
        shift += 7;
    }
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn get_bytes<'a>(buf: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
    let len = usize::try_from(get_varint(buf, pos)?).ok()?;
    let bytes = buf.get(*pos..pos.checked_add(len)?)?;
    *pos += len;
    Some(bytes)
}

/// Entries that Redis would keep in one listpack node.
#[derive(Clone, Default)]
struct Chunk {
    /// The first ID ever added to it (Redis' master entry ID), and the last.
    first: StreamId,
    last: StreamId,
    /// Its first entry's field names: count, then each with its length.
    fields: Box<[u8]>,
    /// Its entries: flags, ms and seq after `first`'s, then (unless
    /// `SAME_FIELDS`) the field count and names, and the values.
    data: Vec<u8>,
    live: u32,
    /// Removed from it since: they still count towards a node's entries.
    deleted: u32,
    /// What Redis' listpack for it would take.
    lp_bytes: usize,
}

/// One entry, as stored: its fields and values to walk.
pub struct Entry<'a> {
    pub id: StreamId,
    /// Field names, from the chunk (`SAME_FIELDS`) or the entry itself.
    names: &'a [u8],
    values: &'a [u8],
    pub count: usize,
}

impl<'a> Entry<'a> {
    /// Its field-value pairs, in order.
    pub fn pairs(&self) -> impl Iterator<Item = (&'a [u8], &'a [u8])> + 'a {
        let (names, values) = (self.names, self.values);
        let (mut n, mut v) = (0, 0);
        (0..self.count).map(move |_| {
            let name = get_bytes(names, &mut n).expect("valid chunk");
            let value = get_bytes(values, &mut v).expect("valid chunk");
            (name, value)
        })
    }
}

impl Chunk {
    /// Decodes the entry at `pos`, returning it and where the next starts.
    fn entry_at(&self, mut pos: usize) -> Option<(Entry<'_>, usize)> {
        let data = &self.data;
        let flags = *data.get(pos)?;
        pos += 1;
        let ms = self.first.ms.wrapping_add(get_varint(data, &mut pos)?);
        let seq = get_varint(data, &mut pos)?;
        let (count, names) = if flags & SAME_FIELDS != 0 {
            let mut p = 0;
            let count = get_varint(&self.fields, &mut p)?;
            (count, &self.fields[p..])
        } else {
            let count = get_varint(data, &mut pos)?;
            let start = pos;
            for _ in 0..count {
                get_bytes(data, &mut pos)?;
            }
            (count, &data[start..pos])
        };
        let count = usize::try_from(count).ok()?;
        let start = pos;
        for _ in 0..count {
            get_bytes(data, &mut pos)?;
        }
        let entry = Entry {
            id: StreamId { ms, seq },
            names,
            values: &data[start..pos],
            count,
        };
        Some((entry, pos))
    }

    /// Its entries, with where each starts and ends.
    fn entries(&self) -> impl Iterator<Item = (usize, usize, Entry<'_>)> + '_ {
        let mut pos = 0;
        std::iter::from_fn(move || {
            if pos >= self.data.len() {
                return None;
            }
            let start = pos;
            let (entry, next) = self.entry_at(pos).expect("valid chunk");
            pos = next;
            Some((start, next, entry))
        })
    }

    fn same_fields(&self, pairs: &[(&[u8], &[u8])]) -> bool {
        let mut p = 0;
        let Some(count) = get_varint(&self.fields, &mut p) else {
            return false;
        };
        count == pairs.len() as u64
            && pairs
                .iter()
                .all(|(field, _)| get_bytes(&self.fields, &mut p) == Some(field))
    }

    fn allocation_size(&self) -> usize {
        self.data.capacity() + self.fields.len()
    }
}

/// A stream.
#[derive(Clone, Default)]
pub struct BigStream {
    chunks: VecDeque<Chunk>,
    len: usize,
    last_id: StreamId,
    /// Ever added, and the largest removed by `XDEL` (for `XINFO`, later).
    entries_added: u64,
    max_deleted: StreamId,
    /// The chunks' allocations, kept as they change.
    bytes: usize,
}

/// Why `XADD` didn't add.
#[derive(Debug, PartialEq, Eq)]
pub enum AddError {
    /// Equal to or below the last ID.
    TooSmall,
    /// The last ID is the largest possible.
    Exhausted,
}

/// Which entries trimming keeps.
#[derive(Clone, Copy, Debug)]
pub enum Trim {
    /// The newest this many.
    MaxLen(usize),
    /// Those from this ID on.
    MinId(StreamId),
}

impl BigStream {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn last_id(&self) -> StreamId {
        self.last_id
    }

    pub fn allocation_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.chunks.capacity() * std::mem::size_of::<Chunk>()
            + self.bytes
    }

    /// The ID `XADD` would use: given (`ms` and maybe `seq`), or with `id`
    /// `None`, made from `now_ms`.
    pub fn next_id(
        &self,
        id: Option<(u64, Option<u64>)>,
        now_ms: u64,
    ) -> Result<StreamId, AddError> {
        if self.last_id == StreamId::MAX {
            return Err(AddError::Exhausted);
        }
        let id = match id {
            Some((ms, Some(seq))) => StreamId { ms, seq },
            Some((ms, None)) if ms == self.last_id.ms => {
                if self.last_id.seq == u64::MAX {
                    return Err(AddError::TooSmall);
                }
                StreamId {
                    ms,
                    seq: self.last_id.seq + 1,
                }
            }
            Some((ms, None)) => StreamId { ms, seq: 0 },
            None if now_ms > self.last_id.ms => StreamId { ms: now_ms, seq: 0 },
            None => self.last_id.next().expect("not exhausted"),
        };
        if id <= self.last_id {
            return Err(AddError::TooSmall);
        }
        Ok(id)
    }

    /// Adds an entry under `id` (from [`BigStream::next_id`]).
    pub fn add(&mut self, id: StreamId, pairs: &[(&[u8], &[u8])]) {
        let raw: usize = pairs.iter().map(|(f, v)| f.len() + v.len()).sum();
        let starts_chunk = match self.chunks.back() {
            None => true,
            Some(chunk) => {
                chunk.lp_bytes + raw >= NODE_MAX_BYTES
                    || (chunk.live + chunk.deleted) as usize >= NODE_MAX_ENTRIES
            }
        };
        if starts_chunk {
            // The last chunk is done growing: give back its spare capacity.
            if let Some(last) = self.chunks.back_mut() {
                let before = last.allocation_size();
                last.data.shrink_to_fit();
                self.bytes = self.bytes + last.allocation_size() - before;
            }
            let mut fields = Vec::new();
            put_varint(&mut fields, pairs.len() as u64);
            for (field, _) in pairs {
                put_bytes(&mut fields, field);
            }
            // An empty listpack, then the master entry: counts of entries
            // and deleted ones, the fields, and a terminator.
            let lp_bytes = 7
                + lp_int(1)
                + lp_int(0)
                + lp_int(pairs.len() as i64)
                + pairs.iter().map(|(f, _)| lp_str(f)).sum::<usize>()
                + lp_int(0);
            self.chunks.push_back(Chunk {
                first: id,
                last: id,
                fields: fields.into(),
                lp_bytes,
                ..Chunk::default()
            });
        }
        let chunk = self.chunks.back_mut().expect("a chunk");
        let same = chunk.same_fields(pairs);
        // (A new chunk's field names weren't counted yet.)
        let before = if starts_chunk {
            0
        } else {
            chunk.allocation_size()
        };
        let ms_diff = id.ms.wrapping_sub(chunk.first.ms);
        let seq_diff = id.seq.wrapping_sub(chunk.first.seq);
        chunk.data.push(if same { SAME_FIELDS } else { 0 });
        put_varint(&mut chunk.data, ms_diff);
        put_varint(&mut chunk.data, id.seq);
        if !same {
            put_varint(&mut chunk.data, pairs.len() as u64);
            for (field, _) in pairs {
                put_bytes(&mut chunk.data, field);
            }
        }
        for (_, value) in pairs {
            put_bytes(&mut chunk.data, value);
        }
        // Redis' entry: flags, ms and seq diffs, (fields,) values, and a
        // count of its elements.
        let mut lp = lp_int(2) + lp_int(ms_diff as i64) + lp_int(seq_diff as i64);
        let mut elements = pairs.len() + 3;
        if !same {
            lp += lp_int(pairs.len() as i64) + pairs.iter().map(|(f, _)| lp_str(f)).sum::<usize>();
            elements += pairs.len() + 1;
        }
        lp += pairs.iter().map(|(_, v)| lp_str(v)).sum::<usize>() + lp_int(elements as i64);
        chunk.lp_bytes += lp;
        chunk.live += 1;
        chunk.last = id;
        self.bytes = self.bytes + chunk.allocation_size() - before;
        self.len += 1;
        self.last_id = id;
        self.entries_added += 1;
    }

    /// Calls `f` with the entries from `start` to `end`, both included,
    /// oldest first (or with `rev`, newest first), until it returns false.
    pub fn range(
        &self,
        start: StreamId,
        end: StreamId,
        rev: bool,
        mut f: impl FnMut(&Entry<'_>) -> bool,
    ) {
        if start > end {
            return;
        }
        if !rev {
            let from = self.chunks.partition_point(|c| c.last < start);
            for chunk in self.chunks.range(from..) {
                for (_, _, entry) in chunk.entries() {
                    if entry.id < start {
                        continue;
                    }
                    if entry.id > end || !f(&entry) {
                        return;
                    }
                }
            }
        } else {
            let to = self.chunks.partition_point(|c| c.first <= end);
            for chunk in self.chunks.range(..to).rev() {
                let entries: Vec<_> = chunk.entries().map(|(_, _, e)| e).collect();
                for entry in entries.iter().rev() {
                    if entry.id > end {
                        continue;
                    }
                    if entry.id < start || !f(entry) {
                        return;
                    }
                }
            }
        }
    }

    /// Removes the entry `id`; whether there was one.
    pub fn delete(&mut self, id: StreamId) -> bool {
        let at = self.chunks.partition_point(|c| c.last < id);
        let Some(chunk) = self.chunks.get_mut(at) else {
            return false;
        };
        let Some((start, end)) = chunk
            .entries()
            .find(|(_, _, e)| e.id == id)
            .map(|(start, end, _)| (start, end))
        else {
            return false;
        };
        self.len -= 1;
        self.max_deleted = self.max_deleted.max(id);
        let before = chunk.allocation_size();
        if chunk.live == 1 {
            // Its last entry: the node goes, as in Redis.
            self.bytes -= before;
            self.chunks.remove(at);
            return true;
        }
        chunk.data.drain(start..end);
        chunk.live -= 1;
        chunk.deleted += 1;
        self.bytes = self.bytes + chunk.allocation_size() - before;
        true
    }

    /// Trims as Redis' `streamTrim` does: whole chunks from the oldest while
    /// they can all go and `limit` allows (0: no limit), then (unless
    /// `approx`) entries from within the next. Returns how many went.
    pub fn trim(&mut self, how: Trim, approx: bool, limit: usize) -> usize {
        let mut removed = 0;
        while let Some(chunk) = self.chunks.front() {
            let entries = chunk.live as usize;
            if limit != 0 && removed + entries > limit {
                break;
            }
            let whole = match how {
                Trim::MaxLen(maxlen) => self.len - entries >= maxlen,
                Trim::MinId(min) => chunk.last < min,
            };
            if whole {
                self.bytes -= chunk.allocation_size();
                self.chunks.pop_front();
                self.len -= entries;
                removed += entries;
                continue;
            }
            if approx {
                break;
            }
            let len = self.len;
            let chunk = self.chunks.front_mut().expect("a chunk");
            let mut cut = 0;
            let mut dropped = 0;
            for (_, end, entry) in chunk.entries() {
                let goes = match how {
                    Trim::MaxLen(maxlen) => len - dropped > maxlen,
                    Trim::MinId(min) => entry.id < min,
                };
                if !goes {
                    break;
                }
                cut = end;
                dropped += 1;
            }
            let before = chunk.allocation_size();
            chunk.data.drain(..cut);
            chunk.live -= dropped as u32;
            chunk.deleted += dropped as u32;
            self.bytes = self.bytes + chunk.allocation_size() - before;
            self.len -= dropped;
            removed += dropped;
            break;
        }
        removed
    }

    /// Its contents, for snapshots: everything, chunk layout included, so
    /// that it trims the same after a restart.
    pub fn to_compact(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.bytes + 64);
        for n in [
            self.last_id.ms,
            self.last_id.seq,
            self.entries_added,
            self.max_deleted.ms,
            self.max_deleted.seq,
            self.chunks.len() as u64,
        ] {
            put_varint(&mut out, n);
        }
        for chunk in &self.chunks {
            for n in [
                chunk.first.ms,
                chunk.first.seq,
                chunk.last.ms,
                chunk.last.seq,
                u64::from(chunk.live),
                u64::from(chunk.deleted),
                chunk.lp_bytes as u64,
            ] {
                put_varint(&mut out, n);
            }
            put_bytes(&mut out, &chunk.fields);
            put_bytes(&mut out, &chunk.data);
        }
        out
    }

    /// A stream from [`BigStream::to_compact`]'s bytes; `None` if they
    /// aren't a valid stream.
    pub fn from_compact(bytes: &[u8]) -> Option<Self> {
        let mut pos = 0;
        let mut next = || get_varint(bytes, &mut pos);
        let last_id = StreamId {
            ms: next()?,
            seq: next()?,
        };
        let entries_added = next()?;
        let max_deleted = StreamId {
            ms: next()?,
            seq: next()?,
        };
        let count = next()?;
        let mut stream = BigStream {
            last_id,
            entries_added,
            max_deleted,
            ..BigStream::default()
        };
        let mut previous = None;
        for _ in 0..count {
            let mut n = || get_varint(bytes, &mut pos);
            let first = StreamId {
                ms: n()?,
                seq: n()?,
            };
            let last = StreamId {
                ms: n()?,
                seq: n()?,
            };
            let live = u32::try_from(n()?).ok()?;
            let deleted = u32::try_from(n()?).ok()?;
            let lp_bytes = usize::try_from(n()?).ok()?;
            let fields: Box<[u8]> = get_bytes(bytes, &mut pos)?.into();
            let data = get_bytes(bytes, &mut pos)?.to_vec();
            let chunk = Chunk {
                first,
                last,
                fields,
                data,
                live,
                deleted,
                lp_bytes,
            };
            // Check it decodes, in order, within the stream's IDs.
            let mut seen = 0;
            let mut at = 0;
            while at < chunk.data.len() {
                let (entry, end) = chunk.entry_at(at)?;
                if previous.is_some_and(|p| entry.id <= p) || entry.id < first || entry.id > last {
                    return None;
                }
                previous = Some(entry.id);
                seen += 1;
                at = end;
            }
            if seen != live || first > last || last > last_id {
                return None;
            }
            stream.len += live as usize;
            stream.bytes += chunk.allocation_size();
            stream.chunks.push_back(chunk);
        }
        (pos == bytes.len()).then_some(stream)
    }
}

#[cfg(test)]
impl BigStream {
    /// Recounts everything kept up as it changes.
    pub fn check(&self) {
        let bytes: usize = self.chunks.iter().map(Chunk::allocation_size).sum();
        assert_eq!(bytes, self.bytes, "stream bytes");
        let live: usize = self.chunks.iter().map(|c| c.live as usize).sum();
        assert_eq!(live, self.len, "stream length");
        let mut previous = None;
        for chunk in &self.chunks {
            assert_eq!(chunk.entries().count(), chunk.live as usize, "chunk count");
            assert!(chunk.first <= chunk.last && chunk.last <= self.last_id);
            assert!((chunk.live + chunk.deleted) as usize <= NODE_MAX_ENTRIES);
            for (_, _, entry) in chunk.entries() {
                assert!(previous.is_none_or(|p| entry.id > p), "ids in order");
                assert!(entry.id >= chunk.first && entry.id <= chunk.last);
                previous = Some(entry.id);
            }
        }
    }
}

pub fn is_valid(bytes: &[u8]) -> bool {
    BigStream::from_compact(bytes).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(ms: u64, seq: u64) -> StreamId {
        StreamId { ms, seq }
    }

    #[test]
    fn ids_parse_as_redis_reads_them() {
        let p = |s: &str| parse_id(s.as_bytes(), 0, IdForm::Strict, true);
        assert_eq!(p("1-1"), Some((1, Some(1))));
        assert_eq!(p("2"), Some((2, Some(0))));
        assert_eq!(p("2-*"), Some((2, None)));
        assert_eq!(p("01-1"), Some((1, Some(1))));
        assert_eq!(p("+1"), Some((1, Some(0))));
        assert_eq!(p(" 1"), Some((1, Some(0))));
        assert_eq!(
            p("18446744073709551615-18446744073709551615"),
            Some((u64::MAX, Some(u64::MAX)))
        );
        for bad in [
            "abc",
            "1-x",
            "-1",
            "3-",
            "-3",
            "18446744073709551616",
            "-",
            "+",
            "",
            "1 ",
        ] {
            assert_eq!(p(bad), None, "{bad:?}");
        }
        let interval = |s: &str| parse_whole_id(s.as_bytes(), u64::MAX, IdForm::Interval);
        assert_eq!(interval("-"), Some(StreamId::MIN));
        assert_eq!(interval("+"), Some(StreamId::MAX));
        assert_eq!(interval("5"), Some(id(5, u64::MAX)));
    }

    #[test]
    fn ids_go_up() {
        let mut s = BigStream::default();
        assert_eq!(s.next_id(Some((0, Some(0))), 0), Err(AddError::TooSmall));
        let first = s.next_id(None, 100).unwrap();
        assert_eq!(first, id(100, 0));
        s.add(first, &[(b"f", b"v")]);
        assert_eq!(s.next_id(None, 50), Ok(id(100, 1)));
        assert_eq!(s.next_id(Some((100, None)), 0), Ok(id(100, 1)));
        assert_eq!(s.next_id(Some((101, None)), 0), Ok(id(101, 0)));
        assert_eq!(s.next_id(Some((99, None)), 0), Err(AddError::TooSmall));
        assert_eq!(s.next_id(Some((100, Some(0))), 0), Err(AddError::TooSmall));
        s.add(StreamId::MAX, &[(b"f", b"v")]);
        assert_eq!(s.next_id(Some((1, Some(1))), 0), Err(AddError::Exhausted));
    }

    /// Nodes end where Redis 7.2.4's do: `XTRIM MAXLEN ~ 1` after adding
    /// entries of one field with values of these lengths removed what it
    /// removed there.
    #[test]
    fn chunks_end_where_redis_nodes_do() {
        for (n, value_len, redis_removed) in [
            (250, 1, 200),
            (250, 30, 200),
            (250, 100, 216),
            (150, 500, 147),
        ] {
            let mut s = BigStream::default();
            let value = vec![b'x'; value_len];
            for i in 0..n {
                s.add(id(i + 1, 1), &[(b"f", &value)]);
            }
            assert_eq!(
                s.trim(Trim::MaxLen(1), true, 10_000),
                redis_removed,
                "{n} x {value_len}"
            );
        }
    }

    #[test]
    fn ranges_deletes_and_trims() {
        let mut s = BigStream::default();
        for i in 1..=250u64 {
            let v = i.to_string();
            if i % 7 == 0 {
                s.add(id(i, 0), &[(b"other", v.as_bytes()), (b"x", b"y")]);
            } else {
                s.add(id(i, 0), &[(b"f", v.as_bytes())]);
            }
        }
        let ids = |s: &BigStream, a, b, rev| {
            let mut out = Vec::new();
            s.range(a, b, rev, |e| {
                out.push(e.id.ms);
                true
            });
            out
        };
        assert_eq!(
            ids(&s, id(5, 0), id(9, u64::MAX), false),
            vec![5, 6, 7, 8, 9]
        );
        assert_eq!(
            ids(&s, id(5, 0), id(9, u64::MAX), true),
            vec![9, 8, 7, 6, 5]
        );
        let mut seven = Vec::new();
        s.range(id(7, 0), id(7, 0), false, |e| {
            seven.extend(e.pairs().map(|(f, v)| (f.to_vec(), v.to_vec())));
            true
        });
        assert_eq!(
            seven,
            vec![
                (b"other".to_vec(), b"7".to_vec()),
                (b"x".to_vec(), b"y".to_vec())
            ]
        );
        assert!(s.delete(id(6, 0)));
        assert!(!s.delete(id(6, 0)));
        assert_eq!(ids(&s, id(5, 0), id(8, 0), false), vec![5, 7, 8]);
        assert_eq!(s.len(), 249);
        // Exact: from within a chunk too.
        assert_eq!(s.trim(Trim::MaxLen(240), false, 0), 9);
        assert_eq!(s.len(), 240);
        assert_eq!(ids(&s, StreamId::MIN, id(12, 0), false), vec![11, 12]);
        assert_eq!(s.trim(Trim::MinId(id(200, 0)), false, 0), 189);
        assert_eq!(ids(&s, StreamId::MIN, id(201, 0), false), vec![200, 201]);
        let copy = BigStream::from_compact(&s.to_compact()).unwrap();
        assert_eq!(
            ids(&copy, StreamId::MIN, StreamId::MAX, false),
            ids(&s, StreamId::MIN, StreamId::MAX, false)
        );
        assert_eq!(copy.to_compact(), s.to_compact());
        assert!(BigStream::from_compact(&s.to_compact()[..10]).is_none());
        assert_eq!(s.trim(Trim::MaxLen(0), false, 0), 51);
        assert!(s.is_empty());
        assert_eq!(s.last_id(), id(250, 0));
    }
}
