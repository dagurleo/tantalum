//! Streams: entries of fields and values under increasing IDs (`XADD`,
//! `XRANGE`, `XREAD` ...), always kept as an object by the shard.
//!
//! Entries live in chunks, as Redis keeps them in listpack nodes, and a new
//! chunk starts exactly where Redis would start a new node: after 100
//! entries, or when its listpack would pass 4 KB (by default: see
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

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use crate::id_map::IdMap;

/// What starts a new chunk: `stream-node-max-entries` (0: no limit) and
/// `stream-node-max-bytes`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeLimits {
    pub entries: usize,
    pub bytes: usize,
}

impl Default for NodeLimits {
    /// Redis' defaults.
    fn default() -> Self {
        NodeLimits {
            entries: 100,
            bytes: 4096,
        }
    }
}

impl NodeLimits {
    /// `stream-node-max-bytes` as it acts: 0 and what's beyond a listpack's limit mean that limit.
    pub fn bytes_from_setting(bytes: usize) -> usize {
        const MOST: usize = 1 << 30;
        if bytes == 0 { MOST } else { bytes.min(MOST) }
    }

    /// How many entries `~` removes at most when not told: 100 nodes' worth, within limits.
    pub fn default_trim_limit(self) -> usize {
        match self.entries.saturating_mul(100) {
            0 => 10_000,
            limit => limit.min(1_000_000),
        }
    }
}

/// A whole entry in a chunk takes its fields from the chunk's first.
const SAME_FIELDS: u8 = 1;
/// A group's `entries_read` when it isn't known.
pub const UNKNOWN_READS: i64 = -1;
/// About what a consumer takes besides its slot and its pending list: its name's
/// header, and its place in the tree of names.
const CONSUMER_BYTES: usize = 64;
/// About what a group takes besides what it holds.
const GROUP_BYTES: usize = 192;

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

/// A chunk as Redis' RDB format writes its listpack node, for `DUMP`.
pub struct Node<'a>(&'a Chunk);

impl<'a> Node<'a> {
    /// Its master entry's ID: the first ever added to it.
    pub fn master(&self) -> StreamId {
        self.0.first
    }

    pub fn live(&self) -> u32 {
        self.0.live
    }

    /// Its master entry's field names.
    pub fn fields(&self) -> impl Iterator<Item = &'a [u8]> + 'a {
        let fields: &'a [u8] = &self.0.fields;
        let mut p = 0;
        let count = get_varint(fields, &mut p).expect("valid chunk");
        (0..count).map(move |_| get_bytes(fields, &mut p).expect("valid chunk"))
    }

    /// Its entries, each with whether it has the master entry's fields
    /// (Redis' `SAMEFIELDS`).
    pub fn entries(&self) -> impl Iterator<Item = (Entry<'a>, bool)> + 'a {
        let chunk = self.0;
        chunk
            .entries()
            .map(move |(start, _, entry)| (entry, chunk.data[start] & SAME_FIELDS != 0))
    }
}

/// An entry delivered to a consumer and not acknowledged. There can be millions,
/// so it's packed, and knows its consumer by number.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C, packed(4))]
pub struct Pending {
    /// Unix milliseconds.
    pub delivery_time: i64,
    pub delivery_count: i64,
    consumer: u32,
}

/// A consumer in a group.
#[derive(Clone, Debug)]
pub struct Consumer {
    name: Arc<[u8]>,
    /// When it last tried to read or claim, Unix milliseconds.
    pub seen_time: i64,
    /// When it last got an entry; -1 if it never has.
    pub active_time: i64,
    /// The entries pending for it, each also in its group's list.
    pending: IdMap<()>,
}

impl Consumer {
    pub fn name(&self) -> &[u8] {
        &self.name
    }

    pub fn pending(&self) -> &IdMap<()> {
        &self.pending
    }
}

/// A consumer of a group, for the length of a command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsumerId(u32);

/// A consumer group. Consumers and groups sort by name, as Redis' radix trees
/// do, which is the order `XINFO` lists them in.
#[derive(Debug)]
pub struct Group {
    pub last_id: StreamId,
    /// How many entries it has read, or [`UNKNOWN_READS`].
    pub entries_read: i64,
    pending: IdMap<Pending>,
    by_name: BTreeMap<Arc<[u8]>, ConsumerId>,
    /// Its consumers by number; `free` has the numbers of those gone.
    slots: Vec<Option<Consumer>>,
    free: Vec<u32>,
    /// What its consumers take but for their slots, kept as they change.
    consumer_bytes: usize,
}

impl Clone for Group {
    fn clone(&self) -> Self {
        let slots = self.slots.clone();
        // Copies of pending lists have no spare room, so they take less.
        let consumer_bytes = slots
            .iter()
            .flatten()
            .map(|consumer| CONSUMER_BYTES + consumer.name.len() + consumer.pending.bytes())
            .sum();
        Group {
            last_id: self.last_id,
            entries_read: self.entries_read,
            pending: self.pending.clone(),
            by_name: self.by_name.clone(),
            slots,
            free: self.free.clone(),
            consumer_bytes,
        }
    }
}

/// The same as far as a client can tell, however it's laid out.
impl PartialEq for Group {
    fn eq(&self, other: &Self) -> bool {
        fn pending(group: &Group) -> impl Iterator<Item = (StreamId, i64, i64, &[u8])> {
            group
                .pending
                .iter()
                .map(|(id, p)| (id, p.delivery_time, p.delivery_count, group.owner(p)))
        }
        fn consumers(group: &Group) -> impl Iterator<Item = (&[u8], i64, i64, Vec<StreamId>)> {
            group.consumers().map(|c| {
                let ids = c.pending.iter().map(|(id, _)| id).collect();
                (c.name(), c.seen_time, c.active_time, ids)
            })
        }
        self.last_id == other.last_id
            && self.entries_read == other.entries_read
            && pending(self).eq(pending(other))
            && consumers(self).eq(consumers(other))
    }
}

impl Eq for Group {}

impl Group {
    pub fn new(last_id: StreamId, entries_read: i64) -> Self {
        Group {
            last_id,
            entries_read,
            pending: IdMap::default(),
            by_name: BTreeMap::new(),
            slots: Vec::new(),
            free: Vec::new(),
            consumer_bytes: 0,
        }
    }

    /// What it has allocated.
    fn bytes(&self) -> usize {
        GROUP_BYTES
            + self.pending.bytes()
            + self.consumer_bytes
            + self.slots.capacity() * size_of::<Option<Consumer>>()
            + self.free.capacity() * size_of::<u32>()
    }

    pub fn pending(&self) -> &IdMap<Pending> {
        &self.pending
    }

    /// The name of the consumer an entry is pending for.
    pub fn owner(&self, pending: &Pending) -> &[u8] {
        self.slot(ConsumerId(pending.consumer)).name()
    }

    fn slot(&self, id: ConsumerId) -> &Consumer {
        self.slots[id.0 as usize].as_ref().expect("a live consumer")
    }

    pub fn consumer_mut(&mut self, id: ConsumerId) -> &mut Consumer {
        self.slots[id.0 as usize].as_mut().expect("a live consumer")
    }

    /// Its consumers, by name.
    pub fn consumers(&self) -> impl Iterator<Item = &Consumer> + Clone {
        self.by_name.values().map(|&id| self.slot(id))
    }

    pub fn consumer_count(&self) -> usize {
        self.by_name.len()
    }

    pub fn consumer_named(&self, name: &[u8]) -> Option<&Consumer> {
        self.by_name.get(name).map(|&id| self.slot(id))
    }

    /// The consumer called `name`, made if there's none (then `true`).
    pub fn consumer(&mut self, name: &[u8], now: i64) -> (ConsumerId, bool) {
        if let Some(&id) = self.by_name.get(name) {
            return (id, false);
        }
        let name: Arc<[u8]> = name.into();
        self.consumer_bytes += CONSUMER_BYTES + name.len();
        let consumer = Consumer {
            name: name.clone(),
            seen_time: now,
            active_time: -1,
            pending: IdMap::default(),
        };
        let id = match self.free.pop() {
            Some(slot) => {
                self.slots[slot as usize] = Some(consumer);
                ConsumerId(slot)
            }
            None => {
                self.slots.push(Some(consumer));
                ConsumerId(self.slots.len() as u32 - 1)
            }
        };
        self.by_name.insert(name, id);
        (id, true)
    }

    /// Removes a consumer and what's pending for it, returning how many were.
    pub fn remove_consumer(&mut self, name: &[u8]) -> Option<usize> {
        let (name, id) = self.by_name.remove_entry(name)?;
        let consumer = self.slots[id.0 as usize].take().expect("a live consumer");
        self.free.push(id.0);
        self.consumer_bytes -= CONSUMER_BYTES + name.len() + consumer.pending.bytes();
        for (id, ()) in consumer.pending.iter() {
            self.pending.remove(id);
        }
        Some(consumer.pending.len())
    }

    /// Runs `f` on a consumer's pending list, keeping count of what it takes.
    fn pending_of<R>(&mut self, id: ConsumerId, f: impl FnOnce(&mut IdMap<()>) -> R) -> R {
        let list = &mut self.consumer_mut(id).pending;
        let before = list.bytes();
        let result = f(list);
        let after = list.bytes();
        self.consumer_bytes = self.consumer_bytes + after - before;
        result
    }

    /// Takes an entry off the pending lists.
    pub fn acknowledge(&mut self, id: StreamId) -> bool {
        let Some(pending) = self.pending.remove(id) else {
            return false;
        };
        self.pending_of(ConsumerId(pending.consumer), |list| list.remove(id));
        true
    }

    /// [`Group::assign`] for an entry that's likely pending already: a claim,
    /// which needn't look for where a new one would go.
    pub fn reassign(&mut self, id: StreamId, consumer: ConsumerId, time: i64, count: i64) {
        let Some(pending) = self.pending.get_mut(id) else {
            return self.assign(id, consumer, time, count);
        };
        let owner = ConsumerId(pending.consumer);
        pending.delivery_time = time;
        pending.delivery_count = count;
        pending.consumer = consumer.0;
        if owner != consumer {
            self.pending_of(owner, |list| list.remove(id));
            self.pending_of(consumer, |list| list.insert(id, ()));
        }
    }

    /// Makes `id` pending for `consumer` with this time and count: from whoever
    /// had it, if someone did.
    pub fn assign(&mut self, id: StreamId, consumer: ConsumerId, time: i64, count: i64) {
        let pending = Pending {
            delivery_time: time,
            delivery_count: count,
            consumer: consumer.0,
        };
        if let Some(old) = self.pending.insert(id, pending) {
            let owner = ConsumerId(old.consumer);
            if owner == consumer {
                return;
            }
            self.pending_of(owner, |list| list.remove(id));
        }
        self.pending_of(consumer, |list| list.insert(id, ()));
    }
}

/// A stream.
#[derive(Clone, Default)]
pub struct BigStream {
    chunks: VecDeque<Chunk>,
    len: usize,
    last_id: StreamId,
    /// Ever added, and the largest removed by `XDEL`.
    entries_added: u64,
    max_deleted: StreamId,
    /// The chunks' allocations, kept as they change.
    bytes: usize,
    /// Its consumer groups, by name: empty, as most streams' are, it holds no
    /// memory.
    pub groups: BTreeMap<Box<[u8]>, Group>,
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
            + self
                .groups
                .iter()
                .map(|(name, group)| name.len() + group.bytes())
                .sum::<usize>()
    }

    pub fn entries_added(&self) -> u64 {
        self.entries_added
    }

    pub fn max_deleted(&self) -> StreamId {
        self.max_deleted
    }

    /// How many chunks it has: Redis' `radix-tree-keys`.
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Its chunks, as Redis' listpack nodes.
    pub fn nodes(&self) -> impl Iterator<Item = Node<'_>> {
        self.chunks.iter().map(Node)
    }

    /// `XSETID`: what it checks first, then this.
    pub fn set_ids(&mut self, last: StreamId, added: Option<u64>, max_deleted: StreamId) {
        self.last_id = last;
        if let Some(added) = added {
            self.entries_added = added;
        }
        if max_deleted != StreamId::MIN {
            self.max_deleted = max_deleted;
        }
    }

    /// The first entry's ID, or 0-0 if it's empty: Redis' `first_id`.
    pub fn first_id(&self) -> StreamId {
        let mut first = StreamId::MIN;
        self.range(StreamId::MIN, StreamId::MAX, false, |entry| {
            first = entry.id;
            false
        });
        first
    }

    /// The last entry's ID, which `XDEL` may have left below [`BigStream::last_id`].
    pub fn last_entry_id(&self) -> StreamId {
        let mut last = StreamId::MIN;
        self.range(StreamId::MIN, StreamId::MAX, true, |entry| {
            last = entry.id;
            false
        });
        last
    }

    pub fn has_entry(&self, id: StreamId) -> bool {
        let mut found = false;
        self.range(id, id, false, |_| {
            found = true;
            false
        });
        found
    }

    /// Whether a deleted entry may lie after `start`: Redis'
    /// `streamRangeHasTombstones` with no end.
    pub fn tombstones_after(&self, start: StreamId) -> bool {
        if self.len == 0 || self.max_deleted == StreamId::MIN {
            return false;
        }
        self.first_id() <= self.max_deleted && start <= self.max_deleted
    }

    /// How many entries were added up to `id`, where that can be known: Redis'
    /// `streamEstimateDistanceFromFirstEverEntry`.
    pub fn entries_up_to(&self, id: StreamId) -> Option<i64> {
        let added = self.entries_added as i64;
        if self.entries_added == 0 {
            return Some(0);
        }
        let len = self.len as i64;
        if self.len == 0 && id <= self.last_id {
            return Some(added);
        }
        if id == self.last_id {
            return Some(added);
        }
        if id > self.last_id {
            return None;
        }
        let first = self.first_id();
        if self.max_deleted == StreamId::MIN || self.max_deleted < first {
            if id < first {
                return Some(added.wrapping_sub(len));
            }
            if id == first {
                return Some(added.wrapping_sub(len).wrapping_add(1));
            }
        }
        None
    }

    /// How many entries `group` has yet to read, where that can be known.
    pub fn lag(&self, group: &Group) -> Option<i64> {
        if self.entries_added == 0 {
            return Some(0);
        }
        if group.entries_read != UNKNOWN_READS && !self.tombstones_after(group.last_id) {
            return Some((self.entries_added as i64).wrapping_sub(group.entries_read));
        }
        self.entries_up_to(group.last_id)
            .map(|read| (self.entries_added as i64).wrapping_sub(read))
    }

    /// Runs `f` with this stream and its group `name` apart, for what reads
    /// entries while it changes the group. `f`'s stream shows no groups.
    pub fn with_group<R>(
        &mut self,
        name: &[u8],
        f: impl FnOnce(&BigStream, &mut Group) -> R,
    ) -> Option<R> {
        let mut groups = std::mem::take(&mut self.groups);
        let result = groups.get_mut(name).map(|group| f(self, group));
        self.groups = groups;
        result
    }

    /// `XREADGROUP` with `>`: gives `emit` the entries after the group's last
    /// (at most `count`, unless 0) and makes them pending for `consumer`, which
    /// exists (unless `noack`). Returns how many.
    pub fn deliver(
        &self,
        group: &mut Group,
        consumer: ConsumerId,
        count: usize,
        noack: bool,
        now: i64,
        mut emit: impl FnMut(&Entry<'_>),
    ) -> usize {
        let Some(start) = group.last_id.next() else {
            return 0;
        };
        let mut n = 0;
        self.range(start, StreamId::MAX, false, |entry| {
            if group.entries_read != UNKNOWN_READS && !self.tombstones_after(entry.id) {
                group.entries_read = group.entries_read.wrapping_add(1);
            } else if self.entries_added != 0 {
                group.entries_read = self.entries_up_to(entry.id).unwrap_or(UNKNOWN_READS);
            }
            group.last_id = entry.id;
            emit(entry);
            if !noack {
                group.assign(entry.id, consumer, now, 1);
            }
            n += 1;
            count == 0 || n < count
        });
        if n > 0 && !noack {
            group.consumer_mut(consumer).active_time = now;
        }
        n
    }

    /// `XREADGROUP` with an ID: gives `emit` what's pending for `consumer` from
    /// `start` on (at most `count`, unless 0), as delivered again, or `None` for
    /// an entry no longer in the stream. Returns how many.
    pub fn redeliver(
        &self,
        group: &mut Group,
        consumer: ConsumerId,
        start: StreamId,
        count: usize,
        now: i64,
        mut emit: impl FnMut(StreamId, Option<&Entry<'_>>),
    ) -> usize {
        let Some(Some(consumer)) = group.slots.get(consumer.0 as usize) else {
            return 0;
        };
        let pending = &mut group.pending;
        let mut n = 0;
        for (id, ()) in consumer.pending.range(start, StreamId::MAX) {
            if count != 0 && n == count {
                break;
            }
            let mut found = false;
            self.range(id, id, false, |entry| {
                found = true;
                emit(id, Some(entry));
                false
            });
            if !found {
                emit(id, None);
            } else if let Some(pending) = pending.get_mut(id) {
                pending.delivery_time = now;
                pending.delivery_count = pending.delivery_count.wrapping_add(1);
            }
            n += 1;
        }
        n
    }

    /// How many nodes Redis' radix tree of its chunks has, for `XINFO`: chunks
    /// are keyed by their first ID in 16 big-endian bytes.
    pub fn radix_nodes(&self) -> usize {
        fn nodes(keys: &[[u8; 16]], depth: usize) -> usize {
            if keys.is_empty() {
                return 1;
            }
            let shared = |at: usize| keys.iter().all(|key| key[at] == keys[0][at]);
            let mut end = depth;
            while end < 16 && shared(end) {
                end += 1;
            }
            // A run of bytes in common is one node, before what comes after it.
            let run = usize::from(end > depth);
            if end == 16 {
                return run + 1;
            }
            let mut total = run + 1;
            let mut rest = keys;
            while let Some(first) = rest.first() {
                let n = rest.iter().take_while(|key| key[end] == first[end]).count();
                total += nodes(&rest[..n], end + 1);
                rest = &rest[n..];
            }
            total
        }
        let keys: Vec<[u8; 16]> = self
            .chunks
            .iter()
            .map(|chunk| {
                let mut key = [0; 16];
                key[..8].copy_from_slice(&chunk.first.ms.to_be_bytes());
                key[8..].copy_from_slice(&chunk.first.seq.to_be_bytes());
                key
            })
            .collect();
        nodes(&keys, 0)
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
    pub fn add(&mut self, id: StreamId, pairs: &[(&[u8], &[u8])], limits: NodeLimits) {
        let raw: usize = pairs.iter().map(|(f, v)| f.len() + v.len()).sum();
        let starts_chunk = match self.chunks.back() {
            None => true,
            Some(chunk) => {
                chunk.lp_bytes + raw >= limits.bytes
                    || (limits.entries != 0
                        && (chunk.live + chunk.deleted) as usize >= limits.entries)
            }
        };
        if starts_chunk {
            // The last chunk is done growing: a copy takes only what it holds.
            // (Shrinking it in place wouldn't: an allocator may keep a block
            // asked to shrink by less than half, and mimalloc does.) The next will
            // likely hold about as much, so it won't have to grow its way there.
            let mut room = 0;
            if let Some(last) = self.chunks.back_mut() {
                let before = last.allocation_size();
                let held = last.data.len();
                if last.data.capacity() > held {
                    last.data = last.data.as_slice().to_vec();
                }
                self.bytes = self.bytes + last.allocation_size() - before;
                room = held + held / 8;
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
                data: Vec::with_capacity(room),
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
        // Only with groups: a stream without them is as version 4 has it.
        if !self.groups.is_empty() {
            put_varint(&mut out, self.groups.len() as u64);
            for (name, group) in &self.groups {
                put_bytes(&mut out, name);
                put_varint(&mut out, group.last_id.ms);
                put_varint(&mut out, group.last_id.seq);
                put_varint(&mut out, (group.entries_read as u64).wrapping_add(1));
                put_varint(&mut out, group.consumer_count() as u64);
                // Each pending entry says its consumer's place among them.
                let mut places = vec![0; group.slots.len()];
                for (place, (name, &id)) in group.by_name.iter().enumerate() {
                    let consumer = group.slot(id);
                    places[id.0 as usize] = place as u64;
                    put_bytes(&mut out, name);
                    put_varint(&mut out, consumer.seen_time as u64);
                    put_varint(&mut out, (consumer.active_time + 1) as u64);
                }
                put_varint(&mut out, group.pending.len() as u64);
                for (id, pending) in group.pending.iter() {
                    put_varint(&mut out, id.ms);
                    put_varint(&mut out, id.seq);
                    put_varint(&mut out, pending.delivery_time as u64);
                    put_varint(&mut out, pending.delivery_count as u64);
                    put_varint(&mut out, places[pending.consumer as usize]);
                }
            }
        }
        out
    }

    /// Whether [`BigStream::to_compact`]'s bytes have groups, which only a newer
    /// snapshot version can hold. Bytes that aren't valid say no.
    pub fn compact_has_groups(bytes: &[u8]) -> bool {
        fn after_chunks(bytes: &[u8]) -> Option<usize> {
            let mut pos = 0;
            for _ in 0..5 {
                get_varint(bytes, &mut pos)?;
            }
            for _ in 0..get_varint(bytes, &mut pos)? {
                for _ in 0..7 {
                    get_varint(bytes, &mut pos)?;
                }
                get_bytes(bytes, &mut pos)?;
                get_bytes(bytes, &mut pos)?;
            }
            Some(pos)
        }
        after_chunks(bytes).is_some_and(|pos| pos < bytes.len())
    }

    /// The groups from where [`BigStream::to_compact`] puts them.
    fn groups_from_compact(bytes: &[u8], pos: &mut usize) -> Option<BTreeMap<Box<[u8]>, Group>> {
        let mut groups = BTreeMap::new();
        for _ in 0..get_varint(bytes, pos)? {
            let name: Box<[u8]> = get_bytes(bytes, pos)?.into();
            let last_id = StreamId {
                ms: get_varint(bytes, pos)?,
                seq: get_varint(bytes, pos)?,
            };
            let entries_read = (get_varint(bytes, pos)?.wrapping_sub(1)) as i64;
            if entries_read < UNKNOWN_READS {
                return None;
            }
            let mut group = Group::new(last_id, entries_read);
            let mut names = Vec::new();
            for _ in 0..get_varint(bytes, pos)? {
                let consumer = get_bytes(bytes, pos)?;
                let seen_time = get_varint(bytes, pos)? as i64;
                let active_time = i64::try_from(get_varint(bytes, pos)?).ok()? - 1;
                let (id, created) = group.consumer(consumer, seen_time);
                if !created {
                    return None;
                }
                group.consumer_mut(id).active_time = active_time;
                names.push(id);
            }
            for _ in 0..get_varint(bytes, pos)? {
                let id = StreamId {
                    ms: get_varint(bytes, pos)?,
                    seq: get_varint(bytes, pos)?,
                };
                let time = get_varint(bytes, pos)? as i64;
                let count = get_varint(bytes, pos)? as i64;
                let consumer = *names.get(usize::try_from(get_varint(bytes, pos)?).ok()?)?;
                if group.pending.get(id).is_some() {
                    return None;
                }
                group.assign(id, consumer, time, count);
            }
            if groups.insert(name, group).is_some() {
                return None;
            }
        }
        Some(groups)
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
        if pos < bytes.len() {
            stream.groups = Self::groups_from_compact(bytes, &mut pos)?;
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
            for (_, _, entry) in chunk.entries() {
                assert!(previous.is_none_or(|p| entry.id > p), "ids in order");
                assert!(entry.id >= chunk.first && entry.id <= chunk.last);
                previous = Some(entry.id);
            }
        }
        for group in self.groups.values() {
            group.pending.check();
            let mut held = 0;
            let mut bytes = 0;
            for consumer in group.consumers() {
                consumer.pending.check();
                bytes += CONSUMER_BYTES + consumer.name.len() + consumer.pending.bytes();
                held += consumer.pending.len();
                for (id, ()) in consumer.pending.iter() {
                    let pending = group.pending.get(id).expect("pending in the group too");
                    assert_eq!(group.owner(pending), consumer.name(), "its consumer");
                }
            }
            assert_eq!(held, group.pending.len(), "pending entries, both ways");
            assert_eq!(bytes, group.consumer_bytes, "a group's bytes");
            let live = group.slots.iter().flatten().count();
            assert_eq!(live, group.by_name.len(), "its consumers");
            assert_eq!(live + group.free.len(), group.slots.len(), "its slots");
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
    fn groups_keep_their_books_and_come_back_from_their_bytes() {
        let mut stream = BigStream::default();
        for ms in 1..=5 {
            stream.add(id(ms, 0), &[(b"f", b"v")], NodeLimits::default());
        }
        assert!(!BigStream::compact_has_groups(&stream.to_compact()));
        let mut group = Group::new(id(3, 0), 3);
        let (a, created) = group.consumer(b"a", 100);
        assert!(created);
        assert!(!group.consumer(b"a", 200).1);
        let (b, _) = group.consumer(b"b", 100);
        group.assign(id(1, 0), a, 150, 1);
        group.assign(id(2, 0), a, 160, 2);
        group.assign(id(3, 0), b, 170, 1);
        // From one to the other.
        group.assign(id(2, 0), b, 180, 3);
        assert_eq!(group.consumer_named(b"a").unwrap().pending().len(), 1);
        assert_eq!(group.consumer_named(b"b").unwrap().pending().len(), 2);
        assert!(group.acknowledge(id(1, 0)));
        assert!(!group.acknowledge(id(1, 0)));
        group.consumer_mut(b).active_time = 190;
        stream.groups.insert((*b"g").into(), group);
        stream
            .groups
            .insert((*b"empty").into(), Group::new(id(0, 0), UNKNOWN_READS));
        stream.check();
        let bytes = stream.to_compact();
        assert!(BigStream::compact_has_groups(&bytes));
        let back = BigStream::from_compact(&bytes).unwrap();
        back.check();
        assert_eq!(back.groups, stream.groups);
        assert_eq!(back.to_compact(), bytes);
        assert!(BigStream::from_compact(&bytes[..bytes.len() - 1]).is_none());

        let group = stream.groups.get_mut(&b"g"[..]).unwrap();
        assert_eq!(group.remove_consumer(b"b"), Some(2));
        assert_eq!(group.remove_consumer(b"b"), None);
        assert!(group.pending.is_empty());
        stream.check();

        // A number that's free goes to the next, and entries find theirs.
        let group = stream.groups.get_mut(&b"g"[..]).unwrap();
        let (c, _) = group.consumer(b"c", 300);
        assert_eq!(c, b);
        group.assign(id(4, 0), c, 310, 1);
        assert_eq!(group.owner(group.pending().get(id(4, 0)).unwrap()), b"c");
        stream.check();
    }

    /// A chunk that's done takes what it holds, however it grew: asked to
    /// shrink by less than half, an allocator keeps the block.
    #[test]
    fn full_chunks_fit_what_they_hold() {
        for value in [&b"v"[..], &[b'v'; 32], &[b'v'; 700]] {
            let mut stream = BigStream::default();
            for ms in 1..=2_000 {
                stream.add(id(ms, 0), &[(b"f", value)], NodeLimits::default());
            }
            stream.check();
            let last = stream.chunks.len() - 1;
            assert!(last > 1);
            for chunk in stream.chunks.iter().take(last) {
                assert_eq!(chunk.data.capacity(), chunk.data.len());
            }
            // And the one still filling, sized by the last, has room to finish.
            let full = stream.chunks[last - 1].data.len();
            assert!(stream.chunks[last].data.capacity() >= full);
        }
    }

    /// Groups leave the other collections' objects the size they were: a
    /// `Collection` is as big as its biggest kind.
    #[test]
    fn groups_cost_other_kinds_nothing() {
        assert!(size_of::<BigStream>() <= size_of::<crate::zset::BigZSet>());
    }

    #[test]
    fn radix_nodes_are_as_redis_counts_them() {
        let mut stream = BigStream::default();
        assert_eq!(stream.radix_nodes(), 1);
        stream.add(id(1, 0), &[(b"f", b"v")], NodeLimits::default());
        // A run of 16 bytes, and the key it ends in.
        assert_eq!(stream.radix_nodes(), 2);
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
        s.add(first, &[(b"f", b"v")], NodeLimits::default());
        assert_eq!(s.next_id(None, 50), Ok(id(100, 1)));
        assert_eq!(s.next_id(Some((100, None)), 0), Ok(id(100, 1)));
        assert_eq!(s.next_id(Some((101, None)), 0), Ok(id(101, 0)));
        assert_eq!(s.next_id(Some((99, None)), 0), Err(AddError::TooSmall));
        assert_eq!(s.next_id(Some((100, Some(0))), 0), Err(AddError::TooSmall));
        s.add(StreamId::MAX, &[(b"f", b"v")], NodeLimits::default());
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
                s.add(id(i + 1, 1), &[(b"f", &value)], NodeLimits::default());
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
                s.add(
                    id(i, 0),
                    &[(b"other", v.as_bytes()), (b"x", b"y")],
                    NodeLimits::default(),
                );
            } else {
                s.add(id(i, 0), &[(b"f", v.as_bytes())], NodeLimits::default());
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
