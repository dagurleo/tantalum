//! Bitmap commands, on strings, following Redis' `bitops.c`. Bit 0 is the most
//! significant bit of the first byte. Writes change a big string where it lies
//! when they can ([`Store::edit_string`]), so a `SETBIT` in a big bitmap doesn't
//! copy it; reads of a big one work on a shared reference, outside the lock.

use super::{Args, NOT_AN_INTEGER, OOM, Reply, SYNTAX_ERROR, WRONGTYPE};
use crate::context::{Context, Session};
use crate::resp;
use crate::store::{COPY_UNDER_LOCK_MAX, Kind, Millis, OutOfMemory, Update};

/// Redis' `proto-max-bulk-len` default, the longest string.
const MAX_STRING_BYTES: u64 = 512 * 1024 * 1024;
const BAD_OFFSET: &[u8] = b"ERR bit offset is not an integer or out of range";
const BAD_BIT: &[u8] = b"ERR bit is not an integer or out of range";
const BAD_TYPE: &[u8] =
    b"ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.";

/// A bit offset: within the longest string, or with `bits` given, a `#n` means
/// the n-th field of that many bits.
fn bit_offset(text: &[u8], bits: Option<u32>) -> Option<u64> {
    let (fields, digits) = match (text.first(), bits) {
        (Some(b'#'), Some(bits)) => (Some(bits), &text[1..]),
        _ => (None, text),
    };
    let n = resp::parse_int(digits)?;
    let n = match fields {
        Some(bits) => n.wrapping_mul(i64::from(bits)),
        None => n,
    };
    let n = u64::try_from(n).ok()?;
    (n >> 3 < MAX_STRING_BYTES).then_some(n)
}

/// Runs `f` on the string at the key, or `None` if there's none: under its
/// shard's lock if it's small, or outside it on a shared reference.
fn read(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    f: impl FnOnce(Option<&[u8]>, &mut Vec<u8>),
) -> Reply {
    let store = &*ctx.store;
    let mut f = Some(f);
    let mut run =
        |value: Option<&[u8]>, out: &mut Vec<u8>| f.take().expect("runs once")(value, out);
    let (big, hit) = store.get_key(args.key(store), now, |value| match value {
        None => {
            run(None, out);
            (None, false)
        }
        Some(v) if v.kind() != Kind::String => {
            resp::write_error(out, WRONGTYPE);
            (None, true)
        }
        Some(v) if v.as_bytes().len() <= COPY_UNDER_LOCK_MAX => {
            run(Some(v.as_bytes()), out);
            (None, true)
        }
        Some(v) => (Some(v.share()), true),
    });
    session.count_lookup(hit);
    if let Some(value) = big {
        run(Some(value.as_bytes()), out);
    }
    Reply::Written
}

/// `SETBIT key offset value`: the bit's old value. A change that isn't one
/// (the same bit, the same length) doesn't count as a write to the key.
pub(super) fn setbit(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let Some(offset) = bit_offset(args.get(2), None) else {
        resp::write_error(out, BAD_OFFSET);
        return Reply::Written;
    };
    let on = match resp::parse_int(args.get(3)) {
        Some(0) => false,
        Some(1) => true,
        _ => {
            resp::write_error(out, BAD_BIT);
            return Reply::Written;
        }
    };
    let byte = (offset >> 3) as usize;
    let mask = 0x80u8 >> (offset & 7);
    let set = |bytes: &mut [u8]| {
        if on {
            bytes[byte] |= mask;
        } else {
            bytes[byte] &= !mask;
        }
    };
    let store = &*ctx.store;
    let key = args.key(store);
    let edited = store.edit_string(
        key,
        now,
        |old| Some(old.max(byte + 1)),
        |bytes, old| {
            let was = bytes[byte] & mask != 0;
            set(bytes);
            (was, old <= byte || was != on)
        },
    );
    let result = match edited {
        Some(was) => Ok(Ok(was)),
        None => store.update_key(key, now, |current| {
            let old = match &current {
                None => &[][..],
                Some(c) if c.value.kind() != Kind::String => {
                    return (Update::Peek, Err(WRONGTYPE));
                }
                Some(c) => c.value.as_bytes(),
            };
            let was = old.get(byte).is_some_and(|b| b & mask != 0);
            if current.is_some() && old.len() > byte && was == on {
                return (Update::Touch, Ok(was));
            }
            let mut new = old.to_vec();
            if new.len() <= byte {
                new.resize(byte + 1, 0);
            }
            set(&mut new);
            (Update::Replace(Kind::String, new), Ok(was))
        }),
    };
    match result {
        Ok(Ok(was)) => resp::write_integer(out, i64::from(was)),
        Ok(Err(message)) => resp::write_error(out, message),
        Err(OutOfMemory) => resp::write_error(out, OOM),
    }
    Reply::Written
}

/// `GETBIT key offset`
pub(super) fn getbit(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let Some(offset) = bit_offset(args.get(2), None) else {
        resp::write_error(out, BAD_OFFSET);
        return Reply::Written;
    };
    let byte = (offset >> 3) as usize;
    let mask = 0x80u8 >> (offset & 7);
    let store = &*ctx.store;
    let (bit, hit) = store.get_key(args.key(store), now, |value| match value {
        None => (Some(false), false),
        Some(v) if v.kind() != Kind::String => (None, true),
        Some(v) => (
            Some(v.as_bytes().get(byte).is_some_and(|b| b & mask != 0)),
            true,
        ),
    });
    session.count_lookup(hit);
    match bit {
        Some(bit) => resp::write_integer(out, i64::from(bit)),
        None => resp::write_error(out, WRONGTYPE),
    }
    Reply::Written
}

fn count_ones(bytes: &[u8]) -> u64 {
    let (words, rest) = bytes.as_chunks::<8>();
    let mut total = 0;
    for word in words {
        total += u64::from(u64::from_ne_bytes(*word).count_ones());
    }
    for byte in rest {
        total += u64::from(byte.count_ones());
    }
    total
}

/// [`count_ones`], with the CPU's own instruction.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "popcnt")]
fn count_ones_popcnt(bytes: &[u8]) -> u64 {
    count_ones(bytes)
}

fn popcount(bytes: &[u8]) -> u64 {
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("popcnt") {
        // SAFETY: the CPU has the feature, as just checked.
        return unsafe { count_ones_popcnt(bytes) };
    }
    count_ones(bytes)
}

/// A range in a string, from `start` and `end` as given, each possibly from
/// the end, in bytes or bits: the bytes it covers and, with bits, masks of the
/// bits in the first and last of them that it leaves out.
struct Span {
    start: i64,
    end: i64,
    first_mask: u8,
    last_mask: u8,
}

fn span(mut start: i64, mut end: i64, len: i64, in_bits: bool) -> Span {
    let total = if in_bits { len << 3 } else { len };
    if start < 0 {
        start = start.saturating_add(total);
    }
    if end < 0 {
        end = end.saturating_add(total);
    }
    start = start.max(0);
    end = end.max(0);
    if end >= total {
        end = total - 1;
    }
    let (mut first_mask, mut last_mask) = (0, 0);
    if in_bits && start <= end {
        first_mask = !(((1u32 << (8 - (start & 7))) - 1) as u8);
        last_mask = ((1u32 << (7 - (end & 7))) - 1) as u8;
        start >>= 3;
        end >>= 3;
    }
    Span {
        start,
        end,
        first_mask,
        last_mask,
    }
}

/// `BIT` or `BYTE`; `None` for anything else.
fn unit(text: &[u8]) -> Option<bool> {
    if text.eq_ignore_ascii_case(b"BIT") {
        Some(true)
    } else if text.eq_ignore_ascii_case(b"BYTE") {
        Some(false)
    } else {
        None
    }
}

/// `BITCOUNT key [start end [BYTE | BIT]]`
pub(super) fn bitcount(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    read(ctx, session, now, args, out, |value, out| {
        // A missing key is 0 whatever the rest says, as in Redis, which looks it
        // up first.
        let Some(value) = value else {
            resp::write_integer(out, 0);
            return;
        };
        let mut range = None;
        if args.len() == 4 || args.len() == 5 {
            let (Some(start), Some(end)) =
                (resp::parse_int(args.get(2)), resp::parse_int(args.get(3)))
            else {
                resp::write_error(out, NOT_AN_INTEGER);
                return;
            };
            let in_bits = if args.len() == 5 {
                let Some(in_bits) = unit(args.get(4)) else {
                    resp::write_error(out, SYNTAX_ERROR);
                    return;
                };
                in_bits
            } else {
                false
            };
            range = Some((start, end, in_bits));
        } else if args.len() != 2 {
            resp::write_error(out, SYNTAX_ERROR);
            return;
        }
        let len = value.len() as i64;
        let span = match range {
            Some((start, end, _)) if start < 0 && end < 0 && start > end => {
                resp::write_integer(out, 0);
                return;
            }
            Some((start, end, in_bits)) => span(start, end, len, in_bits),
            None => Span {
                start: 0,
                end: len - 1,
                first_mask: 0,
                last_mask: 0,
            },
        };
        if span.start > span.end {
            resp::write_integer(out, 0);
            return;
        }
        let (start, end) = (span.start as usize, span.end as usize);
        let mut count = popcount(&value[start..=end]);
        count -= u64::from((value[start] & span.first_mask).count_ones());
        count -= u64::from((value[end] & span.last_mask).count_ones());
        resp::write_integer(out, count as i64);
    })
}

/// Where the first `bit` is in `bytes`. Past the end, for a 0, is `bytes.len() *
/// 8`, as if the string went on in zeroes; for a 1, -1.
fn first_bit(bytes: &[u8], bit: bool) -> i64 {
    let skip = if bit { 0u64 } else { u64::MAX };
    let mut at = 0;
    for word in bytes.as_chunks::<8>().0 {
        if u64::from_ne_bytes(*word) != skip {
            break;
        }
        at += 8;
    }
    for (i, &byte) in bytes[at..].iter().enumerate() {
        let wanted = if bit { byte } else { !byte };
        if wanted != 0 {
            return ((at + i) * 8) as i64 + i64::from(wanted.leading_zeros());
        }
    }
    if bit { -1 } else { bytes.len() as i64 * 8 }
}

/// `BITPOS key bit [start [end [BYTE | BIT]]]`
pub(super) fn bitpos(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let bit = match resp::parse_int(args.get(2)) {
        None => {
            resp::write_error(out, NOT_AN_INTEGER);
            return Reply::Written;
        }
        Some(0) => false,
        Some(1) => true,
        Some(_) => {
            resp::write_error(out, b"ERR The bit argument must be 1 or 0.");
            return Reply::Written;
        }
    };
    read(ctx, session, now, args, out, |value, out| {
        // No key is endless zeroes.
        let Some(value) = value else {
            resp::write_integer(out, if bit { -1 } else { 0 });
            return;
        };
        let len = value.len() as i64;
        let mut end_given = false;
        let span = match args.len() {
            3 => Span {
                start: 0,
                end: len - 1,
                first_mask: 0,
                last_mask: 0,
            },
            4..=6 => {
                let Some(start) = resp::parse_int(args.get(3)) else {
                    resp::write_error(out, NOT_AN_INTEGER);
                    return;
                };
                let mut in_bits = false;
                if args.len() == 6 {
                    let Some(given) = unit(args.get(5)) else {
                        resp::write_error(out, SYNTAX_ERROR);
                        return;
                    };
                    in_bits = given;
                }
                let end = if args.len() >= 5 {
                    let Some(end) = resp::parse_int(args.get(4)) else {
                        resp::write_error(out, NOT_AN_INTEGER);
                        return;
                    };
                    end_given = true;
                    end
                } else {
                    len - 1
                };
                span(start, end, len, in_bits)
            }
            _ => {
                resp::write_error(out, SYNTAX_ERROR);
                return;
            }
        };
        if span.start > span.end {
            resp::write_integer(out, -1);
            return;
        }
        let (mut start, end) = (span.start as usize, span.end as usize);
        let mut bytes = end - start + 1;
        let masked = |byte: u8, mask: u8| if bit { byte & !mask } else { byte | mask };
        let pos = 'found: {
            let mut pos = -1;
            if span.first_mask != 0 {
                let mut byte = masked(value[start], span.first_mask);
                // There's only one byte.
                if span.last_mask != 0 && bytes == 1 {
                    byte = masked(byte, span.last_mask);
                }
                pos = first_bit(&[byte], bit);
                if bytes == 1 || (pos != -1 && pos != 8) {
                    break 'found pos;
                }
                start += 1;
                bytes -= 1;
            }
            // The last byte, if it has bits outside the range, comes apart.
            let whole = bytes - usize::from(span.last_mask != 0);
            if whole > 0 {
                pos = first_bit(&value[start..start + whole], bit);
                if bytes == whole || (pos != -1 && pos != whole as i64 * 8) {
                    break 'found pos;
                }
                start += whole;
                bytes -= whole;
            }
            if bytes > 0 {
                pos = first_bit(&[masked(value[end], span.last_mask)], bit);
            }
            pos
        };
        // Looking for a 0 in a range with an end, what's right of it isn't
        // zeroes: none found is none.
        if end_given && !bit && pos == bytes as i64 * 8 {
            resp::write_integer(out, -1);
            return;
        }
        let pos = if pos == -1 {
            -1
        } else {
            pos + (start as i64) * 8
        };
        resp::write_integer(out, pos);
    })
}

/// `BITOP AND|OR|XOR|NOT destkey key [key ...]`: the length stored. Nothing
/// else runs meanwhile. An empty result removes `destkey`, and otherwise it goes
/// like a `SET`: whatever it held, and its TTL, are gone.
pub(super) fn bitop(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    #[derive(Clone, Copy, PartialEq)]
    enum Op {
        And,
        Or,
        Xor,
        Not,
    }
    let name = args.get(1);
    let op = if name.eq_ignore_ascii_case(b"AND") {
        Op::And
    } else if name.eq_ignore_ascii_case(b"OR") {
        Op::Or
    } else if name.eq_ignore_ascii_case(b"XOR") {
        Op::Xor
    } else if name.eq_ignore_ascii_case(b"NOT") {
        Op::Not
    } else {
        resp::write_error(out, SYNTAX_ERROR);
        return Reply::Written;
    };
    if op == Op::Not && args.len() != 4 {
        resp::write_error(
            out,
            b"ERR BITOP NOT must be called with a single source key.",
        );
        return Reply::Written;
    }
    let store = &*ctx.store;
    store.exclusive(|| {
        let mut sources = Vec::with_capacity(args.len() - 3);
        for i in 3..args.len() {
            let source = store.get_key(args.key_at(store, i), now, |value| match value {
                None => Ok(None),
                Some(v) if v.kind() != Kind::String => Err(()),
                Some(v) => Ok(Some(v.share())),
            });
            match source {
                Ok(source) => sources.push(source),
                Err(()) => {
                    resp::write_error(out, WRONGTYPE);
                    return;
                }
            }
        }
        let bytes = |source: &Option<crate::store::SharedValue>| -> Vec<u8> {
            source
                .as_ref()
                .map_or_else(Vec::new, |v| v.as_bytes().to_vec())
        };
        let longest = sources
            .iter()
            .map(|s| s.as_ref().map_or(0, |v| v.as_bytes().len()))
            .max()
            .unwrap_or(0);
        let mut result = bytes(&sources[0]);
        result.resize(longest, 0);
        if op == Op::Not {
            for byte in &mut result {
                *byte = !*byte;
            }
        }
        for source in &sources[1..] {
            let other = source.as_ref().map_or(&[][..], |v| v.as_bytes());
            let (shared, rest) = result.split_at_mut(other.len().min(longest));
            for (byte, with) in shared.iter_mut().zip(other) {
                match op {
                    Op::And => *byte &= with,
                    Op::Or => *byte |= with,
                    Op::Xor => *byte ^= with,
                    Op::Not => unreachable!("NOT has one source"),
                }
            }
            // A shorter source goes on in zeroes.
            if op == Op::And {
                rest.fill(0);
            }
        }
        let destination = args.key_at(store, 2);
        let _ = store.update_key(destination, now, |_| (Update::Delete, ()));
        if longest > 0 {
            let written = store.update_key(destination, now, |_| {
                (Update::Replace(Kind::String, result.clone()), ())
            });
            if written.is_err() {
                resp::write_error(out, OOM);
                return;
            }
        }
        resp::write_integer(out, longest as i64);
    });
    Reply::Written
}

#[derive(Clone, Copy, PartialEq)]
enum Overflow {
    Wrap,
    Sat,
    Fail,
}

#[derive(Clone, Copy, PartialEq)]
enum FieldOp {
    Get,
    Set,
    IncrBy,
}

struct Field {
    op: FieldOp,
    offset: u64,
    value: i64,
    overflow: Overflow,
    bits: u32,
    signed: bool,
}

fn get_unsigned(bytes: &[u8], offset: u64, bits: u32) -> u64 {
    let mut value = 0u64;
    for i in 0..u64::from(bits) {
        let at = offset + i;
        let byte = bytes[(at >> 3) as usize];
        value = value << 1 | u64::from(byte >> (7 - (at & 7)) & 1);
    }
    value
}

fn get_signed(bytes: &[u8], offset: u64, bits: u32) -> i64 {
    let mut value = get_unsigned(bytes, offset, bits);
    if bits < 64 && value & 1 << (bits - 1) != 0 {
        value |= u64::MAX << bits;
    }
    value as i64
}

fn set_bits(bytes: &mut [u8], offset: u64, bits: u32, value: u64) {
    for i in 0..u64::from(bits) {
        let on = value & 1 << (u64::from(bits) - 1 - i) != 0;
        let at = offset + i;
        let mask = 0x80u8 >> (at & 7);
        let byte = &mut bytes[(at >> 3) as usize];
        if on {
            *byte |= mask;
        } else {
            *byte &= !mask;
        }
    }
}

/// Whether `value + incr` overflows an unsigned field, and what it comes to
/// then, if the overflow rule gives it a value.
fn unsigned_overflow(value: u64, incr: i64, bits: u32, rule: Overflow) -> (bool, u64) {
    let max = (1u64 << bits) - 1;
    let max_incr = max.wrapping_sub(value) as i64;
    let min_incr = value.wrapping_neg() as i64;
    let wrapped = value.wrapping_add(incr as u64) & !(u64::MAX << bits);
    if value > max || (incr > 0 && incr > max_incr) {
        let limit = match rule {
            Overflow::Wrap => wrapped,
            Overflow::Sat => max,
            Overflow::Fail => 0,
        };
        (true, limit)
    } else if incr < 0 && incr < min_incr {
        let limit = match rule {
            Overflow::Wrap => wrapped,
            _ => 0,
        };
        (true, limit)
    } else {
        (false, 0)
    }
}

/// [`unsigned_overflow`] for a signed field.
fn signed_overflow(value: i64, incr: i64, bits: u32, rule: Overflow) -> (bool, i64) {
    let max = if bits == 64 {
        i64::MAX
    } else {
        (1i64 << (bits - 1)) - 1
    };
    let min = -max - 1;
    let max_incr = (max as u64).wrapping_sub(value as u64) as i64;
    let min_incr = min.wrapping_sub(value);
    let wrapped = || {
        let mut sum = (value as u64).wrapping_add(incr as u64);
        if bits < 64 {
            let mask = u64::MAX << bits;
            if sum & 1 << (bits - 1) != 0 {
                sum |= mask;
            } else {
                sum &= !mask;
            }
        }
        sum as i64
    };
    if value > max || (bits != 64 && incr > max_incr) || (value >= 0 && incr > 0 && incr > max_incr)
    {
        let limit = match rule {
            Overflow::Wrap => wrapped(),
            Overflow::Sat => max,
            Overflow::Fail => 0,
        };
        (true, limit)
    } else if value < min
        || (bits != 64 && incr < min_incr)
        || (value < 0 && incr < 0 && incr < min_incr)
    {
        let limit = match rule {
            Overflow::Wrap => wrapped(),
            Overflow::Sat => min,
            Overflow::Fail => 0,
        };
        (true, limit)
    } else {
        (false, 0)
    }
}

/// Runs the fields' operations on `bytes`, big enough for all the writes,
/// writing the replies to `reply`. `grew`: the string is new or got longer.
/// Returns how many changed the string, a grown string counting each write.
fn run_fields(
    fields: &[Field],
    bytes: &mut [u8],
    grew: bool,
    null: &[u8],
    reply: &mut Vec<u8>,
) -> u32 {
    resp::write_array_header(reply, fields.len());
    let mut changes = 0;
    for field in fields {
        let (offset, bits) = (field.offset, field.bits);
        match field.op {
            FieldOp::Get => {
                resp::write_integer(reply, read_field(bytes, field));
            }
            op if field.signed => {
                let old = get_signed(bytes, offset, bits);
                let (overflowed, limit, new, returned);
                if op == FieldOp::IncrBy {
                    (overflowed, limit) = signed_overflow(old, field.value, bits, field.overflow);
                    new = if overflowed {
                        limit
                    } else {
                        old.wrapping_add(field.value)
                    };
                    returned = new;
                } else {
                    (overflowed, limit) = signed_overflow(field.value, 0, bits, field.overflow);
                    new = if overflowed { limit } else { field.value };
                    returned = old;
                }
                if overflowed && field.overflow == Overflow::Fail {
                    reply.extend_from_slice(null);
                } else {
                    resp::write_integer(reply, returned);
                    set_bits(bytes, offset, bits, new as u64);
                    changes += u32::from(grew || old != new);
                }
            }
            op => {
                let old = get_unsigned(bytes, offset, bits);
                let (overflowed, limit, new, returned);
                if op == FieldOp::IncrBy {
                    (overflowed, limit) = unsigned_overflow(old, field.value, bits, field.overflow);
                    new = if overflowed {
                        limit
                    } else {
                        old.wrapping_add(field.value as u64)
                    };
                    returned = new;
                } else {
                    (overflowed, limit) =
                        unsigned_overflow(field.value as u64, 0, bits, field.overflow);
                    new = if overflowed {
                        limit
                    } else {
                        field.value as u64
                    };
                    returned = old;
                }
                if overflowed && field.overflow == Overflow::Fail {
                    reply.extend_from_slice(null);
                } else {
                    resp::write_integer(reply, returned as i64);
                    set_bits(bytes, offset, bits, new);
                    changes += u32::from(grew || old != new);
                }
            }
        }
    }
    changes
}

/// A `GET` field, as if the string went on in zeroes.
fn read_field(bytes: &[u8], field: &Field) -> i64 {
    let start = field.offset >> 3;
    let mut window = [0u8; 9];
    if let Some(rest) = usize::try_from(start).ok().and_then(|at| bytes.get(at..)) {
        let n = rest.len().min(window.len());
        window[..n].copy_from_slice(&rest[..n]);
    }
    let offset = field.offset - start * 8;
    if field.signed {
        get_signed(&window, offset, field.bits)
    } else {
        get_unsigned(&window, offset, field.bits) as i64
    }
}

/// `BITFIELD`'s operations, or `None` after writing the error.
fn parse_fields(args: &Args<'_>, out: &mut Vec<u8>) -> Option<Vec<Field>> {
    let mut fields = Vec::new();
    let mut overflow = Overflow::Wrap;
    let mut i = 2;
    while i < args.len() {
        let name = args.get(i);
        let left = args.len() - i - 1;
        let is = |word: &[u8]| name.eq_ignore_ascii_case(word);
        let op = if is(b"GET") && left >= 2 {
            FieldOp::Get
        } else if is(b"SET") && left >= 3 {
            FieldOp::Set
        } else if is(b"INCRBY") && left >= 3 {
            FieldOp::IncrBy
        } else if is(b"OVERFLOW") && left >= 1 {
            let rule = args.get(i + 1);
            overflow = if rule.eq_ignore_ascii_case(b"WRAP") {
                Overflow::Wrap
            } else if rule.eq_ignore_ascii_case(b"SAT") {
                Overflow::Sat
            } else if rule.eq_ignore_ascii_case(b"FAIL") {
                Overflow::Fail
            } else {
                resp::write_error(out, b"ERR Invalid OVERFLOW type specified");
                return None;
            };
            i += 2;
            continue;
        } else {
            resp::write_error(out, SYNTAX_ERROR);
            return None;
        };
        let kind = args.get(i + 1);
        let signed = match kind.first() {
            Some(b'i') => true,
            Some(b'u') => false,
            _ => {
                resp::write_error(out, BAD_TYPE);
                return None;
            }
        };
        let bits =
            resp::parse_int(&kind[1..]).filter(|&n| n >= 1 && n <= if signed { 64 } else { 63 });
        let Some(bits) = bits else {
            resp::write_error(out, BAD_TYPE);
            return None;
        };
        let bits = bits as u32;
        let Some(offset) = bit_offset(args.get(i + 2), Some(bits)) else {
            resp::write_error(out, BAD_OFFSET);
            return None;
        };
        let mut value = 0;
        if op != FieldOp::Get {
            let Some(given) = resp::parse_int(args.get(i + 3)) else {
                resp::write_error(out, NOT_AN_INTEGER);
                return None;
            };
            value = given;
        }
        fields.push(Field {
            op,
            offset,
            value,
            overflow,
            bits,
            signed,
        });
        i += if op == FieldOp::Get { 3 } else { 4 };
    }
    Some(fields)
}

fn bitfield_generic(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    read_only: bool,
) -> Reply {
    let Some(fields) = parse_fields(args, out) else {
        return Reply::Written;
    };
    let null = session.null();
    let writes_to = fields
        .iter()
        .filter(|field| field.op != FieldOp::Get)
        .map(|field| field.offset + u64::from(field.bits) - 1)
        .max();
    let Some(highest) = writes_to else {
        return read(ctx, session, now, args, out, |value, out| {
            resp::write_array_header(out, fields.len());
            for field in &fields {
                resp::write_integer(out, read_field(value.unwrap_or(&[]), field));
            }
        });
    };
    if read_only {
        resp::write_error(out, b"ERR BITFIELD_RO only supports the GET subcommand");
        return Reply::Written;
    }
    let needed = (highest >> 3) as usize + 1;
    let store = &*ctx.store;
    let key = args.key(store);
    let mut reply = Vec::new();
    let edited = store.edit_string(
        key,
        now,
        |old| Some(old.max(needed)),
        |bytes, old| {
            let grew = old < needed;
            (
                (),
                run_fields(&fields, bytes, grew, null, &mut reply) > 0 || grew,
            )
        },
    );
    let result = match edited {
        Some(()) => Ok(Ok(())),
        None => store.update_key(key, now, |current| {
            let old = match &current {
                None => &[][..],
                Some(c) if c.value.kind() != Kind::String => {
                    return (Update::Peek, Err(WRONGTYPE));
                }
                Some(c) => c.value.as_bytes(),
            };
            let grew = current.is_none() || old.len() < needed;
            let mut new = old.to_vec();
            if new.len() < needed {
                new.resize(needed, 0);
            }
            reply.clear();
            let changes = run_fields(&fields, &mut new, grew, null, &mut reply);
            if changes == 0 && !grew {
                (Update::Touch, Ok(()))
            } else {
                (Update::Replace(Kind::String, new), Ok(()))
            }
        }),
    };
    match result {
        Ok(Ok(())) => out.extend_from_slice(&reply),
        Ok(Err(message)) => resp::write_error(out, message),
        Err(OutOfMemory) => resp::write_error(out, OOM),
    }
    Reply::Written
}

/// `BITFIELD key [GET type offset | SET type offset value | INCRBY type offset
/// increment | OVERFLOW WRAP|SAT|FAIL ...]`
pub(super) fn bitfield(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    bitfield_generic(ctx, session, now, args, out, false)
}

/// `BITFIELD_RO key [GET type offset ...]`
pub(super) fn bitfield_ro(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    bitfield_generic(ctx, session, now, args, out, true)
}
