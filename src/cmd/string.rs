//! String commands beyond `GET` and `SET`, and `LCS`. A big string is one
//! allocation, so `APPEND` and `SETRANGE` copy it to change it (see the README's
//! "Not there yet").

use std::os::raw::{c_char, c_int};

use super::{Args, NOT_AN_INTEGER, OOM, Reply, SYNTAX_ERROR, WRONGTYPE, bulk};
use crate::context::{Context, Session};
use crate::resp;
use crate::store::{
    COPY_UNDER_LOCK_MAX, Condition, Expiry, Key, Kind, Millis, OutOfMemory, Store, Update,
};

/// Redis' `proto-max-bulk-len` default: the longest string.
const MAX_STRING: usize = 512 * 1024 * 1024;
const STRING_TOO_LONG: &[u8] = b"ERR string exceeds maximum allowed size (proto-max-bulk-len)";
const NOT_A_FLOAT: &[u8] = b"ERR value is not a valid float";
const NOT_FINITE: &[u8] = b"ERR increment would produce NaN or Infinity";

unsafe extern "C" {
    fn tantalum_long_double_add(
        current: *const c_char,
        current_len: usize,
        incr: *const c_char,
        incr_len: usize,
        out: *mut c_char,
        out_len: *mut usize,
    ) -> c_int;
}

/// Why [`add_floats`] failed.
pub(super) enum FloatError {
    BadCurrent,
    BadIncrement,
    IncrementNotFinite,
    SumNotFinite,
}

/// `current` (0 if `None`) plus `incr`, in `long double` and formatted as
/// Redis does (see `long_double.c`).
pub(super) fn add_floats(current: Option<&[u8]>, incr: &[u8]) -> Result<Vec<u8>, FloatError> {
    let mut sum = vec![0u8; 5 * 1024];
    let mut len = 0;
    let (current_ptr, current_len) = current.map_or((std::ptr::null(), 0), |current| {
        (current.as_ptr().cast::<c_char>(), current.len())
    });
    // SAFETY: the pointers and lengths are of live slices (or null for none),
    // and `sum` holds the 5 KB `long_double.c` writes at most.
    let status = unsafe {
        tantalum_long_double_add(
            current_ptr,
            current_len,
            incr.as_ptr().cast(),
            incr.len(),
            sum.as_mut_ptr().cast(),
            &raw mut len,
        )
    };
    match status {
        0 => {
            sum.truncate(len);
            Ok(sum)
        }
        1 => Err(FloatError::BadCurrent),
        2 => Err(FloatError::BadIncrement),
        3 => Err(FloatError::IncrementNotFinite),
        _ => Err(FloatError::SumNotFinite),
    }
}

/// Writes the reply to a command changing a string: `n`, an error, or OOM.
fn reply_integer<T>(
    result: Result<Result<i64, T>, OutOfMemory>,
    out: &mut Vec<u8>,
    message: impl FnOnce(T) -> &'static [u8],
) -> Reply {
    match result {
        Ok(Ok(n)) => resp::write_integer(out, n),
        Ok(Err(e)) => resp::write_error(out, message(e)),
        Err(OutOfMemory) => resp::write_error(out, OOM),
    }
    Reply::Written
}

/// `STRLEN key`
pub(super) fn strlen(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = &*ctx.store;
    let (len, hit) = store.get_key(args.key(store), now, |value| match value {
        None => (Some(0), false),
        Some(v) if v.kind() != Kind::String => (None, true),
        Some(v) => (Some(v.as_bytes().len()), true),
    });
    session.count_lookup(hit);
    match len {
        Some(len) => resp::write_integer(out, len as i64),
        None => resp::write_error(out, WRONGTYPE),
    }
    Reply::Written
}

/// `APPEND key value`: the new length.
pub(super) fn append(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = &*ctx.store;
    let value = args.get(2);
    let result = store.update_key(args.key(store), now, |current| match current {
        None => (
            Update::Replace(Kind::String, value.to_vec()),
            Ok(value.len() as i64),
        ),
        Some(c) if c.value.kind() != Kind::String => (Update::Peek, Err(WRONGTYPE)),
        Some(c) => {
            let old = c.value.as_bytes();
            if old.len() + value.len() > MAX_STRING {
                return (Update::Peek, Err(STRING_TOO_LONG));
            }
            let mut new = Vec::with_capacity(old.len() + value.len());
            new.extend_from_slice(old);
            new.extend_from_slice(value);
            let len = new.len() as i64;
            (Update::Replace(Kind::String, new), Ok(len))
        }
    });
    reply_integer(result, out, |message| message)
}

/// What of a string of `len` bytes `GETRANGE` returns.
fn range(mut start: i64, mut end: i64, len: usize) -> Option<std::ops::Range<usize>> {
    let len = len as i64;
    if start < 0 && end < 0 && start > end {
        return None;
    }
    if start < 0 {
        start = start.saturating_add(len);
    }
    if end < 0 {
        end = end.saturating_add(len);
    }
    let start = start.max(0);
    let end = end.max(0).min(len - 1);
    if start > end || len == 0 {
        return None;
    }
    Some(start as usize..end as usize + 1)
}

/// `GETRANGE key start end`, and `SUBSTR`, its old name.
pub(super) fn getrange(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let (Some(start), Some(end)) = (resp::parse_int(args.get(2)), resp::parse_int(args.get(3)))
    else {
        resp::write_error(out, NOT_AN_INTEGER);
        return Reply::Written;
    };
    let write = |value: &[u8], out: &mut Vec<u8>| {
        let part = range(start, end, value.len()).map_or(&[][..], |range| &value[range]);
        resp::write_bulk(out, part);
    };
    let store = &*ctx.store;
    let (big, hit) = store.get_key(args.key(store), now, |value| match value {
        None => {
            write(&[], out);
            (None, false)
        }
        Some(v) if v.kind() != Kind::String => {
            resp::write_error(out, WRONGTYPE);
            (None, true)
        }
        Some(v) if v.as_bytes().len() <= COPY_UNDER_LOCK_MAX => {
            write(v.as_bytes(), out);
            (None, true)
        }
        Some(v) => (Some(v.share()), true),
    });
    session.count_lookup(hit);
    if let Some(value) = big {
        write(value.as_bytes(), out);
    }
    Reply::Written
}

/// `SETRANGE key offset value`: the new length.
pub(super) fn setrange(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let Some(offset) = resp::parse_int(args.get(2)) else {
        resp::write_error(out, NOT_AN_INTEGER);
        return Reply::Written;
    };
    let Ok(offset) = usize::try_from(offset) else {
        resp::write_error(out, b"ERR offset is out of range");
        return Reply::Written;
    };
    let store = &*ctx.store;
    let value = args.get(3);
    let too_long = offset
        .checked_add(value.len())
        .is_none_or(|total| total > MAX_STRING);
    let result = store.update_key(args.key(store), now, |current| {
        let old = match &current {
            None => &[][..],
            Some(c) if c.value.kind() != Kind::String => {
                return (Update::Peek, Err(WRONGTYPE));
            }
            Some(c) => c.value.as_bytes(),
        };
        // Nothing to set: a key is neither made nor changed.
        if value.is_empty() {
            return (Update::Peek, Ok(old.len() as i64));
        }
        if too_long {
            return (Update::Peek, Err(STRING_TOO_LONG));
        }
        let mut new = Vec::with_capacity(old.len().max(offset + value.len()));
        new.extend_from_slice(old);
        if new.len() < offset + value.len() {
            new.resize(offset + value.len(), 0);
        }
        new[offset..offset + value.len()].copy_from_slice(value);
        let len = new.len() as i64;
        (Update::Replace(Kind::String, new), Ok(len))
    });
    reply_integer(result, out, |message| message)
}

/// `GETSET key value`: as `SET key value GET`.
pub(super) fn getset(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = &*ctx.store;
    let result = store.set_key(
        args.key(store),
        args.get(2),
        Condition::Always,
        Expiry::Never,
        now,
        true,
    );
    let outcome = match result {
        Ok(outcome) => outcome,
        Err(OutOfMemory) => {
            resp::write_error(out, OOM);
            return Reply::Written;
        }
    };
    session.count_lookup(outcome.wrong_type || outcome.previous.is_some());
    if outcome.wrong_type {
        resp::write_error(out, WRONGTYPE);
        return Reply::Written;
    }
    match outcome.previous {
        Some(previous) => bulk(out, previous),
        None => {
            out.extend_from_slice(session.null());
            Reply::Written
        }
    }
}

/// `INCRBYFLOAT key increment`: the new value. Logged as what it set, as Redis
/// propagates it, so a restart elsewhere can't round it differently.
pub(super) fn incrbyfloat(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = &*ctx.store;
    let incr = args.get(2);
    let result = store.update_key(args.key(store), now, |current| {
        let old = match &current {
            None => None,
            Some(c) if c.value.kind() != Kind::String => {
                return (Update::Peek, Err(WRONGTYPE));
            }
            Some(c) => Some(c.value.as_bytes()),
        };
        match add_floats(old, incr) {
            Ok(sum) => (Update::Replace(Kind::String, sum.clone()), Ok(sum)),
            Err(FloatError::BadCurrent | FloatError::BadIncrement) => {
                (Update::Peek, Err(NOT_A_FLOAT))
            }
            Err(FloatError::IncrementNotFinite | FloatError::SumNotFinite) => {
                (Update::Peek, Err(NOT_FINITE))
            }
        }
    });
    match result {
        Ok(Ok(sum)) => resp::write_bulk(out, &sum),
        Ok(Err(message)) => resp::write_error(out, message),
        Err(OutOfMemory) => resp::write_error(out, OOM),
    }
    Reply::Written
}

/// `MSETNX key value [key value ...]`: 1 if none of the keys existed and all
/// were set, or else 0 with nothing set. Nothing else runs meanwhile. Logged
/// pair by pair, as `MSET` is.
pub(super) fn msetnx(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    if args.len().is_multiple_of(2) {
        super::wrong_arity(out, "msetnx");
        return Reply::Written;
    }
    let store = &*ctx.store;
    let keys: Vec<Key<'_>> = (1..args.len())
        .step_by(2)
        .map(|i| args.key_at(store, i))
        .collect();
    let logged = ctx.aof().is_some();
    store.exclusive(|| {
        let exists = |key: Key<'_>| {
            store
                .update_key(key, now, |current| (Update::Peek, current.is_some()))
                .unwrap_or(false)
        };
        if keys.iter().any(|&key| exists(key)) {
            resp::write_integer(out, 0);
            return;
        }
        for (i, &key) in keys.iter().enumerate() {
            let value = args.get(2 * i + 2);
            if store
                .set_key(key, value, Condition::Always, Expiry::Never, now, false)
                .is_err()
            {
                resp::write_error(out, OOM);
                return;
            }
            if logged {
                crate::aof::record_parts([&b"SET"[..], args.get(2 * i + 1), value].into_iter());
            }
        }
        resp::write_integer(out, 1);
    });
    Reply::Written
}

/// A string for `LCS`: empty if there's no key, `Err` if it isn't a string.
fn lcs_string(store: &Store, key: Key<'_>, now: Millis) -> Result<Vec<u8>, ()> {
    store.get_key(key, now, |value| match value {
        None => Ok(Vec::new()),
        Some(v) if v.kind() != Kind::String => Err(()),
        Some(v) => Ok(v.as_bytes().to_vec()),
    })
}

/// `LCS key1 key2 [LEN] [IDX] [MINMATCHLEN n] [WITHMATCHLEN]`: the longest
/// common subsequence, by the same table and walk as Redis, so the same ranges
/// in the same order.
pub(super) fn lcs(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = &*ctx.store;
    let (Ok(a), Ok(b)) = (
        lcs_string(store, args.key(store), now),
        lcs_string(store, args.key_at(store, 2), now),
    ) else {
        resp::write_error(out, b"ERR The specified keys must contain string values");
        return Reply::Written;
    };
    let (mut want_len, mut want_idx, mut with_match_len) = (false, false, false);
    let mut min_match = 0i64;
    let mut i = 3;
    while i < args.len() {
        let option = args.get(i);
        if option.eq_ignore_ascii_case(b"IDX") {
            want_idx = true;
        } else if option.eq_ignore_ascii_case(b"LEN") {
            want_len = true;
        } else if option.eq_ignore_ascii_case(b"WITHMATCHLEN") {
            with_match_len = true;
        } else if option.eq_ignore_ascii_case(b"MINMATCHLEN") && i + 1 < args.len() {
            i += 1;
            let Some(n) = resp::parse_int(args.get(i)) else {
                resp::write_error(out, NOT_AN_INTEGER);
                return Reply::Written;
            };
            min_match = n.max(0);
        } else {
            resp::write_error(out, SYNTAX_ERROR);
            return Reply::Written;
        }
        i += 1;
    }
    if want_idx && want_len {
        resp::write_error(
            out,
            b"ERR If you want both the length and indexes, please just use IDX.",
        );
        return Reply::Written;
    }
    if a.len() >= u32::MAX as usize - 1 || b.len() >= u32::MAX as usize - 1 {
        resp::write_error(out, b"ERR String too long for LCS");
        return Reply::Written;
    }
    let (rows, columns) = (a.len() + 1, b.len() + 1);
    let cells = rows as u128 * columns as u128;
    if cells * 4 > MAX_STRING as u128 {
        resp::write_error(
            out,
            b"ERR Insufficient memory, transient memory for LCS exceeds proto-max-bulk-len",
        );
        return Reply::Written;
    }
    // A cell is the length of the LCS of the first i bytes of a, j of b.
    let mut table = vec![0u32; cells as usize];
    for i in 1..rows {
        for j in 1..columns {
            table[i * columns + j] = if a[i - 1] == b[j - 1] {
                table[(i - 1) * columns + j - 1] + 1
            } else {
                table[(i - 1) * columns + j].max(table[i * columns + j - 1])
            };
        }
    }
    let total = table[a.len() * columns + b.len()];
    let mut result = vec![0; total as usize];
    let mut matches = Vec::new();
    let mut found = 0usize;
    if want_idx || !want_len {
        let (mut left, mut i, mut j) = (total as usize, a.len(), b.len());
        // `start_a == a.len()`: none being tracked.
        let (mut start_a, mut end_a, mut start_b, mut end_b) = (a.len(), 0, 0, 0);
        while i > 0 && j > 0 {
            let mut emit = false;
            if a[i - 1] == b[j - 1] {
                result[left - 1] = a[i - 1];
                if start_a == a.len() {
                    (start_a, end_a, start_b, end_b) = (i - 1, i - 1, j - 1, j - 1);
                } else if start_a == i && start_b == j {
                    start_a -= 1;
                    start_b -= 1;
                } else {
                    emit = true;
                }
                if start_a == 0 || start_b == 0 {
                    emit = true;
                }
                left -= 1;
                i -= 1;
                j -= 1;
            } else {
                if table[(i - 1) * columns + j] > table[i * columns + j - 1] {
                    i -= 1;
                } else {
                    j -= 1;
                }
                if start_a != a.len() {
                    emit = true;
                }
            }
            if emit {
                let length = end_a as i64 - start_a as i64 + 1;
                if want_idx && (min_match == 0 || length >= min_match) {
                    resp::write_array_header(&mut matches, 2 + usize::from(with_match_len));
                    resp::write_array_header(&mut matches, 2);
                    resp::write_integer(&mut matches, start_a as i64);
                    resp::write_integer(&mut matches, end_a as i64);
                    resp::write_array_header(&mut matches, 2);
                    resp::write_integer(&mut matches, start_b as i64);
                    resp::write_integer(&mut matches, end_b as i64);
                    if with_match_len {
                        resp::write_integer(&mut matches, length);
                    }
                    found += 1;
                }
                start_a = a.len();
            }
        }
    }
    if want_idx {
        if session.resp3 {
            out.extend_from_slice(b"%2\r\n");
        } else {
            out.extend_from_slice(b"*4\r\n");
        }
        resp::write_bulk(out, b"matches");
        resp::write_array_header(out, found);
        out.extend_from_slice(&matches);
        resp::write_bulk(out, b"len");
        resp::write_integer(out, i64::from(total));
    } else if want_len {
        resp::write_integer(out, i64::from(total));
    } else {
        resp::write_bulk(out, &result);
    }
    Reply::Written
}
