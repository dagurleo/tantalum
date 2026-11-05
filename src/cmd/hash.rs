//! Hash commands. A hash is one entry (see `crate::hash`), so reads scan it
//! and changes write it again whole.

use super::set::random_below;
use super::string::{FloatError, add_floats};
use super::{
    Args, INVALID_CURSOR, NOT_AN_INTEGER, OOM, Reply, SYNTAX_ERROR, Scan, ScanOptions, WRONGTYPE,
    parse_cursor, wrong_arity,
};
use crate::context::{Context, Session};
use crate::hash::{self, Edit};
use crate::resp;
use crate::store::{COPY_UNDER_LOCK_MAX, Kind, Millis, OutOfMemory, Store, Update};

/// Runs `f` on the hash at the key (`None` if there's none) to write the
/// reply: under the shard lock when the hash is small, and on a shared
/// reference after it when it's big. Replies `WRONGTYPE` to other kinds.
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
    let mut run = |hash: Option<&[u8]>, out: &mut Vec<u8>| f.take().expect("runs once")(hash, out);
    let (big, hit) = store.get_key(args.key(store), now, |value| match value {
        None => {
            run(None, out);
            (None, false)
        }
        Some(v) if v.kind() != Kind::Hash => {
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

/// Changes the hash at the key with `f`, creating it if there's none and
/// deleting it if it ends up empty. `f` gets an [`Edit`] of it, expecting
/// about `lookups` field lookups, the arguments and a buffer for formatting
/// numbers; an `Err` from `f` is the error reply and changes nothing. Writes
/// `WRONGTYPE` or the OOM error itself, returning `None`.
fn write<R>(
    store: &Store,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    lookups: usize,
    mut f: impl for<'v> FnMut(
        &mut Edit<'v>,
        &Args<'v>,
        &'v mut itoa::Buffer,
    ) -> Result<R, &'static [u8]>,
) -> Option<R> {
    let result = store.update_key(args.key(store), now, |current| {
        let old = match &current {
            None => None,
            Some(c) if c.value.kind() != Kind::Hash => return (Update::Peek, Err(WRONGTYPE)),
            Some(c) => Some(c.value.as_bytes()),
        };
        let mut digits = itoa::Buffer::new();
        let mut edit = Edit::new(old, lookups);
        let reply = match f(&mut edit, args, &mut digits) {
            Ok(reply) => reply,
            Err(msg) => return (Update::Peek, Err(msg)),
        };
        let update = if !edit.changed() {
            match current {
                Some(_) => Update::Touch,
                None => Update::Peek,
            }
        } else if edit.is_empty() {
            Update::Delete
        } else {
            Update::Replace(Kind::Hash, edit.encode())
        };
        (update, Ok(reply))
    });
    match result {
        Ok(Ok(reply)) => Some(reply),
        Ok(Err(msg)) => {
            resp::write_error(out, msg);
            None
        }
        Err(OutOfMemory) => {
            resp::write_error(out, OOM);
            None
        }
    }
}

/// `HSET key field value [field value ...]`: how many fields are new.
pub(super) fn hset(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    if let Some(added) = set_pairs(&ctx.store, now, args, out, "hset") {
        resp::write_integer(out, added as i64);
    }
    Reply::Written
}

/// `HMSET key field value [field value ...]`, the older `HSET`.
pub(super) fn hmset(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    if set_pairs(&ctx.store, now, args, out, "hmset").is_some() {
        out.extend_from_slice(resp::OK);
    }
    Reply::Written
}

fn set_pairs(
    store: &Store,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    name: &str,
) -> Option<usize> {
    if args.len() % 2 == 1 {
        wrong_arity(out, name);
        return None;
    }
    write(
        store,
        now,
        args,
        out,
        (args.len() - 2) / 2,
        |edit, args, _| {
            let mut added = 0;
            for i in (2..args.len()).step_by(2) {
                added += usize::from(edit.set(args.get(i), args.get(i + 1)));
            }
            Ok(added)
        },
    )
}

/// `HSETNX key field value`: 1 if it set the field, 0 if it was there.
pub(super) fn hsetnx(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let set = write(&ctx.store, now, args, out, 1, |edit, args, _| {
        Ok(edit.get(args.get(2)).is_none() && edit.set(args.get(2), args.get(3)))
    });
    if let Some(set) = set {
        resp::write_integer(out, i64::from(set));
    }
    Reply::Written
}

/// `HDEL key field [field ...]`: how many it removed.
pub(super) fn hdel(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let removed = write(
        &ctx.store,
        now,
        args,
        out,
        args.len() - 2,
        |edit, args, _| {
            Ok((2..args.len())
                .filter(|&i| edit.delete(args.get(i)))
                .count())
        },
    );
    if let Some(removed) = removed {
        resp::write_integer(out, removed as i64);
    }
    Reply::Written
}

/// `HINCRBY key field increment`: the new value.
pub(super) fn hincrby(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let Some(delta) = resp::parse_int(args.get(3)) else {
        resp::write_error(out, NOT_AN_INTEGER);
        return Reply::Written;
    };
    let value = write(&ctx.store, now, args, out, 1, |edit, args, digits| {
        let field = args.get(2);
        let old = match edit.get(field) {
            None => 0,
            Some(value) => {
                resp::parse_int(value).ok_or(&b"ERR hash value is not an integer"[..])?
            }
        };
        let new = old
            .checked_add(delta)
            .ok_or(&b"ERR increment or decrement would overflow"[..])?;
        edit.set(field, digits.format(new).as_bytes());
        Ok(new)
    });
    if let Some(value) = value {
        resp::write_integer(out, value);
    }
    Reply::Written
}

/// `HINCRBYFLOAT key field increment`: the new value. Logged as what it set
/// (see `aof::record`).
pub(super) fn hincrbyfloat(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let (field, incr) = (args.get(2), args.get(3));
    match add_floats(None, incr) {
        Err(FloatError::IncrementNotFinite) => {
            resp::write_error(out, b"ERR value is NaN or Infinity");
            return Reply::Written;
        }
        Err(_) => {
            resp::write_error(out, b"ERR value is not a valid float");
            return Reply::Written;
        }
        Ok(_) => {}
    }
    let store = &*ctx.store;
    let result = store.update_key(args.key(store), now, |current| {
        let old = match &current {
            None => None,
            Some(c) if c.value.kind() != Kind::Hash => return (Update::Peek, Err(WRONGTYPE)),
            Some(c) => Some(c.value.as_bytes()),
        };
        let sum = match add_floats(old.and_then(|hash| hash::get(hash, field)), incr) {
            Ok(sum) => sum,
            Err(FloatError::BadCurrent) => {
                return (Update::Peek, Err(&b"ERR hash value is not a float"[..]));
            }
            Err(_) => {
                return (
                    Update::Peek,
                    Err(&b"ERR increment would produce NaN or Infinity"[..]),
                );
            }
        };
        let encoded = {
            let mut edit = Edit::new(old, 1);
            edit.set(field, &sum);
            edit.encode()
        };
        (Update::Replace(Kind::Hash, encoded), Ok(sum))
    });
    match result {
        Ok(Ok(sum)) => resp::write_bulk(out, &sum),
        Ok(Err(message)) => resp::write_error(out, message),
        Err(OutOfMemory) => resp::write_error(out, OOM),
    }
    Reply::Written
}

/// `HRANDFIELD key [count [WITHVALUES]]`: a positive count asks for distinct
/// fields (all of them if there are fewer), a negative one allows repeats.
pub(super) fn hrandfield(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let null = session.null();
    if args.len() == 2 {
        return read(ctx, session, now, args, out, |hash, out| match hash {
            Some(hash) => {
                let pairs: Vec<_> = hash::iter(hash).collect();
                resp::write_bulk(out, pairs[random_below(pairs.len())].0);
            }
            None => out.extend_from_slice(null),
        });
    }
    let Some(count) = resp::parse_int(args.get(2)) else {
        resp::write_error(out, NOT_AN_INTEGER);
        return Reply::Written;
    };
    if count == i64::MIN {
        resp::write_error(
            out,
            b"ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807",
        );
        return Reply::Written;
    }
    let with_values = match args.len() {
        3 => false,
        4 if args.get(3).eq_ignore_ascii_case(b"WITHVALUES") => {
            if count.unsigned_abs() > i64::MAX as u64 / 2 {
                resp::write_error(out, b"ERR value is out of range");
                return Reply::Written;
            }
            true
        }
        _ => {
            resp::write_error(out, SYNTAX_ERROR);
            return Reply::Written;
        }
    };
    let resp3 = session.resp3;
    read(ctx, session, now, args, out, |hash, out| {
        let pairs: Vec<_> = hash.map_or_else(Vec::new, |hash| hash::iter(hash).collect());
        if pairs.is_empty() || count == 0 {
            resp::write_array_header(out, 0);
            return;
        }
        let n = count.unsigned_abs() as usize;
        let header = |out: &mut Vec<u8>, n: usize| {
            let flat = with_values && !resp3;
            resp::write_array_header(out, if flat { n * 2 } else { n });
        };
        let one = |out: &mut Vec<u8>, (field, value): (&[u8], &[u8])| {
            if with_values && resp3 {
                resp::write_array_header(out, 2);
            }
            resp::write_bulk(out, field);
            if with_values {
                resp::write_bulk(out, value);
            }
        };
        if count < 0 {
            header(out, n);
            for _ in 0..n {
                one(out, pairs[random_below(pairs.len())]);
            }
        } else {
            let n = n.min(pairs.len());
            let mut picks = pairs;
            header(out, n);
            // The first n of a shuffle.
            for i in 0..n {
                let j = i + random_below(picks.len() - i);
                picks.swap(i, j);
                one(out, picks[i]);
            }
        }
    })
}

/// `HGET key field`
pub(super) fn hget(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let null = session.null();
    read(ctx, session, now, args, out, |hash, out| {
        match hash.and_then(|hash| hash::get(hash, args.get(2))) {
            Some(value) => resp::write_bulk(out, value),
            None => out.extend_from_slice(null),
        }
    })
}

/// `HMGET key field [field ...]`
pub(super) fn hmget(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let null = session.null();
    read(ctx, session, now, args, out, |hash, out| {
        let fields = 2..args.len();
        resp::write_array_header(out, fields.len());
        // One pass over the hash for any number of fields, beyond a few.
        let found: Vec<Option<&[u8]>> = match hash {
            None => vec![None; fields.len()],
            Some(hash) if fields.len() > 8 => {
                let edit = Edit::new(Some(hash), fields.len());
                fields.map(|i| edit.get(args.get(i))).collect()
            }
            Some(hash) => fields.map(|i| hash::get(hash, args.get(i))).collect(),
        };
        for value in found {
            match value {
                Some(value) => resp::write_bulk(out, value),
                None => out.extend_from_slice(null),
            }
        }
    })
}

/// `HGETALL key`: a map in RESP3, a flat array in RESP2.
pub(super) fn hgetall(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let resp3 = session.resp3;
    read(ctx, session, now, args, out, |hash, out| {
        let Some(hash) = hash else {
            resp::write_map_header(out, 0, resp3);
            return;
        };
        resp::write_map_header(out, hash::len(hash), resp3);
        for (field, value) in hash::iter(hash) {
            resp::write_bulk(out, field);
            resp::write_bulk(out, value);
        }
    })
}

/// `HKEYS key`
pub(super) fn hkeys(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    read(ctx, session, now, args, out, |hash, out| {
        write_each(out, hash, |(field, _)| field)
    })
}

/// `HVALS key`
pub(super) fn hvals(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    read(ctx, session, now, args, out, |hash, out| {
        write_each(out, hash, |(_, value)| value)
    })
}

/// An array of one part of each field.
fn write_each<'a>(
    out: &mut Vec<u8>,
    hash: Option<&'a [u8]>,
    part: impl Fn((&'a [u8], &'a [u8])) -> &'a [u8],
) {
    let Some(hash) = hash else {
        resp::write_array_header(out, 0);
        return;
    };
    resp::write_array_header(out, hash::len(hash));
    for pair in hash::iter(hash) {
        resp::write_bulk(out, part(pair));
    }
}

/// `HLEN key`
pub(super) fn hlen(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    read(ctx, session, now, args, out, |hash, out| {
        resp::write_integer(out, hash.map_or(0, hash::len) as i64)
    })
}

/// `HEXISTS key field`
pub(super) fn hexists(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    read(ctx, session, now, args, out, |hash, out| {
        let found = hash.is_some_and(|hash| hash::get(hash, args.get(2)).is_some());
        resp::write_integer(out, i64::from(found))
    })
}

/// `HSTRLEN key field`
pub(super) fn hstrlen(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    read(ctx, session, now, args, out, |hash, out| {
        let len = hash
            .and_then(|hash| hash::get(hash, args.get(2)))
            .map_or(0, <[u8]>::len);
        resp::write_integer(out, len as i64)
    })
}

/// `HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]`. A hash
/// Redis keeps as a listpack comes whole, with cursor 0, as from Redis. One
/// it would keep as a hash table (over 128 fields, or a field or value over
/// 64 bytes) comes `COUNT` fields at a time, as Redis pages those, from the
/// end of the fields' order: removing a field keeps the others' order, so
/// nothing there all along is skipped.
pub(super) fn hscan(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    // In Redis' order: the cursor, then the key, then the options.
    let Some(cursor) = parse_cursor(args.get(2)) else {
        resp::write_error(out, INVALID_CURSOR);
        return Reply::Written;
    };
    read(ctx, session, now, args, out, |hash, out| {
        let Some(hash) = hash else {
            out.extend_from_slice(b"*2\r\n$1\r\n0\r\n*0\r\n");
            return;
        };
        let Some(options) = ScanOptions::parse(args, 3, Scan::Hash, out) else {
            return;
        };
        let fields: Vec<_> = hash::iter(hash).collect();
        let big = fields.len() > 128 || fields.iter().any(|(f, v)| f.len() > 64 || v.len() > 64);
        let (page, next) = if big {
            let end = match cursor {
                0 => fields.len(),
                c => usize::try_from(c).unwrap_or(usize::MAX).min(fields.len()),
            };
            let start = end.saturating_sub(options.count);
            (&fields[start..end], start)
        } else {
            (&fields[..], 0)
        };
        let found: Vec<_> = page
            .iter()
            .filter(|(field, _)| options.matches(field))
            .collect();
        out.extend_from_slice(b"*2\r\n");
        resp::write_bulk(out, itoa::Buffer::new().format(next).as_bytes());
        let per_field = if options.values { 2 } else { 1 };
        resp::write_array_header(out, found.len() * per_field);
        for (field, value) in found {
            resp::write_bulk(out, field);
            if options.values {
                resp::write_bulk(out, value);
            }
        }
    })
}
