//! Hash commands. A small hash is one entry, which reads scan and changes write
//! again whole; a big one is an object, changed in place (see `crate::hash`).

use std::collections::HashMap;

use super::set::random_below;
use super::string::{FloatError, add_floats};
use super::{
    Args, INVALID_CURSOR, NOT_AN_INTEGER, OOM, Reply, SYNTAX_ERROR, Scan, ScanOptions, WRONGTYPE,
    parse_cursor, wrong_arity,
};
use crate::collection::Collection;
use crate::context::{Context, Session};
use crate::hash::{self, BigHash, Edit, HashEdit, HashView};
use crate::resp;
use crate::store::{Entry, EntryValue, Kind, Millis, OutOfMemory, Store, Update, View};

/// The hash a value of its kind holds.
pub(super) fn view(value: View<'_>) -> HashView<'_> {
    match value {
        View::Bytes(bytes) => HashView::Packed(bytes),
        View::Object(Collection::Hash(hash)) => HashView::Big(hash),
        View::Object(_) => unreachable!("a hash's object is a hash"),
    }
}

/// Runs `f` on the hash at the key (`None` if there's none) to write the
/// reply, under the shard lock. Replies `WRONGTYPE` to other kinds.
fn read(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    f: impl FnOnce(Option<HashView<'_>>, &mut Vec<u8>),
) -> Reply {
    let store = ctx.db(session);
    let mut f = Some(f);
    let mut run =
        |hash: Option<HashView<'_>>, out: &mut Vec<u8>| f.take().expect("runs once")(hash, out);
    let hit = store.get_key(args.key(store), now, |value| match value {
        None => {
            run(None, out);
            false
        }
        Some(v) if v.kind() != Kind::Hash => {
            resp::write_error(out, WRONGTYPE);
            true
        }
        Some(v) => {
            run(Some(view(v.view())), out);
            true
        }
    });
    session.count_lookup(hit);
    Reply::Written
}

/// How to store a hash after an edit: packed, big, or deleted when empty.
#[inline]
fn finish(edit: HashEdit<'_, '_>, existed: bool) -> Update {
    let unchanged = || if existed { Update::Touch } else { Update::Peek };
    match edit {
        HashEdit::Big { changed: false, .. } => unchanged(),
        HashEdit::Big { hash, .. } if hash.is_empty() => Update::Delete,
        HashEdit::Big { .. } => Update::Changed,
        HashEdit::Packed(edit) if !edit.changed() => unchanged(),
        HashEdit::Packed(edit) if edit.is_empty() => Update::Delete,
        HashEdit::Packed(edit) => {
            let encoded = edit.encode();
            if hash::fits_packed(&encoded) {
                Update::Replace(Kind::Hash, encoded)
            } else {
                Update::Object(Collection::Hash(BigHash::from_packed(&encoded)))
            }
        }
    }
}

/// Changes the hash at the key with `f`, creating it if there's none and
/// deleting it if it ends up empty. `f` gets a [`HashEdit`] of it, expecting
/// about `lookups` field lookups, the arguments and a buffer for formatting
/// numbers; an `Err` from `f`, before it has changed anything, is the error
/// reply. With `grows` (commands that may add data) it may refuse with the OOM
/// error. Writes `WRONGTYPE` or the OOM error itself, returning `None`.
fn write<R>(
    store: &Store,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    lookups: usize,
    grows: bool,
    f: impl for<'v> FnMut(
        &mut HashEdit<'_, 'v>,
        &Args<'v>,
        &'v mut itoa::Buffer,
    ) -> Result<R, &'static [u8]>,
) -> Option<R> {
    write_with(store, now, args, out, lookups, grows, f)
}

/// [`write`] with a `B` for what `f` works out to put in the hash, there until the
/// hash is stored.
fn write_with<B: Default, R>(
    store: &Store,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    lookups: usize,
    grows: bool,
    mut f: impl for<'v> FnMut(&mut HashEdit<'_, 'v>, &Args<'v>, &'v mut B) -> Result<R, &'static [u8]>,
) -> Option<R> {
    let result = store.update_collection(args.key(store), now, grows, |entry| {
        let existed = entry.is_some();
        let mut scratch = B::default();
        let mut packed;
        let mut edit = match entry {
            None => {
                packed = Edit::new(None, lookups);
                HashEdit::Packed(&mut packed)
            }
            Some(Entry {
                kind: Kind::Hash,
                value,
                ..
            }) => match value {
                EntryValue::Bytes(bytes) => {
                    packed = Edit::new(Some(bytes), lookups);
                    HashEdit::Packed(&mut packed)
                }
                EntryValue::Object(Collection::Hash(hash)) => HashEdit::Big {
                    hash,
                    changed: false,
                },
                EntryValue::Object(_) => unreachable!("a hash's object is a hash"),
            },
            Some(_) => return (Update::Peek, Err(WRONGTYPE)),
        };
        match f(&mut edit, args, &mut scratch) {
            Ok(reply) => (finish(edit, existed), Ok(reply)),
            Err(msg) => (Update::Peek, Err(msg)),
        }
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
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    if let Some(added) = set_pairs(ctx.db(session), now, args, out, "hset") {
        resp::write_integer(out, added as i64);
    }
    Reply::Written
}

/// `HMSET key field value [field value ...]`, the older `HSET`.
pub(super) fn hmset(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    if set_pairs(ctx.db(session), now, args, out, "hmset").is_some() {
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
        true,
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
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let set = write(ctx.db(session), now, args, out, 1, true, |edit, args, _| {
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
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let removed = write(
        ctx.db(session),
        now,
        args,
        out,
        args.len() - 2,
        false,
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
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let Some(delta) = resp::parse_int(args.get(3)) else {
        resp::write_error(out, NOT_AN_INTEGER);
        return Reply::Written;
    };
    let value = write(
        ctx.db(session),
        now,
        args,
        out,
        1,
        true,
        |edit, args, digits| {
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
        },
    );
    if let Some(value) = value {
        resp::write_integer(out, value);
    }
    Reply::Written
}

/// `HINCRBYFLOAT key field increment`: the new value. Logged as what it set
/// (see `aof::record`).
pub(super) fn hincrbyfloat(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    match add_floats(None, args.get(3)) {
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
    let reply = write_with::<Vec<u8>, _>(
        ctx.db(session),
        now,
        args,
        out,
        1,
        true,
        |edit, args, sum| {
            let field = args.get(2);
            *sum = add_floats(edit.get(field), args.get(3)).map_err(|e| match e {
                FloatError::BadCurrent => &b"ERR hash value is not a float"[..],
                _ => &b"ERR increment would produce NaN or Infinity"[..],
            })?;
            let sum: &[u8] = sum;
            edit.set(field, sum);
            Ok(sum.to_vec())
        },
    );
    if let Some(sum) = reply {
        resp::write_bulk(out, &sum);
    }
    Reply::Written
}

/// A hash's fields by position: what a packed hash can't do without a list of them.
enum Fields<'a> {
    Packed(Vec<(&'a [u8], &'a [u8])>),
    Big(&'a BigHash),
}

impl<'a> Fields<'a> {
    fn new(hash: HashView<'a>) -> Self {
        match hash {
            HashView::Packed(bytes) => Fields::Packed(hash::iter(bytes).collect()),
            HashView::Big(hash) => Fields::Big(hash),
        }
    }

    fn len(&self) -> usize {
        match self {
            Fields::Packed(pairs) => pairs.len(),
            Fields::Big(hash) => hash.len(),
        }
    }

    fn at(&self, i: usize) -> (&'a [u8], &'a [u8]) {
        match self {
            Fields::Packed(pairs) => pairs[i],
            Fields::Big(hash) => hash.get_index(i),
        }
    }
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
                let fields = Fields::new(hash);
                resp::write_bulk(out, fields.at(random_below(fields.len())).0);
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
        let Some(fields) = hash.map(Fields::new) else {
            resp::write_array_header(out, 0);
            return;
        };
        if count == 0 {
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
                one(out, fields.at(random_below(fields.len())));
            }
        } else {
            let n = n.min(fields.len());
            header(out, n);
            // The first n of a shuffle, noting only what's moved: a big hash
            // needn't be listed for a few of its fields.
            let mut moved = HashMap::new();
            for i in 0..n {
                let j = i + random_below(fields.len() - i);
                let pick = moved.get(&j).copied().unwrap_or(j);
                moved.insert(j, moved.get(&i).copied().unwrap_or(i));
                one(out, fields.at(pick));
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
        match hash.and_then(|hash| hash.get(args.get(2))) {
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
        // One pass over a packed hash for any number of fields, beyond a few.
        let found: Vec<Option<&[u8]>> = match hash {
            None => vec![None; fields.len()],
            Some(HashView::Packed(bytes)) if fields.len() > 8 => {
                let edit = Edit::new(Some(bytes), fields.len());
                fields.map(|i| edit.get(args.get(i))).collect()
            }
            Some(hash) => fields.map(|i| hash.get(args.get(i))).collect(),
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
        resp::write_map_header(out, hash.len(), resp3);
        for (field, value) in hash.iter() {
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
    hash: Option<HashView<'a>>,
    part: impl Fn((&'a [u8], &'a [u8])) -> &'a [u8],
) {
    let Some(hash) = hash else {
        resp::write_array_header(out, 0);
        return;
    };
    resp::write_array_header(out, hash.len());
    for pair in hash.iter() {
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
        resp::write_integer(out, hash.map_or(0, HashView::len) as i64)
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
        let found = hash.is_some_and(|hash| hash.get(args.get(2)).is_some());
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
            .and_then(|hash| hash.get(args.get(2)))
            .map_or(0, <[u8]>::len);
        resp::write_integer(out, len as i64)
    })
}

/// `HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]`. A hash
/// Redis keeps as a listpack comes whole, with cursor 0, as from Redis. One
/// it would keep as a hash table (over 512 fields, or a field or value over
/// 64 bytes) comes `COUNT` fields at a time, as Redis pages those, from the
/// end of the fields' order: removing a field moves the others up, or the last
/// into its place, so nothing there all along is skipped.
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
        let fields = Fields::new(hash);
        let (start, end, next) = if hash.fits_listpack() {
            (0, fields.len(), 0)
        } else {
            let end = match cursor {
                0 => fields.len(),
                c => usize::try_from(c).unwrap_or(usize::MAX).min(fields.len()),
            };
            let start = end.saturating_sub(options.count);
            (start, end, start)
        };
        let found: Vec<_> = (start..end)
            .map(|i| fields.at(i))
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
