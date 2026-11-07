//! Set commands. Small sets are packed in their entry and decoded to
//! change; big ones are changed in place (see `crate::set`).

use std::cell::Cell;
use std::hash::BuildHasher;

use super::{
    Args, INVALID_CURSOR, NOT_AN_INTEGER, OOM, Reply, SYNTAX_ERROR, Scan, ScanOptions, WRONGTYPE,
    parse_cursor,
};
use crate::collection::Collection;
use crate::context::{Context, Session};
use crate::resp;
use crate::set::{Compact, Finished, SetMut, SetView};
use crate::store::{Entry, EntryValue, Key, Kind, Millis, OutOfMemory, Store, Update, View};

/// A random number below `n`, from a per-thread xorshift seeded at random.
pub(super) fn random_below(n: usize) -> usize {
    thread_local! {
        static STATE: Cell<u64> = Cell::new(foldhash::fast::RandomState::default().hash_one(0u64) | 1);
    }
    STATE.with(|state| {
        let mut x = state.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        state.set(x);
        (x % n as u64) as usize
    })
}

/// Reads the set at `key` under its shard's lock: `f` gets `None` if there
/// is none. `Err` if the key holds another kind.
fn read<R>(
    store: &Store,
    key: Key<'_>,
    now: Millis,
    session: &mut Session,
    f: impl FnOnce(Option<SetView<'_>>) -> R,
) -> Result<R, ()> {
    let (result, hit) = store.get_key(key, now, |value| match value {
        None => (Ok(f(None)), false),
        Some(v) if v.kind() != Kind::Set => (Err(()), true),
        Some(v) => (Ok(f(Some(view(v.view())))), true),
    });
    session.count_lookup(hit);
    result
}

fn view(value: View<'_>) -> SetView<'_> {
    match value {
        View::Bytes(bytes) => SetView::Compact(Compact::new(bytes)),
        View::Object(Collection::Set(set)) => SetView::Big(set),
        #[allow(unreachable_patterns)]
        View::Object(_) => unreachable!("a set's object is a set"),
    }
}

/// Changes the set at the key with `f`, which gets it as a [`SetMut`] (or
/// `None`, to create it if it adds members) and the arguments. Stores the
/// result as Redis would hold it: packed, big, or deleted when empty.
/// `Err(WRONGTYPE)` if the key holds another kind.
fn change<R>(
    store: &Store,
    key: Key<'_>,
    now: Millis,
    grows: bool,
    args: &Args<'_>,
    mut f: impl for<'v> FnMut(&mut Option<SetMut<'v>>, &Args<'v>) -> R,
) -> Result<Result<R, &'static [u8]>, OutOfMemory> {
    store.update_collection(key, now, grows, |entry| {
        let mut set = match set_of(entry) {
            Ok(set) => set,
            Err(()) => return (Update::Peek, Err(WRONGTYPE)),
        };
        let existed = set.is_some();
        let result = f(&mut set, args);
        (finish(set, existed), Ok(result))
    })
}

/// The set an entry holds; `Err` for another kind.
fn set_of(entry: Option<Entry<'_>>) -> Result<Option<SetMut<'_>>, ()> {
    match entry {
        None => Ok(None),
        Some(Entry {
            kind: Kind::Set,
            value,
            ..
        }) => Ok(Some(match value {
            EntryValue::Bytes(bytes) => SetMut::from_compact(Compact::new(bytes)),
            EntryValue::Object(Collection::Set(set)) => SetMut::big(set),
            #[allow(unreachable_patterns)]
            EntryValue::Object(_) => unreachable!("a set's object is a set"),
        })),
        Some(_) => Err(()),
    }
}

/// How to store a changed set.
fn finish(set: Option<SetMut<'_>>, existed: bool) -> Update {
    match set.and_then(SetMut::finish) {
        None if existed => Update::Touch,
        None => Update::Peek,
        Some(Finished::Packed(bytes)) => Update::Replace(Kind::Set, bytes),
        Some(Finished::Grown(set)) => Update::Object(Collection::Set(set)),
        Some(Finished::Changed) => Update::Changed,
        Some(Finished::Delete) => Update::Delete,
    }
}

/// Writes the error of a failed [`change`], returning the result if any.
fn done<R>(out: &mut Vec<u8>, result: Result<Result<R, &'static [u8]>, OutOfMemory>) -> Option<R> {
    match result {
        Ok(Ok(r)) => Some(r),
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

/// `SADD key member [member ...]`: how many are new.
pub(super) fn sadd(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = ctx.db(session);
    let added = change(store, args.key(store), now, true, args, |set, args| {
        let set = set.get_or_insert_with(|| SetMut::new(args.get(2), args.len() - 2));
        set.reserve(args.len() - 2);
        (2..args.len()).filter(|&i| set.insert(args.get(i))).count()
    });
    if let Some(added) = done(out, added) {
        resp::write_integer(out, added as i64);
    }
    Reply::Written
}

/// `SREM key member [member ...]`: how many it removed.
pub(super) fn srem(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = ctx.db(session);
    let removed = change(store, args.key(store), now, false, args, |set, args| {
        set.as_mut().map_or(0, |set| {
            (2..args.len()).filter(|&i| set.remove(args.get(i))).count()
        })
    });
    if let Some(removed) = done(out, removed) {
        resp::write_integer(out, removed as i64);
    }
    Reply::Written
}

/// Runs `f` on the set at the key, for read commands, writing `WRONGTYPE`
/// instead if it's another kind.
fn reading(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    f: impl FnOnce(Option<SetView<'_>>, &mut Vec<u8>),
) -> Reply {
    let store = ctx.db(session);
    if read(store, args.key(store), now, session, |set| f(set, out)).is_err() {
        resp::write_error(out, WRONGTYPE);
    }
    Reply::Written
}

/// `SISMEMBER key member`
pub(super) fn sismember(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    reading(ctx, session, now, args, out, |set, out| {
        let found = set.is_some_and(|set| set.contains(args.get(2)));
        resp::write_integer(out, i64::from(found));
    })
}

/// `SMISMEMBER key member [member ...]`
pub(super) fn smismember(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    reading(ctx, session, now, args, out, |set, out| {
        resp::write_array_header(out, args.len() - 2);
        for i in 2..args.len() {
            let found = set.is_some_and(|set| set.contains(args.get(i)));
            resp::write_integer(out, i64::from(found));
        }
    })
}

/// `SCARD key`
pub(super) fn scard(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    reading(ctx, session, now, args, out, |set, out| {
        resp::write_integer(out, set.map_or(0, |set| set.len()) as i64);
    })
}

/// `SMEMBERS key`: a set in RESP3.
pub(super) fn smembers(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let resp3 = session.resp3;
    reading(ctx, session, now, args, out, |set, out| match set {
        None => resp::write_set_header(out, 0, resp3),
        Some(set) => {
            resp::write_set_header(out, set.len(), resp3);
            set.for_each(|m| resp::write_bulk(out, m));
        }
    })
}

/// `SRANDMEMBER key [count]`: one random member, or `count` distinct ones
/// (every member, in order, if there aren't more), or with a negative
/// count, that many with repeats.
pub(super) fn srandmember(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    if args.len() > 3 {
        resp::write_error(out, SYNTAX_ERROR);
        return Reply::Written;
    }
    let null = session.null();
    if args.len() == 2 {
        return reading(ctx, session, now, args, out, |set, out| match set {
            Some(set) if !set.is_empty() => set
                .get(random_below(set.len()))
                .with_bytes(|m| resp::write_bulk(out, m)),
            _ => out.extend_from_slice(null),
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
    reading(ctx, session, now, args, out, |set, out| {
        let Some(set) = set.filter(|set| !set.is_empty() && count != 0) else {
            resp::write_array_header(out, 0);
            return;
        };
        let n = count.unsigned_abs() as usize;
        if count < 0 {
            resp::write_array_header(out, n);
            for _ in 0..n {
                set.get(random_below(set.len()))
                    .with_bytes(|m| resp::write_bulk(out, m));
            }
        } else if n >= set.len() {
            // All of them, in the order Redis' union gives them.
            let mut all = Vec::with_capacity(set.len());
            set.for_each(|m| all.push(m.to_vec()));
            let union = build(all.iter().map(Vec::as_slice));
            resp::write_array_header(out, union.len());
            union.for_each(|m| resp::write_bulk(out, m));
        } else {
            resp::write_array_header(out, n);
            for i in pick_distinct(set.len(), n) {
                set.get(i).with_bytes(|m| resp::write_bulk(out, m));
            }
        }
    })
}

/// `n` distinct random indexes below `len` (`n < len`).
fn pick_distinct(len: usize, n: usize) -> Vec<usize> {
    let mut picked = std::collections::HashSet::with_capacity(n);
    let mut order = Vec::with_capacity(n);
    while order.len() < n {
        let i = random_below(len);
        if picked.insert(i) {
            order.push(i);
        }
    }
    order
}

/// `SPOP key [count]`: removes and returns random members (every member,
/// in order, if there aren't more than `count`).
pub(super) fn spop(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    if args.len() > 3 {
        resp::write_error(out, SYNTAX_ERROR);
        return Reply::Written;
    }
    let store = ctx.db(session);
    let count = match args.len() {
        2 => None,
        _ => match resp::parse_int(args.get(2)) {
            Some(n) if n >= 0 => Some(n as usize),
            _ => {
                resp::write_error(out, b"ERR value is out of range, must be positive");
                return Reply::Written;
            }
        },
    };
    let popped = change(store, args.key(store), now, false, args, |set, _| {
        let Some(set) = set.as_mut().filter(|set| !set.is_empty()) else {
            return (Vec::new(), false);
        };
        let n = count.unwrap_or(1).min(set.len());
        if n == set.len() && count.is_some() {
            let mut all = Vec::with_capacity(n);
            set.for_each(|m| all.push(m.to_vec()));
            while !set.is_empty() {
                set.remove_index(set.len() - 1);
            }
            return (all, true);
        }
        let popped = (0..n).map(|_| set.remove_index(random_below(set.len())));
        (popped.collect(), false)
    });
    let Some((popped, all)) = done(out, popped) else {
        return Reply::Written;
    };
    match count {
        None => match popped.first() {
            Some(member) => resp::write_bulk(out, member),
            None => out.extend_from_slice(session.null()),
        },
        // All of them, as Redis' union replies them.
        Some(_) if all => write_union(out, popped.iter().map(Vec::as_slice), session.resp3),
        Some(_) => {
            resp::write_set_header(out, popped.len(), session.resp3);
            for member in &popped {
                resp::write_bulk(out, member);
            }
        }
    }
    Reply::Written
}

/// `SMOVE source destination member`, atomically.
pub(super) fn smove(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = ctx.db(session);
    let (from, to, member) = (args.key(store), args.key_at(store, 2), args.get(3));
    let same = args.get(1) == args.get(2);
    let moved = store.update_pair(from, to, now, false, |pair| {
        // In Redis' order: no source is 0, then either key's type.
        let source = pair.update(0, |entry| {
            let found = match &entry {
                None => None,
                Some(Entry {
                    kind: Kind::Set, ..
                }) => Some(()),
                Some(_) => return (Update::Peek, Err(WRONGTYPE)),
            };
            (Update::Peek, Ok(found))
        });
        match source {
            Err(e) => return Err(e),
            Ok(None) => return Ok(false),
            Ok(Some(())) => {}
        }
        let destination_ok = pair.update(1, |entry| {
            let ok = !matches!(&entry, Some(e) if e.kind != Kind::Set);
            (Update::Peek, ok)
        });
        if !destination_ok {
            return Err(WRONGTYPE);
        }
        if same {
            return Ok(pair.update(0, |entry| {
                let set = set_of(entry).expect("a set");
                (Update::Peek, set.is_some_and(|s| s.contains(member)))
            }));
        }
        let removed = pair.update(0, |entry| {
            let mut set = set_of(entry).expect("a set");
            let removed = set.as_mut().is_some_and(|s| s.remove(member));
            (finish(set, true), removed)
        });
        if removed {
            pair.update(1, |entry| {
                let mut set = set_of(entry).expect("a set");
                set.get_or_insert_with(|| SetMut::new(member, 1))
                    .insert(member);
                (finish(set, true), ())
            });
        }
        Ok(removed)
    });
    match moved {
        Ok(Ok(moved)) => resp::write_integer(out, i64::from(moved)),
        Ok(Err(msg)) => resp::write_error(out, msg),
        Err(OutOfMemory) => resp::write_error(out, OOM),
    }
    Reply::Written
}

/// A set read from one key: its members, copied out, in reply order.
struct Members(Vec<Vec<u8>>);

/// Reads every key's set, one key at a time: `None` for a missing key,
/// `Err` if one holds another kind.
fn read_all(
    store: &Store,
    now: Millis,
    session: &mut Session,
    args: &Args<'_>,
    keys: std::ops::Range<usize>,
) -> Result<Vec<Option<Members>>, ()> {
    keys.map(|i| {
        read(store, args.key_at(store, i), now, session, |set| {
            set.map(|set| {
                let mut members = Vec::with_capacity(set.len());
                set.for_each(|m| members.push(m.to_vec()));
                Members(members)
            })
        })
    })
    .collect()
}

/// The intersection, in the order of the smallest set's members (the
/// first of equal ones), as Redis replies it.
fn intersect(sets: Vec<Option<Members>>) -> Vec<Vec<u8>> {
    let Some(mut sets) = sets.into_iter().collect::<Option<Vec<_>>>() else {
        return Vec::new();
    };
    sets.sort_by_key(|set| set.0.len());
    let mut sets = sets.into_iter();
    let Some(Members(mut result)) = sets.next() else {
        return Vec::new();
    };
    for Members(other) in sets {
        let other: std::collections::HashSet<&[u8]> = other.iter().map(Vec::as_slice).collect();
        result.retain(|m| other.contains(&m[..]));
    }
    result
}

/// Members built into a set as Redis builds its result sets (from an empty
/// integer set): integers sorted while they all are, otherwise in order.
fn build<'a>(members: impl IntoIterator<Item = &'a [u8]>) -> SetMut<'a> {
    let mut set = SetMut::results();
    for member in members {
        set.insert(member);
    }
    set
}

/// Writes members as a set reply, in the order Redis' union gives them.
fn write_union<'a>(out: &mut Vec<u8>, members: impl IntoIterator<Item = &'a [u8]>, resp3: bool) {
    let set = build(members);
    resp::write_set_header(out, set.len(), resp3);
    set.for_each(|m| resp::write_bulk(out, m));
}

/// Set algebra: `SINTER`, `SUNION`, `SDIFF` and their `STORE` forms.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Op {
    Inter,
    Union,
    Diff,
}

fn combine(sets: Vec<Option<Members>>, op: Op) -> Vec<Vec<u8>> {
    match op {
        Op::Inter => intersect(sets),
        Op::Union => sets.into_iter().flatten().flat_map(|m| m.0).collect(),
        Op::Diff => {
            let mut sets = sets.into_iter();
            let Some(Some(Members(first))) = sets.next() else {
                return Vec::new();
            };
            let others: std::collections::HashSet<Vec<u8>> =
                sets.flatten().flat_map(|m| m.0).collect();
            first.into_iter().filter(|m| !others.contains(m)).collect()
        }
    }
}

fn algebra(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    op: Op,
) -> Reply {
    let store = ctx.db(session);
    let Ok(sets) = read_all(store, now, session, args, 1..args.len()) else {
        resp::write_error(out, WRONGTYPE);
        return Reply::Written;
    };
    let result = combine(sets, op);
    if op == Op::Inter {
        // Straight from the smallest set, as Redis replies it.
        resp::write_set_header(out, result.len(), session.resp3);
        result.iter().for_each(|m| resp::write_bulk(out, m));
        return Reply::Written;
    }
    write_union(out, result.iter().map(Vec::as_slice), session.resp3);
    Reply::Written
}

fn algebra_store(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    op: Op,
) -> Reply {
    let store = ctx.db(session);
    let Ok(sets) = read_all(store, now, session, args, 2..args.len()) else {
        resp::write_error(out, WRONGTYPE);
        return Reply::Written;
    };
    let result = combine(sets, op);
    // The destination is replaced whatever it held.
    let stored = store.update_collection(args.key(store), now, true, |_| {
        let set = build(result.iter().map(Vec::as_slice));
        let len = set.len();
        let update = match set.finish() {
            Some(Finished::Packed(bytes)) => Update::Replace(Kind::Set, bytes),
            Some(Finished::Grown(set)) => Update::Object(Collection::Set(set)),
            _ => Update::Delete,
        };
        (update, len)
    });
    match stored {
        Ok(len) => {
            store.forget_expiry(args.key(store), now);
            resp::write_integer(out, len as i64);
        }
        Err(OutOfMemory) => resp::write_error(out, OOM),
    }
    Reply::Written
}

pub(super) fn sinter(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    algebra(c, s, n, a, o, Op::Inter)
}

pub(super) fn sunion(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    algebra(c, s, n, a, o, Op::Union)
}

pub(super) fn sdiff(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    algebra(c, s, n, a, o, Op::Diff)
}

pub(super) fn sinterstore(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    algebra_store(c, s, n, a, o, Op::Inter)
}

pub(super) fn sunionstore(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    algebra_store(c, s, n, a, o, Op::Union)
}

pub(super) fn sdiffstore(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    algebra_store(c, s, n, a, o, Op::Diff)
}

/// `SINTERCARD numkeys key [key ...] [LIMIT limit]`
pub(super) fn sintercard(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let numkeys = match resp::parse_int(args.get(1)) {
        Some(n) if n > 0 => n as usize,
        _ => {
            resp::write_error(out, b"ERR numkeys should be greater than 0");
            return Reply::Written;
        }
    };
    if numkeys > args.len() - 2 {
        resp::write_error(
            out,
            b"ERR Number of keys can't be greater than number of args",
        );
        return Reply::Written;
    }
    let mut limit = 0;
    let mut i = 2 + numkeys;
    while i < args.len() {
        if args.get(i).eq_ignore_ascii_case(b"LIMIT") && i + 1 < args.len() {
            match resp::parse_int(args.get(i + 1)) {
                Some(n) if n >= 0 => limit = n as usize,
                _ => {
                    resp::write_error(out, b"ERR LIMIT can't be negative");
                    return Reply::Written;
                }
            }
            i += 2;
        } else {
            resp::write_error(out, SYNTAX_ERROR);
            return Reply::Written;
        }
    }
    let store = ctx.db(session);
    let Ok(sets) = read_all(store, now, session, args, 2..2 + numkeys) else {
        resp::write_error(out, WRONGTYPE);
        return Reply::Written;
    };
    let n = intersect(sets).len();
    resp::write_integer(out, if limit > 0 { n.min(limit) } else { n } as i64);
    Reply::Written
}

/// `SSCAN key cursor [MATCH pattern] [COUNT count]`. A packed set comes
/// whole, with cursor 0, as Redis does. A big one comes `COUNT` members
/// at a time from the end of its order: removing a member moves only the
/// last one, into its place, so nothing there all along is skipped.
pub(super) fn sscan(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let Some(cursor) = parse_cursor(args.get(2)) else {
        resp::write_error(out, INVALID_CURSOR);
        return Reply::Written;
    };
    reading(ctx, session, now, args, out, |set, out| {
        let Some(set) = set else {
            out.extend_from_slice(b"*2\r\n$1\r\n0\r\n*0\r\n");
            return;
        };
        let Some(options) = ScanOptions::parse(args, 3, Scan::Other, out) else {
            return;
        };
        let mut found = Vec::new();
        let mut keep = |m: &[u8]| {
            if options.matches(m) {
                found.push(m.to_vec());
            }
        };
        let next = match set {
            SetView::Compact(_) => {
                set.for_each(&mut keep);
                0
            }
            SetView::Big(big) => {
                let end = match cursor {
                    0 => big.len(),
                    c => usize::try_from(c).unwrap_or(usize::MAX).min(big.len()),
                };
                let start = end.saturating_sub(options.count);
                (start..end).for_each(|i| keep(big.get_index(i)));
                start as u64
            }
        };
        out.extend_from_slice(b"*2\r\n");
        resp::write_bulk(out, itoa::Buffer::new().format(next).as_bytes());
        resp::write_array_header(out, found.len());
        found.iter().for_each(|m| resp::write_bulk(out, m));
    })
}
