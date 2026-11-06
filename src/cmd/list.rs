//! List commands. Each checks its arguments and the key in Redis' order,
//! which differs from command to command.

use super::{Args, NOT_AN_INTEGER, OOM, Reply, SYNTAX_ERROR, WRONGTYPE};
use crate::blocking::{self, Attempt};
use crate::collection::Collection;
use crate::context::{Context, Session};
use crate::list::{Finished, ListMut, ListView, Packed};
use crate::resp;
use crate::store::{Entry, EntryValue, Key, Kind, Millis, Store, Update, View};

const NO_SUCH_KEY: &[u8] = b"ERR no such key";
const MUST_BE_POSITIVE: &[u8] = b"ERR value is out of range, must be positive";

fn view(value: View<'_>) -> ListView<'_> {
    match value {
        View::Bytes(bytes) => ListView::Packed(Packed::new(bytes)),
        View::Object(Collection::List(list)) => ListView::Big(list),
        View::Object(_) => unreachable!("a list's object is a list"),
    }
}

/// The list an entry holds; `Err` for another kind.
fn list_of(entry: Option<Entry<'_>>) -> Result<Option<ListMut<'_>>, ()> {
    match entry {
        None => Ok(None),
        Some(Entry {
            kind: Kind::List,
            value,
            ..
        }) => Ok(Some(match value {
            EntryValue::Bytes(bytes) => ListMut::from_packed(bytes),
            EntryValue::Object(Collection::List(list)) => ListMut::big(list),
            EntryValue::Object(_) => unreachable!("a list's object is a list"),
        })),
        Some(_) => Err(()),
    }
}

fn finish(list: Option<ListMut<'_>>, existed: bool) -> Update {
    match list.and_then(ListMut::finish) {
        None if existed => Update::Touch,
        None => Update::Peek,
        Some(Finished::Packed(bytes)) => Update::Replace(Kind::List, bytes),
        Some(Finished::Grown(list)) => Update::Object(Collection::List(list)),
        Some(Finished::Changed) => Update::Changed,
        Some(Finished::Delete) => Update::Delete,
    }
}

/// Reads the list at `key` under its lock: `f` gets `None` if there's
/// none. `Err` if the key holds another kind.
fn read<R>(
    store: &Store,
    key: Key<'_>,
    now: Millis,
    session: &mut Session,
    f: impl FnOnce(Option<ListView<'_>>) -> R,
) -> Result<R, ()> {
    let (result, hit) = store.get_key(key, now, |value| match value {
        None => (Ok(f(None)), false),
        Some(v) if v.kind() != Kind::List => (Err(()), true),
        Some(v) => (Ok(f(Some(view(v.view())))), true),
    });
    session.count_lookup(hit);
    result
}

/// Runs `f` on the list at the key for a read command; `WRONGTYPE` instead
/// if it's another kind.
fn reading(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    f: impl FnOnce(Option<ListView<'_>>, &mut Vec<u8>),
) -> Reply {
    let store = ctx.db(session);
    let mut reply = Vec::new();
    match read(store, args.key(store), now, session, |list| {
        f(list, &mut reply)
    }) {
        Ok(()) => out.extend_from_slice(&reply),
        Err(()) => resp::write_error(out, WRONGTYPE),
    }
    Reply::Written
}

/// Changes the list at the key with `f` (`None` if there's none: `f` may
/// create one), storing the result packed, big, or deleted when empty.
/// `Err` (the error to reply) if the key holds another kind, or from `f`.
fn change<R>(
    store: &Store,
    key: Key<'_>,
    now: Millis,
    grows: bool,
    args: &Args<'_>,
    mut f: impl for<'v> FnMut(&mut Option<ListMut<'v>>, &Args<'v>) -> Result<R, &'static [u8]>,
) -> Result<R, &'static [u8]> {
    let result = store.update_collection(key, now, grows, |entry| {
        let Ok(mut list) = list_of(entry) else {
            return (Update::Peek, Err(WRONGTYPE));
        };
        let existed = list.is_some();
        match f(&mut list, args) {
            Ok(r) => (finish(list, existed), Ok(r)),
            Err(msg) => (Update::Peek, Err(msg)),
        }
    });
    result.unwrap_or(Err(OOM))
}

fn reply_error(out: &mut Vec<u8>, msg: &[u8]) -> Reply {
    resp::write_error(out, msg);
    Reply::Written
}

/// Redis' range of `start..=stop` in a list of `len`, negative ones from
/// the end: empty if it's outside.
fn range(start: i64, stop: i64, len: usize) -> std::ops::Range<usize> {
    let len = len as i64;
    let start = if start < 0 {
        (len + start).max(0)
    } else {
        start
    };
    let stop = if stop < 0 {
        len + stop
    } else {
        stop.min(len - 1)
    };
    if start > stop || start >= len {
        0..0
    } else {
        start as usize..stop as usize + 1
    }
}

/// `index` (negative from the end) in a list of `len`, if in range.
fn index(index: i64, len: usize) -> Option<usize> {
    let i = if index < 0 { len as i64 + index } else { index };
    (0..len as i64).contains(&i).then_some(i as usize)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum End {
    Left,
    Right,
}

fn end_of(arg: &[u8]) -> Option<End> {
    if arg.eq_ignore_ascii_case(b"LEFT") {
        Some(End::Left)
    } else if arg.eq_ignore_ascii_case(b"RIGHT") {
        Some(End::Right)
    } else {
        None
    }
}

fn push(list: &mut ListMut<'_>, end: End, element: &[u8]) {
    match end {
        End::Left => list.edit().push_front(element),
        End::Right => list.edit().push_back(element),
    }
}

fn pop(list: &mut ListMut<'_>, end: End) -> Option<Vec<u8>> {
    if list.is_empty() {
        return None;
    }
    match end {
        End::Left => list.edit().pop_front(),
        End::Right => list.edit().pop_back(),
    }
}

/// `LPUSH`/`RPUSH` (and with `only_existing`, `LPUSHX`/`RPUSHX`).
fn push_command(
    ctx: &Context,
    session: &Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    end: End,
    only_existing: bool,
) -> Reply {
    let store = ctx.db(session);
    let len = change(store, args.key(store), now, true, args, |list, args| {
        if list.is_none() && only_existing {
            return Ok(0);
        }
        let list = list.get_or_insert_with(ListMut::new);
        for i in 2..args.len() {
            push(list, end, args.get(i));
        }
        Ok(list.len())
    });
    match len {
        Ok(len) => resp::write_integer(out, len as i64),
        Err(msg) => resp::write_error(out, msg),
    }
    Reply::Written
}

pub(super) fn lpush(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    push_command(c, s, n, a, o, End::Left, false)
}

pub(super) fn rpush(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    push_command(c, s, n, a, o, End::Right, false)
}

pub(super) fn lpushx(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    push_command(c, s, n, a, o, End::Left, true)
}

pub(super) fn rpushx(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    push_command(c, s, n, a, o, End::Right, true)
}

/// `LPOP`/`RPOP key [count]`
fn pop_command(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    end: End,
) -> Reply {
    if args.len() > 3 {
        super::wrong_arity(out, if end == End::Left { "lpop" } else { "rpop" });
        return Reply::Written;
    }
    let count = match args.len() {
        3 => match resp::parse_int(args.get(2)) {
            Some(n) if n >= 0 => Some(n as usize),
            _ => return reply_error(out, MUST_BE_POSITIVE),
        },
        _ => None,
    };
    let store = ctx.db(session);
    let popped = change(store, args.key(store), now, false, args, |list, _| {
        let Some(list) = list else {
            return Ok(None);
        };
        let n = count.unwrap_or(1).min(list.len());
        Ok(Some(
            (0..n).filter_map(|_| pop(list, end)).collect::<Vec<_>>(),
        ))
    });
    let popped = match popped {
        Ok(popped) => popped,
        Err(msg) => return reply_error(out, msg),
    };
    match (count, popped) {
        (None, Some(popped)) if !popped.is_empty() => resp::write_bulk(out, &popped[0]),
        (None, _) => out.extend_from_slice(session.null()),
        (Some(_), None) => out.extend_from_slice(null_array(session)),
        (Some(_), Some(popped)) => {
            resp::write_array_header(out, popped.len());
            popped.iter().for_each(|e| resp::write_bulk(out, e));
        }
    }
    Reply::Written
}

fn null_array(session: &Session) -> &'static [u8] {
    if session.resp3 {
        resp::NULL
    } else {
        b"*-1\r\n"
    }
}

pub(super) fn lpop(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    pop_command(c, s, n, a, o, End::Left)
}

pub(super) fn rpop(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    pop_command(c, s, n, a, o, End::Right)
}

/// `LLEN key`
pub(super) fn llen(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    reading(ctx, session, now, args, out, |list, out| {
        resp::write_integer(out, list.map_or(0, |l| l.len()) as i64)
    })
}

/// `LRANGE key start stop`
pub(super) fn lrange(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let (Some(start), Some(stop)) = (resp::parse_int(args.get(2)), resp::parse_int(args.get(3)))
    else {
        return reply_error(out, NOT_AN_INTEGER);
    };
    reading(ctx, session, now, args, out, |list, out| {
        let Some(list) = list else {
            resp::write_array_header(out, 0);
            return;
        };
        let range = range(start, stop, list.len());
        resp::write_array_header(out, range.len());
        list.for_range(range.start, range.end, |e| resp::write_bulk(out, e));
    })
}

/// `LINDEX key index`: the key first, then the index, as in Redis.
pub(super) fn lindex(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let null = session.null();
    reading(ctx, session, now, args, out, |list, out| {
        let Some(list) = list else {
            return out.extend_from_slice(null);
        };
        let Some(i) = resp::parse_int(args.get(2)) else {
            return resp::write_error(out, NOT_AN_INTEGER);
        };
        match index(i, list.len()).and_then(|i| list.get(i)) {
            Some(e) => resp::write_bulk(out, e),
            None => out.extend_from_slice(null),
        }
    })
}

/// `LSET key index element`: the key first, then the index.
pub(super) fn lset(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = ctx.db(session);
    let set = change(store, args.key(store), now, true, args, |list, args| {
        let Some(list) = list else {
            return Err(NO_SUCH_KEY);
        };
        let Some(i) = resp::parse_int(args.get(2)) else {
            return Err(NOT_AN_INTEGER);
        };
        let Some(i) = index(i, list.len()) else {
            return Err(b"ERR index out of range");
        };
        list.edit().set(i, args.get(3));
        Ok(())
    });
    match set {
        Ok(()) => out.extend_from_slice(resp::OK),
        Err(msg) => resp::write_error(out, msg),
    }
    Reply::Written
}

/// `LINSERT key BEFORE|AFTER pivot element`: the new length, -1 if there's
/// no pivot, 0 if there's no list.
pub(super) fn linsert(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let where_ = args.get(2);
    let after = if where_.eq_ignore_ascii_case(b"AFTER") {
        true
    } else if where_.eq_ignore_ascii_case(b"BEFORE") {
        false
    } else {
        return reply_error(out, SYNTAX_ERROR);
    };
    let store = ctx.db(session);
    let len = change(store, args.key(store), now, true, args, |list, args| {
        let Some(list) = list else {
            return Ok(0);
        };
        let pivot = args.get(3);
        let Some(at) = list.list().iter().position(|e| e == pivot) else {
            return Ok(-1);
        };
        list.edit().insert(at + usize::from(after), args.get(4));
        Ok(list.len() as i64)
    });
    match len {
        Ok(len) => resp::write_integer(out, len),
        Err(msg) => resp::write_error(out, msg),
    }
    Reply::Written
}

/// `LREM key count element`
pub(super) fn lrem(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let Some(count) = resp::parse_int(args.get(2)) else {
        return reply_error(out, NOT_AN_INTEGER);
    };
    let store = ctx.db(session);
    let removed = change(store, args.key(store), now, false, args, |list, args| {
        let Some(list) = list else {
            return Ok(0);
        };
        let value = args.get(3);
        if !list.list().iter().any(|e| e == value) {
            return Ok(0);
        }
        let n = count.unsigned_abs() as usize;
        Ok(list.edit().remove_matching(value, n, count < 0))
    });
    match removed {
        Ok(n) => resp::write_integer(out, n as i64),
        Err(msg) => resp::write_error(out, msg),
    }
    Reply::Written
}

/// `LTRIM key start stop`
pub(super) fn ltrim(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let (Some(start), Some(stop)) = (resp::parse_int(args.get(2)), resp::parse_int(args.get(3)))
    else {
        return reply_error(out, NOT_AN_INTEGER);
    };
    let store = ctx.db(session);
    let trimmed = change(store, args.key(store), now, false, args, |list, _| {
        if let Some(list) = list {
            let range = range(start, stop, list.len());
            // A change even if nothing goes, as in Redis (WATCH can tell).
            let edit = list.edit();
            if range.len() != edit.len() {
                edit.trim(range.start, range.end);
            }
        }
        Ok(())
    });
    match trimmed {
        Ok(()) => out.extend_from_slice(resp::OK),
        Err(msg) => resp::write_error(out, msg),
    }
    Reply::Written
}

/// `LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]`
pub(super) fn lpos(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let (mut rank, mut count, mut maxlen) = (1i64, None, 0usize);
    let mut i = 3;
    while i < args.len() {
        let opt = args.get(i);
        let is = |name: &[u8]| opt.eq_ignore_ascii_case(name);
        let value = (i + 1 < args.len()).then(|| resp::parse_int(args.get(i + 1)));
        match value {
            Some(n) if is(b"RANK") => match n {
                None => return reply_error(out, NOT_AN_INTEGER),
                Some(i64::MIN) => {
                    return reply_error(
                        out,
                        b"ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807",
                    );
                }
                Some(0) => {
                    return reply_error(
                        out,
                        b"ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list",
                    );
                }
                Some(n) => rank = n,
            },
            Some(n) if is(b"COUNT") => match n {
                Some(n) if n >= 0 => count = Some(n as usize),
                _ => return reply_error(out, b"ERR COUNT can't be negative"),
            },
            Some(n) if is(b"MAXLEN") => match n {
                Some(n) if n >= 0 => maxlen = n as usize,
                _ => return reply_error(out, b"ERR MAXLEN can't be negative"),
            },
            _ => return reply_error(out, SYNTAX_ERROR),
        }
        i += 2;
    }
    let null = session.null();
    reading(ctx, session, now, args, out, |list, out| {
        let mut found = Vec::new();
        if let Some(list) = list {
            let element = args.get(2);
            let mut skip = rank.unsigned_abs() - 1;
            let wanted = match count {
                None => 1,
                Some(0) => usize::MAX,
                Some(n) => n,
            };
            let mut compared = 0;
            list.search(rank < 0, |i, e| {
                if maxlen != 0 && compared == maxlen {
                    return false;
                }
                compared += 1;
                if e == element {
                    if skip > 0 {
                        skip -= 1;
                    } else {
                        found.push(i);
                    }
                }
                found.len() < wanted
            });
        }
        match count {
            None => match found.first() {
                Some(&i) => resp::write_integer(out, i as i64),
                None => out.extend_from_slice(null),
            },
            Some(_) => {
                resp::write_array_header(out, found.len());
                found
                    .iter()
                    .for_each(|&i| resp::write_integer(out, i as i64));
            }
        }
    })
}

/// `LMOVE source destination LEFT|RIGHT LEFT|RIGHT`, atomically: the
/// element moved, or nil if the source is empty.
pub(super) fn lmove(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let (Some(from), Some(to)) = (end_of(args.get(3)), end_of(args.get(4))) else {
        return reply_error(out, SYNTAX_ERROR);
    };
    match move_element(ctx, session, now, args, from, to) {
        Ok(Some(element)) => resp::write_bulk(out, &element),
        Ok(None) => out.extend_from_slice(session.null()),
        Err(msg) => resp::write_error(out, msg),
    }
    Reply::Written
}

/// `RPOPLPUSH source destination`: `LMOVE source destination RIGHT LEFT`.
pub(super) fn rpoplpush(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    match move_element(ctx, session, now, args, End::Right, End::Left) {
        Ok(Some(element)) => resp::write_bulk(out, &element),
        Ok(None) => out.extend_from_slice(session.null()),
        Err(msg) => resp::write_error(out, msg),
    }
    Reply::Written
}

/// Moves an element from the list at argument 1 to the one at argument 2:
/// the element, `None` if the source is empty, or the error to reply.
fn move_element(
    ctx: &Context,
    session: &Session,
    now: Millis,
    args: &Args<'_>,
    from: End,
    to: End,
) -> Result<Option<Vec<u8>>, &'static [u8]> {
    let store = ctx.db(session);
    let moved = store.update_pair(args.key(store), args.key_at(store, 2), now, true, |pair| {
        // In Redis' order: the source's type, its emptiness, then the
        // destination's type.
        let has_source = pair.update(0, |entry| match entry {
            None => (Update::Peek, Ok(false)),
            Some(e) if e.kind != Kind::List => (Update::Peek, Err(WRONGTYPE)),
            Some(_) => (Update::Peek, Ok(true)),
        })?;
        if !has_source {
            return Ok(None);
        }
        let destination_ok = pair.update(1, |entry| {
            (
                Update::Peek,
                !matches!(&entry, Some(e) if e.kind != Kind::List),
            )
        });
        if !destination_ok {
            return Err(WRONGTYPE);
        }
        let element = pair.update(0, |entry| {
            let mut list = list_of(entry).expect("a list");
            let element = list.as_mut().and_then(|l| pop(l, from));
            (finish(list, true), element)
        });
        if let Some(element) = &element {
            pair.update(1, |entry| {
                let mut list = list_of(entry).expect("a list");
                push(list.get_or_insert_with(ListMut::new), to, element);
                (finish(list, true), ())
            });
        }
        Ok(element)
    });
    moved.unwrap_or(Err(OOM))
}

/// `BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout`
pub(super) fn blmove(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    blocking::block(c, s, n, a, o, |ctx, session, now, args, out| {
        let (Some(from), Some(to)) = (end_of(args.get(3)), end_of(args.get(4))) else {
            resp::write_error(out, SYNTAX_ERROR);
            return Attempt::Done;
        };
        blocking_move(ctx, session, now, args, out, from, to, 5)
    })
}

/// `BRPOPLPUSH source destination timeout`
pub(super) fn brpoplpush(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    blocking::block(c, s, n, a, o, |ctx, session, now, args, out| {
        blocking_move(ctx, session, now, args, out, End::Right, End::Left, 3)
    })
}

/// `LMOVE`, blocking while the source is empty: the timeout comes first.
#[allow(clippy::too_many_arguments)]
fn blocking_move(
    ctx: &Context,
    session: &Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    from: End,
    to: End,
    timeout_at: usize,
) -> Attempt {
    let timeout = match blocking::parse_timeout(args.get(timeout_at)) {
        Ok(timeout) => timeout,
        Err(msg) => {
            resp::write_error(out, msg);
            return Attempt::Done;
        }
    };
    match move_element(ctx, session, now, args, from, to) {
        Ok(Some(element)) => resp::write_bulk(out, &element),
        Ok(None) => {
            return Attempt::Wait {
                on_change: false,
                keys: 1..2,
                timeout,
                kind: Kind::List,
                nil_bulk: true,
                args: None,
            };
        }
        Err(msg) => resp::write_error(out, msg),
    }
    Attempt::Done
}

/// `BLPOP`/`BRPOP key [key ...] timeout`: pops from the first non-empty
/// list, replying its key and the element.
fn blocking_pop(
    ctx: &Context,
    session: &Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    end: End,
) -> Attempt {
    let timeout = match blocking::parse_timeout(args.get(args.len() - 1)) {
        Ok(timeout) => timeout,
        Err(msg) => {
            resp::write_error(out, msg);
            return Attempt::Done;
        }
    };
    let store = ctx.db(session);
    let keys = 1..args.len() - 1;
    for k in keys.clone() {
        let popped = change(store, args.key_at(store, k), now, false, args, |list, _| {
            Ok(list.as_mut().and_then(|list| pop(list, end)))
        });
        match popped {
            Err(msg) => {
                resp::write_error(out, msg);
                return Attempt::Done;
            }
            Ok(None) => continue,
            Ok(Some(element)) => {
                resp::write_array_header(out, 2);
                resp::write_bulk(out, args.get(k));
                resp::write_bulk(out, &element);
                return Attempt::Done;
            }
        }
    }
    Attempt::Wait {
        on_change: false,
        keys,
        timeout,
        kind: Kind::List,
        nil_bulk: false,
        args: None,
    }
}

pub(super) fn blpop(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    blocking::block(c, s, n, a, o, |ctx, session, now, args, out| {
        blocking_pop(ctx, session, now, args, out, End::Left)
    })
}

pub(super) fn brpop(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    blocking::block(c, s, n, a, o, |ctx, session, now, args, out| {
        blocking_pop(ctx, session, now, args, out, End::Right)
    })
}

/// `LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]`: pops from the
/// first non-empty list, replying its key and the elements.
pub(super) fn lmpop(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    if let Attempt::Wait { .. } = mpop(ctx, session, now, args, out, 1) {
        out.extend_from_slice(null_array(session));
    }
    Reply::Written
}

/// `BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]`
pub(super) fn blmpop(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    blocking::block(c, s, n, a, o, |ctx, session, now, args, out| {
        mpop(ctx, session, now, args, out, 2)
    })
}

/// `LMPOP` with its `numkeys` at argument `at`, or `BLMPOP` (at 2, after
/// the timeout, which is read after the other arguments).
fn mpop(
    ctx: &Context,
    session: &Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    at: usize,
) -> Attempt {
    let error = |out: &mut Vec<u8>, msg: &[u8]| {
        resp::write_error(out, msg);
        Attempt::Done
    };
    let numkeys = match resp::parse_int(args.get(at)) {
        Some(n) if n > 0 => n as usize,
        _ => return error(out, b"ERR numkeys should be greater than 0"),
    };
    // The direction comes after the keys.
    let keys = at + 1..(at + 1).saturating_add(numkeys);
    if keys.end >= args.len() {
        return error(out, SYNTAX_ERROR);
    }
    let Some(end) = end_of(args.get(keys.end)) else {
        return error(out, SYNTAX_ERROR);
    };
    let mut count = None;
    let mut i = keys.end + 1;
    while i < args.len() {
        if count.is_none() && args.get(i).eq_ignore_ascii_case(b"COUNT") && i + 1 < args.len() {
            match resp::parse_int(args.get(i + 1)) {
                Some(n) if n > 0 => count = Some(n as usize),
                _ => return error(out, b"ERR count should be greater than 0"),
            }
            i += 2;
        } else {
            return error(out, SYNTAX_ERROR);
        }
    }
    let timeout = if at == 2 {
        match blocking::parse_timeout(args.get(1)) {
            Ok(timeout) => timeout,
            Err(msg) => return error(out, msg),
        }
    } else {
        None
    };
    let store = ctx.db(session);
    for k in keys.clone() {
        let popped = change(store, args.key_at(store, k), now, false, args, |list, _| {
            let Some(list) = list else {
                return Ok(None);
            };
            let n = count.unwrap_or(1).min(list.len());
            Ok(Some(
                (0..n).filter_map(|_| pop(list, end)).collect::<Vec<_>>(),
            ))
        });
        match popped {
            Err(msg) => return error(out, msg),
            Ok(None) => continue,
            Ok(Some(popped)) => {
                resp::write_array_header(out, 2);
                resp::write_bulk(out, args.get(k));
                resp::write_array_header(out, popped.len());
                popped.iter().for_each(|e| resp::write_bulk(out, e));
                return Attempt::Done;
            }
        }
    }
    Attempt::Wait {
        on_change: false,
        keys,
        timeout,
        kind: Kind::List,
        nil_bulk: false,
        args: None,
    }
}
