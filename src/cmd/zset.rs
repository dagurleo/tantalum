//! Sorted set commands, each checking its arguments and the key in Redis'
//! order (`t_zset.c`), which differs from command to command.

use std::cell::Cell;
use std::hash::BuildHasher;

use super::{
    Args, INVALID_CURSOR, NOT_AN_INTEGER, OOM, Reply, SYNTAX_ERROR, Scan, ScanOptions, WRONGTYPE,
    parse_cursor,
};
use crate::blocking::{self, Attempt};
use crate::collection::Collection;
use crate::context::{Context, Session};
use crate::resp;
use crate::set::{Compact as PackedSet, SetView};
use crate::store::{Entry, EntryValue, Key, Kind, Millis, OutOfMemory, Store, Update, View};
use crate::zset::{
    self, BigZSet, Finished, LexRange, Packed, ScoreRange, ZMut, ZView, format_score,
    parse_lex_bound, parse_score, parse_score_bound,
};

const NOT_A_FLOAT: &[u8] = b"ERR value is not a valid float";
const NAN_SCORE: &[u8] = b"ERR resulting score is not a number (NaN)";
const BAD_SCORE_RANGE: &[u8] = b"ERR min or max is not a float";
const BAD_LEX_RANGE: &[u8] = b"ERR min or max not valid string range item";
const MUST_BE_POSITIVE: &[u8] = b"ERR value is out of range, must be positive";

fn random_below(n: usize) -> usize {
    thread_local! {
        static STATE: Cell<u64> = Cell::new(foldhash::fast::RandomState::default().hash_one(1u64) | 1);
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

fn view(value: View<'_>) -> ZView<'_> {
    match value {
        View::Bytes(bytes) => ZView::Packed(Packed::new(bytes)),
        View::Object(Collection::ZSet(set)) => ZView::Big(set),
        View::Object(_) => unreachable!("a sorted set's object is one"),
    }
}

fn zset_of(entry: Option<Entry<'_>>) -> Result<Option<ZMut<'_>>, ()> {
    match entry {
        None => Ok(None),
        Some(Entry {
            kind: Kind::ZSet,
            value,
            ..
        }) => Ok(Some(match value {
            EntryValue::Bytes(bytes) => ZMut::from_packed(bytes),
            EntryValue::Object(Collection::ZSet(set)) => ZMut::big(set),
            EntryValue::Object(_) => unreachable!("a sorted set's object is one"),
        })),
        Some(_) => Err(()),
    }
}

fn finish(set: Option<ZMut<'_>>, existed: bool) -> Update {
    match set.and_then(ZMut::finish) {
        None if existed => Update::Touch,
        None => Update::Peek,
        Some(Finished::Packed(bytes)) => Update::Replace(Kind::ZSet, bytes),
        Some(Finished::Grown(set)) => Update::Object(Collection::ZSet(set)),
        Some(Finished::Changed) => Update::Changed,
        Some(Finished::Delete) => Update::Delete,
    }
}

/// How a whole result set is stored: packed, big, or deleted when empty.
fn store_update(set: BigZSet) -> Update {
    if set.is_empty() {
        Update::Delete
    } else if set.fits_packed() {
        Update::Replace(Kind::ZSet, set.to_packed())
    } else {
        Update::Object(Collection::ZSet(Box::new(set)))
    }
}

fn read<R>(
    store: &Store,
    key: Key<'_>,
    now: Millis,
    session: &mut Session,
    f: impl FnOnce(Option<ZView<'_>>) -> R,
) -> Result<R, ()> {
    let (result, hit) = store.get_key(key, now, |value| match value {
        None => (Ok(f(None)), false),
        Some(v) if v.kind() != Kind::ZSet => (Err(()), true),
        Some(v) => (Ok(f(Some(view(v.view())))), true),
    });
    session.count_lookup(hit);
    result
}

fn reading(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    f: impl FnOnce(Option<ZView<'_>>, &mut Vec<u8>),
) -> Reply {
    let store = ctx.db(session);
    if read(store, args.key(store), now, session, |set| f(set, out)).is_err() {
        resp::write_error(out, WRONGTYPE);
    }
    Reply::Written
}

fn change<R>(
    store: &Store,
    key: Key<'_>,
    now: Millis,
    grows: bool,
    args: &Args<'_>,
    mut f: impl for<'v> FnMut(&mut Option<ZMut<'v>>, &Args<'v>) -> Result<R, &'static [u8]>,
) -> Result<R, &'static [u8]> {
    let result = store.update_collection(key, now, grows, |entry| {
        let Ok(mut set) = zset_of(entry) else {
            return (Update::Peek, Err(WRONGTYPE));
        };
        let existed = set.is_some();
        match f(&mut set, args) {
            Ok(r) => (finish(set, existed), Ok(r)),
            Err(msg) => (finish(set, existed), Err(msg)),
        }
    });
    result.unwrap_or(Err(OOM))
}

fn reply_error(out: &mut Vec<u8>, msg: &[u8]) -> Reply {
    resp::write_error(out, msg);
    Reply::Written
}

/// A score as a reply: a double in RESP3, a bulk string in RESP2.
pub(super) fn write_score(out: &mut Vec<u8>, score: f64, resp3: bool) {
    if resp3 {
        out.push(b',');
        format_score(score, out);
        out.extend_from_slice(b"\r\n");
        return;
    }
    // The text first, then its length put in front (a few bytes moved).
    let start = out.len();
    format_score(score, out);
    let len = out.len() - start;
    let mut buffer = itoa::Buffer::new();
    let digits = buffer.format(len).as_bytes();
    let mut header = [0u8; 8];
    header[0] = b'$';
    header[1..1 + digits.len()].copy_from_slice(digits);
    header[1 + digits.len()..3 + digits.len()].copy_from_slice(b"\r\n");
    out.splice(start..start, header[..3 + digits.len()].iter().copied());
    out.extend_from_slice(b"\r\n");
}

/// Entries as a reply: members, or with scores, flat or as `pairs`.
fn write_entries(
    out: &mut Vec<u8>,
    entries: &[(f64, Vec<u8>)],
    scores: bool,
    resp3: bool,
    pairs: bool,
) {
    let pairs = scores && pairs;
    resp::write_array_header(
        out,
        if scores && !pairs {
            entries.len() * 2
        } else {
            entries.len()
        },
    );
    for (score, member) in entries {
        if pairs {
            resp::write_array_header(out, 2);
        }
        resp::write_bulk(out, member);
        if scores {
            write_score(out, *score, resp3);
        }
    }
}

/// `ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]`
pub(super) fn zadd(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    add(ctx, session, now, args, out, false)
}

/// `ZINCRBY key increment member`: `ZADD key INCR increment member`.
pub(super) fn zincrby(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    add(ctx, session, now, args, out, true)
}

fn add(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    mut incr: bool,
) -> Reply {
    let (mut nx, mut xx, mut gt, mut lt, mut ch) = (false, false, false, false, false);
    let mut first = 2;
    while first < args.len() {
        let opt = args.get(first);
        let is = |name: &[u8]| opt.eq_ignore_ascii_case(name);
        if is(b"NX") {
            nx = true;
        } else if is(b"XX") {
            xx = true;
        } else if is(b"CH") {
            ch = true;
        } else if is(b"INCR") {
            incr = true;
        } else if is(b"GT") {
            gt = true;
        } else if is(b"LT") {
            lt = true;
        } else {
            break;
        }
        first += 1;
    }
    let elements = args.len() - first;
    if elements % 2 == 1 || elements == 0 {
        return reply_error(out, SYNTAX_ERROR);
    }
    if nx && xx {
        return reply_error(
            out,
            b"ERR XX and NX options at the same time are not compatible",
        );
    }
    if (nx && (gt || lt)) || (gt && lt) {
        return reply_error(
            out,
            b"ERR GT, LT, and/or NX options at the same time are not compatible",
        );
    }
    if incr && elements > 2 {
        return reply_error(
            out,
            b"ERR INCR option supports a single increment-element pair",
        );
    }
    let mut scores = Vec::with_capacity(elements / 2);
    for i in (first..args.len()).step_by(2) {
        match parse_score(args.get(i)) {
            Some(score) => scores.push(score),
            None => return reply_error(out, NOT_A_FLOAT),
        }
    }
    let store = ctx.db(session);
    let result = change(store, args.key(store), now, true, args, |set, args| {
        let (mut added, mut updated, mut processed) = (0, 0, 0);
        let mut last = None;
        if set.is_none() {
            if xx {
                return Ok((0, None));
            }
            *set = Some(ZMut::new());
        }
        let set = set.as_mut().expect("made");
        for (n, &score) in scores.iter().enumerate() {
            let member = args.get(first + 1 + 2 * n);
            match set.set().score(member) {
                Some(current) => {
                    if nx {
                        continue;
                    }
                    let mut score = score;
                    if incr {
                        score += current;
                        if score.is_nan() {
                            return Err(NAN_SCORE);
                        }
                    }
                    if (lt && score >= current) || (gt && score <= current) {
                        continue;
                    }
                    processed += 1;
                    last = Some(score);
                    if score != current {
                        set.edit().insert(member, score);
                        updated += 1;
                    }
                }
                None if xx => {}
                None => {
                    set.edit().insert(member, score);
                    added += 1;
                    processed += 1;
                    last = Some(score);
                }
            }
        }
        Ok((
            if ch { added + updated } else { added },
            last.filter(|_| processed > 0),
        ))
    });
    match result {
        Err(msg) => resp::write_error(out, msg),
        Ok((_, Some(score))) if incr => write_score(out, score, session.resp3),
        Ok(_) if incr => out.extend_from_slice(session.null()),
        Ok((n, _)) => resp::write_integer(out, n),
    }
    Reply::Written
}

/// `ZREM key member [member ...]`
pub(super) fn zrem(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = ctx.db(session);
    let removed = change(store, args.key(store), now, false, args, |set, args| {
        let Some(set) = set else {
            return Ok(0);
        };
        let mut removed = 0;
        for i in 2..args.len() {
            if set.set().score(args.get(i)).is_some() {
                set.edit().remove(args.get(i));
                removed += 1;
            }
        }
        Ok(removed)
    });
    match removed {
        Ok(n) => resp::write_integer(out, n),
        Err(msg) => resp::write_error(out, msg),
    }
    Reply::Written
}

/// `ZCARD key`
pub(super) fn zcard(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    reading(ctx, session, now, args, out, |set, out| {
        resp::write_integer(out, set.map_or(0, |s| s.len()) as i64)
    })
}

/// `ZSCORE key member`
pub(super) fn zscore(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let (null, resp3) = (session.null(), session.resp3);
    reading(ctx, session, now, args, out, |set, out| {
        match set.and_then(|s| s.score(args.get(2))) {
            Some(score) => write_score(out, score, resp3),
            None => out.extend_from_slice(null),
        }
    })
}

/// `ZMSCORE key member [member ...]`
pub(super) fn zmscore(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let (null, resp3) = (session.null(), session.resp3);
    reading(ctx, session, now, args, out, |set, out| {
        resp::write_array_header(out, args.len() - 2);
        for i in 2..args.len() {
            match set.and_then(|s| s.score(args.get(i))) {
                Some(score) => write_score(out, score, resp3),
                None => out.extend_from_slice(null),
            }
        }
    })
}

/// `ZRANK`/`ZREVRANK key member [WITHSCORE]`
fn rank_command(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    rev: bool,
) -> Reply {
    if args.len() > 4 {
        super::wrong_arity(out, if rev { "zrevrank" } else { "zrank" });
        return Reply::Written;
    }
    let with_score = args.len() == 4;
    if with_score && !args.get(3).eq_ignore_ascii_case(b"WITHSCORE") {
        return reply_error(out, SYNTAX_ERROR);
    }
    let resp3 = session.resp3;
    let missing: &[u8] = match (with_score, resp3) {
        (true, false) => b"*-1\r\n",
        (_, true) => resp::NULL,
        (false, false) => resp::NULL_BULK,
    };
    reading(ctx, session, now, args, out, |set, out| {
        let member = args.get(2);
        let found = set.and_then(|s| Some((s.rank(member)?, s.score(member)?, s.len())));
        let Some((rank, score, len)) = found else {
            return out.extend_from_slice(missing);
        };
        let rank = if rev { len - 1 - rank } else { rank };
        if with_score {
            resp::write_array_header(out, 2);
        }
        resp::write_integer(out, rank as i64);
        if with_score {
            write_score(out, score, resp3);
        }
    })
}

pub(super) fn zrank(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    rank_command(c, s, n, a, o, false)
}

pub(super) fn zrevrank(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    rank_command(c, s, n, a, o, true)
}

fn score_range(min: &[u8], max: &[u8]) -> Option<ScoreRange> {
    Some(ScoreRange {
        min: parse_score_bound(min)?,
        max: parse_score_bound(max)?,
    })
}

fn lex_range(min: &[u8], max: &[u8]) -> Option<LexRange> {
    Some(LexRange {
        min: parse_lex_bound(min)?,
        max: parse_lex_bound(max)?,
    })
}

/// The ranks (from the lowest) a score range covers.
fn score_ranks(set: &ZView<'_>, range: &ScoreRange) -> std::ops::Range<usize> {
    if range.is_empty() {
        return 0..0;
    }
    let start = set.partition_point(|s, _| range.below(s));
    let end = set.partition_point(|s, _| range.not_above(s));
    start..end.max(start)
}

fn lex_ranks(set: &ZView<'_>, range: &LexRange) -> std::ops::Range<usize> {
    if range.is_empty() {
        return 0..0;
    }
    let start = set.partition_point(|_, m| range.below(m));
    let end = set.partition_point(|_, m| range.not_above(m));
    start..end.max(start)
}

/// Redis' rank range `start..=stop` (negative from the end) in `len`.
fn rank_range(start: i64, stop: i64, len: usize) -> std::ops::Range<usize> {
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum By {
    Rank,
    Score,
    Lex,
}

/// The entries a `ZRANGE`-style query selects, copied out.
fn select(
    set: &ZView<'_>,
    by: By,
    rev: bool,
    min: &Bounds,
    limit: Option<(i64, i64)>,
) -> Vec<(f64, Vec<u8>)> {
    let mut entries = Vec::new();
    for_selected(set, by, rev, min, limit, |s, m| {
        entries.push((s, m.to_vec()))
    });
    entries
}

/// Calls `f` with each entry a `ZRANGE`-style query selects, in reply
/// order, and returns how many.
fn for_selected(
    set: &ZView<'_>,
    by: By,
    rev: bool,
    min: &Bounds,
    limit: Option<(i64, i64)>,
    mut f: impl FnMut(f64, &[u8]),
) -> usize {
    let len = set.len();
    // Ranks from the lowest.
    let ranks = match (by, min) {
        (By::Rank, Bounds::Rank(start, stop)) => {
            let r = rank_range(*start, *stop, len);
            // In reverse, ranks count from the highest.
            if rev { len - r.end..len - r.start } else { r }
        }
        (By::Score, Bounds::Score(range)) => score_ranks(set, range),
        (By::Lex, Bounds::Lex(range)) => lex_ranks(set, range),
        _ => unreachable!("bounds match the kind"),
    };
    let (mut skip, mut take) = (0usize, usize::MAX);
    if let Some((offset, count)) = limit {
        if offset < 0 {
            return 0;
        }
        skip = offset as usize;
        if count >= 0 {
            take = count as usize;
        }
    }
    let n = ranks.len().saturating_sub(skip).min(take);
    if n == 0 {
        return 0;
    }
    if rev {
        // Highest first: in reverse ranks, `len - end .. len - start`.
        let start = len - ranks.end + skip;
        set.for_range(start, start + n, true, &mut f);
    } else {
        let start = ranks.start + skip;
        set.for_range(start, start + n, false, &mut f);
    }
    n
}

enum Bounds {
    Rank(i64, i64),
    Score(ScoreRange),
    Lex(LexRange),
}

/// `ZRANGE` and its older forms, and `ZRANGESTORE`: Redis'
/// `zrangeGenericCommand`, with `by` and `rev` fixed (`Some`) for the older
/// forms. `first` is where the source key is.
#[allow(clippy::too_many_arguments)]
fn range_command(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    fixed_by: Option<By>,
    fixed_rev: Option<bool>,
    store_into: bool,
) -> Reply {
    let first = if store_into { 2 } else { 1 };
    let (mut by, mut rev) = (fixed_by, fixed_rev);
    let (mut with_scores, mut limit) = (false, None);
    let mut i = first + 3;
    while i < args.len() {
        let opt = args.get(i);
        let is = |name: &[u8]| opt.eq_ignore_ascii_case(name);
        let left = args.len() - i - 1;
        if !store_into && is(b"WITHSCORES") {
            with_scores = true;
        } else if is(b"LIMIT") && left >= 2 {
            match (
                resp::parse_int(args.get(i + 1)),
                resp::parse_int(args.get(i + 2)),
            ) {
                (Some(offset), Some(count)) => limit = Some((offset, count)),
                _ => return reply_error(out, NOT_AN_INTEGER),
            }
            i += 2;
        } else if rev.is_none() && is(b"REV") {
            rev = Some(true);
        } else if by.is_none() && is(b"BYLEX") {
            by = Some(By::Lex);
        } else if by.is_none() && is(b"BYSCORE") {
            by = Some(By::Score);
        } else {
            return reply_error(out, SYNTAX_ERROR);
        }
        i += 1;
    }
    let (by, rev) = (by.unwrap_or(By::Rank), rev.unwrap_or(false));
    if limit.is_some() && by == By::Rank {
        return reply_error(out, b"ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX");
    }
    if with_scores && by == By::Lex {
        return reply_error(
            out,
            b"ERR syntax error, WITHSCORES not supported in combination with BYLEX",
        );
    }
    let (mut min, mut max) = (args.get(first + 1), args.get(first + 2));
    if rev && by != By::Rank {
        std::mem::swap(&mut min, &mut max);
    }
    let bounds = match by {
        By::Rank => match (resp::parse_int(min), resp::parse_int(max)) {
            (Some(start), Some(stop)) => Bounds::Rank(start, stop),
            _ => return reply_error(out, NOT_AN_INTEGER),
        },
        By::Score => match score_range(min, max) {
            Some(range) => Bounds::Score(range),
            None => return reply_error(out, BAD_SCORE_RANGE),
        },
        By::Lex => match lex_range(min, max) {
            Some(range) => Bounds::Lex(range),
            None => return reply_error(out, BAD_LEX_RANGE),
        },
    };
    let store = ctx.db(session);
    let source = args.key_at(store, first);
    if !store_into {
        // Written straight from the set, under its lock.
        let resp3 = session.resp3;
        let pairs = with_scores && resp3;
        let mut body = Vec::new();
        let written = read(store, source, now, session, |set| {
            set.map_or(0, |set| {
                for_selected(&set, by, rev, &bounds, limit, |score, member| {
                    if pairs {
                        resp::write_array_header(&mut body, 2);
                    }
                    resp::write_bulk(&mut body, member);
                    if with_scores {
                        write_score(&mut body, score, resp3);
                    }
                })
            })
        });
        match written {
            Ok(n) => {
                resp::write_array_header(out, if with_scores && !pairs { n * 2 } else { n });
                out.extend_from_slice(&body);
            }
            Err(()) => resp::write_error(out, WRONGTYPE),
        }
        return Reply::Written;
    }
    let selected = match read(store, source, now, session, |set| {
        set.map(|set| select(&set, by, rev, &bounds, limit))
            .unwrap_or_default()
    }) {
        Ok(entries) => entries,
        Err(()) => return reply_error(out, WRONGTYPE),
    };
    let n = selected.len();
    let stored = store.update_collection(args.key(store), now, true, |_| {
        let mut set = BigZSet::default();
        for (score, member) in &selected {
            set.insert(member, *score);
        }
        (store_update(set), ())
    });
    match stored {
        Ok(()) => resp::write_integer(out, n as i64),
        Err(OutOfMemory) => resp::write_error(out, OOM),
    }
    Reply::Written
}

pub(super) fn zrange(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    range_command(c, s, n, a, o, None, None, false)
}

pub(super) fn zrangestore(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    range_command(c, s, n, a, o, None, None, true)
}

pub(super) fn zrevrange(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    range_command(c, s, n, a, o, Some(By::Rank), Some(true), false)
}

pub(super) fn zrangebyscore(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    range_command(c, s, n, a, o, Some(By::Score), Some(false), false)
}

pub(super) fn zrevrangebyscore(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    range_command(c, s, n, a, o, Some(By::Score), Some(true), false)
}

pub(super) fn zrangebylex(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    range_command(c, s, n, a, o, Some(By::Lex), Some(false), false)
}

pub(super) fn zrevrangebylex(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    range_command(c, s, n, a, o, Some(By::Lex), Some(true), false)
}

/// `ZCOUNT key min max`
pub(super) fn zcount(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let Some(range) = score_range(args.get(2), args.get(3)) else {
        return reply_error(out, BAD_SCORE_RANGE);
    };
    reading(ctx, session, now, args, out, |set, out| {
        resp::write_integer(out, set.map_or(0, |s| score_ranks(&s, &range).len()) as i64)
    })
}

/// `ZLEXCOUNT key min max`
pub(super) fn zlexcount(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let Some(range) = lex_range(args.get(2), args.get(3)) else {
        return reply_error(out, BAD_LEX_RANGE);
    };
    reading(ctx, session, now, args, out, |set, out| {
        resp::write_integer(out, set.map_or(0, |s| lex_ranks(&s, &range).len()) as i64)
    })
}

/// `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE` and `ZREMRANGEBYLEX key min max`
fn remove_range(
    ctx: &Context,
    session: &Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    by: By,
) -> Reply {
    let (min, max) = (args.get(2), args.get(3));
    let bounds = match by {
        By::Rank => match (resp::parse_int(min), resp::parse_int(max)) {
            (Some(start), Some(stop)) => Bounds::Rank(start, stop),
            _ => return reply_error(out, NOT_AN_INTEGER),
        },
        By::Score => match score_range(min, max) {
            Some(range) => Bounds::Score(range),
            None => return reply_error(out, BAD_SCORE_RANGE),
        },
        By::Lex => match lex_range(min, max) {
            Some(range) => Bounds::Lex(range),
            None => return reply_error(out, BAD_LEX_RANGE),
        },
    };
    let store = ctx.db(session);
    let removed = change(store, args.key(store), now, false, args, |set, _| {
        let Some(set) = set else {
            return Ok(0);
        };
        let view = ZView::Big(set.set());
        let gone = select(&view, by, false, &bounds, None);
        for (_, member) in &gone {
            set.edit().remove(member);
        }
        Ok(gone.len())
    });
    match removed {
        Ok(n) => resp::write_integer(out, n as i64),
        Err(msg) => resp::write_error(out, msg),
    }
    Reply::Written
}

pub(super) fn zremrangebyrank(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    remove_range(c, s, n, a, o, By::Rank)
}

pub(super) fn zremrangebyscore(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    remove_range(c, s, n, a, o, By::Score)
}

pub(super) fn zremrangebylex(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    remove_range(c, s, n, a, o, By::Lex)
}

/// Pops up to `count` lowest (or highest, if `max`) entries.
fn pop_entries(set: &mut ZMut<'_>, count: usize, max: bool) -> Vec<(f64, Vec<u8>)> {
    let n = count.min(set.set().len());
    let view = ZView::Big(set.set());
    let mut popped = Vec::with_capacity(n);
    view.for_range(0, n, max, |s, m| popped.push((s, m.to_vec())));
    for (_, member) in &popped {
        set.edit().remove(member);
    }
    popped
}

/// `ZPOPMIN`/`ZPOPMAX key [count]`
fn pop_command(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    max: bool,
) -> Reply {
    if args.len() > 3 {
        return reply_error(out, SYNTAX_ERROR);
    }
    let count = match args.len() {
        3 => match resp::parse_int(args.get(2)) {
            Some(n) if n >= 0 => Some(n as usize),
            _ => return reply_error(out, MUST_BE_POSITIVE),
        },
        _ => None,
    };
    let store = ctx.db(session);
    let popped = change(store, args.key(store), now, false, args, |set, _| {
        Ok(set
            .as_mut()
            .map(|set| pop_entries(set, count.unwrap_or(1), max))
            .unwrap_or_default())
    });
    match popped {
        Ok(popped) => write_entries(
            out,
            &popped,
            true,
            session.resp3,
            session.resp3 && count.is_some(),
        ),
        Err(msg) => resp::write_error(out, msg),
    }
    Reply::Written
}

pub(super) fn zpopmin(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    pop_command(c, s, n, a, o, false)
}

pub(super) fn zpopmax(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    pop_command(c, s, n, a, o, true)
}

/// `ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]`
pub(super) fn zmpop(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    if let Attempt::Wait { .. } = mpop(ctx, session, now, args, out, 1) {
        out.extend_from_slice(if session.resp3 {
            resp::NULL
        } else {
            b"*-1\r\n"
        });
    }
    Reply::Written
}

/// `BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]`
pub(super) fn bzmpop(
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

/// `ZMPOP` with its `numkeys` at argument `at`, or `BZMPOP` (at 2, after
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
    let keys = at + 1..(at + 1).saturating_add(numkeys);
    if keys.end >= args.len() {
        return error(out, SYNTAX_ERROR);
    }
    let where_ = args.get(keys.end);
    let max = if where_.eq_ignore_ascii_case(b"MAX") {
        true
    } else if where_.eq_ignore_ascii_case(b"MIN") {
        false
    } else {
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
        let popped = change(store, args.key_at(store, k), now, false, args, |set, _| {
            Ok(set
                .as_mut()
                .map(|set| pop_entries(set, count.unwrap_or(1), max)))
        });
        match popped {
            Err(msg) => return error(out, msg),
            Ok(None) => continue,
            Ok(Some(popped)) => {
                resp::write_array_header(out, 2);
                resp::write_bulk(out, args.get(k));
                write_entries(out, &popped, true, session.resp3, true);
                return Attempt::Done;
            }
        }
    }
    Attempt::Wait {
        on_change: false,
        keys,
        timeout,
        kind: Kind::ZSet,
        nil_bulk: false,
        args: None,
    }
}

/// `BZPOPMIN`/`BZPOPMAX key [key ...] timeout`: pops from the first
/// non-empty sorted set, replying its key, the member and its score.
fn blocking_pop(
    ctx: &Context,
    session: &Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    max: bool,
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
        let popped = change(store, args.key_at(store, k), now, false, args, |set, _| {
            Ok(set.as_mut().map(|set| pop_entries(set, 1, max)))
        });
        match popped {
            Err(msg) => {
                resp::write_error(out, msg);
                return Attempt::Done;
            }
            Ok(None) => continue,
            Ok(Some(popped)) => {
                let (score, member) = &popped[0];
                resp::write_array_header(out, 3);
                resp::write_bulk(out, args.get(k));
                resp::write_bulk(out, member);
                write_score(out, *score, session.resp3);
                return Attempt::Done;
            }
        }
    }
    Attempt::Wait {
        on_change: false,
        keys,
        timeout,
        kind: Kind::ZSet,
        nil_bulk: false,
        args: None,
    }
}

pub(super) fn bzpopmin(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    blocking::block(c, s, n, a, o, |ctx, session, now, args, out| {
        blocking_pop(ctx, session, now, args, out, false)
    })
}

pub(super) fn bzpopmax(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    blocking::block(c, s, n, a, o, |ctx, session, now, args, out| {
        blocking_pop(ctx, session, now, args, out, true)
    })
}

/// `ZRANDMEMBER key [count [WITHSCORES]]`
pub(super) fn zrandmember(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let null = session.null();
    let resp3 = session.resp3;
    if args.len() == 2 {
        return reading(ctx, session, now, args, out, |set, out| match set {
            Some(set) if set.len() > 0 => {
                resp::write_bulk(out, set.at_position(random_below(set.len())).1)
            }
            _ => out.extend_from_slice(null),
        });
    }
    let Some(count) = resp::parse_int(args.get(2)) else {
        return reply_error(out, NOT_AN_INTEGER);
    };
    if count == i64::MIN {
        return reply_error(
            out,
            b"ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807",
        );
    }
    if args.len() > 4 || (args.len() == 4 && !args.get(3).eq_ignore_ascii_case(b"WITHSCORES")) {
        return reply_error(out, SYNTAX_ERROR);
    }
    let with_scores = args.len() == 4;
    if with_scores && !(-(i64::MAX / 2)..=i64::MAX / 2).contains(&count) {
        return reply_error(out, b"ERR value is out of range");
    }
    reading(ctx, session, now, args, out, |set, out| {
        let Some(set) = set.filter(|s| s.len() > 0 && count != 0) else {
            return resp::write_array_header(out, 0);
        };
        let n = count.unsigned_abs() as usize;
        let entries: Vec<(f64, Vec<u8>)> = if count < 0 {
            (0..n)
                .map(|_| {
                    let (s, m) = set.at_position(random_below(set.len()));
                    (s, m.to_vec())
                })
                .collect()
        } else if n >= set.len() {
            // All of them, highest first, as Redis gives them.
            let mut all = Vec::with_capacity(set.len());
            set.for_range(0, set.len(), true, |s, m| all.push((s, m.to_vec())));
            all
        } else {
            let mut picked = std::collections::HashSet::with_capacity(n);
            let mut entries = Vec::with_capacity(n);
            while entries.len() < n {
                let i = random_below(set.len());
                if picked.insert(i) {
                    let (s, m) = set.at_position(i);
                    entries.push((s, m.to_vec()));
                }
            }
            entries
        };
        write_entries(out, &entries, with_scores, resp3, resp3);
    })
}

/// `ZSCAN key cursor [MATCH pattern] [COUNT count]`: a packed set whole,
/// with cursor 0; a big one `COUNT` members at a time from the end of its
/// member order, as `SSCAN` does.
pub(super) fn zscan(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let Some(cursor) = parse_cursor(args.get(2)) else {
        return reply_error(out, INVALID_CURSOR);
    };
    reading(ctx, session, now, args, out, |set, out| {
        let Some(set) = set else {
            return out.extend_from_slice(b"*2\r\n$1\r\n0\r\n*0\r\n");
        };
        let Some(options) = ScanOptions::parse(args, 3, Scan::Other, out) else {
            return;
        };
        let mut found = Vec::new();
        let next = match set {
            ZView::Packed(z) => {
                for (s, m) in z.iter() {
                    if options.matches(m) {
                        found.push((s, m.to_vec()));
                    }
                }
                0
            }
            ZView::Big(z) => {
                let end = match cursor {
                    0 => z.len(),
                    c => usize::try_from(c).unwrap_or(usize::MAX).min(z.len()),
                };
                let start = end.saturating_sub(options.count);
                for i in start..end {
                    let (s, m) = z.at_position(i);
                    if options.matches(m) {
                        found.push((s, m.to_vec()));
                    }
                }
                start as u64
            }
        };
        out.extend_from_slice(b"*2\r\n");
        resp::write_bulk(out, itoa::Buffer::new().format(next).as_bytes());
        resp::write_array_header(out, found.len() * 2);
        for (score, member) in &found {
            resp::write_bulk(out, member);
            resp::write_bulk(out, &zset::score_string(*score));
        }
    })
}

/// An input of `ZUNION` and the rest: a sorted set, or a set (scores 1).
type Input = Vec<(f64, Vec<u8>)>;

/// Reads input keys (sorted sets or sets) one at a time; `Err` if one
/// holds another kind.
fn read_inputs(
    store: &Store,
    now: Millis,
    session: &mut Session,
    args: &Args<'_>,
    keys: std::ops::Range<usize>,
) -> Result<Vec<Input>, ()> {
    let mut inputs = Vec::with_capacity(keys.len());
    for i in keys {
        let (input, hit) = store.get_key(args.key_at(store, i), now, |value| match value {
            None => (Ok(Vec::new()), false),
            Some(v) if v.kind() == Kind::ZSet => {
                let set = view(v.view());
                let mut entries = Vec::with_capacity(set.len());
                set.for_range(0, set.len(), false, |s, m| entries.push((s, m.to_vec())));
                (Ok(entries), true)
            }
            Some(v) if v.kind() == Kind::Set => {
                let set = match v.view() {
                    View::Bytes(bytes) => SetView::Compact(PackedSet::new(bytes)),
                    View::Object(Collection::Set(set)) => SetView::Big(set),
                    View::Object(_) => unreachable!("a set's object is a set"),
                };
                let mut entries = Vec::with_capacity(set.len());
                set.for_each(|m| entries.push((1.0, m.to_vec())));
                (Ok(entries), true)
            }
            Some(_) => (Err(()), true),
        });
        session.count_lookup(hit);
        inputs.push(input?);
    }
    Ok(inputs)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Op {
    Union,
    Inter,
    Diff,
}

#[derive(Clone, Copy)]
enum Aggregate {
    Sum,
    Min,
    Max,
}

fn aggregate(a: f64, b: f64, how: Aggregate) -> f64 {
    match how {
        Aggregate::Sum => {
            // inf + -inf is NaN; Redis makes it 0.
            let sum = a + b;
            if sum.is_nan() { 0.0 } else { sum }
        }
        Aggregate::Min => a.min(b),
        Aggregate::Max => a.max(b),
    }
}

/// The result of `ZUNION`, `ZINTER` or `ZDIFF` over `inputs`, in order.
fn combine(inputs: Vec<Input>, weights: &[f64], how: Aggregate, op: Op) -> BigZSet {
    let weighted = |score: f64, w: f64| {
        let s = score * w;
        if s.is_nan() { 0.0 } else { s }
    };
    let mut result = BigZSet::default();
    match op {
        Op::Union => {
            for (input, &w) in inputs.iter().zip(weights) {
                for (score, member) in input {
                    let score = weighted(*score, w);
                    let new = match result.score(member) {
                        Some(old) => aggregate(old, score, how),
                        None => score,
                    };
                    result.insert(member, new);
                }
            }
        }
        Op::Inter => {
            if inputs.iter().any(Vec::is_empty) {
                return result;
            }
            let others: Vec<std::collections::HashMap<&[u8], f64>> = inputs[1..]
                .iter()
                .map(|input| input.iter().map(|(s, m)| (&m[..], *s)).collect())
                .collect();
            for (score, member) in &inputs[0] {
                let mut total = weighted(*score, weights[0]);
                let mut everywhere = true;
                for (other, &w) in others.iter().zip(&weights[1..]) {
                    match other.get(&member[..]) {
                        Some(&s) => total = aggregate(total, weighted(s, w), how),
                        None => {
                            everywhere = false;
                            break;
                        }
                    }
                }
                if everywhere {
                    result.insert(member, total);
                }
            }
        }
        Op::Diff => {
            let Some((first, rest)) = inputs.split_first() else {
                return result;
            };
            let others: std::collections::HashSet<&[u8]> =
                rest.iter().flatten().map(|(_, m)| &m[..]).collect();
            for (score, member) in first {
                if !others.contains(&member[..]) {
                    result.insert(member, *score);
                }
            }
        }
    }
    result
}

/// `ZUNION`/`ZINTER`/`ZDIFF numkeys key [key ...] [WEIGHTS ...]
/// [AGGREGATE SUM|MIN|MAX] [WITHSCORES]` and their `STORE` forms (with the
/// destination first): Redis' `zunionInterDiffGenericCommand`, which reads
/// the inputs before the options.
fn algebra(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    op: Op,
    store_into: bool,
) -> Reply {
    let at = if store_into { 2 } else { 1 };
    let name = match (op, store_into) {
        (Op::Union, false) => "zunion",
        (Op::Inter, false) => "zinter",
        (Op::Diff, false) => "zdiff",
        (Op::Union, true) => "zunionstore",
        (Op::Inter, true) => "zinterstore",
        (Op::Diff, true) => "zdiffstore",
    };
    let Some(numkeys) = resp::parse_int(args.get(at)) else {
        return reply_error(out, NOT_AN_INTEGER);
    };
    if numkeys < 1 {
        let msg = format!("ERR at least 1 input key is needed for '{name}' command");
        return reply_error(out, msg.as_bytes());
    }
    let numkeys = numkeys as usize;
    if numkeys > args.len() - at - 1 {
        return reply_error(out, SYNTAX_ERROR);
    }
    let store = ctx.db(session);
    let Ok(inputs) = read_inputs(store, now, session, args, at + 1..at + 1 + numkeys) else {
        return reply_error(out, WRONGTYPE);
    };
    let (mut weights, mut how, mut with_scores) = (vec![1.0; numkeys], Aggregate::Sum, false);
    let mut i = at + 1 + numkeys;
    while i < args.len() {
        let opt = args.get(i);
        let is = |name: &[u8]| opt.eq_ignore_ascii_case(name);
        let left = args.len() - i - 1;
        if op != Op::Diff && left >= numkeys && is(b"WEIGHTS") {
            for (n, w) in weights.iter_mut().enumerate() {
                match parse_score(args.get(i + 1 + n)) {
                    Some(value) => *w = value,
                    None => return reply_error(out, b"ERR weight value is not a float"),
                }
            }
            i += numkeys;
        } else if op != Op::Diff && left >= 1 && is(b"AGGREGATE") {
            let value = args.get(i + 1);
            how = if value.eq_ignore_ascii_case(b"SUM") {
                Aggregate::Sum
            } else if value.eq_ignore_ascii_case(b"MIN") {
                Aggregate::Min
            } else if value.eq_ignore_ascii_case(b"MAX") {
                Aggregate::Max
            } else {
                return reply_error(out, SYNTAX_ERROR);
            };
            i += 1;
        } else if !store_into && is(b"WITHSCORES") {
            with_scores = true;
        } else {
            return reply_error(out, SYNTAX_ERROR);
        }
        i += 1;
    }
    let result = combine(inputs, &weights, how, op);
    if !store_into {
        let mut entries = Vec::with_capacity(result.len());
        result.for_range(0, result.len(), false, |s, m| entries.push((s, m.to_vec())));
        write_entries(out, &entries, with_scores, session.resp3, session.resp3);
        return Reply::Written;
    }
    let n = result.len();
    let mut result = Some(result);
    let stored = store.update_collection(args.key(store), now, true, |_| {
        (store_update(result.take().unwrap_or_default()), ())
    });
    match stored {
        Ok(()) => resp::write_integer(out, n as i64),
        Err(OutOfMemory) => resp::write_error(out, OOM),
    }
    Reply::Written
}

pub(super) fn zunion(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    algebra(c, s, n, a, o, Op::Union, false)
}

pub(super) fn zinter(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    algebra(c, s, n, a, o, Op::Inter, false)
}

pub(super) fn zdiff(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    algebra(c, s, n, a, o, Op::Diff, false)
}

pub(super) fn zunionstore(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    algebra(c, s, n, a, o, Op::Union, true)
}

pub(super) fn zinterstore(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    algebra(c, s, n, a, o, Op::Inter, true)
}

pub(super) fn zdiffstore(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    algebra(c, s, n, a, o, Op::Diff, true)
}

/// `ZINTERCARD numkeys key [key ...] [LIMIT limit]`: checked as `ZINTER` is,
/// inputs before options.
pub(super) fn zintercard(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let Some(numkeys) = resp::parse_int(args.get(1)) else {
        return reply_error(out, NOT_AN_INTEGER);
    };
    if numkeys < 1 {
        return reply_error(
            out,
            b"ERR at least 1 input key is needed for 'zintercard' command",
        );
    }
    let numkeys = numkeys as usize;
    if numkeys > args.len() - 2 {
        return reply_error(out, SYNTAX_ERROR);
    }
    let store = ctx.db(session);
    let Ok(inputs) = read_inputs(store, now, session, args, 2..2 + numkeys) else {
        return reply_error(out, WRONGTYPE);
    };
    let mut limit = 0;
    let mut i = 2 + numkeys;
    while i < args.len() {
        if args.get(i).eq_ignore_ascii_case(b"LIMIT") && i + 1 < args.len() {
            match resp::parse_int(args.get(i + 1)) {
                Some(n) if n >= 0 => limit = n as usize,
                _ => return reply_error(out, b"ERR LIMIT can't be negative"),
            }
            i += 2;
        } else {
            return reply_error(out, SYNTAX_ERROR);
        }
    }
    let n = combine(inputs, &vec![1.0; numkeys], Aggregate::Sum, Op::Inter).len();
    resp::write_integer(out, if limit > 0 { n.min(limit) } else { n } as i64);
    Reply::Written
}
