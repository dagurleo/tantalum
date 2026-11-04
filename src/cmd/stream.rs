//! Stream commands (see `src/stream.rs`). Each checks its arguments and the
//! key in Redis' order.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::{Args, NOT_AN_INTEGER, OOM, Reply, SYNTAX_ERROR, WRONGTYPE, wrong_arity};
use crate::blocking::{self, Attempt};
use crate::collection::Collection;
use crate::context::{Context, Session};
use crate::resp;
use crate::store::{Entry, EntryValue, Key, Kind, Millis, Store, Update, View};
use crate::stream::{self, AddError, BigStream, IdForm, StreamId, Trim};

const INVALID_ID: &[u8] = b"ERR Invalid stream ID specified as stream command argument";

fn reply_error(out: &mut Vec<u8>, msg: &[u8]) -> Reply {
    resp::write_error(out, msg);
    Reply::Written
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// Redis' `string2ll`, as `getLongLongFromObject` reads options.
fn parse_i64(arg: &[u8]) -> Option<i64> {
    resp::parse_int(arg)
}

/// Reads the stream at `key` under its lock: `f` gets `None` if there's
/// none. `Err` if the key holds another kind.
fn read<R>(
    store: &Store,
    key: Key<'_>,
    now: Millis,
    session: &mut Session,
    f: impl FnOnce(Option<&BigStream>) -> R,
) -> Result<R, ()> {
    let (result, hit) = store.get_key(key, now, |value| match value {
        None => (Ok(f(None)), false),
        Some(v) if v.kind() != Kind::Stream => (Err(()), true),
        Some(v) => match v.view() {
            View::Object(Collection::Stream(stream)) => (Ok(f(Some(stream))), true),
            _ => unreachable!("a stream is an object"),
        },
    });
    session.count_lookup(hit);
    result
}

/// What a change did.
enum Done {
    Nothing,
    Changed,
    /// Made a new stream, to store.
    New(BigStream),
}

/// Changes the stream at `key` with `f` (`None` if there's none). `Err` if
/// the key holds another kind.
fn change<R>(
    store: &Store,
    key: Key<'_>,
    now: Millis,
    grows: bool,
    mut f: impl FnMut(Option<&mut BigStream>) -> (Done, R),
) -> Result<Result<R, ()>, &'static [u8]> {
    store
        .update_collection(key, now, grows, |entry| match entry {
            None => match f(None) {
                (Done::New(stream), r) => (Update::Object(Collection::Stream(stream)), Ok(r)),
                (_, r) => (Update::Peek, Ok(r)),
            },
            Some(Entry {
                kind: Kind::Stream,
                value: EntryValue::Object(Collection::Stream(stream)),
                ..
            }) => match f(Some(stream)) {
                (Done::Changed, r) => (Update::Changed, Ok(r)),
                (_, r) => (Update::Peek, Ok(r)),
            },
            Some(_) => (Update::Peek, Err(())),
        })
        .map_err(|_| OOM)
}

/// `XADD`'s and `XTRIM`'s options, read as Redis'
/// `streamParseAddOrTrimArgsOrReply` does.
struct TrimArgs {
    trim: Option<Trim>,
    approx: bool,
    /// For `~`: at most this many go per call (0: no limit).
    limit: usize,
    no_mkstream: bool,
    /// `XADD`'s ID (`None` for `*`; no sequence for `ms-*`).
    id: Option<(u64, Option<u64>)>,
    /// Where the ID is (`XADD`).
    at: usize,
}

impl TrimArgs {
    fn parse(args: &Args<'_>, xadd: bool) -> Result<TrimArgs, &'static [u8]> {
        const INCOMPATIBLE: &[u8] =
            b"ERR syntax error, MAXLEN and MINID options at the same time are not compatible";
        let mut parsed = TrimArgs {
            trim: None,
            approx: false,
            limit: 0,
            no_mkstream: false,
            id: None,
            at: 0,
        };
        let mut limit = None;
        let mut i = 2;
        while i < args.len() {
            let more = args.len() - 1 - i;
            let opt = args.get(i);
            let is = |name: &[u8]| opt.eq_ignore_ascii_case(name);
            if xadd && opt == b"*" {
                break;
            } else if (is(b"MAXLEN") || is(b"MINID")) && more > 0 {
                if parsed.trim.is_some() {
                    return Err(INCOMPATIBLE);
                }
                parsed.approx = false;
                let next = args.get(i + 1);
                if more >= 2 && next == b"~" {
                    parsed.approx = true;
                    i += 1;
                } else if more >= 2 && next == b"=" {
                    i += 1;
                }
                let threshold = args.get(i + 1);
                parsed.trim = Some(if is(b"MAXLEN") {
                    match parse_i64(threshold) {
                        None => return Err(NOT_AN_INTEGER),
                        Some(n) if n < 0 => return Err(b"ERR The MAXLEN argument must be >= 0."),
                        Some(n) => Trim::MaxLen(n as usize),
                    }
                } else {
                    Trim::MinId(
                        stream::parse_whole_id(threshold, 0, IdForm::Strict).ok_or(INVALID_ID)?,
                    )
                });
                i += 1;
            } else if is(b"LIMIT") && more > 0 {
                match parse_i64(args.get(i + 1)) {
                    None => return Err(NOT_AN_INTEGER),
                    Some(n) if n < 0 => return Err(b"ERR The LIMIT argument must be >= 0."),
                    Some(n) => limit = Some(n as usize),
                }
                i += 1;
            } else if xadd && is(b"NOMKSTREAM") {
                parsed.no_mkstream = true;
            } else if xadd {
                parsed.id = Some(stream::parse_id(opt, 0, IdForm::Strict, true).ok_or(INVALID_ID)?);
                break;
            } else {
                return Err(SYNTAX_ERROR);
            }
            i += 1;
        }
        if limit.is_some_and(|l| l != 0) && parsed.trim.is_none() {
            return Err(
                b"ERR syntax error, LIMIT cannot be used without specifying a trimming strategy",
            );
        }
        if !xadd && parsed.trim.is_none() {
            return Err(b"ERR syntax error, XTRIM must be called with a trimming strategy");
        }
        parsed.limit = match limit {
            Some(_) if !parsed.approx => {
                return Err(b"ERR syntax error, LIMIT cannot be used without the special ~ option");
            }
            Some(limit) => limit,
            // Redis' default: 100 * stream-node-max-entries.
            None if parsed.approx => 10_000,
            None => 0,
        };
        parsed.at = i;
        Ok(parsed)
    }
}

/// `XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]] *|id field value [field value ...]`
pub(super) fn xadd(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let parsed = match TrimArgs::parse(args, true) {
        Ok(parsed) => parsed,
        Err(msg) => return reply_error(out, msg),
    };
    let fields_at = parsed.at + 1;
    if args.len() < fields_at + 2 || (args.len() - fields_at) % 2 == 1 {
        wrong_arity(out, "xadd");
        return Reply::Written;
    }
    if parsed.id == Some((0, Some(0))) {
        return reply_error(
            out,
            b"ERR The ID specified in XADD must be greater than 0-0",
        );
    }
    let pairs: Vec<(&[u8], &[u8])> = (fields_at..args.len())
        .step_by(2)
        .map(|i| (args.get(i), args.get(i + 1)))
        .collect();
    let now_ms = unix_ms();
    let add = |stream: &mut BigStream| -> Result<StreamId, AddError> {
        let id = stream.next_id(parsed.id, now_ms)?;
        stream.add(id, &pairs);
        if let Some(how) = parsed.trim {
            stream.trim(how, parsed.approx, parsed.limit);
        }
        Ok(id)
    };
    let store = &*ctx.store;
    let key = args.key(store);
    let added = change(store, key, now, true, |stream| match stream {
        None if parsed.no_mkstream => (Done::Nothing, Ok(None)),
        None => {
            let mut stream = BigStream::default();
            match add(&mut stream) {
                Ok(id) => (Done::New(stream), Ok(Some(id))),
                Err(e) => (Done::Nothing, Err(e)),
            }
        }
        Some(stream) => match add(stream) {
            Ok(id) => (Done::Changed, Ok(Some(id))),
            Err(e) => (Done::Nothing, Err(e)),
        },
    });
    match added {
        Err(msg) => resp::write_error(out, msg),
        Ok(Err(())) => resp::write_error(out, WRONGTYPE),
        Ok(Ok(Ok(None))) => out.extend_from_slice(session.null()),
        Ok(Ok(Ok(Some(id)))) => {
            let mut text = Vec::with_capacity(41);
            id.write(&mut text);
            resp::write_bulk(out, &text);
            // Clients blocked in XREAD on it may have something new.
            store.wake_waiters(key);
        }
        Ok(Ok(Err(AddError::TooSmall))) => resp::write_error(
            out,
            b"ERR The ID specified in XADD is equal or smaller than the target stream top item",
        ),
        Ok(Ok(Err(AddError::Exhausted))) => resp::write_error(
            out,
            b"ERR The stream has exhausted the last possible ID, unable to add more items",
        ),
    }
    Reply::Written
}

/// `XLEN key`
pub(super) fn xlen(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = &*ctx.store;
    match read(store, args.key(store), now, session, |s| {
        s.map_or(0, BigStream::len)
    }) {
        Ok(len) => resp::write_integer(out, len as i64),
        Err(()) => resp::write_error(out, WRONGTYPE),
    }
    Reply::Written
}

/// An entry as Redis replies one: its ID, and its fields and values.
fn write_entry(out: &mut Vec<u8>, entry: &stream::Entry<'_>) {
    resp::write_array_header(out, 2);
    let mut id = Vec::with_capacity(41);
    entry.id.write(&mut id);
    resp::write_bulk(out, &id);
    resp::write_array_header(out, entry.count * 2);
    for (field, value) in entry.pairs() {
        resp::write_bulk(out, field);
        resp::write_bulk(out, value);
    }
}

/// One end of an `XRANGE` interval: `(` for exclusive.
fn interval(arg: &[u8], missing_seq: u64) -> Option<(StreamId, bool)> {
    if arg.len() > 1 && arg[0] == b'(' {
        Some((
            stream::parse_whole_id(&arg[1..], missing_seq, IdForm::Strict)?,
            true,
        ))
    } else {
        Some((
            stream::parse_whole_id(arg, missing_seq, IdForm::Interval)?,
            false,
        ))
    }
}

fn range_command(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    rev: bool,
) -> Reply {
    let (start_arg, end_arg) = if rev {
        (args.get(3), args.get(2))
    } else {
        (args.get(2), args.get(3))
    };
    let Some((mut start, start_ex)) = interval(start_arg, 0) else {
        return reply_error(out, INVALID_ID);
    };
    if start_ex {
        match start.next() {
            Some(next) => start = next,
            None => return reply_error(out, b"ERR invalid start ID for the interval"),
        }
    }
    let Some((mut end, end_ex)) = interval(end_arg, u64::MAX) else {
        return reply_error(out, INVALID_ID);
    };
    if end_ex {
        match end.previous() {
            Some(previous) => end = previous,
            None => return reply_error(out, b"ERR invalid end ID for the interval"),
        }
    }
    let mut count = None;
    let mut j = 4;
    while j < args.len() {
        if args.get(j).eq_ignore_ascii_case(b"COUNT") && j + 1 < args.len() {
            match parse_i64(args.get(j + 1)) {
                Some(n) => count = Some(n.max(0) as usize),
                None => return reply_error(out, NOT_AN_INTEGER),
            }
            j += 2;
        } else {
            return reply_error(out, SYNTAX_ERROR);
        }
    }
    let store = &*ctx.store;
    let reply = read(store, args.key(store), now, session, |stream| {
        let Some(stream) = stream else {
            return b"*0\r\n".to_vec();
        };
        if count == Some(0) {
            return b"*-1\r\n".to_vec();
        }
        let mut body = Vec::new();
        let mut n = 0;
        stream.range(start, end, rev, |entry| {
            write_entry(&mut body, entry);
            n += 1;
            count.is_none_or(|count| n < count)
        });
        let mut reply = Vec::with_capacity(body.len() + 16);
        resp::write_array_header(&mut reply, n);
        reply.extend_from_slice(&body);
        reply
    });
    match reply {
        Ok(reply) if reply == b"*-1\r\n" && session.resp3 => out.extend_from_slice(resp::NULL),
        Ok(reply) => out.extend_from_slice(&reply),
        Err(()) => resp::write_error(out, WRONGTYPE),
    }
    Reply::Written
}

/// `XRANGE key start end [COUNT count]`
pub(super) fn xrange(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    range_command(c, s, n, a, o, false)
}

/// `XREVRANGE key end start [COUNT count]`
pub(super) fn xrevrange(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    range_command(c, s, n, a, o, true)
}

/// `XDEL key id [id ...]`: how many were there.
pub(super) fn xdel(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = &*ctx.store;
    // A missing key is 0 before any ID is checked, as in Redis.
    let deleted = change(store, args.key(store), now, false, |stream| {
        let Some(stream) = stream else {
            return (Done::Nothing, Ok(0));
        };
        let mut ids = Vec::with_capacity(args.len() - 2);
        for i in 2..args.len() {
            match stream::parse_whole_id(args.get(i), 0, IdForm::Strict) {
                Some(id) => ids.push(id),
                None => return (Done::Nothing, Err(INVALID_ID)),
            }
        }
        let deleted = ids.into_iter().filter(|&id| stream.delete(id)).count();
        let done = if deleted > 0 {
            Done::Changed
        } else {
            Done::Nothing
        };
        (done, Ok(deleted))
    });
    match deleted {
        Err(msg) | Ok(Ok(Err(msg))) => resp::write_error(out, msg),
        Ok(Err(())) => resp::write_error(out, WRONGTYPE),
        Ok(Ok(Ok(n))) => resp::write_integer(out, n as i64),
    }
    Reply::Written
}

/// `XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count]`: how many went.
pub(super) fn xtrim(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let parsed = match TrimArgs::parse(args, false) {
        Ok(parsed) => parsed,
        Err(msg) => return reply_error(out, msg),
    };
    let how = parsed.trim.expect("XTRIM has a strategy");
    let store = &*ctx.store;
    let trimmed = change(store, args.key(store), now, false, |stream| {
        let Some(stream) = stream else {
            return (Done::Nothing, 0);
        };
        let n = stream.trim(how, parsed.approx, parsed.limit);
        (if n > 0 { Done::Changed } else { Done::Nothing }, n)
    });
    match trimmed {
        Err(msg) => resp::write_error(out, msg),
        Ok(Err(())) => resp::write_error(out, WRONGTYPE),
        Ok(Ok(n)) => resp::write_integer(out, n as i64),
    }
    Reply::Written
}

/// A blocking timeout in milliseconds, as Redis reads `XREAD`'s `BLOCK`.
fn parse_timeout_ms(arg: &[u8]) -> Result<Option<Duration>, &'static [u8]> {
    let Some(ms) = parse_i64(arg) else {
        return Err(b"ERR timeout is not an integer or out of range");
    };
    if ms < 0 {
        return Err(b"ERR timeout is negative");
    }
    if ms == 0 {
        return Ok(None);
    }
    if ms > i64::MAX - unix_ms() as i64 {
        return Err(b"ERR timeout is out of range");
    }
    Ok(Some(Duration::from_millis(ms as u64)))
}

/// `XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]`
pub(super) fn xread(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    blocking::block(c, s, n, a, o, xread_attempt)
}

fn xread_attempt(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Attempt {
    let error = |out: &mut Vec<u8>, msg: &[u8]| {
        resp::write_error(out, msg);
        Attempt::Done
    };
    let mut block = None;
    let mut count = 0;
    let mut streams_at = 0;
    let mut i = 1;
    while i < args.len() {
        let more = args.len() - i - 1;
        let opt = args.get(i);
        let is = |name: &[u8]| opt.eq_ignore_ascii_case(name);
        if is(b"BLOCK") && more > 0 {
            if session.in_script {
                let mut msg = b"ERR ".to_vec();
                msg.extend_from_slice(args.get(0));
                msg.extend_from_slice(b" command is not allowed with BLOCK option from scripts");
                return error(out, &msg);
            }
            i += 1;
            match parse_timeout_ms(args.get(i)) {
                Ok(timeout) => block = Some(timeout),
                Err(msg) => return error(out, msg),
            }
        } else if is(b"COUNT") && more > 0 {
            i += 1;
            match parse_i64(args.get(i)) {
                Some(n) => count = n.max(0) as usize,
                None => return error(out, NOT_AN_INTEGER),
            }
        } else if is(b"STREAMS") && more > 0 {
            streams_at = i + 1;
            if !(args.len() - streams_at).is_multiple_of(2) {
                return error(
                    out,
                    b"ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.",
                );
            }
            break;
        } else if is(b"GROUP") && more >= 2 {
            return error(
                out,
                b"ERR The GROUP option is only supported by XREADGROUP. You called XREAD instead.",
            );
        } else if is(b"NOACK") {
            return error(
                out,
                b"ERR The NOACK option is only supported by XREADGROUP. You called XREAD instead.",
            );
        } else {
            return error(out, SYNTAX_ERROR);
        }
        i += 1;
    }
    if streams_at == 0 {
        return error(out, SYNTAX_ERROR);
    }
    let streams = (args.len() - streams_at) / 2;
    let store = &*ctx.store;
    // Every key's type, then its ID, before anything is read.
    let mut ids = Vec::with_capacity(streams);
    let mut dollar = false;
    for k in 0..streams {
        let key = args.key_at(store, streams_at + k);
        let Ok(last) = read(store, key, now, session, |s| s.map(BigStream::last_id)) else {
            return error(out, WRONGTYPE);
        };
        let id = args.get(streams_at + streams + k);
        if id == b"$" {
            ids.push(last.unwrap_or(StreamId::MIN));
            dollar = true;
        } else if id == b">" {
            return error(
                out,
                b"ERR The > ID can be specified only when calling XREADGROUP using the GROUP <group> <consumer> option.",
            );
        } else {
            match stream::parse_whole_id(id, 0, IdForm::Strict) {
                Some(id) => ids.push(id),
                None => return error(out, INVALID_ID),
            }
        }
    }
    let mut body = Vec::new();
    let mut served = 0;
    let resp3 = session.resp3;
    for (k, &after) in ids.iter().enumerate() {
        let key_at = streams_at + k;
        let read = read(store, args.key_at(store, key_at), now, session, |stream| {
            let (Some(stream), Some(start)) = (stream, after.next()) else {
                return;
            };
            let mut entries = Vec::new();
            let mut n = 0;
            stream.range(start, StreamId::MAX, false, |entry| {
                write_entry(&mut entries, entry);
                n += 1;
                count == 0 || n < count
            });
            if n > 0 {
                served += 1;
                if !resp3 {
                    resp::write_array_header(&mut body, 2);
                }
                resp::write_bulk(&mut body, args.get(key_at));
                resp::write_array_header(&mut body, n);
                body.extend_from_slice(&entries);
            }
        });
        if read.is_err() {
            return error(out, WRONGTYPE);
        }
    }
    if served > 0 {
        // A map in RESP3; in RESP2, an array of [key, entries] pairs.
        if session.resp3 {
            out.extend_from_slice(format!("%{served}\r\n").as_bytes());
        } else {
            resp::write_array_header(out, served);
        }
        out.extend_from_slice(&body);
        return Attempt::Done;
    }
    let Some(timeout) = block else {
        out.extend_from_slice(if session.resp3 {
            resp::NULL
        } else {
            b"*-1\r\n"
        });
        return Attempt::Done;
    };
    // Waiting, `$` means the last ID now, as Redis rewrites it.
    let rewritten = dollar.then(|| {
        (0..args.len())
            .map(|i| {
                let at_id = i >= streams_at + streams && args.get(i) == b"$";
                if at_id {
                    let mut text = Vec::new();
                    ids[i - streams_at - streams].write(&mut text);
                    text
                } else {
                    args.get(i).to_vec()
                }
            })
            .collect()
    });
    Attempt::Wait {
        keys: streams_at..streams_at + streams,
        timeout,
        kind: Kind::Stream,
        nil_bulk: false,
        args: rewritten,
    }
}
