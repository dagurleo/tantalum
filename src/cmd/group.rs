//! Stream consumer groups (the state is in `src/stream.rs`): each command
//! checks its arguments and the key in Redis' order.
//!
//! As in Redis, what these do to a stream's groups isn't a change to clients
//! watching the key (a `Done::Unseen`), unless they make the key itself.

use super::stream::{
    Done, INVALID_ID, change, interval, parse_i64, parse_timeout_ms, read, reply_error, write_entry,
};
use super::{NOT_AN_INTEGER, SYNTAX_ERROR, WRONGTYPE, unknown_subcommand, wrong_arity};
use crate::aof;
use crate::blocking::{self, Attempt};
use crate::cmd::{Args, Reply};
use crate::context::{Context, Session};
use crate::resp;
use crate::store::{Key, Kind, Millis, Store};
use crate::stream::{self, BigStream, ConsumerId, Group, IdForm, StreamId, UNKNOWN_READS};

/// A name as Redis' `%s` shows it: up to a NUL byte.
fn shown(name: &[u8]) -> &[u8] {
    &name[..name.iter().position(|&b| b == 0).unwrap_or(name.len())]
}

/// `-NOGROUP No such consumer group 'group' for key name 'key'`.
fn no_such_group(key: &[u8], group: &[u8]) -> Vec<u8> {
    [
        b"NOGROUP No such consumer group '",
        shown(group),
        b"' for key name '",
        shown(key),
        b"'",
    ]
    .concat()
}

/// `-NOGROUP No such key 'key' or consumer group 'group'` and what follows.
fn no_such_key_or_group(key: &[u8], group: &[u8], rest: &[u8]) -> Vec<u8> {
    [
        b"NOGROUP No such key '",
        shown(key),
        b"' or consumer group '",
        shown(group),
        b"'",
        rest,
    ]
    .concat()
}

/// Redis' `addReplySubcommandSyntaxError`.
fn subcommand_syntax_error(args: &Args<'_>, command: &str) -> Vec<u8> {
    let sub = shown(args.get(1));
    [
        b"ERR unknown subcommand or wrong number of arguments for '",
        &sub[..sub.len().min(128)],
        b"'. Try ",
        command.as_bytes(),
        b" HELP.",
    ]
    .concat()
}

/// [`change`] for a command with errors of its own.
fn changing<R>(
    store: &Store,
    key: Key<'_>,
    now: Millis,
    grows: bool,
    f: impl FnMut(Option<&mut BigStream>) -> (Done, Result<R, Vec<u8>>),
) -> Result<R, Vec<u8>> {
    match change(store, key, now, grows, f) {
        Err(out_of_memory) => Err(out_of_memory.to_vec()),
        Ok(Err(())) => Err(WRONGTYPE.to_vec()),
        Ok(Ok(result)) => result,
    }
}

fn write_id(out: &mut Vec<u8>, id: StreamId) {
    let mut text = Vec::with_capacity(41);
    id.write(&mut text);
    resp::write_bulk(out, &text);
}

fn write_help(out: &mut Vec<u8>, lines: &[&str]) {
    resp::write_array_header(out, lines.len());
    for line in lines {
        out.push(b'+');
        out.extend_from_slice(line.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
}

fn null_array(session: &Session) -> &'static [u8] {
    if session.resp3 {
        resp::NULL
    } else {
        b"*-1\r\n"
    }
}

fn unix_now(store: &Store, now: Millis) -> i64 {
    store.unix_ms(now) as i64
}

/// `XREADGROUP`'s request.
struct GroupRead<'a> {
    group: &'a [u8],
    consumer: &'a [u8],
    count: usize,
    noack: bool,
    now: i64,
}

/// The ID a stream is read after ([`StreamId::MAX`] for `>`), once it's
/// checked that the stream has the group.
fn read_after(
    stream: Option<&BigStream>,
    key: &[u8],
    request: &GroupRead<'_>,
    id: &[u8],
) -> Result<StreamId, Vec<u8>> {
    if stream.is_none_or(|stream| !stream.groups.contains_key(request.group)) {
        return Err(no_such_key_or_group(
            key,
            request.group,
            b" in XREADGROUP with GROUP option",
        ));
    }
    match id {
        b"$" => Err(b"ERR The $ ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The $ ID would just return an empty result set.".to_vec()),
        b">" => Ok(StreamId::MAX),
        id => stream::parse_whole_id(id, 0, IdForm::Strict).ok_or_else(|| INVALID_ID.to_vec()),
    }
}

/// `XREADGROUP`'s reply for one stream, added to `body`: whether it has one.
/// And whether the consumer was made.
fn serve_stream(
    stream: &mut BigStream,
    key: &[u8],
    request: &GroupRead<'_>,
    after: StreamId,
    resp3: bool,
    body: &mut Vec<u8>,
) -> (bool, bool) {
    let mut entries = Vec::new();
    let Some((n, shows, created)) = stream.with_group(request.group, |stream, group| {
        let (consumer, created) = group.consumer(request.consumer, request.now);
        group.consumer_mut(consumer).seen_time = request.now;
        if after == StreamId::MAX {
            let propagating = crate::repl::propagate::active();
            let mut delivered = Vec::new();
            let n = stream.deliver(
                group,
                consumer,
                request.count,
                request.noack,
                request.now,
                |entry| {
                    if propagating {
                        delivered.push(entry.id);
                    }
                    write_entry(&mut entries, entry)
                },
            );
            if propagating && n > 0 {
                // As Redis propagates a read: each entry it delivered as its
                // claim, or with NOACK, where the group has read to.
                if request.noack {
                    if created {
                        crate::repl::propagate::xgroup_createconsumer(
                            key,
                            request.group,
                            request.consumer,
                        );
                    }
                    crate::repl::propagate::xgroup_setid(
                        key,
                        request.group,
                        group.last_id,
                        group.entries_read,
                    );
                } else {
                    for id in delivered {
                        crate::repl::propagate::xclaim(
                            key,
                            request.group,
                            request.consumer,
                            id,
                            request.now,
                            1,
                            id,
                        );
                    }
                }
            }
            return (n, n > 0, created);
        }
        // Its own history. (Only the largest ID has no next: it's taken for `>`.)
        let start = after.next().unwrap_or(StreamId::MAX);
        let n = stream.redeliver(
            group,
            consumer,
            start,
            request.count,
            request.now,
            |id, entry| match entry {
                Some(entry) => write_entry(&mut entries, entry),
                None => {
                    resp::write_array_header(&mut entries, 2);
                    write_id(&mut entries, id);
                    entries.extend_from_slice(if resp3 { resp::NULL } else { b"*-1\r\n" });
                }
            },
        );
        (n, true, created)
    }) else {
        return (false, false);
    };
    if shows {
        if !resp3 {
            resp::write_array_header(body, 2);
        }
        resp::write_bulk(body, key);
        resp::write_array_header(body, n);
        body.extend_from_slice(&entries);
    }
    (shows, created)
}

/// `XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] [NOACK] STREAMS key [key ...] id [id ...]`
pub(super) fn xreadgroup(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    blocking::block(c, s, n, a, o, xreadgroup_attempt)
}

fn xreadgroup_attempt(
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
    let mut names = None;
    let mut noack = false;
    let mut i = 1;
    while i < args.len() {
        let more = args.len() - i - 1;
        let opt = args.get(i);
        let is = |name: &[u8]| opt.eq_ignore_ascii_case(name);
        if is(b"BLOCK") && more > 0 {
            if session.in_script {
                let msg = [
                    b"ERR ",
                    args.get(0),
                    b" command is not allowed with BLOCK option from scripts",
                ]
                .concat();
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
                    b"ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified.",
                );
            }
            break;
        } else if is(b"GROUP") && more >= 2 {
            names = Some((args.get(i + 1), args.get(i + 2)));
            i += 2;
        } else if is(b"NOACK") {
            noack = true;
        } else {
            return error(out, SYNTAX_ERROR);
        }
        i += 1;
    }
    if streams_at == 0 {
        return error(out, SYNTAX_ERROR);
    }
    let Some((group, consumer)) = names else {
        return error(out, b"ERR Missing GROUP option for XREADGROUP");
    };
    let streams = (args.len() - streams_at) / 2;
    let store = ctx.db(session);
    let request = GroupRead {
        group,
        consumer,
        count,
        noack,
        now: unix_now(store, now),
    };
    let resp3 = session.resp3;
    let mut body = Vec::new();
    let mut served = 0;
    let mut created = Vec::new();
    // One stream, `after` found under the same lock; or several with theirs
    // found first, so that none is read if one can't be.
    let mut read_one =
        |session: &mut Session, k: usize, after: Option<StreamId>| -> Result<(), Vec<u8>> {
            let at = streams_at + k;
            let key = args.get(at);
            let id = args.get(streams_at + streams + k);
            let mut hit = false;
            let result = changing(store, args.key_at(store, at), now, false, |stream| {
                hit = stream.is_some();
                let after = match after {
                    Some(after) => after,
                    None => match read_after(stream.as_deref(), key, &request, id) {
                        Ok(after) => after,
                        Err(msg) => return (Done::Nothing, Err(msg)),
                    },
                };
                let Some(stream) = stream else {
                    return (Done::Nothing, Ok((false, false)));
                };
                let (shows, made) = serve_stream(stream, key, &request, after, resp3, &mut body);
                let done = if shows || made {
                    Done::Unseen
                } else {
                    // Only its seen time changed, which isn't worth saving.
                    Done::Nothing
                };
                (done, Ok((shows, made)))
            });
            session.count_lookup(hit);
            let (shows, made) = result?;
            served += usize::from(shows);
            if made {
                created.push(at);
            }
            Ok(())
        };
    let result = if streams == 1 {
        read_one(session, 0, None)
    } else {
        store.exclusive(|| {
            let mut afters = Vec::with_capacity(streams);
            for k in 0..streams {
                let at = streams_at + k;
                let after = read(store, args.key_at(store, at), now, session, |stream| {
                    read_after(
                        stream,
                        args.get(at),
                        &request,
                        args.get(streams_at + streams + k),
                    )
                })
                .map_err(|()| WRONGTYPE.to_vec())??;
                afters.push(after);
            }
            afters
                .into_iter()
                .enumerate()
                .try_for_each(|(k, after)| read_one(session, k, Some(after)))
        })
    };
    if let Err(msg) = result {
        return error(out, &msg);
    }
    if served > 0 {
        // A map in RESP3; in RESP2, an array of [key, entries] pairs.
        if resp3 {
            out.extend_from_slice(format!("%{served}\r\n").as_bytes());
        } else {
            resp::write_array_header(out, served);
        }
        out.extend_from_slice(&body);
        return Attempt::Done;
    }
    // Nothing is logged for this read, so a consumer it made is by itself.
    // (Redis propagates nothing for it, and its replicas go without.)
    for at in created {
        aof::record_parts(
            [
                &b"XGROUP"[..],
                b"CREATECONSUMER",
                args.get(at),
                group,
                consumer,
            ]
            .into_iter(),
        );
    }
    let Some(timeout) = block else {
        out.extend_from_slice(null_array(session));
        return Attempt::Done;
    };
    Attempt::Wait {
        on_change: true,
        keys: streams_at..streams_at + streams,
        timeout,
        kind: Kind::Stream,
        nil_bulk: false,
        args: None,
    }
}

const XGROUP_HELP: &[&str] = &[
    "XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    "CREATE <key> <groupname> <id|$> [option]",
    "    Create a new consumer group. Options are:",
    "    * MKSTREAM",
    "      Create the empty stream if it does not exist.",
    "    * ENTRIESREAD entries_read",
    "      Set the group's entries_read counter (internal use).",
    "CREATECONSUMER <key> <groupname> <consumer>",
    "    Create a new consumer in the specified group.",
    "DELCONSUMER <key> <groupname> <consumer>",
    "    Remove the specified consumer.",
    "DESTROY <key> <groupname>",
    "    Remove the specified group.",
    "SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]",
    "    Set the current group ID and entries_read counter.",
    "HELP",
    "    Print this help.",
];

/// What `XGROUP` replies, once it can't fail.
enum GroupReply {
    Ok,
    Int(i64),
}

/// `XGROUP CREATE|SETID|DESTROY|CREATECONSUMER|DELCONSUMER|HELP ...`
pub(super) fn xgroup(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let sub = args.get(1);
    let is = |name: &[u8]| sub.eq_ignore_ascii_case(name);
    let (create, setid, destroy, make, remove, help) = (
        is(b"CREATE"),
        is(b"SETID"),
        is(b"DESTROY"),
        is(b"CREATECONSUMER"),
        is(b"DELCONSUMER"),
        is(b"HELP"),
    );
    if !(create || setid || destroy || make || remove || help) {
        unknown_subcommand(out, sub, "XGROUP");
        return Reply::Written;
    }
    let fits = match () {
        () if create || setid => args.len() >= 5,
        () if destroy => args.len() == 4,
        () if help => args.len() == 2,
        () => args.len() == 5,
    };
    if !fits {
        let name = String::from_utf8_lossy(sub).to_ascii_lowercase();
        wrong_arity(out, &format!("xgroup|{name}"));
        return Reply::Written;
    }
    if help {
        write_help(out, XGROUP_HELP);
        return Reply::Written;
    }
    let mut mkstream = false;
    let mut entries_read = UNKNOWN_READS;
    let mut i = 5;
    while i < args.len() {
        let opt = args.get(i);
        if create && opt.eq_ignore_ascii_case(b"MKSTREAM") {
            mkstream = true;
            i += 1;
        } else if (create || setid)
            && opt.eq_ignore_ascii_case(b"ENTRIESREAD")
            && i + 1 < args.len()
        {
            match parse_i64(args.get(i + 1)) {
                None => return reply_error(out, NOT_AN_INTEGER),
                Some(n) if n < 0 && n != UNKNOWN_READS => {
                    return reply_error(out, b"ERR value for ENTRIESREAD must be positive or -1");
                }
                Some(n) => entries_read = n,
            }
            i += 2;
        } else {
            return reply_error(out, &subcommand_syntax_error(args, "XGROUP"));
        }
    }
    let store = ctx.db(session);
    let key = args.key_at(store, 2);
    let (key_name, group_name) = (args.get(2), args.get(3));
    let time = unix_now(store, now);
    let syntax = || subcommand_syntax_error(args, "XGROUP");
    let result = changing(store, key, now, create || make, |stream| {
        let Some(stream) = stream else {
            if !mkstream {
                return (
                    Done::Nothing,
                    Err(b"ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.".to_vec()),
                );
            }
            // CREATE with MKSTREAM, which can't fail once the ID is good.
            if args.len() > 8 {
                return (Done::Nothing, Err(syntax()));
            }
            let id = args.get(4);
            let id = if id == b"$" {
                Some(StreamId::MIN)
            } else {
                stream::parse_whole_id(id, 0, IdForm::Strict)
            };
            let Some(id) = id else {
                return (Done::Nothing, Err(INVALID_ID.to_vec()));
            };
            let mut stream = BigStream::default();
            stream
                .groups
                .insert(group_name.into(), Group::new(id, entries_read));
            return (Done::New(stream), Ok(GroupReply::Ok));
        };
        if !mkstream && (setid || make || remove) && !stream.groups.contains_key(group_name) {
            return (Done::Nothing, Err(no_such_group(key_name, group_name)));
        }
        if create {
            if args.len() > 8 {
                return (Done::Nothing, Err(syntax()));
            }
            let id = args.get(4);
            let id = if id == b"$" {
                Some(stream.last_id())
            } else {
                stream::parse_whole_id(id, 0, IdForm::Strict)
            };
            let Some(id) = id else {
                return (Done::Nothing, Err(INVALID_ID.to_vec()));
            };
            if stream.groups.contains_key(group_name) {
                return (
                    Done::Nothing,
                    Err(b"BUSYGROUP Consumer Group name already exists".to_vec()),
                );
            }
            stream
                .groups
                .insert(group_name.into(), Group::new(id, entries_read));
            (Done::Unseen, Ok(GroupReply::Ok))
        } else if setid {
            if args.len() != 5 && args.len() != 7 {
                return (Done::Nothing, Err(syntax()));
            }
            let id = args.get(4);
            let id = if id == b"$" {
                Some(stream.last_id())
            } else {
                stream::parse_whole_id(id, 0, IdForm::Interval)
            };
            let Some(id) = id else {
                return (Done::Nothing, Err(INVALID_ID.to_vec()));
            };
            if let Some(group) = stream.groups.get_mut(group_name) {
                group.last_id = id;
                group.entries_read = entries_read;
            }
            (Done::Unseen, Ok(GroupReply::Ok))
        } else if destroy {
            if stream.groups.remove(group_name).is_some() {
                (Done::Unseen, Ok(GroupReply::Int(1)))
            } else {
                (Done::Nothing, Ok(GroupReply::Int(0)))
            }
        } else if make {
            let Some(group) = stream.groups.get_mut(group_name) else {
                return (Done::Nothing, Ok(GroupReply::Int(0)));
            };
            let (_, made) = group.consumer(args.get(4), time);
            let done = if made { Done::Unseen } else { Done::Nothing };
            (done, Ok(GroupReply::Int(i64::from(made))))
        } else {
            let pending = stream
                .groups
                .get_mut(group_name)
                .and_then(|group| group.remove_consumer(args.get(4)));
            match pending {
                Some(pending) => (Done::Unseen, Ok(GroupReply::Int(pending as i64))),
                None => (Done::Nothing, Ok(GroupReply::Int(0))),
            }
        }
    });
    match result {
        Ok(GroupReply::Ok) => out.extend_from_slice(b"+OK\r\n"),
        Ok(GroupReply::Int(n)) => {
            resp::write_integer(out, n);
            // Clients blocked in XREADGROUP on the group get an error.
            if destroy && n == 1 {
                store.wake_waiters(key);
            }
        }
        Err(msg) => resp::write_error(out, &msg),
    }
    Reply::Written
}

/// `XSETID key last-id [ENTRIESADDED entries-added] [MAXDELETEDID max-deleted-id]`
pub(super) fn xsetid(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let Some(id) = stream::parse_whole_id(args.get(2), 0, IdForm::Strict) else {
        return reply_error(out, INVALID_ID);
    };
    let mut added = None;
    let mut max_deleted = StreamId::MIN;
    let mut i = 3;
    while i < args.len() {
        let more = args.len() - 1 - i;
        let opt = args.get(i);
        if opt.eq_ignore_ascii_case(b"ENTRIESADDED") && more > 0 {
            match parse_i64(args.get(i + 1)) {
                None => return reply_error(out, NOT_AN_INTEGER),
                Some(n) if n < 0 => {
                    return reply_error(out, b"ERR entries_added must be positive");
                }
                Some(n) => added = Some(n as u64),
            }
        } else if opt.eq_ignore_ascii_case(b"MAXDELETEDID") && more > 0 {
            let Some(deleted) = stream::parse_whole_id(args.get(i + 1), 0, IdForm::Strict) else {
                return reply_error(out, INVALID_ID);
            };
            if id < deleted {
                return reply_error(
                    out,
                    b"ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id",
                );
            }
            max_deleted = deleted;
        } else {
            return reply_error(out, SYNTAX_ERROR);
        }
        i += 2;
    }
    let store = ctx.db(session);
    let result = changing(store, args.key(store), now, false, |stream| {
        let Some(stream) = stream else {
            return (Done::Nothing, Err(b"ERR no such key".to_vec()));
        };
        let refuse = |msg: &[u8]| (Done::Nothing, Err(msg.to_vec()));
        if id < stream.max_deleted() {
            return refuse(
                b"ERR The ID specified in XSETID is smaller than current max_deleted_entry_id",
            );
        }
        if !stream.is_empty() {
            if id < stream.last_entry_id() {
                return refuse(
                    b"ERR The ID specified in XSETID is smaller than the target stream top item",
                );
            }
            if added.is_some_and(|added| (stream.len() as u64) > added) {
                return refuse(b"ERR The entries_added specified in XSETID is smaller than the target stream length");
            }
        }
        stream.set_ids(id, added, max_deleted);
        (Done::Unseen, Ok(()))
    });
    match result {
        Ok(()) => out.extend_from_slice(b"+OK\r\n"),
        Err(msg) => resp::write_error(out, &msg),
    }
    Reply::Written
}

/// `XACK key group id [id ...]`
pub(super) fn xack(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = ctx.db(session);
    let group_name = args.get(2);
    let mut hit = false;
    let result = changing(store, args.key(store), now, false, |stream| {
        hit = stream.is_some();
        // No key or group: nothing to acknowledge, before any ID is checked.
        let Some(group) = stream.and_then(|stream| stream.groups.get_mut(group_name)) else {
            return (Done::Nothing, Ok(0));
        };
        let mut ids = Vec::with_capacity(args.len() - 3);
        for i in 3..args.len() {
            match stream::parse_whole_id(args.get(i), 0, IdForm::Strict) {
                Some(id) => ids.push(id),
                None => return (Done::Nothing, Err(INVALID_ID.to_vec())),
            }
        }
        let acknowledged = ids.into_iter().filter(|&id| group.acknowledge(id)).count();
        let done = if acknowledged > 0 {
            Done::Unseen
        } else {
            Done::Nothing
        };
        (done, Ok(acknowledged))
    });
    session.count_lookup(hit);
    match result {
        Ok(n) => resp::write_integer(out, n as i64),
        Err(msg) => resp::write_error(out, &msg),
    }
    Reply::Written
}

/// `XPENDING key group [[IDLE min-idle-time] start end count [consumer]]`
pub(super) fn xpending(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let summary = args.len() == 3;
    if !summary && !(6..=9).contains(&args.len()) {
        return reply_error(out, SYNTAX_ERROR);
    }
    let mut start = StreamId::MIN;
    let mut end = StreamId::MAX;
    let mut count = 0;
    let mut min_idle = 0;
    let mut consumer = None;
    if !summary {
        let mut at = 3;
        if args.get(3).eq_ignore_ascii_case(b"IDLE") {
            match parse_i64(args.get(4)) {
                Some(n) => min_idle = n,
                None => return reply_error(out, NOT_AN_INTEGER),
            }
            if args.len() < 8 {
                return reply_error(out, SYNTAX_ERROR);
            }
            at = 5;
        }
        match parse_i64(args.get(at + 2)) {
            Some(n) => count = n.max(0) as usize,
            None => return reply_error(out, NOT_AN_INTEGER),
        }
        let Some((first, excluded)) = interval(args.get(at), 0) else {
            return reply_error(out, INVALID_ID);
        };
        start = first;
        if excluded {
            match first.next() {
                Some(next) => start = next,
                None => return reply_error(out, b"ERR invalid start ID for the interval"),
            }
        }
        let Some((last, excluded)) = interval(args.get(at + 1), u64::MAX) else {
            return reply_error(out, INVALID_ID);
        };
        end = last;
        if excluded {
            match last.previous() {
                Some(previous) => end = previous,
                None => return reply_error(out, b"ERR invalid end ID for the interval"),
            }
        }
        if at + 3 < args.len() {
            consumer = Some(args.get(at + 3));
        }
    }
    let store = ctx.db(session);
    let (key_name, group_name) = (args.get(1), args.get(2));
    let time = unix_now(store, now);
    let null = session.null();
    let null_list = null_array(session);
    let found = read(store, args.key(store), now, session, |stream| {
        let Some(group) = stream.and_then(|stream| stream.groups.get(group_name)) else {
            resp::write_error(out, &no_such_key_or_group(key_name, group_name, b""));
            return;
        };
        if summary {
            resp::write_array_header(out, 4);
            resp::write_integer(out, group.pending().len() as i64);
            let (Some(first), Some(last)) = (group.pending().first(), group.pending().last())
            else {
                out.extend_from_slice(null);
                out.extend_from_slice(null);
                out.extend_from_slice(null_list);
                return;
            };
            write_id(out, first);
            write_id(out, last);
            let busy = group.consumers().filter(|c| !c.pending().is_empty());
            resp::write_array_header(out, busy.clone().count());
            for consumer in busy {
                resp::write_array_header(out, 2);
                resp::write_bulk(out, consumer.name());
                resp::write_bulk(out, consumer.pending().len().to_string().as_bytes());
            }
            return;
        }
        let mut body = Vec::new();
        let mut n = 0;
        // Whether there's room for more.
        let mut add = |id: StreamId, pending: &stream::Pending| {
            let elapsed = time.wrapping_sub(pending.delivery_time);
            if min_idle != 0 && elapsed < min_idle {
                return true;
            }
            n += 1;
            resp::write_array_header(&mut body, 4);
            write_id(&mut body, id);
            resp::write_bulk(&mut body, group.owner(pending));
            resp::write_integer(&mut body, elapsed.max(0));
            resp::write_integer(&mut body, pending.delivery_count);
            n < count
        };
        if count > 0 && start <= end {
            match consumer {
                Some(name) => {
                    if let Some(consumer) = group.consumer_named(name) {
                        for (id, ()) in consumer.pending().range(start, end) {
                            if let Some(pending) = group.pending().get(id)
                                && !add(id, pending)
                            {
                                break;
                            }
                        }
                    }
                }
                None => {
                    for (id, pending) in group.pending().range(start, end) {
                        if !add(id, pending) {
                            break;
                        }
                    }
                }
            }
        }
        resp::write_array_header(out, n);
        out.extend_from_slice(&body);
    });
    if found.is_err() {
        resp::write_error(out, WRONGTYPE);
    }
    Reply::Written
}

/// `XCLAIM`'s options, read as Redis does.
struct Claim {
    min_idle: i64,
    ids: Vec<StreamId>,
    delivery: i64,
    retry: i64,
    force: bool,
    just_id: bool,
    last_id: StreamId,
}

fn parse_claim(args: &Args<'_>, now: i64) -> Result<Claim, Vec<u8>> {
    let integer = |i: usize, msg: &[u8]| parse_i64(args.get(i)).ok_or_else(|| msg.to_vec());
    let min_idle = integer(4, b"ERR Invalid min-idle-time argument for XCLAIM")?.max(0);
    let mut j = 5;
    let mut ids = Vec::with_capacity(args.len() - 5);
    while j < args.len() {
        let Some(id) = stream::parse_whole_id(args.get(j), 0, IdForm::Strict) else {
            break;
        };
        ids.push(id);
        j += 1;
    }
    let mut delivery = -1;
    let mut claim = Claim {
        min_idle,
        ids,
        delivery: now,
        retry: -1,
        force: false,
        just_id: false,
        last_id: StreamId::MIN,
    };
    while j < args.len() {
        let more = args.len() - 1 - j;
        let opt = args.get(j);
        let is = |name: &[u8]| opt.eq_ignore_ascii_case(name);
        if is(b"FORCE") {
            claim.force = true;
        } else if is(b"JUSTID") {
            claim.just_id = true;
        } else if is(b"IDLE") && more > 0 {
            j += 1;
            let idle = integer(j, b"ERR Invalid IDLE option argument for XCLAIM")?;
            delivery = now.wrapping_sub(idle);
        } else if is(b"TIME") && more > 0 {
            j += 1;
            delivery = integer(j, b"ERR Invalid TIME option argument for XCLAIM")?;
        } else if is(b"RETRYCOUNT") && more > 0 {
            j += 1;
            claim.retry = integer(j, b"ERR Invalid RETRYCOUNT option argument for XCLAIM")?;
        } else if is(b"LASTID") && more > 0 {
            j += 1;
            claim.last_id = stream::parse_whole_id(args.get(j), 0, IdForm::Strict)
                .ok_or_else(|| INVALID_ID.to_vec())?;
        } else {
            return Err([b"ERR Unrecognized XCLAIM option '", shown(opt), b"'"].concat());
        }
        j += 1;
    }
    // A time that makes no sense is taken as now, not an error: a client's
    // clock may run ahead.
    if delivery != -1 && (0..=now).contains(&delivery) {
        claim.delivery = delivery;
    }
    Ok(claim)
}

/// The consumer `name`, made if need be, as just seen.
/// A pending entry gone from the stream, which a claim takes out of the
/// pending lists: propagated as Redis does, as its claim by its owner.
fn propagate_gone(
    key: &[u8],
    group: &Group,
    group_name: &[u8],
    id: StreamId,
    delivered: i64,
    deliveries: i64,
) {
    if crate::repl::propagate::active()
        && let Some(pending) = group.pending().get(id)
    {
        crate::repl::propagate::xclaim(
            key,
            group_name,
            group.owner(pending),
            id,
            delivered,
            deliveries,
            group.last_id,
        );
    }
}

/// The consumer named, made if there's none, as just seen. And whether it
/// was made.
fn seen_consumer(group: &mut Group, name: &[u8], now: i64) -> (ConsumerId, bool) {
    let (consumer, created) = group.consumer(name, now);
    group.consumer_mut(consumer).seen_time = now;
    (consumer, created)
}

/// A consumer a claim made, if it claimed nothing (else its claims make it):
/// propagated, so that replicas have it. (Redis propagates nothing for it,
/// and its replicas go without.)
fn propagate_made(key: &[u8], group: &[u8], consumer: &[u8], created: bool, claimed: usize) {
    if created && claimed == 0 {
        crate::repl::propagate::xgroup_createconsumer(key, group, consumer);
    }
}

/// Adds entry `id` to a claim's reply.
fn write_claimed(out: &mut Vec<u8>, stream: &BigStream, id: StreamId, just_id: bool) {
    if just_id {
        write_id(out, id);
    } else {
        stream.range(id, id, false, |entry| {
            write_entry(out, entry);
            false
        });
    }
}

/// `XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME unix-ms] [RETRYCOUNT count] [FORCE] [JUSTID] [LASTID id]`
pub(super) fn xclaim(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = ctx.db(session);
    let (key_name, group_name) = (args.get(1), args.get(2));
    let time = unix_now(store, now);
    let mut hit = false;
    let result = changing(store, args.key(store), now, false, |stream| {
        hit = stream.is_some();
        let Some(stream) = stream.filter(|stream| stream.groups.contains_key(group_name)) else {
            return (
                Done::Nothing,
                Err(no_such_key_or_group(key_name, group_name, b"")),
            );
        };
        let claim = match parse_claim(args, time) {
            Ok(claim) => claim,
            Err(msg) => return (Done::Nothing, Err(msg)),
        };
        let reply = stream.with_group(group_name, |stream, group| {
            if claim.last_id > group.last_id {
                group.last_id = claim.last_id;
            }
            let (consumer, created) = seen_consumer(group, args.get(3), time);
            let mut body = Vec::new();
            let mut n = 0;
            for &id in &claim.ids {
                let pending = group
                    .pending()
                    .get(id)
                    .map(|p| (p.delivery_time, p.delivery_count));
                // Gone from the stream: gone from the pending lists too.
                if !stream.has_entry(id) {
                    if let Some((delivered, deliveries)) = pending {
                        propagate_gone(key_name, group, group_name, id, delivered, deliveries);
                        group.acknowledge(id);
                    }
                    continue;
                }
                let deliveries = match pending {
                    Some((delivered, deliveries)) => {
                        if claim.min_idle != 0 && time.wrapping_sub(delivered) < claim.min_idle {
                            continue;
                        }
                        deliveries
                    }
                    // FORCE makes one, delivered once so far.
                    None if claim.force => 1,
                    None => continue,
                };
                let deliveries = if claim.retry >= 0 {
                    claim.retry
                } else if claim.just_id {
                    deliveries
                } else {
                    deliveries.wrapping_add(1)
                };
                group.reassign(id, consumer, claim.delivery, deliveries);
                crate::repl::propagate::xclaim(
                    key_name,
                    group_name,
                    args.get(3),
                    id,
                    claim.delivery,
                    deliveries,
                    group.last_id,
                );
                write_claimed(&mut body, stream, id, claim.just_id);
                n += 1;
                group.consumer_mut(consumer).active_time = time;
            }
            propagate_made(key_name, group_name, args.get(3), created, n);
            let mut reply = Vec::with_capacity(body.len() + 16);
            resp::write_array_header(&mut reply, n);
            reply.extend_from_slice(&body);
            reply
        });
        (Done::Unseen, Ok(reply.unwrap_or_default()))
    });
    session.count_lookup(hit);
    match result {
        Ok(reply) => out.extend_from_slice(&reply),
        Err(msg) => resp::write_error(out, &msg),
    }
    Reply::Written
}

/// `XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]`
pub(super) fn xautoclaim(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let Some(min_idle) = parse_i64(args.get(4)) else {
        return reply_error(out, b"ERR Invalid min-idle-time argument for XAUTOCLAIM");
    };
    let min_idle = min_idle.max(0);
    let Some((mut start, excluded)) = interval(args.get(5), 0) else {
        return reply_error(out, INVALID_ID);
    };
    if excluded {
        match start.next() {
            Some(next) => start = next,
            None => return reply_error(out, b"ERR invalid start ID for the interval"),
        }
    }
    let mut count = 100;
    let mut just_id = false;
    let mut j = 6;
    while j < args.len() {
        let more = args.len() - 1 - j;
        let opt = args.get(j);
        if opt.eq_ignore_ascii_case(b"COUNT") && more > 0 {
            match parse_i64(args.get(j + 1)) {
                Some(n) if (1..=i64::MAX / 16).contains(&n) => count = n,
                _ => return reply_error(out, b"ERR COUNT must be > 0"),
            }
            j += 1;
        } else if opt.eq_ignore_ascii_case(b"JUSTID") {
            just_id = true;
        } else {
            return reply_error(out, SYNTAX_ERROR);
        }
        j += 1;
    }
    let store = ctx.db(session);
    let (key_name, group_name) = (args.get(1), args.get(2));
    let time = unix_now(store, now);
    let mut hit = false;
    let result = changing(store, args.key(store), now, false, |stream| {
        hit = stream.is_some();
        let Some(stream) = stream.filter(|stream| stream.groups.contains_key(group_name)) else {
            return (
                Done::Nothing,
                Err(no_such_key_or_group(key_name, group_name, b"")),
            );
        };
        // Redis allocates room for `count` IDs first, and says so if it can't:
        // more than any machine has.
        if count > 1 << 43 {
            return (
                Done::Nothing,
                Err(
                    b"ERR Insufficient memory, failed allocating transient memory, COUNT too high."
                        .to_vec(),
                ),
            );
        }
        let reply = stream.with_group(group_name, |stream, group| {
            let (consumer, created) = seen_consumer(group, args.get(3), time);
            let mut left = count;
            let mut attempts = count.saturating_mul(10);
            let mut from = Some(start);
            let mut body = Vec::new();
            let mut n = 0;
            let mut deleted = Vec::new();
            while attempts > 0 && left > 0 {
                attempts -= 1;
                let Some(at) = from else { break };
                let Some((id, pending)) = group.pending().first_from(at) else {
                    from = None;
                    break;
                };
                let (delivered, deliveries) = (pending.delivery_time, pending.delivery_count);
                from = id.next();
                // Gone from the stream: gone from the pending lists too.
                if !stream.has_entry(id) {
                    propagate_gone(key_name, group, group_name, id, delivered, deliveries);
                    group.acknowledge(id);
                    deleted.push(id);
                    left -= 1;
                    continue;
                }
                if min_idle != 0 && time.wrapping_sub(delivered) < min_idle {
                    continue;
                }
                let deliveries = if just_id {
                    deliveries
                } else {
                    deliveries.wrapping_add(1)
                };
                group.reassign(id, consumer, time, deliveries);
                crate::repl::propagate::xclaim(
                    key_name,
                    group_name,
                    args.get(3),
                    id,
                    time,
                    deliveries,
                    group.last_id,
                );
                write_claimed(&mut body, stream, id, just_id);
                n += 1;
                left -= 1;
                group.consumer_mut(consumer).active_time = time;
            }
            propagate_made(key_name, group_name, args.get(3), created, n);
            let next = from
                .and_then(|at| group.pending().first_from(at))
                .map(|(id, _)| id)
                .unwrap_or(StreamId::MIN);
            let mut reply = Vec::with_capacity(body.len() + 64);
            resp::write_array_header(&mut reply, 3);
            write_id(&mut reply, next);
            resp::write_array_header(&mut reply, n);
            reply.extend_from_slice(&body);
            resp::write_array_header(&mut reply, deleted.len());
            for id in deleted {
                write_id(&mut reply, id);
            }
            reply
        });
        (Done::Unseen, Ok(reply.unwrap_or_default()))
    });
    session.count_lookup(hit);
    match result {
        Ok(reply) => out.extend_from_slice(&reply),
        Err(msg) => resp::write_error(out, &msg),
    }
    Reply::Written
}

const XINFO_HELP: &[&str] = &[
    "XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    "CONSUMERS <key> <groupname>",
    "    Show consumers of <groupname>.",
    "GROUPS <key>",
    "    Show the stream consumer groups.",
    "STREAM <key> [FULL [COUNT <count>]",
    "    Show information about the stream.",
    "HELP",
    "    Print this help.",
];

fn field(out: &mut Vec<u8>, name: &str) {
    resp::write_bulk(out, name.as_bytes());
}

/// A group's lag, or a null if it can't be known.
fn write_lag(out: &mut Vec<u8>, stream: &BigStream, group: &Group, null: &[u8]) {
    match stream.lag(group) {
        Some(lag) => resp::write_integer(out, lag),
        None => out.extend_from_slice(null),
    }
}

fn write_entries_read(out: &mut Vec<u8>, group: &Group, null: &[u8]) {
    if group.entries_read == UNKNOWN_READS {
        out.extend_from_slice(null);
    } else {
        resp::write_integer(out, group.entries_read);
    }
}

/// `XINFO STREAM key [FULL [COUNT count]]`, the stream found.
fn xinfo_stream(stream: &BigStream, args: &Args<'_>, resp3: bool, null: &[u8], out: &mut Vec<u8>) {
    let options = args.len() - 3;
    let full = options > 0;
    let mut count = 10;
    if full {
        let fits = (options == 1 || options == 3)
            && args.get(3).eq_ignore_ascii_case(b"FULL")
            && (options == 1 || args.get(4).eq_ignore_ascii_case(b"COUNT"));
        if !fits {
            resp::write_error(out, &subcommand_syntax_error(args, "XINFO"));
            return;
        }
        if options == 3 {
            match parse_i64(args.get(5)) {
                Some(n) if n >= 0 => count = n as usize,
                Some(_) => {}
                None => {
                    resp::write_error(out, NOT_AN_INTEGER);
                    return;
                }
            }
        }
    }
    resp::write_map_header(out, if full { 9 } else { 10 }, resp3);
    field(out, "length");
    resp::write_integer(out, stream.len() as i64);
    field(out, "radix-tree-keys");
    resp::write_integer(out, stream.chunk_count() as i64);
    field(out, "radix-tree-nodes");
    resp::write_integer(out, stream.radix_nodes() as i64);
    field(out, "last-generated-id");
    write_id(out, stream.last_id());
    field(out, "max-deleted-entry-id");
    write_id(out, stream.max_deleted());
    field(out, "entries-added");
    resp::write_integer(out, stream.entries_added() as i64);
    field(out, "recorded-first-entry-id");
    write_id(out, stream.first_id());
    if !full {
        field(out, "groups");
        resp::write_integer(out, stream.groups.len() as i64);
        for (name, rev) in [("first-entry", false), ("last-entry", true)] {
            field(out, name);
            let mut emitted = false;
            stream.range(StreamId::MIN, StreamId::MAX, rev, |entry| {
                write_entry(out, entry);
                emitted = true;
                false
            });
            if !emitted {
                out.extend_from_slice(null);
            }
        }
        return;
    }
    field(out, "entries");
    let mut entries = Vec::new();
    let mut n = 0;
    stream.range(StreamId::MIN, StreamId::MAX, false, |entry| {
        write_entry(&mut entries, entry);
        n += 1;
        count == 0 || n < count
    });
    resp::write_array_header(out, n);
    out.extend_from_slice(&entries);
    field(out, "groups");
    resp::write_array_header(out, stream.groups.len());
    let limit = |len: usize| if count == 0 { len } else { len.min(count) };
    for (name, group) in &stream.groups {
        resp::write_map_header(out, 7, resp3);
        field(out, "name");
        resp::write_bulk(out, name);
        field(out, "last-delivered-id");
        write_id(out, group.last_id);
        field(out, "entries-read");
        write_entries_read(out, group, null);
        field(out, "lag");
        write_lag(out, stream, group, null);
        field(out, "pel-count");
        resp::write_integer(out, group.pending().len() as i64);
        field(out, "pending");
        let shown = limit(group.pending().len());
        resp::write_array_header(out, shown);
        for (id, pending) in group.pending().iter().take(shown) {
            resp::write_array_header(out, 4);
            write_id(out, id);
            resp::write_bulk(out, group.owner(pending));
            resp::write_integer(out, pending.delivery_time);
            resp::write_integer(out, pending.delivery_count);
        }
        field(out, "consumers");
        resp::write_array_header(out, group.consumer_count());
        for consumer in group.consumers() {
            resp::write_map_header(out, 5, resp3);
            field(out, "name");
            resp::write_bulk(out, consumer.name());
            field(out, "seen-time");
            resp::write_integer(out, consumer.seen_time);
            field(out, "active-time");
            resp::write_integer(out, consumer.active_time);
            field(out, "pel-count");
            resp::write_integer(out, consumer.pending().len() as i64);
            field(out, "pending");
            let shown = limit(consumer.pending().len());
            resp::write_array_header(out, shown);
            for (id, ()) in consumer.pending().iter().take(shown) {
                resp::write_array_header(out, 3);
                write_id(out, id);
                let pending = group.pending().get(id);
                resp::write_integer(out, pending.map_or(0, |p| p.delivery_time));
                resp::write_integer(out, pending.map_or(0, |p| p.delivery_count));
            }
        }
    }
}

/// `XINFO CONSUMERS key group`, `XINFO GROUPS key`, `XINFO STREAM key [FULL [COUNT count]]`, `XINFO HELP`
pub(super) fn xinfo(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let sub = args.get(1);
    let is = |name: &[u8]| sub.eq_ignore_ascii_case(name);
    let (consumers, groups, full, help) =
        (is(b"CONSUMERS"), is(b"GROUPS"), is(b"STREAM"), is(b"HELP"));
    if !(consumers || groups || full || help) {
        unknown_subcommand(out, sub, "XINFO");
        return Reply::Written;
    }
    let fits = match () {
        () if consumers => args.len() == 4,
        () if groups => args.len() == 3,
        () if full => args.len() >= 3,
        () => args.len() == 2,
    };
    if !fits {
        let name = String::from_utf8_lossy(sub).to_ascii_lowercase();
        wrong_arity(out, &format!("xinfo|{name}"));
        return Reply::Written;
    }
    if help {
        write_help(out, XINFO_HELP);
        return Reply::Written;
    }
    let store = ctx.db(session);
    let resp3 = session.resp3;
    let null = session.null();
    let time = unix_now(store, now);
    let key_name = args.get(2);
    let found = read(store, args.key_at(store, 2), now, session, |stream| {
        let Some(stream) = stream else {
            resp::write_error(out, b"ERR no such key");
            return;
        };
        if consumers {
            let Some(group) = stream.groups.get(args.get(3)) else {
                resp::write_error(out, &no_such_group(key_name, args.get(3)));
                return;
            };
            resp::write_array_header(out, group.consumer_count());
            for consumer in group.consumers() {
                resp::write_map_header(out, 4, resp3);
                field(out, "name");
                resp::write_bulk(out, consumer.name());
                field(out, "pending");
                resp::write_integer(out, consumer.pending().len() as i64);
                field(out, "idle");
                resp::write_integer(out, time.wrapping_sub(consumer.seen_time).max(0));
                field(out, "inactive");
                resp::write_integer(
                    out,
                    if consumer.active_time == -1 {
                        -1
                    } else {
                        time.wrapping_sub(consumer.active_time)
                    },
                );
            }
        } else if groups {
            resp::write_array_header(out, stream.groups.len());
            for (name, group) in &stream.groups {
                resp::write_map_header(out, 6, resp3);
                field(out, "name");
                resp::write_bulk(out, name);
                field(out, "consumers");
                resp::write_integer(out, group.consumer_count() as i64);
                field(out, "pending");
                resp::write_integer(out, group.pending().len() as i64);
                field(out, "last-delivered-id");
                write_id(out, group.last_id);
                field(out, "entries-read");
                write_entries_read(out, group, null);
                field(out, "lag");
                write_lag(out, stream, group, null);
            }
        } else {
            xinfo_stream(stream, args, resp3, null, out);
        }
    });
    if found.is_err() {
        resp::write_error(out, WRONGTYPE);
    }
    Reply::Written
}
