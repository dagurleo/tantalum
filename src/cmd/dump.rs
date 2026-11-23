//! `DUMP`, `RESTORE` and `MIGRATE`: a value in Redis' RDB format
//! (`src/rdb.rs`), so values move between Redis and Tantalum both ways. A
//! small collection is written in the encoding Redis would hold it in (a
//! listpack or an intset, as `OBJECT ENCODING` says), byte for byte as Redis
//! writes it; a big one as Redis' hash table, skip list or quicklist would be,
//! in Tantalum's own order. `RESTORE` reads every encoding Redis 2.6 to 7.2
//! writes but modules'.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, ErrorKind, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use super::{Args, NOT_AN_INTEGER, OOM, Reply, SYNTAX_ERROR, update};
use crate::collection::Collection;
use crate::context::{Context, Session};
use crate::hash::BigHash;
use crate::list::{self, BigList};
use crate::rdb::{self, Object, SetEncoding};
use crate::resp;
use crate::set::{self, BigSet};
use crate::store::{Expiry, Kind, Millis, Store, Update, ValueRef, View};
use crate::zset::{self, BigZSet};

/// The `DUMP` payload of a value: its type, the value, the RDB version and a
/// checksum.
pub(crate) fn payload_of(value: &ValueRef<'_>) -> Vec<u8> {
    let mut out = Vec::new();
    value_body(&mut out, value);
    rdb::finish_payload(&mut out);
    out
}

/// A value as `DUMP` payloads and RDB files have it: its RDB type, then the
/// value, without a payload's footer.
pub(crate) fn value_body(out: &mut Vec<u8>, value: &ValueRef<'_>) {
    match value.kind() {
        Kind::String => rdb::dump_string(out, value.as_bytes()),
        Kind::List => match value.view() {
            View::Bytes(bytes) => rdb::dump_list(out, list::Packed::new(bytes).iter()),
            View::Object(Collection::List(big)) => rdb::dump_list(out, big.iter()),
            View::Object(_) => unreachable!("a list's object is a list"),
        },
        Kind::Set => {
            let mut members = Vec::new();
            match value.view() {
                View::Bytes(bytes) => {
                    for member in set::Compact::new(bytes).iter() {
                        member.with_bytes(|m| members.push(m.to_vec()));
                    }
                }
                View::Object(Collection::Set(big)) => {
                    members.extend(big.iter().map(<[u8]>::to_vec));
                }
                View::Object(_) => unreachable!("a set's object is a set"),
            }
            let encoding = match super::keys::encoding(value) {
                "intset" => SetEncoding::Intset,
                "listpack" => SetEncoding::Listpack,
                _ => SetEncoding::Hashtable,
            };
            let len = members.len();
            rdb::dump_set(out, members.iter().map(Vec::as_slice), len, encoding);
        }
        Kind::ZSet => {
            let mut members: Vec<(f64, Vec<u8>)> = Vec::new();
            match value.view() {
                View::Bytes(bytes) => {
                    members.extend(
                        zset::Packed::new(bytes)
                            .iter()
                            .map(|(s, m)| (s, m.to_vec())),
                    );
                }
                View::Object(Collection::ZSet(big)) => {
                    big.for_range(0, big.len(), false, |s, m| members.push((s, m.to_vec())));
                }
                View::Object(_) => unreachable!("a sorted set's object is a sorted set"),
            }
            let listpack = super::keys::encoding(value) == "listpack";
            let borrowed: Vec<(f64, &[u8])> =
                members.iter().map(|(s, m)| (*s, m.as_slice())).collect();
            rdb::dump_zset(out, &borrowed, listpack);
        }
        Kind::Hash => {
            let view = super::hash::view(value.view());
            rdb::dump_hash(out, view.iter(), view.len(), view.fits_listpack());
        }
        Kind::Stream => match value.view() {
            View::Object(Collection::Stream(stream)) => rdb::dump_stream(out, stream),
            _ => unreachable!("a stream is an object"),
        },
    }
}

/// `DUMP key`.
pub(super) fn dump(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = ctx.db(session);
    let payload = store.get_key(args.key(store), now, |value| value.map(|v| payload_of(&v)));
    session.count_lookup(payload.is_some());
    match payload {
        Some(payload) => resp::write_bulk(out, &payload),
        None => out.extend_from_slice(session.null()),
    }
    Reply::Written
}

/// A value read from a payload, in the packed format of its kind, for
/// `Store::restore`.
pub(crate) fn packed(object: Object) -> (Kind, Vec<u8>) {
    match object {
        Object::String(value) => (Kind::String, value),
        Object::List(elements) => {
            let mut list = BigList::default();
            for element in &elements {
                list.push_back(element);
            }
            (Kind::List, list.to_packed())
        }
        Object::Set(members) => {
            let mut set = BigSet::with_capacity(members.len());
            for member in &members {
                set.insert(member);
            }
            (Kind::Set, set.to_compact())
        }
        Object::ZSet(members) => {
            let mut set = BigZSet::default();
            for (member, score) in &members {
                set.insert(member, *score);
            }
            (Kind::ZSet, set.to_packed())
        }
        Object::Hash(pairs) => {
            // The packed format, which `BigHash` reads with what's repeated
            // dropped: a count, then each field and value after its length.
            let mut bytes = Vec::new();
            put_varint(&mut bytes, pairs.len());
            for (field, value) in &pairs {
                put_varint(&mut bytes, field.len());
                bytes.extend_from_slice(field);
                put_varint(&mut bytes, value.len());
                bytes.extend_from_slice(value);
            }
            (Kind::Hash, BigHash::from_packed(&bytes).to_packed())
        }
        Object::Stream(compact) => (Kind::Stream, compact),
    }
}

fn put_varint(out: &mut Vec<u8>, mut n: usize) {
    while n >= 0x80 {
        out.push(n as u8 | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

fn exists(store: &Store, args: &Args<'_>, i: usize, now: Millis) -> bool {
    update(store, args.key_at(store, i), now, |current| {
        (Update::Peek, current.is_some())
    })
}

fn delete(store: &Store, args: &Args<'_>, i: usize, now: Millis) -> bool {
    update(store, args.key_at(store, i), now, |current| match current {
        Some(_) => (Update::Delete, true),
        None => (Update::Peek, false),
    })
}

/// `RESTORE key ttl payload [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]`,
/// checked in Redis' order. No access times or frequencies are kept here, so
/// those two are checked and go no further.
pub(super) fn restore(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = ctx.db(session);
    let (mut replace, mut absttl, mut idle, mut freq) = (false, false, false, false);
    let mut j = 4;
    while j < args.len() {
        let more = args.len() - j - 1;
        let option = args.get(j);
        let is = |name: &[u8]| option.eq_ignore_ascii_case(name);
        if is(b"REPLACE") {
            replace = true;
        } else if is(b"ABSTTL") {
            absttl = true;
        } else if is(b"IDLETIME") && more >= 1 && !freq {
            match resp::parse_int(args.get(j + 1)) {
                Some(n) if n >= 0 => idle = true,
                Some(_) => {
                    resp::write_error(out, b"ERR Invalid IDLETIME value, must be >= 0");
                    return Reply::Written;
                }
                None => {
                    resp::write_error(out, NOT_AN_INTEGER);
                    return Reply::Written;
                }
            }
            j += 1;
        } else if is(b"FREQ") && more >= 1 && !idle {
            match resp::parse_int(args.get(j + 1)) {
                Some(0..=255) => freq = true,
                Some(_) => {
                    resp::write_error(out, b"ERR Invalid FREQ value, must be >= 0 and <= 255");
                    return Reply::Written;
                }
                None => {
                    resp::write_error(out, NOT_AN_INTEGER);
                    return Reply::Written;
                }
            }
            j += 1;
        } else {
            resp::write_error(out, SYNTAX_ERROR);
            return Reply::Written;
        }
        j += 1;
    }
    if !replace && exists(store, args, 1, now) {
        resp::write_error(out, b"BUSYKEY Target key name already exists.");
        return Reply::Written;
    }
    let ttl = match resp::parse_int(args.get(2)) {
        Some(ttl) if ttl >= 0 => ttl,
        Some(_) => {
            resp::write_error(out, b"ERR Invalid TTL value, must be >= 0");
            return Reply::Written;
        }
        None => {
            resp::write_error(out, NOT_AN_INTEGER);
            return Reply::Written;
        }
    };
    if rdb::payload_body(args.get(3)).is_none() {
        resp::write_error(out, b"ERR DUMP payload version or checksum are wrong");
        return Reply::Written;
    }
    // From the whole payload, as Redis reads it: what says it's longer than the
    // body reads into the footer.
    let mut reader = rdb::Reader::new(args.get(3));
    let Some(object) = reader
        .byte()
        .and_then(|kind| rdb::load_object(kind, &mut reader, ctx.stream_limits()))
    else {
        resp::write_error(out, b"ERR Bad data format");
        return Reply::Written;
    };
    if replace {
        delete(store, args, 1, now);
    }
    let expiry = if ttl == 0 {
        Expiry::Never
    } else {
        // When, in Unix milliseconds. Past it already (or past what a time can
        // be, where Redis' sum wraps around), it's made and gone at once:
        // nothing is made, and it's OK.
        let unix_now = store.deadline_to_unix_ms(now, now);
        let at = if absttl {
            Some(ttl)
        } else {
            unix_now.checked_add(ttl)
        };
        match at {
            // (On a replica, the master's word: it's made, and goes when it says.)
            Some(at) if at > unix_now || crate::segment::expiry_suspended() => {
                Expiry::At(store.deadline_from_unix_ms(now, at))
            }
            _ => {
                out.extend_from_slice(resp::OK);
                return Reply::Written;
            }
        }
    };
    let (kind, value) = packed(object);
    match store.restore(args.get(1), kind, &value, expiry, now) {
        Ok(()) => out.extend_from_slice(resp::OK),
        Err(_) => resp::write_error(out, OOM),
    }
    Reply::Written
}

/// How long `MIGRATE` keeps a connection it isn't using, and how many it keeps:
/// Redis' `MIGRATE_SOCKET_CACHE_TTL` and `_ITEMS`.
const CACHE_TTL: Duration = Duration::from_secs(10);
const CACHE_ITEMS: usize = 64;

const CONNECT_ERROR: &[u8] = b"IOERR error or timeout connecting to the client";
const WRITE_ERROR: &[u8] = b"IOERR error or timeout writing to target instance";
const READ_ERROR: &[u8] = b"IOERR error or timeout reading to target instance";

/// The connections `MIGRATE` keeps to where it sends keys, as Redis keeps them:
/// by `host:port` as given, each with the database it last selected, until
/// unused for 10 seconds (closed when `MIGRATE` or `INFO` next looks). One in
/// use is out of the cache, so that another thread's `MIGRATE` to the same
/// place opens its own.
#[derive(Default)]
pub(crate) struct MigrateSockets(Mutex<HashMap<Vec<u8>, Cached>>);

struct Cached {
    stream: TcpStream,
    /// What it last selected, or -1 if that isn't known, as Redis has it (so
    /// that after an error, `MIGRATE` to database -1 doesn't select).
    db: i64,
    used: Instant,
}

impl MigrateSockets {
    fn open(&self) -> std::sync::MutexGuard<'_, HashMap<Vec<u8>, Cached>> {
        let mut cache = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        cache.retain(|_, cached| cached.used.elapsed() <= CACHE_TTL);
        cache
    }

    /// How many it keeps: `INFO`'s `migrate_cached_sockets`.
    pub(crate) fn count(&self) -> usize {
        self.open().len()
    }

    fn take(&self, name: &[u8]) -> Option<Cached> {
        self.open().remove(name)
    }

    fn put(&self, name: Vec<u8>, mut cached: Cached) {
        cached.used = Instant::now();
        let mut cache = self.open();
        if cache.len() >= CACHE_ITEMS && !cache.contains_key(&name) {
            // Too many: one goes, as in Redis (which picks it at random).
            if let Some(other) = cache.keys().next().cloned() {
                cache.remove(&other);
            }
        }
        cache.insert(name, cached);
    }
}

/// C's `atoi`, which Redis reads `MIGRATE`'s port with.
fn atoi(s: &[u8]) -> i32 {
    let s = s.trim_ascii_start();
    let (negative, digits) = match s.first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    let n = digits
        .iter()
        .take_while(|b| b.is_ascii_digit())
        .fold(0i64, |n, b| {
            n.saturating_mul(10).saturating_add(i64::from(b - b'0'))
        });
    (if negative { -n } else { n }) as i32
}

/// Connects as Redis' `connBlockingConnect` does. Its connect doesn't wait to
/// hear back, so a refusal only shows when it writes: that's `Err(WRITE_ERROR)`,
/// and anything else `Err(CONNECT_ERROR)`.
fn connect(host: &[u8], port: &[u8], timeout: Duration) -> Result<TcpStream, &'static [u8]> {
    // The port as the C library has it after `atoi`: in 16 bits.
    let port = atoi(port) as u16;
    let host = std::str::from_utf8(host).map_err(|_| CONNECT_ERROR)?;
    let addrs = (host, port).to_socket_addrs().map_err(|_| CONNECT_ERROR)?;
    for addr in addrs {
        match TcpStream::connect_timeout(&addr, timeout) {
            Ok(stream) => return Ok(stream),
            Err(e) => match e.kind() {
                ErrorKind::ConnectionRefused | ErrorKind::HostUnreachable => {
                    return Err(WRITE_ERROR);
                }
                ErrorKind::TimedOut | ErrorKind::WouldBlock => return Err(CONNECT_ERROR),
                // One it couldn't even try: the next.
                _ => {}
            },
        }
    }
    Err(CONNECT_ERROR)
}

/// What `AUTH` and `SELECT` replied, those of them sent.
type FirstReplies = (Option<Vec<u8>>, Option<Vec<u8>>);

/// A reply's line, without its line end, or `Err(timed out)`.
fn read_line(reader: &mut impl BufRead) -> Result<Vec<u8>, bool> {
    let mut line = Vec::new();
    match reader.read_until(b'\n', &mut line) {
        Ok(_) if line.ends_with(b"\n") => {
            line.pop();
            if line.ends_with(b"\r") {
                line.pop();
            }
            // What Redis' buffer for it holds.
            line.truncate(1023);
            Ok(line)
        }
        Ok(_) => Err(false),
        Err(e) => Err(timed_out(&e)),
    }
}

fn timed_out(e: &std::io::Error) -> bool {
    matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut)
}

/// `MIGRATE host port key|"" destination-db timeout [COPY] [REPLACE] [AUTH password |
/// AUTH2 username password] [KEYS key ...]`: `RESTORE`s the keys there, and deletes
/// them here unless `COPY`, step by step as Redis' `migrateCommand`: over a
/// connection it keeps (see [`MigrateSockets`]), sending `SELECT` only when
/// it's another database, and trying once more on a new one when a kept one
/// turns out closed. It waits for the other server on this connection's
/// thread, for up to `timeout` milliseconds each step, as Redis does on its
/// only one.
pub(super) fn migrate(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = ctx.db(session);
    let (mut copy, mut replace) = (false, false);
    let mut auth: Option<(Option<&[u8]>, &[u8])> = None;
    let (mut first_key, mut num_keys) = (3, 1);
    let mut j = 6;
    while j < args.len() {
        let more = args.len() - 1 - j;
        let option = args.get(j);
        let is = |name: &[u8]| option.eq_ignore_ascii_case(name);
        if is(b"COPY") {
            copy = true;
        } else if is(b"REPLACE") {
            replace = true;
        } else if is(b"AUTH") {
            if more == 0 {
                resp::write_error(out, SYNTAX_ERROR);
                return Reply::Written;
            }
            j += 1;
            auth = Some((None, args.get(j)));
        } else if is(b"AUTH2") {
            if more < 2 {
                resp::write_error(out, SYNTAX_ERROR);
                return Reply::Written;
            }
            auth = Some((Some(args.get(j + 1)), args.get(j + 2)));
            j += 2;
        } else if is(b"KEYS") {
            if !args.get(3).is_empty() {
                resp::write_error(
                    out,
                    b"ERR When using MIGRATE KEYS option, the key argument must be set to the \
                      empty string",
                );
                return Reply::Written;
            }
            first_key = j + 1;
            num_keys = args.len() - j - 1;
            break;
        } else {
            resp::write_error(out, SYNTAX_ERROR);
            return Reply::Written;
        }
        j += 1;
    }
    let (Some(timeout), Some(db)) = (resp::parse_int(args.get(5)), resp::parse_int(args.get(4)))
    else {
        resp::write_error(out, NOT_AN_INTEGER);
        return Reply::Written;
    };
    let timeout = Duration::from_millis(if timeout <= 0 { 1000 } else { timeout as u64 });
    // The keys that are here (looked up as reads are), with their payloads and
    // what's left of their TTLs.
    let mut keys = Vec::new();
    for i in first_key..first_key + num_keys {
        let found = store.get_key(args.key_at(store, i), now, |value| {
            value.map(|v| payload_of(&v))
        });
        session.count_lookup(found.is_some());
        if let Some(payload) = found {
            let expiry = update(store, args.key_at(store, i), now, |current| {
                (Update::Peek, current.map(|c| c.expiry))
            });
            let ttl = match expiry {
                Some(Expiry::At(deadline)) => (deadline.saturating_sub(now) as i64).max(1),
                _ => 0,
            };
            keys.push((i, ttl.to_string(), payload));
        }
    }
    if keys.is_empty() {
        out.extend_from_slice(b"+NOKEY\r\n");
        return Reply::Written;
    }
    let name = [args.get(1), b":", args.get(2)].concat();
    let sockets = &ctx.migrate_sockets;
    let mut may_retry = true;
    loop {
        let cached = match sockets.take(&name) {
            Some(cached) => cached,
            None => match connect(args.get(1), args.get(2), timeout) {
                Ok(stream) => {
                    let _ = stream.set_nodelay(true);
                    Cached {
                        stream,
                        db: -1,
                        used: Instant::now(),
                    }
                }
                Err(error) => {
                    resp::write_error(out, error);
                    return Reply::Written;
                }
            },
        };
        let _ = cached.stream.set_read_timeout(Some(timeout));
        let _ = cached.stream.set_write_timeout(Some(timeout));
        let select = cached.db != db;
        let mut request = Vec::new();
        if let Some((user, password)) = auth {
            match user {
                Some(user) => put_command(&mut request, &[b"AUTH", user, password]),
                None => put_command(&mut request, &[b"AUTH", password]),
            }
        }
        if select {
            put_command(&mut request, &[b"SELECT", db.to_string().as_bytes()]);
        }
        for (i, ttl, payload) in &keys {
            let key = args.get(*i);
            if replace {
                put_command(
                    &mut request,
                    &[b"RESTORE", key, ttl.as_bytes(), payload, b"REPLACE"],
                );
            } else {
                put_command(&mut request, &[b"RESTORE", key, ttl.as_bytes(), payload]);
            }
        }
        // Sent in 64 KB pieces, each within the timeout, as Redis sends it.
        let mut writer = &cached.stream;
        if let Some(e) = request
            .chunks(64 * 1024)
            .find_map(|chunk| writer.write_all(chunk).err())
        {
            // Closed. Another try, on a new one, unless it timed out.
            drop(cached);
            if !timed_out(&e) && may_retry {
                may_retry = false;
                continue;
            }
            resp::write_error(out, WRITE_ERROR);
            return Reply::Written;
        }
        let mut reader = BufReader::new(&cached.stream);
        let mut first_replies = || -> Result<FirstReplies, bool> {
            let auth_reply = match auth {
                Some(_) => Some(read_line(&mut reader)?),
                None => None,
            };
            let select_reply = if select {
                Some(read_line(&mut reader)?)
            } else {
                None
            };
            Ok((auth_reply, select_reply))
        };
        let (auth_reply, select_reply) = match first_replies() {
            Ok(replies) => replies,
            Err(timed_out) => {
                drop(reader);
                drop(cached);
                if !timed_out && may_retry {
                    may_retry = false;
                    continue;
                }
                resp::write_error(out, READ_ERROR);
                return Reply::Written;
            }
        };
        // Each RESTORE's reply: the first error is the reply, and the keys that
        // went are deleted here.
        let mut target_error: Option<Vec<u8>> = None;
        let mut socket_error = None;
        let mut deleted = vec![&b"DEL"[..]];
        let mut read = 0;
        for (i, _, _) in &keys {
            let reply = match read_line(&mut reader) {
                Ok(reply) => reply,
                Err(timed_out) => {
                    socket_error = Some(timed_out);
                    break;
                }
            };
            read += 1;
            let failed = [
                auth_reply.as_deref(),
                select_reply.as_deref(),
                Some(&reply[..]),
            ]
            .into_iter()
            .flatten()
            .find(|r| r.first() == Some(&b'-'));
            match failed {
                Some(error) => {
                    target_error.get_or_insert_with(|| error.to_vec());
                }
                None if !copy => {
                    delete(store, args, *i, now);
                    deleted.push(args.get(*i));
                }
                None => {}
            }
        }
        drop(reader);
        if let Some(timed_out) = socket_error
            && target_error.is_none()
            && read == 0
            && may_retry
            && !timed_out
        {
            // Nothing was taken: another try on a new connection.
            drop(cached);
            may_retry = false;
            continue;
        }
        // The log has what happened here: the keys deleted.
        if deleted.len() > 1 {
            crate::aof::record_parts(deleted.iter().copied());
        }
        match target_error {
            Some(error) => {
                if socket_error.is_none() {
                    // Kept, with what it selected no longer known.
                    sockets.put(name, Cached { db: -1, ..cached });
                }
                let mut message = b"ERR Target instance replied with error: ".to_vec();
                message.extend_from_slice(&error[1..]);
                resp::write_error(out, &message);
            }
            None if socket_error.is_some() => resp::write_error(out, READ_ERROR),
            None => {
                sockets.put(name, Cached { db, ..cached });
                out.extend_from_slice(resp::OK);
            }
        }
        return Reply::Written;
    }
}

fn put_command(out: &mut Vec<u8>, parts: &[&[u8]]) {
    out.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for part in parts {
        out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }
}
