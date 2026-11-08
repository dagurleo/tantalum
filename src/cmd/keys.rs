//! Commands over the keyspace as a whole: `SCAN`, `KEYS`, `RENAME`, `COPY`,
//! `RANDOMKEY`, `TOUCH` and `OBJECT`.

use super::set::random_below;
use super::{Args, INVALID_CURSOR, OOM, Reply, SYNTAX_ERROR, Scan, ScanOptions, parse_cursor};
use crate::collection::Collection;
use crate::context::{Context, Session};
use crate::glob;
use crate::resp;
use crate::store::{
    EvictionPolicy, Expiry, Key, Kind, Millis, OutOfMemory, Renamed, Store, Update, ValueRef, View,
};
use crate::{list, set, zset};

/// `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]`. Cursors are
/// Tantalum's own (see `Store::scan`), and a whole scan returns each key
/// that existed throughout exactly once.
pub(super) fn scan(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let Some(cursor) = parse_cursor(args.get(1)) else {
        resp::write_error(out, INVALID_CURSOR);
        return Reply::Written;
    };
    let Some(options) = ScanOptions::parse(args, 2, Scan::Keys, out) else {
        return Reply::Written;
    };
    let (mut keys, mut found) = (Vec::new(), 0);
    let next = ctx
        .db(session)
        .scan(cursor, options.count, now, |key, kind| {
            if options.has_type(kind) && options.matches(key) {
                resp::write_bulk(&mut keys, key);
                found += 1;
            }
        });
    out.extend_from_slice(b"*2\r\n");
    resp::write_bulk(out, itoa::Buffer::new().format(next).as_bytes());
    resp::write_array_header(out, found);
    out.extend_from_slice(&keys);
    Reply::Written
}

/// `KEYS pattern`: every key matching it at once, one shard at a time.
pub(super) fn keys(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = ctx.db(session);
    let pattern = args.get(1);
    let everything = pattern == b"*";
    let (mut keys, mut found) = (Vec::new(), 0);
    for shard in 0..store.shard_count() {
        store.for_each_key(shard, now, |key, _| {
            if everything || glob::matches(pattern, key, false) {
                resp::write_bulk(&mut keys, key);
                found += 1;
            }
        });
    }
    resp::write_array_header(out, found);
    out.extend_from_slice(&keys);
    Reply::Written
}

/// `RENAME key newkey`
pub(super) fn rename(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    rename_key(ctx, session, now, args, out, false)
}

/// `RENAMENX key newkey`: 1 if renamed, 0 if `newkey` exists.
pub(super) fn renamenx(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    rename_key(ctx, session, now, args, out, true)
}

fn rename_key(
    ctx: &Context,
    session: &Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    nx: bool,
) -> Reply {
    let store = ctx.db(session);
    match store.rename(args.key(store), args.key_at(store, 2), nx, now) {
        Ok(Renamed::Done) if nx => resp::write_integer(out, 1),
        Ok(Renamed::Done) => out.extend_from_slice(resp::OK),
        Ok(Renamed::Exists) => resp::write_integer(out, 0),
        Ok(Renamed::NoSuchKey) => resp::write_error(out, b"ERR no such key"),
        Err(OutOfMemory) => resp::write_error(out, OOM),
    }
    Reply::Written
}

/// `TOUCH key [key ...]`: how many exist, counting as using them.
pub(super) fn touch(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = ctx.db(session);
    let mut found = 0;
    for i in 1..args.len() {
        let exists = store
            .update_key(args.key_at(store, i), now, |current| match current {
                Some(_) => (Update::Touch, true),
                None => (Update::Peek, false),
            })
            .unwrap_or(false);
        found += i64::from(exists);
    }
    resp::write_integer(out, found);
    Reply::Written
}

/// `RANDOMKEY`: the first key from a random place in the hash order, so about
/// uniform, or nil if there are none.
pub(super) fn randomkey(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    _: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = ctx.db(session);
    let mut key: Option<Vec<u8>> = None;
    let shard = random_below(store.shard_count()) as u64;
    let from = random_below(u32::MAX as usize) as u64;
    let mut cursor = shard << 32 | from;
    // Round from there to the end of the keyspace, then from the start.
    for _ in 0..2 {
        loop {
            cursor = store.scan(cursor, 1, now, |found, _| {
                key.get_or_insert_with(|| found.to_vec());
            });
            if key.is_some() || cursor == 0 {
                break;
            }
        }
        if key.is_some() {
            break;
        }
    }
    match key {
        Some(key) => resp::write_bulk(out, &key),
        None => out.extend_from_slice(session.null()),
    }
    Reply::Written
}

/// A value taken out to copy, with its expiry.
enum Copied {
    Bytes(Kind, Vec<u8>),
    Object(Box<Collection>),
}

/// What's at `key`, to copy: its value and expiry.
fn read_copy(store: &Store, key: Key<'_>, now: Millis) -> Option<(Copied, Expiry)> {
    store
        .update_key(key, now, |current| match current {
            None => (Update::Peek, None),
            Some(c) => {
                let value = match c.value.view() {
                    View::Bytes(bytes) => Copied::Bytes(c.value.kind(), bytes.to_vec()),
                    View::Object(object) => Copied::Object(Box::new(object.clone())),
                };
                (Update::Touch, Some((value, c.expiry)))
            }
        })
        .unwrap_or(None)
}

/// Whether `key` holds anything.
fn exists_in(store: &Store, key: Key<'_>, now: Millis) -> bool {
    store
        .update_key(key, now, |current| (Update::Peek, current.is_some()))
        .unwrap_or(false)
}

/// Writes what [`read_copy`] read at `key`: false if there's no room for it.
fn write_copy(store: &Store, key: Key<'_>, (value, expiry): (Copied, Expiry), now: Millis) -> bool {
    let mut value = Some(value);
    let written = store.update_key(key, now, |_| {
        let update = match value.take().expect("written once") {
            Copied::Bytes(kind, bytes) => Update::Replace(kind, bytes),
            Copied::Object(object) => Update::Object(*object),
        };
        (update, ())
    });
    if written.is_err() {
        return false;
    }
    if matches!(expiry, Expiry::At(_)) {
        let _ = store.update_key(key, now, |_| (Update::Expire(expiry), ()));
    }
    true
}

/// `COPY source destination [DB destination-db] [REPLACE]`: 1 if copied, or 0
/// if there's no source or (without `REPLACE`) the destination exists. Value
/// and TTL go together, and nothing else runs meanwhile.
pub(super) fn copy(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let mut replace = false;
    let mut db = session.db;
    let mut i = 3;
    while i < args.len() {
        let option = args.get(i);
        if option.eq_ignore_ascii_case(b"REPLACE") {
            replace = true;
        } else if option.eq_ignore_ascii_case(b"DB") && i + 1 < args.len() {
            i += 1;
            match super::db_index(ctx, args.get(i), out) {
                Some(index) => db = index,
                None => return Reply::Written,
            }
        } else {
            resp::write_error(out, SYNTAX_ERROR);
            return Reply::Written;
        }
        i += 1;
    }
    if db == session.db && args.get(1) == args.get(2) {
        resp::write_error(out, b"ERR source and destination objects are the same");
        return Reply::Written;
    }
    let store = ctx.db(session);
    let target: &Store = ctx.store.db(db as usize);
    let (from, to) = (args.key(store), args.key_at(store, 2));
    let found = store.exclusive(|| {
        let Some(source) = read_copy(store, from, now) else {
            resp::write_integer(out, 0);
            return false;
        };
        if exists_in(target, to, now) {
            if !replace {
                resp::write_integer(out, 0);
                return true;
            }
            // So its TTL goes too.
            let _ = target.update_key(to, now, |_| (Update::Delete, ()));
        }
        if write_copy(target, to, source, now) {
            resp::write_integer(out, 1);
        } else {
            resp::write_error(out, OOM);
        }
        true
    });
    session.count_lookup(found);
    Reply::Written
}

/// `MOVE key db`: 1 if it moved there with its TTL, or 0 if there's no such key
/// here or there is one there. Nothing else runs meanwhile.
pub(super) fn move_key(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let Some(db) = super::db_index(ctx, args.get(2), out) else {
        return Reply::Written;
    };
    if db == session.db {
        resp::write_error(out, b"ERR source and destination objects are the same");
        return Reply::Written;
    }
    let store = ctx.db(session);
    let target: &Store = ctx.store.db(db as usize);
    let key = args.key(store);
    store.exclusive(|| {
        let source = read_copy(store, key, now);
        match source {
            Some(source) if !exists_in(target, key, now) => {
                if write_copy(target, key, source, now) {
                    let _ = store.update_key(key, now, |_| (Update::Delete, ()));
                    resp::write_integer(out, 1);
                } else {
                    resp::write_error(out, OOM);
                }
            }
            _ => resp::write_integer(out, 0),
        }
    });
    Reply::Written
}

/// `SWAPDB index1 index2`: each database now holds what the other did.
pub(super) fn swapdb(
    ctx: &Context,
    _: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let index = |arg: &[u8]| resp::parse_int(arg).filter(|&n| i32::try_from(n).is_ok());
    let Some(a) = index(args.get(1)) else {
        resp::write_error(out, b"ERR invalid first DB index");
        return Reply::Written;
    };
    let Some(b) = index(args.get(2)) else {
        resp::write_error(out, b"ERR invalid second DB index");
        return Reply::Written;
    };
    let databases = 0..ctx.store.databases() as i64;
    if !databases.contains(&a) || !databases.contains(&b) {
        resp::write_error(out, b"ERR DB index is out of range");
        return Reply::Written;
    }
    if a != b {
        ctx.store.db(a as usize).swap_with(ctx.store.db(b as usize));
    }
    out.extend_from_slice(resp::OK);
    Reply::Written
}

/// What a listpack takes for `element`, as `OBJECT ENCODING` needs to tell when
/// Redis would have left one.
fn listpack_entry_size(element: &[u8]) -> usize {
    let encoded = match resp::parse_int(element) {
        Some(0..=127) => 1,
        Some(-4096..=4095) => 2,
        Some(-32768..=32767) => 3,
        Some(-8_388_608..=8_388_607) => 4,
        Some(-2_147_483_648..=2_147_483_647) => 5,
        Some(_) => 9,
        None if element.len() < 64 => 1 + element.len(),
        None if element.len() < 4096 => 2 + element.len(),
        None => 5 + element.len(),
    };
    encoded
        + match encoded {
            0..=127 => 1,
            128..=16382 => 2,
            16383..=2_097_150 => 3,
            2_097_151..=268_435_454 => 4,
            _ => 5,
        }
}

/// The encoding Redis would use for what the value holds now, built up from
/// nothing. Redis' also depends on the value's past: a string made by `APPEND`
/// is `raw`, and a collection that has shrunk back may stay big.
pub(super) fn encoding(value: &ValueRef<'_>) -> &'static str {
    const PACKED_ENTRIES: usize = 128;
    const PACKED_VALUE: usize = 64;
    const INTSET_ENTRIES: usize = 512;
    const LIST_BYTES: usize = 8192;
    match value.kind() {
        Kind::String => {
            let bytes = value.as_bytes();
            if bytes.len() <= 20 && resp::parse_int(bytes).is_some() {
                "int"
            } else if bytes.len() <= 44 {
                "embstr"
            } else {
                "raw"
            }
        }
        Kind::Hash => {
            if super::hash::view(value.view()).fits_listpack() {
                "listpack"
            } else {
                "hashtable"
            }
        }
        Kind::List => {
            let mut total = 7;
            let mut fits = |element: &[u8]| {
                total += listpack_entry_size(element);
                total <= LIST_BYTES
            };
            let packed = match value.view() {
                View::Bytes(bytes) => list::Packed::new(bytes).iter().all(&mut fits),
                View::Object(Collection::List(big)) => big.iter().all(&mut fits),
                View::Object(_) => false,
            };
            if packed { "listpack" } else { "quicklist" }
        }
        Kind::Set => {
            let (mut len, mut ints, mut short) = (0, true, true);
            let mut member = |bytes: &[u8]| {
                len += 1;
                ints &= resp::parse_int(bytes).is_some();
                short &= bytes.len() <= PACKED_VALUE;
            };
            match value.view() {
                View::Bytes(bytes) => {
                    for m in set::Compact::new(bytes).iter() {
                        m.with_bytes(&mut member);
                    }
                }
                View::Object(Collection::Set(big)) => big.iter().for_each(&mut member),
                View::Object(_) => {}
            }
            if ints && len <= INTSET_ENTRIES {
                "intset"
            } else if short && len <= PACKED_ENTRIES {
                "listpack"
            } else {
                "hashtable"
            }
        }
        Kind::ZSet => {
            let packed = match value.view() {
                View::Bytes(bytes) => {
                    let set = zset::Packed::new(bytes);
                    set.len() <= PACKED_ENTRIES
                        && set.iter().all(|(_, member)| member.len() <= PACKED_VALUE)
                }
                View::Object(Collection::ZSet(big)) => {
                    let mut short = big.len() <= PACKED_ENTRIES;
                    if short {
                        big.for_range(0, big.len(), false, |_, member| {
                            short &= member.len() <= PACKED_VALUE;
                        });
                    }
                    short
                }
                View::Object(_) => false,
            };
            if packed { "listpack" } else { "skiplist" }
        }
        Kind::Stream => "stream",
    }
}

const OBJECT_HELP: &[&str] = &[
    "OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    "ENCODING <key>",
    "    Return the kind of internal representation used in order to store the value",
    "    associated with a <key>.",
    "FREQ <key>",
    "    Return the access frequency index of the <key>. The returned integer is",
    "    proportional to the logarithm of the recent access frequency of the key.",
    "IDLETIME <key>",
    "    Return the idle time of the <key>, that is the approximated number of",
    "    seconds elapsed since the last access to the key.",
    "REFCOUNT <key>",
    "    Return the number of references of the value associated with the specified",
    "    <key>.",
    "HELP",
    "    Print this help.",
];

/// `OBJECT ENCODING|FREQ|IDLETIME|REFCOUNT key` and `OBJECT HELP`. Tantalum
/// keeps no last-access time per key (it would add bytes to every key), so
/// `IDLETIME` is always 0, and no LFU policy, so `FREQ` is the error Redis gives
/// without one.
pub(super) fn object(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let sub = args.get(1);
    let is = |name: &[u8]| sub.eq_ignore_ascii_case(name);
    if is(b"HELP") && args.len() == 2 {
        resp::write_array_header(out, OBJECT_HELP.len());
        for line in OBJECT_HELP {
            out.push(b'+');
            out.extend_from_slice(line.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        return Reply::Written;
    }
    let known = is(b"ENCODING") || is(b"FREQ") || is(b"IDLETIME") || is(b"REFCOUNT");
    if !known && !is(b"HELP") {
        super::unknown_subcommand(out, sub, "OBJECT");
        return Reply::Written;
    }
    if is(b"HELP") || args.len() != 3 {
        let name = String::from_utf8_lossy(sub).to_ascii_lowercase();
        super::wrong_arity(out, &format!("object|{name}"));
        return Reply::Written;
    }
    let store = ctx.db(session);
    let null = session.null();
    let found = store
        .update_key(args.key_at(store, 2), now, |current| match current {
            None => (Update::Peek, None),
            Some(c) => (
                Update::Peek,
                Some((
                    encoding(&c.value),
                    c.value.kind() == Kind::String
                        && resp::parse_int(c.value.as_bytes())
                            .is_some_and(|n| (0..10_000).contains(&n)),
                )),
            ),
        })
        .unwrap_or(None);
    let Some((encoding, small_int)) = found else {
        out.extend_from_slice(null);
        return Reply::Written;
    };
    if is(b"ENCODING") {
        resp::write_bulk(out, encoding.as_bytes());
    } else if is(b"FREQ") {
        resp::write_error(
            out,
            b"ERR An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.",
        );
    } else if is(b"IDLETIME") {
        resp::write_integer(out, 0);
    } else {
        // Redis shares small integers, but not under an LRU policy.
        let shared =
            small_int && (store.maxmemory() == 0 || store.policy() == EvictionPolicy::NoEviction);
        resp::write_integer(out, if shared { i64::from(i32::MAX) } else { 1 });
    }
    Reply::Written
}
