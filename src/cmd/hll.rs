//! HyperLogLog commands (`src/hll.rs`), on strings, following Redis'
//! `hyperloglog.c`. A dense one is changed where it lies, as Redis does: no copy
//! of its 12 KB for each `PFADD`. A sparse one, a few KB at most, is written anew.

use super::{Args, OOM, Reply, WRONGTYPE};
use crate::context::{Context, Session};
use crate::hll::{self, SparseSet};
use crate::resp;
use crate::store::{Key, Kind, Millis, OutOfMemory, Store, Update};

const NOT_AN_HLL: &[u8] = b"WRONGTYPE Key is not a valid HyperLogLog string value.";
const CORRUPT: &[u8] = b"INVALIDOBJ Corrupted HLL object detected";

fn reply_error(out: &mut Vec<u8>, message: &[u8]) -> Reply {
    resp::write_error(out, message);
    Reply::Written
}

/// `PFADD key [element ...]`: 1 if it was created or a register changed.
pub(super) fn pfadd(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = ctx.db(session);
    let key = args.key(store);
    let elements = || (2..args.len()).map(|i| args.get(i));
    let max_bytes = ctx.hll_sparse_max_bytes();
    // Where it lies, if it can be: a dense one always, a sparse one kept apart if it
    // has room. What this got done counts, if the rest has to be done below.
    let room = 3 * elements().len();
    let mut changed_here = false;
    let in_place = store.edit_string_room(key, now, room, |bytes, len| {
        if !hll::is_valid(&bytes[..len]) {
            return (Some(Err(NOT_AN_HLL)), len, false);
        }
        if hll::is_dense(bytes) {
            let mut changed = false;
            for element in elements() {
                changed |= hll::dense_add(&mut bytes[..len], element);
            }
            if changed {
                hll::invalidate(bytes);
            }
            return (Some(Ok(changed)), len, changed);
        }
        if bytes.len() < len + room {
            return (None, len, false);
        }
        let mut end = len;
        for element in elements() {
            match hll::sparse_add_in(bytes, end, element, max_bytes) {
                Ok(SparseSet::Unchanged) => {}
                Ok(SparseSet::Changed(new_end)) => {
                    end = new_end;
                    changed_here = true;
                }
                Ok(SparseSet::Promote) => return (None, end, changed_here),
                Err(hll::Corrupt) => return (Some(Err(CORRUPT)), end, changed_here),
            }
        }
        (Some(Ok(changed_here)), end, changed_here)
    });
    let result = match in_place.flatten() {
        Some(result) => Ok(result),
        None => store.update_key(key, now, |current| {
            let exists = current.is_some();
            let (mut value, mut changed) = match &current {
                None => (hll::new_sparse(), true),
                Some(c) if c.value.kind() != Kind::String => {
                    return (Update::Peek, Err(WRONGTYPE));
                }
                Some(c) if !hll::is_valid(c.value.as_bytes()) => {
                    return (Update::Peek, Err(NOT_AN_HLL));
                }
                Some(c) => (c.value.as_bytes().to_vec(), changed_here),
            };
            // A sparse one that goes on changing is worth a place of its own.
            let update = |value: Vec<u8>| {
                if exists && !hll::is_dense(&value) {
                    Update::ReplaceWithRoom(value)
                } else {
                    Update::Replace(Kind::String, value)
                }
            };
            for element in elements() {
                match hll::add(&mut value, element, max_bytes) {
                    Ok(register_changed) => changed |= register_changed,
                    // What it changed before it found out stays, as in Redis.
                    Err(hll::Corrupt) => {
                        return (
                            if changed {
                                update(value)
                            } else {
                                Update::Touch
                            },
                            Err(CORRUPT),
                        );
                    }
                }
            }
            if changed {
                hll::invalidate(&mut value);
                (update(value), Ok(true))
            } else {
                (Update::Touch, Ok(false))
            }
        }),
    };
    match result {
        Ok(Ok(changed)) => resp::write_integer(out, i64::from(changed)),
        Ok(Err(message)) => resp::write_error(out, message),
        Err(OutOfMemory) => resp::write_error(out, OOM),
    }
    Reply::Written
}

/// Raises `max`'s registers to those of the key, if it's there: as `PFCOUNT` and
/// `PFMERGE` read each. Whether it was dense.
fn merge_key(
    store: &Store,
    key: Key<'_>,
    now: Millis,
    session: &mut Session,
    max: &mut [u8],
) -> Result<bool, &'static [u8]> {
    let (result, hit) = store.get_key(key, now, |value| match value {
        None => (Ok(false), false),
        Some(v) if v.kind() != Kind::String => (Err(WRONGTYPE), true),
        Some(v) if !hll::is_valid(v.as_bytes()) => (Err(NOT_AN_HLL), true),
        Some(v) => (
            hll::merge(max, v.as_bytes())
                .map(|()| hll::is_dense(v.as_bytes()))
                .map_err(|hll::Corrupt| CORRUPT),
            true,
        ),
    });
    session.count_lookup(hit);
    result
}

/// `PFCOUNT key [key ...]`: the cardinality of one, which it caches in the
/// string, or of the union of several. The cache isn't logged, nor counted as a
/// write: the log rebuilds it when it's next asked for.
pub(super) fn pfcount(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = ctx.db(session);
    if args.len() > 2 {
        let union: Result<u64, &'static [u8]> = store.exclusive(|| {
            let mut max = vec![0; hll::REGISTERS];
            for i in 1..args.len() {
                merge_key(store, args.key_at(store, i), now, session, &mut max)?;
            }
            Ok(hll::count_registers(&max))
        });
        match union {
            Ok(cardinality) => resp::write_integer(out, cardinality as i64),
            Err(message) => resp::write_error(out, message),
        }
        return Reply::Written;
    }
    let key = args.key(store);
    // In place whatever its size, since this never changes its length.
    let in_place = store.edit_string(key, now, Some, |bytes, _| {
        if !hll::is_valid(bytes) {
            return (Err(NOT_AN_HLL), false);
        }
        if let Some(cardinality) = hll::cached(bytes) {
            return (Ok(cardinality), false);
        }
        match hll::count(bytes) {
            Ok(cardinality) => {
                hll::set_cached(bytes, cardinality);
                (Ok(cardinality), true)
            }
            Err(hll::Corrupt) => (Err(CORRUPT), false),
        }
    });
    let (result, hit) = match in_place {
        Some(result) => (Ok(result), true),
        // Not a string, not there, or a reader is sending it now.
        None => {
            let mut hit = false;
            let result = store.update_key(key, now, |current| {
                let Some(current) = current else {
                    hit = false;
                    return (Update::Peek, Ok(0));
                };
                hit = true;
                if current.value.kind() != Kind::String {
                    return (Update::Peek, Err(WRONGTYPE));
                }
                let bytes = current.value.as_bytes();
                if !hll::is_valid(bytes) {
                    return (Update::Peek, Err(NOT_AN_HLL));
                }
                if let Some(cardinality) = hll::cached(bytes) {
                    return (Update::Touch, Ok(cardinality));
                }
                match hll::count(bytes) {
                    Ok(cardinality) => {
                        let mut value = bytes.to_vec();
                        hll::set_cached(&mut value, cardinality);
                        (Update::Replace(Kind::String, value), Ok(cardinality))
                    }
                    Err(hll::Corrupt) => (Update::Touch, Err(CORRUPT)),
                }
            });
            (result, hit)
        }
    };
    session.count_lookup(hit);
    match result {
        Ok(Ok(cardinality)) => resp::write_integer(out, cardinality as i64),
        Ok(Err(message)) => resp::write_error(out, message),
        Err(OutOfMemory) => resp::write_error(out, OOM),
    }
    Reply::Written
}

/// `PFMERGE destkey [sourcekey ...]`: the union of them all, destination too,
/// in the destination. Dense if any was.
pub(super) fn pfmerge(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = ctx.db(session);
    let max_bytes = ctx.hll_sparse_max_bytes();
    let result: Result<(), &'static [u8]> = store.exclusive(|| {
        let mut max = vec![0; hll::REGISTERS];
        let mut use_dense = false;
        for i in 1..args.len() {
            use_dense |= merge_key(store, args.key_at(store, i), now, session, &mut max)?;
        }
        let stored = store.update_key(args.key(store), now, |current| {
            let mut value = match &current {
                None => hll::new_sparse(),
                Some(c) => c.value.as_bytes().to_vec(),
            };
            if use_dense && hll::to_dense(&mut value).is_err() {
                return (Update::Touch, Err(CORRUPT));
            }
            for (index, &register) in max.iter().enumerate() {
                if register != 0 {
                    hll::raise(&mut value, index, register, max_bytes);
                }
            }
            hll::invalidate(&mut value);
            (Update::Replace(Kind::String, value), Ok(()))
        });
        match stored {
            Ok(result) => result,
            Err(OutOfMemory) => Err(OOM),
        }
    });
    match result {
        Ok(()) => out.extend_from_slice(resp::OK),
        Err(message) => resp::write_error(out, message),
    }
    Reply::Written
}

/// `PFSELFTEST`
pub(super) fn pfselftest(
    ctx: &Context,
    _: &mut Session,
    _: Millis,
    _: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let mut state = (std::process::id() | 1).wrapping_mul(2_654_435_761);
    let random = move || {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        state
    };
    match hll::self_test(random, ctx.hll_sparse_max_bytes()) {
        Ok(()) => out.extend_from_slice(resp::OK),
        Err(message) => resp::write_error(out, message.as_bytes()),
    }
    Reply::Written
}

/// What `PFDEBUG` replies, made under the key's lock.
enum Debug {
    Registers(Vec<u8>),
    Decoded(Vec<u8>),
    Encoding(&'static [u8]),
    Converted(bool),
    Error(Vec<u8>),
}

/// `PFDEBUG GETREG | DECODE | ENCODING | TODENSE key`
pub(super) fn pfdebug(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = ctx.db(session);
    let sub = args.get(1);
    let is = |name: &[u8]| sub.eq_ignore_ascii_case(name);
    let result = store.update_key(args.key_at(store, 2), now, |current| {
        let error = |message: &[u8]| (Update::Peek, Debug::Error(message.to_vec()));
        let Some(current) = current else {
            return error(b"ERR The specified key does not exist");
        };
        if current.value.kind() != Kind::String {
            return error(WRONGTYPE);
        }
        let bytes = current.value.as_bytes();
        if !hll::is_valid(bytes) {
            return error(NOT_AN_HLL);
        }
        let dense = hll::is_dense(bytes);
        if is(b"GETREG") || is(b"TODENSE") {
            let mut value = bytes.to_vec();
            if hll::to_dense(&mut value).is_err() {
                return error(CORRUPT);
            }
            let reply = if is(b"GETREG") {
                Debug::Registers(hll::registers(&value).collect())
            } else {
                Debug::Converted(!dense)
            };
            let update = if dense {
                Update::Touch
            } else {
                Update::Replace(Kind::String, value)
            };
            (update, reply)
        } else if is(b"DECODE") {
            if dense {
                return error(b"ERR HLL encoding is not sparse");
            }
            (Update::Touch, Debug::Decoded(hll::decode(bytes)))
        } else if is(b"ENCODING") {
            let name: &[u8] = if dense { b"dense" } else { b"sparse" };
            (Update::Touch, Debug::Encoding(name))
        } else {
            let mut message = b"ERR Unknown PFDEBUG subcommand '".to_vec();
            message.extend_from_slice(sub);
            message.push(b'\'');
            (Update::Touch, Debug::Error(message))
        }
    });
    match result {
        Ok(Debug::Registers(registers)) => {
            resp::write_array_header(out, registers.len());
            for register in registers {
                resp::write_integer(out, i64::from(register));
            }
        }
        Ok(Debug::Decoded(decoded)) => resp::write_bulk(out, &decoded),
        Ok(Debug::Encoding(name)) => resp::write_simple(out, name),
        Ok(Debug::Converted(converted)) => resp::write_integer(out, i64::from(converted)),
        Ok(Debug::Error(message)) => return reply_error(out, &message),
        Err(OutOfMemory) => return reply_error(out, OOM),
    }
    Reply::Written
}
