//! Commands over the keyspace as a whole: `SCAN`, `KEYS`, and `RENAME`.

use super::{Args, INVALID_CURSOR, OOM, Reply, Scan, ScanOptions, parse_cursor};
use crate::context::{Context, Session};
use crate::glob;
use crate::resp;
use crate::store::{Millis, OutOfMemory, Renamed};

/// `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]`. Cursors are
/// Tantalum's own (see `Store::scan`), and a whole scan returns each key
/// that existed throughout exactly once.
pub(super) fn scan(
    ctx: &Context,
    _: &mut Session,
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
    let next = ctx.store.scan(cursor, options.count, now, |key, kind| {
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
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = &*ctx.store;
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
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    rename_key(ctx, now, args, out, false)
}

/// `RENAMENX key newkey`: 1 if renamed, 0 if `newkey` exists.
pub(super) fn renamenx(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    rename_key(ctx, now, args, out, true)
}

fn rename_key(ctx: &Context, now: Millis, args: &Args<'_>, out: &mut Vec<u8>, nx: bool) -> Reply {
    let store = &*ctx.store;
    match store.rename(args.key(store), args.key_at(store, 2), nx, now) {
        Ok(Renamed::Done) if nx => resp::write_integer(out, 1),
        Ok(Renamed::Done) => out.extend_from_slice(resp::OK),
        Ok(Renamed::Exists) => resp::write_integer(out, 0),
        Ok(Renamed::NoSuchKey) => resp::write_error(out, b"ERR no such key"),
        Err(OutOfMemory) => resp::write_error(out, OOM),
    }
    Reply::Written
}
