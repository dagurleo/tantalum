//! What replicas hear of each write, as Redis 7.2 propagates it: a command
//! that changed something, rewritten so that running it again gives the same
//! data at any time (`SET ... EX` as `SET ... PXAT`, `EXPIRE` as `PEXPIREAT`,
//! `SPOP` as the `SREM` of what it popped, `INCRBYFLOAT` as the `SET` of what
//! it came to, a blocking pop as the pop it made, `XADD *` with the ID it
//! made, `XREADGROUP` as the `XCLAIM`s of what it delivered...), keys that
//! expired or were evicted as `DEL`s, and a script's or transaction's
//! commands as the writes they made. What one command (or `EXEC`, or a script)
//! makes is one unit: between `MULTI` and `EXEC` if it's more than one
//! command, each in its database (`SELECT` when it changes), as Redis sends
//! them (`tools/repl-diff.py` holds the two side by side).
//!
//! It runs in the write's turn (`aof::Log::turn`), which begins a unit and
//! ends it, appending it to the backlog: writes take turns while replicas
//! may follow, so the stream has them in the order they ran. Whether a
//! command changed anything is counted from the store's notices of changed
//! keys (`Store::set_replicating`), which a command that changes nothing
//! doesn't make, as Redis propagates only a command that made its data
//! "dirty".

use std::cell::{Cell, RefCell};
use std::sync::Arc;

use super::Master;
use crate::cmd::Args;
use crate::resp;
use crate::store::{Change, Millis, Store};

/// A command to propagate, in its database.
struct Op {
    db: u32,
    args: Vec<Vec<u8>>,
}

#[derive(Default)]
struct Unit {
    /// The stream, while this thread takes a turn.
    master: Option<Arc<Master>>,
    /// Database 0, during the turn, for what a rewrite looks up.
    store: Option<*const Store>,
    now: Millis,
    /// The turn's time as Unix milliseconds: Redis' `commandTimeSnapshot`.
    unix_ms: i64,
    /// The database commands run in by now.
    db: u32,
    ops: Vec<Op>,
    /// Keys changed so far, and as of the last command recorded: a command
    /// changed something if they differ when it's recorded.
    changes: u64,
    mark: u64,
    /// Each op is a unit of its own: active expiry's `DEL`s.
    alone: bool,
    /// What a replica's master sent, which goes to its own replicas as it
    /// came, in place of what the command would propagate.
    raw: Option<Vec<u8>>,
    /// The command running changed nothing Redis would propagate, whatever
    /// its watchers were told (`LTRIM` of the whole list).
    clean: bool,
}

thread_local! {
    static UNIT: RefCell<Unit> = RefCell::new(Unit::default());
    /// Expired keys reads found and left while replicating, to remove after
    /// the batch, in a turn: `(database, key)`.
    static EXPIRED_READS: RefCell<Vec<(u32, Box<[u8]>)>> = const { RefCell::new(Vec::new()) };
    static ANY_EXPIRED_READS: Cell<bool> = const { Cell::new(false) };
}

/// A write's turn begins, at store-clock time `now`, in database `db`.
pub(crate) fn begin(master: &Arc<Master>, store: &Store, now: Millis, db: u32) {
    UNIT.with_borrow_mut(|unit| {
        unit.master = Some(master.clone());
        unit.store = Some(store as *const Store);
        unit.now = now;
        unit.unix_ms = store.unix_ms(now) as i64;
        unit.db = db;
        unit.mark = unit.changes;
        unit.alone = false;
        unit.raw = None;
        unit.clean = false;
        debug_assert!(unit.ops.is_empty());
    });
}

/// Whether this thread is in a turn that propagates.
#[inline]
pub(crate) fn active() -> bool {
    UNIT.with_borrow(|unit| unit.master.is_some())
}

/// What the turn propagates is `bytes`, as they came from this replica's
/// master, not what its commands make.
pub(crate) fn raw(bytes: &[u8]) {
    UNIT.with_borrow_mut(|unit| unit.raw = Some(bytes.to_vec()));
}

/// Each `DEL` from here to the end of the turn goes alone (active expiry).
pub(crate) fn alone() {
    UNIT.with_borrow_mut(|unit| unit.alone = true);
}

/// Appends the unit made so far to the stream.
pub(crate) fn end_unit() {
    UNIT.with_borrow_mut(flush);
}

/// The turn ends.
pub(crate) fn end() {
    UNIT.with_borrow_mut(|unit| {
        flush(unit);
        unit.master = None;
        unit.store = None;
    });
}

fn flush(unit: &mut Unit) {
    let Some(master) = &unit.master else {
        return;
    };
    if let Some(raw) = unit.raw.take() {
        unit.ops.clear();
        master.backlog.append_raw(&raw);
        return;
    }
    if unit.ops.is_empty() {
        return;
    }
    let ops = std::mem::take(&mut unit.ops);
    master.backlog.append(|db, out| {
        let multi = ops.len() > 1;
        if multi {
            push(out, [&b"MULTI"[..]]);
        }
        for op in &ops {
            if *db != Some(op.db) {
                push(
                    out,
                    [&b"SELECT"[..], itoa::Buffer::new().format(op.db).as_bytes()],
                );
                *db = Some(op.db);
            }
            push(out, op.args.iter().map(Vec::as_slice));
        }
        if multi {
            push(out, [&b"EXEC"[..]]);
        }
    });
    unit.ops = ops;
    unit.ops.clear();
}

fn push<'a>(out: &mut Vec<u8>, args: impl IntoIterator<Item = &'a [u8]>) {
    let args: Vec<&[u8]> = args.into_iter().collect();
    resp::write_array_header(out, args.len());
    for arg in args {
        resp::write_bulk(out, arg);
    }
}

/// Commands from here on run in database `db`.
pub(crate) fn select(db: u32) {
    UNIT.with_borrow_mut(|unit| unit.db = db);
}

fn add(unit: &mut Unit, db: u32, args: Vec<Vec<u8>>) {
    unit.ops.push(Op { db, args });
    if unit.alone {
        flush(unit);
    }
}

/// A command a write made, to propagate as it is (`MSET`'s pairs,
/// `FUNCTION`'s changes, the keys `MIGRATE` deleted).
pub(crate) fn op<'a>(parts: impl Iterator<Item = &'a [u8]>) {
    UNIT.with_borrow_mut(|unit| {
        if unit.master.is_some() && unit.raw.is_none() {
            let db = unit.db;
            add(unit, db, parts.map(<[u8]>::to_vec).collect());
        }
    });
}

/// [`op`], for one made of owned parts.
pub(crate) fn op_owned(args: Vec<Vec<u8>>) {
    UNIT.with_borrow_mut(|unit| {
        if unit.master.is_some() && unit.raw.is_none() {
            let db = unit.db;
            add(unit, db, args);
        }
    });
}

/// From the store, under a shard's lock: `key`, in database `db`, changed.
/// An expiry or eviction is a `DEL` (when a write's turn made it, which is
/// always so while replicating: reads leave expired keys); anything else
/// counts toward whether the command propagates.
pub(crate) fn changed(db: u16, key: &[u8], how: Change) {
    UNIT.with_borrow_mut(|unit| {
        if unit.master.is_none() {
            return;
        }
        match how {
            Change::Expired | Change::Evicted => {
                if unit.raw.is_none() {
                    add(unit, u32::from(db), vec![b"DEL".to_vec(), key.to_vec()]);
                }
            }
            Change::Written | Change::Created | Change::Removed => unit.changes += 1,
        }
    });
}

/// From the store: a change watchers don't see (a stream's groups), which
/// replicas must.
#[cold]
pub(crate) fn changed_unseen() {
    UNIT.with_borrow_mut(|unit| {
        if unit.master.is_some() {
            unit.changes += 1;
        }
    });
}

/// The command running changed nothing that propagates, whatever the store
/// says: as Redis' `LTRIM` of all there is.
pub(crate) fn not_dirty() {
    UNIT.with_borrow_mut(|unit| unit.clean = true);
}

/// Commands that take a write's turn only while replicas follow: they
/// propagate though they write nothing (or nothing the log needs).
pub(crate) fn takes_turn(name: &str) -> bool {
    matches!(name, "publish" | "spublish" | "pfcount")
}

/// For a command run as a group (a script, `EXEC`, `MSET`) or one that only
/// replicas hear of, after it ran: what of it propagates beyond what its
/// commands did.
pub(crate) fn grouped(name: &str, args: &Args<'_>, reply: &[u8]) {
    match name {
        "mset" | "msetnx" => UNIT.with_borrow_mut(|unit| {
            // Its pairs were propagated as `SET`s. If that's all the unit
            // has, they go as the command itself, as from Redis.
            let pairs = (args.len() - 1) / 2;
            if unit.master.is_some()
                && unit.raw.is_none()
                && unit.ops.len() == pairs
                && unit
                    .ops
                    .iter()
                    .all(|op| op.args.first().is_some_and(|c| c == b"SET"))
            {
                let db = unit.ops[0].db;
                unit.ops.clear();
                unit.ops.push(Op {
                    db,
                    args: (0..args.len()).map(|i| args.get(i).to_vec()).collect(),
                });
            }
            unit.mark = unit.changes;
        }),
        _ if takes_turn(name) => record(name, args, reply),
        _ => {}
    }
}

/// Whether the last command changed anything; the next counts from here.
fn dirty(unit: &mut Unit) -> bool {
    let dirty = unit.changes != unit.mark && !std::mem::take(&mut unit.clean);
    unit.mark = unit.changes;
    dirty
}

/// Propagates write command `name` (its table name), which ran with `args`
/// and replied `reply`, if it changed anything: as Redis would, rewritten
/// where running it again could do otherwise.
pub(crate) fn record(name: &str, args: &Args<'_>, reply: &[u8]) {
    UNIT.with_borrow_mut(|unit| {
        if unit.master.is_none() || unit.raw.is_some() {
            return;
        }
        let dirty = dirty(unit);
        let always = matches!(
            name,
            "publish"
                | "spublish"
                | "flushall"
                | "flushdb"
                | "swapdb"
                | "incr"
                | "decr"
                | "incrby"
                | "decrby"
                | "hincrby"
        );
        if !(dirty || always) || reply.first() == Some(&b'-') {
            return;
        }
        let db = unit.db;
        if let Some(op) = rewrite(unit, name, args, reply)
            && !op.is_empty()
        {
            add(unit, db, op);
        }
    });
}

/// What `args` propagate as: themselves, or a rewrite of them (`None`: as
/// they are; an empty one: nothing).
fn rewrite(unit: &mut Unit, name: &str, args: &Args<'_>, reply: &[u8]) -> Option<Vec<Vec<u8>>> {
    let arg = |i: usize| args.get(i).to_vec();
    let all = || {
        (0..args.len())
            .map(|i| args.get(i).to_vec())
            .collect::<Vec<_>>()
    };
    let is = |i: usize, word: &[u8]| i < args.len() && args.get(i).eq_ignore_ascii_case(word);
    let num = |n: i64| itoa::Buffer::new().format(n).as_bytes().to_vec();
    let now = unit.unix_ms;
    let parse = |i: usize| resp::parse_int(args.get(i)).unwrap_or(0);
    Some(match name {
        "set" => {
            // An expiry other than PXAT goes as PXAT, the rest as it came; GET
            // goes either way (nothing's read again).
            let mut at = None;
            let mut j = 3;
            while j < args.len() {
                if is(j, b"EX") || is(j, b"PX") || is(j, b"EXAT") {
                    let n = parse(j + 1);
                    let word = args.get(j).to_ascii_uppercase();
                    at = Some(match word.as_slice() {
                        b"EX" => now.saturating_add(n.saturating_mul(1000)),
                        b"PX" => now.saturating_add(n),
                        _ => n.saturating_mul(1000),
                    });
                    j += 1;
                } else if is(j, b"PXAT") {
                    j += 1;
                }
                j += 1;
            }
            let pxat = (3..args.len()).any(|i| is(i, b"PXAT"));
            match at {
                Some(at) => vec![b"SET".to_vec(), arg(1), arg(2), b"PXAT".to_vec(), num(at)],
                // With PXAT, GET stays, as from Redis.
                None if pxat => all(),
                None => (0..args.len())
                    .filter(|&i| !(i >= 3 && is(i, b"GET")))
                    .map(arg)
                    .collect(),
            }
        }
        "setex" => vec![
            b"SET".to_vec(),
            arg(1),
            arg(3),
            b"PXAT".to_vec(),
            num(now.saturating_add(parse(2).saturating_mul(1000))),
        ],
        "psetex" => vec![
            b"SET".to_vec(),
            arg(1),
            arg(3),
            b"PXAT".to_vec(),
            num(now.saturating_add(parse(2))),
        ],
        "getset" => vec![b"SET".to_vec(), arg(1), arg(2)],
        "getdel" => vec![b"DEL".to_vec(), arg(1)],
        "getex" => {
            if is(2, b"PERSIST") {
                vec![b"PERSIST".to_vec(), arg(1)]
            } else if args.len() > 3 {
                let n = parse(3);
                let at = if is(2, b"EX") {
                    now.saturating_add(n.saturating_mul(1000))
                } else if is(2, b"PX") {
                    now.saturating_add(n)
                } else if is(2, b"EXAT") {
                    n.saturating_mul(1000)
                } else {
                    n
                };
                if at <= now {
                    vec![b"DEL".to_vec(), arg(1)]
                } else {
                    vec![b"PEXPIREAT".to_vec(), arg(1), num(at)]
                }
            } else {
                return Some(Vec::new());
            }
        }
        "expire" | "pexpire" | "expireat" | "pexpireat" => {
            let n = parse(2);
            let at = match name {
                "expire" => now.saturating_add(n.saturating_mul(1000)),
                "pexpire" => now.saturating_add(n),
                "expireat" => n.saturating_mul(1000),
                _ => n,
            };
            if at <= now {
                vec![b"DEL".to_vec(), arg(1)]
            } else {
                let mut op = vec![b"PEXPIREAT".to_vec(), arg(1), num(at)];
                op.extend((3..args.len()).map(arg));
                op
            }
        }
        "incrbyfloat" | "hincrbyfloat" => {
            let value = bulk(reply)?;
            if name == "incrbyfloat" {
                vec![b"SET".to_vec(), arg(1), value, b"KEEPTTL".to_vec()]
            } else {
                vec![b"HSET".to_vec(), arg(1), arg(2), value]
            }
        }
        "spop" => {
            let members = match bulks(reply) {
                Some(members) => members,
                None => vec![bulk(reply)?],
            };
            if args.len() > 2 && !exists(unit, args.get(1)) {
                vec![b"DEL".to_vec(), arg(1)]
            } else {
                let mut op = vec![b"SREM".to_vec(), arg(1)];
                op.extend(members);
                op
            }
        }
        "blpop" | "brpop" | "bzpopmin" | "bzpopmax" => {
            let key = bulks(reply)?.into_iter().next()?;
            let command = match name {
                "blpop" => "LPOP",
                "brpop" => "RPOP",
                "bzpopmin" => "ZPOPMIN",
                _ => "ZPOPMAX",
            };
            vec![command.as_bytes().to_vec(), key]
        }
        "lmpop" | "blmpop" | "zmpop" | "bzmpop" => {
            // [key, [popped ...]]: the pop of that many from that key.
            let (key, count) = popped(reply)?;
            // `[B]LMPOP [timeout] numkeys key ... side`.
            let numkeys_at = if name.starts_with('b') { 2 } else { 1 };
            let numkeys = parse(numkeys_at).max(0) as usize;
            let side = args.get(numkeys_at + 1 + numkeys).to_ascii_uppercase();
            let command: &[u8] = match side.as_slice() {
                b"LEFT" => b"LPOP",
                b"RIGHT" => b"RPOP",
                b"MIN" => b"ZPOPMIN",
                _ => b"ZPOPMAX",
            };
            vec![command.to_vec(), key, num(count as i64)]
        }
        "blmove" => vec![b"LMOVE".to_vec(), arg(1), arg(2), arg(3), arg(4)],
        "brpoplpush" => vec![b"RPOPLPUSH".to_vec(), arg(1), arg(2)],
        "geoadd" => geoadd(args)?,
        "xadd" => xadd(unit, args, reply)?,
        "xtrim" => {
            if resp::parse_int(reply.get(1..reply.len().saturating_sub(2))?) == Some(0) {
                return Some(Vec::new());
            }
            trimmed(unit, args, 2, all())
        }
        // What they did propagates from where they did it (`group.rs`).
        "xreadgroup" | "xclaim" | "xautoclaim" => return Some(Vec::new()),
        // The keys it deleted, from where it deleted them.
        "migrate" => return Some(Vec::new()),
        "restore" => {
            let ttl = parse(2);
            let absttl = (4..args.len()).any(|i| is(i, b"ABSTTL"));
            if ttl == 0 || absttl {
                all()
            } else {
                let mut op = all();
                op[2] = num(now.saturating_add(ttl));
                op.push(b"ABSTTL".to_vec());
                op
            }
        }
        _ => all(),
    })
}

/// A reply, as far as a rewrite reads one.
enum Value {
    Bulk(Vec<u8>),
    /// An array, a set, a push, or a map's keys and values in turn.
    Array(Vec<Value>),
    Other,
}

/// Reads one RESP2 or RESP3 reply off the front of `input`.
fn value(input: &mut &[u8]) -> Option<Value> {
    let end = input.windows(2).position(|w| w == b"\r\n")?;
    let (kind, rest) = (input[0], &input[1..end]);
    *input = &input[end + 2..];
    let len = || resp::parse_int(rest);
    Some(match kind {
        b'$' | b'=' | b'!' => {
            let n = len()?;
            if n < 0 {
                return Some(Value::Other);
            }
            let n = n as usize;
            let bytes = input.get(..n)?.to_vec();
            *input = input.get(n + 2..)?;
            Value::Bulk(bytes)
        }
        b'*' | b'~' | b'>' | b'%' | b'|' => {
            let n = len()?;
            if n < 0 {
                return Some(Value::Other);
            }
            let n = n as usize * if matches!(kind, b'%' | b'|') { 2 } else { 1 };
            let items = (0..n).map(|_| value(input)).collect::<Option<Vec<_>>>()?;
            Value::Array(items)
        }
        _ => Value::Other,
    })
}

/// A bulk reply's value.
fn bulk(reply: &[u8]) -> Option<Vec<u8>> {
    match value(&mut { reply })? {
        Value::Bulk(bytes) => Some(bytes),
        _ => None,
    }
}

/// An array reply's bulk values.
fn bulks(reply: &[u8]) -> Option<Vec<Vec<u8>>> {
    match value(&mut { reply })? {
        Value::Array(items) => Some(
            items
                .into_iter()
                .filter_map(|item| match item {
                    Value::Bulk(bytes) => Some(bytes),
                    _ => None,
                })
                .collect(),
        ),
        _ => None,
    }
}

/// `[key, [popped ...]]` (or a RESP3 map of the key to them): the key and how
/// many.
fn popped(reply: &[u8]) -> Option<(Vec<u8>, usize)> {
    let Value::Array(items) = value(&mut { reply })? else {
        return None;
    };
    let mut items = items.into_iter();
    let Value::Bulk(key) = items.next()? else {
        return None;
    };
    match items.next()? {
        Value::Array(popped) => Some((key, popped.len())),
        _ => None,
    }
}

fn store(unit: &Unit) -> Option<&Store> {
    // SAFETY: the pointer is the turn's caller's store, which outlives it.
    unit.store.map(|store| unsafe { &*store })
}

/// The database the unit runs in by now.
fn db_of<'a>(unit: &Unit, base: &'a Store) -> Option<&'a Store> {
    base.live().find(|db| db.index() == unit.db as usize)
}

fn exists(unit: &Unit, key: &[u8]) -> bool {
    let Some(base) = store(unit) else {
        return true;
    };
    db_of(unit, base).is_some_and(|db| db.get_with(key, unit.now, |value| value.is_some()))
}

/// `GEOADD`, as the `ZADD` Redis makes of it: in lowercase, with the scores.
fn geoadd(args: &Args<'_>) -> Option<Vec<Vec<u8>>> {
    let mut op = vec![b"zadd".to_vec(), args.get(1).to_vec()];
    let mut i = 2;
    while i < args.len() {
        let word = args.get(i);
        if ["NX", "XX", "CH"]
            .iter()
            .any(|w| word.eq_ignore_ascii_case(w.as_bytes()))
        {
            op.push(word.to_vec());
            i += 1;
        } else {
            break;
        }
    }
    while i + 2 < args.len() {
        let float = |arg: &[u8]| std::str::from_utf8(arg).ok()?.parse::<f64>().ok();
        let longitude = float(args.get(i))?;
        let latitude = float(args.get(i + 1))?;
        let score = crate::geo::encode(longitude, latitude, crate::geo::STEP_MAX).align52();
        op.push(itoa::Buffer::new().format(score).as_bytes().to_vec());
        op.push(args.get(i + 2).to_vec());
        i += 3;
    }
    Some(op)
}

/// `XADD`: with the ID it made, and with `~` trimming as what it trimmed to.
fn xadd(unit: &Unit, args: &Args<'_>, reply: &[u8]) -> Option<Vec<Vec<u8>>> {
    let id = bulk(reply)?;
    let mut op: Vec<Vec<u8>> = (0..args.len()).map(|i| args.get(i).to_vec()).collect();
    // Find the ID: past NOMKSTREAM, a trim and its LIMIT.
    let mut i = 2;
    let mut trim_at = None;
    while i < args.len() {
        let word = args.get(i);
        if word.eq_ignore_ascii_case(b"NOMKSTREAM") {
            i += 1;
        } else if word.eq_ignore_ascii_case(b"MAXLEN") || word.eq_ignore_ascii_case(b"MINID") {
            trim_at = Some(i);
            i += 1;
            if matches!(args.get(i), b"~" | b"=") {
                i += 1;
            }
            i += 1;
        } else if word.eq_ignore_ascii_case(b"LIMIT") {
            i += 2;
        } else {
            break;
        }
    }
    *op.get_mut(i)? = id;
    if let Some(at) = trim_at {
        op = trimmed(unit, args, at, op);
    }
    Some(op)
}

/// A trim at `args[at]` (`MAXLEN` or `MINID`) in `op`: with `~`, as `=`
/// what the stream has now, so that a replica trims as much.
fn trimmed(unit: &Unit, args: &Args<'_>, at: usize, mut op: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    if args.get(at + 1) != b"~" {
        return op;
    }
    let maxlen = args.get(at).eq_ignore_ascii_case(b"MAXLEN");
    let Some(base) = store(unit) else {
        return op;
    };
    let Some(db) = db_of(unit, base) else {
        return op;
    };
    let now_is = db.get_with(args.get(1), unit.now, |value| {
        let value = value?;
        let crate::store::View::Object(crate::collection::Collection::Stream(stream)) =
            value.view()
        else {
            return None;
        };
        let mut threshold = Vec::new();
        if maxlen {
            threshold.extend_from_slice(itoa::Buffer::new().format(stream.len()).as_bytes());
        } else {
            stream.first_id().write(&mut threshold);
        }
        Some(threshold)
    });
    if let Some(threshold) = now_is {
        op[at + 1] = b"=".to_vec();
        op[at + 2] = threshold;
    }
    op
}

/// `XCLAIM` as Redis propagates what a group read or claim did to one
/// pending entry: `key group consumer 0 id TIME t RETRYCOUNT n FORCE JUSTID
/// LASTID last`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn xclaim(
    key: &[u8],
    group: &[u8],
    consumer: &[u8],
    id: crate::stream::StreamId,
    time: i64,
    count: i64,
    last_id: crate::stream::StreamId,
) {
    if !active() {
        return;
    }
    let id_text = |id: crate::stream::StreamId| {
        let mut out = Vec::new();
        id.write(&mut out);
        out
    };
    let num = |n: i64| itoa::Buffer::new().format(n).as_bytes().to_vec();
    op_owned(vec![
        b"XCLAIM".to_vec(),
        key.to_vec(),
        group.to_vec(),
        consumer.to_vec(),
        b"0".to_vec(),
        id_text(id),
        b"TIME".to_vec(),
        num(time),
        b"RETRYCOUNT".to_vec(),
        num(count),
        b"FORCE".to_vec(),
        b"JUSTID".to_vec(),
        b"LASTID".to_vec(),
        id_text(last_id),
    ]);
}

/// `XGROUP SETID key group last ENTRIESREAD n`, for a group read with
/// `NOACK`.
pub(crate) fn xgroup_setid(key: &[u8], group: &[u8], last_id: crate::stream::StreamId, read: i64) {
    if !active() {
        return;
    }
    let mut last = Vec::new();
    last_id.write(&mut last);
    op_owned(vec![
        b"XGROUP".to_vec(),
        b"SETID".to_vec(),
        key.to_vec(),
        group.to_vec(),
        last,
        b"ENTRIESREAD".to_vec(),
        itoa::Buffer::new().format(read).as_bytes().to_vec(),
    ]);
}

/// `XGROUP CREATECONSUMER key group consumer`.
pub(crate) fn xgroup_createconsumer(key: &[u8], group: &[u8], consumer: &[u8]) {
    if active() {
        op_owned(vec![
            b"XGROUP".to_vec(),
            b"CREATECONSUMER".to_vec(),
            key.to_vec(),
            group.to_vec(),
            consumer.to_vec(),
        ]);
    }
}

/// From the store, under a shard's lock: a read found `key` expired in
/// database `db` and left it (see `Store::set_replicating`).
#[cold]
pub(crate) fn expired_read(db: u16, key: &[u8]) {
    EXPIRED_READS.with_borrow_mut(|keys| {
        if keys.len() < 1024 {
            keys.push((u32::from(db), key.into()));
        }
    });
    ANY_EXPIRED_READS.set(true);
}

/// After a connection's batch: removes the expired keys its reads found,
/// each propagated as a `DEL` (as from Redis when a read finds one), in a
/// write's turn.
#[inline]
pub(crate) fn after_batch(ctx: &crate::context::Context) {
    if ANY_EXPIRED_READS.get() {
        remove_expired_reads(ctx);
    }
}

#[cold]
fn remove_expired_reads(ctx: &crate::context::Context) {
    ANY_EXPIRED_READS.set(false);
    let keys = EXPIRED_READS.with_borrow_mut(std::mem::take);
    // A replica's keys go when its master says.
    if ctx.repl.following() {
        return;
    }
    let Some(log) = ctx.log() else {
        return;
    };
    if crate::aof::logging() {
        return;
    }
    let turn = log.turn(&ctx.store, false, 0);
    for (db, key) in keys {
        crate::aof::select(db as usize);
        let store = ctx.store.db(db as usize);
        let _ = store.update_key(store.key(&key), turn.now, |_| {
            (crate::store::Update::Peek, ())
        });
        end_unit();
    }
    drop(turn);
}
