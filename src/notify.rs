//! Keyspace notifications: `notify-keyspace-events`, as Redis 7.2 sends them.
//!
//! Redis calls its notifier at each of the ninety-odd places where a command
//! changes a key. Here no command changes for it. The store already tells its
//! listeners of every key that changes (`Notices` in `store.rs`, for `WATCH`
//! and `CLIENT TRACKING`), through a flag that's off while nobody listens.
//! While notifications are on, that flag is set in every shard, and each
//! change goes into this thread's log: the key, its database, and how it
//! changed (written, made, removed, expired or evicted). Meanwhile every
//! connection takes the checked path (`Context::special`), whose commands are
//! run by `watched` and `watched_inside` in `cmd.rs`; after each command, the
//! command's name, arguments and reply, with what the log says it did, give
//! Redis' events in Redis' order ([`plan`]). With notifications off, commands
//! run the same code as before, and nothing is logged.
//!
//! A few events depend on what the log can't say, as whether `XADD` trimmed
//! or `XREADGROUP` made a consumer: those commands look before they run (only
//! while notifications are on). The events of a command go out once the
//! command that ran it ends (`EXEC` and scripts included), then those of the
//! blocked clients it served, as Redis sends them.
//!
//! Approximations, where Redis' events come from inside its commands:
//! `keymiss` is sent for the keys of read-only commands that weren't there
//! (Redis also sends it for the keys some writes read, as `SINTERSTORE`'s
//! sources); and of a command's keys that expire as it runs, those it sends
//! events for hear `expired` just before them, the rest first.

use std::cell::{Cell, RefCell};

use crate::cmd::Args;
use crate::collection::Collection;
use crate::context::{Context, Session};
use crate::store::{Change, Millis, Store, Update, View};

/// The classes, as Redis' `NOTIFY_*` bits (and its letters).
pub(crate) const KEYSPACE: u32 = 1 << 0; // K
pub(crate) const KEYEVENT: u32 = 1 << 1; // E
const GENERIC: u32 = 1 << 2; // g
const STRING: u32 = 1 << 3; // $
const LIST: u32 = 1 << 4; // l
const SET: u32 = 1 << 5; // s
const HASH: u32 = 1 << 6; // h
const ZSET: u32 = 1 << 7; // z
const EXPIRED: u32 = 1 << 8; // x
const EVICTED: u32 = 1 << 9; // e
const STREAM: u32 = 1 << 10; // t
const KEY_MISS: u32 = 1 << 11; // m (not in A)
const MODULE: u32 = 1 << 12; // d
const NEW: u32 = 1 << 13; // n (not in A)
const ALL: u32 = GENERIC | STRING | LIST | SET | HASH | ZSET | EXPIRED | EVICTED | STREAM | MODULE;

/// `CONFIG SET`'s error for a letter Redis doesn't know.
pub(crate) const INVALID: &str = "Invalid event class character. Use 'Ag$lshzxeKEtmdn'.";

/// The flags `classes` asks for, or `None` for a letter Redis doesn't know.
pub(crate) fn parse(classes: &[u8]) -> Option<u32> {
    let mut flags = 0;
    for c in classes {
        flags |= match c {
            b'A' => ALL,
            b'g' => GENERIC,
            b'$' => STRING,
            b'l' => LIST,
            b's' => SET,
            b'h' => HASH,
            b'z' => ZSET,
            b'x' => EXPIRED,
            b'e' => EVICTED,
            b'K' => KEYSPACE,
            b'E' => KEYEVENT,
            b't' => STREAM,
            b'm' => KEY_MISS,
            b'd' => MODULE,
            b'n' => NEW,
            _ => return None,
        };
    }
    Some(flags)
}

/// The flags as `CONFIG GET` says them, in Redis' order.
pub(crate) fn format(flags: u32) -> String {
    let mut s = String::new();
    if flags & ALL == ALL {
        s.push('A');
    } else {
        for (bit, c) in [
            (GENERIC, 'g'),
            (STRING, '$'),
            (LIST, 'l'),
            (SET, 's'),
            (HASH, 'h'),
            (ZSET, 'z'),
            (EXPIRED, 'x'),
            (EVICTED, 'e'),
            (STREAM, 't'),
            (MODULE, 'd'),
            (NEW, 'n'),
        ] {
            if flags & bit != 0 {
                s.push(c);
            }
        }
    }
    for (bit, c) in [(KEYSPACE, 'K'), (KEYEVENT, 'E'), (KEY_MISS, 'm')] {
        if flags & bit != 0 {
            s.push(c);
        }
    }
    s
}

/// Whether the flags send anything: a channel kind and a class.
pub(crate) fn sends(flags: u32) -> bool {
    flags & (KEYSPACE | KEYEVENT) != 0 && flags & !(KEYSPACE | KEYEVENT) != 0
}

/// A change to a key, as the store logged it.
struct Logged {
    db: u16,
    key: Box<[u8]>,
    how: Change,
}

/// An event to send.
struct Event {
    db: u16,
    class: u32,
    name: &'static str,
    key: Box<[u8]>,
}

thread_local! {
    /// What the store changed on this thread since it was last looked at.
    static LOG: RefCell<Vec<Logged>> = const { RefCell::new(Vec::new()) };
    /// Events of the commands running, to send when the outermost ends.
    static PENDING: RefCell<Vec<Event>> = const { RefCell::new(Vec::new()) };
    /// Events of blocked clients served meanwhile, sent after those.
    static LATER: RefCell<Vec<Event>> = const { RefCell::new(Vec::new()) };
    /// How many commands are running whose events wait for the outermost.
    static DEPTH: Cell<u32> = const { Cell::new(0) };
}

/// From the store, while notifications are on: `key` in database `db` changed.
#[cold]
pub(crate) fn log(db: u16, key: &[u8], how: Change) {
    LOG.with_borrow_mut(|log| {
        log.push(Logged {
            db,
            key: key.into(),
            how,
        })
    });
}

/// What a command's events depend on that it may change, looked at before it runs.
pub(crate) struct Before {
    mark: usize,
    /// `XADD` with a limit: the stream's length.
    xlen: Option<usize>,
    /// For each key whose group the command names: whether its consumer was there.
    consumers: Vec<bool>,
    /// The keys a command reads that weren't there, for `keymiss`.
    missing: Vec<usize>,
    /// The first of those keys that was there: a `WRONGTYPE` stops at it.
    present_first: Option<usize>,
}

/// Before a command runs while notifications are on (see [`after`]).
pub(crate) fn before(
    ctx: &Context,
    session: &Session,
    now: Millis,
    command: &'static str,
    readonly: bool,
    args: &Args<'_>,
) -> Before {
    if DEPTH.get() == 0 {
        // Changes no command looked at: made just as notifications came on.
        LOG.with_borrow_mut(Vec::clear);
    }
    DEPTH.set(DEPTH.get() + 1);
    let store = ctx.db(session);
    let flags = ctx.keyspace_events();
    let mut before = Before {
        mark: LOG.with_borrow(Vec::len),
        xlen: None,
        consumers: Vec::new(),
        missing: Vec::new(),
        present_first: None,
    };
    if flags & STREAM != 0 {
        let sub = |i: usize| args.len() > i && args.get(i).eq_ignore_ascii_case(b"delconsumer");
        match command {
            "xadd" if trims(args) => before.xlen = stream_len(store, args.get(1), now),
            "xreadgroup" | "xclaim" | "xautoclaim" | "xgroup" => {
                before.consumers = group_keys(command, args)
                    .map(|(key, group, consumer)| has_consumer(store, key, group, consumer, now))
                    .collect();
                if command == "xgroup" && !sub(1) {
                    before.consumers.clear();
                }
            }
            _ => {}
        }
    }
    if flags & KEY_MISS != 0 {
        let read = if readonly && command != "object" {
            let all: Vec<&[u8]> = (0..args.len()).map(|i| args.get(i)).collect();
            crate::command_table::keys_of(&all)
                .map(|keys| keys.into_iter().map(|(i, _)| i).collect())
                .unwrap_or_default()
        } else {
            read_by_writes(command, args)
        };
        for i in read {
            if exists(store, args.get(i), now) {
                before.present_first = before.present_first.or(Some(i));
            } else {
                before.missing.push(i);
            }
        }
    }
    before
}

/// The keys writes look up as reads (Redis' `lookupKeyRead`), which hear
/// `keymiss` when they aren't there.
fn read_by_writes(command: &str, args: &Args<'_>) -> Vec<usize> {
    let n = args.len();
    let count = |i: usize| {
        (i < n)
            .then(|| std::str::from_utf8(args.get(i)).ok()?.parse::<usize>().ok())
            .flatten()
            .unwrap_or(0)
    };
    match command {
        "getdel" | "getex" | "getset" | "xclaim" | "xautoclaim" => vec![1],
        // The same key twice is refused before anything's looked up.
        "copy" if args.get(1) != args.get(2) || find(args, 3, b"DB").is_some() => vec![1],
        "set" if (3..n).any(|i| is(args, i, b"GET")) => vec![1],
        "bitfield" if !(2..n).any(|i| is(args, i, b"SET") || is(args, i, b"INCRBY")) => vec![1],
        "bitop" => (3..n).collect(),
        "sinterstore" | "sunionstore" | "sdiffstore" => (2..n).collect(),
        "zunionstore" | "zinterstore" | "zdiffstore" => (3..(3 + count(2)).min(n)).collect(),
        "zrangestore" | "geosearchstore" => vec![2],
        "sort" | "georadius" | "georadiusbymember" => vec![1],
        "pfmerge" => (1..n).collect(),
        "xreadgroup" => match find(args, 4, b"STREAMS") {
            Some(at) => (at + 1..at + 1 + (n - at - 1) / 2).collect(),
            None => Vec::new(),
        },
        _ => Vec::new(),
    }
}

/// After the command `before` was taken for: its events, sent when the
/// outermost command running ends. `reply` is what it replied.
#[allow(clippy::too_many_arguments)]
pub(crate) fn after(
    ctx: &Context,
    session: &Session,
    now: Millis,
    command: &'static str,
    args: &Args<'_>,
    reply: &[u8],
    before: Before,
) {
    let logged = LOG.with_borrow_mut(|log| log.split_off(before.mark.min(log.len())));
    let flags = ctx.keyspace_events();
    let mut events = Vec::new();
    let db = session.db as u16;
    plan(
        ctx,
        db,
        now,
        command,
        args,
        reply,
        &before,
        logged,
        flags,
        &mut events,
    );
    PENDING.with_borrow_mut(|pending| pending.append(&mut events));
    DEPTH.set(DEPTH.get().saturating_sub(1));
    if DEPTH.get() == 0 {
        send_waiting(ctx, flags);
    }
}

/// Where a blocked client is served: how much is logged before its command.
pub(crate) fn mark() -> usize {
    LOG.with_borrow(Vec::len)
}

/// A blocked client's command, served in database `db`, with what it logged
/// since `mark`: its events go after those of the command that served it.
pub(crate) fn served(
    ctx: &Context,
    db: usize,
    now: Millis,
    args: &Args<'_>,
    reply: &[u8],
    mark: usize,
) {
    let logged = LOG.with_borrow_mut(|log| log.split_off(mark.min(log.len())));
    let flags = ctx.keyspace_events();
    let before = Before {
        mark,
        xlen: None,
        consumers: Vec::new(),
        missing: Vec::new(),
        present_first: None,
    };
    let mut events = Vec::new();
    let name = crate::cmd::command_name(args.get(0)).unwrap_or("");
    plan(
        ctx,
        db as u16,
        now,
        name,
        args,
        reply,
        &before,
        logged,
        flags,
        &mut events,
    );
    LATER.with_borrow_mut(|later| later.append(&mut events));
}

/// Outside a command (the expiry thread, or blocked clients served after
/// one): sends what's waiting, and what the store logged meanwhile (keys
/// that expired).
pub(crate) fn flush(ctx: &Context) {
    if DEPTH.get() != 0 {
        return;
    }
    let flags = ctx.keyspace_events();
    let logged = LOG.with_borrow_mut(std::mem::take);
    let mut events = Vec::new();
    for entry in logged {
        if let Some((class, name)) = by_store(entry.how) {
            events.push(Event {
                db: entry.db,
                class,
                name,
                key: entry.key,
            });
        }
    }
    PENDING.with_borrow_mut(|pending| pending.append(&mut events));
    send_waiting(ctx, flags);
}

fn send_waiting(ctx: &Context, flags: u32) {
    let pending = PENDING.with_borrow_mut(std::mem::take);
    let later = LATER.with_borrow_mut(std::mem::take);
    for event in pending.into_iter().chain(later) {
        send(ctx, flags, &event);
    }
}

/// Publishes one event, as Redis' `notifyKeyspaceEvent`: to the key's
/// channel, then to the event's.
fn send(ctx: &Context, flags: u32, event: &Event) {
    if flags & event.class == 0 {
        return;
    }
    if flags & KEYSPACE != 0 {
        let mut channel = format!("__keyspace@{}__:", event.db).into_bytes();
        channel.extend_from_slice(&event.key);
        ctx.pubsub.publish(&channel, event.name.as_bytes());
    }
    if flags & KEYEVENT != 0 {
        let channel = format!("__keyevent@{}__:{}", event.db, event.name);
        ctx.pubsub.publish(channel.as_bytes(), &event.key);
    }
}

/// The events the store's own changes are, whatever the command.
fn by_store(how: Change) -> Option<(u32, &'static str)> {
    match how {
        Change::Expired => Some((EXPIRED, "expired")),
        Change::Evicted => Some((EVICTED, "evicted")),
        _ => None,
    }
}

/// A command's events, from what it was asked, replied and changed.
struct Plan<'a> {
    db: u16,
    logged: Vec<Logged>,
    /// Which of `logged` have been sent as events.
    sent: Vec<bool>,
    events: &'a mut Vec<Event>,
    /// The command's own events, before `expired` and `new` are put in.
    own: Vec<Own>,
}

/// One of a command's own events: (database, class, name, key, and whether
/// it's a `new` sent whether or not the key was made).
type Own = (u16, u32, &'static str, Box<[u8]>, bool);

impl Plan<'_> {
    fn has(&self, db: u16, key: &[u8], f: impl Fn(Change) -> bool) -> bool {
        self.logged
            .iter()
            .any(|e| e.db == db && &*e.key == key && f(e.how))
    }

    /// Whether the command wrote, made or removed `key`.
    fn changed(&self, key: &[u8]) -> bool {
        self.has(self.db, key, |how| {
            matches!(how, Change::Written | Change::Created | Change::Removed)
        })
    }

    /// Whether the command's last change to `key` removed it.
    fn removed(&self, key: &[u8]) -> bool {
        self.logged
            .iter()
            .rev()
            .find(|e| {
                e.db == self.db
                    && &*e.key == key
                    && matches!(e.how, Change::Written | Change::Created | Change::Removed)
            })
            .is_some_and(|e| e.how == Change::Removed)
    }

    fn event(&mut self, class: u32, name: &'static str, key: &[u8]) {
        self.own.push((self.db, class, name, key.into(), false));
    }

    fn event_in(&mut self, db: u16, class: u32, name: &'static str, key: &[u8]) {
        self.own.push((db, class, name, key.into(), false));
    }

    /// Whether the command made `key`: its first change to it made it (where it
    /// changed and then remade it, it was there before).
    fn made(&self, db: u16, key: &[u8]) -> bool {
        self.logged
            .iter()
            .find(|e| {
                e.db == db
                    && &*e.key == key
                    && matches!(e.how, Change::Written | Change::Created | Change::Removed)
            })
            .is_some_and(|e| e.how == Change::Created)
    }

    /// Whether `key` expired as the command looked at it, taking that expiry
    /// for the caller to send (or not) as it sees fit.
    fn take_expired(&mut self, key: &[u8]) -> bool {
        let found = self.logged.iter().enumerate().position(|(i, e)| {
            !self.sent[i] && e.how == Change::Expired && e.db == self.db && &*e.key == key
        });
        if let Some(i) = found {
            self.sent[i] = true;
        }
        found.is_some()
    }

    /// `new` before the next event, made or not: Redis deletes and adds the key.
    fn new_in(&mut self, db: u16, key: &[u8]) {
        self.own.push((db, NEW, "new", key.into(), true));
    }

    /// The events in Redis' order: evictions (made before the command ran);
    /// expiries of keys it sends nothing for; then its events, each key's
    /// expiry and `new` just before its first.
    fn finish(mut self) {
        for (i, e) in self.logged.iter().enumerate() {
            if e.how == Change::Evicted {
                self.sent[i] = true;
                self.events.push(Event {
                    db: e.db,
                    class: EVICTED,
                    name: "evicted",
                    key: e.key.clone(),
                });
            }
        }
        for (i, e) in self.logged.iter().enumerate() {
            if !self.sent[i]
                && e.how == Change::Expired
                && !self
                    .own
                    .iter()
                    .any(|(db, _, _, key, _)| *db == e.db && *key == e.key)
            {
                self.sent[i] = true;
                self.events.push(Event {
                    db: e.db,
                    class: EXPIRED,
                    name: "expired",
                    key: e.key.clone(),
                });
            }
        }
        let mut newed: Vec<(u16, Box<[u8]>)> = Vec::new();
        let own = std::mem::take(&mut self.own);
        for (db, class, name, key, forced) in own {
            for (i, e) in self.logged.iter().enumerate() {
                if !self.sent[i] && e.how == Change::Expired && e.db == db && e.key == key {
                    self.sent[i] = true;
                    self.events.push(Event {
                        db,
                        class: EXPIRED,
                        name: "expired",
                        key: key.clone(),
                    });
                }
            }
            let fresh = !newed.iter().any(|(d, k)| *d == db && *k == key);
            if forced {
                if fresh {
                    newed.push((db, key.clone()));
                    self.events.push(Event {
                        db,
                        class,
                        name,
                        key,
                    });
                }
                continue;
            }
            if fresh && class != EXPIRED && self.made(db, &key) {
                newed.push((db, key.clone()));
                self.events.push(Event {
                    db,
                    class: NEW,
                    name: "new",
                    key: key.clone(),
                });
            }
            self.events.push(Event {
                db,
                class,
                name,
                key,
            });
        }
        // Keys made that no event was for (as by `BITFIELD` failing every write).
        let made: Vec<(u16, Box<[u8]>)> = self
            .logged
            .iter()
            .filter(|e| e.how == Change::Created && self.made(e.db, &e.key))
            .map(|e| (e.db, e.key.clone()))
            .collect();
        for (db, key) in made {
            if !newed.iter().any(|(d, k)| *d == db && *k == key) {
                newed.push((db, key.clone()));
                self.events.push(Event {
                    db,
                    class: NEW,
                    name: "new",
                    key,
                });
            }
        }
    }
}

/// The reply as an integer, if it's one.
fn int(reply: &[u8]) -> Option<i64> {
    let digits = reply.strip_prefix(b":")?;
    let end = digits.iter().position(|&b| b == b'\r')?;
    std::str::from_utf8(&digits[..end]).ok()?.parse().ok()
}

/// Whether the reply is nil (in either protocol).
fn nil(reply: &[u8]) -> bool {
    reply.starts_with(b"$-1") || reply.starts_with(b"*-1") || reply.starts_with(b"_")
}

/// Argument `i` matches `word`, ignoring case.
fn is(args: &Args<'_>, i: usize, word: &[u8]) -> bool {
    i < args.len() && args.get(i).eq_ignore_ascii_case(word)
}

/// Where the option `word` is among the arguments from `from` on.
fn find(args: &Args<'_>, from: usize, word: &[u8]) -> Option<usize> {
    (from..args.len()).find(|&i| is(args, i, word))
}

fn exists(store: &Store, key: &[u8], now: Millis) -> bool {
    store
        .update_key(store.key(key), now, |current| {
            (Update::Peek, current.is_some())
        })
        .unwrap_or(false)
}

fn stream_len(store: &Store, key: &[u8], now: Millis) -> Option<usize> {
    store.get_with(key, now, |value| match value?.view() {
        View::Object(Collection::Stream(stream)) => Some(stream.len()),
        _ => None,
    })
}

/// Whether the group `group` of the stream at `key` has the consumer.
fn has_consumer(store: &Store, key: &[u8], group: &[u8], consumer: &[u8], now: Millis) -> bool {
    store.get_with(key, now, |value| {
        value.is_some_and(|value| match value.view() {
            View::Object(Collection::Stream(stream)) => stream
                .groups
                .get(group)
                .is_some_and(|g| g.consumer_named(consumer).is_some()),
            _ => false,
        })
    })
}

/// `XADD` with `MAXLEN` or `MINID`, before its ID.
fn trims(args: &Args<'_>) -> bool {
    (2..args.len()).any(|i| is(args, i, b"MAXLEN") || is(args, i, b"MINID"))
}

/// The (key, group, consumer) a consumer-group command names, for each key.
fn group_keys<'a>(
    command: &str,
    args: &'a Args<'a>,
) -> impl Iterator<Item = (&'a [u8], &'a [u8], &'a [u8])> + 'a {
    let one = |key: usize, group: usize, consumer: usize| -> Vec<(&'a [u8], &'a [u8], &'a [u8])> {
        if consumer < args.len() {
            vec![(args.get(key), args.get(group), args.get(consumer))]
        } else {
            Vec::new()
        }
    };
    let found = match command {
        "xclaim" | "xautoclaim" => one(1, 2, 3),
        "xgroup" => one(2, 3, 4),
        "xreadgroup" if is(args, 1, b"GROUP") && args.len() > 3 => {
            let (group, consumer) = (args.get(2), args.get(3));
            match find(args, 4, b"STREAMS") {
                Some(at) => {
                    let n = (args.len() - at - 1) / 2;
                    (at + 1..at + 1 + n)
                        .map(|i| (args.get(i), group, consumer))
                        .collect()
                }
                None => Vec::new(),
            }
        }
        _ => Vec::new(),
    };
    found.into_iter()
}

/// A command's events, in Redis' order: see each command's notifications in
/// Redis' source (`notifyKeyspaceEvent`).
#[allow(clippy::too_many_arguments)]
fn plan(
    ctx: &Context,
    db: u16,
    now: Millis,
    command: &str,
    args: &Args<'_>,
    reply: &[u8],
    before: &Before,
    logged: Vec<Logged>,
    flags: u32,
    events: &mut Vec<Event>,
) {
    let sent = vec![false; logged.len()];
    let mut p = Plan {
        db,
        logged,
        sent,
        events,
        own: Vec::new(),
    };
    let ok = !reply.starts_with(b"-");
    let n = args.len();
    let k = |i: usize| if i < n { args.get(i) } else { &[][..] };
    let store = || ctx.store.db(db as usize);
    // Looked up first, so heard first. A command refused for its arguments
    // looks nothing up (but these two, which look first; and a missing group
    // is found after the lookup), and a WRONGTYPE stops at the first key that
    // was there.
    if flags & KEY_MISS != 0 {
        let looked =
            ok || matches!(command, "xclaim" | "xautoclaim") || reply.starts_with(b"-NOGROUP");
        let wrong_type = reply.starts_with(b"-WRONGTYPE");
        for &i in &before.missing {
            if !looked && (!wrong_type || before.present_first.is_some_and(|at| at < i)) {
                break;
            }
            p.event(KEY_MISS, "keymiss", k(i));
        }
    }
    match command {
        "del" | "unlink" => {
            let mut seen: Vec<&[u8]> = Vec::new();
            for i in 1..n {
                let key = k(i);
                if !seen.contains(&key) {
                    // Each key in turn: one that had expired is heard as it's looked at.
                    if p.take_expired(key) {
                        p.event(EXPIRED, "expired", key);
                    }
                    if p.removed(key) {
                        p.event(GENERIC, "del", key);
                    }
                }
                seen.push(key);
            }
        }
        "getdel" => {
            if p.removed(k(1)) {
                p.event(GENERIC, "del", k(1));
            }
        }
        "expire" | "pexpire" | "expireat" | "pexpireat" => {
            if p.removed(k(1)) {
                p.event(GENERIC, "del", k(1));
            } else if p.changed(k(1)) {
                p.event(GENERIC, "expire", k(1));
            }
        }
        "persist" => {
            if p.changed(k(1)) {
                p.event(GENERIC, "persist", k(1));
            }
        }
        "getex" => {
            let expiring = (2..n).any(|i| {
                is(args, i, b"EX")
                    || is(args, i, b"PX")
                    || is(args, i, b"EXAT")
                    || is(args, i, b"PXAT")
            });
            if p.removed(k(1)) {
                p.event(GENERIC, "del", k(1));
            } else if p.changed(k(1)) {
                p.event(GENERIC, if expiring { "expire" } else { "persist" }, k(1));
            }
        }
        "rename" | "renamenx" => {
            if ok && p.changed(k(1)) {
                p.new_in(db, k(2));
                p.event(GENERIC, "rename_from", k(1));
                p.event(GENERIC, "rename_to", k(2));
            }
        }
        "move" => {
            if let (Some(1), Some(to)) = (
                int(reply),
                std::str::from_utf8(k(2))
                    .ok()
                    .and_then(|s| s.parse::<u16>().ok()),
            ) {
                p.new_in(to, k(1));
                p.event(GENERIC, "move_from", k(1));
                p.event_in(to, GENERIC, "move_to", k(1));
            }
        }
        "copy" => {
            if int(reply) == Some(1) {
                let to = find(args, 3, b"DB")
                    .and_then(|i| std::str::from_utf8(k(i + 1)).ok()?.parse::<u16>().ok())
                    .unwrap_or(db);
                p.new_in(to, k(2));
                p.event_in(to, GENERIC, "copy_to", k(2));
            }
        }
        "restore" | "restore-asking" => {
            if ok {
                if p.removed(k(1)) {
                    p.event(GENERIC, "del", k(1));
                } else if p.changed(k(1)) {
                    p.new_in(db, k(1));
                    p.event(GENERIC, "restore", k(1));
                }
            }
        }
        "migrate" => {
            let keys: Vec<&[u8]> = if k(3).is_empty() {
                find(args, 6, b"KEYS").map_or(Vec::new(), |at| (at + 1..n).map(k).collect())
            } else {
                vec![k(3)]
            };
            for key in keys {
                if p.removed(key) {
                    p.event(GENERIC, "del", key);
                }
            }
        }
        "sort" => {
            if let Some(at) = find(args, 2, b"STORE") {
                let dst = k(at + 1);
                if p.removed(dst) {
                    p.event(GENERIC, "del", dst);
                } else if p.changed(dst) {
                    p.event(LIST, "sortstore", dst);
                } else if p.take_expired(dst) {
                    // Nothing to store: Redis deletes the key, expired or not.
                    p.event(GENERIC, "del", dst);
                }
            }
        }
        "set" => {
            if p.changed(k(1)) {
                p.event(STRING, "set", k(1));
                let expiring = (3..n).any(|i| {
                    is(args, i, b"EX")
                        || is(args, i, b"PX")
                        || is(args, i, b"EXAT")
                        || is(args, i, b"PXAT")
                });
                if expiring {
                    p.event(GENERIC, "expire", k(1));
                }
            }
        }
        "setnx" | "getset" => {
            if p.changed(k(1)) {
                p.event(STRING, "set", k(1));
            }
        }
        "setex" | "psetex" => {
            if p.changed(k(1)) {
                p.event(STRING, "set", k(1));
                p.event(GENERIC, "expire", k(1));
            }
        }
        "mset" | "msetnx" => {
            if ok && (command == "mset" || int(reply) == Some(1)) {
                for i in (1..n).step_by(2) {
                    p.event(STRING, "set", k(i));
                }
            }
        }
        "incr" | "decr" | "incrby" | "decrby" => {
            if ok {
                p.event(STRING, "incrby", k(1));
            }
        }
        "incrbyfloat" => {
            if ok {
                p.event(STRING, "incrbyfloat", k(1));
            }
        }
        "append" => {
            if ok {
                p.event(STRING, "append", k(1));
            }
        }
        "setrange" => {
            if ok && !k(3).is_empty() {
                p.event(STRING, "setrange", k(1));
            }
        }
        // Only when a bit changed, or the string was made or grew (Redis 7.2).
        "setbit" | "bitfield" => {
            if p.changed(k(1)) {
                p.event(STRING, "setbit", k(1));
            }
        }
        "bitop" => {
            if ok {
                if p.removed(k(2)) {
                    p.event(GENERIC, "del", k(2));
                } else if p.changed(k(2)) {
                    p.event(STRING, "set", k(2));
                }
            }
        }
        "pfadd" => {
            if p.changed(k(1)) {
                p.event(STRING, "pfadd", k(1));
            }
        }
        "pfmerge" => {
            if ok {
                p.event(STRING, "pfadd", k(1));
            }
        }
        "lpush" | "lpushx" | "rpush" | "rpushx" => {
            if p.changed(k(1)) {
                p.event(
                    LIST,
                    if command.starts_with('l') {
                        "lpush"
                    } else {
                        "rpush"
                    },
                    k(1),
                );
            }
        }
        "linsert" => {
            if int(reply).is_some_and(|n| n > 0) {
                p.event(LIST, "linsert", k(1));
            }
        }
        "lset" => {
            if ok {
                p.event(LIST, "lset", k(1));
            }
        }
        "lpop" | "rpop" => popped(
            &mut p,
            LIST,
            if command == "lpop" { "lpop" } else { "rpop" },
            k(1),
        ),
        "blpop" | "brpop" => {
            let name = if command == "blpop" { "lpop" } else { "rpop" };
            first_popped(&mut p, LIST, name, (1..n.saturating_sub(1)).map(k));
        }
        "lmpop" | "blmpop" => {
            let first = if command == "lmpop" { 1 } else { 2 };
            if let Some(count) = std::str::from_utf8(k(first))
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
            {
                let name = if is(args, first + 1 + count, b"LEFT") {
                    "lpop"
                } else {
                    "rpop"
                };
                first_popped(
                    &mut p,
                    LIST,
                    name,
                    (first + 1..(first + 1 + count).min(n)).map(k),
                );
            }
        }
        "lrem" => popped(&mut p, LIST, "lrem", k(1)),
        "ltrim" => {
            if ok && (p.changed(k(1)) || exists(store(), k(1), now)) {
                p.event(LIST, "ltrim", k(1));
                if p.removed(k(1)) {
                    p.event(GENERIC, "del", k(1));
                }
            }
        }
        "rpoplpush" | "brpoplpush" | "lmove" | "blmove" => {
            let (from, to) = match command {
                "lmove" | "blmove" => (is(args, 3, b"LEFT"), is(args, 4, b"LEFT")),
                _ => (false, true),
            };
            if ok && !nil(reply) && p.changed(k(1)) {
                p.event(LIST, if to { "lpush" } else { "rpush" }, k(2));
                p.event(LIST, if from { "lpop" } else { "rpop" }, k(1));
                if k(1) != k(2) && p.removed(k(1)) {
                    p.event(GENERIC, "del", k(1));
                }
            }
        }
        "sadd" => {
            if p.changed(k(1)) {
                p.event(SET, "sadd", k(1));
            }
        }
        "srem" => popped(&mut p, SET, "srem", k(1)),
        "spop" => popped(&mut p, SET, "spop", k(1)),
        "smove" => {
            if int(reply) == Some(1) && k(1) != k(2) {
                p.event(SET, "srem", k(1));
                if p.removed(k(1)) {
                    p.event(GENERIC, "del", k(1));
                }
                if p.changed(k(2)) {
                    p.event(SET, "sadd", k(2));
                }
            }
        }
        "sinterstore" | "sunionstore" | "sdiffstore" | "zunionstore" | "zinterstore"
        | "zdiffstore" | "zrangestore" | "geosearchstore" => {
            let class = if command.starts_with('s') { SET } else { ZSET };
            stored(&mut p, class, static_name(command), k(1), reply);
        }
        "georadius" | "georadiusbymember" => {
            let at = find(args, 3, b"STORE").or_else(|| find(args, 3, b"STOREDIST"));
            if let Some(at) = at {
                stored(&mut p, ZSET, "georadiusstore", k(at + 1), reply);
            }
        }
        "zadd" => {
            if p.changed(k(1)) {
                let incr = (2..n).any(|i| is(args, i, b"INCR"));
                p.event(ZSET, if incr { "zincr" } else { "zadd" }, k(1));
            }
        }
        "zincrby" => {
            if p.changed(k(1)) {
                p.event(ZSET, "zincr", k(1));
            }
        }
        "geoadd" => {
            if p.changed(k(1)) {
                p.event(ZSET, "zadd", k(1));
            }
        }
        "zrem" | "zremrangebyrank" | "zremrangebyscore" | "zremrangebylex" | "zpopmin"
        | "zpopmax" => popped(&mut p, ZSET, static_name(command), k(1)),
        "bzpopmin" | "bzpopmax" => {
            let name = if command == "bzpopmin" {
                "zpopmin"
            } else {
                "zpopmax"
            };
            first_popped(&mut p, ZSET, name, (1..n.saturating_sub(1)).map(k));
        }
        "zmpop" | "bzmpop" => {
            let first = if command == "zmpop" { 1 } else { 2 };
            if let Some(count) = std::str::from_utf8(k(first))
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
            {
                let name = if is(args, first + 1 + count, b"MIN") {
                    "zpopmin"
                } else {
                    "zpopmax"
                };
                first_popped(
                    &mut p,
                    ZSET,
                    name,
                    (first + 1..(first + 1 + count).min(n)).map(k),
                );
            }
        }
        "hset" | "hmset" => {
            if ok {
                p.event(HASH, "hset", k(1));
            }
        }
        "hsetnx" => {
            if int(reply) == Some(1) {
                p.event(HASH, "hset", k(1));
            }
        }
        "hincrby" | "hincrbyfloat" => {
            if ok {
                p.event(HASH, static_name(command), k(1));
            }
        }
        "hdel" => popped(&mut p, HASH, "hdel", k(1)),
        "xadd" => {
            if ok && !nil(reply) {
                p.event(STREAM, "xadd", k(1));
                if let Some(len) = before.xlen
                    && stream_len(store(), k(1), now).is_some_and(|after| after < len + 1)
                {
                    p.event(STREAM, "xtrim", k(1));
                }
            }
        }
        "xtrim" | "xdel" => {
            if int(reply).is_some_and(|n| n > 0) {
                p.event(STREAM, static_name(command), k(1));
            }
        }
        "xsetid" => {
            if ok {
                p.event(STREAM, "xsetid", k(1));
            }
        }
        "xgroup" => {
            let name = if is(args, 1, b"CREATE") && ok {
                Some("xgroup-create")
            } else if is(args, 1, b"SETID") && ok {
                Some("xgroup-setid")
            } else if is(args, 1, b"DESTROY") && int(reply) == Some(1) {
                Some("xgroup-destroy")
            } else if is(args, 1, b"CREATECONSUMER") && int(reply) == Some(1) {
                Some("xgroup-createconsumer")
            } else if is(args, 1, b"DELCONSUMER") && ok && before.consumers == [true] {
                Some("xgroup-delconsumer")
            } else {
                None
            };
            if let Some(name) = name {
                p.event(STREAM, name, k(2));
            }
        }
        "xreadgroup" | "xclaim" | "xautoclaim" if ok => {
            for (i, (key, group, consumer)) in group_keys(command, args).enumerate() {
                if before.consumers.get(i) == Some(&false)
                    && has_consumer(store(), key, group, consumer, now)
                {
                    p.event(STREAM, "xgroup-createconsumer", key);
                }
            }
        }
        _ => {}
    }
    p.finish();
}

/// A change that may empty the key: its event, then `del` if it went.
fn popped(p: &mut Plan<'_>, class: u32, name: &'static str, key: &[u8]) {
    if p.changed(key) {
        p.event(class, name, key);
        if p.removed(key) {
            p.event(GENERIC, "del", key);
        }
    }
}

/// Commands that pop from the first of their keys with something (named twice
/// or not): that key's events.
fn first_popped<'k>(
    p: &mut Plan<'_>,
    class: u32,
    name: &'static str,
    mut keys: impl Iterator<Item = &'k [u8]>,
) {
    if let Some(key) = keys.find(|key| p.changed(key)) {
        popped(p, class, name, key);
    }
}

/// A `...STORE` command's result at `dst`: its event if it has something,
/// else `del` if there was something to remove.
fn stored(p: &mut Plan<'_>, class: u32, name: &'static str, dst: &[u8], reply: &[u8]) {
    if int(reply).is_some_and(|n| n > 0) {
        p.event(class, name, dst);
    } else if p.removed(dst) || p.take_expired(dst) {
        // Nothing to store: Redis deletes the key, expired or not.
        p.event(GENERIC, "del", dst);
    }
}

/// The command's name as the event's, for the commands named so.
fn static_name(command: &str) -> &'static str {
    const NAMES: &[&str] = &[
        "sinterstore",
        "sunionstore",
        "sdiffstore",
        "zunionstore",
        "zinterstore",
        "zdiffstore",
        "zrangestore",
        "geosearchstore",
        "zrem",
        "zremrangebyrank",
        "zremrangebyscore",
        "zremrangebylex",
        "zpopmin",
        "zpopmax",
        "hincrby",
        "hincrbyfloat",
        "xtrim",
        "xdel",
    ];
    NAMES.iter().copied().find(|n| *n == command).unwrap_or("")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_read_and_say_as_in_redis() {
        let says = |s: &str| format(parse(s.as_bytes()).unwrap());
        assert_eq!(says("gKE"), "gKE");
        assert_eq!(says("$lshzxeKE"), "$lshzxeKE");
        assert_eq!(says("KA"), "AK");
        assert_eq!(says("EA"), "AE");
        assert_eq!(says("KEA"), "AKE");
        assert_eq!(says("Egx"), "gxE");
        assert_eq!(says("AKEmn"), "AKEm");
        assert_eq!(says("nm"), "nm");
        assert_eq!(says(""), "");
        assert_eq!(parse(b"KEw"), None);
        assert!(sends(parse(b"Ex").unwrap()));
        assert!(!sends(parse(b"KE").unwrap()));
        assert!(!sends(parse(b"gx").unwrap()));
    }

    #[test]
    fn replies_read_as_counts() {
        assert_eq!(int(b":12\r\n"), Some(12));
        assert_eq!(int(b"+OK\r\n"), None);
        assert!(nil(b"$-1\r\n") && nil(b"_\r\n") && nil(b"*-1\r\n"));
        assert!(!nil(b"$2\r\nab\r\n"));
    }
}
