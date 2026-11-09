//! Client-side caching: `CLIENT TRACKING`, `CACHING`, `GETREDIR` and
//! `TRACKINGINFO`, as Redis 7.2 has them.
//!
//! A tracking connection's reads leave an entry ([`Tracked`]) with each key
//! they read, among the key's watchers in its shard (`Notices` in `store.rs`).
//! A change to the key, made under the shard's lock, finds whom to tell as it
//! finds `WATCH`'s watchers, and with nobody tracking it costs commands
//! nothing more than the check for watchers they already make. The entry goes
//! with the message, as in Redis: a client hears of a key once per read. The
//! message goes to the connection's outbox, as pub/sub's do (or to the one
//! it redirects to), so it follows the reply of the command that caused it.
//!
//! Redis remembers a command's keys after it runs. Here they're registered
//! before it runs, so that a change on another thread between the read and the
//! registration can't be missed. What the command changes itself (a key that
//! expires as it's read) is then no news to its client, as Redis wouldn't have
//! registered the key yet, unless the client read the key before.
//!
//! In `BCAST` mode there are no entries: while some client broadcasts, every
//! change is queued (in the store's ready queue), and after the command the
//! keys go to the clients of each prefix they start with, one message a
//! prefix, as Redis sends them once an event loop. Meanwhile every connection
//! takes the checked path, where commands look at that queue when they end.
//!
//! The table is by database here, as everything is, where Redis' is by key
//! name only: a client hears of changes to keys it read in the database it
//! read them in. And a connection redirected to hears only once it has an
//! outbox: when it's subscribed, tracks or monitors, or has run a command since
//! the redirection began (Redis needs no such thing, its connections all being
//! one thread's).

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};

use crate::cmd::Args;
use crate::context::{Context, Session};
use crate::pubsub::Subscriber;
use crate::resp;

/// In [`Tracker::flags`].
const ON: u8 = 1;
const BCAST: u8 = 2;
const NOLOOP: u8 = 4;
const BROKEN: u8 = 8;

/// How a connection hears of a change: as a RESP3 push, or, if it's the
/// RESP2 connection invalidations are redirected to, as a pub/sub message.
const PUSH: &[u8] = b">2\r\n$10\r\ninvalidate\r\n";
const MESSAGE: &[u8] = b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n";

thread_local! {
    /// The tracking client whose command this thread runs, if any (0), and
    /// whether that command is a script.
    static RUNNING: Cell<(u64, bool)> = const { Cell::new((0, false)) };
    /// While a writer serves a blocked client's command: that client, whose
    /// messages wait (in `HELD`) until its reply is in place.
    static HOLDING: Cell<u64> = const { Cell::new(0) };
    static HELD: std::cell::RefCell<Held> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Messages held back while a blocked client's command was served.
pub(crate) type Held = Vec<(Arc<Subscriber>, Vec<u8>)>;

/// Runs `f`, a blocked client's command, for it: what it changes is its own,
/// as in Redis, and the messages it causes its client are returned, to
/// [`deliver`] once its reply is in place.
pub(crate) fn on_behalf_of<R>(client: u64, f: impl FnOnce() -> R) -> (R, Held) {
    let running = RUNNING.replace((client, false));
    let holding = HOLDING.replace(client);
    let result = f();
    HOLDING.set(holding);
    RUNNING.set(running);
    let held = HELD.with_borrow_mut(std::mem::take);
    (result, held)
}

/// Sends what [`on_behalf_of`] held back.
pub(crate) fn deliver(held: Held) {
    for (outbox, message) in held {
        outbox.push(&message, b"");
    }
}

/// The tracking client whose command this thread runs: Redis' "current client"
/// for `NOLOOP`. 0 for none.
fn current() -> u64 {
    RUNNING.get().0
}

/// Who made a change, for `NOLOOP` in `BCAST` mode: as in Redis, not the
/// client whose script made it, nor anyone for an expiry. 0 for no one.
pub(crate) fn modifier(by_expiry: bool) -> u64 {
    match RUNNING.get() {
        _ if by_expiry => 0,
        (_, true) => 0,
        (id, false) => id,
    }
}

/// How many keys the tracking table has, and entries (`INFO`'s
/// `tracking_total_keys` and `tracking_total_items`).
#[derive(Default)]
pub(crate) struct Counts {
    keys: AtomicUsize,
    items: AtomicUsize,
}

impl Counts {
    pub(crate) fn key_added(&self) {
        self.keys.fetch_add(1, Relaxed);
    }

    pub(crate) fn key_gone(&self) {
        self.keys.fetch_sub(1, Relaxed);
    }
}

/// A tracking client, as changes reach it.
pub(crate) struct Tracker {
    id: u64,
    /// Its connection's outbox.
    outbox: Arc<Subscriber>,
    flags: AtomicU8,
    /// The client it redirects to, and that one's outbox, if it does.
    redirect: Mutex<Option<(u64, Weak<Subscriber>)>>,
    /// The command it runs, numbered, or 0 between commands (see [`Tracked`]).
    running: AtomicU64,
    commands: AtomicU64,
    counts: Arc<Counts>,
}

impl Tracker {
    fn has(&self, flag: u8) -> bool {
        self.flags.load(Relaxed) & flag != 0
    }

    /// Whether it hears of the keys it read: on, and not broadcasting.
    fn tracks_keys(&self) -> bool {
        self.flags.load(Relaxed) & (ON | BCAST) == ON
    }

    /// Numbers the command it starts.
    fn start(&self) -> u64 {
        let n = self.commands.fetch_add(1, Relaxed) + 1;
        self.running.store(n, Relaxed);
        n
    }

    /// Sends it `body` (keys, or a null for all of them) as Redis'
    /// `sendTrackingMessage` does.
    fn tell(&self, body: &[u8]) {
        let redirect = self.redirect.lock();
        match &*redirect {
            None => {
                if self.outbox.is_resp3() {
                    if HOLDING.get() == self.id {
                        let message = [PUSH, body].concat();
                        HELD.with_borrow_mut(|held| held.push((self.outbox.clone(), message)));
                    } else {
                        self.outbox.push(PUSH, body);
                    }
                }
            }
            Some((id, to)) => match to.upgrade() {
                Some(to) if to.is_resp3() => to.push(PUSH, body),
                Some(to) if to.is_subscribed() => to.push(MESSAGE, body),
                Some(_) => {}
                None => {
                    // The connection it redirects to is gone: it hears so.
                    self.flags.fetch_or(BROKEN, Relaxed);
                    if self.outbox.is_resp3() {
                        let mut broken = b">2\r\n$21\r\ntracking-redir-broken\r\n".to_vec();
                        resp::write_integer(&mut broken, *id as i64);
                        self.outbox.push(&broken, b"");
                    }
                }
            },
        }
    }

    /// `CLIENT LIST`'s flags for it, and `redir=`.
    pub(crate) fn list_flags(&self, flags: &mut String) -> i64 {
        let had = self.flags.load(Relaxed);
        if had & ON == 0 {
            return -1;
        }
        flags.push('t');
        if had & BROKEN != 0 {
            flags.push('R');
        }
        if had & BCAST != 0 {
            flags.push('B');
        }
        self.redirect
            .lock()
            .as_ref()
            .map_or(0, |(id, _)| *id as i64)
    }
}

/// One array of keys, as messages carry them.
fn keys_body<'a>(keys: impl ExactSizeIterator<Item = &'a [u8]>) -> Vec<u8> {
    let mut body = Vec::new();
    resp::write_array_header(&mut body, keys.len());
    for key in keys {
        resp::write_bulk(&mut body, key);
    }
    body
}

/// A tracking client's entry with a key it read: the key's next change goes
/// to it, and then the entry goes.
pub(crate) struct Tracked {
    tracker: Arc<Tracker>,
    /// The command that last read the key.
    since: u64,
    /// That command registered it anew. (Redis registers keys after the
    /// command, so a change it makes itself is no news to its client then.)
    fresh: bool,
}

impl Tracked {
    pub(crate) fn new(tracker: &Arc<Tracker>, since: u64) -> Self {
        tracker.counts.items.fetch_add(1, Relaxed);
        Tracked {
            tracker: tracker.clone(),
            since,
            fresh: true,
        }
    }

    pub(crate) fn counts(&self) -> &Arc<Counts> {
        &self.tracker.counts
    }

    pub(crate) fn is_of(&self, tracker: &Arc<Tracker>) -> bool {
        Arc::ptr_eq(&self.tracker, tracker)
    }

    /// The key was read again, by command `since`.
    pub(crate) fn renew(&mut self, since: u64) {
        self.since = since;
        self.fresh = false;
    }

    /// The key changed: tells the client if it's to hear of it. Whether the
    /// entry stays.
    pub(crate) fn changed(&mut self, key: &[u8]) -> bool {
        let tracker = &self.tracker;
        let me = current() == tracker.id;
        let reading = tracker.running.load(Relaxed) == self.since;
        if reading && self.fresh {
            if me {
                // The read itself changed it (an expiry): the key stays
                // registered, as Redis registers it after the command.
                return true;
            }
            // Another thread changed it while it was read: it may have read the
            // old value, so it hears.
            self.tell(key);
            return false;
        }
        if !(me && tracker.has(NOLOOP)) {
            self.tell(key);
        }
        if reading {
            // It reads the key again, which Redis would register afterwards.
            self.fresh = true;
            return true;
        }
        false
    }

    /// The entry is removed to keep the table within `tracking-table-max-keys`.
    pub(crate) fn evicted(&self, key: &[u8]) {
        self.tell(key);
    }

    fn tell(&self, key: &[u8]) {
        if self.tracker.tracks_keys() {
            self.tracker.tell(&keys_body([key].into_iter()));
        }
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.tracker.counts.items.fetch_sub(1, Relaxed);
    }
}

/// `BCAST` prefixes, and the clients of each, in order.
type Prefixes = BTreeMap<Box<[u8]>, Vec<Arc<Tracker>>>;

/// The server's tracking clients.
pub(crate) struct Tracking {
    counts: Arc<Counts>,
    /// Those with tracking on, by id.
    clients: Mutex<HashMap<u64, Arc<Tracker>>>,
    /// `BCAST` prefixes, and the clients of each.
    prefixes: RwLock<Prefixes>,
    /// `tracking-table-max-keys`, 0 for no limit.
    max_keys: AtomicU64,
    /// Where trimming the table goes on from.
    cursor: AtomicUsize,
}

impl Default for Tracking {
    fn default() -> Self {
        Tracking {
            counts: Arc::default(),
            clients: Mutex::default(),
            prefixes: RwLock::default(),
            max_keys: AtomicU64::new(1_000_000),
            cursor: AtomicUsize::new(0),
        }
    }
}

impl Tracking {
    /// `INFO`'s `tracking_clients`.
    pub(crate) fn clients(&self) -> usize {
        self.clients.lock().len()
    }

    /// `INFO`'s `tracking_total_keys`, `tracking_total_items` and
    /// `tracking_total_prefixes`.
    pub(crate) fn totals(&self) -> (usize, usize, usize) {
        (
            self.counts.keys.load(Relaxed),
            self.counts.items.load(Relaxed),
            self.prefixes.read().len(),
        )
    }

    pub(crate) fn set_max_keys(&self, ctx: &Context, max: u64) {
        self.max_keys.store(max, Relaxed);
        self.limit(ctx);
    }

    /// Keeps the table within `tracking-table-max-keys`: as Redis does after
    /// each round of commands, keys go, and their clients hear of them.
    pub(crate) fn limit(&self, ctx: &Context) {
        let max = self.max_keys.load(Relaxed) as usize;
        if max == 0 || self.counts.keys.load(Relaxed) <= max {
            return;
        }
        // Nobody's command makes this change, for `NOLOOP`.
        let running = RUNNING.replace((0, false));
        let done = || self.counts.keys.load(Relaxed) <= max;
        let start = self.cursor.fetch_add(1, Relaxed);
        for db in ctx.store.live() {
            if db.untrack_until(start, &done) {
                break;
            }
        }
        RUNNING.set(running);
    }

    /// After a flush: every tracking client hears of all keys at once (a null),
    /// and the table is emptied.
    pub(crate) fn flushed(&self, ctx: &Context) {
        for tracker in self.clients.lock().values() {
            // A null in the client's protocol.
            let null: &[u8] = if tracker.outbox.is_resp3() {
                resp::NULL
            } else {
                resp::NULL_BULK
            };
            tracker.tell(null);
        }
        if self.counts.items.load(Relaxed) != 0 {
            for db in ctx.store.live() {
                db.forget_tracked();
            }
        }
    }

    /// Tells the broadcasting clients of `changed` keys, each with whoever
    /// changed it last (for `NOLOOP`): a message a prefix, keys in order.
    pub(crate) fn broadcast(&self, changed: Vec<(Box<[u8]>, u64)>) {
        let mut keys: BTreeMap<&[u8], u64> = BTreeMap::new();
        for (key, by) in &changed {
            keys.insert(key, *by);
        }
        let prefixes = self.prefixes.read();
        for (prefix, clients) in prefixes.iter() {
            let matching: Vec<(&[u8], u64)> = keys
                .range::<[u8], _>((Bound::Included(&**prefix), Bound::Unbounded))
                .take_while(|(key, _)| key.starts_with(prefix))
                .map(|(key, by)| (*key, *by))
                .collect();
            if matching.is_empty() {
                continue;
            }
            let mut all = None;
            for tracker in clients {
                if tracker.has(NOLOOP) {
                    let theirs: Vec<&[u8]> = matching
                        .iter()
                        .filter(|(_, by)| *by != tracker.id)
                        .map(|(key, _)| *key)
                        .collect();
                    if !theirs.is_empty() {
                        tracker.tell(&keys_body(theirs.into_iter()));
                    }
                } else {
                    let body =
                        all.get_or_insert_with(|| keys_body(matching.iter().map(|(key, _)| *key)));
                    tracker.tell(body);
                }
            }
        }
    }

    /// A tracking connection went: it tracks no more.
    pub(crate) fn gone(&self, ctx: &Context, tracker: &Arc<Tracker>) {
        tracker.flags.store(0, Relaxed);
        self.clients.lock().remove(&tracker.id);
        let theirs: Vec<Box<[u8]>> = self
            .prefixes
            .read()
            .iter()
            .filter(|(_, clients)| clients.iter().any(|t| Arc::ptr_eq(t, tracker)))
            .map(|(prefix, _)| prefix.clone())
            .collect();
        for prefix in theirs {
            self.remove_prefix(ctx, &prefix, tracker);
        }
    }

    fn add_prefix(&self, ctx: &Context, prefix: &[u8], tracker: &Arc<Tracker>) {
        let mut prefixes = self.prefixes.write();
        let was_empty = prefixes.is_empty();
        prefixes
            .entry(prefix.into())
            .or_default()
            .push(tracker.clone());
        if was_empty {
            ctx.store.set_broadcast(true);
            ctx.set_broadcasting(true);
        }
    }

    fn remove_prefix(&self, ctx: &Context, prefix: &[u8], tracker: &Arc<Tracker>) {
        let mut prefixes = self.prefixes.write();
        if let Some(clients) = prefixes.get_mut(prefix) {
            clients.retain(|t| !Arc::ptr_eq(t, tracker));
            if clients.is_empty() {
                prefixes.remove(prefix);
            }
        }
        if prefixes.is_empty() && ctx.store.broadcasting() {
            ctx.store.set_broadcast(false);
            ctx.set_broadcasting(false);
        }
    }
}

/// A connection's tracking, kept once it has turned tracking on (as Redis
/// keeps a client's id in the table after tracking is turned off, and its
/// entries count again if it's turned on).
pub(crate) struct SessionTracking {
    pub(crate) tracker: Arc<Tracker>,
    /// Its outbox, kept while it lasts.
    pub(crate) outbox: Arc<Subscriber>,
    pub(crate) on: bool,
    optin: bool,
    optout: bool,
    /// `CLIENT CACHING` was given for the next command.
    caching: bool,
    /// Its `BCAST` prefixes, in order.
    prefixes: Vec<Box<[u8]>>,
    /// Set while [`execute`] runs a command, which then runs as any other.
    pub(crate) busy: bool,
}

impl SessionTracking {
    fn bcast(&self) -> bool {
        self.tracker.has(BCAST)
    }

    /// Whether the next command's reads are remembered: in OPTIN mode only
    /// with `CLIENT CACHING yes`, and in OPTOUT mode unless `CLIENT CACHING no`.
    fn remembers(&self) -> bool {
        let wanted = if self.optin {
            self.caching
        } else {
            !(self.optout && self.caching)
        };
        self.on && !self.bcast() && wanted
    }

    /// The tracker a script's reads are remembered for, if they are.
    pub(crate) fn for_script(&self) -> Option<Arc<Tracker>> {
        self.remembers().then(|| self.tracker.clone())
    }
}

/// Runs a tracking client's command: what it changes is its own, for
/// `NOLOOP`, and what it reads is remembered (in [`before`]).
pub(crate) fn execute(
    ctx: &Context,
    session: &mut Session,
    now: crate::store::Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> crate::cmd::Reply {
    let Some(tracking) = session.tracking.as_mut() else {
        return crate::cmd::execute(ctx, session, now, args, out);
    };
    tracking.busy = true;
    let tracker = tracking.tracker.clone();
    tracker.start();
    let name = args.get(0);
    let script = [
        &b"EVAL"[..],
        b"EVALSHA",
        b"EVAL_RO",
        b"EVALSHA_RO",
        b"FCALL",
        b"FCALL_RO",
    ]
    .iter()
    .any(|s| name.eq_ignore_ascii_case(s));
    let running = RUNNING.replace((tracker.id, script));
    let reply = crate::cmd::execute(ctx, session, now, args, out);
    RUNNING.set(running);
    tracker.running.store(0, Relaxed);
    if let Some(tracking) = session.tracking.as_mut() {
        tracking.busy = false;
        // `CLIENT CACHING` holds for the next command, but for `CLIENT`'s and
        // those in a transaction, as in Redis.
        if session.multi.is_none() && !name.eq_ignore_ascii_case(b"CLIENT") {
            tracking.caching = false;
        }
    }
    ctx.tracking.limit(ctx);
    reply
}

/// Before a command runs: remembers its keys for its client if it reads them
/// (Redis' `readonly` commands but scripts, which go by what they call). In
/// a transaction or a script (`inner`), each command is numbered anew.
pub(crate) fn before(
    ctx: &Context,
    session: &Session,
    name: &'static str,
    args: &Args<'_>,
    inner: bool,
) {
    let tracker = match (&session.tracking, &session.reads_for) {
        (Some(tracking), _) if tracking.on => {
            if !tracking.remembers() {
                return;
            }
            &tracking.tracker
        }
        (_, Some(tracker)) => tracker,
        _ => return,
    };
    let since = if inner {
        tracker.start()
    } else {
        tracker.running.load(Relaxed)
    };
    if matches!(name, "eval_ro" | "evalsha_ro" | "fcall_ro") {
        return;
    }
    let sub = (args.len() > 1).then(|| args.get(1));
    let flags = crate::command_table::hidden(name, sub);
    if !flags.readonly || flags.pubsub {
        return;
    }
    let Some(specs) = crate::acl::target(name, args).and_then(|t| t.keys()) else {
        return;
    };
    let store = ctx.db(session);
    specs.each_key(args, |at, _| {
        store.track(args.get(at), tracker, since);
        true
    });
}

/// Ends tracking for a connection that goes, or `RESET`s.
pub(crate) fn end(ctx: &Context, session: &mut Session) {
    if let Some(tracking) = session.tracking.as_mut() {
        disable(ctx, tracking);
    }
}

fn disable(ctx: &Context, tracking: &mut SessionTracking) {
    let tracker = &tracking.tracker;
    for prefix in std::mem::take(&mut tracking.prefixes) {
        ctx.tracking.remove_prefix(ctx, &prefix, tracker);
    }
    if tracking.on {
        ctx.tracking.clients.lock().remove(&tracker.id);
    }
    tracker.flags.store(0, Relaxed);
    tracking.on = false;
    tracking.optin = false;
    tracking.optout = false;
    tracking.caching = false;
}

/// Whether one of `prefixes` starts the other.
fn overlap(a: &[u8], b: &[u8]) -> bool {
    a.starts_with(b) || b.starts_with(a)
}

/// `CLIENT TRACKING ON|OFF [REDIRECT id] [PREFIX p ...] [BCAST] [OPTIN]
/// [OPTOUT] [NOLOOP]`, checked in Redis' order.
pub(crate) fn client_tracking(
    ctx: &Context,
    session: &mut Session,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) {
    let mut redirect: Option<u64> = None;
    let (mut bcast, mut optin, mut optout, mut noloop) = (false, false, false, false);
    let mut prefixes: Vec<&[u8]> = Vec::new();
    let mut i = 3;
    while i < args.len() {
        let option = args.get(i);
        let more = i + 1 < args.len();
        let is = |name: &[u8]| option.eq_ignore_ascii_case(name);
        if is(b"REDIRECT") && more {
            i += 1;
            if redirect.is_some() {
                resp::write_error(
                    out,
                    b"ERR A client can only redirect to a single other client",
                );
                return;
            }
            let Some(id) = resp::parse_int(args.get(i)) else {
                resp::write_error(out, b"ERR value is not an integer or out of range");
                return;
            };
            if id <= 0 || !ctx.client_exists(id as u64) {
                resp::write_error(
                    out,
                    b"ERR The client ID you want redirect to does not exist",
                );
                return;
            }
            redirect = Some(id as u64);
        } else if is(b"BCAST") {
            bcast = true;
        } else if is(b"OPTIN") {
            optin = true;
        } else if is(b"OPTOUT") {
            optout = true;
        } else if is(b"NOLOOP") {
            noloop = true;
        } else if is(b"PREFIX") && more {
            i += 1;
            prefixes.push(args.get(i));
        } else {
            resp::write_error(out, b"ERR syntax error");
            return;
        }
        i += 1;
    }
    let on_off = args.get(2);
    if on_off.eq_ignore_ascii_case(b"ON") {
        if !bcast && !prefixes.is_empty() {
            resp::write_error(out, b"ERR PREFIX option requires BCAST mode to be enabled");
            return;
        }
        if let Some(tracking) = session.tracking.as_ref().filter(|t| t.on)
            && tracking.bcast() != bcast
        {
            resp::write_error(
                out,
                b"ERR You can't switch BCAST mode on/off before disabling tracking for this \
                  client, and then re-enabling it with a different mode.",
            );
            return;
        }
        if bcast && (optin || optout) {
            resp::write_error(out, b"ERR OPTIN and OPTOUT are not compatible with BCAST");
            return;
        }
        if optin && optout {
            resp::write_error(
                out,
                b"ERR You can't specify both OPTIN mode and OPTOUT mode",
            );
            return;
        }
        if let Some(tracking) = session.tracking.as_ref().filter(|t| t.on)
            && ((optin && tracking.optout) || (optout && tracking.optin))
        {
            resp::write_error(
                out,
                b"ERR You can't switch OPTIN/OPTOUT mode before disabling tracking for this \
                  client, and then re-enabling it with a different mode.",
            );
            return;
        }
        if bcast {
            let had: &[Box<[u8]>] = session.tracking.as_ref().map_or(&[], |t| &t.prefixes);
            for (n, prefix) in prefixes.iter().enumerate() {
                if let Some(old) = had.iter().find(|old| overlap(old, prefix)) {
                    let error = format!(
                        "ERR Prefix '{}' overlaps with an existing prefix '{}'. Prefixes for a \
                         single client must not overlap.",
                        String::from_utf8_lossy(prefix),
                        String::from_utf8_lossy(old)
                    );
                    resp::write_error(out, error.as_bytes());
                    return;
                }
                if let Some(other) = prefixes[n + 1..].iter().find(|p| overlap(prefix, p)) {
                    let error = format!(
                        "ERR Prefix '{}' overlaps with another provided prefix '{}'. Prefixes \
                         for a single client must not overlap.",
                        String::from_utf8_lossy(prefix),
                        String::from_utf8_lossy(other)
                    );
                    resp::write_error(out, error.as_bytes());
                    return;
                }
            }
        }
        let to = redirect.map(|id| (id, ctx.keep_outbox(id)));
        let tracking = start(ctx, session);
        let tracker = tracking.tracker.clone();
        if !tracking.on {
            tracking.on = true;
            ctx.tracking
                .clients
                .lock()
                .insert(tracker.id, tracker.clone());
        }
        *tracker.redirect.lock() = to;
        let mut flags = ON;
        if bcast {
            flags |= BCAST;
            if prefixes.is_empty() {
                prefixes.push(b"");
            }
            for prefix in prefixes {
                // (One it has already stays where it is.)
                if let Err(at) = tracking.prefixes.binary_search_by(|p| (**p).cmp(prefix)) {
                    tracking.prefixes.insert(at, prefix.into());
                    ctx.tracking.add_prefix(ctx, prefix, &tracker);
                }
            }
        }
        if noloop {
            flags |= NOLOOP;
        }
        tracker.flags.store(flags, Relaxed);
        tracking.optin = optin;
        tracking.optout = optout;
    } else if on_off.eq_ignore_ascii_case(b"OFF") {
        end(ctx, session);
    } else {
        resp::write_error(out, b"ERR syntax error");
        return;
    }
    session.refresh();
    out.extend_from_slice(resp::OK);
}

/// The connection's tracking, made the first time it's turned on.
fn start<'a>(ctx: &Context, session: &'a mut Session) -> &'a mut SessionTracking {
    if session.tracking.is_none() {
        let outbox = session.outbox(ctx);
        let tracker = Arc::new(Tracker {
            id: session.id,
            outbox: outbox.clone(),
            flags: AtomicU8::new(0),
            redirect: Mutex::new(None),
            running: AtomicU64::new(0),
            commands: AtomicU64::new(0),
            counts: ctx.tracking.counts.clone(),
        });
        ctx.update_client(session.id, |client| client.tracker = Some(tracker.clone()));
        session.tracking = Some(Box::new(SessionTracking {
            tracker,
            outbox,
            on: false,
            optin: false,
            optout: false,
            caching: false,
            prefixes: Vec::new(),
            busy: false,
        }));
    }
    session.tracking.as_mut().expect("just made")
}

/// `CLIENT CACHING YES|NO`.
pub(crate) fn client_caching(session: &mut Session, args: &Args<'_>, out: &mut Vec<u8>) {
    let Some(tracking) = session.tracking.as_mut().filter(|t| t.on) else {
        resp::write_error(
            out,
            b"ERR CLIENT CACHING can be called only when the client is in tracking mode with \
              OPTIN or OPTOUT mode enabled",
        );
        return;
    };
    let option = args.get(2);
    if option.eq_ignore_ascii_case(b"YES") {
        if !tracking.optin {
            resp::write_error(
                out,
                b"ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.",
            );
            return;
        }
    } else if option.eq_ignore_ascii_case(b"NO") {
        if !tracking.optout {
            resp::write_error(
                out,
                b"ERR CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.",
            );
            return;
        }
    } else {
        resp::write_error(out, b"ERR syntax error");
        return;
    }
    tracking.caching = true;
    out.extend_from_slice(resp::OK);
}

/// `CLIENT GETREDIR`: the client it redirects to, 0 for none, -1 if
/// tracking's off.
pub(crate) fn client_getredir(session: &Session, out: &mut Vec<u8>) {
    let redirect = match session.tracking.as_ref().filter(|t| t.on) {
        None => -1,
        Some(tracking) => redirect_of(&tracking.tracker),
    };
    resp::write_integer(out, redirect);
}

fn redirect_of(tracker: &Tracker) -> i64 {
    tracker
        .redirect
        .lock()
        .as_ref()
        .map_or(0, |(id, _)| *id as i64)
}

/// `CLIENT TRACKINGINFO`.
pub(crate) fn client_trackinginfo(session: &Session, out: &mut Vec<u8>) {
    let resp3 = session.resp3;
    let tracking = session.tracking.as_ref().filter(|t| t.on);
    let mut flags: Vec<&[u8]> = Vec::new();
    match tracking {
        None => flags.push(b"off"),
        Some(tracking) => {
            let had = tracking.tracker.flags.load(Relaxed);
            flags.push(b"on");
            if had & BCAST != 0 {
                flags.push(b"bcast");
            }
            if tracking.optin {
                flags.push(b"optin");
                if tracking.caching {
                    flags.push(b"caching-yes");
                }
            }
            if tracking.optout {
                flags.push(b"optout");
                if tracking.caching {
                    flags.push(b"caching-no");
                }
            }
            if had & NOLOOP != 0 {
                flags.push(b"noloop");
            }
            if had & BROKEN != 0 {
                flags.push(b"broken_redirect");
            }
        }
    }
    resp::write_map_header(out, 3, resp3);
    resp::write_bulk(out, b"flags");
    resp::write_set_header(out, flags.len(), resp3);
    for flag in flags {
        resp::write_bulk(out, flag);
    }
    resp::write_bulk(out, b"redirect");
    resp::write_integer(out, tracking.map_or(-1, |t| redirect_of(&t.tracker)));
    resp::write_bulk(out, b"prefixes");
    let prefixes: &[Box<[u8]>] = tracking.map_or(&[], |t| &t.prefixes);
    resp::write_array_header(out, prefixes.len());
    for prefix in prefixes {
        resp::write_bulk(out, prefix);
    }
}
