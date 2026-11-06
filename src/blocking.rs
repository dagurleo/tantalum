//! Blocking commands (`BLPOP` and the rest): a client whose keys are all
//! empty waits until another client's write gives one of them data.
//!
//! How it fits together:
//! - A blocking command first tries its keys as its non-blocking twin
//!   would. If all are empty, [`block`] registers a [`Waiter`] on each key
//!   (in the key's shard, under its lock: [`Store::wait_on`]) and tries
//!   once more, so that data added in between isn't missed, then hands the
//!   connection a [`Blocked`] to wait on ([`Reply::Block`]).
//! - A write that creates one of those keys queues it as ready. After that
//!   command, [`serve_ready`] runs the waiting commands again, oldest first,
//!   while the key has data of their kind: as Redis 7.2 does, right after
//!   the command that added it and before that client's next one. The
//!   writer sends each reply itself, on the waiter's socket, before its own
//!   reply goes out (so the woken client hears first, as from Redis); what
//!   it can't send there goes into the waiter, whose connection is woken to
//!   send it.
//! - The connection waits for that, for its timeout, or for the client to
//!   go away. Commands the client pipelines meanwhile wait their turn.
//!
//! Unlike in Redis, other clients' commands run in parallel on other
//! threads, so one of them may take the data between the write and the
//! serving. The waiter then just goes on waiting.
//!
//! `XREADGROUP` also waits for its key to change at all, because its group goes
//! if the key does: it registers with the clients watching the key too
//! ([`Store::wake_on_change`]), and it's run again, even if the key is gone or
//! holds another kind now, to reply with the error. After a change by `GET` (an
//! expiry) or `SET`, whose fast paths don't check for ready keys, that comes
//! with the next command or from the expiry thread, within 100 ms.
//!
//! None of this costs commands anything while nothing is blocked: writes
//! check for waiters only when they create a key and the key's shard has
//! some, and commands check one flag for ready keys.

use std::ops::Range;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::task::Poll;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use crate::cmd::{Args, Reply};
use crate::context::{Context, ReplyMode, Session};
use crate::resp;
use crate::store::{Kind, Millis, Store};
use crate::wake::{Mailbox, Wake};

/// What a blocking command's attempt did.
pub(crate) enum Attempt {
    /// It replied: with data, or an error.
    Done,
    /// Every key was empty, and nothing was written: wait on the arguments
    /// `keys` for data of `kind`, for `timeout` (`None`: forever). In a
    /// transaction it replies nil at once instead: a null array, or with
    /// `nil_bulk`, a null bulk string (as Redis' BLMOVE does).
    /// With `args`, the command waits with these arguments instead (XREAD's
    /// `$` made the ID it stood for).
    Wait {
        keys: Range<usize>,
        timeout: Option<Duration>,
        kind: Kind,
        nil_bulk: bool,
        args: Option<Vec<Vec<u8>>>,
        /// It also runs again when a key changes in any way, even if it's gone
        /// or now holds another kind (`XREADGROUP`, whose group may be gone).
        on_change: bool,
    },
}

/// A blocking command's attempt, run first by the command and then again
/// each time it may be served.
pub(crate) type Try = fn(&Context, &mut Session, Millis, &Args<'_>, &mut Vec<u8>) -> Attempt;

/// Registering on its keys, by its own command. A writer that finds it
/// leaves it [`READIED`] instead of serving it.
const ARMING: u8 = 0;
/// A key got data while it was arming: its command tries again.
const READIED: u8 = 1;
/// Registered, waiting to be served.
const WAITING: u8 = 2;
/// A writer is running its command.
const SERVING: u8 = 3;
/// And another of its keys got data meanwhile: should this run find them
/// all empty (it may have looked before), it runs again.
const SERVING_AGAIN: u8 = 4;
/// Its reply is ready.
const SERVED: u8 = 5;
/// Timed out or gone: writers skip it.
const CANCELLED: u8 = 6;

/// A blocked command, as the keys it waits on hold it.
pub struct Waiter {
    state: AtomicU8,
    /// The kind of data it waits for.
    kind: Kind,
    /// The command, to run again: its arguments back to back.
    buf: Box<[u8]>,
    ranges: Box<[Range<usize>]>,
    /// Which arguments are the keys it waits on.
    keys: Range<usize>,
    attempt: Try,
    client: u64,
    resp3: bool,
    /// The database it's blocked in.
    db: u32,
    /// Its connection's socket, for the writer to send the reply on (-1:
    /// none, or one only its connection may write to: TLS, a subscriber).
    socket: RawFd,
    /// Its reply once served, or what the writer couldn't send of it.
    reply: Mutex<Vec<u8>>,
    /// Wakes its connection while it waits.
    waker: Mutex<Option<Wake>>,
    /// With [`Attempt::Wait`]'s `on_change`: how the keys know it.
    on_change: Option<Arc<AtomicBool>>,
    /// What it replies when time's up, for a wait that isn't for a key (`WAIT`):
    /// otherwise a null array.
    timed_out: Option<&'static [u8]>,
}

impl Waiter {
    pub(crate) fn client(&self) -> u64 {
        self.client
    }

    pub(crate) fn kind(&self) -> Kind {
        self.kind
    }

    fn timed_out_reply(&self) -> &'static [u8] {
        match self.timed_out {
            Some(reply) => reply,
            None if self.resp3 => resp::NULL,
            None => b"*-1\r\n",
        }
    }

    pub(crate) fn wakes_on_change(&self) -> bool {
        self.on_change.is_some()
    }

    fn keys(&self) -> impl Iterator<Item = &[u8]> {
        self.ranges[self.keys.clone()]
            .iter()
            .map(|r| &self.buf[r.clone()])
    }

    /// For a writer, under the key's lock: takes it to serve if it's
    /// waiting. One still arming, or being served for another key, is
    /// marked to try again instead.
    pub(crate) fn claim(&self) -> bool {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            let (from, to) = match state {
                WAITING => (WAITING, SERVING),
                ARMING => (ARMING, READIED),
                SERVING => (SERVING, SERVING_AGAIN),
                _ => return false,
            };
            match self
                .state
                .compare_exchange(from, to, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return to == SERVING,
                Err(now) => state = now,
            }
        }
    }

    /// Whether it's waiting, past its command's last look at its keys.
    pub(crate) fn is_waiting(&self) -> bool {
        self.state.load(Ordering::Acquire) == WAITING
    }

    /// Done arming, unless a key got data meanwhile (then `false`, to try
    /// again).
    fn arm(&self) -> bool {
        if self
            .state
            .compare_exchange(ARMING, WAITING, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return true;
        }
        self.state.store(ARMING, Ordering::Release);
        false
    }

    /// After a run that found its keys empty: back to waiting, unless one
    /// got data meanwhile (then `false`, to run again).
    fn unclaim(&self) -> bool {
        if self
            .state
            .compare_exchange(SERVING, WAITING, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return true;
        }
        self.state.store(SERVING, Ordering::Release);
        false
    }

    /// Stops waiting, unless it was served: then `false`.
    fn give_up(&self) -> bool {
        loop {
            match self.state.load(Ordering::Acquire) {
                SERVED => return false,
                CANCELLED => return true,
                // A writer on another thread is running its command, which
                // takes microseconds.
                SERVING | SERVING_AGAIN => std::thread::yield_now(),
                state => {
                    if self
                        .state
                        .compare_exchange(state, CANCELLED, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        return true;
                    }
                }
            }
        }
    }

    /// Runs its command again, for a writer that claimed it.
    fn run_again(&self, ctx: &Context, now: Millis, out: &mut Vec<u8>) -> Attempt {
        let mut session = Session {
            id: self.client,
            authenticated: true,
            unrestricted: true,
            pubsub: None,
            multi: None,
            watching: None,
            deny_blocking: false,
            in_script: false,
            name: None,
            resp3: self.resp3,
            db: self.db,
            special: false,
            reply: ReplyMode::On,
            no_evict: false,
            no_touch: false,
            monitor: None,
            peer: None,
            commands: 0,
            hits: 0,
            misses: 0,
            socket: -1,
        };
        let args = Args::new(&self.buf, &self.ranges);
        (self.attempt)(ctx, &mut session, now, &args, out)
    }

    /// For the writer that claimed it, which holds the connection (and so
    /// its socket) until it serves it: sends what of `reply` its socket
    /// takes at once, returning the rest. The woken client then hears before
    /// the writer does, as from Redis: the connection, on another thread,
    /// would send after the writer had. (BullMQ's waiting workers count on
    /// it.)
    fn send_now<'r>(&self, reply: &'r [u8]) -> &'r [u8] {
        if self.socket < 0 {
            return reply;
        }
        #[cfg(target_os = "linux")]
        let flags = libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL;
        #[cfg(not(target_os = "linux"))]
        let flags = libc::MSG_DONTWAIT;
        // SAFETY: the socket is open while its waiter is claimed (its
        // connection waits for the claim before it drops it), and `reply`
        // is valid for its length.
        let sent = unsafe { libc::send(self.socket, reply.as_ptr().cast(), reply.len(), flags) };
        &reply[usize::try_from(sent).unwrap_or(0)..]
    }

    /// Logs its command, served with `reply`, if the append-only log is on
    /// (run again, it finds the data it was served).
    fn log(&self, reply: &[u8]) {
        if !crate::aof::logging() {
            return;
        }
        crate::aof::select(self.db as usize);
        let args = Args::new(&self.buf, &self.ranges);
        if let Some(command) = crate::cmd::find_write_command(args.get(0)) {
            crate::aof::record(command, &args, reply);
        }
    }

    fn serve(&self, reply: Vec<u8>) {
        *self.reply.lock() = reply;
        self.state.store(SERVED, Ordering::Release);
        let waker = self.waker.lock().take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// A claimed waiter, served when dropped if its command didn't finish
/// (it panicked), so its connection isn't left waiting on it forever.
/// Only while this claim holds it: once served, or given back to waiting,
/// another writer may claim it, and that claim is its own.
struct Claimed {
    waiter: Arc<Waiter>,
    holds: bool,
}

impl Claimed {
    fn serve(&mut self, reply: Vec<u8>) {
        self.holds = false;
        self.waiter.serve(reply);
    }

    /// [`Waiter::unclaim`]; once it's waiting again, this claim lets go.
    fn unclaim(&mut self) -> bool {
        let unclaimed = self.waiter.unclaim();
        self.holds = !unclaimed;
        unclaimed
    }
}

impl Drop for Claimed {
    fn drop(&mut self) {
        if self.holds {
            self.waiter
                .serve(b"-ERR internal error, closing the connection\r\n".to_vec());
        }
    }
}

/// Runs a blocking command: `attempt`, and if it found every key empty,
/// registers to wait on them.
pub(crate) fn block(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    attempt: Try,
) -> Reply {
    let Attempt::Wait {
        keys,
        timeout,
        kind,
        nil_bulk,
        args: rewritten,
        on_change,
    } = attempt(ctx, session, now, args, out)
    else {
        return Reply::Written;
    };
    if session.deny_blocking {
        // In EXEC, which can't wait: as if timed out.
        out.extend_from_slice(match (session.resp3, nil_bulk) {
            (true, _) => resp::NULL,
            (false, true) => resp::NULL_BULK,
            (false, false) => b"*-1\r\n",
        });
        return Reply::Written;
    }
    let mut buf = Vec::new();
    let mut ranges = Vec::with_capacity(args.len());
    let mut push = |arg: &[u8]| {
        ranges.push(buf.len()..buf.len() + arg.len());
        buf.extend_from_slice(arg);
    };
    match &rewritten {
        Some(rewritten) => rewritten.iter().for_each(|arg| push(arg)),
        None => (0..args.len()).for_each(|i| push(args.get(i))),
    }
    let waiter = Arc::new(Waiter {
        state: AtomicU8::new(ARMING),
        kind,
        buf: buf.into(),
        ranges: ranges.into(),
        keys,
        attempt,
        client: session.id,
        resp3: session.resp3,
        db: session.db,
        // A subscriber's connection writes messages while it waits.
        socket: if session.pubsub.is_none() {
            session.socket
        } else {
            -1
        },
        reply: Mutex::new(Vec::new()),
        waker: Mutex::new(None),
        on_change: on_change.then(|| Arc::new(AtomicBool::new(false))),
        timed_out: None,
    });
    let store = ctx.store.db(session.db as usize);
    let blocked = Blocked {
        waiter,
        store: store.clone(),
        timeout,
    };
    for key in blocked.waiter.keys() {
        store.wait_on(store.key(key), &blocked.waiter);
        if let Some(flag) = &blocked.waiter.on_change {
            store.wake_on_change(store.key(key), flag);
        }
    }
    store.count_blocked(1);
    store.track_waiter(&blocked.waiter);
    // Again, now that a write creating a key would see it (with what it
    // waits with).
    let waiter = blocked.waiter.clone();
    let args = Args::new(&waiter.buf, &waiter.ranges);
    loop {
        if let Attempt::Done = attempt(ctx, session, now, &args, out) {
            return Reply::Written; // `blocked` deregisters as it drops
        }
        if blocked.waiter.arm() {
            return Reply::Block(Box::new(blocked));
        }
    }
}

/// Waits for no key, only for `timeout`, then replies `reply`: `WAIT`, as there are
/// no replicas to hear from.
pub(crate) fn block_for(
    ctx: &Context,
    session: &Session,
    timeout: Option<Duration>,
    reply: &'static [u8],
) -> Reply {
    fn never(_: &Context, _: &mut Session, _: Millis, _: &Args<'_>, _: &mut Vec<u8>) -> Attempt {
        Attempt::Done
    }
    let waiter = Arc::new(Waiter {
        state: AtomicU8::new(ARMING),
        kind: Kind::String,
        buf: Box::default(),
        ranges: Box::default(),
        keys: 0..0,
        attempt: never,
        client: session.id,
        resp3: session.resp3,
        db: session.db,
        socket: -1,
        reply: Mutex::new(Vec::new()),
        waker: Mutex::new(None),
        on_change: None,
        timed_out: Some(reply),
    });
    let store = ctx.store.db(session.db as usize);
    store.count_blocked(1);
    store.track_waiter(&waiter);
    waiter.arm();
    Reply::Block(Box::new(Blocked {
        waiter,
        store: store.clone(),
        timeout,
    }))
}

/// `CLIENT UNBLOCK`: ends client `id`'s blocking command as if it had timed out, or
/// with an error. Whether it was blocked in one.
pub(crate) fn unblock(ctx: &Context, id: u64, error: bool) -> bool {
    let Some(waiter) = ctx.store.waiter_of(id) else {
        return false;
    };
    if !waiter.claim() {
        return false;
    }
    let mut claimed = Claimed {
        waiter,
        holds: true,
    };
    let reply: Vec<u8> = if error {
        b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n".to_vec()
    } else {
        claimed.waiter.timed_out_reply().to_vec()
    };
    claimed.serve(reply);
    true
}

/// Serves the clients waiting on keys that got data. Commands call this
/// after running when [`Store::has_ready`].
pub(crate) fn serve_ready(ctx: &Context, now: Millis) {
    while let Some((db, key)) = ctx.store.take_ready() {
        let store = ctx.store.db(db);
        let key = store.key(&key);
        // Stream readers all read what came: one that found nothing new
        // doesn't mean the next won't.
        let mut tried = Vec::new();
        while let Some(waiter) = store.claim_waiter(key, now, &tried) {
            let mut claimed = Claimed {
                waiter,
                holds: true,
            };
            let served = loop {
                let mut out = Vec::new();
                match claimed.waiter.run_again(ctx, now, &mut out) {
                    Attempt::Done => {
                        claimed.waiter.log(&out);
                        let rest = claimed.waiter.send_now(&out).to_vec();
                        claimed.serve(rest);
                        break true;
                    }
                    // Another client took the data first.
                    Attempt::Wait { .. } => {
                        if claimed.unclaim() {
                            break false;
                        }
                    }
                }
            };
            if !served {
                if claimed.waiter.kind() == Kind::Stream {
                    tried.push(claimed.waiter.clone());
                    continue;
                }
                break; // the key is empty again
            }
        }
    }
}

/// A blocked command, which its connection waits on. Dropping it stops the
/// wait: the command ends without a reply.
pub struct Blocked {
    waiter: Arc<Waiter>,
    store: Arc<Store>,
    timeout: Option<Duration>,
}

impl Blocked {
    /// How long it waits; `None` for as long as it takes.
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    /// Resolves once it's served. A monoio connection, which can only be
    /// woken from its own thread, passes that thread's mailbox.
    pub fn served<'a>(&'a self, home: Option<&'a Arc<Mailbox>>) -> impl Future<Output = ()> + 'a {
        let waiter = &*self.waiter;
        std::future::poll_fn(move |cx| {
            if waiter.state.load(Ordering::Acquire) == SERVED {
                return Poll::Ready(());
            }
            *waiter.waker.lock() = Some(Wake::new(cx, home));
            // Served before the waker was in place?
            if waiter.state.load(Ordering::Acquire) == SERVED {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
    }

    /// Ends the wait, served or timed out: writes its reply.
    pub fn finish(self, out: &mut Vec<u8>) {
        if self.waiter.give_up() {
            // Timed out: a null array, whatever the command.
            out.extend_from_slice(self.waiter.timed_out_reply());
        } else {
            out.extend_from_slice(&self.waiter.reply.lock());
        }
    }
}

impl Drop for Blocked {
    fn drop(&mut self) {
        self.waiter.give_up();
        for key in self.waiter.keys() {
            self.store.stop_waiting(self.store.key(key), &self.waiter);
            if let Some(flag) = &self.waiter.on_change {
                self.store.unwatch(self.store.key(key), flag);
            }
        }
        self.store.count_blocked(-1);
        self.store.untrack_waiter(self.waiter.client);
    }
}

/// Redis' blocking timeout: seconds, fractions allowed, 0 for none.
pub(crate) fn parse_timeout(arg: &[u8]) -> Result<Option<Duration>, &'static [u8]> {
    const NOT_A_FLOAT: &[u8] = b"ERR timeout is not a float or out of range";
    const OUT_OF_RANGE: &[u8] = b"ERR timeout is out of range";
    // Read as Redis' `string2ld` does, with a long double: one too small
    // for a double is still a little more than 0.
    if arg.is_empty() || arg.len() >= 5 * 1024 {
        return Err(NOT_A_FLOAT);
    }
    let Some((value, underflow)) = crate::zset::parse_timeout_seconds(arg) else {
        return Err(NOT_A_FLOAT);
    };
    let ms = value * 1000.0;
    if ms > i64::MAX as f64 {
        return Err(OUT_OF_RANGE);
    }
    let ms = if underflow && value == 0.0 && arg[0] != b'-' {
        1
    } else {
        ms.ceil() as i64
    };
    if ms < 0 {
        return Err(b"ERR timeout is negative");
    }
    if ms == 0 {
        return Ok(None);
    }
    let unix_now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64);
    if ms > i64::MAX - unix_now {
        return Err(OUT_OF_RANGE);
    }
    Ok(Some(Duration::from_millis(ms as u64)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn waiter() -> Waiter {
        Waiter {
            state: AtomicU8::new(ARMING),
            kind: Kind::List,
            timed_out: None,
            db: 0,
            buf: Box::new([]),
            ranges: Box::new([]),
            keys: 0..0,
            attempt: |_, _, _, _, _| Attempt::Done,
            client: 1,
            resp3: false,
            socket: -1,
            reply: Mutex::new(Vec::new()),
            waker: Mutex::new(None),
            on_change: None,
        }
    }

    /// A writer gives a waiter back (its data was taken), and another
    /// claims it before the first's guard goes: the guard leaves the second
    /// claim alone. (It used to serve an internal error over it, and the
    /// data the second writer then popped was lost.)
    #[test]
    fn a_claim_given_back_is_not_served_by_its_guard() {
        let w = Arc::new(waiter());
        assert!(w.arm());
        assert!(w.claim());
        let mut first = Claimed {
            waiter: w.clone(),
            holds: true,
        };
        assert!(first.unclaim());
        assert!(w.claim(), "a second writer claims it");
        drop(first);
        assert_eq!(w.state.load(Ordering::Acquire), SERVING);
        assert!(w.reply.lock().is_empty());
        // And a claim that panics mid-run is still served an error.
        drop(Claimed {
            waiter: w.clone(),
            holds: true,
        });
        assert_eq!(w.state.load(Ordering::Acquire), SERVED);
        assert!(w.reply.lock().starts_with(b"-ERR internal error"));
    }

    #[test]
    fn a_key_getting_data_at_the_wrong_moment_is_never_missed() {
        let w = waiter();
        // While its command registers, a writer can't serve it but marks it,
        // and the command tries its keys again.
        assert!(!w.claim());
        assert!(!w.arm());
        assert!(w.arm());
        // While one writer runs it, another key's writer marks it, and if
        // that run found nothing, it runs again.
        assert!(w.claim());
        assert!(!w.claim());
        assert!(!w.unclaim());
        assert!(w.unclaim());
        // Served, it stays served.
        assert!(w.claim());
        w.serve(b"+OK\r\n".to_vec());
        assert!(!w.claim());
        assert!(!w.give_up());
        // Given up, writers skip it.
        let w = waiter();
        assert!(w.arm());
        assert!(w.give_up());
        assert!(!w.claim());
    }

    #[test]
    fn timeouts_parse_as_redis_does() {
        let ms = |s: &str| parse_timeout(s.as_bytes()).map(|t| t.map(|t| t.as_millis()));
        assert_eq!(ms("0"), Ok(None));
        assert_eq!(ms("1"), Ok(Some(1000)));
        assert_eq!(ms("0.0001"), Ok(Some(1)));
        assert_eq!(ms("1.5"), Ok(Some(1500)));
        assert_eq!(ms("1e-400"), Ok(Some(1)));
        // Rounds up to -0: forever.
        assert_eq!(ms("-0.0001"), Ok(None));
        assert_eq!(ms("-1"), Err(&b"ERR timeout is negative"[..]));
        assert_eq!(ms("-inf"), Err(&b"ERR timeout is negative"[..]));
        assert_eq!(ms("inf"), Err(&b"ERR timeout is out of range"[..]));
        assert_eq!(ms("1e300"), Err(&b"ERR timeout is out of range"[..]));
        for bad in ["", " 1", "1 ", "abc", "nan", "1x"] {
            assert_eq!(
                ms(bad),
                Err(&b"ERR timeout is not a float or out of range"[..]),
                "{bad:?}"
            );
        }
    }
}
