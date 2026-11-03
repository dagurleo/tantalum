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
//!   the command that added it and before that client's next one. Each reply
//!   goes into its waiter, whose connection is woken to send it.
//! - The connection waits for that, for its timeout, or for the client to
//!   go away. Commands the client pipelines meanwhile wait their turn.
//!
//! Unlike in Redis, other clients' commands run in parallel on other
//! threads, so one of them may take the data between the write and the
//! serving. The waiter then just goes on waiting.
//!
//! None of this costs commands anything while nothing is blocked: writes
//! check for waiters only when they create a key and the key's shard has
//! some, and commands check one flag for ready keys.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::task::Poll;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use crate::cmd::{Args, Reply};
use crate::context::{Context, Session};
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
    Wait {
        keys: Range<usize>,
        timeout: Option<Duration>,
        kind: Kind,
        nil_bulk: bool,
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
    /// Its reply, once served.
    reply: Mutex<Vec<u8>>,
    /// Wakes its connection while it waits.
    waker: Mutex<Option<Wake>>,
}

impl Waiter {
    pub(crate) fn kind(&self) -> Kind {
        self.kind
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
            name: None,
            resp3: self.resp3,
            commands: 0,
            hits: 0,
            misses: 0,
        };
        let args = Args::new(&self.buf, &self.ranges);
        (self.attempt)(ctx, &mut session, now, &args, out)
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
struct Claimed(Arc<Waiter>);

impl Drop for Claimed {
    fn drop(&mut self) {
        if matches!(
            self.0.state.load(Ordering::Acquire),
            SERVING | SERVING_AGAIN
        ) {
            self.0
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
    for i in 0..args.len() {
        let arg = args.get(i);
        ranges.push(buf.len()..buf.len() + arg.len());
        buf.extend_from_slice(arg);
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
        reply: Mutex::new(Vec::new()),
        waker: Mutex::new(None),
    });
    let store = &ctx.store;
    let blocked = Blocked {
        waiter,
        store: store.clone(),
        timeout,
    };
    for key in blocked.waiter.keys() {
        store.wait_on(store.key(key), &blocked.waiter);
    }
    store.count_blocked(1);
    // Again, now that a write creating a key would see it.
    loop {
        if let Attempt::Done = attempt(ctx, session, now, args, out) {
            return Reply::Written; // `blocked` deregisters as it drops
        }
        if blocked.waiter.arm() {
            return Reply::Block(Box::new(blocked));
        }
    }
}

/// Serves the clients waiting on keys that got data. Commands call this
/// after running when [`Store::has_ready`].
pub(crate) fn serve_ready(ctx: &Context, now: Millis) {
    let store = &*ctx.store;
    while let Some(key) = store.take_ready() {
        let key = store.key(&key);
        while let Some(waiter) = store.claim_waiter(key, now) {
            let claimed = Claimed(waiter);
            let served = loop {
                let mut out = Vec::new();
                match claimed.0.run_again(ctx, now, &mut out) {
                    Attempt::Done => {
                        claimed.0.serve(out);
                        break true;
                    }
                    // Another client took the data first.
                    Attempt::Wait { .. } => {
                        if claimed.0.unclaim() {
                            break false;
                        }
                    }
                }
            };
            if !served {
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
            out.extend_from_slice(if self.waiter.resp3 {
                resp::NULL
            } else {
                b"*-1\r\n"
            });
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
        }
        self.store.count_blocked(-1);
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
            buf: Box::new([]),
            ranges: Box::new([]),
            keys: 0..0,
            attempt: |_, _, _, _, _| Attempt::Done,
            client: 1,
            resp3: false,
            reply: Mutex::new(Vec::new()),
            waker: Mutex::new(None),
        }
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
