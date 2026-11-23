//! `FAILOVER`: a master hands over to one of its replicas without losing a
//! write, as Redis 6.2's does. Writes are held back (as `CLIENT PAUSE WRITE`
//! holds them, and nothing expires by itself meanwhile, nor are pings sent,
//! so the stream stands still); once a replica has acknowledged all of it
//! (the one named, or the first to), this server follows it, asking to
//! continue with `PSYNC <id> <offset> FAILOVER`, which tells the replica to
//! take over first. When it answers, writes go on: refused here, as on any
//! replica. `FAILOVER ABORT`, or a timeout without `FORCE`, ends it, and
//! this server stays (or is again) a master.

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use crate::cmd::Args;
use crate::context::Context;
use crate::resp;

/// Where a failover is (`INFO`'s `master_failover_state`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum State {
    #[default]
    None,
    /// Writes held, waiting for a replica to have all of the stream.
    WaitingForSync,
    /// Following the replica, asking it to take over.
    InProgress,
}

impl State {
    pub(crate) fn name(self) -> &'static str {
        match self {
            State::None => "no-failover",
            State::WaitingForSync => "waiting-for-sync",
            State::InProgress => "failover-in-progress",
        }
    }
}

#[derive(Default)]
pub(crate) struct Failover {
    pub(crate) state: State,
    /// The replica to hand over to, once known.
    target: Option<(String, u16)>,
    /// When to stop waiting (`TIMEOUT`).
    end: Option<Instant>,
    force: bool,
}

/// `FAILOVER [TO host port [FORCE]] [ABORT] [TIMEOUT ms]`, with Redis'
/// replies.
pub(crate) fn command(ctx: &Context, args: &Args<'_>, out: &mut Vec<u8>) {
    if args.len() == 2 && args.get(1).eq_ignore_ascii_case(b"ABORT") {
        if ctx.repl.failover.lock().state == State::None {
            resp::write_error(out, b"ERR No failover in progress.");
            return;
        }
        abort(ctx, "Failover manually aborted");
        out.extend_from_slice(resp::OK);
        return;
    }
    let (mut timeout, mut force, mut to) = (0i64, false, None);
    let mut j = 1;
    while j < args.len() {
        let word = args.get(j);
        if word.eq_ignore_ascii_case(b"TIMEOUT") && j + 1 < args.len() && timeout == 0 {
            let Some(n) = resp::parse_int(args.get(j + 1)) else {
                resp::write_error(out, crate::cmd::NOT_AN_INTEGER);
                return;
            };
            if n <= 0 {
                resp::write_error(out, b"ERR FAILOVER timeout must be greater than 0");
                return;
            }
            timeout = n;
            j += 2;
        } else if word.eq_ignore_ascii_case(b"TO") && j + 2 < args.len() && to.is_none() {
            let Some(port) = resp::parse_int(args.get(j + 2)) else {
                resp::write_error(out, crate::cmd::NOT_AN_INTEGER);
                return;
            };
            to = Some((String::from_utf8_lossy(args.get(j + 1)).into_owned(), port));
            j += 3;
        } else if word.eq_ignore_ascii_case(b"FORCE") && !force {
            force = true;
            j += 1;
        } else {
            resp::write_error(out, crate::cmd::SYNTAX_ERROR);
            return;
        }
    }
    let mut failover = ctx.repl.failover.lock();
    if failover.state != State::None {
        resp::write_error(out, b"ERR FAILOVER already in progress.");
        return;
    }
    if ctx.repl.following() {
        resp::write_error(out, b"ERR FAILOVER is not valid when server is a replica.");
        return;
    }
    let master = ctx.repl.master();
    let followers = master.as_ref().map(|m| m.followers()).unwrap_or_default();
    if followers.is_empty() {
        resp::write_error(out, b"ERR FAILOVER requires connected replicas.");
        return;
    }
    if force && (timeout == 0 || to.is_none()) {
        resp::write_error(
            out,
            b"ERR FAILOVER with force option requires both a timeout and target HOST and IP.",
        );
        return;
    }
    let target = match to {
        Some((host, port)) => {
            let Some(replica) = followers
                .iter()
                .find(|f| f.ip.eq_ignore_ascii_case(&host) && i64::from(f.port) == port)
            else {
                resp::write_error(out, b"ERR FAILOVER target HOST and PORT is not a replica.");
                return;
            };
            if replica.state.load(Relaxed) != super::master::ONLINE {
                resp::write_error(out, b"ERR FAILOVER target replica is not online.");
                return;
            }
            eprintln!("FAILOVER requested to {host}:{port}.");
            Some((host, replica.port))
        }
        None => {
            eprintln!("FAILOVER requested to any replica.");
            None
        }
    };
    let Some(arc) = ctx.arc() else {
        resp::write_error(out, b"ERR replication needs a running server");
        return;
    };
    *failover = Failover {
        state: State::WaitingForSync,
        target,
        end: (timeout > 0).then(|| Instant::now() + Duration::from_millis(timeout as u64)),
        force,
    };
    drop(failover);
    ctx.pause_for_failover(true);
    out.extend_from_slice(resp::OK);
    let _ = std::thread::Builder::new()
        .name("tantalum-failover".into())
        .spawn(move || wait_for_sync(&arc));
}

/// Waits for a replica to have all of the stream (or the time to run out),
/// then follows it.
fn wait_for_sync(ctx: &Arc<Context>) {
    loop {
        std::thread::sleep(Duration::from_millis(10));
        let mut failover = ctx.repl.failover.lock();
        if failover.state != State::WaitingForSync || ctx.is_shutting_down() {
            return;
        }
        if failover.end.is_some_and(|end| Instant::now() >= end) {
            if failover.force {
                let (host, port) = failover.target.clone().expect("FORCE needs a target");
                eprintln!("FAILOVER to {host}:{port} time out exceeded, failing over.");
                failover.state = State::InProgress;
                drop(failover);
                super::replica::follow(ctx, host, port);
            } else {
                drop(failover);
                abort(ctx, "Replica never caught up before timeout");
            }
            return;
        }
        let Some(master) = ctx.repl.master() else {
            drop(failover);
            abort(ctx, "No replicas");
            return;
        };
        let offset = master.backlog.offset();
        let followers = master.followers();
        // Has it all: said so (not just been sent it).
        let has_all =
            |f: &super::master::Follower| f.acked.load(Relaxed) && f.ack.load(Relaxed) == offset;
        let synced = match &failover.target {
            Some((host, port)) => followers
                .iter()
                .any(|f| f.ip.eq_ignore_ascii_case(host) && f.port == *port && has_all(f)),
            None => match followers.iter().find(|f| has_all(f)) {
                Some(f) => {
                    failover.target = Some((f.ip.clone(), f.port));
                    true
                }
                None => false,
            },
        };
        if synced {
            let (host, port) = failover.target.clone().expect("a target by now");
            eprintln!("Failover target {host}:{port} is synced, failing over.");
            failover.state = State::InProgress;
            drop(failover);
            super::replica::follow(ctx, host, port);
            return;
        }
    }
}

/// The replica said it took over (or didn't): writes go on, or the failover
/// is over and this server a master again.
pub(crate) fn answered(ctx: &Context, accepted: bool) {
    if accepted {
        clear(ctx);
    } else {
        abort(ctx, "Failover target rejected psync request");
    }
}

/// Ends a failover that hasn't worked: a master again, writes going on.
pub(crate) fn abort(ctx: &Context, why: &str) {
    let mut failover = ctx.repl.failover.lock();
    if failover.state == State::None {
        return;
    }
    match &failover.target {
        Some((host, port)) => eprintln!("FAILOVER to {host}:{port} aborted: {why}"),
        None => eprintln!("FAILOVER to any replica aborted: {why}"),
    }
    let following = failover.state == State::InProgress;
    *failover = Failover::default();
    drop(failover);
    if following {
        super::replica::promote(ctx);
    }
    ctx.pause_for_failover(false);
}

fn clear(ctx: &Context) {
    *ctx.repl.failover.lock() = Failover::default();
    ctx.pause_for_failover(false);
}

/// Whether this server, failing over, follows the replica it hands over to:
/// its `PSYNC` asks that replica to take over.
pub(crate) fn in_progress(ctx: &Context) -> bool {
    ctx.repl.failover.lock().state == State::InProgress
}
