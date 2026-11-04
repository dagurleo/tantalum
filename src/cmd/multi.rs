//! Transactions: `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` (see
//! `src/transaction.rs`).

use super::{Args, Reply, find_command};
use crate::context::{Context, Session};
use crate::resp;
use crate::store::Millis;
use crate::transaction::{Queue, Watching};

/// What `MULTI` doesn't queue: these run at once.
pub(super) fn runs_in_multi(command: &str) -> bool {
    matches!(
        command,
        "exec" | "discard" | "multi" | "watch" | "quit" | "reset"
    )
}

/// Commands Redis refuses inside a transaction.
pub(super) fn refused_in_multi(command: &str) -> bool {
    command == "save"
}

fn reply_error(out: &mut Vec<u8>, msg: &[u8]) -> Reply {
    resp::write_error(out, msg);
    Reply::Written
}

pub(super) fn multi(
    _: &Context,
    session: &mut Session,
    _: Millis,
    _: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    if session.multi.is_some() {
        return reply_error(out, b"ERR MULTI calls can not be nested");
    }
    session.multi = Some(Box::<Queue>::default());
    session.refresh();
    out.extend_from_slice(resp::OK);
    Reply::Written
}

pub(super) fn discard(
    _: &Context,
    session: &mut Session,
    _: Millis,
    _: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    if session.multi.take().is_none() {
        return reply_error(out, b"ERR DISCARD without MULTI");
    }
    session.watching = None;
    session.refresh();
    out.extend_from_slice(resp::OK);
    Reply::Written
}

pub(super) fn watch(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    if session.multi.is_some() {
        return reply_error(out, b"ERR WATCH inside MULTI is not allowed");
    }
    let watching = session
        .watching
        .get_or_insert_with(|| Box::new(Watching::new(&ctx.store)));
    for i in 1..args.len() {
        watching.add(args.get(i), now);
    }
    out.extend_from_slice(resp::OK);
    Reply::Written
}

pub(super) fn unwatch(
    _: &Context,
    session: &mut Session,
    _: Millis,
    _: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    session.watching = None;
    out.extend_from_slice(resp::OK);
    Reply::Written
}

/// `EXEC`: runs the queued commands with the whole store to itself, unless
/// a watched key changed (then nil), replying all their replies.
pub(super) fn exec(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    _: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let Some(queue) = session.multi.take() else {
        return reply_error(out, b"ERR EXEC without MULTI");
    };
    session.refresh();
    let watching = session.watching.take();
    if queue.refused {
        return reply_error(
            out,
            b"EXECABORT Transaction discarded because of previous errors.",
        );
    }
    ctx.store.exclusive(|| {
        if watching.as_ref().is_some_and(|w| w.changed(now)) {
            out.extend_from_slice(if session.resp3 {
                resp::NULL
            } else {
                b"*-1\r\n"
            });
            return;
        }
        drop(watching);
        resp::write_array_header(out, queue.len());
        session.deny_blocking = true;
        for i in 0..queue.len() {
            let args = queue.command(i);
            let command = find_command(args.get(0)).expect("checked when queued");
            let start = out.len();
            match (command.run)(ctx, session, now, &args, out) {
                Reply::Bulk(value) => resp::write_bulk(out, value.as_bytes()),
                Reply::Written | Reply::Close | Reply::Block(_) => {}
            }
            if command.writes() {
                crate::aof::record(command.name, &args, &out[start..]);
            }
        }
        session.deny_blocking = false;
    });
    Reply::Written
}
