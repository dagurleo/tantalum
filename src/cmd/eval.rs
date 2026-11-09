//! Scripts: `EVAL`, `EVALSHA`, `EVAL_RO`, `EVALSHA_RO` and `SCRIPT` (the
//! interpreter is `src/script.rs`), and running a script's `redis.call`.

use std::borrow::Cow;

use super::{Args, Reply, find_command, unknown_subcommand, wrong_arity};
use crate::context::{Context, Session};
use crate::resp;
use crate::store::Millis;

pub(super) fn eval(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    run(ctx, session, now, args, out, false, false)
}

pub(super) fn eval_ro(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    run(ctx, session, now, args, out, false, true)
}

pub(super) fn evalsha(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    run(ctx, session, now, args, out, true, false)
}

pub(super) fn evalsha_ro(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    run(ctx, session, now, args, out, true, true)
}

/// Checks the arguments in Redis' order, then runs the script as a transaction
/// (a native one, holding the shards it uses), so nothing else touches what it
/// touches meanwhile.
fn run(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    sha: bool,
    read_only: bool,
) -> Reply {
    if sha && args.get(1).len() != 40 {
        resp::write_error(out, b"NOSCRIPT No matching script. Please use EVAL.");
        return Reply::Written;
    }
    let Some(numkeys) = resp::parse_int(args.get(2)) else {
        resp::write_error(out, b"ERR value is not an integer or out of range");
        return Reply::Written;
    };
    if numkeys > (args.len() - 3) as i64 {
        resp::write_error(
            out,
            b"ERR Number of keys can't be greater than number of args",
        );
        return Reply::Written;
    }
    if numkeys < 0 {
        resp::write_error(out, b"ERR Number of keys can't be negative");
        return Reply::Written;
    }
    // BullMQ's scripts natively, holding only the shards they use (but
    // not inside EXEC, whose transaction holds them).
    // (Not while something wants to hear of what scripts run.)
    let watched = session.special || ctx.monitoring();
    // A user that may not run everything has each of its commands checked,
    // which only Lua does.
    let checked = session
        .user
        .as_ref()
        .is_some_and(|u| !u.may_run_everything());
    let native = sha
        && !read_only
        && ctx.settings.native_scripts
        && !watched
        && !checked
        // A tracking client's reads are its script's calls: Lua makes them.
        && !session.tracking.as_ref().is_some_and(|t| t.on)
        && !crate::shard_lock::in_transaction();
    if native
        && ctx
            .scripting
            .run_native(ctx, session.db, now, args, numkeys as usize, out)
    {
        return Reply::Written;
    }
    ctx.store.exclusive(|| {
        ctx.scripting.eval(
            ctx,
            session,
            !native && !watched && !checked,
            now,
            args,
            numkeys as usize,
            sha,
            read_only,
            out,
        )
    });
    // Its `SELECT`s were its own, as in Redis.
    crate::aof::select(session.db as usize);
    Reply::Written
}

/// `SCRIPT LOAD | EXISTS | FLUSH | KILL | HELP` (and `DEBUG NO`; there's no
/// debugger).
pub(super) fn script(
    ctx: &Context,
    _: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let sub = args.get(1);
    let is = |name: &[u8]| sub.eq_ignore_ascii_case(name);
    // Each subcommand's arity, as Redis checks it before the subcommand
    // runs.
    let arity = |ok: bool, name: &str, out: &mut Vec<u8>| {
        if !ok {
            wrong_arity(out, &format!("script|{name}"));
        }
        ok
    };
    let n = args.len();
    if is(b"LOAD") {
        if arity(n == 3, "load", out) {
            ctx.scripting.load(args.get(2), out);
        }
    } else if is(b"EXISTS") {
        if arity(n >= 3, "exists", out) {
            resp::write_array_header(out, n - 2);
            for i in 2..n {
                resp::write_integer(out, i64::from(ctx.scripting.exists(args.get(i))));
            }
        }
    } else if is(b"FLUSH") {
        if n == 2 || (n == 3 && is_any(args.get(2), &[b"SYNC", b"ASYNC"])) {
            ctx.scripting.flush();
            out.extend_from_slice(resp::OK);
        } else {
            resp::write_error(out, b"ERR SCRIPT FLUSH only support SYNC|ASYNC option");
        }
    } else if is(b"KILL") {
        if arity(n == 2, "kill", out) {
            ctx.scripting.kill(out, false);
        }
    } else if is(b"DEBUG") {
        if arity(n == 3, "debug", out) {
            let mode = args.get(2);
            if mode.eq_ignore_ascii_case(b"NO") {
                out.extend_from_slice(resp::OK);
            } else if is_any(mode, &[b"YES", b"SYNC"]) {
                resp::write_error(out, b"ERR The Lua debugger is not supported");
            } else {
                resp::write_error(out, b"ERR Use SCRIPT DEBUG YES/SYNC/NO");
            }
        }
    } else if is(b"HELP") {
        if arity(n == 2, "help", out) {
            write_help(out);
        }
    } else {
        unknown_subcommand(out, sub, "SCRIPT");
    }
    Reply::Written
}

fn is_any(arg: &[u8], names: &[&[u8]]) -> bool {
    names.iter().any(|name| arg.eq_ignore_ascii_case(name))
}

fn write_help(out: &mut Vec<u8>) {
    const HELP: &[&[u8]] = &[
        b"SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        b"DEBUG (YES|SYNC|NO)",
        b"    Set the debug mode for subsequent scripts executed.",
        b"EXISTS <sha1> [<sha1> ...]",
        b"    Return information about the existence of the scripts in the script cache.",
        b"FLUSH [ASYNC|SYNC]",
        b"    Flush the Lua scripts cache. Very dangerous on replicas.",
        b"    When called without the optional mode argument, the behavior is determined by the",
        b"    lazyfree-lazy-user-flush configuration directive. Valid modes are:",
        b"    * ASYNC: Asynchronously flush the scripts cache.",
        b"    * SYNC: Synchronously flush the scripts cache.",
        b"KILL",
        b"    Kill the currently executing Lua script.",
        b"LOAD <script>",
        b"    Load a script into the scripts cache without executing it.",
        b"HELP",
        b"    Print this help.",
    ];
    resp::write_array_header(out, HELP.len());
    for line in HELP {
        resp::write_simple(out, line);
    }
}

/// Whether a command (or its subcommand) may not run from a script: what
/// Redis flags `noscript`, for the container commands here as well.
fn refused_in_script(name: &str, flags: &[&str], args: &Args<'_>) -> bool {
    if flags.contains(&"noscript") {
        return true;
    }
    let sub = |names: &[&[u8]]| args.len() > 1 && is_any(args.get(1), names);
    match name {
        // Every subcommand but HELP.
        "client" | "script" | "function" => !sub(&[b"HELP"]),
        "config" => sub(&[b"GET", b"SET", b"RESETSTAT", b"REWRITE"]),
        _ => false,
    }
}

/// Runs one of a script's `redis.call`s (Redis' `scriptCall`) with its
/// reply in `out`, returning whether it was a write command, or the error
/// Lua raises instead.
#[allow(clippy::too_many_arguments)]
pub(crate) fn call_from_script(
    ctx: &Context,
    session: &mut Session,
    caller: (u64, &str),
    now: Millis,
    args: &Args<'_>,
    read_only: bool,
    out: &mut Vec<u8>,
) -> Result<bool, Cow<'static, str>> {
    let Some(command) = find_command(args.get(0)) else {
        return Err("Unknown Redis command called from script".into());
    };
    if !command.arity_allows(args.len()) {
        return Err("Wrong number of args calling Redis command from script".into());
    }
    if refused_in_script(command.name, command.flags, args) {
        return Err("This Redis command is not allowed from script".into());
    }
    if session.user.is_some()
        && let Some(e) = super::acl::refused_in_script(ctx, session, caller, command, args)
    {
        return Err(e.into());
    }
    let write = command.flags.contains(&"write");
    // Commands Redis flags `may_replicate` (a flag `COMMAND` doesn't show).
    let may_replicate = matches!(command.name, "publish" | "spublish" | "pfcount");
    if read_only && (write || may_replicate) {
        return Err("Write commands are not allowed from read-only scripts.".into());
    }
    // The log can't be written: the write fails as a command's error would.
    if write && let Some(aof) = ctx.aof().filter(|aof| aof.failing()) {
        resp::write_error(out, aof.failure().as_bytes());
        return Ok(false);
    }
    session.commands += 1;
    if session.reads_for.is_some() {
        crate::tracking::before(ctx, session, command.name, args, true);
    }
    let start = out.len();
    let reply = if session.special {
        super::watched_inside(ctx, session, now, command, args, out)
    } else {
        super::timed(ctx, session, now, command, args, out)
    };
    if let Reply::Bulk(value) = reply {
        resp::write_bulk(out, value.as_bytes());
    }
    if write {
        crate::aof::record(command.name, args, &out[start..]);
    }
    Ok(write)
}
