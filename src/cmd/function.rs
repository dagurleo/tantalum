//! `FUNCTION` and `FCALL`, `FCALL_RO` (the libraries and their Lua are in
//! `src/script/functions.rs`), with Redis 7.2.4's replies.

use super::{Args, Reply, subcommand_syntax_error, unknown_subcommand, wrong_arity};
use crate::context::{Context, Session};
use crate::resp;
use crate::script::functions::{FLAG_NAMES, Policy};
use crate::store::{EvictionPolicy, Millis};

/// How long a library's top level may run when loaded by a client (Redis'
/// `LOAD_TIMEOUT_MS`); none for one the log loads again.
const LOAD_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

const OOM: &[u8] = b"OOM command not allowed when used memory > 'maxmemory'.";

pub(super) fn fcall(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    run(ctx, session, now, args, out, false)
}

pub(super) fn fcall_ro(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    run(ctx, session, now, args, out, true)
}

/// Checks the arguments in Redis' order (the function first, then the keys),
/// then runs it as a transaction, as `EVAL` runs a script.
fn run(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    read_only: bool,
) -> Reply {
    {
        let mut functions = ctx.scripting.functions.lock();
        functions.follow(&ctx.store);
        if functions.find(args.get(1)).is_none() {
            resp::write_error(out, b"ERR Function not found");
            return Reply::Written;
        }
    }
    let Some(numkeys) = resp::parse_int(args.get(2)) else {
        resp::write_error(out, b"ERR Bad number of keys provided");
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
    ctx.store.exclusive(|| {
        ctx.scripting
            .fcall(ctx, session, now, args, numkeys as usize, read_only, out)
    });
    // Its `SELECT`s were its own, as in Redis.
    crate::aof::select(session.db as usize);
    Reply::Written
}

/// Whether `FUNCTION` with this subcommand changes the libraries, and so
/// takes a turn to be logged.
pub(super) fn writes(args: &Args<'_>) -> bool {
    args.len() > 1
        && [&b"LOAD"[..], b"DELETE", b"FLUSH", b"RESTORE"]
            .iter()
            .any(|sub| args.get(1).eq_ignore_ascii_case(sub))
}

/// What a write that worked logs: itself, run again at startup.
fn logged(args: &Args<'_>) {
    crate::aof::record_parts((0..args.len()).map(|i| args.get(i)));
}

/// No room under `maxmemory` for `incoming` more bytes, with nothing to evict:
/// what Redis' `denyoom` refuses (as a write here is refused that doesn't fit).
fn full(ctx: &Context, incoming: usize) -> bool {
    let store = &ctx.store;
    store.maxmemory() > 0
        && store.used_memory() + incoming > store.maxmemory()
        && store.policy() == EvictionPolicy::NoEviction
}

/// `FUNCTION LOAD | DELETE | FLUSH | LIST | DUMP | RESTORE | STATS | KILL | HELP`.
pub(super) fn function(
    ctx: &Context,
    session: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let sub = args.get(1);
    let is = |name: &[u8]| sub.eq_ignore_ascii_case(name);
    let argc = args.len();
    let arity: Option<(&str, i64)> = [
        ("delete", 3),
        ("dump", 2),
        ("flush", -2),
        ("help", 2),
        ("kill", 2),
        ("list", -2),
        ("load", -3),
        ("restore", -3),
        ("stats", 2),
    ]
    .into_iter()
    .find(|(name, _)| is(name.as_bytes()));
    let Some((name, arity)) = arity else {
        unknown_subcommand(out, sub, "FUNCTION");
        return Reply::Written;
    };
    let n = argc as i64;
    if (arity > 0 && n != arity) || n < -arity {
        wrong_arity(out, &format!("function|{name}"));
        return Reply::Written;
    }
    let error = |out: &mut Vec<u8>, e: &[u8]| {
        let mut msg = b"ERR ".to_vec();
        msg.extend_from_slice(e);
        resp::write_error(out, &msg);
    };
    match name {
        "load" => {
            if full(ctx, args.get(argc - 1).len()) {
                resp::write_error(out, OOM);
                return Reply::Written;
            }
            let mut replace = false;
            let mut at = 2;
            while at < argc - 1 {
                let option = args.get(at);
                at += 1;
                if option.eq_ignore_ascii_case(b"REPLACE") {
                    replace = true;
                    continue;
                }
                let mut e = b"Unknown option given: ".to_vec();
                e.extend_from_slice(option);
                error(out, &e);
                return Reply::Written;
            }
            // The log's own commands load whatever they take.
            let timeout = (session.id != 0).then_some(LOAD_TIMEOUT);
            let mut functions = ctx.scripting.functions.lock();
            match functions.load(&ctx.store, args.get(at), replace, timeout) {
                Ok(name) => {
                    ctx.scripting.count_functions(&functions);
                    resp::write_bulk(out, &name);
                    logged(args);
                }
                Err(e) => error(out, &e),
            }
        }
        "delete" => {
            let mut functions = ctx.scripting.functions.lock();
            if functions.delete(&ctx.store, args.get(2)) {
                ctx.scripting.count_functions(&functions);
                out.extend_from_slice(resp::OK);
                logged(args);
            } else {
                error(out, b"Library not found");
            }
        }
        "flush" => {
            if argc > 3 {
                subcommand_syntax_error(out, sub, "FUNCTION");
                return Reply::Written;
            }
            if argc == 3
                && !(args.get(2).eq_ignore_ascii_case(b"SYNC")
                    || args.get(2).eq_ignore_ascii_case(b"ASYNC"))
            {
                error(out, b"FUNCTION FLUSH only supports SYNC|ASYNC option");
                return Reply::Written;
            }
            let mut functions = ctx.scripting.functions.lock();
            functions.flush(&ctx.store);
            ctx.scripting.count_functions(&functions);
            out.extend_from_slice(resp::OK);
            logged(args);
        }
        "list" => list(ctx, session, args, out),
        "dump" => {
            let payload = ctx.scripting.functions.lock().dump(&ctx.store);
            resp::write_bulk(out, &payload);
        }
        "restore" => {
            if argc > 4 {
                subcommand_syntax_error(out, sub, "FUNCTION");
                return Reply::Written;
            }
            if full(ctx, args.get(2).len()) {
                resp::write_error(out, OOM);
                return Reply::Written;
            }
            let policy = if argc == 4 {
                let given = args.get(3);
                if given.eq_ignore_ascii_case(b"APPEND") {
                    Policy::Append
                } else if given.eq_ignore_ascii_case(b"REPLACE") {
                    Policy::Replace
                } else if given.eq_ignore_ascii_case(b"FLUSH") {
                    Policy::Flush
                } else {
                    error(
                        out,
                        b"Wrong restore policy given, value should be either FLUSH, APPEND or REPLACE.",
                    );
                    return Reply::Written;
                }
            } else {
                Policy::Append
            };
            let mut functions = ctx.scripting.functions.lock();
            match functions.restore(&ctx.store, args.get(2), policy) {
                Ok(()) => {
                    ctx.scripting.count_functions(&functions);
                    out.extend_from_slice(resp::OK);
                    logged(args);
                }
                Err(e) => error(out, &e),
            }
        }
        "stats" => ctx.scripting.function_stats(out, session.resp3),
        "kill" => ctx.scripting.kill(out, true),
        _ => super::admin::help(
            out,
            &[
                "FUNCTION <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "LOAD [REPLACE] <FUNCTION CODE>",
                "    Create a new library with the given library name and code.",
                "DELETE <LIBRARY NAME>",
                "    Delete the given library.",
                "LIST [LIBRARYNAME PATTERN] [WITHCODE]",
                "    Return general information on all the libraries:",
                "    * Library name",
                "    * The engine used to run the Library",
                "    * Library description",
                "    * Functions list",
                "    * Library code (if WITHCODE is given)",
                "    It also possible to get only function that matches a pattern using LIBRARYNAME argument.",
                "STATS",
                "    Return information about the current function running:",
                "    * Function name",
                "    * Command used to run the function",
                "    * Duration in MS that the function is running",
                "    If no function is running, return nil",
                "    In addition, returns a list of available engines.",
                "KILL",
                "    Kill the current running function.",
                "FLUSH [ASYNC|SYNC]",
                "    Delete all the libraries.",
                "    When called without the optional mode argument, the behavior is determined by the",
                "    lazyfree-lazy-user-flush configuration directive. Valid modes are:",
                "    * ASYNC: Asynchronously flush the libraries.",
                "    * SYNC: Synchronously flush the libraries.",
                "DUMP",
                "    Return a serialized payload representing the current libraries, can be restored using FUNCTION RESTORE command",
                "RESTORE <PAYLOAD> [FLUSH|APPEND|REPLACE]",
                "    Restore the libraries represented by the given payload, it is possible to give a restore policy to",
                "    control how to handle existing libraries (default APPEND):",
                "    * FLUSH: delete all existing libraries.",
                "    * APPEND: appends the restored libraries to the existing libraries. On collision, abort.",
                "    * REPLACE: appends the restored libraries to the existing libraries, On collision, replace the old",
                "      libraries with the new libraries (notice that even on this option there is a chance of failure",
                "      in case of functions name collision with another library).",
                "HELP",
                "    Print this help.",
            ],
        ),
    }
    Reply::Written
}

/// `FUNCTION LIST [LIBRARYNAME pattern] [WITHCODE]`.
fn list(ctx: &Context, session: &Session, args: &Args<'_>, out: &mut Vec<u8>) {
    let (mut with_code, mut pattern) = (false, None);
    let mut i = 2;
    while i < args.len() {
        let arg = args.get(i);
        if !with_code && arg.eq_ignore_ascii_case(b"WITHCODE") {
            with_code = true;
        } else if pattern.is_none() && arg.eq_ignore_ascii_case(b"LIBRARYNAME") {
            if i >= args.len() - 1 {
                resp::write_error(out, b"ERR library name argument was not given");
                return;
            }
            i += 1;
            pattern = Some(args.get(i));
        } else {
            let mut msg = b"ERR Unknown argument ".to_vec();
            msg.extend_from_slice(arg);
            resp::write_error(out, &msg);
            return;
        }
        i += 1;
    }
    let mut functions = ctx.scripting.functions.lock();
    functions.follow(&ctx.store);
    let resp3 = session.resp3;
    let listed: Vec<_> = functions
        .libraries()
        .filter(|l| pattern.is_none_or(|p| crate::glob::matches(p, &l.name, true)))
        .collect();
    resp::write_array_header(out, listed.len());
    for library in listed {
        resp::write_map_header(out, if with_code { 4 } else { 3 }, resp3);
        resp::write_bulk(out, b"library_name");
        resp::write_bulk(out, &library.name);
        resp::write_bulk(out, b"engine");
        resp::write_bulk(out, b"LUA");
        resp::write_bulk(out, b"functions");
        resp::write_array_header(out, library.functions.len());
        for function in &library.functions {
            resp::write_map_header(out, 3, resp3);
            resp::write_bulk(out, b"name");
            resp::write_bulk(out, &function.name);
            resp::write_bulk(out, b"description");
            match &function.description {
                Some(description) => resp::write_bulk(out, description),
                None => out.extend_from_slice(session.null()),
            }
            resp::write_bulk(out, b"flags");
            let flags: Vec<&str> = FLAG_NAMES
                .iter()
                .enumerate()
                .filter(|(i, _)| function.flags & (1 << i) != 0)
                .map(|(_, name)| *name)
                .collect();
            resp::write_set_header(out, flags.len(), resp3);
            for flag in flags {
                resp::write_simple(out, flag.as_bytes());
            }
        }
        if with_code {
            resp::write_bulk(out, b"library_code");
            resp::write_bulk(out, &library.code);
        }
    }
}
