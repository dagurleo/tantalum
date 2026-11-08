//! `ACL`, and the checks a restricted user's commands get (see `src/acl.rs`).

use std::sync::Arc;

use super::{
    Args, Command, Reply, find_command, subcommand_syntax_error, unknown_subcommand, wrong_arity,
};
use crate::acl::{self, Denied, Reason, Target, User, Where};
use crate::context::{Context, Session};
use crate::resp;
use crate::store::Millis;

/// `ACL CAT | DELUSER | DRYRUN | GENPASS | GETUSER | HELP | LIST | LOAD | LOG |
/// SAVE | SETUSER | USERS | WHOAMI`.
pub(super) fn acl(
    ctx: &Context,
    session: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let sub = args.get(1);
    let is = |name: &[u8]| sub.eq_ignore_ascii_case(name);
    let argc = args.len();
    // Each subcommand's arity, as Redis checks it before running anything.
    let arity: Option<(&str, i64)> = [
        ("cat", -2),
        ("deluser", -3),
        ("dryrun", -4),
        ("genpass", -2),
        ("getuser", 3),
        ("help", 2),
        ("list", 2),
        ("load", 2),
        ("log", -2),
        ("save", 2),
        ("setuser", -3),
        ("users", 2),
        ("whoami", 2),
    ]
    .into_iter()
    .find(|(name, _)| is(name.as_bytes()));
    let Some((name, arity)) = arity else {
        unknown_subcommand(out, sub, "ACL");
        return Reply::Written;
    };
    let n = argc as i64;
    if (arity > 0 && n != arity) || n < -arity {
        wrong_arity(out, &format!("acl|{name}"));
        return Reply::Written;
    }
    match name {
        "setuser" => return set_user(ctx, session, args, out),
        "deluser" => return delete_users(ctx, session, args, out),
        "getuser" => match ctx.acl.get(args.get(2)) {
            Some(user) => user.write_details(out, session.resp3),
            None => out.extend_from_slice(session.null()),
        },
        "list" | "users" => {
            let users = ctx.acl.users();
            resp::write_array_header(out, users.len());
            for user in users {
                if name == "users" {
                    resp::write_bulk(out, user.name());
                } else {
                    let mut line = b"user ".to_vec();
                    line.extend_from_slice(user.name());
                    line.push(b' ');
                    line.extend_from_slice(&user.describe());
                    resp::write_bulk(out, &line);
                }
            }
        }
        "whoami" => resp::write_bulk(out, session.user_name()),
        "load" | "save" if ctx.acl.file().is_none() => resp::write_error(
            out,
            b"ERR This Redis instance is not configured to use an ACL file. You may want to \
              specify users via the ACL SETUSER command and then issue a CONFIG REWRITE \
              (assuming you have a Redis configuration file set) in order to store users in the \
              Redis configuration.",
        ),
        "load" => match ctx.acl.load() {
            Ok(replaced) => {
                ctx.acl_changed();
                let me = replaced.iter().any(|u| session.is_user(u));
                for user in &replaced {
                    ctx.kill_user_clients(session.id, user.name());
                }
                out.extend_from_slice(resp::OK);
                if me {
                    return Reply::Close;
                }
            }
            Err(errors) => {
                let mut msg = b"ERR ".to_vec();
                msg.extend_from_slice(errors.as_bytes());
                resp::write_error(out, &msg);
            }
        },
        "save" => match ctx.acl.save() {
            Ok(()) => out.extend_from_slice(resp::OK),
            Err(e) => {
                eprintln!("Writing ACL file for ACL SAVE: {e}");
                resp::write_error(
                    out,
                    b"ERR There was an error trying to save the ACLs. Please check the server \
                      logs for more information",
                );
            }
        },
        "cat" if argc == 2 => {
            resp::write_array_header(out, acl::CATEGORIES.len());
            for category in acl::CATEGORIES {
                resp::write_bulk(out, category.as_bytes());
            }
        }
        "cat" if argc == 3 => match acl::commands_in(args.get(2)) {
            Some(names) => {
                resp::write_array_header(out, names.len());
                for name in names {
                    resp::write_bulk(out, name.as_bytes());
                }
            }
            None => {
                let category = args.get(2);
                let mut msg = b"ERR Unknown category '".to_vec();
                msg.extend_from_slice(&category[..category.len().min(128)]);
                msg.push(b'\'');
                resp::write_error(out, &msg);
            }
        },
        "genpass" if argc <= 3 => {
            let bits = if argc == 3 {
                match resp::parse_int(args.get(2)) {
                    Some(bits) => bits,
                    None => {
                        resp::write_error(out, b"ERR value is not an integer or out of range");
                        return Reply::Written;
                    }
                }
            } else {
                256
            };
            if bits <= 0 || bits > 4096 {
                resp::write_error(
                    out,
                    b"ERR ACL GENPASS argument must be the number of bits for the output \
                      password, a positive number up to 4096",
                );
            } else {
                resp::write_bulk(out, &acl::random_hex(((bits + 3) / 4) as usize));
            }
        }
        "log" if argc <= 3 => log(ctx, session, args, out),
        "dryrun" => dry_run(ctx, args, out),
        "help" => super::admin::help(
            out,
            &[
                "ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "CAT [<category>]",
                "    List all commands that belong to <category>, or all command categories",
                "    when no category is specified.",
                "DELUSER <username> [<username> ...]",
                "    Delete a list of users.",
                "DRYRUN <username> <command> [<arg> ...]",
                "    Returns whether the user can execute the given command without executing the command.",
                "GETUSER <username>",
                "    Get the user's details.",
                "GENPASS [<bits>]",
                "    Generate a secure 256-bit user password. The optional `bits` argument can",
                "    be used to specify a different size.",
                "LIST",
                "    Show users details in config file format.",
                "LOAD",
                "    Reload users from the ACL file.",
                "LOG [<count> | RESET]",
                "    Show the ACL log entries.",
                "SAVE",
                "    Save the current config to the ACL file.",
                "SETUSER <username> <attribute> [<attribute> ...]",
                "    Create or modify a user with the specified attributes.",
                "USERS",
                "    List all the registered usernames.",
                "WHOAMI",
                "    Return the current connection username.",
                "HELP",
                "    Print this help.",
            ],
        ),
        _ => subcommand_syntax_error(out, sub, "ACL"),
    }
    Reply::Written
}

fn set_user(ctx: &Context, session: &mut Session, args: &Args<'_>, out: &mut Vec<u8>) -> Reply {
    let name = args.get(2);
    if acl_has_spaces(name) {
        resp::write_error(
            out,
            b"ERR Usernames can't contain spaces or null characters",
        );
        return Reply::Written;
    }
    let ops: Vec<&[u8]> = (3..args.len()).map(|i| args.get(i)).collect();
    match ctx.acl.set_user(name, &ops) {
        Ok(changed) => {
            if let Some(old) = &changed.old
                && acl::channels_revoked(old, &changed.new)
            {
                ctx.kill_unsubscribable(session.id, &changed.new);
            }
            ctx.acl_changed();
            ctx.follow_special(session);
            out.extend_from_slice(resp::OK);
        }
        Err(msg) => resp::write_error(out, &msg),
    }
    Reply::Written
}

fn acl_has_spaces(name: &[u8]) -> bool {
    name.iter()
        .any(|&b| matches!(b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r' | 0))
}

fn delete_users(ctx: &Context, session: &mut Session, args: &Args<'_>, out: &mut Vec<u8>) -> Reply {
    let names: Vec<&[u8]> = (2..args.len()).map(|i| args.get(i)).collect();
    if names.contains(&&b"default"[..]) {
        resp::write_error(out, b"ERR The 'default' user cannot be removed");
        return Reply::Written;
    }
    let deleted = ctx.acl.delete(&names);
    let me = deleted.iter().any(|u| session.is_user(u));
    for user in &deleted {
        ctx.kill_user_clients(session.id, user.name());
    }
    ctx.acl_changed();
    resp::write_integer(out, deleted.len() as i64);
    if me {
        return Reply::Close;
    }
    Reply::Written
}

/// `ACL LOG [count | RESET]`.
fn log(ctx: &Context, session: &Session, args: &Args<'_>, out: &mut Vec<u8>) {
    let mut count = 10;
    if args.len() == 3 {
        if args.get(2).eq_ignore_ascii_case(b"RESET") {
            ctx.acl.reset_log();
            out.extend_from_slice(resp::OK);
            return;
        }
        match resp::parse_int(args.get(2)) {
            Some(n) => count = n.max(0) as usize,
            None => {
                resp::write_error(out, b"ERR value is not an integer or out of range");
                return;
            }
        }
    }
    let now = acl::millis_now();
    ctx.acl.with_log(|entries| {
        let entries: Vec<_> = entries.take(count).collect();
        resp::write_array_header(out, entries.len());
        for entry in entries {
            resp::write_map_header(out, 10, session.resp3);
            resp::write_bulk(out, b"count");
            resp::write_integer(out, entry.count as i64);
            resp::write_bulk(out, b"reason");
            resp::write_bulk(out, entry.reason_name().as_bytes());
            resp::write_bulk(out, b"context");
            resp::write_bulk(out, entry.context_name().as_bytes());
            resp::write_bulk(out, b"object");
            resp::write_bulk(out, &entry.object);
            resp::write_bulk(out, b"username");
            resp::write_bulk(out, &entry.username);
            resp::write_bulk(out, b"age-seconds");
            let mut age = Vec::new();
            crate::zset::format_score((now - entry.updated) as f64 / 1000.0, &mut age);
            if session.resp3 {
                out.push(b',');
                out.extend_from_slice(&age);
                out.extend_from_slice(b"\r\n");
            } else {
                resp::write_bulk(out, &age);
            }
            resp::write_bulk(out, b"client-info");
            resp::write_bulk(out, &entry.client_info);
            resp::write_bulk(out, b"entry-id");
            resp::write_integer(out, entry.id as i64);
            resp::write_bulk(out, b"timestamp-created");
            resp::write_integer(out, entry.created);
            resp::write_bulk(out, b"timestamp-last-updated");
            resp::write_integer(out, entry.updated);
        }
    });
}

/// `ACL DRYRUN username command [arg ...]`: what the check would say, run
/// as nothing.
fn dry_run(ctx: &Context, args: &Args<'_>, out: &mut Vec<u8>) {
    let Some(user) = ctx.acl.get(args.get(2)) else {
        let mut msg = b"ERR User '".to_vec();
        msg.extend_from_slice(args.get(2));
        msg.extend_from_slice(b"' not found");
        resp::write_error(out, &msg);
        return;
    };
    let rest = args.after(3);
    let not_found = |out: &mut Vec<u8>| {
        let mut msg = b"ERR Command '".to_vec();
        msg.extend_from_slice(rest.get(0));
        msg.extend_from_slice(b"' not found");
        resp::write_error(out, &msg);
    };
    let Some(command) = find_command(rest.get(0)) else {
        // One of Redis' commands that isn't here: what the rules say of it.
        let Some(target) = acl::target_elsewhere(&rest) else {
            return not_found(out);
        };
        match user.check(&target, &rest) {
            Ok(()) => out.extend_from_slice(resp::OK),
            Err(denied) => resp::write_bulk(out, &denied.message(&user, &target, &rest, true)),
        }
        return;
    };
    let container = crate::command_table::hidden(command.name, None).container;
    let Some(target) = acl::target(command.name, &rest) else {
        return not_found(out);
    };
    let arity = if container && rest.len() >= 2 {
        crate::command_table::arity_of(target.name()).unwrap_or(i64::from(command.arity))
    } else {
        i64::from(command.arity)
    };
    let n = rest.len() as i64;
    if (arity > 0 && arity != n) || n < -arity {
        wrong_arity(out, target.name());
        return;
    }
    match user.check(&target, &rest) {
        Ok(()) => out.extend_from_slice(resp::OK),
        Err(denied) => resp::write_bulk(out, &denied.message(&user, &target, &rest, true)),
    }
}

/// Checks a restricted user's command before it runs (Redis' `processCommand`
/// does it after the arity and authentication checks): `None` if it may
/// run, or the reply that refuses it, logged. Commands with a subcommand they
/// don't have, or the wrong count of arguments for it, run, and say so.
#[cold]
#[inline(never)]
pub(super) fn refused(
    ctx: &Context,
    session: &mut Session,
    command: &Command,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Option<Reply> {
    let user = session.user.clone()?;
    if user.deleted {
        return Some(Reply::Close);
    }
    if args.len() > 1
        && crate::command_table::refused_subcommand(command.name, args.get(1), args.len())
    {
        return None;
    }
    let target = acl::target(command.name, args)?;
    let denied = user.check(&target, args).err()?;
    let context = if session.multi.is_some() {
        Where::Multi
    } else {
        Where::Toplevel
    };
    log_denied(
        ctx,
        session,
        &user,
        &target,
        args,
        denied,
        context,
        target.name(),
    );
    let mut msg = b"NOPERM ".to_vec();
    msg.extend_from_slice(&denied.message(&user, &target, args, false));
    resp::write_error(out, &msg);
    Some(Reply::Written)
}

/// `EXEC`'s check of each queued command, in case the rules changed since it
/// was queued: the error that takes its reply's place, if it's refused.
pub(super) fn refused_in_exec(
    ctx: &Context,
    session: &mut Session,
    queued: usize,
    command: &Command,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> bool {
    let Some(user) = session.user.clone().filter(|u| !u.may_run_everything()) else {
        return false;
    };
    let Some(target) = acl::target(command.name, args) else {
        return false;
    };
    let Err(denied) = user.check(&target, args) else {
        return false;
    };
    let object = denied.object(&target, args);
    let info = ctx.client_info_in_exec(session, queued);
    ctx.acl
        .log(denied.reason(), Where::Multi, &object, user.name(), info);
    let reason = match denied {
        Denied::Command => "no permission to execute the command or subcommand",
        Denied::Key(_) => "no permission to touch the specified keys",
        Denied::Channel(_) => "no permission to access one of the channels used as arguments",
    };
    let msg = format!(
        "NOPERM ACLs rules changed between the moment the transaction was accumulated and the \
         EXEC call. This command is no longer allowed for the following reason: {reason}"
    );
    resp::write_error(out, msg.as_bytes());
    true
}

/// A script's `redis.call` of a command its user may not run: the error Lua
/// raises, logged (`caller` is the connection that ran the script, and its
/// command).
pub(crate) fn refused_in_script(
    ctx: &Context,
    session: &Session,
    caller: (u64, &str),
    command: &Command,
    args: &Args<'_>,
) -> Option<String> {
    let user = session.user.as_ref().filter(|u| !u.may_run_everything())?;
    let target = acl::target(command.name, args)?;
    let denied = user.check(&target, args).err()?;
    let object = denied.object(&target, args);
    let info = ctx.client_info_of(caller.0, caller.1);
    ctx.acl
        .log(denied.reason(), Where::Lua, &object, user.name(), info);
    let msg = denied.message(user, &target, args, false);
    Some(format!(
        "ACL failure in script: {}",
        String::from_utf8_lossy(&msg)
    ))
}

/// A blocked command run again now that its keys have data: Redis checks it
/// again, as its user's rules may have changed meanwhile. Whether it was
/// refused, its error written (`client` is its connection).
pub(crate) fn refused_when_served(
    ctx: &Context,
    client: u64,
    user: &Arc<User>,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> bool {
    let current = match ctx.acl.get(user.name()) {
        Some(now) if now.serial() == user.serial() => now,
        _ => user.tombstone(),
    };
    if current.may_run_everything() {
        return false;
    }
    let Some(command) = find_command(args.get(0)) else {
        return false;
    };
    let Some(target) = acl::target(command.name, args) else {
        return false;
    };
    let Err(denied) = current.check(&target, args) else {
        return false;
    };
    let object = denied.object(&target, args);
    let info = ctx.client_info_of(client, target.name());
    ctx.acl.log(
        denied.reason(),
        Where::Toplevel,
        &object,
        current.name(),
        info,
    );
    let mut msg = b"NOPERM ".to_vec();
    msg.extend_from_slice(&denied.message(&current, &target, args, false));
    resp::write_error(out, &msg);
    true
}

/// `redis.acl_check_cmd`: whether the script's user may run this.
pub(crate) fn may_run(session: &Session, command_args: &Args<'_>) -> Option<bool> {
    let command = find_command(command_args.get(0))?;
    let Some(user) = session.user.as_ref().filter(|u| !u.may_run_everything()) else {
        return Some(true);
    };
    Some(
        acl::target(command.name, command_args)
            .is_none_or(|t| user.check(&t, command_args).is_ok()),
    )
}

#[allow(clippy::too_many_arguments)]
fn log_denied(
    ctx: &Context,
    session: &Session,
    user: &User,
    target: &Target,
    args: &Args<'_>,
    denied: Denied,
    context: Where,
    cmd: &str,
) {
    let object = denied.object(target, args);
    let info = ctx.client_info(session, cmd);
    ctx.acl
        .log(denied.reason(), context, &object, user.name(), info);
}

/// A failed `AUTH` or `HELLO ... AUTH`, for `ACL LOG` and `INFO`.
pub(super) fn log_failed_auth(ctx: &Context, session: &Session, args: &Args<'_>, username: &[u8]) {
    // (Run by `EXEC`, which blocking commands can't wait in, is in a transaction too.)
    let context = if session.multi.is_some() || session.deny_blocking {
        Where::Multi
    } else {
        Where::Toplevel
    };
    let cmd = String::from_utf8_lossy(args.get(0)).to_ascii_lowercase();
    let info = ctx.client_info(session, &cmd);
    ctx.acl
        .log(Reason::Auth, context, args.get(0), username, info);
}

/// Whether a restricted user may give `SORT` a `BY` or `GET` pattern: only
/// with every key to read.
pub(super) fn may_sort_by_pattern(
    session: &Session,
    command: &'static str,
    args: &Args<'_>,
) -> bool {
    let Some(user) = session.user.as_ref().filter(|u| !u.may_run_everything()) else {
        return true;
    };
    acl::target(command, args).is_none_or(|t| user.may_read_every_key(&t, args))
}

/// Authenticates the connection as `name`, or says why not.
pub(super) fn authenticate(
    ctx: &Context,
    session: &mut Session,
    args: &Args<'_>,
    name: &[u8],
    password: &[u8],
) -> bool {
    match ctx.acl.authenticate(name, password) {
        Some(user) => {
            session.set_user(ctx, user);
            true
        }
        None => {
            log_failed_auth(ctx, session, args, name);
            false
        }
    }
}

impl Session {
    /// Whether the connection is authenticated as this very user.
    fn is_user(&self, user: &Arc<User>) -> bool {
        match &self.user {
            Some(mine) => mine.name() == user.name() && mine.serial() == user.serial(),
            None => user.name() == b"default",
        }
    }
}
