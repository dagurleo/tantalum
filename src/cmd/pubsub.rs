//! Pub/sub commands (see `src/pubsub.rs`), and `RESET`.

use super::{Args, Reply, unknown_subcommand, wrong_arity};
use crate::context::{Context, Session};
use crate::pubsub::To;
use crate::resp;
use crate::store::Millis;

/// What a subscribed RESP2 client may still run, as in Redis.
pub(super) fn allowed_when_subscribed(command: &str) -> bool {
    matches!(
        command,
        "subscribe"
            | "unsubscribe"
            | "psubscribe"
            | "punsubscribe"
            | "ssubscribe"
            | "sunsubscribe"
            | "ping"
            | "quit"
            | "reset"
    )
}

/// Subcommands of the commands that have them, which Redis names as
/// `command|subcommand` in errors.
const SUBCOMMANDS: &[(&str, &[&str])] = &[
    ("client", &["setname", "getname", "id", "setinfo"]),
    ("command", &["count", "info", "list", "docs"]),
    ("config", &["get", "set"]),
    (
        "pubsub",
        &[
            "channels",
            "numsub",
            "numpat",
            "shardchannels",
            "shardnumsub",
            "help",
        ],
    ),
];

/// The error a subscribed RESP2 client gets for `command`. Returns `false`
/// for an unknown subcommand, which Redis reports first.
pub(super) fn not_allowed(out: &mut Vec<u8>, command: &str, args: &Args<'_>) -> bool {
    let mut name = command.to_owned();
    if let Some((_, subcommands)) = SUBCOMMANDS.iter().find(|(c, _)| *c == command)
        && args.len() > 1
    {
        let sub = String::from_utf8_lossy(args.get(1)).to_ascii_lowercase();
        if !subcommands.contains(&sub.as_str()) {
            return false;
        }
        name = format!("{command}|{sub}");
    }
    resp::write_error(
        out,
        format!(
            "ERR Can't execute '{name}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / \
             RESET are allowed in this context"
        )
        .as_bytes(),
    );
    true
}

/// A (un)subscribe reply's header: a push in RESP3.
fn header(out: &mut Vec<u8>, resp3: bool) {
    out.extend_from_slice(if resp3 { b">3\r\n" } else { b"*3\r\n" });
}

fn subscribe_to(
    ctx: &Context,
    session: &mut Session,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    to: To,
    reply: &[u8],
) -> Reply {
    let resp3 = session.resp3;
    let subscriptions = session.subscriptions(&ctx.pubsub);
    for i in 1..args.len() {
        subscriptions.add(to, args.get(i));
        header(out, resp3);
        resp::write_bulk(out, reply);
        resp::write_bulk(out, args.get(i));
        resp::write_integer(out, subscriptions.count(to) as i64);
    }
    Reply::Written
}

/// Unsubscribes from the channels (or patterns) given, or from all of them.
fn unsubscribe_from(
    session: &mut Session,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    to: To,
    reply: &[u8],
) -> Reply {
    let names: Vec<Box<[u8]>> = if args.len() > 1 {
        (1..args.len()).map(|i| args.get(i).into()).collect()
    } else {
        session
            .pubsub
            .as_ref()
            .map(|s| s.names(to))
            .unwrap_or_default()
    };
    if names.is_empty() {
        // Subscribed to none: one reply, with no name.
        header(out, session.resp3);
        resp::write_bulk(out, reply);
        out.extend_from_slice(session.null());
        let count = session.pubsub.as_ref().map_or(0, |s| s.count(to));
        resp::write_integer(out, count as i64);
        return Reply::Written;
    }
    for name in &names {
        let count = match session.pubsub.as_deref_mut() {
            Some(subscriptions) => {
                subscriptions.remove(to, name);
                // Messages sent before it unsubscribed go out before its
                // reply; none can come after.
                if !subscriptions.subscriber().take(out) {
                    return Reply::Close;
                }
                subscriptions.count(to)
            }
            None => 0,
        };
        header(out, session.resp3);
        resp::write_bulk(out, reply);
        resp::write_bulk(out, name);
        resp::write_integer(out, count as i64);
    }
    session.end_subscriptions(false);
    Reply::Written
}

/// `SUBSCRIBE channel [channel ...]`
pub(super) fn subscribe(
    c: &Context,
    s: &mut Session,
    _: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    subscribe_to(c, s, a, o, To::Channels, b"subscribe")
}

/// `PSUBSCRIBE pattern [pattern ...]`
pub(super) fn psubscribe(
    c: &Context,
    s: &mut Session,
    _: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    subscribe_to(c, s, a, o, To::Patterns, b"psubscribe")
}

/// `SSUBSCRIBE shardchannel [shardchannel ...]`
pub(super) fn ssubscribe(
    c: &Context,
    s: &mut Session,
    _: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    subscribe_to(c, s, a, o, To::ShardChannels, b"ssubscribe")
}

/// `UNSUBSCRIBE [channel ...]`
pub(super) fn unsubscribe(
    _: &Context,
    s: &mut Session,
    _: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    unsubscribe_from(s, a, o, To::Channels, b"unsubscribe")
}

/// `PUNSUBSCRIBE [pattern ...]`
pub(super) fn punsubscribe(
    _: &Context,
    s: &mut Session,
    _: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    unsubscribe_from(s, a, o, To::Patterns, b"punsubscribe")
}

/// `SUNSUBSCRIBE [shardchannel ...]`
pub(super) fn sunsubscribe(
    _: &Context,
    s: &mut Session,
    _: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    unsubscribe_from(s, a, o, To::ShardChannels, b"sunsubscribe")
}

/// `PUBLISH channel message`: how many received it.
pub(super) fn publish(
    ctx: &Context,
    _: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let receivers = ctx.pubsub.publish(args.get(1), args.get(2));
    resp::write_integer(out, receivers as i64);
    Reply::Written
}

/// `SPUBLISH shardchannel message`
pub(super) fn spublish(
    ctx: &Context,
    _: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let receivers = ctx.pubsub.publish_shard(args.get(1), args.get(2));
    resp::write_integer(out, receivers as i64);
    Reply::Written
}

const HELP: &[&str] = &[
    "PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    "CHANNELS [<pattern>]",
    "    Return the currently active channels matching a <pattern> (default: '*').",
    "NUMPAT",
    "    Return number of subscriptions to patterns.",
    "NUMSUB [<channel> ...]",
    "    Return the number of subscribers for the specified channels, excluding",
    "    pattern subscriptions(default: no channels).",
    "SHARDCHANNELS [<pattern>]",
    "    Return the currently active shard level channels matching a <pattern> (default: '*').",
    "SHARDNUMSUB [<shardchannel> ...]",
    "    Return the number of subscribers for the specified shard level channel(s)",
    "HELP",
    "    Print this help.",
];

/// `PUBSUB CHANNELS|NUMSUB|NUMPAT|SHARDCHANNELS|SHARDNUMSUB|HELP`
pub(super) fn pubsub(
    ctx: &Context,
    _: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let sub = args.get(1);
    let is = |name: &[u8]| sub.eq_ignore_ascii_case(name);
    if is(b"CHANNELS") || is(b"SHARDCHANNELS") {
        if args.len() > 3 {
            let mut msg = b"ERR unknown subcommand or wrong number of arguments for '".to_vec();
            msg.extend_from_slice(&sub[..sub.len().min(128)]);
            msg.extend_from_slice(b"'. Try PUBSUB HELP.");
            resp::write_error(out, &msg);
            return Reply::Written;
        }
        let pattern = (args.len() == 3).then(|| args.get(2));
        let channels = ctx.pubsub.channels(is(b"SHARDCHANNELS"), pattern);
        resp::write_array_header(out, channels.len());
        channels.iter().for_each(|c| resp::write_bulk(out, c));
    } else if is(b"NUMSUB") || is(b"SHARDNUMSUB") {
        let shard = is(b"SHARDNUMSUB");
        resp::write_array_header(out, 2 * (args.len() - 2));
        for i in 2..args.len() {
            resp::write_bulk(out, args.get(i));
            resp::write_integer(out, ctx.pubsub.subscribers(shard, args.get(i)) as i64);
        }
    } else if is(b"NUMPAT") {
        if args.len() != 2 {
            wrong_arity(out, "pubsub|numpat");
        } else {
            resp::write_integer(out, ctx.pubsub.pattern_count() as i64);
        }
    } else if is(b"HELP") {
        if args.len() != 2 {
            wrong_arity(out, "pubsub|help");
        } else {
            resp::write_array_header(out, HELP.len());
            HELP.iter()
                .for_each(|line| resp::write_simple(out, line.as_bytes()));
        }
    } else {
        unknown_subcommand(out, sub, "PUBSUB");
    }
    Reply::Written
}

/// `RESET`: the connection as it was when it connected: no subscriptions
/// (nor messages waiting), RESP2, no name, and if there's a password, not
/// authenticated.
pub(super) fn reset(
    ctx: &Context,
    session: &mut Session,
    _: Millis,
    _: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    session.end_subscriptions(true);
    session.name = None;
    session.set_resp3(false);
    session.set_authenticated(!ctx.has_password());
    out.extend_from_slice(b"+RESET\r\n");
    Reply::Written
}
