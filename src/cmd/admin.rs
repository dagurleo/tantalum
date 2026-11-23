//! What operators and tools ask a server: `MONITOR`, `SLOWLOG`, `CONFIG`, `MEMORY`,
//! `COMMAND GETKEYS`, `WAIT`, `ROLE`, `DEBUG` and the like. Replies are Redis 7.2.4's.

use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use super::{Args, NOT_AN_INTEGER, Reply, SYNTAX_ERROR, unknown_subcommand, wrong_arity};
use crate::command_table::{self, NoKeys};
use crate::config::{self, Kind};
use crate::context::{Context, Session};
use crate::glob;
use crate::resp;
use crate::store::{EvictionPolicy, Millis, View};

pub(super) fn help(out: &mut Vec<u8>, lines: &[&str]) {
    resp::write_array_header(out, lines.len());
    for line in lines {
        resp::write_simple(out, line.as_bytes());
    }
}

/// `MONITOR`: from now on, every command anyone runs is sent here.
pub(super) fn monitor(
    ctx: &Context,
    session: &mut Session,
    _: Millis,
    _: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    if session.deny_blocking {
        resp::write_error(out, b"ERR MONITOR isn't allowed for DENY_BLOCKING clients");
        return Reply::Written;
    }
    if session.monitor.is_none() {
        ctx.add_monitor(session);
        ctx.update_client(session.id, |client| client.monitor = true);
    }
    out.extend_from_slice(resp::OK);
    Reply::Written
}

/// `SLOWLOG GET [count] | LEN | RESET | HELP`
pub(super) fn slowlog(
    ctx: &Context,
    _: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let sub = args.get(1);
    let is = |name: &[u8]| sub.eq_ignore_ascii_case(name);
    if is(b"HELP") && args.len() != 2 {
        wrong_arity(out, "slowlog|help");
    } else if is(b"HELP") {
        help(
            out,
            &[
                "SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "GET [<count>]",
                "    Return top <count> entries from the slowlog (default: 10, -1 mean all).",
                "LEN",
                "    Return the length of the slowlog.",
                "RESET",
                "    Reset the slowlog.",
                "HELP",
                "    Print this help.",
            ],
        );
    } else if is(b"GET") && args.len() <= 3 {
        let count = if args.len() == 3 {
            match resp::parse_int(args.get(2)).filter(|&n| n >= -1) {
                Some(-1) => usize::MAX,
                Some(n) => n as usize,
                None => {
                    resp::write_error(out, b"ERR count should be greater than or equal to -1");
                    return Reply::Written;
                }
            }
        } else {
            10
        };
        ctx.slowlog.write(count, out);
    } else if is(b"GET") {
        resp::write_error(
            out,
            b"ERR unknown subcommand or wrong number of arguments for 'GET'. Try SLOWLOG HELP.",
        );
    } else if is(b"LEN") || is(b"RESET") {
        if args.len() != 2 {
            let name = sub.to_ascii_lowercase();
            wrong_arity(out, &format!("slowlog|{}", String::from_utf8_lossy(&name)));
        } else if is(b"LEN") {
            resp::write_integer(out, ctx.slowlog.len() as i64);
        } else {
            ctx.slowlog.reset();
            out.extend_from_slice(resp::OK);
        }
    } else {
        unknown_subcommand(out, sub, "SLOWLOG");
    }
    Reply::Written
}

/// `CONFIG GET | SET | RESETSTAT | REWRITE | HELP`
pub(super) fn config(
    ctx: &Context,
    session: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let sub = args.get(1);
    let is = |name: &[u8]| sub.eq_ignore_ascii_case(name);
    if is(b"GET") {
        if args.len() < 3 {
            wrong_arity(out, "config|get");
        } else {
            config_get(ctx, session, args, out);
        }
    } else if is(b"SET") {
        if args.len() < 4 || !args.len().is_multiple_of(2) {
            wrong_arity(out, "config|set");
        } else {
            let started = Instant::now();
            let was_special = session.special;
            config_set(ctx, args, out);
            ctx.follow_special(session);
            // This very command turned on what times commands, and Redis' log has it.
            if session.special
                && !was_special
                && let Some(command) = super::find_command(args.get(0))
            {
                super::has_run(ctx, session, command, args, started, false, None);
            }
        }
    } else if is(b"RESETSTAT") {
        if args.len() != 2 {
            wrong_arity(out, "config|resetstat");
        } else {
            ctx.reset_stats();
            // From now on commands are counted, this one too (as in Redis, after the reset).
            if ctx.stats.start(super::command_id(args)) {
                ctx.update_special();
            }
            out.extend_from_slice(resp::OK);
        }
    } else if is(b"REWRITE") {
        if args.len() != 2 {
            wrong_arity(out, "config|rewrite");
        } else {
            resp::write_error(out, b"ERR The server is running without a config file");
        }
    } else if is(b"HELP") && args.len() != 2 {
        wrong_arity(out, "config|help");
    } else if is(b"HELP") {
        help(
            out,
            &[
                "CONFIG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "GET <pattern>",
                "    Return parameters matching the glob-like <pattern> and their values.",
                "SET <directive> <value>",
                "    Set the configuration <directive> to <value>.",
                "RESETSTAT",
                "    Reset statistics reported by the INFO command.",
                "REWRITE",
                "    Rewrite the configuration file.",
                "HELP",
                "    Print this help.",
            ],
        );
    } else {
        unknown_subcommand(out, sub, "CONFIG");
    }
    Reply::Written
}

/// What settings that act on the server are right now.
fn live_setting(ctx: &Context, name: &str) -> Option<String> {
    let store = &*ctx.store;
    Some(match name {
        "maxmemory" => store.maxmemory().to_string(),
        "maxmemory-policy" => store.policy().name().to_string(),
        "databases" => store.databases().to_string(),
        "maxclients" => ctx.maxclients().to_string(),
        "timeout" => ctx.timeout().map_or(0, |t| t.as_secs()).to_string(),
        "tcp-keepalive" => ctx.tcp_keepalive().map_or(0, |t| t.as_secs()).to_string(),
        "requirepass" => String::from_utf8_lossy(&ctx.password().unwrap_or_default()).into_owned(),
        "aclfile" => ctx
            .acl
            .file()
            .map_or(String::new(), |f| f.display().to_string()),
        "slowlog-log-slower-than" => ctx.slowlog.threshold().to_string(),
        "slowlog-max-len" => ctx.slowlog.max_len().to_string(),
        "appendonly" => if ctx.aof().is_some() { "yes" } else { "no" }.to_string(),
        // In Redis' "seconds changes" form: save after this long if anything changed.
        "save" => ctx
            .snapshots
            .as_ref()
            .and_then(|s| s.interval)
            .map_or(String::new(), |i| format!("{} 1", i.as_secs())),
        _ => return None,
    })
}

fn config_get(ctx: &Context, session: &Session, args: &Args<'_>, out: &mut Vec<u8>) {
    let matching: Vec<_> = ctx
        .config
        .all()
        .into_iter()
        .filter(|(name, _)| {
            (2..args.len()).any(|i| glob::matches(args.get(i), name.as_bytes(), true))
        })
        .collect();
    resp::write_map_header(out, matching.len(), session.resp3);
    for (name, value) in matching {
        resp::write_bulk(out, name.as_bytes());
        match live_setting(ctx, name) {
            Some(live) => resp::write_bulk(out, live.as_bytes()),
            None => resp::write_bulk(out, value.as_bytes()),
        }
    }
}

/// `CONFIG SET name value [name value ...]`: all of them or none.
fn config_set(ctx: &Context, args: &Args<'_>, out: &mut Vec<u8>) {
    let failed = |out: &mut Vec<u8>, name: &str, why: &str| {
        let message =
            format!("ERR CONFIG SET failed (possibly related to argument '{name}') - {why}");
        resp::write_error(out, message.as_bytes());
    };
    let mut changes: Vec<(&'static str, String)> = Vec::new();
    for i in (2..args.len()).step_by(2) {
        let name = String::from_utf8_lossy(args.get(i)).to_ascii_lowercase();
        let value = args.get(i + 1);
        let Some((name, kind)) = config::find(&name) else {
            let message = format!(
                "ERR Unknown option or number of arguments for CONFIG SET - '{}'",
                String::from_utf8_lossy(args.get(i))
            );
            resp::write_error(out, message.as_bytes());
            return;
        };
        if changes.iter().any(|(known, _)| *known == name) {
            failed(out, name, "duplicate parameter");
            return;
        }
        let text = String::from_utf8_lossy(value).into_owned();
        let value = match kind {
            Kind::Immutable => {
                failed(out, name, "can't set immutable config");
                return;
            }
            Kind::Protected => {
                failed(out, name, "can't set protected config");
                return;
            }
            Kind::Bool => {
                if !value.eq_ignore_ascii_case(b"yes") && !value.eq_ignore_ascii_case(b"no") {
                    failed(out, name, "argument must be 'yes' or 'no'");
                    return;
                }
                text.to_ascii_lowercase()
            }
            Kind::Int(min, max) => match resp::parse_int(value) {
                Some(n) if (min..=max).contains(&n) => n.to_string(),
                Some(_) => {
                    let why = format!("argument must be between {min} and {max} inclusive");
                    failed(out, name, &why);
                    return;
                }
                None => {
                    failed(out, name, "argument couldn't be parsed into an integer");
                    return;
                }
            },
            Kind::Memory(min, max) => match config::parse_memory(value) {
                Some(n) if (min..=max).contains(&n) => n.to_string(),
                Some(_) => {
                    let why = format!("argument must be between {min} and {max} inclusive");
                    failed(out, name, &why);
                    return;
                }
                None => {
                    failed(out, name, "argument must be a memory value");
                    return;
                }
            },
            Kind::MemoryOrPercent => {
                let percent = text
                    .strip_suffix('%')
                    .and_then(|n| n.parse::<i64>().ok())
                    .filter(|n| *n >= 0);
                match (config::parse_memory(value), percent) {
                    (Some(n), _) => n.to_string(),
                    (None, Some(0)) => "0".to_string(),
                    (None, Some(n)) if n <= 100 => format!("{n}%"),
                    (None, Some(_)) => {
                        failed(
                            out,
                            name,
                            "percentage argument must be less or equal to 100",
                        );
                        return;
                    }
                    (None, None) => {
                        failed(out, name, "argument must be a memory or percent value");
                        return;
                    }
                }
            }
            Kind::OneOf(choices) => {
                let lower = text.to_ascii_lowercase();
                // These two take several, between spaces.
                let fits = if name.starts_with("shutdown-on-") {
                    lower.split(' ').all(|word| choices.contains(&word))
                } else {
                    choices.contains(&lower.as_str())
                };
                if !fits {
                    let why = format!(
                        "argument(s) must be one of the following: {}",
                        choices.join(", ")
                    );
                    failed(out, name, &why);
                    return;
                }
                lower
            }
            Kind::Text if name == "latency-tracking-info-percentiles" => {
                match crate::stats::parse_percentiles(&text) {
                    Ok((normal, _)) => normal,
                    Err(why) => {
                        failed(out, name, why);
                        return;
                    }
                }
            }
            // Applied as given: Redis shows `KEAn` as `AKE`, but keeps the `n`.
            Kind::Text if name == "notify-keyspace-events" => {
                if crate::notify::parse(text.as_bytes()).is_none() {
                    failed(out, name, crate::notify::INVALID);
                    return;
                }
                text
            }
            Kind::Text => text,
        };
        if let Err(why) = can_set(ctx, name, &value) {
            failed(out, name, why);
            return;
        }
        // Redis keeps it within what it can do, and says what it kept.
        let value = match (name, value.parse::<i64>()) {
            ("hz", Ok(hz)) => hz.clamp(1, 500).to_string(),
            _ => value,
        };
        changes.push((name, value));
    }
    for (name, value) in changes {
        apply(ctx, name, &value);
        if name == "notify-keyspace-events" {
            let flags = crate::notify::parse(value.as_bytes()).unwrap_or(0);
            ctx.config.set(name, &crate::notify::format(flags));
        } else {
            ctx.config.set(name, &value);
        }
    }
    out.extend_from_slice(resp::OK);
}

/// Whether Tantalum can do what a setting asks: those it can't say so, rather than
/// being remembered and doing nothing.
fn can_set(ctx: &Context, name: &str, value: &str) -> Result<(), &'static str> {
    match name {
        "appendonly" if (value == "yes") != ctx.aof().is_some() => {
            Err("the append-only log is turned on or off by starting with --aof")
        }
        "save" if ctx.snapshots.is_some() => {
            Err("snapshots are set by starting with --snapshot and --snapshot-interval")
        }
        "replica-read-only" | "slave-read-only" if value == "no" => {
            Err("replicas here are read-only: writable replicas aren't supported")
        }
        // Where `~` trims depends on it, and the log's commands run again with the default.
        "stream-node-max-entries" | "stream-node-max-bytes"
            if ctx.aof().is_some() && config::default_of(name) != Some(value) =>
        {
            Err("not with the append-only log on")
        }
        _ => Ok(()),
    }
}

/// Makes a setting that acts on the server take effect.
fn apply(ctx: &Context, name: &str, value: &str) {
    let number = || value.parse::<i64>().unwrap_or(0);
    let seconds = || Some(Duration::from_secs(number().max(0) as u64)).filter(|t| !t.is_zero());
    match name {
        "maxmemory" => ctx
            .store
            .set_maxmemory(value.parse::<u64>().unwrap_or(0) as usize),
        "maxmemory-policy" => {
            if let Some(policy) = EvictionPolicy::parse(value) {
                ctx.store.set_policy(policy);
            }
        }
        "maxclients" => ctx.set_maxclients(number().max(1) as usize),
        "latency-tracking" => ctx.stats.set_latency_tracking(value == "yes"),
        "notify-keyspace-events" => {
            ctx.set_keyspace_events(crate::notify::parse(value.as_bytes()).unwrap_or(0))
        }
        "latency-tracking-info-percentiles" => {
            if let Ok((_, percentiles)) = crate::stats::parse_percentiles(value) {
                ctx.stats.set_percentiles(percentiles);
            }
        }
        "timeout" => ctx.set_timeout(seconds()),
        "tcp-keepalive" => ctx.set_tcp_keepalive(seconds()),
        "requirepass" => {
            ctx.set_password(Some(value.as_bytes().to_vec()).filter(|p| !p.is_empty()))
        }
        "slowlog-log-slower-than" => {
            ctx.slowlog.set_threshold(number());
            ctx.update_special();
        }
        "slowlog-max-len" => ctx.slowlog.set_max_len(number().max(0) as usize),
        "latency-monitor-threshold" => {
            ctx.latency.set_threshold(number().max(0) as u64);
            ctx.update_special();
        }
        "stream-node-max-entries" => ctx.set_stream_node_entries(number().max(0) as usize),
        "stream-node-max-bytes" => ctx.set_stream_node_bytes(number().max(0) as usize),
        "hll-sparse-max-bytes" => ctx.set_hll_sparse_max_bytes(number().max(0) as usize),
        "tracking-table-max-keys" => ctx.tracking.set_max_keys(ctx, number().max(0) as u64),
        "acllog-max-len" => ctx.acl.set_log_max(number().max(0) as usize),
        "busy-reply-threshold" | "lua-time-limit" => {
            ctx.scripting.set_busy_threshold(number().max(0) as u64)
        }
        "acl-pubsub-default" => ctx
            .acl
            .set_all_channels(value.eq_ignore_ascii_case("allchannels")),
        "repl-backlog-size" => ctx.repl.set_backlog_size(number().max(0) as usize),
        "repl-ping-replica-period" | "repl-ping-slave-period" => ctx
            .repl
            .settings
            .ping_period
            .store(number().max(1) as u64, Relaxed),
        "repl-timeout" => ctx
            .repl
            .settings
            .timeout
            .store(number().max(1) as u64, Relaxed),
        "masterauth" => {
            *ctx.repl.settings.masterauth.lock() =
                Some(value.as_bytes().to_vec()).filter(|p| !p.is_empty())
        }
        "masteruser" => {
            *ctx.repl.settings.masteruser.lock() =
                Some(value.as_bytes().to_vec()).filter(|u| !u.is_empty())
        }
        "min-replicas-to-write" | "min-slaves-to-write" => {
            ctx.repl
                .settings
                .min_replicas_to_write
                .store(number().max(0) as usize, Relaxed);
            ctx.update_special();
        }
        "min-replicas-max-lag" | "min-slaves-max-lag" => ctx
            .repl
            .settings
            .min_replicas_max_lag
            .store(number().max(0) as u64, Relaxed),
        "replica-announce-ip" | "slave-announce-ip" => {
            *ctx.repl.settings.announce_ip.lock() =
                Some(value.to_string()).filter(|ip| !ip.is_empty())
        }
        "replica-announce-port" | "slave-announce-port" => ctx
            .repl
            .settings
            .announce_port
            .store(number().clamp(0, 65535) as u16, Relaxed),
        "replica-priority" | "slave-priority" => ctx
            .repl
            .settings
            .priority
            .store(number().max(0) as u32, Relaxed),
        "replica-serve-stale-data" | "slave-serve-stale-data" => ctx
            .repl
            .settings
            .serve_stale_data
            .store(value == "yes", Relaxed),
        _ => {}
    }
}

/// `MEMORY USAGE key [SAMPLES n] | DOCTOR | STATS | MALLOC-STATS | PURGE | HELP`
pub(super) fn memory(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let sub = args.get(1);
    let is = |name: &[u8]| sub.eq_ignore_ascii_case(name);
    let exact = |n: usize, name: &str, out: &mut Vec<u8>| {
        let ok = args.len() == n;
        if !ok {
            wrong_arity(out, &format!("memory|{name}"));
        }
        ok
    };
    if is(b"USAGE") {
        if args.len() < 3 {
            wrong_arity(out, "memory|usage");
        } else if args.len() > 3 {
            if args.len() != 5 || !args.get(3).eq_ignore_ascii_case(b"SAMPLES") {
                resp::write_error(out, SYNTAX_ERROR);
            } else {
                match resp::parse_int(args.get(4)) {
                    None => resp::write_error(out, NOT_AN_INTEGER),
                    Some(n) if n < 0 => resp::write_error(out, SYNTAX_ERROR),
                    Some(_) => memory_usage(ctx, session, now, args, out),
                }
            }
        } else {
            memory_usage(ctx, session, now, args, out);
        }
    } else if is(b"DOCTOR") {
        if exact(2, "doctor", out) {
            let text: &[u8] = if ctx.store.stats().keys == 0 {
                b"Hi Sam, this instance is empty or is using very little memory, my issues detector can't be used in these conditions. Please, leave for your mission on Earth and fill it with some data. The new Sam and I will be back to our programming as soon as I finished rebooting.\n"
            } else {
                b"Hi Sam, I can't find any memory issue in your instance. I can only account for what occurs on this base.\n"
            };
            resp::write_verbatim_text(out, text, session.resp3);
        }
    } else if is(b"MALLOC-STATS") {
        if exact(2, "malloc-stats", out) {
            resp::write_verbatim_text(
                out,
                b"Stats not supported for the current allocator",
                session.resp3,
            );
        }
    } else if is(b"PURGE") {
        if exact(2, "purge", out) {
            out.extend_from_slice(resp::OK);
        }
    } else if is(b"STATS") {
        if exact(2, "stats", out) {
            memory_stats(ctx, session, out);
        }
    } else if is(b"HELP") && args.len() != 2 {
        wrong_arity(out, "memory|help");
    } else if is(b"HELP") {
        help(
            out,
            &[
                "MEMORY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "DOCTOR",
                "    Return memory problems reports.",
                "MALLOC-STATS",
                "    Return internal statistics report from the memory allocator.",
                "PURGE",
                "    Attempt to purge dirty pages for reclamation by the allocator.",
                "STATS",
                "    Return information about the memory usage of the server.",
                "USAGE <key> [SAMPLES <count>]",
                "    Return memory in bytes used by <key> and its value.",
                "HELP",
                "    Print this help.",
            ],
        );
    } else {
        unknown_subcommand(out, sub, "MEMORY");
    }
    Reply::Written
}

fn memory_usage(ctx: &Context, session: &Session, now: Millis, args: &Args<'_>, out: &mut Vec<u8>) {
    let store = ctx.db(session);
    let key = args.get(2);
    let size = store
        .update_key(store.key(key), now, |current| {
            let size = current.map(|c| match c.value.view() {
                View::Bytes(bytes) => crate::store::entry_size(key.len(), bytes.len()),
                View::Object(object) => {
                    crate::store::entry_size(key.len(), 0) + object.allocation_size()
                }
            });
            (crate::store::Update::Peek, size)
        })
        .unwrap_or(None);
    match size {
        Some(size) => resp::write_integer(out, size as i64),
        None => out.extend_from_slice(session.null()),
    }
}

fn memory_stats(ctx: &Context, session: &Session, out: &mut Vec<u8>) {
    enum Number {
        Int(usize),
        Float(f64),
    }
    use Number::{Float, Int};
    let write = |out: &mut Vec<u8>, name: &str, number: Number| {
        resp::write_bulk(out, name.as_bytes());
        match number {
            Int(n) => resp::write_integer(out, n as i64),
            Float(n) => super::zset::write_score(out, n, session.resp3),
        }
    };
    let stats = ctx.store.stats();
    let total = stats.used_memory;
    let peak = ctx.note_memory_peak().max(total);
    let dataset = stats.segment_bytes + stats.external_bytes;
    let overhead = total.saturating_sub(dataset);
    let startup = overhead.saturating_sub(stats.table_bytes + stats.client_bytes);
    let net = total - startup.min(total);
    let rss = super::rss_bytes().unwrap_or(total);
    let ratio = |part: usize, whole: usize| part as f64 / whole.max(1) as f64;
    let dbs: Vec<_> = ctx
        .store
        .live()
        .filter(|db| db.key_counts().0 > 0)
        .map(|db| (db.index(), db.table_bytes()))
        .collect();
    let head = [
        ("peak.allocated", Int(peak)),
        ("total.allocated", Int(total)),
        ("startup.allocated", Int(startup)),
        ("replication.backlog", Int(0)),
        ("clients.slaves", Int(0)),
        ("clients.normal", Int(stats.client_bytes)),
        ("cluster.links", Int(0)),
        ("aof.buffer", Int(0)),
        ("lua.caches", Int(0)),
        ("functions.caches", Int(0)),
    ];
    // The allocator's own numbers aren't asked for: what's in use stands in for them.
    let tail = [
        ("overhead.total", Int(overhead)),
        ("keys.count", Int(stats.keys)),
        (
            "keys.bytes-per-key",
            Int(net.checked_div(stats.keys).unwrap_or(0)),
        ),
        ("dataset.bytes", Int(dataset)),
        ("dataset.percentage", Float(100.0 * ratio(dataset, net))),
        ("peak.percentage", Float(100.0 * ratio(total, peak))),
        ("allocator.allocated", Int(total)),
        ("allocator.active", Int(total)),
        ("allocator.resident", Int(rss)),
        ("allocator-fragmentation.ratio", Float(1.0)),
        ("allocator-fragmentation.bytes", Int(0)),
        ("allocator-rss.ratio", Float(ratio(rss, total))),
        ("allocator-rss.bytes", Int(rss.saturating_sub(total))),
        ("rss-overhead.ratio", Float(1.0)),
        ("rss-overhead.bytes", Int(0)),
        ("fragmentation", Float(ratio(rss, total))),
        ("fragmentation.bytes", Int(rss.saturating_sub(total))),
    ];
    resp::write_map_header(out, head.len() + dbs.len() + tail.len(), session.resp3);
    for (name, number) in head {
        write(out, name, number);
    }
    for (index, table) in dbs {
        resp::write_bulk(out, format!("db.{index}").as_bytes());
        resp::write_map_header(out, 3, session.resp3);
        write(out, "overhead.hashtable.main", Int(table));
        write(out, "overhead.hashtable.expires", Int(0));
        write(out, "overhead.hashtable.slot-to-keys", Int(0));
    }
    for (name, number) in tail {
        write(out, name, number);
    }
}

/// A timeout as `WAIT` reads it, in milliseconds: 0 for as long as it takes.
fn wait_timeout(arg: &[u8], out: &mut Vec<u8>) -> Option<Option<Duration>> {
    match resp::parse_int(arg) {
        None => {
            resp::write_error(out, b"ERR timeout is not an integer or out of range");
            None
        }
        Some(n) if n < 0 => {
            resp::write_error(out, b"ERR timeout is negative");
            None
        }
        Some(0) => Some(None),
        Some(n) => Some(Some(Duration::from_millis(n as u64))),
    }
}

/// `WAIT numreplicas timeout`: there are no replicas, so 0, at once if none are
/// asked for and otherwise when the time is up.
pub(super) fn wait(
    ctx: &Context,
    session: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let Some(replicas) = resp::parse_int(args.get(1)) else {
        resp::write_error(out, NOT_AN_INTEGER);
        return Reply::Written;
    };
    let Some(timeout) = wait_timeout(args.get(2), out) else {
        return Reply::Written;
    };
    if ctx.repl.following() {
        resp::write_error(
            out,
            b"ERR WAIT cannot be used with replica instances. Please also note that since Redis 4.0 if a replica is configured to be writable (which is not the default) writes to replicas are just local and are not propagated.",
        );
        return Reply::Written;
    }
    if let Some(master) = ctx.repl.master() {
        return crate::repl::master::wait_for_replicas(
            ctx,
            session,
            &master,
            replicas.max(0) as usize,
            timeout,
            out,
        );
    }
    if replicas <= 0 || session.deny_blocking {
        resp::write_integer(out, 0);
        return Reply::Written;
    }
    crate::blocking::block_for(ctx, session, timeout, b":0\r\n")
}

/// `WAITAOF numlocal numreplicas timeout`: how many have what's been written so far
/// on disk: this server, if it's asked and has the log on, and no replicas.
pub(super) fn waitaof(
    ctx: &Context,
    session: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let Some(local) = resp::parse_int(args.get(1)) else {
        resp::write_error(out, NOT_AN_INTEGER);
        return Reply::Written;
    };
    if !(0..=1).contains(&local) {
        resp::write_error(
            out,
            b"ERR value is out of range, value must between 0 and 1",
        );
        return Reply::Written;
    }
    let Some(replicas) = resp::parse_int(args.get(2)).filter(|&n| n >= 0) else {
        resp::write_error(out, b"ERR value is out of range, must be positive");
        return Reply::Written;
    };
    let Some(timeout) = wait_timeout(args.get(3), out) else {
        return Reply::Written;
    };
    let logged = match ctx.aof() {
        Some(aof) => i64::from(aof.flush().is_ok()),
        None if local > 0 => {
            resp::write_error(
                out,
                b"ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.",
            );
            return Reply::Written;
        }
        None => 0,
    };
    if replicas == 0 || session.deny_blocking {
        resp::write_array_header(out, 2);
        resp::write_integer(out, logged);
        resp::write_integer(out, 0);
        return Reply::Written;
    }
    let reply: &'static [u8] = if logged == 1 {
        b"*2\r\n:1\r\n:0\r\n"
    } else {
        b"*2\r\n:0\r\n:0\r\n"
    };
    crate::blocking::block_for(ctx, session, timeout, reply)
}

/// `ROLE`: a master, with the replicas that follow, or a replica.
pub(super) fn role(
    ctx: &Context,
    _: &mut Session,
    _: Millis,
    _: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    crate::repl::role(ctx, out);
    Reply::Written
}

/// `REPLICAOF host port` (and `SLAVEOF`): follow that master; `REPLICAOF NO
/// ONE`: stop following, keeping the data.
pub(super) fn replicaof(
    ctx: &Context,
    _: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    crate::repl::replicaof(ctx, args, out);
    Reply::Written
}

/// `LOLWUT [VERSION n]`: the line that matters of Redis' art.
pub(super) fn lolwut(
    _: &Context,
    session: &mut Session,
    _: Millis,
    _: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let text = format!("Redis ver. {}\n", super::REDIS_VERSION);
    resp::write_verbatim_text(out, text.as_bytes(), session.resp3);
    Reply::Written
}

/// `MODULE LIST`: there are no modules, nor a way to load them.
pub(super) fn module(
    _: &Context,
    _: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let sub = args.get(1);
    if sub.eq_ignore_ascii_case(b"LIST") {
        if args.len() == 2 {
            resp::write_array_header(out, 0);
        } else {
            wrong_arity(out, "module|list");
        }
    } else if sub.eq_ignore_ascii_case(b"LOAD") || sub.eq_ignore_ascii_case(b"LOADEX") {
        resp::write_error(
            out,
            b"ERR Error loading the extension. Please check the server logs.",
        );
    } else if sub.eq_ignore_ascii_case(b"UNLOAD") {
        resp::write_error(
            out,
            b"ERR Error unloading module: no such module with that name",
        );
    } else if sub.eq_ignore_ascii_case(b"HELP") && args.len() != 2 {
        wrong_arity(out, "module|help");
    } else if sub.eq_ignore_ascii_case(b"HELP") {
        help(
            out,
            &[
                "MODULE <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "LIST",
                "    Return a list of loaded modules.",
                "HELP",
                "    Print this help.",
            ],
        );
    } else {
        unknown_subcommand(out, sub, "MODULE");
    }
    Reply::Written
}

/// `LATENCY`: nothing's measured, as with Redis' default of the monitor off.
pub(super) fn latency(
    ctx: &Context,
    session: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let sub = args.get(1);
    let is = |name: &[u8]| sub.eq_ignore_ascii_case(name);
    let exact = |n: usize, name: &str, out: &mut Vec<u8>| {
        let ok = args.len() == n;
        if !ok {
            wrong_arity(out, &format!("latency|{name}"));
        }
        ok
    };
    if is(b"LATEST") {
        if exact(2, "latest", out) {
            let latest = ctx.latency.latest();
            resp::write_array_header(out, latest.len());
            for (name, time, millis, max) in latest {
                resp::write_array_header(out, 4);
                resp::write_bulk(out, name.as_bytes());
                resp::write_integer(out, time as i64);
                resp::write_integer(out, millis as i64);
                resp::write_integer(out, max as i64);
            }
        }
    } else if is(b"HISTORY") {
        if exact(3, "history", out) {
            let samples = ctx.latency.history(args.get(2));
            resp::write_array_header(out, samples.len());
            for (time, millis) in samples {
                resp::write_array_header(out, 2);
                resp::write_integer(out, time as i64);
                resp::write_integer(out, millis as i64);
            }
        }
    } else if is(b"RESET") {
        let named: Vec<&[u8]> = (2..args.len()).map(|i| args.get(i)).collect();
        resp::write_integer(out, ctx.latency.reset(&named) as i64);
    } else if is(b"GRAPH") {
        if exact(3, "graph", out) {
            match ctx.latency.graph(args.get(2)) {
                Some(graph) => resp::write_verbatim_text(out, graph.as_bytes(), session.resp3),
                None => {
                    let mut message = b"ERR No samples available for event '".to_vec();
                    message.extend_from_slice(args.get(2));
                    message.push(b'\'');
                    resp::write_error(out, &message);
                }
            }
        }
    } else if is(b"DOCTOR") {
        if exact(2, "doctor", out) {
            let report = ctx.latency.doctor(ctx.slowlog.threshold());
            resp::write_verbatim_text(out, report.as_bytes(), session.resp3);
        }
    } else if is(b"HISTOGRAM") {
        // Counted from now on, as for INFO's sections.
        let started = ctx.stats.start(None);
        if started {
            ctx.update_special();
        }
        ctx.stats.latency_histogram(args, session.resp3, out);
        if started && let Some(id) = super::command_id(args) {
            // It counts as having run, after its reply as in Redis.
            ctx.stats.start_counted(id);
        }
    } else if is(b"HELP") && args.len() != 2 {
        wrong_arity(out, "latency|help");
    } else if is(b"HELP") {
        help(
            out,
            &[
                "LATENCY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "DOCTOR",
                "    Return a human readable latency analysis report.",
                "GRAPH <event>",
                "    Return an ASCII latency graph for the <event> class.",
                "HISTORY <event>",
                "    Return time-latency samples for the <event> class.",
                "LATEST",
                "    Return the latest latency samples for all events.",
                "RESET [<event> ...]",
                "    Reset latency data of one or more <event> classes.",
                "    (default: reset all data for all event classes)",
                "HISTOGRAM [COMMAND ...]",
                "    Return a cumulative distribution of latencies in the format of a histogram for the specified command names.",
                "    If no commands are specified then all histograms are replied.",
                "HELP",
                "    Print this help.",
            ],
        );
    } else {
        unknown_subcommand(out, sub, "LATENCY");
    }
    Reply::Written
}

/// `DEBUG POPULATE | SLEEP | ERROR | OBJECT`, with `--enable-debug-command`, as in Redis.
pub(super) fn debug(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    if !ctx.debug_allowed(session) {
        resp::write_error(out, b"ERR DEBUG command not allowed. If the enable-debug-command option is set to \"local\", you can run it from a local connection, otherwise you need to set this option in the configuration file, and then restart the server.");
        return Reply::Written;
    }
    let sub = args.get(1);
    let is = |name: &[u8]| sub.eq_ignore_ascii_case(name);
    if is(b"POPULATE") && (3..=5).contains(&args.len()) {
        let Some(count) = resp::parse_int(args.get(2)).filter(|&n| n >= 0) else {
            resp::write_error(out, NOT_AN_INTEGER);
            return Reply::Written;
        };
        let prefix = if args.len() > 3 { args.get(3) } else { b"key" };
        let size = if args.len() > 4 {
            match resp::parse_int(args.get(4)).filter(|&n| n >= 0) {
                Some(size) => Some(size as usize),
                None => {
                    resp::write_error(out, NOT_AN_INTEGER);
                    return Reply::Written;
                }
            }
        } else {
            None
        };
        let store = ctx.db(session);
        for i in 0..count {
            let mut key = prefix.to_vec();
            key.extend_from_slice(format!(":{i}").as_bytes());
            let mut value = format!("value:{i}").into_bytes();
            if let Some(size) = size {
                value.resize(size, 0);
            }
            let wrote = store.set(
                &key,
                &value,
                crate::store::Condition::IfAbsent,
                crate::store::Expiry::Never,
                now,
                false,
            );
            if wrote.is_err() {
                resp::write_error(out, super::OOM);
                return Reply::Written;
            }
        }
        out.extend_from_slice(resp::OK);
    } else if is(b"SLEEP") && args.len() == 3 {
        let seconds = std::str::from_utf8(args.get(2))
            .ok()
            .and_then(|t| t.parse::<f64>().ok());
        std::thread::sleep(Duration::from_secs_f64(seconds.unwrap_or(0.0).max(0.0)));
        out.extend_from_slice(resp::OK);
    } else if is(b"ERROR") && args.len() == 3 {
        // The error as given, on one line.
        let message: Vec<u8> = args
            .get(2)
            .iter()
            .map(|&b| if matches!(b, b'\r' | b'\n') { b' ' } else { b })
            .collect();
        resp::write_error(out, &message);
    } else if is(b"OBJECT") && args.len() >= 3 {
        let store = ctx.db(session);
        let key = args.get(2);
        let described = store
            .update_key(store.key(key), now, |current| {
                let size = current.map(|c| match c.value.view() {
                    View::Bytes(bytes) => bytes.len(),
                    View::Object(object) => object.allocation_size(),
                });
                (crate::store::Update::Peek, size)
            })
            .unwrap_or(None);
        match described {
            Some(size) => resp::write_simple(
                out,
                format!("Value at:0x0 refcount:1 encoding:raw serializedlength:{size} lru:0 lru_seconds_idle:0").as_bytes(),
            ),
            None => resp::write_error(out, b"ERR no such key"),
        }
    } else if (is(b"SET-ACTIVE-EXPIRE") || is(b"PAUSE-CRON")) && args.len() == 3 {
        // `atoi`, as Redis reads it: anything but a number is 0.
        let on = std::str::from_utf8(args.get(2))
            .ok()
            .and_then(|s| {
                let s = s.trim_start();
                let end = s
                    .char_indices()
                    .find(|&(i, c)| !(c.is_ascii_digit() || (i == 0 && (c == '-' || c == '+'))))
                    .map_or(s.len(), |(i, _)| i);
                s[..end].parse::<i64>().ok()
            })
            .is_some_and(|n| n != 0);
        if is(b"PAUSE-CRON") {
            // What runs by itself here is the background expiry.
            ctx.set_cron_paused(on);
        } else {
            ctx.set_active_expire(on);
        }
        out.extend_from_slice(resp::OK);
    } else if is(b"HELP") && args.len() == 2 {
        help(
            out,
            &[
                "DEBUG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "OBJECT <key>",
                "    Show low level info about `key` and associated value.",
                "POPULATE <count> [<prefix>] [<size>]",
                "    Create <count> string keys named key:<num>.",
                "SET-ACTIVE-EXPIRE <0|1>",
                "    Setting it to 0 disables expiring keys in background when they are not",
                "    accessed (otherwise the Redis behavior). Setting it to 1 reenables back the",
                "    default.",
                "PAUSE-CRON <0|1>",
                "    Stop periodic cron job processing.",
                "SLEEP <seconds>",
                "    Stop the server for <seconds>.",
                "ERROR <string>",
                "    Return a Redis protocol error with <string> as message.",
                "HELP",
                "    Print this help.",
            ],
        );
    } else {
        super::subcommand_syntax_error(out, sub, "DEBUG");
    }
    Reply::Written
}

/// `COMMAND GETKEYS` and `GETKEYSANDFLAGS`.
pub(super) fn command_getkeys(
    session: &Session,
    args: &Args<'_>,
    with_flags: bool,
    out: &mut Vec<u8>,
) {
    let name = if with_flags {
        "getkeysandflags"
    } else {
        "getkeys"
    };
    if args.len() < 3 {
        wrong_arity(out, &format!("command|{name}"));
        return;
    }
    let command: Vec<&[u8]> = (2..args.len()).map(|i| args.get(i)).collect();
    match command_table::keys_of(&command) {
        Ok(keys) => {
            resp::write_array_header(out, keys.len());
            for (at, flags) in keys {
                if !with_flags {
                    resp::write_bulk(out, command[at]);
                    continue;
                }
                resp::write_array_header(out, 2);
                resp::write_bulk(out, command[at]);
                resp::write_set_header(out, flags.len(), session.resp3);
                for flag in flags {
                    resp::write_simple(out, &flag);
                }
            }
        }
        Err(NoKeys::NoSuchCommand) => resp::write_error(out, b"ERR Invalid command specified"),
        Err(NoKeys::NoKeyArguments) => {
            resp::write_error(out, b"ERR The command has no key arguments");
        }
        Err(NoKeys::WrongCount) => {
            resp::write_error(
                out,
                b"ERR Invalid number of arguments specified for command",
            );
        }
        Err(NoKeys::Invalid {
            may_have_none: true,
        }) => resp::write_array_header(out, 0),
        Err(NoKeys::Invalid {
            may_have_none: false,
        }) => {
            resp::write_error(out, b"ERR Invalid arguments specified for command");
        }
    }
}
