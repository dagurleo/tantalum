//! Command dispatch. Replies match Redis 7 byte for byte where practical.

mod hash;
mod keys;
mod list;
mod pubsub;
mod set;
mod zset;

use std::ops::Range;
use std::sync::OnceLock;

pub use crate::blocking::Blocked;
use crate::context::{Context, Session};
use crate::glob;
use crate::resp;
use crate::store::{
    COPY_UNDER_LOCK_MAX, Condition, Current, Expiry, Key, KeyHash, Kind, MAX_DEADLINE, Millis,
    OutOfMemory, SharedValue, Store, Update,
};

/// The Redis version whose replies Tantalum matches, for clients that check
/// it in `HELLO` or `INFO`. Valkey reports the version it forked from the
/// same way.
pub const REDIS_VERSION: &str = "7.2.4";

/// A parsed command's arguments: ranges into the connection's read buffer.
pub struct Args<'a> {
    buf: &'a [u8],
    ranges: &'a [Range<usize>],
    key_hash: Option<KeyHash>,
}

impl<'a> Args<'a> {
    pub fn new(buf: &'a [u8], ranges: &'a [Range<usize>]) -> Self {
        Args {
            buf,
            ranges,
            key_hash: None,
        }
    }

    /// Supplies the hash of the first argument (the key, for commands that
    /// take one), computed earlier with [`Store::key`].
    pub fn with_key_hash(mut self, hash: Option<KeyHash>) -> Self {
        self.key_hash = hash;
        self
    }

    /// The first argument as a store key.
    fn key(&self, store: &Store) -> Key<'a> {
        self.key_at(store, 1)
    }

    /// Argument `i` as a store key.
    fn key_at(&self, store: &Store, i: usize) -> Key<'a> {
        match self.key_hash {
            Some(hash) if i == 1 => store.key_with_hash(self.get(1), hash),
            _ => store.key(self.get(i)),
        }
    }

    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn get(&self, i: usize) -> &'a [u8] {
        &self.buf[self.ranges[i].clone()]
    }
}

pub enum Reply {
    /// The reply is in the output buffer.
    Written,
    /// A large bulk value the connection should write straight to the socket.
    Bulk(SharedValue),
    /// The reply is in the output buffer; close after flushing it.
    Close,
    /// A blocking command found its keys empty: the connection sends what
    /// it has, then waits for the reply (see `blocking.rs`). Boxed, so
    /// that every other reply stays as small as before.
    Block(Box<Blocked>),
}

type Handler = fn(&Context, &mut Session, Millis, &Args<'_>, &mut Vec<u8>) -> Reply;

/// A command's handler and what `COMMAND` reports about it.
struct Command {
    /// Lowercase, as Redis reports it.
    name: &'static str,
    /// Number of arguments including the name; negative means at least that.
    arity: i32,
    flags: &'static [&'static str],
    /// First key, last key (negative counts from the end) and the step
    /// between keys; zeros for commands without keys.
    keys: (i32, i32, i32),
    run: Handler,
}

impl Command {
    fn arity_allows(&self, len: usize) -> bool {
        let len = len as i64;
        let arity = i64::from(self.arity);
        if arity >= 0 {
            len == arity
        } else {
            len >= -arity
        }
    }
}

const NO_KEYS: (i32, i32, i32) = (0, 0, 0);
const ONE_KEY: (i32, i32, i32) = (1, 1, 1);
const ALL_KEYS: (i32, i32, i32) = (1, -1, 1);

/// Every command, in the order `COMMAND` lists them. Arity and flags as in
/// Redis 7.
#[rustfmt::skip]
static COMMANDS: &[Command] = &[
    Command { name: "get", arity: 2, flags: &["readonly", "fast"], keys: ONE_KEY, run: get },
    Command { name: "set", arity: -3, flags: &["write", "denyoom"], keys: ONE_KEY, run: set },
    Command { name: "mget", arity: -2, flags: &["readonly", "fast"], keys: ALL_KEYS, run: mget },
    Command { name: "mset", arity: -3, flags: &["write", "denyoom"], keys: (1, -1, 2), run: mset },
    Command { name: "del", arity: -2, flags: &["write"], keys: ALL_KEYS, run: del },
    Command { name: "unlink", arity: -2, flags: &["write", "fast"], keys: ALL_KEYS, run: del },
    Command { name: "exists", arity: -2, flags: &["readonly", "fast"], keys: ALL_KEYS, run: exists },
    Command { name: "expire", arity: -3, flags: &["write", "fast"], keys: ONE_KEY, run: expire },
    Command { name: "pexpire", arity: -3, flags: &["write", "fast"], keys: ONE_KEY, run: pexpire },
    Command { name: "ttl", arity: 2, flags: &["readonly", "fast"], keys: ONE_KEY, run: ttl },
    Command { name: "pttl", arity: 2, flags: &["readonly", "fast"], keys: ONE_KEY, run: pttl },
    Command { name: "incr", arity: 2, flags: &["write", "denyoom", "fast"], keys: ONE_KEY, run: incr },
    Command { name: "incrby", arity: 3, flags: &["write", "denyoom", "fast"], keys: ONE_KEY, run: incrby },
    Command { name: "decr", arity: 2, flags: &["write", "denyoom", "fast"], keys: ONE_KEY, run: decr },
    Command { name: "decrby", arity: 3, flags: &["write", "denyoom", "fast"], keys: ONE_KEY, run: decrby },
    Command { name: "setex", arity: 4, flags: &["write", "denyoom"], keys: ONE_KEY, run: setex },
    Command { name: "psetex", arity: 4, flags: &["write", "denyoom"], keys: ONE_KEY, run: psetex },
    Command { name: "setnx", arity: 3, flags: &["write", "denyoom", "fast"], keys: ONE_KEY, run: setnx },
    Command { name: "getdel", arity: 2, flags: &["write", "fast"], keys: ONE_KEY, run: getdel },
    Command { name: "getex", arity: -2, flags: &["write", "fast"], keys: ONE_KEY, run: getex },
    Command { name: "expireat", arity: -3, flags: &["write", "fast"], keys: ONE_KEY, run: expireat },
    Command { name: "pexpireat", arity: -3, flags: &["write", "fast"], keys: ONE_KEY, run: pexpireat },
    Command { name: "expiretime", arity: 2, flags: &["readonly", "fast"], keys: ONE_KEY, run: expiretime },
    Command { name: "pexpiretime", arity: 2, flags: &["readonly", "fast"], keys: ONE_KEY, run: pexpiretime },
    Command { name: "persist", arity: 2, flags: &["write", "fast"], keys: ONE_KEY, run: persist },
    Command { name: "ping", arity: -1, flags: &["fast"], keys: NO_KEYS, run: ping },
    Command { name: "echo", arity: 2, flags: &["fast"], keys: NO_KEYS, run: echo },
    Command { name: "select", arity: 2, flags: &["loading", "stale", "fast"], keys: NO_KEYS, run: select },
    Command { name: "dbsize", arity: 1, flags: &["readonly", "fast"], keys: NO_KEYS, run: dbsize },
    Command { name: "info", arity: -1, flags: &["loading", "stale"], keys: NO_KEYS, run: info },
    Command { name: "auth", arity: -2, flags: &["noscript", "loading", "stale", "fast", "no_auth"], keys: NO_KEYS, run: auth },
    Command { name: "hello", arity: -1, flags: &["noscript", "loading", "stale", "fast", "no_auth"], keys: NO_KEYS, run: hello },
    Command { name: "client", arity: -2, flags: &[], keys: NO_KEYS, run: client },
    Command { name: "command", arity: -1, flags: &["loading", "stale"], keys: NO_KEYS, run: command },
    Command { name: "config", arity: -2, flags: &[], keys: NO_KEYS, run: config },
    Command { name: "flushall", arity: -1, flags: &["write"], keys: NO_KEYS, run: flush },
    Command { name: "save", arity: 1, flags: &["admin", "noscript"], keys: NO_KEYS, run: save },
    Command { name: "bgsave", arity: -1, flags: &["admin", "noscript"], keys: NO_KEYS, run: bgsave },
    Command { name: "lastsave", arity: 1, flags: &["loading", "stale", "fast"], keys: NO_KEYS, run: lastsave },
    Command { name: "flushdb", arity: -1, flags: &["write"], keys: NO_KEYS, run: flush },
    Command { name: "quit", arity: -1, flags: &["noscript", "loading", "stale", "fast", "no_auth"], keys: NO_KEYS, run: quit },
    Command { name: "reset", arity: 1, flags: &["noscript", "loading", "stale", "fast", "no_auth", "allow_busy"], keys: NO_KEYS, run: pubsub::reset },
    Command { name: "subscribe", arity: -2, flags: &["pubsub", "noscript", "loading", "stale"], keys: NO_KEYS, run: pubsub::subscribe },
    Command { name: "unsubscribe", arity: -1, flags: &["pubsub", "noscript", "loading", "stale"], keys: NO_KEYS, run: pubsub::unsubscribe },
    Command { name: "psubscribe", arity: -2, flags: &["pubsub", "noscript", "loading", "stale"], keys: NO_KEYS, run: pubsub::psubscribe },
    Command { name: "punsubscribe", arity: -1, flags: &["pubsub", "noscript", "loading", "stale"], keys: NO_KEYS, run: pubsub::punsubscribe },
    Command { name: "ssubscribe", arity: -2, flags: &["pubsub", "noscript", "loading", "stale"], keys: (1, -1, 1), run: pubsub::ssubscribe },
    Command { name: "sunsubscribe", arity: -1, flags: &["pubsub", "noscript", "loading", "stale"], keys: (1, -1, 1), run: pubsub::sunsubscribe },
    Command { name: "publish", arity: 3, flags: &["pubsub", "loading", "stale", "fast"], keys: NO_KEYS, run: pubsub::publish },
    Command { name: "spublish", arity: 3, flags: &["pubsub", "loading", "stale", "fast"], keys: ONE_KEY, run: pubsub::spublish },
    Command { name: "pubsub", arity: -2, flags: &[], keys: NO_KEYS, run: pubsub::pubsub },
    Command { name: "type", arity: 2, flags: &["readonly", "fast"], keys: ONE_KEY, run: type_of },
    Command { name: "scan", arity: -2, flags: &["readonly"], keys: NO_KEYS, run: keys::scan },
    Command { name: "keys", arity: 2, flags: &["readonly"], keys: NO_KEYS, run: keys::keys },
    Command { name: "rename", arity: 3, flags: &["write"], keys: (1, 2, 1), run: keys::rename },
    Command { name: "renamenx", arity: 3, flags: &["write", "fast"], keys: (1, 2, 1), run: keys::renamenx },
    Command { name: "hget", arity: 3, flags: &["readonly", "fast"], keys: ONE_KEY, run: hash::hget },
    Command { name: "hset", arity: -4, flags: &["write", "denyoom", "fast"], keys: ONE_KEY, run: hash::hset },
    Command { name: "hgetall", arity: 2, flags: &["readonly"], keys: ONE_KEY, run: hash::hgetall },
    Command { name: "hdel", arity: -3, flags: &["write", "fast"], keys: ONE_KEY, run: hash::hdel },
    Command { name: "hmget", arity: -3, flags: &["readonly", "fast"], keys: ONE_KEY, run: hash::hmget },
    Command { name: "hincrby", arity: 4, flags: &["write", "denyoom", "fast"], keys: ONE_KEY, run: hash::hincrby },
    Command { name: "hexists", arity: 3, flags: &["readonly", "fast"], keys: ONE_KEY, run: hash::hexists },
    Command { name: "hlen", arity: 2, flags: &["readonly", "fast"], keys: ONE_KEY, run: hash::hlen },
    Command { name: "hmset", arity: -4, flags: &["write", "denyoom", "fast"], keys: ONE_KEY, run: hash::hmset },
    Command { name: "hsetnx", arity: 4, flags: &["write", "denyoom", "fast"], keys: ONE_KEY, run: hash::hsetnx },
    Command { name: "hkeys", arity: 2, flags: &["readonly"], keys: ONE_KEY, run: hash::hkeys },
    Command { name: "hvals", arity: 2, flags: &["readonly"], keys: ONE_KEY, run: hash::hvals },
    Command { name: "hstrlen", arity: 3, flags: &["readonly", "fast"], keys: ONE_KEY, run: hash::hstrlen },
    Command { name: "hscan", arity: -3, flags: &["readonly"], keys: ONE_KEY, run: hash::hscan },
    Command { name: "sadd", arity: -3, flags: &["write", "denyoom", "fast"], keys: ONE_KEY, run: set::sadd },
    Command { name: "sismember", arity: 3, flags: &["readonly", "fast"], keys: ONE_KEY, run: set::sismember },
    Command { name: "smembers", arity: 2, flags: &["readonly"], keys: ONE_KEY, run: set::smembers },
    Command { name: "srem", arity: -3, flags: &["write", "fast"], keys: ONE_KEY, run: set::srem },
    Command { name: "scard", arity: 2, flags: &["readonly", "fast"], keys: ONE_KEY, run: set::scard },
    Command { name: "smismember", arity: -3, flags: &["readonly", "fast"], keys: ONE_KEY, run: set::smismember },
    Command { name: "spop", arity: -2, flags: &["write", "fast"], keys: ONE_KEY, run: set::spop },
    Command { name: "srandmember", arity: -2, flags: &["readonly"], keys: ONE_KEY, run: set::srandmember },
    Command { name: "smove", arity: 4, flags: &["write", "fast"], keys: (1, 2, 1), run: set::smove },
    Command { name: "sinter", arity: -2, flags: &["readonly"], keys: ALL_KEYS, run: set::sinter },
    Command { name: "sinterstore", arity: -3, flags: &["write", "denyoom"], keys: ALL_KEYS, run: set::sinterstore },
    Command { name: "sintercard", arity: -3, flags: &["readonly"], keys: NO_KEYS, run: set::sintercard },
    Command { name: "sunion", arity: -2, flags: &["readonly"], keys: ALL_KEYS, run: set::sunion },
    Command { name: "sunionstore", arity: -3, flags: &["write", "denyoom"], keys: ALL_KEYS, run: set::sunionstore },
    Command { name: "sdiff", arity: -2, flags: &["readonly"], keys: ALL_KEYS, run: set::sdiff },
    Command { name: "sdiffstore", arity: -3, flags: &["write", "denyoom"], keys: ALL_KEYS, run: set::sdiffstore },
    Command { name: "sscan", arity: -3, flags: &["readonly"], keys: ONE_KEY, run: set::sscan },
    Command { name: "lpush", arity: -3, flags: &["write", "denyoom", "fast"], keys: ONE_KEY, run: list::lpush },
    Command { name: "rpush", arity: -3, flags: &["write", "denyoom", "fast"], keys: ONE_KEY, run: list::rpush },
    Command { name: "lpop", arity: -2, flags: &["write", "fast"], keys: ONE_KEY, run: list::lpop },
    Command { name: "rpop", arity: -2, flags: &["write", "fast"], keys: ONE_KEY, run: list::rpop },
    Command { name: "lrange", arity: 4, flags: &["readonly"], keys: ONE_KEY, run: list::lrange },
    Command { name: "llen", arity: 2, flags: &["readonly", "fast"], keys: ONE_KEY, run: list::llen },
    Command { name: "lindex", arity: 3, flags: &["readonly"], keys: ONE_KEY, run: list::lindex },
    Command { name: "lset", arity: 4, flags: &["write", "denyoom"], keys: ONE_KEY, run: list::lset },
    Command { name: "linsert", arity: 5, flags: &["write", "denyoom"], keys: ONE_KEY, run: list::linsert },
    Command { name: "lrem", arity: 4, flags: &["write"], keys: ONE_KEY, run: list::lrem },
    Command { name: "ltrim", arity: 4, flags: &["write"], keys: ONE_KEY, run: list::ltrim },
    Command { name: "lpos", arity: -3, flags: &["readonly"], keys: ONE_KEY, run: list::lpos },
    Command { name: "lpushx", arity: -3, flags: &["write", "denyoom", "fast"], keys: ONE_KEY, run: list::lpushx },
    Command { name: "rpushx", arity: -3, flags: &["write", "denyoom", "fast"], keys: ONE_KEY, run: list::rpushx },
    Command { name: "lmove", arity: 5, flags: &["write", "denyoom"], keys: (1, 2, 1), run: list::lmove },
    Command { name: "rpoplpush", arity: 3, flags: &["write", "denyoom"], keys: (1, 2, 1), run: list::rpoplpush },
    Command { name: "lmpop", arity: -4, flags: &["write"], keys: NO_KEYS, run: list::lmpop },
    Command { name: "blpop", arity: -3, flags: &["write", "blocking"], keys: (1, -2, 1), run: list::blpop },
    Command { name: "brpop", arity: -3, flags: &["write", "blocking"], keys: (1, -2, 1), run: list::brpop },
    Command { name: "blmove", arity: 6, flags: &["write", "denyoom", "blocking"], keys: (1, 2, 1), run: list::blmove },
    Command { name: "brpoplpush", arity: 4, flags: &["write", "denyoom", "blocking"], keys: (1, 2, 1), run: list::brpoplpush },
    Command { name: "blmpop", arity: -5, flags: &["write", "blocking"], keys: NO_KEYS, run: list::blmpop },
    Command { name: "zadd", arity: -4, flags: &["write", "denyoom", "fast"], keys: ONE_KEY, run: zset::zadd },
    Command { name: "zscore", arity: 3, flags: &["readonly", "fast"], keys: ONE_KEY, run: zset::zscore },
    Command { name: "zrange", arity: -4, flags: &["readonly"], keys: ONE_KEY, run: zset::zrange },
    Command { name: "zincrby", arity: 4, flags: &["write", "denyoom", "fast"], keys: ONE_KEY, run: zset::zincrby },
    Command { name: "zrem", arity: -3, flags: &["write", "fast"], keys: ONE_KEY, run: zset::zrem },
    Command { name: "zcard", arity: 2, flags: &["readonly", "fast"], keys: ONE_KEY, run: zset::zcard },
    Command { name: "zrank", arity: -3, flags: &["readonly", "fast"], keys: ONE_KEY, run: zset::zrank },
    Command { name: "zrevrank", arity: -3, flags: &["readonly", "fast"], keys: ONE_KEY, run: zset::zrevrank },
    Command { name: "zrevrange", arity: -4, flags: &["readonly"], keys: ONE_KEY, run: zset::zrevrange },
    Command { name: "zrangebyscore", arity: -4, flags: &["readonly"], keys: ONE_KEY, run: zset::zrangebyscore },
    Command { name: "zrevrangebyscore", arity: -4, flags: &["readonly"], keys: ONE_KEY, run: zset::zrevrangebyscore },
    Command { name: "zrangebylex", arity: -4, flags: &["readonly"], keys: ONE_KEY, run: zset::zrangebylex },
    Command { name: "zrevrangebylex", arity: -4, flags: &["readonly"], keys: ONE_KEY, run: zset::zrevrangebylex },
    Command { name: "zrangestore", arity: -5, flags: &["write", "denyoom"], keys: (1, 2, 1), run: zset::zrangestore },
    Command { name: "zcount", arity: 4, flags: &["readonly", "fast"], keys: ONE_KEY, run: zset::zcount },
    Command { name: "zlexcount", arity: 4, flags: &["readonly", "fast"], keys: ONE_KEY, run: zset::zlexcount },
    Command { name: "zmscore", arity: -3, flags: &["readonly", "fast"], keys: ONE_KEY, run: zset::zmscore },
    Command { name: "zremrangebyrank", arity: 4, flags: &["write"], keys: ONE_KEY, run: zset::zremrangebyrank },
    Command { name: "zremrangebyscore", arity: 4, flags: &["write"], keys: ONE_KEY, run: zset::zremrangebyscore },
    Command { name: "zremrangebylex", arity: 4, flags: &["write"], keys: ONE_KEY, run: zset::zremrangebylex },
    Command { name: "zpopmin", arity: -2, flags: &["write", "fast"], keys: ONE_KEY, run: zset::zpopmin },
    Command { name: "zpopmax", arity: -2, flags: &["write", "fast"], keys: ONE_KEY, run: zset::zpopmax },
    Command { name: "zmpop", arity: -4, flags: &["write"], keys: NO_KEYS, run: zset::zmpop },
    Command { name: "bzpopmin", arity: -3, flags: &["write", "blocking", "fast"], keys: (1, -2, 1), run: zset::bzpopmin },
    Command { name: "bzpopmax", arity: -3, flags: &["write", "blocking", "fast"], keys: (1, -2, 1), run: zset::bzpopmax },
    Command { name: "bzmpop", arity: -5, flags: &["write", "blocking"], keys: NO_KEYS, run: zset::bzmpop },
    Command { name: "zrandmember", arity: -2, flags: &["readonly"], keys: ONE_KEY, run: zset::zrandmember },
    Command { name: "zscan", arity: -3, flags: &["readonly"], keys: ONE_KEY, run: zset::zscan },
    Command { name: "zunion", arity: -3, flags: &["readonly"], keys: NO_KEYS, run: zset::zunion },
    Command { name: "zinter", arity: -3, flags: &["readonly"], keys: NO_KEYS, run: zset::zinter },
    Command { name: "zdiff", arity: -3, flags: &["readonly"], keys: NO_KEYS, run: zset::zdiff },
    Command { name: "zunionstore", arity: -4, flags: &["write", "denyoom"], keys: ONE_KEY, run: zset::zunionstore },
    Command { name: "zinterstore", arity: -4, flags: &["write", "denyoom"], keys: ONE_KEY, run: zset::zinterstore },
    Command { name: "zdiffstore", arity: -4, flags: &["write", "denyoom"], keys: ONE_KEY, run: zset::zdiffstore },
    Command { name: "zintercard", arity: -3, flags: &["readonly"], keys: NO_KEYS, run: zset::zintercard },
];

/// Runs one command and writes its reply to `out`. `args` must be non-empty.
pub fn execute(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let name = args.get(0);
    session.commands += 1;
    // Most traffic: skip the table.
    if session.unrestricted {
        if args.len() == 2 && name.eq_ignore_ascii_case(b"GET") {
            return get(ctx, session, now, args, out);
        }
        if args.len() >= 3 && name.eq_ignore_ascii_case(b"SET") {
            return set(ctx, session, now, args, out);
        }
    } else if let Some(subscriber) = session.subscriber()
        && !subscriber.take(out)
    {
        // Messages that came before this command go out first; this
        // client is over its output limit.
        return Reply::Close;
    }
    // Checked in Redis' order: unknown command, arity, then authentication.
    let Some(command) = find_command(name) else {
        if ctx
            .settings
            .panic_command
            .is_some_and(|c| name.eq_ignore_ascii_case(c.as_bytes()))
        {
            panic!("{} sent: panicking as asked", String::from_utf8_lossy(name));
        }
        unknown_command(out, args);
        return Reply::Written;
    };
    if !command.arity_allows(args.len()) {
        wrong_arity(out, command.name);
        return Reply::Written;
    }
    if !session.unrestricted {
        if !session.authenticated && !command.flags.contains(&"no_auth") {
            resp::write_error(out, b"NOAUTH Authentication required.");
            return Reply::Written;
        }
        // A subscribed RESP2 client may only run the pub/sub ones, PING,
        // QUIT and RESET (but hears about an unknown subcommand first).
        if session.pubsub.is_some()
            && !session.resp3
            && !pubsub::allowed_when_subscribed(command.name)
            && pubsub::not_allowed(out, command.name, args)
        {
            return Reply::Written;
        }
    }
    let reply = (command.run)(ctx, session, now, args, out);
    if ctx.store.has_ready() {
        crate::blocking::serve_ready(ctx, now);
    }
    reply
}

/// The command called `name`, in any case. [`COMMANDS`] is split by name
/// length and first letter, so this takes a comparison or two however many
/// commands there are.
fn find_command(name: &[u8]) -> Option<&'static Command> {
    const BUCKETS: usize = 32 * 32;
    // ASCII letters land in the same bucket in either case.
    let bucket =
        |name: &[u8]| name.len().min(31) * 32 + usize::from(name.first().map_or(0, |c| c & 31));
    static INDEX: OnceLock<Vec<Vec<u16>>> = OnceLock::new();
    let index = INDEX.get_or_init(|| {
        let mut index = vec![Vec::new(); BUCKETS];
        for (i, c) in COMMANDS.iter().enumerate() {
            index[bucket(c.name.as_bytes())].push(i as u16);
        }
        index
    });
    index[bucket(name)]
        .iter()
        .map(|&i| &COMMANDS[usize::from(i)])
        .find(|c| name.eq_ignore_ascii_case(c.name.as_bytes()))
}

/// [`Store::update_key`] for updates that need no memory, so can't fail.
fn update<R>(
    store: &Store,
    key: Key<'_>,
    now: Millis,
    f: impl FnMut(Option<Current<'_>>) -> (Update, R),
) -> R {
    store
        .update_key(key, now, f)
        .unwrap_or_else(|OutOfMemory| unreachable!("only writing a value needs memory"))
}

fn get(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = &*ctx.store;
    let null = session.null();
    let (reply, hit) = store.get_key(args.key(store), now, |value| match value {
        None => {
            out.extend_from_slice(null);
            (Reply::Written, false)
        }
        Some(v) if v.kind() != Kind::String => {
            resp::write_error(out, WRONGTYPE);
            (Reply::Written, true)
        }
        Some(v) if v.as_bytes().len() <= COPY_UNDER_LOCK_MAX => {
            resp::write_bulk(out, v.as_bytes());
            (Reply::Written, true)
        }
        Some(v) => (Reply::Bulk(v.share()), true),
    });
    session.count_lookup(hit);
    reply
}

/// `MGET key [key ...]`, prefetching ahead like a pipelined batch.
fn mget(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = &*ctx.store;
    let keys: Vec<Key<'_>> = (1..args.len()).map(|i| args.key_at(store, i)).collect();
    resp::write_array_header(out, keys.len());
    for (i, &key) in keys.iter().enumerate() {
        if keys.len() > 1 {
            store.prefetch_ahead(i, |j| keys.get(j).map(Key::hash));
        }
        let null = session.null();
        let (big, hit) = store.get_key(key, now, |value| match value {
            None => {
                out.extend_from_slice(null);
                (None, false)
            }
            // Other kinds read as nil, as in Redis.
            Some(v) if v.kind() != Kind::String => {
                out.extend_from_slice(null);
                (None, true)
            }
            Some(v) if v.as_bytes().len() <= COPY_UNDER_LOCK_MAX => {
                resp::write_bulk(out, v.as_bytes());
                (None, true)
            }
            Some(v) => (Some(v.share()), true),
        });
        session.count_lookup(hit);
        if let Some(value) = big {
            resp::write_bulk(out, value.as_bytes());
        }
    }
    Reply::Written
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TtlUnit {
    Seconds,
    Millis,
    UnixSeconds,
    UnixMillis,
}

/// The unit of a `SET`/`GETEX` expiry option.
fn ttl_unit(opt: &[u8]) -> Option<TtlUnit> {
    let is = |name: &[u8]| opt.eq_ignore_ascii_case(name);
    if is(b"EX") {
        Some(TtlUnit::Seconds)
    } else if is(b"PX") {
        Some(TtlUnit::Millis)
    } else if is(b"EXAT") {
        Some(TtlUnit::UnixSeconds)
    } else if is(b"PXAT") {
        Some(TtlUnit::UnixMillis)
    } else {
        None
    }
}

/// `SET key value [NX | XX] [GET] [EX s | PX ms | EXAT ts | PXAT ts-ms | KEEPTTL]`
fn set(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = &*ctx.store;
    let mut cond = Condition::Always;
    let mut want_previous = false;
    let mut keep_ttl = false;
    let mut ttl: Option<(TtlUnit, &[u8])> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = args.get(i);
        let is = |name: &[u8]| opt.eq_ignore_ascii_case(name);
        // Repeating an option is allowed (last one wins); conflicting ones are not.
        if is(b"NX") && cond != Condition::IfPresent {
            cond = Condition::IfAbsent;
        } else if is(b"XX") && cond != Condition::IfAbsent {
            cond = Condition::IfPresent;
        } else if is(b"GET") {
            want_previous = true;
        } else if is(b"KEEPTTL") && ttl.is_none() {
            keep_ttl = true;
        } else if let Some(unit) = ttl_unit(opt)
            && !keep_ttl
            && ttl.is_none_or(|(u, _)| u == unit)
            && i + 1 < args.len()
        {
            i += 1;
            ttl = Some((unit, args.get(i)));
        } else {
            resp::write_error(out, SYNTAX_ERROR);
            return Reply::Written;
        }
        i += 1;
    }

    let expiry = match ttl {
        _ if keep_ttl => Expiry::Keep,
        None => Expiry::Never,
        Some((unit, raw)) => match parse_deadline(store, now, unit, raw, "set", out) {
            Some(deadline) => Expiry::At(deadline),
            None => return Reply::Written,
        },
    };

    let outcome = match store.set_key(
        args.key(store),
        args.get(2),
        cond,
        expiry,
        now,
        want_previous,
    ) {
        Ok(outcome) => outcome,
        Err(OutOfMemory) => {
            resp::write_error(out, OOM);
            return Reply::Written;
        }
    };
    if outcome.wrong_type {
        resp::write_error(out, WRONGTYPE);
    } else if want_previous {
        match outcome.previous {
            Some(previous) => return bulk(out, previous),
            None => out.extend_from_slice(session.null()),
        }
    } else if outcome.applied {
        out.extend_from_slice(resp::OK);
    } else {
        out.extend_from_slice(session.null());
    }
    // Any previous value is dropped here, after the shard lock was released.
    Reply::Written
}

fn setex(ctx: &Context, _: &mut Session, now: Millis, args: &Args<'_>, out: &mut Vec<u8>) -> Reply {
    set_expiring(&ctx.store, now, args, out, TtlUnit::Seconds, "setex")
}

fn psetex(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    set_expiring(&ctx.store, now, args, out, TtlUnit::Millis, "psetex")
}

/// `SETEX key seconds value` and `PSETEX key milliseconds value`.
fn set_expiring(
    store: &Store,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    unit: TtlUnit,
    name: &str,
) -> Reply {
    let Some(deadline) = parse_deadline(store, now, unit, args.get(2), name, out) else {
        return Reply::Written;
    };
    let result = store.set_key(
        args.key(store),
        args.get(3),
        Condition::Always,
        Expiry::At(deadline),
        now,
        false,
    );
    match result {
        Ok(_) => out.extend_from_slice(resp::OK),
        Err(OutOfMemory) => resp::write_error(out, OOM),
    }
    Reply::Written
}

fn setnx(ctx: &Context, _: &mut Session, now: Millis, args: &Args<'_>, out: &mut Vec<u8>) -> Reply {
    let store = &*ctx.store;
    let result = store.set_key(
        args.key(store),
        args.get(2),
        Condition::IfAbsent,
        Expiry::Never,
        now,
        false,
    );
    match result {
        Ok(outcome) => resp::write_integer(out, i64::from(outcome.applied)),
        Err(OutOfMemory) => resp::write_error(out, OOM),
    }
    Reply::Written
}

/// `MSET key value [key value ...]`. Each key is written on its own, so
/// unlike Redis, other clients can see some keys written before the rest,
/// and an OOM error can come after some were.
fn mset(ctx: &Context, _: &mut Session, now: Millis, args: &Args<'_>, out: &mut Vec<u8>) -> Reply {
    if args.len().is_multiple_of(2) {
        wrong_arity(out, "mset");
        return Reply::Written;
    }
    let store = &*ctx.store;
    let keys: Vec<Key<'_>> = (1..args.len())
        .step_by(2)
        .map(|i| args.key_at(store, i))
        .collect();
    for (i, &key) in keys.iter().enumerate() {
        if keys.len() > 1 {
            store.prefetch_ahead(i, |j| keys.get(j).map(Key::hash));
        }
        let value = args.get(2 * i + 2);
        if store
            .set_key(key, value, Condition::Always, Expiry::Never, now, false)
            .is_err()
        {
            resp::write_error(out, OOM);
            return Reply::Written;
        }
    }
    out.extend_from_slice(resp::OK);
    Reply::Written
}

/// `DEL` and `UNLINK`. Big values are always freed after the shard lock is
/// released, so the two are the same.
fn del(ctx: &Context, _: &mut Session, now: Millis, args: &Args<'_>, out: &mut Vec<u8>) -> Reply {
    let store = &*ctx.store;
    let removed: i64 = (1..args.len())
        .map(|i| {
            update(store, args.key_at(store, i), now, |current| match current {
                Some(_) => (Update::Delete, 1),
                None => (Update::Peek, 0),
            })
        })
        .sum();
    resp::write_integer(out, removed);
    Reply::Written
}

fn exists(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = &*ctx.store;
    let found: i64 = (1..args.len())
        .map(|i| {
            update(store, args.key_at(store, i), now, |current| {
                (Update::Peek, i64::from(current.is_some()))
            })
        })
        .sum();
    resp::write_integer(out, found);
    Reply::Written
}

fn getdel(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = &*ctx.store;
    let value = update(store, args.key(store), now, |current| match current {
        Some(current) if current.value.kind() != Kind::String => (Update::Touch, Err(())),
        Some(current) => (Update::Delete, Ok(Some(current.value.share()))),
        None => (Update::Peek, Ok(None)),
    });
    string_or_null(out, session, value)
}

/// `GETEX key [EX s | PX ms | EXAT ts | PXAT ts-ms | PERSIST]`
fn getex(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = &*ctx.store;
    let mut persist = false;
    let mut ttl: Option<(TtlUnit, &[u8])> = None;
    let mut i = 2;
    while i < args.len() {
        let opt = args.get(i);
        if opt.eq_ignore_ascii_case(b"PERSIST") && ttl.is_none() {
            persist = true;
        } else if let Some(unit) = ttl_unit(opt)
            && !persist
            && ttl.is_none_or(|(u, _)| u == unit)
            && i + 1 < args.len()
        {
            i += 1;
            ttl = Some((unit, args.get(i)));
        } else {
            resp::write_error(out, SYNTAX_ERROR);
            return Reply::Written;
        }
        i += 1;
    }
    let change = match ttl {
        _ if persist => Update::Expire(Expiry::Never),
        None => Update::Touch,
        Some((unit, raw)) => match parse_deadline(store, now, unit, raw, "getex", out) {
            Some(deadline) => Update::Expire(Expiry::At(deadline)),
            None => return Reply::Written,
        },
    };
    let value = update(store, args.key(store), now, |current| match current {
        Some(current) if current.value.kind() != Kind::String => (Update::Touch, Err(())),
        Some(current) => (change.clone(), Ok(Some(current.value.share()))),
        None => (Update::Peek, Ok(None)),
    });
    string_or_null(out, session, value)
}

fn expire(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    set_expiry(&ctx.store, now, args, out, TtlUnit::Seconds, "expire")
}

fn pexpire(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    set_expiry(&ctx.store, now, args, out, TtlUnit::Millis, "pexpire")
}

fn expireat(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    set_expiry(&ctx.store, now, args, out, TtlUnit::UnixSeconds, "expireat")
}

fn pexpireat(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    set_expiry(&ctx.store, now, args, out, TtlUnit::UnixMillis, "pexpireat")
}

/// `EXPIRE key seconds [NX | XX | GT | LT]` and its millisecond and Unix
/// time forms. A time already passed deletes the key.
fn set_expiry(
    store: &Store,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    unit: TtlUnit,
    name: &str,
) -> Reply {
    let (mut nx, mut xx, mut gt, mut lt) = (false, false, false, false);
    for i in 3..args.len() {
        let opt = args.get(i);
        let is = |name: &[u8]| opt.eq_ignore_ascii_case(name);
        if is(b"NX") {
            nx = true;
        } else if is(b"XX") {
            xx = true;
        } else if is(b"GT") {
            gt = true;
        } else if is(b"LT") {
            lt = true;
        } else {
            let mut msg = b"ERR Unsupported option ".to_vec();
            msg.extend_from_slice(opt);
            resp::write_error(out, &msg);
            return Reply::Written;
        }
    }
    if nx && (xx || gt || lt) {
        resp::write_error(
            out,
            b"ERR NX and XX, GT or LT options at the same time are not compatible",
        );
        return Reply::Written;
    }
    if gt && lt {
        resp::write_error(
            out,
            b"ERR GT and LT options at the same time are not compatible",
        );
        return Reply::Written;
    }
    let Some(n) = resp::parse_int(args.get(2)) else {
        resp::write_error(out, NOT_AN_INTEGER);
        return Reply::Written;
    };
    let Some(target) = expire_target(store, now, unit, n) else {
        invalid_expire_time(out, name);
        return Reply::Written;
    };
    let applied = update(store, args.key(store), now, |current| {
        let Some(current) = current else {
            return (Update::Peek, false);
        };
        let deadline = match current.expiry {
            Expiry::At(deadline) => Some(deadline),
            Expiry::Never | Expiry::Keep => None,
        };
        // A key without a TTL counts as expiring last.
        let skip = (nx && deadline.is_some())
            || (xx && deadline.is_none())
            || (gt && deadline.is_none_or(|d| target <= d))
            || (lt && deadline.is_some_and(|d| target >= d));
        if skip {
            (Update::Peek, false)
        } else {
            (Update::Expire(Expiry::At(target)), true)
        }
    });
    resp::write_integer(out, i64::from(applied));
    Reply::Written
}

/// The deadline `EXPIRE` and friends set: one at or before `now` (but never
/// 0, which means no expiry) if the time has passed. `None` on overflow.
fn expire_target(store: &Store, now: Millis, unit: TtlUnit, n: i64) -> Option<Millis> {
    let ms = match unit {
        TtlUnit::Seconds | TtlUnit::UnixSeconds => n.checked_mul(1000)?,
        TtlUnit::Millis | TtlUnit::UnixMillis => n,
    };
    match unit {
        TtlUnit::Seconds | TtlUnit::Millis => Some((now as i64).checked_add(ms)?.max(1) as Millis),
        TtlUnit::UnixSeconds | TtlUnit::UnixMillis => Some(store.deadline_from_unix_ms(now, ms)),
    }
}

fn ttl(ctx: &Context, _: &mut Session, now: Millis, args: &Args<'_>, out: &mut Vec<u8>) -> Reply {
    reply_expiry(&ctx.store, now, args, out, TtlUnit::Seconds)
}

fn pttl(ctx: &Context, _: &mut Session, now: Millis, args: &Args<'_>, out: &mut Vec<u8>) -> Reply {
    reply_expiry(&ctx.store, now, args, out, TtlUnit::Millis)
}

fn expiretime(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    reply_expiry(&ctx.store, now, args, out, TtlUnit::UnixSeconds)
}

fn pexpiretime(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    reply_expiry(&ctx.store, now, args, out, TtlUnit::UnixMillis)
}

/// `TTL`, `PTTL`, `EXPIRETIME` and `PEXPIRETIME`: -2 if the key doesn't
/// exist, -1 if it has no TTL.
fn reply_expiry(
    store: &Store,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    unit: TtlUnit,
) -> Reply {
    let expiry = update(store, args.key(store), now, |current| {
        (Update::Peek, current.map(|c| c.expiry))
    });
    let n = match expiry {
        None => -2,
        Some(Expiry::At(deadline)) => match unit {
            // Rounded, as Redis does. A live key's deadline is after `now`.
            TtlUnit::Seconds => ((deadline - now + 500) / 1000) as i64,
            TtlUnit::Millis => (deadline - now) as i64,
            TtlUnit::UnixSeconds => store.deadline_to_unix_ms(now, deadline) / 1000,
            TtlUnit::UnixMillis => store.deadline_to_unix_ms(now, deadline),
        },
        Some(Expiry::Never | Expiry::Keep) => -1,
    };
    resp::write_integer(out, n);
    Reply::Written
}

fn persist(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = &*ctx.store;
    let removed = update(store, args.key(store), now, |current| match current {
        Some(Current {
            expiry: Expiry::At(_),
            ..
        }) => (Update::Expire(Expiry::Never), true),
        _ => (Update::Peek, false),
    });
    resp::write_integer(out, i64::from(removed));
    Reply::Written
}

fn incr(ctx: &Context, _: &mut Session, now: Millis, args: &Args<'_>, out: &mut Vec<u8>) -> Reply {
    incr_by(&ctx.store, now, args, out, 1)
}

fn decr(ctx: &Context, _: &mut Session, now: Millis, args: &Args<'_>, out: &mut Vec<u8>) -> Reply {
    incr_by(&ctx.store, now, args, out, -1)
}

fn incrby(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    match resp::parse_int(args.get(2)) {
        Some(delta) => incr_by(&ctx.store, now, args, out, delta),
        None => {
            resp::write_error(out, NOT_AN_INTEGER);
            Reply::Written
        }
    }
}

fn decrby(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    match resp::parse_int(args.get(2)).map(i64::checked_neg) {
        Some(Some(delta)) => incr_by(&ctx.store, now, args, out, delta),
        Some(None) => {
            resp::write_error(out, b"ERR decrement would overflow");
            Reply::Written
        }
        None => {
            resp::write_error(out, NOT_AN_INTEGER);
            Reply::Written
        }
    }
}

/// Adds `delta` to the integer stored at the key (0 if it doesn't exist),
/// keeping its TTL.
fn incr_by(store: &Store, now: Millis, args: &Args<'_>, out: &mut Vec<u8>, delta: i64) -> Reply {
    let result = store.update_key(args.key(store), now, |current| {
        let old = match current {
            None => 0,
            Some(current) if current.value.kind() != Kind::String => {
                return (Update::Peek, Err(WRONGTYPE));
            }
            Some(current) => match resp::parse_int(current.value.as_bytes()) {
                Some(n) => n,
                None => return (Update::Peek, Err(NOT_AN_INTEGER)),
            },
        };
        match old.checked_add(delta) {
            Some(n) => (Update::SetInt(n), Ok(n)),
            None => (
                Update::Peek,
                Err(&b"ERR increment or decrement would overflow"[..]),
            ),
        }
    });
    match result {
        Ok(Ok(n)) => resp::write_integer(out, n),
        Ok(Err(msg)) => resp::write_error(out, msg),
        Err(OutOfMemory) => resp::write_error(out, OOM),
    }
    Reply::Written
}

/// `FLUSHALL` and `FLUSHDB` (there is one database), `ASYNC` or `SYNC`.
/// Freeing happens outside the shard locks either way.
fn flush(ctx: &Context, _: &mut Session, _: Millis, args: &Args<'_>, out: &mut Vec<u8>) -> Reply {
    let mode_ok =
        |arg: &[u8]| arg.eq_ignore_ascii_case(b"ASYNC") || arg.eq_ignore_ascii_case(b"SYNC");
    if args.len() > 2 || (args.len() == 2 && !mode_ok(args.get(1))) {
        resp::write_error(out, SYNTAX_ERROR);
        return Reply::Written;
    }
    ctx.store.clear();
    out.extend_from_slice(resp::OK);
    Reply::Written
}

const SNAPSHOTS_OFF: &[u8] = b"ERR snapshots are off: start Tantalum with --snapshot <file>";

/// `SAVE`: writes a snapshot now. Like Redis' it holds up its caller (and
/// this connection's I/O thread) until done.
fn save(ctx: &Context, _: &mut Session, _: Millis, _: &Args<'_>, out: &mut Vec<u8>) -> Reply {
    match &ctx.snapshots {
        None => resp::write_error(out, SNAPSHOTS_OFF),
        Some(snapshots) => match snapshots.save() {
            Ok(_) => out.extend_from_slice(resp::OK),
            Err(e) => resp::write_error(out, format!("ERR snapshot failed: {e}").as_bytes()),
        },
    }
    Reply::Written
}

/// `BGSAVE`: writes a snapshot on another thread.
fn bgsave(ctx: &Context, _: &mut Session, _: Millis, args: &Args<'_>, out: &mut Vec<u8>) -> Reply {
    match &ctx.snapshots {
        _ if args.len() > 1 => resp::write_error(out, SYNTAX_ERROR),
        None => resp::write_error(out, SNAPSHOTS_OFF),
        Some(snapshots) if snapshots.save_in_background() => {
            resp::write_simple(out, b"Background saving started");
        }
        Some(_) => resp::write_error(out, b"ERR Background save already in progress"),
    }
    Reply::Written
}

/// `LASTSAVE`: the Unix time of the last successful save, or of startup.
fn lastsave(ctx: &Context, _: &mut Session, _: Millis, _: &Args<'_>, out: &mut Vec<u8>) -> Reply {
    let time = match &ctx.snapshots {
        Some(snapshots) => snapshots.status().last_save,
        None => unix_seconds() - ctx.stats().uptime.as_secs() as i64,
    };
    resp::write_integer(out, time);
    Reply::Written
}

fn unix_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn ping(
    _: &Context,
    session: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let message = match args.len() {
        1 => None,
        2 => Some(args.get(1)),
        _ => {
            wrong_arity(out, "ping");
            return Reply::Written;
        }
    };
    if session.is_subscribed() && !session.resp3 {
        // Subscribed RESP2 clients get it in a message's shape.
        resp::write_array_header(out, 2);
        resp::write_bulk(out, b"pong");
        resp::write_bulk(out, message.unwrap_or_default());
    } else {
        match message {
            None => out.extend_from_slice(resp::PONG),
            Some(message) => resp::write_bulk(out, message),
        }
    }
    Reply::Written
}

/// `redis-cli --pipe` uses ECHO to detect when all replies have arrived.
fn echo(_: &Context, _: &mut Session, _: Millis, args: &Args<'_>, out: &mut Vec<u8>) -> Reply {
    resp::write_bulk(out, args.get(1));
    Reply::Written
}

/// Some clients send `SELECT 0` on connect. There is only one database.
fn select(_: &Context, _: &mut Session, _: Millis, args: &Args<'_>, out: &mut Vec<u8>) -> Reply {
    match resp::parse_int(args.get(1)) {
        Some(0) => out.extend_from_slice(resp::OK),
        Some(_) => resp::write_error(out, b"ERR DB index is out of range"),
        None => resp::write_error(out, NOT_AN_INTEGER),
    }
    Reply::Written
}

fn dbsize(ctx: &Context, _: &mut Session, _: Millis, _: &Args<'_>, out: &mut Vec<u8>) -> Reply {
    resp::write_integer(out, ctx.store.len() as i64);
    Reply::Written
}

fn quit(_: &Context, _: &mut Session, _: Millis, _: &Args<'_>, out: &mut Vec<u8>) -> Reply {
    out.extend_from_slice(resp::OK);
    Reply::Close
}

/// `AUTH [username] password`.
fn auth(
    ctx: &Context,
    session: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    if args.len() > 3 {
        resp::write_error(out, SYNTAX_ERROR);
        return Reply::Written;
    }
    let (user, password) = if args.len() == 2 {
        if !ctx.has_password() {
            resp::write_error(
                out,
                b"ERR AUTH <password> called without any password configured for the default \
                  user. Are you sure your configuration is correct?",
            );
            return Reply::Written;
        }
        (&b"default"[..], args.get(1))
    } else {
        (args.get(1), args.get(2))
    };
    if ctx.check_password(user, password) {
        session.set_authenticated(true);
        out.extend_from_slice(resp::OK);
    } else {
        resp::write_error(out, WRONGPASS);
    }
    Reply::Written
}

/// `HELLO [protover [AUTH username password] [SETNAME name]]`: RESP2 or
/// RESP3, which newer clients (redis-py 8) require.
fn hello(
    ctx: &Context,
    session: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let mut resp3 = session.resp3;
    if args.len() >= 2 {
        match resp::parse_int(args.get(1)) {
            Some(version @ (2 | 3)) => resp3 = version == 3,
            Some(_) => {
                resp::write_error(out, b"NOPROTO unsupported protocol version");
                return Reply::Written;
            }
            None => {
                resp::write_error(
                    out,
                    b"ERR Protocol version is not an integer or out of range",
                );
                return Reply::Written;
            }
        }
    }
    let mut credentials = None;
    let mut name = None;
    let mut i = 2;
    while i < args.len() {
        let opt = args.get(i);
        let more = args.len() - i - 1;
        if opt.eq_ignore_ascii_case(b"AUTH") && more >= 2 {
            credentials = Some((args.get(i + 1), args.get(i + 2)));
            i += 3;
        } else if opt.eq_ignore_ascii_case(b"SETNAME") && more >= 1 {
            name = Some(args.get(i + 1));
            i += 2;
        } else {
            let mut msg = b"ERR Syntax error in HELLO option '".to_vec();
            msg.extend_from_slice(opt);
            msg.push(b'\'');
            resp::write_error(out, &msg);
            return Reply::Written;
        }
    }
    if let Some((user, password)) = credentials {
        if !ctx.check_password(user, password) {
            resp::write_error(out, WRONGPASS);
            return Reply::Written;
        }
        session.set_authenticated(true);
    }
    if !session.authenticated {
        resp::write_error(
            out,
            b"NOAUTH HELLO must be called with the client already authenticated, otherwise \
              the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the \
              client and select the RESP protocol version at the same time",
        );
        return Reply::Written;
    }
    if let Some(name) = name
        && !set_client_name(session, name, out)
    {
        return Reply::Written;
    }
    session.set_resp3(resp3);
    resp::write_map_header(out, 7, resp3);
    for (field, value) in [("server", "redis"), ("version", REDIS_VERSION)] {
        resp::write_bulk(out, field.as_bytes());
        resp::write_bulk(out, value.as_bytes());
    }
    resp::write_bulk(out, b"proto");
    resp::write_integer(out, if resp3 { 3 } else { 2 });
    resp::write_bulk(out, b"id");
    resp::write_integer(out, session.id as i64);
    for (field, value) in [("mode", "standalone"), ("role", "master")] {
        resp::write_bulk(out, field.as_bytes());
        resp::write_bulk(out, value.as_bytes());
    }
    resp::write_bulk(out, b"modules");
    resp::write_array_header(out, 0);
    Reply::Written
}

/// Sets the connection's name (an empty one clears it). Returns false after
/// writing an error if it isn't valid.
fn set_client_name(session: &mut Session, name: &[u8], out: &mut Vec<u8>) -> bool {
    if !valid_client_attr(name, "Client names", out) {
        return false;
    }
    session.name = (!name.is_empty()).then(|| name.into());
    true
}

/// Checks a client name, library name or version the way Redis does:
/// printable ASCII without spaces. Returns false after writing an error.
fn valid_client_attr(value: &[u8], what: &str, out: &mut Vec<u8>) -> bool {
    let valid = value.iter().all(|b| (b'!'..=b'~').contains(b));
    if !valid {
        resp::write_error(
            out,
            format!("ERR {what} cannot contain spaces, newlines or special characters.").as_bytes(),
        );
    }
    valid
}

/// `CLIENT SETNAME | GETNAME | ID | SETINFO`: what client libraries send on
/// connect.
fn client(
    _: &Context,
    session: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let sub = args.get(1);
    let is = |name: &[u8]| sub.eq_ignore_ascii_case(name);
    let arity = |n: usize, name: &str, out: &mut Vec<u8>| {
        let ok = args.len() == n;
        if !ok {
            wrong_arity(out, &format!("client|{name}"));
        }
        ok
    };
    if is(b"SETNAME") {
        if arity(3, "setname", out) && set_client_name(session, args.get(2), out) {
            out.extend_from_slice(resp::OK);
        }
    } else if is(b"GETNAME") {
        if arity(2, "getname", out) {
            match &session.name {
                Some(name) => resp::write_bulk(out, name),
                None => out.extend_from_slice(session.null()),
            }
        }
    } else if is(b"ID") {
        if arity(2, "id", out) {
            resp::write_integer(out, session.id as i64);
        }
    } else if is(b"SETINFO") {
        if arity(4, "setinfo", out) {
            // Accepted and checked, but not kept: nothing reports them.
            let attr = args.get(2);
            let what = if attr.eq_ignore_ascii_case(b"LIB-NAME") {
                "lib-name"
            } else if attr.eq_ignore_ascii_case(b"LIB-VER") {
                "lib-ver"
            } else {
                let mut msg = b"ERR Unrecognized option '".to_vec();
                msg.extend_from_slice(attr);
                msg.push(b'\'');
                resp::write_error(out, &msg);
                return Reply::Written;
            };
            if valid_client_attr(args.get(3), what, out) {
                out.extend_from_slice(resp::OK);
            }
        }
    } else {
        unknown_subcommand(out, sub, "CLIENT");
    }
    Reply::Written
}

/// `COMMAND`, `COMMAND COUNT | INFO [name ...] | LIST | DOCS`: enough for
/// clients and `redis-cli` to discover what's supported.
fn command(
    _: &Context,
    session: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let resp3 = session.resp3;
    if args.len() == 1 {
        resp::write_array_header(out, COMMANDS.len());
        for c in COMMANDS {
            write_command_info(out, c, resp3);
        }
        return Reply::Written;
    }
    let sub = args.get(1);
    let is = |name: &[u8]| sub.eq_ignore_ascii_case(name);
    if is(b"COUNT") && args.len() == 2 {
        resp::write_integer(out, COMMANDS.len() as i64);
    } else if is(b"LIST") && args.len() == 2 {
        resp::write_array_header(out, COMMANDS.len());
        for c in COMMANDS {
            resp::write_bulk(out, c.name.as_bytes());
        }
    } else if is(b"INFO") && args.len() == 2 {
        resp::write_array_header(out, COMMANDS.len());
        for c in COMMANDS {
            write_command_info(out, c, resp3);
        }
    } else if is(b"INFO") {
        resp::write_array_header(out, args.len() - 2);
        for i in 2..args.len() {
            let name = args.get(i);
            match find_command(name) {
                Some(c) => write_command_info(out, c, resp3),
                None => out.extend_from_slice(session.null()),
            }
        }
    } else if is(b"DOCS") {
        // No docs, which redis-cli is fine with.
        resp::write_map_header(out, 0, resp3);
    } else if is(b"COUNT") || is(b"LIST") {
        wrong_arity(
            out,
            &format!("command|{}", sub.to_ascii_lowercase().escape_ascii()),
        );
    } else {
        unknown_subcommand(out, sub, "COMMAND");
    }
    Reply::Written
}

/// One command's entry in a `COMMAND` reply, in Redis 7's 10-field shape.
fn write_command_info(out: &mut Vec<u8>, c: &Command, resp3: bool) {
    resp::write_array_header(out, 10);
    resp::write_bulk(out, c.name.as_bytes());
    resp::write_integer(out, i64::from(c.arity));
    resp::write_set_header(out, c.flags.len(), resp3);
    for flag in c.flags {
        resp::write_simple(out, flag.as_bytes());
    }
    let (first, last, step) = c.keys;
    for n in [first, last, step] {
        resp::write_integer(out, i64::from(n));
    }
    // ACL categories and tips (sets), key specs and subcommands (arrays).
    resp::write_set_header(out, 0, resp3);
    resp::write_set_header(out, 0, resp3);
    resp::write_array_header(out, 0);
    resp::write_array_header(out, 0);
}

/// `CONFIG GET pattern [pattern ...]`, for clients and tools that read the
/// settings. Nothing can be changed at runtime.
fn config(
    ctx: &Context,
    session: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let sub = args.get(1);
    if sub.eq_ignore_ascii_case(b"GET") {
        if args.len() < 3 {
            wrong_arity(out, "config|get");
            return Reply::Written;
        }
        let store = &*ctx.store;
        let settings = [
            ("maxmemory", store.maxmemory().to_string()),
            ("maxmemory-policy", store.policy().name().to_string()),
            ("databases", "1".to_string()),
            // In Redis' "seconds changes" form: save after this long if
            // anything changed.
            (
                "save",
                ctx.snapshots
                    .as_ref()
                    .and_then(|s| s.interval)
                    .map_or(String::new(), |i| format!("{} 1", i.as_secs())),
            ),
            ("appendonly", "no".to_string()),
        ];
        let matching: Vec<_> = settings
            .iter()
            .filter(|(name, _)| {
                (2..args.len()).any(|i| glob::matches(args.get(i), name.as_bytes(), true))
            })
            .collect();
        resp::write_map_header(out, matching.len(), session.resp3);
        for (name, value) in matching {
            resp::write_bulk(out, name.as_bytes());
            resp::write_bulk(out, value.as_bytes());
        }
    } else if sub.eq_ignore_ascii_case(b"SET") {
        resp::write_error(
            out,
            b"ERR CONFIG SET is not supported: restart with the option instead",
        );
    } else {
        unknown_subcommand(out, sub, "CONFIG");
    }
    Reply::Written
}

/// A string read by `GETDEL` or `GETEX`: `Err` when the key held another kind.
fn string_or_null(
    out: &mut Vec<u8>,
    session: &mut Session,
    value: Result<Option<SharedValue>, ()>,
) -> Reply {
    session.count_lookup(!matches!(value, Ok(None)));
    match value {
        Ok(value) => bulk_or_null(out, session, value),
        Err(()) => {
            resp::write_error(out, WRONGTYPE);
            Reply::Written
        }
    }
}

/// `TYPE key`
fn type_of(
    ctx: &Context,
    _: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = &*ctx.store;
    let kind = update(store, args.key(store), now, |current| {
        (Update::Peek, current.map(|c| c.value.kind()))
    });
    resp::write_simple(out, kind.map_or("none", Kind::name).as_bytes());
    Reply::Written
}

/// A `SCAN`-family cursor as Redis reads it (with C's `strtoul`): decimal
/// digits after an optional sign (a minus wraps around), in range, or
/// nothing at all (0), up to any NUL byte.
fn parse_cursor(cursor: &[u8]) -> Option<u64> {
    let cursor = cursor.split(|&b| b == 0).next().unwrap_or_default();
    let (negative, digits) = match cursor.first() {
        None => return Some(0),
        Some(b'-') => (true, &cursor[1..]),
        Some(b'+') => (false, &cursor[1..]),
        Some(_) => (false, cursor),
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let n: u64 = std::str::from_utf8(digits).ok()?.parse().ok()?;
    Some(if negative { n.wrapping_neg() } else { n })
}

/// Which command's options [`ScanOptions::parse`] reads.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Scan {
    /// `SCAN`, which also takes `TYPE`.
    Keys,
    /// `HSCAN`, which also takes `NOVALUES`.
    Hash,
    /// `SSCAN` and `ZSCAN`.
    Other,
}

/// `MATCH`, `COUNT` and `TYPE` or `NOVALUES`.
struct ScanOptions<'a> {
    /// `None` for no pattern, or a lone `*`.
    pattern: Option<&'a [u8]>,
    count: usize,
    type_name: Option<&'a [u8]>,
    values: bool,
}

impl<'a> ScanOptions<'a> {
    /// Reads the options from argument `from` on, as Redis does: the last
    /// of a repeated option wins. Writes the error if they're bad.
    fn parse(args: &Args<'a>, from: usize, scan: Scan, out: &mut Vec<u8>) -> Option<Self> {
        let mut options = ScanOptions {
            pattern: None,
            count: 10,
            type_name: None,
            values: true,
        };
        let mut i = from;
        while i < args.len() {
            let opt = args.get(i);
            let is = |name: &[u8]| opt.eq_ignore_ascii_case(name);
            let has_value = i + 1 < args.len();
            if is(b"COUNT") && has_value {
                match resp::parse_int(args.get(i + 1)) {
                    None => {
                        resp::write_error(out, NOT_AN_INTEGER);
                        return None;
                    }
                    Some(count) if count < 1 => {
                        resp::write_error(out, SYNTAX_ERROR);
                        return None;
                    }
                    Some(count) => options.count = usize::try_from(count).unwrap_or(usize::MAX),
                }
                i += 2;
            } else if is(b"MATCH") && has_value {
                options.pattern = Some(args.get(i + 1)).filter(|p| *p != b"*");
                i += 2;
            } else if is(b"TYPE") && scan == Scan::Keys && has_value {
                options.type_name = Some(args.get(i + 1));
                i += 2;
            } else if is(b"NOVALUES") {
                if scan != Scan::Hash {
                    // As Redis 7.4, which added it.
                    resp::write_error(out, b"ERR NOVALUES option can only be used in HSCAN");
                    return None;
                }
                options.values = false;
                i += 1;
            } else {
                resp::write_error(out, SYNTAX_ERROR);
                return None;
            }
        }
        Some(options)
    }

    /// Whether a key or field passes `MATCH`.
    fn matches(&self, name: &[u8]) -> bool {
        self.pattern.is_none_or(|p| glob::matches(p, name, false))
    }

    /// Whether a key of this kind passes `TYPE`. Names of types Tantalum
    /// doesn't have match nothing, as in Redis 7.
    fn has_type(&self, kind: Kind) -> bool {
        self.type_name
            .is_none_or(|t| t.eq_ignore_ascii_case(kind.name().as_bytes()))
    }
}

fn bulk_or_null(out: &mut Vec<u8>, session: &Session, value: Option<SharedValue>) -> Reply {
    match value {
        Some(value) => bulk(out, value),
        None => {
            out.extend_from_slice(session.null());
            Reply::Written
        }
    }
}

fn bulk(out: &mut Vec<u8>, value: SharedValue) -> Reply {
    if value.as_bytes().len() <= COPY_UNDER_LOCK_MAX {
        resp::write_bulk(out, value.as_bytes());
        Reply::Written
    } else {
        Reply::Bulk(value)
    }
}

/// Parses an expiry argument as `SET` and `GETEX` take it: must be positive.
/// Writes the error and returns `None` if it isn't valid.
fn parse_deadline(
    store: &Store,
    now: Millis,
    unit: TtlUnit,
    raw: &[u8],
    name: &str,
    out: &mut Vec<u8>,
) -> Option<Millis> {
    let Some(n) = resp::parse_int(raw) else {
        resp::write_error(out, NOT_AN_INTEGER);
        return None;
    };
    let deadline = deadline(store, now, unit, n);
    if deadline.is_none() {
        invalid_expire_time(out, name);
    }
    deadline
}

fn deadline(store: &Store, now: Millis, unit: TtlUnit, n: i64) -> Option<Millis> {
    if n <= 0 {
        return None;
    }
    let ms = match unit {
        TtlUnit::Seconds | TtlUnit::UnixSeconds => n.checked_mul(1000)?,
        TtlUnit::Millis | TtlUnit::UnixMillis => n,
    };
    match unit {
        TtlUnit::Seconds | TtlUnit::Millis => now
            .checked_add(ms as u64)
            .filter(|&deadline| deadline <= MAX_DEADLINE),
        TtlUnit::UnixSeconds | TtlUnit::UnixMillis => Some(store.deadline_from_unix_ms(now, ms)),
    }
}

fn invalid_expire_time(out: &mut Vec<u8>, name: &str) {
    resp::write_error(
        out,
        format!("ERR invalid expire time in '{name}' command").as_bytes(),
    );
}

/// `INFO` in Redis' `key:value` format, with the fields tools commonly read.
/// Sections are accepted but everything is always returned.
fn info(ctx: &Context, session: &mut Session, _: Millis, _: &Args<'_>, out: &mut Vec<u8>) -> Reply {
    use std::fmt::Write;
    // Include this connection's latest counts.
    ctx.publish(session);
    let server = ctx.stats();
    let stats = ctx.store.stats();
    let mut s = String::new();
    let _ = write!(
        s,
        "# Server\r\nredis_version:{REDIS_VERSION}\r\ntantalum_version:{}\r\nredis_mode:standalone\r\n\
         process_id:{}\r\nuptime_in_seconds:{}\r\n\r\n\
         # Clients\r\nconnected_clients:{}\r\nmaxclients:{}\r\nblocked_clients:{}\r\n\r\n\
         # Memory\r\nused_memory:{}\r\n",
        env!("CARGO_PKG_VERSION"),
        std::process::id(),
        server.uptime.as_secs(),
        server.connected_clients,
        ctx.settings.maxclients,
        ctx.store.blocked_clients(),
        stats.used_memory,
    );
    // Where used_memory goes: packed entries (and how much of that is live),
    // values too big to pack, hash tables, and connection buffers.
    let _ = write!(
        s,
        "segment_bytes:{}\r\nsegment_live_bytes:{}\r\nexternal_bytes:{}\r\ntable_bytes:{}\r\n\
         mem_clients_normal:{}\r\n",
        stats.segment_bytes,
        stats.live_bytes,
        stats.external_bytes,
        stats.table_bytes,
        stats.client_bytes,
    );
    if let Some(rss) = rss_bytes() {
        let _ = write!(s, "used_memory_rss:{rss}\r\n");
    }
    let _ = write!(
        s,
        "maxmemory:{}\r\nmaxmemory_policy:{}\r\n\r\n",
        stats.maxmemory,
        stats.policy.name(),
    );
    // Redis' field names, so monitoring tools find them.
    let _ = write!(s, "# Persistence\r\nloading:0\r\n");
    match &ctx.snapshots {
        Some(snapshots) => {
            let status = snapshots.status();
            let _ = write!(
                s,
                "snapshots:on\r\nsnapshot_file:{}\r\nrdb_changes_since_last_save:{}\r\n\
                 rdb_bgsave_in_progress:{}\r\nrdb_last_save_time:{}\r\nrdb_last_bgsave_status:{}\r\n\r\n",
                snapshots.path.display(),
                snapshots.unsaved_changes(),
                u8::from(snapshots.saving_in_background()),
                status.last_save,
                if status.last_ok { "ok" } else { "err" },
            );
        }
        None => {
            let _ = write!(s, "snapshots:off\r\n\r\n");
        }
    }
    let _ = write!(
        s,
        "# Stats\r\ntotal_connections_received:{}\r\ntotal_commands_processed:{}\r\n\
         rejected_connections:{}\r\nexpired_keys:{}\r\nevicted_keys:{}\r\n\
         keyspace_hits:{}\r\nkeyspace_misses:{}\r\npubsub_channels:{}\r\npubsub_patterns:{}\r\n\
         pubsubshard_channels:{}\r\n\r\n\
         # Keyspace\r\n",
        server.total_connections,
        server.total_commands,
        server.rejected_connections,
        stats.expired_keys,
        stats.evicted_keys,
        server.keyspace_hits,
        server.keyspace_misses,
        ctx.pubsub.channel_count(false),
        ctx.pubsub.pattern_count(),
        ctx.pubsub.channel_count(true),
    );
    if stats.keys > 0 {
        let _ = write!(
            s,
            "db0:keys={},expires={},avg_ttl=0\r\n",
            stats.keys, stats.keys_with_ttl
        );
    }
    resp::write_verbatim_text(out, s.as_bytes(), session.resp3);
    Reply::Written
}

#[cfg(target_os = "linux")]
fn rss_bytes() -> Option<usize> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
    let kb: usize = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb * 1024)
}

#[cfg(not(target_os = "linux"))]
fn rss_bytes() -> Option<usize> {
    None
}

const OOM: &[u8] = b"OOM command not allowed when used memory > 'maxmemory'.";
const NOT_AN_INTEGER: &[u8] = b"ERR value is not an integer or out of range";
const SYNTAX_ERROR: &[u8] = b"ERR syntax error";
const WRONGTYPE: &[u8] = b"WRONGTYPE Operation against a key holding the wrong kind of value";
const INVALID_CURSOR: &[u8] = b"ERR invalid cursor";
const WRONGPASS: &[u8] = b"WRONGPASS invalid username-password pair or user is disabled.";

fn wrong_arity(out: &mut Vec<u8>, name: &str) {
    resp::write_error(
        out,
        format!("ERR wrong number of arguments for '{name}' command").as_bytes(),
    );
}

fn unknown_subcommand(out: &mut Vec<u8>, sub: &[u8], command: &str) {
    let mut msg = b"ERR unknown subcommand '".to_vec();
    msg.extend_from_slice(&sub[..sub.len().min(128)]);
    msg.extend_from_slice(format!("'. Try {command} HELP.").as_bytes());
    resp::write_error(out, &msg);
}

/// Same wording and truncation as Redis.
fn unknown_command(out: &mut Vec<u8>, args: &Args<'_>) {
    let name = args.get(0);
    let mut msg = b"ERR unknown command '".to_vec();
    msg.extend_from_slice(&name[..name.len().min(128)]);
    msg.extend_from_slice(b"', with args beginning with: ");
    let mut listed = Vec::new();
    for i in 1..args.len() {
        if listed.len() >= 128 {
            break;
        }
        let arg = args.get(i);
        let room = 128 - listed.len();
        listed.push(b'\'');
        listed.extend_from_slice(&arg[..arg.len().min(room)]);
        listed.extend_from_slice(b"' ");
    }
    msg.extend_from_slice(&listed);
    resp::write_error(out, &msg);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursors_parse_like_strtoul() {
        for (cursor, parsed) in [
            ("0", Some(0)),
            ("", Some(0)),
            ("123", Some(123)),
            ("+5", Some(5)),
            ("-1", Some(u64::MAX)),
            ("-0", Some(0)),
            ("007", Some(7)),
            ("18446744073709551615", Some(u64::MAX)),
            ("18446744073709551616", None),
            ("-", None),
            ("+", None),
            (" 1", None),
            ("1 ", None),
            ("0x10", None),
            ("abc", None),
            ("12\0junk", Some(12)),
        ] {
            assert_eq!(parse_cursor(cursor.as_bytes()), parsed, "{cursor:?}");
        }
    }

    #[test]
    fn finds_every_command_in_any_case() {
        for c in COMMANDS {
            let upper = c.name.to_ascii_uppercase();
            for name in [c.name.as_bytes(), upper.as_bytes()] {
                assert!(std::ptr::eq(find_command(name).unwrap(), c), "{}", c.name);
            }
        }
        for name in [&b""[..], b"g", b"gett", b"nope", b"\xff", &[b'h'; 40]] {
            assert!(find_command(name).is_none());
        }
    }

    #[test]
    fn command_table_is_consistent() {
        for (i, c) in COMMANDS.iter().enumerate() {
            assert_eq!(c.name, c.name.to_ascii_lowercase());
            assert!(
                COMMANDS[..i].iter().all(|other| other.name != c.name),
                "{} listed twice",
                c.name
            );
            assert_ne!(c.arity, 0, "{}", c.name);
        }
    }
}
