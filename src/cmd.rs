//! Command dispatch. Replies match Redis 7 byte for byte where practical.

mod admin;
mod bit;
mod eval;
mod group;
mod hash;
mod keys;
mod list;
mod multi;
mod pubsub;
mod set;
mod sort;
mod stream;
mod string;
mod zset;

use std::ops::Range;
use std::sync::{Arc, OnceLock};

pub use crate::blocking::Blocked;
use crate::command_table;
use crate::context::{Context, KillFilter, ReplyMode, Session};
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
    /// `CLIENT PAUSE` holds it back: nothing's run. The connection sends what it
    /// has, and runs it again in a moment.
    Paused,
}

type Handler = fn(&Context, &mut Session, Millis, &Args<'_>, &mut Vec<u8>) -> Reply;

/// A command's handler and what `COMMAND` reports about it.
struct Command {
    /// Lowercase, as Redis reports it.
    name: &'static str,
    /// Number of arguments including the name; negative means at least that.
    arity: i32,
    flags: &'static [&'static str],
    /// Redis' `fast`: not timed for `SLOWLOG`, unless asked for below 10 ms.
    fast: bool,
    /// First key, last key (negative counts from the end) and the step
    /// between keys; zeros for commands without keys.
    keys: (i32, i32, i32),
    run: Handler,
}

impl Command {
    /// Whether it may change the data (Redis' `write` flag).
    fn writes(&self) -> bool {
        self.flags.contains(&"write")
    }

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
    Command { name: "get", arity: 2, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: get },
    Command { name: "set", arity: -3, flags: &["write", "denyoom"], fast: false, keys: ONE_KEY, run: set },
    Command { name: "mget", arity: -2, flags: &["readonly", "fast"], fast: true, keys: ALL_KEYS, run: mget },
    Command { name: "mset", arity: -3, flags: &["write", "denyoom"], fast: false, keys: (1, -1, 2), run: mset },
    Command { name: "del", arity: -2, flags: &["write"], fast: false, keys: ALL_KEYS, run: del },
    Command { name: "unlink", arity: -2, flags: &["write", "fast"], fast: true, keys: ALL_KEYS, run: del },
    Command { name: "exists", arity: -2, flags: &["readonly", "fast"], fast: true, keys: ALL_KEYS, run: exists },
    Command { name: "expire", arity: -3, flags: &["write", "fast"], fast: true, keys: ONE_KEY, run: expire },
    Command { name: "pexpire", arity: -3, flags: &["write", "fast"], fast: true, keys: ONE_KEY, run: pexpire },
    Command { name: "ttl", arity: 2, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: ttl },
    Command { name: "pttl", arity: 2, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: pttl },
    Command { name: "incr", arity: 2, flags: &["write", "denyoom", "fast"], fast: true, keys: ONE_KEY, run: incr },
    Command { name: "incrby", arity: 3, flags: &["write", "denyoom", "fast"], fast: true, keys: ONE_KEY, run: incrby },
    Command { name: "decr", arity: 2, flags: &["write", "denyoom", "fast"], fast: true, keys: ONE_KEY, run: decr },
    Command { name: "decrby", arity: 3, flags: &["write", "denyoom", "fast"], fast: true, keys: ONE_KEY, run: decrby },
    Command { name: "setex", arity: 4, flags: &["write", "denyoom"], fast: false, keys: ONE_KEY, run: setex },
    Command { name: "psetex", arity: 4, flags: &["write", "denyoom"], fast: false, keys: ONE_KEY, run: psetex },
    Command { name: "setnx", arity: 3, flags: &["write", "denyoom", "fast"], fast: true, keys: ONE_KEY, run: setnx },
    Command { name: "setbit", arity: 4, flags: &["write", "denyoom"], fast: false, keys: ONE_KEY, run: bit::setbit },
    Command { name: "getbit", arity: 3, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: bit::getbit },
    Command { name: "bitcount", arity: -2, flags: &["readonly"], fast: false, keys: ONE_KEY, run: bit::bitcount },
    Command { name: "bitpos", arity: -3, flags: &["readonly"], fast: false, keys: ONE_KEY, run: bit::bitpos },
    Command { name: "bitop", arity: -4, flags: &["write", "denyoom"], fast: false, keys: (2, -1, 1), run: bit::bitop },
    Command { name: "bitfield", arity: -2, flags: &["write", "denyoom"], fast: false, keys: ONE_KEY, run: bit::bitfield },
    Command { name: "bitfield_ro", arity: -2, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: bit::bitfield_ro },
    Command { name: "strlen", arity: 2, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: string::strlen },
    Command { name: "append", arity: 3, flags: &["write", "denyoom", "fast"], fast: true, keys: ONE_KEY, run: string::append },
    Command { name: "getrange", arity: 4, flags: &["readonly"], fast: false, keys: ONE_KEY, run: string::getrange },
    Command { name: "substr", arity: 4, flags: &["readonly"], fast: false, keys: ONE_KEY, run: string::getrange },
    Command { name: "setrange", arity: 4, flags: &["write", "denyoom"], fast: false, keys: ONE_KEY, run: string::setrange },
    Command { name: "getset", arity: 3, flags: &["write", "denyoom", "fast"], fast: true, keys: ONE_KEY, run: string::getset },
    Command { name: "incrbyfloat", arity: 3, flags: &["write", "denyoom", "fast"], fast: true, keys: ONE_KEY, run: string::incrbyfloat },
    Command { name: "msetnx", arity: -3, flags: &["write", "denyoom"], fast: false, keys: (1, -1, 2), run: string::msetnx },
    Command { name: "lcs", arity: -3, flags: &["readonly"], fast: false, keys: (1, 1, 1), run: string::lcs },
    Command { name: "getdel", arity: 2, flags: &["write", "fast"], fast: true, keys: ONE_KEY, run: getdel },
    Command { name: "getex", arity: -2, flags: &["write", "fast"], fast: true, keys: ONE_KEY, run: getex },
    Command { name: "expireat", arity: -3, flags: &["write", "fast"], fast: true, keys: ONE_KEY, run: expireat },
    Command { name: "pexpireat", arity: -3, flags: &["write", "fast"], fast: true, keys: ONE_KEY, run: pexpireat },
    Command { name: "expiretime", arity: 2, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: expiretime },
    Command { name: "pexpiretime", arity: 2, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: pexpiretime },
    Command { name: "persist", arity: 2, flags: &["write", "fast"], fast: true, keys: ONE_KEY, run: persist },
    Command { name: "ping", arity: -1, flags: &["fast"], fast: true, keys: NO_KEYS, run: ping },
    Command { name: "echo", arity: 2, flags: &["fast"], fast: true, keys: NO_KEYS, run: echo },
    Command { name: "select", arity: 2, flags: &["loading", "stale", "fast"], fast: true, keys: NO_KEYS, run: select },
    Command { name: "dbsize", arity: 1, flags: &["readonly", "fast"], fast: true, keys: NO_KEYS, run: dbsize },
    Command { name: "info", arity: -1, flags: &["loading", "stale"], fast: false, keys: NO_KEYS, run: info },
    Command { name: "auth", arity: -2, flags: &["noscript", "loading", "stale", "fast", "no_auth"], fast: true, keys: NO_KEYS, run: auth },
    Command { name: "hello", arity: -1, flags: &["noscript", "loading", "stale", "fast", "no_auth"], fast: true, keys: NO_KEYS, run: hello },
    Command { name: "client", arity: -2, flags: &[], fast: false, keys: NO_KEYS, run: client },
    Command { name: "command", arity: -1, flags: &["loading", "stale"], fast: false, keys: NO_KEYS, run: command },
    Command { name: "config", arity: -2, flags: &[], fast: false, keys: NO_KEYS, run: admin::config },
    Command { name: "flushall", arity: -1, flags: &["write"], fast: false, keys: NO_KEYS, run: flush },
    Command { name: "save", arity: 1, flags: &["admin", "noscript"], fast: false, keys: NO_KEYS, run: save },
    Command { name: "bgsave", arity: -1, flags: &["admin", "noscript"], fast: false, keys: NO_KEYS, run: bgsave },
    Command { name: "bgrewriteaof", arity: 1, flags: &["admin", "noscript", "no_async_loading"], fast: false, keys: NO_KEYS, run: bgrewriteaof },
    Command { name: "lastsave", arity: 1, flags: &["loading", "stale", "fast"], fast: true, keys: NO_KEYS, run: lastsave },
    Command { name: "flushdb", arity: -1, flags: &["write"], fast: false, keys: NO_KEYS, run: flush },
    Command { name: "quit", arity: -1, flags: &["noscript", "loading", "stale", "fast", "no_auth"], fast: true, keys: NO_KEYS, run: quit },
    Command { name: "multi", arity: 1, flags: &["noscript", "loading", "stale", "fast", "allow_busy"], fast: true, keys: NO_KEYS, run: multi::multi },
    Command { name: "exec", arity: 1, flags: &["noscript", "loading", "stale", "skip_slowlog"], fast: false, keys: NO_KEYS, run: multi::exec },
    Command { name: "discard", arity: 1, flags: &["noscript", "loading", "stale", "fast", "allow_busy"], fast: true, keys: NO_KEYS, run: multi::discard },
    Command { name: "watch", arity: -2, flags: &["noscript", "loading", "stale", "fast", "allow_busy"], fast: true, keys: (1, -1, 1), run: multi::watch },
    Command { name: "unwatch", arity: 1, flags: &["noscript", "loading", "stale", "fast", "allow_busy"], fast: true, keys: NO_KEYS, run: multi::unwatch },
    Command { name: "time", arity: 1, flags: &["loading", "stale", "fast"], fast: true, keys: NO_KEYS, run: time },
    Command { name: "eval", arity: -3, flags: &["noscript", "stale", "skip_monitor", "no_mandatory_keys", "movablekeys"], fast: false, keys: NO_KEYS, run: eval::eval },
    Command { name: "evalsha", arity: -3, flags: &["noscript", "stale", "skip_monitor", "no_mandatory_keys", "movablekeys"], fast: false, keys: NO_KEYS, run: eval::evalsha },
    Command { name: "eval_ro", arity: -3, flags: &["readonly", "noscript", "stale", "skip_monitor", "no_mandatory_keys", "movablekeys"], fast: false, keys: NO_KEYS, run: eval::eval_ro },
    Command { name: "evalsha_ro", arity: -3, flags: &["readonly", "noscript", "stale", "skip_monitor", "no_mandatory_keys", "movablekeys"], fast: false, keys: NO_KEYS, run: eval::evalsha_ro },
    Command { name: "script", arity: -2, flags: &[], fast: false, keys: NO_KEYS, run: eval::script },
    Command { name: "reset", arity: 1, flags: &["noscript", "loading", "stale", "fast", "no_auth", "allow_busy"], fast: true, keys: NO_KEYS, run: pubsub::reset },
    Command { name: "subscribe", arity: -2, flags: &["pubsub", "noscript", "loading", "stale"], fast: false, keys: NO_KEYS, run: pubsub::subscribe },
    Command { name: "unsubscribe", arity: -1, flags: &["pubsub", "noscript", "loading", "stale"], fast: false, keys: NO_KEYS, run: pubsub::unsubscribe },
    Command { name: "psubscribe", arity: -2, flags: &["pubsub", "noscript", "loading", "stale"], fast: false, keys: NO_KEYS, run: pubsub::psubscribe },
    Command { name: "punsubscribe", arity: -1, flags: &["pubsub", "noscript", "loading", "stale"], fast: false, keys: NO_KEYS, run: pubsub::punsubscribe },
    Command { name: "ssubscribe", arity: -2, flags: &["pubsub", "noscript", "loading", "stale"], fast: false, keys: (1, -1, 1), run: pubsub::ssubscribe },
    Command { name: "sunsubscribe", arity: -1, flags: &["pubsub", "noscript", "loading", "stale"], fast: false, keys: (1, -1, 1), run: pubsub::sunsubscribe },
    Command { name: "publish", arity: 3, flags: &["pubsub", "loading", "stale", "fast"], fast: true, keys: NO_KEYS, run: pubsub::publish },
    Command { name: "spublish", arity: 3, flags: &["pubsub", "loading", "stale", "fast"], fast: true, keys: ONE_KEY, run: pubsub::spublish },
    Command { name: "pubsub", arity: -2, flags: &[], fast: false, keys: NO_KEYS, run: pubsub::pubsub },
    Command { name: "type", arity: 2, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: type_of },
    Command { name: "scan", arity: -2, flags: &["readonly"], fast: false, keys: NO_KEYS, run: keys::scan },
    Command { name: "keys", arity: 2, flags: &["readonly"], fast: false, keys: NO_KEYS, run: keys::keys },
    Command { name: "rename", arity: 3, flags: &["write"], fast: false, keys: (1, 2, 1), run: keys::rename },
    Command { name: "renamenx", arity: 3, flags: &["write", "fast"], fast: true, keys: (1, 2, 1), run: keys::renamenx },
    Command { name: "touch", arity: -2, flags: &["readonly", "fast"], fast: true, keys: ALL_KEYS, run: keys::touch },
    Command { name: "randomkey", arity: 1, flags: &["readonly"], fast: false, keys: NO_KEYS, run: keys::randomkey },
    Command { name: "sort", arity: -2, flags: &["write", "denyoom"], fast: false, keys: ONE_KEY, run: sort::sort },
    Command { name: "sort_ro", arity: -2, flags: &["readonly"], fast: false, keys: ONE_KEY, run: sort::sort_ro },
    Command { name: "copy", arity: -3, flags: &["write", "denyoom"], fast: false, keys: (1, 2, 1), run: keys::copy },
    Command { name: "monitor", arity: 1, flags: &["admin", "noscript", "loading", "stale"], fast: false, keys: NO_KEYS, run: admin::monitor },
    Command { name: "slowlog", arity: -2, flags: &[], fast: false, keys: NO_KEYS, run: admin::slowlog },
    Command { name: "memory", arity: -2, flags: &[], fast: false, keys: NO_KEYS, run: admin::memory },
    Command { name: "wait", arity: 3, flags: &[], fast: false, keys: NO_KEYS, run: admin::wait },
    Command { name: "waitaof", arity: 4, flags: &["noscript"], fast: false, keys: NO_KEYS, run: admin::waitaof },
    Command { name: "role", arity: 1, flags: &["noscript", "loading", "stale", "fast"], fast: true, keys: NO_KEYS, run: admin::role },
    Command { name: "replicaof", arity: 3, flags: &["admin", "noscript", "stale", "no_async_loading"], fast: false, keys: NO_KEYS, run: admin::replicaof },
    Command { name: "slaveof", arity: 3, flags: &["admin", "noscript", "stale", "no_async_loading"], fast: false, keys: NO_KEYS, run: admin::replicaof },
    Command { name: "lolwut", arity: -1, flags: &["readonly", "fast"], fast: true, keys: NO_KEYS, run: admin::lolwut },
    Command { name: "module", arity: -2, flags: &[], fast: false, keys: NO_KEYS, run: admin::module },
    Command { name: "latency", arity: -2, flags: &[], fast: false, keys: NO_KEYS, run: admin::latency },
    Command { name: "debug", arity: -2, flags: &["admin", "noscript", "loading", "stale"], fast: false, keys: NO_KEYS, run: admin::debug },
    Command { name: "move", arity: 3, flags: &["write", "fast"], fast: true, keys: (1, 1, 1), run: keys::move_key },
    Command { name: "swapdb", arity: 3, flags: &["write", "fast"], fast: true, keys: NO_KEYS, run: keys::swapdb },
    Command { name: "object", arity: -2, flags: &[], fast: false, keys: NO_KEYS, run: keys::object },
    Command { name: "hget", arity: 3, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: hash::hget },
    Command { name: "hset", arity: -4, flags: &["write", "denyoom", "fast"], fast: true, keys: ONE_KEY, run: hash::hset },
    Command { name: "hgetall", arity: 2, flags: &["readonly"], fast: false, keys: ONE_KEY, run: hash::hgetall },
    Command { name: "hdel", arity: -3, flags: &["write", "fast"], fast: true, keys: ONE_KEY, run: hash::hdel },
    Command { name: "hmget", arity: -3, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: hash::hmget },
    Command { name: "hincrby", arity: 4, flags: &["write", "denyoom", "fast"], fast: true, keys: ONE_KEY, run: hash::hincrby },
    Command { name: "hincrbyfloat", arity: 4, flags: &["write", "denyoom", "fast"], fast: true, keys: ONE_KEY, run: hash::hincrbyfloat },
    Command { name: "hrandfield", arity: -2, flags: &["readonly"], fast: false, keys: ONE_KEY, run: hash::hrandfield },
    Command { name: "hexists", arity: 3, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: hash::hexists },
    Command { name: "hlen", arity: 2, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: hash::hlen },
    Command { name: "hmset", arity: -4, flags: &["write", "denyoom", "fast"], fast: true, keys: ONE_KEY, run: hash::hmset },
    Command { name: "hsetnx", arity: 4, flags: &["write", "denyoom", "fast"], fast: true, keys: ONE_KEY, run: hash::hsetnx },
    Command { name: "hkeys", arity: 2, flags: &["readonly"], fast: false, keys: ONE_KEY, run: hash::hkeys },
    Command { name: "hvals", arity: 2, flags: &["readonly"], fast: false, keys: ONE_KEY, run: hash::hvals },
    Command { name: "hstrlen", arity: 3, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: hash::hstrlen },
    Command { name: "hscan", arity: -3, flags: &["readonly"], fast: false, keys: ONE_KEY, run: hash::hscan },
    Command { name: "sadd", arity: -3, flags: &["write", "denyoom", "fast"], fast: true, keys: ONE_KEY, run: set::sadd },
    Command { name: "sismember", arity: 3, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: set::sismember },
    Command { name: "smembers", arity: 2, flags: &["readonly"], fast: false, keys: ONE_KEY, run: set::smembers },
    Command { name: "srem", arity: -3, flags: &["write", "fast"], fast: true, keys: ONE_KEY, run: set::srem },
    Command { name: "scard", arity: 2, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: set::scard },
    Command { name: "smismember", arity: -3, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: set::smismember },
    Command { name: "spop", arity: -2, flags: &["write", "fast"], fast: true, keys: ONE_KEY, run: set::spop },
    Command { name: "srandmember", arity: -2, flags: &["readonly"], fast: false, keys: ONE_KEY, run: set::srandmember },
    Command { name: "smove", arity: 4, flags: &["write", "fast"], fast: true, keys: (1, 2, 1), run: set::smove },
    Command { name: "sinter", arity: -2, flags: &["readonly"], fast: false, keys: ALL_KEYS, run: set::sinter },
    Command { name: "sinterstore", arity: -3, flags: &["write", "denyoom"], fast: false, keys: ALL_KEYS, run: set::sinterstore },
    Command { name: "sintercard", arity: -3, flags: &["readonly"], fast: false, keys: NO_KEYS, run: set::sintercard },
    Command { name: "sunion", arity: -2, flags: &["readonly"], fast: false, keys: ALL_KEYS, run: set::sunion },
    Command { name: "sunionstore", arity: -3, flags: &["write", "denyoom"], fast: false, keys: ALL_KEYS, run: set::sunionstore },
    Command { name: "sdiff", arity: -2, flags: &["readonly"], fast: false, keys: ALL_KEYS, run: set::sdiff },
    Command { name: "sdiffstore", arity: -3, flags: &["write", "denyoom"], fast: false, keys: ALL_KEYS, run: set::sdiffstore },
    Command { name: "sscan", arity: -3, flags: &["readonly"], fast: false, keys: ONE_KEY, run: set::sscan },
    Command { name: "lpush", arity: -3, flags: &["write", "denyoom", "fast"], fast: true, keys: ONE_KEY, run: list::lpush },
    Command { name: "rpush", arity: -3, flags: &["write", "denyoom", "fast"], fast: true, keys: ONE_KEY, run: list::rpush },
    Command { name: "lpop", arity: -2, flags: &["write", "fast"], fast: true, keys: ONE_KEY, run: list::lpop },
    Command { name: "rpop", arity: -2, flags: &["write", "fast"], fast: true, keys: ONE_KEY, run: list::rpop },
    Command { name: "lrange", arity: 4, flags: &["readonly"], fast: false, keys: ONE_KEY, run: list::lrange },
    Command { name: "llen", arity: 2, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: list::llen },
    Command { name: "lindex", arity: 3, flags: &["readonly"], fast: false, keys: ONE_KEY, run: list::lindex },
    Command { name: "lset", arity: 4, flags: &["write", "denyoom"], fast: false, keys: ONE_KEY, run: list::lset },
    Command { name: "linsert", arity: 5, flags: &["write", "denyoom"], fast: false, keys: ONE_KEY, run: list::linsert },
    Command { name: "lrem", arity: 4, flags: &["write"], fast: false, keys: ONE_KEY, run: list::lrem },
    Command { name: "ltrim", arity: 4, flags: &["write"], fast: false, keys: ONE_KEY, run: list::ltrim },
    Command { name: "lpos", arity: -3, flags: &["readonly"], fast: false, keys: ONE_KEY, run: list::lpos },
    Command { name: "lpushx", arity: -3, flags: &["write", "denyoom", "fast"], fast: true, keys: ONE_KEY, run: list::lpushx },
    Command { name: "rpushx", arity: -3, flags: &["write", "denyoom", "fast"], fast: true, keys: ONE_KEY, run: list::rpushx },
    Command { name: "lmove", arity: 5, flags: &["write", "denyoom"], fast: false, keys: (1, 2, 1), run: list::lmove },
    Command { name: "rpoplpush", arity: 3, flags: &["write", "denyoom"], fast: false, keys: (1, 2, 1), run: list::rpoplpush },
    Command { name: "lmpop", arity: -4, flags: &["write"], fast: false, keys: NO_KEYS, run: list::lmpop },
    Command { name: "blpop", arity: -3, flags: &["write", "blocking"], fast: false, keys: (1, -2, 1), run: list::blpop },
    Command { name: "brpop", arity: -3, flags: &["write", "blocking"], fast: false, keys: (1, -2, 1), run: list::brpop },
    Command { name: "blmove", arity: 6, flags: &["write", "denyoom", "blocking"], fast: false, keys: (1, 2, 1), run: list::blmove },
    Command { name: "brpoplpush", arity: 4, flags: &["write", "denyoom", "blocking"], fast: false, keys: (1, 2, 1), run: list::brpoplpush },
    Command { name: "blmpop", arity: -5, flags: &["write", "blocking"], fast: false, keys: NO_KEYS, run: list::blmpop },
    Command { name: "zadd", arity: -4, flags: &["write", "denyoom", "fast"], fast: true, keys: ONE_KEY, run: zset::zadd },
    Command { name: "zscore", arity: 3, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: zset::zscore },
    Command { name: "zrange", arity: -4, flags: &["readonly"], fast: false, keys: ONE_KEY, run: zset::zrange },
    Command { name: "zincrby", arity: 4, flags: &["write", "denyoom", "fast"], fast: true, keys: ONE_KEY, run: zset::zincrby },
    Command { name: "zrem", arity: -3, flags: &["write", "fast"], fast: true, keys: ONE_KEY, run: zset::zrem },
    Command { name: "zcard", arity: 2, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: zset::zcard },
    Command { name: "zrank", arity: -3, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: zset::zrank },
    Command { name: "zrevrank", arity: -3, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: zset::zrevrank },
    Command { name: "zrevrange", arity: -4, flags: &["readonly"], fast: false, keys: ONE_KEY, run: zset::zrevrange },
    Command { name: "zrangebyscore", arity: -4, flags: &["readonly"], fast: false, keys: ONE_KEY, run: zset::zrangebyscore },
    Command { name: "zrevrangebyscore", arity: -4, flags: &["readonly"], fast: false, keys: ONE_KEY, run: zset::zrevrangebyscore },
    Command { name: "zrangebylex", arity: -4, flags: &["readonly"], fast: false, keys: ONE_KEY, run: zset::zrangebylex },
    Command { name: "zrevrangebylex", arity: -4, flags: &["readonly"], fast: false, keys: ONE_KEY, run: zset::zrevrangebylex },
    Command { name: "zrangestore", arity: -5, flags: &["write", "denyoom"], fast: false, keys: (1, 2, 1), run: zset::zrangestore },
    Command { name: "zcount", arity: 4, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: zset::zcount },
    Command { name: "zlexcount", arity: 4, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: zset::zlexcount },
    Command { name: "zmscore", arity: -3, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: zset::zmscore },
    Command { name: "zremrangebyrank", arity: 4, flags: &["write"], fast: false, keys: ONE_KEY, run: zset::zremrangebyrank },
    Command { name: "zremrangebyscore", arity: 4, flags: &["write"], fast: false, keys: ONE_KEY, run: zset::zremrangebyscore },
    Command { name: "zremrangebylex", arity: 4, flags: &["write"], fast: false, keys: ONE_KEY, run: zset::zremrangebylex },
    Command { name: "zpopmin", arity: -2, flags: &["write", "fast"], fast: true, keys: ONE_KEY, run: zset::zpopmin },
    Command { name: "zpopmax", arity: -2, flags: &["write", "fast"], fast: true, keys: ONE_KEY, run: zset::zpopmax },
    Command { name: "zmpop", arity: -4, flags: &["write"], fast: false, keys: NO_KEYS, run: zset::zmpop },
    Command { name: "bzpopmin", arity: -3, flags: &["write", "blocking", "fast"], fast: true, keys: (1, -2, 1), run: zset::bzpopmin },
    Command { name: "bzpopmax", arity: -3, flags: &["write", "blocking", "fast"], fast: true, keys: (1, -2, 1), run: zset::bzpopmax },
    Command { name: "xadd", arity: -5, flags: &["write", "denyoom", "fast"], fast: true, keys: ONE_KEY, run: stream::xadd },
    Command { name: "xlen", arity: 2, flags: &["readonly", "fast"], fast: true, keys: ONE_KEY, run: stream::xlen },
    Command { name: "xrange", arity: -4, flags: &["readonly"], fast: false, keys: ONE_KEY, run: stream::xrange },
    Command { name: "xrevrange", arity: -4, flags: &["readonly"], fast: false, keys: ONE_KEY, run: stream::xrevrange },
    Command { name: "xdel", arity: -3, flags: &["write", "fast"], fast: true, keys: ONE_KEY, run: stream::xdel },
    Command { name: "xtrim", arity: -4, flags: &["write"], fast: false, keys: ONE_KEY, run: stream::xtrim },
    Command { name: "xread", arity: -4, flags: &["readonly", "blocking"], fast: false, keys: NO_KEYS, run: stream::xread },
    Command { name: "xreadgroup", arity: -7, flags: &["write", "blocking"], fast: false, keys: NO_KEYS, run: group::xreadgroup },
    Command { name: "xgroup", arity: -2, flags: &["write"], fast: false, keys: NO_KEYS, run: group::xgroup },
    Command { name: "xack", arity: -4, flags: &["write", "fast"], fast: true, keys: ONE_KEY, run: group::xack },
    Command { name: "xpending", arity: -3, flags: &["readonly"], fast: false, keys: ONE_KEY, run: group::xpending },
    Command { name: "xclaim", arity: -6, flags: &["write", "fast"], fast: true, keys: ONE_KEY, run: group::xclaim },
    Command { name: "xautoclaim", arity: -6, flags: &["write", "fast"], fast: true, keys: ONE_KEY, run: group::xautoclaim },
    Command { name: "xinfo", arity: -2, flags: &["readonly"], fast: false, keys: NO_KEYS, run: group::xinfo },
    Command { name: "xsetid", arity: -3, flags: &["write", "denyoom", "fast"], fast: true, keys: ONE_KEY, run: group::xsetid },
    Command { name: "bzmpop", arity: -5, flags: &["write", "blocking"], fast: false, keys: NO_KEYS, run: zset::bzmpop },
    Command { name: "zrandmember", arity: -2, flags: &["readonly"], fast: false, keys: ONE_KEY, run: zset::zrandmember },
    Command { name: "zscan", arity: -3, flags: &["readonly"], fast: false, keys: ONE_KEY, run: zset::zscan },
    Command { name: "zunion", arity: -3, flags: &["readonly"], fast: false, keys: NO_KEYS, run: zset::zunion },
    Command { name: "zinter", arity: -3, flags: &["readonly"], fast: false, keys: NO_KEYS, run: zset::zinter },
    Command { name: "zdiff", arity: -3, flags: &["readonly"], fast: false, keys: NO_KEYS, run: zset::zdiff },
    Command { name: "zunionstore", arity: -4, flags: &["write", "denyoom"], fast: false, keys: ONE_KEY, run: zset::zunionstore },
    Command { name: "zinterstore", arity: -4, flags: &["write", "denyoom"], fast: false, keys: ONE_KEY, run: zset::zinterstore },
    Command { name: "zdiffstore", arity: -4, flags: &["write", "denyoom"], fast: false, keys: ONE_KEY, run: zset::zdiffstore },
    Command { name: "zintercard", arity: -3, flags: &["readonly"], fast: false, keys: NO_KEYS, run: zset::zintercard },
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
            match ctx.aof() {
                None => return set(ctx, session, now, args, out),
                Some(aof) if !aof.failing() => {
                    return logged(ctx, session, aof, set_command(), args, out);
                }
                Some(_) => {}
            }
        }
    } else {
        if !matches!(session.reply, ReplyMode::On | ReplyMode::Passing) {
            return silenced(ctx, session, now, args, out);
        }
        if let Some(subscriber) = session.subscriber()
            && !subscriber.take(out)
        {
            // Messages that came before this command go out first; this
            // client is over its output limit.
            return Reply::Close;
        }
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
        refuse_in_multi(session);
        return Reply::Written;
    };
    if !command.arity_allows(args.len()) {
        wrong_arity(out, command.name);
        refuse_in_multi(session);
        return Reply::Written;
    }
    if !session.unrestricted && !session.authenticated && !command.flags.contains(&"no_auth") {
        resp::write_error(out, b"NOAUTH Authentication required.");
        refuse_in_multi(session);
        return Reply::Written;
    }
    if let Some(aof) = ctx.aof()
        && aof.failing()
        && refused_unlogged(session, aof, command, out)
    {
        return Reply::Written;
    }
    if !session.unrestricted {
        if session.special && held_back(ctx, session, command) {
            session.commands -= 1;
            return Reply::Paused;
        }
        // A subscribed RESP2 client may only run the pub/sub ones, PING,
        // QUIT and RESET (but hears about an unknown subcommand first).
        if session.pubsub.is_some()
            && !session.resp3
            && !pubsub::allowed_when_subscribed(command.name)
            && pubsub::not_allowed(out, command.name, args)
        {
            refuse_in_multi(session);
            return Reply::Written;
        }
        // In a transaction, commands wait for EXEC.
        if let Some(queue) = &mut session.multi
            && !multi::runs_in_multi(command.name)
        {
            if multi::refused_in_multi(command.name) {
                resp::write_error(out, b"ERR Command not allowed inside a transaction");
                queue.refused = true;
            } else {
                queue.push(args);
                out.extend_from_slice(b"+QUEUED\r\n");
            }
            return Reply::Written;
        }
        if session.special {
            return watched(ctx, session, now, command, args, out);
        }
    }
    dispatch(ctx, session, now, command, args, out)
}

/// A command's run, past every check.
#[inline]
fn dispatch(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    command: &Command,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    if let Some(aof) = ctx.aof()
        && (command.writes() || matches!(command.name, "eval" | "evalsha" | "exec"))
    {
        return logged(ctx, session, aof, command, args, out);
    }
    let reply = timed(ctx, session, now, command, args, out);
    if ctx.store.has_ready() {
        serve_ready(ctx, now);
    }
    reply
}

/// Runs a command, logging it if it took long: by the clock a thread ticks, as
/// reading it is next to nothing (see `slowlog.rs`), and not at all if it's `fast`.
#[inline]
fn timed(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    command: &Command,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    if command.fast {
        return (command.run)(ctx, session, now, args, out);
    }
    let before = ctx.ticks();
    let reply = (command.run)(ctx, session, now, args, out);
    if ctx.ticks() != before {
        slow_by_ticks(ctx, session, command, args, before);
    }
    reply
}

#[cold]
fn slow_by_ticks(
    ctx: &Context,
    session: &mut Session,
    command: &Command,
    args: &Args<'_>,
    before: u64,
) {
    // The clock may have gone to sleep while this ran: ask what time it really is.
    ctx.tick();
    let ticks = ctx.ticks().saturating_sub(before);
    if ticks >= ctx.slowlog.threshold_ticks() && !session.special {
        log_slow(
            ctx,
            session,
            command,
            args,
            ticks * crate::slowlog::TICK_MICROS as u64,
        );
    }
}

fn log_slow(ctx: &Context, session: &mut Session, command: &Command, args: &Args<'_>, micros: u64) {
    let sub = (args.len() > 1).then(|| args.get(1));
    if command_table::hidden(command.name, sub).from_slowlog || turned_away(command, args) {
        return;
    }
    // A script's own commands have no one to name, as in Redis.
    if session.in_script {
        ctx.slowlog.push(args, micros, String::new(), b"");
        return;
    }
    let peer = ctx.peer_of(session);
    let name = session.name.as_deref().unwrap_or_default();
    ctx.slowlog.push(args, micros, peer, name);
}

/// A subcommand that Redis would have refused before running anything, so that nothing hears of it.
fn turned_away(command: &Command, args: &Args<'_>) -> bool {
    args.len() > 1 && command_table::refused_subcommand(command.name, args.get(1), args.len())
}

/// Whether `CLIENT PAUSE` holds this command back: all of them, or those that
/// write or may.
#[cold]
fn held_back(ctx: &Context, session: &Session, command: &Command) -> bool {
    match ctx.pause_mode() {
        None => false,
        Some(true) => true,
        Some(false) => {
            command.writes()
                || match command.name {
                    "publish" | "spublish" | "eval" | "evalsha" | "script" | "wait" | "waitaof" => {
                        true
                    }
                    "exec" => session.multi.as_ref().is_some_and(|queue| {
                        (0..queue.len()).any(|i| {
                            let name = queue.command(i);
                            find_write_command(name.get(0)).is_some()
                                || matches!(
                                    find_command(name.get(0)).map(|c| c.name),
                                    Some("publish" | "spublish" | "eval" | "evalsha")
                                )
                        })
                    }),
                    _ => false,
                }
        }
    }
}

/// A command run while something's watching: `MONITOR` hears of it, and it's timed
/// with a real clock (see `Context::special`).
#[cold]
fn watched(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    command: &Command,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let script = heard_first(ctx, session, command, args);
    let started = std::time::Instant::now();
    let reply = dispatch(ctx, session, now, command, args, out);
    has_run(ctx, session, command, args, started, !script);
    reply
}

/// Monitors hear of a script before what it runs (even one that isn't found), and of the
/// rest once they've run. Says whether this is a script.
fn heard_first(ctx: &Context, session: &mut Session, command: &Command, args: &Args<'_>) -> bool {
    let script = matches!(command.name, "eval" | "evalsha" | "eval_ro" | "evalsha_ro");
    if script && ctx.monitoring() {
        let peer = ctx.peer_of(session);
        ctx.feed_monitors(session.db, &peer, args);
    }
    script
}

/// A command inside `EXEC` or a script, while something's watching.
fn watched_inside(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    command: &Command,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let script = heard_first(ctx, session, command, args);
    let started = std::time::Instant::now();
    let reply = (command.run)(ctx, session, now, args, out);
    has_run(ctx, session, command, args, started, !script);
    reply
}

/// What's watching hears of a command that's run.
fn has_run(
    ctx: &Context,
    session: &mut Session,
    command: &Command,
    args: &Args<'_>,
    started: std::time::Instant,
    tell_monitors: bool,
) {
    let micros = started.elapsed().as_micros() as u64;
    if tell_monitors && ctx.monitoring() {
        let sub = (args.len() > 1).then(|| args.get(1));
        if !command_table::hidden(command.name, sub).from_monitor && !turned_away(command, args) {
            if session.in_script {
                ctx.feed_monitors(session.db, "lua", args);
            } else {
                let peer = ctx.peer_of(session);
                ctx.feed_monitors(session.db, &peer, args);
            }
        }
    }
    if ctx.slowlog.is_slow(micros) {
        log_slow(ctx, session, command, args, micros);
    }
    let event = if command.fast {
        "fast-command"
    } else {
        "command"
    };
    ctx.latency.note(event, micros / 1000);
}

/// A command run with `CLIENT REPLY OFF` or `SKIP`: its reply goes, and `SKIP` is
/// over. (`CLIENT REPLY ON` is heard, and answered.)
#[cold]
fn silenced(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    if let Some(subscriber) = session.subscriber()
        && !subscriber.take(out)
    {
        return Reply::Close;
    }
    let mode = session.reply;
    session.reply = ReplyMode::Passing;
    session.refresh();
    let start = out.len();
    let reply = execute(ctx, session, now, args, out);
    if matches!(session.reply, ReplyMode::Passing) {
        out.truncate(start);
        session.reply = if matches!(mode, ReplyMode::Skip) {
            ReplyMode::On
        } else {
            mode
        };
    } else if !matches!(session.reply, ReplyMode::On) {
        out.truncate(start);
    }
    session.refresh();
    match reply {
        Reply::Bulk(_) if !matches!(session.reply, ReplyMode::On) => Reply::Written,
        reply => reply,
    }
}

/// Runs active expiry every `interval` until `ctx` is dropped. A client blocked
/// in `XREADGROUP` on a key it removes hears at once that the group is gone, as
/// nothing else may run for a while.
pub fn spawn_expirer(
    ctx: &Arc<Context>,
    interval: std::time::Duration,
) -> std::thread::JoinHandle<()> {
    let clock = Arc::downgrade(ctx);
    let ctx = Arc::downgrade(ctx);
    std::thread::Builder::new()
        .name("tantalum-clock".into())
        .spawn(move || {
            let tick = std::time::Duration::from_micros(crate::slowlog::TICK_MICROS as u64);
            if let Some(ctx) = clock.upgrade() {
                ctx.clock_starts();
            }
            let (mut seen, mut quiet) = (0, 0);
            loop {
                std::thread::sleep(tick);
                let Some(ctx) = clock.upgrade() else { break };
                ctx.tick();
                let commands = ctx.commands_so_far();
                quiet = if commands == seen { quiet + 1 } else { 0 };
                seen = commands;
                if quiet < crate::slowlog::TICKS_BEFORE_SLEEP {
                    continue;
                }
                // Nothing to time, so an idle server isn't woken for it. It looks now and
                // then only to go when the server does.
                quiet = 0;
                ctx.clock_sleeps();
                drop(ctx);
                loop {
                    std::thread::park_timeout(std::time::Duration::from_secs(1));
                    let Some(ctx) = clock.upgrade() else { return };
                    if !ctx.clock_asleep() {
                        break;
                    }
                }
            }
        })
        .expect("spawn clock thread");
    std::thread::Builder::new()
        .name("tantalum-expire".into())
        .spawn(move || {
            loop {
                std::thread::sleep(interval);
                let Some(ctx) = ctx.upgrade() else { break };
                let now = ctx.store.now();
                // As in Redis, what's paused doesn't change by itself meanwhile.
                if ctx.pause_mode().is_none() {
                    for db in ctx.store.live() {
                        db.expire_cycle(now);
                    }
                }
                ctx.note_memory_peak();
                if ctx.store.has_ready() {
                    serve_ready(&ctx, now);
                }
            }
        })
        .expect("spawn expiry thread")
}

/// Serves blocked clients whose keys are ready, after a command that isn't
/// a write (a write serves them in its turn). Their commands write: with
/// the append-only log on, they take a turn too, so they're logged, in order.
fn serve_ready(ctx: &Context, now: Millis) {
    match ctx.aof() {
        Some(aof) => {
            let turn = aof.turn(false, 0);
            crate::blocking::serve_ready(ctx, turn.now);
        }
        None => crate::blocking::serve_ready(ctx, now),
    }
}

/// While the append-only log can't be written, Redis refuses commands that
/// write (and `EXEC` with one queued, discarding it), and `PING`, so health
/// checks notice: true if it refused this one, replying why.
#[cold]
fn refused_unlogged(
    session: &mut Session,
    aof: &crate::aof::Aof,
    command: &Command,
    out: &mut Vec<u8>,
) -> bool {
    match command.name {
        "ping" => {}
        "exec" => {
            let Some(queue) = &session.multi else {
                return false;
            };
            if !(0..queue.len()).any(|i| find_write_command(queue.command(i).get(0)).is_some()) {
                return false;
            }
            session.multi = None;
            session.watching = None;
            session.refresh();
            let error = format!(
                "EXECABORT Transaction discarded because of: {}",
                aof.failure()
            );
            resp::write_error(out, error.as_bytes());
            return true;
        }
        _ if command.writes() => {}
        _ => return false,
    }
    resp::write_error(out, aof.failure().as_bytes());
    refuse_in_multi(session);
    true
}

/// [`execute`] for a command that may write, with the append-only log on:
/// in a write turn, at the time the turn comes, logging what it did (a
/// script's or transaction's writes as a group).
fn logged(
    ctx: &Context,
    session: &mut Session,
    aof: &crate::aof::Aof,
    command: &Command,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    // Scripts and transactions log their commands' writes, and MSET its
    // pairs, as a group.
    let grouped = !command.writes() || matches!(command.name, "mset" | "msetnx");
    let mut turn = aof.turn(grouped, session.db);
    let now = turn.now;
    let start = out.len();
    let reply = timed(ctx, session, now, command, args, out);
    // A blocked command is logged when it's served.
    if !grouped && !matches!(reply, Reply::Block(_)) {
        let bulk;
        let written = match &reply {
            Reply::Bulk(value) => {
                bulk = value.as_bytes();
                bulk
            }
            _ => &out[start..],
        };
        crate::aof::record(command.name, args, written);
    }
    turn.end_group();
    if ctx.store.has_ready() {
        crate::blocking::serve_ready(ctx, now);
    }
    reply
}

/// `SET`'s table entry, for `execute`'s fast path.
fn set_command() -> &'static Command {
    static SET: std::sync::OnceLock<&'static Command> = std::sync::OnceLock::new();
    SET.get_or_init(|| find_command(b"SET").expect("SET is in the table"))
}

/// The name of the write command called `name`, if it is one.
pub(crate) fn find_write_command(name: &[u8]) -> Option<&'static str> {
    find_command(name)
        .filter(|command| command.writes())
        .map(|command| command.name)
}

pub(crate) use eval::call_from_script;

/// Whether `name` is a command here, in any case.
pub(crate) fn is_command(name: &[u8]) -> bool {
    find_command(name).is_some()
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
    let store = ctx.db(session);
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
    let store = ctx.db(session);
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
    let store = ctx.db(session);
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

fn setex(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    set_expiring(ctx.db(session), now, args, out, TtlUnit::Seconds, "setex")
}

fn psetex(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    set_expiring(ctx.db(session), now, args, out, TtlUnit::Millis, "psetex")
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

fn setnx(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = ctx.db(session);
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
fn mset(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    if args.len().is_multiple_of(2) {
        wrong_arity(out, "mset");
        return Reply::Written;
    }
    let store = ctx.db(session);
    let keys: Vec<Key<'_>> = (1..args.len())
        .step_by(2)
        .map(|i| args.key_at(store, i))
        .collect();
    let logged = ctx.aof().is_some();
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
        // Logged a pair at a time (a group: see `logged`), so evictions
        // between pairs, and the pairs set before running out of memory,
        // come back as they were.
        if logged {
            crate::aof::record_parts([&b"SET"[..], args.get(2 * i + 1), value].into_iter());
        }
    }
    out.extend_from_slice(resp::OK);
    Reply::Written
}

/// `DEL` and `UNLINK`. Big values are always freed after the shard lock is
/// released, so the two are the same.
fn del(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = ctx.db(session);
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
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = ctx.db(session);
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
    let store = ctx.db(session);
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
    let store = ctx.db(session);
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
        // PERSIST of a key without a TTL changes nothing (WATCH can tell).
        Some(current) if persist && matches!(current.expiry, Expiry::Never) => {
            (Update::Touch, Ok(Some(current.value.share())))
        }
        Some(current) => (change.clone(), Ok(Some(current.value.share()))),
        None => (Update::Peek, Ok(None)),
    });
    string_or_null(out, session, value)
}

fn expire(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    set_expiry(ctx.db(session), now, args, out, TtlUnit::Seconds, "expire")
}

fn pexpire(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    set_expiry(ctx.db(session), now, args, out, TtlUnit::Millis, "pexpire")
}

fn expireat(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    set_expiry(
        ctx.db(session),
        now,
        args,
        out,
        TtlUnit::UnixSeconds,
        "expireat",
    )
}

fn pexpireat(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    set_expiry(
        ctx.db(session),
        now,
        args,
        out,
        TtlUnit::UnixMillis,
        "pexpireat",
    )
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

fn ttl(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    reply_expiry(ctx.db(session), now, args, out, TtlUnit::Seconds)
}

fn pttl(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    reply_expiry(ctx.db(session), now, args, out, TtlUnit::Millis)
}

fn expiretime(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    reply_expiry(ctx.db(session), now, args, out, TtlUnit::UnixSeconds)
}

fn pexpiretime(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    reply_expiry(ctx.db(session), now, args, out, TtlUnit::UnixMillis)
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
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = ctx.db(session);
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

fn incr(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    incr_by(ctx.db(session), now, args, out, 1)
}

fn decr(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    incr_by(ctx.db(session), now, args, out, -1)
}

fn incrby(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    match resp::parse_int(args.get(2)) {
        Some(delta) => incr_by(ctx.db(session), now, args, out, delta),
        None => {
            resp::write_error(out, NOT_AN_INTEGER);
            Reply::Written
        }
    }
}

fn decrby(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    match resp::parse_int(args.get(2)).map(i64::checked_neg) {
        Some(Some(delta)) => incr_by(ctx.db(session), now, args, out, delta),
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

/// `FLUSHALL` and `FLUSHDB`, `ASYNC` or `SYNC`. Freeing happens outside the
/// shard locks either way.
fn flush(
    ctx: &Context,
    session: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let mode_ok =
        |arg: &[u8]| arg.eq_ignore_ascii_case(b"ASYNC") || arg.eq_ignore_ascii_case(b"SYNC");
    if args.len() > 2 || (args.len() == 2 && !mode_ok(args.get(1))) {
        resp::write_error(out, SYNTAX_ERROR);
        return Reply::Written;
    }
    if args.get(0).eq_ignore_ascii_case(b"FLUSHALL") {
        ctx.store.live().for_each(Store::clear);
    } else {
        ctx.db(session).clear();
    }
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
        _ if args.len() > 2
            || (args.len() == 2 && !args.get(1).eq_ignore_ascii_case(b"SCHEDULE")) =>
        {
            resp::write_error(out, SYNTAX_ERROR)
        }
        None => resp::write_error(out, SNAPSHOTS_OFF),
        Some(snapshots) if snapshots.save_in_background() => {
            resp::write_simple(out, b"Background saving started");
        }
        Some(_) => resp::write_error(out, b"ERR Background save already in progress"),
    }
    Reply::Written
}

/// `BGREWRITEAOF`: rewrites the append-only log in the background.
fn bgrewriteaof(
    ctx: &Context,
    _: &mut Session,
    _: Millis,
    _: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    match ctx.aof() {
        None => resp::write_error(
            out,
            b"ERR the append-only log is off (start with --aof DIR)",
        ),
        Some(aof) if aof.rewrite_in_background() => {
            resp::write_simple(out, b"Background append only file rewriting started");
        }
        Some(_) => resp::write_error(
            out,
            b"ERR Background append only file rewriting already in progress",
        ),
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

/// `SELECT index`: the database this connection's commands use from now on.
fn select(
    ctx: &Context,
    session: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    if let Some(index) = db_index(ctx, args.get(1), out) {
        session.select(ctx, index);
        crate::aof::select(index as usize);
        out.extend_from_slice(resp::OK);
    }
    Reply::Written
}

/// The database numbered `arg`: or, if it isn't one, writes the error.
fn db_index(ctx: &Context, arg: &[u8], out: &mut Vec<u8>) -> Option<u32> {
    let Some(index) = resp::parse_int(arg) else {
        resp::write_error(out, NOT_AN_INTEGER);
        return None;
    };
    if i32::try_from(index).is_err() {
        resp::write_error(
            out,
            b"ERR value is out of range, value must between -2147483648 and 2147483647",
        );
        return None;
    }
    if !(0..ctx.store.databases() as i64).contains(&index) {
        resp::write_error(out, b"ERR DB index is out of range");
        return None;
    }
    Some(index as u32)
}

/// `TIME`: the Unix time, in seconds and microseconds.
fn time(_: &Context, _: &mut Session, _: Millis, _: &Args<'_>, out: &mut Vec<u8>) -> Reply {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    resp::write_array_header(out, 2);
    resp::write_bulk(out, now.as_secs().to_string().as_bytes());
    resp::write_bulk(out, now.subsec_micros().to_string().as_bytes());
    Reply::Written
}

fn dbsize(
    ctx: &Context,
    session: &mut Session,
    _: Millis,
    _: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    resp::write_integer(out, ctx.db(session).len() as i64);
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
        && !set_client_name(ctx, session, name, out)
    {
        return Reply::Written;
    }
    session.set_resp3(resp3);
    ctx.update_client(session.id, |client| client.resp3 = resp3);
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
fn set_client_name(ctx: &Context, session: &mut Session, name: &[u8], out: &mut Vec<u8>) -> bool {
    if !valid_client_attr(name, "Client names", out) {
        return false;
    }
    session.name = (!name.is_empty()).then(|| name.into());
    ctx.update_client(session.id, |client| client.name.clone_from(&session.name));
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

/// `CLIENT SETNAME | GETNAME | ID | SETINFO | LIST | INFO | KILL | PAUSE | UNPAUSE |
/// UNBLOCK | REPLY | NO-EVICT | NO-TOUCH | GETREDIR | TRACKINGINFO`: what client
/// libraries send on connect, what job queues read to find their workers, and
/// what operators use.
fn client(
    ctx: &Context,
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
        if arity(3, "setname", out) && set_client_name(ctx, session, args.get(2), out) {
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
            let attr = args.get(2);
            let lib_name = attr.eq_ignore_ascii_case(b"LIB-NAME");
            let what = if lib_name {
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
            let value = args.get(3);
            if valid_client_attr(value, what, out) {
                let value = (!value.is_empty()).then(|| value.into());
                ctx.update_client(session.id, |client| match lib_name {
                    true => client.lib_name = value,
                    false => client.lib_ver = value,
                });
                out.extend_from_slice(resp::OK);
            }
        }
    } else if is(b"INFO") && args.len() == 2 {
        let line = ctx.client_lines(session, &[session.id], "client|info");
        resp::write_verbatim_text(out, &line, session.resp3);
    } else if is(b"LIST") {
        client_list(ctx, session, args, out);
    } else if is(b"KILL") {
        return client_kill(ctx, session, args, out);
    } else if is(b"PAUSE") {
        client_pause(ctx, args, out);
        ctx.follow_special(session);
    } else if is(b"UNPAUSE") {
        if arity(2, "unpause", out) {
            ctx.unpause_clients();
            ctx.follow_special(session);
            out.extend_from_slice(resp::OK);
        }
    } else if is(b"UNBLOCK") {
        client_unblock(ctx, args, out);
    } else if is(b"REPLY") {
        if arity(3, "reply", out) {
            let mode = args.get(2);
            if mode.eq_ignore_ascii_case(b"ON") {
                session.reply = ReplyMode::On;
                out.extend_from_slice(resp::OK);
            } else if mode.eq_ignore_ascii_case(b"OFF") {
                session.reply = ReplyMode::Off;
            } else if mode.eq_ignore_ascii_case(b"SKIP") {
                // This one's reply, which there is none of, isn't the one it skips.
                if !matches!(session.reply, ReplyMode::Off) {
                    session.reply = ReplyMode::Skip;
                }
            } else {
                resp::write_error(out, SYNTAX_ERROR);
            }
            session.refresh();
        }
    } else if is(b"NO-EVICT") || is(b"NO-TOUCH") {
        let name = if is(b"NO-EVICT") {
            "no-evict"
        } else {
            "no-touch"
        };
        if arity(3, name, out) {
            let on = args.get(2).eq_ignore_ascii_case(b"ON");
            if !on && !args.get(2).eq_ignore_ascii_case(b"OFF") {
                resp::write_error(out, SYNTAX_ERROR);
            } else {
                // Clients aren't evicted here, and keys don't remember being touched.
                if is(b"NO-EVICT") {
                    session.no_evict = on;
                } else {
                    session.no_touch = on;
                }
                out.extend_from_slice(resp::OK);
            }
        }
    } else if is(b"GETREDIR") {
        if arity(2, "getredir", out) {
            resp::write_integer(out, -1);
        }
    } else if is(b"TRACKINGINFO") {
        if arity(2, "trackinginfo", out) {
            resp::write_map_header(out, 3, session.resp3);
            resp::write_bulk(out, b"flags");
            resp::write_set_header(out, 1, session.resp3);
            resp::write_bulk(out, b"off");
            resp::write_bulk(out, b"redirect");
            resp::write_integer(out, -1);
            resp::write_bulk(out, b"prefixes");
            resp::write_array_header(out, 0);
        }
    } else {
        unknown_subcommand(out, sub, "CLIENT");
    }
    Reply::Written
}

/// `CLIENT PAUSE timeout [WRITE|ALL]`
fn client_pause(ctx: &Context, args: &Args<'_>, out: &mut Vec<u8>) {
    if args.len() < 3 {
        wrong_arity(out, "client|pause");
        return;
    }
    if args.len() > 4 {
        subcommand_syntax_error(out, args.get(1), "CLIENT");
        return;
    }
    let Some(timeout) = resp::parse_int(args.get(2)) else {
        resp::write_error(out, b"ERR timeout is not an integer or out of range");
        return;
    };
    if timeout < 0 {
        resp::write_error(out, b"ERR timeout is negative");
        return;
    }
    let all = if args.len() == 4 {
        let mode = args.get(3);
        if mode.eq_ignore_ascii_case(b"ALL") {
            true
        } else if mode.eq_ignore_ascii_case(b"WRITE") {
            false
        } else {
            resp::write_error(out, b"ERR CLIENT PAUSE mode must be WRITE or ALL");
            return;
        }
    } else {
        true
    };
    ctx.pause_clients(timeout as u64, all);
    out.extend_from_slice(resp::OK);
}

/// `CLIENT UNBLOCK id [TIMEOUT|ERROR]`: 1 if it was blocked in a command.
fn client_unblock(ctx: &Context, args: &Args<'_>, out: &mut Vec<u8>) {
    if args.len() < 3 {
        wrong_arity(out, "client|unblock");
        return;
    }
    if args.len() > 4 {
        subcommand_syntax_error(out, args.get(1), "CLIENT");
        return;
    }
    let error = if args.len() == 4 {
        let reason = args.get(3);
        if reason.eq_ignore_ascii_case(b"ERROR") {
            true
        } else if reason.eq_ignore_ascii_case(b"TIMEOUT") {
            false
        } else {
            resp::write_error(out, b"ERR CLIENT UNBLOCK reason should be TIMEOUT or ERROR");
            return;
        }
    } else {
        false
    };
    let Some(id) = resp::parse_int(args.get(2)) else {
        resp::write_error(out, NOT_AN_INTEGER);
        return;
    };
    resp::write_integer(
        out,
        i64::from(crate::blocking::unblock(ctx, id as u64, error)),
    );
}

/// `CLIENT KILL addr:port`, which replies OK and can close the caller's own
/// connection, or `CLIENT KILL [ID id] [TYPE type] [USER name] [ADDR addr:port]
/// [LADDR addr:port] [SKIPME yes|no]`, which replies how many it closed.
fn client_kill(ctx: &Context, session: &Session, args: &Args<'_>, out: &mut Vec<u8>) -> Reply {
    let mut filter = KillFilter {
        addr: None,
        laddr: None,
        id: 0,
        pubsub: None,
        none: false,
        skip_me: true,
    };
    let n = args.len();
    if n < 3 {
        resp::write_error(
            out,
            b"ERR wrong number of arguments for 'client|kill' command",
        );
        return Reply::Written;
    }
    if n == 3 {
        filter.addr = Some(args.get(2));
        filter.skip_me = false;
    } else if n % 2 == 1 {
        // An option without its value (Redis reads them in pairs).
        resp::write_error(out, SYNTAX_ERROR);
        return Reply::Written;
    }
    if n > 3 {
        let mut i = 2;
        while i < n {
            let (option, value) = (args.get(i), args.get(i + 1));
            let is = |name: &[u8]| option.eq_ignore_ascii_case(name);
            if is(b"ID") {
                match resp::parse_int(value) {
                    Some(id) if id >= 1 => filter.id = id as u64,
                    _ => {
                        resp::write_error(out, b"ERR client-id should be greater than 0");
                        return Reply::Written;
                    }
                }
            } else if is(b"TYPE") {
                let kind = |name: &[u8]| value.eq_ignore_ascii_case(name);
                if kind(b"NORMAL") {
                    filter.pubsub = Some(false);
                } else if kind(b"PUBSUB") {
                    filter.pubsub = Some(true);
                } else if kind(b"MASTER") || kind(b"REPLICA") || kind(b"SLAVE") {
                    filter.none = true;
                } else {
                    let mut msg = b"ERR Unknown client type '".to_vec();
                    msg.extend_from_slice(value);
                    msg.push(b'\'');
                    resp::write_error(out, &msg);
                    return Reply::Written;
                }
            } else if is(b"ADDR") {
                filter.addr = Some(value);
            } else if is(b"LADDR") {
                filter.laddr = Some(value);
            } else if is(b"USER") {
                if value != b"default" {
                    let mut msg = b"ERR No such user '".to_vec();
                    msg.extend_from_slice(value);
                    msg.push(b'\'');
                    resp::write_error(out, &msg);
                    return Reply::Written;
                }
            } else if is(b"SKIPME") {
                if value.eq_ignore_ascii_case(b"YES") {
                    filter.skip_me = true;
                } else if value.eq_ignore_ascii_case(b"NO") {
                    filter.skip_me = false;
                } else {
                    resp::write_error(out, SYNTAX_ERROR);
                    return Reply::Written;
                }
            } else {
                resp::write_error(out, SYNTAX_ERROR);
                return Reply::Written;
            }
            i += 2;
        }
    }
    let (killed, me_too) = ctx.kill_clients(session.id, &filter);
    if n > 3 {
        resp::write_integer(out, killed as i64);
    } else if killed == 0 {
        resp::write_error(out, b"ERR No such client");
    } else {
        out.extend_from_slice(resp::OK);
    }
    if me_too { Reply::Close } else { Reply::Written }
}

/// `CLIENT LIST [TYPE NORMAL|MASTER|REPLICA|PUBSUB] [ID id [id ...]]`, as
/// Redis 7.2 reads its arguments.
fn client_list(ctx: &Context, session: &Session, args: &Args<'_>, out: &mut Vec<u8>) {
    let n = args.len();
    let lines = if n == 4 && args.get(2).eq_ignore_ascii_case(b"TYPE") {
        let kind = args.get(3);
        let is = |name: &[u8]| kind.eq_ignore_ascii_case(name);
        if is(b"NORMAL") || is(b"PUBSUB") {
            let ids = ctx.client_ids(is(b"PUBSUB"));
            if ids.is_empty() {
                Vec::new()
            } else {
                ctx.client_lines(session, &ids, "client|list")
            }
        } else if is(b"MASTER") || is(b"REPLICA") || is(b"SLAVE") {
            Vec::new()
        } else {
            let mut msg = b"ERR Unknown client type '".to_vec();
            msg.extend_from_slice(kind);
            msg.push(b'\'');
            resp::write_error(out, &msg);
            return;
        }
    } else if n > 3 && args.get(2).eq_ignore_ascii_case(b"ID") {
        let mut ids = Vec::with_capacity(n - 3);
        for i in 3..n {
            match resp::parse_int(args.get(i)) {
                Some(id) => ids.push(id as u64),
                None => {
                    resp::write_error(out, b"ERR Invalid client ID");
                    return;
                }
            }
        }
        ctx.client_lines(session, &ids, "client|list")
    } else if n != 2 {
        resp::write_error(out, SYNTAX_ERROR);
        return;
    } else {
        ctx.client_lines(session, &[], "client|list")
    };
    resp::write_verbatim_text(out, &lines, session.resp3);
}

/// `COMMAND`, `COMMAND COUNT | INFO [name ...] | LIST [FILTERBY ...] | DOCS [name ...] |
/// GETKEYS | GETKEYSANDFLAGS | HELP`, with what Redis says of them (`command_table.rs`).
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
    } else if is(b"LIST") {
        command_list(args, out);
    } else if is(b"GETKEYS") {
        admin::command_getkeys(session, args, false, out);
    } else if is(b"GETKEYSANDFLAGS") {
        admin::command_getkeys(session, args, true, out);
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
        let docs = |c: &'static Command| command_table::entry(c.name).map(|e| (c.name, e.docs));
        let found: Vec<_> = if args.len() == 2 {
            COMMANDS.iter().filter_map(docs).collect()
        } else {
            (2..args.len())
                .filter_map(|i| find_command(args.get(i)).and_then(docs))
                .collect()
        };
        resp::write_map_header(out, found.len(), resp3);
        for (name, docs) in found {
            resp::write_bulk(out, name.as_bytes());
            command_table::write(docs, resp3, out);
        }
    } else if is(b"COUNT") {
        wrong_arity(out, "command|count");
    } else {
        unknown_subcommand(out, sub, "COMMAND");
    }
    Reply::Written
}

/// `COMMAND LIST [FILTERBY MODULE name | ACLCAT category | PATTERN pattern]`
fn command_list(args: &Args<'_>, out: &mut Vec<u8>) {
    if args.len() == 2 {
        resp::write_array_header(out, COMMANDS.len());
        for c in COMMANDS {
            resp::write_bulk(out, c.name.as_bytes());
        }
        return;
    }
    if args.len() != 5 || !args.get(2).eq_ignore_ascii_case(b"FILTERBY") {
        resp::write_error(out, SYNTAX_ERROR);
        return;
    }
    let (by, what) = (args.get(3), args.get(4));
    let names: Vec<&str> = if by.eq_ignore_ascii_case(b"MODULE") {
        Vec::new()
    } else if by.eq_ignore_ascii_case(b"ACLCAT") {
        COMMANDS
            .iter()
            .filter(|c| command_table::in_category(c.name, what))
            .map(|c| c.name)
            .collect()
    } else if by.eq_ignore_ascii_case(b"PATTERN") {
        COMMANDS
            .iter()
            .filter(|c| glob::matches(what, c.name.as_bytes(), true))
            .map(|c| c.name)
            .collect()
    } else {
        resp::write_error(out, SYNTAX_ERROR);
        return;
    };
    resp::write_array_header(out, names.len());
    for name in names {
        resp::write_bulk(out, name.as_bytes());
    }
}

/// One command's entry in a `COMMAND` reply, in Redis 7's 10-field shape: what
/// Redis says of it, or for one it doesn't have, what the table here says.
fn write_command_info(out: &mut Vec<u8>, c: &Command, resp3: bool) {
    if let Some(entry) = command_table::entry(c.name) {
        command_table::write(entry.info, resp3, out);
        return;
    }
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
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = ctx.db(session);
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

/// `INFO [section ...]` in Redis' `key:value` format, with the fields tools
/// commonly read. What isn't kept reads as Redis shows it when it's not in use.
fn info(
    ctx: &Context,
    session: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    use std::fmt::Write;
    // Include this connection's latest counts.
    ctx.publish(session);
    let asked: Vec<Vec<u8>> = (1..args.len())
        .map(|i| args.get(i).to_ascii_lowercase())
        .collect();
    let named = |name: &[u8]| asked.iter().any(|a| a == name);
    let everything = named(b"all") || named(b"everything");
    let by_default = asked.is_empty() || named(b"default");
    // `commandstats` and `latencystats` only when asked for, as in Redis.
    let wants =
        |name: &str, default: bool| everything || (by_default && default) || named(name.as_bytes());
    let server = ctx.stats();
    let stats = ctx.store.stats();
    let peak = ctx.note_memory_peak().max(stats.used_memory);
    let mut s = String::new();
    if wants("server", true) {
        let (os, machine) = uname();
        let _ = write!(
            s,
            "# Server\r\nredis_version:{REDIS_VERSION}\r\ntantalum_version:{}\r\nredis_git_sha1:00000000\r\n\
             redis_git_dirty:0\r\nredis_mode:standalone\r\nos:{os} {machine}\r\narch_bits:{}\r\n\
             process_id:{}\r\nrun_id:{}\r\nuptime_in_seconds:{}\r\nuptime_in_days:{}\r\nhz:10\r\n\
             configured_hz:10\r\n\r\n",
            env!("CARGO_PKG_VERSION"),
            usize::BITS,
            std::process::id(),
            ctx.run_id(),
            server.uptime.as_secs(),
            server.uptime.as_secs() / 86_400,
        );
    }
    if wants("clients", true) {
        let _ = write!(
            s,
            "# Clients\r\nconnected_clients:{}\r\nmaxclients:{}\r\nblocked_clients:{}\r\n\
             tracking_clients:0\r\n\r\n",
            server.connected_clients,
            ctx.maxclients(),
            ctx.store.blocked_clients(),
        );
    }
    if wants("memory", true) {
        let _ = write!(
            s,
            "# Memory\r\nused_memory:{}\r\nused_memory_human:{}\r\nused_memory_peak:{}\r\n\
             used_memory_peak_human:{}\r\n",
            stats.used_memory,
            bytes_to_human(stats.used_memory),
            peak,
            bytes_to_human(peak),
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
            let _ = write!(
                s,
                "used_memory_rss:{rss}\r\nused_memory_rss_human:{}\r\nmem_fragmentation_ratio:{:.2}\r\n",
                bytes_to_human(rss),
                rss as f64 / stats.used_memory.max(1) as f64,
            );
        }
        let _ = write!(
            s,
            "maxmemory:{}\r\nmaxmemory_human:{}\r\nmaxmemory_policy:{}\r\nmem_allocator:mimalloc\r\n\r\n",
            stats.maxmemory,
            bytes_to_human(stats.maxmemory),
            stats.policy.name(),
        );
    }
    if wants("persistence", true) {
        // Redis' field names, so monitoring tools find them.
        let _ = write!(s, "# Persistence\r\nloading:0\r\nasync_loading:0\r\n");
        match &ctx.snapshots {
            Some(snapshots) => {
                let status = snapshots.status();
                let _ = write!(
                    s,
                    "snapshots:on\r\nsnapshot_file:{}\r\nrdb_changes_since_last_save:{}\r\n\
                     rdb_bgsave_in_progress:{}\r\nrdb_last_save_time:{}\r\nrdb_last_bgsave_status:{}\r\n",
                    snapshots.path.display(),
                    snapshots.unsaved_changes(),
                    u8::from(snapshots.saving_in_background()),
                    status.last_save,
                    if status.last_ok { "ok" } else { "err" },
                );
            }
            None => {
                let _ = write!(
                    s,
                    "snapshots:off\r\nrdb_changes_since_last_save:0\r\nrdb_bgsave_in_progress:0\r\n\
                     rdb_last_save_time:{}\r\nrdb_last_bgsave_status:ok\r\n",
                    unix_seconds() - server.uptime.as_secs() as i64,
                );
            }
        }
        match ctx.aof() {
            Some(aof) => s.push_str(&aof.info()),
            None => s.push_str("aof_enabled:0\r\naof_rewrite_in_progress:0\r\n"),
        }
        s.push_str("\r\n");
    }
    if wants("stats", true) {
        let _ = write!(
            s,
            "# Stats\r\ntotal_connections_received:{}\r\ntotal_commands_processed:{}\r\n\
             rejected_connections:{}\r\nexpired_keys:{}\r\nevicted_keys:{}\r\n\
             keyspace_hits:{}\r\nkeyspace_misses:{}\r\npubsub_channels:{}\r\npubsub_patterns:{}\r\n\
             pubsubshard_channels:{}\r\nnative_scripts:{}\r\nnative_script_fallbacks:{}\r\n\r\n",
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
            ctx.scripting_stats().0,
            ctx.scripting_stats().1,
        );
    }
    if wants("replication", true) {
        let _ = write!(
            s,
            "# Replication\r\nrole:master\r\nconnected_slaves:0\r\nmaster_failover_state:no-failover\r\n\
             master_replid:{}\r\nmaster_replid2:0000000000000000000000000000000000000000\r\n\
             master_repl_offset:0\r\nsecond_repl_offset:-1\r\nrepl_backlog_active:0\r\n\
             repl_backlog_size:1048576\r\nrepl_backlog_first_byte_offset:0\r\nrepl_backlog_histlen:0\r\n\r\n",
            ctx.run_id(),
        );
    }
    if wants("cpu", true) {
        let (user, system) = cpu_seconds();
        let _ = write!(
            s,
            "# CPU\r\nused_cpu_sys:{system:.6}\r\nused_cpu_user:{user:.6}\r\nused_cpu_sys_children:0.000000\r\n\
             used_cpu_user_children:0.000000\r\n\r\n",
        );
    }
    if wants("modules", true) {
        s.push_str("# Modules\r\n\r\n");
    }
    if wants("commandstats", false) {
        s.push_str("# Commandstats\r\n\r\n");
    }
    if wants("errorstats", true) {
        s.push_str("# Errorstats\r\n\r\n");
    }
    if wants("latencystats", false) {
        s.push_str("# Latencystats\r\n\r\n");
    }
    if wants("cluster", true) {
        s.push_str("# Cluster\r\ncluster_enabled:0\r\n\r\n");
    }
    if wants("keyspace", true) {
        s.push_str("# Keyspace\r\n");
        for db in ctx.store.live() {
            let (keys, with_ttl) = db.key_counts();
            if keys > 0 {
                let _ = write!(
                    s,
                    "db{}:keys={keys},expires={with_ttl},avg_ttl=0\r\n",
                    db.index()
                );
            }
        }
    }
    resp::write_verbatim_text(out, s.as_bytes(), session.resp3);
    Reply::Written
}

/// The system's name and machine, as `uname` gives them.
fn uname() -> (String, String) {
    // SAFETY: `utsname` is plain bytes, and `uname` fills in what it's given.
    let name = unsafe {
        let mut name: libc::utsname = std::mem::zeroed();
        libc::uname(&mut name);
        name
    };
    let text = |field: &[libc::c_char]| {
        let bytes: Vec<u8> = field
            .iter()
            .take_while(|&&c| c != 0)
            .map(|&c| c as u8)
            .collect();
        String::from_utf8_lossy(&bytes).into_owned()
    };
    (
        format!("{} {}", text(&name.sysname), text(&name.release)),
        text(&name.machine),
    )
}

/// The CPU the process has used, in seconds: in user code, and in the system.
fn cpu_seconds() -> (f64, f64) {
    // SAFETY: `rusage` is plain numbers, and `getrusage` fills in what it's given.
    let usage = unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        usage
    };
    let seconds = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    (seconds(usage.ru_utime), seconds(usage.ru_stime))
}

/// `bytes` as Redis' `INFO` shows them for people.
fn bytes_to_human(bytes: usize) -> String {
    let b = bytes as f64;
    const K: f64 = 1024.0;
    match bytes {
        n if n < 1 << 10 => format!("{n}B"),
        n if n < 1 << 20 => format!("{:.2}K", b / K),
        n if n < 1 << 30 => format!("{:.2}M", b / K / K),
        n if n < 1 << 40 => format!("{:.2}G", b / K / K / K),
        n if n < 1 << 50 => format!("{:.2}T", b / K / K / K / K),
        _ => format!("{:.2}P", b / K / K / K / K / K),
    }
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

/// After an error that refuses a command: in a transaction, EXEC will
/// refuse it all.
fn refuse_in_multi(session: &mut Session) {
    if let Some(queue) = &mut session.multi {
        queue.refused = true;
    }
}

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

/// What a subcommand gets that its own code doesn't accept, or that isn't one.
fn subcommand_syntax_error(out: &mut Vec<u8>, sub: &[u8], command: &str) {
    let mut msg = b"ERR unknown subcommand or wrong number of arguments for '".to_vec();
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
