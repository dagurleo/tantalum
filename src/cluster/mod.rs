//! Cluster mode (`--cluster-enabled yes`): Redis 7.2's cluster, over its
//! bus protocol (`wire.rs`), so Tantalum and Redis nodes can be in one
//! cluster, and its clients and tools (`redis-cli --cluster`) work as they
//! do with Redis'. Keys hash to 16384 slots; each master serves some, and a
//! command whose keys aren't in this node's slots is answered `-MOVED slot
//! host:port` (or `-ASK` while the slot moves, `-CROSSSLOT` for keys in
//! several slots), as Redis answers. Only database 0, as in Redis.
//!
//! Commands take the checked path, which asks [`route`] first; outside
//! cluster mode nothing here runs, and the store is as it is without it
//! (finding a slot's keys looks at every key, which only moving a slot
//! does).

pub(crate) mod bus;
pub(crate) mod command;
pub(crate) mod state;
pub(crate) mod wire;

use std::path::PathBuf;
use std::sync::{Arc, OnceLock, Weak};

use parking_lot::Mutex;

use crate::context::{Context, Session};
use state::{Settings, State};

/// CRC16-CCITT (XMODEM), as Redis' `crc16`, a byte at a time by table.
const CRC16: [u16; 256] = {
    let mut table = [0u16; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = (i as u16) << 8;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

fn crc16(bytes: &[u8]) -> u16 {
    bytes.iter().fold(0u16, |crc, &b| {
        (crc << 8) ^ CRC16[usize::from((crc >> 8) as u8 ^ b)]
    })
}

/// `keyHashSlot`: the slot of `key`, or of what's between its first `{`
/// and the `}` after it, if that isn't empty.
pub fn key_slot(key: &[u8]) -> u16 {
    if let Some(s) = key.iter().position(|&b| b == b'{')
        && let Some(len) = key[s + 1..].iter().position(|&b| b == b'}')
        && len > 0
    {
        return crc16(&key[s + 1..s + 1 + len]) & 0x3fff;
    }
    crc16(key) & 0x3fff
}

/// A node's part in a cluster.
pub struct Cluster {
    pub(crate) state: Mutex<State>,
    ctx: OnceLock<Weak<Context>>,
}

/// What a node is started with (`--cluster-*`).
#[derive(Clone, Debug)]
pub struct Options {
    pub config_file: PathBuf,
    pub node_timeout: i64,
    pub port: u16,
    /// 0: the client port + 10000.
    pub cport: u16,
    pub announce_ip: Option<String>,
    pub announce_hostname: Option<String>,
    pub require_full_coverage: bool,
    pub allow_reads_when_down: bool,
    pub replica_no_failover: bool,
    pub replica_validity_factor: i64,
    pub bind: Vec<String>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            config_file: PathBuf::from("nodes.conf"),
            node_timeout: 15_000,
            port: 6379,
            cport: 0,
            announce_ip: None,
            announce_hostname: None,
            require_full_coverage: true,
            allow_reads_when_down: false,
            replica_no_failover: false,
            replica_validity_factor: 10,
            bind: vec!["0.0.0.0".into()],
        }
    }
}

impl Cluster {
    /// The node `options` describe: what its file says (or a new node,
    /// whose file is written now).
    pub fn new(options: &Options) -> Result<Arc<Cluster>, String> {
        let cport = if options.cport != 0 {
            options.cport
        } else {
            options
                .port
                .checked_add(10_000)
                .ok_or("Redis port number too high. Cluster communication port is 10,000 port numbers higher than your Redis port. Your Redis port number must be 55535 or less.")?
        };
        let settings = Settings {
            node_timeout: options.node_timeout,
            port: options.port,
            cport,
            announce_ip: options.announce_ip.clone(),
            announce_hostname: options.announce_hostname.clone().unwrap_or_default(),
            require_full_coverage: options.require_full_coverage,
            allow_reads_when_down: options.allow_reads_when_down,
            replica_no_failover: options.replica_no_failover,
            replica_validity_factor: options.replica_validity_factor,
            ..Settings::default()
        };
        let mut state = State::new(settings, options.config_file.clone());
        if state.load_config()? {
            state::log(&format!(
                "Node configuration loaded, I'm {}",
                state::name_str(&state.myself)
            ));
        } else {
            state::log(&format!(
                "No cluster configuration found, I'm {}",
                state::name_str(&state.myself)
            ));
            state.save_config().map_err(|e| {
                format!(
                    "Can't create the cluster config file {}: {e}",
                    options.config_file.display()
                )
            })?;
        }
        let hostname = state.settings.announce_hostname.clone();
        let me = state.myself;
        if let Some(n) = state.nodes.get_mut(&me) {
            n.hostname = hostname;
        }
        state.update_state();
        Ok(Arc::new(Cluster {
            state: Mutex::new(state),
            ctx: OnceLock::new(),
        }))
    }

    /// Starts the bus, once the server's context is there.
    pub fn start(self: &Arc<Self>, ctx: &Arc<Context>, bind: Vec<String>) -> std::io::Result<()> {
        let _ = self.ctx.set(bus::weak(ctx));
        // A replica by its file follows its master at once.
        {
            let mut state = self.state.lock();
            let me = state.me();
            if me.is_slave()
                && let Some(m) = me.slaveof.and_then(|m| state.nodes.get(&m))
            {
                let (ip, port) = (m.ip.clone(), m.tcp_port);
                state.actions.push(state::Action::Follow(ip, port));
            }
        }
        bus::after_command(self);
        bus::start(self.clone(), bind)
    }

    pub(crate) fn ctx(&self) -> Option<Arc<Context>> {
        self.ctx.get().and_then(Weak::upgrade)
    }

    /// `PUBLISH`/`SPUBLISH` here: to the other nodes too.
    pub(crate) fn propagate_publish(&self, channel: &[u8], message: &[u8], shard: bool) {
        self.state.lock().propagate_publish(channel, message, shard);
    }
}

/// A client's cluster flags.
pub(crate) const ASKING: u8 = 1;
pub(crate) const READONLY: u8 = 2;

/// Why a command isn't served here (`clusterRedirectClient`).
enum Redirect {
    CrossSlot,
    Unstable,
    Down,
    DownReadOnly,
    Unbound,
    Moved(u16, String, u16),
    Ask(u16, String, u16),
}

impl Redirect {
    fn write(&self, out: &mut Vec<u8>) {
        let msg = match self {
            Redirect::CrossSlot => {
                "CROSSSLOT Keys in request don't hash to the same slot".to_string()
            }
            Redirect::Unstable => {
                "TRYAGAIN Multiple keys request during rehashing of slot".to_string()
            }
            Redirect::Down => "CLUSTERDOWN The cluster is down".to_string(),
            Redirect::DownReadOnly => {
                "CLUSTERDOWN The cluster is down and only accepts read commands".to_string()
            }
            Redirect::Unbound => "CLUSTERDOWN Hash slot not served".to_string(),
            Redirect::Moved(slot, host, port) => format!("MOVED {slot} {host}:{port}"),
            Redirect::Ask(slot, host, port) => format!("ASK {slot} {host}:{port}"),
        };
        crate::resp::write_error(out, msg.as_bytes());
    }
}

/// One command's name and keys, for [`route`].
pub(crate) struct Query<'a> {
    pub(crate) writes: bool,
    pub(crate) keys: Vec<&'a [u8]>,
    /// `MIGRATE`, which may run in a slot that's moving.
    pub(crate) migrate: bool,
    /// Sharded pub/sub (`SSUBSCRIBE`, `SPUBLISH`, ...).
    pub(crate) pubsub_shard: bool,
    /// `RESTORE-ASKING`: as if after `ASKING`.
    pub(crate) asking: bool,
}

/// `getNodeByQuery`: whether this node serves `queries` (a command, or a
/// transaction's), and if not, the error that says where to go. `exists`
/// says whether a key is here (for a slot moving in or out).
pub(crate) fn route(
    cluster: &Cluster,
    session: &Session,
    queries: &[Query<'_>],
    exists: impl Fn(&[u8]) -> bool,
    out: &mut Vec<u8>,
) -> bool {
    let state = cluster.state.lock();
    let myself = state.myself;
    let mut first: Option<&[u8]> = None;
    let mut slot = 0u16;
    let mut owner = None;
    let (mut migrating, mut importing, mut multiple) = (false, false, false);
    let (mut missing, mut existing) = (0, 0);
    let pubsub_shard = queries.iter().any(|q| q.pubsub_shard);
    let mut writes = false;
    for q in queries {
        writes |= q.writes;
        for &key in &q.keys {
            let this = key_slot(key);
            match first {
                None => {
                    first = Some(key);
                    slot = this;
                    owner = state.slots[slot as usize];
                    if owner.is_none() {
                        Redirect::Unbound.write(out);
                        return false;
                    }
                    if owner == Some(myself) && state.migrating[slot as usize].is_some() {
                        migrating = true;
                    } else if state.importing[slot as usize].is_some() {
                        importing = true;
                    }
                }
                Some(f) => {
                    if this != slot {
                        Redirect::CrossSlot.write(out);
                        return false;
                    }
                    if importing && !multiple && f != key {
                        multiple = true;
                    }
                }
            }
            if (migrating || importing) && !pubsub_shard {
                if exists(key) {
                    existing += 1;
                } else {
                    missing += 1;
                }
            }
        }
    }
    let Some(owner) = owner else {
        return true;
    };
    if state.state != state::OK {
        if pubsub_shard {
            if !state.settings.allow_pubsubshard_when_down {
                Redirect::Down.write(out);
                return false;
            }
        } else if !state.settings.allow_reads_when_down {
            Redirect::Down.write(out);
            return false;
        } else if writes {
            Redirect::DownReadOnly.write(out);
            return false;
        }
    }
    let migrate = queries.iter().any(|q| q.migrate);
    if (migrating || importing) && migrate {
        return true;
    }
    let addr = |name: wire::Name| {
        state
            .nodes
            .get(&name)
            .map(|n| (n.ip.clone(), n.tcp_port))
            .unwrap_or_default()
    };
    if migrating && missing > 0 {
        if existing > 0 {
            Redirect::Unstable.write(out);
        } else {
            let (host, port) = addr(state.migrating[slot as usize].expect("migrating"));
            Redirect::Ask(slot, host, port).write(out);
        }
        return false;
    }
    let asking = session.cluster & ASKING != 0 || queries.iter().any(|q| q.asking);
    if importing && asking {
        if multiple && missing > 0 {
            Redirect::Unstable.write(out);
            return false;
        }
        return true;
    }
    let me = state.me();
    if (session.cluster & READONLY != 0 || pubsub_shard)
        && !writes
        && me.is_slave()
        && me.slaveof == Some(owner)
    {
        return true;
    }
    if owner != myself {
        let (host, port) = addr(owner);
        Redirect::Moved(slot, host, port).write(out);
        return false;
    }
    true
}

/// The keys of slot `slot` in this node (up to `max`). The store isn't laid
/// out by slot (that would cost every key's lookup, cluster or not), so this
/// looks at every key: a slot's keys found in one pass.
pub(crate) fn keys_in_slot(ctx: &Context, slot: u16, max: usize) -> Vec<Vec<u8>> {
    let store = &ctx.store;
    let mut keys = Vec::new();
    for shard in 0..store.shard_count() {
        if keys.len() >= max {
            break;
        }
        store.for_each_key(shard, 0, |key, _| {
            if keys.len() < max && key_slot(key) == slot {
                keys.push(key.to_vec());
            }
        });
    }
    keys
}

pub(crate) fn count_keys_in_slot(ctx: &Context, slot: u16) -> usize {
    let store = &ctx.store;
    let mut n = 0;
    for shard in 0..store.shard_count() {
        store.for_each_key(shard, 0, |key, _| {
            if key_slot(key) == slot {
                n += 1;
            }
        });
    }
    n
}

/// `delKeysInSlot`: each `DEL`ed as a command would be (so the log and
/// replicas have it).
pub(crate) fn delete_keys_in_slot(ctx: &Context, slot: u16) {
    loop {
        let keys = keys_in_slot(ctx, slot, 1000);
        if keys.is_empty() {
            return;
        }
        let mut session = Session::for_replay();
        session.set_authenticated(true);
        for key in keys {
            let words: Vec<&[u8]> = vec![b"DEL", &key];
            run_internal(ctx, &mut session, &words);
        }
    }
}

fn run_internal(ctx: &Context, session: &mut Session, words: &[&[u8]]) {
    let mut buf = Vec::new();
    let mut ranges = Vec::new();
    for w in words {
        ranges.push(buf.len()..buf.len() + w.len());
        buf.extend_from_slice(w);
    }
    let args = crate::cmd::Args::new(&buf, &ranges);
    let mut out = Vec::new();
    let now = ctx.store.now();
    let _ = crate::cmd::execute(ctx, session, now, &args, &mut out);
}

/// Empties the data (`CLUSTER RESET` of a replica): `FLUSHALL`.
pub(crate) fn empty_data(ctx: &Context) {
    let mut session = Session::for_replay();
    session.set_authenticated(true);
    run_internal(ctx, &mut session, &[b"FLUSHALL"]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_are_redis_slots() {
        // As Redis' CLUSTER KEYSLOT says.
        assert_eq!(key_slot(b"foo"), 12182);
        assert_eq!(key_slot(b"bar"), 5061);
        assert_eq!(key_slot(b"{user1000}.following"), key_slot(b"user1000"));
        assert_eq!(key_slot(b"foo{}{bar}"), key_slot(b"foo{}{bar}"));
        assert_ne!(key_slot(b"foo{}{bar}"), key_slot(b"bar"));
        assert_eq!(key_slot(b"foo{{bar}}zap"), key_slot(b"{bar"));
        assert_eq!(key_slot(b""), 0);
    }
}
