//! Replication, as Redis 7.2 has it, over the same protocol, so Redis and
//! Tantalum follow each other either way: a replica connects, says what it can
//! do (`REPLCONF`), and asks to continue from where it was (`PSYNC replid
//! offset`). If this server's stream still has that, it sends what came
//! since (`+CONTINUE`); otherwise the whole data as a Redis RDB file
//! (`+FULLRESYNC replid offset`, `src/rdb_file.rs`), and the stream after it.
//! The stream is every write, as Redis propagates it (`propagate.rs`), with
//! offsets counting its bytes, kept in a backlog (`backlog.rs`) that each
//! replica's thread sends from (`master.rs`). A replica (`replica.rs`) runs
//! what its master sends, tells it how far it got (`REPLCONF ACK`), and
//! passes the same bytes on to replicas of its own.
//!
//! None of this costs anything until a replica first connects (or this server
//! is told to follow one): then writes take turns, as with the append-only
//! log, so the stream has them in order, and reads leave expired keys for
//! writes to remove and propagate.

pub(crate) mod backlog;
pub(crate) mod failover;
pub(crate) mod master;
pub(crate) mod propagate;
pub(crate) mod replica;

use std::sync::Arc;
use std::sync::atomic::{
    AtomicBool, AtomicU16, AtomicU32, AtomicU64, AtomicUsize, Ordering::Relaxed,
};

use parking_lot::Mutex;

use crate::store::Store;

pub(crate) use master::Master;

/// This server's replication IDs, as Redis keeps them: its own, and the one
/// it had before (its old master's, once promoted) with the offset that one
/// holds to, so that replicas of the old master can continue here.
#[derive(Clone, Debug)]
pub(crate) struct Ids {
    pub(crate) id: String,
    pub(crate) previous: String,
    /// The offset after which `previous` no longer holds (-1: none).
    pub(crate) previous_until: i64,
}

/// A new random replication ID: 40 hex digits.
pub(crate) fn new_id() -> String {
    let mut bytes = [0u8; 20];
    let _ = ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

const NO_ID: &str = "0000000000000000000000000000000000000000";

/// What `CONFIG` sets of replication.
pub(crate) struct Settings {
    pub(crate) backlog_size: AtomicUsize,
    /// `repl-ping-replica-period`, seconds.
    pub(crate) ping_period: AtomicU64,
    /// `repl-timeout`, seconds.
    pub(crate) timeout: AtomicU64,
    pub(crate) masterauth: Mutex<Option<Vec<u8>>>,
    pub(crate) masteruser: Mutex<Option<Vec<u8>>>,
    pub(crate) min_replicas_to_write: AtomicUsize,
    /// `repl-backlog-ttl`, seconds: the stream is freed after this long
    /// without replicas (0: never).
    pub(crate) backlog_ttl: AtomicU64,
    /// `min-replicas-max-lag`, seconds.
    pub(crate) min_replicas_max_lag: AtomicU64,
    pub(crate) announce_ip: Mutex<Option<String>>,
    pub(crate) announce_port: AtomicU16,
    pub(crate) priority: AtomicU32,
    pub(crate) read_only: AtomicBool,
    pub(crate) serve_stale_data: AtomicBool,
    /// `--tls-replication`: how a replica connects to its master over TLS.
    pub(crate) tls: Mutex<Option<Arc<rustls::ClientConfig>>>,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            backlog_size: AtomicUsize::new(1 << 20),
            ping_period: AtomicU64::new(10),
            timeout: AtomicU64::new(60),
            masterauth: Mutex::new(None),
            masteruser: Mutex::new(None),
            min_replicas_to_write: AtomicUsize::new(0),
            backlog_ttl: AtomicU64::new(3600),
            min_replicas_max_lag: AtomicU64::new(10),
            announce_ip: Mutex::new(None),
            announce_port: AtomicU16::new(0),
            priority: AtomicU32::new(100),
            read_only: AtomicBool::new(true),
            serve_stale_data: AtomicBool::new(true),
            tls: Mutex::new(None),
        }
    }
}

/// Replication's state, in the `Context`.
pub(crate) struct Replication {
    ids: Mutex<Ids>,
    /// The stream replicas follow: made when the first one asks, or when this
    /// server starts following a master (for replicas of its own).
    master: Mutex<Option<Arc<Master>>>,
    /// Where the last stream ended, if one was freed (or a snapshot says where
    /// the data it loaded was): the next goes on from there.
    freed_at: Mutex<Option<u64>>,
    /// The store's change count when a snapshot's place in a stream was taken
    /// up (`resume`): if writes came before the next stream, that place no
    /// longer holds.
    resumed_at_changes: Mutex<Option<u64>>,
    /// The master this server follows, if any.
    pub(crate) link: Mutex<Option<Arc<replica::Link>>>,
    /// Whether `link` is set: read once a batch by connections (`special`).
    following: AtomicBool,
    /// What replicas said of themselves before their `PSYNC`, by client id.
    handshakes: Mutex<std::collections::HashMap<u64, master::Handshake>>,
    pub(crate) settings: Settings,
    pub(crate) syncs: Syncs,
    /// A writable replica's keys given a TTL by its own writes, by database:
    /// they expire here, as the master doesn't know of them (Redis'
    /// `slaveKeysWithExpire`).
    written_here: Mutex<std::collections::HashSet<(u32, Box<[u8]>)>>,
    /// A `FAILOVER` going on, if one is.
    pub(crate) failover: Mutex<failover::Failover>,
}

/// `INFO`'s counts of replicas' syncs: full, and continued or not.
#[derive(Default)]
pub(crate) struct Syncs {
    pub(crate) full: AtomicU64,
    pub(crate) partial_ok: AtomicU64,
    pub(crate) partial_err: AtomicU64,
}

impl Default for Replication {
    fn default() -> Self {
        Replication {
            ids: Mutex::new(Ids {
                id: new_id(),
                previous: NO_ID.to_string(),
                previous_until: -1,
            }),
            master: Mutex::new(None),
            freed_at: Mutex::new(None),
            resumed_at_changes: Mutex::new(None),
            link: Mutex::new(None),
            following: AtomicBool::new(false),
            handshakes: Mutex::new(std::collections::HashMap::new()),
            settings: Settings::default(),
            syncs: Syncs::default(),
            written_here: Mutex::new(std::collections::HashSet::new()),
            failover: Mutex::new(failover::Failover::default()),
        }
    }
}

impl Replication {
    pub(crate) fn ids(&self) -> Ids {
        self.ids.lock().clone()
    }

    pub(crate) fn set_ids(&self, ids: Ids) {
        *self.ids.lock() = ids;
    }

    /// The stream, if replicas may follow by now.
    pub(crate) fn master(&self) -> Option<Arc<Master>> {
        self.master.lock().clone()
    }

    /// The stream, made if there's none yet, starting after `offset`.
    /// Whether it was made now, for the caller to start logging to it.
    pub(crate) fn master_or_new(&self, offset: u64) -> (Arc<Master>, bool) {
        let mut master = self.master.lock();
        if let Some(master) = &*master {
            return (master.clone(), false);
        }
        let made = Arc::new(Master::new(
            self.settings.backlog_size.load(Relaxed),
            offset,
        ));
        self.resumed_at_changes.lock().take();
        made.set_following(self.following());
        *master = Some(made.clone());
        (made, true)
    }

    /// `repl-backlog-size`: for the stream there is, and one made later.
    pub(crate) fn set_backlog_size(&self, size: usize) {
        let size = size.max(16 * 1024);
        self.settings.backlog_size.store(size, Relaxed);
        if let Some(master) = self.master() {
            master.backlog.set_size(size);
        }
    }

    /// Frees the stream, which ended at `offset`: the next one goes on from
    /// there, under a new ID (see `master::disable`).
    pub(crate) fn free_master(&self, offset: u64) {
        *self.master.lock() = None;
        *self.freed_at.lock() = Some(offset);
        self.set_ids(Ids {
            id: new_id(),
            previous: NO_ID.to_string(),
            previous_until: -1,
        });
    }

    /// Whether writes came since a snapshot's place in a stream was taken up.
    pub(crate) fn written_since_resume(&self, store: &crate::store::Store) -> bool {
        let resumed = *self.resumed_at_changes.lock();
        resumed.is_some_and(|changes| store.changes() != changes)
    }

    /// Whether this server follows a master.
    #[inline]
    pub(crate) fn following(&self) -> bool {
        self.following.load(Relaxed)
    }

    pub(crate) fn set_following(&self, on: bool) {
        let master = self.master.lock();
        self.following.store(on, Relaxed);
        if let Some(master) = &*master {
            master.set_following(on);
        }
        drop(master);
        self.written_here.lock().clear();
    }

    /// After a writable replica's own writes: of the keys they wrote, those
    /// with a TTL now expire here, and those without no longer do.
    pub(crate) fn note_written_here(&self, store: &Arc<Store>, keys: Vec<(u32, Box<[u8]>)>) {
        let mut here = self.written_here.lock();
        for (db, key) in keys {
            let db_store = store.db(db as usize);
            let expires = db_store.update_key(db_store.key(&key), store.now(), |c| {
                (
                    crate::store::Update::Peek,
                    c.is_some_and(|c| c.expiry != crate::store::Expiry::Never),
                )
            });
            if expires.unwrap_or(false) {
                here.insert((db, key));
            } else {
                here.remove(&(db, key));
            }
        }
    }

    /// Whether a writable replica has keys of its own to expire.
    pub(crate) fn any_written_here(&self) -> bool {
        !self.written_here.lock().is_empty()
    }

    /// Expires a writable replica's own keys whose time has come (in a
    /// write's turn, which removes them, and keeps the removal here), and
    /// forgets those gone or no longer expiring (`expireSlaveKeys`).
    pub(crate) fn expire_written_here(&self, ctx: &crate::context::Context) {
        let Some(log) = ctx.log() else {
            return;
        };
        let keys: Vec<_> = self.written_here.lock().iter().cloned().collect();
        let mut gone = Vec::new();
        {
            let turn = log.turn(&ctx.store, false, 0);
            for (db, key) in &keys {
                crate::aof::select(*db as usize);
                let store = ctx.store.db(*db as usize);
                let expires = store.update_key(store.key(key), turn.now, |c| {
                    (
                        crate::store::Update::Peek,
                        c.is_some_and(|c| c.expiry != crate::store::Expiry::Never),
                    )
                });
                if !expires.unwrap_or(false) {
                    gone.push((*db, key.clone()));
                }
            }
        }
        let mut here = self.written_here.lock();
        for key in gone {
            here.remove(&key);
        }
    }

    /// A full sync replaced the data: the keys this replica wrote are gone.
    pub(crate) fn forget_written_here(&self) {
        self.written_here.lock().clear();
    }

    /// Whether this server follows a master and is connected to it, synced.
    pub(crate) fn link_up(&self) -> bool {
        self.link.lock().as_ref().is_some_and(|link| link.up())
    }

    /// `master_repl_offset`: how far the stream has got (0 if there's none).
    pub(crate) fn offset(&self) -> u64 {
        if let Some(master) = self.master() {
            return master.backlog.offset();
        }
        if let Some(link) = self.link.lock().as_ref() {
            return link.offset();
        }
        self.freed_at.lock().unwrap_or(0)
    }
}

/// `INFO`'s replication section, as Redis 7.2 writes it.
pub(crate) fn info(ctx: &crate::context::Context) -> String {
    use std::fmt::Write as _;
    let repl = &ctx.repl;
    let ids = repl.ids();
    let master = repl.master();
    let mut s = String::from("# Replication\r\n");
    let link = repl.link.lock().clone();
    match &link {
        Some(link) => {
            let state = link.state.load(Relaxed);
            let up = state == replica::CONNECTED;
            let offset = link.offset();
            let _ = write!(
                s,
                "role:slave\r\nmaster_host:{}\r\nmaster_port:{}\r\nmaster_link_status:{}\r\n\
                 master_last_io_seconds_ago:{}\r\nmaster_sync_in_progress:{}\r\n\
                 slave_read_repl_offset:{offset}\r\nslave_repl_offset:{offset}\r\n",
                link.host,
                link.port,
                if up { "up" } else { "down" },
                if up {
                    link.last_io_seconds() as i64
                } else {
                    -1
                },
                u8::from(state == replica::SYNC),
            );
            if state == replica::SYNC {
                let (total, read) = (link.sync_size.load(Relaxed), link.sync_read.load(Relaxed));
                let perc = if total > 0 {
                    read as f64 * 100.0 / total as f64
                } else {
                    0.0
                };
                let _ = write!(
                    s,
                    "master_sync_total_bytes:{}\r\nmaster_sync_read_bytes:{read}\r\n\
                     master_sync_left_bytes:{}\r\nmaster_sync_perc:{perc:.2}\r\n\
                     master_sync_last_io_seconds_ago:{}\r\n",
                    if total > 0 { total as i64 } else { -1 },
                    if total > 0 {
                        total.saturating_sub(read) as i64
                    } else {
                        -1
                    },
                    link.last_io_seconds(),
                );
            }
            if !up {
                let _ = write!(
                    s,
                    "master_link_down_since_seconds:{}\r\n",
                    link.down_seconds()
                );
            }
            let _ = write!(
                s,
                "slave_priority:{}\r\nslave_read_only:{}\r\nreplica_announced:1\r\n",
                repl.settings.priority.load(Relaxed),
                u8::from(repl.settings.read_only.load(Relaxed)),
            );
        }
        None => s.push_str("role:master\r\n"),
    }
    let followers = master.as_ref().map(|m| m.followers()).unwrap_or_default();
    let _ = write!(s, "connected_slaves:{}\r\n", followers.len());
    for (i, follower) in followers.iter().enumerate() {
        let state = match follower.state.load(Relaxed) {
            master::WAIT_BGSAVE => "wait_bgsave",
            master::SEND_BULK => "send_bulk",
            _ => "online",
        };
        let lag = master.as_ref().map_or(0, |m| m.lag(follower));
        let _ = write!(
            s,
            "slave{i}:ip={},port={},state={state},offset={},lag={lag}\r\n",
            follower.ip,
            follower.port,
            follower.ack.load(Relaxed),
        );
    }
    let offset = repl.offset();
    let (first, histlen, size, active) = match &master {
        Some(master) => {
            let (first, histlen) = master.backlog.extent();
            (first, histlen, master.backlog.size(), 1)
        }
        None => (0, 0, repl.settings.backlog_size.load(Relaxed), 0),
    };
    let _ = write!(
        s,
        "master_failover_state:{}\r\nmaster_replid:{}\r\nmaster_replid2:{}\r\n\
         master_repl_offset:{offset}\r\nsecond_repl_offset:{}\r\nrepl_backlog_active:{active}\r\n\
         repl_backlog_size:{size}\r\nrepl_backlog_first_byte_offset:{first}\r\n\
         repl_backlog_histlen:{histlen}\r\n\r\n",
        repl.failover.lock().state.name(),
        ids.id,
        ids.previous,
        if ids.previous_until >= 0 {
            ids.previous_until + 1
        } else {
            -1
        },
    );
    s
}

/// `ROLE`.
pub(crate) fn role(ctx: &crate::context::Context, out: &mut Vec<u8>) {
    use crate::resp;
    let repl = &ctx.repl;
    if let Some(link) = repl.link.lock().clone() {
        let state = match link.state.load(Relaxed) {
            replica::CONNECT => "connect",
            replica::CONNECTING => "connecting",
            replica::SYNC => "sync",
            _ => "connected",
        };
        resp::write_array_header(out, 5);
        resp::write_bulk(out, b"slave");
        resp::write_bulk(out, link.host.as_bytes());
        resp::write_integer(out, i64::from(link.port));
        resp::write_bulk(out, state.as_bytes());
        resp::write_integer(out, if link.up() { link.offset() as i64 } else { -1 });
        return;
    }
    let followers = repl.master().map(|m| m.followers()).unwrap_or_default();
    resp::write_array_header(out, 3);
    resp::write_bulk(out, b"master");
    resp::write_integer(out, repl.offset() as i64);
    let online: Vec<_> = followers
        .iter()
        .filter(|f| f.state.load(Relaxed) == master::ONLINE)
        .collect();
    resp::write_array_header(out, online.len());
    for follower in online {
        resp::write_array_header(out, 3);
        resp::write_bulk(out, follower.ip.as_bytes());
        resp::write_bulk(out, follower.port.to_string().as_bytes());
        resp::write_bulk(out, follower.ack.load(Relaxed).to_string().as_bytes());
    }
}

/// `REPLICAOF host port` and `REPLICAOF NO ONE`.
pub(crate) fn replicaof(
    ctx: &crate::context::Context,
    args: &crate::cmd::Args<'_>,
    out: &mut Vec<u8>,
) {
    use crate::resp;
    if ctx.repl.failover.lock().state != failover::State::None {
        resp::write_error(out, b"ERR REPLICAOF not allowed while failing over.");
        return;
    }
    let (host, port) = (args.get(1), args.get(2));
    if host.eq_ignore_ascii_case(b"NO") && port.eq_ignore_ascii_case(b"ONE") {
        replica::promote(ctx);
        out.extend_from_slice(resp::OK);
        return;
    }
    let Some(port) = resp::parse_int(port).and_then(|p| u16::try_from(p).ok()) else {
        resp::write_error(out, b"ERR Invalid master port");
        return;
    };
    let host = String::from_utf8_lossy(host).into_owned();
    if let Some(link) = ctx.repl.link.lock().as_ref()
        && link.host.eq_ignore_ascii_case(&host)
        && link.port == port
    {
        out.extend_from_slice(b"+OK Already connected to specified master\r\n");
        return;
    }
    let Some(ctx) = ctx.arc() else {
        resp::write_error(out, b"ERR replication needs a running server");
        return;
    };
    replica::follow(&ctx, host, port);
    out.extend_from_slice(resp::OK);
}

/// Whether this thread is a replica's, running what its master sent: its
/// commands pass the checks other clients' get on a replica (`-READONLY`,
/// `CLIENT PAUSE`, a busy script), and no key expires on it by this server's
/// clock. Looked at only on the checked path.
#[cold]
pub(crate) fn applying() -> bool {
    crate::segment::expiry_suspended()
}

/// Replication's settings given at startup: the password and user to `AUTH`
/// to a master with, and the backlog's size.
pub fn configure(
    ctx: &crate::context::Context,
    masterauth: Option<Vec<u8>>,
    masteruser: Option<Vec<u8>>,
    backlog_size: Option<usize>,
) {
    let settings = &ctx.repl.settings;
    if let Some(password) = masterauth {
        ctx.config
            .set("masterauth", &String::from_utf8_lossy(&password));
        *settings.masterauth.lock() = Some(password);
    }
    if let Some(user) = masteruser {
        ctx.config
            .set("masteruser", &String::from_utf8_lossy(&user));
        *settings.masteruser.lock() = Some(user);
    }
    if let Some(size) = backlog_size {
        ctx.config.set("repl-backlog-size", &size.to_string());
        settings.backlog_size.store(size.max(16 * 1024), Relaxed);
    }
}

/// A replica connects to its master over TLS (`--tls-replication yes`),
/// trusting the certificates in `ca_file` (`--tls-ca-cert-file`): as CAs, or
/// one the master presents as it is (a self-signed one).
pub fn use_tls(ctx: &crate::context::Context, ca_file: &std::path::Path) -> std::io::Result<()> {
    let config = replica::tls_config(ca_file)?;
    *ctx.repl.settings.tls.lock() = Some(config);
    ctx.config.set("tls-replication", "yes");
    ctx.config
        .set("tls-ca-cert-file", &ca_file.display().to_string());
    Ok(())
}

/// The data a snapshot loaded was at offset `offset` of the replication
/// stream with ID `id`: replicas of this server (or, with `--replicaof`, its
/// master) can continue from there, if no write comes first.
pub fn resume(ctx: &crate::context::Context, id: String, offset: u64) {
    let ids = ctx.repl.ids();
    ctx.repl.set_ids(Ids { id, ..ids });
    *ctx.repl.freed_at.lock() = Some(offset);
    *ctx.repl.resumed_at_changes.lock() = Some(ctx.store.changes());
}

/// Follows the master at `host:port` (`--replicaof`): `ctx` must have been
/// adopted (`Context::adopt`).
pub fn follow(ctx: &Arc<crate::context::Context>, host: String, port: u16) {
    ctx.config.set("replicaof", &format!("{host} {port}"));
    replica::follow(ctx, host, port);
}
