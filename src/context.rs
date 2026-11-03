//! Server-wide state shared by every connection (settings, connected
//! clients, counters, shutdown), and each connection's own.

use std::collections::HashMap;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::watch;

use crate::resp;
use crate::snapshot::Snapshots;
use crate::store::Store;

/// Settings that apply to connections rather than the store.
#[derive(Debug, Clone)]
pub struct Settings {
    /// `requirepass`: clients must `AUTH` with it before anything else.
    pub password: Option<Vec<u8>>,
    /// Connections beyond this many are refused with an error.
    pub maxclients: usize,
    /// Connections idle this long are closed. `None` keeps them forever.
    pub timeout: Option<Duration>,
    /// Probe connections idle this long, to find clients that vanished
    /// without closing them. `None` turns probes off.
    pub tcp_keepalive: Option<Duration>,
    /// Tests only: an otherwise unknown command that panics, to check that a
    /// panic closes just its own connection.
    #[doc(hidden)]
    pub panic_command: Option<&'static str>,
}

impl Default for Settings {
    /// Redis' defaults.
    fn default() -> Self {
        Settings {
            password: None,
            maxclients: 10_000,
            timeout: None,
            tcp_keepalive: Some(Duration::from_secs(300)),
            panic_command: None,
        }
    }
}

/// A connection reports its buffer memory to the store only once it has
/// changed by this much: buffers come and go with every request (an idle
/// connection holds none), and reporting each would make every request
/// write one shared counter. So the count shows big buffers (a large value
/// or reply), within this much per connection.
const CLIENT_MEMORY_SLACK: usize = 64 * 1024;

/// Counter slots, one per thread (modulo this), so threads don't contend on
/// the counters' cache lines.
const COUNTER_SLOTS: usize = 64;

#[derive(Default)]
#[repr(align(128))]
struct Counters {
    commands: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
}

static NEXT_SLOT: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static SLOT: usize = NEXT_SLOT.fetch_add(1, Relaxed) % COUNTER_SLOTS;
}

/// Server-wide state that connections and commands use besides the store.
pub struct Context {
    pub store: Arc<Store>,
    pub settings: Settings,
    /// Set when the server saves snapshots (`--snapshot`).
    pub snapshots: Option<Arc<Snapshots>>,
    started: Instant,
    next_client_id: AtomicU64,
    /// Open connections, by client id, so shutdown can reach them.
    clients: Mutex<HashMap<u64, RawFd>>,
    connected: AtomicUsize,
    total_connections: AtomicU64,
    rejected_connections: AtomicU64,
    counters: Box<[Counters]>,
    shutting_down: AtomicBool,
    /// Wakes the accept loops when shutdown starts.
    shutdown: watch::Sender<bool>,
}

impl Context {
    pub fn new(store: Arc<Store>, settings: Settings) -> Self {
        Context {
            store,
            settings,
            snapshots: None,
            started: Instant::now(),
            next_client_id: AtomicU64::new(1),
            clients: Mutex::new(HashMap::new()),
            connected: AtomicUsize::new(0),
            total_connections: AtomicU64::new(0),
            rejected_connections: AtomicU64::new(0),
            counters: (0..COUNTER_SLOTS).map(|_| Counters::default()).collect(),
            shutting_down: AtomicBool::new(false),
            shutdown: watch::Sender::new(false),
        }
    }

    /// Saves snapshots with `snapshots` (for `SAVE`, `BGSAVE` and `INFO`).
    pub fn with_snapshots(mut self, snapshots: Arc<Snapshots>) -> Self {
        self.snapshots = Some(snapshots);
        self
    }

    /// Registers a new connection on `fd`. `None` if `maxclients` are
    /// connected already (counted as rejected) or the server is shutting
    /// down; the caller should close it.
    pub fn connect(&self, fd: RawFd) -> Option<Client<'_>> {
        if self.connected.fetch_add(1, Relaxed) >= self.settings.maxclients {
            self.connected.fetch_sub(1, Relaxed);
            self.rejected_connections.fetch_add(1, Relaxed);
            return None;
        }
        let id = self.next_client_id.fetch_add(1, Relaxed);
        {
            // Checked under the lock `shutdown` holds while closing
            // connections, so a connection either gets closed by it or sees
            // the flag here.
            let mut clients = self.clients.lock();
            if self.shutting_down.load(Relaxed) {
                drop(clients);
                self.connected.fetch_sub(1, Relaxed);
                return None;
            }
            clients.insert(id, fd);
        }
        self.total_connections.fetch_add(1, Relaxed);
        Some(Client {
            ctx: self,
            id,
            reported: 0,
        })
    }

    /// Starts a graceful shutdown: accept loops stop, and every connection
    /// finishes the commands it has read, sends their replies and closes.
    /// The read side of each socket is shut down, so a connection notices at
    /// its next read, and nothing has to check a flag per request.
    pub fn shutdown(&self) {
        let clients = self.clients.lock();
        self.shutting_down.store(true, Relaxed);
        self.shutdown.send_replace(true);
        for &fd in clients.values() {
            // SAFETY: the fd is open: a connection deregisters (under this
            // lock) before closing its socket.
            unsafe { libc::shutdown(fd, libc::SHUT_RD) };
        }
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Relaxed)
    }

    /// Resolves once shutdown has started.
    pub async fn until_shutdown(&self) {
        let mut rx = self.shutdown.subscribe();
        let _ = rx.wait_for(|&down| down).await;
    }

    /// Connections open right now.
    pub fn connected(&self) -> usize {
        self.connected.load(Relaxed)
    }

    pub fn has_password(&self) -> bool {
        self.settings.password.is_some()
    }

    /// Whether these credentials are valid. There is one user, `default`,
    /// which takes any password when none is configured (as in Redis).
    pub fn check_password(&self, user: &[u8], password: &[u8]) -> bool {
        user == b"default"
            && self
                .settings
                .password
                .as_ref()
                .is_none_or(|expected| constant_time_eq(expected, password))
    }

    /// Adds a session's counts since the last call to the server's totals.
    pub fn publish(&self, session: &mut Session) {
        if session.commands == 0 {
            return;
        }
        let slot = &self.counters[SLOT.with(|&slot| slot)];
        slot.commands.fetch_add(session.commands, Relaxed);
        slot.hits.fetch_add(session.hits, Relaxed);
        slot.misses.fetch_add(session.misses, Relaxed);
        (session.commands, session.hits, session.misses) = (0, 0, 0);
    }

    pub fn stats(&self) -> ServerStats {
        let sum = |field: fn(&Counters) -> &AtomicU64| {
            self.counters.iter().map(|c| field(c).load(Relaxed)).sum()
        };
        ServerStats {
            uptime: self.started.elapsed(),
            connected_clients: self.connected(),
            total_connections: self.total_connections.load(Relaxed),
            rejected_connections: self.rejected_connections.load(Relaxed),
            total_commands: sum(|c| &c.commands),
            keyspace_hits: sum(|c| &c.hits),
            keyspace_misses: sum(|c| &c.misses),
        }
    }
}

/// Compares without stopping at the first difference, so response times
/// don't reveal how much of a guessed password was right.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Server totals for `INFO`. Counts lag by up to one read's worth of
/// commands per connection.
#[derive(Debug)]
pub struct ServerStats {
    pub uptime: Duration,
    pub connected_clients: usize,
    pub total_connections: u64,
    pub rejected_connections: u64,
    pub total_commands: u64,
    /// Lookups by `GET`, `MGET`, `GETEX` and `GETDEL` that found the key or
    /// didn't.
    pub keyspace_hits: u64,
    pub keyspace_misses: u64,
}

/// A connection, counted as connected until dropped. Drop it before
/// closing the socket (see [`Context::shutdown`]).
pub struct Client<'a> {
    ctx: &'a Context,
    id: u64,
    /// The connection's buffer bytes as last reported to the store.
    reported: usize,
}

impl Client<'_> {
    /// Per-connection state for commands.
    pub fn session(&self) -> Session {
        Session {
            id: self.id,
            authenticated: !self.ctx.has_password(),
            name: None,
            resp3: false,
            commands: 0,
            hits: 0,
            misses: 0,
        }
    }

    /// Records the connection's buffer capacity, which counts against
    /// `maxmemory` like stored data, as in Redis (see `CLIENT_MEMORY_SLACK`).
    pub fn set_buffer_bytes(&mut self, bytes: usize) {
        if bytes.abs_diff(self.reported) >= CLIENT_MEMORY_SLACK {
            self.report(bytes);
        }
    }

    fn report(&mut self, bytes: usize) {
        self.ctx
            .store
            .add_client_memory(bytes as isize - self.reported as isize);
        self.reported = bytes;
    }
}

impl Drop for Client<'_> {
    fn drop(&mut self) {
        self.report(0);
        self.ctx.clients.lock().remove(&self.id);
        self.ctx.connected.fetch_sub(1, Relaxed);
    }
}

/// Per-connection state.
pub struct Session {
    pub(crate) id: u64,
    pub(crate) authenticated: bool,
    /// Set by `CLIENT SETNAME` or `HELLO ... SETNAME`.
    pub(crate) name: Option<Box<[u8]>>,
    /// Whether the client switched to RESP3 with `HELLO 3`.
    pub(crate) resp3: bool,
    /// Counts not yet added to the server's (see [`Context::publish`]).
    pub(crate) commands: u64,
    pub(crate) hits: u64,
    pub(crate) misses: u64,
}

impl Session {
    /// The null reply in the connection's protocol.
    pub(crate) fn null(&self) -> &'static [u8] {
        if self.resp3 {
            resp::NULL
        } else {
            resp::NULL_BULK
        }
    }

    /// Counts a key lookup for `keyspace_hits` and `keyspace_misses`.
    pub(crate) fn count_lookup(&mut self, hit: bool) {
        if hit {
            self.hits += 1;
        } else {
            self.misses += 1;
        }
    }
}
