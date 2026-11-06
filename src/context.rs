//! Server-wide state shared by every connection (settings, connected
//! clients, counters, shutdown), and each connection's own.

use std::collections::HashMap;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::watch;

use crate::pubsub::{PubSub, Subscriber, Subscriptions};
use crate::resp;
use crate::script::Scripting;
use crate::snapshot::Snapshots;
use crate::store::Store;
use crate::transaction::{Queue, Watching};

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
    /// Whether BullMQ's hottest scripts run natively (`src/bullmq.rs`).
    pub native_scripts: bool,
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
            native_scripts: true,
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
    /// The append-only log (`--aof`), once it's taking writes.
    aof: std::sync::OnceLock<Arc<crate::aof::Aof>>,
    /// Who is subscribed to what.
    pub pubsub: Arc<PubSub>,
    /// The most memory seen in use, as `INFO` reports it.
    memory_peak: AtomicUsize,
    /// Lua scripts and their interpreter, made on the first one.
    pub(crate) scripting: Scripting,
    started: Instant,
    next_client_id: AtomicU64,
    /// Open connections, by client id, so shutdown can reach them (by a
    /// duplicate of each socket's fd, open until the client is dropped, so
    /// it never refers to another socket however the connection ends) and
    /// `CLIENT LIST` can show them.
    clients: Mutex<HashMap<u64, Registered>>,
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
            pubsub: Arc::new(PubSub::new(store.clone())),
            memory_peak: AtomicUsize::new(0),
            scripting: Scripting::default(),
            store,
            settings,
            snapshots: None,
            aof: std::sync::OnceLock::new(),
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

    /// The database `session` has selected.
    #[inline]
    pub(crate) fn db(&self, session: &Session) -> &Store {
        if session.db == 0 {
            &self.store
        } else {
            self.store.db(session.db as usize)
        }
    }

    /// Takes note of how much memory is in use now, as Redis does on a timer:
    /// the most seen.
    pub(crate) fn note_memory_peak(&self) -> usize {
        self.memory_peak
            .fetch_max(self.store.used_memory(), Relaxed)
            .max(self.store.used_memory())
    }

    /// Logs writes to `aof` from now on (see `aof::start`).
    pub fn set_aof(&self, aof: Arc<crate::aof::Aof>) {
        let _ = self.aof.set(aof);
    }

    /// The append-only log, if it's on.
    pub fn aof(&self) -> Option<&Arc<crate::aof::Aof>> {
        self.aof.get()
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
        // SAFETY: duplicating an fd has no memory effects; failure is -1.
        let dup = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
        {
            // Checked under the lock `shutdown` holds while closing
            // connections, so a connection either gets closed by it or sees
            // the flag here.
            let mut clients = self.clients.lock();
            if self.shutting_down.load(Relaxed) {
                drop(clients);
                close(dup);
                self.connected.fetch_sub(1, Relaxed);
                return None;
            }
            clients.insert(
                id,
                Registered {
                    dup,
                    fd,
                    connected: self.started.elapsed(),
                    name: None,
                    lib_name: None,
                    lib_ver: None,
                    resp3: false,
                    db: 0,
                    pubsub: false,
                },
            );
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
        for &Registered { dup: fd, .. } in clients.values() {
            // SAFETY: the fd is our own duplicate, open until its client is
            // deregistered under this lock. (-1, if dup failed, is ignored.)
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

    /// Changes what `CLIENT LIST` shows of a connection: its name, library
    /// and protocol, as `CLIENT SETNAME`, `CLIENT SETINFO`, `HELLO` and
    /// `RESET` set them.
    pub(crate) fn update_client(&self, id: u64, f: impl FnOnce(&mut Registered)) {
        if let Some(client) = self.clients.lock().get_mut(&id) {
            f(client);
        }
    }

    /// The connections that are (or aren't) subscribed, in order.
    pub(crate) fn client_ids(&self, pubsub: bool) -> Vec<u64> {
        let mut ids: Vec<u64> = self
            .clients
            .lock()
            .iter()
            .filter(|(_, client)| client.pubsub == pubsub)
            .map(|(&id, _)| id)
            .collect();
        ids.sort_unstable();
        ids
    }

    /// `CLIENT KILL`: closes the connections it picks, but for `me`'s own,
    /// which the caller closes after its reply. How many it picked, and
    /// whether `me` was one.
    pub(crate) fn kill_clients(&self, me: u64, filter: &KillFilter<'_>) -> (usize, bool) {
        let clients = self.clients.lock();
        let (mut killed, mut me_too) = (0, false);
        for (&id, client) in clients.iter() {
            if !filter.picks(id, me, client) {
                continue;
            }
            killed += 1;
            if id == me {
                me_too = true;
            } else {
                // SAFETY: as in `shutdown`.
                unsafe { libc::shutdown(client.dup, libc::SHUT_RDWR) };
            }
        }
        (killed, me_too)
    }

    /// `CLIENT LIST` (`ids` empty: every connection, by id) and `CLIENT
    /// INFO` (`ids` the caller's): a line per connection. `me` is the
    /// caller, whose line shows its session's state; `cmd` its command.
    pub(crate) fn client_lines(&self, me: &Session, ids: &[u64], cmd: &str) -> Vec<u8> {
        let clients = self.clients.lock();
        let mut listed: Vec<u64> = if ids.is_empty() {
            clients.keys().copied().collect()
        } else {
            ids.iter()
                .copied()
                .filter(|id| clients.contains_key(id))
                .collect()
        };
        if ids.is_empty() {
            listed.sort_unstable();
        }
        let now = self.started.elapsed();
        let mut out = Vec::with_capacity(300 * listed.len());
        for id in listed {
            client_line(
                &mut out,
                id,
                &clients[&id],
                now,
                (id == me.id).then_some((me, cmd)),
            );
        }
        out
    }

    /// BullMQ scripts run natively, and calls sent back to Lua
    /// (`src/bullmq.rs`), for `INFO`.
    pub fn scripting_stats(&self) -> (u64, u64) {
        self.scripting.native_stats()
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

fn close(fd: RawFd) {
    if fd >= 0 {
        // SAFETY: `fd` is a duplicate only we hold.
        unsafe { libc::close(fd) };
    }
}

/// A connection, counted as connected until dropped.
pub struct Client<'a> {
    ctx: &'a Context,
    id: u64,
    /// The connection's buffer bytes as last reported to the store.
    reported: usize,
}

impl Client<'_> {
    /// Per-connection state for commands.
    pub fn session(&self) -> Session {
        let authenticated = !self.ctx.has_password();
        Session {
            id: self.id,
            authenticated,
            unrestricted: authenticated,
            pubsub: None,
            multi: None,
            watching: None,
            deny_blocking: false,
            in_script: false,
            name: None,
            resp3: false,
            db: 0,
            commands: 0,
            hits: 0,
            misses: 0,
            socket: -1,
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
        if let Some(client) = self.ctx.clients.lock().remove(&self.id) {
            close(client.dup);
        }
        self.ctx.connected.fetch_sub(1, Relaxed);
    }
}

/// A connection in the registry: what shutdown and `CLIENT LIST` need.
pub(crate) struct Registered {
    /// Our duplicate of its socket: shutdown shuts it down, and `CLIENT
    /// LIST` reads its addresses from it.
    dup: RawFd,
    /// The socket's own descriptor, as `CLIENT LIST` shows it.
    fd: RawFd,
    /// When it connected, since the server started.
    connected: Duration,
    pub(crate) name: Option<Box<[u8]>>,
    pub(crate) lib_name: Option<Box<[u8]>>,
    pub(crate) lib_ver: Option<Box<[u8]>>,
    pub(crate) resp3: bool,
    pub(crate) db: u32,
    /// Subscribed to something, as `CLIENT LIST` and `CLIENT KILL` go by.
    pub(crate) pubsub: bool,
}

/// What `CLIENT KILL` goes by.
pub(crate) struct KillFilter<'a> {
    pub(crate) addr: Option<&'a [u8]>,
    pub(crate) laddr: Option<&'a [u8]>,
    /// 0 for any.
    pub(crate) id: u64,
    /// Subscribed or not; `None` for either.
    pub(crate) pubsub: Option<bool>,
    /// A kind of client there's none of here: replicas, a master.
    pub(crate) none: bool,
    pub(crate) skip_me: bool,
}

impl KillFilter<'_> {
    fn picks(&self, id: u64, me: u64, client: &Registered) -> bool {
        if self.none || (self.skip_me && id == me) || (self.id != 0 && self.id != id) {
            return false;
        }
        if self.pubsub.is_some_and(|pubsub| pubsub != client.pubsub) {
            return false;
        }
        if self.addr.is_some() || self.laddr.is_some() {
            let (peer, local) = client.addresses();
            let differs =
                |wanted: Option<&[u8]>, is: String| wanted.is_some_and(|w| w != is.as_bytes());
            if differs(self.addr, peer) || differs(self.laddr, local) {
                return false;
            }
        }
        true
    }
}

impl Registered {
    /// The socket's addresses, the peer's first, as `CLIENT LIST` shows them.
    fn addresses(&self) -> (String, String) {
        use std::os::fd::BorrowedFd;
        let addr = |a: Option<socket2::SockAddr>| {
            a.and_then(|a| a.as_socket())
                .map_or_else(|| "?:0".to_string(), |a| a.to_string())
        };
        // (No socket: a client made in-process, or one whose fd didn't dup.)
        let (peer, local) = if self.dup >= 0 {
            // SAFETY: the duplicate stays open while the client is registered,
            // which the caller's lock guarantees.
            let fd = unsafe { BorrowedFd::borrow_raw(self.dup) };
            let socket = socket2::SockRef::from(&fd);
            (socket.peer_addr().ok(), socket.local_addr().ok())
        } else {
            (None, None)
        };
        (addr(peer), addr(local))
    }
}

/// One `CLIENT LIST` line, in Redis 7.2's format. What isn't tracked per
/// connection (idle time, buffers, the last command) reads as Redis shows
/// it for a client that's done nothing: except for the caller (`me`), whose
/// own session is at hand.
fn client_line(
    out: &mut Vec<u8>,
    id: u64,
    client: &Registered,
    now: Duration,
    me: Option<(&Session, &str)>,
) {
    let (peer, local) = client.addresses();
    let text = |v: &Option<Box<[u8]>>| {
        v.as_deref()
            .map_or_else(String::new, |v| String::from_utf8_lossy(v).into_owned())
    };
    let session = me.map(|(session, _)| session);
    let subscriptions = session.and_then(|s| s.pubsub.as_deref());
    let count = |to| subscriptions.map_or(0, |s| s.count_of(to));
    let mut flags = String::new();
    if client.pubsub {
        flags.push('P');
    }
    if session.is_some_and(|s| s.multi.is_some()) {
        flags.push('x');
    }
    if flags.is_empty() {
        flags.push('N');
    }
    let resp3 = session.map_or(client.resp3, |s| s.resp3);
    let db = session.map_or(client.db, |s| s.db);
    let multi = session
        .and_then(|s| s.multi.as_ref())
        .map_or(-1, |queue| queue.len() as i64);
    let line = format!(
        "id={id} addr={} laddr={} fd={} name={} age={} idle=0 flags={flags} db={db} sub={} psub={} \
         ssub={} multi={multi} qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 \
         oll=0 omem=0 tot-mem=0 events=r cmd={} user=default redir=-1 resp={} lib-name={} \
         lib-ver={}\n",
        peer,
        local,
        client.fd,
        text(&client.name),
        now.saturating_sub(client.connected).as_secs(),
        count(crate::pubsub::To::Channels),
        count(crate::pubsub::To::Patterns),
        count(crate::pubsub::To::ShardChannels),
        me.map_or("NULL", |(_, cmd)| cmd),
        if resp3 { 3 } else { 2 },
        text(&client.lib_name),
        text(&client.lib_ver),
    );
    out.extend_from_slice(line.as_bytes());
}

/// Per-connection state.
pub struct Session {
    pub(crate) id: u64,
    pub(crate) authenticated: bool,
    /// Authenticated, not subscribed and not in `MULTI`: commands take the
    /// fast path and skip the checks below it. Kept by [`Session::refresh`].
    pub(crate) unrestricted: bool,
    /// What the client is subscribed to, if anything.
    pub(crate) pubsub: Option<Box<Subscriptions>>,
    /// The commands queued since `MULTI`.
    pub(crate) multi: Option<Box<Queue>>,
    /// The keys `WATCH`ed for the next `EXEC`.
    pub(crate) watching: Option<Box<Watching>>,
    /// Set while `EXEC` runs: blocking commands reply at once instead.
    pub(crate) deny_blocking: bool,
    /// A script's session, for what Redis refuses scripts in particular.
    pub(crate) in_script: bool,
    /// Set by `CLIENT SETNAME` or `HELLO ... SETNAME`.
    pub(crate) name: Option<Box<[u8]>>,
    /// Whether the client switched to RESP3 with `HELLO 3`.
    pub(crate) resp3: bool,
    /// The database it has selected (`SELECT`).
    pub(crate) db: u32,
    /// Counts not yet added to the server's (see [`Context::publish`]).
    pub(crate) commands: u64,
    pub(crate) hits: u64,
    pub(crate) misses: u64,
    /// Its connection's plain TCP socket, for a write that serves it while
    /// it's blocked to send the reply on (see `blocking`); -1 for none.
    pub(crate) socket: RawFd,
}

impl Session {
    /// Uses database `index` from now on.
    pub(crate) fn select(&mut self, ctx: &Context, index: u32) {
        if self.db != index {
            self.db = index;
            ctx.update_client(self.id, |client| client.db = index);
        }
    }

    /// The session a script's `redis.call`s run in, as Redis' script client:
    /// authenticated, RESP2 unless the script asks for RESP3, and blocking
    /// commands reply at once.
    pub(crate) fn for_script() -> Session {
        Session {
            id: 0,
            authenticated: true,
            unrestricted: true,
            pubsub: None,
            multi: None,
            watching: None,
            deny_blocking: true,
            in_script: true,
            name: None,
            resp3: false,
            db: 0,
            commands: 0,
            hits: 0,
            misses: 0,
            socket: -1,
        }
    }

    /// The session the append-only log's commands run in again at startup:
    /// as a script's (blocking commands reply at once), but not a script.
    pub(crate) fn for_replay() -> Session {
        Session {
            in_script: false,
            ..Session::for_script()
        }
    }

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

    pub(crate) fn set_authenticated(&mut self, authenticated: bool) {
        self.authenticated = authenticated;
        self.refresh();
    }

    /// Recomputes `unrestricted` after what it depends on changed.
    pub(crate) fn refresh(&mut self) {
        self.unrestricted = self.authenticated && self.pubsub.is_none() && self.multi.is_none();
    }

    pub(crate) fn set_resp3(&mut self, resp3: bool) {
        self.resp3 = resp3;
        if let Some(subscriptions) = &self.pubsub {
            subscriptions.set_resp3(resp3);
        }
    }

    /// Its subscriptions, made on its first subscribe.
    pub(crate) fn subscriptions(&mut self, pubsub: &Arc<PubSub>) -> &mut Subscriptions {
        self.unrestricted = false; // (as `refresh` would make it)
        let (id, resp3) = (self.id, self.resp3);
        self.pubsub
            .get_or_insert_with(|| Box::new(Subscriptions::new(pubsub, id, resp3)))
    }

    /// Leaves pub/sub once it's subscribed to nothing (or always, with
    /// `force`): what was still waiting to be sent is dropped.
    pub(crate) fn end_subscriptions(&mut self, force: bool) {
        if force || self.pubsub.as_ref().is_some_and(|s| s.is_empty()) {
            self.pubsub = None;
            self.refresh();
        }
    }

    /// Whether the client is in `MULTI`, queueing commands.
    pub fn in_transaction(&self) -> bool {
        self.multi.is_some()
    }

    /// Whether the client is subscribed to anything.
    pub fn is_subscribed(&self) -> bool {
        self.pubsub.is_some()
    }

    /// Its outbox while it's subscribed, for its connection to send.
    pub fn subscriber(&self) -> Option<&Arc<Subscriber>> {
        self.pubsub.as_deref().map(Subscriptions::subscriber)
    }
}
