//! Server-wide state shared by every connection (settings, connected
//! clients, counters, shutdown), and each connection's own.

use std::collections::HashMap;
use std::os::fd::RawFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::watch;

use crate::acl::{Acl, User};
use crate::cmd::Args;
use crate::config::ConfigTable;
use crate::latency::Latency;
use crate::pubsub::{PubSub, Subscriber, Subscriptions};
use crate::resp;
use crate::script::Scripting;
use crate::slowlog::SlowLog;
use crate::snapshot::Snapshots;
use crate::store::Store;
use crate::stream::NodeLimits;
use crate::transaction::{Queue, Watching};

/// Settings that apply to connections rather than the store.
#[derive(Debug, Clone)]
pub struct Settings {
    /// `requirepass`: clients must `AUTH` with it before anything else.
    pub password: Option<Vec<u8>>,
    /// `aclfile`: where `ACL SAVE` writes users and `ACL LOAD` reads them,
    /// read at startup too.
    pub aclfile: Option<PathBuf>,
    /// Connections beyond this many are refused with an error.
    pub maxclients: usize,
    /// Connections idle this long are closed. `None` keeps them forever.
    pub timeout: Option<Duration>,
    /// Probe connections idle this long, to find clients that vanished
    /// without closing them. `None` turns probes off.
    pub tcp_keepalive: Option<Duration>,
    /// Whether BullMQ's hottest scripts run natively (`src/bullmq.rs`).
    pub native_scripts: bool,
    /// Who may run `DEBUG`.
    pub debug_command: DebugCommand,
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
            aclfile: None,
            maxclients: 10_000,
            timeout: None,
            tcp_keepalive: Some(Duration::from_secs(300)),
            native_scripts: true,
            debug_command: DebugCommand::No,
            panic_command: None,
        }
    }
}

/// In [`Context::special`]: something's watching every command.
const SPECIAL_ON: u32 = 1;
/// In [`Context::special`]: the ticking clock's asleep, as nothing has run for a while. In the
/// same word, so that the first batch after a quiet spell wakes it, and it costs the others nothing.
const CLOCK_ASLEEP: u32 = 2;
/// In [`Context::special`]: a script has run past `busy-reply-threshold`, so other
/// connections' commands are refused with `-BUSY` but for a few, as in Redis.
const SCRIPT_BUSY: u32 = 4;
/// In [`Context::special`]: a client tracks in `BCAST` mode, so every connection takes
/// the checked path, whose commands send it what they changed when they end.
const BROADCASTING: u32 = 8;
/// In [`Context::special`], counted in the bits above: users changed (`ACL SETUSER` and
/// the like), or a connection was given an outbox, so each connection looks again at
/// its user's rules and its outbox at its next batch.
const USERS_CHANGED: u32 = 16;
/// The flags in [`Context::special`] that put a connection on the watched path.
const WATCHED: u32 = SPECIAL_ON | SCRIPT_BUSY;
/// The flags that aren't a count.
const FLAGS: u32 = USERS_CHANGED - 1;

/// `--enable-debug-command`, as in Redis.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DebugCommand {
    No,
    Yes,
    /// From this machine only.
    Local,
}

/// `CLIENT REPLY`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReplyMode {
    On,
    Off,
    /// The next command's isn't sent.
    Skip,
    /// A command with `Off` or `Skip` is running: whether this is still what it's
    /// set to afterwards says whether that command changed it.
    Passing,
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
/// A thread that runs connections ([`Context::register_io_thread`]).
#[derive(Clone)]
pub(crate) enum IoThread {
    /// A single-threaded tokio runtime's.
    Tokio(tokio::runtime::Handle),
    /// A monoio thread's, woken through its mailbox.
    Monoio(Arc<crate::wake::Mailbox>),
    /// A multi-threaded tokio runtime, with its number of workers.
    Shared(tokio::runtime::Handle, usize),
}

pub struct Context {
    pub store: Arc<Store>,
    pub settings: Settings,
    /// Set when the server saves snapshots (`--snapshot`).
    pub snapshots: Option<Arc<Snapshots>>,
    /// The append-only log (`--aof`), once it's taking writes.
    aof: std::sync::OnceLock<Arc<crate::aof::Aof>>,
    /// What writes are logged to, while there's anything (the log, replicas):
    /// a leaked `Log`, replaced when replication starts, read with one load
    /// by every write. Null while there's nothing.
    log: std::sync::atomic::AtomicPtr<crate::aof::Log>,
    /// Replication: this server's IDs, the stream replicas follow, the master
    /// it follows.
    pub(crate) repl: crate::repl::Replication,
    /// This context, for threads that outlive a command (replicas'): set by
    /// the server's loops (`Context::adopt`).
    me: OnceLock<Weak<Context>>,
    /// The threads that run connections, to wait for until each has finished
    /// what it was running (`Context::wait_for_io_threads`).
    io_threads: Mutex<Vec<IoThread>>,
    /// Held while active expiry runs, for that wait too.
    pub(crate) expiring: Mutex<()>,
    /// A replica is loading its master's data: commands get `-LOADING`.
    loading: AtomicBool,
    /// Who is subscribed to what.
    pub pubsub: Arc<PubSub>,
    /// The most memory seen in use, as `INFO` reports it.
    memory_peak: AtomicUsize,
    /// Forty hex digits, different each time the server starts.
    run_id: String,
    /// `CONFIG`'s settings.
    pub config: ConfigTable,
    pub(crate) slowlog: SlowLog,
    pub(crate) latency: Latency,
    /// The connections `MIGRATE` keeps.
    pub(crate) migrate_sockets: crate::cmd::MigrateSockets,
    /// Clients caching what they read (`CLIENT TRACKING`).
    pub(crate) tracking: crate::tracking::Tracking,
    /// Per-command statistics, once something's asked for them.
    pub(crate) stats: crate::stats::Stats,
    /// Whether keys expire in the background, not only when read: `DEBUG
    /// SET-ACTIVE-EXPIRE` turns it off for tests.
    active_expire: AtomicBool,
    cron_paused: AtomicBool,
    /// `stream-node-max-entries` and `-bytes`, as streams read them.
    stream_node_entries: AtomicUsize,
    /// `hll-sparse-max-bytes`.
    hll_sparse_max_bytes: AtomicUsize,
    stream_node_bytes: AtomicUsize,
    /// A clock a thread ticks, for commands that aren't `fast` to read (`slowlog.rs`).
    ticks: AtomicU64,
    /// Something's watching every command: `MONITOR`, `CLIENT PAUSE`, the latency
    /// monitor, or a `SLOWLOG` threshold below what the ticking clock can tell.
    /// Connections look once a batch (which also wakes the clock: see [`CLOCK_ASLEEP`]),
    /// and while it's on, take the restricted path, whose checks are the ones that
    /// see to it. So none of this costs a command anything when it's off.
    special: AtomicU32,
    /// The thread that ticks the clock.
    clock: OnceLock<std::thread::Thread>,
    monitoring: AtomicBool,
    /// Who's running `MONITOR`.
    monitors: Mutex<Vec<Weak<Subscriber>>>,
    pause: Mutex<Option<Pause>>,
    /// Whether `pause` holds one, so that the checked path's commands needn't lock it.
    paused: AtomicBool,
    /// A `FAILOVER` holds writes back (`pause_for_failover`).
    failover_paused: AtomicBool,
    /// `notify-keyspace-events`, as flags (see `notify.rs`).
    keyspace_events: AtomicU32,
    /// What `CONFIG SET` can change of [`Settings`].
    maxclients: AtomicUsize,
    /// In milliseconds, 0 for none.
    timeout: AtomicU64,
    tcp_keepalive: AtomicU64,
    /// `requirepass` as set, for `CONFIG GET` (the default user has its hash).
    password: Mutex<Option<Vec<u8>>>,
    /// Users and what they may run.
    pub(crate) acl: Acl,
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
            pubsub: Arc::new(PubSub::new()),
            memory_peak: AtomicUsize::new(0),
            run_id: new_run_id(),
            config: ConfigTable::default(),
            slowlog: SlowLog::default(),
            migrate_sockets: Default::default(),
            tracking: Default::default(),
            stats: Default::default(),
            active_expire: AtomicBool::new(true),
            cron_paused: AtomicBool::new(false),
            latency: Latency::default(),
            stream_node_entries: AtomicUsize::new(NodeLimits::default().entries),
            hll_sparse_max_bytes: AtomicUsize::new(3000),
            stream_node_bytes: AtomicUsize::new(NodeLimits::default().bytes),
            ticks: AtomicU64::new(0),
            special: AtomicU32::new(0),
            clock: OnceLock::new(),
            monitoring: AtomicBool::new(false),
            monitors: Mutex::new(Vec::new()),
            pause: Mutex::new(None),
            paused: AtomicBool::new(false),
            failover_paused: AtomicBool::new(false),
            keyspace_events: AtomicU32::new(0),
            maxclients: AtomicUsize::new(settings.maxclients),
            timeout: AtomicU64::new(settings.timeout.map_or(0, |t| t.as_millis() as u64)),
            tcp_keepalive: AtomicU64::new(
                settings.tcp_keepalive.map_or(0, |t| t.as_millis() as u64),
            ),
            password: Mutex::new(settings.password.clone()),
            acl: Acl::new(settings.password.as_deref(), settings.aclfile.clone()),
            scripting: Scripting::default(),
            store,
            settings,
            snapshots: None,
            aof: std::sync::OnceLock::new(),
            log: std::sync::atomic::AtomicPtr::new(std::ptr::null_mut()),
            repl: crate::repl::Replication::default(),
            me: OnceLock::new(),
            io_threads: Mutex::new(Vec::new()),
            expiring: Mutex::new(()),
            loading: AtomicBool::new(false),
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

    /// Whether a connection's commands take the watched path, and its user's
    /// rules, once a batch.
    #[inline]
    pub(crate) fn follow_special(&self, session: &mut Session) {
        let state = self.special.load(Relaxed);
        if state != session.seen {
            self.special_changed(session, state);
        }
    }

    #[cold]
    fn special_changed(&self, session: &mut Session, state: u32) {
        if state & CLOCK_ASLEEP != 0 {
            let was = self.special.fetch_and(!CLOCK_ASLEEP, Relaxed);
            if was & CLOCK_ASLEEP != 0 {
                self.tick();
                if let Some(clock) = self.clock.get() {
                    clock.unpark();
                }
            }
        }
        let state = state & !CLOCK_ASLEEP;
        if state & !FLAGS != session.seen & !FLAGS {
            self.take_up_user(session);
            self.take_up_outbox(session);
        }
        session.seen = state;
        let special = state & WATCHED != 0;
        if special != session.special {
            session.special = special;
        }
        session.broadcasting = state & BROADCASTING != 0;
        session.refresh();
    }

    /// Some client tracks in `BCAST` mode, or none does any more.
    pub(crate) fn set_broadcasting(&self, on: bool) {
        if on {
            self.special.fetch_or(BROADCASTING, Relaxed);
        } else {
            self.special.fetch_and(!BROADCASTING, Relaxed);
        }
    }

    /// The outbox another connection's tracking redirects to, if this one has
    /// none yet, so that it hears what goes there.
    fn take_up_outbox(&self, session: &mut Session) {
        if session.handed.is_none()
            && let Some(kept) = self
                .clients
                .lock()
                .get(&session.id)
                .and_then(|client| client.kept.clone())
        {
            kept.set_resp3(session.resp3);
            session.handed = Some(kept);
            session.refresh();
        }
    }

    /// Whether connection `id` is open.
    pub(crate) fn client_exists(&self, id: u64) -> bool {
        self.clients.lock().contains_key(&id)
    }

    /// For tracking that redirects to connection `id`: its outbox, made if it
    /// has none, and kept while it's connected. (Dead if it has gone.) A
    /// connection given one takes it up at its next batch.
    pub(crate) fn keep_outbox(&self, id: u64) -> Weak<Subscriber> {
        let mut clients = self.clients.lock();
        let Some(client) = clients.get_mut(&id) else {
            return Weak::new();
        };
        if let Some(kept) = &client.kept {
            return Arc::downgrade(kept);
        }
        let outbox = client.outbox.upgrade().unwrap_or_else(|| {
            let outbox = Subscriber::alone(id, self.store.clone());
            outbox.set_resp3(client.resp3);
            client.outbox = Arc::downgrade(&outbox);
            outbox
        });
        let weak = Arc::downgrade(&outbox);
        client.kept = Some(outbox);
        drop(clients);
        // So that it takes it up.
        self.special.fetch_add(USERS_CHANGED, Relaxed);
        weak
    }

    /// A new outbox for connection `id`: the one kept for it, if there is one.
    fn new_outbox(&self, id: u64, resp3: bool) -> Arc<Subscriber> {
        let mut clients = self.clients.lock();
        let client = clients.get_mut(&id);
        let outbox = match client.as_ref().and_then(|c| c.kept.clone()) {
            Some(kept) => kept,
            None => Subscriber::alone(id, self.store.clone()),
        };
        outbox.set_resp3(resp3);
        if let Some(client) = client {
            client.outbox = Arc::downgrade(&outbox);
        }
        outbox
    }

    /// A script has run past `busy-reply-threshold` (or stopped, with `false`).
    pub(crate) fn set_script_busy(&self, busy: bool) {
        if busy {
            self.special.fetch_or(SCRIPT_BUSY, Relaxed);
        } else {
            self.special.fetch_and(!SCRIPT_BUSY, Relaxed);
        }
    }

    /// Whether other connections' commands get `-BUSY`.
    pub(crate) fn script_busy(&self) -> bool {
        self.special.load(Relaxed) & SCRIPT_BUSY != 0
    }

    /// Users changed: every connection takes up its user's new rules.
    pub(crate) fn acl_changed(&self) {
        self.special.fetch_add(USERS_CHANGED, Relaxed);
    }

    /// The connection's user as it is now: the same one changed, or, if it
    /// was deleted, one that closes the connection.
    fn take_up_user(&self, session: &mut Session) {
        let name: &[u8] = session.user.as_ref().map_or(b"default", |u| u.name());
        let current = self.acl.get(name);
        let user = match (current, &session.user) {
            (Some(user), Some(had)) if user.serial() != had.serial() => had.tombstone(),
            (Some(user), _) => user,
            (None, Some(had)) => had.tombstone(),
            (None, None) => self.acl.default_user(),
        };
        session.user = Session::held(user);
    }

    /// After what `special` goes by changed.
    pub(crate) fn update_special(&self) {
        let on = !self.monitors.lock().is_empty()
            || self.repl.following()
            || self.loading()
            || self.repl.settings.min_replicas_to_write.load(Relaxed) > 0
            || self.pause.lock().is_some()
            || self.failover_paused.load(Relaxed)
            || self.slowlog.needs_precise()
            || self.latency.threshold() != 0
            || self.stats.is_on()
            || self.notifying();
        if on {
            self.special.fetch_or(SPECIAL_ON, Relaxed);
        } else {
            self.special.fetch_and(!SPECIAL_ON, Relaxed);
        }
    }

    /// What starts a new node in a stream.
    #[inline]
    pub(crate) fn stream_limits(&self) -> NodeLimits {
        NodeLimits {
            entries: self.stream_node_entries.load(Relaxed),
            bytes: self.stream_node_bytes.load(Relaxed),
        }
    }

    pub(crate) fn hll_sparse_max_bytes(&self) -> usize {
        self.hll_sparse_max_bytes.load(Relaxed)
    }

    pub(crate) fn set_hll_sparse_max_bytes(&self, bytes: usize) {
        self.hll_sparse_max_bytes.store(bytes, Relaxed);
    }

    pub(crate) fn set_stream_node_entries(&self, entries: usize) {
        self.stream_node_entries.store(entries, Relaxed);
    }

    pub(crate) fn set_stream_node_bytes(&self, bytes: usize) {
        self.stream_node_bytes
            .store(NodeLimits::bytes_from_setting(bytes), Relaxed);
    }

    /// The clock a thread ticks, in ticks since the start.
    #[inline]
    pub(crate) fn ticks(&self) -> u64 {
        self.ticks.load(Relaxed)
    }

    /// Brings the clock up to date. Only forwards: see [`Context::clock_sleeps`].
    pub(crate) fn tick(&self) {
        let ticks = self.started.elapsed().as_micros() as u64 / crate::slowlog::TICK_MICROS as u64;
        self.ticks.fetch_max(ticks, Relaxed);
    }

    /// For the thread that ticks the clock: it's this one.
    pub(crate) fn clock_starts(&self) {
        let _ = self.clock.set(std::thread::current());
    }

    /// For the thread that ticks the clock: no command has run for a while, so it stops
    /// until a connection's next batch. It moves once more, so a command running now
    /// finds it moved when it ends, and asks what time it really is.
    pub(crate) fn clock_sleeps(&self) {
        self.special.fetch_or(CLOCK_ASLEEP, Relaxed);
        self.ticks.fetch_add(1, Relaxed);
    }

    pub(crate) fn clock_asleep(&self) -> bool {
        self.special.load(Relaxed) & CLOCK_ASLEEP != 0
    }

    /// Commands run so far, as far as connections have said.
    pub(crate) fn commands_so_far(&self) -> u64 {
        self.counters.iter().map(|c| c.commands.load(Relaxed)).sum()
    }

    /// Whether anyone's running `MONITOR`.
    pub(crate) fn monitoring(&self) -> bool {
        self.monitoring.load(Relaxed)
    }

    /// Makes `session` a monitor.
    pub(crate) fn add_monitor(&self, session: &mut Session) {
        let outbox = session.outbox(self);
        self.monitors.lock().push(Arc::downgrade(&outbox));
        session.monitor = Some(outbox);
        session.refresh();
        self.monitoring.store(true, Relaxed);
        self.update_special();
    }

    /// Tells the monitors of a command, as `+time [db who] "arg" "arg"`.
    pub(crate) fn feed_monitors(&self, db: u32, who: &str, args: &Args<'_>) {
        let time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let mut line = format!(
            "+{}.{:06} [{db} {who}]",
            time.as_secs(),
            time.subsec_micros()
        )
        .into_bytes();
        for (i, secret) in crate::slowlog::secret_args(args).into_iter().enumerate() {
            line.push(b' ');
            if secret {
                line.extend_from_slice(b"\"(redacted)\"");
            } else {
                write_repr(&mut line, args.get(i));
            }
        }
        line.extend_from_slice(b"\r\n");
        let empty = {
            let mut monitors = self.monitors.lock();
            monitors.retain(|monitor| match monitor.upgrade() {
                Some(monitor) => {
                    monitor.deliver_line(&line);
                    true
                }
                None => false,
            });
            monitors.is_empty()
        };
        if empty {
            self.monitoring.store(false, Relaxed);
            self.update_special();
        }
    }

    /// A connection's address, as logs and `MONITOR` show it.
    pub(crate) fn peer_of(&self, session: &mut Session) -> String {
        if let Some(peer) = &session.peer {
            return peer.to_string();
        }
        let peer = self
            .clients
            .lock()
            .get(&session.id)
            .map_or_else(|| "?:0".to_string(), |client| client.addresses().0);
        session.peer = Some(peer.clone().into_boxed_str());
        peer
    }

    /// `CLIENT PAUSE`: for this long, the commands it says are held back.
    pub(crate) fn pause_clients(&self, millis: u64, all: bool) {
        *self.pause.lock() = Some(Pause {
            until: Instant::now() + Duration::from_millis(millis),
            all,
        });
        self.paused
            .store(true, std::sync::atomic::Ordering::Release);
        self.update_special();
    }

    pub(crate) fn unpause_clients(&self) {
        *self.pause.lock() = None;
        self.paused
            .store(self.failover_paused.load(Relaxed), Relaxed);
        self.update_special();
    }

    /// A `FAILOVER` holds writes back (as `CLIENT PAUSE WRITE`, but until it's
    /// over), or lets them go.
    pub(crate) fn pause_for_failover(&self, on: bool) {
        let pause = self.pause.lock();
        self.failover_paused.store(on, Relaxed);
        self.paused
            .store(on || pause.is_some(), std::sync::atomic::Ordering::Release);
        drop(pause);
        self.update_special();
    }

    /// Whether keys expire in the background (see `active_expire`).
    pub(crate) fn active_expire(&self) -> bool {
        self.active_expire.load(Relaxed)
    }

    pub(crate) fn set_active_expire(&self, on: bool) {
        self.active_expire.store(on, Relaxed);
    }

    /// `DEBUG PAUSE-CRON`: whether what runs by itself (the background expiry)
    /// is paused.
    pub(crate) fn cron_paused(&self) -> bool {
        self.cron_paused.load(Relaxed)
    }

    pub(crate) fn set_cron_paused(&self, paused: bool) {
        self.cron_paused.store(paused, Relaxed);
    }

    /// `notify-keyspace-events`, as flags.
    pub(crate) fn keyspace_events(&self) -> u32 {
        self.keyspace_events.load(Relaxed)
    }

    /// Whether keyspace notifications send anything (and so every command is
    /// watched, and every change logged).
    pub(crate) fn notifying(&self) -> bool {
        crate::notify::sends(self.keyspace_events())
    }

    /// `--notify-keyspace-events`: as `CONFIG SET` would.
    pub fn set_notify_keyspace_events(&self, classes: &str) -> Result<(), &'static str> {
        let flags = crate::notify::parse(classes.as_bytes()).ok_or(crate::notify::INVALID)?;
        self.config
            .set("notify-keyspace-events", &crate::notify::format(flags));
        self.set_keyspace_events(flags);
        Ok(())
    }

    /// `notify-keyspace-events` changed.
    pub(crate) fn set_keyspace_events(&self, flags: u32) {
        let was = self.notifying();
        self.keyspace_events.store(flags, Relaxed);
        let on = self.notifying();
        if on != was {
            self.store.set_notifying(on);
            self.update_special();
        }
    }

    /// Whether clients are paused: `Some(true)` for every command, `Some(false)` for
    /// those that write. It's over by itself when the time's up.
    pub(crate) fn pause_mode(&self) -> Option<bool> {
        if !self.paused.load(std::sync::atomic::Ordering::Acquire) {
            return None;
        }
        let mut pause = self.pause.lock();
        let failover = self.failover_paused.load(Relaxed);
        if let Some(current) = *pause {
            if Instant::now() < current.until {
                return Some(current.all);
            }
            *pause = None;
        }
        if failover {
            return Some(false);
        }
        self.paused.store(false, Relaxed);
        drop(pause);
        self.update_special();
        None
    }

    /// `CONFIG RESETSTAT`.
    pub(crate) fn reset_stats(&self) {
        for counters in self.counters.iter() {
            counters.commands.store(0, Relaxed);
            counters.hits.store(0, Relaxed);
            counters.misses.store(0, Relaxed);
        }
        self.total_connections.store(0, Relaxed);
        self.rejected_connections.store(0, Relaxed);
        self.memory_peak.store(0, Relaxed);
        self.store.reset_stats();
        self.stats.reset();
        // (Not the ACL counts, which Redis keeps too.)
    }

    pub(crate) fn maxclients(&self) -> usize {
        self.maxclients.load(Relaxed)
    }

    pub(crate) fn set_maxclients(&self, max: usize) {
        self.maxclients.store(max, Relaxed);
    }

    /// How long an idle connection is kept.
    #[inline]
    pub(crate) fn timeout(&self) -> Option<Duration> {
        match self.timeout.load(Relaxed) {
            0 => None,
            millis => Some(Duration::from_millis(millis)),
        }
    }

    pub(crate) fn set_timeout(&self, timeout: Option<Duration>) {
        self.timeout
            .store(timeout.map_or(0, |t| t.as_millis() as u64), Relaxed);
    }

    pub(crate) fn tcp_keepalive(&self) -> Option<Duration> {
        match self.tcp_keepalive.load(Relaxed) {
            0 => None,
            millis => Some(Duration::from_millis(millis)),
        }
    }

    pub(crate) fn set_tcp_keepalive(&self, time: Option<Duration>) {
        self.tcp_keepalive
            .store(time.map_or(0, |t| t.as_millis() as u64), Relaxed);
    }

    pub(crate) fn password(&self) -> Option<Vec<u8>> {
        self.password.lock().clone()
    }

    /// The default user's password (`requirepass`). Clients already
    /// authenticated stay so, as in Redis.
    pub(crate) fn set_password(&self, password: Option<Vec<u8>>) {
        self.acl.set_default_password(password.as_deref());
        *self.password.lock() = password;
        self.acl_changed();
    }

    /// Whether this connection may run `DEBUG`.
    pub(crate) fn debug_allowed(&self, session: &mut Session) -> bool {
        match self.settings.debug_command {
            DebugCommand::Yes => true,
            DebugCommand::No => false,
            DebugCommand::Local => {
                let peer = self.peer_of(session);
                peer.starts_with("127.") || peer.starts_with("[::1]")
            }
        }
    }

    /// What identifies this run of the server, in `INFO`.
    pub(crate) fn run_id(&self) -> &str {
        &self.run_id
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
        let repl = self.log().and_then(|log| log.repl.clone());
        self.publish_log(crate::aof::Log::new(self.aof.get().cloned(), repl));
    }

    /// What writes are logged to, if anything: every write takes a turn then.
    #[inline]
    pub(crate) fn log(&self) -> Option<&crate::aof::Log> {
        let log = self.log.load(std::sync::atomic::Ordering::Acquire);
        // SAFETY: a published log is leaked: it lives as long as the process.
        unsafe { log.as_ref() }
    }

    /// From now on, writes go to replicas through `master` too.
    pub(crate) fn log_to_replicas(&self, master: Arc<crate::repl::Master>) {
        self.publish_log(crate::aof::Log::new(self.aof.get().cloned(), Some(master)));
    }

    /// Writes no longer go to replicas (the stream was freed): only to the
    /// log, if it's on, or nowhere, so they stop taking turns.
    pub(crate) fn stop_logging_to_replicas(&self) {
        match self.aof.get() {
            Some(aof) => self.publish_log(crate::aof::Log::new(Some(aof.clone()), None)),
            None => self
                .log
                .store(std::ptr::null_mut(), std::sync::atomic::Ordering::Release),
        }
    }

    fn publish_log(&self, log: crate::aof::Log) {
        // Leaked: commands hold it by reference without counting.
        let log = Box::into_raw(Box::new(log));
        self.log.store(log, std::sync::atomic::Ordering::Release);
    }

    /// Lets threads that outlive a command have this context: called by the
    /// server's loops, which hold it in an `Arc`.
    pub fn adopt(self: &Arc<Self>) {
        let _ = self.me.set(Arc::downgrade(self));
        if let Some(snapshots) = &self.snapshots {
            snapshots.attach(Arc::downgrade(self));
        }
    }

    /// This context, if a server's loop runs it (see [`Context::adopt`]).
    pub(crate) fn arc(&self) -> Option<Arc<Context>> {
        self.me.get().and_then(Weak::upgrade)
    }

    /// A thread that runs connections, so that [`Context::wait_for_io_threads`]
    /// waits for it.
    pub(crate) fn register_io_thread(&self, thread: IoThread) {
        self.io_threads.lock().push(thread);
    }

    /// Returns once every thread that runs connections, and active expiry,
    /// has finished what it was running when this was called: once a write
    /// that started then has ended. For a change every write must see (writes
    /// taking turns), made just before. Not from such a thread.
    pub(crate) fn wait_for_io_threads(&self) {
        let threads: Vec<IoThread> = self.io_threads.lock().clone();
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let mut expected = 0;
        for thread in &threads {
            match thread {
                IoThread::Tokio(handle) => {
                    let tx = tx.clone();
                    handle.spawn(async move {
                        let _ = tx.send(());
                    });
                    expected += 1;
                }
                IoThread::Monoio(mailbox) => {
                    mailbox.call_back(tx.clone());
                    expected += 1;
                }
                IoThread::Shared(handle, workers) => {
                    // Each worker holds one of these until all have one: then
                    // none is running anything else.
                    let gate = Arc::new((
                        std::sync::Mutex::new((0usize, false)),
                        std::sync::Condvar::new(),
                    ));
                    for _ in 0..*workers {
                        let (gate, tx) = (gate.clone(), tx.clone());
                        handle.spawn(async move {
                            let (lock, cvar) = &*gate;
                            let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
                            state.0 += 1;
                            cvar.notify_all();
                            let _ =
                                cvar.wait_timeout_while(state, Duration::from_secs(2), |s| !s.1);
                            let _ = tx.send(());
                        });
                    }
                    let (lock, cvar) = &*gate;
                    let state = lock.lock().unwrap_or_else(|e| e.into_inner());
                    let mut state = cvar
                        .wait_timeout_while(state, Duration::from_secs(2), |s| s.0 < *workers)
                        .map_or_else(|e| e.into_inner().0, |r| r.0);
                    state.1 = true;
                    cvar.notify_all();
                    expected += *workers;
                }
            }
        }
        drop(tx);
        for _ in 0..expected {
            if rx.recv_timeout(Duration::from_secs(5)).is_err() {
                break;
            }
        }
        drop(self.expiring.lock());
    }

    /// A replica is loading its master's data (`true`), or done.
    pub(crate) fn set_loading(&self, loading: bool) {
        self.loading.store(loading, Relaxed);
        self.update_special();
    }

    /// Whether commands get `-LOADING`.
    #[inline]
    pub(crate) fn loading(&self) -> bool {
        self.loading.load(Relaxed)
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
        if self.connected.fetch_add(1, Relaxed) >= self.maxclients() {
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
                    monitor: false,
                    last_command: None,
                    user: None,
                    outbox: std::sync::Weak::new(),
                    kept: None,
                    tracker: None,
                    replica: false,
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
            .filter(|(_, client)| client.pubsub == pubsub && !client.replica)
            .map(|(&id, _)| id)
            .collect();
        ids.sort_unstable();
        ids
    }

    /// The replicas' client ids, for `CLIENT LIST TYPE replica`.
    pub(crate) fn replica_ids(&self) -> Vec<u64> {
        let mut ids: Vec<u64> = self
            .clients
            .lock()
            .iter()
            .filter(|(_, client)| client.replica)
            .map(|(&id, _)| id)
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Connection `id` is a replica's from now on (`SYNC`, `PSYNC`): its
    /// thread has the socket, and the connection stays listed until
    /// [`Context::forget_replica`].
    pub(crate) fn hand_to_replica(&self, id: u64, command: &'static str) {
        if let Some(client) = self.clients.lock().get_mut(&id) {
            client.replica = true;
            client.last_command = Some(command);
        }
    }

    /// A replica's thread has ended.
    pub(crate) fn forget_replica(&self, id: u64) {
        let gone = self.clients.lock().remove(&id);
        if let Some(client) = gone {
            close(client.dup);
        }
    }

    /// `CLIENT KILL`: closes the connections it picks, but for `me`'s own,
    /// which the caller closes after its reply. How many it picked, and
    /// whether `me` was one. They're gone from the list at once, as in Redis,
    /// though their threads may not have noticed yet.
    pub(crate) fn kill_clients(&self, me: u64, filter: &KillFilter<'_>) -> (usize, bool) {
        let mut clients = self.clients.lock();
        let picked: Vec<u64> = clients
            .iter()
            .filter(|&(&id, client)| filter.picks(id, me, client))
            .map(|(&id, _)| id)
            .collect();
        let me_too = picked.contains(&me);
        let mut trackers = Vec::new();
        for id in &picked {
            if *id != me
                && let Some(client) = clients.remove(id)
            {
                // SAFETY: as in `shutdown`.
                unsafe { libc::shutdown(client.dup, libc::SHUT_RDWR) };
                close(client.dup);
                trackers.extend(client.tracker);
            }
        }
        drop(clients);
        // They stop tracking at once, as in Redis.
        for tracker in &trackers {
            self.tracking.gone(self, tracker);
        }
        (picked.len(), me_too)
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
            let mine = id == me.id;
            client_line(
                &mut out,
                id,
                &clients[&id],
                now,
                mine.then_some(me),
                mine.then_some(cmd),
                None,
            );
        }
        out
    }

    /// A connection's `CLIENT INFO` line, without its newline, as `ACL LOG`
    /// keeps it: `cmd` is the command it's running.
    pub(crate) fn client_info(&self, session: &Session, cmd: &str) -> Vec<u8> {
        let mut line = self.client_lines(session, &[session.id], cmd);
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        line
    }

    /// The same for a connection running `EXEC` of `queued` commands, which
    /// shows as still in its transaction, as in Redis.
    pub(crate) fn client_info_in_exec(&self, session: &Session, queued: usize) -> Vec<u8> {
        let clients = self.clients.lock();
        let mut out = Vec::new();
        if let Some(client) = clients.get(&session.id) {
            let now = self.started.elapsed();
            client_line(
                &mut out,
                session.id,
                client,
                now,
                Some(session),
                Some("exec"),
                Some(queued),
            );
        }
        if out.last() == Some(&b'\n') {
            out.pop();
        }
        out
    }

    /// The same for a connection by id, from what the registry knows (for a
    /// script's caller, which is running `cmd`).
    pub(crate) fn client_info_of(&self, id: u64, cmd: &str) -> Vec<u8> {
        let clients = self.clients.lock();
        let mut out = Vec::new();
        if let Some(client) = clients.get(&id) {
            client_line(
                &mut out,
                id,
                client,
                self.started.elapsed(),
                None,
                Some(cmd),
                None,
            );
        }
        if out.last() == Some(&b'\n') {
            out.pop();
        }
        out
    }

    /// Closes the connections authenticated as the user `name` (as `ACL
    /// DELUSER` does), but for `me`'s, which the caller closes.
    pub(crate) fn kill_user_clients(&self, me: u64, name: &[u8]) {
        let filter = KillFilter {
            addr: None,
            laddr: None,
            id: 0,
            pubsub: None,
            replica: None,
            none: false,
            skip_me: true,
            user: Some(name),
        };
        self.kill_clients(me, &filter);
    }

    /// Closes the subscribed connections of `user` (just changed) whose
    /// subscriptions it no longer allows, as Redis does, but for `me`'s.
    pub(crate) fn kill_unsubscribable(&self, me: u64, user: &User) {
        let ids: Vec<u64> = {
            let clients = self.clients.lock();
            clients
                .iter()
                .filter(|&(&id, client)| {
                    id != me
                        && client.pubsub
                        && client.user.as_deref().unwrap_or(b"default") == user.name()
                })
                .map(|(&id, _)| id)
                .collect()
        };
        for id in ids {
            let (channels, patterns, shard) = self.pubsub.subscriptions_of(id);
            let allowed = channels
                .iter()
                .chain(&shard)
                .all(|c| crate::acl::may_keep(user, c, false))
                && patterns.iter().all(|p| crate::acl::may_keep(user, p, true));
            if !allowed {
                let filter = KillFilter {
                    addr: None,
                    laddr: None,
                    id,
                    pubsub: None,
                    replica: None,
                    none: false,
                    skip_me: true,
                    user: None,
                };
                self.kill_clients(me, &filter);
            }
        }
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

    /// Loads users from the ACL file, if one was given: at startup, where an
    /// error should stop the server, as in Redis.
    pub fn load_acl_file(&self) -> Result<(), String> {
        self.acl.load_at_startup()
    }

    /// Makes the function libraries from the code on the store (loaded from a
    /// snapshot or the log), at startup.
    pub fn load_functions(&self) {
        let mut functions = self.scripting.functions.lock();
        functions.follow(&self.store);
        self.scripting.count_functions(&functions);
    }

    /// Whether connections start authenticated: the default user is on and
    /// needs no password.
    pub fn open(&self) -> bool {
        self.acl.open()
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
        let authenticated = self.ctx.open();
        let user = Session::held(self.ctx.acl.default_user());
        let mut session = Session {
            id: self.id,
            authenticated,
            unrestricted: false,
            user,
            pubsub: None,
            multi: None,
            watching: None,
            deny_blocking: false,
            in_script: false,
            name: None,
            resp3: false,
            db: 0,
            special: false,
            reply: ReplyMode::On,
            no_evict: false,
            no_touch: false,
            monitor: None,
            peer: None,
            commands: 0,
            hits: 0,
            misses: 0,
            socket: -1,
            seen: 0,
            tracking: None,
            reads_for: None,
            handed: None,
            broadcasting: false,
            current_outbox: None,
        };
        session.refresh();
        session
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
        let gone = {
            let mut clients = self.ctx.clients.lock();
            // A replica's thread has it now (`Context::forget_replica`).
            if clients.get(&self.id).is_some_and(|c| c.replica) {
                None
            } else {
                clients.remove(&self.id)
            }
        };
        if let Some(client) = gone {
            close(client.dup);
            if let Some(tracker) = &client.tracker {
                self.ctx.tracking.gone(self.ctx, tracker);
            }
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
    pub(crate) monitor: bool,
    /// What it last ran, where that's known: what subscribed it.
    pub(crate) last_command: Option<&'static str>,
    /// The user it's authenticated as, if not `default`.
    pub(crate) user: Option<Box<[u8]>>,
    /// Its outbox, if it has one (see [`Session::outbox`]).
    outbox: Weak<Subscriber>,
    /// The same, kept as long as it's connected, once another connection's
    /// tracking redirects to it: what it hears goes there.
    kept: Option<Arc<Subscriber>>,
    /// Its tracking, once it has turned it on, for `CLIENT LIST`.
    pub(crate) tracker: Option<Arc<crate::tracking::Tracker>>,
    /// A replica, whose socket a thread of its own has now (`PSYNC`): it stays
    /// listed after its connection's task ends, until that thread's does.
    pub(crate) replica: bool,
}

fn new_run_id() -> String {
    use std::hash::BuildHasher;
    let random = std::collections::hash_map::RandomState::new();
    let mut id = String::with_capacity(48);
    for i in 0..3u8 {
        id.push_str(&format!("{:016x}", random.hash_one(i)));
    }
    id.truncate(40);
    id
}

/// A `CLIENT PAUSE` going on.
#[derive(Clone, Copy)]
struct Pause {
    until: Instant,
    all: bool,
}

/// An argument as `MONITOR` shows it: quoted, and what's not printable escaped.
fn write_repr(out: &mut Vec<u8>, arg: &[u8]) {
    out.push(b'"');
    for &byte in arg {
        match byte {
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'"' => out.extend_from_slice(b"\\\""),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            7 => out.extend_from_slice(b"\\a"),
            8 => out.extend_from_slice(b"\\b"),
            0x20..=0x7e => out.push(byte),
            _ => out.extend_from_slice(format!("\\x{byte:02x}").as_bytes()),
        }
    }
    out.push(b'"');
}

/// What `CLIENT KILL` goes by.
pub(crate) struct KillFilter<'a> {
    pub(crate) addr: Option<&'a [u8]>,
    pub(crate) laddr: Option<&'a [u8]>,
    /// 0 for any.
    pub(crate) id: u64,
    /// Subscribed or not; `None` for either.
    pub(crate) pubsub: Option<bool>,
    /// A replica or not; `None` for either.
    pub(crate) replica: Option<bool>,
    /// A kind of client there's none of here: replicas, a master.
    pub(crate) none: bool,
    pub(crate) skip_me: bool,
    /// Authenticated as this user.
    pub(crate) user: Option<&'a [u8]>,
}

impl KillFilter<'_> {
    fn picks(&self, id: u64, me: u64, client: &Registered) -> bool {
        if self.none || (self.skip_me && id == me) || (self.id != 0 && self.id != id) {
            return false;
        }
        if self.pubsub.is_some_and(|pubsub| pubsub != client.pubsub)
            || self
                .replica
                .is_some_and(|replica| replica != client.replica)
        {
            return false;
        }
        if self
            .user
            .is_some_and(|user| user != client.user.as_deref().unwrap_or(b"default"))
        {
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
    session: Option<&Session>,
    cmd: Option<&str>,
    exec: Option<usize>,
) {
    let (peer, local) = client.addresses();
    let text = |v: &Option<Box<[u8]>>| {
        v.as_deref()
            .map_or_else(String::new, |v| String::from_utf8_lossy(v).into_owned())
    };
    let subscriptions = session.and_then(|s| s.pubsub.as_deref());
    let count = |to| subscriptions.map_or(0, |s| s.count_of(to));
    let mut flags = String::new();
    if client.replica {
        flags.push('S');
    }
    if client.pubsub {
        flags.push('P');
    }
    if client.monitor {
        flags.push('O');
    }
    if session.is_some_and(|s| s.multi.is_some()) || exec.is_some() {
        flags.push('x');
    }
    let tracker = session
        .and_then(|s| s.tracking.as_ref().map(|t| &t.tracker))
        .or(client.tracker.as_ref());
    let redir = tracker.map_or(-1, |t| t.list_flags(&mut flags));
    if session.is_some_and(|s| s.no_evict) {
        flags.push('e');
    }
    if session.is_some_and(|s| s.no_touch) {
        flags.push('T');
    }
    if flags.is_empty() {
        flags.push('N');
    }
    let resp3 = session.map_or(client.resp3, |s| s.resp3);
    let db = session.map_or(client.db, |s| s.db);
    let multi = session
        .and_then(|s| s.multi.as_ref())
        .map(|queue| queue.len())
        .or(exec)
        .map_or(-1, |queued| queued as i64);
    let line = format!(
        "id={id} addr={} laddr={} fd={} name={} age={} idle=0 flags={flags} db={db} sub={} psub={} \
         ssub={} multi={multi} qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 \
         oll=0 omem=0 tot-mem=0 events=r cmd={} user={} redir={redir} resp={} lib-name={} \
         lib-ver={}\n",
        peer,
        local,
        client.fd,
        text(&client.name),
        now.saturating_sub(client.connected).as_secs(),
        count(crate::pubsub::To::Channels),
        count(crate::pubsub::To::Patterns),
        count(crate::pubsub::To::ShardChannels),
        cmd.unwrap_or(client.last_command.unwrap_or("NULL")),
        client
            .user
            .as_deref()
            .map_or("default".into(), String::from_utf8_lossy),
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
    /// Authenticated, not subscribed and not in `MULTI`, and its user may run
    /// everything: commands take the fast path and skip the checks below it.
    /// Kept by [`Session::refresh`].
    pub(crate) unrestricted: bool,
    /// The user it runs as: `None` for the default user while that may run
    /// everything (most connections, which then hold nothing shared).
    pub(crate) user: Option<Arc<User>>,
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
    /// As it last saw [`Context::follow_special`].
    pub(crate) special: bool,
    pub(crate) reply: ReplyMode,
    pub(crate) no_evict: bool,
    pub(crate) no_touch: bool,
    /// Its outbox, if it's run `MONITOR`.
    pub(crate) monitor: Option<Arc<Subscriber>>,
    /// Its address, once something's wanted it.
    pub(crate) peer: Option<Box<str>>,
    /// Counts not yet added to the server's (see [`Context::publish`]).
    pub(crate) commands: u64,
    pub(crate) hits: u64,
    pub(crate) misses: u64,
    /// Its connection's plain TCP socket, for a write that serves it while
    /// it's blocked to send the reply on (see `blocking`); -1 for none.
    pub(crate) socket: RawFd,
    /// [`Context::special`] as it last saw it, but for the sleeping clock.
    pub(crate) seen: u32,
    /// Its `CLIENT TRACKING`, once turned on.
    pub(crate) tracking: Option<Box<crate::tracking::SessionTracking>>,
    /// A script's: the tracking client its reads are remembered for.
    pub(crate) reads_for: Option<Arc<crate::tracking::Tracker>>,
    /// Its outbox, kept because another connection's tracking redirects to it.
    pub(crate) handed: Option<Arc<Subscriber>>,
    /// Some client tracks in `BCAST` mode (see [`BROADCASTING`]).
    pub(crate) broadcasting: bool,
    /// Its outbox while something uses it, of those above: what connections
    /// look at once a batch (see [`Session::subscriber`]).
    pub(crate) current_outbox: Option<Arc<Subscriber>>,
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
            user: None,
            pubsub: None,
            multi: None,
            watching: None,
            deny_blocking: true,
            in_script: true,
            name: None,
            resp3: false,
            db: 0,
            special: false,
            reply: ReplyMode::On,
            no_evict: false,
            no_touch: false,
            monitor: None,
            peer: None,
            commands: 0,
            hits: 0,
            misses: 0,
            socket: -1,
            seen: 0,
            tracking: None,
            reads_for: None,
            handed: None,
            broadcasting: false,
            current_outbox: None,
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

    /// How a session holds `user`: the default user that may run everything
    /// as `None`.
    pub(crate) fn held(user: Arc<User>) -> Option<Arc<User>> {
        (!(user.may_run_everything() && user.name() == b"default" && user.serial() == 0))
            .then_some(user)
    }

    /// Authenticated as `user` from now on.
    pub(crate) fn set_user(&mut self, ctx: &Context, user: Arc<User>) {
        let name = (user.name() != b"default").then(|| user.name().into());
        ctx.update_client(self.id, |client| client.user = name);
        self.user = Session::held(user);
        self.authenticated = true;
        self.refresh();
    }

    /// The name of its user.
    pub(crate) fn user_name(&self) -> &[u8] {
        self.user.as_ref().map_or(b"default", |u| u.name())
    }

    /// Recomputes `unrestricted` and the outbox after what they depend on changed.
    pub(crate) fn refresh(&mut self) {
        self.update_outbox();
        self.unrestricted = self.authenticated
            && self.user.as_ref().is_none_or(|u| u.may_run_everything())
            && self.pubsub.is_none()
            && self.multi.is_none()
            && self.monitor.is_none()
            && !self.special
            && !self.broadcasting
            && !self.tracking.as_ref().is_some_and(|t| t.on)
            && matches!(self.reply, ReplyMode::On | ReplyMode::Passing);
    }

    pub(crate) fn set_resp3(&mut self, resp3: bool) {
        self.resp3 = resp3;
        if let Some(outbox) = self.subscriber() {
            outbox.set_resp3(resp3);
        }
    }

    /// Its outbox: the one it has, or a new one. A connection has one at a
    /// time, for what it subscribes to, `MONITOR` and `CLIENT TRACKING`, so
    /// that what each sends goes out in order.
    pub(crate) fn outbox(&self, ctx: &Context) -> Arc<Subscriber> {
        match self.subscriber() {
            Some(outbox) => outbox.clone(),
            None => ctx.new_outbox(self.id, self.resp3),
        }
    }

    /// Its subscriptions, made on its first subscribe.
    pub(crate) fn subscriptions(&mut self, ctx: &Context) -> &mut Subscriptions {
        self.unrestricted = false; // (as `refresh` would make it)
        if self.pubsub.is_none() {
            let me = self.outbox(ctx);
            self.pubsub = Some(Box::new(Subscriptions::new(&ctx.pubsub, me)));
            self.update_outbox();
        }
        self.pubsub.as_deref_mut().expect("just made")
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
    #[inline]
    pub fn subscriber(&self) -> Option<&Arc<Subscriber>> {
        self.current_outbox.as_ref()
    }

    /// The outbox of what's using one (they share it): see [`Session::outbox`].
    fn update_outbox(&mut self) {
        self.current_outbox = self
            .pubsub
            .as_deref()
            .map(Subscriptions::subscriber)
            .or(self.monitor.as_ref())
            .or(self.tracking.as_ref().map(|t| &t.outbox))
            .or(self.handed.as_ref())
            .cloned();
    }
}
