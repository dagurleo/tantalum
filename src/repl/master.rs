//! The master's side: a replica's `SYNC` or `PSYNC` hands its connection to a
//! thread of its own, which sends it the data (an RDB file of it all, or
//! nothing if it can continue) and then the stream, as it grows, and reads
//! its `REPLCONF ACK`s, for `WAIT` and `INFO`.
//!
//! The full sync's RDB is the data at one point of the stream: while no
//! write runs (`Log::exclusive`; reads go on), every shard is marked as owing
//! a copy of itself, at the offset the replica's reading starts from. Then
//! writes go on, and each shard makes its copy before the first of them can
//! change it (`Store::request_copies`), as a fork's pages are copied before
//! they're written; the file is written from the copies, streamed to a
//! replica that says it reads that (`capa eof`).

use std::io::{Read, Write};
use std::net::TcpStream;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use super::backlog::{Backlog, Bell, Read as BacklogRead};
use crate::cmd::{Args, Reply};
use crate::context::{Context, Session};
use crate::resp;
use crate::store::Millis;

/// A replica's state, as `INFO` names it.
pub(crate) const WAIT_BGSAVE: u8 = 0;
pub(crate) const SEND_BULK: u8 = 1;
pub(crate) const ONLINE: u8 = 2;

/// A replica following this server.
pub(crate) struct Follower {
    /// Its connection's client id.
    pub(crate) id: u64,
    pub(crate) ip: String,
    /// The port it says it listens on (`REPLCONF listening-port`).
    pub(crate) port: u16,
    pub(crate) state: AtomicU8,
    /// How far it says it got (`REPLCONF ACK`).
    pub(crate) ack: AtomicU64,
    /// When it last said so, in milliseconds since the stream was made.
    pub(crate) ack_at: AtomicU64,
    bell: Arc<Bell>,
    stop: AtomicBool,
}

/// What a replica said of itself before `PSYNC` (`REPLCONF`).
#[derive(Clone, Debug, Default)]
pub(crate) struct Handshake {
    pub(crate) port: u16,
    pub(crate) ip: Option<String>,
    pub(crate) eof: bool,
}

/// A client blocked in `WAIT`, served once enough replicas have its offset.
struct Waiting {
    offset: u64,
    needed: usize,
    waiter: Arc<crate::blocking::Waiter>,
}

/// The stream replicas follow, and who follows it.
pub(crate) struct Master {
    pub(crate) backlog: Backlog,
    followers: Mutex<Vec<Arc<Follower>>>,
    waiting: Mutex<Vec<Waiting>>,
    born: Instant,
}

impl Master {
    pub(crate) fn new(backlog_size: usize, offset: u64) -> Self {
        Master {
            backlog: Backlog::new(backlog_size, offset),
            followers: Mutex::new(Vec::new()),
            waiting: Mutex::new(Vec::new()),
            born: Instant::now(),
        }
    }

    pub(crate) fn followers(&self) -> Vec<Arc<Follower>> {
        self.followers.lock().clone()
    }

    fn millis(&self) -> u64 {
        self.born.elapsed().as_millis() as u64
    }

    /// Seconds since `follower` last acknowledged: `INFO`'s `lag`.
    pub(crate) fn lag(&self, follower: &Follower) -> u64 {
        self.millis().saturating_sub(follower.ack_at.load(Relaxed)) / 1000
    }

    /// How many online replicas have acknowledged `offset`.
    pub(crate) fn acknowledged(&self, offset: u64) -> usize {
        self.followers
            .lock()
            .iter()
            .filter(|f| f.state.load(Relaxed) == ONLINE && f.ack.load(Relaxed) >= offset)
            .count()
    }

    /// Replicas that acknowledged within `max_lag` seconds: for
    /// `min-replicas-to-write`.
    pub(crate) fn good_replicas(&self, max_lag: u64) -> usize {
        let followers = self.followers.lock();
        followers
            .iter()
            .filter(|f| f.state.load(Relaxed) == ONLINE && self.lag(f) <= max_lag)
            .count()
    }

    /// Disconnects every replica (it syncs again, if it comes back).
    pub(crate) fn drop_followers(&self) {
        for follower in self.followers.lock().iter() {
            follower.stop.store(true, Relaxed);
            follower.bell.ring();
        }
    }

    fn acked(&self) {
        let mut waiting = self.waiting.lock();
        waiting.retain(|w| {
            let count = self.acknowledged(w.offset);
            if count >= w.needed {
                crate::blocking::serve_from_outside(
                    &w.waiter,
                    format!(":{count}\r\n").into_bytes(),
                );
                false
            } else {
                w.waiter.is_pending()
            }
        });
    }
}

/// The stream replicas follow, made if there's none: then from now on every
/// write takes a turn and propagates. Returns once that holds for every write
/// (each I/O thread has finished what it was running), so that a full sync
/// after it misses none. Not on an I/O thread: it waits for them.
pub(crate) fn enable(ctx: &Arc<Context>) -> Arc<Master> {
    enable_with(ctx, |_| {})
}

/// Held while the stream is made or freed.
static ENABLING: Mutex<()> = Mutex::new(());

/// [`enable`], running `then` with the stream before it can be freed (a
/// replica joining it).
fn enable_with(ctx: &Arc<Context>, then: impl FnOnce(&Arc<Master>)) -> Arc<Master> {
    let _one = ENABLING.lock();
    // A snapshot's place in a stream, taken up at startup, holds only if no
    // write came since: otherwise replicas must sync in full.
    if ctx.repl.written_since_resume(&ctx.store) {
        ctx.repl.set_ids(super::Ids {
            id: super::new_id(),
            previous: super::NO_ID.to_string(),
            previous_until: -1,
        });
    }
    let offset = ctx.repl.offset();
    let (master, made) = ctx.repl.master_or_new(offset);
    if made {
        ctx.log_to_replicas(master.clone());
        ctx.wait_for_io_threads();
        ctx.store.set_replicating(true);
        spawn_cron(ctx, &master);
    }
    then(&master);
    master
}

/// Frees the stream after `repl-backlog-ttl` without replicas, as Redis frees
/// its backlog: writes stop taking turns, and this server's replication ID
/// changes (a replica that comes back syncs in full). Whether it did.
fn disable(ctx: &Context, master: &Arc<Master>) -> bool {
    let _one = ENABLING.lock();
    if !master.followers().is_empty() || ctx.repl.following() {
        return false;
    }
    // Reads go back to removing what they find expired before writes stop
    // taking turns: a write outside a turn must never meet a key a read left.
    ctx.store.set_replicating(false);
    ctx.stop_logging_to_replicas();
    ctx.repl.free_master(master.backlog.offset());
    true
}

/// Every second: pings replicas through the stream every
/// `repl-ping-replica-period` (so that a quiet stream still says it's alive),
/// and drops replicas that have said nothing for `repl-timeout`.
fn spawn_cron(ctx: &Arc<Context>, master: &Arc<Master>) {
    let ctx = Arc::downgrade(ctx);
    let master = Arc::downgrade(master);
    let _ = std::thread::Builder::new()
        .name("tantalum-repl-cron".into())
        .spawn(move || {
            let mut since_ping = 0;
            let mut alone_since = Instant::now();
            loop {
                std::thread::sleep(Duration::from_secs(1));
                let (Some(ctx), Some(master)) = (ctx.upgrade(), master.upgrade()) else {
                    return;
                };
                if !ctx.repl.master().is_some_and(|m| Arc::ptr_eq(&m, &master)) {
                    return; // freed
                }
                if !master.followers().is_empty() || ctx.repl.following() {
                    alone_since = Instant::now();
                }
                let ttl = ctx.repl.settings.backlog_ttl.load(Relaxed);
                if ttl > 0
                    && alone_since.elapsed() >= Duration::from_secs(ttl)
                    && disable(&ctx, &master)
                {
                    return;
                }
                since_ping += 1;
                let period = ctx.repl.settings.ping_period.load(Relaxed).max(1);
                let followers = master.followers();
                // A replica's own replicas hear its master's pings.
                if since_ping >= period && !followers.is_empty() && !ctx.repl.following() {
                    since_ping = 0;
                    if let Some(log) = ctx.log() {
                        log.exclusive(|| master.backlog.append_raw(b"*1\r\n$4\r\nPING\r\n"));
                    }
                }
                let timeout = ctx.repl.settings.timeout.load(Relaxed).max(1) * 1000;
                for follower in followers {
                    if follower.state.load(Relaxed) == ONLINE
                        && master
                            .millis()
                            .saturating_sub(follower.ack_at.load(Relaxed))
                            > timeout
                    {
                        follower.stop.store(true, Relaxed);
                        follower.bell.ring();
                    }
                }
            }
        });
}

/// `REPLCONF option value ...`: what a replica says of itself, and its acks
/// (which come on its own thread, after `PSYNC`, not here).
pub(crate) fn replconf(
    ctx: &Context,
    session: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    if args.len().is_multiple_of(2) {
        resp::write_error(out, crate::cmd::SYNTAX_ERROR);
        return Reply::Written;
    }
    let mut handshake = ctx.repl.handshake(session.id);
    let mut i = 1;
    while i + 1 < args.len() {
        let (option, value) = (args.get(i), args.get(i + 1));
        let is = |name: &[u8]| option.eq_ignore_ascii_case(name);
        if is(b"listening-port") {
            match resp::parse_int(value).and_then(|p| u16::try_from(p).ok()) {
                Some(port) => handshake.port = port,
                None => {
                    resp::write_error(out, crate::cmd::NOT_AN_INTEGER);
                    return Reply::Written;
                }
            }
        } else if is(b"ip-address") {
            handshake.ip = Some(String::from_utf8_lossy(value).into_owned());
        } else if is(b"capa") {
            if value.eq_ignore_ascii_case(b"eof") {
                handshake.eof = true;
            }
        } else if is(b"ack") || is(b"fack") || is(b"getack") {
            // An ack before PSYNC, or a master's question to a replica: nothing.
            return Reply::Written;
        } else if is(b"rdb-only") || is(b"rdb-filter-only") {
        } else {
            let msg = format!(
                "ERR Unrecognized REPLCONF option: {}",
                String::from_utf8_lossy(option)
            );
            resp::write_error(out, msg.as_bytes());
            return Reply::Written;
        }
        i += 2;
    }
    ctx.repl.set_handshake(session.id, handshake);
    out.extend_from_slice(resp::OK);
    Reply::Written
}

/// `SYNC` and `PSYNC replid offset`: this connection becomes a replica's
/// feed, on a thread of its own.
pub(crate) fn sync(
    ctx: &Context,
    session: &mut Session,
    _: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    if session.multi.is_some() {
        resp::write_error(out, b"ERR Command not allowed inside a transaction");
        return Reply::Written;
    }
    if ctx.repl.following() && !ctx.repl.link_up() {
        resp::write_error(
            out,
            b"NOMASTERLINK Can't SYNC while not connected with my master",
        );
        return Reply::Written;
    }
    let psync = args.get(0).eq_ignore_ascii_case(b"PSYNC").then(|| {
        let offset = resp::parse_int(args.get(2)).unwrap_or(-1);
        (String::from_utf8_lossy(args.get(1)).into_owned(), offset)
    });
    let Some(ctx) = ctx.arc() else {
        resp::write_error(out, b"ERR replication needs a running server");
        return Reply::Written;
    };
    let handshake = ctx.repl.take_handshake(session.id);
    let peer = ctx.peer_of(session);
    let ip = handshake.ip.clone().unwrap_or_else(|| {
        peer.rsplit_once(':')
            .map_or(peer.clone(), |(ip, _)| ip.to_string())
    });
    let id = session.id;
    let command = if psync.is_some() { "psync" } else { "sync" };
    // Before its thread starts, which forgets it when it ends.
    ctx.hand_to_replica(id, command);
    let start = move |conn: Box<dyn FeedConn>| {
        let feeding = ctx.clone();
        let started = std::thread::Builder::new()
            .name("tantalum-replica-feed".into())
            .spawn(move || {
                if let Err(e) = feed(&feeding, conn, id, ip, handshake, psync) {
                    eprintln!("replica {id}: {e}");
                }
                feeding.forget_replica(id);
            });
        if started.is_err() {
            ctx.forget_replica(id);
        }
    };
    if session.socket < 0 {
        // TLS: the connection hands its stream over as it closes.
        HANDOFF.set(Some(Box::new(start)));
        return Reply::Close;
    }
    // SAFETY: the connection's socket is open while its command runs; this
    // copy of it lives on after the connection closes its own.
    let fd: RawFd = unsafe { libc::dup(session.socket) };
    if fd < 0 {
        resp::write_error(out, b"ERR can't hand the connection to a replica's thread");
        return Reply::Written;
    }
    // SAFETY: `fd` is a fresh duplicate this owns.
    let socket = unsafe { TcpStream::from_raw_fd(fd) };
    let _ = socket.set_nonblocking(false);
    start(Box::new(socket));
    // The connection closes its own copy of the socket; the replica's thread
    // has it now.
    Reply::Close
}

/// A replica's connection, as its thread uses it: plain, or TLS over it.
pub(crate) trait FeedConn: Read + Write + Send {
    /// The TCP socket under it.
    fn tcp(&self) -> &TcpStream;
}

impl FeedConn for TcpStream {
    fn tcp(&self) -> &TcpStream {
        self
    }
}

impl FeedConn for rustls::StreamOwned<rustls::ServerConnection, TcpStream> {
    fn tcp(&self) -> &TcpStream {
        &self.sock
    }
}

type Handoff = Box<dyn FnOnce(Box<dyn FeedConn>)>;

thread_local! {
    /// A replica's `PSYNC` over TLS: what its connection does with its stream
    /// as it closes (it can't be shared, as a plain socket is).
    static HANDOFF: std::cell::Cell<Option<Handoff>> = const { std::cell::Cell::new(None) };
}

/// For a connection that's closing: what to do with its stream, if it's a
/// replica's.
pub(crate) fn take_handoff() -> Option<Handoff> {
    HANDOFF.take()
}

/// A replica's thread: syncs it, then sends it the stream and reads its acks
/// until it goes.
fn feed(
    ctx: &Arc<Context>,
    mut socket: Box<dyn FeedConn>,
    id: u64,
    ip: String,
    handshake: Handshake,
    psync: Option<(String, i64)>,
) -> std::io::Result<()> {
    let bell = Arc::new(Bell::new()?);
    let mut follower = None;
    let master = enable_with(ctx, |master| {
        let joined = Arc::new(Follower {
            id,
            ip,
            port: handshake.port,
            state: AtomicU8::new(WAIT_BGSAVE),
            ack: AtomicU64::new(0),
            ack_at: AtomicU64::new(master.millis()),
            bell: bell.clone(),
            stop: AtomicBool::new(false),
        });
        master.followers.lock().push(joined.clone());
        follower = Some(joined);
    });
    let follower = follower.expect("joined");
    let log = ctx.log().expect("enabled");
    struct Leave<'a>(&'a Master, u64);
    impl Drop for Leave<'_> {
        fn drop(&mut self) {
            self.0.backlog.remove_reader(self.1);
            self.0.followers.lock().retain(|f| f.id != self.1);
        }
    }
    let _leave = Leave(&master, id);
    let timeout = Duration::from_secs(ctx.repl.settings.timeout.load(Relaxed).max(1));
    socket.tcp().set_write_timeout(Some(timeout))?;
    let _ = socket.tcp().set_nodelay(true);

    // Continue where it was, if the stream still has that.
    let ids = ctx.repl.ids();
    if let Some((replid, offset)) = &psync
        && *offset > 0
        && (*replid == ids.id || (*replid == ids.previous && *offset - 1 <= ids.previous_until))
        && master
            .backlog
            .add_reader(id, *offset as u64 - 1, bell.clone())
    {
        socket.write_all(format!("+CONTINUE {}\r\n", ids.id).as_bytes())?;
        follower.ack.store(*offset as u64 - 1, Relaxed);
        ctx.repl.syncs.partial_ok.fetch_add(1, Relaxed);
    } else {
        if psync.as_ref().is_some_and(|(replid, _)| replid != "?") {
            ctx.repl.syncs.partial_err.fetch_add(1, Relaxed);
        }
        ctx.repl.syncs.full.fetch_add(1, Relaxed);
        follower.state.store(SEND_BULK, Relaxed);
        // Writes wait only while the data is copied out; it's written from
        // the copy after, while they go on (into the backlog, from `offset`).
        let paused = Instant::now();
        let (copied, offset) = log.exclusive(|| {
            master.backlog.forget_db();
            let offset = master.backlog.offset();
            master.backlog.add_reader(id, offset, bell.clone());
            (crate::rdb_file::copy(&ctx.store, ctx.store.now()), offset)
        });
        let waited = paused.elapsed();
        if waited > Duration::from_millis(100) {
            eprintln!(
                "replica {id}: full sync: writes waited {} ms while the data was copied",
                waited.as_millis()
            );
        }
        let aux = crate::rdb_file::Aux {
            repl_id: ids.id.clone(),
            repl_offset: offset as i64,
            repl_stream_db: 0,
        };
        if psync.is_some() {
            socket.write_all(format!("+FULLRESYNC {} {offset}\r\n", ids.id).as_bytes())?;
        }
        if handshake.eof {
            // Streamed as it's written, between a mark it ends with, for a
            // replica that said it reads that (`capa eof`): never held whole.
            let mark = super::new_id();
            socket.write_all(format!("$EOF:{mark}\r\n").as_bytes())?;
            let mut out = std::io::BufWriter::with_capacity(256 * 1024, &mut socket);
            crate::rdb_file::write_copied(copied, &aux, &mut out)?;
            out.write_all(mark.as_bytes())?;
            out.flush()?;
            drop(out);
            // As Redis: the stream comes once the replica says it has loaded
            // the file (its first ack), so that what comes after the mark
            // isn't read as more of the file.
            let mut acks = Vec::new();
            let started = Instant::now();
            loop {
                if follower.stop.load(Relaxed) || started.elapsed() > timeout {
                    return Ok(());
                }
                let (readable, _) = wait(socket.tcp().as_raw_fd(), bell.ear.as_raw_fd(), 1000);
                if readable {
                    if !read_acks(&mut socket, &mut acks, true)? {
                        return Ok(());
                    }
                    took_acks(&master, &follower, &mut acks);
                    if follower.ack.load(Relaxed) >= offset {
                        break;
                    }
                }
            }
        } else {
            let mut rdb = Vec::new();
            crate::rdb_file::write_copied(copied, &aux, &mut rdb)?;
            socket.write_all(format!("${}\r\n", rdb.len()).as_bytes())?;
            socket.write_all(&rdb)?;
        }
        follower.ack.store(offset, Relaxed);
    }
    follower.ack_at.store(master.millis(), Relaxed);
    follower.state.store(ONLINE, Relaxed);

    let mut buf = Vec::with_capacity(64 * 1024);
    let mut acks = Vec::new();
    loop {
        if follower.stop.load(Relaxed) || ctx.is_shutting_down() {
            return Ok(());
        }
        buf.clear();
        match master.backlog.read(id, &mut buf, 256 * 1024) {
            BacklogRead::Some => {
                socket.write_all(&buf)?;
                if read_acks(&mut socket, &mut acks, false)? {
                    took_acks(&master, &follower, &mut acks);
                }
            }
            BacklogRead::Dropped => return Ok(()),
            BacklogRead::Waiting => {
                let (readable, _) = wait(socket.tcp().as_raw_fd(), bell.ear.as_raw_fd(), 1000);
                bell.hush();
                if readable {
                    if !read_acks(&mut socket, &mut acks, true)? {
                        return Ok(()); // it closed
                    }
                    took_acks(&master, &follower, &mut acks);
                }
            }
        }
    }
}

/// Waits up to `millis` for either fd to be readable: (`a`, `b`).
fn wait(a: RawFd, b: RawFd, millis: i32) -> (bool, bool) {
    let mut fds = [
        libc::pollfd {
            fd: a,
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: b,
            events: libc::POLLIN,
            revents: 0,
        },
    ];
    // SAFETY: two valid pollfds.
    let n = unsafe { libc::poll(fds.as_mut_ptr(), 2, millis) };
    if n <= 0 {
        return (false, false);
    }
    (fds[0].revents != 0, fds[1].revents != 0)
}

/// Reads what the replica sent into `acks`, without waiting. `false` if it
/// closed (when `must` says something was there).
fn read_acks(
    socket: &mut Box<dyn FeedConn>,
    acks: &mut Vec<u8>,
    must: bool,
) -> std::io::Result<bool> {
    socket.tcp().set_nonblocking(true)?;
    let mut chunk = [0u8; 4096];
    let mut any = false;
    let result = loop {
        match socket.read(&mut chunk) {
            Ok(0) => break Ok(!must && !any),
            Ok(n) => {
                acks.extend_from_slice(&chunk[..n]);
                any = true;
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => break Err(e),
        }
    };
    socket.tcp().set_nonblocking(false)?;
    if acks.len() > 1 << 20 {
        return Err(std::io::Error::other("a replica sent too much"));
    }
    result
}

/// Takes the `REPLCONF ACK offset [FACK offset]`s in `acks`.
fn took_acks(master: &Master, follower: &Follower, acks: &mut Vec<u8>) {
    let mut pos = 0;
    let mut acked = false;
    let mut args = Vec::new();
    while let Ok(Some(end)) = crate::resp::parse_command(acks, pos, &mut args) {
        let get = |i: usize| args.get(i).map(|r| &acks[r.clone()]);
        if get(0).is_some_and(|c| c.eq_ignore_ascii_case(b"REPLCONF"))
            && get(1).is_some_and(|c| c.eq_ignore_ascii_case(b"ACK"))
            && let Some(offset) = get(2).and_then(resp::parse_int)
        {
            follower.ack.fetch_max(offset.max(0) as u64, Relaxed);
            follower.ack_at.store(master.millis(), Relaxed);
            acked = true;
        }
        pos = end;
    }
    acks.drain(..pos);
    if acked {
        master.acked();
    }
}

/// `WAIT numreplicas timeout`, while replicas may follow: how many have
/// what's been written so far, waiting until that many do or time's up.
pub(crate) fn wait_for_replicas(
    ctx: &Context,
    session: &mut Session,
    master: &Arc<Master>,
    needed: usize,
    timeout: Option<Duration>,
    out: &mut Vec<u8>,
) -> Reply {
    let offset = master.backlog.offset();
    let count = master.acknowledged(offset);
    if count >= needed || session.deny_blocking {
        resp::write_integer(out, count as i64);
        return Reply::Written;
    }
    // Ask them now, rather than at their next ack.
    if let Some(log) = ctx.log() {
        log.exclusive(|| {
            master
                .backlog
                .append_raw(b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n")
        });
    }
    let counted = Arc::downgrade(master);
    let (reply, waiter) = crate::blocking::block_until(
        ctx,
        session,
        timeout,
        Box::new(move || {
            let count = counted.upgrade().map_or(0, |m| m.acknowledged(offset));
            format!(":{count}\r\n").into_bytes()
        }),
    );
    master.waiting.lock().push(Waiting {
        offset,
        needed,
        waiter,
    });
    // An ack may have come meanwhile.
    master.acked();
    reply
}

impl super::Replication {
    fn handshake(&self, client: u64) -> Handshake {
        self.handshakes
            .lock()
            .get(&client)
            .cloned()
            .unwrap_or_default()
    }

    fn set_handshake(&self, client: u64, handshake: Handshake) {
        let mut handshakes = self.handshakes.lock();
        if handshakes.len() > 10_000 {
            handshakes.clear();
        }
        handshakes.insert(client, handshake);
    }

    fn take_handshake(&self, client: u64) -> Handshake {
        self.handshakes.lock().remove(&client).unwrap_or_default()
    }
}
