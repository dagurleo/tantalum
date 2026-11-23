//! The replica's side (`REPLICAOF host port`): a thread connects to the
//! master, says what it is (`PING`, `AUTH` with `masterauth`, `REPLCONF
//! listening-port` and `capa eof psync2`) and asks to continue from where it
//! was (`PSYNC replid offset`, or `PSYNC ? -1` the first time). For a full
//! sync it reads the master's RDB file, replaces the data with it (clients
//! get `-LOADING` meanwhile, and the old data until then), and from then on
//! runs each command the master sends, as the master ran it: in a write's
//! turn, with no key expiring by this server's clock (the master says when
//! one goes), past `maxmemory` (a replica holds what its master has), and
//! passing the same bytes on to replicas of its own. Every second it says
//! how far it got (`REPLCONF ACK offset`), and at once when asked (`REPLCONF
//! GETACK`). When the connection drops, it keeps the data and continues
//! where it stopped, once the master is back.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::cmd::Args;
use crate::context::{Context, Session};

/// The link's state, as `INFO`'s `master_link_status` and `ROLE` say it.
pub(crate) const CONNECT: u8 = 0;
pub(crate) const CONNECTING: u8 = 1;
pub(crate) const SYNC: u8 = 2;
pub(crate) const CONNECTED: u8 = 3;

/// The master this server follows.
pub(crate) struct Link {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) state: AtomicU8,
    /// How much of the master's stream has been run: `master_repl_offset`.
    offset: AtomicU64,
    /// The database the master's stream is in by then: a stream that
    /// continues after a reconnect doesn't say again.
    db: std::sync::atomic::AtomicU32,
    /// When the master last sent anything, for `master_last_io_seconds_ago`.
    last_io: Mutex<Instant>,
    /// When the link last went down, for `master_link_down_since_seconds`.
    down_since: Mutex<Instant>,
    /// How much of a full sync's file has come, and how big it is (0: not
    /// known yet), while one runs.
    pub(crate) sync_read: AtomicU64,
    pub(crate) sync_size: AtomicU64,
    stop: AtomicBool,
    /// The connection, to shut down when the link stops.
    socket: Mutex<Option<TcpStream>>,
}

impl Link {
    pub(crate) fn up(&self) -> bool {
        self.state.load(Relaxed) == CONNECTED
    }

    pub(crate) fn offset(&self) -> u64 {
        self.offset.load(Relaxed)
    }

    pub(crate) fn last_io_seconds(&self) -> u64 {
        self.last_io.lock().elapsed().as_secs()
    }

    pub(crate) fn down_seconds(&self) -> u64 {
        self.down_since.lock().elapsed().as_secs()
    }

    /// Stops following: the thread ends at its next look.
    pub(crate) fn stop(&self) {
        self.stop.store(true, Relaxed);
        if let Some(socket) = self.socket.lock().take() {
            let _ = socket.shutdown(std::net::Shutdown::Both);
        }
    }
}

/// Starts following `host:port`, asking to continue this server's own
/// stream (its ID and offset as of now: for a master demoted to follow its
/// promoted replica, which can continue it), on a thread of its own.
pub(crate) fn follow(ctx: &Arc<Context>, host: String, port: u16) {
    let ids = ctx.repl.ids();
    let offset = ctx.repl.offset();
    // Its own replicas sync again, from what it has from its new master.
    if let Some(master) = ctx.repl.master() {
        master.drop_followers();
    }
    let link = Arc::new(Link {
        host,
        port,
        state: AtomicU8::new(CONNECT),
        offset: AtomicU64::new(offset),
        db: std::sync::atomic::AtomicU32::new(0),
        last_io: Mutex::new(Instant::now()),
        down_since: Mutex::new(Instant::now()),
        sync_read: AtomicU64::new(0),
        sync_size: AtomicU64::new(0),
        stop: AtomicBool::new(false),
        socket: Mutex::new(None),
    });
    if let Some(old) = ctx.repl.link.lock().replace(link.clone()) {
        old.stop();
    }
    ctx.repl.set_following(true);
    ctx.update_special();
    let ctx = ctx.clone();
    let _ = std::thread::Builder::new()
        .name("tantalum-replica".into())
        .spawn(move || {
            // Here, not on the I/O thread that ran `REPLICAOF`: it waits for them.
            let master = super::master::enable(&ctx);
            crate::segment::suspend_expiry();
            let mut cached = Some((ids.id, offset));
            while !link.stop.load(Relaxed) && !ctx.is_shutting_down() {
                link.state.store(CONNECTING, Relaxed);
                match run(&ctx, &link, &master, &mut cached) {
                    Ok(()) => {}
                    Err(e) if link.stop.load(Relaxed) => drop(e),
                    // A failover whose target can't be reached is over.
                    Err(e) if super::failover::in_progress(&ctx) => {
                        super::failover::abort(&ctx, &e.to_string());
                    }
                    Err(e) => eprintln!(
                        "replica of {}:{}: {e}; trying again in a second",
                        link.host, link.port
                    ),
                }
                if link.state.swap(CONNECT, Relaxed) == CONNECTED {
                    *link.down_since.lock() = Instant::now();
                }
                ctx.set_loading(false);
                if !link.stop.load(Relaxed) {
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        });
}

/// Stops following, keeping the data, which is this server's own now: a
/// new replication ID, with the master's kept as the one before, so that its
/// other replicas can continue from here (`REPLICAOF NO ONE`).
pub(crate) fn promote(ctx: &Context) {
    let Some(link) = ctx.repl.link.lock().take() else {
        return;
    };
    link.stop();
    let offset = ctx.repl.offset();
    let ids = ctx.repl.ids();
    ctx.repl.set_ids(super::Ids {
        id: super::new_id(),
        previous: ids.id,
        previous_until: offset as i64,
    });
    ctx.repl.set_following(false);
    ctx.update_special();
    ctx.set_loading(false);
}

fn timeout_of(ctx: &Context) -> Duration {
    Duration::from_secs(ctx.repl.settings.timeout.load(Relaxed).max(1))
}

/// One connection to the master: from its handshake until it drops.
fn run(
    ctx: &Arc<Context>,
    link: &Link,
    master: &Arc<super::Master>,
    cached: &mut Option<(String, u64)>,
) -> std::io::Result<()> {
    let timeout = timeout_of(ctx);
    // Each address the name has, in turn (localhost may be ::1 or 127.0.0.1).
    let mut connected = Err(std::io::Error::other("no address for the master"));
    for addr in (link.host.as_str(), link.port).to_socket_addrs()? {
        connected = TcpStream::connect_timeout(&addr, timeout);
        if connected.is_ok() {
            break;
        }
    }
    let socket = connected?;
    socket.set_nodelay(true)?;
    socket.set_read_timeout(Some(timeout))?;
    socket.set_write_timeout(Some(timeout))?;
    *link.socket.lock() = Some(socket.try_clone()?);
    if link.stop.load(Relaxed) {
        return Ok(());
    }
    let tls = ctx.repl.settings.tls.lock().clone();
    let conn = match tls {
        None => Conn::Plain(socket),
        Some(config) => {
            let name =
                rustls::pki_types::ServerName::try_from(link.host.clone()).map_err(|_| {
                    std::io::Error::other("the master's host isn't a name TLS can check")
                })?;
            let session =
                rustls::ClientConnection::new(config, name).map_err(std::io::Error::other)?;
            Conn::Tls(Box::new(rustls::StreamOwned::new(session, socket)))
        }
    };
    let mut reader = BufReader::with_capacity(64 * 1024, conn);
    let ask = |args: &[&[u8]], reader: &mut BufReader<Conn>| -> std::io::Result<Vec<u8>> {
        reader.get_mut().write_all(&command(args))?;
        line(reader)
    };

    // Who this is.
    let pong = ask(&[b"PING"], &mut reader)?;
    if pong.starts_with(b"-") && !pong.starts_with(b"-NOAUTH") && !pong.starts_with(b"-NOPERM") {
        return Err(error("PING", &pong));
    }
    let password = ctx.repl.settings.masterauth.lock().clone();
    if let Some(password) = password {
        let user = ctx.repl.settings.masteruser.lock().clone();
        let reply = match &user {
            Some(user) => ask(&[b"AUTH", user, &password], &mut reader)?,
            None => ask(&[b"AUTH", &password], &mut reader)?,
        };
        if reply.starts_with(b"-") {
            return Err(error("AUTH", &reply));
        }
    }
    let port = ctx.repl.settings.announce_port.load(Relaxed).to_string();
    let port = if port == "0" {
        ctx.config.get("port").unwrap_or_else(|| "6379".into())
    } else {
        port
    };
    ask(
        &[b"REPLCONF", b"listening-port", port.as_bytes()],
        &mut reader,
    )?;
    if let Some(ip) = ctx.repl.settings.announce_ip.lock().clone() {
        ask(&[b"REPLCONF", b"ip-address", ip.as_bytes()], &mut reader)?;
    }
    ask(
        &[b"REPLCONF", b"capa", b"eof", b"capa", b"psync2"],
        &mut reader,
    )?;

    // Where to start.
    let (id, next) = match cached.as_ref() {
        Some((id, offset)) => (id.clone(), (offset + 1).to_string()),
        None => ("?".to_string(), "-1".to_string()),
    };
    // A master failing over asks the replica it follows now to take over.
    let failing_over = super::failover::in_progress(ctx);
    let mut reply = if failing_over {
        ask(
            &[b"PSYNC", id.as_bytes(), next.as_bytes(), b"FAILOVER"],
            &mut reader,
        )?
    } else {
        ask(&[b"PSYNC", id.as_bytes(), next.as_bytes()], &mut reader)?
    };
    // Newlines are the master saying it's getting ready (a diskless sync
    // waits `repl-diskless-sync-delay` for more replicas first).
    while reply.is_empty() {
        reply = line(&mut reader)?;
        *link.last_io.lock() = Instant::now();
    }
    let words: Vec<&[u8]> = reply.split(|&b| b == b' ').collect();
    if failing_over {
        // (A full sync isn't what's expected, but works.)
        let accepted = reply.starts_with(b"+FULLRESYNC") || reply.starts_with(b"+CONTINUE");
        super::failover::answered(ctx, accepted);
        if !accepted {
            return Err(error("PSYNC FAILOVER", &reply));
        }
    }
    if reply.starts_with(b"+FULLRESYNC") && words.len() >= 3 {
        let id = String::from_utf8_lossy(words[1]).into_owned();
        let offset = crate::resp::parse_int(words[2])
            .filter(|&n| n >= 0)
            .ok_or_else(|| error("PSYNC", &reply))? as u64;
        link.state.store(SYNC, Relaxed);
        load(ctx, &mut reader, link)?;
        ctx.repl.set_ids(super::Ids {
            id: id.clone(),
            previous: super::NO_ID.to_string(),
            previous_until: -1,
        });
        master.backlog.reset(offset);
        master.backlog.forget_db();
        link.offset.store(offset, Relaxed);
        link.db.store(0, Relaxed);
        *cached = Some((id, offset));
    } else if reply.starts_with(b"+CONTINUE") {
        // A master with a new ID (promoted since): this one's history goes on
        // under it, the old one kept for replicas of this server.
        if let Some(new) = words.get(1).filter(|id| id.len() == 40) {
            let new = String::from_utf8_lossy(new).into_owned();
            let ids = ctx.repl.ids();
            if new != ids.id {
                ctx.repl.set_ids(super::Ids {
                    id: new.clone(),
                    previous: ids.id,
                    previous_until: link.offset() as i64,
                });
                if let Some((id, _)) = cached.as_mut() {
                    *id = new;
                }
            }
        }
    } else {
        // A master still syncing itself, loading, or one that won't: later.
        return Err(error("PSYNC", &reply));
    }
    link.state.store(CONNECTED, Relaxed);
    *link.last_io.lock() = Instant::now();
    apply(ctx, link, master, reader, cached)
}

/// Replaces the data with the master's file after `+FULLRESYNC`, loaded as
/// it comes (as Redis' `repl-diskless-load` loads it), so it's never held
/// whole: every database emptied, then the file, with no write running
/// meanwhile, and the server `LOADING` from the start. The file is `$len` and
/// that many bytes, or (from a master that streams it, for a replica that
/// said `capa eof`) `$EOF:mark`, the file, and the mark. Newlines before it
/// are the master saying it's still working on it. If it fails partway, the
/// databases are emptied again (as Redis empties them), for the next sync.
fn load(ctx: &Context, reader: &mut BufReader<Conn>, link: &Link) -> std::io::Result<()> {
    let header = loop {
        let line = line(reader)?;
        *link.last_io.lock() = Instant::now();
        if !line.is_empty() {
            break line;
        }
    };
    let mark = header.strip_prefix(b"$EOF:");
    let len = match mark {
        Some(_) => None,
        None => Some(
            header
                .strip_prefix(b"$")
                .and_then(crate::resp::parse_int)
                .and_then(|n| u64::try_from(n).ok())
                .ok_or_else(|| error("the RDB file", &header))?,
        ),
    };
    let log = ctx.log().expect("replicas take turns");
    let empty = || {
        for db in ctx.store.live() {
            db.clear();
        }
        let mut functions = ctx.scripting.functions.lock();
        functions.flush(&ctx.store);
        ctx.scripting.count_functions(&functions);
    };
    ctx.set_loading(true);
    let loaded = log.exclusive(|| {
        empty();
        ctx.repl.forget_written_here();
        let loaded = if let Some(mark) = mark {
            let mut src = Marked::new(Progress::new(&mut *reader, link), mark);
            crate::rdb_file::load(ctx, &mut src).and_then(|loaded| {
                // What's left of the stream to the mark.
                std::io::copy(&mut src, &mut std::io::sink())
                    .map_err(|e| format!("reading the file: {e}"))?;
                Ok(loaded)
            })
        } else {
            let len = len.expect("a length or a mark");
            link.sync_size.store(len, Relaxed);
            let mut src = Progress::new(&mut *reader, link).take(len);
            crate::rdb_file::load(ctx, &mut src).and_then(|loaded| {
                std::io::copy(&mut src, &mut std::io::sink())
                    .map_err(|e| format!("reading the file: {e}"))?;
                if src.limit() > 0 {
                    return Err(format!(
                        "the connection closed {} bytes before the file's end",
                        src.limit()
                    ));
                }
                Ok(loaded)
            })
        };
        if loaded.is_err() {
            empty();
        }
        loaded
    });
    ctx.set_loading(false);
    loaded
        .map(drop)
        .map_err(|e| std::io::Error::other(format!("loading the master's RDB: {e}")))
}

/// The master's connection as the file's read from it: what's come counted,
/// for `INFO`, and when, for the link's timeout.
struct Progress<'a, R> {
    inner: R,
    link: &'a Link,
    read: u64,
}

impl<'a, R> Progress<'a, R> {
    fn new(inner: R, link: &'a Link) -> Self {
        link.sync_read.store(0, Relaxed);
        Progress {
            inner,
            link,
            read: 0,
        }
    }
}

impl<R: Read> Read for Progress<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.read += n as u64;
        self.link.sync_read.store(self.read, Relaxed);
        *self.link.last_io.lock() = Instant::now();
        Ok(n)
    }
}

/// A streamed file, which ends with its mark: the bytes before it. The last
/// `mark.len()` bytes read are held back until more come, and when what's
/// been read ends with the mark, the file has ended (as Redis tells: a file
/// that ends with the 40 random bytes before its end isn't one to expect),
/// so nothing past it is read.
struct Marked<R> {
    inner: R,
    mark: Vec<u8>,
    held: Vec<u8>,
    ended: bool,
}

impl<R: Read> Marked<R> {
    fn new(inner: R, mark: &[u8]) -> Self {
        Marked {
            inner,
            mark: mark.to_vec(),
            held: Vec::new(),
            ended: false,
        }
    }
}

impl<R: Read> Read for Marked<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            let free = if self.ended {
                self.held.len()
            } else {
                self.held.len().saturating_sub(self.mark.len())
            };
            if free > 0 || self.ended {
                let n = free.min(buf.len());
                buf[..n].copy_from_slice(&self.held[..n]);
                self.held.drain(..n);
                return Ok(n);
            }
            let have = self.held.len();
            self.held.resize(have + buf.len().max(64 * 1024), 0);
            let n = match self.inner.read(&mut self.held[have..]) {
                Ok(n) => n,
                Err(e) => {
                    self.held.truncate(have);
                    return Err(e);
                }
            };
            self.held.truncate(have + n);
            if n == 0 {
                return Err(std::io::ErrorKind::UnexpectedEof.into());
            }
            if self.held.ends_with(&self.mark) {
                self.held.truncate(self.held.len() - self.mark.len());
                self.ended = true;
            }
        }
    }
}

/// Runs what the master sends until the connection drops or the link stops.
fn apply(
    ctx: &Arc<Context>,
    link: &Link,
    master: &Arc<super::Master>,
    mut reader: BufReader<Conn>,
    cached: &mut Option<(String, u64)>,
) -> std::io::Result<()> {
    let log = ctx.log().expect("replicas take turns");
    // Reads wake at least every second, to ack.
    reader
        .get_ref()
        .tcp()
        .set_read_timeout(Some(Duration::from_secs(1)))?;
    let timeout = timeout_of(ctx);
    let mut session = master_session(ctx);
    session.select(ctx, link.db.load(Relaxed));
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut ranges = Vec::new();
    let mut out = Vec::new();
    let mut acked = Instant::now();
    let mut chunk = vec![0u8; 64 * 1024];
    let mut held = Vec::new();
    // Loaded, or continuing: the master hears so at once (after a streamed
    // file, it waits for this before sending the stream).
    reader.get_mut().write_all(&ack(link.offset()))?;
    loop {
        if link.stop.load(Relaxed) {
            return Ok(());
        }
        let n = match reader.read(&mut chunk) {
            Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => n,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                0
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => 0,
            Err(e) => return Err(e),
        };
        if n > 0 {
            buf.extend_from_slice(&chunk[..n]);
            *link.last_io.lock() = Instant::now();
        } else if link.last_io.lock().elapsed() > timeout {
            return Err(std::io::Error::other("the master went quiet: timeout"));
        }
        let mut pos = 0;
        while let Ok(Some(end)) = crate::resp::parse_command(&buf, pos, &mut ranges) {
            let bytes = &buf[pos..end];
            let args = Args::new(&buf, &ranges);
            let getack = args.len() >= 2
                && args.get(0).eq_ignore_ascii_case(b"REPLCONF")
                && args.get(1).eq_ignore_ascii_case(b"GETACK");
            {
                // As other connections do once a batch: so that what's
                // watching every command (keyspace notifications, MONITOR,
                // statistics) sees the master's, as on Redis' replicas.
                ctx.follow_special(&mut session);
                let turn = log.turn(&ctx.store, false, session.db);
                if !args.is_empty() && !getack && !args.get(0).eq_ignore_ascii_case(b"REPLCONF") {
                    out.clear();
                    crate::store::allowing_oom(|| {
                        crate::cmd::execute(ctx, &mut session, turn.now, &args, &mut out)
                    });
                }
                // A transaction counts as run, and goes on to replicas of this
                // one, once its EXEC has: should the link drop in the middle,
                // the master sends it again whole, as Redis does.
                held.extend_from_slice(bytes);
                if session.multi.is_none() {
                    super::propagate::raw(&held);
                    link.offset.fetch_add(held.len() as u64, Relaxed);
                    link.db.store(session.db, Relaxed);
                    held.clear();
                } else {
                    super::propagate::raw(&[]);
                }
                drop(turn);
            }
            if let Some((_, offset)) = cached.as_mut() {
                *offset = link.offset();
            }
            if getack {
                reader.get_mut().write_all(&ack(link.offset()))?;
                acked = Instant::now();
            }
            pos = end;
        }
        buf.drain(..pos);
        ctx.publish(&mut session);
        if acked.elapsed() >= Duration::from_secs(1) {
            reader.get_mut().write_all(&ack(link.offset()))?;
            acked = Instant::now();
        }
        let _ = master;
    }
}

/// A session for the master's commands: it may run everything, its replies
/// go nowhere.
fn master_session(_: &Context) -> Session {
    let mut session = Session::for_replay();
    session.set_authenticated(true);
    session
}

fn ack(offset: u64) -> Vec<u8> {
    command(&[b"REPLCONF", b"ACK", offset.to_string().as_bytes()])
}

fn command(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    crate::resp::write_array_header(&mut out, args.len());
    for arg in args {
        crate::resp::write_bulk(&mut out, arg);
    }
    out
}

/// A one-line reply, without its `\r\n`.
fn line(reader: &mut impl BufRead) -> std::io::Result<Vec<u8>> {
    let mut line = Vec::new();
    reader.read_until(b'\n', &mut line)?;
    if line.is_empty() {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }
    while matches!(line.last(), Some(b'\n' | b'\r')) {
        line.pop();
    }
    Ok(line)
}

fn error(what: &str, reply: &[u8]) -> std::io::Error {
    std::io::Error::other(format!(
        "the master replied to {what}: {}",
        String::from_utf8_lossy(reply)
    ))
}

/// The connection to the master: plain, or TLS (`--tls-replication`).
enum Conn {
    Plain(TcpStream),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}

impl Conn {
    fn tcp(&self) -> &TcpStream {
        match self {
            Conn::Plain(socket) => socket,
            Conn::Tls(stream) => &stream.sock,
        }
    }
}

impl Read for Conn {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Conn::Plain(socket) => socket.read(buf),
            Conn::Tls(stream) => stream.read(buf),
        }
    }
}

impl Write for Conn {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Conn::Plain(socket) => socket.write(buf),
            Conn::Tls(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Conn::Plain(socket) => socket.flush(),
            Conn::Tls(stream) => stream.flush(),
        }
    }
}

/// How a replica checks its master's certificate (`--tls-ca-cert-file`): by
/// the CAs in the file, or, for a certificate in it that the master presents
/// as it is, by that alone (a self-signed one, pinned).
#[derive(Debug)]
struct Verifier {
    by_ca: Option<Arc<rustls::client::WebPkiServerVerifier>>,
    pinned: Vec<rustls::pki_types::CertificateDer<'static>>,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl rustls::client::danger::ServerCertVerifier for Verifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        ocsp: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        if self
            .pinned
            .iter()
            .any(|cert| cert.as_ref() == end_entity.as_ref())
        {
            return Ok(rustls::client::danger::ServerCertVerified::assertion());
        }
        match &self.by_ca {
            Some(by_ca) => {
                by_ca.verify_server_cert(end_entity, intermediates, server_name, ocsp, now)
            }
            None => Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::UnknownIssuer,
            )),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// The TLS setup a replica connects to its master with: trusting the
/// certificates in the PEM file `ca_file`, as CAs or as they are.
pub(crate) fn tls_config(ca_file: &std::path::Path) -> std::io::Result<Arc<rustls::ClientConfig>> {
    use rustls::pki_types::pem::PemObject;
    let certs = rustls::pki_types::CertificateDer::pem_file_iter(ca_file)
        .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
        .map_err(|e| std::io::Error::other(format!("{}: {e}", ca_file.display())))?;
    if certs.is_empty() {
        return Err(std::io::Error::other(format!(
            "{}: no certificates in it",
            ca_file.display()
        )));
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    for cert in &certs {
        let _ = roots.add(cert.clone());
    }
    let by_ca = rustls::client::WebPkiServerVerifier::builder_with_provider(
        Arc::new(roots),
        provider.clone(),
    )
    .build()
    .ok();
    let verifier = Verifier {
        by_ca,
        pinned: certs,
        provider: provider.clone(),
    };
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(std::io::Error::other)?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth();
    Ok(Arc::new(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Gives what it holds `n` bytes a read, then fails (as a connection
    /// with nothing more would hang).
    struct Trickle(Vec<u8>, usize);

    impl Read for Trickle {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.0.is_empty() {
                return Err(std::io::ErrorKind::WouldBlock.into());
            }
            let n = self.1.min(buf.len()).min(self.0.len());
            buf[..n].copy_from_slice(&self.0[..n]);
            self.0.drain(..n);
            Ok(n)
        }
    }

    #[test]
    fn a_marked_file_ends_at_its_mark_and_reads_nothing_past_it() {
        // As a master makes one: 40 random hex digits (which don't overlap
        // themselves, as 40 of one byte would).
        let mark = *b"3f1c9a07e25b8d64c0f7a2e19b53d8604ae7c21f";
        // The file has a mark's start in it, and ends with part of one.
        let mut file = b"REDIS0011".to_vec();
        file.extend_from_slice(&mark[..39]);
        file.extend_from_slice(b"xyz");
        file.extend_from_slice(&mark[..20]);
        for n in [1, 3, 39, 40, 41, 1000] {
            let mut stream = file.clone();
            stream.extend_from_slice(&mark);
            let mut marked = Marked::new(Trickle(stream, n), &mark);
            let mut out = Vec::new();
            marked.read_to_end(&mut out).unwrap();
            assert_eq!(out, file, "{n}");
        }
        // A connection that closes before the mark is an error.
        let mut marked = Marked::new(&file[..], &mark);
        assert!(marked.read_to_end(&mut Vec::new()).is_err());
    }
}
