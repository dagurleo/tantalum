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
    let addr = (link.host.as_str(), link.port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| std::io::Error::other("no address for the master"))?;
    let socket = TcpStream::connect_timeout(&addr, timeout)?;
    socket.set_nodelay(true)?;
    socket.set_read_timeout(Some(timeout))?;
    socket.set_write_timeout(Some(timeout))?;
    *link.socket.lock() = Some(socket.try_clone()?);
    if link.stop.load(Relaxed) {
        return Ok(());
    }
    let mut writer = socket.try_clone()?;
    let mut reader = BufReader::with_capacity(64 * 1024, socket);
    let mut ask = |args: &[&[u8]], reader: &mut BufReader<TcpStream>| -> std::io::Result<Vec<u8>> {
        writer.write_all(&command(args))?;
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
    let mut reply = ask(&[b"PSYNC", id.as_bytes(), next.as_bytes()], &mut reader)?;
    // Newlines are the master saying it's getting ready (a diskless sync
    // waits `repl-diskless-sync-delay` for more replicas first).
    while reply.is_empty() {
        reply = line(&mut reader)?;
        *link.last_io.lock() = Instant::now();
    }
    let words: Vec<&[u8]> = reply.split(|&b| b == b' ').collect();
    if reply.starts_with(b"+FULLRESYNC") && words.len() >= 3 {
        let id = String::from_utf8_lossy(words[1]).into_owned();
        let offset = crate::resp::parse_int(words[2])
            .filter(|&n| n >= 0)
            .ok_or_else(|| error("PSYNC", &reply))? as u64;
        link.state.store(SYNC, Relaxed);
        let rdb = read_rdb(&mut reader, link)?;
        load(ctx, &rdb)?;
        drop(rdb);
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

/// The RDB file after `+FULLRESYNC`: `$len` and that many bytes, or (from a
/// master that streams it, for a replica that said `capa eof`) `$EOF:mark`,
/// the file, and the mark. Newlines before it are the master saying it's
/// still working on it.
fn read_rdb(reader: &mut BufReader<TcpStream>, link: &Link) -> std::io::Result<Vec<u8>> {
    let header = loop {
        let line = line(reader)?;
        *link.last_io.lock() = Instant::now();
        if !line.is_empty() {
            break line;
        }
    };
    if !header.starts_with(b"$") {
        return Err(error("the RDB file", &header));
    }
    if let Some(mark) = header.strip_prefix(b"$EOF:") {
        let mark = mark.to_vec();
        let mut data = Vec::new();
        let mut chunk = vec![0u8; 64 * 1024];
        loop {
            let n = reader.read(&mut chunk)?;
            if n == 0 {
                return Err(std::io::ErrorKind::UnexpectedEof.into());
            }
            data.extend_from_slice(&chunk[..n]);
            link.sync_read.store(data.len() as u64, Relaxed);
            *link.last_io.lock() = Instant::now();
            if data.ends_with(&mark) {
                data.truncate(data.len() - mark.len());
                return Ok(data);
            }
        }
    }
    let len = crate::resp::parse_int(&header[1..])
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| error("the RDB file", &header))?;
    link.sync_size.store(len as u64, Relaxed);
    let mut data = vec![0u8; len];
    let mut got = 0;
    while got < len {
        let n = reader.read(&mut data[got..])?;
        if n == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        got += n;
        link.sync_read.store(got as u64, Relaxed);
        *link.last_io.lock() = Instant::now();
    }
    Ok(data)
}

/// Replaces the data with the master's: every database emptied, then the
/// file loaded, with no write running meanwhile.
fn load(ctx: &Context, rdb: &[u8]) -> std::io::Result<()> {
    let log = ctx.log().expect("replicas take turns");
    ctx.set_loading(true);
    let loaded = log.exclusive(|| {
        for db in ctx.store.live() {
            db.clear();
        }
        {
            let mut functions = ctx.scripting.functions.lock();
            functions.flush(&ctx.store);
            ctx.scripting.count_functions(&functions);
        }
        crate::rdb_file::load(ctx, rdb)
    });
    ctx.set_loading(false);
    loaded
        .map(drop)
        .map_err(|e| std::io::Error::other(format!("loading the master's RDB: {e}")))
}

/// Runs what the master sends until the connection drops or the link stops.
fn apply(
    ctx: &Arc<Context>,
    link: &Link,
    master: &Arc<super::Master>,
    mut reader: BufReader<TcpStream>,
    cached: &mut Option<(String, u64)>,
) -> std::io::Result<()> {
    let log = ctx.log().expect("replicas take turns");
    let mut writer = reader.get_ref().try_clone()?;
    // Reads wake at least every second, to ack.
    reader
        .get_ref()
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
                writer.write_all(&ack(link.offset()))?;
                acked = Instant::now();
            }
            pos = end;
        }
        buf.drain(..pos);
        ctx.publish(&mut session);
        if acked.elapsed() >= Duration::from_secs(1) {
            writer.write_all(&ack(link.offset()))?;
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
