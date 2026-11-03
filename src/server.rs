//! TCP server: one task per connection.
//!
//! Connections beyond `maxclients` get an error and are closed. A graceful
//! shutdown ([`Context::shutdown`], on SIGTERM or SIGINT via
//! [`handle_signals`]) stops the accept loops and shuts the read side of
//! every connection: each finishes what it has read, replies, sees end of
//! file and closes, and [`drain`] waits for that.
//!
//! Each connection reads whatever is available, runs every complete command
//! in that buffer (pipelining), and answers them all with a single write. That
//! keeps syscalls per command low. The buffer's commands are parsed before any
//! runs (`Batch`), so the store can prefetch the memory each one will read
//! while earlier ones run: at 1M keys, waiting on those cache misses one at a
//! time cost pipelined throughput almost half.
//!
//! Three ways to run the connections:
//! - `serve`: one shared, work-stealing tokio runtime. A ready connection may
//!   run on any thread, which balances uneven load but costs cross-thread
//!   wake-ups and queue traffic on every request.
//! - `serve_per_thread`: one single-threaded runtime per thread, like
//!   memcached's event loop per thread. Each connection stays on one thread for
//!   life, so a request never crosses threads. Without pipelining, where each
//!   request is tiny work between two syscalls, that overhead is what's left.
//! - `serve_uring` (experimental): one monoio event loop per thread. monoio
//!   uses io_uring where the kernel allows it, so a single `io_uring_enter`
//!   submits and reaps the reads and writes of many connections instead of two
//!   syscalls per request; elsewhere it falls back to epoll/kqueue. See
//!   `uring_setups` for how the rings are configured.

use std::io::{self, IoSlice};
use std::ops::Range;
use std::os::fd::AsRawFd;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::cmd::{self, Args, Reply};
use crate::context::{Context, Session};
use crate::resp;
use crate::store::{KeyHash, Millis, Store};

/// Minimum free space in the read buffer before each read.
const READ_RESERVE: usize = 16 * 1024;
/// A reply buffer's first allocation.
const WRITE_RESERVE: usize = 16 * 1024;
/// The read buffer an idle io_uring connection keeps posted: a read must
/// have its buffer in flight before data arrives. It grows as reads fill it.
const URING_READ_BUFFER: usize = 2 * 1024;
/// Flush mid-batch once this many reply bytes are queued, so a pipeline of
/// big replies can't buffer unbounded memory.
const FLUSH_THRESHOLD: usize = 64 * 1024;
/// Max buffered bytes of not-yet-complete commands, as Redis' `client-query-buffer-limit`.
const MAX_QUERY_BUFFER: usize = 1024 * 1024 * 1024;
/// Buffers larger than this are released after a big request or reply, even
/// mid-stream.
const SHRINK_ABOVE: usize = 1024 * 1024;
/// Most commands parsed ahead at once, which bounds a batch's memory.
const MAX_BATCH: usize = 256;
/// What a connection over `maxclients` gets, worded as Redis does.
const MAX_CLIENTS_ERROR: &[u8] = b"-ERR max number of clients reached\r\n";
/// What a connection gets if a command panics (a bug), before it's closed.
const PANIC_ERROR: &[u8] = b"-ERR internal error, closing the connection\r\n";

/// Commands parsed ahead of running them. Run one at a time, each command of a
/// pipelined batch waits on its own cache misses in turn (its buckets, then
/// its entry). Parsed first, the store can start loading the buckets and then
/// the entries of the commands a few ahead while command `i` runs
/// ([`Store::prefetch_ahead`]), so the misses overlap.
#[derive(Default)]
struct Batch {
    /// Argument ranges of every parsed command, back to back.
    args: Vec<Range<usize>>,
    cmds: Vec<Parsed>,
    scratch: Vec<Range<usize>>,
}

struct Parsed {
    /// Where its arguments end in `Batch::args`.
    args_end: usize,
    /// The read buffer position just after it.
    end: usize,
    /// The hash of its first argument (the key, for commands with one),
    /// shared by the prefetches and the command.
    key: Option<KeyHash>,
}

impl Batch {
    /// Parses up to `MAX_BATCH` complete commands from `buf[pos..]`. On a
    /// protocol error, the commands before it stay parsed.
    fn parse(
        &mut self,
        buf: &[u8],
        mut pos: usize,
        store: &Store,
    ) -> Result<(), resp::ProtocolError> {
        self.args.clear();
        self.cmds.clear();
        while self.cmds.len() < MAX_BATCH {
            let Some(next) = resp::parse_command(buf, pos, &mut self.scratch)? else {
                break;
            };
            self.args.extend_from_slice(&self.scratch);
            self.cmds.push(Parsed {
                args_end: self.args.len(),
                end: next,
                key: self
                    .scratch
                    .get(1)
                    .map(|key| store.key(&buf[key.clone()]).hash()),
            });
            pos = next;
        }
        Ok(())
    }

    fn len(&self) -> usize {
        self.cmds.len()
    }

    /// Command `i`, ready to run.
    fn command<'b>(&'b self, i: usize, buf: &'b [u8]) -> Args<'b> {
        let start = if i == 0 { 0 } else { self.cmds[i - 1].args_end };
        Args::new(buf, &self.args[start..self.cmds[i].args_end]).with_key_hash(self.cmds[i].key)
    }

    /// The read buffer position after command `i`.
    fn end(&self, i: usize) -> usize {
        self.cmds[i].end
    }

    /// Command `i`'s key hash, if there is a command `i` with a key and
    /// others to overlap it with.
    fn ahead(&self, i: usize) -> Option<KeyHash> {
        if self.len() > 1 {
            self.cmds.get(i)?.key
        } else {
            None
        }
    }

    /// Prefetches for the commands ahead of command `i`, before it runs.
    fn prefetch(&self, i: usize, store: &Store) {
        store.prefetch_ahead(i, |j| self.ahead(j));
    }
}

pub async fn serve(listener: TcpListener, ctx: Arc<Context>) -> io::Result<()> {
    loop {
        let socket = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((socket, _)) => socket,
                Err(e) => {
                    accept_failed(e).await;
                    continue;
                }
            },
            () = ctx.until_shutdown() => return Ok(()),
        };
        let ctx = ctx.clone();
        tokio::spawn(async move {
            // Errors here are almost always the client going away.
            let _ = handle_connection(socket, &ctx).await;
        });
    }
}

/// After a failed accept: retry at once if that connection was aborted,
/// otherwise (usually out of file descriptors) back off instead of spinning.
async fn accept_failed(e: io::Error) {
    if !matches!(
        e.kind(),
        io::ErrorKind::ConnectionAborted | io::ErrorKind::Interrupted
    ) {
        eprintln!("accept failed: {e}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Runs `threads` single-threaded runtimes and hands accepted connections to
/// them round-robin from the calling thread. Returns once shutdown starts
/// (the runtimes keep serving open connections) or if a runtime thread dies.
pub fn serve_per_thread(
    listener: std::net::TcpListener,
    ctx: Arc<Context>,
    threads: usize,
) -> io::Result<()> {
    let mut workers = Vec::with_capacity(threads);
    for i in 0..threads.max(1) {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<std::net::TcpStream>();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let ctx = ctx.clone();
        std::thread::Builder::new()
            .name(format!("tantalum-io-{i}"))
            .spawn(move || {
                runtime.block_on(async move {
                    while let Some(socket) = rx.recv().await {
                        // Registers the socket with this thread's reactor.
                        let Ok(socket) = TcpStream::from_std(socket) else {
                            continue;
                        };
                        let ctx = ctx.clone();
                        tokio::spawn(async move {
                            let _ = handle_connection(socket, &ctx).await;
                        });
                    }
                })
            })?;
        workers.push(tx);
    }
    listener.set_nonblocking(true)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let listener = TcpListener::from_std(listener)?;
        let mut next = 0;
        loop {
            let socket = tokio::select! {
                accepted = listener.accept() => match accepted {
                    Ok((socket, _)) => socket,
                    Err(e) => {
                        accept_failed(e).await;
                        continue;
                    }
                },
                () = ctx.until_shutdown() => return Ok(()),
            };
            // Deregistered from this thread's reactor, for the worker's.
            let Ok(socket) = socket.into_std() else {
                continue;
            };
            if workers[next].send(socket).is_err() {
                return Err(io::Error::other("an I/O thread exited"));
            }
            next = (next + 1) % workers.len();
        }
    })
}

/// Runs `threads` monoio event loops, each with its own listening socket on
/// `addr` (`SO_REUSEPORT`, so Linux spreads connections across them).
/// Returns once shutdown starts (the loops keep serving open connections),
/// or with an error if a loop fails.
pub fn serve_uring(
    addr: std::net::SocketAddr,
    ctx: Arc<Context>,
    threads: usize,
) -> io::Result<()> {
    let mut handles = (0..threads.max(1))
        .map(|i| {
            let ctx = ctx.clone();
            std::thread::Builder::new()
                .name(format!("tantalum-uring-{i}"))
                .spawn(move || -> io::Result<()> {
                    #[cfg(target_os = "linux")]
                    let runtime = {
                        let mut runtime = Err(io::Error::other("no io_uring setup to try"));
                        for (name, urb) in uring_setups() {
                            runtime = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                                .uring_builder(urb)
                                .enable_timer()
                                .build();
                            match &runtime {
                                Ok(_) => break,
                                Err(e) if i == 0 => eprintln!("io_uring setup {name} failed: {e}"),
                                Err(_) => {}
                            }
                        }
                        runtime
                    };
                    #[cfg(not(target_os = "linux"))]
                    let runtime = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                        .enable_timer()
                        .build();
                    let mut runtime = runtime?;
                    runtime.block_on(async move {
                        let opts = monoio::net::ListenerOpts::new()
                            .reuse_port(true)
                            .backlog(1024);
                        let listener = monoio::net::TcpListener::bind_with_config(addr, &opts)?;
                        loop {
                            monoio::select! {
                                accepted = listener.accept() => match accepted {
                                    Ok((socket, _)) => {
                                        let ctx = ctx.clone();
                                        monoio::spawn(async move {
                                            let _ = handle_uring(socket, &ctx).await;
                                        });
                                    }
                                    Err(e) => {
                                        eprintln!("accept failed: {e}");
                                        monoio::time::sleep(Duration::from_millis(10)).await;
                                    }
                                },
                                () = ctx.until_shutdown() => break,
                            }
                        }
                        drop(listener);
                        // Keep serving the open connections; the process
                        // exits once they're done.
                        std::future::pending().await
                    })
                })
        })
        .collect::<io::Result<Vec<_>>>()?;
    while !ctx.is_shutting_down() {
        if let Some(i) = handles.iter().position(|h| h.is_finished()) {
            return match handles.swap_remove(i).join() {
                Ok(Err(e)) => Err(e),
                Ok(Ok(())) => Err(io::Error::other("an I/O thread exited")),
                Err(_) => Err(io::Error::other("an I/O thread panicked")),
            };
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}

/// Starts a graceful shutdown on SIGTERM or SIGINT. A second one exits at
/// once.
pub fn handle_signals(ctx: Arc<Context>) -> io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()?;
    let (mut term, mut int) = {
        let _guard = runtime.enter();
        (
            signal(SignalKind::terminate())?,
            signal(SignalKind::interrupt())?,
        )
    };
    std::thread::Builder::new()
        .name("tantalum-signals".into())
        .spawn(move || {
            runtime.block_on(async move {
                let name = tokio::select! {
                    _ = term.recv() => "SIGTERM",
                    _ = int.recv() => "SIGINT",
                };
                eprintln!(
                    "{name}: shutting down; {} connections finishing",
                    ctx.connected()
                );
                ctx.shutdown();
                tokio::select! {
                    _ = term.recv() => {}
                    _ = int.recv() => {}
                }
                eprintln!("second signal: exiting now");
                std::process::exit(1);
            });
        })?;
    Ok(())
}

/// After shutdown has started, waits up to `timeout` for every connection
/// to close. Returns how many are still open.
pub fn drain(ctx: &Context, timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    while ctx.connected() > 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    ctx.connected()
}

/// Turns on TCP keepalive probes, as Redis' `tcp-keepalive` does: after
/// `time` idle, then every third of that, three times.
fn set_keepalive(socket: socket2::SockRef<'_>, time: Option<Duration>) {
    if let Some(time) = time {
        let keepalive = socket2::TcpKeepalive::new()
            .with_time(time)
            .with_interval(time / 3)
            .with_retries(3);
        let _ = socket.set_tcp_keepalive(&keepalive);
    }
}

/// Runs one command. A panic (a bug) is caught so it ends only this
/// connection, which gets an error: monoio, unlike tokio, doesn't contain a
/// panicking task, so it would take its whole thread down. Returns `None`
/// if it panicked.
fn run(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Option<Reply> {
    let reply = catch_unwind(AssertUnwindSafe(|| {
        cmd::execute(ctx, session, now, args, out)
    }));
    if reply.is_err() {
        out.extend_from_slice(PANIC_ERROR);
    }
    reply.ok()
}

/// Ring setups to try, best first. By default the kernel interrupts the thread
/// to post each completion. `defer` (DEFER_TASKRUN, Linux 6.1+) posts them all
/// at once when the event loop runs out of work and waits; `coop`
/// (COOP_TASKRUN, 5.19+) skips the interrupt and posts them at the next
/// syscall. `TANTALUM_URING_SETUP=coop|plain` starts lower, for comparison.
#[cfg(target_os = "linux")]
fn uring_setups() -> Vec<(&'static str, io_uring::Builder)> {
    let mut defer = io_uring::IoUring::builder();
    defer.setup_single_issuer().setup_defer_taskrun();
    let mut coop = io_uring::IoUring::builder();
    coop.setup_coop_taskrun();
    let mut setups = vec![
        ("defer", defer),
        ("coop", coop),
        ("plain", io_uring::IoUring::builder()),
    ];
    let first = match std::env::var("TANTALUM_URING_SETUP").as_deref() {
        Ok("coop") => 1,
        Ok("plain") => 2,
        _ => 0,
    };
    setups.drain(..first);
    setups
}

/// The connection loop of `handle_connection`, on monoio's owned-buffer I/O.
/// Large values are copied into the reply buffer rather than written with a
/// separate `writev`.
async fn handle_uring(mut socket: monoio::net::TcpStream, ctx: &Context) -> io::Result<()> {
    use monoio::buf::IoBufMut;
    use monoio::io::{AsyncReadRent, AsyncWriteRentExt};

    async fn flush(socket: &mut monoio::net::TcpStream, buf: Vec<u8>) -> io::Result<Vec<u8>> {
        let (result, mut buf) = socket.write_all(buf).await;
        result?;
        buf.clear();
        Ok(buf)
    }

    let Some(mut client) = ctx.connect(socket.as_raw_fd()) else {
        if !ctx.is_shutting_down() {
            let _ = socket.write_all(MAX_CLIENTS_ERROR).await;
        }
        return Ok(());
    };
    socket.set_nodelay(true)?;
    if let Some(time) = ctx.settings.tcp_keepalive {
        let _ = socket.set_tcp_keepalive(Some(time), Some(time / 3), Some(3));
    }
    let store = &*ctx.store;
    let mut session = client.session();
    let mut rbuf: Vec<u8> = Vec::with_capacity(URING_READ_BUFFER);
    let mut wbuf: Vec<u8> = Vec::new();
    let mut batch = Batch::default();
    loop {
        // Grow by doubling when reads fill it (a big value, a long pipeline).
        if rbuf.capacity() - rbuf.len() < URING_READ_BUFFER / 2 {
            rbuf.reserve(rbuf.capacity().max(URING_READ_BUFFER));
        }
        client.set_buffer_bytes(rbuf.capacity() + wbuf.capacity());
        let filled = rbuf.len();
        let read = socket.read(rbuf.slice_mut(filled..));
        let (result, slice) = match ctx.settings.timeout {
            None => read.await,
            Some(timeout) => match monoio::time::timeout(timeout, read).await {
                Ok(read) => read,
                Err(_) => return Ok(()),
            },
        };
        rbuf = slice.into_inner();
        if result? == 0 {
            return Ok(());
        }
        if wbuf.capacity() == 0 {
            wbuf.reserve(WRITE_RESERVE);
        }
        let now = store.now();
        let mut pos = 0;
        let mut close = false;
        loop {
            let parsed = batch.parse(&rbuf, pos, store);
            for i in 0..batch.len() {
                batch.prefetch(i, store);
                let args = batch.command(i, &rbuf);
                if !args.is_empty() {
                    match run(ctx, &mut session, now, &args, &mut wbuf) {
                        Some(Reply::Written) => {}
                        Some(Reply::Close) | None => close = true,
                        Some(Reply::Bulk(value)) => resp::write_bulk(&mut wbuf, value.as_bytes()),
                    }
                }
                pos = batch.end(i);
                if wbuf.len() >= FLUSH_THRESHOLD {
                    wbuf = flush(&mut socket, wbuf).await?;
                }
                if close {
                    break;
                }
            }
            if let Err(e) = parsed
                && !close
            {
                resp::write_error(&mut wbuf, format!("ERR {}", e.message()).as_bytes());
                flush(&mut socket, wbuf).await?;
                return Ok(());
            }
            if close || batch.len() < MAX_BATCH {
                break;
            }
        }
        ctx.publish(&mut session);
        rbuf.drain(..pos);
        if !wbuf.is_empty() {
            wbuf = flush(&mut socket, wbuf).await?;
        }
        if close {
            return Ok(());
        }
        if rbuf.len() > MAX_QUERY_BUFFER {
            resp::write_error(&mut wbuf, b"ERR query buffer limit exceeded");
            flush(&mut socket, wbuf).await?;
            return Ok(());
        }
        if rbuf.is_empty() {
            // Idle until the next request: keep only a small read buffer.
            if rbuf.capacity() > READ_RESERVE {
                rbuf = Vec::with_capacity(URING_READ_BUFFER);
            }
            wbuf = Vec::new();
        } else if wbuf.capacity() > SHRINK_ABOVE {
            wbuf = Vec::new();
        }
    }
}

pub async fn handle_connection(mut socket: TcpStream, ctx: &Context) -> io::Result<()> {
    let Some(mut client) = ctx.connect(socket.as_raw_fd()) else {
        if !ctx.is_shutting_down() {
            let _ = socket.write_all(MAX_CLIENTS_ERROR).await;
        }
        return Ok(());
    };
    // Without this, Nagle + delayed ACK can stall small replies by ~40ms.
    socket.set_nodelay(true)?;
    set_keepalive(socket2::SockRef::from(&socket), ctx.settings.tcp_keepalive);
    let store = &*ctx.store;
    let mut session = client.session();
    // Vecs rather than a BytesMut: after a big request, a BytesMut keeps the
    // whole allocation but reports only its unused tail as capacity, so the
    // shrinking below (and the memory accounting) would miss it. Both are
    // empty, holding no memory, while the connection waits for a request.
    let mut rbuf: Vec<u8> = Vec::new();
    let mut wbuf: Vec<u8> = Vec::new();
    let mut batch = Batch::default();

    loop {
        // Wait for data before taking a buffer to read it into. (Not
        // `readable()`, which queues each wait on a locked list: through
        // `poll_read_ready` it takes the reader's own waker slot, as reads do.)
        let ready = std::future::poll_fn(|cx| socket.poll_read_ready(cx));
        match ctx.settings.timeout {
            None => ready.await?,
            Some(timeout) => match tokio::time::timeout(timeout, ready).await {
                Ok(ready) => ready?,
                Err(_) => return Ok(()),
            },
        }
        if rbuf.capacity() - rbuf.len() < READ_RESERVE / 4 {
            rbuf.reserve(READ_RESERVE);
        }
        client.set_buffer_bytes(rbuf.capacity() + wbuf.capacity());
        // Data is ready, so this reads at once. Through `read_buf` (unlike
        // `try_read_buf`), a short read tells tokio the socket is drained, so
        // the next wait doesn't first try a read that finds nothing: that
        // cost an extra syscall per request.
        if socket.read_buf(&mut rbuf).await? == 0 {
            return Ok(());
        }
        if wbuf.capacity() == 0 {
            wbuf.reserve(WRITE_RESERVE);
        }
        // One clock read per batch, like Redis' cached time per event loop tick.
        let now = store.now();
        let mut pos = 0;
        let mut close = false;
        loop {
            let parsed = batch.parse(&rbuf, pos, store);
            for i in 0..batch.len() {
                batch.prefetch(i, store);
                let args = batch.command(i, &rbuf);
                if !args.is_empty() {
                    match run(ctx, &mut session, now, &args, &mut wbuf) {
                        Some(Reply::Written) => {}
                        Some(Reply::Close) | None => close = true,
                        Some(Reply::Bulk(value)) => {
                            // Header, value and trailer in one writev, without
                            // copying the value into the output buffer.
                            resp::write_bulk_header(&mut wbuf, value.as_bytes().len());
                            let mut parts = [
                                IoSlice::new(&wbuf),
                                IoSlice::new(value.as_bytes()),
                                IoSlice::new(b"\r\n"),
                            ];
                            write_all_vectored(&mut socket, &mut parts).await?;
                            wbuf.clear();
                        }
                    }
                }
                pos = batch.end(i);
                if wbuf.len() >= FLUSH_THRESHOLD {
                    socket.write_all(&wbuf).await?;
                    wbuf.clear();
                }
                if close {
                    break;
                }
            }
            if let Err(e) = parsed
                && !close
            {
                resp::write_error(&mut wbuf, format!("ERR {}", e.message()).as_bytes());
                socket.write_all(&wbuf).await?;
                return Ok(());
            }
            if close || batch.len() < MAX_BATCH {
                break;
            }
        }
        ctx.publish(&mut session);
        rbuf.drain(..pos);
        if !wbuf.is_empty() {
            socket.write_all(&wbuf).await?;
            wbuf.clear();
        }
        if close {
            return Ok(());
        }
        if rbuf.len() > MAX_QUERY_BUFFER {
            resp::write_error(&mut wbuf, b"ERR query buffer limit exceeded");
            socket.write_all(&wbuf).await?;
            return Ok(());
        }
        if rbuf.is_empty() {
            // Idle until the next request: hold no buffers.
            rbuf = Vec::new();
            wbuf = Vec::new();
            client.set_buffer_bytes(0);
        } else if wbuf.capacity() > SHRINK_ABOVE {
            wbuf = Vec::new();
        }
    }
}

async fn write_all_vectored(
    socket: &mut TcpStream,
    mut parts: &mut [IoSlice<'_>],
) -> io::Result<()> {
    while !parts.is_empty() {
        let n = socket.write_vectored(parts).await?;
        if n == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        IoSlice::advance_slices(&mut parts, n);
    }
    Ok(())
}
