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
//! keeps syscalls per command low. A blocking command (`BLPOP` ...) that has
//! to wait sends the replies before it and waits; the commands after it run
//! once it's done. The buffer's commands are parsed before any
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
use std::path::Path;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
pub use tokio_rustls::TlsAcceptor;

use crate::cmd::{self, Args, Blocked, Reply};
use crate::context::{Context, Session};
use crate::pubsub::Subscriber;
use crate::resp;
use crate::store::{KeyHash, Millis, Store};
use crate::wake::Mailbox;

/// Minimum free space in the read buffer before each read.
const READ_RESERVE: usize = 16 * 1024;
/// A reply buffer's first allocation.
const WRITE_RESERVE: usize = READ_RESERVE;
/// Buffers a thread keeps for its connections (see `SPARE_BUFFERS`).
const MAX_SPARE_BUFFERS: usize = 8;
/// The biggest reply an `Outbox` holds, and the most bytes it holds at once.
const OUTBOX_REPLY: usize = 4 * 1024;
const OUTBOX_BYTES: usize = 256 * 1024;
/// From how many replies held the flusher looks at the reactor before sending.
/// A look costs a syscall, not worth it for a client or two.
const LOOK_FROM: usize = 8;
/// Looks at most, a further one only if the last found more.
const LOOKS: usize = 2;
/// The longest batch whose replies are held: 16 commands gain 10-15%, each
/// sparing the client a wake, where 32 gain little and wait longer (a p99 worse
/// than Redis').
const HELD_BATCH: usize = 16;
/// How many of a batch's first commands are prefetched across connections.
const PREFETCH_ACROSS: usize = 4;
/// The longest request copied out of the shared read buffer to wait.
const COPIED_REQUEST: usize = 2 * 1024;
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
/// How long a TLS client may take to finish its handshake.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Task polls between looks at the reactor on a `--io per-thread` thread.
/// Tokio's default, 61, is about 30 one-command requests (two polls each):
/// the thread would serve 30 connections of the hundreds that are ready, and
/// wake their clients for that, where Redis serves all that are ready. It
/// looks anyway when it runs out of work.
const EVENT_INTERVAL: u32 = 1024;

/// Commands parsed ahead of running them. Run one at a time, each command of a
/// pipelined batch waits on its own cache misses in turn (its buckets, then
/// its entry). Parsed first, the store can start loading the buckets and then
/// the entries of the commands a few ahead while command `i` runs
/// ([`Store::prefetch_ahead`]), so the misses overlap.
#[derive(Default)]
#[doc(hidden)] // public for the in-process benchmark (src/bin/bench.rs)
pub struct Batch {
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
    pub fn parse(
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

    pub fn is_empty(&self) -> bool {
        self.cmds.is_empty()
    }

    pub fn len(&self) -> usize {
        self.cmds.len()
    }

    /// Command `i`, ready to run.
    pub fn command<'b>(&'b self, i: usize, buf: &'b [u8]) -> Args<'b> {
        let start = if i == 0 { 0 } else { self.cmds[i - 1].args_end };
        Args::new(buf, &self.args[start..self.cmds[i].args_end]).with_key_hash(self.cmds[i].key)
    }

    /// The read buffer position after command `i`.
    pub fn end(&self, i: usize) -> usize {
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
    pub fn prefetch(&self, i: usize, store: &Store) {
        store.prefetch_ahead(i, |j| self.ahead(j));
    }

    /// Starts loading the buckets of the first commands, for when they wait
    /// for the thread's other ready connections to read theirs.
    fn prefetch_across(&self, store: &Store) {
        for key in self.cmds.iter().take(PREFETCH_ACROSS).filter_map(|c| c.key) {
            store.prefetch(key);
        }
    }
}

pub async fn serve(listener: TcpListener, ctx: Arc<Context>) -> io::Result<()> {
    serve_with(listener, ctx, None).await
}

/// [`serve`], speaking TLS on this listener if `tls` is given.
pub async fn serve_with(
    listener: TcpListener,
    ctx: Arc<Context>,
    tls: Option<TlsAcceptor>,
) -> io::Result<()> {
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
        let (ctx, tls) = (ctx.clone(), tls.clone());
        tokio::spawn(async move {
            // Errors here are almost always the client going away.
            let _ = handle_connection(socket, &ctx, tls.as_ref()).await;
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
    serve_per_thread_with(vec![(listener, None)], ctx, threads)
}

/// [`serve_per_thread`] for several listeners, each speaking TLS if it
/// comes with an acceptor.
pub fn serve_per_thread_with(
    listeners: Vec<(std::net::TcpListener, Option<TlsAcceptor>)>,
    ctx: Arc<Context>,
    threads: usize,
) -> io::Result<()> {
    type Incoming = (std::net::TcpStream, Option<TlsAcceptor>);
    let mut workers = Vec::with_capacity(threads);
    for i in 0..threads.max(1) {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Incoming>();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .event_interval(EVENT_INTERVAL)
            .build()?;
        let ctx = ctx.clone();
        std::thread::Builder::new()
            .name(format!("tantalum-io-{i}"))
            .spawn(move || {
                runtime.block_on(async move {
                    while let Some((socket, tls)) = rx.recv().await {
                        // Registers the socket with this thread's reactor.
                        let Ok(socket) = TcpStream::from_std(socket) else {
                            continue;
                        };
                        let ctx = ctx.clone();
                        tokio::spawn(async move {
                            let _ = handle_connection(socket, &ctx, tls.as_ref()).await;
                        });
                    }
                })
            })?;
        workers.push(tx);
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let mut loops = Vec::new();
        for (listener, tls) in listeners {
            listener.set_nonblocking(true)?;
            let listener = TcpListener::from_std(listener)?;
            let (ctx, workers) = (ctx.clone(), workers.clone());
            loops.push(tokio::spawn(async move {
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
                    if workers[next].send((socket, tls.clone())).is_err() {
                        return Err(io::Error::other("an I/O thread exited"));
                    }
                    next = (next + 1) % workers.len();
                }
            }));
        }
        for accept_loop in loops {
            accept_loop.await.map_err(io::Error::other)??;
        }
        Ok(())
    })
}

/// The TLS setup for `--tls-port`, from PEM files: a certificate chain and
/// its private key. Clients aren't asked for certificates.
pub fn tls_acceptor(cert_file: &Path, key_file: &Path) -> io::Result<TlsAcceptor> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    let bad = |what: &str, file: &Path, e: &dyn std::fmt::Display| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{what} {}: {e}", file.display()),
        )
    };
    let certs = CertificateDer::pem_file_iter(cert_file)
        .and_then(Iterator::collect::<Result<Vec<_>, _>>)
        .map_err(|e| bad("can't read the certificate", cert_file, &e))?;
    if certs.is_empty() {
        return Err(bad("no certificate in", cert_file, &"empty"));
    }
    let key = PrivateKeyDer::from_pem_file(key_file)
        .map_err(|e| bad("can't read the private key", key_file, &e))?;
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(io::Error::other)?
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .map_err(|e| bad("the certificate doesn't go with the key", key_file, &e))?;
    Ok(TlsAcceptor::from(Arc::new(config)))
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
                    let (mailbox, bell) = Mailbox::new()?;
                    runtime.block_on(async move {
                        monoio::spawn(deliver_mail(
                            mailbox.clone(),
                            monoio::net::UnixStream::from_std(bell)?,
                        ));
                        let opts = monoio::net::ListenerOpts::new()
                            .reuse_port(true)
                            .backlog(1024);
                        let listener = monoio::net::TcpListener::bind_with_config(addr, &opts)?;
                        loop {
                            monoio::select! {
                                accepted = listener.accept() => match accepted {
                                    Ok((socket, _)) => {
                                        let (ctx, mailbox) = (ctx.clone(), mailbox.clone());
                                        monoio::spawn(async move {
                                            let _ = handle_uring(socket, &ctx, &mailbox).await;
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
#[doc(hidden)] // public for the in-process benchmark
pub fn run(
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

/// Wakes this monoio thread's blocked connections that other threads served
/// (see [`Mailbox`]), each time its bell rings.
async fn deliver_mail(mailbox: Arc<Mailbox>, mut bell: monoio::net::UnixStream) {
    use monoio::io::AsyncReadRent;
    let mut buf = vec![0u8; 64];
    loop {
        let (read, b) = bell.read(buf).await;
        buf = b;
        if !matches!(read, Ok(n) if n > 0) {
            return;
        }
        mailbox.deliver();
    }
}

/// For a subscribed monoio connection, instead of waiting idle: sends its
/// messages as they come until the client sends something. `false` if
/// it's over its output limit and must close.
async fn forward_messages_uring(
    socket: &mut monoio::net::TcpStream,
    subscriber: &Subscriber,
    mailbox: &Arc<Mailbox>,
) -> io::Result<bool> {
    let mut messages = Vec::new();
    loop {
        monoio::select! {
            ready = socket.readable(false) => {
                ready?;
                return Ok(true);
            }
            () = subscriber.ready(Some(mailbox)) => {}
        }
        if !subscriber.take(&mut messages) {
            return Ok(false);
        }
        match write_unless_closed(socket, messages, subscriber, mailbox).await? {
            Some(buf) => messages = buf,
            None => return Ok(false),
        }
        messages.clear();
    }
}

/// Writes `buf`, unless the subscriber goes over its output limit meanwhile
/// (then `None`): see `send_unless_closed`.
async fn write_unless_closed(
    socket: &mut monoio::net::TcpStream,
    buf: Vec<u8>,
    subscriber: &Subscriber,
    mailbox: &Arc<Mailbox>,
) -> io::Result<Option<Vec<u8>>> {
    use monoio::io::AsyncWriteRentExt;
    monoio::select! {
        (result, buf) = socket.write_all(buf) => result.map(|_| Some(buf)),
        () = subscriber.closed(Some(mailbox)) => Ok(None),
    }
}

/// [`wait_blocked`] for monoio, waking through this thread's `mailbox`.
async fn wait_blocked_uring(
    socket: &mut monoio::net::TcpStream,
    blocked: &Blocked,
    mut rbuf: Vec<u8>,
    mailbox: &Arc<Mailbox>,
    subscriber: Option<&Subscriber>,
) -> (io::Result<bool>, Vec<u8>) {
    use monoio::buf::IoBufMut;
    use monoio::io::AsyncReadRent;
    let mut served = std::pin::pin!(blocked.served(Some(mailbox)));
    let mut timeout = std::pin::pin!(async {
        match blocked.timeout() {
            Some(timeout) => monoio::time::sleep(timeout).await,
            None => std::future::pending().await,
        }
    });
    loop {
        // Readiness, not a read: a read cancelled in flight could lose data.
        let mut messages_came = false;
        monoio::select! {
            () = &mut served => return (Ok(true), rbuf),
            () = &mut timeout => return (Ok(true), rbuf),
            ready = socket.readable(false) => {
                if let Err(e) = ready {
                    return (Err(e), rbuf);
                }
            }
            () = messages_ready(subscriber, Some(mailbox)) => messages_came = true,
        }
        if let (true, Some(subscriber)) = (messages_came, subscriber) {
            let mut messages = Vec::new();
            if !subscriber.take(&mut messages) {
                return (Ok(false), rbuf);
            }
            match write_unless_closed(socket, messages, subscriber, mailbox).await {
                Ok(Some(_)) => {}
                Ok(None) => return (Ok(false), rbuf),
                Err(e) => return (Err(e), rbuf),
            }
            continue;
        }
        if rbuf.capacity() - rbuf.len() < URING_READ_BUFFER / 2 {
            rbuf.reserve(rbuf.capacity().max(URING_READ_BUFFER));
        }
        let filled = rbuf.len();
        let (result, slice) = socket.read(rbuf.slice_mut(filled..)).await;
        rbuf = slice.into_inner();
        match result {
            Ok(0) => return (Ok(false), rbuf),
            Ok(_) if rbuf.len() > MAX_QUERY_BUFFER => return (Ok(false), rbuf),
            Ok(_) => {}
            Err(e) => return (Err(e), rbuf),
        }
    }
}

/// Batches to go before trying [`read_more_now`] again, at most.
const MAX_READ_MORE_BACKOFF: u32 = 256;

/// Reads what the socket holds right now into `buf`'s spare room (one read,
/// so a client that floods it can't grow the buffer here), without
/// waiting. True if it got anything.
fn read_more_now(fd: std::os::fd::RawFd, buf: &mut Vec<u8>) -> bool {
    if buf.capacity() - buf.len() < 4096 {
        buf.reserve(URING_READ_BUFFER);
    }
    let spare = buf.capacity() - buf.len();
    // SAFETY: writes at most `spare` bytes into the vector's spare room.
    let n = unsafe {
        libc::recv(
            fd,
            buf.as_mut_ptr().add(buf.len()).cast(),
            spare,
            libc::MSG_DONTWAIT,
        )
    };
    if n <= 0 {
        return false;
    }
    // SAFETY: `recv` wrote `n` bytes there.
    unsafe { buf.set_len(buf.len() + n as usize) };
    true
}

/// The connection loop of `handle_connection`, on monoio's owned-buffer I/O.
/// Large values are copied into the reply buffer rather than written with a
/// separate `writev`. `mailbox` is its thread's, for blocking commands.
async fn handle_uring(
    mut socket: monoio::net::TcpStream,
    ctx: &Context,
    mailbox: &Arc<Mailbox>,
) -> io::Result<()> {
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
    session.socket = socket.as_raw_fd();
    let mut rbuf: Vec<u8> = Vec::with_capacity(URING_READ_BUFFER);
    let mut wbuf: Vec<u8> = Vec::new();
    let mut batch = Batch::default();
    // For reading more before a batch runs (see below).
    let (mut backoff, mut skip) = (0u32, 0u32);
    loop {
        if let Some(subscriber) = session.subscriber() {
            let subscriber = subscriber.clone();
            if !forward_messages_uring(&mut socket, &subscriber, mailbox).await? {
                return Ok(()); // over its output limit
            }
        }
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
        // A read here completes as soon as anything comes, where an epoll
        // thread reads what has gathered by the time it gets to the socket:
        // BullMQ's streams of pipelined commands came in about 2 at a time,
        // against 20, each batch costing a send that wakes the client. So
        // let the thread's other ready connections run, then take what
        // came meanwhile. For clients that send in bursts (or one at a
        // time) nothing does: after a miss, this backs off, trying again
        // only every 2, 4, ... 256 batches until it gets something.
        if skip == 0 {
            yield_to_others().await;
            if read_more_now(socket.as_raw_fd(), &mut rbuf) {
                backoff = 0;
            } else {
                backoff = (backoff * 2).clamp(1, MAX_READ_MORE_BACKOFF);
                skip = backoff;
            }
        } else {
            skip -= 1;
        }
        if wbuf.capacity() == 0 {
            wbuf.reserve(WRITE_RESERVE);
        }
        let mut now = store.now();
        let mut pos = 0;
        let mut close = false;
        // Whether a command blocked, during which more may have been read.
        let mut blocked_meanwhile = false;
        loop {
            // Prefetching in the database it has selected. (A `SELECT` in the
            // batch only makes the rest of its prefetching useless.)
            let store = ctx.db(&session);
            let parsed = batch.parse(&rbuf, pos, store);
            for i in 0..batch.len() {
                batch.prefetch(i, store);
                let args = batch.command(i, &rbuf);
                if !args.is_empty() {
                    match run(ctx, &mut session, now, &args, &mut wbuf) {
                        Some(Reply::Written) => {}
                        Some(Reply::Close) | None => close = true,
                        Some(Reply::Bulk(value)) => resp::write_bulk(&mut wbuf, value.as_bytes()),
                        Some(Reply::Block(blocked)) => {
                            if !wbuf.is_empty() {
                                wbuf = flush(&mut socket, wbuf).await?;
                            }
                            let subscriber = session.subscriber().cloned();
                            let (result, buf) = wait_blocked_uring(
                                &mut socket,
                                &blocked,
                                rbuf,
                                mailbox,
                                subscriber.as_deref(),
                            )
                            .await;
                            rbuf = buf;
                            if !result? {
                                return Ok(());
                            }
                            blocked.finish(&mut wbuf);
                            now = store.now();
                            blocked_meanwhile = true;
                        }
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
            if close || (batch.len() < MAX_BATCH && !std::mem::take(&mut blocked_meanwhile)) {
                break;
            }
        }
        ctx.publish(&mut session);
        rbuf.drain(..pos);
        // Messages that came meanwhile go with the replies.
        let subscriber = session.subscriber().cloned();
        if let Some(subscriber) = &subscriber
            && !subscriber.take(&mut wbuf)
        {
            close = true;
        }
        if !wbuf.is_empty() {
            match &subscriber {
                None => wbuf = flush(&mut socket, wbuf).await?,
                Some(subscriber) => {
                    match write_unless_closed(&mut socket, wbuf, subscriber, mailbox).await? {
                        Some(mut buf) => {
                            buf.clear();
                            wbuf = buf;
                        }
                        None => return Ok(()),
                    }
                }
            }
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

/// A connection's byte stream: plain TCP, or TLS over it.
trait Stream: AsyncRead + AsyncWrite + Unpin {
    /// Whether to wait for data before taking a buffer to read it into, so
    /// an idle connection holds none. Not for TLS: rustls may already hold
    /// decrypted bytes from an earlier read, so it reads right away.
    const WAIT_FOR_DATA: bool;

    fn poll_read_ready(&self, _: &mut std::task::Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    /// The socket, if bytes written to it go to the client as they are
    /// (not TLS): for a write that serves this connection while it's
    /// blocked to send the reply on. -1 for none.
    fn plain_socket(&self) -> std::os::fd::RawFd {
        -1
    }
}

impl Stream for TcpStream {
    const WAIT_FOR_DATA: bool = true;

    fn plain_socket(&self) -> std::os::fd::RawFd {
        self.as_raw_fd()
    }

    // Not `readable()`, which queues each wait on a locked list: through
    // `poll_read_ready` it takes the reader's own waker slot, as reads do.
    fn poll_read_ready(&self, cx: &mut std::task::Context<'_>) -> Poll<io::Result<()>> {
        TcpStream::poll_read_ready(self, cx)
    }
}

impl Stream for tokio_rustls::server::TlsStream<TcpStream> {
    const WAIT_FOR_DATA: bool = false;
}

/// Serves one connection, after a TLS handshake if `tls` is given.
pub async fn handle_connection(
    mut socket: TcpStream,
    ctx: &Context,
    tls: Option<&TlsAcceptor>,
) -> io::Result<()> {
    let Some(client) = ctx.connect(socket.as_raw_fd()) else {
        // (A TLS client couldn't read a plain-text error.)
        if tls.is_none() && !ctx.is_shutting_down() {
            let _ = socket.write_all(MAX_CLIENTS_ERROR).await;
        }
        return Ok(());
    };
    // Without this, Nagle + delayed ACK can stall small replies by ~40ms.
    socket.set_nodelay(true)?;
    set_keepalive(socket2::SockRef::from(&socket), ctx.settings.tcp_keepalive);
    match tls {
        None => serve_stream(socket, ctx, client).await,
        Some(acceptor) => {
            match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(socket)).await {
                Ok(Ok(stream)) => serve_stream(stream, ctx, client).await,
                // Not TLS, or too slow about it.
                Ok(Err(_)) | Err(_) => Ok(()),
            }
        }
    }
}

/// Lets the thread's other ready connections run first: woken at once, this
/// task goes to the back of the runtime's queue (and comes straight back if
/// it's alone), without waiting on the reactor as `yield_now` does.
async fn yield_to_others() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if std::mem::replace(&mut yielded, true) {
            return Poll::Ready(());
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await
}

/// Short batches' replies on this thread (`--io per-thread`), held until its
/// other ready connections have run, then sent together in a tight loop by
/// the thread's flusher task. A client thread that's busy when its next reply
/// comes needn't be woken, and a wake costs more than the send. With a task
/// switch between sends, replies came just far enough apart for client threads
/// to go back to sleep; and in the order requests came, a client's replies
/// alternated with those of its other threads. A client's connections were
/// mostly accepted together, so in socket order its replies come in one run:
/// 0.017 wakes per request against 0.22 (Redis: 0.08, io_uring: 0.03). The
/// order favours nobody: with the flusher's look at the reactor, every
/// connection is served once per cycle wherever it comes in it.
struct Outbox {
    /// The replies, back to back. Copied here, a reply leaves its connection's
    /// buffers free for the next connection, still in the CPU's cache.
    bytes: Vec<u8>,
    held: Vec<Held>,
    /// How many times it has been sent: a connection's reply is out once this
    /// has moved on from when it was held.
    flushes: u64,
    /// What sockets didn't take, for their connections' normal, waiting send
    /// (which reports errors too).
    stalled: Vec<(std::os::fd::RawFd, Vec<u8>)>,
    /// To tell connections that have something in `stalled`.
    wakers: std::collections::BTreeMap<std::os::fd::RawFd, std::task::Waker>,
    flusher_running: bool,
    flusher: Option<std::task::Waker>,
}

struct Held {
    socket: std::os::fd::RawFd,
    start: u32,
    len: u32,
}

impl Outbox {
    /// Sends what's held.
    fn flush(&mut self) {
        let Self {
            bytes,
            held,
            flushes,
            stalled,
            wakers,
            ..
        } = self;
        LAST_FLUSH.set(held.len());
        held.sort_unstable_by_key(|held| held.socket);
        for held in held.iter() {
            let start = held.start as usize;
            let reply = &bytes[start..start + held.len as usize];
            let sent = send_now(held.socket, reply);
            if sent < reply.len() {
                stalled.push((held.socket, reply[sent..].to_vec()));
                if let Some(waker) = wakers.get(&held.socket) {
                    waker.wake_by_ref();
                }
            }
        }
        held.clear();
        bytes.clear();
        *flushes += 1;
    }
}

thread_local! {
    static OUTBOX: std::cell::RefCell<Outbox> = const {
        std::cell::RefCell::new(Outbox {
            bytes: Vec::new(),
            held: Vec::new(),
            flushes: 0,
            stalled: Vec::new(),
            wakers: std::collections::BTreeMap::new(),
            flusher_running: false,
            flusher: None,
        })
    };
    /// How many replies the last flush sent: what tells a busy thread.
    static LAST_FLUSH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Buffers connections have finished with, for the next one on this
    /// thread: a connection holds none while idle, and taking one from here
    /// costs far less than allocating it, and it's still in the CPU's cache.
    static SPARE_BUFFERS: std::cell::RefCell<Vec<Vec<u8>>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// The thread's task that sends what's in its [`Outbox`]. It's woken by the
/// first reply held, so it runs after the connections that were ready then.
async fn run_flusher() {
    struct Stopped;
    impl Drop for Stopped {
        fn drop(&mut self) {
            let _ = OUTBOX.try_with(|outbox| {
                let mut outbox = outbox.borrow_mut();
                outbox.flusher_running = false;
                outbox.flusher = None;
            });
        }
    }
    let _stopped = Stopped;
    loop {
        let held = std::future::poll_fn(|cx| {
            OUTBOX.with_borrow_mut(|outbox| {
                if outbox.held.is_empty() {
                    outbox.flusher = Some(cx.waker().clone());
                    Poll::Pending
                } else {
                    Poll::Ready(outbox.held.len())
                }
            })
        })
        .await;
        // Before a big batch goes, what has come in meanwhile joins it: the
        // clients served last in the cycle before answer just too late for the
        // look that started this one, and would wait out all of it (p99 at
        // twice the median; with this, just above it).
        if held >= LOOK_FROM {
            let mut before = held;
            for _ in 0..LOOKS {
                tokio::task::yield_now().await;
                let now = OUTBOX.with_borrow(|outbox| outbox.held.len());
                if now == before {
                    break;
                }
                before = now;
            }
        }
        OUTBOX.with_borrow_mut(Outbox::flush);
    }
}

/// A connection's side of the [`Outbox`].
struct OutboxSlot {
    socket: std::os::fd::RawFd,
    /// `flushes` when its reply was held, until the connection hears more.
    held_at: Option<u64>,
    registered: bool,
}

impl OutboxSlot {
    fn new(socket: std::os::fd::RawFd) -> Self {
        Self {
            socket,
            held_at: None,
            registered: false,
        }
    }

    /// Holds a copy of `reply`, unless it's big or the outbox is full.
    async fn hold(&mut self, reply: &[u8]) -> bool {
        if !self.registered {
            let waker = std::future::poll_fn(|cx| Poll::Ready(cx.waker().clone())).await;
            OUTBOX.with_borrow_mut(|outbox| outbox.wakers.insert(self.socket, waker));
            self.registered = true;
        }
        let start_flusher = OUTBOX.with_borrow_mut(|outbox| {
            if reply.len() > OUTBOX_REPLY || outbox.bytes.len() + reply.len() > OUTBOX_BYTES {
                return None;
            }
            outbox.held.push(Held {
                socket: self.socket,
                start: outbox.bytes.len() as u32,
                len: reply.len() as u32,
            });
            outbox.bytes.extend_from_slice(reply);
            self.held_at = Some(outbox.flushes);
            if let Some(flusher) = outbox.flusher.take() {
                flusher.wake();
            }
            Some(!std::mem::replace(&mut outbox.flusher_running, true))
        });
        match start_flusher {
            Some(start) => {
                if start {
                    tokio::spawn(run_flusher());
                }
                true
            }
            None => false,
        }
    }

    /// Whether the socket didn't take all of the last reply.
    fn stalled(&self) -> bool {
        self.held_at.is_some()
            && OUTBOX.with_borrow(|outbox| {
                !outbox.stalled.is_empty()
                    && outbox
                        .stalled
                        .iter()
                        .any(|(socket, _)| *socket == self.socket)
            })
    }

    /// For when the connection has more to do: sees its last reply out, if
    /// it isn't yet. What the socket didn't take is returned, to send first.
    fn settle(&mut self) -> Option<Vec<u8>> {
        let held_at = self.held_at.take()?;
        OUTBOX.with_borrow_mut(|outbox| {
            if outbox.flushes == held_at {
                outbox.flush();
            }
            if outbox.stalled.is_empty() {
                return None;
            }
            let i = outbox
                .stalled
                .iter()
                .position(|(socket, _)| *socket == self.socket)?;
            Some(outbox.stalled.swap_remove(i).1)
        })
    }
}

impl Drop for OutboxSlot {
    fn drop(&mut self) {
        if !self.registered {
            return;
        }
        // Its socket closes next: nothing can go to it after this.
        OUTBOX.with_borrow_mut(|outbox| {
            if self.held_at == Some(outbox.flushes) {
                outbox.flush();
            }
            outbox.wakers.remove(&self.socket);
            outbox.stalled.retain(|(socket, _)| *socket != self.socket);
        });
    }
}

/// A buffer of `READ_RESERVE` (or `WRITE_RESERVE`) bytes.
fn take_buffer() -> Vec<u8> {
    SPARE_BUFFERS
        .with_borrow_mut(Vec::pop)
        .unwrap_or_else(|| Vec::with_capacity(READ_RESERVE))
}

/// Leaves a connection's buffer for the next one, if it's the usual size.
fn give_back(mut buf: Vec<u8>) {
    if buf.capacity() == READ_RESERVE {
        buf.clear();
        SPARE_BUFFERS.with_borrow_mut(|spare| {
            if spare.len() < MAX_SPARE_BUFFERS {
                spare.push(buf);
            }
        });
    }
}

/// Sends what of `reply` the socket takes without waiting.
fn send_now(socket: std::os::fd::RawFd, reply: &[u8]) -> usize {
    #[cfg(target_os = "linux")]
    let flags = libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL;
    #[cfg(not(target_os = "linux"))]
    let flags = libc::MSG_DONTWAIT;
    // SAFETY: `reply` is valid for its length, and the socket is open: its
    // connection's `InOutbox` marks it gone before the socket closes.
    let sent = unsafe { libc::send(socket, reply.as_ptr().cast(), reply.len(), flags) };
    usize::try_from(sent).unwrap_or(0)
}

/// Writes `buf` and flushes: rustls may hold encrypted bytes until then (for
/// plain TCP, flushing does nothing).
async fn send(socket: &mut impl Stream, buf: &[u8]) -> io::Result<()> {
    socket.write_all(buf).await?;
    socket.flush().await
}

async fn serve_stream(
    mut socket: impl Stream,
    ctx: &Context,
    mut client: crate::context::Client<'_>,
) -> io::Result<()> {
    let store = &*ctx.store;
    let mut session = client.session();
    session.socket = socket.plain_socket();
    // Vecs rather than a BytesMut: after a big request, a BytesMut keeps the
    // whole allocation but reports only its unused tail as capacity, so the
    // shrinking below (and the memory accounting) would miss it. Both are
    // empty, holding no memory, while a plain connection waits for a request.
    let mut rbuf: Vec<u8> = Vec::new();
    let mut wbuf: Vec<u8> = Vec::new();
    let mut batch = Batch::default();
    let mut slot = OutboxSlot::new(session.socket);
    // On a thread of its own (`--io per-thread`), a connection holds its reply
    // until the thread's other ready connections have read and run theirs,
    // much as Redis sends replies just before it waits for more. Sent together,
    // they wake each client fewer times: one wakeup per reply cost the kernel
    // more than reading and running the request. See `Outbox`.
    let defer_replies = tokio::runtime::Handle::current().runtime_flavor()
        == tokio::runtime::RuntimeFlavor::CurrentThread;

    loop {
        fn wait_for_data<S: Stream>(_: &S) -> bool {
            S::WAIT_FOR_DATA
        }
        let plain = wait_for_data(&socket);
        let n = if let Some(subscriber) = session.subscriber() {
            let subscriber = subscriber.clone();
            let read = read_subscribed(&mut socket, &subscriber, plain, &mut rbuf).await?;
            client.set_buffer_bytes(rbuf.capacity() + wbuf.capacity());
            match read {
                Some(n) => n,
                None => return Ok(()), // over its output limit
            }
        } else {
            if plain {
                // Wait for data before taking a buffer to read it into.
                let ready = std::future::poll_fn(|cx| {
                    if slot.stalled() {
                        return Poll::Ready(Ok(()));
                    }
                    socket.poll_read_ready(cx)
                });
                match ctx.settings.timeout {
                    None => ready.await?,
                    Some(timeout) => match tokio::time::timeout(timeout, ready).await {
                        Ok(ready) => ready?,
                        Err(_) => return Ok(()),
                    },
                }
                if let Some(rest) = slot.settle() {
                    send(&mut socket, &rest).await?;
                    continue;
                }
            }
            if rbuf.capacity() == 0 {
                rbuf = take_buffer();
            } else if rbuf.capacity() - rbuf.len() < READ_RESERVE / 4 {
                rbuf.reserve(READ_RESERVE);
            }
            client.set_buffer_bytes(rbuf.capacity() + wbuf.capacity());
            // For plain TCP, data is ready, so this reads at once. Through
            // `read_buf` (unlike `try_read_buf`), a short read tells tokio the
            // socket is drained, so the next wait doesn't first try a read that
            // finds nothing: that cost an extra syscall per request.
            let read = socket.read_buf(&mut rbuf);
            match ctx.settings.timeout {
                Some(timeout) if !plain => match tokio::time::timeout(timeout, read).await {
                    Ok(n) => n?,
                    Err(_) => return Ok(()),
                },
                _ => read.await?,
            }
        };
        if n == 0 {
            return Ok(());
        }
        // One clock read per batch, like Redis' cached time per event loop tick.
        let mut now = store.now();
        let mut pos = 0;
        let mut ran = 0usize;
        // On a busy thread, all that are ready read first and start loading
        // their keys' buckets, then they run. The lookups saved about pay for
        // the second poll, so throughput stays where it was, but p99 falls
        // 14-22% (a thread with few connections ready pays nothing for this).
        let mut read_first = defer_replies && LAST_FLUSH.get() >= LOOK_FROM;
        let mut close = false;
        // Whether a command blocked, during which more may have been read.
        let mut blocked_meanwhile = false;
        loop {
            // Prefetching in the database it has selected. (A `SELECT` in the
            // batch only makes the rest of its prefetching useless.)
            let store = ctx.db(&session);
            let parsed = batch.parse(&rbuf, pos, store);
            if std::mem::take(&mut read_first)
                && parsed.is_ok()
                && !batch.is_empty()
                && batch.len() <= HELD_BATCH
            {
                batch.prefetch_across(store);
                // The shared buffer goes on to the next connection, for its
                // read to find in the CPU's cache.
                if rbuf.len() <= COPIED_REQUEST {
                    let request = rbuf.as_slice().to_vec();
                    give_back(std::mem::replace(&mut rbuf, request));
                }
                yield_to_others().await;
                now = store.now();
            }
            if wbuf.capacity() == 0 {
                wbuf = take_buffer();
            }
            for i in 0..batch.len() {
                batch.prefetch(i, store);
                let args = batch.command(i, &rbuf);
                if !args.is_empty() {
                    ran += 1;
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
                        Some(Reply::Block(blocked)) => {
                            if !wbuf.is_empty() {
                                send(&mut socket, &wbuf).await?;
                                wbuf.clear();
                            }
                            let subscriber = session.subscriber().cloned();
                            if !wait_blocked(
                                &mut socket,
                                &blocked,
                                &mut rbuf,
                                subscriber.as_deref(),
                            )
                            .await?
                            {
                                return Ok(());
                            }
                            blocked.finish(&mut wbuf);
                            now = store.now();
                            blocked_meanwhile = true;
                        }
                    }
                }
                pos = batch.end(i);
                if wbuf.len() >= FLUSH_THRESHOLD {
                    send(&mut socket, &wbuf).await?;
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
                send(&mut socket, &wbuf).await?;
                return Ok(());
            }
            if close || (batch.len() < MAX_BATCH && !std::mem::take(&mut blocked_meanwhile)) {
                break;
            }
        }
        ctx.publish(&mut session);
        rbuf.drain(..pos);
        if defer_replies && !wbuf.is_empty() && ran <= HELD_BATCH {
            // (A connection closing waits to see its reply out.)
            if !close
                && session.socket >= 0
                && session.subscriber().is_none()
                && slot.hold(&wbuf).await
            {
                wbuf.clear();
            } else if ran == 1 {
                yield_to_others().await;
            }
        }
        // Messages that came meanwhile go with the replies.
        let subscriber = session.subscriber().cloned();
        if let Some(subscriber) = &subscriber
            && !subscriber.take(&mut wbuf)
        {
            close = true;
        }
        if !wbuf.is_empty() {
            if !send_unless_closed(&mut socket, &wbuf, subscriber.as_deref()).await? {
                return Ok(());
            }
            wbuf.clear();
        }
        if close {
            return Ok(());
        }
        if rbuf.len() > MAX_QUERY_BUFFER {
            resp::write_error(&mut wbuf, b"ERR query buffer limit exceeded");
            send(&mut socket, &wbuf).await?;
            return Ok(());
        }
        if rbuf.is_empty() {
            // Idle until the next request: hold no buffers.
            give_back(std::mem::take(&mut rbuf));
            give_back(std::mem::take(&mut wbuf));
            client.set_buffer_bytes(0);
        } else if wbuf.capacity() > SHRINK_ABOVE {
            wbuf = Vec::new();
        }
    }
}

/// Waits while a command is blocked, until it's served or times out
/// (`true`), or the client goes away (`false`). What the client sends
/// meanwhile is read into `rbuf`, to run after; a subscribed client's
/// messages are sent meanwhile. Blocked clients aren't subject to the idle
/// `timeout`, as in Redis.
async fn wait_blocked(
    socket: &mut impl Stream,
    blocked: &Blocked,
    rbuf: &mut Vec<u8>,
    subscriber: Option<&Subscriber>,
) -> io::Result<bool> {
    let mut served = std::pin::pin!(blocked.served(None));
    let mut timeout = std::pin::pin!(async {
        match blocked.timeout() {
            Some(timeout) => tokio::time::sleep(timeout).await,
            None => std::future::pending().await,
        }
    });
    let mut messages = Vec::new();
    loop {
        if rbuf.capacity() - rbuf.len() < READ_RESERVE / 4 {
            rbuf.reserve(READ_RESERVE);
        }
        tokio::select! {
            () = &mut served => return Ok(true),
            () = &mut timeout => return Ok(true),
            read = socket.read_buf(rbuf) => {
                if read? == 0 || rbuf.len() > MAX_QUERY_BUFFER {
                    return Ok(false);
                }
            }
            () = messages_ready(subscriber, None) => {
                if !subscriber.is_some_and(|s| s.take(&mut messages))
                    || !send_unless_closed(socket, &messages, subscriber).await?
                {
                    return Ok(false);
                }
                messages.clear();
            }
        }
    }
}

/// [`send`], given up (`false`) if the connection is a subscriber that goes
/// over its output limit meanwhile: a client that doesn't read would hold
/// the write, and its subscriptions, forever.
async fn send_unless_closed(
    socket: &mut impl Stream,
    buf: &[u8],
    subscriber: Option<&Subscriber>,
) -> io::Result<bool> {
    let Some(subscriber) = subscriber else {
        send(socket, buf).await?;
        return Ok(true);
    };
    tokio::select! {
        sent = send(socket, buf) => sent.map(|()| true),
        () = subscriber.closed(None) => Ok(false),
    }
}

/// Resolves when a subscriber has messages to send; never without one.
async fn messages_ready(subscriber: Option<&Subscriber>, home: Option<&Arc<Mailbox>>) {
    match subscriber {
        Some(subscriber) => subscriber.ready(home).await,
        None => std::future::pending().await,
    }
}

/// For a subscribed connection, instead of waiting idle: sends its messages
/// as they come until the client sends something, then reads it. `None` if
/// it's over its output limit and must close. Subscribed clients aren't
/// subject to the idle `timeout`, as in Redis.
async fn read_subscribed(
    socket: &mut impl Stream,
    subscriber: &Subscriber,
    plain: bool,
    rbuf: &mut Vec<u8>,
) -> io::Result<Option<usize>> {
    let mut messages = Vec::new();
    loop {
        if plain {
            tokio::select! {
                ready = std::future::poll_fn(|cx| socket.poll_read_ready(cx)) => {
                    ready?;
                    // Readiness can be stale (on the shared runtime, another
                    // thread's I/O event can keep it set after a read drained
                    // the socket), and a client that only listens sends
                    // nothing more: try the read once, and if there's nothing
                    // after all, go back to waiting for both.
                    if rbuf.capacity() - rbuf.len() < READ_RESERVE / 4 {
                        rbuf.reserve(READ_RESERVE);
                    }
                    let mut read = std::pin::pin!(socket.read_buf(rbuf));
                    let attempt = std::future::poll_fn(|cx| Poll::Ready(read.as_mut().poll(cx)));
                    match attempt.await {
                        Poll::Ready(n) => return Ok(Some(n?)),
                        Poll::Pending => continue,
                    }
                }
                () = subscriber.ready(None) => {}
            }
        } else {
            // TLS: rustls may hold decrypted bytes already, so read.
            if rbuf.capacity() - rbuf.len() < READ_RESERVE / 4 {
                rbuf.reserve(READ_RESERVE);
            }
            tokio::select! {
                read = socket.read_buf(rbuf) => return Ok(Some(read?)),
                () = subscriber.ready(None) => {}
            }
        }
        if !subscriber.take(&mut messages)
            || !send_unless_closed(socket, &messages, Some(subscriber)).await?
        {
            return Ok(None);
        }
        messages.clear();
    }
}

async fn write_all_vectored(
    socket: &mut impl Stream,
    mut parts: &mut [IoSlice<'_>],
) -> io::Result<()> {
    while !parts.is_empty() {
        let n = socket.write_vectored(parts).await?;
        if n == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        IoSlice::advance_slices(&mut parts, n);
    }
    socket.flush().await
}
