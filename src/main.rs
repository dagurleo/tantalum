use std::alloc::{GlobalAlloc, Layout};
use std::ffi::c_void;
use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tantalum::aof;
use tantalum::cmd;
use tantalum::context::{Context, Settings};
use tantalum::server;
use tantalum::snapshot::{self, Snapshots};
use tantalum::store::{self, EvictionPolicy};
use tokio::net::TcpListener;

#[global_allocator]
static GLOBAL: Allocator = Allocator;

unsafe extern "C" {
    fn mi_collect(force: bool);
    fn mi_malloc(size: usize) -> *mut c_void;
    fn mi_zalloc(size: usize) -> *mut c_void;
}

/// mimalloc, asked for an alignment only when one is needed. `MiMalloc` always asks,
/// and when the size's bin has no block ready, unless its blocks are a power of two,
/// that adds `align - 1` to the size: 320 bytes aligned to 8 take a 384-byte block,
/// and the bin stays empty, so the next asks the same way, on a slower path. Every
/// block is aligned to a word anyway.
struct Allocator;

// SAFETY: as `MiMalloc`, whose functions these are, with a plain allocation only where
// its block is aligned enough; `mi_free` and `mi_realloc_aligned` take either.
unsafe impl GlobalAlloc for Allocator {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.align() <= align_of::<usize>() {
            unsafe { mi_malloc(layout.size()).cast() }
        } else {
            unsafe { mimalloc::MiMalloc.alloc(layout) }
        }
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if layout.align() <= align_of::<usize>() {
            unsafe { mi_zalloc(layout.size()).cast() }
        } else {
            unsafe { mimalloc::MiMalloc.alloc_zeroed(layout) }
        }
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(ptr, layout) }
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        unsafe { mimalloc::MiMalloc.realloc(ptr, layout, new_size) }
    }
}

/// Hands back to the OS what the allocator has had free for a while. It would do
/// so only when something next asks it for whole pages, which a steady server may
/// never do: a big set's old buffer stayed resident from the moment it doubled,
/// for 66% more memory than the set needed. Asking from this thread costs commands
/// nothing, and memory freed and used again within the allocator's delay (a second
/// by default) stays resident, so steady churn doesn't fault it back in.
fn spawn_memory_returner() -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("tantalum-purge".into())
        .spawn(|| {
            loop {
                std::thread::sleep(Duration::from_secs(1));
                // SAFETY: takes no pointers, and may be called from any thread.
                unsafe { mi_collect(false) };
            }
        })
}

const USAGE: &str = "\
Usage: tantalum [OPTIONS]

Options:
  --bind <ADDR>     Address to listen on [default: 127.0.0.1]
  --port <PORT>     Port to listen on; 0 for none, with --tls-port
                    [default: 6379]
  --threads <N>     Worker threads [default: number of CPUs]
  --io <MODE>       uring: an event loop per thread on io_uring, each
                    connection pinned to one; per-thread: the same on
                    epoll/kqueue; shared: one work-stealing runtime, which
                    balances a few very busy connections better
                    [default: uring where the kernel and container allow it,
                    otherwise per-thread]
  --shards <N>      Store shards, rounded up to a power of two
                    [default: 256, or 1 per 4 MB of --maxmemory between 16 and 256]
  --databases <N>   How many numbered databases there are (SELECT) [default: 16]
  --maxmemory <SIZE>
                    Memory limit, e.g. 100mb or 2gb; 0 is unlimited [default: 0]
  --maxmemory-policy <POLICY>
                    noeviction, allkeys-lru or volatile-lru [default: noeviction]
  --requirepass <PASSWORD>
                    Clients must AUTH with this password first. The
                    TANTALUM_REQUIREPASS environment variable sets it too and
                    keeps it out of the process list [default: none]
  --maxclients <N>  Connections beyond this are refused [default: 10000]
  --timeout <SECS>  Close connections idle this long; 0 never [default: 0]
  --tcp-keepalive <SECS>
                    Probe idle connections after this long to detect dead
                    clients; 0 turns it off [default: 300]
  --tls-port <PORT> Also listen on this port for TLS connections, with the
                    certificate chain and key from these PEM files:
  --tls-cert-file <FILE>
  --tls-key-file <FILE>
  --snapshot <FILE> Keep the data across restarts: load FILE at startup if it
                    exists, save to it periodically and at shutdown
                    [default: none, nothing is saved]
  --snapshot-interval <SECS>
                    Save this often if anything changed; 0 saves only at
                    shutdown and on SAVE or BGSAVE [default: 60]
  --aof <DIR>       Log every write to files in DIR, synced every second,
                    and load from them at startup (a snapshot loaded only
                    when DIR has no log yet) [default: none, no log]
  --aof-fsync <everysec|no>
                    Sync the log every second, or when the system does
                    [default: everysec]
  --aof-rewrite-percentage <N>
  --aof-rewrite-min-size <SIZE>
                    Rewrite the log once it's N% of its base and at least
                    SIZE [defaults: 100, 64mb]
  --native-scripts <yes|no>
                    Run BullMQ's hottest scripts natively, with the same
                    results as their Lua [default: yes]
  -h, --help        Print this help

On SIGTERM or SIGINT, Tantalum stops accepting, lets each connection finish
the commands it has read, waits until they've closed (at most 10 s), syncs
the log if --aof is set, saves a snapshot if --snapshot is set, and exits.
A second signal exits at once.";

/// How long a graceful shutdown waits for connections to close.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

struct Config {
    bind: String,
    port: u16,
    threads: usize,
    /// `None`: io_uring where available, otherwise per-thread.
    io: Option<Io>,
    store: store::Config,
    settings: Settings,
    snapshot: Option<PathBuf>,
    snapshot_interval: Option<Duration>,
    aof: Option<aof::Options>,
    tls_port: Option<u16>,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Io {
    Shared,
    PerThread,
    Uring,
}

/// Parses sizes the way Redis does: `1k` is 1000 bytes, `1kb` is 1024.
fn parse_size(s: &str) -> Option<usize> {
    let s = s.to_ascii_lowercase();
    let digits = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let multiplier = match &s[digits..] {
        "" | "b" => 1,
        "k" => 1000,
        "kb" => 1 << 10,
        "m" => 1000 * 1000,
        "mb" => 1 << 20,
        "g" => 1000 * 1000 * 1000,
        "gb" => 1 << 30,
        _ => return None,
    };
    s[..digits].parse::<usize>().ok()?.checked_mul(multiplier)
}

/// Whole seconds, where 0 means off.
fn parse_secs(s: &str) -> Option<Option<Duration>> {
    let secs: u64 = s.parse().ok()?;
    Some((secs > 0).then(|| Duration::from_secs(secs)))
}

/// Raises the open-file limit to fit `maxclients` connections (plus a few
/// files of our own), as Redis does, and warns if it can't.
#[cfg(unix)]
fn raise_open_file_limit(maxclients: usize) {
    let wanted = maxclients as libc::rlim_t + 32;
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit and setrlimit only read and write `limit`.
    unsafe {
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) != 0 || limit.rlim_cur >= wanted {
            return;
        }
        let raised = libc::rlimit {
            rlim_cur: wanted.min(limit.rlim_max),
            ..limit
        };
        if libc::setrlimit(libc::RLIMIT_NOFILE, &raised) == 0 {
            limit = raised;
        }
    }
    if limit.rlim_cur < wanted {
        eprintln!(
            "warning: the open-file limit is {}, so only about {} of --maxclients {maxclients} \
             can connect",
            limit.rlim_cur,
            limit.rlim_cur.saturating_sub(32)
        );
    }
}

fn parse_args() -> Result<Config, String> {
    let mut config = Config {
        bind: "127.0.0.1".into(),
        port: 6379,
        threads: std::thread::available_parallelism().map_or(1, |n| n.get()),
        io: None,
        store: store::Config::default(),
        settings: Settings {
            password: std::env::var("TANTALUM_REQUIREPASS")
                .ok()
                .filter(|p| !p.is_empty())
                .map(String::into_bytes),
            ..Settings::default()
        },
        snapshot: None,
        snapshot_interval: Some(Duration::from_secs(60)),
        aof: None,
        tls_port: None,
        tls_cert: None,
        tls_key: None,
    };
    let mut aof_options = aof::Options {
        dir: PathBuf::new(),
        fsync: aof::Fsync::EverySec,
        rewrite_percentage: 100,
        rewrite_min_size: 64 << 20,
    };
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        if flag == "-h" || flag == "--help" {
            println!("{USAGE}");
            std::process::exit(0);
        }
        let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
        let bad = |_: ()| format!("invalid value for {flag}: {value}");
        match flag.as_str() {
            "--bind" => config.bind = value.clone(),
            "--port" => config.port = value.parse().map_err(|_| bad(()))?,
            "--threads" => config.threads = value.parse::<usize>().map_err(|_| bad(()))?.max(1),
            "--shards" => config.store.shards = value.parse().map_err(|_| bad(()))?,
            "--databases" => {
                config.store.databases = value
                    .parse::<usize>()
                    .ok()
                    .filter(|&n| n >= 1)
                    .ok_or_else(|| bad(()))?;
            }
            "--io" => {
                config.io = Some(match value.as_str() {
                    "shared" => Io::Shared,
                    "per-thread" => Io::PerThread,
                    "uring" => Io::Uring,
                    _ => return Err(bad(())),
                })
            }
            "--maxmemory" => config.store.maxmemory = parse_size(&value).ok_or_else(|| bad(()))?,
            "--maxmemory-policy" => {
                config.store.policy = EvictionPolicy::parse(&value).ok_or_else(|| bad(()))?
            }
            "--requirepass" => {
                config.settings.password = Some(value.into_bytes()).filter(|p| !p.is_empty())
            }
            "--maxclients" => {
                config.settings.maxclients = value.parse().map_err(|_| bad(()))?;
            }
            "--timeout" => config.settings.timeout = parse_secs(&value).ok_or_else(|| bad(()))?,
            "--tcp-keepalive" => {
                config.settings.tcp_keepalive = parse_secs(&value).ok_or_else(|| bad(()))?;
            }
            "--snapshot" => config.snapshot = Some(PathBuf::from(value)),
            "--tls-port" => config.tls_port = Some(value.parse().map_err(|_| bad(()))?),
            "--tls-cert-file" => config.tls_cert = Some(PathBuf::from(value)),
            "--tls-key-file" => config.tls_key = Some(PathBuf::from(value)),
            "--snapshot-interval" => {
                config.snapshot_interval = parse_secs(&value).ok_or_else(|| bad(()))?;
            }
            "--aof" => aof_options.dir = PathBuf::from(&value),
            "--aof-fsync" => {
                aof_options.fsync = match value.as_str() {
                    "everysec" => aof::Fsync::EverySec,
                    "no" => aof::Fsync::No,
                    _ => return Err(bad(())),
                }
            }
            "--aof-rewrite-percentage" => {
                aof_options.rewrite_percentage = value.parse().map_err(|_| bad(()))?;
            }
            "--aof-rewrite-min-size" => {
                aof_options.rewrite_min_size = parse_size(&value).ok_or_else(|| bad(()))? as u64;
            }
            "--native-scripts" => {
                config.settings.native_scripts = match value.as_str() {
                    "yes" => true,
                    "no" => false,
                    _ => return Err(bad(())),
                }
            }
            _ => return Err(format!("unknown option: {flag}")),
        }
    }
    if !aof_options.dir.as_os_str().is_empty() {
        // The log's commands run again at the times they ran at.
        config.store.unix_clock = true;
        config.aof = Some(aof_options);
    }
    Ok(config)
}

/// Transparent huge pages let the kernel back a barely used 2 MB region with a
/// whole 2 MB page, which inflated RSS by up to 3x in tests. Redis turns them
/// off for itself too (`disable-thp`).
#[cfg(target_os = "linux")]
fn disable_transparent_huge_pages() {
    // SAFETY: PR_SET_THP_DISABLE only sets a flag on this process.
    if unsafe { libc::prctl(libc::PR_SET_THP_DISABLE, 1, 0, 0, 0) } != 0 {
        eprintln!("warning: could not disable transparent huge pages");
    }
}

fn main() -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    disable_transparent_huge_pages();
    let config = parse_args().unwrap_or_else(|e| {
        eprintln!("error: {e}\n\n{USAGE}");
        std::process::exit(2);
    });
    // io_uring beat the epoll loop at every thread count on a 32-core EPYC
    // (bench/aws): +17-25% without pipelining, +12-21% with, lower p99.
    // Where it's unavailable, monoio's own fallback (epoll/kqueue) is slower
    // than the tokio loop, so use that instead.
    let uring = monoio::utils::detect_uring();
    let io = match config.io {
        None if uring => Io::Uring,
        None => Io::PerThread,
        Some(Io::Uring) if !uring => {
            eprintln!("io_uring is unavailable here (kernel or seccomp): using per-thread I/O");
            Io::PerThread
        }
        Some(io) => io,
    };
    #[cfg(unix)]
    raise_open_file_limit(config.settings.maxclients);
    let store = Arc::new(store::Store::with_config(config.store.clone()));
    let mut ctx = Context::new(store.clone(), config.settings.clone());
    let logged = config
        .aof
        .as_ref()
        .is_some_and(|options| aof::exists(&options.dir));
    if let Some(path) = &config.snapshot {
        if logged {
            eprintln!("loading from the append-only log, not {}", path.display());
        } else {
            load_snapshot(&store, path);
        }
        let snapshots = Snapshots::new(store.clone(), path.clone(), config.snapshot_interval);
        snapshots.spawn_periodic()?;
        ctx = ctx.with_snapshots(snapshots);
    }
    if let Some(options) = &config.aof {
        start_aof(&ctx, options.clone());
    }
    let ctx = Arc::new(ctx);
    // After the log's commands ran again: they may still use keys that have
    // expired since.
    let _expirer = cmd::spawn_expirer(&ctx, Duration::from_millis(100));
    let _memory_returner = spawn_memory_returner()?;
    server::handle_signals(ctx.clone())?;
    // TLS listens on its own port, on tokio loops even with io_uring (monoio
    // has no TLS here).
    let tls = match config.tls_port {
        None => None,
        Some(port) => {
            let (Some(cert), Some(key)) = (&config.tls_cert, &config.tls_key) else {
                eprintln!("error: --tls-port needs --tls-cert-file and --tls-key-file");
                std::process::exit(2);
            };
            let acceptor = server::tls_acceptor(cert, key).unwrap_or_else(|e| {
                eprintln!("error: {e}");
                std::process::exit(2);
            });
            Some((
                std::net::TcpListener::bind((config.bind.as_str(), port))?,
                acceptor,
            ))
        }
    };
    if config.port == 0 && tls.is_none() {
        eprintln!("error: nothing to listen on: --port 0 needs --tls-port");
        std::process::exit(2);
    }
    let tls_addr = tls.as_ref().map(|(l, _)| l.local_addr()).transpose()?;
    let banner = |plain: Option<std::net::SocketAddr>| {
        let limit = match config.store.maxmemory {
            0 => "no memory limit".to_string(),
            n => format!("maxmemory {n} bytes, {}", config.store.policy.name()),
        };
        let io = match io {
            Io::Shared => "shared",
            Io::PerThread => "per-thread",
            Io::Uring => "io_uring",
        };
        let addrs = match (plain, tls_addr) {
            (Some(plain), Some(tls)) => format!("{plain} and {tls} (TLS)"),
            (Some(plain), None) => plain.to_string(),
            (None, Some(tls)) => format!("{tls} (TLS)"),
            (None, None) => unreachable!("checked above"),
        };
        eprintln!(
            "tantalum {} listening on {addrs} ({} threads, {io} I/O, {limit})",
            env!("CARGO_PKG_VERSION"),
            config.threads,
        );
        let exposed = plain.iter().chain(&tls_addr).any(|a| !a.ip().is_loopback());
        if config.settings.password.is_none() && exposed {
            eprintln!(
                "warning: no password set (--requirepass): anyone who can reach {addrs} can read and write"
            );
        }
    };
    // Each returns once shutdown starts; connections are still finishing.
    let mut _shared_runtime = None;
    match io {
        Io::PerThread => {
            let mut listeners = Vec::new();
            let mut plain = None;
            if config.port != 0 {
                let listener = std::net::TcpListener::bind((config.bind.as_str(), config.port))?;
                plain = Some(listener.local_addr()?);
                listeners.push((listener, None));
            }
            banner(plain);
            listeners.extend(tls.map(|(listener, acceptor)| (listener, Some(acceptor))));
            server::serve_per_thread_with(listeners, ctx.clone(), config.threads)?;
        }
        Io::Uring => {
            let tls_loops = tls
                .map(|(listener, acceptor)| {
                    let (ctx, threads) = (ctx.clone(), config.threads);
                    std::thread::Builder::new()
                        .name("tantalum-tls".into())
                        .spawn(move || {
                            server::serve_per_thread_with(
                                vec![(listener, Some(acceptor))],
                                ctx,
                                threads,
                            )
                        })
                })
                .transpose()?;
            if config.port != 0 {
                let addr = (config.bind.as_str(), config.port)
                    .to_socket_addrs()?
                    .next()
                    .ok_or_else(|| std::io::Error::other("no address to bind"))?;
                banner(Some(addr));
                server::serve_uring(addr, ctx.clone(), config.threads)?;
            } else if let Some(tls_loops) = tls_loops {
                banner(None);
                tls_loops
                    .join()
                    .map_err(|_| std::io::Error::other("the TLS thread panicked"))??;
            }
        }
        Io::Shared => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(config.threads)
                .enable_all()
                .build()?;
            runtime.block_on(async {
                let mut accept_loops = Vec::new();
                let mut plain = None;
                if config.port != 0 {
                    let listener = TcpListener::bind((config.bind.as_str(), config.port)).await?;
                    plain = Some(listener.local_addr()?);
                    accept_loops.push(tokio::spawn(server::serve_with(
                        listener,
                        ctx.clone(),
                        None,
                    )));
                }
                banner(plain);
                if let Some((listener, acceptor)) = tls {
                    listener.set_nonblocking(true)?;
                    let listener = TcpListener::from_std(listener)?;
                    accept_loops.push(tokio::spawn(server::serve_with(
                        listener,
                        ctx.clone(),
                        Some(acceptor),
                    )));
                }
                for accept_loop in accept_loops {
                    accept_loop.await.map_err(std::io::Error::other)??;
                }
                Ok::<_, std::io::Error>(())
            })?;
            // Its workers run the connections still open.
            _shared_runtime = Some(runtime);
        }
    }
    match server::drain(&ctx, SHUTDOWN_TIMEOUT) {
        0 => eprintln!("all connections closed"),
        open => eprintln!("{open} connections still open after {SHUTDOWN_TIMEOUT:?}"),
    }
    if let Some(aof) = ctx.aof() {
        aof.stop();
        match aof.flush() {
            Ok(()) => eprintln!("synced the append-only log"),
            Err(e) => eprintln!("error: can't sync the append-only log: {e}"),
        }
    }
    if let Some(snapshots) = &ctx.snapshots
        && let Ok(keys) = snapshots.save()
    {
        eprintln!("saved {keys} keys to {}", snapshots.path.display());
    }
    eprintln!("exiting");
    Ok(())
}

/// Opens the append-only log: loads it (or starts one), then logs writes.
/// A log that can't be read stops the server, as a damaged snapshot does.
fn start_aof(ctx: &Context, options: aof::Options) {
    let started = Instant::now();
    let dir = options.dir.clone();
    match aof::start(ctx, options) {
        Ok((log, done)) => {
            let mut line = if done.created {
                format!("started an append-only log in {}", dir.display())
            } else {
                format!(
                    "loaded {} keys and ran {} logged commands from {} in {} ms",
                    done.base_keys.unwrap_or(0),
                    done.commands,
                    dir.display(),
                    started.elapsed().as_millis()
                )
            };
            if done.truncated > 0 {
                line += &format!(
                    "; dropped {} bytes the server stopped in the middle of writing",
                    done.truncated
                );
            }
            if done.failed > 0 {
                line += &format!("; {} of them failed (see above)", done.failed);
            }
            eprintln!("{line}");
            ctx.set_aof(log);
        }
        Err(e) => {
            eprintln!(
                "error: can't load the append-only log in {}: {e}. Move it away to start without it.",
                dir.display()
            );
            std::process::exit(1);
        }
    }
}

/// Loads the snapshot at `path`, if there is one. A damaged one stops the
/// server, as in Redis: starting empty would overwrite it at the next save.
fn load_snapshot(store: &Arc<store::Store>, path: &std::path::Path) {
    let started = Instant::now();
    match snapshot::load(store, path) {
        Ok(None) => eprintln!("no snapshot at {} yet: starting empty", path.display()),
        Ok(Some(loaded)) => {
            let mut line = format!(
                "loaded {} keys from {} in {} ms",
                loaded.keys,
                path.display(),
                started.elapsed().as_millis()
            );
            if loaded.expired > 0 {
                line += &format!("; {} had expired", loaded.expired);
            }
            if loaded.dropped > 0 {
                line += &format!(
                    "; {} didn't fit in --maxmemory and were dropped",
                    loaded.dropped
                );
            }
            eprintln!("{line}");
        }
        Err(e) => {
            eprintln!(
                "error: can't load the snapshot {}: {e}. Move it away to start without it.",
                path.display()
            );
            std::process::exit(1);
        }
    }
}
