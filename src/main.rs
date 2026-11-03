use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use tantalum::context::{Context, Settings};
use tantalum::server;
use tantalum::store::{self, EvictionPolicy};
use tokio::net::TcpListener;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const USAGE: &str = "\
Usage: tantalum [OPTIONS]

Options:
  --bind <ADDR>     Address to listen on [default: 127.0.0.1]
  --port <PORT>     Port to listen on [default: 6379]
  --threads <N>     Worker threads [default: number of CPUs]
  --io <MODE>       uring: an event loop per thread on io_uring, each
                    connection pinned to one; per-thread: the same on
                    epoll/kqueue; shared: one work-stealing runtime, which
                    balances a few very busy connections better
                    [default: uring where the kernel and container allow it,
                    otherwise per-thread]
  --shards <N>      Store shards, rounded up to a power of two
                    [default: 256, or 1 per 4 MB of --maxmemory between 16 and 256]
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
  -h, --help        Print this help

On SIGTERM or SIGINT, Tantalum stops accepting, lets each connection finish
the commands it has read, and exits once they've closed (at most 10 s). A
second signal exits at once.";

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
            _ => return Err(format!("unknown option: {flag}")),
        }
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
    let _expirer = store::spawn_expirer(&store, Duration::from_millis(100));
    let ctx = Arc::new(Context::new(store, config.settings.clone()));
    server::handle_signals(ctx.clone())?;
    let banner = |addr: std::net::SocketAddr| {
        let limit = match config.store.maxmemory {
            0 => "no memory limit".to_string(),
            n => format!("maxmemory {n} bytes, {}", config.store.policy.name()),
        };
        let io = match io {
            Io::Shared => "shared",
            Io::PerThread => "per-thread",
            Io::Uring => "io_uring",
        };
        eprintln!(
            "tantalum {} listening on {addr} ({} threads, {io} I/O, {limit})",
            env!("CARGO_PKG_VERSION"),
            config.threads,
        );
        if config.settings.password.is_none() && !addr.ip().is_loopback() {
            eprintln!(
                "warning: no password set (--requirepass): anyone who can reach {addr} can read and write"
            );
        }
    };
    // Each returns once shutdown starts; connections are still finishing.
    let mut _shared_runtime = None;
    match io {
        Io::PerThread => {
            let listener = std::net::TcpListener::bind((config.bind.as_str(), config.port))?;
            banner(listener.local_addr()?);
            server::serve_per_thread(listener, ctx.clone(), config.threads)?;
        }
        Io::Uring => {
            let addr = (config.bind.as_str(), config.port)
                .to_socket_addrs()?
                .next()
                .ok_or_else(|| std::io::Error::other("no address to bind"))?;
            banner(addr);
            server::serve_uring(addr, ctx.clone(), config.threads)?;
        }
        Io::Shared => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(config.threads)
                .enable_all()
                .build()?;
            runtime.block_on(async {
                let listener = TcpListener::bind((config.bind.as_str(), config.port)).await?;
                banner(listener.local_addr()?);
                server::serve(listener, ctx.clone()).await
            })?;
            // Its workers run the connections still open.
            _shared_runtime = Some(runtime);
        }
    }
    match server::drain(&ctx, SHUTDOWN_TIMEOUT) {
        0 => eprintln!("all connections closed: exiting"),
        open => {
            eprintln!("{open} connections still open after {SHUTDOWN_TIMEOUT:?}: exiting anyway")
        }
    }
    Ok(())
}
