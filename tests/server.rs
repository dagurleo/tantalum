//! End-to-end tests over real TCP connections.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tantalum::cmd;
use tantalum::context::{Context, Settings};
use tantalum::server;
use tantalum::store::{Config, EvictionPolicy, Store};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn start() -> SocketAddr {
    start_with(Config {
        shards: 64,
        ..Config::default()
    })
    .await
}

async fn start_with(config: Config) -> SocketAddr {
    let store = Arc::new(Store::with_config(config));
    let ctx = Arc::new(Context::new(store, Settings::default()));
    std::mem::forget(cmd::spawn_expirer(&ctx, Duration::from_millis(10)));
    serve(ctx).await
}

/// A server that requires `AUTH` with `password`.
async fn start_with_password(password: &str) -> SocketAddr {
    serve(context(Settings {
        password: Some(password.into()),
        ..Settings::default()
    }))
    .await
}

fn context(settings: Settings) -> Arc<Context> {
    Arc::new(Context::new(Arc::new(Store::new(16)), settings))
}

async fn serve(ctx: Arc<Context>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(server::serve(listener, ctx));
    addr
}

/// A server running `serve_per_thread` on two threads.
fn start_per_thread() -> SocketAddr {
    serve_per_thread(context(Settings::default()))
}

fn serve_per_thread(ctx: Arc<Context>) -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || server::serve_per_thread(listener, ctx, 2));
    addr
}

/// A server running `serve_uring` on two threads (io_uring on Linux where
/// allowed, epoll/kqueue elsewhere).
fn start_uring() -> SocketAddr {
    serve_uring(context(Settings::default()))
}

fn serve_uring(ctx: Arc<Context>) -> SocketAddr {
    // Find a free port; the loops then bind it themselves with SO_REUSEPORT.
    let addr = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    std::thread::spawn(move || server::serve_uring(addr, ctx, 2));
    addr
}

/// The three ways to run connections.
#[derive(Clone, Copy, Debug)]
enum Mode {
    Shared,
    PerThread,
    Uring,
}

const MODES: [Mode; 3] = [Mode::Shared, Mode::PerThread, Mode::Uring];

async fn serve_mode(mode: Mode, ctx: Arc<Context>) -> SocketAddr {
    match mode {
        Mode::Shared => serve(ctx).await,
        Mode::PerThread => serve_per_thread(ctx),
        Mode::Uring => serve_uring(ctx),
    }
}

/// Everything the server sends until it closes the connection.
async fn read_to_end(stream: &mut TcpStream) -> Vec<u8> {
    let mut out = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut out))
        .await
        .expect("timed out waiting for the server to close")
        .unwrap();
    out
}

/// Polls `condition` until it holds, for up to 10 seconds.
async fn wait_until(what: &str, condition: impl Fn() -> bool) {
    for _ in 0..1_000 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting until {what}");
}

async fn connect_when_up(addr: SocketAddr) -> TcpStream {
    for _ in 0..100 {
        if let Ok(stream) = TcpStream::connect(addr).await {
            stream.set_nodelay(true).unwrap();
            return stream;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("server never came up on {addr}");
}

async fn connect(addr: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect(addr).await.unwrap();
    stream.set_nodelay(true).unwrap();
    stream
}

/// Encodes a command as a RESP multibulk request.
fn req(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

async fn read_exact(stream: &mut TcpStream, n: usize) -> Vec<u8> {
    let mut buf = vec![0; n];
    tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut buf))
        .await
        .expect("timed out waiting for reply")
        .unwrap();
    buf
}

/// Sends `request` and asserts the server replies with exactly `expected`.
async fn check(stream: &mut TcpStream, request: &[u8], expected: &[u8]) {
    stream.write_all(request).await.unwrap();
    let got = read_exact(stream, expected.len()).await;
    assert_eq!(
        got.escape_ascii().to_string(),
        expected.escape_ascii().to_string()
    );
}

#[tokio::test]
async fn basic_commands() {
    let mut c = connect(start().await).await;
    check(&mut c, &req(&[b"GET", b"missing"]), b"$-1\r\n").await;
    check(&mut c, &req(&[b"SET", b"k", b"hello"]), b"+OK\r\n").await;
    check(&mut c, &req(&[b"get", b"k"]), b"$5\r\nhello\r\n").await;
    check(
        &mut c,
        &req(&[b"SET", b"bin\r\n", b"\0\r\n\xff"]),
        b"+OK\r\n",
    )
    .await;
    check(&mut c, &req(&[b"GET", b"bin\r\n"]), b"$4\r\n\0\r\n\xff\r\n").await;
    check(&mut c, &req(&[b"PING"]), b"+PONG\r\n").await;
    check(&mut c, &req(&[b"PING", b"hi"]), b"$2\r\nhi\r\n").await;
    check(&mut c, &req(&[b"ECHO", b"x"]), b"$1\r\nx\r\n").await;
    check(&mut c, &req(&[b"SELECT", b"0"]), b"+OK\r\n").await;
    check(
        &mut c,
        &req(&[b"SELECT", b"16"]),
        b"-ERR DB index is out of range\r\n",
    )
    .await;
    check(&mut c, b"PING\r\n", b"+PONG\r\n").await;
    check(
        &mut c,
        b"set inline yes\r\nget inline\r\n",
        b"+OK\r\n$3\r\nyes\r\n",
    )
    .await;
}

#[tokio::test]
async fn errors_keep_the_connection_open() {
    let mut c = connect(start().await).await;
    check(
        &mut c,
        &req(&[b"GET"]),
        b"-ERR wrong number of arguments for 'get' command\r\n",
    )
    .await;
    check(
        &mut c,
        &req(&[b"SET", b"k"]),
        b"-ERR wrong number of arguments for 'set' command\r\n",
    )
    .await;
    check(
        &mut c,
        &req(&[b"NOPE", b"3", b"a\r\nb"]),
        b"-ERR unknown command 'NOPE', with args beginning with: '3' 'a  b' \r\n",
    )
    .await;
    check(
        &mut c,
        &req(&[b"SET", b"k", b"v", b"NX", b"XX"]),
        b"-ERR syntax error\r\n",
    )
    .await;
    check(
        &mut c,
        &req(&[b"SET", b"k", b"v", b"EX", b"1", b"PX", b"1"]),
        b"-ERR syntax error\r\n",
    )
    .await;
    check(
        &mut c,
        &req(&[b"SET", b"k", b"v", b"EX", b"1", b"KEEPTTL"]),
        b"-ERR syntax error\r\n",
    )
    .await;
    check(
        &mut c,
        &req(&[b"SET", b"k", b"v", b"EX"]),
        b"-ERR syntax error\r\n",
    )
    .await;
    check(
        &mut c,
        &req(&[b"SET", b"k", b"v", b"EX", b"abc"]),
        b"-ERR value is not an integer or out of range\r\n",
    )
    .await;
    check(
        &mut c,
        &req(&[b"SET", b"k", b"v", b"EX", b"0"]),
        b"-ERR invalid expire time in 'set' command\r\n",
    )
    .await;
    check(
        &mut c,
        &req(&[b"SET", b"k", b"v", b"PX", b"-5"]),
        b"-ERR invalid expire time in 'set' command\r\n",
    )
    .await;
    check(
        &mut c,
        &req(&[b"SET", b"k", b"v", b"EX", b"9223372036854775807"]),
        b"-ERR invalid expire time in 'set' command\r\n",
    )
    .await;
    check(
        &mut c,
        &req(&[b"SET", b"k", b"v", b"PX", b"9223372036854775807"]),
        b"-ERR invalid expire time in 'set' command\r\n",
    )
    .await;
    check(&mut c, &req(&[b"GET", b"k"]), b"$-1\r\n").await;
}

#[tokio::test]
async fn set_options() {
    let mut c = connect(start().await).await;
    check(&mut c, &req(&[b"SET", b"k", b"1", b"XX"]), b"$-1\r\n").await;
    check(&mut c, &req(&[b"SET", b"k", b"1", b"NX"]), b"+OK\r\n").await;
    check(&mut c, &req(&[b"SET", b"k", b"2", b"NX"]), b"$-1\r\n").await;
    check(
        &mut c,
        &req(&[b"SET", b"k", b"3", b"xx", b"GET"]),
        b"$1\r\n1\r\n",
    )
    .await;
    check(
        &mut c,
        &req(&[b"SET", b"k", b"4", b"NX", b"GET"]),
        b"$1\r\n3\r\n",
    )
    .await;
    check(&mut c, &req(&[b"SET", b"new", b"x", b"GET"]), b"$-1\r\n").await;
    check(&mut c, &req(&[b"GET", b"k"]), b"$1\r\n3\r\n").await;
}

#[tokio::test]
async fn setex_and_psetex() {
    let mut c = connect(start().await).await;
    check(&mut c, &req(&[b"SETEX", b"a", b"100", b"v1"]), b"+OK\r\n").await;
    check(&mut c, &req(&[b"PSETEX", b"b", b"30", b"v2"]), b"+OK\r\n").await;
    check(&mut c, &req(&[b"GET", b"a"]), b"$2\r\nv1\r\n").await;
    check(&mut c, &req(&[b"GET", b"b"]), b"$2\r\nv2\r\n").await;
    check(
        &mut c,
        &req(&[b"SETEX", b"a", b"0", b"v"]),
        b"-ERR invalid expire time in 'setex' command\r\n",
    )
    .await;
    check(
        &mut c,
        &req(&[b"SETEX", b"a", b"x", b"v"]),
        b"-ERR value is not an integer or out of range\r\n",
    )
    .await;
    check(
        &mut c,
        &req(&[b"SETEX", b"a", b"1"]),
        b"-ERR wrong number of arguments for 'setex' command\r\n",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    check(&mut c, &req(&[b"GET", b"b"]), b"$-1\r\n").await;
    check(&mut c, &req(&[b"GET", b"a"]), b"$2\r\nv1\r\n").await;
}

#[tokio::test]
async fn ttl_expiry() {
    let mut c = connect(start().await).await;
    check(
        &mut c,
        &req(&[b"SET", b"short", b"v", b"PX", b"50"]),
        b"+OK\r\n",
    )
    .await;
    check(
        &mut c,
        &req(&[b"SET", b"long", b"v", b"EX", b"100"]),
        b"+OK\r\n",
    )
    .await;
    check(
        &mut c,
        &req(&[b"SET", b"past", b"v", b"PXAT", b"1"]),
        b"+OK\r\n",
    )
    .await;
    check(&mut c, &req(&[b"GET", b"short"]), b"$1\r\nv\r\n").await;
    check(&mut c, &req(&[b"GET", b"past"]), b"$-1\r\n").await;
    // KEEPTTL keeps the 50ms deadline across the overwrite.
    check(
        &mut c,
        &req(&[b"SET", b"short", b"w", b"KEEPTTL"]),
        b"+OK\r\n",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(120)).await;
    check(&mut c, &req(&[b"GET", b"short"]), b"$-1\r\n").await;
    check(&mut c, &req(&[b"GET", b"long"]), b"$1\r\nv\r\n").await;
}

#[tokio::test]
async fn pipelining() {
    let mut c = connect(start().await).await;
    let (mut request, mut expected) = (Vec::new(), Vec::new());
    for i in 0..5_000 {
        let (k, v) = (format!("key:{i}"), format!("value-{i}"));
        request.extend(req(&[b"SET", k.as_bytes(), v.as_bytes()]));
        request.extend(req(&[b"GET", k.as_bytes()]));
        expected.extend_from_slice(b"+OK\r\n");
        expected.extend(format!("${}\r\n{v}\r\n", v.len()).into_bytes());
    }
    check(&mut c, &request, &expected).await;
}

#[tokio::test]
async fn byte_at_a_time() {
    let mut c = connect(start().await).await;
    let mut request = req(&[b"SET", b"slow", b"value"]);
    request.extend(b"GET slow\r\n");
    request.extend(req(&[b"GET", b"slow"]));
    for byte in &request {
        c.write_all(std::slice::from_ref(byte)).await.unwrap();
    }
    let expected = b"+OK\r\n$5\r\nvalue\r\n$5\r\nvalue\r\n";
    assert_eq!(read_exact(&mut c, expected.len()).await, expected);
}

#[tokio::test]
async fn large_values() {
    let mut c = connect(start().await).await;
    let big: Vec<u8> = (0..4 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    check(&mut c, &req(&[b"SET", b"big", &big]), b"+OK\r\n").await;
    let mut expected = format!("${}\r\n", big.len()).into_bytes();
    expected.extend_from_slice(&big);
    expected.extend_from_slice(b"\r\n");
    // Pipeline it with a small reply on each side to check ordering around the writev path.
    let mut request = req(&[b"PING"]);
    request.extend(req(&[b"GET", b"big"]));
    request.extend(req(&[b"PING"]));
    let mut all = b"+PONG\r\n".to_vec();
    all.extend_from_slice(&expected);
    all.extend_from_slice(b"+PONG\r\n");
    check(&mut c, &request, &all).await;
    let got = read_exact_after(
        &mut c,
        &req(&[b"SET", b"big", b"v", b"GET"]),
        expected.len(),
    )
    .await;
    assert!(
        got == expected,
        "SET ... GET returned the wrong previous value"
    );
}

async fn read_exact_after(c: &mut TcpStream, request: &[u8], n: usize) -> Vec<u8> {
    c.write_all(request).await.unwrap();
    read_exact(c, n).await
}

#[tokio::test]
async fn protocol_error_closes_connection() {
    let mut c = connect(start().await).await;
    c.write_all(b"*1\r\n$-5\r\n").await.unwrap();
    let mut reply = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), c.read_to_end(&mut reply))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply, b"-ERR Protocol error: invalid bulk length\r\n");
}

#[tokio::test]
async fn commands_before_a_protocol_error_still_run() {
    for addr in [start().await, start_uring()] {
        let mut c = connect_when_up(addr).await;
        let mut request = req(&[b"SET", b"a", b"1"]);
        request.extend(req(&[b"GET", b"a"]));
        request.extend(b"*1\r\n$-5\r\n");
        request.extend(req(&[b"SET", b"after", b"error"]));
        c.write_all(&request).await.unwrap();
        let mut reply = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), c.read_to_end(&mut reply))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            reply,
            b"+OK\r\n$1\r\n1\r\n-ERR Protocol error: invalid bulk length\r\n"
        );
    }
}

#[tokio::test]
async fn quit_stops_processing() {
    let mut c = connect(start().await).await;
    let mut request = req(&[b"QUIT"]);
    request.extend(req(&[b"SET", b"after", b"quit"]));
    c.write_all(&request).await.unwrap();
    let mut reply = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), c.read_to_end(&mut reply))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply, b"+OK\r\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_clients() {
    let addr = start().await;
    let tasks: Vec<_> = (0..32)
        .map(|client| {
            tokio::spawn(async move {
                let mut c = connect(addr).await;
                for i in 0..300 {
                    let (k, v) = (format!("c{client}:{i}"), format!("{i}-{client}"));
                    check(
                        &mut c,
                        &req(&[b"SET", k.as_bytes(), v.as_bytes()]),
                        b"+OK\r\n",
                    )
                    .await;
                    let expected = format!("${}\r\n{v}\r\n", v.len());
                    check(&mut c, &req(&[b"GET", k.as_bytes()]), expected.as_bytes()).await;
                }
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
}

/// Reads one RESP reply of any type (arrays included), returned raw.
async fn read_reply(c: &mut TcpStream) -> Vec<u8> {
    let mut out = Vec::new();
    let mut byte = [0u8; 1];
    let mut pending = 1;
    while pending > 0 {
        pending -= 1;
        let start = out.len();
        while !out[start..].ends_with(b"\r\n") {
            c.read_exact(&mut byte).await.unwrap();
            out.push(byte[0]);
        }
        let n: i64 = match out[start] {
            b'$' | b'*' => std::str::from_utf8(&out[start + 1..out.len() - 2])
                .unwrap()
                .parse()
                .unwrap(),
            _ => continue,
        };
        if out[start] == b'*' {
            pending += n.max(0);
        } else if n >= 0 {
            out.extend(read_exact(c, n as usize + 2).await);
        }
    }
    out
}

/// Sends `request` and returns its integer reply.
async fn integer(c: &mut TcpStream, request: &[u8]) -> i64 {
    c.write_all(request).await.unwrap();
    let reply = read_reply(c).await;
    assert_eq!(reply[0], b':', "{}", reply.escape_ascii());
    std::str::from_utf8(&reply[1..reply.len() - 2])
        .unwrap()
        .parse()
        .unwrap()
}

fn info_field(info: &[u8], field: &str) -> u64 {
    let text = String::from_utf8_lossy(info);
    let line = text
        .lines()
        .find(|l| l.starts_with(&format!("{field}:")))
        .unwrap_or_else(|| panic!("{field} missing from INFO:\n{text}"));
    line[field.len() + 1..].parse().unwrap()
}

#[tokio::test]
async fn eviction_over_the_wire() {
    let addr = start_with(Config {
        shards: 16,
        maxmemory: 1_000_000,
        policy: EvictionPolicy::AllKeysLru,
        ..Config::default()
    })
    .await;
    let mut c = connect(addr).await;
    let mut request = Vec::new();
    for i in 0..20_000 {
        request.extend(req(&[b"SET", format!("key:{i}").as_bytes(), &[b'v'; 100]]));
    }
    c.write_all(&request).await.unwrap();
    assert_eq!(
        read_exact(&mut c, 20_000 * 5).await,
        b"+OK\r\n".repeat(20_000)
    );

    c.write_all(&req(&[b"INFO"])).await.unwrap();
    let info = read_reply(&mut c).await;
    assert!(info_field(&info, "used_memory") <= 1_000_000 + 1_000_000 / 32);
    assert_eq!(info_field(&info, "maxmemory"), 1_000_000);
    assert!(info_field(&info, "evicted_keys") > 10_000);
    assert!(String::from_utf8_lossy(&info).contains("maxmemory_policy:allkeys-lru"));

    c.write_all(&req(&[b"DBSIZE"])).await.unwrap();
    let dbsize = read_reply(&mut c).await;
    let keys: u64 = std::str::from_utf8(&dbsize[1..dbsize.len() - 2])
        .unwrap()
        .parse()
        .unwrap();
    assert!(keys > 4_000 && keys < 10_000, "{keys} keys");
    // The most recent write is still there.
    check(
        &mut c,
        &req(&[b"GET", b"key:19999"]),
        &[b"$100\r\n".as_slice(), &[b'v'; 100], b"\r\n"].concat(),
    )
    .await;
}

#[tokio::test]
async fn noeviction_replies_oom() {
    let addr = start_with(Config {
        shards: 4,
        maxmemory: 100_000,
        policy: EvictionPolicy::NoEviction,
        ..Config::default()
    })
    .await;
    let mut c = connect(addr).await;
    let oom: &[u8] = b"-OOM command not allowed when used memory > 'maxmemory'.\r\n";
    let mut got_oom = false;
    for i in 0..2_000 {
        c.write_all(&req(&[b"SET", format!("key:{i}").as_bytes(), &[b'v'; 100]]))
            .await
            .unwrap();
        let reply = read_reply(&mut c).await;
        if reply == oom {
            got_oom = true;
            break;
        }
        assert_eq!(reply, b"+OK\r\n");
    }
    assert!(got_oom);
    // Reads still work, and the connection stays usable.
    check(
        &mut c,
        &req(&[b"GET", b"key:0"]),
        &[b"$100\r\n".as_slice(), &[b'v'; 100], b"\r\n"].concat(),
    )
    .await;
    check(&mut c, &req(&[b"PING"]), b"+PONG\r\n").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_thread_io_serves_many_clients() {
    let addr = start_per_thread();
    let mut c = connect(addr).await;
    check(&mut c, &req(&[b"SET", b"k", b"v"]), b"+OK\r\n").await;
    check(&mut c, &req(&[b"GET", b"k"]), b"$1\r\nv\r\n").await;
    // Clients land on both threads but share one store.
    let tasks: Vec<_> = (0..16)
        .map(|client| {
            tokio::spawn(async move {
                let mut c = connect(addr).await;
                let mut request = Vec::new();
                let mut expected = Vec::new();
                for i in 0..500 {
                    let (k, v) = (format!("p{client}:{i}"), format!("{i}"));
                    request.extend(req(&[b"SET", k.as_bytes(), v.as_bytes()]));
                    request.extend(req(&[b"GET", b"k"]));
                    expected.extend_from_slice(b"+OK\r\n$1\r\nv\r\n");
                }
                check(&mut c, &request, &expected).await;
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    check(&mut c, &req(&[b"GET", b"p15:499"]), b"$3\r\n499\r\n").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uring_io_serves_clients() {
    let addr = start_uring();
    let mut c = connect_when_up(addr).await;
    check(&mut c, &req(&[b"SET", b"k", b"v"]), b"+OK\r\n").await;
    check(&mut c, &req(&[b"GET", b"k"]), b"$1\r\nv\r\n").await;
    // A command split across reads, then a big value, then concurrent clients.
    for byte in req(&[b"GET", b"k"]) {
        c.write_all(&[byte]).await.unwrap();
    }
    assert_eq!(read_exact(&mut c, 7).await, b"$1\r\nv\r\n");
    let big = vec![b'x'; 100_000];
    check(&mut c, &req(&[b"SET", b"big", &big]), b"+OK\r\n").await;
    let mut expected = format!("${}\r\n", big.len()).into_bytes();
    expected.extend_from_slice(&big);
    expected.extend_from_slice(b"\r\n");
    check(&mut c, &req(&[b"GET", b"big"]), &expected).await;
    let tasks: Vec<_> = (0..8)
        .map(|client| {
            tokio::spawn(async move {
                let mut c = connect_when_up(addr).await;
                let mut request = Vec::new();
                let mut expected = Vec::new();
                for i in 0..500 {
                    let (k, v) = (format!("u{client}:{i}"), format!("{i}"));
                    request.extend(req(&[b"SET", k.as_bytes(), v.as_bytes()]));
                    expected.extend_from_slice(b"+OK\r\n");
                }
                check(&mut c, &request, &expected).await;
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    check(&mut c, &req(&[b"GET", b"u7:499"]), b"$3\r\n499\r\n").await;
}

#[tokio::test]
async fn delete_and_exists() {
    let mut c = connect(start().await).await;
    check(
        &mut c,
        &[
            req(&[b"MSET", b"a", b"1", b"b", b"2", b"c", b"3"]),
            req(&[b"EXISTS", b"a", b"b", b"missing", b"a"]),
            req(&[b"DEL", b"a", b"missing"]),
            req(&[b"UNLINK", b"b", b"c", b"b"]),
            req(&[b"EXISTS", b"a", b"b", b"c"]),
            req(&[b"DEL", b"a"]),
            req(&[b"GET", b"a"]),
            req(&[b"DEL"]),
        ]
        .concat(),
        b"+OK\r\n:3\r\n:1\r\n:2\r\n:0\r\n:0\r\n$-1\r\n\
          -ERR wrong number of arguments for 'del' command\r\n",
    )
    .await;
    // A big value is deleted too.
    let big = vec![b'x'; 100_000];
    check(&mut c, &req(&[b"SET", b"big", &big]), b"+OK\r\n").await;
    check(&mut c, &req(&[b"DEL", b"big"]), b":1\r\n").await;
    check(&mut c, &req(&[b"GET", b"big"]), b"$-1\r\n").await;
}

#[tokio::test]
async fn expire_and_ttl() {
    let mut c = connect(start().await).await;
    check(
        &mut c,
        &[
            req(&[b"TTL", b"k"]),
            req(&[b"EXPIRE", b"k", b"100"]),
            req(&[b"SET", b"k", b"v"]),
            req(&[b"TTL", b"k"]),
            req(&[b"PERSIST", b"k"]),
            req(&[b"EXPIRE", b"k", b"100"]),
            req(&[b"TTL", b"k"]),
        ]
        .concat(),
        b":-2\r\n:0\r\n+OK\r\n:-1\r\n:0\r\n:1\r\n:100\r\n",
    )
    .await;
    let pttl = integer(&mut c, &req(&[b"PTTL", b"k"])).await;
    assert!((99_000..=100_000).contains(&pttl), "{pttl}");
    let unix_now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let at = integer(&mut c, &req(&[b"EXPIRETIME", b"k"])).await;
    assert!((unix_now + 98..=unix_now + 101).contains(&at), "{at}");
    let pat = integer(&mut c, &req(&[b"PEXPIRETIME", b"k"])).await;
    assert_eq!(pat / 1000, at);

    // NX, XX, GT and LT. A key without a TTL counts as expiring last.
    check(
        &mut c,
        &[
            req(&[b"EXPIRE", b"k", b"50", b"NX"]),
            req(&[b"EXPIRE", b"k", b"50", b"GT"]),
            req(&[b"EXPIRE", b"k", b"200", b"gt"]),
            req(&[b"EXPIRE", b"k", b"300", b"LT"]),
            req(&[b"EXPIRE", b"k", b"150", b"LT", b"XX"]),
            req(&[b"TTL", b"k"]),
            req(&[b"PERSIST", b"k"]),
            req(&[b"EXPIRE", b"k", b"10", b"XX"]),
            req(&[b"EXPIRE", b"k", b"10", b"GT"]),
            req(&[b"EXPIRE", b"k", b"10", b"LT"]),
            req(&[b"PERSIST", b"k"]),
            req(&[b"EXPIRE", b"k", b"10", b"NX"]),
            req(&[b"TTL", b"k"]),
        ]
        .concat(),
        b":0\r\n:0\r\n:1\r\n:0\r\n:1\r\n:150\r\n:1\r\n:0\r\n:0\r\n:1\r\n:1\r\n:1\r\n:10\r\n",
    )
    .await;

    // A time already passed deletes the key.
    check(
        &mut c,
        &[
            req(&[b"SET", b"gone", b"v"]),
            req(&[b"EXPIRE", b"gone", b"-1"]),
            req(&[b"EXISTS", b"gone"]),
            req(&[b"SET", b"gone", b"v"]),
            req(&[b"PEXPIREAT", b"gone", b"1"]),
            req(&[b"GET", b"gone"]),
            req(&[b"SET", b"later", b"v"]),
            req(&[b"PEXPIRE", b"later", b"50"]),
        ]
        .concat(),
        b"+OK\r\n:1\r\n:0\r\n+OK\r\n:1\r\n$-1\r\n+OK\r\n:1\r\n",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    check(&mut c, &req(&[b"GET", b"later"]), b"$-1\r\n").await;

    // EXPIREAT in the future, and a TTL change on a big value.
    let big = vec![b'x'; 100_000];
    check(
        &mut c,
        &req(&[b"SET", b"big", &big, b"EX", b"5"]),
        b"+OK\r\n",
    )
    .await;
    let at = (unix_now + 1_000).to_string();
    check(
        &mut c,
        &[
            req(&[b"EXPIREAT", b"big", at.as_bytes()]),
            req(&[b"EXPIRETIME", b"big"]),
        ]
        .concat(),
        format!(":1\r\n:{at}\r\n").as_bytes(),
    )
    .await;
    let mut expected = format!("${}\r\n", big.len()).into_bytes();
    expected.extend_from_slice(&big);
    expected.extend_from_slice(b"\r\n");
    check(&mut c, &req(&[b"GET", b"big"]), &expected).await;

    // Errors.
    check(
        &mut c,
        &[
            req(&[b"EXPIRE", b"k", b"10", b"NX", b"XX"]),
            req(&[b"EXPIRE", b"k", b"10", b"GT", b"LT"]),
            req(&[b"EXPIRE", b"k", b"10", b"FOO"]),
            req(&[b"EXPIRE", b"k", b"ten"]),
            req(&[b"EXPIRE", b"k", b"9223372036854775807"]),
            req(&[b"PEXPIRE", b"k", b"9223372036854775807"]),
            req(&[b"TTL"]),
        ]
        .concat(),
        b"-ERR NX and XX, GT or LT options at the same time are not compatible\r\n\
          -ERR GT and LT options at the same time are not compatible\r\n\
          -ERR Unsupported option FOO\r\n\
          -ERR value is not an integer or out of range\r\n\
          -ERR invalid expire time in 'expire' command\r\n\
          -ERR invalid expire time in 'pexpire' command\r\n\
          -ERR wrong number of arguments for 'ttl' command\r\n",
    )
    .await;
}

#[tokio::test]
async fn counters() {
    let mut c = connect(start().await).await;
    check(
        &mut c,
        &[
            req(&[b"INCR", b"n"]),
            req(&[b"INCRBY", b"n", b"8"]),
            // 9 -> 10: the value gets longer.
            req(&[b"INCR", b"n"]),
            req(&[b"DECR", b"n"]),
            req(&[b"DECRBY", b"n", b"-991"]),
            req(&[b"GET", b"n"]),
            req(&[b"DECRBY", b"m", b"5"]),
            req(&[b"SET", b"s", b"abc"]),
            req(&[b"INCR", b"s"]),
            req(&[b"SET", b"s", b" 1"]),
            req(&[b"INCR", b"s"]),
            req(&[b"INCRBY", b"n", b"x"]),
            req(&[b"SET", b"max", b"9223372036854775807"]),
            req(&[b"INCR", b"max"]),
            req(&[b"DECRBY", b"n", b"-9223372036854775808"]),
            req(&[b"GET", b"max"]),
        ]
        .concat(),
        b":1\r\n:9\r\n:10\r\n:9\r\n:1000\r\n$4\r\n1000\r\n:-5\r\n+OK\r\n\
          -ERR value is not an integer or out of range\r\n+OK\r\n\
          -ERR value is not an integer or out of range\r\n\
          -ERR value is not an integer or out of range\r\n+OK\r\n\
          -ERR increment or decrement would overflow\r\n\
          -ERR decrement would overflow\r\n$19\r\n9223372036854775807\r\n",
    )
    .await;
    // INCR keeps the TTL.
    check(
        &mut c,
        &req(&[b"SET", b"t", b"5", b"EX", b"100"]),
        b"+OK\r\n",
    )
    .await;
    check(&mut c, &req(&[b"INCRBY", b"t", b"100"]), b":105\r\n").await;
    check(&mut c, &req(&[b"TTL", b"t"]), b":100\r\n").await;

    // Concurrent increments from many clients don't lose updates.
    let addr = c.peer_addr().unwrap();
    let tasks: Vec<_> = (0..8)
        .map(|_| {
            tokio::spawn(async move {
                let mut c = connect(addr).await;
                let request = req(&[b"INCR", b"shared"]).repeat(1_000);
                c.write_all(&request).await.unwrap();
                for _ in 0..1_000 {
                    read_reply(&mut c).await;
                }
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    check(&mut c, &req(&[b"GET", b"shared"]), b"$4\r\n8000\r\n").await;
}

#[tokio::test]
async fn multi_key_and_get_variants() {
    let mut c = connect(start().await).await;
    let big = vec![b'x'; 100_000];
    let mut big_reply = format!("${}\r\n", big.len()).into_bytes();
    big_reply.extend_from_slice(&big);
    big_reply.extend_from_slice(b"\r\n");
    check(
        &mut c,
        &req(&[b"MSET", b"a", b"1", b"b", b"22", b"big", &big]),
        b"+OK\r\n",
    )
    .await;
    check(
        &mut c,
        &req(&[b"MGET", b"a", b"missing", b"big", b"b"]),
        &[
            b"*4\r\n$1\r\n1\r\n$-1\r\n".as_slice(),
            &big_reply,
            b"$2\r\n22\r\n",
        ]
        .concat(),
    )
    .await;
    // Enough keys for MGET's prefetching to run ahead.
    let keys: Vec<Vec<u8>> = (0..100).map(|i| format!("key:{i}").into_bytes()).collect();
    let mut mset: Vec<&[u8]> = vec![b"MSET"];
    for k in &keys {
        mset.push(k);
        mset.push(k);
    }
    check(&mut c, &req(&mset), b"+OK\r\n").await;
    let mut mget: Vec<&[u8]> = vec![b"MGET"];
    let mut expected = b"*100\r\n".to_vec();
    for k in &keys {
        mget.push(k);
        expected.extend(format!("${}\r\n", k.len()).into_bytes());
        expected.extend_from_slice(k);
        expected.extend_from_slice(b"\r\n");
    }
    check(&mut c, &req(&mget), &expected).await;

    check(
        &mut c,
        &[
            req(&[b"MSET", b"a", b"1", b"b"]),
            req(&[b"SETNX", b"a", b"x"]),
            req(&[b"SETNX", b"new", b"x"]),
            req(&[b"GETDEL", b"new"]),
            req(&[b"GETDEL", b"new"]),
            req(&[b"GETEX", b"a", b"EX", b"100"]),
            req(&[b"TTL", b"a"]),
            req(&[b"GETEX", b"a", b"PERSIST"]),
            req(&[b"TTL", b"a"]),
            req(&[b"GETEX", b"a"]),
            req(&[b"GETEX", b"missing", b"EX", b"5"]),
            req(&[b"GETEX", b"a", b"PXAT", b"1"]),
            req(&[b"EXISTS", b"a"]),
            req(&[b"GETEX", b"b", b"EX", b"0"]),
            req(&[b"GETEX", b"b", b"EX", b"1", b"PERSIST"]),
        ]
        .concat(),
        b"-ERR wrong number of arguments for 'mset' command\r\n:0\r\n:1\r\n$1\r\nx\r\n$-1\r\n\
          $1\r\n1\r\n:100\r\n$1\r\n1\r\n:-1\r\n$1\r\n1\r\n$-1\r\n$1\r\n1\r\n:0\r\n\
          -ERR invalid expire time in 'getex' command\r\n-ERR syntax error\r\n",
    )
    .await;
    check(&mut c, &req(&[b"GETDEL", b"big"]), &big_reply).await;
}

#[tokio::test]
async fn flushall() {
    let mut c = connect(start().await).await;
    let mut request = Vec::new();
    for i in 0..1_000 {
        request.extend(req(&[b"SET", format!("k{i}").as_bytes(), b"v"]));
    }
    c.write_all(&request).await.unwrap();
    read_exact(&mut c, 5 * 1_000).await;
    check(
        &mut c,
        &[
            req(&[b"FLUSHALL"]),
            req(&[b"DBSIZE"]),
            req(&[b"GET", b"k1"]),
            req(&[b"SET", b"k1", b"again"]),
            req(&[b"FLUSHDB", b"ASYNC"]),
            req(&[b"FLUSHALL", b"SYNC"]),
            req(&[b"FLUSHALL", b"NOW"]),
            req(&[b"DBSIZE"]),
        ]
        .concat(),
        b"+OK\r\n:0\r\n$-1\r\n+OK\r\n+OK\r\n+OK\r\n-ERR syntax error\r\n:0\r\n",
    )
    .await;
    c.write_all(&req(&[b"INFO"])).await.unwrap();
    let info = read_reply(&mut c).await;
    assert!(
        info_field(&info, "used_memory") < 256 * 1024,
        "memory was freed"
    );
}

#[tokio::test]
async fn password() {
    let addr = start_with_password("s3cret").await;
    let mut c = connect(addr).await;
    let noauth: &[u8] = b"-NOAUTH Authentication required.\r\n";
    let wrongpass: &[u8] = b"-WRONGPASS invalid username-password pair or user is disabled.\r\n";
    // Unknown commands and wrong arity are reported before authentication.
    check(
        &mut c,
        &[
            req(&[b"GET", b"k"]),
            req(&[b"SET", b"k", b"v"]),
            req(&[b"PING"]),
            req(&[b"NOPE"]),
            req(&[b"GET"]),
            req(&[b"AUTH", b"wrong"]),
            req(&[b"AUTH", b"other", b"s3cret"]),
            req(&[b"AUTH", b"a", b"b", b"c"]),
            req(&[b"HELLO", b"2"]),
            req(&[b"GET", b"k"]),
        ]
        .concat(),
        &[
            noauth,
            noauth,
            noauth,
            b"-ERR unknown command 'NOPE', with args beginning with: \r\n",
            b"-ERR wrong number of arguments for 'get' command\r\n",
            wrongpass,
            wrongpass,
            b"-ERR syntax error\r\n",
            b"-NOAUTH HELLO must be called with the client already authenticated, otherwise \
              the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the \
              client and select the RESP protocol version at the same time\r\n",
            noauth,
        ]
        .concat(),
    )
    .await;
    check(
        &mut c,
        &[
            req(&[b"AUTH", b"s3cret"]),
            req(&[b"SET", b"k", b"v"]),
            // A failed AUTH doesn't log an authenticated client out.
            req(&[b"AUTH", b"wrong"]),
            req(&[b"GET", b"k"]),
        ]
        .concat(),
        &[b"+OK\r\n+OK\r\n".as_slice(), wrongpass, b"$1\r\nv\r\n"].concat(),
    )
    .await;

    // The username form, and HELLO with AUTH.
    let mut c = connect(addr).await;
    check(
        &mut c,
        &[req(&[b"AUTH", b"default", b"s3cret"]), req(&[b"GET", b"k"])].concat(),
        b"+OK\r\n$1\r\nv\r\n",
    )
    .await;
    let mut c = connect(addr).await;
    c.write_all(&req(&[b"HELLO", b"2", b"AUTH", b"default", b"s3cret"]))
        .await
        .unwrap();
    assert!(read_reply(&mut c).await.starts_with(b"*14\r\n"));
    check(&mut c, &req(&[b"GET", b"k"]), b"$1\r\nv\r\n").await;
    // QUIT works without authenticating.
    let mut c = connect(addr).await;
    check(&mut c, &req(&[b"QUIT"]), b"+OK\r\n").await;
}

#[tokio::test]
async fn auth_without_a_password() {
    let mut c = connect(start().await).await;
    check(
        &mut c,
        &[
            req(&[b"AUTH", b"anything"]),
            req(&[b"AUTH", b"default", b"anything"]),
            req(&[b"AUTH", b"someone", b"anything"]),
        ]
        .concat(),
        b"-ERR AUTH <password> called without any password configured for the default user. \
          Are you sure your configuration is correct?\r\n+OK\r\n\
          -WRONGPASS invalid username-password pair or user is disabled.\r\n",
    )
    .await;
}

#[tokio::test]
async fn connection_commands() {
    let mut c = connect(start().await).await;
    let hello: &[u8] = b"*14\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.2.4\r\n\
        $5\r\nproto\r\n:2\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n\
        $4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n";
    check(
        &mut c,
        &[
            req(&[b"HELLO"]),
            req(&[b"HELLO", b"4"]),
            req(&[b"HELLO", b"x"]),
            req(&[b"HELLO", b"2", b"SETNAME", b"app"]),
            req(&[b"CLIENT", b"GETNAME"]),
            req(&[b"HELLO", b"2", b"FOO"]),
            req(&[b"CLIENT", b"SETNAME", b"bad name"]),
            req(&[b"CLIENT", b"SETNAME", b"worker-1"]),
            req(&[b"CLIENT", b"GETNAME"]),
            req(&[b"CLIENT", b"SETNAME", b""]),
            req(&[b"CLIENT", b"GETNAME"]),
            req(&[b"CLIENT", b"ID"]),
            req(&[b"CLIENT", b"SETINFO", b"LIB-NAME", b"redis-py"]),
            req(&[b"CLIENT", b"SETINFO", b"lib-ver", b"5.0.1"]),
            req(&[b"CLIENT", b"SETINFO", b"color", b"red"]),
            req(&[b"CLIENT", b"SETNAME"]),
            req(&[b"CLIENT", b"NOPE", b"x"]),
        ]
        .concat(),
        &[
            hello,
            b"-NOPROTO unsupported protocol version\r\n",
            b"-ERR Protocol version is not an integer or out of range\r\n",
            hello,
            b"$3\r\napp\r\n",
            b"-ERR Syntax error in HELLO option 'FOO'\r\n",
            b"-ERR Client names cannot contain spaces, newlines or special characters.\r\n",
            b"+OK\r\n$8\r\nworker-1\r\n+OK\r\n$-1\r\n:1\r\n+OK\r\n+OK\r\n",
            b"-ERR Unrecognized option 'color'\r\n",
            b"-ERR wrong number of arguments for 'client|setname' command\r\n",
            b"-ERR unknown subcommand 'NOPE'. Try CLIENT HELP.\r\n",
        ]
        .concat(),
    )
    .await;
    // Connections get increasing ids.
    let mut d = connect(c.peer_addr().unwrap()).await;
    check(&mut d, &req(&[b"CLIENT", b"ID"]), b":2\r\n").await;

    c.write_all(&req(&[b"COMMAND", b"COUNT"])).await.unwrap();
    let count = read_reply(&mut c).await;
    c.write_all(&req(&[b"COMMAND"])).await.unwrap();
    let all = read_reply(&mut c).await;
    assert!(
        all.starts_with(format!("*{}\r\n", count[1..count.len() - 2].escape_ascii()).as_bytes())
    );
    c.write_all(&req(&[b"COMMAND", b"INFO", b"get", b"nope", b"MSET"]))
        .await
        .unwrap();
    let info = read_reply(&mut c).await.escape_ascii().to_string();
    assert!(
        info.starts_with(
            r"*3\r\n*10\r\n$3\r\nget\r\n:2\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n:1\r\n:1\r\n*3\r\n+@read"
        ),
        "{info}"
    );
    assert!(
        info.contains(r"$-1\r\n*10\r\n$4\r\nmset\r\n:-3\r\n"),
        "{info}"
    );
    check(
        &mut c,
        &[
            req(&[b"CONFIG", b"GET", b"maxmemory", b"maxmemory-policy"]),
            req(&[b"CONFIG", b"GET", b"save", b"APPENDONLY", b"nothing"]),
            req(&[b"CONFIG", b"GET"]),
        ]
        .concat(),
        b"*4\r\n$9\r\nmaxmemory\r\n$1\r\n0\r\n$16\r\nmaxmemory-policy\r\n$10\r\nnoeviction\r\n\
          *4\r\n$10\r\nappendonly\r\n$2\r\nno\r\n$4\r\nsave\r\n$0\r\n\r\n\
          -ERR wrong number of arguments for 'config|get' command\r\n",
    )
    .await;
    c.write_all(&req(&[b"INFO"])).await.unwrap();
    let info = String::from_utf8(read_reply(&mut c).await).unwrap();
    assert!(info.contains("redis_version:7.2.4\r\n"), "{info}");
}

#[tokio::test]
async fn resp3() {
    let mut c = connect(start().await).await;
    check(
        &mut c,
        &[
            req(&[b"HELLO", b"3"]),
            req(&[b"GET", b"missing"]),
            req(&[b"SET", b"a", b"1"]),
            req(&[b"MGET", b"a", b"missing"]),
            req(&[b"SET", b"a", b"2", b"NX"]),
            req(&[b"SET", b"b", b"2", b"XX", b"GET"]),
            req(&[b"GETDEL", b"missing"]),
            req(&[b"CLIENT", b"GETNAME"]),
            req(&[b"CONFIG", b"GET", b"maxmemory"]),
            // HELLO without a version keeps the protocol.
            req(&[b"HELLO"]),
        ]
        .concat(),
        b"%7\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.2.4\r\n\
          $5\r\nproto\r\n:3\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n\
          $4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n\
          _\r\n+OK\r\n*2\r\n$1\r\n1\r\n_\r\n_\r\n_\r\n_\r\n_\r\n\
          %1\r\n$9\r\nmaxmemory\r\n$1\r\n0\r\n\
          %7\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.2.4\r\n\
          $5\r\nproto\r\n:3\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n\
          $4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n",
    )
    .await;
    // INFO is verbatim text.
    c.write_all(&req(&[b"INFO"])).await.unwrap();
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n") {
        header.extend(read_exact(&mut c, 1).await);
    }
    assert_eq!(header[0], b'=', "{}", header.escape_ascii());
    let len: usize = std::str::from_utf8(&header[1..header.len() - 2])
        .unwrap()
        .parse()
        .unwrap();
    let body = read_exact(&mut c, len + 2).await;
    assert!(body.starts_with(b"txt:# Server\r\n"));
    // And back to RESP2.
    check(
        &mut c,
        &[req(&[b"HELLO", b"2"]), req(&[b"GET", b"missing"])].concat(),
        b"*14\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.2.4\r\n\
          $5\r\nproto\r\n:2\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n\
          $4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n$-1\r\n",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maxclients_refuses_extra_connections() {
    for mode in MODES {
        let ctx = context(Settings {
            maxclients: 2,
            ..Settings::default()
        });
        let addr = serve_mode(mode, ctx.clone()).await;
        let mut a = connect_when_up(addr).await;
        check(&mut a, &req(&[b"PING"]), b"+PONG\r\n").await;
        let mut b = connect(addr).await;
        check(&mut b, &req(&[b"PING"]), b"+PONG\r\n").await;
        let mut c = connect(addr).await;
        assert_eq!(
            read_to_end(&mut c).await,
            b"-ERR max number of clients reached\r\n",
            "{mode:?}"
        );
        drop(a);
        wait_until("the first client is gone", || ctx.connected() == 1).await;
        let mut d = connect(addr).await;
        check(&mut d, &req(&[b"PING"]), b"+PONG\r\n").await;
        d.write_all(&req(&[b"INFO"])).await.unwrap();
        let info = read_reply(&mut d).await;
        assert_eq!(info_field(&info, "rejected_connections"), 1, "{mode:?}");
        assert_eq!(info_field(&info, "connected_clients"), 2, "{mode:?}");
        assert_eq!(info_field(&info, "maxclients"), 2, "{mode:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_connections_time_out() {
    for mode in MODES {
        let ctx = context(Settings {
            timeout: Some(Duration::from_millis(300)),
            ..Settings::default()
        });
        let addr = serve_mode(mode, ctx.clone()).await;
        let mut idle = connect_when_up(addr).await;
        check(&mut idle, &req(&[b"PING"]), b"+PONG\r\n").await;
        let mut busy = connect(addr).await;
        for _ in 0..8 {
            check(&mut busy, &req(&[b"PING"]), b"+PONG\r\n").await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(read_to_end(&mut idle).await, b"", "{mode:?}");
        check(&mut busy, &req(&[b"PING"]), b"+PONG\r\n").await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown() {
    for mode in MODES {
        let ctx = context(Settings::default());
        let addr = serve_mode(mode, ctx.clone()).await;
        let mut idle = connect_when_up(addr).await;
        check(&mut idle, &req(&[b"PING"]), b"+PONG\r\n").await;
        let mut busy = connect(addr).await;
        check(
            &mut busy,
            &[req(&[b"SET", b"k", b"v"]), req(&[b"GET", b"k"])].concat(),
            b"+OK\r\n$1\r\nv\r\n",
        )
        .await;
        ctx.shutdown();
        // Open connections are closed (each after finishing what it had read).
        assert_eq!(read_to_end(&mut idle).await, b"", "{mode:?}");
        assert_eq!(read_to_end(&mut busy).await, b"", "{mode:?}");
        let drained = tokio::task::spawn_blocking({
            let ctx = ctx.clone();
            move || server::drain(&ctx, Duration::from_secs(5))
        });
        assert_eq!(drained.await.unwrap(), 0, "{mode:?}");
        // And no new ones get served.
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Ok(mut late) = TcpStream::connect(addr).await {
            let _ = late.write_all(&req(&[b"PING"])).await;
            let mut out = Vec::new();
            let read =
                tokio::time::timeout(Duration::from_secs(5), late.read_to_end(&mut out)).await;
            assert!(
                matches!(read, Ok(Err(_))) || out.is_empty(),
                "{mode:?}: served after shutdown: {}",
                out.escape_ascii()
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panic_only_closes_its_connection() {
    for mode in MODES {
        let ctx = context(Settings {
            panic_command: Some("BOOM"),
            ..Settings::default()
        });
        let addr = serve_mode(mode, ctx.clone()).await;
        let mut other = connect_when_up(addr).await;
        check(&mut other, &req(&[b"PING"]), b"+PONG\r\n").await;
        let mut c = connect(addr).await;
        c.write_all(
            &[
                req(&[b"SET", b"a", b"1"]),
                req(&[b"BOOM"]),
                req(&[b"GET", b"a"]),
            ]
            .concat(),
        )
        .await
        .unwrap();
        assert_eq!(
            read_to_end(&mut c).await,
            b"+OK\r\n-ERR internal error, closing the connection\r\n",
            "{mode:?}"
        );
        check(&mut other, &req(&[b"GET", b"a"]), b"$1\r\n1\r\n").await;
        let mut new = connect(addr).await;
        check(&mut new, &req(&[b"PING"]), b"+PONG\r\n").await;
        wait_until("the panicked client is gone", || ctx.connected() == 2).await;
    }
}

#[tokio::test]
async fn info_metrics() {
    let addr = start().await;
    let mut c = connect(addr).await;
    check(
        &mut c,
        &[
            req(&[b"SET", b"a", b"1"]),
            req(&[b"GET", b"a"]),
            req(&[b"GET", b"b"]),
            req(&[b"MGET", b"a", b"b", b"c"]),
            req(&[b"GETDEL", b"a"]),
            req(&[b"GETEX", b"z"]),
        ]
        .concat(),
        b"+OK\r\n$1\r\n1\r\n$-1\r\n*3\r\n$1\r\n1\r\n$-1\r\n$-1\r\n$1\r\n1\r\n$-1\r\n",
    )
    .await;
    c.write_all(&req(&[b"INFO"])).await.unwrap();
    let info = read_reply(&mut c).await;
    assert_eq!(info_field(&info, "keyspace_hits"), 3);
    assert_eq!(info_field(&info, "keyspace_misses"), 4);
    assert_eq!(info_field(&info, "total_commands_processed"), 7);
    assert_eq!(info_field(&info, "connected_clients"), 1);
    assert_eq!(info_field(&info, "total_connections_received"), 1);
    // An idle connection holds no buffers (and small ones aren't counted).
    assert_eq!(info_field(&info, "mem_clients_normal"), 0);
    assert!(info_field(&info, "process_id") > 0);
}

#[tokio::test]
async fn connection_buffers_count_as_used_memory() {
    let addr = start().await;
    let mut c = connect(addr).await;
    let clients_memory = async |c: &mut TcpStream| {
        c.write_all(&req(&[b"INFO"])).await.unwrap();
        let info = read_reply(c).await;
        let clients = info_field(&info, "mem_clients_normal");
        assert!(info_field(&info, "used_memory") >= clients);
        clients
    };
    let idle = clients_memory(&mut c).await;
    assert!(idle < 100_000, "{idle}");
    // A half-sent 8 MB value holds a buffer at least that big.
    let mut big = connect(addr).await;
    big.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$8000000\r\n")
        .await
        .unwrap();
    big.write_all(&vec![b'x'; 6_000_000]).await.unwrap();
    let mut held = 0;
    for _ in 0..500 {
        held = clients_memory(&mut c).await;
        if held >= 6_000_000 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(held >= 6_000_000, "{held}");
    // Once it's done, the big buffer is released.
    big.write_all(&[&vec![b'x'; 2_000_000][..], b"\r\n"].concat())
        .await
        .unwrap();
    assert_eq!(read_exact(&mut big, 5).await, b"+OK\r\n");
    // (The connection updates its count just after replying.)
    let mut now_held = held;
    for _ in 0..500 {
        now_held = clients_memory(&mut c).await;
        if now_held < 200_000 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(now_held < 200_000, "{now_held}");
}

#[tokio::test]
async fn snapshot_commands_when_snapshots_are_off() {
    let mut c = connect(start().await).await;
    let off: &[u8] = b"-ERR snapshots are off: start Tantalum with --snapshot <file>\r\n";
    check(
        &mut c,
        &[
            req(&[b"SAVE"]),
            req(&[b"BGSAVE"]),
            req(&[b"BGSAVE", b"SCHEDULE"]),
        ]
        .concat(),
        &[off, off, off].concat(),
    )
    .await;
    let lastsave = integer(&mut c, &req(&[b"LASTSAVE"])).await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    assert!((now - 60..=now).contains(&lastsave), "{lastsave}");
    c.write_all(&req(&[b"INFO"])).await.unwrap();
    let info = String::from_utf8(read_reply(&mut c).await).unwrap();
    assert!(
        info.contains("# Persistence\r\nloading:0\r\nasync_loading:0\r\nsnapshots:off\r\n"),
        "{info}"
    );
    for field in [
        "rdb_changes_since_last_save:0\r\n",
        "rdb_last_save_time:",
        "rdb_bgsave_in_progress:0\r\n",
    ] {
        assert!(info.contains(field), "{field} in {info}");
    }
    check(&mut c, &req(&[b"BGSAVE", b"NOW"]), b"-ERR syntax error\r\n").await;
}

/// A blocking connection for the tests of held replies below, sending each
/// request in a `write` of its own.
fn blocking_connection(addr: SocketAddr) -> std::net::TcpStream {
    let stream = std::net::TcpStream::connect(addr).unwrap();
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    stream
}

fn request(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        out.extend_from_slice(format!("${}\r\n{arg}\r\n", arg.len()).as_bytes());
    }
    out
}

/// With `--io per-thread`, one-command replies are held and sent together. A
/// socket that can't take one (the client stopped reading) must get the rest
/// of it later, ahead of anything else.
#[test]
fn a_client_that_stops_reading_gets_every_reply_in_order() {
    use std::io::{Read, Write};
    const REQUESTS: usize = 8000;
    // A reply just under the size that's held, far more of them than the
    // socket buffers hold.
    let padding = "x".repeat(3900);
    let payload = |i: usize| format!("{i:08}{padding}");

    let addr = start_per_thread();
    let mut reader = blocking_connection(addr);
    let mut writer = reader.try_clone().unwrap();
    let sender = {
        let padding = padding.clone();
        std::thread::spawn(move || {
            for i in 0..REQUESTS {
                let payload = format!("{i:08}{padding}");
                writer.write_all(&request(&["ECHO", &payload])).unwrap();
                // The server catches up, so each is read alone.
                std::thread::sleep(Duration::from_micros(20));
            }
        })
    };
    // Long enough for the socket buffers to fill.
    std::thread::sleep(Duration::from_millis(500));
    let mut reply = vec![0; format!("$3908\r\n{}\r\n", payload(0)).len()];
    for i in 0..REQUESTS {
        reader.read_exact(&mut reply).unwrap();
        let expected = format!("$3908\r\n{}\r\n", payload(i));
        assert!(reply == expected.as_bytes(), "reply {i} is wrong");
    }
    sender.join().unwrap();
}

/// Connections' replies held at the same moment go to the right clients.
#[test]
fn held_replies_go_to_their_own_connections() {
    use std::io::{Read, Write};
    const CLIENTS: usize = 40;
    const ROUNDS: usize = 300;
    let addr = start_per_thread();
    let barrier = Arc::new(std::sync::Barrier::new(CLIENTS));
    let clients: Vec<_> = (0..CLIENTS)
        .map(|client| {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut stream = blocking_connection(addr);
                for round in 0..ROUNDS {
                    // Together, so the server finds many ready at once.
                    barrier.wait();
                    // Of different lengths, so a mix-up can't go unnoticed,
                    // and some in batches, whose replies are held too.
                    let message = format!("{client}:{round}:{}", "y".repeat(client));
                    let one = request(&["ECHO", &message]);
                    let batch = 1 + (client + round) % 4;
                    let requests = one.repeat(batch);
                    // And some split across writes.
                    let (start, rest) = requests.split_at(requests.len() / (1 + round % 3));
                    stream.write_all(start).unwrap();
                    stream.write_all(rest).unwrap();
                    let expected = format!("${}\r\n{message}\r\n", message.len()).repeat(batch);
                    let mut reply = vec![0; expected.len()];
                    stream.read_exact(&mut reply).unwrap();
                    assert_eq!(String::from_utf8_lossy(&reply), expected);
                }
            })
        })
        .collect();
    for client in clients {
        client.join().unwrap();
    }
}

/// A client that goes away with its reply held costs nobody else anything,
/// and what it sent still runs, as with Redis.
#[test]
fn a_client_gone_before_its_reply_disturbs_nobody() {
    use std::io::{Read, Write};
    const CLIENTS: usize = 500;
    let addr = start_per_thread();
    let mut other = blocking_connection(addr);
    let mut pong = [0; 7];
    for _ in 0..CLIENTS {
        let mut leaving = blocking_connection(addr);
        leaving.write_all(&request(&["INCR", "n"])).unwrap();
        drop(leaving);
        other.write_all(&request(&["PING"])).unwrap();
        other.read_exact(&mut pong).unwrap();
        assert_eq!(&pong, b"+PONG\r\n");
    }
    // They ran, whichever way each connection's end and its request met.
    let expected = format!("${}\r\n{CLIENTS}\r\n", CLIENTS.to_string().len());
    let mut reply = vec![0; expected.len()];
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        other.write_all(&request(&["GET", "n"])).unwrap();
        other.read_exact(&mut reply).unwrap();
        if reply == expected.as_bytes() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{}",
            String::from_utf8_lossy(&reply)
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// A batch's replies are held, and the rest of a command split across reads
/// comes later: its reply must follow theirs.
#[test]
fn a_command_split_across_reads_is_answered_in_order() {
    use std::io::{Read, Write};
    let mut stream = blocking_connection(start_per_thread());
    let third = request(&["ECHO", "c"]);
    let (start, rest) = third.split_at(5);
    let mut first = [request(&["ECHO", "a"]), request(&["ECHO", "b"])].concat();
    first.extend_from_slice(start);
    stream.write_all(&first).unwrap();
    let mut reply = [0; 14];
    stream.read_exact(&mut reply).unwrap();
    assert_eq!(&reply, b"$1\r\na\r\n$1\r\nb\r\n");
    stream.write_all(rest).unwrap();
    let mut reply = [0; 7];
    stream.read_exact(&mut reply).unwrap();
    assert_eq!(&reply, b"$1\r\nc\r\n");
}
