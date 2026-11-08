//! Helpers shared by the end-to-end tests that replay commands recorded
//! from a real Redis (`tools/redis-diff.py --rust`).

#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tantalum::cmd::{self, Args, Reply};
use tantalum::context::{Context, Session, Settings};
use tantalum::server;
use tantalum::store::Store;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

/// A connection to a fresh server.
pub async fn connect() -> TcpStream {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ctx = Arc::new(Context::new(Arc::new(Store::new(16)), Settings::default()));
    tokio::spawn(server::serve(listener, ctx));
    let stream = TcpStream::connect(addr).await.unwrap();
    stream.set_nodelay(true).unwrap();
    stream
}

pub fn req(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Sends `request` and asserts the server replies with exactly `expected`.
pub async fn check(c: &mut TcpStream, request: &[u8], expected: &[u8]) {
    c.write_all(request).await.unwrap();
    let mut got = vec![0; expected.len()];
    tokio::time::timeout(Duration::from_secs(10), c.read_exact(&mut got))
        .await
        .expect("timed out waiting for a reply")
        .unwrap();
    assert_eq!(
        got.escape_ascii().to_string(),
        expected.escape_ascii().to_string(),
        "after {}",
        request.escape_ascii()
    );
}

/// Replays commands and their recorded replies, in order, on one connection.
pub async fn replay(script: &[(&[&[u8]], &[u8])]) {
    let mut c = connect().await;
    for (args, expected) in script {
        check(&mut c, &req(args), expected).await;
    }
}

pub fn bulk(b: &[u8]) -> Vec<u8> {
    [format!("${}\r\n", b.len()).as_bytes(), b, b"\r\n"].concat()
}

/// Runs one command straight through `cmd::execute`, returning its reply.
pub fn execute(ctx: &Context, session: &mut Session, now: u64, args: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut ranges = Vec::new();
    for arg in args {
        ranges.push(buf.len()..buf.len() + arg.len());
        buf.extend_from_slice(arg);
    }
    let mut out = Vec::new();
    let reply = cmd::execute(ctx, session, now, &Args::new(&buf, &ranges), &mut out);
    assert!(matches!(reply, Reply::Written));
    out
}

/// A parsed RESP reply.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Bulk(Vec<u8>),
    Simple(String),
    Error(String),
    Nil,
    /// Arrays, sets and maps (flattened).
    Array(Vec<Value>),
    Double(String),
}

impl Value {
    pub fn int(&self) -> i64 {
        match self {
            Value::Int(n) => *n,
            other => panic!("not an integer: {other:?}"),
        }
    }

    pub fn bulks(&self) -> Vec<Vec<u8>> {
        match self {
            Value::Array(items) => items
                .iter()
                .map(|v| match v {
                    Value::Bulk(b) => b.clone(),
                    other => panic!("not a bulk: {other:?}"),
                })
                .collect(),
            other => panic!("not an array: {other:?}"),
        }
    }
}

/// Parses exactly one reply from `bytes`.
pub fn parse(bytes: &[u8]) -> Value {
    let (value, rest) = parse_one(bytes);
    assert!(rest.is_empty(), "trailing bytes after {value:?}");
    value
}

fn parse_one(bytes: &[u8]) -> (Value, &[u8]) {
    let end = bytes.windows(2).position(|w| w == b"\r\n").expect("a line");
    let (line, rest) = (&bytes[1..end], &bytes[end + 2..]);
    let text = || String::from_utf8(line.to_vec()).unwrap();
    let n = || text().parse::<i64>().unwrap();
    match bytes[0] {
        b':' => (Value::Int(n()), rest),
        b'+' => (Value::Simple(text()), rest),
        b'-' => (Value::Error(text()), rest),
        b'_' => (Value::Nil, rest),
        b',' => (Value::Double(text()), rest),
        b'$' if n() < 0 => (Value::Nil, rest),
        b'$' => {
            let len = n() as usize;
            (Value::Bulk(rest[..len].to_vec()), &rest[len + 2..])
        }
        b'*' if n() < 0 => (Value::Nil, rest),
        b'*' | b'~' | b'%' => {
            let count = n() as usize * if bytes[0] == b'%' { 2 } else { 1 };
            let mut items = Vec::with_capacity(count);
            let mut rest = rest;
            for _ in 0..count {
                let (item, more) = parse_one(rest);
                items.push(item);
                rest = more;
            }
            (Value::Array(items), rest)
        }
        other => panic!("unknown reply type {:?}", other as char),
    }
}

/// The three ways to run connections.
#[derive(Clone, Copy, Debug)]
pub enum Mode {
    Shared,
    PerThread,
    Uring,
}

pub const MODES: [Mode; 3] = [Mode::Shared, Mode::PerThread, Mode::Uring];

/// A server in `mode` on two threads, and its context.
pub async fn start(mode: Mode) -> (SocketAddr, Arc<Context>) {
    let ctx = Arc::new(Context::new(Arc::new(Store::new(16)), Settings::default()));
    let addr = match mode {
        Mode::Shared => {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(server::serve(listener, ctx.clone()));
            addr
        }
        Mode::PerThread => {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let ctx = ctx.clone();
            std::thread::spawn(move || server::serve_per_thread(listener, ctx, 2));
            addr
        }
        Mode::Uring => {
            let addr = std::net::TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap();
            let ctx = ctx.clone();
            std::thread::spawn(move || server::serve_uring(addr, ctx, 2));
            addr
        }
    };
    (addr, ctx)
}

pub struct Client {
    pub stream: BufReader<TcpStream>,
}

impl Client {
    pub async fn connect(addr: SocketAddr) -> Client {
        for _ in 0..100 {
            if let Ok(stream) = TcpStream::connect(addr).await {
                stream.set_nodelay(true).unwrap();
                return Client {
                    stream: BufReader::new(stream),
                };
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("server never came up on {addr}");
    }

    pub async fn send(&mut self, args: &[&str]) {
        let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        self.send_raw(&req(&args)).await;
    }

    pub async fn send_raw(&mut self, bytes: &[u8]) {
        self.stream.get_mut().write_all(bytes).await.unwrap();
    }

    /// The next reply, raw.
    pub async fn reply(&mut self) -> String {
        let reply = tokio::time::timeout(Duration::from_secs(10), read_reply(&mut self.stream))
            .await
            .expect("timed out waiting for a reply");
        String::from_utf8(reply).unwrap()
    }

    pub async fn call(&mut self, args: &[&str]) -> String {
        self.send(args).await;
        self.reply().await
    }

    /// Whether a reply arrives within a moment.
    pub async fn has_reply(&mut self) -> bool {
        if !self.stream.buffer().is_empty() {
            return true;
        }
        let mut byte = [0];
        let peek = self.stream.get_mut().peek(&mut byte);
        tokio::time::timeout(Duration::from_millis(100), peek)
            .await
            .is_ok()
    }
}

/// Reads one whole RESP reply.
pub async fn read_reply(r: &mut BufReader<TcpStream>) -> Vec<u8> {
    let mut line = Vec::new();
    r.read_until(b'\n', &mut line).await.unwrap();
    assert!(!line.is_empty(), "connection closed");
    let n = || {
        std::str::from_utf8(&line[1..line.len() - 2])
            .unwrap()
            .parse::<i64>()
            .unwrap()
    };
    match line[0] {
        b'$' if n() >= 0 => {
            let mut body = vec![0; n() as usize + 2];
            r.read_exact(&mut body).await.unwrap();
            line.extend_from_slice(&body);
        }
        b'*' | b'>' | b'~' | b'%' if n() >= 0 => {
            let count = n() * if line[0] == b'%' { 2 } else { 1 };
            for _ in 0..count {
                let item = Box::pin(read_reply(r)).await;
                line.extend_from_slice(&item);
            }
        }
        _ => {}
    }
    line
}

/// A buffered connection to a fresh server (`serve`), for raw replies.
pub async fn connect_buffered() -> Client {
    let stream = connect().await;
    Client {
        stream: BufReader::new(stream),
    }
}

impl Client {
    /// The next reply, raw bytes.
    pub async fn raw_reply(&mut self) -> Vec<u8> {
        tokio::time::timeout(Duration::from_secs(10), read_reply(&mut self.stream))
            .await
            .expect("timed out waiting for a reply")
    }
}

// Scenarios over several connections (`tools/acl-diff.py` and the tools
// built on it record them).

/// A step of a scenario recorded from Redis (`tools/acl-diff.py --rust`).
pub enum Step {
    /// On connection N, this command, and Redis' reply (`<closed>` for none).
    Run(usize, &'static [&'static [u8]], &'static [u8]),
    /// On connection N, a command whose reply varies (Redis' is compared in
    /// part by the tool): run, not checked.
    Send(usize, &'static [&'static [u8]]),
    /// Users, the log and the data back to the start, connections closed.
    Reset,
    Sleep(u64),
}

pub struct Connections {
    addr: SocketAddr,
    open: HashMap<usize, BufReader<TcpStream>>,
}

impl Connections {
    pub async fn call(&mut self, n: usize, args: &[&[u8]]) -> Vec<u8> {
        if !self.open.contains_key(&n) {
            // (A server may still be coming up.)
            let mut stream = None;
            for _ in 0..100 {
                if let Ok(s) = TcpStream::connect(self.addr).await {
                    stream = Some(s);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let stream = stream.expect("the server comes up");
            stream.set_nodelay(true).unwrap();
            self.open.insert(n, BufReader::new(stream));
        }
        let conn = self.open.get_mut(&n).expect("open");
        let read_only = args == [b"READ"];
        if !read_only && conn.get_mut().write_all(&req(args)).await.is_err() {
            self.open.remove(&n);
            return b"<closed>".to_vec();
        }
        // A connection the server closed: nothing more to read.
        let closed = match tokio::time::timeout(Duration::from_secs(10), conn.fill_buf()).await {
            Ok(Ok(buf)) => buf.is_empty(),
            Ok(Err(_)) => true,
            Err(_) => panic!("no reply to {args:?}"),
        };
        if closed {
            self.open.remove(&n);
            return b"<closed>".to_vec();
        }
        read_reply(conn).await
    }

    pub async fn reset(&mut self, extra: &[&[&[u8]]]) {
        self.open.clear();
        let users = self.call(0, &[b"ACL", b"USERS"]).await;
        let text = String::from_utf8(users).unwrap();
        for name in text
            .split("\r\n")
            .filter(|l| !l.starts_with(['*', '$']) && !l.is_empty())
        {
            if name != "default" {
                self.call(0, &[b"ACL", b"DELUSER", name.as_bytes()]).await;
            }
        }
        for cmd in [
            &[
                &b"ACL"[..],
                b"SETUSER",
                b"default",
                b"reset",
                b"on",
                b"nopass",
                b"~*",
                b"&*",
                b"+@all",
            ][..],
            &[b"CONFIG", b"SET", b"requirepass", b""],
            &[b"ACL", b"LOG", b"RESET"],
            &[b"CONFIG", b"RESETSTAT"],
            &[b"FLUSHALL"],
        ] {
            self.call(0, cmd).await;
        }
        for cmd in extra {
            self.call(0, cmd).await;
        }
    }
}

fn shown(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// Runs `steps` on a fresh server in `mode`, checking each reply; `extra` are
/// commands a reset sends too.
pub async fn replay_steps(steps: &[Step], extra: &[&[&[u8]]], mode: Mode) {
    let (addr, _ctx) = start(mode).await;
    let mut conns = Connections {
        addr,
        open: HashMap::new(),
    };
    for (i, step) in steps.iter().enumerate() {
        match step {
            Step::Run(n, args, expected) => {
                let reply = conns.call(*n, args).await;
                let command: Vec<String> = args.iter().map(|a| shown(a)).collect();
                assert_eq!(
                    shown(&reply),
                    shown(expected),
                    "step {i}, {mode:?}, on connection {n}: {command:?}"
                );
            }
            Step::Send(n, args) => {
                conns.call(*n, args).await;
            }
            Step::Reset => conns.reset(extra).await,
            Step::Sleep(ms) => tokio::time::sleep(Duration::from_millis(*ms)).await,
        }
    }
}
