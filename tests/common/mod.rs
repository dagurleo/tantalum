//! Helpers shared by the end-to-end tests that replay commands recorded
//! from a real Redis (`tools/redis-diff.py --rust`).

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use tantalum::cmd::{self, Args, Reply};
use tantalum::context::{Context, Session, Settings};
use tantalum::server;
use tantalum::store::Store;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
