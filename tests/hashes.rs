//! Hash commands over the wire, against replies recorded from a real Redis
//! (7.0.11, with `tools/redis-diff.py --rust`), errors included.

use std::sync::Arc;
use std::time::Duration;

use tantalum::context::{Context, Settings};
use tantalum::server;
use tantalum::store::Store;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn connect() -> TcpStream {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ctx = Arc::new(Context::new(Arc::new(Store::new(16)), Settings::default()));
    tokio::spawn(server::serve(listener, ctx));
    let stream = TcpStream::connect(addr).await.unwrap();
    stream.set_nodelay(true).unwrap();
    stream
}

fn req(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Sends `request` and asserts the server replies with exactly `expected`.
async fn check(c: &mut TcpStream, request: &[u8], expected: &[u8]) {
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

/// Commands and Redis' replies, in order, on one connection.
#[rustfmt::skip]
const SCRIPT: &[(&[&[u8]], &[u8])] = &[
    (&[b"HSET", b"h", b"f1", b"v1", b"f2", b"v2"], b":2\r\n"),
    (&[b"HSET", b"h", b"f1", b"x", b"f3", b"v3"], b":1\r\n"),
    (&[b"HSET", b"h"], b"-ERR wrong number of arguments for 'hset' command\r\n"),
    (&[b"HSET", b"h", b"f"], b"-ERR wrong number of arguments for 'hset' command\r\n"),
    (&[b"HSET", b"h", b"f", b"v", b"f2"], b"-ERR wrong number of arguments for 'hset' command\r\n"),
    (&[b"HMSET", b"h", b"a", b"1", b"b"], b"-ERR wrong number of arguments for 'hmset' command\r\n"),
    (&[b"HMSET", b"h", b"a", b"1", b"b", b"2"], b"+OK\r\n"),
    (&[b"HSET", b"d", b"a", b"1", b"a", b"2"], b":1\r\n"),
    (&[b"HGET", b"d", b"a"], b"$1\r\n2\r\n"),
    (&[b"HSET", b"h", b"f1", b"x"], b":0\r\n"),
    (&[b"HGET", b"h", b"f1"], b"$1\r\nx\r\n"),
    (&[b"HGET", b"h", b"nope"], b"$-1\r\n"),
    (&[b"HGET", b"nokey", b"f"], b"$-1\r\n"),
    (&[b"HMGET", b"h", b"f1", b"nope", b"f3"], b"*3\r\n$1\r\nx\r\n$-1\r\n$2\r\nv3\r\n"),
    (&[b"HMGET", b"nokey", b"a", b"b"], b"*2\r\n$-1\r\n$-1\r\n"),
    (&[b"HMGET", b"h", b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h", b"i", b"j", b"k", b"l", b"f1", b"f2", b"f3"], b"*15\r\n$1\r\n1\r\n$1\r\n2\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n$1\r\nx\r\n$2\r\nv2\r\n$2\r\nv3\r\n"),
    (&[b"HGETALL", b"h"], b"*10\r\n$2\r\nf1\r\n$1\r\nx\r\n$2\r\nf2\r\n$2\r\nv2\r\n$2\r\nf3\r\n$2\r\nv3\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n"),
    (&[b"HGETALL", b"nokey"], b"*0\r\n"),
    (&[b"HKEYS", b"h"], b"*5\r\n$2\r\nf1\r\n$2\r\nf2\r\n$2\r\nf3\r\n$1\r\na\r\n$1\r\nb\r\n"),
    (&[b"HKEYS", b"nokey"], b"*0\r\n"),
    (&[b"HVALS", b"h"], b"*5\r\n$1\r\nx\r\n$2\r\nv2\r\n$2\r\nv3\r\n$1\r\n1\r\n$1\r\n2\r\n"),
    (&[b"HVALS", b"nokey"], b"*0\r\n"),
    (&[b"HLEN", b"h"], b":5\r\n"),
    (&[b"HLEN", b"nokey"], b":0\r\n"),
    (&[b"HEXISTS", b"h", b"f1"], b":1\r\n"),
    (&[b"HEXISTS", b"h", b"nope"], b":0\r\n"),
    (&[b"HEXISTS", b"nokey", b"f"], b":0\r\n"),
    (&[b"HSTRLEN", b"h", b"f1"], b":1\r\n"),
    (&[b"HSTRLEN", b"h", b"nope"], b":0\r\n"),
    (&[b"HSTRLEN", b"nokey", b"f"], b":0\r\n"),
    (&[b"HSETNX", b"h", b"f1", b"z"], b":0\r\n"),
    (&[b"HSETNX", b"h", b"new", b"z"], b":1\r\n"),
    (&[b"HSETNX", b"fresh", b"a", b"b"], b":1\r\n"),
    (&[b"HGETALL", b"fresh"], b"*2\r\n$1\r\na\r\n$1\r\nb\r\n"),
    (&[b"HSET", b"o", b"a", b"1", b"b", b"2", b"c", b"3"], b":3\r\n"),
    (&[b"HDEL", b"o", b"a"], b":1\r\n"),
    (&[b"HSET", b"o", b"a", b"4"], b":1\r\n"),
    (&[b"HSET", b"o", b"b", b"2"], b":0\r\n"),
    (&[b"HGETALL", b"o"], b"*6\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n$1\r\na\r\n$1\r\n4\r\n"),
    (&[b"HDEL", b"o", b"a", b"b", b"c", b"nope"], b":3\r\n"),
    (&[b"EXISTS", b"o"], b":0\r\n"),
    (&[b"HDEL", b"o", b"a"], b":0\r\n"),
    (&[b"HDEL", b"nokey", b"a"], b":0\r\n"),
    (&[b"HINCRBY", b"c", b"n", b"5"], b":5\r\n"),
    (&[b"HINCRBY", b"c", b"n", b"-10"], b":-5\r\n"),
    (&[b"HINCRBY", b"c", b"n", b"abc"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"HINCRBY", b"c", b"n", b"1.5"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"HINCRBY", b"c", b"n", b""], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"HSET", b"c", b"s", b"hello"], b":1\r\n"),
    (&[b"HINCRBY", b"c", b"s", b"1"], b"-ERR hash value is not an integer\r\n"),
    (&[b"HSET", b"c", b"big", b"9223372036854775800"], b":1\r\n"),
    (&[b"HINCRBY", b"c", b"big", b"7"], b":9223372036854775807\r\n"),
    (&[b"HINCRBY", b"c", b"big", b"1"], b"-ERR increment or decrement would overflow\r\n"),
    (&[b"HSET", b"c", b"small", b"-9223372036854775800"], b":1\r\n"),
    (&[b"HINCRBY", b"c", b"small", b"-8"], b":-9223372036854775808\r\n"),
    (&[b"HINCRBY", b"c", b"small", b"-1"], b"-ERR increment or decrement would overflow\r\n"),
    (&[b"HSET", b"c", b"lead", b"007"], b":1\r\n"),
    (&[b"HINCRBY", b"c", b"lead", b"1"], b"-ERR hash value is not an integer\r\n"),
    (&[b"HSET", b"c", b"plus", b"+5"], b":1\r\n"),
    (&[b"HINCRBY", b"c", b"plus", b"1"], b"-ERR hash value is not an integer\r\n"),
    (&[b"HGET", b"c", b"n"], b"$2\r\n-5\r\n"),
    (&[b"SET", b"str", b"hello"], b"+OK\r\n"),
    (&[b"HSET", b"str", b"f", b"v"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"HGET", b"str", b"f"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"HMGET", b"str", b"f"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"HGETALL", b"str"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"HKEYS", b"str"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"HVALS", b"str"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"HLEN", b"str"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"HEXISTS", b"str", b"f"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"HSTRLEN", b"str", b"f"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"HSETNX", b"str", b"f", b"v"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"HDEL", b"str", b"f"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"HINCRBY", b"str", b"f", b"1"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"HINCRBY", b"str", b"f", b"abc"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"HSCAN", b"str", b"0"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"HSCAN", b"str", b"abc"], b"-ERR invalid cursor\r\n"),
    (&[b"GET", b"h"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"MGET", b"h", b"str", b"nokey"], b"*3\r\n$-1\r\n$5\r\nhello\r\n$-1\r\n"),
    (&[b"GETDEL", b"h"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"GETEX", b"h"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"GETEX", b"h", b"EX", b"100"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"INCR", b"h"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"INCRBY", b"h", b"5"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"DECR", b"h"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"DECRBY", b"h", b"5"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SET", b"h", b"x", b"GET"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SET", b"h", b"x", b"NX", b"GET"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"SET", b"h", b"x", b"XX", b"GET"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    (&[b"TYPE", b"h"], b"+hash\r\n"),
    (&[b"SETNX", b"h", b"x"], b":0\r\n"),
    (&[b"TYPE", b"h"], b"+hash\r\n"),
    (&[b"TYPE", b"str"], b"+string\r\n"),
    (&[b"TYPE", b"nokey"], b"+none\r\n"),
    (&[b"SET", b"h2", b"v"], b"+OK\r\n"),
    (&[b"HSET", b"h3", b"a", b"b"], b":1\r\n"),
    (&[b"SET", b"h3", b"v"], b"+OK\r\n"),
    (&[b"TYPE", b"h3"], b"+string\r\n"),
    (&[b"GET", b"h3"], b"$1\r\nv\r\n"),
    (&[b"HSCAN", b"h", b"0"], b"*2\r\n$1\r\n0\r\n*12\r\n$2\r\nf1\r\n$1\r\nx\r\n$2\r\nf2\r\n$2\r\nv2\r\n$2\r\nf3\r\n$2\r\nv3\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$3\r\nnew\r\n$1\r\nz\r\n"),
    (&[b"HSCAN", b"h", b"0", b"MATCH", b"f*"], b"*2\r\n$1\r\n0\r\n*6\r\n$2\r\nf1\r\n$1\r\nx\r\n$2\r\nf2\r\n$2\r\nv2\r\n$2\r\nf3\r\n$2\r\nv3\r\n"),
    (&[b"HSCAN", b"h", b"0", b"MATCH", b"*"], b"*2\r\n$1\r\n0\r\n*12\r\n$2\r\nf1\r\n$1\r\nx\r\n$2\r\nf2\r\n$2\r\nv2\r\n$2\r\nf3\r\n$2\r\nv3\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$3\r\nnew\r\n$1\r\nz\r\n"),
    (&[b"HSCAN", b"h", b"0", b"MATCH", b"F*"], b"*2\r\n$1\r\n0\r\n*0\r\n"),
    (&[b"HSCAN", b"h", b"0", b"MATCH", b"f[12]"], b"*2\r\n$1\r\n0\r\n*4\r\n$2\r\nf1\r\n$1\r\nx\r\n$2\r\nf2\r\n$2\r\nv2\r\n"),
    (&[b"HSCAN", b"h", b"0", b"MATCH", b"f?", b"COUNT", b"1"], b"*2\r\n$1\r\n0\r\n*6\r\n$2\r\nf1\r\n$1\r\nx\r\n$2\r\nf2\r\n$2\r\nv2\r\n$2\r\nf3\r\n$2\r\nv3\r\n"),
    (&[b"HSCAN", b"h", b"0", b"COUNT", b"0"], b"-ERR syntax error\r\n"),
    (&[b"HSCAN", b"h", b"0", b"COUNT", b"-1"], b"-ERR syntax error\r\n"),
    (&[b"HSCAN", b"h", b"0", b"COUNT", b"x"], b"-ERR value is not an integer or out of range\r\n"),
    (&[b"HSCAN", b"h", b"0", b"COUNT"], b"-ERR syntax error\r\n"),
    (&[b"HSCAN", b"h", b"0", b"MATCH"], b"-ERR syntax error\r\n"),
    (&[b"HSCAN", b"h", b"0", b"BADOPT"], b"-ERR syntax error\r\n"),
    (&[b"HSCAN", b"h", b"0", b"TYPE", b"string"], b"-ERR syntax error\r\n"),
    (&[b"HSCAN", b"h", b"abc"], b"-ERR invalid cursor\r\n"),
    (&[b"HSCAN", b"h", b""], b"*2\r\n$1\r\n0\r\n*12\r\n$2\r\nf1\r\n$1\r\nx\r\n$2\r\nf2\r\n$2\r\nv2\r\n$2\r\nf3\r\n$2\r\nv3\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$3\r\nnew\r\n$1\r\nz\r\n"),
    (&[b"HSCAN", b"h", b"-1"], b"*2\r\n$1\r\n0\r\n*12\r\n$2\r\nf1\r\n$1\r\nx\r\n$2\r\nf2\r\n$2\r\nv2\r\n$2\r\nf3\r\n$2\r\nv3\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$3\r\nnew\r\n$1\r\nz\r\n"),
    (&[b"HSCAN", b"h", b"+7"], b"*2\r\n$1\r\n0\r\n*12\r\n$2\r\nf1\r\n$1\r\nx\r\n$2\r\nf2\r\n$2\r\nv2\r\n$2\r\nf3\r\n$2\r\nv3\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$3\r\nnew\r\n$1\r\nz\r\n"),
    (&[b"HSCAN", b"h", b"18446744073709551615"], b"*2\r\n$1\r\n0\r\n*12\r\n$2\r\nf1\r\n$1\r\nx\r\n$2\r\nf2\r\n$2\r\nv2\r\n$2\r\nf3\r\n$2\r\nv3\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$3\r\nnew\r\n$1\r\nz\r\n"),
    (&[b"HSCAN", b"h", b"18446744073709551616"], b"-ERR invalid cursor\r\n"),
    (&[b"HSCAN", b"h", b" 1"], b"-ERR invalid cursor\r\n"),
    (&[b"HSCAN", b"nokey", b"0"], b"*2\r\n$1\r\n0\r\n*0\r\n"),
    (&[b"HSCAN", b"nokey", b"0", b"BADOPT"], b"*2\r\n$1\r\n0\r\n*0\r\n"),
    (&[b"HSCAN", b"nokey", b"abc"], b"-ERR invalid cursor\r\n"),
    (&[b"HSCAN", b"h", b"0", b"MATCH", b"nothing"], b"*2\r\n$1\r\n0\r\n*0\r\n"),
    (&[b"HSET", b"t", b"a", b"1"], b":1\r\n"),
    (&[b"EXPIRE", b"t", b"100"], b":1\r\n"),
    (&[b"HSET", b"t", b"b", b"2"], b":1\r\n"),
    (&[b"TTL", b"t"], b":100\r\n"),
    (&[b"HDEL", b"t", b"a"], b":1\r\n"),
    (&[b"TTL", b"t"], b":100\r\n"),
    (&[b"HINCRBY", b"t", b"n", b"1"], b":1\r\n"),
    (&[b"TTL", b"t"], b":100\r\n"),
    (&[b"PERSIST", b"t"], b":1\r\n"),
    (&[b"TTL", b"t"], b":-1\r\n"),
    (&[b"DEL", b"t"], b":1\r\n"),
    (&[b"DEL", b"t"], b":0\r\n"),
    (&[b"HSET", b"bin", b"", b"", b"\x00\r\n", b"\xff"], b":2\r\n"),
    (&[b"HGETALL", b"bin"], b"*4\r\n$0\r\n\r\n$0\r\n\r\n$3\r\n\x00\r\n\r\n$1\r\n\xff\r\n"),
    (&[b"HGET", b"bin", b""], b"$0\r\n\r\n"),
    (&[b"HSTRLEN", b"bin", b"\x00\r\n"], b":1\r\n"),
    (&[b"HSCAN", b"bin", b"0", b"MATCH", b""], b"*2\r\n$1\r\n0\r\n*2\r\n$0\r\n\r\n$0\r\n\r\n"),
    (&[b"DBSIZE"], b":8\r\n"),
    (&[b"EXISTS", b"h", b"h2", b"h3", b"bin", b"c", b"d", b"fresh", b"nokey"], b":7\r\n"),
    // Redis 7.0.11 reports its own version and id here.
    (&[b"HELLO", b"3"], b"%7\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.2.4\r\n$5\r\nproto\r\n:3\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n"),
    (&[b"HGETALL", b"h"], b"%6\r\n$2\r\nf1\r\n$1\r\nx\r\n$2\r\nf2\r\n$2\r\nv2\r\n$2\r\nf3\r\n$2\r\nv3\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$3\r\nnew\r\n$1\r\nz\r\n"),
    (&[b"HGETALL", b"nokey"], b"%0\r\n"),
    (&[b"HMGET", b"h", b"f1", b"nope"], b"*2\r\n$1\r\nx\r\n_\r\n"),
    (&[b"HGET", b"h", b"nope"], b"_\r\n"),
    (&[b"HKEYS", b"h"], b"*6\r\n$2\r\nf1\r\n$2\r\nf2\r\n$2\r\nf3\r\n$1\r\na\r\n$1\r\nb\r\n$3\r\nnew\r\n"),
    (&[b"HSCAN", b"h", b"0"], b"*2\r\n$1\r\n0\r\n*12\r\n$2\r\nf1\r\n$1\r\nx\r\n$2\r\nf2\r\n$2\r\nv2\r\n$2\r\nf3\r\n$2\r\nv3\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$3\r\nnew\r\n$1\r\nz\r\n"),
    (&[b"HGETALL", b"str"], b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
];

#[tokio::test]
async fn replies_match_redis() {
    let mut c = connect().await;
    for (args, expected) in SCRIPT {
        check(&mut c, &req(args), expected).await;
    }
}

/// Hashes big enough to live outside the segments (over 16 KB) and with
/// enough fields that edits index them, growing and shrinking across both.
#[tokio::test]
async fn big_hashes() {
    let mut c = connect().await;
    let value = |i: usize| format!("{i:0>200}").into_bytes();
    let mut set = vec![&b"HSET"[..], b"big"];
    let fields: Vec<Vec<u8>> = (0..300).map(|i| format!("f{i}").into_bytes()).collect();
    let values: Vec<Vec<u8>> = (0..300).map(value).collect();
    for (f, v) in fields.iter().zip(&values) {
        set.extend([&f[..], v]);
    }
    check(&mut c, &req(&set), b":300\r\n").await;
    check(&mut c, &req(&[b"HLEN", b"big"]), b":300\r\n").await;
    check(
        &mut c,
        &req(&[b"HGET", b"big", b"f299"]),
        &bulk(&value(299)),
    )
    .await;
    // In order: Tantalum keeps insertion order at any size (Redis only for
    // small hashes).
    let mut all = b"*600\r\n".to_vec();
    for (f, v) in fields.iter().zip(&values) {
        all.extend(bulk(f));
        all.extend(bulk(v));
    }
    check(&mut c, &req(&[b"HGETALL", b"big"]), &all).await;
    let mut del = vec![&b"HDEL"[..], b"big"];
    del.extend(fields[..290].iter().map(|f| &f[..]));
    check(&mut c, &req(&del), b":290\r\n").await;
    check(&mut c, &req(&[b"HKEYS", b"big"]), &array(&fields[290..])).await;
    check(&mut c, &req(&[b"HINCRBY", b"big", b"n", b"5"]), b":5\r\n").await;
    check(&mut c, &req(&[b"HINCRBY", b"big", b"n", b"-6"]), b":-1\r\n").await;
    check(&mut c, &req(&[b"HDEL", b"big", b"n"]), b":1\r\n").await;
    check(
        &mut c,
        &req(&[b"HSCAN", b"big", b"0", b"MATCH", b"f29[0-2]", b"NOVALUES"]),
        &[&b"*2\r\n$1\r\n0\r\n"[..], &array(&fields[290..293])].concat(),
    )
    .await;
    del = vec![&b"HDEL"[..], b"big"];
    del.extend(fields[290..].iter().map(|f| &f[..]));
    check(&mut c, &req(&del), b":10\r\n").await;
    check(&mut c, &req(&[b"EXISTS", b"big"]), b":0\r\n").await;
}

fn bulk(b: &[u8]) -> Vec<u8> {
    [format!("${}\r\n", b.len()).as_bytes(), b, b"\r\n"].concat()
}

fn array(items: &[Vec<u8>]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", items.len()).into_bytes();
    for item in items {
        out.extend(bulk(item));
    }
    out
}

type Fields = Vec<(Vec<u8>, Vec<u8>)>;

/// Random hash commands, straight through `cmd::execute`, against a model
/// that keeps each hash's fields in order, reply for reply.
#[test]
fn matches_a_model() {
    use tantalum::cmd::{self, Args, Reply};

    let store = Arc::new(Store::new(4));
    let ctx = Context::new(store, Settings::default());
    let client = ctx.connect(-1).unwrap();
    let mut session = client.session();
    // Each hash's fields and values, in order.
    let mut model: std::collections::HashMap<Vec<u8>, Fields> = Default::default();
    let mut rng = 0x2545_F491_4F6C_DD1Du64;
    let mut below = move |n: usize| {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        (rng % n as u64) as usize
    };
    let mut out = Vec::new();
    for step in 0..40_000 {
        let key = format!("h{}", below(4)).into_bytes();
        // Mostly few fields, now and then enough for big hashes.
        let field_count = if below(20) == 0 { 400 } else { 12 };
        let count = 1 + below(4);
        let fields: Vec<Vec<u8>> = (0..count)
            .map(|_| format!("f{}", below(field_count)).into_bytes())
            .collect();
        let hash = model.entry(key.clone()).or_default();
        let find = |hash: &[(Vec<u8>, Vec<u8>)], f: &[u8]| hash.iter().position(|(g, _)| g == f);
        let (args, expected): (Vec<Vec<u8>>, Vec<u8>) = match below(8) {
            0 | 1 => {
                let mut args = vec![b"HSET".to_vec(), key.clone()];
                let mut added = 0;
                for f in &fields {
                    let v = if below(10) == 0 {
                        vec![b'v'; below(3_000)]
                    } else {
                        below(1_000).to_string().into_bytes()
                    };
                    match find(hash, f) {
                        Some(i) => hash[i].1 = v.clone(),
                        None => {
                            hash.push((f.clone(), v.clone()));
                            added += 1;
                        }
                    }
                    args.extend([f.clone(), v]);
                }
                (args, format!(":{added}\r\n").into_bytes())
            }
            2 => {
                let mut args = vec![b"HDEL".to_vec(), key.clone()];
                let mut removed = 0;
                for f in &fields {
                    if let Some(i) = find(hash, f) {
                        hash.remove(i);
                        removed += 1;
                    }
                    args.push(f.clone());
                }
                (args, format!(":{removed}\r\n").into_bytes())
            }
            3 => {
                let delta = below(2_001) as i64 - 1_000;
                let f = &fields[0];
                let args = vec![
                    b"HINCRBY".to_vec(),
                    key.clone(),
                    f.clone(),
                    delta.to_string().into_bytes(),
                ];
                let old = find(hash, f).map(|i| {
                    std::str::from_utf8(&hash[i].1)
                        .ok()
                        .and_then(|v| v.parse::<i64>().ok())
                });
                let expected = match old {
                    Some(None) => b"-ERR hash value is not an integer\r\n".to_vec(),
                    Some(Some(n)) => {
                        let i = find(hash, f).unwrap();
                        hash[i].1 = (n + delta).to_string().into_bytes();
                        format!(":{}\r\n", n + delta).into_bytes()
                    }
                    None => {
                        hash.push((f.clone(), delta.to_string().into_bytes()));
                        format!(":{delta}\r\n").into_bytes()
                    }
                };
                (args, expected)
            }
            4 => {
                let mut args = vec![b"HMGET".to_vec(), key.clone()];
                let mut expected = format!("*{}\r\n", fields.len()).into_bytes();
                for f in &fields {
                    args.push(f.clone());
                    match find(hash, f) {
                        Some(i) => expected.extend(bulk(&hash[i].1)),
                        None => expected.extend(b"$-1\r\n"),
                    }
                }
                (args, expected)
            }
            5 => {
                let mut expected = format!("*{}\r\n", hash.len() * 2).into_bytes();
                for (f, v) in hash.iter() {
                    expected.extend(bulk(f));
                    expected.extend(bulk(v));
                }
                (vec![b"HGETALL".to_vec(), key.clone()], expected)
            }
            6 => {
                let f = &fields[0];
                let v = b"nx".to_vec();
                let set = find(hash, f).is_none();
                if set {
                    hash.push((f.clone(), v.clone()));
                }
                (
                    vec![b"HSETNX".to_vec(), key.clone(), f.clone(), v],
                    format!(":{}\r\n", u8::from(set)).into_bytes(),
                )
            }
            _ => (
                vec![b"HLEN".to_vec(), key.clone()],
                format!(":{}\r\n", hash.len()).into_bytes(),
            ),
        };
        let mut buf = Vec::new();
        let mut ranges = Vec::new();
        for arg in &args {
            ranges.push(buf.len()..buf.len() + arg.len());
            buf.extend_from_slice(arg);
        }
        out.clear();
        let reply = cmd::execute(&ctx, &mut session, 1, &Args::new(&buf, &ranges), &mut out);
        assert!(matches!(reply, Reply::Written), "step {step}");
        assert_eq!(
            out.escape_ascii().to_string(),
            expected.escape_ascii().to_string(),
            "step {step}: {}",
            args.iter()
                .map(|a| a.escape_ascii().to_string())
                .collect::<Vec<_>>()
                .join(" ")
        );
    }
    // Something got big enough to be external.
    assert!(
        model
            .values()
            .any(|h| h.iter().map(|(f, v)| f.len() + v.len()).sum::<usize>() > 16_384)
    );
}
