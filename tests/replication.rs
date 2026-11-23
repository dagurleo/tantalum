//! Replication end to end: Tantalum masters and replicas in this process, on
//! real sockets, compared key by key. (Against Redis either way round:
//! `tools/repl-diff.py`, which holds the stream a master sends against
//! Redis', and `tools/repl-compare.py`.)

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tantalum::context::{Context, Settings};
use tantalum::server;
use tantalum::store::{Config, Store};

/// A server on its own thread, with active expiry.
struct Server {
    addr: SocketAddr,
    _ctx: Arc<Context>,
}

fn start() -> Server {
    start_with(Config {
        shards: 16,
        ..Config::default()
    })
}

fn start_with(config: Config) -> Server {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let ctx = Arc::new(Context::new(
        Arc::new(Store::with_config(config)),
        Settings {
            debug_command: tantalum::context::DebugCommand::Yes,
            ..Settings::default()
        },
    ));
    let _ = tantalum::cmd::spawn_expirer(&ctx, Duration::from_millis(20));
    let serving = ctx.clone();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            listener.set_nonblocking(true).unwrap();
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            let _ = server::serve(listener, serving).await;
        });
    });
    Server { addr, _ctx: ctx }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Value {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Value>>),
}

impl Value {
    fn text(&self) -> String {
        match self {
            Value::Simple(s) | Value::Error(s) => s.clone(),
            Value::Bulk(Some(b)) => String::from_utf8_lossy(b).into_owned(),
            Value::Int(n) => n.to_string(),
            other => format!("{other:?}"),
        }
    }

    fn items(&self) -> Vec<Value> {
        match self {
            Value::Array(Some(items)) => items.clone(),
            _ => Vec::new(),
        }
    }
}

struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Client {
    fn new(addr: SocketAddr) -> Client {
        let stream = TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        Client {
            reader: BufReader::new(stream.try_clone().unwrap()),
            writer: stream,
        }
    }

    fn call(&mut self, args: &[&[u8]]) -> Value {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            out.extend_from_slice(arg);
            out.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&out).unwrap();
        self.read()
    }

    fn cmd(&mut self, line: &str) -> Value {
        let args: Vec<&[u8]> = line.split(' ').map(str::as_bytes).collect();
        self.call(&args)
    }

    fn read(&mut self) -> Value {
        let mut line = Vec::new();
        self.reader.read_until(b'\n', &mut line).unwrap();
        assert!(line.len() >= 3, "connection closed");
        let text = String::from_utf8_lossy(&line[1..line.len() - 2]).into_owned();
        match line[0] {
            b'+' => Value::Simple(text),
            b'-' => Value::Error(text),
            b':' => Value::Int(text.parse().unwrap()),
            b'$' => {
                let n: i64 = text.parse().unwrap();
                if n < 0 {
                    return Value::Bulk(None);
                }
                let mut data = vec![0; n as usize + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(n as usize);
                Value::Bulk(Some(data))
            }
            b'*' => {
                let n: i64 = text.parse().unwrap();
                if n < 0 {
                    return Value::Array(None);
                }
                Value::Array(Some((0..n).map(|_| self.read()).collect()))
            }
            other => panic!("unexpected reply type {}", other as char),
        }
    }

    /// A field of `INFO`'s.
    fn info(&mut self, field: &str) -> String {
        let info = self.cmd("INFO everything").text();
        info.lines()
            .find_map(|line| line.strip_prefix(&format!("{field}:")))
            .unwrap_or_default()
            .to_string()
    }
}

fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let start = Instant::now();
    while !f() {
        assert!(
            start.elapsed() < Duration::from_secs(15),
            "timed out waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn follow(replica: &Server, master: SocketAddr) {
    let mut c = Client::new(replica.addr);
    let reply = c.cmd(&format!("REPLICAOF {} {}", master.ip(), master.port()));
    assert_eq!(reply, Value::Simple("OK".into()));
}

/// Waits until the replica has run all the master has propagated.
fn synced(master: SocketAddr, replica: SocketAddr) {
    let (mut m, mut r) = (Client::new(master), Client::new(replica));
    wait_until("the replica to catch up", || {
        r.info("master_link_status") == "up"
            && r.info("slave_repl_offset") == m.info("master_repl_offset")
    });
}

/// Every key in every database, with its type, value (in an order that
/// doesn't depend on hashing) and whether it has a TTL.
/// A key as `snapshot` holds it: database, name, type, value, whether it has a TTL.
type Held = (u32, Vec<u8>, String, Vec<Value>, bool);

fn snapshot(addr: SocketAddr) -> Vec<Held> {
    let mut c = Client::new(addr);
    let mut all = Vec::new();
    for db in 0..16u32 {
        c.cmd(&format!("SELECT {db}"));
        let mut keys = Vec::new();
        let mut cursor = b"0".to_vec();
        loop {
            let reply = c.call(&[b"SCAN", &cursor, b"COUNT", b"1000"]).items();
            cursor = match &reply[0] {
                Value::Bulk(Some(c)) => c.clone(),
                other => panic!("{other:?}"),
            };
            for key in reply[1].items() {
                if let Value::Bulk(Some(key)) = key {
                    keys.push(key);
                }
            }
            if cursor == b"0" {
                break;
            }
        }
        keys.sort();
        for key in keys {
            let kind = c.call(&[b"TYPE", &key]).text();
            let mut value = match kind.as_str() {
                "string" => vec![c.call(&[b"GET", &key])],
                "list" => c.call(&[b"LRANGE", &key, b"0", b"-1"]).items(),
                "set" => c.call(&[b"SMEMBERS", &key]).items(),
                "zset" => c
                    .call(&[b"ZRANGE", &key, b"0", b"-1", b"WITHSCORES"])
                    .items(),
                "hash" => {
                    let flat = c.call(&[b"HGETALL", &key]).items();
                    let mut pairs: Vec<Value> = flat
                        .chunks(2)
                        .map(|p| Value::Array(Some(p.to_vec())))
                        .collect();
                    pairs.sort();
                    pairs
                }
                "stream" => {
                    let mut v = c.call(&[b"XRANGE", &key, b"-", b"+"]).items();
                    for group in c.call(&[b"XINFO", b"GROUPS", &key]).items() {
                        let fields = group.items();
                        v.extend(fields[..8].iter().cloned());
                        let name = match &fields[1] {
                            Value::Bulk(Some(name)) => name.clone(),
                            other => panic!("{other:?}"),
                        };
                        v.extend(
                            c.call(&[b"XPENDING", &key, &name, b"-", b"+", b"1000"])
                                .items()
                                .into_iter()
                                // Without the idle time, which runs on.
                                .map(|p| {
                                    Value::Array(Some(vec![
                                        p.items()[0].clone(),
                                        p.items()[1].clone(),
                                        p.items()[3].clone(),
                                    ]))
                                }),
                        );
                    }
                    v
                }
                // Expired since the scan.
                "none" => continue,
                other => panic!("type {other}"),
            };
            if kind == "set" {
                value.sort();
            }
            let pttl = match c.call(&[b"PTTL", &key]) {
                Value::Int(n) => n,
                _ => -2,
            };
            // About to expire, by one clock or the other: not compared.
            if (0..1000).contains(&pttl) {
                continue;
            }
            all.push((db, key, kind, value, pttl >= 0));
        }
    }
    all
}

fn assert_same(master: SocketAddr, replica: SocketAddr) {
    let (m, r) = (snapshot(master), snapshot(replica));
    for (a, b) in m.iter().zip(&r) {
        assert_eq!(a, b, "a key differs");
    }
    assert_eq!(
        m.len(),
        r.len(),
        "the replica has a different number of keys"
    );
}

#[test]
fn a_replica_gets_the_data_then_the_writes() {
    let master = start();
    let replica = start();
    let mut m = Client::new(master.addr);
    for line in [
        "SET s hello",
        "SET n 12",
        "SET ttl v EX 1000",
        "HSET h a 1 b 2",
        "RPUSH l a b c",
        "SADD set x y z",
        "SADD ints 1 2 3",
        "ZADD z 1 a 2 b",
        "PFADD p a b c",
        "XADD x 1-1 f v",
        "XGROUP CREATE x g 0",
        "XREADGROUP GROUP g c STREAMS x >",
        "SELECT 3",
        "SET other db",
        "SELECT 0",
    ] {
        assert!(!matches!(m.cmd(line), Value::Error(_)), "{line}");
    }
    m.call(&[
        b"FUNCTION",
        b"LOAD",
        b"#!lua name=lib\nredis.register_function('f', function(keys, args) return redis.call('set', keys[1], args[1]) end)",
    ]);
    follow(&replica, master.addr);
    synced(master.addr, replica.addr);
    assert_same(master.addr, replica.addr);
    let mut r = Client::new(replica.addr);
    assert_eq!(r.cmd("FUNCTION LIST").items().len(), 1);
    assert_eq!(r.info("role"), "slave");
    assert_eq!(m.info("connected_slaves"), "1");

    // And what comes after, as Redis propagates it.
    for line in [
        "INCR n",
        "INCRBYFLOAT n 1.5",
        "SET ttl2 v PX 100000",
        "EXPIRE s 5000",
        "LPOP l",
        "SPOP set",
        "HINCRBYFLOAT h a 0.5",
        "XADD x * f v2",
        "XREADGROUP GROUP g c2 STREAMS x >",
        "XCLAIM x g c3 0 1-1",
        "EVAL return.redis.call('set','fromscript','1') 0",
        "FCALL f 1 viafunction yes",
        "MULTI",
        "SET in multi",
        "INCR n",
        "EXEC",
        "SELECT 5",
        "SET five 5",
        "SELECT 0",
        "GEOADD g 13.361389 38.115556 Palermo",
        "SETBIT bits 7 1",
        "MOVE s 2",
        "RENAME h h2",
        "COPY l l2",
    ] {
        let line = line.replace("return.redis", "return redis");
        m.cmd(&line);
    }
    synced(master.addr, replica.addr);
    assert_same(master.addr, replica.addr);
}

#[test]
fn a_replica_refuses_writes_and_a_master_without_enough_replicas_too() {
    let master = start();
    let replica = start();
    follow(&replica, master.addr);
    synced(master.addr, replica.addr);
    let mut r = Client::new(replica.addr);
    assert_eq!(
        r.cmd("SET k v"),
        Value::Error("READONLY You can't write against a read only replica.".into())
    );
    assert_eq!(r.cmd("GET k"), Value::Bulk(None));
    let scripted = r.call(&[b"EVAL", b"return redis.call('set','k','v')", b"0"]);
    assert!(
        scripted
            .text()
            .starts_with("READONLY You can't write against a read only replica."),
        "{scripted:?}"
    );
    assert!(matches!(r.cmd("WAIT 1 0"), Value::Error(_)));
    let mut m = Client::new(master.addr);
    m.cmd("CONFIG SET min-replicas-to-write 2");
    assert_eq!(
        m.cmd("SET k v"),
        Value::Error("NOREPLICAS Not enough good replicas to write.".into())
    );
    m.cmd("CONFIG SET min-replicas-to-write 1");
    assert_eq!(m.cmd("SET k v"), Value::Simple("OK".into()));
    m.cmd("CONFIG SET min-replicas-to-write 0");
}

#[test]
fn keys_expire_on_a_replica_when_the_master_says() {
    let master = start();
    let replica = start();
    follow(&replica, master.addr);
    synced(master.addr, replica.addr);
    let mut m = Client::new(master.addr);
    let mut r = Client::new(replica.addr);
    m.cmd("SET gone v PX 50");
    m.cmd("SET kept v");
    synced(master.addr, replica.addr);
    // The master's active expiry removes it, and says so: gone from the
    // replica too, not just hidden.
    wait_until("the key to go from the replica", || {
        r.cmd("DBSIZE") == Value::Int(1)
    });
    assert_eq!(r.cmd("EXISTS gone"), Value::Int(0));
    // An expired key a master's read finds goes too.
    assert_eq!(
        m.cmd("DEBUG SET-ACTIVE-EXPIRE 0"),
        Value::Simple("OK".into())
    );
    m.cmd("SET lazy v PX 20");
    synced(master.addr, replica.addr);
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(r.cmd("GET lazy"), Value::Bulk(None));
    assert_eq!(r.cmd("DBSIZE"), Value::Int(2));
    assert_eq!(m.cmd("GET lazy"), Value::Bulk(None));
    wait_until("the read's DEL", || r.cmd("DBSIZE") == Value::Int(1));
}

#[test]
fn wait_counts_the_replicas_that_have_the_writes() {
    let master = start();
    let replica = start();
    let mut m = Client::new(master.addr);
    assert_eq!(m.cmd("WAIT 1 100"), Value::Int(0));
    follow(&replica, master.addr);
    synced(master.addr, replica.addr);
    m.cmd("SET k v");
    assert_eq!(m.cmd("WAIT 1 0"), Value::Int(1));
    let started = Instant::now();
    assert_eq!(m.cmd("WAIT 2 200"), Value::Int(1));
    assert!(started.elapsed() >= Duration::from_millis(190));
    let role = m.cmd("ROLE").items();
    assert_eq!(role[0].text(), "master");
    assert_eq!(role[2].items().len(), 1);
}

/// A proxy that can cut every connection through it.
struct Proxy {
    addr: SocketAddr,
    cut: Arc<AtomicBool>,
    open: Arc<std::sync::Mutex<Vec<TcpStream>>>,
}

impl Proxy {
    fn new(to: SocketAddr) -> Proxy {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let cut = Arc::new(AtomicBool::new(false));
        let open: Arc<std::sync::Mutex<Vec<TcpStream>>> = Arc::default();
        let (c, o) = (cut.clone(), open.clone());
        std::thread::spawn(move || {
            for client in listener.incoming() {
                let Ok(client) = client else { continue };
                if c.load(Ordering::SeqCst) {
                    continue;
                }
                let server = TcpStream::connect(to).unwrap();
                o.lock().unwrap().push(client.try_clone().unwrap());
                o.lock().unwrap().push(server.try_clone().unwrap());
                for (mut from, mut into) in [
                    (client.try_clone().unwrap(), server.try_clone().unwrap()),
                    (server, client),
                ] {
                    std::thread::spawn(move || {
                        let _ = std::io::copy(&mut from, &mut into);
                        let _ = into.shutdown(std::net::Shutdown::Both);
                    });
                }
            }
        });
        Proxy { addr, cut, open }
    }

    fn cut(&self) {
        self.cut.store(true, Ordering::SeqCst);
        for s in self.open.lock().unwrap().drain(..) {
            let _ = s.shutdown(std::net::Shutdown::Both);
        }
    }

    fn mend(&self) {
        self.cut.store(false, Ordering::SeqCst);
    }
}

#[test]
fn a_replica_that_lost_its_master_continues_where_it_was() {
    let master = start();
    let replica = start();
    let proxy = Proxy::new(master.addr);
    let mut m = Client::new(master.addr);
    m.cmd("SET before v");
    follow(&replica, proxy.addr);
    synced(master.addr, replica.addr);
    assert_eq!(m.info("sync_full"), "1");
    proxy.cut();
    let mut r = Client::new(replica.addr);
    wait_until("the link to go down", || {
        r.info("master_link_status") == "down"
    });
    for i in 0..100 {
        m.cmd(&format!("RPUSH during {i}"));
    }
    proxy.mend();
    synced(master.addr, replica.addr);
    assert_eq!(m.info("sync_partial_ok"), "1");
    assert_eq!(m.info("sync_full"), "1");
    assert_same(master.addr, replica.addr);
}

#[test]
fn a_promoted_replica_lets_the_old_master_continue_from_it() {
    let a = start();
    let b = start();
    let mut ca = Client::new(a.addr);
    let mut cb = Client::new(b.addr);
    ca.cmd("SET k 1");
    follow(&b, a.addr);
    synced(a.addr, b.addr);
    let old_id = ca.info("master_replid");
    // Failover: B takes over, A follows it.
    assert_eq!(cb.cmd("REPLICAOF NO ONE"), Value::Simple("OK".into()));
    assert_eq!(cb.info("role"), "master");
    assert_eq!(cb.info("master_replid2"), old_id);
    assert_ne!(cb.info("master_replid"), old_id);
    assert_eq!(cb.cmd("SET k 2"), Value::Simple("OK".into()));
    follow(&a, b.addr);
    synced(b.addr, a.addr);
    assert_eq!(ca.cmd("GET k"), Value::Bulk(Some(b"2".to_vec())));
    assert_eq!(
        cb.info("sync_partial_ok"),
        "1",
        "A continued, without a full sync"
    );
    assert_eq!(cb.info("sync_full"), "0");
}

#[test]
fn a_replica_of_a_replica_gets_the_same_stream() {
    let a = start();
    let b = start();
    let c = start();
    follow(&b, a.addr);
    follow(&c, b.addr);
    synced(a.addr, b.addr);
    synced(b.addr, c.addr);
    let mut ca = Client::new(a.addr);
    for i in 0..50 {
        ca.cmd(&format!("HSET h f{i} {i}"));
    }
    ca.cmd("SET t v EX 100");
    synced(a.addr, b.addr);
    synced(b.addr, c.addr);
    assert_same(a.addr, c.addr);
    let mut cc = Client::new(c.addr);
    assert_eq!(cc.info("master_repl_offset"), ca.info("master_repl_offset"));
    assert_eq!(cc.info("master_replid"), ca.info("master_replid"));
}

#[test]
fn the_append_only_log_and_replicas_together() {
    let dir = std::env::temp_dir().join(format!("tantalum-repl-aof-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let config = Config {
        shards: 16,
        unix_clock: true,
        ..Config::default()
    };
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let ctx = Arc::new(Context::new(
        Arc::new(Store::with_config(config)),
        Settings::default(),
    ));
    let (aof, _) = tantalum::aof::start(
        &ctx,
        tantalum::aof::Options {
            dir: dir.clone(),
            fsync: tantalum::aof::Fsync::EverySec,
            rewrite_percentage: 100,
            rewrite_min_size: 64 << 20,
        },
    )
    .unwrap();
    ctx.set_aof(aof.clone());
    let serving = ctx.clone();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            listener.set_nonblocking(true).unwrap();
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            let _ = server::serve(listener, serving).await;
        });
    });
    let replica = start();
    let mut m = Client::new(addr);
    m.cmd("SET a 1");
    follow(&replica, addr);
    synced(addr, replica.addr);
    for i in 0..200 {
        m.cmd(&format!("INCR a{}", i % 10));
        m.cmd(&format!("ZADD z {i} m{}", i % 17));
    }
    m.cmd("BGREWRITEAOF");
    m.cmd("SET after rewrite");
    synced(addr, replica.addr);
    assert_same(addr, replica.addr);
    aof.flush().unwrap();
    // What the log has loads as the replica has it.
    let reloaded = Arc::new(Context::new(
        Arc::new(Store::with_config(Config {
            shards: 16,
            unix_clock: true,
            ..Config::default()
        })),
        Settings::default(),
    ));
    let (again, _) = tantalum::aof::start(
        &reloaded,
        tantalum::aof::Options {
            dir: dir.clone(),
            fsync: tantalum::aof::Fsync::EverySec,
            rewrite_percentage: 100,
            rewrite_min_size: 64 << 20,
        },
    )
    .unwrap();
    again.stop();
    assert_eq!(
        reloaded.store.len(),
        snapshot(replica.addr).len(),
        "the log reloads the keys the replica has"
    );
    aof.stop();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A tiny random number generator, so a failure can be run again.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// A random write, of every kind, on a few keys of each type (so commands
/// meet each other's keys, wrong types included).
fn random_command(rng: &mut Rng) -> String {
    let k = rng.below(6);
    let n = rng.below(100);
    let commands = [
        format!("SET s{k} v{n}"),
        format!("SET s{k} v{n} PX {}", 1 + rng.below(300)),
        format!("SET s{k} v{n} EX 1000 GET"),
        format!("GETSET s{k} g{n}"),
        format!("INCRBY i{k} {n}"),
        format!("INCRBYFLOAT f{k} 0.{n}"),
        format!("APPEND s{k} a{n}"),
        format!("SETRANGE s{k} {} x", rng.below(5)),
        format!("DEL s{k} i{k}"),
        format!("EXPIRE s{k} {}", rng.below(2) * 1000),
        format!("PEXPIRE i{k} {}", 1 + rng.below(200)),
        format!("PERSIST s{k}"),
        format!("GETDEL s{k}"),
        format!("GETEX s{k} PX {}", 1 + rng.below(500)),
        format!("HSET h{k} f{} {n}", rng.below(10)),
        format!("HDEL h{k} f{}", rng.below(10)),
        format!("HINCRBY h{k} c {n}"),
        format!("HINCRBYFLOAT h{k} d 0.{n}"),
        format!("LPUSH l{k} {n} {}", n + 1),
        format!("RPUSH l{k} {n}"),
        format!("LPOP l{k} {}", 1 + rng.below(3)),
        format!("RPOP l{k}"),
        format!("LTRIM l{k} 0 {}", rng.below(5)),
        format!("LREM l{k} 0 {n}"),
        format!("LMOVE l{k} l{} LEFT RIGHT", rng.below(6)),
        format!("BLPOP l{k} 0.01"),
        format!("LMPOP 2 l{k} l{} RIGHT COUNT 2", rng.below(6)),
        format!("SADD t{k} {n} m{}", rng.below(5)),
        format!("SREM t{k} {n}"),
        format!("SPOP t{k}"),
        format!("SPOP t{k} {}", 1 + rng.below(4)),
        format!("SMOVE t{k} t{} {n}", rng.below(6)),
        format!("SINTERSTORE t{k} t{} t{}", rng.below(6), rng.below(6)),
        format!("SUNIONSTORE t{k} t{} t{}", rng.below(6), rng.below(6)),
        format!("ZADD z{k} {n} m{}", rng.below(10)),
        format!("ZINCRBY z{k} 1.5 m{}", rng.below(10)),
        format!("ZREM z{k} m{}", rng.below(10)),
        format!("ZPOPMIN z{k} {}", 1 + rng.below(2)),
        format!("ZREMRANGEBYRANK z{k} 0 0"),
        format!("ZUNIONSTORE z{k} 2 z{} z{}", rng.below(6), rng.below(6)),
        format!("ZMPOP 1 z{k} MAX COUNT 2"),
        format!("BZPOPMAX z{k} 0.01"),
        format!("XADD x{k} * f {n}"),
        format!("XADD x{k} MAXLEN ~ 5 * f {n}"),
        format!("XTRIM x{k} MAXLEN {}", rng.below(8)),
        format!("XDEL x{k} 0-1"),
        format!("XGROUP CREATE x{k} g{} 0 MKSTREAM", rng.below(2)),
        format!(
            "XREADGROUP GROUP g{} c{} COUNT 2 STREAMS x{k} >",
            rng.below(2),
            rng.below(3)
        ),
        format!("XREADGROUP GROUP g0 c{} NOACK STREAMS x{k} >", rng.below(3)),
        format!(
            "XAUTOCLAIM x{k} g{} c{} 0 0 COUNT 3",
            rng.below(2),
            rng.below(3)
        ),
        format!("PFADD p{k} {n}"),
        format!("PFMERGE p{k} p{}", rng.below(6)),
        format!("SETBIT b{k} {} 1", rng.below(64)),
        format!("BITOP OR b{k} b{} b{}", rng.below(6), rng.below(6)),
        format!("GEOADD g{k} 13.{n} 38.{n} p{}", rng.below(5)),
        format!("RENAME s{k} s{}", rng.below(6)),
        format!("COPY h{k} h{} REPLACE", rng.below(6)),
        format!("MOVE s{k} 1"),
        format!("SELECT {}", rng.below(2)),
        "EVAL redis.call('incr','i0');redis.call('rpush','l0',ARGV[1]) 0 x".to_string(),
        format!("EVAL redis.call('spop','t{k}');return.1 0"),
        "MSET s0 a s1 b".to_string(),
        "MULTI".to_string(),
        "EXEC".to_string(),
        format!("RESTORE r{k} {} PAYLOAD REPLACE", rng.below(2) * 1000),
    ];
    commands[rng.below(commands.len() as u64) as usize].clone()
}

thread_local! {
    static PAIR: std::cell::Cell<Option<(SocketAddr, SocketAddr)>> = const { std::cell::Cell::new(None) };
}

/// For STEP=1: whether master and replica differ now (once the replica follows).
fn step_check(_: &mut Client) -> bool {
    let Some((master, replica)) = PAIR.get() else {
        return false;
    };
    if Client::new(replica).info("master_link_status") != "up" {
        return false;
    }
    synced(master, replica);
    let (m, r) = (snapshot(master), snapshot(replica));
    for (a, b) in m.iter().zip(&r) {
        if a != b {
            eprintln!("master  {a:?}\nreplica {b:?}");
            return true;
        }
    }
    m.len() != r.len()
}

#[test]
fn a_replica_has_what_its_master_has_after_random_writes() {
    let master = start_with(Config {
        shards: 16,
        maxmemory: 0,
        ..Config::default()
    });
    let replica = start();
    let proxy = Proxy::new(master.addr);
    let mut m = Client::new(master.addr);
    let seed = std::env::var("SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x5eed_1234_u64);
    let mut rng = Rng(seed);
    let mut in_multi = false;
    let mut run = |m: &mut Client, rng: &mut Rng, count: usize| {
        for _ in 0..count {
            let line = random_command(rng)
                .replace("return.1", "return 1")
                .replace(";", " ");
            let args: Vec<Vec<u8>> = if let Some(rest) = line.strip_prefix("EVAL ") {
                // The script is one argument.
                let (script, tail) = rest.rsplit_once(" 0").unwrap();
                let mut args = vec![b"EVAL".to_vec(), script.as_bytes().to_vec(), b"0".to_vec()];
                args.extend(tail.split_whitespace().map(|a| a.as_bytes().to_vec()));
                args
            } else {
                line.split(' ').map(|a| a.as_bytes().to_vec()).collect()
            };
            if line == "MULTI" {
                if in_multi {
                    continue;
                }
                in_multi = true;
            }
            if line == "EXEC" {
                if !in_multi {
                    continue;
                }
                in_multi = false;
            }
            // A DUMP of "v", as Redis 7.2 makes it.
            let args: Vec<Vec<u8>> = args
                .into_iter()
                .map(|a| {
                    if a == b"PAYLOAD" {
                        b"\x00\x01v\x0b\x00\xf8\xd7s\xc1\xe4\xa7cG".to_vec()
                    } else {
                        a
                    }
                })
                .collect();
            let refs: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
            m.call(&refs);
            // STEP=1: compare after every command, to find the first that differs.
            if std::env::var("STEP").is_ok() && !in_multi && step_check(m) {
                panic!("differs after {line}");
            }
        }
        if in_multi {
            m.cmd("EXEC");
            in_multi = false;
        }
    };
    run(&mut m, &mut rng, 1500);
    follow(&replica, proxy.addr);
    synced(master.addr, replica.addr);
    PAIR.set(Some((master.addr, replica.addr)));
    run(&mut m, &mut rng, 1500);
    synced(master.addr, replica.addr);
    // A cut in the middle: it continues.
    let cut = std::env::var("NOCUT").is_err();
    if cut {
        proxy.cut();
    }
    run(&mut m, &mut rng, 500);
    proxy.mend();
    run(&mut m, &mut rng, 1500);
    m.cmd("SELECT 0");
    // Keys with short TTLs have gone by now, on both.
    std::thread::sleep(Duration::from_millis(1000));
    synced(master.addr, replica.addr);
    assert_same(master.addr, replica.addr);
    assert_eq!(m.info("sync_full"), "1");
    if cut {
        assert_eq!(m.info("sync_partial_ok"), "1");
    }
}
