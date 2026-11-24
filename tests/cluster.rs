//! Cluster mode end to end: six nodes of the built binary, each its own
//! process, made a cluster with the commands `redis-cli --cluster create`
//! sends (`CLUSTER MEET`, `ADDSLOTSRANGE`, `REPLICATE`). (With Redis nodes
//! among them, and `redis-cli --cluster` itself: see "Cluster" in the
//! README.)

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Procs(Vec<(u16, Child)>);

impl Procs {
    fn kill(&mut self, port: u16) {
        if let Some(at) = self.0.iter().position(|(p, _)| *p == port) {
            let (_, mut child) = self.0.remove(at);
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for Procs {
    fn drop(&mut self) {
        for (_, child) in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// A port to listen on, below the range outgoing connections take theirs
/// from (32768 and up, on Linux), so that a node started again on its old
/// port finds it free.
fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    let base = 20_000 + (std::process::id() % 500) as u16 * 20;
    loop {
        let port = base + NEXT.fetch_add(1, Ordering::Relaxed) % 2000;
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
enum Value {
    Text(String),
    Error(String),
    Int(i64),
    Nil,
    Array(Vec<Value>),
}

impl Value {
    fn text(&self) -> String {
        match self {
            Value::Text(s) | Value::Error(s) => s.clone(),
            Value::Int(n) => n.to_string(),
            _ => String::new(),
        }
    }
}

struct Client {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Client {
    fn new(port: u16) -> Option<Client> {
        let stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        Some(Client { stream, reader })
    }

    fn call(&mut self, words: &[&str]) -> Value {
        let mut out = format!("*{}\r\n", words.len());
        for w in words {
            out.push_str(&format!("${}\r\n{w}\r\n", w.len()));
        }
        self.stream.write_all(out.as_bytes()).unwrap();
        self.read()
    }

    fn cmd(&mut self, line: &str) -> Value {
        let words: Vec<&str> = line.split(' ').collect();
        self.call(&words)
    }

    fn read(&mut self) -> Value {
        let mut line = String::new();
        if self.reader.read_line(&mut line).unwrap_or(0) == 0 {
            return Value::Nil;
        }
        let line = line.trim_end();
        let rest = &line[1..];
        match &line[..1] {
            "+" => Value::Text(rest.to_string()),
            "-" => Value::Error(rest.to_string()),
            ":" => Value::Int(rest.parse().unwrap()),
            "$" => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    return Value::Nil;
                }
                let mut buf = vec![0u8; n as usize + 2];
                std::io::Read::read_exact(&mut self.reader, &mut buf).unwrap();
                Value::Text(String::from_utf8_lossy(&buf[..n as usize]).into_owned())
            }
            "*" => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    return Value::Nil;
                }
                Value::Array((0..n).map(|_| self.read()).collect())
            }
            other => panic!("unexpected reply {other}{rest}"),
        }
    }
}

fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let start = Instant::now();
    while !f() {
        assert!(
            start.elapsed() < Duration::from_secs(40),
            "timed out waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn connect(port: u16) -> Client {
    let mut c = None;
    wait_until("the node to listen", || {
        c = Client::new(port);
        c.is_some()
    });
    c.unwrap()
}

/// A line of `CLUSTER INFO`, on a connection of its own.
fn info(port: u16, field: &str) -> String {
    Client::new(port).map_or_else(String::new, |mut c| {
        c.cmd("CLUSTER INFO")
            .text()
            .lines()
            .find_map(|l| l.strip_prefix(&format!("{field}:")).map(str::to_string))
            .unwrap_or_default()
    })
}

struct Node {
    port: u16,
    cport: u16,
}

fn spawn(dir: &std::path::Path, node: &Node) -> Child {
    let conf = dir.join(format!("nodes-{}.conf", node.port));
    Command::new(env!("CARGO_BIN_EXE_tantalum"))
        .args([
            "--port",
            &node.port.to_string(),
            "--threads",
            "1",
            "--cluster-enabled",
            "yes",
            "--cluster-port",
            &node.cport.to_string(),
            "--cluster-config-file",
            conf.to_str().unwrap(),
            "--cluster-node-timeout",
            "1000",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

/// Follows `-MOVED` (and `-ASK`) from any node, as a cluster client does.
fn run(start: u16, words: &[&str]) -> Value {
    let mut port = start;
    let mut asking = false;
    for _ in 0..5 {
        let mut c = connect(port);
        if asking {
            c.cmd("ASKING");
        }
        match c.call(words) {
            Value::Error(e) if e.starts_with("MOVED") || e.starts_with("ASK") => {
                asking = e.starts_with("ASK");
                port = e.rsplit(':').next().unwrap().parse().unwrap();
            }
            other => return other,
        }
    }
    panic!("redirected too often: {words:?}")
}

#[test]
fn a_cluster_serves_moves_slots_and_fails_over() {
    let dir = std::env::temp_dir().join(format!("tantalum-cluster-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let nodes: Vec<Node> = (0..6)
        .map(|_| Node {
            port: free_port(),
            cport: free_port(),
        })
        .collect();
    let mut procs = Procs(nodes.iter().map(|n| (n.port, spawn(&dir, n))).collect());
    let ports: Vec<u16> = nodes.iter().map(|n| n.port).collect();
    let mut first = connect(ports[0]);
    for n in &nodes[1..] {
        let reply = first.cmd(&format!("CLUSTER MEET 127.0.0.1 {} {}", n.port, n.cport));
        assert_eq!(reply, Value::Text("OK".into()));
    }
    for (i, range) in ["0 5460", "5461 10922", "10923 16383"].iter().enumerate() {
        let reply = connect(ports[i]).cmd(&format!("CLUSTER ADDSLOTSRANGE {range}"));
        assert_eq!(reply, Value::Text("OK".into()));
    }
    for &p in &ports {
        wait_until("every node to know the others", || {
            info(p, "cluster_known_nodes") == "6"
        });
    }
    let ids: Vec<String> = ports
        .iter()
        .map(|&p| connect(p).cmd("CLUSTER MYID").text())
        .collect();
    for i in 0..3 {
        let reply = connect(ports[i + 3]).cmd(&format!("CLUSTER REPLICATE {}", ids[i]));
        assert_eq!(reply, Value::Text("OK".into()), "replica {i}");
    }
    for &p in &ports {
        wait_until("the cluster to be ok", || info(p, "cluster_state") == "ok");
    }
    // Redis' answers for keys elsewhere.
    let mut c0 = connect(ports[0]);
    assert_eq!(c0.cmd("CLUSTER KEYSLOT foo"), Value::Int(12182));
    assert_eq!(
        c0.cmd("SET foo bar"),
        Value::Error(format!("MOVED 12182 127.0.0.1:{}", ports[2]))
    );
    assert_eq!(
        c0.cmd("MSET a 1 b 2"),
        Value::Error("CROSSSLOT Keys in request don't hash to the same slot".into())
    );
    assert_eq!(
        c0.cmd("SELECT 1"),
        Value::Error("ERR SELECT is not allowed in cluster mode".into())
    );
    for i in 0..300 {
        assert_eq!(
            run(ports[0], &["SET", &format!("k{i}"), &i.to_string()]),
            Value::Text("OK".into())
        );
    }
    assert_eq!(
        run(ports[1], &["MSET", "{t}a", "1", "{t}b", "2"]),
        Value::Text("OK".into())
    );
    // A replica answers reads after READONLY.
    let slot = c0.cmd("CLUSTER KEYSLOT k1").text().parse::<u16>().unwrap();
    let owner = (0..3).find(|&i| {
        let ranges = [(0, 5460), (5461, 10922), (10923, 16383)];
        (ranges[i].0..=ranges[i].1).contains(&slot)
    });
    let replica = ports[owner.unwrap() + 3];
    wait_until("the replica to have the key", || {
        let mut r = connect(replica);
        r.cmd("READONLY");
        r.cmd("GET k1") == Value::Text("1".into())
    });

    // Moving a slot with keys, as `redis-cli --cluster reshard` does it.
    let slot = c0.cmd("CLUSTER KEYSLOT {t}a").text();
    let from = (0..3)
        .find(|&i| connect(ports[i]).cmd("GET {t}a") == Value::Text("1".into()))
        .expect("an owner");
    let to = (from + 1) % 3;
    let (mut src, mut dst) = (connect(ports[from]), connect(ports[to]));
    assert_eq!(
        dst.cmd(&format!("CLUSTER SETSLOT {slot} IMPORTING {}", ids[from])),
        Value::Text("OK".into())
    );
    assert_eq!(
        src.cmd(&format!("CLUSTER SETSLOT {slot} MIGRATING {}", ids[to])),
        Value::Text("OK".into())
    );
    assert_eq!(
        src.cmd(&format!("CLUSTER COUNTKEYSINSLOT {slot}")),
        Value::Int(2)
    );
    let keys = src.cmd(&format!("CLUSTER GETKEYSINSLOT {slot} 10"));
    let Value::Array(keys) = keys else {
        panic!("{keys:?}")
    };
    assert_eq!(keys.len(), 2);
    // A key not there any more is asked for at the target, meanwhile.
    let moved = src.call(&[
        "MIGRATE",
        "127.0.0.1",
        &ports[to].to_string(),
        "{t}a",
        "0",
        "5000",
    ]);
    assert_eq!(moved, Value::Text("OK".into()));
    assert_eq!(
        src.cmd("GET {t}a"),
        Value::Error(format!("ASK {slot} 127.0.0.1:{}", ports[to]))
    );
    assert_eq!(
        src.cmd("MGET {t}a {t}b"),
        Value::Error("TRYAGAIN Multiple keys request during rehashing of slot".into())
    );
    let moved = src.call(&[
        "MIGRATE",
        "127.0.0.1",
        &ports[to].to_string(),
        "",
        "0",
        "5000",
        "KEYS",
        "{t}b",
    ]);
    assert_eq!(moved, Value::Text("OK".into()));
    for p in [ports[to], ports[from]] {
        assert_eq!(
            connect(p).cmd(&format!("CLUSTER SETSLOT {slot} NODE {}", ids[to])),
            Value::Text("OK".into())
        );
    }
    wait_until("every node to know where the slot went", || {
        (0..3).all(|i| {
            let r = connect(ports[i]).cmd("GET {t}b");
            r == Value::Text("2".into())
                || r == Value::Error(format!("MOVED {slot} 127.0.0.1:{}", ports[to]))
        })
    });
    assert_eq!(run(ports[from], &["GET", "{t}a"]), Value::Text("1".into()));

    // Pub/sub reaches every node's subscribers.
    let mut sub = connect(ports[5]);
    sub.cmd("SUBSCRIBE news");
    assert_eq!(connect(ports[0]).cmd("PUBLISH news hello"), Value::Int(0));
    assert_eq!(
        sub.read(),
        Value::Array(vec![
            Value::Text("message".into()),
            Value::Text("news".into()),
            Value::Text("hello".into())
        ])
    );

    // A master gone: its replica takes over, and nothing's lost.
    let victim = owner.unwrap();
    procs.kill(ports[victim]);
    let promoted = ports[victim + 3];
    wait_until("the replica to take over", || {
        Client::new(promoted).is_some_and(|mut c| {
            matches!(c.cmd("ROLE"), Value::Array(r) if r.first() == Some(&Value::Text("master".into())))
        })
    });
    let live = ports[(victim + 1) % 3];
    wait_until("the cluster to be ok again", || {
        info(live, "cluster_state") == "ok"
    });
    for i in 0..300 {
        assert_eq!(
            run(live, &["GET", &format!("k{i}")]),
            Value::Text(i.to_string()),
            "k{i}"
        );
    }
    // Back from its nodes.conf, the old master is a replica of the new one.
    procs.0.push((ports[victim], spawn(&dir, &nodes[victim])));
    wait_until("the old master to follow the new one", || {
        Client::new(ports[victim]).is_some_and(|mut c| {
            matches!(c.cmd("ROLE"), Value::Array(r) if r.first() == Some(&Value::Text("slave".into()))
                && r.get(2) == Some(&Value::Int(i64::from(promoted))))
        })
    });
    drop(procs);
    let _ = std::fs::remove_dir_all(&dir);
}
