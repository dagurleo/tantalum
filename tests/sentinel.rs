//! Sentinel end to end: Tantalum servers and three Tantalum Sentinels, each
//! its own process of the built binary, on real sockets. (With Redis'
//! Sentinels among them, and Redis' watching Tantalum's servers: see
//! "Sentinel" in the README.)

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Processes killed when the test ends, however it ends.
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

fn spawn(args: &[&str]) -> Child {
    Command::new(env!("CARGO_BIN_EXE_tantalum"))
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

fn server(port: u16, master: Option<u16>) -> Child {
    let port = port.to_string();
    match master {
        None => spawn(&["--port", &port, "--threads", "1"]),
        Some(m) => spawn(&[
            "--port",
            &port,
            "--threads",
            "1",
            "--replicaof",
            &format!("127.0.0.1 {m}"),
        ]),
    }
}

struct Client {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

#[derive(Debug, PartialEq)]
enum Value {
    Text(String),
    Int(i64),
    Nil,
    Array(Vec<Value>),
}

impl Value {
    fn text(&self) -> String {
        match self {
            Value::Text(s) => s.clone(),
            Value::Int(n) => n.to_string(),
            _ => String::new(),
        }
    }
}

impl Client {
    fn new(port: u16) -> Option<Client> {
        let stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        Some(Client { stream, reader })
    }

    fn cmd(&mut self, line: &str) -> Value {
        let words: Vec<&str> = line.split(' ').collect();
        let mut out = format!("*{}\r\n", words.len());
        for w in words {
            out.push_str(&format!("${}\r\n{w}\r\n", w.len()));
        }
        self.stream.write_all(out.as_bytes()).unwrap();
        self.read()
    }

    fn read(&mut self) -> Value {
        let mut line = String::new();
        // A connection Sentinel closed (`CLIENT KILL`, as it reconfigures).
        if self.reader.read_line(&mut line).unwrap_or(0) == 0 {
            return Value::Nil;
        }
        let line = line.trim_end();
        let rest = &line[1..];
        match &line[..1] {
            "+" | "-" => Value::Text(rest.to_string()),
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

    /// A field of `INFO`, or of a `SENTINEL MASTER` reply.
    fn field(&mut self, command: &str, name: &str) -> String {
        match self.cmd(command) {
            Value::Text(info) => info
                .lines()
                .find_map(|l| l.strip_prefix(&format!("{name}:")).map(str::to_string))
                .unwrap_or_default(),
            Value::Array(items) => items
                .chunks(2)
                .find(|kv| kv[0].text() == name)
                .map(|kv| kv[1].text())
                .unwrap_or_default(),
            _ => String::new(),
        }
    }
}

fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let start = Instant::now();
    while !f() {
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "timed out waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A field of a server's `INFO`, on a connection of its own: Sentinel
/// disconnects a server's clients when it reconfigures it.
fn info(port: u16, name: &str) -> String {
    Client::new(port).map_or_else(String::new, |mut c| c.field("INFO replication", name))
}

fn connect(port: u16) -> Client {
    let mut client = None;
    wait_until("the server to listen", || {
        client = Client::new(port);
        client.is_some()
    });
    client.unwrap()
}

#[test]
fn sentinels_fail_a_master_over_and_bring_it_back_as_a_replica() {
    let dir = std::env::temp_dir().join(format!("tantalum-sentinel-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let ports: Vec<u16> = (0..3).map(|_| free_port()).collect();
    let (m, r1, r2) = (ports[0], ports[1], ports[2]);
    let mut procs = Procs(vec![(m, server(m, None))]);
    let mut master = connect(m);
    for i in 0..50 {
        master.cmd(&format!("SET k{i} {i}"));
    }
    procs.0.push((r1, server(r1, Some(m))));
    procs.0.push((r2, server(r2, Some(m))));
    let sentinels: Vec<u16> = (0..3).map(|_| free_port()).collect();
    let mut confs: Vec<PathBuf> = Vec::new();
    for &s in &sentinels {
        let conf = dir.join(format!("sentinel-{s}.conf"));
        std::fs::write(
            &conf,
            format!(
                "port {s}\nsentinel monitor m 127.0.0.1 {m} 2\n\
                 sentinel down-after-milliseconds m 500\nsentinel failover-timeout m 3000\n"
            ),
        )
        .unwrap();
        let log = std::fs::File::create(dir.join(format!("sentinel-{s}.log"))).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_tantalum"))
            .args(["--sentinel", conf.to_str().unwrap()])
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap();
        procs.0.push((s, child));
        confs.push(conf);
    }
    let mut clients: Vec<Client> = sentinels.iter().map(|&s| connect(s)).collect();
    for c in &mut clients {
        wait_until("each Sentinel to know the others and the replicas", || {
            c.field("SENTINEL MASTER m", "num-other-sentinels") == "2"
                && c.field("SENTINEL MASTER m", "num-slaves") == "2"
        });
    }
    assert_eq!(
        clients[0].cmd("SENTINEL GET-MASTER-ADDR-BY-NAME m"),
        Value::Array(vec![
            Value::Text("127.0.0.1".into()),
            Value::Text(m.to_string())
        ])
    );
    assert!(
        clients[0]
            .cmd("SENTINEL CKQUORUM m")
            .text()
            .starts_with("OK 3 usable Sentinels")
    );
    // Clients learn of the new master from the event.
    let mut events = connect(sentinels[1]);
    events.cmd("SUBSCRIBE +switch-master");
    procs.kill(m);
    let switched = events.read();
    let Value::Array(message) = switched else {
        panic!("{switched:?}")
    };
    let words: Vec<String> = message[2].text().split(' ').map(str::to_string).collect();
    assert_eq!(&words[..3], &["m", "127.0.0.1", &m.to_string()]);
    let new: u16 = words[4].parse().unwrap();
    assert!(new == r1 || new == r2, "{words:?}");
    for c in &mut clients {
        wait_until("every Sentinel to name the new master", || {
            c.cmd("SENTINEL GET-MASTER-ADDR-BY-NAME m")
                == Value::Array(vec![
                    Value::Text("127.0.0.1".into()),
                    Value::Text(new.to_string()),
                ])
        });
    }
    let other = if new == r1 { r2 } else { r1 };
    assert_eq!(info(new, "role"), "master");
    wait_until("the other replica to follow the new master", || {
        info(other, "master_port") == new.to_string() && info(other, "master_link_status") == "up"
    });
    wait_until("the write on the new master", || {
        Client::new(new)
            .is_some_and(|mut c| c.cmd("SET after failover") == Value::Text("OK".into()))
    });
    // The old master, back, is made a replica of the new one.
    procs.0.push((m, server(m, None)));
    wait_until("the old master to follow the new one", || {
        info(m, "master_port") == new.to_string() && info(m, "master_link_status") == "up"
    });
    wait_until("the old master to have the new write", || {
        Client::new(m).is_some_and(|mut c| c.cmd("GET after") == Value::Text("failover".into()))
    });
    assert_eq!(connect(m).cmd("GET k7"), Value::Text("7".into()));
    // What the Sentinels learned is in their files.
    let conf = std::fs::read_to_string(&confs[0]).unwrap();
    assert!(
        conf.contains(&format!("sentinel monitor m 127.0.0.1 {new} 2")),
        "{conf}"
    );
    assert!(
        conf.contains("sentinel known-replica m 127.0.0.1"),
        "{conf}"
    );
    assert!(
        conf.contains("sentinel known-sentinel m 127.0.0.1"),
        "{conf}"
    );
    assert!(!conf.contains("sentinel current-epoch 0"), "{conf}");
    drop(procs);
    let _ = std::fs::remove_dir_all(&dir);
}
