//! Snapshots across real restarts of the `tantalum` binary.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Server {
    child: Child,
    port: u16,
    log: std::sync::mpsc::Receiver<String>,
}

/// A test that fails mid-way mustn't leave its server running (it would
/// keep the test runner's output open).
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("tantalum-persist-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Starts the binary with `args`, collecting its stderr lines.
fn start(snapshot: &Path, args: &[&str]) -> Server {
    let port = free_port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_tantalum"))
        .args(["--port", &port.to_string(), "--threads", "2", "--snapshot"])
        .arg(snapshot)
        .args(args)
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stderr = child.stderr.take().unwrap();
    let (tx, log) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            let _ = tx.send(line);
        }
    });
    Server { child, port, log }
}

impl Server {
    /// Waits for a log line containing `text`; returns it.
    fn expect_log(&self, text: &str) -> String {
        let deadline = Instant::now() + Duration::from_secs(10);
        while let Some(left) = deadline.checked_duration_since(Instant::now()) {
            match self.log.recv_timeout(left) {
                Ok(line) if line.contains(text) => return line,
                Ok(_) => {}
                Err(_) => break,
            }
        }
        panic!("no log line containing {text:?}");
    }

    fn connect(&self) -> TcpStream {
        for _ in 0..100 {
            if let Ok(s) = TcpStream::connect(("127.0.0.1", self.port)) {
                s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
                return s;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!("server never came up");
    }

    /// Sends SIGTERM and waits for a clean exit.
    fn stop(mut self) {
        // SAFETY: kill only sends a signal to our own child.
        unsafe { libc::kill(self.child.id() as i32, libc::SIGTERM) };
        let status = self.child.wait().unwrap();
        assert!(status.success(), "{status}");
    }
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

fn call(c: &mut TcpStream, args: &[&[u8]], expected: &[u8]) {
    c.write_all(&req(args)).unwrap();
    let mut got = vec![0; expected.len()];
    c.read_exact(&mut got).unwrap();
    assert_eq!(
        got.escape_ascii().to_string(),
        expected.escape_ascii().to_string()
    );
}

#[test]
fn data_survives_a_restart() {
    let dir = temp_dir("restart");
    let snapshot = dir.join("data.snapshot");
    let server = start(&snapshot, &[]);
    server.expect_log("no snapshot at");
    let mut c = server.connect();
    call(&mut c, &[b"SET", b"plain", b"value"], b"+OK\r\n");
    call(&mut c, &[b"SET", b"ttl", b"t", b"EX", b"1000"], b"+OK\r\n");
    call(&mut c, &[b"SET", b"gone", b"g", b"PX", b"100"], b"+OK\r\n");
    call(&mut c, &[b"INCRBY", b"counter", b"41"], b":41\r\n");
    let big = vec![b'x'; 100_000];
    call(&mut c, &[b"SET", b"big", &big], b"+OK\r\n");
    drop(c);
    server.stop();
    std::thread::sleep(Duration::from_millis(150)); // "gone" expires meanwhile

    let server = start(&snapshot, &[]);
    let line = server.expect_log("loaded");
    assert!(
        line.contains("loaded 4 keys") && line.contains("1 had expired"),
        "{line}"
    );
    let mut c = server.connect();
    call(&mut c, &[b"GET", b"plain"], b"$5\r\nvalue\r\n");
    call(&mut c, &[b"INCR", b"counter"], b":42\r\n");
    call(&mut c, &[b"GET", b"gone"], b"$-1\r\n");
    c.write_all(&req(&[b"TTL", b"ttl"])).unwrap();
    let mut ttl = String::new();
    BufReader::new(&mut c).read_line(&mut ttl).unwrap();
    let ttl: i64 = ttl.trim()[1..].parse().unwrap();
    assert!((995..=1000).contains(&ttl), "{ttl}");
    let mut expected = b"$100000\r\n".to_vec();
    expected.extend_from_slice(&big);
    expected.extend_from_slice(b"\r\n");
    call(&mut c, &[b"GET", b"big"], &expected);
    drop(c);
    server.stop();
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn periodic_saves_and_commands() {
    let dir = temp_dir("periodic");
    let snapshot = dir.join("data.snapshot");
    let server = start(&snapshot, &["--snapshot-interval", "1"]);
    let mut c = server.connect();
    call(&mut c, &[b"SET", b"a", b"1"], b"+OK\r\n");
    // Saved by the background thread within about a second.
    let deadline = Instant::now() + Duration::from_secs(5);
    while !snapshot.exists() {
        assert!(Instant::now() < deadline, "no periodic save");
        std::thread::sleep(Duration::from_millis(50));
    }
    call(&mut c, &[b"SAVE"], b"+OK\r\n");
    call(&mut c, &[b"BGSAVE"], b"+Background saving started\r\n");
    call(
        &mut c,
        &[b"CONFIG", b"GET", b"save"],
        b"*2\r\n$4\r\nsave\r\n$3\r\n1 1\r\n",
    );
    // A kill without a chance to save keeps what was last saved.
    call(&mut c, &[b"SET", b"b", b"2"], b"+OK\r\n");
    let mut server = server;
    server.child.kill().unwrap();
    server.child.wait().unwrap();
    let server = start(&snapshot, &[]);
    server.expect_log("loaded");
    let mut c = server.connect();
    call(&mut c, &[b"GET", b"a"], b"$1\r\n1\r\n");
    drop(c);
    server.stop();
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn a_damaged_snapshot_stops_startup() {
    let dir = temp_dir("damaged");
    let snapshot = dir.join("data.snapshot");
    std::fs::write(&snapshot, b"TANTALUM SNAPSHOT 1\nnot really").unwrap();
    let mut server = start(&snapshot, &[]);
    let line = server.expect_log("can't load the snapshot");
    assert!(line.contains("Move it away"), "{line}");
    let status = server.child.wait().unwrap();
    assert_eq!(status.code(), Some(1));
    // The file is left alone for inspection.
    assert_eq!(
        std::fs::read(&snapshot).unwrap(),
        b"TANTALUM SNAPSHOT 1\nnot really"
    );
    std::fs::remove_dir_all(dir).unwrap();
}
