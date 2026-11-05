//! Real memory, the server's own binary and allocator: what `used_memory` can't
//! see, since it counts what's in use and not what the process still holds.
//! (Linux only: it reads /proc, and other systems' resident sizes aren't alike.)

#![cfg(target_os = "linux")]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start() -> (Server, TcpStream) {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let child = Command::new(env!("CARGO_BIN_EXE_tantalum"))
        .args([
            "--bind",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--threads",
            "1",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let server = Server(child);
    for _ in 0..200 {
        if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
            stream
                .set_read_timeout(Some(Duration::from_secs(30)))
                .unwrap();
            return (server, stream);
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("the server didn't start");
}

fn resident_bytes(server: &Server) -> usize {
    let status = std::fs::read_to_string(format!("/proc/{}/status", server.0.id())).unwrap();
    let line = status
        .lines()
        .find(|line| line.starts_with("VmRSS:"))
        .unwrap();
    let kb: usize = line.split_whitespace().nth(1).unwrap().parse().unwrap();
    kb * 1024
}

/// A big set's buffer doubles as it grows. The old one, freed, must go back to the
/// OS: mimalloc does that only when something next asks it for whole pages, which
/// a steady server may never do, so `spawn_memory_returner` asks. Without it this
/// set holds about twice what it needs, however long it's left. Its size is just
/// past a doubling (2^18 members of 202 bytes), where what's left behind is largest
/// against what's in use: further on, the same old buffer is a smaller share.
#[test]
fn what_a_growing_buffer_frees_goes_back_to_the_os() {
    const MEMBERS: usize = 270_000;
    const BATCH: usize = 1_000;
    const MEMBER_BYTES: usize = 200;
    // Each member's bytes and length, its offset, and its place in the table.
    const NEEDS_PER_MEMBER: usize = MEMBER_BYTES + 2 + 4 + 7;
    let (server, mut client) = start();
    let before = resident_bytes(&server);
    let mut request = Vec::new();
    let mut replies = vec![0; BATCH * b":1\r\n".len()];
    for batch in 0..MEMBERS / BATCH {
        request.clear();
        for i in 0..BATCH {
            let member = format!("{:0width$}", batch * BATCH + i, width = MEMBER_BYTES);
            write!(
                request,
                "*3\r\n$4\r\nSADD\r\n$1\r\ns\r\n${MEMBER_BYTES}\r\n{member}\r\n"
            )
            .unwrap();
        }
        client.write_all(&request).unwrap();
        client.read_exact(&mut replies).unwrap();
        assert!(replies.starts_with(b":1\r\n"), "a new member");
    }
    // Half as much again is room for what's spare in the table and the allocator's
    // pages; the old buffer alone would be more than that.
    let budget = NEEDS_PER_MEMBER * MEMBERS * 3 / 2;
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut grown;
    loop {
        grown = resident_bytes(&server).saturating_sub(before);
        if grown <= budget || Instant::now() > deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    println!(
        "{} bytes per member resident, budget {}",
        grown / MEMBERS,
        budget / MEMBERS
    );
    assert!(
        grown <= budget,
        "{} bytes per member still resident after 20 s, over the budget of {}",
        grown / MEMBERS,
        budget / MEMBERS,
    );
}
