//! A Sentinel's own clients: what they may run, and its replies, as Redis'
//! Sentinel gives them. Connections are served here (`PING`, `AUTH`, `HELLO`,
//! `CLIENT`, pub/sub for the events); what needs the Sentinel's state goes to
//! the main loop and back (`SENTINEL`, `INFO`, `ROLE`, `PUBLISH`,
//! `SHUTDOWN`).

use std::collections::{BTreeSet, HashMap};
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, oneshot};

use super::monitor::{self, FailoverState, Instance, Kind, Request, State, Who};
use super::{Config, mstime};

/// Counts for `INFO`, and whether `SHUTDOWN` was asked for.
#[derive(Default)]
pub(crate) struct Stats {
    pub(crate) shutdown: bool,
}

/// What every connection shares.
#[derive(Clone)]
pub(crate) struct Shared {
    requests: mpsc::UnboundedSender<Request>,
    publish: broadcast::Sender<(String, String)>,
    requirepass: Option<String>,
    next_id: Arc<AtomicU64>,
    clients: Arc<Mutex<HashMap<u64, ClientInfo>>>,
    pub(crate) connections: Arc<AtomicU64>,
    pub(crate) commands: Arc<AtomicU64>,
}

#[derive(Clone, Default)]
struct ClientInfo {
    addr: String,
    name: String,
    since: i64,
    last_command: String,
    kill: Option<Arc<tokio::sync::Notify>>,
}

impl Shared {
    pub(crate) fn new(
        requests: mpsc::UnboundedSender<Request>,
        publish: broadcast::Sender<(String, String)>,
        requirepass: Option<String>,
    ) -> Shared {
        Shared {
            requests,
            publish,
            requirepass,
            next_id: Arc::new(AtomicU64::new(0)),
            clients: Arc::new(Mutex::new(HashMap::new())),
            connections: Arc::new(AtomicU64::new(0)),
            commands: Arc::new(AtomicU64::new(0)),
        }
    }
}

/// The listeners `bind` asks for (any address, if it doesn't say).
pub(crate) async fn listen(config: &Config) -> std::io::Result<Vec<TcpListener>> {
    let mut addrs: Vec<String> = config
        .bind
        .iter()
        .map(|b| b.trim_start_matches('-').to_string())
        .map(|b| if b == "*" { "0.0.0.0".to_string() } else { b })
        .filter(|b| b != "::*")
        .collect();
    if addrs.is_empty() {
        addrs.push("0.0.0.0".into());
    }
    let mut listeners = Vec::new();
    for addr in addrs {
        let host = if addr.contains(':') {
            format!("[{addr}]")
        } else {
            addr
        };
        listeners.push(TcpListener::bind(format!("{host}:{}", config.port)).await?);
    }
    Ok(listeners)
}

pub(crate) async fn accept(listener: TcpListener, shared: Shared) {
    loop {
        let Ok((stream, peer)) = listener.accept().await else {
            continue;
        };
        let _ = stream.set_nodelay(true);
        shared.connections.fetch_add(1, Relaxed);
        tokio::spawn(connection(stream, peer.to_string(), shared.clone()));
    }
}

/// A reply being written, in RESP2 or RESP3.
pub(crate) struct Out {
    pub(crate) buf: Vec<u8>,
    pub(crate) resp3: bool,
}

impl Out {
    pub(crate) fn new(resp3: bool) -> Out {
        Out {
            buf: Vec::new(),
            resp3,
        }
    }
    fn put(&mut self, s: String) {
        self.buf.extend_from_slice(s.as_bytes());
    }
    pub(crate) fn simple(&mut self, s: &str) {
        self.put(format!("+{s}\r\n"));
    }
    pub(crate) fn error(&mut self, s: &str) {
        self.put(format!("-{s}\r\n"));
    }
    pub(crate) fn int(&mut self, n: i64) {
        self.put(format!(":{n}\r\n"));
    }
    pub(crate) fn bulk(&mut self, b: &[u8]) {
        self.put(format!("${}\r\n", b.len()));
        self.buf.extend_from_slice(b);
        self.buf.extend_from_slice(b"\r\n");
    }
    pub(crate) fn text(&mut self, s: &str) {
        self.bulk(s.as_bytes());
    }
    pub(crate) fn array(&mut self, n: usize) {
        self.put(format!("*{n}\r\n"));
    }
    pub(crate) fn map(&mut self, n: usize) {
        if self.resp3 {
            self.put(format!("%{n}\r\n"));
        } else {
            self.put(format!("*{}\r\n", n * 2));
        }
    }
    pub(crate) fn null_array(&mut self) {
        self.buf
            .extend_from_slice(if self.resp3 { b"_\r\n" } else { b"*-1\r\n" });
    }
    pub(crate) fn null(&mut self) {
        self.buf
            .extend_from_slice(if self.resp3 { b"_\r\n" } else { b"$-1\r\n" });
    }
    /// Fields of a map, each a name and a bulk value.
    pub(crate) fn fields(&mut self, fields: &[(&str, String)]) {
        self.map(fields.len());
        for (name, value) in fields {
            self.text(name);
            self.text(value);
        }
    }
    /// A pub/sub message's start: a push in RESP3.
    fn push(&mut self, n: usize) {
        if self.resp3 {
            self.put(format!(">{n}\r\n"));
        } else {
            self.array(n);
        }
    }
}

/// One command from `buf`, and how much it took; `Err` for a protocol error.
/// A command's words.
type Words = Vec<Vec<u8>>;

fn parse_command(buf: &[u8]) -> Result<Option<(Words, usize)>, &'static str> {
    if buf.is_empty() {
        return Ok(None);
    }
    if buf[0] != b'*' {
        // Inline.
        let Some(end) = buf.iter().position(|&b| b == b'\n') else {
            return Ok(None);
        };
        let line = String::from_utf8_lossy(&buf[..end]);
        let words = super::split_args(line.trim_end_matches('\r'))
            .ok_or("Protocol error: unbalanced quotes in request")?;
        return Ok(Some((
            words.into_iter().map(String::into_bytes).collect(),
            end + 1,
        )));
    }
    let line_end = |from: usize| {
        buf[from..]
            .windows(2)
            .position(|w| w == b"\r\n")
            .map(|p| from + p)
    };
    let Some(end) = line_end(0) else {
        return Ok(None);
    };
    let n: i64 = std::str::from_utf8(&buf[1..end])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or("Protocol error: invalid multibulk length")?;
    let mut at = end + 2;
    let mut args = Vec::new();
    for _ in 0..n.max(0) {
        if at >= buf.len() {
            return Ok(None);
        }
        if buf[at] != b'$' {
            return Err("Protocol error: expected '$'");
        }
        let Some(end) = line_end(at) else {
            return Ok(None);
        };
        let len: usize = std::str::from_utf8(&buf[at + 1..end])
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or("Protocol error: invalid bulk length")?;
        let start = end + 2;
        if buf.len() < start + len + 2 {
            return Ok(None);
        }
        args.push(buf[start..start + len].to_vec());
        at = start + len + 2;
    }
    Ok(Some((args, at)))
}

/// Redis' error for a command that isn't here.
fn unknown_command(out: &mut Out, args: &[Vec<u8>]) {
    let mut msg = format!(
        "ERR unknown command '{}', with args beginning with: ",
        String::from_utf8_lossy(&args[0])
    );
    for arg in &args[1..] {
        let _ = write!(msg, "'{}' ", String::from_utf8_lossy(arg));
    }
    out.error(&msg);
}

fn lower(arg: &[u8]) -> String {
    String::from_utf8_lossy(arg).to_ascii_lowercase()
}

async fn connection(stream: TcpStream, addr: String, shared: Shared) {
    let id = shared.next_id.fetch_add(1, Relaxed) + 1;
    let kill = Arc::new(tokio::sync::Notify::new());
    shared.clients.lock().insert(
        id,
        ClientInfo {
            addr: addr.clone(),
            since: mstime(),
            kill: Some(kill.clone()),
            ..ClientInfo::default()
        },
    );
    let (mut read, mut write) = stream.into_split();
    let mut buf = Vec::new();
    let mut chunk = vec![0u8; 16 * 1024];
    let mut resp3 = false;
    let mut authed = shared.requirepass.is_none();
    let mut channels: BTreeSet<String> = BTreeSet::new();
    let mut patterns: BTreeSet<String> = BTreeSet::new();
    let mut events = shared.publish.subscribe();
    'conn: loop {
        let subscribed = !channels.is_empty() || !patterns.is_empty();
        tokio::select! {
            got = read.read(&mut chunk) => {
                let n = match got {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                buf.extend_from_slice(&chunk[..n]);
                let mut out = Out::new(resp3);
                let mut at = 0;
                loop {
                    let (args, used) = match parse_command(&buf[at..]) {
                        Ok(Some(c)) => c,
                        Ok(None) => break,
                        Err(e) => {
                            out.error(&format!("ERR {e}"));
                            let _ = write.write_all(&out.buf).await;
                            break 'conn;
                        }
                    };
                    at += used;
                    if args.is_empty() {
                        continue;
                    }
                    shared.commands.fetch_add(1, Relaxed);
                    let name = lower(&args[0]);
                    if let Some(c) = shared.clients.lock().get_mut(&id) {
                        c.last_command = name.clone();
                    }
                    if !authed && !matches!(name.as_str(), "auth" | "hello" | "quit") {
                        out.error("NOAUTH Authentication required.");
                        continue;
                    }
                    if subscribed
                        && !resp3
                        && !matches!(
                            name.as_str(),
                            "subscribe" | "psubscribe" | "unsubscribe" | "punsubscribe" | "ping" | "quit" | "reset"
                        )
                    {
                        out.error(&format!(
                            "ERR Can't execute '{name}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
                        ));
                        continue;
                    }
                    match name.as_str() {
                        "ping" => {
                            if args.len() > 2 {
                                out.error("ERR wrong number of arguments for 'ping' command");
                            } else if subscribed && !resp3 {
                                out.array(2);
                                out.text("pong");
                                out.bulk(args.get(1).map_or(&b""[..], |a| a));
                            } else if let Some(msg) = args.get(1) {
                                out.bulk(msg);
                            } else {
                                out.simple("PONG");
                            }
                        }
                        "quit" => {
                            out.simple("OK");
                            let _ = write.write_all(&out.buf).await;
                            break 'conn;
                        }
                        "auth" => match auth(&shared, &args) {
                            Ok(()) => {
                                authed = true;
                                out.simple("OK");
                            }
                            Err(e) => out.error(e),
                        },
                        "hello" => hello(&shared, id, &args, &mut resp3, &mut authed, &mut out),
                        "reset" => {
                            channels.clear();
                            patterns.clear();
                            resp3 = false;
                            out = Out { buf: std::mem::take(&mut out.buf), resp3 };
                            authed = shared.requirepass.is_none();
                            out.simple("RESET");
                        }
                        "client" => client(&shared, id, &args, &mut out),
                        "command" => command_command(&args, &mut out),
                        "subscribe" | "psubscribe" => {
                            if args.len() < 2 {
                                out.error(&format!("ERR wrong number of arguments for '{name}' command"));
                                continue;
                            }
                            let pattern = name == "psubscribe";
                            for ch in &args[1..] {
                                let ch = String::from_utf8_lossy(ch).into_owned();
                                let set = if pattern { &mut patterns } else { &mut channels };
                                set.insert(ch.clone());
                                out.push(3);
                                out.text(&name);
                                out.text(&ch);
                                out.int((channels.len() + patterns.len()) as i64);
                            }
                        }
                        "unsubscribe" | "punsubscribe" => {
                            let pattern = name == "punsubscribe";
                            let set = if pattern { &mut patterns } else { &mut channels };
                            let which: Vec<String> = if args.len() > 1 {
                                args[1..].iter().map(|a| String::from_utf8_lossy(a).into_owned()).collect()
                            } else {
                                set.iter().cloned().collect()
                            };
                            if which.is_empty() {
                                out.push(3);
                                out.text(&name);
                                out.null();
                                out.int((channels.len() + patterns.len()) as i64);
                            }
                            for ch in which {
                                let set = if pattern { &mut patterns } else { &mut channels };
                                set.remove(&ch);
                                out.push(3);
                                out.text(&name);
                                out.text(&ch);
                                out.int((channels.len() + patterns.len()) as i64);
                            }
                        }
                        _ => {
                            let (tx, rx) = oneshot::channel();
                            let _ = shared.requests.send(Request { args: args.clone(), resp3, reply: tx });
                            match rx.await {
                                Ok(reply) => out.buf.extend_from_slice(&reply),
                                Err(_) => break 'conn,
                            }
                        }
                    }
                }
                buf.drain(..at);
                if !out.buf.is_empty() && write.write_all(&out.buf).await.is_err() {
                    break;
                }
            }
            event = events.recv(), if subscribed => {
                let Ok((channel, message)) = event else { continue };
                let mut out = Out::new(resp3);
                if channels.contains(&channel) {
                    out.push(3);
                    out.text("message");
                    out.text(&channel);
                    out.text(&message);
                }
                for p in &patterns {
                    if crate::glob::matches(p.as_bytes(), channel.as_bytes(), false) {
                        out.push(4);
                        out.text("pmessage");
                        out.text(p);
                        out.text(&channel);
                        out.text(&message);
                    }
                }
                if !out.buf.is_empty() && write.write_all(&out.buf).await.is_err() {
                    break;
                }
            }
            _ = kill.notified() => break,
        }
    }
    shared.clients.lock().remove(&id);
}

fn auth(shared: &Shared, args: &[Vec<u8>]) -> Result<(), &'static str> {
    if args.len() < 2 || args.len() > 3 {
        return Err("ERR wrong number of arguments for 'auth' command");
    }
    let Some(pass) = &shared.requirepass else {
        return Err(
            "ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?",
        );
    };
    let (user, given) = if args.len() == 3 {
        (&args[1][..], &args[2][..])
    } else {
        (&b"default"[..], &args[1][..])
    };
    if user == b"default" && given == pass.as_bytes() {
        Ok(())
    } else {
        Err("WRONGPASS invalid username-password pair or user is disabled.")
    }
}

fn hello(
    shared: &Shared,
    id: u64,
    args: &[Vec<u8>],
    resp3: &mut bool,
    authed: &mut bool,
    out: &mut Out,
) {
    let mut proto = if *resp3 { 3 } else { 2 };
    let mut i = 1;
    if let Some(v) = args.get(1) {
        match std::str::from_utf8(v)
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
        {
            Some(v @ 2..=3) => proto = v,
            Some(_) => {
                out.error("NOPROTO unsupported protocol version");
                return;
            }
            None => {
                out.error("ERR Protocol version is not an integer or out of range");
                return;
            }
        }
        i = 2;
    }
    while i < args.len() {
        let opt = lower(&args[i]);
        if opt == "auth" && i + 2 < args.len() {
            match auth(
                shared,
                &[b"AUTH".to_vec(), args[i + 1].clone(), args[i + 2].clone()],
            ) {
                Ok(()) => *authed = true,
                Err(e) => {
                    out.error(e);
                    return;
                }
            }
            i += 3;
        } else if opt == "setname" && i + 1 < args.len() {
            if let Some(c) = shared.clients.lock().get_mut(&id) {
                c.name = String::from_utf8_lossy(&args[i + 1]).into_owned();
            }
            i += 2;
        } else {
            out.error(&format!(
                "ERR Syntax error in HELLO option '{}'",
                String::from_utf8_lossy(&args[i])
            ));
            return;
        }
    }
    if !*authed {
        out.error("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time");
        return;
    }
    *resp3 = proto == 3;
    out.resp3 = *resp3;
    out.map(7);
    out.text("server");
    out.text("redis");
    out.text("version");
    out.text("7.2.4");
    out.text("proto");
    out.int(proto);
    out.text("id");
    out.int(id as i64);
    out.text("mode");
    out.text("sentinel");
    out.text("role");
    out.text("sentinel");
    out.text("modules");
    out.array(0);
}

fn client(shared: &Shared, id: u64, args: &[Vec<u8>], out: &mut Out) {
    let sub = args.get(1).map(|a| lower(a)).unwrap_or_default();
    let mut clients = shared.clients.lock();
    match (sub.as_str(), args.len()) {
        ("id", 2) => out.int(id as i64),
        ("setname", 3) => {
            let name = String::from_utf8_lossy(&args[2]).into_owned();
            if name.contains(' ') || name.contains('\n') {
                out.error(
                    "ERR Client names cannot contain spaces, newlines or special characters.",
                );
                return;
            }
            if let Some(c) = clients.get_mut(&id) {
                c.name = name;
            }
            out.simple("OK");
        }
        ("getname", 2) => match clients
            .get(&id)
            .map(|c| c.name.clone())
            .filter(|n| !n.is_empty())
        {
            Some(name) => out.text(&name),
            None => out.null(),
        },
        ("setinfo", 4) | ("no-evict", 3) | ("no-touch", 3) => out.simple("OK"),
        ("list", 2) | ("info", 2) => {
            let now = mstime();
            let mut lines = String::new();
            let mut ids: Vec<u64> = clients.keys().copied().collect();
            ids.sort_unstable();
            for cid in ids {
                if sub == "info" && cid != id {
                    continue;
                }
                let c = &clients[&cid];
                let _ = writeln!(
                    lines,
                    "id={cid} addr={} name={} age={} cmd={} user=default",
                    c.addr,
                    c.name,
                    (now - c.since) / 1000,
                    c.last_command
                );
            }
            out.text(&lines);
        }
        ("kill", n) if n >= 3 => {
            // `CLIENT KILL addr`, or `ID id` / `ADDR addr` filters.
            let mut killed = 0;
            let filter: Vec<String> = args[2..]
                .iter()
                .map(|a| String::from_utf8_lossy(a).into_owned())
                .collect();
            let picks = |cid: u64, c: &ClientInfo| -> bool {
                if filter.len() == 1 {
                    return c.addr == filter[0];
                }
                filter.chunks(2).all(|pair| {
                    match (pair[0].to_ascii_lowercase().as_str(), pair.get(1)) {
                        ("id", Some(v)) => v.parse::<u64>().ok() == Some(cid),
                        ("addr", Some(v)) => c.addr == *v,
                        ("skipme", Some(v)) => !(v == "yes" && cid == id),
                        _ => false,
                    }
                })
            };
            for (cid, c) in clients.iter() {
                if picks(*cid, c)
                    && let Some(k) = &c.kill
                {
                    k.notify_one();
                    killed += 1;
                }
            }
            if filter.len() == 1 {
                if killed == 0 {
                    out.error("ERR No such client");
                } else {
                    out.simple("OK");
                }
            } else {
                out.int(killed);
            }
        }
        _ => out.error(&format!(
            "ERR unknown subcommand or wrong number of arguments for '{}'. Try CLIENT HELP.",
            String::from_utf8_lossy(args.get(1).map_or(&b""[..], |a| a))
        )),
    }
}

const COMMANDS: &[&str] = &[
    "auth",
    "client",
    "command",
    "hello",
    "info",
    "ping",
    "psubscribe",
    "publish",
    "punsubscribe",
    "quit",
    "reset",
    "role",
    "sentinel",
    "shutdown",
    "subscribe",
    "unsubscribe",
];

fn command_command(args: &[Vec<u8>], out: &mut Out) {
    match args.get(1).map(|a| lower(a)).as_deref() {
        Some("count") => out.int(COMMANDS.len() as i64),
        Some("list") => {
            out.array(COMMANDS.len());
            for c in COMMANDS {
                out.text(c);
            }
        }
        Some("docs") => out.map(0),
        _ => out.array(0),
    }
}

/// A command for the main loop: the Sentinel's own.
pub(crate) fn command(state: &mut State, args: &[Vec<u8>], resp3: bool) -> Vec<u8> {
    let mut out = Out::new(resp3);
    match lower(&args[0]).as_str() {
        "sentinel" => sentinel(state, args, &mut out),
        "info" => info(state, args, &mut out),
        "role" => {
            out.array(2);
            out.text("sentinel");
            out.array(state.masters.len());
            for m in &state.masters {
                out.text(&m.name);
            }
        }
        "publish" => {
            if args.len() != 3 {
                out.error("ERR wrong number of arguments for 'publish' command");
            } else if args[1] != b"__sentinel__:hello" {
                out.error("ERR Only HELLO messages are accepted by Sentinel instances.");
            } else {
                let message = String::from_utf8_lossy(&args[2]).into_owned();
                if !message.contains(&state.myid) {
                    monitor::process_hello(state, &message);
                }
                out.int(1);
            }
        }
        "shutdown" => {
            state.stats.shutdown = true;
            out.simple("OK");
        }
        _ => unknown_command(&mut out, args),
    }
    out.buf
}

fn arity(out: &mut Out, sub: &str) {
    out.error(&format!(
        "ERR wrong number of arguments for 'sentinel|{sub}' command"
    ));
}

fn master_by_name(state: &State, name: &[u8], out: &mut Out) -> Option<usize> {
    let found = state.masters.iter().position(|m| m.name.as_bytes() == name);
    if found.is_none() {
        out.error("ERR No such master with that name");
    }
    found
}

const HELP: &[&str] = &[
    "CKQUORUM <master-name>",
    "    Check if the current Sentinel configuration is able to reach the quorum",
    "    needed to failover a master and the majority needed to authorize the",
    "    failover.",
    "CONFIG SET param value [param value ...]",
    "    Set a global Sentinel configuration parameter.",
    "CONFIG GET <param> [param param param ...]",
    "    Get global Sentinel configuration parameter.",
    "DEBUG [<param> <value> ...]",
    "    Show a list of configurable time parameters and their values (milliseconds).",
    "    Or update current configurable parameters values (one or more).",
    "GET-MASTER-ADDR-BY-NAME <master-name>",
    "    Return the ip and port number of the master with that name.",
    "FAILOVER <master-name>",
    "    Manually failover a master node without asking for agreement from other",
    "    Sentinels",
    "FLUSHCONFIG",
    "    Force Sentinel to rewrite its configuration on disk, including the current",
    "    Sentinel state.",
    "INFO-CACHE <master-name>",
    "    Return last cached INFO output from masters and all its replicas.",
    "IS-MASTER-DOWN-BY-ADDR <ip> <port> <current-epoch> <runid>",
    "    Check if the master specified by ip:port is down from current Sentinel's",
    "    point of view.",
    "MASTER <master-name>",
    "    Show the state and info of the specified master.",
    "MASTERS",
    "    Show a list of monitored masters and their state.",
    "MONITOR <name> <ip> <port> <quorum>",
    "    Start monitoring a new master with the specified name, ip, port and quorum.",
    "MYID",
    "    Return the ID of the Sentinel instance.",
    "PENDING-SCRIPTS",
    "    Get pending scripts information.",
    "REMOVE <master-name>",
    "    Remove master from Sentinel's monitor list.",
    "REPLICAS <master-name>",
    "    Show a list of replicas for this master and their state.",
    "RESET <pattern>",
    "    Reset masters for specific master name matching this pattern.",
    "SENTINELS <master-name>",
    "    Show a list of Sentinel instances for this master and their state.",
    "SET <master-name> <option> <value> [<option> <value> ...]",
    "    Set configuration parameters for certain masters.",
    "SIMULATE-FAILURE [CRASH-AFTER-ELECTION] [CRASH-AFTER-PROMOTION] [HELP]",
    "    Simulate a Sentinel crash.",
];

fn sentinel(state: &mut State, args: &[Vec<u8>], out: &mut Out) {
    if args.len() < 2 {
        out.error("ERR wrong number of arguments for 'sentinel' command");
        return;
    }
    let sub = lower(&args[1]);
    let n = args.len();
    let text = |i: usize| String::from_utf8_lossy(&args[i]).into_owned();
    match sub.as_str() {
        "help" if n == 2 => {
            out.array(HELP.len() + 3);
            out.simple("SENTINEL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:");
            for line in HELP {
                out.simple(line);
            }
            out.simple("HELP");
            out.simple("    Print this help.");
        }
        "masters" => {
            if n != 2 {
                return arity(out, &sub);
            }
            out.array(state.masters.len());
            for m in 0..state.masters.len() {
                instance_reply(state, m, Who::Master, out);
            }
        }
        "master" => {
            if n != 3 {
                return arity(out, &sub);
            }
            if let Some(m) = master_by_name(state, &args[2], out) {
                instance_reply(state, m, Who::Master, out);
            }
        }
        "slaves" | "replicas" => {
            if n != 3 {
                return arity(out, &sub);
            }
            if let Some(m) = master_by_name(state, &args[2], out) {
                let count = state.masters[m].replicas.len();
                out.array(count);
                for i in 0..count {
                    instance_reply(state, m, Who::Replica(i), out);
                }
            }
        }
        "sentinels" => {
            if n != 3 {
                return arity(out, &sub);
            }
            if let Some(m) = master_by_name(state, &args[2], out) {
                let count = state.masters[m].sentinels.len();
                out.array(count);
                for i in 0..count {
                    instance_reply(state, m, Who::Sentinel(i), out);
                }
            }
        }
        "myid" if n == 2 => out.text(&state.myid),
        "is-master-down-by-addr" => {
            if n != 6 {
                return arity(out, &sub);
            }
            let (Ok(port), Ok(epoch)) = (text(3).parse::<i64>(), text(4).parse::<i64>()) else {
                out.error("ERR value is not an integer or out of range");
                return;
            };
            let ip = text(2);
            let m = state
                .masters
                .iter()
                .position(|m| m.me.host == ip && i64::from(m.me.port) == port);
            let down = !state.tilt && m.is_some_and(|m| state.masters[m].me.s_down);
            let mut leader = None;
            let mut leader_epoch = 0;
            if let Some(m) = m
                && args[5] != b"*"
            {
                let (l, e) = monitor::vote_leader(state, m, epoch.max(0) as u64, &text(5));
                leader = l;
                leader_epoch = e;
            }
            out.array(3);
            out.int(i64::from(down));
            out.text(leader.as_deref().unwrap_or("*"));
            out.int(leader_epoch as i64);
        }
        "reset" => {
            if n != 3 {
                return arity(out, &sub);
            }
            let mut reset = 0;
            for m in 0..state.masters.len() {
                if crate::glob::matches(&args[2], state.masters[m].name.as_bytes(), false) {
                    monitor::reset_master(state, m, false, true);
                    reset += 1;
                }
            }
            let _ = state.flush_config();
            out.int(reset);
        }
        "get-master-addr-by-name" => {
            if n != 3 {
                return arity(out, &sub);
            }
            match state
                .masters
                .iter()
                .find(|m| m.name.as_bytes() == args[2].as_slice())
            {
                None => out.null_array(),
                Some(m) => {
                    let (host, port) = m.current_addr();
                    out.array(2);
                    out.text(&host);
                    out.text(&port.to_string());
                }
            }
        }
        "failover" => {
            if n != 3 {
                return arity(out, &sub);
            }
            let Some(m) = master_by_name(state, &args[2], out) else {
                return;
            };
            if state.masters[m].failover_in_progress {
                out.error("INPROG Failover already in progress");
                return;
            }
            if monitor::select_replica(state, m).is_none() {
                out.error("NOGOODSLAVE No suitable replica to promote");
                return;
            }
            monitor::log(&format!(
                "* Executing user requested FAILOVER of '{}'",
                state.masters[m].name
            ));
            monitor::start_failover(state, m);
            state.masters[m].force_failover = true;
            out.simple("OK");
        }
        "pending-scripts" => {
            if n != 2 {
                return arity(out, &sub);
            }
            out.array(0);
        }
        "monitor" => {
            if n != 6 {
                return arity(out, &sub);
            }
            let Ok(quorum) = text(5).parse::<i64>() else {
                out.error("ERR Invalid quorum");
                return;
            };
            let Ok(port) = text(4).parse::<i64>() else {
                out.error("ERR Invalid port");
                return;
            };
            if quorum <= 0 {
                out.error("ERR Quorum must be 1 or greater.");
                return;
            }
            let host = text(3);
            if !state.config.resolve_hostnames && host.parse::<std::net::IpAddr>().is_err() {
                out.error("ERR Invalid IP address or hostname specified");
                return;
            }
            if !(1..=65535).contains(&port) {
                out.error("ERR Invalid port number");
                return;
            }
            if state
                .masters
                .iter()
                .any(|m| m.name.as_bytes() == args[2].as_slice())
            {
                out.error("ERR Duplicate master name.");
                return;
            }
            let name = text(2);
            state.add_master(&super::MasterConfig {
                name,
                host,
                port: port as u16,
                quorum: quorum as u32,
                down_after: state.timings.default_down_after,
                failover_timeout: state.timings.default_failover_timeout,
                parallel_syncs: 1,
                auth_pass: None,
                auth_user: None,
                notification_script: None,
                client_reconfig_script: None,
                master_reboot_down_after: 0,
                config_epoch: 0,
                leader_epoch: 0,
                rename: Default::default(),
                replicas: Vec::new(),
                sentinels: Vec::new(),
            });
            flush_and_reply(state, out);
            let m = state.masters.len() - 1;
            let q = state.masters[m].quorum;
            state.event(true, "+monitor", m, Who::Master, &format!("quorum {q}"));
        }
        "flushconfig" => {
            if n != 2 {
                return arity(out, &sub);
            }
            flush_and_reply(state, out);
        }
        "remove" => {
            if n != 3 {
                return arity(out, &sub);
            }
            let Some(m) = master_by_name(state, &args[2], out) else {
                return;
            };
            state.event(true, "-monitor", m, Who::Master, "");
            state.masters.remove(m);
            flush_and_reply(state, out);
        }
        "ckquorum" => {
            if n != 3 {
                return arity(out, &sub);
            }
            let Some(m) = master_by_name(state, &args[2], out) else {
                return;
            };
            let master = &state.masters[m];
            let voters = master.sentinels.len() + 1;
            let usable = 1 + master
                .sentinels
                .iter()
                .filter(|s| !s.s_down && !s.o_down)
                .count();
            let no_quorum = usable < master.quorum as usize;
            let no_auth = usable < voters / 2 + 1;
            if !no_quorum && !no_auth {
                out.buf.extend_from_slice(
                    format!("+OK {usable} usable Sentinels. Quorum and failover authorization can be reached\r\n")
                        .as_bytes(),
                );
            } else {
                let mut e = format!("NOQUORUM {usable} usable Sentinels. ");
                if no_quorum {
                    e.push_str("Not enough available Sentinels to reach the specified quorum for this master");
                }
                if no_auth {
                    if no_quorum {
                        e.push_str(". ");
                    }
                    e.push_str("Not enough available Sentinels to reach the majority and authorize a failover");
                }
                out.error(&e);
            }
        }
        "set" => set(state, args, out),
        "config" => {
            if n < 4 {
                return arity(out, &sub);
            }
            match lower(&args[2]).as_str() {
                "set" if n >= 5 => config_set(state, args, out),
                "get" => config_get(state, args, out),
                _ => out.error("ERR Only SENTINEL CONFIG GET <param> [<param> <param> ...]/ SET <param> <value> [<param> <value> ...] are supported."),
            }
        }
        "info-cache" => {
            if n < 2 {
                return arity(out, &sub);
            }
            let now = mstime();
            let wanted: Vec<usize> = (0..state.masters.len())
                .filter(|&m| {
                    n == 2
                        || args[2..]
                            .iter()
                            .any(|a| state.masters[m].name.as_bytes() == a.as_slice())
                })
                .collect();
            out.array(wanted.len() * 2);
            for m in wanted {
                let master = &state.masters[m];
                out.text(&master.name);
                out.array(master.replicas.len() + 1);
                for inst in std::iter::once(&master.me).chain(master.replicas.iter()) {
                    out.array(2);
                    out.int(if inst.info_refresh != 0 {
                        now - inst.info_refresh
                    } else {
                        0
                    });
                    if inst.info.is_empty() {
                        out.null();
                    } else {
                        out.text(&inst.info);
                    }
                }
            }
        }
        "simulate-failure" => {
            for a in &args[2..] {
                match lower(a).as_str() {
                    "crash-after-election" => state.simfailure_crash_after_election = true,
                    "crash-after-promotion" => state.simfailure_crash_after_promotion = true,
                    "help" => {
                        out.array(2);
                        out.text("crash-after-election");
                        out.text("crash-after-promotion");
                        return;
                    }
                    _ => {
                        out.error("ERR Unknown failure simulation specified");
                        return;
                    }
                }
            }
            out.simple("OK");
        }
        "debug" => {
            if n == 2 {
                let mut t = state.timings.clone();
                let fields: Vec<(&str, String)> = t
                    .fields()
                    .into_iter()
                    .map(|(k, v)| (k, v.to_string()))
                    .collect();
                out.fields(&fields);
                return;
            }
            let mut t = state.timings.clone();
            let mut j = 2;
            while j < n {
                let name = lower(&args[j]);
                let value = args
                    .get(j + 1)
                    .and_then(|v| std::str::from_utf8(v).ok()?.parse::<i64>().ok());
                let mut fields = t.fields();
                let Some((_, slot)) = fields.iter_mut().find(|(k, _)| *k == name) else {
                    out.error(&format!(
                        "ERR Unknown option or number of arguments for SENTINEL DEBUG '{name}'"
                    ));
                    return;
                };
                match value {
                    Some(v) if v > 0 => **slot = v,
                    _ => {
                        out.error(&format!(
                            "ERR Invalid argument '{}' for SENTINEL DEBUG '{name}'",
                            args.get(j + 1)
                                .map(|v| String::from_utf8_lossy(v).into_owned())
                                .unwrap_or_default()
                        ));
                        return;
                    }
                }
                j += 2;
            }
            state.timings = t;
            out.simple("OK");
        }
        _ => {
            if matches!(sub.as_str(), "help" | "myid") {
                return arity(out, &sub);
            }
            out.error(&format!(
                "ERR unknown subcommand '{}'. Try SENTINEL HELP.",
                String::from_utf8_lossy(&args[1])
            ));
        }
    }
}

fn flush_and_reply(state: &mut State, out: &mut Out) {
    match state.flush_config() {
        Ok(()) => out.simple("OK"),
        Err(e) => out.error(&format!("ERR Failed to save config file: {e}")),
    }
}

fn set(state: &mut State, args: &[Vec<u8>], out: &mut Out) {
    if args.len() < 5 {
        return arity(out, "set");
    }
    let Some(m) = master_by_name(state, &args[2], out) else {
        return;
    };
    let mut j = 3;
    let mut changes = 0;
    while j < args.len() {
        let option = lower(&args[j]);
        let more = args.len() - 1 - j;
        let value = |i: usize| String::from_utf8_lossy(&args[i]).into_owned();
        let number = |i: usize| value(i).parse::<i64>().ok();
        let old_j = j;
        let mut redacted = false;
        let bad = |out: &mut Out, at: usize| {
            out.error(&format!(
                "ERR Invalid argument '{}' for SENTINEL SET '{option}'",
                String::from_utf8_lossy(&args[at])
            ));
        };
        match option.as_str() {
            "down-after-milliseconds" | "failover-timeout" | "parallel-syncs" | "quorum"
                if more > 0 =>
            {
                j += 1;
                let Some(v) = number(j).filter(|&v| v > 0) else {
                    bad(out, j);
                    break;
                };
                let master = &mut state.masters[m];
                match option.as_str() {
                    "down-after-milliseconds" => master.down_after = v,
                    "failover-timeout" => master.failover_timeout = v,
                    "parallel-syncs" => master.parallel_syncs = v as u32,
                    _ => master.quorum = v as u32,
                }
            }
            "master-reboot-down-after-period" if more > 0 => {
                j += 1;
                let Some(v) = number(j).filter(|&v| v >= 0) else {
                    bad(out, j);
                    break;
                };
                state.masters[m].master_reboot_down_after = v;
            }
            "notification-script" | "client-reconfig-script" if more > 0 => {
                j += 1;
                if state.config.deny_scripts_reconfig {
                    out.error("ERR Reconfiguration of scripts path is denied for security reasons. Check the deny-scripts-reconfig configuration directive in your Sentinel configuration");
                    break;
                }
                let path = value(j);
                let master = &mut state.masters[m];
                let slot = if option == "notification-script" {
                    &mut master.notification_script
                } else {
                    &mut master.client_reconfig_script
                };
                *slot = Some(path).filter(|p| !p.is_empty());
            }
            "auth-pass" | "auth-user" if more > 0 => {
                j += 1;
                let v = Some(value(j)).filter(|v| !v.is_empty());
                let master = &mut state.masters[m];
                if option == "auth-pass" {
                    master.auth_pass = v;
                    redacted = true;
                } else {
                    master.auth_user = v;
                }
                master.me.close_links();
                for r in &mut master.replicas {
                    r.close_links();
                }
            }
            "rename-command" if more > 1 => {
                let (from, to) = (value(j + 1), value(j + 2));
                j += 2;
                if from.is_empty() || to.is_empty() {
                    bad(out, if to.is_empty() { j } else { j - 1 });
                    break;
                }
                let master = &mut state.masters[m];
                master.rename.retain(|k, _| !k.eq_ignore_ascii_case(&from));
                if !from.eq_ignore_ascii_case(&to) {
                    master.rename.insert(from, to);
                }
            }
            _ => {
                out.error(&format!(
                    "ERR Unknown option or number of arguments for SENTINEL SET '{}'",
                    String::from_utf8_lossy(&args[j])
                ));
                break;
            }
        }
        changes += 1;
        let shown: Vec<String> = (old_j..=j)
            .map(|i| {
                if redacted && i > old_j {
                    "******".into()
                } else {
                    value(i)
                }
            })
            .collect();
        state.event(true, "+set", m, Who::Master, &shown.join(" "));
        j += 1;
    }
    if changes > 0 {
        let _ = state.flush_config();
    }
    if out.buf.is_empty() {
        out.simple("OK");
    }
}

const CONFIG_OPTIONS: &[&str] = &[
    "resolve-hostnames",
    "announce-hostnames",
    "announce-ip",
    "announce-port",
    "sentinel-user",
    "sentinel-pass",
    "loglevel",
];

fn config_get(state: &State, args: &[Vec<u8>], out: &mut Out) {
    let c = &state.config;
    let mut found: Vec<(&str, String)> = Vec::new();
    for pattern in &args[3..] {
        for &name in CONFIG_OPTIONS {
            if found.iter().any(|(n, _)| *n == name)
                || !crate::glob::matches(pattern, name.as_bytes(), true)
            {
                continue;
            }
            let value = match name {
                "resolve-hostnames" => yes_no(c.resolve_hostnames),
                "announce-hostnames" => yes_no(c.announce_hostnames),
                "announce-ip" => c.announce_ip.clone().unwrap_or_default(),
                "announce-port" => c.announce_port.to_string(),
                "sentinel-user" => c.sentinel_user.clone().unwrap_or_default(),
                "sentinel-pass" => c.sentinel_pass.clone().unwrap_or_default(),
                _ => "notice".to_string(),
            };
            found.push((name, value));
        }
    }
    out.fields(&found);
}

fn yes_no(on: bool) -> String {
    if on { "yes" } else { "no" }.to_string()
}

fn config_set(state: &mut State, args: &[Vec<u8>], out: &mut Out) {
    if !(args.len() - 3).is_multiple_of(2) {
        return arity(out, "config");
    }
    let mut seen = BTreeSet::new();
    let mut config = state.config.clone();
    for pair in args[3..].chunks(2) {
        let name = lower(&pair[0]);
        let value = String::from_utf8_lossy(&pair[1]).into_owned();
        if !CONFIG_OPTIONS.contains(&name.as_str()) {
            out.error(&format!(
                "ERR Invalid argument '{}' to SENTINEL CONFIG SET",
                String::from_utf8_lossy(&pair[0])
            ));
            return;
        }
        if !seen.insert(name.clone()) {
            out.error(&format!(
                "ERR Duplicate argument '{}' to SENTINEL CONFIG SET",
                String::from_utf8_lossy(&pair[0])
            ));
            return;
        }
        let yes = || match value.to_ascii_lowercase().as_str() {
            "yes" => Some(true),
            "no" => Some(false),
            _ => None,
        };
        let ok = match name.as_str() {
            "resolve-hostnames" => yes().map(|v| config.resolve_hostnames = v).is_some(),
            "announce-hostnames" => yes().map(|v| config.announce_hostnames = v).is_some(),
            "announce-ip" => {
                config.announce_ip = Some(value.clone()).filter(|v| !v.is_empty());
                true
            }
            "announce-port" => value
                .parse::<u16>()
                .map(|p| config.announce_port = p)
                .is_ok(),
            "sentinel-user" => {
                config.sentinel_user = Some(value.clone()).filter(|v| !v.is_empty());
                true
            }
            "sentinel-pass" => {
                config.sentinel_pass = Some(value.clone()).filter(|v| !v.is_empty());
                true
            }
            "loglevel" => matches!(
                value.as_str(),
                "debug" | "verbose" | "notice" | "warning" | "nothing"
            ),
            _ => false,
        };
        if !ok {
            out.error(&format!(
                "ERR Invalid value '{value}' to SENTINEL CONFIG SET '{}'",
                String::from_utf8_lossy(&pair[0])
            ));
            return;
        }
    }
    state.config = config;
    flush_and_reply(state, out);
}

/// One instance as `SENTINEL MASTER`/`REPLICAS`/`SENTINELS` show it.
fn instance_reply(state: &State, m: usize, who: Who, out: &mut Out) {
    let now = mstime();
    let master = &state.masters[m];
    let inst: &Instance = match who {
        Who::Master | Who::None => &master.me,
        Who::Replica(i) => &master.replicas[i],
        Who::Sentinel(i) => &master.sentinels[i],
    };
    let is_master = who == Who::Master;
    let name = if is_master {
        master.name.clone()
    } else {
        inst.name()
    };
    let mut fields: Vec<(&str, String)> = vec![
        ("name", name),
        ("ip", inst.host.clone()),
        ("port", inst.port.to_string()),
        ("runid", inst.runid.clone().unwrap_or_default()),
        (
            "flags",
            inst.flags(
                is_master && master.failover_in_progress,
                is_master && master.force_failover,
            ),
        ),
        ("link-pending-commands", inst.pending.to_string()),
        ("link-refcount", "1".into()),
    ];
    if is_master && master.failover_in_progress {
        fields.push(("failover-state", master.failover_state.name().into()));
    }
    fields.push((
        "last-ping-sent",
        if inst.act_ping_time != 0 {
            now - inst.act_ping_time
        } else {
            0
        }
        .to_string(),
    ));
    fields.push((
        "last-ok-ping-reply",
        (now - inst.last_avail_time).to_string(),
    ));
    fields.push(("last-ping-reply", (now - inst.last_pong_time).to_string()));
    if inst.s_down {
        fields.push(("s-down-time", (now - inst.s_down_since).to_string()));
    }
    if inst.o_down {
        fields.push(("o-down-time", (now - inst.o_down_since).to_string()));
    }
    fields.push(("down-after-milliseconds", master.down_after.to_string()));
    if inst.kind != Kind::Sentinel {
        fields.push((
            "info-refresh",
            if inst.info_refresh != 0 {
                now - inst.info_refresh
            } else {
                0
            }
            .to_string(),
        ));
        fields.push((
            "role-reported",
            if inst.role_reported == Kind::Master {
                "master"
            } else {
                "slave"
            }
            .into(),
        ));
        fields.push((
            "role-reported-time",
            (now - inst.role_reported_time).to_string(),
        ));
    }
    if is_master {
        fields.push(("config-epoch", master.config_epoch.to_string()));
        fields.push(("num-slaves", master.replicas.len().to_string()));
        fields.push(("num-other-sentinels", master.sentinels.len().to_string()));
        fields.push(("quorum", master.quorum.to_string()));
        fields.push(("failover-timeout", master.failover_timeout.to_string()));
        fields.push(("parallel-syncs", master.parallel_syncs.to_string()));
        if let Some(s) = &master.notification_script {
            fields.push(("notification-script", s.clone()));
        }
        if let Some(s) = &master.client_reconfig_script {
            fields.push(("client-reconfig-script", s.clone()));
        }
    }
    if inst.kind == Kind::Replica {
        fields.push((
            "master-link-down-time",
            inst.master_link_down_time.to_string(),
        ));
        fields.push((
            "master-link-status",
            if inst.slave_master_link_up {
                "ok"
            } else {
                "err"
            }
            .into(),
        ));
        fields.push((
            "master-host",
            inst.slave_master_host.clone().unwrap_or_else(|| "?".into()),
        ));
        fields.push(("master-port", inst.slave_master_port.to_string()));
        fields.push(("slave-priority", inst.slave_priority.to_string()));
        fields.push(("slave-repl-offset", inst.slave_repl_offset.to_string()));
        fields.push((
            "replica-announced",
            u8::from(inst.replica_announced).to_string(),
        ));
    }
    if inst.kind == Kind::Sentinel {
        fields.push((
            "last-hello-message",
            (now - inst.last_hello_time).to_string(),
        ));
        fields.push((
            "voted-leader",
            inst.leader.clone().unwrap_or_else(|| "?".into()),
        ));
        fields.push(("voted-leader-epoch", inst.leader_epoch.to_string()));
    }
    let _ = FailoverState::None;
    out.fields(&fields);
}

fn info(state: &State, args: &[Vec<u8>], out: &mut Out) {
    let wanted: Vec<String> = args[1..].iter().map(|a| lower(a)).collect();
    let all = wanted.is_empty()
        || wanted
            .iter()
            .any(|w| w == "all" || w == "everything" || w == "default");
    let want = |s: &str| all || wanted.iter().any(|w| w == s);
    let mut s = String::new();
    let uptime = (mstime() - state.started) / 1000;
    if want("server") {
        let _ = write!(
            s,
            "# Server\r\nredis_version:7.2.4\r\nredis_mode:sentinel\r\nos:{} {}\r\narch_bits:64\r\n\
             process_id:{}\r\nrun_id:{}\r\ntcp_port:{}\r\nuptime_in_seconds:{uptime}\r\n\
             uptime_in_days:{}\r\nhz:10\r\nexecutable:tantalum\r\nconfig_file:{}\r\n\r\n",
            std::env::consts::OS,
            std::env::consts::ARCH,
            std::process::id(),
            state.myid,
            state.config.port,
            uptime / 86400,
            state.config.path.display()
        );
    }
    if want("clients") {
        s.push_str("# Clients\r\nconnected_clients:1\r\n\r\n");
    }
    if want("cpu") {
        s.push_str("# CPU\r\nused_cpu_sys:0.000000\r\nused_cpu_user:0.000000\r\n\r\n");
    }
    if want("stats") {
        s.push_str("# Stats\r\ntotal_connections_received:0\r\ntotal_commands_processed:0\r\n\r\n");
    }
    if want("sentinel") {
        let _ = write!(
            s,
            "# Sentinel\r\nsentinel_masters:{}\r\nsentinel_tilt:{}\r\nsentinel_tilt_since_seconds:{}\r\n\
             sentinel_running_scripts:{}\r\nsentinel_scripts_queue_length:0\r\n\
             sentinel_simulate_failure_flags:{}\r\n",
            state.masters.len(),
            u8::from(state.tilt),
            if state.tilt {
                (mstime() - state.tilt_start) / 1000
            } else {
                -1
            },
            state.running_scripts,
            u8::from(state.simfailure_crash_after_election)
                | (u8::from(state.simfailure_crash_after_promotion) << 1),
        );
        for (i, m) in state.masters.iter().enumerate() {
            let status = if m.me.o_down {
                "odown"
            } else if m.me.s_down {
                "sdown"
            } else {
                "ok"
            };
            let _ = write!(
                s,
                "master{i}:name={},status={status},address={}:{},slaves={},sentinels={}\r\n",
                m.name,
                m.me.host,
                m.me.port,
                m.replicas.len(),
                m.sentinels.len() + 1
            );
        }
    }
    while s.ends_with("\r\n\r\n") {
        s.truncate(s.len() - 2);
    }
    out.text(&s);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_parse_from_resp_and_inline() {
        let (args, used) = parse_command(b"*2\r\n$4\r\nPING\r\n$2\r\nhi\r\n")
            .unwrap()
            .unwrap();
        assert_eq!((args, used), (vec![b"PING".to_vec(), b"hi".to_vec()], 22));
        assert!(parse_command(b"*2\r\n$4\r\nPING\r\n").unwrap().is_none());
        let (args, _) = parse_command(b"sentinel masters\r\n").unwrap().unwrap();
        assert_eq!(args, vec![b"sentinel".to_vec(), b"masters".to_vec()]);
    }
}
