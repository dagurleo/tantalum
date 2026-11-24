//! The cluster bus: a thread of its own with a small runtime, listening on
//! the cluster port (the client port + 10000, or `--cluster-port`), with a
//! link to each node it knows (and the links they open to it), which frame
//! messages (`RCmb`, then the total length) and hand them to the state,
//! and the cron, ten times a second. After each, what the state asked of
//! the server is done, with the state's lock let go.

use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use super::Cluster;
use super::state::{self, Action, Link, State, log};
use super::wire::{self, Msg};
use crate::context::Context;

enum Event {
    Accepted(TcpStream),
    Connected { link: u64, stream: TcpStream },
    ConnectFailed { link: u64, name: wire::Name },
    Packet { link: u64, bytes: Vec<u8> },
    Closed(u64),
}

/// Starts the bus on `bind:cport`.
pub(crate) fn start(cluster: Arc<Cluster>, bind: Vec<String>) -> std::io::Result<()> {
    let cport = cluster.state.lock().settings.cport;
    let listeners: Vec<std::net::TcpListener> = bind
        .iter()
        .map(|b| std::net::TcpListener::bind((b.as_str(), cport)))
        .collect::<Result<_, _>>()?;
    for l in &listeners {
        l.set_nonblocking(true)?;
    }
    std::thread::Builder::new()
        .name("tantalum-cluster".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a runtime for the cluster bus");
            runtime.block_on(run(cluster, listeners));
        })?;
    Ok(())
}

async fn run(cluster: Arc<Cluster>, listeners: Vec<std::net::TcpListener>) {
    let (events_tx, mut events) = mpsc::unbounded_channel::<Event>();
    for l in listeners {
        let l = TcpListener::from_std(l).expect("a listener");
        let tx = events_tx.clone();
        tokio::spawn(async move {
            loop {
                if let Ok((stream, _)) = l.accept().await {
                    let _ = tx.send(Event::Accepted(stream));
                }
            }
        });
    }
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    loop {
        tokio::select! {
            _ = tick.tick() => {
                let connect = {
                    let mut state = cluster.state.lock();
                    refresh(&cluster, &mut state);
                    let connect = state.cron();
                    connect.into_iter().map(|(name, ip, cport)| {
                        let id = new_link(&mut state, Some(name), false, &events_tx);
                        if let Some(n) = state.nodes.get_mut(&name) {
                            n.link = Some(id);
                        }
                        (id, name, ip, cport)
                    }).collect::<Vec<_>>()
                };
                for (id, name, ip, cport) in connect {
                    let tx = events_tx.clone();
                    tokio::spawn(async move {
                        let timeout = Duration::from_millis(1000);
                        match tokio::time::timeout(timeout, TcpStream::connect((ip.as_str(), cport))).await {
                            Ok(Ok(stream)) => { let _ = tx.send(Event::Connected { link: id, stream }); }
                            _ => { let _ = tx.send(Event::ConnectFailed { link: id, name }); }
                        }
                    });
                }
            }
            Some(event) = events.recv() => {
                let mut state = cluster.state.lock();
                refresh(&cluster, &mut state);
                match event {
                    Event::Accepted(stream) => {
                        let id = new_link(&mut state, None, true, &events_tx);
                        attach(&mut state, id, stream, &events_tx);
                    }
                    Event::Connected { link, stream } => {
                        if state.links.contains_key(&link) {
                            attach(&mut state, link, stream, &events_tx);
                            state.connected(link);
                        }
                    }
                    Event::ConnectFailed { link, name } => {
                        state.free_link(link);
                        state.connect_failed(name);
                    }
                    Event::Packet { link, bytes } => {
                        if state.links.contains_key(&link) {
                            match Msg::decode(&bytes) {
                                Some(msg) => state.process(link, msg),
                                None => log("Received an invalid packet: dropped"),
                            }
                        }
                    }
                    Event::Closed(link) => state.free_link(link),
                }
            }
        }
        let actions = {
            let mut state = cluster.state.lock();
            state.before_sleep();
            std::mem::take(&mut state.actions)
        };
        if !actions.is_empty() {
            act(&cluster, actions);
        }
    }
}

/// What the server says of its replication, for the state.
fn refresh(cluster: &Cluster, state: &mut State) {
    let Some(ctx) = cluster.ctx() else { return };
    let mut repl = state::Repl {
        offset: ctx.repl.offset(),
        data_age: 0,
        following: ctx.repl.following(),
    };
    if let Some(link) = ctx.repl.link.lock().as_ref() {
        repl.data_age = if link.up() {
            link.last_io_seconds() as i64 * 1000
        } else {
            link.down_seconds() as i64 * 1000
        };
    }
    state.repl = repl;
}

/// A link's handle, before its socket is there (messages queue).
fn new_link(
    state: &mut State,
    node: Option<wire::Name>,
    inbound: bool,
    _events: &mpsc::UnboundedSender<Event>,
) -> u64 {
    state.next_link += 1;
    let id = state.next_link;
    let (tx, rx) = mpsc::unbounded_channel::<Vec<u8>>();
    state.links.insert(
        id,
        Link {
            node,
            inbound,
            ctime: state::mstime(),
            tx,
            rx: Some(rx),
            abort: None,
            peer_ip: String::new(),
            local_ip: String::new(),
            send_bytes: 0,
        },
    );
    id
}

/// Gives link `id` its socket: a task writes what's queued for it and
/// reads its messages.
fn attach(state: &mut State, id: u64, stream: TcpStream, events: &mpsc::UnboundedSender<Event>) {
    let Some(link) = state.links.get_mut(&id) else {
        return;
    };
    let _ = stream.set_nodelay(true);
    link.peer_ip = stream
        .peer_addr()
        .map(|a| a.ip().to_string())
        .unwrap_or_default();
    link.local_ip = stream
        .local_addr()
        .map(|a| a.ip().to_string())
        .unwrap_or_default();
    // What was queued before goes first.
    let Some(mut rx) = link.rx.take() else { return };
    let events = events.clone();
    let task = tokio::spawn(async move {
        let (mut read, mut write) = stream.into_split();
        let writer = async move {
            while let Some(bytes) = rx.recv().await {
                if write.write_all(&bytes).await.is_err() {
                    break;
                }
            }
        };
        let reader = async {
            let mut buf = Vec::with_capacity(8192);
            let mut chunk = vec![0u8; 16 * 1024];
            loop {
                // Whole messages first.
                loop {
                    if buf.len() < 8 {
                        break;
                    }
                    let total = u32::from_be_bytes(buf[4..8].try_into().expect("4 bytes")) as usize;
                    if &buf[..4] != b"RCmb" || total < wire::HEADER {
                        return;
                    }
                    if buf.len() < total {
                        break;
                    }
                    let bytes: Vec<u8> = buf.drain(..total).collect();
                    if events.send(Event::Packet { link: id, bytes }).is_err() {
                        return;
                    }
                }
                match read.read(&mut chunk).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                }
            }
        };
        tokio::select! {
            _ = writer => {}
            _ = reader => {}
        }
        let _ = events.send(Event::Closed(id));
    });
    link.abort = Some(task.abort_handle());
}

/// Does what the state asked of the server.
fn act(cluster: &Cluster, actions: Vec<Action>) {
    let Some(ctx) = cluster.ctx() else { return };
    for action in actions {
        match action {
            Action::Follow(ip, port) => {
                let same = ctx
                    .repl
                    .link
                    .lock()
                    .as_ref()
                    .is_some_and(|l| l.host == ip && l.port == port);
                if !same && !ip.is_empty() && port != 0 {
                    crate::repl::replica::follow(&ctx, ip, port);
                }
            }
            Action::Promote => crate::repl::replica::promote(&ctx),
            Action::PromoteAndEmpty => {
                crate::repl::replica::promote(&ctx);
                super::empty_data(&ctx);
            }
            Action::Pause(until) => ctx.pause_for_failover(until != 0),
            Action::DeleteSlot(slot) => super::delete_keys_in_slot(&ctx, slot),
            Action::Publish {
                channel,
                message,
                shard,
            } => {
                if shard {
                    ctx.pubsub.publish_shard(&channel, &message);
                } else {
                    ctx.pubsub.publish(&channel, &message);
                }
            }
        }
    }
}

/// Runs what a command did to the state that needs the server or the bus
/// now (from a client's thread).
pub(crate) fn after_command(cluster: &Cluster) {
    let actions = std::mem::take(&mut cluster.state.lock().actions);
    if !actions.is_empty() {
        act(cluster, actions);
    }
}

pub(crate) fn weak(ctx: &Arc<Context>) -> Weak<Context> {
    Arc::downgrade(ctx)
}
