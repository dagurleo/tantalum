//! A Sentinel's connections to what it watches: masters, their replicas, and
//! the other Sentinels. Each is a task that connects, authenticates, names
//! itself (`CLIENT SETNAME sentinel-<id>-cmd`, as Redis' Sentinel does), and
//! then sends what the main loop asks, handing each reply back to it as an
//! event, in order. A pub/sub link subscribes to `__sentinel__:hello` and
//! hands back what's published there.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

/// A reply, as an instance sends it (RESP2, and RESP3's kinds as their RESP2
/// equivalents).
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Value {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Value>>),
}

impl Value {
    pub(crate) fn text(&self) -> Option<String> {
        match self {
            Value::Simple(s) => Some(s.clone()),
            Value::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
            _ => None,
        }
    }
}

/// What a command is for, to know what to do with its reply.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Tag {
    Ping,
    Info,
    Publish,
    /// `SENTINEL is-master-down-by-addr`.
    IsMasterDown,
    /// A reply nothing waits for (`SLAVEOF`'s transaction, `SCRIPT KILL`).
    Discard,
}

/// Which link of an instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Which {
    Cmd,
    Pubsub,
}

/// What a link tells the main loop.
pub(crate) enum Happened {
    /// Connected (and authenticated, if it had to).
    Up,
    /// Gone, or never connected.
    Down,
    Reply(Tag, Value),
    /// A message on the hello channel (pub/sub link).
    Hello(Vec<u8>),
    /// Anything on the pub/sub link.
    Activity,
}

/// To the main loop: which instance (by its id), which of its links, and
/// which connection of that link (a link that reconnects starts a new one,
/// whose events the old one's mustn't be mistaken for).
pub(crate) struct Event {
    pub(crate) instance: u64,
    pub(crate) which: Which,
    pub(crate) generation: u64,
    pub(crate) happened: Happened,
}

/// A link to one instance, from the main loop's side.
pub(crate) struct Link {
    commands: Option<mpsc::UnboundedSender<(Vec<Vec<u8>>, Tag)>>,
}

/// How to log in to an instance, and what to call this connection.
#[derive(Clone)]
pub(crate) struct Login {
    pub(crate) user: Option<String>,
    pub(crate) pass: Option<String>,
    pub(crate) name: String,
}

impl Link {
    /// Starts connecting to `host:port`.
    pub(crate) fn connect(
        instance: u64,
        which: Which,
        generation: u64,
        host: String,
        port: u16,
        login: Login,
        events: mpsc::UnboundedSender<Event>,
    ) -> Link {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(run(
            instance, which, generation, host, port, login, rx, events,
        ));
        Link { commands: Some(tx) }
    }

    /// Queues a command; false if the link has gone.
    pub(crate) fn send(&self, args: Vec<Vec<u8>>, tag: Tag) -> bool {
        self.commands
            .as_ref()
            .is_some_and(|tx| tx.send((args, tag)).is_ok())
    }
}

pub(crate) fn args(words: &[&[u8]]) -> Vec<Vec<u8>> {
    words.iter().map(|w| w.to_vec()).collect()
}

fn encode(args: &[Vec<u8>], out: &mut Vec<u8>) {
    out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for arg in args {
        out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
}

#[allow(clippy::too_many_arguments)]
async fn run(
    instance: u64,
    which: Which,
    generation: u64,
    host: String,
    port: u16,
    login: Login,
    mut commands: mpsc::UnboundedReceiver<(Vec<Vec<u8>>, Tag)>,
    events: mpsc::UnboundedSender<Event>,
) {
    let tell = |happened| {
        let _ = events.send(Event {
            instance,
            which,
            generation,
            happened,
        });
    };
    let connected = tokio::time::timeout(
        Duration::from_secs(5),
        TcpStream::connect((host.as_str(), port)),
    )
    .await;
    let Ok(Ok(stream)) = connected else {
        tell(Happened::Down);
        return;
    };
    let _ = stream.set_nodelay(true);
    let (mut read, mut write) = stream.into_split();
    // What's sent first: the login, the name, and for pub/sub the subscription;
    // their replies are skipped (an error in them shows as the instance not
    // answering, or, for a wrong password, as its `PING`'s error).
    let mut first = Vec::new();
    let mut skip = 0usize;
    if let Some(pass) = &login.pass {
        let mut auth = vec![b"AUTH".to_vec()];
        if let Some(user) = &login.user {
            auth.push(user.clone().into_bytes());
        }
        auth.push(pass.clone().into_bytes());
        encode(&auth, &mut first);
        skip += 1;
    }
    encode(
        &args(&[b"CLIENT", b"SETNAME", login.name.as_bytes()]),
        &mut first,
    );
    skip += 1;
    if which == Which::Pubsub {
        encode(&args(&[b"SUBSCRIBE", b"__sentinel__:hello"]), &mut first);
    }
    if write.write_all(&first).await.is_err() {
        tell(Happened::Down);
        return;
    }
    tell(Happened::Up);
    // Tags of what's been sent, in order: replies come in that order.
    let (sent_tx, mut sent_rx) = mpsc::unbounded_channel::<Tag>();
    let writer = async move {
        while let Some((args, tag)) = commands.recv().await {
            let mut out = Vec::new();
            encode(&args, &mut out);
            if sent_tx.send(tag).is_err() || write.write_all(&out).await.is_err() {
                break;
            }
        }
    };
    let reader = async {
        let mut buf = Vec::with_capacity(16 * 1024);
        let mut chunk = vec![0u8; 16 * 1024];
        loop {
            let mut at = 0;
            while let Some((value, used)) = parse(&buf[at..]) {
                at += used;
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                if which == Which::Pubsub {
                    tell(Happened::Activity);
                    if let Value::Array(Some(items)) = &value
                        && items.len() == 3
                        && items[0].text().as_deref() == Some("message")
                        && let Value::Bulk(Some(message)) = &items[2]
                    {
                        tell(Happened::Hello(message.clone()));
                    }
                    continue;
                }
                let tag = sent_rx.recv().await.unwrap_or(Tag::Discard);
                tell(Happened::Reply(tag, value));
            }
            buf.drain(..at);
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
    tell(Happened::Down);
}

/// One reply from the start of `buf`, and how many bytes it took; `None` if
/// it's not all there yet.
pub(crate) fn parse(buf: &[u8]) -> Option<(Value, usize)> {
    let end = buf.windows(2).position(|w| w == b"\r\n")?;
    let line = &buf[1..end];
    let text = || String::from_utf8_lossy(line).into_owned();
    let int = || std::str::from_utf8(line).ok()?.parse::<i64>().ok();
    let after = end + 2;
    match *buf.first()? {
        b'+' => Some((Value::Simple(text()), after)),
        b'-' => Some((Value::Error(text()), after)),
        b':' => Some((Value::Int(int()?), after)),
        b'$' => {
            let n = int()?;
            if n < 0 {
                return Some((Value::Bulk(None), after));
            }
            let n = n as usize;
            if buf.len() < after + n + 2 {
                return None;
            }
            Some((
                Value::Bulk(Some(buf[after..after + n].to_vec())),
                after + n + 2,
            ))
        }
        b'*' | b'>' => {
            let n = int()?;
            if n < 0 {
                return Some((Value::Array(None), after));
            }
            let mut at = after;
            let mut items = Vec::with_capacity(n as usize);
            for _ in 0..n {
                let (item, used) = parse(&buf[at..])?;
                items.push(item);
                at += used;
            }
            Some((Value::Array(Some(items)), at))
        }
        _ => Some((Value::Error(format!("unexpected reply: {}", text())), after)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replies_parse_whole_or_not_at_all() {
        let all = b"*3\r\n$7\r\nmessage\r\n$4\r\nchan\r\n$2\r\nhi\r\n:5\r\n";
        let (value, used) = parse(all).unwrap();
        assert_eq!(
            value,
            Value::Array(Some(vec![
                Value::Bulk(Some(b"message".to_vec())),
                Value::Bulk(Some(b"chan".to_vec())),
                Value::Bulk(Some(b"hi".to_vec())),
            ]))
        );
        assert_eq!(parse(&all[used..]).unwrap().0, Value::Int(5));
        for cut in 0..used {
            assert!(parse(&all[..cut]).is_none(), "{cut}");
        }
        assert_eq!(
            parse(b"-ERR no\r\n").unwrap().0,
            Value::Error("ERR no".into())
        );
        assert_eq!(parse(b"$-1\r\n").unwrap().0, Value::Bulk(None));
    }
}
