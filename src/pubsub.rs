//! Pub/sub: `PUBLISH` sends a message to every connection subscribed to its
//! channel, or to a pattern that matches it, and keeps nothing.
//!
//! The server's registry ([`PubSub`]) maps each channel to its subscribers,
//! in shards so that publishers on different threads rarely meet; patterns
//! are a list every `PUBLISH` matches against, as in Redis, skipped while
//! there are none. A subscribed connection has an outbox ([`Subscriber`]):
//! a publisher appends the encoded message to it and wakes the connection,
//! which sends whatever has gathered. Its own commands take what's waiting
//! first, so replies and messages go out in the order they happened, and
//! messages a command causes (a `PUBLISH` to a channel the client is
//! subscribed to) come after its reply, as in Redis 7.2.
//!
//! A client that doesn't read its messages is disconnected, as Redis'
//! default `client-output-buffer-limit pubsub 32mb 8mb 60` does: once its
//! outbox passes 32 MB, or stays over 8 MB for a minute.
//!
//! Connections that never subscribe pay nothing for this: their commands
//! check one flag they checked before anyway, and their loops one more.

use std::collections::{HashMap, HashSet};
use std::hash::BuildHasher;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
use std::task::Poll;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};

use crate::glob;
use crate::resp;
use crate::store::Store;
use crate::wake::{Mailbox, Wake};

type Hasher = foldhash::fast::RandomState;
type Subscribers = HashMap<u64, Arc<Subscriber>, Hasher>;
/// Channels (or patterns) and their subscribers.
type Registry = HashMap<Box<[u8]>, Subscribers, Hasher>;
/// Channel or pattern names.
type Names = Vec<Box<[u8]>>;

/// Outbox sizes past which a subscriber is disconnected: at once, or after
/// staying over the soft limit this long (Redis' defaults).
const HARD_LIMIT: usize = 32 << 20;
const SOFT_LIMIT: usize = 8 << 20;
const SOFT_SECONDS: Duration = Duration::from_secs(60);
/// An outbox reports its size to the store's client memory only when it
/// has changed by this much, as connection buffers do.
const MEMORY_SLACK: usize = 64 * 1024;
const SHARDS: usize = 64;

/// The server's subscriptions.
pub struct PubSub {
    channels: Channels,
    /// `SSUBSCRIBE`'s channels: a namespace of their own, as in Redis.
    shard_channels: Channels,
    patterns: RwLock<Registry>,
    /// How many patterns have subscribers, so `PUBLISH` can skip the lock.
    pattern_count: AtomicUsize,
}

/// Channels and their subscribers, sharded by channel.
struct Channels {
    shards: Box<[RwLock<Registry>]>,
    hasher: Hasher,
    /// Channels with subscribers.
    count: AtomicUsize,
}

impl Channels {
    fn new() -> Self {
        Channels {
            shards: (0..SHARDS).map(|_| RwLock::default()).collect(),
            hasher: Hasher::default(),
            count: AtomicUsize::new(0),
        }
    }

    fn shard(&self, channel: &[u8]) -> &RwLock<Registry> {
        &self.shards[self.hasher.hash_one(channel) as usize % SHARDS]
    }

    fn add(&self, channel: &[u8], subscriber: &Arc<Subscriber>) {
        let mut shard = self.shard(channel).write();
        let subscribers = shard.entry(channel.into()).or_insert_with(|| {
            self.count.fetch_add(1, Relaxed);
            Subscribers::default()
        });
        subscribers.insert(subscriber.id, subscriber.clone());
    }

    fn remove(&self, channel: &[u8], id: u64) {
        let mut shard = self.shard(channel).write();
        if let Some(subscribers) = shard.get_mut(channel) {
            subscribers.remove(&id);
            if subscribers.is_empty() {
                shard.remove(channel);
                self.count.fetch_sub(1, Relaxed);
            }
        }
    }

    /// Sends to the channel's subscribers; how many there were.
    fn publish<'b>(&self, channel: &[u8], kind: &Kind<'_>, body: &dyn Fn() -> &'b [u8]) -> usize {
        let shard = self.shard(channel).read();
        let Some(subscribers) = shard.get(channel) else {
            return 0;
        };
        for subscriber in subscribers.values() {
            subscriber.deliver(kind, body());
        }
        subscribers.len()
    }

    fn subscribers(&self, channel: &[u8]) -> usize {
        self.shard(channel)
            .read()
            .get(channel)
            .map_or(0, HashMap::len)
    }

    /// The channels connection `id` is subscribed to.
    fn of(&self, id: u64) -> Vec<Box<[u8]>> {
        let mut found = Vec::new();
        for shard in self.shards.iter() {
            let shard = shard.read();
            found.extend(
                shard
                    .iter()
                    .filter(|(_, subscribers)| subscribers.contains_key(&id))
                    .map(|(channel, _)| channel.clone()),
            );
        }
        found
    }

    /// The channels with subscribers that match `pattern` (all if `None`).
    fn matching(&self, pattern: Option<&[u8]>) -> Vec<Box<[u8]>> {
        let mut found = Vec::new();
        for shard in self.shards.iter() {
            let shard = shard.read();
            found.extend(
                shard
                    .keys()
                    .filter(|c| pattern.is_none_or(|p| glob::matches(p, c, false)))
                    .cloned(),
            );
        }
        found
    }
}

/// A message's leading parts, which differ by protocol: the array (or push)
/// header and its type, and for `pmessage` the pattern. The channel and
/// the message follow.
struct Kind<'a> {
    resp2: &'a [u8],
    resp3: &'a [u8],
}

const MESSAGE: Kind<'static> = Kind {
    resp2: b"*3\r\n$7\r\nmessage\r\n",
    resp3: b">3\r\n$7\r\nmessage\r\n",
};
const SMESSAGE: Kind<'static> = Kind {
    resp2: b"*3\r\n$8\r\nsmessage\r\n",
    resp3: b">3\r\n$8\r\nsmessage\r\n",
};

/// A message's channel and payload, as bulk strings.
fn body(channel: &[u8], message: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(channel.len() + message.len() + 32);
    resp::write_bulk(&mut body, channel);
    resp::write_bulk(&mut body, message);
    body
}

impl Default for PubSub {
    fn default() -> Self {
        PubSub {
            channels: Channels::new(),
            shard_channels: Channels::new(),
            patterns: RwLock::default(),
            pattern_count: AtomicUsize::new(0),
        }
    }
}

impl PubSub {
    pub fn new() -> Self {
        PubSub::default()
    }

    /// `PUBLISH`: sends `message` to the channel's subscribers and to those
    /// of patterns matching it. Returns how many got it (a client subscribed
    /// both ways counts, and gets it, twice).
    pub(crate) fn publish(&self, channel: &[u8], message: &[u8]) -> usize {
        // Encoded once there's someone to send it to.
        let encoded = std::cell::OnceCell::new();
        let body = || encoded.get_or_init(|| body(channel, message)).as_slice();
        let mut receivers = self.channels.publish(channel, &MESSAGE, &body);
        if self.pattern_count.load(Relaxed) > 0 {
            for (pattern, subscribers) in self.patterns.read().iter() {
                if glob::matches(pattern, channel, false) {
                    let mut resp2 = b"*4\r\n$8\r\npmessage\r\n".to_vec();
                    resp::write_bulk(&mut resp2, pattern);
                    let mut resp3 = resp2.clone();
                    resp3[0] = b'>';
                    let kind = Kind {
                        resp2: &resp2,
                        resp3: &resp3,
                    };
                    for subscriber in subscribers.values() {
                        subscriber.deliver(&kind, body());
                    }
                    receivers += subscribers.len();
                }
            }
        }
        receivers
    }

    /// `SPUBLISH`: to the shard channel's subscribers only.
    pub(crate) fn publish_shard(&self, channel: &[u8], message: &[u8]) -> usize {
        let encoded = std::cell::OnceCell::new();
        let body = || encoded.get_or_init(|| body(channel, message)).as_slice();
        self.shard_channels.publish(channel, &SMESSAGE, &body)
    }

    fn add_pattern(&self, pattern: &[u8], subscriber: &Arc<Subscriber>) {
        let mut patterns = self.patterns.write();
        let subscribers = patterns.entry(pattern.into()).or_insert_with(|| {
            self.pattern_count.fetch_add(1, Relaxed);
            Subscribers::default()
        });
        subscribers.insert(subscriber.id, subscriber.clone());
    }

    fn remove_pattern(&self, pattern: &[u8], id: u64) {
        let mut patterns = self.patterns.write();
        if let Some(subscribers) = patterns.get_mut(pattern) {
            subscribers.remove(&id);
            if subscribers.is_empty() {
                patterns.remove(pattern);
                self.pattern_count.fetch_sub(1, Relaxed);
            }
        }
    }

    /// `PUBSUB CHANNELS`, or with `shard`, `SHARDCHANNELS`.
    pub(crate) fn channels(&self, shard: bool, pattern: Option<&[u8]>) -> Vec<Box<[u8]>> {
        self.namespace(shard).matching(pattern)
    }

    /// `PUBSUB NUMSUB`, or with `shard`, `SHARDNUMSUB`, for one channel.
    pub(crate) fn subscribers(&self, shard: bool, channel: &[u8]) -> usize {
        self.namespace(shard).subscribers(channel)
    }

    /// What connection `id` is subscribed to: channels, patterns and shard
    /// channels (for `ACL SETUSER`, which looks through them all).
    pub(crate) fn subscriptions_of(&self, id: u64) -> (Names, Names, Names) {
        let patterns = self
            .patterns
            .read()
            .iter()
            .filter(|(_, subscribers)| subscribers.contains_key(&id))
            .map(|(pattern, _)| pattern.clone())
            .collect();
        (self.channels.of(id), patterns, self.shard_channels.of(id))
    }

    /// Channels with subscribers, for `INFO`.
    pub fn channel_count(&self, shard: bool) -> usize {
        self.namespace(shard).count.load(Relaxed)
    }

    /// Patterns with subscribers: `PUBSUB NUMPAT` and `INFO`.
    pub fn pattern_count(&self) -> usize {
        self.pattern_count.load(Relaxed)
    }

    fn namespace(&self, shard: bool) -> &Channels {
        if shard {
            &self.shard_channels
        } else {
            &self.channels
        }
    }
}

/// A subscribed connection's outbox.
pub struct Subscriber {
    id: u64,
    /// The protocol its messages are written in, which `HELLO` can change.
    resp3: AtomicBool,
    /// Whether its connection is subscribed to something: then a RESP2
    /// connection hears of tracked keys as messages (see `tracking.rs`).
    subscribed: AtomicBool,
    outbox: Mutex<Outbox>,
    store: Arc<Store>,
}

#[derive(Default)]
struct Outbox {
    bytes: Vec<u8>,
    /// Wakes the connection when the outbox stops being empty.
    wake: Option<Wake>,
    /// Over its output limit: the connection is to close.
    closed: bool,
    /// Since when it has been over the soft limit.
    over_soft: Option<Instant>,
    /// Of `bytes`, how much the store counts as client memory.
    reported: usize,
}

impl Subscriber {
    /// An outbox with no subscriptions yet: a connection's one outbox, for
    /// what it subscribes to, `MONITOR` and `CLIENT TRACKING`.
    pub(crate) fn alone(id: u64, store: Arc<Store>) -> Arc<Subscriber> {
        Arc::new(Subscriber {
            id,
            resp3: AtomicBool::new(false),
            subscribed: AtomicBool::new(false),
            outbox: Mutex::default(),
            store,
        })
    }

    pub(crate) fn is_resp3(&self) -> bool {
        self.resp3.load(Relaxed)
    }

    pub(crate) fn set_resp3(&self, resp3: bool) {
        self.resp3.store(resp3, Relaxed);
    }

    pub(crate) fn is_subscribed(&self) -> bool {
        self.subscribed.load(Relaxed)
    }

    /// Puts a message in the outbox: `header`, then `body`.
    pub(crate) fn push(&self, header: &[u8], body: &[u8]) {
        self.deliver(
            &Kind {
                resp2: header,
                resp3: header,
            },
            body,
        );
    }

    /// Puts bytes in the outbox as they are.
    pub(crate) fn deliver_line(&self, line: &[u8]) {
        self.deliver(
            &Kind {
                resp2: b"",
                resp3: b"",
            },
            line,
        );
    }

    fn deliver(&self, kind: &Kind<'_>, body: &[u8]) {
        let header = if self.resp3.load(Relaxed) {
            kind.resp3
        } else {
            kind.resp2
        };
        let mut outbox = self.outbox.lock();
        if outbox.closed {
            return;
        }
        let was_empty = outbox.bytes.is_empty();
        outbox.bytes.extend_from_slice(header);
        outbox.bytes.extend_from_slice(body);
        let len = outbox.bytes.len();
        if len > SOFT_LIMIT {
            let since = *outbox.over_soft.get_or_insert_with(Instant::now);
            if len > HARD_LIMIT || since.elapsed() >= SOFT_SECONDS {
                eprintln!(
                    "closing client {}: {len} bytes of pub/sub messages unread, over the output limit",
                    self.id
                );
                outbox.closed = true;
                outbox.bytes = Vec::new();
            }
        }
        self.account(&mut outbox);
        let wake = if was_empty || outbox.closed {
            outbox.wake.take()
        } else {
            None
        };
        drop(outbox);
        if let Some(wake) = wake {
            wake.wake();
        }
    }

    /// Counts the outbox against `maxmemory`, like connection buffers.
    fn account(&self, outbox: &mut Outbox) {
        let len = outbox.bytes.capacity();
        if len.abs_diff(outbox.reported) >= MEMORY_SLACK || (len == 0 && outbox.reported > 0) {
            self.store
                .add_client_memory(len as isize - outbox.reported as isize);
            outbox.reported = len;
        }
    }

    /// Resolves once there are messages to send (or the connection is to
    /// close). A monoio connection passes its thread's mailbox.
    pub fn ready<'a>(&'a self, home: Option<&'a Arc<Mailbox>>) -> impl Future<Output = ()> + 'a {
        std::future::poll_fn(move |cx| {
            let mut outbox = self.outbox.lock();
            if !outbox.bytes.is_empty() || outbox.closed {
                return Poll::Ready(());
            }
            outbox.wake = Some(Wake::new(cx, home));
            Poll::Pending
        })
    }

    /// Resolves once the connection is over its output limit, to abandon a
    /// write the client isn't reading.
    pub fn closed<'a>(&'a self, home: Option<&'a Arc<Mailbox>>) -> impl Future<Output = ()> + 'a {
        std::future::poll_fn(move |cx| {
            let mut outbox = self.outbox.lock();
            if outbox.closed {
                return Poll::Ready(());
            }
            outbox.wake = Some(Wake::new(cx, home));
            Poll::Pending
        })
    }

    /// Moves the waiting messages to `out`. `false` if the connection is
    /// over its output limit and must close.
    pub fn take(&self, out: &mut Vec<u8>) -> bool {
        let mut outbox = self.outbox.lock();
        if outbox.closed {
            return false;
        }
        if outbox.bytes.is_empty() {
            return true;
        }
        if out.is_empty() && out.capacity() < outbox.bytes.capacity() {
            std::mem::swap(out, &mut outbox.bytes);
        } else {
            out.extend_from_slice(&outbox.bytes);
            outbox.bytes.clear();
        }
        if outbox.bytes.capacity() > MEMORY_SLACK {
            outbox.bytes = Vec::new();
        }
        outbox.over_soft = None;
        self.account(&mut outbox);
        true
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        let reported = self.outbox.get_mut().reported;
        self.store.add_client_memory(-(reported as isize));
    }
}

/// What one connection is subscribed to. Dropping it unsubscribes from
/// everything.
pub struct Subscriptions {
    pubsub: Arc<PubSub>,
    me: Arc<Subscriber>,
    channels: HashSet<Box<[u8]>, Hasher>,
    patterns: HashSet<Box<[u8]>, Hasher>,
    shard_channels: HashSet<Box<[u8]>, Hasher>,
}

/// One of the three kinds of subscription.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum To {
    Channels,
    Patterns,
    ShardChannels,
}

impl Subscriptions {
    /// Subscriptions whose messages go to `me`, the connection's outbox.
    pub(crate) fn new(pubsub: &Arc<PubSub>, me: Arc<Subscriber>) -> Self {
        me.subscribed.store(true, Relaxed);
        Subscriptions {
            pubsub: pubsub.clone(),
            me,
            channels: HashSet::default(),
            patterns: HashSet::default(),
            shard_channels: HashSet::default(),
        }
    }

    pub fn subscriber(&self) -> &Arc<Subscriber> {
        &self.me
    }

    fn set(&mut self, to: To) -> &mut HashSet<Box<[u8]>, Hasher> {
        match to {
            To::Channels => &mut self.channels,
            To::Patterns => &mut self.patterns,
            To::ShardChannels => &mut self.shard_channels,
        }
    }

    /// Subscribes, if it isn't already.
    pub(crate) fn add(&mut self, to: To, name: &[u8]) {
        if !self.set(to).insert(name.into()) {
            return;
        }
        match to {
            To::Channels => self.pubsub.channels.add(name, &self.me),
            To::Patterns => self.pubsub.add_pattern(name, &self.me),
            To::ShardChannels => self.pubsub.shard_channels.add(name, &self.me),
        }
    }

    /// Unsubscribes, if it was subscribed.
    pub(crate) fn remove(&mut self, to: To, name: &[u8]) {
        if !self.set(to).remove(name) {
            return;
        }
        let id = self.me.id;
        match to {
            To::Channels => self.pubsub.channels.remove(name, id),
            To::Patterns => self.pubsub.remove_pattern(name, id),
            To::ShardChannels => self.pubsub.shard_channels.remove(name, id),
        }
    }

    /// What it's subscribed to of one kind.
    pub(crate) fn names(&self, to: To) -> Vec<Box<[u8]>> {
        match to {
            To::Channels => self.channels.iter().cloned().collect(),
            To::Patterns => self.patterns.iter().cloned().collect(),
            To::ShardChannels => self.shard_channels.iter().cloned().collect(),
        }
    }

    /// The count Redis replies to (un)subscribing: channels and patterns
    /// together, shard channels on their own.
    pub(crate) fn count(&self, to: To) -> usize {
        match to {
            To::ShardChannels => self.shard_channels.len(),
            _ => self.channels.len() + self.patterns.len(),
        }
    }

    /// How many of one kind, for `CLIENT LIST`.
    pub(crate) fn count_of(&self, to: To) -> usize {
        match to {
            To::Channels => self.channels.len(),
            To::Patterns => self.patterns.len(),
            To::ShardChannels => self.shard_channels.len(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.channels.is_empty() && self.patterns.is_empty() && self.shard_channels.is_empty()
    }
}

impl Drop for Subscriptions {
    fn drop(&mut self) {
        for to in [To::Channels, To::Patterns, To::ShardChannels] {
            for name in self.names(to) {
                self.remove(to, &name);
            }
        }
        self.me.subscribed.store(false, Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subscriptions(
        store: &Arc<Store>,
        pubsub: &Arc<PubSub>,
        id: u64,
        resp3: bool,
    ) -> Subscriptions {
        let me = Subscriber::alone(id, store.clone());
        me.set_resp3(resp3);
        Subscriptions::new(pubsub, me)
    }

    fn setup() -> (Arc<Store>, Arc<PubSub>) {
        let store = Arc::new(Store::new(4));
        let pubsub = Arc::new(PubSub::new());
        (store, pubsub)
    }

    fn sent(s: &Subscriptions) -> Vec<u8> {
        let mut out = Vec::new();
        assert!(s.subscriber().take(&mut out));
        out
    }

    #[test]
    fn publishing_counts_and_reaches_the_right_subscribers() {
        let (store, pubsub) = setup();
        let mut a = subscriptions(&store, &pubsub, 1, false);
        let mut b = subscriptions(&store, &pubsub, 2, true);
        a.add(To::Channels, b"news");
        a.add(To::Patterns, b"n*");
        b.add(To::ShardChannels, b"news");
        // Twice to one client: by channel and by pattern.
        assert_eq!(pubsub.publish(b"news", b"hi"), 2);
        assert_eq!(
            sent(&a),
            b"*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$2\r\nhi\r\n\
              *4\r\n$8\r\npmessage\r\n$2\r\nn*\r\n$4\r\nnews\r\n$2\r\nhi\r\n"
        );
        assert!(sent(&b).is_empty());
        assert_eq!(pubsub.publish_shard(b"news", b"s"), 1);
        assert_eq!(
            sent(&b),
            b">3\r\n$8\r\nsmessage\r\n$4\r\nnews\r\n$1\r\ns\r\n"
        );
        assert_eq!(pubsub.publish(b"nothing", b"x"), 1); // the pattern
        assert_eq!(pubsub.publish(b"other", b"x"), 0);
        assert_eq!(
            (
                pubsub.channel_count(false),
                pubsub.channel_count(true),
                pubsub.pattern_count()
            ),
            (1, 1, 1)
        );
        assert_eq!(pubsub.subscribers(false, b"news"), 1);
        assert_eq!(
            pubsub.channels(false, Some(b"n*")),
            vec![Box::from(&b"news"[..])]
        );
        a.remove(To::Patterns, b"n*");
        assert_eq!(pubsub.pattern_count(), 0);
        drop((a, b));
        assert_eq!(
            (
                pubsub.channel_count(false),
                pubsub.channel_count(true),
                pubsub.pattern_count()
            ),
            (0, 0, 0)
        );
    }

    #[test]
    fn a_subscriber_over_its_limit_is_cut_off_and_its_memory_given_back() {
        let (store, pubsub) = setup();
        let mut a = subscriptions(&store, &pubsub, 1, false);
        a.add(To::Channels, b"c");
        let big = vec![b'x'; 1 << 20];
        for _ in 0..4 {
            pubsub.publish(b"c", &big);
        }
        assert!(
            store.stats().client_bytes >= 4 << 20,
            "counted as client memory"
        );
        assert!(!sent(&a).is_empty());
        assert_eq!(store.stats().client_bytes, 0);
        for _ in 0..40 {
            pubsub.publish(b"c", &big);
        }
        let mut out = Vec::new();
        assert!(!a.subscriber().take(&mut out), "over 32 MB: to be closed");
        assert!(out.is_empty());
        assert_eq!(store.stats().client_bytes, 0);
        // It stays cut off; messages to it go nowhere.
        pubsub.publish(b"c", b"more");
        assert!(!a.subscriber().take(&mut out));
        drop(a);
        assert_eq!(store.stats().client_bytes, 0);
    }
}
