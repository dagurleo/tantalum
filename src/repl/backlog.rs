//! The replication stream as replicas read it: every byte propagated since
//! replication started, numbered by offset as Redis numbers it, kept as long
//! as the backlog's size asks (`repl-backlog-size`, for a replica that comes
//! back to continue where it left off) or a connected replica hasn't read it
//! yet (as Redis 7's shared replication buffer keeps it), up to a limit per
//! replica (`client-output-buffer-limit replica`'s hard limit: 256 MB), past
//! which that replica is dropped.
//!
//! Writes append while holding the write turn; each replica's feed thread
//! copies out what it hasn't sent under the lock and writes it to its socket
//! after. A feed with nothing to send waits on its bell, which an append rings
//! only if the feed said it's waiting, so a busy stream costs appends no
//! syscalls.

use std::collections::VecDeque;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::sync::Arc;

use parking_lot::Mutex;

/// How far a replica may fall behind before it's dropped: Redis'
/// `client-output-buffer-limit replica 256mb`.
pub(crate) const REPLICA_LIMIT: u64 = 256 * 1024 * 1024;

/// A feed's wakeup: a byte on a socket pair, which its thread polls with
/// its replica's socket.
pub(crate) struct Bell {
    ring: UnixStream,
    pub(crate) ear: UnixStream,
}

impl Bell {
    pub(crate) fn new() -> std::io::Result<Self> {
        let (ring, ear) = UnixStream::pair()?;
        ring.set_nonblocking(true)?;
        ear.set_nonblocking(true)?;
        Ok(Bell { ring, ear })
    }

    pub(crate) fn ring(&self) {
        // A full buffer means it's ringing already.
        let _ = (&self.ring).write(&[1]);
    }

    /// Empties it, after waking.
    pub(crate) fn hush(&self) {
        let mut buf = [0u8; 64];
        while matches!(std::io::Read::read(&mut &self.ear, &mut buf), Ok(n) if n > 0) {}
    }
}

/// A replica reading the stream: where it is, and its bell.
struct Reader {
    id: u64,
    /// The offset of the next byte it reads (the bytes before are sent).
    next: u64,
    bell: Arc<Bell>,
    /// It said it's waiting for more: the next append rings.
    waiting: bool,
    /// Fell too far behind: dropped.
    dropped: bool,
}

struct Inner {
    /// The bytes held, from offset `start` on.
    data: VecDeque<u8>,
    start: u64,
    /// The size to keep for replicas that come back (`repl-backlog-size`).
    size: usize,
    /// The database the stream's commands are in by now, if a replica knows:
    /// `None` makes the next command `SELECT` its own.
    db: Option<u32>,
    readers: Vec<Reader>,
}

impl Inner {
    /// The offset of the byte after the last: Redis' `master_repl_offset`.
    fn end(&self) -> u64 {
        self.start + self.data.len() as u64
    }

    /// Drops what neither the size nor a reader keeps, and readers too far
    /// behind.
    fn trim(&mut self) {
        let end = self.end();
        for reader in &mut self.readers {
            if !reader.dropped && end - reader.next > REPLICA_LIMIT {
                reader.dropped = true;
                reader.bell.ring();
            }
        }
        let keep_from = self
            .readers
            .iter()
            .filter(|r| !r.dropped)
            .map(|r| r.next)
            .min()
            .unwrap_or(end)
            .min(end.saturating_sub(self.size as u64))
            .max(self.start);
        let drop = (keep_from - self.start) as usize;
        if drop > 0 {
            self.data.drain(..drop);
            self.start = keep_from;
            if self.data.capacity() > 4 * self.size.max(1 << 20) && self.data.len() < self.size {
                self.data.shrink_to(self.size.max(self.data.len()));
            }
        }
    }
}

/// What a replica gets for asking for more.
pub(crate) enum Read {
    /// Some, appended to its buffer.
    Some,
    /// Nothing new: it waits on its bell, which rings with the next.
    Waiting,
    /// It fell too far behind, and was dropped.
    Dropped,
}

pub(crate) struct Backlog {
    inner: Mutex<Inner>,
}

impl Backlog {
    /// A stream that starts after offset `offset` (the next byte is
    /// `offset + 1`'s, numbered from 1 as Redis numbers them).
    pub(crate) fn new(size: usize, offset: u64) -> Self {
        Backlog {
            inner: Mutex::new(Inner {
                data: VecDeque::new(),
                start: offset,
                size,
                db: None,
                readers: Vec::new(),
            }),
        }
    }

    /// Redis' `master_repl_offset`: how many bytes the stream has had.
    pub(crate) fn offset(&self) -> u64 {
        self.inner.lock().end()
    }

    /// Appends `f`'s bytes, given the database the stream is in (which it
    /// updates if it selects another): under the write turn.
    pub(crate) fn append(&self, f: impl FnOnce(&mut Option<u32>, &mut Vec<u8>)) {
        let mut buf = Vec::new();
        let mut inner = self.inner.lock();
        f(&mut inner.db, &mut buf);
        if buf.is_empty() {
            return;
        }
        inner.data.extend(&buf);
        for reader in &mut inner.readers {
            if std::mem::take(&mut reader.waiting) {
                reader.bell.ring();
            }
        }
        inner.trim();
    }

    /// Appends bytes as they are: what a replica's master sent, for its own
    /// replicas.
    pub(crate) fn append_raw(&self, bytes: &[u8]) {
        self.append(|_, buf| buf.extend_from_slice(bytes));
    }

    /// The next command starts by selecting its database (a replica that
    /// just synced doesn't know which the stream is in).
    pub(crate) fn forget_db(&self) {
        self.inner.lock().db = None;
    }

    /// A replica reads from offset `next` on (its next byte's), if the
    /// stream still has it: for a partial resync, or a full one that starts
    /// at the end.
    pub(crate) fn add_reader(&self, id: u64, next: u64, bell: Arc<Bell>) -> bool {
        let mut inner = self.inner.lock();
        if next < inner.start || next > inner.end() {
            return false;
        }
        inner.readers.push(Reader {
            id,
            next,
            bell,
            waiting: false,
            dropped: false,
        });
        true
    }

    pub(crate) fn remove_reader(&self, id: u64) {
        let mut inner = self.inner.lock();
        inner.readers.retain(|r| r.id != id);
        inner.trim();
    }

    /// Copies up to `max` bytes reader `id` hasn't read into `out`, and moves
    /// it past them.
    pub(crate) fn read(&self, id: u64, out: &mut Vec<u8>, max: usize) -> Read {
        let mut inner = self.inner.lock();
        let (start, end) = (inner.start, inner.end());
        let Some(at) = inner.readers.iter().position(|r| r.id == id) else {
            return Read::Dropped;
        };
        if inner.readers[at].dropped {
            return Read::Dropped;
        }
        let next = inner.readers[at].next;
        if next == end {
            inner.readers[at].waiting = true;
            return Read::Waiting;
        }
        let from = (next - start) as usize;
        let n = ((end - next) as usize).min(max);
        let (a, b) = inner.data.as_slices();
        let to = from + n;
        if from < a.len() {
            out.extend_from_slice(&a[from..to.min(a.len())]);
        }
        if to > a.len() {
            out.extend_from_slice(&b[from.saturating_sub(a.len())..to - a.len()]);
        }
        inner.readers[at].next += n as u64;
        inner.trim();
        Read::Some
    }

    /// `INFO`'s `repl_backlog_first_byte_offset` and `repl_backlog_histlen`.
    pub(crate) fn extent(&self) -> (u64, u64) {
        let inner = self.inner.lock();
        (inner.start + 1, inner.data.len() as u64)
    }

    pub(crate) fn size(&self) -> usize {
        self.inner.lock().size
    }

    pub(crate) fn set_size(&self, size: usize) {
        let mut inner = self.inner.lock();
        inner.size = size;
        inner.trim();
    }

    /// Starts over after offset `offset`, holding nothing: a replica that
    /// synced from its master anew, whose own replicas must too.
    pub(crate) fn reset(&self, offset: u64) {
        let mut inner = self.inner.lock();
        inner.data.clear();
        inner.data.shrink_to_fit();
        inner.start = offset;
        inner.db = None;
        for reader in &mut inner.readers {
            reader.dropped = true;
            reader.bell.ring();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bell() -> Arc<Bell> {
        Arc::new(Bell::new().unwrap())
    }

    #[test]
    fn offsets_count_every_byte() {
        let backlog = Backlog::new(16, 100);
        assert_eq!(backlog.offset(), 100);
        backlog.append_raw(b"abc");
        assert_eq!(backlog.offset(), 103);
        assert_eq!(backlog.extent(), (101, 3));
    }

    #[test]
    fn readers_get_what_came_after_them() {
        let backlog = Backlog::new(4, 0);
        backlog.append_raw(b"ab");
        assert!(backlog.add_reader(1, 2, bell()));
        let mut out = Vec::new();
        assert!(matches!(backlog.read(1, &mut out, 10), Read::Waiting));
        backlog.append_raw(b"cdef");
        assert!(matches!(backlog.read(1, &mut out, 3), Read::Some));
        assert!(matches!(backlog.read(1, &mut out, 10), Read::Some));
        assert_eq!(out, b"cdef");
    }

    #[test]
    fn a_slow_reader_keeps_its_bytes_and_the_size_the_rest() {
        let backlog = Backlog::new(2, 0);
        assert!(backlog.add_reader(1, 0, bell()));
        backlog.append_raw(b"abcdefgh");
        // The reader holds all of it.
        assert_eq!(backlog.extent(), (1, 8));
        let mut out = Vec::new();
        backlog.read(1, &mut out, 6);
        // Now the size: the last 2 (the reader needs the last 2 too).
        assert_eq!(backlog.extent(), (7, 2));
        backlog.remove_reader(1);
        assert!(!backlog.add_reader(2, 5, bell()));
        assert!(backlog.add_reader(3, 6, bell()));
        assert!(backlog.add_reader(4, 8, bell()));
    }

    #[test]
    fn a_reader_past_the_limit_is_dropped() {
        let backlog = Backlog::new(0, 0);
        assert!(backlog.add_reader(1, 0, bell()));
        let chunk = vec![0u8; 1 << 20];
        for _ in 0..=REPLICA_LIMIT >> 20 {
            backlog.append_raw(&chunk);
        }
        let mut out = Vec::new();
        assert!(matches!(backlog.read(1, &mut out, 10), Read::Dropped));
        assert_eq!(backlog.extent().1, 0);
    }

    #[test]
    fn appends_ring_a_waiting_reader_once() {
        let backlog = Backlog::new(4, 0);
        let b = bell();
        assert!(backlog.add_reader(1, 0, b.clone()));
        let mut out = Vec::new();
        assert!(matches!(backlog.read(1, &mut out, 10), Read::Waiting));
        backlog.append_raw(b"a");
        backlog.append_raw(b"b");
        let mut buf = [0u8; 8];
        assert_eq!(std::io::Read::read(&mut &b.ear, &mut buf).unwrap(), 1);
    }
}
