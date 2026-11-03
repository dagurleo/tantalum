//! Snapshots: the whole dataset written to a file now and then (and on
//! shutdown), and read back at startup, so data survives restarts. Like
//! Redis' RDB files, though in a format of our own:
//!
//! ```text
//! "TANTALUM SNAPSHOT 4\n"
//! per key:  kind, key length, key, value length, value, expiry
//! trailer:  0x00, key count (u64 LE), CRC32 of everything before it (u32 LE)
//! ```
//!
//! The kind is 0x01 for a string, 0x02 a hash, 0x03 a list, 0x04 a set,
//! 0x05 a sorted set and 0x06 a stream; a collection's value is its packed
//! encoding (see `hash.rs`, `set.rs`; for a stream, its chunks as they are,
//! `stream.rs`), however big it is in memory. Lengths and expiries
//! are LEB128 varints; an expiry is a Unix time in milliseconds, 0 for none,
//! so TTLs carry over a restart and keys that expired meanwhile are dropped
//! on load.
//!
//! Version 1 has only strings, version 2 adds hashes, version 3 the other
//! collections and version 4 streams. A save writes the lowest version that holds its data,
//! so a build from before a kind existed can still read a snapshot without
//! it (after a rollback, say), and loading reads every version.
//!
//! A save copies one shard at a time under its lock, so it's consistent per
//! shard rather than across the whole store: a write made during a save may
//! be in it or not. It writes to `<file>.tmp`, syncs it to disk and renames
//! it over the old snapshot, so a crash mid-save leaves the previous one.

use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use crate::store::{Expiry, Kind, NO_EXPIRY, Store};
use crate::{collection, hash};

/// The header, with the version in its next-to-last byte.
const MAGIC: &[u8] = b"TANTALUM SNAPSHOT 4\n";
const VERSION_AT: usize = MAGIC.len() - 2;
const MAGIC_V1: &[u8] = b"TANTALUM SNAPSHOT 1\n";
const MAGIC_V2: &[u8] = b"TANTALUM SNAPSHOT 2\n";
const MAGIC_V3: &[u8] = b"TANTALUM SNAPSHOT 3\n";
const STRING: u8 = 1;
const HASH: u8 = 2;
const LIST: u8 = 3;
const SET: u8 = 4;
const ZSET: u8 = 5;
const STREAM: u8 = 6;
const END: u8 = 0;

/// A record's tag and the first format version that has it.
fn tag_of(kind: Kind) -> (u8, u8) {
    match kind {
        Kind::String => (STRING, b'1'),
        Kind::Hash => (HASH, b'2'),
        Kind::List => (LIST, b'3'),
        Kind::Set => (SET, b'3'),
        Kind::ZSet => (ZSET, b'3'),
        Kind::Stream => (STREAM, b'4'),
    }
}

fn unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn put_varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        out.push(n as u8 | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

/// Writes through to `inner`, keeping a CRC32 of everything written.
struct Checksummed<W> {
    inner: W,
    crc: crc32fast::Hasher,
}

impl<W: Write> Write for Checksummed<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.crc.update(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Writes a snapshot of `store` to `path`. Returns how many keys it holds.
pub fn save(store: &Store, path: &Path) -> io::Result<u64> {
    let tmp = tmp_path(path);
    let written = write_to(store, &tmp);
    if written.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    let keys = written?;
    fs::rename(&tmp, path)?;
    // Make the rename itself durable.
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        File::open(dir)?.sync_all()?;
    }
    Ok(keys)
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".tmp");
    PathBuf::from(name)
}

fn write_to(store: &Store, tmp: &Path) -> io::Result<u64> {
    let file = File::create(tmp)?;
    let mut out = Checksummed {
        inner: BufWriter::with_capacity(1 << 20, file),
        crc: crc32fast::Hasher::new(),
    };
    // Version 1 until a hash turns up; the CRC is of what follows, combined
    // with the header's at the end.
    let mut magic = MAGIC_V1.to_vec();
    out.inner.write_all(&magic)?;
    let now = store.now();
    // Store-clock deadlines become Unix times by this offset.
    let offset = unix_ms() - now as i64;
    let mut keys = 0u64;
    let mut buf = Vec::new();
    for shard in 0..store.shard_count() {
        buf.clear();
        store.for_each_live(shard, now, |key, kind, value, deadline| {
            let (tag, version) = tag_of(kind);
            magic[VERSION_AT] = magic[VERSION_AT].max(version);
            buf.push(tag);
            put_varint(&mut buf, key.len() as u64);
            buf.extend_from_slice(key);
            put_varint(&mut buf, value.len() as u64);
            buf.extend_from_slice(value);
            let expires = match deadline {
                NO_EXPIRY => 0,
                deadline => (deadline as i64 + offset).max(1) as u64,
            };
            put_varint(&mut buf, expires);
            keys += 1;
        });
        out.write_all(&buf)?;
    }
    out.write_all(&[END])?;
    out.write_all(&keys.to_le_bytes())?;
    let mut file = out.inner.into_inner().map_err(|e| e.into_error())?;
    if magic != MAGIC_V1 {
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&magic)?;
        file.seek(SeekFrom::End(0))?;
    }
    let mut crc = crc32fast::Hasher::new();
    crc.update(&magic);
    crc.combine(&out.crc);
    file.write_all(&crc.finalize().to_le_bytes())?;
    file.sync_all()?;
    Ok(keys)
}

/// What [`load`] found.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Loaded {
    pub keys: u64,
    /// Keys whose TTL ran out while the server was down.
    pub expired: u64,
    /// Keys left out because `maxmemory` was reached with `noeviction`.
    pub dropped: u64,
}

/// Loads the snapshot at `path` into `store`: `Ok(None)` if there's none.
/// The checksum is verified before anything is loaded, so a damaged file
/// loads nothing.
pub fn load(store: &Store, path: &Path) -> io::Result<Option<Loaded>> {
    let file = match File::open(path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        file => file?,
    };
    let len = file.metadata()?.len();
    if len < (MAGIC.len() + 1 + 8 + 4) as u64 {
        return Err(invalid("too short to be a snapshot"));
    }
    verify(&file, len)?;
    let mut reader = BufReader::with_capacity(1 << 20, File::open(path)?);
    let mut magic = [0; MAGIC.len()];
    reader.read_exact(&mut magic)?;
    let version = match &magic[..] {
        MAGIC_V1 => 1,
        MAGIC_V2 => 2,
        MAGIC_V3 => 3,
        MAGIC => 4,
        _ if magic.starts_with(&MAGIC[..VERSION_AT]) => {
            return Err(invalid(
                "a snapshot from a newer Tantalum: upgrade to load it",
            ));
        }
        _ => return Err(invalid("not a Tantalum snapshot")),
    };
    let now = store.now();
    let unix_now = unix_ms();
    let mut loaded = Loaded::default();
    let (mut key, mut value) = (Vec::new(), Vec::new());
    let mut records = 0u64;
    loop {
        let mut tag = [0];
        reader.read_exact(&mut tag)?;
        let kind = match tag[0] {
            STRING => Kind::String,
            HASH if version >= 2 => Kind::Hash,
            LIST if version >= 3 => Kind::List,
            SET if version >= 3 => Kind::Set,
            ZSET if version >= 3 => Kind::ZSet,
            STREAM if version >= 4 => Kind::Stream,
            END => break,
            other => return Err(invalid(format!("unknown record type {other}"))),
        };
        records += 1;
        read_bytes(&mut reader, &mut key)?;
        read_bytes(&mut reader, &mut value)?;
        let expires = read_varint(&mut reader)?;
        let valid = match kind {
            Kind::String => true,
            Kind::Hash => hash::is_valid(&value),
            _ => collection::is_valid(kind, &value),
        };
        if !valid {
            return Err(invalid(format!("a damaged {}", kind.name())));
        }
        if loaded.dropped > 0 {
            // Full: count the rest without trying.
            loaded.dropped += 1;
            continue;
        }
        let expiry = match expires {
            0 => Expiry::Never,
            t if t as i64 <= unix_now => {
                loaded.expired += 1;
                continue;
            }
            t => Expiry::At(store.deadline_from_unix_ms(now, t as i64)),
        };
        match store.restore(&key, kind, &value, expiry, now) {
            Ok(()) => loaded.keys += 1,
            Err(_) => loaded.dropped += 1,
        }
    }
    let mut count = [0; 8];
    reader.read_exact(&mut count)?;
    if u64::from_le_bytes(count) != records {
        return Err(invalid("key count doesn't match"));
    }
    Ok(Some(loaded))
}

/// Checks the trailing CRC32 against the rest of the file.
fn verify(file: &File, len: u64) -> io::Result<()> {
    let mut reader = BufReader::with_capacity(1 << 20, file);
    let mut crc = crc32fast::Hasher::new();
    let mut body = (&mut reader).take(len - 4);
    let mut chunk = vec![0; 1 << 16];
    loop {
        let n = body.read(&mut chunk)?;
        if n == 0 {
            break;
        }
        crc.update(&chunk[..n]);
    }
    let mut stored = [0; 4];
    reader.read_exact(&mut stored)?;
    if crc.finalize() != u32::from_le_bytes(stored) {
        return Err(invalid("checksum mismatch: the file is damaged"));
    }
    Ok(())
}

fn read_varint(reader: &mut impl Read) -> io::Result<u64> {
    let mut n = 0u64;
    for shift in (0..64).step_by(7) {
        let mut byte = [0];
        reader.read_exact(&mut byte)?;
        n |= u64::from(byte[0] & 0x7f) << shift;
        if byte[0] < 0x80 {
            return Ok(n);
        }
    }
    Err(invalid("varint too long"))
}

fn read_bytes(reader: &mut impl Read, buf: &mut Vec<u8>) -> io::Result<()> {
    let len = read_varint(reader)?;
    // Same cap as the protocol's bulk length, so a bad length can't make
    // us allocate wildly (the checksum makes this a can't-happen anyway).
    if len > crate::resp::MAX_BULK_LEN as u64 {
        return Err(invalid("entry too long"));
    }
    buf.resize(len as usize, 0);
    reader.read_exact(buf)
}

/// Snapshots of one store to one file: saving on demand, in the
/// background, and periodically when something changed.
pub struct Snapshots {
    pub path: PathBuf,
    /// How often to save if anything changed; `None` saves only on demand
    /// and at shutdown.
    pub interval: Option<Duration>,
    store: Arc<Store>,
    /// Held while saving, so saves never overlap.
    saving: Mutex<()>,
    in_background: AtomicBool,
    status: Mutex<Status>,
}

#[derive(Clone, Copy, Debug)]
pub struct Status {
    /// Unix seconds of the last successful save (or of startup).
    pub last_save: i64,
    pub last_ok: bool,
    /// `Store::changes` as of the last successful save's start.
    pub changes_at_last_save: u64,
}

impl Snapshots {
    /// Snapshots of `store` at `path`. Call after loading, so that what was
    /// loaded doesn't count as unsaved changes.
    pub fn new(store: Arc<Store>, path: PathBuf, interval: Option<Duration>) -> Arc<Self> {
        let changes = store.changes();
        Arc::new(Snapshots {
            path,
            interval,
            store,
            saving: Mutex::new(()),
            in_background: AtomicBool::new(false),
            status: Mutex::new(Status {
                last_save: unix_ms() / 1000,
                last_ok: true,
                changes_at_last_save: changes,
            }),
        })
    }

    /// Saves now, after any save in progress. Returns how many keys it wrote.
    pub fn save(&self) -> io::Result<u64> {
        let _saving = self.saving.lock();
        let changes = self.store.changes();
        let started = Instant::now();
        let result = save(&self.store, &self.path);
        let mut status = self.status.lock();
        status.last_ok = result.is_ok();
        match &result {
            Ok(keys) => {
                status.last_save = unix_ms() / 1000;
                status.changes_at_last_save = changes;
                if started.elapsed() > Duration::from_secs(1) {
                    eprintln!(
                        "snapshot: saved {keys} keys in {:.1} s",
                        started.elapsed().as_secs_f64()
                    );
                }
            }
            Err(e) => eprintln!("snapshot: saving {} failed: {e}", self.path.display()),
        }
        result
    }

    /// Starts a save on another thread, unless one is already running.
    /// Returns whether it started.
    pub fn save_in_background(self: &Arc<Self>) -> bool {
        if self.in_background.swap(true, Relaxed) {
            return false;
        }
        let this = self.clone();
        std::thread::Builder::new()
            .name("tantalum-save".into())
            .spawn(move || {
                let _ = this.save();
                this.in_background.store(false, Relaxed);
            })
            .is_ok()
    }

    pub fn saving_in_background(&self) -> bool {
        self.in_background.load(Relaxed)
    }

    pub fn status(&self) -> Status {
        *self.status.lock()
    }

    /// Changes made since the last successful save started.
    pub fn unsaved_changes(&self) -> u64 {
        self.store.changes() - self.status().changes_at_last_save
    }

    /// Saves every `interval` if anything changed, until the process ends.
    pub fn spawn_periodic(self: &Arc<Self>) -> io::Result<()> {
        let Some(interval) = self.interval else {
            return Ok(());
        };
        let this = self.clone();
        std::thread::Builder::new()
            .name("tantalum-snapshots".into())
            .spawn(move || {
                loop {
                    std::thread::sleep(interval);
                    if this.unsaved_changes() > 0 {
                        let _ = this.save();
                    }
                }
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{Condition, Config};

    fn temp_path(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("tantalum-snapshot-{}-{name}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        dir.join("data.snapshot")
    }

    fn get(store: &Store, key: &[u8]) -> Option<Vec<u8>> {
        store.get_with(key, store.now(), |v| v.map(|v| v.as_bytes().to_vec()))
    }

    fn filled() -> Store {
        let s = Store::new(8);
        let now = s.now();
        for i in 0..5_000u32 {
            let value = format!("value-{i}").repeat(i as usize % 7 + 1);
            s.set(
                &i.to_le_bytes(),
                value.as_bytes(),
                Condition::Always,
                Expiry::Never,
                now,
                false,
            )
            .unwrap();
        }
        // A value too big to pack, an empty key and value, a binary key, a TTL.
        s.set(
            b"big",
            &vec![7; 100_000],
            Condition::Always,
            Expiry::Never,
            now,
            false,
        )
        .unwrap();
        s.set(b"", b"", Condition::Always, Expiry::Never, now, false)
            .unwrap();
        s.set(
            b"\0\r\n\xff",
            b"bin",
            Condition::Always,
            Expiry::Never,
            now,
            false,
        )
        .unwrap();
        s.set(
            b"ttl",
            b"t",
            Condition::Always,
            Expiry::At(now + 3_600_000),
            now,
            false,
        )
        .unwrap();
        s
    }

    #[test]
    fn roundtrip() {
        let path = temp_path("roundtrip");
        let s = filled();
        assert_eq!(save(&s, &path).unwrap(), 5_004);
        let t = Store::new(4);
        let loaded = load(&t, &path).unwrap().unwrap();
        assert_eq!(
            loaded,
            Loaded {
                keys: 5_004,
                expired: 0,
                dropped: 0
            }
        );
        assert_eq!(t.len(), 5_004);
        for i in 0..5_000u32 {
            assert_eq!(
                get(&t, &i.to_le_bytes()),
                get(&s, &i.to_le_bytes()),
                "key {i}"
            );
        }
        for key in [&b"big"[..], b"", b"\0\r\n\xff", b"ttl"] {
            assert_eq!(get(&t, key), get(&s, key));
        }
        // The TTL came back about an hour out.
        let ttl = t.update_key(t.key(b"ttl"), t.now(), |c| {
            (crate::store::Update::Peek, c.map(|c| c.expiry))
        });
        match ttl.unwrap() {
            Some(Expiry::At(deadline)) => {
                let left = deadline - t.now();
                assert!((3_590_000..=3_600_000).contains(&left), "{left}");
            }
            other => panic!("{other:?}"),
        }
        assert!(!tmp_path(&path).exists(), "temp file left behind");
        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    fn hash(pairs: &[(&[u8], &[u8])]) -> Vec<u8> {
        let mut edit = hash::Edit::new(None, pairs.len());
        for &(field, value) in pairs {
            edit.set(field, value);
        }
        edit.encode()
    }

    fn kind_and_value(store: &Store, key: &[u8]) -> Option<(Kind, Vec<u8>)> {
        store.get_with(key, store.now(), |v| {
            v.map(|v| (v.kind(), v.as_bytes().to_vec()))
        })
    }

    #[test]
    fn hashes_roundtrip_in_version_2() {
        let path = temp_path("hashes");
        let s = filled();
        let now = s.now();
        let small = hash(&[(b"name", b"tantalum"), (b"", b"")]);
        let fields: Vec<_> = (0..200)
            .map(|i| (format!("f{i}"), "v".repeat(100)))
            .collect();
        let pairs: Vec<_> = fields
            .iter()
            .map(|(f, v)| (f.as_bytes(), v.as_bytes()))
            .collect();
        let big = hash(&pairs);
        assert!(big.len() > 16_384, "big enough to be external");
        s.restore(b"h", Kind::Hash, &small, Expiry::Never, now)
            .unwrap();
        s.restore(b"big", Kind::Hash, &big, Expiry::At(now + 3_600_000), now)
            .unwrap();
        assert_eq!(save(&s, &path).unwrap(), 5_005);
        // Hashes need version 2, no more.
        assert!(fs::read(&path).unwrap().starts_with(MAGIC_V2));
        let t = Store::new(4);
        assert_eq!(load(&t, &path).unwrap().unwrap().keys, 5_005);
        assert_eq!(kind_and_value(&t, b"h"), Some((Kind::Hash, small)));
        assert_eq!(kind_and_value(&t, b"big"), Some((Kind::Hash, big)));
        assert_eq!(
            kind_and_value(&t, b"ttl"),
            Some((Kind::String, b"t".to_vec()))
        );
        // Without hashes, it's version 1 again.
        let u = filled();
        save(&u, &path).unwrap();
        assert!(fs::read(&path).unwrap().starts_with(MAGIC_V1));
        assert_eq!(load(&Store::new(1), &path).unwrap().unwrap().keys, 5_004);
        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn sets_roundtrip_in_version_3() {
        use crate::collection::Collection;
        use crate::set::{BigSet, Finished, SetMut};
        use crate::store::Update;
        let path = temp_path("sets");
        let s = filled();
        let now = s.now();
        // A packed integer set, a packed one of strings, a big one with a TTL.
        let packed = |members: &[&[u8]]| {
            let mut set = SetMut::new(members[0], members.len());
            members.iter().for_each(|m| _ = set.insert(m));
            match set.finish() {
                Some(Finished::Packed(bytes)) => bytes,
                _ => unreachable!(),
            }
        };
        let ints = packed(&[b"3", b"1", b"2"]);
        let strings = packed(&[b"b", b"a"]);
        s.restore(b"ints", Kind::Set, &ints, Expiry::Never, now)
            .unwrap();
        s.restore(b"strings", Kind::Set, &strings, Expiry::Never, now)
            .unwrap();
        let mut big = BigSet::default();
        for i in 0..1_000 {
            big.insert(format!("member{i}").as_bytes());
        }
        let mut made = Some(Collection::Set(big));
        s.update_collection(s.key(b"bigset"), now, false, |_| {
            (Update::Object(made.take().unwrap()), ())
        })
        .unwrap();
        s.update_key(s.key(b"bigset"), now, |_| {
            (Update::Expire(Expiry::At(now + 60_000)), ())
        })
        .unwrap();
        assert_eq!(save(&s, &path).unwrap(), 5_007);
        assert!(fs::read(&path).unwrap().starts_with(MAGIC_V3));
        let t = Store::new(4);
        assert_eq!(load(&t, &path).unwrap().unwrap().keys, 5_007);
        assert_eq!(kind_and_value(&t, b"ints"), Some((Kind::Set, ints)));
        assert_eq!(kind_and_value(&t, b"strings"), Some((Kind::Set, strings)));
        // The big one is big again, with its members and TTL.
        let (members, expiry) = t
            .update_collection(t.key(b"bigset"), t.now(), false, |entry| {
                let entry = entry.expect("loaded");
                let members = match entry.value {
                    crate::store::EntryValue::Object(Collection::Set(set)) => set.len(),
                    _ => panic!("not big"),
                };
                (Update::Peek, (members, entry.expiry))
            })
            .unwrap();
        assert_eq!(members, 1_000);
        assert!(matches!(expiry, Expiry::At(_)));
        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn streams_roundtrip_in_version_4() {
        use crate::collection::Collection;
        use crate::store::Update;
        use crate::stream::{BigStream, StreamId, Trim};
        let path = temp_path("streams");
        let s = Store::new(2);
        let now = s.now();
        // Chunks with entries removed from them, and an emptied stream that
        // keeps its last ID: trimming must go the same after a restart.
        let mut stream = BigStream::default();
        for i in 1..=350u64 {
            let value = "v".repeat((i % 40) as usize);
            stream.add(StreamId { ms: i, seq: 0 }, &[(b"f", value.as_bytes())]);
        }
        stream.delete(StreamId { ms: 120, seq: 0 });
        stream.trim(Trim::MaxLen(300), false, 0);
        let mut empty = BigStream::default();
        empty.add(StreamId { ms: 9, seq: 9 }, &[(b"a", b"b")]);
        empty.trim(Trim::MaxLen(0), false, 0);
        for (key, value) in [(&b"s"[..], stream.clone()), (b"empty", empty)] {
            let mut made = Some(Collection::Stream(value));
            s.update_collection(s.key(key), now, false, |_| {
                (Update::Object(made.take().unwrap()), ())
            })
            .unwrap();
        }
        assert_eq!(save(&s, &path).unwrap(), 2);
        assert!(fs::read(&path).unwrap().starts_with(MAGIC));
        let t = Store::new(4);
        assert_eq!(load(&t, &path).unwrap().unwrap().keys, 2);
        let mut reloaded = t
            .update_collection(t.key(b"s"), t.now(), false, |entry| {
                match entry.unwrap().value {
                    crate::store::EntryValue::Object(Collection::Stream(s)) => {
                        (Update::Peek, s.clone())
                    }
                    _ => panic!("not a stream"),
                }
            })
            .unwrap();
        assert_eq!(reloaded.to_compact(), stream.to_compact());
        assert_eq!(
            reloaded.trim(Trim::MaxLen(1), true, 0),
            stream.trim(Trim::MaxLen(1), true, 0)
        );
        let last = t
            .update_collection(t.key(b"empty"), t.now(), false, |entry| {
                match entry.unwrap().value {
                    crate::store::EntryValue::Object(Collection::Stream(s)) => {
                        (Update::Peek, (s.len(), s.last_id()))
                    }
                    _ => panic!("not a stream"),
                }
            })
            .unwrap();
        assert_eq!(last, (0, StreamId { ms: 9, seq: 9 }));
        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn sorted_sets_roundtrip() {
        use crate::collection::Collection;
        use crate::store::Update;
        use crate::zset::BigZSet;
        let path = temp_path("zsets");
        let s = Store::new(2);
        let now = s.now();
        let mut small = BigZSet::default();
        small.insert(b"a", 1.5);
        small.insert(b"b", f64::NEG_INFINITY);
        s.restore(b"small", Kind::ZSet, &small.to_packed(), Expiry::Never, now)
            .unwrap();
        let mut big = BigZSet::default();
        for i in 0..3_000 {
            big.insert(format!("member {i}").as_bytes(), (i % 17) as f64 * 0.5);
        }
        let packed = big.to_packed();
        let mut made = Some(Collection::ZSet(big));
        s.update_collection(s.key(b"big"), now, false, |_| {
            (Update::Object(made.take().unwrap()), ())
        })
        .unwrap();
        save(&s, &path).unwrap();
        // No streams: still version 3, which 0.14 reads.
        assert!(fs::read(&path).unwrap().starts_with(MAGIC_V3));
        let t = Store::new(4);
        assert_eq!(load(&t, &path).unwrap().unwrap().keys, 2);
        assert_eq!(
            kind_and_value(&t, b"small"),
            Some((Kind::ZSet, small.to_packed()))
        );
        let reloaded = t
            .update_collection(t.key(b"big"), t.now(), false, |entry| {
                match entry.unwrap().value {
                    crate::store::EntryValue::Object(Collection::ZSet(set)) => {
                        (Update::Peek, set.to_packed())
                    }
                    _ => panic!("not big"),
                }
            })
            .unwrap();
        assert_eq!(reloaded, packed);
        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn lists_roundtrip() {
        use crate::collection::Collection;
        use crate::list::BigList;
        use crate::store::Update;
        let path = temp_path("lists");
        let s = Store::new(2);
        let now = s.now();
        let mut small = BigList::default();
        for e in [&b"a"[..], b"", b"\0\r\n"] {
            small.push_back(e);
        }
        s.restore(b"small", Kind::List, &small.to_packed(), Expiry::Never, now)
            .unwrap();
        let mut big = BigList::default();
        for i in 0..5_000 {
            big.push_back(format!("element {i}").as_bytes());
        }
        big.push_back(&[9; 20_000]);
        let packed = big.to_packed();
        let mut made = Some(Collection::List(big));
        s.update_collection(s.key(b"big"), now, false, |_| {
            (Update::Object(made.take().unwrap()), ())
        })
        .unwrap();
        save(&s, &path).unwrap();
        let t = Store::new(4);
        assert_eq!(load(&t, &path).unwrap().unwrap().keys, 2);
        assert_eq!(
            kind_and_value(&t, b"small"),
            Some((Kind::List, small.to_packed()))
        );
        let reloaded = t
            .update_collection(t.key(b"big"), t.now(), false, |entry| {
                match entry.unwrap().value {
                    crate::store::EntryValue::Object(Collection::List(list)) => {
                        (Update::Peek, list.to_packed())
                    }
                    _ => panic!("not big"),
                }
            })
            .unwrap();
        assert_eq!(reloaded, packed);
        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// `tests/data/v1.snapshot` was saved by Tantalum 0.7.0, before hashes:
    /// `SET name tantalum`, `SET counter 73`, an empty key and value, a
    /// binary key, a 20,000-byte value and a key expiring in 2100.
    #[test]
    fn loads_version_1_from_before_hashes() {
        let path = temp_path("v1");
        fs::write(&path, include_bytes!("../tests/data/v1.snapshot")).unwrap();
        let t = Store::new(4);
        let loaded = load(&t, &path).unwrap().unwrap();
        assert_eq!(
            loaded,
            Loaded {
                keys: 6,
                expired: 0,
                dropped: 0
            }
        );
        let string = |v: &[u8]| Some((Kind::String, v.to_vec()));
        assert_eq!(kind_and_value(&t, b"name"), string(b"tantalum"));
        assert_eq!(kind_and_value(&t, b"counter"), string(b"73"));
        assert_eq!(kind_and_value(&t, b""), string(b""));
        assert_eq!(kind_and_value(&t, b"\0\r\n\xff"), string(b"binary"));
        assert_eq!(kind_and_value(&t, b"big"), string(&[b'x'; 20_000]));
        assert_eq!(kind_and_value(&t, b"forever"), string(b"until 2100"));
        let expiry = t.update_key(t.key(b"forever"), t.now(), |c| {
            (crate::store::Update::Peek, c.map(|c| c.expiry))
        });
        let Some(Expiry::At(deadline)) = expiry.unwrap() else {
            panic!("lost its TTL");
        };
        // Each conversion between the store's clock and Unix time reads the
        // wall clock, so a millisecond can tick between them.
        let unix = t.deadline_to_unix_ms(t.now(), deadline);
        assert!((unix - 4_102_444_800_000).abs() <= 2, "{unix}");
        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    /// A file with a good checksum around these records.
    fn file_of(magic: &[u8], records: &[u8], count: u64) -> Vec<u8> {
        let mut file = [magic, records, &[END], &count.to_le_bytes()].concat();
        let crc = crc32fast::hash(&file);
        file.extend(crc.to_le_bytes());
        file
    }

    #[test]
    fn rejects_hashes_that_should_not_be_there() {
        let path = temp_path("bad-hashes");
        let good = hash(&[(b"a", b"1")]);
        let record = |kind: u8, value: &[u8]| {
            let mut r = vec![kind, 1, b'h', value.len() as u8];
            r.extend_from_slice(value);
            r.push(0);
            r
        };
        // The good one loads...
        fs::write(&path, file_of(MAGIC, &record(HASH, &good), 1)).unwrap();
        let t = Store::new(1);
        load(&t, &path).unwrap();
        assert_eq!(kind_and_value(&t, b"h"), Some((Kind::Hash, good.clone())));
        for (what, file) in [
            (
                "a hash in version 1",
                file_of(MAGIC_V1, &record(HASH, &good), 1),
            ),
            (
                "a damaged hash",
                file_of(MAGIC, &record(HASH, b"\x02\x01a"), 1),
            ),
            ("an empty hash", file_of(MAGIC, &record(HASH, b"\x00"), 1)),
            ("an unknown kind", file_of(MAGIC, &record(9, b"x"), 1)),
            ("a newer version", file_of(b"TANTALUM SNAPSHOT 5\n", &[], 0)),
        ] {
            fs::write(&path, file).unwrap();
            let err = load(&Store::new(1), &path).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{what}: {err}");
        }
        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn missing_file_loads_nothing() {
        let path = temp_path("missing");
        assert_eq!(load(&Store::new(1), &path).unwrap(), None);
    }

    #[test]
    fn damage_is_detected_before_loading() {
        let path = temp_path("damage");
        save(&filled(), &path).unwrap();
        let good = fs::read(&path).unwrap();
        for (what, bytes) in [
            ("flipped byte", {
                let mut b = good.clone();
                b[good.len() / 2] ^= 1;
                b
            }),
            ("truncated", good[..good.len() - 10].to_vec()),
            ("empty", Vec::new()),
            ("extended", [&good[..], b"x"].concat()),
        ] {
            fs::write(&path, &bytes).unwrap();
            let t = Store::new(1);
            assert!(load(&t, &path).is_err(), "{what} not detected");
            assert_eq!(t.len(), 0, "{what}: loaded keys from a bad file");
        }
        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn expired_keys_are_dropped_on_load() {
        let path = temp_path("expired");
        let s = Store::new(1);
        let now = s.now();
        s.set(
            b"soon",
            b"x",
            Condition::Always,
            Expiry::At(now + 50),
            now,
            false,
        )
        .unwrap();
        s.set(b"stays", b"y", Condition::Always, Expiry::Never, now, false)
            .unwrap();
        save(&s, &path).unwrap();
        std::thread::sleep(Duration::from_millis(100));
        let t = Store::new(1);
        let loaded = load(&t, &path).unwrap().unwrap();
        assert_eq!((loaded.keys, loaded.expired), (1, 1));
        assert_eq!(get(&t, b"stays").as_deref(), Some(&b"y"[..]));
        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn a_full_store_drops_the_rest() {
        let path = temp_path("full");
        save(&filled(), &path).unwrap();
        let t = Store::with_config(Config {
            shards: 1,
            maxmemory: 64 * 1024,
            policy: crate::store::EvictionPolicy::NoEviction,
            ..Config::default()
        });
        let loaded = load(&t, &path).unwrap().unwrap();
        assert!(loaded.keys > 0 && loaded.dropped > 0, "{loaded:?}");
        assert_eq!(loaded.keys + loaded.dropped, 5_004);
        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn saves_only_what_changed() {
        let path = temp_path("changes");
        let store = Arc::new(Store::new(2));
        let snapshots = Snapshots::new(store.clone(), path.clone(), None);
        assert_eq!(snapshots.unsaved_changes(), 0);
        let now = store.now();
        store
            .set(b"a", b"1", Condition::Always, Expiry::Never, now, false)
            .unwrap();
        assert_eq!(snapshots.unsaved_changes(), 1);
        assert_eq!(snapshots.save().unwrap(), 1);
        assert_eq!(snapshots.unsaved_changes(), 0);
        assert!(snapshots.status().last_ok);
        assert!(snapshots.save_in_background());
        while snapshots.saving_in_background() {
            std::thread::sleep(Duration::from_millis(1));
        }
        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }
}
