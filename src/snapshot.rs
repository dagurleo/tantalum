//! Snapshots: the whole dataset written to a file now and then (and on
//! shutdown), and read back at startup, so data survives restarts. Like
//! Redis' RDB files, though in a format of our own:
//!
//! ```text
//! "TANTALUM SNAPSHOT 1\n"
//! per key:  0x01, key length, key, value length, value, expiry
//! trailer:  0x00, key count (u64 LE), CRC32 of everything before it (u32 LE)
//! ```
//!
//! Lengths and expiries are LEB128 varints; an expiry is a Unix time in
//! milliseconds, 0 for none, so TTLs carry over a restart and keys that
//! expired meanwhile are dropped on load.
//!
//! A save copies one shard at a time under its lock, so it's consistent per
//! shard rather than across the whole store: a write made during a save may
//! be in it or not. It writes to `<file>.tmp`, syncs it to disk and renames
//! it over the old snapshot, so a crash mid-save leaves the previous one.

use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use crate::store::{Condition, Expiry, NO_EXPIRY, Store};

const MAGIC: &[u8] = b"TANTALUM SNAPSHOT 1\n";
const RECORD: u8 = 1;
const END: u8 = 0;

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
    out.write_all(MAGIC)?;
    let now = store.now();
    // Store-clock deadlines become Unix times by this offset.
    let offset = unix_ms() - now as i64;
    let mut keys = 0u64;
    let mut buf = Vec::new();
    for shard in 0..store.shard_count() {
        buf.clear();
        store.for_each_live(shard, now, |key, value, deadline| {
            buf.push(RECORD);
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
    let crc = out.crc.clone().finalize();
    let mut file = out.inner.into_inner().map_err(|e| e.into_error())?;
    file.write_all(&crc.to_le_bytes())?;
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
    if magic != MAGIC {
        return Err(invalid("not a Tantalum snapshot"));
    }
    let now = store.now();
    let unix_now = unix_ms();
    let mut loaded = Loaded::default();
    let (mut key, mut value) = (Vec::new(), Vec::new());
    let mut records = 0u64;
    loop {
        let mut tag = [0];
        reader.read_exact(&mut tag)?;
        match tag[0] {
            RECORD => {}
            END => break,
            other => return Err(invalid(format!("unknown record type {other}"))),
        }
        records += 1;
        read_bytes(&mut reader, &mut key)?;
        read_bytes(&mut reader, &mut value)?;
        let expires = read_varint(&mut reader)?;
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
        match store.set(&key, &value, Condition::Always, expiry, now, false) {
            Ok(_) => loaded.keys += 1,
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
    use crate::store::Config;

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
