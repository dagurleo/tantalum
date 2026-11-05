//! The append-only log (`--aof DIR`): every change commands make, written to
//! a file as they make it and synced to disk every second, so a crash loses
//! at most about a second (a snapshot every minute could lose a minute).
//!
//! What it holds: the write commands as they were sent, in RESP as Redis'
//! AOF has them, each with the time it ran at. With the log on, the store
//! clock reads Unix milliseconds (`Config::unix_clock`), and at startup each
//! command runs again at its own time, so TTLs, `XADD`'s IDs and what had
//! expired by then come out as they first did, with nothing rewritten. Only
//! what isn't decided by the data and the time is written as what it did:
//! `SPOP` as the `SREM` of what it popped, an eviction as a `DEL`. Scripts
//! and transactions are written as the write commands they ran, between
//! `MULTI` and `EXEC`, so they come back whole or not at all; so is `MSET`,
//! as a `SET` per pair (an eviction or a failure out of memory can come
//! between two); a blocked command is written when it's served.
//!
//! Order: with the log on, write commands take turns ([`Aof::turn`]), each
//! taking its time when its turn comes, so the log has them in the order
//! they ran, at times that only go up. Reads don't take turns.
//!
//! Files, in DIR:
//!
//! ```text
//! tantalum.aof.manifest     the base and logs that make up the data, in order
//! tantalum.base.N.snapshot  a snapshot (snapshot.rs's format) at a time
//! tantalum.incr.N.aof       "TANTALUM AOF 1\r\n", then commands
//! ```
//!
//! A writer thread writes what's been logged every 10 ms (a crash of the
//! server loses that much) and syncs it every second (a crash of the
//! machine, that much more). When the logs grow past their base, a rewrite
//! starts a new log and writes the data as it is then as a new base: it
//! names the new log in the manifest before anything goes there, and the
//! new base only once it's on disk, so whenever it stops, the manifest names
//! a base and logs that make up all the data.
//!
//! If a write or sync fails (a full disk, say), what wasn't written stays in
//! memory and goes first next time, a record written in part is taken out
//! again, and until a write works, commands that write are refused with
//! Redis' `-MISCONF Errors writing to the AOF file: ...` (and `PING`, as in
//! Redis, so health checks notice).

use std::cell::Cell;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use parking_lot::{Mutex, MutexGuard};

use crate::cmd::{self, Args};
use crate::context::{Context, Session};
use crate::resp;
use crate::snapshot;
use crate::store::{Millis, Store};

/// When the log is synced to disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fsync {
    /// Every second.
    EverySec,
    /// When the operating system decides.
    No,
}

#[derive(Debug, Clone)]
pub struct Options {
    pub dir: PathBuf,
    pub fsync: Fsync,
    /// A rewrite starts once the logs since the last one are this percent of
    /// the base's size...
    pub rewrite_percentage: u64,
    /// ...and at least this many bytes.
    pub rewrite_min_size: u64,
}

const HEADER: &[u8] = b"TANTALUM AOF 1\r\n";
const MANIFEST: &str = "tantalum.aof.manifest";
const MANIFEST_HEADER: &str = "TANTALUM AOF MANIFEST 1";
/// The time the commands after it ran at, in Unix milliseconds.
const TIME: &[u8] = b"TANTALUM.TIME";
/// The biggest log buffer kept for the next write, so a burst doesn't hold
/// memory for good.
const MAX_SPARE: usize = 16 * 1024 * 1024;
const WRITE_EVERY: Duration = Duration::from_millis(10);
const SYNC_EVERY: Duration = Duration::from_secs(1);

fn base_name(n: u64) -> String {
    format!("tantalum.base.{n}.snapshot")
}

fn incr_name(n: u64) -> String {
    format!("tantalum.incr.{n}.aof")
}

/// What's been logged and not written yet; locking it is a write's turn.
#[derive(Default)]
struct Pending {
    buf: Vec<u8>,
    /// The buffer the last write finished with, to fill next: one grown from
    /// nothing every `WRITE_EVERY` was copied again and again as it grew.
    spare: Vec<u8>,
    /// The last time written to the current log (0: none yet).
    time: Millis,
}

/// The base and logs that make up the data, in the manifest.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Manifest {
    /// A base snapshot and its store-clock time.
    base: Option<(String, Millis)>,
    /// Logs, oldest first; the last is written to.
    incrs: Vec<String>,
}

impl Manifest {
    fn read(dir: &Path) -> io::Result<Option<Manifest>> {
        let text = match fs::read_to_string(dir.join(MANIFEST)) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            text => text?,
        };
        let mut lines = text.lines();
        if lines.next() != Some(MANIFEST_HEADER) {
            return Err(invalid(
                "not a Tantalum AOF manifest, or from a newer Tantalum",
            ));
        }
        let mut manifest = Manifest::default();
        for line in lines {
            let mut words = line.split(' ');
            match (words.next(), words.next(), words.next()) {
                (Some("base"), Some(name), Some(time)) if manifest.base.is_none() => {
                    let time = time.parse().map_err(|_| invalid("a bad base time"))?;
                    manifest.base = Some((name.to_string(), time));
                }
                (Some("incr"), Some(name), None) => manifest.incrs.push(name.to_string()),
                (Some(""), None, None) => {}
                _ => return Err(invalid(format!("a bad manifest line: {line}"))),
            }
        }
        if manifest.incrs.is_empty() {
            return Err(invalid("a manifest without a log"));
        }
        Ok(Some(manifest))
    }

    /// Replaces the manifest, all at once (a temporary file renamed over it).
    fn write(&self, dir: &Path) -> io::Result<()> {
        let mut text = format!("{MANIFEST_HEADER}\n");
        if let Some((name, time)) = &self.base {
            text += &format!("base {name} {time}\n");
        }
        for name in &self.incrs {
            text += &format!("incr {name}\n");
        }
        let tmp = dir.join(format!("{MANIFEST}.tmp"));
        let mut file = File::create(&tmp)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        fs::rename(&tmp, dir.join(MANIFEST))?;
        sync_dir(dir)
    }

    /// The next free file number.
    fn next(&self) -> u64 {
        let number = |name: &str| {
            name.split('.')
                .nth(2)
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(0)
        };
        self.base
            .iter()
            .map(|(name, _)| number(name))
            .chain(self.incrs.iter().map(|name| number(name)))
            .max()
            .unwrap_or(0)
            + 1
    }
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

/// The writer's side: the log being written and the sizes.
struct Files {
    manifest: Manifest,
    current: File,
    /// Bytes in `current`.
    current_len: u64,
    /// Logged, but not in `current` yet: a write failed (the disk is full,
    /// say). Written first next time.
    unwritten: Vec<u8>,
    /// Bytes of the base, and of the logs since it.
    base_size: u64,
    logged: u64,
    synced: Instant,
    /// For tests: writes get half in, then fail with this OS error.
    fail: Option<i32>,
}

impl Files {
    /// Appends what's unwritten and `data` to the current log. If that
    /// fails, what didn't get in stays unwritten, and the log still ends
    /// with a whole record: one partly written is taken out again (or, if
    /// that fails too, the rest of it is written next time), as in Redis.
    fn write(&mut self, data: Vec<u8>) -> io::Result<()> {
        if self.unwritten.is_empty() {
            self.unwritten = data;
        } else {
            self.unwritten.extend_from_slice(&data);
        }
        let mut done = 0;
        let result = loop {
            if done == self.unwritten.len() {
                break Ok(());
            }
            if let Some(errno) = self.fail.filter(|_| done > 0 || self.unwritten.len() == 1) {
                break Err(io::Error::from_raw_os_error(errno));
            }
            let end = match self.fail {
                Some(_) => self.unwritten.len() / 2,
                None => self.unwritten.len(),
            };
            match self.current.write(&self.unwritten[done..end]) {
                Ok(0) => break Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => done += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => break Err(e),
            }
        };
        if result.is_err() && done > 0 && self.current.set_len(self.current_len).is_ok() {
            done = 0;
        }
        self.current_len += done as u64;
        self.logged += done as u64;
        if result.is_ok() {
            self.unwritten.clear();
        } else {
            self.unwritten.drain(..done);
        }
        result
    }
}

/// The log, once it's taking writes.
pub struct Aof {
    options: Options,
    store: Arc<Store>,
    pending: Mutex<Pending>,
    files: Mutex<Files>,
    rewriting: AtomicBool,
    rewrites_failed: AtomicU64,
    /// The last write or sync of the log failed, and why (`strerror`):
    /// until one works, write commands are refused, as in Redis.
    failing: AtomicBool,
    failure: Mutex<String>,
    stop: AtomicBool,
}

/// What [`start`] found and did.
#[derive(Debug, Default)]
pub struct Started {
    /// Keys in the base it loaded, if any.
    pub base_keys: Option<u64>,
    /// Commands it ran again from the logs.
    pub commands: u64,
    /// Of those, ones that failed (a log that doesn't match its base).
    pub failed: u64,
    /// Bytes dropped from the end of the last log: a record or a group the
    /// server stopped in the middle of writing.
    pub truncated: u64,
    /// Whether it started a new log (none yet in DIR).
    pub created: bool,
}

/// Whether `dir` holds a log (its manifest).
pub fn exists(dir: &Path) -> bool {
    dir.join(MANIFEST).exists()
}

/// Opens the log in `options.dir`: loads its base and runs its commands
/// again into `ctx`'s store, or, with none there yet, starts one from what
/// the store holds (a snapshot loaded before, say). Then the log takes
/// `ctx`'s writes, and a thread writes and syncs it. The store must run on
/// the Unix clock (`Config::unix_clock`).
pub fn start(ctx: &Context, options: Options) -> io::Result<(Arc<Aof>, Started)> {
    fs::create_dir_all(&options.dir)?;
    let store = ctx.store.clone();
    let mut started = Started::default();
    let (manifest, current) = match Manifest::read(&options.dir)? {
        Some(manifest) => {
            if let Some((name, time)) = &manifest.base {
                let loaded = snapshot::load_base(&store, &options.dir.join(name), *time)?
                    .ok_or_else(|| invalid(format!("the base {name} is missing")))?;
                started.base_keys = Some(loaded.keys);
            }
            let last = manifest.incrs.len() - 1;
            for (i, name) in manifest.incrs.iter().enumerate() {
                let path = options.dir.join(name);
                let replayed = replay(ctx, &path, i == last)?;
                started.commands += replayed.commands;
                started.failed += replayed.failed;
                started.truncated += replayed.truncated;
            }
            let current = OpenOptions::new()
                .append(true)
                .open(options.dir.join(&manifest.incrs[last]))?;
            (manifest, current)
        }
        None => {
            // A base of what's there now, and a log for what comes next.
            started.created = true;
            let time = store.now();
            let base = base_name(1);
            let tmp = options.dir.join(format!("{base}.tmp"));
            snapshot::write_base(&store, time, &tmp)?;
            File::open(&tmp)?.sync_all()?;
            fs::rename(&tmp, options.dir.join(&base))?;
            let current = new_log(&options.dir, &incr_name(2))?;
            let manifest = Manifest {
                base: Some((base, time)),
                incrs: vec![incr_name(2)],
            };
            manifest.write(&options.dir)?;
            (manifest, current)
        }
    };
    let base_size = match &manifest.base {
        Some((name, _)) => fs::metadata(options.dir.join(name))?.len(),
        None => 0,
    };
    let logged = manifest
        .incrs
        .iter()
        .map(|name| fs::metadata(options.dir.join(name)).map_or(0, |m| m.len()))
        .sum();
    let aof = Arc::new(Aof {
        options,
        store,
        pending: Mutex::new(Pending::default()),
        files: Mutex::new(Files {
            manifest,
            current_len: current.metadata()?.len(),
            current,
            unwritten: Vec::new(),
            fail: None,
            base_size,
            logged,
            synced: Instant::now(),
        }),
        rewriting: AtomicBool::new(false),
        rewrites_failed: AtomicU64::new(0),
        failing: AtomicBool::new(false),
        failure: Mutex::new(String::new()),
        stop: AtomicBool::new(false),
    });
    let writer = aof.clone();
    std::thread::Builder::new()
        .name("aof-writer".into())
        .spawn(move || writer.write_loop())?;
    Ok((aof, started))
}

/// Creates a log with its header, on disk.
fn new_log(dir: &Path, name: &str) -> io::Result<File> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .append(true)
        .open(dir.join(name))?;
    file.write_all(HEADER)?;
    file.sync_all()?;
    Ok(file)
}

impl Aof {
    /// A write command's turn: until it drops, other writes wait, and what
    /// this one does (and what its scripts' or transaction's commands do,
    /// with `grouped`) is logged at store-clock time `now`, which must be
    /// taken once the turn has come (see the module docs).
    pub(crate) fn turn(&self, grouped: bool) -> Turn<'_> {
        let mut pending = self.pending.lock();
        let now = self.store.now();
        let active = Active {
            pending: &mut *pending as *mut Pending,
            now,
            grouped,
            open: false,
        };
        let previous = ACTIVE.replace(Some(active));
        debug_assert!(previous.is_none(), "a write turn inside another");
        Turn {
            now,
            _pending: pending,
        }
    }

    fn write_loop(self: Arc<Self>) {
        loop {
            std::thread::sleep(WRITE_EVERY);
            let stopping = self.stop.load(Relaxed);
            self.wrote(self.write_pending(stopping));
            if stopping {
                return;
            }
            if self.rewrite_due() && !self.rewriting.load(Relaxed) {
                let aof = self.clone();
                let _ = std::thread::Builder::new()
                    .name("aof-rewrite".into())
                    .spawn(move || aof.rewrite());
            }
        }
    }

    /// Writes what's been logged to the current log; syncs it if a second
    /// has passed (or `sync`).
    fn write_pending(&self, sync: bool) -> io::Result<()> {
        let (data, mut files) = {
            let mut pending = self.pending.lock();
            // Taken before the turn goes, so a rewrite can't switch logs
            // between the two.
            let files = self.files.lock();
            let spare = std::mem::take(&mut pending.spare);
            (std::mem::replace(&mut pending.buf, spare), files)
        };
        files.write(data)?;
        let due = self.options.fsync == Fsync::EverySec && files.synced.elapsed() >= SYNC_EVERY;
        if sync || due {
            files.current.sync_data()?;
            files.synced = Instant::now();
        }
        let spare = std::mem::take(&mut files.unwritten);
        // (Not holding the files: a write's turn comes first, as above.)
        drop(files);
        if spare.capacity() <= MAX_SPARE {
            self.pending.lock().spare = spare;
        }
        Ok(())
    }

    /// Notes how writing (and syncing) the log went: write commands are
    /// refused from a failure until it works again.
    fn wrote(&self, result: io::Result<()>) {
        match result {
            Ok(()) => {
                if self.failing.swap(false, Relaxed) {
                    eprintln!("the append-only log can be written again");
                }
            }
            Err(e) => {
                // `strerror`'s text, as Redis gives it.
                let text = e.to_string();
                let text = match text.find(" (os error ") {
                    Some(at) => text[..at].to_string(),
                    None => text,
                };
                if !self.failing.load(Relaxed) {
                    eprintln!("error: can't write the append-only log: {text}; refusing writes");
                }
                *self.failure.lock() = text;
                self.failing.store(true, Relaxed);
            }
        }
    }

    /// Whether the log can't be written now: then commands that write are
    /// refused with [`failure`](Self::failure).
    pub(crate) fn failing(&self) -> bool {
        self.failing.load(Relaxed)
    }

    /// The error a write command gets while the log can't be written.
    pub(crate) fn failure(&self) -> String {
        format!(
            "MISCONF Errors writing to the AOF file: {}",
            self.failure.lock()
        )
    }

    fn rewrite_due(&self) -> bool {
        let files = self.files.lock();
        let since = files.logged;
        since >= self.options.rewrite_min_size
            && since * 100 >= files.base_size * self.options.rewrite_percentage
    }

    /// Writes what's been logged and syncs it: at shutdown.
    pub fn flush(&self) -> io::Result<()> {
        self.write_pending(true)
    }

    /// For tests: until called with `None`, writes to the log get half in,
    /// then fail with OS error `errno` (`ENOSPC`, say).
    #[doc(hidden)]
    pub fn fail_writes(&self, errno: Option<i32>) {
        self.files.lock().fail = errno;
    }

    /// Stops the writer thread after it writes and syncs what's left.
    pub fn stop(&self) {
        self.stop.store(true, Relaxed);
    }

    /// `BGREWRITEAOF`: starts a rewrite on a thread of its own. False if one
    /// is running already.
    pub fn rewrite_in_background(self: &Arc<Self>) -> bool {
        if self.rewriting.load(Relaxed) {
            return false;
        }
        let aof = self.clone();
        std::thread::Builder::new()
            .name("aof-rewrite".into())
            .spawn(move || aof.rewrite())
            .is_ok()
    }

    /// A new base of the data as it is now, and a new log for what comes
    /// after; then the old ones go. Returns whether it ran (not if another
    /// was running).
    pub fn rewrite(&self) -> bool {
        if self.rewriting.swap(true, Relaxed) {
            return false;
        }
        if let Err(e) = self.try_rewrite() {
            self.rewrites_failed.fetch_add(1, Relaxed);
            eprintln!("error: rewriting the append-only log failed: {e}");
        } else {
            self.rewrites_failed.store(0, Relaxed);
        }
        self.rewriting.store(false, Relaxed);
        true
    }

    fn try_rewrite(&self) -> io::Result<()> {
        let dir = &self.options.dir;
        let (n, time, tmp) = {
            // No write runs meanwhile (reads do).
            let mut pending = self.pending.lock();
            let mut files = self.files.lock();
            let data = std::mem::take(&mut pending.buf);
            files.write(data)?;
            files.current.sync_data()?;
            let n = files.manifest.next();
            // The new log goes in the manifest before anything goes in it.
            let current = new_log(dir, &incr_name(n))?;
            files.manifest.incrs.push(incr_name(n));
            files.manifest.write(dir)?;
            files.current = current;
            files.logged = HEADER.len() as u64;
            pending.time = 0;
            let time = self.store.now();
            let tmp = dir.join(format!("{}.tmp", base_name(n)));
            snapshot::write_base(&self.store, time, &tmp)?;
            (n, time, tmp)
        };
        File::open(&tmp)?.sync_all()?;
        let base = base_name(n);
        fs::rename(&tmp, dir.join(&base))?;
        sync_dir(dir)?;
        let old = {
            let mut files = self.files.lock();
            let old = files.manifest.clone();
            let incrs = vec![files.manifest.incrs.last().cloned().expect("a log")];
            files.manifest = Manifest {
                base: Some((base.clone(), time)),
                incrs,
            };
            files.manifest.write(dir)?;
            files.base_size = fs::metadata(dir.join(&base))?.len();
            old
        };
        for name in old.base.iter().map(|(name, _)| name).chain(&old.incrs) {
            if *name != incr_name(n) {
                let _ = fs::remove_file(dir.join(name));
            }
        }
        Ok(())
    }

    /// `INFO`'s persistence lines for the log.
    pub fn info(&self) -> String {
        let files = self.files.lock();
        format!(
            "aof_enabled:1\r\naof_rewrite_in_progress:{}\r\naof_last_bgrewrite_status:{}\r\n\
             aof_last_write_status:{}\r\naof_current_size:{}\r\naof_base_size:{}\r\n",
            u8::from(self.rewriting.load(Relaxed)),
            if self.rewrites_failed.load(Relaxed) == 0 {
                "ok"
            } else {
                "err"
            },
            if self.failing() { "err" } else { "ok" },
            files.base_size + files.logged,
            files.base_size,
        )
    }

    pub fn dir(&self) -> &Path {
        &self.options.dir
    }
}

/// A write command's turn ([`Aof::turn`]).
pub(crate) struct Turn<'a> {
    /// The time to run it at.
    pub(crate) now: Millis,
    _pending: MutexGuard<'a, Pending>,
}

impl Turn<'_> {
    /// Closes a script's or transaction's group: what's logged after
    /// (blocked clients it served) is logged on its own.
    pub(crate) fn end_group(&mut self) {
        with_active(|pending, active| {
            if active.open {
                push_command(&mut pending.buf, [&b"EXEC"[..]].into_iter());
            }
            active.open = false;
            active.grouped = false;
        });
    }
}

impl Drop for Turn<'_> {
    fn drop(&mut self) {
        self.end_group();
        ACTIVE.set(None);
    }
}

/// The turn this thread is taking, if any.
#[derive(Clone, Copy)]
struct Active {
    pending: *mut Pending,
    now: Millis,
    /// Several commands that come back together: `MULTI` before the first.
    grouped: bool,
    /// `MULTI` written.
    open: bool,
}

thread_local! {
    static ACTIVE: Cell<Option<Active>> = const { Cell::new(None) };
}

fn with_active(f: impl FnOnce(&mut Pending, &mut Active)) {
    if let Some(mut active) = ACTIVE.get() {
        // SAFETY: the turn's guard holds the lock, and lives until it
        // clears ACTIVE; this thread is the only one using it.
        let pending = unsafe { &mut *active.pending };
        f(pending, &mut active);
        ACTIVE.set(Some(active));
    }
}

fn push_command<'a>(buf: &mut Vec<u8>, args: impl ExactSizeIterator<Item = &'a [u8]>) {
    resp::write_array_header(buf, args.len());
    for arg in args {
        resp::write_bulk(buf, arg);
    }
}

/// Logs a command that changed (or may have changed) the data, if this
/// thread is taking a write turn: with its time first if that's new.
fn push<'a>(args: impl ExactSizeIterator<Item = &'a [u8]>) {
    with_active(|pending, active| {
        if active.grouped && !active.open {
            push_command(&mut pending.buf, [&b"MULTI"[..]].into_iter());
            active.open = true;
        }
        if pending.time != active.now {
            let time = active.now.to_string();
            push_command(&mut pending.buf, [TIME, time.as_bytes()].into_iter());
            pending.time = active.now;
        }
        push_command(&mut pending.buf, args);
    });
}

/// Whether this thread is taking a write turn (the log is on).
pub(crate) fn logging() -> bool {
    ACTIVE.get().is_some()
}

/// Logs write command `name` (its table name) with `args`, which replied
/// `reply` (an error isn't logged: it changed nothing).
pub(crate) fn record(name: &str, args: &Args<'_>, reply: &[u8]) {
    if !logging() || reply.first() == Some(&b'-') {
        return;
    }
    // Without `STORE` it changed nothing.
    if name == "sort" && reply.first() != Some(&b':') {
        return;
    }
    // Nor does a `BITFIELD` of only `GET`s.
    if name == "bitfield" {
        let writes = |i: usize| {
            let word = args.get(i);
            word.eq_ignore_ascii_case(b"SET") || word.eq_ignore_ascii_case(b"INCRBY")
        };
        if !(2..args.len()).any(writes) {
            return;
        }
    }
    if matches!(name, "incrbyfloat" | "hincrbyfloat") {
        // What it came to, so a machine that rounds differently gets the same.
        if let Some(Popped::One(value)) = resp_value(&mut { reply }) {
            if name == "incrbyfloat" {
                push([&b"SET"[..], args.get(1), &value, b"KEEPTTL"].into_iter());
            } else {
                push([&b"HSET"[..], args.get(1), args.get(2), &value].into_iter());
            }
        }
        return;
    }
    if name == "spop" {
        // What it popped, which was chance.
        let mut members = Vec::new();
        let mut input = reply;
        match resp_value(&mut input) {
            Some(Popped::One(member)) => members.push(member),
            Some(Popped::Many(many)) => members = many,
            None => {}
        }
        if !members.is_empty() {
            let key = args.get(1);
            let parts = [&b"SREM"[..], key]
                .into_iter()
                .chain(members.iter().map(Vec::as_slice))
                .collect::<Vec<_>>();
            push(parts.into_iter());
        }
        return;
    }
    push((0..args.len()).map(|i| args.get(i)));
}

/// A write a command made, as the command `parts` that makes just it: for
/// one logged piece by piece (`MSET`'s pairs).
pub(crate) fn record_parts<'a>(parts: impl ExactSizeIterator<Item = &'a [u8]>) {
    if logging() {
        push(parts);
    }
}

/// An eviction, if it happened in a write turn: as a `DEL`.
pub(crate) fn evicted(key: &[u8]) {
    if logging() {
        push([&b"DEL"[..], key].into_iter());
    }
}

enum Popped {
    One(Vec<u8>),
    Many(Vec<Vec<u8>>),
}

/// `SPOP`'s reply: a bulk string, an array of them, or a nil (`None`).
fn resp_value(input: &mut &[u8]) -> Option<Popped> {
    fn line<'a>(input: &mut &'a [u8]) -> Option<&'a [u8]> {
        let end = memchr::memchr(b'\n', input)?;
        let line = &input[..end.saturating_sub(1)];
        *input = &input[end + 1..];
        Some(line)
    }
    fn bulk(input: &mut &[u8]) -> Option<Vec<u8>> {
        let head = line(input)?;
        let n = usize::try_from(resp::parse_int(head.strip_prefix(b"$")?)?).ok()?;
        let data = input.get(..n)?.to_vec();
        *input = input.get(n + 2..)?;
        Some(data)
    }
    match input.first()? {
        b'$' => bulk(input).map(Popped::One),
        b'*' | b'~' => {
            let head = line(input)?;
            let n = usize::try_from(resp::parse_int(&head[1..])?).ok()?;
            (0..n)
                .map(|_| bulk(input))
                .collect::<Option<Vec<_>>>()
                .map(Popped::Many)
        }
        _ => None,
    }
}

/// What [`replay`] did with one log.
#[derive(Debug, Default)]
struct Replayed {
    commands: u64,
    failed: u64,
    truncated: u64,
}

/// Runs a log's commands again into `ctx`'s store, each at its time. A
/// torn record or group at the end (the server stopped writing it) is cut
/// off when `last` (the log to go on writing), else is an error; anything
/// else unreadable is an error.
fn replay(ctx: &Context, path: &Path, last: bool) -> io::Result<Replayed> {
    let mut file = OpenOptions::new().read(true).write(last).open(path)?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)?;
    if !data.starts_with(HEADER) {
        return Err(invalid(format!(
            "{} isn't a Tantalum AOF log",
            path.display()
        )));
    }
    let mut session = Session::for_replay();
    let mut replayed = Replayed::default();
    let mut now: Millis = ctx.store.now();
    let mut pos = HEADER.len();
    // Where the last whole record (or group) ends.
    let mut good = pos;
    let mut group: Option<Vec<Vec<Vec<u8>>>> = None;
    let mut out = Vec::new();
    ctx.store.set_loading(true);
    let result = (|| {
        while pos < data.len() {
            let (args, end) = match parse_command(&data[pos..]) {
                Parsed::Whole(args, end) => (args, end),
                Parsed::Partial => break,
                Parsed::Bad => {
                    return Err(invalid(format!(
                        "{} is damaged at byte {pos}",
                        path.display()
                    )));
                }
            };
            pos += end;
            let name = args.first().map(|a| a.to_ascii_uppercase());
            match name.as_deref() {
                Some(TIME) if args.len() == 2 => {
                    now = std::str::from_utf8(&args[1])
                        .ok()
                        .and_then(|t| t.parse().ok())
                        .ok_or_else(|| invalid("a bad time in the log"))?;
                }
                Some(b"MULTI") if group.is_none() => group = Some(Vec::new()),
                Some(b"EXEC") if group.is_some() => {
                    for command in group.take().unwrap_or_default() {
                        run(ctx, &mut session, now, &command, &mut out, &mut replayed);
                    }
                }
                _ => match &mut group {
                    Some(commands) => commands.push(args),
                    None => run(ctx, &mut session, now, &args, &mut out, &mut replayed),
                },
            }
            if group.is_none() {
                good = pos;
            }
        }
        Ok::<_, io::Error>(())
    })();
    ctx.store.set_loading(false);
    result?;
    if good < data.len() {
        // The server stopped in the middle of writing a record, or a
        // script's or transaction's group: in the log it went on writing,
        // drop the rest.
        if !last {
            return Err(invalid(format!(
                "{} ends in the middle of a record",
                path.display()
            )));
        }
        replayed.truncated = (data.len() - good) as u64;
        file.set_len(good as u64)?;
        file.seek(SeekFrom::End(0))?;
        file.sync_all()?;
    }
    Ok(replayed)
}

fn run(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &[Vec<u8>],
    out: &mut Vec<u8>,
    replayed: &mut Replayed,
) {
    let mut buf = Vec::new();
    let mut ranges = Vec::with_capacity(args.len());
    for arg in args {
        ranges.push(buf.len()..buf.len() + arg.len());
        buf.extend_from_slice(arg);
    }
    out.clear();
    cmd::execute(ctx, session, now, &Args::new(&buf, &ranges), out);
    replayed.commands += 1;
    if out.first() == Some(&b'-') {
        if replayed.failed == 0 {
            eprintln!(
                "warning: a logged command failed when run again: {} ({})",
                String::from_utf8_lossy(&args[0]),
                String::from_utf8_lossy(&out[..out.len().saturating_sub(2)])
            );
        }
        replayed.failed += 1;
    }
}

enum Parsed {
    /// A command, and its length.
    Whole(Vec<Vec<u8>>, usize),
    /// The start of one: the data ends first.
    Partial,
    /// Not a command.
    Bad,
}

/// One RESP array of bulk strings at the start of `data`.
fn parse_command(data: &[u8]) -> Parsed {
    /// A header line (`*3`, `$5`): `Err(true)` if the data ends first,
    /// `Err(false)` if it's not one.
    fn header(data: &[u8], pos: &mut usize, kind: u8) -> Result<usize, bool> {
        match data.get(*pos) {
            None => return Err(true),
            Some(&k) if k != kind => return Err(false),
            _ => {}
        }
        let Some(nl) = memchr::memchr(b'\n', &data[*pos..]) else {
            // A header is short: a long run without one isn't a header.
            return Err(data.len() - *pos < 32);
        };
        let end = *pos + nl;
        if end == 0 || data[end - 1] != b'\r' {
            return Err(false);
        }
        let n = resp::parse_int(&data[*pos + 1..end - 1]).ok_or(false)?;
        *pos = end + 1;
        usize::try_from(n).map_err(|_| false)
    }
    let fail = |partial: bool| {
        if partial {
            Parsed::Partial
        } else {
            Parsed::Bad
        }
    };
    let mut pos = 0;
    let count = match header(data, &mut pos, b'*') {
        Ok(n) => n,
        Err(partial) => return fail(partial),
    };
    let mut args = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let len = match header(data, &mut pos, b'$') {
            Ok(n) => n,
            Err(partial) => return fail(partial),
        };
        let Some(arg) = data.get(pos..pos + len) else {
            return Parsed::Partial;
        };
        match data.get(pos + len..pos + len + 2) {
            Some(b"\r\n") => {}
            Some(_) => return Parsed::Bad,
            None => return Parsed::Partial,
        }
        args.push(arg.to_vec());
        pos += len + 2;
    }
    Parsed::Whole(args, pos)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_parse_whole_or_not_at_all() {
        let mut data = Vec::new();
        push_command(&mut data, [&b"SET"[..], b"k", b"a\r\nb"].into_iter());
        let Parsed::Whole(args, len) = parse_command(&data) else {
            panic!("not whole");
        };
        assert_eq!(args, [b"SET".to_vec(), b"k".to_vec(), b"a\r\nb".to_vec()]);
        assert_eq!(len, data.len());
        for cut in 0..data.len() {
            assert!(
                matches!(parse_command(&data[..cut]), Parsed::Partial),
                "cut at {cut}"
            );
        }
        assert!(matches!(parse_command(b"GET k\r\n"), Parsed::Bad));
        assert!(matches!(
            parse_command(b"*1\r\n$3\r\nGETX\r\n"),
            Parsed::Bad
        ));
    }

    #[test]
    fn manifests_round_trip() {
        let dir =
            std::env::temp_dir().join(format!("tantalum-aof-manifest-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let manifest = Manifest {
            base: Some((base_name(3), 1_780_000_000_000)),
            incrs: vec![incr_name(3), incr_name(4)],
        };
        manifest.write(&dir).unwrap();
        assert_eq!(Manifest::read(&dir).unwrap(), Some(manifest.clone()));
        assert_eq!(manifest.next(), 5);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn spop_replies_read_back() {
        let mut input = &b"*2\r\n$1\r\na\r\n$2\r\nbc\r\n"[..];
        assert!(
            matches!(resp_value(&mut input), Some(Popped::Many(m)) if m == [b"a".to_vec(), b"bc".to_vec()])
        );
        let mut input = &b"$3\r\nxyz\r\n"[..];
        assert!(matches!(resp_value(&mut input), Some(Popped::One(m)) if m == b"xyz"));
        let mut input = &b"$-1\r\n"[..];
        assert!(resp_value(&mut input).is_none());
    }
}
