//! `SLOWLOG`: commands that took long. At Redis' default of 10 ms and above, commands
//! flagged `fast` aren't timed, and the rest read a clock a thread ticks every 5 ms,
//! so that this costs them two loads: a command is logged once it surely took as long.
//! That thread sleeps when no command has run for a second, so an idle server isn't woken.
//! Below that, every command of every connection takes the slow path with a real clock
//! (see `Context::special`), as Redis' does.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering::Relaxed};

use parking_lot::Mutex;

use crate::cmd::Args;
use crate::resp;

/// How often the clock ticks.
pub(crate) const TICK_MICROS: i64 = 5_000;
/// With no command run for this long, a second, the clock stops until one comes.
pub(crate) const TICKS_BEFORE_SLEEP: u32 = 200;
/// Below this threshold, the clock's too coarse: a real clock.
pub(crate) const PRECISE_BELOW: i64 = 10_000;
const MAX_ARGS: usize = 32;
const MAX_ARG_BYTES: usize = 128;
/// The settings whose values logs don't show, as in Redis.
const SECRET_SETTINGS: [&[u8]; 5] = [
    b"masteruser",
    b"masterauth",
    b"requirepass",
    b"tls-key-file-pass",
    b"tls-client-key-file-pass",
];

/// Which of a command's arguments `MONITOR` and `SLOWLOG` show as `(redacted)`.
pub(crate) fn secret_args(args: &Args<'_>) -> Vec<bool> {
    let mut secret = vec![false; args.len()];
    let name = args.get(0);
    if name.eq_ignore_ascii_case(b"AUTH") {
        if args.len() <= 3 {
            secret[1..].fill(true);
        }
    } else if name.eq_ignore_ascii_case(b"HELLO") {
        // Its options, as it reads them, so that a name called AUTH isn't one.
        let mut i = 2;
        while i < args.len() {
            let more = args.len() - 1 - i;
            let option = args.get(i);
            if option.eq_ignore_ascii_case(b"AUTH") && more >= 2 {
                secret[i + 1] = true;
                secret[i + 2] = true;
                i += 3;
            } else if option.eq_ignore_ascii_case(b"SETNAME") && more >= 1 {
                i += 2;
            } else {
                break;
            }
        }
    } else if name.eq_ignore_ascii_case(b"CONFIG")
        && args.len() > 3
        && args.get(1).eq_ignore_ascii_case(b"SET")
    {
        for i in (2..args.len() - 1).step_by(2) {
            let setting = args.get(i);
            if SECRET_SETTINGS
                .iter()
                .any(|name| setting.eq_ignore_ascii_case(name))
            {
                secret[i + 1] = true;
            }
        }
    }
    secret
}

struct Entry {
    id: u64,
    time: i64,
    micros: u64,
    args: Vec<Vec<u8>>,
    peer: String,
    name: Vec<u8>,
}

pub(crate) struct SlowLog {
    entries: Mutex<VecDeque<Entry>>,
    next_id: AtomicU64,
    /// `slowlog-log-slower-than`, in microseconds: negative for off.
    threshold: AtomicI64,
    /// The same in ticks: what a command with a coarse clock must take to be logged.
    ticks: AtomicU64,
    max_len: AtomicUsize,
}

impl Default for SlowLog {
    fn default() -> Self {
        let log = SlowLog {
            entries: Mutex::default(),
            next_id: AtomicU64::new(0),
            threshold: AtomicI64::new(0),
            ticks: AtomicU64::new(u64::MAX),
            max_len: AtomicUsize::new(128),
        };
        log.set_threshold(PRECISE_BELOW);
        log
    }
}

impl SlowLog {
    pub(crate) fn threshold(&self) -> i64 {
        self.threshold.load(Relaxed)
    }

    pub(crate) fn set_threshold(&self, micros: i64) {
        self.threshold.store(micros, Relaxed);
        // One more than it fits, as a tick may have just gone by at the start.
        let ticks = if micros >= PRECISE_BELOW {
            (micros / TICK_MICROS) as u64 + 1
        } else {
            u64::MAX
        };
        self.ticks.store(ticks, Relaxed);
    }

    /// Whether the threshold wants a real clock on every command.
    pub(crate) fn needs_precise(&self) -> bool {
        (0..PRECISE_BELOW).contains(&self.threshold())
    }

    /// How many ticks a command must take to be logged.
    pub(crate) fn threshold_ticks(&self) -> u64 {
        self.ticks.load(Relaxed)
    }

    pub(crate) fn max_len(&self) -> usize {
        self.max_len.load(Relaxed)
    }

    pub(crate) fn set_max_len(&self, max: usize) {
        self.max_len.store(max, Relaxed);
        let mut entries = self.entries.lock();
        while entries.len() > max {
            entries.pop_back();
        }
    }

    /// Whether a command that took `micros` by a real clock is one to log.
    pub(crate) fn is_slow(&self, micros: u64) -> bool {
        let threshold = self.threshold();
        threshold >= 0 && micros >= threshold as u64
    }

    pub(crate) fn push(&self, args: &Args<'_>, micros: u64, peer: String, name: &[u8]) {
        let shown = if args.len() > MAX_ARGS {
            MAX_ARGS - 1
        } else {
            args.len()
        };
        let mut kept = Vec::with_capacity(shown + 1);
        for (i, secret) in secret_args(args).into_iter().take(shown).enumerate() {
            let arg = args.get(i);
            if secret {
                kept.push(b"(redacted)".to_vec());
            } else if arg.len() > MAX_ARG_BYTES {
                let mut short = arg[..MAX_ARG_BYTES].to_vec();
                short.extend_from_slice(
                    format!("... ({} more bytes)", arg.len() - MAX_ARG_BYTES).as_bytes(),
                );
                kept.push(short);
            } else {
                kept.push(arg.to_vec());
            }
        }
        if args.len() > MAX_ARGS {
            kept.push(format!("... ({} more arguments)", args.len() - shown).into_bytes());
        }
        let time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() as i64);
        let entry = Entry {
            id: self.next_id.fetch_add(1, Relaxed),
            time,
            micros,
            args: kept,
            peer,
            name: name.to_vec(),
        };
        let max = self.max_len();
        let mut entries = self.entries.lock();
        entries.push_front(entry);
        while entries.len() > max {
            entries.pop_back();
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.lock().len()
    }

    pub(crate) fn reset(&self) {
        self.entries.lock().clear();
    }

    /// `SLOWLOG GET`'s reply: the latest `count`, newest first.
    pub(crate) fn write(&self, count: usize, out: &mut Vec<u8>) {
        let entries = self.entries.lock();
        resp::write_array_header(out, count.min(entries.len()));
        for entry in entries.iter().take(count) {
            resp::write_array_header(out, 6);
            resp::write_integer(out, entry.id as i64);
            resp::write_integer(out, entry.time);
            resp::write_integer(out, entry.micros as i64);
            resp::write_array_header(out, entry.args.len());
            for arg in &entry.args {
                resp::write_bulk(out, arg);
            }
            resp::write_bulk(out, entry.peer.as_bytes());
            resp::write_bulk(out, &entry.name);
        }
    }
}
