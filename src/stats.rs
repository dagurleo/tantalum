//! Per-command statistics, as Redis 7.2 keeps them: `INFO commandstats`,
//! `errorstats` and `latencystats`, `LATENCY HISTOGRAM` and `total_error_replies`.
//!
//! Counting costs every command two clock reads and the checked path, so it
//! starts only once something asks for these: `INFO` naming one of those
//! sections (or `all`, `everything`), `LATENCY HISTOGRAM` or `CONFIG RESETSTAT`.
//! From then until the server stops, every connection takes the checked path
//! (`Context::special`), whose commands are timed already. Until then nothing's
//! allocated and the fast path is as it was. So the first such `INFO` shows only
//! itself; monitoring tools, which ask again and again, see what came after.
//!
//! Counts are kept per thread (sixteen slots), so threads running the same
//! command don't share its cache lines, and summed when read. A command's
//! latencies go into a histogram with sixteen steps for each power of two of
//! nanoseconds (to within 1/16, where Redis' keeps two significant figures), made
//! when it first runs.
//!
//! What counts as what, as in Redis: a command refused before it runs (arity, no
//! authentication, ACLs, `-BUSY`, out of memory for one flagged `denyoom`, refused in
//! a transaction or while subscribed) is a rejected call; one that ran and replied
//! with an error, a failed call. Each error reply counts once, by its first word:
//! a script's error that's the error of the command it called isn't counted again.
//! A blocked command counts when it blocks; its error, if `CLIENT UNBLOCK` or
//! what served it gives one, when it ends. BullMQ's scripts that run natively
//! count as the script, not the commands inside.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};

use parking_lot::Mutex;

use crate::acl;
use crate::cmd::{Args, Reply};
use crate::context::{Context, Session};
use crate::resp;
use crate::store::Millis;

/// Per-thread slots, so that threads don't contend on counters.
const SLOTS: usize = 16;
/// Steps for each power of two in a histogram.
const SUB_BITS: u32 = 4;
const SUB: usize = 1 << SUB_BITS;
/// Latencies above this are kept as this, as in Redis (a second).
const MAX_NANOS: u64 = 1_000_000_000;
/// Histogram buckets: one per nanosecond below 16, then 16 per power of two
/// up to 2^30 ns.
const BUCKETS: usize = (30 - SUB_BITS as usize + 1) * SUB;

static NEXT_SLOT: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static SLOT: usize = NEXT_SLOT.fetch_add(1, Relaxed) % SLOTS;
    /// Set while [`execute`] runs a command.
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    /// What the command [`execute`] runs ran as.
    static RAN: Cell<Option<Ran>> = const { Cell::new(None) };
    /// The error code of the last error a command inside it gave (a script's
    /// or a transaction's): up to 32 bytes.
    static INNER_ERROR: Cell<Option<([u8; 32], u8)>> = const { Cell::new(None) };
}

/// A command that ran: its id, how long it took, and what would have made
/// Redis refuse it before it ran.
#[derive(Clone, Copy)]
pub(crate) struct Ran {
    pub(crate) id: u16,
    pub(crate) nanos: u64,
    /// Flagged `denyoom`: an `OOM` error means Redis refused it.
    pub(crate) denyoom: bool,
    /// A subcommand's arguments that Redis refuses before running it.
    pub(crate) turned_away: bool,
}

/// The server's command statistics.
pub(crate) struct Stats {
    on: AtomicBool,
    /// `latency-tracking`.
    latency: AtomicBool,
    /// `latency-tracking-info-percentiles`.
    percentiles: Mutex<Vec<f64>>,
    table: OnceLock<Table>,
}

struct Table {
    slots: Box<[Slot]>,
    /// Error replies by code, in the order Redis lists them (its rax's).
    errors: Mutex<BTreeMap<Box<[u8]>, u64>>,
}

#[repr(align(128))]
struct Slot {
    counts: Box<[Counts]>,
    histograms: Box<[OnceLock<Box<Histogram>>]>,
}

#[derive(Default)]
struct Counts {
    calls: AtomicU64,
    nanos: AtomicU64,
    rejected: AtomicU64,
    failed: AtomicU64,
}

struct Histogram([AtomicU64; BUCKETS]);

impl Default for Stats {
    fn default() -> Self {
        Self {
            on: AtomicBool::new(false),
            latency: AtomicBool::new(true),
            percentiles: Mutex::new(vec![50.0, 99.0, 99.9]),
            table: OnceLock::new(),
        }
    }
}

impl Stats {
    #[inline]
    pub(crate) fn is_on(&self) -> bool {
        self.on.load(Relaxed)
    }

    /// Starts counting, if it hasn't. The command `id` that asked counts as
    /// having run, as it would have been had counting been on. Whether it
    /// started now (then `Context::update_special` is due).
    pub(crate) fn start(&self, id: Option<u16>) -> bool {
        if self.is_on() {
            return false;
        }
        self.table();
        self.on.store(true, Relaxed);
        if let Some(id) = id {
            let ran = Ran {
                id,
                nanos: 0,
                denyoom: false,
                turned_away: false,
            };
            self.called(ran, None);
        }
        true
    }

    /// For a command that started counting before its reply was written:
    /// it counts as having run.
    pub(crate) fn start_counted(&self, id: u16) {
        let ran = Ran {
            id,
            nanos: 0,
            denyoom: false,
            turned_away: false,
        };
        self.called(ran, None);
    }

    fn table(&self) -> &Table {
        self.table.get_or_init(|| {
            let ids = acl::command_count();
            Table {
                slots: (0..SLOTS)
                    .map(|_| Slot {
                        counts: (0..ids).map(|_| Counts::default()).collect(),
                        histograms: (0..ids).map(|_| OnceLock::new()).collect(),
                    })
                    .collect(),
                errors: Mutex::new(BTreeMap::new()),
            }
        })
    }

    fn slot(&self) -> &Slot {
        &self.table().slots[SLOT.with(|&slot| slot)]
    }

    /// `CONFIG RESETSTAT`.
    pub(crate) fn reset(&self) {
        let Some(table) = self.table.get() else {
            return;
        };
        for slot in table.slots.iter() {
            for counts in slot.counts.iter() {
                counts.calls.store(0, Relaxed);
                counts.nanos.store(0, Relaxed);
                counts.rejected.store(0, Relaxed);
                counts.failed.store(0, Relaxed);
            }
            for histogram in slot.histograms.iter().filter_map(OnceLock::get) {
                for bucket in histogram.0.iter() {
                    bucket.store(0, Relaxed);
                }
            }
        }
        table.errors.lock().clear();
    }

    pub(crate) fn set_latency_tracking(&self, on: bool) {
        self.latency.store(on, Relaxed);
    }

    pub(crate) fn set_percentiles(&self, percentiles: Vec<f64>) {
        *self.percentiles.lock() = percentiles;
    }

    /// A command ran, and replied with error `code` if any.
    fn called(&self, ran: Ran, code: Option<&[u8]>) {
        let Ran { id, nanos, .. } = ran;
        let counts = &self.slot().counts[id as usize];
        // What Redis refuses before running it: a subcommand's arguments, or
        // a command flagged `denyoom` while out of memory.
        if code.is_some() && (ran.turned_away || (ran.denyoom && code == Some(b"OOM"))) {
            counts.rejected.fetch_add(1, Relaxed);
            return;
        }
        counts.calls.fetch_add(1, Relaxed);
        counts.nanos.fetch_add(nanos, Relaxed);
        if code.is_some() {
            counts.failed.fetch_add(1, Relaxed);
        }
        if self.latency.load(Relaxed) {
            let histogram = self.slot().histograms[id as usize]
                .get_or_init(|| Box::new(Histogram(std::array::from_fn(|_| AtomicU64::new(0)))));
            histogram.0[bucket(nanos)].fetch_add(1, Relaxed);
        }
    }

    fn rejected(&self, id: u16) {
        self.slot().counts[id as usize]
            .rejected
            .fetch_add(1, Relaxed);
    }

    fn failed(&self, id: u16) {
        self.slot().counts[id as usize].failed.fetch_add(1, Relaxed);
    }

    /// An error reply with this code.
    fn error(&self, code: &[u8]) {
        *self.table().errors.lock().entry(code.into()).or_default() += 1;
    }

    /// `total_error_replies`.
    pub(crate) fn total_errors(&self) -> u64 {
        self.table
            .get()
            .map_or(0, |table| table.errors.lock().values().sum())
    }

    /// Command `id`'s counts, summed over the slots: calls, nanoseconds,
    /// rejected, failed.
    fn counts(&self, table: &Table, id: u16) -> [u64; 4] {
        let mut sum = [0; 4];
        for slot in table.slots.iter() {
            let counts = &slot.counts[id as usize];
            sum[0] += counts.calls.load(Relaxed);
            sum[1] += counts.nanos.load(Relaxed);
            sum[2] += counts.rejected.load(Relaxed);
            sum[3] += counts.failed.load(Relaxed);
        }
        sum
    }

    /// Command `id`'s histogram, summed over the slots, if it has one.
    fn histogram(&self, table: &Table, id: u16) -> Option<Vec<u64>> {
        let mut sum = vec![0; BUCKETS];
        let mut any = false;
        for histogram in table
            .slots
            .iter()
            .filter_map(|slot| slot.histograms[id as usize].get())
        {
            for (total, bucket) in sum.iter_mut().zip(histogram.0.iter()) {
                *total += bucket.load(Relaxed);
            }
            any = true;
        }
        (any && sum.iter().any(|&n| n > 0)).then_some(sum)
    }

    /// `INFO commandstats`'s lines.
    pub(crate) fn commandstats(&self, s: &mut String) {
        let Some(table) = self.table.get() else {
            return;
        };
        for id in 0..acl::command_count() as u16 {
            let [calls, nanos, rejected, failed] = self.counts(table, id);
            if calls == 0 && rejected == 0 && failed == 0 {
                continue;
            }
            let usec = nanos / 1000;
            let per_call = if calls == 0 {
                0.0
            } else {
                nanos as f64 / 1000.0 / calls as f64
            };
            let _ = write!(
                s,
                "cmdstat_{}:calls={calls},usec={usec},usec_per_call={per_call:.2},\
                 rejected_calls={rejected},failed_calls={failed}\r\n",
                acl::command_name(id)
            );
        }
    }

    /// `INFO errorstats`'s lines.
    pub(crate) fn errorstats(&self, s: &mut String) {
        let Some(table) = self.table.get() else {
            return;
        };
        for (code, count) in table.errors.lock().iter() {
            let _ = write!(
                s,
                "errorstat_{}:count={count}\r\n",
                String::from_utf8_lossy(code)
            );
        }
    }

    /// `INFO latencystats`'s lines.
    pub(crate) fn latencystats(&self, s: &mut String) {
        let Some(table) = self.table.get() else {
            return;
        };
        let percentiles = self.percentiles.lock().clone();
        for id in 0..acl::command_count() as u16 {
            let Some(histogram) = self.histogram(table, id) else {
                continue;
            };
            let _ = write!(s, "latency_percentiles_usec_{}:", acl::command_name(id));
            for (i, &p) in percentiles.iter().enumerate() {
                if i > 0 {
                    s.push(',');
                }
                let nanos = value_at_percentile(&histogram, p);
                let _ = write!(s, "p{}={:.3}", format_percentile(p), nanos as f64 / 1000.0);
            }
            s.push_str("\r\n");
        }
    }

    /// `LATENCY HISTOGRAM [command ...]`.
    pub(crate) fn latency_histogram(&self, args: &Args<'_>, resp3: bool, out: &mut Vec<u8>) {
        let Some(table) = self.table.get() else {
            resp::write_map_header(out, 0, resp3);
            return;
        };
        let mut ids = Vec::new();
        let add = |id: u16, ids: &mut Vec<u16>| {
            ids.push(id);
            ids.extend_from_slice(acl::subcommand_ids(id));
        };
        if args.len() == 2 {
            for id in 0..acl::command_count() as u16 {
                ids.push(id);
            }
        } else {
            for i in 2..args.len() {
                if let Some(id) = acl::find_command_id(args.get(i)) {
                    add(id, &mut ids);
                }
            }
        }
        let found: Vec<(u16, Vec<u64>)> = ids
            .into_iter()
            .filter_map(|id| Some((id, self.histogram(table, id)?)))
            .collect();
        resp::write_map_header(out, found.len(), resp3);
        for (id, histogram) in found {
            resp::write_bulk(out, acl::command_name(id).as_bytes());
            resp::write_map_header(out, 2, resp3);
            resp::write_bulk(out, b"calls");
            resp::write_integer(out, histogram.iter().sum::<u64>() as i64);
            resp::write_bulk(out, b"histogram_usec");
            let steps = cumulative(&histogram);
            resp::write_map_header(out, steps.len(), resp3);
            for (micros, count) in steps {
                resp::write_integer(out, micros as i64);
                resp::write_integer(out, count as i64);
            }
        }
    }
}

/// The bucket for a latency of `nanos`.
fn bucket(nanos: u64) -> usize {
    let v = nanos.clamp(1, MAX_NANOS);
    if v < SUB as u64 {
        return v as usize;
    }
    let power = 63 - v.leading_zeros();
    let step = (v >> (power - SUB_BITS)) as usize & (SUB - 1);
    (power - SUB_BITS + 1) as usize * SUB + step
}

/// The lowest and highest latency bucket `i` holds.
fn bucket_range(i: usize) -> (u64, u64) {
    if i < SUB {
        return (i as u64, i as u64);
    }
    let power = (i / SUB) as u32 + SUB_BITS - 1;
    let unit = 1u64 << (power - SUB_BITS);
    let low = (1u64 << power) + (i % SUB) as u64 * unit;
    (low, low + unit - 1)
}

/// The latency at percentile `p`, as Redis' `hdr_value_at_percentile` finds it:
/// the highest in the bucket that holds it (the lowest, for 0).
fn value_at_percentile(histogram: &[u64], p: f64) -> u64 {
    let total: u64 = histogram.iter().sum();
    let wanted = ((p.min(100.0) / 100.0 * total as f64) + 0.5) as u64;
    let wanted = wanted.max(1);
    let mut seen = 0;
    for (i, &count) in histogram.iter().enumerate() {
        seen += count;
        if seen >= wanted {
            let (low, high) = bucket_range(i);
            return if p == 0.0 { low } else { high };
        }
    }
    0
}

/// `LATENCY HISTOGRAM`'s steps: how many took at most each power of two of
/// microseconds, as Redis reports them (its keys are the highest value its
/// histogram holds with 1024 ns and its doublings, in microseconds), each
/// once it's more than the step before.
fn cumulative(histogram: &[u64]) -> Vec<(u64, u64)> {
    let mut steps = Vec::new();
    let mut level: u64 = 1024;
    let mut seen = 0;
    let mut i = 0;
    let total: u64 = histogram.iter().sum();
    let mut previous = 0;
    while previous < total && level <= 1 << 31 {
        while i < histogram.len() && bucket_range(i).0 <= level {
            seen += histogram[i];
            i += 1;
        }
        if seen > previous {
            // Redis' histogram keeps two significant figures: a unit of a 128th.
            let highest = level + (level >> 7) - 1;
            steps.push((highest / 1000, seen));
            previous = seen;
        }
        level *= 2;
    }
    steps
}

/// A percentile as Redis prints it: `%f`, trailing zeros trimmed.
pub(crate) fn format_percentile(p: f64) -> String {
    let fixed = format!("{p:.6}");
    let trimmed = fixed.trim_end_matches('0').trim_end_matches('.');
    trimmed.to_string()
}

/// Parses `latency-tracking-info-percentiles` as Redis does: numbers between
/// spaces from 0 to 100, or nothing. Its normal form, or the error.
pub(crate) fn parse_percentiles(value: &str) -> Result<(String, Vec<f64>), &'static str> {
    let mut percentiles = Vec::new();
    for word in value.split_whitespace() {
        // As Redis' `string2ld`: a whole number, not NaN.
        let p = word
            .parse::<f64>()
            .ok()
            .filter(|p| !p.is_nan())
            .ok_or("Invalid latency-tracking-info-percentiles parameters")?;
        if !(0.0..=100.0).contains(&p) {
            return Err(
                "latency-tracking-info-percentiles parameters should sit between [0.0,100.0]",
            );
        }
        percentiles.push(p);
    }
    let normal: Vec<String> = percentiles.iter().map(|&p| format_percentile(p)).collect();
    Ok((normal.join(" "), percentiles))
}

/// A command run while counting: what it ran as, what it replied, and what
/// it's counted as.
pub(crate) fn execute(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    COUNTING.set(true);
    RAN.set(None);
    INNER_ERROR.set(None);
    let start = out.len();
    let reply = crate::cmd::execute(ctx, session, now, args, out);
    COUNTING.set(false);
    if matches!(reply, Reply::Paused) {
        return reply; // nothing's run: it runs again later
    }
    let code = match reply {
        Reply::Bulk(_) => None,
        _ => error_code(&out[start..]),
    };
    // A script's error that's the error of a command it called counts once.
    let inner = INNER_ERROR.take();
    let counted =
        code.filter(|code| inner.is_none_or(|(bytes, len)| &bytes[..len as usize] != *code));
    if let Some(code) = counted {
        ctx.stats.error(code);
    }
    match RAN.take() {
        Some(mut ran) => {
            if code.is_some() {
                ran.turned_away = crate::cmd::turned_away_args(args);
            }
            ctx.stats.called(ran, code);
        }
        None => {
            if code.is_some()
                && let Some(id) = crate::cmd::command_id(args)
            {
                ctx.stats.rejected(id);
            }
        }
    }
    reply
}

/// Whether [`execute`] is running a command on this thread.
#[inline]
pub(crate) fn counting() -> bool {
    COUNTING.get()
}

/// A command ran (on the checked path, while counting): the one [`execute`]
/// runs, or with `inner` its reply, one inside a transaction or a script.
pub(crate) fn ran(ctx: &Context, ran: Ran, inner: Option<&[u8]>) {
    match inner {
        Some(reply) => {
            let code = error_code(reply);
            if let Some(code) = code {
                ctx.stats.error(code);
                note_inner_error(code);
            }
            ctx.stats.called(ran, code);
        }
        None if COUNTING.get() => RAN.set(Some(ran)),
        // Counting started while it ran.
        None => ctx.stats.called(ran, None),
    }
}

/// A script's call refused before it ran: Lua hears `message`, an `ERR`.
pub(crate) fn refused_in_script(ctx: &Context, args: &Args<'_>) {
    ctx.stats.error(b"ERR");
    note_inner_error(b"ERR");
    if let Some(id) = crate::cmd::command_id(args) {
        ctx.stats.rejected(id);
    }
}

/// A blocked command ended with `reply`: an error from what served it, or
/// from `CLIENT UNBLOCK`, makes it a failed call.
pub(crate) fn blocked_ended(ctx: &Context, args: &Args<'_>, reply: &[u8]) {
    if let Some(code) = error_code(reply) {
        ctx.stats.error(code);
        if let Some(id) = crate::cmd::command_id(args) {
            ctx.stats.failed(id);
        }
    }
}

fn note_inner_error(code: &[u8]) {
    let mut bytes = [0; 32];
    let len = code.len().min(32);
    bytes[..len].copy_from_slice(&code[..len]);
    INNER_ERROR.set(Some((bytes, len as u8)));
}

/// An error reply's code, as Redis' `afterErrorReply` takes it: the word after
/// the `-`, if a space comes within 32 bytes, else `ERR`.
fn error_code(reply: &[u8]) -> Option<&[u8]> {
    let rest = reply.strip_prefix(b"-")?;
    let window = &reply[..reply.len().min(32)];
    match window.iter().position(|&b| b == b' ') {
        Some(space) => Some(&rest[..space - 1]),
        None => Some(b"ERR"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_hold_what_they_say() {
        let mut last = 0;
        for v in 1..200_000u64 {
            let i = bucket(v);
            let (low, high) = bucket_range(i);
            assert!(low <= v && v <= high, "{v} in bucket {i}: {low}..={high}");
            assert!(i >= last);
            last = i;
            // To within a sixteenth.
            assert!((high - low) * 16 <= low.max(16), "{v}: {low}..={high}");
        }
        assert_eq!(bucket(MAX_NANOS * 5), bucket(MAX_NANOS));
        assert!(bucket(MAX_NANOS) < BUCKETS);
        assert_eq!(bucket(0), 1);
    }

    #[test]
    fn histogram_steps_are_redis_keys() {
        let mut histogram = vec![0; BUCKETS];
        for nanos in [500, 1_500, 1_500, 3_000, 40_000, 2_000_000_000] {
            histogram[bucket(nanos)] += 1;
        }
        assert_eq!(
            cumulative(&histogram),
            [(1, 1), (2, 3), (4, 4), (66, 5), (1_082_130, 6)]
        );
        assert_eq!(value_at_percentile(&histogram, 0.0), 496);
        assert_eq!(value_at_percentile(&histogram, 50.0), 1_535);
        assert_eq!(
            value_at_percentile(&histogram, 100.0),
            bucket_range(bucket(MAX_NANOS)).1
        );
    }

    #[test]
    fn percentiles_parse_as_in_redis() {
        assert_eq!(parse_percentiles("50 99 99.9").unwrap().0, "50 99 99.9");
        assert_eq!(parse_percentiles("0.0 50.0 100.0").unwrap().0, "0 50 100");
        assert_eq!(parse_percentiles("").unwrap().0, "");
        assert!(parse_percentiles("10.0 50.0 a").is_err());
        assert!(parse_percentiles("10.0 50.0 101.0").is_err());
    }

    #[test]
    fn error_codes_as_in_redis() {
        assert_eq!(error_code(b"-ERR wrong number\r\n"), Some(&b"ERR"[..]));
        assert_eq!(
            error_code(b"-NOSCRIPT No matching\r\n"),
            Some(&b"NOSCRIPT"[..])
        );
        assert_eq!(
            error_code(b"-AVERYLONGERRORCODEWITHOUTANYSPACEATALL x\r\n"),
            Some(&b"ERR"[..])
        );
        assert_eq!(error_code(b"+OK\r\n"), None);
    }
}
