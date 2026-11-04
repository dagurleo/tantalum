//! BullMQ's hottest scripts, natively: `moveToActive` (a worker takes a
//! job) and `moveToFinished` (it completes or fails one, and takes the
//! next), as BullMQ 6.5.4 sends them (the exact texts, by SHA-1, are in
//! `tests/data/bullmq`; other versions run on Lua as before). A busy queue
//! runs them for every job, and in Lua they cost most of the server's time.
//!
//! Each runs the commands its script runs, in the same order, through the
//! same command code as `redis.call`, so the store ends up exactly as the
//! script would leave it, and replies the same. What's rare or intricate
//! (rate limits, delayed jobs coming due, parents, deduplication,
//! retention by age or count, metrics, legacy markers) goes back to the Lua
//! script: decided by reads before anything is written, so the script then
//! runs on the same state. A command that fails once writing has begun
//! fails as it would in Lua, with the line of its `redis.call`.

use std::ops::Range;

use crate::cmd::{self, Args};
use crate::context::{Context, Session};
use crate::resp;
use crate::script::format_number;
use crate::store::Millis;

/// What a native script did.
pub(crate) enum Outcome {
    /// It ran, and its reply is written.
    Done,
    /// It wrote nothing: run the Lua script instead.
    Lua,
}

pub(crate) type Native = fn(&mut Call<'_>) -> Outcome;

/// The native version of the script with SHA-1 `sha`, if there is one.
pub(crate) fn find(sha: &[u8; 40]) -> Option<Native> {
    match sha {
        b"d96ca626f1ea141b44a856b312fc636d99aa9ea9" => Some(move_to_active),
        b"69666e69dd5621df59067a03de685e55a486560d" => Some(move_to_finished),
        _ => None,
    }
}

/// A native script's run: its keys and arguments, and commands run as its
/// `redis.call`s would be.
pub(crate) struct Call<'a> {
    pub(crate) ctx: &'a Context,
    pub(crate) session: Session,
    pub(crate) now: Millis,
    pub(crate) sha: &'a [u8; 40],
    pub(crate) keys: Vec<&'a [u8]>,
    pub(crate) argv: Vec<&'a [u8]>,
    /// The reply to the client.
    pub(crate) out: &'a mut Vec<u8>,
    buf: Vec<u8>,
    ranges: Vec<Range<usize>>,
    reply: Vec<u8>,
}

/// A command's reply, as a script sees it (RESP2).
#[derive(Debug, PartialEq)]
enum Value {
    Int(i64),
    Bulk(Vec<u8>),
    /// A null bulk string or array: `false` in Lua.
    Nil,
    Array(Vec<Value>),
    Status(Vec<u8>),
}

/// A command failed: its error reply (`-ERR ...`, without CRLF).
struct Failed(Vec<u8>);

impl<'a> Call<'a> {
    pub(crate) fn new(
        ctx: &'a Context,
        now: Millis,
        sha: &'a [u8; 40],
        keys: Vec<&'a [u8]>,
        argv: Vec<&'a [u8]>,
        out: &'a mut Vec<u8>,
    ) -> Self {
        Call {
            ctx,
            session: Session::for_script(),
            now,
            sha,
            keys,
            argv,
            out,
            buf: Vec::new(),
            ranges: Vec::new(),
            reply: Vec::new(),
        }
    }

    fn key(&self, i: usize) -> &'a [u8] {
        self.keys[i - 1]
    }

    fn arg(&self, i: usize) -> Option<&'a [u8]> {
        self.argv.get(i - 1).copied()
    }

    /// Runs a command as `redis.call` does, leaving its reply in `reply`.
    fn run(&mut self, args: &[&[u8]]) -> Result<(), Failed> {
        self.buf.clear();
        self.ranges.clear();
        for arg in args {
            let start = self.buf.len();
            self.buf.extend_from_slice(arg);
            self.ranges.push(start..self.buf.len());
        }
        self.reply.clear();
        let args = Args::new(&self.buf, &self.ranges);
        if let Err(e) = cmd::call_from_script(
            self.ctx,
            &mut self.session,
            self.now,
            &args,
            false,
            &mut self.reply,
        ) {
            return Err(Failed(format!("-ERR {e}").into_bytes()));
        }
        if self.reply.first() == Some(&b'-') {
            let end = self.reply.len().saturating_sub(2);
            return Err(Failed(self.reply[..end].to_vec()));
        }
        Ok(())
    }

    /// [`run`](Self::run), and its reply parsed.
    fn call(&mut self, args: &[&[u8]]) -> Result<Value, Failed> {
        self.run(args)?;
        let mut input = &self.reply[..];
        Ok(parse(&mut input))
    }

    /// A command while writing: if it fails, the script fails as Lua does at
    /// `line`, and `None` stops the native run.
    fn write(&mut self, line: u32, args: &[&[u8]]) -> Option<Value> {
        match self.call(args) {
            Ok(value) => Some(value),
            Err(Failed(error)) => {
                self.fail(line, &error);
                None
            }
        }
    }

    /// The reply to a script whose `redis.call` at `line` got `error`, as
    /// Lua builds it: the error table's text, then where it happened.
    fn fail(&mut self, line: u32, error: &[u8]) {
        let text = &error[1..];
        self.out.push(b'-');
        match memchr::memchr(b' ', text) {
            Some(_) => self.out.extend_from_slice(text),
            None => {
                self.out.extend_from_slice(b"ERR ");
                self.out.extend_from_slice(text);
            }
        }
        self.out.extend_from_slice(b" script: ");
        self.out.extend_from_slice(self.sha);
        self.out
            .extend_from_slice(format!(", on @user_script:{line}.\r\n").as_bytes());
    }

    fn reply_int(&mut self, n: i64) -> Outcome {
        resp::write_integer(self.out, n);
        Outcome::Done
    }

    /// `{0, 0, a, b}`, which tells a worker there's no job (yet).
    fn reply_no_job(&mut self, a: i64, b: i64) -> Outcome {
        self.out
            .extend_from_slice(format!("*4\r\n:0\r\n:0\r\n:{a}\r\n:{b}\r\n").as_bytes());
        Outcome::Done
    }
}

/// Parses one RESP2 reply (not an error).
fn parse(input: &mut &[u8]) -> Value {
    let Some((&kind, rest)) = input.split_first() else {
        return Value::Nil;
    };
    let end = memchr::memchr(b'\n', rest).unwrap_or(rest.len());
    let head = rest[..end].strip_suffix(b"\r").unwrap_or(&rest[..end]);
    *input = &rest[(end + 1).min(rest.len())..];
    let number = || resp::parse_int(head).unwrap_or(-1);
    match kind {
        b':' => Value::Int(number()),
        b'+' => Value::Status(head.to_vec()),
        b'$' => match usize::try_from(number()) {
            Ok(n) => {
                let n = n.min(input.len());
                let data = input[..n].to_vec();
                *input = &input[(n + 2).min(input.len())..];
                Value::Bulk(data)
            }
            Err(_) => Value::Nil,
        },
        b'*' => match usize::try_from(number()) {
            Ok(n) => Value::Array((0..n).map(|_| parse(input)).collect()),
            Err(_) => Value::Nil,
        },
        _ => Value::Nil,
    }
}

impl Value {
    fn int(&self) -> Option<i64> {
        match self {
            Value::Int(n) => Some(*n),
            _ => None,
        }
    }

    fn bulk(&self) -> Option<&[u8]> {
        match self {
            Value::Bulk(b) => Some(b),
            _ => None,
        }
    }
}

/// A msgpack value as `cmsgpack.unpack` gives it to Lua: every number a
/// double.
#[derive(Debug, Clone, PartialEq)]
enum Msg {
    Nil,
    Bool(bool),
    Num(f64),
    Str(Vec<u8>),
    Array(Vec<Msg>),
    Map(Vec<(Msg, Msg)>),
}

impl Msg {
    /// Decodes one value; `None` for anything this doesn't read (Lua does:
    /// such calls go back to it).
    fn decode(bytes: &[u8]) -> Option<Msg> {
        let mut input = bytes;
        Msg::read(&mut input, 0)
    }

    fn read(input: &mut &[u8], depth: usize) -> Option<Msg> {
        if depth > 32 {
            return None;
        }
        let take = |input: &mut &[u8], n: usize| -> Option<Vec<u8>> {
            if input.len() < n {
                return None;
            }
            let (data, rest) = input.split_at(n);
            *input = rest;
            Some(data.to_vec())
        };
        let uint = |input: &mut &[u8], n: usize| -> Option<u64> {
            take(input, n).map(|b| b.iter().fold(0u64, |acc, &x| acc << 8 | u64::from(x)))
        };
        let (&tag, rest) = input.split_first()?;
        *input = rest;
        let items = |input: &mut &[u8], n: u64, pairs: bool| -> Option<Msg> {
            if pairs {
                let mut map = Vec::new();
                for _ in 0..n {
                    let k = Msg::read(input, depth + 1)?;
                    let v = Msg::read(input, depth + 1)?;
                    map.push((k, v));
                }
                Some(Msg::Map(map))
            } else {
                let mut array = Vec::new();
                for _ in 0..n {
                    array.push(Msg::read(input, depth + 1)?);
                }
                Some(Msg::Array(array))
            }
        };
        Some(match tag {
            0x00..=0x7f => Msg::Num(f64::from(tag)),
            0x80..=0x8f => return items(input, u64::from(tag & 0x0f), true),
            0x90..=0x9f => return items(input, u64::from(tag & 0x0f), false),
            0xa0..=0xbf => Msg::Str(take(input, usize::from(tag & 0x1f))?),
            0xc0 => Msg::Nil,
            0xc2 => Msg::Bool(false),
            0xc3 => Msg::Bool(true),
            0xc4 | 0xd9 => {
                let n = uint(input, 1)? as usize;
                Msg::Str(take(input, n)?)
            }
            0xc5 | 0xda => {
                let n = uint(input, 2)? as usize;
                Msg::Str(take(input, n)?)
            }
            0xc6 | 0xdb => {
                let n = uint(input, 4)? as usize;
                Msg::Str(take(input, n)?)
            }
            0xca => Msg::Num(f64::from(f32::from_bits(uint(input, 4)? as u32))),
            0xcb => Msg::Num(f64::from_bits(uint(input, 8)?)),
            0xcc => Msg::Num(uint(input, 1)? as f64),
            0xcd => Msg::Num(uint(input, 2)? as f64),
            0xce => Msg::Num(uint(input, 4)? as f64),
            0xcf => Msg::Num(uint(input, 8)? as f64),
            0xd0 => Msg::Num(f64::from(uint(input, 1)? as u8 as i8)),
            0xd1 => Msg::Num(f64::from(uint(input, 2)? as u16 as i16)),
            0xd2 => Msg::Num(f64::from(uint(input, 4)? as u32 as i32)),
            0xd3 => Msg::Num(uint(input, 8)? as i64 as f64),
            0xdc => {
                let n = uint(input, 2)?;
                return items(input, n, false);
            }
            0xdd => {
                let n = uint(input, 4)?;
                return items(input, n, false);
            }
            0xde => {
                let n = uint(input, 2)?;
                return items(input, n, true);
            }
            0xdf => {
                let n = uint(input, 4)?;
                return items(input, n, true);
            }
            0xe0..=0xff => Msg::Num(f64::from(tag as i8)),
            // Extensions and anything else: Lua's to read.
            _ => return None,
        })
    }

    /// `t[name]` of a map; `Nil` when absent, as in Lua.
    fn get(&self, name: &str) -> &Msg {
        const NIL: &Msg = &Msg::Nil;
        match self {
            Msg::Map(entries) => entries
                .iter()
                .rev()
                .find(|(k, _)| matches!(k, Msg::Str(s) if s == name.as_bytes()))
                .map_or(NIL, |(_, v)| v),
            _ => NIL,
        }
    }

    /// The value as a `redis.call` argument: a string, or a number as Lua
    /// formats it there.
    fn arg(&self) -> Option<Vec<u8>> {
        match self {
            Msg::Str(s) => Some(s.clone()),
            Msg::Num(n) => Some(format_number(*n)),
            _ => None,
        }
    }
}

/// `tonumber(s)` for the decimal integers BullMQ passes (timestamps):
/// anything else goes to Lua.
fn plain_number(s: &[u8]) -> Option<f64> {
    let digits = s.strip_prefix(b"-").unwrap_or(s);
    if digits.is_empty() || digits.len() > 15 || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(s).ok()?.parse().ok()
}

/// The queue's state for taking a job, read before anything is written: a
/// worker's options, the queue's metadata, and that no delayed job is due
/// and no legacy marker waits (or `None`, for Lua).
struct Fetch {
    token: Vec<u8>,
    lock_duration: Option<Vec<u8>>,
    name: Option<Vec<u8>>,
    paused: bool,
    concurrency: Option<f64>,
}

impl Fetch {
    /// `opts` is the worker's (msgpack); `timestamp` the script's.
    fn read(
        call: &mut Call<'_>,
        opts: &Msg,
        timestamp: f64,
        meta: &[u8],
        delayed: &[u8],
        wait: &[u8],
    ) -> Option<Fetch> {
        if !matches!(opts, Msg::Map(_)) || *opts.get("limiter") != Msg::Nil {
            return None;
        }
        let Msg::Str(token) = opts.get("token") else {
            return None;
        };
        let lock_duration = if token.as_slice() != b"0" {
            Some(opts.get("lockDuration").arg()?)
        } else {
            None
        };
        let name = match opts.get("name") {
            Msg::Nil => None,
            Msg::Str(name) => Some(name.clone()),
            _ => return None,
        };
        let Value::Array(attributes) = call
            .call(&[
                b"HMGET",
                meta,
                b"paused",
                b"concurrency",
                b"max",
                b"duration",
            ])
            .ok()?
        else {
            return None;
        };
        // Rate limited: Lua's.
        if attributes.get(2) != Some(&Value::Nil) {
            return None;
        }
        let paused = attributes.first() != Some(&Value::Nil);
        let concurrency = match attributes.get(1) {
            Some(Value::Nil) => None,
            Some(Value::Bulk(c)) => Some(plain_number(c)?),
            _ => return None,
        };
        // Delayed jobs due now: Lua promotes them.
        let due = format_number((timestamp + 1.0) * 4096.0 - 1.0);
        let jobs = call
            .call(&[
                b"ZRANGEBYSCORE",
                delayed,
                b"0",
                &due,
                b"LIMIT",
                b"0",
                b"1000",
            ])
            .ok()?;
        if jobs != Value::Array(Vec::new()) {
            return None;
        }
        // A legacy marker ("0:...") next in line: Lua's.
        match call.call(&[b"LINDEX", wait, b"-1"]).ok()? {
            Value::Nil => {}
            Value::Bulk(next) if !next.starts_with(b"0:") => {}
            _ => return None,
        }
        Some(Fetch {
            token: token.clone(),
            lock_duration,
            name,
            paused,
            concurrency,
        })
    }
}

/// Lines of the `redis.call`s a fetch makes, in one script's text.
struct FetchLines {
    llen_active: u32,
    rpoplpush: u32,
    zpopmin: u32,
    lpush_active: u32,
    del_priority_counter: u32,
    zrange_delayed: u32,
    set_lock: u32,
    xadd_active: u32,
    hmset_processed: u32,
    hincrby_ats: u32,
    zadd_marker: u32,
    hgetall: u32,
}

const MOVE_TO_ACTIVE_LINES: FetchLines = FetchLines {
    llen_active: 48,
    rpoplpush: 217,
    zpopmin: 89,
    lpush_active: 91,
    del_priority_counter: 94,
    zrange_delayed: 62,
    set_lock: 130,
    xadd_active: 138,
    hmset_processed: 139,
    hincrby_ats: 140,
    zadd_marker: 110,
    hgetall: 144,
};

const MOVE_TO_FINISHED_LINES: FetchLines = FetchLines {
    llen_active: 139,
    rpoplpush: 313,
    zpopmin: 180,
    lpush_active: 182,
    del_priority_counter: 185,
    zrange_delayed: 153,
    set_lock: 221,
    xadd_active: 229,
    hmset_processed: 230,
    hincrby_ats: 231,
    zadd_marker: 201,
    hgetall: 235,
};

/// The keys a fetch uses.
struct FetchKeys<'a> {
    wait: &'a [u8],
    active: &'a [u8],
    prioritized: &'a [u8],
    events: &'a [u8],
    delayed: &'a [u8],
    priority_counter: &'a [u8],
    marker: &'a [u8],
    prefix: &'a [u8],
    timestamp: &'a [u8],
}

/// What a fetch did: replied (a job, or when to look again), or found
/// nothing, or failed (the reply is the error).
enum Fetched {
    Replied,
    Nothing,
    Failed,
}

/// Takes the next job, as `moveToActive`'s body and `moveToFinished`'s
/// `fetchNextJob` do (without a rate limit, due delayed jobs or legacy
/// markers: [`Fetch::read`] sent those to Lua).
fn fetch(call: &mut Call<'_>, keys: &FetchKeys<'_>, state: &Fetch, lines: &FetchLines) -> Fetched {
    let mut maxed = state.paused;
    if !maxed && let Some(concurrency) = state.concurrency {
        let Some(active) = call.write(lines.llen_active, &[b"LLEN", keys.active]) else {
            return Fetched::Failed;
        };
        maxed = active.int().unwrap_or(0) as f64 >= concurrency;
    }
    if maxed {
        call.reply_no_job(0, 0);
        return Fetched::Replied;
    }
    let Some(popped) = call.write(lines.rpoplpush, &[b"RPOPLPUSH", keys.wait, keys.active]) else {
        return Fetched::Failed;
    };
    let job = match popped {
        Value::Bulk(id) => Some(id),
        _ => {
            let Some(Value::Array(top)) =
                call.write(lines.zpopmin, &[b"ZPOPMIN", keys.prioritized])
            else {
                return Fetched::Failed;
            };
            match top.into_iter().next() {
                Some(Value::Bulk(id)) => {
                    if call
                        .write(lines.lpush_active, &[b"LPUSH", keys.active, &id])
                        .is_none()
                    {
                        return Fetched::Failed;
                    }
                    Some(id)
                }
                _ => {
                    if call
                        .write(lines.del_priority_counter, &[b"DEL", keys.priority_counter])
                        .is_none()
                    {
                        return Fetched::Failed;
                    }
                    None
                }
            }
        }
    };
    let Some(id) = job else {
        // When the next delayed job is due, if any.
        let Some(Value::Array(first)) = call.write(
            lines.zrange_delayed,
            &[b"ZRANGE", keys.delayed, b"0", b"0", b"WITHSCORES"],
        ) else {
            return Fetched::Failed;
        };
        let score = first
            .get(1)
            .and_then(Value::bulk)
            .and_then(|s| std::str::from_utf8(s).ok()?.parse::<f64>().ok());
        return match score {
            Some(score) => {
                call.reply_no_job(0, crate::script::lua_integer(score / 4096.0));
                Fetched::Replied
            }
            None => Fetched::Nothing,
        };
    };
    if prepare(call, keys, state, lines, &id) {
        Fetched::Replied
    } else {
        Fetched::Failed
    }
}

/// `prepareJobForProcessing` without a rate limit: locks the job, records
/// it, and replies `{job's fields, id, 0, 0}`. False if it failed.
fn prepare(
    call: &mut Call<'_>,
    keys: &FetchKeys<'_>,
    state: &Fetch,
    lines: &FetchLines,
    id: &[u8],
) -> bool {
    let job = [keys.prefix, id].concat();
    if let Some(duration) = &state.lock_duration {
        let lock = [&job[..], b":lock"].concat();
        if call
            .write(
                lines.set_lock,
                &[b"SET", &lock, &state.token, b"PX", duration],
            )
            .is_none()
        {
            return false;
        }
    }
    let event: [&[u8]; 9] = [
        b"XADD",
        keys.events,
        b"*",
        b"event",
        b"active",
        b"jobId",
        id,
        b"prev",
        b"waiting",
    ];
    if call.write(lines.xadd_active, &event).is_none() {
        return false;
    }
    let mut fields: Vec<&[u8]> = vec![b"HMSET", &job, b"processedOn", keys.timestamp];
    if let Some(name) = &state.name {
        fields.extend([&b"pb"[..], name]);
    }
    if call.write(lines.hmset_processed, &fields).is_none()
        || call
            .write(lines.hincrby_ats, &[b"HINCRBY", &job, b"ats", b"1"])
            .is_none()
        || call
            .write(lines.zadd_marker, &[b"ZADD", keys.marker, b"0", b"0"])
            .is_none()
    {
        return false;
    }
    if let Err(Failed(error)) = call.run(&[b"HGETALL", &job]) {
        call.fail(lines.hgetall, &error);
        return false;
    }
    // {HGETALL's reply, id, 0, 0}: the fields come over as they are.
    call.out.extend_from_slice(b"*4\r\n");
    call.out.extend_from_slice(&call.reply);
    resp::write_bulk(call.out, id);
    call.out.extend_from_slice(b":0\r\n:0\r\n");
    true
}

/// `moveToActive-11`.
fn move_to_active(call: &mut Call<'_>) -> Outcome {
    if call.keys.len() != 11 || call.argv.len() < 3 {
        return Outcome::Lua;
    }
    let (Some(prefix), Some(timestamp), Some(opts)) = (call.arg(1), call.arg(2), call.arg(3))
    else {
        return Outcome::Lua;
    };
    let (Some(opts), Some(ts)) = (Msg::decode(opts), plain_number(timestamp)) else {
        return Outcome::Lua;
    };
    let keys = FetchKeys {
        wait: call.key(1),
        active: call.key(2),
        prioritized: call.key(3),
        events: call.key(4),
        delayed: call.key(7),
        priority_counter: call.key(10),
        marker: call.key(11),
        prefix,
        timestamp,
    };
    let Some(state) = Fetch::read(call, &opts, ts, call.key(9), keys.delayed, keys.wait) else {
        return Outcome::Lua;
    };
    match fetch(call, &keys, &state, &MOVE_TO_ACTIVE_LINES) {
        Fetched::Replied | Fetched::Failed => Outcome::Done,
        Fetched::Nothing => call.reply_no_job(0, 0),
    }
}

/// `moveToFinished-14`, for jobs without a parent or deduplication, kept
/// all (`removeOnComplete: false`) or none (`true`), without metrics.
fn move_to_finished(call: &mut Call<'_>) -> Outcome {
    if call.keys.len() != 14 || call.argv.len() < 8 {
        return Outcome::Lua;
    }
    let job = call.key(12);
    // The checks the script makes before writing anything.
    match call.call(&[b"EXISTS", job]) {
        Ok(Value::Int(1)) => {}
        Ok(Value::Int(_)) => return call.reply_int(-1),
        _ => return Outcome::Lua,
    }
    let [
        id,
        timestamp,
        field,
        value,
        target,
        fetch_next,
        prefix,
        opts,
    ] = [1, 2, 3, 4, 5, 6, 7, 8].map(|i| call.arg(i).unwrap_or_default());
    let completed = target == b"completed";
    if completed {
        let dependencies = [job, b":dependencies"].concat();
        match call.call(&[b"SCARD", &dependencies]) {
            Ok(Value::Int(0)) => {}
            Ok(Value::Int(_)) => return call.reply_int(-4),
            _ => return Outcome::Lua,
        }
        let unsuccessful = [job, b":unsuccessful"].concat();
        match call.call(&[b"ZCARD", &unsuccessful]) {
            Ok(Value::Int(0)) => {}
            Ok(Value::Int(_)) => return call.reply_int(-9),
            _ => return Outcome::Lua,
        }
    }
    let (Some(opts), Some(ts)) = (Msg::decode(opts), plain_number(timestamp)) else {
        return Outcome::Lua;
    };
    let Msg::Str(token) = opts.get("token") else {
        return Outcome::Lua;
    };
    // Only what's handled here: no metrics, no retention by age, kept all
    // or none.
    if *opts.get("maxMetricsSize") != Msg::Str(Vec::new())
        || *opts.get("keepJobs").get("age") != Msg::Nil
    {
        return Outcome::Lua;
    }
    let keep = match opts.get("keepJobs") {
        Msg::Map(_) => match opts.get("keepJobs").get("count") {
            Msg::Num(n) if *n == 0.0 => false,
            Msg::Num(n) if *n < 0.0 => true,
            Msg::Nil => true,
            _ => return Outcome::Lua,
        },
        _ => return Outcome::Lua,
    };
    let attempts = match opts.get("attempts") {
        Msg::Num(n) => Some(*n),
        _ if completed => None,
        _ => return Outcome::Lua,
    };
    // The fields to update: strings and numbers in pairs, none of them
    // the parent or deduplication ones read below.
    let updates = match call.arg(9) {
        None | Some(b"") => None,
        Some(packed) => {
            let Some(Msg::Array(items)) = Msg::decode(packed) else {
                return Outcome::Lua;
            };
            let Some(items) = items.iter().map(Msg::arg).collect::<Option<Vec<_>>>() else {
                return Outcome::Lua;
            };
            if items.is_empty()
                || items.len() % 2 != 0
                || items
                    .iter()
                    .step_by(2)
                    .any(|f| [&b"parentKey"[..], b"parent", b"deid"].contains(&f.as_slice()))
            {
                return Outcome::Lua;
            }
            Some(items)
        }
    };
    match call.call(&[b"HMGET", job, b"parentKey", b"parent", b"deid"]) {
        Ok(Value::Array(a)) if a.iter().all(|v| *v == Value::Nil) => {}
        _ => return Outcome::Lua,
    }
    let fetching = fetch_next == b"1";
    let keys = FetchKeys {
        wait: call.key(1),
        active: call.key(2),
        prioritized: call.key(3),
        events: call.key(4),
        delayed: call.key(7),
        priority_counter: call.key(10),
        marker: call.key(14),
        prefix,
        timestamp,
    };
    let state = if fetching {
        match Fetch::read(call, &opts, ts, call.key(9), keys.delayed, keys.wait) {
            Some(state) => Some(state),
            None => return Outcome::Lua,
        }
    } else {
        None
    };

    // removeLock, then writes in the script's order.
    if token.as_slice() != b"0" {
        let lock = [job, b":lock"].concat();
        let held = match call.call(&[b"GET", &lock]) {
            Ok(held) => held,
            Err(_) => return Outcome::Lua,
        };
        match held {
            Value::Bulk(t) if t == *token => {}
            Value::Bulk(_) => return call.reply_int(-6),
            _ => return call.reply_int(-2),
        }
        if call.write(679, &[b"DEL", &lock]).is_none()
            || call.write(680, &[b"SREM", call.key(5), id]).is_none()
        {
            return Outcome::Done;
        }
    }
    if let Some(updates) = &updates {
        let mut args: Vec<&[u8]> = vec![b"HMSET", job];
        args.extend(updates.iter().map(Vec::as_slice));
        if call.write(898, &args).is_none() {
            return Outcome::Done;
        }
    }
    match call.write(938, &[b"LREM", keys.active, b"-1", id]) {
        Some(Value::Int(n)) if n >= 1 => {}
        Some(_) => return call.reply_int(-3),
        None => return Outcome::Done,
    }
    // trimEvents.
    let meta = call.key(9);
    let max_events = match call.write(705, &[b"HGET", meta, b"opts.maxLenEvents"]) {
        Some(Value::Bulk(n)) => n,
        Some(_) => {
            if call
                .write(708, &[b"HSET", meta, b"opts.maxLenEvents", b"10000"])
                .is_none()
            {
                return Outcome::Done;
            }
            b"10000".to_vec()
        }
        None => return Outcome::Done,
    };
    if call
        .write(876, &[b"XTRIM", keys.events, b"MAXLEN", b"~", &max_events])
        .is_none()
    {
        return Outcome::Done;
    }
    let Some(made) = call.write(974, &[b"HINCRBY", job, b"atm", b"1"]) else {
        return Outcome::Done;
    };
    let made = made.int().unwrap_or(0);
    if keep {
        if call
            .write(979, &[b"ZADD", call.key(11), timestamp, id])
            .is_none()
            || call
                .write(980, &[b"HSET", job, field, value, b"finishedOn", timestamp])
                .is_none()
            || (!completed && call.write(983, &[b"HDEL", job, b"defa"]).is_none())
        {
            return Outcome::Done;
        }
    } else {
        let parts: [&[u8]; 5] = [
            b":logs",
            b":dependencies",
            b":processed",
            b":failed",
            b":unsuccessful",
        ];
        let related: Vec<Vec<u8>> = parts.iter().map(|p| [job, p].concat()).collect();
        let mut args: Vec<&[u8]> = vec![b"DEL", job];
        args.extend(related.iter().map(Vec::as_slice));
        if call.write(521, &args).is_none() {
            return Outcome::Done;
        }
    }
    let event: [&[u8]; 11] = [
        b"XADD",
        keys.events,
        b"*",
        b"event",
        target,
        b"jobId",
        id,
        field,
        value,
        b"prev",
        b"active",
    ];
    if call.write(1001, &event).is_none() {
        return Outcome::Done;
    }
    if !completed && attempts.is_some_and(|a| made as f64 >= a) {
        let made = format_number(made as f64);
        let exhausted: [&[u8]; 9] = [
            b"XADD",
            keys.events,
            b"*",
            b"event",
            b"retries-exhausted",
            b"jobId",
            id,
            b"attemptsMade",
            &made,
        ];
        if call.write(1004, &exhausted).is_none() {
            return Outcome::Done;
        }
    }
    if let Some(state) = &state {
        match fetch(call, &keys, state, &MOVE_TO_FINISHED_LINES) {
            Fetched::Replied | Fetched::Failed => return Outcome::Done,
            Fetched::Nothing => {}
        }
    }
    // A drained queue says so.
    for (line, args) in [
        (1022, [&b"LLEN"[..], keys.wait]),
        (1024, [&b"LLEN"[..], keys.active]),
        (1026, [&b"ZCARD"[..], keys.prioritized]),
    ] {
        match call.write(line, &args) {
            Some(Value::Int(0)) => {}
            Some(_) => return call.reply_int(0),
            None => return Outcome::Done,
        }
    }
    if call
        .write(1028, &[b"XADD", keys.events, b"*", b"event", b"drained"])
        .is_none()
    {
        return Outcome::Done;
    }
    call.reply_int(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn msgpack_reads_as_cmsgpack_does() {
        // {"token": "t", "n": 300, "neg": -2, "f": 1.5, "a": [true, nil]}
        let packed = [
            0x85, 0xa5, b't', b'o', b'k', b'e', b'n', 0xa1, b't', 0xa1, b'n', 0xcd, 0x01, 0x2c,
            0xa3, b'n', b'e', b'g', 0xfe, 0xa1, b'f', 0xcb, 0x3f, 0xf8, 0, 0, 0, 0, 0, 0, 0xa1,
            b'a', 0x92, 0xc3, 0xc0,
        ];
        let m = Msg::decode(&packed).unwrap();
        assert_eq!(*m.get("token"), Msg::Str(b"t".to_vec()));
        assert_eq!(*m.get("n"), Msg::Num(300.0));
        assert_eq!(*m.get("neg"), Msg::Num(-2.0));
        assert_eq!(*m.get("f"), Msg::Num(1.5));
        assert_eq!(*m.get("a"), Msg::Array(vec![Msg::Bool(true), Msg::Nil]));
        assert_eq!(*m.get("missing"), Msg::Nil);
        assert_eq!(m.get("n").arg(), Some(b"300".to_vec()));
        assert_eq!(Msg::decode(&[0xc7, 1, 1, 0]), None);
    }

    /// The line numbers native failures report are where the scripts'
    /// `redis.call`s are.
    #[test]
    fn failure_lines_point_at_the_scripts_calls() {
        let read = |name: &str| {
            let path = format!(
                "{}/tests/data/bullmq/{name}.lua",
                env!("CARGO_MANIFEST_DIR")
            );
            std::fs::read_to_string(path).unwrap()
        };
        let check = |text: &str, line: u32, call: &str| {
            let at = text.lines().nth(line as usize - 1).unwrap();
            assert!(at.contains(call), "line {line} is {at:?}, not {call}");
        };
        let active = read("moveToActive-11");
        let finished = read("moveToFinished-14");
        for (text, l) in [
            (&active, &MOVE_TO_ACTIVE_LINES),
            (&finished, &MOVE_TO_FINISHED_LINES),
        ] {
            check(text, l.llen_active, "rcall(\"LLEN\", activeKey)");
            check(
                text,
                l.rpoplpush,
                "rcall(\"RPOPLPUSH\", waitKey, activeKey)",
            );
            check(text, l.zpopmin, "rcall(\"ZPOPMIN\", priorityKey)");
            check(
                text,
                l.lpush_active,
                "rcall(\"LPUSH\", activeKey, prioritizedJob[1])",
            );
            check(
                text,
                l.del_priority_counter,
                "rcall(\"DEL\", priorityCounterKey)",
            );
            check(
                text,
                l.zrange_delayed,
                "rcall(\"ZRANGE\", delayedKey, 0, 0, \"WITHSCORES\")",
            );
            check(text, l.set_lock, "rcall(\"SET\", lockKey, opts['token']");
            check(text, l.xadd_active, "\"event\", \"active\"");
            check(
                text,
                l.hmset_processed,
                "rcall(\"HMSET\", jobKey, \"processedOn\"",
            );
            check(
                text,
                l.hincrby_ats,
                "rcall(\"HINCRBY\", jobKey, \"ats\", 1)",
            );
            check(text, l.zadd_marker, "rcall(\"ZADD\", markerKey, 0, \"0\")");
            check(text, l.hgetall, "rcall(\"HGETALL\", jobKey)");
        }
        for (line, call) in [
            (679, "rcall(\"DEL\", lockKey)"),
            (680, "rcall(\"SREM\", stalledKey, jobId)"),
            (898, "rcall(\"HMSET\", jobKey, unpack(fieldsToUpdate))"),
            (938, "rcall(\"LREM\", KEYS[2], -1, jobId)"),
            (705, "rcall(\"HGET\", metaKey, \"opts.maxLenEvents\")"),
            (
                708,
                "rcall(\"HSET\", metaKey, \"opts.maxLenEvents\", maxEvents)",
            ),
            (
                876,
                "rcall(\"XTRIM\", eventStreamKey, \"MAXLEN\", \"~\", maxEvents)",
            ),
            (974, "rcall(\"HINCRBY\", jobIdKey, \"atm\", 1)"),
            (979, "rcall(\"ZADD\", targetSet, timestamp, jobId)"),
            (
                980,
                "rcall(\"HSET\", jobIdKey, ARGV[3], ARGV[4], \"finishedOn\", timestamp)",
            ),
            (983, "rcall(\"HDEL\", jobIdKey, \"defa\")"),
            (521, "return rcall(\"DEL\", jobKey, jobKey .. ':logs'"),
            (1001, "\"prev\", \"active\")"),
            (1004, "\"retries-exhausted\""),
            (1022, "rcall(\"LLEN\", KEYS[1])"),
            (1024, "rcall(\"LLEN\", KEYS[2])"),
            (1026, "rcall(\"ZCARD\", KEYS[3])"),
            (1028, "\"event\", \"drained\""),
        ] {
            check(&finished, line, call);
        }
    }

    #[test]
    fn the_scripts_are_the_ones_recognised() {
        for (name, native) in [
            ("moveToActive-11", true),
            ("moveToFinished-14", true),
            ("addStandardJob-9", false),
        ] {
            let path = format!(
                "{}/tests/data/bullmq/{name}.lua",
                env!("CARGO_MANIFEST_DIR")
            );
            let sha = crate::script::sha1_hex(&std::fs::read(path).unwrap());
            assert_eq!(find(&sha).is_some(), native, "{name}");
        }
    }
}
