//! Whole RDB files, as Redis writes and reads them: what a master sends a
//! replica for a full sync, and what a replica of Redis is sent. Values are
//! written as `DUMP` writes them (`cmd/dump.rs`, `rdb.rs`), so a file is what
//! Redis 7.2 would write for the same data but for its hash tables' order; and
//! read with `RESTORE`'s reader, so every encoding Redis 2.6 to 7.2 writes
//! loads (modules' types don't).
//!
//! A file: `REDIS` and a four-digit version, auxiliary fields, function
//! libraries, then for each database a `SELECTDB` and a `RESIZEDB` and its
//! keys (each perhaps after its expiry time), then `EOF` and, from version 5,
//! a CRC-64 of everything before it (0 when Redis was told not to compute
//! one).

use std::io::{self, Write};

use crate::cmd::dump;
use crate::context::Context;
use crate::rdb::{self, Reader};
use std::sync::Arc;

use crate::store::{CopySlot, Expiry, Millis, NO_EXPIRY, Store};

/// What Redis 7.2 writes; anything up to it loads.
const VERSION: u16 = 11;

const OPCODE_IDLE: u8 = 248;
const OPCODE_FREQ: u8 = 249;
const OPCODE_AUX: u8 = 250;
const OPCODE_RESIZEDB: u8 = 251;
const OPCODE_EXPIRETIME_MS: u8 = 252;
const OPCODE_EXPIRETIME: u8 = 253;
const OPCODE_SELECTDB: u8 = 254;
const OPCODE_EOF: u8 = 255;
const OPCODE_MODULE_AUX: u8 = 247;
/// Modules' value types.
const TYPE_MODULE_PRE_GA: u8 = 6;
const TYPE_MODULE_2: u8 = 7;

/// The replication fields of an RDB made for a replica (Redis'
/// `rdbSaveInfo`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Aux {
    /// The master's replication ID, 40 hex digits.
    pub repl_id: String,
    /// Its replication offset as of the data in the file.
    pub repl_offset: i64,
    /// The database its replication stream was in last (-1 if none yet).
    pub repl_stream_db: i64,
}

/// Counts the CRC-64 of what goes through it.
struct Summed<'a, W: Write> {
    out: &'a mut W,
    crc: u64,
}

impl<W: Write> Summed<'_, W> {
    fn put(&mut self, data: &[u8]) -> io::Result<()> {
        self.crc = rdb::crc64(self.crc, data);
        self.out.write_all(data)
    }
}

fn aux_field(buf: &mut Vec<u8>, name: &str, value: &[u8]) {
    buf.push(OPCODE_AUX);
    rdb::put_string(buf, name.as_bytes());
    rdb::put_string(buf, value);
}

/// Writes all of `store` (every database, and the function libraries) as an
/// RDB file to `out`, as of store-clock time `now`: what has expired by then
/// is left out (`now` 0 leaves nothing out). Nothing may write meanwhile (the
/// caller holds the write turn). Returns how many keys it wrote.
#[cfg(test)]
pub(crate) fn write(
    store: &Arc<Store>,
    now: Millis,
    aux: &Aux,
    out: &mut impl Write,
) -> io::Result<u64> {
    write_copied(copy(store, now), aux, out)
}

/// The data as it is, copied to be written later ([`copy`]).
pub(crate) struct Copied {
    now: Millis,
    /// The store clock's Unix time at 0: deadlines are this much after it.
    unix_base: i64,
    used_memory: usize,
    functions: Vec<std::sync::Arc<[u8]>>,
    dbs: Vec<CopiedDb>,
}

/// A database as [`copy`] found it.
struct CopiedDb {
    index: usize,
    /// How many keys, and how many with a TTL (for `RESIZEDB`).
    keys: usize,
    with_ttl: usize,
    db: Arc<Store>,
    /// Its shards' copies, owed.
    slots: Vec<Arc<CopySlot>>,
}

/// The data as it is now, to be written as an RDB file later
/// ([`write_copied`]), with nothing copied yet: each shard copies itself
/// before anything changes it (its segments' bytes, big collections cloned,
/// big values shared), or when the writing gets to it. So a full sync holds
/// writes only while this marks them. Keys expired at `now` (unless it's 0)
/// are left out when written.
pub(crate) fn copy(store: &Arc<Store>, now: Millis) -> Copied {
    let made: Vec<usize> = store.live().map(Store::index).collect();
    let dbs = made
        .into_iter()
        .map(|i| {
            let db = store.db(i).clone();
            let (keys, with_ttl, slots) = db.request_copies();
            CopiedDb {
                index: i,
                keys,
                with_ttl,
                db,
                slots,
            }
        })
        .collect();
    Copied {
        now,
        unix_base: store.deadline_to_unix_ms(0, 0),
        used_memory: store.used_memory(),
        functions: store.function_codes().1,
        dbs,
    }
}

/// Writes what [`copy`] copied as a whole RDB file, as Redis 7.2.4 writes one
/// for a replica, dropping each shard's copy once written. Returns how many
/// keys it wrote.
pub(crate) fn write_copied(copied: Copied, aux: &Aux, out: &mut impl Write) -> io::Result<u64> {
    let Copied {
        now,
        unix_base,
        used_memory,
        functions,
        dbs,
    } = copied;
    let mut out = Summed { out, crc: 0 };
    let mut buf = Vec::with_capacity(1 << 16);
    buf.extend_from_slice(format!("REDIS{VERSION:04}").as_bytes());
    // As Redis 7.2.4's `rdbSaveInfoAuxFields` writes them for a replica.
    let int = |n: i64| itoa::Buffer::new().format(n).as_bytes().to_vec();
    let ctime = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64);
    aux_field(&mut buf, "redis-ver", b"7.2.4");
    aux_field(&mut buf, "redis-bits", &int(usize::BITS as i64));
    aux_field(&mut buf, "ctime", &int(ctime));
    aux_field(&mut buf, "used-mem", &int(used_memory as i64));
    aux_field(&mut buf, "repl-stream-db", &int(aux.repl_stream_db));
    aux_field(&mut buf, "repl-id", aux.repl_id.as_bytes());
    aux_field(&mut buf, "repl-offset", &int(aux.repl_offset));
    aux_field(&mut buf, "aof-base", b"0");
    for code in &functions {
        buf.push(rdb::OPCODE_FUNCTION2);
        rdb::put_string(&mut buf, code);
    }
    out.put(&buf)?;
    let mut keys = 0u64;
    let mut body = Vec::new();
    for CopiedDb {
        index,
        keys: count,
        with_ttl,
        db,
        slots,
    } in dbs
    {
        if count == 0 {
            // (Its shards made, or will make, their copies, which go unread.)
            for (i, slot) in slots.iter().enumerate() {
                drop(db.take_copy(i, slot));
            }
            continue;
        }
        buf.clear();
        buf.push(OPCODE_SELECTDB);
        rdb::put_len(&mut buf, index as u64);
        buf.push(OPCODE_RESIZEDB);
        rdb::put_len(&mut buf, count as u64);
        rdb::put_len(&mut buf, with_ttl as u64);
        out.put(&buf)?;
        for (i, slot) in slots.iter().enumerate() {
            let shard = db.take_copy(i, slot);
            buf.clear();
            shard.for_each_live_ref(now, |key, value, deadline| {
                if deadline != NO_EXPIRY {
                    buf.push(OPCODE_EXPIRETIME_MS);
                    let at = unix_base.saturating_add(deadline as i64);
                    buf.extend_from_slice(&at.to_le_bytes());
                }
                body.clear();
                dump::value_body(&mut body, &value);
                // The type, the key, then the value.
                buf.push(body[0]);
                rdb::put_string(&mut buf, key);
                buf.extend_from_slice(&body[1..]);
                keys += 1;
            });
            out.put(&buf)?;
        }
    }
    out.put(&[OPCODE_EOF])?;
    let crc = out.crc;
    out.out.write_all(&crc.to_le_bytes())?;
    Ok(keys)
}

/// What [`load`] found.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Loaded {
    pub keys: u64,
    /// Of those, keys whose expiry time had passed: loaded all the same (a
    /// replica keeps them until its master deletes them; reads don't see
    /// them).
    pub expired: u64,
    /// Function libraries loaded.
    pub functions: usize,
    /// The file's RDB version.
    pub version: u16,
    /// The replication fields, if it has them.
    pub repl_id: Option<String>,
    pub repl_offset: Option<i64>,
    pub repl_stream_db: Option<i64>,
}

/// Loads a whole RDB file into `ctx`'s databases, over what's there (the
/// caller flushes first, functions included: libraries named again are an
/// error). Writes aren't held to `maxmemory` meanwhile. On an error, what
/// came before it stays loaded.
pub(crate) fn load(ctx: &Context, data: &[u8]) -> Result<Loaded, String> {
    ctx.store.set_loading(true);
    let loaded = load_keys(ctx, data);
    ctx.store.set_loading(false);
    loaded
}

fn load_keys(ctx: &Context, data: &[u8]) -> Result<Loaded, String> {
    let short = || "Short read or OOM loading DB. Unrecoverable error, aborting now.".to_string();
    if data.len() < 9 || &data[..5] != b"REDIS" {
        return Err("Wrong signature trying to load DB from file".into());
    }
    let version = std::str::from_utf8(&data[5..9])
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .filter(|v| (1..=VERSION).contains(v))
        .ok_or_else(|| {
            format!(
                "Can't handle RDB format version {}",
                String::from_utf8_lossy(&data[5..9])
            )
        })?;
    let mut loaded = Loaded {
        version,
        ..Loaded::default()
    };
    let store = &ctx.store;
    let now = store.now();
    let limits = ctx.stream_limits();
    let mut reader = Reader::new(&data[9..]);
    let mut db = store;
    let mut expiry: Option<i64> = None;
    let mut functions = Vec::new();
    loop {
        let kind = reader.byte().ok_or_else(short)?;
        match kind {
            OPCODE_EXPIRETIME => {
                let s = reader.take(4).ok_or_else(short)?;
                let s = i32::from_le_bytes(s.try_into().expect("4 bytes"));
                expiry = Some(i64::from(s) * 1000);
                continue;
            }
            OPCODE_EXPIRETIME_MS => {
                let ms = reader.take(8).ok_or_else(short)?;
                expiry = Some(i64::from_le_bytes(ms.try_into().expect("8 bytes")));
                continue;
            }
            OPCODE_FREQ => {
                reader.byte().ok_or_else(short)?;
                continue;
            }
            OPCODE_IDLE => {
                reader.len().ok_or_else(short)?;
                continue;
            }
            OPCODE_EOF => break,
            OPCODE_SELECTDB => {
                let index = reader.len().ok_or_else(short)?;
                if index >= store.databases() as u64 {
                    return Err(format!(
                        "FATAL: Data file was created with a Redis server configured to \
                         handle more than {} databases. Exiting\n",
                        store.databases()
                    ));
                }
                db = store.db(index as usize);
                continue;
            }
            OPCODE_RESIZEDB => {
                reader.len().ok_or_else(short)?;
                reader.len().ok_or_else(short)?;
                continue;
            }
            OPCODE_AUX => {
                let name = reader.string().ok_or_else(short)?;
                let value = reader.string().ok_or_else(short)?;
                let int = || std::str::from_utf8(&value).ok()?.parse::<i64>().ok();
                match &name[..] {
                    b"repl-id" => loaded.repl_id = Some(String::from_utf8_lossy(&value).into()),
                    b"repl-offset" => loaded.repl_offset = int(),
                    b"repl-stream-db" => loaded.repl_stream_db = int(),
                    _ => {}
                }
                continue;
            }
            OPCODE_MODULE_AUX => {
                return Err(
                    "The RDB file contains AUX module data I can't load: no matching module".into(),
                );
            }
            rdb::OPCODE_FUNCTION_PRE_GA => {
                return Err("Pre-GA function format not supported".into());
            }
            rdb::OPCODE_FUNCTION2 => {
                let code = reader.string().ok_or_else(short)?;
                functions.push(rdb::OPCODE_FUNCTION2);
                rdb::put_string(&mut functions, &code);
                loaded.functions += 1;
                continue;
            }
            TYPE_MODULE_PRE_GA | TYPE_MODULE_2 => {
                return Err(
                    "The RDB file contains module data I can't load: no matching module".into(),
                );
            }
            _ => {}
        }
        let key = reader.string().ok_or_else(short)?;
        let object = rdb::load_object(kind, &mut reader, limits)
            .ok_or_else(|| format!("Bad data format (type {kind})"))?;
        let at = expiry.take();
        // Redis skips the empty collections an older version could leave.
        let empty = match &object {
            rdb::Object::List(v) | rdb::Object::Set(v) => v.is_empty(),
            rdb::Object::ZSet(v) => v.is_empty(),
            rdb::Object::Hash(v) => v.is_empty(),
            rdb::Object::String(_) | rdb::Object::Stream(_) => false,
        };
        if empty {
            continue;
        }
        let expiry = match at {
            None => Expiry::Never,
            Some(at) => {
                if at <= db.deadline_to_unix_ms(now, now) {
                    loaded.expired += 1;
                }
                Expiry::At(db.deadline_from_unix_ms(now, at))
            }
        };
        let (kind, value) = dump::packed(object);
        db.restore(&key, kind, &value, expiry, now)
            .map_err(|_| short())?;
        loaded.keys += 1;
    }
    // The checksum, of everything before it, from version 5 (0: not made).
    if version >= 5 {
        let end = data.len() - reader.remaining();
        let stored = reader.take(8).ok_or_else(short)?;
        let stored = u64::from_le_bytes(stored.try_into().expect("8 bytes"));
        if stored != 0 && stored != rdb::crc64(0, &data[..end]) {
            return Err("Wrong RDB checksum. Aborting now.".into());
        }
    }
    if !functions.is_empty() {
        rdb::finish_payload(&mut functions);
        let mut libraries = ctx.scripting.functions.lock();
        libraries
            .restore(store, &functions, crate::script::functions::Policy::Append)
            .map_err(|e| String::from_utf8_lossy(&e).into_owned())?;
        ctx.scripting.count_functions(&libraries);
    }
    Ok(loaded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::{self, Args};
    use crate::context::{Session, Settings};
    use std::sync::Arc;

    fn context() -> Arc<Context> {
        Arc::new(Context::new(Arc::new(Store::new(16)), Settings::default()))
    }

    fn run(ctx: &Context, session: &mut Session, words: &[&[u8]]) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut ranges = Vec::new();
        for w in words {
            ranges.push(buf.len()..buf.len() + w.len());
            buf.extend_from_slice(w);
        }
        let args = Args::new(&buf, &ranges);
        let mut out = Vec::new();
        let now = ctx.store.now();
        if let cmd::Reply::Bulk(value) = cmd::execute(ctx, session, now, &args, &mut out) {
            crate::resp::write_bulk(&mut out, value.as_bytes());
        }
        out
    }

    fn cmd(ctx: &Context, session: &mut Session, line: &str) -> Vec<u8> {
        let words: Vec<&[u8]> = line.split(' ').map(str::as_bytes).collect();
        run(ctx, session, &words)
    }

    /// A key as held: its database, name, `DUMP` reply and `PTTL` reply.
    type Held = (usize, Vec<u8>, Vec<u8>, Vec<u8>);

    /// Every key of every database, as held.
    fn contents(ctx: &Context) -> Vec<Held> {
        let client = ctx.connect(-1).unwrap();
        let mut session = client.session();
        let mut all = Vec::new();
        for db in 0..16 {
            cmd(ctx, &mut session, &format!("SELECT {db}"));
            let mut keys = Vec::new();
            for shard in 0..ctx.store.db(db).shard_count() {
                ctx.store
                    .db(db)
                    .for_each_key(shard, 0, |k, _| keys.push(k.to_vec()));
            }
            keys.sort();
            for key in keys {
                let dump = run(ctx, &mut session, &[b"DUMP", &key]);
                let pttl = run(ctx, &mut session, &[b"PTTL", &key]);
                let pttl = if pttl.starts_with(b":-") {
                    pttl
                } else {
                    // Within a second: the clock moves between the two.
                    let n: i64 = std::str::from_utf8(&pttl[1..pttl.len() - 2])
                        .unwrap()
                        .parse()
                        .unwrap();
                    format!(":{}\r\n", n / 10_000).into_bytes()
                };
                all.push((db, key, dump, pttl));
            }
        }
        all
    }

    fn fill(ctx: &Context) {
        let client = ctx.connect(-1).unwrap();
        let mut s = client.session();
        for line in [
            "SET s hello",
            "SET n 12345",
            "SET neg -7",
            "SET ttl x EX 1000",
            "RPUSH l a b c",
            "SADD is 1 2 3",
            "SADD ss a b c",
            "ZADD z 1 a 2.5 b",
            "HSET h f v g w",
            "XADD x 1-1 f v",
            "XADD x 2-1 f v g w",
            "XGROUP CREATE x g 0",
            "XREADGROUP GROUP g c COUNT 1 STREAMS x >",
            "SELECT 3",
            "SET other db",
            "HSET h3 a 1",
            "SELECT 0",
        ] {
            let reply = cmd(ctx, &mut s, line);
            assert!(!reply.starts_with(b"-"), "{line}: {reply:?}");
        }
        let long = "x".repeat(200);
        for i in 0..300 {
            cmd(ctx, &mut s, &format!("RPUSH bigl {long}{i}"));
            cmd(ctx, &mut s, &format!("SADD bigs m{i}"));
            cmd(ctx, &mut s, &format!("SADD bigis {i}"));
            cmd(ctx, &mut s, &format!("ZADD bigz {i} m{i}"));
            cmd(ctx, &mut s, &format!("HSET bigh f{i} {long}"));
            cmd(ctx, &mut s, &format!("XADD bigx * f {i}"));
        }
        let code = b"#!lua name=lib\nredis.register_function('f', function() return 1 end)";
        let reply = run(ctx, &mut s, &[b"FUNCTION", b"LOAD", code]);
        assert!(!reply.starts_with(b"-"), "{reply:?}");
    }

    #[test]
    fn every_type_comes_back() {
        let ctx = context();
        fill(&ctx);
        let aux = Aux {
            repl_id: "a".repeat(40),
            repl_offset: 1234,
            repl_stream_db: 0,
        };
        let mut file = Vec::new();
        let keys = write(&ctx.store, ctx.store.now(), &aux, &mut file).unwrap();
        assert_eq!(keys, 18);
        let copy = context();
        let loaded = load(&copy, &file).unwrap();
        assert_eq!(loaded.keys, 18);
        assert_eq!(loaded.functions, 1);
        assert_eq!(loaded.repl_id.as_deref(), Some(&*aux.repl_id));
        assert_eq!(loaded.repl_offset, Some(1234));
        assert_eq!(loaded.repl_stream_db, Some(0));
        assert_eq!(contents(&ctx), contents(&copy));
        let client = copy.connect(-1).unwrap();
        let mut s = client.session();
        assert_eq!(cmd(&copy, &mut s, "FCALL f 0"), b":1\r\n");
        // And it writes the same file again (but for the time in it).
        let mut again = Vec::new();
        write(&copy.store, copy.store.now(), &aux, &mut again).unwrap();
        assert_eq!(again.len(), file.len());
    }

    #[test]
    fn damage_and_strangers_are_refused() {
        let ctx = context();
        fill(&ctx);
        let mut file = Vec::new();
        write(&ctx.store, ctx.store.now(), &Aux::default(), &mut file).unwrap();
        let mut bad = file.clone();
        let at = bad.len() / 2;
        bad[at] ^= 0xff;
        assert!(load(&context(), &bad).is_err());
        assert!(load(&context(), b"REDIS0012\xff").is_err());
        assert!(load(&context(), b"NOTREDIS").is_err());
        // No checksum (Redis' `rdbchecksum no`) loads.
        let mut unsummed = file.clone();
        let n = unsummed.len();
        unsummed[n - 8..].fill(0);
        assert!(load(&context(), &unsummed).is_ok());
    }

    #[test]
    fn expired_keys_are_kept_and_hidden() {
        let ctx = context();
        // A string that expired in 2001, then a live one.
        let mut file = b"REDIS0011".to_vec();
        file.push(OPCODE_EXPIRETIME_MS);
        file.extend_from_slice(&1_000_000_000_000i64.to_le_bytes());
        file.push(rdb::TYPE_STRING);
        rdb::put_string(&mut file, b"gone");
        rdb::put_string(&mut file, b"v");
        file.push(rdb::TYPE_STRING);
        rdb::put_string(&mut file, b"here");
        rdb::put_string(&mut file, b"v");
        file.push(OPCODE_EOF);
        let crc = rdb::crc64(0, &file);
        file.extend_from_slice(&crc.to_le_bytes());
        let loaded = load(&ctx, &file).unwrap();
        assert_eq!((loaded.keys, loaded.expired), (2, 1));
        assert_eq!(ctx.store.len(), 2);
        let client = ctx.connect(-1).unwrap();
        let mut s = client.session();
        assert_eq!(cmd(&ctx, &mut s, "EXISTS gone here"), b":1\r\n");
    }

    /// Written by Redis 7.2.4 (`tests/data/redis-7.2.4.rdb.txt` says what's in it).
    #[test]
    fn reads_what_redis_writes() {
        let file = std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/data/redis-7.2.4.rdb"
        ))
        .unwrap();
        let ctx = context();
        let loaded = load(&ctx, &file).unwrap();
        assert_eq!(loaded.version, 11);
        assert_eq!(loaded.functions, 1);
        let client = ctx.connect(-1).unwrap();
        let mut s = client.session();
        let expected = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/data/redis-7.2.4.rdb.txt"
        ))
        .unwrap();
        for line in expected
            .lines()
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
        {
            let (command, reply) = line.split_once(" => ").unwrap();
            let reply = reply.replace("\\r\\n", "\r\n");
            assert_eq!(
                String::from_utf8_lossy(&cmd(&ctx, &mut s, command)),
                reply,
                "{command}"
            );
        }
        assert_eq!(
            loaded.keys as usize,
            ctx.store.live().map(Store::len).sum::<usize>()
        );
    }
}
