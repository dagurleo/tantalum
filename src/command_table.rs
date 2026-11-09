//! What Redis 7.2.4 says about each command here, for `COMMAND`: its flags, ACL
//! categories, tips, key specifications, subcommands and docs, generated from a
//! real Redis by `tools/command-table.py` so that they're its own. In RESP3, and
//! read only when asked for: nothing here is on a command's way.

use std::collections::HashMap;
use std::sync::OnceLock;

static TABLE: &[u8] = include_bytes!("command_table.resp");

/// One command's raw `COMMAND INFO` entry and `COMMAND DOCS` map.
pub(crate) struct Entry {
    pub(crate) info: &'static [u8],
    pub(crate) docs: &'static [u8],
}

/// A RESP3 value.
pub(crate) enum Value<'a> {
    Text(&'a [u8]),
    Int(i64),
    /// An array or a set.
    List(Vec<Value<'a>>),
    Map(Vec<(Value<'a>, Value<'a>)>),
    Null,
}

impl<'a> Value<'a> {
    pub(crate) fn text(&self) -> &'a [u8] {
        match self {
            Value::Text(text) => text,
            _ => b"",
        }
    }

    pub(crate) fn int(&self) -> i64 {
        match self {
            Value::Int(n) => *n,
            _ => 0,
        }
    }

    pub(crate) fn list(&self) -> &[Value<'a>] {
        match self {
            Value::List(items) => items,
            _ => &[],
        }
    }

    /// In a map, the value of the field `name`.
    pub(crate) fn field(&self, name: &[u8]) -> Option<&Value<'a>> {
        match self {
            Value::Map(pairs) => pairs.iter().find(|(k, _)| k.text() == name).map(|(_, v)| v),
            _ => None,
        }
    }
}

/// A line's kind, its text, and where the next line starts.
fn line(data: &[u8], at: usize) -> (u8, &[u8], usize) {
    let end = at
        + data[at..]
            .windows(2)
            .position(|w| w == b"\r\n")
            .expect("a whole line");
    (data[at], &data[at + 1..end], end + 2)
}

fn number(text: &[u8]) -> i64 {
    std::str::from_utf8(text)
        .ok()
        .and_then(|t| t.parse().ok())
        .expect("a number")
}

/// The value at `at`, and where the next one starts.
pub(crate) fn parse(data: &[u8], at: usize) -> (Value<'_>, usize) {
    let (kind, text, mut next) = line(data, at);
    match kind {
        b'$' => {
            let size = number(text) as usize;
            (Value::Text(&data[next..next + size]), next + size + 2)
        }
        b'+' => (Value::Text(text), next),
        b':' => (Value::Int(number(text)), next),
        b'_' => (Value::Null, next),
        b'*' | b'~' => {
            let n = number(text) as usize;
            let mut items = Vec::with_capacity(n);
            for _ in 0..n {
                let (item, after) = parse(data, next);
                items.push(item);
                next = after;
            }
            (Value::List(items), next)
        }
        b'%' => {
            let n = number(text) as usize;
            let mut pairs = Vec::with_capacity(n);
            for _ in 0..n {
                let (key, after) = parse(data, next);
                let (value, after) = parse(data, after);
                pairs.push((key, value));
                next = after;
            }
            (Value::Map(pairs), next)
        }
        other => panic!("a RESP3 type not in the table: {}", other as char),
    }
}

/// The same value as RESP2 (maps flat, sets as arrays), as Redis would write it.
fn downgrade(data: &[u8], at: usize, out: &mut Vec<u8>) -> usize {
    let (kind, text, mut next) = line(data, at);
    match kind {
        b'$' => {
            let size = number(text) as usize;
            out.extend_from_slice(&data[at..next + size + 2]);
            next + size + 2
        }
        b'+' | b':' => {
            out.extend_from_slice(&data[at..next]);
            next
        }
        b'_' => {
            out.extend_from_slice(b"$-1\r\n");
            next
        }
        b'*' | b'~' | b'%' => {
            let mut n = number(text) as usize;
            if kind == b'%' {
                n *= 2;
            }
            out.extend_from_slice(format!("*{n}\r\n").as_bytes());
            for _ in 0..n {
                next = downgrade(data, next, out);
            }
            next
        }
        other => panic!("a RESP3 type not in the table: {}", other as char),
    }
}

/// Writes a value of the table in the connection's protocol.
pub(crate) fn write(value: &[u8], resp3: bool, out: &mut Vec<u8>) {
    if resp3 {
        out.extend_from_slice(value);
    } else {
        downgrade(value, 0, out);
    }
}

fn entries() -> &'static HashMap<&'static str, Entry> {
    static ENTRIES: OnceLock<HashMap<&'static str, Entry>> = OnceLock::new();
    ENTRIES.get_or_init(|| {
        let mut entries = HashMap::new();
        let mut at = 0;
        while at < TABLE.len() {
            let (name, info_at) = parse(TABLE, at);
            let (_, docs_at) = parse(TABLE, info_at);
            let (_, end) = parse(TABLE, docs_at);
            let name = std::str::from_utf8(name.text()).expect("a name in ASCII");
            entries.insert(
                name,
                Entry {
                    info: &TABLE[info_at..docs_at],
                    docs: &TABLE[docs_at..end],
                },
            );
            at = end;
        }
        entries
    })
}

/// The table's entry for command `name`, in lowercase.
pub(crate) fn entry(name: &str) -> Option<&'static Entry> {
    entries().get(name)
}

/// The fields of a `COMMAND INFO` entry.
pub(crate) const ARITY: usize = 1;
pub(crate) const FLAGS: usize = 2;
pub(crate) const CATEGORIES: usize = 6;
pub(crate) const KEY_SPECS: usize = 8;
pub(crate) const SUBCOMMANDS: usize = 9;

fn has(item: &Value<'_>, field: usize, name: &[u8]) -> bool {
    item.list()[field].list().iter().any(|v| v.text() == name)
}

/// The table's parsed entry for `name` (with `subcommand`, if it has that one).
/// `None` if there's no such command, or it has subcommands and not that one.
fn parsed(name: &str, subcommand: Option<&[u8]>) -> Option<Value<'static>> {
    let (value, _) = parse(entry(name)?.info, 0);
    let Some(subcommand) = subcommand else {
        return Some(value);
    };
    let subs = value.list()[SUBCOMMANDS].list();
    if subs.is_empty() {
        return Some(value);
    }
    let full = format!(
        "{name}|{}",
        String::from_utf8_lossy(subcommand).to_ascii_lowercase()
    );
    let found = subs
        .iter()
        .position(|s| s.list()[0].text() == full.as_bytes())?;
    let Value::List(mut items) = value else {
        return None;
    };
    let Value::List(mut subs) = items.remove(SUBCOMMANDS) else {
        return None;
    };
    Some(subs.swap_remove(found))
}

/// Which of the flags that `MONITOR` and `SLOWLOG` go by a command has.
#[derive(Clone, Copy, Default)]
pub(crate) struct Hidden {
    pub(crate) from_monitor: bool,
    pub(crate) from_slowlog: bool,
    pub(crate) container: bool,
    /// What `CLIENT TRACKING` goes by: it remembers the keys of `readonly`
    /// commands that aren't `pubsub` ones.
    pub(crate) readonly: bool,
    pub(crate) pubsub: bool,
}

/// The flags of `name`, in lowercase, with the subcommand in `arg` if it has
/// subcommands.
pub(crate) fn hidden(name: &str, arg: Option<&[u8]>) -> Hidden {
    static ALL: OnceLock<HashMap<String, Hidden>> = OnceLock::new();
    let all = ALL.get_or_init(|| {
        let mut all = HashMap::new();
        for (name, entry) in entries() {
            let (value, _) = parse(entry.info, 0);
            let mut add = |full: String, item: &Value<'_>| {
                all.insert(
                    full,
                    Hidden {
                        from_monitor: has(item, FLAGS, b"admin")
                            || has(item, FLAGS, b"skip_monitor"),
                        from_slowlog: has(item, FLAGS, b"skip_slowlog"),
                        container: !item.list()[SUBCOMMANDS].list().is_empty(),
                        readonly: has(item, FLAGS, b"readonly"),
                        pubsub: has(item, FLAGS, b"pubsub"),
                    },
                );
            };
            add((*name).to_string(), &value);
            for sub in value.list()[SUBCOMMANDS].list() {
                add(
                    String::from_utf8_lossy(sub.list()[0].text()).into_owned(),
                    sub,
                );
            }
        }
        all
    });
    if let Some(arg) = arg {
        let full = format!(
            "{name}|{}",
            String::from_utf8_lossy(arg).to_ascii_lowercase()
        );
        if let Some(hidden) = all.get(&full) {
            return *hidden;
        }
    }
    all.get(name).copied().unwrap_or_default()
}

/// Whether a command with subcommands was refused before it ran: no such subcommand, or
/// the wrong number of arguments for it. Tantalum's commands find that out as they run.
pub(crate) fn refused_subcommand(name: &str, sub: &[u8], argc: usize) -> bool {
    if !hidden(name, None).container {
        return false;
    }
    let Some(item) = parsed(name, Some(sub)) else {
        return true;
    };
    let arity = item.list()[ARITY].int();
    let argc = argc as i64;
    (arity > 0 && arity != argc) || argc < -arity
}

/// The arity of a command or subcommand (`name|sub`), lowercase.
pub(crate) fn arity_of(full: &str) -> Option<i64> {
    let (name, sub) = match full.split_once('|') {
        Some((name, sub)) => (name, Some(sub.as_bytes())),
        None => (full, None),
    };
    Some(parsed(name, sub)?.list()[ARITY].int())
}

/// Whether command `name` is in ACL category `category` (without its `@`).
pub(crate) fn in_category(name: &str, category: &[u8]) -> bool {
    let Some(entry) = entry(name) else {
        return false;
    };
    let (value, _) = parse(entry.info, 0);
    value.list()[CATEGORIES].list().iter().any(|c| {
        c.text()
            .strip_prefix(b"@")
            .is_some_and(|c| c.eq_ignore_ascii_case(category))
    })
}

/// Why `COMMAND GETKEYS` has no keys to give.
pub(crate) enum NoKeys {
    NoSuchCommand,
    NoKeyArguments,
    WrongCount,
    /// What was asked for has none: `Ok` for a command that may have no keys.
    Invalid {
        may_have_none: bool,
    },
}

/// A key's position among a command's arguments, and its flags.
pub(crate) type KeyAt = (usize, Vec<Vec<u8>>);

/// The arguments of `args` that are keys: `COMMAND GETKEYS`, by the same steps as Redis.
pub(crate) fn keys_of(args: &[&[u8]]) -> Result<Vec<KeyAt>, NoKeys> {
    let name = String::from_utf8_lossy(args[0]).to_ascii_lowercase();
    if !crate::cmd::is_command(args[0]) {
        return Err(NoKeys::NoSuchCommand);
    }
    let item = parsed(&name, args.get(1).copied()).ok_or(NoKeys::NoSuchCommand)?;
    let fields = item.list();
    let specs = fields[KEY_SPECS].list();
    let not_key = |spec: &Value<'_>| {
        spec.field(b"flags")
            .is_some_and(|f| f.list().iter().any(|v| v.text() == b"not_key"))
    };
    if specs.iter().all(not_key) {
        return Err(NoKeys::NoKeyArguments);
    }
    let arity = fields[ARITY].int();
    let argc = args.len() as i64;
    if (arity > 0 && arity != argc) || argc < -arity {
        return Err(NoKeys::WrongCount);
    }
    let invalid = || NoKeys::Invalid {
        may_have_none: has(&item, FLAGS, b"no_mandatory_keys"),
    };
    // Those whose specs can't say: their own rules.
    if name == "sort" {
        return Ok(sort_keys(args));
    }
    if name == "migrate" {
        let (first, count) = migrate_keys(args.len(), |i| args[i]).ok_or_else(invalid)?;
        let flags: Vec<Vec<u8>> = [&b"RW"[..], b"access", b"delete"]
            .map(<[u8]>::to_vec)
            .into();
        return Ok((first..first + count)
            .map(|at| (at, flags.clone()))
            .collect());
    }
    let mut keys = Vec::new();
    for spec in specs {
        let flags: Vec<Vec<u8>> = spec
            .field(b"flags")
            .map(|f| f.list().iter().map(|v| v.text().to_vec()).collect())
            .unwrap_or_default();
        if flags.iter().any(|f| f == b"not_key") {
            continue;
        }
        let Some(first) = begin(spec, args).map_err(|()| invalid())? else {
            continue; // no such keyword: no keys of this spec
        };
        let (first, last, step) = range(spec, args, first).map_err(|()| invalid())?;
        if last >= argc || last < first || first >= argc {
            return Err(invalid());
        }
        let flags = match name.as_str() {
            "set" => set_flags(args),
            "bitfield" => bitfield_flags(args),
            _ => flags,
        };
        let mut i = first;
        while i <= last {
            keys.push((i as usize, flags.clone()));
            i += step.max(1);
        }
    }
    Ok(keys)
}

/// Redis' `migrateGetKeys`: where `MIGRATE`'s keys start and how many there are
/// (the key, or those after `KEYS`); `None` when `KEYS` comes after a key,
/// which the command refuses.
pub(crate) fn migrate_keys<'a>(
    len: usize,
    arg: impl Fn(usize) -> &'a [u8],
) -> Option<(usize, usize)> {
    let mut i = 6;
    while i < len {
        let word = arg(i);
        if word.eq_ignore_ascii_case(b"KEYS") {
            return arg(3).is_empty().then_some((i + 1, len - i - 1));
        }
        // Skip what options take.
        if word.eq_ignore_ascii_case(b"AUTH") {
            i += 1;
        } else if word.eq_ignore_ascii_case(b"AUTH2") {
            i += 2;
        }
        i += 1;
    }
    Some((3, 1))
}

/// `SET`'s key is read too if it has `GET`.
fn set_flags(args: &[&[u8]]) -> Vec<Vec<u8>> {
    let names: &[&[u8]] = if args[3..].iter().any(|a| a.eq_ignore_ascii_case(b"GET")) {
        &[b"RW", b"access", b"update"]
    } else {
        &[b"OW", b"update"]
    };
    names.iter().map(|n| n.to_vec()).collect()
}

/// Redis' `bitfieldGetKeys`: reads, if all it does is `GET`.
fn bitfield_flags(args: &[&[u8]]) -> Vec<Vec<u8>> {
    let mut read_only = true;
    let mut i = 2;
    while i < args.len() {
        let more = args.len() - i - 1;
        let arg = args[i];
        if arg.eq_ignore_ascii_case(b"GET") && more >= 2 {
            i += 3;
        } else if arg.eq_ignore_ascii_case(b"OVERFLOW") && more >= 1 {
            i += 2;
        } else {
            // A write, or what's wrong: safest to say it writes.
            read_only = false;
            break;
        }
    }
    let names: &[&[u8]] = if read_only {
        &[b"RO", b"access"]
    } else {
        &[b"RW", b"access", b"update"]
    };
    names.iter().map(|n| n.to_vec()).collect()
}

/// Redis' `sortGetKeys`: the key, and the last `STORE`'s.
fn sort_keys(args: &[&[u8]]) -> Vec<KeyAt> {
    let owned = |names: &[&[u8]]| names.iter().map(|n| n.to_vec()).collect::<Vec<_>>();
    let mut keys = vec![(1, owned(&[b"RO", b"access"]))];
    let (mut i, mut store) = (2, None);
    while i < args.len() {
        let arg = args[i];
        if arg.eq_ignore_ascii_case(b"LIMIT") {
            i += 2;
        } else if arg.eq_ignore_ascii_case(b"GET") || arg.eq_ignore_ascii_case(b"BY") {
            i += 1;
        } else if arg.eq_ignore_ascii_case(b"STORE") && i + 1 < args.len() {
            store = Some(i + 1);
            i += 1;
        }
        i += 1;
    }
    if let Some(at) = store {
        keys.push((at, owned(&[b"OW", b"update"])));
    }
    keys
}

/// Where a spec's keys start: `Ok(None)` for a keyword that isn't there, `Err`
/// for a spec that can't say.
fn begin(spec: &Value<'_>, args: &[&[u8]]) -> Result<Option<i64>, ()> {
    let search = spec.field(b"begin_search").ok_or(())?;
    let kind = search.field(b"type").ok_or(())?.text();
    let detail = search.field(b"spec").ok_or(())?;
    let argc = args.len() as i64;
    match kind {
        b"index" => Ok(Some(detail.field(b"index").ok_or(())?.int())),
        b"keyword" => {
            let keyword = detail.field(b"keyword").ok_or(())?.text();
            let from = detail.field(b"startfrom").ok_or(())?.int();
            let start = if from > 0 { from } else { argc + from };
            let end = if from > 0 { argc - 1 } else { 1 };
            let mut i = start;
            loop {
                if i >= argc || i < 1 {
                    return Ok(None);
                }
                if args[i as usize].eq_ignore_ascii_case(keyword) {
                    return Ok(Some(i + 1));
                }
                if i == end {
                    return Ok(None);
                }
                i += if start <= end { 1 } else { -1 };
            }
        }
        _ => Err(()),
    }
}

/// A spec's first and last keys, and the step between them.
fn range(spec: &Value<'_>, args: &[&[u8]], first: i64) -> Result<(i64, i64, i64), ()> {
    let find = spec.field(b"find_keys").ok_or(())?;
    let kind = find.field(b"type").ok_or(())?.text();
    let detail = find.field(b"spec").ok_or(())?;
    let argc = args.len() as i64;
    match kind {
        b"range" => {
            let last_key = detail.field(b"lastkey").ok_or(())?.int();
            let step = detail.field(b"keystep").ok_or(())?.int();
            let limit = detail.field(b"limit").ok_or(())?.int();
            let last = if last_key >= 0 {
                first + last_key
            } else if limit == 0 {
                argc + last_key
            } else {
                first + ((argc - first) / limit + last_key)
            };
            Ok((first, last, step))
        }
        b"keynum" => {
            let index = detail.field(b"keynumidx").ok_or(())?.int();
            let first_key = detail.field(b"firstkey").ok_or(())?.int();
            let step = detail.field(b"keystep").ok_or(())?.int();
            let at = first + index;
            if at >= argc {
                return Err(());
            }
            let numkeys = crate::resp::parse_int(args[at as usize])
                .filter(|&n| n >= 0)
                .ok_or(())?;
            let first = first + first_key;
            Ok((first, first + (numkeys - 1) * step, step))
        }
        _ => Err(()),
    }
}

/// A key spec flag's bit, as Redis numbers its `CMD_KEY_*`.
fn key_flag(name: &[u8]) -> u16 {
    const NAMES: [&[u8]; 11] = [
        b"RO",
        b"RW",
        b"OW",
        b"RM",
        b"access",
        b"update",
        b"insert",
        b"delete",
        b"not_key",
        b"incomplete",
        b"variable_flags",
    ];
    NAMES.iter().position(|&n| n == name).map_or(0, |i| 1 << i)
}

/// Where a key spec's keys start.
enum Begin {
    Index(i64),
    Keyword { keyword: &'static [u8], from: i64 },
    Unknown,
}

/// How a key spec's keys go on from there.
enum Find {
    Range { last: i64, step: i64, limit: i64 },
    Keynum { index: i64, first: i64, step: i64 },
    Unknown,
}

struct Spec {
    flags: u16,
    begin: Begin,
    find: Find,
}

/// Commands whose keys their specs can't tell, which Redis finds by their
/// own rules (`getkeys_proc`).
enum Special {
    No,
    /// `SET`: read too with `GET`.
    Set,
    /// `BITFIELD`: read only, if all it does is `GET`.
    Bitfield,
    /// `SORT`: the key, and the last `STORE`'s.
    Sort,
    /// `MIGRATE`: the key, or those after `KEYS`.
    Migrate,
}

/// A command's key specs, compiled for the ACL checks a restricted user's
/// commands get, which run them for every command (`COMMAND GETKEYS`, which
/// is rare, reads the table instead).
pub(crate) struct KeySpecs {
    special: Special,
    specs: Vec<Spec>,
}

const RO_ACCESS: u16 = 1 | 1 << 4;
const RW_ACCESS_UPDATE: u16 = 1 << 1 | 1 << 4 | 1 << 5;
const OW_UPDATE: u16 = 1 << 2 | 1 << 5;
const RW_ACCESS_DELETE: u16 = 1 << 1 | 1 << 4 | 1 << 7;

impl KeySpecs {
    fn compile(name: &str, item: &Value<'static>) -> Option<KeySpecs> {
        let special = match name {
            "set" => Special::Set,
            "bitfield" => Special::Bitfield,
            "sort" => Special::Sort,
            "migrate" => Special::Migrate,
            _ => Special::No,
        };
        let mut specs = Vec::new();
        for spec in item.list()[KEY_SPECS].list() {
            let flags = spec.field(b"flags").map_or(0, |f| {
                f.list().iter().fold(0, |bits, v| bits | key_flag(v.text()))
            });
            if flags & key_flag(b"not_key") != 0 {
                continue;
            }
            let begin = spec.field(b"begin_search").and_then(|search| {
                let detail = search.field(b"spec")?;
                Some(match search.field(b"type")?.text() {
                    b"index" => Begin::Index(detail.field(b"index")?.int()),
                    b"keyword" => Begin::Keyword {
                        keyword: detail.field(b"keyword")?.text(),
                        from: detail.field(b"startfrom")?.int(),
                    },
                    _ => Begin::Unknown,
                })
            });
            let find = spec.field(b"find_keys").and_then(|find| {
                let detail = find.field(b"spec")?;
                Some(match find.field(b"type")?.text() {
                    b"range" => Find::Range {
                        last: detail.field(b"lastkey")?.int(),
                        step: detail.field(b"keystep")?.int(),
                        limit: detail.field(b"limit")?.int(),
                    },
                    b"keynum" => Find::Keynum {
                        index: detail.field(b"keynumidx")?.int(),
                        first: detail.field(b"firstkey")?.int(),
                        step: detail.field(b"keystep")?.int(),
                    },
                    _ => Find::Unknown,
                })
            });
            specs.push(Spec {
                flags,
                begin: begin.unwrap_or(Begin::Unknown),
                find: find.unwrap_or(Find::Unknown),
            });
        }
        (!specs.is_empty() || !matches!(special, Special::No))
            .then_some(KeySpecs { special, specs })
    }

    /// Calls `f` with each key's position and flags, until it says to stop:
    /// those Redis' `getKeysFromCommandWithSpecs` finds, which is none when a
    /// spec can't make sense of the arguments (the command then fails anyway).
    pub(crate) fn each_key(
        &self,
        args: &crate::cmd::Args<'_>,
        mut f: impl FnMut(usize, u16) -> bool,
    ) {
        let argc = args.len() as i64;
        match self.special {
            Special::No => {}
            Special::Set => {
                let get = (3..args.len()).any(|i| args.get(i).eq_ignore_ascii_case(b"GET"));
                f(1, if get { RW_ACCESS_UPDATE } else { OW_UPDATE });
                return;
            }
            Special::Bitfield => {
                let mut read_only = true;
                let mut i = 2;
                while i < args.len() {
                    let more = args.len() - i - 1;
                    let arg = args.get(i);
                    if arg.eq_ignore_ascii_case(b"GET") && more >= 2 {
                        i += 3;
                    } else if arg.eq_ignore_ascii_case(b"OVERFLOW") && more >= 1 {
                        i += 2;
                    } else {
                        read_only = false;
                        break;
                    }
                }
                f(
                    1,
                    if read_only {
                        RO_ACCESS
                    } else {
                        RW_ACCESS_UPDATE
                    },
                );
                return;
            }
            Special::Sort => {
                let (mut i, mut store) = (2, None);
                while i < args.len() {
                    let arg = args.get(i);
                    if arg.eq_ignore_ascii_case(b"LIMIT") {
                        i += 2;
                    } else if arg.eq_ignore_ascii_case(b"GET") || arg.eq_ignore_ascii_case(b"BY") {
                        i += 1;
                    } else if arg.eq_ignore_ascii_case(b"STORE") && i + 1 < args.len() {
                        store = Some(i + 1);
                        i += 1;
                    }
                    i += 1;
                }
                if f(1, RO_ACCESS)
                    && let Some(at) = store
                {
                    f(at, OW_UPDATE);
                }
                return;
            }
            Special::Migrate => {
                if let Some((first, count)) = migrate_keys(args.len(), |i| args.get(i)) {
                    for at in first..first + count {
                        if !f(at, RW_ACCESS_DELETE) {
                            break;
                        }
                    }
                }
                return;
            }
        }
        // Every spec first, so that one that makes no sense means no keys.
        let mut ranges = [(0i64, 0i64, 0i64, 0u16); 4];
        let mut found = Vec::new();
        let mut count = 0;
        for spec in &self.specs {
            let first = match spec.begin {
                Begin::Index(at) => at,
                Begin::Keyword { keyword, from } => {
                    let start = if from > 0 { from } else { argc + from };
                    let end = if from > 0 { argc - 1 } else { 1 };
                    let mut i = start;
                    let mut first = 0;
                    while i != end {
                        if i >= argc || i < 1 {
                            break;
                        }
                        if args.get(i as usize).eq_ignore_ascii_case(keyword) {
                            first = i + 1;
                            break;
                        }
                        i += if start <= end { 1 } else { -1 };
                    }
                    if first == 0 {
                        continue;
                    }
                    first
                }
                Begin::Unknown => return,
            };
            let (first, last, step) = match spec.find {
                Find::Range { last, step, limit } => {
                    let last = if last >= 0 {
                        first + last
                    } else if limit == 0 {
                        argc + last
                    } else {
                        first + ((argc - first) / limit + last)
                    };
                    (first, last, step)
                }
                Find::Keynum {
                    index,
                    first: first_key,
                    step,
                } => {
                    if index >= argc || first + index >= argc {
                        return;
                    }
                    let Some(numkeys) = crate::resp::parse_int(args.get((first + index) as usize))
                        .filter(|&n| n >= 0)
                    else {
                        return;
                    };
                    let first = first + first_key;
                    // A count past the arguments (even past an i64) means none.
                    let Some(last) = first.checked_add(numkeys - 1) else {
                        return;
                    };
                    (first, last, step)
                }
                Find::Unknown => return,
            };
            if last >= argc || last < first || first >= argc {
                return;
            }
            let range = (first, last, step.max(1), spec.flags);
            if count < ranges.len() {
                ranges[count] = range;
            } else {
                found.push(range);
            }
            count += 1;
        }
        for &(first, last, step, flags) in ranges[..count.min(ranges.len())].iter().chain(&found) {
            let mut i = first;
            while i <= last {
                if !f(i as usize, flags) {
                    return;
                }
                i += step;
            }
        }
    }
}

/// The compiled key specs of a command or subcommand (`name|sub`), lowercase;
/// `None` for one without keys.
pub(crate) fn key_specs(full: &str) -> Option<&'static KeySpecs> {
    static ALL: OnceLock<HashMap<String, Option<KeySpecs>>> = OnceLock::new();
    let all = ALL.get_or_init(|| {
        let mut all = HashMap::new();
        for (name, entry) in entries() {
            let (value, _) = parse(entry.info, 0);
            for sub in value.list()[SUBCOMMANDS].list() {
                let full = String::from_utf8_lossy(sub.list()[0].text()).into_owned();
                let specs = KeySpecs::compile(&full, sub);
                all.insert(full, specs);
            }
            all.insert((*name).to_string(), KeySpecs::compile(name, &value));
        }
        all
    });
    all.get(full)?.as_ref()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The compiled specs find the keys `COMMAND GETKEYSANDFLAGS` finds.
    #[test]
    fn compiled_specs_find_the_same_keys() {
        let cases: &[&[&str]] = &[
            &["get", "k"],
            &["set", "k", "v"],
            &["set", "k", "v", "GET"],
            &["mset", "a", "1", "b", "2", "c", "3"],
            &["del", "a", "b", "c"],
            &["eval", "return 1", "2", "a", "b", "x"],
            &["zunionstore", "d", "2", "a", "b", "WEIGHTS", "1", "2"],
            &["xread", "COUNT", "2", "STREAMS", "a", "b", "0", "0"],
            &["xreadgroup", "GROUP", "g", "c", "STREAMS", "a", "0"],
            &["georadius", "k", "1", "2", "3", "m", "STORE", "d"],
            &["sort", "k", "BY", "w_*", "GET", "#", "STORE", "d"],
            &["sort_ro", "k"],
            &["bitfield", "k", "GET", "u8", "0"],
            &["bitfield", "k", "SET", "u8", "0", "1"],
            &["lmove", "a", "b", "LEFT", "RIGHT"],
            &["blmpop", "0", "2", "a", "b", "LEFT"],
            &["object", "encoding", "k"],
            &["xinfo", "stream", "k"],
            &["rename", "a", "b"],
            &["copy", "a", "b"],
            &["smove", "a", "b", "m"],
            &[
                "geosearchstore",
                "d",
                "s",
                "FROMLONLAT",
                "1",
                "2",
                "BYRADIUS",
                "1",
                "m",
            ],
            &["pfmerge", "d", "a", "b"],
            &["memory", "usage", "k"],
            &["migrate", "h", "1", "k", "0", "100"],
            &["migrate", "h", "1", "", "0", "100", "KEYS", "a", "b"],
            &[
                "migrate", "h", "1", "", "0", "100", "AUTH", "KEYS", "KEYS", "a",
            ],
            &[
                "migrate", "h", "1", "", "0", "100", "AUTH2", "KEYS", "KEYS", "KEYS", "a",
            ],
            &["migrate", "h", "1", "k", "0", "100", "COPY", "REPLACE"],
            &["dump", "k"],
            &["restore", "k", "0", "x", "REPLACE"],
        ];
        let names = |bits: u16| -> Vec<Vec<u8>> {
            [
                b"RO".as_ref(),
                b"RW",
                b"OW",
                b"RM",
                b"access",
                b"update",
                b"insert",
                b"delete",
            ]
            .iter()
            .enumerate()
            .filter(|(i, _)| bits & (1 << i) != 0)
            .map(|(_, n)| n.to_vec())
            .collect()
        };
        for case in cases {
            let bytes: Vec<&[u8]> = case.iter().map(|s| s.as_bytes()).collect();
            let mut buf = Vec::new();
            let mut ranges = Vec::new();
            for arg in &bytes {
                let start = buf.len();
                buf.extend_from_slice(arg);
                ranges.push(start..buf.len());
            }
            let args = crate::cmd::Args::new(&buf, &ranges);
            let full = if hidden(case[0], None).container {
                format!("{}|{}", case[0], case[1])
            } else {
                case[0].to_string()
            };
            let mut compiled = Vec::new();
            if let Some(specs) = key_specs(&full) {
                specs.each_key(&args, |at, flags| {
                    compiled.push((at, names(flags)));
                    true
                });
            }
            let expected = keys_of(&bytes).unwrap_or_default();
            assert_eq!(compiled, expected, "{case:?}");
        }
    }

    fn has_map(value: &Value<'_>) -> bool {
        match value {
            Value::Map(_) => true,
            Value::List(items) => items.iter().any(has_map),
            _ => false,
        }
    }

    #[test]
    fn every_entry_reads_and_goes_to_resp2() {
        assert!(entries().len() > 200);
        for entry in entries().values() {
            let (value, end) = parse(entry.info, 0);
            assert_eq!(end, entry.info.len());
            assert_eq!(value.list().len(), 10);
            let (_, end) = parse(entry.docs, 0);
            assert_eq!(end, entry.docs.len());
            for value in [entry.info, entry.docs] {
                let mut out = Vec::new();
                write(value, false, &mut out);
                let (flat, end) = parse(&out, 0);
                assert_eq!(end, out.len());
                assert!(!has_map(&flat));
            }
        }
    }

    #[test]
    fn admin_commands_are_hidden_from_monitor_by_subcommand() {
        assert!(hidden("config", Some(b"GET")).from_monitor);
        assert!(hidden("client", Some(b"list")).from_monitor);
        assert!(!hidden("client", Some(b"ID")).from_monitor);
        assert!(!hidden("get", None).from_monitor);
        assert!(hidden("eval", None).from_monitor);
        assert!(hidden("exec", None).from_slowlog);
    }
}
