//! The settings `CONFIG` knows: Redis 7.2.4's, with its defaults
//! (`config_defaults.txt`, by `tools/command-table.py`). Most mean nothing here:
//! they're remembered, as clients set and read them, and do nothing. Those that
//! act are in `cmd/admin.rs`.

use std::collections::HashMap;
use std::sync::OnceLock;

use parking_lot::Mutex;

static DEFAULTS: &str = include_str!("config_defaults.txt");

/// How each setting's value reads: name, kind and its bounds or choices, tab between.
static KINDS: &str = include_str!("config_kinds.txt");

/// How a setting's value reads.
pub(crate) enum Kind {
    Immutable,
    /// Refused unless the server was started with `enable-protected-configs`.
    Protected,
    Bool,
    Int(i64, i64),
    Memory(u64, u64),
    /// A size, or a percentage of `maxmemory`.
    MemoryOrPercent,
    OneOf(Vec<&'static str>),
    Text,
}

fn defaults() -> &'static [(&'static str, String)] {
    static TABLE: OnceLock<Vec<(&'static str, String)>> = OnceLock::new();
    TABLE.get_or_init(|| {
        DEFAULTS
            .lines()
            .filter_map(|line| line.split_once('\t'))
            .map(|(name, value)| (name, value.replace("\\n", "\n").replace("\\\\", "\\")))
            .collect()
    })
}

/// The setting called `name` (lowercase), and how its value reads.
pub(crate) fn find(name: &str) -> Option<(&'static str, Kind)> {
    let name = defaults()
        .iter()
        .map(|(known, _)| *known)
        .find(|known| *known == name)?;
    let kind = kind_of(name);
    Some((name, kind))
}

fn kind_of(name: &str) -> Kind {
    let Some(line) = KINDS
        .lines()
        .find(|line| line.split('\t').next() == Some(name))
    else {
        return Kind::Text;
    };
    let mut parts = line.split('\t').skip(1);
    let kind = parts.next();
    let (first, second) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
    match kind {
        Some("immutable") => Kind::Immutable,
        Some("protected") => Kind::Protected,
        Some("bool") => Kind::Bool,
        Some("int") => Kind::Int(
            first.parse().unwrap_or(i64::MIN),
            second.parse().unwrap_or(i64::MAX),
        ),
        Some("memory") => Kind::Memory(
            first.parse().unwrap_or(0),
            second.parse().unwrap_or(u64::MAX),
        ),
        Some("percent") => Kind::MemoryOrPercent,
        Some("oneof") => Kind::OneOf(first.split(", ").collect()),
        _ => Kind::Text,
    }
}

/// A setting's default.
pub(crate) fn default_of(name: &str) -> Option<&'static str> {
    defaults()
        .iter()
        .find(|(known, _)| *known == name)
        .map(|(_, default)| default.as_str())
}

/// The other name a setting goes by, as Redis' old names and new do.
fn alias(name: &str) -> Option<String> {
    let swap = |from: &str, to: &str| {
        let other = name.replace(from, to);
        (other != name && defaults().iter().any(|(known, _)| *known == other)).then_some(other)
    };
    swap("replica", "slave")
        .or_else(|| swap("slave", "replica"))
        .or_else(|| swap("listpack", "ziplist"))
        .or_else(|| swap("ziplist", "listpack"))
}

/// A size as Redis reads it: `1k` is 1000, `1kb` 1024, and so on.
pub(crate) fn parse_memory(value: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(value).ok()?.to_ascii_lowercase();
    let digits = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    if digits >= 128 {
        return None;
    }
    let multiplier: u64 = match &text[digits..] {
        "" | "b" => 1,
        "k" => 1000,
        "kb" => 1 << 10,
        "m" => 1000 * 1000,
        "mb" => 1 << 20,
        "g" => 1000 * 1000 * 1000,
        "gb" => 1 << 30,
        _ => return None,
    };
    // No digits read as 0, and past what fits it stops at the most there is, and wraps,
    // all as Redis' does.
    let number = match digits {
        0 => 0,
        _ => text[..digits].parse::<u64>().unwrap_or(u64::MAX),
    };
    Some(number.wrapping_mul(multiplier))
}

/// What's been set since the start, by `CONFIG SET` or by the server's options.
#[derive(Default)]
pub struct ConfigTable {
    values: Mutex<HashMap<String, String>>,
}

impl ConfigTable {
    /// Remembers `value` for `name`, and for the other name it goes by.
    pub fn set(&self, name: &str, value: &str) {
        let mut values = self.values.lock();
        if let Some(other) = alias(name) {
            values.insert(other, value.to_string());
        }
        values.insert(name.to_string(), value.to_string());
    }

    /// Every setting and its value, by name.
    pub(crate) fn all(&self) -> Vec<(&'static str, String)> {
        let values = self.values.lock();
        defaults()
            .iter()
            .map(|(name, default)| (*name, values.get(*name).unwrap_or(default).clone()))
            .collect()
    }

    /// A setting's value: as set, or its default.
    pub(crate) fn get(&self, name: &str) -> Option<String> {
        self.values.lock().get(name).cloned().or_else(|| {
            defaults()
                .iter()
                .find(|(known, _)| *known == name)
                .map(|(_, value)| value.clone())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_read_as_redis_reads_them() {
        assert_eq!(parse_memory(b"100"), Some(100));
        assert_eq!(parse_memory(b"1k"), Some(1000));
        assert_eq!(parse_memory(b"1KB"), Some(1024));
        assert_eq!(parse_memory(b"2gb"), Some(2 << 30));
        for bad in [&b"x"[..], b"1x", b"-1"] {
            assert_eq!(parse_memory(bad), None);
        }
        // What Redis reads as nothing, and as too much.
        assert_eq!(parse_memory(b""), Some(0));
        assert_eq!(parse_memory(b"mb"), Some(0));
        assert_eq!(parse_memory(b"99999999999999999999999"), Some(u64::MAX));
    }

    #[test]
    fn the_old_and_new_names_go_together() {
        let table = ConfigTable::default();
        table.set("hash-max-listpack-entries", "7");
        assert_eq!(table.get("hash-max-ziplist-entries").as_deref(), Some("7"));
        assert!(matches!(find("databases"), Some((_, Kind::Immutable))));
        assert!(matches!(find("hz"), Some((_, Kind::Int(0, _)))));
        assert!(matches!(find("appendonly"), Some((_, Kind::Bool))));
        assert!(find("nope").is_none());
    }
}
