//! `cjson.encode` of a value `cmsgpack.unpack` made, byte for byte as Redis'
//! Lua gives it. The one hard part is an object's key order: cjson walks the
//! Lua table, so keys come in the order Lua 5.1 placed them in the table's
//! hash part. That follows from the keys' hashes and the order they were set
//! in, nil values included (setting a new key to nil still takes a node,
//! until the table next grows), so [`Table`] places them as `ltable.c` does.

use super::Msg;
use crate::script::lua_tostring;

/// `cjson.encode(value)`; `None` where cjson would fail (NaN or infinite
/// numbers, excessively sparse arrays) or for tables with keys other than
/// strings, which this doesn't lay out: Lua's to do.
pub(super) fn encode(value: &Msg) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    append(value, &mut out)?;
    Some(out)
}

fn append(value: &Msg, out: &mut Vec<u8>) -> Option<()> {
    match value {
        Msg::Nil => out.extend_from_slice(b"null"),
        Msg::Bool(true) => out.extend_from_slice(b"true"),
        Msg::Bool(false) => out.extend_from_slice(b"false"),
        Msg::Num(n) => {
            if !n.is_finite() {
                return None; // "must not be NaN or Inf"
            }
            out.extend_from_slice(&lua_tostring(*n));
        }
        Msg::Str(s) => string(s, out),
        Msg::Array(items) => array(items, out)?,
        Msg::Map(entries) => object(entries, out)?,
    }
    Some(())
}

/// A msgpack array is a table with keys 1 to n (nils leave theirs out).
/// cjson writes it as an array up to its last non-nil element, nulls in
/// the gaps, unless it's empty (then `{}`) or excessively sparse (an error).
fn array(items: &[Msg], out: &mut Vec<u8>) -> Option<()> {
    let present = items.iter().filter(|m| !matches!(m, Msg::Nil)).count();
    let max = items
        .iter()
        .rposition(|m| !matches!(m, Msg::Nil))
        .map_or(0, |i| i + 1);
    if max == 0 {
        out.extend_from_slice(b"{}");
        return Some(());
    }
    // cjson's defaults: sparse ratio 2, safe 10, no converting.
    if max > present * 2 && max > 10 {
        return None;
    }
    out.push(b'[');
    for (i, item) in items[..max].iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        append(item, out)?;
    }
    out.push(b']');
    Some(())
}

fn object(entries: &[(Msg, Msg)], out: &mut Vec<u8>) -> Option<()> {
    let mut table = Table::default();
    for (key, value) in entries {
        // Numbers would go to the array part, nil and NaN keys are errors:
        // all rare in BullMQ's options, so Lua's.
        let Msg::Str(key) = key else {
            return None;
        };
        table.set(key, (!matches!(value, Msg::Nil)).then_some(value));
    }
    out.push(b'{');
    let mut first = true;
    for node in &table.nodes {
        if let (Some(key), Some(value)) = (node.key, node.value) {
            if !first {
                out.push(b',');
            }
            first = false;
            string(key, out);
            out.push(b':');
            append(value, out)?;
        }
    }
    out.push(b'}');
    Some(())
}

/// A string as cjson writes it: control characters, `"`, `\`, `/` and DEL
/// escaped, everything else (UTF-8 or not) as it is.
fn string(s: &[u8], out: &mut Vec<u8>) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push(b'"');
    for &c in s {
        match c {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'/' => out.extend_from_slice(b"\\/"),
            0x08 => out.extend_from_slice(b"\\b"),
            b'\t' => out.extend_from_slice(b"\\t"),
            b'\n' => out.extend_from_slice(b"\\n"),
            0x0c => out.extend_from_slice(b"\\f"),
            b'\r' => out.extend_from_slice(b"\\r"),
            0..=0x1f | 0x7f => {
                out.extend_from_slice(b"\\u00");
                out.push(HEX[usize::from(c >> 4)]);
                out.push(HEX[usize::from(c & 0xf)]);
            }
            _ => out.push(c),
        }
    }
    out.push(b'"');
}

/// The string hash of Redis' Lua (`luaS_newlstr`, which Redis changed to
/// hash every byte): seeded with the length, last byte first.
fn hash(s: &[u8]) -> u32 {
    let mut h = s.len() as u32;
    for &c in s.iter().rev() {
        h ^= (h << 5).wrapping_add(h >> 2).wrapping_add(u32::from(c));
    }
    h
}

#[derive(Clone, Copy, Default)]
struct Node<'m> {
    key: Option<&'m [u8]>,
    hash: u32,
    /// `None` is nil: a key set to nil keeps its node until a rehash.
    value: Option<&'m Msg>,
    next: Option<usize>,
}

/// The hash part of a Lua 5.1 table made by `lua_newtable` and filled by
/// `lua_settable` with string keys (`ltable.c`: `luaH_set`, `newkey`,
/// `getfreepos`, `rehash`). No nodes is Lua's shared dummy node.
#[derive(Default)]
struct Table<'m> {
    nodes: Vec<Node<'m>>,
    /// Free positions are looked for below this, downwards.
    last_free: usize,
}

impl<'m> Table<'m> {
    /// `t[key] = value`.
    fn set(&mut self, key: &'m [u8], value: Option<&'m Msg>) {
        let hash = hash(key);
        let i = self.slot(key, hash);
        self.nodes[i].value = value;
    }

    /// `luaH_set`: the key's node, made if it has none.
    fn slot(&mut self, key: &'m [u8], hash: u32) -> usize {
        match self.find(key, hash) {
            Some(i) => i,
            None => self.new_key(key, hash),
        }
    }

    fn main_position(&self, hash: u32) -> usize {
        hash as usize & (self.nodes.len() - 1)
    }

    /// `luaH_getstr`: the node holding `key`, nil value or not.
    fn find(&self, key: &[u8], hash: u32) -> Option<usize> {
        if self.nodes.is_empty() {
            return None;
        }
        let mut at = Some(self.main_position(hash));
        while let Some(i) = at {
            if self.nodes[i].key == Some(key) {
                return Some(i);
            }
            at = self.nodes[i].next;
        }
        None
    }

    /// `newkey`: places a key that isn't in the table.
    fn new_key(&mut self, key: &'m [u8], hash: u32) -> usize {
        if self.nodes.is_empty() {
            self.rehash();
            return self.slot(key, hash);
        }
        let mut mp = self.main_position(hash);
        // Taken unless its value is nil (a nil-valued node is reused).
        if self.nodes[mp].value.is_some() {
            let Some(free) = self.free_position() else {
                self.rehash();
                return self.slot(key, hash);
            };
            let other = self.main_position(self.nodes[mp].hash);
            if other != mp {
                // The colliding node isn't in its main position: move it to
                // the free one, and take its place.
                let mut previous = other;
                while self.nodes[previous].next != Some(mp) {
                    previous = self.nodes[previous]
                        .next
                        .expect("a chain reaches its nodes");
                }
                self.nodes[previous].next = Some(free);
                self.nodes[free] = self.nodes[mp];
                self.nodes[mp].next = None;
                self.nodes[mp].value = None;
            } else {
                // It is: the new key goes to the free position, chained.
                self.nodes[free].next = self.nodes[mp].next;
                self.nodes[mp].next = Some(free);
                mp = free;
            }
        }
        self.nodes[mp].key = Some(key);
        self.nodes[mp].hash = hash;
        mp
    }

    /// `getfreepos`: the next node down without a key.
    fn free_position(&mut self) -> Option<usize> {
        while self.last_free > 0 {
            self.last_free -= 1;
            if self.nodes[self.last_free].key.is_none() {
                return Some(self.last_free);
            }
        }
        None
    }

    /// `rehash` for a table of string keys: room for those with values and
    /// one more, rounded up to a power of two, then the old nodes put back
    /// from the last to the first.
    fn rehash(&mut self) {
        let used = self.nodes.iter().filter(|n| n.value.is_some()).count();
        let size = (used + 1).next_power_of_two();
        let old = std::mem::replace(&mut self.nodes, vec![Node::default(); size]);
        self.last_free = size;
        for node in old.iter().rev() {
            if let (Some(key), Some(value)) = (node.key, node.value) {
                let i = self.slot(key, node.hash);
                self.nodes[i].value = Some(value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strings_and_numbers_are_written_as_cjson_does() {
        let mut out = Vec::new();
        string(b"a\"b\\c/d\x01\n\x7f\xc3\xa9", &mut out);
        assert_eq!(out, b"\"a\\\"b\\\\c\\/d\\u0001\\n\\u007f\xc3\xa9\"");
        for (n, text) in [
            (3.0, "3"),
            (-0.5, "-0.5"),
            (1e15, "1e+15"),
            (0.1, "0.1"),
            (99999999999999.0, "99999999999999"),
            (1e14, "1e+14"),
        ] {
            assert_eq!(encode(&Msg::Num(n)).unwrap(), text.as_bytes());
        }
        assert_eq!(encode(&Msg::Num(f64::NAN)), None);
    }

    #[test]
    fn arrays_end_at_their_last_value() {
        let n = |x: f64| Msg::Num(x);
        let enc = |items: Vec<Msg>| encode(&Msg::Array(items));
        assert_eq!(enc(vec![]).unwrap(), b"{}");
        assert_eq!(enc(vec![Msg::Nil]).unwrap(), b"{}");
        assert_eq!(
            enc(vec![n(1.0), Msg::Nil, n(3.0), Msg::Nil]).unwrap(),
            b"[1,null,3]"
        );
        let mut sparse = vec![Msg::Nil; 11];
        sparse.push(n(1.0));
        assert_eq!(enc(sparse), None);
    }
}
