//! Sorted sets. A small one is packed in its entry, in order:
//!
//! ```text
//! count, then each entry: its score, and its member's length (a varint)
//! and bytes, by score and then member
//! ```
//!
//! A whole score (below 2^52 either way, not -0) is a varint of its zigzag
//! times two, a byte or two for the counts and ranks scores often are;
//! any other is a 1 and then its 8 bytes (an f64, little-endian).
//!
//! up to [`MAX_PACKED`] members of up to [`MAX_MEMBER_LEN`] bytes (Redis'
//! listpack limits). A bigger one is a [`BigZSet`]: its members kept as a
//! big set keeps them (a buffer, their offsets by position, a table of
//! positions), a score per position, and the order as chunks of positions,
//! with a Fenwick tree of chunk sizes so ranks are O(log n). A write to a
//! packed one decodes it into a `BigZSet`, changes that, and packs it
//! again if it fits.
//!
//! Scores reply as Redis 7.2 prints doubles (shortest digits, laid out as
//! JavaScript's `Number.prototype.toString`), and parse as its `strtod`.

use std::cmp::Ordering;

use crate::big::Paged;
use crate::set::BigSet;

pub const MAX_PACKED: usize = 128;
pub const MAX_MEMBER_LEN: usize = 64;
/// Chunks of the order split past this many positions.
const CHUNK_MAX: usize = 256;

fn put_varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        out.push(n as u8 | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

fn take_varint(bytes: &mut &[u8]) -> Option<u64> {
    let mut n = 0u64;
    for shift in (0..64).step_by(7) {
        let (&byte, rest) = bytes.split_first()?;
        *bytes = rest;
        n |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return Some(n);
        }
    }
    None
}

fn put_score(out: &mut Vec<u8>, score: f64) {
    const WHOLE: f64 = (1u64 << 52) as f64;
    if score.fract() == 0.0 && score.abs() < WHOLE && !(score == 0.0 && score.is_sign_negative()) {
        let n = score as i64;
        put_varint(out, (((n << 1) ^ (n >> 63)) as u64) << 1);
    } else {
        out.push(1);
        out.extend_from_slice(&score.to_le_bytes());
    }
}

fn take_score(bytes: &mut &[u8]) -> Option<f64> {
    let v = take_varint(bytes)?;
    if v & 1 == 0 {
        let z = v >> 1;
        return Some(((z >> 1) as i64 ^ -((z & 1) as i64)) as f64);
    }
    if v != 1 || bytes.len() < 8 {
        return None;
    }
    let (score, rest) = bytes.split_at(8);
    *bytes = rest;
    Some(f64::from_le_bytes(score.try_into().expect("8 bytes")))
}

/// Redis' order: by score, then by member bytes. Scores are never NaN.
pub fn cmp(a: (f64, &[u8]), b: (f64, &[u8])) -> Ordering {
    a.0.partial_cmp(&b.0)
        .expect("no NaN scores")
        .then_with(|| a.1.cmp(b.1))
}

/// Writes `x` as Redis 7.2 replies a double: shortest round-trip digits,
/// laid out as JavaScript's `Number.prototype.toString` does (plain from
/// 1e-6 up to 1e21, exponent form outside), `inf`, `-inf`, `nan`; -0 is 0.
pub fn format_score(x: f64, out: &mut Vec<u8>) {
    if x.is_nan() {
        return out.extend_from_slice(b"nan");
    }
    if x.is_infinite() {
        return out.extend_from_slice(if x < 0.0 { b"-inf" } else { b"inf" });
    }
    if x == 0.0 {
        return out.push(b'0');
    }
    // Whole numbers below 2^53 (counts, ranks, timestamps) print as
    // integers, without the general path.
    if x.fract() == 0.0 && x.abs() < 9007199254740992.0 {
        return out.extend_from_slice(itoa::Buffer::new().format(x as i64).as_bytes());
    }
    if x < 0.0 {
        out.push(b'-');
    }
    // Shortest digits and exponent, from Rust's own shortest formatting,
    // written to the stack.
    let mut sci = Stack::default();
    std::fmt::Write::write_fmt(&mut sci, format_args!("{:e}", x.abs())).expect("fits");
    let sci = &sci.0[..sci.1];
    let e = sci.iter().position(|&b| b == b'e').expect("an exponent");
    let (mantissa, exp) = (&sci[..e], &sci[e + 1..]);
    let mut digits = [0u8; 24];
    let mut k = 0;
    for &b in mantissa.iter().filter(|&&b| b != b'.') {
        digits[k] = b;
        k += 1;
    }
    let digits = &digits[..k];
    let k = k as i32;
    // The decimal point goes after `n` digits.
    let exp: i32 = std::str::from_utf8(exp)
        .expect("ASCII")
        .parse()
        .expect("an exponent");
    let n = exp + 1;
    if k <= n && n <= 21 {
        out.extend_from_slice(digits);
        out.extend(std::iter::repeat_n(b'0', (n - k) as usize));
    } else if 0 < n && n <= 21 {
        out.extend_from_slice(&digits[..n as usize]);
        out.push(b'.');
        out.extend_from_slice(&digits[n as usize..]);
    } else if -6 < n && n <= 0 {
        out.extend_from_slice(b"0.");
        out.extend(std::iter::repeat_n(b'0', (-n) as usize));
        out.extend_from_slice(digits);
    } else {
        out.push(digits[0]);
        if k > 1 {
            out.push(b'.');
            out.extend_from_slice(&digits[1..]);
        }
        out.push(b'e');
        out.push(if n - 1 < 0 { b'-' } else { b'+' });
        out.extend_from_slice((n - 1).abs().to_string().as_bytes());
    }
}

/// A small buffer on the stack to format into.
struct Stack([u8; 40], usize);

impl Default for Stack {
    fn default() -> Self {
        Stack([0; 40], 0)
    }
}

impl std::fmt::Write for Stack {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        let end = self.1 + s.len();
        self.0
            .get_mut(self.1..end)
            .ok_or(std::fmt::Error)?
            .copy_from_slice(s.as_bytes());
        self.1 = end;
        Ok(())
    }
}

pub fn score_string(x: f64) -> Vec<u8> {
    let mut out = Vec::new();
    format_score(x, &mut out);
    out
}

fn is_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// What C's `strtod` reads from the start of `s`: the number, the bytes it
/// used (0 if none), and whether it was out of range (overflowed, or
/// underflowed to zero).
fn strtod(s: &[u8]) -> (f64, usize, bool) {
    let mut i = 0;
    while i < s.len() && is_space(s[i]) {
        i += 1;
    }
    let start = i;
    let negative = s.get(i) == Some(&b'-');
    if matches!(s.get(i), Some(b'-' | b'+')) {
        i += 1;
    }
    let sign = if negative { -1.0 } else { 1.0 };
    let rest = &s[i..];
    let starts =
        |word: &[u8]| rest.len() >= word.len() && rest[..word.len()].eq_ignore_ascii_case(word);
    if starts(b"infinity") {
        return (sign * f64::INFINITY, i + 8, false);
    }
    if starts(b"inf") {
        return (sign * f64::INFINITY, i + 3, false);
    }
    if starts(b"nan") {
        let mut end = i + 3;
        if s.get(end) == Some(&b'(')
            && let Some(close) = s[end..].iter().position(|&b| b == b')')
            && s[end + 1..end + close]
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || *b == b'_')
        {
            end += close + 1;
        }
        return (f64::NAN, end, false);
    }
    if (starts(b"0x"))
        && rest.get(2).is_some_and(|b| {
            b.is_ascii_hexdigit() || (*b == b'.' && rest.get(3).is_some_and(u8::is_ascii_hexdigit))
        })
    {
        return hex(s, i + 2, sign);
    }
    // Decimal: digits, an optional fraction, an optional exponent.
    let mut j = i;
    let mut digits = 0;
    while j < s.len() && s[j].is_ascii_digit() {
        j += 1;
        digits += 1;
    }
    if s.get(j) == Some(&b'.') {
        j += 1;
        while j < s.len() && s[j].is_ascii_digit() {
            j += 1;
            digits += 1;
        }
    }
    if digits == 0 {
        return (0.0, 0, false);
    }
    if matches!(s.get(j), Some(b'e' | b'E')) {
        let mut k = j + 1;
        if matches!(s.get(k), Some(b'-' | b'+')) {
            k += 1;
        }
        if s.get(k).is_some_and(u8::is_ascii_digit) {
            while k < s.len() && s[k].is_ascii_digit() {
                k += 1;
            }
            j = k;
        }
    }
    let text = std::str::from_utf8(&s[start..j]).expect("ASCII");
    let value: f64 = text.parse().unwrap_or(0.0);
    let nonzero = s[i..j]
        .iter()
        .take_while(|b| !matches!(b, b'e' | b'E'))
        .any(|&b| (b'1'..=b'9').contains(&b));
    let out_of_range = value.is_infinite() || (value == 0.0 && nonzero);
    (value, j, out_of_range)
}

/// A hexadecimal float after its `0x`, as `strtod` reads one.
fn hex(s: &[u8], mut i: usize, sign: f64) -> (f64, usize, bool) {
    let mut mantissa = 0u128;
    let mut exp = 0i64;
    let mut any = false;
    while i < s.len() && s[i].is_ascii_hexdigit() {
        if mantissa < (1 << 120) {
            mantissa = mantissa * 16 + u128::from(hex_value(s[i]));
        } else {
            exp += 4;
        }
        i += 1;
        any = true;
    }
    if s.get(i) == Some(&b'.') {
        i += 1;
        while i < s.len() && s[i].is_ascii_hexdigit() {
            if mantissa < (1 << 120) {
                mantissa = mantissa * 16 + u128::from(hex_value(s[i]));
                exp -= 4;
            }
            i += 1;
            any = true;
        }
    }
    debug_assert!(any);
    if matches!(s.get(i), Some(b'p' | b'P')) {
        let mut k = i + 1;
        let negative = s.get(k) == Some(&b'-');
        if matches!(s.get(k), Some(b'-' | b'+')) {
            k += 1;
        }
        if s.get(k).is_some_and(u8::is_ascii_digit) {
            let mut e = 0i64;
            while k < s.len() && s[k].is_ascii_digit() {
                e = (e * 10 + i64::from(s[k] - b'0')).min(1 << 20);
                k += 1;
            }
            exp += if negative { -e } else { e };
            i = k;
        }
    }
    let mut value = mantissa as f64;
    // Scale in steps that can't overflow or underflow on the way.
    let mut e = exp;
    while e > 0 {
        let step = e.min(1000);
        value *= 2f64.powi(step as i32);
        e -= step;
    }
    while e < 0 {
        let step = (-e).min(1000);
        value /= 2f64.powi(step as i32);
        e += step;
    }
    let out_of_range = value.is_infinite() || (value == 0.0 && mantissa != 0);
    (sign * value, i, out_of_range)
}

fn hex_value(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        _ => b - b'A' + 10,
    }
}

/// A score argument, as Redis reads one (`getDoubleFromObject`): all of it
/// a number, no space before it, not NaN, not out of range.
pub fn parse_score(s: &[u8]) -> Option<f64> {
    if s.first().is_some_and(|&b| is_space(b)) {
        return None;
    }
    let (value, used, out_of_range) = strtod(s);
    (used == s.len() && used > 0 && !value.is_nan() && !out_of_range).then_some(value)
}

/// A blocking command's timeout in seconds, as Redis reads one (with
/// `strtold`): all of it a number, no space before it, not NaN. Also
/// whether it underflowed to zero, though a long double wouldn't have.
pub(crate) fn parse_timeout_seconds(s: &[u8]) -> Option<(f64, bool)> {
    if s.first().is_some_and(|&b| is_space(b)) {
        return None;
    }
    let (value, used, out_of_range) = strtod(s);
    (used == s.len() && used > 0 && !value.is_nan())
        .then_some((value, out_of_range && value == 0.0))
}

/// One end of a score range: `(` for exclusive. Read as Redis'
/// `zslParseRange` does: leading space and out-of-range values are fine
/// (an empty one is 0), NaN isn't.
pub fn parse_score_bound(s: &[u8]) -> Option<Bound> {
    let (exclusive, number) = match s.split_first() {
        Some((b'(', rest)) => (true, rest),
        _ => (false, s),
    };
    let (value, used, _) = strtod(number);
    (used == number.len() && !value.is_nan()).then_some(Bound { value, exclusive })
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Bound {
    pub value: f64,
    pub exclusive: bool,
}

/// A score range, `min` to `max`.
#[derive(Clone, Copy, Debug)]
pub struct ScoreRange {
    pub min: Bound,
    pub max: Bound,
}

impl ScoreRange {
    /// Whether no score can be in it.
    pub fn is_empty(&self) -> bool {
        self.min.value > self.max.value
            || (self.min.value == self.max.value && (self.min.exclusive || self.max.exclusive))
    }

    /// Entries before the range (for `partition_point`).
    pub fn below(&self, score: f64) -> bool {
        if self.min.exclusive {
            score <= self.min.value
        } else {
            score < self.min.value
        }
    }

    /// Entries not past the range's end.
    pub fn not_above(&self, score: f64) -> bool {
        if self.max.exclusive {
            score < self.max.value
        } else {
            score <= self.max.value
        }
    }
}

/// One end of a lexicographic range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LexBound {
    Min,
    Max,
    Inclusive(Vec<u8>),
    Exclusive(Vec<u8>),
}

/// `-`, `+`, `[member` or `(member`.
pub fn parse_lex_bound(s: &[u8]) -> Option<LexBound> {
    match s.split_first() {
        Some((b'-', [])) => Some(LexBound::Min),
        Some((b'+', [])) => Some(LexBound::Max),
        Some((b'[', rest)) => Some(LexBound::Inclusive(rest.to_vec())),
        Some((b'(', rest)) => Some(LexBound::Exclusive(rest.to_vec())),
        _ => None,
    }
}

#[derive(Clone, Debug)]
pub struct LexRange {
    pub min: LexBound,
    pub max: LexBound,
}

impl LexRange {
    pub fn is_empty(&self) -> bool {
        match (&self.min, &self.max) {
            (LexBound::Max, _) | (_, LexBound::Min) => true,
            (LexBound::Min, _) | (_, LexBound::Max) => false,
            (LexBound::Inclusive(a), LexBound::Inclusive(b)) => a > b,
            (
                LexBound::Inclusive(a) | LexBound::Exclusive(a),
                LexBound::Inclusive(b) | LexBound::Exclusive(b),
            ) => a >= b,
        }
    }

    pub fn below(&self, member: &[u8]) -> bool {
        match &self.min {
            LexBound::Min => false,
            LexBound::Max => true,
            LexBound::Inclusive(m) => member < &m[..],
            LexBound::Exclusive(m) => member <= &m[..],
        }
    }

    pub fn not_above(&self, member: &[u8]) -> bool {
        match &self.max {
            LexBound::Min => false,
            LexBound::Max => true,
            LexBound::Inclusive(m) => member <= &m[..],
            LexBound::Exclusive(m) => member < &m[..],
        }
    }
}

/// A packed sorted set, read in place.
#[derive(Clone, Copy)]
pub struct Packed<'a> {
    len: usize,
    body: &'a [u8],
}

impl<'a> Packed<'a> {
    pub fn new(mut bytes: &'a [u8]) -> Self {
        let len = take_varint(&mut bytes).expect("a packed sorted set") as usize;
        Packed { len, body: bytes }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn iter(&self) -> PackedIter<'a> {
        PackedIter {
            body: self.body,
            left: self.len,
        }
    }
}

pub struct PackedIter<'a> {
    body: &'a [u8],
    left: usize,
}

impl<'a> Iterator for PackedIter<'a> {
    type Item = (f64, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let mut rest = self.body;
        let score = take_score(&mut rest).expect("a score");
        let len = take_varint(&mut rest).expect("a member") as usize;
        let (member, rest) = rest.split_at(len);
        self.body = rest;
        Some((score, member))
    }
}

/// Whether `bytes` is a packed sorted set as this module writes them:
/// scores not NaN, entries in order and members distinct.
pub fn is_valid(mut bytes: &[u8]) -> bool {
    let Some(len) = take_varint(&mut bytes) else {
        return false;
    };
    if len == 0 || len as usize > bytes.len() / 2 {
        return false;
    }
    let mut seen = std::collections::HashSet::new();
    let mut last: Option<(f64, &[u8])> = None;
    for _ in 0..len {
        let Some(score) = take_score(&mut bytes) else {
            return false;
        };
        let Some(n) = take_varint(&mut bytes) else {
            return false;
        };
        if score.is_nan() || n as usize > bytes.len() {
            return false;
        }
        let (member, rest) = bytes.split_at(n as usize);
        bytes = rest;
        if last.is_some_and(|l| cmp(l, (score, member)) != Ordering::Less) || !seen.insert(member) {
            return false;
        }
        last = Some((score, member));
    }
    bytes.is_empty()
}

pub fn fits_packed(bytes: &[u8]) -> bool {
    let set = Packed::new(bytes);
    set.len() <= MAX_PACKED && set.iter().all(|(_, m)| m.len() <= MAX_MEMBER_LEN)
}

/// `i`'s lowest set bit (`i > 0`): a Fenwick tree's step.
fn lowest_bit(i: usize) -> usize {
    1 << i.trailing_zeros()
}

/// Sums of chunk sizes, for ranks in O(log n): a Fenwick tree.
#[derive(Clone, Default)]
struct Sizes(Vec<usize>);

impl Sizes {
    fn build(sizes: impl Iterator<Item = usize>) -> Self {
        let mut tree: Vec<usize> = std::iter::once(0).chain(sizes).collect();
        for i in 1..tree.len() {
            let parent = i + lowest_bit(i);
            if parent < tree.len() {
                tree[parent] += tree[i];
            }
        }
        Sizes(tree)
    }

    fn add(&mut self, chunk: usize, delta: isize) {
        let mut i = chunk + 1;
        while i < self.0.len() {
            self.0[i] = self.0[i].wrapping_add_signed(delta);
            i += lowest_bit(i);
        }
    }

    /// Entries in the chunks before `chunk`.
    fn before(&self, chunk: usize) -> usize {
        let (mut i, mut sum) = (chunk, 0);
        while i > 0 {
            sum += self.0[i];
            i -= lowest_bit(i);
        }
        sum
    }

    /// The chunk holding rank `rank`, and the rank within it.
    fn find(&self, mut rank: usize) -> (usize, usize) {
        let n = self.0.len() - 1;
        let mut pos = 0;
        let mut step = n
            .checked_next_power_of_two()
            .map_or(0, |p| if p > n { p / 2 } else { p });
        while step > 0 {
            if pos + step <= n && self.0[pos + step] <= rank {
                pos += step;
                rank -= self.0[pos];
            }
            step /= 2;
        }
        (pos, rank)
    }
}

/// A big sorted set.
#[derive(Clone, Default)]
pub struct BigZSet {
    members: BigSet,
    /// By position in `members`.
    scores: Paged<f64>,
    /// Positions, by score and then member, in chunks.
    order: Vec<Vec<u32>>,
    sizes: Sizes,
}

impl BigZSet {
    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    fn key(&self, pos: u32) -> (f64, &[u8]) {
        (
            self.scores.get(pos as usize),
            self.members.get_index(pos as usize),
        )
    }

    pub fn score(&self, member: &[u8]) -> Option<f64> {
        self.members.position(member).map(|p| self.scores.get(p))
    }

    /// Whether the entry at `pos` comes before `key`. The member is only read when the
    /// scores tie, as it takes two more cache misses, at every step of a search.
    #[inline]
    fn comes_before(&self, pos: u32, key: (f64, &[u8])) -> bool {
        let score = self.scores.get(pos as usize);
        match score.partial_cmp(&key.0).expect("no NaN scores") {
            Ordering::Less => true,
            Ordering::Greater => false,
            Ordering::Equal => self.members.get_index(pos as usize) < key.1,
        }
    }

    /// Where `(score, member)` is, or would go: its chunk and index there.
    fn locate(&self, key: (f64, &[u8])) -> (usize, usize) {
        let c = self
            .order
            .partition_point(|chunk| self.comes_before(*chunk.last().expect("non-empty"), key));
        if c == self.order.len() {
            return match self.order.last() {
                Some(chunk) => (c - 1, chunk.len()),
                None => (0, 0),
            };
        }
        let i = self.order[c].partition_point(|&p| self.comes_before(p, key));
        (c, i)
    }

    fn rebuild_sizes(&mut self) {
        self.sizes = Sizes::build(self.order.iter().map(Vec::len));
    }

    fn insert_order(&mut self, pos: u32) {
        let (c, i) = self.locate(self.key(pos));
        if self.order.is_empty() {
            self.order.push(vec![pos]);
            return self.rebuild_sizes();
        }
        self.order[c].insert(i, pos);
        if self.order[c].len() > CHUNK_MAX {
            let tail = self.order[c].split_off(CHUNK_MAX / 2);
            self.order.insert(c + 1, tail);
            self.rebuild_sizes();
        } else {
            self.sizes.add(c, 1);
        }
    }

    /// Takes the entry for `key` out of the order.
    fn remove_order(&mut self, key: (f64, &[u8])) {
        let (c, i) = self.locate(key);
        self.order[c].remove(i);
        self.sizes.add(c, -1);
        let len = self.order[c].len();
        if len == 0 {
            self.order.remove(c);
            self.rebuild_sizes();
        } else if len < CHUNK_MAX / 4 {
            // Merge with a small neighbor.
            for (a, b) in [(c.wrapping_sub(1), c), (c, c + 1)] {
                if a < self.order.len()
                    && b < self.order.len()
                    && self.order[a].len() + self.order[b].len() <= CHUNK_MAX / 2
                {
                    let next = self.order.remove(b);
                    self.order[a].extend(next);
                    self.rebuild_sizes();
                    break;
                }
            }
        }
    }

    /// Sets `member`'s score, adding it if new. Returns its old score.
    pub fn insert(&mut self, member: &[u8], score: f64) -> Option<f64> {
        debug_assert!(!score.is_nan());
        if let Some(pos) = self.members.position(member) {
            let old = self.scores.get(pos);
            // Equal scores (0 and -0 too) leave it as it is, as in Redis.
            if old != score {
                self.remove_order((old, member));
                self.scores.set(pos, score);
                self.insert_order(pos as u32);
            }
            return Some(old);
        }
        self.members.insert(member);
        self.scores.push(score);
        self.insert_order((self.members.len() - 1) as u32);
        None
    }

    /// Removes `member`, returning its score.
    pub fn remove(&mut self, member: &[u8]) -> Option<f64> {
        let pos = self.members.position(member)?;
        let score = self.scores.get(pos);
        self.remove_order((score, member));
        let last = self.members.len() - 1;
        if pos != last {
            // The last member moves to `pos`: point its order entry there.
            let (c, i) = self.locate(self.key(last as u32));
            self.order[c][i] = pos as u32;
        }
        self.members.remove_at(pos);
        self.scores.swap_remove(pos);
        Some(score)
    }

    /// The entry at `rank` (from the lowest).
    pub fn get(&self, rank: usize) -> (f64, &[u8]) {
        let (c, i) = self.sizes.find(rank);
        self.key(self.order[c][i])
    }

    /// The member's rank (from the lowest).
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        let score = self.score(member)?;
        let (c, i) = self.locate((score, member));
        Some(self.sizes.before(c) + i)
    }

    /// How many entries come before the first for which `before` is false
    /// (entries for which it holds must all come first).
    pub fn partition_point(&self, before: impl Fn(f64, &[u8]) -> bool) -> usize {
        let c = self.order.partition_point(|chunk| {
            let (s, m) = self.key(*chunk.last().expect("non-empty"));
            before(s, m)
        });
        if c == self.order.len() {
            return self.len();
        }
        let i = self.order[c].partition_point(|&p| {
            let (s, m) = self.key(p);
            before(s, m)
        });
        self.sizes.before(c) + i
    }

    /// Entries `start..end` by rank, lowest first (or highest first if `rev`, counting ranks from the highest).
    pub fn for_range(&self, start: usize, end: usize, rev: bool, mut f: impl FnMut(f64, &[u8])) {
        if start >= end {
            return;
        }
        if !rev {
            let (mut c, mut i) = self.sizes.find(start);
            for _ in start..end {
                let (s, m) = self.key(self.order[c][i]);
                f(s, m);
                i += 1;
                if i == self.order[c].len() {
                    c += 1;
                    i = 0;
                }
            }
        } else {
            let len = self.len();
            let (mut c, mut i) = self.sizes.find(len - 1 - start);
            for n in start..end {
                let (s, m) = self.key(self.order[c][i]);
                f(s, m);
                if n + 1 == end {
                    break;
                }
                if i == 0 {
                    c -= 1;
                    i = self.order[c].len() - 1;
                } else {
                    i -= 1;
                }
            }
        }
    }

    /// The member at `pos` in its member order (for random picks and
    /// `ZSCAN`), with its score.
    pub fn at_position(&self, pos: usize) -> (f64, &[u8]) {
        self.key(pos as u32)
    }

    /// Memory it takes, in O(1), as every change asks: the order's chunks
    /// are counted as a position each plus a quarter for growth room.
    pub fn allocation_size(&self) -> usize {
        self.members.allocation_size()
            + self.scores.allocation_size()
            + self.order.capacity() * size_of::<Vec<u32>>()
            + self.len() * 5
            + self.sizes.0.capacity() * 8
    }

    pub fn to_packed(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_varint(&mut out, self.len() as u64);
        self.for_range(0, self.len(), false, |score, member| {
            put_score(&mut out, score);
            put_varint(&mut out, member.len() as u64);
            out.extend_from_slice(member);
        });
        out
    }

    pub fn from_packed(bytes: &[u8]) -> Self {
        let packed = Packed::new(bytes);
        let mut set = BigZSet {
            members: BigSet::with_capacity(packed.len()),
            scores: Paged::with_capacity(packed.len()),
            order: Vec::new(),
            sizes: Sizes::default(),
        };
        // Already in order: positions are ranks.
        for (score, member) in packed.iter() {
            set.members.insert(member);
            set.scores.push(score);
        }
        let positions: Vec<u32> = (0..packed.len() as u32).collect();
        set.order = positions
            .chunks(CHUNK_MAX / 2)
            .map(<[u32]>::to_vec)
            .collect();
        set.rebuild_sizes();
        set
    }

    pub fn fits_packed(&self) -> bool {
        self.len() <= MAX_PACKED
            && (0..self.len()).all(|p| self.members.get_index(p).len() <= MAX_MEMBER_LEN)
    }
}

/// A sorted set being read: packed or big.
#[derive(Clone, Copy)]
pub enum ZView<'a> {
    Packed(Packed<'a>),
    Big(&'a BigZSet),
}

impl<'a> ZView<'a> {
    pub fn len(&self) -> usize {
        match self {
            ZView::Packed(z) => z.len(),
            ZView::Big(z) => z.len(),
        }
    }

    pub fn score(&self, member: &[u8]) -> Option<f64> {
        match self {
            ZView::Packed(z) => z.iter().find(|(_, m)| *m == member).map(|(s, _)| s),
            ZView::Big(z) => z.score(member),
        }
    }

    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        match self {
            ZView::Packed(z) => z.iter().position(|(_, m)| m == member),
            ZView::Big(z) => z.rank(member),
        }
    }

    pub fn partition_point(&self, before: impl Fn(f64, &[u8]) -> bool) -> usize {
        match self {
            ZView::Packed(z) => z.iter().take_while(|&(s, m)| before(s, m)).count(),
            ZView::Big(z) => z.partition_point(before),
        }
    }

    /// Entries `start..end` by rank, from the lowest, or from the highest
    /// if `rev`.
    pub fn for_range(&self, start: usize, end: usize, rev: bool, mut f: impl FnMut(f64, &[u8])) {
        let end = end.min(self.len());
        if start >= end {
            return;
        }
        match self {
            ZView::Packed(z) if !rev => z
                .iter()
                .skip(start)
                .take(end - start)
                .for_each(|(s, m)| f(s, m)),
            ZView::Packed(z) => {
                let all: Vec<(f64, &[u8])> = z.iter().collect();
                all[all.len() - end..all.len() - start]
                    .iter()
                    .rev()
                    .for_each(|&(s, m)| f(s, m));
            }
            ZView::Big(z) => z.for_range(start, end, rev, f),
        }
    }

    /// The entry at `i` of the member order, for random picks and scans:
    /// O(1) when big.
    pub fn at_position(&self, i: usize) -> (f64, &'a [u8]) {
        match *self {
            ZView::Packed(z) => z.iter().nth(i).expect("in range"),
            ZView::Big(z) => z.at_position(i),
        }
    }
}

/// A sorted set being changed: a packed one decoded into a [`BigZSet`]
/// (packed again if it still fits), or a big one in place.
pub enum ZMut<'a> {
    Owned { set: Box<BigZSet>, changed: bool },
    Big { set: &'a mut BigZSet, changed: bool },
}

pub enum Finished {
    Packed(Vec<u8>),
    Grown(Box<BigZSet>),
    Changed,
    Delete,
}

impl<'a> ZMut<'a> {
    pub fn new() -> Self {
        ZMut::Owned {
            set: Box::default(),
            changed: false,
        }
    }

    pub fn from_packed(bytes: &[u8]) -> Self {
        ZMut::Owned {
            set: Box::new(BigZSet::from_packed(bytes)),
            changed: false,
        }
    }

    pub fn big(set: &'a mut BigZSet) -> Self {
        ZMut::Big {
            set,
            changed: false,
        }
    }

    pub fn set(&self) -> &BigZSet {
        match self {
            ZMut::Owned { set, .. } => set,
            ZMut::Big { set, .. } => set,
        }
    }

    pub fn edit(&mut self) -> &mut BigZSet {
        match self {
            ZMut::Owned { set, changed } => {
                *changed = true;
                set
            }
            ZMut::Big { set, changed } => {
                *changed = true;
                set
            }
        }
    }

    pub fn finish(self) -> Option<Finished> {
        match self {
            ZMut::Owned { changed: false, .. } | ZMut::Big { changed: false, .. } => None,
            ZMut::Owned { set, .. } if set.is_empty() => Some(Finished::Delete),
            ZMut::Owned { set, .. } if set.fits_packed() => Some(Finished::Packed(set.to_packed())),
            ZMut::Owned { set, .. } => Some(Finished::Grown(set)),
            ZMut::Big { set, .. } if set.is_empty() => Some(Finished::Delete),
            ZMut::Big { .. } => Some(Finished::Changed),
        }
    }
}

impl Default for ZMut<'_> {
    fn default() -> Self {
        ZMut::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scores_pack_small_when_whole() {
        for (score, len) in [
            (0.0, 1),
            (5.0, 1),
            (-5.0, 1),
            (31.0, 1),
            (32.0, 2),
            (1.7e9, 5),
            (4503599627370495.0, 8),
            (4503599627370496.0, 9),
            (-0.0, 9),
            (1.5, 9),
            (f64::INFINITY, 9),
            (f64::NEG_INFINITY, 9),
        ] {
            let mut out = Vec::new();
            put_score(&mut out, score);
            assert_eq!(out.len(), len, "{score}");
            let back = take_score(&mut &out[..]).unwrap();
            assert_eq!(back.to_bits(), score.to_bits(), "{score}");
        }
    }

    #[test]
    fn scores_print_as_redis_7_2() {
        for (x, s) in [
            (1.1, "1.1"),
            (0.1, "0.1"),
            (3.0, "3"),
            (-0.0, "0"),
            (1e15, "1000000000000000"),
            (1e17, "100000000000000000"),
            (123456789012345678.0, "123456789012345680"),
            (1e21, "1e+21"),
            (1e22, "1e+22"),
            (1e-5, "0.00001"),
            (1e-7, "1e-7"),
            (2.5e-10, "2.5e-10"),
            (1.5e300, "1.5e+300"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
            (1.7976931348623157e308, "1.7976931348623157e+308"),
            (5e-324, "5e-324"),
            (0.30000000000000004, "0.30000000000000004"),
            (-1.5, "-1.5"),
            (12345678.9, "12345678.9"),
            (9007199254740992.0, "9007199254740992"),
            (1e20, "100000000000000000000"),
            (1e-6, "0.000001"),
            (-123.456e-9, "-1.23456e-7"),
        ] {
            assert_eq!(String::from_utf8(score_string(x)).unwrap(), s, "{x:e}");
        }
    }

    #[test]
    fn scores_parse_as_redis_7_2() {
        for (s, x) in [
            ("1.1", Some(1.1)),
            ("3", Some(3.0)),
            ("-0", Some(-0.0)),
            ("inf", Some(f64::INFINITY)),
            ("+inf", Some(f64::INFINITY)),
            ("-Infinity", Some(f64::NEG_INFINITY)),
            ("INF", Some(f64::INFINITY)),
            ("0x10", Some(16.0)),
            ("-0x10", Some(-16.0)),
            ("0x1p3", Some(8.0)),
            ("0x1.8p1", Some(3.0)),
            ("1.", Some(1.0)),
            (".5", Some(0.5)),
            ("+.5e1", Some(5.0)),
            ("00012", Some(12.0)),
            ("1e-310", Some(1e-310)),
            ("4.9e-324", Some(5e-324)),
            ("1e999", None),
            ("1e-400", None),
            (" 1", None),
            ("1 ", None),
            ("1e", None),
            ("1e+", None),
            ("nan", None),
            ("", None),
            ("0x", None),
            ("-", None),
            ("1_000", None),
        ] {
            assert_eq!(parse_score(s.as_bytes()), x, "{s:?}");
        }
        let bound = |s: &str| parse_score_bound(s.as_bytes()).map(|b| (b.value, b.exclusive));
        assert_eq!(bound(" 1"), Some((1.0, false)));
        assert_eq!(bound("(1"), Some((1.0, true)));
        assert_eq!(bound("( 1"), Some((1.0, true)));
        assert_eq!(bound("("), Some((0.0, true)));
        assert_eq!(bound("1e999"), Some((f64::INFINITY, false)));
        assert_eq!(bound("1 "), None);
        assert_eq!(bound("nan"), None);
        assert_eq!(bound("0x2"), Some((2.0, false)));
    }

    /// Against a model kept in order, through inserts, updates, removals,
    /// ranks and ranges, with chunks splitting and merging.
    #[test]
    fn big_sorted_sets_match_a_model() {
        use std::collections::{BTreeMap, BTreeSet};
        let mut z = BigZSet::default();
        // Scores are quarters, kept as integers so the model can order them.
        let mut scores: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
        let mut order: BTreeSet<(i64, Vec<u8>)> = BTreeSet::new();
        let mut rng = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move |n: u64| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng % n
        };
        let real = |q: i64| q as f64 / 4.0;
        for step in 0..40_000u64 {
            let member = format!("m{}", next(3_000)).into_bytes();
            let q = next(200) as i64 - 40;
            match next(6) {
                0..3 => {
                    let old = scores.insert(member.clone(), q);
                    if let Some(old) = old {
                        order.remove(&(old, member.clone()));
                    }
                    order.insert((q, member.clone()));
                    assert_eq!(z.insert(&member, real(q)), old.map(real), "step {step}");
                }
                3 => {
                    let old = scores.remove(&member);
                    if let Some(old) = old {
                        order.remove(&(old, member.clone()));
                    }
                    assert_eq!(z.remove(&member), old.map(real), "step {step}");
                }
                4 => {
                    let rank = scores
                        .get(&member)
                        .map(|&s| order.range(..(s, member.clone())).count());
                    assert_eq!(z.rank(&member), rank, "step {step}");
                    if !order.is_empty() {
                        let r = next(order.len() as u64) as usize;
                        let (s, m) = order.iter().nth(r).unwrap();
                        assert_eq!(z.get(r), (real(*s), &m[..]), "step {step}");
                    }
                }
                _ => {
                    let below = order.range(..(q, Vec::new())).count();
                    assert_eq!(z.partition_point(|s, _| s < real(q)), below, "step {step}");
                }
            }
            assert_eq!(z.len(), scores.len());
            if step % 4_000 == 0 {
                let all: Vec<(f64, Vec<u8>)> =
                    order.iter().map(|(s, m)| (real(*s), m.clone())).collect();
                let mut seen = Vec::new();
                z.for_range(0, z.len(), false, |s, m| seen.push((s, m.to_vec())));
                assert_eq!(seen, all);
                let mut rev = Vec::new();
                z.for_range(0, z.len(), true, |s, m| rev.push((s, m.to_vec())));
                assert_eq!(rev, all.iter().rev().cloned().collect::<Vec<_>>());
                let packed = z.to_packed();
                assert!(is_valid(&packed) || scores.is_empty());
                let again = BigZSet::from_packed(&packed);
                let mut seen = Vec::new();
                again.for_range(0, again.len(), false, |s, m| seen.push((s, m.to_vec())));
                assert_eq!(seen, all);
            }
        }
    }
}
