//! HyperLogLog, as Redis 7.2.4's `hyperloglog.c` has it: a string holding
//! 16,384 registers of 6 bits, dense, or sparse as runs of opcodes, in the same
//! bytes, so `GET`, `SET` and a snapshot move them between the two servers. See
//! that file for how the representations work; the names here follow it.
//!
//! A string can hold anything, so nothing here trusts what it reads: a corrupt
//! one gives [`Corrupt`], where Redis replies `INVALIDOBJ`.

/// A sparse representation that doesn't add up.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Corrupt;

const P: u32 = 14;
/// Bits of the hash left to count the zeroes of.
const Q: usize = 64 - P as usize;
pub(crate) const REGISTERS: usize = 1 << P;
const BITS: usize = 6;
/// Magic, encoding, three unused bytes and the cached cardinality.
const HEADER: usize = 16;
pub(crate) const DENSE_SIZE: usize = HEADER + (REGISTERS * BITS).div_ceil(8);
const ENCODING: usize = 4;
const DENSE: u8 = 0;
const SPARSE: u8 = 1;
/// The cached cardinality's last byte, whose top bit says it's out of date.
const CACHE_TOP: usize = 15;
const ALPHA_INF: f64 = 0.721_347_520_444_481_7;

const SPARSE_VAL_MAX_VALUE: u8 = 32;
const SPARSE_VAL_MAX_LEN: usize = 4;
const SPARSE_ZERO_MAX_LEN: usize = 64;
const SPARSE_XZERO_MAX_LEN: usize = 16384;

fn is_zero(op: u8) -> bool {
    op & 0xc0 == 0
}

fn is_xzero(op: u8) -> bool {
    op & 0xc0 == 0x40
}

fn is_val(op: u8) -> bool {
    op & 0x80 != 0
}

/// How many registers each opcode covers, 0 for `XZERO`, which takes two bytes
/// to say: rare once there's data.
const SHORT_SPANS: [u8; 256] = {
    let mut spans = [0; 256];
    let mut op = 0;
    while op < 256 {
        let byte = op as u8;
        spans[op] = if byte & 0xc0 == 0 {
            (byte & 0x3f) + 1
        } else if byte & 0x80 != 0 {
            (byte & 0x3) + 1
        } else {
            0
        };
        op += 1;
    }
    spans
};

/// The value of the registers each opcode covers.
const VALUES: [u8; 256] = {
    let mut values = [0; 256];
    let mut op = 0;
    while op < 256 {
        let byte = op as u8;
        if byte & 0x80 != 0 {
            values[op] = ((byte >> 2) & 0x1f) + 1;
        }
        op += 1;
    }
    values
};

fn zero_len(op: u8) -> usize {
    usize::from(op & 0x3f) + 1
}

/// The run of the `XZERO` at `p`. (Its second byte may be past the end of a
/// corrupt one, where Redis reads the string's terminating zero.)
fn xzero_len(bytes: &[u8], p: usize) -> usize {
    let next = bytes.get(p + 1).copied().unwrap_or(0);
    ((usize::from(bytes[p] & 0x3f) << 8) | usize::from(next)) + 1
}

fn val_value(op: u8) -> u8 {
    ((op >> 2) & 0x1f) + 1
}

fn val_len(op: u8) -> usize {
    usize::from(op & 0x3) + 1
}

fn val_op(value: u8, len: usize) -> u8 {
    ((value - 1) << 2) | (len as u8 - 1) | 0x80
}

/// Redis' 64-bit MurmurHash2, always little endian.
fn murmur64a(data: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4_a793_5bd1_e995;
    const R: u32 = 47;
    let mut h = seed ^ (data.len() as u64).wrapping_mul(M);
    let (chunks, rest) = data.as_chunks::<8>();
    for chunk in chunks {
        let mut k = u64::from_le_bytes(*chunk);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
    }
    if !rest.is_empty() {
        for (i, &byte) in rest.iter().enumerate() {
            h ^= u64::from(byte) << (8 * i);
        }
        h = h.wrapping_mul(M);
    }
    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

/// The register an element falls in, and the length of its hash's `000...1`.
fn pattern(element: &[u8]) -> (usize, u8) {
    let hash = murmur64a(element, 0xadc8_3b19);
    let index = (hash & (REGISTERS as u64 - 1)) as usize;
    let rest = (hash >> P) | (1 << Q);
    (index, rest.trailing_zeros() as u8 + 1)
}

/// A register of the dense representation. (The last one's "next byte", past
/// the end, is only touched when it holds nothing of the register.)
fn get_register(registers: &[u8], i: usize) -> u8 {
    let byte = i * BITS / 8;
    let first = (i * BITS) & 7;
    let low = u32::from(registers[byte]) >> first;
    let high = if first > 2 {
        u32::from(registers[byte + 1]) << (8 - first)
    } else {
        0
    };
    ((low | high) & 63) as u8
}

fn set_register(registers: &mut [u8], i: usize, value: u8) {
    let byte = i * BITS / 8;
    let first = (i * BITS) & 7;
    let value = u32::from(value);
    let low = u32::from(registers[byte]) & !(63 << first);
    registers[byte] = (low | (value << first)) as u8;
    if first > 2 {
        let high = u32::from(registers[byte + 1]) & !(63 >> (8 - first));
        registers[byte + 1] = (high | (value >> (8 - first))) as u8;
    }
}

/// Raises a dense register to `count`; whether that changed it.
fn dense_set(registers: &mut [u8], index: usize, count: u8) -> bool {
    if count > get_register(registers, index) {
        set_register(registers, index, count);
        true
    } else {
        false
    }
}

/// Whether a string can be a HyperLogLog, as `PFADD` and the rest check first.
pub(crate) fn is_valid(bytes: &[u8]) -> bool {
    bytes.len() >= HEADER
        && &bytes[..4] == b"HYLL"
        && bytes[ENCODING] <= SPARSE
        && (bytes[ENCODING] != DENSE || bytes.len() == DENSE_SIZE)
}

pub(crate) fn is_dense(hll: &[u8]) -> bool {
    hll[ENCODING] == DENSE
}

/// An empty one, which is sparse: `XZERO` opcodes covering every register.
pub(crate) fn new_sparse() -> Vec<u8> {
    let mut hll = Vec::with_capacity(HEADER + 2);
    hll.extend_from_slice(b"HYLL");
    hll.push(SPARSE);
    hll.resize(HEADER, 0);
    let run = SPARSE_XZERO_MAX_LEN - 1;
    hll.push((run >> 8) as u8 | 0x40);
    hll.push((run & 0xff) as u8);
    hll
}

/// The cached cardinality, unless it's out of date.
pub(crate) fn cached(hll: &[u8]) -> Option<u64> {
    (hll[CACHE_TOP] & 0x80 == 0)
        .then(|| u64::from_le_bytes(hll[8..HEADER].try_into().expect("eight bytes")))
}

pub(crate) fn set_cached(hll: &mut [u8], cardinality: u64) {
    hll[8..HEADER].copy_from_slice(&cardinality.to_le_bytes());
}

pub(crate) fn invalidate(hll: &mut [u8]) {
    hll[CACHE_TOP] |= 0x80;
}

/// Makes a sparse one dense, keeping its header.
pub(crate) fn to_dense(hll: &mut Vec<u8>) -> Result<(), Corrupt> {
    if hll[ENCODING] == DENSE {
        return Ok(());
    }
    let mut dense = vec![0; DENSE_SIZE];
    dense[..HEADER].copy_from_slice(&hll[..HEADER]);
    dense[ENCODING] = DENSE;
    let registers = &mut dense[HEADER..];
    let mut index = 0;
    let mut p = HEADER;
    while p < hll.len() {
        let op = hll[p];
        if is_zero(op) {
            index += zero_len(op);
            p += 1;
        } else if is_xzero(op) {
            index += xzero_len(hll, p);
            p += 2;
        } else {
            let run = val_len(op);
            if index + run > REGISTERS {
                break;
            }
            for _ in 0..run {
                set_register(registers, index, val_value(op));
                index += 1;
            }
            p += 1;
        }
    }
    if index != REGISTERS {
        return Err(Corrupt);
    }
    *hll = dense;
    Ok(())
}

/// What [`sparse_set_in`] came to.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SparseSet {
    Unchanged,
    /// Its new length.
    Changed(usize),
    /// It has to turn dense, and nothing's changed.
    Promote,
}

/// Raises the register at `index` to `count` in a sparse one of `end` bytes, in
/// `buf`, which has 3 zeroes more after them, as `hllSparseSet` does. The scan,
/// where all the time goes, looks spans up rather than asking each opcode what
/// it is: which it is can't be predicted, in a string being filled at random.
pub(crate) fn sparse_set_in(
    buf: &mut [u8],
    mut end: usize,
    index: usize,
    count: u8,
    max_bytes: usize,
) -> Result<SparseSet, Corrupt> {
    if count > SPARSE_VAL_MAX_VALUE {
        return Ok(SparseSet::Promote);
    }
    // Step 1: the opcode covering the register.
    let mut p = HEADER;
    let mut first = 0;
    let mut prev = None;
    let mut span = 0;
    while p < end {
        let short = usize::from(SHORT_SPANS[usize::from(buf[p])]);
        let op_len;
        if short != 0 {
            span = short;
            op_len = 1;
        } else {
            span = xzero_len(buf, p);
            op_len = 2;
        }
        if index < first + span {
            break;
        }
        prev = Some(p);
        p += op_len;
        first += span;
    }
    if span == 0 || p >= end {
        return Err(Corrupt);
    }
    let op = buf[p];

    // Step 2: nothing to do, or nothing to split, or the general case.
    let mut updated = false;
    if is_val(op) {
        if val_value(op) >= count {
            return Ok(SparseSet::Unchanged);
        }
        if span == 1 {
            buf[p] = val_op(count, 1);
            updated = true;
        }
    }
    if is_zero(op) && span == 1 {
        buf[p] = val_op(count, 1);
        updated = true;
    }
    if !updated {
        let mut seq = [0u8; 5];
        let mut n = 0;
        let last = first + span - 1;
        let mut push = |byte: u8| {
            seq[n] = byte;
            n += 1;
        };
        if is_val(op) {
            let value = val_value(op);
            if index != first {
                push(val_op(value, index - first));
            }
            push(val_op(count, 1));
            if index != last {
                push(val_op(value, last - index));
            }
        } else {
            if index != first {
                zero_ops(index - first, &mut push);
            }
            push(val_op(count, 1));
            if index != last {
                zero_ops(last - index, &mut push);
            }
        }

        // Step 3: the new opcodes in place of the old one.
        let old_len = if is_xzero(op) { 2 } else { 1 };
        let delta = n as isize - old_len as isize;
        if delta > 0 && end + delta as usize > max_bytes {
            return Ok(SparseSet::Promote);
        }
        let next = p + old_len;
        if delta != 0 && next < end {
            buf.copy_within(next..end, (next as isize + delta) as usize);
        }
        // (A corrupt one's last `XZERO` may have had no second byte.)
        let fits = n.min(buf.len() - p);
        buf[p..p + fits].copy_from_slice(&seq[..fits]);
        end = (end as isize + delta) as usize;
    }

    // Step 4: merges neighbouring `VAL`s that came out the same.
    let mut p = prev.unwrap_or(HEADER);
    let mut scan = 5;
    while p < end && scan > 0 {
        scan -= 1;
        let op = buf[p];
        if is_xzero(op) {
            p += 2;
            continue;
        }
        if is_zero(op) {
            p += 1;
            continue;
        }
        if p + 1 < end && is_val(buf[p + 1]) {
            let next = buf[p + 1];
            if val_value(op) == val_value(next) {
                let len = val_len(op) + val_len(next);
                if len <= SPARSE_VAL_MAX_LEN {
                    buf[p + 1] = val_op(val_value(op), len);
                    buf.copy_within(p + 1..end, p);
                    end -= 1;
                    continue;
                }
            }
        }
        p += 1;
    }
    invalidate(buf);
    Ok(SparseSet::Changed(end))
}

/// Adds an element to a sparse one of `end` bytes in `buf`, as
/// [`sparse_set_in`].
pub(crate) fn sparse_add_in(
    buf: &mut [u8],
    end: usize,
    element: &[u8],
    max_bytes: usize,
) -> Result<SparseSet, Corrupt> {
    let (index, count) = pattern(element);
    sparse_set_in(buf, end, index, count, max_bytes)
}

/// Raises the register at `index` to `count` in a sparse one: whether it changed
/// it. It may turn dense, when the value doesn't fit the sparse form or the string
/// would pass `max_bytes`.
pub(crate) fn sparse_set(
    hll: &mut Vec<u8>,
    index: usize,
    count: u8,
    max_bytes: usize,
) -> Result<bool, Corrupt> {
    let end = hll.len();
    hll.resize(end + 3, 0);
    let result = sparse_set_in(hll, end, index, count, max_bytes);
    let len = match result {
        Ok(SparseSet::Changed(len)) => len,
        _ => end,
    };
    hll.truncate(len);
    match result? {
        SparseSet::Unchanged => Ok(false),
        SparseSet::Changed(_) => Ok(true),
        SparseSet::Promote => promote(hll, index, count),
    }
}

/// The opcode for a run of empty registers.
fn zero_ops(len: usize, push: &mut impl FnMut(u8)) {
    if len > SPARSE_ZERO_MAX_LEN {
        let run = len - 1;
        push((run >> 8) as u8 | 0x40);
        push((run & 0xff) as u8);
    } else {
        push((len - 1) as u8);
    }
}

/// Turns dense to set a register the sparse form can't.
fn promote(hll: &mut Vec<u8>, index: usize, count: u8) -> Result<bool, Corrupt> {
    to_dense(hll)?;
    Ok(dense_set(&mut hll[HEADER..], index, count))
}

/// Adds an element to a dense one's registers; whether that changed one.
pub(crate) fn dense_add(hll: &mut [u8], element: &[u8]) -> bool {
    let (index, count) = pattern(element);
    dense_set(&mut hll[HEADER..], index, count)
}

/// Adds an element; whether that changed a register.
pub(crate) fn add(hll: &mut Vec<u8>, element: &[u8], max_bytes: usize) -> Result<bool, Corrupt> {
    if is_dense(hll) {
        Ok(dense_add(hll, element))
    } else {
        let (index, count) = pattern(element);
        sparse_set(hll, index, count, max_bytes)
    }
}

/// Sets a register to at least `count`, ignoring a corrupt one, as `PFMERGE`
/// does (it has read it whole by then).
pub(crate) fn raise(hll: &mut Vec<u8>, index: usize, count: u8, max_bytes: usize) {
    if is_dense(hll) {
        dense_set(&mut hll[HEADER..], index, count);
    } else {
        let _ = sparse_set(hll, index, count, max_bytes);
    }
}

/// `MAX(max[i], register i)` for each register.
pub(crate) fn merge(max: &mut [u8], hll: &[u8]) -> Result<(), Corrupt> {
    if is_dense(hll) {
        let registers = &hll[HEADER..];
        for (i, slot) in max.iter_mut().enumerate().take(REGISTERS) {
            *slot = (*slot).max(get_register(registers, i));
        }
        return Ok(());
    }
    let mut index = 0;
    let mut p = HEADER;
    while p < hll.len() {
        let op = hll[p];
        if is_zero(op) {
            index += zero_len(op);
            p += 1;
        } else if is_xzero(op) {
            index += xzero_len(hll, p);
            p += 2;
        } else {
            let run = val_len(op);
            if index + run > REGISTERS {
                break;
            }
            for slot in &mut max[index..index + run] {
                *slot = (*slot).max(val_value(op));
            }
            index += run;
            p += 1;
        }
    }
    if index == REGISTERS {
        Ok(())
    } else {
        Err(Corrupt)
    }
}

fn sigma(mut x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }
    let mut y = 1.0;
    let mut z = x;
    loop {
        x *= x;
        let before = z;
        z += x * y;
        y += y;
        if before == z {
            return z;
        }
    }
}

fn tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let mut y = 1.0;
    let mut z = 1.0 - x;
    loop {
        x = x.sqrt();
        let before = z;
        y *= 0.5;
        z -= (1.0 - x) * (1.0 - x) * y;
        if before == z {
            return z / 3.0;
        }
    }
}

/// The cardinality from how many registers hold each value (Ertl, "New
/// cardinality estimation algorithms for HyperLogLog sketches").
fn estimate(histogram: &[u32; 64]) -> u64 {
    let m = REGISTERS as f64;
    let mut z = m * tau((m - f64::from(histogram[Q + 1])) / m);
    for count in histogram[1..=Q].iter().rev() {
        z += f64::from(*count);
        z *= 0.5;
    }
    z += m * sigma(f64::from(histogram[0]) / m);
    (ALPHA_INF * m * m / z).round() as u64
}

/// The approximate cardinality of a valid one.
pub(crate) fn count(hll: &[u8]) -> Result<u64, Corrupt> {
    let mut histogram = [0; 64];
    if is_dense(hll) {
        for [a, b, c] in hll[HEADER..].as_chunks::<3>().0 {
            let word = u32::from(*a) | u32::from(*b) << 8 | u32::from(*c) << 16;
            histogram[(word & 63) as usize] += 1;
            histogram[((word >> 6) & 63) as usize] += 1;
            histogram[((word >> 12) & 63) as usize] += 1;
            histogram[((word >> 18) & 63) as usize] += 1;
        }
    } else {
        let mut index = 0;
        let mut p = HEADER;
        while p < hll.len() {
            let op = usize::from(hll[p]);
            let short = usize::from(SHORT_SPANS[op]);
            if short != 0 {
                index += short;
                histogram[usize::from(VALUES[op])] += short as u32;
                p += 1;
            } else {
                let run = xzero_len(hll, p);
                index += run;
                histogram[0] += run as u32;
                p += 2;
            }
        }
        if index != REGISTERS {
            return Err(Corrupt);
        }
    }
    Ok(estimate(&histogram))
}

/// The approximate cardinality of registers of a byte each, as [`merge`] makes.
pub(crate) fn count_registers(max: &[u8]) -> u64 {
    let mut histogram = [0; 64];
    for &register in max {
        histogram[usize::from(register & 63)] += 1;
    }
    estimate(&histogram)
}

/// Every register of a dense one.
pub(crate) fn registers(hll: &[u8]) -> impl Iterator<Item = u8> + '_ {
    let registers = &hll[HEADER..];
    (0..REGISTERS).map(|i| get_register(registers, i))
}

/// `PFDEBUG DECODE`: a sparse one's opcodes, in words.
pub(crate) fn decode(hll: &[u8]) -> Vec<u8> {
    let mut decoded = Vec::new();
    let mut p = HEADER;
    while p < hll.len() {
        let op = hll[p];
        let word = if is_zero(op) {
            p += 1;
            format!("z:{} ", zero_len(op))
        } else if is_xzero(op) {
            let run = xzero_len(hll, p);
            p += 2;
            format!("Z:{run} ")
        } else {
            p += 1;
            format!("v:{},{} ", val_value(op), val_len(op))
        };
        decoded.extend_from_slice(word.as_bytes());
    }
    if decoded.last() == Some(&b' ') {
        decoded.pop();
    }
    decoded
}

/// `PFSELFTEST`: registers keep their values, and the estimate stays close and
/// the same in both representations. `random` gives random numbers.
pub(crate) fn self_test(mut random: impl FnMut() -> u32, max_bytes: usize) -> Result<(), String> {
    let mut dense = vec![0; DENSE_SIZE];
    dense[..4].copy_from_slice(b"HYLL");
    let mut bytes = vec![0u8; REGISTERS];
    for _ in 0..1000 {
        for (i, byte) in bytes.iter_mut().enumerate() {
            let value = (random() & 63) as u8;
            *byte = value;
            set_register(&mut dense[HEADER..], i, value);
        }
        for (i, &byte) in bytes.iter().enumerate() {
            let value = get_register(&dense[HEADER..], i);
            if value != byte {
                return Err(format!(
                    "TESTFAILED Register {i} should be {byte} but is {value}"
                ));
            }
        }
    }
    dense[HEADER..].fill(0);
    let mut sparse = new_sparse();
    let relative_error = 1.04 / (REGISTERS as f64).sqrt();
    let mut checkpoint: u64 = 1;
    let seed = u64::from(random()) | u64::from(random()) << 32;
    for j in 1..=10_000_000u64 {
        let element = (j ^ seed).to_le_bytes();
        dense_add(&mut dense, &element);
        let _ = add(&mut sparse, &element, max_bytes);
        if j != checkpoint {
            continue;
        }
        if (j as usize) < max_bytes / 2 && is_dense(&sparse) {
            return Err("TESTFAILED sparse encoding not used".to_string());
        }
        let estimated = count(&dense).unwrap_or(0);
        if count(&sparse) != Ok(estimated) {
            return Err("TESTFAILED dense/sparse disagree".to_string());
        }
        let error = checkpoint.abs_diff(estimated);
        let allowed = if j == 10 {
            1
        } else {
            (relative_error * 6.0 * checkpoint as f64).ceil() as u64
        };
        if error > allowed {
            return Err(format!(
                "TESTFAILED Too big error. card:{checkpoint} abserr:{error}"
            ));
        }
        checkpoint *= 10;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX: usize = 3000;

    fn added(n: usize) -> Vec<u8> {
        let mut hll = new_sparse();
        for i in 0..n {
            add(&mut hll, format!("element:{i}").as_bytes(), MAX).unwrap();
        }
        hll
    }

    #[test]
    fn registers_keep_their_values_and_leave_their_neighbours() {
        let mut registers = vec![0u8; DENSE_SIZE - HEADER];
        for i in 0..REGISTERS {
            set_register(&mut registers, i, (i % 64) as u8);
        }
        for i in 0..REGISTERS {
            assert_eq!(get_register(&registers, i), (i % 64) as u8);
        }
        set_register(&mut registers, REGISTERS - 1, 63);
        assert_eq!(get_register(&registers, REGISTERS - 1), 63);
        assert_eq!(
            get_register(&registers, REGISTERS - 2),
            ((REGISTERS - 2) % 64) as u8
        );
    }

    #[test]
    fn starts_empty_and_sparse() {
        let mut hll = new_sparse();
        assert_eq!(hll, b"HYLL\x01\0\0\0\0\0\0\0\0\0\0\0\x7f\xff");
        assert_eq!(add(&mut hll, b"a", MAX), Ok(true));
        assert_eq!(cached(&hll), None);
        assert!(is_valid(&hll));
    }

    #[test]
    fn dense_and_sparse_agree_at_every_size() {
        for n in [0, 1, 2, 10, 100, 1000, 5000, 20000] {
            let sparse = added(n);
            let mut dense = sparse.clone();
            to_dense(&mut dense).unwrap();
            assert_eq!(dense.len(), DENSE_SIZE);
            assert_eq!(count(&sparse), count(&dense), "{n}");
            let mut max = vec![0; REGISTERS];
            merge(&mut max, &sparse).unwrap();
            assert_eq!(Ok(count_registers(&max)), count(&sparse), "{n}");
            let error = (count(&sparse).unwrap() as f64 - n as f64).abs();
            assert!(error <= (n as f64) * 0.05 + 1.0, "{n}: {error}");
        }
    }

    #[test]
    fn stays_sparse_until_it_passes_the_limit() {
        let hll = added(100);
        assert!(!is_dense(&hll));
        assert!(hll.len() < 1000);
        let hll = added(20000);
        assert!(is_dense(&hll));
        assert_eq!(hll.len(), DENSE_SIZE);
        let mut small = new_sparse();
        for i in 0..100 {
            add(&mut small, format!("{i}").as_bytes(), 30).unwrap();
        }
        assert!(is_dense(&small));
    }

    #[test]
    fn corrupt_ones_are_found_not_trusted() {
        let mut tail = added(3);
        tail.extend_from_slice(b"hello");
        assert_eq!(count(&tail), Err(Corrupt));
        assert_eq!(to_dense(&mut tail.clone()), Err(Corrupt));
        assert_eq!(merge(&mut [0; REGISTERS], &tail), Err(Corrupt));
        // Whatever a string holds, nothing here panics.
        let mut state = 0x1234_5678_u64;
        for _ in 0..3000 {
            let mut hll = added(5);
            for _ in 0..4 {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                let at = HEADER + (state >> 33) as usize % (hll.len() - HEADER);
                hll[at] = (state >> 20) as u8;
            }
            if state & 1 == 0 {
                hll.truncate(hll.len() - 1);
            }
            let _ = count(&hll);
            let _ = merge(&mut [0; REGISTERS], &hll);
            let _ = add(&mut hll.clone(), b"x", MAX);
            let _ = to_dense(&mut hll.clone());
            let _ = decode(&hll);
        }
    }

    #[test]
    fn checks_what_a_string_is() {
        assert!(is_valid(&new_sparse()));
        assert!(!is_valid(b"HYLL"));
        assert!(!is_valid(b"not a hyperloglog, this"));
        let mut hll = new_sparse();
        hll[ENCODING] = 2;
        assert!(!is_valid(&hll));
        hll[ENCODING] = DENSE;
        assert!(!is_valid(&hll), "dense of the wrong length");
    }

    #[test]
    fn caches_little_endian() {
        let mut hll = new_sparse();
        assert_eq!(cached(&hll), Some(0));
        set_cached(&mut hll, 0x0102_0304);
        assert_eq!(&hll[8..12], &[4, 3, 2, 1]);
        assert_eq!(cached(&hll), Some(0x0102_0304));
        invalidate(&mut hll);
        assert_eq!(cached(&hll), None);
    }

    /// The scan by tables has to read just what the opcodes say.
    #[test]
    fn spans_and_values_are_what_the_opcodes_say() {
        for op in 0..=255u8 {
            let (span, value) = (SHORT_SPANS[usize::from(op)], VALUES[usize::from(op)]);
            if is_xzero(op) {
                assert_eq!((span, value), (0, 0), "{op}");
            } else if is_zero(op) {
                assert_eq!((usize::from(span), value), (zero_len(op), 0), "{op}");
            } else {
                assert_eq!(
                    (usize::from(span), value),
                    (val_len(op), val_value(op)),
                    "{op}"
                );
            }
        }
    }

    /// In place, with room, it comes to the same bytes as in a `Vec`.
    #[test]
    fn in_place_and_in_a_vec_agree() {
        let mut vec = new_sparse();
        let mut buf = vec![0; 200_000];
        buf[..vec.len()].copy_from_slice(&vec);
        let mut end = vec.len();
        for i in 0..5000 {
            let element = format!("element:{i}");
            let in_vec = add(&mut vec, element.as_bytes(), MAX).unwrap();
            if is_dense(&vec) {
                assert_eq!(
                    sparse_add_in(&mut buf, end, element.as_bytes(), MAX),
                    Ok(SparseSet::Promote),
                    "{i}"
                );
                return;
            }
            match sparse_add_in(&mut buf, end, element.as_bytes(), MAX).unwrap() {
                SparseSet::Unchanged => assert!(!in_vec, "{i}"),
                SparseSet::Changed(new_end) => {
                    assert!(in_vec, "{i}");
                    end = new_end;
                }
                SparseSet::Promote => panic!("{i}"),
            }
            assert_eq!(&buf[..end], &vec[..], "{i}");
            buf[end..end + 3].fill(0);
        }
        panic!("it never turned dense");
    }

    #[test]
    fn decodes_in_words() {
        assert_eq!(decode(&new_sparse()), b"Z:16384");
    }

    #[test]
    #[cfg_attr(debug_assertions, ignore = "takes a while unoptimized")]
    fn self_test_passes() {
        let mut state = 1u32;
        let random = move || {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state
        };
        assert_eq!(self_test(random, MAX), Ok(()));
    }
}
