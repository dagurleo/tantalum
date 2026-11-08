//! Pieces of Redis' RDB format, for payloads Redis and Tantalum hand each
//! other (`FUNCTION DUMP` and `FUNCTION RESTORE`): lengths, strings with
//! their integer and LZF encodings (Redis' own LZF, `vendor/lzf`), and the
//! footer of a dump: the RDB version and a CRC-64 (Jones) of what's before.

use std::ffi::{c_uint, c_void};

/// The RDB version Redis 7.2 writes.
pub(crate) const VERSION: u16 = 11;
/// A function library: its code follows, as a string.
pub(crate) const OPCODE_FUNCTION2: u8 = 245;
/// Libraries in the format before Redis 7.0's release.
pub(crate) const OPCODE_FUNCTION_PRE_GA: u8 = 246;

const LEN_6BIT: u8 = 0;
const LEN_14BIT: u8 = 1;
const LEN_32BIT: u8 = 0x80;
const LEN_64BIT: u8 = 0x81;
const ENCODED: u8 = 3;
const ENC_INT8: u8 = 0;
const ENC_INT16: u8 = 1;
const ENC_INT32: u8 = 2;
const ENC_LZF: u8 = 3;

unsafe extern "C" {
    fn lzf_compress(
        in_data: *const c_void,
        in_len: c_uint,
        out: *mut c_void,
        out_len: c_uint,
    ) -> c_uint;
    fn lzf_decompress(
        in_data: *const c_void,
        in_len: c_uint,
        out: *mut c_void,
        out_len: c_uint,
    ) -> c_uint;
}

/// Redis' `rdbSaveLen`.
pub(crate) fn put_len(out: &mut Vec<u8>, len: u64) {
    if len < 1 << 6 {
        out.push(len as u8 | LEN_6BIT << 6);
    } else if len < 1 << 14 {
        out.push((len >> 8) as u8 | LEN_14BIT << 6);
        out.push(len as u8);
    } else if len <= u64::from(u32::MAX) {
        out.push(LEN_32BIT);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    } else {
        out.push(LEN_64BIT);
        out.extend_from_slice(&len.to_be_bytes());
    }
}

/// Redis' `rdbSaveRawString`: as an integer if it's one in canonical form and
/// fits 32 bits, LZF-compressed if that saves at least four bytes (over 20
/// bytes, as Redis tries it), or as it is.
pub(crate) fn put_string(out: &mut Vec<u8>, s: &[u8]) {
    if s.len() <= 11
        && let Some(n) = canonical_int(s)
    {
        if let Ok(n) = i8::try_from(n) {
            out.extend_from_slice(&[ENCODED << 6 | ENC_INT8, n as u8]);
            return;
        }
        if let Ok(n) = i16::try_from(n) {
            out.push(ENCODED << 6 | ENC_INT16);
            out.extend_from_slice(&n.to_le_bytes());
            return;
        }
        if let Ok(n) = i32::try_from(n) {
            out.push(ENCODED << 6 | ENC_INT32);
            out.extend_from_slice(&n.to_le_bytes());
            return;
        }
    }
    if s.len() > 20
        && s.len() <= c_uint::MAX as usize
        && let Some(compressed) = compress(s)
    {
        out.push(ENCODED << 6 | ENC_LZF);
        put_len(out, compressed.len() as u64);
        put_len(out, s.len() as u64);
        out.extend_from_slice(&compressed);
        return;
    }
    put_len(out, s.len() as u64);
    out.extend_from_slice(s);
}

/// `s` as Redis' `string2ll` reads it: digits with an optional `-`, no
/// leading zeros (but `0`), within 64 bits.
fn canonical_int(s: &[u8]) -> Option<i64> {
    let n: i64 = std::str::from_utf8(s).ok()?.parse().ok()?;
    (itoa::Buffer::new().format(n).as_bytes() == s).then_some(n)
}

/// LZF, if it saves at least four bytes (Redis' `rdbSaveLzfStringObject`).
fn compress(s: &[u8]) -> Option<Vec<u8>> {
    if s.len() <= 4 {
        return None;
    }
    let room = s.len() - 4;
    let mut out = vec![0u8; room + 1];
    // SAFETY: both buffers are as long as said; LZF writes at most `room` bytes.
    let n = unsafe {
        lzf_compress(
            s.as_ptr().cast(),
            s.len() as c_uint,
            out.as_mut_ptr().cast(),
            room as c_uint,
        )
    };
    if n == 0 {
        return None;
    }
    out.truncate(n as usize);
    Some(out)
}

/// Reads RDB data from a slice.
pub(crate) struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Reader { data, pos: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    pub(crate) fn byte(&mut self) -> Option<u8> {
        let b = *self.data.get(self.pos)?;
        self.pos += 1;
        Some(b)
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let bytes = self.data.get(self.pos..self.pos.checked_add(n)?)?;
        self.pos += n;
        Some(bytes)
    }

    /// Redis' `rdbLoadLenByRef`: a length, or an encoding (`true`).
    fn len_or_encoding(&mut self) -> Option<(u64, bool)> {
        let first = self.byte()?;
        match first >> 6 {
            ENCODED => Some((u64::from(first & 0x3f), true)),
            LEN_6BIT => Some((u64::from(first & 0x3f), false)),
            LEN_14BIT => Some((
                u64::from(first & 0x3f) << 8 | u64::from(self.byte()?),
                false,
            )),
            _ => match first {
                LEN_32BIT => {
                    let b = self.take(4)?;
                    Some((u64::from(u32::from_be_bytes(b.try_into().ok()?)), false))
                }
                LEN_64BIT => {
                    let b = self.take(8)?;
                    Some((u64::from_be_bytes(b.try_into().ok()?), false))
                }
                _ => None,
            },
        }
    }

    pub(crate) fn len(&mut self) -> Option<u64> {
        match self.len_or_encoding()? {
            (len, false) => Some(len),
            (_, true) => None,
        }
    }

    /// Redis' `rdbGenericLoadStringObject`.
    pub(crate) fn string(&mut self) -> Option<Vec<u8>> {
        let (len, encoded) = self.len_or_encoding()?;
        if !encoded {
            return Some(self.take(usize::try_from(len).ok()?)?.to_vec());
        }
        let int = |n: i64| itoa::Buffer::new().format(n).as_bytes().to_vec();
        match len as u8 {
            ENC_INT8 => Some(int(i64::from(self.byte()? as i8))),
            ENC_INT16 => Some(int(i64::from(i16::from_le_bytes(
                self.take(2)?.try_into().ok()?,
            )))),
            ENC_INT32 => Some(int(i64::from(i32::from_le_bytes(
                self.take(4)?.try_into().ok()?,
            )))),
            ENC_LZF => {
                let compressed_len = usize::try_from(self.len()?).ok()?;
                let len = usize::try_from(self.len()?).ok()?;
                let compressed = self.take(compressed_len)?;
                if len > crate::resp::MAX_BULK_LEN || compressed_len > c_uint::MAX as usize {
                    return None;
                }
                let mut out = vec![0u8; len];
                // SAFETY: both buffers are as long as said.
                let n = unsafe {
                    lzf_decompress(
                        compressed.as_ptr().cast(),
                        compressed_len as c_uint,
                        out.as_mut_ptr().cast(),
                        len as c_uint,
                    )
                };
                (n as usize == len).then_some(out)
            }
            _ => None,
        }
    }
}

/// CRC-64 with the Jones polynomial, reflected, as Redis' `crc64`.
pub(crate) fn crc64(mut crc: u64, data: &[u8]) -> u64 {
    const POLY: u64 = 0x95ac_9329_ac4b_c9b5;
    const TABLE: [u64; 256] = {
        let mut table = [0u64; 256];
        let mut i = 0;
        while i < 256 {
            let mut c = i as u64;
            let mut bit = 0;
            while bit < 8 {
                c = if c & 1 != 0 { (c >> 1) ^ POLY } else { c >> 1 };
                bit += 1;
            }
            table[i] = c;
            i += 1;
        }
        table
    };
    for &b in data {
        crc = TABLE[((crc ^ u64::from(b)) & 0xff) as usize] ^ (crc >> 8);
    }
    crc
}

/// Ends a dump payload: the RDB version and the CRC-64 of everything.
pub(crate) fn finish_payload(out: &mut Vec<u8>) {
    out.extend_from_slice(&VERSION.to_le_bytes());
    let crc = crc64(0, out);
    out.extend_from_slice(&crc.to_le_bytes());
}

/// Redis' `verifyDumpPayload`: the body, if the version is one this reads
/// and the checksum matches.
pub(crate) fn payload_body(payload: &[u8]) -> Option<&[u8]> {
    if payload.len() < 10 {
        return None;
    }
    let footer = &payload[payload.len() - 10..];
    if u16::from_le_bytes([footer[0], footer[1]]) > VERSION {
        return None;
    }
    let crc = crc64(0, &payload[..payload.len() - 8]);
    (crc.to_le_bytes() == footer[2..]).then_some(&payload[..payload.len() - 10])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc64_is_redis() {
        // Redis' crc64 test vector.
        assert_eq!(crc64(0, b"123456789"), 0xe9c6_d914_c4b8_d9ca);
    }

    #[test]
    fn strings_come_back() {
        let long =
            b"#!lua name=lib\nredis.register_function('f', function() return 1 end) -- ".repeat(5);
        for s in [
            &b""[..],
            b"0",
            b"-1",
            b"127",
            b"128",
            b"-32768",
            b"40000",
            b"-0",
            b"007",
            b"abc",
            &long,
        ] {
            let mut out = Vec::new();
            put_string(&mut out, s);
            let mut reader = Reader::new(&out);
            assert_eq!(reader.string().as_deref(), Some(s), "{s:?}");
            assert_eq!(reader.remaining(), 0);
        }
        // Integers and repetitive text are encoded.
        let mut out = Vec::new();
        put_string(&mut out, b"127");
        assert_eq!(out, [0xc0, 127]);
        out.clear();
        put_string(&mut out, &long);
        assert_eq!(out[0], 0xc3);
        assert!(out.len() < long.len());
    }

    #[test]
    fn payloads_check_their_footer() {
        let mut payload = vec![OPCODE_FUNCTION2];
        put_string(&mut payload, b"code");
        finish_payload(&mut payload);
        assert_eq!(payload_body(&payload), Some(&payload[..6]));
        let mut damaged = payload.clone();
        damaged[1] ^= 1;
        assert_eq!(payload_body(&damaged), None);
        assert_eq!(payload_body(b"short"), None);
    }
}
