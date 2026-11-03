//! RESP2 request parsing and reply encoding.
//!
//! The parser is zero-copy: it records argument positions as ranges into the
//! connection's read buffer instead of allocating per argument. It is also
//! restartable: when a command is only partially received it returns
//! `Ok(None)` and is simply called again once more bytes arrive. Bulk payloads
//! are length-prefixed, so a restart only rescans the short header lines.

use std::ops::Range;

/// Longest inline (telnet-style) command we accept, same as Redis.
pub const MAX_INLINE_LEN: usize = 64 * 1024;
/// Largest bulk string we accept, same as Redis' `proto-max-bulk-len` default.
pub const MAX_BULK_LEN: usize = 512 * 1024 * 1024;
/// Most arguments allowed in one command.
pub const MAX_ARGS: usize = 1024 * 1024;
/// A `*<n>` or `$<n>` header never needs more than this many bytes.
const MAX_HEADER_LEN: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolError {
    InlineTooBig,
    InvalidMultibulkLength,
    InvalidBulkLength,
    ExpectedDollar(u8),
    MissingCrlf,
}

impl ProtocolError {
    pub fn message(&self) -> String {
        match self {
            Self::InlineTooBig => "Protocol error: too big inline request".into(),
            Self::InvalidMultibulkLength => "Protocol error: invalid multibulk length".into(),
            Self::InvalidBulkLength => "Protocol error: invalid bulk length".into(),
            Self::ExpectedDollar(got) => {
                format!("Protocol error: expected '$', got '{}'", got.escape_ascii())
            }
            Self::MissingCrlf => "Protocol error: expected CRLF".into(),
        }
    }
}

/// Parses one command starting at `buf[start..]`.
///
/// On success returns the offset just past the command and fills `args` with
/// absolute ranges into `buf`. `args` may be empty (blank inline line, `*0`),
/// in which case the caller should skip the command. Returns `Ok(None)` when
/// the command is not complete yet.
pub fn parse_command(
    buf: &[u8],
    start: usize,
    args: &mut Vec<Range<usize>>,
) -> Result<Option<usize>, ProtocolError> {
    args.clear();
    match buf.get(start) {
        None => Ok(None),
        Some(b'*') => parse_multibulk(buf, start, args),
        Some(_) => parse_inline(buf, start, args),
    }
}

fn parse_multibulk(
    buf: &[u8],
    start: usize,
    args: &mut Vec<Range<usize>>,
) -> Result<Option<usize>, ProtocolError> {
    let Some((count, mut pos)) =
        read_header(buf, start + 1, ProtocolError::InvalidMultibulkLength)?
    else {
        return Ok(None);
    };
    if count > MAX_ARGS as i64 {
        return Err(ProtocolError::InvalidMultibulkLength);
    }
    for _ in 0..count.max(0) {
        match buf.get(pos) {
            None => return Ok(None),
            Some(b'$') => {}
            Some(&other) => return Err(ProtocolError::ExpectedDollar(other)),
        }
        let Some((len, data_start)) = read_header(buf, pos + 1, ProtocolError::InvalidBulkLength)?
        else {
            return Ok(None);
        };
        if len < 0 || len as usize > MAX_BULK_LEN {
            return Err(ProtocolError::InvalidBulkLength);
        }
        let data_end = data_start + len as usize;
        if buf.len() < data_end + 2 {
            return Ok(None);
        }
        if &buf[data_end..data_end + 2] != b"\r\n" {
            return Err(ProtocolError::MissingCrlf);
        }
        args.push(data_start..data_end);
        pos = data_end + 2;
    }
    Ok(Some(pos))
}

/// Reads a `<integer>\r\n` header line beginning at `from`.
/// Returns the integer and the offset just past the line.
fn read_header(
    buf: &[u8],
    from: usize,
    invalid: ProtocolError,
) -> Result<Option<(i64, usize)>, ProtocolError> {
    match read_short_header(buf, from) {
        Some(header) => Ok(Some(header)),
        None => read_header_strict(buf, from, invalid),
    }
}

/// The common case of [`read_header`] in one pass: a complete line of up to
/// 17 digits without a leading zero. Headers are a few bytes long, so this
/// beats searching for the CR first. Anything else is left to the strict path.
fn read_short_header(buf: &[u8], from: usize) -> Option<(i64, usize)> {
    let line = buf.get(from..)?;
    let (&first @ b'1'..=b'9', rest) = line.split_first()? else {
        return None;
    };
    let mut n = i64::from(first - b'0');
    for (i, &b) in rest.iter().enumerate().take(17) {
        match b {
            b'0'..=b'9' => n = n * 10 + i64::from(b - b'0'),
            b'\r' if rest.get(i + 1) == Some(&b'\n') => return Some((n, from + i + 3)),
            _ => return None,
        }
    }
    None
}

fn read_header_strict(
    buf: &[u8],
    from: usize,
    invalid: ProtocolError,
) -> Result<Option<(i64, usize)>, ProtocolError> {
    let window = &buf[from.min(buf.len())..buf.len().min(from + MAX_HEADER_LEN)];
    let Some(cr) = memchr::memchr(b'\r', window) else {
        return if window.len() == MAX_HEADER_LEN {
            Err(invalid)
        } else {
            Ok(None)
        };
    };
    let line_end = from + cr;
    match buf.get(line_end + 1) {
        None => Ok(None),
        Some(b'\n') => {
            let n = parse_int(&buf[from..line_end]).ok_or(invalid)?;
            Ok(Some((n, line_end + 2)))
        }
        Some(_) => Err(ProtocolError::MissingCrlf),
    }
}

fn parse_inline(
    buf: &[u8],
    start: usize,
    args: &mut Vec<Range<usize>>,
) -> Result<Option<usize>, ProtocolError> {
    let Some(nl) = memchr::memchr(b'\n', &buf[start..]) else {
        return if buf.len() - start > MAX_INLINE_LEN {
            Err(ProtocolError::InlineTooBig)
        } else {
            Ok(None)
        };
    };
    if nl > MAX_INLINE_LEN {
        return Err(ProtocolError::InlineTooBig);
    }
    let mut end = start + nl;
    let next = end + 1;
    if end > start && buf[end - 1] == b'\r' {
        end -= 1;
    }
    let mut i = start;
    while i < end {
        while i < end && is_inline_space(buf[i]) {
            i += 1;
        }
        let arg_start = i;
        while i < end && !is_inline_space(buf[i]) {
            i += 1;
        }
        if i > arg_start {
            args.push(arg_start..i);
        }
    }
    Ok(Some(next))
}

fn is_inline_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\r')
}

/// Strict base-10 integer parsing with the same rules as Redis' `string2ll`:
/// no `+`, no leading zeros, no `-0`, no surrounding whitespace.
pub fn parse_int(s: &[u8]) -> Option<i64> {
    let (negative, digits) = match s {
        [b'-', rest @ ..] => (true, rest),
        _ => (false, s),
    };
    match digits {
        [] => return None,
        [b'0'] if !negative => return Some(0),
        [b'0', ..] => return None,
        _ => {}
    }
    let mut n: i64 = 0;
    for &d in digits {
        if !d.is_ascii_digit() {
            return None;
        }
        let d = (d - b'0') as i64;
        n = n.checked_mul(10)?;
        n = if negative {
            n.checked_sub(d)?
        } else {
            n.checked_add(d)?
        };
    }
    Some(n)
}

pub const OK: &[u8] = b"+OK\r\n";
pub const PONG: &[u8] = b"+PONG\r\n";
pub const NULL_BULK: &[u8] = b"$-1\r\n";

pub fn write_integer(out: &mut Vec<u8>, n: i64) {
    out.push(b':');
    out.extend_from_slice(itoa::Buffer::new().format(n).as_bytes());
    out.extend_from_slice(b"\r\n");
}

/// Writes the header of an array of `len` replies; the replies follow.
pub fn write_array_header(out: &mut Vec<u8>, len: usize) {
    out.push(b'*');
    out.extend_from_slice(itoa::Buffer::new().format(len).as_bytes());
    out.extend_from_slice(b"\r\n");
}

/// RESP3's null, for connections that asked for RESP3 with `HELLO 3`.
pub const NULL: &[u8] = b"_\r\n";

/// Writes the header of a map of `len` key/value pairs: RESP3's `%`, or in
/// RESP2 a flat array of keys and values.
pub fn write_map_header(out: &mut Vec<u8>, len: usize, resp3: bool) {
    if resp3 {
        out.push(b'%');
        out.extend_from_slice(itoa::Buffer::new().format(len).as_bytes());
        out.extend_from_slice(b"\r\n");
    } else {
        write_array_header(out, len * 2);
    }
}

/// Writes the header of a set: RESP3's `~`, or in RESP2 an array.
pub fn write_set_header(out: &mut Vec<u8>, len: usize, resp3: bool) {
    if resp3 {
        out.push(b'~');
        out.extend_from_slice(itoa::Buffer::new().format(len).as_bytes());
        out.extend_from_slice(b"\r\n");
    } else {
        write_array_header(out, len);
    }
}

/// Writes text as RESP3's verbatim string (`=`, format `txt`), or in RESP2
/// a bulk string.
pub fn write_verbatim_text(out: &mut Vec<u8>, text: &[u8], resp3: bool) {
    if resp3 {
        out.push(b'=');
        out.extend_from_slice(itoa::Buffer::new().format(text.len() + 4).as_bytes());
        out.extend_from_slice(b"\r\ntxt:");
        out.extend_from_slice(text);
        out.extend_from_slice(b"\r\n");
    } else {
        write_bulk(out, text);
    }
}

/// Writes a simple string, `+<s>\r\n`. `s` must not contain CR or LF.
pub fn write_simple(out: &mut Vec<u8>, s: &[u8]) {
    out.push(b'+');
    out.extend_from_slice(s);
    out.extend_from_slice(b"\r\n");
}

pub fn write_bulk_header(out: &mut Vec<u8>, len: usize) {
    if len < 10_000 {
        // Almost every value is this short: format by hand, in one write.
        let digits = match len {
            0..=9 => 1,
            10..=99 => 2,
            100..=999 => 3,
            _ => 4,
        };
        let mut line = [b'$', 0, 0, 0, 0, 0, 0];
        let mut n = len;
        for d in line[1..=digits].iter_mut().rev() {
            *d = b'0' + (n % 10) as u8;
            n /= 10;
        }
        line[digits + 1..digits + 3].copy_from_slice(b"\r\n");
        out.extend_from_slice(&line[..digits + 3]);
        return;
    }
    out.push(b'$');
    out.extend_from_slice(itoa::Buffer::new().format(len).as_bytes());
    out.extend_from_slice(b"\r\n");
}

pub fn write_bulk(out: &mut Vec<u8>, data: &[u8]) {
    out.reserve(data.len() + 16);
    write_bulk_header(out, data.len());
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
}

/// Writes `-<msg>\r\n`. CR and LF in `msg` are replaced with spaces so user
/// input echoed in an error can never inject extra protocol frames.
pub fn write_error(out: &mut Vec<u8>, msg: &[u8]) {
    out.push(b'-');
    out.extend(
        msg.iter()
            .map(|&b| if b == b'\r' || b == b'\n' { b' ' } else { b }),
    );
    out.extend_from_slice(b"\r\n");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_all(buf: &[u8]) -> Result<Vec<Vec<Vec<u8>>>, ProtocolError> {
        let mut args = Vec::new();
        let mut out = Vec::new();
        let mut pos = 0;
        while let Some(next) = parse_command(buf, pos, &mut args)? {
            out.push(args.iter().map(|r| buf[r.clone()].to_vec()).collect());
            pos = next;
        }
        assert_eq!(pos, buf.len(), "trailing bytes left unparsed");
        Ok(out)
    }

    fn cmd(parts: &[&str]) -> Vec<Vec<u8>> {
        parts.iter().map(|p| p.as_bytes().to_vec()).collect()
    }

    #[test]
    fn multibulk_and_pipelining() {
        let buf = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
        assert_eq!(
            parse_all(buf).unwrap(),
            vec![cmd(&["SET", "k", "hello"]), cmd(&["GET", "k"])]
        );
    }

    #[test]
    fn binary_safe_values() {
        let buf = b"*2\r\n$3\r\nGET\r\n$4\r\n\r\n\0\xff\r\n";
        assert_eq!(
            parse_all(buf).unwrap(),
            vec![vec![b"GET".to_vec(), b"\r\n\0\xff".to_vec()]]
        );
    }

    #[test]
    fn inline_commands() {
        let buf = b"PING\r\nset  k\tv\n\r\n\nGET k\r\n";
        assert_eq!(
            parse_all(buf).unwrap(),
            vec![
                cmd(&["PING"]),
                cmd(&["set", "k", "v"]),
                cmd(&[]),
                cmd(&[]),
                cmd(&["GET", "k"])
            ]
        );
    }

    #[test]
    fn empty_and_null_multibulk_are_skipped() {
        assert_eq!(
            parse_all(b"*0\r\n*-1\r\n").unwrap(),
            vec![cmd(&[]), cmd(&[])]
        );
    }

    #[test]
    fn every_split_point_is_incomplete_until_the_end() {
        let buf: &[u8] = b"*3\r\n$3\r\nSET\r\n$10\r\n0123456789\r\n$2\r\nab\r\n";
        let mut args = Vec::new();
        for cut in 0..buf.len() {
            assert_eq!(
                parse_command(&buf[..cut], 0, &mut args),
                Ok(None),
                "cut at {cut}"
            );
        }
        assert_eq!(parse_command(buf, 0, &mut args), Ok(Some(buf.len())));
    }

    #[test]
    fn short_headers_parse_like_the_strict_path() {
        let mut lines: Vec<Vec<u8>> = [
            &b"0"[..],
            b"1",
            b"9",
            b"10",
            b"01",
            b"-1",
            b"-0",
            b"+1",
            b" 1",
            b"1 ",
            b"12x",
            b"99999999999999999",
            b"999999999999999999",
            b"9223372036854775807",
            b"9223372036854775808",
            b"",
        ]
        .iter()
        .map(|s| s.to_vec())
        .collect();
        lines.extend((0..2000).map(|n| n.to_string().into_bytes()));
        for line in lines {
            for tail in [&b"\r\n"[..], b"\r\nrest", b"\r", b"", b"\rx", b"\n"] {
                let buf = [&b"$"[..], &line, tail].concat();
                let strict = read_header_strict(&buf, 1, ProtocolError::InvalidBulkLength);
                if let Some(short) = read_short_header(&buf, 1) {
                    assert_eq!(
                        Ok(Some(short)),
                        strict,
                        "{:?}",
                        String::from_utf8_lossy(&buf)
                    );
                }
                assert_eq!(
                    read_header(&buf, 1, ProtocolError::InvalidBulkLength),
                    strict,
                    "{:?}",
                    String::from_utf8_lossy(&buf)
                );
            }
        }
    }

    #[test]
    fn bulk_headers() {
        for len in (0..20_000).chain([99_999, 100_000, usize::MAX]) {
            let mut out = Vec::new();
            write_bulk_header(&mut out, len);
            assert_eq!(out, format!("${len}\r\n").into_bytes());
        }
    }

    #[test]
    fn protocol_errors() {
        let mut a = Vec::new();
        assert_eq!(
            parse_command(b"*x\r\n", 0, &mut a),
            Err(ProtocolError::InvalidMultibulkLength)
        );
        assert_eq!(
            parse_command(b"*1\r\n:1\r\n", 0, &mut a),
            Err(ProtocolError::ExpectedDollar(b':'))
        );
        assert_eq!(
            parse_command(b"*1\r\n$-1\r\n", 0, &mut a),
            Err(ProtocolError::InvalidBulkLength)
        );
        assert_eq!(
            parse_command(b"*1\r\n$1\r\nab\r\n", 0, &mut a),
            Err(ProtocolError::MissingCrlf)
        );
        assert_eq!(
            parse_command(b"*1\r\r", 0, &mut a),
            Err(ProtocolError::MissingCrlf)
        );
        assert_eq!(
            parse_command(b"*99999999999\r\n", 0, &mut a),
            Err(ProtocolError::InvalidMultibulkLength)
        );
        // A bulk length over the limit is rejected before any buffering happens.
        assert_eq!(
            parse_command(b"*1\r\n$4000000000\r\n", 0, &mut a),
            Err(ProtocolError::InvalidBulkLength)
        );
        // Headers that never terminate are rejected once they get too long.
        assert_eq!(
            parse_command(&[b'*'; 64], 0, &mut a),
            Err(ProtocolError::InvalidMultibulkLength)
        );
        let long_inline = vec![b'a'; MAX_INLINE_LEN + 1];
        assert_eq!(
            parse_command(&long_inline, 0, &mut a),
            Err(ProtocolError::InlineTooBig)
        );
    }

    #[test]
    fn int_parsing_matches_redis() {
        assert_eq!(parse_int(b"0"), Some(0));
        assert_eq!(parse_int(b"-12"), Some(-12));
        assert_eq!(parse_int(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_int(b"-9223372036854775808"), Some(i64::MIN));
        for bad in [
            &b""[..],
            b"-",
            b"+1",
            b"01",
            b"-0",
            b" 1",
            b"1 ",
            b"1a",
            b"9223372036854775808",
        ] {
            assert_eq!(parse_int(bad), None, "{:?}", bad.escape_ascii().to_string());
        }
    }

    #[test]
    fn errors_cannot_inject_frames() {
        let mut out = Vec::new();
        write_error(&mut out, b"ERR bad\r\n+OK");
        assert_eq!(out, b"-ERR bad  +OK\r\n");
    }
}
