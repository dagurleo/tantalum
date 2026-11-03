//! Glob patterns as Redis matches them (`stringmatchlen` in its util.c), for
//! `MATCH` options and `CONFIG GET`: `*` matches any run of bytes, `?` any
//! one byte, `[abc]`, `[^abc]` and `[a-z]` one byte from a set, and `\`
//! makes the next byte literal. Like Redis, `*` doesn't match an empty
//! string on its own; commands treat a lone `*` as "everything" first.

/// Whether `text` matches `pattern`, ignoring ASCII case if `nocase`.
pub fn matches(pattern: &[u8], text: &[u8], nocase: bool) -> bool {
    let mut give_up = false;
    match_from(pattern, 0, text, 0, nocase, &mut give_up, 0)
}

/// Redis' algorithm step by step, including how it reads one byte past a
/// pattern's end (a C string's terminating 0).
fn match_from(
    pat: &[u8],
    mut p: usize,
    text: &[u8],
    mut t: usize,
    nocase: bool,
    give_up: &mut bool,
    nesting: u32,
) -> bool {
    // Protection against abusive patterns, as in Redis.
    if nesting > 1000 {
        return false;
    }
    let at = |i: usize| pat.get(i).copied().unwrap_or(0);
    let fold = |c: u8| if nocase { c.to_ascii_lowercase() } else { c };
    while p < pat.len() && t < text.len() {
        match pat[p] {
            b'*' => {
                while p + 1 < pat.len() && pat[p + 1] == b'*' {
                    p += 1;
                }
                if p + 1 == pat.len() {
                    return true;
                }
                while t < text.len() {
                    if match_from(pat, p + 1, text, t, nocase, give_up, nesting + 1) {
                        return true;
                    }
                    if *give_up {
                        return false;
                    }
                    t += 1;
                }
                // Nothing after this `*` matches anywhere in the rest of the
                // text, so no earlier `*` taking more of it can help.
                *give_up = true;
                return false;
            }
            b'?' => t += 1,
            b'[' => {
                p += 1;
                let not = at(p) == b'^';
                if not {
                    p += 1;
                }
                let mut matched = false;
                loop {
                    let left = pat.len().saturating_sub(p);
                    if at(p) == b'\\' && left >= 2 {
                        p += 1;
                        if pat[p] == text[t] {
                            matched = true;
                        }
                    } else if at(p) == b']' {
                        break;
                    } else if left == 0 {
                        // No closing `]`: the set ends with the pattern.
                        p -= 1;
                        break;
                    } else if left >= 3 && pat[p + 1] == b'-' {
                        let (mut start, mut end) = (fold(pat[p]), fold(pat[p + 2]));
                        if start > end {
                            std::mem::swap(&mut start, &mut end);
                        }
                        p += 2;
                        if (start..=end).contains(&fold(text[t])) {
                            matched = true;
                        }
                    } else if fold(pat[p]) == fold(text[t]) {
                        matched = true;
                    }
                    p += 1;
                }
                if matched == not {
                    return false;
                }
                t += 1;
            }
            c => {
                let c = if c == b'\\' && p + 1 < pat.len() {
                    p += 1;
                    pat[p]
                } else {
                    c
                };
                if fold(c) != fold(text[t]) {
                    return false;
                }
                t += 1;
            }
        }
        p += 1;
        if t == text.len() {
            while at(p) == b'*' && p < pat.len() {
                p += 1;
            }
            break;
        }
    }
    p == pat.len() && t == text.len()
}

#[cfg(test)]
mod tests {
    use super::matches;

    #[test]
    fn matches_like_redis() {
        for (pattern, text, nocase, expected) in [
            ("maxmemory", "maxmemory", true, true),
            ("MAXMEMORY", "maxmemory", true, true),
            ("MAXMEMORY", "maxmemory", false, false),
            ("max*", "maxmemory-policy", true, true),
            ("*policy", "maxmemory-policy", true, true),
            ("*", "save", true, true),
            ("*", "", false, false),
            ("a*", "a", false, true),
            ("a**", "a", false, true),
            ("*mem*", "maxmemory", true, true),
            ("m?xmemory", "maxmemory", true, true),
            ("maxmemory", "maxmemory-policy", true, false),
            ("*x", "maxmemory", true, false),
            ("", "save", true, false),
            ("", "", false, true),
            ("a*b*c", "aXbYbZc", true, true),
            ("a*b*c", "aXbYbZ", true, false),
            ("h[ae]llo", "hello", false, true),
            ("h[ae]llo", "hallo", false, true),
            ("h[ae]llo", "hillo", false, false),
            ("h[^e]llo", "hallo", false, true),
            ("h[^e]llo", "hello", false, false),
            ("h[a-b]llo", "hbllo", false, true),
            ("h[b-a]llo", "hbllo", false, true),
            ("h[a-b]llo", "hcllo", false, false),
            ("h[A-B]llo", "hbllo", true, true),
            ("h\\*llo", "h*llo", false, true),
            ("h\\*llo", "hello", false, false),
            ("h[\\]]llo", "h]llo", false, true),
            ("[abc", "a", false, true),
            ("[abc", "b", false, true),
            ("[abc", "d", false, false),
            ("ab\\", "ab\\", false, true),
            (
                "*a*a*a*a*a*a*a*a*a*b",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                false,
                false,
            ),
        ] {
            assert_eq!(
                matches(pattern.as_bytes(), text.as_bytes(), nocase),
                expected,
                "{pattern:?} vs {text:?}"
            );
        }
    }
}
