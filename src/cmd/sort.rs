//! `SORT` and `SORT_RO`, following Redis' `sortCommandGeneric`: nothing else
//! runs meanwhile, as it reads other keys for `BY` and `GET`.

use std::cmp::Ordering;
use std::os::raw::{c_char, c_int};

use super::{Args, NOT_AN_INTEGER, OOM, Reply, SYNTAX_ERROR, WRONGTYPE};
use crate::collection::Collection;
use crate::context::{Context, Session};
use crate::list::{self, BigList};
use crate::resp;
use crate::store::{Key, Kind, Millis, OutOfMemory, Store, Update, View};
use crate::{set, zset};

unsafe extern "C" {
    fn tantalum_strtod(s: *const c_char, len: usize, out: *mut f64) -> c_int;
}

/// A `BY` value as a number, as Redis' `strtod` reads it (up to a NUL, and
/// allowing what `strtod` does: spaces first, nothing at all, `inf`). `None` if
/// there's more after it, or it's out of range or NaN.
fn number(text: &[u8]) -> Option<f64> {
    let mut value = 0.0;
    // SAFETY: the pointer and length are of a live slice.
    let ok = unsafe { tantalum_strtod(text.as_ptr().cast(), text.len(), &raw mut value) };
    (ok != 0).then_some(value)
}

/// Options from the command line.
struct Options<'a> {
    desc: bool,
    alpha: bool,
    limit: (i64, i64),
    store: Option<&'a [u8]>,
    by: Option<&'a [u8]>,
    /// `BY` a pattern without a `*`: nothing to sort by.
    dont_sort: bool,
    gets: Vec<&'a [u8]>,
}

impl<'a> Options<'a> {
    /// `None` after writing the error.
    fn parse(args: &Args<'a>, read_only: bool, out: &mut Vec<u8>) -> Option<Self> {
        let mut options = Options {
            desc: false,
            alpha: false,
            limit: (0, -1),
            store: None,
            by: None,
            dont_sort: false,
            gets: Vec::new(),
        };
        let mut i = 2;
        while i < args.len() {
            let option = args.get(i);
            let left = args.len() - i - 1;
            let is = |name: &[u8]| option.eq_ignore_ascii_case(name);
            if is(b"ASC") {
                options.desc = false;
            } else if is(b"DESC") {
                options.desc = true;
            } else if is(b"ALPHA") {
                options.alpha = true;
            } else if is(b"LIMIT") && left >= 2 {
                let (Some(start), Some(count)) = (
                    resp::parse_int(args.get(i + 1)),
                    resp::parse_int(args.get(i + 2)),
                ) else {
                    resp::write_error(out, NOT_AN_INTEGER);
                    return None;
                };
                options.limit = (start, count);
                i += 2;
            } else if !read_only && is(b"STORE") && left >= 1 {
                options.store = Some(args.get(i + 1));
                i += 1;
            } else if is(b"BY") && left >= 1 {
                let pattern = args.get(i + 1);
                options.by = Some(pattern);
                options.dont_sort = !pattern.contains(&b'*');
                i += 1;
            } else if is(b"GET") && left >= 1 {
                options.gets.push(args.get(i + 1));
                i += 1;
            } else {
                resp::write_error(out, SYNTAX_ERROR);
                return None;
            }
            i += 1;
        }
        Some(options)
    }
}

/// The elements of the list, set or sorted set at `key`, in its own order
/// (empty if there's none). `Err` if it's another kind.
fn elements(store: &Store, key: Key<'_>, now: Millis) -> Result<(Kind, Vec<Vec<u8>>), ()> {
    store.get_key(key, now, |value| {
        let Some(value) = value else {
            return Ok((Kind::List, Vec::new()));
        };
        let mut all = Vec::new();
        let view = value.view();
        match (value.kind(), view) {
            (Kind::List, View::Bytes(bytes)) => {
                all.extend(list::Packed::new(bytes).iter().map(<[u8]>::to_vec));
            }
            (Kind::List, View::Object(Collection::List(big))) => {
                all.extend(big.iter().map(<[u8]>::to_vec));
            }
            (Kind::Set, View::Bytes(bytes)) => {
                for member in set::Compact::new(bytes).iter() {
                    member.with_bytes(|m| all.push(m.to_vec()));
                }
            }
            (Kind::Set, View::Object(Collection::Set(big))) => {
                all.extend(big.iter().map(<[u8]>::to_vec));
            }
            (Kind::ZSet, View::Bytes(bytes)) => {
                all.extend(zset::Packed::new(bytes).iter().map(|(_, m)| m.to_vec()));
            }
            (Kind::ZSet, View::Object(Collection::ZSet(big))) => {
                big.for_range(0, big.len(), false, |_, m| all.push(m.to_vec()));
            }
            _ => return Err(()),
        }
        Ok((value.kind(), all))
    })
}

/// What `pattern` finds for `element`: `#` is the element itself; otherwise the
/// `*` is replaced by it, and `->field` reads a hash's field. `None` if there
/// is no such key or field, or no `*`.
fn look_up(store: &Store, now: Millis, pattern: &[u8], element: &[u8]) -> Option<Vec<u8>> {
    if pattern == b"#" {
        return Some(element.to_vec());
    }
    let star = pattern.iter().position(|&c| c == b'*')?;
    let after = &pattern[star + 1..];
    // A field only if something follows the arrow.
    let arrow = after
        .windows(2)
        .position(|w| w == b"->")
        .filter(|&at| at + 2 < after.len());
    let (suffix, field) = match arrow {
        Some(at) => (&after[..at], Some(&after[at + 2..])),
        None => (after, None),
    };
    let mut name = Vec::with_capacity(star + element.len() + suffix.len());
    name.extend_from_slice(&pattern[..star]);
    name.extend_from_slice(element);
    name.extend_from_slice(suffix);
    store.get_key(store.key(&name), now, |value| {
        let value = value?;
        match field {
            Some(field) if value.kind() == Kind::Hash => super::hash::view(value.view())
                .get(field)
                .map(<[u8]>::to_vec),
            None if value.kind() == Kind::String => Some(value.as_bytes().to_vec()),
            _ => None,
        }
    })
}

/// `strcoll` in the C locale, which `SORT` uses unless it's storing: up to a
/// NUL.
fn collate(a: &[u8], b: &[u8]) -> Ordering {
    let c_string = |s: &[u8]| s.iter().position(|&c| c == 0).unwrap_or(s.len());
    a[..c_string(a)].cmp(&b[..c_string(b)])
}

/// One element to sort.
struct Item {
    element: Vec<u8>,
    score: f64,
    /// What `ALPHA` compares under `BY`.
    by: Option<Vec<u8>>,
}

fn run(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    read_only: bool,
) -> Reply {
    let Some(mut options) = Options::parse(args, read_only, out) else {
        return Reply::Written;
    };
    let store = &*ctx.store;
    let in_script = session.in_script;
    let null = session.null();
    store.exclusive(|| {
        let Ok((kind, loaded)) = elements(store, args.key(store), now) else {
            resp::write_error(out, WRONGTYPE);
            return;
        };
        // A set has no order of its own: when the result is written down or
        // goes on to a script's commands, it must be the same every time.
        if options.dont_sort && kind == Kind::Set && (options.store.is_some() || in_script) {
            options.dont_sort = false;
            options.alpha = true;
            options.by = None;
        }
        let total = loaded.len() as i64;
        let mut start = options.limit.0.max(0).min(total);
        let count = options.limit.1.max(-1).min(total);
        let mut end = if count < 0 {
            total - 1
        } else {
            start + count - 1
        };
        if start >= total {
            start = total - 1;
            end = total - 2;
        }
        if end >= total {
            end = total - 1;
        }
        let mut items: Vec<Item> = loaded
            .into_iter()
            .map(|element| Item {
                element,
                score: 0.0,
                by: None,
            })
            .collect();
        if options.dont_sort && kind != Kind::Set {
            // A list's or sorted set's own order, from the far end for `DESC`.
            if options.desc {
                items.reverse();
            }
        }
        let mut bad_score = false;
        if !options.dont_sort {
            let by = options.by;
            for item in &mut items {
                let value = match by {
                    Some(pattern) => match look_up(store, now, pattern, &item.element) {
                        Some(value) => value,
                        None => continue,
                    },
                    None => item.element.clone(),
                };
                if options.alpha {
                    if by.is_some() {
                        item.by = Some(value);
                    }
                } else {
                    match number(&value) {
                        Some(score) => item.score = score,
                        None => bad_score = true,
                    }
                }
            }
            let storing = options.store.is_some();
            let (alpha, by_pattern, desc) = (options.alpha, by.is_some(), options.desc);
            let compare = |a: &Item, b: &Item| {
                let order = if !alpha {
                    a.score
                        .partial_cmp(&b.score)
                        .unwrap_or(Ordering::Equal)
                        .then_with(|| a.element.cmp(&b.element))
                } else if by_pattern {
                    match (&a.by, &b.by) {
                        (None, None) => Ordering::Equal,
                        (None, Some(_)) => Ordering::Less,
                        (Some(_), None) => Ordering::Greater,
                        (Some(x), Some(y)) if storing => x.cmp(y),
                        (Some(x), Some(y)) => collate(x, y),
                    }
                } else if storing {
                    a.element.cmp(&b.element)
                } else {
                    collate(&a.element, &b.element)
                };
                if desc { order.reverse() } else { order }
            };
            items.sort_by(compare);
        }
        if bad_score {
            resp::write_error(
                out,
                b"ERR One or more scores can't be converted into double",
            );
            return;
        }
        let chosen: &[Item] = if end >= start {
            &items[start as usize..=end as usize]
        } else {
            &[]
        };
        let per_element = options.gets.len().max(1);
        let Some(destination) = options.store else {
            resp::write_array_header(out, chosen.len() * per_element);
            for item in chosen {
                if options.gets.is_empty() {
                    resp::write_bulk(out, &item.element);
                }
                for pattern in &options.gets {
                    match look_up(store, now, pattern, &item.element) {
                        Some(value) => resp::write_bulk(out, &value),
                        None => out.extend_from_slice(null),
                    }
                }
            }
            return;
        };
        let mut result = BigList::default();
        for item in chosen {
            if options.gets.is_empty() {
                result.push_back(&item.element);
            }
            for pattern in &options.gets {
                let value = look_up(store, now, pattern, &item.element).unwrap_or_default();
                result.push_back(&value);
            }
        }
        let stored = result.len() as i64;
        let key = store.key(destination);
        let _ = store.update_key(key, now, |_| (Update::Delete, ()));
        if stored > 0 {
            let mut result = Some(result);
            let written = store.update_key(key, now, |_| {
                let list = result.take().expect("written once");
                let update = if list.fits_packed() {
                    Update::Replace(Kind::List, list.to_packed())
                } else {
                    Update::Object(Collection::List(list))
                };
                (update, ())
            });
            if matches!(written, Err(OutOfMemory)) {
                resp::write_error(out, OOM);
                return;
            }
        }
        resp::write_integer(out, stored);
    });
    Reply::Written
}

/// `SORT key [BY pattern] [LIMIT offset count] [GET pattern ...] [ASC | DESC]
/// [ALPHA] [STORE destination]`
pub(super) fn sort(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    run(ctx, session, now, args, out, false)
}

/// `SORT_RO`: `SORT` without `STORE`.
pub(super) fn sort_ro(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    run(ctx, session, now, args, out, true)
}
