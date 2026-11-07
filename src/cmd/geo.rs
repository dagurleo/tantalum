//! Geo commands, on sorted sets, checking their arguments and the key in the
//! order Redis' `geo.c` does (see `src/geo.rs` for the numbers). A search reads
//! the set under its lock and copies out what it finds, as Redis does.

use std::ops::Range;

use super::zset::{self, read, store_update};
use super::{Args, NOT_AN_INTEGER, OOM, Reply, SYNTAX_ERROR, WRONGTYPE};
use crate::context::{Context, Session};
use crate::geo::{self, Shape};
use crate::resp;
use crate::store::{Millis, OutOfMemory, Update};
use crate::zset::{BigZSet, ZView, parse_score};

const NOT_A_FLOAT: &[u8] = b"ERR value is not a valid float";
const BAD_MEMBER: &[u8] = b"ERR could not decode requested zset member";

fn reply_error(out: &mut Vec<u8>, message: &[u8]) -> Reply {
    resp::write_error(out, message);
    Reply::Written
}

/// The longitude and latitude at `args[at..at + 2]`, or `None`, having replied.
fn long_lat(args: &Args<'_>, at: usize, out: &mut Vec<u8>) -> Option<[f64; 2]> {
    let mut xy = [0.0; 2];
    for (i, coordinate) in xy.iter_mut().enumerate() {
        match parse_score(args.get(at + i)) {
            Some(value) => *coordinate = value,
            None => {
                resp::write_error(out, NOT_A_FLOAT);
                return None;
            }
        }
    }
    if !(geo::LONG_MIN..=geo::LONG_MAX).contains(&xy[0])
        || !(geo::LAT_MIN..=geo::LAT_MAX).contains(&xy[1])
    {
        let message = format!(
            "ERR invalid longitude,latitude pair {:.6},{:.6}",
            xy[0], xy[1]
        );
        resp::write_error(out, message.as_bytes());
        return None;
    }
    Some(xy)
}

/// How many meters are in the unit named, or `None`, having replied.
fn unit(name: &[u8], out: &mut Vec<u8>) -> Option<f64> {
    let is = |unit: &[u8]| name.eq_ignore_ascii_case(unit);
    if is(b"m") {
        Some(1.0)
    } else if is(b"km") {
        Some(1000.0)
    } else if is(b"ft") {
        Some(0.3048)
    } else if is(b"mi") {
        Some(1609.34)
    } else {
        resp::write_error(
            out,
            b"ERR unsupported unit provided. please use M, KM, FT, MI",
        );
        None
    }
}

/// `radius unit` at `args[at..]` into the shape, or `None`, having replied.
fn radius(args: &Args<'_>, at: usize, shape: &mut Shape, out: &mut Vec<u8>) -> Option<()> {
    let Some(distance) = parse_score(args.get(at)) else {
        resp::write_error(out, b"ERR need numeric radius");
        return None;
    };
    if distance < 0.0 {
        resp::write_error(out, b"ERR radius cannot be negative");
        return None;
    }
    shape.radius = distance;
    shape.conversion = unit(args.get(at + 1), out)?;
    Some(())
}

/// `width height unit` at `args[at..]` into the shape, or `None`, having replied.
fn dimensions(args: &Args<'_>, at: usize, shape: &mut Shape, out: &mut Vec<u8>) -> Option<()> {
    let Some(width) = parse_score(args.get(at)) else {
        resp::write_error(out, b"ERR need numeric width");
        return None;
    };
    let Some(height) = parse_score(args.get(at + 1)) else {
        resp::write_error(out, b"ERR need numeric height");
        return None;
    };
    if height < 0.0 || width < 0.0 {
        resp::write_error(out, b"ERR height or width cannot be negative");
        return None;
    }
    shape.height = height;
    shape.width = width;
    shape.conversion = unit(args.get(at + 2), out)?;
    Some(())
}

/// `GEOADD key [NX|XX] [CH] longitude latitude member [...]`: `ZADD` with each
/// position as its score.
pub(super) fn geoadd(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let (mut nx, mut xx) = (false, false);
    let mut first = 2;
    while first < args.len() {
        let option = args.get(first);
        if option.eq_ignore_ascii_case(b"NX") {
            nx = true;
        } else if option.eq_ignore_ascii_case(b"XX") {
            xx = true;
        } else if !option.eq_ignore_ascii_case(b"CH") {
            break;
        }
        first += 1;
    }
    if !(args.len() - first).is_multiple_of(3) || (nx && xx) {
        return reply_error(out, SYNTAX_ERROR);
    }
    let mut buf = Vec::new();
    let mut ranges: Vec<Range<usize>> = Vec::with_capacity(first + (args.len() - first) / 3 * 2);
    let mut push = |bytes: &[u8]| {
        let start = buf.len();
        buf.extend_from_slice(bytes);
        ranges.push(start..buf.len());
    };
    push(b"zadd");
    for i in 1..first {
        push(args.get(i));
    }
    for i in (first..args.len()).step_by(3) {
        let Some([longitude, latitude]) = long_lat(args, i, out) else {
            return Reply::Written;
        };
        let score = geo::encode(longitude, latitude, geo::STEP_MAX).align52();
        push(itoa::Buffer::new().format(score).as_bytes());
        push(args.get(i + 2));
    }
    zset::zadd(ctx, session, now, &Args::new(&buf, &ranges), out)
}

/// `GEOHASH key [member ...]`
pub(super) fn geohash(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    const ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let store = ctx.db(session);
    let null = session.null();
    let read_set = read(store, args.key(store), now, session, |set| {
        resp::write_array_header(out, args.len() - 2);
        for i in 2..args.len() {
            let xy = set
                .as_ref()
                .and_then(|set| set.score(args.get(i)))
                .and_then(geo::decode_score);
            let Some([longitude, latitude]) = xy else {
                out.extend_from_slice(null);
                continue;
            };
            // The score's latitudes run to 85.05 where the standard's run to 90.
            let bits = geo::encode_standard(longitude, latitude);
            let mut text = [ALPHABET[0]; 11];
            for (position, letter) in text.iter_mut().take(10).enumerate() {
                *letter = ALPHABET[((bits >> (52 - (position + 1) * 5)) & 0x1f) as usize];
            }
            resp::write_bulk(out, &text);
        }
    });
    if read_set.is_err() {
        return reply_error(out, WRONGTYPE);
    }
    Reply::Written
}

/// `GEOPOS key [member ...]`
pub(super) fn geopos(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let store = ctx.db(session);
    let resp3 = session.resp3;
    let null: &[u8] = if resp3 { resp::NULL } else { b"*-1\r\n" };
    let read_set = read(store, args.key(store), now, session, |set| {
        resp::write_array_header(out, args.len() - 2);
        for i in 2..args.len() {
            let xy = set
                .as_ref()
                .and_then(|set| set.score(args.get(i)))
                .and_then(geo::decode_score);
            match xy {
                Some([longitude, latitude]) => {
                    resp::write_array_header(out, 2);
                    write_coordinate(out, longitude, resp3);
                    write_coordinate(out, latitude, resp3);
                }
                None => out.extend_from_slice(null),
            }
        }
    });
    if read_set.is_err() {
        return reply_error(out, WRONGTYPE);
    }
    Reply::Written
}

/// A long double as Redis replies one: a bulk string, or a double in RESP3.
fn write_coordinate(out: &mut Vec<u8>, value: f64, resp3: bool) {
    let text = geo::format_coordinate(value);
    if resp3 {
        out.push(b',');
        out.extend_from_slice(&text);
        out.extend_from_slice(b"\r\n");
    } else {
        resp::write_bulk(out, &text);
    }
}

/// `GEODIST key member member [unit]`
pub(super) fn geodist(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let mut to_meters = 1.0;
    if args.len() == 5 {
        match unit(args.get(4), out) {
            Some(meters) => to_meters = meters,
            None => return Reply::Written,
        }
    } else if args.len() > 5 {
        return reply_error(out, SYNTAX_ERROR);
    }
    let store = ctx.db(session);
    let null = session.null();
    let found = read(store, args.key(store), now, session, |set| {
        let set = set?;
        let (a, b) = (set.score(args.get(2))?, set.score(args.get(3))?);
        Some((geo::decode_score(a)?, geo::decode_score(b)?))
    });
    match found {
        Err(()) => return reply_error(out, WRONGTYPE),
        Ok(None) => out.extend_from_slice(null),
        Ok(Some((a, b))) => {
            let distance = geo::distance(a[0], a[1], b[0], b[1]) / to_meters;
            resp::write_bulk(out, &geo::format_distance(distance));
        }
    }
    Reply::Written
}

#[derive(Clone, Copy, PartialEq)]
enum Sort {
    None,
    Ascending,
    Descending,
}

/// The form of a search.
#[derive(Clone, Copy, PartialEq)]
enum Form {
    /// `GEORADIUS`: around coordinates.
    Coordinates {
        read_only: bool,
    },
    /// `GEORADIUSBYMEMBER`: around a member.
    Member {
        read_only: bool,
    },
    Search,
    SearchStore,
}

impl Form {
    fn is_search(self) -> bool {
        matches!(self, Form::Search | Form::SearchStore)
    }

    /// Whether it takes `STORE key` and `STOREDIST key`.
    fn takes_store_key(self) -> bool {
        matches!(
            self,
            Form::Coordinates { read_only: false } | Form::Member { read_only: false }
        )
    }
}

struct Point {
    longitude: f64,
    latitude: f64,
    distance: f64,
    score: f64,
    member: Vec<u8>,
}

/// What a search asked for, and what it found.
struct Found {
    source_exists: bool,
    points: Vec<Point>,
    shape: Shape,
    store_key: Option<usize>,
    store_distance: bool,
    with_distance: bool,
    with_hash: bool,
    with_coordinates: bool,
    sort: Sort,
    count: i64,
}

/// Appends the points in one box, in score order, down to the `limit` wanted, if
/// any (`ANY`).
fn points_in_box(
    set: &ZView<'_>,
    hash: geo::Bits,
    shape: &Shape,
    found: &mut Vec<Point>,
    limit: usize,
) {
    let (min, max) = geo::score_range(hash);
    let start = set.partition_by_score(|score| score < min);
    set.scan_from(start, |score, member| {
        if score >= max {
            return false;
        }
        let Some([longitude, latitude]) = geo::decode_score(score) else {
            return true;
        };
        if let Some(distance) = shape.distance_within([longitude, latitude]) {
            found.push(Point {
                longitude,
                latitude,
                distance,
                score,
                member: member.get().to_vec(),
            });
        }
        limit == 0 || found.len() < limit
    });
}

/// Reads a search's arguments in Redis' order, which runs into the set for
/// `FROMMEMBER`, and searches. `None` if it replied with an error.
fn search_set(
    set: Option<ZView<'_>>,
    args: &Args<'_>,
    form: Form,
    out: &mut Vec<u8>,
) -> Option<Found> {
    let mut shape = Shape::default();
    let mut store_key = None;
    let mut store_distance = false;
    let base = match form {
        Form::Coordinates { .. } => {
            shape.circular = true;
            shape.xy = long_lat(args, 2, out)?;
            radius(args, 4, &mut shape, out)?;
            6
        }
        Form::Member { .. } => {
            if let Some(set) = &set {
                shape.circular = true;
                let position = set.score(args.get(2)).and_then(geo::decode_score);
                let Some(xy) = position else {
                    resp::write_error(out, BAD_MEMBER);
                    return None;
                };
                shape.xy = xy;
                radius(args, 3, &mut shape, out)?;
            }
            5
        }
        Form::Search => 2,
        Form::SearchStore => {
            store_key = Some(1);
            3
        }
    };
    let (mut with_distance, mut with_hash, mut with_coordinates) = (false, false, false);
    let (mut from_member, mut from_location, mut by_radius, mut by_box) =
        (false, false, false, false);
    let mut sort = Sort::None;
    let mut any = false;
    let mut count = 0;
    let remaining = args.len().saturating_sub(base);
    let mut i = 0;
    while i < remaining {
        let at = base + i;
        let arg = args.get(at);
        let is = |word: &[u8]| arg.eq_ignore_ascii_case(word);
        if is(b"WITHDIST") {
            with_distance = true;
        } else if is(b"WITHHASH") {
            with_hash = true;
        } else if is(b"WITHCOORD") {
            with_coordinates = true;
        } else if is(b"ANY") {
            any = true;
        } else if is(b"ASC") {
            sort = Sort::Ascending;
        } else if is(b"DESC") {
            sort = Sort::Descending;
        } else if is(b"COUNT") && i + 1 < remaining {
            let Some(value) = resp::parse_int(args.get(at + 1)) else {
                resp::write_error(out, NOT_AN_INTEGER);
                return None;
            };
            if value <= 0 {
                resp::write_error(out, b"ERR COUNT must be > 0");
                return None;
            }
            count = value;
            i += 1;
        } else if is(b"STORE") && i + 1 < remaining && form.takes_store_key() {
            store_key = Some(at + 1);
            store_distance = false;
            i += 1;
        } else if is(b"STOREDIST") && i + 1 < remaining && form.takes_store_key() {
            store_key = Some(at + 1);
            store_distance = true;
            i += 1;
        } else if is(b"STOREDIST") && form == Form::SearchStore {
            store_distance = true;
        } else if is(b"FROMMEMBER") && i + 1 < remaining && form.is_search() && !from_location {
            if let Some(set) = &set {
                let position = set.score(args.get(at + 1)).and_then(geo::decode_score);
                let Some(xy) = position else {
                    resp::write_error(out, BAD_MEMBER);
                    return None;
                };
                shape.xy = xy;
            }
            from_member = true;
            i += 1;
        } else if is(b"FROMLONLAT") && i + 2 < remaining && form.is_search() && !from_member {
            shape.xy = long_lat(args, at + 1, out)?;
            from_location = true;
            i += 2;
        } else if is(b"BYRADIUS") && i + 2 < remaining && form.is_search() && !by_box {
            radius(args, at + 1, &mut shape, out)?;
            shape.circular = true;
            by_radius = true;
            i += 2;
        } else if is(b"BYBOX") && i + 3 < remaining && form.is_search() && !by_radius {
            dimensions(args, at + 1, &mut shape, out)?;
            shape.circular = false;
            by_box = true;
            i += 3;
        } else {
            resp::write_error(out, SYNTAX_ERROR);
            return None;
        }
        i += 1;
    }
    if store_key.is_some() && (with_distance || with_hash || with_coordinates) {
        let what: &[u8] = if form == Form::SearchStore {
            b"GEOSEARCHSTORE"
        } else {
            b"STORE option in GEORADIUS"
        };
        let message = [
            b"ERR ",
            what,
            b" is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
        ]
        .concat();
        resp::write_error(out, &message);
        return None;
    }
    if form.is_search() && !(from_member || from_location) {
        let message = [
            b"ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for ",
            args.get(0),
        ]
        .concat();
        resp::write_error(out, &message);
        return None;
    }
    if form.is_search() && !(by_radius || by_box) {
        let message = [
            b"ERR exactly one of BYRADIUS and BYBOX can be specified for ",
            args.get(0),
        ]
        .concat();
        resp::write_error(out, &message);
        return None;
    }
    if any && count == 0 {
        resp::write_error(out, b"ERR the ANY argument requires COUNT argument");
        return None;
    }
    // Sorted so that it's the closest, unless any will do.
    if count != 0 && sort == Sort::None && !any {
        sort = Sort::Ascending;
    }
    let mut points = Vec::new();
    if let Some(set) = &set {
        let limit = if any { count as usize } else { 0 };
        let boxes = geo::search_boxes(&shape);
        let mut last = None;
        for (i, hash) in boxes.into_iter().enumerate() {
            if geo::is_empty_box(hash) {
                continue;
            }
            // A huge radius may have the same neighbour twice in a row. (As in
            // Redis, the center isn't compared.)
            if last == Some(hash) {
                continue;
            }
            if limit != 0 && points.len() >= limit {
                break;
            }
            points_in_box(set, hash, &shape, &mut points, limit);
            if i != 0 {
                last = Some(hash);
            }
        }
    }
    Some(Found {
        source_exists: set.is_some(),
        points,
        shape,
        store_key,
        store_distance,
        with_distance,
        with_hash,
        with_coordinates,
        sort,
        count,
    })
}

fn search(
    ctx: &Context,
    session: &mut Session,
    now: Millis,
    args: &Args<'_>,
    out: &mut Vec<u8>,
    source: usize,
    form: Form,
) -> Reply {
    let store = ctx.db(session);
    let run = |session: &mut Session, out: &mut Vec<u8>| {
        let searched = read(store, args.key_at(store, source), now, session, |set| {
            search_set(set, args, form, out)
        });
        match searched {
            Err(()) => {
                resp::write_error(out, WRONGTYPE);
            }
            Ok(None) => {}
            Ok(Some(found)) => reply_or_store(store, now, args, session.resp3, found, out),
        }
    };
    // Source and destination together, if it may write.
    let may_store = form == Form::SearchStore
        || (form.takes_store_key()
            && (2..args.len()).any(|i| {
                let arg = args.get(i);
                arg.eq_ignore_ascii_case(b"STORE") || arg.eq_ignore_ascii_case(b"STOREDIST")
            }));
    if may_store {
        store.exclusive(|| run(session, out));
    } else {
        run(session, out);
    }
    Reply::Written
}

fn reply_or_store(
    store: &crate::store::Store,
    now: Millis,
    args: &Args<'_>,
    resp3: bool,
    found: Found,
    out: &mut Vec<u8>,
) {
    let Found {
        source_exists,
        mut points,
        shape,
        store_key,
        store_distance,
        with_distance,
        with_hash,
        with_coordinates,
        sort,
        count,
    } = found;
    if !source_exists {
        if let Some(at) = store_key {
            let _ = store.update_key(args.key_at(store, at), now, |_| (Update::Delete, ()));
            resp::write_integer(out, 0);
        } else {
            resp::write_array_header(out, 0);
        }
        return;
    }
    if points.is_empty() && store_key.is_none() {
        resp::write_array_header(out, 0);
        return;
    }
    let length = points.len();
    let returned = if count == 0 || (length as i64) < count {
        length
    } else {
        count as usize
    };
    if sort != Sort::None {
        let sign = if sort == Sort::Ascending { 1 } else { -1 };
        if returned == length {
            // C's `qsort`, which glibc makes a stable merge sort.
            points.sort_by(|a, b| {
                if sign > 0 {
                    a.distance.total_cmp(&b.distance)
                } else {
                    b.distance.total_cmp(&a.distance)
                }
            });
        } else {
            geo::partial_sort(&mut points, returned - 1, sign, |point| point.distance);
        }
    }
    points.truncate(returned);
    let Some(at) = store_key else {
        let options =
            usize::from(with_distance) + usize::from(with_hash) + usize::from(with_coordinates);
        resp::write_array_header(out, returned);
        for point in &points {
            if options != 0 {
                resp::write_array_header(out, options + 1);
            }
            resp::write_bulk(out, &point.member);
            if with_distance {
                let distance = point.distance / shape.conversion;
                resp::write_bulk(out, &geo::format_distance(distance));
            }
            if with_hash {
                resp::write_integer(out, point.score as i64);
            }
            if with_coordinates {
                resp::write_array_header(out, 2);
                write_coordinate(out, point.longitude, resp3);
                write_coordinate(out, point.latitude, resp3);
            }
        }
        return;
    };
    let key = args.key_at(store, at);
    let stored = store.update_collection(key, now, true, |_| {
        let mut set = BigZSet::default();
        for point in &points {
            let score = if store_distance {
                point.distance / shape.conversion
            } else {
                point.score
            };
            set.insert(&point.member, score);
        }
        (store_update(set), ())
    });
    match stored {
        Ok(()) => {
            store.forget_expiry(key, now);
            resp::write_integer(out, returned as i64);
        }
        Err(OutOfMemory) => resp::write_error(out, OOM),
    }
}

/// `GEORADIUS key longitude latitude radius unit [...]`
pub(super) fn georadius(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    search(c, s, n, a, o, 1, Form::Coordinates { read_only: false })
}

/// `GEORADIUS_RO`, which has no `STORE`.
pub(super) fn georadius_ro(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    search(c, s, n, a, o, 1, Form::Coordinates { read_only: true })
}

/// `GEORADIUSBYMEMBER key member radius unit [...]`
pub(super) fn georadiusbymember(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    search(c, s, n, a, o, 1, Form::Member { read_only: false })
}

/// `GEORADIUSBYMEMBER_RO`, which has no `STORE`.
pub(super) fn georadiusbymember_ro(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    search(c, s, n, a, o, 1, Form::Member { read_only: true })
}

/// `GEOSEARCH key FROMMEMBER member | FROMLONLAT long lat BYRADIUS radius unit | BYBOX width height unit [...]`
pub(super) fn geosearch(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    search(c, s, n, a, o, 1, Form::Search)
}

/// `GEOSEARCHSTORE destination source [...]`
pub(super) fn geosearchstore(
    c: &Context,
    s: &mut Session,
    n: Millis,
    a: &Args<'_>,
    o: &mut Vec<u8>,
) -> Reply {
    search(c, s, n, a, o, 2, Form::SearchStore)
}
