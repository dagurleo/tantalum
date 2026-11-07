//! Geohashes and the distances between points, as Redis 7.2.4's `geohash.c` and
//! `geohash_helper.c` work them out (they came from the ardb project), so that
//! scores, searches and the order results come in are the same. A point is a
//! sorted set member whose score is its position: 26 bits of longitude and 26 of
//! latitude, interleaved.

use std::f64::consts::PI;

pub(crate) const LONG_MIN: f64 = -180.0;
pub(crate) const LONG_MAX: f64 = 180.0;
pub(crate) const LAT_MIN: f64 = -85.05112878;
pub(crate) const LAT_MAX: f64 = 85.05112878;
/// Each coordinate's bits: 52 in all, which a double holds exactly.
pub(crate) const STEP_MAX: u8 = 26;

const D_R: f64 = PI / 180.0;
/// The Earth's quadratic mean radius for WGS-84.
const EARTH_RADIUS_IN_METERS: f64 = 6372797.560856;
const MERCATOR_MAX: f64 = 20037726.37;

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct Bits {
    pub(crate) bits: u64,
    pub(crate) step: u8,
}

impl Bits {
    fn is_zero(self) -> bool {
        self.bits == 0 && self.step == 0
    }

    /// As a whole score, the low bits of its 52 empty.
    pub(crate) fn align52(self) -> u64 {
        self.bits << (52 - u32::from(self.step) * 2)
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct Range {
    min: f64,
    max: f64,
}

#[derive(Clone, Copy, Debug, Default)]
struct Area {
    longitude: Range,
    latitude: Range,
}

const WORLD_LONG: Range = Range {
    min: LONG_MIN,
    max: LONG_MAX,
};
const WORLD_LAT: Range = Range {
    min: LAT_MIN,
    max: LAT_MAX,
};

/// The bits of `x` in the even positions and those of `y` in the odd.
fn interleave(x: u32, y: u32) -> u64 {
    fn spread(v: u32) -> u64 {
        let mut v = u64::from(v);
        v = (v | (v << 16)) & 0x0000_FFFF_0000_FFFF;
        v = (v | (v << 8)) & 0x00FF_00FF_00FF_00FF;
        v = (v | (v << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
        v = (v | (v << 2)) & 0x3333_3333_3333_3333;
        (v | (v << 1)) & 0x5555_5555_5555_5555
    }
    spread(x) | (spread(y) << 1)
}

/// Undoes [`interleave`]: `x` in the low half, `y` in the high.
fn deinterleave(bits: u64) -> u64 {
    fn gather(v: u64) -> u64 {
        let mut v = v & 0x5555_5555_5555_5555;
        v = (v | (v >> 1)) & 0x3333_3333_3333_3333;
        v = (v | (v >> 2)) & 0x0F0F_0F0F_0F0F_0F0F;
        v = (v | (v >> 4)) & 0x00FF_00FF_00FF_00FF;
        v = (v | (v >> 8)) & 0x0000_FFFF_0000_FFFF;
        (v | (v >> 16)) & 0x0000_0000_FFFF_FFFF
    }
    gather(bits) | (gather(bits >> 1) << 32)
}

fn encode_in(long_range: Range, lat_range: Range, longitude: f64, latitude: f64, step: u8) -> Bits {
    let lat_offset = (latitude - lat_range.min) / (lat_range.max - lat_range.min);
    let long_offset = (longitude - long_range.min) / (long_range.max - long_range.min);
    let scale = (1u64 << step) as f64;
    Bits {
        bits: interleave((lat_offset * scale) as u32, (long_offset * scale) as u32),
        step,
    }
}

/// A position within the range points can have, at `step` bits a coordinate.
pub(crate) fn encode(longitude: f64, latitude: f64, step: u8) -> Bits {
    encode_in(WORLD_LONG, WORLD_LAT, longitude, latitude, step)
}

/// The standard geohash's bits of a position: its latitudes run to 90, not 85.05.
pub(crate) fn encode_standard(longitude: f64, latitude: f64) -> u64 {
    let lat = Range {
        min: -90.0,
        max: 90.0,
    };
    let long = Range {
        min: -180.0,
        max: 180.0,
    };
    encode_in(long, lat, longitude, latitude, 26).bits
}

fn decode_area(hash: Bits) -> Option<Area> {
    if hash.is_zero() {
        return None;
    }
    let separate = deinterleave(hash.bits);
    let lat_scale = WORLD_LAT.max - WORLD_LAT.min;
    let long_scale = WORLD_LONG.max - WORLD_LONG.min;
    let ilato = separate as u32;
    let ilono = (separate >> 32) as u32;
    let steps = (1u64 << hash.step) as f64;
    Some(Area {
        latitude: Range {
            min: WORLD_LAT.min + (f64::from(ilato) / steps) * lat_scale,
            max: WORLD_LAT.min + (f64::from(ilato.wrapping_add(1)) / steps) * lat_scale,
        },
        longitude: Range {
            min: WORLD_LONG.min + (f64::from(ilono) / steps) * long_scale,
            max: WORLD_LONG.min + (f64::from(ilono.wrapping_add(1)) / steps) * long_scale,
        },
    })
}

/// A double as C casts it to an unsigned 64-bit number on x86-64: Redis does this
/// to any score it finds, which needn't be a position.
fn score_bits(score: f64) -> u64 {
    const TWO_63: f64 = 9_223_372_036_854_775_808.0;
    fn truncate(x: f64) -> i64 {
        if x.is_nan() || x >= TWO_63 || x < -TWO_63 {
            i64::MIN
        } else {
            x as i64
        }
    }
    if score < TWO_63 {
        truncate(score) as u64
    } else {
        (truncate(score - TWO_63) as u64) ^ (1 << 63)
    }
}

/// The longitude and latitude a score stands for.
pub(crate) fn decode_score(score: f64) -> Option<[f64; 2]> {
    let area = decode_area(Bits {
        bits: score_bits(score),
        step: STEP_MAX,
    })?;
    Some([
        ((area.longitude.min + area.longitude.max) / 2.0).clamp(LONG_MIN, LONG_MAX),
        ((area.latitude.min + area.latitude.max) / 2.0).clamp(LAT_MIN, LAT_MAX),
    ])
}

fn move_x(hash: &mut Bits, forward: bool) {
    let shift = 64 - u32::from(hash.step) * 2;
    let x = hash.bits & 0xaaaa_aaaa_aaaa_aaaa;
    let y = hash.bits & 0x5555_5555_5555_5555;
    let zz = 0x5555_5555_5555_5555_u64 >> shift;
    let x = if forward {
        x.wrapping_add(zz.wrapping_add(1))
    } else {
        (x | zz).wrapping_sub(zz.wrapping_add(1))
    };
    hash.bits = (x & (0xaaaa_aaaa_aaaa_aaaa_u64 >> shift)) | y;
}

fn move_y(hash: &mut Bits, forward: bool) {
    let shift = 64 - u32::from(hash.step) * 2;
    let x = hash.bits & 0xaaaa_aaaa_aaaa_aaaa;
    let y = hash.bits & 0x5555_5555_5555_5555;
    let zz = 0xaaaa_aaaa_aaaa_aaaa_u64 >> shift;
    let y = if forward {
        y.wrapping_add(zz.wrapping_add(1))
    } else {
        (y | zz).wrapping_sub(zz.wrapping_add(1))
    };
    hash.bits = x | (y & (0x5555_5555_5555_5555_u64 >> shift));
}

/// What to search around: a circle, or a box (`width` and `height`), in the unit
/// `conversion` turns into meters.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Shape {
    pub(crate) circular: bool,
    pub(crate) xy: [f64; 2],
    pub(crate) conversion: f64,
    pub(crate) radius: f64,
    pub(crate) width: f64,
    pub(crate) height: f64,
}

impl Shape {
    /// Whether a point is in it, and how far from its center.
    pub(crate) fn distance_within(&self, xy: [f64; 2]) -> Option<f64> {
        let [x1, y1] = self.xy;
        let [x2, y2] = xy;
        if self.circular {
            let distance = distance(x1, y1, x2, y2);
            (distance <= self.radius * self.conversion || distance.is_nan()).then_some(distance)
        } else {
            if lat_distance(y2, y1) > self.height * self.conversion / 2.0 {
                return None;
            }
            if distance(x2, y2, x1, y2) > self.width * self.conversion / 2.0 {
                return None;
            }
            Some(distance(x1, y1, x2, y2))
        }
    }

    /// Longitude and latitude, least and most, of what it covers.
    fn bounds(&self) -> [f64; 4] {
        let [longitude, latitude] = self.xy;
        let (height, width) = if self.circular {
            (self.radius, self.radius)
        } else {
            (self.height / 2.0, self.width / 2.0)
        };
        let (height, width) = (self.conversion * height, self.conversion * width);
        let lat_delta = (height / EARTH_RADIUS_IN_METERS) / D_R;
        let long_delta_top =
            (width / EARTH_RADIUS_IN_METERS / ((latitude + lat_delta) * D_R).cos()) / D_R;
        let long_delta_bottom =
            (width / EARTH_RADIUS_IN_METERS / ((latitude - lat_delta) * D_R).cos()) / D_R;
        let long_delta = if latitude < 0.0 {
            long_delta_bottom
        } else {
            long_delta_top
        };
        [
            longitude - long_delta,
            latitude - lat_delta,
            longitude + long_delta,
            latitude + lat_delta,
        ]
    }
}

/// The bits a coordinate needs for 9 boxes to cover a search this wide.
fn estimate_steps(range_meters: f64, latitude: f64) -> u8 {
    if range_meters == 0.0 {
        return 26;
    }
    let mut range_meters = range_meters;
    let mut step: i32 = 1;
    while range_meters < MERCATOR_MAX {
        range_meters *= 2.0;
        step += 1;
    }
    step -= 2;
    // Wider towards the poles.
    if !(-66.0..=66.0).contains(&latitude) {
        step -= 1;
        if !(-80.0..=80.0).contains(&latitude) {
            step -= 1;
        }
    }
    step.clamp(1, 26) as u8
}

/// The boxes to search, in the order Redis searches them, which is the order
/// results come in unless sorted: the center's, then the neighbours that matter.
pub(crate) fn search_boxes(shape: &Shape) -> [Bits; 9] {
    let [min_lon, min_lat, max_lon, max_lat] = shape.bounds();
    let [longitude, latitude] = shape.xy;
    let radius = if shape.circular {
        shape.radius
    } else {
        let (half_width, half_height) = (shape.width / 2.0, shape.height / 2.0);
        (half_width * half_width + half_height * half_height).sqrt()
    };
    let mut steps = estimate_steps(radius * shape.conversion, latitude);

    let neighbours = |hash: Bits| {
        let moved = |x: Option<bool>, y: Option<bool>| {
            let mut neighbour = hash;
            if let Some(forward) = x {
                move_x(&mut neighbour, forward);
            }
            if let Some(forward) = y {
                move_y(&mut neighbour, forward);
            }
            neighbour
        };
        (
            moved(None, Some(true)),         // north
            moved(None, Some(false)),        // south
            moved(Some(true), None),         // east
            moved(Some(false), None),        // west
            moved(Some(true), Some(true)),   // north east
            moved(Some(false), Some(true)),  // north west
            moved(Some(true), Some(false)),  // south east
            moved(Some(false), Some(false)), // south west
        )
    };

    let mut hash = encode(longitude, latitude, steps);
    let mut around = neighbours(hash);
    // Near the edge of its box, one more bit may be too many.
    let (north, south, east, west) = (
        decode_area(around.0),
        decode_area(around.1),
        decode_area(around.2),
        decode_area(around.3),
    );
    let too_small = north.is_some_and(|a| a.latitude.max < max_lat)
        || south.is_some_and(|a| a.latitude.min > min_lat)
        || east.is_some_and(|a| a.longitude.max < max_lon)
        || west.is_some_and(|a| a.longitude.min > min_lon);
    if steps > 1 && too_small {
        steps -= 1;
        hash = encode(longitude, latitude, steps);
        around = neighbours(hash);
    }
    let area = decode_area(hash).unwrap_or_default();
    let (mut n, mut s, mut e, mut w, mut ne, mut nw, mut se, mut sw) = around;
    let none = Bits::default();
    // Those of no use.
    if steps >= 2 {
        if area.latitude.min < min_lat {
            (s, sw, se) = (none, none, none);
        }
        if area.latitude.max > max_lat {
            (n, ne, nw) = (none, none, none);
        }
        if area.longitude.min < min_lon {
            (w, sw, nw) = (none, none, none);
        }
        if area.longitude.max > max_lon {
            (e, se, ne) = (none, none, none);
        }
    }
    [hash, n, s, e, w, ne, nw, se, sw]
}

/// The lowest score in a box, and the lowest above it.
pub(crate) fn score_range(hash: Bits) -> (f64, f64) {
    let next = Bits {
        bits: hash.bits + 1,
        step: hash.step,
    };
    (hash.align52() as f64, next.align52() as f64)
}

/// Whether a box is one of those left out of a search.
pub(crate) fn is_empty_box(hash: Bits) -> bool {
    hash.is_zero()
}

fn lat_distance(lat1: f64, lat2: f64) -> f64 {
    EARTH_RADIUS_IN_METERS * (lat2 * D_R - lat1 * D_R).abs()
}

/// In meters, along the great circle (the haversine formula).
pub(crate) fn distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let (lon1r, lon2r) = (lon1 * D_R, lon2 * D_R);
    let v = ((lon2r - lon1r) / 2.0).sin();
    if v == 0.0 {
        return lat_distance(lat1, lat2);
    }
    let (lat1r, lat2r) = (lat1 * D_R, lat2 * D_R);
    let u = ((lat2r - lat1r) / 2.0).sin();
    let a = u * u + lat1r.cos() * lat2r.cos() * v * v;
    2.0 * EARTH_RADIUS_IN_METERS * a.sqrt().asin()
}

/// A distance as Redis replies it, with four digits after the point
/// (`fixedpoint_d2string`): rounded by `llrint`, half to even.
pub(crate) fn format_distance(value: f64) -> Vec<u8> {
    if value == 0.0 {
        return b"0.0000".to_vec();
    }
    let scaled = value * 10000.0;
    let rounded = if scaled.is_nan() || scaled.abs() >= 9.2e18 {
        i64::MIN
    } else {
        scaled.round_ties_even() as i64
    };
    let magnitude = rounded.unsigned_abs();
    let sign = if rounded < 0 { "-" } else { "" };
    format!("{sign}{}.{:04}", magnitude / 10000, magnitude % 10000).into_bytes()
}

/// A coordinate as Redis replies it: `%.17Lf` without the zeroes at the end.
pub(crate) fn format_coordinate(value: f64) -> Vec<u8> {
    let mut text = format!("{value:.17}");
    if text.contains('.') {
        text.truncate(text.trim_end_matches('0').trim_end_matches('.').len());
    }
    if text == "-0" {
        text = "0".to_string();
    }
    text.into_bytes()
}

/// A distance comparison as Redis' `qsort` callback makes it.
fn compare(a: f64, b: f64) -> i32 {
    if a > b {
        1
    } else if a == b {
        0
    } else {
        -1
    }
}

/// Redis' `pqsort`, a quicksort that only sorts as far as it must for `0..=upto`
/// to be in order: reimplemented step for step, since which of several points at
/// the same distance make the cut depends on it. `distance` gives what to compare
/// by, in the order wanted (negated for descending).
pub(crate) fn partial_sort<T>(
    items: &mut [T],
    upto: usize,
    sign: i32,
    distance: impl Fn(&T) -> f64,
) {
    let cmp =
        |items: &[T], a: usize, b: usize| sign * compare(distance(&items[a]), distance(&items[b]));
    pqsort(items, 0, items.len(), upto, &cmp);
}

fn med3<T>(
    items: &[T],
    a: usize,
    b: usize,
    c: usize,
    cmp: &impl Fn(&[T], usize, usize) -> i32,
) -> usize {
    if cmp(items, a, b) < 0 {
        if cmp(items, b, c) < 0 {
            b
        } else if cmp(items, a, c) < 0 {
            c
        } else {
            a
        }
    } else if cmp(items, b, c) > 0 {
        b
    } else if cmp(items, a, c) < 0 {
        a
    } else {
        c
    }
}

fn vecswap<T>(items: &mut [T], a: usize, b: usize, n: usize) {
    for i in 0..n {
        items.swap(a + i, b + i);
    }
}

fn pqsort<T>(
    items: &mut [T],
    mut a: usize,
    mut n: usize,
    upto: usize,
    cmp: &impl Fn(&[T], usize, usize) -> i32,
) {
    loop {
        if n < 7 {
            for pm in a + 1..a + n {
                let mut pl = pm;
                while pl > a && cmp(items, pl - 1, pl) > 0 {
                    items.swap(pl, pl - 1);
                    pl -= 1;
                }
            }
            return;
        }
        let mut pm = a + n / 2;
        if n > 7 {
            let mut pl = a;
            let mut pn = a + n - 1;
            if n > 40 {
                let d = n / 8;
                pl = med3(items, pl, pl + d, pl + 2 * d, cmp);
                pm = med3(items, pm - d, pm, pm + d, cmp);
                pn = med3(items, pn - 2 * d, pn - d, pn, cmp);
            }
            pm = med3(items, pl, pm, pn, cmp);
        }
        items.swap(a, pm);
        let (mut pa, mut pb) = (a + 1, a + 1);
        let (mut pc, mut pd) = (a + n - 1, a + n - 1);
        loop {
            while pb <= pc {
                let order = cmp(items, pb, a);
                if order > 0 {
                    break;
                }
                if order == 0 {
                    items.swap(pa, pb);
                    pa += 1;
                }
                pb += 1;
            }
            while pb <= pc {
                let order = cmp(items, pc, a);
                if order < 0 {
                    break;
                }
                if order == 0 {
                    items.swap(pc, pd);
                    pd -= 1;
                }
                pc -= 1;
            }
            if pb > pc {
                break;
            }
            items.swap(pb, pc);
            pb += 1;
            pc -= 1;
        }
        let pn = a + n;
        let r = (pa - a).min(pb - pa);
        vecswap(items, a, pb - r, r);
        let r = (pd - pc).min(pn - pd - 1);
        vecswap(items, pb, pn - r, r);
        // Each side only if it holds part of what must end up sorted.
        let left = pb - pa;
        if left > 1 && a <= upto {
            pqsort(items, a, left, upto, cmp);
        }
        let right = pd - pc;
        if right <= 1 {
            return;
        }
        a = pn - right;
        n = right;
        if upto < a {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interleaves_and_back() {
        for (x, y) in [
            (0, 0),
            (1, 0),
            (0, 1),
            (0xffff_ffff, 0),
            (0x1234_5678, 0x9abc_def0),
        ] {
            assert_eq!(
                deinterleave(interleave(x, y)),
                u64::from(x) | u64::from(y) << 32
            );
        }
    }

    #[test]
    fn a_position_comes_back_close() {
        for (longitude, latitude) in [
            (13.361389, 38.115556),
            (-122.4, 37.8),
            (0.0, 0.0),
            (180.0, 85.0),
        ] {
            let score = encode(longitude, latitude, STEP_MAX).align52() as f64;
            let [x, y] = decode_score(score).unwrap();
            assert!((x - longitude).abs() < 1e-5 && (y - latitude).abs() < 1e-5);
        }
    }

    /// From Redis: `GEOADD s 13.361389 38.115556 Palermo`, `ZSCORE`, `GEODIST`.
    #[test]
    fn same_numbers_as_redis() {
        assert_eq!(
            encode(13.361389, 38.115556, STEP_MAX).align52(),
            3479099956230698
        );
        assert_eq!(
            encode(15.087269, 37.502669, STEP_MAX).align52(),
            3479447370796909
        );
        let palermo = decode_score(3479099956230698.0).unwrap();
        let catania = decode_score(3479447370796909.0).unwrap();
        assert_eq!(
            format_distance(distance(palermo[0], palermo[1], catania[0], catania[1])),
            b"166274.1516"
        );
        assert_eq!(format_coordinate(palermo[0]), b"13.36138933897018433");
        assert_eq!(format_coordinate(palermo[1]), b"38.11555639549629859");
    }

    #[test]
    fn formats_distances() {
        assert_eq!(format_distance(0.0), b"0.0000");
        assert_eq!(format_distance(0.0001), b"0.0001");
        assert_eq!(format_distance(6.06429515983917e-5), b"0.0001");
        assert_eq!(format_distance(0.01), b"0.0100");
        assert_eq!(format_distance(1.0), b"1.0000");
        assert_eq!(format_distance(1234.56789), b"1234.5679");
        assert_eq!(format_distance(-1.5), b"-1.5000");
    }

    #[test]
    fn formats_coordinates() {
        assert_eq!(format_coordinate(0.0), b"0");
        assert_eq!(format_coordinate(-0.0), b"0");
        assert_eq!(format_coordinate(1.5), b"1.5");
        assert_eq!(format_coordinate(-180.0), b"-180");
    }

    #[test]
    fn scores_that_are_not_positions_cast_as_c_does() {
        assert_eq!(score_bits(1.0), 1);
        assert_eq!(score_bits(-1.0), u64::MAX);
        assert_eq!(score_bits(1e19), 10_000_000_000_000_000_000);
        assert_eq!(score_bits(f64::INFINITY), 0);
        assert!(decode_score(-1.0).is_some());
    }

    #[test]
    fn a_partial_sort_sorts_what_it_must() {
        let mut state = 12345u32;
        for n in [0, 1, 6, 7, 8, 40, 41, 100, 1000] {
            let mut items: Vec<f64> = (0..n)
                .map(|_| {
                    state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                    f64::from(state >> 24)
                })
                .collect();
            if n == 0 {
                continue;
            }
            let mut sorted = items.clone();
            sorted.sort_by(f64::total_cmp);
            let upto = n / 3;
            partial_sort(&mut items, upto, 1, |x| *x);
            assert_eq!(items[..=upto], sorted[..=upto], "{n}");
            let mut rest = items.clone();
            rest.sort_by(f64::total_cmp);
            assert_eq!(rest, sorted, "nothing lost");
        }
    }

    #[test]
    fn boxes_cover_a_search() {
        let shape = Shape {
            circular: true,
            xy: [15.0, 37.0],
            conversion: 1000.0,
            radius: 200.0,
            ..Shape::default()
        };
        let boxes = search_boxes(&shape);
        assert_eq!(boxes[0].step, boxes.iter().map(|b| b.step).max().unwrap());
        assert!(!is_empty_box(boxes[0]));
        let [x, y] = shape.xy;
        let hash = encode(x, y, boxes[0].step);
        assert_eq!(hash, boxes[0]);
    }
}
