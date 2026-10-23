//! Geopoly SQL functions, ported from SQLite's `ext/rtree/geopoly.c`.
//!
//! Stock keeps every coordinate as a 32-bit float (`GeoCoord`) and does part of
//! its arithmetic in float precision. These functions take [`Point`]s but round
//! each coordinate to `f32` where stock does, so blobs, JSON and SVG text, areas
//! and overlap codes match a stock build exactly
//! (`crates/fsqlite-core/tests/geopoly_stock_oracle.rs`).

use std::sync::Arc;

use fsqlite_error::Result;
use fsqlite_func::{AggregateFunction, FunctionRegistry, ScalarFunction};
use fsqlite_types::value::format_sqlite_float_g;
use fsqlite_types::{SmallText, SqliteValue};

use crate::{BoundingBox, Point};

/// The smallest polygon blob stock accepts: a 4-byte header and three vertices.
const GEOPOLY_MIN_BLOB_BYTES: usize = 4 + 6 * 4;

/// `geopoly_regular()` never makes more vertices than this.
const GEOPOLY_REGULAR_MAX_SIDES: usize = 1000;

/// Stock's `GEOPOLY_PI` (3.1415926535897932385) as a double.
const GEOPOLY_PI: f64 = std::f64::consts::PI;

/// A coordinate as stock stores it.
fn geo(value: f64) -> f32 {
    value as f32
}

// ---------------------------------------------------------------------------
// Binary and text formats
// ---------------------------------------------------------------------------

/// Encode a polygon in stock's binary format.
///
/// Header byte 1 (little-endian coordinates), the vertex count as 3 big-endian
/// bytes, then `x, y` for each vertex as little-endian `f32`s. The closing
/// vertex is not repeated.
#[must_use]
pub fn geopoly_blob(vertices: &[Point]) -> Vec<u8> {
    let count = vertices.len() as u32;
    let mut blob = Vec::with_capacity(4 + vertices.len() * 8);
    blob.push(1);
    blob.extend_from_slice(&count.to_be_bytes()[1..]);
    for vertex in vertices {
        blob.extend_from_slice(&geo(vertex.x).to_le_bytes());
        blob.extend_from_slice(&geo(vertex.y).to_le_bytes());
    }
    blob
}

/// Decode a stock geopoly blob.
///
/// Header byte 1 means little-endian coordinates and 0 big-endian; the blob
/// must hold at least three vertices and exactly as many bytes as its vertex
/// count says. Returns `None` otherwise.
#[must_use]
pub fn geopoly_blob_decode(blob: &[u8]) -> Option<Vec<Point>> {
    if blob.len() < GEOPOLY_MIN_BLOB_BYTES {
        return None;
    }
    let little_endian = match blob[0] {
        1 => true,
        0 => false,
        _ => return None,
    };
    let count = u32::from_be_bytes([0, blob[1], blob[2], blob[3]]) as usize;
    if count * 8 + 4 != blob.len() {
        return None;
    }
    let coord = |raw: [u8; 4]| {
        f64::from(if little_endian {
            f32::from_le_bytes(raw)
        } else {
            f32::from_be_bytes(raw)
        })
    };
    let (pairs, _) = blob[4..].as_chunks::<8>();
    Some(
        pairs
            .iter()
            .map(|&[x0, x1, x2, x3, y0, y1, y2, y3]| {
                Point::new(coord([x0, x1, x2, x3]), coord([y0, y1, y2, y3]))
            })
            .collect(),
    )
}

/// What stock `geopolyFuncParam()` makes of one SQL argument.
enum PolygonParam {
    Polygon(Vec<Point>),
    /// Not a polygon, but stock leaves its error code at `SQLITE_OK`: a blob of
    /// 28 bytes or more with a bad header or length, or text that does not
    /// start with `[`. `geopoly_group_bbox()` counts such a value as an
    /// all-zero box.
    NotPolygonOk,
    /// Not a polygon, with an error code: NULL, a number, a shorter blob, or
    /// malformed JSON.
    NotPolygon,
}

/// The bytes stock reads through `sqlite3_value_text()`: those before the
/// first NUL.
fn until_nul(bytes: &[u8]) -> &[u8] {
    bytes
        .iter()
        .position(|&byte| byte == 0)
        .map_or(bytes, |end| &bytes[..end])
}

/// Geopoly whitespace: tab, newline, carriage return and space (not `\v` or
/// `\f`).
fn skip_geopoly_space(text: &[u8], mut pos: usize) -> usize {
    while matches!(text.get(pos), Some(b'\t' | b'\n' | b'\r' | b' ')) {
        pos += 1;
    }
    pos
}

enum GeoNumber {
    Number {
        len: usize,
        value: f64,
    },
    NotNumber,
    /// Stock's `-1` (a second exponent). Its caller always fails the parse.
    Malformed,
}

/// Port of `geopolyParseNumber()` for the token at `start`. Its checks of
/// `z[j-1]` can read the byte before the token, which always exists because a
/// number follows at least `[[`. That is why `[[e5,0],...` reads x as 0 while
/// `[[ e5,0],...` is malformed, as in stock.
fn parse_geopoly_number(text: &[u8], start: usize) -> GeoNumber {
    let at = |j: usize| text.get(start + j).copied().unwrap_or(0);
    let before = |j: usize| text.get((start + j).wrapping_sub(1)).copied().unwrap_or(0);
    let mut j = usize::from(at(0) == b'-');
    if at(j) == b'0' && at(j + 1).is_ascii_digit() {
        return GeoNumber::NotNumber;
    }
    let mut seen_dp = false;
    let mut seen_e = false;
    loop {
        let c = at(j);
        if c == b'.' {
            if before(j) == b'-' || seen_dp {
                return GeoNumber::NotNumber;
            }
            seen_dp = true;
        } else if c == b'e' || c == b'E' {
            if before(j) < b'0' {
                return GeoNumber::NotNumber;
            }
            if seen_e {
                return GeoNumber::Malformed;
            }
            seen_dp = true;
            seen_e = true;
            if matches!(at(j + 1), b'+' | b'-') {
                j += 1;
            }
            if !at(j + 1).is_ascii_digit() {
                return GeoNumber::NotNumber;
            }
        } else if !c.is_ascii_digit() {
            break;
        }
        j += 1;
    }
    if before(j) < b'0' {
        return GeoNumber::NotNumber;
    }
    // Stock converts with `sqlite3AtoF()`, which reads 0 for the digit-less
    // tokens the grammar lets through: an empty token or a bare exponent.
    let value = std::str::from_utf8(&text[start..start + j])
        .ok()
        .and_then(|token| token.parse::<f64>().ok())
        .unwrap_or(0.0);
    GeoNumber::Number { len: j, value }
}

/// Port of `geopolyParseJson()`: `[[x,y],...]` with at least four vertices and
/// the last equal to the first (compared as `f32`), which is then dropped.
#[allow(clippy::float_cmp)] // stock compares the closing vertex exactly
fn parse_geopoly_json(text: &[u8]) -> PolygonParam {
    let byte = |pos: usize| text.get(pos).copied().unwrap_or(0);
    let mut pos = skip_geopoly_space(text, 0);
    if byte(pos) != b'[' {
        return PolygonParam::NotPolygonOk;
    }
    pos += 1;
    let mut coords: Vec<f32> = Vec::new();
    loop {
        pos = skip_geopoly_space(text, pos);
        if byte(pos) != b'[' {
            break;
        }
        pos += 1;
        let mut vertex = [0.0_f32; 2];
        let mut values = 0usize;
        loop {
            pos = skip_geopoly_space(text, pos);
            let (len, value) = match parse_geopoly_number(text, pos) {
                GeoNumber::Number { len, value } => (len, value),
                GeoNumber::NotNumber => break,
                GeoNumber::Malformed => return PolygonParam::NotPolygon,
            };
            // Values after the first two are parsed and ignored.
            if let Some(slot) = vertex.get_mut(values) {
                *slot = geo(value);
            }
            pos += len;
            values += 1;
            if values == 2 {
                coords.extend_from_slice(&vertex);
            }
            pos = skip_geopoly_space(text, pos);
            let c = byte(pos);
            pos += 1;
            if c == b',' {
                continue;
            }
            if c == b']' && values >= 2 {
                break;
            }
            return PolygonParam::NotPolygon;
        }
        pos = skip_geopoly_space(text, pos);
        if byte(pos) == b',' {
            pos += 1;
            continue;
        }
        break;
    }
    pos = skip_geopoly_space(text, pos);
    let count = coords.len() / 2;
    let closed =
        count >= 4 && coords[0] == coords[2 * count - 2] && coords[1] == coords[2 * count - 1];
    if byte(pos) == b']' && closed && skip_geopoly_space(text, pos + 1) >= text.len() {
        coords.truncate(2 * (count - 1));
        let (pairs, _) = coords.as_chunks::<2>();
        return PolygonParam::Polygon(
            pairs
                .iter()
                .map(|&[x, y]| Point::new(f64::from(x), f64::from(y)))
                .collect(),
        );
    }
    PolygonParam::NotPolygon
}

/// Parse stock geopoly JSON text, `[[x0,y0],[x1,y1],...,[x0,y0]]`: at least
/// three vertices and a closing copy of the first, which is dropped. Returns
/// `None` for anything stock rejects.
#[must_use]
pub fn geopoly_json_decode(json: &str) -> Option<Vec<Point>> {
    match parse_geopoly_json(until_nul(json.as_bytes())) {
        PolygonParam::Polygon(vertices) => Some(vertices),
        PolygonParam::NotPolygonOk | PolygonParam::NotPolygon => None,
    }
}

/// One coordinate as stock's `%!g` (`alternate_form`, for `geopoly_json()`) or
/// `%g` (for `geopoly_svg()`): six significant digits, and `%!g` keeps a `.0`
/// on whole numbers.
fn geopoly_coord_text(value: f32, alternate_form: bool) -> String {
    let text = format_sqlite_float_g(f64::from(value), 6);
    if alternate_form {
        return text;
    }
    if let Some(whole) = text.strip_suffix(".0") {
        return whole.to_owned();
    }
    text.replacen(".0e", "e", 1)
}

/// Render a polygon as stock geopoly JSON: every vertex as `[x,y]` with
/// `%!g` coordinates, then the first vertex again to close the ring.
#[must_use]
pub fn geopoly_json(vertices: &[Point]) -> String {
    let items: Vec<String> = vertices
        .iter()
        .chain(vertices.first())
        .map(|vertex| {
            format!(
                "[{},{}]",
                geopoly_coord_text(geo(vertex.x), true),
                geopoly_coord_text(geo(vertex.y), true)
            )
        })
        .collect();
    format!("[{}]", items.join(","))
}

/// `<polyline points='x0,y0 x1,y1 ... x0,y0'`, the part of `geopoly_svg()`
/// before any attributes.
fn geopoly_svg_points(vertices: &[Point]) -> String {
    let mut out = String::from("<polyline points=");
    let mut separator = '\'';
    for vertex in vertices.iter().chain(vertices.first()) {
        out.push(separator);
        out.push_str(&geopoly_coord_text(geo(vertex.x), false));
        out.push(',');
        out.push_str(&geopoly_coord_text(geo(vertex.y), false));
        separator = ' ';
    }
    out.push('\'');
    out
}

/// Render a polygon as stock's SVG element:
/// `<polyline points='x0,y0 x1,y1 ... x0,y0'></polyline>`.
#[must_use]
pub fn geopoly_svg(vertices: &[Point]) -> String {
    geopoly_svg_with_attributes(vertices, &[])
}

/// [`geopoly_svg`] with attributes, as stock appends its extra arguments: each
/// non-empty one is added as ` attribute`.
#[must_use]
pub fn geopoly_svg_with_attributes(vertices: &[Point], attributes: &[&str]) -> String {
    let mut out = geopoly_svg_points(vertices);
    for attribute in attributes.iter().filter(|attribute| !attribute.is_empty()) {
        out.push(' ');
        out.push_str(attribute);
    }
    out.push_str("></polyline>");
    out
}

// ---------------------------------------------------------------------------
// Geometry
// ---------------------------------------------------------------------------

/// Stock `geopolyBBox()`: the extremes of the `f32` coordinates. It compares
/// the way stock does, so a NaN first vertex stays NaN.
#[must_use]
pub fn geopoly_bbox(vertices: &[Point]) -> Option<BoundingBox> {
    let (first, rest) = vertices.split_first()?;
    let (x, y) = (f64::from(geo(first.x)), f64::from(geo(first.y)));
    let mut bounds = BoundingBox {
        min_x: x,
        min_y: y,
        max_x: x,
        max_y: y,
    };
    for vertex in rest {
        let x = f64::from(geo(vertex.x));
        if x < bounds.min_x {
            bounds.min_x = x;
        } else if x > bounds.max_x {
            bounds.max_x = x;
        }
        let y = f64::from(geo(vertex.y));
        if y < bounds.min_y {
            bounds.min_y = y;
        } else if y > bounds.max_y {
            bounds.max_y = y;
        }
    }
    Some(bounds)
}

/// Widen `bounds` to cover `next`, as `geopoly_group_bbox()` does.
fn extend_bbox(bounds: &mut BoundingBox, next: BoundingBox) {
    if next.min_x < bounds.min_x {
        bounds.min_x = next.min_x;
    }
    if next.max_x > bounds.max_x {
        bounds.max_x = next.max_x;
    }
    if next.min_y < bounds.min_y {
        bounds.min_y = next.min_y;
    }
    if next.max_y > bounds.max_y {
        bounds.max_y = next.max_y;
    }
}

/// The bounding box of several polygons, as `geopoly_group_bbox()` computes it.
#[must_use]
pub fn geopoly_group_bbox(polygons: &[&[Point]]) -> Option<BoundingBox> {
    let mut boxes = polygons.iter().filter_map(|polygon| geopoly_bbox(polygon));
    let mut bounds = boxes.next()?;
    for next in boxes {
        extend_bbox(&mut bounds, next);
    }
    Some(bounds)
}

/// The rectangle stock returns for a bounding box: `(min_x, min_y)`,
/// `(max_x, min_y)`, `(max_x, max_y)`, `(min_x, max_y)`.
#[must_use]
pub fn geopoly_bbox_polygon(bounds: BoundingBox) -> [Point; 4] {
    [
        Point::new(bounds.min_x, bounds.min_y),
        Point::new(bounds.max_x, bounds.min_y),
        Point::new(bounds.max_x, bounds.max_y),
        Point::new(bounds.min_x, bounds.max_y),
    ]
}

/// Stock `geopolyArea()`: the enclosed area, negative for a clockwise polygon.
/// Each term's difference, sum and product are `f32` arithmetic, as in stock.
#[must_use]
pub fn geopoly_area(vertices: &[Point]) -> f64 {
    let (Some(&first), Some(&last)) = (vertices.first(), vertices.last()) else {
        return 0.0;
    };
    let term = |a: Point, b: Point| f64::from((geo(a.x) - geo(b.x)) * (geo(a.y) + geo(b.y))) * 0.5;
    let mut area = 0.0;
    for pair in vertices.windows(2) {
        area += term(pair[0], pair[1]);
    }
    area + term(last, first)
}

/// Stock `geopoly_ccw()`: a clockwise polygon (negative area) has every vertex
/// but the first reversed, so it winds counter-clockwise.
#[must_use]
pub fn geopoly_ccw(vertices: &[Point]) -> Vec<Point> {
    let mut out = vertices.to_vec();
    if geopoly_area(vertices) < 0.0
        && let Some(rest) = out.get_mut(1..)
    {
        rest.reverse();
    }
    out
}

/// Stock's polynomial sine approximation (`geopolySine()`) for `r` in
/// `[-pi/2, 2*pi]`.
#[allow(clippy::suboptimal_flops)] // stock evaluates the polynomial unfused
fn geopoly_sine(r: f64) -> f64 {
    let r = if r >= 1.5 * GEOPOLY_PI {
        r - 2.0 * GEOPOLY_PI
    } else {
        r
    };
    if r >= 0.5 * GEOPOLY_PI {
        return -geopoly_sine(r - GEOPOLY_PI);
    }
    let r2 = r * r;
    let r3 = r2 * r;
    let r5 = r3 * r2;
    0.9996949 * r - 0.1656700 * r3 + 0.0075134 * r5
}

/// Stock `geopoly_regular(X, Y, R, N)`: a regular `N`-gon around `(X, Y)`.
///
/// The circumradius is `R`, computed with stock's sine approximation, so a
/// unit square's vertices sit at 1.00007. Empty when `N < 3` or `R <= 0`; `N`
/// is capped at 1000.
#[must_use]
#[allow(clippy::suboptimal_flops)] // stock computes the vertices unfused
pub fn geopoly_regular(center_x: f64, center_y: f64, radius: f64, sides: usize) -> Vec<Point> {
    if sides < 3 || radius <= 0.0 {
        return Vec::new();
    }
    let sides = sides.min(GEOPOLY_REGULAR_MAX_SIDES);
    (0..sides)
        .map(|index| {
            let angle = 2.0 * GEOPOLY_PI * index as f64 / sides as f64;
            Point::new(
                f64::from(geo(
                    center_x - radius * geopoly_sine(angle - 0.5 * GEOPOLY_PI)
                )),
                f64::from(geo(center_y + radius * geopoly_sine(angle))),
            )
        })
        .collect()
}

/// Stock `geopoly_xform(P, A, B, C, D, E, F)`: each vertex becomes
/// `(A*x + B*y + E, C*x + D*y + F)`, computed in double and stored as `f32`.
#[must_use]
#[allow(clippy::many_single_char_names)] // Affine transform matrix coefficients (standard naming)
#[allow(clippy::suboptimal_flops)] // stock computes the transform unfused
pub fn geopoly_xform(
    vertices: &[Point],
    a: f64,
    b: f64,
    c: f64,
    d: f64,
    e: f64,
    f: f64,
) -> Vec<Point> {
    vertices
        .iter()
        .map(|vertex| {
            let x = f64::from(geo(vertex.x));
            let y = f64::from(geo(vertex.y));
            Point::new(
                f64::from(geo(a * x + b * y + e)),
                f64::from(geo(c * x + d * y + f)),
            )
        })
        .collect()
}

/// Stock `pointBeneathLine()`: 2 when `point` is on the segment `a`-`b`, 1 when
/// it is beneath it, otherwise 0. The segment's left end is not part of it, and
/// a vertical segment is never above a point.
#[allow(clippy::float_cmp)] // stock compares exactly
fn point_beneath_line(point: Point, a: (f64, f64), b: (f64, f64)) -> i32 {
    let (x0, y0) = (point.x, point.y);
    let ((x1, y1), (x2, y2)) = (a, b);
    if x0 == x1 && y0 == y1 {
        return 2;
    }
    if x1 < x2 {
        if x0 <= x1 || x0 > x2 {
            return 0;
        }
    } else if x1 > x2 {
        if x0 <= x2 || x0 > x1 {
            return 0;
        }
    } else {
        if x0 != x1 || (y0 < y1 && y0 < y2) || (y0 > y1 && y0 > y2) {
            return 0;
        }
        return 2;
    }
    let y = y1 + (y2 - y1) * (x0 - x1) / (x2 - x1);
    if y0 == y {
        2
    } else if y0 < y {
        1
    } else {
        0
    }
}

/// Stock `geopoly_contains_point()`: 2 when `point` is inside the polygon, 1
/// when it is on the boundary, 0 when it is outside.
#[must_use]
pub fn geopoly_contains_point(vertices: &[Point], point: Point) -> i64 {
    let Some(&first) = vertices.first() else {
        return 0;
    };
    let coords = |vertex: Point| (f64::from(geo(vertex.x)), f64::from(geo(vertex.y)));
    let mut verdict = 0;
    let mut beneath = 0;
    let mut last = vertices.len() - 1;
    for (index, pair) in vertices.windows(2).enumerate() {
        verdict = point_beneath_line(point, coords(pair[0]), coords(pair[1]));
        if verdict == 2 {
            last = index;
            break;
        }
        beneath += verdict;
    }
    if verdict != 2 {
        verdict = point_beneath_line(point, coords(vertices[last]), coords(first));
    }
    if verdict == 2 {
        1
    } else if ((verdict + beneath) & 1) == 0 {
        0
    } else {
        2
    }
}

/// One polygon edge in stock's overlap sweep (`GeoSegment`): `y = c * x + b`.
struct SweepSegment {
    c: f64,
    b: f64,
    /// The edge's y at the sweep's current x.
    y: f64,
    /// The y of the edge's left end.
    y0: f32,
    /// 1 for the first polygon, 2 for the second.
    side: u8,
}

/// `GeoEvent`: the sweep adds (`remove == false`) or removes `segment` at `x`.
struct SweepEvent {
    x: f64,
    remove: bool,
    segment: usize,
}

/// Stock `geopolyAddOneSegment()`: vertical edges take no part.
#[allow(clippy::float_cmp, clippy::suboptimal_flops)] // stock compares exactly, computes unfused
fn add_sweep_segment(
    segments: &mut Vec<SweepSegment>,
    events: &mut Vec<SweepEvent>,
    start: (f32, f32),
    end: (f32, f32),
    side: u8,
) {
    if start.0 == end.0 {
        return;
    }
    let ((x0, y0), (x1, y1)) = if start.0 > end.0 {
        (end, start)
    } else {
        (start, end)
    };
    let c = f64::from((y1 - y0) / (x1 - x0));
    let segment = segments.len();
    segments.push(SweepSegment {
        c,
        b: f64::from(y1) - f64::from(x1) * c,
        y: 0.0,
        y0,
        side,
    });
    events.push(SweepEvent {
        x: f64::from(x0),
        remove: false,
        segment,
    });
    events.push(SweepEvent {
        x: f64::from(x1),
        remove: true,
        segment,
    });
}

/// Stock `geopolyAddSegments()`: every edge, the closing one last.
fn add_polygon_segments(
    segments: &mut Vec<SweepSegment>,
    events: &mut Vec<SweepEvent>,
    vertices: &[Point],
    side: u8,
) {
    let coords = |vertex: &Point| (geo(vertex.x), geo(vertex.y));
    for pair in vertices.windows(2) {
        add_sweep_segment(segments, events, coords(&pair[0]), coords(&pair[1]), side);
    }
    if let (Some(first), Some(last)) = (vertices.first(), vertices.last()) {
        add_sweep_segment(segments, events, coords(last), coords(first), side);
    }
}

/// Stock's list merge: the head of `right` goes first when
/// `right_first(right_head, left_head)`.
fn merge_runs(
    left: Vec<usize>,
    right: Vec<usize>,
    right_first: &impl Fn(usize, usize) -> bool,
) -> Vec<usize> {
    let mut out = Vec::with_capacity(left.len() + right.len());
    let mut left = left.into_iter().peekable();
    let mut right = right.into_iter().peekable();
    while let (Some(&left_head), Some(&right_head)) = (left.peek(), right.peek()) {
        if right_first(right_head, left_head) {
            out.push(right_head);
            right.next();
        } else {
            out.push(left_head);
            left.next();
        }
    }
    out.extend(right);
    out.extend(left);
    out
}

/// Stock's bottom-up list merge sort (`geopolySortEventsByX()`,
/// `geopolySortSegmentsByYAndC()`), kept literal so ties fall in stock's order.
fn merge_sort(
    items: impl IntoIterator<Item = usize>,
    right_first: &impl Fn(usize, usize) -> bool,
) -> Vec<usize> {
    let mut slots: Vec<Option<Vec<usize>>> = Vec::new();
    for item in items {
        let mut run = vec![item];
        let mut level = 0;
        while let Some(slot) = slots.get_mut(level) {
            let Some(older) = slot.take() else {
                break;
            };
            run = merge_runs(older, run, right_first);
            level += 1;
        }
        if level == slots.len() {
            slots.push(Some(run));
        } else {
            slots[level] = Some(run);
        }
    }
    slots.into_iter().flatten().fold(Vec::new(), |sorted, run| {
        merge_runs(run, sorted, right_first)
    })
}

/// Stock `geopolyOverlap()`: a sweep over both polygons' edges.
#[allow(clippy::float_cmp, clippy::suboptimal_flops)] // stock compares exactly, computes unfused
fn geopoly_overlap_code(lhs: &[Point], rhs: &[Point]) -> i64 {
    let mut segments = Vec::new();
    let mut events = Vec::new();
    add_polygon_segments(&mut segments, &mut events, lhs, 1);
    add_polygon_segments(&mut segments, &mut events, rhs, 2);
    let order = merge_sort(0..events.len(), &|right, left| {
        events[right].x <= events[left].x
    });
    let mut sweep_x = match order.first() {
        Some(&first) if events[first].x == 0.0 => -1.0,
        _ => 0.0,
    };
    // Which side masks (1 = lhs only, 2 = rhs only, 3 = both) cover some area.
    let mut covered = [false; 4];
    let mut active: Vec<usize> = Vec::new();
    let mut need_sort = false;
    for &event_index in &order {
        let event = &events[event_index];
        if event.x != sweep_x {
            sweep_x = event.x;
            if need_sort {
                active = merge_sort(active.iter().copied(), &|right, left| {
                    let mut delta = segments[right].y - segments[left].y;
                    if delta == 0.0 {
                        delta = segments[right].c - segments[left].c;
                    }
                    delta < 0.0
                });
                need_sort = false;
            }
            let mut mask = 0usize;
            let mut previous_y: Option<f64> = None;
            for &segment in &active {
                let y = segments[segment].y;
                if previous_y.is_some_and(|previous| previous != y) {
                    covered[mask] = true;
                }
                mask ^= usize::from(segments[segment].side);
                previous_y = Some(y);
            }
            // Stock does not reset the mask between the two passes.
            let mut previous: Option<usize> = None;
            for &segment in &active {
                let y = segments[segment].c * sweep_x + segments[segment].b;
                segments[segment].y = y;
                if let Some(previous) = previous {
                    let previous = &segments[previous];
                    if previous.y > y && previous.side != segments[segment].side {
                        return 1;
                    }
                    if previous.y != y {
                        covered[mask] = true;
                    }
                }
                mask ^= usize::from(segments[segment].side);
                previous = Some(segment);
            }
        }
        if event.remove {
            if let Some(position) = active.iter().position(|&s| s == event.segment) {
                active.remove(position);
            }
        } else {
            let segment = &mut segments[event.segment];
            segment.y = f64::from(segment.y0);
            active.insert(0, event.segment);
            need_sort = true;
        }
    }
    match (covered[1], covered[2], covered[3]) {
        (_, _, false) => 0,
        (true, false, true) => 3,
        (false, true, true) => 2,
        (false, false, true) => 4,
        (true, true, true) => 1,
    }
}

/// Stock `geopoly_overlap()`: 0 when the polygons are disjoint, 1 when they
/// overlap, 2 when `lhs` is inside `rhs`, 3 when `rhs` is inside `lhs`, and 4
/// when they are the same polygon.
#[must_use]
pub fn geopoly_overlap(lhs: &[Point], rhs: &[Point]) -> i64 {
    geopoly_overlap_code(lhs, rhs)
}

/// Stock `geopoly_within()`: 1 when `inner` is inside `outer`, 2 when they are
/// the same polygon, otherwise 0.
#[must_use]
pub fn geopoly_within(inner: &[Point], outer: &[Point]) -> i64 {
    match geopoly_overlap_code(inner, outer) {
        2 => 1,
        4 => 2,
        _ => 0,
    }
}

// ---------------------------------------------------------------------------
// SQL functions
// ---------------------------------------------------------------------------

/// Port of `geopolyFuncParam()`: a blob or JSON text polygon.
fn polygon_param(value: &SqliteValue) -> PolygonParam {
    match value {
        SqliteValue::Blob(blob) if blob.len() >= GEOPOLY_MIN_BLOB_BYTES => {
            geopoly_blob_decode(blob).map_or(PolygonParam::NotPolygonOk, PolygonParam::Polygon)
        }
        SqliteValue::Text(text) => parse_geopoly_json(until_nul(text.as_bytes_direct())),
        _ => PolygonParam::NotPolygon,
    }
}

/// The polygon in argument `index`. Like stock, the geopoly functions return
/// NULL rather than an error when it is not one.
fn polygon_arg(args: &[SqliteValue], index: usize) -> Option<Vec<Point>> {
    match args.get(index).map(polygon_param) {
        Some(PolygonParam::Polygon(vertices)) => Some(vertices),
        _ => None,
    }
}

/// Argument `index` as stock's `sqlite3_value_double()` reads it.
fn float_arg(args: &[SqliteValue], index: usize) -> f64 {
    args.get(index).map_or(0.0, SqliteValue::to_float)
}

fn polygon_blob_value(vertices: &[Point]) -> SqliteValue {
    SqliteValue::Blob(Arc::from(geopoly_blob(vertices)))
}

/// `geopoly_blob(P)`: the polygon in binary form.
pub struct GeopolyBlobFunc;

impl ScalarFunction for GeopolyBlobFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(polygon_arg(args, 0).map_or(SqliteValue::Null, |p| polygon_blob_value(&p)))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "geopoly_blob"
    }
}

/// `geopoly_json(P)`: the polygon as JSON text.
pub struct GeopolyJsonFunc;

impl ScalarFunction for GeopolyJsonFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(polygon_arg(args, 0).map_or(SqliteValue::Null, |p| {
            SqliteValue::Text(geopoly_json(&p).into())
        }))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "geopoly_json"
    }
}

/// `geopoly_svg(P, ...)`: the polygon as an SVG `<polyline>`, with each
/// further non-empty argument appended as an attribute.
pub struct GeopolySvgFunc;

impl ScalarFunction for GeopolySvgFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let Some(vertices) = polygon_arg(args, 0) else {
            return Ok(SqliteValue::Null);
        };
        let mut out = geopoly_svg_points(&vertices).into_bytes();
        for arg in &args[1..] {
            // Stock appends `sqlite3_value_text()` of each, up to its first NUL.
            let attribute = match arg {
                SqliteValue::Null => Vec::new(),
                SqliteValue::Text(text) => until_nul(text.as_bytes_direct()).to_vec(),
                SqliteValue::Blob(blob) => until_nul(blob).to_vec(),
                other => other.to_text().into_bytes(),
            };
            if !attribute.is_empty() {
                out.push(b' ');
                out.extend_from_slice(&attribute);
            }
        }
        out.extend_from_slice(b"></polyline>");
        Ok(SqliteValue::Text(SmallText::from_bytes(&out)))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "geopoly_svg"
    }
}

/// `geopoly_area(P)`: the signed area (negative when clockwise).
pub struct GeopolyAreaFunc;

impl ScalarFunction for GeopolyAreaFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(
            polygon_arg(args, 0)
                .map_or(SqliteValue::Null, |p| SqliteValue::Float(geopoly_area(&p))),
        )
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "geopoly_area"
    }
}

/// `geopoly_contains_point(P, X, Y)`: 2 inside, 1 on the boundary, 0 outside.
pub struct GeopolyContainsPointFunc;

impl ScalarFunction for GeopolyContainsPointFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let point = Point::new(float_arg(args, 1), float_arg(args, 2));
        Ok(polygon_arg(args, 0).map_or(SqliteValue::Null, |p| {
            SqliteValue::Integer(geopoly_contains_point(&p, point))
        }))
    }

    fn num_args(&self) -> i32 {
        3
    }

    fn name(&self) -> &'static str {
        "geopoly_contains_point"
    }
}

/// `geopoly_overlap(P1, P2)`: see [`geopoly_overlap`] for the codes.
pub struct GeopolyOverlapFunc;

impl ScalarFunction for GeopolyOverlapFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(match (polygon_arg(args, 0), polygon_arg(args, 1)) {
            (Some(lhs), Some(rhs)) => SqliteValue::Integer(geopoly_overlap(&lhs, &rhs)),
            _ => SqliteValue::Null,
        })
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &'static str {
        "geopoly_overlap"
    }
}

/// `geopoly_within(P1, P2)`: 1 when `P1` is inside `P2`, 2 when they are the
/// same polygon, otherwise 0.
pub struct GeopolyWithinFunc;

impl ScalarFunction for GeopolyWithinFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(match (polygon_arg(args, 0), polygon_arg(args, 1)) {
            (Some(inner), Some(outer)) => SqliteValue::Integer(geopoly_within(&inner, &outer)),
            _ => SqliteValue::Null,
        })
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &'static str {
        "geopoly_within"
    }
}

/// `geopoly_bbox(P)`: the polygon's bounding rectangle, as a polygon blob.
pub struct GeopolyBboxFunc;

impl ScalarFunction for GeopolyBboxFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(polygon_arg(args, 0)
            .and_then(|p| geopoly_bbox(&p))
            .map_or(SqliteValue::Null, |bounds| {
                polygon_blob_value(&geopoly_bbox_polygon(bounds))
            }))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "geopoly_bbox"
    }
}

/// `geopoly_xform(P, A, B, C, D, E, F)`: the polygon under an affine transform.
pub struct GeopolyXformFunc;

impl ScalarFunction for GeopolyXformFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(polygon_arg(args, 0).map_or(SqliteValue::Null, |p| {
            polygon_blob_value(&geopoly_xform(
                &p,
                float_arg(args, 1),
                float_arg(args, 2),
                float_arg(args, 3),
                float_arg(args, 4),
                float_arg(args, 5),
                float_arg(args, 6),
            ))
        }))
    }

    fn num_args(&self) -> i32 {
        7
    }

    fn name(&self) -> &'static str {
        "geopoly_xform"
    }
}

/// `geopoly_regular(X, Y, R, N)`: a regular polygon blob, or NULL when `N < 3`
/// or `R <= 0`.
pub struct GeopolyRegularFunc;

impl ScalarFunction for GeopolyRegularFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        // Stock reads N with `sqlite3_value_int()`, which truncates to 32 bits.
        let sides = args.get(3).map_or(0, |value| value.to_integer() as i32);
        let Ok(sides) = usize::try_from(sides) else {
            return Ok(SqliteValue::Null);
        };
        let vertices = geopoly_regular(
            float_arg(args, 0),
            float_arg(args, 1),
            float_arg(args, 2),
            sides,
        );
        Ok(if vertices.is_empty() {
            SqliteValue::Null
        } else {
            polygon_blob_value(&vertices)
        })
    }

    fn num_args(&self) -> i32 {
        4
    }

    fn name(&self) -> &'static str {
        "geopoly_regular"
    }
}

/// `geopoly_ccw(P)`: the polygon wound counter-clockwise.
pub struct GeopolyCcwFunc;

impl ScalarFunction for GeopolyCcwFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(
            polygon_arg(args, 0)
                .map_or(SqliteValue::Null, |p| polygon_blob_value(&geopoly_ccw(&p))),
        )
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "geopoly_ccw"
    }
}

/// `geopoly_debug(X)`: stock's debug switch, which does nothing in builds
/// without `GEOPOLY_ENABLE_DEBUG` and returns NULL.
pub struct GeopolyDebugFunc;

impl ScalarFunction for GeopolyDebugFunc {
    fn invoke(&self, _args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(SqliteValue::Null)
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "geopoly_debug"
    }
}

/// `geopoly_group_bbox(P)`: the bounding rectangle of every polygon in the group.
///
/// NULL when there is none. Like stock, a value that is not a polygon but
/// leaves stock's error code clear (text not starting with `[`, or a long blob
/// with a bad header) counts as the box (0, 0)-(0, 0).
pub struct GeopolyGroupBboxFunc;

impl AggregateFunction for GeopolyGroupBboxFunc {
    type State = Option<BoundingBox>;

    fn initial_state(&self) -> Self::State {
        None
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        let next = match args.first().map(polygon_param) {
            Some(PolygonParam::Polygon(vertices)) => geopoly_bbox(&vertices),
            Some(PolygonParam::NotPolygonOk) => Some(BoundingBox {
                min_x: 0.0,
                min_y: 0.0,
                max_x: 0.0,
                max_y: 0.0,
            }),
            Some(PolygonParam::NotPolygon) | None => None,
        };
        if let Some(next) = next {
            match state {
                Some(bounds) => extend_bbox(bounds, next),
                None => *state = Some(next),
            }
        }
        Ok(())
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        Ok(state.map_or(SqliteValue::Null, |bounds| {
            polygon_blob_value(&geopoly_bbox_polygon(bounds))
        }))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "geopoly_group_bbox"
    }
}

/// Register stock's geopoly SQL functions: the scalars and the
/// `geopoly_group_bbox()` aggregate.
pub fn register_geopoly_scalars(registry: &mut FunctionRegistry) {
    registry.register_scalar(GeopolyAreaFunc);
    registry.register_scalar(GeopolyBlobFunc);
    registry.register_scalar(GeopolyJsonFunc);
    registry.register_scalar(GeopolySvgFunc);
    registry.register_scalar(GeopolyWithinFunc);
    registry.register_scalar(GeopolyContainsPointFunc);
    registry.register_scalar(GeopolyOverlapFunc);
    registry.register_scalar(GeopolyDebugFunc);
    registry.register_scalar(GeopolyBboxFunc);
    registry.register_scalar(GeopolyXformFunc);
    registry.register_scalar(GeopolyRegularFunc);
    registry.register_scalar(GeopolyCcwFunc);
    registry.register_aggregate(GeopolyGroupBboxFunc);
}

#[cfg(test)]
#[allow(clippy::float_cmp)] // expected values are stock's exact results
mod tests {
    use super::*;

    fn text(value: &str) -> SqliteValue {
        SqliteValue::Text(SmallText::from(value))
    }

    fn square(x0: f64, y0: f64, size: f64) -> [Point; 4] {
        [
            Point::new(x0, y0),
            Point::new(x0 + size, y0),
            Point::new(x0 + size, y0 + size),
            Point::new(x0, y0 + size),
        ]
    }

    #[test]
    fn test_register_geopoly_scalars_registers_stock_function_surface() {
        let mut registry = FunctionRegistry::new();
        register_geopoly_scalars(&mut registry);
        for (name, arity) in [
            ("geopoly_area", 1),
            ("geopoly_blob", 1),
            ("geopoly_json", 1),
            ("geopoly_svg", 0),
            ("geopoly_svg", 1),
            ("geopoly_svg", 4),
            ("geopoly_within", 2),
            ("geopoly_contains_point", 3),
            ("geopoly_overlap", 2),
            ("geopoly_debug", 1),
            ("geopoly_bbox", 1),
            ("geopoly_xform", 7),
            ("geopoly_regular", 4),
            ("geopoly_ccw", 1),
        ] {
            assert!(
                registry.find_scalar(name, arity).is_some(),
                "expected {name}/{arity} to be registered"
            );
        }
        assert!(registry.find_aggregate("geopoly_group_bbox", 1).is_some());
    }

    #[test]
    fn test_geopoly_blob_matches_stock_layout() {
        // Stock: hex(geopoly_blob('[[0.1,-2.5],[1,0],[1,1],[0.1,-2.5]]')).
        let polygon = geopoly_json_decode("[[0.1,-2.5],[1,0],[1,1],[0.1,-2.5]]").unwrap();
        let expected: [u8; 28] = [
            0x01, 0x00, 0x00, 0x03, 0xCD, 0xCC, 0xCC, 0x3D, 0x00, 0x00, 0x20, 0xC0, 0x00, 0x00,
            0x80, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x3F, 0x00, 0x00, 0x80, 0x3F,
        ];
        assert_eq!(geopoly_blob(&polygon), expected);
        assert_eq!(geopoly_blob_decode(&expected), Some(polygon));
    }

    #[test]
    fn test_geopoly_blob_decode_big_endian_and_rejects_bad_blobs() {
        let big_endian: [u8; 28] = [
            0x00, 0x00, 0x00, 0x03, 0x3F, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x3F, 0x80, 0x00, 0x00, 0x3F, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert_eq!(
            geopoly_blob_decode(&big_endian),
            Some(vec![
                Point::new(1.0, 0.0),
                Point::new(0.0, 1.0),
                Point::new(1.0, 0.0)
            ])
        );
        let mut bad_header = big_endian;
        bad_header[0] = 2;
        assert_eq!(geopoly_blob_decode(&bad_header), None);
        let mut bad_count = big_endian;
        bad_count[3] = 4;
        assert_eq!(geopoly_blob_decode(&bad_count), None);
        assert_eq!(geopoly_blob_decode(&big_endian[..20]), None);
    }

    #[test]
    fn test_geopoly_json_parse_follows_stock_grammar() {
        for (input, expected) in [
            (
                "[[0,0],[1,0],[1,1],[0,0]]",
                Some("[[0.0,0.0],[1.0,0.0],[1.0,1.0],[0.0,0.0]]"),
            ),
            ("[[0,0],[2,0],[2,2],[0,2]]", None),
            ("[[0,0],[1,0],[0,0]]", None),
            ("[[0,0],[1,0],[1,1],[0,0]] x", None),
            (
                "[[0,0],[1,0],[1,1],[0,0],]",
                Some("[[0.0,0.0],[1.0,0.0],[1.0,1.0],[0.0,0.0]]"),
            ),
            (
                "[[0,0],[1,0],[1,1],[0,0,]",
                Some("[[0.0,0.0],[1.0,0.0],[1.0,1.0],[0.0,0.0]]"),
            ),
            ("[[01,0],[1,0],[1,1],[01,0]]", None),
            (
                "[[.5,0],[1,0],[1,1],[.5,0]]",
                Some("[[0.5,0.0],[1.0,0.0],[1.0,1.0],[0.5,0.0]]"),
            ),
            ("[[-.5,0],[1,0],[1,1],[-.5,0]]", None),
            ("[[5.,0],[1,0],[1,1],[5.,0]]", None),
            (
                "[[e5,0],[1,0],[1,1],[e5,0]]",
                Some("[[0.0,0.0],[1.0,0.0],[1.0,1.0],[0.0,0.0]]"),
            ),
            ("[[ e5,0],[1,0],[1,1],[ e5,0]]", None),
            (
                "[[,5],[1,0],[1,1],[,5]]",
                Some("[[0.0,5.0],[1.0,0.0],[1.0,1.0],[0.0,5.0]]"),
            ),
            ("[[1e5e3,0],[1,0],[1,1],[1e5e3,0]]", None),
            (
                "[[0,0,9],[1,0,9],[1,1],[0,0]]",
                Some("[[0.0,0.0],[1.0,0.0],[1.0,1.0],[0.0,0.0]]"),
            ),
            ("[[1.0000001,2],[3,4],[5,6],[1,2]]", None),
            ("\u{b}[[0,0],[1,0],[1,1],[0,0]]", None),
            ("junk", None),
        ] {
            assert_eq!(
                geopoly_json_decode(input)
                    .map(|p| geopoly_json(&p))
                    .as_deref(),
                expected,
                "{input}"
            );
        }
    }

    #[test]
    fn test_geopoly_json_and_svg_use_stock_number_format() {
        let polygon =
            geopoly_json_decode("[[1e20,1e-7],[123456789,0.000123456],[1,1],[1e20,1e-7]]").unwrap();
        assert_eq!(
            geopoly_json(&polygon),
            "[[1.0e+20,1.0e-07],[1.23457e+08,0.000123456],[1.0,1.0],[1.0e+20,1.0e-07]]"
        );
        assert_eq!(
            geopoly_svg(&polygon),
            "<polyline points='1e+20,1e-07 1.23457e+08,0.000123456 1,1 1e+20,1e-07'></polyline>"
        );
        assert_eq!(
            geopoly_svg_with_attributes(&square(0.0, 0.0, 2.0), &["style=\"fill:red\"", ""]),
            "<polyline points='0,0 2,0 2,2 0,2 0,0' style=\"fill:red\"></polyline>"
        );
        let rounded =
            geopoly_json_decode("[[999999.5,0.0001],[1,0],[1,1],[999999.5,0.0001]]").unwrap();
        assert_eq!(
            geopoly_json(&rounded),
            "[[1.0e+06,0.0001],[1.0,0.0],[1.0,1.0],[1.0e+06,0.0001]]"
        );
    }

    #[test]
    fn test_geopoly_area_is_signed_float_arithmetic() {
        assert_eq!(geopoly_area(&square(0.0, 0.0, 1.0)), 1.0);
        let clockwise = [
            Point::new(0.0, 0.0),
            Point::new(0.0, 1.0),
            Point::new(1.0, 1.0),
            Point::new(1.0, 0.0),
        ];
        assert_eq!(geopoly_area(&clockwise), -1.0);
        // Stock: 0.23000001162290573 and 2.0 (the latter only in f32 arithmetic).
        let skewed = geopoly_json_decode("[[0.1,0.1],[0.7,0.2],[0.3,0.9],[0.1,0.1]]").unwrap();
        assert_eq!(geopoly_area(&skewed), 0.230_000_011_622_905_73);
        let far = geopoly_json_decode("[[1e7,1e7],[1.0000001e7,1e7],[1e7,1.0000003e7],[1e7,1e7]]")
            .unwrap();
        assert_eq!(geopoly_area(&far), 2.0);
    }

    #[test]
    fn test_geopoly_contains_point_codes() {
        let sq = square(0.0, 0.0, 2.0);
        assert_eq!(geopoly_contains_point(&sq, Point::new(1.0, 1.0)), 2);
        assert_eq!(geopoly_contains_point(&sq, Point::new(2.0, 1.0)), 1);
        assert_eq!(geopoly_contains_point(&sq, Point::new(0.0, 0.0)), 1);
        assert_eq!(geopoly_contains_point(&sq, Point::new(3.0, 1.0)), 0);
        let notch = geopoly_json_decode("[[0,0],[4,0],[4,4],[2,1],[0,4],[0,0]]").unwrap();
        assert_eq!(geopoly_contains_point(&notch, Point::new(2.0, 3.0)), 0);
        assert_eq!(geopoly_contains_point(&notch, Point::new(2.0, 0.5)), 2);
        assert_eq!(geopoly_contains_point(&notch, Point::new(3.0, 2.5)), 1);
        assert_eq!(geopoly_contains_point(&[], Point::new(0.0, 0.0)), 0);
    }

    #[test]
    fn test_geopoly_overlap_and_within_codes() {
        let unit = square(0.0, 0.0, 1.0);
        let big = square(0.0, 0.0, 3.0);
        let inner = square(1.0, 1.0, 1.0);
        assert_eq!(geopoly_overlap(&unit, &square(2.0, 2.0, 1.0)), 0);
        assert_eq!(
            geopoly_overlap(&square(0.0, 0.0, 2.0), &square(1.0, 1.0, 2.0)),
            1
        );
        assert_eq!(geopoly_overlap(&inner, &big), 2);
        assert_eq!(geopoly_overlap(&big, &inner), 3);
        assert_eq!(geopoly_overlap(&big, &big), 4);
        // Edge- and corner-touching squares do not overlap.
        assert_eq!(geopoly_overlap(&unit, &square(1.0, 0.0, 1.0)), 0);
        assert_eq!(geopoly_overlap(&unit, &square(1.0, 1.0, 1.0)), 0);
        assert_eq!(geopoly_overlap(&unit, &[]), 0);
        assert_eq!(geopoly_within(&inner, &big), 1);
        assert_eq!(geopoly_within(&big, &inner), 0);
        assert_eq!(geopoly_within(&big, &big), 2);
        assert_eq!(geopoly_within(&unit, &square(2.0, 2.0, 1.0)), 0);
    }

    #[test]
    fn test_geopoly_regular_ccw_xform_bbox_match_stock() {
        assert_eq!(
            geopoly_json(&geopoly_regular(0.0, 0.0, 1.0, 4)),
            "[[1.00007,0.0],[0.0,1.00007],[-1.00007,0.0],[0.0,-1.00007],[1.00007,0.0]]"
        );
        assert_eq!(
            geopoly_regular(0.0, 0.0, 1.0, 4)[0].x,
            f64::from(f32::from_bits(0x3F80_0235))
        );
        assert_eq!(geopoly_regular(0.0, 0.0, 1.0, 2000).len(), 1000);
        assert!(geopoly_regular(0.0, 0.0, 1.0, 2).is_empty());
        assert!(geopoly_regular(0.0, 0.0, 0.0, 4).is_empty());
        assert_eq!(
            geopoly_area(&geopoly_regular(0.0, 0.0, 1.0, 1000)),
            3.141_555_217_559_613_3
        );

        let ccw = geopoly_json_decode("[[5,5],[5,6],[6,6],[6,5],[7,4],[5,5]]").unwrap();
        assert_eq!(
            geopoly_json(&geopoly_ccw(&ccw)),
            "[[5.0,5.0],[7.0,4.0],[6.0,5.0],[6.0,6.0],[5.0,6.0],[5.0,5.0]]"
        );

        let skewed = geopoly_json_decode("[[0.1,0.2],[1,0],[1,1],[0.1,0.2]]").unwrap();
        assert_eq!(
            geopoly_json(&geopoly_xform(&skewed, 3.0, 0.5, 0.25, 7.0, 0.1, 0.3)),
            "[[0.5,1.725],[3.1,0.55],[3.6,7.55],[0.5,1.725]]"
        );

        let triangle = geopoly_json_decode("[[5,-1],[2,7],[-3,0.5],[5,-1]]").unwrap();
        let bounds = geopoly_bbox(&triangle).unwrap();
        assert_eq!(
            geopoly_json(&geopoly_bbox_polygon(bounds)),
            "[[-3.0,-1.0],[5.0,-1.0],[5.0,7.0],[-3.0,7.0],[-3.0,-1.0]]"
        );
        let grouped =
            geopoly_group_bbox(&[&square(0.0, 0.0, 1.0), &square(5.0, -2.0, 2.0)]).unwrap();
        assert_eq!(
            geopoly_json(&geopoly_bbox_polygon(grouped)),
            "[[0.0,-2.0],[7.0,-2.0],[7.0,1.0],[0.0,1.0],[0.0,-2.0]]"
        );
        assert!(geopoly_bbox(&[]).is_none());
        assert!(geopoly_group_bbox(&[]).is_none());
    }

    #[test]
    fn test_geopoly_sql_wrappers_return_null_for_non_polygons() {
        for value in [
            SqliteValue::Null,
            SqliteValue::Integer(5),
            SqliteValue::Float(5.5),
            text("junk"),
            text("[[0,0],[2,0],[2,2],[0,2]]"),
            SqliteValue::Blob(Arc::from(&b"[[0,0],[1,0],[1,1],[0,0]]"[..])),
        ] {
            let args = [value];
            assert!(GeopolyJsonFunc.invoke(&args).unwrap().is_null());
            assert!(GeopolyAreaFunc.invoke(&args).unwrap().is_null());
            assert!(GeopolyBboxFunc.invoke(&args).unwrap().is_null());
            assert!(GeopolySvgFunc.invoke(&args).unwrap().is_null());
        }
        assert!(GeopolySvgFunc.invoke(&[]).unwrap().is_null());
        assert!(
            GeopolyDebugFunc
                .invoke(&[SqliteValue::Integer(1)])
                .unwrap()
                .is_null()
        );
    }

    #[test]
    fn test_geopoly_svg_func_appends_attribute_arguments() {
        let value = GeopolySvgFunc
            .invoke(&[
                text("[[0,0],[1,0],[1,1],[0,0]]"),
                text("style=\"fill:red\""),
                SqliteValue::Null,
                text(""),
                SqliteValue::Integer(5),
                SqliteValue::Float(1.5),
                text("a\0b"),
                SqliteValue::Blob(Arc::from(&b"abc"[..])),
            ])
            .unwrap();
        assert_eq!(
            value,
            text("<polyline points='0,0 1,0 1,1 0,0' style=\"fill:red\" 5 1.5 a abc></polyline>")
        );
    }

    #[test]
    fn test_geopoly_group_bbox_counts_quiet_non_polygons_as_zero_box() {
        let run = |values: &[SqliteValue]| {
            let mut state = GeopolyGroupBboxFunc.initial_state();
            for value in values {
                GeopolyGroupBboxFunc
                    .step(&mut state, std::slice::from_ref(value))
                    .unwrap();
            }
            match GeopolyGroupBboxFunc.finalize(state).unwrap() {
                SqliteValue::Blob(blob) => geopoly_blob_decode(&blob).map(|p| geopoly_json(&p)),
                SqliteValue::Null => None,
                other => panic!("unexpected {other:?}"),
            }
        };
        let five = text("[[5,5],[6,5],[6,6],[5,5]]");
        assert_eq!(
            run(&[five.clone(), text("junk")]).as_deref(),
            Some("[[0.0,0.0],[6.0,0.0],[6.0,6.0],[0.0,6.0],[0.0,0.0]]")
        );
        assert_eq!(
            run(&[
                five,
                SqliteValue::Null,
                text("[[0,0],[1,0]]"),
                SqliteValue::Integer(7)
            ])
            .as_deref(),
            Some("[[5.0,5.0],[6.0,5.0],[6.0,6.0],[5.0,6.0],[5.0,5.0]]")
        );
        assert_eq!(run(&[SqliteValue::Null, SqliteValue::Integer(7)]), None);
        assert_eq!(run(&[]), None);
    }

    #[test]
    fn test_geopoly_regular_func_reads_sides_like_sqlite3_value_int() {
        let regular = |sides: SqliteValue| {
            GeopolyRegularFunc
                .invoke(&[text("1"), text("2"), text("3"), sides])
                .unwrap()
        };
        let SqliteValue::Blob(blob) = regular(text("4.9")) else {
            panic!("expected a blob");
        };
        assert_eq!(
            geopoly_json(&geopoly_blob_decode(&blob).unwrap()),
            "[[4.0002,2.0],[1.0,5.0002],[-2.0002,2.0],[1.0,-1.0002],[4.0002,2.0]]"
        );
        assert!(regular(SqliteValue::Integer(-4)).is_null());
        assert!(regular(SqliteValue::Null).is_null());
    }
}
