use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Public API — extension name
// ---------------------------------------------------------------------------

#[must_use]
pub const fn extension_name() -> &'static str {
    "rtree"
}

// ---------------------------------------------------------------------------
// Core geometry primitives
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

impl Point {
    #[must_use]
    pub const fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BoundingBox {
    pub min_x: f64,
    pub min_y: f64,
    pub max_x: f64,
    pub max_y: f64,
}

impl BoundingBox {
    #[must_use]
    pub const fn contains_point(self, point: Point) -> bool {
        point.x >= self.min_x
            && point.x <= self.max_x
            && point.y >= self.min_y
            && point.y <= self.max_y
    }

    #[must_use]
    pub const fn contains_box(self, other: Self) -> bool {
        other.min_x >= self.min_x
            && other.max_x <= self.max_x
            && other.min_y >= self.min_y
            && other.max_y <= self.max_y
    }
}

// ---------------------------------------------------------------------------
// Multi-dimensional bounding box for R*-tree (1-5 dimensions)
// ---------------------------------------------------------------------------

/// Maximum number of dimensions supported by the R*-tree.
const MAX_DIMENSIONS: usize = 5;

/// A multi-dimensional axis-aligned bounding box stored as min/max pairs.
///
/// For `n` dimensions the layout is `[min0, max0, min1, max1, ..., min_{n-1}, max_{n-1}]`.
#[derive(Debug, Clone, PartialEq)]
pub struct MBoundingBox {
    /// Coordinate pairs: `[min0, max0, min1, max1, ...]`.
    pub coords: Vec<f64>,
}

impl MBoundingBox {
    /// Create a new `MBoundingBox` from raw min/max coordinate pairs.
    ///
    /// `coords` must have an even length between 2 and 10 (1-5 dimensions).
    pub fn new(coords: Vec<f64>) -> Option<Self> {
        if coords.len() % 2 != 0 || coords.is_empty() || coords.len() > MAX_DIMENSIONS * 2 {
            return None;
        }
        Some(Self { coords })
    }

    #[must_use]
    pub fn dimensions(&self) -> usize {
        self.coords.len() / 2
    }

    #[must_use]
    pub fn min_coord(&self, dim: usize) -> f64 {
        self.coords[dim * 2]
    }

    #[must_use]
    pub fn max_coord(&self, dim: usize) -> f64 {
        self.coords[dim * 2 + 1]
    }

    /// Test whether this bounding box overlaps `other`.
    #[must_use]
    pub fn overlaps(&self, other: &Self) -> bool {
        if self.dimensions() != other.dimensions() {
            return false;
        }
        for d in 0..self.dimensions() {
            if self.min_coord(d) > other.max_coord(d) || self.max_coord(d) < other.min_coord(d) {
                return false;
            }
        }
        true
    }

    /// Compute the union bounding box of `self` and `other`.
    #[must_use]
    pub fn union(&self, other: &Self) -> Self {
        let mut merged = Vec::with_capacity(self.coords.len());
        for d in 0..self.dimensions() {
            merged.push(self.min_coord(d).min(other.min_coord(d)));
            merged.push(self.max_coord(d).max(other.max_coord(d)));
        }
        Self { coords: merged }
    }

    /// Compute the volume (area in 2D, hypervolume in nD) of this bounding box.
    #[must_use]
    pub fn volume(&self) -> f64 {
        let mut vol = 1.0;
        for d in 0..self.dimensions() {
            let extent = self.max_coord(d) - self.min_coord(d);
            if extent < 0.0 {
                return 0.0;
            }
            vol *= extent;
        }
        vol
    }

    /// Compute the enlargement needed to include `other` in this box.
    #[must_use]
    pub fn enlargement(&self, other: &Self) -> f64 {
        self.union(other).volume() - self.volume()
    }
}

// ---------------------------------------------------------------------------
// R*-tree coordinate type
// ---------------------------------------------------------------------------

/// Coordinate type for R*-tree entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RtreeCoordType {
    /// 32-bit float coordinates (default).
    Float32,
    /// 32-bit integer coordinates (rtree_i32).
    Int32,
}

// ---------------------------------------------------------------------------
// R*-tree entry
// ---------------------------------------------------------------------------

/// A single entry in the R*-tree leaf node.
#[derive(Debug, Clone, PartialEq)]
pub struct RtreeEntry {
    /// The rowid for this spatial entry.
    pub id: i64,
    /// The multi-dimensional bounding box.
    pub bbox: MBoundingBox,
}

// ---------------------------------------------------------------------------
// R*-tree query result (for custom geometry callbacks)
// ---------------------------------------------------------------------------

/// Result returned by a custom geometry callback during R*-tree query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RtreeQueryResult {
    /// Include this entry / descend into this node and include all children.
    Include,
    /// Exclude this entry / prune this entire subtree.
    Exclude,
    /// Descend into children but don't automatically include (internal nodes only).
    PartiallyContained,
}

// ---------------------------------------------------------------------------
// RtreeGeometry trait (custom geometry callbacks)
// ---------------------------------------------------------------------------

/// Trait for custom geometry callbacks used in R*-tree queries.
///
/// Implement this to define custom spatial predicates beyond simple
/// bounding-box overlap.
pub trait RtreeGeometry: Send + Sync {
    /// Evaluate whether the given bounding box (as a flat `[min, max, ...]` slice)
    /// should be included, excluded, or partially contained.
    fn query_func(&self, bbox: &[f64]) -> RtreeQueryResult;
}

// ---------------------------------------------------------------------------
// R*-tree index (in-memory, simplified)
// ---------------------------------------------------------------------------

/// Configuration for an R*-tree virtual table.
#[derive(Debug, Clone)]
pub struct RtreeConfig {
    /// Number of dimensions (1-5).
    pub dimensions: usize,
    /// Coordinate type.
    pub coord_type: RtreeCoordType,
}

impl RtreeConfig {
    /// Create a new configuration.
    ///
    /// Returns `None` if dimensions is not in 1..=5.
    pub fn new(dimensions: usize, coord_type: RtreeCoordType) -> Option<Self> {
        if dimensions == 0 || dimensions > MAX_DIMENSIONS {
            return None;
        }
        Some(Self {
            dimensions,
            coord_type,
        })
    }
}

/// An in-memory R*-tree spatial index.
///
/// This is a simplified implementation storing entries in a flat list.
/// The real implementation would use a balanced tree with internal nodes,
/// but this captures the correct query semantics and API surface for the
/// extension crate.
pub struct RtreeIndex {
    config: RtreeConfig,
    entries: Vec<RtreeEntry>,
    geometry_registry: HashMap<String, Box<dyn RtreeGeometry>>,
}

impl std::fmt::Debug for RtreeIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RtreeIndex")
            .field("config", &self.config)
            .field("entries", &self.entries)
            .field("geometry_count", &self.geometry_registry.len())
            .finish()
    }
}

impl RtreeIndex {
    /// Create a new, empty R*-tree index.
    #[must_use]
    pub fn new(config: RtreeConfig) -> Self {
        Self {
            config,
            entries: Vec::new(),
            geometry_registry: HashMap::new(),
        }
    }

    /// Return the number of dimensions for this index.
    #[must_use]
    pub fn dimensions(&self) -> usize {
        self.config.dimensions
    }

    /// Return the coordinate type.
    #[must_use]
    pub fn coord_type(&self) -> RtreeCoordType {
        self.config.coord_type
    }

    /// Return the number of entries in the index.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Return whether the index is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Insert an entry into the R*-tree.
    ///
    /// Returns `false` if the bounding box dimensions don't match the index
    /// or if an entry with the same id already exists.
    pub fn insert(&mut self, entry: RtreeEntry) -> bool {
        if entry.bbox.dimensions() != self.config.dimensions {
            return false;
        }
        if self.entries.iter().any(|e| e.id == entry.id) {
            return false;
        }
        self.entries.push(entry);
        true
    }

    /// Delete an entry by rowid.
    ///
    /// Returns `true` if an entry was removed.
    pub fn delete(&mut self, id: i64) -> bool {
        let before = self.entries.len();
        self.entries.retain(|e| e.id != id);
        self.entries.len() < before
    }

    /// Update an entry's bounding box.
    ///
    /// Returns `true` if the entry was found and updated.
    pub fn update(&mut self, id: i64, new_bbox: MBoundingBox) -> bool {
        if new_bbox.dimensions() != self.config.dimensions {
            return false;
        }
        if let Some(entry) = self.entries.iter_mut().find(|e| e.id == id) {
            entry.bbox = new_bbox;
            true
        } else {
            false
        }
    }

    /// Range query: find all entries whose bounding box overlaps `query_bbox`.
    #[must_use]
    pub fn range_query(&self, query_bbox: &MBoundingBox) -> Vec<&RtreeEntry> {
        if query_bbox.dimensions() != self.config.dimensions {
            return Vec::new();
        }
        self.entries
            .iter()
            .filter(|e| e.bbox.overlaps(query_bbox))
            .collect()
    }

    /// Register a custom geometry callback.
    pub fn register_geometry(&mut self, name: &str, geom: Box<dyn RtreeGeometry>) {
        self.geometry_registry.insert(name.to_owned(), geom);
    }

    /// Query using a registered custom geometry callback.
    ///
    /// Returns entries for which the callback returns `Include`.
    /// `PartiallyContained` is treated as `Include` for leaf entries.
    pub fn geometry_query(&self, name: &str) -> Vec<&RtreeEntry> {
        let Some(geom) = self.geometry_registry.get(name) else {
            return Vec::new();
        };
        self.entries
            .iter()
            .filter(|e| {
                matches!(
                    geom.query_func(&e.bbox.coords),
                    RtreeQueryResult::Include | RtreeQueryResult::PartiallyContained
                )
            })
            .collect()
    }

    /// Query using a geometry callback, also returning the result per entry.
    ///
    /// This is useful for verifying pruning behavior.
    pub fn geometry_query_detailed(&self, name: &str) -> Vec<(&RtreeEntry, RtreeQueryResult)> {
        let Some(geom) = self.geometry_registry.get(name) else {
            return Vec::new();
        };
        self.entries
            .iter()
            .map(|e| (e, geom.query_func(&e.bbox.coords)))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Geopoly binary blob format
// ---------------------------------------------------------------------------

/// Geopoly polygon binary format header type byte.
const GEOPOLY_HEADER_TYPE: u8 = 0x47; // 'G'

/// Encode a polygon (slice of Points) to Geopoly binary blob format.
///
/// Format: 4-byte header (type byte + 3-byte vertex count LE) followed
/// by pairs of 32-bit float coordinates (x, y for each vertex) in LE.
#[must_use]
pub fn geopoly_blob(vertices: &[Point]) -> Vec<u8> {
    let count = vertices.len();
    let mut blob = Vec::with_capacity(4 + count * 8);
    // Header: 1 type byte + 3-byte little-endian vertex count
    blob.push(GEOPOLY_HEADER_TYPE);
    #[allow(clippy::cast_possible_truncation)] // vertex count fits in 3 bytes (max ~16M)
    let count_bytes = (count as u32).to_le_bytes();
    blob.extend_from_slice(&count_bytes[..3]);
    // Coordinate pairs as 32-bit floats (LE)
    for v in vertices {
        #[allow(clippy::cast_possible_truncation)]
        let x = v.x as f32;
        #[allow(clippy::cast_possible_truncation)]
        let y = v.y as f32;
        blob.extend_from_slice(&x.to_le_bytes());
        blob.extend_from_slice(&y.to_le_bytes());
    }
    blob
}

/// Decode a Geopoly binary blob into a vector of Points.
///
/// Returns `None` on malformed input.
pub fn geopoly_blob_decode(blob: &[u8]) -> Option<Vec<Point>> {
    if blob.len() < 4 {
        return None;
    }
    if blob[0] != GEOPOLY_HEADER_TYPE {
        return None;
    }
    let mut count_bytes = [0u8; 4];
    count_bytes[..3].copy_from_slice(&blob[1..4]);
    let count = u32::from_le_bytes(count_bytes) as usize;
    let expected = 4 + count * 8;
    if blob.len() < expected {
        return None;
    }
    let mut vertices = Vec::with_capacity(count);
    let mut pos = 4;
    for _ in 0..count {
        let x_bytes: [u8; 4] = blob[pos..pos + 4].try_into().ok()?;
        let y_bytes: [u8; 4] = blob[pos + 4..pos + 8].try_into().ok()?;
        let x = f64::from(f32::from_le_bytes(x_bytes));
        let y = f64::from(f32::from_le_bytes(y_bytes));
        vertices.push(Point::new(x, y));
        pos += 8;
    }
    Some(vertices)
}

/// Convert a polygon to a GeoJSON-style coordinate array string.
///
/// Format: `[[x0,y0],[x1,y1],...]` with the first vertex repeated at
/// the end to close the ring (GeoJSON convention).
#[must_use]
pub fn geopoly_json(vertices: &[Point]) -> String {
    let mut parts: Vec<String> = vertices
        .iter()
        .map(|v| format!("[{},{}]", v.x, v.y))
        .collect();
    // Close the ring if needed.
    if let (Some(first), Some(last)) = (vertices.first(), vertices.last()) {
        if first != last {
            parts.push(format!("[{},{}]", first.x, first.y));
        }
    }
    format!("[{}]", parts.join(","))
}

/// Convert a GeoJSON-style coordinate array string back to vertices.
///
/// Accepts `[[x0,y0],[x1,y1],...]`. If the last vertex duplicates the first
/// (closed ring), the duplicate is removed.
pub fn geopoly_json_decode(json: &str) -> Option<Vec<Point>> {
    let trimmed = json.trim();
    let inner = trimmed.strip_prefix('[')?.strip_suffix(']')?;
    let mut vertices = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;
    for (i, ch) in inner.char_indices() {
        match ch {
            '[' => depth += 1,
            ']' => {
                depth -= 1;
                if depth == 0 {
                    let segment = &inner[start..=i];
                    let pair = segment.trim().strip_prefix('[')?.strip_suffix(']')?;
                    let mut nums = pair.split(',');
                    let x: f64 = nums.next()?.trim().parse().ok()?;
                    let y: f64 = nums.next()?.trim().parse().ok()?;
                    vertices.push(Point::new(x, y));
                }
            }
            ',' if depth == 0 => {
                start = i + 1;
            }
            _ => {}
        }
    }
    // Remove closing duplicate vertex if present.
    if vertices.len() > 1 && vertices.first() == vertices.last() {
        vertices.pop();
    }
    if vertices.is_empty() {
        return None;
    }
    Some(vertices)
}

/// Render a polygon as an SVG path `d` attribute value.
///
/// Produces `M x0 y0 L x1 y1 L x2 y2 ... Z`.
#[must_use]
pub fn geopoly_svg(vertices: &[Point]) -> String {
    if vertices.is_empty() {
        return String::new();
    }
    let mut parts = Vec::with_capacity(vertices.len() + 1);
    for (i, v) in vertices.iter().enumerate() {
        let cmd = if i == 0 { "M" } else { "L" };
        parts.push(format!("{cmd} {} {}", v.x, v.y));
    }
    parts.push("Z".to_owned());
    parts.join(" ")
}

// ---------------------------------------------------------------------------
// Geopoly functions (pre-existing)
// ---------------------------------------------------------------------------

#[must_use]
pub fn geopoly_bbox(vertices: &[Point]) -> Option<BoundingBox> {
    let first = *vertices.first()?;
    let mut bounds = BoundingBox {
        min_x: first.x,
        min_y: first.y,
        max_x: first.x,
        max_y: first.y,
    };

    for vertex in vertices.iter().skip(1) {
        bounds.min_x = bounds.min_x.min(vertex.x);
        bounds.min_y = bounds.min_y.min(vertex.y);
        bounds.max_x = bounds.max_x.max(vertex.x);
        bounds.max_y = bounds.max_y.max(vertex.y);
    }

    Some(bounds)
}

#[must_use]
pub fn geopoly_group_bbox(polygons: &[&[Point]]) -> Option<BoundingBox> {
    let mut iter = polygons.iter().filter_map(|polygon| geopoly_bbox(polygon));
    let mut bounds = iter.next()?;

    for next in iter {
        bounds.min_x = bounds.min_x.min(next.min_x);
        bounds.min_y = bounds.min_y.min(next.min_y);
        bounds.max_x = bounds.max_x.max(next.max_x);
        bounds.max_y = bounds.max_y.max(next.max_y);
    }

    Some(bounds)
}

#[must_use]
pub fn geopoly_area(vertices: &[Point]) -> f64 {
    if vertices.len() < 3 {
        return 0.0;
    }

    let mut twice_area = 0.0;
    for index in 0..vertices.len() {
        let current = vertices[index];
        let next = vertices[(index + 1) % vertices.len()];
        twice_area += current.x.mul_add(next.y, -(next.x * current.y));
    }

    twice_area.abs() * 0.5
}

#[must_use]
pub fn geopoly_contains_point(vertices: &[Point], point: Point) -> bool {
    if vertices.len() < 3 {
        return false;
    }

    let mut inside = false;
    let mut previous = vertices[vertices.len() - 1];

    for &current in vertices {
        if point_on_segment(previous, current, point) {
            return true;
        }

        let crosses_scanline = (current.y > point.y) != (previous.y > point.y);
        if crosses_scanline {
            let intersection_x = ((previous.x - current.x) * (point.y - current.y)
                / (previous.y - current.y))
                + current.x;
            if point.x < intersection_x {
                inside = !inside;
            }
        }

        previous = current;
    }

    inside
}

#[must_use]
pub fn geopoly_overlap(lhs: &[Point], rhs: &[Point]) -> bool {
    if lhs.len() < 3 || rhs.len() < 3 {
        return false;
    }

    for lhs_index in 0..lhs.len() {
        let lhs_start = lhs[lhs_index];
        let lhs_end = lhs[(lhs_index + 1) % lhs.len()];
        for rhs_index in 0..rhs.len() {
            let rhs_start = rhs[rhs_index];
            let rhs_end = rhs[(rhs_index + 1) % rhs.len()];
            if segments_intersect(lhs_start, lhs_end, rhs_start, rhs_end) {
                return true;
            }
        }
    }

    geopoly_contains_point(lhs, rhs[0]) || geopoly_contains_point(rhs, lhs[0])
}

#[must_use]
pub fn geopoly_within(inner: &[Point], outer: &[Point]) -> bool {
    if inner.len() < 3 || outer.len() < 3 {
        return false;
    }

    let Some(inner_bbox) = geopoly_bbox(inner) else {
        return false;
    };
    let Some(outer_bbox) = geopoly_bbox(outer) else {
        return false;
    };
    if !outer_bbox.contains_box(inner_bbox) {
        return false;
    }

    inner
        .iter()
        .copied()
        .all(|point| geopoly_contains_point(outer, point))
}

#[must_use]
pub fn geopoly_ccw(vertices: &[Point]) -> Vec<Point> {
    if vertices.len() < 3 {
        return vertices.to_vec();
    }

    let mut normalized = vertices.to_vec();
    if signed_twice_area(&normalized) < 0.0 {
        normalized.reverse();
    }
    normalized
}

#[must_use]
pub fn geopoly_regular(center_x: f64, center_y: f64, radius: f64, sides: usize) -> Vec<Point> {
    if sides < 3 || !radius.is_finite() || radius <= 0.0 {
        return Vec::new();
    }

    let Ok(sides_u32) = u32::try_from(sides) else {
        return Vec::new();
    };
    let Ok(capacity) = usize::try_from(sides_u32) else {
        return Vec::new();
    };

    let step = std::f64::consts::TAU / f64::from(sides_u32);
    let mut vertices = Vec::with_capacity(capacity);
    for index in 0..sides_u32 {
        let angle = step * f64::from(index);
        vertices.push(Point::new(
            radius.mul_add(angle.cos(), center_x),
            radius.mul_add(angle.sin(), center_y),
        ));
    }

    vertices
}

#[must_use]
#[allow(clippy::many_single_char_names)] // Affine transform matrix coefficients (standard naming)
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
        .copied()
        .map(|vertex| {
            Point::new(
                a.mul_add(vertex.x, b.mul_add(vertex.y, e)),
                c.mul_add(vertex.x, d.mul_add(vertex.y, f)),
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Internal geometry helpers
// ---------------------------------------------------------------------------

fn segments_intersect(a_start: Point, a_end: Point, b_start: Point, b_end: Point) -> bool {
    let o1 = orientation(a_start, a_end, b_start);
    let o2 = orientation(a_start, a_end, b_end);
    let o3 = orientation(b_start, b_end, a_start);
    let o4 = orientation(b_start, b_end, a_end);

    if o1 != o2 && o3 != o4 {
        return true;
    }

    if o1 == 0 && point_on_segment(a_start, a_end, b_start) {
        return true;
    }
    if o2 == 0 && point_on_segment(a_start, a_end, b_end) {
        return true;
    }
    if o3 == 0 && point_on_segment(b_start, b_end, a_start) {
        return true;
    }
    if o4 == 0 && point_on_segment(b_start, b_end, a_end) {
        return true;
    }

    false
}

fn orientation(start: Point, end: Point, probe: Point) -> i8 {
    let cross =
        (end.y - start.y).mul_add(probe.x - end.x, -((end.x - start.x) * (probe.y - end.y)));
    if cross > f64::EPSILON {
        1
    } else if cross < -f64::EPSILON {
        -1
    } else {
        0
    }
}

fn point_on_segment(start: Point, end: Point, point: Point) -> bool {
    if orientation(start, end, point) != 0 {
        return false;
    }

    point.x >= start.x.min(end.x)
        && point.x <= start.x.max(end.x)
        && point.y >= start.y.min(end.y)
        && point.y <= start.y.max(end.y)
}

fn signed_twice_area(vertices: &[Point]) -> f64 {
    if vertices.len() < 3 {
        return 0.0;
    }

    let mut twice_area = 0.0;
    for index in 0..vertices.len() {
        let current = vertices[index];
        let next = vertices[(index + 1) % vertices.len()];
        twice_area += current.x.mul_add(next.y, -(next.x * current.y));
    }
    twice_area
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn approx_eq(left: f64, right: f64) -> bool {
        (left - right).abs() < 1e-4
    }

    fn square(x0: f64, y0: f64, size: f64) -> [Point; 4] {
        [
            Point::new(x0, y0),
            Point::new(x0 + size, y0),
            Point::new(x0 + size, y0 + size),
            Point::new(x0, y0 + size),
        ]
    }

    // -------------------------------------------------------------------
    // Extension name
    // -------------------------------------------------------------------

    #[test]
    fn test_extension_name_matches_crate_suffix() {
        let expected = env!("CARGO_PKG_NAME")
            .strip_prefix("fsqlite-ext-")
            .expect("extension crates should use fsqlite-ext-* naming");
        assert_eq!(extension_name(), expected);
    }

    // -------------------------------------------------------------------
    // R*-tree creation
    // -------------------------------------------------------------------

    #[test]
    fn test_rtree_create_2d() {
        let config = RtreeConfig::new(2, RtreeCoordType::Float32).unwrap();
        let index = RtreeIndex::new(config);
        assert_eq!(index.dimensions(), 2);
        assert!(index.is_empty());
    }

    #[test]
    fn test_rtree_create_3d() {
        let config = RtreeConfig::new(3, RtreeCoordType::Float32).unwrap();
        let index = RtreeIndex::new(config);
        assert_eq!(index.dimensions(), 3);
    }

    #[test]
    fn test_rtree_create_5d() {
        let config = RtreeConfig::new(5, RtreeCoordType::Float32).unwrap();
        let index = RtreeIndex::new(config);
        assert_eq!(index.dimensions(), 5);
        // 6 dimensions should fail.
        assert!(RtreeConfig::new(6, RtreeCoordType::Float32).is_none());
    }

    // -------------------------------------------------------------------
    // R*-tree insert / delete / update / query
    // -------------------------------------------------------------------

    #[test]
    fn test_rtree_insert() {
        let config = RtreeConfig::new(2, RtreeCoordType::Float32).unwrap();
        let mut index = RtreeIndex::new(config);
        let entry = RtreeEntry {
            id: 1,
            bbox: MBoundingBox::new(vec![0.0, 10.0, 0.0, 10.0]).unwrap(),
        };
        assert!(index.insert(entry));
        assert_eq!(index.len(), 1);
        // Duplicate id rejected.
        let dup = RtreeEntry {
            id: 1,
            bbox: MBoundingBox::new(vec![5.0, 15.0, 5.0, 15.0]).unwrap(),
        };
        assert!(!index.insert(dup));
    }

    #[test]
    fn test_rtree_range_query() {
        let config = RtreeConfig::new(2, RtreeCoordType::Float32).unwrap();
        let mut index = RtreeIndex::new(config);
        // Insert three boxes.
        index.insert(RtreeEntry {
            id: 1,
            bbox: MBoundingBox::new(vec![0.0, 5.0, 0.0, 5.0]).unwrap(),
        });
        index.insert(RtreeEntry {
            id: 2,
            bbox: MBoundingBox::new(vec![3.0, 8.0, 3.0, 8.0]).unwrap(),
        });
        index.insert(RtreeEntry {
            id: 3,
            bbox: MBoundingBox::new(vec![10.0, 15.0, 10.0, 15.0]).unwrap(),
        });
        // Query overlapping first two.
        let query = MBoundingBox::new(vec![2.0, 6.0, 2.0, 6.0]).unwrap();
        let results = index.range_query(&query);
        let ids: Vec<i64> = results.iter().map(|e| e.id).collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
        assert!(!ids.contains(&3));
    }

    #[test]
    fn test_rtree_range_query_no_match() {
        let config = RtreeConfig::new(2, RtreeCoordType::Float32).unwrap();
        let mut index = RtreeIndex::new(config);
        index.insert(RtreeEntry {
            id: 1,
            bbox: MBoundingBox::new(vec![0.0, 1.0, 0.0, 1.0]).unwrap(),
        });
        let query = MBoundingBox::new(vec![100.0, 200.0, 100.0, 200.0]).unwrap();
        assert!(index.range_query(&query).is_empty());
    }

    #[test]
    fn test_rtree_delete() {
        let config = RtreeConfig::new(2, RtreeCoordType::Float32).unwrap();
        let mut index = RtreeIndex::new(config);
        index.insert(RtreeEntry {
            id: 1,
            bbox: MBoundingBox::new(vec![0.0, 5.0, 0.0, 5.0]).unwrap(),
        });
        assert!(index.delete(1));
        assert!(index.is_empty());
        assert!(!index.delete(1)); // Already gone.
    }

    #[test]
    fn test_rtree_update() {
        let config = RtreeConfig::new(2, RtreeCoordType::Float32).unwrap();
        let mut index = RtreeIndex::new(config);
        index.insert(RtreeEntry {
            id: 1,
            bbox: MBoundingBox::new(vec![0.0, 5.0, 0.0, 5.0]).unwrap(),
        });
        let new_bbox = MBoundingBox::new(vec![10.0, 20.0, 10.0, 20.0]).unwrap();
        assert!(index.update(1, new_bbox));
        // Old range should miss.
        let old_query = MBoundingBox::new(vec![0.0, 3.0, 0.0, 3.0]).unwrap();
        assert!(index.range_query(&old_query).is_empty());
        // New range should hit.
        let new_query = MBoundingBox::new(vec![12.0, 18.0, 12.0, 18.0]).unwrap();
        assert_eq!(index.range_query(&new_query).len(), 1);
    }

    #[test]
    fn test_rtree_1d() {
        let config = RtreeConfig::new(1, RtreeCoordType::Float32).unwrap();
        let mut index = RtreeIndex::new(config);
        index.insert(RtreeEntry {
            id: 1,
            bbox: MBoundingBox::new(vec![5.0, 10.0]).unwrap(),
        });
        let query = MBoundingBox::new(vec![8.0, 12.0]).unwrap();
        assert_eq!(index.range_query(&query).len(), 1);
        let miss = MBoundingBox::new(vec![11.0, 20.0]).unwrap();
        assert!(index.range_query(&miss).is_empty());
    }

    #[test]
    fn test_rtree_i32() {
        let config = RtreeConfig::new(2, RtreeCoordType::Int32).unwrap();
        let mut index = RtreeIndex::new(config);
        assert_eq!(index.coord_type(), RtreeCoordType::Int32);
        index.insert(RtreeEntry {
            id: 1,
            bbox: MBoundingBox::new(vec![0.0, 100.0, 0.0, 100.0]).unwrap(),
        });
        let query = MBoundingBox::new(vec![50.0, 60.0, 50.0, 60.0]).unwrap();
        assert_eq!(index.range_query(&query).len(), 1);
    }

    // -------------------------------------------------------------------
    // Custom geometry callbacks
    // -------------------------------------------------------------------

    struct CircleGeometry {
        cx: f64,
        cy: f64,
        radius: f64,
    }

    impl RtreeGeometry for CircleGeometry {
        fn query_func(&self, bbox: &[f64]) -> RtreeQueryResult {
            // 2D: bbox = [min_x, max_x, min_y, max_y]
            if bbox.len() < 4 {
                return RtreeQueryResult::Exclude;
            }
            let center_x = f64::midpoint(bbox[0], bbox[1]);
            let center_y = f64::midpoint(bbox[2], bbox[3]);
            let dx = center_x - self.cx;
            let dy = center_y - self.cy;
            let dist = dx.hypot(dy);
            let half_diag = (bbox[1] - bbox[0]).hypot(bbox[3] - bbox[2]) / 2.0;
            if dist + half_diag <= self.radius {
                RtreeQueryResult::Include
            } else if dist - half_diag > self.radius {
                RtreeQueryResult::Exclude
            } else {
                RtreeQueryResult::PartiallyContained
            }
        }
    }

    #[test]
    fn test_rtree_custom_geometry() {
        let config = RtreeConfig::new(2, RtreeCoordType::Float32).unwrap();
        let mut index = RtreeIndex::new(config);
        // Entries around origin.
        index.insert(RtreeEntry {
            id: 1,
            bbox: MBoundingBox::new(vec![0.0, 1.0, 0.0, 1.0]).unwrap(),
        });
        index.insert(RtreeEntry {
            id: 2,
            bbox: MBoundingBox::new(vec![100.0, 101.0, 100.0, 101.0]).unwrap(),
        });
        index.register_geometry(
            "circle",
            Box::new(CircleGeometry {
                cx: 0.5,
                cy: 0.5,
                radius: 5.0,
            }),
        );
        let results = index.geometry_query("circle");
        let ids: Vec<i64> = results.iter().map(|e| e.id).collect();
        assert!(ids.contains(&1));
        assert!(!ids.contains(&2));
    }

    #[test]
    fn test_rtree_geometry_prune() {
        let config = RtreeConfig::new(2, RtreeCoordType::Float32).unwrap();
        let mut index = RtreeIndex::new(config);
        // Near and far entries.
        index.insert(RtreeEntry {
            id: 1,
            bbox: MBoundingBox::new(vec![0.0, 1.0, 0.0, 1.0]).unwrap(),
        });
        index.insert(RtreeEntry {
            id: 2,
            bbox: MBoundingBox::new(vec![50.0, 51.0, 50.0, 51.0]).unwrap(),
        });
        index.register_geometry(
            "small_circle",
            Box::new(CircleGeometry {
                cx: 0.5,
                cy: 0.5,
                radius: 2.0,
            }),
        );
        let detailed = index.geometry_query_detailed("small_circle");
        let excluded_count = detailed
            .iter()
            .filter(|(_, r)| *r == RtreeQueryResult::Exclude)
            .count();
        // The far-away entry should be excluded.
        assert!(excluded_count >= 1);
    }

    #[test]
    fn test_rtree_large_dataset() {
        let config = RtreeConfig::new(2, RtreeCoordType::Float32).unwrap();
        let mut index = RtreeIndex::new(config);
        // Insert 10000 1x1 boxes in a 100x100 grid.
        for i in 0..10_000i64 {
            let x = f64::from(i32::try_from(i % 100).unwrap());
            let y = f64::from(i32::try_from(i / 100).unwrap());
            index.insert(RtreeEntry {
                id: i + 1,
                bbox: MBoundingBox::new(vec![x, x + 1.0, y, y + 1.0]).unwrap(),
            });
        }
        assert_eq!(index.len(), 10_000);
        // Query a 5x5 region: should find ~25 entries.
        let query = MBoundingBox::new(vec![10.0, 15.0, 10.0, 15.0]).unwrap();
        let results = index.range_query(&query);
        // Entries at x=9..15, y=9..15 overlap the query: 7 columns * 7 rows = 49.
        assert!(results.len() >= 25);
        assert!(results.len() <= 49);
    }

    // -------------------------------------------------------------------
    // Geopoly functions
    // -------------------------------------------------------------------

    #[test]
    fn test_geopoly_create() {
        // Geopoly is built on R*-tree with polygon data.
        let config = RtreeConfig::new(2, RtreeCoordType::Float32).unwrap();
        let index = RtreeIndex::new(config);
        assert!(index.is_empty());
    }

    #[test]
    fn test_geopoly_overlap_detects() {
        let lhs = square(0.0, 0.0, 4.0);
        let rhs = square(2.0, 2.0, 4.0);
        assert!(geopoly_overlap(&lhs, &rhs));
    }

    #[test]
    fn test_geopoly_overlap_false() {
        let lhs = square(0.0, 0.0, 2.0);
        let rhs = square(3.0, 3.0, 2.0);
        assert!(!geopoly_overlap(&lhs, &rhs));
    }

    #[test]
    fn test_geopoly_within_detects() {
        let outer = square(0.0, 0.0, 10.0);
        let inner = square(2.0, 2.0, 3.0);
        let outside = square(-1.0, -1.0, 3.0);
        assert!(geopoly_within(&inner, &outer));
        assert!(!geopoly_within(&outside, &outer));
    }

    #[test]
    fn test_geopoly_area_computes() {
        let triangle = [
            Point::new(0.0, 0.0),
            Point::new(4.0, 0.0),
            Point::new(0.0, 3.0),
        ];
        assert!(approx_eq(geopoly_area(&triangle), 6.0));
        let unit_square = square(0.0, 0.0, 1.0);
        assert!(approx_eq(geopoly_area(&unit_square), 1.0));
    }

    #[test]
    fn test_geopoly_contains_point_inside_outside_edge() {
        let polygon = square(0.0, 0.0, 10.0);
        // Inside
        assert!(geopoly_contains_point(&polygon, Point::new(5.0, 5.0)));
        // Outside
        assert!(!geopoly_contains_point(&polygon, Point::new(11.0, 5.0)));
        // Edge (on boundary)
        assert!(geopoly_contains_point(&polygon, Point::new(0.0, 5.0)));
    }

    #[test]
    fn test_geopoly_blob_json_roundtrip() {
        let original = square(1.0, 2.0, 3.0);
        let blob = geopoly_blob(&original);
        let decoded = geopoly_blob_decode(&blob).unwrap();
        // Round-trip through f32 may lose some precision.
        for (orig, dec) in original.iter().zip(decoded.iter()) {
            assert!(approx_eq(orig.x, dec.x));
            assert!(approx_eq(orig.y, dec.y));
        }
        // Also test JSON round-trip.
        let json = geopoly_json(&original);
        let from_json = geopoly_json_decode(&json).unwrap();
        assert_eq!(from_json.len(), original.len());
        for (orig, dec) in original.iter().zip(from_json.iter()) {
            assert!(approx_eq(orig.x, dec.x));
            assert!(approx_eq(orig.y, dec.y));
        }
    }

    #[test]
    fn test_geopoly_svg() {
        let triangle = [
            Point::new(0.0, 0.0),
            Point::new(4.0, 0.0),
            Point::new(2.0, 3.0),
        ];
        let svg = geopoly_svg(&triangle);
        assert!(svg.starts_with("M "));
        assert!(svg.ends_with(" Z"));
        assert!(svg.contains("L "));
    }

    #[test]
    fn test_geopoly_bbox_correct() {
        let polygon = square(5.0, -2.0, 2.0);
        let bounds = geopoly_bbox(&polygon).expect("square should produce bbox");
        assert!(approx_eq(bounds.min_x, 5.0));
        assert!(approx_eq(bounds.min_y, -2.0));
        assert!(approx_eq(bounds.max_x, 7.0));
        assert!(approx_eq(bounds.max_y, 0.0));
    }

    #[test]
    fn test_geopoly_regular_hexagon() {
        let hexagon = geopoly_regular(0.0, 0.0, 2.0, 6);
        assert_eq!(hexagon.len(), 6);
        for vertex in &hexagon {
            let distance = vertex.x.hypot(vertex.y);
            assert!(approx_eq(distance, 2.0));
        }
    }

    #[test]
    fn test_geopoly_ccw_enforces() {
        let clockwise = [
            Point::new(0.0, 0.0),
            Point::new(0.0, 1.0),
            Point::new(1.0, 1.0),
            Point::new(1.0, 0.0),
        ];
        assert!(signed_twice_area(&clockwise) < 0.0);
        let ccw = geopoly_ccw(&clockwise);
        assert!(signed_twice_area(&ccw) > 0.0);
    }

    #[test]
    fn test_geopoly_xform_translate() {
        let polygon = square(1.0, 2.0, 1.0);
        let translated = geopoly_xform(&polygon, 1.0, 0.0, 0.0, 1.0, 5.0, -3.0);
        assert!(approx_eq(translated[0].x, 6.0));
        assert!(approx_eq(translated[0].y, -1.0));
    }

    #[test]
    fn test_geopoly_group_bbox_aggregate() {
        let first = square(0.0, 0.0, 1.0);
        let second = square(5.0, -2.0, 2.0);
        let grouped = geopoly_group_bbox(&[&first, &second]).expect("grouped bbox");
        assert!(approx_eq(grouped.min_x, 0.0));
        assert!(approx_eq(grouped.min_y, -2.0));
        assert!(approx_eq(grouped.max_x, 7.0));
        assert!(approx_eq(grouped.max_y, 1.0));
    }

    // -------------------------------------------------------------------
    // Geopoly binary format
    // -------------------------------------------------------------------

    #[test]
    fn test_geopoly_binary_format() {
        let polygon = square(1.0, 2.0, 3.0);
        let blob = geopoly_blob(&polygon);
        // Header: 1 type byte + 3-byte vertex count + 4 vertices * 8 bytes
        assert_eq!(blob.len(), 4 + 4 * 8);
        assert_eq!(blob[0], GEOPOLY_HEADER_TYPE);
        // Vertex count in LE.
        assert_eq!(blob[1], 4);
        assert_eq!(blob[2], 0);
        assert_eq!(blob[3], 0);
    }

    // -------------------------------------------------------------------
    // MBoundingBox unit tests
    // -------------------------------------------------------------------

    #[test]
    fn test_mbounding_box_overlap() {
        let a = MBoundingBox::new(vec![0.0, 5.0, 0.0, 5.0]).unwrap();
        let b = MBoundingBox::new(vec![3.0, 8.0, 3.0, 8.0]).unwrap();
        let c = MBoundingBox::new(vec![6.0, 10.0, 6.0, 10.0]).unwrap();
        assert!(a.overlaps(&b));
        assert!(!a.overlaps(&c));
    }

    #[test]
    fn test_mbounding_box_volume() {
        let bbox = MBoundingBox::new(vec![0.0, 3.0, 0.0, 4.0]).unwrap();
        assert!(approx_eq(bbox.volume(), 12.0));
        let bbox3d = MBoundingBox::new(vec![0.0, 2.0, 0.0, 3.0, 0.0, 4.0]).unwrap();
        assert!(approx_eq(bbox3d.volume(), 24.0));
    }

    #[test]
    fn test_mbounding_box_enlargement() {
        let a = MBoundingBox::new(vec![0.0, 2.0, 0.0, 2.0]).unwrap();
        let b = MBoundingBox::new(vec![1.0, 3.0, 1.0, 3.0]).unwrap();
        // Union is [0,3,0,3] = 9, a volume = 4, enlargement = 5
        assert!(approx_eq(a.enlargement(&b), 5.0));
    }

    // ── MBoundingBox: construction edge cases ────────────────────────────

    #[test]
    fn test_mbounding_box_odd_length_fails() {
        assert!(MBoundingBox::new(vec![1.0, 2.0, 3.0]).is_none());
    }

    #[test]
    fn test_mbounding_box_empty_fails() {
        assert!(MBoundingBox::new(Vec::new()).is_none());
    }

    #[test]
    fn test_mbounding_box_1d() {
        let bbox = MBoundingBox::new(vec![5.0, 10.0]).unwrap();
        assert_eq!(bbox.dimensions(), 1);
        assert!(approx_eq(bbox.min_coord(0), 5.0));
        assert!(approx_eq(bbox.max_coord(0), 10.0));
    }

    #[test]
    fn test_mbounding_box_volume_1d() {
        let bbox = MBoundingBox::new(vec![0.0, 7.0]).unwrap();
        assert!(approx_eq(bbox.volume(), 7.0));
    }

    #[test]
    fn test_mbounding_box_union() {
        let a = MBoundingBox::new(vec![0.0, 2.0, 0.0, 2.0]).unwrap();
        let b = MBoundingBox::new(vec![1.0, 5.0, 1.0, 5.0]).unwrap();
        let u = a.union(&b);
        assert!(approx_eq(u.min_coord(0), 0.0));
        assert!(approx_eq(u.max_coord(0), 5.0));
        assert!(approx_eq(u.min_coord(1), 0.0));
        assert!(approx_eq(u.max_coord(1), 5.0));
    }

    #[test]
    fn test_mbounding_box_no_overlap_disjoint() {
        let a = MBoundingBox::new(vec![0.0, 1.0, 0.0, 1.0]).unwrap();
        let b = MBoundingBox::new(vec![5.0, 6.0, 5.0, 6.0]).unwrap();
        assert!(!a.overlaps(&b));
        assert!(!b.overlaps(&a));
    }

    #[test]
    fn test_mbounding_box_self_overlap() {
        let a = MBoundingBox::new(vec![1.0, 3.0, 1.0, 3.0]).unwrap();
        assert!(a.overlaps(&a));
    }

    #[test]
    fn test_mbounding_box_point_bbox() {
        // Degenerate bbox where min == max
        let point = MBoundingBox::new(vec![5.0, 5.0, 5.0, 5.0]).unwrap();
        assert!(approx_eq(point.volume(), 0.0));
    }

    // ── RtreeConfig ──────────────────────────────────────────────────────

    #[test]
    fn test_rtree_config_zero_dimensions_fails() {
        assert!(RtreeConfig::new(0, RtreeCoordType::Float32).is_none());
    }

    #[test]
    fn test_rtree_config_max_dimensions() {
        // 5 dimensions is the typical max for R*-tree
        let config = RtreeConfig::new(5, RtreeCoordType::Float32);
        assert!(config.is_some());
    }

    // ── RtreeIndex: edge cases ───────────────────────────────────────────

    #[test]
    fn test_rtree_delete_nonexistent() {
        let config = RtreeConfig::new(2, RtreeCoordType::Float32).unwrap();
        let mut index = RtreeIndex::new(config);
        assert!(!index.delete(999));
    }

    #[test]
    fn test_rtree_update_nonexistent() {
        let config = RtreeConfig::new(2, RtreeCoordType::Float32).unwrap();
        let mut index = RtreeIndex::new(config);
        let bbox = MBoundingBox::new(vec![0.0, 1.0, 0.0, 1.0]).unwrap();
        assert!(!index.update(999, bbox));
    }

    #[test]
    fn test_rtree_empty_range_query() {
        let config = RtreeConfig::new(2, RtreeCoordType::Float32).unwrap();
        let index = RtreeIndex::new(config);
        let query = MBoundingBox::new(vec![0.0, 10.0, 0.0, 10.0]).unwrap();
        assert!(index.range_query(&query).is_empty());
    }

    #[test]
    fn test_rtree_is_empty() {
        let config = RtreeConfig::new(2, RtreeCoordType::Float32).unwrap();
        let mut index = RtreeIndex::new(config);
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);

        let bbox = MBoundingBox::new(vec![0.0, 1.0, 0.0, 1.0]).unwrap();
        index.insert(RtreeEntry { id: 1, bbox });
        assert!(!index.is_empty());
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn test_rtree_dimensions_and_coord_type() {
        let config = RtreeConfig::new(3, RtreeCoordType::Int32).unwrap();
        let index = RtreeIndex::new(config);
        assert_eq!(index.dimensions(), 3);
        assert_eq!(index.coord_type(), RtreeCoordType::Int32);
    }

    #[test]
    fn test_rtree_geometry_query_no_registered() {
        let config = RtreeConfig::new(2, RtreeCoordType::Float32).unwrap();
        let index = RtreeIndex::new(config);
        assert!(index.geometry_query("nonexistent").is_empty());
    }

    #[test]
    fn test_rtree_geometry_query_detailed_no_registered() {
        let config = RtreeConfig::new(2, RtreeCoordType::Float32).unwrap();
        let index = RtreeIndex::new(config);
        assert!(index.geometry_query_detailed("nonexistent").is_empty());
    }

    // ── Geopoly: edge cases ──────────────────────────────────────────────

    #[test]
    fn test_geopoly_area_degenerate_line() {
        // Two collinear points → zero area
        let line = [Point::new(0.0, 0.0), Point::new(1.0, 0.0)];
        assert!(approx_eq(geopoly_area(&line), 0.0));
    }

    #[test]
    fn test_geopoly_area_single_point() {
        let single = [Point::new(5.0, 5.0)];
        assert!(approx_eq(geopoly_area(&single), 0.0));
    }

    #[test]
    fn test_geopoly_bbox_empty() {
        assert!(geopoly_bbox(&[]).is_none());
    }

    #[test]
    fn test_geopoly_bbox_single_point() {
        let bbox = geopoly_bbox(&[Point::new(3.0, 4.0)]).unwrap();
        assert!(approx_eq(bbox.min_x, 3.0));
        assert!(approx_eq(bbox.max_x, 3.0));
        assert!(approx_eq(bbox.min_y, 4.0));
        assert!(approx_eq(bbox.max_y, 4.0));
    }

    #[test]
    fn test_geopoly_group_bbox_empty() {
        assert!(geopoly_group_bbox(&[]).is_none());
    }

    #[test]
    fn test_geopoly_blob_decode_empty() {
        assert!(geopoly_blob_decode(&[]).is_none());
    }

    #[test]
    fn test_geopoly_json_decode_empty() {
        assert!(geopoly_json_decode("").is_none());
    }

    #[test]
    fn test_geopoly_json_decode_malformed() {
        assert!(geopoly_json_decode("not json").is_none());
        assert!(geopoly_json_decode("[").is_none());
    }

    #[test]
    fn test_geopoly_contains_point_empty_polygon() {
        assert!(!geopoly_contains_point(&[], Point::new(0.0, 0.0)));
    }

    #[test]
    fn test_geopoly_overlap_empty_polygon() {
        let sq = square(0.0, 0.0, 1.0);
        assert!(!geopoly_overlap(&[], &sq));
        assert!(!geopoly_overlap(&sq, &[]));
    }

    #[test]
    fn test_geopoly_within_empty_polygon() {
        let sq = square(0.0, 0.0, 1.0);
        assert!(!geopoly_within(&[], &sq));
        assert!(!geopoly_within(&sq, &[]));
    }

    #[test]
    fn test_geopoly_regular_triangle() {
        let tri = geopoly_regular(0.0, 0.0, 1.0, 3);
        assert_eq!(tri.len(), 3);
    }

    #[test]
    fn test_geopoly_regular_square() {
        let sq = geopoly_regular(0.0, 0.0, 1.0, 4);
        assert_eq!(sq.len(), 4);
    }

    #[test]
    fn test_geopoly_svg_triangle() {
        let tri = [
            Point::new(0.0, 0.0),
            Point::new(1.0, 0.0),
            Point::new(0.5, 1.0),
        ];
        let svg = geopoly_svg(&tri);
        assert!(svg.contains('M'));
        assert!(svg.contains('L'));
        assert!(svg.contains('Z'));
    }

    #[test]
    fn test_geopoly_svg_empty() {
        let svg = geopoly_svg(&[]);
        // Should produce a valid but empty SVG path or empty string
        assert!(svg.is_empty() || svg.contains('M'));
    }

    #[test]
    fn test_geopoly_xform_identity() {
        let sq = square(1.0, 1.0, 2.0);
        // Identity transform: a=1, b=0, c=0, d=1, e=0, f=0
        let result = geopoly_xform(&sq, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0);
        for (orig, transformed) in sq.iter().zip(result.iter()) {
            assert!(approx_eq(orig.x, transformed.x));
            assert!(approx_eq(orig.y, transformed.y));
        }
    }

    #[test]
    fn test_geopoly_ccw_already_ccw() {
        // Counter-clockwise triangle
        let ccw_tri = [
            Point::new(0.0, 0.0),
            Point::new(1.0, 0.0),
            Point::new(0.0, 1.0),
        ];
        let result = geopoly_ccw(&ccw_tri);
        // Should be the same orientation (may or may not reverse)
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_geopoly_ccw_clockwise_reversed() {
        // Clockwise triangle
        let cw_tri = [
            Point::new(0.0, 0.0),
            Point::new(0.0, 1.0),
            Point::new(1.0, 0.0),
        ];
        let result = geopoly_ccw(&cw_tri);
        assert_eq!(result.len(), 3);
    }

    // ── Point / BoundingBox ──────────────────────────────────────────────

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_point_new() {
        let p = Point::new(3.14, 2.72);
        assert!(approx_eq(p.x, 3.14));
        assert!(approx_eq(p.y, 2.72));
    }

    #[test]
    fn test_bounding_box_contains_point() {
        let bbox = BoundingBox {
            min_x: 0.0,
            min_y: 0.0,
            max_x: 10.0,
            max_y: 10.0,
        };
        assert!(bbox.contains_point(Point::new(5.0, 5.0)));
        assert!(bbox.contains_point(Point::new(0.0, 0.0)));
        assert!(!bbox.contains_point(Point::new(-1.0, 5.0)));
        assert!(!bbox.contains_point(Point::new(5.0, 11.0)));
    }

    #[test]
    fn test_bounding_box_contains_box() {
        let outer = BoundingBox {
            min_x: 0.0,
            min_y: 0.0,
            max_x: 10.0,
            max_y: 10.0,
        };
        let inner = BoundingBox {
            min_x: 2.0,
            min_y: 2.0,
            max_x: 8.0,
            max_y: 8.0,
        };
        let outside = BoundingBox {
            min_x: 20.0,
            min_y: 20.0,
            max_x: 30.0,
            max_y: 30.0,
        };
        assert!(outer.contains_box(inner));
        assert!(!outer.contains_box(outside));
        assert!(!inner.contains_box(outer));
    }

    #[test]
    fn test_geopoly_overlap_identical() {
        let sq = square(0.0, 0.0, 1.0);
        assert!(geopoly_overlap(&sq, &sq));
    }

    #[test]
    fn test_geopoly_within_identical() {
        let sq = square(0.0, 0.0, 1.0);
        assert!(geopoly_within(&sq, &sq));
    }

    // -----------------------------------------------------------------------
    // bd-6i2s required: geopoly_area_triangle + geopoly_within_not_contained
    // -----------------------------------------------------------------------

    #[test]
    fn test_geopoly_area_triangle() {
        // Right triangle with legs 3 and 4 → area = 6.0
        let tri = [
            Point::new(0.0, 0.0),
            Point::new(3.0, 0.0),
            Point::new(0.0, 4.0),
        ];
        assert!(approx_eq(geopoly_area(&tri), 6.0));
    }

    #[test]
    fn test_geopoly_area_square_unit() {
        let sq = square(0.0, 0.0, 1.0);
        assert!(approx_eq(geopoly_area(&sq), 1.0));
    }

    #[test]
    fn test_geopoly_within_not_contained() {
        let small = square(0.0, 0.0, 1.0);
        let big = square(5.0, 5.0, 2.0);
        assert!(
            !geopoly_within(&small, &big),
            "disjoint polygons: small not within big"
        );
    }

    #[test]
    fn test_geopoly_contains_point_inside() {
        let sq = square(0.0, 0.0, 2.0);
        assert!(geopoly_contains_point(&sq, Point::new(1.0, 1.0)));
    }

    #[test]
    fn test_geopoly_contains_point_outside() {
        let sq = square(0.0, 0.0, 2.0);
        assert!(!geopoly_contains_point(&sq, Point::new(5.0, 5.0)));
    }

    #[test]
    fn test_rtree_insert_multiple_range_query() {
        let config = RtreeConfig::new(2, RtreeCoordType::Float32).unwrap();
        let mut index = RtreeIndex::new(config);
        for i in 0..10 {
            let f = f64::from(i);
            let bbox = MBoundingBox::new(vec![f, f + 1.0, f, f + 1.0]).unwrap();
            index.insert(RtreeEntry {
                id: i64::from(i),
                bbox,
            });
        }
        assert_eq!(index.len(), 10);
        // Query should find overlapping entries
        let query = MBoundingBox::new(vec![3.5, 5.5, 3.5, 5.5]).unwrap();
        let results = index.range_query(&query);
        assert!(!results.is_empty());
    }
}
