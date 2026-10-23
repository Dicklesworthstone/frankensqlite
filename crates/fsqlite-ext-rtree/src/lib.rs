use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use fsqlite_types::SqliteValue;

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
        if !coords.len().is_multiple_of(2) || coords.is_empty() || coords.len() > MAX_DIMENSIONS * 2
        {
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
#[derive(Clone)]
pub struct RtreeIndex {
    config: RtreeConfig,
    entries: Vec<RtreeEntry>,
    entry_ids: HashSet<i64>,
    /// Auxiliary (non-indexed) column values per rowid, in declared order.
    /// Kept alongside `entries` (not in the bbox) so spatial queries ignore them
    /// while `column()` can still project them. Absent id ⇒ all-NULL aux.
    aux_by_id: HashMap<i64, Vec<SqliteValue>>,
    geometry_registry: HashMap<String, Arc<dyn RtreeGeometry>>,
}

impl std::fmt::Debug for RtreeIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RtreeIndex")
            .field("config", &self.config)
            .field("entries", &self.entries)
            .field("entry_ids", &self.entry_ids)
            .field("aux_columns", &self.aux_by_id.len())
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
            entry_ids: HashSet::new(),
            aux_by_id: HashMap::new(),
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
        if !self.entry_ids.insert(entry.id) {
            return false;
        }
        self.entries.push(entry);
        true
    }

    /// Delete an entry by rowid.
    ///
    /// Returns `true` if an entry was removed.
    pub fn delete(&mut self, id: i64) -> bool {
        if !self.entry_ids.remove(&id) {
            return false;
        }
        self.entries.retain(|e| e.id != id);
        self.aux_by_id.remove(&id);
        true
    }

    /// Set (or clear, when empty) the auxiliary column values for a rowid.
    pub fn set_aux(&mut self, id: i64, aux: Vec<SqliteValue>) {
        if aux.is_empty() {
            self.aux_by_id.remove(&id);
        } else {
            self.aux_by_id.insert(id, aux);
        }
    }

    /// Auxiliary column values for a rowid, if any were stored.
    #[must_use]
    pub fn aux_for_id(&self, id: i64) -> Option<&Vec<SqliteValue>> {
        self.aux_by_id.get(&id)
    }

    #[must_use]
    pub fn contains_id(&self, id: i64) -> bool {
        self.entry_ids.contains(&id)
    }

    #[must_use]
    pub fn bbox_for_id(&self, id: i64) -> Option<MBoundingBox> {
        self.entries
            .iter()
            .find(|entry| entry.id == id)
            .map(|entry| entry.bbox.clone())
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
        self.geometry_registry
            .insert(name.to_owned(), Arc::from(geom));
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
// Virtual table adapter (stock SQLite shadow-table storage)
// ---------------------------------------------------------------------------

mod stock;
mod vtab;

pub use vtab::{
    RtreeCursor, RtreeShadowRowChanges, RtreeShadowWrites, RtreeVirtualTable,
    rtree_i32_module_factory, rtree_module_factory,
};

// ---------------------------------------------------------------------------
// Geopoly SQL functions
// ---------------------------------------------------------------------------

mod geopoly;

pub use geopoly::{
    GeopolyAreaFunc, GeopolyBboxFunc, GeopolyBlobFunc, GeopolyCcwFunc, GeopolyContainsPointFunc,
    GeopolyDebugFunc, GeopolyGroupBboxFunc, GeopolyJsonFunc, GeopolyOverlapFunc,
    GeopolyRegularFunc, GeopolySvgFunc, GeopolyWithinFunc, GeopolyXformFunc, geopoly_area,
    geopoly_bbox, geopoly_bbox_polygon, geopoly_blob, geopoly_blob_decode, geopoly_ccw,
    geopoly_contains_point, geopoly_group_bbox, geopoly_json, geopoly_json_decode, geopoly_overlap,
    geopoly_regular, geopoly_svg, geopoly_svg_with_attributes, geopoly_within, geopoly_xform,
    register_geopoly_scalars,
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_error::FrankenError;
    use fsqlite_func::vtab::VtabModuleFactory;
    use fsqlite_types::cx::Cx;

    fn approx_eq(left: f64, right: f64) -> bool {
        (left - right).abs() < 1e-4
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
            bbox: MBoundingBox::new(vec![0.0, 5.0, 0.0, 10.0]).unwrap(),
        };
        assert!(index.insert(entry));
        assert_eq!(index.len(), 1);
        // Duplicate id rejected.
        let dup = RtreeEntry {
            id: 1,
            bbox: MBoundingBox::new(vec![5.0, 15.0, 5.0, 20.0]).unwrap(),
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
            bbox: MBoundingBox::new(vec![0.0, 5.0, 0.0, 10.0]).unwrap(),
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
            bbox: MBoundingBox::new(vec![0.0, 5.0, 0.0, 10.0]).unwrap(),
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

    #[test]
    fn test_rtree_module_args_reject_incomplete_dimension_pair() {
        let cx = Cx::new();
        let factory = rtree_module_factory();
        // Fewer than one full coordinate pair → "Too few columns" (stock message).
        let err = match factory.create(&cx, &["rtree", "main", "spatial", "id", "min_x"]) {
            Ok(_) => panic!("incomplete rtree args should be rejected"),
            Err(err) => err,
        };
        assert!(matches!(err, FrankenError::FunctionError(_)));
        assert_eq!(err.to_string(), "Too few columns for an rtree table");

        // An odd number of coordinate columns → "Wrong number of columns".
        let err = match factory.create(&cx, &["rtree", "main", "spatial", "id", "x0", "x1", "x2"]) {
            Ok(_) => panic!("odd coordinate count should be rejected"),
            Err(err) => err,
        };
        assert_eq!(
            err.to_string(),
            "Wrong number of columns for an rtree table"
        );
    }

    /// bd-2pibv: auxiliary columns must follow every coordinate column; a `+aux`
    /// declared before a coordinate is rejected with SQLite's exact message.
    #[test]
    fn test_rtree_auxiliary_columns_must_be_last() {
        let cx = Cx::new();
        let factory = rtree_module_factory();
        let err = match factory.create(
            &cx,
            &["rtree", "main", "spatial", "id", "x0", "+name TEXT", "x1"],
        ) {
            Ok(_) => panic!("auxiliary column before a coordinate should be rejected"),
            Err(err) => err,
        };
        assert_eq!(err.to_string(), "Auxiliary rtree columns must be last");

        // Auxiliary columns get NONE affinity ('A'); coordinates keep 'E'.
        let cols =
            factory.column_info(&["rtree", "main", "spatial", "id", "x0", "x1", "+name TEXT"]);
        assert_eq!(
            cols,
            vec![
                ("id".to_owned(), 'D'),
                ("x0".to_owned(), 'E'),
                ("x1".to_owned(), 'E'),
                ("name".to_owned(), 'A'),
            ]
        );
    }

    #[test]
    fn test_rtree_factory_column_info_matches_coord_type() {
        let full_args = [
            "rtree", "main", "spatial", "id", "min_x", "max_x", "min_y", "max_y",
        ];
        let float_columns = rtree_module_factory().column_info(&full_args);
        assert_eq!(
            float_columns,
            vec![
                ("id".to_owned(), 'D'),
                ("min_x".to_owned(), 'E'),
                ("max_x".to_owned(), 'E'),
                ("min_y".to_owned(), 'E'),
                ("max_y".to_owned(), 'E'),
            ]
        );

        let int_columns = rtree_i32_module_factory().column_info(&full_args);
        assert_eq!(
            int_columns,
            vec![
                ("id".to_owned(), 'D'),
                ("min_x".to_owned(), 'D'),
                ("max_x".to_owned(), 'D'),
                ("min_y".to_owned(), 'D'),
                ("max_y".to_owned(), 'D'),
            ]
        );
    }

    #[test]
    fn test_rtree_factory_accepts_sqlite_full_argv() {
        let cx = Cx::new();
        let args = [
            "rtree", "main", "spatial", "id", "min_x", "max_x", "min_y", "max_y",
        ];
        let factory = rtree_module_factory();

        assert!(factory.create(&cx, &args).is_ok());
        assert_eq!(
            factory.column_info(&args),
            vec![
                ("id".to_owned(), 'D'),
                ("min_x".to_owned(), 'E'),
                ("max_x".to_owned(), 'E'),
                ("min_y".to_owned(), 'E'),
                ("max_y".to_owned(), 'E'),
            ]
        );
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
