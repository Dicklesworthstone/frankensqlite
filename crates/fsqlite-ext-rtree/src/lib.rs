use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use fsqlite_error::{FrankenError, Result};
use fsqlite_func::vtab::{
    ColumnContext, ConstraintOp, ErasedVtabInstance, IndexInfo, TransactionalVtabState,
    VirtualTable, VirtualTableCursor, VtabModuleFactory,
};
use fsqlite_func::{FunctionRegistry, ScalarFunction};
use fsqlite_types::{SqliteValue, cx::Cx};

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
#[derive(Clone)]
pub struct RtreeIndex {
    config: RtreeConfig,
    entries: Vec<RtreeEntry>,
    geometry_registry: HashMap<String, Arc<dyn RtreeGeometry>>,
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

    #[must_use]
    pub fn contains_id(&self, id: i64) -> bool {
        self.entries.iter().any(|entry| entry.id == id)
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
// Virtual table adapter
// ---------------------------------------------------------------------------

const RTREE_SCAN_FULL: i32 = 0;
const RTREE_SCAN_BBOX: i32 = 1;
const RTREE_SCAN_GEOMETRY: i32 = 2;

#[derive(Debug, Clone)]
struct ParsedRtreeModuleArgs {
    column_names: Vec<String>,
    dimensions: usize,
}

fn parse_rtree_module_args(args: &[&str]) -> Result<ParsedRtreeModuleArgs> {
    if args.len() < 3 {
        return Err(FrankenError::function_error(
            "rtree requires an id column plus at least one min/max coordinate pair",
        ));
    }
    if args.len() % 2 == 0 {
        return Err(FrankenError::function_error(
            "rtree requires an odd number of arguments: id plus min/max coordinate pairs",
        ));
    }

    let dimensions = (args.len() - 1) / 2;
    if dimensions > MAX_DIMENSIONS {
        return Err(FrankenError::function_error(format!(
            "rtree supports at most {MAX_DIMENSIONS} dimensions",
        )));
    }

    let mut seen = HashSet::new();
    let mut column_names = Vec::with_capacity(args.len());
    for arg in args {
        let column = arg
            .trim()
            .trim_matches(|ch| matches!(ch, '"' | '\'' | '`' | '[' | ']'));
        if column.is_empty() {
            return Err(FrankenError::function_error(
                "rtree column names must not be empty",
            ));
        }
        if column.contains('=') {
            return Err(FrankenError::function_error(
                "rtree does not support option assignments in module arguments",
            ));
        }
        let key = column.to_ascii_lowercase();
        if !seen.insert(key) {
            return Err(FrankenError::function_error(format!(
                "rtree column '{column}' is declared more than once",
            )));
        }
        column_names.push(column.to_owned());
    }

    Ok(ParsedRtreeModuleArgs {
        column_names,
        dimensions,
    })
}

fn column_affinity(coord_type: RtreeCoordType, is_id: bool) -> char {
    if is_id || matches!(coord_type, RtreeCoordType::Int32) {
        'D'
    } else {
        'E'
    }
}

fn parse_coordinate_value(value: &SqliteValue, coord_type: RtreeCoordType) -> Result<f64> {
    match coord_type {
        RtreeCoordType::Float32 => {
            let coord = value.to_float();
            if !coord.is_finite() {
                return Err(FrankenError::function_error(
                    "rtree coordinates must be finite",
                ));
            }
            #[allow(clippy::cast_possible_truncation)]
            let narrowed = coord as f32;
            Ok(f64::from(narrowed))
        }
        RtreeCoordType::Int32 => {
            let narrowed = i32::try_from(value.to_integer()).map_err(|_| {
                FrankenError::function_error("rtree_i32 coordinates must fit in signed 32-bit")
            })?;
            Ok(f64::from(narrowed))
        }
    }
}

/// Runtime virtual-table instance for `CREATE VIRTUAL TABLE ... USING rtree(...)`.
#[derive(Debug, Clone)]
pub struct RtreeVirtualTable {
    index: RtreeIndex,
    txn_state: TransactionalVtabState<RtreeVirtualTableSnapshot>,
}

#[derive(Debug, Clone)]
struct RtreeVirtualTableSnapshot {
    index: RtreeIndex,
}

impl RtreeVirtualTable {
    fn from_args(args: &[&str], coord_type: RtreeCoordType) -> Result<Self> {
        let parsed = parse_rtree_module_args(args)?;
        let config = RtreeConfig::new(parsed.dimensions, coord_type).ok_or_else(|| {
            FrankenError::function_error("rtree dimensions must be between 1 and 5")
        })?;
        Ok(Self {
            index: RtreeIndex::new(config),
            txn_state: TransactionalVtabState::default(),
        })
    }

    fn snapshot_state(&self) -> RtreeVirtualTableSnapshot {
        RtreeVirtualTableSnapshot {
            index: self.index.clone(),
        }
    }

    fn restore_state(&mut self, snapshot: RtreeVirtualTableSnapshot) {
        self.index = snapshot.index;
    }

    fn next_rowid(&self) -> i64 {
        self.index
            .entries
            .iter()
            .map(|entry| entry.id)
            .max()
            .unwrap_or(0)
            + 1
    }

    fn parse_bbox_values(&self, values: &[SqliteValue]) -> Result<MBoundingBox> {
        let expected = self.index.dimensions() * 2;
        if values.len() != expected {
            return Err(FrankenError::function_error(format!(
                "rtree expected {expected} coordinate values, got {}",
                values.len()
            )));
        }

        let coords = values
            .iter()
            .map(|value| parse_coordinate_value(value, self.index.coord_type()))
            .collect::<Result<Vec<_>>>()?;
        MBoundingBox::new(coords).ok_or_else(|| {
            FrankenError::function_error("rtree coordinates must form complete min/max pairs")
        })
    }

    /// Register a custom geometry callback on a live virtual table instance.
    pub fn register_geometry(&mut self, name: &str, geom: Box<dyn RtreeGeometry>) {
        self.index.register_geometry(name, geom);
    }
}

impl VirtualTable for RtreeVirtualTable {
    type Cursor = RtreeCursor;

    fn create(_cx: &Cx, args: &[&str]) -> Result<Self> {
        Self::from_args(args, RtreeCoordType::Float32)
    }

    fn connect(cx: &Cx, args: &[&str]) -> Result<Self> {
        Self::create(cx, args)
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        let mut geometry_constraint = None;
        let mut lower_bounds = vec![None; self.index.dimensions()];
        let mut upper_bounds = vec![None; self.index.dimensions()];

        for (index, constraint) in info.constraints.iter().enumerate() {
            if !constraint.usable {
                continue;
            }

            if constraint.op == ConstraintOp::Match {
                geometry_constraint = Some(index);
                continue;
            }

            let Ok(column_index) = usize::try_from(constraint.column) else {
                continue;
            };
            if column_index == 0 {
                continue;
            }

            let coord_index = column_index - 1;
            let dimension = coord_index / 2;
            if dimension >= self.index.dimensions() {
                continue;
            }

            if coord_index % 2 == 0 && constraint.op == ConstraintOp::Le {
                upper_bounds[dimension] = Some(index);
            } else if constraint.op == ConstraintOp::Ge {
                lower_bounds[dimension] = Some(index);
            }
        }

        if lower_bounds.iter().all(Option::is_some) && upper_bounds.iter().all(Option::is_some) {
            for dimension in 0..self.index.dimensions() {
                let Some(lower_idx) = lower_bounds[dimension] else {
                    continue;
                };
                info.constraint_usage[lower_idx].argv_index = i32::try_from(dimension * 2 + 1)
                    .map_err(|error| {
                        FrankenError::function_error(format!(
                            "rtree lower-bound argv index overflow: {error}"
                        ))
                    })?;
                info.constraint_usage[lower_idx].omit = true;

                let Some(upper_idx) = upper_bounds[dimension] else {
                    continue;
                };
                info.constraint_usage[upper_idx].argv_index = i32::try_from(dimension * 2 + 2)
                    .map_err(|error| {
                        FrankenError::function_error(format!(
                            "rtree upper-bound argv index overflow: {error}"
                        ))
                    })?;
                info.constraint_usage[upper_idx].omit = true;
            }

            info.idx_num = RTREE_SCAN_BBOX;
            info.estimated_cost = 10.0;
            info.estimated_rows = 64;
            return Ok(());
        }

        if let Some(index) = geometry_constraint {
            info.constraint_usage[index].argv_index = 1;
            info.constraint_usage[index].omit = true;
            info.idx_num = RTREE_SCAN_GEOMETRY;
            info.estimated_cost = 25.0;
            info.estimated_rows = 128;
            return Ok(());
        }

        info.idx_num = RTREE_SCAN_FULL;
        info.estimated_cost = 1_000_000.0;
        #[allow(clippy::cast_possible_wrap)]
        {
            info.estimated_rows = self.index.entries.len() as i64;
        }
        Ok(())
    }

    fn open(&self) -> Result<Self::Cursor> {
        Ok(RtreeCursor {
            index: self.index.clone(),
            rows: Vec::new(),
            pos: 0,
        })
    }

    fn begin(&mut self, _cx: &Cx) -> Result<()> {
        self.txn_state.begin(self.snapshot_state());
        Ok(())
    }

    fn sync_txn(&mut self, _cx: &Cx) -> Result<()> {
        Ok(())
    }

    fn update(&mut self, _cx: &Cx, args: &[SqliteValue]) -> Result<Option<i64>> {
        if args.is_empty() {
            return Err(FrankenError::function_error("rtree: empty update args"));
        }

        if args.len() == 1 && !args[0].is_null() {
            let old_rowid = args[0].to_integer();
            self.index.delete(old_rowid);
            return Ok(None);
        }

        let old_rowid = args.first().and_then(SqliteValue::as_integer);
        let new_rowid = args
            .get(1)
            .and_then(SqliteValue::as_integer)
            .or(old_rowid)
            .unwrap_or_else(|| self.next_rowid());

        let coordinate_values = match args.len().saturating_sub(2) {
            count if count == self.index.dimensions() * 2 => &args[2..],
            count if count == self.index.dimensions() * 2 + 1 => {
                if let Some(id_value) = args[2].as_integer() {
                    if id_value != new_rowid {
                        return Err(FrankenError::PrimaryKeyViolation);
                    }
                }
                &args[3..]
            }
            count => {
                return Err(FrankenError::function_error(format!(
                    "rtree update expected {} or {} payload values, got {count}",
                    self.index.dimensions() * 2,
                    self.index.dimensions() * 2 + 1
                )));
            }
        };

        let bbox = self.parse_bbox_values(coordinate_values)?;

        if let Some(old_rowid) = old_rowid {
            if old_rowid != new_rowid {
                if self.index.contains_id(new_rowid) {
                    return Err(FrankenError::PrimaryKeyViolation);
                }
                let previous_bbox = self.index.bbox_for_id(old_rowid).ok_or_else(|| {
                    FrankenError::Internal("rtree update referenced a missing rowid".to_owned())
                })?;
                let deleted = self.index.delete(old_rowid);
                if !deleted {
                    return Err(FrankenError::Internal(
                        "rtree update referenced a missing rowid".to_owned(),
                    ));
                }
                if !self.index.insert(RtreeEntry {
                    id: new_rowid,
                    bbox,
                }) {
                    let restored = self.index.insert(RtreeEntry {
                        id: old_rowid,
                        bbox: previous_bbox,
                    });
                    debug_assert!(restored, "rtree rollback after failed rowid change");
                    return Err(FrankenError::Internal(
                        "rtree update could not replace rowid".to_owned(),
                    ));
                }
                return Ok(None);
            }

            if !self.index.update(old_rowid, bbox) {
                return Err(FrankenError::Internal(
                    "rtree update referenced a missing rowid".to_owned(),
                ));
            }
            return Ok(None);
        }

        if !self.index.insert(RtreeEntry {
            id: new_rowid,
            bbox,
        }) {
            return Err(FrankenError::PrimaryKeyViolation);
        }
        Ok(Some(new_rowid))
    }

    fn commit(&mut self, _cx: &Cx) -> Result<()> {
        self.txn_state.commit();
        Ok(())
    }

    fn rollback(&mut self, _cx: &Cx) -> Result<()> {
        if let Some(snapshot) = self.txn_state.rollback() {
            self.restore_state(snapshot);
        }
        Ok(())
    }

    fn savepoint(&mut self, _cx: &Cx, n: i32) -> Result<()> {
        self.txn_state.savepoint(n, self.snapshot_state());
        Ok(())
    }

    fn release(&mut self, _cx: &Cx, n: i32) -> Result<()> {
        self.txn_state.release(n);
        Ok(())
    }

    fn rollback_to(&mut self, _cx: &Cx, n: i32) -> Result<()> {
        if let Some(snapshot) = self.txn_state.rollback_to(n) {
            self.restore_state(snapshot);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RtreeCursor {
    index: RtreeIndex,
    rows: Vec<RtreeEntry>,
    pos: usize,
}

impl RtreeCursor {
    fn current_row(&self) -> Result<&RtreeEntry> {
        self.rows.get(self.pos).ok_or_else(|| {
            FrankenError::function_error("rtree cursor is out of bounds for the current row")
        })
    }

    fn coordinate_value(&self, coord: f64) -> Result<SqliteValue> {
        match self.index.coord_type() {
            RtreeCoordType::Float32 => Ok(SqliteValue::Float(coord)),
            RtreeCoordType::Int32 => {
                if !coord.is_finite() || coord < f64::from(i32::MIN) || coord > f64::from(i32::MAX)
                {
                    return Err(FrankenError::function_error(
                        "rtree_i32 cursor produced an out-of-range coordinate",
                    ));
                }
                #[allow(clippy::cast_possible_truncation)]
                let coord_i32 = coord as i32;
                Ok(SqliteValue::Integer(i64::from(coord_i32)))
            }
        }
    }
}

impl VirtualTableCursor for RtreeCursor {
    fn filter(
        &mut self,
        _cx: &Cx,
        idx_num: i32,
        _idx_str: Option<&str>,
        args: &[SqliteValue],
    ) -> Result<()> {
        self.pos = 0;
        self.rows = match idx_num {
            RTREE_SCAN_FULL => self.index.entries.clone(),
            RTREE_SCAN_BBOX => {
                let expected = self.index.dimensions() * 2;
                if args.len() != expected {
                    return Err(FrankenError::function_error(format!(
                        "rtree bbox filter expected {expected} arguments, got {}",
                        args.len()
                    )));
                }
                let coords = args
                    .iter()
                    .map(|value| parse_coordinate_value(value, self.index.coord_type()))
                    .collect::<Result<Vec<_>>>()?;
                let query_bbox = MBoundingBox::new(coords).ok_or_else(|| {
                    FrankenError::function_error(
                        "rtree bbox filter arguments must form complete min/max pairs",
                    )
                })?;
                self.index
                    .range_query(&query_bbox)
                    .into_iter()
                    .cloned()
                    .collect()
            }
            RTREE_SCAN_GEOMETRY => {
                let geometry_name =
                    args.first().and_then(SqliteValue::as_text).ok_or_else(|| {
                        FrankenError::function_error(
                            "rtree geometry filters require a geometry callback name",
                        )
                    })?;
                self.index
                    .geometry_query(geometry_name)
                    .into_iter()
                    .cloned()
                    .collect()
            }
            _ => {
                return Err(FrankenError::function_error(format!(
                    "rtree does not recognize scan strategy {idx_num}",
                )));
            }
        };
        Ok(())
    }

    fn next(&mut self, _cx: &Cx) -> Result<()> {
        if self.pos < self.rows.len() {
            self.pos += 1;
        }
        Ok(())
    }

    fn eof(&self) -> bool {
        self.pos >= self.rows.len()
    }

    fn column(&self, ctx: &mut ColumnContext, col: i32) -> Result<()> {
        let row = self.current_row()?;
        if col == 0 {
            ctx.set_value(SqliteValue::Integer(row.id));
            return Ok(());
        }

        let coordinate_index = usize::try_from(col - 1).map_err(|error| {
            FrankenError::function_error(format!("rtree column index conversion failed: {error}",))
        })?;
        if let Some(coord) = row.bbox.coords.get(coordinate_index) {
            ctx.set_value(self.coordinate_value(*coord)?);
        } else {
            ctx.set_value(SqliteValue::Null);
        }
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.current_row()?.id)
    }
}

#[derive(Debug, Clone, Copy)]
struct RtreeFactory {
    coord_type: RtreeCoordType,
}

impl RtreeFactory {
    const fn new(coord_type: RtreeCoordType) -> Self {
        Self { coord_type }
    }
}

impl VtabModuleFactory for RtreeFactory {
    fn create(&self, _cx: &Cx, args: &[&str]) -> Result<Box<dyn ErasedVtabInstance>> {
        let vtab = RtreeVirtualTable::from_args(args, self.coord_type)?;
        Ok(Box::new(vtab))
    }

    fn connect(&self, cx: &Cx, args: &[&str]) -> Result<Box<dyn ErasedVtabInstance>> {
        self.create(cx, args)
    }

    fn column_info(&self, args: &[&str]) -> Vec<(String, char)> {
        let Ok(parsed) = parse_rtree_module_args(args) else {
            return Vec::new();
        };
        parsed
            .column_names
            .into_iter()
            .enumerate()
            .map(|(index, name)| (name, column_affinity(self.coord_type, index == 0)))
            .collect()
    }
}

/// Return a module factory for `CREATE VIRTUAL TABLE ... USING rtree(...)`.
#[must_use]
pub const fn rtree_module_factory() -> impl VtabModuleFactory {
    RtreeFactory::new(RtreeCoordType::Float32)
}

/// Return a module factory for `CREATE VIRTUAL TABLE ... USING rtree_i32(...)`.
#[must_use]
pub const fn rtree_i32_module_factory() -> impl VtabModuleFactory {
    RtreeFactory::new(RtreeCoordType::Int32)
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
// Geopoly scalar function wrappers
// ---------------------------------------------------------------------------

fn invalid_arity(function_name: &str, expected: &str, actual: usize) -> FrankenError {
    FrankenError::function_error(format!("{function_name} requires {expected}, got {actual}"))
}

const fn value_kind(value: &SqliteValue) -> &'static str {
    match value {
        SqliteValue::Null => "null",
        SqliteValue::Integer(_) => "integer",
        SqliteValue::Float(_) => "real",
        SqliteValue::Text(_) => "text",
        SqliteValue::Blob(_) => "blob",
    }
}

fn polygon_arg(
    function_name: &str,
    value: &SqliteValue,
    index: usize,
) -> Result<Option<Vec<Point>>> {
    match value {
        SqliteValue::Null => Ok(None),
        SqliteValue::Text(text) => geopoly_json_decode(text).map(Some).ok_or_else(|| {
            FrankenError::function_error(format!(
                "{function_name}: argument {} must be a valid geopoly JSON coordinate array",
                index + 1
            ))
        }),
        SqliteValue::Blob(blob) => geopoly_blob_decode(blob).map(Some).ok_or_else(|| {
            FrankenError::function_error(format!(
                "{function_name}: argument {} must be a valid geopoly blob",
                index + 1
            ))
        }),
        other => Err(FrankenError::function_error(format!(
            "{function_name}: argument {} must be text or blob, got {}",
            index + 1,
            value_kind(other)
        ))),
    }
}

fn unary_polygon(function_name: &str, args: &[SqliteValue]) -> Result<Option<Vec<Point>>> {
    if args.len() != 1 {
        return Err(invalid_arity(
            function_name,
            "exactly 1 argument",
            args.len(),
        ));
    }
    polygon_arg(function_name, &args[0], 0)
}

fn binary_polygons(
    function_name: &str,
    args: &[SqliteValue],
) -> Result<Option<(Vec<Point>, Vec<Point>)>> {
    if args.len() != 2 {
        return Err(invalid_arity(
            function_name,
            "exactly 2 arguments",
            args.len(),
        ));
    }
    let Some(lhs) = polygon_arg(function_name, &args[0], 0)? else {
        return Ok(None);
    };
    let Some(rhs) = polygon_arg(function_name, &args[1], 1)? else {
        return Ok(None);
    };
    Ok(Some((lhs, rhs)))
}

/// `geopoly_blob(X)`: encode a geopoly JSON/text/blob polygon into blob form.
pub struct GeopolyBlobFunc;

impl ScalarFunction for GeopolyBlobFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let Some(vertices) = unary_polygon(self.name(), args)? else {
            return Ok(SqliteValue::Null);
        };
        Ok(SqliteValue::Blob(geopoly_blob(&vertices)))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "geopoly_blob"
    }
}

/// `geopoly_json(X)`: normalize a geopoly polygon into JSON text form.
pub struct GeopolyJsonFunc;

impl ScalarFunction for GeopolyJsonFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let Some(vertices) = unary_polygon(self.name(), args)? else {
            return Ok(SqliteValue::Null);
        };
        Ok(SqliteValue::Text(geopoly_json(&vertices)))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "geopoly_json"
    }
}

/// `geopoly_svg(X)`: render a geopoly polygon as an SVG path.
pub struct GeopolySvgFunc;

impl ScalarFunction for GeopolySvgFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let Some(vertices) = unary_polygon(self.name(), args)? else {
            return Ok(SqliteValue::Null);
        };
        Ok(SqliteValue::Text(geopoly_svg(&vertices)))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "geopoly_svg"
    }
}

/// `geopoly_area(X)`: compute polygon area using the shoelace formula.
pub struct GeopolyAreaFunc;

impl ScalarFunction for GeopolyAreaFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let Some(vertices) = unary_polygon(self.name(), args)? else {
            return Ok(SqliteValue::Null);
        };
        Ok(SqliteValue::Float(geopoly_area(&vertices)))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "geopoly_area"
    }
}

/// `geopoly_overlap(X, Y)`: test whether two polygons overlap.
pub struct GeopolyOverlapFunc;

impl ScalarFunction for GeopolyOverlapFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let Some((lhs, rhs)) = binary_polygons(self.name(), args)? else {
            return Ok(SqliteValue::Null);
        };
        Ok(SqliteValue::Integer(if geopoly_overlap(&lhs, &rhs) {
            1
        } else {
            0
        }))
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &'static str {
        "geopoly_overlap"
    }
}

/// `geopoly_within(X, Y)`: test whether `X` is contained in `Y`.
pub struct GeopolyWithinFunc;

impl ScalarFunction for GeopolyWithinFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let Some((inner, outer)) = binary_polygons(self.name(), args)? else {
            return Ok(SqliteValue::Null);
        };
        Ok(SqliteValue::Integer(if geopoly_within(&inner, &outer) {
            1
        } else {
            0
        }))
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &'static str {
        "geopoly_within"
    }
}

/// Register the current Geopoly scalar-function surface into a registry.
pub fn register_geopoly_scalars(registry: &mut FunctionRegistry) {
    registry.register_scalar(GeopolyBlobFunc);
    registry.register_scalar(GeopolyJsonFunc);
    registry.register_scalar(GeopolySvgFunc);
    registry.register_scalar(GeopolyAreaFunc);
    registry.register_scalar(GeopolyOverlapFunc);
    registry.register_scalar(GeopolyWithinFunc);
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
    use fsqlite_error::FrankenError;
    use fsqlite_func::FunctionRegistry;
    use fsqlite_func::vtab::{
        ColumnContext, IndexConstraint, IndexInfo, VirtualTable, VirtualTableCursor,
        VtabModuleFactory,
    };
    use fsqlite_types::cx::Cx;

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

    fn json_value(vertices: &[Point]) -> SqliteValue {
        SqliteValue::Text(geopoly_json(vertices))
    }

    fn blob_value(vertices: &[Point]) -> SqliteValue {
        SqliteValue::Blob(geopoly_blob(vertices))
    }

    fn collect_rtree_rows(
        cursor: &mut RtreeCursor,
        cx: &Cx,
        column_count: usize,
    ) -> Vec<Vec<SqliteValue>> {
        let mut rows = Vec::new();
        while !cursor.eof() {
            let mut row = Vec::with_capacity(column_count);
            for column in 0..column_count {
                let mut ctx = ColumnContext::new();
                cursor
                    .column(&mut ctx, i32::try_from(column).unwrap())
                    .unwrap();
                row.push(ctx.take_value().unwrap_or(SqliteValue::Null));
            }
            rows.push(row);
            cursor.next(cx).unwrap();
        }
        rows
    }

    struct UpperRightGeometry;

    impl RtreeGeometry for UpperRightGeometry {
        fn query_func(&self, bbox: &[f64]) -> RtreeQueryResult {
            if bbox.len() >= 4 && bbox[0] >= 5.0 && bbox[2] >= 5.0 {
                RtreeQueryResult::Include
            } else {
                RtreeQueryResult::Exclude
            }
        }
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

    #[test]
    fn test_register_geopoly_scalars_registers_function_surface() {
        let mut registry = FunctionRegistry::new();
        register_geopoly_scalars(&mut registry);

        for (name, arity) in [
            ("geopoly_blob", 1),
            ("geopoly_json", 1),
            ("geopoly_svg", 1),
            ("geopoly_area", 1),
            ("geopoly_overlap", 2),
            ("geopoly_within", 2),
        ] {
            assert!(
                registry.find_scalar(name, arity).is_some(),
                "expected {name}/{arity} to be registered"
            );
        }
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

    #[test]
    fn test_geopoly_blob_func_encodes_json_polygon() {
        let polygon = square(0.0, 0.0, 2.0);
        let value = GeopolyBlobFunc
            .invoke(&[json_value(&polygon)])
            .expect("blob wrapper should succeed");
        let SqliteValue::Blob(blob) = value else {
            panic!("expected blob result");
        };
        let decoded = geopoly_blob_decode(&blob).expect("blob result should decode");
        assert_eq!(decoded, polygon);
    }

    #[test]
    fn test_geopoly_json_func_accepts_blob_polygon() {
        let polygon = square(1.0, 2.0, 3.0);
        let value = GeopolyJsonFunc
            .invoke(&[blob_value(&polygon)])
            .expect("json wrapper should succeed");
        assert_eq!(value, SqliteValue::Text(geopoly_json(&polygon)));
    }

    #[test]
    fn test_geopoly_area_func_returns_polygon_area() {
        let polygon = square(0.0, 0.0, 4.0);
        let value = GeopolyAreaFunc
            .invoke(&[json_value(&polygon)])
            .expect("area wrapper should succeed");
        assert_eq!(value, SqliteValue::Float(16.0));
    }

    #[test]
    fn test_geopoly_overlap_func_accepts_mixed_blob_and_json() {
        let lhs = square(0.0, 0.0, 4.0);
        let rhs = square(2.0, 2.0, 4.0);
        let disjoint = square(10.0, 10.0, 1.0);

        assert_eq!(
            GeopolyOverlapFunc
                .invoke(&[json_value(&lhs), blob_value(&rhs)])
                .expect("overlap wrapper should succeed"),
            SqliteValue::Integer(1)
        );
        assert_eq!(
            GeopolyOverlapFunc
                .invoke(&[blob_value(&lhs), json_value(&disjoint)])
                .expect("disjoint overlap wrapper should succeed"),
            SqliteValue::Integer(0)
        );
    }

    #[test]
    fn test_geopoly_within_func_returns_integer_truth_value() {
        let outer = square(0.0, 0.0, 10.0);
        let inner = square(2.0, 2.0, 3.0);
        let outside = square(-1.0, -1.0, 3.0);

        assert_eq!(
            GeopolyWithinFunc
                .invoke(&[json_value(&inner), blob_value(&outer)])
                .expect("within wrapper should succeed"),
            SqliteValue::Integer(1)
        );
        assert_eq!(
            GeopolyWithinFunc
                .invoke(&[blob_value(&outside), json_value(&outer)])
                .expect("outside wrapper should succeed"),
            SqliteValue::Integer(0)
        );
    }

    #[test]
    fn test_geopoly_scalar_wrappers_propagate_null() {
        assert!(
            GeopolyAreaFunc
                .invoke(&[SqliteValue::Null])
                .expect("null should propagate")
                .is_null()
        );
        assert!(
            GeopolyOverlapFunc
                .invoke(&[SqliteValue::Null, json_value(&square(0.0, 0.0, 1.0))])
                .expect("null lhs should propagate")
                .is_null()
        );
    }

    #[test]
    fn test_geopoly_scalar_wrappers_reject_malformed_text() {
        let error = GeopolyJsonFunc
            .invoke(&[SqliteValue::Text("not json".to_owned())])
            .expect_err("malformed polygon text should fail");
        assert!(matches!(error, FrankenError::FunctionError(_)));
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
        let err = match factory.create(&cx, &["id", "min_x"]) {
            Ok(_) => panic!("incomplete rtree args should be rejected"),
            Err(err) => err,
        };
        assert!(matches!(err, FrankenError::FunctionError(_)));
        assert!(err.to_string().contains("min/max coordinate pair"));
    }

    #[test]
    fn test_rtree_virtual_table_insert_and_full_scan() {
        let cx = Cx::new();
        let mut table = RtreeVirtualTable::from_args(
            &["id", "min_x", "max_x", "min_y", "max_y"],
            RtreeCoordType::Float32,
        )
        .unwrap();

        assert_eq!(
            VirtualTable::update(
                &mut table,
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(1),
                    SqliteValue::Integer(1),
                    SqliteValue::Float(0.0),
                    SqliteValue::Float(1.0),
                    SqliteValue::Float(0.0),
                    SqliteValue::Float(1.0),
                ],
            )
            .unwrap(),
            Some(1)
        );
        assert_eq!(
            VirtualTable::update(
                &mut table,
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(2),
                    SqliteValue::Integer(2),
                    SqliteValue::Float(2.0),
                    SqliteValue::Float(3.0),
                    SqliteValue::Float(2.0),
                    SqliteValue::Float(3.0),
                ],
            )
            .unwrap(),
            Some(2)
        );

        let mut cursor = table.open().unwrap();
        cursor.filter(&cx, RTREE_SCAN_FULL, None, &[]).unwrap();
        let rows = collect_rtree_rows(&mut cursor, &cx, 5);
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0],
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Float(0.0),
                SqliteValue::Float(1.0),
                SqliteValue::Float(0.0),
                SqliteValue::Float(1.0),
            ]
        );
        assert_eq!(rows[1][0], SqliteValue::Integer(2));
    }

    #[test]
    fn test_rtree_virtual_table_bbox_filter() {
        let cx = Cx::new();
        let mut table = RtreeVirtualTable::from_args(
            &["id", "min_x", "max_x", "min_y", "max_y"],
            RtreeCoordType::Float32,
        )
        .unwrap();

        VirtualTable::update(
            &mut table,
            &cx,
            &[
                SqliteValue::Null,
                SqliteValue::Integer(10),
                SqliteValue::Integer(10),
                SqliteValue::Float(0.0),
                SqliteValue::Float(1.0),
                SqliteValue::Float(0.0),
                SqliteValue::Float(1.0),
            ],
        )
        .unwrap();
        VirtualTable::update(
            &mut table,
            &cx,
            &[
                SqliteValue::Null,
                SqliteValue::Integer(20),
                SqliteValue::Integer(20),
                SqliteValue::Float(4.0),
                SqliteValue::Float(5.0),
                SqliteValue::Float(4.0),
                SqliteValue::Float(5.0),
            ],
        )
        .unwrap();

        let mut info = IndexInfo::new(
            vec![
                IndexConstraint {
                    column: 1,
                    op: ConstraintOp::Le,
                    usable: true,
                },
                IndexConstraint {
                    column: 2,
                    op: ConstraintOp::Ge,
                    usable: true,
                },
                IndexConstraint {
                    column: 3,
                    op: ConstraintOp::Le,
                    usable: true,
                },
                IndexConstraint {
                    column: 4,
                    op: ConstraintOp::Ge,
                    usable: true,
                },
            ],
            Vec::new(),
        );
        VirtualTable::best_index(&table, &mut info).unwrap();
        assert_eq!(info.idx_num, RTREE_SCAN_BBOX);

        let mut cursor = table.open().unwrap();
        cursor
            .filter(
                &cx,
                info.idx_num,
                None,
                &[
                    SqliteValue::Float(3.5),
                    SqliteValue::Float(4.5),
                    SqliteValue::Float(3.5),
                    SqliteValue::Float(4.5),
                ],
            )
            .unwrap();
        let rows = collect_rtree_rows(&mut cursor, &cx, 5);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], SqliteValue::Integer(20));
    }

    #[test]
    fn test_rtree_best_index_does_not_omit_noninclusive_bbox_constraints() {
        let table = RtreeVirtualTable::from_args(
            &["id", "min_x", "max_x", "min_y", "max_y"],
            RtreeCoordType::Float32,
        )
        .unwrap();
        let cases = [
            vec![
                IndexConstraint {
                    column: 1,
                    op: ConstraintOp::Lt,
                    usable: true,
                },
                IndexConstraint {
                    column: 2,
                    op: ConstraintOp::Gt,
                    usable: true,
                },
                IndexConstraint {
                    column: 3,
                    op: ConstraintOp::Lt,
                    usable: true,
                },
                IndexConstraint {
                    column: 4,
                    op: ConstraintOp::Gt,
                    usable: true,
                },
            ],
            vec![
                IndexConstraint {
                    column: 1,
                    op: ConstraintOp::Eq,
                    usable: true,
                },
                IndexConstraint {
                    column: 2,
                    op: ConstraintOp::Eq,
                    usable: true,
                },
                IndexConstraint {
                    column: 3,
                    op: ConstraintOp::Eq,
                    usable: true,
                },
                IndexConstraint {
                    column: 4,
                    op: ConstraintOp::Eq,
                    usable: true,
                },
            ],
        ];

        for constraints in cases {
            let mut info = IndexInfo::new(constraints, Vec::new());
            VirtualTable::best_index(&table, &mut info).unwrap();
            assert_eq!(info.idx_num, RTREE_SCAN_FULL);
            assert!(
                info.constraint_usage
                    .iter()
                    .all(|usage| usage.argv_index == 0 && !usage.omit)
            );
        }
    }

    #[test]
    fn test_rtree_virtual_table_update_returns_none() {
        let cx = Cx::new();
        let mut table = RtreeVirtualTable::from_args(
            &["id", "min_x", "max_x", "min_y", "max_y"],
            RtreeCoordType::Float32,
        )
        .unwrap();

        VirtualTable::update(
            &mut table,
            &cx,
            &[
                SqliteValue::Null,
                SqliteValue::Integer(1),
                SqliteValue::Integer(1),
                SqliteValue::Float(0.0),
                SqliteValue::Float(1.0),
                SqliteValue::Float(0.0),
                SqliteValue::Float(1.0),
            ],
        )
        .unwrap();

        let result = VirtualTable::update(
            &mut table,
            &cx,
            &[
                SqliteValue::Integer(1),
                SqliteValue::Integer(1),
                SqliteValue::Integer(1),
                SqliteValue::Float(4.0),
                SqliteValue::Float(5.0),
                SqliteValue::Float(4.0),
                SqliteValue::Float(5.0),
            ],
        )
        .unwrap();
        assert_eq!(result, None);

        let mut cursor = table.open().unwrap();
        cursor.filter(&cx, RTREE_SCAN_FULL, None, &[]).unwrap();
        let rows = collect_rtree_rows(&mut cursor, &cx, 5);
        assert_eq!(
            rows,
            vec![vec![
                SqliteValue::Integer(1),
                SqliteValue::Float(4.0),
                SqliteValue::Float(5.0),
                SqliteValue::Float(4.0),
                SqliteValue::Float(5.0),
            ]]
        );
    }

    #[test]
    fn test_rtree_virtual_table_rowid_conflict_preserves_original_entry() {
        let cx = Cx::new();
        let mut table = RtreeVirtualTable::from_args(
            &["id", "min_x", "max_x", "min_y", "max_y"],
            RtreeCoordType::Float32,
        )
        .unwrap();

        for (rowid, coords) in [(1_i64, [0.0, 1.0, 0.0, 1.0]), (2_i64, [2.0, 3.0, 2.0, 3.0])] {
            VirtualTable::update(
                &mut table,
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(rowid),
                    SqliteValue::Integer(rowid),
                    SqliteValue::Float(coords[0]),
                    SqliteValue::Float(coords[1]),
                    SqliteValue::Float(coords[2]),
                    SqliteValue::Float(coords[3]),
                ],
            )
            .unwrap();
        }

        let err = VirtualTable::update(
            &mut table,
            &cx,
            &[
                SqliteValue::Integer(1),
                SqliteValue::Integer(2),
                SqliteValue::Integer(2),
                SqliteValue::Float(8.0),
                SqliteValue::Float(9.0),
                SqliteValue::Float(8.0),
                SqliteValue::Float(9.0),
            ],
        )
        .unwrap_err();
        assert!(matches!(err, FrankenError::PrimaryKeyViolation));

        let mut cursor = table.open().unwrap();
        cursor.filter(&cx, RTREE_SCAN_FULL, None, &[]).unwrap();
        let rows = collect_rtree_rows(&mut cursor, &cx, 5);
        assert_eq!(
            rows,
            vec![
                vec![
                    SqliteValue::Integer(1),
                    SqliteValue::Float(0.0),
                    SqliteValue::Float(1.0),
                    SqliteValue::Float(0.0),
                    SqliteValue::Float(1.0),
                ],
                vec![
                    SqliteValue::Integer(2),
                    SqliteValue::Float(2.0),
                    SqliteValue::Float(3.0),
                    SqliteValue::Float(2.0),
                    SqliteValue::Float(3.0),
                ],
            ]
        );
    }

    #[test]
    fn test_rtree_virtual_table_geometry_filter() {
        let cx = Cx::new();
        let mut table = RtreeVirtualTable::from_args(
            &["id", "min_x", "max_x", "min_y", "max_y"],
            RtreeCoordType::Float32,
        )
        .unwrap();
        table.register_geometry("upper_right", Box::new(UpperRightGeometry));

        VirtualTable::update(
            &mut table,
            &cx,
            &[
                SqliteValue::Null,
                SqliteValue::Integer(1),
                SqliteValue::Integer(1),
                SqliteValue::Float(0.0),
                SqliteValue::Float(1.0),
                SqliteValue::Float(0.0),
                SqliteValue::Float(1.0),
            ],
        )
        .unwrap();
        VirtualTable::update(
            &mut table,
            &cx,
            &[
                SqliteValue::Null,
                SqliteValue::Integer(2),
                SqliteValue::Integer(2),
                SqliteValue::Float(6.0),
                SqliteValue::Float(7.0),
                SqliteValue::Float(6.0),
                SqliteValue::Float(7.0),
            ],
        )
        .unwrap();

        let mut cursor = table.open().unwrap();
        cursor
            .filter(
                &cx,
                RTREE_SCAN_GEOMETRY,
                None,
                &[SqliteValue::Text("upper_right".to_owned())],
            )
            .unwrap();
        let rows = collect_rtree_rows(&mut cursor, &cx, 5);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], SqliteValue::Integer(2));
    }

    #[test]
    fn test_rtree_i32_cursor_emits_integer_coordinates() {
        let cx = Cx::new();
        let mut table = RtreeVirtualTable::from_args(
            &["id", "min_x", "max_x", "min_y", "max_y"],
            RtreeCoordType::Int32,
        )
        .unwrap();
        VirtualTable::update(
            &mut table,
            &cx,
            &[
                SqliteValue::Null,
                SqliteValue::Integer(7),
                SqliteValue::Integer(7),
                SqliteValue::Integer(1),
                SqliteValue::Integer(3),
                SqliteValue::Integer(5),
                SqliteValue::Integer(9),
            ],
        )
        .unwrap();

        let mut cursor = table.open().unwrap();
        cursor.filter(&cx, RTREE_SCAN_FULL, None, &[]).unwrap();
        let rows = collect_rtree_rows(&mut cursor, &cx, 5);
        assert_eq!(
            rows[0],
            vec![
                SqliteValue::Integer(7),
                SqliteValue::Integer(1),
                SqliteValue::Integer(3),
                SqliteValue::Integer(5),
                SqliteValue::Integer(9),
            ]
        );
    }

    #[test]
    fn test_rtree_factory_column_info_matches_coord_type() {
        let float_columns =
            rtree_module_factory().column_info(&["id", "min_x", "max_x", "min_y", "max_y"]);
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

        let int_columns =
            rtree_i32_module_factory().column_info(&["id", "min_x", "max_x", "min_y", "max_y"]);
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
