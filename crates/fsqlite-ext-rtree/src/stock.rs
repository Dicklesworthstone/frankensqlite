//! Stock SQLite R-tree storage, ported from `rtree.c` (SQLite 3.53).
//!
//! A tree lives in three ordinary tables, exactly as stock SQLite lays it out:
//!
//! ```text
//! CREATE TABLE "<t>_rowid"(rowid INTEGER PRIMARY KEY,nodeno[,a0,a1,...])
//! CREATE TABLE "<t>_node"(nodeno INTEGER PRIMARY KEY,data)
//! CREATE TABLE "<t>_parent"(nodeno INTEGER PRIMARY KEY,parentnode)
//! ```
//!
//! Every `_node` blob is `node_size` bytes: a big-endian 16-bit tree depth
//! (root node 1 only), a 16-bit cell count, then cells of an 8-byte rowid or
//! child node number followed by `2 * dimensions` 4-byte coordinates (`f32`
//! or `i32` bits, big-endian).
//!
//! [`StockTree`] keeps all three tables in memory and runs `rtree.c`'s
//! ChooseLeaf, AdjustTree, R*-tree split, condense-on-delete and
//! reinsertion algorithms on the node images byte for byte, including the
//! floating-point evaluation order of the area, margin and overlap scores
//! and the stale bytes `nodeDeleteCell` leaves behind. The same sequence of
//! writes therefore produces the same shadow rows as stock SQLite. Every
//! shadow row a write touches is recorded in a [`ShadowChanges`] set for the
//! engine to persist.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::SqliteValue;

use crate::{RtreeGeometry, RtreeQueryResult};

/// `RTREE_MAXCELLS`: a created tree's nodes hold at most this many cells.
pub const RTREE_MAXCELLS: usize = 51;
/// `RTREE_MAX_DEPTH`: deeper trees are corrupt.
const RTREE_MAX_DEPTH: usize = 40;
/// Smallest node size stock accepts when connecting (`512 - 64`).
pub const RTREE_MIN_NODE_SIZE: usize = 512 - 64;
/// `RTREE_DEFAULT_ROWEST`: the row estimate without `sqlite_stat1` data.
pub const RTREE_DEFAULT_ROWEST: i64 = 1_048_576;

/// `rtreeValueDown` / `rtreeValueUp` rounding factors.
const RNDTOWARDS: f64 = 1.0 - 1.0 / 8_388_608.0;
const RNDAWAY: f64 = 1.0 + 1.0 / 8_388_608.0;

/// Converts a coordinate to `f32`, rounding toward negative infinity
/// (`rtreeValueDown`), so a stored lower bound never exceeds the value.
#[must_use]
pub fn rtree_value_down(d: f64) -> f32 {
    let mut f = d as f32;
    if f64::from(f) > d {
        f = (d * if d < 0.0 { RNDAWAY } else { RNDTOWARDS }) as f32;
    }
    f
}

/// Converts a coordinate to `f32`, rounding toward positive infinity
/// (`rtreeValueUp`), so a stored upper bound is never below the value.
#[must_use]
pub fn rtree_value_up(d: f64) -> f32 {
    let mut f = d as f32;
    if f64::from(f) < d {
        f = (d * if d < 0.0 { RNDTOWARDS } else { RNDAWAY }) as f32;
    }
    f
}

/// The fixed shape of one tree's node images.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeShape {
    /// Number of dimensions (1-5).
    pub dimensions: usize,
    /// `rtree_i32` (integer coordinates) instead of `rtree` (`f32`).
    pub int32: bool,
    /// Size of every node blob in bytes.
    pub node_size: usize,
}

impl NodeShape {
    /// The node size `xCreate` picks for a database page size (`getNodeSize`):
    /// 64 bytes less than a page, capped at [`RTREE_MAXCELLS`] cells.
    #[must_use]
    pub fn node_size_for_page(page_size: usize, dimensions: usize) -> usize {
        let per_cell = 8 + dimensions * 8;
        page_size
            .saturating_sub(64)
            .min(4 + per_cell * RTREE_MAXCELLS)
    }

    const fn coords(self) -> usize {
        self.dimensions * 2
    }

    const fn bytes_per_cell(self) -> usize {
        8 + self.dimensions * 8
    }

    const fn max_cells(self) -> usize {
        (self.node_size - 4) / self.bytes_per_cell()
    }

    /// `RTREE_MINCELLS`: a non-root node with fewer cells is condensed away.
    const fn min_cells(self) -> usize {
        self.max_cells() / 3
    }

    const fn cell_offset(self, index: usize) -> usize {
        4 + self.bytes_per_cell() * index
    }

    /// A coordinate's raw bits as `rtree.c`'s `DCOORD` double.
    fn dcoord(self, bits: u32) -> f64 {
        if self.int32 {
            f64::from(bits as i32)
        } else {
            f64::from(f32::from_bits(bits))
        }
    }
}

/// One cell: a rowid (leaf) or child node number (interior) and its box as
/// raw coordinate bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cell {
    pub rowid: i64,
    pub coords: [u32; 10],
}

impl Cell {
    #[must_use]
    pub const fn new(rowid: i64) -> Self {
        Self {
            rowid,
            coords: [0; 10],
        }
    }
}

/// Matches `rtree.c`'s `MIN` macro: `b` unless `a < b`.
fn min_f32(a: f32, b: f32) -> f32 {
    if a < b { a } else { b }
}

/// Matches `rtree.c`'s `MAX` macro: `b` unless `a > b`.
fn max_f32(a: f32, b: f32) -> f32 {
    if a > b { a } else { b }
}

fn min_f64(a: f64, b: f64) -> f64 {
    if a < b { a } else { b }
}

fn max_f64(a: f64, b: f64) -> f64 {
    if a > b { a } else { b }
}

/// `cellArea`: the product of the extents, highest dimension first. For
/// `f32` trees each extent is an `f32` difference widened to `f64`.
fn cell_area(shape: NodeShape, cell: &Cell) -> f64 {
    let mut area = 1.0_f64;
    for dim in (0..shape.dimensions).rev() {
        let lo = cell.coords[dim * 2];
        let hi = cell.coords[dim * 2 + 1];
        let extent = if shape.int32 {
            (i64::from(hi as i32) - i64::from(lo as i32)) as f64
        } else {
            f64::from(f32::from_bits(hi) - f32::from_bits(lo))
        };
        area *= extent;
    }
    area
}

/// `cellMargin`: the sum of the extents in double precision, highest
/// dimension first.
fn cell_margin(shape: NodeShape, cell: &Cell) -> f64 {
    let mut margin = 0.0_f64;
    for dim in (0..shape.dimensions).rev() {
        margin += shape.dcoord(cell.coords[dim * 2 + 1]) - shape.dcoord(cell.coords[dim * 2]);
    }
    margin
}

/// `cellUnion`: grows `target` to cover `other`.
fn cell_union(shape: NodeShape, target: &mut Cell, other: &Cell) {
    for dim in 0..shape.dimensions {
        let (lo, hi) = (dim * 2, dim * 2 + 1);
        if shape.int32 {
            target.coords[lo] = (target.coords[lo] as i32).min(other.coords[lo] as i32) as u32;
            target.coords[hi] = (target.coords[hi] as i32).max(other.coords[hi] as i32) as u32;
        } else {
            target.coords[lo] = min_f32(
                f32::from_bits(target.coords[lo]),
                f32::from_bits(other.coords[lo]),
            )
            .to_bits();
            target.coords[hi] = max_f32(
                f32::from_bits(target.coords[hi]),
                f32::from_bits(other.coords[hi]),
            )
            .to_bits();
        }
    }
}

/// `cellContains`: whether `outer` covers `inner`.
fn cell_contains(shape: NodeShape, outer: &Cell, inner: &Cell) -> bool {
    (0..shape.dimensions).all(|dim| {
        let (lo, hi) = (dim * 2, dim * 2 + 1);
        if shape.int32 {
            !((inner.coords[lo] as i32) < (outer.coords[lo] as i32)
                || (inner.coords[hi] as i32) > (outer.coords[hi] as i32))
        } else {
            !(f32::from_bits(inner.coords[lo]) < f32::from_bits(outer.coords[lo])
                || f32::from_bits(inner.coords[hi]) > f32::from_bits(outer.coords[hi]))
        }
    })
}

/// `cellOverlap` against a single cell: the volume both boxes share.
fn cell_overlap(shape: NodeShape, a: &Cell, b: &Cell) -> f64 {
    let mut overlap = 1.0_f64;
    for dim in 0..shape.dimensions {
        let (lo, hi) = (dim * 2, dim * 2 + 1);
        let x1 = max_f64(shape.dcoord(a.coords[lo]), shape.dcoord(b.coords[lo]));
        let x2 = min_f64(shape.dcoord(a.coords[hi]), shape.dcoord(b.coords[hi]));
        if x2 < x1 {
            return 0.0;
        }
        overlap *= x2 - x1;
    }
    overlap
}

fn read_u16(data: &[u8], at: usize) -> usize {
    usize::from(u16::from_be_bytes([data[at], data[at + 1]]))
}

fn write_u16(data: &mut [u8], at: usize, value: usize) {
    let bytes = (value as u16).to_be_bytes();
    data[at] = bytes[0];
    data[at + 1] = bytes[1];
}

fn node_cell_count(data: &[u8]) -> usize {
    read_u16(data, 2)
}

fn node_rowid(shape: NodeShape, data: &[u8], index: usize) -> i64 {
    let at = shape.cell_offset(index);
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&data[at..at + 8]);
    i64::from_be_bytes(bytes)
}

fn node_coord(shape: NodeShape, data: &[u8], index: usize, coord: usize) -> u32 {
    let at = shape.cell_offset(index) + 8 + coord * 4;
    u32::from_be_bytes([data[at], data[at + 1], data[at + 2], data[at + 3]])
}

/// `nodeGetCell`.
fn node_cell(shape: NodeShape, data: &[u8], index: usize) -> Cell {
    let mut cell = Cell::new(node_rowid(shape, data, index));
    for (coord, value) in cell.coords.iter_mut().enumerate().take(shape.coords()) {
        *value = node_coord(shape, data, index, coord);
    }
    cell
}

/// `nodeOverwriteCell`.
fn node_overwrite_cell(shape: NodeShape, data: &mut [u8], cell: &Cell, index: usize) {
    let mut at = shape.cell_offset(index);
    data[at..at + 8].copy_from_slice(&cell.rowid.to_be_bytes());
    at += 8;
    for coord in &cell.coords[..shape.coords()] {
        data[at..at + 4].copy_from_slice(&coord.to_be_bytes());
        at += 4;
    }
}

/// `nodeDeleteCell`: shifts the later cells down without clearing the bytes
/// the last cell leaves behind, as stock does.
fn node_delete_cell(shape: NodeShape, data: &mut [u8], index: usize) {
    let count = node_cell_count(data);
    let start = shape.cell_offset(index);
    let end = shape.cell_offset(count);
    data.copy_within(start + shape.bytes_per_cell()..end, start);
    write_u16(data, 2, count - 1);
}

/// `nodeInsertCell`: appends `cell` unless the node is full, and reports
/// whether it was full (the caller then splits).
fn node_insert_cell(shape: NodeShape, data: &mut [u8], cell: &Cell) -> bool {
    let count = node_cell_count(data);
    let full = count >= shape.max_cells();
    if !full {
        node_overwrite_cell(shape, data, cell, count);
        write_u16(data, 2, count + 1);
    }
    full
}

/// `SortByDimension`: rtree.c's top-down merge sort of cell indices by the
/// lower bound in `dim`, then the upper bound. On a full tie the right half
/// wins, so the order is not stable; it is reproduced exactly because the
/// split depends on it.
fn sort_by_dimension(shape: NodeShape, indices: &mut [usize], dim: usize, cells: &[Cell]) {
    let count = indices.len();
    if count <= 1 {
        return;
    }
    let n_left = count / 2;
    sort_by_dimension(shape, &mut indices[..n_left], dim, cells);
    sort_by_dimension(shape, &mut indices[n_left..], dim, cells);
    let left: Vec<usize> = indices[..n_left].to_vec();
    let right: Vec<usize> = indices[n_left..].to_vec();
    let (mut i_left, mut i_right) = (0, 0);
    while i_left < left.len() || i_right < right.len() {
        let take_left = i_left != left.len()
            && (i_right == right.len() || {
                let l = &cells[left[i_left]];
                let r = &cells[right[i_right]];
                let (l1, l2) = (
                    shape.dcoord(l.coords[dim * 2]),
                    shape.dcoord(l.coords[dim * 2 + 1]),
                );
                let (r1, r2) = (
                    shape.dcoord(r.coords[dim * 2]),
                    shape.dcoord(r.coords[dim * 2 + 1]),
                );
                l1 < r1 || (l1 == r1 && l2 < r2)
            });
        if take_left {
            indices[i_left + i_right] = left[i_left];
            i_left += 1;
        } else {
            indices[i_left + i_right] = right[i_right];
            i_right += 1;
        }
    }
}

/// A `_rowid` table row: the leaf holding the entry and its auxiliary
/// column values (`a0`, `a1`, ...).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowidRow {
    pub nodeno: Option<i64>,
    pub aux: Vec<SqliteValue>,
}

/// Keys of the shadow rows written since the engine last persisted them.
/// A key whose row no longer exists is a delete.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShadowChanges {
    pub nodes: BTreeSet<i64>,
    pub rowids: BTreeSet<i64>,
    pub parents: BTreeSet<i64>,
}

/// How one search constraint tests a coordinate (`RTREE_EQ` ... `RTREE_FALSE`).
#[derive(Clone)]
pub enum ConstraintTest {
    Eq,
    Le,
    Lt,
    Ge,
    Gt,
    /// A text or blob bound that every row satisfies (`< 'x'`, `<= 'x'`).
    True,
    /// A NULL bound, or a text or blob bound no row satisfies.
    False,
    /// A FrankenSQLite geometry callback (`MATCH`).
    Geometry(Arc<dyn RtreeGeometry>),
}

impl std::fmt::Debug for ConstraintTest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eq => f.write_str("Eq"),
            Self::Le => f.write_str("Le"),
            Self::Lt => f.write_str("Lt"),
            Self::Ge => f.write_str("Ge"),
            Self::Gt => f.write_str("Gt"),
            Self::True => f.write_str("True"),
            Self::False => f.write_str("False"),
            Self::Geometry(_) => f.write_str("Geometry"),
        }
    }
}

/// One `xFilter` constraint: `coordinate <test> value`.
#[derive(Debug, Clone)]
pub struct SearchConstraint {
    pub test: ConstraintTest,
    pub coord: usize,
    pub value: f64,
}

/// A row a search found: the entry's rowid and where its cell lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchHit {
    pub rowid: i64,
    pub node: i64,
    pub cell: usize,
}

fn corrupt(detail: impl Into<String>) -> FrankenError {
    FrankenError::DatabaseCorrupt {
        detail: detail.into(),
    }
}

/// An R-tree held as its three stock shadow tables.
#[derive(Debug, Clone)]
pub struct StockTree {
    shape: NodeShape,
    aux_count: usize,
    nodes: BTreeMap<i64, Vec<u8>>,
    rowids: BTreeMap<i64, RowidRow>,
    parents: BTreeMap<i64, i64>,
    changes: ShadowChanges,
    /// Nodes removed during the current delete, with the height of the
    /// subtree each headed, awaiting reinsertion (`Rtree.pDeleted`, LIFO).
    deleted: Vec<(Vec<u8>, usize)>,
}

impl StockTree {
    /// A new tree: an empty root node 1, which a create writes to `_node`.
    #[must_use]
    pub fn new_empty(shape: NodeShape, aux_count: usize) -> Self {
        let mut tree = Self {
            shape,
            aux_count,
            nodes: BTreeMap::new(),
            rowids: BTreeMap::new(),
            parents: BTreeMap::new(),
            changes: ShadowChanges::default(),
            deleted: Vec::new(),
        };
        tree.nodes.insert(1, vec![0; shape.node_size]);
        tree.changes.nodes.insert(1);
        tree
    }

    /// Loads a tree from its shadow rows. The node size is the length of
    /// node 1, as stock's `xConnect` reads it.
    pub fn from_shadow_rows(
        table_name: &str,
        dimensions: usize,
        int32: bool,
        aux_count: usize,
        node_rows: Vec<(i64, Vec<u8>)>,
        rowid_rows: Vec<(i64, RowidRow)>,
        parent_rows: Vec<(i64, i64)>,
    ) -> Result<Self> {
        let nodes: BTreeMap<i64, Vec<u8>> = node_rows.into_iter().collect();
        let node_size = nodes.get(&1).map_or(0, Vec::len);
        if node_size < RTREE_MIN_NODE_SIZE {
            return Err(corrupt(format!(
                "undersize RTree blobs in \"{table_name}_node\""
            )));
        }
        let shape = NodeShape {
            dimensions,
            int32,
            node_size,
        };
        for (nodeno, data) in &nodes {
            if data.len() != node_size {
                return Err(corrupt(format!(
                    "rtree node {nodeno} of {table_name} is {} bytes, not {node_size}",
                    data.len()
                )));
            }
            if node_cell_count(data) > shape.max_cells() {
                return Err(corrupt(format!(
                    "rtree node {nodeno} of {table_name} claims {} cells",
                    node_cell_count(data)
                )));
            }
        }
        let tree = Self {
            shape,
            aux_count,
            nodes,
            rowids: rowid_rows.into_iter().collect(),
            parents: parent_rows.into_iter().collect(),
            changes: ShadowChanges::default(),
            deleted: Vec::new(),
        };
        if tree.depth() > RTREE_MAX_DEPTH {
            return Err(corrupt(format!(
                "rtree {table_name} has depth {}",
                tree.depth()
            )));
        }
        Ok(tree)
    }

    #[must_use]
    pub const fn shape(&self) -> NodeShape {
        self.shape
    }

    /// Number of entries (rows of `_rowid`).
    #[must_use]
    pub fn len(&self) -> usize {
        self.rowids.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rowids.is_empty()
    }

    #[must_use]
    pub fn contains_rowid(&self, rowid: i64) -> bool {
        self.rowids.contains_key(&rowid)
    }

    /// The `_node` row for `nodeno`, if it exists.
    #[must_use]
    pub fn node_blob(&self, nodeno: i64) -> Option<&[u8]> {
        self.nodes.get(&nodeno).map(Vec::as_slice)
    }

    /// The `_rowid` row for `rowid`, if it exists.
    #[must_use]
    pub fn rowid_row(&self, rowid: i64) -> Option<&RowidRow> {
        self.rowids.get(&rowid)
    }

    /// The `_parent` row for `nodeno`, if it exists.
    #[must_use]
    pub fn parent_row(&self, nodeno: i64) -> Option<i64> {
        self.parents.get(&nodeno).copied()
    }

    /// Takes the set of shadow rows changed since the last call.
    pub fn take_changes(&mut self) -> ShadowChanges {
        std::mem::take(&mut self.changes)
    }

    /// The tree depth stored in the first two bytes of node 1.
    #[must_use]
    pub fn depth(&self) -> usize {
        self.nodes.get(&1).map_or(0, |root| read_u16(root, 0))
    }

    fn set_depth(&mut self, depth: usize) -> Result<()> {
        let root = self.node_mut(1)?;
        write_u16(root, 0, depth);
        Ok(())
    }

    fn node(&self, nodeno: i64) -> Result<&[u8]> {
        self.nodes
            .get(&nodeno)
            .map(Vec::as_slice)
            .ok_or_else(|| corrupt(format!("rtree node {nodeno} is missing")))
    }

    fn node_mut(&mut self, nodeno: i64) -> Result<&mut [u8]> {
        self.changes.nodes.insert(nodeno);
        self.nodes
            .get_mut(&nodeno)
            .map(Vec::as_mut_slice)
            .ok_or_else(|| corrupt(format!("rtree node {nodeno} is missing")))
    }

    fn cell_count(&self, nodeno: i64) -> Result<usize> {
        Ok(node_cell_count(self.node(nodeno)?))
    }

    fn cell(&self, nodeno: i64, index: usize) -> Result<Cell> {
        Ok(node_cell(self.shape, self.node(nodeno)?, index))
    }

    fn parent_of(&self, nodeno: i64) -> Option<i64> {
        if nodeno == 1 {
            None
        } else {
            self.parents.get(&nodeno).copied()
        }
    }

    /// `nodeRowidIndex`: the index of the cell holding `rowid` in `nodeno`.
    fn rowid_index(&self, nodeno: i64, rowid: i64) -> Result<usize> {
        let data = self.node(nodeno)?;
        (0..node_cell_count(data))
            .find(|&index| node_rowid(self.shape, data, index) == rowid)
            .ok_or_else(|| corrupt(format!("rtree node {nodeno} has no cell for {rowid}")))
    }

    /// `nodeParentIndex`: `nodeno`'s cell index in its parent.
    fn parent_index(&self, nodeno: i64) -> Result<(i64, usize)> {
        let parent = self
            .parent_of(nodeno)
            .ok_or_else(|| corrupt(format!("rtree node {nodeno} has no parent")))?;
        Ok((parent, self.rowid_index(parent, nodeno)?))
    }

    fn set_parent(&mut self, nodeno: i64, parent: i64) {
        self.parents.insert(nodeno, parent);
        self.changes.parents.insert(nodeno);
    }

    fn set_rowid_node(&mut self, rowid: i64, nodeno: i64) {
        let aux_count = self.aux_count;
        self.rowids
            .entry(rowid)
            .or_insert_with(|| RowidRow {
                nodeno: None,
                aux: vec![SqliteValue::Null; aux_count],
            })
            .nodeno = Some(nodeno);
        self.changes.rowids.insert(rowid);
    }

    /// `nodeWrite` of a new node: `INSERT INTO _node VALUES(NULL, ...)` takes
    /// the largest node number plus one.
    fn allocate_node(&mut self, data: Vec<u8>) -> i64 {
        let nodeno = self.nodes.keys().next_back().map_or(1, |last| last + 1);
        self.nodes.insert(nodeno, data);
        self.changes.nodes.insert(nodeno);
        nodeno
    }

    /// `rtreeNewRowid`: the rowid `INSERT INTO _rowid VALUES(NULL, ...)`
    /// assigns, the largest rowid plus one. Stock picks a random unused rowid
    /// once the largest is `i64::MAX`; this picks the smallest unused one.
    #[must_use]
    pub fn new_rowid(&self) -> i64 {
        match self.rowids.keys().next_back() {
            None => 1,
            Some(&last) if last < i64::MAX => last + 1,
            Some(_) => (1..=i64::MAX)
                .find(|rowid| !self.rowids.contains_key(rowid))
                .unwrap_or(1),
        }
    }

    /// `ChooseLeaf`: the node at `height` above the leaves whose box grows
    /// least to take `cell`.
    fn choose_leaf(&self, cell: &Cell, height: usize) -> Result<i64> {
        let shape = self.shape;
        let mut nodeno = 1;
        for _ in 0..self.depth().saturating_sub(height) {
            let data = self.node(nodeno)?;
            let count = node_cell_count(data);
            let mut best = 0;
            let mut found = false;
            let mut min_area = 0.0;
            for index in 0..count {
                let candidate = node_cell(shape, data, index);
                if cell_contains(shape, &candidate, cell) {
                    let area = cell_area(shape, &candidate);
                    if !found || area < min_area {
                        best = candidate.rowid;
                        min_area = area;
                        found = true;
                    }
                }
            }
            if !found {
                let mut min_growth = 0.0;
                for index in 0..count {
                    let mut candidate = node_cell(shape, data, index);
                    let area = cell_area(shape, &candidate);
                    cell_union(shape, &mut candidate, cell);
                    let growth = cell_area(shape, &candidate) - area;
                    if index == 0
                        || growth < min_growth
                        || (growth == min_growth && area < min_area)
                    {
                        min_growth = growth;
                        min_area = area;
                        best = candidate.rowid;
                    }
                }
            }
            nodeno = best;
        }
        self.node(nodeno)?;
        Ok(nodeno)
    }

    /// `AdjustTree`: widens the ancestors' boxes to cover `cell`, just added
    /// under `nodeno`.
    fn adjust_tree(&mut self, nodeno: i64, cell: &Cell) -> Result<()> {
        let shape = self.shape;
        let mut node = nodeno;
        let mut steps = 0;
        while let Some(parent) = self.parent_of(node) {
            steps += 1;
            if steps > 100 {
                return Err(corrupt("rtree parent chain loops"));
            }
            let index = self.rowid_index(parent, node)?;
            let mut parent_cell = self.cell(parent, index)?;
            if !cell_contains(shape, &parent_cell, cell) {
                cell_union(shape, &mut parent_cell, cell);
                node_overwrite_cell(shape, self.node_mut(parent)?, &parent_cell, index);
            }
            node = parent;
        }
        Ok(())
    }

    /// `updateMapping`: records that `rowid` (an entry at height 0, else a
    /// child node) now lives in `nodeno`.
    fn update_mapping(&mut self, rowid: i64, nodeno: i64, height: usize) {
        if height == 0 {
            self.set_rowid_node(rowid, nodeno);
        } else {
            self.set_parent(rowid, nodeno);
        }
    }

    /// `rtreeInsertCell`.
    fn insert_cell(&mut self, nodeno: i64, cell: &Cell, height: usize) -> Result<()> {
        let shape = self.shape;
        if node_insert_cell(shape, self.node_mut(nodeno)?, cell) {
            self.split_node(nodeno, cell, height)
        } else {
            self.adjust_tree(nodeno, cell)?;
            self.update_mapping(cell.rowid, nodeno, height);
            Ok(())
        }
    }

    /// `splitNodeStartree`: the R*-tree split. Fills the two (zeroed) node
    /// images and returns their bounding boxes.
    fn split_cells(&self, cells: &[Cell], left: &mut [u8], right: &mut [u8]) -> (Cell, Cell) {
        let shape = self.shape;
        let count = cells.len();
        let min = shape.min_cells();
        let sorted: Vec<Vec<usize>> = (0..shape.dimensions)
            .map(|dim| {
                let mut indices: Vec<usize> = (0..count).collect();
                sort_by_dimension(shape, &mut indices, dim, cells);
                indices
            })
            .collect();

        let mut best_dim = 0;
        let mut best_split = 0;
        let mut best_margin = 0.0;
        for (dim, order) in sorted.iter().enumerate() {
            let mut margin = 0.0;
            let mut best_overlap = 0.0;
            let mut best_area = 0.0;
            let mut best_left = 0;
            for n_left in min..=(count - min) {
                let mut left_box = cells[order[0]];
                let mut right_box = cells[order[count - 1]];
                for (kk, &cell_index) in order.iter().enumerate().take(count - 1).skip(1) {
                    if kk < n_left {
                        cell_union(shape, &mut left_box, &cells[cell_index]);
                    } else {
                        cell_union(shape, &mut right_box, &cells[cell_index]);
                    }
                }
                margin += cell_margin(shape, &left_box);
                margin += cell_margin(shape, &right_box);
                let overlap = cell_overlap(shape, &left_box, &right_box);
                let area = cell_area(shape, &left_box) + cell_area(shape, &right_box);
                if n_left == min
                    || overlap < best_overlap
                    || (overlap == best_overlap && area < best_area)
                {
                    best_left = n_left;
                    best_overlap = overlap;
                    best_area = area;
                }
            }
            if dim == 0 || margin < best_margin {
                best_dim = dim;
                best_margin = margin;
                best_split = best_left;
            }
        }

        let order = &sorted[best_dim];
        let mut left_box = cells[order[0]];
        let mut right_box = cells[order[best_split]];
        for (position, &cell_index) in order.iter().enumerate() {
            let cell = &cells[cell_index];
            if position < best_split {
                node_insert_cell(shape, left, cell);
                cell_union(shape, &mut left_box, cell);
            } else {
                node_insert_cell(shape, right, cell);
                cell_union(shape, &mut right_box, cell);
            }
        }
        (left_box, right_box)
    }

    /// `SplitNode`: `nodeno` is full and `cell` must go in it.
    fn split_node(&mut self, nodeno: i64, cell: &Cell, height: usize) -> Result<()> {
        let shape = self.shape;
        let count = self.cell_count(nodeno)?;
        let mut cells = Vec::with_capacity(count + 1);
        for index in 0..count {
            cells.push(self.cell(nodeno, index)?);
        }
        cells.push(*cell);
        // nodeZero: keep the depth bytes, clear the rest.
        self.node_mut(nodeno)?[2..].fill(0);

        let is_root = nodeno == 1;
        let right_parent = if is_root {
            let depth = self.depth() + 1;
            self.set_depth(depth)?;
            1
        } else {
            self.parent_of(nodeno)
                .ok_or_else(|| corrupt(format!("rtree node {nodeno} has no parent")))?
        };

        let mut left = vec![0_u8; shape.node_size];
        let mut right = vec![0_u8; shape.node_size];
        let (mut left_box, mut right_box) = self.split_cells(&cells, &mut left, &mut right);

        // The right node is numbered first; a root split also numbers its new
        // left node, while any other split keeps the old node on the left.
        let right_no = self.allocate_node(right);
        let left_no = if is_root {
            self.allocate_node(left)
        } else {
            self.node_mut(nodeno)?.copy_from_slice(&left);
            nodeno
        };
        if !is_root {
            // nodeNew(pLeft->pParent): the new right node's in-memory parent.
            self.parents.insert(right_no, right_parent);
        }
        right_box.rowid = right_no;
        left_box.rowid = left_no;

        if is_root {
            self.insert_cell(1, &left_box, height + 1)?;
        } else {
            let (parent, index) = self.parent_index(left_no)?;
            node_overwrite_cell(shape, self.node_mut(parent)?, &left_box, index);
            self.adjust_tree(parent, &left_box)?;
        }
        self.insert_cell(right_parent, &right_box, height + 1)?;

        let mut new_cell_is_right = false;
        for index in 0..self.cell_count(right_no)? {
            let rowid = node_rowid(shape, self.node(right_no)?, index);
            self.update_mapping(rowid, right_no, height);
            if rowid == cell.rowid {
                new_cell_is_right = true;
            }
        }
        if is_root {
            for index in 0..self.cell_count(left_no)? {
                let rowid = node_rowid(shape, self.node(left_no)?, index);
                self.update_mapping(rowid, left_no, height);
            }
        } else if !new_cell_is_right {
            self.update_mapping(cell.rowid, left_no, height);
        }
        Ok(())
    }

    /// `fixBoundingBox`: shrinks the ancestors' boxes after a delete.
    fn fix_bounding_box(&mut self, nodeno: i64) -> Result<()> {
        let shape = self.shape;
        let mut node = nodeno;
        while let Some(parent) = self.parent_of(node) {
            let count = self.cell_count(node)?;
            let mut bbox = self.cell(node, 0)?;
            for index in 1..count {
                let cell = self.cell(node, index)?;
                cell_union(shape, &mut bbox, &cell);
            }
            bbox.rowid = node;
            let index = self.rowid_index(parent, node)?;
            node_overwrite_cell(shape, self.node_mut(parent)?, &bbox, index);
            node = parent;
        }
        Ok(())
    }

    /// `deleteCell`: removes cell `index` from `nodeno`, condensing the node
    /// away when it falls below the minimum fill.
    fn delete_cell(&mut self, nodeno: i64, index: usize, height: usize) -> Result<()> {
        let shape = self.shape;
        node_delete_cell(shape, self.node_mut(nodeno)?, index);
        if self.parent_of(nodeno).is_some() {
            if self.cell_count(nodeno)? < shape.min_cells() {
                self.remove_node(nodeno, height)?;
            } else {
                self.fix_bounding_box(nodeno)?;
            }
        }
        Ok(())
    }

    /// `removeNode`: unlinks `nodeno` from its parent and deletes its
    /// `_node` and `_parent` rows, keeping its cells for reinsertion.
    fn remove_node(&mut self, nodeno: i64, height: usize) -> Result<()> {
        let (parent, index) = self.parent_index(nodeno)?;
        self.delete_cell(parent, index, height + 1)?;
        let data = self
            .nodes
            .remove(&nodeno)
            .ok_or_else(|| corrupt(format!("rtree node {nodeno} is missing")))?;
        self.changes.nodes.insert(nodeno);
        self.parents.remove(&nodeno);
        self.changes.parents.insert(nodeno);
        self.deleted.push((data, height));
        Ok(())
    }

    /// `reinsertNodeContent`.
    fn reinsert_node_content(&mut self, data: &[u8], height: usize) -> Result<()> {
        let shape = self.shape;
        for index in 0..node_cell_count(data) {
            let cell = node_cell(shape, data, index);
            let target = self.choose_leaf(&cell, height)?;
            self.insert_cell(target, &cell, height)?;
        }
        Ok(())
    }

    /// `rtreeDeleteRowid`. Deleting a rowid that is not present is a no-op.
    pub fn delete_rowid(&mut self, rowid: i64) -> Result<()> {
        if let Some(row) = self.rowids.get(&rowid) {
            let leaf = row
                .nodeno
                .ok_or_else(|| corrupt(format!("rtree rowid {rowid} has no node")))?;
            let index = self.rowid_index(leaf, rowid)?;
            self.delete_cell(leaf, index, 0)?;
        }
        if self.rowids.remove(&rowid).is_some() {
            self.changes.rowids.insert(rowid);
        }

        let depth = self.depth();
        if depth > 0 && self.cell_count(1)? == 1 {
            let child = node_rowid(self.shape, self.node(1)?, 0);
            self.remove_node(child, depth - 1)?;
            self.set_depth(depth - 1)?;
        }

        while let Some((data, height)) = self.deleted.pop() {
            self.reinsert_node_content(&data, height)?;
        }
        Ok(())
    }

    /// Inserts an entry (the insert half of `rtreeUpdate`). The rowid must
    /// not be present.
    pub fn insert_entry(&mut self, cell: &Cell, aux: Vec<SqliteValue>) -> Result<()> {
        let leaf = self.choose_leaf(cell, 0)?;
        self.insert_cell(leaf, cell, 0)?;
        if self.aux_count > 0 {
            let aux_count = self.aux_count;
            let row = self
                .rowids
                .get_mut(&cell.rowid)
                .ok_or_else(|| corrupt(format!("rtree rowid {} was not mapped", cell.rowid)))?;
            row.aux = aux;
            row.aux.resize(aux_count, SqliteValue::Null);
            self.changes.rowids.insert(cell.rowid);
        }
        Ok(())
    }

    /// The cell an entry occupies, found through `_rowid`.
    pub fn entry_cell(&self, rowid: i64) -> Result<Option<SearchHit>> {
        let Some(row) = self.rowids.get(&rowid) else {
            return Ok(None);
        };
        let leaf = row
            .nodeno
            .ok_or_else(|| corrupt(format!("rtree rowid {rowid} has no node")))?;
        let cell = self.rowid_index(leaf, rowid)?;
        Ok(Some(SearchHit {
            rowid,
            node: leaf,
            cell,
        }))
    }

    /// Coordinate `coord` of the cell at `hit`, as raw bits.
    pub fn hit_coord(&self, hit: SearchHit, coord: usize) -> Result<u32> {
        let data = self.node(hit.node)?;
        if hit.cell >= node_cell_count(data) || coord >= self.shape.coords() {
            return Err(corrupt("rtree cursor points past its node"));
        }
        Ok(node_coord(self.shape, data, hit.cell, coord))
    }

    /// All cells matching every constraint, in stock's depth-first
    /// traversal order (`rtreeStepToLeaf` with every score zero).
    pub fn search(&self, constraints: &[SearchConstraint]) -> Result<Vec<SearchHit>> {
        let mut hits = Vec::new();
        let level = self.depth() + 1;
        self.search_node(1, level, constraints, &mut hits, 0)?;
        Ok(hits)
    }

    fn search_node(
        &self,
        nodeno: i64,
        level: usize,
        constraints: &[SearchConstraint],
        hits: &mut Vec<SearchHit>,
        recursion: usize,
    ) -> Result<()> {
        if recursion > RTREE_MAX_DEPTH + 1 {
            return Err(corrupt("rtree is deeper than its root says"));
        }
        let shape = self.shape;
        let data = self.node(nodeno)?;
        let count = node_cell_count(data);
        if count > RTREE_MAXCELLS.max(shape.max_cells()) {
            return Err(corrupt(format!("rtree node {nodeno} claims {count} cells")));
        }
        for index in 0..count {
            if !constraints
                .iter()
                .all(|constraint| cell_passes(shape, data, index, level, constraint))
            {
                continue;
            }
            if level > 1 {
                let child = node_rowid(shape, data, index);
                self.search_node(child, level - 1, constraints, hits, recursion + 1)?;
            } else {
                hits.push(SearchHit {
                    rowid: node_rowid(shape, data, index),
                    node: nodeno,
                    cell: index,
                });
            }
        }
        Ok(())
    }
}

/// `rtreeLeafConstraint` / `rtreeNonleafConstraint`, plus the geometry
/// callback. `level` is 1 for a leaf node's cells.
fn cell_passes(
    shape: NodeShape,
    data: &[u8],
    index: usize,
    level: usize,
    constraint: &SearchConstraint,
) -> bool {
    let value = constraint.value;
    match &constraint.test {
        ConstraintTest::True => true,
        ConstraintTest::False => false,
        ConstraintTest::Geometry(geometry) => {
            let bbox: Vec<f64> = (0..shape.coords())
                .map(|coord| shape.dcoord(node_coord(shape, data, index, coord)))
                .collect();
            !matches!(geometry.query_func(&bbox), RtreeQueryResult::Exclude)
        }
        test if level == 1 => {
            let x = shape.dcoord(node_coord(shape, data, index, constraint.coord));
            match test {
                ConstraintTest::Le => x <= value,
                ConstraintTest::Lt => x < value,
                ConstraintTest::Ge => x >= value,
                ConstraintTest::Gt => x > value,
                _ => x == value,
            }
        }
        test => {
            let lower = constraint.coord & !1;
            let lo = shape.dcoord(node_coord(shape, data, index, lower));
            let hi = shape.dcoord(node_coord(shape, data, index, lower + 1));
            match test {
                ConstraintTest::Eq => value >= lo && value <= hi,
                ConstraintTest::Le | ConstraintTest::Lt => value >= lo,
                _ => value <= hi,
            }
        }
    }
}
