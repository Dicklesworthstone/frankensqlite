//! The `rtree` / `rtree_i32` virtual table modules.
//!
//! The table's state is a [`StockTree`]: stock SQLite's node, rowid and
//! parent tables held in memory, mutated by the ported `rtree.c` algorithms.
//! A table created by FrankenSQLite (or by stock SQLite) persists it in the
//! three stock shadow tables; the engine drains the rows each statement
//! changed with [`RtreeVirtualTable::take_shadow_writes`]. Tables created by
//! older FrankenSQLite releases instead store one row per entry in their own
//! b-tree and are rebuilt from those rows on open.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use fsqlite_error::{FrankenError, Result};
use fsqlite_func::vtab::{
    ColumnContext, ConstraintOp, ErasedVtabInstance, IndexInfo, TransactionalVtabState,
    VirtualTable, VirtualTableCursor, VtabModuleFactory,
};
use fsqlite_types::{SqliteValue, TypeAffinity, cx::Cx};

use crate::stock::{
    Cell, ConstraintTest, NodeShape, RTREE_DEFAULT_ROWEST, RowidRow, SearchConstraint, SearchHit,
    ShadowChanges, StockTree, rtree_value_down, rtree_value_up,
};
use crate::{MAX_DIMENSIONS, RtreeCoordType, RtreeGeometry};

/// `xBestIndex` strategy: look one entry up by rowid.
const RTREE_LOOKUP_ROWID: i32 = 1;
/// `xBestIndex` strategy: walk the tree under the constraints in `idxStr`.
const RTREE_SCAN_TREE: i32 = 2;

/// `idxStr` operator bytes (`RTREE_EQ` ... `RTREE_MATCH`).
const OP_EQ: u8 = b'A';
const OP_LE: u8 = b'B';
const OP_LT: u8 = b'C';
const OP_GE: u8 = b'D';
const OP_GT: u8 = b'E';
const OP_MATCH: u8 = b'F';

/// Stock's `zIdxStr` buffer holds at most this many constraints.
const MAX_INDEX_CONSTRAINTS: usize = MAX_DIMENSIONS * 4;

/// Page size assumed for a table that does not persist node images.
const MATERIALIZED_PAGE_SIZE: usize = 4096;

#[derive(Debug, Clone)]
struct ParsedRtreeModuleArgs {
    /// All declared columns in order: `[id, min0, max0, ..., aux0, aux1, ...]`.
    column_names: Vec<String>,
    /// Number of spatial dimensions (coordinate min/max pairs).
    dimensions: usize,
    /// Number of trailing auxiliary (non-indexed) columns.
    aux_count: usize,
}

fn rtree_sqlite_module_args<'a>(args: &'a [&'a str]) -> Result<&'a [&'a str]> {
    args.get(3..).ok_or_else(|| {
        FrankenError::function_error(
            "rtree factory requires SQLite argv: module, database, table, then module arguments",
        )
    })
}

/// Strip surrounding SQL identifier quoting (`"`, `'`, `` ` ``, `[`, `]`) and
/// surrounding whitespace from a raw argv token.
fn unquote_identifier(raw: &str) -> String {
    raw.trim()
        .trim_matches(|ch| matches!(ch, '"' | '\'' | '`' | '[' | ']'))
        .to_owned()
}

fn parse_rtree_module_args(args: &[&str]) -> Result<ParsedRtreeModuleArgs> {
    // Module args are `id, <min0 max0 pairs...>, [+aux TYPE ...]`. The id is
    // argv[0]; every following bare column is a coordinate, and every column
    // whose declaration begins with `+` is an auxiliary (non-indexed) column.
    // SQLite requires all auxiliary columns to come last.
    let Some((id_arg, rest)) = args.split_first() else {
        return Err(FrankenError::function_error(
            "Too few columns for an rtree table",
        ));
    };

    let mut coord_names: Vec<String> = Vec::new();
    let mut aux_names: Vec<String> = Vec::new();
    for arg in rest {
        if let Some(aux) = arg.trim().strip_prefix('+') {
            // Auxiliary column: the name is the first whitespace-delimited token;
            // any trailing declared type is accepted but does not affect the NONE
            // affinity SQLite gives auxiliary rtree columns.
            aux_names.push(unquote_identifier(
                aux.split_whitespace().next().unwrap_or(""),
            ));
        } else if aux_names.is_empty() {
            coord_names.push(unquote_identifier(arg));
        } else {
            return Err(FrankenError::function_error(
                "Auxiliary rtree columns must be last",
            ));
        }
    }

    // Need the id column plus at least one complete coordinate pair.
    if coord_names.len() < 2 {
        return Err(FrankenError::function_error(
            "Too few columns for an rtree table",
        ));
    }
    if !coord_names.len().is_multiple_of(2) {
        return Err(FrankenError::function_error(
            "Wrong number of columns for an rtree table",
        ));
    }
    let dimensions = coord_names.len() / 2;
    if dimensions > MAX_DIMENSIONS {
        return Err(FrankenError::function_error(
            "Too many columns for an rtree table",
        ));
    }

    let aux_count = aux_names.len();
    let mut column_names = Vec::with_capacity(1 + coord_names.len() + aux_count);
    column_names.push(unquote_identifier(id_arg));
    column_names.extend(coord_names);
    column_names.extend(aux_names);

    let mut seen = HashSet::new();
    for column in &column_names {
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
        if !seen.insert(column.to_ascii_lowercase()) {
            return Err(FrankenError::function_error(format!(
                "rtree column '{column}' is declared more than once",
            )));
        }
    }

    Ok(ParsedRtreeModuleArgs {
        column_names,
        dimensions,
        aux_count,
    })
}

fn column_affinity(coord_type: RtreeCoordType, is_id: bool) -> char {
    if is_id || matches!(coord_type, RtreeCoordType::Int32) {
        'D'
    } else {
        'E'
    }
}

/// `sqlite3_value_numeric_type`: text that reads as a number is taken as
/// that number; every other value keeps its type.
fn numeric_value(value: &SqliteValue) -> SqliteValue {
    match value {
        SqliteValue::Text(_) => value.clone().apply_affinity(TypeAffinity::Numeric),
        _ => value.clone(),
    }
}

/// Where a table keeps its entries between connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RtreeStorage {
    /// One row per entry in the table's own b-tree (FrankenSQLite releases
    /// before stock-format R-trees); the engine persists those rows.
    Materialized,
    /// Stock SQLite's `_node`, `_rowid` and `_parent` shadow tables.
    Shadow,
}

/// One shadow table's changed rows: `(rowid, Some(record))` stores the
/// record, `(rowid, None)` deletes the row.
pub type RtreeShadowRowChanges = Vec<(i64, Option<Vec<SqliteValue>>)>;

/// The shadow-table rows R-tree writes changed since the last drain.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RtreeShadowWrites {
    /// `<table>_node(nodeno INTEGER PRIMARY KEY, data)`.
    pub node: RtreeShadowRowChanges,
    /// `<table>_rowid(rowid INTEGER PRIMARY KEY, nodeno, a0, ...)`.
    pub rowid: RtreeShadowRowChanges,
    /// `<table>_parent(nodeno INTEGER PRIMARY KEY, parentnode)`.
    pub parent: RtreeShadowRowChanges,
}

impl RtreeShadowWrites {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.node.is_empty() && self.rowid.is_empty() && self.parent.is_empty()
    }
}

/// Runtime virtual-table instance for `CREATE VIRTUAL TABLE ... USING rtree(...)`.
#[derive(Clone)]
pub struct RtreeVirtualTable {
    tree: Arc<StockTree>,
    storage: RtreeStorage,
    coord_type: RtreeCoordType,
    dimensions: usize,
    /// Declared table name (SQLite argv[2]), for constraint messages and the
    /// shadow table names.
    table_name: String,
    /// Declared column names `[id, min0, max0, ..., aux0, ...]`.
    column_names: Vec<String>,
    /// Number of trailing auxiliary (non-indexed) columns.
    aux_count: usize,
    /// Geometry callbacks for `<table> MATCH '<name>'`. Not transactional.
    geometries: HashMap<String, Arc<dyn RtreeGeometry>>,
    txn_state: TransactionalVtabState<RtreeVirtualTableSnapshot>,
}

impl std::fmt::Debug for RtreeVirtualTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RtreeVirtualTable")
            .field("table_name", &self.table_name)
            .field("storage", &self.storage)
            .field("coord_type", &self.coord_type)
            .field("dimensions", &self.dimensions)
            .field("entries", &self.tree.len())
            .field("geometry_count", &self.geometries.len())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
struct RtreeVirtualTableSnapshot {
    tree: Arc<StockTree>,
}

impl RtreeVirtualTable {
    /// Build an instance from the full SQLite argv `[module, database, table, ...module args]`.
    fn from_args(args: &[&str], coord_type: RtreeCoordType) -> Result<Self> {
        let module_args = rtree_sqlite_module_args(args)?;
        let parsed = parse_rtree_module_args(module_args)?;
        let table_name = args
            .get(2)
            .map(|name| unquote_identifier(name))
            .unwrap_or_default();
        let mut tree = StockTree::new_empty(
            Self::shape_for(coord_type, parsed.dimensions, MATERIALIZED_PAGE_SIZE),
            parsed.aux_count,
        );
        tree.take_changes();
        Ok(Self {
            tree: Arc::new(tree),
            storage: RtreeStorage::Materialized,
            coord_type,
            dimensions: parsed.dimensions,
            table_name,
            column_names: parsed.column_names,
            aux_count: parsed.aux_count,
            geometries: HashMap::new(),
            txn_state: TransactionalVtabState::default(),
        })
    }

    fn shape_for(coord_type: RtreeCoordType, dimensions: usize, page_size: usize) -> NodeShape {
        NodeShape {
            dimensions,
            int32: matches!(coord_type, RtreeCoordType::Int32),
            node_size: NodeShape::node_size_for_page(page_size, dimensions),
        }
    }

    /// Number of entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tree.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }

    /// Whether this table persists the stock shadow tables (rather than the
    /// legacy one-row-per-entry layout).
    #[must_use]
    pub fn uses_shadow_storage(&self) -> bool {
        self.storage == RtreeStorage::Shadow
    }

    /// The shadow tables `xCreate` makes, as `(name, CREATE TABLE text)` in
    /// stock's order, with stock's exact text.
    #[must_use]
    pub fn shadow_table_definitions(&self) -> Vec<(String, String)> {
        let name = &self.table_name;
        let quoted = |suffix: &str| format!("\"{}{suffix}\"", name.replace('"', "\"\""));
        let mut rowid = format!(
            "CREATE TABLE {}(rowid INTEGER PRIMARY KEY,nodeno",
            quoted("_rowid")
        );
        for aux in 0..self.aux_count {
            rowid.push_str(&format!(",a{aux}"));
        }
        rowid.push(')');
        vec![
            (format!("{name}_rowid"), rowid),
            (
                format!("{name}_node"),
                format!(
                    "CREATE TABLE {}(nodeno INTEGER PRIMARY KEY,data)",
                    quoted("_node")
                ),
            ),
            (
                format!("{name}_parent"),
                format!(
                    "CREATE TABLE {}(nodeno INTEGER PRIMARY KEY,parentnode)",
                    quoted("_parent")
                ),
            ),
        ]
    }

    /// Switches a newly created table to stock shadow storage: an empty root
    /// node sized for `page_size`, which the next drain writes to `_node`.
    pub fn init_shadow_storage(&mut self, page_size: usize) {
        self.tree = Arc::new(StockTree::new_empty(
            Self::shape_for(self.coord_type, self.dimensions, page_size),
            self.aux_count,
        ));
        self.storage = RtreeStorage::Shadow;
    }

    /// Loads the tree from its shadow tables' rows, as read from storage.
    /// Each row's values may lead with its INTEGER PRIMARY KEY column.
    pub fn load_shadow_storage(
        &mut self,
        node_rows: &[(i64, Vec<SqliteValue>)],
        rowid_rows: &[(i64, Vec<SqliteValue>)],
        parent_rows: &[(i64, Vec<SqliteValue>)],
    ) -> Result<()> {
        let corrupt = |detail: String| FrankenError::DatabaseCorrupt { detail };
        let payload = |values: &[SqliteValue], width: usize| -> Option<Vec<SqliteValue>> {
            match values.len() {
                len if len == width => Some(values.to_vec()),
                len if len == width + 1 => Some(values[1..].to_vec()),
                _ => None,
            }
        };
        let table = &self.table_name;

        let mut nodes = Vec::with_capacity(node_rows.len());
        for (nodeno, values) in node_rows {
            let data = match payload(values, 1).as_deref() {
                Some([SqliteValue::Blob(data)]) => data.to_vec(),
                Some([SqliteValue::Null]) => Vec::new(),
                _ => {
                    return Err(corrupt(format!(
                        "rtree node {nodeno} in \"{table}_node\" is not a blob"
                    )));
                }
            };
            nodes.push((*nodeno, data));
        }
        let mut rowids = Vec::with_capacity(rowid_rows.len());
        for (rowid, values) in rowid_rows {
            let Some(mut values) = payload(values, 1 + self.aux_count) else {
                return Err(corrupt(format!(
                    "rtree row {rowid} in \"{table}_rowid\" has {} columns",
                    values.len()
                )));
            };
            let aux = values.split_off(1);
            let nodeno = match values[0] {
                SqliteValue::Null => None,
                ref value => Some(value.to_integer()),
            };
            rowids.push((*rowid, RowidRow { nodeno, aux }));
        }
        let mut parents = Vec::with_capacity(parent_rows.len());
        for (nodeno, values) in parent_rows {
            let Some(values) = payload(values, 1) else {
                return Err(corrupt(format!(
                    "rtree row {nodeno} in \"{table}_parent\" has {} columns",
                    values.len()
                )));
            };
            parents.push((*nodeno, values[0].to_integer()));
        }

        self.tree = Arc::new(StockTree::from_shadow_rows(
            table,
            self.dimensions,
            matches!(self.coord_type, RtreeCoordType::Int32),
            self.aux_count,
            nodes,
            rowids,
            parents,
        )?);
        self.storage = RtreeStorage::Shadow;
        Ok(())
    }

    /// Drains the shadow-table rows changed since the last call, as the
    /// records to store (the INTEGER PRIMARY KEY column stored as NULL, as
    /// stock stores it). A table without shadow storage reports nothing.
    pub fn take_shadow_writes(&mut self) -> RtreeShadowWrites {
        if self.storage != RtreeStorage::Shadow {
            return RtreeShadowWrites::default();
        }
        let ShadowChanges {
            nodes,
            rowids,
            parents,
        } = Arc::make_mut(&mut self.tree).take_changes();
        let tree = &self.tree;
        RtreeShadowWrites {
            node: nodes
                .into_iter()
                .map(|nodeno| {
                    let record = tree
                        .node_blob(nodeno)
                        .map(|data| vec![SqliteValue::Null, SqliteValue::Blob(Arc::from(data))]);
                    (nodeno, record)
                })
                .collect(),
            rowid: rowids
                .into_iter()
                .map(|rowid| {
                    let record = tree.rowid_row(rowid).map(|row| {
                        let mut record = Vec::with_capacity(2 + row.aux.len());
                        record.push(SqliteValue::Null);
                        record.push(row.nodeno.map_or(SqliteValue::Null, SqliteValue::Integer));
                        record.extend(row.aux.iter().cloned());
                        record
                    });
                    (rowid, record)
                })
                .collect(),
            parent: parents
                .into_iter()
                .map(|nodeno| {
                    let record = tree
                        .parent_row(nodeno)
                        .map(|parent| vec![SqliteValue::Null, SqliteValue::Integer(parent)]);
                    (nodeno, record)
                })
                .collect(),
        }
    }

    /// Register a custom geometry callback on a live virtual table instance.
    pub fn register_geometry(&mut self, name: &str, geom: Box<dyn RtreeGeometry>) {
        self.geometries.insert(name.to_owned(), Arc::from(geom));
    }

    /// Rebuild a legacy materialized table from its persisted rows.
    ///
    /// Each persisted row is `[<id>?, min0, max0, ..., aux0, aux1, ...]`: the
    /// coordinate pairs are followed by the auxiliary column values.
    pub fn rebuild_rows(&mut self, rows: &[(i64, Vec<SqliteValue>)]) -> Result<()> {
        let mut tree = StockTree::new_empty(self.tree.shape(), self.aux_count);
        let payload_len = self.dimensions * 2 + self.aux_count;
        for (rowid, values) in rows {
            let payload = match values.len() {
                len if len == payload_len => values.as_slice(),
                len if len == payload_len + 1 => {
                    if values[0].as_integer() != Some(*rowid) {
                        return Err(FrankenError::DatabaseCorrupt {
                            detail: format!(
                                "rtree persisted row {rowid} stores a mismatched id column"
                            ),
                        });
                    }
                    &values[1..]
                }
                len => {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "rtree persisted row {rowid} has {len} values; expected {payload_len} or {}",
                            payload_len + 1
                        ),
                    });
                }
            };
            if tree.contains_rowid(*rowid) {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!("rtree persisted row {rowid} is duplicated"),
                });
            }
            let mut cell = self
                .coordinate_cell(&payload[..self.dimensions * 2])
                .map_err(|error| FrankenError::DatabaseCorrupt {
                    detail: format!("rtree persisted row {rowid}: {error}"),
                })?;
            cell.rowid = *rowid;
            tree.insert_entry(&cell, payload[self.dimensions * 2..].to_vec())?;
        }
        tree.take_changes();
        self.tree = Arc::new(tree);
        Ok(())
    }

    /// The declared column name of coordinate `coord` (0-based).
    fn coord_column_name(&self, coord: usize) -> &str {
        self.column_names.get(coord + 1).map_or("", String::as_str)
    }

    /// The new entry's box from its coordinate values, rounded outward to
    /// `f32` (`rtreeValueDown` / `rtreeValueUp`) or truncated to `i32`
    /// (`sqlite3_value_int`), rejecting a pair whose low end exceeds its
    /// high end.
    fn coordinate_cell(&self, values: &[SqliteValue]) -> Result<Cell> {
        let mut cell = Cell::new(0);
        for dim in 0..self.dimensions {
            let (lo, hi) = (&values[dim * 2], &values[dim * 2 + 1]);
            let swapped = match self.coord_type {
                RtreeCoordType::Float32 => {
                    let (lo, hi) = (
                        rtree_value_down(lo.to_float()),
                        rtree_value_up(hi.to_float()),
                    );
                    cell.coords[dim * 2] = lo.to_bits();
                    cell.coords[dim * 2 + 1] = hi.to_bits();
                    lo > hi
                }
                RtreeCoordType::Int32 => {
                    let (lo, hi) = (lo.to_integer() as i32, hi.to_integer() as i32);
                    cell.coords[dim * 2] = lo as u32;
                    cell.coords[dim * 2 + 1] = hi as u32;
                    lo > hi
                }
            };
            if swapped {
                return Err(FrankenError::RtreeConstraint {
                    constraint: format!(
                        "{}.({}<={})",
                        self.table_name,
                        self.coord_column_name(dim * 2),
                        self.coord_column_name(dim * 2 + 1),
                    ),
                });
            }
        }
        Ok(cell)
    }

    fn snapshot_state(&self) -> RtreeVirtualTableSnapshot {
        RtreeVirtualTableSnapshot {
            tree: Arc::clone(&self.tree),
        }
    }

    fn restore_state(&mut self, snapshot: RtreeVirtualTableSnapshot) {
        self.tree = snapshot.tree;
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

    /// `rtreeBestIndex`: a rowid equality is a direct lookup (strategy 1);
    /// otherwise every usable comparison on a coordinate and every MATCH
    /// becomes an `idxStr` operator/coordinate pair for the tree walk
    /// (strategy 2). LE, GE and MATCH are omitted from the residual WHERE;
    /// EQ, LT and GT are rechecked, because the stored `f32` box is only an
    /// outward-rounded bound of the inserted value.
    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        let has_match = info
            .constraints
            .iter()
            .any(|constraint| constraint.op == ConstraintOp::Match);
        let coord_count = i32::try_from(self.dimensions * 2).unwrap_or(i32::MAX);
        let mut idx_str = Vec::new();
        for (index, constraint) in info.constraints.iter().enumerate() {
            if idx_str.len() >= MAX_INDEX_CONSTRAINTS * 2 {
                break;
            }
            if !has_match
                && constraint.usable
                && constraint.column <= 0
                && constraint.op == ConstraintOp::Eq
            {
                for usage in &mut info.constraint_usage {
                    usage.argv_index = 0;
                    usage.omit = false;
                }
                info.constraint_usage[index].argv_index = 1;
                info.constraint_usage[index].omit = true;
                info.idx_num = RTREE_LOOKUP_ROWID;
                info.idx_str = None;
                info.estimated_cost = 30.0;
                info.estimated_rows = 1;
                return Ok(());
            }
            let on_coordinate = constraint.column > 0 && constraint.column <= coord_count;
            if !constraint.usable || !(on_coordinate || constraint.op == ConstraintOp::Match) {
                continue;
            }
            let (op, omit) = match constraint.op {
                ConstraintOp::Eq => (OP_EQ, false),
                ConstraintOp::Gt => (OP_GT, false),
                ConstraintOp::Le => (OP_LE, true),
                ConstraintOp::Lt => (OP_LT, false),
                ConstraintOp::Ge => (OP_GE, true),
                ConstraintOp::Match => (OP_MATCH, true),
                _ => continue,
            };
            idx_str.push(op);
            idx_str.push((i32::from(b'0') + constraint.column - 1).clamp(0, 127) as u8);
            info.constraint_usage[index].argv_index =
                i32::try_from(idx_str.len() / 2).unwrap_or(i32::MAX);
            info.constraint_usage[index].omit = omit;
        }
        let rows = RTREE_DEFAULT_ROWEST >> (idx_str.len() / 2);
        info.idx_num = RTREE_SCAN_TREE;
        info.idx_str =
            (!idx_str.is_empty()).then(|| String::from_utf8_lossy(&idx_str).into_owned());
        info.estimated_cost = 6.0 * rows as f64;
        info.estimated_rows = rows;
        Ok(())
    }

    fn open(&self) -> Result<Self::Cursor> {
        Ok(RtreeCursor {
            tree: Arc::clone(&self.tree),
            coord_type: self.coord_type,
            geometries: self.geometries.clone(),
            hits: Vec::new(),
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

    /// `rtreeUpdate`. `args` is `[old rowid, new rowid, id, coordinates...,
    /// auxiliary...]`; as in stock, the id column (`args[2]`) names the new
    /// entry, and a NULL id takes the largest rowid plus one.
    fn update(&mut self, _cx: &Cx, args: &[SqliteValue]) -> Result<Option<i64>> {
        let Some(old) = args.first() else {
            return Err(FrankenError::function_error("rtree: empty update args"));
        };
        let old_rowid = (!old.is_null()).then(|| old.to_integer());
        if args.len() == 1 {
            if let Some(rowid) = old_rowid {
                Arc::make_mut(&mut self.tree).delete_rowid(rowid)?;
            }
            return Ok(None);
        }

        let coord_count = self.dimensions * 2;
        let expected = 3 + coord_count + self.aux_count;
        if args.len() != expected {
            return Err(FrankenError::function_error(format!(
                "rtree update expected {expected} arguments, got {}",
                args.len()
            )));
        }
        let mut cell = self.coordinate_cell(&args[3..3 + coord_count])?;
        let id = &args[2];
        let explicit_rowid = (!id.is_null()).then(|| id.to_integer());
        if let Some(rowid) = explicit_rowid
            && old_rowid != Some(rowid)
            && self.tree.contains_rowid(rowid)
        {
            return Err(FrankenError::UniqueViolation {
                columns: format!(
                    "{}.{}",
                    self.table_name,
                    self.column_names.first().map_or("", String::as_str)
                ),
            });
        }

        let tree = Arc::make_mut(&mut self.tree);
        if let Some(rowid) = old_rowid {
            tree.delete_rowid(rowid)?;
        }
        cell.rowid = explicit_rowid.unwrap_or_else(|| tree.new_rowid());
        tree.insert_entry(&cell, args[3 + coord_count..].to_vec())?;
        Ok(old_rowid.is_none().then_some(cell.rowid))
    }

    fn restore_materialized_rows(
        &mut self,
        cx: &Cx,
        rows: &[(i64, Vec<SqliteValue>)],
    ) -> Result<bool> {
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;
        self.rebuild_rows(rows)?;
        Ok(true)
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

/// A scan over one snapshot of the tree.
pub struct RtreeCursor {
    tree: Arc<StockTree>,
    coord_type: RtreeCoordType,
    geometries: HashMap<String, Arc<dyn RtreeGeometry>>,
    hits: Vec<SearchHit>,
    pos: usize,
}

impl std::fmt::Debug for RtreeCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RtreeCursor")
            .field("hits", &self.hits.len())
            .field("pos", &self.pos)
            .finish_non_exhaustive()
    }
}

impl RtreeCursor {
    /// `rtreeFilter`'s conversion of one constraint value.
    fn search_constraint(&self, op: u8, coord: usize, value: &SqliteValue) -> SearchConstraint {
        let mut constraint = SearchConstraint {
            test: ConstraintTest::False,
            coord,
            value: 0.0,
        };
        if op == OP_MATCH {
            if let Some(geometry) = value.as_text().and_then(|name| self.geometries.get(name)) {
                constraint.test = ConstraintTest::Geometry(Arc::clone(geometry));
            }
            return constraint;
        }
        let mut test = match op {
            OP_EQ => ConstraintTest::Eq,
            OP_LE => ConstraintTest::Le,
            OP_LT => ConstraintTest::Lt,
            OP_GE => ConstraintTest::Ge,
            _ => ConstraintTest::Gt,
        };
        match numeric_value(value) {
            SqliteValue::Integer(integer) => {
                constraint.value = integer as f64;
                // Beyond 2^48 the double may round toward the bound, so a
                // strict comparison could drop a row stock keeps.
                if integer.unsigned_abs() >= 1 << 48 {
                    test = match test {
                        ConstraintTest::Lt => ConstraintTest::Le,
                        ConstraintTest::Gt => ConstraintTest::Ge,
                        other => other,
                    };
                }
            }
            SqliteValue::Float(float) => constraint.value = float,
            SqliteValue::Null => test = ConstraintTest::False,
            _ => {
                test = if matches!(test, ConstraintTest::Lt | ConstraintTest::Le) {
                    ConstraintTest::True
                } else {
                    ConstraintTest::False
                };
            }
        }
        constraint.test = test;
        constraint
    }

    fn current(&self) -> Option<SearchHit> {
        self.hits.get(self.pos).copied()
    }
}

impl VirtualTableCursor for RtreeCursor {
    fn filter(
        &mut self,
        _cx: &Cx,
        idx_num: i32,
        idx_str: Option<&str>,
        args: &[SqliteValue],
    ) -> Result<()> {
        self.pos = 0;
        self.hits.clear();
        if idx_num == RTREE_LOOKUP_ROWID {
            let Some(value) = args.first() else {
                return Err(FrankenError::function_error(
                    "rtree rowid lookup requires one argument",
                ));
            };
            let rowid = match numeric_value(value) {
                SqliteValue::Integer(rowid) => Some(rowid),
                SqliteValue::Float(float) => {
                    let rowid = float as i64;
                    (rowid as f64 == float).then_some(rowid)
                }
                _ => None,
            };
            if let Some(rowid) = rowid
                && let Some(hit) = self.tree.entry_cell(rowid)?
            {
                self.hits.push(hit);
            }
            return Ok(());
        }

        let ops = idx_str.unwrap_or("").as_bytes();
        if ops.len() != args.len() * 2 {
            return Err(FrankenError::function_error(format!(
                "rtree filter got {} arguments for index string {:?}",
                args.len(),
                idx_str.unwrap_or("")
            )));
        }
        let (pairs, _) = ops.as_chunks::<2>();
        let constraints: Vec<SearchConstraint> = pairs
            .iter()
            .zip(args)
            .map(|([op, coord], value)| {
                self.search_constraint(*op, usize::from(coord.saturating_sub(b'0')), value)
            })
            .collect();
        self.hits = self.tree.search(&constraints)?;
        Ok(())
    }

    fn next(&mut self, _cx: &Cx) -> Result<()> {
        if self.pos < self.hits.len() {
            self.pos += 1;
        }
        Ok(())
    }

    fn eof(&self) -> bool {
        self.pos >= self.hits.len()
    }

    /// `rtreeColumn`: the id, a coordinate as stored (`f32` widened to a
    /// double, or `i32`), or an auxiliary value from `_rowid`.
    fn column(&self, ctx: &mut ColumnContext, col: i32) -> Result<()> {
        let (Some(hit), Ok(col)) = (self.current(), usize::try_from(col)) else {
            ctx.set_value(SqliteValue::Null);
            return Ok(());
        };
        let coord_count = self.tree.shape().dimensions * 2;
        let value = if col == 0 {
            SqliteValue::Integer(hit.rowid)
        } else if col <= coord_count {
            let bits = self.tree.hit_coord(hit, col - 1)?;
            match self.coord_type {
                RtreeCoordType::Float32 => SqliteValue::Float(f64::from(f32::from_bits(bits))),
                RtreeCoordType::Int32 => SqliteValue::Integer(i64::from(bits as i32)),
            }
        } else {
            self.tree
                .rowid_row(hit.rowid)
                .and_then(|row| row.aux.get(col - coord_count - 1))
                .cloned()
                .unwrap_or(SqliteValue::Null)
        };
        ctx.set_value(value);
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.current().map_or(0, |hit| hit.rowid))
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
        let Ok(module_args) = rtree_sqlite_module_args(args) else {
            return Vec::new();
        };
        let Ok(parsed) = parse_rtree_module_args(module_args) else {
            return Vec::new();
        };
        // Columns are `[id, <coordinates>, <auxiliary>]`. The id and coordinate
        // columns take their affinity from the coordinate type; auxiliary columns
        // get NONE affinity ('A'), matching SQLite, which never coerces them.
        let coord_end = 1 + parsed.dimensions * 2;
        parsed
            .column_names
            .into_iter()
            .enumerate()
            .map(|(index, name)| {
                let affinity = if index >= coord_end {
                    'A'
                } else {
                    column_affinity(self.coord_type, index == 0)
                };
                (name, affinity)
            })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RtreeQueryResult;
    use fsqlite_func::vtab::IndexConstraint;

    const SPATIAL: [&str; 8] = [
        "rtree", "main", "spatial", "id", "min_x", "max_x", "min_y", "max_y",
    ];

    fn table(args: &[&str], coord_type: RtreeCoordType) -> RtreeVirtualTable {
        RtreeVirtualTable::from_args(args, coord_type).unwrap()
    }

    fn insert(
        table: &mut RtreeVirtualTable,
        id: SqliteValue,
        values: &[SqliteValue],
    ) -> Result<Option<i64>> {
        let mut args = vec![SqliteValue::Null, id.clone(), id];
        args.extend(values.iter().cloned());
        VirtualTable::update(table, &Cx::new(), &args)
    }

    fn floats(values: &[f64]) -> Vec<SqliteValue> {
        values
            .iter()
            .map(|value| SqliteValue::Float(*value))
            .collect()
    }

    fn scan(
        table: &RtreeVirtualTable,
        idx_num: i32,
        idx_str: Option<&str>,
        args: &[SqliteValue],
    ) -> Vec<Vec<SqliteValue>> {
        let cx = Cx::new();
        let mut cursor = table.open().unwrap();
        cursor.filter(&cx, idx_num, idx_str, args).unwrap();
        let columns = table.column_names.len();
        let mut rows = Vec::new();
        while !cursor.eof() {
            let mut row = Vec::with_capacity(columns);
            for column in 0..columns {
                let mut ctx = ColumnContext::new();
                cursor
                    .column(&mut ctx, i32::try_from(column).unwrap())
                    .unwrap();
                row.push(ctx.take_value().unwrap_or(SqliteValue::Null));
            }
            rows.push(row);
            cursor.next(&cx).unwrap();
        }
        rows
    }

    fn full_scan(table: &RtreeVirtualTable) -> Vec<Vec<SqliteValue>> {
        scan(table, RTREE_SCAN_TREE, None, &[])
    }

    fn constraint(column: i32, op: ConstraintOp) -> IndexConstraint {
        IndexConstraint {
            column,
            op,
            usable: true,
        }
    }

    #[test]
    fn insert_rounds_float_boxes_outward_like_stock() {
        let mut table = table(&SPATIAL, RtreeCoordType::Float32);
        assert_eq!(
            insert(
                &mut table,
                SqliteValue::Integer(1),
                &floats(&[0.1, 0.1, 0.0, 1.0])
            )
            .unwrap(),
            Some(1)
        );
        // Stock stores 0.1 as the f32 just below it (low end) and just above
        // it (high end); the shell prints them as these doubles.
        assert_eq!(
            full_scan(&table),
            vec![vec![
                SqliteValue::Integer(1),
                SqliteValue::Float(0.099_999_986_588_954_93),
                SqliteValue::Float(0.100_000_001_490_116_12),
                SqliteValue::Float(0.0),
                SqliteValue::Float(1.0),
            ]]
        );
    }

    #[test]
    fn null_id_takes_largest_rowid_plus_one_and_new_rowid_argument_is_ignored() {
        let mut table = table(&SPATIAL, RtreeCoordType::Float32);
        insert(
            &mut table,
            SqliteValue::Integer(7),
            &floats(&[0.0, 1.0, 0.0, 1.0]),
        )
        .unwrap();
        // `INSERT INTO spatial(rowid, ...) VALUES(5, ...)`: stock reads the id
        // column, which is NULL, and ignores the separate new-rowid argument.
        let args = [
            vec![
                SqliteValue::Null,
                SqliteValue::Integer(5),
                SqliteValue::Null,
            ],
            floats(&[2.0, 3.0, 2.0, 3.0]),
        ]
        .concat();
        assert_eq!(
            VirtualTable::update(&mut table, &Cx::new(), &args).unwrap(),
            Some(8)
        );
    }

    #[test]
    fn swapped_coordinates_report_stock_constraint_messages() {
        let mut table = table(&SPATIAL, RtreeCoordType::Float32);
        let err = insert(
            &mut table,
            SqliteValue::Integer(1),
            &floats(&[5.0, 2.0, 0.0, 1.0]),
        )
        .unwrap_err();
        assert_eq!(err.error_code(), fsqlite_error::ErrorCode::Constraint);
        assert_eq!(
            err.to_string(),
            "rtree constraint failed: spatial.(min_x<=max_x)"
        );
        let err = insert(
            &mut table,
            SqliteValue::Integer(2),
            &floats(&[0.0, 1.0, 9.0, 3.0]),
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "rtree constraint failed: spatial.(min_y<=max_y)"
        );
        assert!(full_scan(&table).is_empty());

        insert(
            &mut table,
            SqliteValue::Integer(3),
            &floats(&[4.0, 4.0, 4.0, 4.0]),
        )
        .unwrap();
        let update = [
            vec![SqliteValue::Integer(3); 3],
            floats(&[10.0, 4.0, 4.0, 4.0]),
        ]
        .concat();
        let err = VirtualTable::update(&mut table, &Cx::new(), &update).unwrap_err();
        assert_eq!(
            err.to_string(),
            "rtree constraint failed: spatial.(min_x<=max_x)"
        );
        assert_eq!(full_scan(&table)[0][1], SqliteValue::Float(4.0));

        let mut grid = super::tests::table(
            &["rtree_i32", "main", "grid", "id", "a", "b"],
            RtreeCoordType::Int32,
        );
        let err = insert(
            &mut grid,
            SqliteValue::Integer(1),
            &[SqliteValue::Integer(7), SqliteValue::Integer(2)],
        )
        .unwrap_err();
        assert_eq!(err.to_string(), "rtree constraint failed: grid.(a<=b)");
    }

    #[test]
    fn duplicate_id_is_a_unique_violation_and_keeps_both_rows() {
        let mut table = table(&SPATIAL, RtreeCoordType::Float32);
        insert(
            &mut table,
            SqliteValue::Integer(1),
            &floats(&[0.0, 1.0, 0.0, 1.0]),
        )
        .unwrap();
        insert(
            &mut table,
            SqliteValue::Integer(2),
            &floats(&[2.0, 3.0, 2.0, 3.0]),
        )
        .unwrap();
        let err = insert(
            &mut table,
            SqliteValue::Integer(2),
            &floats(&[5.0, 6.0, 5.0, 6.0]),
        )
        .unwrap_err();
        assert_eq!(err.to_string(), "UNIQUE constraint failed: spatial.id");

        // UPDATE moving id 1 onto id 2 fails the same way.
        let update = [
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Integer(2),
                SqliteValue::Integer(2),
            ],
            floats(&[8.0, 9.0, 8.0, 9.0]),
        ]
        .concat();
        let err = VirtualTable::update(&mut table, &Cx::new(), &update).unwrap_err();
        assert!(matches!(err, FrankenError::UniqueViolation { .. }));
        let ids: Vec<_> = full_scan(&table)
            .into_iter()
            .map(|row| row[0].clone())
            .collect();
        assert_eq!(ids, vec![SqliteValue::Integer(1), SqliteValue::Integer(2)]);
    }

    #[test]
    fn update_can_change_the_id_and_returns_none() {
        let mut table = table(&SPATIAL, RtreeCoordType::Float32);
        insert(
            &mut table,
            SqliteValue::Integer(1),
            &floats(&[0.0, 1.0, 0.0, 1.0]),
        )
        .unwrap();
        let update = [
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Integer(10),
                SqliteValue::Integer(10),
            ],
            floats(&[4.0, 5.0, 4.0, 5.0]),
        ]
        .concat();
        assert_eq!(
            VirtualTable::update(&mut table, &Cx::new(), &update).unwrap(),
            None
        );
        assert_eq!(
            full_scan(&table),
            vec![
                [
                    vec![SqliteValue::Integer(10)],
                    floats(&[4.0, 5.0, 4.0, 5.0])
                ]
                .concat()
            ]
        );
    }

    #[test]
    fn auxiliary_columns_round_trip_through_rowid_rows() {
        let mut table = table(
            &[
                "rtree",
                "main",
                "spatial",
                "id",
                "x0",
                "x1",
                "+label TEXT",
                "+weight INTEGER",
            ],
            RtreeCoordType::Float32,
        );
        assert_eq!(table.aux_count, 2);
        assert_eq!(
            table.column_names,
            vec!["id", "x0", "x1", "label", "weight"]
        );
        insert(
            &mut table,
            SqliteValue::Integer(1),
            &[
                SqliteValue::Float(0.0),
                SqliteValue::Float(1.0),
                SqliteValue::Text("box".into()),
                SqliteValue::Integer(42),
            ],
        )
        .unwrap();
        insert(
            &mut table,
            SqliteValue::Integer(2),
            &[
                SqliteValue::Float(5.0),
                SqliteValue::Float(6.0),
                SqliteValue::Null,
                SqliteValue::Null,
            ],
        )
        .unwrap();
        // x0 <= 0.5 AND x1 >= 0.5 finds only id 1, with its auxiliary values.
        let rows = scan(
            &table,
            RTREE_SCAN_TREE,
            Some("B0D1"),
            &[SqliteValue::Float(0.5), SqliteValue::Float(0.5)],
        );
        assert_eq!(
            rows,
            vec![vec![
                SqliteValue::Integer(1),
                SqliteValue::Float(0.0),
                SqliteValue::Float(1.0),
                SqliteValue::Text("box".into()),
                SqliteValue::Integer(42),
            ]]
        );
        assert_eq!(
            full_scan(&table)[1][3..],
            [SqliteValue::Null, SqliteValue::Null]
        );

        VirtualTable::update(&mut table, &Cx::new(), &[SqliteValue::Integer(1)]).unwrap();
        assert!(table.tree.rowid_row(1).is_none());
    }

    #[test]
    fn cursor_past_end_or_out_of_range_column_returns_null() {
        let mut table = table(&SPATIAL, RtreeCoordType::Float32);
        insert(
            &mut table,
            SqliteValue::Integer(1),
            &floats(&[0.0, 1.0, 0.0, 1.0]),
        )
        .unwrap();
        let cx = Cx::new();
        let mut cursor = table.open().unwrap();
        cursor.filter(&cx, RTREE_SCAN_TREE, None, &[]).unwrap();
        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, -1).unwrap();
        assert_eq!(ctx.take_value(), Some(SqliteValue::Null));
        cursor.next(&cx).unwrap();
        assert!(cursor.eof());
        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, 0).unwrap();
        assert_eq!(ctx.take_value(), Some(SqliteValue::Null));
        assert_eq!(cursor.rowid().unwrap(), 0);
    }

    #[test]
    fn best_index_matches_stock_strategies() {
        let table = table(&SPATIAL, RtreeCoordType::Float32);

        // A rowid (or id) equality is a direct lookup, whatever came before.
        let mut info = IndexInfo::new(
            vec![
                constraint(1, ConstraintOp::Le),
                constraint(0, ConstraintOp::Eq),
            ],
            Vec::new(),
        );
        VirtualTable::best_index(&table, &mut info).unwrap();
        assert_eq!(info.idx_num, RTREE_LOOKUP_ROWID);
        assert_eq!(info.constraint_usage[0].argv_index, 0);
        assert_eq!(info.constraint_usage[1].argv_index, 1);
        assert!(info.constraint_usage[1].omit);
        assert_eq!(info.estimated_rows, 1);

        // Coordinate constraints become operator/coordinate pairs; only LE,
        // GE and MATCH are omitted from the residual WHERE.
        let mut info = IndexInfo::new(
            vec![
                constraint(1, ConstraintOp::Le),
                constraint(2, ConstraintOp::Ge),
                constraint(3, ConstraintOp::Lt),
                constraint(4, ConstraintOp::Gt),
                constraint(3, ConstraintOp::Eq),
                constraint(0, ConstraintOp::Gt),
            ],
            Vec::new(),
        );
        VirtualTable::best_index(&table, &mut info).unwrap();
        assert_eq!(info.idx_num, RTREE_SCAN_TREE);
        assert_eq!(info.idx_str.as_deref(), Some("B0D1C2E3A2"));
        let usage: Vec<_> = info
            .constraint_usage
            .iter()
            .map(|usage| (usage.argv_index, usage.omit))
            .collect();
        assert_eq!(
            usage,
            vec![
                (1, true),
                (2, true),
                (3, false),
                (4, false),
                (5, false),
                (0, false)
            ]
        );
        assert_eq!(info.estimated_rows, RTREE_DEFAULT_ROWEST >> 5);

        // Any MATCH, usable or not, rules the rowid lookup out.
        let mut info = IndexInfo::new(
            vec![
                constraint(0, ConstraintOp::Eq),
                IndexConstraint {
                    column: 1,
                    op: ConstraintOp::Match,
                    usable: false,
                },
            ],
            Vec::new(),
        );
        VirtualTable::best_index(&table, &mut info).unwrap();
        assert_eq!(info.idx_num, RTREE_SCAN_TREE);
        assert_eq!(info.idx_str, None);
    }

    #[test]
    fn filter_converts_bounds_like_stock() {
        let mut table = table(&SPATIAL, RtreeCoordType::Float32);
        insert(
            &mut table,
            SqliteValue::Integer(10),
            &floats(&[0.0, 1.0, 0.0, 1.0]),
        )
        .unwrap();
        insert(
            &mut table,
            SqliteValue::Integer(20),
            &floats(&[4.0, 5.0, 4.0, 5.0]),
        )
        .unwrap();
        let ids = |rows: Vec<Vec<SqliteValue>>| -> Vec<SqliteValue> {
            rows.into_iter().map(|row| row[0].clone()).collect()
        };

        let bbox = floats(&[4.5, 3.5, 4.5, 3.5]);
        assert_eq!(
            ids(scan(&table, RTREE_SCAN_TREE, Some("B0D1B2D3"), &bbox)),
            vec![SqliteValue::Integer(20)]
        );
        // Numeric text is a number; other text passes `<`/`<=` and fails the
        // rest; NULL fails everything.
        assert_eq!(
            ids(scan(
                &table,
                RTREE_SCAN_TREE,
                Some("D0"),
                &[SqliteValue::Text("3".into())]
            )),
            vec![SqliteValue::Integer(20)]
        );
        assert_eq!(
            ids(scan(
                &table,
                RTREE_SCAN_TREE,
                Some("C0"),
                &[SqliteValue::Text("x".into())]
            ))
            .len(),
            2
        );
        assert!(
            scan(
                &table,
                RTREE_SCAN_TREE,
                Some("E0"),
                &[SqliteValue::Text("x".into())]
            )
            .is_empty()
        );
        assert!(scan(&table, RTREE_SCAN_TREE, Some("B0"), &[SqliteValue::Null]).is_empty());

        // Rowid lookups accept an integral float and nothing else.
        assert_eq!(
            ids(scan(
                &table,
                RTREE_LOOKUP_ROWID,
                None,
                &[SqliteValue::Float(20.0)]
            )),
            vec![SqliteValue::Integer(20)]
        );
        assert!(
            scan(
                &table,
                RTREE_LOOKUP_ROWID,
                None,
                &[SqliteValue::Float(20.5)]
            )
            .is_empty()
        );
        assert!(
            scan(
                &table,
                RTREE_LOOKUP_ROWID,
                None,
                &[SqliteValue::Integer(30)]
            )
            .is_empty()
        );
    }

    struct UpperRightGeometry;

    impl RtreeGeometry for UpperRightGeometry {
        fn query_func(&self, bbox: &[f64]) -> RtreeQueryResult {
            if bbox.len() >= 4 && bbox[1] >= 5.0 && bbox[3] >= 5.0 {
                RtreeQueryResult::Include
            } else {
                RtreeQueryResult::Exclude
            }
        }
    }

    #[test]
    fn match_runs_the_registered_geometry() {
        let mut table = table(&SPATIAL, RtreeCoordType::Float32);
        table.register_geometry("upper_right", Box::new(UpperRightGeometry));
        insert(
            &mut table,
            SqliteValue::Integer(1),
            &floats(&[0.0, 1.0, 0.0, 1.0]),
        )
        .unwrap();
        insert(
            &mut table,
            SqliteValue::Integer(2),
            &floats(&[6.0, 7.0, 6.0, 7.0]),
        )
        .unwrap();
        let rows = scan(
            &table,
            RTREE_SCAN_TREE,
            Some("F/"),
            &[SqliteValue::Text("upper_right".into())],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], SqliteValue::Integer(2));
        assert!(
            scan(
                &table,
                RTREE_SCAN_TREE,
                Some("F/"),
                &[SqliteValue::Text("unknown".into())]
            )
            .is_empty()
        );
    }

    #[test]
    fn rtree_i32_stores_truncated_integers() {
        let mut table = table(&SPATIAL, RtreeCoordType::Int32);
        insert(
            &mut table,
            SqliteValue::Integer(7),
            &[
                SqliteValue::Integer(1),
                SqliteValue::Float(3.9),
                SqliteValue::Integer(5),
                SqliteValue::Integer(9),
            ],
        )
        .unwrap();
        assert_eq!(
            full_scan(&table)[0],
            [7, 1, 3, 5, 9].map(SqliteValue::Integer).to_vec()
        );
    }

    /// Applies drained shadow writes to in-memory tables, as the engine does.
    fn apply(tables: &mut [BTreeMapRows; 3], writes: RtreeShadowWrites) {
        for (table, changes) in tables
            .iter_mut()
            .zip([writes.node, writes.rowid, writes.parent])
        {
            for (rowid, record) in changes {
                match record {
                    Some(record) => table.insert(rowid, record),
                    None => table.remove(&rowid),
                };
            }
        }
    }

    type BTreeMapRows = std::collections::BTreeMap<i64, Vec<SqliteValue>>;

    #[test]
    fn shadow_storage_writes_reload_to_the_same_tree() {
        let mut table = table(&SPATIAL, RtreeCoordType::Float32);
        assert!(!table.uses_shadow_storage());
        assert!(table.take_shadow_writes().is_empty());
        table.init_shadow_storage(4096);
        assert!(table.uses_shadow_storage());

        let mut stored: [BTreeMapRows; 3] = Default::default();
        let writes = table.take_shadow_writes();
        // xCreate writes an all-zero root node 1: 4 + 24 * 51 bytes in 2-D.
        assert_eq!(
            writes.node,
            vec![(
                1,
                Some(vec![
                    SqliteValue::Null,
                    SqliteValue::Blob(Arc::from(vec![0_u8; 1228]))
                ])
            )]
        );
        apply(&mut stored, writes);

        for id in 1..=300_i64 {
            let x = (id * 37 % 101) as f64;
            let y = (id * 53 % 97) as f64;
            insert(
                &mut table,
                SqliteValue::Integer(id),
                &floats(&[x, x + 1.5, y, y + 2.0]),
            )
            .unwrap();
            if id % 7 == 0 {
                VirtualTable::update(&mut table, &Cx::new(), &[SqliteValue::Integer(id / 2)])
                    .unwrap();
            }
            apply(&mut stored, table.take_shadow_writes());
        }
        assert!(table.tree.depth() >= 1, "300 entries must split the root");
        assert!(stored[2].len() > 1, "split nodes have parent rows");

        let rows = |table: &BTreeMapRows| -> Vec<(i64, Vec<SqliteValue>)> {
            table
                .iter()
                .map(|(rowid, values)| (*rowid, values.clone()))
                .collect()
        };
        let mut reloaded = super::tests::table(&SPATIAL, RtreeCoordType::Float32);
        reloaded
            .load_shadow_storage(&rows(&stored[0]), &rows(&stored[1]), &rows(&stored[2]))
            .unwrap();
        assert_eq!(full_scan(&reloaded), full_scan(&table));
        assert_eq!(reloaded.len(), table.len());

        // An undersized node blob is refused as stock refuses it.
        let short = vec![(
            1,
            vec![
                SqliteValue::Null,
                SqliteValue::Blob(Arc::from(vec![0_u8; 100])),
            ],
        )];
        let err = super::tests::table(&SPATIAL, RtreeCoordType::Float32)
            .load_shadow_storage(&short, &[], &[])
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("undersize RTree blobs in \"spatial_node\""),
            "{err}"
        );
    }

    #[test]
    fn shadow_table_definitions_match_stock_text() {
        let table = table(
            &["rtree", "main", "geo", "id", "x0", "x1", "+name", "+kind"],
            RtreeCoordType::Float32,
        );
        assert_eq!(
            table.shadow_table_definitions(),
            vec![
                (
                    "geo_rowid".to_owned(),
                    "CREATE TABLE \"geo_rowid\"(rowid INTEGER PRIMARY KEY,nodeno,a0,a1)".to_owned()
                ),
                (
                    "geo_node".to_owned(),
                    "CREATE TABLE \"geo_node\"(nodeno INTEGER PRIMARY KEY,data)".to_owned()
                ),
                (
                    "geo_parent".to_owned(),
                    "CREATE TABLE \"geo_parent\"(nodeno INTEGER PRIMARY KEY,parentnode)".to_owned()
                ),
            ]
        );
    }

    #[test]
    fn legacy_materialized_rows_rebuild_the_tree() {
        let mut table = table(&SPATIAL, RtreeCoordType::Float32);
        let rows = vec![
            (
                3,
                [vec![SqliteValue::Integer(3)], floats(&[0.0, 1.0, 0.0, 1.0])].concat(),
            ),
            (9, floats(&[2.0, 3.0, 2.0, 3.0])),
        ];
        table.rebuild_rows(&rows).unwrap();
        let ids: Vec<_> = full_scan(&table)
            .into_iter()
            .map(|row| row[0].clone())
            .collect();
        assert_eq!(ids, vec![SqliteValue::Integer(3), SqliteValue::Integer(9)]);
        assert!(table.take_shadow_writes().is_empty());

        let duplicate = vec![rows[1].clone(), rows[1].clone()];
        assert!(matches!(
            table.rebuild_rows(&duplicate),
            Err(FrankenError::DatabaseCorrupt { .. })
        ));
    }
}
