//! Ungrouped aggregates of one rowid-table scan on several threads
//! (bd-w4pr1.13.11).
//!
//! Codegen attaches a [`ParallelAggregatePlan`] to the program of
//! `SELECT agg, ... FROM t [WHERE ...]` when every result column is
//! `count(*)`, `count(col)`, `sum(col)`, `min(col)` or `max(col)` over a
//! column of the scanned rowid table, the WHERE is built from AND / OR of
//! column-versus-literal comparisons and `IS [NOT] NULL` tests, and the
//! program walks the table in rowid order. The bytecode is unchanged. At the
//! plan's `OpenRead` the engine may instead read the transaction's committed
//! snapshot on several threads ([`fsqlite_btree::snapshot_walk`]): each run of
//! subtrees folds its rows into its own accumulators, the runs are merged in
//! rowid order, and the engine jumps to the plan's `ResultRow` with the values
//! the serial loop would have produced there.
//!
//! What the threads cannot reproduce exactly declines the whole plan and the
//! serial loop runs as if there were none: a `sum` input that is not a stored
//! integer (REAL and coerced TEXT sums depend on addition order), a record the
//! threads cannot parse (the serial path then reports it), a database whose
//! text is not UTF-8.

use std::cmp::Ordering;
use std::sync::Arc;

use fsqlite_btree::cell::{self, BtreePageType, CellRef};
use fsqlite_btree::snapshot_walk::{LeafPage, ParallelWalk, WalkOutcome, split_subtrees};
use fsqlite_error::{FrankenError, Result};
use fsqlite_pager::{SnapshotPageBuf, SnapshotPageReader};
use fsqlite_types::record::{
    ColumnOffset, decode_column_from_offset_with_encoding, decode_scalar_value,
};
use fsqlite_types::value::SqliteValue;
use fsqlite_types::{PageNumber, TextEncoding};

use crate::engine::{
    RecordColumnTable, RecordColumns, SQLITE_AFF_MASK, SQLITE_AFF_TEXT, column_payload_end,
    compare_for_where, payload_rowid_alias_header_verdict, storage_payload_column_index,
};

/// A column a plan reads from each row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanColumn {
    /// The row's rowid.
    Rowid,
    /// Declared column `idx` of the table.
    Declared(usize),
}

/// One result column of the plan, reading the plan column at its index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParallelAggregate {
    CountStar,
    Count(usize),
    Sum(usize),
    Min(usize),
    Max(usize),
}

/// A comparison operator of the WHERE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CompareOp {
    fn holds(self, ord: Ordering) -> bool {
        match self {
            Self::Eq => ord == Ordering::Equal,
            Self::Ne => ord != Ordering::Equal,
            Self::Lt => ord == Ordering::Less,
            Self::Le => ord != Ordering::Greater,
            Self::Gt => ord == Ordering::Greater,
            Self::Ge => ord != Ordering::Less,
        }
    }
}

/// The WHERE of a plan, over plan columns.
#[derive(Debug, Clone, PartialEq)]
pub enum ScanPredicate {
    /// `column op literal`, or `literal op column` when `literal_left`,
    /// compared as the serial comparison opcode compares them (`p5` carries
    /// its affinity; the collation is BINARY).
    Compare {
        column: usize,
        op: CompareOp,
        literal: SqliteValue,
        literal_left: bool,
        p5: u16,
    },
    /// `column IS NULL`, or `IS NOT NULL` when `negated`.
    IsNull { column: usize, negated: bool },
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

/// What codegen proved about one ungrouped aggregate statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ParallelAggregatePlan {
    /// Cursor and root page of the program's `OpenRead` at `open_addr`.
    pub cursor: i32,
    pub root_page: i32,
    pub open_addr: usize,
    /// The `ResultRow` at `result_addr` emits `aggregates.len()` registers
    /// from `out_reg`.
    pub result_addr: usize,
    pub out_reg: i32,
    /// The columns the plan reads, each decoded at most once per row.
    pub columns: Vec<ScanColumn>,
    /// The result columns, in order.
    pub aggregates: Vec<ParallelAggregate>,
    pub filter: Option<ScanPredicate>,
}

/// How the scanned table stores its rows, as the storage cursor knows it.
#[derive(Debug, Clone)]
pub struct TableShape {
    pub ipk_col_idx: Option<usize>,
    pub table_column_count: Option<usize>,
    pub first_not_null_non_ipk_col: Option<usize>,
    pub record_slots: Option<Arc<[usize]>>,
    pub real_columns: Option<Arc<[bool]>>,
    /// Values of columns past a short record's end (ALTER TABLE ADD COLUMN).
    pub defaults: Vec<Option<SqliteValue>>,
}

/// Accumulator of one aggregate over one run of rows.
#[derive(Debug, Clone)]
enum AggState {
    Count(u64),
    /// Exact integer sum: whether any input was seen, the run's total and
    /// the highest and lowest of its running totals.
    Sum {
        seen: bool,
        total: i128,
        high: i128,
        low: i128,
    },
    Min(Option<SqliteValue>),
    Max(Option<SqliteValue>),
}

impl AggState {
    fn new(agg: ParallelAggregate) -> Self {
        match agg {
            ParallelAggregate::CountStar | ParallelAggregate::Count(_) => Self::Count(0),
            ParallelAggregate::Sum(_) => Self::Sum {
                seen: false,
                total: 0,
                high: i128::MIN,
                low: i128::MAX,
            },
            ParallelAggregate::Min(_) => Self::Min(None),
            ParallelAggregate::Max(_) => Self::Max(None),
        }
    }

    /// Fold `next`, the run right after this one, into this one.
    fn merge(&mut self, next: Self) {
        match (self, next) {
            (Self::Count(a), Self::Count(b)) => *a += b,
            (
                Self::Sum {
                    seen,
                    total,
                    high,
                    low,
                },
                Self::Sum {
                    seen: next_seen,
                    total: next_total,
                    high: next_high,
                    low: next_low,
                },
            ) => {
                if next_seen {
                    *high = (*high).max(*total + next_high);
                    *low = (*low).min(*total + next_low);
                    *total += next_total;
                    *seen = true;
                }
            }
            (Self::Min(best), Self::Min(next)) => {
                if let Some(next) = next
                    && best
                        .as_ref()
                        .is_none_or(|b| next.cmp_binary_in(b, TextEncoding::Utf8) == Ordering::Less)
                {
                    *best = Some(next);
                }
            }
            (Self::Max(best), Self::Max(next)) => {
                if let Some(next) = next
                    && best.as_ref().is_none_or(|b| {
                        next.cmp_binary_in(b, TextEncoding::Utf8) == Ordering::Greater
                    })
                {
                    *best = Some(next);
                }
            }
            _ => unreachable!("runs of one plan hold the same aggregates"),
        }
    }

    /// The value `AggFinal` would have produced for these rows.
    fn finish(self) -> Result<SqliteValue> {
        Ok(match self {
            Self::Count(n) => SqliteValue::Integer(i64::try_from(n).unwrap_or(i64::MAX)),
            Self::Sum {
                seen,
                total,
                high,
                low,
            } => {
                if !seen {
                    SqliteValue::Null
                } else if high > i128::from(i64::MAX) || low < i128::from(i64::MIN) {
                    // A running total left the integer range: the serial sum
                    // overflowed there and, every input being an integer,
                    // reports it at finalization.
                    return Err(FrankenError::IntegerOverflow);
                } else {
                    SqliteValue::Integer(i64::try_from(total).map_err(|_| FrankenError::IntegerOverflow)?)
                }
            }
            Self::Min(best) | Self::Max(best) => best.unwrap_or(SqliteValue::Null),
        })
    }
}

/// One plan column of the current row, as the serial `Column` opcode would
/// have decoded it. A NULL, INTEGER or REAL is held in the cell itself, so a
/// row's cells reset without running any destructor; a TEXT, BLOB or other
/// value sits in the row's wide slot for that column.
#[derive(Debug, Clone, Copy)]
enum ColumnCell {
    Null,
    Int(i64),
    Float(f64),
    /// The value is in the column's wide slot.
    Wide,
}

/// One thread's scratch: the current record's column table, its decoded
/// plan columns (`cells`, with the TEXT and BLOB values in `wide`), and room
/// to reassemble a payload that spills onto overflow pages.
struct RowScratch {
    layout: RecordColumnTable,
    cells: Vec<Option<ColumnCell>>,
    wide: Vec<SqliteValue>,
    includes_rowid_alias: Option<bool>,
    payload: Vec<u8>,
    overflow: SnapshotPageBuf,
}

/// Run `plan` over the table b-tree rooted at `root` on `workers` threads.
/// `Ok(None)`: the plan declined or `stop` stopped it; run the serial loop.
pub fn run_parallel_aggregate(
    plan: &ParallelAggregatePlan,
    shape: &TableShape,
    reader: &SnapshotPageReader,
    root: PageNumber,
    usable_size: u32,
    workers: usize,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<Option<Vec<SqliteValue>>> {
    let split = split_subtrees(reader, root, usable_size, workers.saturating_mul(4))?;
    let walk = ParallelWalk {
        reader,
        usable_size,
        workers,
        stop,
    };
    let outcome = walk.run(
        &split,
        || RowScratch {
            layout: RecordColumnTable::default(),
            cells: vec![None; plan.columns.len()],
            wide: vec![SqliteValue::Null; plan.columns.len()],
            includes_rowid_alias: None,
            payload: Vec::new(),
            overflow: reader.new_buffer(),
        },
        || plan.aggregates.iter().map(|&agg| AggState::new(agg)).collect::<Vec<_>>(),
        |scratch, states, leaf| scan_leaf(plan, shape, reader, usable_size, scratch, states, leaf),
    )?;
    let WalkOutcome::Done(runs) = outcome else {
        return Ok(None);
    };
    let mut runs = runs.into_iter().map(|(states, _)| states);
    let Some(mut total) = runs.next() else {
        return Ok(None);
    };
    for run in runs {
        for (state, next) in total.iter_mut().zip(run) {
            state.merge(next);
        }
    }
    total
        .into_iter()
        .map(AggState::finish)
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

/// Fold the rows of one table leaf into `states`. `Ok(false)` declines.
fn scan_leaf(
    plan: &ParallelAggregatePlan,
    shape: &TableShape,
    reader: &SnapshotPageReader,
    usable_size: u32,
    scratch: &mut RowScratch,
    states: &mut [AggState],
    leaf: LeafPage<'_>,
) -> Result<bool> {
    if leaf.header.page_type != BtreePageType::LeafTable {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!("page {} is not a table leaf", leaf.page_no.get()),
        });
    }
    let page = leaf.bytes;
    let pointers = cell::header_offset_for_page(leaf.page_no)
        + usize::from(BtreePageType::LeafTable.header_size());
    for idx in 0..usize::from(leaf.header.cell_count) {
        let at = pointers + idx * 2;
        let offset = page
            .get(at..at + 2)
            .map(|b| usize::from(u16::from_be_bytes([b[0], b[1]])))
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: format!("cell pointer {idx} of page {} is past the page", leaf.page_no.get()),
            })?;
        let cell = CellRef::parse(page, offset, BtreePageType::LeafTable, usable_size)?;
        let rowid = cell.rowid.ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: "table leaf cell without a rowid".to_owned(),
        })?;
        let RowScratch {
            layout,
            cells,
            wide,
            includes_rowid_alias,
            payload,
            overflow,
        } = scratch;
        let record: &[u8] = match cell.overflow_page {
            None => cell.local_payload(page),
            Some(first) => {
                reassemble_payload(reader, usable_size, &cell, page, first, payload, overflow)?;
                payload.as_slice()
            }
        };
        if !layout.parse(record) {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("malformed record header in row {rowid}"),
            });
        }
        cells.fill(None);
        *includes_rowid_alias = None;
        let mut row = Row {
            plan,
            shape,
            rowid,
            record,
            layout,
            cells,
            wide,
            includes_rowid_alias,
        };
        if let Some(filter) = &plan.filter
            && row.eval(filter)? != Some(true)
        {
            continue;
        }
        for (state, agg) in states.iter_mut().zip(&plan.aggregates) {
            match (state, *agg) {
                (AggState::Count(n), ParallelAggregate::CountStar) => *n += 1,
                (AggState::Count(n), ParallelAggregate::Count(column)) => {
                    if !matches!(row.cell(column)?, ColumnCell::Null) {
                        *n += 1;
                    }
                }
                (
                    AggState::Sum {
                        seen,
                        total,
                        high,
                        low,
                    },
                    ParallelAggregate::Sum(column),
                ) => match row.cell(column)? {
                    ColumnCell::Null => {}
                    ColumnCell::Int(v) => {
                        *seen = true;
                        *total += i128::from(v);
                        *high = (*high).max(*total);
                        *low = (*low).min(*total);
                    }
                    // A REAL, or a TEXT or BLOB that sum() coerces: the
                    // serial sum depends on the order of its additions.
                    ColumnCell::Float(_) | ColumnCell::Wide => return Ok(false),
                },
                (AggState::Min(best), ParallelAggregate::Min(column)) => {
                    let cell = row.cell(column)?;
                    // Two integers order as cmp_binary_in orders them.
                    if let ColumnCell::Int(v) = cell
                        && let Some(SqliteValue::Integer(b)) = best
                    {
                        if v < *b {
                            *b = v;
                        }
                    } else if !matches!(cell, ColumnCell::Null) {
                        row.with_value(column, cell, |value| {
                            if best.as_ref().is_none_or(|b| {
                                value.cmp_binary_in(b, TextEncoding::Utf8) == Ordering::Less
                            }) {
                                *best = Some(value.clone());
                            }
                        });
                    }
                }
                (AggState::Max(best), ParallelAggregate::Max(column)) => {
                    let cell = row.cell(column)?;
                    if let ColumnCell::Int(v) = cell
                        && let Some(SqliteValue::Integer(b)) = best
                    {
                        if v > *b {
                            *b = v;
                        }
                    } else if !matches!(cell, ColumnCell::Null) {
                        row.with_value(column, cell, |value| {
                            if best.as_ref().is_none_or(|b| {
                                value.cmp_binary_in(b, TextEncoding::Utf8) == Ordering::Greater
                            }) {
                                *best = Some(value.clone());
                            }
                        });
                    }
                }
                _ => unreachable!("states follow the plan's aggregates"),
            }
        }
    }
    Ok(true)
}

/// Copy a payload that continues on overflow pages into `payload`.
fn reassemble_payload(
    reader: &SnapshotPageReader,
    usable_size: u32,
    cell: &CellRef,
    page: &[u8],
    first: PageNumber,
    payload: &mut Vec<u8>,
    overflow: &mut SnapshotPageBuf,
) -> Result<()> {
    let total = usize::try_from(cell.payload_size).unwrap_or(usize::MAX);
    let per_page = usize::try_from(usable_size).unwrap_or(0).saturating_sub(4);
    if per_page == 0 {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!("usable size {usable_size} holds no overflow content"),
        });
    }
    payload.clear();
    payload.extend_from_slice(cell.local_payload(page));
    let mut next = Some(first);
    while payload.len() < total {
        let Some(page_no) = next else {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "overflow chain ends before the payload".to_owned(),
            });
        };
        let bytes = cell::usable_prefix(reader.read_page(page_no, overflow)?, usable_size);
        if bytes.len() < 4 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("overflow page {} is too short", page_no.get()),
            });
        }
        next = PageNumber::new(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]));
        let take = (total - payload.len()).min(per_page).min(bytes.len() - 4);
        if take == 0 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("overflow page {} holds no content", page_no.get()),
            });
        }
        payload.extend_from_slice(&bytes[4..4 + take]);
    }
    Ok(())
}

/// One row being folded: its record and the plan columns decoded so far.
struct Row<'a> {
    plan: &'a ParallelAggregatePlan,
    shape: &'a TableShape,
    rowid: i64,
    record: &'a [u8],
    layout: &'a RecordColumnTable,
    cells: &'a mut [Option<ColumnCell>],
    wide: &'a mut [SqliteValue],
    includes_rowid_alias: &'a mut Option<bool>,
}

impl Row<'_> {
    /// Plan column `column`, decoded as the serial `Column` opcode decodes it.
    fn cell(&mut self, column: usize) -> Result<ColumnCell> {
        if let Some(cell) = self.cells[column] {
            return Ok(cell);
        }
        let cell = self.decode(column)?;
        self.cells[column] = Some(cell);
        Ok(cell)
    }

    /// `f` of plan column `column`'s value, `cell` being its cell: a scalar's
    /// value is built here, a wide one's is borrowed from its slot.
    fn with_value<R>(&self, column: usize, cell: ColumnCell, f: impl FnOnce(&SqliteValue) -> R) -> R {
        match cell {
            ColumnCell::Null => f(&SqliteValue::Null),
            ColumnCell::Int(v) => f(&SqliteValue::Integer(v)),
            ColumnCell::Float(v) => f(&SqliteValue::Float(v)),
            ColumnCell::Wide => f(&self.wide[column]),
        }
    }

    /// The cell of `value` for plan column `column`; a TEXT or BLOB goes to
    /// the column's wide slot.
    fn hold(&mut self, column: usize, value: SqliteValue) -> ColumnCell {
        match value {
            SqliteValue::Null => ColumnCell::Null,
            SqliteValue::Integer(v) => ColumnCell::Int(v),
            SqliteValue::Float(v) => ColumnCell::Float(v),
            wide => {
                self.wide[column] = wide;
                ColumnCell::Wide
            }
        }
    }

    fn decode(&mut self, column: usize) -> Result<ColumnCell> {
        let idx = match self.plan.columns[column] {
            ScanColumn::Rowid => return Ok(ColumnCell::Int(self.rowid)),
            ScanColumn::Declared(idx) => idx,
        };
        let shape = self.shape;
        if shape.ipk_col_idx == Some(idx) {
            return Ok(ColumnCell::Int(self.rowid));
        }
        let includes_alias = match shape.ipk_col_idx {
            None => false,
            Some(ipk) => {
                if let Some(known) = *self.includes_rowid_alias {
                    known
                } else {
                    let (mut includes, slot) = payload_rowid_alias_header_verdict(
                        self.layout,
                        ipk,
                        shape.table_column_count,
                        shape.first_not_null_non_ipk_col,
                    );
                    if let Some(slot) = slot {
                        // A short record whose slot holds an integer stores
                        // the alias exactly when that integer is the rowid.
                        let bytes = column_bytes(self.record, &slot, self.rowid)?;
                        includes = matches!(
                            decode_scalar_value(slot.serial_type, bytes),
                            Some(SqliteValue::Integer(v)) if v == self.rowid
                        );
                    }
                    *self.includes_rowid_alias = Some(includes);
                    includes
                }
            }
        };
        let payload_idx = storage_payload_column_index(
            idx,
            shape.ipk_col_idx,
            includes_alias,
            shape.record_slots.as_deref(),
        );
        let Some(col) = self.layout.column_offset(payload_idx).copied() else {
            // Past the record's end: the value ALTER TABLE ADD COLUMN gave it.
            let value = shape.defaults.get(idx).cloned().flatten();
            return Ok(self.hold(column, value.unwrap_or(SqliteValue::Null)));
        };
        let bytes = column_bytes(self.record, &col, self.rowid)?;
        Ok(match decode_scalar_value(col.serial_type, bytes) {
            Some(SqliteValue::Integer(stored)) => {
                if shape
                    .real_columns
                    .as_deref()
                    .is_some_and(|real| real.get(idx).copied().unwrap_or(false))
                {
                    ColumnCell::Float(stored as f64)
                } else {
                    ColumnCell::Int(stored)
                }
            }
            Some(value) => self.hold(column, value),
            None => {
                let value = decode_column_from_offset_with_encoding(
                    self.record,
                    &col,
                    TextEncoding::Utf8,
                    false,
                )
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: format!("failed to decode column {payload_idx} of row {}", self.rowid),
                })?;
                self.hold(column, value)
            }
        })
    }

    /// The WHERE's truth value for this row: `None` is SQL's unknown.
    fn eval(&mut self, predicate: &ScanPredicate) -> Result<Option<bool>> {
        Ok(match predicate {
            ScanPredicate::Compare {
                column,
                op,
                literal,
                literal_left,
                p5,
            } => {
                let cell = self.cell(*column)?;
                match (cell, literal) {
                    (ColumnCell::Null, _) | (_, SqliteValue::Null) => None,
                    // Two integers, as compare_for_where orders them unless
                    // the comparison's affinity is TEXT.
                    (ColumnCell::Int(v), SqliteValue::Integer(l))
                        if *p5 & SQLITE_AFF_MASK != SQLITE_AFF_TEXT =>
                    {
                        Some(op.holds(if *literal_left { l.cmp(&v) } else { v.cmp(l) }))
                    }
                    _ => self.with_value(*column, cell, |value| {
                        let ord = if *literal_left {
                            compare_for_where(literal, value, *p5, TextEncoding::Utf8)
                        } else {
                            compare_for_where(value, literal, *p5, TextEncoding::Utf8)
                        };
                        ord.map(|ord| op.holds(ord))
                    }),
                }
            }
            ScanPredicate::IsNull { column, negated } => {
                Some(matches!(self.cell(*column)?, ColumnCell::Null) != *negated)
            }
            ScanPredicate::And(left, right) => match self.eval(left)? {
                Some(false) => Some(false),
                left => match (left, self.eval(right)?) {
                    (_, Some(false)) => Some(false),
                    (Some(true), Some(true)) => Some(true),
                    _ => None,
                },
            },
            ScanPredicate::Or(left, right) => match self.eval(left)? {
                Some(true) => Some(true),
                left => match (left, self.eval(right)?) {
                    (_, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    _ => None,
                },
            },
        })
    }
}

/// The bytes of column `col` of `record`, which must hold them.
fn column_bytes<'r>(record: &'r [u8], col: &ColumnOffset, rowid: i64) -> Result<&'r [u8]> {
    usize::try_from(col.body_offset)
        .ok()
        .zip(column_payload_end(col))
        .and_then(|(start, end)| record.get(start..end))
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: format!("record of row {rowid} is shorter than its header"),
        })
}
