//! §5.10.2.1 Index Regeneration on Rebase — bd-zj56
//!
//! During deterministic rebase, `IndexDelete`/`IndexInsert` ops associated with
//! an `UpdateExpression` (same table, same rowid) carry stale key bytes from
//! the original snapshot. This module discards those stale ops and regenerates
//! correct index operations from the schema and rebased row images.
//!
//! The 7-step algorithm (spec lines 10240-10272):
//! 1. Discard stale index ops for the rebased (table, rowid)
//! 2. Enumerate secondary indexes from schema
//! 3. Compute participation for base and updated rows
//! 4. Compute index key bytes (affinity + collation)
//! 5. Emit index ops based on participation delta
//! 6. UNIQUE enforcement against committed base snapshot
//! 7. Overflow page chain management (delegated to B-tree layer)

use fsqlite_types::TypeAffinity;
use fsqlite_types::glossary::{ColumnIdx, IndexId, IntentOpKind, RebaseExpr, RowId, TableId};
use fsqlite_types::record::{parse_record, serialize_record};
use fsqlite_types::value::SqliteValue;

/// Bead identifier for tracing.
const BEAD_ID: &str = "bd-zj56";

// ── Error types ──────────────────────────────────────────────────────────────

/// Errors that can occur during index regeneration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexRegenError {
    /// UNIQUE constraint violation: a conflicting key exists for a different rowid.
    UniqueConstraintViolation {
        index_id: IndexId,
        conflicting_rowid: RowId,
    },
    /// The record bytes could not be parsed.
    MalformedRecord,
    /// A column index in an index definition is out of bounds.
    ColumnOutOfBounds {
        column_idx: u32,
        record_columns: usize,
    },
    /// A `RebaseExpr` referenced a column that doesn't exist.
    ExprEvalError { detail: &'static str },
}

impl std::fmt::Display for IndexRegenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UniqueConstraintViolation {
                index_id,
                conflicting_rowid,
            } => write!(
                f,
                "UNIQUE constraint failed: index {} has conflicting rowid {}",
                index_id.get(),
                conflicting_rowid.get()
            ),
            Self::MalformedRecord => write!(f, "malformed record bytes"),
            Self::ColumnOutOfBounds {
                column_idx,
                record_columns,
            } => write!(
                f,
                "column index {column_idx} out of bounds (record has {record_columns} columns)"
            ),
            Self::ExprEvalError { detail } => write!(f, "expression eval error: {detail}"),
        }
    }
}

impl std::error::Error for IndexRegenError {}

// ── Schema types (lightweight, for rebase context) ───────────────────────────

/// Collation ordering for index key comparison.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum Collation {
    /// BINARY: raw byte comparison (memcmp).
    #[default]
    Binary,
    /// NOCASE: case-insensitive comparison for ASCII range.
    Nocase,
    /// RTRIM: like BINARY but trailing spaces ignored.
    Rtrim,
}

/// An indexed column definition within a secondary index.
#[derive(Debug, Clone)]
pub enum IndexKeyPart {
    /// An ordinary column reference by position.
    Column {
        col_idx: ColumnIdx,
        affinity: TypeAffinity,
        collation: Collation,
    },
    /// An expression index (e.g., `CREATE INDEX ... ON t(expr)`).
    Expression {
        expr: RebaseExpr,
        affinity: TypeAffinity,
        collation: Collation,
    },
}

/// Schema definition for a secondary index, sufficient for rebase-time
/// index regeneration.
#[derive(Debug, Clone)]
pub struct IndexDef {
    /// The index identifier.
    pub index_id: IndexId,
    /// The table this index belongs to.
    pub table_id: TableId,
    /// Whether this is a UNIQUE index.
    pub unique: bool,
    /// The key columns/expressions of the index, in order.
    pub key_parts: Vec<IndexKeyPart>,
    /// Optional partial index WHERE predicate (only rows satisfying this
    /// participate in the index). `None` means a full index.
    pub where_predicate: Option<RebaseExpr>,
    /// Column affinities for the table's columns (used for expression eval).
    pub table_column_affinities: Vec<TypeAffinity>,
}

/// Trait for checking UNIQUE constraint violations against the committed base
/// snapshot during rebase.
pub trait UniqueChecker {
    /// Check if `key_bytes` already exists in the given index for a rowid
    /// different from `exclude_rowid`.
    ///
    /// Returns `Some(conflicting_rowid)` if a conflict exists, `None` otherwise.
    fn check_unique(
        &self,
        index_id: IndexId,
        key_bytes: &[u8],
        exclude_rowid: RowId,
    ) -> Option<RowId>;
}

/// A no-op unique checker that never finds conflicts (for non-UNIQUE indexes
/// or testing).
pub struct NoOpUniqueChecker;

impl UniqueChecker for NoOpUniqueChecker {
    fn check_unique(
        &self,
        _index_id: IndexId,
        _key_bytes: &[u8],
        _exclude_rowid: RowId,
    ) -> Option<RowId> {
        None
    }
}

// ── Expression evaluation ────────────────────────────────────────────────────

/// Evaluate a `RebaseExpr` against a row of `SqliteValue`s.
///
/// `ColumnRef(idx)` looks up the value at position `idx.get()` in `row`.
pub fn eval_rebase_expr(
    expr: &RebaseExpr,
    row: &[SqliteValue],
) -> Result<SqliteValue, IndexRegenError> {
    match expr {
        RebaseExpr::ColumnRef(idx) => {
            let i = idx.get() as usize;
            row.get(i)
                .cloned()
                .ok_or_else(|| IndexRegenError::ColumnOutOfBounds {
                    column_idx: idx.get(),
                    record_columns: row.len(),
                })
        }
        RebaseExpr::Literal(val) => Ok(val.clone()),
        RebaseExpr::UnaryOp { op, operand } => {
            let v = eval_rebase_expr(operand, row)?;
            Ok(eval_unary_op(*op, v))
        }
        RebaseExpr::BinaryOp { op, left, right } => {
            let l = eval_rebase_expr(left, row)?;
            let r = eval_rebase_expr(right, row)?;
            Ok(eval_binary_op(*op, l, r))
        }
        RebaseExpr::FunctionCall { name, args } => {
            let evaluated: Result<Vec<_>, _> =
                args.iter().map(|a| eval_rebase_expr(a, row)).collect();
            eval_function(name, &evaluated?)
        }
        RebaseExpr::Cast { expr, type_name } => {
            let v = eval_rebase_expr(expr, row)?;
            let affinity = TypeAffinity::from_type_name(type_name);
            Ok(v.apply_affinity(affinity))
        }
        RebaseExpr::Case {
            operand,
            when_clauses,
            else_clause,
        } => eval_case(
            operand.as_deref(),
            when_clauses,
            else_clause.as_deref(),
            row,
        ),
        RebaseExpr::Coalesce(exprs) => {
            for e in exprs {
                let v = eval_rebase_expr(e, row)?;
                if !matches!(v, SqliteValue::Null) {
                    return Ok(v);
                }
            }
            Ok(SqliteValue::Null)
        }
        RebaseExpr::NullIf { left, right } => {
            let l = eval_rebase_expr(left, row)?;
            let r = eval_rebase_expr(right, row)?;
            if sqlite_values_equal(&l, &r) {
                Ok(SqliteValue::Null)
            } else {
                Ok(l)
            }
        }
        RebaseExpr::Concat { left, right } => {
            let l = eval_rebase_expr(left, row)?;
            let r = eval_rebase_expr(right, row)?;
            Ok(sqlite_concat(&l, &r))
        }
    }
}

/// Evaluate a CASE expression.
fn eval_case(
    operand: Option<&RebaseExpr>,
    when_clauses: &[(RebaseExpr, RebaseExpr)],
    else_clause: Option<&RebaseExpr>,
    row: &[SqliteValue],
) -> Result<SqliteValue, IndexRegenError> {
    if let Some(op_expr) = operand {
        // Simple CASE: compare operand to each WHEN value.
        let op_val = eval_rebase_expr(op_expr, row)?;
        for (when_expr, then_expr) in when_clauses {
            let when_val = eval_rebase_expr(when_expr, row)?;
            if sqlite_values_equal(&op_val, &when_val) {
                return eval_rebase_expr(then_expr, row);
            }
        }
    } else {
        // Searched CASE: evaluate each WHEN as a boolean.
        for (when_expr, then_expr) in when_clauses {
            let when_val = eval_rebase_expr(when_expr, row)?;
            if sqlite_value_is_truthy(&when_val) {
                return eval_rebase_expr(then_expr, row);
            }
        }
    }
    if let Some(else_expr) = else_clause {
        eval_rebase_expr(else_expr, row)
    } else {
        Ok(SqliteValue::Null)
    }
}

/// Check if two `SqliteValue`s are equal using SQLite semantics (NULL != NULL).
/// SQLite equality uses exact comparison for floats (no epsilon), and NULL != NULL.
#[allow(clippy::float_cmp)]
fn sqlite_values_equal(a: &SqliteValue, b: &SqliteValue) -> bool {
    match (a, b) {
        (SqliteValue::Integer(x), SqliteValue::Integer(y)) => x == y,
        (SqliteValue::Float(x), SqliteValue::Float(y)) => x == y,
        #[allow(clippy::cast_precision_loss)]
        (SqliteValue::Integer(x), SqliteValue::Float(y))
        | (SqliteValue::Float(y), SqliteValue::Integer(x)) => *x as f64 == *y,
        (SqliteValue::Text(x), SqliteValue::Text(y)) => x == y,
        (SqliteValue::Blob(x), SqliteValue::Blob(y)) => x == y,
        // NULL != anything (including NULL), and different type groups are not equal.
        _ => false,
    }
}

/// SQLite truthiness: non-zero integer or non-zero float is true.
fn sqlite_value_is_truthy(v: &SqliteValue) -> bool {
    match v {
        SqliteValue::Integer(i) => *i != 0,
        SqliteValue::Float(f) => *f != 0.0,
        SqliteValue::Null => false,
        other => match other.clone().apply_affinity(TypeAffinity::Numeric) {
            SqliteValue::Integer(i) => i != 0,
            SqliteValue::Float(f) => f != 0.0,
            _ => false,
        },
    }
}

/// Concatenate two values as text (SQLite `||` operator).
fn sqlite_concat(a: &SqliteValue, b: &SqliteValue) -> SqliteValue {
    if matches!(a, SqliteValue::Null) || matches!(b, SqliteValue::Null) {
        return SqliteValue::Null;
    }
    let sa = a.to_text();
    let sb = b.to_text();
    SqliteValue::Text(format!("{sa}{sb}"))
}

use fsqlite_types::glossary::{RebaseBinaryOp, RebaseUnaryOp};

/// Evaluate a unary operation.
fn eval_unary_op(op: RebaseUnaryOp, v: SqliteValue) -> SqliteValue {
    match op {
        RebaseUnaryOp::Negate => {
            if matches!(v, SqliteValue::Null) {
                return SqliteValue::Null;
            }
            match v.apply_affinity(TypeAffinity::Numeric) {
                SqliteValue::Integer(i) => match i.checked_neg() {
                    Some(val) => SqliteValue::Integer(val),
                    None => SqliteValue::Float(-(i as f64)),
                },
                SqliteValue::Float(f) => SqliteValue::Float(-f),
                _ => SqliteValue::Integer(0),
            }
        }
        RebaseUnaryOp::BitwiseNot => match v {
            SqliteValue::Integer(i) => SqliteValue::Integer(!i),
            SqliteValue::Null => SqliteValue::Null,
            other => {
                let coerced = other.apply_affinity(TypeAffinity::Integer);
                if let SqliteValue::Integer(i) = coerced {
                    SqliteValue::Integer(!i)
                } else {
                    SqliteValue::Integer(!0)
                }
            }
        },
        RebaseUnaryOp::Not => {
            if matches!(v, SqliteValue::Null) {
                SqliteValue::Null
            } else {
                SqliteValue::Integer(i64::from(!sqlite_value_is_truthy(&v)))
            }
        }
    }
}

/// Evaluate a binary arithmetic/bitwise operation.
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn eval_binary_op(op: RebaseBinaryOp, left: SqliteValue, right: SqliteValue) -> SqliteValue {
    // NULL propagation.
    if matches!(left, SqliteValue::Null) || matches!(right, SqliteValue::Null) {
        return SqliteValue::Null;
    }

    // Coerce to numeric for arithmetic.
    let left = left.apply_affinity(TypeAffinity::Numeric);
    let right = right.apply_affinity(TypeAffinity::Numeric);

    match op {
        RebaseBinaryOp::Add => left.sql_add(&right),
        RebaseBinaryOp::Subtract => left.sql_sub(&right),
        RebaseBinaryOp::Multiply => left.sql_mul(&right),
        RebaseBinaryOp::Divide => numeric_div(&left, &right),
        RebaseBinaryOp::Remainder => numeric_rem(&left, &right),
        RebaseBinaryOp::BitwiseAnd => integer_bitop(&left, &right, |a, b| a & b),
        RebaseBinaryOp::BitwiseOr => integer_bitop(&left, &right, |a, b| a | b),
        #[allow(clippy::cast_sign_loss)]
        RebaseBinaryOp::ShiftLeft => integer_bitop(&left, &right, |a, b| {
            let shift = b.unsigned_abs() as u32;
            if shift >= 64 {
                0
            } else if b < 0 {
                a >> shift
            } else {
                a << shift
            }
        }),
        #[allow(clippy::cast_sign_loss)]
        RebaseBinaryOp::ShiftRight => integer_bitop(&left, &right, |a, b| {
            let shift = b.unsigned_abs() as u32;
            if shift >= 64 {
                0
            } else if b < 0 {
                a << shift
            } else {
                a >> shift
            }
        }),
    }
}

fn numeric_div(l: &SqliteValue, r: &SqliteValue) -> SqliteValue {
    if let (SqliteValue::Integer(a), SqliteValue::Integer(b)) = (l, r) {
        if *b == 0 {
            return SqliteValue::Null;
        }
        SqliteValue::Integer(a.wrapping_div(*b))
    } else {
        let fb = r.to_float();
        if fb == 0.0 {
            return SqliteValue::Null;
        }
        SqliteValue::from(l.to_float() / fb)
    }
}

fn numeric_rem(l: &SqliteValue, r: &SqliteValue) -> SqliteValue {
    if let (SqliteValue::Integer(a), SqliteValue::Integer(b)) = (l, r) {
        if *b == 0 {
            return SqliteValue::Null;
        }
        SqliteValue::Integer(a.wrapping_rem(*b))
    } else {
        let fb = r.to_float();
        if fb == 0.0 {
            return SqliteValue::Null;
        }
        SqliteValue::from(l.to_float() % fb)
    }
}

fn integer_bitop(l: &SqliteValue, r: &SqliteValue, f: impl FnOnce(i64, i64) -> i64) -> SqliteValue {
    SqliteValue::Integer(f(l.to_integer(), r.to_integer()))
}

/// Evaluate a built-in function. Only a limited set is supported for rebase.
#[allow(clippy::too_many_lines)]
fn eval_function(name: &str, args: &[SqliteValue]) -> Result<SqliteValue, IndexRegenError> {
    match name.to_ascii_lowercase().as_str() {
        "abs" => {
            if let Some(v) = args.first() {
                if matches!(v, SqliteValue::Null) {
                    return Ok(SqliteValue::Null);
                }
                let coerced = v.clone().apply_affinity(TypeAffinity::Numeric);
                Ok(match coerced {
                    SqliteValue::Integer(i) => match i.checked_abs() {
                        Some(val) => SqliteValue::Integer(val),
                        None => SqliteValue::Float((i as f64).abs()),
                    },
                    SqliteValue::Float(f) => SqliteValue::Float(f.abs()),
                    _ => SqliteValue::Integer(0),
                })
            } else {
                Ok(SqliteValue::Null)
            }
        }
        "lower" => {
            if let Some(SqliteValue::Text(s)) = args.first() {
                Ok(SqliteValue::Text(s.to_ascii_lowercase()))
            } else {
                Ok(args.first().cloned().unwrap_or(SqliteValue::Null))
            }
        }
        "upper" => {
            if let Some(SqliteValue::Text(s)) = args.first() {
                Ok(SqliteValue::Text(s.to_ascii_uppercase()))
            } else {
                Ok(args.first().cloned().unwrap_or(SqliteValue::Null))
            }
        }
        "length" =>
        {
            #[allow(clippy::cast_possible_wrap)]
            if let Some(v) = args.first() {
                Ok(match v {
                    SqliteValue::Null => SqliteValue::Null,
                    SqliteValue::Text(s) => SqliteValue::Integer(s.chars().count() as i64),
                    SqliteValue::Blob(b) => SqliteValue::Integer(b.len() as i64),
                    SqliteValue::Integer(n) => SqliteValue::Integer(n.to_string().len() as i64),
                    SqliteValue::Float(f) => SqliteValue::Integer(f.to_string().len() as i64),
                })
            } else {
                Ok(SqliteValue::Null)
            }
        }
        "typeof" => {
            if let Some(v) = args.first() {
                Ok(SqliteValue::Text(
                    match v {
                        SqliteValue::Null => "null",
                        SqliteValue::Integer(_) => "integer",
                        SqliteValue::Float(_) => "real",
                        SqliteValue::Text(_) => "text",
                        SqliteValue::Blob(_) => "blob",
                    }
                    .to_owned(),
                ))
            } else {
                Ok(SqliteValue::Null)
            }
        }
        "ifnull" => {
            if args.len() >= 2 {
                if matches!(args[0], SqliteValue::Null) {
                    Ok(args[1].clone())
                } else {
                    Ok(args[0].clone())
                }
            } else {
                Ok(SqliteValue::Null)
            }
        }
        "max" => {
            let mut best: Option<&SqliteValue> = None;
            for a in args {
                if matches!(a, SqliteValue::Null) {
                    continue;
                }
                if let Some(cur) = best {
                    if sqlite_value_compare(a, cur) == std::cmp::Ordering::Greater {
                        best = Some(a);
                    }
                } else {
                    best = Some(a);
                }
            }
            Ok(best.cloned().unwrap_or(SqliteValue::Null))
        }
        "min" => {
            let mut best: Option<&SqliteValue> = None;
            for a in args {
                if matches!(a, SqliteValue::Null) {
                    continue;
                }
                if let Some(cur) = best {
                    if sqlite_value_compare(a, cur) == std::cmp::Ordering::Less {
                        best = Some(a);
                    }
                } else {
                    best = Some(a);
                }
            }
            Ok(best.cloned().unwrap_or(SqliteValue::Null))
        }
        _ => Err(IndexRegenError::ExprEvalError {
            detail: "unsupported function in rebase expression",
        }),
    }
}

/// Compare two non-NULL `SqliteValue`s using SQLite ordering rules.
fn sqlite_value_compare(a: &SqliteValue, b: &SqliteValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    // SQLite sort order: NULL < INTEGER/REAL < TEXT < BLOB
    fn type_order(v: &SqliteValue) -> u8 {
        match v {
            SqliteValue::Null => 0,
            SqliteValue::Integer(_) | SqliteValue::Float(_) => 1,
            SqliteValue::Text(_) => 2,
            SqliteValue::Blob(_) => 3,
        }
    }

    let ta = type_order(a);
    let tb = type_order(b);
    if ta != tb {
        return ta.cmp(&tb);
    }

    match (a, b) {
        (SqliteValue::Integer(x), SqliteValue::Integer(y)) => x.cmp(y),
        (SqliteValue::Float(x), SqliteValue::Float(y)) => x.total_cmp(y),
        #[allow(clippy::cast_precision_loss)]
        (SqliteValue::Integer(x), SqliteValue::Float(y)) => (*x as f64).total_cmp(y),
        #[allow(clippy::cast_precision_loss)]
        (SqliteValue::Float(x), SqliteValue::Integer(y)) => x.total_cmp(&(*y as f64)),
        (SqliteValue::Text(x), SqliteValue::Text(y)) => x.cmp(y),
        (SqliteValue::Blob(x), SqliteValue::Blob(y)) => x.cmp(y),
        _ => Ordering::Equal,
    }
}

// ── Index key encoding ───────────────────────────────────────────────────────

/// Compute the index key bytes for a given row and index definition.
///
/// Applies SQLite affinity coercion per the index column's declared affinity,
/// then serializes the result using the SQLite record format.
pub fn compute_index_key(
    index_def: &IndexDef,
    row: &[SqliteValue],
) -> Result<Vec<u8>, IndexRegenError> {
    let mut key_values = Vec::with_capacity(index_def.key_parts.len());

    for part in &index_def.key_parts {
        let (val, affinity, collation) = match part {
            IndexKeyPart::Column {
                col_idx,
                affinity,
                collation,
            } => {
                let i = col_idx.get() as usize;
                let v = row
                    .get(i)
                    .cloned()
                    .ok_or_else(|| IndexRegenError::ColumnOutOfBounds {
                        column_idx: col_idx.get(),
                        record_columns: row.len(),
                    })?;
                (v, *affinity, *collation)
            }
            IndexKeyPart::Expression {
                expr,
                affinity,
                collation,
            } => {
                let v = eval_rebase_expr(expr, row)?;
                (v, *affinity, *collation)
            }
        };

        // Apply affinity coercion.
        let coerced = val.apply_affinity(affinity);

        // Apply collation normalization for NOCASE.
        let final_val = apply_collation(coerced, collation);
        key_values.push(final_val);
    }

    Ok(serialize_record(&key_values))
}

/// Apply collation to a value for index key comparison purposes.
///
/// For NOCASE, text is lowercased before encoding so that the binary
/// comparison of encoded keys matches NOCASE semantics.
fn apply_collation(val: SqliteValue, collation: Collation) -> SqliteValue {
    match collation {
        Collation::Binary => val,
        Collation::Nocase => {
            if let SqliteValue::Text(s) = val {
                // SQLite NOCASE only folds ASCII a-z → A-Z. We must uppercase
                // for index key encoding so binary comparison matches SQLite.
                SqliteValue::Text(s.to_ascii_uppercase())
            } else {
                val
            }
        }
        Collation::Rtrim => {
            if let SqliteValue::Text(s) = val {
                SqliteValue::Text(s.trim_end().to_owned())
            } else {
                val
            }
        }
    }
}

// ── Participation check ──────────────────────────────────────────────────────

/// Check whether a row participates in an index.
///
/// For non-partial indexes this is always `true`. For partial indexes,
/// evaluates the WHERE predicate against the row.
fn row_participates(index_def: &IndexDef, row: &[SqliteValue]) -> Result<bool, IndexRegenError> {
    if let Some(ref predicate) = index_def.where_predicate {
        let result = eval_rebase_expr(predicate, row)?;
        Ok(sqlite_value_is_truthy(&result))
    } else {
        Ok(true)
    }
}

// ── Core regeneration logic ──────────────────────────────────────────────────

/// Apply `UpdateExpression` column updates to a base row, producing the updated row.
pub fn apply_column_updates(
    base_row: &[SqliteValue],
    column_updates: &[(ColumnIdx, RebaseExpr)],
    table_column_affinities: &[TypeAffinity],
) -> Result<Vec<SqliteValue>, IndexRegenError> {
    let mut updated = base_row.to_vec();

    for (col_idx, expr) in column_updates {
        let i = col_idx.get() as usize;
        if i >= updated.len() {
            return Err(IndexRegenError::ColumnOutOfBounds {
                column_idx: col_idx.get(),
                record_columns: updated.len(),
            });
        }
        let new_val = eval_rebase_expr(expr, base_row)?;
        // Apply table column affinity.
        let affinity = table_column_affinities
            .get(i)
            .copied()
            .unwrap_or(TypeAffinity::Blob);
        updated[i] = new_val.apply_affinity(affinity);
    }

    Ok(updated)
}

/// Result of index regeneration: a set of index operations to emit.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexRegenOps {
    /// Index operations to emit in the rebased intent log.
    pub ops: Vec<IntentOpKind>,
}

/// Step 1: Filter stale index ops from the intent log.
///
/// Returns the intent ops with stale `IndexInsert`/`IndexDelete` for the
/// given (table, rowid) removed.
pub fn discard_stale_index_ops(
    ops: &[IntentOpKind],
    table_id: TableId,
    rowid: RowId,
    indexes: &[IndexDef],
) -> Vec<IntentOpKind> {
    // Build set of index IDs belonging to this table.
    let table_index_ids: std::collections::HashSet<u32> = indexes
        .iter()
        .filter(|idx| idx.table_id == table_id)
        .map(|idx| idx.index_id.get())
        .collect();

    ops.iter()
        .filter(|op| {
            match op {
                IntentOpKind::IndexInsert {
                    index, rowid: r, ..
                }
                | IntentOpKind::IndexDelete {
                    index, rowid: r, ..
                } => {
                    // Keep if NOT for this (table's indexes, rowid).
                    !(table_index_ids.contains(&index.get()) && *r == rowid)
                }
                _ => true,
            }
        })
        .cloned()
        .collect()
}

/// Steps 2-6: Regenerate index operations for a rebased `UpdateExpression`.
///
/// Given the base row (from committed snapshot), the column updates, and the
/// table's secondary index definitions, this function:
/// - Computes the updated row
/// - For each index, checks participation and key changes
/// - Emits the minimal set of `IndexInsert`/`IndexDelete` ops
/// - Enforces UNIQUE constraints via the provided checker
#[allow(clippy::too_many_lines)]
pub fn regenerate_index_ops(
    base_record: &[u8],
    column_updates: &[(ColumnIdx, RebaseExpr)],
    indexes: &[IndexDef],
    rowid: RowId,
    unique_checker: &dyn UniqueChecker,
) -> Result<IndexRegenOps, IndexRegenError> {
    let _ = BEAD_ID;

    // Parse the base record.
    let base_row = parse_record(base_record).ok_or(IndexRegenError::MalformedRecord)?;

    // Determine table column affinities from the first index def (all share same table).
    let table_col_affinities = indexes.first().map_or(&[] as &[TypeAffinity], |idx| {
        idx.table_column_affinities.as_slice()
    });

    // Apply column updates to get the updated row.
    let updated_row = apply_column_updates(&base_row, column_updates, table_col_affinities)?;

    let mut ops = Vec::new();

    for index_def in indexes {
        // Step 2: enumerate secondary indexes (already provided).
        // Step 3: compute participation.
        let base_participates = row_participates(index_def, &base_row)?;
        let updated_participates = row_participates(index_def, &updated_row)?;

        // Step 4 + 5: compute keys and emit ops based on participation delta.
        match (base_participates, updated_participates) {
            (true, false) => {
                // Base participates, updated does not → IndexDelete.
                let old_key = compute_index_key(index_def, &base_row)?;
                ops.push(IntentOpKind::IndexDelete {
                    index: index_def.index_id,
                    key: old_key,
                    rowid,
                });
            }
            (false, true) => {
                // Base does not participate, updated does → IndexInsert.
                let new_key = compute_index_key(index_def, &updated_row)?;

                // Step 6: UNIQUE enforcement.
                if index_def.unique {
                    if let Some(conflicting) =
                        unique_checker.check_unique(index_def.index_id, &new_key, rowid)
                    {
                        return Err(IndexRegenError::UniqueConstraintViolation {
                            index_id: index_def.index_id,
                            conflicting_rowid: conflicting,
                        });
                    }
                }

                ops.push(IntentOpKind::IndexInsert {
                    index: index_def.index_id,
                    key: new_key,
                    rowid,
                });
            }
            (true, true) => {
                // Both participate: compare keys.
                let old_key = compute_index_key(index_def, &base_row)?;
                let new_key = compute_index_key(index_def, &updated_row)?;

                if old_key != new_key {
                    // Key changed → delete old, insert new.
                    ops.push(IntentOpKind::IndexDelete {
                        index: index_def.index_id,
                        key: old_key,
                        rowid,
                    });

                    // Step 6: UNIQUE enforcement for the new key.
                    if index_def.unique {
                        if let Some(conflicting) =
                            unique_checker.check_unique(index_def.index_id, &new_key, rowid)
                        {
                            return Err(IndexRegenError::UniqueConstraintViolation {
                                index_id: index_def.index_id,
                                conflicting_rowid: conflicting,
                            });
                        }
                    }

                    ops.push(IntentOpKind::IndexInsert {
                        index: index_def.index_id,
                        key: new_key,
                        rowid,
                    });
                }
                // If old_key == new_key → no op (Step 5d).
            }
            (false, false) => {
                // Neither participates → no op.
            }
        }
    }

    Ok(IndexRegenOps { ops })
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use fsqlite_types::record::serialize_record;

    /// Helper: build a simple ordinary-column index def.
    fn ordinary_index(
        index_id: u32,
        table_id: u32,
        columns: &[(u32, TypeAffinity)],
        unique: bool,
        table_affinities: Vec<TypeAffinity>,
    ) -> IndexDef {
        IndexDef {
            index_id: IndexId::new(index_id),
            table_id: TableId::new(table_id),
            unique,
            key_parts: columns
                .iter()
                .map(|&(col, aff)| IndexKeyPart::Column {
                    col_idx: ColumnIdx::new(col),
                    affinity: aff,
                    collation: Collation::Binary,
                })
                .collect(),
            where_predicate: None,
            table_column_affinities: table_affinities,
        }
    }

    /// Helper: serialize a row of values into record bytes.
    fn record_bytes(values: &[SqliteValue]) -> Vec<u8> {
        serialize_record(values)
    }

    // Test 1: Ordinary index key change produces Delete+Insert.
    #[test]
    fn test_index_regen_ordinary_index_key_change() {
        let base = record_bytes(&[
            SqliteValue::Integer(1),             // col 0: id
            SqliteValue::Text("old".to_owned()), // col 1: name (indexed)
            SqliteValue::Integer(42),            // col 2: value
        ]);

        let indexes = vec![ordinary_index(
            10,
            1,
            &[(1, TypeAffinity::Text)],
            false,
            vec![
                TypeAffinity::Integer,
                TypeAffinity::Text,
                TypeAffinity::Integer,
            ],
        )];

        let updates = vec![(
            ColumnIdx::new(1),
            RebaseExpr::Literal(SqliteValue::Text("new".to_owned())),
        )];

        let result =
            regenerate_index_ops(&base, &updates, &indexes, RowId::new(1), &NoOpUniqueChecker)
                .unwrap();

        assert_eq!(result.ops.len(), 2, "bead_id={BEAD_ID} key_change");
        assert!(
            matches!(&result.ops[0], IntentOpKind::IndexDelete { index, .. } if index.get() == 10),
            "bead_id={BEAD_ID} first op is delete"
        );
        assert!(
            matches!(&result.ops[1], IntentOpKind::IndexInsert { index, .. } if index.get() == 10),
            "bead_id={BEAD_ID} second op is insert"
        );
    }

    // Test 2: Partial index — row enters participation.
    #[test]
    fn test_index_regen_partial_index_participation_change() {
        // Base: col1=0, which is falsy → doesn't participate.
        let base = record_bytes(&[SqliteValue::Integer(1), SqliteValue::Integer(0)]);

        // Partial index with predicate = col1 (0 is falsy, 20 is truthy).
        let indexes = vec![IndexDef {
            index_id: IndexId::new(20),
            table_id: TableId::new(1),
            unique: false,
            key_parts: vec![IndexKeyPart::Column {
                col_idx: ColumnIdx::new(1),
                affinity: TypeAffinity::Integer,
                collation: Collation::Binary,
            }],
            where_predicate: Some(RebaseExpr::ColumnRef(ColumnIdx::new(1))),
            table_column_affinities: vec![TypeAffinity::Integer, TypeAffinity::Integer],
        }];

        let updates = vec![(
            ColumnIdx::new(1),
            RebaseExpr::Literal(SqliteValue::Integer(20)),
        )];

        let result =
            regenerate_index_ops(&base, &updates, &indexes, RowId::new(1), &NoOpUniqueChecker)
                .unwrap();

        // Base: col1=0 → predicate=0 → false (no participation).
        // Updated: col1=20 → predicate=20 → true (participates).
        // Expect: IndexInsert only.
        assert_eq!(result.ops.len(), 1, "bead_id={BEAD_ID} partial_insert_only");
        assert!(
            matches!(&result.ops[0], IntentOpKind::IndexInsert { index, .. } if index.get() == 20),
            "bead_id={BEAD_ID} op is insert"
        );
    }

    // Test 3: Partial index — both rows participate, key unchanged → no op.
    #[test]
    fn test_index_regen_partial_index_entry_to_entry() {
        // col0 = id, col1 = category (indexed, partial), col2 = name (updated).
        let base = record_bytes(&[
            SqliteValue::Integer(1),
            SqliteValue::Integer(5),
            SqliteValue::Text("foo".to_owned()),
        ]);

        // Partial index on col1 where col1 (truthy = non-zero).
        let indexes = vec![IndexDef {
            index_id: IndexId::new(30),
            table_id: TableId::new(1),
            unique: false,
            key_parts: vec![IndexKeyPart::Column {
                col_idx: ColumnIdx::new(1),
                affinity: TypeAffinity::Integer,
                collation: Collation::Binary,
            }],
            where_predicate: Some(RebaseExpr::ColumnRef(ColumnIdx::new(1))),
            table_column_affinities: vec![
                TypeAffinity::Integer,
                TypeAffinity::Integer,
                TypeAffinity::Text,
            ],
        }];

        // Update col2 (name), NOT col1 (the indexed column).
        let updates = vec![(
            ColumnIdx::new(2),
            RebaseExpr::Literal(SqliteValue::Text("bar".to_owned())),
        )];

        let result =
            regenerate_index_ops(&base, &updates, &indexes, RowId::new(1), &NoOpUniqueChecker)
                .unwrap();

        // Both participate (col1=5, truthy). Key is col1=5 → unchanged.
        assert!(result.ops.is_empty(), "bead_id={BEAD_ID} no_op_same_key");
    }

    // Test 4: UNIQUE constraint violation aborts.
    #[test]
    fn test_index_regen_unique_constraint_violation_aborts() {
        // Unique checker that reports a conflict with rowid 99.
        struct ConflictChecker;
        impl UniqueChecker for ConflictChecker {
            fn check_unique(
                &self,
                _index_id: IndexId,
                _key_bytes: &[u8],
                _exclude_rowid: RowId,
            ) -> Option<RowId> {
                Some(RowId::new(99))
            }
        }

        let base = record_bytes(&[
            SqliteValue::Integer(1),
            SqliteValue::Text("alpha".to_owned()),
        ]);

        let indexes = vec![ordinary_index(
            40,
            1,
            &[(1, TypeAffinity::Text)],
            true, // UNIQUE
            vec![TypeAffinity::Integer, TypeAffinity::Text],
        )];

        let updates = vec![(
            ColumnIdx::new(1),
            RebaseExpr::Literal(SqliteValue::Text("beta".to_owned())),
        )];

        let result =
            regenerate_index_ops(&base, &updates, &indexes, RowId::new(1), &ConflictChecker);

        assert!(result.is_err(), "bead_id={BEAD_ID} unique_violation");
        if let Err(IndexRegenError::UniqueConstraintViolation {
            index_id,
            conflicting_rowid,
        }) = result
        {
            assert_eq!(index_id.get(), 40, "bead_id={BEAD_ID} correct_index");
            assert_eq!(
                conflicting_rowid.get(),
                99,
                "bead_id={BEAD_ID} correct_rowid"
            );
        } else {
            panic!("bead_id={BEAD_ID} wrong error type");
        }
    }

    // Test 5: Expression index produces correct key.
    #[test]
    fn test_index_regen_expression_index() {
        let base = record_bytes(&[
            SqliteValue::Integer(1),
            SqliteValue::Text("Hello".to_owned()),
        ]);

        // Expression index: lower(col1).
        let indexes = vec![IndexDef {
            index_id: IndexId::new(50),
            table_id: TableId::new(1),
            unique: false,
            key_parts: vec![IndexKeyPart::Expression {
                expr: RebaseExpr::FunctionCall {
                    name: "lower".to_owned(),
                    args: vec![RebaseExpr::ColumnRef(ColumnIdx::new(1))],
                },
                affinity: TypeAffinity::Text,
                collation: Collation::Binary,
            }],
            where_predicate: None,
            table_column_affinities: vec![TypeAffinity::Integer, TypeAffinity::Text],
        }];

        // Update col1 from "Hello" to "World".
        let updates = vec![(
            ColumnIdx::new(1),
            RebaseExpr::Literal(SqliteValue::Text("World".to_owned())),
        )];

        let result =
            regenerate_index_ops(&base, &updates, &indexes, RowId::new(1), &NoOpUniqueChecker)
                .unwrap();

        // Old key: lower("Hello") = "hello"
        // New key: lower("World") = "world"
        // Both different → Delete + Insert.
        assert_eq!(result.ops.len(), 2, "bead_id={BEAD_ID} expr_index");

        // Verify the key bytes encode the lowered value.
        if let IntentOpKind::IndexDelete { key, .. } = &result.ops[0] {
            let parsed = parse_record(key).unwrap();
            assert_eq!(
                parsed,
                vec![SqliteValue::Text("hello".to_owned())],
                "bead_id={BEAD_ID} old_key_lower"
            );
        } else {
            panic!("bead_id={BEAD_ID} expected delete");
        }

        if let IntentOpKind::IndexInsert { key, .. } = &result.ops[1] {
            let parsed = parse_record(key).unwrap();
            assert_eq!(
                parsed,
                vec![SqliteValue::Text("world".to_owned())],
                "bead_id={BEAD_ID} new_key_lower"
            );
        } else {
            panic!("bead_id={BEAD_ID} expected insert");
        }
    }

    // Test 6: No-op when key unchanged.
    #[test]
    fn test_index_regen_no_op_when_key_unchanged() {
        // col0=id (indexed), col1=name (updated but not indexed).
        let base = record_bytes(&[SqliteValue::Integer(1), SqliteValue::Text("foo".to_owned())]);

        let indexes = vec![ordinary_index(
            60,
            1,
            &[(0, TypeAffinity::Integer)], // Index on col0.
            false,
            vec![TypeAffinity::Integer, TypeAffinity::Text],
        )];

        // Update col1 (not indexed).
        let updates = vec![(
            ColumnIdx::new(1),
            RebaseExpr::Literal(SqliteValue::Text("bar".to_owned())),
        )];

        let result =
            regenerate_index_ops(&base, &updates, &indexes, RowId::new(1), &NoOpUniqueChecker)
                .unwrap();

        assert!(result.ops.is_empty(), "bead_id={BEAD_ID} no_op_unchanged");
    }

    // Test 7: Multiple indexes on same table.
    #[test]
    fn test_index_regen_multiple_indexes_same_table() {
        let base = record_bytes(&[
            SqliteValue::Integer(1),
            SqliteValue::Text("alice".to_owned()),
            SqliteValue::Integer(30),
        ]);

        let affinities = vec![
            TypeAffinity::Integer,
            TypeAffinity::Text,
            TypeAffinity::Integer,
        ];

        let indexes = vec![
            ordinary_index(70, 1, &[(1, TypeAffinity::Text)], false, affinities.clone()),
            ordinary_index(71, 1, &[(2, TypeAffinity::Integer)], false, affinities),
        ];

        // Update col1 (affects index 70) and col2 (affects index 71).
        let updates = vec![
            (
                ColumnIdx::new(1),
                RebaseExpr::Literal(SqliteValue::Text("bob".to_owned())),
            ),
            (
                ColumnIdx::new(2),
                RebaseExpr::Literal(SqliteValue::Integer(40)),
            ),
        ];

        let result =
            regenerate_index_ops(&base, &updates, &indexes, RowId::new(1), &NoOpUniqueChecker)
                .unwrap();

        // Both indexes have key changes → 2 deletes + 2 inserts = 4 ops.
        assert_eq!(result.ops.len(), 4, "bead_id={BEAD_ID} multi_index");

        // Index 70: delete old, insert new.
        assert!(
            matches!(&result.ops[0], IntentOpKind::IndexDelete { index, .. } if index.get() == 70)
        );
        assert!(
            matches!(&result.ops[1], IntentOpKind::IndexInsert { index, .. } if index.get() == 70)
        );
        // Index 71: delete old, insert new.
        assert!(
            matches!(&result.ops[2], IntentOpKind::IndexDelete { index, .. } if index.get() == 71)
        );
        assert!(
            matches!(&result.ops[3], IntentOpKind::IndexInsert { index, .. } if index.get() == 71)
        );
    }

    // Test 8: Overflow key handling (large key values).
    #[test]
    fn test_index_regen_overflow_key_handling() {
        // Use a large text value that would require overflow pages in real B-tree.
        let large_text = "x".repeat(4000);
        let base = record_bytes(&[
            SqliteValue::Integer(1),
            SqliteValue::Text(large_text.clone()),
        ]);

        let indexes = vec![ordinary_index(
            80,
            1,
            &[(1, TypeAffinity::Text)],
            false,
            vec![TypeAffinity::Integer, TypeAffinity::Text],
        )];

        let new_large_text = "y".repeat(4000);
        let updates = vec![(
            ColumnIdx::new(1),
            RebaseExpr::Literal(SqliteValue::Text(new_large_text.clone())),
        )];

        let result =
            regenerate_index_ops(&base, &updates, &indexes, RowId::new(1), &NoOpUniqueChecker)
                .unwrap();

        assert_eq!(result.ops.len(), 2, "bead_id={BEAD_ID} overflow_key");

        // Verify keys encode the large values correctly.
        if let IntentOpKind::IndexDelete { key, .. } = &result.ops[0] {
            let parsed = parse_record(key).unwrap();
            assert_eq!(parsed[0], SqliteValue::Text(large_text));
        }
        if let IntentOpKind::IndexInsert { key, .. } = &result.ops[1] {
            let parsed = parse_record(key).unwrap();
            assert_eq!(parsed[0], SqliteValue::Text(new_large_text));
        }
    }

    // Test 9: Discard stale index ops.
    #[test]
    fn test_discard_stale_index_ops() {
        let indexes = vec![ordinary_index(
            10,
            1,
            &[(1, TypeAffinity::Text)],
            false,
            vec![TypeAffinity::Integer, TypeAffinity::Text],
        )];

        let ops = vec![
            // Stale: same table's index, same rowid.
            IntentOpKind::IndexDelete {
                index: IndexId::new(10),
                key: vec![1, 2, 3],
                rowid: RowId::new(5),
            },
            IntentOpKind::IndexInsert {
                index: IndexId::new(10),
                key: vec![4, 5, 6],
                rowid: RowId::new(5),
            },
            // Keep: different rowid.
            IntentOpKind::IndexInsert {
                index: IndexId::new(10),
                key: vec![7, 8, 9],
                rowid: RowId::new(99),
            },
            // Keep: different index (not in table 1).
            IntentOpKind::IndexDelete {
                index: IndexId::new(999),
                key: vec![10, 11],
                rowid: RowId::new(5),
            },
            // Keep: non-index op.
            IntentOpKind::Update {
                table: TableId::new(1),
                key: RowId::new(5),
                new_record: vec![],
            },
        ];

        let filtered = discard_stale_index_ops(&ops, TableId::new(1), RowId::new(5), &indexes);

        assert_eq!(filtered.len(), 3, "bead_id={BEAD_ID} stale_discard");
        // Kept: different-rowid insert, different-index delete, non-index update.
        assert!(
            matches!(&filtered[0], IntentOpKind::IndexInsert { rowid, .. } if rowid.get() == 99)
        );
        assert!(
            matches!(&filtered[1], IntentOpKind::IndexDelete { index, .. } if index.get() == 999)
        );
        assert!(matches!(&filtered[2], IntentOpKind::Update { .. }));
    }

    // Test 10: Schema epoch mismatch (column out of bounds).
    #[test]
    fn test_column_out_of_bounds_error() {
        let base = record_bytes(&[SqliteValue::Integer(1)]);

        let indexes = vec![ordinary_index(
            90,
            1,
            &[(5, TypeAffinity::Text)], // col 5 doesn't exist
            false,
            vec![TypeAffinity::Integer],
        )];

        let updates = vec![(
            ColumnIdx::new(0),
            RebaseExpr::Literal(SqliteValue::Integer(2)),
        )];

        let result =
            regenerate_index_ops(&base, &updates, &indexes, RowId::new(1), &NoOpUniqueChecker);

        assert!(
            matches!(result, Err(IndexRegenError::ColumnOutOfBounds { .. })),
            "bead_id={BEAD_ID} col_oob"
        );
    }

    // Test 11: NOCASE collation normalizes keys.
    #[test]
    fn test_nocase_collation_key_normalization() {
        let base = record_bytes(&[
            SqliteValue::Integer(1),
            SqliteValue::Text("hello".to_owned()),
        ]);

        let indexes = vec![IndexDef {
            index_id: IndexId::new(100),
            table_id: TableId::new(1),
            unique: false,
            key_parts: vec![IndexKeyPart::Column {
                col_idx: ColumnIdx::new(1),
                affinity: TypeAffinity::Text,
                collation: Collation::Nocase,
            }],
            where_predicate: None,
            table_column_affinities: vec![TypeAffinity::Integer, TypeAffinity::Text],
        }];

        // Update from "hello" to "HELLO" — NOCASE means these are the SAME key.
        let updates = vec![(
            ColumnIdx::new(1),
            RebaseExpr::Literal(SqliteValue::Text("HELLO".to_owned())),
        )];

        let result =
            regenerate_index_ops(&base, &updates, &indexes, RowId::new(1), &NoOpUniqueChecker)
                .unwrap();

        // With NOCASE, "hello" and "HELLO" normalize to same key → no op.
        assert!(result.ops.is_empty(), "bead_id={BEAD_ID} nocase_same_key");
    }

    // E2E test placeholder: test_e2e_index_regeneration_on_rebase_partial_unique
    // (Full E2E requires B-tree integration + C sqlite3 oracle, deferred to
    // integration test suite.)

    // ── Additional edge case tests ───────────────────────────────────────────

    // Test: RebaseExpr evaluation — arithmetic operations.
    #[test]
    fn test_eval_rebase_expr_arithmetic() {
        let row = vec![SqliteValue::Integer(10), SqliteValue::Integer(3)];

        // col0 + col1
        let expr = RebaseExpr::BinaryOp {
            op: RebaseBinaryOp::Add,
            left: Box::new(RebaseExpr::ColumnRef(ColumnIdx::new(0))),
            right: Box::new(RebaseExpr::ColumnRef(ColumnIdx::new(1))),
        };
        let result = eval_rebase_expr(&expr, &row).unwrap();
        assert_eq!(result, SqliteValue::Integer(13), "bead_id={BEAD_ID} add");

        // col0 * col1
        let expr = RebaseExpr::BinaryOp {
            op: RebaseBinaryOp::Multiply,
            left: Box::new(RebaseExpr::ColumnRef(ColumnIdx::new(0))),
            right: Box::new(RebaseExpr::ColumnRef(ColumnIdx::new(1))),
        };
        let result = eval_rebase_expr(&expr, &row).unwrap();
        assert_eq!(result, SqliteValue::Integer(30), "bead_id={BEAD_ID} mul");
    }

    // Test: Division by zero returns NULL.
    #[test]
    fn test_eval_division_by_zero() {
        let row = vec![SqliteValue::Integer(10), SqliteValue::Integer(0)];
        let expr = RebaseExpr::BinaryOp {
            op: RebaseBinaryOp::Divide,
            left: Box::new(RebaseExpr::ColumnRef(ColumnIdx::new(0))),
            right: Box::new(RebaseExpr::ColumnRef(ColumnIdx::new(1))),
        };
        let result = eval_rebase_expr(&expr, &row).unwrap();
        assert_eq!(result, SqliteValue::Null, "bead_id={BEAD_ID} div_zero");
    }

    // Test: Partial index — row leaves participation → IndexDelete.
    #[test]
    fn test_index_regen_partial_leaves_participation() {
        let base = record_bytes(&[
            SqliteValue::Integer(1),
            SqliteValue::Integer(10), // col1 = 10 (truthy → participates)
        ]);

        let indexes = vec![IndexDef {
            index_id: IndexId::new(110),
            table_id: TableId::new(1),
            unique: false,
            key_parts: vec![IndexKeyPart::Column {
                col_idx: ColumnIdx::new(1),
                affinity: TypeAffinity::Integer,
                collation: Collation::Binary,
            }],
            where_predicate: Some(RebaseExpr::ColumnRef(ColumnIdx::new(1))),
            table_column_affinities: vec![TypeAffinity::Integer, TypeAffinity::Integer],
        }];

        // Update col1 from 10 to 0 (falsy → no longer participates).
        let updates = vec![(
            ColumnIdx::new(1),
            RebaseExpr::Literal(SqliteValue::Integer(0)),
        )];

        let result =
            regenerate_index_ops(&base, &updates, &indexes, RowId::new(1), &NoOpUniqueChecker)
                .unwrap();

        assert_eq!(result.ops.len(), 1, "bead_id={BEAD_ID} leaves_partial");
        assert!(
            matches!(&result.ops[0], IntentOpKind::IndexDelete { index, .. } if index.get() == 110),
            "bead_id={BEAD_ID} delete_on_leave"
        );
    }

    // Test: UNIQUE index with no conflict passes.
    #[test]
    fn test_index_regen_unique_no_conflict() {
        let base = record_bytes(&[
            SqliteValue::Integer(1),
            SqliteValue::Text("alpha".to_owned()),
        ]);

        let indexes = vec![ordinary_index(
            120,
            1,
            &[(1, TypeAffinity::Text)],
            true,
            vec![TypeAffinity::Integer, TypeAffinity::Text],
        )];

        let updates = vec![(
            ColumnIdx::new(1),
            RebaseExpr::Literal(SqliteValue::Text("beta".to_owned())),
        )];

        let result =
            regenerate_index_ops(&base, &updates, &indexes, RowId::new(1), &NoOpUniqueChecker)
                .unwrap();

        assert_eq!(result.ops.len(), 2, "bead_id={BEAD_ID} unique_ok");
    }

    // Test: apply_column_updates produces correct updated row.
    #[test]
    fn test_apply_column_updates() {
        let base = vec![
            SqliteValue::Integer(1),
            SqliteValue::Text("old".to_owned()),
            SqliteValue::Float(3.125),
        ];
        let affinities = vec![
            TypeAffinity::Integer,
            TypeAffinity::Text,
            TypeAffinity::Real,
        ];

        let updates = vec![
            (
                ColumnIdx::new(1),
                RebaseExpr::Literal(SqliteValue::Text("new".to_owned())),
            ),
            (
                ColumnIdx::new(2),
                RebaseExpr::Literal(SqliteValue::Float(2.72)),
            ),
        ];

        let result = apply_column_updates(&base, &updates, &affinities).unwrap();
        assert_eq!(result[0], SqliteValue::Integer(1));
        assert_eq!(result[1], SqliteValue::Text("new".to_owned()));
        assert_eq!(result[2], SqliteValue::Float(2.72));
    }

    // Test: Malformed record returns error.
    #[test]
    fn test_malformed_record_error() {
        let bad_record = vec![0xFF, 0xFF, 0xFF]; // Invalid varint header.
        let indexes = vec![ordinary_index(
            130,
            1,
            &[(0, TypeAffinity::Integer)],
            false,
            vec![TypeAffinity::Integer],
        )];

        let result = regenerate_index_ops(
            &bad_record,
            &[],
            &indexes,
            RowId::new(1),
            &NoOpUniqueChecker,
        );

        assert!(
            matches!(result, Err(IndexRegenError::MalformedRecord)),
            "bead_id={BEAD_ID} malformed"
        );
    }

    // Test: Composite index key with multiple columns.
    #[test]
    fn test_composite_index_key() {
        let base = record_bytes(&[
            SqliteValue::Integer(1),
            SqliteValue::Text("alice".to_owned()),
            SqliteValue::Integer(30),
        ]);

        // Composite index on (col1, col2).
        let indexes = vec![IndexDef {
            index_id: IndexId::new(140),
            table_id: TableId::new(1),
            unique: false,
            key_parts: vec![
                IndexKeyPart::Column {
                    col_idx: ColumnIdx::new(1),
                    affinity: TypeAffinity::Text,
                    collation: Collation::Binary,
                },
                IndexKeyPart::Column {
                    col_idx: ColumnIdx::new(2),
                    affinity: TypeAffinity::Integer,
                    collation: Collation::Binary,
                },
            ],
            where_predicate: None,
            table_column_affinities: vec![
                TypeAffinity::Integer,
                TypeAffinity::Text,
                TypeAffinity::Integer,
            ],
        }];

        // Update only col2.
        let updates = vec![(
            ColumnIdx::new(2),
            RebaseExpr::Literal(SqliteValue::Integer(40)),
        )];

        let result =
            regenerate_index_ops(&base, &updates, &indexes, RowId::new(1), &NoOpUniqueChecker)
                .unwrap();

        // Key changes because col2 is part of the composite key.
        assert_eq!(result.ops.len(), 2, "bead_id={BEAD_ID} composite_key");

        if let IntentOpKind::IndexInsert { key, .. } = &result.ops[1] {
            let parsed = parse_record(key).unwrap();
            assert_eq!(parsed.len(), 2, "bead_id={BEAD_ID} composite_2_cols");
            assert_eq!(parsed[0], SqliteValue::Text("alice".to_owned()));
            assert_eq!(parsed[1], SqliteValue::Integer(40));
        } else {
            panic!("bead_id={BEAD_ID} expected insert");
        }
    }

    // Test: Using a `BTreeMap` unique checker for more realistic scenario.
    #[test]
    fn test_unique_checker_with_btreemap() {
        /// A simple in-memory unique checker backed by a `BTreeMap`.
        struct BTreeMapChecker {
            entries: BTreeMap<(u32, Vec<u8>), RowId>,
        }

        impl UniqueChecker for BTreeMapChecker {
            fn check_unique(
                &self,
                index_id: IndexId,
                key_bytes: &[u8],
                exclude_rowid: RowId,
            ) -> Option<RowId> {
                self.entries
                    .get(&(index_id.get(), key_bytes.to_vec()))
                    .filter(|&&rid| rid != exclude_rowid)
                    .copied()
            }
        }

        let base = record_bytes(&[
            SqliteValue::Integer(1),
            SqliteValue::Text("alice".to_owned()),
        ]);

        let indexes = vec![ordinary_index(
            150,
            1,
            &[(1, TypeAffinity::Text)],
            true,
            vec![TypeAffinity::Integer, TypeAffinity::Text],
        )];

        let updates = vec![(
            ColumnIdx::new(1),
            RebaseExpr::Literal(SqliteValue::Text("bob".to_owned())),
        )];

        // Pre-populate: "bob" key already exists for rowid 42.
        let new_key = compute_index_key(
            &indexes[0],
            &[SqliteValue::Integer(1), SqliteValue::Text("bob".to_owned())],
        )
        .unwrap();
        let mut entries = BTreeMap::new();
        entries.insert((150, new_key), RowId::new(42));

        let checker = BTreeMapChecker { entries };

        let result = regenerate_index_ops(&base, &updates, &indexes, RowId::new(1), &checker);

        assert!(
            matches!(
                result,
                Err(IndexRegenError::UniqueConstraintViolation {
                    conflicting_rowid,
                    ..
                }) if conflicting_rowid.get() == 42
            ),
            "bead_id={BEAD_ID} btreemap_unique"
        );
    }
}
