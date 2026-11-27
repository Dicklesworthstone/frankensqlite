//! Bloodstream: FrankenSQLite materialized view push to render tree (bd-ehk.3).
//!
//! This module defines the delta-propagation infrastructure for pushing algebraic
//! deltas from FrankenSQLite materialized views directly into a render tree.
//! Database row changes propagate to targeted ANSI terminal diffs in O(delta) time,
//! avoiding intermediary query-serialize-deserialize-render polling.
//!
//! Contract:
//!   TRACING: span `bloodstream.delta` with fields `source_table`, `rows_changed`,
//!            `propagation_duration_us`, `widgets_invalidated`.
//!   LOG: DEBUG for delta propagation, INFO for new materialized view binding.
//!   METRICS: counter `bloodstream_deltas_total`,
//!            histogram `bloodstream_propagation_duration_us`,
//!            gauge `bloodstream_active_bindings`.

// SmallText carries a OnceLock for lazy long-string interning, which trips
// `mutable_key_type` at every BTreeMap<SmallText, _> site even though hash
// and equality remain stable.
#![allow(clippy::mutable_key_type)]

use fsqlite_types::SqliteValue;
use fsqlite_types::opcode::{Opcode, P4, VdbeOp};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-ehk.3";

/// Schema version for structured output compatibility.
pub const BLOODSTREAM_SCHEMA_VERSION: u32 = 1;

// ── Delta Types ──────────────────────────────────────────────────────────────

/// The kind of row-level change that triggered a delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DeltaKind {
    /// New row inserted.
    Insert,
    /// Existing row updated (before + after images).
    Update,
    /// Existing row deleted.
    Delete,
}

impl fmt::Display for DeltaKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Insert => write!(f, "INSERT"),
            Self::Update => write!(f, "UPDATE"),
            Self::Delete => write!(f, "DELETE"),
        }
    }
}

impl DeltaKind {
    /// All variants for exhaustive testing.
    pub const ALL: [Self; 3] = [Self::Insert, Self::Update, Self::Delete];
}

/// A single algebraic delta representing one row change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlgebraicDelta {
    /// Source table that produced the change.
    pub source_table: String,
    /// Row identifier (rowid or primary key).
    pub row_id: i64,
    /// Kind of mutation.
    pub kind: DeltaKind,
    /// Column indices affected (empty for DELETE).
    pub affected_columns: Vec<usize>,
    /// Monotonic sequence number within a propagation batch.
    pub seq: u64,
}

/// A batch of deltas propagated atomically from a single transaction commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaBatch {
    /// Transaction ID that produced these deltas.
    pub txn_id: u64,
    /// Commit sequence number from the WAL.
    pub commit_seq: u64,
    /// Ordered deltas within this batch.
    pub deltas: Vec<AlgebraicDelta>,
    /// Unix timestamp (nanoseconds) when the batch was created.
    pub created_at_ns: u64,
}

impl DeltaBatch {
    /// Create a new empty batch.
    pub fn new(txn_id: u64, commit_seq: u64, created_at_ns: u64) -> Self {
        Self {
            txn_id,
            commit_seq,
            deltas: Vec::new(),
            created_at_ns,
        }
    }

    /// Number of deltas in this batch.
    pub fn len(&self) -> usize {
        self.deltas.len()
    }

    /// Whether the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.deltas.is_empty()
    }

    /// Push a delta into the batch.
    pub fn push(&mut self, delta: AlgebraicDelta) {
        self.deltas.push(delta);
    }

    /// Distinct source tables referenced in this batch.
    pub fn source_tables(&self) -> BTreeSet<&str> {
        self.deltas
            .iter()
            .map(|d| d.source_table.as_str())
            .collect()
    }

    /// Count deltas by kind.
    pub fn count_by_kind(&self) -> BTreeMap<DeltaKind, usize> {
        let mut counts = BTreeMap::new();
        for d in &self.deltas {
            *counts.entry(d.kind).or_insert(0) += 1;
        }
        counts
    }
}

// ── Weighted Rows & Differential Operators ──────────────────────────────────

/// A row plus its algebraic weight in the differential stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WeightedRow {
    /// Row payload.
    pub values: Vec<SqliteValue>,
    /// Algebraic multiplicity (`+1` insert, `-1` delete, higher magnitudes for
    /// aggregated or coalesced deltas).
    pub weight: i64,
}

impl WeightedRow {
    /// Construct a weighted row.
    pub fn new(values: Vec<SqliteValue>, weight: i64) -> Self {
        Self { values, weight }
    }

    fn is_zero(&self) -> bool {
        self.weight == 0
    }

    fn project(&self, columns: &[usize]) -> Result<Vec<SqliteValue>, DifferentialPlanError> {
        columns
            .iter()
            .map(|&column| {
                self.values
                    .get(column)
                    .cloned()
                    .ok_or(DifferentialPlanError::ColumnOutOfBounds {
                        column,
                        width: self.values.len(),
                    })
            })
            .collect()
    }
}

/// Errors from the weighted-row differential automata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DifferentialPlanError {
    /// A requested column index exceeded the row width.
    ColumnOutOfBounds { column: usize, width: usize },
    /// Join keys must specify the same number of left/right columns.
    JoinKeyArityMismatch { left: usize, right: usize },
    /// The compiled automaton no longer matches the input row shape.
    SchemaChanged {
        expected_width: usize,
        actual_width: usize,
    },
    /// The VDBE program shape is not yet supported by the differential bootstrap.
    UnsupportedProgram { detail: String },
}

impl fmt::Display for DifferentialPlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ColumnOutOfBounds { column, width } => {
                write!(f, "column {column} out of bounds for row width {width}")
            }
            Self::JoinKeyArityMismatch { left, right } => {
                write!(f, "join key arity mismatch: left={left}, right={right}")
            }
            Self::SchemaChanged {
                expected_width,
                actual_width,
            } => write!(
                f,
                "schema changed: expected row width {expected_width}, got {actual_width}"
            ),
            Self::UnsupportedProgram { detail } => {
                write!(f, "unsupported differential VDBE program: {detail}")
            }
        }
    }
}

impl std::error::Error for DifferentialPlanError {}

/// A simple weighted-row operator sequence representing the initial
/// differential automata surface for Bloodstream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DifferentialAutomaton {
    operators: Vec<DifferentialOperator>,
    expected_input_width: Option<usize>,
}

impl DifferentialAutomaton {
    /// Create an automaton from an ordered operator list.
    pub fn new(operators: Vec<DifferentialOperator>) -> Self {
        Self {
            operators,
            expected_input_width: None,
        }
    }

    /// Bootstrap an automaton from a narrow supported subset of VDBE bytecode.
    ///
    /// Current support is intentionally fail-closed:
    /// - single `ResultRow`
    /// - projection via plain `Column` opcodes
    /// - optional `=` filter compiled as `Column`, literal load, `IsNull`, `Ne`
    /// - full-scan loop shape (`Rewind` ... `ResultRow` ... `Next`)
    pub fn from_vdbe_ops(
        ops: &[VdbeOp],
        expected_input_width: usize,
    ) -> Result<Self, DifferentialPlanError> {
        Ok(Self {
            operators: translate_vdbe_ops(ops)?,
            expected_input_width: Some(expected_input_width),
        })
    }

    /// Execute the operator sequence over a weighted-row batch.
    pub fn execute(&self, rows: &[WeightedRow]) -> Result<Vec<WeightedRow>, DifferentialPlanError> {
        tracing::debug!(
            target: "fsqlite::differential::automata",
            event = "execute",
            operators = self.operators.len(),
            input_rows = rows.len()
        );

        let mut current: Vec<_> = rows.iter().filter(|row| !row.is_zero()).cloned().collect();
        if let Some(expected_width) = self.expected_input_width {
            for row in &current {
                if row.values.len() != expected_width {
                    return Err(DifferentialPlanError::SchemaChanged {
                        expected_width,
                        actual_width: row.values.len(),
                    });
                }
            }
        }
        for operator in &self.operators {
            current = operator.apply(&current)?;
        }

        tracing::debug!(
            target: "fsqlite::differential::automata",
            event = "execute_complete",
            operators = self.operators.len(),
            output_rows = current.len()
        );
        Ok(current)
    }

    /// Access the operator list.
    pub fn operators(&self) -> &[DifferentialOperator] {
        &self.operators
    }
}

/// Initial operator set for the Bloodstream differential automata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DifferentialOperator {
    /// Keep only rows where `column == value`.
    FilterEq { column: usize, value: SqliteValue },
    /// Keep only the requested columns, preserving row weight.
    Project { columns: Vec<usize> },
    /// Consolidate algebraic weights by key and elide zero-weight results.
    ConsolidateByKey { key_columns: Vec<usize> },
    /// Compute `ΔLeft ⋈ Right` with the current stream as the delta-left input.
    DeltaJoinLeft {
        stable_right: Vec<WeightedRow>,
        key_spec: JoinKeySpec,
    },
    /// Compute `Left ⋈ ΔRight` with the current stream as the delta-right input.
    DeltaJoinRight {
        stable_left: Vec<WeightedRow>,
        key_spec: JoinKeySpec,
    },
}

impl DifferentialOperator {
    fn apply(&self, rows: &[WeightedRow]) -> Result<Vec<WeightedRow>, DifferentialPlanError> {
        match self {
            Self::FilterEq { column, value } => rows
                .iter()
                .filter_map(|row| match row.values.get(*column) {
                    Some(candidate) if candidate == value => Some(Ok(row.clone())),
                    Some(_) => None,
                    None => Some(Err(DifferentialPlanError::ColumnOutOfBounds {
                        column: *column,
                        width: row.values.len(),
                    })),
                })
                .collect(),
            Self::Project { columns } => rows
                .iter()
                .map(|row| Ok(WeightedRow::new(row.project(columns)?, row.weight)))
                .collect(),
            Self::ConsolidateByKey { key_columns } => {
                let mut grouped: BTreeMap<Vec<SqliteValue>, i64> = BTreeMap::new();
                for row in rows {
                    let key = row.project(key_columns)?;
                    let weight = grouped.entry(key).or_insert(0);
                    *weight = weight.saturating_add(row.weight);
                }
                Ok(grouped
                    .into_iter()
                    .filter_map(|(values, weight)| {
                        (weight != 0).then(|| WeightedRow::new(values, weight))
                    })
                    .collect())
            }
            Self::DeltaJoinLeft {
                stable_right,
                key_spec,
            } => delta_join_left(rows, stable_right, key_spec),
            Self::DeltaJoinRight {
                stable_left,
                key_spec,
            } => delta_join_right(stable_left, rows, key_spec),
        }
    }
}

/// Column mapping for delta-aware inner joins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinKeySpec {
    /// Key columns read from the left relation.
    pub left_columns: Vec<usize>,
    /// Key columns read from the right relation.
    pub right_columns: Vec<usize>,
}

impl JoinKeySpec {
    /// Construct a join-key spec.
    pub fn new(left_columns: Vec<usize>, right_columns: Vec<usize>) -> Self {
        Self {
            left_columns,
            right_columns,
        }
    }

    fn validate(&self) -> Result<(), DifferentialPlanError> {
        if self.left_columns.len() != self.right_columns.len() {
            return Err(DifferentialPlanError::JoinKeyArityMismatch {
                left: self.left_columns.len(),
                right: self.right_columns.len(),
            });
        }
        Ok(())
    }
}

fn index_weighted_rows(
    rows: &[WeightedRow],
    key_columns: &[usize],
) -> Result<BTreeMap<Vec<SqliteValue>, Vec<WeightedRow>>, DifferentialPlanError> {
    let mut index: BTreeMap<Vec<SqliteValue>, Vec<WeightedRow>> = BTreeMap::new();
    for row in rows {
        if row.is_zero() {
            continue;
        }
        let key = row.project(key_columns)?;
        index.entry(key).or_default().push(row.clone());
    }
    Ok(index)
}

fn translate_vdbe_ops(ops: &[VdbeOp]) -> Result<Vec<DifferentialOperator>, DifferentialPlanError> {
    let result_row_indices = ops
        .iter()
        .enumerate()
        .filter_map(|(idx, op)| (op.opcode == Opcode::ResultRow).then_some(idx))
        .collect::<Vec<_>>();
    let [result_row_idx] = result_row_indices.as_slice() else {
        return Err(DifferentialPlanError::UnsupportedProgram {
            detail: format!(
                "expected exactly one ResultRow opcode, found {}",
                result_row_indices.len()
            ),
        });
    };
    let result_row = &ops[*result_row_idx];
    let output_columns =
        usize::try_from(result_row.p2).map_err(|_| DifferentialPlanError::UnsupportedProgram {
            detail: format!(
                "ResultRow column count is not representable as usize (p2={})",
                result_row.p2
            ),
        })?;
    let output_register = result_row.p1;
    let projection_start = result_row_idx.checked_sub(output_columns).ok_or_else(|| {
        DifferentialPlanError::UnsupportedProgram {
            detail: "ResultRow appears before its projection loads".to_owned(),
        }
    })?;
    let projection_columns = extract_vdbe_projection(
        &ops[projection_start..*result_row_idx],
        output_register,
        output_columns,
    )?;
    let rewind_idx = ops[..projection_start]
        .iter()
        .rposition(|op| op.opcode == Opcode::Rewind)
        .ok_or_else(|| DifferentialPlanError::UnsupportedProgram {
            detail: "expected a Rewind opcode before ResultRow".to_owned(),
        })?;
    let next_exists = ops[result_row_idx + 1..]
        .iter()
        .any(|op| op.opcode == Opcode::Next);
    if !next_exists {
        return Err(DifferentialPlanError::UnsupportedProgram {
            detail: "expected a Next opcode after ResultRow".to_owned(),
        });
    }

    let mut operators = Vec::new();
    let filter_slice = &ops[rewind_idx + 1..projection_start];
    if !filter_slice.is_empty() {
        let hoisted_literals = extract_hoisted_prologue_literals(ops, rewind_idx);
        operators.push(extract_vdbe_filter(filter_slice, &hoisted_literals)?);
    }
    operators.push(DifferentialOperator::Project {
        columns: projection_columns,
    });
    Ok(operators)
}

fn extract_vdbe_projection(
    projection_ops: &[VdbeOp],
    base_register: i32,
    output_columns: usize,
) -> Result<Vec<usize>, DifferentialPlanError> {
    if projection_ops.len() != output_columns {
        return Err(DifferentialPlanError::UnsupportedProgram {
            detail: format!(
                "expected {output_columns} projection opcodes before ResultRow, found {}",
                projection_ops.len()
            ),
        });
    }

    projection_ops
        .iter()
        .enumerate()
        .map(|(offset, op)| {
            let expected_register = base_register
                + i32::try_from(offset).map_err(|_| DifferentialPlanError::UnsupportedProgram {
                    detail: "projection register offset overflowed i32".to_owned(),
                })?;
            match op.opcode {
                Opcode::Column if op.p3 == expected_register => {
                    usize::try_from(op.p2).map_err(|_| DifferentialPlanError::UnsupportedProgram {
                        detail: format!("negative column index in projection opcode: {}", op.p2),
                    })
                }
                Opcode::Rowid => Err(DifferentialPlanError::UnsupportedProgram {
                    detail: "rowid projection is not yet supported by differential bootstrap"
                        .to_owned(),
                }),
                _ => Err(DifferentialPlanError::UnsupportedProgram {
                    detail: format!(
                        "projection opcode {:?} is not supported by differential bootstrap",
                        op.opcode
                    ),
                }),
            }
        })
        .collect()
}

fn extract_filter_column(column_op: &VdbeOp) -> Result<usize, DifferentialPlanError> {
    match column_op.opcode {
        Opcode::Column => {
            usize::try_from(column_op.p2).map_err(|_| DifferentialPlanError::UnsupportedProgram {
                detail: format!("negative filter column index: {}", column_op.p2),
            })
        }
        Opcode::Rowid => Err(DifferentialPlanError::UnsupportedProgram {
            detail: "rowid filters are not yet supported by differential bootstrap".to_owned(),
        }),
        _ => Err(DifferentialPlanError::UnsupportedProgram {
            detail: format!(
                "filter must start with Column/Rowid, found {:?}",
                column_op.opcode
            ),
        }),
    }
}

/// Extract non-NULL literals hoisted into the `Init` prologue by
/// `ProgramBuilder::finish()` (bd-w4pr1.13.31).
///
/// The prologue starts at `ops[0].p2` (after the main scan loop), consists
/// exclusively of non-NULL literal loads into distinct registers, and returns
/// before `rewind_idx` via `Opcode::Goto`. Any register written outside the
/// prologue is excluded so stale/mutated registers cannot match.
fn extract_hoisted_prologue_literals(
    ops: &[VdbeOp],
    rewind_idx: usize,
) -> BTreeMap<i32, SqliteValue> {
    let Some(init) = ops.first().filter(|op| op.opcode == Opcode::Init) else {
        return BTreeMap::new();
    };
    let Ok(prologue_start) = usize::try_from(init.p2) else {
        return BTreeMap::new();
    };
    if prologue_start <= rewind_idx || prologue_start >= ops.len() {
        return BTreeMap::new();
    }

    let mut literals = BTreeMap::new();
    let mut terminated_by_valid_goto = false;
    for op in &ops[prologue_start..] {
        if op.opcode == Opcode::Goto {
            let valid_resume =
                usize::try_from(op.p2).is_ok_and(|resume| resume > 0 && resume <= rewind_idx);
            if !valid_resume {
                return BTreeMap::new();
            }
            terminated_by_valid_goto = true;
            break;
        }
        let Ok((reg, value)) = decode_vdbe_literal_load(op) else {
            return BTreeMap::new();
        };
        if matches!(value, SqliteValue::Null) || literals.insert(reg, value).is_some() {
            return BTreeMap::new();
        }
    }
    if !terminated_by_valid_goto {
        return BTreeMap::new();
    }

    // Drop any register that is written by an instruction in the main body
    // before the prologue so hoisted literal lookups stay sound.
    for op in &ops[1..prologue_start] {
        let written_reg = match op.opcode {
            Opcode::Column => Some(op.p3),
            Opcode::Integer
            | Opcode::Int64
            | Opcode::Real
            | Opcode::String8
            | Opcode::Null
            | Opcode::Rowid
            | Opcode::Copy
            | Opcode::SCopy => Some(op.p2),
            _ => None,
        };
        if let Some(reg) = written_reg {
            literals.remove(&reg);
        }
    }

    literals
}

fn extract_vdbe_filter(
    filter_ops: &[VdbeOp],
    hoisted_literals: &BTreeMap<i32, SqliteValue>,
) -> Result<DifferentialOperator, DifferentialPlanError> {
    match filter_ops {
        // Inline literal equality filter: [Column, Literal, IsNull, Ne].
        [column_op, literal_op, is_null_op, compare_op] => {
            let column = extract_filter_column(column_op)?;
            let (literal_register, value) = decode_vdbe_literal_load(literal_op)?;
            if matches!(value, SqliteValue::Null) {
                return Err(DifferentialPlanError::UnsupportedProgram {
                    detail: "NULL equality filters are not supported by differential bootstrap"
                        .to_owned(),
                });
            }
            if is_null_op.opcode != Opcode::IsNull || is_null_op.p1 != literal_register {
                return Err(DifferentialPlanError::UnsupportedProgram {
                    detail: "simple equality filter must null-check the literal register"
                        .to_owned(),
                });
            }
            if compare_op.opcode != Opcode::Ne
                || compare_op.p1 != literal_register
                || compare_op.p3 != column_op.p3
            {
                return Err(DifferentialPlanError::UnsupportedProgram {
                    detail: "simple equality filter must compare literal register against the filter column using Ne".to_owned(),
                });
            }
            Ok(DifferentialOperator::FilterEq { column, value })
        }
        // Hoisted literal equality filter (bd-w4pr1.13.31): [Column, Ne], with
        // the non-NULL literal loaded once in the Init prologue.
        [column_op, compare_op] => {
            let column = extract_filter_column(column_op)?;
            if compare_op.opcode != Opcode::Ne || compare_op.p3 != column_op.p3 {
                return Err(DifferentialPlanError::UnsupportedProgram {
                    detail: "hoisted equality filter must compare hoisted literal register against the filter column using Ne".to_owned(),
                });
            }
            let value = hoisted_literals.get(&compare_op.p1).cloned().ok_or_else(|| {
                DifferentialPlanError::UnsupportedProgram {
                    detail: format!(
                        "hoisted equality filter register {} was not loaded as a non-NULL literal in the Init prologue",
                        compare_op.p1
                    ),
                }
            })?;
            Ok(DifferentialOperator::FilterEq { column, value })
        }
        _ => Err(DifferentialPlanError::UnsupportedProgram {
            detail: format!(
                "expected 2 (hoisted literal) or 4 (inline literal) opcodes for simple equality filter, found {}",
                filter_ops.len()
            ),
        }),
    }
}

fn decode_vdbe_literal_load(op: &VdbeOp) -> Result<(i32, SqliteValue), DifferentialPlanError> {
    match op.opcode {
        Opcode::Integer => Ok((op.p2, SqliteValue::Integer(i64::from(op.p1)))),
        Opcode::Int64 => match op.p4 {
            P4::Int64(value) => Ok((op.p2, SqliteValue::Integer(value))),
            _ => Err(DifferentialPlanError::UnsupportedProgram {
                detail: "Int64 opcode missing P4::Int64 payload".to_owned(),
            }),
        },
        Opcode::Real => match op.p4 {
            P4::Real(value) => Ok((op.p2, SqliteValue::Float(value))),
            _ => Err(DifferentialPlanError::UnsupportedProgram {
                detail: "Real opcode missing P4::Real payload".to_owned(),
            }),
        },
        Opcode::String8 => match &op.p4 {
            P4::Str(value) => Ok((op.p2, SqliteValue::Text(value.clone().into()))),
            _ => Err(DifferentialPlanError::UnsupportedProgram {
                detail: "String8 opcode missing P4::Str payload".to_owned(),
            }),
        },
        Opcode::Null => Ok((op.p2, SqliteValue::Null)),
        _ => Err(DifferentialPlanError::UnsupportedProgram {
            detail: format!("literal opcode {:?} is not supported", op.opcode),
        }),
    }
}

/// Compute `ΔLeft ⋈ Right`, preserving algebraic weights.
pub fn delta_join_left(
    delta_left: &[WeightedRow],
    stable_right: &[WeightedRow],
    key_spec: &JoinKeySpec,
) -> Result<Vec<WeightedRow>, DifferentialPlanError> {
    key_spec.validate()?;
    let right_index = index_weighted_rows(stable_right, &key_spec.right_columns)?;
    let mut joined = Vec::new();

    for left in delta_left {
        if left.is_zero() {
            continue;
        }
        let left_key = left.project(&key_spec.left_columns)?;
        if let Some(matches) = right_index.get(&left_key) {
            for right in matches {
                let mut values = left.values.clone();
                values.extend(right.values.clone());
                joined.push(WeightedRow::new(
                    values,
                    left.weight.saturating_mul(right.weight),
                ));
            }
        }
    }

    tracing::debug!(
        target: "fsqlite::differential::automata",
        event = "delta_join_left",
        delta_rows = delta_left.len(),
        stable_rows = stable_right.len(),
        output_rows = joined.len()
    );
    Ok(joined)
}

/// Compute `Left ⋈ ΔRight`, preserving algebraic weights.
pub fn delta_join_right(
    stable_left: &[WeightedRow],
    delta_right: &[WeightedRow],
    key_spec: &JoinKeySpec,
) -> Result<Vec<WeightedRow>, DifferentialPlanError> {
    key_spec.validate()?;
    let left_index = index_weighted_rows(stable_left, &key_spec.left_columns)?;
    let mut joined = Vec::new();

    for right in delta_right {
        if right.is_zero() {
            continue;
        }
        let right_key = right.project(&key_spec.right_columns)?;
        if let Some(matches) = left_index.get(&right_key) {
            for left in matches {
                let mut values = left.values.clone();
                values.extend(right.values.clone());
                joined.push(WeightedRow::new(
                    values,
                    left.weight.saturating_mul(right.weight),
                ));
            }
        }
    }

    tracing::debug!(
        target: "fsqlite::differential::automata",
        event = "delta_join_right",
        stable_rows = stable_left.len(),
        delta_rows = delta_right.len(),
        output_rows = joined.len()
    );
    Ok(joined)
}

// ── Materialized View Binding ────────────────────────────────────────────────

/// State of a binding between a materialized view and a render widget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BindingState {
    /// Actively propagating deltas.
    Active,
    /// Temporarily paused (e.g., widget not visible).
    Suspended,
    /// Permanently detached; will not receive further deltas.
    Detached,
}

impl fmt::Display for BindingState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Suspended => write!(f, "suspended"),
            Self::Detached => write!(f, "detached"),
        }
    }
}

impl BindingState {
    /// Whether the binding can receive deltas.
    pub fn is_receiving(&self) -> bool {
        matches!(self, Self::Active)
    }

    /// All variants.
    pub const ALL: [Self; 3] = [Self::Active, Self::Suspended, Self::Detached];
}

/// A binding between a SQL materialized view and a render tree widget.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewBinding {
    /// Unique binding identifier.
    pub binding_id: u64,
    /// SQL view name driving this binding.
    pub view_name: String,
    /// Widget identifier in the render tree.
    pub widget_id: String,
    /// Current binding state.
    pub state: BindingState,
    /// Tables this view depends on (for delta routing).
    pub source_tables: BTreeSet<String>,
    /// Number of deltas delivered through this binding.
    pub deltas_delivered: u64,
    /// Last commit_seq successfully propagated.
    pub last_commit_seq: Option<u64>,
}

impl ViewBinding {
    /// Create a new active binding.
    pub fn new(
        binding_id: u64,
        view_name: String,
        widget_id: String,
        source_tables: BTreeSet<String>,
    ) -> Self {
        Self {
            binding_id,
            view_name,
            widget_id,
            state: BindingState::Active,
            source_tables,
            deltas_delivered: 0,
            last_commit_seq: None,
        }
    }

    /// Suspend the binding (pauses delta delivery).
    pub fn suspend(&mut self) {
        if self.state == BindingState::Active {
            self.state = BindingState::Suspended;
        }
    }

    /// Resume a suspended binding.
    pub fn resume(&mut self) {
        if self.state == BindingState::Suspended {
            self.state = BindingState::Active;
        }
    }

    /// Permanently detach the binding.
    pub fn detach(&mut self) {
        self.state = BindingState::Detached;
    }

    /// Whether this binding cares about a given source table.
    pub fn matches_table(&self, table: &str) -> bool {
        self.source_tables.contains(table)
    }

    /// Record delivery of a delta batch.
    pub fn record_delivery(&mut self, batch: &DeltaBatch) {
        let relevant = batch
            .deltas
            .iter()
            .filter(|d| self.matches_table(&d.source_table))
            .count() as u64;
        self.deltas_delivered += relevant;
        if relevant > 0 {
            self.last_commit_seq = Some(batch.commit_seq);
        }
    }
}

// ── Propagation Engine ───────────────────────────────────────────────────────

/// Result of propagating a delta batch to the render tree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PropagationResult {
    /// All bindings received the batch successfully.
    Success { widgets_invalidated: usize },
    /// Some bindings received deltas; others were suspended or detached.
    Partial {
        widgets_invalidated: usize,
        skipped_suspended: usize,
        skipped_detached: usize,
    },
    /// No bindings matched the batch's source tables.
    NoMatch,
    /// The propagation pipeline is shut down.
    Shutdown,
}

impl PropagationResult {
    /// Number of widgets invalidated.
    pub fn widgets_invalidated(&self) -> usize {
        match self {
            Self::Success {
                widgets_invalidated,
            }
            | Self::Partial {
                widgets_invalidated,
                ..
            } => *widgets_invalidated,
            Self::NoMatch | Self::Shutdown => 0,
        }
    }

    /// Whether propagation succeeded (fully or partially).
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success { .. } | Self::Partial { .. })
    }
}

/// Configuration for the delta propagation engine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropagationConfig {
    /// Maximum batch size before forced flush.
    pub max_batch_size: usize,
    /// Maximum propagation latency target (microseconds).
    pub target_latency_us: u64,
    /// Whether to coalesce multiple deltas to the same row.
    pub coalesce_row_deltas: bool,
    /// Maximum number of active bindings.
    pub max_active_bindings: usize,
}

impl Default for PropagationConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1024,
            target_latency_us: 1000, // 1ms target
            coalesce_row_deltas: true,
            max_active_bindings: 256,
        }
    }
}

/// Metrics tracked by the propagation engine.
///
/// Maps to the contract:
///   counter `bloodstream_deltas_total`
///   histogram `bloodstream_propagation_duration_us`
///   gauge `bloodstream_active_bindings`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropagationMetrics {
    /// Total deltas propagated (counter).
    pub deltas_total: u64,
    /// Total batches propagated.
    pub batches_total: u64,
    /// Propagation durations in microseconds (histogram samples).
    pub propagation_durations_us: Vec<u64>,
    /// Current number of active bindings (gauge).
    pub active_bindings: usize,
    /// Number of suspended bindings.
    pub suspended_bindings: usize,
    /// Number of detached bindings.
    pub detached_bindings: usize,
    /// Total widgets invalidated across all propagations.
    pub total_widgets_invalidated: u64,
    /// Number of NoMatch results (batch had no matching bindings).
    pub no_match_count: u64,
}

impl PropagationMetrics {
    /// Create fresh zero-valued metrics.
    pub fn new() -> Self {
        Self {
            deltas_total: 0,
            batches_total: 0,
            propagation_durations_us: Vec::new(),
            active_bindings: 0,
            suspended_bindings: 0,
            detached_bindings: 0,
            total_widgets_invalidated: 0,
            no_match_count: 0,
        }
    }

    /// Mean propagation duration in microseconds.
    pub fn mean_duration_us(&self) -> f64 {
        if self.propagation_durations_us.is_empty() {
            return 0.0;
        }
        let sum: u64 = self.propagation_durations_us.iter().sum();
        sum as f64 / self.propagation_durations_us.len() as f64
    }

    /// P99 propagation duration in microseconds.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    pub fn p99_duration_us(&self) -> u64 {
        if self.propagation_durations_us.is_empty() {
            return 0;
        }
        let mut sorted = self.propagation_durations_us.clone();
        sorted.sort_unstable();
        let idx = ((sorted.len() as f64) * 0.99).ceil() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }
}

impl Default for PropagationMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// The propagation engine routes delta batches to matching view bindings.
pub struct PropagationEngine {
    config: PropagationConfig,
    bindings: Vec<ViewBinding>,
    metrics: PropagationMetrics,
    next_binding_id: u64,
    shutdown: bool,
}

impl PropagationEngine {
    /// Create a new engine with the given configuration.
    pub fn new(config: PropagationConfig) -> Self {
        Self {
            config,
            bindings: Vec::new(),
            metrics: PropagationMetrics::new(),
            next_binding_id: 1,
            shutdown: false,
        }
    }

    /// Register a new view binding. Returns the binding ID.
    pub fn bind(
        &mut self,
        view_name: String,
        widget_id: String,
        source_tables: BTreeSet<String>,
    ) -> Result<u64, BindingError> {
        if self.shutdown {
            return Err(BindingError::EngineShutdown);
        }
        let active = self
            .bindings
            .iter()
            .filter(|b| b.state == BindingState::Active)
            .count();
        if active >= self.config.max_active_bindings {
            return Err(BindingError::MaxBindingsExceeded {
                limit: self.config.max_active_bindings,
            });
        }
        let id = self.next_binding_id;
        self.next_binding_id += 1;
        let binding = ViewBinding::new(id, view_name, widget_id, source_tables);
        self.bindings.push(binding);
        self.refresh_binding_counts();
        Ok(id)
    }

    /// Unbind (detach) a binding by ID.
    pub fn unbind(&mut self, binding_id: u64) -> Result<(), BindingError> {
        let binding = self
            .bindings
            .iter_mut()
            .find(|b| b.binding_id == binding_id)
            .ok_or(BindingError::NotFound { binding_id })?;
        binding.detach();
        self.refresh_binding_counts();
        Ok(())
    }

    /// Suspend a binding by ID.
    pub fn suspend(&mut self, binding_id: u64) -> Result<(), BindingError> {
        let binding = self
            .bindings
            .iter_mut()
            .find(|b| b.binding_id == binding_id)
            .ok_or(BindingError::NotFound { binding_id })?;
        binding.suspend();
        self.refresh_binding_counts();
        Ok(())
    }

    /// Resume a suspended binding by ID.
    pub fn resume(&mut self, binding_id: u64) -> Result<(), BindingError> {
        let binding = self
            .bindings
            .iter_mut()
            .find(|b| b.binding_id == binding_id)
            .ok_or(BindingError::NotFound { binding_id })?;
        binding.resume();
        self.refresh_binding_counts();
        Ok(())
    }

    /// Propagate a delta batch to all matching active bindings.
    pub fn propagate(&mut self, batch: &DeltaBatch, duration_us: u64) -> PropagationResult {
        if self.shutdown {
            return PropagationResult::Shutdown;
        }

        let tables = batch.source_tables();
        let mut widgets_invalidated = 0usize;
        let mut skipped_suspended = 0usize;
        let mut skipped_detached = 0usize;
        let mut any_match = false;

        for binding in &mut self.bindings {
            let has_match = tables.iter().any(|t| binding.matches_table(t));
            if !has_match {
                continue;
            }
            any_match = true;

            match binding.state {
                BindingState::Active => {
                    binding.record_delivery(batch);
                    widgets_invalidated += 1;
                }
                BindingState::Suspended => {
                    skipped_suspended += 1;
                }
                BindingState::Detached => {
                    skipped_detached += 1;
                }
            }
        }

        // Update metrics.
        self.metrics.deltas_total += batch.len() as u64;
        self.metrics.batches_total += 1;
        self.metrics.propagation_durations_us.push(duration_us);
        self.metrics.total_widgets_invalidated += widgets_invalidated as u64;

        if !any_match {
            self.metrics.no_match_count += 1;
            return PropagationResult::NoMatch;
        }

        if skipped_suspended == 0 && skipped_detached == 0 {
            PropagationResult::Success {
                widgets_invalidated,
            }
        } else {
            PropagationResult::Partial {
                widgets_invalidated,
                skipped_suspended,
                skipped_detached,
            }
        }
    }

    /// Initiate engine shutdown; further binds and propagations are rejected.
    pub fn shutdown(&mut self) {
        self.shutdown = true;
    }

    /// Whether the engine is shut down.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown
    }

    /// Current metrics snapshot.
    pub fn metrics(&self) -> &PropagationMetrics {
        &self.metrics
    }

    /// Current configuration.
    pub fn config(&self) -> &PropagationConfig {
        &self.config
    }

    /// All bindings (for inspection).
    pub fn bindings(&self) -> &[ViewBinding] {
        &self.bindings
    }

    /// Get a binding by ID.
    pub fn get_binding(&self, binding_id: u64) -> Option<&ViewBinding> {
        self.bindings.iter().find(|b| b.binding_id == binding_id)
    }

    fn refresh_binding_counts(&mut self) {
        self.metrics.active_bindings = self
            .bindings
            .iter()
            .filter(|b| b.state == BindingState::Active)
            .count();
        self.metrics.suspended_bindings = self
            .bindings
            .iter()
            .filter(|b| b.state == BindingState::Suspended)
            .count();
        self.metrics.detached_bindings = self
            .bindings
            .iter()
            .filter(|b| b.state == BindingState::Detached)
            .count();
    }
}

/// Errors from binding operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindingError {
    /// Engine is shut down.
    EngineShutdown,
    /// Maximum active bindings exceeded.
    MaxBindingsExceeded { limit: usize },
    /// Binding ID not found.
    NotFound { binding_id: u64 },
}

impl fmt::Display for BindingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EngineShutdown => write!(f, "propagation engine is shut down"),
            Self::MaxBindingsExceeded { limit } => {
                write!(f, "max active bindings exceeded (limit: {limit})")
            }
            Self::NotFound { binding_id } => {
                write!(f, "binding {binding_id} not found")
            }
        }
    }
}

impl std::error::Error for BindingError {}

// ── Delta Coalescing ─────────────────────────────────────────────────────────

/// Coalesce deltas to the same row within a batch.
///
/// Rules:
/// - INSERT + UPDATE → INSERT (with merged columns)
/// - INSERT + DELETE → cancel (row never materialized)
/// - UPDATE + UPDATE → UPDATE (merged columns)
/// - UPDATE + DELETE → DELETE
/// - DELETE + INSERT → UPDATE (row replaced)
pub fn coalesce_deltas(deltas: &[AlgebraicDelta]) -> Vec<AlgebraicDelta> {
    // Group by (source_table, row_id) while preserving order of first appearance.
    let mut groups: BTreeMap<(&str, i64), Vec<&AlgebraicDelta>> = BTreeMap::new();
    let mut group_order = Vec::new();
    for d in deltas {
        let key = (d.source_table.as_str(), d.row_id);
        let entry = groups.entry(key).or_insert_with(|| {
            group_order.push(key);
            Vec::new()
        });
        entry.push(d);
    }

    let mut result = Vec::new();
    let mut next_seq = 0u64;

    for (table, row_id) in group_order {
        let Some(group) = groups.get(&(table, row_id)) else {
            continue;
        };
        let mut current_kind: Option<DeltaKind> = None;
        let mut merged_cols: BTreeSet<usize> = BTreeSet::new();

        for d in group {
            match (current_kind, d.kind) {
                (None, k) => {
                    current_kind = Some(k);
                    merged_cols.extend(&d.affected_columns);
                }
                (Some(DeltaKind::Insert), DeltaKind::Update) => {
                    // INSERT + UPDATE → INSERT
                    merged_cols.extend(&d.affected_columns);
                }
                (Some(DeltaKind::Insert), DeltaKind::Delete) => {
                    // INSERT + DELETE → cancel
                    current_kind = None;
                    merged_cols.clear();
                }
                (Some(DeltaKind::Update), DeltaKind::Update) => {
                    // UPDATE + UPDATE → UPDATE
                    merged_cols.extend(&d.affected_columns);
                }
                (Some(DeltaKind::Update), DeltaKind::Delete) => {
                    // UPDATE + DELETE → DELETE
                    current_kind = Some(DeltaKind::Delete);
                    merged_cols.clear();
                }
                (Some(DeltaKind::Delete), DeltaKind::Insert) => {
                    // DELETE + INSERT → UPDATE
                    current_kind = Some(DeltaKind::Update);
                    merged_cols = d.affected_columns.iter().copied().collect();
                }
                _ => {
                    // Other combinations: keep the latest.
                    current_kind = Some(d.kind);
                    merged_cols = d.affected_columns.iter().copied().collect();
                }
            }
        }

        if let Some(kind) = current_kind {
            result.push(AlgebraicDelta {
                source_table: table.to_string(),
                row_id,
                kind,
                affected_columns: merged_cols.into_iter().collect(),
                seq: next_seq,
            });
            next_seq += 1;
        }
    }

    result
}

// ── Tracing Contract Verification ────────────────────────────────────────────

/// Span field names required by the bloodstream tracing contract.
pub const REQUIRED_SPAN_FIELDS: &[&str] = &[
    "source_table",
    "rows_changed",
    "propagation_duration_us",
    "widgets_invalidated",
];

/// Metric names required by the bloodstream metrics contract.
pub const REQUIRED_METRICS: &[&str] = &[
    "bloodstream_deltas_total",
    "bloodstream_propagation_duration_us",
    "bloodstream_active_bindings",
];

/// The span name for delta propagation events.
pub const DELTA_SPAN_NAME: &str = "bloodstream.delta";

/// Verify that a propagation metrics snapshot satisfies the tracing contract.
#[allow(clippy::too_many_lines)]
pub fn verify_metrics_contract(metrics: &PropagationMetrics) -> Vec<ContractViolation> {
    let mut violations = Vec::new();

    // Active bindings gauge must be non-negative (always true for usize, but
    // verify against the metric semantics).
    if metrics.deltas_total > 0 && metrics.batches_total == 0 {
        violations.push(ContractViolation {
            field: "batches_total".to_string(),
            message: "deltas_total > 0 but batches_total == 0".to_string(),
        });
    }

    // Duration histogram should have one entry per batch.
    #[allow(clippy::cast_possible_truncation)]
    if metrics.propagation_durations_us.len() != metrics.batches_total as usize {
        violations.push(ContractViolation {
            field: "propagation_durations_us".to_string(),
            message: format!(
                "duration samples ({}) != batches_total ({})",
                metrics.propagation_durations_us.len(),
                metrics.batches_total
            ),
        });
    }

    // Binding counts should be consistent.
    let total_bindings =
        metrics.active_bindings + metrics.suspended_bindings + metrics.detached_bindings;
    if total_bindings == 0 && metrics.deltas_total > 0 && metrics.no_match_count == 0 {
        violations.push(ContractViolation {
            field: "binding_counts".to_string(),
            message: "deltas propagated but no bindings registered".to_string(),
        });
    }

    violations
}

/// A single contract violation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractViolation {
    pub field: String,
    pub message: String,
}

impl fmt::Display for ContractViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.field, self.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_ast::Statement;
    use fsqlite_parser::Parser;
    use fsqlite_vdbe::ProgramBuilder;
    use fsqlite_vdbe::codegen::{CodegenContext, ColumnInfo, TableSchema, codegen_select};

    fn bloodstream_test_schema() -> Vec<TableSchema> {
        vec![TableSchema {
            name: "users".to_owned(),
            root_page: 2,
            columns: vec![
                ColumnInfo::basic("id", 'd', false),
                ColumnInfo::basic("name", 'B', false),
            ],
            indexes: vec![],
            strict: false,
            without_rowid: false,
            without_rowid_pk_conflict: None,
            primary_key_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
        }]
    }

    fn compile_select(sql: &str, schema: &[TableSchema]) -> fsqlite_vdbe::VdbeProgram {
        let mut parser = Parser::from_sql(sql);
        let (stmts, errors) = parser.parse_all();
        assert!(errors.is_empty(), "parse errors: {errors:?}");
        let stmt = match &stmts[0] {
            Statement::Select(stmt) => stmt,
            other => panic!("expected SELECT statement, got {other:?}"),
        };
        let mut builder = ProgramBuilder::new();
        codegen_select(&mut builder, stmt, schema, &CodegenContext::default())
            .expect("codegen select");
        builder.finish().expect("finish program")
    }

    #[test]
    fn delta_kind_display_roundtrip() {
        for kind in DeltaKind::ALL {
            let s = kind.to_string();
            assert!(!s.is_empty());
        }
    }

    #[test]
    fn binding_state_lifecycle() {
        for state in BindingState::ALL {
            let s = state.to_string();
            assert!(!s.is_empty());
        }
        assert!(BindingState::Active.is_receiving());
        assert!(!BindingState::Suspended.is_receiving());
        assert!(!BindingState::Detached.is_receiving());
    }

    #[test]
    fn delta_batch_source_tables() {
        let mut batch = DeltaBatch::new(1, 1, 0);
        batch.push(AlgebraicDelta {
            source_table: "users".to_string(),
            row_id: 1,
            kind: DeltaKind::Insert,
            affected_columns: vec![0, 1],
            seq: 0,
        });
        batch.push(AlgebraicDelta {
            source_table: "orders".to_string(),
            row_id: 2,
            kind: DeltaKind::Update,
            affected_columns: vec![3],
            seq: 1,
        });
        let tables = batch.source_tables();
        assert!(tables.contains("users"));
        assert!(tables.contains("orders"));
        assert_eq!(tables.len(), 2);
    }

    #[test]
    fn differential_automaton_filter_project_consolidate_by_key() {
        let automaton = DifferentialAutomaton::new(vec![
            DifferentialOperator::FilterEq {
                column: 0,
                value: SqliteValue::Text("users".into()),
            },
            DifferentialOperator::Project { columns: vec![1] },
            DifferentialOperator::ConsolidateByKey {
                key_columns: vec![0],
            },
        ]);

        let rows = vec![
            WeightedRow::new(
                vec![SqliteValue::Text("users".into()), SqliteValue::Integer(10)],
                1,
            ),
            WeightedRow::new(
                vec![SqliteValue::Text("users".into()), SqliteValue::Integer(10)],
                1,
            ),
            WeightedRow::new(
                vec![SqliteValue::Text("users".into()), SqliteValue::Integer(11)],
                -1,
            ),
            WeightedRow::new(
                vec![SqliteValue::Text("orders".into()), SqliteValue::Integer(10)],
                1,
            ),
        ];

        let output = automaton.execute(&rows).expect("execute automaton");
        assert_eq!(
            output,
            vec![
                WeightedRow::new(vec![SqliteValue::Integer(10)], 2),
                WeightedRow::new(vec![SqliteValue::Integer(11)], -1),
            ]
        );
    }

    #[test]
    fn differential_automaton_rejects_bad_projection_column() {
        let automaton =
            DifferentialAutomaton::new(vec![DifferentialOperator::Project { columns: vec![2] }]);
        let rows = vec![WeightedRow::new(vec![SqliteValue::Integer(1)], 1)];

        assert_eq!(
            automaton.execute(&rows),
            Err(DifferentialPlanError::ColumnOutOfBounds {
                column: 2,
                width: 1,
            })
        );
    }

    #[test]
    fn differential_automaton_rejects_bad_filter_column() {
        let automaton = DifferentialAutomaton::new(vec![DifferentialOperator::FilterEq {
            column: 1,
            value: SqliteValue::Integer(1),
        }]);
        let rows = vec![WeightedRow::new(vec![SqliteValue::Integer(1)], 1)];

        assert_eq!(
            automaton.execute(&rows),
            Err(DifferentialPlanError::ColumnOutOfBounds {
                column: 1,
                width: 1,
            })
        );
    }

    #[test]
    fn differential_automaton_consolidate_by_key_elides_zero_weights() {
        let automaton = DifferentialAutomaton::new(vec![DifferentialOperator::ConsolidateByKey {
            key_columns: vec![0],
        }]);
        let rows = vec![
            WeightedRow::new(vec![SqliteValue::Integer(7)], 1),
            WeightedRow::new(vec![SqliteValue::Integer(7)], -1),
        ];

        assert_eq!(automaton.execute(&rows).unwrap(), Vec::<WeightedRow>::new());
    }

    #[test]
    fn differential_automaton_ignores_zero_weight_input_rows() {
        let automaton =
            DifferentialAutomaton::new(vec![DifferentialOperator::Project { columns: vec![0] }]);
        let rows = vec![WeightedRow::new(vec![SqliteValue::Integer(7)], 0)];

        assert_eq!(automaton.execute(&rows).unwrap(), Vec::<WeightedRow>::new());
    }

    #[test]
    fn differential_automaton_delta_join_left_can_chain_into_projection() {
        let automaton = DifferentialAutomaton::new(vec![
            DifferentialOperator::DeltaJoinLeft {
                stable_right: vec![WeightedRow::new(
                    vec![SqliteValue::Integer(1), SqliteValue::Text("admin".into())],
                    3,
                )],
                key_spec: JoinKeySpec::new(vec![0], vec![0]),
            },
            DifferentialOperator::Project {
                columns: vec![1, 3],
            },
        ]);
        let rows = vec![WeightedRow::new(
            vec![SqliteValue::Integer(1), SqliteValue::Text("alice".into())],
            1,
        )];

        assert_eq!(
            automaton.execute(&rows).unwrap(),
            vec![WeightedRow::new(
                vec![
                    SqliteValue::Text("alice".into()),
                    SqliteValue::Text("admin".into()),
                ],
                3,
            )]
        );
    }

    #[test]
    fn differential_automaton_delta_join_right_can_chain_into_projection() {
        let automaton = DifferentialAutomaton::new(vec![
            DifferentialOperator::DeltaJoinRight {
                stable_left: vec![WeightedRow::new(
                    vec![SqliteValue::Integer(1), SqliteValue::Text("alice".into())],
                    2,
                )],
                key_spec: JoinKeySpec::new(vec![0], vec![0]),
            },
            DifferentialOperator::Project {
                columns: vec![1, 3],
            },
        ]);
        let rows = vec![WeightedRow::new(
            vec![SqliteValue::Integer(1), SqliteValue::Text("guest".into())],
            -1,
        )];

        assert_eq!(
            automaton.execute(&rows).unwrap(),
            vec![WeightedRow::new(
                vec![
                    SqliteValue::Text("alice".into()),
                    SqliteValue::Text("guest".into()),
                ],
                -2,
            )]
        );
    }

    #[test]
    fn differential_automaton_delta_join_operator_propagates_key_errors() {
        let automaton = DifferentialAutomaton::new(vec![DifferentialOperator::DeltaJoinLeft {
            stable_right: vec![WeightedRow::new(vec![SqliteValue::Integer(1)], 1)],
            key_spec: JoinKeySpec::new(vec![0], vec![1]),
        }]);
        let rows = vec![WeightedRow::new(vec![SqliteValue::Integer(1)], 1)];

        assert_eq!(
            automaton.execute(&rows),
            Err(DifferentialPlanError::ColumnOutOfBounds {
                column: 1,
                width: 1,
            })
        );
    }

    #[test]
    fn differential_automaton_bootstraps_projection_from_vdbe_ops() {
        let schema = bloodstream_test_schema();
        let program = compile_select("SELECT name FROM users;", &schema);
        let automaton =
            DifferentialAutomaton::from_vdbe_ops(program.ops(), schema[0].columns.len()).unwrap();
        let rows = vec![
            WeightedRow::new(
                vec![SqliteValue::Integer(1), SqliteValue::Text("alice".into())],
                1,
            ),
            WeightedRow::new(
                vec![SqliteValue::Integer(2), SqliteValue::Text("bob".into())],
                -1,
            ),
        ];

        assert_eq!(
            automaton.execute(&rows).unwrap(),
            vec![
                WeightedRow::new(vec![SqliteValue::Text("alice".into())], 1),
                WeightedRow::new(vec![SqliteValue::Text("bob".into())], -1),
            ]
        );
    }

    #[test]
    fn differential_automaton_bootstraps_filter_eq_from_vdbe_ops() {
        let schema = bloodstream_test_schema();
        let program = compile_select("SELECT name FROM users WHERE id = 2;", &schema);
        let automaton =
            DifferentialAutomaton::from_vdbe_ops(program.ops(), schema[0].columns.len()).unwrap();
        let rows = vec![
            WeightedRow::new(
                vec![SqliteValue::Integer(1), SqliteValue::Text("alice".into())],
                1,
            ),
            WeightedRow::new(
                vec![SqliteValue::Integer(2), SqliteValue::Text("bob".into())],
                1,
            ),
        ];

        assert_eq!(
            automaton.execute(&rows).unwrap(),
            vec![WeightedRow::new(vec![SqliteValue::Text("bob".into())], 1,)]
        );
    }

    #[test]
    fn differential_automaton_bootstrap_rejects_unsupported_sorter_program() {
        let schema = bloodstream_test_schema();
        let program = compile_select("SELECT name FROM users ORDER BY name;", &schema);

        assert!(matches!(
            DifferentialAutomaton::from_vdbe_ops(program.ops(), schema[0].columns.len()),
            Err(DifferentialPlanError::UnsupportedProgram { .. })
        ));
    }

    #[test]
    fn differential_automaton_bootstrap_detects_schema_width_drift() {
        let schema = bloodstream_test_schema();
        let program = compile_select("SELECT name FROM users;", &schema);
        let automaton =
            DifferentialAutomaton::from_vdbe_ops(program.ops(), schema[0].columns.len()).unwrap();
        let rows = vec![WeightedRow::new(
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Text("alice".into()),
                SqliteValue::Text("unexpected".into()),
            ],
            1,
        )];

        assert_eq!(
            automaton.execute(&rows),
            Err(DifferentialPlanError::SchemaChanged {
                expected_width: 2,
                actual_width: 3,
            })
        );
    }

    #[test]
    fn differential_automaton_bootstrap_rejects_null_equality_filter() {
        let schema = bloodstream_test_schema();
        let program = compile_select("SELECT name FROM users WHERE id = NULL;", &schema);

        assert!(matches!(
            DifferentialAutomaton::from_vdbe_ops(program.ops(), schema[0].columns.len()),
            Err(DifferentialPlanError::UnsupportedProgram { .. })
        ));
    }

    #[test]
    fn differential_automaton_bootstraps_inline_four_opcode_filter_eq() {
        let ops = vec![
            VdbeOp {
                opcode: Opcode::Init,
                p1: 0,
                p2: 1,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::OpenRead,
                p1: 0,
                p2: 2,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Rewind,
                p1: 0,
                p2: 10,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Column,
                p1: 0,
                p2: 0,
                p3: 1,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Integer,
                p1: 2,
                p2: 2,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::IsNull,
                p1: 2,
                p2: 9,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Ne,
                p1: 2,
                p2: 9,
                p3: 1,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Column,
                p1: 0,
                p2: 1,
                p3: 3,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::ResultRow,
                p1: 3,
                p2: 1,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Next,
                p1: 0,
                p2: 3,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Halt,
                p1: 0,
                p2: 0,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
        ];
        let automaton = DifferentialAutomaton::from_vdbe_ops(&ops, 2).unwrap();
        let rows = vec![
            WeightedRow::new(
                vec![SqliteValue::Integer(1), SqliteValue::Text("alice".into())],
                1,
            ),
            WeightedRow::new(
                vec![SqliteValue::Integer(2), SqliteValue::Text("bob".into())],
                1,
            ),
        ];
        assert_eq!(
            automaton.execute(&rows).unwrap(),
            vec![WeightedRow::new(vec![SqliteValue::Text("bob".into())], 1)]
        );
    }

    #[test]
    fn differential_automaton_bootstrap_rejects_malformed_hoisted_filters() {
        // Negative control 1: 2-opcode filter whose literal register is never
        // loaded in the Init prologue must be rejected.
        let missing_prologue_ops = vec![
            VdbeOp {
                opcode: Opcode::Init,
                p1: 0,
                p2: 1,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::OpenRead,
                p1: 0,
                p2: 2,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Rewind,
                p1: 0,
                p2: 8,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Column,
                p1: 0,
                p2: 0,
                p3: 1,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Ne,
                p1: 9,
                p2: 7,
                p3: 1,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Column,
                p1: 0,
                p2: 1,
                p3: 3,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::ResultRow,
                p1: 3,
                p2: 1,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Next,
                p1: 0,
                p2: 3,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Halt,
                p1: 0,
                p2: 0,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
        ];
        assert!(matches!(
            DifferentialAutomaton::from_vdbe_ops(&missing_prologue_ops, 2),
            Err(DifferentialPlanError::UnsupportedProgram { .. })
        ));

        // Negative control 2: Init prologue loading NULL into the filter
        // literal register must be rejected.
        let null_prologue_ops = vec![
            VdbeOp {
                opcode: Opcode::Init,
                p1: 0,
                p2: 9,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::OpenRead,
                p1: 0,
                p2: 2,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Rewind,
                p1: 0,
                p2: 8,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Column,
                p1: 0,
                p2: 0,
                p3: 1,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Ne,
                p1: 9,
                p2: 7,
                p3: 1,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Column,
                p1: 0,
                p2: 1,
                p3: 3,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::ResultRow,
                p1: 3,
                p2: 1,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Next,
                p1: 0,
                p2: 3,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Halt,
                p1: 0,
                p2: 0,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Null,
                p1: 0,
                p2: 9,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Goto,
                p1: 0,
                p2: 1,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Halt,
                p1: 0,
                p2: 0,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
        ];
        assert!(matches!(
            DifferentialAutomaton::from_vdbe_ops(&null_prologue_ops, 2),
            Err(DifferentialPlanError::UnsupportedProgram { .. })
        ));

        // Negative control 3: 3-opcode filter slice must be rejected.
        let three_opcode_ops = vec![
            VdbeOp {
                opcode: Opcode::Init,
                p1: 0,
                p2: 1,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::OpenRead,
                p1: 0,
                p2: 2,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Rewind,
                p1: 0,
                p2: 9,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Column,
                p1: 0,
                p2: 0,
                p3: 1,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Integer,
                p1: 2,
                p2: 2,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Ne,
                p1: 2,
                p2: 8,
                p3: 1,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Column,
                p1: 0,
                p2: 1,
                p3: 3,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::ResultRow,
                p1: 3,
                p2: 1,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Next,
                p1: 0,
                p2: 3,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Halt,
                p1: 0,
                p2: 0,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
        ];
        assert!(matches!(
            DifferentialAutomaton::from_vdbe_ops(&three_opcode_ops, 2),
            Err(DifferentialPlanError::UnsupportedProgram { .. })
        ));
    }

    #[test]
    fn delta_join_left_multiplies_weights_and_concatenates_rows() {
        let delta_left = vec![
            WeightedRow::new(
                vec![SqliteValue::Integer(1), SqliteValue::Text("alice".into())],
                1,
            ),
            WeightedRow::new(
                vec![SqliteValue::Integer(2), SqliteValue::Text("bob".into())],
                -1,
            ),
        ];
        let stable_right = vec![
            WeightedRow::new(
                vec![SqliteValue::Integer(1), SqliteValue::Text("admin".into())],
                3,
            ),
            WeightedRow::new(
                vec![SqliteValue::Integer(2), SqliteValue::Text("user".into())],
                2,
            ),
        ];
        let key_spec = JoinKeySpec::new(vec![0], vec![0]);

        let joined = delta_join_left(&delta_left, &stable_right, &key_spec).unwrap();
        assert_eq!(
            joined,
            vec![
                WeightedRow::new(
                    vec![
                        SqliteValue::Integer(1),
                        SqliteValue::Text("alice".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Text("admin".into()),
                    ],
                    3,
                ),
                WeightedRow::new(
                    vec![
                        SqliteValue::Integer(2),
                        SqliteValue::Text("bob".into()),
                        SqliteValue::Integer(2),
                        SqliteValue::Text("user".into()),
                    ],
                    -2,
                ),
            ]
        );
    }

    #[test]
    fn delta_join_left_ignores_zero_weight_rows() {
        let delta_left = vec![WeightedRow::new(vec![SqliteValue::Integer(1)], 0)];
        let stable_right = vec![WeightedRow::new(vec![SqliteValue::Integer(1)], 1)];
        let key_spec = JoinKeySpec::new(vec![0], vec![0]);

        assert_eq!(
            delta_join_left(&delta_left, &stable_right, &key_spec).unwrap(),
            Vec::<WeightedRow>::new()
        );
    }

    #[test]
    fn delta_join_left_ignores_zero_weight_stable_rows() {
        let delta_left = vec![WeightedRow::new(vec![SqliteValue::Integer(1)], 1)];
        let stable_right = vec![WeightedRow::new(vec![SqliteValue::Integer(1)], 0)];
        let key_spec = JoinKeySpec::new(vec![0], vec![0]);

        assert_eq!(
            delta_join_left(&delta_left, &stable_right, &key_spec).unwrap(),
            Vec::<WeightedRow>::new()
        );
    }

    #[test]
    fn delta_join_right_uses_delta_weight_from_right_relation() {
        let stable_left = vec![
            WeightedRow::new(
                vec![SqliteValue::Integer(1), SqliteValue::Text("alice".into())],
                2,
            ),
            WeightedRow::new(
                vec![SqliteValue::Integer(2), SqliteValue::Text("bob".into())],
                3,
            ),
        ];
        let delta_right = vec![
            WeightedRow::new(
                vec![SqliteValue::Integer(2), SqliteValue::Text("pro".into())],
                1,
            ),
            WeightedRow::new(
                vec![SqliteValue::Integer(1), SqliteValue::Text("guest".into())],
                -1,
            ),
        ];
        let key_spec = JoinKeySpec::new(vec![0], vec![0]);

        let joined = delta_join_right(&stable_left, &delta_right, &key_spec).unwrap();
        assert_eq!(
            joined,
            vec![
                WeightedRow::new(
                    vec![
                        SqliteValue::Integer(2),
                        SqliteValue::Text("bob".into()),
                        SqliteValue::Integer(2),
                        SqliteValue::Text("pro".into()),
                    ],
                    3,
                ),
                WeightedRow::new(
                    vec![
                        SqliteValue::Integer(1),
                        SqliteValue::Text("alice".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Text("guest".into()),
                    ],
                    -2,
                ),
            ]
        );
    }

    #[test]
    fn delta_join_rejects_mismatched_key_arity() {
        let delta_left = vec![WeightedRow::new(vec![SqliteValue::Integer(1)], 1)];
        let stable_right = vec![WeightedRow::new(vec![SqliteValue::Integer(1)], 1)];
        let key_spec = JoinKeySpec::new(vec![0], vec![0, 1]);

        assert_eq!(
            delta_join_left(&delta_left, &stable_right, &key_spec),
            Err(DifferentialPlanError::JoinKeyArityMismatch { left: 1, right: 2 })
        );
    }

    #[test]
    fn delta_join_left_rejects_out_of_bounds_key_column() {
        let delta_left = vec![WeightedRow::new(vec![SqliteValue::Integer(1)], 1)];
        let stable_right = vec![WeightedRow::new(vec![SqliteValue::Integer(1)], 1)];
        let key_spec = JoinKeySpec::new(vec![0], vec![1]);

        assert_eq!(
            delta_join_left(&delta_left, &stable_right, &key_spec),
            Err(DifferentialPlanError::ColumnOutOfBounds {
                column: 1,
                width: 1,
            })
        );
    }

    #[test]
    fn coalesce_insert_then_delete_cancels() {
        let deltas = vec![
            AlgebraicDelta {
                source_table: "t".to_string(),
                row_id: 1,
                kind: DeltaKind::Insert,
                affected_columns: vec![0],
                seq: 0,
            },
            AlgebraicDelta {
                source_table: "t".to_string(),
                row_id: 1,
                kind: DeltaKind::Delete,
                affected_columns: vec![],
                seq: 1,
            },
        ];
        let coalesced = coalesce_deltas(&deltas);
        assert!(coalesced.is_empty(), "INSERT+DELETE should cancel");
    }

    #[test]
    fn coalesce_preserves_first_appearance_order() {
        let deltas = vec![
            AlgebraicDelta {
                source_table: "t".to_string(),
                row_id: 2,
                kind: DeltaKind::Update,
                affected_columns: vec![0],
                seq: 11,
            },
            AlgebraicDelta {
                source_table: "t".to_string(),
                row_id: 1,
                kind: DeltaKind::Update,
                affected_columns: vec![1],
                seq: 12,
            },
        ];

        let coalesced = coalesce_deltas(&deltas);
        assert_eq!(coalesced.len(), 2);
        assert_eq!(coalesced[0].row_id, 2);
        assert_eq!(coalesced[0].seq, 0);
        assert_eq!(coalesced[1].row_id, 1);
        assert_eq!(coalesced[1].seq, 1);
    }

    #[test]
    fn propagation_config_defaults() {
        let config = PropagationConfig::default();
        assert!(config.max_batch_size > 0);
        assert!(config.target_latency_us > 0);
        assert!(config.coalesce_row_deltas);
        assert!(config.max_active_bindings > 0);
    }

    #[test]
    fn schema_version_stable() {
        assert_eq!(BLOODSTREAM_SCHEMA_VERSION, 1);
    }
}
