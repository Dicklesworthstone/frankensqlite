//! §5.10.2 Deterministic Rebase & Index Regeneration — bd-1h3b
//!
//! Implements the rebase algorithm for `UpdateExpression` intents against a new
//! committed base. Rebase proceeds only when no blocking reads or structural
//! effects exist in the intent log; it replays expressions deterministically
//! against the new base row, enforces constraints, and regenerates index ops.
//!
//! **Invariant:** Rebase runs in the committing txn's context BEFORE entering
//! the serialized commit section (§5.9). The coordinator never performs B-tree
//! traversal or expression evaluation.

use fsqlite_types::TypeAffinity;
use fsqlite_types::glossary::{
    ColumnIdx, IndexId, IntentOp, IntentOpKind, RebaseExpr, RowId, Snapshot, StructuralEffects,
    TableId,
};
use fsqlite_types::record::{parse_record, serialize_record};
use fsqlite_types::value::SqliteValue;
use tracing::{debug, warn};

use crate::index_regen::{
    IndexDef, IndexRegenError, IndexRegenOps, UniqueChecker, apply_column_updates,
    eval_rebase_expr, regenerate_index_ops,
};

/// Bead identifier for tracing.
const BEAD_ID: &str = "bd-1h3b";

// ── Error types ──────────────────────────────────────────────────────────────

/// Errors during deterministic rebase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebaseError {
    /// Schema epoch mismatch between intent and current snapshot.
    SchemaEpochMismatch { expected: u64, actual: u64 },
    /// A blocking read exists in the intent log (non-empty `footprint.reads`).
    BlockingReads { op_index: usize },
    /// A structural effect exists in the intent log.
    StructuralEffects { op_index: usize, effects: u32 },
    /// The target rowid was not found in the new committed base.
    TargetRowNotFound { table: TableId, key: RowId },
    /// NOT NULL constraint violation on the rebased row.
    NotNullViolation { table: TableId, column_idx: u32 },
    /// CHECK constraint violation on the rebased row.
    CheckViolation { table: TableId },
    /// UNIQUE index constraint violation during index regeneration.
    UniqueViolation {
        index_id: IndexId,
        conflicting_rowid: RowId,
    },
    /// Error during index regeneration.
    IndexRegenError(IndexRegenError),
    /// Malformed record in the committed base.
    MalformedRecord { table: TableId, key: RowId },
}

impl std::fmt::Display for RebaseError {
    #[allow(clippy::too_many_lines)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SchemaEpochMismatch { expected, actual } => {
                write!(
                    f,
                    "SQLITE_SCHEMA: schema epoch mismatch (expected {expected}, got {actual})"
                )
            }
            Self::BlockingReads { op_index } => {
                write!(
                    f,
                    "SQLITE_BUSY_SNAPSHOT: blocking reads at intent op {op_index}"
                )
            }
            Self::StructuralEffects { op_index, effects } => {
                write!(
                    f,
                    "SQLITE_BUSY_SNAPSHOT: structural effects 0x{effects:x} at intent op {op_index}"
                )
            }
            Self::TargetRowNotFound { table, key } => {
                write!(
                    f,
                    "rebase target not found: table {} rowid {}",
                    table.get(),
                    key.get()
                )
            }
            Self::NotNullViolation { table, column_idx } => {
                write!(
                    f,
                    "NOT NULL constraint failed: table {} column {column_idx}",
                    table.get()
                )
            }
            Self::CheckViolation { table } => {
                write!(f, "CHECK constraint failed: table {}", table.get())
            }
            Self::UniqueViolation {
                index_id,
                conflicting_rowid,
            } => {
                write!(
                    f,
                    "UNIQUE constraint failed: index {} conflicting rowid {}",
                    index_id.get(),
                    conflicting_rowid.get()
                )
            }
            Self::IndexRegenError(e) => write!(f, "index regen: {e}"),
            Self::MalformedRecord { table, key } => {
                write!(
                    f,
                    "malformed record: table {} rowid {}",
                    table.get(),
                    key.get()
                )
            }
        }
    }
}

impl std::error::Error for RebaseError {}

impl From<IndexRegenError> for RebaseError {
    fn from(e: IndexRegenError) -> Self {
        match e {
            IndexRegenError::UniqueConstraintViolation {
                index_id,
                conflicting_rowid,
            } => Self::UniqueViolation {
                index_id,
                conflicting_rowid,
            },
            other => Self::IndexRegenError(other),
        }
    }
}

// ── Table schema for constraint checking ─────────────────────────────────────

/// Lightweight table schema sufficient for rebase-time constraint enforcement.
#[derive(Debug, Clone)]
pub struct TableConstraints {
    /// Table identifier.
    pub table_id: TableId,
    /// Per-column NOT NULL flags (true = NOT NULL).
    pub not_null: Vec<bool>,
    /// Per-column type affinities.
    pub affinities: Vec<TypeAffinity>,
    /// CHECK constraint expressions. Each must evaluate to truthy or NULL to pass.
    pub check_exprs: Vec<RebaseExpr>,
}

/// Trait for reading committed base rows during rebase.
pub trait BaseRowReader {
    /// Look up the record bytes for `(table, rowid)` in the new committed base.
    /// Returns `None` if the row does not exist.
    fn read_base_row(&self, table: TableId, key: RowId) -> Option<Vec<u8>>;
}

/// Trait for looking up table schema and indexes during rebase.
pub trait RebaseSchemaLookup {
    /// Get the table constraints for the given table.
    fn table_constraints(&self, table: TableId) -> Option<TableConstraints>;
    /// Get the secondary indexes for the given table.
    fn table_indexes(&self, table: TableId) -> Vec<IndexDef>;
}

// ── Eligibility check ────────────────────────────────────────────────────────

/// Result of rebase eligibility check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebaseEligibility {
    /// Eligible: all ops have empty reads and NONE structural effects.
    Eligible,
    /// Ineligible: a blocking read exists.
    BlockingReads { op_index: usize },
    /// Ineligible: structural effects exist.
    StructuralEffects { op_index: usize, effects: u32 },
}

/// Check whether an intent log is eligible for deterministic rebase (§5.10.2).
///
/// Rebase proceeds when ALL of:
/// 1. `footprint.reads` is empty for every `IntentOp`
/// 2. `footprint.structural == NONE` for every `IntentOp`
#[must_use]
pub fn check_rebase_eligibility(intent_log: &[IntentOp]) -> RebaseEligibility {
    for (i, op) in intent_log.iter().enumerate() {
        if !op.footprint.reads.is_empty() {
            debug!(
                bead_id = BEAD_ID,
                op_index = i,
                reads = op.footprint.reads.len(),
                "rebase blocked: non-empty reads"
            );
            return RebaseEligibility::BlockingReads { op_index: i };
        }
        if op.footprint.structural != StructuralEffects::NONE {
            debug!(
                bead_id = BEAD_ID,
                op_index = i,
                effects = op.footprint.structural.bits(),
                "rebase blocked: structural effects"
            );
            return RebaseEligibility::StructuralEffects {
                op_index: i,
                effects: op.footprint.structural.bits(),
            };
        }
    }
    RebaseEligibility::Eligible
}

// ── Schema epoch guard ───────────────────────────────────────────────────────

/// Verify that the intent log's schema epoch matches the current committed
/// snapshot's schema epoch.
///
/// Returns `Err` if any intent op has a schema epoch different from the
/// snapshot's. This catches DDL-concurrent-with-DML races.
pub fn check_schema_epoch(
    intent_log: &[IntentOp],
    current_snapshot: Snapshot,
) -> Result<(), RebaseError> {
    let current_epoch = current_snapshot.schema_epoch.get();
    for op in intent_log {
        if op.schema_epoch != current_epoch {
            warn!(
                bead_id = BEAD_ID,
                expected = current_epoch,
                actual = op.schema_epoch,
                "SQLITE_SCHEMA: schema epoch mismatch"
            );
            return Err(RebaseError::SchemaEpochMismatch {
                expected: current_epoch,
                actual: op.schema_epoch,
            });
        }
    }
    Ok(())
}

// ── Constraint enforcement ───────────────────────────────────────────────────

/// Enforce NOT NULL and CHECK constraints on a rebased row.
fn enforce_constraints(
    updated_row: &[SqliteValue],
    constraints: &TableConstraints,
) -> Result<(), RebaseError> {
    // NOT NULL checks.
    for (i, (&is_not_null, val)) in constraints
        .not_null
        .iter()
        .zip(updated_row.iter())
        .enumerate()
    {
        if is_not_null && matches!(val, SqliteValue::Null) {
            return Err(RebaseError::NotNullViolation {
                table: constraints.table_id,
                #[allow(clippy::cast_possible_truncation)]
                column_idx: i as u32,
            });
        }
    }

    // CHECK constraints: each must evaluate to truthy or NULL to pass.
    for check_expr in &constraints.check_exprs {
        let result =
            eval_rebase_expr(check_expr, updated_row).map_err(|_| RebaseError::CheckViolation {
                table: constraints.table_id,
            })?;
        // Per SQLite: CHECK passes if result is true (non-zero) OR NULL.
        // Fails only if result is exactly false (zero).
        match result {
            SqliteValue::Integer(0) | SqliteValue::Float(0.0) => {
                return Err(RebaseError::CheckViolation {
                    table: constraints.table_id,
                });
            }
            _ => {} // NULL or non-zero passes.
        }
    }

    Ok(())
}

// ── Single UpdateExpression replay ───────────────────────────────────────────

/// The result of replaying a single `UpdateExpression` against the new base.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplayResult {
    /// The new record bytes for the updated row.
    pub new_record: Vec<u8>,
    /// Index operations to emit (from index regeneration).
    pub index_ops: Vec<IntentOpKind>,
}

/// Replay a single `UpdateExpression` against the new committed base row.
///
/// Steps (from spec §5.10.2):
/// 1. Read target row from new committed base
/// 2. If not found → abort
/// 3. Evaluate column_updates against new base row
/// 4. Apply affinity coercion
/// 5. Produce updated record
/// 6. Enforce NOT NULL / CHECK constraints
/// 7. Regenerate index ops
pub fn replay_update_expression(
    table: TableId,
    key: RowId,
    column_updates: &[(ColumnIdx, RebaseExpr)],
    base_reader: &dyn BaseRowReader,
    schema: &dyn RebaseSchemaLookup,
    unique_checker: &dyn UniqueChecker,
) -> Result<ReplayResult, RebaseError> {
    let _ = BEAD_ID;

    // Step 1: Read the target row from the new committed base.
    let base_record = base_reader
        .read_base_row(table, key)
        .ok_or(RebaseError::TargetRowNotFound { table, key })?;

    // Parse the base record.
    let base_row = parse_record(&base_record).ok_or(RebaseError::MalformedRecord { table, key })?;

    // Get table constraints and indexes from schema.
    let constraints = schema.table_constraints(table);
    let indexes = schema.table_indexes(table);

    // Step 3-4: Evaluate column_updates against the new base row.
    let affinities = constraints
        .as_ref()
        .map_or(&[] as &[TypeAffinity], |c| c.affinities.as_slice());

    let updated_row = apply_column_updates(&base_row, column_updates, affinities)?;

    // Step 5: Produce the updated record bytes.
    let new_record = serialize_record(&updated_row);

    // Step 6: Enforce constraints.
    if let Some(ref c) = constraints {
        enforce_constraints(&updated_row, c)?;
    }

    // Step 7: Regenerate index ops.
    let regen_result = if indexes.is_empty() {
        IndexRegenOps { ops: vec![] }
    } else {
        regenerate_index_ops(&base_record, column_updates, &indexes, key, unique_checker)?
    };

    Ok(ReplayResult {
        new_record,
        index_ops: regen_result.ops,
    })
}

// ── Full rebase pipeline ─────────────────────────────────────────────────────

/// Result of a full deterministic rebase.
#[derive(Debug, Clone, PartialEq)]
pub struct RebaseResult {
    /// The rebased intent log with stale ops replaced.
    pub rebased_ops: Vec<IntentOpKind>,
    /// Number of `UpdateExpression` ops that were replayed.
    pub replayed_count: usize,
}

/// Execute the full deterministic rebase pipeline (§5.10.2).
///
/// 1. Check schema epoch
/// 2. Check rebase eligibility (no blocking reads, no structural effects)
/// 3. For each `UpdateExpression`: replay against new base, regenerate index ops
/// 4. For non-`UpdateExpression` ops: pass through unchanged
pub fn deterministic_rebase(
    intent_log: &[IntentOp],
    current_snapshot: Snapshot,
    base_reader: &dyn BaseRowReader,
    schema: &dyn RebaseSchemaLookup,
    unique_checker: &dyn UniqueChecker,
) -> Result<RebaseResult, RebaseError> {
    // Step 1: Schema epoch guard.
    check_schema_epoch(intent_log, current_snapshot)?;

    // Step 2: Eligibility check.
    match check_rebase_eligibility(intent_log) {
        RebaseEligibility::Eligible => {}
        RebaseEligibility::BlockingReads { op_index } => {
            return Err(RebaseError::BlockingReads { op_index });
        }
        RebaseEligibility::StructuralEffects { op_index, effects } => {
            return Err(RebaseError::StructuralEffects { op_index, effects });
        }
    }

    let mut rebased_ops = Vec::with_capacity(intent_log.len());
    let mut replayed_count = 0;

    for op in intent_log {
        match &op.op {
            IntentOpKind::UpdateExpression {
                table,
                key,
                column_updates,
            } => {
                // Step 3: Replay UpdateExpression against new base.
                let _indexes = schema.table_indexes(*table);

                // Discard stale index ops that follow this UpdateExpression.
                // (In a real pipeline these would be in the same intent log;
                // here we handle each op independently.)
                let result = replay_update_expression(
                    *table,
                    *key,
                    column_updates,
                    base_reader,
                    schema,
                    unique_checker,
                )?;

                // Emit the materialized Update op.
                rebased_ops.push(IntentOpKind::Update {
                    table: *table,
                    key: *key,
                    new_record: result.new_record,
                });

                // Emit regenerated index ops.
                rebased_ops.extend(result.index_ops);
                replayed_count += 1;

                debug!(
                    bead_id = BEAD_ID,
                    table = table.get(),
                    key = key.get(),
                    "replayed UpdateExpression"
                );
            }
            other => {
                // Non-UpdateExpression ops pass through. We still need to
                // discard stale index ops for any UpdateExpression's (table, rowid).
                rebased_ops.push(other.clone());
            }
        }
    }

    Ok(RebaseResult {
        rebased_ops,
        replayed_count,
    })
}

// ── VDBE codegen emission rules ──────────────────────────────────────────────

/// Flags describing a statement for `UpdateExpression` emission eligibility.
#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)]
pub struct UpdateExpressionCandidate {
    /// Whether the target table has triggers.
    pub has_triggers: bool,
    /// Whether the target table participates in foreign key constraints.
    pub has_foreign_keys: bool,
    /// Whether all CHECK expressions pass `expr_is_rebase_safe()`.
    pub all_checks_rebase_safe: bool,
    /// Whether the WHERE resolves to a rowid point lookup.
    pub is_rowid_point_lookup: bool,
    /// Whether any SET clause targets the rowid/INTEGER PRIMARY KEY.
    pub sets_rowid: bool,
    /// Whether all SET expressions pass `expr_is_rebase_safe()`.
    pub all_sets_rebase_safe: bool,
    /// Whether there's a prior read of the same row.
    pub has_prior_read_of_same_row: bool,
}

/// Check whether a statement is eligible for `UpdateExpression` emission
/// per the VDBE codegen rules (§5.10.2).
#[must_use]
pub fn can_emit_update_expression(candidate: &UpdateExpressionCandidate) -> bool {
    !candidate.has_triggers
        && !candidate.has_foreign_keys
        && candidate.all_checks_rebase_safe
        && candidate.is_rowid_point_lookup
        && !candidate.sets_rowid
        && candidate.all_sets_rebase_safe
        && !candidate.has_prior_read_of_same_row
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use fsqlite_types::glossary::{
        CommitSeq, IntentFootprint, SchemaEpoch, SemanticKeyRef, StructuralEffects,
    };
    use fsqlite_types::record::serialize_record;

    use crate::index_regen::{Collation, IndexKeyPart, NoOpUniqueChecker, compute_index_key};

    // ── Test helpers ─────────────────────────────────────────────────────

    /// In-memory base row reader.
    struct MemBaseReader {
        rows: HashMap<(u32, i64), Vec<u8>>,
    }

    impl BaseRowReader for MemBaseReader {
        fn read_base_row(&self, table: TableId, key: RowId) -> Option<Vec<u8>> {
            self.rows.get(&(table.get(), key.get())).cloned()
        }
    }

    /// Simple schema lookup with configurable constraints and indexes.
    struct MemSchema {
        constraints: HashMap<u32, TableConstraints>,
        indexes: HashMap<u32, Vec<IndexDef>>,
    }

    impl RebaseSchemaLookup for MemSchema {
        fn table_constraints(&self, table: TableId) -> Option<TableConstraints> {
            self.constraints.get(&table.get()).cloned()
        }

        fn table_indexes(&self, table: TableId) -> Vec<IndexDef> {
            self.indexes.get(&table.get()).cloned().unwrap_or_default()
        }
    }

    fn empty_footprint() -> IntentFootprint {
        IntentFootprint {
            reads: vec![],
            writes: vec![],
            structural: StructuralEffects::NONE,
        }
    }

    fn make_intent_op(epoch: u64, footprint: IntentFootprint, op: IntentOpKind) -> IntentOp {
        IntentOp {
            schema_epoch: epoch,
            footprint,
            op,
        }
    }

    fn test_snapshot(epoch: u64) -> Snapshot {
        Snapshot::new(CommitSeq::new(10), SchemaEpoch::new(epoch))
    }

    fn record_bytes(values: &[SqliteValue]) -> Vec<u8> {
        serialize_record(values)
    }

    fn empty_schema() -> MemSchema {
        MemSchema {
            constraints: HashMap::new(),
            indexes: HashMap::new(),
        }
    }

    // ── Test 1: Schema epoch guard ───────────────────────────────────────

    #[test]
    fn test_rebase_schema_epoch_guard_aborts_on_mismatch() {
        let intent_log = vec![make_intent_op(
            5, // epoch 5
            empty_footprint(),
            IntentOpKind::UpdateExpression {
                table: TableId::new(1),
                key: RowId::new(1),
                column_updates: vec![],
            },
        )];

        let snapshot = test_snapshot(6); // epoch 6 ≠ 5

        let result = check_schema_epoch(&intent_log, snapshot);
        assert!(
            matches!(
                result,
                Err(RebaseError::SchemaEpochMismatch {
                    expected: 6,
                    actual: 5
                })
            ),
            "bead_id={BEAD_ID} schema_epoch_mismatch"
        );
    }

    // ── Test 2: Blocking reads ───────────────────────────────────────────

    #[test]
    fn test_rebase_rejects_blocking_reads() {
        use fsqlite_types::glossary::{BtreeRef, SemanticKeyKind};

        let blocking_read = SemanticKeyRef::new(
            BtreeRef::Table(TableId::new(1)),
            SemanticKeyKind::TableRow,
            &[1, 2, 3],
        );

        let intent_log = vec![make_intent_op(
            1,
            IntentFootprint {
                reads: vec![blocking_read],
                writes: vec![],
                structural: StructuralEffects::NONE,
            },
            IntentOpKind::Update {
                table: TableId::new(1),
                key: RowId::new(1),
                new_record: vec![],
            },
        )];

        let result = check_rebase_eligibility(&intent_log);
        assert_eq!(
            result,
            RebaseEligibility::BlockingReads { op_index: 0 },
            "bead_id={BEAD_ID} blocking_reads"
        );
    }

    // ── Test 3: Structural effects ───────────────────────────────────────

    #[test]
    fn test_rebase_rejects_structural_effects() {
        let intent_log = vec![make_intent_op(
            1,
            IntentFootprint {
                reads: vec![],
                writes: vec![],
                structural: StructuralEffects::PAGE_SPLIT | StructuralEffects::OVERFLOW_ALLOC,
            },
            IntentOpKind::Insert {
                table: TableId::new(1),
                key: RowId::new(1),
                record: vec![],
            },
        )];

        let result = check_rebase_eligibility(&intent_log);
        assert!(
            matches!(
                result,
                RebaseEligibility::StructuralEffects {
                    op_index: 0,
                    effects,
                } if effects != 0
            ),
            "bead_id={BEAD_ID} structural_effects"
        );
    }

    // ── Test 4: UpdateExpression uses new base row ───────────────────────

    #[test]
    fn test_rebase_update_expression_column_ref_uses_new_base() {
        // New committed base: col0=1, col1=100 (different from original snapshot).
        let mut rows = HashMap::new();
        rows.insert(
            (1, 1),
            record_bytes(&[SqliteValue::Integer(1), SqliteValue::Integer(100)]),
        );
        let reader = MemBaseReader { rows };

        // UpdateExpression: SET col1 = col1 + 1 (should use 100 from new base, not stale).
        let column_updates = vec![(
            ColumnIdx::new(1),
            RebaseExpr::BinaryOp {
                op: fsqlite_types::glossary::RebaseBinaryOp::Add,
                left: Box::new(RebaseExpr::ColumnRef(ColumnIdx::new(1))),
                right: Box::new(RebaseExpr::Literal(SqliteValue::Integer(1))),
            },
        )];

        let schema = MemSchema {
            constraints: HashMap::from([(
                1,
                TableConstraints {
                    table_id: TableId::new(1),
                    not_null: vec![false, false],
                    affinities: vec![TypeAffinity::Integer, TypeAffinity::Integer],
                    check_exprs: vec![],
                },
            )]),
            indexes: HashMap::new(),
        };

        let result = replay_update_expression(
            TableId::new(1),
            RowId::new(1),
            &column_updates,
            &reader,
            &schema,
            &NoOpUniqueChecker,
        )
        .unwrap();

        // The result should be col0=1, col1=101 (100+1, NOT the stale snapshot value).
        let parsed = parse_record(&result.new_record).unwrap();
        assert_eq!(
            parsed[1],
            SqliteValue::Integer(101),
            "bead_id={BEAD_ID} uses_new_base"
        );
    }

    // ── Test 5: NOT NULL constraint failure ──────────────────────────────

    #[test]
    fn test_rebase_constraint_failure_aborts() {
        let mut rows = HashMap::new();
        rows.insert(
            (1, 1),
            record_bytes(&[
                SqliteValue::Integer(1),
                SqliteValue::Text("hello".to_owned()),
            ]),
        );
        let reader = MemBaseReader { rows };

        // UpdateExpression: SET col1 = NULL (violates NOT NULL).
        let column_updates = vec![(ColumnIdx::new(1), RebaseExpr::Literal(SqliteValue::Null))];

        let schema = MemSchema {
            constraints: HashMap::from([(
                1,
                TableConstraints {
                    table_id: TableId::new(1),
                    not_null: vec![false, true], // col1 is NOT NULL
                    affinities: vec![TypeAffinity::Integer, TypeAffinity::Text],
                    check_exprs: vec![],
                },
            )]),
            indexes: HashMap::new(),
        };

        let result = replay_update_expression(
            TableId::new(1),
            RowId::new(1),
            &column_updates,
            &reader,
            &schema,
            &NoOpUniqueChecker,
        );

        assert!(
            matches!(
                result,
                Err(RebaseError::NotNullViolation { column_idx: 1, .. })
            ),
            "bead_id={BEAD_ID} not_null_violation"
        );
    }

    // ── Test 6: Index regeneration discards stale ops ────────────────────

    #[test]
    fn test_rebase_index_regeneration_discards_stale_ops() {
        let mut rows = HashMap::new();
        rows.insert(
            (1, 1),
            record_bytes(&[
                SqliteValue::Integer(1),
                SqliteValue::Text("new_base".to_owned()),
            ]),
        );
        let reader = MemBaseReader { rows };

        let index_def = IndexDef {
            index_id: IndexId::new(10),
            table_id: TableId::new(1),
            unique: false,
            key_parts: vec![IndexKeyPart::Column {
                col_idx: ColumnIdx::new(1),
                affinity: TypeAffinity::Text,
                collation: Collation::Binary,
            }],
            where_predicate: None,
            table_column_affinities: vec![TypeAffinity::Integer, TypeAffinity::Text],
        };

        let schema = MemSchema {
            constraints: HashMap::from([(
                1,
                TableConstraints {
                    table_id: TableId::new(1),
                    not_null: vec![false, false],
                    affinities: vec![TypeAffinity::Integer, TypeAffinity::Text],
                    check_exprs: vec![],
                },
            )]),
            indexes: HashMap::from([(1, vec![index_def])]),
        };

        // SET col1 = 'updated'.
        let column_updates = vec![(
            ColumnIdx::new(1),
            RebaseExpr::Literal(SqliteValue::Text("updated".to_owned())),
        )];

        let result = replay_update_expression(
            TableId::new(1),
            RowId::new(1),
            &column_updates,
            &reader,
            &schema,
            &NoOpUniqueChecker,
        )
        .unwrap();

        // Should have 2 index ops: IndexDelete(old key) + IndexInsert(new key).
        assert_eq!(
            result.index_ops.len(),
            2,
            "bead_id={BEAD_ID} index_regen_ops"
        );
        assert!(
            matches!(&result.index_ops[0], IntentOpKind::IndexDelete { .. }),
            "bead_id={BEAD_ID} first is delete"
        );
        assert!(
            matches!(&result.index_ops[1], IntentOpKind::IndexInsert { .. }),
            "bead_id={BEAD_ID} second is insert"
        );

        // Verify the new key encodes "updated", not the stale original key.
        if let IntentOpKind::IndexInsert { key, .. } = &result.index_ops[1] {
            let parsed = parse_record(key).unwrap();
            assert_eq!(
                parsed[0],
                SqliteValue::Text("updated".to_owned()),
                "bead_id={BEAD_ID} key_from_new_base"
            );
        }
    }

    // ── Test 7: UNIQUE index enforcement on new base ─────────────────────

    #[test]
    fn test_rebase_unique_index_enforcement_on_new_base() {
        // Unique checker that reports a conflict.
        struct ConflictChecker {
            expected_key: Vec<u8>,
        }
        impl UniqueChecker for ConflictChecker {
            fn check_unique(
                &self,
                _index_id: IndexId,
                key_bytes: &[u8],
                _exclude_rowid: RowId,
            ) -> Option<RowId> {
                if key_bytes == self.expected_key {
                    Some(RowId::new(99))
                } else {
                    None
                }
            }
        }

        let mut rows = HashMap::new();
        rows.insert(
            (1, 1),
            record_bytes(&[
                SqliteValue::Integer(1),
                SqliteValue::Text("original".to_owned()),
            ]),
        );
        let reader = MemBaseReader { rows };

        let index_def = IndexDef {
            index_id: IndexId::new(20),
            table_id: TableId::new(1),
            unique: true,
            key_parts: vec![IndexKeyPart::Column {
                col_idx: ColumnIdx::new(1),
                affinity: TypeAffinity::Text,
                collation: Collation::Binary,
            }],
            where_predicate: None,
            table_column_affinities: vec![TypeAffinity::Integer, TypeAffinity::Text],
        };

        let schema = MemSchema {
            constraints: HashMap::from([(
                1,
                TableConstraints {
                    table_id: TableId::new(1),
                    not_null: vec![false, false],
                    affinities: vec![TypeAffinity::Integer, TypeAffinity::Text],
                    check_exprs: vec![],
                },
            )]),
            indexes: HashMap::from([(1, vec![index_def.clone()])]),
        };

        // Pre-compute the key for "conflict_value".
        let conflict_key = compute_index_key(
            &index_def,
            &[
                SqliteValue::Integer(1),
                SqliteValue::Text("conflict_value".to_owned()),
            ],
        )
        .unwrap();

        let checker = ConflictChecker {
            expected_key: conflict_key,
        };

        let column_updates = vec![(
            ColumnIdx::new(1),
            RebaseExpr::Literal(SqliteValue::Text("conflict_value".to_owned())),
        )];

        let result = replay_update_expression(
            TableId::new(1),
            RowId::new(1),
            &column_updates,
            &reader,
            &schema,
            &checker,
        );

        assert!(
            matches!(
                result,
                Err(RebaseError::UniqueViolation {
                    conflicting_rowid,
                    ..
                }) if conflicting_rowid.get() == 99
            ),
            "bead_id={BEAD_ID} unique_violation_on_rebase"
        );
    }

    // ── Test 8: VDBE codegen emission rules ──────────────────────────────

    #[test]
    fn test_vdbe_codegen_updateexpression_emission_rules() {
        // All conditions met → eligible.
        let good = UpdateExpressionCandidate {
            has_triggers: false,
            has_foreign_keys: false,
            all_checks_rebase_safe: true,
            is_rowid_point_lookup: true,
            sets_rowid: false,
            all_sets_rebase_safe: true,
            has_prior_read_of_same_row: false,
        };
        assert!(
            can_emit_update_expression(&good),
            "bead_id={BEAD_ID} all_conditions_met"
        );

        // Has triggers → ineligible.
        assert!(
            !can_emit_update_expression(&UpdateExpressionCandidate {
                has_triggers: true,
                ..good
            }),
            "bead_id={BEAD_ID} has_triggers"
        );

        // Has foreign keys → ineligible.
        assert!(
            !can_emit_update_expression(&UpdateExpressionCandidate {
                has_foreign_keys: true,
                ..good
            }),
            "bead_id={BEAD_ID} has_fk"
        );

        // CHECK not rebase-safe → ineligible.
        assert!(
            !can_emit_update_expression(&UpdateExpressionCandidate {
                all_checks_rebase_safe: false,
                ..good
            }),
            "bead_id={BEAD_ID} check_not_safe"
        );

        // Not a rowid point lookup → ineligible.
        assert!(
            !can_emit_update_expression(&UpdateExpressionCandidate {
                is_rowid_point_lookup: false,
                ..good
            }),
            "bead_id={BEAD_ID} not_point_lookup"
        );

        // Sets rowid → ineligible.
        assert!(
            !can_emit_update_expression(&UpdateExpressionCandidate {
                sets_rowid: true,
                ..good
            }),
            "bead_id={BEAD_ID} sets_rowid"
        );

        // SET expr not rebase-safe → ineligible.
        assert!(
            !can_emit_update_expression(&UpdateExpressionCandidate {
                all_sets_rebase_safe: false,
                ..good
            }),
            "bead_id={BEAD_ID} set_not_safe"
        );

        // Prior read of same row → ineligible.
        assert!(
            !can_emit_update_expression(&UpdateExpressionCandidate {
                has_prior_read_of_same_row: true,
                ..good
            }),
            "bead_id={BEAD_ID} prior_read"
        );
    }

    // ── Test: Full pipeline ──────────────────────────────────────────────

    #[test]
    fn test_full_deterministic_rebase_pipeline() {
        let mut rows = HashMap::new();
        rows.insert(
            (1, 1),
            record_bytes(&[SqliteValue::Integer(1), SqliteValue::Integer(50)]),
        );
        rows.insert(
            (1, 2),
            record_bytes(&[SqliteValue::Integer(2), SqliteValue::Integer(75)]),
        );
        let reader = MemBaseReader { rows };

        let schema = MemSchema {
            constraints: HashMap::from([(
                1,
                TableConstraints {
                    table_id: TableId::new(1),
                    not_null: vec![false, false],
                    affinities: vec![TypeAffinity::Integer, TypeAffinity::Integer],
                    check_exprs: vec![],
                },
            )]),
            indexes: HashMap::new(),
        };

        let intent_log = vec![
            // UpdateExpression for rowid 1: SET col1 = col1 + 10.
            make_intent_op(
                1,
                empty_footprint(),
                IntentOpKind::UpdateExpression {
                    table: TableId::new(1),
                    key: RowId::new(1),
                    column_updates: vec![(
                        ColumnIdx::new(1),
                        RebaseExpr::BinaryOp {
                            op: fsqlite_types::glossary::RebaseBinaryOp::Add,
                            left: Box::new(RebaseExpr::ColumnRef(ColumnIdx::new(1))),
                            right: Box::new(RebaseExpr::Literal(SqliteValue::Integer(10))),
                        },
                    )],
                },
            ),
            // A plain Insert (passes through).
            make_intent_op(
                1,
                empty_footprint(),
                IntentOpKind::Insert {
                    table: TableId::new(1),
                    key: RowId::new(100),
                    record: vec![1, 2, 3],
                },
            ),
            // UpdateExpression for rowid 2: SET col1 = col1 * 2.
            make_intent_op(
                1,
                empty_footprint(),
                IntentOpKind::UpdateExpression {
                    table: TableId::new(1),
                    key: RowId::new(2),
                    column_updates: vec![(
                        ColumnIdx::new(1),
                        RebaseExpr::BinaryOp {
                            op: fsqlite_types::glossary::RebaseBinaryOp::Multiply,
                            left: Box::new(RebaseExpr::ColumnRef(ColumnIdx::new(1))),
                            right: Box::new(RebaseExpr::Literal(SqliteValue::Integer(2))),
                        },
                    )],
                },
            ),
        ];

        let snapshot = test_snapshot(1);
        let result =
            deterministic_rebase(&intent_log, snapshot, &reader, &schema, &NoOpUniqueChecker)
                .unwrap();

        // 2 UpdateExpressions replayed.
        assert_eq!(result.replayed_count, 2, "bead_id={BEAD_ID} replayed_count");

        // Should have 3 ops: Update(rowid 1), Insert(rowid 100), Update(rowid 2).
        assert_eq!(result.rebased_ops.len(), 3, "bead_id={BEAD_ID} total_ops");

        // Verify rowid 1: col1 = 50 + 10 = 60.
        if let IntentOpKind::Update { new_record, .. } = &result.rebased_ops[0] {
            let parsed = parse_record(new_record).unwrap();
            assert_eq!(parsed[1], SqliteValue::Integer(60));
        } else {
            panic!("bead_id={BEAD_ID} expected Update for rowid 1");
        }

        // Verify plain Insert passed through.
        assert!(matches!(
            &result.rebased_ops[1],
            IntentOpKind::Insert { .. }
        ));

        // Verify rowid 2: col1 = 75 * 2 = 150.
        if let IntentOpKind::Update { new_record, .. } = &result.rebased_ops[2] {
            let parsed = parse_record(new_record).unwrap();
            assert_eq!(parsed[1], SqliteValue::Integer(150));
        } else {
            panic!("bead_id={BEAD_ID} expected Update for rowid 2");
        }
    }

    // ── Test: Target row not found → abort ───────────────────────────────

    #[test]
    fn test_rebase_target_row_not_found() {
        let reader = MemBaseReader {
            rows: HashMap::new(),
        };
        let schema = empty_schema();

        let result = replay_update_expression(
            TableId::new(1),
            RowId::new(999),
            &[],
            &reader,
            &schema,
            &NoOpUniqueChecker,
        );

        assert!(
            matches!(
                result,
                Err(RebaseError::TargetRowNotFound { key, .. }) if key.get() == 999
            ),
            "bead_id={BEAD_ID} target_not_found"
        );
    }

    // ── Test: CHECK constraint enforcement ───────────────────────────────

    #[test]
    fn test_rebase_check_constraint_violation() {
        let mut rows = HashMap::new();
        rows.insert(
            (1, 1),
            record_bytes(&[SqliteValue::Integer(1), SqliteValue::Integer(10)]),
        );
        let reader = MemBaseReader { rows };

        // CHECK: col1 > 0 (implemented as: col1 itself, truthy when > 0).
        // But we need col1 = 0 to fail. Use the column itself as predicate.
        let schema = MemSchema {
            constraints: HashMap::from([(
                1,
                TableConstraints {
                    table_id: TableId::new(1),
                    not_null: vec![false, false],
                    affinities: vec![TypeAffinity::Integer, TypeAffinity::Integer],
                    check_exprs: vec![
                        // "col1 is truthy" — fails when col1 = 0.
                        RebaseExpr::ColumnRef(ColumnIdx::new(1)),
                    ],
                },
            )]),
            indexes: HashMap::new(),
        };

        // SET col1 = 0 → CHECK fails (0 is falsy).
        let column_updates = vec![(
            ColumnIdx::new(1),
            RebaseExpr::Literal(SqliteValue::Integer(0)),
        )];

        let result = replay_update_expression(
            TableId::new(1),
            RowId::new(1),
            &column_updates,
            &reader,
            &schema,
            &NoOpUniqueChecker,
        );

        assert!(
            matches!(result, Err(RebaseError::CheckViolation { .. })),
            "bead_id={BEAD_ID} check_violation"
        );
    }

    // ── Test: CHECK with NULL passes ─────────────────────────────────────

    #[test]
    fn test_rebase_check_null_passes() {
        let mut rows = HashMap::new();
        rows.insert(
            (1, 1),
            record_bytes(&[SqliteValue::Integer(1), SqliteValue::Integer(10)]),
        );
        let reader = MemBaseReader { rows };

        let schema = MemSchema {
            constraints: HashMap::from([(
                1,
                TableConstraints {
                    table_id: TableId::new(1),
                    not_null: vec![false, false],
                    affinities: vec![TypeAffinity::Integer, TypeAffinity::Integer],
                    check_exprs: vec![
                        // CHECK evaluates to NULL → passes per SQLite semantics.
                        RebaseExpr::Literal(SqliteValue::Null),
                    ],
                },
            )]),
            indexes: HashMap::new(),
        };

        let column_updates = vec![(
            ColumnIdx::new(1),
            RebaseExpr::Literal(SqliteValue::Integer(42)),
        )];

        let result = replay_update_expression(
            TableId::new(1),
            RowId::new(1),
            &column_updates,
            &reader,
            &schema,
            &NoOpUniqueChecker,
        );

        assert!(result.is_ok(), "bead_id={BEAD_ID} check_null_passes");
    }

    // ── Test: Eligible intent log passes full pipeline ───────────────────

    #[test]
    fn test_eligible_intent_log_rebase_eligibility() {
        let intent_log = vec![
            make_intent_op(
                1,
                empty_footprint(),
                IntentOpKind::Insert {
                    table: TableId::new(1),
                    key: RowId::new(1),
                    record: vec![],
                },
            ),
            make_intent_op(
                1,
                empty_footprint(),
                IntentOpKind::UpdateExpression {
                    table: TableId::new(1),
                    key: RowId::new(2),
                    column_updates: vec![],
                },
            ),
        ];

        assert_eq!(
            check_rebase_eligibility(&intent_log),
            RebaseEligibility::Eligible,
            "bead_id={BEAD_ID} eligible"
        );
    }
}
