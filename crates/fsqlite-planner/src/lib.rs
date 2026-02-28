//! Query planner: name resolution, WHERE analysis, cost model, join ordering.
//!
//! Implements:
//! - Compound SELECT ORDER BY resolution (§19 quirk: first SELECT wins)
//! - Cost model for access paths in page reads (§10.5)
//! - Index usability analysis for WHERE terms (§10.5)
//! - Bounded beam search join ordering — NGQP-style (§10.5)
//!
//! Note: AST-to-VDBE compilation is an integration concern and lives above the
//! planner layer per the workspace layering rules (bd-1wwc).

pub mod codegen;
pub mod decision_contract;
pub mod stats;

use decision_contract::access_path_kind_label;
use fsqlite_ast::{
    BinaryOp as AstBinaryOp, ColumnRef, CompoundOp, Expr, FromClause, InSet, IndexHint,
    JoinConstraint, JoinKind, LikeOp, Literal, NullsOrder, OrderingTerm, ResultColumn, SelectBody,
    SelectCore, SortDirection, Span, TableOrSubquery,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};

// ---------------------------------------------------------------------------
// Compound ORDER BY resolution (§19 quirk: first SELECT wins)
// ---------------------------------------------------------------------------

/// A resolved ORDER BY term for a compound SELECT.
///
/// After resolution, each term is bound to a 0-based column index in the
/// compound result set, with optional direction, collation, and nulls ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedCompoundOrderBy {
    /// 0-based index into the compound result columns.
    pub column_idx: usize,
    /// ASC or DESC.
    pub direction: Option<SortDirection>,
    /// COLLATE override (e.g. `ORDER BY a COLLATE NOCASE`).
    pub collation: Option<String>,
    /// NULLS FIRST or NULLS LAST.
    pub nulls: Option<NullsOrder>,
}

/// Errors during compound ORDER BY resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompoundOrderByError {
    /// The referenced column name was not found in any SELECT's output aliases.
    ColumnNotFound { name: String, span: Span },
    /// A numeric column index is out of range (1-based in SQL, but converted).
    IndexOutOfRange {
        index: usize,
        num_columns: usize,
        span: Span,
    },
    /// A zero or negative numeric column index.
    IndexZeroOrNegative { value: i64, span: Span },
    /// An expression (e.g. `a+1`) is not allowed in compound ORDER BY.
    ExpressionNotAllowed { span: Span },
}

impl std::fmt::Display for CompoundOrderByError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ColumnNotFound { name, .. } => {
                write!(
                    f,
                    "1st ORDER BY term does not match any column in the result set: {name}"
                )
            }
            Self::IndexOutOfRange {
                index, num_columns, ..
            } => {
                write!(
                    f,
                    "ORDER BY column index {index} out of range (result has {num_columns} columns)"
                )
            }
            Self::IndexZeroOrNegative { value, .. } => {
                write!(
                    f,
                    "ORDER BY column index {value} out of range - must be positive"
                )
            }
            Self::ExpressionNotAllowed { .. } => {
                write!(
                    f,
                    "ORDER BY expression not allowed in compound SELECT - use column name or number"
                )
            }
        }
    }
}

impl std::error::Error for CompoundOrderByError {}

/// Extract output column alias names from a single `SelectCore`.
///
/// For `SELECT expr AS alias, ...` → `[Some("alias"), ...]`.
/// For unaliased `SELECT col` → uses the column name from a bare column ref.
/// For `*`, `table.*`, expressions without aliases → `None`.
/// For `VALUES (...)` → all `None`.
#[must_use]
pub fn extract_output_aliases(core: &SelectCore) -> Vec<Option<String>> {
    match core {
        SelectCore::Select { columns, .. } => columns
            .iter()
            .map(|rc| match rc {
                ResultColumn::Expr { alias: Some(a), .. } => Some(a.clone()),
                ResultColumn::Expr {
                    expr: Expr::Column(col_ref, _),
                    alias: None,
                    ..
                } => Some(col_ref.column.clone()),
                _ => None,
            })
            .collect(),
        SelectCore::Values(rows) => {
            let width = rows.first().map_or(0, Vec::len);
            vec![None; width]
        }
    }
}

/// Count the number of output columns in a `SelectCore`.
#[must_use]
pub fn count_output_columns(core: &SelectCore) -> usize {
    match core {
        SelectCore::Select { columns, .. } => columns.len(),
        SelectCore::Values(rows) => rows.first().map_or(0, Vec::len),
    }
}

// ---------------------------------------------------------------------------
// Single-table projection resolution (`*` / `table.*` expansion)
// ---------------------------------------------------------------------------

/// Errors during single-table result-column resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SingleTableProjectionError {
    /// The core is `VALUES`, not `SELECT`.
    NotSelectCore,
    /// A `FROM` clause is required for table-backed projection resolution.
    MissingFromClause,
    /// Unsupported source shape (non-table source or joins present).
    UnsupportedFromSource,
    /// A table qualifier did not match the single table or its alias.
    UnknownTableQualifier { qualifier: String },
    /// A referenced column does not exist on the table.
    ColumnNotFound { column: String },
}

impl fmt::Display for SingleTableProjectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotSelectCore => write!(f, "projection resolution requires SELECT core"),
            Self::MissingFromClause => write!(f, "projection resolution requires FROM clause"),
            Self::UnsupportedFromSource => {
                write!(f, "only single-table FROM without JOIN is supported")
            }
            Self::UnknownTableQualifier { qualifier } => {
                write!(f, "unknown table qualifier: {qualifier}")
            }
            Self::ColumnNotFound { column } => write!(f, "column not found: {column}"),
        }
    }
}

impl std::error::Error for SingleTableProjectionError {}

/// Resolve result columns for a single-table SELECT by:
/// - expanding `*` and `table.*` into explicit column refs
/// - validating table qualifiers and unqualified column refs
///
/// Non-column expressions are preserved as-is; codegen decides if they are
/// supported for table-backed execution.
pub fn resolve_single_table_result_columns(
    core: &SelectCore,
    table_columns: &[String],
) -> Result<Vec<ResultColumn>, SingleTableProjectionError> {
    let SelectCore::Select { columns, from, .. } = core else {
        return Err(SingleTableProjectionError::NotSelectCore);
    };
    let from_clause = from
        .as_ref()
        .ok_or(SingleTableProjectionError::MissingFromClause)?;
    let (table_name, table_alias) = single_table_source_name_and_alias(from_clause)?;

    let mut resolved = Vec::new();
    for result_col in columns {
        match result_col {
            ResultColumn::Star => {
                for column_name in table_columns {
                    resolved.push(ResultColumn::Expr {
                        expr: Expr::Column(ColumnRef::bare(column_name.clone()), Span::ZERO),
                        alias: None,
                    });
                }
            }
            ResultColumn::TableStar(qualifier) => {
                if !qualifier_matches_table(qualifier, table_name, table_alias) {
                    return Err(SingleTableProjectionError::UnknownTableQualifier {
                        qualifier: qualifier.clone(),
                    });
                }
                for column_name in table_columns {
                    resolved.push(ResultColumn::Expr {
                        expr: Expr::Column(ColumnRef::bare(column_name.clone()), Span::ZERO),
                        alias: None,
                    });
                }
            }
            ResultColumn::Expr {
                expr: Expr::Column(col_ref, _),
                ..
            } => {
                if let Some(qualifier) = &col_ref.table {
                    if !qualifier_matches_table(qualifier, table_name, table_alias) {
                        return Err(SingleTableProjectionError::UnknownTableQualifier {
                            qualifier: qualifier.clone(),
                        });
                    }
                }
                if !column_exists_ignore_case(table_columns, &col_ref.column)
                    && !is_rowid_alias_name(&col_ref.column)
                {
                    return Err(SingleTableProjectionError::ColumnNotFound {
                        column: col_ref.column.clone(),
                    });
                }
                resolved.push(result_col.clone());
            }
            ResultColumn::Expr { .. } => resolved.push(result_col.clone()),
        }
    }

    Ok(resolved)
}

fn single_table_source_name_and_alias(
    from_clause: &FromClause,
) -> Result<(&str, Option<&str>), SingleTableProjectionError> {
    if !from_clause.joins.is_empty() {
        return Err(SingleTableProjectionError::UnsupportedFromSource);
    }
    match &from_clause.source {
        TableOrSubquery::Table { name, alias, .. } => Ok((&name.name, alias.as_deref())),
        _ => Err(SingleTableProjectionError::UnsupportedFromSource),
    }
}

fn column_exists_ignore_case(columns: &[String], name: &str) -> bool {
    columns.iter().any(|c| c.eq_ignore_ascii_case(name))
}

fn qualifier_matches_table(qualifier: &str, table_name: &str, table_alias: Option<&str>) -> bool {
    qualifier.eq_ignore_ascii_case(table_name)
        || table_alias.is_some_and(|alias| qualifier.eq_ignore_ascii_case(alias))
}

fn is_rowid_alias_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower == "rowid" || lower == "_rowid_" || lower == "oid"
}

/// Resolve all ORDER BY terms for a compound SELECT statement.
///
/// # SQLite compound ORDER BY resolution rules
///
/// 1. **Integer literal** `ORDER BY N`: 1-based column index into the result.
/// 2. **Bare column reference** `ORDER BY name`: search output aliases of all
///    SELECTs in declaration order (first SELECT, then second, etc.). The first
///    SELECT that contains a matching alias wins, and the column resolves to the
///    *position* of that alias in that SELECT.
/// 3. **COLLATE wrapper** `ORDER BY name COLLATE X`: resolve the inner
///    expression as above, attach the collation override.
/// 4. **Any other expression**: rejected (expressions like `a+1` are not
///    allowed in compound SELECT ORDER BY).
///
/// # Errors
///
/// Returns [`CompoundOrderByError`] if a term cannot be resolved.
pub fn resolve_compound_order_by(
    body: &SelectBody,
    order_by: &[OrderingTerm],
) -> Result<Vec<ResolvedCompoundOrderBy>, CompoundOrderByError> {
    // Gather aliases from all SELECT cores in order.
    let mut all_aliases: Vec<Vec<Option<String>>> = Vec::with_capacity(1 + body.compounds.len());
    all_aliases.push(extract_output_aliases(&body.select));
    for (_, core) in &body.compounds {
        all_aliases.push(extract_output_aliases(core));
    }

    let num_columns = count_output_columns(&body.select);

    let mut resolved = Vec::with_capacity(order_by.len());
    for term in order_by {
        let (col_idx, collation) = resolve_single_term(&term.expr, &all_aliases, num_columns)?;
        resolved.push(ResolvedCompoundOrderBy {
            column_idx: col_idx,
            direction: term.direction,
            collation,
            nulls: term.nulls,
        });
    }

    Ok(resolved)
}

/// Resolve a single ORDER BY expression to a 0-based column index and optional
/// collation override.
fn resolve_single_term(
    expr: &Expr,
    all_aliases: &[Vec<Option<String>>],
    num_columns: usize,
) -> Result<(usize, Option<String>), CompoundOrderByError> {
    match expr {
        // Integer literal: 1-based column index.
        Expr::Literal(Literal::Integer(n), span) => {
            if *n <= 0 {
                return Err(CompoundOrderByError::IndexZeroOrNegative {
                    value: *n,
                    span: *span,
                });
            }
            #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
            let idx = (*n as usize) - 1;
            if idx >= num_columns {
                return Err(CompoundOrderByError::IndexOutOfRange {
                    index: idx + 1,
                    num_columns,
                    span: *span,
                });
            }
            Ok((idx, None))
        }

        // Bare column reference: search all SELECTs in order.
        Expr::Column(col_ref, span) => {
            let name = &col_ref.column;
            for aliases in all_aliases {
                for (pos, alias_opt) in aliases.iter().enumerate() {
                    if let Some(alias) = alias_opt {
                        if alias.eq_ignore_ascii_case(name) {
                            return Ok((pos, None));
                        }
                    }
                }
            }
            Err(CompoundOrderByError::ColumnNotFound {
                name: name.clone(),
                span: *span,
            })
        }

        // COLLATE wrapper: resolve inner expr, attach collation.
        Expr::Collate {
            expr: inner,
            collation,
            ..
        } => {
            let (idx, _) = resolve_single_term(inner, all_aliases, num_columns)?;
            Ok((idx, Some(collation.clone())))
        }

        // Any other expression is not allowed in compound ORDER BY.
        other => Err(CompoundOrderByError::ExpressionNotAllowed { span: other.span() }),
    }
}

/// Check whether a `SelectBody` is a compound query (has UNION/INTERSECT/EXCEPT).
#[must_use]
pub fn is_compound(body: &SelectBody) -> bool {
    !body.compounds.is_empty()
}

/// Get the compound operator type names for a compound SELECT (for logging).
#[must_use]
pub fn compound_op_name(op: CompoundOp) -> &'static str {
    match op {
        CompoundOp::Union => "UNION",
        CompoundOp::UnionAll => "UNION ALL",
        CompoundOp::Intersect => "INTERSECT",
        CompoundOp::Except => "EXCEPT",
    }
}

// ===========================================================================
// §10.5 Query Planning: Cost Model, Index Selection, Join Ordering
// ===========================================================================

// ---------------------------------------------------------------------------
// Statistics and metadata types
// ---------------------------------------------------------------------------

/// How table/index statistics were obtained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatsSource {
    /// From `ANALYZE` (`sqlite_stat1` / `sqlite_stat4`).
    Analyze,
    /// Heuristic fallback (no ANALYZE data available).
    Heuristic,
}

/// Statistics about a table, used for cost estimation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableStats {
    /// Table name.
    pub name: String,
    /// Number of B-tree pages occupied by the table.
    pub n_pages: u64,
    /// Estimated number of rows (from ANALYZE or heuristic).
    pub n_rows: u64,
    /// Source of these statistics.
    pub source: StatsSource,
}

/// Metadata about an index, used for cost estimation and usability checks.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexInfo {
    /// Index name.
    pub name: String,
    /// Table this index belongs to.
    pub table: String,
    /// Ordered list of indexed column names (leftmost first).
    pub columns: Vec<String>,
    /// Whether this is a UNIQUE index.
    pub unique: bool,
    /// Number of B-tree pages occupied by the index.
    pub n_pages: u64,
    /// Source of the page count.
    pub source: StatsSource,
    /// For partial indexes: the WHERE clause that restricts which rows appear.
    /// The planner can only use this index if the query's WHERE implies this predicate.
    pub partial_where: Option<Expr>,
    /// For expression indexes: the expressions indexed (parallel to `columns`).
    /// When present, the planner matches query expressions structurally against these.
    /// `columns` should contain synthetic names; the real matching uses these exprs.
    pub expression_columns: Vec<Expr>,
}

// ---------------------------------------------------------------------------
// Access path types
// ---------------------------------------------------------------------------

/// The kind of access path the planner can choose for a table scan.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::derive_partial_eq_without_eq)]
pub enum AccessPathKind {
    /// Sequential scan of all table pages.
    FullTableScan,
    /// Index range scan (e.g. `col > expr`, `col BETWEEN`).
    IndexScanRange { selectivity: f64 },
    /// Index equality scan (e.g. `col = expr`).
    IndexScanEquality,
    /// Covering index scan (all needed columns are in the index).
    CoveringIndexScan { selectivity: f64 },
    /// Direct rowid lookup (e.g. `WHERE rowid = ?`).
    RowidLookup,
}

/// A concrete access path chosen by the planner.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::derive_partial_eq_without_eq)]
pub struct AccessPath {
    /// Table being accessed.
    pub table: String,
    /// Kind of scan.
    pub kind: AccessPathKind,
    /// Index used (None for full table scan / rowid lookup).
    pub index: Option<String>,
    /// Estimated cost in page reads.
    pub estimated_cost: f64,
    /// Estimated rows returned.
    pub estimated_rows: f64,
}

/// The final output of the query planner: an ordered access plan.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryPlan {
    /// Tables in the chosen join order.
    pub join_order: Vec<String>,
    /// Access path for each table (parallel to `join_order`).
    pub access_paths: Vec<AccessPath>,
    /// Join operator segments selected for execution/explain.
    pub join_segments: Vec<JoinPlanSegment>,
    /// Total estimated cost in page reads.
    pub total_cost: f64,
}

/// Planner feature toggles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PlannerFeatureFlags {
    /// Enable Leapfrog Triejoin routing for compatible 3+ relation equi-joins.
    pub leapfrog_join: bool,
    /// Enable DPccp exhaustive search for small joins (<= `DPCCP_MAX_TABLES`).
    /// Falls back to beam search above the threshold.
    pub dpccp_join: bool,
}

/// Maximum table count for DPccp exhaustive search.
/// Above this threshold we use bounded beam search.
#[allow(dead_code)]
const DPCCP_MAX_TABLES: usize = 8;

/// Monotonic counter: total join plans enumerated.
static FSQLITE_PLANNER_PLANS_ENUMERATED: AtomicU64 = AtomicU64::new(0);

/// Take a snapshot of plans-enumerated counter.
#[must_use]
pub fn plans_enumerated_total() -> u64 {
    FSQLITE_PLANNER_PLANS_ENUMERATED.load(Ordering::Relaxed)
}

/// Reset plans-enumerated counter.
pub fn reset_plans_enumerated() {
    FSQLITE_PLANNER_PLANS_ENUMERATED.store(0, Ordering::Relaxed);
}

/// Join operator chosen for a segment of the join plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinOperator {
    /// Pairwise hash join execution.
    HashJoin,
    /// Multi-way Leapfrog Triejoin execution.
    LeapfrogTriejoin,
}

impl JoinOperator {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::HashJoin => "HASH JOIN",
            Self::LeapfrogTriejoin => "LEAPFROG TRIEJOIN",
        }
    }
}

/// One join-operator decision segment.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::derive_partial_eq_without_eq)]
pub struct JoinPlanSegment {
    /// Relations covered by this segment in execution order.
    pub relations: Vec<String>,
    /// Operator chosen for this segment.
    pub operator: JoinOperator,
    /// Estimated operator cost.
    pub estimated_cost: f64,
    /// Human-readable decision reason.
    pub reason: String,
}

impl fmt::Display for QueryPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "QUERY PLAN (est. cost {:.1}):", self.total_cost)?;
        for (i, ap) in self.access_paths.iter().enumerate() {
            let idx_str = ap
                .index
                .as_deref()
                .map_or(String::new(), |n| format!(" USING INDEX {n}"));
            writeln!(
                f,
                "  {i}: SCAN {}{idx_str} (~{:.0} rows, cost {:.1})",
                ap.table, ap.estimated_rows, ap.estimated_cost
            )?;
        }
        if !self.join_segments.is_empty() {
            writeln!(f, "JOIN OPERATORS:")?;
            for segment in &self.join_segments {
                writeln!(
                    f,
                    "  {} {} (est. {:.1}) [{}]",
                    segment.operator.label(),
                    segment.relations.join(" JOIN "),
                    segment.estimated_cost,
                    segment.reason
                )?;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Cost model (§10.5)
// ---------------------------------------------------------------------------

/// Estimate the cost (in page reads) for a given access path.
///
/// Formulas from §10.5:
/// - Full table scan: `N_pages(table)`
/// - Index scan (range): `log2(idx_pages) + selectivity * idx_pages + selectivity * tbl_pages`
/// - Index scan (equality): `log2(idx_pages) + log2(tbl_pages)`
/// - Covering index scan: `log2(idx_pages) + selectivity * idx_pages`
/// - Rowid lookup: `log2(tbl_pages)`
#[must_use]
pub fn estimate_cost(kind: &AccessPathKind, table_pages: u64, index_pages: u64) -> f64 {
    let tp = table_pages.max(1) as f64;
    let ip = index_pages.max(1) as f64;

    let cost = match kind {
        AccessPathKind::FullTableScan => tp,
        AccessPathKind::IndexScanRange { selectivity } => {
            ip.log2() + selectivity * ip + selectivity * tp
        }
        AccessPathKind::IndexScanEquality => ip.log2() + tp.log2(),
        AccessPathKind::CoveringIndexScan { selectivity } => ip.log2() + selectivity * ip,
        AccessPathKind::RowidLookup => tp.log2(),
    };

    FSQLITE_PLANNER_COST_ESTIMATES_TOTAL.fetch_add(1, Ordering::Relaxed);

    tracing::debug!(
        target: "fsqlite.planner",
        table_pages,
        index_pages,
        estimated_cost = cost,
        actual_method = %access_path_metric_label(kind),
        "cost_estimate"
    );

    cost
}

const ADAPTIVE_HINT_COST_BIAS: f64 = 0.90;

static INDEX_SELECTION_TOTAL: LazyLock<Mutex<HashMap<&'static str, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

// ---------------------------------------------------------------------------
// Cost estimation metrics (bd-1as.1)
// ---------------------------------------------------------------------------

/// Monotonic counter: total cost estimates computed.
static FSQLITE_PLANNER_COST_ESTIMATES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Estimation error ratio observations stored as fixed-point
/// (ratio × 1000, truncated to u64). Used to compute histogram buckets.
static ESTIMATION_ERROR_OBSERVATIONS: LazyLock<Mutex<Vec<f64>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

/// Point-in-time snapshot of planner cost metrics.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CostMetricsSnapshot {
    /// Total number of cost estimates computed.
    pub fsqlite_planner_cost_estimates_total: u64,
    /// Estimation error ratio observations (actual/estimated).
    /// Bucketed: [0, 0.5), [0.5, 1.0), [1.0, 2.0), [2.0, 5.0), [5.0, +inf).
    pub error_ratio_buckets: [u64; 5],
    /// Mean error ratio (NaN if no observations).
    pub error_ratio_mean: f64,
}

/// Bucket boundaries for the error ratio histogram.
const ERROR_RATIO_BOUNDARIES: [f64; 4] = [0.5, 1.0, 2.0, 5.0];

/// Take a point-in-time snapshot of cost estimation metrics.
#[must_use]
pub fn cost_metrics_snapshot() -> CostMetricsSnapshot {
    let total = FSQLITE_PLANNER_COST_ESTIMATES_TOTAL.load(Ordering::Relaxed);
    let observations = ESTIMATION_ERROR_OBSERVATIONS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    let mut buckets = [0u64; 5];
    let mut sum = 0.0;
    for &ratio in observations.iter() {
        sum += ratio;
        let idx = ERROR_RATIO_BOUNDARIES
            .iter()
            .position(|&b| ratio < b)
            .unwrap_or(4);
        buckets[idx] += 1;
    }
    let mean = if observations.is_empty() {
        f64::NAN
    } else {
        sum / observations.len() as f64
    };

    CostMetricsSnapshot {
        fsqlite_planner_cost_estimates_total: total,
        error_ratio_buckets: buckets,
        error_ratio_mean: mean,
    }
}

/// Reset cost estimation metrics.
pub fn reset_cost_metrics() {
    FSQLITE_PLANNER_COST_ESTIMATES_TOTAL.store(0, Ordering::Relaxed);
    let mut obs = ESTIMATION_ERROR_OBSERVATIONS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    obs.clear();
}

/// Record an estimation error observation (actual_cost / estimated_cost).
pub fn record_estimation_error(actual: f64, estimated: f64) {
    if estimated <= 0.0 || actual < 0.0 {
        return;
    }
    let ratio = actual / estimated;
    {
        let mut obs = ESTIMATION_ERROR_OBSERVATIONS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        obs.push(ratio);
    }

    tracing::debug!(
        actual,
        estimated,
        ratio,
        miscalibrated = !(0.2..=5.0).contains(&ratio),
        "planner.estimation_error"
    );
}

/// Decision-theoretic asymmetric loss function for cost estimation.
///
/// Underestimation (actual > estimated) is penalized more heavily than
/// overestimation because underestimation leads to slow queries that miss
/// deadlines, while overestimation merely causes slightly suboptimal plans.
///
/// Loss = if actual > estimated:
///     UNDERESTIMATE_PENALTY × (actual/estimated - 1)²  (quadratic)
///   else:
///     (1 - actual/estimated)                            (linear)
const UNDERESTIMATE_PENALTY: f64 = 3.0;

/// Compute asymmetric loss between estimated and actual costs.
///
/// Higher loss for underestimation (surprise slowness) than overestimation.
#[must_use]
pub fn asymmetric_estimation_loss(estimated: f64, actual: f64) -> f64 {
    if estimated <= 0.0 {
        return actual; // Degenerate case.
    }
    let ratio = actual / estimated;
    if ratio > 1.0 {
        // Underestimate: quadratic penalty.
        UNDERESTIMATE_PENALTY * (ratio - 1.0).powi(2)
    } else {
        // Overestimate: linear penalty.
        1.0 - ratio
    }
}

fn access_path_metric_label(kind: &AccessPathKind) -> &'static str {
    match kind {
        AccessPathKind::FullTableScan => "full_table_scan",
        AccessPathKind::IndexScanRange { .. } => "index_scan_range",
        AccessPathKind::IndexScanEquality => "index_scan_equality",
        AccessPathKind::CoveringIndexScan { .. } => "covering_index_scan",
        AccessPathKind::RowidLookup => "rowid_lookup",
    }
}

fn increment_index_selection_total(kind: &AccessPathKind) -> u64 {
    let label = access_path_metric_label(kind);
    let mut counters = INDEX_SELECTION_TOTAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let updated_count = {
        let entry = counters.entry(label).or_insert(0);
        *entry += 1;
        *entry
    };
    drop(counters);
    updated_count
}

#[must_use]
pub fn snapshot_index_selection_totals() -> BTreeMap<String, u64> {
    let counters = INDEX_SELECTION_TOTAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    counters
        .iter()
        .map(|(label, count)| ((*label).to_owned(), *count))
        .collect()
}

fn canonical_table_key(table_name: &str) -> String {
    table_name.to_ascii_lowercase()
}

fn lookup_table_index_hint<'a>(
    table_name: &str,
    table_index_hints: Option<&'a BTreeMap<String, IndexHint>>,
) -> Option<&'a IndexHint> {
    table_index_hints.and_then(|hints| hints.get(&canonical_table_key(table_name)))
}

/// Minimal adaptive hint cache keyed by table name.
///
/// The planner records the last chosen index for each table and can reuse it as
/// a soft preference on subsequent planning passes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CrackingHintStore {
    preferred_index_by_table: HashMap<String, String>,
}

impl CrackingHintStore {
    #[must_use]
    pub fn preferred_index(&self, table_name: &str) -> Option<&str> {
        self.preferred_index_by_table
            .get(&canonical_table_key(table_name))
            .map(String::as_str)
    }

    pub fn record_access_path(&mut self, access_path: &AccessPath) {
        if let Some(index_name) = &access_path.index {
            self.preferred_index_by_table
                .insert(canonical_table_key(&access_path.table), index_name.clone());
        }
    }
}

fn collect_table_index_hints_inner(
    from_clause: &FromClause,
    output: &mut BTreeMap<String, IndexHint>,
) {
    fn collect_source(source: &TableOrSubquery, output: &mut BTreeMap<String, IndexHint>) {
        match source {
            TableOrSubquery::Table {
                name,
                alias,
                index_hint,
            } => {
                if let Some(hint) = index_hint {
                    output.insert(canonical_table_key(&name.name), hint.clone());
                    if let Some(alias_name) = alias {
                        output.insert(canonical_table_key(alias_name), hint.clone());
                    }
                }
            }
            TableOrSubquery::ParenJoin(inner) => {
                collect_table_index_hints_inner(inner, output);
            }
            TableOrSubquery::Subquery { .. } | TableOrSubquery::TableFunction { .. } => {}
        }
    }

    collect_source(&from_clause.source, output);
    for join in &from_clause.joins {
        collect_source(&join.table, output);
    }
}

/// Extract per-table index hints from a FROM clause.
///
/// Keys are normalized to ASCII-lowercase table names and aliases.
#[must_use]
pub fn collect_table_index_hints(from_clause: &FromClause) -> BTreeMap<String, IndexHint> {
    let mut hints = BTreeMap::new();
    collect_table_index_hints_inner(from_clause, &mut hints);
    hints
}

/// Build the cheapest [`AccessPath`] for a table given available indexes and
/// WHERE terms. Returns the lowest-cost option.
#[must_use]
pub fn best_access_path(
    table: &TableStats,
    indexes: &[IndexInfo],
    where_terms: &[WhereTerm<'_>],
    needed_columns: Option<&[String]>,
) -> AccessPath {
    best_access_path_with_hints(table, indexes, where_terms, needed_columns, None, None)
}

/// Build the cheapest [`AccessPath`] while applying explicit index hints and
/// optional adaptive cracking hint reuse.
#[must_use]
pub fn best_access_path_with_hints(
    table: &TableStats,
    indexes: &[IndexInfo],
    where_terms: &[WhereTerm<'_>],
    needed_columns: Option<&[String]>,
    index_hint: Option<&IndexHint>,
    cracking_hints: Option<&mut CrackingHintStore>,
) -> AccessPath {
    let adaptive_preferred_index = cracking_hints
        .as_deref()
        .and_then(|store| store.preferred_index(&table.name))
        .map(ToOwned::to_owned);

    let best = best_access_path_internal(
        table,
        indexes,
        where_terms,
        needed_columns,
        index_hint,
        adaptive_preferred_index.as_deref(),
    );

    if let Some(store) = cracking_hints {
        store.record_access_path(&best);
    }

    best
}

/// Build the cheapest [`AccessPath`] with optional explicit and adaptive hints.
#[must_use]
#[allow(clippy::too_many_lines)]
fn best_access_path_internal(
    table: &TableStats,
    indexes: &[IndexInfo],
    where_terms: &[WhereTerm<'_>],
    needed_columns: Option<&[String]>,
    index_hint: Option<&IndexHint>,
    adaptive_preferred_index: Option<&str>,
) -> AccessPath {
    let started = std::time::Instant::now();
    let explicit_indexed_by = match index_hint {
        Some(IndexHint::IndexedBy(index_name)) => Some(index_name.as_str()),
        _ => None,
    };
    let not_indexed = matches!(index_hint, Some(IndexHint::NotIndexed));

    let mut best = AccessPath {
        table: table.name.clone(),
        kind: AccessPathKind::FullTableScan,
        index: None,
        estimated_cost: if explicit_indexed_by.is_some() {
            f64::INFINITY
        } else {
            estimate_cost(&AccessPathKind::FullTableScan, table.n_pages, 0)
        },
        estimated_rows: table.n_rows as f64,
    };

    let mut candidates_considered: usize = 0;
    let mut partial_indexes_pruned: usize = 0;
    let mut hint_filtered_indexes: usize = 0;
    let mut skip_scan_candidates: usize = 0;
    let mut adaptive_hint_applied = false;
    let mut explicit_hint_applied = false;
    let mut explicit_hint_missing = explicit_indexed_by.is_some();

    // Check each index for usability.
    for idx in indexes {
        if !idx.table.eq_ignore_ascii_case(&table.name) {
            continue;
        }
        if not_indexed {
            hint_filtered_indexes += 1;
            continue;
        }
        if let Some(hinted_name) = explicit_indexed_by {
            if !idx.name.eq_ignore_ascii_case(hinted_name) {
                hint_filtered_indexes += 1;
                continue;
            }
            explicit_hint_missing = false;
        }

        // Partial index gate: skip unless the query's WHERE implies the
        // index's WHERE predicate. We use a conservative structural check:
        // the index predicate must appear as a conjunct in the query WHERE.
        if let Some(ref partial_pred) = idx.partial_where {
            if !where_terms_imply_predicate(where_terms, partial_pred) {
                partial_indexes_pruned += 1;
                continue;
            }
        }

        let mut skip_scan_candidate = None;
        let usability = match analyze_index_usability(idx, where_terms) {
            IndexUsability::NotUsable => {
                if let Some(candidate) = analyze_skip_scan_candidate(table, idx, where_terms) {
                    skip_scan_candidates += 1;
                    skip_scan_candidate = Some(candidate);
                    IndexUsability::Range {
                        selectivity: candidate.per_probe_selectivity,
                    }
                } else {
                    IndexUsability::NotUsable
                }
            }
            usable => usable,
        };

        if matches!(usability, IndexUsability::NotUsable) {
            continue;
        }

        candidates_considered += 1;

        let is_covering = needed_columns.is_some_and(|needed| {
            needed
                .iter()
                .all(|c| idx.columns.iter().any(|ic| ic.eq_ignore_ascii_case(c)))
        });

        let mut cost_multiplier: f64 = 1.0;
        let (kind, mut est_rows) = match usability {
            IndexUsability::Equality => {
                let rows = if idx.unique {
                    1.0
                } else {
                    (table.n_rows as f64 / 10.0).max(1.0)
                };
                if is_covering {
                    (
                        AccessPathKind::CoveringIndexScan {
                            selectivity: rows / table.n_rows.max(1) as f64,
                        },
                        rows,
                    )
                } else {
                    (AccessPathKind::IndexScanEquality, rows)
                }
            }
            IndexUsability::MultiColumnEquality {
                eq_columns,
                has_trailing_range,
            } => {
                // Multi-column equality narrows selectivity geometrically.
                // Each additional equality column reduces rows by ~1/10.
                #[allow(clippy::cast_precision_loss)]
                let base_rows = if idx.unique && eq_columns == idx.columns.len() {
                    1.0
                } else {
                    let divisor = 10.0_f64.powi(i32::try_from(eq_columns).unwrap_or(i32::MAX));
                    (table.n_rows as f64 / divisor).max(1.0)
                };
                let (rows, sel) = if has_trailing_range {
                    let range_factor = DEFAULT_RANGE_SELECTIVITY;
                    let r = (base_rows * range_factor).max(1.0);
                    (r, range_factor * base_rows / table.n_rows.max(1) as f64)
                } else {
                    (base_rows, base_rows / table.n_rows.max(1) as f64)
                };
                if is_covering {
                    (AccessPathKind::CoveringIndexScan { selectivity: sel }, rows)
                } else if has_trailing_range {
                    (AccessPathKind::IndexScanRange { selectivity: sel }, rows)
                } else {
                    (AccessPathKind::IndexScanEquality, rows)
                }
            }
            IndexUsability::Range { selectivity } => {
                let rows = (selectivity * table.n_rows as f64).max(1.0);
                if is_covering {
                    (AccessPathKind::CoveringIndexScan { selectivity }, rows)
                } else {
                    (AccessPathKind::IndexScanRange { selectivity }, rows)
                }
            }
            IndexUsability::InExpansion { probe_count } => {
                // Each probe is like an equality lookup; total cost
                // and rows are scaled by the number of probes.
                let per_probe_rows = if idx.unique {
                    1.0
                } else {
                    (table.n_rows as f64 / 10.0).max(1.0)
                };
                let rows = per_probe_rows * probe_count as f64;
                cost_multiplier = probe_count as f64;
                (AccessPathKind::IndexScanEquality, rows)
            }
            IndexUsability::LikePrefix { .. } => {
                let selectivity = 0.1; // Heuristic: 10% for prefix LIKE.
                let rows = (selectivity * table.n_rows as f64).max(1.0);
                if is_covering {
                    (AccessPathKind::CoveringIndexScan { selectivity }, rows)
                } else {
                    (AccessPathKind::IndexScanRange { selectivity }, rows)
                }
            }
            IndexUsability::NotUsable => unreachable!(),
        };

        if let Some(candidate) = skip_scan_candidate {
            let probe_multiplier =
                (candidate.leading_probes * candidate.trailing_probe_count) as f64;
            cost_multiplier *= probe_multiplier;
            est_rows = (est_rows * probe_multiplier).min(table.n_rows.max(1) as f64);
        }

        let mut cost = estimate_cost(&kind, table.n_pages, idx.n_pages) * cost_multiplier;

        if let Some(hinted_name) = explicit_indexed_by {
            if idx.name.eq_ignore_ascii_case(hinted_name) {
                // Respect explicit INDEXED BY by strongly preferring that index.
                cost *= 0.01;
                explicit_hint_applied = true;
            }
        } else if let Some(adaptive_hint) = adaptive_preferred_index {
            if idx.name.eq_ignore_ascii_case(adaptive_hint) {
                cost *= ADAPTIVE_HINT_COST_BIAS;
                adaptive_hint_applied = true;
            }
        }

        if cost < best.estimated_cost {
            best = AccessPath {
                table: table.name.clone(),
                kind,
                index: Some(idx.name.clone()),
                estimated_cost: cost,
                estimated_rows: est_rows,
            };
        }
    }

    // Check rowid lookup.
    if !not_indexed && explicit_indexed_by.is_none() && has_rowid_equality(where_terms) {
        let kind = AccessPathKind::RowidLookup;
        let cost = estimate_cost(&kind, table.n_pages, 0);
        if cost < best.estimated_cost {
            best = AccessPath {
                table: table.name.clone(),
                kind,
                index: None,
                estimated_cost: cost,
                estimated_rows: 1.0,
            };
        }
    }

    if !best.estimated_cost.is_finite() {
        best = AccessPath {
            table: table.name.clone(),
            kind: AccessPathKind::FullTableScan,
            index: None,
            estimated_cost: estimate_cost(&AccessPathKind::FullTableScan, table.n_pages, 0),
            estimated_rows: table.n_rows as f64,
        };
    }

    let chosen_index = best.index.as_deref().unwrap_or("(none)");
    let selectivity = match &best.kind {
        AccessPathKind::IndexScanRange { selectivity }
        | AccessPathKind::CoveringIndexScan { selectivity } => *selectivity,
        AccessPathKind::IndexScanEquality | AccessPathKind::RowidLookup => {
            best.estimated_rows / table.n_rows.max(1) as f64
        }
        AccessPathKind::FullTableScan => 1.0,
    };
    let metric_index_type = access_path_metric_label(&best.kind);
    let metric_total = increment_index_selection_total(&best.kind);
    let explicit_hint = match index_hint {
        Some(IndexHint::IndexedBy(index_name)) => format!("indexed_by:{index_name}"),
        Some(IndexHint::NotIndexed) => "not_indexed".to_owned(),
        None => "(none)".to_owned(),
    };
    let run_id = std::env::var("RUN_ID").unwrap_or_else(|_| "(none)".to_owned());
    let trace_id = std::env::var("TRACE_ID")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let scenario_id = std::env::var("SCENARIO_ID").unwrap_or_else(|_| "(none)".to_owned());
    let selection_elapsed_us = started.elapsed().as_micros().max(1);
    let adaptive_hint = adaptive_preferred_index.unwrap_or("(none)");
    let hint_applied = explicit_hint_applied || adaptive_hint_applied;
    let span = tracing::info_span!(
        "index_select",
        run_id = %run_id,
        trace_id,
        scenario_id = %scenario_id,
        table = %table.name,
        explicit_hint = %explicit_hint,
        adaptive_hint = %adaptive_hint,
        candidates = candidates_considered,
        partial_pruned = partial_indexes_pruned,
        hint_filtered = hint_filtered_indexes,
        skip_scan_candidates
    );
    let _span_guard = span.enter();

    tracing::info!(
        table = %table.name,
        candidates = candidates_considered,
        chosen_index = %chosen_index,
        estimated_selectivity = selectivity,
        access_path = %access_path_kind_label(&best.kind),
        estimated_cost = best.estimated_cost,
        estimated_rows = best.estimated_rows,
        selection_elapsed_us,
        run_id = %run_id,
        trace_id,
        scenario_id = %scenario_id,
        index_type = metric_index_type,
        fsqlite_index_selection_total = metric_total,
        hint_applied,
        explicit_hint_missing,
        "planner.index_select.choice"
    );

    best
}

/// Check if the WHERE terms collectively imply a partial index predicate.
///
/// Conservative structural check: the predicate (or each conjunct of it)
/// must appear as the expression of one of the WHERE terms.
fn where_terms_imply_predicate(terms: &[WhereTerm<'_>], predicate: &Expr) -> bool {
    // Decompose the predicate into conjuncts.
    let pred_conjuncts = decompose_where(predicate);

    // Each conjunct of the predicate must be matched by some WHERE term.
    pred_conjuncts.iter().all(|pc| {
        terms.iter().any(|t| {
            // Structural equality of the AST nodes.
            *t.expr == **pc
        })
    })
}

// ---------------------------------------------------------------------------
// Index usability analysis (§10.5)
// ---------------------------------------------------------------------------

/// Result of analyzing a WHERE term against an index.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::derive_partial_eq_without_eq)]
pub enum IndexUsability {
    /// Index can satisfy an equality constraint on its leftmost column.
    Equality,
    /// Multi-column equality prefix: equality on the first `eq_columns` index
    /// columns, optionally followed by a range constraint on the next column.
    MultiColumnEquality {
        /// Number of leading columns with equality constraints.
        eq_columns: usize,
        /// Whether the column after the equality prefix has a range constraint.
        has_trailing_range: bool,
    },
    /// Index can satisfy a range constraint (rightmost usable position).
    Range { selectivity: f64 },
    /// `IN (...)` expanded to multiple equality probes.
    InExpansion { probe_count: usize },
    /// `LIKE`/`GLOB` with a constant prefix and derived upper bound.
    /// Represents the range: `column >= low` and optionally `column < high`.
    LikePrefix { low: String, high: Option<String> },
    /// The term cannot use this index.
    NotUsable,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct SkipScanCandidate {
    leading_probes: usize,
    trailing_probe_count: usize,
    per_probe_selectivity: f64,
}

/// A decomposed WHERE term with the column it references (if any).
#[derive(Debug, Clone)]
pub struct WhereTerm<'a> {
    /// The original expression.
    pub expr: &'a Expr,
    /// The column referenced on the left side (if this is a simple comparison).
    pub column: Option<WhereColumn>,
    /// The kind of constraint.
    pub kind: WhereTermKind,
}

/// The column side of a WHERE comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WhereColumn {
    /// Optional table qualifier.
    pub table: Option<String>,
    /// Column name.
    pub column: String,
}

/// Classification of a WHERE term for index usability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WhereTermKind {
    /// `col = expr`
    Equality,
    /// `col > expr`, `col >= expr`, `col < expr`, `col <= expr`
    Range,
    /// `col BETWEEN low AND high`
    Between,
    /// `col IN (...)`
    InList { count: usize },
    /// `col LIKE 'prefix%'` or `col GLOB 'prefix*'`, rewritten as
    /// `col >= prefix AND col < upper_bound`.
    LikePrefix {
        prefix: String,
        upper_bound: Option<String>,
    },
    /// Rowid equality: `rowid = expr` or `_rowid_ = expr` or `oid = expr`
    RowidEquality,
    /// Any other expression (not directly usable for index lookup).
    Other,
}

/// Decompose a WHERE clause into individual conjuncts (AND-separated terms).
#[must_use]
pub fn decompose_where(expr: &Expr) -> Vec<&Expr> {
    let mut terms = Vec::new();
    collect_conjuncts(expr, &mut terms);
    terms
}

fn collect_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    if let Expr::BinaryOp {
        left,
        op: AstBinaryOp::And,
        right,
        ..
    } = expr
    {
        collect_conjuncts(left, out);
        collect_conjuncts(right, out);
    } else {
        out.push(expr);
    }
}

fn collect_disjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    if let Expr::BinaryOp {
        left,
        op: AstBinaryOp::Or,
        right,
        ..
    } = expr
    {
        collect_disjuncts(left, out);
        collect_disjuncts(right, out);
    } else {
        out.push(expr);
    }
}

fn where_columns_equivalent(left: &WhereColumn, right: &WhereColumn) -> bool {
    left.column.eq_ignore_ascii_case(&right.column)
        && match (&left.table, &right.table) {
            (Some(l), Some(r)) => l.eq_ignore_ascii_case(r),
            (None, None) => true,
            _ => false,
        }
}

fn classify_or_disjunction_as_in_list(expr: &Expr) -> Option<(WhereColumn, usize)> {
    let mut disjuncts = Vec::new();
    collect_disjuncts(expr, &mut disjuncts);
    if disjuncts.len() < 2 {
        return None;
    }

    let mut shared_column: Option<WhereColumn> = None;

    for disjunct in disjuncts.iter().copied() {
        let Expr::BinaryOp {
            left,
            op: AstBinaryOp::Eq,
            right,
            ..
        } = disjunct
        else {
            return None;
        };

        let column = match (extract_where_column(left), extract_where_column(right)) {
            (Some(column), None) | (None, Some(column)) => column,
            _ => return None,
        };

        if is_rowid_column(&column) {
            return None;
        }

        if let Some(ref existing) = shared_column {
            if !where_columns_equivalent(existing, &column) {
                return None;
            }
        } else {
            shared_column = Some(column);
        }
    }

    shared_column.map(|column| (column, disjuncts.len()))
}

/// Classify a single WHERE expression into a [`WhereTerm`].
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn classify_where_term(expr: &Expr) -> WhereTerm<'_> {
    match expr {
        // (col = v1) OR (col = v2) OR ... => treat as IN-list probe expansion.
        Expr::BinaryOp {
            op: AstBinaryOp::Or,
            ..
        } => {
            if let Some((column, probe_count)) = classify_or_disjunction_as_in_list(expr) {
                tracing::debug!(
                    target: "fsqlite.planner",
                    rewrite = "or_disjunction_to_in_list",
                    column = ?column,
                    probe_count,
                    "planner.where_term.rewrite"
                );
                return WhereTerm {
                    expr,
                    column: Some(column),
                    kind: WhereTermKind::InList { count: probe_count },
                };
            }

            WhereTerm {
                expr,
                column: None,
                kind: WhereTermKind::Other,
            }
        }

        // col = expr or expr = col
        Expr::BinaryOp {
            left,
            op: AstBinaryOp::Eq,
            right,
            ..
        } => {
            if let Some(wc) = extract_where_column(left) {
                if is_rowid_column(&wc) {
                    return WhereTerm {
                        expr,
                        column: Some(wc),
                        kind: WhereTermKind::RowidEquality,
                    };
                }
                return WhereTerm {
                    expr,
                    column: Some(wc),
                    kind: WhereTermKind::Equality,
                };
            }
            if let Some(wc) = extract_where_column(right) {
                if is_rowid_column(&wc) {
                    return WhereTerm {
                        expr,
                        column: Some(wc),
                        kind: WhereTermKind::RowidEquality,
                    };
                }
                return WhereTerm {
                    expr,
                    column: Some(wc),
                    kind: WhereTermKind::Equality,
                };
            }
            WhereTerm {
                expr,
                column: None,
                kind: WhereTermKind::Other,
            }
        }

        // col < expr, col <= expr, col > expr, col >= expr
        // Also handles reversed forms like `5 < col` by checking both sides.
        Expr::BinaryOp {
            left,
            op: AstBinaryOp::Lt | AstBinaryOp::Le | AstBinaryOp::Gt | AstBinaryOp::Ge,
            right,
            ..
        } => {
            let column = extract_where_column(left).or_else(|| extract_where_column(right));
            WhereTerm {
                expr,
                column,
                kind: WhereTermKind::Range,
            }
        }

        // col BETWEEN low AND high
        Expr::Between {
            expr: inner, not, ..
        } if !not => {
            let column = extract_where_column(inner);
            WhereTerm {
                expr,
                column,
                kind: WhereTermKind::Between,
            }
        }

        // col IN (...)
        Expr::In {
            expr: inner,
            set,
            not,
            ..
        } if !not => {
            let column = extract_where_column(inner);
            let count = match set {
                InSet::List(items) => items.len(),
                InSet::Subquery(_) | InSet::Table(_) => 10, // Heuristic
            };
            WhereTerm {
                expr,
                column,
                kind: WhereTermKind::InList { count },
            }
        }

        // col LIKE 'prefix%' or col GLOB 'prefix*'
        Expr::Like {
            expr: inner,
            pattern,
            op,
            not,
            ..
        } if !not && matches!(op, LikeOp::Like | LikeOp::Glob) => {
            let column = extract_where_column(inner);
            let (prefix, operator) = match op {
                LikeOp::Like => (extract_like_prefix(pattern), "LIKE"),
                LikeOp::Glob => (extract_glob_prefix(pattern), "GLOB"),
                _ => unreachable!("guard restricts to LIKE/GLOB"),
            };
            if let Some(pfx) = prefix {
                let upper_bound = like_prefix_upper_bound(&pfx);
                tracing::debug!(
                    target: "fsqlite.planner",
                    rewrite = "pattern_prefix_to_range",
                    operator,
                    column = ?column,
                    prefix = %pfx,
                    upper_bound = ?upper_bound,
                    "planner.where_term.rewrite"
                );
                WhereTerm {
                    expr,
                    column,
                    kind: WhereTermKind::LikePrefix {
                        upper_bound,
                        prefix: pfx,
                    },
                }
            } else {
                WhereTerm {
                    expr,
                    column,
                    kind: WhereTermKind::Other,
                }
            }
        }

        _ => WhereTerm {
            expr,
            column: None,
            kind: WhereTermKind::Other,
        },
    }
}

/// Extract a `WhereColumn` from an expression if it's a simple column reference.
fn extract_where_column(expr: &Expr) -> Option<WhereColumn> {
    if let Expr::Column(col_ref, _) = expr {
        Some(WhereColumn {
            table: col_ref.table.clone(),
            column: col_ref.column.clone(),
        })
    } else {
        None
    }
}

/// Check if a `WhereColumn` is a rowid alias.
fn is_rowid_column(wc: &WhereColumn) -> bool {
    let name = wc.column.to_ascii_lowercase();
    name == "rowid" || name == "_rowid_" || name == "oid"
}

/// Check if any WHERE term has a rowid equality constraint.
fn has_rowid_equality(terms: &[WhereTerm<'_>]) -> bool {
    terms
        .iter()
        .any(|t| matches!(t.kind, WhereTermKind::RowidEquality))
}

/// Extract a constant prefix from a LIKE pattern (e.g. `'abc%'` → `"abc"`).
///
/// Returns `None` if the pattern has no constant prefix (starts with `%` or `_`)
/// or is not a string literal.
fn extract_like_prefix(pattern: &Expr) -> Option<String> {
    if let Expr::Literal(Literal::String(s), _) = pattern {
        let mut prefix = String::new();
        for ch in s.chars() {
            if ch == '%' || ch == '_' {
                break;
            }
            prefix.push(ch);
        }
        if prefix.is_empty() {
            None
        } else {
            Some(prefix)
        }
    } else {
        None
    }
}

/// Extract a constant prefix from a GLOB pattern (e.g. `'abc*'` → `"abc"`).
///
/// Returns `None` if the pattern has no constant prefix (starts with `*`, `?`,
/// or `[`), or is not a string literal.
fn extract_glob_prefix(pattern: &Expr) -> Option<String> {
    if let Expr::Literal(Literal::String(s), _) = pattern {
        let mut prefix = String::new();
        for ch in s.chars() {
            if matches!(ch, '*' | '?' | '[') {
                break;
            }
            prefix.push(ch);
        }
        if prefix.is_empty() {
            None
        } else {
            Some(prefix)
        }
    } else {
        None
    }
}

/// Compute the exclusive upper bound for a LIKE prefix range.
///
/// Example: `"abc"` becomes `"abd"` so the planner can model:
/// `column >= "abc"` and `column < "abd"`.
/// Returns `None` when no valid successor exists.
fn like_prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    for idx in (0..chars.len()).rev() {
        let codepoint = u32::from(chars[idx]);
        if codepoint == u32::from(char::MAX) {
            continue;
        }
        if let Some(next) = char::from_u32(codepoint + 1) {
            chars[idx] = next;
            chars.truncate(idx + 1);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

/// Determine the usability of an index for a set of WHERE terms.
///
/// Rules from §10.5, extended for multi-column indexes:
/// - Walk the index columns left-to-right; for each column, check if the WHERE
///   has an equality constraint. The equality prefix can be extended as long as
///   consecutive leading columns have equality terms.
/// - After the equality prefix, check for a range/BETWEEN on the next column.
/// - For single-column leftmost matches, also check IN and LIKE prefix.
/// - For expression indexes, match query expressions structurally against the
///   index's expression columns.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn analyze_index_usability(index: &IndexInfo, terms: &[WhereTerm<'_>]) -> IndexUsability {
    if index.columns.is_empty() {
        return IndexUsability::NotUsable;
    }

    // --- Expression index matching ---
    // If the index has expression columns, try to match WHERE terms against
    // the expressions structurally (AST PartialEq) rather than by column name.
    if !index.expression_columns.is_empty() {
        return analyze_expression_index_usability(index, terms);
    }

    // Helper: check if a WHERE column matches an index column, respecting
    // the table qualifier when present.  Unqualified columns (table = None)
    // are conservatively considered matching.
    let col_matches = |wc: &WhereColumn, idx_col: &str| -> bool {
        wc.column.eq_ignore_ascii_case(idx_col)
            && wc
                .table
                .as_ref()
                .is_none_or(|t| t.eq_ignore_ascii_case(&index.table))
    };

    let leftmost = &index.columns[0];

    // --- Multi-column equality prefix ---
    // Walk index columns left-to-right, counting how many have equality terms.
    let mut eq_columns = 0;
    for idx_col in &index.columns {
        let has_eq = terms.iter().any(|t| {
            t.column.as_ref().is_some_and(|wc| {
                col_matches(wc, idx_col) && matches!(t.kind, WhereTermKind::Equality)
            })
        });
        if has_eq {
            eq_columns += 1;
        } else {
            break;
        }
    }

    // If we have equality on 2+ columns, return MultiColumnEquality.
    if eq_columns >= 2 {
        // Check for trailing range on the next column after the prefix.
        let has_trailing_range = if eq_columns < index.columns.len() {
            let next_col = &index.columns[eq_columns];
            terms.iter().any(|t| {
                t.column.as_ref().is_some_and(|wc| {
                    col_matches(wc, next_col)
                        && matches!(t.kind, WhereTermKind::Range | WhereTermKind::Between)
                })
            })
        } else {
            false
        };
        return IndexUsability::MultiColumnEquality {
            eq_columns,
            has_trailing_range,
        };
    }

    // --- Single leftmost column checks (original logic) ---
    // Check for equality on the leftmost column.
    for term in terms {
        if let Some(ref wc) = term.column {
            if col_matches(wc, leftmost) {
                match &term.kind {
                    WhereTermKind::Equality => return IndexUsability::Equality,
                    WhereTermKind::InList { count } => {
                        return IndexUsability::InExpansion {
                            probe_count: *count,
                        };
                    }
                    WhereTermKind::LikePrefix {
                        prefix,
                        upper_bound,
                    } => {
                        return IndexUsability::LikePrefix {
                            low: prefix.clone(),
                            high: upper_bound.clone(),
                        };
                    }
                    _ => {}
                }
            }
        }
    }

    // Check for range on the leftmost column.
    for term in terms {
        if let Some(ref wc) = term.column {
            if col_matches(wc, leftmost)
                && matches!(term.kind, WhereTermKind::Range | WhereTermKind::Between)
            {
                return IndexUsability::Range {
                    selectivity: DEFAULT_RANGE_SELECTIVITY,
                };
            }
        }
    }

    IndexUsability::NotUsable
}

/// Analyze usability for an expression index by matching WHERE term expressions
/// against the index's expression columns using structural equality (AST `PartialEq`).
fn analyze_expression_index_usability(
    index: &IndexInfo,
    terms: &[WhereTerm<'_>],
) -> IndexUsability {
    // For expression indexes, we check if any WHERE equality term's left-side
    // expression structurally matches the index's first expression column.
    if let Some(first_expr) = index.expression_columns.first() {
        for term in terms {
            if matches!(term.kind, WhereTermKind::Equality) {
                // Check if the term's expression is `expr_col = value` where
                // expr_col matches the index expression.
                if let Expr::BinaryOp { left, .. } = term.expr {
                    if **left == *first_expr {
                        return IndexUsability::Equality;
                    }
                }
            }
            if matches!(term.kind, WhereTermKind::Range | WhereTermKind::Between) {
                if let Expr::BinaryOp { left, .. } | Expr::Between { expr: left, .. } = term.expr {
                    if **left == *first_expr {
                        return IndexUsability::Range {
                            selectivity: DEFAULT_RANGE_SELECTIVITY,
                        };
                    }
                }
            }
        }
    }
    IndexUsability::NotUsable
}

/// Default selectivity for range constraints when no ANALYZE data is available.
const DEFAULT_RANGE_SELECTIVITY: f64 = 0.33;
const SKIP_SCAN_EQ_SELECTIVITY: f64 = 0.01;
const SKIP_SCAN_RANGE_SELECTIVITY: f64 = 0.20;
const SKIP_SCAN_MAX_LEADING_DISTINCT: u64 = 16;
const SKIP_SCAN_PAGES_PER_LEADING_DISTINCT: u64 = 8;

fn estimate_skip_scan_leading_distinct(index: &IndexInfo) -> u64 {
    (index.n_pages / SKIP_SCAN_PAGES_PER_LEADING_DISTINCT).max(1)
}

fn analyze_skip_scan_candidate(
    table: &TableStats,
    index: &IndexInfo,
    terms: &[WhereTerm<'_>],
) -> Option<SkipScanCandidate> {
    if index.columns.len() < 2
        || (!matches!(table.source, StatsSource::Analyze)
            && !matches!(index.source, StatsSource::Analyze))
    {
        return None;
    }

    let col_matches = |wc: &WhereColumn, idx_col: &str| -> bool {
        wc.column.eq_ignore_ascii_case(idx_col)
            && wc
                .table
                .as_ref()
                .is_none_or(|t| t.eq_ignore_ascii_case(&index.table))
    };

    let leading_col = &index.columns[0];
    let leading_constrained = terms.iter().any(|term| {
        term.column.as_ref().is_some_and(|wc| {
            col_matches(wc, leading_col)
                && matches!(
                    term.kind,
                    WhereTermKind::Equality
                        | WhereTermKind::Range
                        | WhereTermKind::Between
                        | WhereTermKind::InList { .. }
                        | WhereTermKind::LikePrefix { .. }
                )
        })
    });
    if leading_constrained {
        return None;
    }

    let leading_distinct = estimate_skip_scan_leading_distinct(index);
    if leading_distinct > SKIP_SCAN_MAX_LEADING_DISTINCT {
        return None;
    }

    for idx_col in index.columns.iter().skip(1) {
        for term in terms {
            let Some(wc) = term.column.as_ref() else {
                continue;
            };
            if !col_matches(wc, idx_col) {
                continue;
            }

            let (trailing_probe_count, per_probe_selectivity) = match term.kind {
                WhereTermKind::Equality => (1, SKIP_SCAN_EQ_SELECTIVITY),
                WhereTermKind::InList { count } if count > 0 => (count, SKIP_SCAN_EQ_SELECTIVITY),
                WhereTermKind::Range
                | WhereTermKind::Between
                | WhereTermKind::LikePrefix { .. } => (1, SKIP_SCAN_RANGE_SELECTIVITY),
                _ => continue,
            };

            return Some(SkipScanCandidate {
                leading_probes: leading_distinct as usize,
                trailing_probe_count,
                per_probe_selectivity,
            });
        }
    }

    None
}

// ---------------------------------------------------------------------------
// Join ordering: bounded beam search (§10.5)
// ---------------------------------------------------------------------------

/// Compute the `mxChoice` beam width from the number of tables in the join.
///
/// From §10.5 / C SQLite's `computeMxChoice`:
/// - 1 for single-table queries
/// - 5 for two-table joins
/// - 12 for 3+ table joins (18 if star-query heuristic applies)
#[must_use]
pub fn compute_mx_choice(n_tables: usize, is_star: bool) -> usize {
    match n_tables {
        0 | 1 => 1,
        2 => 5,
        _ => {
            if is_star {
                18
            } else {
                12
            }
        }
    }
}

/// Detect a star-query pattern: one table joins to all other tables.
///
/// A star query has a central "fact" table that every dimension table
/// has a direct join predicate with.
#[must_use]
pub fn detect_star_query(tables: &[TableStats], where_terms: &[WhereTerm<'_>]) -> bool {
    if tables.len() < 3 {
        return false;
    }

    // For each table, count how many OTHER tables it shares a join predicate with.
    let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();

    for candidate in &table_names {
        let mut join_partners = 0usize;
        for other in &table_names {
            if *other == *candidate {
                continue;
            }
            if has_join_predicate(candidate, other, where_terms) {
                join_partners += 1;
            }
        }
        if join_partners == table_names.len() - 1 {
            return true;
        }
    }
    false
}

/// Check if two tables share a join predicate in the WHERE terms.
fn has_join_predicate(table_a: &str, table_b: &str, terms: &[WhereTerm<'_>]) -> bool {
    for term in terms {
        if let Expr::BinaryOp {
            left,
            op: AstBinaryOp::Eq,
            right,
            ..
        } = term.expr
        {
            let left_col = extract_where_column(left);
            let right_col = extract_where_column(right);
            if let (Some(lc), Some(rc)) = (left_col, right_col) {
                let lt = lc.table.as_deref().unwrap_or("");
                let rt = rc.table.as_deref().unwrap_or("");
                if (lt.eq_ignore_ascii_case(table_a) && rt.eq_ignore_ascii_case(table_b))
                    || (lt.eq_ignore_ascii_case(table_b) && rt.eq_ignore_ascii_case(table_a))
                {
                    return true;
                }
            }
        }
    }
    false
}

const HASH_JOIN_SELECTIVITY_HEURISTIC: f64 = 0.25;
const LEAPFROG_SEEK_OVERHEAD_FACTOR: f64 = 0.20;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ColumnKey {
    table: String,
    column: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EquiJoinPredicate {
    left: ColumnKey,
    right: ColumnKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TrieHypergraph {
    relation_variables: Vec<Vec<usize>>,
    variable_count: usize,
    arity: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<usize>,
}

impl UnionFind {
    fn new(size: usize) -> Self {
        Self {
            parent: (0..size).collect(),
            rank: vec![0; size],
        }
    }

    fn find(&mut self, idx: usize) -> usize {
        if self.parent[idx] != idx {
            let root = self.find(self.parent[idx]);
            self.parent[idx] = root;
        }
        self.parent[idx]
    }

    fn union(&mut self, left: usize, right: usize) {
        let left_root = self.find(left);
        let right_root = self.find(right);
        if left_root == right_root {
            return;
        }
        let left_rank = self.rank[left_root];
        let right_rank = self.rank[right_root];
        match left_rank.cmp(&right_rank) {
            std::cmp::Ordering::Less => {
                self.parent[left_root] = right_root;
            }
            std::cmp::Ordering::Greater => {
                self.parent[right_root] = left_root;
            }
            std::cmp::Ordering::Equal => {
                self.parent[right_root] = left_root;
                self.rank[left_root] = left_rank + 1;
            }
        }
    }
}

/// Select join operator segments for a query plan.
///
/// This function is additive to `order_joins`: it annotates a chosen join order
/// with hash vs Leapfrog routing decisions and can be called directly by higher
/// layers that have `FROM`-clause shape information.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn choose_join_segments(
    join_order: &[String],
    tables: &[TableStats],
    where_terms: &[WhereTerm<'_>],
    from_clause: Option<&FromClause>,
    feature_flags: PlannerFeatureFlags,
) -> Vec<JoinPlanSegment> {
    if join_order.len() < 2 {
        return vec![];
    }

    let join_order_canonical = join_order
        .iter()
        .map(|table| canonical_table_key(table))
        .collect::<Vec<_>>();

    let canonical_to_original = join_order
        .iter()
        .map(|table| (canonical_table_key(table), table.clone()))
        .collect::<HashMap<_, _>>();

    let join_table_set = join_order_canonical.iter().cloned().collect::<HashSet<_>>();
    let rows_by_table = build_table_row_map(tables, &join_order_canonical);
    let (equi_predicates, theta_join_tables) =
        collect_join_predicates(where_terms, &join_table_set);
    let leapfrog_shape_supported = from_clause_supports_leapfrog(from_clause);

    let mut selected_components: Vec<(Vec<String>, f64, f64, usize)> = vec![];
    let mut selected_tables = HashSet::<String>::new();

    if feature_flags.leapfrog_join && leapfrog_shape_supported {
        let leapfrog_candidates = join_order_canonical
            .iter()
            .filter(|table| !theta_join_tables.contains(*table))
            .cloned()
            .collect::<Vec<_>>();

        for component in connected_components(&leapfrog_candidates, &equi_predicates) {
            if component.len() < 3 {
                continue;
            }
            let component_set = component.iter().cloned().collect::<HashSet<_>>();
            let ordered_component = ordered_subset(&join_order_canonical, &component_set);
            let Some(hypergraph) = build_trie_hypergraph(&ordered_component, &equi_predicates)
            else {
                continue;
            };
            let hash_cost = estimate_pairwise_hash_join_cost(&ordered_component, &rows_by_table);
            let Some(agm_bound) =
                estimate_agm_upper_bound(&ordered_component, &rows_by_table, &hypergraph)
            else {
                continue;
            };
            let leapfrog_cost = agm_bound
                * LEAPFROG_SEEK_OVERHEAD_FACTOR.mul_add(ordered_component.len() as f64, 1.0);
            if leapfrog_cost < hash_cost {
                for table in &ordered_component {
                    selected_tables.insert(table.clone());
                }
                selected_components.push((
                    ordered_component,
                    leapfrog_cost,
                    hash_cost,
                    hypergraph.arity,
                ));
            }
        }
    }

    let mut segments = selected_components
        .into_iter()
        .map(
            |(relations, leapfrog_cost, hash_cost, arity)| JoinPlanSegment {
                relations: relations
                    .into_iter()
                    .filter_map(|table| canonical_to_original.get(&table).cloned())
                    .collect(),
                operator: JoinOperator::LeapfrogTriejoin,
                estimated_cost: leapfrog_cost,
                reason: format!(
                    "AGM estimate {:.1} beats hash cost {:.1}; trie arity {}",
                    leapfrog_cost, hash_cost, arity
                ),
            },
        )
        .collect::<Vec<_>>();

    if segments.is_empty() {
        let hash_cost = estimate_pairwise_hash_join_cost(&join_order_canonical, &rows_by_table);
        let reason = if !feature_flags.leapfrog_join {
            "leapfrog_join feature flag disabled".to_owned()
        } else if !leapfrog_shape_supported {
            "outer/natural/theta join shape is not Leapfrog-compatible".to_owned()
        } else if join_order.len() < 3 {
            "2-way joins stay on pairwise hash join".to_owned()
        } else if !theta_join_tables.is_empty() {
            "theta/non-equi join predicates require hash fallback".to_owned()
        } else {
            "no compatible 3+ equi-join component with lower AGM estimate".to_owned()
        };
        return vec![JoinPlanSegment {
            relations: join_order.to_vec(),
            operator: JoinOperator::HashJoin,
            estimated_cost: hash_cost,
            reason,
        }];
    }

    let remaining_tables = join_order_canonical
        .iter()
        .filter(|table| !selected_tables.contains(*table))
        .cloned()
        .collect::<Vec<_>>();
    if remaining_tables.len() >= 2 {
        let hash_cost = estimate_pairwise_hash_join_cost(&remaining_tables, &rows_by_table);
        segments.push(JoinPlanSegment {
            relations: remaining_tables
                .iter()
                .filter_map(|table| canonical_to_original.get(table).cloned())
                .collect(),
            operator: JoinOperator::HashJoin,
            estimated_cost: hash_cost,
            reason: "remaining joins use pairwise hash join".to_owned(),
        });
    }

    let join_order_position = join_order_canonical
        .iter()
        .enumerate()
        .map(|(idx, table)| (table.clone(), idx))
        .collect::<HashMap<_, _>>();
    segments.sort_by_key(|segment| {
        segment
            .relations
            .first()
            .and_then(|table| {
                join_order_position
                    .get(&canonical_table_key(table))
                    .copied()
            })
            .unwrap_or(usize::MAX)
    });
    segments
}

fn build_table_row_map(
    tables: &[TableStats],
    join_order_canonical: &[String],
) -> HashMap<String, f64> {
    let mut rows_by_table = tables
        .iter()
        .map(|table| (canonical_table_key(&table.name), table.n_rows.max(1) as f64))
        .collect::<HashMap<_, _>>();
    for table in join_order_canonical {
        rows_by_table.entry(table.clone()).or_insert(1.0);
    }
    rows_by_table
}

fn collect_join_predicates(
    where_terms: &[WhereTerm<'_>],
    join_table_set: &HashSet<String>,
) -> (Vec<EquiJoinPredicate>, HashSet<String>) {
    let mut equi_predicates = Vec::new();
    let mut theta_join_tables = HashSet::new();

    for term in where_terms {
        let Expr::BinaryOp {
            left, op, right, ..
        } = term.expr
        else {
            continue;
        };
        let Some(left_col) = extract_qualified_column(left) else {
            continue;
        };
        let Some(right_col) = extract_qualified_column(right) else {
            continue;
        };
        if left_col.table == right_col.table {
            continue;
        }
        if !join_table_set.contains(&left_col.table) || !join_table_set.contains(&right_col.table) {
            continue;
        }

        if *op == AstBinaryOp::Eq {
            equi_predicates.push(EquiJoinPredicate {
                left: left_col,
                right: right_col,
            });
        } else {
            theta_join_tables.insert(left_col.table);
            theta_join_tables.insert(right_col.table);
        }
    }

    (equi_predicates, theta_join_tables)
}

fn extract_qualified_column(expr: &Expr) -> Option<ColumnKey> {
    let Expr::Column(column_ref, _) = expr else {
        return None;
    };
    let table = column_ref.table.as_ref()?;
    Some(ColumnKey {
        table: canonical_table_key(table),
        column: column_ref.column.to_ascii_lowercase(),
    })
}

fn connected_components(tables: &[String], predicates: &[EquiJoinPredicate]) -> Vec<Vec<String>> {
    if tables.is_empty() {
        return vec![];
    }

    let table_set = tables.iter().cloned().collect::<HashSet<_>>();
    let mut adjacency = tables
        .iter()
        .map(|table| (table.clone(), HashSet::<String>::new()))
        .collect::<HashMap<_, _>>();

    for predicate in predicates {
        if table_set.contains(&predicate.left.table) && table_set.contains(&predicate.right.table) {
            adjacency
                .entry(predicate.left.table.clone())
                .or_default()
                .insert(predicate.right.table.clone());
            adjacency
                .entry(predicate.right.table.clone())
                .or_default()
                .insert(predicate.left.table.clone());
        }
    }

    let mut visited = HashSet::<String>::new();
    let mut components = Vec::new();
    for table in tables {
        if visited.contains(table) {
            continue;
        }
        let mut stack = vec![table.clone()];
        let mut component = Vec::new();
        while let Some(current) = stack.pop() {
            if !visited.insert(current.clone()) {
                continue;
            }
            component.push(current.clone());
            if let Some(neighbors) = adjacency.get(&current) {
                for neighbor in neighbors {
                    if !visited.contains(neighbor) {
                        stack.push(neighbor.clone());
                    }
                }
            }
        }
        components.push(component);
    }

    components
}

fn ordered_subset(join_order: &[String], selected_tables: &HashSet<String>) -> Vec<String> {
    join_order
        .iter()
        .filter(|table| selected_tables.contains(*table))
        .cloned()
        .collect()
}

fn estimate_pairwise_hash_join_cost(
    component: &[String],
    rows_by_table: &HashMap<String, f64>,
) -> f64 {
    if component.len() < 2 {
        return 0.0;
    }

    let mut iter = component.iter();
    let first_rows = iter
        .next()
        .and_then(|table| rows_by_table.get(table))
        .copied()
        .unwrap_or(1.0)
        .max(1.0);
    let mut intermediate_rows = first_rows;
    let mut total_cost = 0.0;

    for table in iter {
        let relation_rows = rows_by_table.get(table).copied().unwrap_or(1.0).max(1.0);
        total_cost += intermediate_rows.min(relation_rows) + intermediate_rows.max(relation_rows);
        intermediate_rows =
            (intermediate_rows * relation_rows * HASH_JOIN_SELECTIVITY_HEURISTIC).max(1.0);
    }

    total_cost
}

#[allow(clippy::too_many_lines)]
fn build_trie_hypergraph(
    component: &[String],
    predicates: &[EquiJoinPredicate],
) -> Option<TrieHypergraph> {
    if component.len() < 2 {
        return None;
    }

    let component_set = component.iter().cloned().collect::<HashSet<_>>();
    let table_to_index = component
        .iter()
        .enumerate()
        .map(|(idx, table)| (table.clone(), idx))
        .collect::<HashMap<_, _>>();

    let mut endpoint_ids = HashMap::<ColumnKey, usize>::new();
    let mut edge_endpoint_pairs = Vec::<(usize, usize, String, String)>::new();
    for predicate in predicates {
        if !component_set.contains(&predicate.left.table)
            || !component_set.contains(&predicate.right.table)
        {
            continue;
        }
        let left_entry = if let Some(existing) = endpoint_ids.get(&predicate.left).copied() {
            existing
        } else {
            let next = endpoint_ids.len();
            endpoint_ids.insert(predicate.left.clone(), next);
            next
        };
        let right_entry = if let Some(existing) = endpoint_ids.get(&predicate.right).copied() {
            existing
        } else {
            let next = endpoint_ids.len();
            endpoint_ids.insert(predicate.right.clone(), next);
            next
        };
        edge_endpoint_pairs.push((
            left_entry,
            right_entry,
            predicate.left.table.clone(),
            predicate.right.table.clone(),
        ));
    }

    if edge_endpoint_pairs.is_empty() {
        return None;
    }

    let mut union_find = UnionFind::new(endpoint_ids.len());
    for (left_id, right_id, _, _) in &edge_endpoint_pairs {
        union_find.union(*left_id, *right_id);
    }

    let mut root_to_variable = HashMap::<usize, usize>::new();
    let mut relation_variable_sets = vec![HashSet::<usize>::new(); component.len()];
    for (left_id, right_id, left_table, right_table) in edge_endpoint_pairs {
        let left_root = union_find.find(left_id);
        let right_root = union_find.find(right_id);
        let left_variable = if let Some(existing) = root_to_variable.get(&left_root).copied() {
            existing
        } else {
            let next = root_to_variable.len();
            root_to_variable.insert(left_root, next);
            next
        };
        let right_variable = if let Some(existing) = root_to_variable.get(&right_root).copied() {
            existing
        } else {
            let next = root_to_variable.len();
            root_to_variable.insert(right_root, next);
            next
        };
        let left_index = *table_to_index.get(&left_table)?;
        let right_index = *table_to_index.get(&right_table)?;
        relation_variable_sets[left_index].insert(left_variable);
        relation_variable_sets[right_index].insert(right_variable);
    }

    if relation_variable_sets.iter().any(HashSet::is_empty) {
        return None;
    }
    let expected_arity = relation_variable_sets.first()?.len();
    if expected_arity == 0
        || relation_variable_sets
            .iter()
            .any(|variables| variables.len() != expected_arity)
    {
        return None;
    }

    let variable_count = root_to_variable.len();
    let mut variable_degree = vec![0usize; variable_count];
    for variables in &relation_variable_sets {
        for variable in variables {
            variable_degree[*variable] += 1;
        }
    }
    if variable_degree.iter().any(|degree| *degree < 2) {
        return None;
    }

    let relation_variables = relation_variable_sets
        .into_iter()
        .map(|variables| {
            let mut ordered = variables.into_iter().collect::<Vec<_>>();
            ordered.sort_unstable();
            ordered
        })
        .collect::<Vec<_>>();

    Some(TrieHypergraph {
        relation_variables,
        variable_count,
        arity: expected_arity,
    })
}

fn estimate_agm_upper_bound(
    component: &[String],
    rows_by_table: &HashMap<String, f64>,
    hypergraph: &TrieHypergraph,
) -> Option<f64> {
    if component.len() != hypergraph.relation_variables.len() || hypergraph.variable_count == 0 {
        return None;
    }

    let mut variable_degree = vec![0usize; hypergraph.variable_count];
    for variables in &hypergraph.relation_variables {
        for variable in variables {
            variable_degree[*variable] += 1;
        }
    }

    let mut bound = 1.0;
    for (relation_idx, table) in component.iter().enumerate() {
        let row_count = rows_by_table.get(table).copied().unwrap_or(1.0).max(1.0);
        let exponent = hypergraph.relation_variables[relation_idx]
            .iter()
            .map(|variable| 1.0 / variable_degree[*variable] as f64)
            .fold(0.0, f64::max);
        bound *= row_count.powf(exponent);
    }
    Some(bound.max(1.0))
}

fn from_clause_supports_leapfrog(from_clause: Option<&FromClause>) -> bool {
    let Some(from_clause) = from_clause else {
        return true;
    };

    for join in &from_clause.joins {
        if join.join_type.natural {
            return false;
        }
        if !matches!(join.join_type.kind, JoinKind::Inner | JoinKind::Cross) {
            return false;
        }
        if let Some(constraint) = &join.constraint {
            match constraint {
                JoinConstraint::Using(columns) => {
                    if columns.is_empty() {
                        return false;
                    }
                }
                JoinConstraint::On(expr) => {
                    let conjuncts = decompose_where(expr);
                    if conjuncts.is_empty() {
                        return false;
                    }
                    if conjuncts
                        .iter()
                        .any(|conjunct| !expression_is_equi_column_predicate(conjunct))
                    {
                        return false;
                    }
                }
            }
        }
    }

    true
}

fn expression_is_equi_column_predicate(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::BinaryOp {
            left,
            op: AstBinaryOp::Eq,
            right,
            ..
        } if extract_where_column(left).is_some() && extract_where_column(right).is_some()
    )
}

/// A partial join path during beam search.
#[derive(Debug, Clone)]
struct PartialPath {
    /// Tables joined so far, in order.
    tables: Vec<String>,
    /// Access paths for each table.
    access_paths: Vec<AccessPath>,
    /// Cumulative cost.
    cost: f64,
    /// Product of estimated rows across all tables joined so far.
    cumulative_rows: f64,
}

/// Order tables using bounded beam search (NGQP-style, §10.5).
///
/// Maintains up to `mxChoice` best partial paths at each level, pruning
/// suboptimal paths early. Complexity: `O(mxChoice * N^2)`, not `N!`.
///
/// # Arguments
///
/// - `tables`: Statistics for each table in the FROM clause.
/// - `indexes`: All available indexes.
/// - `where_terms`: Classified WHERE terms.
/// - `needed_columns`: Columns needed in the result (for covering index detection).
/// - `cross_join_pairs`: Pairs of tables that are `CROSS JOIN`ed (prevents reordering).
#[must_use]
pub fn order_joins(
    tables: &[TableStats],
    indexes: &[IndexInfo],
    where_terms: &[WhereTerm<'_>],
    needed_columns: Option<&[String]>,
    cross_join_pairs: &[(String, String)],
) -> QueryPlan {
    order_joins_with_hints(
        tables,
        indexes,
        where_terms,
        needed_columns,
        cross_join_pairs,
        None,
        None,
    )
}

fn join_access_path(
    table: &TableStats,
    indexes: &[IndexInfo],
    where_terms: &[WhereTerm<'_>],
    needed_columns: Option<&[String]>,
    table_index_hints: Option<&BTreeMap<String, IndexHint>>,
    cracking_hints: Option<&CrackingHintStore>,
) -> AccessPath {
    let explicit_hint = lookup_table_index_hint(&table.name, table_index_hints);
    let adaptive_hint = cracking_hints.and_then(|store| store.preferred_index(&table.name));
    best_access_path_internal(
        table,
        indexes,
        where_terms,
        needed_columns,
        explicit_hint,
        adaptive_hint,
    )
}

/// Order tables using bounded beam search while honoring table-level
/// `INDEXED BY`/`NOT INDEXED` hints and optional adaptive cracking hints.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn order_joins_with_hints(
    tables: &[TableStats],
    indexes: &[IndexInfo],
    where_terms: &[WhereTerm<'_>],
    needed_columns: Option<&[String]>,
    cross_join_pairs: &[(String, String)],
    table_index_hints: Option<&BTreeMap<String, IndexHint>>,
    cracking_hints: Option<&mut CrackingHintStore>,
) -> QueryPlan {
    order_joins_with_hints_and_features(
        tables,
        indexes,
        where_terms,
        needed_columns,
        cross_join_pairs,
        table_index_hints,
        cracking_hints,
        PlannerFeatureFlags::default(),
    )
}

/// Order tables using bounded beam search and select join operators (hash vs
/// Leapfrog Triejoin) based on feature flags and cost model.
#[must_use]
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub fn order_joins_with_hints_and_features(
    tables: &[TableStats],
    indexes: &[IndexInfo],
    where_terms: &[WhereTerm<'_>],
    needed_columns: Option<&[String]>,
    cross_join_pairs: &[(String, String)],
    table_index_hints: Option<&BTreeMap<String, IndexHint>>,
    cracking_hints: Option<&mut CrackingHintStore>,
    feature_flags: PlannerFeatureFlags,
) -> QueryPlan {
    let n = tables.len();

    if n == 0 {
        return QueryPlan {
            join_order: vec![],
            access_paths: vec![],
            join_segments: vec![],
            total_cost: 0.0,
        };
    }

    if n == 1 {
        let ap = join_access_path(
            &tables[0],
            indexes,
            where_terms,
            needed_columns,
            table_index_hints,
            cracking_hints.as_deref(),
        );
        let plan = QueryPlan {
            join_order: vec![tables[0].name.clone()],
            access_paths: vec![ap.clone()],
            join_segments: vec![],
            total_cost: ap.estimated_cost,
        };
        if let Some(store) = cracking_hints {
            for access_path in &plan.access_paths {
                store.record_access_path(access_path);
            }
        }
        FSQLITE_PLANNER_PLANS_ENUMERATED.fetch_add(1, Ordering::Relaxed);
        return plan;
    }

    if n <= DPCCP_MAX_TABLES {
        if let Some((order_indices, total_cost, plans_counted, branches_pruned)) = dpccp_order_joins(
            tables,
            indexes,
            where_terms,
            needed_columns,
            table_index_hints,
            cross_join_pairs,
            cracking_hints.as_deref(),
        ) {
            let join_order = order_indices
                .iter()
                .map(|idx| tables[*idx].name.clone())
                .collect::<Vec<_>>();
            let access_paths = order_indices
                .iter()
                .map(|idx| {
                    join_access_path(
                        &tables[*idx],
                        indexes,
                        where_terms,
                        needed_columns,
                        table_index_hints,
                        cracking_hints.as_deref(),
                    )
                })
                .collect::<Vec<_>>();
            let join_segments =
                choose_join_segments(&join_order, tables, where_terms, None, feature_flags);
            let plan = QueryPlan {
                join_order,
                access_paths,
                join_segments,
                total_cost,
            };

            if let Some(store) = cracking_hints {
                for access_path in &plan.access_paths {
                    store.record_access_path(access_path);
                }
            }

            FSQLITE_PLANNER_PLANS_ENUMERATED.fetch_add(plans_counted, Ordering::Relaxed);

            tracing::debug!(
                join_order = ?plan.join_order,
                total_cost = plan.total_cost,
                table_count = n,
                plans_enumerated = plans_counted,
                branches_pruned,
                threshold = DPCCP_MAX_TABLES,
                algorithm = "dpccp_exhaustive",
                "planner.order_joins.complete"
            );

            tracing::info!(
                join_order = ?plan.join_order,
                total_cost = plan.total_cost,
                table_count = n,
                plans_enumerated = plans_counted,
                branches_pruned,
                algorithm = "dpccp_exhaustive",
                "planner.plan_selected"
            );

            return plan;
        }

        tracing::debug!(
            table_count = n,
            threshold = DPCCP_MAX_TABLES,
            "planner.dpccp.no_plan_fallback_greedy"
        );
    }

    let mut plans_enumerated: u64 = 0;

    let is_star = detect_star_query(tables, where_terms);
    let mx_choice = if n > DPCCP_MAX_TABLES {
        // For large joins, use a greedy-width search (single best partial path).
        1
    } else {
        compute_mx_choice(n, is_star)
    };

    // Seed: start with each table as a single-element path.
    // Skip tables that are blocked by CROSS JOIN constraints (right side of a
    // cross-join pair cannot appear unless the left side is already visited).
    let mut paths: Vec<PartialPath> = Vec::with_capacity(n);
    for t in tables {
        if !cross_join_allowed(&[], &t.name, cross_join_pairs) {
            continue;
        }
        let ap = join_access_path(
            t,
            indexes,
            where_terms,
            needed_columns,
            table_index_hints,
            cracking_hints.as_deref(),
        );
        let cumulative_rows = ap.estimated_rows;
        paths.push(PartialPath {
            tables: vec![t.name.clone()],
            access_paths: vec![ap.clone()],
            cost: ap.estimated_cost,
            cumulative_rows,
        });
    }
    paths.sort_by(|a, b| {
        a.cost
            .partial_cmp(&b.cost)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    paths.truncate(mx_choice);

    // Extend paths one table at a time.
    for level in 1..n {
        let mut next_paths: Vec<PartialPath> = Vec::with_capacity(paths.len() * (n - level));

        for path in &paths {
            for t in tables {
                // Skip if already in this path.
                if path
                    .tables
                    .iter()
                    .any(|existing| existing.eq_ignore_ascii_case(&t.name))
                {
                    continue;
                }

                // Check CROSS JOIN constraint: if (last_in_path, t) is a cross-join
                // pair, only allow adding t if it's the next in the original order.
                if !cross_join_allowed(&path.tables, &t.name, cross_join_pairs) {
                    continue;
                }

                let ap = join_access_path(
                    t,
                    indexes,
                    where_terms,
                    needed_columns,
                    table_index_hints,
                    cracking_hints.as_deref(),
                );
                // Scale inner table cost by the cumulative cardinality of
                // all outer tables (nested loop model).  For a 3-table join
                // T1⋈T2⋈T3, T3 executes once per (T1, T2) pair.
                let outer_rows = path.cumulative_rows;
                let inner_cost = ap.estimated_cost * outer_rows;

                let mut new_tables = path.tables.clone();
                new_tables.push(t.name.clone());
                let mut new_aps = path.access_paths.clone();
                new_aps.push(ap.clone());
                let new_cost = path.cost + inner_cost;
                let new_cumulative_rows = path.cumulative_rows * ap.estimated_rows;

                plans_enumerated += 1;
                tracing::debug!(
                    target: "fsqlite.planner",
                    tables = ?new_tables,
                    cost = new_cost,
                    "planner.candidate_plan"
                );

                next_paths.push(PartialPath {
                    tables: new_tables,
                    access_paths: new_aps,
                    cost: new_cost,
                    cumulative_rows: new_cumulative_rows,
                });
            }
        }

        next_paths.sort_by(|a, b| {
            a.cost
                .partial_cmp(&b.cost)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        next_paths.truncate(mx_choice);
        paths = next_paths;
    }

    // Pick the lowest-cost complete path.  If CROSS JOIN constraints
    // eliminated all seed paths (shouldn't happen with valid SQL but
    // guard defensively), fall back to seeding every table.
    if paths.is_empty() {
        for t in tables {
            let ap = join_access_path(
                t,
                indexes,
                where_terms,
                needed_columns,
                table_index_hints,
                cracking_hints.as_deref(),
            );
            paths.push(PartialPath {
                tables: vec![t.name.clone()],
                access_paths: vec![ap.clone()],
                cost: ap.estimated_cost,
                cumulative_rows: ap.estimated_rows,
            });
        }
    }

    let best = paths
        .into_iter()
        .min_by(|a, b| {
            a.cost
                .partial_cmp(&b.cost)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .expect("tables must be non-empty (checked n == 0 above)");

    let join_segments =
        choose_join_segments(&best.tables, tables, where_terms, None, feature_flags);

    let plan = QueryPlan {
        join_order: best.tables,
        access_paths: best.access_paths,
        join_segments,
        total_cost: best.cost,
    };

    if let Some(store) = cracking_hints {
        for access_path in &plan.access_paths {
            store.record_access_path(access_path);
        }
    }

    FSQLITE_PLANNER_PLANS_ENUMERATED.fetch_add(plans_enumerated, Ordering::Relaxed);

    let span = tracing::info_span!(
        target: "fsqlite.planner",
        "join_ordering",
        tables_count = n,
        plans_enumerated,
        selected_cost = plan.total_cost,
    );
    let _g = span.enter();

    tracing::debug!(
        join_order = ?plan.join_order,
        total_cost = plan.total_cost,
        beam_width = mx_choice,
        star_query = is_star,
        table_count = n,
        index_hint_entries = table_index_hints.map_or(0, BTreeMap::len),
        algorithm = "greedy_width",
        threshold = DPCCP_MAX_TABLES,
        "planner.order_joins.complete"
    );

    tracing::info!(
        join_order = ?plan.join_order,
        total_cost = plan.total_cost,
        table_count = n,
        plans_enumerated,
        algorithm = "greedy_width",
        "planner.plan_selected"
    );

    plan
}

/// Check that adding `candidate` to `current_path` does not violate any
/// CROSS JOIN ordering constraint.
fn cross_join_allowed(
    current_path: &[String],
    candidate: &str,
    cross_join_pairs: &[(String, String)],
) -> bool {
    for (left, right) in cross_join_pairs {
        // If (left, right) is a cross join pair, right can only appear after left.
        if right.eq_ignore_ascii_case(candidate)
            && !current_path.iter().any(|t| t.eq_ignore_ascii_case(left))
        {
            return false;
        }
    }
    true
}

fn cross_join_allowed_indices(
    current_path: &[usize],
    candidate: &str,
    tables: &[TableStats],
    cross_join_pairs: &[(String, String)],
) -> bool {
    for (left, right) in cross_join_pairs {
        if right.eq_ignore_ascii_case(candidate)
            && !current_path
                .iter()
                .any(|idx| tables[*idx].name.eq_ignore_ascii_case(left))
        {
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------------------
// DPccp: exhaustive join ordering for small join counts (bd-1as.3)
// ---------------------------------------------------------------------------

/// Exhaustive join-order search for small joins (`n <= DPCCP_MAX_TABLES`).
///
/// Enumerates permutations with branch-and-bound pruning:
/// - explores candidate next tables in deterministic cost order
/// - prunes any partial branch whose cost already exceeds best complete plan
/// - returns the best order, total cost, enumerated candidates, pruned branches
#[allow(dead_code, clippy::cast_possible_truncation)]
fn dpccp_order_joins(
    tables: &[TableStats],
    indexes: &[IndexInfo],
    where_terms: &[WhereTerm<'_>],
    needed_columns: Option<&[String]>,
    table_index_hints: Option<&BTreeMap<String, IndexHint>>,
    cross_join_pairs: &[(String, String)],
    cracking_hints: Option<&CrackingHintStore>,
) -> Option<(Vec<usize>, f64, u64, u64)> {
    let n = tables.len();
    assert!(n <= DPCCP_MAX_TABLES);

    let access_paths = tables
        .iter()
        .map(|table| {
            join_access_path(
                table,
                indexes,
                where_terms,
                needed_columns,
                table_index_hints,
                cracking_hints,
            )
        })
        .collect::<Vec<_>>();

    let mut visit_order = (0..n).collect::<Vec<_>>();
    visit_order.sort_by(|lhs, rhs| {
        access_paths[*lhs]
            .estimated_cost
            .partial_cmp(&access_paths[*rhs].estimated_cost)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| lhs.cmp(rhs))
    });

    let mut state =
        ExhaustiveJoinSearchState::new(tables, &access_paths, &visit_order, cross_join_pairs);
    state.search();

    let order = state.best_order?;

    Some((
        order,
        state.best_cost,
        state.plans_enumerated,
        state.branches_pruned,
    ))
}

struct ExhaustiveJoinSearchState<'a> {
    tables: &'a [TableStats],
    access_paths: &'a [AccessPath],
    visit_order: &'a [usize],
    cross_join_pairs: &'a [(String, String)],
    best_order: Option<Vec<usize>>,
    best_cost: f64,
    plans_enumerated: u64,
    branches_pruned: u64,
}

impl<'a> ExhaustiveJoinSearchState<'a> {
    fn new(
        tables: &'a [TableStats],
        access_paths: &'a [AccessPath],
        visit_order: &'a [usize],
        cross_join_pairs: &'a [(String, String)],
    ) -> Self {
        Self {
            tables,
            access_paths,
            visit_order,
            cross_join_pairs,
            best_order: None,
            best_cost: f64::INFINITY,
            plans_enumerated: 0,
            branches_pruned: 0,
        }
    }

    fn search(&mut self) {
        let mut current_order = Vec::with_capacity(self.tables.len());
        self.search_dfs(&mut current_order, 0, 0.0, 1.0);
    }

    fn search_dfs(
        &mut self,
        current_order: &mut Vec<usize>,
        used_mask: u64,
        current_cost: f64,
        current_rows: f64,
    ) {
        if current_order.len() == self.tables.len() {
            if current_cost < self.best_cost {
                self.best_cost = current_cost;
                self.best_order = Some(current_order.clone());
                tracing::debug!(
                    target: "fsqlite.planner",
                    algorithm = "dpccp_exhaustive",
                    join_order = ?order_indices_to_names(current_order, self.tables),
                    total_cost = current_cost,
                    "planner.best_plan_updated"
                );
            }
            return;
        }

        for &candidate_idx in self.visit_order {
            if used_mask & (1u64 << candidate_idx) != 0 {
                continue;
            }

            let candidate = &self.tables[candidate_idx];
            if !cross_join_allowed_indices(
                current_order,
                &candidate.name,
                self.tables,
                self.cross_join_pairs,
            ) {
                continue;
            }

            let ap = &self.access_paths[candidate_idx];
            let (new_cost, new_rows) = if current_order.is_empty() {
                (ap.estimated_cost, ap.estimated_rows)
            } else {
                let inner_cost = ap.estimated_cost * current_rows;
                (current_cost + inner_cost, current_rows * ap.estimated_rows)
            };

            self.plans_enumerated += 1;
            let should_prune = self.best_cost.is_finite() && new_cost >= self.best_cost;

            let mut candidate_order = current_order
                .iter()
                .map(|idx| self.tables[*idx].name.as_str())
                .collect::<Vec<_>>();
            candidate_order.push(candidate.name.as_str());

            tracing::debug!(
                target: "fsqlite.planner",
                algorithm = "dpccp_exhaustive",
                depth = candidate_order.len(),
                candidate_order = ?candidate_order,
                cost = new_cost,
                best_complete_cost = if self.best_cost.is_finite() {
                    Some(self.best_cost)
                } else {
                    None::<f64>
                },
                pruned = should_prune,
                "planner.candidate_plan"
            );

            if should_prune {
                self.branches_pruned += 1;
                continue;
            }

            current_order.push(candidate_idx);
            self.search_dfs(
                current_order,
                used_mask | (1u64 << candidate_idx),
                new_cost,
                new_rows,
            );
            current_order.pop();
        }
    }
}

fn order_indices_to_names(order: &[usize], tables: &[TableStats]) -> Vec<String> {
    order.iter().map(|idx| tables[*idx].name.clone()).collect()
}

// ---------------------------------------------------------------------------
// Predicate pushdown (bd-1as.3)
// ---------------------------------------------------------------------------

/// A pushed-down predicate: WHERE term assigned to a specific table.
#[derive(Debug, Clone)]
pub struct PushedPredicate<'a> {
    /// Table name this predicate applies to.
    pub table: String,
    /// The original WHERE term.
    pub term: &'a WhereTerm<'a>,
}

/// Push WHERE predicates down to the lowest possible table in the join tree.
///
/// A predicate can be pushed down if it references columns from only one table.
/// Predicates referencing multiple tables remain as join conditions.
///
/// Returns (single_table_predicates, join_predicates).
pub fn pushdown_predicates<'a>(
    where_terms: &'a [WhereTerm<'a>],
    table_names: &[String],
) -> (Vec<PushedPredicate<'a>>, Vec<&'a WhereTerm<'a>>) {
    let span = tracing::debug_span!(
        target: "fsqlite.planner",
        "predicate_pushdown",
        total_terms = where_terms.len(),
        pushed = tracing::field::Empty,
        remaining = tracing::field::Empty,
    );
    let _g = span.enter();

    let mut pushed = Vec::new();
    let mut remaining = Vec::new();

    for term in where_terms {
        if let Some(ref col) = term.column {
            // Check if the column's table qualifier matches exactly one table.
            if let Some(ref tq) = col.table {
                let matching: Vec<_> = table_names
                    .iter()
                    .filter(|t| t.eq_ignore_ascii_case(tq))
                    .collect();
                if matching.len() == 1 {
                    pushed.push(PushedPredicate {
                        table: matching[0].clone(),
                        term,
                    });
                    continue;
                }
            }

            // Unqualified column: if only one table in scope, push there.
            if table_names.len() == 1 {
                pushed.push(PushedPredicate {
                    table: table_names[0].clone(),
                    term,
                });
                continue;
            }
        }
        remaining.push(term);
    }

    span.record("pushed", pushed.len() as u64);
    span.record("remaining", remaining.len() as u64);

    tracing::debug!(
        pushed_count = pushed.len(),
        remaining_count = remaining.len(),
        "planner.predicate_pushdown.complete"
    );

    (pushed, remaining)
}

// ---------------------------------------------------------------------------
// Constant folding (bd-1as.3)
// ---------------------------------------------------------------------------

/// Result of attempting to fold a constant expression.
#[derive(Debug, Clone, PartialEq)]
pub enum FoldResult {
    /// Expression was folded to a literal value.
    Literal(Literal),
    /// Expression could not be folded (contains column references).
    NotConstant,
}

/// Attempt to constant-fold an expression.
///
/// Evaluates expressions that contain only literals and deterministic operators
/// at plan time, avoiding repeated evaluation during execution.
pub fn try_constant_fold(expr: &Expr) -> FoldResult {
    match expr {
        Expr::Literal(lit, _) => FoldResult::Literal(lit.clone()),

        Expr::UnaryOp {
            op, expr: inner, ..
        } => {
            let inner_val = try_constant_fold(inner);
            match inner_val {
                FoldResult::Literal(Literal::Integer(i)) => match op {
                    fsqlite_ast::UnaryOp::Negate => FoldResult::Literal(Literal::Integer(-i)),
                    fsqlite_ast::UnaryOp::Plus => FoldResult::Literal(Literal::Integer(i)),
                    fsqlite_ast::UnaryOp::BitNot => FoldResult::Literal(Literal::Integer(!i)),
                    fsqlite_ast::UnaryOp::Not => FoldResult::Literal(if i == 0 {
                        Literal::True
                    } else {
                        Literal::False
                    }),
                },
                FoldResult::Literal(Literal::Float(f)) => match op {
                    fsqlite_ast::UnaryOp::Negate => FoldResult::Literal(Literal::Float(-f)),
                    fsqlite_ast::UnaryOp::Plus => FoldResult::Literal(Literal::Float(f)),
                    _ => FoldResult::NotConstant,
                },
                _ => FoldResult::NotConstant,
            }
        }

        Expr::BinaryOp {
            left, op, right, ..
        } => {
            let l = try_constant_fold(left);
            let r = try_constant_fold(right);
            match (l, r) {
                (
                    FoldResult::Literal(Literal::Integer(a)),
                    FoldResult::Literal(Literal::Integer(b)),
                ) => match op {
                    fsqlite_ast::BinaryOp::Add => {
                        FoldResult::Literal(Literal::Integer(a.wrapping_add(b)))
                    }
                    fsqlite_ast::BinaryOp::Subtract => {
                        FoldResult::Literal(Literal::Integer(a.wrapping_sub(b)))
                    }
                    fsqlite_ast::BinaryOp::Multiply => {
                        FoldResult::Literal(Literal::Integer(a.wrapping_mul(b)))
                    }
                    fsqlite_ast::BinaryOp::Divide => {
                        if b == 0 {
                            FoldResult::Literal(Literal::Null)
                        } else {
                            FoldResult::Literal(Literal::Integer(a / b))
                        }
                    }
                    fsqlite_ast::BinaryOp::Modulo => {
                        if b == 0 {
                            FoldResult::Literal(Literal::Null)
                        } else {
                            FoldResult::Literal(Literal::Integer(a % b))
                        }
                    }
                    fsqlite_ast::BinaryOp::Eq => FoldResult::Literal(if a == b {
                        Literal::True
                    } else {
                        Literal::False
                    }),
                    fsqlite_ast::BinaryOp::Ne => FoldResult::Literal(if a == b {
                        Literal::False
                    } else {
                        Literal::True
                    }),
                    fsqlite_ast::BinaryOp::Lt => {
                        FoldResult::Literal(if a < b { Literal::True } else { Literal::False })
                    }
                    fsqlite_ast::BinaryOp::Le => FoldResult::Literal(if a <= b {
                        Literal::True
                    } else {
                        Literal::False
                    }),
                    fsqlite_ast::BinaryOp::Gt => {
                        FoldResult::Literal(if a > b { Literal::True } else { Literal::False })
                    }
                    fsqlite_ast::BinaryOp::Ge => FoldResult::Literal(if a >= b {
                        Literal::True
                    } else {
                        Literal::False
                    }),
                    _ => FoldResult::NotConstant,
                },
                _ => FoldResult::NotConstant,
            }
        }

        // Any expression containing column references is not constant.
        _ => FoldResult::NotConstant,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_ast::{
        ColumnRef, CompoundOp, Distinctness, Expr, FromClause, InSet, IndexHint, Literal,
        OrderingTerm, QualifiedName, ResultColumn, SelectBody, SelectCore, SortDirection, Span,
        TableOrSubquery,
    };
    use std::{path::PathBuf, time::Instant};

    /// Helper: build a SELECT core with named result columns.
    fn select_core_with_aliases(aliases: &[&str]) -> SelectCore {
        SelectCore::Select {
            distinct: Distinctness::All,
            columns: aliases
                .iter()
                .map(|a| ResultColumn::Expr {
                    expr: Expr::Literal(Literal::Integer(0), Span::ZERO),
                    alias: Some((*a).to_owned()),
                })
                .collect(),
            from: None,
            where_clause: None,
            group_by: vec![],
            having: None,
            windows: vec![],
        }
    }

    /// Helper: build a compound body from multiple sets of aliases.
    fn compound_body(first: &[&str], rest: &[(&[&str], CompoundOp)]) -> SelectBody {
        SelectBody {
            select: select_core_with_aliases(first),
            compounds: rest
                .iter()
                .map(|(aliases, op)| (*op, select_core_with_aliases(aliases)))
                .collect(),
        }
    }

    /// Helper: ORDER BY a bare column name.
    fn order_by_name(name: &str) -> OrderingTerm {
        OrderingTerm {
            expr: Expr::Column(ColumnRef::bare(name), Span::ZERO),
            direction: None,
            nulls: None,
        }
    }

    /// Helper: ORDER BY a numeric index.
    fn order_by_num(n: i64) -> OrderingTerm {
        OrderingTerm {
            expr: Expr::Literal(Literal::Integer(n), Span::ZERO),
            direction: None,
            nulls: None,
        }
    }

    /// Helper: ORDER BY a name with direction.
    fn order_by_name_dir(name: &str, dir: SortDirection) -> OrderingTerm {
        OrderingTerm {
            expr: Expr::Column(ColumnRef::bare(name), Span::ZERO),
            direction: Some(dir),
            nulls: None,
        }
    }

    fn select_core_single_table(
        columns: Vec<ResultColumn>,
        table_name: &str,
        alias: Option<&str>,
    ) -> SelectCore {
        SelectCore::Select {
            distinct: Distinctness::All,
            columns,
            from: Some(FromClause {
                source: TableOrSubquery::Table {
                    name: QualifiedName::bare(table_name),
                    alias: alias.map(str::to_owned),
                    index_hint: None,
                },
                joins: vec![],
            }),
            where_clause: None,
            group_by: vec![],
            having: None,
            windows: vec![],
        }
    }

    // --- Core resolution tests ---

    #[test]
    fn test_single_table_projection_expands_star() {
        let core = select_core_single_table(vec![ResultColumn::Star], "t", None);
        let table_columns = vec!["a".to_owned(), "b".to_owned()];
        let resolved =
            resolve_single_table_result_columns(&core, &table_columns).expect("star should expand");
        assert_eq!(
            resolved,
            vec![
                ResultColumn::Expr {
                    expr: Expr::Column(ColumnRef::bare("a"), Span::ZERO),
                    alias: None
                },
                ResultColumn::Expr {
                    expr: Expr::Column(ColumnRef::bare("b"), Span::ZERO),
                    alias: None
                },
            ]
        );
    }

    #[test]
    fn test_single_table_projection_expands_table_star_with_alias() {
        let core = select_core_single_table(
            vec![ResultColumn::TableStar("tt".to_owned())],
            "t",
            Some("tt"),
        );
        let table_columns = vec!["a".to_owned(), "b".to_owned()];
        let resolved = resolve_single_table_result_columns(&core, &table_columns)
            .expect("table.* should expand");
        assert_eq!(resolved.len(), 2);
    }

    #[test]
    fn test_single_table_projection_rejects_unknown_column() {
        let core = select_core_single_table(
            vec![ResultColumn::Expr {
                expr: Expr::Column(ColumnRef::bare("z"), Span::ZERO),
                alias: None,
            }],
            "t",
            None,
        );
        let table_columns = vec!["a".to_owned(), "b".to_owned()];
        let err = resolve_single_table_result_columns(&core, &table_columns)
            .expect_err("unknown column should fail");
        assert_eq!(
            err,
            SingleTableProjectionError::ColumnNotFound {
                column: "z".to_owned()
            }
        );
    }

    #[test]
    fn test_single_table_projection_accepts_rowid_aliases_with_qualifiers() {
        let core = select_core_single_table(
            vec![
                ResultColumn::Expr {
                    expr: Expr::Column(ColumnRef::bare("rowid"), Span::ZERO),
                    alias: None,
                },
                ResultColumn::Expr {
                    expr: Expr::Column(
                        ColumnRef {
                            table: Some("tt".to_owned()),
                            column: "_rowid_".to_owned(),
                        },
                        Span::ZERO,
                    ),
                    alias: None,
                },
                ResultColumn::Expr {
                    expr: Expr::Column(
                        ColumnRef {
                            table: Some("t".to_owned()),
                            column: "oid".to_owned(),
                        },
                        Span::ZERO,
                    ),
                    alias: None,
                },
            ],
            "t",
            Some("tt"),
        );
        let table_columns = vec!["a".to_owned(), "b".to_owned()];
        let resolved = resolve_single_table_result_columns(&core, &table_columns)
            .expect("rowid aliases should be accepted in projection");
        assert_eq!(resolved.len(), 3);
    }

    #[test]
    fn test_compound_order_by_uses_first_alias() {
        // SELECT 1 AS a UNION SELECT 2 AS b ORDER BY a
        // → a is in the first SELECT at col 0
        let body = compound_body(&["a"], &[(&["b"], CompoundOp::Union)]);
        let result =
            resolve_compound_order_by(&body, &[order_by_name("a")]).expect("should resolve");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].column_idx, 0);
    }

    #[test]
    fn test_compound_order_by_second_select_alias() {
        // SELECT 1 AS a UNION SELECT 2 AS b ORDER BY b
        // → b is in the second SELECT at col 0
        let body = compound_body(&["a"], &[(&["b"], CompoundOp::Union)]);
        let result =
            resolve_compound_order_by(&body, &[order_by_name("b")]).expect("should resolve");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].column_idx, 0);
    }

    #[test]
    fn test_compound_order_by_first_select_wins_conflict() {
        // SELECT 10 AS a, 1 AS b UNION ALL SELECT 2 AS b, 20 AS a ORDER BY b
        // → b is in first SELECT at col 1 AND second SELECT at col 0
        // → first SELECT wins → col 1
        let body = compound_body(&["a", "b"], &[(&["b", "a"], CompoundOp::UnionAll)]);
        let result =
            resolve_compound_order_by(&body, &[order_by_name("b")]).expect("should resolve");
        assert_eq!(result[0].column_idx, 1);
    }

    #[test]
    fn test_compound_order_by_numeric_column() {
        // ORDER BY 1 → col 0, ORDER BY 2 → col 1
        let body = compound_body(&["a", "b"], &[(&["c", "d"], CompoundOp::Union)]);
        let result = resolve_compound_order_by(&body, &[order_by_num(1), order_by_num(2)])
            .expect("should resolve");
        assert_eq!(result[0].column_idx, 0);
        assert_eq!(result[1].column_idx, 1);
    }

    #[test]
    fn test_compound_order_by_unknown_name_error() {
        let body = compound_body(&["a"], &[(&["b"], CompoundOp::Union)]);
        let err =
            resolve_compound_order_by(&body, &[order_by_name("z")]).expect_err("should error");
        assert!(matches!(
            err,
            CompoundOrderByError::ColumnNotFound { ref name, .. } if name == "z"
        ));
    }

    #[test]
    fn test_compound_order_by_numeric_out_of_range() {
        let body = compound_body(&["a"], &[(&["b"], CompoundOp::Union)]);
        let err = resolve_compound_order_by(&body, &[order_by_num(5)]).expect_err("should error");
        assert!(matches!(
            err,
            CompoundOrderByError::IndexOutOfRange {
                index: 5,
                num_columns: 1,
                ..
            }
        ));
    }

    #[test]
    fn test_compound_order_by_numeric_zero() {
        let body = compound_body(&["a"], &[(&["b"], CompoundOp::Union)]);
        let err = resolve_compound_order_by(&body, &[order_by_num(0)]).expect_err("should error");
        assert!(matches!(
            err,
            CompoundOrderByError::IndexZeroOrNegative { value: 0, .. }
        ));
    }

    #[test]
    fn test_compound_order_by_expression_rejected() {
        let body = compound_body(&["a"], &[(&["b"], CompoundOp::Union)]);
        let term = OrderingTerm {
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
                op: fsqlite_ast::BinaryOp::Add,
                right: Box::new(Expr::Literal(Literal::Integer(0), Span::ZERO)),
                span: Span::ZERO,
            },
            direction: None,
            nulls: None,
        };
        let err = resolve_compound_order_by(&body, &[term]).expect_err("should error");
        assert!(matches!(
            err,
            CompoundOrderByError::ExpressionNotAllowed { .. }
        ));
    }

    #[test]
    fn test_compound_order_by_with_direction() {
        let body = compound_body(&["a", "b"], &[(&["c", "d"], CompoundOp::Union)]);
        let result =
            resolve_compound_order_by(&body, &[order_by_name_dir("a", SortDirection::Desc)])
                .expect("should resolve");
        assert_eq!(result[0].column_idx, 0);
        assert_eq!(result[0].direction, Some(SortDirection::Desc));
    }

    #[test]
    fn test_compound_order_by_collate() {
        let body = compound_body(&["a"], &[(&["b"], CompoundOp::Union)]);
        let term = OrderingTerm {
            expr: Expr::Collate {
                expr: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
                collation: "NOCASE".to_owned(),
                span: Span::ZERO,
            },
            direction: None,
            nulls: None,
        };
        let result = resolve_compound_order_by(&body, &[term]).expect("should resolve");
        assert_eq!(result[0].column_idx, 0);
        assert_eq!(result[0].collation.as_deref(), Some("NOCASE"));
    }

    #[test]
    fn test_compound_order_by_three_selects() {
        // Alias c only in 3rd SELECT at col 0
        let body = compound_body(
            &["a"],
            &[(&["b"], CompoundOp::Union), (&["c"], CompoundOp::Union)],
        );
        let result =
            resolve_compound_order_by(&body, &[order_by_name("c")]).expect("should resolve");
        assert_eq!(result[0].column_idx, 0);
    }

    #[test]
    fn test_compound_order_by_earlier_select_wins() {
        // 2nd SELECT has 'c' at col 1, 3rd SELECT has 'c' at col 0
        // → 2nd SELECT wins → col 1
        let body = compound_body(
            &["a", "x"],
            &[
                (&["b", "c"], CompoundOp::UnionAll),
                (&["c", "b"], CompoundOp::UnionAll),
            ],
        );
        let result =
            resolve_compound_order_by(&body, &[order_by_name("c")]).expect("should resolve");
        assert_eq!(result[0].column_idx, 1);
    }

    #[test]
    fn test_compound_order_by_case_insensitive() {
        let body = compound_body(&["MyCol"], &[(&["other"], CompoundOp::Union)]);
        let result =
            resolve_compound_order_by(&body, &[order_by_name("mycol")]).expect("should resolve");
        assert_eq!(result[0].column_idx, 0);
    }

    #[test]
    fn test_compound_order_by_intersect_except() {
        // Same resolution rules for all compound operators
        let body = compound_body(&["a"], &[(&["b"], CompoundOp::Intersect)]);
        let result =
            resolve_compound_order_by(&body, &[order_by_name("b")]).expect("should resolve");
        assert_eq!(result[0].column_idx, 0);

        let body = compound_body(&["a"], &[(&["b"], CompoundOp::Except)]);
        let result =
            resolve_compound_order_by(&body, &[order_by_name("b")]).expect("should resolve");
        assert_eq!(result[0].column_idx, 0);
    }

    #[test]
    fn test_extract_output_aliases_select() {
        let core = select_core_with_aliases(&["x", "y", "z"]);
        let aliases = extract_output_aliases(&core);
        assert_eq!(
            aliases,
            vec![
                Some("x".to_owned()),
                Some("y".to_owned()),
                Some("z".to_owned())
            ]
        );
    }

    #[test]
    fn test_extract_output_aliases_bare_column() {
        // SELECT col_name (no alias) → uses column name
        let core = SelectCore::Select {
            distinct: Distinctness::All,
            columns: vec![ResultColumn::Expr {
                expr: Expr::Column(ColumnRef::bare("my_col"), Span::ZERO),
                alias: None,
            }],
            from: None,
            where_clause: None,
            group_by: vec![],
            having: None,
            windows: vec![],
        };
        let aliases = extract_output_aliases(&core);
        assert_eq!(aliases, vec![Some("my_col".to_owned())]);
    }

    #[test]
    fn test_extract_output_aliases_values() {
        let core = SelectCore::Values(vec![vec![
            Expr::Literal(Literal::Integer(1), Span::ZERO),
            Expr::Literal(Literal::Integer(2), Span::ZERO),
        ]]);
        let aliases = extract_output_aliases(&core);
        assert_eq!(aliases, vec![None, None]);
    }

    #[test]
    fn test_is_compound() {
        let simple = SelectBody {
            select: select_core_with_aliases(&["a"]),
            compounds: vec![],
        };
        assert!(!is_compound(&simple));

        let compound = compound_body(&["a"], &[(&["b"], CompoundOp::Union)]);
        assert!(is_compound(&compound));
    }

    #[test]
    fn test_compound_op_name_all_variants() {
        assert_eq!(compound_op_name(CompoundOp::Union), "UNION");
        assert_eq!(compound_op_name(CompoundOp::UnionAll), "UNION ALL");
        assert_eq!(compound_op_name(CompoundOp::Intersect), "INTERSECT");
        assert_eq!(compound_op_name(CompoundOp::Except), "EXCEPT");
    }

    #[test]
    fn test_compound_order_by_error_display() {
        let err = CompoundOrderByError::ColumnNotFound {
            name: "z".to_owned(),
            span: Span::ZERO,
        };
        assert!(err.to_string().contains("does not match"));

        let err = CompoundOrderByError::IndexOutOfRange {
            index: 5,
            num_columns: 2,
            span: Span::ZERO,
        };
        assert!(err.to_string().contains("out of range"));

        let err = CompoundOrderByError::ExpressionNotAllowed { span: Span::ZERO };
        assert!(err.to_string().contains("not allowed"));
    }

    #[test]
    fn test_compound_order_by_negative_index() {
        let body = compound_body(&["a"], &[(&["b"], CompoundOp::Union)]);
        let err = resolve_compound_order_by(&body, &[order_by_num(-1)]).expect_err("should error");
        assert!(matches!(
            err,
            CompoundOrderByError::IndexZeroOrNegative { value: -1, .. }
        ));
    }

    #[test]
    fn test_compound_order_by_multiple_terms() {
        let body = compound_body(
            &["a", "b", "c"],
            &[(&["x", "y", "z"], CompoundOp::UnionAll)],
        );
        let result = resolve_compound_order_by(
            &body,
            &[
                order_by_name_dir("c", SortDirection::Desc),
                order_by_num(1),
                order_by_name("y"),
            ],
        )
        .expect("should resolve");
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].column_idx, 2); // c → first SELECT col 2
        assert_eq!(result[0].direction, Some(SortDirection::Desc));
        assert_eq!(result[1].column_idx, 0); // 1 → col 0
        assert_eq!(result[2].column_idx, 1); // y → second SELECT col 1
    }

    // ===================================================================
    // §10.5 Cost Model tests
    // ===================================================================

    fn table_stats(name: &str, n_pages: u64, n_rows: u64) -> TableStats {
        TableStats {
            name: name.to_owned(),
            n_pages,
            n_rows,
            source: StatsSource::Heuristic,
        }
    }

    fn index_info(
        name: &str,
        table: &str,
        columns: &[&str],
        unique: bool,
        n_pages: u64,
    ) -> IndexInfo {
        IndexInfo {
            name: name.to_owned(),
            table: table.to_owned(),
            columns: columns.iter().map(|c| (*c).to_owned()).collect(),
            unique,
            n_pages,
            source: StatsSource::Heuristic,
            partial_where: None,
            expression_columns: vec![],
        }
    }

    fn eq_term_value(col: &str, value: i64) -> WhereTerm<'static> {
        // Leaked for convenience in tests — we just need the lifetime.
        let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare(col), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(value), Span::ZERO)),
            span: Span::ZERO,
        }));
        classify_where_term(expr)
    }

    fn eq_term(col: &str) -> WhereTerm<'static> {
        eq_term_value(col, 1)
    }

    fn range_term(col: &str) -> WhereTerm<'static> {
        let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare(col), Span::ZERO)),
            op: AstBinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Integer(5), Span::ZERO)),
            span: Span::ZERO,
        }));
        classify_where_term(expr)
    }

    fn in_term(col: &str, count: usize) -> WhereTerm<'static> {
        let items: Vec<Expr> = (0..count)
            .map(|i| {
                #[allow(clippy::cast_possible_wrap)]
                Expr::Literal(Literal::Integer(i as i64), Span::ZERO)
            })
            .collect();
        let expr: &'static Expr = Box::leak(Box::new(Expr::In {
            expr: Box::new(Expr::Column(ColumnRef::bare(col), Span::ZERO)),
            set: InSet::List(items),
            not: false,
            span: Span::ZERO,
        }));
        classify_where_term(expr)
    }

    fn like_term(col: &str, pattern: &str) -> WhereTerm<'static> {
        let expr: &'static Expr = Box::leak(Box::new(Expr::Like {
            expr: Box::new(Expr::Column(ColumnRef::bare(col), Span::ZERO)),
            pattern: Box::new(Expr::Literal(
                Literal::String(pattern.to_owned()),
                Span::ZERO,
            )),
            escape: None,
            op: LikeOp::Like,
            not: false,
            span: Span::ZERO,
        }));
        classify_where_term(expr)
    }

    fn glob_term(col: &str, pattern: &str) -> WhereTerm<'static> {
        let expr: &'static Expr = Box::leak(Box::new(Expr::Like {
            expr: Box::new(Expr::Column(ColumnRef::bare(col), Span::ZERO)),
            pattern: Box::new(Expr::Literal(
                Literal::String(pattern.to_owned()),
                Span::ZERO,
            )),
            escape: None,
            op: LikeOp::Glob,
            not: false,
            span: Span::ZERO,
        }));
        classify_where_term(expr)
    }

    fn or_eq_term(col: &str, values: &[i64]) -> WhereTerm<'static> {
        assert!(
            values.len() >= 2,
            "or_eq_term requires at least two disjunct values"
        );

        let mut disjuncts = values
            .iter()
            .map(|value| Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::bare(col), Span::ZERO)),
                op: AstBinaryOp::Eq,
                right: Box::new(Expr::Literal(Literal::Integer(*value), Span::ZERO)),
                span: Span::ZERO,
            })
            .collect::<Vec<_>>();

        let mut combined = disjuncts.pop().expect("values is non-empty");
        while let Some(left_disjunct) = disjuncts.pop() {
            combined = Expr::BinaryOp {
                left: Box::new(left_disjunct),
                op: AstBinaryOp::Or,
                right: Box::new(combined),
                span: Span::ZERO,
            };
        }

        let expr: &'static Expr = Box::leak(Box::new(combined));
        classify_where_term(expr)
    }

    fn join_term(t1: &str, c1: &str, t2: &str, c2: &str) -> WhereTerm<'static> {
        let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::qualified(t1, c1), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Column(ColumnRef::qualified(t2, c2), Span::ZERO)),
            span: Span::ZERO,
        }));
        classify_where_term(expr)
    }

    #[test]
    fn test_cost_full_table_scan() {
        // Full table scan cost = N_pages(table)
        assert!(
            (estimate_cost(&AccessPathKind::FullTableScan, 100, 0) - 100.0).abs() < f64::EPSILON
        );
        assert!((estimate_cost(&AccessPathKind::FullTableScan, 1, 0) - 1.0).abs() < f64::EPSILON);
        assert!(
            (estimate_cost(&AccessPathKind::FullTableScan, 10000, 0) - 10000.0).abs()
                < f64::EPSILON
        );
    }

    #[test]
    fn test_cost_rowid_lookup() {
        // Rowid lookup cost = log2(N_pages(table))
        let cost = estimate_cost(&AccessPathKind::RowidLookup, 1024, 0);
        assert!((cost - 10.0).abs() < f64::EPSILON); // log2(1024) = 10
    }

    #[test]
    fn test_cost_index_scan_equality() {
        // Equality scan cost = log2(idx_pages) + log2(tbl_pages)
        let cost = estimate_cost(&AccessPathKind::IndexScanEquality, 200, 50);
        let expected = 50_f64.log2() + 200_f64.log2();
        assert!((cost - expected).abs() < 1e-10);
    }

    #[test]
    fn test_cost_index_scan_range() {
        // Range scan cost = log2(idx_pages) + sel * idx_pages + sel * tbl_pages
        let sel = 0.1;
        let cost = estimate_cost(
            &AccessPathKind::IndexScanRange { selectivity: sel },
            200,
            50,
        );
        let expected = 50_f64.log2() + sel * 50.0 + sel * 200.0;
        assert!((cost - expected).abs() < 1e-10);
    }

    #[test]
    fn test_cost_covering_index_scan() {
        // Covering index cost = log2(idx_pages) + sel * idx_pages (no table lookup)
        let sel = 0.1;
        let cost = estimate_cost(
            &AccessPathKind::CoveringIndexScan { selectivity: sel },
            200,
            50,
        );
        let expected = 50_f64.log2() + sel * 50.0;
        assert!((cost - expected).abs() < 1e-10);
    }

    #[test]
    fn test_cost_comparison_table_scan_vs_index() {
        // For low selectivity, index should be cheaper than full scan.
        let full = estimate_cost(&AccessPathKind::FullTableScan, 1000, 0);
        let idx = estimate_cost(
            &AccessPathKind::IndexScanRange { selectivity: 0.01 },
            1000,
            100,
        );
        assert!(
            idx < full,
            "index scan ({idx:.1}) should be cheaper than full scan ({full:.1}) at 1% selectivity"
        );

        // For high selectivity (~1.0), full scan may be cheaper.
        let idx_high = estimate_cost(
            &AccessPathKind::IndexScanRange { selectivity: 0.95 },
            1000,
            100,
        );
        // idx_high = log2(100) + 0.95*100 + 0.95*1000 = ~6.6 + 95 + 950 = ~1051
        // That's MORE than the 1000-page full scan.
        assert!(
            idx_high > full,
            "index scan ({idx_high:.1}) should be pricier than full scan ({full:.1}) at 95% selectivity"
        );
    }

    // ===================================================================
    // §10.5 Index usability tests
    // ===================================================================

    #[test]
    fn test_index_usability_equality_leftmost() {
        let idx = index_info("idx_abc", "t1", &["a", "b", "c"], false, 50);
        // a = 1 → usable (leftmost)
        let terms = [eq_term("a")];
        assert!(matches!(
            analyze_index_usability(&idx, &terms),
            IndexUsability::Equality
        ));
        // b = 1 alone → NOT usable (not leftmost)
        let terms = [eq_term("b")];
        assert!(matches!(
            analyze_index_usability(&idx, &terms),
            IndexUsability::NotUsable
        ));
    }

    #[test]
    fn test_index_usability_qualified_column_rejects_wrong_table() {
        // Index on t1.a — a WHERE term on t2.a should NOT match.
        let idx = index_info("idx_a", "t1", &["a"], false, 50);
        let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::qualified("t2", "a"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            span: Span::ZERO,
        }));
        let terms = [classify_where_term(expr)];
        assert!(matches!(
            analyze_index_usability(&idx, &terms),
            IndexUsability::NotUsable
        ));

        // Same column name but qualified to the correct table → usable.
        let expr2: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::qualified("t1", "a"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            span: Span::ZERO,
        }));
        let terms2 = [classify_where_term(expr2)];
        assert!(matches!(
            analyze_index_usability(&idx, &terms2),
            IndexUsability::Equality
        ));

        // Unqualified column → conservatively considered usable.
        let terms3 = [eq_term("a")];
        assert!(matches!(
            analyze_index_usability(&idx, &terms3),
            IndexUsability::Equality
        ));
    }

    #[test]
    fn test_index_usability_range_rightmost() {
        let idx = index_info("idx_ab", "t1", &["a", "b"], false, 50);
        // a > 5 → range usable on leftmost column
        let terms = [range_term("a")];
        assert!(matches!(
            analyze_index_usability(&idx, &terms),
            IndexUsability::Range { .. }
        ));
        // b > 5 alone → NOT usable (not leftmost)
        let terms = [range_term("b")];
        assert!(matches!(
            analyze_index_usability(&idx, &terms),
            IndexUsability::NotUsable
        ));
    }

    #[test]
    fn test_index_usability_in_expansion() {
        let idx = index_info("idx_col", "t1", &["col"], false, 50);
        let terms = [in_term("col", 3)];
        let result = analyze_index_usability(&idx, &terms);
        assert!(matches!(
            result,
            IndexUsability::InExpansion { probe_count: 3 }
        ));
    }

    #[test]
    fn test_in_expansion_cost_scales_by_probe_count() {
        // Regression: IN (v1, v2, v3) should cost ~3x a single equality
        // probe, not the same as a single probe.
        let table = table_stats("t1", 100, 1000);
        let idx = index_info("idx_col", "t1", &["col"], false, 50);
        let single_eq_term = [eq_term("col")];
        let in_3_term = [in_term("col", 3)];

        let ap_eq = best_access_path(&table, std::slice::from_ref(&idx), &single_eq_term, None);
        let ap_in = best_access_path(&table, std::slice::from_ref(&idx), &in_3_term, None);

        // IN with 3 probes should cost approximately 3x a single equality.
        let ratio = ap_in.estimated_cost / ap_eq.estimated_cost;
        assert!(
            (ratio - 3.0).abs() < 0.01,
            "IN(3) cost should be 3x equality cost: eq={} in3={} ratio={}",
            ap_eq.estimated_cost,
            ap_in.estimated_cost,
            ratio,
        );
    }

    #[test]
    fn test_best_access_path_or_disjunction_uses_in_expansion_index_probe() {
        let table = table_stats("t1", 1_000, 100_000);
        let idx = index_info("idx_a", "t1", &["a"], false, 80);
        let term = or_eq_term("a", &[1, 2, 3, 4]);
        assert!(matches!(term.kind, WhereTermKind::InList { count: 4 }));

        let ap = best_access_path(&table, &[idx], &[term], None);
        assert_eq!(ap.index.as_deref(), Some("idx_a"));
        assert!(matches!(ap.kind, AccessPathKind::IndexScanEquality));
    }

    #[test]
    fn test_index_usability_like_prefix() {
        let idx = index_info("idx_name", "t1", &["name"], false, 50);
        // LIKE 'Jo%' → usable (constant prefix)
        let terms = [like_term("name", "Jo%")];
        let result = analyze_index_usability(&idx, &terms);
        assert!(matches!(
            result,
            IndexUsability::LikePrefix {
                ref low,
                high: Some(ref high)
            } if low == "Jo" && high == "Jp"
        ));

        // LIKE '%Jo%' → not usable (no constant prefix)
        let terms = [like_term("name", "%Jo%")];
        assert!(matches!(
            analyze_index_usability(&idx, &terms),
            IndexUsability::NotUsable
        ));
    }

    #[test]
    fn test_index_usability_glob_prefix() {
        let idx = index_info("idx_name", "t1", &["name"], false, 50);
        // GLOB 'Jo*' → usable (constant prefix)
        let terms = [glob_term("name", "Jo*")];
        let result = analyze_index_usability(&idx, &terms);
        assert!(matches!(
            result,
            IndexUsability::LikePrefix {
                ref low,
                high: Some(ref high)
            } if low == "Jo" && high == "Jp"
        ));

        // GLOB '*Jo*' → not usable (no constant prefix)
        let terms = [glob_term("name", "*Jo*")];
        assert!(matches!(
            analyze_index_usability(&idx, &terms),
            IndexUsability::NotUsable
        ));
    }

    #[test]
    fn test_classify_where_term_equality() {
        let term = eq_term("x");
        assert!(matches!(term.kind, WhereTermKind::Equality));
        assert_eq!(term.column.as_ref().unwrap().column, "x");
    }

    #[test]
    fn test_classify_where_term_range() {
        let term = range_term("y");
        assert!(matches!(term.kind, WhereTermKind::Range));
        assert_eq!(term.column.as_ref().unwrap().column, "y");
    }

    #[test]
    fn test_classify_where_term_rowid() {
        let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("rowid"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(42), Span::ZERO)),
            span: Span::ZERO,
        }));
        let term = classify_where_term(expr);
        assert!(matches!(term.kind, WhereTermKind::RowidEquality));
    }

    #[test]
    fn test_decompose_where_and() {
        let inner = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
                op: AstBinaryOp::Eq,
                right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
                span: Span::ZERO,
            }),
            op: AstBinaryOp::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::bare("b"), Span::ZERO)),
                op: AstBinaryOp::Gt,
                right: Box::new(Expr::Literal(Literal::Integer(5), Span::ZERO)),
                span: Span::ZERO,
            }),
            span: Span::ZERO,
        };
        let terms = decompose_where(&inner);
        assert_eq!(terms.len(), 2);
    }

    #[test]
    fn test_extract_like_prefix_constant() {
        let pat = Expr::Literal(Literal::String("abc%def".to_owned()), Span::ZERO);
        assert_eq!(extract_like_prefix(&pat), Some("abc".to_owned()));
    }

    #[test]
    fn test_extract_like_prefix_none() {
        let pat = Expr::Literal(Literal::String("%xyz".to_owned()), Span::ZERO);
        assert_eq!(extract_like_prefix(&pat), None);
    }

    // ===================================================================
    // §10.5 Join ordering tests
    // ===================================================================

    #[test]
    fn test_join_ordering_single_table() {
        let tables = [table_stats("t1", 100, 1000)];
        let plan = order_joins(&tables, &[], &[], None, &[]);
        assert_eq!(plan.join_order, vec!["t1"]);
        assert!((plan.total_cost - 100.0).abs() < f64::EPSILON); // full table scan
    }

    #[test]
    fn test_join_ordering_two_tables() {
        let tables = [table_stats("t1", 10, 100), table_stats("t2", 1000, 50000)];
        let plan = order_joins(&tables, &[], &[], None, &[]);
        assert_eq!(plan.join_order.len(), 2);
        // Smaller table should be scanned first (lower startup cost).
        assert_eq!(plan.join_order[0], "t1");
    }

    #[test]
    fn test_join_ordering_three_tables() {
        let tables = [
            table_stats("t1", 10, 100),
            table_stats("t2", 100, 1000),
            table_stats("t3", 1000, 10000),
        ];
        let plan = order_joins(&tables, &[], &[], None, &[]);
        assert_eq!(plan.join_order.len(), 3);
        // All tables present; beam search picks cost-optimal order
        // (nested loop model considers outer-row scaling, so smallest
        // last-stage rows wins — the exact order depends on the cost model).
        for t in &tables {
            assert!(plan.join_order.contains(&t.name));
        }
        assert!(plan.total_cost > 0.0);
    }

    #[test]
    fn test_join_ordering_prefers_indexed() {
        let tables = [table_stats("t1", 10, 100), table_stats("t2", 1000, 50000)];
        let indexes = [index_info("idx_t2_fk", "t2", &["fk"], false, 50)];
        let terms = [eq_term("fk")];
        let plan = order_joins(&tables, &indexes, &terms, None, &[]);
        // t1 should still come first (small outer), t2 uses index.
        assert_eq!(plan.join_order[0], "t1");
        assert!(plan.access_paths[1].index.is_some());
    }

    #[test]
    fn test_join_ordering_beam_search_bounded() {
        // 6 tables — should NOT explore all 720 orderings.
        let tables: Vec<TableStats> = (1..=6_u64)
            .map(|i| table_stats(&format!("t{i}"), i * 10, i * 100))
            .collect();
        let plan = order_joins(&tables, &[], &[], None, &[]);
        assert_eq!(plan.join_order.len(), 6);
        // Verify it produced a valid plan (all tables present).
        for t in &tables {
            assert!(plan.join_order.contains(&t.name));
        }
    }

    #[test]
    fn test_three_way_join_cost_scales_by_cumulative_rows() {
        // Regression: the cost of the 3rd table in a nested loop join must
        // be scaled by T1.rows * T2.rows, not just T2.rows.
        let small = table_stats("small", 1, 10);
        let medium = table_stats("medium", 10, 100);
        let large = table_stats("large", 100, 1000);
        let plan_sml = order_joins(&[small, medium, large], &[], &[], None, &[]);

        // With correct cumulative scaling, putting the largest table last
        // is expensive because it scans once per (small * medium) row.
        // The planner should NOT produce the same cost as it would if
        // outer_rows were only the second table's rows.
        #[allow(clippy::suboptimal_flops)]
        let cost_if_only_last = 1.0_f64 // small full scan cost
            + 10.0 * 10.0 // medium scanned 10 times
            + 100.0 * 100.0; // BUG cost: large scanned only 100 times (medium.rows)
        // The plan's total cost should be larger than this naive estimate
        // because large is actually scanned 10*100=1000 times.
        assert!(
            plan_sml.total_cost > cost_if_only_last,
            "3-way join cost should scale by cumulative rows, not just last table: plan_cost={} bug_cost={}",
            plan_sml.total_cost,
            cost_if_only_last,
        );
    }

    #[test]
    fn test_mx_choice_single_table() {
        assert_eq!(compute_mx_choice(1, false), 1);
    }

    #[test]
    fn test_mx_choice_two_tables() {
        assert_eq!(compute_mx_choice(2, false), 5);
    }

    #[test]
    fn test_mx_choice_three_tables() {
        assert_eq!(compute_mx_choice(3, false), 12);
    }

    #[test]
    fn test_mx_choice_star_query() {
        assert_eq!(compute_mx_choice(4, true), 18);
    }

    #[test]
    fn test_detect_star_query_true() {
        // Central table "fact" joins to dim1, dim2, dim3.
        let tables = [
            table_stats("fact", 1000, 100_000),
            table_stats("dim1", 10, 100),
            table_stats("dim2", 10, 100),
            table_stats("dim3", 10, 100),
        ];
        let terms = [
            join_term("fact", "d1_id", "dim1", "id"),
            join_term("fact", "d2_id", "dim2", "id"),
            join_term("fact", "d3_id", "dim3", "id"),
        ];
        assert!(detect_star_query(&tables, &terms));
    }

    #[test]
    fn test_detect_star_query_false() {
        // 4-node chain: t1-t2-t3-t4. No single table joins ALL others.
        // t2 joins t1,t3 (2/3); t3 joins t2,t4 (2/3). Neither reaches 3/3.
        let tables = [
            table_stats("t1", 100, 1000),
            table_stats("t2", 100, 1000),
            table_stats("t3", 100, 1000),
            table_stats("t4", 100, 1000),
        ];
        let terms = [
            join_term("t1", "id", "t2", "fk1"),
            join_term("t2", "id", "t3", "fk2"),
            join_term("t3", "id", "t4", "fk3"),
        ];
        assert!(!detect_star_query(&tables, &terms));
    }

    #[test]
    fn test_cross_join_no_reorder() {
        // CROSS JOIN between t1 and t2: t2 cannot appear before t1.
        let tables = [
            table_stats("t1", 1000, 50000), // Big table first
            table_stats("t2", 10, 100),     // Small table second
        ];
        let cross = [("t1".to_owned(), "t2".to_owned())];
        let plan = order_joins(&tables, &[], &[], None, &cross);
        // Despite t2 being smaller, CROSS JOIN forces t1 first.
        assert_eq!(plan.join_order[0], "t1");
        assert_eq!(plan.join_order[1], "t2");
    }

    #[test]
    fn test_two_way_join_stays_hash_even_with_leapfrog_enabled() {
        let tables = [table_stats("t1", 10, 100), table_stats("t2", 12, 120)];
        let terms = [join_term("t1", "k", "t2", "k")];
        let plan = order_joins_with_hints_and_features(
            &tables,
            &[],
            &terms,
            None,
            &[],
            None,
            None,
            PlannerFeatureFlags {
                leapfrog_join: true,
                ..PlannerFeatureFlags::default()
            },
        );

        assert_eq!(plan.join_segments.len(), 1);
        assert_eq!(plan.join_segments[0].operator, JoinOperator::HashJoin);
    }

    #[test]
    fn test_three_way_equi_join_uses_leapfrog_when_feature_enabled() {
        let tables = [
            table_stats("a", 1024, 1_000_000),
            table_stats("b", 1024, 1_000_000),
            table_stats("c", 1024, 1_000_000),
        ];
        let terms = [join_term("a", "k", "b", "k"), join_term("b", "k", "c", "k")];
        let plan = order_joins_with_hints_and_features(
            &tables,
            &[],
            &terms,
            None,
            &[],
            None,
            None,
            PlannerFeatureFlags {
                leapfrog_join: true,
                ..PlannerFeatureFlags::default()
            },
        );

        assert!(
            plan.join_segments
                .iter()
                .any(|segment| segment.operator == JoinOperator::LeapfrogTriejoin
                    && segment.relations.len() == 3),
            "expected Leapfrog segment, got {:?}",
            plan.join_segments
        );
    }

    #[test]
    fn test_leapfrog_feature_flag_gates_routing() {
        let tables = [
            table_stats("a", 1024, 1_000_000),
            table_stats("b", 1024, 1_000_000),
            table_stats("c", 1024, 1_000_000),
        ];
        let terms = [join_term("a", "k", "b", "k"), join_term("b", "k", "c", "k")];
        let plan = order_joins_with_hints_and_features(
            &tables,
            &[],
            &terms,
            None,
            &[],
            None,
            None,
            PlannerFeatureFlags {
                leapfrog_join: false,
                ..PlannerFeatureFlags::default()
            },
        );

        assert_eq!(plan.join_segments.len(), 1);
        assert_eq!(plan.join_segments[0].operator, JoinOperator::HashJoin);
    }

    #[test]
    fn test_mixed_join_segments_support_leapfrog_and_hash() {
        let tables = [
            table_stats("a", 512, 900_000),
            table_stats("b", 512, 900_000),
            table_stats("c", 512, 900_000),
            table_stats("d", 64, 10_000),
            table_stats("e", 64, 10_000),
        ];
        let terms = [
            join_term("a", "k", "b", "k"),
            join_term("b", "k", "c", "k"),
            join_term("d", "k", "e", "k"),
        ];
        let plan = order_joins_with_hints_and_features(
            &tables,
            &[],
            &terms,
            None,
            &[],
            None,
            None,
            PlannerFeatureFlags {
                leapfrog_join: true,
                ..PlannerFeatureFlags::default()
            },
        );

        assert!(
            plan.join_segments
                .iter()
                .any(|segment| segment.operator == JoinOperator::LeapfrogTriejoin
                    && segment.relations.len() == 3),
            "expected 3-way Leapfrog segment, got {:?}",
            plan.join_segments
        );
        assert!(
            plan.join_segments
                .iter()
                .any(|segment| segment.operator == JoinOperator::HashJoin
                    && segment.relations.len() == 2),
            "expected 2-way hash segment, got {:?}",
            plan.join_segments
        );
    }

    #[test]
    fn test_incompatible_trie_ordering_falls_back_to_hash_join() {
        let tables = [
            table_stats("a", 256, 100_000),
            table_stats("b", 256, 100_000),
            table_stats("c", 256, 100_000),
        ];
        let terms = [join_term("a", "x", "b", "x"), join_term("b", "y", "c", "y")];
        let plan = order_joins_with_hints_and_features(
            &tables,
            &[],
            &terms,
            None,
            &[],
            None,
            None,
            PlannerFeatureFlags {
                leapfrog_join: true,
                ..PlannerFeatureFlags::default()
            },
        );

        assert!(
            plan.join_segments
                .iter()
                .all(|segment| segment.operator == JoinOperator::HashJoin),
            "incompatible trie ordering should stay hash-only: {:?}",
            plan.join_segments
        );
    }

    #[test]
    fn test_outer_join_shape_forces_hash_fallback() {
        use fsqlite_ast::{JoinClause, JoinConstraint, JoinKind, JoinType};

        let from = FromClause {
            source: TableOrSubquery::Table {
                name: QualifiedName::bare("a"),
                alias: None,
                index_hint: None,
            },
            joins: vec![JoinClause {
                join_type: JoinType {
                    natural: false,
                    kind: JoinKind::Left,
                },
                table: TableOrSubquery::Table {
                    name: QualifiedName::bare("b"),
                    alias: None,
                    index_hint: None,
                },
                constraint: Some(JoinConstraint::On(Expr::BinaryOp {
                    left: Box::new(Expr::Column(ColumnRef::qualified("a", "k"), Span::ZERO)),
                    op: AstBinaryOp::Eq,
                    right: Box::new(Expr::Column(ColumnRef::qualified("b", "k"), Span::ZERO)),
                    span: Span::ZERO,
                })),
            }],
        };
        let tables = [
            table_stats("a", 128, 100_000),
            table_stats("b", 128, 100_000),
            table_stats("c", 128, 100_000),
        ];
        let join_order = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        let terms = [join_term("a", "k", "b", "k"), join_term("b", "k", "c", "k")];
        let segments = choose_join_segments(
            &join_order,
            &tables,
            &terms,
            Some(&from),
            PlannerFeatureFlags {
                leapfrog_join: true,
                ..PlannerFeatureFlags::default()
            },
        );

        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].operator, JoinOperator::HashJoin);
    }

    #[test]
    fn test_collect_table_index_hints_from_clause_includes_aliases() {
        use fsqlite_ast::{JoinClause, JoinKind, JoinType};

        let from = FromClause {
            source: TableOrSubquery::Table {
                name: QualifiedName::bare("users"),
                alias: Some("u".to_owned()),
                index_hint: Some(IndexHint::IndexedBy("idx_users_email".to_owned())),
            },
            joins: vec![JoinClause {
                join_type: JoinType {
                    kind: JoinKind::Inner,
                    natural: false,
                },
                table: TableOrSubquery::Table {
                    name: QualifiedName::bare("events"),
                    alias: Some("e".to_owned()),
                    index_hint: Some(IndexHint::NotIndexed),
                },
                constraint: None,
            }],
        };

        let hints = collect_table_index_hints(&from);
        assert!(matches!(
            hints.get("users"),
            Some(IndexHint::IndexedBy(name)) if name == "idx_users_email"
        ));
        assert!(matches!(
            hints.get("u"),
            Some(IndexHint::IndexedBy(name)) if name == "idx_users_email"
        ));
        assert!(matches!(hints.get("events"), Some(IndexHint::NotIndexed)));
        assert!(matches!(hints.get("e"), Some(IndexHint::NotIndexed)));
    }

    #[test]
    fn test_order_joins_with_hints_respects_not_indexed() {
        let tables = [table_stats("t1", 1000, 50000)];
        let idx = index_info("idx_t1_a", "t1", &["a"], false, 100);
        let terms = [eq_term("a")];
        let hints = BTreeMap::from([(canonical_table_key("t1"), IndexHint::NotIndexed)]);

        let plan = order_joins_with_hints(&tables, &[idx], &terms, None, &[], Some(&hints), None);
        assert_eq!(plan.join_order, vec!["t1".to_owned()]);
        assert_eq!(plan.access_paths.len(), 1);
        assert!(matches!(
            plan.access_paths[0].kind,
            AccessPathKind::FullTableScan
        ));
    }

    #[test]
    fn test_order_joins_with_hints_respects_indexed_by() {
        let tables = [table_stats("t1", 2000, 100_000)];
        let fast = index_info("idx_fast", "t1", &["a"], false, 10);
        let slow = index_info("idx_slow", "t1", &["a"], false, 600);
        let terms = [eq_term("a")];
        let hints = BTreeMap::from([(
            canonical_table_key("t1"),
            IndexHint::IndexedBy("idx_slow".to_owned()),
        )]);

        let plan = order_joins_with_hints(
            &tables,
            &[fast, slow],
            &terms,
            None,
            &[],
            Some(&hints),
            None,
        );
        assert_eq!(plan.access_paths.len(), 1);
        assert_eq!(plan.access_paths[0].index.as_deref(), Some("idx_slow"));
    }

    #[test]
    fn test_order_joins_with_hints_reuses_cracking_store() {
        let tables = [table_stats("t1", 1000, 50000)];
        let idx_a = index_info("idx_a", "t1", &["a"], false, 40);
        let idx_b = index_info("idx_b", "t1", &["a"], false, 40);
        let terms = [eq_term("a")];
        let mut store = CrackingHintStore::default();

        let first = order_joins_with_hints(
            &tables,
            &[idx_a.clone(), idx_b.clone()],
            &terms,
            None,
            &[],
            None,
            Some(&mut store),
        );
        assert_eq!(first.access_paths[0].index.as_deref(), Some("idx_a"));
        assert_eq!(store.preferred_index("t1"), Some("idx_a"));

        let second = order_joins_with_hints(
            &tables,
            &[idx_b, idx_a],
            &terms,
            None,
            &[],
            None,
            Some(&mut store),
        );
        assert_eq!(second.access_paths[0].index.as_deref(), Some("idx_a"));
    }

    #[test]
    fn test_planner_selects_covering_index() {
        let table = table_stats("t1", 1000, 50000);
        let idx = index_info("idx_t1_ab", "t1", &["a", "b"], false, 100);
        let terms = [eq_term("a")];
        let needed = ["a".to_owned(), "b".to_owned()];
        let ap = best_access_path(&table, &[idx], &terms, Some(&needed));
        assert!(matches!(ap.kind, AccessPathKind::CoveringIndexScan { .. }));
    }

    #[test]
    fn test_planner_heuristic_fallback() {
        // Without any indexes, should fall back to full table scan.
        let table = table_stats("t1", 100, 1000);
        let ap = best_access_path(&table, &[], &[], None);
        assert!(matches!(ap.kind, AccessPathKind::FullTableScan));
        assert!((ap.estimated_cost - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_query_plan_display() {
        let plan = QueryPlan {
            join_order: vec!["t1".to_owned(), "t2".to_owned()],
            access_paths: vec![
                AccessPath {
                    table: "t1".to_owned(),
                    kind: AccessPathKind::FullTableScan,
                    index: None,
                    estimated_cost: 100.0,
                    estimated_rows: 1000.0,
                },
                AccessPath {
                    table: "t2".to_owned(),
                    kind: AccessPathKind::IndexScanEquality,
                    index: Some("idx_t2".to_owned()),
                    estimated_cost: 15.0,
                    estimated_rows: 10.0,
                },
            ],
            join_segments: vec![JoinPlanSegment {
                relations: vec!["t1".to_owned(), "t2".to_owned()],
                operator: JoinOperator::HashJoin,
                estimated_cost: 115.0,
                reason: "2-way joins stay on pairwise hash join".to_owned(),
            }],
            total_cost: 115.0,
        };
        let display = plan.to_string();
        assert!(display.contains("QUERY PLAN"));
        assert!(display.contains("SCAN t1"));
        assert!(display.contains("JOIN OPERATORS"));
        assert!(display.contains("HASH JOIN"));
        assert!(display.contains("USING INDEX idx_t2"));
    }

    #[test]
    fn test_query_plan_display_mentions_leapfrog_operator() {
        let plan = QueryPlan {
            join_order: vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
            access_paths: vec![],
            join_segments: vec![JoinPlanSegment {
                relations: vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
                operator: JoinOperator::LeapfrogTriejoin,
                estimated_cost: 42.0,
                reason: "AGM estimate 42.0 beats hash cost 100.0; trie arity 1".to_owned(),
            }],
            total_cost: 42.0,
        };

        let display = plan.to_string();
        assert!(display.contains("LEAPFROG TRIEJOIN"));
        assert!(display.contains("JOIN OPERATORS"));
    }

    #[test]
    fn test_best_access_path_rowid_lookup() {
        let table = table_stats("t1", 1024, 50000);
        let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("rowid"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(42), Span::ZERO)),
            span: Span::ZERO,
        }));
        let term = classify_where_term(expr);
        let ap = best_access_path(&table, &[], &[term], None);
        assert!(matches!(ap.kind, AccessPathKind::RowidLookup));
        assert!((ap.estimated_cost - 10.0).abs() < f64::EPSILON); // log2(1024) = 10
    }

    #[test]
    fn test_analyze_stats_override() {
        // With ANALYZE stats, the source is recorded.
        let table = TableStats {
            name: "t1".to_owned(),
            n_pages: 500,
            n_rows: 10000,
            source: StatsSource::Analyze,
        };
        assert_eq!(table.source, StatsSource::Analyze);
        let ap = best_access_path(&table, &[], &[], None);
        assert!(matches!(ap.kind, AccessPathKind::FullTableScan));
        assert!((ap.estimated_cost - 500.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_order_joins_empty() {
        let plan = order_joins(&[], &[], &[], None, &[]);
        assert!(plan.join_order.is_empty());
        assert!((plan.total_cost - 0.0).abs() < f64::EPSILON);
    }

    // ===================================================================
    // Error Display / Error trait tests
    // ===================================================================

    #[test]
    fn test_compound_order_by_error_display_zero_or_negative() {
        let err = CompoundOrderByError::IndexZeroOrNegative {
            value: -3,
            span: Span::ZERO,
        };
        let msg = err.to_string();
        assert!(msg.contains("-3"), "should contain the value: {msg}");
        assert!(
            msg.contains("must be positive"),
            "should say must be positive: {msg}"
        );
    }

    #[test]
    fn test_compound_order_by_error_is_error() {
        let err = CompoundOrderByError::ColumnNotFound {
            name: "x".to_owned(),
            span: Span::ZERO,
        };
        // std::error::Error is implemented — verify source() returns None (leaf error).
        assert!(std::error::Error::source(&err).is_none());
    }

    #[test]
    fn test_single_table_projection_error_display_all_variants() {
        let cases: Vec<(SingleTableProjectionError, &str)> = vec![
            (SingleTableProjectionError::NotSelectCore, "SELECT core"),
            (SingleTableProjectionError::MissingFromClause, "FROM clause"),
            (
                SingleTableProjectionError::UnsupportedFromSource,
                "single-table",
            ),
            (
                SingleTableProjectionError::UnknownTableQualifier {
                    qualifier: "bad".to_owned(),
                },
                "bad",
            ),
            (
                SingleTableProjectionError::ColumnNotFound {
                    column: "missing_col".to_owned(),
                },
                "missing_col",
            ),
        ];
        for (err, expected_fragment) in cases {
            let msg = err.to_string();
            assert!(
                msg.contains(expected_fragment),
                "{err:?} display should contain '{expected_fragment}': got '{msg}'"
            );
        }
    }

    #[test]
    fn test_single_table_projection_error_is_error() {
        let err = SingleTableProjectionError::NotSelectCore;
        assert!(std::error::Error::source(&err).is_none());
    }

    // ===================================================================
    // count_output_columns tests
    // ===================================================================

    #[test]
    fn test_count_output_columns_select() {
        let core = select_core_with_aliases(&["a", "b", "c"]);
        assert_eq!(count_output_columns(&core), 3);
    }

    #[test]
    fn test_count_output_columns_values() {
        let core = SelectCore::Values(vec![vec![
            Expr::Literal(Literal::Integer(1), Span::ZERO),
            Expr::Literal(Literal::Integer(2), Span::ZERO),
        ]]);
        assert_eq!(count_output_columns(&core), 2);
    }

    #[test]
    fn test_count_output_columns_empty_values() {
        let core = SelectCore::Values(vec![]);
        assert_eq!(count_output_columns(&core), 0);
    }

    // ===================================================================
    // extract_output_aliases edge cases
    // ===================================================================

    #[test]
    fn test_extract_output_aliases_star_is_none() {
        let core = SelectCore::Select {
            distinct: Distinctness::All,
            columns: vec![ResultColumn::Star],
            from: None,
            where_clause: None,
            group_by: vec![],
            having: None,
            windows: vec![],
        };
        let aliases = extract_output_aliases(&core);
        assert_eq!(aliases, vec![None]);
    }

    #[test]
    fn test_extract_output_aliases_expression_no_alias() {
        // SELECT 1+2 (expression, no alias) → None
        let core = SelectCore::Select {
            distinct: Distinctness::All,
            columns: vec![ResultColumn::Expr {
                expr: Expr::BinaryOp {
                    left: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
                    op: fsqlite_ast::BinaryOp::Add,
                    right: Box::new(Expr::Literal(Literal::Integer(2), Span::ZERO)),
                    span: Span::ZERO,
                },
                alias: None,
            }],
            from: None,
            where_clause: None,
            group_by: vec![],
            having: None,
            windows: vec![],
        };
        let aliases = extract_output_aliases(&core);
        assert_eq!(aliases, vec![None]);
    }

    // ===================================================================
    // resolve_single_table_result_columns edge cases
    // ===================================================================

    #[test]
    fn test_resolve_projection_values_core_error() {
        let core = SelectCore::Values(vec![vec![Expr::Literal(Literal::Integer(1), Span::ZERO)]]);
        let err = resolve_single_table_result_columns(&core, &["a".to_owned()])
            .expect_err("VALUES should fail");
        assert_eq!(err, SingleTableProjectionError::NotSelectCore);
    }

    #[test]
    fn test_resolve_projection_missing_from_error() {
        let core = SelectCore::Select {
            distinct: Distinctness::All,
            columns: vec![ResultColumn::Star],
            from: None,
            where_clause: None,
            group_by: vec![],
            having: None,
            windows: vec![],
        };
        let err = resolve_single_table_result_columns(&core, &["a".to_owned()])
            .expect_err("missing FROM should fail");
        assert_eq!(err, SingleTableProjectionError::MissingFromClause);
    }

    #[test]
    fn test_resolve_projection_with_joins_error() {
        use fsqlite_ast::{JoinClause, JoinKind, JoinType};
        let core = SelectCore::Select {
            distinct: Distinctness::All,
            columns: vec![ResultColumn::Star],
            from: Some(FromClause {
                source: TableOrSubquery::Table {
                    name: QualifiedName::bare("t"),
                    alias: None,
                    index_hint: None,
                },
                joins: vec![JoinClause {
                    join_type: JoinType {
                        kind: JoinKind::Inner,
                        natural: false,
                    },
                    table: TableOrSubquery::Table {
                        name: QualifiedName::bare("u"),
                        alias: None,
                        index_hint: None,
                    },
                    constraint: None,
                }],
            }),
            where_clause: None,
            group_by: vec![],
            having: None,
            windows: vec![],
        };
        let err = resolve_single_table_result_columns(&core, &["a".to_owned()])
            .expect_err("JOIN should fail");
        assert_eq!(err, SingleTableProjectionError::UnsupportedFromSource);
    }

    #[test]
    fn test_resolve_projection_unknown_table_qualifier() {
        let core = select_core_single_table(
            vec![ResultColumn::TableStar("wrong_table".to_owned())],
            "t",
            None,
        );
        let err = resolve_single_table_result_columns(&core, &["a".to_owned()])
            .expect_err("wrong qualifier should fail");
        assert_eq!(
            err,
            SingleTableProjectionError::UnknownTableQualifier {
                qualifier: "wrong_table".to_owned()
            }
        );
    }

    #[test]
    fn test_resolve_projection_qualified_column_wrong_table() {
        let core = select_core_single_table(
            vec![ResultColumn::Expr {
                expr: Expr::Column(ColumnRef::qualified("other", "a"), Span::ZERO),
                alias: None,
            }],
            "t",
            None,
        );
        let err = resolve_single_table_result_columns(&core, &["a".to_owned()])
            .expect_err("wrong table qualifier should fail");
        assert!(matches!(
            err,
            SingleTableProjectionError::UnknownTableQualifier { .. }
        ));
    }

    #[test]
    fn test_resolve_projection_preserves_expression() {
        // Non-column expressions should be preserved as-is.
        let core = select_core_single_table(
            vec![ResultColumn::Expr {
                expr: Expr::Literal(Literal::Integer(42), Span::ZERO),
                alias: Some("answer".to_owned()),
            }],
            "t",
            None,
        );
        let resolved = resolve_single_table_result_columns(&core, &["a".to_owned()])
            .expect("expression should be preserved");
        assert_eq!(resolved.len(), 1);
        assert!(matches!(
            &resolved[0],
            ResultColumn::Expr {
                alias: Some(a), ..
            } if a == "answer"
        ));
    }

    // ===================================================================
    // classify_where_term edge cases
    // ===================================================================

    #[test]
    fn test_classify_where_term_between() {
        let expr: &'static Expr = Box::leak(Box::new(Expr::Between {
            expr: Box::new(Expr::Column(ColumnRef::bare("x"), Span::ZERO)),
            low: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            high: Box::new(Expr::Literal(Literal::Integer(10), Span::ZERO)),
            not: false,
            span: Span::ZERO,
        }));
        let term = classify_where_term(expr);
        assert!(matches!(term.kind, WhereTermKind::Between));
        assert_eq!(term.column.as_ref().unwrap().column, "x");
    }

    #[test]
    fn test_classify_where_term_not_between_is_other() {
        let expr: &'static Expr = Box::leak(Box::new(Expr::Between {
            expr: Box::new(Expr::Column(ColumnRef::bare("x"), Span::ZERO)),
            low: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            high: Box::new(Expr::Literal(Literal::Integer(10), Span::ZERO)),
            not: true,
            span: Span::ZERO,
        }));
        let term = classify_where_term(expr);
        assert!(matches!(term.kind, WhereTermKind::Other));
    }

    #[test]
    fn test_classify_where_term_in_list() {
        let term = in_term("col", 5);
        assert!(matches!(term.kind, WhereTermKind::InList { count: 5 }));
        assert_eq!(term.column.as_ref().unwrap().column, "col");
    }

    #[test]
    fn test_classify_where_term_not_in_is_other() {
        let expr: &'static Expr = Box::leak(Box::new(Expr::In {
            expr: Box::new(Expr::Column(ColumnRef::bare("x"), Span::ZERO)),
            set: InSet::List(vec![Expr::Literal(Literal::Integer(1), Span::ZERO)]),
            not: true,
            span: Span::ZERO,
        }));
        let term = classify_where_term(expr);
        assert!(matches!(term.kind, WhereTermKind::Other));
    }

    #[test]
    fn test_classify_where_term_like_prefix() {
        let term = like_term("name", "abc%");
        assert!(matches!(
            term.kind,
            WhereTermKind::LikePrefix {
                ref prefix,
                upper_bound: Some(ref upper_bound),
            } if prefix == "abc" && upper_bound == "abd"
        ));
        assert_eq!(term.column.as_ref().unwrap().column, "name");
    }

    #[test]
    fn test_classify_where_term_like_no_prefix_is_other() {
        let term = like_term("name", "%wildcard");
        assert!(matches!(term.kind, WhereTermKind::Other));
    }

    #[test]
    fn test_classify_where_term_glob_prefix() {
        let term = glob_term("name", "abc*");
        assert!(matches!(
            term.kind,
            WhereTermKind::LikePrefix {
                ref prefix,
                upper_bound: Some(ref upper_bound),
            } if prefix == "abc" && upper_bound == "abd"
        ));
        assert_eq!(term.column.as_ref().unwrap().column, "name");
    }

    #[test]
    fn test_classify_where_term_glob_no_prefix_is_other() {
        let term = glob_term("name", "*wildcard");
        assert!(matches!(term.kind, WhereTermKind::Other));
    }

    #[test]
    fn test_classify_where_term_rowid_aliases() {
        // _rowid_ and oid are also rowid aliases
        for alias in &["_rowid_", "oid", "ROWID", "OID"] {
            let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::bare(*alias), Span::ZERO)),
                op: AstBinaryOp::Eq,
                right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
                span: Span::ZERO,
            }));
            let term = classify_where_term(expr);
            assert!(
                matches!(term.kind, WhereTermKind::RowidEquality),
                "'{alias}' should be classified as RowidEquality"
            );
        }
    }

    #[test]
    fn test_classify_where_term_reversed_equality() {
        // expr = col (column on the right side)
        let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Literal(Literal::Integer(42), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Column(ColumnRef::bare("x"), Span::ZERO)),
            span: Span::ZERO,
        }));
        let term = classify_where_term(expr);
        assert!(matches!(term.kind, WhereTermKind::Equality));
        assert_eq!(term.column.as_ref().unwrap().column, "x");
    }

    #[test]
    fn test_classify_where_term_reversed_rowid_equality() {
        // 42 = rowid (column on the right side)
        let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Literal(Literal::Integer(42), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Column(ColumnRef::bare("rowid"), Span::ZERO)),
            span: Span::ZERO,
        }));
        let term = classify_where_term(expr);
        assert!(matches!(term.kind, WhereTermKind::RowidEquality));
    }

    #[test]
    fn test_classify_where_term_eq_no_columns_is_other() {
        // 1 = 2 (no columns on either side)
        let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(2), Span::ZERO)),
            span: Span::ZERO,
        }));
        let term = classify_where_term(expr);
        assert!(matches!(term.kind, WhereTermKind::Other));
        assert!(term.column.is_none());
    }

    #[test]
    fn test_classify_where_term_generic_fallback() {
        // OR expression → Other
        let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            op: AstBinaryOp::Or,
            right: Box::new(Expr::Literal(Literal::Integer(0), Span::ZERO)),
            span: Span::ZERO,
        }));
        let term = classify_where_term(expr);
        assert!(matches!(term.kind, WhereTermKind::Other));
    }

    #[test]
    fn test_classify_where_term_or_same_column_becomes_in_list() {
        let term = or_eq_term("a", &[1, 2, 3]);
        assert!(matches!(term.kind, WhereTermKind::InList { count: 3 }));
        assert_eq!(term.column.as_ref().map(|c| c.column.as_str()), Some("a"));
    }

    #[test]
    fn test_classify_where_term_or_reversed_equalities_becomes_in_list() {
        let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
                op: AstBinaryOp::Eq,
                right: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
                span: Span::ZERO,
            }),
            op: AstBinaryOp::Or,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Literal(Literal::Integer(2), Span::ZERO)),
                op: AstBinaryOp::Eq,
                right: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
                span: Span::ZERO,
            }),
            span: Span::ZERO,
        }));

        let term = classify_where_term(expr);
        assert!(matches!(term.kind, WhereTermKind::InList { count: 2 }));
        assert_eq!(term.column.as_ref().map(|c| c.column.as_str()), Some("a"));
    }

    #[test]
    fn test_classify_where_term_or_mixed_columns_is_other() {
        let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
                op: AstBinaryOp::Eq,
                right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
                span: Span::ZERO,
            }),
            op: AstBinaryOp::Or,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::bare("b"), Span::ZERO)),
                op: AstBinaryOp::Eq,
                right: Box::new(Expr::Literal(Literal::Integer(2), Span::ZERO)),
                span: Span::ZERO,
            }),
            span: Span::ZERO,
        }));

        let term = classify_where_term(expr);
        assert!(matches!(term.kind, WhereTermKind::Other));
    }

    // ===================================================================
    // decompose_where edge cases
    // ===================================================================

    #[test]
    fn test_decompose_where_nested_and() {
        // (a = 1 AND b = 2) AND c = 3 → 3 terms
        let inner = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
                    op: AstBinaryOp::Eq,
                    right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
                    span: Span::ZERO,
                }),
                op: AstBinaryOp::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Column(ColumnRef::bare("b"), Span::ZERO)),
                    op: AstBinaryOp::Eq,
                    right: Box::new(Expr::Literal(Literal::Integer(2), Span::ZERO)),
                    span: Span::ZERO,
                }),
                span: Span::ZERO,
            }),
            op: AstBinaryOp::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::bare("c"), Span::ZERO)),
                op: AstBinaryOp::Eq,
                right: Box::new(Expr::Literal(Literal::Integer(3), Span::ZERO)),
                span: Span::ZERO,
            }),
            span: Span::ZERO,
        };
        let terms = decompose_where(&inner);
        assert_eq!(terms.len(), 3);
    }

    #[test]
    fn test_decompose_where_single_term() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            span: Span::ZERO,
        };
        let terms = decompose_where(&expr);
        assert_eq!(terms.len(), 1);
    }

    // ===================================================================
    // extract_like_prefix edge cases
    // ===================================================================

    #[test]
    fn test_extract_like_prefix_underscore_wildcard() {
        // "abc_def" → prefix = "abc" (underscore is wildcard)
        let pat = Expr::Literal(Literal::String("abc_def".to_owned()), Span::ZERO);
        assert_eq!(extract_like_prefix(&pat), Some("abc".to_owned()));
    }

    #[test]
    fn test_extract_like_prefix_no_wildcards() {
        // "exact" → prefix = "exact" (no wildcards)
        let pat = Expr::Literal(Literal::String("exact".to_owned()), Span::ZERO);
        assert_eq!(extract_like_prefix(&pat), Some("exact".to_owned()));
    }

    #[test]
    fn test_extract_like_prefix_non_string_expr() {
        // Non-string expression → None
        let pat = Expr::Literal(Literal::Integer(42), Span::ZERO);
        assert_eq!(extract_like_prefix(&pat), None);
    }

    #[test]
    fn test_extract_glob_prefix_star_wildcard() {
        // "abc*def" → prefix = "abc" (star is wildcard)
        let pat = Expr::Literal(Literal::String("abc*def".to_owned()), Span::ZERO);
        assert_eq!(extract_glob_prefix(&pat), Some("abc".to_owned()));
    }

    #[test]
    fn test_extract_glob_prefix_char_class_wildcard() {
        // "abc[0-9]" → prefix = "abc" (char class starts wildcard region)
        let pat = Expr::Literal(Literal::String("abc[0-9]".to_owned()), Span::ZERO);
        assert_eq!(extract_glob_prefix(&pat), Some("abc".to_owned()));
    }

    #[test]
    fn test_extract_glob_prefix_non_string_expr() {
        // Non-string expression → None
        let pat = Expr::Literal(Literal::Integer(42), Span::ZERO);
        assert_eq!(extract_glob_prefix(&pat), None);
    }

    // ===================================================================
    // Join ordering / star query edge cases
    // ===================================================================

    #[test]
    fn test_detect_star_query_too_few_tables() {
        let tables = [table_stats("t1", 100, 1000), table_stats("t2", 100, 1000)];
        let terms = [join_term("t1", "id", "t2", "fk")];
        assert!(!detect_star_query(&tables, &terms));
    }

    #[test]
    fn test_mx_choice_zero_tables() {
        assert_eq!(compute_mx_choice(0, false), 1);
    }

    // ===================================================================
    // best_access_path edge cases
    // ===================================================================

    #[test]
    fn test_best_access_path_unique_index_equality() {
        let table = table_stats("t1", 1000, 50000);
        let idx = index_info("idx_pk", "t1", &["id"], true, 100);
        let terms = [eq_term("id")];
        let ap = best_access_path(&table, &[idx], &terms, None);
        // Unique index equality → estimated_rows = 1.0
        assert!(
            (ap.estimated_rows - 1.0).abs() < f64::EPSILON,
            "unique index equality should return 1 row, got {}",
            ap.estimated_rows
        );
    }

    #[test]
    fn test_best_access_path_in_expansion() {
        let table = table_stats("t1", 100, 1000);
        let idx = index_info("idx_col", "t1", &["col"], false, 20);
        let terms = [in_term("col", 3)];
        let ap = best_access_path(&table, &[idx], &terms, None);
        assert!(matches!(ap.kind, AccessPathKind::IndexScanEquality));
        assert!(ap.index.is_some());
    }

    #[test]
    fn test_best_access_path_like_prefix() {
        let table = table_stats("t1", 100, 1000);
        let idx = index_info("idx_name", "t1", &["name"], false, 20);
        let terms = [like_term("name", "Jo%")];
        let ap = best_access_path(&table, &[idx], &terms, None);
        // LIKE prefix should use index range scan
        assert!(
            matches!(
                ap.kind,
                AccessPathKind::IndexScanRange { .. } | AccessPathKind::CoveringIndexScan { .. }
            ),
            "LIKE prefix should use index scan, got {:?}",
            ap.kind
        );
    }

    #[test]
    fn test_best_access_path_glob_prefix() {
        let table = table_stats("t1", 100, 1000);
        let idx = index_info("idx_name", "t1", &["name"], false, 20);
        let terms = [glob_term("name", "Jo*")];
        let ap = best_access_path(&table, &[idx], &terms, None);
        // GLOB prefix should use index range scan
        assert!(
            matches!(
                ap.kind,
                AccessPathKind::IndexScanRange { .. } | AccessPathKind::CoveringIndexScan { .. }
            ),
            "GLOB prefix should use index scan, got {:?}",
            ap.kind
        );
    }

    #[test]
    fn test_best_access_path_between_range() {
        let table = table_stats("t1", 100, 1000);
        let idx = index_info("idx_a", "t1", &["a"], false, 20);
        let expr: &'static Expr = Box::leak(Box::new(Expr::Between {
            expr: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            low: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            high: Box::new(Expr::Literal(Literal::Integer(100), Span::ZERO)),
            not: false,
            span: Span::ZERO,
        }));
        let term = classify_where_term(expr);
        let ap = best_access_path(&table, &[idx], &[term], None);
        assert!(matches!(ap.kind, AccessPathKind::IndexScanRange { .. }));
    }

    #[test]
    fn test_best_access_path_ignores_wrong_table_index() {
        // Index belongs to different table — should not be used.
        let table = table_stats("t1", 100, 1000);
        let idx = index_info("idx_other", "t2", &["a"], false, 20);
        let terms = [eq_term("a")];
        let ap = best_access_path(&table, &[idx], &terms, None);
        assert!(matches!(ap.kind, AccessPathKind::FullTableScan));
    }

    #[test]
    fn test_best_access_path_empty_index_columns() {
        // Index with no columns → not usable.
        let table = table_stats("t1", 100, 1000);
        let idx = IndexInfo {
            name: "idx_empty".to_owned(),
            table: "t1".to_owned(),
            columns: vec![],
            unique: false,
            n_pages: 10,
            source: StatsSource::Heuristic,
            partial_where: None,
            expression_columns: vec![],
        };
        let terms = [eq_term("a")];
        let ap = best_access_path(&table, &[idx], &terms, None);
        assert!(matches!(ap.kind, AccessPathKind::FullTableScan));
    }

    #[test]
    fn test_best_access_path_skip_scan_on_low_cardinality_leading_column() {
        let table = TableStats {
            name: "users".to_owned(),
            n_pages: 4_096,
            n_rows: 2_000_000,
            source: StatsSource::Analyze,
        };
        let idx = IndexInfo {
            name: "idx_tenant_email".to_owned(),
            table: "users".to_owned(),
            columns: vec!["tenant_id".to_owned(), "email".to_owned()],
            unique: false,
            n_pages: 64,
            source: StatsSource::Analyze,
            partial_where: None,
            expression_columns: vec![],
        };

        let ap = best_access_path(&table, &[idx], &[eq_term("email")], None);
        assert_eq!(ap.index.as_deref(), Some("idx_tenant_email"));
        assert!(matches!(
            ap.kind,
            AccessPathKind::IndexScanRange { .. } | AccessPathKind::CoveringIndexScan { .. }
        ));
    }

    #[test]
    fn test_best_access_path_skip_scan_rejects_high_cardinality_leading_column() {
        let table = TableStats {
            name: "users".to_owned(),
            n_pages: 2_000,
            n_rows: 1_000_000,
            source: StatsSource::Analyze,
        };
        let idx = IndexInfo {
            name: "idx_region_email".to_owned(),
            table: "users".to_owned(),
            columns: vec!["region_code".to_owned(), "email".to_owned()],
            unique: false,
            n_pages: SKIP_SCAN_PAGES_PER_LEADING_DISTINCT * (SKIP_SCAN_MAX_LEADING_DISTINCT + 2),
            source: StatsSource::Analyze,
            partial_where: None,
            expression_columns: vec![],
        };

        let ap = best_access_path(&table, &[idx], &[eq_term("email")], None);
        assert!(matches!(ap.kind, AccessPathKind::FullTableScan));
    }

    #[test]
    fn test_best_access_path_partial_index_requires_implied_predicate() {
        let table = table_stats("t1", 100, 1000);
        let mut partial_idx = index_info("idx_partial_a", "t1", &["a"], false, 20);
        partial_idx.partial_where = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            span: Span::ZERO,
        });

        let ap_not_implied = best_access_path(
            &table,
            &[partial_idx.clone()],
            &[eq_term_value("a", 2)],
            None,
        );
        assert!(matches!(ap_not_implied.kind, AccessPathKind::FullTableScan));

        let ap_implied = best_access_path(&table, &[partial_idx], &[eq_term_value("a", 1)], None);
        assert!(matches!(
            ap_implied.kind,
            AccessPathKind::IndexScanEquality | AccessPathKind::CoveringIndexScan { .. }
        ));
    }

    #[test]
    fn test_best_access_path_respects_indexed_by_hint() {
        let table = table_stats("t1", 2000, 100_000);
        let fast = index_info("idx_fast", "t1", &["a"], false, 10);
        let slow = index_info("idx_slow", "t1", &["a"], false, 600);
        let terms = [eq_term("a")];
        let hint = IndexHint::IndexedBy("idx_slow".to_owned());

        let ap =
            best_access_path_with_hints(&table, &[fast, slow], &terms, None, Some(&hint), None);
        assert_eq!(ap.index.as_deref(), Some("idx_slow"));
        assert!(matches!(
            ap.kind,
            AccessPathKind::IndexScanEquality
                | AccessPathKind::IndexScanRange { .. }
                | AccessPathKind::CoveringIndexScan { .. }
        ));
    }

    #[test]
    fn test_best_access_path_respects_not_indexed_hint() {
        let table = table_stats("t1", 1024, 50000);
        let idx = index_info("idx_a", "t1", &["a"], false, 20);
        let rowid_expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("rowid"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(42), Span::ZERO)),
            span: Span::ZERO,
        }));
        let rowid_term = classify_where_term(rowid_expr);
        let hint = IndexHint::NotIndexed;

        let ap =
            best_access_path_with_hints(&table, &[idx], &[rowid_term], None, Some(&hint), None);
        assert!(matches!(ap.kind, AccessPathKind::FullTableScan));
        assert!(ap.index.is_none());
    }

    #[test]
    fn test_cracking_hint_store_reuses_prior_index_choice() {
        let table = table_stats("t1", 1000, 50000);
        let idx_a = index_info("idx_a", "t1", &["a"], false, 40);
        let idx_b = index_info("idx_b", "t1", &["a"], false, 40);
        let terms = [eq_term("a")];
        let mut hint_store = CrackingHintStore::default();

        let first = best_access_path_with_hints(
            &table,
            &[idx_a.clone(), idx_b.clone()],
            &terms,
            None,
            None,
            Some(&mut hint_store),
        );
        assert_eq!(first.index.as_deref(), Some("idx_a"));
        assert_eq!(hint_store.preferred_index("t1"), Some("idx_a"));

        // Reverse candidate order; adaptive hint should bias back to idx_a.
        let second = best_access_path_with_hints(
            &table,
            &[idx_b, idx_a],
            &terms,
            None,
            None,
            Some(&mut hint_store),
        );
        assert_eq!(second.index.as_deref(), Some("idx_a"));
    }

    #[test]
    fn test_index_selection_metric_counter_advances() {
        let table = table_stats("t1", 500, 10000);
        let idx = index_info("idx_a", "t1", &["a"], false, 20);
        let terms = [eq_term("a")];
        let before = snapshot_index_selection_totals()
            .get("index_scan_equality")
            .copied()
            .unwrap_or(0);

        let _ = best_access_path(&table, &[idx], &terms, None);

        let after = snapshot_index_selection_totals()
            .get("index_scan_equality")
            .copied()
            .unwrap_or(0);
        assert!(after > before);
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn planner_index_selection_e2e_replay_emits_artifact() {
        use fsqlite_ast::{JoinClause, JoinKind, JoinType};

        const BEAD_ID: &str = "bd-1as.4";
        const DEFAULT_SCENARIO_ID: &str = "PLANNER-INDEX-1";
        const DEFAULT_SEED: u64 = 20_260_219;

        let run_id =
            std::env::var("RUN_ID").unwrap_or_else(|_| format!("{BEAD_ID}-seed-{DEFAULT_SEED}"));
        let trace_id = std::env::var("TRACE_ID")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_SEED);
        let scenario_id =
            std::env::var("SCENARIO_ID").unwrap_or_else(|_| DEFAULT_SCENARIO_ID.to_owned());
        let seed = std::env::var("SEED")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_SEED);

        let artifact_path = std::env::var("FSQLITE_PLANNER_INDEX_E2E_ARTIFACT").map_or_else(
            |_| {
                PathBuf::from("artifacts")
                    .join(BEAD_ID)
                    .join("planner_index_selection_e2e_artifact.json")
            },
            PathBuf::from,
        );
        if let Some(parent) = artifact_path.parent() {
            std::fs::create_dir_all(parent)
                .expect("bead_id={BEAD_ID} artifact directory should be writable");
        }

        let started = Instant::now();
        let mut cracking_hints = CrackingHintStore::default();
        let before_metrics = snapshot_index_selection_totals();

        let from = FromClause {
            source: TableOrSubquery::Table {
                name: QualifiedName::bare("users"),
                alias: Some("u".to_owned()),
                index_hint: Some(IndexHint::IndexedBy("idx_users_email".to_owned())),
            },
            joins: vec![JoinClause {
                join_type: JoinType {
                    kind: JoinKind::Inner,
                    natural: false,
                },
                table: TableOrSubquery::Table {
                    name: QualifiedName::bare("events"),
                    alias: Some("e".to_owned()),
                    index_hint: Some(IndexHint::NotIndexed),
                },
                constraint: None,
            }],
        };
        let table_hints = collect_table_index_hints(&from);

        let tables = [
            table_stats("users", 2_048, 120_000),
            table_stats("events", 8_192, 1_200_000),
            table_stats("sessions", 4_096, 900_000),
        ];
        let indexes = [
            index_info("idx_users_email", "users", &["email"], true, 120),
            index_info("idx_users_id", "users", &["id"], true, 240),
            index_info("idx_events_user_id", "events", &["user_id"], false, 110),
            index_info(
                "idx_sessions_user_id_a",
                "sessions",
                &["user_id"],
                false,
                90,
            ),
            index_info(
                "idx_sessions_user_id_b",
                "sessions",
                &["user_id"],
                false,
                90,
            ),
        ];
        let where_terms = [
            eq_term("email"),
            eq_term("user_id"),
            join_term("events", "user_id", "users", "id"),
        ];

        let first_plan = order_joins_with_hints(
            &tables[..2],
            &indexes,
            &where_terms,
            Some(&["email".to_owned(), "user_id".to_owned()]),
            &[],
            Some(&table_hints),
            Some(&mut cracking_hints),
        );
        let users_path = first_plan
            .access_paths
            .iter()
            .find(|path| path.table.eq_ignore_ascii_case("users"))
            .expect("bead_id={BEAD_ID} users path should exist");
        assert_eq!(users_path.index.as_deref(), Some("idx_users_email"));
        let events_path = first_plan
            .access_paths
            .iter()
            .find(|path| path.table.eq_ignore_ascii_case("events"))
            .expect("bead_id={BEAD_ID} events path should exist");
        assert!(
            matches!(events_path.kind, AccessPathKind::FullTableScan),
            "bead_id={BEAD_ID} NOT INDEXED must force full scan for events",
        );

        let first_session_path = best_access_path_with_hints(
            &tables[2],
            &indexes[3..5],
            &where_terms,
            None,
            None,
            Some(&mut cracking_hints),
        );
        let second_session_path = best_access_path_with_hints(
            &tables[2],
            &[indexes[4].clone(), indexes[3].clone()],
            &where_terms,
            None,
            None,
            Some(&mut cracking_hints),
        );
        assert_eq!(
            first_session_path.index.as_deref(),
            second_session_path.index.as_deref(),
            "bead_id={BEAD_ID} adaptive cracking hint should keep stable index preference",
        );

        let after_metrics = snapshot_index_selection_totals();
        let metric_delta = after_metrics
            .iter()
            .map(|(label, after)| {
                let before = before_metrics.get(label).copied().unwrap_or(0);
                (label.clone(), after.saturating_sub(before))
            })
            .collect::<BTreeMap<_, _>>();
        let elapsed_us = started.elapsed().as_micros().max(1);
        let replay_command = format!(
            "RUN_ID='{}' TRACE_ID={} SCENARIO_ID='{}' SEED={} FSQLITE_PLANNER_INDEX_E2E_ARTIFACT='{}' cargo test -p fsqlite-planner planner_index_selection_e2e_replay_emits_artifact -- --exact --nocapture",
            run_id,
            trace_id,
            scenario_id,
            seed,
            artifact_path.display(),
        );

        let plan_fingerprint = blake3::hash(
            format!(
                "{}|{}|{}|{}|{:?}|{:?}",
                first_plan.join_order.join(","),
                users_path.index.clone().unwrap_or_default(),
                access_path_metric_label(&events_path.kind),
                second_session_path.index.clone().unwrap_or_default(),
                first_session_path.kind,
                second_session_path.kind,
            )
            .as_bytes(),
        )
        .to_hex()
        .to_string();
        let artifact = serde_json::json!({
            "bead_id": BEAD_ID,
            "run_id": run_id,
            "trace_id": trace_id,
            "scenario_id": scenario_id,
            "seed": seed,
            "overall_status": "pass",
            "timing": {
                "selection_elapsed_us": elapsed_us,
            },
            "checks": [
                {
                    "id": "indexed_by_respected",
                    "status": "pass",
                    "detail": "users path honors INDEXED BY idx_users_email"
                },
                {
                    "id": "not_indexed_respected",
                    "status": "pass",
                    "detail": "events path honors NOT INDEXED by forcing full scan"
                },
                {
                    "id": "adaptive_hint_reuse",
                    "status": "pass",
                    "detail": "sessions path reuses prior cracking hint under candidate reordering"
                }
            ],
            "metric_delta": metric_delta,
            "plan_fingerprint_blake3": plan_fingerprint,
            "observability": {
                "required_fields": [
                    "run_id",
                    "trace_id",
                    "scenario_id",
                    "selection_elapsed_us",
                    "table",
                    "chosen_index",
                    "index_type",
                    "candidates"
                ],
                "event_name": "planner.index_select.choice"
            },
            "replay_command": replay_command,
        });
        let artifact_bytes = serde_json::to_vec_pretty(&artifact)
            .expect("bead_id={BEAD_ID} artifact serialization should succeed");
        std::fs::write(&artifact_path, artifact_bytes)
            .expect("bead_id={BEAD_ID} artifact write should succeed");
        assert!(
            artifact_path.exists(),
            "bead_id={BEAD_ID} e2e artifact path should exist"
        );
    }

    #[test]
    fn test_index_usability_between_on_leftmost() {
        let idx = index_info("idx_a", "t1", &["a"], false, 50);
        let expr: &'static Expr = Box::leak(Box::new(Expr::Between {
            expr: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            low: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            high: Box::new(Expr::Literal(Literal::Integer(10), Span::ZERO)),
            not: false,
            span: Span::ZERO,
        }));
        let term = classify_where_term(expr);
        assert!(matches!(
            analyze_index_usability(&idx, &[term]),
            IndexUsability::Range { .. }
        ));
    }

    // ===================================================================
    // WhereTermKind / WhereColumn equality tests
    // ===================================================================

    #[test]
    fn test_where_term_kind_equality() {
        assert_eq!(WhereTermKind::Equality, WhereTermKind::Equality);
        assert_eq!(WhereTermKind::Range, WhereTermKind::Range);
        assert_eq!(WhereTermKind::Between, WhereTermKind::Between);
        assert_eq!(
            WhereTermKind::InList { count: 3 },
            WhereTermKind::InList { count: 3 }
        );
        assert_ne!(
            WhereTermKind::InList { count: 3 },
            WhereTermKind::InList { count: 5 }
        );
        assert_eq!(
            WhereTermKind::LikePrefix {
                prefix: "abc".to_owned(),
                upper_bound: Some("abd".to_owned()),
            },
            WhereTermKind::LikePrefix {
                prefix: "abc".to_owned(),
                upper_bound: Some("abd".to_owned()),
            }
        );
        assert_ne!(WhereTermKind::Equality, WhereTermKind::Range);
    }

    #[test]
    fn test_where_column_equality() {
        let wc1 = WhereColumn {
            table: Some("t".to_owned()),
            column: "a".to_owned(),
        };
        let wc2 = WhereColumn {
            table: Some("t".to_owned()),
            column: "a".to_owned(),
        };
        let wc3 = WhereColumn {
            table: None,
            column: "a".to_owned(),
        };
        assert_eq!(wc1, wc2);
        assert_ne!(wc1, wc3);
    }

    // ===================================================================
    // StatsSource tests
    // ===================================================================

    #[test]
    fn test_stats_source_equality() {
        assert_eq!(StatsSource::Analyze, StatsSource::Analyze);
        assert_eq!(StatsSource::Heuristic, StatsSource::Heuristic);
        assert_ne!(StatsSource::Analyze, StatsSource::Heuristic);
    }

    // ===================================================================
    // cost model minimum page clamp
    // ===================================================================

    #[test]
    fn test_cost_minimum_page_clamp() {
        // With 0 pages, cost should use max(1) = 1.
        let cost = estimate_cost(&AccessPathKind::FullTableScan, 0, 0);
        assert!(
            (cost - 1.0).abs() < f64::EPSILON,
            "0 pages should clamp to 1"
        );

        let cost = estimate_cost(&AccessPathKind::RowidLookup, 0, 0);
        assert!(
            (cost - 0.0).abs() < f64::EPSILON,
            "log2(1) = 0.0 for clamped 0 pages"
        );
    }

    // -----------------------------------------------------------------------
    // Proptest: property-based tests for query planner (bd-1lsfu.4)
    // -----------------------------------------------------------------------

    mod proptest_planner {
        use super::*;
        use fsqlite_ast::{
            ColumnRef, Distinctness, Expr, Literal, OrderingTerm, ResultColumn, SelectBody,
            SelectCore, Span,
        };
        use proptest::prelude::*;

        /// Generate random table stats with realistic ranges.
        fn arb_table_stats() -> BoxedStrategy<TableStats> {
            (
                prop::string::string_regex("[a-z][a-z0-9]{0,5}").expect("valid regex"),
                1u64..10_000,
                1u64..1_000_000,
            )
                .prop_map(|(name, n_pages, n_rows)| TableStats {
                    name,
                    n_pages,
                    n_rows,
                    source: StatsSource::Heuristic,
                })
                .boxed()
        }

        /// Generate random index info for a given table.
        #[allow(dead_code)]
        fn arb_index_info(table_name: String) -> BoxedStrategy<IndexInfo> {
            (
                prop::string::string_regex("idx_[a-z]{1,4}").expect("valid regex"),
                proptest::collection::vec(
                    prop::string::string_regex("[a-z]{1,4}").expect("valid regex"),
                    1..4,
                ),
                any::<bool>(),
                1u64..5_000,
            )
                .prop_map(move |(name, columns, unique, n_pages)| IndexInfo {
                    name,
                    table: table_name.clone(),
                    columns,
                    unique,
                    n_pages,
                    source: StatsSource::Heuristic,
                    partial_where: None,
                    expression_columns: vec![],
                })
                .boxed()
        }

        /// Generate a selectivity in (0, 1].
        fn arb_selectivity() -> BoxedStrategy<f64> {
            (1u32..1000).prop_map(|n| f64::from(n) / 1000.0).boxed()
        }

        // Property 1: Cost model non-negativity — all costs >= 0.
        proptest::proptest! {
            #![proptest_config(proptest::prelude::ProptestConfig::with_cases(1000))]

            #[test]
            fn test_cost_non_negative(
                table_pages in 0u64..100_000,
                index_pages in 0u64..100_000,
                selectivity in arb_selectivity(),
            ) {
                let kinds = [
                    AccessPathKind::FullTableScan,
                    AccessPathKind::IndexScanEquality,
                    AccessPathKind::RowidLookup,
                    AccessPathKind::IndexScanRange { selectivity },
                    AccessPathKind::CoveringIndexScan { selectivity },
                ];
                for kind in &kinds {
                    let cost = estimate_cost(kind, table_pages, index_pages);
                    prop_assert!(
                        cost >= 0.0,
                        "cost must be non-negative, got {cost} for {kind:?} \
                         (table_pages={table_pages}, index_pages={index_pages})"
                    );
                    prop_assert!(
                        cost.is_finite(),
                        "cost must be finite, got {cost} for {kind:?}"
                    );
                }
            }
        }

        // Property 2: Cost hierarchy — RowidLookup ≤ IndexScanEquality ≤ FullTableScan
        // for tables with at least a few pages.
        proptest::proptest! {
            #![proptest_config(proptest::prelude::ProptestConfig::with_cases(500))]

            #[test]
            fn test_cost_hierarchy(
                table_pages in 10u64..100_000,
                index_pages in 2u64..10_000,
            ) {
                let rowid_cost = estimate_cost(
                    &AccessPathKind::RowidLookup,
                    table_pages,
                    index_pages,
                );
                let eq_cost = estimate_cost(
                    &AccessPathKind::IndexScanEquality,
                    table_pages,
                    index_pages,
                );
                let full_cost = estimate_cost(
                    &AccessPathKind::FullTableScan,
                    table_pages,
                    index_pages,
                );

                prop_assert!(
                    rowid_cost <= eq_cost + f64::EPSILON,
                    "rowid lookup ({rowid_cost}) should be ≤ index equality ({eq_cost}) \
                     for table_pages={table_pages}, index_pages={index_pages}"
                );
                prop_assert!(
                    eq_cost <= full_cost + f64::EPSILON,
                    "index equality ({eq_cost}) should be ≤ full scan ({full_cost}) \
                     for table_pages={table_pages}, index_pages={index_pages}"
                );
            }
        }

        // Property 3: Cost monotonicity in selectivity — lower selectivity means
        // lower cost for range scans.
        proptest::proptest! {
            #![proptest_config(proptest::prelude::ProptestConfig::with_cases(500))]

            #[test]
            fn test_cost_selectivity_monotonic(
                table_pages in 10u64..100_000,
                index_pages in 2u64..10_000,
                s1 in 1u32..500,
                s2 in 500u32..1000,
            ) {
                let sel_low = f64::from(s1) / 1000.0;
                let sel_high = f64::from(s2) / 1000.0;

                let cost_low = estimate_cost(
                    &AccessPathKind::IndexScanRange { selectivity: sel_low },
                    table_pages,
                    index_pages,
                );
                let cost_high = estimate_cost(
                    &AccessPathKind::IndexScanRange { selectivity: sel_high },
                    table_pages,
                    index_pages,
                );

                prop_assert!(
                    cost_low <= cost_high + f64::EPSILON,
                    "lower selectivity ({sel_low}) should have lower cost ({cost_low}) \
                     than higher selectivity ({sel_high}) cost ({cost_high})"
                );
            }
        }

        // Property 4: Join ordering determinism — same inputs always produce
        // the same plan.
        proptest::proptest! {
            #![proptest_config(proptest::prelude::ProptestConfig::with_cases(200))]

            #[test]
            fn test_join_order_determinism(
                stats1 in arb_table_stats(),
                stats2 in arb_table_stats(),
            ) {
                // Ensure distinct table names.
                let s1 = stats1;
                let mut s2 = stats2;
                if s1.name == s2.name {
                    s2.name = format!("{}_b", s2.name);
                }

                let tables = [s1, s2];
                let empty_indexes: Vec<IndexInfo> = vec![];
                let empty_terms: Vec<WhereTerm<'_>> = vec![];
                let empty_cross: Vec<(String, String)> = vec![];

                let plan_a = order_joins(
                    &tables,
                    &empty_indexes,
                    &empty_terms,
                    None,
                    &empty_cross,
                );
                let plan_b = order_joins(
                    &tables,
                    &empty_indexes,
                    &empty_terms,
                    None,
                    &empty_cross,
                );

                prop_assert_eq!(
                    plan_a.join_order,
                    plan_b.join_order,
                    "join order should be deterministic"
                );
                prop_assert!(
                    (plan_a.total_cost - plan_b.total_cost).abs() < f64::EPSILON,
                    "total cost should be deterministic: {:.6} vs {:.6}",
                    plan_a.total_cost,
                    plan_b.total_cost,
                );
            }
        }

        // Property 5: Adding an index never increases the best access path cost.
        proptest::proptest! {
            #![proptest_config(proptest::prelude::ProptestConfig::with_cases(300))]

            #[test]
            fn test_index_never_increases_cost(
                stats in arb_table_stats(),
            ) {
                let table = stats;
                let empty_terms: Vec<WhereTerm<'_>> = vec![];

                // Cost without any index.
                let no_index_path = best_access_path(
                    &table,
                    &[],
                    &empty_terms,
                    None,
                );

                // Create an index on this table.
                let idx = IndexInfo {
                    name: "idx_test".to_string(),
                    table: table.name.clone(),
                    columns: vec!["col_a".to_string()],
                    unique: false,
                    n_pages: table.n_pages / 5 + 1,
                    source: StatsSource::Heuristic,
                    partial_where: None,
                    expression_columns: vec![],
                };

                let with_index_path = best_access_path(
                    &table,
                    &[idx],
                    &empty_terms,
                    None,
                );

                prop_assert!(
                    with_index_path.estimated_cost <= no_index_path.estimated_cost + f64::EPSILON,
                    "adding an index should not increase cost: \
                     without={:.2}, with={:.2}",
                    no_index_path.estimated_cost,
                    with_index_path.estimated_cost,
                );
            }
        }

        // Property 6: Compound ORDER BY resolution is deterministic.
        proptest::proptest! {
            #![proptest_config(proptest::prelude::ProptestConfig::with_cases(200))]

            #[test]
            fn test_order_by_resolution_deterministic(
                ncols in 1usize..5,
                order_idx in 1usize..5,
            ) {
                // Build a synthetic compound SELECT with aliases.
                let cols: Vec<ResultColumn> = (0..ncols)
                    .map(|i| ResultColumn::Expr {
                        expr: Expr::Column(
                            ColumnRef::bare(format!("c{i}")),
                            Span::ZERO,
                        ),
                        alias: Some(format!("a{i}")),
                    })
                    .collect();
                let core = SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: cols,
                    from: None,
                    where_clause: None,
                    group_by: vec![],
                    having: None,
                    windows: vec![],
                };

                // ORDER BY a numeric index (clamped to valid range).
                let valid_idx = (order_idx % ncols) + 1;
                let order_term = OrderingTerm {
                    expr: Expr::Literal(
                        Literal::Integer(i64::try_from(valid_idx).unwrap_or(1)),
                        Span::ZERO,
                    ),
                    direction: None,
                    nulls: None,
                };

                let body = SelectBody {
                    select: core,
                    compounds: vec![],
                };

                let result1 = resolve_compound_order_by(
                    &body,
                    std::slice::from_ref(&order_term),
                );
                let result2 = resolve_compound_order_by(
                    &body,
                    std::slice::from_ref(&order_term),
                );

                prop_assert_eq!(
                    result1, result2,
                    "ORDER BY resolution should be deterministic"
                );
            }
        }

        // Property 7: Full table scan cost scales linearly with page count.
        proptest::proptest! {
            #![proptest_config(proptest::prelude::ProptestConfig::with_cases(500))]

            #[test]
            fn test_full_scan_linear_scaling(
                pages in 1u64..100_000,
                multiplier in 2u64..10,
            ) {
                let cost_base = estimate_cost(
                    &AccessPathKind::FullTableScan,
                    pages,
                    0,
                );
                let cost_scaled = estimate_cost(
                    &AccessPathKind::FullTableScan,
                    pages * multiplier,
                    0,
                );

                // For full scan, cost = table_pages, so scaling should be exact.
                let expected_ratio = multiplier as f64;
                let actual_ratio = cost_scaled / cost_base;
                prop_assert!(
                    (actual_ratio - expected_ratio).abs() < 0.01,
                    "full scan cost should scale linearly: \
                     expected ratio {expected_ratio}, got {actual_ratio}"
                );
            }
        }
    }

    // ── Cost metrics and asymmetric loss tests (bd-1as.1) ──

    #[test]
    fn test_cost_estimates_metric_increments() {
        reset_cost_metrics();
        let before = cost_metrics_snapshot();

        // Each estimate_cost call should increment the counter.
        let _ = estimate_cost(&AccessPathKind::FullTableScan, 100, 0);
        let _ = estimate_cost(&AccessPathKind::RowidLookup, 100, 0);

        let after = cost_metrics_snapshot();
        assert!(
            after.fsqlite_planner_cost_estimates_total
                >= before.fsqlite_planner_cost_estimates_total + 2
        );
    }

    #[test]
    fn test_estimation_error_recording() {
        reset_cost_metrics();

        record_estimation_error(100.0, 50.0); // ratio = 2.0, bucket [2.0, 5.0)
        record_estimation_error(10.0, 100.0); // ratio = 0.1, bucket [0, 0.5)
        record_estimation_error(50.0, 50.0); // ratio = 1.0, bucket [1.0, 2.0)

        let snap = cost_metrics_snapshot();
        assert_eq!(snap.error_ratio_buckets[0], 1); // [0, 0.5)
        assert_eq!(snap.error_ratio_buckets[2], 1); // [1.0, 2.0)
        assert_eq!(snap.error_ratio_buckets[3], 1); // [2.0, 5.0)
        assert!(snap.error_ratio_mean.is_finite());
    }

    #[test]
    fn test_asymmetric_loss_underestimate_penalized_more() {
        // Underestimate: actual 200, estimated 100 → ratio 2.0
        let loss_under = asymmetric_estimation_loss(100.0, 200.0);
        // Overestimate: actual 50, estimated 100 → ratio 0.5
        let loss_over = asymmetric_estimation_loss(100.0, 50.0);

        // Underestimation should have higher loss.
        assert!(
            loss_under > loss_over,
            "underestimate loss ({loss_under}) should exceed overestimate loss ({loss_over})"
        );
    }

    #[test]
    fn test_asymmetric_loss_perfect_estimate() {
        let loss = asymmetric_estimation_loss(100.0, 100.0);
        assert!((loss - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_asymmetric_loss_degenerate() {
        // Zero estimated cost → loss = actual.
        let loss = asymmetric_estimation_loss(0.0, 50.0);
        assert!((loss - 50.0).abs() < 1e-10);
    }

    // ── DPccp tests (bd-1as.3) ──

    #[test]
    fn test_dpccp_two_tables() {
        let tables = vec![
            TableStats {
                name: "a".to_owned(),
                n_pages: 10,
                n_rows: 100,
                source: StatsSource::Heuristic,
            },
            TableStats {
                name: "b".to_owned(),
                n_pages: 20,
                n_rows: 200,
                source: StatsSource::Heuristic,
            },
        ];
        let indexes = vec![];
        let where_terms = vec![];

        let (order, cost, plans, _pruned) =
            dpccp_order_joins(&tables, &indexes, &where_terms, None, None, &[], None)
                .expect("2-table exhaustive plan should exist");
        assert_eq!(order.len(), 2);
        assert!(cost > 0.0);
        assert!(plans >= 2); // At least 2 seed + extensions.
    }

    #[test]
    fn test_dpccp_three_tables() {
        let tables = vec![
            TableStats {
                name: "x".to_owned(),
                n_pages: 5,
                n_rows: 50,
                source: StatsSource::Heuristic,
            },
            TableStats {
                name: "y".to_owned(),
                n_pages: 100,
                n_rows: 1000,
                source: StatsSource::Heuristic,
            },
            TableStats {
                name: "z".to_owned(),
                n_pages: 10,
                n_rows: 100,
                source: StatsSource::Heuristic,
            },
        ];
        let indexes = vec![];
        let where_terms = vec![];

        let (order, cost, plans, _pruned) =
            dpccp_order_joins(&tables, &indexes, &where_terms, None, None, &[], None)
                .expect("3-table exhaustive plan should exist");
        assert_eq!(order.len(), 3);
        assert!(cost > 0.0);
        assert!(plans > 3); // More than just seed.
        // Small table should be chosen first (lower cost).
        assert_eq!(order[0], 0); // "x" has fewest pages.
    }

    #[test]
    fn test_dpccp_respects_cross_join_constraint() {
        let tables = vec![
            TableStats {
                name: "t1".to_owned(),
                n_pages: 100,
                n_rows: 10_000,
                source: StatsSource::Heuristic,
            },
            TableStats {
                name: "t2".to_owned(),
                n_pages: 1,
                n_rows: 10,
                source: StatsSource::Heuristic,
            },
        ];

        let (order, _cost, _plans, _pruned) = dpccp_order_joins(
            &tables,
            &[],
            &[],
            None,
            None,
            &[("t1".to_owned(), "t2".to_owned())],
            None,
        )
        .expect("cross-join constrained exhaustive plan should exist");

        assert_eq!(order, vec![0, 1], "CROSS JOIN should force t1 before t2");
    }

    #[test]
    fn test_order_joins_five_tables_uses_exhaustive_search() {
        reset_plans_enumerated();
        let tables = (0..5)
            .map(|i| TableStats {
                name: format!("t{i}"),
                n_pages: 10,
                n_rows: 100,
                source: StatsSource::Heuristic,
            })
            .collect::<Vec<_>>();

        let plan = order_joins(&tables, &[], &[], None, &[]);
        assert_eq!(plan.join_order.len(), 5);

        let enumerated = plans_enumerated_total();
        assert!(
            enumerated > 120,
            "5-table exhaustive search should enumerate well beyond greedy-width bounds, got {enumerated}"
        );
    }

    #[test]
    fn test_dpccp_branch_and_bound_prunes_high_cost_branches() {
        let tables = vec![
            TableStats {
                name: "tiny".to_owned(),
                n_pages: 1,
                n_rows: 1,
                source: StatsSource::Heuristic,
            },
            TableStats {
                name: "small".to_owned(),
                n_pages: 2,
                n_rows: 2,
                source: StatsSource::Heuristic,
            },
            TableStats {
                name: "huge_a".to_owned(),
                n_pages: 10_000,
                n_rows: 10_000,
                source: StatsSource::Heuristic,
            },
            TableStats {
                name: "huge_b".to_owned(),
                n_pages: 20_000,
                n_rows: 20_000,
                source: StatsSource::Heuristic,
            },
            TableStats {
                name: "huge_c".to_owned(),
                n_pages: 30_000,
                n_rows: 30_000,
                source: StatsSource::Heuristic,
            },
        ];

        let (_order, _cost, _plans, pruned) =
            dpccp_order_joins(&tables, &[], &[], None, None, &[], None)
                .expect("5-table exhaustive plan should exist");

        assert!(pruned > 0, "expected branch-and-bound pruning to occur");
    }

    #[test]
    fn test_order_joins_large_join_uses_greedy_width() {
        reset_plans_enumerated();
        let tables = (0..10)
            .map(|i| TableStats {
                name: format!("t{i}"),
                n_pages: (i as u64 + 1) * 10,
                n_rows: (i as u64 + 1) * 100,
                source: StatsSource::Heuristic,
            })
            .collect::<Vec<_>>();

        let plan = order_joins(&tables, &[], &[], None, &[]);
        assert_eq!(plan.join_order.len(), 10);

        let enumerated = plans_enumerated_total();
        assert!(
            enumerated <= 800,
            "greedy-width search should keep enumeration bounded for 10-table joins, got {enumerated}"
        );
    }

    #[test]
    fn test_plans_enumerated_metric() {
        reset_plans_enumerated();
        let before = plans_enumerated_total();

        let tables = vec![
            TableStats {
                name: "t1".to_owned(),
                n_pages: 10,
                n_rows: 100,
                source: StatsSource::Heuristic,
            },
            TableStats {
                name: "t2".to_owned(),
                n_pages: 20,
                n_rows: 200,
                source: StatsSource::Heuristic,
            },
        ];
        let _ = order_joins(&tables, &[], &[], None, &[]);

        let after = plans_enumerated_total();
        assert!(after > before);
    }

    // ── Predicate pushdown tests (bd-1as.3) ──

    #[test]
    fn test_pushdown_qualified_predicate() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column(
                ColumnRef {
                    table: Some("users".to_owned()),
                    column: "id".to_owned(),
                },
                Span::ZERO,
            )),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            span: Span::ZERO,
        };
        let term = classify_where_term(&expr);
        let terms = [term];
        let table_names = vec!["users".to_owned(), "orders".to_owned()];

        let (pushed, remaining) = pushdown_predicates(&terms, &table_names);
        assert_eq!(pushed.len(), 1);
        assert_eq!(pushed[0].table, "users");
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_pushdown_single_table_unqualified() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column(
                ColumnRef {
                    table: None,
                    column: "id".to_owned(),
                },
                Span::ZERO,
            )),
            op: AstBinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Integer(10), Span::ZERO)),
            span: Span::ZERO,
        };
        let term = classify_where_term(&expr);
        let terms = [term];
        let table_names = vec!["users".to_owned()];

        let (pushed, remaining) = pushdown_predicates(&terms, &table_names);
        assert_eq!(pushed.len(), 1);
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_pushdown_unqualified_multi_table_stays() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column(
                ColumnRef {
                    table: None,
                    column: "id".to_owned(),
                },
                Span::ZERO,
            )),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            span: Span::ZERO,
        };
        let term = classify_where_term(&expr);
        let terms = [term];
        let table_names = vec!["users".to_owned(), "orders".to_owned()];

        let (pushed, remaining) = pushdown_predicates(&terms, &table_names);
        // Unqualified with multiple tables → stays as join predicate.
        assert!(pushed.is_empty());
        assert_eq!(remaining.len(), 1);
    }

    // ── Constant folding tests (bd-1as.3) ──

    #[test]
    fn test_fold_literal() {
        let expr = Expr::Literal(Literal::Integer(42), Span::ZERO);
        assert_eq!(
            try_constant_fold(&expr),
            FoldResult::Literal(Literal::Integer(42))
        );
    }

    #[test]
    fn test_fold_addition() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(Literal::Integer(10), Span::ZERO)),
            op: fsqlite_ast::BinaryOp::Add,
            right: Box::new(Expr::Literal(Literal::Integer(32), Span::ZERO)),
            span: Span::ZERO,
        };
        assert_eq!(
            try_constant_fold(&expr),
            FoldResult::Literal(Literal::Integer(42))
        );
    }

    #[test]
    fn test_fold_division_by_zero() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(Literal::Integer(10), Span::ZERO)),
            op: fsqlite_ast::BinaryOp::Divide,
            right: Box::new(Expr::Literal(Literal::Integer(0), Span::ZERO)),
            span: Span::ZERO,
        };
        assert_eq!(try_constant_fold(&expr), FoldResult::Literal(Literal::Null));
    }

    #[test]
    fn test_fold_negation() {
        let expr = Expr::UnaryOp {
            op: fsqlite_ast::UnaryOp::Negate,
            expr: Box::new(Expr::Literal(Literal::Integer(5), Span::ZERO)),
            span: Span::ZERO,
        };
        assert_eq!(
            try_constant_fold(&expr),
            FoldResult::Literal(Literal::Integer(-5))
        );
    }

    #[test]
    fn test_fold_column_ref_not_constant() {
        let expr = Expr::Column(
            ColumnRef {
                table: None,
                column: "id".to_owned(),
            },
            Span::ZERO,
        );
        assert_eq!(try_constant_fold(&expr), FoldResult::NotConstant);
    }

    #[test]
    fn test_fold_comparison() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(Literal::Integer(10), Span::ZERO)),
            op: fsqlite_ast::BinaryOp::Lt,
            right: Box::new(Expr::Literal(Literal::Integer(20), Span::ZERO)),
            span: Span::ZERO,
        };
        assert_eq!(try_constant_fold(&expr), FoldResult::Literal(Literal::True));
    }

    #[test]
    fn test_fold_nested_expression() {
        // (3 + 4) * 6 = 42
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Literal(Literal::Integer(3), Span::ZERO)),
                op: fsqlite_ast::BinaryOp::Add,
                right: Box::new(Expr::Literal(Literal::Integer(4), Span::ZERO)),
                span: Span::ZERO,
            }),
            op: fsqlite_ast::BinaryOp::Multiply,
            right: Box::new(Expr::Literal(Literal::Integer(6), Span::ZERO)),
            span: Span::ZERO,
        };
        assert_eq!(
            try_constant_fold(&expr),
            FoldResult::Literal(Literal::Integer(42))
        );
    }
}
