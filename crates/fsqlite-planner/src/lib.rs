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
pub mod differential;
pub mod stats;

use decision_contract::access_path_kind_label;
use fsqlite_ast::{
    BinaryOp as AstBinaryOp, ColumnRef, CompoundOp, Expr, FromClause, InSet, IndexHint,
    JoinConstraint, JoinKind, LikeOp, Literal, NullsOrder, OrderingTerm, ResultColumn, SelectBody,
    SelectCore, SortDirection, Span, TableOrSubquery,
};
use fsqlite_types::{SqliteValue, sync_primitives::Instant};
use lru::LruCache;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use xxhash_rust::xxh3::xxh3_64_with_seed;

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
                } => Some(col_ref.column.to_string()),
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
    resolve_single_table_result_columns_with_options(core, table_columns, true)
}

/// Resolve result columns for a single-table SELECT with explicit control over
/// whether hidden rowid aliases (`rowid`, `_rowid_`, `oid`) are available.
///
/// `WITHOUT ROWID` tables should pass `supports_hidden_rowid = false` so hidden
/// aliases are rejected unless a visible column of the same name exists.
pub fn resolve_single_table_result_columns_with_options(
    core: &SelectCore,
    table_columns: &[String],
    supports_hidden_rowid: bool,
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
                if !qualifier_matches_table(&qualifier.name, table_name, table_alias) {
                    return Err(SingleTableProjectionError::UnknownTableQualifier {
                        qualifier: qualifier.to_string(),
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
                if let Some(qualifier) = &col_ref.table
                    && !qualifier_matches_table(qualifier, table_name, table_alias)
                {
                    return Err(SingleTableProjectionError::UnknownTableQualifier {
                        qualifier: qualifier.to_string(),
                    });
                }
                if !(column_exists_ignore_case(table_columns, &col_ref.column)
                    || supports_hidden_rowid && is_rowid_alias_name(&col_ref.column))
                {
                    return Err(SingleTableProjectionError::ColumnNotFound {
                        column: col_ref.column.to_string(),
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

/// Match canonical spellings through `str` equality before paying for ASCII folding.
#[inline]
fn identifier_eq(left: &str, right: &str) -> bool {
    left == right || left.eq_ignore_ascii_case(right)
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
                    if let Some(alias) = alias_opt
                        && alias.eq_ignore_ascii_case(name)
                    {
                        return Ok((pos, None));
                    }
                }
            }
            Err(CompoundOrderByError::ColumnNotFound {
                name: name.to_string(),
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

/// Schema hint that a visible table column is an alias for SQLite's hidden
/// rowid, as with `INTEGER PRIMARY KEY`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowidAliasHint {
    /// Optional table name or query alias qualifier accepted for the column.
    pub qualifier: Option<String>,
    /// Visible column name that aliases the rowid.
    pub column: String,
}

impl RowidAliasHint {
    /// Build an unqualified rowid-alias hint for a table-local column.
    #[must_use]
    pub fn new(column: impl Into<String>) -> Self {
        Self {
            qualifier: None,
            column: column.into(),
        }
    }

    /// Build a rowid-alias hint for a specific table name or query alias.
    #[must_use]
    pub fn qualified(qualifier: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            qualifier: Some(qualifier.into()),
            column: column.into(),
        }
    }

    fn matches_column(&self, table_name: &str, column: &WhereColumn) -> bool {
        if !column.column.eq_ignore_ascii_case(&self.column) {
            return false;
        }

        match (column.table.as_deref(), self.qualifier.as_deref()) {
            (None, _) => true,
            (Some(column_qualifier), Some(hint_qualifier)) => {
                column_qualifier.eq_ignore_ascii_case(hint_qualifier)
            }
            (Some(column_qualifier), None) => column_qualifier.eq_ignore_ascii_case(table_name),
        }
    }
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

/// Probe expressions extracted from the WHERE clause during access-path
/// selection.  Carried forward so downstream consumers (connection seam, VDBE
/// codegen) do not re-extract from the AST.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::derive_partial_eq_without_eq)]
pub enum AccessPathProbe {
    /// `WHERE rowid = <target>`
    RowidEquality { target: Box<Expr> },
    /// `WHERE <column> = <target>` backed by an index.
    Equality { column: String, target: Box<Expr> },
    /// `WHERE <column> {>|>=} <lo> AND <column> {<|<=} <hi>` backed by an index.
    Range {
        column: String,
        lower: Option<(Box<Expr>, bool)>,
        upper: Option<(Box<Expr>, bool)>,
    },
    /// `WHERE <column> IN (<v1>, <v2>, ...)` backed by an index — one seek per
    /// value.
    InList {
        column: String,
        values: Vec<Box<Expr>>,
    },
}

/// A concrete access path chosen by the planner.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::derive_partial_eq_without_eq)]
pub struct AccessPath {
    /// Table being accessed.
    pub table: String,
    /// Kind of scan.
    pub kind: AccessPathKind,
    /// Index used (None for full table scan / rowid lookup / rowid range).
    pub index: Option<String>,
    /// Estimated cost in page reads.
    pub estimated_cost: f64,
    /// Estimated rows returned.
    pub estimated_rows: f64,
    /// Time-travel clause (SQL:2011 temporal query) — `FOR SYSTEM_TIME AS OF ...`.
    pub time_travel: Option<fsqlite_ast::TimeTravelClause>,
    /// Probe expressions extracted during path selection — avoids downstream
    /// re-extraction from the WHERE clause.
    pub probe: Option<AccessPathProbe>,
}

/// Morsel-parallel SELECT eligibility decision produced by the planner.
///
/// When `eligible` is true the executor may split the driving table scan
/// into `morsel_count` page-range morsels and process them in parallel
/// under separate snapshot-consistent cursors, merging results afterward.
#[derive(Debug, Clone, PartialEq)]
pub struct MorselEligibility {
    pub eligible: bool,
    pub driving_table: Option<String>,
    pub estimated_rows: f64,
    pub morsel_count: u16,
    pub rows_per_morsel: u64,
    pub reason: MorselIneligibleReason,
}

/// Why a query was deemed ineligible for morsel-parallel execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MorselIneligibleReason {
    None,
    TooFewRows,
    NoFullTableScan,
    MultiTableJoin,
    HasLimit,
    CompoundQuery,
}

impl MorselEligibility {
    const MIN_ROWS_FOR_MORSEL: f64 = 4096.0;
    const DEFAULT_MORSEL_TARGET_ROWS: u64 = 1024;
    const MAX_MORSELS: u16 = 64;

    fn ineligible(reason: MorselIneligibleReason) -> Self {
        Self {
            eligible: false,
            driving_table: None,
            estimated_rows: 0.0,
            morsel_count: 1,
            rows_per_morsel: 0,
            reason,
        }
    }

    /// Evaluate morsel eligibility for a single-table full-scan query.
    #[must_use]
    pub fn evaluate(
        plan: &QueryPlan,
        has_limit: bool,
        is_compound: bool,
        available_workers: u16,
    ) -> Self {
        if is_compound {
            return Self::ineligible(MorselIneligibleReason::CompoundQuery);
        }
        if has_limit {
            return Self::ineligible(MorselIneligibleReason::HasLimit);
        }
        if plan.join_order.len() != 1 {
            return Self::ineligible(MorselIneligibleReason::MultiTableJoin);
        }
        let path = match plan.access_paths.first() {
            Some(p) => p,
            None => return Self::ineligible(MorselIneligibleReason::NoFullTableScan),
        };
        if !matches!(path.kind, AccessPathKind::FullTableScan) {
            return Self::ineligible(MorselIneligibleReason::NoFullTableScan);
        }
        if path.estimated_rows < Self::MIN_ROWS_FOR_MORSEL {
            return Self::ineligible(MorselIneligibleReason::TooFewRows);
        }

        let est_rows = path.estimated_rows as u64;
        let workers = u64::from(available_workers.clamp(1, Self::MAX_MORSELS));
        let rows_per_morsel = (est_rows / workers).max(Self::DEFAULT_MORSEL_TARGET_ROWS);
        let morsel_count =
            u16::try_from((est_rows / rows_per_morsel).max(1)).unwrap_or(Self::MAX_MORSELS);

        Self {
            eligible: true,
            driving_table: Some(path.table.clone()),
            estimated_rows: path.estimated_rows,
            morsel_count,
            rows_per_morsel,
            reason: MorselIneligibleReason::None,
        }
    }
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
    /// Morsel-parallel SELECT eligibility (populated after planning).
    pub morsel_eligibility: Option<MorselEligibility>,
}

/// Default number of cached query plans retained by [`QueryPlanner`].
pub const DEFAULT_PLAN_CACHE_CAPACITY: usize = 128;

/// Stateful planner wrapper that memoizes query plans by SQL template and schema cookie.
///
/// The caller is responsible for supplying a stable SQL template string for the
/// query shape being planned. Literal normalization, placeholder canonicalization,
/// and any higher-level SQL parsing remain above this crate's current scope.
#[derive(Debug)]
pub struct QueryPlanner {
    plan_cache: LruCache<u64, Rc<QueryPlan>>,
    cached_schema_cookie: Option<u32>,
    hot_plan_cache_key: Option<u64>,
    hot_plan_cache_plan: Option<Rc<QueryPlan>>,
    hot_plan_cache_needs_lru_touch: bool,
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryPlanner {
    /// Construct a planner with the default 128-entry LRU plan cache.
    #[must_use]
    pub fn new() -> Self {
        Self::with_plan_cache_capacity(DEFAULT_PLAN_CACHE_CAPACITY)
    }

    /// Construct a planner with a caller-provided cache capacity.
    ///
    /// A zero capacity is clamped to 1 so callers can tune the cache without
    /// dealing with `NonZeroUsize`.
    #[must_use]
    pub fn with_plan_cache_capacity(capacity: usize) -> Self {
        Self {
            plan_cache: LruCache::new(normalize_plan_cache_capacity(capacity)),
            cached_schema_cookie: None,
            hot_plan_cache_key: None,
            hot_plan_cache_plan: None,
            hot_plan_cache_needs_lru_touch: false,
        }
    }

    /// Return the number of cached plans currently retained.
    #[must_use]
    pub fn plan_cache_len(&self) -> usize {
        self.plan_cache.len()
    }

    /// Return `true` when no cached plans are currently retained.
    #[must_use]
    pub fn is_plan_cache_empty(&self) -> bool {
        self.plan_cache.is_empty()
    }

    /// Clear all cached plans and forget the schema cookie they were built under.
    pub fn clear_plan_cache(&mut self) {
        self.plan_cache.clear();
        self.cached_schema_cookie = None;
        self.clear_hot_plan_cache();
    }

    /// Return a cached plan for the given SQL template and schema cookie, or compute one.
    ///
    /// When the schema cookie changes, the entire cache is flushed because any
    /// DDL may invalidate earlier planning decisions.
    #[must_use]
    pub fn cached_plan<F>(
        &mut self,
        sql_template: &str,
        schema_cookie: u32,
        build: F,
    ) -> Rc<QueryPlan>
    where
        F: FnOnce() -> QueryPlan,
    {
        self.invalidate_plan_cache_if_schema_cookie_changed(schema_cookie);
        let key = plan_cache_key(sql_template, schema_cookie);
        self.prepare_plan_cache_lookup(key);

        if let Some(plan) = self.lookup_hot_plan_cache(key) {
            return plan;
        }

        if let Some(plan) = self.plan_cache.get(&key).map(Rc::clone) {
            return self.record_plan_cache_hit(key, plan);
        }

        let plan = Rc::new(build());
        self.plan_cache.put(key, Rc::clone(&plan));
        self.record_plan_cache_hit(key, plan)
    }

    /// Cached wrapper around [`order_joins_with_hints_and_features`].
    ///
    /// This preserves the current stateless free-function API while exposing a
    /// planner-local cache for repeated SELECT templates.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn order_joins_with_cache(
        &mut self,
        sql_template: &str,
        schema_cookie: u32,
        tables: &[TableStats],
        indexes: &[IndexInfo],
        where_terms: &[WhereTerm<'_>],
        needed_columns: Option<&[String]>,
        cross_join_pairs: &[(String, String)],
        table_index_hints: Option<&BTreeMap<String, IndexHint>>,
        cracking_hints: Option<&mut CrackingHintStore>,
        feature_flags: PlannerFeatureFlags,
    ) -> Rc<QueryPlan> {
        // Adaptive cracking hints are mutable runtime state, not schema state.
        // They can legitimately change the preferred index for the same SQL
        // template, so they must not be served from the stable plan cache.
        if cracking_hints.is_some() {
            return Rc::new(order_joins_with_hints_and_features(
                tables,
                indexes,
                where_terms,
                needed_columns,
                cross_join_pairs,
                table_index_hints,
                cracking_hints,
                feature_flags,
            ));
        }

        self.invalidate_plan_cache_if_schema_cookie_changed(schema_cookie);
        let key = plan_cache_key_with_feature_flags(sql_template, schema_cookie, feature_flags);
        self.prepare_plan_cache_lookup(key);

        if let Some(plan) = self.lookup_hot_plan_cache(key) {
            return plan;
        }

        if let Some(plan) = self.plan_cache.get(&key).map(Rc::clone) {
            return self.record_plan_cache_hit(key, plan);
        }

        let plan = Rc::new(order_joins_with_hints_and_features(
            tables,
            indexes,
            where_terms,
            needed_columns,
            cross_join_pairs,
            table_index_hints,
            cracking_hints,
            feature_flags,
        ));
        self.plan_cache.put(key, Rc::clone(&plan));
        self.record_plan_cache_hit(key, plan)
    }

    fn invalidate_plan_cache_if_schema_cookie_changed(&mut self, schema_cookie: u32) {
        if self
            .cached_schema_cookie
            .is_some_and(|cached| cached != schema_cookie)
        {
            self.plan_cache.clear();
            self.clear_hot_plan_cache();
        }
        self.cached_schema_cookie = Some(schema_cookie);
    }

    fn prepare_plan_cache_lookup(&mut self, key: u64) {
        if self
            .hot_plan_cache_key
            .is_some_and(|hot_key| hot_key != key)
        {
            self.flush_hot_plan_cache_lru_touch();
            self.clear_hot_plan_cache();
        }
    }

    fn lookup_hot_plan_cache(&mut self, key: u64) -> Option<Rc<QueryPlan>> {
        if self.hot_plan_cache_key == Some(key) {
            self.hot_plan_cache_needs_lru_touch = true;
            return self.hot_plan_cache_plan.as_ref().map(Rc::clone);
        }
        None
    }

    fn record_plan_cache_hit(&mut self, key: u64, plan: Rc<QueryPlan>) -> Rc<QueryPlan> {
        self.hot_plan_cache_key = Some(key);
        self.hot_plan_cache_plan = Some(Rc::clone(&plan));
        self.hot_plan_cache_needs_lru_touch = false;
        plan
    }

    fn flush_hot_plan_cache_lru_touch(&mut self) {
        if !self.hot_plan_cache_needs_lru_touch {
            return;
        }
        if let Some(key) = self.hot_plan_cache_key {
            let _ = self.plan_cache.get(&key);
        }
        self.hot_plan_cache_needs_lru_touch = false;
    }

    fn clear_hot_plan_cache(&mut self) {
        self.hot_plan_cache_key = None;
        self.hot_plan_cache_plan = None;
        self.hot_plan_cache_needs_lru_touch = false;
    }
}

fn normalize_plan_cache_capacity(capacity: usize) -> NonZeroUsize {
    let normalized = capacity.max(1);
    if let Some(capacity) = NonZeroUsize::new(normalized) {
        capacity
    } else {
        unreachable!("cache capacity is clamped to a non-zero value");
    }
}

const PLAN_CACHE_DIRECT_SEED_TAG: u64 = 0x5A00_0000_0000_0000;
const PLAN_CACHE_JOIN_SEED_TAG: u64 = 0xA500_0000_0000_0000;
const PLAN_CACHE_FEATURE_LEAPFROG: u64 = 1_u64 << 32;
const PLAN_CACHE_FEATURE_DPCCP: u64 = 1_u64 << 33;

fn plan_cache_key(sql_template: &str, schema_cookie: u32) -> u64 {
    xxh3_64_with_seed(
        sql_template.as_bytes(),
        PLAN_CACHE_DIRECT_SEED_TAG | u64::from(schema_cookie),
    )
}

fn plan_cache_key_with_feature_flags(
    sql_template: &str,
    schema_cookie: u32,
    feature_flags: PlannerFeatureFlags,
) -> u64 {
    // Keep the schema cookie in the low 32 bits and pack feature toggles above
    // it so each plan-cache variant gets a distinct seed without heap work.
    // The high tag separates this join-order cache from the generic
    // `cached_plan()` API; both APIs share `QueryPlanner::plan_cache`.
    let feature_mask = if feature_flags.leapfrog_join {
        PLAN_CACHE_FEATURE_LEAPFROG
    } else {
        0
    } | if feature_flags.dpccp_join {
        PLAN_CACHE_FEATURE_DPCCP
    } else {
        0
    };
    xxh3_64_with_seed(
        sql_template.as_bytes(),
        PLAN_CACHE_JOIN_SEED_TAG | u64::from(schema_cookie) | feature_mask,
    )
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
///
/// This is the legacy entry point that ignores row-count statistics; it is a
/// thin wrapper around [`estimate_cost_ext`] with `n_rows = 0` (i.e. no row
/// statistics available). When `sqlite_stat1` data has been loaded, prefer
/// [`estimate_cost_ext`] so per-row decode/access costs participate in the
/// score.
#[must_use]
pub fn estimate_cost(kind: &AccessPathKind, table_pages: u64, index_pages: u64) -> f64 {
    estimate_cost_ext(kind, table_pages, index_pages, 0)
}

/// Per-row cost added on top of page-level cost for a full table scan.
///
/// Reflects the VDBE/record-decode overhead per emitted row, tuned to keep
/// the scan cost of a tiny page-count table proportional to its row count
/// so that ANALYZE-populated stats change the plan meaningfully.
const ROW_DECODE_COST: f64 = 0.01;

/// Per-row cost added to each table visit from an indexed access path
/// (one rowid dereference + row decode).
const ROW_ACCESS_COST: f64 = 0.02;

/// Estimate the cost (in page reads) for a given access path, optionally
/// incorporating the table row count (PLANNER-2).
///
/// When `n_rows == 0`, this is equivalent to the legacy [`estimate_cost`]
/// formulas and the cost is computed purely from page counts. When `n_rows`
/// is available (e.g. from `sqlite_stat1` after `ANALYZE`), per-row terms are
/// added so that two tables with the same page count but wildly different row
/// counts are ranked differently:
///
/// - Full table scan: `tbl_pages + n_rows * ROW_DECODE_COST`
/// - Index equality / range / covering / rowid: the legacy page-level cost
///   plus `selectivity * n_rows * ROW_ACCESS_COST` (for equality we use
///   `1 / max(1, n_rows)` as the selectivity floor; rowid lookups yield
///   exactly one row).
#[must_use]
pub fn estimate_cost_ext(
    kind: &AccessPathKind,
    table_pages: u64,
    index_pages: u64,
    n_rows: u64,
) -> f64 {
    let tp = table_pages.max(1) as f64;
    let ip = index_pages.max(1) as f64;
    let nr = n_rows as f64;

    let cost = match kind {
        AccessPathKind::FullTableScan => nr.mul_add(ROW_DECODE_COST, tp),
        AccessPathKind::IndexScanRange { selectivity } => {
            let page_cost = ip.log2() + selectivity * ip + selectivity * tp;
            (selectivity * nr).mul_add(ROW_ACCESS_COST, page_cost)
        }
        AccessPathKind::IndexScanEquality => {
            // Equality: selectivity ≈ 1 / n_rows (unique) or floor at 1 row.
            let page_cost = ip.log2() + tp.log2();
            let matched_rows: f64 = if nr > 0.0 { 1.0 } else { 0.0 };
            matched_rows.mul_add(ROW_ACCESS_COST, page_cost)
        }
        AccessPathKind::CoveringIndexScan { selectivity } => {
            let page_cost = ip.log2() + selectivity * ip;
            // Covering scan still pays per-row decode but avoids the table
            // dereference, so use ROW_DECODE_COST (cheaper than ROW_ACCESS).
            (selectivity * nr).mul_add(ROW_DECODE_COST, page_cost)
        }
        AccessPathKind::RowidLookup => {
            let page_cost = tp.log2();
            let matched_rows: f64 = if nr > 0.0 { 1.0 } else { 0.0 };
            matched_rows.mul_add(ROW_ACCESS_COST, page_cost)
        }
    };

    FSQLITE_PLANNER_COST_ESTIMATES_TOTAL.fetch_add(1, Ordering::Relaxed);

    tracing::debug!(
        target: "fsqlite.planner",
        table_pages,
        index_pages,
        n_rows,
        estimated_cost = cost,
        actual_method = %access_path_metric_label(kind),
        "cost_estimate"
    );

    cost
}

// ---------------------------------------------------------------------------
// PLANNER-3: join ordering with sqlite_stat1 row-count hints
// ---------------------------------------------------------------------------

/// A table reference paired with cost-model inputs for join ordering.
///
/// Used by [`order_join_inputs_with_hints`] to decide the evaluation order of a
/// multi-table FROM clause. The `has_stats` flag lets the caller distinguish
/// ANALYZE-populated inputs from pure heuristic fallbacks: when every
/// reference is marked `has_stats == false`, callers should preserve the
/// source order (there is nothing to optimize on).
///
/// The struct is intentionally minimal so it can be constructed directly by
/// `crates/fsqlite-core/src/connection.rs` without pulling in the full
/// bound-statement type surface. Wire-up from connection.rs is staged
/// separately (see PLANNER-3 follow-up); for now this lives in the planner
/// and is exercised via unit tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRefWithStats {
    /// Table name (used only for diagnostics / test assertions).
    pub name: String,
    /// Estimated B-tree pages — passed to [`estimate_cost_ext`].
    pub n_pages: u64,
    /// Estimated row count — passed to [`estimate_cost_ext`]. `0` means
    /// "no row-count hint available"; see [`Self::has_stats`].
    pub n_rows: u64,
    /// Whether this input was populated from `sqlite_stat1` / ANALYZE.
    /// When false, ordering callers should fall back to source order.
    pub has_stats: bool,
}

impl TableRefWithStats {
    /// Construct a `TableRefWithStats` from a [`TableStats`] snapshot.
    ///
    /// `has_stats` is derived from the stats source: only
    /// [`StatsSource::Analyze`] (populated from `sqlite_stat1`) counts as
    /// authoritative for join ordering.
    #[must_use]
    pub fn from_table_stats(stats: &TableStats) -> Self {
        Self {
            name: stats.name.clone(),
            n_pages: stats.n_pages,
            n_rows: stats.n_rows,
            has_stats: matches!(stats.source, StatsSource::Analyze),
        }
    }
}

/// Threshold above which [`order_join_inputs_with_hints`] falls back to a greedy
/// smallest-first heuristic instead of exhaustive permutation search.
///
/// For N ≤ 4 the permutation count is at most 24, which is trivially cheap.
/// Beyond that, `N!` grows quickly enough that greedy ordering (sorting by
/// full-scan cost) is a better trade for planning latency.
const JOIN_ORDER_EXHAUSTIVE_LIMIT: usize = 4;

/// Decide a join evaluation order for `tables` using per-table cost hints.
///
/// Returns a permutation `perm` such that `tables[perm[i]]` is the `i`-th
/// table to evaluate. The first entry is typically the smallest relation so
/// it can act as the hash-join build side while larger relations probe it.
///
/// Strategy:
/// - If **no** table has `has_stats == true`, the function returns the
///   identity permutation (source order). This preserves pre-PLANNER-3
///   behavior when ANALYZE has not been run.
/// - For `N <= JOIN_ORDER_EXHAUSTIVE_LIMIT` (4), try every permutation and
///   pick the one whose summed full-scan cost is minimal. For inner-equi
///   hash joins, minimizing the cost of probing smaller-first is a sound
///   approximation: the build side pays `n_pages + n_rows * DECODE` once
///   and the probe side scans the remaining relations, so sorting by
///   ascending cost is equivalent to picking the smallest build side.
/// - For `N > JOIN_ORDER_EXHAUSTIVE_LIMIT`, use greedy smallest-first
///   ordering (stable sort by per-table full-scan cost).
///
/// The permutation is stable: ties break on source order, so deterministic
/// replays stay reproducible.
///
/// # Safety / semantics
///
/// This function is *purely advisory*. It does **not** inspect join
/// predicates or join kinds. Callers that reorder LEFT/RIGHT/FULL OUTER
/// joins must verify the outer-preservation semantics are still correct
/// (typically: only reorder INNER joins). Wire-up in connection.rs will
/// gate the reorder behind an inner-join-only check.
///
/// # Wire-up plan for `connection.rs` (punted — PLANNER-3 follow-up)
///
/// `try_prepare_simple_join_rows` in `crates/fsqlite-core/src/connection.rs`
/// builds `table_sources`, `table_rows`, `join_plans`, `col_map`,
/// `projection_indices`, and `col_collations` in **source order**. A safe
/// wire would:
///
/// 1. Early-bail if any `join.join_type.kind != JoinKind::Inner`
///    (LEFT/RIGHT/FULL are non-commutative).
/// 2. After `table_sources` is populated, call
///    `order_join_inputs_with_hints` with a `TableRefWithStats` built from
///    `self.sqlite_stat1_row_counts()` (already exposed) plus table-page
///    estimates.
/// 3. If the returned permutation is non-identity, apply it to:
///    - `table_sources` (and the parallel `all_sources`)
///    - `col_map` (rebuild — it's derived from `table_sources`)
///    - `col_collations` (rebuild — ditto)
///    - `projection_indices` (remap via an old→new index table)
///    - The join-plan build loop (which currently consumes
///      `from.joins[i]` position-by-position): the equi-pair extraction
///      looks up columns via `col_map`, so once `col_map` is rebuilt the
///      same WHERE-style ON predicates still resolve, but `left_width`
///      must accumulate from the permuted `table_sources[..=i]`.
///
/// That last bullet — rebuilding the join-plan loop against a permuted
/// order while still consuming the AST's `from.joins` in source order — is
/// why this wire was punted from the initial PLANNER-3 commit. Landing it
/// requires either (a) refactoring the planner to carry an explicit join
/// tree instead of a flat source list, or (b) a careful single-site
/// rewrite with broad test coverage. Neither fit in the PLANNER-3 scope.
#[must_use]
pub fn order_join_inputs_with_hints(tables: &[TableRefWithStats]) -> Vec<usize> {
    let n = tables.len();
    if n <= 1 {
        return (0..n).collect();
    }

    // Fallback: no stats anywhere → preserve source order.
    if !tables.iter().any(|t| t.has_stats) {
        return (0..n).collect();
    }

    // Per-table full-scan cost: build-side picking minimizes this.
    let scan_cost = |idx: usize| -> f64 {
        let t = &tables[idx];
        estimate_cost_ext(&AccessPathKind::FullTableScan, t.n_pages, 0, t.n_rows)
    };

    if n <= JOIN_ORDER_EXHAUSTIVE_LIMIT {
        // Exhaustive: try every permutation, score by the sum of scan costs
        // weighted so that the first (build-side) table dominates. We sum
        // `cost[i] * (n - i)` — equivalent to "smaller cost first" but with
        // an explicit weighting that mirrors the left-deep probe chain.
        let indices: Vec<usize> = (0..n).collect();
        let mut best_perm = indices.clone();
        let mut best_score = f64::INFINITY;

        // Heap's-algorithm-style permutation over a small scratch buffer.
        // We don't need a crate — N is at most 4 here, so a recursive helper
        // is fine and still O(N!).
        let mut scratch = indices.clone();
        permute_scoring(
            &mut scratch,
            0,
            n,
            &scan_cost,
            &mut best_score,
            &mut best_perm,
        );
        best_perm
    } else {
        // Greedy: stable sort by ascending scan cost. Stable keeps ties in
        // source order, which matches the "no stats" fallback for the
        // equal-cost region.
        let mut indexed: Vec<(usize, f64)> = (0..n).map(|i| (i, scan_cost(i))).collect();
        // `sort_by` is stable in std.
        indexed.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        indexed.into_iter().map(|(i, _)| i).collect()
    }
}

/// Helper: enumerate every permutation of `slice`, scoring each and tracking
/// the cheapest. Uses Heap's algorithm in-place.
fn permute_scoring<F>(
    slice: &mut [usize],
    k: usize,
    n: usize,
    scan_cost: &F,
    best_score: &mut f64,
    best_perm: &mut Vec<usize>,
) where
    F: Fn(usize) -> f64,
{
    if k == n {
        // Score this permutation: sum of cost[slice[i]] * (n - i).
        // Lower = better.
        let mut score = 0.0_f64;
        for (i, &tbl_idx) in slice.iter().enumerate() {
            let weight = (n - i) as f64;
            score = scan_cost(tbl_idx).mul_add(weight, score);
        }
        if score < *best_score {
            *best_score = score;
            best_perm.clear();
            best_perm.extend_from_slice(slice);
        }
        return;
    }
    for i in k..n {
        slice.swap(k, i);
        permute_scoring(slice, k + 1, n, scan_cost, best_score, best_perm);
        slice.swap(k, i);
    }
}

const ADAPTIVE_HINT_COST_BIAS: f64 = 0.90;

struct AccessPathSelectionCounters {
    full_table_scan: AtomicU64,
    index_scan_range: AtomicU64,
    index_scan_equality: AtomicU64,
    covering_index_scan: AtomicU64,
    rowid_lookup: AtomicU64,
}

impl AccessPathSelectionCounters {
    const fn new() -> Self {
        Self {
            full_table_scan: AtomicU64::new(0),
            index_scan_range: AtomicU64::new(0),
            index_scan_equality: AtomicU64::new(0),
            covering_index_scan: AtomicU64::new(0),
            rowid_lookup: AtomicU64::new(0),
        }
    }

    fn counter_for(&self, kind: &AccessPathKind) -> &AtomicU64 {
        match kind {
            AccessPathKind::FullTableScan => &self.full_table_scan,
            AccessPathKind::IndexScanRange { .. } => &self.index_scan_range,
            AccessPathKind::IndexScanEquality => &self.index_scan_equality,
            AccessPathKind::CoveringIndexScan { .. } => &self.covering_index_scan,
            AccessPathKind::RowidLookup => &self.rowid_lookup,
        }
    }

    fn snapshot(&self) -> BTreeMap<String, u64> {
        [
            (
                "covering_index_scan",
                self.covering_index_scan.load(Ordering::Relaxed),
            ),
            (
                "full_table_scan",
                self.full_table_scan.load(Ordering::Relaxed),
            ),
            (
                "index_scan_equality",
                self.index_scan_equality.load(Ordering::Relaxed),
            ),
            (
                "index_scan_range",
                self.index_scan_range.load(Ordering::Relaxed),
            ),
            ("rowid_lookup", self.rowid_lookup.load(Ordering::Relaxed)),
        ]
        .into_iter()
        .map(|(label, count)| (label.to_owned(), count))
        .collect()
    }
}

static INDEX_SELECTION_TOTAL: AccessPathSelectionCounters = AccessPathSelectionCounters::new();

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
    INDEX_SELECTION_TOTAL
        .counter_for(kind)
        .fetch_add(1, Ordering::Relaxed)
        + 1
}

#[must_use]
pub fn snapshot_index_selection_totals() -> BTreeMap<String, u64> {
    INDEX_SELECTION_TOTAL.snapshot()
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
                ..
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

/// Build the cheapest [`AccessPath`] while recognizing schema-provided rowid
/// alias columns such as `id INTEGER PRIMARY KEY`.
///
/// Existing callers that do not have schema metadata should use
/// [`best_access_path`]. Schema-aware callers can pass table-local
/// [`RowidAliasHint`] values so predicates like `id = ?1` are costed as
/// [`AccessPathKind::RowidLookup`] without mutating the classified WHERE terms.
#[must_use]
pub fn best_access_path_with_rowid_alias_hints(
    table: &TableStats,
    indexes: &[IndexInfo],
    where_terms: &[WhereTerm<'_>],
    needed_columns: Option<&[String]>,
    rowid_alias_hints: &[RowidAliasHint],
) -> AccessPath {
    best_access_path_internal(
        table,
        indexes,
        where_terms,
        where_terms,
        needed_columns,
        None,
        None,
        rowid_alias_hints,
        true,
    )
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
        where_terms,
        needed_columns,
        index_hint,
        adaptive_preferred_index.as_deref(),
        &[],
        true,
    );

    if let Some(store) = cracking_hints {
        store.record_access_path(&best);
    }

    best
}

/// Build the cheapest [`AccessPath`] with optional explicit and adaptive hints.
#[must_use]
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn best_access_path_internal(
    table: &TableStats,
    indexes: &[IndexInfo],
    where_terms: &[WhereTerm<'_>],
    partial_index_terms: &[WhereTerm<'_>],
    needed_columns: Option<&[String]>,
    index_hint: Option<&IndexHint>,
    adaptive_preferred_index: Option<&str>,
    rowid_alias_hints: &[RowidAliasHint],
    unqualified_terms_are_table_local: bool,
) -> AccessPath {
    // Only pay the clock read when an INFO subscriber will consume the
    // `selection_elapsed_us` diagnostic below. The cost-estimation path is
    // otherwise allocation- and syscall-free on the per-compile hot loop.
    let started = tracing::enabled!(tracing::Level::INFO).then(Instant::now);
    let locally_evaluable_terms;
    let access_terms = if unqualified_terms_are_table_local
        && where_terms.iter().any(|term| {
            !table_local_access_path_probe_is_evaluable(term, &table.name, rowid_alias_hints)
        }) {
        // A table-local comparison such as `a = b` is a valid residual
        // predicate, but `b` cannot be evaluated until after reading the same
        // row. It must not become an index/rowid seek target. Keep it in
        // `partial_index_terms` for implication, while restricting access-path
        // selection and executable probe extraction to operands available
        // before this table scan starts.
        locally_evaluable_terms = where_terms
            .iter()
            .filter(|term| {
                table_local_access_path_probe_is_evaluable(term, &table.name, rowid_alias_hints)
            })
            .cloned()
            .collect::<Vec<_>>();
        &locally_evaluable_terms
    } else {
        // Multi-table callers have already applied order-aware binding in
        // `join_access_path`; an ordinary table-local term set reaches this
        // branch without allocating when every probe is already evaluable.
        where_terms
    };
    let explicit_indexed_by = match index_hint {
        Some(IndexHint::IndexedBy(index_name)) => Some(index_name.as_str()),
        _ => None,
    };
    let not_indexed = matches!(index_hint, Some(IndexHint::NotIndexed));
    let rowid_equality_candidate =
        find_rowid_equality_term(&table.name, access_terms, rowid_alias_hints).is_some();
    // The range branch below is only reached when the equality branch did not
    // match, so the range candidate is dead work in the common point-lookup
    // case — short-circuit it. When it is needed, probe with the
    // allocation-free matcher instead of `find_rowid_range_column`, which
    // clones the matched column name only to discard it for this boolean.
    // `where_term_matches_rowid_range` already requires a present column, so
    // `.any(..)` is equivalent to the previous `.is_some()`.
    let rowid_range_candidate = !rowid_equality_candidate
        && access_terms
            .iter()
            .any(|term| where_term_matches_rowid_range(&table.name, term, rowid_alias_hints));

    let mut best = if explicit_indexed_by.is_some() {
        AccessPath {
            table: table.name.clone(),
            kind: AccessPathKind::FullTableScan,
            index: None,
            estimated_cost: f64::INFINITY,
            estimated_rows: table.n_rows as f64,
            time_travel: None,
            probe: None,
        }
    } else if !not_indexed && rowid_equality_candidate {
        let kind = AccessPathKind::RowidLookup;
        AccessPath {
            table: table.name.clone(),
            estimated_cost: estimate_cost_ext(&kind, table.n_pages, 0, table.n_rows),
            kind,
            index: None,
            estimated_rows: 1.0,
            time_travel: None,
            probe: None,
        }
    } else if !not_indexed && rowid_range_candidate {
        let selectivity = DEFAULT_RANGE_SELECTIVITY;
        let kind = AccessPathKind::IndexScanRange { selectivity };
        AccessPath {
            table: table.name.clone(),
            estimated_cost: estimate_cost_ext(&kind, table.n_pages, 0, table.n_rows),
            kind,
            index: None,
            estimated_rows: (table.n_rows as f64 * selectivity).max(1.0),
            time_travel: None,
            probe: None,
        }
    } else {
        AccessPath {
            table: table.name.clone(),
            kind: AccessPathKind::FullTableScan,
            index: None,
            estimated_cost: estimate_cost_ext(
                &AccessPathKind::FullTableScan,
                table.n_pages,
                0,
                table.n_rows,
            ),
            estimated_rows: table.n_rows as f64,
            time_travel: None,
            probe: None,
        }
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
        if !identifier_eq(&idx.table, &table.name) {
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

        // Partial index gate: skip unless the query's WHERE conservatively
        // implies every conjunct in the index predicate. The proof accepts a
        // small explicit lattice of same-bound comparisons and non-NULL
        // guarantees; unknown affinity, collation, or function semantics fail
        // closed.
        if let Some(ref partial_pred) = idx.partial_where
            && !where_terms_imply_predicate(
                partial_index_terms,
                partial_pred,
                &idx.table,
                unqualified_terms_are_table_local,
            )
        {
            partial_indexes_pruned += 1;
            continue;
        }

        let mut skip_scan_candidate = None;
        let usability = match analyze_index_usability(idx, access_terms) {
            IndexUsability::NotUsable => {
                if let Some(candidate) = analyze_skip_scan_candidate(table, idx, access_terms) {
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
            needed.iter().all(|column| {
                idx.columns
                    .iter()
                    .any(|index_column| identifier_eq(index_column, column))
                    // Ordinary SQLite indexes carry the rowid payload, so
                    // rowid projections remain index-only even if the rowid
                    // alias is not listed in idx.columns.
                    || is_rowid_alias_name(column)
            })
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
                trailing_constraint,
            } => {
                // Multi-column equality narrows selectivity geometrically.
                // Each additional constrained column reduces rows by ~1/10.
                let equality_width = eq_columns
                    + usize::from(matches!(
                        trailing_constraint,
                        MultiColumnTrailingConstraint::InExpansion { .. }
                    ));
                #[allow(clippy::cast_precision_loss)]
                let per_probe_rows = if idx.unique
                    && equality_width == idx.columns.len()
                    && !matches!(
                        trailing_constraint,
                        MultiColumnTrailingConstraint::Range
                            | MultiColumnTrailingConstraint::LikePrefix
                    ) {
                    1.0
                } else {
                    let divisor = 10.0_f64.powi(i32::try_from(equality_width).unwrap_or(i32::MAX));
                    (table.n_rows as f64 / divisor).max(1.0)
                };
                let (rows, sel) = match trailing_constraint {
                    MultiColumnTrailingConstraint::Range => {
                        let range_factor = DEFAULT_RANGE_SELECTIVITY;
                        let r = (per_probe_rows * range_factor).max(1.0);
                        (
                            r,
                            range_factor * per_probe_rows / table.n_rows.max(1) as f64,
                        )
                    }
                    MultiColumnTrailingConstraint::LikePrefix => {
                        let range_factor = LIKE_PREFIX_SELECTIVITY;
                        let r = (per_probe_rows * range_factor).max(1.0);
                        (
                            r,
                            range_factor * per_probe_rows / table.n_rows.max(1) as f64,
                        )
                    }
                    MultiColumnTrailingConstraint::InExpansion { probe_count } => {
                        cost_multiplier = probe_count as f64;
                        let r =
                            (per_probe_rows * probe_count as f64).min(table.n_rows.max(1) as f64);
                        (r, r / table.n_rows.max(1) as f64)
                    }
                    MultiColumnTrailingConstraint::None => {
                        (per_probe_rows, per_probe_rows / table.n_rows.max(1) as f64)
                    }
                };
                if is_covering {
                    (AccessPathKind::CoveringIndexScan { selectivity: sel }, rows)
                } else if matches!(
                    trailing_constraint,
                    MultiColumnTrailingConstraint::Range
                        | MultiColumnTrailingConstraint::LikePrefix
                ) {
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
                let per_probe_rows: f64 = if idx.unique {
                    1.0
                } else {
                    (table.n_rows as f64 / 10.0).max(1.0)
                };
                let rows = per_probe_rows * probe_count as f64;
                cost_multiplier = probe_count as f64;
                (AccessPathKind::IndexScanEquality, rows)
            }
            IndexUsability::LikePrefix { .. } => {
                let selectivity = LIKE_PREFIX_SELECTIVITY;
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

        let mut cost =
            estimate_cost_ext(&kind, table.n_pages, idx.n_pages, table.n_rows) * cost_multiplier;

        if let Some(hinted_name) = explicit_indexed_by {
            if idx.name.eq_ignore_ascii_case(hinted_name) {
                // Respect explicit INDEXED BY by strongly preferring that index.
                cost *= 0.01;
                explicit_hint_applied = true;
            }
        } else if let Some(adaptive_hint) = adaptive_preferred_index
            && idx.name.eq_ignore_ascii_case(adaptive_hint)
        {
            cost *= ADAPTIVE_HINT_COST_BIAS;
            adaptive_hint_applied = true;
        }

        if cost < best.estimated_cost {
            best = AccessPath {
                table: table.name.clone(),
                kind,
                index: Some(idx.name.clone()),
                estimated_cost: cost,
                estimated_rows: est_rows,
                time_travel: None,
                probe: None,
            };
        }
    }

    if !best.estimated_cost.is_finite() {
        best = AccessPath {
            table: table.name.clone(),
            kind: AccessPathKind::FullTableScan,
            index: None,
            estimated_cost: estimate_cost_ext(
                &AccessPathKind::FullTableScan,
                table.n_pages,
                0,
                table.n_rows,
            ),
            estimated_rows: table.n_rows as f64,
            time_travel: None,
            probe: None,
        };
    }

    best.probe = extract_access_path_probe_with_rowid_aliases(
        &best,
        indexes,
        access_terms,
        rowid_alias_hints,
    );

    // The index-selection metric counter is a real always-on metric: it must
    // increment for every planning decision regardless of tracing config.
    let metric_total = increment_index_selection_total(&best.kind);

    // The structured `index_select` span/event below is the only consumer of
    // three `std::env::var` lookups (each a global env lock + heap String), a
    // `format!`/`to_owned` hint label, and the `Instant` clock read above.
    // None of that work is observable unless an INFO subscriber is listening,
    // so gate it behind a cheap level check. When INFO is enabled the emitted
    // diagnostics are identical to before.
    if tracing::enabled!(tracing::Level::INFO) {
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
        let selection_elapsed_us = started.map_or(1, |start| start.elapsed().as_micros().max(1));
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
    }

    best
}

/// Check if the WHERE terms collectively imply a partial index predicate.
///
/// This is intentionally stronger than plain structural equality for common
/// partial-index predicates. It accepts exact conjunct matches, commuted
/// comparisons with the same literal, same-bound operator implications, and
/// non-NULL comparisons implying `IS NOT NULL`. Distinct literal bounds are
/// deliberately not ordered here: without proven column affinity and collation,
/// their Rust ordering does not establish SQLite comparison implication.
fn where_terms_imply_predicate(
    terms: &[WhereTerm<'_>],
    predicate: &Expr,
    index_table: &str,
    unqualified_terms_are_table_local: bool,
) -> bool {
    let pred_conjuncts = decompose_where(predicate);
    pred_conjuncts.iter().all(|predicate_conjunct| {
        terms.iter().any(|term| {
            expr_implies_partial_predicate(
                term.expr,
                predicate_conjunct,
                index_table,
                unqualified_terms_are_table_local,
            )
        })
    })
}

fn expr_implies_partial_predicate(
    query_expr: &Expr,
    predicate: &Expr,
    index_table: &str,
    unqualified_terms_are_table_local: bool,
) -> bool {
    // Exact arbitrary predicates are a binding proof only after normalizing
    // columns within a proven table scope. Single-table callers have already
    // resolved bare columns to `index_table`; multi-table callers must spell
    // every column with that table's visible qualifier. The normalizer strips
    // only that qualifier, folds identifier case, and rejects nested scopes or
    // any foreign table before structural comparison.
    let query_is_bound_to_index_table = unqualified_terms_are_table_local
        || expr_uses_only_explicit_table_columns(query_expr, index_table);
    if query_is_bound_to_index_table
        && expression_matches_index_key(query_expr, predicate, index_table)
    {
        return true;
    }

    if let Some(predicate_column) = normalize_is_not_null_predicate(predicate) {
        return expr_guarantees_non_null(
            query_expr,
            &predicate_column,
            index_table,
            unqualified_terms_are_table_local,
        );
    }

    match (
        normalize_column_literal_comparison(query_expr),
        normalize_column_literal_comparison(predicate),
    ) {
        (Some(query_cmp), Some(predicate_cmp)) => query_cmp.implies(
            &predicate_cmp,
            index_table,
            unqualified_terms_are_table_local,
        ),
        _ => false,
    }
}

#[derive(Debug, Clone, PartialEq)]
struct NormalizedColumnComparison {
    column: WhereColumn,
    op: AstBinaryOp,
    literal: Literal,
}

impl NormalizedColumnComparison {
    fn implies(
        &self,
        predicate: &Self,
        index_table: &str,
        unqualified_terms_are_table_local: bool,
    ) -> bool {
        if !where_columns_match_partial_index(
            &self.column,
            &predicate.column,
            index_table,
            unqualified_terms_are_table_local,
        ) || self.literal != predicate.literal
        {
            return false;
        }

        match self.op {
            AstBinaryOp::Eq => matches!(
                predicate.op,
                AstBinaryOp::Eq | AstBinaryOp::Ge | AstBinaryOp::Le
            ),
            AstBinaryOp::Gt => {
                matches!(predicate.op, AstBinaryOp::Gt | AstBinaryOp::Ge)
            }
            AstBinaryOp::Ge => predicate.op == AstBinaryOp::Ge,
            AstBinaryOp::Lt => {
                matches!(predicate.op, AstBinaryOp::Lt | AstBinaryOp::Le)
            }
            AstBinaryOp::Le => predicate.op == AstBinaryOp::Le,
            _ => false,
        }
    }
}

fn expr_guarantees_non_null(
    expr: &Expr,
    predicate_column: &WhereColumn,
    index_table: &str,
    unqualified_terms_are_table_local: bool,
) -> bool {
    if direct_comparison_guarantees_non_null(
        expr,
        predicate_column,
        index_table,
        unqualified_terms_are_table_local,
    ) {
        return true;
    }

    if let Some((column, _)) = classify_or_disjunction_as_in_list(expr) {
        return where_columns_match_partial_index(
            &column,
            predicate_column,
            index_table,
            unqualified_terms_are_table_local,
        );
    }

    match expr {
        Expr::Between { expr: inner, .. } => extract_where_column(inner).is_some_and(|column| {
            where_columns_match_partial_index(
                &column,
                predicate_column,
                index_table,
                unqualified_terms_are_table_local,
            )
        }),
        Expr::In {
            expr: inner,
            set,
            not,
            ..
        } => {
            // SQLite makes `NULL NOT IN` true when the RHS is empty. A
            // subquery/table may be empty at execution, and an explicit empty
            // list is empty by construction, so those negative forms do not
            // prove the left operand non-NULL. A non-empty literal list does:
            // with a NULL left operand its result is NULL, never true.
            let proves_non_null = !*not || matches!(set, InSet::List(items) if !items.is_empty());
            proves_non_null
                && extract_where_column(inner).is_some_and(|column| {
                    where_columns_match_partial_index(
                        &column,
                        predicate_column,
                        index_table,
                        unqualified_terms_are_table_local,
                    )
                })
        }
        Expr::IsNull {
            expr: inner,
            not: true,
            ..
        } => extract_where_column(inner).is_some_and(|column| {
            where_columns_match_partial_index(
                &column,
                predicate_column,
                index_table,
                unqualified_terms_are_table_local,
            )
        }),
        _ => false,
    }
}

fn direct_comparison_guarantees_non_null(
    expr: &Expr,
    predicate_column: &WhereColumn,
    index_table: &str,
    unqualified_terms_are_table_local: bool,
) -> bool {
    let Expr::BinaryOp {
        left, op, right, ..
    } = expr
    else {
        return false;
    };
    let column_matches = |candidate: &Expr| {
        extract_where_column(candidate).is_some_and(|column| {
            where_columns_match_partial_index(
                &column,
                predicate_column,
                index_table,
                unqualified_terms_are_table_local,
            )
        })
    };
    let is_explicit_null = |candidate: &Expr| {
        let mut candidate = candidate;
        loop {
            match candidate {
                Expr::Literal(Literal::Null, _)
                | Expr::BoundOuterValue {
                    value: SqliteValue::Null,
                    ..
                } => break true,
                Expr::UnaryOp { expr: inner, .. }
                | Expr::Cast { expr: inner, .. }
                | Expr::Collate { expr: inner, .. } => candidate = inner,
                _ => break false,
            }
        }
    };

    match op {
        // SQL's ordinary comparisons are NULL-propagating. Regardless of the
        // opposite operand's shape or runtime value, a TRUE result proves that
        // every directly referenced comparison column is non-NULL. Keep an
        // explicit NULL operand fail-closed: that predicate can never be TRUE,
        // so selecting a partial index provides no useful probe while making
        // the implication proof depend on vacuous truth.
        AstBinaryOp::Eq
        | AstBinaryOp::Ne
        | AstBinaryOp::Lt
        | AstBinaryOp::Le
        | AstBinaryOp::Gt
        | AstBinaryOp::Ge => {
            (column_matches(left) && !is_explicit_null(right))
                || (column_matches(right) && !is_explicit_null(left))
        }
        // Unlike ordinary comparisons, `NULL IS NULL` is TRUE. Only admit an
        // IS proof when the opposite side is a source-level non-NULL literal;
        // a placeholder may bind NULL and must remain fail-closed.
        AstBinaryOp::Is => {
            let is_non_null_literal = |candidate: &Expr| matches!(candidate, Expr::Literal(literal, _) if !matches!(literal, Literal::Null));
            (column_matches(left) && is_non_null_literal(right))
                || (column_matches(right) && is_non_null_literal(left))
        }
        _ => false,
    }
}

fn where_columns_match_partial_index(
    query: &WhereColumn,
    predicate: &WhereColumn,
    index_table: &str,
    unqualified_terms_are_table_local: bool,
) -> bool {
    query.column.eq_ignore_ascii_case(&predicate.column)
        && match (&query.table, &predicate.table) {
            (None, None) => unqualified_terms_are_table_local,
            // Partial-index predicates are table-local. A query qualifier that
            // names the indexed table therefore binds the otherwise bare
            // predicate column without treating arbitrary qualifiers as
            // wildcards.
            (Some(query_table), None) => query_table.eq_ignore_ascii_case(index_table),
            // The reverse direction needs name-resolution evidence for the
            // unqualified query column, which this planner seam does not carry.
            (None, Some(_)) => false,
            (Some(query_table), Some(predicate_table)) => {
                query_table.eq_ignore_ascii_case(predicate_table)
                    && query_table.eq_ignore_ascii_case(index_table)
            }
        }
}

fn normalize_is_not_null_predicate(expr: &Expr) -> Option<WhereColumn> {
    let Expr::IsNull {
        expr: inner,
        not: true,
        ..
    } = expr
    else {
        return None;
    };
    extract_where_column(inner)
}

fn normalize_column_literal_comparison(expr: &Expr) -> Option<NormalizedColumnComparison> {
    let Expr::BinaryOp {
        left,
        op: AstBinaryOp::Eq | AstBinaryOp::Lt | AstBinaryOp::Le | AstBinaryOp::Gt | AstBinaryOp::Ge,
        right,
        ..
    } = expr
    else {
        return None;
    };

    if let (Some(column), Expr::Literal(literal, _)) = (extract_where_column(left), right.as_ref())
    {
        return Some(NormalizedColumnComparison {
            column,
            op: match expr {
                Expr::BinaryOp { op, .. } => *op,
                _ => unreachable!(),
            },
            literal: literal.clone(),
        });
    }

    if let (Expr::Literal(literal, _), Some(column)) = (left.as_ref(), extract_where_column(right))
    {
        return Some(NormalizedColumnComparison {
            column,
            op: reverse_comparison_op(match expr {
                Expr::BinaryOp { op, .. } => *op,
                _ => unreachable!(),
            })?,
            literal: literal.clone(),
        });
    }

    None
}

fn reverse_comparison_op(op: AstBinaryOp) -> Option<AstBinaryOp> {
    match op {
        AstBinaryOp::Eq => Some(AstBinaryOp::Eq),
        AstBinaryOp::Lt => Some(AstBinaryOp::Gt),
        AstBinaryOp::Le => Some(AstBinaryOp::Ge),
        AstBinaryOp::Gt => Some(AstBinaryOp::Lt),
        AstBinaryOp::Ge => Some(AstBinaryOp::Le),
        _ => None,
    }
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
    /// columns, optionally followed by an additional constraint on the next
    /// column.
    MultiColumnEquality {
        /// Number of leading columns with equality constraints.
        eq_columns: usize,
        /// Constraint on the column immediately after the equality prefix.
        trailing_constraint: MultiColumnTrailingConstraint,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultiColumnTrailingConstraint {
    None,
    Range,
    InExpansion { probe_count: usize },
    LikePrefix,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct SkipScanCandidate {
    leading_probes: usize,
    trailing_probe_count: usize,
    per_probe_selectivity: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct IndexColumnTermSummary {
    has_equality: bool,
    first_in_probe_count: Option<usize>,
    has_range: bool,
    first_like_prefix: Option<(String, Option<String>)>,
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
    let mut pending = FoldStack::<_, 16>::new();
    pending.push(expr);
    while let Some(term) = pending.pop() {
        if let Expr::BinaryOp {
            left,
            op: AstBinaryOp::And,
            right,
            ..
        } = term
        {
            pending.push(right);
            pending.push(left);
        } else {
            out.push(term);
        }
    }
}

fn collect_disjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    let mut pending = FoldStack::<_, 16>::new();
    pending.push(expr);
    while let Some(term) = pending.pop() {
        if let Expr::BinaryOp {
            left,
            op: AstBinaryOp::Or,
            right,
            ..
        } = term
        {
            pending.push(right);
            pending.push(left);
        } else {
            out.push(term);
        }
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

    let mut shared_candidates: Option<Vec<WhereColumn>> = None;

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

        let mut candidates = [extract_where_column(left), extract_where_column(right)]
            .into_iter()
            .flatten()
            .filter(|column| !is_rowid_column(column))
            .collect::<Vec<_>>();
        candidates.dedup_by(|left, right| where_columns_equivalent(left, right));
        if candidates.is_empty() {
            return None;
        }

        if let Some(existing) = &mut shared_candidates {
            existing.retain(|candidate| {
                candidates
                    .iter()
                    .any(|column| where_columns_equivalent(candidate, column))
            });
            if existing.is_empty() {
                return None;
            }
        } else {
            shared_candidates = Some(candidates);
        }
    }

    let mut shared_candidates = shared_candidates?;
    if shared_candidates.len() != 1 {
        // More than one shared column makes probe orientation ambiguous, as
        // with `(t1.a = t2.a) OR (t1.a = t2.a)`.
        return None;
    }
    let column = shared_candidates.pop()?;
    let table_name = column.table.as_deref().unwrap_or("");
    if !disjuncts.iter().all(|disjunct| {
        comparison_operand_for_column(disjunct, table_name, &column.column).is_some()
    }) {
        // The shared column must occur on exactly one side of every equality.
        // A tautology such as `t1.a = t1.a` has no probe operand and therefore
        // cannot be represented as an executable IN-list rewrite.
        return None;
    }
    Some((column, disjuncts.len()))
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
        //
        // `col = NULL` is special-cased: in SQL, `x = NULL` evaluates to NULL
        // (unknown), never TRUE, so it cannot drive an index seek or equality
        // constraint.  Classify it as Other instead of Equality.
        Expr::BinaryOp {
            left,
            op: AstBinaryOp::Eq,
            right,
            ..
        } => {
            if expr_is_null_constant(left) || expr_is_null_constant(right) {
                return WhereTerm {
                    expr,
                    column: None,
                    kind: WhereTermKind::Other,
                };
            }
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
        } => match extract_where_column(left).or_else(|| extract_where_column(right)) {
            Some(column) => WhereTerm {
                expr,
                column: Some(column),
                kind: WhereTermKind::Range,
            },
            // bd-rwaxp: no plain column on either side — e.g. an expression-index
            // key range like `lower(name) >= ?1`. Classify as Other (mirroring the
            // Eq arm above), so the expression-index-key access path in
            // `table_local_index_probe_is_evaluable` applies. Leaving it as Range
            // with no column made the unqualified-single-table access-term filter
            // drop it (that filter's Range case requires a plain column), so the
            // planner never surfaced IndexScanRange for a plain expression-index
            // range (only the aliased/partial form, which skips that filter, did).
            None => WhereTerm {
                expr,
                column: None,
                kind: WhereTermKind::Other,
            },
        },

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

        // col GLOB 'prefix*' or col LIKE 'prefix%' — prefix-to-range optimisation.
        //
        // GLOB is always case-sensitive, so prefix extraction is always safe.
        //
        // LIKE is case-INSENSITIVE by default (for ASCII), so converting
        // `col LIKE 'abc%'` into the range `col >= 'abc' AND col < 'abd'`
        // would miss rows like 'ABC…'. The optimisation is only safe when:
        //   (a) PRAGMA case_sensitive_like = ON, OR
        //   (b) The column has BINARY collation
        //
        // Until collation/pragma state is wired through the planner, we still
        // have one sound subset we can lower today: prefixes with no ASCII
        // letters. SQLite's default LIKE only case-folds ASCII, so those
        // prefixes are already case-stable.
        Expr::Like {
            expr: inner,
            pattern,
            op,
            not,
            escape,
            ..
        } if !not => {
            let column = extract_where_column(inner);
            let (prefix, operator) = match op {
                LikeOp::Glob => (extract_glob_prefix(pattern), "GLOB"),
                LikeOp::Like => {
                    let prefix = extract_like_prefix(pattern, escape.as_deref())
                        .filter(|prefix| is_like_prefix_safe_for_column(column.as_ref(), prefix));
                    (prefix, "LIKE")
                }
                // Match and Regexp are not optimizable via prefix-to-range.
                LikeOp::Match | LikeOp::Regexp => (None, "MATCH/REGEXP"),
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
            table: col_ref.table.as_ref().map(ToString::to_string),
            column: col_ref.column.to_string(),
        })
    } else {
        None
    }
}

fn expr_is_null_constant(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(Literal::Null, _)
            | Expr::BoundOuterValue {
                value: SqliteValue::Null,
                ..
            }
    )
}

/// Check if a `WhereColumn` is a rowid alias.
fn is_rowid_column(wc: &WhereColumn) -> bool {
    is_rowid_alias_name(&wc.column)
}

fn where_term_matches_rowid_equality(
    table_name: &str,
    term: &WhereTerm<'_>,
    rowid_alias_hints: &[RowidAliasHint],
) -> bool {
    if matches!(term.kind, WhereTermKind::RowidEquality) {
        return true;
    }

    matches!(term.kind, WhereTermKind::Equality)
        && term.column.as_ref().is_some_and(|column| {
            rowid_alias_hints
                .iter()
                .any(|hint| hint.matches_column(table_name, column))
        })
}

fn where_term_matches_rowid_range(
    table_name: &str,
    term: &WhereTerm<'_>,
    rowid_alias_hints: &[RowidAliasHint],
) -> bool {
    matches!(term.kind, WhereTermKind::Range | WhereTermKind::Between)
        && term.column.as_ref().is_some_and(|column| {
            is_rowid_column(column)
                || rowid_alias_hints
                    .iter()
                    .any(|hint| hint.matches_column(table_name, column))
        })
}

fn find_rowid_equality_term<'terms, 'expr>(
    table_name: &str,
    terms: &'terms [WhereTerm<'expr>],
    rowid_alias_hints: &[RowidAliasHint],
) -> Option<&'terms WhereTerm<'expr>> {
    terms
        .iter()
        .find(|term| where_term_matches_rowid_equality(table_name, term, rowid_alias_hints))
}

fn find_rowid_range_column<'a>(
    table_name: &str,
    terms: &'a [WhereTerm<'_>],
    rowid_alias_hints: &[RowidAliasHint],
) -> Option<&'a str> {
    terms.iter().find_map(|term| {
        where_term_matches_rowid_range(table_name, term, rowid_alias_hints)
            .then(|| term.column.as_ref().map(|column| column.column.as_str()))
            .flatten()
    })
}

/// Extract the side opposite one specific table column from a binary
/// comparison expression.
fn comparison_operand_for_column<'expr>(
    expr: &'expr Expr,
    table_name: &str,
    column_name: &str,
) -> Option<&'expr Expr> {
    let Expr::BinaryOp { left, right, .. } = expr else {
        return None;
    };
    let matches_target = |expr: &Expr| {
        extract_where_column(expr).is_some_and(|column| {
            identifier_eq(&column.column, column_name)
                && column
                    .table
                    .as_deref()
                    .is_none_or(|qualifier| identifier_eq(qualifier, table_name))
        })
    };
    match (matches_target(left), matches_target(right)) {
        (true, false) => Some(right),
        (false, true) => Some(left),
        (true, true) | (false, false) => None,
    }
}

/// Extract the opposite operand of a comparison against one fully identified
/// WHERE column.  Unlike [`comparison_operand_for_column`], this preserves a
/// query alias qualifier, which is required for schema-provided rowid aliases.
fn comparison_operand_for_where_column<'expr>(
    expr: &'expr Expr,
    target_column: &WhereColumn,
) -> Option<&'expr Expr> {
    let Expr::BinaryOp { left, right, .. } = expr else {
        return None;
    };
    let matches_target = |expr: &Expr| {
        extract_where_column(expr)
            .is_some_and(|column| where_columns_equivalent(&column, target_column))
    };
    match (matches_target(left), matches_target(right)) {
        (true, false) => Some(right),
        (false, true) => Some(left),
        (true, true) | (false, false) => None,
    }
}

fn extract_comparison_operand_for_column(
    expr: &Expr,
    table_name: &str,
    column_name: &str,
) -> Option<Expr> {
    comparison_operand_for_column(expr, table_name, column_name).cloned()
}

/// Given a finalized [`AccessPath`] and the WHERE terms that produced it,
/// extract probe expressions so downstream consumers do not re-parse the
/// WHERE clause.
fn extract_access_path_probe_with_rowid_aliases(
    best: &AccessPath,
    indexes: &[IndexInfo],
    where_terms: &[WhereTerm<'_>],
    rowid_alias_hints: &[RowidAliasHint],
) -> Option<AccessPathProbe> {
    match &best.kind {
        AccessPathKind::FullTableScan => None,
        AccessPathKind::RowidLookup => {
            let term = find_rowid_equality_term(&best.table, where_terms, rowid_alias_hints)?;
            let column = term.column.as_ref()?;
            let target = comparison_operand_for_where_column(term.expr, column)?.clone();
            Some(AccessPathProbe::RowidEquality {
                target: Box::new(target),
            })
        }
        AccessPathKind::IndexScanEquality => {
            let index_name = best.index.as_deref()?;
            let idx = indexes
                .iter()
                .find(|i| identifier_eq(&i.name, index_name))?;
            let leading_col = idx.columns.first()?;
            if let Some(term) = where_terms.iter().find(|t| {
                matches!(t.kind, WhereTermKind::Equality)
                    && t.column
                        .as_ref()
                        .is_some_and(|c| identifier_eq(&c.column, leading_col))
            }) {
                let target =
                    extract_comparison_operand_for_column(term.expr, &best.table, leading_col)?;
                return Some(AccessPathProbe::Equality {
                    column: leading_col.clone(),
                    target: Box::new(target),
                });
            }
            if let Some(term) = where_terms.iter().find(|t| {
                matches!(t.kind, WhereTermKind::InList { .. })
                    && t.column
                        .as_ref()
                        .is_some_and(|c| identifier_eq(&c.column, leading_col))
            }) {
                return extract_in_list_probe(term.expr, &best.table, leading_col);
            }
            None
        }
        AccessPathKind::IndexScanRange { .. } | AccessPathKind::CoveringIndexScan { .. } => {
            if best.index.is_none() {
                let leading_col =
                    find_rowid_range_column(&best.table, where_terms, rowid_alias_hints)?;
                return extract_range_probe_for_column(where_terms, &best.table, leading_col);
            }
            let index_name = best.index.as_deref()?;
            let idx = indexes
                .iter()
                .find(|i| identifier_eq(&i.name, index_name))?;
            let leading_col = idx.columns.first()?;
            extract_range_probe_for_column(where_terms, &best.table, leading_col)
        }
    }
}

fn extract_range_probe_for_column(
    where_terms: &[WhereTerm<'_>],
    table_name: &str,
    leading_col: &str,
) -> Option<AccessPathProbe> {
    let mut lower: Option<(Box<Expr>, bool)> = None;
    let mut upper: Option<(Box<Expr>, bool)> = None;
    for term in where_terms {
        let col = match &term.column {
            Some(c) if identifier_eq(&c.column, leading_col) => c,
            _ => continue,
        };
        if matches!(term.kind, WhereTermKind::Equality) {
            let target = extract_comparison_operand_for_column(term.expr, table_name, leading_col)?;
            return Some(AccessPathProbe::Equality {
                column: col.column.clone(),
                target: Box::new(target),
            });
        }
        if let WhereTermKind::LikePrefix {
            prefix,
            upper_bound,
        } = &term.kind
        {
            let lo = Expr::Literal(Literal::String(prefix.clone()), Span::ZERO);
            let lo_bound = Some((Box::new(lo), true));
            let hi_bound = upper_bound.as_ref().map(|ub| {
                (
                    Box::new(Expr::Literal(Literal::String(ub.clone()), Span::ZERO)),
                    false,
                )
            });
            return Some(AccessPathProbe::Range {
                column: col.column.clone(),
                lower: lo_bound,
                upper: hi_bound,
            });
        }
        if matches!(term.kind, WhereTermKind::Between)
            && let Expr::Between { low, high, not, .. } = term.expr
            && !not
        {
            return Some(AccessPathProbe::Range {
                column: col.column.clone(),
                lower: Some((Box::new(low.as_ref().clone()), true)),
                upper: Some((Box::new(high.as_ref().clone()), true)),
            });
        }
        if !matches!(term.kind, WhereTermKind::Range) {
            continue;
        }
        if let Expr::BinaryOp {
            left, op, right, ..
        } = term.expr
        {
            let side_matches = |expr: &Expr| {
                extract_where_column(expr).is_some_and(|column| {
                    identifier_eq(&column.column, leading_col)
                        && column
                            .table
                            .as_deref()
                            .is_none_or(|qualifier| identifier_eq(qualifier, table_name))
                })
            };
            let (col_on_left, col_on_right) = (side_matches(left), side_matches(right));
            if col_on_left == col_on_right {
                continue;
            }
            match op {
                AstBinaryOp::Gt => {
                    let val = if col_on_left { right } else { left };
                    if col_on_left {
                        lower = Some((Box::new(val.as_ref().clone()), false));
                    } else {
                        upper = Some((Box::new(val.as_ref().clone()), false));
                    }
                }
                AstBinaryOp::Ge => {
                    let val = if col_on_left { right } else { left };
                    if col_on_left {
                        lower = Some((Box::new(val.as_ref().clone()), true));
                    } else {
                        upper = Some((Box::new(val.as_ref().clone()), true));
                    }
                }
                AstBinaryOp::Lt => {
                    let val = if col_on_left { right } else { left };
                    if col_on_left {
                        upper = Some((Box::new(val.as_ref().clone()), false));
                    } else {
                        lower = Some((Box::new(val.as_ref().clone()), false));
                    }
                }
                AstBinaryOp::Le => {
                    let val = if col_on_left { right } else { left };
                    if col_on_left {
                        upper = Some((Box::new(val.as_ref().clone()), true));
                    } else {
                        lower = Some((Box::new(val.as_ref().clone()), true));
                    }
                }
                _ => {}
            }
        }
    }
    if lower.is_some() || upper.is_some() {
        Some(AccessPathProbe::Range {
            column: leading_col.to_owned(),
            lower,
            upper,
        })
    } else {
        None
    }
}

fn extract_in_list_probe(expr: &Expr, table_name: &str, column: &str) -> Option<AccessPathProbe> {
    if let Expr::In {
        set: InSet::List(items),
        not: false,
        ..
    } = expr
    {
        let values: Vec<Box<Expr>> = items.iter().map(|item| Box::new(item.clone())).collect();
        if values.is_empty() {
            return None;
        }
        return Some(AccessPathProbe::InList {
            column: column.to_owned(),
            values,
        });
    }
    if matches!(
        expr,
        Expr::BinaryOp {
            op: AstBinaryOp::Or,
            ..
        }
    ) {
        let mut disjuncts = Vec::new();
        collect_disjuncts(expr, &mut disjuncts);
        if disjuncts.len() < 2 {
            return None;
        }
        let values = disjuncts
            .into_iter()
            .map(|disjunct| {
                extract_comparison_operand_for_column(disjunct, table_name, column).map(Box::new)
            })
            .collect::<Option<Vec<_>>>()?;
        return Some(AccessPathProbe::InList {
            column: column.to_owned(),
            values,
        });
    }
    None
}

/// Extract a pure trailing-wildcard prefix from a GLOB pattern (e.g.
/// `'abc*'` → `"abc"`).
///
/// Returns `None` unless the pattern is a string literal whose only wildcard
/// region is one or more trailing `*` characters. Shapes such as `abc*def`,
/// `abc[0-9]`, or exact-match `abc` require either residual filtering or
/// equality handling, so the current prefix-range lowering refuses them.
fn extract_glob_prefix(pattern: &Expr) -> Option<String> {
    if let Expr::Literal(Literal::String(s), _) = pattern {
        let mut prefix = String::new();
        let mut saw_trailing_star = false;
        for ch in s.chars() {
            match ch {
                '*' => saw_trailing_star = true,
                '?' | '[' => return None,
                _ if saw_trailing_star => return None,
                _ => prefix.push(ch),
            }
        }
        if prefix.is_empty() || !saw_trailing_star {
            None
        } else {
            Some(prefix)
        }
    } else {
        None
    }
}

/// Extract a pure trailing-wildcard prefix from a LIKE pattern (e.g.
/// `'abc%'` → `"abc"`).
///
/// Returns `None` if:
/// - The pattern has no trailing `%` wildcard
/// - The pattern is not a string literal
/// - The `ESCAPE` expression is not a literal single character
/// - The pattern contains an unescaped `_` or any non-trailing wildcard/text
///   after the first unescaped `%`
///
/// bd-wwqen.6: This enables the LIKE prefix-to-range optimization when
/// collation makes it safe (BINARY collation or case_sensitive_like = ON).
fn extract_like_prefix(pattern: &Expr, escape: Option<&Expr>) -> Option<String> {
    let escape_char = match escape {
        None => None,
        Some(Expr::Literal(Literal::String(s), _)) => {
            let mut chars = s.chars();
            let ch = chars.next()?;
            if chars.next().is_some() {
                return None;
            }
            Some(ch)
        }
        Some(_) => return None,
    };

    if let Expr::Literal(Literal::String(s), _) = pattern {
        let mut prefix = String::new();
        let mut saw_trailing_percent = false;
        let mut chars = s.chars();
        while let Some(ch) = chars.next() {
            if escape_char.is_some_and(|esc| esc == ch) {
                if saw_trailing_percent {
                    return None;
                }
                prefix.push(chars.next()?);
                continue;
            }
            match ch {
                '%' => saw_trailing_percent = true,
                '_' => return None,
                _ if saw_trailing_percent => return None,
                _ => prefix.push(ch),
            }
        }
        if prefix.is_empty() || !saw_trailing_percent {
            None
        } else {
            Some(prefix)
        }
    } else {
        None
    }
}

/// Check if a LIKE prefix is guaranteed to be case-stable under SQLite's
/// default ASCII-only case folding.
///
/// The conservative fallback we can enable today, even without collation or
/// pragma plumbing, is: if the extracted prefix contains no ASCII letters, the
/// default LIKE case folding cannot expand the match set beyond the byte range
/// defined by `prefix .. upper_bound(prefix)`.
///
/// Examples that are safe under default SQLite semantics:
/// - `"2024-%"` (digits and punctuation only)
/// - `"é%"` (non-ASCII characters are not case-folded by built-in LIKE)
///
/// Future planner context can widen this by checking:
/// - `PRAGMA case_sensitive_like`
/// - BINARY/case-sensitive column or index collations
fn is_like_prefix_safe_for_column(_column: Option<&WhereColumn>, prefix: &str) -> bool {
    prefix.chars().all(|ch| !ch.is_ascii_alphabetic())
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
/// - After the equality prefix, check for a range/BETWEEN, `IN (...)`, or
///   `LIKE`/`GLOB` prefix probe on the next column.
/// - For single-column leftmost matches, also check IN and LIKE prefix.
/// - For expression indexes, match query expressions structurally against the
///   index's expression columns.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn analyze_index_usability(index: &IndexInfo, terms: &[WhereTerm<'_>]) -> IndexUsability {
    // --- Expression index matching ---
    // Expression indexes store their real key terms in `expression_columns`
    // and leave `columns` empty by convention (see the schema loader at
    // fsqlite-core/src/connection.rs), so the expression-index branch MUST
    // run BEFORE the `columns.is_empty()` guard below — otherwise it would
    // be unreachable and every expression index would appear planner-dead
    // (issue #63).  We still fall through to `NotUsable` if neither
    // `columns` nor `expression_columns` carries a matchable term.
    if !index.expression_columns.is_empty() {
        return analyze_expression_index_usability(index, terms);
    }

    if index.columns.is_empty() {
        return IndexUsability::NotUsable;
    }

    // Helper: check if a WHERE column matches an index column, respecting
    // the table qualifier when present.  Unqualified columns (table = None)
    // are conservatively considered matching.
    let col_matches = |wc: &WhereColumn, idx_col: &str| -> bool {
        identifier_eq(&wc.column, idx_col)
            && wc
                .table
                .as_ref()
                .is_none_or(|t| identifier_eq(t, &index.table))
    };

    let mut column_summaries = vec![IndexColumnTermSummary::default(); index.columns.len()];
    let mut leftmost_first_constraint = None;

    for term in terms {
        let Some(wc) = term.column.as_ref() else {
            continue;
        };
        for (column_index, index_column) in index.columns.iter().enumerate() {
            if !col_matches(wc, index_column) {
                continue;
            }
            let summary = &mut column_summaries[column_index];
            match &term.kind {
                WhereTermKind::Equality => {
                    summary.has_equality = true;
                    if column_index == 0 {
                        // Equality must dominate weaker leftmost probes on the
                        // same column, regardless of term visitation order.
                        leftmost_first_constraint = Some(IndexUsability::Equality);
                    }
                }
                WhereTermKind::InList { count } => {
                    if summary
                        .first_in_probe_count
                        .is_none_or(|existing| *count < existing)
                    {
                        summary.first_in_probe_count = Some(*count);
                    }
                    if column_index == 0 {
                        match leftmost_first_constraint {
                            Some(IndexUsability::InExpansion { probe_count })
                                if *count < probe_count =>
                            {
                                leftmost_first_constraint = Some(IndexUsability::InExpansion {
                                    probe_count: *count,
                                });
                            }
                            None => {
                                leftmost_first_constraint = Some(IndexUsability::InExpansion {
                                    probe_count: *count,
                                });
                            }
                            _ => {}
                        }
                    }
                }
                WhereTermKind::LikePrefix {
                    prefix,
                    upper_bound,
                } => {
                    summary
                        .first_like_prefix
                        .get_or_insert_with(|| (prefix.clone(), upper_bound.clone()));
                    if column_index == 0 && leftmost_first_constraint.is_none() {
                        leftmost_first_constraint = Some(IndexUsability::LikePrefix {
                            low: prefix.clone(),
                            high: upper_bound.clone(),
                        });
                    }
                }
                WhereTermKind::Range | WhereTermKind::Between => {
                    summary.has_range = true;
                }
                WhereTermKind::RowidEquality | WhereTermKind::Other => {}
            }
        }
    }

    // --- Multi-column equality prefix ---
    // Walk index columns left-to-right, counting how many have equality terms.
    let eq_columns = column_summaries
        .iter()
        .take_while(|summary| summary.has_equality)
        .count();

    // Preserve the composite shape when we have either:
    // - equality on 2+ consecutive index columns, or
    // - equality on a leading prefix plus an IN/range constraint on the next
    //   column.
    if eq_columns >= 1 {
        let trailing_constraint = if eq_columns < index.columns.len() {
            let summary = &column_summaries[eq_columns];
            if let Some(probe_count) = summary.first_in_probe_count {
                MultiColumnTrailingConstraint::InExpansion { probe_count }
            } else if summary.first_like_prefix.is_some() {
                MultiColumnTrailingConstraint::LikePrefix
            } else if summary.has_range {
                MultiColumnTrailingConstraint::Range
            } else {
                MultiColumnTrailingConstraint::None
            }
        } else {
            MultiColumnTrailingConstraint::None
        };

        if eq_columns >= 2 || !matches!(trailing_constraint, MultiColumnTrailingConstraint::None) {
            return IndexUsability::MultiColumnEquality {
                eq_columns,
                trailing_constraint,
            };
        }
    }

    // --- Single leftmost column checks (original logic) ---
    if let Some(usability) = leftmost_first_constraint {
        return usability;
    }

    if column_summaries[0].has_range {
        return IndexUsability::Range {
            selectivity: DEFAULT_RANGE_SELECTIVITY,
        };
    }

    IndexUsability::NotUsable
}

/// Analyze usability for an expression index by matching WHERE term expressions
/// against the index's expression columns using structural equality
/// (`Expr::PartialEq`, which is manually implemented in fsqlite-ast to ignore
/// every node's `Span` field — see the doc comment on `impl PartialEq for
/// Expr`).  That span-insensitivity is what makes cross-parse-context
/// matching work: the index key is parsed from its stand-alone SQL text at
/// schema-load time while the WHERE clause is parsed as part of the
/// enclosing SELECT, so the two ASTs carry different byte offsets.
///
/// Note on classification interplay (issue #63):
/// `classify_where_term` only assigns `WhereTermKind::Equality` when the left-
/// hand side of an `=` BinaryOp is a bare column (via `extract_where_column`).
/// For predicates like `lower(name) = 'alice'` the left side is a function
/// call, so the term is classified as `WhereTermKind::Other` even though it
/// is structurally `<expr> = <literal>`.  We therefore match against the raw
/// `term.expr` AST here — inspecting the BinaryOp / Between directly —
/// instead of filtering by `term.kind`.
fn analyze_expression_index_usability(
    index: &IndexInfo,
    terms: &[WhereTerm<'_>],
) -> IndexUsability {
    let Some(first_expr) = index.expression_columns.first() else {
        return IndexUsability::NotUsable;
    };

    // Pass 1: prefer Equality matches (Equality beats Range on the same key).
    for term in terms {
        if let Expr::BinaryOp {
            left,
            op: AstBinaryOp::Eq,
            right,
            ..
        } = term.expr
        {
            // Match <expr> = <value> or <value> = <expr>.  NULL equality
            // cannot drive an index seek (SQL semantics), so skip the
            // `x = NULL` / `NULL = x` degenerate forms exactly like
            // classify_where_term does for plain columns.
            let left_is_null = expr_is_null_constant(left);
            let right_is_null = expr_is_null_constant(right);
            if left_is_null || right_is_null {
                continue;
            }
            if expression_matches_index_key(left, first_expr, &index.table)
                || expression_matches_index_key(right, first_expr, &index.table)
            {
                return IndexUsability::Equality;
            }
        }
    }

    // Pass 2: fall back to Range/Between matches.
    for term in terms {
        if let Expr::BinaryOp {
            left,
            op: AstBinaryOp::Lt | AstBinaryOp::Le | AstBinaryOp::Gt | AstBinaryOp::Ge,
            right,
            ..
        } = term.expr
            && (expression_matches_index_key(left, first_expr, &index.table)
                || expression_matches_index_key(right, first_expr, &index.table))
        {
            return IndexUsability::Range {
                selectivity: DEFAULT_RANGE_SELECTIVITY,
            };
        }
        if let Expr::Between {
            expr: inner, not, ..
        } = term.expr
            && !*not
            && expression_matches_index_key(inner, first_expr, &index.table)
        {
            return IndexUsability::Range {
                selectivity: DEFAULT_RANGE_SELECTIVITY,
            };
        }
    }

    IndexUsability::NotUsable
}

fn expression_matches_index_key(query: &Expr, key: &Expr, index_table: &str) -> bool {
    let mut normalized_query = query.clone();
    let mut normalized_key = key.clone();
    normalize_expression_index_columns(&mut normalized_query, index_table)
        && normalize_expression_index_columns(&mut normalized_key, index_table)
        && normalized_query == normalized_key
}

/// Canonicalize columns for expression-index structural matching.
///
/// Index definitions are parsed outside the query and therefore normally
/// store bare columns, while a query may qualify the same column with the
/// table's visible name or alias. Strip only that proven-local qualifier and
/// fold the column name to SQLite's case-insensitive identifier form. A
/// foreign qualifier or a nested scope fails closed.
fn normalize_expression_index_columns(expr: &mut Expr, index_table: &str) -> bool {
    match expr {
        Expr::Literal(..) | Expr::Placeholder(..) => true,
        Expr::Column(column, _) => {
            if column
                .table
                .as_deref()
                .is_some_and(|qualifier| !identifier_eq(qualifier, index_table))
            {
                return false;
            }
            column.table = None;
            column.column = column.column.to_ascii_lowercase().into();
            true
        }
        Expr::BinaryOp { left, right, .. }
        | Expr::JsonAccess {
            expr: left,
            path: right,
            ..
        } => {
            normalize_expression_index_columns(left, index_table)
                && normalize_expression_index_columns(right, index_table)
        }
        Expr::UnaryOp { expr, .. } | Expr::Collate { expr, .. } | Expr::IsNull { expr, .. } => {
            normalize_expression_index_columns(expr, index_table)
        }
        Expr::Cast {
            expr, type_name, ..
        } => {
            type_name.name.make_ascii_lowercase();
            if let Some(arg) = &mut type_name.arg1 {
                arg.make_ascii_lowercase();
            }
            if let Some(arg) = &mut type_name.arg2 {
                arg.make_ascii_lowercase();
            }
            normalize_expression_index_columns(expr, index_table)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            normalize_expression_index_columns(expr, index_table)
                && normalize_expression_index_columns(low, index_table)
                && normalize_expression_index_columns(high, index_table)
        }
        Expr::In { expr, set, .. } => {
            normalize_expression_index_columns(expr, index_table)
                && match set {
                    InSet::List(items) => items
                        .iter_mut()
                        .all(|item| normalize_expression_index_columns(item, index_table)),
                    InSet::Subquery(_) | InSet::Table(_) => false,
                }
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            normalize_expression_index_columns(expr, index_table)
                && normalize_expression_index_columns(pattern, index_table)
                && escape
                    .as_deref_mut()
                    .is_none_or(|escape| normalize_expression_index_columns(escape, index_table))
        }
        Expr::Case {
            operand,
            whens,
            else_expr,
            ..
        } => {
            operand
                .as_deref_mut()
                .is_none_or(|operand| normalize_expression_index_columns(operand, index_table))
                && whens.iter_mut().all(|(when, then)| {
                    normalize_expression_index_columns(when, index_table)
                        && normalize_expression_index_columns(then, index_table)
                })
                && else_expr.as_deref_mut().is_none_or(|else_expr| {
                    normalize_expression_index_columns(else_expr, index_table)
                })
        }
        Expr::FunctionCall {
            args,
            order_by,
            filter,
            over,
            ..
        } => {
            let args_local = match args {
                fsqlite_ast::FunctionArgs::Star => false,
                fsqlite_ast::FunctionArgs::List(args) => args
                    .iter_mut()
                    .all(|arg| normalize_expression_index_columns(arg, index_table)),
            };
            args_local
                && order_by
                    .iter_mut()
                    .all(|term| normalize_expression_index_columns(&mut term.expr, index_table))
                && filter
                    .as_deref_mut()
                    .is_none_or(|filter| normalize_expression_index_columns(filter, index_table))
                && over.is_none()
        }
        Expr::RowValue(items, _) => items
            .iter_mut()
            .all(|item| normalize_expression_index_columns(item, index_table)),
        // Bound values are runtime-only nodes, while subqueries and RAISE
        // expressions cannot be part of a local expression-index key.
        Expr::BoundOuterValue { .. }
        | Expr::Exists { .. }
        | Expr::Subquery(..)
        | Expr::Raise { .. } => false,
    }
}

/// Default selectivity for range constraints when no ANALYZE data is available.
/// 0.33 means "a range predicate eliminates ~67% of rows." This is a
/// conservative estimate matching C SQLite's heuristic for tables without
/// `sqlite_stat1` data. When ANALYZE has been run, the planner uses the
/// actual statistics from sqlite_stat1 instead.
const DEFAULT_RANGE_SELECTIVITY: f64 = 0.33;
/// Selectivity heuristic for a constant LIKE/GLOB prefix range.
const LIKE_PREFIX_SELECTIVITY: f64 = 0.10;
/// Equality selectivity for skip-scan leading columns (1% = 100 distinct values).
const SKIP_SCAN_EQ_SELECTIVITY: f64 = 0.01;
/// Range selectivity for skip-scan trailing columns.
const SKIP_SCAN_RANGE_SELECTIVITY: f64 = 0.20;
/// Maximum estimated distinct values for a skip-scan leading column.
const SKIP_SCAN_MAX_LEADING_DISTINCT: u64 = 16;
/// Pages per distinct value for skip-scan cost estimation.
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
    let second_col = &index.columns[1];
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

    let mut second_column_summary = IndexColumnTermSummary::default();

    // The current heuristic only prices skip-scan over one skipped leading
    // column. If the first usable constraint is deeper in the key, the planner
    // would also need the distinct cardinality of every skipped prefix, not
    // just the leftmost column, to avoid underestimating cost.
    for term in terms {
        let Some(wc) = term.column.as_ref() else {
            continue;
        };
        if !col_matches(wc, second_col) {
            continue;
        }

        match &term.kind {
            WhereTermKind::Equality => second_column_summary.has_equality = true,
            WhereTermKind::InList { count }
                if *count > 0
                    && second_column_summary
                        .first_in_probe_count
                        .is_none_or(|existing| *count < existing) =>
            {
                second_column_summary.first_in_probe_count = Some(*count);
            }
            WhereTermKind::Range | WhereTermKind::Between | WhereTermKind::LikePrefix { .. } => {
                second_column_summary.has_range = true;
            }
            _ => {}
        }
    }

    let (trailing_probe_count, per_probe_selectivity) = if second_column_summary.has_equality {
        (1, SKIP_SCAN_EQ_SELECTIVITY)
    } else if let Some(probe_count) = second_column_summary.first_in_probe_count {
        (probe_count, SKIP_SCAN_EQ_SELECTIVITY)
    } else if second_column_summary.has_range {
        (1, SKIP_SCAN_RANGE_SELECTIVITY)
    } else {
        return None;
    };

    Some(SkipScanCandidate {
        leading_probes: leading_distinct as usize,
        trailing_probe_count,
        per_probe_selectivity,
    })
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

#[derive(Clone, Copy)]
struct JoinAccessPathContext<'a> {
    table_index_hints: Option<&'a BTreeMap<String, IndexHint>>,
    cracking_hints: Option<&'a CrackingHintStore>,
    available_outer_tables: &'a [String],
    unqualified_terms_are_table_local: bool,
}

fn join_access_path(
    table: &TableStats,
    indexes: &[IndexInfo],
    where_terms: &[WhereTerm<'_>],
    needed_columns: Option<&[String]>,
    context: JoinAccessPathContext<'_>,
) -> AccessPath {
    let explicit_hint = lookup_table_index_hint(&table.name, context.table_index_hints);
    let forced_index = match explicit_hint {
        Some(IndexHint::IndexedBy(index_name)) => indexes.iter().find(|index| {
            identifier_eq(&index.table, &table.name) && identifier_eq(&index.name, index_name)
        }),
        Some(IndexHint::NotIndexed) | None => None,
    };
    let adaptive_hint = context
        .cracking_hints
        .and_then(|store| store.preferred_index(&table.name));
    let bound_terms;
    let access_terms = if context.unqualified_terms_are_table_local {
        where_terms
    } else {
        // `order_joins` receives one global WHERE-term set. Without completed
        // name-resolution metadata, a bare constrained column cannot safely
        // drive an ordinary index, rowid lookup, skip-scan, or extracted probe
        // for every table that happens to expose the same name. Keep explicitly
        // qualified constrained columns. A join comparison is usable only when
        // every column in its opposite probe operand belongs to an outer table
        // that is already present in this candidate join prefix. The complete
        // term set is still passed separately for fail-closed partial-index
        // implication, because a same-row residual may prove an index predicate
        // without being executable as a pre-scan probe.
        bound_terms = where_terms
            .iter()
            .filter_map(|term| {
                bind_where_term_to_table(term, &table.name, context.available_outer_tables).or_else(
                    || {
                        forced_index
                            .filter(|index| {
                                bare_term_is_forced_index_probe(term, &table.name, index)
                            })
                            .map(|_| term.clone())
                    },
                )
            })
            .collect::<Vec<_>>();
        &bound_terms
    };
    best_access_path_internal(
        table,
        indexes,
        access_terms,
        where_terms,
        needed_columns,
        explicit_hint,
        adaptive_hint,
        &[],
        context.unqualified_terms_are_table_local,
    )
}

/// A join planner has no schema-level owner for an unqualified column.  An
/// explicit `INDEXED BY` requirement makes one ordinary index mandatory,
/// though, so a scalar equality on that index's leftmost bare column can be
/// bound to the required table without admitting unrelated predicates.
fn bare_term_is_forced_index_probe(
    term: &WhereTerm<'_>,
    table_name: &str,
    index: &IndexInfo,
) -> bool {
    matches!(term.kind, WhereTermKind::Equality)
        && index.expression_columns.is_empty()
        && term.column.as_ref().is_some_and(|column| {
            column.table.is_none()
                && index
                    .columns
                    .first()
                    .is_some_and(|leading| identifier_eq(leading, &column.column))
        })
        && table_local_index_probe_is_evaluable(term, table_name)
}

fn bind_where_term_to_table<'expr>(
    term: &WhereTerm<'expr>,
    table_name: &str,
    available_outer_tables: &[String],
) -> Option<WhereTerm<'expr>> {
    let qualifier_matches = |column: &WhereColumn| {
        column
            .table
            .as_deref()
            .is_some_and(|qualifier| identifier_eq(qualifier, table_name))
    };
    let (column, probe_operand) = match term.kind {
        WhereTermKind::Equality | WhereTermKind::RowidEquality | WhereTermKind::Range => {
            let (column, operand) = qualified_comparison_probe_operand(term.expr, table_name)?;
            (Some(column), operand)
        }
        WhereTermKind::Other => {
            let column = term
                .column
                .clone()
                .filter(|column| qualifier_matches(column));
            let mut qualifiers = HashSet::new();
            collect_table_refs(term.expr, &mut qualifiers);
            let references_target = qualifiers
                .iter()
                .any(|qualifier| identifier_eq(qualifier, table_name));
            if !references_target
                || !expr_uses_only_qualified_table_columns(term.expr, table_name)
                || !table_local_index_probe_is_evaluable(term, table_name)
            {
                return None;
            }
            (column, term.expr)
        }
        WhereTermKind::Between
        | WhereTermKind::InList { .. }
        | WhereTermKind::LikePrefix { .. } => (
            Some(
                term.column
                    .clone()
                    .filter(|column| qualifier_matches(column))?,
            ),
            term.expr,
        ),
    };
    if matches!(
        term.kind,
        WhereTermKind::Equality | WhereTermKind::RowidEquality | WhereTermKind::Range
    ) && !probe_operand_uses_only_available_columns(probe_operand, available_outer_tables)
    {
        return None;
    }
    let remaining_probe_operands_available = match (&term.kind, term.expr) {
        (WhereTermKind::Between, Expr::Between { low, high, .. }) => {
            probe_operand_uses_only_available_columns(low, available_outer_tables)
                && probe_operand_uses_only_available_columns(high, available_outer_tables)
        }
        (WhereTermKind::InList { .. }, Expr::In { set, .. }) => match set {
            InSet::List(items) => {
                !items.is_empty()
                    && items.iter().all(|item| {
                        probe_operand_uses_only_available_columns(item, available_outer_tables)
                    })
            }
            InSet::Subquery(_) | InSet::Table(_) => false,
        },
        (
            WhereTermKind::InList { .. },
            Expr::BinaryOp {
                op: AstBinaryOp::Or,
                ..
            },
        ) => {
            let mut disjuncts = Vec::new();
            collect_disjuncts(term.expr, &mut disjuncts);
            disjuncts.iter().all(|disjunct| {
                qualified_comparison_probe_operand(disjunct, table_name).is_some_and(
                    |(_, operand)| {
                        probe_operand_uses_only_available_columns(operand, available_outer_tables)
                    },
                )
            })
        }
        (
            WhereTermKind::LikePrefix { .. },
            Expr::Like {
                pattern, escape, ..
            },
        ) => {
            probe_operand_uses_only_available_columns(pattern, available_outer_tables)
                && escape.as_deref().is_none_or(|escape| {
                    probe_operand_uses_only_available_columns(escape, available_outer_tables)
                })
        }
        (
            WhereTermKind::Between
            | WhereTermKind::InList { .. }
            | WhereTermKind::LikePrefix { .. },
            _,
        ) => false,
        _ => true,
    };
    if !remaining_probe_operands_available {
        return None;
    }
    let kind = match term.kind {
        WhereTermKind::Equality | WhereTermKind::RowidEquality => {
            if column.as_ref().is_some_and(is_rowid_column) {
                WhereTermKind::RowidEquality
            } else {
                WhereTermKind::Equality
            }
        }
        ref kind => kind.clone(),
    };
    Some(WhereTerm {
        expr: term.expr,
        column,
        kind,
    })
}

fn qualified_comparison_probe_operand<'expr>(
    expr: &'expr Expr,
    table_name: &str,
) -> Option<(WhereColumn, &'expr Expr)> {
    let Expr::BinaryOp { left, right, .. } = expr else {
        return None;
    };
    let qualifier_matches = |column: &WhereColumn| {
        column
            .table
            .as_deref()
            .is_some_and(|qualifier| identifier_eq(qualifier, table_name))
    };
    let left_column = extract_where_column(left);
    let right_column = extract_where_column(right);
    let left_matches = left_column.as_ref().is_some_and(&qualifier_matches);
    let right_matches = right_column.as_ref().is_some_and(&qualifier_matches);
    if left_matches && !right_matches {
        Some((left_column?, right.as_ref()))
    } else if right_matches && !left_matches {
        Some((right_column?, left.as_ref()))
    } else {
        // A same-table column-vs-column predicate cannot be probed before
        // reading that table, and two matching sides have no unambiguous outer
        // lookup operand.
        None
    }
}

/// Return whether a prospective index-probe operand is independent of the
/// candidate table and references only columns from tables already admitted to
/// the outer join prefix.
///
/// Subqueries and `RAISE` expressions fail closed: their binding scopes cannot
/// be proven from the planner's flat table-name list. Other expression forms
/// recurse through every operand so a nested bare or not-yet-available column
/// cannot masquerade as a constant probe.
fn probe_operand_uses_only_available_columns(
    expr: &Expr,
    available_outer_tables: &[String],
) -> bool {
    let column_available = |column: &ColumnRef| {
        column.table.as_deref().is_some_and(|qualifier| {
            available_outer_tables
                .iter()
                .any(|table| identifier_eq(table, qualifier))
        })
    };
    expr_columns_satisfy(expr, &column_available)
}

fn expr_uses_only_qualified_table_columns(expr: &Expr, table_name: &str) -> bool {
    expr_columns_satisfy(expr, &|column| {
        column
            .table
            .as_deref()
            .is_some_and(|qualifier| identifier_eq(qualifier, table_name))
    })
}

fn expr_uses_only_explicit_table_columns(expr: &Expr, table_name: &str) -> bool {
    let saw_column = std::cell::Cell::new(false);
    let columns_are_explicitly_local = expr_columns_satisfy(expr, &|column| {
        saw_column.set(true);
        column
            .table
            .as_deref()
            .is_some_and(|qualifier| identifier_eq(qualifier, table_name))
    });
    columns_are_explicitly_local && saw_column.get()
}

fn table_local_index_probe_is_evaluable(term: &WhereTerm<'_>, table_name: &str) -> bool {
    let has_no_columns = |expr: &Expr| expr_columns_satisfy(expr, &|_| false);
    let column_belongs_to_table = |column: &WhereColumn| {
        column
            .table
            .as_deref()
            .is_none_or(|qualifier| identifier_eq(qualifier, table_name))
    };

    match (&term.kind, term.expr) {
        (WhereTermKind::Equality | WhereTermKind::RowidEquality | WhereTermKind::Range, _) => {
            let Some(column) = term
                .column
                .as_ref()
                .filter(|column| column_belongs_to_table(column))
            else {
                return false;
            };
            comparison_operand_for_column(term.expr, table_name, &column.column)
                .is_some_and(&has_no_columns)
        }
        (WhereTermKind::Between, Expr::Between { low, high, .. }) => {
            term.column.as_ref().is_some_and(column_belongs_to_table)
                && has_no_columns(low)
                && has_no_columns(high)
        }
        (WhereTermKind::InList { .. }, Expr::In { set, .. }) => {
            term.column.as_ref().is_some_and(column_belongs_to_table)
                && match set {
                    InSet::List(items) => !items.is_empty() && items.iter().all(&has_no_columns),
                    InSet::Subquery(_) | InSet::Table(_) => false,
                }
        }
        (
            WhereTermKind::InList { .. },
            Expr::BinaryOp {
                op: AstBinaryOp::Or,
                ..
            },
        ) => {
            let Some(column) = term
                .column
                .as_ref()
                .filter(|column| column_belongs_to_table(column))
            else {
                return false;
            };
            let mut disjuncts = Vec::new();
            collect_disjuncts(term.expr, &mut disjuncts);
            disjuncts.iter().all(|disjunct| {
                comparison_operand_for_column(disjunct, table_name, &column.column)
                    .is_some_and(&has_no_columns)
            })
        }
        (
            WhereTermKind::LikePrefix { .. },
            Expr::Like {
                pattern, escape, ..
            },
        ) => {
            term.column.as_ref().is_some_and(column_belongs_to_table)
                && has_no_columns(pattern)
                && escape.as_deref().is_none_or(&has_no_columns)
        }
        (
            WhereTermKind::Other,
            Expr::BinaryOp {
                left,
                right,
                op:
                    AstBinaryOp::Eq
                    | AstBinaryOp::Lt
                    | AstBinaryOp::Le
                    | AstBinaryOp::Gt
                    | AstBinaryOp::Ge,
                ..
            },
        ) => {
            let left_is_key = expr_is_table_local_index_key(left, table_name);
            let right_is_key = expr_is_table_local_index_key(right, table_name);
            match (left_is_key, right_is_key) {
                (true, false) => has_no_columns(right),
                (false, true) => has_no_columns(left),
                (true, true) => false,
                // Constant expression-index keys are unusual but legal enough
                // that this seam must also prove both sides pre-scan
                // evaluable rather than assuming no table-local column means
                // no possible key match.
                (false, false) => has_no_columns(left) && has_no_columns(right),
            }
        }
        (
            WhereTermKind::Other,
            Expr::Between {
                expr,
                low,
                high,
                not: false,
                ..
            },
        ) => {
            (expr_is_table_local_index_key(expr, table_name) || has_no_columns(expr))
                && has_no_columns(low)
                && has_no_columns(high)
        }
        (WhereTermKind::Other, _) => true,
        _ => false,
    }
}

/// Return whether `term` can drive an access path before scanning `table_name`.
///
/// Ordinary index probes may use the table name or no qualifier.  A rowid alias
/// may instead be exposed through a query alias, so retain that equality only
/// when its opposite operand is scalar and the alias hint matches exactly.
fn table_local_access_path_probe_is_evaluable(
    term: &WhereTerm<'_>,
    table_name: &str,
    rowid_alias_hints: &[RowidAliasHint],
) -> bool {
    if table_local_index_probe_is_evaluable(term, table_name) {
        return true;
    }

    let has_no_columns = |expr: &Expr| expr_columns_satisfy(expr, &|_| false);
    matches!(
        term.kind,
        WhereTermKind::Equality | WhereTermKind::RowidEquality
    ) && term.column.as_ref().is_some_and(|column| {
        rowid_alias_hints
            .iter()
            .any(|hint| hint.matches_column(table_name, column))
            && comparison_operand_for_where_column(term.expr, column).is_some_and(&has_no_columns)
    })
}

fn expr_is_table_local_index_key(expr: &Expr, table_name: &str) -> bool {
    let saw_column = std::cell::Cell::new(false);
    let columns_are_local = expr_columns_satisfy(expr, &|column| {
        saw_column.set(true);
        column
            .table
            .as_deref()
            .is_none_or(|qualifier| identifier_eq(qualifier, table_name))
    });
    columns_are_local && saw_column.get()
}

fn expr_columns_satisfy(expr: &Expr, column_allowed: &impl Fn(&ColumnRef) -> bool) -> bool {
    match expr {
        Expr::Literal(..) | Expr::BoundOuterValue { .. } | Expr::Placeholder(..) => true,
        Expr::Column(column, _) => column_allowed(column),
        Expr::BinaryOp { left, right, .. }
        | Expr::JsonAccess {
            expr: left,
            path: right,
            ..
        } => {
            expr_columns_satisfy(left, column_allowed)
                && expr_columns_satisfy(right, column_allowed)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::IsNull { expr, .. } => expr_columns_satisfy(expr, column_allowed),
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_columns_satisfy(expr, column_allowed)
                && expr_columns_satisfy(low, column_allowed)
                && expr_columns_satisfy(high, column_allowed)
        }
        Expr::In { expr, set, .. } => {
            expr_columns_satisfy(expr, column_allowed)
                && match set {
                    InSet::List(items) => items
                        .iter()
                        .all(|item| expr_columns_satisfy(item, column_allowed)),
                    InSet::Subquery(_) | InSet::Table(_) => false,
                }
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_columns_satisfy(expr, column_allowed)
                && expr_columns_satisfy(pattern, column_allowed)
                && escape
                    .as_deref()
                    .is_none_or(|escape| expr_columns_satisfy(escape, column_allowed))
        }
        Expr::Case {
            operand,
            whens,
            else_expr,
            ..
        } => {
            operand
                .as_deref()
                .is_none_or(|operand| expr_columns_satisfy(operand, column_allowed))
                && whens.iter().all(|(when, then)| {
                    expr_columns_satisfy(when, column_allowed)
                        && expr_columns_satisfy(then, column_allowed)
                })
                && else_expr
                    .as_deref()
                    .is_none_or(|else_expr| expr_columns_satisfy(else_expr, column_allowed))
        }
        Expr::FunctionCall {
            args,
            order_by,
            filter,
            over,
            ..
        } => {
            let args_available = match args {
                fsqlite_ast::FunctionArgs::Star => true,
                fsqlite_ast::FunctionArgs::List(args) => args
                    .iter()
                    .all(|arg| expr_columns_satisfy(arg, column_allowed)),
            };
            args_available
                && order_by.iter().all(|term| {
                    expr_columns_satisfy(&term.expr, column_allowed)
                })
                && filter
                    .as_deref()
                    .is_none_or(|filter| expr_columns_satisfy(filter, column_allowed))
                // Window functions are not legal in WHERE comparisons, and
                // named/base-window scope cannot be resolved at this seam.
                && over.is_none()
        }
        Expr::RowValue(items, _) => items
            .iter()
            .all(|item| expr_columns_satisfy(item, column_allowed)),
        Expr::Exists { .. } | Expr::Subquery(..) | Expr::Raise { .. } => false,
    }
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
            morsel_eligibility: None,
        };
    }

    if n == 1 {
        let ap = join_access_path(
            &tables[0],
            indexes,
            where_terms,
            needed_columns,
            JoinAccessPathContext {
                table_index_hints,
                cracking_hints: cracking_hints.as_deref(),
                available_outer_tables: &[],
                unqualified_terms_are_table_local: true,
            },
        );
        // Move the access path into the plan rather than cloning it (its cost
        // is a Copy f64, captured first), so the single owned AccessPath — which
        // carries two heap Strings — is not duplicated on this dominant
        // single-table planning path.
        let total_cost = ap.estimated_cost;
        let plan = QueryPlan {
            join_order: vec![tables[0].name.clone()],
            access_paths: vec![ap],
            join_segments: vec![],
            total_cost,
            morsel_eligibility: None,
        };
        if let Some(store) = cracking_hints {
            for access_path in &plan.access_paths {
                store.record_access_path(access_path);
            }
        }
        FSQLITE_PLANNER_PLANS_ENUMERATED.fetch_add(1, Ordering::Relaxed);
        return plan;
    }

    if feature_flags.dpccp_join && n <= DPCCP_MAX_TABLES {
        if let Some(DpccpPlan {
            order: order_indices,
            access_paths,
            total_cost,
            plans_enumerated: plans_counted,
            branches_pruned,
        }) = dpccp_order_joins(
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
            // `access_paths` are the exact paths the search costed against their
            // real outer prefixes, returned alongside the order that won. They
            // are emitted verbatim rather than re-derived, so `total_cost` always
            // describes the plan this function actually returns.
            let join_segments =
                choose_join_segments(&join_order, tables, where_terms, None, feature_flags);
            let plan = QueryPlan {
                join_order,
                access_paths,
                join_segments,
                total_cost,
                morsel_eligibility: None,
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
            JoinAccessPathContext {
                table_index_hints,
                cracking_hints: cracking_hints.as_deref(),
                available_outer_tables: &[],
                unqualified_terms_are_table_local: false,
            },
        );
        let cumulative_rows = ap.estimated_rows;
        let cost = ap.estimated_cost;
        paths.push(PartialPath {
            tables: vec![t.name.clone()],
            access_paths: vec![ap],
            cost,
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
                    JoinAccessPathContext {
                        table_index_hints,
                        cracking_hints: cracking_hints.as_deref(),
                        available_outer_tables: &path.tables,
                        unqualified_terms_are_table_local: false,
                    },
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
                JoinAccessPathContext {
                    table_index_hints,
                    cracking_hints: cracking_hints.as_deref(),
                    available_outer_tables: &[],
                    unqualified_terms_are_table_local: false,
                },
            );
            let cost = ap.estimated_cost;
            let cumulative_rows = ap.estimated_rows;
            paths.push(PartialPath {
                tables: vec![t.name.clone()],
                access_paths: vec![ap],
                cost,
                cumulative_rows,
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
        morsel_eligibility: None,
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
) -> Option<DpccpPlan> {
    let n = tables.len();
    assert!(n <= DPCCP_MAX_TABLES);

    // Order-independent seeds. Used solely to derive a deterministic
    // `visit_order`; the search costs every candidate against its real outer
    // prefix and never reads these as a cost or bound.
    let seed_paths = tables
        .iter()
        .map(|table| {
            join_access_path(
                table,
                indexes,
                where_terms,
                needed_columns,
                JoinAccessPathContext {
                    table_index_hints,
                    cracking_hints,
                    available_outer_tables: &[],
                    unqualified_terms_are_table_local: false,
                },
            )
        })
        .collect::<Vec<_>>();

    let mut visit_order = (0..n).collect::<Vec<_>>();
    visit_order.sort_by(|&lhs, &rhs| {
        seed_paths[lhs]
            .estimated_rows
            .partial_cmp(&seed_paths[rhs].estimated_rows)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                seed_paths[lhs]
                    .estimated_cost
                    .partial_cmp(&seed_paths[rhs].estimated_cost)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| lhs.cmp(&rhs))
    });

    let mut state = ExhaustiveJoinSearchState::new(
        tables,
        &visit_order,
        cross_join_pairs,
        JoinCostingInputs {
            indexes,
            where_terms,
            needed_columns,
            table_index_hints,
            cracking_hints,
        },
    );
    state.search();

    let order = state.best_order?;
    let access_paths = state
        .best_access_paths
        .expect("best_order implies best_access_paths were captured together");

    Some(DpccpPlan {
        order,
        access_paths,
        total_cost: state.best_cost,
        plans_enumerated: state.plans_enumerated,
        branches_pruned: state.branches_pruned,
    })
}

/// Winning exhaustive-search plan: the order, the access paths that were costed
/// against their real outer prefixes to produce `total_cost`, and the search
/// counters. Carried as a named struct so the order and the paths that justify
/// its cost cannot drift apart at a call site.
struct DpccpPlan {
    order: Vec<usize>,
    access_paths: Vec<AccessPath>,
    total_cost: f64,
    plans_enumerated: u64,
    branches_pruned: u64,
}

/// Inputs `join_access_path` needs to cost a candidate. Grouped so the search
/// state carries one borrow bundle instead of five parallel parameters.
#[derive(Clone, Copy)]
struct JoinCostingInputs<'a, 'w> {
    indexes: &'a [IndexInfo],
    where_terms: &'a [WhereTerm<'w>],
    needed_columns: Option<&'a [String]>,
    table_index_hints: Option<&'a BTreeMap<String, IndexHint>>,
    cracking_hints: Option<&'a CrackingHintStore>,
}

struct ExhaustiveJoinSearchState<'a, 'w> {
    tables: &'a [TableStats],
    /// Precomputed by the caller from order-independent seed paths. The seeds
    /// themselves are deliberately *not* held here: a path costed with no outer
    /// tables is only an upper bound on what the same relation costs once a
    /// probe becomes admissible, so it must never be reachable as a cost or a
    /// pruning bound from inside the search.
    visit_order: &'a [usize],
    cross_join_pairs: &'a [(String, String)],
    indexes: &'a [IndexInfo],
    where_terms: &'a [WhereTerm<'w>],
    needed_columns: Option<&'a [String]>,
    table_index_hints: Option<&'a BTreeMap<String, IndexHint>>,
    cracking_hints: Option<&'a CrackingHintStore>,
    /// Access paths keyed by `(outer_set, candidate)` as
    /// `used_mask * tables.len() + candidate_idx`. `join_access_path` consults
    /// `available_outer_tables` only for membership, so the path depends on the
    /// outer *set* and not on the order that produced it. Keying by the mask is
    /// therefore exact and costs each distinct pair once, which keeps the search
    /// at O(2^n * n) costings instead of one per enumerated permutation.
    path_cache: Vec<Option<AccessPath>>,
    best_order: Option<Vec<usize>>,
    best_access_paths: Option<Vec<AccessPath>>,
    best_cost: f64,
    plans_enumerated: u64,
    branches_pruned: u64,
}

impl<'a, 'w> ExhaustiveJoinSearchState<'a, 'w> {
    fn new(
        tables: &'a [TableStats],
        visit_order: &'a [usize],
        cross_join_pairs: &'a [(String, String)],
        costing: JoinCostingInputs<'a, 'w>,
    ) -> Self {
        let n = tables.len();
        Self {
            tables,
            visit_order,
            cross_join_pairs,
            indexes: costing.indexes,
            where_terms: costing.where_terms,
            needed_columns: costing.needed_columns,
            table_index_hints: costing.table_index_hints,
            cracking_hints: costing.cracking_hints,
            path_cache: vec![None; (1usize << n) * n],
            best_order: None,
            best_access_paths: None,
            best_cost: f64::INFINITY,
            plans_enumerated: 0,
            branches_pruned: 0,
        }
    }

    /// Slot index for the `(outer_set, candidate)` pair.
    fn path_cache_slot(&self, used_mask: u64, candidate_idx: usize) -> usize {
        (used_mask as usize) * self.tables.len() + candidate_idx
    }

    /// Ensure the `(outer_set, candidate)` pair is costed, returning only its
    /// scalar cost and row estimate.
    ///
    /// Deliberately does **not** hand back an owned `AccessPath`: the caller
    /// needs the scalars to run the branch-and-bound test, and a pruned branch
    /// must not pay for cloning a path (two heap `String`s plus a probe) that is
    /// then discarded. The clone happens once, only on the descending branch.
    fn ensure_costed(
        &mut self,
        used_mask: u64,
        candidate_idx: usize,
        prefix_names: &[String],
    ) -> (f64, f64) {
        let slot = self.path_cache_slot(used_mask, candidate_idx);
        if self.path_cache[slot].is_none() {
            let access_path = join_access_path(
                &self.tables[candidate_idx],
                self.indexes,
                self.where_terms,
                self.needed_columns,
                JoinAccessPathContext {
                    table_index_hints: self.table_index_hints,
                    cracking_hints: self.cracking_hints,
                    available_outer_tables: prefix_names,
                    unqualified_terms_are_table_local: false,
                },
            );
            self.path_cache[slot] = Some(access_path);
        }
        let cached = self.path_cache[slot]
            .as_ref()
            .expect("slot was just populated");
        (cached.estimated_cost, cached.estimated_rows)
    }

    fn search(&mut self) {
        let mut current_order = Vec::with_capacity(self.tables.len());
        let mut current_paths = Vec::with_capacity(self.tables.len());
        let mut prefix_names = Vec::with_capacity(self.tables.len());
        self.search_dfs(
            &mut current_order,
            &mut current_paths,
            &mut prefix_names,
            0,
            0.0,
            1.0,
        );
    }

    fn search_dfs(
        &mut self,
        current_order: &mut Vec<usize>,
        current_paths: &mut Vec<AccessPath>,
        prefix_names: &mut Vec<String>,
        used_mask: u64,
        current_cost: f64,
        current_rows: f64,
    ) {
        if current_order.len() == self.tables.len() {
            if current_cost < self.best_cost {
                self.best_cost = current_cost;
                self.best_order = Some(current_order.clone());
                // Capture the paths that produced this cost. Emitting these
                // verbatim is what keeps `total_cost` describing the plan the
                // caller actually carries.
                self.best_access_paths = Some(current_paths.clone());
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

            if !cross_join_allowed_indices(
                current_order,
                &self.tables[candidate_idx].name,
                self.tables,
                self.cross_join_pairs,
            ) {
                continue;
            }

            // Cost this candidate against the tables already joined, so an
            // equality probe onto an outer column is admitted exactly when that
            // outer table is present in the prefix.
            let (candidate_cost, candidate_rows) =
                self.ensure_costed(used_mask, candidate_idx, prefix_names);
            let (new_cost, new_rows) = if current_order.is_empty() {
                (candidate_cost, candidate_rows)
            } else {
                let inner_cost = candidate_cost * current_rows;
                (current_cost + inner_cost, current_rows * candidate_rows)
            };

            self.plans_enumerated += 1;
            let should_prune = self.best_cost.is_finite() && new_cost >= self.best_cost;

            // Borrowed view for tracing only: no `String` is cloned per visit.
            let mut candidate_order = prefix_names.iter().map(String::as_str).collect::<Vec<_>>();
            candidate_order.push(self.tables[candidate_idx].name.as_str());

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

            // Only the descending branch materializes the path.
            let slot = self.path_cache_slot(used_mask, candidate_idx);
            let descend_path = self.path_cache[slot]
                .as_ref()
                .expect("candidate was costed above")
                .clone();
            current_order.push(candidate_idx);
            prefix_names.push(self.tables[candidate_idx].name.clone());
            current_paths.push(descend_path);
            self.search_dfs(
                current_order,
                current_paths,
                prefix_names,
                used_mask | (1u64 << candidate_idx),
                new_cost,
                new_rows,
            );
            current_paths.pop();
            prefix_names.pop();
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

/// Collect all distinct table qualifiers referenced by column expressions
/// within an AST node.  Used to determine whether a predicate is a
/// single-table filter (pushable) or a cross-table join condition (not
/// pushable).
fn collect_table_refs(expr: &Expr, out: &mut HashSet<String>) {
    match expr {
        Expr::Column(col_ref, _) => {
            if let Some(ref tq) = col_ref.table {
                out.insert(tq.to_ascii_lowercase());
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_table_refs(left, out);
            collect_table_refs(right, out);
        }
        Expr::UnaryOp { expr: inner, .. }
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull { expr: inner, .. } => {
            collect_table_refs(inner, out);
        }
        Expr::Between {
            expr: e, low, high, ..
        } => {
            collect_table_refs(e, out);
            collect_table_refs(low, out);
            collect_table_refs(high, out);
        }
        Expr::In { expr: e, set, .. } => {
            collect_table_refs(e, out);
            if let InSet::List(items) = set {
                for item in items {
                    collect_table_refs(item, out);
                }
            }
        }
        Expr::Like {
            expr: e,
            pattern,
            escape,
            ..
        } => {
            collect_table_refs(e, out);
            collect_table_refs(pattern, out);
            if let Some(esc) = escape {
                collect_table_refs(esc, out);
            }
        }
        Expr::FunctionCall { args, filter, .. } => {
            if let fsqlite_ast::FunctionArgs::List(exprs) = args {
                for arg in exprs {
                    collect_table_refs(arg, out);
                }
            }
            if let Some(f) = filter {
                collect_table_refs(f, out);
            }
        }
        Expr::Case {
            operand,
            whens,
            else_expr,
            ..
        } => {
            if let Some(op) = operand {
                collect_table_refs(op, out);
            }
            for (when_e, then_e) in whens {
                collect_table_refs(when_e, out);
                collect_table_refs(then_e, out);
            }
            if let Some(el) = else_expr {
                collect_table_refs(el, out);
            }
        }
        Expr::Cast { expr: e, .. } => collect_table_refs(e, out),
        Expr::JsonAccess { expr: e, path, .. } => {
            collect_table_refs(e, out);
            collect_table_refs(path, out);
        }
        Expr::RowValue(exprs, _) => {
            for e in exprs {
                collect_table_refs(e, out);
            }
        }
        Expr::Exists { subquery, .. } | Expr::Subquery(subquery, _) => {
            // Recurse into the subquery's WHERE clause and select list to
            // find outer table references (correlated subquery columns).
            if let SelectCore::Select {
                where_clause,
                columns,
                ..
            } = &subquery.body.select
            {
                if let Some(wc) = where_clause {
                    collect_table_refs(wc, out);
                }
                for col in columns {
                    if let ResultColumn::Expr { expr, .. } = col {
                        collect_table_refs(expr, out);
                    }
                }
            }
        }
        // Constant leaves and parser placeholders contain no column refs.
        Expr::Literal(..)
        | Expr::BoundOuterValue { .. }
        | Expr::Placeholder(..)
        | Expr::Raise { .. } => {}
    }
}

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
        // Collect all table qualifiers referenced anywhere in the expression.
        // A predicate is only pushable if it references at most one table;
        // cross-table predicates (join conditions) must remain as join filters.
        let mut refs = HashSet::new();
        collect_table_refs(term.expr, &mut refs);

        if refs.len() == 1 {
            // Single qualified table — push to that table.
            let tq = refs.into_iter().next().unwrap();
            let matching: Vec<_> = table_names
                .iter()
                .filter(|t| t.to_ascii_lowercase() == tq)
                .collect();
            if matching.len() == 1 {
                pushed.push(PushedPredicate {
                    table: matching[0].clone(),
                    term,
                });
                continue;
            }
        } else if refs.is_empty() {
            // No table qualifiers (unqualified columns or pure literals in the RHS).
            if let Some(ref col) = term.column {
                if let Some(ref tname) = col.table {
                    if let Some(matched) =
                        table_names.iter().find(|t| t.eq_ignore_ascii_case(tname))
                    {
                        pushed.push(PushedPredicate {
                            table: matched.clone(),
                            term,
                        });
                        continue;
                    }
                } else if table_names.len() == 1 {
                    pushed.push(PushedPredicate {
                        table: table_names[0].clone(),
                        term,
                    });
                    continue;
                }
            }
        }
        // Multi-table references or ambiguous — keep as join condition.
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
    /// Expression is non-constant or cannot be folded safely.
    NotConstant,
}

struct FoldStack<T, const N: usize> {
    inline: [Option<T>; N],
    inline_len: usize,
    spill: Vec<T>,
}

impl<T, const N: usize> FoldStack<T, N> {
    fn new() -> Self {
        Self {
            inline: [const { None }; N],
            inline_len: 0,
            spill: Vec::new(),
        }
    }

    fn push(&mut self, value: T) {
        if self.inline_len < N && self.spill.is_empty() {
            self.inline[self.inline_len] = Some(value);
            self.inline_len += 1;
        } else {
            self.spill.push(value);
        }
    }

    fn pop(&mut self) -> Option<T> {
        if let Some(value) = self.spill.pop() {
            return Some(value);
        }
        if self.inline_len == 0 {
            return None;
        }
        self.inline_len -= 1;
        self.inline[self.inline_len].take()
    }

    fn len(&self) -> usize {
        self.inline_len.saturating_add(self.spill.len())
    }
}

/// Attempt to constant-fold an expression.
///
/// Evaluates expressions that contain only literals and deterministic operators
/// at plan time, avoiding repeated evaluation during execution.
pub fn try_constant_fold(expr: &Expr) -> FoldResult {
    enum FoldTask<'a> {
        Visit(&'a Expr),
        ApplyUnary(fsqlite_ast::UnaryOp),
        ApplyBinary(AstBinaryOp),
    }

    let mut tasks = FoldStack::<_, 16>::new();
    tasks.push(FoldTask::Visit(expr));
    let mut values = FoldStack::<_, 16>::new();
    while let Some(task) = tasks.pop() {
        match task {
            FoldTask::Visit(Expr::Literal(literal, _)) => {
                values.push(FoldResult::Literal(literal.clone()));
            }
            FoldTask::Visit(Expr::UnaryOp {
                op, expr: inner, ..
            }) => {
                tasks.push(FoldTask::ApplyUnary(*op));
                tasks.push(FoldTask::Visit(inner));
            }
            FoldTask::Visit(Expr::BinaryOp {
                left, op, right, ..
            }) => {
                tasks.push(FoldTask::ApplyBinary(*op));
                tasks.push(FoldTask::Visit(right));
                tasks.push(FoldTask::Visit(left));
            }
            FoldTask::Visit(_) => values.push(FoldResult::NotConstant),
            FoldTask::ApplyUnary(op) => {
                let Some(value) = values.pop() else {
                    return FoldResult::NotConstant;
                };
                values.push(fold_unary_literal(op, value));
            }
            FoldTask::ApplyBinary(op) => {
                let Some(right) = values.pop() else {
                    return FoldResult::NotConstant;
                };
                let Some(left) = values.pop() else {
                    return FoldResult::NotConstant;
                };
                values.push(fold_binary_literals(op, left, right));
            }
        }
    }

    if values.len() == 1 {
        values.pop().unwrap_or(FoldResult::NotConstant)
    } else {
        FoldResult::NotConstant
    }
}

#[derive(Clone, Copy)]
enum FoldNumeric {
    Integer(i64),
    Real(f64),
}

#[derive(Clone, Copy)]
enum FoldTruth {
    False,
    True,
    Null,
}

fn boolean_literal(value: bool) -> Literal {
    if value { Literal::True } else { Literal::False }
}

fn normalized_float_literal(value: f64) -> Literal {
    if value.is_nan() {
        Literal::Null
    } else {
        Literal::Float(value)
    }
}

fn fold_numeric(literal: &Literal) -> Option<FoldNumeric> {
    match literal {
        Literal::Integer(value) => Some(FoldNumeric::Integer(*value)),
        Literal::Float(value) if !value.is_nan() => Some(FoldNumeric::Real(*value)),
        Literal::True => Some(FoldNumeric::Integer(1)),
        Literal::False => Some(FoldNumeric::Integer(0)),
        _ => None,
    }
}

fn fold_truth(literal: &Literal) -> Option<FoldTruth> {
    match literal {
        Literal::Null => Some(FoldTruth::Null),
        Literal::Integer(value) => Some(if *value == 0 {
            FoldTruth::False
        } else {
            FoldTruth::True
        }),
        Literal::Float(value) if value.is_nan() => Some(FoldTruth::Null),
        Literal::Float(value) => Some(if *value == 0.0 {
            FoldTruth::False
        } else {
            FoldTruth::True
        }),
        Literal::True => Some(FoldTruth::True),
        Literal::False => Some(FoldTruth::False),
        _ => None,
    }
}

fn fold_unary_literal(op: fsqlite_ast::UnaryOp, value: FoldResult) -> FoldResult {
    let FoldResult::Literal(literal) = value else {
        return FoldResult::NotConstant;
    };
    if matches!(&literal, Literal::Null)
        || matches!(&literal, Literal::Float(value) if value.is_nan())
    {
        return FoldResult::Literal(Literal::Null);
    }

    match (op, fold_numeric(&literal)) {
        (fsqlite_ast::UnaryOp::Negate, Some(FoldNumeric::Integer(value))) => {
            FoldResult::Literal(value.checked_neg().map_or_else(
                || normalized_float_literal(-(value as f64)),
                Literal::Integer,
            ))
        }
        (fsqlite_ast::UnaryOp::Negate, Some(FoldNumeric::Real(value))) => {
            FoldResult::Literal(normalized_float_literal(-value))
        }
        (fsqlite_ast::UnaryOp::Plus, Some(FoldNumeric::Integer(value))) => {
            FoldResult::Literal(Literal::Integer(value))
        }
        (fsqlite_ast::UnaryOp::Plus, Some(FoldNumeric::Real(value))) => {
            FoldResult::Literal(normalized_float_literal(value))
        }
        (fsqlite_ast::UnaryOp::BitNot, Some(FoldNumeric::Integer(value))) => {
            FoldResult::Literal(Literal::Integer(!value))
        }
        (fsqlite_ast::UnaryOp::Not, _) => {
            fold_truth(&literal).map_or(FoldResult::NotConstant, |truth| match truth {
                FoldTruth::False => FoldResult::Literal(Literal::True),
                FoldTruth::True => FoldResult::Literal(Literal::False),
                FoldTruth::Null => FoldResult::Literal(Literal::Null),
            })
        }
        _ => FoldResult::NotConstant,
    }
}

fn compare_integer_real(integer: i64, real: f64) -> Option<std::cmp::Ordering> {
    if real.is_nan() {
        return None;
    }
    const TWO_TO_63: f64 = 9_223_372_036_854_775_808.0;
    if real >= TWO_TO_63 {
        return Some(std::cmp::Ordering::Less);
    }
    if real < -TWO_TO_63 {
        return Some(std::cmp::Ordering::Greater);
    }

    let truncated = real as i64;
    match integer.cmp(&truncated) {
        std::cmp::Ordering::Equal => (integer as f64).partial_cmp(&real),
        ordering => Some(ordering),
    }
}

fn compare_numeric(left: FoldNumeric, right: FoldNumeric) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (FoldNumeric::Integer(left), FoldNumeric::Integer(right)) => Some(left.cmp(&right)),
        (FoldNumeric::Real(left), FoldNumeric::Real(right)) => left.partial_cmp(&right),
        (FoldNumeric::Integer(left), FoldNumeric::Real(right)) => compare_integer_real(left, right),
        (FoldNumeric::Real(left), FoldNumeric::Integer(right)) => {
            compare_integer_real(right, left).map(std::cmp::Ordering::reverse)
        }
    }
}

fn sqlite_is_equal(left: &Literal, right: &Literal) -> Option<bool> {
    let left_is_null =
        matches!(left, Literal::Null) || matches!(left, Literal::Float(value) if value.is_nan());
    let right_is_null =
        matches!(right, Literal::Null) || matches!(right, Literal::Float(value) if value.is_nan());
    if left_is_null || right_is_null {
        return Some(left_is_null && right_is_null);
    }

    if let (Some(left), Some(right)) = (fold_numeric(left), fold_numeric(right)) {
        return compare_numeric(left, right).map(std::cmp::Ordering::is_eq);
    }

    match (left, right) {
        (Literal::String(left), Literal::String(right)) => Some(left == right),
        (Literal::Blob(left), Literal::Blob(right)) => Some(left == right),
        (Literal::CurrentTime | Literal::CurrentDate | Literal::CurrentTimestamp, _)
        | (_, Literal::CurrentTime | Literal::CurrentDate | Literal::CurrentTimestamp) => None,
        _ => Some(false),
    }
}

fn sqlite_is_result(left: &Literal, right: &Literal) -> Option<bool> {
    match right {
        Literal::True => fold_truth(left).map(|truth| matches!(truth, FoldTruth::True)),
        Literal::False => fold_truth(left).map(|truth| matches!(truth, FoldTruth::False)),
        _ => sqlite_is_equal(left, right),
    }
}

fn fold_logical(op: AstBinaryOp, left: &Literal, right: &Literal) -> FoldResult {
    let (Some(left), Some(right)) = (fold_truth(left), fold_truth(right)) else {
        return FoldResult::NotConstant;
    };
    let truth = match op {
        AstBinaryOp::And => match (left, right) {
            (FoldTruth::False, _) | (_, FoldTruth::False) => FoldTruth::False,
            (FoldTruth::Null, _) | (_, FoldTruth::Null) => FoldTruth::Null,
            (FoldTruth::True, FoldTruth::True) => FoldTruth::True,
        },
        AstBinaryOp::Or => match (left, right) {
            (FoldTruth::True, _) | (_, FoldTruth::True) => FoldTruth::True,
            (FoldTruth::Null, _) | (_, FoldTruth::Null) => FoldTruth::Null,
            (FoldTruth::False, FoldTruth::False) => FoldTruth::False,
        },
        _ => return FoldResult::NotConstant,
    };
    FoldResult::Literal(match truth {
        FoldTruth::False => Literal::False,
        FoldTruth::True => Literal::True,
        FoldTruth::Null => Literal::Null,
    })
}

fn fold_numeric_binary(op: AstBinaryOp, left: FoldNumeric, right: FoldNumeric) -> FoldResult {
    match (left, right) {
        (FoldNumeric::Integer(left), FoldNumeric::Integer(right)) => match op {
            AstBinaryOp::Add => FoldResult::Literal(left.checked_add(right).map_or_else(
                || normalized_float_literal((left as f64) + (right as f64)),
                Literal::Integer,
            )),
            AstBinaryOp::Subtract => FoldResult::Literal(left.checked_sub(right).map_or_else(
                || normalized_float_literal((left as f64) - (right as f64)),
                Literal::Integer,
            )),
            AstBinaryOp::Multiply => FoldResult::Literal(left.checked_mul(right).map_or_else(
                || normalized_float_literal((left as f64) * (right as f64)),
                Literal::Integer,
            )),
            AstBinaryOp::Divide | AstBinaryOp::Modulo if right == 0 => {
                FoldResult::Literal(Literal::Null)
            }
            AstBinaryOp::Divide => FoldResult::Literal(left.checked_div(right).map_or_else(
                || normalized_float_literal((left as f64) / (right as f64)),
                Literal::Integer,
            )),
            AstBinaryOp::Modulo => {
                FoldResult::Literal(Literal::Integer(left.checked_rem(right).unwrap_or(0)))
            }
            AstBinaryOp::Eq
            | AstBinaryOp::Ne
            | AstBinaryOp::Lt
            | AstBinaryOp::Le
            | AstBinaryOp::Gt
            | AstBinaryOp::Ge => fold_ordering_comparison(op, Some(left.cmp(&right))),
            _ => FoldResult::NotConstant,
        },
        (left, right) => match op {
            AstBinaryOp::Add => {
                FoldResult::Literal(normalized_float_literal(as_real(left) + as_real(right)))
            }
            AstBinaryOp::Subtract => {
                FoldResult::Literal(normalized_float_literal(as_real(left) - as_real(right)))
            }
            AstBinaryOp::Multiply => {
                FoldResult::Literal(normalized_float_literal(as_real(left) * as_real(right)))
            }
            AstBinaryOp::Divide if as_real(right) == 0.0 => FoldResult::Literal(Literal::Null),
            AstBinaryOp::Divide => {
                FoldResult::Literal(normalized_float_literal(as_real(left) / as_real(right)))
            }
            AstBinaryOp::Eq
            | AstBinaryOp::Ne
            | AstBinaryOp::Lt
            | AstBinaryOp::Le
            | AstBinaryOp::Gt
            | AstBinaryOp::Ge => fold_ordering_comparison(op, compare_numeric(left, right)),
            _ => FoldResult::NotConstant,
        },
    }
}

fn as_real(value: FoldNumeric) -> f64 {
    match value {
        FoldNumeric::Integer(value) => value as f64,
        FoldNumeric::Real(value) => value,
    }
}

fn fold_ordering_comparison(op: AstBinaryOp, ordering: Option<std::cmp::Ordering>) -> FoldResult {
    let Some(ordering) = ordering else {
        return FoldResult::Literal(Literal::Null);
    };
    let value = match op {
        AstBinaryOp::Eq => ordering.is_eq(),
        AstBinaryOp::Ne => !ordering.is_eq(),
        AstBinaryOp::Lt => ordering.is_lt(),
        AstBinaryOp::Le => ordering.is_le(),
        AstBinaryOp::Gt => ordering.is_gt(),
        AstBinaryOp::Ge => ordering.is_ge(),
        _ => return FoldResult::NotConstant,
    };
    FoldResult::Literal(boolean_literal(value))
}

fn fold_binary_literals(op: AstBinaryOp, left: FoldResult, right: FoldResult) -> FoldResult {
    let (FoldResult::Literal(left), FoldResult::Literal(right)) = (left, right) else {
        return FoldResult::NotConstant;
    };

    match op {
        AstBinaryOp::Is | AstBinaryOp::IsNot => {
            let Some(equal) = sqlite_is_result(&left, &right) else {
                return FoldResult::NotConstant;
            };
            return FoldResult::Literal(boolean_literal(if op == AstBinaryOp::Is {
                equal
            } else {
                !equal
            }));
        }
        AstBinaryOp::And | AstBinaryOp::Or => return fold_logical(op, &left, &right),
        _ => {}
    }

    if matches!(&left, Literal::Null)
        || matches!(&right, Literal::Null)
        || matches!(&left, Literal::Float(value) if value.is_nan())
        || matches!(&right, Literal::Float(value) if value.is_nan())
    {
        return FoldResult::Literal(Literal::Null);
    }

    if let (Some(left), Some(right)) = (fold_numeric(&left), fold_numeric(&right)) {
        fold_numeric_binary(op, left, right)
    } else {
        FoldResult::NotConstant
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_ast::{
        BoundCollation, ColumnRef, CompoundOp, Distinctness, Expr, FromClause, InSet, IndexHint,
        Literal, OrderingTerm, QualifiedName, ResultColumn, SelectBody, SelectCore,
        SelectStatement, SortDirection, Span, TableOrSubquery,
    };
    use std::{cell::Cell, path::PathBuf, time::Instant};

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
                    time_travel: None,
                },
                joins: vec![],
            }),
            where_clause: None,
            group_by: vec![],
            having: None,
            windows: vec![],
        }
    }

    fn sample_cached_query_plan(label: &str) -> QueryPlan {
        QueryPlan {
            join_order: vec![label.to_owned()],
            access_paths: vec![],
            join_segments: vec![],
            total_cost: label.len() as f64,
            morsel_eligibility: None,
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
            vec![ResultColumn::TableStar(QualifiedName::bare("tt"))],
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
                    expr: Expr::Column(ColumnRef::qualified("tt", "_rowid_"), Span::ZERO),
                    alias: None,
                },
                ResultColumn::Expr {
                    expr: Expr::Column(ColumnRef::qualified("t", "oid"), Span::ZERO),
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
    fn test_single_table_projection_rejects_hidden_rowid_aliases_when_disabled() {
        let core = select_core_single_table(
            vec![
                ResultColumn::Expr {
                    expr: Expr::Column(ColumnRef::bare("rowid"), Span::ZERO),
                    alias: None,
                },
                ResultColumn::Expr {
                    expr: Expr::Column(ColumnRef::qualified("tt", "_rowid_"), Span::ZERO),
                    alias: None,
                },
            ],
            "t",
            Some("tt"),
        );
        let table_columns = vec!["a".to_owned(), "b".to_owned()];
        let err = resolve_single_table_result_columns_with_options(&core, &table_columns, false)
            .expect_err("WITHOUT ROWID tables should reject hidden rowid aliases");
        assert_eq!(
            err,
            SingleTableProjectionError::ColumnNotFound {
                column: "rowid".to_owned()
            }
        );
    }

    #[test]
    fn test_single_table_projection_still_accepts_visible_rowid_column_when_disabled() {
        let core = select_core_single_table(
            vec![ResultColumn::Expr {
                expr: Expr::Column(ColumnRef::bare("rowid"), Span::ZERO),
                alias: None,
            }],
            "t",
            None,
        );
        let table_columns = vec!["rowid".to_owned(), "payload".to_owned()];
        let resolved =
            resolve_single_table_result_columns_with_options(&core, &table_columns, false)
                .expect("visible rowid-named columns should still resolve");
        assert_eq!(resolved.len(), 1);
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
    fn test_extract_output_aliases_and_count_output_columns() {
        // SELECT 1 AS renamed, bare_col, 2 -> aliased / bare-column-name / unaliased-expr.
        let core = SelectCore::Select {
            distinct: Distinctness::All,
            columns: vec![
                ResultColumn::Expr {
                    expr: Expr::Literal(Literal::Integer(1), Span::ZERO),
                    alias: Some("renamed".to_owned()),
                },
                ResultColumn::Expr {
                    expr: Expr::Column(ColumnRef::bare("bare_col"), Span::ZERO),
                    alias: None,
                },
                ResultColumn::Expr {
                    expr: Expr::Literal(Literal::Integer(2), Span::ZERO),
                    alias: None,
                },
            ],
            from: None,
            where_clause: None,
            group_by: vec![],
            having: None,
            windows: vec![],
        };
        assert_eq!(count_output_columns(&core), 3);
        assert_eq!(
            extract_output_aliases(&core),
            vec![
                Some("renamed".to_owned()),
                Some("bare_col".to_owned()),
                None
            ]
        );

        // VALUES: width comes from the first row; every column is unnamed.
        let values = SelectCore::Values(
            vec![
                vec![
                    Expr::Literal(Literal::Integer(1), Span::ZERO),
                    Expr::Literal(Literal::Integer(2), Span::ZERO),
                ],
                vec![
                    Expr::Literal(Literal::Integer(3), Span::ZERO),
                    Expr::Literal(Literal::Integer(4), Span::ZERO),
                ],
            ]
            .into(),
        );
        assert_eq!(count_output_columns(&values), 2);
        assert_eq!(extract_output_aliases(&values), vec![None, None]);

        // Empty VALUES -> zero columns.
        let empty = SelectCore::Values(vec![].into());
        assert_eq!(count_output_columns(&empty), 0);
        assert!(extract_output_aliases(&empty).is_empty());
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
        let core = SelectCore::Values(
            vec![vec![
                Expr::Literal(Literal::Integer(1), Span::ZERO),
                Expr::Literal(Literal::Integer(2), Span::ZERO),
            ]]
            .into(),
        );
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

    fn like_term_with_escape(col: &str, pattern: &str, escape: &str) -> WhereTerm<'static> {
        let expr: &'static Expr = Box::leak(Box::new(Expr::Like {
            expr: Box::new(Expr::Column(ColumnRef::bare(col), Span::ZERO)),
            pattern: Box::new(Expr::Literal(
                Literal::String(pattern.to_owned()),
                Span::ZERO,
            )),
            escape: Some(Box::new(Expr::Literal(
                Literal::String(escape.to_owned()),
                Span::ZERO,
            ))),
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
    fn test_cost_ranks_covering_index_below_non_covering_range_scan() {
        // A covering index avoids the per-match table dereference, so for the
        // same selectivity/pages it must cost strictly less than a non-covering
        // range scan — by exactly the table-access term (sel * table_pages) it
        // skips. The existing tests check each formula in isolation; this pins
        // the cross-kind ordering that makes the planner prefer covering indexes.
        let sel = 0.1;
        let range = estimate_cost(
            &AccessPathKind::IndexScanRange { selectivity: sel },
            200,
            50,
        );
        let covering = estimate_cost(
            &AccessPathKind::CoveringIndexScan { selectivity: sel },
            200,
            50,
        );
        assert!(
            covering < range,
            "covering index must rank below a range scan: {covering} vs {range}"
        );
        // The gap is exactly the avoided table-access term: sel * table_pages.
        assert!(
            ((range - covering) - sel * 200.0).abs() < 1e-9,
            "covering/range gap should equal sel*table_pages (= {}), got {}",
            sel * 200.0,
            range - covering
        );

        // With a row count, the covering scan also pays the cheaper per-row term
        // (decode only, not decode + dereference), so its advantage widens.
        let range_r = estimate_cost_ext(
            &AccessPathKind::IndexScanRange { selectivity: sel },
            200,
            50,
            1_000,
        );
        let covering_r = estimate_cost_ext(
            &AccessPathKind::CoveringIndexScan { selectivity: sel },
            200,
            50,
            1_000,
        );
        assert!(
            covering_r < range_r,
            "covering must stay cheaper once rows are counted: {covering_r} vs {range_r}"
        );
        assert!(
            (range_r - covering_r) > (range - covering),
            "per-row terms must widen the covering advantage"
        );
    }

    // ===================================================================
    // PLANNER-2: estimate_cost_ext should react monotonically to n_rows
    // ===================================================================

    #[test]
    fn access_path_metric_label_maps_every_kind() {
        // The bare metric/tracing label for each access path (no selectivity),
        // used in cost-estimate tracing and differential-plan fingerprints. It
        // is only ever used as a value, never directly asserted per variant, so
        // a wrong label would silently break observability.
        assert_eq!(
            access_path_metric_label(&AccessPathKind::FullTableScan),
            "full_table_scan"
        );
        assert_eq!(
            access_path_metric_label(&AccessPathKind::IndexScanRange { selectivity: 0.1 }),
            "index_scan_range"
        );
        assert_eq!(
            access_path_metric_label(&AccessPathKind::IndexScanEquality),
            "index_scan_equality"
        );
        assert_eq!(
            access_path_metric_label(&AccessPathKind::CoveringIndexScan { selectivity: 0.1 }),
            "covering_index_scan"
        );
        assert_eq!(
            access_path_metric_label(&AccessPathKind::RowidLookup),
            "rowid_lookup"
        );
    }

    #[test]
    fn test_snapshot_index_selection_totals_has_five_access_path_labels() {
        // The snapshot builds its map from a fixed 5-label array; the KEY set
        // is a structural contract regardless of the live counter values, so
        // this assertion is race-safe under parallel tests (values are not
        // checked).
        let snap = snapshot_index_selection_totals();
        for label in [
            "covering_index_scan",
            "full_table_scan",
            "index_scan_equality",
            "index_scan_range",
            "rowid_lookup",
        ] {
            assert!(snap.contains_key(label), "missing label: {label}");
        }
        assert_eq!(snap.len(), 5, "no extra labels");
    }

    #[test]
    fn test_estimate_cost_ext_exact_page_costs_at_zero_rows() {
        // At n_rows == 0 every per-row term vanishes, leaving the closed-form
        // page-level cost for each access path. test_estimate_cost_ext_zero_rows_
        // matches_legacy only checks FullTableScan and IndexScanEquality; pin the
        // exact log2-based formulas for the remaining variants too. Power-of-two
        // page counts keep the logs exact: ip=16 -> log2=4, tp=64 -> log2=6.
        let approx = |a: f64, b: f64| (a - b).abs() < 1e-9;
        let (ip, tp) = (16u64, 64u64);

        // Full scan == table page count.
        assert!(approx(
            estimate_cost_ext(&AccessPathKind::FullTableScan, tp, ip, 0),
            64.0
        ));
        // Rowid lookup == log2(table pages); no index term.
        assert!(approx(
            estimate_cost_ext(&AccessPathKind::RowidLookup, tp, ip, 0),
            6.0
        ));
        // Index equality == log2(index pages) + log2(table pages).
        assert!(approx(
            estimate_cost_ext(&AccessPathKind::IndexScanEquality, tp, ip, 0),
            10.0
        ));

        // Range scan == log2(ip) + sel*ip + sel*tp = 4 + 8 + 32.
        let range = estimate_cost_ext(
            &AccessPathKind::IndexScanRange { selectivity: 0.5 },
            tp,
            ip,
            0,
        );
        assert!(approx(range, 44.0), "range page cost, got {range}");

        // Covering scan omits the table-page (row dereference) term:
        // log2(ip) + sel*ip = 4 + 8, with no sel*tp.
        let covering = estimate_cost_ext(
            &AccessPathKind::CoveringIndexScan { selectivity: 0.5 },
            tp,
            ip,
            0,
        );
        assert!(approx(covering, 12.0), "covering page cost, got {covering}");

        // The structural difference is exactly the avoided table dereference,
        // sel*tp = 0.5*64 = 32 -- the reason a covering scan ranks below a range
        // scan over the same index.
        assert!(approx(range - covering, 0.5 * 64.0));
    }

    #[test]
    fn test_expression_is_equi_column_predicate() {
        // True only for a column = column equality; column=literal, literal=
        // column, literal=literal, a non-Eq op, and a non-BinaryOp all fail.
        let col = |n: &str| Box::new(Expr::Column(ColumnRef::bare(n), Span::ZERO));
        let lit = |n: i64| Box::new(Expr::Literal(Literal::Integer(n), Span::ZERO));
        let bin = |l: Box<Expr>, op: AstBinaryOp, r: Box<Expr>| Expr::BinaryOp {
            left: l,
            op,
            right: r,
            span: Span::ZERO,
        };

        assert!(expression_is_equi_column_predicate(&bin(
            col("a"),
            AstBinaryOp::Eq,
            col("b")
        )));
        assert!(!expression_is_equi_column_predicate(&bin(
            col("a"),
            AstBinaryOp::Eq,
            lit(5)
        )));
        assert!(!expression_is_equi_column_predicate(&bin(
            lit(5),
            AstBinaryOp::Eq,
            col("b")
        )));
        assert!(!expression_is_equi_column_predicate(&bin(
            lit(5),
            AstBinaryOp::Eq,
            lit(6)
        )));
        assert!(!expression_is_equi_column_predicate(&bin(
            col("a"),
            AstBinaryOp::Lt,
            col("b")
        )));
        assert!(!expression_is_equi_column_predicate(&Expr::Literal(
            Literal::Integer(1),
            Span::ZERO
        )));
    }

    #[test]
    fn test_collect_join_predicates() {
        // Equi-join terms between two tables in the set become EquiJoinPredicate
        // entries; a term involving a table outside the set is skipped; an
        // empty term list yields nothing.
        let mut set: HashSet<String> = HashSet::new();
        set.insert("a".to_owned());
        set.insert("b".to_owned());
        let terms = [join_term("a", "x", "b", "y")]; // a.x = b.y

        let (equi, theta) = collect_join_predicates(&terms, &set);
        assert_eq!(equi.len(), 1);
        assert!(theta.is_empty());

        // With only one table in the set, the predicate is skipped.
        let mut just_a: HashSet<String> = HashSet::new();
        just_a.insert("a".to_owned());
        let (equi, theta) = collect_join_predicates(&terms, &just_a);
        assert!(equi.is_empty());
        assert!(theta.is_empty());

        // An empty term list yields nothing.
        let (equi, theta) = collect_join_predicates(&[], &set);
        assert!(equi.is_empty() && theta.is_empty());
    }

    #[test]
    fn test_has_join_predicate_detects_equi_join_either_orientation() {
        // has_join_predicate finds an equi-join column predicate between two
        // tables in either argument order, case-insensitively; absent or
        // unrelated tables yield false.
        let terms = [join_term("a", "x", "b", "y")]; // a.x = b.y

        assert!(has_join_predicate("a", "b", &terms));
        assert!(
            has_join_predicate("b", "a", &terms),
            "either argument order"
        );
        assert!(has_join_predicate("A", "B", &terms), "case-insensitive");
        assert!(!has_join_predicate("a", "c", &terms), "no predicate to c");
        assert!(!has_join_predicate("c", "d", &terms));
        assert!(
            !has_join_predicate("a", "b", &[]),
            "no terms -> no predicate"
        );
    }

    #[test]
    fn test_order_indices_to_names() {
        // order_indices_to_names applies a permutation of indices to a tables
        // slice, producing the named permutation.
        let tables = vec![
            table_stats("a", 1, 1),
            table_stats("b", 1, 1),
            table_stats("c", 1, 1),
        ];
        // Empty order -> empty vec.
        assert!(order_indices_to_names(&[], &tables).is_empty());
        // Identity preserves the table order.
        assert_eq!(
            order_indices_to_names(&[0, 1, 2], &tables),
            vec!["a".to_owned(), "b".to_owned(), "c".to_owned()]
        );
        // A non-trivial permutation reorders the names accordingly.
        assert_eq!(
            order_indices_to_names(&[2, 0, 1], &tables),
            vec!["c".to_owned(), "a".to_owned(), "b".to_owned()]
        );
        // A single-element permutation yields just that table's name.
        assert_eq!(order_indices_to_names(&[1], &tables), vec!["b".to_owned()]);
    }

    #[test]
    fn test_plan_cache_key_with_feature_flags() {
        // plan_cache_key_with_feature_flags packs the schema cookie and feature
        // toggles into the xxh3 seed, so distinct (sql, cookie, flags) tuples
        // get distinct keys. The function is deterministic for fixed inputs.
        let sql = "SELECT * FROM t";

        // Determinism: same inputs -> same key.
        assert_eq!(
            plan_cache_key_with_feature_flags(sql, 1, PlannerFeatureFlags::default()),
            plan_cache_key_with_feature_flags(sql, 1, PlannerFeatureFlags::default())
        );

        // The four feature-flag combinations produce four distinct keys.
        let kd = plan_cache_key_with_feature_flags(sql, 1, PlannerFeatureFlags::default());
        let kl = plan_cache_key_with_feature_flags(
            sql,
            1,
            PlannerFeatureFlags {
                leapfrog_join: true,
                ..PlannerFeatureFlags::default()
            },
        );
        let kp = plan_cache_key_with_feature_flags(
            sql,
            1,
            PlannerFeatureFlags {
                dpccp_join: true,
                ..PlannerFeatureFlags::default()
            },
        );
        let kb = plan_cache_key_with_feature_flags(
            sql,
            1,
            PlannerFeatureFlags {
                leapfrog_join: true,
                dpccp_join: true,
            },
        );
        let set: std::collections::HashSet<u64> = [kd, kl, kp, kb].into_iter().collect();
        assert_eq!(
            set.len(),
            4,
            "all four feature-flag combinations must produce distinct keys"
        );

        // Different schema cookies -> different keys.
        assert_ne!(
            plan_cache_key_with_feature_flags(sql, 1, PlannerFeatureFlags::default()),
            plan_cache_key_with_feature_flags(sql, 2, PlannerFeatureFlags::default())
        );

        // Different SQL -> different keys.
        assert_ne!(
            plan_cache_key_with_feature_flags(sql, 1, PlannerFeatureFlags::default()),
            plan_cache_key_with_feature_flags("SELECT 1", 1, PlannerFeatureFlags::default())
        );
    }

    #[test]
    fn test_prepare_plan_cache_lookup_evicts_stale_hot_entry() {
        // No-op when the hot key is None or matches the lookup key; on a
        // mismatch it flushes the LRU touch and clears the hot cache so the
        // upcoming lookup starts clean.
        let mut p = QueryPlanner::new();

        // Fresh: hot key None -> is_some_and false -> no-op.
        p.prepare_plan_cache_lookup(42);
        assert!(p.hot_plan_cache_key.is_none());
        assert!(p.hot_plan_cache_plan.is_none());

        // Same hot key as the lookup -> no-op (state preserved).
        p.hot_plan_cache_key = Some(42);
        p.hot_plan_cache_needs_lru_touch = true;
        p.prepare_plan_cache_lookup(42);
        assert_eq!(p.hot_plan_cache_key, Some(42));
        assert!(p.hot_plan_cache_needs_lru_touch);

        // Different key -> flushes (clears the touch flag) and clears the hot
        // cache so the upcoming lookup starts clean.
        p.hot_plan_cache_key = Some(42);
        p.hot_plan_cache_needs_lru_touch = true;
        p.prepare_plan_cache_lookup(99);
        assert!(p.hot_plan_cache_key.is_none());
        assert!(p.hot_plan_cache_plan.is_none());
        assert!(!p.hot_plan_cache_needs_lru_touch);
    }

    #[test]
    fn test_flush_hot_plan_cache_lru_touch_clears_flag() {
        // flush is a no-op when needs_lru_touch is false; otherwise it touches
        // the LRU for the cached key (discarding the result) and clears the
        // flag. The flag-transition is directly observable.
        let mut p = QueryPlanner::new();
        // Fresh: flag is false, flush is a no-op.
        assert!(!p.hot_plan_cache_needs_lru_touch);
        p.flush_hot_plan_cache_lru_touch();
        assert!(!p.hot_plan_cache_needs_lru_touch);

        // Flag true, no key -> still clears the flag (no plan_cache.get).
        p.hot_plan_cache_needs_lru_touch = true;
        p.flush_hot_plan_cache_lru_touch();
        assert!(!p.hot_plan_cache_needs_lru_touch);

        // Flag true with a key -> plan_cache.get is called (returns None on
        // an empty cache, ignored), and the flag is cleared. The cache stays
        // empty because get does not insert.
        p.hot_plan_cache_key = Some(42);
        p.hot_plan_cache_needs_lru_touch = true;
        p.flush_hot_plan_cache_lru_touch();
        assert!(!p.hot_plan_cache_needs_lru_touch);
        assert!(p.is_plan_cache_empty());
    }

    #[test]
    fn test_lookup_hot_plan_cache_and_clear() {
        // On a fresh planner the hot cache is empty; any lookup misses without
        // setting needs_lru_touch. Seeding just the key (no plan) makes a
        // matching lookup still return None but trigger the needs_lru_touch
        // side effect because the key matched. A non-matching lookup leaves
        // needs_lru_touch alone. clear_hot_plan_cache zeroes all three fields.
        let mut p = QueryPlanner::new();
        assert!(p.lookup_hot_plan_cache(42).is_none());
        assert!(!p.hot_plan_cache_needs_lru_touch);

        // Seed the key only; plan stays None.
        p.hot_plan_cache_key = Some(42);
        assert!(p.lookup_hot_plan_cache(42).is_none()); // plan is None
        assert!(p.hot_plan_cache_needs_lru_touch); // side effect: key matched

        // A non-matching lookup does not touch the side-effect flag.
        assert!(p.lookup_hot_plan_cache(99).is_none());
        assert!(p.hot_plan_cache_needs_lru_touch); // unchanged

        // clear_hot_plan_cache zeroes all three hot-cache fields.
        p.clear_hot_plan_cache();
        assert!(p.hot_plan_cache_key.is_none());
        assert!(p.hot_plan_cache_plan.is_none());
        assert!(!p.hot_plan_cache_needs_lru_touch);
    }

    #[test]
    fn test_invalidate_plan_cache_if_schema_cookie_changed_tracks_cookie() {
        // Tracks the latest schema cookie on every call and clears the cache
        // when the new cookie differs from the cached one. With an empty cache
        // we directly observe the cookie tracking; the clear-on-change effect
        // on an already-empty cache is a no-op but the cookie transitions are.
        let mut p = QueryPlanner::new();
        assert_eq!(p.cached_schema_cookie, None);
        assert!(p.is_plan_cache_empty());

        // First call seeds the cookie without clearing (no prior cookie).
        p.invalidate_plan_cache_if_schema_cookie_changed(5);
        assert_eq!(p.cached_schema_cookie, Some(5));
        assert!(p.is_plan_cache_empty());

        // Same cookie -> no change (the is_some_and predicate is false).
        p.invalidate_plan_cache_if_schema_cookie_changed(5);
        assert_eq!(p.cached_schema_cookie, Some(5));

        // Different cookie -> cache cleared (still empty here) and cookie
        // updated to the new value.
        p.invalidate_plan_cache_if_schema_cookie_changed(7);
        assert_eq!(p.cached_schema_cookie, Some(7));
        assert!(p.is_plan_cache_empty());
    }

    #[test]
    fn test_is_plan_cache_empty_and_clear_on_fresh_planner() {
        // A freshly-constructed QueryPlanner has an empty plan cache (owned
        // state, no globals), custom capacities are empty initially, capacity 0
        // is clamped to 1 but still empty, and clear_plan_cache is idempotent
        // on an already-empty cache.
        let p = QueryPlanner::new();
        assert!(p.is_plan_cache_empty());

        let p2 = QueryPlanner::with_plan_cache_capacity(8);
        assert!(p2.is_plan_cache_empty());

        let p3 = QueryPlanner::with_plan_cache_capacity(0);
        assert!(p3.is_plan_cache_empty());

        let mut p4 = QueryPlanner::new();
        p4.clear_plan_cache();
        assert!(p4.is_plan_cache_empty());
    }

    #[test]
    fn test_normalize_plan_cache_capacity_floors_at_one() {
        // A requested plan-cache capacity is clamped to a non-zero value: 0
        // becomes 1 (no zero-capacity cache), positive values pass through.
        assert_eq!(normalize_plan_cache_capacity(0).get(), 1);
        assert_eq!(normalize_plan_cache_capacity(1).get(), 1);
        assert_eq!(normalize_plan_cache_capacity(10).get(), 10);
    }

    #[test]
    fn test_ordered_subset_preserves_join_order() {
        // ordered_subset keeps only the selected tables but in join_order's
        // order (not the set's), and ignores selected tables absent from the
        // join order.
        let order: Vec<String> = ["c", "a", "b", "d"]
            .iter()
            .map(|s| (*s).to_owned())
            .collect();

        let sel: HashSet<String> = ["a", "d"].iter().map(|s| (*s).to_owned()).collect();
        assert_eq!(
            ordered_subset(&order, &sel),
            vec!["a".to_owned(), "d".to_owned()] // join-order order, not set order
        );

        // Selecting everything returns the join order unchanged.
        let all: HashSet<String> = ["a", "b", "c", "d"]
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        assert_eq!(ordered_subset(&order, &all), order);

        // An empty selection yields nothing.
        assert!(ordered_subset(&order, &HashSet::new()).is_empty());

        // A selected table absent from the join order is ignored.
        let extra: HashSet<String> = ["a", "x"].iter().map(|s| (*s).to_owned()).collect();
        assert_eq!(ordered_subset(&order, &extra), vec!["a".to_owned()]);
    }

    #[test]
    fn test_cross_join_allowed_indices_via_tables() {
        // Index-based variant of cross_join_allowed: current_path holds table
        // indices, resolved against tables[*idx].name for the ordering check.
        let tables = vec![
            table_stats("a", 1, 1),
            table_stats("b", 1, 1),
            table_stats("c", 1, 1),
        ];
        let pairs = vec![("A".to_owned(), "B".to_owned())];

        // B before A (empty path) -> false.
        assert!(!cross_join_allowed_indices(&[], "B", &tables, &pairs));
        // B after A: path [0] resolves to "a", matching "A" case-insensitively.
        assert!(cross_join_allowed_indices(&[0], "B", &tables, &pairs));
        // A (the left side) is unconstrained.
        assert!(cross_join_allowed_indices(&[], "A", &tables, &pairs));
        // A table not in any pair is allowed.
        assert!(cross_join_allowed_indices(&[], "C", &tables, &pairs));
        // Case-insensitive on both the candidate and the resolved table name.
        assert!(cross_join_allowed_indices(&[0], "b", &tables, &pairs));
    }

    #[test]
    fn test_cross_join_allowed_enforces_right_after_left_ordering() {
        // For a cross-join pair (A, B), B may only be placed after A in the join
        // order. cross_join_allowed enforces this case-insensitively; candidates
        // not on the right of any pair are always allowed.
        let pairs = vec![("A".to_owned(), "B".to_owned())];

        // B before A is not allowed (A is not yet in the path).
        assert!(!cross_join_allowed(&[], "B", &pairs));
        // B after A is allowed.
        assert!(cross_join_allowed(&["A".to_owned()], "B", &pairs));
        // A (the left side) is unconstrained -- allowed anywhere.
        assert!(cross_join_allowed(&[], "A", &pairs));
        // A table not in any pair is allowed.
        assert!(cross_join_allowed(&[], "C", &pairs));
        // The check is case-insensitive on both the candidate and the path.
        assert!(!cross_join_allowed(&[], "b", &pairs));
        assert!(cross_join_allowed(&["a".to_owned()], "b", &pairs));
    }

    #[test]
    fn test_collect_disjuncts_flattens_or_tree_regardless_of_nesting() {
        // Symmetric pair to test_collect_conjuncts: collect_disjuncts recurses
        // on OR (both sides), so any OR tree flattens to its leaves regardless
        // of nesting; a non-OR expression yields a single disjunct.
        let leaf = |n: i64| Expr::Literal(Literal::Integer(n), Span::ZERO);
        let or = |l: Expr, r: Expr| Expr::BinaryOp {
            left: Box::new(l),
            op: AstBinaryOp::Or,
            right: Box::new(r),
            span: Span::ZERO,
        };
        let count = |e: &Expr| {
            let mut v = Vec::new();
            collect_disjuncts(e, &mut v);
            v.len()
        };

        // A non-OR expression is a single disjunct.
        assert_eq!(count(&leaf(1)), 1);
        // a OR b -> 2.
        assert_eq!(count(&or(leaf(1), leaf(2))), 2);
        // Right-nested a OR (b OR c) -> 3.
        assert_eq!(count(&or(leaf(1), or(leaf(2), leaf(3)))), 3);
        // Left-nested (a OR b) OR c -> 3.
        assert_eq!(count(&or(or(leaf(1), leaf(2)), leaf(3))), 3);
        // Balanced (a OR b) OR (c OR d) -> 4.
        assert_eq!(count(&or(or(leaf(1), leaf(2)), or(leaf(3), leaf(4)))), 4);
    }

    #[test]
    fn test_collect_conjuncts_flattens_and_tree_regardless_of_nesting() {
        // collect_conjuncts recursively splits on AND (both sides), so any AND
        // tree flattens to its leaves no matter how it is nested; a non-AND
        // expression yields a single conjunct.
        let leaf = |n: i64| Expr::Literal(Literal::Integer(n), Span::ZERO);
        let and = |l: Expr, r: Expr| Expr::BinaryOp {
            left: Box::new(l),
            op: AstBinaryOp::And,
            right: Box::new(r),
            span: Span::ZERO,
        };
        let count = |e: &Expr| {
            let mut v = Vec::new();
            collect_conjuncts(e, &mut v);
            v.len()
        };

        // A non-AND expression is a single conjunct.
        assert_eq!(count(&leaf(1)), 1);
        // a AND b -> 2.
        assert_eq!(count(&and(leaf(1), leaf(2))), 2);
        // Right-nested a AND (b AND c) -> 3.
        assert_eq!(count(&and(leaf(1), and(leaf(2), leaf(3)))), 3);
        // Left-nested (a AND b) AND c -> 3.
        assert_eq!(count(&and(and(leaf(1), leaf(2)), leaf(3))), 3);
        // Balanced (a AND b) AND (c AND d) -> 4.
        assert_eq!(count(&and(and(leaf(1), leaf(2)), and(leaf(3), leaf(4)))), 4);
    }

    #[test]
    fn test_classify_or_disjunction_as_in_list() {
        // a = 1 OR a = 2 OR a = 3 classifies as an IN-list on column a with 3
        // disjuncts. Mixed columns, a single (non-OR) equality, and a non-
        // equality disjunct all decline.
        let col = |n: &str| Box::new(Expr::Column(ColumnRef::bare(n), Span::ZERO));
        let lit = |n: i64| Box::new(Expr::Literal(Literal::Integer(n), Span::ZERO));
        let eqc = |c: &str, n: i64| Expr::BinaryOp {
            left: col(c),
            op: AstBinaryOp::Eq,
            right: lit(n),
            span: Span::ZERO,
        };
        let or = |l: Expr, r: Expr| Expr::BinaryOp {
            left: Box::new(l),
            op: AstBinaryOp::Or,
            right: Box::new(r),
            span: Span::ZERO,
        };

        // a = 1 OR a = 2 OR a = 3 -> IN-list on a, 3 disjuncts.
        let three = or(eqc("a", 1), or(eqc("a", 2), eqc("a", 3)));
        assert_eq!(
            classify_or_disjunction_as_in_list(&three),
            Some((
                WhereColumn {
                    table: None,
                    column: "a".to_owned()
                },
                3
            ))
        );

        // Mixed columns decline.
        assert!(classify_or_disjunction_as_in_list(&or(eqc("a", 1), eqc("b", 2))).is_none());

        // A single equality (no OR) has too few disjuncts.
        assert!(classify_or_disjunction_as_in_list(&eqc("a", 1)).is_none());

        // A non-equality disjunct declines.
        let gt = Expr::BinaryOp {
            left: col("a"),
            op: AstBinaryOp::Gt,
            right: lit(2),
            span: Span::ZERO,
        };
        assert!(classify_or_disjunction_as_in_list(&or(eqc("a", 1), gt)).is_none());
    }

    #[test]
    fn test_extract_comparison_operand_returns_other_side_of_target_column() {
        // The extractor orients a comparison around one requested table
        // column, including a commuted qualified join equality.
        let col = |n: &str| Box::new(Expr::Column(ColumnRef::bare(n), Span::ZERO));
        let qualified = |table: &str, column: &str| {
            Box::new(Expr::Column(
                ColumnRef::qualified(table, column),
                Span::ZERO,
            ))
        };
        let lit = |n: i64| Box::new(Expr::Literal(Literal::Integer(n), Span::ZERO));
        let binop = |l: Box<Expr>, r: Box<Expr>| Expr::BinaryOp {
            left: l,
            op: AstBinaryOp::Eq,
            right: r,
            span: Span::ZERO,
        };

        // x = 5 -> the literal 5 (column on the left).
        assert!(matches!(
            extract_comparison_operand_for_column(&binop(col("x"), lit(5)), "t", "x"),
            Some(Expr::Literal(Literal::Integer(5), _))
        ));
        // 5 = x -> the literal 5 (column on the right).
        assert!(matches!(
            extract_comparison_operand_for_column(&binop(lit(5), col("x")), "t", "x"),
            Some(Expr::Literal(Literal::Integer(5), _))
        ));
        assert!(matches!(
            extract_comparison_operand_for_column(
                &binop(qualified("outer", "x"), qualified("t", "x")),
                "t",
                "x",
            ),
            Some(Expr::Column(column, _))
                if column.table.as_deref() == Some("outer") && column.column.as_ref() == "x"
        ));
        // No column operand -> None.
        assert!(extract_comparison_operand_for_column(&binop(lit(5), lit(6)), "t", "x").is_none());
        // Not a binary op -> None.
        assert!(
            extract_comparison_operand_for_column(
                &Expr::Literal(Literal::Integer(1), Span::ZERO),
                "t",
                "x",
            )
            .is_none()
        );
    }

    #[test]
    fn test_like_prefix_upper_bound() {
        // The exclusive upper bound for a LIKE-prefix range scan increments the
        // last incrementable character and truncates after it; a trailing
        // char::MAX rolls over to the previous character.
        assert_eq!(like_prefix_upper_bound("abc").as_deref(), Some("abd"));
        assert_eq!(like_prefix_upper_bound("a").as_deref(), Some("b"));
        // Empty prefix has no upper bound.
        assert_eq!(like_prefix_upper_bound(""), None);
        // A trailing char::MAX rolls over: it is skipped and the previous
        // character is incremented (truncating the max away).
        let with_max = format!("a{}", char::MAX);
        assert_eq!(like_prefix_upper_bound(&with_max).as_deref(), Some("b"));
        // A lone char::MAX cannot be incremented -> None.
        assert_eq!(like_prefix_upper_bound(&char::MAX.to_string()), None);
    }

    #[test]
    fn test_is_like_prefix_safe_for_column_rejects_ascii_alphabetic_prefixes() {
        // A LIKE prefix is safe for a prefix-range index scan only when it
        // contains no ASCII alphabetic characters: default LIKE folds ASCII
        // letter case, so an alphabetic prefix could miss the opposite-case rows
        // a plain range scan would skip. Digits, punctuation, an empty prefix,
        // and non-ASCII letters are safe.
        assert!(is_like_prefix_safe_for_column(None, "123"));
        assert!(is_like_prefix_safe_for_column(None, ""));
        assert!(is_like_prefix_safe_for_column(None, "_5%"));
        // Any ASCII letter makes the prefix unsafe.
        assert!(!is_like_prefix_safe_for_column(None, "abc"));
        assert!(!is_like_prefix_safe_for_column(None, "1a"));
        assert!(!is_like_prefix_safe_for_column(None, "Z"));
        // Non-ASCII letters are not ASCII-alphabetic, so they stay safe.
        assert!(is_like_prefix_safe_for_column(None, "é"));
    }

    #[test]
    fn test_union_find() {
        // UnionFind starts with each index as its own root; union merges sets so
        // find on either side returns the same root; merging already-merged sets
        // and self-union are no-ops. Standard union-by-rank with path
        // compression in find.
        let mut uf = UnionFind::new(5);
        for i in 0..5 {
            assert_eq!(uf.find(i), i);
        }

        // union(0, 1): they share a root.
        uf.union(0, 1);
        let r0 = uf.find(0);
        assert_eq!(uf.find(1), r0);

        // union(2, 3): a second group with a different root.
        uf.union(2, 3);
        let r2 = uf.find(2);
        assert_eq!(uf.find(3), r2);
        assert_ne!(r0, r2);
        // Index 4 is still alone.
        assert_eq!(uf.find(4), 4);

        // Merge the two groups: {0,1,2,3} now share a single root.
        uf.union(0, 2);
        let r = uf.find(0);
        for i in [1, 2, 3] {
            assert_eq!(uf.find(i), r);
        }
        assert_eq!(uf.find(4), 4); // 4 still separate

        // Self-union and re-union of already-merged are no-ops.
        uf.union(0, 0);
        uf.union(0, 2);
        assert_eq!(uf.find(2), r);
    }

    #[test]
    fn test_connected_components_groups_join_connected_tables() {
        // connected_components builds a join graph from equi-join predicates and
        // returns the sets of tables reachable from one another.
        let pred = |lt: &str, rt: &str| EquiJoinPredicate {
            left: ColumnKey {
                table: lt.to_owned(),
                column: "x".to_owned(),
            },
            right: ColumnKey {
                table: rt.to_owned(),
                column: "y".to_owned(),
            },
        };
        let tables = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];

        // a joined to b; c isolated -> components of size 2 ({a,b}) and 1 ({c}).
        let comps = connected_components(&tables, &[pred("a", "b")]);
        let mut sizes: Vec<usize> = comps.iter().map(Vec::len).collect();
        sizes.sort_unstable();
        assert_eq!(sizes, vec![1, 2]);

        // a-b-c chain -> one component covering all three.
        let comps = connected_components(&tables, &[pred("a", "b"), pred("b", "c")]);
        assert_eq!(comps.len(), 1);
        assert_eq!(comps[0].len(), 3);

        // No predicates -> each table is its own component.
        let comps = connected_components(&tables, &[]);
        assert_eq!(comps.len(), 3);
        assert!(comps.iter().all(|c| c.len() == 1));

        // No tables -> no components.
        assert!(connected_components(&[], &[pred("a", "b")]).is_empty());
    }

    #[test]
    fn test_column_exists_ignore_case() {
        // column_exists_ignore_case is a case-insensitive membership check over
        // a column-name list.
        let cols = vec!["Name".to_owned(), "Age".to_owned()];
        assert!(column_exists_ignore_case(&cols, "Name")); // exact
        assert!(column_exists_ignore_case(&cols, "name")); // case-insensitive
        assert!(column_exists_ignore_case(&cols, "AGE"));
        assert!(!column_exists_ignore_case(&cols, "id")); // absent
        assert!(!column_exists_ignore_case(&[], "name")); // empty list
    }

    #[test]
    fn test_identifier_eq_preserves_ascii_case_insensitivity() {
        assert!(identifier_eq("users", "users"));
        assert!(identifier_eq("USERS", "users"));
        assert!(!identifier_eq("users", "orders"));
    }

    #[test]
    fn test_extract_range_probe_for_column() {
        // For the leading column, an equality term yields an Equality probe and
        // a range term (x > 5) yields a Range probe; terms on other columns (or
        // no terms) yield no probe.
        match extract_range_probe_for_column(&[eq_term_value("x", 5)], "t", "x") {
            Some(AccessPathProbe::Equality { column, .. }) => assert_eq!(column, "x"),
            _ => panic!("expected an Equality probe"),
        }
        assert!(matches!(
            extract_range_probe_for_column(&[range_term("x")], "t", "x"),
            Some(AccessPathProbe::Range { .. })
        ));
        // A term on a different column yields nothing for the leading column.
        assert!(extract_range_probe_for_column(&[eq_term_value("y", 5)], "t", "x").is_none());
        // No terms -> no probe.
        assert!(extract_range_probe_for_column(&[], "t", "x").is_none());
    }

    #[test]
    fn test_extract_in_list_probe() {
        // x IN (1, 2, 3) yields an InList probe carrying the column and its
        // values; an empty list, NOT IN, and a non-IN expression all yield None.
        let col = || Box::new(Expr::Column(ColumnRef::bare("x"), Span::ZERO));
        let lit = |n: i64| Expr::Literal(Literal::Integer(n), Span::ZERO);
        let in_expr = |items: Vec<Expr>, not: bool| Expr::In {
            expr: col(),
            set: InSet::List(items),
            not,
            span: Span::ZERO,
        };

        match extract_in_list_probe(&in_expr(vec![lit(1), lit(2), lit(3)], false), "t", "x") {
            Some(AccessPathProbe::InList { column, values }) => {
                assert_eq!(column, "x");
                assert_eq!(values.len(), 3);
            }
            _ => panic!("expected an InList probe"),
        }

        // Empty list -> None.
        assert!(extract_in_list_probe(&in_expr(vec![], false), "t", "x").is_none());
        // x NOT IN (1, 2) -> None.
        assert!(extract_in_list_probe(&in_expr(vec![lit(1), lit(2)], true), "t", "x").is_none());
        // A non-IN expression -> None.
        assert!(
            extract_in_list_probe(&Expr::Literal(Literal::Integer(1), Span::ZERO), "t", "x",)
                .is_none()
        );
    }

    #[test]
    fn test_reverse_comparison_op() {
        use AstBinaryOp::{Add, Eq, Ge, Gt, Le, Lt, Ne};
        // Reversing a comparison swaps operand order: Eq is symmetric, and
        // Lt<->Gt, Le<->Ge swap. Ne and non-comparison ops return None.
        assert!(matches!(reverse_comparison_op(Eq), Some(Eq)));
        assert!(matches!(reverse_comparison_op(Lt), Some(Gt)));
        assert!(matches!(reverse_comparison_op(Gt), Some(Lt)));
        assert!(matches!(reverse_comparison_op(Le), Some(Ge)));
        assert!(matches!(reverse_comparison_op(Ge), Some(Le)));
        assert!(reverse_comparison_op(Ne).is_none());
        assert!(reverse_comparison_op(Add).is_none());
    }

    #[test]
    fn test_normalize_column_literal_comparison_orients_column_left() {
        // normalize_column_literal_comparison puts the column on the left: a
        // column-OP-literal comparison keeps its op, while literal-OP-column
        // reverses the op (5 < x becomes x > 5). A non-comparison op and a
        // column-column comparison normalize to None.
        let col = |n: &str| Box::new(Expr::Column(ColumnRef::bare(n), Span::ZERO));
        let lit = |n: i64| Box::new(Expr::Literal(Literal::Integer(n), Span::ZERO));
        let bin = |l: Box<Expr>, op: AstBinaryOp, r: Box<Expr>| Expr::BinaryOp {
            left: l,
            op,
            right: r,
            span: Span::ZERO,
        };

        // x > 5 -> column x, op Gt, literal 5.
        let n =
            normalize_column_literal_comparison(&bin(col("x"), AstBinaryOp::Gt, lit(5))).unwrap();
        assert_eq!(n.column.column, "x");
        assert!(matches!(n.op, AstBinaryOp::Gt));
        assert!(matches!(n.literal, Literal::Integer(5)));

        // 5 < x -> column x, op reversed to Gt, literal 5.
        let n =
            normalize_column_literal_comparison(&bin(lit(5), AstBinaryOp::Lt, col("x"))).unwrap();
        assert_eq!(n.column.column, "x");
        assert!(matches!(n.op, AstBinaryOp::Gt));
        assert!(matches!(n.literal, Literal::Integer(5)));

        // A non-comparison op (Add) normalizes to None.
        assert!(
            normalize_column_literal_comparison(&bin(col("x"), AstBinaryOp::Add, lit(5))).is_none()
        );
        // A column-column comparison (no literal) normalizes to None.
        assert!(
            normalize_column_literal_comparison(&bin(col("x"), AstBinaryOp::Eq, col("y")))
                .is_none()
        );
    }

    #[test]
    fn test_where_terms_imply_predicate() {
        // For every AND-conjunct of the predicate, some term must imply it (via
        // expr_implies_partial_predicate). Pinning the multi-conjunct and
        // empty-terms behaviors.
        let col = |n: &str| Box::new(Expr::Column(ColumnRef::bare(n), Span::ZERO));
        let is_not_null = |n: &str| Expr::IsNull {
            expr: col(n),
            not: true,
            span: Span::ZERO,
        };
        let and = |l: Expr, r: Expr| Expr::BinaryOp {
            left: Box::new(l),
            op: AstBinaryOp::And,
            right: Box::new(r),
            span: Span::ZERO,
        };

        // x = 5 implies x IS NOT NULL.
        let terms = [eq_term_value("x", 5)];
        assert!(where_terms_imply_predicate(
            &terms,
            &is_not_null("x"),
            "t",
            true
        ));

        // x = 5 does not imply y IS NOT NULL (different column).
        assert!(!where_terms_imply_predicate(
            &terms,
            &is_not_null("y"),
            "t",
            true
        ));

        // Both terms together imply (x IS NOT NULL AND y IS NOT NULL).
        let both = [eq_term_value("x", 5), eq_term_value("y", 7)];
        assert!(where_terms_imply_predicate(
            &both,
            &and(is_not_null("x"), is_not_null("y")),
            "t",
            true
        ));

        // Only the x term -> the y conjunct is unimplied -> overall false.
        assert!(!where_terms_imply_predicate(
            &terms,
            &and(is_not_null("x"), is_not_null("y")),
            "t",
            true
        ));

        // No terms -> any() over empty is false -> no implication possible.
        assert!(!where_terms_imply_predicate(
            &[],
            &is_not_null("x"),
            "t",
            true
        ));
    }

    #[test]
    fn test_expr_implies_partial_predicate() {
        // A query predicate implies a partial-index predicate when it is
        // structurally identical, when it guarantees the column non-null for an
        // IS NOT NULL index predicate, or when the same-bound comparison
        // operator is logically stricter.
        let col = |n: &str| Box::new(Expr::Column(ColumnRef::bare(n), Span::ZERO));
        let lit = |n: i64| Box::new(Expr::Literal(Literal::Integer(n), Span::ZERO));
        let cmp = |c: &str, op: AstBinaryOp, n: i64| Expr::BinaryOp {
            left: col(c),
            op,
            right: lit(n),
            span: Span::ZERO,
        };

        // Structural identity implies trivially.
        assert!(expr_implies_partial_predicate(
            &cmp("x", AstBinaryOp::Eq, 5),
            &cmp("x", AstBinaryOp::Eq, 5),
            "t",
            true
        ));
        assert!(
            !expr_implies_partial_predicate(
                &cmp("x", AstBinaryOp::Eq, 5),
                &cmp("x", AstBinaryOp::Eq, 5),
                "t",
                false
            ),
            "bare structural identity is not a table-binding proof in a join"
        );

        // Distinct bounds are not ordered without affinity/collation proof.
        assert!(!expr_implies_partial_predicate(
            &cmp("x", AstBinaryOp::Gt, 10),
            &cmp("x", AstBinaryOp::Gt, 5),
            "t",
            true
        ));
        // At the same bound, `>` safely implies `>=`.
        assert!(expr_implies_partial_predicate(
            &cmp("x", AstBinaryOp::Gt, 5),
            &cmp("x", AstBinaryOp::Ge, 5),
            "t",
            true
        ));
        assert!(!expr_implies_partial_predicate(
            &cmp("x", AstBinaryOp::Gt, 5),
            &cmp("x", AstBinaryOp::Gt, 10),
            "t",
            true
        ));

        // x = 5 implies the partial-index predicate x IS NOT NULL.
        let is_not_null = Expr::IsNull {
            expr: col("x"),
            not: true,
            span: Span::ZERO,
        };
        assert!(expr_implies_partial_predicate(
            &cmp("x", AstBinaryOp::Eq, 5),
            &is_not_null,
            "t",
            true
        ));

        // Different columns do not imply.
        assert!(!expr_implies_partial_predicate(
            &cmp("x", AstBinaryOp::Eq, 5),
            &cmp("y", AstBinaryOp::Eq, 3),
            "t",
            true
        ));
    }

    #[test]
    fn test_partial_index_is_not_null_accepts_direct_comparison_placeholders() {
        let is_not_null = Expr::IsNull {
            expr: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            not: true,
            span: Span::ZERO,
        };
        for op in [
            AstBinaryOp::Eq,
            AstBinaryOp::Ne,
            AstBinaryOp::Lt,
            AstBinaryOp::Le,
            AstBinaryOp::Gt,
            AstBinaryOp::Ge,
        ] {
            let comparison = Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
                op,
                right: Box::new(Expr::Placeholder(
                    fsqlite_ast::PlaceholderType::Numbered(1),
                    Span::ZERO,
                )),
                span: Span::ZERO,
            };
            assert!(
                expr_implies_partial_predicate(&comparison, &is_not_null, "t", true),
                "a TRUE {op:?} comparison proves its direct column is non-NULL"
            );
        }

        let reversed = Expr::BinaryOp {
            left: Box::new(Expr::Placeholder(
                fsqlite_ast::PlaceholderType::Numbered(1),
                Span::ZERO,
            )),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            span: Span::ZERO,
        };
        assert!(expr_implies_partial_predicate(
            &reversed,
            &is_not_null,
            "t",
            true
        ));

        let is_non_null_literal = Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            op: AstBinaryOp::Is,
            right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            span: Span::ZERO,
        };
        assert!(expr_implies_partial_predicate(
            &is_non_null_literal,
            &is_not_null,
            "t",
            true
        ));

        for nullable_rhs in [
            Expr::Placeholder(fsqlite_ast::PlaceholderType::Numbered(1), Span::ZERO),
            Expr::Literal(Literal::Null, Span::ZERO),
        ] {
            let nullable_is = Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
                op: AstBinaryOp::Is,
                right: Box::new(nullable_rhs),
                span: Span::ZERO,
            };
            assert!(
                !expr_implies_partial_predicate(&nullable_is, &is_not_null, "t", true),
                "IS against a nullable operand cannot prove the column non-NULL"
            );
        }
    }

    #[test]
    fn test_join_partial_index_is_not_null_accepts_qualified_placeholder_comparison() {
        let table = table_stats("p", 100, 1_000);
        let mut partial_idx = index_info("idx_p_a_not_null", "p", &["a"], false, 20);
        partial_idx.partial_where = Some(Expr::IsNull {
            expr: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            not: true,
            span: Span::ZERO,
        });
        let context = JoinAccessPathContext {
            table_index_hints: None,
            cracking_hints: None,
            available_outer_tables: &[],
            unqualified_terms_are_table_local: false,
        };

        let local_comparison = Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::qualified("p", "a"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Placeholder(
                fsqlite_ast::PlaceholderType::Numbered(7),
                Span::ZERO,
            )),
            span: Span::ZERO,
        };
        let local_terms = [classify_where_term(&local_comparison)];
        let local_path = join_access_path(
            &table,
            std::slice::from_ref(&partial_idx),
            &local_terms,
            None,
            context,
        );
        assert_eq!(local_path.index.as_deref(), Some("idx_p_a_not_null"));
        assert!(matches!(
            local_path.probe,
            Some(AccessPathProbe::Equality { target, .. })
                if matches!(
                    target.as_ref(),
                    Expr::Placeholder(fsqlite_ast::PlaceholderType::Numbered(7), _)
                )
        ));

        let foreign_comparison = Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::qualified("other", "a"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Placeholder(
                fsqlite_ast::PlaceholderType::Numbered(7),
                Span::ZERO,
            )),
            span: Span::ZERO,
        };
        let foreign_terms = [classify_where_term(&foreign_comparison)];
        let foreign_path = join_access_path(&table, &[partial_idx], &foreign_terms, None, context);
        assert!(
            matches!(foreign_path.kind, AccessPathKind::FullTableScan),
            "a foreign qualifier must not prove p.a IS NOT NULL"
        );
    }

    #[test]
    fn test_partial_index_implication_requires_same_literal_and_bound_column() {
        let comparison =
            |table: Option<&str>, op: AstBinaryOp, literal: Literal| NormalizedColumnComparison {
                column: WhereColumn {
                    table: table.map(str::to_owned),
                    column: "x".to_owned(),
                },
                op,
                literal,
            };
        let bare = |op, literal| comparison(None, op, literal);
        let implies = |query: &NormalizedColumnComparison,
                       predicate: &NormalizedColumnComparison| {
            query.implies(predicate, "t", true)
        };

        let equal_five = bare(AstBinaryOp::Eq, Literal::Integer(5));
        assert!(implies(
            &equal_five,
            &bare(AstBinaryOp::Eq, Literal::Integer(5))
        ));
        assert!(implies(
            &equal_five,
            &bare(AstBinaryOp::Ge, Literal::Integer(5))
        ));
        assert!(implies(
            &equal_five,
            &bare(AstBinaryOp::Le, Literal::Integer(5))
        ));
        assert!(!implies(
            &equal_five,
            &bare(AstBinaryOp::Gt, Literal::Integer(5))
        ));

        let greater_five = bare(AstBinaryOp::Gt, Literal::Integer(5));
        assert!(implies(
            &greater_five,
            &bare(AstBinaryOp::Gt, Literal::Integer(5))
        ));
        assert!(implies(
            &greater_five,
            &bare(AstBinaryOp::Ge, Literal::Integer(5))
        ));
        assert!(!implies(
            &bare(AstBinaryOp::Ge, Literal::Integer(5)),
            &bare(AstBinaryOp::Gt, Literal::Integer(5))
        ));

        // Rust ordering between distinct literals does not prove SQLite
        // implication without the column's affinity and collation. On a TEXT
        // column, for example, `x > 10` can include the value `'2'` even though
        // a partial index on `x > 2` excludes it.
        assert!(!implies(
            &bare(AstBinaryOp::Gt, Literal::Integer(10)),
            &bare(AstBinaryOp::Gt, Literal::Integer(2))
        ));
        assert!(!implies(
            &bare(AstBinaryOp::Gt, Literal::String("z".to_owned())),
            &bare(AstBinaryOp::Gt, Literal::String("a".to_owned()))
        ));

        // Numerically equal INTEGER/REAL spellings also remain distinct until
        // the planner carries proof of the comparison affinity.
        let max_integer = bare(AstBinaryOp::Eq, Literal::Integer(i64::MAX));
        let rounded_max_real = bare(AstBinaryOp::Eq, Literal::Float(i64::MAX as f64));
        assert!(!implies(&max_integer, &rounded_max_real));
        assert!(!implies(&rounded_max_real, &max_integer));
        let min_integer = bare(AstBinaryOp::Eq, Literal::Integer(i64::MIN));
        let exact_min_real = bare(AstBinaryOp::Eq, Literal::Float(i64::MIN as f64));
        assert!(!implies(&min_integer, &exact_min_real));
        assert!(!implies(&exact_min_real, &min_integer));

        let qualified = comparison(Some("t"), AstBinaryOp::Eq, Literal::Integer(5));
        assert!(implies(
            &qualified,
            &comparison(Some("T"), AstBinaryOp::Eq, Literal::Integer(5))
        ));
        assert!(!implies(
            &qualified,
            &comparison(Some("other"), AstBinaryOp::Eq, Literal::Integer(5))
        ));
        assert!(implies(
            &qualified,
            &bare(AstBinaryOp::Eq, Literal::Integer(5))
        ));
        assert!(!implies(
            &bare(AstBinaryOp::Eq, Literal::Integer(5)),
            &comparison(Some("t"), AstBinaryOp::Eq, Literal::Integer(5))
        ));
        assert!(!implies(
            &comparison(Some("other"), AstBinaryOp::Eq, Literal::Integer(5)),
            &bare(AstBinaryOp::Eq, Literal::Integer(5))
        ));
    }

    #[test]
    fn test_lookup_table_index_hint() {
        // Lookup canonicalizes the requested table name to lowercase via
        // canonical_table_key; the map keys must already be canonical.
        let mut hints: std::collections::BTreeMap<String, IndexHint> =
            std::collections::BTreeMap::new();
        hints.insert("users".to_owned(), IndexHint::NotIndexed);

        // Hit by exact canonical key.
        assert!(matches!(
            lookup_table_index_hint("users", Some(&hints)),
            Some(IndexHint::NotIndexed)
        ));
        // Hit by case-insensitive lookup (USERS canonicalizes to users).
        assert!(matches!(
            lookup_table_index_hint("USERS", Some(&hints)),
            Some(IndexHint::NotIndexed)
        ));
        // Miss: a table name not in the map.
        assert!(lookup_table_index_hint("other", Some(&hints)).is_none());
        // No hints map at all -> None.
        assert!(lookup_table_index_hint("users", None).is_none());
    }

    #[test]
    fn test_is_rowid_column_ignores_table_qualifier() {
        // is_rowid_column delegates to is_rowid_alias_name on the column part:
        // rowid / _rowid_ / oid (case-insensitive) -> true; others -> false. The
        // table qualifier on the WhereColumn is ignored.
        let wc = |table: Option<&str>, column: &str| WhereColumn {
            table: table.map(str::to_owned),
            column: column.to_owned(),
        };
        assert!(is_rowid_column(&wc(None, "rowid")));
        assert!(is_rowid_column(&wc(None, "ROWID")));
        assert!(is_rowid_column(&wc(None, "_rowid_")));
        assert!(is_rowid_column(&wc(None, "oid")));
        assert!(!is_rowid_column(&wc(None, "id")));
        assert!(!is_rowid_column(&wc(None, "row_id")));
        // The table qualifier is ignored; only the column name decides.
        assert!(is_rowid_column(&wc(Some("t"), "rowid")));
        assert!(!is_rowid_column(&wc(Some("t"), "id")));
    }

    #[test]
    fn test_where_columns_equivalent_requires_matching_qualification() {
        let bare = |c: &str| WhereColumn {
            table: None,
            column: c.to_owned(),
        };
        let qual = |t: &str, c: &str| WhereColumn {
            table: Some(t.to_owned()),
            column: c.to_owned(),
        };

        // Same column, both unqualified.
        assert!(where_columns_equivalent(&bare("x"), &bare("X")));
        // Same column, both qualified by the same table (case-insensitive).
        assert!(where_columns_equivalent(&qual("t", "x"), &qual("T", "X")));
        // Different table qualifiers do not identify the same bound column.
        assert!(!where_columns_equivalent(&qual("t", "x"), &qual("u", "x")));
        // A missing qualifier is not a wildcard: without name-resolution
        // evidence, it cannot prove a partial-index predicate about `t.x`.
        assert!(!where_columns_equivalent(&qual("t", "x"), &bare("x")));
        // Different columns never match.
        assert!(!where_columns_equivalent(&bare("x"), &bare("y")));
    }

    #[test]
    fn test_qualifier_matches_table() {
        // A qualifier matches by table name or by alias, case-insensitively.
        // Table name, no alias.
        assert!(qualifier_matches_table("t", "t", None));
        assert!(qualifier_matches_table("T", "t", None)); // case-insensitive
        assert!(!qualifier_matches_table("u", "t", None)); // no match, no alias
        // With an alias, the qualifier may match either the name or the alias.
        assert!(qualifier_matches_table("users", "users", Some("u")));
        assert!(qualifier_matches_table("U", "users", Some("u"))); // alias, case-insensitive
        assert!(!qualifier_matches_table("x", "users", Some("u"))); // matches neither
    }

    #[test]
    fn test_extract_qualified_column_requires_qualifier_and_canonicalizes() {
        // extract_qualified_column requires a table-qualified column and lower-
        // cases both the table and the column; bare columns and non-columns
        // yield None. (Distinct from extract_where_column, which accepts bare
        // columns and preserves case.)
        let qualified = Expr::Column(ColumnRef::qualified("T", "Col"), Span::ZERO);
        assert_eq!(
            extract_qualified_column(&qualified),
            Some(ColumnKey {
                table: "t".to_owned(),
                column: "col".to_owned()
            })
        );

        // A bare (unqualified) column has no table -> None.
        assert_eq!(
            extract_qualified_column(&Expr::Column(ColumnRef::bare("x"), Span::ZERO)),
            None
        );

        // A non-column expression -> None.
        assert_eq!(
            extract_qualified_column(&Expr::Literal(Literal::Integer(1), Span::ZERO)),
            None
        );
    }

    #[test]
    fn test_extract_where_column_preserves_qualifier_and_rejects_non_columns() {
        // extract_where_column lifts a column reference into a WhereColumn,
        // preserving the table qualifier, and returns None for anything that is
        // not a bare column expression.
        let bare = Expr::Column(ColumnRef::bare("x"), Span::ZERO);
        assert_eq!(
            extract_where_column(&bare),
            Some(WhereColumn {
                table: None,
                column: "x".to_owned()
            })
        );

        let qualified = Expr::Column(ColumnRef::qualified("t", "x"), Span::ZERO);
        assert_eq!(
            extract_where_column(&qualified),
            Some(WhereColumn {
                table: Some("t".to_owned()),
                column: "x".to_owned()
            })
        );

        // Non-column expressions yield None.
        assert_eq!(
            extract_where_column(&Expr::Literal(Literal::Integer(1), Span::ZERO)),
            None
        );
        let binop = Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("x"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            span: Span::ZERO,
        };
        assert_eq!(extract_where_column(&binop), None);
    }

    #[test]
    fn test_bound_outer_value_is_a_runtime_constant_leaf() {
        let bound_null = Expr::BoundOuterValue {
            value: SqliteValue::Null,
            collation: BoundCollation::Named("NOCASE".to_owned()),
            affinity: Some(fsqlite_types::TypeAffinity::Text),
            span: Span::ZERO,
        };

        assert!(expr_columns_satisfy(&bound_null, &|_| false));
        let mut table_refs = HashSet::new();
        collect_table_refs(&bound_null, &mut table_refs);
        assert!(table_refs.is_empty());

        let mut index_definition_expr = bound_null.clone();
        assert!(!normalize_expression_index_columns(
            &mut index_definition_expr,
            "t"
        ));

        let comparison = Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("x"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(bound_null),
            span: Span::ZERO,
        };
        assert!(matches!(
            classify_where_term(&comparison).kind,
            WhereTermKind::Other
        ));
    }

    #[test]
    fn test_normalize_is_not_null_predicate() {
        // Only `<column> IS NOT NULL` normalizes to its column; IS NULL, a
        // non-column operand, and non-IsNull expressions yield None.
        let isnull = |inner: Expr, not: bool| Expr::IsNull {
            expr: Box::new(inner),
            not,
            span: Span::ZERO,
        };
        let col = |n: &str| Expr::Column(ColumnRef::bare(n), Span::ZERO);

        // x IS NOT NULL -> Some(column x).
        assert_eq!(
            normalize_is_not_null_predicate(&isnull(col("x"), true)),
            Some(WhereColumn {
                table: None,
                column: "x".to_owned()
            })
        );
        // x IS NULL (not: false) -> None.
        assert_eq!(
            normalize_is_not_null_predicate(&isnull(col("x"), false)),
            None
        );
        // (5) IS NOT NULL -> None: the operand is not a column.
        assert_eq!(
            normalize_is_not_null_predicate(&isnull(
                Expr::Literal(Literal::Integer(5), Span::ZERO),
                true
            )),
            None
        );
        // A non-IsNull expression -> None.
        assert_eq!(
            normalize_is_not_null_predicate(&Expr::Literal(Literal::Integer(1), Span::ZERO)),
            None
        );
    }

    #[test]
    fn test_partial_index_expr_guarantees_non_null_for_matching_column() {
        // expr_guarantees_non_null reports whether a WHERE expression can be
        // true only when the target column is non-NULL. Explicit IS NOT NULL,
        // comparisons, positive IN, and non-empty literal NOT IN can qualify;
        // empty or runtime-sized NOT IN right-hand sides, function-backed
        // pattern operators, IS NULL, and predicates on another column do not.
        let pcol = WhereColumn {
            table: None,
            column: "x".to_owned(),
        };
        let col = |n: &str| Box::new(Expr::Column(ColumnRef::bare(n), Span::ZERO));

        // x IS NOT NULL guarantees x is non-null.
        let is_not_null = Expr::IsNull {
            expr: col("x"),
            not: true,
            span: Span::ZERO,
        };
        assert!(expr_guarantees_non_null(&is_not_null, &pcol, "t", true));

        // x IS NULL does not.
        let is_null = Expr::IsNull {
            expr: col("x"),
            not: false,
            span: Span::ZERO,
        };
        assert!(!expr_guarantees_non_null(&is_null, &pcol, "t", true));

        // x = 5 (non-null literal) guarantees non-null; x = NULL does not.
        let eq = |lit: Literal| Expr::BinaryOp {
            left: col("x"),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(lit, Span::ZERO)),
            span: Span::ZERO,
        };
        assert!(expr_guarantees_non_null(
            &eq(Literal::Integer(5)),
            &pcol,
            "t",
            true
        ));
        assert!(!expr_guarantees_non_null(
            &eq(Literal::Null),
            &pcol,
            "t",
            true
        ));

        let in_expr = |set: InSet, not: bool| Expr::In {
            expr: col("x"),
            set,
            not,
            span: Span::ZERO,
        };
        assert!(
            !expr_guarantees_non_null(&in_expr(InSet::List(Vec::new()), true), &pcol, "t", true),
            "NULL NOT IN () is true in SQLite"
        );
        assert!(
            expr_guarantees_non_null(
                &in_expr(
                    InSet::List(vec![Expr::Literal(Literal::Integer(1), Span::ZERO)]),
                    true
                ),
                &pcol,
                "t",
                true
            ),
            "a non-empty literal NOT IN list cannot be true for a NULL left operand"
        );
        let empty_subquery = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![ResultColumn::Expr {
                        expr: Expr::Literal(Literal::Integer(1), Span::ZERO),
                        alias: None,
                    }],
                    from: None,
                    where_clause: Some(Box::new(Expr::Literal(Literal::False, Span::ZERO))),
                    group_by: Vec::new(),
                    having: None,
                    windows: Vec::new(),
                },
                compounds: Vec::new(),
            },
            order_by: Vec::new(),
            limit: None,
        };
        assert!(
            !expr_guarantees_non_null(
                &in_expr(InSet::Subquery(Box::new(empty_subquery)), true),
                &pcol,
                "t",
                true
            ),
            "NULL NOT IN an empty subquery is true in SQLite"
        );
        assert!(
            !expr_guarantees_non_null(
                &in_expr(InSet::Table(QualifiedName::bare("rhs")), true),
                &pcol,
                "t",
                true
            ),
            "a table-form NOT IN RHS may be empty at execution"
        );
        assert!(
            expr_guarantees_non_null(&in_expr(InSet::List(Vec::new()), false), &pcol, "t", true),
            "positive IN can only be true for a non-NULL left operand"
        );

        // An IS NOT NULL on a DIFFERENT column does not help.
        let other = Expr::IsNull {
            expr: col("y"),
            not: true,
            span: Span::ZERO,
        };
        assert!(!expr_guarantees_non_null(&other, &pcol, "t", true));
    }

    #[test]
    fn test_estimate_cost_ext_zero_rows_matches_legacy() {
        // With n_rows == 0 the ext function must match the legacy formulas.
        let legacy = estimate_cost(&AccessPathKind::FullTableScan, 1000, 0);
        let ext = estimate_cost_ext(&AccessPathKind::FullTableScan, 1000, 0, 0);
        assert!((ext - legacy).abs() < f64::EPSILON);

        let legacy = estimate_cost(&AccessPathKind::IndexScanEquality, 1000, 100);
        let ext = estimate_cost_ext(&AccessPathKind::IndexScanEquality, 1000, 100, 0);
        assert!((ext - legacy).abs() < f64::EPSILON);
    }

    #[test]
    fn test_estimate_cost_ext_full_scan_monotonic_in_n_rows() {
        // Full table scan: same pages, growing rows -> cost must grow.
        let c_small = estimate_cost_ext(&AccessPathKind::FullTableScan, 100, 0, 1_000);
        let c_mid = estimate_cost_ext(&AccessPathKind::FullTableScan, 100, 0, 100_000);
        let c_big = estimate_cost_ext(&AccessPathKind::FullTableScan, 100, 0, 10_000_000);
        assert!(
            c_small < c_mid && c_mid < c_big,
            "expected monotonic growth with n_rows, got {c_small} < {c_mid} < {c_big}"
        );
    }

    #[test]
    fn test_estimate_cost_ext_range_scan_monotonic_in_n_rows() {
        // Index range scan: fixed selectivity, growing rows -> cost must grow.
        let kind = AccessPathKind::IndexScanRange { selectivity: 0.1 };
        let c_small = estimate_cost_ext(&kind, 1000, 100, 1_000);
        let c_big = estimate_cost_ext(&kind, 1000, 100, 1_000_000);
        assert!(c_big > c_small);
    }

    #[test]
    fn test_estimate_cost_ext_ranks_point_access_below_full_scan_for_large_tables() {
        // The PLANNER-2 per-row terms exist so the cost model ranks a point
        // access *below* a full scan once a table has many rows. Verify the
        // cross-path ordering (the planning consequence), not just per-path
        // monotonicity: for the same table, rowid <= index-equality << full scan.
        let (tp, ip, big) = (100u64, 50u64, 1_000_000u64);
        let full = estimate_cost_ext(&AccessPathKind::FullTableScan, tp, ip, big);
        let eq = estimate_cost_ext(&AccessPathKind::IndexScanEquality, tp, ip, big);
        let rowid = estimate_cost_ext(&AccessPathKind::RowidLookup, tp, ip, big);

        assert!(
            rowid <= eq,
            "rowid lookup should not cost more than index equality: {rowid} vs {eq}"
        );
        assert!(
            eq < full,
            "index equality must rank below a full scan on a large table: {eq} vs {full}"
        );

        // Equality/rowid stay ~row-count-insensitive (only one matched row's
        // access cost), unlike the full scan which scales with n_rows.
        let eq_zero = estimate_cost_ext(&AccessPathKind::IndexScanEquality, tp, ip, 0);
        let rowid_zero = estimate_cost_ext(&AccessPathKind::RowidLookup, tp, ip, 0);
        assert!(
            eq - eq_zero < 1.0,
            "equality cost must not scale with n_rows: delta {}",
            eq - eq_zero
        );
        assert!(
            rowid - rowid_zero < 1.0,
            "rowid cost must not scale with n_rows: delta {}",
            rowid - rowid_zero
        );

        // Sanity: n_rows=0 full scan equals table-page count, and a large row
        // count grows it by orders of magnitude.
        let full_zero = estimate_cost_ext(&AccessPathKind::FullTableScan, tp, ip, 0);
        assert!(
            (full_zero - 100.0).abs() < f64::EPSILON,
            "n_rows=0 full scan == table pages"
        );
        assert!(
            full > full_zero * 10.0,
            "full scan must grow strongly with n_rows: {full} vs {full_zero}"
        );
    }

    #[test]
    fn test_estimate_cost_ext_scales_full_vs_index_preference() {
        // Scenario: two tables with the same (small) page count but very
        // different row counts. For a moderately selective index scan the
        // large-row table should prefer the index over the full scan.
        let small_rows = 100_u64;
        let big_rows = 10_000_000_u64;
        let kind = AccessPathKind::IndexScanRange { selectivity: 0.01 };
        let full_small = estimate_cost_ext(&AccessPathKind::FullTableScan, 10, 0, small_rows);
        let idx_small = estimate_cost_ext(&kind, 10, 5, small_rows);
        let full_big = estimate_cost_ext(&AccessPathKind::FullTableScan, 10, 0, big_rows);
        let idx_big = estimate_cost_ext(&kind, 10, 5, big_rows);

        // Index vs full gap should widen when n_rows blows up (full scan cost
        // grows linearly in rows, index cost grows as selectivity * rows).
        let gap_small = full_small - idx_small;
        let gap_big = full_big - idx_big;
        assert!(
            gap_big > gap_small,
            "expected bigger index advantage at high n_rows: small_gap={gap_small}, big_gap={gap_big}"
        );
    }

    // ===================================================================
    // PLANNER-3: order_join_inputs_with_hints tests
    // ===================================================================

    fn stats_ref(name: &str, n_pages: u64, n_rows: u64, has_stats: bool) -> TableRefWithStats {
        TableRefWithStats {
            name: name.to_owned(),
            n_pages,
            n_rows,
            has_stats,
        }
    }

    #[test]
    fn test_order_joins_puts_small_relation_first() {
        // Classic "10 row small table JOIN 10k row big table": the small
        // relation should end up on the build side (index 0).
        let inputs = vec![
            stats_ref("t_big", 200, 10_000, true),
            stats_ref("t_small", 1, 10, true),
        ];
        let perm = order_join_inputs_with_hints(&inputs);
        assert_eq!(perm.len(), 2);
        assert_eq!(
            inputs[perm[0]].name, "t_small",
            "small table should sort to build-side first, got perm={perm:?}",
        );
        assert_eq!(inputs[perm[1]].name, "t_big");
    }

    #[test]
    fn test_order_joins_no_stats_preserves_source_order() {
        // No ANALYZE data: every entry has has_stats = false. Even though
        // n_rows differs wildly, we preserve the identity permutation so
        // callers see the same row order they handed in.
        let inputs = vec![
            stats_ref("t_first", 200, 10_000, false),
            stats_ref("t_second", 1, 10, false),
            stats_ref("t_third", 5, 50, false),
        ];
        let perm = order_join_inputs_with_hints(&inputs);
        assert_eq!(
            perm,
            vec![0, 1, 2],
            "source order must be preserved when no stats are available",
        );
    }

    #[test]
    fn test_order_joins_partial_stats_still_orders() {
        // At least one table has stats → we reorder. Tables missing stats
        // default to n_rows == 0, which yields the smallest scan cost, so
        // they naturally sort to the front. That matches the "assume small
        // until proven otherwise" heuristic.
        let inputs = vec![
            stats_ref("t_big_analyzed", 500, 100_000, true),
            stats_ref("t_unknown", 0, 0, false),
        ];
        let perm = order_join_inputs_with_hints(&inputs);
        assert_eq!(inputs[perm[0]].name, "t_unknown");
        assert_eq!(inputs[perm[1]].name, "t_big_analyzed");
    }

    #[test]
    fn test_order_joins_trivial_sizes() {
        // N=0 and N=1 must return the identity.
        assert_eq!(order_join_inputs_with_hints(&[]), Vec::<usize>::new());
        let single = vec![stats_ref("only", 10, 100, true)];
        assert_eq!(order_join_inputs_with_hints(&single), vec![0]);
    }

    #[test]
    fn test_order_joins_greedy_above_limit() {
        // N > 4 uses greedy smallest-first. Verify that five tables with
        // strictly increasing cost produce the identity permutation, and
        // that a reversed input is fully sorted.
        let reversed = vec![
            stats_ref("a_5", 500, 50_000, true),
            stats_ref("a_4", 400, 40_000, true),
            stats_ref("a_3", 300, 30_000, true),
            stats_ref("a_2", 200, 20_000, true),
            stats_ref("a_1", 100, 10_000, true),
        ];
        let perm = order_join_inputs_with_hints(&reversed);
        let ordered_names: Vec<&str> = perm.iter().map(|&i| reversed[i].name.as_str()).collect();
        assert_eq!(
            ordered_names,
            vec!["a_1", "a_2", "a_3", "a_4", "a_5"],
            "greedy path should sort ascending by scan cost",
        );
    }

    #[test]
    fn test_order_joins_exhaustive_minimizes_weighted_cost() {
        // N=4 goes through the exhaustive permutation search. The tiny
        // relation should dominate the build-side slot even when it sits
        // in the middle of the input.
        let inputs = vec![
            stats_ref("t_a", 100, 5_000, true),
            stats_ref("t_b", 50, 2_000, true),
            stats_ref("t_tiny", 1, 10, true),
            stats_ref("t_huge", 1_000, 1_000_000, true),
        ];
        let perm = order_join_inputs_with_hints(&inputs);
        assert_eq!(
            inputs[perm[0]].name, "t_tiny",
            "exhaustive search should pick the smallest relation first; perm={perm:?}",
        );
        assert_eq!(
            inputs[perm[3]].name, "t_huge",
            "largest relation should sink to the last probe slot; perm={perm:?}",
        );
    }

    #[test]
    fn test_order_joins_preserves_source_order_on_equal_cost_ties() {
        // Equal-cost tables must keep their source order in BOTH branches: the
        // greedy path uses a stable sort, and the exhaustive path scores the
        // identity permutation first with a strict-less update, so no equal-cost
        // permutation can displace it. (Documented "stable keeps ties in source
        // order" contract; existing tests only exercise distinct costs.)

        // Exhaustive branch (N <= JOIN_ORDER_EXHAUSTIVE_LIMIT = 4).
        let exhaustive = vec![
            stats_ref("e0", 100, 5_000, true),
            stats_ref("e1", 100, 5_000, true),
            stats_ref("e2", 100, 5_000, true),
        ];
        assert_eq!(
            order_join_inputs_with_hints(&exhaustive),
            vec![0, 1, 2],
            "equal-cost tables keep source order (exhaustive branch)"
        );

        // Greedy branch (N > 4, stable sort).
        let greedy = vec![
            stats_ref("g0", 100, 5_000, true),
            stats_ref("g1", 100, 5_000, true),
            stats_ref("g2", 100, 5_000, true),
            stats_ref("g3", 100, 5_000, true),
            stats_ref("g4", 100, 5_000, true),
        ];
        assert_eq!(
            order_join_inputs_with_hints(&greedy),
            vec![0, 1, 2, 3, 4],
            "equal-cost tables keep source order (greedy branch)"
        );

        // Deterministic: repeated calls yield identical permutations.
        assert_eq!(
            order_join_inputs_with_hints(&exhaustive),
            order_join_inputs_with_hints(&exhaustive)
        );
        assert_eq!(
            order_join_inputs_with_hints(&greedy),
            order_join_inputs_with_hints(&greedy)
        );
    }

    #[test]
    fn test_order_joins_from_table_stats_derives_has_stats() {
        // TableRefWithStats::from_table_stats should mark Analyze-sourced
        // entries as has_stats=true, Heuristic as false.
        let analyzed = TableStats {
            name: "t_analyzed".to_owned(),
            n_pages: 10,
            n_rows: 1000,
            source: StatsSource::Analyze,
        };
        let heur = TableStats {
            name: "t_heur".to_owned(),
            n_pages: 10,
            n_rows: 1000,
            source: StatsSource::Heuristic,
        };
        let a = TableRefWithStats::from_table_stats(&analyzed);
        let h = TableRefWithStats::from_table_stats(&heur);
        assert!(a.has_stats);
        assert!(!h.has_stats);
        assert_eq!(a.n_rows, 1000);
        assert_eq!(h.n_pages, 10);
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
    fn test_index_usability_multicolumn_trailing_in_expansion() {
        let idx = index_info("idx_ab", "t1", &["a", "b"], false, 50);
        let terms = [eq_term("a"), in_term("b", 3)];
        let result = analyze_index_usability(&idx, &terms);
        assert!(matches!(
            result,
            IndexUsability::MultiColumnEquality {
                eq_columns: 1,
                trailing_constraint: MultiColumnTrailingConstraint::InExpansion { probe_count: 3 }
            }
        ));
    }

    #[test]
    fn test_index_usability_multicolumn_trailing_like_prefix() {
        let idx = index_info("idx_ab", "t1", &["a", "b"], false, 50);
        let terms = [eq_term("a"), like_term("b", "123%")];
        let result = analyze_index_usability(&idx, &terms);
        assert!(matches!(
            result,
            IndexUsability::MultiColumnEquality {
                eq_columns: 1,
                trailing_constraint: MultiColumnTrailingConstraint::LikePrefix
            }
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
    fn test_best_access_path_multicolumn_trailing_in_refines_row_estimate() {
        let table = table_stats("t1", 1_000, 1_000_000);
        let idx = index_info("idx_ab", "t1", &["a", "b"], false, 80);
        let equality_only = [eq_term("a")];
        let trailing_in = [eq_term("a"), in_term("b", 3)];

        let ap_eq = best_access_path(&table, std::slice::from_ref(&idx), &equality_only, None);
        let ap_in = best_access_path(&table, &[idx], &trailing_in, None);

        assert_eq!(ap_in.index.as_deref(), Some("idx_ab"));
        assert!(matches!(ap_in.kind, AccessPathKind::IndexScanEquality));
        assert!(
            ap_in.estimated_rows < ap_eq.estimated_rows,
            "composite IN should narrow row estimates: eq_only={} trailing_in={}",
            ap_eq.estimated_rows,
            ap_in.estimated_rows
        );
        assert!(
            (ap_in.estimated_rows - 30_000.0).abs() < f64::EPSILON,
            "expected 1e6 / 10^2 * 3 = 30000 rows, got {}",
            ap_in.estimated_rows
        );
    }

    #[test]
    fn test_best_access_path_multicolumn_trailing_in_prefers_tighter_probe_count()
    -> Result<(), String> {
        let table = table_stats("t1", 1_000, 1_000_000);
        let idx = index_info("idx_ab", "t1", &["a", "b"], false, 80);
        let ap = best_access_path(
            &table,
            &[idx],
            &[eq_term("a"), in_term("b", 5), in_term("b", 2)],
            None,
        );

        if ap.index.as_deref() == Some("idx_ab") {
            if ap.kind == AccessPathKind::IndexScanEquality {
                if (ap.estimated_rows - 20_000.0).abs() < f64::EPSILON {
                    return Ok(());
                }
                return Err("expected tighter IN-list row estimate".to_owned());
            }
            return Err("expected equality access path".to_owned());
        }
        Err("expected idx_ab access path".to_owned())
    }
    #[test]
    fn test_best_access_path_multicolumn_or_disjunction_reuses_composite_in_expansion() {
        let table = table_stats("t1", 1_000, 1_000_000);
        let idx = index_info("idx_ab", "t1", &["a", "b"], false, 80);
        let term = or_eq_term("b", &[1, 2, 3, 4]);
        assert!(matches!(term.kind, WhereTermKind::InList { count: 4 }));

        let ap = best_access_path(&table, &[idx], &[eq_term("a"), term], None);

        assert_eq!(ap.index.as_deref(), Some("idx_ab"));
        assert!(matches!(ap.kind, AccessPathKind::IndexScanEquality));
        assert!(
            (ap.estimated_rows - 40_000.0).abs() < f64::EPSILON,
            "expected 1e6 / 10^2 * 4 = 40000 rows, got {}",
            ap.estimated_rows
        );
    }

    #[test]
    fn test_best_access_path_multicolumn_trailing_like_prefix_refines_row_estimate() {
        let table = table_stats("t1", 1_000, 1_000_000);
        let idx = index_info("idx_ab", "t1", &["a", "b"], false, 80);
        let equality_only = [eq_term("a")];
        let trailing_like = [eq_term("a"), like_term("b", "123%")];

        let ap_eq = best_access_path(&table, std::slice::from_ref(&idx), &equality_only, None);
        let ap_like = best_access_path(&table, &[idx], &trailing_like, None);

        assert_eq!(ap_like.index.as_deref(), Some("idx_ab"));
        assert!(matches!(
            ap_like.kind,
            AccessPathKind::IndexScanRange { .. }
        ));
        assert!(
            ap_like.estimated_rows < ap_eq.estimated_rows,
            "composite LIKE prefix should narrow row estimates: eq_only={} trailing_like={}",
            ap_eq.estimated_rows,
            ap_like.estimated_rows
        );
        assert!(
            (ap_like.estimated_rows - 10_000.0).abs() < f64::EPSILON,
            "expected 1e6 / 10 * 0.1 = 10000 rows, got {}",
            ap_like.estimated_rows
        );
    }

    #[test]
    fn test_best_access_path_multicolumn_trailing_glob_prefix_refines_row_estimate() {
        let table = table_stats("t1", 1_000, 1_000_000);
        let idx = index_info("idx_ab", "t1", &["a", "b"], false, 80);
        let trailing_glob = [eq_term("a"), glob_term("b", "abc*")];

        let ap = best_access_path(&table, &[idx], &trailing_glob, None);

        assert_eq!(ap.index.as_deref(), Some("idx_ab"));
        assert!(matches!(ap.kind, AccessPathKind::IndexScanRange { .. }));
        assert!(
            (ap.estimated_rows - 10_000.0).abs() < f64::EPSILON,
            "expected 1e6 / 10 * 0.1 = 10000 rows, got {}",
            ap.estimated_rows
        );
    }

    #[test]
    fn test_index_usability_like_not_usable() {
        let idx = index_info("idx_name", "t1", &["name"], false, 50);
        // ASCII LIKE prefixes remain unsafe under default SQLite semantics
        // because LIKE folds ASCII case.
        let terms = [like_term("name", "Jo%")];
        assert!(matches!(
            analyze_index_usability(&idx, &terms),
            IndexUsability::NotUsable
        ));

        let terms = [like_term("name", "%Jo%")];
        assert!(matches!(
            analyze_index_usability(&idx, &terms),
            IndexUsability::NotUsable
        ));
    }

    #[test]
    fn test_index_usability_like_case_stable_prefix() {
        let idx = index_info("idx_name", "t1", &["name"], false, 50);
        let terms = [like_term("name", "123%")];
        let result = analyze_index_usability(&idx, &terms);
        assert!(matches!(
            result,
            IndexUsability::LikePrefix {
                ref low,
                high: Some(ref high)
            } if low == "123" && high == "124"
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
    fn test_index_usability_leftmost_preserves_first_non_range_probe_order() {
        let idx = index_info("idx_name", "t1", &["name"], false, 50);
        let terms = [glob_term("name", "Jo*"), in_term("name", 3)];
        let result = analyze_index_usability(&idx, &terms);

        assert!(matches!(
            result,
            IndexUsability::LikePrefix {
                ref low,
                high: Some(ref high)
            } if low == "Jo" && high == "Jp"
        ));
    }

    #[test]
    fn test_index_usability_equality_beats_range_on_same_leftmost_column() {
        let idx = index_info("idx_a", "t1", &["a"], false, 50);
        let terms = [range_term("a"), eq_term("a")];

        assert!(matches!(
            analyze_index_usability(&idx, &terms),
            IndexUsability::Equality
        ));
    }

    #[test]
    fn test_index_usability_equality_beats_like_prefix_on_same_leftmost_column() {
        let idx = index_info("idx_name", "t1", &["name"], false, 50);
        let terms = [like_term("name", "123%"), eq_term("name")];

        assert!(matches!(
            analyze_index_usability(&idx, &terms),
            IndexUsability::Equality
        ));
    }

    /// Regression test for issue #63.
    ///
    /// Expression indexes store their real key terms in `expression_columns`
    /// and leave `columns` empty by convention (see the schema loader in
    /// fsqlite-core/src/connection.rs).  Before the fix, analyze_index_usability
    /// bailed out at the `columns.is_empty()` guard BEFORE checking
    /// `expression_columns`, so every expression index looked planner-dead
    /// and queries like `WHERE lower(name) = 'alice'` degraded to a full
    /// table scan despite a matching expression index being present.
    #[test]
    fn test_index_usability_expression_index_equality() {
        // Build a `lower(name)` expression that the index will match against.
        // The key_expression stored on the index is an identical AST so that
        // structural `PartialEq` succeeds.
        let lower_name_expr = |val: &'static str| -> &'static Expr {
            Box::leak(Box::new(Expr::BinaryOp {
                left: Box::new(Expr::FunctionCall {
                    name: "lower".to_owned(),
                    args: fsqlite_ast::FunctionArgs::List(vec![Expr::Column(
                        ColumnRef::bare("name"),
                        Span::ZERO,
                    )]),
                    distinct: false,
                    order_by: vec![],
                    filter: None,
                    over: None,
                    span: Span::ZERO,
                }),
                op: AstBinaryOp::Eq,
                right: Box::new(Expr::Literal(Literal::String(val.to_owned()), Span::ZERO)),
                span: Span::ZERO,
            }))
        };

        let where_expr = lower_name_expr("alice");
        // The index's recorded key expression is just `lower(name)` (no
        // equality wrapper), matching how connection.rs parses the DDL
        // expression string.
        let key_expr = Expr::FunctionCall {
            name: "lower".to_owned(),
            args: fsqlite_ast::FunctionArgs::List(vec![Expr::Column(
                ColumnRef::bare("name"),
                Span::ZERO,
            )]),
            distinct: false,
            order_by: vec![],
            filter: None,
            over: None,
            span: Span::ZERO,
        };

        let idx = IndexInfo {
            name: "idx_lower_name".to_owned(),
            table: "users".to_owned(),
            // Expression indexes leave `columns` empty by convention.
            columns: vec![],
            unique: false,
            n_pages: 50,
            source: StatsSource::Heuristic,
            partial_where: None,
            expression_columns: vec![key_expr],
        };

        let terms = [classify_where_term(where_expr)];
        assert!(
            matches!(
                analyze_index_usability(&idx, &terms),
                IndexUsability::Equality
            ),
            "expression index must reach analyze_expression_index_usability \
             even though `columns` is empty (issue #63)"
        );
    }

    /// SQL function names are case-insensitive, so `lower(name)` in the
    /// index key must match `LOWER(name)` in the WHERE clause.  Before the
    /// `eq_ignore_ascii_case` fix in `impl PartialEq for Expr`, this would
    /// silently fall back to a full scan.
    #[test]
    fn test_index_usability_expression_index_case_insensitive_function_name() {
        // Index key uses lowercase function name.
        let key_expr = Expr::FunctionCall {
            name: "lower".to_owned(),
            args: fsqlite_ast::FunctionArgs::List(vec![Expr::Column(
                ColumnRef::bare("name"),
                Span::ZERO,
            )]),
            distinct: false,
            order_by: vec![],
            filter: None,
            over: None,
            span: Span::ZERO,
        };

        // WHERE clause uses UPPERCASE function name.
        let where_expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::FunctionCall {
                name: "LOWER".to_owned(),
                args: fsqlite_ast::FunctionArgs::List(vec![Expr::Column(
                    ColumnRef::bare("name"),
                    Span::ZERO,
                )]),
                distinct: false,
                order_by: vec![],
                filter: None,
                over: None,
                span: Span::ZERO,
            }),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(
                Literal::String("alice".to_owned()),
                Span::ZERO,
            )),
            span: Span::ZERO,
        }));

        let idx = IndexInfo {
            name: "idx_lower_name".to_owned(),
            table: "users".to_owned(),
            columns: vec![],
            unique: false,
            n_pages: 50,
            source: StatsSource::Heuristic,
            partial_where: None,
            expression_columns: vec![key_expr],
        };

        let terms = [classify_where_term(where_expr)];
        assert!(
            matches!(
                analyze_index_usability(&idx, &terms),
                IndexUsability::Equality
            ),
            "case-insensitive function name match must reach Equality \
             (lower vs LOWER)"
        );
    }

    /// Expression-index regression companion: a non-matching WHERE term must
    /// still return NotUsable (i.e. the reordered guard does not accidentally
    /// widen acceptance).
    #[test]
    fn test_index_usability_expression_index_non_matching() {
        // Index is on `lower(name)`, but the WHERE clause uses `upper(name)`.
        let upper_name_eq: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::FunctionCall {
                name: "upper".to_owned(),
                args: fsqlite_ast::FunctionArgs::List(vec![Expr::Column(
                    ColumnRef::bare("name"),
                    Span::ZERO,
                )]),
                distinct: false,
                order_by: vec![],
                filter: None,
                over: None,
                span: Span::ZERO,
            }),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(
                Literal::String("ALICE".to_owned()),
                Span::ZERO,
            )),
            span: Span::ZERO,
        }));

        let key_expr = Expr::FunctionCall {
            name: "lower".to_owned(),
            args: fsqlite_ast::FunctionArgs::List(vec![Expr::Column(
                ColumnRef::bare("name"),
                Span::ZERO,
            )]),
            distinct: false,
            order_by: vec![],
            filter: None,
            over: None,
            span: Span::ZERO,
        };

        let idx = IndexInfo {
            name: "idx_lower_name".to_owned(),
            table: "users".to_owned(),
            columns: vec![],
            unique: false,
            n_pages: 50,
            source: StatsSource::Heuristic,
            partial_where: None,
            expression_columns: vec![key_expr],
        };

        let terms = [classify_where_term(upper_name_eq)];
        assert!(
            matches!(
                analyze_index_usability(&idx, &terms),
                IndexUsability::NotUsable
            ),
            "expression index must reject structurally-unrelated WHERE terms"
        );
    }

    /// Real-parser regression test for issue #63.
    ///
    /// The index key is parsed from its stand-alone SQL text (the way the
    /// schema loader in `fsqlite-core/src/connection.rs` builds
    /// `expression_columns`); the WHERE clause is extracted from a full
    /// SELECT parse where the `lower(name)` sub-expression lands at a
    /// non-zero byte offset inside the outer query.  This test asserts:
    ///
    /// 1. The two parse contexts really do produce different `Span`
    ///    byte offsets for the logically identical expression (sanity
    ///    check — if this ever stops being true the test loses its
    ///    teeth but the bug it guards against may come back).
    /// 2. `Expr::PartialEq` — manually implemented in fsqlite-ast to
    ///    skip the span field on every variant — still reports the
    ///    two expressions as equal despite the span mismatch.  A
    ///    future refactor that accidentally auto-derived `PartialEq`
    ///    would silently break the expression-index planner, so this
    ///    assertion catches that.
    /// 3. The full `analyze_index_usability` path reaches
    ///    `IndexUsability::Equality` for a real-parser round trip —
    ///    the end-to-end guarantee that the bounded repro from the
    ///    issue ships as an index lookup plan.
    #[test]
    fn test_index_usability_expression_index_real_parser_spans_differ() {
        use fsqlite_ast::{SelectCore, Statement};

        // Parse the index key the way the schema loader does: from its
        // stand-alone text, with spans starting at 0.
        let key_expr =
            fsqlite_parser::expr::parse_expr("lower(name)").expect("key expression should parse");

        // Parse a full SELECT so the WHERE clause's `lower(name)` lands
        // at a non-zero byte offset inside the outer query, exactly
        // matching how the planner sees it at runtime.
        let select_sql = "SELECT id FROM users WHERE lower(name) = 'alice'";
        let mut scratch = fsqlite_parser::StatementParseScratch::default();
        let statement =
            fsqlite_parser::parse_single_statement_with_scratch(select_sql, &mut scratch)
                .expect("select should parse");
        let Statement::Select(select) = statement else {
            panic!("expected SELECT statement");
        };
        let SelectCore::Select { where_clause, .. } = select.body.select else {
            panic!("expected SELECT core");
        };
        let where_expr = *where_clause.expect("WHERE clause must be present");
        let left_of_where = match &where_expr {
            Expr::BinaryOp { left, .. } => left.as_ref().clone(),
            _ => panic!("expected BinaryOp for `lower(name) = 'alice'`"),
        };

        // Sanity: the two spans really are different — if this ever stops
        // being true, the test's premise is wrong.
        assert_ne!(
            left_of_where.span(),
            key_expr.span(),
            "real parser should assign different spans across parse \
             contexts: stand-alone `lower(name)` starts at 0 but the \
             WHERE-side one starts after `SELECT id FROM users WHERE `"
        );

        // Span-insensitive structural equality must accept — this is
        // the property the planner relies on for expression-index
        // matching and it is provided by the manual `impl PartialEq
        // for Expr` in fsqlite-ast.  If someone ever changes that
        // impl to a derive, this assertion will fail loudly.
        assert_eq!(
            left_of_where, key_expr,
            "Expr::PartialEq is manually span-insensitive in fsqlite-ast; \
             if that invariant breaks, the expression-index planner stops \
             matching across parse contexts (issue #63)"
        );

        // And the full planner path should accept it end-to-end.
        let idx = IndexInfo {
            name: "idx_lower_name".to_owned(),
            table: "users".to_owned(),
            columns: vec![],
            unique: false,
            n_pages: 50,
            source: StatsSource::Heuristic,
            partial_where: None,
            expression_columns: vec![key_expr],
        };
        // Leak the parsed WHERE expression so the WhereTerm can hold a
        // reference with `'static` lifetime, matching the other tests.
        let leaked: &'static Expr = Box::leak(Box::new(where_expr));
        let terms = [classify_where_term(leaked)];
        assert!(
            matches!(
                analyze_index_usability(&idx, &terms),
                IndexUsability::Equality
            ),
            "real-parser expression index lookup must reach Equality"
        );
    }

    #[test]
    fn test_best_access_path_partial_index_expression_uses_probe_fallback() {
        let lower_name = || Expr::FunctionCall {
            name: "lower".to_owned(),
            args: fsqlite_ast::FunctionArgs::List(vec![Expr::Column(
                ColumnRef::bare("name"),
                Span::ZERO,
            )]),
            distinct: false,
            order_by: Vec::new(),
            filter: None,
            over: None,
            span: Span::ZERO,
        };
        let partial_predicate = || Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("active"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            span: Span::ZERO,
        };
        let expression_equality = Expr::BinaryOp {
            left: Box::new(lower_name()),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(
                Literal::String("alice".to_owned()),
                Span::ZERO,
            )),
            span: Span::ZERO,
        };
        let query_partial_predicate = partial_predicate();
        let terms = [
            classify_where_term(&expression_equality),
            classify_where_term(&query_partial_predicate),
        ];
        let index = IndexInfo {
            name: "idx_active_lower_name".to_owned(),
            table: "users".to_owned(),
            columns: Vec::new(),
            unique: false,
            n_pages: 10,
            source: StatsSource::Heuristic,
            partial_where: Some(partial_predicate()),
            expression_columns: vec![lower_name()],
        };
        let table = table_stats("users", 1_000, 100_000);

        let path = best_access_path(&table, &[index], &terms, None);
        assert_eq!(path.index.as_deref(), Some("idx_active_lower_name"));
        assert!(matches!(path.kind, AccessPathKind::IndexScanEquality));
        assert!(
            path.probe.is_none(),
            "expression-index targets are intentionally re-extracted by the core directive fallback"
        );
    }

    /// An index with neither `columns` nor `expression_columns` is degenerate
    /// and must still fall through to NotUsable.  Guards against the reorder
    /// accidentally exposing a new reachable code path.
    #[test]
    fn test_index_usability_empty_index_still_not_usable() {
        let idx = IndexInfo {
            name: "idx_empty".to_owned(),
            table: "t1".to_owned(),
            columns: vec![],
            unique: false,
            n_pages: 50,
            source: StatsSource::Heuristic,
            partial_where: None,
            expression_columns: vec![],
        };
        let terms = [eq_term("a")];
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
    fn test_associative_where_walkers_are_stack_safe_at_parser_height_boundary() {
        std::thread::Builder::new()
            .stack_size(1024 * 1024)
            .spawn(|| {
                let equality = |value| Expr::BinaryOp {
                    left: Box::new(Expr::Column(ColumnRef::bare("x"), Span::ZERO)),
                    op: AstBinaryOp::Eq,
                    right: Box::new(Expr::Literal(Literal::Integer(value), Span::ZERO)),
                    span: Span::ZERO,
                };

                for height in [1_000_i64, 1_001] {
                    let expected_count =
                        usize::try_from(height).expect("positive test height fits usize");
                    let mut conjunction = equality(0);
                    let mut disjunction = equality(0);
                    for value in 1..height {
                        conjunction = Expr::BinaryOp {
                            left: Box::new(conjunction),
                            op: AstBinaryOp::And,
                            right: Box::new(equality(value)),
                            span: Span::ZERO,
                        };
                        disjunction = Expr::BinaryOp {
                            left: Box::new(disjunction),
                            op: AstBinaryOp::Or,
                            right: Box::new(equality(value)),
                            span: Span::ZERO,
                        };
                    }

                    assert_eq!(decompose_where(&conjunction).len(), expected_count);
                    let (column, count) = classify_or_disjunction_as_in_list(&disjunction)
                        .expect("same-column equality disjunction must classify as an IN-list");
                    assert_eq!(column.column, "x");
                    assert_eq!(count, expected_count);
                    drop((conjunction, disjunction));
                }
            })
            .expect("one-MiB WHERE-walker test thread must spawn")
            .join()
            .expect("height-1000/1001 WHERE walkers must not overflow the native stack");
    }

    // ===================================================================
    // §10.5 Join ordering tests
    // ===================================================================

    #[test]
    fn test_join_ordering_single_table() {
        let tables = [table_stats("t1", 100, 1000)];
        let plan = order_joins(&tables, &[], &[], None, &[]);
        assert_eq!(plan.join_order, vec!["t1"]);
        // PLANNER-2: full scan cost = n_pages + n_rows * ROW_DECODE_COST.
        let expected = estimate_cost_ext(&AccessPathKind::FullTableScan, 100, 0, 1000);
        assert!((plan.total_cost - expected).abs() < 1e-9);
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
        let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::qualified("t2", "fk"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            span: Span::ZERO,
        }));
        let terms = [classify_where_term(expr)];
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
    fn test_single_table_source_name_and_alias() {
        use fsqlite_ast::{JoinClause, JoinKind, JoinType};

        // A single-table FROM with no joins yields Ok((name, alias)); any joins
        // or a non-Table source yield Err(UnsupportedFromSource).
        let tbl = |alias: Option<&str>| TableOrSubquery::Table {
            name: QualifiedName::bare("users"),
            alias: alias.map(str::to_owned),
            index_hint: None,
            time_travel: None,
        };
        let fc = |source: TableOrSubquery, joins: Vec<JoinClause>| FromClause { source, joins };

        // Bare table, no alias.
        let bare_fc = fc(tbl(None), vec![]);
        let (name, alias) = single_table_source_name_and_alias(&bare_fc).unwrap();
        assert_eq!(name, "users");
        assert_eq!(alias, None);

        // Bare table with an alias.
        let aliased_fc = fc(tbl(Some("u")), vec![]);
        let (name, alias) = single_table_source_name_and_alias(&aliased_fc).unwrap();
        assert_eq!(name, "users");
        assert_eq!(alias, Some("u"));

        // A join present -> Err.
        let with_join = fc(
            tbl(None),
            vec![JoinClause {
                join_type: JoinType {
                    natural: false,
                    kind: JoinKind::Inner,
                },
                table: tbl(None),
                constraint: None,
            }],
        );
        assert!(single_table_source_name_and_alias(&with_join).is_err());
    }

    #[test]
    fn test_from_clause_supports_leapfrog_branches() {
        use fsqlite_ast::{JoinClause, JoinConstraint, JoinKind, JoinType};

        // from_clause_supports_leapfrog gates leapfrog routing on join shape.
        // The routing tests only ever pass None (-> supported); the rejection
        // branches were never exercised directly.
        let tbl = |name: &str| TableOrSubquery::Table {
            name: QualifiedName::bare(name),
            alias: None,
            index_hint: None,
            time_travel: None,
        };
        let col = |name: &str| Expr::Column(ColumnRef::bare(name), Span::ZERO);
        let from = |jt: JoinType, constraint: Option<JoinConstraint>| FromClause {
            source: tbl("a"),
            joins: vec![JoinClause {
                join_type: jt,
                table: tbl("b"),
                constraint,
            }],
        };
        let inner = || JoinType {
            natural: false,
            kind: JoinKind::Inner,
        };

        // No FROM clause at all -> trivially supported.
        assert!(from_clause_supports_leapfrog(None));

        // Inner join with an equi-column ON predicate (x = y) is supported.
        let equi_on = Expr::BinaryOp {
            left: Box::new(col("x")),
            op: AstBinaryOp::Eq,
            right: Box::new(col("y")),
            span: Span::ZERO,
        };
        assert!(from_clause_supports_leapfrog(Some(&from(
            inner(),
            Some(JoinConstraint::On(equi_on))
        ))));

        // A non-empty USING constraint is supported.
        assert!(from_clause_supports_leapfrog(Some(&from(
            inner(),
            Some(JoinConstraint::Using(vec!["x".to_owned()]))
        ))));

        // Rejection: a non-equi ON (column = literal) is not equi-column.
        let nonequi_on = Expr::BinaryOp {
            left: Box::new(col("x")),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(5), Span::ZERO)),
            span: Span::ZERO,
        };
        assert!(!from_clause_supports_leapfrog(Some(&from(
            inner(),
            Some(JoinConstraint::On(nonequi_on))
        ))));

        // Rejection: an empty USING list.
        assert!(!from_clause_supports_leapfrog(Some(&from(
            inner(),
            Some(JoinConstraint::Using(vec![]))
        ))));

        // Rejection: a NATURAL join.
        assert!(!from_clause_supports_leapfrog(Some(&from(
            JoinType {
                natural: true,
                kind: JoinKind::Inner,
            },
            None
        ))));

        // Rejection: an outer (LEFT) join.
        assert!(!from_clause_supports_leapfrog(Some(&from(
            JoinType {
                natural: false,
                kind: JoinKind::Left,
            },
            None
        ))));
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
                time_travel: None,
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
                    time_travel: None,
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
                time_travel: None,
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
                    time_travel: None,
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
    fn test_join_indexed_by_keeps_unrelated_bare_predicate_residual() {
        let tables = [
            table_stats("users", 2_000, 100_000),
            table_stats("events", 5_000, 500_000),
        ];
        let index = index_info("idx_users_email", "users", &["email"], false, 100);
        let terms = [eq_term("user_id")];
        let hints = BTreeMap::from([(
            canonical_table_key("users"),
            IndexHint::IndexedBy("idx_users_email".to_owned()),
        )]);

        let plan = order_joins_with_hints(&tables, &[index], &terms, None, &[], Some(&hints), None);
        let users_path = plan
            .access_paths
            .iter()
            .find(|path| path.table.eq_ignore_ascii_case("users"))
            .expect("users path should exist");

        assert!(matches!(users_path.kind, AccessPathKind::FullTableScan));
        assert!(users_path.index.is_none());
        assert!(users_path.probe.is_none());
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
    fn test_planner_treats_rowid_projection_as_covering_index_payload() {
        let table = table_stats("t1", 1000, 50000);
        let idx = index_info("idx_t1_a", "t1", &["a"], false, 100);
        let terms = [eq_term("a")];
        let needed = ["rowid".to_owned()];
        let ap = best_access_path(&table, &[idx], &terms, Some(&needed));
        assert!(matches!(ap.kind, AccessPathKind::CoveringIndexScan { .. }));
    }

    #[test]
    fn test_planner_heuristic_fallback() {
        // Without any indexes, should fall back to full table scan.
        let table = table_stats("t1", 100, 1000);
        let ap = best_access_path(&table, &[], &[], None);
        assert!(matches!(ap.kind, AccessPathKind::FullTableScan));
        let expected = estimate_cost_ext(&AccessPathKind::FullTableScan, 100, 0, 1000);
        assert!((ap.estimated_cost - expected).abs() < 1e-9);
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
                    time_travel: None,
                    probe: None,
                },
                AccessPath {
                    table: "t2".to_owned(),
                    kind: AccessPathKind::IndexScanEquality,
                    index: Some("idx_t2".to_owned()),
                    estimated_cost: 15.0,
                    estimated_rows: 10.0,
                    time_travel: None,
                    probe: None,
                },
            ],
            join_segments: vec![JoinPlanSegment {
                relations: vec!["t1".to_owned(), "t2".to_owned()],
                operator: JoinOperator::HashJoin,
                estimated_cost: 115.0,
                reason: "2-way joins stay on pairwise hash join".to_owned(),
            }],
            total_cost: 115.0,
            morsel_eligibility: None,
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
            morsel_eligibility: None,
        };

        let display = plan.to_string();
        assert!(display.contains("LEAPFROG TRIEJOIN"));
        assert!(display.contains("JOIN OPERATORS"));
    }

    #[test]
    fn test_morsel_eligibility_full_scan_large_table() {
        let plan = QueryPlan {
            join_order: vec!["big_table".to_owned()],
            access_paths: vec![AccessPath {
                table: "big_table".to_owned(),
                kind: AccessPathKind::FullTableScan,
                index: None,
                estimated_cost: 10000.0,
                estimated_rows: 100_000.0,
                time_travel: None,
                probe: None,
            }],
            join_segments: vec![],
            total_cost: 10000.0,
            morsel_eligibility: None,
        };
        let elig = MorselEligibility::evaluate(&plan, false, false, 8);
        assert!(
            elig.eligible,
            "bead_id=bd-b434d case=morsel_eligible_full_scan"
        );
        assert_eq!(elig.driving_table.as_deref(), Some("big_table"));
        assert!(elig.morsel_count > 1);
        assert!(elig.morsel_count <= 64);
        eprintln!(
            "INFO bead_id=bd-b434d case=morsel_eligible morsels={} rows_per={}",
            elig.morsel_count, elig.rows_per_morsel
        );
    }

    #[test]
    fn test_morsel_eligibility_small_table_ineligible() {
        let plan = QueryPlan {
            join_order: vec!["small".to_owned()],
            access_paths: vec![AccessPath {
                table: "small".to_owned(),
                kind: AccessPathKind::FullTableScan,
                index: None,
                estimated_cost: 10.0,
                estimated_rows: 500.0,
                time_travel: None,
                probe: None,
            }],
            join_segments: vec![],
            total_cost: 10.0,
            morsel_eligibility: None,
        };
        let elig = MorselEligibility::evaluate(&plan, false, false, 8);
        assert!(!elig.eligible);
        assert_eq!(elig.reason, MorselIneligibleReason::TooFewRows);
    }

    #[test]
    fn test_morsel_eligibility_index_scan_ineligible() {
        let plan = QueryPlan {
            join_order: vec!["t1".to_owned()],
            access_paths: vec![AccessPath {
                table: "t1".to_owned(),
                kind: AccessPathKind::IndexScanEquality,
                index: Some("idx".to_owned()),
                estimated_cost: 5.0,
                estimated_rows: 10000.0,
                time_travel: None,
                probe: None,
            }],
            join_segments: vec![],
            total_cost: 5.0,
            morsel_eligibility: None,
        };
        let elig = MorselEligibility::evaluate(&plan, false, false, 8);
        assert!(!elig.eligible);
        assert_eq!(elig.reason, MorselIneligibleReason::NoFullTableScan);
    }

    #[test]
    fn test_morsel_eligibility_limit_ineligible() {
        let plan = QueryPlan {
            join_order: vec!["t1".to_owned()],
            access_paths: vec![AccessPath {
                table: "t1".to_owned(),
                kind: AccessPathKind::FullTableScan,
                index: None,
                estimated_cost: 1000.0,
                estimated_rows: 50000.0,
                time_travel: None,
                probe: None,
            }],
            join_segments: vec![],
            total_cost: 1000.0,
            morsel_eligibility: None,
        };
        let elig = MorselEligibility::evaluate(&plan, true, false, 8);
        assert!(!elig.eligible);
        assert_eq!(elig.reason, MorselIneligibleReason::HasLimit);
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
        // PLANNER-2: rowid lookup cost = log2(n_pages) + 1 * ROW_ACCESS_COST.
        let expected = estimate_cost_ext(&AccessPathKind::RowidLookup, 1024, 0, 50000);
        assert!((ap.estimated_cost - expected).abs() < 1e-9);
    }

    #[test]
    fn test_best_access_path_ipk_oltp_shapes_without_schema_context() {
        let table = table_stats("bench", 128, 5000);

        // The planner crate itself has no schema-aware INTEGER PRIMARY KEY
        // alias detection, so the mixed-OLTP benchmark's `id = ?1` shape is
        // still priced as a full scan until fsqlite-core upgrades it to a
        // rowid fast path after planning.
        let point = best_access_path(&table, &[], &[eq_term("id")], None);
        assert!(matches!(point.kind, AccessPathKind::FullTableScan));

        let lower_expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("id"), Span::ZERO)),
            op: AstBinaryOp::Ge,
            right: Box::new(Expr::Literal(Literal::Integer(100), Span::ZERO)),
            span: Span::ZERO,
        }));
        let upper_expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("id"), Span::ZERO)),
            op: AstBinaryOp::Lt,
            right: Box::new(Expr::Literal(Literal::Integer(150), Span::ZERO)),
            span: Span::ZERO,
        }));
        let range = best_access_path(
            &table,
            &[],
            &[
                classify_where_term(lower_expr),
                classify_where_term(upper_expr),
            ],
            None,
        );
        assert!(matches!(range.kind, AccessPathKind::FullTableScan));

        let aggregate = best_access_path(&table, &[], &[], None);
        assert!(matches!(aggregate.kind, AccessPathKind::FullTableScan));
    }

    #[test]
    fn test_best_access_path_ipk_alias_hint_uses_rowid_lookup() {
        let table = table_stats("bench", 128, 5000);
        let hints = [RowidAliasHint::new("id")];

        let point =
            best_access_path_with_rowid_alias_hints(&table, &[], &[eq_term("id")], None, &hints);

        assert!(matches!(point.kind, AccessPathKind::RowidLookup));
        assert_eq!(point.estimated_rows, 1.0);
        assert!(matches!(
            &point.probe,
            Some(AccessPathProbe::RowidEquality { target })
                if **target == Expr::Literal(Literal::Integer(1), Span::ZERO)
        ));

        let range =
            best_access_path_with_rowid_alias_hints(&table, &[], &[range_term("id")], None, &hints);
        assert!(matches!(range.kind, AccessPathKind::IndexScanRange { .. }));
        assert!(range.index.is_none());
        assert!(matches!(
            &range.probe,
            Some(AccessPathProbe::Range {
                column,
                lower: Some(_),
                ..
            }) if column == "id"
        ));
    }

    #[test]
    fn test_best_access_path_ipk_alias_hint_respects_qualifier() {
        let table = table_stats("bench", 128, 5000);
        let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::qualified("b", "id"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(7), Span::ZERO)),
            span: Span::ZERO,
        }));
        let terms = [classify_where_term(expr)];

        let table_only = [RowidAliasHint::new("id")];
        let miss = best_access_path_with_rowid_alias_hints(&table, &[], &terms, None, &table_only);
        assert!(matches!(miss.kind, AccessPathKind::FullTableScan));

        let qualified = [RowidAliasHint::qualified("b", "id")];
        let hit = best_access_path_with_rowid_alias_hints(&table, &[], &terms, None, &qualified);
        assert!(matches!(hit.kind, AccessPathKind::RowidLookup));
        assert!(matches!(
            &hit.probe,
            Some(AccessPathProbe::RowidEquality { target })
                if **target == Expr::Literal(Literal::Integer(7), Span::ZERO)
        ));
    }

    #[test]
    fn test_qualified_ipk_alias_column_comparison_stays_residual() {
        let table = table_stats("bench", 128, 5000);
        let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::qualified("b", "id"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Column(ColumnRef::qualified("b", "other"), Span::ZERO)),
            span: Span::ZERO,
        }));
        let terms = [classify_where_term(expr)];
        let qualified = [RowidAliasHint::qualified("b", "id")];

        let path = best_access_path_with_rowid_alias_hints(&table, &[], &terms, None, &qualified);

        assert!(matches!(path.kind, AccessPathKind::FullTableScan));
        assert!(path.index.is_none());
        assert!(path.probe.is_none());
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
        let expected = estimate_cost_ext(&AccessPathKind::FullTableScan, 500, 0, 10000);
        assert!((ap.estimated_cost - expected).abs() < 1e-9);
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
        let core = SelectCore::Values(
            vec![vec![
                Expr::Literal(Literal::Integer(1), Span::ZERO),
                Expr::Literal(Literal::Integer(2), Span::ZERO),
            ]]
            .into(),
        );
        assert_eq!(count_output_columns(&core), 2);
    }

    #[test]
    fn test_count_output_columns_empty_values() {
        let core = SelectCore::Values(vec![].into());
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
        let core =
            SelectCore::Values(vec![vec![Expr::Literal(Literal::Integer(1), Span::ZERO)]].into());
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
                    time_travel: None,
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
                        time_travel: None,
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
            vec![ResultColumn::TableStar(QualifiedName::bare("wrong_table"))],
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
    fn test_classify_where_term_like_is_other() {
        // ASCII LIKE prefixes remain unsafe because default SQLite LIKE folds
        // ASCII case, so range lowering would miss rows like 'ABC...'.
        let term = like_term("name", "abc%");
        assert!(matches!(term.kind, WhereTermKind::Other));

        let term = like_term("name", "%wildcard");
        assert!(matches!(term.kind, WhereTermKind::Other));
    }

    #[test]
    fn test_classify_where_term_like_case_stable_prefix() {
        let term = like_term("name", "123%");
        assert!(matches!(
            term.kind,
            WhereTermKind::LikePrefix {
                ref prefix,
                upper_bound: Some(ref upper_bound),
            } if prefix == "123" && upper_bound == "124"
        ));
        assert_eq!(term.column.as_ref().unwrap().column, "name");
    }

    #[test]
    fn test_classify_where_term_like_escape_case_stable_prefix() {
        let term = like_term_with_escape("name", "123\\%%", "\\");
        assert!(matches!(
            term.kind,
            WhereTermKind::LikePrefix {
                ref prefix,
                upper_bound: Some(ref upper_bound),
            } if prefix == "123%" && upper_bound == "123&"
        ));
        assert_eq!(term.column.as_ref().unwrap().column, "name");
    }

    #[test]
    fn test_classify_where_term_like_escape_ascii_prefix_is_other() {
        let term = like_term_with_escape("name", "abc\\%%", "\\");
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
    fn test_classify_where_term_eq_null_is_other() {
        // `col = NULL` is always NULL (unknown) in SQL — not a usable equality.
        let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("x"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Null, Span::ZERO)),
            span: Span::ZERO,
        }));
        let term = classify_where_term(expr);
        assert!(
            matches!(term.kind, WhereTermKind::Other),
            "col = NULL should be Other, got {:?}",
            term.kind
        );

        // Also check NULL = col (reversed)
        let expr2: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Literal(Literal::Null, Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Column(ColumnRef::bare("x"), Span::ZERO)),
            span: Span::ZERO,
        }));
        let term2 = classify_where_term(expr2);
        assert!(
            matches!(term2.kind, WhereTermKind::Other),
            "NULL = col should be Other, got {:?}",
            term2.kind
        );
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

    #[test]
    fn test_extract_glob_prefix_star_wildcard() {
        // "abc*" → prefix = "abc" (pure trailing-star prefix)
        let pat = Expr::Literal(Literal::String("abc*".to_owned()), Span::ZERO);
        assert_eq!(extract_glob_prefix(&pat), Some("abc".to_owned()));
    }

    #[test]
    fn test_extract_glob_prefix_rejects_non_terminal_wildcards() {
        let embedded_star = Expr::Literal(Literal::String("abc*def".to_owned()), Span::ZERO);
        assert_eq!(extract_glob_prefix(&embedded_star), None);

        let char_class = Expr::Literal(Literal::String("abc[0-9]".to_owned()), Span::ZERO);
        assert_eq!(extract_glob_prefix(&char_class), None);
    }

    #[test]
    fn test_extract_glob_prefix_non_string_expr() {
        // Non-string expression → None
        let pat = Expr::Literal(Literal::Integer(42), Span::ZERO);
        assert_eq!(extract_glob_prefix(&pat), None);
    }

    // ===================================================================
    // LIKE prefix extraction (bd-wwqen.6)
    // ===================================================================

    #[test]
    fn test_extract_like_prefix_percent_wildcard() {
        // "abc%" → prefix = "abc" (pure trailing-percent prefix)
        let pat = Expr::Literal(Literal::String("abc%".to_owned()), Span::ZERO);
        assert_eq!(extract_like_prefix(&pat, None), Some("abc".to_owned()));
    }

    #[test]
    fn test_extract_like_prefix_rejects_non_terminal_or_single_char_wildcards() {
        let embedded_percent = Expr::Literal(Literal::String("abc%def".to_owned()), Span::ZERO);
        assert_eq!(extract_like_prefix(&embedded_percent, None), None);

        let underscore = Expr::Literal(Literal::String("abc_def".to_owned()), Span::ZERO);
        assert_eq!(extract_like_prefix(&underscore, None), None);
    }

    #[test]
    fn test_extract_like_prefix_starts_with_wildcard() {
        // "%abc" → None (no constant prefix)
        let pat = Expr::Literal(Literal::String("%abc".to_owned()), Span::ZERO);
        assert_eq!(extract_like_prefix(&pat, None), None);

        // "_abc" → None (no constant prefix)
        let pat2 = Expr::Literal(Literal::String("_abc".to_owned()), Span::ZERO);
        assert_eq!(extract_like_prefix(&pat2, None), None);
    }

    #[test]
    fn test_extract_like_prefix_with_escape_percent_in_prefix() {
        let pat = Expr::Literal(Literal::String("123\\%%".to_owned()), Span::ZERO);
        let esc = Expr::Literal(Literal::String("\\".to_owned()), Span::ZERO);
        assert_eq!(
            extract_like_prefix(&pat, Some(&esc)),
            Some("123%".to_owned())
        );
    }

    #[test]
    fn test_extract_like_prefix_with_escape_underscore_in_prefix() {
        let pat = Expr::Literal(Literal::String("123!_%".to_owned()), Span::ZERO);
        let esc = Expr::Literal(Literal::String("!".to_owned()), Span::ZERO);
        assert_eq!(
            extract_like_prefix(&pat, Some(&esc)),
            Some("123_".to_owned())
        );
    }

    #[test]
    fn test_extract_like_prefix_with_invalid_escape_literal() {
        let pat = Expr::Literal(Literal::String("123\\%%".to_owned()), Span::ZERO);
        let esc = Expr::Literal(Literal::String("xx".to_owned()), Span::ZERO);
        assert_eq!(extract_like_prefix(&pat, Some(&esc)), None);
    }

    #[test]
    fn test_extract_like_prefix_non_string_expr() {
        let pat = Expr::Literal(Literal::Integer(42), Span::ZERO);
        assert_eq!(extract_like_prefix(&pat, None), None);
    }

    #[test]
    fn test_extract_like_prefix_exact_match() {
        // "abc" (no wildcards) is not a prefix-range probe.
        let pat = Expr::Literal(Literal::String("abc".to_owned()), Span::ZERO);
        assert_eq!(extract_like_prefix(&pat, None), None);
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
    fn test_best_access_path_like_no_index() {
        let table = table_stats("t1", 100, 1000);
        let idx = index_info("idx_name", "t1", &["name"], false, 20);
        let terms = [like_term("name", "Jo%")];
        let ap = best_access_path(&table, &[idx], &terms, None);
        // ASCII LIKE prefixes remain unsafe under default SQLite semantics, so
        // a full table scan is expected.
        assert!(
            matches!(ap.kind, AccessPathKind::FullTableScan),
            "LIKE should fall back to full scan, got {:?}",
            ap.kind
        );
    }

    #[test]
    fn test_best_access_path_like_case_stable_prefix_uses_index_scan() {
        let table = table_stats("t1", 100, 1000);
        let idx = index_info("idx_name", "t1", &["name"], false, 20);
        let terms = [like_term("name", "123%")];
        let ap = best_access_path(&table, &[idx], &terms, None);
        assert!(
            matches!(ap.kind, AccessPathKind::IndexScanRange { .. }),
            "case-stable LIKE prefix should use index scan, got {:?}",
            ap.kind
        );
    }

    #[test]
    fn test_best_access_path_like_escape_case_stable_prefix_uses_index_scan() {
        let table = table_stats("t1", 100, 1000);
        let idx = index_info("idx_name", "t1", &["name"], false, 20);
        let terms = [like_term_with_escape("name", "123\\%%", "\\")];
        let ap = best_access_path(&table, &[idx], &terms, None);
        assert!(
            matches!(ap.kind, AccessPathKind::IndexScanRange { .. }),
            "escaped case-stable LIKE prefix should use index scan, got {:?}",
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
    fn test_estimate_skip_scan_leading_distinct() {
        // (n_pages / SKIP_SCAN_PAGES_PER_LEADING_DISTINCT=8).max(1): proportional
        // to the page count, floored at 1.
        let idx = |n_pages: u64| index_info("idx", "t", &["a", "b"], false, n_pages);
        assert_eq!(estimate_skip_scan_leading_distinct(&idx(0)), 1); // floor
        assert_eq!(estimate_skip_scan_leading_distinct(&idx(7)), 1); // 7/8 = 0 -> 1
        assert_eq!(estimate_skip_scan_leading_distinct(&idx(8)), 1); // 8/8 = 1
        assert_eq!(estimate_skip_scan_leading_distinct(&idx(24)), 3); // 24/8 = 3
        assert_eq!(estimate_skip_scan_leading_distinct(&idx(80)), 10); // 80/8 = 10
    }

    #[test]
    fn test_estimate_pairwise_hash_join_cost_left_deep_accumulation() {
        // Left-deep hash-join cost model: each join step charges build+probe
        // (scanning both inputs, written as min+max which equals their sum) and
        // grows the running intermediate cardinality by a factor of the join
        // selectivity heuristic (0.25). A single relation costs nothing.
        // estimate_pairwise_hash_join_cost has no direct unit test, only
        // indirect coverage inside best_access_path.

        // Fewer than two relations: nothing to join, zero cost.
        assert!(estimate_pairwise_hash_join_cost(&["A".to_owned()], &HashMap::new()).abs() < 1e-9);
        let empty: Vec<String> = vec![];
        assert!(estimate_pairwise_hash_join_cost(&empty, &HashMap::new()).abs() < 1e-9);

        let rows = |pairs: &[(&str, f64)]| -> HashMap<String, f64> {
            pairs.iter().map(|&(t, n)| (t.to_owned(), n)).collect()
        };

        // Two relations A(100) |><| B(250): cost is just the two scans, 100+250,
        // independent of selectivity (the intermediate is never reused).
        let ab = estimate_pairwise_hash_join_cost(
            &["A".to_owned(), "B".to_owned()],
            &rows(&[("A", 100.0), ("B", 250.0)]),
        );
        assert!(
            (ab - 350.0).abs() < 1e-9,
            "two-table cost should be 100+250, got {ab}"
        );

        // Three relations A(100), B(250), C(40): after A|><|B the intermediate is
        // 100*250*0.25 = 6250, so the third step charges 6250+40. Total =
        // (100+250) + (6250+40) = 6640.
        let abc = estimate_pairwise_hash_join_cost(
            &["A".to_owned(), "B".to_owned(), "C".to_owned()],
            &rows(&[("A", 100.0), ("B", 250.0), ("C", 40.0)]),
        );
        assert!(
            (abc - 6640.0).abs() < 1e-9,
            "three-table cost should be 6640, got {abc}"
        );

        // Unknown tables default to 1 row (floored at 1.0): cost 1 + 1 = 2.
        let defaulted =
            estimate_pairwise_hash_join_cost(&["X".to_owned(), "Y".to_owned()], &HashMap::new());
        assert!(
            (defaulted - 2.0).abs() < 1e-9,
            "missing rows default to 1 -> 2, got {defaulted}"
        );
    }

    #[test]
    fn test_estimate_agm_upper_bound_triangle_and_guards() {
        // The AGM (Atserias-Grohe-Marx) fractional-cover bound on worst-case join
        // output. The textbook case is the triangle query R(A,B) |><| S(B,C) |><|
        // T(A,C): every variable has degree 2, so each relation's exponent is
        // max(1/2, 1/2) = 1/2 and the bound is (N_R * N_S * N_T)^(1/2). With all
        // three relations at N=100 rows this is 100^(3/2) = 1000 -- the classic
        // sub-N^3 bound. estimate_agm_upper_bound has no direct unit test (only
        // indirect coverage through best_access_path).
        let triangle = TrieHypergraph {
            relation_variables: vec![vec![0, 1], vec![1, 2], vec![0, 2]],
            variable_count: 3,
            arity: 2,
        };
        let component = vec!["R".to_owned(), "S".to_owned(), "T".to_owned()];
        let mut rows: HashMap<String, f64> = HashMap::new();
        rows.insert("R".to_owned(), 100.0);
        rows.insert("S".to_owned(), 100.0);
        rows.insert("T".to_owned(), 100.0);

        let bound = estimate_agm_upper_bound(&component, &rows, &triangle).unwrap();
        assert!(
            (bound - 1000.0).abs() < 1e-6,
            "triangle bound should be 100^1.5 = 1000, got {bound}"
        );

        // A component whose length does not match the relation count is rejected.
        let two = vec!["R".to_owned(), "S".to_owned()];
        assert!(estimate_agm_upper_bound(&two, &rows, &triangle).is_none());

        // An empty hypergraph (variable_count == 0) is rejected.
        let empty_hg = TrieHypergraph {
            relation_variables: vec![],
            variable_count: 0,
            arity: 0,
        };
        let empty_component: Vec<String> = vec![];
        assert!(estimate_agm_upper_bound(&empty_component, &rows, &empty_hg).is_none());

        // Missing row counts default to 1 and the bound is floored at 1.0.
        let no_rows: HashMap<String, f64> = HashMap::new();
        let floored = estimate_agm_upper_bound(&component, &no_rows, &triangle).unwrap();
        assert!(
            (floored - 1.0).abs() < 1e-9,
            "missing row counts default to 1 -> bound 1.0, got {floored}"
        );
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
    fn test_best_access_path_skip_scan_allows_immediate_second_column_on_three_column_index() {
        let table = TableStats {
            name: "users".to_owned(),
            n_pages: 4_096,
            n_rows: 2_000_000,
            source: StatsSource::Analyze,
        };
        let idx = IndexInfo {
            name: "idx_tenant_region_email".to_owned(),
            table: "users".to_owned(),
            columns: vec![
                "tenant_id".to_owned(),
                "region_code".to_owned(),
                "email".to_owned(),
            ],
            unique: false,
            n_pages: 64,
            source: StatsSource::Analyze,
            partial_where: None,
            expression_columns: vec![],
        };

        let ap = best_access_path(&table, &[idx], &[eq_term("region_code")], None);
        assert_eq!(ap.index.as_deref(), Some("idx_tenant_region_email"));
        assert!(matches!(
            ap.kind,
            AccessPathKind::IndexScanRange { .. } | AccessPathKind::CoveringIndexScan { .. }
        ));
    }

    #[test]
    fn test_best_access_path_skip_scan_rejects_gapped_trailing_column() {
        let table = TableStats {
            name: "users".to_owned(),
            n_pages: 4_096,
            n_rows: 2_000_000,
            source: StatsSource::Analyze,
        };
        let idx = IndexInfo {
            name: "idx_tenant_region_email".to_owned(),
            table: "users".to_owned(),
            columns: vec![
                "tenant_id".to_owned(),
                "region_code".to_owned(),
                "email".to_owned(),
            ],
            unique: false,
            n_pages: 64,
            source: StatsSource::Analyze,
            partial_where: None,
            expression_columns: vec![],
        };

        let ap = best_access_path(&table, &[idx], &[eq_term("email")], None);
        assert!(
            matches!(ap.kind, AccessPathKind::FullTableScan),
            "gapped skip-scan should fall back to full scan until multi-prefix cardinality is modeled, got {:?}",
            ap.kind
        );
    }

    #[test]
    fn test_skip_scan_candidate_second_column_equality_beats_range_ordering() {
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

        let candidate =
            analyze_skip_scan_candidate(&table, &idx, &[range_term("email"), eq_term("email")])
                .expect("second-column equality should remain a skip-scan candidate");

        assert_eq!(candidate.leading_probes, 8);
        assert_eq!(candidate.trailing_probe_count, 1);
        assert_eq!(candidate.per_probe_selectivity, SKIP_SCAN_EQ_SELECTIVITY);
    }

    #[test]
    fn test_skip_scan_candidate_second_column_in_beats_range_ordering() {
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

        let candidate =
            analyze_skip_scan_candidate(&table, &idx, &[range_term("email"), in_term("email", 3)])
                .expect("second-column IN-list should remain a skip-scan candidate");

        assert_eq!(candidate.leading_probes, 8);
        assert_eq!(candidate.trailing_probe_count, 3);
        assert_eq!(candidate.per_probe_selectivity, SKIP_SCAN_EQ_SELECTIVITY);
    }

    #[test]
    fn test_skip_scan_candidate_second_column_prefers_tighter_in_probe_count() -> Result<(), String>
    {
        let table = TableStats {
            name: "users".to_owned(),
            n_pages: 4_096,
            n_rows: 2_000_000,
            source: StatsSource::Analyze,
        };
        let idx = IndexInfo {
            name: "idx_tenant_email".to_owned(),
            table: "users".to_owned(),
            columns: ["tenant_id".to_owned(), "email".to_owned()]
                .into_iter()
                .collect(),
            unique: false,
            n_pages: 64,
            source: StatsSource::Analyze,
            partial_where: None,
            expression_columns: Vec::new(),
        };

        let candidate =
            analyze_skip_scan_candidate(&table, &idx, &[in_term("email", 5), in_term("email", 2)])
                .ok_or_else(|| "expected skip-scan candidate".to_owned())?;

        if candidate.leading_probes == 8
            && candidate.trailing_probe_count == 2
            && candidate.per_probe_selectivity == SKIP_SCAN_EQ_SELECTIVITY
        {
            return Ok(());
        }

        Err("expected tighter second-column IN probe count".to_owned())
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
    fn test_best_access_path_partial_index_accepts_commuted_equality() {
        let table = table_stats("t1", 100, 1000);
        let mut partial_idx = index_info("idx_partial_a", "t1", &["a"], false, 20);
        partial_idx.partial_where = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            span: Span::ZERO,
        });

        let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            span: Span::ZERO,
        }));
        let ap = best_access_path(&table, &[partial_idx], &[classify_where_term(expr)], None);
        assert!(matches!(
            ap.kind,
            AccessPathKind::IndexScanEquality | AccessPathKind::CoveringIndexScan { .. }
        ));
    }

    #[test]
    fn test_best_access_path_partial_index_rejects_unproven_cross_bound_ordering() {
        let table = table_stats("t1", 100, 1000);
        let mut partial_idx = index_info("idx_partial_a", "t1", &["a"], false, 20);
        partial_idx.partial_where = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            op: AstBinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Integer(0), Span::ZERO)),
            span: Span::ZERO,
        });

        let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            op: AstBinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Integer(10), Span::ZERO)),
            span: Span::ZERO,
        }));
        let ap = best_access_path(&table, &[partial_idx], &[classify_where_term(expr)], None);
        assert!(
            matches!(ap.kind, AccessPathKind::FullTableScan),
            "literal ordering is affinity-dependent and must not prove partial-index implication"
        );
    }

    #[test]
    fn test_best_access_path_partial_index_rejects_unproven_string_collation_ordering() {
        let table = table_stats("t1", 100, 1000);
        let mut partial_idx = index_info("idx_partial_a", "t1", &["a"], false, 20);
        partial_idx.partial_where = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            op: AstBinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::String("a".to_owned()), Span::ZERO)),
            span: Span::ZERO,
        });

        let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            op: AstBinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::String("z".to_owned()), Span::ZERO)),
            span: Span::ZERO,
        }));
        let ap = best_access_path(&table, &[partial_idx], &[classify_where_term(expr)], None);
        assert!(
            matches!(ap.kind, AccessPathKind::FullTableScan),
            "Rust string ordering must not stand in for an unknown SQLite collation"
        );
    }

    #[test]
    fn test_best_access_path_partial_index_rejects_unbound_qualifier_match() {
        let table = table_stats("t1", 100, 1000);
        let mut partial_idx = index_info("idx_partial_a", "t1", &["a"], false, 20);
        partial_idx.partial_where = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            span: Span::ZERO,
        });

        let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::qualified("other", "a"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            span: Span::ZERO,
        }));
        let ap = best_access_path(
            &table,
            std::slice::from_ref(&partial_idx),
            &[classify_where_term(expr)],
            None,
        );
        assert!(
            matches!(ap.kind, AccessPathKind::FullTableScan),
            "an unqualified index predicate must not treat another table's qualifier as a wildcard"
        );

        let qualified_other_predicate = expr.clone();
        partial_idx.partial_where = Some(qualified_other_predicate);
        assert!(
            !expr_implies_partial_predicate(
                expr,
                partial_idx
                    .partial_where
                    .as_ref()
                    .expect("test predicate is present"),
                "t1",
                true,
            ),
            "the structural-identity shortcut must still validate qualifiers"
        );
        let ap = best_access_path(
            &table,
            std::slice::from_ref(&partial_idx),
            &[classify_where_term(expr)],
            None,
        );
        assert!(
            matches!(ap.kind, AccessPathKind::FullTableScan),
            "structurally identical predicates bound to another table must not bypass qualifier validation"
        );

        let target_expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::qualified("t1", "a"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            span: Span::ZERO,
        }));
        partial_idx.partial_where = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            span: Span::ZERO,
        });
        let ap = best_access_path(
            &table,
            &[partial_idx],
            &[classify_where_term(target_expr)],
            None,
        );
        assert!(matches!(
            ap.kind,
            AccessPathKind::IndexScanEquality | AccessPathKind::CoveringIndexScan { .. }
        ));
    }

    #[test]
    fn test_partial_index_arbitrary_predicate_normalizes_local_alias_and_case() {
        let abs_gt_zero =
            |table: Option<&str>, function_name: &str, column_name: &str| Expr::BinaryOp {
                left: Box::new(Expr::FunctionCall {
                    name: function_name.to_owned(),
                    args: fsqlite_ast::FunctionArgs::List(vec![Expr::Column(
                        table.map_or_else(
                            || ColumnRef::bare(column_name),
                            |qualifier| ColumnRef::qualified(qualifier, column_name),
                        ),
                        Span::ZERO,
                    )]),
                    distinct: false,
                    order_by: Vec::new(),
                    filter: None,
                    over: None,
                    span: Span::ZERO,
                }),
                op: AstBinaryOp::Gt,
                right: Box::new(Expr::Literal(Literal::Integer(0), Span::ZERO)),
                span: Span::ZERO,
            };
        let qualified_a_eq_one: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::qualified("p", "a"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            span: Span::ZERO,
        }));
        let local_predicate: &'static Expr =
            Box::leak(Box::new(abs_gt_zero(Some("p"), "ABS", "A")));
        let foreign_predicate: &'static Expr =
            Box::leak(Box::new(abs_gt_zero(Some("other"), "ABS", "A")));

        let mut partial_idx = index_info("idx_p_partial_a", "p", &["a"], false, 20);
        partial_idx.partial_where = Some(abs_gt_zero(None, "abs", "a"));
        let table = table_stats("p", 100, 1_000);
        let local_terms = [
            classify_where_term(qualified_a_eq_one),
            classify_where_term(local_predicate),
        ];
        let local_path = best_access_path(
            &table,
            std::slice::from_ref(&partial_idx),
            &local_terms,
            None,
        );
        assert_eq!(
            local_path.index.as_deref(),
            Some("idx_p_partial_a"),
            "the visible alias and identifier case should normalize within a proven single-table scope"
        );

        let foreign_terms = [
            classify_where_term(qualified_a_eq_one),
            classify_where_term(foreign_predicate),
        ];
        let foreign_path = best_access_path(&table, &[partial_idx], &foreign_terms, None);
        assert!(
            matches!(foreign_path.kind, AccessPathKind::FullTableScan),
            "a foreign qualifier must not prove the indexed table's arbitrary partial predicate"
        );
    }

    #[test]
    fn test_join_partial_index_proof_retains_same_row_residual_without_probing_it() {
        let comparison =
            |left_table: &str, left_column: &str, right: Expr, op: AstBinaryOp| -> &'static Expr {
                Box::leak(Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Column(
                        ColumnRef::qualified(left_table, left_column),
                        Span::ZERO,
                    )),
                    op,
                    right: Box::new(right),
                    span: Span::ZERO,
                }))
            };
        let access_expr = comparison(
            "t1",
            "a",
            Expr::Literal(Literal::Integer(1), Span::ZERO),
            AstBinaryOp::Eq,
        );
        let local_residual = comparison(
            "t1",
            "b",
            Expr::Column(ColumnRef::qualified("t1", "c"), Span::ZERO),
            AstBinaryOp::Eq,
        );
        let foreign_residual = comparison(
            "t2",
            "b",
            Expr::Column(ColumnRef::qualified("t2", "c"), Span::ZERO),
            AstBinaryOp::Eq,
        );
        let access_term = classify_where_term(access_expr);
        let local_residual_term = classify_where_term(local_residual);
        assert!(
            bind_where_term_to_table(&local_residual_term, "t1", &[]).is_none(),
            "a same-row column comparison is a residual, not a pre-scan index probe"
        );

        let mut partial_idx = index_info("idx_t1_partial_a", "t1", &["a"], false, 20);
        partial_idx.partial_where = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("b"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Column(ColumnRef::bare("c"), Span::ZERO)),
            span: Span::ZERO,
        });
        let table = table_stats("t1", 100, 1_000);
        let context = JoinAccessPathContext {
            table_index_hints: None,
            cracking_hints: None,
            available_outer_tables: &[],
            unqualified_terms_are_table_local: false,
        };
        let local_terms = [access_term.clone(), local_residual_term];
        let local_path = join_access_path(
            &table,
            std::slice::from_ref(&partial_idx),
            &local_terms,
            None,
            context,
        );
        assert_eq!(local_path.index.as_deref(), Some("idx_t1_partial_a"));
        assert!(
            matches!(
                local_path.probe,
                Some(AccessPathProbe::Equality {
                    target,
                    ..
                }) if matches!(target.as_ref(), Expr::Literal(Literal::Integer(1), _))
            ),
            "only the executable a=1 term should become the index probe"
        );

        let foreign_terms = [access_term, classify_where_term(foreign_residual)];
        let foreign_path = join_access_path(&table, &[partial_idx], &foreign_terms, None, context);
        assert!(
            matches!(foreign_path.kind, AccessPathKind::FullTableScan),
            "another table's same-row residual must not prove t1's partial predicate"
        );
    }

    #[test]
    fn test_join_planning_rejects_bare_partial_predicate_identity() {
        let tables = [table_stats("t1", 100, 1000), table_stats("t2", 100, 1000)];
        let mut partial_idx = index_info("idx_partial_a", "t1", &["a"], false, 20);
        partial_idx.partial_where = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            span: Span::ZERO,
        });

        // `order_joins` receives one global WHERE-term set. Without completed
        // name-resolution metadata, bare `a = 1` cannot be attributed to `t1`
        // merely because it is structurally identical to t1's partial predicate.
        let plan = order_joins(&tables, &[partial_idx], &[eq_term_value("a", 1)], None, &[]);
        let t1_path = plan
            .access_paths
            .iter()
            .find(|path| path.table.eq_ignore_ascii_case("t1"))
            .expect("join plan must include t1");
        assert!(
            matches!(t1_path.kind, AccessPathKind::FullTableScan),
            "ambiguous bare term selected partial index: {t1_path:?}"
        );
    }

    #[test]
    fn test_join_planning_rejects_bare_terms_for_all_access_paths() {
        let tables = [table_stats("t1", 100, 1000), table_stats("t2", 100, 1000)];
        let index = index_info("idx_t1_a", "t1", &["a"], false, 20);

        let plan = order_joins(
            &tables,
            std::slice::from_ref(&index),
            &[eq_term_value("a", 1)],
            None,
            &[],
        );
        let t1_path = plan
            .access_paths
            .iter()
            .find(|path| path.table.eq_ignore_ascii_case("t1"))
            .expect("join plan must include t1");
        assert!(
            matches!(t1_path.kind, AccessPathKind::FullTableScan),
            "ambiguous bare term selected an ordinary index: {t1_path:?}"
        );

        let bare_rowid_plan = order_joins(&tables, &[], &[eq_term_value("rowid", 1)], None, &[]);
        assert!(
            bare_rowid_plan
                .access_paths
                .iter()
                .all(|path| matches!(path.kind, AccessPathKind::FullTableScan)),
            "ambiguous bare rowid selected a table-specific lookup: {bare_rowid_plan:?}"
        );

        let qualified_expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::qualified("t1", "a"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            span: Span::ZERO,
        }));
        let qualified_plan = order_joins(
            &tables,
            &[index.clone()],
            &[classify_where_term(qualified_expr)],
            None,
            &[],
        );
        let qualified_t1_path = qualified_plan
            .access_paths
            .iter()
            .find(|path| path.table.eq_ignore_ascii_case("t1"))
            .expect("join plan must include t1");
        assert!(
            matches!(
                qualified_t1_path.kind,
                AccessPathKind::IndexScanEquality | AccessPathKind::CoveringIndexScan { .. }
            ),
            "qualified t1 term should remain indexable: {qualified_t1_path:?}"
        );
        assert!(
            matches!(
                qualified_t1_path.probe,
                Some(AccessPathProbe::Equality { .. })
            ),
            "qualified index selection must retain its executable probe"
        );

        let commuted_join_expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::qualified("t2", "a"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Column(ColumnRef::qualified("t1", "a"), Span::ZERO)),
            span: Span::ZERO,
        }));
        let commuted_term = classify_where_term(commuted_join_expr);
        assert!(
            bind_where_term_to_table(&commuted_term, "t1", &[]).is_none(),
            "a join probe must not reference a table absent from the outer prefix"
        );
        let available_t2 = vec!["t2".to_owned()];
        assert!(
            bind_where_term_to_table(&commuted_term, "t1", &available_t2).is_some(),
            "a commuted join equality should become indexable after its probe table is outer"
        );
        let commuted_tables = [
            table_stats("t1", 100, 1000),
            // Make t2 the unambiguously cheaper outer table. The t1 index may
            // then use t2.a as a probe without referencing a future table.
            table_stats("t2", 1, 10),
        ];
        let commuted_plan = order_joins(&commuted_tables, &[index], &[commuted_term], None, &[]);
        assert_eq!(
            commuted_plan.join_order.first().map(String::as_str),
            Some("t2")
        );
        let commuted_t1_path = commuted_plan
            .access_paths
            .iter()
            .find(|path| path.table.eq_ignore_ascii_case("t1"))
            .expect("join plan must include t1");
        assert!(matches!(
            commuted_t1_path.kind,
            AccessPathKind::IndexScanEquality | AccessPathKind::CoveringIndexScan { .. }
        ));
        assert!(matches!(
            commuted_t1_path.probe,
            Some(AccessPathProbe::Equality {
                target: ref probe,
                ..
            }) if matches!(
                probe.as_ref(),
                Expr::Column(column, _) if column.table.as_deref() == Some("t2")
            )
        ));

        let nested_bare_probe_expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::qualified("t1", "a"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::qualified("t2", "a"), Span::ZERO)),
                op: AstBinaryOp::Add,
                right: Box::new(Expr::Column(ColumnRef::bare("b"), Span::ZERO)),
                span: Span::ZERO,
            }),
            span: Span::ZERO,
        }));
        assert!(
            bind_where_term_to_table(
                &classify_where_term(nested_bare_probe_expr),
                "t1",
                &available_t2,
            )
            .is_none(),
            "a nested ambiguous bare column must not masquerade as an available probe"
        );

        let between_join_expr: &'static Expr = Box::leak(Box::new(Expr::Between {
            expr: Box::new(Expr::Column(ColumnRef::qualified("t1", "a"), Span::ZERO)),
            low: Box::new(Expr::Column(ColumnRef::qualified("t2", "a"), Span::ZERO)),
            high: Box::new(Expr::Literal(Literal::Integer(9), Span::ZERO)),
            not: false,
            span: Span::ZERO,
        }));
        let between_term = classify_where_term(between_join_expr);
        assert!(bind_where_term_to_table(&between_term, "t1", &[]).is_none());
        assert!(bind_where_term_to_table(&between_term, "t1", &available_t2).is_some());

        let or_join_expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::qualified("t1", "a"), Span::ZERO)),
                op: AstBinaryOp::Eq,
                right: Box::new(Expr::Column(ColumnRef::qualified("t2", "a"), Span::ZERO)),
                span: Span::ZERO,
            }),
            op: AstBinaryOp::Or,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::qualified("t1", "a"), Span::ZERO)),
                op: AstBinaryOp::Eq,
                right: Box::new(Expr::Literal(Literal::Integer(7), Span::ZERO)),
                span: Span::ZERO,
            }),
            span: Span::ZERO,
        }));
        let or_term = classify_where_term(or_join_expr);
        assert!(matches!(or_term.kind, WhereTermKind::InList { count: 2 }));
        assert!(bind_where_term_to_table(&or_term, "t1", &[]).is_none());
        assert!(bind_where_term_to_table(&or_term, "t1", &available_t2).is_some());
        assert!(matches!(
            extract_in_list_probe(or_join_expr, "t1", "a"),
            Some(AccessPathProbe::InList { values, .. })
                if values.len() == 2
                    && matches!(
                        values[0].as_ref(),
                        Expr::Column(column, _)
                            if column.table.as_deref() == Some("t2")
                    )
        ));

        let ambiguous_or_expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new((*commuted_join_expr).clone()),
            op: AstBinaryOp::Or,
            right: Box::new((*commuted_join_expr).clone()),
            span: Span::ZERO,
        }));
        assert!(
            matches!(
                classify_where_term(ambiguous_or_expr).kind,
                WhereTermKind::Other
            ),
            "an OR with two equally shared columns has no unambiguous IN-list target"
        );

        let tautological_disjunct = Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::qualified("t1", "a"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Column(ColumnRef::qualified("t1", "a"), Span::ZERO)),
            span: Span::ZERO,
        };
        let tautological_or_expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(tautological_disjunct),
            op: AstBinaryOp::Or,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::qualified("t1", "a"), Span::ZERO)),
                op: AstBinaryOp::Eq,
                right: Box::new(Expr::Literal(Literal::Integer(7), Span::ZERO)),
                span: Span::ZERO,
            }),
            span: Span::ZERO,
        }));
        assert!(
            matches!(
                classify_where_term(tautological_or_expr).kind,
                WhereTermKind::Other
            ),
            "an equality with the shared column on both sides has no executable IN-list probe"
        );
    }

    #[test]
    fn test_join_binding_retains_only_fully_qualified_single_table_expressions() {
        let qualified_is_not_null: &'static Expr = Box::leak(Box::new(Expr::IsNull {
            expr: Box::new(Expr::Column(ColumnRef::qualified("t1", "a"), Span::ZERO)),
            not: true,
            span: Span::ZERO,
        }));
        let qualified_is_not_null_term = classify_where_term(qualified_is_not_null);
        assert!(matches!(
            qualified_is_not_null_term.kind,
            WhereTermKind::Other
        ));
        assert!(
            bind_where_term_to_table(&qualified_is_not_null_term, "t1", &[]).is_some(),
            "a qualified single-table partial-index predicate must survive join binding"
        );
        assert!(
            bind_where_term_to_table(&qualified_is_not_null_term, "t2", &[]).is_none(),
            "a predicate qualified for t1 must not bind to t2"
        );

        let qualified_expression_eq: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::FunctionCall {
                name: "lower".to_owned(),
                args: fsqlite_ast::FunctionArgs::List(vec![Expr::Column(
                    ColumnRef::qualified("t1", "a"),
                    Span::ZERO,
                )]),
                distinct: false,
                order_by: vec![],
                filter: None,
                over: None,
                span: Span::ZERO,
            }),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::String("x".to_owned()), Span::ZERO)),
            span: Span::ZERO,
        }));
        let qualified_expression_term = classify_where_term(qualified_expression_eq);
        assert!(matches!(
            qualified_expression_term.kind,
            WhereTermKind::Other
        ));
        let bound_expression = bind_where_term_to_table(&qualified_expression_term, "t1", &[])
            .expect("qualified t1 expression-index term must survive binding");
        assert!(
            bound_expression.column.is_none(),
            "expression-index terms intentionally retain their raw AST instead of inventing a column"
        );
        let qualified_expression_index = IndexInfo {
            name: "idx_t1_lower_a".to_owned(),
            table: "t1".to_owned(),
            columns: vec![],
            unique: false,
            n_pages: 20,
            source: StatsSource::Heuristic,
            partial_where: None,
            expression_columns: vec![Expr::FunctionCall {
                name: "LOWER".to_owned(),
                args: fsqlite_ast::FunctionArgs::List(vec![Expr::Column(
                    ColumnRef::bare("A"),
                    Span::ZERO,
                )]),
                distinct: false,
                order_by: vec![],
                filter: None,
                over: None,
                span: Span::ZERO,
            }],
        };
        let qualified_expression_path = join_access_path(
            &table_stats("t1", 100, 1_000),
            &[qualified_expression_index],
            std::slice::from_ref(&qualified_expression_term),
            None,
            JoinAccessPathContext {
                table_index_hints: None,
                cracking_hints: None,
                available_outer_tables: &[],
                unqualified_terms_are_table_local: false,
            },
        );
        assert_eq!(
            qualified_expression_path.index.as_deref(),
            Some("idx_t1_lower_a"),
            "qualified query expressions must match bare, case-insensitive expression-index keys"
        );
        let qualified_cast = Expr::Cast {
            expr: Box::new(Expr::Column(ColumnRef::qualified("t1", "a"), Span::ZERO)),
            type_name: fsqlite_ast::TypeName {
                name: "text".to_owned(),
                arg1: None,
                arg2: None,
            },
            span: Span::ZERO,
        };
        let indexed_cast = Expr::Cast {
            expr: Box::new(Expr::Column(ColumnRef::bare("A"), Span::ZERO)),
            type_name: fsqlite_ast::TypeName {
                name: "TEXT".to_owned(),
                arg1: None,
                arg2: None,
            },
            span: Span::ZERO,
        };
        assert!(
            expression_matches_index_key(&qualified_cast, &indexed_cast, "t1"),
            "type names and columns in expression-index keys are SQL identifiers, not case-sensitive data"
        );

        let bare_expression_eq: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::FunctionCall {
                name: "lower".to_owned(),
                args: fsqlite_ast::FunctionArgs::List(vec![Expr::Column(
                    ColumnRef::bare("a"),
                    Span::ZERO,
                )]),
                distinct: false,
                order_by: vec![],
                filter: None,
                over: None,
                span: Span::ZERO,
            }),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::String("x".to_owned()), Span::ZERO)),
            span: Span::ZERO,
        }));
        assert!(
            bind_where_term_to_table(&classify_where_term(bare_expression_eq), "t1", &[]).is_none(),
            "a bare expression-index term is ambiguous in a multi-table query"
        );

        let cross_table_expression_eq: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::FunctionCall {
                name: "lower".to_owned(),
                args: fsqlite_ast::FunctionArgs::List(vec![Expr::Column(
                    ColumnRef::qualified("t1", "a"),
                    Span::ZERO,
                )]),
                distinct: false,
                order_by: vec![],
                filter: None,
                over: None,
                span: Span::ZERO,
            }),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Column(ColumnRef::qualified("t2", "a"), Span::ZERO)),
            span: Span::ZERO,
        }));
        assert!(
            bind_where_term_to_table(
                &classify_where_term(cross_table_expression_eq),
                "t1",
                &["t2".to_owned()],
            )
            .is_none(),
            "expression-index probes are not executable for cross-table operands yet and must fail closed"
        );

        let same_row_expression_eq: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::FunctionCall {
                name: "lower".to_owned(),
                args: fsqlite_ast::FunctionArgs::List(vec![Expr::Column(
                    ColumnRef::qualified("t1", "a"),
                    Span::ZERO,
                )]),
                distinct: false,
                order_by: vec![],
                filter: None,
                over: None,
                span: Span::ZERO,
            }),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Column(ColumnRef::qualified("t1", "b"), Span::ZERO)),
            span: Span::ZERO,
        }));
        assert!(
            bind_where_term_to_table(&classify_where_term(same_row_expression_eq), "t1", &[],)
                .is_none(),
            "an expression-index probe cannot depend on another column from the same unread row"
        );
    }

    #[test]
    fn test_table_local_access_paths_reject_row_dependent_probes() {
        let table = table_stats("t1", 100, 1000);
        let index = index_info("idx_t1_a", "t1", &["a"], false, 20);

        let column_comparison: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Column(ColumnRef::bare("b"), Span::ZERO)),
            span: Span::ZERO,
        }));
        let column_comparison_path = best_access_path(
            &table,
            std::slice::from_ref(&index),
            &[classify_where_term(column_comparison)],
            None,
        );
        assert!(
            matches!(column_comparison_path.kind, AccessPathKind::FullTableScan),
            "a same-row column cannot be evaluated as a pre-scan index probe: \
             {column_comparison_path:?}"
        );

        let rowid_comparison: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("rowid"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            span: Span::ZERO,
        }));
        let rowid_comparison_path =
            best_access_path(&table, &[], &[classify_where_term(rowid_comparison)], None);
        assert!(
            matches!(rowid_comparison_path.kind, AccessPathKind::FullTableScan),
            "a same-row column cannot be evaluated as a pre-scan rowid probe: \
             {rowid_comparison_path:?}"
        );

        let or_comparison: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new((*column_comparison).clone()),
            op: AstBinaryOp::Or,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
                op: AstBinaryOp::Eq,
                right: Box::new(Expr::Literal(Literal::Integer(7), Span::ZERO)),
                span: Span::ZERO,
            }),
            span: Span::ZERO,
        }));
        let or_term = classify_where_term(or_comparison);
        assert!(matches!(or_term.kind, WhereTermKind::InList { count: 2 }));
        let or_comparison_path = best_access_path(&table, &[index], &[or_term], None);
        assert!(
            matches!(or_comparison_path.kind, AccessPathKind::FullTableScan),
            "one row-dependent OR arm makes the whole IN expansion unavailable: \
             {or_comparison_path:?}"
        );

        let empty_in: &'static Expr = Box::leak(Box::new(Expr::In {
            expr: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            set: InSet::List(vec![]),
            not: false,
            span: Span::ZERO,
        }));
        let empty_in_path = best_access_path(
            &table,
            &[index_info("idx_t1_a", "t1", &["a"], false, 20)],
            &[classify_where_term(empty_in)],
            None,
        );
        assert!(
            matches!(empty_in_path.kind, AccessPathKind::FullTableScan),
            "an empty IN list has no executable seek probe and must remain a residual: \
             {empty_in_path:?}"
        );
    }

    #[test]
    fn test_table_local_probe_filter_preserves_expression_index_terms() {
        let table = table_stats("t1", 100, 1000);
        let key_expr = Expr::FunctionCall {
            name: "lower".to_owned(),
            args: fsqlite_ast::FunctionArgs::List(vec![Expr::Column(
                ColumnRef::bare("a"),
                Span::ZERO,
            )]),
            distinct: false,
            order_by: vec![],
            filter: None,
            over: None,
            span: Span::ZERO,
        };
        let where_expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(key_expr.clone()),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::String("x".to_owned()), Span::ZERO)),
            span: Span::ZERO,
        }));
        let index = IndexInfo {
            name: "idx_t1_lower_a".to_owned(),
            table: "t1".to_owned(),
            columns: vec![],
            unique: false,
            n_pages: 20,
            source: StatsSource::Heuristic,
            partial_where: None,
            expression_columns: vec![key_expr],
        };

        let path = best_access_path(&table, &[index], &[classify_where_term(where_expr)], None);
        assert_eq!(
            path.index.as_deref(),
            Some("idx_t1_lower_a"),
            "filtering row-dependent probes must retain evaluable expression-index terms"
        );
    }

    #[test]
    fn test_best_access_path_partial_index_accepts_same_bound_stricter_operator() {
        let table = table_stats("t1", 100, 1000);
        let mut partial_idx = index_info("idx_partial_a", "t1", &["a"], false, 20);
        partial_idx.partial_where = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            op: AstBinaryOp::Ge,
            right: Box::new(Expr::Literal(Literal::Integer(10), Span::ZERO)),
            span: Span::ZERO,
        });

        let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            op: AstBinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Integer(10), Span::ZERO)),
            span: Span::ZERO,
        }));
        let ap = best_access_path(&table, &[partial_idx], &[classify_where_term(expr)], None);
        assert!(matches!(
            ap.kind,
            AccessPathKind::IndexScanRange { .. } | AccessPathKind::CoveringIndexScan { .. }
        ));
    }

    #[test]
    fn test_best_access_path_partial_index_rejects_weaker_lower_bound() {
        let table = table_stats("t1", 100, 1000);
        let mut partial_idx = index_info("idx_partial_a", "t1", &["a"], false, 20);
        partial_idx.partial_where = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            op: AstBinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Integer(10), Span::ZERO)),
            span: Span::ZERO,
        });

        let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            op: AstBinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Integer(0), Span::ZERO)),
            span: Span::ZERO,
        }));
        let ap = best_access_path(&table, &[partial_idx], &[classify_where_term(expr)], None);
        assert!(matches!(ap.kind, AccessPathKind::FullTableScan));
    }

    #[test]
    fn test_best_access_path_partial_index_accepts_is_not_null_from_equality() {
        let table = table_stats("t1", 100, 1000);
        let mut partial_idx = index_info("idx_partial_a", "t1", &["a"], false, 20);
        partial_idx.partial_where = Some(Expr::IsNull {
            expr: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            not: true,
            span: Span::ZERO,
        });

        let ap = best_access_path(&table, &[partial_idx], &[eq_term_value("a", 7)], None);
        assert!(matches!(
            ap.kind,
            AccessPathKind::IndexScanEquality | AccessPathKind::CoveringIndexScan { .. }
        ));
    }

    #[test]
    fn test_best_access_path_partial_index_accepts_is_not_null_from_in_list() {
        let table = table_stats("t1", 100, 1000);
        let mut partial_idx = index_info("idx_partial_a", "t1", &["a"], false, 20);
        partial_idx.partial_where = Some(Expr::IsNull {
            expr: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            not: true,
            span: Span::ZERO,
        });

        let ap = best_access_path(&table, &[partial_idx], &[in_term("a", 3)], None);
        assert!(matches!(
            ap.kind,
            AccessPathKind::IndexScanEquality
                | AccessPathKind::IndexScanRange { .. }
                | AccessPathKind::CoveringIndexScan { .. }
        ));
    }

    #[test]
    fn test_best_access_path_partial_index_rejects_is_not_null_from_function_backed_patterns() {
        let table = table_stats("t1", 100, 1000);

        for op in [LikeOp::Like, LikeOp::Glob, LikeOp::Match, LikeOp::Regexp] {
            let mut partial_idx = index_info("idx_partial_b", "t1", &["b"], false, 20);
            partial_idx.partial_where = Some(Expr::IsNull {
                expr: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
                not: true,
                span: Span::ZERO,
            });
            let pattern_expr: &'static Expr = Box::leak(Box::new(Expr::Like {
                expr: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
                pattern: Box::new(Expr::Literal(
                    Literal::String("needle".to_owned()),
                    Span::ZERO,
                )),
                escape: None,
                op,
                not: false,
                span: Span::ZERO,
            }));
            let terms = [eq_term_value("b", 7), classify_where_term(pattern_expr)];

            let ap = best_access_path(&table, &[partial_idx], &terms, None);
            assert!(
                matches!(ap.kind, AccessPathKind::FullTableScan),
                "{op:?} can be backed by an overridden scalar function and cannot prove that its \
                 left operand is non-NULL"
            );
            assert!(ap.index.is_none());
        }
    }

    #[test]
    fn test_best_access_path_partial_index_accepts_is_not_null_from_or_disjunction() {
        let table = table_stats("t1", 100, 1000);
        let mut partial_idx = index_info("idx_partial_a", "t1", &["a"], false, 20);
        partial_idx.partial_where = Some(Expr::IsNull {
            expr: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            not: true,
            span: Span::ZERO,
        });

        let ap = best_access_path(&table, &[partial_idx], &[or_eq_term("a", &[1, 2, 3])], None);
        assert_eq!(ap.index.as_deref(), Some("idx_partial_a"));
        assert!(matches!(
            ap.kind,
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

        // Ordinary parallel unit-test runs must not rewrite the checked-in replay
        // artifact with wall-clock timing and process-global metric noise.  The
        // dedicated E2E driver sets this variable explicitly when it wants a
        // durable artifact.
        let artifact_path =
            std::env::var_os("FSQLITE_PLANNER_INDEX_E2E_ARTIFACT").map(PathBuf::from);

        let started = Instant::now();
        let mut cracking_hints = CrackingHintStore::default();
        let before_metrics = snapshot_index_selection_totals();

        let from = FromClause {
            source: TableOrSubquery::Table {
                name: QualifiedName::bare("users"),
                alias: Some("u".to_owned()),
                index_hint: Some(IndexHint::IndexedBy("idx_users_email".to_owned())),
                time_travel: None,
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
                    time_travel: None,
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
        assert!(matches!(users_path.kind, AccessPathKind::IndexScanEquality));
        assert!(matches!(
            &users_path.probe,
            Some(AccessPathProbe::Equality { column, target })
                if column == "email"
                    && **target == Expr::Literal(Literal::Integer(1), Span::ZERO)
        ));
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
        let replay_artifact_path = artifact_path.as_ref().map_or_else(
            || "$FSQLITE_PLANNER_INDEX_E2E_ARTIFACT".to_owned(),
            |path| path.display().to_string(),
        );
        let replay_command = format!(
            "RUN_ID='{}' TRACE_ID={} SCENARIO_ID='{}' SEED={} FSQLITE_PLANNER_INDEX_E2E_ARTIFACT='{}' cargo test -p fsqlite-planner planner_index_selection_e2e_replay_emits_artifact -- --exact --nocapture",
            run_id, trace_id, scenario_id, seed, replay_artifact_path,
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
        if let Some(artifact_path) = artifact_path {
            if let Some(parent) = artifact_path.parent() {
                std::fs::create_dir_all(parent)
                    .expect("bead_id={BEAD_ID} artifact directory should be writable");
            }
            std::fs::write(&artifact_path, artifact_bytes)
                .expect("bead_id={BEAD_ID} artifact write should succeed");
            assert!(
                artifact_path.exists(),
                "bead_id={BEAD_ID} e2e artifact path should exist"
            );
        }
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
                // Constrain index_pages ≤ table_pages (realistic: indices are
                // typically smaller than the table they index).
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

                // Rowid lookup (log2(tp)) is always ≤ index equality
                // (log2(ip) + log2(tp)) since log2(ip) ≥ 0.
                prop_assert!(
                    rowid_cost <= eq_cost + f64::EPSILON,
                    "rowid lookup ({rowid_cost}) should be ≤ index equality ({eq_cost}) \
                     for table_pages={table_pages}, index_pages={index_pages}"
                );

                // Index equality ≤ full scan only when index is not
                // disproportionately large: log2(ip) + log2(tp) ≤ tp.
                // For huge indices on tiny tables, full scan can be cheaper.
                if index_pages <= table_pages {
                    prop_assert!(
                        eq_cost <= full_cost + f64::EPSILON,
                        "index equality ({eq_cost}) should be ≤ full scan ({full_cost}) \
                         for table_pages={table_pages}, index_pages={index_pages}"
                    );
                }
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

    #[test]
    fn test_asymmetric_loss_quadratic_under_linear_over() {
        // Existing tests compare one under vs one over point; this pins the
        // functional shape: overestimate is a linear 1 - ratio penalty, while
        // underestimate grows quadratically in (ratio - 1).
        let loss = asymmetric_estimation_loss;
        let approx = |a: f64, b: f64| (a - b).abs() < 1e-9;

        // Overestimate (ratio < 1): exact linear 1 - ratio.
        assert!(approx(loss(100.0, 75.0), 0.25));
        assert!(approx(loss(100.0, 50.0), 0.5));
        assert!(approx(loss(100.0, 25.0), 0.75));
        assert!(approx(loss(100.0, 0.0), 1.0));
        // Linear: equal actual-decrements yield equal loss increments.
        assert!(approx(
            loss(100.0, 50.0) - loss(100.0, 75.0),
            loss(100.0, 25.0) - loss(100.0, 50.0)
        ));

        // Underestimate (ratio > 1): doubling the excess (ratio - 1) quadruples
        // the loss, independent of the penalty constant (it cancels in the ratio).
        let base = loss(100.0, 200.0); // ratio 2 -> k * 1
        assert!(base > 0.0);
        assert!(approx(loss(100.0, 300.0), 4.0 * base)); // ratio 3 -> k * 4
        assert!(approx(loss(100.0, 500.0), 16.0 * base)); // ratio 5 -> k * 16

        // Loss is monotonic in the ratio on both sides.
        assert!(
            loss(100.0, 250.0) > loss(100.0, 200.0),
            "underestimate loss grows with ratio"
        );
        assert!(
            loss(100.0, 25.0) > loss(100.0, 50.0),
            "overestimate loss grows as estimate worsens"
        );
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

        let plan = dpccp_order_joins(&tables, &indexes, &where_terms, None, None, &[], None)
            .expect("2-table exhaustive plan should exist");
        let (order, paths, cost, plans) = (
            plan.order,
            plan.access_paths,
            plan.total_cost,
            plan.plans_enumerated,
        );
        assert_eq!(order.len(), 2);
        assert_eq!(paths.len(), order.len());
        assert!(cost > 0.0);
        assert!(plans >= 2); // At least two candidate extensions.
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

        let plan = dpccp_order_joins(&tables, &indexes, &where_terms, None, None, &[], None)
            .expect("3-table exhaustive plan should exist");
        let (order, paths, cost, plans) = (
            plan.order,
            plan.access_paths,
            plan.total_cost,
            plan.plans_enumerated,
        );
        assert_eq!(order.len(), 3);
        assert_eq!(paths.len(), order.len());
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

        let order = dpccp_order_joins(
            &tables,
            &[],
            &[],
            None,
            None,
            &[("t1".to_owned(), "t2".to_owned())],
            None,
        )
        .expect("cross-join constrained exhaustive plan should exist")
        .order;

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
        // Beam search with mx_choice=12 enumerates ~92 plans (bounded by
        // truncation at each level), much more than greedy (mx_choice=1 → ~10)
        // but less than full exhaustive (5! = 120).
        assert!(
            enumerated > 10,
            "5-table beam search should enumerate well beyond greedy-width-1 bounds, got {enumerated}"
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

        let pruned = dpccp_order_joins(&tables, &[], &[], None, None, &[], None)
            .expect("5-table exhaustive plan should exist")
            .branches_pruned;

        assert!(pruned > 0, "expected branch-and-bound pruning to occur");
    }

    /// `orders.product_id = products.id` is only probeable once `products` is in
    /// the outer prefix. DPccp must therefore admit `idx_orders_product` when it
    /// orders `products` before `orders`, and must refuse the same probe when it
    /// costs `orders` with nothing joined yet.
    fn dpccp_probe_fixture() -> (Vec<TableStats>, Vec<IndexInfo>, Expr) {
        let tables = vec![
            TableStats {
                name: "products".to_owned(),
                n_pages: 180,
                n_rows: 8_000,
                source: StatsSource::Heuristic,
            },
            TableStats {
                name: "orders".to_owned(),
                n_pages: 1_800,
                n_rows: 220_000,
                source: StatsSource::Heuristic,
            },
        ];
        let indexes = vec![IndexInfo {
            name: "idx_orders_product".to_owned(),
            table: "orders".to_owned(),
            columns: vec!["product_id".to_owned()],
            unique: false,
            n_pages: 280,
            source: StatsSource::Heuristic,
            partial_where: None,
            expression_columns: vec![],
        }];
        let join_expr = Expr::BinaryOp {
            left: Box::new(Expr::Column(
                ColumnRef::qualified("orders", "product_id"),
                Span::ZERO,
            )),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Column(
                ColumnRef::qualified("products", "id"),
                Span::ZERO,
            )),
            span: Span::ZERO,
        };
        (tables, indexes, join_expr)
    }

    #[test]
    fn dpccp_admits_join_probe_only_with_referenced_table_in_outer_prefix() {
        let (tables, indexes, join_expr) = dpccp_probe_fixture();
        let where_terms = [WhereTerm {
            expr: &join_expr,
            column: Some(WhereColumn {
                table: Some("orders".to_owned()),
                column: "product_id".to_owned(),
            }),
            kind: WhereTermKind::Equality,
        }];

        // Costed with no outer tables, the probe references a table that is not
        // yet available, so it must not be admitted.
        let without_outer = join_access_path(
            &tables[1],
            &indexes,
            &where_terms,
            None,
            JoinAccessPathContext {
                table_index_hints: None,
                cracking_hints: None,
                available_outer_tables: &[],
                unqualified_terms_are_table_local: false,
            },
        );
        assert!(
            without_outer.index.is_none(),
            "no outer prefix must not admit an outer-column probe; got {:?}",
            without_outer.index
        );

        // With `products` available, the same probe becomes admissible.
        let outer = ["products".to_owned()];
        let with_outer = join_access_path(
            &tables[1],
            &indexes,
            &where_terms,
            None,
            JoinAccessPathContext {
                table_index_hints: None,
                cracking_hints: None,
                available_outer_tables: &outer,
                unqualified_terms_are_table_local: false,
            },
        );
        assert_eq!(
            with_outer.index.as_deref(),
            Some("idx_orders_product"),
            "an outer-column probe must be admitted once its table is in the prefix"
        );
        assert!(
            with_outer.estimated_cost < without_outer.estimated_cost,
            "the indexed probe must cost less than the full scan: {} vs {}",
            with_outer.estimated_cost,
            without_outer.estimated_cost
        );
    }

    #[test]
    fn dpccp_emitted_paths_correspond_to_returned_total_cost() {
        let (tables, indexes, join_expr) = dpccp_probe_fixture();
        let where_terms = [WhereTerm {
            expr: &join_expr,
            column: Some(WhereColumn {
                table: Some("orders".to_owned()),
                column: "product_id".to_owned(),
            }),
            kind: WhereTermKind::Equality,
        }];

        let plan = dpccp_order_joins(&tables, &indexes, &where_terms, None, None, &[], None)
            .expect("2-table exhaustive plan should exist");
        let (order, paths, cost) = (plan.order, plan.access_paths, plan.total_cost);

        assert_eq!(
            order.len(),
            paths.len(),
            "one emitted path per ordered table"
        );
        assert_eq!(
            order,
            vec![0, 1],
            "the smaller products table must be the outer probe source"
        );
        assert_eq!(
            paths[1].index.as_deref(),
            Some("idx_orders_product"),
            "the winning plan must emit the prefix-enabled orders probe"
        );

        // Recompute the nested-loop recurrence from the emitted paths alone. If
        // the paths were re-derived under a different outer contract than the
        // one that produced `cost`, this reconstruction diverges.
        let mut expected_cost = 0.0_f64;
        let mut cumulative_rows = 1.0_f64;
        for (position, path) in paths.iter().enumerate() {
            if position == 0 {
                expected_cost = path.estimated_cost;
                cumulative_rows = path.estimated_rows;
            } else {
                expected_cost = path.estimated_cost.mul_add(cumulative_rows, expected_cost);
                cumulative_rows *= path.estimated_rows;
            }
        }
        assert!(
            (expected_cost - cost).abs() <= f64::EPSILON * expected_cost.abs().max(1.0),
            "total_cost {cost} must be reconstructible from the emitted paths ({expected_cost})"
        );

        // The emitted path for each table must match a fresh costing against the
        // exact prefix that preceded it in the winning order.
        let mut prefix: Vec<String> = Vec::new();
        for (position, table_idx) in order.iter().enumerate() {
            let recosted = join_access_path(
                &tables[*table_idx],
                &indexes,
                &where_terms,
                None,
                JoinAccessPathContext {
                    table_index_hints: None,
                    cracking_hints: None,
                    available_outer_tables: &prefix,
                    unqualified_terms_are_table_local: false,
                },
            );
            assert_eq!(
                paths[position].index, recosted.index,
                "emitted path {position} must match its prefix-bound costing"
            );
            assert!(
                (paths[position].estimated_cost - recosted.estimated_cost).abs() <= f64::EPSILON,
                "emitted cost {} must match prefix-bound cost {}",
                paths[position].estimated_cost,
                recosted.estimated_cost
            );
            prefix.push(tables[*table_idx].name.clone());
        }
    }

    #[test]
    fn dpccp_order_and_paths_are_deterministic_across_runs() {
        let (tables, indexes, join_expr) = dpccp_probe_fixture();
        let where_terms = [WhereTerm {
            expr: &join_expr,
            column: Some(WhereColumn {
                table: Some("orders".to_owned()),
                column: "product_id".to_owned(),
            }),
            kind: WhereTermKind::Equality,
        }];

        let first = dpccp_order_joins(&tables, &indexes, &where_terms, None, None, &[], None)
            .expect("plan should exist");
        let second = dpccp_order_joins(&tables, &indexes, &where_terms, None, None, &[], None)
            .expect("plan should exist");

        assert_eq!(
            first.order, second.order,
            "join order must be deterministic"
        );
        assert!(
            (first.total_cost - second.total_cost).abs() <= f64::EPSILON,
            "total cost must be deterministic"
        );
        let first_indexes = first
            .access_paths
            .iter()
            .map(|p| p.index.clone())
            .collect::<Vec<_>>();
        let second_indexes = second
            .access_paths
            .iter()
            .map(|p| p.index.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            first_indexes, second_indexes,
            "emitted access paths must be deterministic"
        );
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
                ColumnRef::qualified("users", "id"),
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
            left: Box::new(Expr::Column(ColumnRef::bare("id"), Span::ZERO)),
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
            left: Box::new(Expr::Column(ColumnRef::bare("id"), Span::ZERO)),
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

    fn fold_literal(literal: Literal) -> Expr {
        Expr::Literal(literal, Span::ZERO)
    }

    fn fold_binary(left: Expr, op: AstBinaryOp, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
            span: Span::ZERO,
        }
    }

    fn fold_unary(op: fsqlite_ast::UnaryOp, expr: Expr) -> Expr {
        Expr::UnaryOp {
            op,
            expr: Box::new(expr),
            span: Span::ZERO,
        }
    }

    #[test]
    fn test_fold_stack_avoids_spill_allocation_until_inline_capacity() {
        let mut stack = FoldStack::<u8, 2>::new();
        assert_eq!(stack.spill.capacity(), 0);

        stack.push(1);
        stack.push(2);
        assert_eq!(
            stack.spill.capacity(),
            0,
            "the shallow fold path must stay allocation-free"
        );

        stack.push(3);
        assert!(stack.spill.capacity() > 0);
        assert_eq!(stack.pop(), Some(3));
        assert_eq!(stack.pop(), Some(2));
        assert_eq!(stack.pop(), Some(1));
        assert_eq!(stack.pop(), None);
    }

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
        let expr = Expr::Column(ColumnRef::bare("id"), Span::ZERO);
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

    #[test]
    fn test_fold_integer_overflow_promotes_to_real_like_sqlite() {
        for (expr, expected) in [
            (
                fold_binary(
                    fold_literal(Literal::Integer(i64::MAX)),
                    AstBinaryOp::Add,
                    fold_literal(Literal::Integer(1)),
                ),
                (i64::MAX as f64) + 1.0,
            ),
            (
                fold_binary(
                    fold_literal(Literal::Integer(i64::MIN)),
                    AstBinaryOp::Subtract,
                    fold_literal(Literal::Integer(1)),
                ),
                (i64::MIN as f64) - 1.0,
            ),
            (
                fold_binary(
                    fold_literal(Literal::Integer(3_037_000_500)),
                    AstBinaryOp::Multiply,
                    fold_literal(Literal::Integer(3_037_000_500)),
                ),
                3_037_000_500_f64 * 3_037_000_500_f64,
            ),
            (
                fold_binary(
                    fold_literal(Literal::Integer(i64::MIN)),
                    AstBinaryOp::Divide,
                    fold_literal(Literal::Integer(-1)),
                ),
                -(i64::MIN as f64),
            ),
            (
                fold_unary(
                    fsqlite_ast::UnaryOp::Negate,
                    fold_literal(Literal::Integer(i64::MIN)),
                ),
                -(i64::MIN as f64),
            ),
        ] {
            assert_eq!(
                try_constant_fold(&expr),
                FoldResult::Literal(Literal::Float(expected))
            );
        }

        let modulo_overflow = fold_binary(
            fold_literal(Literal::Integer(i64::MIN)),
            AstBinaryOp::Modulo,
            fold_literal(Literal::Integer(-1)),
        );
        assert_eq!(
            try_constant_fold(&modulo_overflow),
            FoldResult::Literal(Literal::Integer(0))
        );
    }

    #[test]
    fn test_fold_is_and_is_not_never_propagate_null() {
        for (left, op, right, expected) in [
            (Literal::Null, AstBinaryOp::Is, Literal::Null, Literal::True),
            (
                Literal::Null,
                AstBinaryOp::IsNot,
                Literal::Null,
                Literal::False,
            ),
            (
                Literal::Null,
                AstBinaryOp::Is,
                Literal::Integer(1),
                Literal::False,
            ),
            (
                Literal::Null,
                AstBinaryOp::IsNot,
                Literal::Integer(1),
                Literal::True,
            ),
            (
                Literal::Integer(1),
                AstBinaryOp::Is,
                Literal::Float(1.0),
                Literal::True,
            ),
            (
                Literal::String("1".to_owned()),
                AstBinaryOp::Is,
                Literal::Integer(1),
                Literal::False,
            ),
            (
                Literal::Integer(2),
                AstBinaryOp::Is,
                Literal::True,
                Literal::True,
            ),
            (
                Literal::True,
                AstBinaryOp::Is,
                Literal::Integer(2),
                Literal::False,
            ),
            (
                Literal::Integer(0),
                AstBinaryOp::Is,
                Literal::False,
                Literal::True,
            ),
            (
                Literal::Integer(i64::MAX),
                AstBinaryOp::Is,
                Literal::Float(i64::MAX as f64),
                Literal::False,
            ),
            (
                Literal::Integer(i64::MIN),
                AstBinaryOp::Is,
                Literal::Float(i64::MIN as f64),
                Literal::True,
            ),
        ] {
            let expr = fold_binary(fold_literal(left), op, fold_literal(right));
            assert_eq!(try_constant_fold(&expr), FoldResult::Literal(expected));
        }

        let ordinary_equality = fold_binary(
            fold_literal(Literal::Null),
            AstBinaryOp::Eq,
            fold_literal(Literal::Null),
        );
        assert_eq!(
            try_constant_fold(&ordinary_equality),
            FoldResult::Literal(Literal::Null)
        );
    }

    #[test]
    fn test_fold_and_or_use_sql_three_valued_logic() {
        for (left, op, right, expected) in [
            (
                Literal::Integer(0),
                AstBinaryOp::And,
                Literal::Null,
                Literal::False,
            ),
            (
                Literal::Null,
                AstBinaryOp::And,
                Literal::Integer(0),
                Literal::False,
            ),
            (
                Literal::Integer(1),
                AstBinaryOp::And,
                Literal::Null,
                Literal::Null,
            ),
            (
                Literal::Null,
                AstBinaryOp::Or,
                Literal::Integer(1),
                Literal::True,
            ),
            (
                Literal::Integer(1),
                AstBinaryOp::Or,
                Literal::Null,
                Literal::True,
            ),
            (
                Literal::Integer(0),
                AstBinaryOp::Or,
                Literal::Null,
                Literal::Null,
            ),
        ] {
            let expr = fold_binary(fold_literal(left), op, fold_literal(right));
            assert_eq!(try_constant_fold(&expr), FoldResult::Literal(expected));
        }
    }

    #[test]
    fn test_fold_height_1000_and_1001_is_stack_safe_on_one_mib_stack() {
        std::thread::Builder::new()
            .stack_size(1024 * 1024)
            .spawn(|| {
                for height in [1_000_i64, 1_001] {
                    let mut expr = fold_literal(Literal::Integer(1));
                    for _ in 1..height {
                        expr =
                            fold_binary(fold_literal(Literal::Integer(1)), AstBinaryOp::Add, expr);
                    }
                    assert_eq!(
                        try_constant_fold(&expr),
                        FoldResult::Literal(Literal::Integer(height))
                    );
                    drop(expr);
                }
            })
            .expect("one-MiB constant-fold test thread must spawn")
            .join()
            .expect("height-1000/1001 constant folding must not overflow the native stack");
    }

    #[test]
    fn test_query_planner_cache_hit_matches_uncached_join_plan() {
        let tables = vec![
            TableStats {
                name: "small".to_owned(),
                n_pages: 4,
                n_rows: 40,
                source: StatsSource::Heuristic,
            },
            TableStats {
                name: "large".to_owned(),
                n_pages: 40,
                n_rows: 4_000,
                source: StatsSource::Heuristic,
            },
        ];
        let uncached = order_joins(&tables, &[], &[], None, &[]);

        let mut planner = QueryPlanner::default();
        let sql_template = "SELECT * FROM small JOIN large ON small.id = large.small_id";

        let first = planner.order_joins_with_cache(
            sql_template,
            7,
            &tables,
            &[],
            &[],
            None,
            &[],
            None,
            None,
            PlannerFeatureFlags::default(),
        );
        let second = planner.order_joins_with_cache(
            sql_template,
            7,
            &tables,
            &[],
            &[],
            None,
            &[],
            None,
            None,
            PlannerFeatureFlags::default(),
        );

        assert_eq!(*first, uncached);
        assert_eq!(*second, uncached);
        assert!(Rc::ptr_eq(&first, &second));
        assert_eq!(planner.plan_cache_len(), 1);
    }

    #[test]
    fn test_query_planner_cache_separates_generic_and_join_entries() {
        let tables = vec![TableStats {
            name: "users".to_owned(),
            n_pages: 16,
            n_rows: 1_000,
            source: StatsSource::Heuristic,
        }];
        let sql_template = "SELECT * FROM users WHERE id = ?1";
        let schema_cookie = 31;
        let mut planner = QueryPlanner::default();

        let generic = planner.cached_plan(sql_template, schema_cookie, || {
            sample_cached_query_plan("generic-sentinel")
        });
        let join_plan = planner.order_joins_with_cache(
            sql_template,
            schema_cookie,
            &tables,
            &[],
            &[],
            None,
            &[],
            None,
            None,
            PlannerFeatureFlags::default(),
        );

        assert_eq!(generic.join_order, vec!["generic-sentinel".to_owned()]);
        assert_eq!(join_plan.join_order, vec!["users".to_owned()]);
        assert!(
            !Rc::ptr_eq(&generic, &join_plan),
            "generic cached_plan entries and join-order cache entries must not alias"
        );
        assert_eq!(planner.plan_cache_len(), 2);
    }

    #[test]
    fn test_query_planner_cache_invalidates_all_entries_on_schema_cookie_change() {
        let mut planner = QueryPlanner::default();
        let build_count = Cell::new(0);

        let plan_a = planner.cached_plan("SELECT * FROM t1", 11, || {
            build_count.set(build_count.get() + 1);
            sample_cached_query_plan("t1-v11")
        });
        let _plan_b = planner.cached_plan("SELECT * FROM t2", 11, || {
            build_count.set(build_count.get() + 1);
            sample_cached_query_plan("t2-v11")
        });

        assert_eq!(planner.plan_cache_len(), 2);

        let rebuilt_plan_a = planner.cached_plan("SELECT * FROM t1", 12, || {
            build_count.set(build_count.get() + 1);
            sample_cached_query_plan("t1-v12")
        });

        assert_eq!(build_count.get(), 3);
        assert_eq!(planner.plan_cache_len(), 1);
        assert_eq!(rebuilt_plan_a.join_order, vec!["t1-v12".to_owned()]);
        assert!(
            !Rc::ptr_eq(&plan_a, &rebuilt_plan_a),
            "schema cookie change must discard prior Rc<QueryPlan> entries"
        );
    }

    #[test]
    fn test_query_planner_cache_lru_eviction_at_capacity() {
        let mut planner = QueryPlanner::default();
        let schema_cookie = 21;

        for idx in 0..DEFAULT_PLAN_CACHE_CAPACITY {
            let sql = format!("SELECT * FROM cached_table WHERE id = ?{idx}");
            let _ = planner.cached_plan(&sql, schema_cookie, || sample_cached_query_plan(&sql));
        }

        assert_eq!(planner.plan_cache_len(), DEFAULT_PLAN_CACHE_CAPACITY);

        let hottest_sql = "SELECT * FROM cached_table WHERE id = ?0";
        let hottest_plan = planner.cached_plan(hottest_sql, schema_cookie, || {
            panic!("expected hottest cache entry to already exist")
        });
        for _ in 0..4 {
            let hottest_plan_again = planner.cached_plan(hottest_sql, schema_cookie, || {
                panic!("expected hottest entry to stay hot across repeated direct hits")
            });
            assert!(Rc::ptr_eq(&hottest_plan, &hottest_plan_again));
        }

        let cold_key = plan_cache_key("SELECT * FROM cached_table WHERE id = ?1", schema_cookie);
        let hot_key = plan_cache_key(hottest_sql, schema_cookie);

        let _ = planner.cached_plan(
            "SELECT * FROM cached_table WHERE id = ?overflow",
            schema_cookie,
            || sample_cached_query_plan("overflow"),
        );

        assert_eq!(planner.plan_cache_len(), DEFAULT_PLAN_CACHE_CAPACITY);
        assert!(
            planner.plan_cache.iter().any(|(key, _)| *key == hot_key),
            "re-accessed entry should remain resident after LRU eviction"
        );
        assert!(
            !planner.plan_cache.iter().any(|(key, _)| *key == cold_key),
            "least-recently-used entry should be evicted at capacity"
        );

        let hottest_plan_again = planner.cached_plan(hottest_sql, schema_cookie, || {
            panic!("expected hottest entry to survive eviction")
        });
        assert!(Rc::ptr_eq(&hottest_plan, &hottest_plan_again));
    }

    #[test]
    fn test_query_planner_cache_separates_feature_flag_variants() {
        let tables = [
            table_stats("a", 1024, 1_000_000),
            table_stats("b", 1024, 1_000_000),
            table_stats("c", 1024, 1_000_000),
        ];
        let terms = [join_term("a", "k", "b", "k"), join_term("b", "k", "c", "k")];
        let sql_template = "SELECT * FROM a JOIN b ON a.k = b.k JOIN c ON b.k = c.k";
        let mut planner = QueryPlanner::default();

        let hash_only = planner.order_joins_with_cache(
            sql_template,
            7,
            &tables,
            &[],
            &terms,
            None,
            &[],
            None,
            None,
            PlannerFeatureFlags::default(),
        );
        let leapfrog = planner.order_joins_with_cache(
            sql_template,
            7,
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
            hash_only
                .join_segments
                .iter()
                .all(|segment| segment.operator == JoinOperator::HashJoin),
            "disabled feature flag should keep hash-only plan: {:?}",
            hash_only.join_segments
        );
        assert!(
            leapfrog
                .join_segments
                .iter()
                .any(|segment| segment.operator == JoinOperator::LeapfrogTriejoin),
            "enabled feature flag should allow leapfrog routing: {:?}",
            leapfrog.join_segments
        );
        assert!(
            !Rc::ptr_eq(&hash_only, &leapfrog),
            "feature-flag variants must not alias the same cached Rc<QueryPlan>"
        );
        assert_eq!(planner.plan_cache_len(), 2);
    }

    #[test]
    fn test_query_planner_cache_bypasses_adaptive_cracking_hints() {
        let tables = [table_stats("t1", 256, 20_000)];
        let indexes = [
            IndexInfo {
                name: "idx_a".to_owned(),
                table: "t1".to_owned(),
                columns: vec!["a".to_owned()],
                unique: false,
                n_pages: 16,
                source: StatsSource::Heuristic,
                partial_where: None,
                expression_columns: vec![],
            },
            IndexInfo {
                name: "idx_b".to_owned(),
                table: "t1".to_owned(),
                columns: vec!["a".to_owned()],
                unique: false,
                n_pages: 12,
                source: StatsSource::Heuristic,
                partial_where: None,
                expression_columns: vec![],
            },
        ];
        let terms = [eq_term("a")];
        let sql_template = "SELECT * FROM t1 WHERE a = ?1";
        let mut planner = QueryPlanner::default();

        let mut first_hints = CrackingHintStore::default();
        first_hints.record_access_path(&AccessPath {
            table: "t1".to_owned(),
            kind: AccessPathKind::IndexScanEquality,
            index: Some("idx_a".to_owned()),
            estimated_cost: 1.0,
            estimated_rows: 1.0,
            time_travel: None,
            probe: None,
        });
        let first = planner.order_joins_with_cache(
            sql_template,
            5,
            &tables,
            &indexes,
            &terms,
            None,
            &[],
            None,
            Some(&mut first_hints),
            PlannerFeatureFlags::default(),
        );

        let mut second_hints = CrackingHintStore::default();
        second_hints.record_access_path(&AccessPath {
            table: "t1".to_owned(),
            kind: AccessPathKind::IndexScanEquality,
            index: Some("idx_b".to_owned()),
            estimated_cost: 1.0,
            estimated_rows: 1.0,
            time_travel: None,
            probe: None,
        });
        let second = planner.order_joins_with_cache(
            sql_template,
            5,
            &tables,
            &indexes,
            &terms,
            None,
            &[],
            None,
            Some(&mut second_hints),
            PlannerFeatureFlags::default(),
        );

        assert_eq!(first.access_paths[0].index.as_deref(), Some("idx_a"));
        assert_eq!(second.access_paths[0].index.as_deref(), Some("idx_b"));
        assert_eq!(planner.plan_cache_len(), 0);
        assert!(!Rc::ptr_eq(&first, &second));
    }
}
#[test]
fn test_join_order_returns_each_table_once() {
    let tables = vec![
        TableStats {
            name: "nation".to_owned(),
            n_pages: 1,
            n_rows: 25,
            source: StatsSource::Analyze,
        },
        TableStats {
            name: "region".to_owned(),
            n_pages: 1,
            n_rows: 5,
            source: StatsSource::Analyze,
        },
        TableStats {
            name: "supplier".to_owned(),
            n_pages: 100,
            n_rows: 10_000,
            source: StatsSource::Analyze,
        },
        TableStats {
            name: "customer".to_owned(),
            n_pages: 500,
            n_rows: 150_000,
            source: StatsSource::Analyze,
        },
        TableStats {
            name: "orders".to_owned(),
            n_pages: 2000,
            n_rows: 1_500_000,
            source: StatsSource::Analyze,
        },
        TableStats {
            name: "lineitem".to_owned(),
            n_pages: 8000,
            n_rows: 6_000_000,
            source: StatsSource::Analyze,
        },
    ];
    let plan = order_joins(&tables, &[], &[], None, &[]);
    assert_eq!(plan.join_order.len(), tables.len());
    let join_order: HashSet<_> = plan.join_order.iter().collect();
    assert_eq!(join_order.len(), tables.len());
    for table in &tables {
        assert!(plan.join_order.iter().any(|name| name == &table.name));
    }
}

#[cfg(test)]
mod probe_tests {
    use super::*;
    use fsqlite_ast::{BinaryOp as AstBinaryOp, ColumnRef, Expr, Literal, Span};

    fn col(name: &str) -> Box<Expr> {
        Box::new(Expr::Column(ColumnRef::bare(name), Span::ZERO))
    }

    fn lit_int(v: i64) -> Box<Expr> {
        Box::new(Expr::Literal(Literal::Integer(v), Span::ZERO))
    }

    fn eq_expr(col_name: &str, val: i64) -> Expr {
        Expr::BinaryOp {
            left: col(col_name),
            op: AstBinaryOp::Eq,
            right: lit_int(val),
            span: Span::ZERO,
        }
    }

    #[test]
    fn extract_probe_rowid_equality() {
        let expr = eq_expr("rowid", 42);
        let terms = [WhereTerm {
            expr: &expr,
            column: Some(WhereColumn {
                table: None,
                column: "rowid".to_owned(),
            }),
            kind: WhereTermKind::RowidEquality,
        }];
        let ap = AccessPath {
            table: "t".to_owned(),
            kind: AccessPathKind::RowidLookup,
            index: None,
            estimated_cost: 1.0,
            estimated_rows: 1.0,
            time_travel: None,
            probe: None,
        };
        let probe = extract_access_path_probe_with_rowid_aliases(&ap, &[], &terms, &[]);
        assert!(
            matches!(&probe, Some(AccessPathProbe::RowidEquality { target }) if **target == Expr::Literal(Literal::Integer(42), Span::ZERO))
        );
    }

    #[test]
    fn extract_probe_index_equality() {
        let expr = eq_expr("name", 7);
        let terms = [WhereTerm {
            expr: &expr,
            column: Some(WhereColumn {
                table: None,
                column: "name".to_owned(),
            }),
            kind: WhereTermKind::Equality,
        }];
        let indexes = [IndexInfo {
            name: "idx_name".to_owned(),
            table: "t".to_owned(),
            columns: vec!["name".to_owned()],
            unique: false,
            n_pages: 1,
            source: StatsSource::Heuristic,
            partial_where: None,
            expression_columns: vec![],
        }];
        let ap = AccessPath {
            table: "t".to_owned(),
            kind: AccessPathKind::IndexScanEquality,
            index: Some("idx_name".to_owned()),
            estimated_cost: 5.0,
            estimated_rows: 1.0,
            time_travel: None,
            probe: None,
        };
        let probe = extract_access_path_probe_with_rowid_aliases(&ap, &indexes, &terms, &[]);
        match &probe {
            Some(AccessPathProbe::Equality { column, target }) => {
                assert_eq!(column, "name");
                assert_eq!(**target, Expr::Literal(Literal::Integer(7), Span::ZERO));
            }
            other => panic!("expected Equality probe, got {other:?}"),
        }
    }

    #[test]
    fn extract_probe_index_range() {
        let gt_expr = Expr::BinaryOp {
            left: col("age"),
            op: AstBinaryOp::Gt,
            right: lit_int(18),
            span: Span::ZERO,
        };
        let lt_expr = Expr::BinaryOp {
            left: col("age"),
            op: AstBinaryOp::Le,
            right: lit_int(65),
            span: Span::ZERO,
        };
        let terms = [
            WhereTerm {
                expr: &gt_expr,
                column: Some(WhereColumn {
                    table: None,
                    column: "age".to_owned(),
                }),
                kind: WhereTermKind::Range,
            },
            WhereTerm {
                expr: &lt_expr,
                column: Some(WhereColumn {
                    table: None,
                    column: "age".to_owned(),
                }),
                kind: WhereTermKind::Range,
            },
        ];
        let indexes = [IndexInfo {
            name: "idx_age".to_owned(),
            table: "t".to_owned(),
            columns: vec!["age".to_owned()],
            unique: false,
            n_pages: 1,
            source: StatsSource::Heuristic,
            partial_where: None,
            expression_columns: vec![],
        }];
        let ap = AccessPath {
            table: "t".to_owned(),
            kind: AccessPathKind::IndexScanRange { selectivity: 0.5 },
            index: Some("idx_age".to_owned()),
            estimated_cost: 50.0,
            estimated_rows: 100.0,
            time_travel: None,
            probe: None,
        };
        let probe = extract_access_path_probe_with_rowid_aliases(&ap, &indexes, &terms, &[]);
        match &probe {
            Some(AccessPathProbe::Range {
                column,
                lower,
                upper,
            }) => {
                assert_eq!(column, "age");
                let (lo_expr, lo_inc) = lower.as_ref().expect("expected lower bound");
                assert_eq!(**lo_expr, Expr::Literal(Literal::Integer(18), Span::ZERO));
                assert!(!lo_inc, "GT should be exclusive");
                let (hi_expr, hi_inc) = upper.as_ref().expect("expected upper bound");
                assert_eq!(**hi_expr, Expr::Literal(Literal::Integer(65), Span::ZERO));
                assert!(hi_inc, "LE should be inclusive");
            }
            other => panic!("expected Range probe, got {other:?}"),
        }
    }

    #[test]
    fn extract_probe_in_list() {
        let in_expr = Expr::In {
            expr: col("status"),
            set: InSet::List(vec![
                Expr::Literal(Literal::Integer(1), Span::ZERO),
                Expr::Literal(Literal::Integer(2), Span::ZERO),
                Expr::Literal(Literal::Integer(3), Span::ZERO),
            ]),
            not: false,
            span: Span::ZERO,
        };
        let terms = [WhereTerm {
            expr: &in_expr,
            column: Some(WhereColumn {
                table: None,
                column: "status".to_owned(),
            }),
            kind: WhereTermKind::InList { count: 3 },
        }];
        let indexes = [IndexInfo {
            name: "idx_status".to_owned(),
            table: "t".to_owned(),
            columns: vec!["status".to_owned()],
            unique: false,
            n_pages: 1,
            source: StatsSource::Heuristic,
            partial_where: None,
            expression_columns: vec![],
        }];
        let ap = AccessPath {
            table: "t".to_owned(),
            kind: AccessPathKind::IndexScanEquality,
            index: Some("idx_status".to_owned()),
            estimated_cost: 15.0,
            estimated_rows: 30.0,
            time_travel: None,
            probe: None,
        };
        let probe = extract_access_path_probe_with_rowid_aliases(&ap, &indexes, &terms, &[]);
        match &probe {
            Some(AccessPathProbe::InList { column, values }) => {
                assert_eq!(column, "status");
                assert_eq!(values.len(), 3);
                assert_eq!(*values[0], Expr::Literal(Literal::Integer(1), Span::ZERO));
                assert_eq!(*values[2], Expr::Literal(Literal::Integer(3), Span::ZERO));
            }
            other => panic!("expected InList probe, got {other:?}"),
        }
    }

    #[test]
    fn extract_probe_in_list_prefers_equality_over_in() {
        let eq_expression = eq_expr("status", 5);
        let in_expr = Expr::In {
            expr: col("status"),
            set: InSet::List(vec![
                Expr::Literal(Literal::Integer(1), Span::ZERO),
                Expr::Literal(Literal::Integer(5), Span::ZERO),
            ]),
            not: false,
            span: Span::ZERO,
        };
        let terms = [
            WhereTerm {
                expr: &eq_expression,
                column: Some(WhereColumn {
                    table: None,
                    column: "status".to_owned(),
                }),
                kind: WhereTermKind::Equality,
            },
            WhereTerm {
                expr: &in_expr,
                column: Some(WhereColumn {
                    table: None,
                    column: "status".to_owned(),
                }),
                kind: WhereTermKind::InList { count: 2 },
            },
        ];
        let indexes = [IndexInfo {
            name: "idx_status".to_owned(),
            table: "t".to_owned(),
            columns: vec!["status".to_owned()],
            unique: false,
            n_pages: 1,
            source: StatsSource::Heuristic,
            partial_where: None,
            expression_columns: vec![],
        }];
        let ap = AccessPath {
            table: "t".to_owned(),
            kind: AccessPathKind::IndexScanEquality,
            index: Some("idx_status".to_owned()),
            estimated_cost: 5.0,
            estimated_rows: 1.0,
            time_travel: None,
            probe: None,
        };
        let probe = extract_access_path_probe_with_rowid_aliases(&ap, &indexes, &terms, &[]);
        assert!(
            matches!(&probe, Some(AccessPathProbe::Equality { .. })),
            "equality should be preferred when both equality and IN terms exist"
        );
    }

    #[test]
    fn extract_probe_like_prefix_as_range() {
        let like_expr = Expr::Like {
            expr: col("name"),
            pattern: Box::new(Expr::Literal(
                Literal::String("abc%".to_owned()),
                Span::ZERO,
            )),
            escape: None,
            not: false,
            op: fsqlite_ast::LikeOp::Like,
            span: Span::ZERO,
        };
        let terms = [WhereTerm {
            expr: &like_expr,
            column: Some(WhereColumn {
                table: None,
                column: "name".to_owned(),
            }),
            kind: WhereTermKind::LikePrefix {
                prefix: "abc".to_owned(),
                upper_bound: Some("abd".to_owned()),
            },
        }];
        let indexes = [IndexInfo {
            name: "idx_name".to_owned(),
            table: "t".to_owned(),
            columns: vec!["name".to_owned()],
            unique: false,
            n_pages: 1,
            source: StatsSource::Heuristic,
            partial_where: None,
            expression_columns: vec![],
        }];
        let ap = AccessPath {
            table: "t".to_owned(),
            kind: AccessPathKind::IndexScanRange { selectivity: 0.1 },
            index: Some("idx_name".to_owned()),
            estimated_cost: 10.0,
            estimated_rows: 100.0,
            time_travel: None,
            probe: None,
        };
        let probe = extract_access_path_probe_with_rowid_aliases(&ap, &indexes, &terms, &[]);
        match &probe {
            Some(AccessPathProbe::Range {
                column,
                lower,
                upper,
            }) => {
                assert_eq!(column, "name");
                let (lo_expr, lo_inc) = lower.as_ref().expect("expected lower bound");
                assert_eq!(
                    **lo_expr,
                    Expr::Literal(Literal::String("abc".to_owned()), Span::ZERO)
                );
                assert!(lo_inc, "LIKE prefix lower bound should be inclusive");
                let (hi_expr, hi_inc) = upper.as_ref().expect("expected upper bound");
                assert_eq!(
                    **hi_expr,
                    Expr::Literal(Literal::String("abd".to_owned()), Span::ZERO)
                );
                assert!(!hi_inc, "LIKE prefix upper bound should be exclusive");
            }
            other => panic!("expected Range probe from LikePrefix, got {other:?}"),
        }
    }

    #[test]
    fn extract_probe_full_scan_returns_none() {
        let ap = AccessPath {
            table: "t".to_owned(),
            kind: AccessPathKind::FullTableScan,
            index: None,
            estimated_cost: 1000.0,
            estimated_rows: 1000.0,
            time_travel: None,
            probe: None,
        };
        assert!(extract_access_path_probe_with_rowid_aliases(&ap, &[], &[], &[]).is_none());
    }

    #[test]
    fn extract_probe_between_as_inclusive_range() {
        let between_expr: &'static Expr = Box::leak(Box::new(Expr::Between {
            expr: Box::new(Expr::Column(ColumnRef::bare("age"), Span::ZERO)),
            low: Box::new(Expr::Literal(Literal::Integer(18), Span::ZERO)),
            high: Box::new(Expr::Literal(Literal::Integer(65), Span::ZERO)),
            not: false,
            span: Span::ZERO,
        }));
        let terms = [WhereTerm {
            expr: between_expr,
            column: Some(WhereColumn {
                table: None,
                column: "age".to_owned(),
            }),
            kind: WhereTermKind::Between,
        }];
        let indexes = [IndexInfo {
            name: "idx_age".to_owned(),
            table: "t".to_owned(),
            columns: vec!["age".to_owned()],
            unique: false,
            n_pages: 1,
            source: StatsSource::Heuristic,
            partial_where: None,
            expression_columns: vec![],
        }];
        let ap = AccessPath {
            table: "t".to_owned(),
            kind: AccessPathKind::IndexScanRange { selectivity: 0.1 },
            index: Some("idx_age".to_owned()),
            estimated_cost: 10.0,
            estimated_rows: 100.0,
            time_travel: None,
            probe: None,
        };
        let probe = extract_access_path_probe_with_rowid_aliases(&ap, &indexes, &terms, &[]);
        match &probe {
            Some(AccessPathProbe::Range {
                column,
                lower,
                upper,
            }) => {
                assert_eq!(column, "age");
                let (lo_expr, lo_inc) = lower.as_ref().expect("expected lower bound");
                assert_eq!(**lo_expr, Expr::Literal(Literal::Integer(18), Span::ZERO));
                assert!(lo_inc, "BETWEEN lower bound must be inclusive");
                let (hi_expr, hi_inc) = upper.as_ref().expect("expected upper bound");
                assert_eq!(**hi_expr, Expr::Literal(Literal::Integer(65), Span::ZERO));
                assert!(hi_inc, "BETWEEN upper bound must be inclusive");
            }
            other => panic!("expected Range probe from Between, got {other:?}"),
        }
    }
}
