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

use decision_contract::access_path_kind_label;
use fsqlite_ast::{
    BinaryOp as AstBinaryOp, ColumnRef, CompoundOp, Expr, FromClause, InSet, LikeOp, Literal,
    NullsOrder, OrderingTerm, ResultColumn, SelectBody, SelectCore, SortDirection, Span,
    TableOrSubquery,
};
use std::fmt;

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
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// Total estimated cost in page reads.
    pub total_cost: f64,
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

    match kind {
        AccessPathKind::FullTableScan => tp,
        AccessPathKind::IndexScanRange { selectivity } => {
            ip.log2() + selectivity * ip + selectivity * tp
        }
        AccessPathKind::IndexScanEquality => ip.log2() + tp.log2(),
        AccessPathKind::CoveringIndexScan { selectivity } => ip.log2() + selectivity * ip,
        AccessPathKind::RowidLookup => tp.log2(),
    }
}

/// Build the cheapest [`AccessPath`] for a table given available indexes and
/// WHERE terms. Returns the lowest-cost option.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn best_access_path(
    table: &TableStats,
    indexes: &[IndexInfo],
    where_terms: &[WhereTerm<'_>],
    needed_columns: Option<&[String]>,
) -> AccessPath {
    let mut best = AccessPath {
        table: table.name.clone(),
        kind: AccessPathKind::FullTableScan,
        index: None,
        estimated_cost: estimate_cost(&AccessPathKind::FullTableScan, table.n_pages, 0),
        estimated_rows: table.n_rows as f64,
    };

    // Check each index for usability.
    for idx in indexes {
        if !idx.table.eq_ignore_ascii_case(&table.name) {
            continue;
        }

        let usability = analyze_index_usability(idx, where_terms);

        if matches!(usability, IndexUsability::NotUsable) {
            continue;
        }

        let is_covering = needed_columns.is_some_and(|needed| {
            needed
                .iter()
                .all(|c| idx.columns.iter().any(|ic| ic.eq_ignore_ascii_case(c)))
        });

        let mut cost_multiplier: f64 = 1.0;
        let (kind, est_rows) = match usability {
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

        let cost = estimate_cost(&kind, table.n_pages, idx.n_pages) * cost_multiplier;
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
    if has_rowid_equality(where_terms) {
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

    tracing::debug!(
        table = %table.name,
        access_path = %access_path_kind_label(&best.kind),
        index = ?best.index,
        estimated_cost = best.estimated_cost,
        estimated_rows = best.estimated_rows,
        "planner.best_access_path"
    );

    best
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
    /// Index can satisfy a range constraint (rightmost usable position).
    Range { selectivity: f64 },
    /// `IN (...)` expanded to multiple equality probes.
    InExpansion { probe_count: usize },
    /// `LIKE 'prefix%'` with constant prefix.
    LikePrefix { prefix: String },
    /// The term cannot use this index.
    NotUsable,
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
    /// `col LIKE 'prefix%'`
    LikePrefix { prefix: String },
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

/// Classify a single WHERE expression into a [`WhereTerm`].
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn classify_where_term(expr: &Expr) -> WhereTerm<'_> {
    match expr {
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
        Expr::BinaryOp {
            left,
            op: AstBinaryOp::Lt | AstBinaryOp::Le | AstBinaryOp::Gt | AstBinaryOp::Ge,
            ..
        } => {
            let column = extract_where_column(left);
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

        // col LIKE 'prefix%'
        Expr::Like {
            expr: inner,
            pattern,
            op: LikeOp::Like,
            not,
            ..
        } if !not => {
            let column = extract_where_column(inner);
            let prefix = extract_like_prefix(pattern);
            if let Some(pfx) = prefix {
                WhereTerm {
                    expr,
                    column,
                    kind: WhereTermKind::LikePrefix { prefix: pfx },
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

/// Determine the usability of an index for a set of WHERE terms.
///
/// Rules from §10.5:
/// - Equality (`col = expr`): usable if col is the LEFTMOST column of the index.
/// - Range: usable as the RIGHTMOST constraint after any equality prefixes.
/// - IN: expanded to multiple equality probes (leftmost column).
/// - LIKE prefix: usable if column is leftmost and prefix is constant.
#[must_use]
pub fn analyze_index_usability(index: &IndexInfo, terms: &[WhereTerm<'_>]) -> IndexUsability {
    if index.columns.is_empty() {
        return IndexUsability::NotUsable;
    }

    let leftmost = &index.columns[0];

    // Check for equality on the leftmost column.
    for term in terms {
        if let Some(ref wc) = term.column {
            if wc.column.eq_ignore_ascii_case(leftmost) {
                match &term.kind {
                    WhereTermKind::Equality => return IndexUsability::Equality,
                    WhereTermKind::InList { count } => {
                        return IndexUsability::InExpansion {
                            probe_count: *count,
                        };
                    }
                    WhereTermKind::LikePrefix { prefix } => {
                        return IndexUsability::LikePrefix {
                            prefix: prefix.clone(),
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
            if wc.column.eq_ignore_ascii_case(leftmost)
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

/// Default selectivity for range constraints when no ANALYZE data is available.
const DEFAULT_RANGE_SELECTIVITY: f64 = 0.33;

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
#[allow(clippy::too_many_lines)]
pub fn order_joins(
    tables: &[TableStats],
    indexes: &[IndexInfo],
    where_terms: &[WhereTerm<'_>],
    needed_columns: Option<&[String]>,
    cross_join_pairs: &[(String, String)],
) -> QueryPlan {
    let n = tables.len();

    if n == 0 {
        return QueryPlan {
            join_order: vec![],
            access_paths: vec![],
            total_cost: 0.0,
        };
    }

    if n == 1 {
        let ap = best_access_path(&tables[0], indexes, where_terms, needed_columns);
        return QueryPlan {
            join_order: vec![tables[0].name.clone()],
            access_paths: vec![ap.clone()],
            total_cost: ap.estimated_cost,
        };
    }

    let is_star = detect_star_query(tables, where_terms);
    let mx_choice = compute_mx_choice(n, is_star);

    // Seed: start with each table as a single-element path.
    // Skip tables that are blocked by CROSS JOIN constraints (right side of a
    // cross-join pair cannot appear unless the left side is already visited).
    let mut paths: Vec<PartialPath> = Vec::with_capacity(n);
    for t in tables {
        if !cross_join_allowed(&[], &t.name, cross_join_pairs) {
            continue;
        }
        let ap = best_access_path(t, indexes, where_terms, needed_columns);
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

                let ap = best_access_path(t, indexes, where_terms, needed_columns);
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
            let ap = best_access_path(t, indexes, where_terms, needed_columns);
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

    let plan = QueryPlan {
        join_order: best.tables,
        access_paths: best.access_paths,
        total_cost: best.cost,
    };

    tracing::debug!(
        join_order = ?plan.join_order,
        total_cost = plan.total_cost,
        beam_width = mx_choice,
        star_query = is_star,
        table_count = n,
        "planner.order_joins.complete"
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_ast::{
        ColumnRef, CompoundOp, Distinctness, Expr, FromClause, InSet, Literal, OrderingTerm,
        QualifiedName, ResultColumn, SelectBody, SelectCore, SortDirection, Span, TableOrSubquery,
    };

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
        }
    }

    fn eq_term(col: &str) -> WhereTerm<'static> {
        // Leaked for convenience in tests — we just need the lifetime.
        let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare(col), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            span: Span::ZERO,
        }));
        classify_where_term(expr)
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
    fn test_index_usability_like_prefix() {
        let idx = index_info("idx_name", "t1", &["name"], false, 50);
        // LIKE 'Jo%' → usable (constant prefix)
        let terms = [like_term("name", "Jo%")];
        let result = analyze_index_usability(&idx, &terms);
        assert!(matches!(result, IndexUsability::LikePrefix { ref prefix } if prefix == "Jo"));

        // LIKE '%Jo%' → not usable (no constant prefix)
        let terms = [like_term("name", "%Jo%")];
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
        let cost_if_only_last = 1.0_f64   // small full scan cost
            + 10.0 * 10.0   // medium scanned 10 times
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
            total_cost: 115.0,
        };
        let display = plan.to_string();
        assert!(display.contains("QUERY PLAN"));
        assert!(display.contains("SCAN t1"));
        assert!(display.contains("USING INDEX idx_t2"));
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
            WhereTermKind::LikePrefix { ref prefix } if prefix == "abc"
        ));
        assert_eq!(term.column.as_ref().unwrap().column, "name");
    }

    #[test]
    fn test_classify_where_term_like_no_prefix_is_other() {
        let term = like_term("name", "%wildcard");
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
        };
        let terms = [eq_term("a")];
        let ap = best_access_path(&table, &[idx], &terms, None);
        assert!(matches!(ap.kind, AccessPathKind::FullTableScan));
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
                prefix: "abc".to_owned()
            },
            WhereTermKind::LikePrefix {
                prefix: "abc".to_owned()
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
}
