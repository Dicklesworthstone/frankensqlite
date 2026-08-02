//! Planner-side fail-closed compiler for Bloodstream-compatible view shapes.
//!
//! This module does not execute differentials. It classifies a `SELECT`
//! statement into a narrow, explicit contract that future runtime slices can
//! compile into commit-time differential operators without rediscovering shape
//! support ad hoc.

use std::collections::HashSet;
use std::fmt;

use fsqlite_ast::{
    BinaryOp, ColumnRef, Distinctness, Expr, FromClause, FunctionArgs, JoinConstraint, JoinKind,
    Literal, OrderingTerm, QualifiedName, ResultColumn, SelectCore, SelectStatement,
    TableOrSubquery, UnaryOp,
};
use fsqlite_types::SqliteValue;

/// Schema version for planner-authored differential view plans.
pub const DIFFERENTIAL_VIEW_PLAN_SCHEMA_VERSION: u32 = 1;

/// Planner mode for a differentially-maintainable view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DifferentialPlanMode {
    /// Pure row-set propagation with joins, filters, and projection only.
    RowSet,
    /// Aggregated output with one or more grouping keys.
    GroupedAggregate,
    /// Aggregated output over the whole stream.
    GlobalAggregate,
}

impl DifferentialPlanMode {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::RowSet => "row_set",
            Self::GroupedAggregate => "grouped_aggregate",
            Self::GlobalAggregate => "global_aggregate",
        }
    }
}

/// One source relation referenced by a differential view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DifferentialSource {
    pub schema: Option<String>,
    pub table: String,
    pub binding: String,
}

impl DifferentialSource {
    #[must_use]
    pub fn from_qualified_name(name: &QualifiedName, alias: Option<&str>) -> Self {
        let binding = alias.unwrap_or(name.name.as_str()).to_owned();
        Self {
            schema: name.schema.clone(),
            table: name.name.clone(),
            binding,
        }
    }

    #[must_use]
    pub fn display_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{schema}.{}", self.table),
            None => self.table.clone(),
        }
    }
}

/// A resolved column reference inside a differential plan.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DifferentialColumn {
    pub binding: String,
    pub column: String,
}

impl fmt::Display for DifferentialColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.binding, self.column)
    }
}

/// A literal equality filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DifferentialLiteralFilter {
    pub column: DifferentialColumn,
    pub value: SqliteValue,
}

/// An inner equi-join predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DifferentialJoinKey {
    pub left: DifferentialColumn,
    pub right: DifferentialColumn,
}

/// Aggregate outputs supported by the planner-side Bloodstream contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DifferentialAggregate {
    CountRows,
    Sum { column: DifferentialColumn },
}

impl fmt::Display for DifferentialAggregate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CountRows => write!(f, "COUNT(*)"),
            Self::Sum { column } => write!(f, "SUM({column})"),
        }
    }
}

/// One emitted output column from a differential view plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DifferentialOutput {
    Column {
        column: DifferentialColumn,
        alias: Option<String>,
    },
    Aggregate {
        aggregate: DifferentialAggregate,
        alias: Option<String>,
    },
}

impl DifferentialOutput {
    #[must_use]
    pub fn explain_label(&self) -> String {
        match self {
            Self::Column { column, alias } => {
                render_output_label(&column.to_string(), alias.as_ref())
            }
            Self::Aggregate { aggregate, alias } => {
                render_output_label(&aggregate.to_string(), alias.as_ref())
            }
        }
    }
}

/// Planner-authored differential contract for one supported view query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DifferentialViewPlan {
    pub schema_version: u32,
    pub mode: DifferentialPlanMode,
    pub sources: Vec<DifferentialSource>,
    pub join_keys: Vec<DifferentialJoinKey>,
    pub literal_filters: Vec<DifferentialLiteralFilter>,
    pub group_by: Vec<DifferentialColumn>,
    pub outputs: Vec<DifferentialOutput>,
}

impl DifferentialViewPlan {
    /// Render a deterministic explain string suitable for future
    /// `EXPLAIN DIFFERENTIAL` wiring.
    #[must_use]
    pub fn explain_text(&self) -> String {
        let mut lines = vec![format!("DIFFERENTIAL {}", self.mode.label())];
        for source in &self.sources {
            lines.push(format!(
                "SOURCE {} AS {}",
                source.display_name(),
                source.binding
            ));
        }
        for join_key in &self.join_keys {
            lines.push(format!("JOIN {} = {}", join_key.left, join_key.right));
        }
        for filter in &self.literal_filters {
            lines.push(format!(
                "FILTER {} = {}",
                filter.column,
                format_sqlite_value(&filter.value)
            ));
        }
        if !self.group_by.is_empty() {
            lines.push(format!(
                "GROUP BY {}",
                self.group_by
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        for output in &self.outputs {
            lines.push(format!("EMIT {}", output.explain_label()));
        }
        lines.join("\n")
    }
}

/// Fail-closed planner error for Bloodstream-compatible view compilation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DifferentialPlanError {
    UnsupportedWithClause,
    UnsupportedCompoundSelect,
    UnsupportedOrderBy,
    UnsupportedLimit,
    UnsupportedValuesCore,
    MissingFromClause,
    UnsupportedDistinct,
    UnsupportedHavingClause,
    UnsupportedWindowClause,
    UnsupportedGroupingWithoutAggregate,
    DuplicateRelationBinding { binding: String },
    AmbiguousUnqualifiedColumn { column: String },
    UnknownRelationBinding { binding: String },
    UnsupportedSource { detail: String },
    UnsupportedJoin { detail: String },
    UnsupportedWhere { detail: String },
    UnsupportedGroupBy { detail: String },
    UnsupportedProjection { detail: String },
    UnsupportedAggregate { detail: String },
    ProjectionNotGrouped { column: DifferentialColumn },
}

impl fmt::Display for DifferentialPlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedWithClause => {
                write!(f, "differential views do not yet support WITH clauses")
            }
            Self::UnsupportedCompoundSelect => {
                write!(
                    f,
                    "differential views do not yet support compound SELECT bodies"
                )
            }
            Self::UnsupportedOrderBy => {
                write!(f, "differential views do not yet support ORDER BY")
            }
            Self::UnsupportedLimit => write!(f, "differential views do not yet support LIMIT"),
            Self::UnsupportedValuesCore => {
                write!(f, "differential views require a table-backed SELECT core")
            }
            Self::MissingFromClause => {
                write!(f, "differential views require an explicit FROM clause")
            }
            Self::UnsupportedDistinct => {
                write!(f, "differential views do not yet support DISTINCT")
            }
            Self::UnsupportedHavingClause => {
                write!(f, "differential views do not yet support HAVING")
            }
            Self::UnsupportedWindowClause => {
                write!(f, "differential views do not yet support WINDOW clauses")
            }
            Self::UnsupportedGroupingWithoutAggregate => write!(
                f,
                "GROUP BY without a supported aggregate is not yet available for differential views"
            ),
            Self::DuplicateRelationBinding { binding } => {
                write!(f, "duplicate differential relation binding: {binding}")
            }
            Self::AmbiguousUnqualifiedColumn { column } => {
                write!(f, "ambiguous unqualified differential column: {column}")
            }
            Self::UnknownRelationBinding { binding } => {
                write!(f, "unknown differential relation binding: {binding}")
            }
            Self::UnsupportedSource { detail }
            | Self::UnsupportedJoin { detail }
            | Self::UnsupportedWhere { detail }
            | Self::UnsupportedGroupBy { detail }
            | Self::UnsupportedProjection { detail }
            | Self::UnsupportedAggregate { detail } => write!(f, "{detail}"),
            Self::ProjectionNotGrouped { column } => {
                write!(
                    f,
                    "non-aggregate projection {column} must appear in GROUP BY"
                )
            }
        }
    }
}

impl std::error::Error for DifferentialPlanError {}

/// Compile a supported `SELECT` into a planner-authored Bloodstream contract.
pub fn compile_differential_view_plan(
    select: &SelectStatement,
) -> Result<DifferentialViewPlan, DifferentialPlanError> {
    let span = tracing::info_span!(
        target: "fsqlite::differential",
        "planner_compile",
        sources = tracing::field::Empty,
        joins = tracing::field::Empty,
        filters = tracing::field::Empty,
        mode = tracing::field::Empty,
    );
    let _guard = span.enter();

    if select.with.is_some() {
        return Err(DifferentialPlanError::UnsupportedWithClause);
    }
    if !select.body.compounds.is_empty() {
        return Err(DifferentialPlanError::UnsupportedCompoundSelect);
    }
    if !select.order_by.is_empty() {
        return Err(DifferentialPlanError::UnsupportedOrderBy);
    }
    if select.limit.is_some() {
        return Err(DifferentialPlanError::UnsupportedLimit);
    }

    let SelectCore::Select {
        distinct,
        columns,
        from,
        where_clause,
        group_by,
        having,
        windows,
    } = &select.body.select
    else {
        return Err(DifferentialPlanError::UnsupportedValuesCore);
    };

    if *distinct == Distinctness::Distinct {
        return Err(DifferentialPlanError::UnsupportedDistinct);
    }
    if having.is_some() {
        return Err(DifferentialPlanError::UnsupportedHavingClause);
    }
    if !windows.is_empty() {
        return Err(DifferentialPlanError::UnsupportedWindowClause);
    }

    let from_clause = from
        .as_ref()
        .ok_or(DifferentialPlanError::MissingFromClause)?;
    let sources = collect_sources(from_clause)?;
    tracing::Span::current().record("sources", sources.len());

    let join_keys = collect_join_keys(from_clause, &sources)?;
    tracing::Span::current().record("joins", join_keys.len());

    let literal_filters = where_clause.as_deref().map_or_else(
        || Ok(Vec::new()),
        |expr| collect_literal_filters(expr, &sources),
    )?;
    tracing::Span::current().record("filters", literal_filters.len());

    let group_by_columns = collect_group_by_columns(group_by, &sources)?;
    let outputs = collect_outputs(columns, &sources)?;
    let has_aggregate = outputs
        .iter()
        .any(|output| matches!(output, DifferentialOutput::Aggregate { .. }));

    if !group_by_columns.is_empty() && !has_aggregate {
        return Err(DifferentialPlanError::UnsupportedGroupingWithoutAggregate);
    }
    if has_aggregate {
        validate_grouped_outputs(&outputs, &group_by_columns)?;
    }

    let mode = if has_aggregate {
        if group_by_columns.is_empty() {
            DifferentialPlanMode::GlobalAggregate
        } else {
            DifferentialPlanMode::GroupedAggregate
        }
    } else {
        DifferentialPlanMode::RowSet
    };
    tracing::Span::current().record("mode", mode.label());

    tracing::info!(
        target: "fsqlite::differential",
        event = "planner_contract_compiled",
        mode = mode.label(),
        sources = sources.len(),
        joins = join_keys.len(),
        filters = literal_filters.len(),
        outputs = outputs.len()
    );

    Ok(DifferentialViewPlan {
        schema_version: DIFFERENTIAL_VIEW_PLAN_SCHEMA_VERSION,
        mode,
        sources,
        join_keys,
        literal_filters,
        group_by: group_by_columns,
        outputs,
    })
}

/// Compile and render a deterministic explain string for a supported shape.
pub fn explain_differential_view_plan(
    select: &SelectStatement,
) -> Result<String, DifferentialPlanError> {
    compile_differential_view_plan(select).map(|plan| plan.explain_text())
}

fn collect_sources(
    from_clause: &FromClause,
) -> Result<Vec<DifferentialSource>, DifferentialPlanError> {
    let mut sources = Vec::with_capacity(from_clause.joins.len() + 1);
    sources.push(source_from_table_or_subquery(&from_clause.source)?);
    for join in &from_clause.joins {
        sources.push(source_from_table_or_subquery(&join.table)?);
    }

    let mut seen = HashSet::new();
    for source in &sources {
        if !seen.insert(source.binding.clone()) {
            return Err(DifferentialPlanError::DuplicateRelationBinding {
                binding: source.binding.clone(),
            });
        }
    }
    Ok(sources)
}

fn source_from_table_or_subquery(
    source: &TableOrSubquery,
) -> Result<DifferentialSource, DifferentialPlanError> {
    match source {
        TableOrSubquery::Table {
            name,
            alias,
            time_travel,
            ..
        } => {
            if time_travel.is_some() {
                return Err(DifferentialPlanError::UnsupportedSource {
                    detail: "differential views do not yet support FOR SYSTEM_TIME sources"
                        .to_owned(),
                });
            }
            Ok(DifferentialSource::from_qualified_name(
                name,
                alias.as_deref(),
            ))
        }
        TableOrSubquery::Subquery { .. } => Err(DifferentialPlanError::UnsupportedSource {
            detail: "differential views do not yet support subqueries in FROM".to_owned(),
        }),
        TableOrSubquery::TableFunction { .. } => Err(DifferentialPlanError::UnsupportedSource {
            detail: "differential views do not yet support table-valued functions".to_owned(),
        }),
        TableOrSubquery::ParenJoin(_) => Err(DifferentialPlanError::UnsupportedSource {
            detail: "differential views do not yet support parenthesized join trees".to_owned(),
        }),
    }
}

fn collect_join_keys(
    from_clause: &FromClause,
    sources: &[DifferentialSource],
) -> Result<Vec<DifferentialJoinKey>, DifferentialPlanError> {
    let mut join_keys = Vec::new();
    for join in &from_clause.joins {
        if join.join_type.natural {
            return Err(DifferentialPlanError::UnsupportedJoin {
                detail: "differential views do not yet support NATURAL JOIN".to_owned(),
            });
        }
        if join.join_type.kind != JoinKind::Inner {
            return Err(DifferentialPlanError::UnsupportedJoin {
                detail: format!(
                    "differential views only support INNER JOIN today, found {:?}",
                    join.join_type.kind
                ),
            });
        }

        let constraint =
            join.constraint
                .as_ref()
                .ok_or_else(|| DifferentialPlanError::UnsupportedJoin {
                    detail: "differential INNER JOIN requires an explicit ON predicate".to_owned(),
                })?;
        match constraint {
            JoinConstraint::On(expr) => join_keys.extend(extract_join_predicates(expr, sources)?),
            JoinConstraint::Using(_) => {
                return Err(DifferentialPlanError::UnsupportedJoin {
                    detail: "differential views do not yet support USING joins".to_owned(),
                });
            }
        }
    }
    Ok(join_keys)
}

fn extract_join_predicates(
    expr: &Expr,
    sources: &[DifferentialSource],
) -> Result<Vec<DifferentialJoinKey>, DifferentialPlanError> {
    let mut join_keys = Vec::new();
    for term in flatten_conjunction(expr) {
        let (left, right) = match term {
            Expr::BinaryOp {
                left,
                op: BinaryOp::Eq,
                right,
                ..
            } => (left.as_ref(), right.as_ref()),
            _ => {
                return Err(DifferentialPlanError::UnsupportedJoin {
                    detail: "differential JOIN ON clauses only support equality predicates joined by AND".to_owned(),
                });
            }
        };
        let left_column =
            extract_column_expr(left).ok_or_else(|| DifferentialPlanError::UnsupportedJoin {
                detail: "differential JOIN predicates must compare columns to columns".to_owned(),
            })?;
        let right_column =
            extract_column_expr(right).ok_or_else(|| DifferentialPlanError::UnsupportedJoin {
                detail: "differential JOIN predicates must compare columns to columns".to_owned(),
            })?;
        let left_resolved = resolve_column_ref(left_column, sources)?;
        let right_resolved = resolve_column_ref(right_column, sources)?;
        if left_resolved.binding == right_resolved.binding {
            return Err(DifferentialPlanError::UnsupportedJoin {
                detail: "differential JOIN predicates must compare different relations".to_owned(),
            });
        }
        join_keys.push(DifferentialJoinKey {
            left: left_resolved,
            right: right_resolved,
        });
    }
    Ok(join_keys)
}

fn collect_literal_filters(
    expr: &Expr,
    sources: &[DifferentialSource],
) -> Result<Vec<DifferentialLiteralFilter>, DifferentialPlanError> {
    let mut filters = Vec::new();
    for term in flatten_conjunction(expr) {
        let Some((column_expr, value)) = match_literal_equality(term) else {
            return Err(DifferentialPlanError::UnsupportedWhere {
                detail: "differential WHERE clauses only support column = literal predicates joined by AND".to_owned(),
            });
        };
        let column = resolve_column_ref(column_expr, sources)?;
        filters.push(DifferentialLiteralFilter { column, value });
    }
    Ok(filters)
}

fn collect_group_by_columns(
    group_by: &[Expr],
    sources: &[DifferentialSource],
) -> Result<Vec<DifferentialColumn>, DifferentialPlanError> {
    group_by
        .iter()
        .map(|expr| match expr {
            Expr::Column(column_ref, _) => resolve_column_ref(column_ref, sources),
            _ => Err(DifferentialPlanError::UnsupportedGroupBy {
                detail: "differential GROUP BY currently supports only column references"
                    .to_owned(),
            }),
        })
        .collect()
}

fn collect_outputs(
    columns: &[ResultColumn],
    sources: &[DifferentialSource],
) -> Result<Vec<DifferentialOutput>, DifferentialPlanError> {
    columns
        .iter()
        .map(|column| match column {
            ResultColumn::Star | ResultColumn::TableStar(_) => {
                Err(DifferentialPlanError::UnsupportedProjection {
                    detail: "differential views do not yet support star projections".to_owned(),
                })
            }
            ResultColumn::Expr { expr, alias } => compile_output_expr(expr, alias.clone(), sources),
        })
        .collect()
}

fn compile_output_expr(
    expr: &Expr,
    alias: Option<String>,
    sources: &[DifferentialSource],
) -> Result<DifferentialOutput, DifferentialPlanError> {
    match expr {
        Expr::Column(column_ref, _) => Ok(DifferentialOutput::Column {
            column: resolve_column_ref(column_ref, sources)?,
            alias,
        }),
        Expr::FunctionCall {
            name,
            args,
            distinct,
            order_by,
            filter,
            over,
            ..
        } => compile_aggregate_output(
            name,
            args,
            *distinct,
            order_by,
            filter.as_deref(),
            over.is_some(),
            alias,
            sources,
        ),
        _ => Err(DifferentialPlanError::UnsupportedProjection {
            detail: "differential projections currently support only bare columns, COUNT(*), and SUM(column)".to_owned(),
        }),
    }
}

#[allow(clippy::too_many_arguments)]
fn compile_aggregate_output(
    name: &str,
    args: &FunctionArgs,
    distinct: bool,
    order_by: &[OrderingTerm],
    filter: Option<&Expr>,
    has_window: bool,
    alias: Option<String>,
    sources: &[DifferentialSource],
) -> Result<DifferentialOutput, DifferentialPlanError> {
    if distinct {
        return Err(DifferentialPlanError::UnsupportedAggregate {
            detail: format!("differential aggregate {name} does not support DISTINCT"),
        });
    }
    if !order_by.is_empty() {
        return Err(DifferentialPlanError::UnsupportedAggregate {
            detail: format!("differential aggregate {name} does not support ORDER BY"),
        });
    }
    if filter.is_some() {
        return Err(DifferentialPlanError::UnsupportedAggregate {
            detail: format!("differential aggregate {name} does not support FILTER"),
        });
    }
    if has_window {
        return Err(DifferentialPlanError::UnsupportedAggregate {
            detail: format!("differential aggregate {name} does not support OVER"),
        });
    }

    if name.eq_ignore_ascii_case("count") {
        if *args != FunctionArgs::Star {
            return Err(DifferentialPlanError::UnsupportedAggregate {
                detail: "differential COUNT currently supports only COUNT(*)".to_owned(),
            });
        }
        return Ok(DifferentialOutput::Aggregate {
            aggregate: DifferentialAggregate::CountRows,
            alias,
        });
    }
    if name.eq_ignore_ascii_case("sum") {
        let FunctionArgs::List(arguments) = args else {
            return Err(DifferentialPlanError::UnsupportedAggregate {
                detail: "differential SUM requires exactly one column argument".to_owned(),
            });
        };
        let [argument] = arguments.as_slice() else {
            return Err(DifferentialPlanError::UnsupportedAggregate {
                detail: "differential SUM requires exactly one column argument".to_owned(),
            });
        };
        let column_ref = extract_column_expr(argument).ok_or_else(|| {
            DifferentialPlanError::UnsupportedAggregate {
                detail: "differential SUM currently supports only bare column arguments".to_owned(),
            }
        })?;
        return Ok(DifferentialOutput::Aggregate {
            aggregate: DifferentialAggregate::Sum {
                column: resolve_column_ref(column_ref, sources)?,
            },
            alias,
        });
    }

    Err(DifferentialPlanError::UnsupportedAggregate {
        detail: format!(
            "differential aggregates currently support only COUNT(*) and SUM(column), found {name}"
        ),
    })
}

fn validate_grouped_outputs(
    outputs: &[DifferentialOutput],
    group_by: &[DifferentialColumn],
) -> Result<(), DifferentialPlanError> {
    for output in outputs {
        if let DifferentialOutput::Column { column, .. } = output
            && !group_by.iter().any(|group| group == column)
        {
            return Err(DifferentialPlanError::ProjectionNotGrouped {
                column: column.clone(),
            });
        }
    }
    Ok(())
}

fn flatten_conjunction(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
            ..
        } => {
            let mut out = flatten_conjunction(left);
            out.extend(flatten_conjunction(right));
            out
        }
        _ => vec![expr],
    }
}

fn match_literal_equality(expr: &Expr) -> Option<(&ColumnRef, SqliteValue)> {
    let Expr::BinaryOp {
        left,
        op: BinaryOp::Eq,
        right,
        ..
    } = expr
    else {
        return None;
    };

    if let (Some(column), Some(value)) = (extract_column_expr(left), extract_literal_value(right)) {
        return Some((column, value));
    }
    if let (Some(value), Some(column)) = (extract_literal_value(left), extract_column_expr(right)) {
        return Some((column, value));
    }
    None
}

fn extract_column_expr(expr: &Expr) -> Option<&ColumnRef> {
    match expr {
        Expr::Column(column_ref, _) => Some(column_ref),
        _ => None,
    }
}

/// Extract a literal value from an expression, including signed numeric
/// literals that the parser represents as `UnaryOp(Negate|Plus, Literal)`.
/// Source-level NULL and time-function constants (CURRENT_TIME/DATE/TIMESTAMP)
/// are intentionally rejected. An internal bound outer value is already a
/// concrete runtime value, so it is returned verbatim, including bound NULL.
fn extract_literal_value(expr: &Expr) -> Option<SqliteValue> {
    match expr {
        Expr::Literal(literal, _) => literal_to_sqlite_value(literal),
        Expr::BoundOuterValue { value, .. } => Some(value.clone()),
        Expr::UnaryOp {
            op: UnaryOp::Plus,
            expr: operand,
            ..
        } => extract_literal_value(operand),
        Expr::UnaryOp {
            op: UnaryOp::Negate,
            expr: operand,
            ..
        } => match extract_literal_value(operand)? {
            SqliteValue::Integer(value) => Some(SqliteValue::Integer(value.wrapping_neg())),
            SqliteValue::Float(value) => Some(SqliteValue::Float(-value)),
            // Negating non-numeric literals is not meaningful.
            _ => None,
        },
        _ => None,
    }
}

fn resolve_column_ref(
    column_ref: &ColumnRef,
    sources: &[DifferentialSource],
) -> Result<DifferentialColumn, DifferentialPlanError> {
    let binding = if let Some(binding) = &column_ref.table {
        if !sources
            .iter()
            .any(|source| source.binding == binding.as_ref())
        {
            return Err(DifferentialPlanError::UnknownRelationBinding {
                binding: binding.to_string(),
            });
        }
        binding.to_string()
    } else if sources.len() == 1 {
        sources[0].binding.clone()
    } else {
        return Err(DifferentialPlanError::AmbiguousUnqualifiedColumn {
            column: column_ref.column.to_string(),
        });
    };

    Ok(DifferentialColumn {
        binding,
        column: column_ref.column.to_string(),
    })
}

fn literal_to_sqlite_value(literal: &Literal) -> Option<SqliteValue> {
    match literal {
        Literal::Integer(value) => Some(SqliteValue::Integer(*value)),
        Literal::Float(value) => Some(SqliteValue::Float(*value)),
        Literal::String(value) => Some(SqliteValue::Text(value.clone().into())),
        Literal::Blob(value) => Some(SqliteValue::Blob(value.clone().into())),
        Literal::True => Some(SqliteValue::Integer(1)),
        Literal::False => Some(SqliteValue::Integer(0)),
        Literal::Null | Literal::CurrentTime | Literal::CurrentDate | Literal::CurrentTimestamp => {
            None
        }
    }
}

fn render_output_label(expr: &str, alias: Option<&String>) -> String {
    match alias {
        Some(alias) => format!("{expr} AS {alias}"),
        None => expr.to_owned(),
    }
}

fn format_sqlite_value(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(value) => value.to_string(),
        SqliteValue::Float(value) => value.to_string(),
        SqliteValue::Text(value) => format!("'{}'", value.replace('\'', "''")),
        SqliteValue::Blob(value) => {
            let hex = value
                .iter()
                .map(|byte| format!("{byte:02X}"))
                .collect::<String>();
            format!("X'{hex}'")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    use fsqlite_ast::{BoundCollation, Statement};
    use fsqlite_parser::Parser;
    use proptest::prelude::*;

    fn parse_select(sql: &str) -> SelectStatement {
        let (statements, errors) = Parser::from_sql(sql).parse_all();
        assert!(errors.is_empty(), "unexpected parse errors: {errors:?}");
        match statements.as_slice() {
            [Statement::Select(select)] => select.clone(),
            other => panic!("expected single SELECT statement, got {other:?}"),
        }
    }

    #[derive(Debug, Clone)]
    enum TestLiteral {
        Integer(i64),
        Text(String),
        Blob(Vec<u8>),
        True,
        False,
    }

    impl TestLiteral {
        fn to_sql(&self) -> String {
            match self {
                Self::Integer(value) => value.to_string(),
                Self::Text(value) => format!("'{value}'"),
                Self::Blob(value) => {
                    let hex = value
                        .iter()
                        .map(|byte| format!("{byte:02X}"))
                        .collect::<String>();
                    format!("X'{hex}'")
                }
                Self::True => "TRUE".to_owned(),
                Self::False => "FALSE".to_owned(),
            }
        }

        fn to_sqlite_value(&self) -> SqliteValue {
            match self {
                Self::Integer(value) => SqliteValue::Integer(*value),
                Self::Text(value) => SqliteValue::Text(value.clone().into()),
                Self::Blob(value) => SqliteValue::Blob(value.clone().into()),
                Self::True => SqliteValue::Integer(1),
                Self::False => SqliteValue::Integer(0),
            }
        }
    }

    #[derive(Debug, Clone)]
    struct RowSetCase {
        table: String,
        use_alias: bool,
        qualify_outputs: bool,
        qualify_filters: bool,
        output_columns: Vec<String>,
        filters: Vec<(String, TestLiteral)>,
    }

    impl RowSetCase {
        fn binding(&self) -> &str {
            if self.use_alias {
                "src"
            } else {
                self.table.as_str()
            }
        }

        fn render_column(&self, column: &str, qualified: bool) -> String {
            if qualified {
                format!("{}.{}", self.binding(), column)
            } else {
                column.to_owned()
            }
        }

        fn sql(&self) -> String {
            let outputs = self
                .output_columns
                .iter()
                .map(|column| self.render_column(column, self.qualify_outputs))
                .collect::<Vec<_>>()
                .join(", ");
            let mut sql = format!("SELECT {outputs} FROM {}", self.table);
            if self.use_alias {
                sql.push_str(" AS src");
            }
            if !self.filters.is_empty() {
                let where_clause = self
                    .filters
                    .iter()
                    .map(|(column, value)| {
                        format!(
                            "{} = {}",
                            self.render_column(column, self.qualify_filters),
                            value.to_sql()
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" AND ");
                sql.push_str(" WHERE ");
                sql.push_str(&where_clause);
            }
            sql
        }

        fn expected_plan(&self) -> DifferentialViewPlan {
            DifferentialViewPlan {
                schema_version: DIFFERENTIAL_VIEW_PLAN_SCHEMA_VERSION,
                mode: DifferentialPlanMode::RowSet,
                sources: vec![DifferentialSource {
                    schema: None,
                    table: self.table.clone(),
                    binding: self.binding().to_owned(),
                }],
                join_keys: Vec::new(),
                literal_filters: self
                    .filters
                    .iter()
                    .map(|(column, value)| DifferentialLiteralFilter {
                        column: DifferentialColumn {
                            binding: self.binding().to_owned(),
                            column: column.clone(),
                        },
                        value: value.to_sqlite_value(),
                    })
                    .collect(),
                group_by: Vec::new(),
                outputs: self
                    .output_columns
                    .iter()
                    .map(|column| DifferentialOutput::Column {
                        column: DifferentialColumn {
                            binding: self.binding().to_owned(),
                            column: column.clone(),
                        },
                        alias: None,
                    })
                    .collect(),
            }
        }
    }

    #[derive(Debug, Clone)]
    struct AggregateJoinCase {
        left_table: String,
        right_table: String,
        join_keys: Vec<(String, String)>,
        group_by: Vec<String>,
        filter: Option<(String, TestLiteral)>,
        sum_column: String,
    }

    impl AggregateJoinCase {
        fn sql(&self) -> String {
            let mut outputs = self
                .group_by
                .iter()
                .enumerate()
                .map(|(index, column)| format!("l.{column} AS g{index}"))
                .collect::<Vec<_>>();
            outputs.push("COUNT(*) AS n_rows".to_owned());
            outputs.push(format!("SUM(r.{}) AS total_sum", self.sum_column));

            let join_clause = self
                .join_keys
                .iter()
                .map(|(left, right)| format!("l.{left} = r.{right}"))
                .collect::<Vec<_>>()
                .join(" AND ");
            let group_by_clause = self
                .group_by
                .iter()
                .map(|column| format!("l.{column}"))
                .collect::<Vec<_>>()
                .join(", ");

            let mut sql = format!(
                "SELECT {} FROM {} AS l INNER JOIN {} AS r ON {}",
                outputs.join(", "),
                self.left_table,
                self.right_table,
                join_clause
            );
            if let Some((column, literal)) = &self.filter {
                sql.push_str(" WHERE ");
                sql.push_str(&format!("r.{column} = {}", literal.to_sql()));
            }
            sql.push_str(" GROUP BY ");
            sql.push_str(&group_by_clause);
            sql
        }

        fn expected_plan(&self) -> DifferentialViewPlan {
            let mut outputs = self
                .group_by
                .iter()
                .enumerate()
                .map(|(index, column)| DifferentialOutput::Column {
                    column: DifferentialColumn {
                        binding: "l".to_owned(),
                        column: column.clone(),
                    },
                    alias: Some(format!("g{index}")),
                })
                .collect::<Vec<_>>();
            outputs.push(DifferentialOutput::Aggregate {
                aggregate: DifferentialAggregate::CountRows,
                alias: Some("n_rows".to_owned()),
            });
            outputs.push(DifferentialOutput::Aggregate {
                aggregate: DifferentialAggregate::Sum {
                    column: DifferentialColumn {
                        binding: "r".to_owned(),
                        column: self.sum_column.clone(),
                    },
                },
                alias: Some("total_sum".to_owned()),
            });

            DifferentialViewPlan {
                schema_version: DIFFERENTIAL_VIEW_PLAN_SCHEMA_VERSION,
                mode: DifferentialPlanMode::GroupedAggregate,
                sources: vec![
                    DifferentialSource {
                        schema: None,
                        table: self.left_table.clone(),
                        binding: "l".to_owned(),
                    },
                    DifferentialSource {
                        schema: None,
                        table: self.right_table.clone(),
                        binding: "r".to_owned(),
                    },
                ],
                join_keys: self
                    .join_keys
                    .iter()
                    .map(|(left, right)| DifferentialJoinKey {
                        left: DifferentialColumn {
                            binding: "l".to_owned(),
                            column: left.clone(),
                        },
                        right: DifferentialColumn {
                            binding: "r".to_owned(),
                            column: right.clone(),
                        },
                    })
                    .collect(),
                literal_filters: self
                    .filter
                    .iter()
                    .map(|(column, literal)| DifferentialLiteralFilter {
                        column: DifferentialColumn {
                            binding: "r".to_owned(),
                            column: column.clone(),
                        },
                        value: literal.to_sqlite_value(),
                    })
                    .collect(),
                group_by: self
                    .group_by
                    .iter()
                    .map(|column| DifferentialColumn {
                        binding: "l".to_owned(),
                        column: column.clone(),
                    })
                    .collect(),
                outputs,
            }
        }
    }

    #[derive(Debug, Clone)]
    enum UnsupportedShapeCase {
        Distinct {
            table: String,
            column: String,
        },
        OrderBy {
            table: String,
            column: String,
        },
        Limit {
            table: String,
            column: String,
        },
        Star {
            table: String,
        },
        GroupWithoutAggregate {
            table: String,
            column: String,
        },
        LeftJoin {
            left_table: String,
            right_table: String,
            column: String,
        },
        UsingJoin {
            left_table: String,
            right_table: String,
            column: String,
        },
        CountArgument {
            table: String,
            column: String,
        },
        SumLiteral {
            table: String,
        },
    }

    impl UnsupportedShapeCase {
        fn sql(&self) -> String {
            match self {
                Self::Distinct { table, column } => {
                    format!("SELECT DISTINCT {column} FROM {table}")
                }
                Self::OrderBy { table, column } => {
                    format!("SELECT {column} FROM {table} ORDER BY {column}")
                }
                Self::Limit { table, column } => {
                    format!("SELECT {column} FROM {table} LIMIT 1")
                }
                Self::Star { table } => format!("SELECT * FROM {table}"),
                Self::GroupWithoutAggregate { table, column } => {
                    format!("SELECT {column} FROM {table} GROUP BY {column}")
                }
                Self::LeftJoin {
                    left_table,
                    right_table,
                    column,
                } => format!(
                    "SELECT l.{column} FROM {left_table} AS l \
                     LEFT JOIN {right_table} AS r ON l.{column} = r.{column}"
                ),
                Self::UsingJoin {
                    left_table,
                    right_table,
                    column,
                } => format!(
                    "SELECT l.{column} FROM {left_table} AS l \
                     INNER JOIN {right_table} AS r USING ({column})"
                ),
                Self::CountArgument { table, column } => {
                    format!("SELECT COUNT({column}) FROM {table}")
                }
                Self::SumLiteral { table } => format!("SELECT SUM(1) FROM {table}"),
            }
        }

        fn assert_expected_error(&self, error: DifferentialPlanError) {
            match self {
                Self::Distinct { .. } => {
                    assert_eq!(error, DifferentialPlanError::UnsupportedDistinct);
                }
                Self::OrderBy { .. } => {
                    assert_eq!(error, DifferentialPlanError::UnsupportedOrderBy);
                }
                Self::Limit { .. } => {
                    assert_eq!(error, DifferentialPlanError::UnsupportedLimit);
                }
                Self::Star { .. } => {
                    assert!(matches!(
                        error,
                        DifferentialPlanError::UnsupportedProjection { ref detail }
                            if detail.contains("star projections")
                    ));
                }
                Self::GroupWithoutAggregate { .. } => {
                    assert_eq!(
                        error,
                        DifferentialPlanError::UnsupportedGroupingWithoutAggregate
                    );
                }
                Self::LeftJoin { .. } => {
                    assert!(matches!(
                        error,
                        DifferentialPlanError::UnsupportedJoin { ref detail }
                            if detail.contains("INNER JOIN")
                    ));
                }
                Self::UsingJoin { .. } => {
                    assert!(matches!(
                        error,
                        DifferentialPlanError::UnsupportedJoin { ref detail }
                            if detail.contains("USING joins")
                    ));
                }
                Self::CountArgument { .. } => {
                    assert!(matches!(
                        error,
                        DifferentialPlanError::UnsupportedAggregate { ref detail }
                            if detail.contains("COUNT(*)")
                    ));
                }
                Self::SumLiteral { .. } => {
                    assert!(matches!(
                        error,
                        DifferentialPlanError::UnsupportedAggregate { ref detail }
                            if detail.contains("bare column arguments")
                    ));
                }
            }
        }
    }

    fn arb_identifier() -> BoxedStrategy<String> {
        // Identifiers are drawn from the lowercase ASCII alphanumeric
        // alphabet and then filtered against the SQL keyword set.  The
        // regex width (up to 5 chars) happily lands on tokens like
        // `in`, `to`, `or`, `not`, `is`, `null`, `into`, `case` etc., so
        // every such keyword must be excluded or the parser will (correctly)
        // reject the generated SQL and the test will fail through no fault
        // of the compiler under test.
        prop::string::string_regex("[a-z][a-z0-9]{0,4}")
            .expect("valid identifier regex")
            .prop_filter("identifiers must avoid SQL keywords", |identifier| {
                // Every SQL keyword recognised by the fsqlite tokenizer
                // that is NOT in `is_nonreserved_kw` (i.e. truly reserved)
                // must appear here, plus any nonreserved keyword that the
                // differential generators use in a clause-starting position
                // where the parser would confuse it with a keyword.
                //
                // Nonreserved keywords (asc, desc, do, each, fail, first,
                // if, key, last, no, over, plan, query, range, row, rows,
                // table, temp, view, etc.) are safe as identifiers and do
                // NOT need to be listed here.
                !matches!(
                    identifier.as_str(),
                    // ── truly reserved (not in is_nonreserved_kw) ──
                    "add"
                        | "all"
                        | "alter"
                        | "and"
                        | "as"
                        | "begin"
                        | "between"
                        | "by"
                        | "case"
                        | "cast"
                        | "check"
                        | "cross"
                        | "drop"
                        | "else"
                        | "except"
                        | "exists"
                        | "false"
                        | "for"
                        | "from"
                        | "glob"
                        | "group"
                        | "having"
                        | "in"
                        | "inner"
                        | "into"
                        | "is"
                        | "isnull"
                        | "join"
                        | "left"
                        | "like"
                        | "limit"
                        | "not"
                        | "notnull"
                        | "null"
                        | "on"
                        | "or"
                        | "order"
                        | "outer"
                        | "raise"
                        | "right"
                        | "select"
                        | "set"
                        | "then"
                        | "to"
                        | "true"
                        | "union"
                        | "using"
                        | "values"
                        | "when"
                        | "where"
                        | "with"
                        // ── nonreserved but used in generated SQL ──
                        // These are safe as identifiers generally, but
                        // confuse the parser when they appear as table/column
                        // names in the specific SQL shapes the generators
                        // produce (e.g. `count` as a column name in a
                        // `COUNT(*)` projection, or `distinct` after SELECT).
                        | "count"
                        | "sum"
                        | "distinct"
                        | "end"
                        | "full"
                        | "match"
                        | "natural"
                        | "offset"
                        | "regexp"
                        | "intersect"
                )
            })
            .boxed()
    }

    fn arb_text_literal() -> BoxedStrategy<String> {
        prop::string::string_regex("[a-z][a-z0-9]{0,5}")
            .expect("valid text literal regex")
            .boxed()
    }

    fn arb_test_literal() -> BoxedStrategy<TestLiteral> {
        prop_oneof![
            (-999i64..=999).prop_map(TestLiteral::Integer),
            arb_text_literal().prop_map(TestLiteral::Text),
            proptest::collection::vec(any::<u8>(), 1..4).prop_map(TestLiteral::Blob),
            Just(TestLiteral::True),
            Just(TestLiteral::False),
        ]
        .boxed()
    }

    fn all_unique(values: &[String]) -> bool {
        let mut seen = HashSet::new();
        values.iter().all(|value| seen.insert(value.as_str()))
    }

    fn unique_column_list(range: std::ops::Range<usize>) -> BoxedStrategy<Vec<String>> {
        proptest::collection::vec(arb_identifier(), range)
            .prop_filter("column names must be unique", |columns| all_unique(columns))
            .boxed()
    }

    fn arb_rowset_case() -> BoxedStrategy<RowSetCase> {
        (
            arb_identifier(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            unique_column_list(1..4),
            proptest::collection::vec((arb_identifier(), arb_test_literal()), 0..4).prop_filter(
                "filter columns must be unique",
                |filters| {
                    let columns = filters
                        .iter()
                        .map(|(column, _)| column.clone())
                        .collect::<Vec<_>>();
                    all_unique(&columns)
                },
            ),
        )
            .prop_map(
                |(table, use_alias, qualify_outputs, qualify_filters, output_columns, filters)| {
                    RowSetCase {
                        table,
                        use_alias,
                        qualify_outputs,
                        qualify_filters,
                        output_columns,
                        filters,
                    }
                },
            )
            .boxed()
    }

    fn arb_grouped_aggregate_case() -> BoxedStrategy<AggregateJoinCase> {
        (
            arb_identifier(),
            arb_identifier(),
            proptest::collection::vec((arb_identifier(), arb_identifier()), 1..4),
            unique_column_list(1..4),
            prop::option::of((arb_identifier(), arb_test_literal())),
            arb_identifier(),
        )
            .prop_map(
                |(left_table, right_table, join_keys, group_by, filter, sum_column)| {
                    AggregateJoinCase {
                        left_table,
                        right_table,
                        join_keys,
                        group_by,
                        filter,
                        sum_column,
                    }
                },
            )
            .boxed()
    }

    fn arb_unsupported_shape_case() -> BoxedStrategy<UnsupportedShapeCase> {
        prop_oneof![
            (arb_identifier(), arb_identifier())
                .prop_map(|(table, column)| { UnsupportedShapeCase::Distinct { table, column } }),
            (arb_identifier(), arb_identifier())
                .prop_map(|(table, column)| { UnsupportedShapeCase::OrderBy { table, column } }),
            (arb_identifier(), arb_identifier())
                .prop_map(|(table, column)| { UnsupportedShapeCase::Limit { table, column } }),
            arb_identifier().prop_map(|table| UnsupportedShapeCase::Star { table }),
            (arb_identifier(), arb_identifier()).prop_map(|(table, column)| {
                UnsupportedShapeCase::GroupWithoutAggregate { table, column }
            }),
            (arb_identifier(), arb_identifier(), arb_identifier()).prop_map(
                |(left_table, right_table, column)| UnsupportedShapeCase::LeftJoin {
                    left_table,
                    right_table,
                    column,
                }
            ),
            (arb_identifier(), arb_identifier(), arb_identifier()).prop_map(
                |(left_table, right_table, column)| UnsupportedShapeCase::UsingJoin {
                    left_table,
                    right_table,
                    column,
                }
            ),
            (arb_identifier(), arb_identifier()).prop_map(|(table, column)| {
                UnsupportedShapeCase::CountArgument { table, column }
            }),
            arb_identifier().prop_map(|table| UnsupportedShapeCase::SumLiteral { table }),
        ]
        .boxed()
    }

    #[test]
    fn differential_plan_compiles_single_table_rowset_shape() {
        let select = parse_select(
            "SELECT id, name \
             FROM users \
             WHERE status = 'paid' AND tenant_id = 7",
        );

        let plan = compile_differential_view_plan(&select).expect("shape should compile");
        assert_eq!(plan.mode, DifferentialPlanMode::RowSet);
        assert_eq!(plan.sources.len(), 1);
        assert_eq!(plan.sources[0].binding, "users");
        assert_eq!(plan.literal_filters.len(), 2);
        assert_eq!(
            plan.outputs,
            vec![
                DifferentialOutput::Column {
                    column: DifferentialColumn {
                        binding: "users".to_owned(),
                        column: "id".to_owned(),
                    },
                    alias: None,
                },
                DifferentialOutput::Column {
                    column: DifferentialColumn {
                        binding: "users".to_owned(),
                        column: "name".to_owned(),
                    },
                    alias: None,
                },
            ]
        );
    }

    #[test]
    fn bound_outer_values_are_lossless_differential_literals() {
        let values = [
            SqliteValue::Null,
            SqliteValue::Integer(i64::MIN),
            SqliteValue::Float(-0.0),
            SqliteValue::Text("outer text".into()),
            SqliteValue::Blob(vec![0, 1, 2, 255].into()),
        ];

        for value in values {
            let expr = Expr::BoundOuterValue {
                value: value.clone(),
                collation: BoundCollation::Named("NOCASE".to_owned()),
                affinity: Some(fsqlite_types::TypeAffinity::Text),
                span: fsqlite_ast::Span::ZERO,
            };
            assert_eq!(extract_literal_value(&expr), Some(value));
        }
    }

    #[test]
    fn differential_plan_compiles_grouped_aggregate_join_shape() {
        let select = parse_select(
            "SELECT u.id AS user_id, COUNT(*) AS n_orders, SUM(o.total) AS gross_total \
             FROM users AS u \
             INNER JOIN orders AS o ON u.id = o.user_id AND u.tenant_id = o.tenant_id \
             WHERE o.status = 'paid' \
             GROUP BY u.id",
        );

        let plan = compile_differential_view_plan(&select).expect("shape should compile");
        assert_eq!(plan.mode, DifferentialPlanMode::GroupedAggregate);
        assert_eq!(plan.sources.len(), 2);
        assert_eq!(plan.join_keys.len(), 2);
        assert_eq!(
            plan.group_by,
            vec![DifferentialColumn {
                binding: "u".to_owned(),
                column: "id".to_owned(),
            }]
        );
        assert!(matches!(
            &plan.outputs[1],
            DifferentialOutput::Aggregate {
                aggregate: DifferentialAggregate::CountRows,
                alias
            } if alias.as_deref() == Some("n_orders")
        ));
        assert!(matches!(
            &plan.outputs[2],
            DifferentialOutput::Aggregate {
                aggregate: DifferentialAggregate::Sum { column },
                alias
            } if column.binding == "o"
                && column.column == "total"
                && alias.as_deref() == Some("gross_total")
        ));
    }

    #[test]
    fn differential_plan_rejects_ambiguous_unqualified_columns() {
        let select = parse_select(
            "SELECT id \
             FROM users AS u \
             INNER JOIN orders AS o ON u.id = o.user_id",
        );

        let error = compile_differential_view_plan(&select)
            .expect_err("multi-table unqualified projection should fail closed");
        assert!(matches!(
            error,
            DifferentialPlanError::AmbiguousUnqualifiedColumn { ref column } if column == "id"
        ));
    }

    #[test]
    fn differential_plan_rejects_distinct_like_shapes() {
        let select = parse_select("SELECT DISTINCT user_id FROM orders");
        let error =
            compile_differential_view_plan(&select).expect_err("DISTINCT should fail closed");
        assert_eq!(error, DifferentialPlanError::UnsupportedDistinct);

        let grouped_only = parse_select("SELECT user_id FROM orders GROUP BY user_id");
        let error = compile_differential_view_plan(&grouped_only)
            .expect_err("GROUP BY without aggregate should fail closed");
        assert_eq!(
            error,
            DifferentialPlanError::UnsupportedGroupingWithoutAggregate
        );
    }

    #[test]
    fn explain_differential_plan_renders_stable_graph() {
        let select = parse_select(
            "SELECT u.id AS user_id, COUNT(*) AS n_orders \
             FROM users AS u \
             INNER JOIN orders AS o ON u.id = o.user_id \
             WHERE o.status = 'paid' \
             GROUP BY u.id",
        );

        let explain = explain_differential_view_plan(&select).expect("shape should compile");
        assert!(explain.contains("DIFFERENTIAL grouped_aggregate"));
        assert!(explain.contains("SOURCE users AS u"));
        assert!(explain.contains("SOURCE orders AS o"));
        assert!(explain.contains("JOIN u.id = o.user_id"));
        assert!(explain.contains("FILTER o.status = 'paid'"));
        assert!(explain.contains("GROUP BY u.id"));
        assert!(explain.contains("EMIT COUNT(*) AS n_orders"));
    }

    #[test]
    fn differential_display_and_explain_labels_are_well_formed() {
        // These pure formatters are only exercised indirectly via explain_text;
        // pin their exact rendering directly.
        let col = DifferentialColumn {
            binding: "t".to_owned(),
            column: "x".to_owned(),
        };
        assert_eq!(col.to_string(), "t.x");

        assert_eq!(DifferentialAggregate::CountRows.to_string(), "COUNT(*)");
        assert_eq!(
            DifferentialAggregate::Sum {
                column: col.clone()
            }
            .to_string(),
            "SUM(t.x)"
        );

        // Source display_name: schema-qualified vs bare.
        let qualified = DifferentialSource {
            schema: Some("main".to_owned()),
            table: "users".to_owned(),
            binding: "u".to_owned(),
        };
        assert_eq!(qualified.display_name(), "main.users");
        let bare = DifferentialSource {
            schema: None,
            table: "users".to_owned(),
            binding: "u".to_owned(),
        };
        assert_eq!(bare.display_name(), "users");

        // Output explain_label: alias appends " AS <alias>", otherwise bare.
        let col_out = DifferentialOutput::Column {
            column: col.clone(),
            alias: None,
        };
        assert_eq!(col_out.explain_label(), "t.x");
        let col_aliased = DifferentialOutput::Column {
            column: col.clone(),
            alias: Some("xx".to_owned()),
        };
        assert_eq!(col_aliased.explain_label(), "t.x AS xx");
        let agg_out = DifferentialOutput::Aggregate {
            aggregate: DifferentialAggregate::CountRows,
            alias: Some("n".to_owned()),
        };
        assert_eq!(agg_out.explain_label(), "COUNT(*) AS n");
        let agg_bare = DifferentialOutput::Aggregate {
            aggregate: DifferentialAggregate::Sum { column: col },
            alias: None,
        };
        assert_eq!(agg_bare.explain_label(), "SUM(t.x)");
    }

    #[test]
    fn differential_source_from_qualified_name_resolves_binding_and_schema() {
        // from_qualified_name is only used inside compile_differential_view_plan,
        // never asserted directly. The binding defaults to the alias when given,
        // otherwise the (unqualified) table name; schema and table are carried
        // from the QualifiedName.

        // With an explicit alias, the binding is the alias.
        let aliased =
            DifferentialSource::from_qualified_name(&QualifiedName::bare("users"), Some("u"));
        assert_eq!(aliased.binding, "u");
        assert_eq!(aliased.table, "users");
        assert_eq!(aliased.schema, None);

        // Without an alias, the binding falls back to the table name.
        let no_alias = DifferentialSource::from_qualified_name(&QualifiedName::bare("users"), None);
        assert_eq!(no_alias.binding, "users");

        // A schema-qualified name carries its schema through; the binding still
        // falls back to the table name when no alias is given.
        let qualified = QualifiedName {
            schema: Some("main".to_owned()),
            name: "users".to_owned(),
        };
        let src = DifferentialSource::from_qualified_name(&qualified, None);
        assert_eq!(src.schema, Some("main".to_owned()));
        assert_eq!(src.table, "users");
        assert_eq!(src.binding, "users");
        assert_eq!(src.display_name(), "main.users");
    }

    #[test]
    fn explain_differential_plan_renders_single_table_rowset_shape() {
        // Companion to the grouped-aggregate explain test: the RowSet (single
        // table, filters, projection) explain rendering was untested.
        let select =
            parse_select("SELECT id, name FROM users WHERE status = 'paid' AND tenant_id = 7");
        let explain = explain_differential_view_plan(&select).expect("rowset shape should compile");
        assert!(
            explain.contains("DIFFERENTIAL row_set"),
            "explain:\n{explain}"
        );
        assert!(explain.contains("SOURCE users AS users"));
        assert!(explain.contains("EMIT users.id"));
        assert!(explain.contains("EMIT users.name"));
        assert!(explain.contains("FILTER users.status = 'paid'"));
        assert!(explain.contains("FILTER users.tenant_id"));
        // A RowSet plan has no join or grouping.
        assert!(!explain.contains("JOIN"), "rowset has no join:\n{explain}");
        assert!(
            !explain.contains("GROUP BY"),
            "rowset has no grouping:\n{explain}"
        );
    }

    #[test]
    fn differential_plan_compiles_global_aggregate_shape() {
        // An aggregate with no GROUP BY is a whole-stream (global) aggregate -- a
        // distinct DifferentialPlanMode that had no compile or explain coverage
        // (existing tests cover only RowSet and GroupedAggregate).
        let select = parse_select("SELECT COUNT(*) AS total FROM orders");

        let plan =
            compile_differential_view_plan(&select).expect("global aggregate should compile");
        assert_eq!(plan.mode, DifferentialPlanMode::GlobalAggregate);
        assert!(
            plan.group_by.is_empty(),
            "a global aggregate has no grouping keys"
        );
        assert_eq!(plan.sources.len(), 1);
        assert_eq!(plan.sources[0].binding, "orders");
        assert_eq!(
            plan.outputs,
            vec![DifferentialOutput::Aggregate {
                aggregate: DifferentialAggregate::CountRows,
                alias: Some("total".to_owned()),
            }]
        );

        // Explain renders the global-aggregate header and emits the aggregate.
        let explain = explain_differential_view_plan(&select).expect("should compile");
        assert!(
            explain.contains("DIFFERENTIAL global_aggregate"),
            "explain:\n{explain}"
        );
        assert!(explain.contains("SOURCE orders AS orders"));
        assert!(explain.contains("EMIT COUNT(*) AS total"));
        assert!(
            !explain.contains("GROUP BY"),
            "global aggregate has no grouping:\n{explain}"
        );
    }

    #[test]
    fn differential_plan_compiles_sum_and_rejects_unsupported_aggregate() {
        // SUM(column) is the other supported aggregate; pin its compile + explain.
        let select = parse_select("SELECT SUM(amount) AS total FROM orders");
        let plan = compile_differential_view_plan(&select).expect("SUM should compile");
        assert_eq!(plan.mode, DifferentialPlanMode::GlobalAggregate);
        assert_eq!(
            plan.outputs,
            vec![DifferentialOutput::Aggregate {
                aggregate: DifferentialAggregate::Sum {
                    column: DifferentialColumn {
                        binding: "orders".to_owned(),
                        column: "amount".to_owned(),
                    },
                },
                alias: Some("total".to_owned()),
            }]
        );
        let explain = explain_differential_view_plan(&select).expect("should compile");
        assert!(
            explain.contains("EMIT SUM(orders.amount) AS total"),
            "explain:\n{explain}"
        );

        // AVG is not a supported differential aggregate -> fails closed.
        let avg = parse_select("SELECT AVG(amount) FROM orders");
        assert!(matches!(
            compile_differential_view_plan(&avg),
            Err(DifferentialPlanError::UnsupportedAggregate { .. })
        ));
    }

    #[test]
    fn differential_plan_rejects_with_compound_and_having_clauses() {
        // Per-clause fail-closed rejections the proptest's case set does not
        // cover (it covers DISTINCT/ORDER BY/LIMIT/star/joins/aggregate-arg).
        let with = parse_select("WITH c AS (SELECT id FROM users) SELECT id FROM c");
        assert_eq!(
            compile_differential_view_plan(&with).unwrap_err(),
            DifferentialPlanError::UnsupportedWithClause
        );

        let compound = parse_select("SELECT id FROM a UNION SELECT id FROM b");
        assert_eq!(
            compile_differential_view_plan(&compound).unwrap_err(),
            DifferentialPlanError::UnsupportedCompoundSelect
        );

        let having =
            parse_select("SELECT status, COUNT(*) FROM orders GROUP BY status HAVING COUNT(*) > 1");
        assert_eq!(
            compile_differential_view_plan(&having).unwrap_err(),
            DifferentialPlanError::UnsupportedHavingClause
        );
    }

    #[test]
    fn differential_plan_rejects_values_core_and_window_clause() {
        // A bare VALUES core is not a differentiable view.
        let values = parse_select("VALUES (1), (2)");
        assert_eq!(
            compile_differential_view_plan(&values).unwrap_err(),
            DifferentialPlanError::UnsupportedValuesCore
        );

        // A named WINDOW clause is unsupported.
        let window = parse_select("SELECT id FROM users WINDOW w AS (PARTITION BY status)");
        assert_eq!(
            compile_differential_view_plan(&window).unwrap_err(),
            DifferentialPlanError::UnsupportedWindowClause
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn differential_rowset_generator_compiles_to_expected_plan(case in arb_rowset_case()) {
            let sql = case.sql();
            let select = parse_select(&sql);
            prop_assert_eq!(
                compile_differential_view_plan(&select),
                Ok(case.expected_plan()),
                "rowset case should compile: {}",
                sql
            );
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn differential_grouped_aggregate_generator_compiles_to_expected_plan(
            case in arb_grouped_aggregate_case()
        ) {
            let sql = case.sql();
            let select = parse_select(&sql);
            prop_assert_eq!(
                compile_differential_view_plan(&select),
                Ok(case.expected_plan()),
                "grouped aggregate case should compile: {}",
                sql
            );
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(96))]

        #[test]
        fn differential_unsupported_generators_fail_closed(case in arb_unsupported_shape_case()) {
            let sql = case.sql();
            let select = parse_select(&sql);
            match compile_differential_view_plan(&select) {
                Ok(plan) => prop_assert!(
                    false,
                    "unsupported case should fail closed: {sql}\nplan: {plan:?}"
                ),
                Err(error) => case.assert_expected_error(error),
            }
        }
    }
}
