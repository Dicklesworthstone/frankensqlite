//! Row-value (vector) comparisons, lowered to scalar ones (bd-s7gqh).
//!
//! SQLite compares row values element by element (`codeVectorCompare` in
//! expr.c). The code generator compares single values only, so every
//! comparison whose operands are row values is rewritten first:
//!
//! - `(a, b) = (c, d)` to `a = c AND b = d`;
//! - `(a, b) <> (c, d)` to `a <> c OR b <> d`;
//! - `IS` and `IS NOT` the same way, with `IS` / `IS NOT` per element;
//! - `<`, `<=`, `>` and `>=` lexicographically: `a < c OR (a = c AND b <= d)`;
//! - `BETWEEN` to two such comparisons;
//! - a simple `CASE v WHEN w THEN ...` whose operand and WHENs are row values
//!   of one size to `CASE WHEN v = w THEN ...`, each WHEN compared element by
//!   element as stock's `codeVectorCompare` compares it (bd-65jog).
//!
//! A row-value subquery `(SELECT x, y ...)` becomes one scalar subquery per
//! column, each reading the same first row. The rewrite reaches every
//! expression of a statement, nested subqueries included.
//!
//! [`check_statement_row_values`] applies SQLite's two rules for misuse, which
//! both report `row value misused`:
//! - the operands of a comparison, `BETWEEN` or simple `CASE` must have the
//!   same size (`sqlite3ExprVectorSize`: a row value's length, a subquery's
//!   column count, else 1). Name resolution checks this before it resolves
//!   any column (resolve.c);
//! - a row value may not stand where one value is expected: an arithmetic or
//!   logical operand, a function argument, a result column, `ORDER BY`, a
//!   `SET` value for one column, a `VALUES` item, an element of another row
//!   value, and so on.
//!
//! One position takes more: stock's WHERE analysis (`exprAnalyze` in where.c)
//! splits a top-level AND conjunct `X = Y` or `X IS Y` of a WHERE or ON clause,
//! whose operands are row values of one size and not both subqueries, into
//! one term per element, splits those terms again, and never codes the
//! original. An element of such a comparison may itself be a row value:
//! `((a, b), c) = ((1, 2), 3)` is accepted there and misuse everywhere else.
//!
//! Some row values are left as they are, for the paths that handle them: the
//! left side of `IN` and the list rows beside it, `SET (a, b) = (...)`, and a
//! comparison or simple `CASE` with a subquery whose width is not known from
//! its text (`SELECT *`, a compound, `VALUES`).

use std::fmt;

use crate::{
    Assignment, AssignmentTarget, BinaryOp, DeleteStatement, Expr, FrameBound, FromClause,
    FunctionArgs, InSet, InsertSource, InsertStatement, JoinConstraint, LimitClause, OrderingTerm,
    ResultColumn, SelectCore, SelectStatement, Span, Statement, TableOrSubquery, UpdateStatement,
    UpsertAction, UpsertClause, WindowSpec, WithClause,
};

/// SQLite's `row value misused`: a row value of the wrong size, or in a place
/// that takes one value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowValueMisused;

impl fmt::Display for RowValueMisused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("row value misused")
    }
}

impl std::error::Error for RowValueMisused {}

/// Whether `statement` needs the rewrite, checked against SQLite's misuse rules.
///
/// Only SELECT, INSERT, UPDATE, DELETE and EXPLAIN of one of them are looked
/// at; any other statement is `Ok(false)`.
///
/// # Errors
///
/// [`RowValueMisused`] for a row value of the wrong size or in a place that
/// takes one value.
pub fn check_statement_row_values(statement: &Statement) -> Result<bool, RowValueMisused> {
    match statement {
        Statement::Select(select) => check_select(select),
        Statement::Insert(insert) => check_insert(insert),
        Statement::Update(update) => check_update(update),
        Statement::Delete(delete) => check_delete(delete),
        Statement::Explain { stmt, .. } => check_statement_row_values(stmt),
        _ => Ok(false),
    }
}

/// Rewrite every row-value comparison of `statement` in place, for a
/// statement [`check_statement_row_values`] accepted.
///
/// An order comparison and `BETWEEN` repeat operands (`a < c OR (a = c AND
/// ...)`), so a caller numbers anonymous and named placeholders first: a
/// repeated `?` would otherwise take a new number.
pub fn lower_row_values_in_statement(statement: &mut Statement) {
    lower_statement(statement);
}

/// Check `expr`, which stands where one value is expected, and rewrite its
/// row-value comparisons.
///
/// # Errors
///
/// [`RowValueMisused`], as [`check_statement_row_values`] reports it. `expr`
/// is left unchanged then.
pub fn check_and_lower_row_values_in_expr(expr: &mut Expr) -> Result<(), RowValueMisused> {
    if check_scalar(expr)? {
        lower_expr(expr);
    }
    Ok(())
}

/// Rewrite every row-value comparison of `expr` without checking for misuse:
/// a comparison of operands whose sizes differ is left as it is.
pub fn lower_row_values_in_expr(expr: &mut Expr) {
    lower_expr(expr);
}

/// [`lower_row_values_in_expr`] over every expression of `select`.
pub fn lower_row_values_in_select(select: &mut SelectStatement) {
    lower_select(select);
}

const fn is_comparison(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Eq
            | BinaryOp::Ne
            | BinaryOp::Lt
            | BinaryOp::Le
            | BinaryOp::Gt
            | BinaryOp::Ge
            | BinaryOp::Is
            | BinaryOp::IsNot
    )
}

/// The column count of a subquery used as a row value, when its text gives
/// it: one SELECT core whose result columns are all expressions. `None` for a
/// compound, `VALUES` or a `*` result column.
fn subquery_width(select: &SelectStatement) -> Option<usize> {
    if !select.body.compounds.is_empty() {
        return None;
    }
    let SelectCore::Select { columns, .. } = &select.body.select else {
        return None;
    };
    columns
        .iter()
        .all(|column| matches!(column, ResultColumn::Expr { .. }))
        .then_some(columns.len())
}

/// `sqlite3ExprVectorSize`: a row value's length, a subquery's column count
/// (`None` when its text does not give it), else 1.
fn vector_size(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::RowValue(items, _) => Some(items.len()),
        Expr::Subquery(select, _) => subquery_width(select),
        _ => Some(1),
    }
}

/// Whether the sizes of these operands, all known, are one equal size above
/// 1, so they compare as row values.
fn equal_vector_sizes(sizes: &[Option<usize>]) -> Option<bool> {
    let first = sizes.first().copied().flatten()?;
    for size in sizes {
        if (*size)? != first {
            return None;
        }
    }
    Some(first > 1)
}

/// Whether a simple CASE with this operand and these WHENs compares row
/// values: the operand's size is known and above 1, and every WHEN has it.
fn case_operand_lowers(operand: &Expr, whens: &[(Expr, Expr)]) -> bool {
    let Some(size) = vector_size(operand) else {
        return false;
    };
    size > 1
        && whens
            .iter()
            .all(|(when, _)| vector_size(when) == Some(size))
}

/// Whether stock's WHERE analysis splits `left OP right`, for OP `=` or `IS`,
/// into one term per element: both sizes known, equal and above 1, and not
/// both subqueries (`exprAnalyze`, where.c).
fn splits_into_terms(left: &Expr, right: &Expr) -> bool {
    equal_vector_sizes(&[vector_size(left), vector_size(right)]) == Some(true)
        && !(matches!(left, Expr::Subquery(..)) && matches!(right, Expr::Subquery(..)))
}

// ---------------------------------------------------------------------------
// Check: SQLite's misuse rules, and whether anything needs the rewrite.
// ---------------------------------------------------------------------------

type Check = Result<bool, RowValueMisused>;

/// Sizes that are both known must agree.
fn sizes_agree(left: Option<usize>, right: Option<usize>) -> Result<(), RowValueMisused> {
    match (left, right) {
        (Some(left), Some(right)) if left != right => Err(RowValueMisused),
        _ => Ok(()),
    }
}

/// `expr` stands where one value is expected.
fn check_scalar(expr: &Expr) -> Check {
    match expr {
        Expr::RowValue(..) => Err(RowValueMisused),
        Expr::BinaryOp {
            left, op, right, ..
        } if is_comparison(*op) => {
            sizes_agree(vector_size(left), vector_size(right))?;
            let row_values = equal_vector_sizes(&[vector_size(left), vector_size(right)]);
            let operands = if row_values == Some(false) {
                check_scalar(left)? | check_scalar(right)?
            } else {
                check_vector(left)? | check_vector(right)?
            };
            Ok(operands || row_values == Some(true))
        }
        Expr::BinaryOp { left, right, .. } => Ok(check_scalar(left)? | check_scalar(right)?),
        Expr::Between {
            expr, low, high, ..
        } => {
            let sizes = [vector_size(expr), vector_size(low), vector_size(high)];
            sizes_agree(sizes[0], sizes[1])?;
            sizes_agree(sizes[0], sizes[2])?;
            let row_values = equal_vector_sizes(&sizes);
            let operands = if row_values == Some(false) {
                check_scalar(expr)? | check_scalar(low)? | check_scalar(high)?
            } else {
                check_vector(expr)? | check_vector(low)? | check_vector(high)?
            };
            Ok(operands || row_values == Some(true))
        }
        Expr::In { expr, set, .. } => {
            let vector_operand = matches!(expr.as_ref(), Expr::RowValue(..) | Expr::Subquery(..));
            let mut changed = check_vector(expr)?;
            match set {
                InSet::List(items) => {
                    for item in items {
                        changed |= if vector_operand {
                            check_vector(item)?
                        } else {
                            check_scalar(item)?
                        };
                    }
                }
                InSet::Subquery(select) => changed |= check_select(select)?,
                InSet::Table(_) => {}
            }
            Ok(changed)
        }
        Expr::Case {
            operand,
            whens,
            else_expr,
            ..
        } => {
            let mut changed = false;
            if let Some(operand) = operand {
                let operand_size = vector_size(operand);
                for (when, _) in whens {
                    sizes_agree(operand_size, vector_size(when))?;
                }
                // A row-value operand whose WHENs all have its size is
                // rewritten to a searched CASE (bd-65jog).
                changed |= case_operand_lowers(operand, whens);
                let scalar_operand = operand_size == Some(1);
                changed |= if scalar_operand {
                    check_scalar(operand)?
                } else {
                    check_vector(operand)?
                };
                for (when, _) in whens {
                    changed |= if scalar_operand {
                        check_scalar(when)?
                    } else {
                        check_vector(when)?
                    };
                }
            } else {
                for (when, _) in whens {
                    changed |= check_scalar(when)?;
                }
            }
            for (_, then) in whens {
                changed |= check_scalar(then)?;
            }
            if let Some(else_expr) = else_expr {
                changed |= check_scalar(else_expr)?;
            }
            Ok(changed)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Collate { expr, .. } => check_scalar(expr),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            let mut changed = check_scalar(expr)? | check_scalar(pattern)?;
            if let Some(escape) = escape {
                changed |= check_scalar(escape)?;
            }
            Ok(changed)
        }
        Expr::JsonAccess { expr, path, .. } => Ok(check_scalar(expr)? | check_scalar(path)?),
        Expr::FunctionCall {
            args,
            order_by,
            filter,
            over,
            ..
        } => {
            let mut changed = false;
            if let FunctionArgs::List(args) = args {
                for arg in args {
                    changed |= check_scalar(arg)?;
                }
            }
            changed |= check_ordering(order_by)?;
            if let Some(filter) = filter {
                changed |= check_scalar(filter)?;
            }
            if let Some(window) = over {
                changed |= check_window(window)?;
            }
            Ok(changed)
        }
        Expr::Exists { subquery, .. } | Expr::Subquery(subquery, _) => check_select(subquery),
        Expr::Literal(..)
        | Expr::BoundOuterValue { .. }
        | Expr::Column(..)
        | Expr::Raise { .. }
        | Expr::Placeholder(..) => Ok(false),
    }
}

/// `expr` is a WHERE or ON clause. Each top-level AND conjunct that stock's
/// WHERE analysis splits (see [`splits_into_terms`]) is checked as its element
/// terms, each one again as a conjunct, so an element may be a row value; any
/// other conjunct stands where one value is expected.
fn check_where(expr: &Expr) -> Check {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
            ..
        } => Ok(check_where(left)? | check_where(right)?),
        Expr::BinaryOp {
            left,
            op: op @ (BinaryOp::Eq | BinaryOp::Is),
            right,
            span,
        } if splits_into_terms(left, right) => {
            let (Some(lefts), Some(rights)) = (vector_elements(left), vector_elements(right))
            else {
                return check_scalar(expr);
            };
            for (left, right) in lefts.into_iter().zip(rights) {
                check_where(&binary(left, *op, right, *span))?;
            }
            // The comparison itself is rewritten.
            Ok(true)
        }
        other => check_scalar(other),
    }
}

/// `expr` may be a row value: each element of one takes one value.
fn check_vector(expr: &Expr) -> Check {
    match expr {
        Expr::RowValue(items, _) => {
            let mut changed = false;
            for item in items {
                changed |= check_scalar(item)?;
            }
            Ok(changed)
        }
        Expr::Subquery(select, _) => check_select(select),
        other => check_scalar(other),
    }
}

fn check_ordering(terms: &[OrderingTerm]) -> Check {
    let mut changed = false;
    for term in terms {
        changed |= check_scalar(&term.expr)?;
    }
    Ok(changed)
}

fn check_limit(limit: Option<&LimitClause>) -> Check {
    let Some(limit) = limit else {
        return Ok(false);
    };
    let mut changed = check_scalar(&limit.limit)?;
    if let Some(offset) = &limit.offset {
        changed |= check_scalar(offset)?;
    }
    Ok(changed)
}

fn check_window(window: &WindowSpec) -> Check {
    let mut changed = false;
    for expr in &window.partition_by {
        changed |= check_scalar(expr)?;
    }
    changed |= check_ordering(&window.order_by)?;
    if let Some(frame) = &window.frame {
        for bound in std::iter::once(&frame.start).chain(frame.end.as_ref()) {
            if let FrameBound::Preceding(expr) | FrameBound::Following(expr) = bound {
                changed |= check_scalar(expr)?;
            }
        }
    }
    Ok(changed)
}

fn check_with(with: Option<&WithClause>) -> Check {
    let mut changed = false;
    for cte in with.into_iter().flat_map(|with| &with.ctes) {
        changed |= check_select(&cte.query)?;
    }
    Ok(changed)
}

fn check_result_columns(columns: &[ResultColumn]) -> Check {
    let mut changed = false;
    for column in columns {
        if let ResultColumn::Expr { expr, .. } = column {
            changed |= check_scalar(expr)?;
        }
    }
    Ok(changed)
}

fn check_source(source: &TableOrSubquery) -> Check {
    match source {
        TableOrSubquery::Table { .. } => Ok(false),
        TableOrSubquery::Subquery { query, .. } => check_select(query),
        TableOrSubquery::TableFunction { args, .. } => {
            let mut changed = false;
            for arg in args {
                changed |= check_scalar(arg)?;
            }
            Ok(changed)
        }
        TableOrSubquery::ParenJoin(from) => check_from(from),
    }
}

fn check_from(from: &FromClause) -> Check {
    let mut changed = check_source(&from.source)?;
    for join in &from.joins {
        changed |= check_source(&join.table)?;
        if let Some(JoinConstraint::On(expr)) = &join.constraint {
            changed |= check_where(expr)?;
        }
    }
    Ok(changed)
}

fn check_core(core: &SelectCore) -> Check {
    let mut changed = false;
    match core {
        SelectCore::Select {
            columns,
            from,
            where_clause,
            group_by,
            having,
            windows,
            ..
        } => {
            changed |= check_result_columns(columns)?;
            if let Some(from) = from {
                changed |= check_from(from)?;
            }
            if let Some(expr) = where_clause {
                changed |= check_where(expr)?;
            }
            for expr in group_by {
                changed |= check_scalar(expr)?;
            }
            if let Some(expr) = having {
                changed |= check_scalar(expr)?;
            }
            for window in windows {
                changed |= check_window(&window.spec)?;
            }
        }
        SelectCore::Values(values) => {
            for expr in values.rows().iter().flatten() {
                changed |= check_scalar(expr)?;
            }
        }
    }
    Ok(changed)
}

fn check_select(select: &SelectStatement) -> Check {
    let mut changed = check_with(select.with.as_ref())?;
    changed |= check_core(&select.body.select)?;
    for (_, core) in &select.body.compounds {
        changed |= check_core(core)?;
    }
    changed |= check_ordering(&select.order_by)?;
    Ok(changed | check_limit(select.limit.as_ref())?)
}

fn check_assignments(assignments: &[Assignment]) -> Check {
    let mut changed = false;
    for assignment in assignments {
        changed |= match &assignment.target {
            AssignmentTarget::Column(_) => check_scalar(&assignment.value)?,
            // `SET (a, b) = (x, y)` takes a row value; a size mismatch is
            // the assignment's own error.
            AssignmentTarget::ColumnList(_) => check_vector(&assignment.value)?,
        };
    }
    Ok(changed)
}

fn check_upsert(upsert: &UpsertClause) -> Check {
    let mut changed = false;
    if let Some(target) = &upsert.target {
        for column in &target.columns {
            changed |= check_scalar(&column.expr)?;
        }
        if let Some(expr) = &target.where_clause {
            changed |= check_scalar(expr)?;
        }
    }
    if let UpsertAction::Update {
        assignments,
        where_clause,
    } = &upsert.action
    {
        changed |= check_assignments(assignments)?;
        if let Some(expr) = where_clause {
            changed |= check_scalar(expr)?;
        }
    }
    Ok(changed)
}

fn check_insert(insert: &InsertStatement) -> Check {
    let mut changed = check_with(insert.with.as_ref())?;
    match &insert.source {
        InsertSource::Values(rows) => {
            for expr in rows.iter().flatten() {
                changed |= check_scalar(expr)?;
            }
        }
        InsertSource::Select(select) => changed |= check_select(select)?,
        InsertSource::DefaultValues => {}
    }
    for upsert in &insert.upsert {
        changed |= check_upsert(upsert)?;
    }
    Ok(changed | check_result_columns(&insert.returning)?)
}

fn check_update(update: &UpdateStatement) -> Check {
    let mut changed = check_with(update.with.as_ref())?;
    changed |= check_assignments(&update.assignments)?;
    if let Some(from) = &update.from {
        changed |= check_from(from)?;
    }
    if let Some(expr) = &update.where_clause {
        changed |= check_where(expr)?;
    }
    changed |= check_result_columns(&update.returning)?;
    changed |= check_ordering(&update.order_by)?;
    Ok(changed | check_limit(update.limit.as_ref())?)
}

fn check_delete(delete: &DeleteStatement) -> Check {
    let mut changed = check_with(delete.with.as_ref())?;
    if let Some(expr) = &delete.where_clause {
        changed |= check_where(expr)?;
    }
    changed |= check_result_columns(&delete.returning)?;
    changed |= check_ordering(&delete.order_by)?;
    Ok(changed | check_limit(delete.limit.as_ref())?)
}

// ---------------------------------------------------------------------------
// Lower: rewrite each row-value comparison whose operand sizes agree.
// ---------------------------------------------------------------------------

/// `select` cut down to its `index`-th result column. FROM, WHERE, ORDER BY
/// and LIMIT stay, so every column is read from the same first row and an
/// outer reference still correlates.
fn project_subquery_column(select: &SelectStatement, index: usize) -> SelectStatement {
    let mut projected = select.clone();
    if let SelectCore::Select { columns, .. } = &mut projected.body.select {
        let column = columns.swap_remove(index);
        *columns = vec![column];
    }
    projected
}

/// The elements of a row-value operand of known size: a row value's items,
/// or one scalar subquery per column of a subquery.
fn vector_elements(expr: &Expr) -> Option<Vec<Expr>> {
    match expr {
        Expr::RowValue(items, _) => Some(items.clone()),
        Expr::Subquery(select, span) => {
            let width = subquery_width(select)?;
            Some(
                (0..width)
                    .map(|index| {
                        Expr::Subquery(Box::new(project_subquery_column(select, index)), *span)
                    })
                    .collect(),
            )
        }
        _ => None,
    }
}

fn binary(left: Expr, op: BinaryOp, right: Expr, span: Span) -> Expr {
    Expr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
        span,
    }
}

/// `a OP b` over equal-length element lists.
fn expand_comparison(left: &[Expr], op: BinaryOp, right: &[Expr], span: Span) -> Option<Expr> {
    let pairs = left.iter().cloned().zip(right.iter().cloned());
    match op {
        // (a, b) = (c, d) is a = c AND b = d; IS is the same, per element.
        BinaryOp::Eq | BinaryOp::Is => pairs
            .map(|(l, r)| binary(l, op, r, span))
            .reduce(|acc, cmp| binary(acc, BinaryOp::And, cmp, span)),
        // (a, b) <> (c, d) is a <> c OR b <> d; IS NOT the same, per element.
        BinaryOp::Ne | BinaryOp::IsNot => pairs
            .map(|(l, r)| binary(l, op, r, span))
            .reduce(|acc, cmp| binary(acc, BinaryOp::Or, cmp, span)),
        BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge => {
            let strict = if matches!(op, BinaryOp::Lt | BinaryOp::Le) {
                BinaryOp::Lt
            } else {
                BinaryOp::Gt
            };
            Some(expand_order(left, right, strict, op, span))
        }
        _ => None,
    }
}

/// Lexicographic order: `a0 STRICT b0 OR (a0 = b0 AND <rest>)`, the last
/// element compared with the operator as written. The first unequal element
/// decides; a NULL there makes the result NULL.
fn expand_order(
    left: &[Expr],
    right: &[Expr],
    strict: BinaryOp,
    last: BinaryOp,
    span: Span,
) -> Expr {
    if left.len() == 1 {
        return binary(left[0].clone(), last, right[0].clone(), span);
    }
    let head_strict = binary(left[0].clone(), strict, right[0].clone(), span);
    let head_equal = binary(left[0].clone(), BinaryOp::Eq, right[0].clone(), span);
    let rest = expand_order(&left[1..], &right[1..], strict, last, span);
    binary(
        head_strict,
        BinaryOp::Or,
        binary(head_equal, BinaryOp::And, rest, span),
        span,
    )
}

/// The rewrite of one node, when it is a row-value comparison, BETWEEN or
/// simple CASE whose operand sizes are known and agree.
fn lowered_node(expr: &Expr) -> Option<Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op,
            right,
            span,
        } if is_comparison(*op)
            && equal_vector_sizes(&[vector_size(left), vector_size(right)]) == Some(true) =>
        {
            expand_comparison(
                &vector_elements(left)?,
                *op,
                &vector_elements(right)?,
                *span,
            )
        }
        Expr::Between {
            expr,
            low,
            high,
            not,
            span,
        } if equal_vector_sizes(&[vector_size(expr), vector_size(low), vector_size(high)])
            == Some(true) =>
        {
            // x BETWEEN l AND h is x >= l AND x <= h; NOT BETWEEN is
            // x < l OR x > h.
            let (low_op, high_op, combine) = if *not {
                (BinaryOp::Lt, BinaryOp::Gt, BinaryOp::Or)
            } else {
                (BinaryOp::Ge, BinaryOp::Le, BinaryOp::And)
            };
            let operand = vector_elements(expr)?;
            let low = expand_comparison(&operand, low_op, &vector_elements(low)?, *span)?;
            let high = expand_comparison(&operand, high_op, &vector_elements(high)?, *span)?;
            Some(binary(low, combine, high, *span))
        }
        // CASE v WHEN w THEN r ... END is CASE WHEN v = w THEN r ... END; the
        // `=` is then lowered element by element below. The operand is
        // evaluated once per WHEN, not once, which only a non-deterministic
        // operand can tell apart (the order comparisons above repeat their
        // operands the same way).
        Expr::Case {
            operand: Some(operand),
            whens,
            else_expr,
            span,
        } if case_operand_lowers(operand, whens) => Some(Expr::Case {
            operand: None,
            whens: whens
                .iter()
                .map(|(when, then)| {
                    (
                        binary((**operand).clone(), BinaryOp::Eq, when.clone(), *span),
                        then.clone(),
                    )
                })
                .collect(),
            else_expr: else_expr.clone(),
            span: *span,
        }),
        _ => None,
    }
}

/// Rewrite `expr` in place: the node first, then everything below it,
/// nested subqueries included.
fn lower_expr(expr: &mut Expr) {
    if let Some(lowered) = lowered_node(expr) {
        *expr = lowered;
    }
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            lower_expr(left);
            lower_expr(right);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            lower_expr(expr);
            lower_expr(low);
            lower_expr(high);
        }
        Expr::In { expr, set, .. } => {
            lower_expr(expr);
            match set {
                InSet::List(items) => items.iter_mut().for_each(lower_expr),
                InSet::Subquery(select) => lower_select(select),
                InSet::Table(_) => {}
            }
        }
        Expr::Case {
            operand,
            whens,
            else_expr,
            ..
        } => {
            if let Some(operand) = operand {
                lower_expr(operand);
            }
            for (when, then) in whens {
                lower_expr(when);
                lower_expr(then);
            }
            if let Some(else_expr) = else_expr {
                lower_expr(else_expr);
            }
        }
        Expr::UnaryOp { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Collate { expr, .. } => lower_expr(expr),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            lower_expr(expr);
            lower_expr(pattern);
            if let Some(escape) = escape {
                lower_expr(escape);
            }
        }
        Expr::JsonAccess { expr, path, .. } => {
            lower_expr(expr);
            lower_expr(path);
        }
        Expr::FunctionCall {
            args,
            order_by,
            filter,
            over,
            ..
        } => {
            if let FunctionArgs::List(args) = args {
                args.iter_mut().for_each(lower_expr);
            }
            lower_ordering(order_by);
            if let Some(filter) = filter {
                lower_expr(filter);
            }
            if let Some(window) = over {
                lower_window(window);
            }
        }
        Expr::RowValue(items, _) => items.iter_mut().for_each(lower_expr),
        Expr::Exists { subquery, .. } | Expr::Subquery(subquery, _) => lower_select(subquery),
        Expr::Literal(..)
        | Expr::BoundOuterValue { .. }
        | Expr::Column(..)
        | Expr::Raise { .. }
        | Expr::Placeholder(..) => {}
    }
}

fn lower_ordering(terms: &mut [OrderingTerm]) {
    for term in terms {
        lower_expr(&mut term.expr);
    }
}

fn lower_limit(limit: Option<&mut LimitClause>) {
    if let Some(limit) = limit {
        lower_expr(&mut limit.limit);
        if let Some(offset) = &mut limit.offset {
            lower_expr(offset);
        }
    }
}

fn lower_window(window: &mut WindowSpec) {
    window.partition_by.iter_mut().for_each(lower_expr);
    lower_ordering(&mut window.order_by);
    if let Some(frame) = &mut window.frame {
        for bound in std::iter::once(&mut frame.start).chain(frame.end.as_mut()) {
            if let FrameBound::Preceding(expr) | FrameBound::Following(expr) = bound {
                lower_expr(expr);
            }
        }
    }
}

fn lower_with(with: Option<&mut WithClause>) {
    for cte in with.into_iter().flat_map(|with| &mut with.ctes) {
        lower_select(&mut cte.query);
    }
}

fn lower_result_columns(columns: &mut [ResultColumn]) {
    for column in columns {
        if let ResultColumn::Expr { expr, .. } = column {
            lower_expr(expr);
        }
    }
}

fn lower_source(source: &mut TableOrSubquery) {
    match source {
        TableOrSubquery::Table { .. } => {}
        TableOrSubquery::Subquery { query, .. } => lower_select(query),
        TableOrSubquery::TableFunction { args, .. } => args.iter_mut().for_each(lower_expr),
        TableOrSubquery::ParenJoin(from) => lower_from(from),
    }
}

fn lower_from(from: &mut FromClause) {
    lower_source(&mut from.source);
    for join in &mut from.joins {
        lower_source(&mut join.table);
        if let Some(JoinConstraint::On(expr)) = &mut join.constraint {
            lower_expr(expr);
        }
    }
}

fn lower_core(core: &mut SelectCore) {
    match core {
        SelectCore::Select {
            columns,
            from,
            where_clause,
            group_by,
            having,
            windows,
            ..
        } => {
            lower_result_columns(columns);
            if let Some(from) = from {
                lower_from(from);
            }
            if let Some(expr) = where_clause {
                lower_expr(expr);
            }
            group_by.iter_mut().for_each(lower_expr);
            if let Some(expr) = having {
                lower_expr(expr);
            }
            for window in windows {
                lower_window(&mut window.spec);
            }
        }
        SelectCore::Values(values) => {
            for row in values.iter_mut() {
                row.iter_mut().for_each(lower_expr);
            }
        }
    }
}

fn lower_select(select: &mut SelectStatement) {
    lower_with(select.with.as_mut());
    lower_core(&mut select.body.select);
    for (_, core) in &mut select.body.compounds {
        lower_core(core);
    }
    lower_ordering(&mut select.order_by);
    lower_limit(select.limit.as_mut());
}

fn lower_assignments(assignments: &mut [Assignment]) {
    for assignment in assignments {
        lower_expr(&mut assignment.value);
    }
}

fn lower_statement(statement: &mut Statement) {
    match statement {
        Statement::Select(select) => lower_select(select),
        Statement::Insert(insert) => {
            lower_with(insert.with.as_mut());
            match &mut insert.source {
                InsertSource::Values(rows) => {
                    rows.iter_mut().flatten().for_each(lower_expr);
                }
                InsertSource::Select(select) => lower_select(select),
                InsertSource::DefaultValues => {}
            }
            for upsert in &mut insert.upsert {
                if let Some(target) = &mut upsert.target {
                    for column in &mut target.columns {
                        lower_expr(&mut column.expr);
                    }
                    if let Some(expr) = &mut target.where_clause {
                        lower_expr(expr);
                    }
                }
                if let UpsertAction::Update {
                    assignments,
                    where_clause,
                } = &mut upsert.action
                {
                    lower_assignments(assignments);
                    if let Some(expr) = where_clause {
                        lower_expr(expr);
                    }
                }
            }
            lower_result_columns(&mut insert.returning);
        }
        Statement::Update(update) => {
            lower_with(update.with.as_mut());
            lower_assignments(&mut update.assignments);
            if let Some(from) = &mut update.from {
                lower_from(from);
            }
            if let Some(expr) = &mut update.where_clause {
                lower_expr(expr);
            }
            lower_result_columns(&mut update.returning);
            lower_ordering(&mut update.order_by);
            lower_limit(update.limit.as_mut());
        }
        Statement::Delete(delete) => {
            lower_with(delete.with.as_mut());
            if let Some(expr) = &mut delete.where_clause {
                lower_expr(expr);
            }
            lower_result_columns(&mut delete.returning);
            lower_ordering(&mut delete.order_by);
            lower_limit(delete.limit.as_mut());
        }
        Statement::Explain { stmt, .. } => lower_statement(stmt),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnRef, Literal, SelectBody};

    fn int(value: i64) -> Expr {
        Expr::Literal(Literal::Integer(value), Span::ZERO)
    }

    fn col(name: &str) -> Expr {
        Expr::Column(ColumnRef::bare(name), Span::ZERO)
    }

    fn row(items: Vec<Expr>) -> Expr {
        Expr::RowValue(items, Span::ZERO)
    }

    fn cmp(left: Expr, op: BinaryOp, right: Expr) -> Expr {
        binary(left, op, right, Span::ZERO)
    }

    fn select_of(columns: Vec<Expr>) -> SelectStatement {
        SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: crate::Distinctness::All,
                    columns: columns
                        .into_iter()
                        .map(|expr| ResultColumn::Expr { expr, alias: None })
                        .collect(),
                    from: None,
                    where_clause: None,
                    group_by: Vec::new(),
                    having: None,
                    windows: Vec::new(),
                },
                compounds: Vec::new(),
            },
            order_by: Vec::new(),
            limit: None,
        }
    }

    fn subquery(columns: Vec<Expr>) -> Expr {
        Expr::Subquery(Box::new(select_of(columns)), Span::ZERO)
    }

    fn lowered(mut expr: Expr) -> Result<Expr, RowValueMisused> {
        check_and_lower_row_values_in_expr(&mut expr)?;
        Ok(expr)
    }

    #[test]
    fn equality_is_a_conjunction_and_inequality_a_disjunction() {
        let pair = || row(vec![col("a"), col("n")]);
        let consts = || row(vec![int(1), int(2)]);
        assert_eq!(
            lowered(cmp(pair(), BinaryOp::Eq, consts())),
            Ok(cmp(
                cmp(col("a"), BinaryOp::Eq, int(1)),
                BinaryOp::And,
                cmp(col("n"), BinaryOp::Eq, int(2)),
            ))
        );
        assert_eq!(
            lowered(cmp(pair(), BinaryOp::IsNot, consts())),
            Ok(cmp(
                cmp(col("a"), BinaryOp::IsNot, int(1)),
                BinaryOp::Or,
                cmp(col("n"), BinaryOp::IsNot, int(2)),
            ))
        );
    }

    #[test]
    fn order_is_lexicographic_not_member_wise() {
        // (a, n) < (1, 2) is a < 1 OR (a = 1 AND n < 2); a member-wise AND of
        // a < 1 and n < 2 would be wrong.
        let expected = cmp(
            cmp(col("a"), BinaryOp::Lt, int(1)),
            BinaryOp::Or,
            cmp(
                cmp(col("a"), BinaryOp::Eq, int(1)),
                BinaryOp::And,
                cmp(col("n"), BinaryOp::Lt, int(2)),
            ),
        );
        assert_eq!(
            lowered(cmp(
                row(vec![col("a"), col("n")]),
                BinaryOp::Lt,
                row(vec![int(1), int(2)])
            )),
            Ok(expected)
        );
        // <= keeps <= only on the last element.
        let le = lowered(cmp(
            row(vec![col("a"), col("n")]),
            BinaryOp::Le,
            row(vec![int(1), int(2)]),
        ))
        .expect("lowers");
        let Expr::BinaryOp { left, right, .. } = le else {
            panic!("not a binary op: {le:?}");
        };
        assert_eq!(*left, cmp(col("a"), BinaryOp::Lt, int(1)));
        assert_eq!(
            *right,
            cmp(
                cmp(col("a"), BinaryOp::Eq, int(1)),
                BinaryOp::And,
                cmp(col("n"), BinaryOp::Le, int(2)),
            )
        );
    }

    #[test]
    fn a_row_value_subquery_becomes_one_scalar_subquery_per_column() {
        let expr = cmp(
            row(vec![col("a"), col("n")]),
            BinaryOp::Eq,
            subquery(vec![col("x"), col("y")]),
        );
        assert_eq!(
            lowered(expr),
            Ok(cmp(
                cmp(col("a"), BinaryOp::Eq, subquery(vec![col("x")])),
                BinaryOp::And,
                cmp(col("n"), BinaryOp::Eq, subquery(vec![col("y")])),
            ))
        );
    }

    #[test]
    fn sizes_that_differ_are_misuse() {
        let two = || row(vec![int(1), int(2)]);
        let three = row(vec![int(1), int(2), int(3)]);
        assert_eq!(
            lowered(cmp(two(), BinaryOp::Eq, three)),
            Err(RowValueMisused)
        );
        assert_eq!(
            lowered(cmp(two(), BinaryOp::Lt, int(1))),
            Err(RowValueMisused)
        );
        // A one-column subquery against a pair, and a scalar against a
        // two-column subquery.
        assert_eq!(
            lowered(cmp(two(), BinaryOp::Eq, subquery(vec![col("a")]))),
            Err(RowValueMisused)
        );
        assert_eq!(
            lowered(cmp(
                col("n"),
                BinaryOp::Eq,
                subquery(vec![col("a"), col("n")])
            )),
            Err(RowValueMisused)
        );
        let between = Expr::Between {
            expr: Box::new(two()),
            low: Box::new(row(vec![int(0), int(0)])),
            high: Box::new(int(5)),
            not: false,
            span: Span::ZERO,
        };
        assert_eq!(lowered(between), Err(RowValueMisused));
    }

    #[test]
    fn a_row_value_where_one_value_is_expected_is_misuse() {
        let two = || row(vec![int(1), int(2)]);
        assert_eq!(lowered(two()), Err(RowValueMisused));
        assert_eq!(
            lowered(cmp(two(), BinaryOp::Add, int(1))),
            Err(RowValueMisused)
        );
        assert_eq!(
            lowered(Expr::IsNull {
                expr: Box::new(two()),
                not: false,
                span: Span::ZERO,
            }),
            Err(RowValueMisused)
        );
        // A row value inside a row value.
        assert_eq!(
            lowered(cmp(
                row(vec![two(), int(3)]),
                BinaryOp::Eq,
                row(vec![two(), int(3)])
            )),
            Err(RowValueMisused)
        );
        // `1 IN (1, (1, 2))`: a row in the list of a single value.
        assert_eq!(
            lowered(Expr::In {
                expr: Box::new(int(1)),
                set: InSet::List(vec![int(1), two()]),
                not: false,
                span: Span::ZERO,
            }),
            Err(RowValueMisused)
        );
    }

    #[test]
    fn vector_in_and_unknown_widths_are_left_alone() {
        let vector_in = Expr::In {
            expr: Box::new(row(vec![col("a"), col("n")])),
            set: InSet::List(vec![row(vec![int(1), int(2)]), row(vec![int(3), int(4)])]),
            not: false,
            span: Span::ZERO,
        };
        assert_eq!(lowered(vector_in.clone()), Ok(vector_in));
        let mut star = select_of(Vec::new());
        if let SelectCore::Select { columns, .. } = &mut star.body.select {
            columns.push(ResultColumn::Star);
        }
        let unknown = cmp(
            row(vec![col("a"), col("n")]),
            BinaryOp::Eq,
            Expr::Subquery(Box::new(star), Span::ZERO),
        );
        assert_eq!(lowered(unknown.clone()), Ok(unknown));
    }

    #[test]
    fn scalar_comparisons_and_lowered_trees_need_no_rewrite() {
        let scalar = cmp(col("a"), BinaryOp::Eq, int(1));
        assert_eq!(check_scalar(&scalar), Ok(false));
        let mut expr = cmp(
            row(vec![col("a"), col("n")]),
            BinaryOp::Ge,
            row(vec![int(1), int(2)]),
        );
        assert_eq!(check_scalar(&expr), Ok(true));
        lower_row_values_in_expr(&mut expr);
        assert_eq!(check_scalar(&expr), Ok(false));
    }

    #[test]
    fn a_simple_case_over_row_values_is_a_searched_case() {
        let case = |operand: Expr, when: Expr| Expr::Case {
            operand: Some(Box::new(operand)),
            whens: vec![(when, int(1))],
            else_expr: Some(Box::new(int(0))),
            span: Span::ZERO,
        };
        let pair = || row(vec![col("a"), col("n")]);
        assert_eq!(
            lowered(case(pair(), row(vec![int(1), int(2)]))),
            Ok(Expr::Case {
                operand: None,
                whens: vec![(
                    cmp(
                        cmp(col("a"), BinaryOp::Eq, int(1)),
                        BinaryOp::And,
                        cmp(col("n"), BinaryOp::Eq, int(2)),
                    ),
                    int(1),
                )],
                else_expr: Some(Box::new(int(0))),
                span: Span::ZERO,
            })
        );
        // The sizes must still agree, and a scalar CASE is no rewrite.
        assert_eq!(lowered(case(pair(), int(1))), Err(RowValueMisused));
        assert_eq!(lowered(case(int(1), pair())), Err(RowValueMisused));
        assert_eq!(check_scalar(&case(col("a"), int(1))), Ok(false));
    }

    #[test]
    fn where_conjuncts_may_compare_nested_row_values() {
        let nested = |first: Expr| row(vec![row(vec![first, col("n")]), int(1)]);
        let eq = || cmp(nested(col("a")), BinaryOp::Eq, nested(int(5)));
        // A WHERE conjunct, alone or under AND, splits into element terms.
        assert_eq!(check_where(&eq()), Ok(true));
        let conjunction = cmp(cmp(col("n"), BinaryOp::Gt, int(0)), BinaryOp::And, eq());
        assert_eq!(check_where(&conjunction), Ok(true));
        let mut lowered_where = conjunction;
        lower_row_values_in_expr(&mut lowered_where);
        assert_eq!(check_scalar(&lowered_where), Ok(false));
        // Anywhere else a nested row value is misuse: as one value, under OR,
        // and with an order comparison.
        assert_eq!(check_scalar(&eq()), Err(RowValueMisused));
        assert_eq!(
            check_where(&cmp(eq(), BinaryOp::Or, int(1))),
            Err(RowValueMisused)
        );
        assert_eq!(
            check_where(&cmp(nested(col("a")), BinaryOp::Lt, nested(int(5)))),
            Err(RowValueMisused)
        );
        // An element pair whose sizes differ is misuse even in a conjunct.
        let uneven = cmp(nested(col("a")), BinaryOp::Eq, row(vec![col("a"), int(1)]));
        assert_eq!(check_where(&uneven), Err(RowValueMisused));
    }
}
