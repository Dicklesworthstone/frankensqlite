//! Query planner: name resolution, WHERE analysis, join ordering.
//!
//! This module currently implements compound SELECT ORDER BY resolution,
//! matching C SQLite's exact behavior for alias and numeric column references.
//!
//! Reference: §19 of the FrankenSQLite specification.

use fsqlite_ast::{
    CompoundOp, Expr, Literal, NullsOrder, OrderingTerm, ResultColumn, SelectBody, SelectCore,
    SortDirection, Span,
};

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_ast::{
        ColumnRef, CompoundOp, Distinctness, Expr, Literal, OrderingTerm, ResultColumn, SelectBody,
        SelectCore, SortDirection, Span,
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

    // --- Core resolution tests ---

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
}
