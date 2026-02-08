//! Expression rebase safety validation (§5.10.1).
//!
//! Converts parser AST expressions ([`Expr`]) to the safe [`RebaseExpr`]
//! subset. Only proven-deterministic, side-effect-free expressions pass
//! validation; everything else returns `None`.

use fsqlite_types::SqliteValue;
use fsqlite_types::glossary::{ColumnIdx, RebaseBinaryOp, RebaseExpr, RebaseUnaryOp};

use crate::{BinaryOp, ColumnRef, Expr, FunctionArgs, Literal, UnaryOp};

/// Known non-deterministic SQLite builtins.
///
/// These functions are ALWAYS rejected regardless of the caller's
/// `is_deterministic` callback.
const ALWAYS_NONDETERMINISTIC: &[&str] = &[
    "random",
    "randomblob",
    "last_insert_rowid",
    "changes",
    "total_changes",
    "sqlite_version",
    "sqlite_source_id",
    "sqlite_compileoption_get",
    "sqlite_compileoption_used",
    "sqlite_offset",
];

/// Convert a parser AST [`Expr`] to a [`RebaseExpr`] if the expression is
/// safe for deterministic replay.
///
/// Returns `None` for subqueries, non-deterministic functions, bind parameters,
/// comparison operators, and other forms that cannot be safely replayed during
/// transaction rebase.
///
/// # Arguments
///
/// * `expr` -- The parser AST expression to validate.
/// * `is_deterministic` -- Called with lowercased function name; return `true`
///   for functions known to be deterministic (e.g., `abs`, `length`). Functions
///   in the built-in non-deterministic blocklist are always rejected.
/// * `resolve_column` -- Maps AST [`ColumnRef`] to the resolved [`ColumnIdx`].
///   Return `None` if the column reference cannot be resolved.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn expr_is_rebase_safe(
    expr: &Expr,
    is_deterministic: &dyn Fn(&str) -> bool,
    resolve_column: &dyn Fn(&ColumnRef) -> Option<ColumnIdx>,
) -> Option<RebaseExpr> {
    match expr {
        Expr::Literal(lit, _) => literal_to_rebase(lit),

        Expr::Column(col_ref, _) => {
            let idx = resolve_column(col_ref)?;
            Some(RebaseExpr::ColumnRef(idx))
        }

        Expr::BinaryOp {
            left, op, right, ..
        } => {
            let left_safe = expr_is_rebase_safe(left, is_deterministic, resolve_column)?;
            let right_safe = expr_is_rebase_safe(right, is_deterministic, resolve_column)?;

            // String concatenation maps to its own variant.
            if *op == BinaryOp::Concat {
                return Some(RebaseExpr::Concat {
                    left: Box::new(left_safe),
                    right: Box::new(right_safe),
                });
            }

            let rebase_op = binary_op_to_rebase(*op)?;
            Some(RebaseExpr::BinaryOp {
                op: rebase_op,
                left: Box::new(left_safe),
                right: Box::new(right_safe),
            })
        }

        Expr::UnaryOp {
            op, expr: inner, ..
        } => {
            let inner_safe = expr_is_rebase_safe(inner, is_deterministic, resolve_column)?;

            // Unary plus is a no-op; pass through.
            if *op == UnaryOp::Plus {
                return Some(inner_safe);
            }

            let rebase_op = unary_op_to_rebase(*op)?;
            Some(RebaseExpr::UnaryOp {
                op: rebase_op,
                operand: Box::new(inner_safe),
            })
        }

        Expr::Cast {
            expr: inner,
            type_name,
            ..
        } => {
            let inner_safe = expr_is_rebase_safe(inner, is_deterministic, resolve_column)?;
            Some(RebaseExpr::Cast {
                expr: Box::new(inner_safe),
                type_name: type_name.name.clone(),
            })
        }

        Expr::Case {
            operand,
            whens,
            else_expr,
            ..
        } => {
            let operand_safe = if let Some(o) = operand {
                Some(expr_is_rebase_safe(o, is_deterministic, resolve_column)?)
            } else {
                None
            };

            let mut when_clauses = Vec::with_capacity(whens.len());
            for (when, then) in whens {
                let when_safe = expr_is_rebase_safe(when, is_deterministic, resolve_column)?;
                let then_safe = expr_is_rebase_safe(then, is_deterministic, resolve_column)?;
                when_clauses.push((when_safe, then_safe));
            }

            let else_safe = if let Some(e) = else_expr {
                Some(expr_is_rebase_safe(e, is_deterministic, resolve_column)?)
            } else {
                None
            };

            Some(RebaseExpr::Case {
                operand: operand_safe.map(Box::new),
                when_clauses,
                else_clause: else_safe.map(Box::new),
            })
        }

        Expr::FunctionCall {
            name,
            args,
            distinct,
            filter,
            over,
            ..
        } => {
            // Window functions, DISTINCT, and FILTER are not rebase-safe.
            if over.is_some() || *distinct || filter.is_some() {
                return None;
            }

            let lower_name = name.to_ascii_lowercase();

            // Always-blocked builtins.
            if ALWAYS_NONDETERMINISTIC.contains(&lower_name.as_str()) {
                return None;
            }

            // Check caller's determinism oracle.
            if !is_deterministic(&lower_name) {
                return None;
            }

            let safe_args = match args {
                FunctionArgs::Star => return None,
                FunctionArgs::List(exprs) => {
                    let mut safe = Vec::with_capacity(exprs.len());
                    for arg in exprs {
                        safe.push(expr_is_rebase_safe(arg, is_deterministic, resolve_column)?);
                    }
                    safe
                }
            };

            // Recognize COALESCE and NULLIF as special RebaseExpr forms.
            if lower_name == "coalesce" {
                return Some(RebaseExpr::Coalesce(safe_args));
            }
            if lower_name == "nullif" && safe_args.len() == 2 {
                let mut args_iter = safe_args.into_iter();
                return Some(RebaseExpr::NullIf {
                    left: Box::new(args_iter.next()?),
                    right: Box::new(args_iter.next()?),
                });
            }

            Some(RebaseExpr::FunctionCall {
                name: lower_name,
                args: safe_args,
            })
        }

        // All other forms are not rebase-safe.
        Expr::Subquery(..)
        | Expr::Exists { .. }
        | Expr::In { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::Collate { .. }
        | Expr::IsNull { .. }
        | Expr::Raise { .. }
        | Expr::JsonAccess { .. }
        | Expr::RowValue(..)
        | Expr::Placeholder(..) => None,
    }
}

/// Convert an AST literal to a `RebaseExpr` literal.
///
/// Non-deterministic literals (`CURRENT_TIME`, `CURRENT_DATE`,
/// `CURRENT_TIMESTAMP`) are rejected.
fn literal_to_rebase(lit: &Literal) -> Option<RebaseExpr> {
    let val = match lit {
        Literal::Integer(i) => SqliteValue::Integer(*i),
        Literal::Float(f) => SqliteValue::Float(*f),
        Literal::String(s) => SqliteValue::Text(s.clone()),
        Literal::Blob(b) => SqliteValue::Blob(b.clone()),
        Literal::Null => SqliteValue::Null,
        Literal::True => SqliteValue::Integer(1),
        Literal::False => SqliteValue::Integer(0),
        // Non-deterministic: depend on wall-clock time.
        Literal::CurrentTime | Literal::CurrentDate | Literal::CurrentTimestamp => return None,
    };
    Some(RebaseExpr::Literal(val))
}

/// Convert an AST binary operator to a [`RebaseBinaryOp`].
///
/// Only arithmetic and bitwise operators are representable. Comparison and
/// logical operators return `None`.
fn binary_op_to_rebase(op: BinaryOp) -> Option<RebaseBinaryOp> {
    match op {
        BinaryOp::Add => Some(RebaseBinaryOp::Add),
        BinaryOp::Subtract => Some(RebaseBinaryOp::Subtract),
        BinaryOp::Multiply => Some(RebaseBinaryOp::Multiply),
        BinaryOp::Divide => Some(RebaseBinaryOp::Divide),
        BinaryOp::Modulo => Some(RebaseBinaryOp::Remainder),
        BinaryOp::BitAnd => Some(RebaseBinaryOp::BitwiseAnd),
        BinaryOp::BitOr => Some(RebaseBinaryOp::BitwiseOr),
        BinaryOp::ShiftLeft => Some(RebaseBinaryOp::ShiftLeft),
        BinaryOp::ShiftRight => Some(RebaseBinaryOp::ShiftRight),
        // Concat is handled separately in the main function.
        // Comparison and logical operators are not representable.
        BinaryOp::Concat
        | BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Le
        | BinaryOp::Gt
        | BinaryOp::Ge
        | BinaryOp::Is
        | BinaryOp::IsNot
        | BinaryOp::And
        | BinaryOp::Or => None,
    }
}

/// Convert an AST unary operator to a [`RebaseUnaryOp`].
fn unary_op_to_rebase(op: UnaryOp) -> Option<RebaseUnaryOp> {
    match op {
        UnaryOp::Negate => Some(RebaseUnaryOp::Negate),
        UnaryOp::BitNot => Some(RebaseUnaryOp::BitwiseNot),
        UnaryOp::Not => Some(RebaseUnaryOp::Not),
        // Plus is handled as a no-op before calling this.
        UnaryOp::Plus => None,
    }
}

/// Default set of SQLite built-in deterministic scalar functions.
///
/// Returns `true` for standard SQLite functions known to be deterministic.
/// Use this as a convenience when no custom function registry is available.
#[must_use]
pub fn sqlite_builtin_is_deterministic(name: &str) -> bool {
    matches!(
        name,
        "abs"
            | "char"
            | "coalesce"
            | "glob"
            | "hex"
            | "ifnull"
            | "iif"
            | "instr"
            | "length"
            | "like"
            | "likelihood"
            | "likely"
            | "lower"
            | "ltrim"
            | "max"
            | "min"
            | "nullif"
            | "printf"
            | "format"
            | "quote"
            | "replace"
            | "round"
            | "rtrim"
            | "sign"
            | "soundex"
            | "substr"
            | "substring"
            | "trim"
            | "typeof"
            | "unicode"
            | "unlikely"
            | "upper"
            | "zeroblob"
            | "json"
            | "json_array"
            | "json_array_length"
            | "json_extract"
            | "json_insert"
            | "json_object"
            | "json_patch"
            | "json_remove"
            | "json_replace"
            | "json_set"
            | "json_type"
            | "json_valid"
            | "json_quote"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InSet, PlaceholderType, SelectBody, SelectCore, SelectStatement, Span, TypeName};
    use fsqlite_types::glossary::ColumnIdx;

    /// Test helper: resolve any single-name column to idx 0, table.col to idx
    /// based on column name's first char.
    fn test_resolve(col: &ColumnRef) -> Option<ColumnIdx> {
        let idx = col
            .column
            .bytes()
            .next()
            .map(|b| u32::from(b) - u32::from(b'a'))?;
        Some(ColumnIdx::new(idx))
    }

    /// Accept everything as deterministic.
    fn accept_all(_: &str) -> bool {
        true
    }

    fn span() -> Span {
        Span::ZERO
    }

    fn lit_int(v: i64) -> Expr {
        Expr::Literal(Literal::Integer(v), span())
    }

    fn col(name: &str) -> Expr {
        Expr::Column(ColumnRef::bare(name), span())
    }

    fn empty_select() -> SelectStatement {
        SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Values(vec![]),
                compounds: vec![],
            },
            order_by: vec![],
            limit: None,
        }
    }

    // ── bd-2blq: test_expr_is_rebase_safe_rejects_subquery ──

    #[test]
    fn test_expr_is_rebase_safe_rejects_subquery() {
        // Scalar subquery.
        let scalar_sub = Expr::Subquery(Box::new(empty_select()), span());
        assert!(
            expr_is_rebase_safe(&scalar_sub, &accept_all, &test_resolve).is_none(),
            "scalar subquery must be rejected"
        );

        // EXISTS subquery.
        let exists = Expr::Exists {
            subquery: Box::new(empty_select()),
            not: false,
            span: span(),
        };
        assert!(
            expr_is_rebase_safe(&exists, &accept_all, &test_resolve).is_none(),
            "EXISTS subquery must be rejected"
        );

        // IN with subquery.
        let in_sub = Expr::In {
            expr: Box::new(col("a")),
            set: InSet::Subquery(Box::new(empty_select())),
            not: false,
            span: span(),
        };
        assert!(
            expr_is_rebase_safe(&in_sub, &accept_all, &test_resolve).is_none(),
            "IN (SELECT ...) must be rejected"
        );

        // IN with list is also rejected (In variant not in RebaseExpr).
        let in_list = Expr::In {
            expr: Box::new(col("a")),
            set: InSet::List(vec![lit_int(1)]),
            not: false,
            span: span(),
        };
        assert!(
            expr_is_rebase_safe(&in_list, &accept_all, &test_resolve).is_none(),
            "IN (list) must be rejected"
        );
    }

    // ── bd-2blq: test_expr_is_rebase_safe_rejects_nondeterministic ──

    #[test]
    fn test_expr_is_rebase_safe_rejects_nondeterministic() {
        // random() — always-blocked builtin.
        let random_call = Expr::FunctionCall {
            name: "random".to_owned(),
            args: FunctionArgs::List(vec![]),
            distinct: false,
            filter: None,
            over: None,
            span: span(),
        };
        assert!(
            expr_is_rebase_safe(&random_call, &accept_all, &test_resolve).is_none(),
            "random() must be rejected"
        );

        // last_insert_rowid() — always-blocked builtin.
        let lir = Expr::FunctionCall {
            name: "last_insert_rowid".to_owned(),
            args: FunctionArgs::List(vec![]),
            distinct: false,
            filter: None,
            over: None,
            span: span(),
        };
        assert!(
            expr_is_rebase_safe(&lir, &accept_all, &test_resolve).is_none(),
            "last_insert_rowid() must be rejected"
        );

        // UDF not in deterministic set.
        let udf = Expr::FunctionCall {
            name: "my_custom_func".to_owned(),
            args: FunctionArgs::List(vec![lit_int(1)]),
            distinct: false,
            filter: None,
            over: None,
            span: span(),
        };
        assert!(
            expr_is_rebase_safe(&udf, &|_| false, &test_resolve).is_none(),
            "UDF without SQLITE_DETERMINISTIC must be rejected"
        );

        // CURRENT_TIMESTAMP literal.
        let ct = Expr::Literal(Literal::CurrentTimestamp, span());
        assert!(
            expr_is_rebase_safe(&ct, &accept_all, &test_resolve).is_none(),
            "CURRENT_TIMESTAMP must be rejected"
        );

        // changes() — always-blocked builtin.
        let changes = Expr::FunctionCall {
            name: "changes".to_owned(),
            args: FunctionArgs::List(vec![]),
            distinct: false,
            filter: None,
            over: None,
            span: span(),
        };
        assert!(
            expr_is_rebase_safe(&changes, &accept_all, &test_resolve).is_none(),
            "changes() must be rejected"
        );

        // Window function (OVER clause).
        let window_fn = Expr::FunctionCall {
            name: "abs".to_owned(),
            args: FunctionArgs::List(vec![col("a")]),
            distinct: false,
            filter: None,
            over: Some(crate::WindowSpec {
                base_window: None,
                partition_by: vec![],
                order_by: vec![],
                frame: None,
            }),
            span: span(),
        };
        assert!(
            expr_is_rebase_safe(&window_fn, &accept_all, &test_resolve).is_none(),
            "window functions must be rejected"
        );

        // Bind parameter.
        let placeholder = Expr::Placeholder(PlaceholderType::Anonymous, span());
        assert!(
            expr_is_rebase_safe(&placeholder, &accept_all, &test_resolve).is_none(),
            "bind parameters must be rejected"
        );
    }

    // ── bd-2blq: test_expr_is_rebase_safe_accepts_pure_arithmetic ──

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_expr_is_rebase_safe_accepts_pure_arithmetic() {
        let det = sqlite_builtin_is_deterministic;

        // ColumnRef.
        let c = col("a");
        let r = expr_is_rebase_safe(&c, &det, &test_resolve);
        assert!(
            matches!(r, Some(RebaseExpr::ColumnRef(idx)) if idx.get() == 0),
            "ColumnRef should be accepted"
        );

        // Integer literal.
        let lit = lit_int(42);
        let r = expr_is_rebase_safe(&lit, &det, &test_resolve);
        assert!(
            matches!(r, Some(RebaseExpr::Literal(SqliteValue::Integer(42)))),
            "integer literal should be accepted"
        );

        // BinaryOp::Add.
        let add = Expr::BinaryOp {
            left: Box::new(col("a")),
            op: BinaryOp::Add,
            right: Box::new(lit_int(1)),
            span: span(),
        };
        let r = expr_is_rebase_safe(&add, &det, &test_resolve);
        assert!(
            matches!(
                r,
                Some(RebaseExpr::BinaryOp {
                    op: RebaseBinaryOp::Add,
                    ..
                })
            ),
            "a + 1 should be accepted"
        );

        // BinaryOp::Subtract.
        let sub = Expr::BinaryOp {
            left: Box::new(col("a")),
            op: BinaryOp::Subtract,
            right: Box::new(lit_int(1)),
            span: span(),
        };
        let r = expr_is_rebase_safe(&sub, &det, &test_resolve);
        assert!(
            matches!(
                r,
                Some(RebaseExpr::BinaryOp {
                    op: RebaseBinaryOp::Subtract,
                    ..
                })
            ),
            "a - 1 should be accepted"
        );

        // BinaryOp::Multiply.
        let mul = Expr::BinaryOp {
            left: Box::new(col("a")),
            op: BinaryOp::Multiply,
            right: Box::new(lit_int(2)),
            span: span(),
        };
        let r = expr_is_rebase_safe(&mul, &det, &test_resolve);
        assert!(
            matches!(
                r,
                Some(RebaseExpr::BinaryOp {
                    op: RebaseBinaryOp::Multiply,
                    ..
                })
            ),
            "a * 2 should be accepted"
        );

        // Deterministic FunctionCall (abs).
        let abs_call = Expr::FunctionCall {
            name: "abs".to_owned(),
            args: FunctionArgs::List(vec![col("a")]),
            distinct: false,
            filter: None,
            over: None,
            span: span(),
        };
        let r = expr_is_rebase_safe(&abs_call, &det, &test_resolve);
        assert!(
            matches!(r, Some(RebaseExpr::FunctionCall { ref name, .. }) if name == "abs"),
            "abs(a) should be accepted"
        );

        // Cast.
        let cast = Expr::Cast {
            expr: Box::new(col("a")),
            type_name: TypeName {
                name: "INTEGER".to_owned(),
                arg1: None,
                arg2: None,
            },
            span: span(),
        };
        let r = expr_is_rebase_safe(&cast, &det, &test_resolve);
        assert!(
            matches!(r, Some(RebaseExpr::Cast { ref type_name, .. }) if type_name == "INTEGER"),
            "CAST(a AS INTEGER) should be accepted"
        );

        // Coalesce.
        let coalesce = Expr::FunctionCall {
            name: "coalesce".to_owned(),
            args: FunctionArgs::List(vec![col("a"), lit_int(0)]),
            distinct: false,
            filter: None,
            over: None,
            span: span(),
        };
        let r = expr_is_rebase_safe(&coalesce, &det, &test_resolve);
        assert!(
            matches!(r, Some(RebaseExpr::Coalesce(ref args)) if args.len() == 2),
            "COALESCE(a, 0) should be accepted"
        );
    }

    // ── Additional coverage ──

    #[test]
    fn test_unary_negate_and_not() {
        let det = sqlite_builtin_is_deterministic;

        let neg = Expr::UnaryOp {
            op: UnaryOp::Negate,
            expr: Box::new(lit_int(5)),
            span: span(),
        };
        let r = expr_is_rebase_safe(&neg, &det, &test_resolve);
        assert!(matches!(
            r,
            Some(RebaseExpr::UnaryOp {
                op: RebaseUnaryOp::Negate,
                ..
            })
        ));

        // Unary plus passes through.
        let plus = Expr::UnaryOp {
            op: UnaryOp::Plus,
            expr: Box::new(lit_int(5)),
            span: span(),
        };
        let r = expr_is_rebase_safe(&plus, &det, &test_resolve);
        assert!(matches!(
            r,
            Some(RebaseExpr::Literal(SqliteValue::Integer(5)))
        ));
    }

    #[test]
    fn test_concat_maps_to_rebase_concat() {
        let concat = Expr::BinaryOp {
            left: Box::new(Expr::Literal(Literal::String("hello".to_owned()), span())),
            op: BinaryOp::Concat,
            right: Box::new(Expr::Literal(Literal::String(" world".to_owned()), span())),
            span: span(),
        };
        let r = expr_is_rebase_safe(&concat, &accept_all, &test_resolve);
        assert!(matches!(r, Some(RebaseExpr::Concat { .. })));
    }

    #[test]
    fn test_nullif_maps_to_rebase_nullif() {
        let nullif = Expr::FunctionCall {
            name: "nullif".to_owned(),
            args: FunctionArgs::List(vec![col("a"), lit_int(0)]),
            distinct: false,
            filter: None,
            over: None,
            span: span(),
        };
        let r = expr_is_rebase_safe(&nullif, &accept_all, &test_resolve);
        assert!(matches!(r, Some(RebaseExpr::NullIf { .. })));
    }

    #[test]
    fn test_case_expression() {
        let case = Expr::Case {
            operand: None,
            whens: vec![(lit_int(1), lit_int(10))],
            else_expr: Some(Box::new(lit_int(20))),
            span: span(),
        };
        let r = expr_is_rebase_safe(&case, &accept_all, &test_resolve);
        assert!(matches!(r, Some(RebaseExpr::Case { .. })));
    }

    #[test]
    fn test_comparison_operators_rejected() {
        let eq = Expr::BinaryOp {
            left: Box::new(col("a")),
            op: BinaryOp::Eq,
            right: Box::new(lit_int(1)),
            span: span(),
        };
        assert!(expr_is_rebase_safe(&eq, &accept_all, &test_resolve).is_none());

        let and = Expr::BinaryOp {
            left: Box::new(lit_int(1)),
            op: BinaryOp::And,
            right: Box::new(lit_int(0)),
            span: span(),
        };
        assert!(expr_is_rebase_safe(&and, &accept_all, &test_resolve).is_none());
    }

    #[test]
    fn test_count_star_rejected() {
        let count_star = Expr::FunctionCall {
            name: "count".to_owned(),
            args: FunctionArgs::Star,
            distinct: false,
            filter: None,
            over: None,
            span: span(),
        };
        assert!(expr_is_rebase_safe(&count_star, &accept_all, &test_resolve).is_none());
    }

    #[test]
    fn test_distinct_function_rejected() {
        let distinct_sum = Expr::FunctionCall {
            name: "abs".to_owned(),
            args: FunctionArgs::List(vec![col("a")]),
            distinct: true,
            filter: None,
            over: None,
            span: span(),
        };
        assert!(expr_is_rebase_safe(&distinct_sum, &accept_all, &test_resolve).is_none());
    }

    #[test]
    fn test_literal_bool_and_blob() {
        let t = Expr::Literal(Literal::True, span());
        let r = expr_is_rebase_safe(&t, &accept_all, &test_resolve);
        assert!(matches!(
            r,
            Some(RebaseExpr::Literal(SqliteValue::Integer(1)))
        ));

        let f = Expr::Literal(Literal::False, span());
        let r = expr_is_rebase_safe(&f, &accept_all, &test_resolve);
        assert!(matches!(
            r,
            Some(RebaseExpr::Literal(SqliteValue::Integer(0)))
        ));

        let blob = Expr::Literal(Literal::Blob(vec![0xDE, 0xAD]), span());
        let r = expr_is_rebase_safe(&blob, &accept_all, &test_resolve);
        assert!(matches!(r, Some(RebaseExpr::Literal(SqliteValue::Blob(_)))));
    }

    #[test]
    fn test_nested_expression_tree() {
        // abs(a + 1) * COALESCE(b, 0)
        let inner_add = Expr::BinaryOp {
            left: Box::new(col("a")),
            op: BinaryOp::Add,
            right: Box::new(lit_int(1)),
            span: span(),
        };
        let abs_call = Expr::FunctionCall {
            name: "abs".to_owned(),
            args: FunctionArgs::List(vec![inner_add]),
            distinct: false,
            filter: None,
            over: None,
            span: span(),
        };
        let coalesce = Expr::FunctionCall {
            name: "coalesce".to_owned(),
            args: FunctionArgs::List(vec![col("b"), lit_int(0)]),
            distinct: false,
            filter: None,
            over: None,
            span: span(),
        };
        let product = Expr::BinaryOp {
            left: Box::new(abs_call),
            op: BinaryOp::Multiply,
            right: Box::new(coalesce),
            span: span(),
        };

        let det = sqlite_builtin_is_deterministic;
        let r = expr_is_rebase_safe(&product, &det, &test_resolve);
        assert!(matches!(
            r,
            Some(RebaseExpr::BinaryOp {
                op: RebaseBinaryOp::Multiply,
                ..
            })
        ));
    }

    #[test]
    fn test_nondeterministic_nested_in_safe_tree_poisons() {
        // abs(random()) — should be rejected because random() is inside.
        let random_call = Expr::FunctionCall {
            name: "random".to_owned(),
            args: FunctionArgs::List(vec![]),
            distinct: false,
            filter: None,
            over: None,
            span: span(),
        };
        let abs_call = Expr::FunctionCall {
            name: "abs".to_owned(),
            args: FunctionArgs::List(vec![random_call]),
            distinct: false,
            filter: None,
            over: None,
            span: span(),
        };
        assert!(expr_is_rebase_safe(&abs_call, &accept_all, &test_resolve).is_none());
    }

    #[test]
    fn test_sqlite_builtin_is_deterministic_coverage() {
        assert!(sqlite_builtin_is_deterministic("abs"));
        assert!(sqlite_builtin_is_deterministic("length"));
        assert!(sqlite_builtin_is_deterministic("lower"));
        assert!(sqlite_builtin_is_deterministic("upper"));
        assert!(sqlite_builtin_is_deterministic("trim"));
        assert!(sqlite_builtin_is_deterministic("coalesce"));
        assert!(sqlite_builtin_is_deterministic("nullif"));
        assert!(sqlite_builtin_is_deterministic("json_extract"));

        assert!(!sqlite_builtin_is_deterministic("random"));
        assert!(!sqlite_builtin_is_deterministic("last_insert_rowid"));
        assert!(!sqlite_builtin_is_deterministic("unknown_func"));
    }
}
