#![recursion_limit = "512"]

//! bd-and-or-short-circuit-value-jump-gaps-dkswh: the FROM-less WHERE evaluator
//! was the last surviving AND/OR short-circuit gap of the dkswh umbrella. After
//! the Tier-1 fold and the bd-lryih landings closed the table-backed WHERE path,
//! CASE-WHEN, and the value-position semantics, a FROM-less (expression-only)
//! `SELECT ... WHERE <boolean skeleton>` still evaluated its predicate eagerly
//! via `eval_expr_with_subqueries` instead of the short-circuiting truth-context
//! evaluator. So `SELECT 1 WHERE 1 OR (SELECT ... FROM json_each('bare'))`
//! surfaced the dead arm's error where stock SQLite short-circuits to a row.
//!
//! The fix routes the FROM-less WHERE predicate through `eval_expr_truthiness`
//! (the same evaluator the table-backed WHERE path uses), which stops an OR
//! chain at the first satisfying operand and an AND chain at the first falsy
//! one, evaluating operands strictly left-to-right and preserving the
//! polarity-dependent NULL rule.
//!
//! All assertions are differential vs rusqlite (bundled SQLite); the boolean
//! short-circuit semantics exercised here are version-stable.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

/// Both engines return the same rows for `sql`.
async fn assert_rows_agree(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let fr: Vec<Vec<String>> = f
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect();
    let mut st = r
        .prepare(sql)
        .unwrap_or_else(|e| panic!("rusqlite prepare `{sql}`: {e:?}"));
    let n = st.column_count();
    let rr: Vec<Vec<String>> = st
        .query_map([], |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect())
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(fr, rr, "row divergence on `{sql}`");
}

/// Both engines REJECT `sql` (the dead arm is reached in both).
async fn assert_both_reject(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let fe = f
        .query(sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("frank accepted `{sql}` but must reject"));
    let msg = format!("{fe:?}").to_ascii_lowercase();
    assert!(
        msg.contains("json"),
        "frank `{sql}`: expected a malformed-JSON error, got {fe:?}"
    );
    // rusqlite errors while stepping (the subquery is evaluated), not at prepare.
    let stepped = r
        .prepare(sql)
        .and_then(|mut st| st.query([]).and_then(|mut rows| rows.next().map(|_| ())));
    assert!(stepped.is_err(), "rusqlite must also reject `{sql}`");
}

/// The fix: a FROM-less `WHERE <truthy> OR <erroring>` short-circuits to a row
/// and never evaluates the dead, erroring right arm.
#[test]
fn fromless_where_or_short_circuits_past_erroring_arm_dkswh() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        // `json_each('bare')` errors if ever evaluated; the OR short-circuit
        // must prevent it. Left arm literal, comparison, and subquery forms all
        // route through the same FROM-less predicate evaluator.
        assert_rows_agree(
            &f,
            &r,
            "SELECT 1 WHERE 1 OR (SELECT count(*) FROM json_each('bare'))",
        )
        .await;
        assert_rows_agree(
            &f,
            &r,
            "SELECT 1 WHERE (1=1) OR (SELECT count(*) FROM json_each('bare'))",
        )
        .await;
        assert_rows_agree(
            &f,
            &r,
            "SELECT 1 WHERE (SELECT 1) OR (SELECT count(*) FROM json_each('bare'))",
        )
        .await;
        // A chain of erroring right arms is all skipped once the head is TRUE.
        assert_rows_agree(
            &f,
            &r,
            "SELECT 1 WHERE 1 OR (SELECT count(*) FROM json_each('bare')) \
             OR (SELECT count(*) FROM json_each('x'))",
        )
        .await;
    });
}

/// Guardrail: the fix must NOT over-short-circuit. An erroring arm reached
/// BEFORE any satisfying operand (left-to-right) must still error in both
/// engines, and `0 AND <erroring>` must still fold to no rows.
#[test]
fn fromless_where_preserves_left_to_right_and_and_fold_dkswh() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        // Erroring arm first with an OR *identity* (`0`) right arm: `X OR 0` = X,
        // so stock does NOT fold — it evaluates X (which errors), and so does
        // frank. (An OR *absorbing* right arm `OR 1` is a separate residual, see
        // the ignored keeper below.)
        assert_both_reject(
            &f,
            &r,
            "SELECT 1 WHERE (SELECT count(*) FROM json_each('bare')) OR 0",
        )
        .await;
        // `0 AND E` still short-circuits to FALSE -> no row (never touches E).
        assert_rows_agree(
            &f,
            &r,
            "SELECT 1 WHERE 0 AND (SELECT count(*) FROM json_each('bare'))",
        )
        .await;
        // `1 AND E` genuinely needs E, so both evaluate and error.
        assert_both_reject(
            &f,
            &r,
            "SELECT 1 WHERE 1 AND (SELECT count(*) FROM json_each('bare'))",
        )
        .await;
    });
}

/// Guardrail: NULL polarity and plain boolean outcomes are unchanged for the
/// FROM-less WHERE path (no regression of the bd-lryih NULL rules).
#[test]
fn fromless_where_null_polarity_and_plain_booleans_dkswh() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        assert_rows_agree(&f, &r, "SELECT 1 WHERE 1 OR 0").await;
        assert_rows_agree(&f, &r, "SELECT 1 WHERE 0 OR 1").await;
        assert_rows_agree(&f, &r, "SELECT 1 WHERE 0 OR 0").await;
        assert_rows_agree(&f, &r, "SELECT 1 WHERE 1 AND 1").await;
        assert_rows_agree(&f, &r, "SELECT 1 WHERE 1 AND 0").await;
        // NULL is absorbing only when the chain can no longer reach the sought
        // outcome: `NULL OR 1` is TRUE (row), `NULL OR 0` is NULL (no row).
        assert_rows_agree(&f, &r, "SELECT 1 WHERE NULL OR 1").await;
        assert_rows_agree(&f, &r, "SELECT 1 WHERE NULL OR 0").await;
        assert_rows_agree(&f, &r, "SELECT 1 WHERE 1 AND NULL").await;
        assert_rows_agree(&f, &r, "SELECT 1 WHERE 0 AND NULL").await;
        assert_rows_agree(&f, &r, "SELECT 1 WHERE NOT (0 OR 0)").await;
    });
}

/// RESIDUAL (tracked, currently RED so `#[ignore]`d): the compile-time
/// absorbing-constant fold `X OR <const-true>` -> TRUE and `X AND <const-false>`
/// -> FALSE discards the OTHER operand even when it would error, regardless of
/// operand position. Stock folds `(erroring) OR 1` to a row and `(erroring) AND
/// 0` to no row; frank still evaluates the erroring operand left-to-right and
/// surfaces its error. This is a distinct gap from the left-truthy short-circuit
/// fixed above, and it affects the table-backed WHERE path too, so it is filed
/// separately. Un-ignore when the absorbing-constant fold lands.
#[test]
#[ignore = "bd-0ivvf residual: absorbing-constant fold (X OR TRUE / X AND FALSE) not yet implemented"]
fn fromless_where_absorbing_constant_fold_residual_dkswh() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        // `X OR 1` absorbs to TRUE without evaluating X.
        assert_rows_agree(
            &f,
            &r,
            "SELECT 1 WHERE (SELECT count(*) FROM json_each('bare')) OR 1",
        )
        .await;
        // `X AND 0` absorbs to FALSE without evaluating X.
        assert_rows_agree(
            &f,
            &r,
            "SELECT 1 WHERE (SELECT count(*) FROM json_each('bare')) AND 0",
        )
        .await;
    });
}
