#![recursion_limit = "512"]

//! bd-rl7i9: row-value comparison tails in the dynamic (FROM-less-subquery)
//! evaluation lane, differential vs rusqlite (bundled SQLite).
//!
//! * **(c) DONE**: `IS` / `IS NOT` over row values (NULL-safe element-wise
//!   equality), e.g. `(SELECT 1,2) IS (1,2)`.
//! * **(a) DONE**: per-position collation. Since bd-9sp6f this follows the
//!   bundled 3.54.1: a subquery operand's fields carry their affinity but no
//!   collation; the other operand's explicit `COLLATE` still applies.
//! * **(b) DONE**: a `RowValue` whose *element* is a subquery, e.g.
//!   `(1,(SELECT 2)) = (1,2)`, falls through to the scalar arm because the
//!   dispatch only routes a *top-level* subquery operand.

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
async fn fq(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query(sql).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(e) => vec![vec![format!("<ERR {e:?}>")]],
    }
}
fn rq(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = match r.prepare(sql) {
        Ok(st) => st,
        Err(e) => return vec![vec![format!("<ERR {e}>")]],
    };
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}
async fn agree(setup: &[&str], query: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = f.execute(s).await;
        let _ = r.execute_batch(s);
    }
    let fr = fq(&f, query).await;
    let rr = rq(&r, query);
    assert_eq!(fr, rr, "{msg}\n  frank={fr:?}\n  stock={rr:?}");
}

// ---- (c) IS / IS NOT over row values (implemented) ----

#[test]
fn row_value_is_subquery_vs_literal() {
    asupersync::test_utils::run_test(|| async {
        agree(&[], "SELECT (SELECT 1,2) IS (1,2)", "(c) IS: equal rows").await;
    });
}

#[test]
fn row_value_is_not_subquery_vs_literal() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[],
            "SELECT (SELECT 1,2) IS NOT (1,3)",
            "(c) IS NOT: unequal rows",
        )
        .await;
    });
}

#[test]
fn row_value_is_null_safe() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[],
            "SELECT (SELECT NULL,2) IS (NULL,2)",
            "(c) IS: NULL-safe equality",
        )
        .await;
        agree(
            &[],
            "SELECT (SELECT NULL,2) IS (NULL,3)",
            "(c) IS: NULL-safe with an unequal tail",
        )
        .await;
    });
}

// ---- (a) per-position collation ----
//
// bd-9sp6f: SQLite 3.54 compares the fields of a subquery operand without
// their own collation (the declared column collation and a `COLLATE` in the
// subquery's select list are both ignored), for a row value as for a scalar;
// 3.46 used it for a row value. The other operand's explicit collation still
// applies, the subquery's affinity still applies, and `IN` still uses the
// left subquery's collation. All run on the bundled 3.54.1.

const NOCASE_T: &[&str] = &[
    "CREATE TABLE t(a TEXT COLLATE NOCASE, n INTEGER)",
    "INSERT INTO t VALUES('A', 5)",
];

#[test]
fn row_value_element_collation_nocase() {
    asupersync::test_utils::run_test(|| async {
        agree(
            NOCASE_T,
            "SELECT (SELECT a,1 FROM t)=('a',1)",
            "(a) a subquery field's declared NOCASE does not drive the comparison",
        )
        .await;
        agree(
            NOCASE_T,
            "SELECT ('a',1)=(SELECT a,1 FROM t)",
            "(a) nor on the right-hand side",
        )
        .await;
        agree(
            NOCASE_T,
            "SELECT (SELECT a COLLATE NOCASE,1 FROM t)=('a',1)",
            "(a) nor a COLLATE inside the subquery",
        )
        .await;
    });
}

/// The negatives a naive "drop every collation" or "drop the metadata" change
/// would get wrong.
#[test]
fn row_value_subquery_keeps_affinity_and_outer_collation() {
    asupersync::test_utils::run_test(|| async {
        agree(
            NOCASE_T,
            "SELECT (SELECT a,1 FROM t)=('a' COLLATE NOCASE,1)",
            "(a) the other operand's explicit COLLATE still applies",
        )
        .await;
        agree(
            NOCASE_T,
            "SELECT (SELECT n,1 FROM t)=('5',1)",
            "(a) the subquery field's INTEGER affinity still applies",
        )
        .await;
        agree(
            NOCASE_T,
            "SELECT (SELECT a,1 FROM t) IN (SELECT 'a',1)",
            "(a) IN still uses the left subquery's collation",
        )
        .await;
    });
}

// ---- (b) RowValue with a subquery element (implemented) ----

#[test]
fn row_value_with_subquery_element() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[],
            "SELECT (1,(SELECT 2)) = (1,2)",
            "(b) RowValue with a subquery element",
        )
        .await;
    });
}
