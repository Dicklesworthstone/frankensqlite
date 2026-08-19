#![recursion_limit = "512"]

//! bd-rl7i9: row-value comparison tails in the dynamic (FROM-less-subquery)
//! evaluation lane, differential vs rusqlite (bundled SQLite).
//!
//! * **(c) DONE**: `IS` / `IS NOT` over row values (NULL-safe element-wise
//!   equality), e.g. `(SELECT 1,2) IS (1,2)`.
//! * **(a) DONE**: per-position collation (declared column collation + an
//!   explicit `COLLATE` wrapper) is dropped — the comparator's no-context
//!   fallback (`compare_join_expr_values`) compares BINARY.
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

// ---- (a) per-position collation (implemented) ----

#[test]
fn row_value_element_collation_nocase() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(a TEXT COLLATE NOCASE)",
                "INSERT INTO t VALUES('A')",
            ],
            "SELECT (SELECT a,1 FROM t)=('a',1)",
            "(a) declared NOCASE must drive the row-value element comparison",
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
