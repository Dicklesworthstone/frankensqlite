//! bd-7zeum: compound SELECTs and scalar/EXISTS subqueries must match stock
//! SQLite 3.54.1 evaluation bounds:
//! - LIMIT / OFFSET are computed before any compound arm runs; LIMIT 0 runs no arm.
//! - UNION ALL without ORDER BY runs arm by arm and stops once LIMIT + OFFSET rows exist.
//! - UNION with literal integer LIMIT 1 (through unary +/-), no OFFSET, and no ORDER BY
//!   runs as UNION ALL (`unionWithLimitOne`), yielding the leftmost arm's first row without
//!   sorting/deduping or running later arms.
//! - Scalar and EXISTS subqueries rewrite any non-0/1 LIMIT X to `X <> CAST(0 AS NUMERIC)`.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use tempfile::tempdir;

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".into(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("x{b:02x?}"),
    }
}

fn render_stock(value: &rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".into(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("x{b:02x?}"),
    }
}

fn stock_outcome(
    conn: &rusqlite::Connection,
    sql: &str,
    params: &[rusqlite::types::Value],
) -> String {
    let result = (|| -> rusqlite::Result<Vec<Vec<String>>> {
        let mut stmt = conn.prepare(sql)?;
        let cols = stmt.column_count();
        let mut rows = Vec::new();
        let mut cursor = stmt.query(rusqlite::params_from_iter(params.iter()))?;
        while let Some(row) = cursor.next()? {
            rows.push(
                (0..cols)
                    .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                    .collect(),
            );
        }
        Ok(rows)
    })();
    match result {
        Ok(rows) => format!("rows={rows:?}"),
        Err(
            rusqlite::Error::SqliteFailure(_, Some(msg)) | rusqlite::Error::SqlInputError { msg, .. },
        ) => format!("error: {msg}"),
        Err(err) => format!("error: {err}"),
    }
}

async fn fsqlite_outcome(
    conn: &Connection,
    sql: &str,
    params: &[SqliteValue],
    prepared: bool,
) -> String {
    let result = if prepared {
        match conn.prepare(sql).await {
            Ok(stmt) => {
                if params.is_empty() {
                    stmt.query().await
                } else {
                    stmt.query_with_params(params).await
                }
            }
            Err(err) => Err(err),
        }
    } else if params.is_empty() {
        conn.query(sql).await
    } else {
        conn.query_with_params(sql, params).await
    };
    match result {
        Ok(rows) => {
            let rendered: Vec<Vec<String>> = rows
                .iter()
                .map(|r| r.values().iter().map(render_fsqlite).collect())
                .collect();
            format!("rows={rendered:?}")
        }
        Err(err) => format!("error: {err}"),
    }
}

const SETUP: &[&str] = &[
    "CREATE TABLE s(k INTEGER, v INTEGER)",
    "INSERT INTO s VALUES (1, 10), (2, 80), (3, 30)",
    "CREATE TABLE o(id INTEGER PRIMARY KEY, k INTEGER)",
    "INSERT INTO o VALUES (1, 2), (2, 99)",
];

const NO_PARAM_PROBES: &[(&str, &str)] = &[
    // Original bd-7zeum reproducer + LIMIT 1 + EXISTS.
    (
        "SELECT (SELECT v FROM s WHERE s.k = 2 UNION ALL SELECT abs(-9223372036854775808))",
        "rows=[[\"80\"]]",
    ),
    (
        "SELECT (SELECT v FROM s WHERE s.k = 2 UNION ALL SELECT abs(-9223372036854775808) LIMIT 1)",
        "rows=[[\"80\"]]",
    ),
    (
        "SELECT EXISTS(SELECT v FROM s WHERE s.k = 2 UNION ALL SELECT abs(-9223372036854775808))",
        "rows=[[\"1\"]]",
    ),
    // Top-level UNION ALL stops once LIMIT + OFFSET rows are produced.
    (
        "SELECT v FROM s WHERE k = 2 UNION ALL SELECT abs(-9223372036854775808) LIMIT 1",
        "rows=[[\"80\"]]",
    ),
    (
        "SELECT v FROM s WHERE k IN (1, 2) UNION ALL SELECT abs(-9223372036854775808) LIMIT 1 OFFSET 1",
        "rows=[[\"80\"]]",
    ),
    (
        "SELECT v FROM s WHERE k = 2 UNION ALL SELECT 90 UNION ALL SELECT abs(-9223372036854775808) LIMIT 2",
        "rows=[[\"80\"], [\"90\"]]",
    ),
    // LIMIT 0 runs no arm for any compound operator.
    (
        "SELECT 1 UNION ALL SELECT abs(-9223372036854775808) LIMIT 0",
        "rows=[]",
    ),
    (
        "SELECT 1 UNION SELECT abs(-9223372036854775808) LIMIT 0",
        "rows=[]",
    ),
    (
        "SELECT 1 EXCEPT SELECT abs(-9223372036854775808) LIMIT 0",
        "rows=[]",
    ),
    (
        "SELECT 1 INTERSECT SELECT abs(-9223372036854775808) LIMIT 0",
        "rows=[]",
    ),
    (
        "SELECT 1 UNION ALL SELECT abs(-9223372036854775808) ORDER BY 1 LIMIT 0",
        "rows=[]",
    ),
    // unionWithLimitOne: UNION with literal integer 1, no OFFSET, no ORDER BY.
    ("SELECT 5 UNION SELECT 3 LIMIT 1", "rows=[[\"5\"]]"),
    ("SELECT (SELECT 5 UNION SELECT 3)", "rows=[[\"5\"]]"),
    ("SELECT 5 UNION SELECT 3 LIMIT +1", "rows=[[\"5\"]]"),
    ("SELECT 5 UNION SELECT 3 LIMIT -(-1)", "rows=[[\"5\"]]"),
    (
        "SELECT 5 UNION SELECT 3 UNION SELECT 4 LIMIT 1",
        "rows=[[\"5\"]]",
    ),
    (
        "SELECT v FROM s INTERSECT SELECT 80 UNION SELECT 1 LIMIT 1",
        "rows=[[\"80\"]]",
    ),
    (
        "SELECT 5 UNION SELECT abs(-9223372036854775808) LIMIT 1",
        "rows=[[\"5\"]]",
    ),
    (
        "SELECT (SELECT 5 UNION SELECT abs(-9223372036854775808))",
        "rows=[[\"5\"]]",
    ),
    // Negative controls: must merge/dedup/sort or evaluate later arm.
    ("SELECT 5 UNION SELECT 3 LIMIT 2 - 1", "rows=[[\"3\"]]"),
    ("SELECT 5 UNION SELECT 3 LIMIT 1 OFFSET 0", "rows=[[\"3\"]]"),
    ("SELECT 5 UNION SELECT 3 ORDER BY 1 LIMIT 1", "rows=[[\"3\"]]"),
    ("SELECT 5 UNION SELECT 3", "rows=[[\"3\"], [\"5\"]]"),
    (
        "SELECT 5 UNION SELECT 3 EXCEPT SELECT 5 LIMIT 1",
        "rows=[[\"3\"]]",
    ),
    (
        "SELECT 5 UNION SELECT 3 INTERSECT SELECT 3 LIMIT 1",
        "rows=[[\"3\"]]",
    ),
    (
        "SELECT 5 UNION SELECT abs(-9223372036854775808) LIMIT 2",
        "error: integer overflow",
    ),
    (
        "SELECT 5 UNION ALL SELECT abs(-9223372036854775808) ORDER BY 1 LIMIT 1",
        "error: integer overflow",
    ),
    (
        "SELECT 5 UNION SELECT abs(-9223372036854775808) ORDER BY 1 LIMIT 1",
        "error: integer overflow",
    ),
    (
        "SELECT 5 UNION SELECT abs(-9223372036854775808) LIMIT 1 OFFSET 0",
        "error: integer overflow",
    ),
    // Scalar / EXISTS subquery LIMIT X -> X <> 0 (numeric affinity).
    ("SELECT (SELECT 7 LIMIT 5)", "rows=[[\"7\"]]"),
    ("SELECT (SELECT 7 LIMIT 2.5)", "rows=[[\"7\"]]"),
    ("SELECT (SELECT 7 LIMIT 'abc')", "rows=[[\"7\"]]"),
    ("SELECT (SELECT 7 LIMIT '0abc')", "rows=[[\"7\"]]"),
    ("SELECT (SELECT 7 LIMIT '0')", "rows=[[\"NULL\"]]"),
    ("SELECT (SELECT 7 LIMIT '0.0')", "rows=[[\"NULL\"]]"),
    ("SELECT (SELECT 7 LIMIT ' 0 ')", "rows=[[\"NULL\"]]"),
    ("SELECT (SELECT 7 LIMIT 0.0)", "rows=[[\"NULL\"]]"),
    ("SELECT (SELECT 7 LIMIT 0.5)", "rows=[[\"7\"]]"),
    ("SELECT (SELECT 7 LIMIT -1)", "rows=[[\"7\"]]"),
    ("SELECT (SELECT 7 LIMIT -0)", "rows=[[\"NULL\"]]"),
    ("SELECT (SELECT 7 LIMIT NULL)", "error: datatype mismatch"),
    ("SELECT (SELECT 5 UNION SELECT 3 LIMIT 5)", "rows=[[\"3\"]]"),
    (
        "SELECT (SELECT 5 UNION SELECT 3 LIMIT 1 OFFSET 0)",
        "rows=[[\"3\"]]",
    ),
    ("SELECT EXISTS(SELECT 7 LIMIT 2.5)", "rows=[[\"1\"]]"),
    ("SELECT EXISTS(SELECT 7 LIMIT 'abc')", "rows=[[\"1\"]]"),
    ("SELECT EXISTS(SELECT 7 LIMIT '0')", "rows=[[\"0\"]]"),
    (
        "SELECT EXISTS(SELECT 7 LIMIT NULL)",
        "error: datatype mismatch",
    ),
    // Correlated scalar subqueries over table o with LIMIT 2.5 / '0' / 'abc' / -1 / compound.
    (
        "SELECT id, (SELECT v FROM s WHERE s.k = o.k LIMIT 2.5) FROM o ORDER BY id",
        "rows=[[\"1\", \"80\"], [\"2\", \"NULL\"]]",
    ),
    (
        "SELECT id, (SELECT v FROM s WHERE s.k = o.k LIMIT 'abc') FROM o ORDER BY id",
        "rows=[[\"1\", \"80\"], [\"2\", \"NULL\"]]",
    ),
    (
        "SELECT id, (SELECT v FROM s WHERE s.k = o.k LIMIT '0') FROM o ORDER BY id",
        "rows=[[\"1\", \"NULL\"], [\"2\", \"NULL\"]]",
    ),
    (
        "SELECT id, (SELECT v FROM s WHERE s.k = o.k LIMIT -1) FROM o ORDER BY id",
        "rows=[[\"1\", \"80\"], [\"2\", \"NULL\"]]",
    ),
    (
        "SELECT id, (SELECT v FROM s WHERE s.k = o.k ORDER BY v LIMIT 2.5) FROM o ORDER BY id",
        "rows=[[\"1\", \"80\"], [\"2\", \"NULL\"]]",
    ),
    (
        "SELECT id, (SELECT v FROM s WHERE s.k = o.k ORDER BY v LIMIT -1) FROM o ORDER BY id",
        "rows=[[\"1\", \"80\"], [\"2\", \"NULL\"]]",
    ),
    (
        "SELECT id, (SELECT v FROM s WHERE s.k = o.k UNION ALL SELECT abs(-9223372036854775808)) FROM o WHERE id = 1",
        "rows=[[\"1\", \"80\"]]",
    ),
];

#[test]
fn compound_and_subquery_limits_match_stock_3_54_1() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let _dir = if file_backed {
                    Some(tempdir().expect("tempdir"))
                } else {
                    None
                };
                let (stock, fsql) = if let Some(dir) = &_dir {
                    let stock_path = dir.path().join("stock.db");
                    let fsql_path = dir.path().join("fsql.db");
                    (
                        rusqlite::Connection::open(&stock_path).expect("open stock file"),
                        Connection::open(fsql_path.to_str().expect("utf8 path"))
                            .await
                            .expect("open fsql file"),
                    )
                } else {
                    (
                        rusqlite::Connection::open_in_memory().expect("open stock mem"),
                        Connection::open(":memory:").await.expect("open fsql mem"),
                    )
                };

                for &sql in SETUP {
                    stock.execute(sql, []).expect("stock setup");
                    fsql.execute(sql).await.expect("fsql setup");
                }

                for &(sql, pinned_stock) in NO_PARAM_PROBES {
                    let stock_res = stock_outcome(&stock, sql, &[]);
                    assert_eq!(
                        stock_res, pinned_stock,
                        "stock 3.54.1 drift on `{sql}` (file_backed={file_backed})"
                    );
                    let fsql_res = fsqlite_outcome(&fsql, sql, &[], prepared).await;
                    assert_eq!(
                        fsql_res, stock_res,
                        "mismatch on `{sql}` (file_backed={file_backed} prepared={prepared})"
                    );
                }

                // Parameterized LIMIT 1 on UNION must NOT trigger unionWithLimitOne.
                let param_sql = "SELECT 5 UNION SELECT 3 LIMIT ?1";
                let stock_param =
                    stock_outcome(&stock, param_sql, &[rusqlite::types::Value::Integer(1)]);
                assert_eq!(stock_param, "rows=[[\"3\"]]");
                let fsql_param =
                    fsqlite_outcome(&fsql, param_sql, &[SqliteValue::Integer(1)], prepared).await;
                assert_eq!(
                    fsql_param, stock_param,
                    "param UNION LIMIT ?1 mismatch (file_backed={file_backed} prepared={prepared})"
                );

                // Parameterized LIMIT 0 on UNION skips overflowing arm.
                let param_zero_sql = "SELECT 5 UNION SELECT abs(-9223372036854775808) LIMIT ?1";
                let stock_zero =
                    stock_outcome(&stock, param_zero_sql, &[rusqlite::types::Value::Integer(0)]);
                assert_eq!(stock_zero, "rows=[]");
                let fsql_zero =
                    fsqlite_outcome(&fsql, param_zero_sql, &[SqliteValue::Integer(0)], prepared)
                        .await;
                assert_eq!(
                    fsql_zero, stock_zero,
                    "param UNION LIMIT 0 mismatch (file_backed={file_backed} prepared={prepared})"
                );

                // Parameterized LIMIT 1 on UNION ALL skips overflowing arm.
                let param_ua_sql = "SELECT 5 UNION ALL SELECT abs(-9223372036854775808) LIMIT ?1";
                let stock_ua =
                    stock_outcome(&stock, param_ua_sql, &[rusqlite::types::Value::Integer(1)]);
                assert_eq!(stock_ua, "rows=[[\"5\"]]");
                let fsql_ua =
                    fsqlite_outcome(&fsql, param_ua_sql, &[SqliteValue::Integer(1)], prepared)
                        .await;
                assert_eq!(
                    fsql_ua, stock_ua,
                    "param UNION ALL LIMIT 1 mismatch (file_backed={file_backed} prepared={prepared})"
                );

                fsql.close().await.expect("close fsql");
            }
        }
    });
}
