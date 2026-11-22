//! bd-x9br8: an unresolvable column, table, or function inside a subquery of a
//! DML statement (UPDATE SET / WHERE / RETURNING, INSERT VALUES / ON CONFLICT
//! DO UPDATE SET & WHERE / RETURNING, DELETE WHERE / RETURNING, plus SELECT
//! list & WHERE) must fail at prepare with stock SQLite 3.54.1's exact error
//! text and write nothing.
//!
//! Differential oracle against live bundled rusqlite (SQLite 3.54.1), across
//! `:memory:` and file-backed databases, both direct (`query`) and prepared
//! (`prepare`).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

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

fn stock_outcome(conn: &rusqlite::Connection, sql: &str) -> String {
    let result = (|| -> rusqlite::Result<Vec<Vec<String>>> {
        let mut statement = conn.prepare(sql)?;
        let columns = statement.column_count();
        let mut rows = Vec::new();
        let mut cursor = statement.query([])?;
        while let Some(row) = cursor.next()? {
            rows.push(
                (0..columns)
                    .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                    .collect(),
            );
        }
        Ok(rows)
    })();
    match result {
        Ok(rows) => format!("rows={rows:?}"),
        Err(
            rusqlite::Error::SqliteFailure(_, Some(message))
            | rusqlite::Error::SqlInputError { msg: message, .. },
        ) => format!("error: {message}"),
        Err(error) => format!("error: {error}"),
    }
}

async fn fsqlite_outcome(conn: &Connection, sql: &str, prepared: bool) -> String {
    let result = if prepared {
        match conn.prepare(sql).await {
            Ok(statement)
                if sql.trim_start().starts_with("SELECT") || sql.contains("RETURNING") =>
            {
                statement.query().await
            }
            Ok(statement) => statement.execute().await.map(|_| Vec::new()),
            Err(error) => Err(error),
        }
    } else {
        conn.query(sql).await
    };
    match result {
        Ok(rows) => {
            let rows: Vec<Vec<String>> = rows
                .iter()
                .map(|row| row.values().iter().map(render_fsqlite).collect())
                .collect();
            format!("rows={rows:?}")
        }
        Err(error) => format!("error: {error}"),
    }
}

const SETUP: &[&str] = &[
    "CREATE TABLE t(n)",
    "INSERT INTO t VALUES (1)",
    "CREATE TABLE s(p, v)",
    "INSERT INTO s VALUES (1, 10)",
    "CREATE TABLE empty_s(p)",
    "CREATE TABLE m(a, b)",
    "INSERT INTO m VALUES (1, 2)",
    "CREATE TABLE u(k UNIQUE, v)",
    "INSERT INTO u VALUES (1, 5)",
];

const NO_COL_NOPE: &str = "error: no such column: nope";
const NO_COL_T_NOPE: &str = "error: no such column: t.nope";
const NO_COL_T_N: &str = "error: no such column: t.n";
const NO_COL_EXCLUDED_NOPE: &str = "error: no such column: excluded.nope";
const NO_TABLE_MISSING: &str = "error: no such table: missing_s";
const NO_FUNC_NOFUNC: &str = "error: no such function: nofunc";

const REJECTED_STATEMENTS: &[(&str, &str)] = &[
    // Exact repro from bd-x9br8 (uncorrelated and correlated UPDATE SET).
    ("UPDATE t SET n = (SELECT nope FROM s)", NO_COL_NOPE),
    (
        "UPDATE t SET n = (SELECT nope FROM s WHERE s.p = t.n)",
        NO_COL_NOPE,
    ),
    (
        "UPDATE t SET n = (SELECT p FROM s WHERE s.p = t.nope)",
        NO_COL_T_NOPE,
    ),
    (
        "UPDATE t SET n = (SELECT p FROM missing_s)",
        NO_TABLE_MISSING,
    ),
    (
        "UPDATE t SET n = (SELECT nofunc(p) FROM s)",
        NO_FUNC_NOFUNC,
    ),
    (
        "UPDATE t SET n = (SELECT nofunc(nope) FROM s)",
        NO_COL_NOPE,
    ),
    (
        "UPDATE m SET (a, b) = (SELECT nope, p FROM s)",
        NO_COL_NOPE,
    ),
    // UPDATE WHERE and RETURNING subqueries.
    ("UPDATE t SET n = 99 WHERE (SELECT nope FROM s)", NO_COL_NOPE),
    (
        "UPDATE t SET n = 99 WHERE EXISTS (SELECT nope FROM s)",
        NO_COL_NOPE,
    ),
    (
        "UPDATE t SET n = 99 WHERE n IN (SELECT nope FROM s)",
        NO_COL_NOPE,
    ),
    (
        "UPDATE t SET n = 99 WHERE n IN (SELECT p FROM missing_s)",
        NO_TABLE_MISSING,
    ),
    (
        "UPDATE t SET n = 99 RETURNING (SELECT nope FROM s)",
        NO_COL_NOPE,
    ),
    (
        "UPDATE t SET n = 99 RETURNING (SELECT p FROM missing_s)",
        NO_TABLE_MISSING,
    ),
    // INSERT VALUES, RETURNING, and UPSERT DO UPDATE SET / WHERE subqueries.
    ("INSERT INTO t VALUES ((SELECT nope FROM s))", NO_COL_NOPE),
    ("INSERT INTO t VALUES ((SELECT t.n FROM s))", NO_COL_T_N),
    (
        "INSERT INTO t VALUES ((SELECT p FROM missing_s))",
        NO_TABLE_MISSING,
    ),
    (
        "INSERT INTO t VALUES ((SELECT nofunc(p) FROM s))",
        NO_FUNC_NOFUNC,
    ),
    (
        "INSERT INTO t VALUES (99) RETURNING (SELECT nope FROM s)",
        NO_COL_NOPE,
    ),
    (
        "INSERT INTO u VALUES (1, 20) ON CONFLICT(k) DO UPDATE SET v = (SELECT nope FROM s)",
        NO_COL_NOPE,
    ),
    (
        "INSERT INTO u VALUES (1, 20) ON CONFLICT(k) DO UPDATE SET v = (SELECT excluded.nope FROM s)",
        NO_COL_EXCLUDED_NOPE,
    ),
    (
        "INSERT INTO u VALUES (5, 20) ON CONFLICT(k) DO UPDATE SET v = (SELECT nope FROM s)",
        NO_COL_NOPE,
    ),
    (
        "INSERT INTO u VALUES (1, 20) ON CONFLICT(k) DO UPDATE SET v = 99 WHERE (SELECT nope FROM s)",
        NO_COL_NOPE,
    ),
    (
        "INSERT INTO u VALUES (1, 20) ON CONFLICT(k) DO UPDATE SET v = (SELECT p FROM missing_s)",
        NO_TABLE_MISSING,
    ),
    // DELETE WHERE and RETURNING subqueries.
    ("DELETE FROM t WHERE (SELECT nope FROM s)", NO_COL_NOPE),
    (
        "DELETE FROM t WHERE EXISTS (SELECT nope FROM s)",
        NO_COL_NOPE,
    ),
    (
        "DELETE FROM t WHERE n IN (SELECT nope FROM s)",
        NO_COL_NOPE,
    ),
    (
        "DELETE FROM t RETURNING (SELECT nope FROM s)",
        NO_COL_NOPE,
    ),
    // SELECT list and WHERE subqueries.
    ("SELECT (SELECT nope FROM s) FROM t", NO_COL_NOPE),
    ("SELECT n FROM t WHERE (SELECT nope FROM s)", NO_COL_NOPE),
    ("SELECT (SELECT p FROM missing_s) FROM t", NO_TABLE_MISSING),
    ("SELECT (SELECT nofunc(p) FROM s) FROM t", NO_FUNC_NOFUNC),
];

const UNCHANGED_CHECKS: &[(&str, &str)] = &[
    ("SELECT n FROM t ORDER BY n", "rows=[[\"1\"]]"),
    ("SELECT a, b FROM m ORDER BY a", "rows=[[\"1\", \"2\"]]"),
    ("SELECT k, v FROM u ORDER BY k", "rows=[[\"1\", \"5\"]]"),
];

const VALID_NEGATIVE_CONTROLS: &[&str] = &[
    // Valid correlated SET subquery reading outer t.n in WHERE.
    "UPDATE t SET n = (SELECT s.v FROM s WHERE s.p = t.n)",
    "SELECT n FROM t ORDER BY n",
    // Valid outer-column reference inside SET subquery SELECT list.
    "UPDATE t SET n = (SELECT t.n + s.v FROM s WHERE s.p = 1)",
    "SELECT n FROM t ORDER BY n",
    // Valid subquery returning no rows (NULL is expected and correct).
    "UPDATE t SET n = (SELECT p FROM empty_s)",
    "SELECT n FROM t ORDER BY n",
    // Valid INSERT ... VALUES with scalar subquery.
    "INSERT INTO t VALUES ((SELECT max(v) FROM s))",
    "SELECT n FROM t ORDER BY n",
    // Valid UPSERT DO UPDATE referencing excluded, target u, and unqualified column.
    "INSERT INTO u VALUES (1, 7) ON CONFLICT(k) DO UPDATE SET v = (SELECT excluded.v + v + s.v FROM s WHERE s.p = u.k)",
    "SELECT k, v FROM u ORDER BY k",
    // Valid UPDATE RETURNING correlated subquery.
    "UPDATE m SET a = 1 RETURNING (SELECT s.v FROM s WHERE s.p = m.a)",
    // Valid WITH CTE referenced inside UPDATE SET subquery (including target-shadowing CTE).
    "WITH c(x) AS (SELECT 42) UPDATE m SET b = (SELECT x FROM c)",
    "SELECT a, b FROM m ORDER BY a",
    "WITH m(x) AS (SELECT 100) UPDATE m SET b = (SELECT x FROM m) WHERE a = 1",
    "SELECT a, b FROM m ORDER BY a",
];

#[test]
fn dml_subquery_unresolved_names_fail_at_prepare_and_write_nothing() {
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in SETUP {
        stock.execute_batch(sql).unwrap();
    }

    let expected_rejected: Vec<String> = REJECTED_STATEMENTS
        .iter()
        .map(|(sql, _)| stock_outcome(&stock, sql))
        .collect();
    for ((sql, pinned), stock_gave) in REJECTED_STATEMENTS.iter().zip(&expected_rejected) {
        assert_eq!(stock_gave, pinned, "stock pin mismatch on `{sql}`");
    }
    for (read_sql, pinned_rows) in UNCHANGED_CHECKS {
        assert_eq!(
            stock_outcome(&stock, read_sql),
            *pinned_rows,
            "stock table mutated by rejected statements on `{read_sql}`"
        );
    }

    let expected_valid: Vec<String> = VALID_NEGATIVE_CONTROLS
        .iter()
        .map(|sql| stock_outcome(&stock, sql))
        .collect();
    for (sql, stock_gave) in VALID_NEGATIVE_CONTROLS.iter().zip(&expected_valid) {
        assert!(
            stock_gave.starts_with("rows="),
            "expected valid negative control `{sql}` to succeed on stock, got {stock_gave}"
        );
    }

    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path()
                        .join("bd_x9br8.db")
                        .to_str()
                        .unwrap()
                        .to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for sql in SETUP {
                    conn.execute(sql).await.unwrap();
                }

                for ((sql, _), expected) in REJECTED_STATEMENTS.iter().zip(&expected_rejected) {
                    let got = fsqlite_outcome(&conn, sql, prepared).await;
                    assert_eq!(
                        &got, expected,
                        "file_backed={file_backed} prepared={prepared}: `{sql}`"
                    );
                }

                for (read_sql, expected_rows) in UNCHANGED_CHECKS {
                    let got = fsqlite_outcome(&conn, read_sql, false).await;
                    assert_eq!(
                        &got, expected_rows,
                        "file_backed={file_backed} prepared={prepared}: table mutated after rejected DML on `{read_sql}`"
                    );
                }

                for (sql, expected) in VALID_NEGATIVE_CONTROLS.iter().zip(&expected_valid) {
                    let got = fsqlite_outcome(&conn, sql, prepared).await;
                    assert_eq!(
                        &got, expected,
                        "file_backed={file_backed} prepared={prepared}: valid negative control `{sql}`"
                    );
                }

                conn.close().await.unwrap();
            }
        }
    });
}
