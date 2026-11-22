//! bd-b1qin: a row-value SET from a correlated subquery,
//! `SET (n, c) = (SELECT ... WHERE ... t.n ...)`. Stock SQLite 3.54.1
//! assigns each row the subquery's first row for that row (NULLs when it has
//! none), after checking its width against the targets ("2 columns assigned
//! 3 values"). bd-4l4y3 covered FROM-less and uncorrelated sources; a
//! correlated one was left to a column-by-column projection, which
//! - refused a `*` or compound source ("not implemented: multi-column SET
//!   subquery source must be a single SELECT with explicit result columns"),
//!   and refused a width mismatch the same way instead of stock's error;
//! - wrote wrong values for a source it could not read one column at a time:
//!   a join's second table read NULL, a derived table read NULLs, a GROUP BY
//!   source failed ("no such function: COUNT/0"), and `ORDER BY 1` or an
//!   ORDER BY naming another column's alias ordered each projection by
//!   nothing;
//! - and in an upsert's DO UPDATE refused every correlated source ("multi-column
//!   SET requires a row-value expression").
//!
//! Every statement runs on bundled SQLite 3.54.1 and on fsqlite, on `:memory:`
//! and on a file, executed directly and prepared, and the table is read back
//! after each. Each outcome (its rows, or its error text) must equal stock's,
//! and is pinned to stock's so the comparison cannot pass vacuously.
//!
//! Negative cases a naive fix fails: three target rows with different
//! correlated answers (one source run for the whole statement fails), a row
//! whose source is empty (NULLs, not the previous row's values), a width
//! mismatch that writes nothing, and the simple correlated source the
//! compiled projection already answered still answering.

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

fn render_rows(rows: &[fsqlite_core::connection::Row]) -> String {
    let rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row.values().iter().map(render_fsqlite).collect())
        .collect();
    format!("rows={rows:?}")
}

/// `sql` on fsqlite, run directly or prepared first: a prepared SELECT is
/// queried, a prepared INSERT or UPDATE executed.
async fn fsqlite_outcome(conn: &Connection, sql: &str, prepared: bool) -> String {
    let result = if prepared {
        match conn.prepare(sql).await {
            Ok(statement) if sql.starts_with("SELECT") => statement.query().await,
            Ok(statement) => statement.execute().await.map(|_| Vec::new()),
            Err(error) => Err(error),
        }
    } else {
        conn.query(sql).await
    };
    match result {
        Ok(rows) => render_rows(&rows),
        Err(error) => format!("error: {error}"),
    }
}

const NO_ROWS: &str = "rows=[]";
const SETUP: &[&str] = &[
    "CREATE TABLE t(a UNIQUE, n, c)",
    "INSERT INTO t VALUES ('abc', 1, 0), ('def', 6, 0), ('ghi', 100, 0)",
    "CREATE TABLE s(p, q)",
    "INSERT INTO s VALUES (5, 6), (7, 8)",
    "CREATE TABLE s2(p, q)",
    "INSERT INTO s2 VALUES (5, 60), (7, 80)",
    "CREATE TABLE s3(p, q, r)",
    "INSERT INTO s3 VALUES (5, 6, 7)",
];
const READ: &str = "SELECT * FROM t ORDER BY a";
const RESET: &str =
    "UPDATE t SET n = CASE a WHEN 'abc' THEN 1 WHEN 'def' THEN 6 ELSE 100 END, c = 0";
const UNCHANGED: &str = "rows=[[\"'abc'\", \"1\", \"0\"], [\"'def'\", \"6\", \"0\"], \
                         [\"'ghi'\", \"100\", \"0\"]]";

const STATEMENTS: &[(&str, &str)] = &[
    // The bead's shape 1: `*`, ORDER BY, the first row per target row.
    (
        "UPDATE t SET (n, c) = (SELECT * FROM s WHERE s.p > t.n ORDER BY p)",
        NO_ROWS,
    ),
    (
        READ,
        "rows=[[\"'abc'\", \"5\", \"6\"], [\"'def'\", \"7\", \"8\"], [\"'ghi'\", \"NULL\", \"NULL\"]]",
    ),
    (RESET, NO_ROWS),
    // Shape 2: a compound, its second arm answering when the first is empty.
    (
        "UPDATE t SET (n, c) = (SELECT p, q FROM s WHERE p > t.n UNION ALL SELECT 0, 0)",
        NO_ROWS,
    ),
    (
        READ,
        "rows=[[\"'abc'\", \"5\", \"6\"], [\"'def'\", \"7\", \"8\"], [\"'ghi'\", \"0\", \"0\"]]",
    ),
    (RESET, NO_ROWS),
    (
        "UPDATE t SET (n, c) = \
         (SELECT p, q FROM s WHERE p > t.n UNION SELECT 0, 0 ORDER BY 1 DESC)",
        NO_ROWS,
    ),
    (
        READ,
        "rows=[[\"'abc'\", \"7\", \"8\"], [\"'def'\", \"7\", \"8\"], [\"'ghi'\", \"0\", \"0\"]]",
    ),
    (RESET, NO_ROWS),
    // A join: the second table's column comes from the same first row.
    (
        "UPDATE t SET (n, c) = (SELECT s.p, s2.q FROM s JOIN s2 ON s2.p = s.p \
         WHERE s.p > t.n ORDER BY s.p DESC)",
        NO_ROWS,
    ),
    (
        READ,
        "rows=[[\"'abc'\", \"7\", \"80\"], [\"'def'\", \"7\", \"80\"], [\"'ghi'\", \"NULL\", \"NULL\"]]",
    ),
    (RESET, NO_ROWS),
    // A derived table.
    (
        "UPDATE t SET (n, c) = (SELECT x, y FROM (SELECT p AS x, q AS y FROM s) \
         WHERE x > t.n ORDER BY x DESC)",
        NO_ROWS,
    ),
    (
        READ,
        "rows=[[\"'abc'\", \"7\", \"8\"], [\"'def'\", \"7\", \"8\"], [\"'ghi'\", \"NULL\", \"NULL\"]]",
    ),
    (RESET, NO_ROWS),
    // GROUP BY with an aggregate, ordered by result position.
    (
        "UPDATE t SET (n, c) = (SELECT p, count(*) FROM s WHERE p > t.n GROUP BY p \
         ORDER BY 2 DESC, 1 DESC)",
        NO_ROWS,
    ),
    (
        READ,
        "rows=[[\"'abc'\", \"7\", \"1\"], [\"'def'\", \"7\", \"1\"], [\"'ghi'\", \"NULL\", \"NULL\"]]",
    ),
    (RESET, NO_ROWS),
    // ORDER BY a result position, then another column's alias: a projection
    // of one column cannot keep either.
    (
        "UPDATE t SET (n, c) = (SELECT p, q FROM s WHERE p > t.n ORDER BY 1 DESC)",
        NO_ROWS,
    ),
    (
        READ,
        "rows=[[\"'abc'\", \"7\", \"8\"], [\"'def'\", \"7\", \"8\"], [\"'ghi'\", \"NULL\", \"NULL\"]]",
    ),
    (RESET, NO_ROWS),
    (
        "UPDATE t SET (n, c) = (SELECT p AS x, q AS y FROM s WHERE p > t.n ORDER BY y DESC)",
        NO_ROWS,
    ),
    (
        READ,
        "rows=[[\"'abc'\", \"7\", \"8\"], [\"'def'\", \"7\", \"8\"], [\"'ghi'\", \"NULL\", \"NULL\"]]",
    ),
    (RESET, NO_ROWS),
    // A bare column beside an aggregate reads the aggregate's row; a window
    // function numbers the whole row set.
    (
        "UPDATE t SET (n, c) = (SELECT max(p), q FROM s WHERE p > t.n)",
        NO_ROWS,
    ),
    (
        READ,
        "rows=[[\"'abc'\", \"7\", \"8\"], [\"'def'\", \"7\", \"8\"], [\"'ghi'\", \"NULL\", \"NULL\"]]",
    ),
    (RESET, NO_ROWS),
    (
        "UPDATE t SET (n, c) = (SELECT p, row_number() OVER (ORDER BY p DESC) FROM s \
         WHERE p > t.n ORDER BY p)",
        NO_ROWS,
    ),
    (
        READ,
        "rows=[[\"'abc'\", \"5\", \"2\"], [\"'def'\", \"7\", \"1\"], [\"'ghi'\", \"NULL\", \"NULL\"]]",
    ),
    (RESET, NO_ROWS),
    // Control: the simple correlated source the compiled projection reads.
    (
        "UPDATE t SET (n, c) = (SELECT p, q FROM s WHERE s.p > t.n ORDER BY p DESC)",
        NO_ROWS,
    ),
    (
        READ,
        "rows=[[\"'abc'\", \"7\", \"8\"], [\"'def'\", \"7\", \"8\"], [\"'ghi'\", \"NULL\", \"NULL\"]]",
    ),
    (RESET, NO_ROWS),
    // No row for any target row: NULLs.
    (
        "UPDATE t SET (n, c) = (SELECT * FROM s WHERE s.p > t.n + 1000)",
        NO_ROWS,
    ),
    (
        READ,
        "rows=[[\"'abc'\", \"NULL\", \"NULL\"], [\"'def'\", \"NULL\", \"NULL\"], \
         [\"'ghi'\", \"NULL\", \"NULL\"]]",
    ),
    (RESET, NO_ROWS),
    // Widths: stock's error, nothing written.
    (
        "UPDATE t SET (n, c) = (SELECT * FROM s3 WHERE s3.p > t.n)",
        "error: 2 columns assigned 3 values",
    ),
    (READ, UNCHANGED),
    (
        "UPDATE t SET (n, c) = (SELECT p, q, 1 FROM s WHERE p > t.n UNION ALL SELECT 0, 0, 0)",
        "error: 2 columns assigned 3 values",
    ),
    (READ, UNCHANGED),
    // A name nothing supplies is refused before the width.
    (
        "UPDATE t SET (n, c) = (SELECT * FROM s WHERE s.p > t.nope)",
        "error: no such column: t.nope",
    ),
    // Control: an uncorrelated `*` source (bd-4l4y3).
    (
        "UPDATE t SET (n, c) = (SELECT * FROM s ORDER BY p DESC)",
        NO_ROWS,
    ),
    (
        READ,
        "rows=[[\"'abc'\", \"7\", \"8\"], [\"'def'\", \"7\", \"8\"], [\"'ghi'\", \"7\", \"8\"]]",
    ),
    (RESET, NO_ROWS),
    // Mixed with an ordinary assignment and a WHERE.
    (
        "UPDATE t SET a = a || '!', (n, c) = (SELECT * FROM s WHERE s.p > t.n ORDER BY p) \
         WHERE a <> 'ghi'",
        NO_ROWS,
    ),
    (
        READ,
        "rows=[[\"'abc!'\", \"5\", \"6\"], [\"'def!'\", \"7\", \"8\"], [\"'ghi'\", \"100\", \"0\"]]",
    ),
    (
        "UPDATE t SET a = rtrim(a, '!'), \
         n = CASE rtrim(a, '!') WHEN 'abc' THEN 1 WHEN 'def' THEN 6 ELSE 100 END, c = 0",
        NO_ROWS,
    ),
    (READ, UNCHANGED),
    // Shape 3: an upsert's DO UPDATE reading `excluded`.
    (
        "INSERT INTO t VALUES ('abc', 6, 9) ON CONFLICT(a) DO UPDATE \
         SET (n, c) = (SELECT p, q FROM s WHERE p > excluded.n)",
        NO_ROWS,
    ),
    (
        READ,
        "rows=[[\"'abc'\", \"7\", \"8\"], [\"'def'\", \"6\", \"0\"], [\"'ghi'\", \"100\", \"0\"]]",
    ),
    (RESET, NO_ROWS),
    // The existing row's columns, and ORDER BY.
    (
        "INSERT INTO t VALUES ('def', 2, 9) ON CONFLICT(a) DO UPDATE \
         SET (n, c) = (SELECT p, q FROM s WHERE p > t.n ORDER BY p DESC)",
        NO_ROWS,
    ),
    (
        READ,
        "rows=[[\"'abc'\", \"1\", \"0\"], [\"'def'\", \"7\", \"8\"], [\"'ghi'\", \"100\", \"0\"]]",
    ),
    (RESET, NO_ROWS),
    // No row: NULLs.
    (
        "INSERT INTO t VALUES ('abc', 50, 9) ON CONFLICT(a) DO UPDATE \
         SET (n, c) = (SELECT p, q FROM s WHERE p > excluded.n)",
        NO_ROWS,
    ),
    (
        READ,
        "rows=[[\"'abc'\", \"NULL\", \"NULL\"], [\"'def'\", \"6\", \"0\"], [\"'ghi'\", \"100\", \"0\"]]",
    ),
    (RESET, NO_ROWS),
    (
        "INSERT INTO t VALUES ('abc', 1, 9) ON CONFLICT(a) DO UPDATE \
         SET (n, c) = (SELECT p, q, 1 FROM s WHERE p > excluded.n)",
        "error: 2 columns assigned 3 values",
    ),
    (READ, UNCHANGED),
    // Mixed with an ordinary assignment.
    (
        "INSERT INTO t VALUES ('abc', 1, 9) ON CONFLICT(a) DO UPDATE \
         SET c = excluded.c, (n) = (SELECT q FROM s WHERE p > excluded.n ORDER BY p DESC)",
        NO_ROWS,
    ),
    (
        READ,
        "rows=[[\"'abc'\", \"8\", \"9\"], [\"'def'\", \"6\", \"0\"], [\"'ghi'\", \"100\", \"0\"]]",
    ),
];

#[test]
fn correlated_row_value_set_assigns_each_rows_first_source_row_like_stock() {
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in SETUP {
        stock.execute_batch(sql).unwrap();
    }
    let expected: Vec<String> = STATEMENTS
        .iter()
        .map(|(sql, _)| stock_outcome(&stock, sql))
        .collect();
    // Not vacuous: stock gives each pinned outcome.
    for ((sql, pinned), stock_gave) in STATEMENTS.iter().zip(&expected) {
        assert_eq!(stock_gave, pinned, "stock on `{sql}`");
    }
    asupersync::test_utils::run_test(|| async {
        let mut mismatches = Vec::new();
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("bd_b1qin.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for sql in SETUP {
                    conn.execute(sql).await.unwrap();
                }
                for ((sql, _), expected) in STATEMENTS.iter().zip(&expected) {
                    let got = fsqlite_outcome(&conn, sql, prepared).await;
                    if &got != expected {
                        mismatches.push(format!(
                            "file_backed={file_backed} prepared={prepared} `{sql}`:\n    \
                             got  {got}\n    want {expected}"
                        ));
                    }
                }
            }
        }
        assert!(
            mismatches.is_empty(),
            "{} outcome(s) differ from stock:\n{}",
            mismatches.len(),
            mismatches.join("\n")
        );
    });
}
