//! bd-gjor8: a nested row-value comparison in a HAVING term that reads only
//! GROUP BY terms. Stock's havingToWhere moves such a term into WHERE, where
//! its WHERE analysis splits `((a, b), 1) = ((1, 2), 1)` into one term per
//! element, so
//!
//! ```sql
//! SELECT a, b, count(*) FROM t GROUP BY a, b HAVING ((a, b), 1) = ((1, 2), 1);
//! ```
//!
//! answers 1|2|2 in stock SQLite 3.54.1. fsqlite refused it as "row value
//! misused", the rule for a row value where one value is expected, which
//! still holds for a term with an aggregate or another column, an OR, or a
//! comparison that does not split (`<`, `IN`).
//!
//! Every statement runs on bundled SQLite 3.54.1 and on fsqlite, on `:memory:`
//! and on a file, executed directly and prepared. Each statement's rows, or
//! its error text, must equal stock's, and is pinned to stock's so the
//! comparison cannot pass vacuously. The file matrix repeats the comparison
//! and misuse cases with fallback rejection enabled. Arithmetic and scalar
//! function arguments additionally exercise the grouped expression evaluator.

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
            Ok(statement) => statement.query().await,
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
    "CREATE TABLE t(a, b, v)",
    "INSERT INTO t VALUES (1, 2, 10), (1, 2, 11), (1, 3, 12), (2, 2, 13)",
];

const MISUSED: &str = "error: row value misused";
const ONE_TWO: &str = "rows=[[\"1\", \"2\", \"2\"]]";

/// Each query with the outcome stock gives.
const QUERIES: &[(&str, &str)] = &[
    // The bead's shape, and beside an aggregate term.
    (
        "SELECT a, b, count(*) FROM t GROUP BY a, b HAVING ((a, b), 1) = ((1, 2), 1)",
        ONE_TWO,
    ),
    (
        "SELECT a, b, count(*) FROM t GROUP BY a, b \
         HAVING ((a, b), 1) = ((1, 2), 1) AND count(*) > 1",
        ONE_TWO,
    ),
    (
        "SELECT a, b, count(*) FROM t GROUP BY a, b \
         HAVING count(*) >= 1 AND ((a, b), 1) IS ((2, 2), 1)",
        "rows=[[\"2\", \"2\", \"1\"]]",
    ),
    (
        "SELECT a, b, count(*) FROM t GROUP BY a, b HAVING ((a, b), NULL) = ((1, 2), 1)",
        "rows=[]",
    ),
    // A one-level row value, as before.
    (
        "SELECT a, b, count(*) FROM t GROUP BY a, b HAVING (a, b) = (1, 2)",
        ONE_TWO,
    ),
    // Negatives: still misuse with an aggregate or another column in the
    // term, inside an OR, or in a comparison that does not split, and for
    // sizes that differ.
    (
        "SELECT a, b, count(*) FROM t GROUP BY a, b HAVING ((a, count(*)), 1) = ((1, 2), 1)",
        MISUSED,
    ),
    (
        "SELECT a, count(*) FROM t GROUP BY a HAVING ((a, v), 1) = ((1, 10), 1)",
        MISUSED,
    ),
    (
        "SELECT a, b, count(*) FROM t GROUP BY a, b HAVING ((a, b), 1) = ((1, 2), 1) OR b = 3",
        MISUSED,
    ),
    (
        "SELECT a, b, count(*) FROM t GROUP BY a, b HAVING ((a, b), 1) < ((1, 3), 1)",
        MISUSED,
    ),
    (
        "SELECT a, b, count(*) FROM t GROUP BY a, b HAVING ((a, b), 1) IN (((1, 2), 1))",
        MISUSED,
    ),
    (
        "SELECT a, b, count(*) FROM t GROUP BY a, b HAVING ((a, b), 1) = ((1, 2), 1, 3)",
        MISUSED,
    ),
];

const EXPRESSION_QUERIES: &[(&str, &str)] = &[
    (
        "SELECT a, b, count(*) FROM t GROUP BY a, b HAVING ((a + 1, b), 1) = ((2, 3), 1)",
        "rows=[[\"1\", \"3\", \"1\"]]",
    ),
    (
        "SELECT a, b, count(*) FROM t GROUP BY a, b HAVING ((abs(a), b), 1) = ((1, 2), 1)",
        ONE_TWO,
    ),
];

#[test]
fn having_nested_row_values_over_group_by_terms_match_stock() {
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in SETUP {
        stock.execute_batch(sql).unwrap();
    }
    for &(sql, pinned) in QUERIES.iter().chain(EXPRESSION_QUERIES) {
        assert_eq!(stock_outcome(&stock, sql), pinned, "stock on `{sql}`");
    }
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("bd_gjor8.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for sql in SETUP {
                    conn.execute(sql).await.unwrap();
                }
                for &(sql, expected) in QUERIES.iter().chain(EXPRESSION_QUERIES) {
                    let got = fsqlite_outcome(&conn, sql, prepared).await;
                    assert_eq!(
                        got, expected,
                        "file_backed={file_backed} prepared={prepared}: `{sql}`"
                    );
                }
                if file_backed {
                    conn.execute("PRAGMA fsqlite.parity_cert_strict=ON")
                        .await
                        .unwrap();
                    for &(sql, expected) in QUERIES {
                        assert_eq!(
                            fsqlite_outcome(&conn, sql, prepared).await,
                            expected,
                            "strict file prepared={prepared}: `{sql}`"
                        );
                    }
                }
                conn.close().await.unwrap();
            }
        }
    });
}
