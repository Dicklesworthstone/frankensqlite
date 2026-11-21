//! bd-4uurt: a prepared statement on a file database runs a GROUP BY with a
//! HAVING through the VDBE grouping substrate when `having_is_substrate_safe`
//! accepts it. Its HAVING coder evaluates aggregates, columns, literals,
//! `IS [NOT] NULL`, comparisons, AND and OR against the group, and codes any
//! other node as if the group's columns were NULL. So
//!
//! ```sql
//! SELECT a, b, count(*) FROM t GROUP BY a, b HAVING a IS 2;
//! ```
//!
//! kept no group where stock SQLite 3.54.1 keeps 2|2|1, and so did `a + 1 =
//! 2`, `abs(a) = 1`, `NOT (a = 2)`, BETWEEN, IN, CASE, `||`, `a IS TRUE`.
//! An ad hoc statement, and a `:memory:` database, run every HAVING on the
//! interpreted path, which answered right.
//!
//! Every statement runs on bundled SQLite 3.54.1 and on fsqlite, on `:memory:`
//! and on a file, executed directly and prepared. Each statement's rows must
//! equal stock's, and are pinned to stock's.

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

const G12: &str = "[\"1\", \"2\", \"2\"]";
const G13: &str = "[\"1\", \"3\", \"1\"]";
const G22: &str = "[\"2\", \"2\", \"1\"]";

/// Each HAVING with the groups stock keeps (as `a|b|count` rows in order).
const HAVINGS: &[(&str, &[&str])] = &[
    ("a IS 2", &[G22]),
    ("a IS NOT 1", &[G22]),
    ("(a, b) IS (2, 2)", &[G22]),
    ("count(*) >= 1 AND a IS 2", &[G22]),
    ("a IS TRUE", &[G12, G13, G22]),
    ("a + 1 = 2", &[G12, G13]),
    ("b - a = 1 OR a = 2", &[G12, G22]),
    ("abs(a) = 1", &[G12, G13]),
    ("typeof(a) = 'integer' AND b = 3", &[G13]),
    ("NOT (a = 2)", &[G12, G13]),
    ("a BETWEEN 1 AND 1", &[G12, G13]),
    ("a IN (2)", &[G22]),
    ("CASE WHEN a = 2 THEN 1 ELSE 0 END", &[G22]),
    ("b || '' = '3'", &[G13]),
    // Controls the substrate already coded right, and an aggregate inside an
    // expression (bd-23kpa's interpreted shape).
    ("a = 1", &[G12, G13]),
    ("count(*) > 1", &[G12]),
    ("sum(v) > 20", &[G12]),
    ("a IS NULL", &[]),
    ("count(*) * 2 > 3", &[G12]),
];

#[test]
fn prepared_group_by_having_keeps_stocks_groups() {
    let queries: Vec<(String, String)> = HAVINGS
        .iter()
        .map(|(having, groups)| {
            (
                format!("SELECT a, b, count(*) FROM t GROUP BY a, b HAVING {having} ORDER BY a, b"),
                format!("rows=[{}]", groups.join(", ")),
            )
        })
        .collect();
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in SETUP {
        stock.execute_batch(sql).unwrap();
    }
    // Not vacuous: stock keeps each pinned set of groups.
    for (sql, pinned) in &queries {
        assert_eq!(&stock_outcome(&stock, sql), pinned, "stock on `{sql}`");
    }
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("bd_4uurt.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for sql in SETUP {
                    conn.execute(sql).await.unwrap();
                }
                for (sql, expected) in &queries {
                    let got = fsqlite_outcome(&conn, sql, prepared).await;
                    assert_eq!(
                        &got, expected,
                        "file_backed={file_backed} prepared={prepared}: `{sql}`"
                    );
                }
            }
        }
    });
}
