//! bd-4uurt: a prepared statement on a file database runs a GROUP BY with a
//! HAVING through the VDBE grouping substrate when its output registers can
//! supply the predicate. Previously an IS comparison read columns without a
//! group context. Thus
//!
//! ```sql
//! SELECT a, b, count(*) FROM t GROUP BY a, b HAVING a IS 2;
//! ```
//!
//! kept no group where stock SQLite 3.54.1 keeps 2|2|1, and so did `a + 1 =
//! 2`, `abs(a) = 1`, `NOT (a = 2)`, BETWEEN, IN, CASE, `||`, `a IS TRUE`.
//! Direct queries formerly used the interpreted path even for simple
//! predicates. Strict file queries also exercise the compiled path directly.
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
                conn.close().await.unwrap();
            }
        }
    });
}

#[test]
fn strict_file_having_uses_finalized_keys_and_aggregates() {
    const DATA: &str = "INSERT INTO t VALUES (NULL,NULL),(1,0),(2,3),(2,4)";
    const CREATE: &str = "CREATE TABLE t(k INTEGER,v INTEGER)";
    let cases: &[(&str, &[Option<i64>])] = &[
        ("k IS '2'", &[Some(2)]),
        ("k = '2'", &[Some(2)]),
        ("k IS NOT 2", &[None, Some(1)]),
        ("k IS NULL", &[None]),
        ("k IS NOT NULL", &[Some(1), Some(2)]),
        ("k IS TRUE", &[Some(1), Some(2)]),
        ("k IS NOT TRUE", &[None]),
        ("sum(v) IS 7", &[Some(2)]),
        ("sum(v) = 7", &[Some(2)]),
        ("sum(v) IS NOT 7", &[None, Some(1)]),
        ("sum(v) IS NULL", &[None]),
        ("sum(v) IS NOT NULL", &[Some(1), Some(2)]),
        ("k IS sum(v)", &[None]),
        ("k = sum(v)", &[]),
        ("kept IS 1 AND count(v) IS 2", &[Some(2)]),
    ];
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    stock.execute_batch(CREATE).unwrap();
    stock.execute_batch(DATA).unwrap();
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("strict.db");
        let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
        conn.execute(CREATE).await.unwrap();
        conn.execute(DATA).await.unwrap();
        conn.execute("PRAGMA fsqlite.parity_cert_strict=ON")
            .await
            .unwrap();
        assert_eq!(
            fsqlite_outcome(&conn, "PRAGMA fsqlite.parity_cert_strict", false).await,
            "rows=[[\"1\"]]"
        );
        for &(predicate, keys) in cases {
            let sql = format!(
                "SELECT k, sum(v), count(v), count(v) FILTER (WHERE v>3) AS kept FROM t GROUP BY k HAVING {predicate} ORDER BY k"
            );
            let expected = stock_outcome(&stock, &sql);
            let actual_keys = stock
                .prepare(&sql)
                .unwrap()
                .query_map([], |row| row.get::<_, Option<i64>>(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            assert_eq!(actual_keys, keys, "stock on {predicate}");
            for prepared in [false, true] {
                assert_eq!(
                    fsqlite_outcome(&conn, &sql, prepared).await,
                    expected,
                    "prepared={prepared}: {sql}"
                );
            }
        }
        conn.close().await.unwrap();
    });
}
