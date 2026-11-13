//! Probe (MistyRabbit): the UNIQUE IdxInsert right-edge append fast path
//! writes from the cursor's cached leaf snapshot. A trigger that inserts into
//! the same index between two outer rows writes that leaf through another
//! cursor; if the outer cursor's next append still trusts its snapshot, the
//! trigger's entry is lost from the index.

#![recursion_limit = "512"]

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use rusqlite::types::Value;

fn frank_text(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(integer) => integer.to_string(),
        SqliteValue::Float(float) => float.to_string(),
        _ => value.as_text().unwrap_or_default().to_owned(),
    }
}

fn stock_text(value: Value) -> String {
    match value {
        Value::Null => "NULL".to_owned(),
        Value::Integer(integer) => integer.to_string(),
        Value::Real(float) => float.to_string(),
        Value::Text(text) => text,
        Value::Blob(blob) => format!("{blob:?}"),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|error| panic!("fsqlite `{sql}`: {error}"))
        .iter()
        .map(|row| row.values().iter().map(frank_text).collect())
        .collect()
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut statement = conn
        .prepare(sql)
        .unwrap_or_else(|error| panic!("stock `{sql}`: {error}"));
    let width = statement.column_count();
    statement
        .query_map([], |row| {
            (0..width)
                .map(|index| row.get::<_, Value>(index).map(stock_text))
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .expect("stock query")
        .collect::<rusqlite::Result<_>>()
        .expect("stock rows")
}

const SCENARIOS: [(&str, &[&str]); 4] = [
    (
        "unique column, AFTER trigger, multi-row VALUES",
        &[
            "CREATE TABLE t(a INTEGER UNIQUE, b INTEGER)",
            "CREATE TRIGGER tr AFTER INSERT ON t WHEN new.b = 0 BEGIN \
             INSERT INTO t VALUES (new.a + 1000, 1); END",
            "INSERT INTO t VALUES (1, 0), (2, 0), (3, 0), (4, 0)",
        ],
    ),
    (
        "unique index, BEFORE trigger, INSERT SELECT over 300 rows",
        &[
            "CREATE TABLE t(a INTEGER, b INTEGER)",
            "CREATE UNIQUE INDEX ix ON t(a)",
            "CREATE TRIGGER tr BEFORE INSERT ON t WHEN new.b = 0 BEGIN \
             INSERT INTO t VALUES (new.a + 100000, 1); END",
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 300) \
             INSERT INTO t SELECT x, 0 FROM c",
        ],
    ),
    (
        "unique text keys, AFTER trigger, INSERT SELECT over 300 rows",
        &[
            "CREATE TABLE t(a TEXT UNIQUE, b INTEGER)",
            "CREATE TRIGGER tr AFTER INSERT ON t WHEN new.b = 0 BEGIN \
             INSERT INTO t VALUES ('z' || new.a, 1); END",
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 300) \
             INSERT INTO t SELECT printf('k%05d', x), 0 FROM c",
        ],
    ),
    (
        "non-unique index (no fast path), AFTER trigger: control",
        &[
            "CREATE TABLE t(a INTEGER, b INTEGER)",
            "CREATE INDEX ix ON t(a)",
            "CREATE TRIGGER tr AFTER INSERT ON t WHEN new.b = 0 BEGIN \
             INSERT INTO t VALUES (new.a + 100000, 1); END",
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 300) \
             INSERT INTO t SELECT x, 0 FROM c",
        ],
    ),
];

const CHECKS: [&str; 3] = [
    "SELECT count(*) FROM t",
    "SELECT a FROM t WHERE a IS NOT NULL ORDER BY a",
    "SELECT count(*) FROM t WHERE a IN (SELECT a FROM t)",
];

#[test]
fn trigger_writes_between_outer_index_appends_keep_every_entry() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        for (label, statements) in SCENARIOS {
            let dir = tempfile::tempdir().expect("tempdir");
            let ours = dir.path().join("frank.db");
            let stock = rusqlite::Connection::open_in_memory().expect("stock open");
            let frank = Connection::open(ours.to_str().expect("utf-8"))
                .await
                .expect("fsqlite open");
            for sql in statements {
                stock
                    .execute_batch(sql)
                    .unwrap_or_else(|error| panic!("{label}: stock `{sql}`: {error}"));
                frank
                    .execute(sql)
                    .await
                    .unwrap_or_else(|error| panic!("{label}: fsqlite `{sql}`: {error}"));
            }
            for sql in CHECKS {
                let (f, s) = (frank_rows(&frank, sql).await, stock_rows(&stock, sql));
                if f != s {
                    failures.push(format!(
                        "{label}: `{sql}` fsqlite {} rows, stock {} rows (first diff: {:?})",
                        f.len(),
                        s.len(),
                        f.iter().zip(&s).find(|(a, b)| a != b)
                    ));
                }
            }
            let frank_check = frank_rows(&frank, "PRAGMA integrity_check").await;
            frank.close().await.expect("fsqlite close");
            let reader = rusqlite::Connection::open(&ours).expect("stock open fsqlite file");
            let stock_check = stock_rows(&reader, "PRAGMA integrity_check");
            eprintln!("{label}: fsqlite integrity {frank_check:?}; stock on fsqlite file {stock_check:?}");
            if stock_check != vec![vec!["ok".to_owned()]] {
                failures.push(format!("{label}: stock integrity_check on the fsqlite file: {stock_check:?}"));
            }
        }
        assert!(failures.is_empty(), "{failures:#?}");
    });
}
