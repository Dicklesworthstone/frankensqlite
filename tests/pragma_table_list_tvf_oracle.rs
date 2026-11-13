#![recursion_limit = "512"]

//! bd-reality-check-1022-llk6g.28: the table-valued `pragma_table_list` form
//! (bare, `()` and `('t')`) must be recognized and return the rows stock
//! SQLite returns. `PRAGMA table_list` itself already worked; the TVF was
//! rejected with "no such table: pragma_table_list".
//!
//! Stock emits table_list rows in schema hash order, so every query here
//! sorts.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SCHEMA: &str = "CREATE TABLE t(a, b, c);\
     CREATE TABLE s(x INTEGER PRIMARY KEY, y TEXT) STRICT;\
     CREATE TABLE w(k TEXT PRIMARY KEY, v) WITHOUT ROWID;\
     CREATE VIEW vt AS SELECT a, b FROM t;\
     CREATE TEMP TABLE tt(q);";

const QUERIES: &[&str] = &[
    "SELECT schema, name, type, ncol, wr, strict FROM pragma_table_list ORDER BY schema, name",
    "SELECT * FROM pragma_table_list() ORDER BY schema, name",
    "SELECT * FROM pragma_table_list('t') ORDER BY schema, name",
    "SELECT * FROM pragma_table_list('s') ORDER BY schema, name",
    "SELECT name FROM pragma_table_list WHERE type = 'view'",
    "SELECT name, strict FROM pragma_table_list WHERE strict ORDER BY name",
    "SELECT count(*) FROM pragma_table_list WHERE schema = 'main'",
];

async fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|value| match value {
                    SqliteValue::Null => "NULL".to_owned(),
                    SqliteValue::Integer(n) => n.to_string(),
                    SqliteValue::Float(f) => format!("{f}"),
                    SqliteValue::Text(s) => s.to_string(),
                    SqliteValue::Blob(b) => format!("{b:?}"),
                })
                .collect()
        })
        .collect()
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn
        .prepare(sql)
        .unwrap_or_else(|e| panic!("stock {sql}: {e}"));
    let columns = stmt.column_count();
    stmt.query_map([], |row| {
        (0..columns)
            .map(|i| {
                Ok(match row.get_ref(i)? {
                    rusqlite::types::ValueRef::Null => "NULL".to_owned(),
                    rusqlite::types::ValueRef::Integer(n) => n.to_string(),
                    rusqlite::types::ValueRef::Real(f) => format!("{f}"),
                    rusqlite::types::ValueRef::Text(s) => String::from_utf8_lossy(s).into_owned(),
                    rusqlite::types::ValueRef::Blob(b) => format!("{b:?}"),
                })
            })
            .collect()
    })
    .unwrap_or_else(|e| panic!("stock {sql}: {e}"))
    .map(Result::unwrap)
    .collect()
}

#[test]
fn pragma_table_list_tvf_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        frank.execute_batch(SCHEMA).await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        stock.execute_batch(SCHEMA).unwrap();

        for sql in QUERIES {
            let expected = stock_rows(&stock, sql);
            assert!(!expected.is_empty(), "stock returned no rows for {sql}");
            assert_eq!(frank_rows(&frank, sql).await, expected, "{sql}");
        }

        // The TVF and the statement form agree row for row.
        let mut statement = frank_rows(&frank, "PRAGMA table_list").await;
        statement.sort();
        let mut tvf = frank_rows(&frank, "SELECT * FROM pragma_table_list").await;
        tvf.sort();
        assert_eq!(tvf, statement, "pragma_table_list must mirror PRAGMA table_list");
    });
}
