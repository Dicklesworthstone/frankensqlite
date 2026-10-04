#![recursion_limit = "512"]

//! bd-gjlhh: a SELECT that reads both `main` and an attached schema runs
//! through the connection's mixed-schema fallback. That fallback sent every
//! shape except a join to the plain join executor, which rejects a FROM-less
//! body ("JOIN on non-SELECT core") and returns aggregates as one NULL per
//! input row. Each query here must answer exactly what stock SQLite answers.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

const SETUP: &[&str] = &[
    "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)",
    "INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, 'x')",
    "ATTACH ':memory:' AS aux",
    "CREATE TABLE aux.t2(z)",
    "INSERT INTO aux.t2 VALUES (5), (6)",
];

const QUERIES: &[&str] = &[
    "SELECT 'q1', (SELECT max(z) FROM aux.t2), (SELECT count(*) FROM main.t)",
    "SELECT (SELECT max(z) FROM aux.t2) + (SELECT count(*) FROM t)",
    "SELECT count(*), (SELECT max(z) FROM aux.t2) FROM t",
    "SELECT count(*), sum(a) FROM t WHERE a < (SELECT max(z) FROM aux.t2)",
    "SELECT b, count(*) FROM t WHERE a <= (SELECT min(z) FROM aux.t2) GROUP BY b ORDER BY b",
    "SELECT count(*) FROM t, aux.t2",
    "SELECT a FROM t WHERE a IN (SELECT z - 4 FROM aux.t2) ORDER BY a",
    "SELECT a, (SELECT count(*) FROM aux.t2) FROM t ORDER BY a",
    "VALUES ((SELECT max(z) FROM aux.t2), (SELECT count(*) FROM t))",
];

fn render_fsqlite(rows: &[Row]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|value| match value {
                    SqliteValue::Null => "null".to_owned(),
                    SqliteValue::Integer(i) => format!("i:{i}"),
                    SqliteValue::Float(f) => format!("r:{f}"),
                    SqliteValue::Text(t) => format!("t:{t}"),
                    SqliteValue::Blob(b) => format!("b:{b:?}"),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

fn render_stock(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<String>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let columns = stmt.column_count();
    stmt.query_map([], |row| {
        let mut cells = Vec::with_capacity(columns);
        for i in 0..columns {
            cells.push(match row.get_ref(i)? {
                rusqlite::types::ValueRef::Null => "null".to_owned(),
                rusqlite::types::ValueRef::Integer(v) => format!("i:{v}"),
                rusqlite::types::ValueRef::Real(v) => format!("r:{v}"),
                rusqlite::types::ValueRef::Text(v) => format!("t:{}", String::from_utf8_lossy(v)),
                rusqlite::types::ValueRef::Blob(v) => format!("b:{v:?}"),
            });
        }
        Ok(cells.join("|"))
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

#[test]
fn mixed_main_and_attached_selects_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open :memory:");
        let stock = rusqlite::Connection::open_in_memory().expect("stock");
        for sql in SETUP {
            conn.execute(sql)
                .await
                .unwrap_or_else(|e| panic!("fsqlite {sql}: {e}"));
            stock
                .execute_batch(sql)
                .unwrap_or_else(|e| panic!("stock {sql}: {e}"));
        }
        let mut mismatches = Vec::new();
        for sql in QUERIES {
            let expected = render_stock(&stock, sql).unwrap_or_else(|e| panic!("stock {sql}: {e}"));
            match conn.query(sql).await {
                Ok(rows) if render_fsqlite(&rows) == expected => {}
                Ok(rows) => mismatches.push(format!(
                    "{sql}\n    stock:   {expected:?}\n    fsqlite: {:?}",
                    render_fsqlite(&rows)
                )),
                Err(error) => mismatches.push(format!(
                    "{sql}\n    stock:   {expected:?}\n    fsqlite error: {error}"
                )),
            }
        }
        assert!(
            mismatches.is_empty(),
            "{} mixed-schema queries differ from stock:\n{}",
            mismatches.len(),
            mismatches.join("\n")
        );
    });
}
