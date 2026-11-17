//! bd-r39kx: `EXPLAIN QUERY PLAN` names the access path fsqlite runs in stock's
//! words, for the single-table shapes whose program and stock's plan agree:
//! a rowid range the program seeks prints `SEARCH t USING INTEGER PRIMARY KEY
//! (rowid>? AND rowid<?)`, an index range prints its bounds (`(a>?)`,
//! `(a>? AND a<?)`) instead of `(a range)`, an index seek that never reads the
//! table is `COVERING`, an alias names the source, and the sorter an ORDER BY
//! needs is reported. Each statement compares with the bundled SQLite 3.54.1
//! on `:memory:` and on a file, and returns the same rows. Negatives: a real
//! full scan still prints `SCAN t`, a seek that looks the row up is not
//! `COVERING`, and an ORDER BY the index already satisfies adds no sorter row.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SETUP: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER)",
    "CREATE INDEX ta ON t(a)",
    "CREATE INDEX tbc ON t(b, c)",
    "CREATE TABLE n(x, y)",
    "CREATE INDEX nx ON n(x)",
    "INSERT INTO t VALUES (1, 10, 'x', 1), (2, 20, 'y', 2), (3, 30, 'x', 3)",
    "INSERT INTO n VALUES (4, 'p'), (6, 'q')",
];

/// Statements whose `EXPLAIN QUERY PLAN` rows must equal stock's.
const SHAPES: &[&str] = &[
    // Rowid ranges the program seeks.
    "SELECT * FROM t WHERE id > 1",
    "SELECT * FROM t WHERE id >= 1",
    "SELECT * FROM t WHERE id BETWEEN 1 AND 2",
    "SELECT * FROM t WHERE id > 1 AND id < 3",
    "SELECT * FROM t WHERE rowid > 1",
    "SELECT b FROM t WHERE id > 1 AND c = 3",
    // Index ranges.
    "SELECT * FROM t WHERE a > 15",
    "SELECT * FROM t WHERE a >= 15",
    "SELECT a FROM t WHERE a BETWEEN 15 AND 40",
    "SELECT a FROM t WHERE a > 15 AND a < 40",
    "SELECT * FROM t WHERE b > 'w'",
    "SELECT * FROM n WHERE x > 5",
    "SELECT * FROM t AS q WHERE q.a > 15",
    "SELECT * FROM t WHERE a > 15 LIMIT 1",
    // Covering and not.
    "SELECT id FROM t WHERE a = 20",
    "SELECT id, a FROM t WHERE a = 20",
    "SELECT * FROM t WHERE a = 20",
    "SELECT * FROM t AS q WHERE q.a = 20",
    // The ORDER BY sorter, and an ORDER BY the index satisfies.
    "SELECT * FROM t WHERE a = 10 ORDER BY b",
    "SELECT * FROM t WHERE a = 20 ORDER BY id",
    // Real scans and point lookups.
    "SELECT * FROM t",
    "SELECT * FROM t WHERE id = 2",
    "SELECT * FROM t WHERE a + 0 = 20",
];

/// Stock's text for the slice, pinned so a change in the oracle shows here.
const PINNED: &[(&str, &[&str])] = &[
    (
        "SELECT * FROM t WHERE id > 1",
        &["SEARCH t USING INTEGER PRIMARY KEY (rowid>?)"],
    ),
    (
        "SELECT * FROM t WHERE id BETWEEN 1 AND 2",
        &["SEARCH t USING INTEGER PRIMARY KEY (rowid>? AND rowid<?)"],
    ),
    ("SELECT * FROM t WHERE a > 15", &["SEARCH t USING INDEX ta (a>?)"]),
    (
        "SELECT a FROM t WHERE a > 15 AND a < 40",
        &["SEARCH t USING COVERING INDEX ta (a>? AND a<?)"],
    ),
    ("SELECT * FROM t AS q WHERE q.a > 15", &["SEARCH q USING INDEX ta (a>?)"]),
    ("SELECT id FROM t WHERE a = 20", &["SEARCH t USING COVERING INDEX ta (a=?)"]),
    ("SELECT * FROM t WHERE a = 20", &["SEARCH t USING INDEX ta (a=?)"]),
    (
        "SELECT * FROM t WHERE a = 10 ORDER BY b",
        &["SEARCH t USING INDEX ta (a=?)", "USE TEMP B-TREE FOR ORDER BY"],
    ),
    ("SELECT * FROM t WHERE a = 20 ORDER BY id", &["SEARCH t USING INDEX ta (a=?)"]),
    ("SELECT * FROM t", &["SCAN t"]),
];

async fn frank_eqp(conn: &Connection, sql: &str) -> Vec<String> {
    conn.query(&format!("EXPLAIN QUERY PLAN {sql}"))
        .await
        .unwrap_or_else(|error| panic!("fsqlite EQP `{sql}`: {error}"))
        .iter()
        .map(|row| match row.values().get(3) {
            Some(SqliteValue::Text(detail)) => detail.to_string(),
            other => panic!("EQP `{sql}`: detail column {other:?}"),
        })
        .collect()
}

fn stock_eqp(conn: &rusqlite::Connection, sql: &str) -> Vec<String> {
    let mut statement = conn
        .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
        .unwrap_or_else(|error| panic!("stock EQP `{sql}`: {error}"));
    statement
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap_or_else(|error| panic!("stock EQP `{sql}`: {error}"))
        .map(|detail| detail.expect("stock EQP row"))
        .collect()
}

fn render(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(integer) => integer.to_string(),
        SqliteValue::Float(float) => format!("{float}r"),
        SqliteValue::Text(text) => format!("'{text}'"),
        SqliteValue::Blob(blob) => format!("{blob:?}"),
    }
}

fn render_stock(value: rusqlite::types::ValueRef<'_>) -> String {
    use rusqlite::types::ValueRef;
    match value {
        ValueRef::Null => "NULL".to_owned(),
        ValueRef::Integer(integer) => integer.to_string(),
        ValueRef::Real(float) => format!("{float}r"),
        ValueRef::Text(text) => format!("'{}'", String::from_utf8_lossy(text)),
        ValueRef::Blob(blob) => format!("{blob:?}"),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|error| panic!("fsqlite `{sql}`: {error}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
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
                .map(|index| row.get_ref(index).map(render_stock))
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .unwrap_or_else(|error| panic!("stock `{sql}`: {error}"))
        .map(|row| row.expect("stock row"))
        .collect()
}

#[test]
fn eqp_names_rowid_ranges_index_bounds_covering_and_sorter_as_stock() {
    asupersync::test_utils::run_test(|| async {
        let stock = rusqlite::Connection::open_in_memory().expect("stock");
        for sql in SETUP {
            stock.execute_batch(sql).expect("stock setup");
        }
        for (sql, expected) in PINNED {
            assert_eq!(stock_eqp(&stock, sql), *expected, "stock EQP `{sql}`");
        }
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("eqp.db");
        for path in [":memory:", file.to_str().expect("utf-8 path")] {
            let conn = Connection::open(path).await.expect("fsqlite open");
            for sql in SETUP {
                conn.execute(sql).await.expect("fsqlite setup");
            }
            for sql in SHAPES {
                assert_eq!(
                    frank_eqp(&conn, sql).await,
                    stock_eqp(&stock, sql),
                    "{path}: EQP `{sql}`"
                );
                let mut frank = frank_rows(&conn, sql).await;
                let mut oracle = stock_rows(&stock, sql);
                if !sql.contains("ORDER BY") {
                    frank.sort();
                    oracle.sort();
                }
                assert_eq!(frank, oracle, "{path}: rows of `{sql}`");
            }
            conn.close().await.expect("fsqlite close");
        }
    });
}
