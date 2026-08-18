//! GH #271 / bd-c6jre: the `fts5vocab` virtual-table module.
//!
//! For each vocabulary type (`row`, `col`, `instance`) this creates and
//! populates an FTS5 table, then compares frankensqlite's
//! `SELECT * FROM <vocab> ORDER BY ...` against bundled stock SQLite (rusqlite,
//! which ships fts5vocab) over an identically-built table. Stock is the oracle.

#![cfg(feature = "ext-fts5")]

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

/// A provider-agnostic cell value for exact row comparison.
#[derive(Debug, Clone, PartialEq)]
enum Cell {
    Null,
    Int(i64),
    Text(String),
    Other(String),
}

fn frank_cell(value: &SqliteValue) -> Cell {
    match value {
        SqliteValue::Null => Cell::Null,
        SqliteValue::Integer(n) => Cell::Int(*n),
        SqliteValue::Text(t) => Cell::Text(String::from_utf8_lossy(t.as_bytes()).into_owned()),
        other => Cell::Other(format!("{other:?}")),
    }
}

fn frank_rows(rows: &[Row]) -> Vec<Vec<Cell>> {
    rows.iter()
        .map(|row| row.values().iter().map(frank_cell).collect())
        .collect()
}

fn sqlite_cell(value: rusqlite::types::ValueRef<'_>) -> Cell {
    use rusqlite::types::ValueRef;
    match value {
        ValueRef::Null => Cell::Null,
        ValueRef::Integer(n) => Cell::Int(n),
        ValueRef::Text(t) => Cell::Text(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Real(f) => Cell::Other(format!("{f}")),
        ValueRef::Blob(b) => Cell::Other(format!("blob:{b:?}")),
    }
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<Cell>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let ncol = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            let mut out = Vec::with_capacity(ncol);
            for i in 0..ncol {
                out.push(sqlite_cell(row.get_ref(i).unwrap()));
            }
            Ok(out)
        })
        .unwrap();
    rows.map(Result::unwrap).collect()
}

const CORPUS: &[&str] = &[
    "CREATE VIRTUAL TABLE ft USING fts5(a, b);",
    "INSERT INTO ft(rowid, a, b) VALUES (10, 'alpha beta beta', 'gamma alpha');",
    "INSERT INTO ft(rowid, a, b) VALUES (20, 'beta delta', 'alpha alpha epsilon');",
    "INSERT INTO ft(rowid, a, b) VALUES (30, 'gamma', 'delta');",
];

/// Build the stock oracle: identical corpus + the three fts5vocab tables.
fn oracle_connection() -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    for stmt in CORPUS {
        conn.execute(stmt, []).unwrap();
    }
    conn.execute(
        "CREATE VIRTUAL TABLE v_row USING fts5vocab('ft', 'row')",
        [],
    )
    .unwrap();
    conn.execute(
        "CREATE VIRTUAL TABLE v_col USING fts5vocab('ft', 'col')",
        [],
    )
    .unwrap();
    conn.execute(
        "CREATE VIRTUAL TABLE v_inst USING fts5vocab('ft', 'instance')",
        [],
    )
    .unwrap();
    conn
}

/// Build the frankensqlite fixture: identical corpus + the three fts5vocab
/// tables. Returns an open in-memory connection.
async fn frank_connection() -> Connection {
    let conn = Connection::open(":memory:").await.unwrap();
    for stmt in CORPUS {
        conn.execute(stmt).await.unwrap();
    }
    conn.execute("CREATE VIRTUAL TABLE v_row USING fts5vocab('ft', 'row')")
        .await
        .unwrap();
    conn.execute("CREATE VIRTUAL TABLE v_col USING fts5vocab('ft', 'col')")
        .await
        .unwrap();
    conn.execute("CREATE VIRTUAL TABLE v_inst USING fts5vocab('ft', 'instance')")
        .await
        .unwrap();
    conn
}

fn compare(kind: &str, sql: &str) {
    asupersync::test_utils::run_test(|| async {
        let conn = frank_connection().await;
        let got = frank_rows(&conn.query(sql).await.unwrap());
        conn.close().await.unwrap();
        let oracle = oracle_connection();
        let want = sqlite_rows(&oracle, sql);
        assert!(
            !want.is_empty(),
            "{kind}: oracle produced no rows (bad fixture)"
        );
        assert_eq!(got, want, "{kind}: frank vs stock fts5vocab divergence");
    });
}

#[test]
fn fts5vocab_row_matches_stock() {
    compare("row", "SELECT * FROM v_row ORDER BY term");
}

#[test]
fn fts5vocab_col_matches_stock() {
    compare("col", "SELECT * FROM v_col ORDER BY term, col");
}

#[test]
fn fts5vocab_instance_matches_stock() {
    compare("instance", "SELECT * FROM v_inst ORDER BY term, doc, col, offset");
}

/// Projection of a subset of columns (not `SELECT *`) still matches stock.
#[test]
fn fts5vocab_row_projection_matches_stock() {
    compare("row-projection", "SELECT term, cnt FROM v_row ORDER BY term");
}

/// An unknown table type is rejected at CREATE, matching stock.
#[test]
fn fts5vocab_unknown_type_rejected() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE ft USING fts5(a, b);")
            .await
            .unwrap();
        let err = conn
            .execute("CREATE VIRTUAL TABLE bad USING fts5vocab('ft', 'bogus')")
            .await;
        assert!(err.is_err(), "unknown fts5vocab type must be rejected");
        conn.close().await.unwrap();
    });
}

/// The lazy on-disk read path: a file-backed FTS5 table reopened so its index
/// binds lazily still yields a vocabulary identical to stock.
#[test]
fn fts5vocab_lazy_on_disk_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fts5vocab_lazy.db");
        let db_str = db_path.to_string_lossy().into_owned();

        // Build and persist the corpus, then close so a later open binds lazily.
        {
            let conn = Connection::open(&db_str).await.unwrap();
            for stmt in CORPUS {
                conn.execute(stmt).await.unwrap();
            }
            conn.close().await.unwrap();
        }

        let conn = Connection::open(&db_str).await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE v_row USING fts5vocab('ft', 'row')")
            .await
            .unwrap();
        conn.execute("CREATE VIRTUAL TABLE v_inst USING fts5vocab('ft', 'instance')")
            .await
            .unwrap();

        let oracle = oracle_connection();
        for sql in [
            "SELECT * FROM v_row ORDER BY term",
            "SELECT * FROM v_inst ORDER BY term, doc, col, offset",
        ] {
            let got = frank_rows(&conn.query(sql).await.unwrap());
            let want = sqlite_rows(&oracle, sql);
            assert!(!want.is_empty(), "lazy: oracle produced no rows for {sql}");
            assert_eq!(got, want, "lazy: frank vs stock divergence for {sql}");
        }
        conn.close().await.unwrap();
    });
}
