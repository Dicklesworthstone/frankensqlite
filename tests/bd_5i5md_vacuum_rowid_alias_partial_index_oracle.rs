#![recursion_limit = "512"]
#![allow(clippy::too_many_lines)]

//! bd-5i5md: `VACUUM` and `VACUUM INTO` must rebuild a partial index whose
//! `WHERE` names a hidden rowid alias (`rowid`, `_rowid_`, `oid`) exactly as
//! stock SQLite does.
//!
//! Defect: the image writer evaluates each partial-index predicate against the
//! stored row with a column map that had no rowid entry. A rowid alias failed
//! to resolve, the error fell through to "include the row", and the rebuilt
//! index held every row. FrankenSQLite's pre-publication integrity_check then
//! refused the image ("index ... contains rowid -3 for a table row that does
//! not satisfy the partial index predicate"), so VACUUM was unusable for such
//! tables.
//!
//! Every expectation comes from a live stock-SQLite (rusqlite, bundled)
//! oracle: stock builds the same file and runs the same VACUUM, its premises
//! (integrity_check `ok`; every partial index read equals a `NOT INDEXED`
//! scan with the same predicate) are asserted, and the file FrankenSQLite
//! vacuums must then read back identically in stock and in FrankenSQLite.

use std::path::Path;

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// `docs` has no INTEGER PRIMARY KEY, so the aliases name the hidden rowid;
/// `docs_unique` admits the duplicate value 7 only outside its predicate.
/// `notes` has one, so the aliases name `id`. `r` declares a `rowid` column,
/// which shadows that alias only: `r_rowid` tests the column, `r_oid` the
/// hidden rowid.
const SCHEMA: &[&str] = &[
    "CREATE TABLE docs(value INTEGER)",
    "CREATE INDEX docs_rowid ON docs(value) WHERE rowid > 0",
    "CREATE INDEX docs_hidden ON docs(value) WHERE _rowid_ > 0",
    "CREATE INDEX docs_oid ON docs(value) WHERE oid > 0",
    "CREATE INDEX docs_mixed ON docs(value) WHERE oid > 0 AND value < 100",
    "CREATE UNIQUE INDEX docs_unique ON docs(value) WHERE rowid > 0",
    "CREATE TABLE notes(id INTEGER PRIMARY KEY, value INTEGER)",
    "CREATE INDEX notes_rowid ON notes(value) WHERE rowid > 0",
    "CREATE INDEX notes_oid ON notes(value) WHERE oid > 0 AND value < 100",
    "CREATE TABLE r(rowid INTEGER, value INTEGER)",
    "CREATE INDEX r_rowid ON r(value) WHERE rowid > 0",
    "CREATE INDEX r_oid ON r(value) WHERE oid > 0",
];

/// Rows on both sides of every predicate.
const SEED: &[&str] = &[
    "INSERT INTO docs(rowid, value) VALUES (1, 7), (2, 8), (-3, 9), (4, 150), (-5, 7)",
    "INSERT INTO notes(id, value) VALUES (1, 7), (2, 8), (-3, 9), (4, 150)",
    "INSERT INTO r(oid, rowid, value) VALUES (1, -1, 10), (-2, 5, 20), (3, 0, 30)",
];

const TABLE_DUMPS: &[&str] = &[
    "SELECT rowid, value FROM docs ORDER BY rowid",
    "SELECT id, value FROM notes ORDER BY id",
    "SELECT oid, rowid, value FROM r ORDER BY oid",
];

/// `(table, index, predicate, projection)`: a read whose WHERE is exactly the
/// index predicate, the shape stock lets `INDEXED BY` use a partial index for.
const PARTIAL_READS: &[(&str, &str, &str, &str)] = &[
    ("docs", "docs_rowid", "rowid > 0", "rowid, value"),
    ("docs", "docs_hidden", "_rowid_ > 0", "rowid, value"),
    ("docs", "docs_oid", "oid > 0", "rowid, value"),
    (
        "docs",
        "docs_mixed",
        "oid > 0 AND value < 100",
        "rowid, value",
    ),
    ("docs", "docs_unique", "rowid > 0", "rowid, value"),
    ("notes", "notes_rowid", "rowid > 0", "id, value"),
    ("notes", "notes_oid", "oid > 0 AND value < 100", "id, value"),
    ("r", "r_rowid", "rowid > 0", "oid, rowid, value"),
    ("r", "r_oid", "oid > 0", "oid, rowid, value"),
];

type Rows = Vec<Vec<i64>>;

/// What stock SQLite observes in one database file.
#[derive(Debug, PartialEq, Eq)]
struct StockView {
    integrity: Vec<String>,
    tables: Vec<Rows>,
    partial_reads: Vec<Rows>,
}

#[derive(Clone, Copy, Debug)]
enum Origin {
    Stock,
    Fsqlite,
}

fn read_sql(table: &str, index: &str, predicate: &str, projection: &str) -> String {
    format!("SELECT {projection} FROM {table} INDEXED BY {index} WHERE {predicate} ORDER BY 1, 2")
}

fn stock_rows(db: &rusqlite::Connection, sql: &str) -> Rows {
    let mut stmt = db
        .prepare(sql)
        .unwrap_or_else(|e| panic!("stock prepare `{sql}`: {e}"));
    let width = stmt.column_count();
    stmt.query_map([], |row| {
        (0..width)
            .map(|i| row.get::<_, i64>(i))
            .collect::<rusqlite::Result<Vec<_>>>()
    })
    .unwrap_or_else(|e| panic!("stock query `{sql}`: {e}"))
    .collect::<rusqlite::Result<Vec<_>>>()
    .unwrap_or_else(|e| panic!("stock rows `{sql}`: {e}"))
}

fn stock_view(path: &Path) -> StockView {
    let db = rusqlite::Connection::open(path).expect("open with stock SQLite");
    let integrity = db
        .prepare("PRAGMA integrity_check")
        .expect("prepare stock integrity_check")
        .query_map([], |row| row.get::<_, String>(0))
        .expect("run stock integrity_check")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("collect stock integrity_check");
    StockView {
        integrity,
        tables: TABLE_DUMPS.iter().map(|sql| stock_rows(&db, sql)).collect(),
        partial_reads: PARTIAL_READS
            .iter()
            .map(|(table, index, predicate, projection)| {
                stock_rows(&db, &read_sql(table, index, predicate, projection))
            })
            .collect(),
    }
}

fn stock_run(path: &Path, statements: &[&str]) {
    let db = rusqlite::Connection::open(path).expect("open with stock SQLite");
    for sql in statements {
        db.execute_batch(sql)
            .unwrap_or_else(|e| panic!("stock `{sql}`: {e}"));
    }
}

async fn fsqlite_run(path: &Path, statements: &[&str], label: &str) {
    let conn = Connection::open(path.to_str().expect("utf-8 temp path"))
        .await
        .unwrap_or_else(|e| panic!("[{label}] fsqlite open: {e}"));
    for sql in statements {
        conn.execute(sql)
            .await
            .unwrap_or_else(|e| panic!("[{label}] fsqlite `{sql}`: {e}"));
    }
    conn.close()
        .await
        .unwrap_or_else(|e| panic!("[{label}] fsqlite close: {e}"));
}

fn integer(value: &SqliteValue, what: &str) -> i64 {
    match value {
        SqliteValue::Integer(n) => *n,
        other => panic!("{what}: expected INTEGER, got {other:?}"),
    }
}

/// FrankenSQLite's own view of a file: its integrity_check and the same
/// partial-index reads stock runs.
async fn fsqlite_view(path: &Path, label: &str) -> (Vec<String>, Vec<Rows>) {
    let conn = Connection::open(path.to_str().expect("utf-8 temp path"))
        .await
        .unwrap_or_else(|e| panic!("[{label}] fsqlite reopen: {e}"));
    let integrity = conn
        .query("PRAGMA integrity_check")
        .await
        .unwrap_or_else(|e| panic!("[{label}] fsqlite integrity_check: {e}"))
        .iter()
        .map(|row| match &row.values()[0] {
            SqliteValue::Text(text) => text.to_string(),
            other => format!("{other:?}"),
        })
        .collect();
    let mut reads = Vec::new();
    for (table, index, predicate, projection) in PARTIAL_READS {
        let sql = read_sql(table, index, predicate, projection);
        let rows: Rows = conn
            .query(&sql)
            .await
            .unwrap_or_else(|e| panic!("[{label}] fsqlite `{sql}`: {e}"))
            .iter()
            .map(|row| row.values().iter().map(|v| integer(v, &sql)).collect())
            .collect();
        reads.push(rows);
    }
    conn.close()
        .await
        .unwrap_or_else(|e| panic!("[{label}] fsqlite close after reads: {e}"));
    (integrity, reads)
}

/// Stock builds and vacuums the oracle file; its premises are asserted and
/// its view returned.
fn oracle_view(path: &Path) -> StockView {
    stock_run(path, SCHEMA);
    stock_run(path, SEED);
    stock_run(path, &["VACUUM"]);
    let view = stock_view(path);
    assert_eq!(
        view.integrity,
        vec!["ok".to_owned()],
        "premise: the stock oracle must be intact"
    );
    let db = rusqlite::Connection::open(path).expect("open oracle");
    for ((table, index, predicate, projection), read) in
        PARTIAL_READS.iter().zip(&view.partial_reads)
    {
        let scan = stock_rows(
            &db,
            &format!(
                "SELECT {projection} FROM {table} NOT INDEXED WHERE {predicate} ORDER BY 1, 2"
            ),
        );
        assert_eq!(
            read, &scan,
            "premise: stock index {index} must hold exactly the rows its predicate selects"
        );
    }
    view
}

async fn create_subject(path: &Path, origin: Origin, label: &str) {
    match origin {
        Origin::Stock => {
            stock_run(path, SCHEMA);
            stock_run(path, SEED);
        }
        Origin::Fsqlite => {
            fsqlite_run(path, SCHEMA, label).await;
            fsqlite_run(path, SEED, label).await;
        }
    }
}

async fn assert_vacuumed_file_matches_oracle(path: &Path, oracle: &StockView, label: &str) {
    let subject = stock_view(path);
    assert_eq!(
        subject.integrity,
        vec!["ok".to_owned()],
        "[{label}] stock integrity_check of the file FrankenSQLite vacuumed"
    );
    assert_eq!(
        subject.tables, oracle.tables,
        "[{label}] table rows after FrankenSQLite VACUUM differ from stock"
    );
    assert_eq!(
        subject.partial_reads, oracle.partial_reads,
        "[{label}] stock INDEXED BY reads after FrankenSQLite VACUUM differ from the stock oracle"
    );
    let (integrity, reads) = fsqlite_view(path, label).await;
    assert_eq!(
        integrity,
        vec!["ok".to_owned()],
        "[{label}] FrankenSQLite integrity_check after its own VACUUM"
    );
    assert_eq!(
        reads, oracle.partial_reads,
        "[{label}] FrankenSQLite INDEXED BY reads after VACUUM differ from the stock oracle"
    );
}

#[test]
fn bd_5i5md_vacuum_rebuilds_rowid_alias_partial_indexes() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let oracle = oracle_view(&dir.path().join("oracle.db"));
        // Oracle-verified pins: rowids -3 and -5 stay out of every hidden
        // rowid predicate; `r_rowid` follows the declared column instead.
        assert_eq!(
            oracle.partial_reads[0],
            vec![vec![1, 7], vec![2, 8], vec![4, 150]]
        );
        assert_eq!(oracle.partial_reads[3], vec![vec![1, 7], vec![2, 8]]);
        assert_eq!(oracle.partial_reads[7], vec![vec![-2, 5, 20]]);
        assert_eq!(
            oracle.partial_reads[8],
            vec![vec![1, -1, 10], vec![3, 0, 30]]
        );

        for origin in [Origin::Stock, Origin::Fsqlite] {
            let label = format!("vacuum_{origin:?}");
            let path = dir.path().join(format!("{label}.db"));
            create_subject(&path, origin, &label).await;
            fsqlite_run(&path, &["VACUUM"], &label).await;
            assert_vacuumed_file_matches_oracle(&path, &oracle, &label).await;
        }
    });
}

#[test]
fn bd_5i5md_vacuum_into_rebuilds_rowid_alias_partial_indexes() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let oracle = oracle_view(&dir.path().join("oracle.db"));

        for origin in [Origin::Stock, Origin::Fsqlite] {
            let label = format!("vacuum_into_{origin:?}");
            let source = dir.path().join(format!("{label}_source.db"));
            let output = dir.path().join(format!("{label}_output.db"));
            create_subject(&source, origin, &label).await;
            let vacuum_into = format!(
                "VACUUM INTO '{}'",
                output.to_str().expect("utf-8 temp path")
            );
            fsqlite_run(&source, &[vacuum_into.as_str()], &label).await;
            assert_vacuumed_file_matches_oracle(&output, &oracle, &label).await;
        }
    });
}
