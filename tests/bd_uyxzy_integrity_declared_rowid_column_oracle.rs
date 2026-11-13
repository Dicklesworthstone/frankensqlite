#![recursion_limit = "512"]
#![allow(clippy::too_many_lines)]

//! bd-uyxzy: FrankenSQLite's `PRAGMA integrity_check` must accept a table
//! whose declared column is named after a rowid alias (`rowid`, `oid`,
//! `_rowid_`) when that column appears in a partial-index predicate, an
//! index key expression or a generated column, exactly as stock SQLite does.
//!
//! Defect: integrity_check re-evaluates those expressions against the row
//! image with the hidden rowid appended as a pseudo column, and it always
//! named that pseudo column `rowid`. Next to a declared `rowid` column the
//! name was ambiguous, so `CREATE TABLE r(rowid INTEGER, value); CREATE INDEX
//! ri ON r(value) WHERE rowid > 0` made integrity_check fail with "internal
//! error: column not found: rowid" while stock reports `ok`.
//!
//! Every expectation comes from a live stock-SQLite (rusqlite, bundled)
//! oracle: stock builds the oracle file, the premises (stock integrity_check
//! is `ok`; each partial index read equals a `NOT INDEXED` scan with the same
//! predicate) are asserted on it, and FrankenSQLite must then report `ok` and
//! read the same index contents from a stock-created and from a
//! FrankenSQLite-created file.

use std::path::Path;

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// `r` declares `rowid` only, so `oid` and `_rowid_` still name the hidden
/// rowid; `g` and `r_expr` read the declared column. `o` declares `oid` and
/// `_rowid_`, leaving `rowid` hidden. `w` declares all three, so no alias
/// names the hidden rowid.
const SCHEMA: &[&str] = &[
    "CREATE TABLE r(rowid INTEGER, value INTEGER, g AS (rowid * 10))",
    "CREATE INDEX r_rowid ON r(value) WHERE rowid > 0",
    "CREATE INDEX r_oid ON r(value) WHERE oid > 0",
    "CREATE INDEX r_hidden ON r(value) WHERE _rowid_ > 1",
    "CREATE INDEX r_mixed ON r(value) WHERE rowid >= 0 AND oid > 1",
    "CREATE INDEX r_expr ON r(rowid + value)",
    "CREATE INDEX r_gen ON r(g) WHERE g > 0",
    "CREATE TABLE o(oid INTEGER, _rowid_ INTEGER, value INTEGER)",
    "CREATE INDEX o_rowid ON o(value) WHERE rowid > 1",
    "CREATE INDEX o_oid ON o(value) WHERE oid > 0",
    "CREATE INDEX o_hidden ON o(value) WHERE _rowid_ > 0",
    "CREATE TABLE w(rowid INTEGER, oid INTEGER, _rowid_ INTEGER, value INTEGER)",
    "CREATE INDEX w_all ON w(value) WHERE rowid > 0 AND oid > 0 AND _rowid_ > 0",
];

/// Rows on both sides of every predicate, declared alias columns differing
/// from the hidden rowid.
const SEED: &[&str] = &[
    "INSERT INTO r(rowid, value) VALUES (-1, 10), (0, 20), (5, 30)",
    "INSERT INTO r(oid, rowid, value) VALUES (-7, 3, 40)",
    "INSERT INTO o(rowid, oid, _rowid_, value) VALUES (1, 5, -5, 10), (2, -1, 7, 20), (-4, 2, 2, 30)",
    "INSERT INTO w(rowid, oid, _rowid_, value) VALUES (1, 1, 1, 10), (-1, 2, 3, 20), (4, 0, 4, 30)",
];

/// `(table, index, predicate, projection)`: an `INDEXED BY` read of each
/// partial index. The projection names the hidden rowid through an alias the
/// table does not shadow (`w` has none left, so it reads `value` only).
const PARTIAL_READS: &[(&str, &str, &str, &str)] = &[
    ("r", "r_rowid", "rowid > 0", "oid, rowid, value"),
    ("r", "r_oid", "oid > 0", "oid, rowid, value"),
    ("r", "r_hidden", "_rowid_ > 1", "oid, rowid, value"),
    (
        "r",
        "r_mixed",
        "rowid >= 0 AND oid > 1",
        "oid, rowid, value",
    ),
    ("r", "r_gen", "g > 0", "oid, g"),
    ("o", "o_rowid", "rowid > 1", "rowid, oid, _rowid_, value"),
    ("o", "o_oid", "oid > 0", "rowid, oid, _rowid_, value"),
    ("o", "o_hidden", "_rowid_ > 0", "rowid, oid, _rowid_, value"),
    (
        "w",
        "w_all",
        "rowid > 0 AND oid > 0 AND _rowid_ > 0",
        "rowid, oid, _rowid_, value",
    ),
];

type Rows = Vec<Vec<i64>>;

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

fn stock_integrity(db: &rusqlite::Connection) -> Vec<String> {
    db.prepare("PRAGMA integrity_check")
        .expect("prepare stock integrity_check")
        .query_map([], |row| row.get::<_, String>(0))
        .expect("run stock integrity_check")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("collect stock integrity_check")
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

/// The stock oracle's index reads, after asserting its premises.
fn oracle_reads(path: &Path) -> Vec<Rows> {
    stock_run(path, SCHEMA);
    stock_run(path, SEED);
    let db = rusqlite::Connection::open(path).expect("open oracle");
    assert_eq!(
        stock_integrity(&db),
        vec!["ok".to_owned()],
        "premise: the stock oracle must be intact"
    );
    PARTIAL_READS
        .iter()
        .map(|(table, index, predicate, projection)| {
            let read = stock_rows(&db, &read_sql(table, index, predicate, projection));
            let scan = stock_rows(
                &db,
                &format!(
                    "SELECT {projection} FROM {table} NOT INDEXED WHERE {predicate} ORDER BY 1, 2"
                ),
            );
            assert_eq!(
                read, scan,
                "premise: stock index {index} must hold exactly the rows its predicate selects"
            );
            read
        })
        .collect()
}

#[test]
fn bd_uyxzy_integrity_check_accepts_declared_rowid_alias_columns() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let oracle = oracle_reads(&dir.path().join("oracle.db"));
        // Oracle-verified pins: the declared `rowid` column selects rows by
        // its own value, `oid` and `_rowid_` by the hidden rowid.
        assert_eq!(oracle[0], vec![vec![-7, 3, 40], vec![3, 5, 30]]);
        assert_eq!(
            oracle[1],
            vec![vec![1, -1, 10], vec![2, 0, 20], vec![3, 5, 30]]
        );
        assert_eq!(oracle[2], vec![vec![2, 0, 20], vec![3, 5, 30]]);

        for origin in [Origin::Stock, Origin::Fsqlite] {
            let label = format!("{origin:?}");
            let path = dir.path().join(format!("subject_{origin:?}.db"));
            match origin {
                Origin::Stock => {
                    stock_run(&path, SCHEMA);
                    stock_run(&path, SEED);
                }
                Origin::Fsqlite => {
                    fsqlite_run(&path, SCHEMA, &label).await;
                    fsqlite_run(&path, SEED, &label).await;
                }
            }
            {
                let db = rusqlite::Connection::open(&path).expect("stock open subject");
                assert_eq!(
                    stock_integrity(&db),
                    vec!["ok".to_owned()],
                    "[{label}] stock integrity_check of the subject file"
                );
                for ((table, index, predicate, projection), expected) in
                    PARTIAL_READS.iter().zip(&oracle)
                {
                    assert_eq!(
                        &stock_rows(&db, &read_sql(table, index, predicate, projection)),
                        expected,
                        "[{label}] stock read of {index} differs from the oracle"
                    );
                }
            }

            let conn = Connection::open(path.to_str().expect("utf-8 temp path"))
                .await
                .unwrap_or_else(|e| panic!("[{label}] fsqlite open: {e}"));
            for pragma in [
                "PRAGMA integrity_check",
                "PRAGMA integrity_check(r)",
                "PRAGMA integrity_check(o)",
                "PRAGMA integrity_check(w)",
            ] {
                let report: Vec<String> = conn
                    .query(pragma)
                    .await
                    .unwrap_or_else(|e| panic!("[{label}] fsqlite `{pragma}`: {e}"))
                    .iter()
                    .map(|row| match &row.values()[0] {
                        SqliteValue::Text(text) => text.to_string(),
                        other => format!("{other:?}"),
                    })
                    .collect();
                assert_eq!(
                    report,
                    vec!["ok".to_owned()],
                    "[{label}] FrankenSQLite `{pragma}` must match stock's `ok`"
                );
            }
            for ((table, index, predicate, projection), expected) in
                PARTIAL_READS.iter().zip(&oracle)
            {
                let sql = read_sql(table, index, predicate, projection);
                let rows: Rows = conn
                    .query(&sql)
                    .await
                    .unwrap_or_else(|e| panic!("[{label}] fsqlite `{sql}`: {e}"))
                    .iter()
                    .map(|row| row.values().iter().map(|v| integer(v, &sql)).collect())
                    .collect();
                assert_eq!(
                    &rows, expected,
                    "[{label}] FrankenSQLite read of {index} differs from the stock oracle"
                );
            }
            conn.close()
                .await
                .unwrap_or_else(|e| panic!("[{label}] fsqlite close: {e}"));
        }
    });
}
