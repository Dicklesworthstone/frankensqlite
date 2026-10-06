#![recursion_limit = "512"]
#![allow(clippy::too_many_lines)]

//! bd-q6sdy: partial indexes whose `WHERE` names a hidden rowid alias
//! (`rowid`, `_rowid_`, `oid`) must be maintained by FrankenSQLite DML exactly
//! as stock SQLite maintains them.
//!
//! Defect: the write paths build a new index entry against the row image held
//! in registers, and that register image carried no rowid, so every rowid
//! alias in the partial-index predicate evaluated to NULL. The predicate was
//! therefore never true, and INSERT (and the re-insert half of UPDATE) silently
//! skipped the index entry. Stock `PRAGMA integrity_check` then reported
//! "row N missing from index ..." and stock `INDEXED BY` reads lost rows.
//!
//! Every expectation here comes from a live stock-SQLite (rusqlite, bundled)
//! oracle: the oracle database runs the whole statement sequence in stock
//! SQLite, the subject database runs the DML under test in FrankenSQLite, and
//! stock then inspects both files. The handful of literal row lists pinned
//! below are asserted against the oracle first, so they are oracle-verified.

use std::path::{Path, PathBuf};

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// `docs` has no INTEGER PRIMARY KEY, so the aliases name the hidden rowid.
/// `notes` has one, so the aliases name the same key as `id`. `docs_mixed`
/// combines a rowid term with a column term, so updates of `value` alone can
/// move a row across its predicate.
const SCHEMA: &[&str] = &[
    "CREATE TABLE docs(value INTEGER)",
    "CREATE INDEX docs_rowid ON docs(value) WHERE rowid > 0",
    "CREATE INDEX docs_hidden_rowid ON docs(value) WHERE _rowid_ > 0",
    "CREATE INDEX docs_oid ON docs(value) WHERE oid > 0",
    "CREATE INDEX docs_mixed ON docs(value) WHERE oid > 0 AND value < 100",
    "CREATE TABLE notes(id INTEGER PRIMARY KEY, value INTEGER)",
    "CREATE INDEX notes_rowid ON notes(value) WHERE rowid > 0",
    "CREATE INDEX notes_hidden_rowid ON notes(value) WHERE _rowid_ > 0",
    "CREATE INDEX notes_oid ON notes(value) WHERE oid > 0",
];

/// `(table, index, predicate)`: a query whose WHERE is exactly the index
/// predicate is the one shape stock lets `INDEXED BY` use a partial index for.
const PARTIAL_INDEXES: &[(&str, &str, &str)] = &[
    ("docs", "docs_rowid", "rowid > 0"),
    ("docs", "docs_hidden_rowid", "_rowid_ > 0"),
    ("docs", "docs_oid", "oid > 0"),
    ("docs", "docs_mixed", "oid > 0 AND value < 100"),
    ("notes", "notes_rowid", "rowid > 0"),
    ("notes", "notes_hidden_rowid", "_rowid_ > 0"),
    ("notes", "notes_oid", "oid > 0"),
];

const TABLES: &[&str] = &["docs", "notes"];

/// The reported repro: one row written before FrankenSQLite touches the file.
const SEED_ONE_ROW: &[&str] = &[
    "INSERT INTO docs(value) VALUES (7)",
    "INSERT INTO notes(value) VALUES (7)",
];

/// Rows on both sides of every predicate: rowid -3 is outside all of them and
/// value 150 is outside `docs_mixed` only.
const SEED_MIXED_ROWS: &[&str] = &[
    "INSERT INTO docs(rowid, value) VALUES (1, 7), (2, 8), (-3, 9), (4, 150)",
    "INSERT INTO notes(id, value) VALUES (1, 7), (2, 8), (-3, 9), (4, 150)",
];

/// Every INSERT codegen shape: auto rowid, explicit rowid outside and inside
/// the predicate, multi-row VALUES, and INSERT ... SELECT.
const INSERT_STEPS: &[&str] = &[
    "INSERT INTO docs(value) VALUES (8)",
    "INSERT INTO docs(rowid, value) VALUES (-3, 9)",
    "INSERT INTO docs(rowid, value) VALUES (40, 150), (41, 11)",
    "INSERT INTO docs(value) SELECT value + 20 FROM docs WHERE value < 9 ORDER BY rowid",
    "INSERT INTO notes(value) VALUES (8)",
    "INSERT INTO notes(id, value) VALUES (-3, 9)",
    "INSERT INTO notes(rowid, value) VALUES (40, 150), (41, 11)",
    "INSERT INTO notes(value) SELECT value + 20 FROM notes WHERE value < 9 ORDER BY rowid",
];

/// UPDATEs that move rows out of (`-rowid`) and into (`rowid = 30`) the rowid
/// predicates, re-key rows that stay inside, move rows across `docs_mixed` by
/// `value` alone, and REPLACE rows through the INTEGER PRIMARY KEY.
const UPDATE_STEPS: &[&str] = &[
    "UPDATE docs SET rowid = -rowid WHERE value = 8",
    "UPDATE docs SET rowid = 30 WHERE value = 9",
    "UPDATE docs SET value = value + 100 WHERE value = 7",
    "UPDATE docs SET value = 60 WHERE value = 150",
    "UPDATE docs SET value = value + 1",
    "UPDATE notes SET id = -id WHERE value = 8",
    "UPDATE notes SET rowid = 30 WHERE value = 9",
    "UPDATE notes SET value = value + 100 WHERE value = 7",
    "UPDATE notes SET value = value + 1",
    "INSERT OR REPLACE INTO notes(id, value) VALUES (4, 70)",
    "REPLACE INTO notes(id, value) VALUES (-2, 5)",
];

/// DELETEs of rows inside and outside the predicates.
const DELETE_STEPS: &[&str] = &[
    "DELETE FROM docs WHERE value = 8",
    "DELETE FROM docs WHERE rowid < 0",
    "DELETE FROM docs WHERE value > 100",
    "DELETE FROM notes WHERE id = 1",
    "DELETE FROM notes WHERE oid < 0",
];

#[derive(Clone, Copy, Debug)]
enum Origin {
    /// Stock SQLite creates the schema and the seed rows.
    Stock,
    /// FrankenSQLite creates the schema and the seed rows.
    Fsqlite,
}

type Rows = Vec<(i64, i64)>;

/// What stock SQLite observes in one database file.
#[derive(Debug, PartialEq, Eq)]
struct StockView {
    integrity: Vec<String>,
    table_rows: Vec<(String, Rows)>,
    index_reads: Vec<(String, Rows)>,
}

fn index_read_sql(table: &str, index: &str, predicate: &str) -> String {
    format!(
        "SELECT rowid, value FROM {table} INDEXED BY {index} WHERE {predicate} \
         ORDER BY value, rowid"
    )
}

fn stock_rows(db: &rusqlite::Connection, sql: &str) -> Rows {
    db.prepare(sql)
        .unwrap_or_else(|e| panic!("stock prepare `{sql}`: {e}"))
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
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
    let table_rows = TABLES
        .iter()
        .map(|table| {
            let sql = format!("SELECT rowid, value FROM {table} ORDER BY rowid");
            ((*table).to_owned(), stock_rows(&db, &sql))
        })
        .collect();
    let index_reads = PARTIAL_INDEXES
        .iter()
        .map(|(table, index, predicate)| {
            let sql = index_read_sql(table, index, predicate);
            ((*index).to_owned(), stock_rows(&db, &sql))
        })
        .collect();
    StockView {
        integrity,
        table_rows,
        index_reads,
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

/// FrankenSQLite's own view of the file: its integrity_check and the same
/// `INDEXED BY` reads stock runs.
async fn fsqlite_view(path: &Path, label: &str) -> (Vec<String>, Vec<(String, Rows)>) {
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
    let mut index_reads = Vec::new();
    for (table, index, predicate) in PARTIAL_INDEXES {
        let sql = index_read_sql(table, index, predicate);
        let rows = conn
            .query(&sql)
            .await
            .unwrap_or_else(|e| panic!("[{label}] fsqlite `{sql}`: {e}"))
            .iter()
            .map(|row| {
                (
                    integer(&row.values()[0], &sql),
                    integer(&row.values()[1], &sql),
                )
            })
            .collect();
        index_reads.push(((*index).to_owned(), rows));
    }
    conn.close()
        .await
        .unwrap_or_else(|e| panic!("[{label}] fsqlite close after reads: {e}"));
    (integrity, index_reads)
}

/// Stock runs schema + seed + steps on an oracle file. On the subject file,
/// `origin` decides who runs schema + seed, and FrankenSQLite runs the steps.
/// Stock and FrankenSQLite must then see the oracle's index contents in the
/// subject file, and stock's integrity_check must be `ok`.
async fn assert_fsqlite_steps_match_stock(
    label: &str,
    origin: Origin,
    seed: &[&str],
    steps: &[&str],
) -> StockView {
    let dir = tempfile::tempdir().expect("tempdir");
    let oracle_path: PathBuf = dir.path().join(format!("{label}_oracle.db"));
    let subject_path: PathBuf = dir.path().join(format!("{label}_subject.db"));

    stock_run(&oracle_path, SCHEMA);
    stock_run(&oracle_path, seed);
    stock_run(&oracle_path, steps);
    let oracle = stock_view(&oracle_path);
    assert_eq!(
        oracle.integrity,
        vec!["ok".to_owned()],
        "[{label}] premise: the stock oracle must be intact"
    );
    // Premise: each partial index holds exactly the rows a full scan with the
    // same predicate returns, so an index read is a complete membership check.
    {
        let db = rusqlite::Connection::open(&oracle_path).expect("open oracle");
        for (table, index, predicate) in PARTIAL_INDEXES {
            let scan = stock_rows(
                &db,
                &format!(
                    "SELECT rowid, value FROM {table} NOT INDEXED WHERE {predicate} \
                     ORDER BY value, rowid"
                ),
            );
            let read = &oracle
                .index_reads
                .iter()
                .find(|(name, _)| name == index)
                .expect("oracle read for every index")
                .1;
            assert_eq!(
                read, &scan,
                "[{label}] premise: stock index {index} must match its table scan"
            );
        }
    }

    match origin {
        Origin::Stock => {
            stock_run(&subject_path, SCHEMA);
            stock_run(&subject_path, seed);
        }
        Origin::Fsqlite => {
            fsqlite_run(&subject_path, SCHEMA, label).await;
            fsqlite_run(&subject_path, seed, label).await;
        }
    }
    fsqlite_run(&subject_path, steps, label).await;

    let subject = stock_view(&subject_path);
    assert_eq!(
        subject.integrity,
        vec!["ok".to_owned()],
        "[{label}] stock integrity_check after FrankenSQLite DML ({origin:?}-created file)"
    );
    assert_eq!(
        subject.table_rows, oracle.table_rows,
        "[{label}] table rows after FrankenSQLite DML differ from stock"
    );
    assert_eq!(
        subject.index_reads, oracle.index_reads,
        "[{label}] stock INDEXED BY reads after FrankenSQLite DML differ from the stock oracle"
    );

    let (fsqlite_integrity, fsqlite_reads) = fsqlite_view(&subject_path, label).await;
    assert_eq!(
        fsqlite_integrity,
        vec!["ok".to_owned()],
        "[{label}] FrankenSQLite integrity_check after its own DML"
    );
    assert_eq!(
        fsqlite_reads, oracle.index_reads,
        "[{label}] FrankenSQLite INDEXED BY reads differ from the stock oracle"
    );

    oracle
}

fn oracle_read<'a>(view: &'a StockView, index: &str) -> &'a Rows {
    &view
        .index_reads
        .iter()
        .find(|(name, _)| name == index)
        .unwrap_or_else(|| panic!("no oracle read for {index}"))
        .1
}

/// The reported repro, plus every INSERT codegen shape: stock creates the file
/// and inserts 7, FrankenSQLite inserts the rest.
#[test]
fn bd_q6sdy_fsqlite_insert_into_stock_file_maintains_rowid_alias_partial_indexes() {
    asupersync::test_utils::run_test(|| async {
        let minimal = assert_fsqlite_steps_match_stock(
            "repro",
            Origin::Stock,
            SEED_ONE_ROW,
            &["INSERT INTO docs(value) VALUES (8)"],
        )
        .await;
        // Oracle-verified pin of the reported shape: [7, 8], not [7].
        for index in ["docs_rowid", "docs_hidden_rowid", "docs_oid", "docs_mixed"] {
            assert_eq!(oracle_read(&minimal, index), &vec![(1, 7), (2, 8)]);
        }

        let full = assert_fsqlite_steps_match_stock(
            "stock_insert",
            Origin::Stock,
            SEED_ONE_ROW,
            INSERT_STEPS,
        )
        .await;
        assert_eq!(
            oracle_read(&full, "docs_oid"),
            &vec![(1, 7), (2, 8), (41, 11), (42, 27), (43, 28), (40, 150)],
            "oracle pin: rowid -3 stays out, every positive rowid is indexed"
        );
        assert_eq!(
            oracle_read(&full, "docs_mixed"),
            &vec![(1, 7), (2, 8), (41, 11), (42, 27), (43, 28)]
        );
    });
}

/// The same inserts on a file FrankenSQLite created from scratch.
#[test]
fn bd_q6sdy_fsqlite_created_file_maintains_rowid_alias_partial_indexes() {
    asupersync::test_utils::run_test(|| async {
        let view = assert_fsqlite_steps_match_stock(
            "fsqlite_insert",
            Origin::Fsqlite,
            SEED_ONE_ROW,
            INSERT_STEPS,
        )
        .await;
        assert_eq!(
            oracle_read(&view, "notes_rowid"),
            &vec![(1, 7), (2, 8), (41, 11), (42, 27), (43, 28), (40, 150)]
        );
    });
}

/// UPDATEs moving rows into and out of the predicates, on both a stock-created
/// and a FrankenSQLite-created file.
#[test]
fn bd_q6sdy_update_moves_rows_across_rowid_alias_partial_predicates() {
    asupersync::test_utils::run_test(|| async {
        for origin in [Origin::Stock, Origin::Fsqlite] {
            let label = format!("update_{origin:?}").to_ascii_lowercase();
            let view =
                assert_fsqlite_steps_match_stock(&label, origin, SEED_MIXED_ROWS, UPDATE_STEPS)
                    .await;
            assert_eq!(
                oracle_read(&view, "docs_rowid"),
                &vec![(30, 10), (4, 61), (1, 108)],
                "oracle pin: rowid 2 moved out (-2), rowid -3 moved in (30)"
            );
            assert_eq!(oracle_read(&view, "docs_mixed"), &vec![(30, 10), (4, 61)]);
            assert_eq!(
                oracle_read(&view, "notes_oid"),
                &vec![(30, 10), (4, 70), (1, 108)]
            );
        }
    });
}

/// DELETEs of rows inside and outside the predicates, on both a stock-created
/// and a FrankenSQLite-created file.
#[test]
fn bd_q6sdy_delete_keeps_rowid_alias_partial_indexes_consistent() {
    asupersync::test_utils::run_test(|| async {
        for origin in [Origin::Stock, Origin::Fsqlite] {
            let label = format!("delete_{origin:?}").to_ascii_lowercase();
            let view =
                assert_fsqlite_steps_match_stock(&label, origin, SEED_MIXED_ROWS, DELETE_STEPS)
                    .await;
            assert_eq!(oracle_read(&view, "docs_rowid"), &vec![(1, 7)]);
            assert_eq!(
                oracle_read(&view, "notes_hidden_rowid"),
                &vec![(2, 8), (4, 150)]
            );
        }
    });
}

/// A UNIQUE partial index over a rowid predicate: duplicates inside the
/// predicate must raise, a duplicate outside it is allowed, and an UPSERT
/// whose target names the same predicate must arbitrate on it.
#[test]
fn bd_q6sdy_unique_partial_index_on_rowid_predicate_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        const SCHEMA_AND_SEED: &[&str] = &[
            "CREATE TABLE u(value INTEGER)",
            "CREATE UNIQUE INDEX u_value ON u(value) WHERE rowid > 0",
            "INSERT INTO u(value) VALUES (7)",
        ];
        const STEPS: &[&str] = &[
            "INSERT INTO u(value) VALUES (7)",
            "INSERT INTO u(rowid, value) VALUES (-1, 7)",
            "INSERT INTO u(value) VALUES (7) ON CONFLICT(value) WHERE rowid > 0 DO NOTHING",
            "INSERT INTO u(value) VALUES (7) ON CONFLICT(value) WHERE rowid > 0 \
             DO UPDATE SET value = excluded.value + 1",
            "INSERT INTO u(value) VALUES (7)",
            "UPDATE u SET rowid = 5 WHERE rowid = -1",
            "INSERT INTO u(value) VALUES (8)",
        ];
        let read_sql = "SELECT rowid, value FROM u INDEXED BY u_value WHERE rowid > 0 \
                        ORDER BY value, rowid";
        let rows_sql = "SELECT rowid, value FROM u ORDER BY rowid";

        let dir = tempfile::tempdir().expect("tempdir");
        let oracle_path = dir.path().join("unique_oracle.db");
        stock_run(&oracle_path, SCHEMA_AND_SEED);
        let oracle_outcomes: Vec<Result<(), String>> = {
            let db = rusqlite::Connection::open(&oracle_path).expect("open oracle");
            STEPS
                .iter()
                .map(|sql| db.execute_batch(sql).map_err(|e| e.to_string()))
                .collect()
        };
        // Oracle-verified pins: the duplicates inside the predicate (plain
        // INSERTs and the UPDATE that moves the -1 duplicate into it) are
        // UNIQUE violations; the duplicate at rowid -1 and both UPSERTs are not.
        let failing: Vec<usize> = oracle_outcomes
            .iter()
            .enumerate()
            .filter(|(_, outcome)| outcome.is_err())
            .map(|(i, _)| i)
            .collect();
        assert_eq!(
            failing,
            vec![0, 5, 6],
            "oracle outcomes: {oracle_outcomes:?}"
        );
        for i in &failing {
            assert_eq!(
                oracle_outcomes[*i].as_ref().unwrap_err(),
                "UNIQUE constraint failed: u.value"
            );
        }
        let (oracle_rows, oracle_read, oracle_integrity) = {
            let db = rusqlite::Connection::open(&oracle_path).expect("open oracle");
            let integrity: String = db
                .query_row("PRAGMA integrity_check", [], |row| row.get(0))
                .expect("oracle integrity_check");
            (
                stock_rows(&db, rows_sql),
                stock_rows(&db, read_sql),
                integrity,
            )
        };
        assert_eq!(oracle_integrity, "ok");
        assert_eq!(oracle_rows, vec![(-1, 7), (1, 8), (2, 7)]);
        assert_eq!(oracle_read, vec![(2, 7), (1, 8)]);

        for origin in [Origin::Stock, Origin::Fsqlite] {
            let label = format!("unique_{origin:?}").to_ascii_lowercase();
            let subject_path = dir.path().join(format!("{label}.db"));
            match origin {
                Origin::Stock => stock_run(&subject_path, SCHEMA_AND_SEED),
                Origin::Fsqlite => fsqlite_run(&subject_path, SCHEMA_AND_SEED, &label).await,
            }
            let conn = Connection::open(subject_path.to_str().expect("utf-8 temp path"))
                .await
                .expect("fsqlite open");
            for (sql, expected) in STEPS.iter().zip(&oracle_outcomes) {
                let outcome = conn
                    .execute(sql)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string());
                match (expected, &outcome) {
                    (Ok(()), Ok(())) => {}
                    (Err(stock), Err(ours)) => assert!(
                        ours.contains(stock.as_str()),
                        "[{label}] `{sql}`: stock error {stock:?}, fsqlite error {ours:?}"
                    ),
                    _ => panic!("[{label}] `{sql}`: stock {expected:?}, fsqlite {outcome:?}"),
                }
            }
            conn.close().await.expect("fsqlite close");

            let db = rusqlite::Connection::open(&subject_path).expect("stock open subject");
            let integrity: String = db
                .query_row("PRAGMA integrity_check", [], |row| row.get(0))
                .expect("stock integrity_check");
            assert_eq!(integrity, "ok", "[{label}] stock integrity_check");
            assert_eq!(stock_rows(&db, rows_sql), oracle_rows, "[{label}] rows");
            assert_eq!(
                stock_rows(&db, read_sql),
                oracle_read,
                "[{label}] stock INDEXED BY read"
            );
        }
    });
}
