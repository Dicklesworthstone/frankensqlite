#![recursion_limit = "512"]
#![allow(
    clippy::significant_drop_tightening,
    reason = "the process-global VDBE opcode counter needs each test serialized"
)]
//! bd-73em1: an explicit COLLATE equal to the index key's collation keeps the
//! index seek, as in stock. The connection used to drop the planner's index
//! directive whenever the WHERE clause held any explicit COLLATE, and codegen
//! takes no COLLATE-wrapped seek operand, so `w = 'ABC' COLLATE NOCASE` over a
//! NOCASE index scanned.
//!
//! The keeper counts the opcodes each SELECT executes on a file-backed table
//! of `ROWS` mixed-case rows: a seek executes a `Seek*` opcode and stays far
//! under `SEEK_LIMIT` opcodes, a scan runs far over it. Each answer, as a
//! multiset of rows, matches bundled SQLite 3.54.1 on `:memory:` and on a
//! file. The negatives must scan, where a wrong seek would drop or add rows:
//! COLLATE BINARY against a NOCASE index, COLLATE NOCASE against a BINARY
//! index, a plain BINARY comparison against a NOCASE index of a BINARY
//! column, and a BETWEEN whose bounds name different collations. The same
//! shapes without COLLATE are the controls that show the seek is seen.

use std::sync::{Mutex, MutexGuard};

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use fsqlite_vdbe::engine::{reset_vdbe_metrics, set_vdbe_metrics_enabled, vdbe_metrics_snapshot};

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".into(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f:?}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:02x?}"),
    }
}

fn render_stock(value: &rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".into(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f:?}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:02x?}"),
    }
}

/// The rows, sorted: without ORDER BY a seek and a scan return the same rows
/// in different orders.
fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut statement = conn.prepare(sql).expect("stock prepares");
    let columns = statement.column_count();
    let mut rows: Vec<Vec<String>> = statement
        .query_map([], |row| {
            Ok((0..columns)
                .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(Iterator::collect)
        .expect("stock runs");
    rows.sort();
    rows
}

async fn fsqlite_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    let mut rows: Vec<Vec<String>> = conn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("fsqlite `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render_fsqlite).collect())
        .collect();
    rows.sort();
    rows
}

const ROWS: u64 = 2000;

/// A seek visits a handful of rows (tens of opcodes each); a scan of `ROWS`
/// rows runs well over ten thousand.
const SEEK_LIMIT: u64 = 2000;

fn setup() -> String {
    // Each table holds the same mixed-case words: 'w17', 'W17', 'x17', ...
    let words = format!(
        "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < {ROWS}) \
         SELECT CASE i % 3 WHEN 0 THEN 'w' WHEN 1 THEN 'W' ELSE 'x' END || (i % 400), i \
         FROM s"
    );
    format!(
        "CREATE TABLE u(w TEXT COLLATE NOCASE, v INTEGER);\
         CREATE INDEX uw ON u(w);\
         INSERT INTO u {words};\
         CREATE TABLE b(w TEXT, v INTEGER);\
         CREATE INDEX bw ON b(w);\
         INSERT INTO b {words};\
         CREATE TABLE c(w TEXT, v INTEGER);\
         CREATE INDEX cw ON c(w COLLATE NOCASE);\
         INSERT INTO c {words};"
    )
}

/// (query, must seek)
const SHAPES: &[(&str, bool)] = &[
    ("SELECT w, v FROM u WHERE w = 'W17' COLLATE NOCASE", true),
    ("SELECT w, v FROM u WHERE w COLLATE NOCASE = 'w17'", true),
    ("SELECT w, v FROM b WHERE w = 'W17' COLLATE BINARY", true),
    (
        "SELECT w, v FROM b WHERE w > 'x398' COLLATE BINARY AND w < 'x39a'",
        true,
    ),
    (
        "SELECT w, v FROM b WHERE w BETWEEN 'W170' COLLATE BINARY AND 'W172'",
        true,
    ),
    (
        "SELECT w, v FROM b WHERE w COLLATE BINARY IN ('W17', 'x18')",
        true,
    ),
    // An IN list compares under its left operand's collation only: a COLLATE
    // on an item does not change it (stock), so this stays a BINARY probe.
    (
        "SELECT w, v FROM b WHERE w IN ('W17' COLLATE NOCASE, 'x18')",
        true,
    ),
    // The COLLATE is not the column's but is the index key's.
    ("SELECT w, v FROM c WHERE w = 'w17' COLLATE NOCASE", true),
    // A COLLATE on another comparison leaves the seek alone.
    (
        "SELECT w, v FROM b WHERE w = 'W17' AND 'X' = 'x' COLLATE NOCASE",
        true,
    ),
    // Negatives: these must scan.
    ("SELECT w, v FROM u WHERE w = 'W17' COLLATE BINARY", false),
    ("SELECT w, v FROM b WHERE w = 'W17' COLLATE NOCASE", false),
    ("SELECT w, v FROM c WHERE w = 'w17'", false),
    (
        "SELECT w, v FROM b WHERE w BETWEEN 'W170' COLLATE BINARY AND 'w172' COLLATE NOCASE",
        false,
    ),
    // Controls without COLLATE: these seek with and without the fix.
    ("SELECT w, v FROM u WHERE w = 'W17'", true),
    ("SELECT w, v FROM b WHERE w > 'x398' AND w < 'x39a'", true),
    ("SELECT w, v FROM b WHERE w IN ('W17', 'x18')", true),
];

#[test]
fn matching_explicit_collate_keeps_the_index_seek() {
    let _serial = serial();
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    stock.execute_batch(&setup()).unwrap();
    let expected: Vec<_> = SHAPES
        .iter()
        .map(|(sql, _)| stock_rows(&stock, sql))
        .collect();
    // Not vacuous: the collation decides which rows match.
    assert_eq!(expected[0].len(), 3, "{:?}", expected[0]);
    assert_eq!(expected[2].len(), 1, "{:?}", expected[2]);
    assert_eq!(expected[7].len(), 3, "{:?}", expected[7]);
    assert_eq!(expected[9].len(), 1, "{:?}", expected[9]);
    assert_eq!(expected[10].len(), 3, "{:?}", expected[10]);
    assert_eq!(expected[11].len(), 2, "{:?}", expected[11]);
    asupersync::test_utils::run_test(|| async move {
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = if file_backed {
                dir.path().join("collate.db").to_str().unwrap().to_owned()
            } else {
                ":memory:".to_owned()
            };
            let conn = Connection::open(&path).await.unwrap();
            conn.execute_batch(&setup()).await.unwrap();
            let mut report = Vec::new();
            let mut wrong = Vec::new();
            for ((sql, must_seek), want) in SHAPES.iter().zip(&expected) {
                set_vdbe_metrics_enabled(true);
                reset_vdbe_metrics();
                let got = fsqlite_rows(&conn, sql).await;
                let metrics = vdbe_metrics_snapshot();
                set_vdbe_metrics_enabled(false);
                assert_eq!(&got, want, "file_backed={file_backed}: {sql}");
                let opcodes = metrics.opcodes_executed_total;
                let count = |names: &[&str]| -> u64 {
                    metrics
                        .opcode_execution_totals
                        .iter()
                        .filter(|entry| names.contains(&entry.opcode.as_str()))
                        .map(|entry| entry.total)
                        .sum()
                };
                let seeks = count(&["SeekGE", "SeekGT", "SeekLE", "SeekLT"]);
                let index_bounds = count(&["IdxGE", "IdxGT", "IdxLE", "IdxLT"]);
                let rewinds = count(&["Rewind"]);
                report.push(format!(
                    "  {opcodes:>8} opcodes, {seeks} seeks, {index_bounds} index-bound checks, \
                     {rewinds} rewinds, must_seek={must_seek}: `{sql}`"
                ));
                let seeked = opcodes <= SEEK_LIMIT && seeks > 0;
                if file_backed && seeked != *must_seek {
                    wrong.push(format!(
                        "  {opcodes} opcodes, {seeks} seeks, must_seek={must_seek}: `{sql}`"
                    ));
                }
            }
            conn.close().await.unwrap();
            eprintln!(
                "bd-73em1: file_backed={file_backed} (seek limit {SEEK_LIMIT} opcodes):\n{}",
                report.join("\n")
            );
            assert!(
                wrong.is_empty(),
                "a seek executes a Seek opcode and stays under {SEEK_LIMIT} opcodes, a scan \
                 runs over it:\n{}",
                wrong.join("\n")
            );
        }
    });
}
