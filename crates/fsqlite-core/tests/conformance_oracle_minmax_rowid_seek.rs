//! Conformance oracle — `MAX(rowid)` / `MIN(rowid)` leaf-seek fast path.
//!
//! Verifies the planner/codegen special-case that lowers
//! `SELECT MAX(id) FROM t` (and `MIN`, and the `COALESCE(MAX(id), 0)` wrapper)
//! on an INTEGER PRIMARY KEY to a single seek of the rightmost/leftmost leaf
//! (O(log n)) instead of a full B-tree walk. This is a pure performance fix:
//!
//! * Differential checks below prove the *results* are byte-identical to stock
//!   SQLite (via `rusqlite`) on empty and on 50k-row tables.
//! * The bounded-read check disassembles the compiled program and asserts the
//!   table is visited with a single `Last`/`Rewind` and **no `Next`** — i.e. the
//!   walk is bounded to one leaf, not the whole table.
//!
//! Shape #2 (`SELECT col FROM t ORDER BY indexed_col LIMIT 1`) was already
//! O(log n) via the index-ordered scan; it is exercised here too as a guard.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Compare frankensqlite and rusqlite results for each query; collect mismatches.
fn oracle_compare(
    fconn: &Connection,
    rconn: &rusqlite::Connection,
    queries: &[&str],
) -> Vec<String> {
    let mut mismatches = Vec::new();
    for query in queries {
        let frank_result = fconn.query(query);
        let csql_result: std::result::Result<Vec<Vec<String>>, String> = (|| {
            let mut stmt = rconn.prepare(query).map_err(|e| format!("prepare: {e}"))?;
            let col_count = stmt.column_count();
            let rows: Vec<Vec<String>> = stmt
                .query_map([], |row| {
                    let mut vals = Vec::new();
                    for i in 0..col_count {
                        let v: rusqlite::types::Value = row.get_unwrap(i);
                        let s = match v {
                            rusqlite::types::Value::Null => "NULL".to_owned(),
                            rusqlite::types::Value::Integer(n) => n.to_string(),
                            rusqlite::types::Value::Real(f) => format!("{f}"),
                            rusqlite::types::Value::Text(s) => format!("'{s}'"),
                            rusqlite::types::Value::Blob(b) => format!(
                                "X'{}'",
                                b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                            ),
                        };
                        vals.push(s);
                    }
                    Ok(vals)
                })
                .map_err(|e| format!("query: {e}"))?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| format!("row: {e}"))?;
            Ok(rows)
        })();
        match (frank_result, csql_result) {
            (Ok(rows), Ok(csql_rows)) => {
                let frank_strs: Vec<Vec<String>> = rows
                    .iter()
                    .map(|row| {
                        row.values()
                            .iter()
                            .map(|v| match v {
                                SqliteValue::Null => "NULL".to_owned(),
                                SqliteValue::Integer(n) => n.to_string(),
                                SqliteValue::Float(f) => format!("{f}"),
                                SqliteValue::Text(s) => format!("'{s}'"),
                                SqliteValue::Blob(b) => format!(
                                    "X'{}'",
                                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                                ),
                            })
                            .collect()
                    })
                    .collect();
                if frank_strs != csql_rows {
                    mismatches.push(format!(
                        "MISMATCH: {query}\n  frank: {frank_strs:?}\n  csql:  {csql_rows:?}"
                    ));
                }
            }
            (Ok(_), Err(csql_err)) => {
                mismatches.push(format!(
                    "DIVERGE: {query}\n  frank: OK\n  csql:  ERROR({csql_err})"
                ));
            }
            (Err(e), Ok(csql_rows)) => {
                mismatches.push(format!(
                    "PAIR_FRANK_ERROR[{query}]\n  frank: ERROR({e})\n  csql:  {csql_rows:?}"
                ));
            }
            (Err(frank_err), Err(csql_err)) => {
                mismatches.push(format!(
                    "BOTH_ERROR: {query}\n  frank: ERROR({frank_err})\n  csql:  ERROR({csql_err})"
                ));
            }
        }
    }
    mismatches
}

fn assert_no_mismatches(mismatches: &[String], label: &str) {
    if !mismatches.is_empty() {
        for m in mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} {label} mismatch(es)", mismatches.len());
    }
}

const SETUP_DDL: &[&str] = &[
    "CREATE TABLE messages (id INTEGER PRIMARY KEY, body TEXT, ts INTEGER)",
    "CREATE INDEX idx_ts ON messages(ts)",
];

const QUERIES: &[&str] = &[
    "SELECT MAX(id) FROM messages",
    "SELECT MIN(id) FROM messages",
    "SELECT COALESCE(MAX(id), 0) FROM messages",
    "SELECT COALESCE(MIN(id), 0) FROM messages",
    "SELECT id FROM messages ORDER BY ts ASC LIMIT 1",
    "SELECT id FROM messages ORDER BY ts DESC LIMIT 1",
    "SELECT ts FROM messages ORDER BY ts ASC LIMIT 1",
    "SELECT ts FROM messages ORDER BY ts DESC LIMIT 1",
];

fn new_pair() -> (Connection, rusqlite::Connection) {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();
    for s in SETUP_DDL {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }
    (fconn, rconn)
}

/// Empty table: MAX/MIN → NULL; COALESCE wrappers → 0; ORDER BY LIMIT 1 → no row.
#[test]
fn minmax_rowid_seek_empty_table_matches_stock() {
    let (fconn, rconn) = new_pair();
    let m = oracle_compare(&fconn, &rconn, QUERIES);
    assert_no_mismatches(&m, "minmax_rowid_seek_empty");
}

/// 50k-row table: extrema and ordered-LIMIT probes match stock SQLite exactly.
#[test]
fn minmax_rowid_seek_populated_matches_stock() {
    let (fconn, rconn) = new_pair();
    // Non-monotonic rowid/ts ordering so MAX(id) != last-inserted and the
    // secondary index extremum differs from the rowid extremum.
    for i in 0..50_000_i64 {
        // Interleave so the largest id is not inserted last and ts is unsorted.
        let id = if i % 2 == 0 { i + 1 } else { 100_000 - i };
        let ts = (id * 7 + 13) % 99_991;
        let sql = format!("INSERT INTO messages(id, body, ts) VALUES ({id}, 'm{id}', {ts})");
        fconn.execute(&sql).unwrap();
        rconn.execute_batch(&sql).unwrap();
    }
    let m = oracle_compare(&fconn, &rconn, QUERIES);
    assert_no_mismatches(&m, "minmax_rowid_seek_populated");

    // Spot-check the canonical cass query shape returns a concrete value.
    let rows = fconn
        .query("SELECT COALESCE(MAX(id), 0) FROM messages")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0].values()[0], SqliteValue::Integer(n) if n > 0));
}

/// Single-row and small-table edge cases (boundary between empty and many).
#[test]
fn minmax_rowid_seek_single_row_matches_stock() {
    let (fconn, rconn) = new_pair();
    let insert = "INSERT INTO messages(id, body, ts) VALUES (42, 'only', 7)";
    fconn.execute(insert).unwrap();
    rconn.execute_batch(insert).unwrap();
    let m = oracle_compare(&fconn, &rconn, QUERIES);
    assert_no_mismatches(&m, "minmax_rowid_seek_single_row");
}

/// Bounded-read proof: the compiled program for `MAX(id)`/`MIN(id)` must seek a
/// single leaf and never emit `Next` over the table cursor — i.e. it reads one
/// leaf, not O(n) rows. Without the fast path the disassembly contains both
/// `Rewind` and `Next` (the full-walk loop).
#[test]
fn minmax_rowid_seek_is_bounded_to_one_leaf() {
    let fconn = Connection::open(":memory:").unwrap();
    for s in SETUP_DDL {
        fconn.execute(s).unwrap();
    }
    for i in 1..=50_000_i64 {
        fconn
            .execute(&format!(
                "INSERT INTO messages(id, body, ts) VALUES ({i}, 'm{i}', {})",
                100_000 - i
            ))
            .unwrap();
    }

    // MAX(id): must use `Last`, must NOT contain `Next`.
    let max_dis = fconn
        .prepare("SELECT MAX(id) FROM messages")
        .unwrap()
        .explain();
    assert!(
        max_dis.contains("Last"),
        "MAX(id) must seek the rightmost leaf via Last; got:\n{max_dis}"
    );
    assert!(
        !max_dis.contains("Next"),
        "MAX(id) must not walk the table with Next (O(n)); got:\n{max_dis}"
    );

    // COALESCE(MAX(id), 0): same — wrapper does not reintroduce a scan.
    let coalesce_dis = fconn
        .prepare("SELECT COALESCE(MAX(id), 0) FROM messages")
        .unwrap()
        .explain();
    assert!(
        coalesce_dis.contains("Last") && !coalesce_dis.contains("Next"),
        "COALESCE(MAX(id),0) must be a single leaf seek; got:\n{coalesce_dis}"
    );

    // MIN(id): seeks the leftmost leaf via `Rewind`, no `Next` loop.
    let min_dis = fconn
        .prepare("SELECT MIN(id) FROM messages")
        .unwrap()
        .explain();
    assert!(
        min_dis.contains("Rewind") && !min_dis.contains("Next"),
        "MIN(id) must seek the leftmost leaf without a Next loop; got:\n{min_dis}"
    );
}

/// Negative guard: the fast path must NOT fire when it would change semantics.
/// These must keep the full-scan loop (`Next`) and still match stock SQLite.
#[test]
fn minmax_rowid_seek_does_not_fire_when_unsafe() {
    let (fconn, rconn) = new_pair();
    for i in 1..=200_i64 {
        let sql = format!(
            "INSERT INTO messages(id, body, ts) VALUES ({i}, 'm{i}', {})",
            i % 5
        );
        fconn.execute(&sql).unwrap();
        rconn.execute_batch(&sql).unwrap();
    }

    // WHERE clause changes the extremum → must NOT use the unconditional seek.
    let where_dis = fconn
        .prepare("SELECT MAX(id) FROM messages WHERE ts = 2")
        .unwrap()
        .explain();
    assert!(
        where_dis.contains("Next"),
        "MAX(id) WHERE ... must still scan/filter; got:\n{where_dis}"
    );

    // MAX over a non-rowid column must not use the rowid leaf seek.  It may,
    // however, use the dedicated secondary index and seek directly to that
    // index's rightmost leaf.
    let nonrowid_dis = fconn
        .prepare("SELECT MAX(ts) FROM messages")
        .unwrap()
        .explain();
    assert!(
        nonrowid_dis.contains("(idx)idx_ts")
            && nonrowid_dis.contains("Last")
            && !nonrowid_dis.contains("Next"),
        "MAX(ts) (non-rowid) must use its secondary-index leaf seek, not the rowid path; got:\n{nonrowid_dis}"
    );

    // Differential correctness for the unsafe-but-must-match shapes.
    let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT MAX(id) FROM messages WHERE ts = 2",
            "SELECT MIN(id) FROM messages WHERE ts = 2",
            "SELECT MAX(ts) FROM messages",
            "SELECT MIN(ts) FROM messages",
            "SELECT MAX(id), MIN(id) FROM messages",
            "SELECT MAX(id) FROM messages GROUP BY ts ORDER BY ts",
            "SELECT COUNT(*), MAX(id) FROM messages",
        ],
    );
    assert_no_mismatches(&m, "minmax_rowid_seek_unsafe");
}
