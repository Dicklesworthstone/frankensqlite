//! GitHub issue #113 — deterministic reproduction.
//!
//! Within a single connection, a table-scan read (`SELECT count(*)` /
//! `PRAGMA integrity_check`) **interleaved** with subsequent delete+reinsert
//! churn corrupts secondary-index b-trees: rows present in the table go missing
//! from their indexes. Canonical SQLite, reading the file fsqlite produced,
//! confirms the corruption (`row N missing from index ...`) and it yields wrong
//! query results (an indexed lookup returns fewer rows than a table scan).
//!
//! Bisected trigger (from the issue): the corruption needs (a) ≥3 secondary
//! indexes incl. a low-cardinality and a composite one, (b) ~8000 rows,
//! (c) ≥3 delete/reinsert cycles, and (d) ≥1 interleaved table-scan read in the
//! same live connection. Drop any of those and it stays clean.
//!
//! This test drives the churn through fsqlite (single-writer), then opens the
//! resulting file with canonical sqlite3 (rusqlite) and asserts
//! `PRAGMA integrity_check` is `ok` and that an indexed lookup agrees with a
//! table scan. It currently FAILS (corruption) and is `#[ignore]`d pending the
//! fix; remove the ignore once #113 is resolved.

use std::path::Path;
use tempfile::TempDir;

const ROWS: i64 = 8000;
const CYCLES: usize = 12;
const BATCH: i64 = 5600; // ~70% churn per cycle

/// Run the churn workload through a single fsqlite connection at `path`.
fn run_churn(path: &str) {
    let conn = fsqlite::Connection::open(path.to_owned()).expect("open fsqlite");
    // Single-writer / MVCC off, matching the issue's reproduction conditions.
    conn.execute("PRAGMA fsqlite.concurrent_mode = OFF;")
        .expect("concurrent_mode off");

    conn.execute_batch(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, j INTEGER, k INTEGER, m INTEGER);
         CREATE INDEX idx_j ON t(j);
         CREATE INDEX idx_k ON t(k);
         CREATE INDEX idx_km ON t(k, m);",
    )
    .expect("create schema");

    // Bulk-load ROWS rows (single-writer transaction).
    conn.execute("BEGIN IMMEDIATE").expect("begin load");
    for id in 1..=ROWS {
        conn.execute(&format!(
            "INSERT INTO t(id, j, k, m) VALUES ({id}, {j}, {k}, {m})",
            j = id,
            k = id % 10,
            m = id % 100,
        ))
        .expect("bulk insert");
    }
    conn.execute("COMMIT").expect("commit load");

    let mut next_id = ROWS + 1;
    let mut lo = 1i64;
    for cycle in 0..CYCLES {
        conn.execute("BEGIN IMMEDIATE")
            .unwrap_or_else(|e| panic!("begin cycle {cycle}: {e}"));

        // Delete a contiguous band of ~70% of the rows (oldest ids).
        let hi = lo + BATCH - 1;
        conn.execute(&format!("DELETE FROM t WHERE id BETWEEN {lo} AND {hi}"))
            .unwrap_or_else(|e| panic!("delete cycle {cycle}: {e}"));
        lo = hi + 1;

        // Reinsert a fresh batch at the high end.
        for _ in 0..BATCH {
            let id = next_id;
            conn.execute(&format!(
                "INSERT INTO t(id, j, k, m) VALUES ({id}, {j}, {k}, {m})",
                j = id,
                k = id % 10,
                m = id % 100,
            ))
            .unwrap_or_else(|e| panic!("reinsert cycle {cycle}: {e}"));
            next_id += 1;
        }

        // Interleaved table-scan read in the SAME connection, INSIDE the write
        // transaction. `WHERE +id >= 0` defeats fsqlite's cached row-count fast
        // path so this opens a real table-btree scan cursor (the bug's
        // precondition per the issue's bisection).
        let scanned = conn
            .query("SELECT count(*) FROM t WHERE +id >= 0")
            .unwrap_or_else(|e| panic!("interleaved scan cycle {cycle}: {e}"));
        assert_eq!(scanned.len(), 1, "scan returns one row");

        conn.execute("COMMIT")
            .unwrap_or_else(|e| panic!("commit cycle {cycle}: {e}"));

        // Frank's own integrity check between cycles (also a btree walk).
        if let Ok(rows) = conn.query("PRAGMA integrity_check") {
            if let Some(first) = rows.first() {
                let v = format!("{:?}", first.values().first());
                if !v.contains("ok") {
                    eprintln!("[diag] frank integrity_check cycle {cycle}: {v}");
                }
            }
        }
    }

    drop(conn);
}

/// Canonical sqlite3 integrity check over the fsqlite-produced file.
fn canonical_integrity_check(path: &Path) -> String {
    let conn = rusqlite::Connection::open(path).expect("open canonical");
    conn.query_row("PRAGMA integrity_check;", [], |row| row.get(0))
        .expect("integrity_check")
}

/// For each distinct `j`, an indexed lookup must agree with a forced table scan.
/// A mismatch means the secondary index silently dropped entries.
fn canonical_index_agrees_with_scan(path: &Path) -> Result<(), String> {
    let conn = rusqlite::Connection::open(path).expect("open canonical");
    let js: Vec<i64> = {
        let mut stmt = conn.prepare("SELECT DISTINCT j FROM t ORDER BY j").unwrap();
        let rows = stmt
            .query_map([], |r| r.get::<_, i64>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        rows
    };
    for j in js {
        // `WHERE j = ?` can use idx_j; `WHERE +j = ?` forces a table scan.
        let via_index: i64 = conn
            .query_row("SELECT count(*) FROM t WHERE j = ?1", [j], |r| r.get(0))
            .unwrap();
        let via_scan: i64 = conn
            .query_row("SELECT count(*) FROM t WHERE +j = ?1", [j], |r| r.get(0))
            .unwrap();
        if via_index != via_scan {
            return Err(format!(
                "j={j}: indexed lookup found {via_index} rows but table scan found {via_scan}"
            ));
        }
    }
    Ok(())
}

#[test]
#[ignore = "issue #113: interleaved table-scan read + churn corrupts secondary indexes (silent missing entries)"]
fn issue_113_interleaved_scan_churn_keeps_indexes_consistent() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("issue113.db");
    let path_str = db_path.to_string_lossy().into_owned();

    run_churn(&path_str);

    // Diagnostics: confirm canonical sqlite3 can actually read fsqlite's file.
    {
        let conn = rusqlite::Connection::open(&db_path).expect("open canonical");
        let n: i64 = conn
            .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
            .unwrap_or(-1);
        eprintln!("[diag] canonical sees {n} rows in fsqlite file");
    }

    let ic = canonical_integrity_check(&db_path);
    let agree = canonical_index_agrees_with_scan(&db_path);
    eprintln!("[diag] canonical integrity_check = {ic:?}; index/scan agree = {agree:?}");

    assert_eq!(
        ic, "ok",
        "canonical integrity_check on fsqlite output should be ok, got: {ic}"
    );
    assert!(agree.is_ok(), "index/scan disagreement: {:?}", agree.err());
}
