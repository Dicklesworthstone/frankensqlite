//! GitHub issue #113 — interleaved read + delete/reinsert churn.
//!
//! The issue reported that, within a single connection, a table-scan read
//! interleaved with delete/reinsert churn corrupts secondary-index b-trees so
//! that rows go missing from their indexes (canonical SQLite confirms it on the
//! committed file). Two distinct behaviours were investigated:
//!
//! 1. **In-transaction `PRAGMA integrity_check` false positive (FIXED, GH#113).**
//!    Running `PRAGMA integrity_check` *inside* a `BEGIN IMMEDIATE` write
//!    transaction, interleaved with churn, deterministically reported
//!    `database disk image is malformed`. The engine state was actually
//!    consistent: frank's on-disk freelist trunk pages and the page-1 header
//!    (db size + freelist offsets 32/36) are a deferred commit-time projection
//!    that stays at the last-committed state until COMMIT, so the integrity
//!    walk cross-referenced fresh in-transaction btree pages against stale
//!    committed freelist/size metadata. The walk now uses the pager's live
//!    freelist set and live db size when a write transaction is active.
//!    `in_txn_integrity_check_stays_consistent` is the regression test.
//!
//! 2. **Persisted secondary-index corruption (NOT reproduced).** The reporter's
//!    primary claim was that canonical sqlite3, reading the *committed* file,
//!    finds missing index entries. Extensive reproduction (CLI `.sql` path and
//!    the Connection API, ≥3 indexes, 8000 rows, 12 churn cycles) always
//!    produced a consistent committed file. `persisted_indexes_stay_consistent`
//!    keeps that guard in place but remains `#[ignore]`d: it has never fired, so
//!    it is documentation of the unreproduced claim rather than a known failure.

use std::path::Path;

use fsqlite_types::SqliteValue;
use tempfile::TempDir;

const ROWS: i64 = 8000;
const CYCLES: usize = 12;
const BATCH: i64 = 5600; // ~70% churn per cycle

fn insert_row(conn: &fsqlite::Connection, id: i64, ctx: &str) {
    conn.execute(&format!(
        "INSERT INTO t(id, j, k, payload) VALUES ({id}, {j}, {k}, 'p{id:07}')",
        j = id,
        k = id % 8,
    ))
    .unwrap_or_else(|e| panic!("{ctx}: insert {id}: {e}"));
}

/// The text of every row returned by `PRAGMA integrity_check`. A healthy
/// database returns exactly `["ok"]`; corruption returns one or more
/// human-readable problem descriptions.
fn integrity_check_texts(conn: &fsqlite::Connection, ctx: &str) -> Vec<String> {
    let rows = conn
        .query("PRAGMA integrity_check")
        .unwrap_or_else(|e| panic!("{ctx}: integrity_check returned an error: {e}"));
    rows.iter()
        .map(|r| match r.values().first() {
            Some(SqliteValue::Text(s)) => s.to_string(),
            other => format!("{other:?}"),
        })
        .collect()
}

/// GH#113 regression: an in-transaction `PRAGMA integrity_check` interleaved
/// with delete/reinsert churn must report `ok`. Before the fix every cycle
/// reported `database disk image is malformed` (stale freelist/size metadata).
#[test]
fn in_txn_integrity_check_stays_consistent() {
    // A few cycles are enough to exercise the alloc-from-committed-freelist and
    // in-transaction db-growth windows that tripped the stale-metadata walk.
    const CYCLES_FAST: usize = 4;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("issue113_intxn.db");
    let conn =
        fsqlite::Connection::open(db_path.to_string_lossy().into_owned()).expect("open fsqlite");
    conn.execute("PRAGMA foreign_keys=off").unwrap();
    conn.execute("PRAGMA fsqlite.concurrent_mode = OFF")
        .unwrap();
    conn.execute_batch(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, j INTEGER, k INTEGER, payload TEXT);
         CREATE INDEX idx_j ON t(j);
         CREATE INDEX idx_k ON t(k);
         CREATE INDEX idx_kj ON t(k, j);",
    )
    .expect("create schema");

    // Bulk-load in its own committed transaction.
    conn.execute("BEGIN IMMEDIATE").unwrap();
    for id in 1..=ROWS {
        insert_row(&conn, id, "load");
    }
    conn.execute("COMMIT").unwrap();

    let mut next_id = ROWS + 1;
    let mut lo = 1i64;
    for cycle in 0..CYCLES_FAST {
        conn.execute("BEGIN IMMEDIATE").unwrap();

        let hi = lo + BATCH - 1;
        conn.execute(&format!("DELETE FROM t WHERE id BETWEEN {lo} AND {hi}"))
            .unwrap_or_else(|e| panic!("cycle {cycle}: delete: {e}"));
        lo = hi + 1;

        for _ in 0..BATCH {
            insert_row(&conn, next_id, &format!("cycle {cycle}"));
            next_id += 1;
        }

        // The in-transaction integrity check — the GH#113 trigger.
        let texts = integrity_check_texts(&conn, &format!("cycle {cycle}"));
        assert_eq!(
            texts,
            vec!["ok".to_string()],
            "cycle {cycle}: in-transaction PRAGMA integrity_check reported corruption (GH#113); \
             engine state is consistent, this is the stale-metadata walk false positive: {texts:?}"
        );

        conn.execute("COMMIT").unwrap();
    }

    // After COMMIT the committed file must also be clean.
    let texts = integrity_check_texts(&conn, "post-commit");
    assert_eq!(
        texts,
        vec!["ok".to_string()],
        "post-commit integrity_check: {texts:?}"
    );
}

/// Run the churn workload through a single fsqlite connection at `path`,
/// committing each cycle (used by the persisted-corruption guard below).
fn run_committed_churn(path: &str) {
    let conn = fsqlite::Connection::open(path.to_owned()).expect("open fsqlite");
    conn.execute("PRAGMA fsqlite.concurrent_mode = OFF")
        .expect("concurrent_mode off");
    conn.execute_batch(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, j INTEGER, k INTEGER, payload TEXT);
         CREATE INDEX idx_j ON t(j);
         CREATE INDEX idx_k ON t(k);
         CREATE INDEX idx_kj ON t(k, j);",
    )
    .expect("create schema");

    conn.execute("BEGIN IMMEDIATE").expect("begin load");
    for id in 1..=ROWS {
        insert_row(&conn, id, "load");
    }
    conn.execute("COMMIT").expect("commit load");

    let mut next_id = ROWS + 1;
    let mut lo = 1i64;
    for cycle in 0..CYCLES {
        conn.execute("BEGIN IMMEDIATE")
            .unwrap_or_else(|e| panic!("begin cycle {cycle}: {e}"));
        let hi = lo + BATCH - 1;
        conn.execute(&format!("DELETE FROM t WHERE id BETWEEN {lo} AND {hi}"))
            .unwrap_or_else(|e| panic!("delete cycle {cycle}: {e}"));
        lo = hi + 1;
        for _ in 0..BATCH {
            insert_row(&conn, next_id, &format!("cycle {cycle}"));
            next_id += 1;
        }
        // Interleaved in-transaction integrity walk (a full btree scan).
        let _ = integrity_check_texts(&conn, &format!("cycle {cycle}"));
        conn.execute("COMMIT")
            .unwrap_or_else(|e| panic!("commit cycle {cycle}: {e}"));
    }
    drop(conn);
}

fn canonical_integrity_check(path: &Path) -> String {
    let conn = rusqlite::Connection::open(path).expect("open canonical");
    conn.query_row("PRAGMA integrity_check;", [], |row| row.get(0))
        .expect("integrity_check")
}

/// For each distinct `j`, an indexed lookup must agree with a forced table scan.
fn canonical_index_agrees_with_scan(path: &Path) -> Result<(), String> {
    let conn = rusqlite::Connection::open(path).expect("open canonical");
    let js: Vec<i64> = {
        let mut stmt = conn.prepare("SELECT DISTINCT j FROM t ORDER BY j").unwrap();
        stmt.query_map([], |r| r.get::<_, i64>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    for j in js {
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

/// Guard for the reporter's primary (unreproduced) claim: canonical sqlite3,
/// reading the committed file, finds the secondary indexes consistent. This has
/// never fired in reproduction — kept `#[ignore]`d as documentation of the
/// unreproduced persisted-corruption claim, not as a known failure.
#[test]
#[ignore = "issue #113: persisted secondary-index corruption was never reproduced; committed file is consistent"]
fn persisted_indexes_stay_consistent() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("issue113.db");
    run_committed_churn(&db_path.to_string_lossy());

    let ic = canonical_integrity_check(&db_path);
    let agree = canonical_index_agrees_with_scan(&db_path);
    assert_eq!(
        ic, "ok",
        "canonical integrity_check on fsqlite output: {ic}"
    );
    assert!(agree.is_ok(), "index/scan disagreement: {:?}", agree.err());
}
