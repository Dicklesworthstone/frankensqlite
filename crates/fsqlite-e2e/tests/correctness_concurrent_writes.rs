//! Correctness test: concurrent multi-thread writes with logical equivalence.
//!
//! Bead: bd-244z
//!
//! Spawns multiple writer threads on C SQLite (via rusqlite, using a temp
//! file with WAL mode), then verifies FrankenSQLite produces the same
//! logical result when executing the same operations sequentially.
//!
//! FrankenSQLite's MVCC concurrent writer path is not yet wired to the
//! persistence layer, so this test validates **logical equivalence**: the
//! final set of rows produced by the same set of operations must be
//! identical regardless of execution order or concurrency model.
//!
//! For C SQLite, true multi-threaded concurrent writes are exercised
//! (each thread opens its own connection to a shared WAL-mode file).
#![recursion_limit = "512"]

use std::sync::{Arc, Barrier};
use std::thread;

use serde_json::json;

const BEAD_ID: &str = "bd-244z";
const SCENARIO_COMPLETENESS_BEAD_ID: &str = "bd-mblr.4";
const SCENARIO_COMPLETENESS_REPLAY: &str = "cargo test -p fsqlite-e2e --test correctness_concurrent_writes -- --nocapture --test-threads=1";
const SEED_CONCURRENT_WRITES_2T: u64 = 0x006D_626C_722E_3402;
const SEED_CONCURRENT_WRITES_4T: u64 = 0x006D_626C_722E_3403;
const SEED_CONCURRENT_WRITES_8T: u64 = 0x006D_626C_722E_3404;
const SEED_CONCURRENT_WRITES_NO_LOSS: u64 = 0x006D_626C_722E_3405;

// ─── Helpers ───────────────────────────────────────────────────────────

fn emit_scenario_completeness_log(
    test_name: &str,
    seed: u64,
    phase: &str,
    extra: serde_json::Value,
) {
    eprintln!(
        "SCENARIO_COMPLETENESS:{}",
        json!({
            "bead_id": SCENARIO_COMPLETENESS_BEAD_ID,
            "seed": seed,
            "replay_command": SCENARIO_COMPLETENESS_REPLAY,
            "test_name": test_name,
            "phase": phase,
            "extra": extra
        })
    );
}

fn row_summary(rows: &[(i64, String, i64)]) -> serde_json::Value {
    let first_row = rows.first().map(|(id, name, val)| {
        json!({
            "id": id,
            "name": name,
            "val": val,
        })
    });
    let last_row = rows.last().map(|(id, name, val)| {
        json!({
            "id": id,
            "name": name,
            "val": val,
        })
    });

    json!({
        "row_count": rows.len(),
        "min_id": rows.first().map(|(id, _, _)| *id),
        "max_id": rows.last().map(|(id, _, _)| *id),
        "first_row": first_row,
        "last_row": last_row,
    })
}

/// Query all rows from the test table, returning sorted by id.
fn query_sorted(conn: &rusqlite::Connection) -> Vec<(i64, String, i64)> {
    let mut stmt = conn
        .prepare("SELECT id, name, val FROM concurrent_test ORDER BY id")
        .unwrap();
    stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0).unwrap(),
            row.get::<_, String>(1).unwrap(),
            row.get::<_, i64>(2).unwrap(),
        ))
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

/// Query all rows from the FrankenSQLite test table.
async fn frank_query_sorted(conn: &fsqlite::Connection) -> Vec<(i64, String, i64)> {
    let rows = conn
        .query("SELECT id, name, val FROM concurrent_test ORDER BY id")
        .await
        .unwrap();
    rows.iter()
        .map(|r| {
            let vals = r.values();
            let id = match &vals[0] {
                fsqlite_types::value::SqliteValue::Integer(i) => *i,
                other => {
                    assert!(
                        matches!(other, fsqlite_types::value::SqliteValue::Integer(_)),
                        "expected Integer for id, got {other:?}"
                    );
                    0
                }
            };
            let name = match &vals[1] {
                fsqlite_types::value::SqliteValue::Text(s) => s.to_string(),
                other => {
                    assert!(
                        matches!(other, fsqlite_types::value::SqliteValue::Text(_)),
                        "expected Text for name, got {other:?}"
                    );
                    String::new()
                }
            };
            let val = match &vals[2] {
                fsqlite_types::value::SqliteValue::Integer(i) => *i,
                other => {
                    assert!(
                        matches!(other, fsqlite_types::value::SqliteValue::Integer(_)),
                        "expected Integer for val, got {other:?}"
                    );
                    0
                }
            };
            (id, name, val)
        })
        .collect()
}

/// Generate INSERT statements for a thread's non-overlapping key range.
fn gen_thread_inserts(thread_id: usize, count: usize, range_size: usize) -> Vec<String> {
    let base = thread_id * range_size;
    (0..count)
        .map(|i| {
            let id = base + i;
            let name = format!("t{thread_id}_row{i}");
            #[allow(clippy::cast_possible_wrap)]
            let val = (id * 7 + 13) as i64;
            format!("INSERT INTO concurrent_test VALUES ({id}, '{name}', {val})")
        })
        .collect()
}

// ─── Tests ─────────────────────────────────────────────────────────────

#[test]
fn concurrent_writes_2_threads_disjoint_keys() {
    asupersync::test_utils::run_test(|| async {
        concurrent_writes_n_threads(
            "concurrent_writes_2_threads_disjoint_keys",
            SEED_CONCURRENT_WRITES_2T,
            2,
            500,
        )
        .await;
    });
}

#[test]
fn concurrent_writes_4_threads_disjoint_keys() {
    asupersync::test_utils::run_test(|| async {
        concurrent_writes_n_threads(
            "concurrent_writes_4_threads_disjoint_keys",
            SEED_CONCURRENT_WRITES_4T,
            4,
            250,
        )
        .await;
    });
}

#[test]
fn concurrent_writes_8_threads_disjoint_keys() {
    asupersync::test_utils::run_test(|| async {
        concurrent_writes_n_threads(
            "concurrent_writes_8_threads_disjoint_keys",
            SEED_CONCURRENT_WRITES_8T,
            8,
            125,
        )
        .await;
    });
}

async fn concurrent_writes_n_threads(
    test_name: &str,
    seed: u64,
    n_threads: usize,
    ops_per_thread: usize,
) {
    let range_size = 10_000; // non-overlapping ranges
    let total_expected = n_threads * ops_per_thread;

    // ── C SQLite: true concurrent writes via temp file ──
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let db_path = tmp.path().to_str().unwrap().to_owned();

    {
        let setup = rusqlite::Connection::open(&db_path).unwrap();
        setup
            .execute_batch(
                "PRAGMA journal_mode=WAL;\
                 CREATE TABLE concurrent_test (id INTEGER PRIMARY KEY, name TEXT, val INTEGER);",
            )
            .unwrap();
    }

    let barrier = Arc::new(Barrier::new(n_threads));
    let handles: Vec<_> = (0..n_threads)
        .map(|tid| {
            let path = db_path.clone();
            let bar = barrier.clone();
            let stmts = gen_thread_inserts(tid, ops_per_thread, range_size);
            thread::spawn(move || {
                let conn = rusqlite::Connection::open(&path).unwrap();
                conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
                    .unwrap();
                bar.wait();
                conn.execute_batch("BEGIN;").unwrap();
                for sql in &stmts {
                    conn.execute(sql, []).unwrap();
                }
                conn.execute_batch("COMMIT;").unwrap();
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let csqlite_conn = rusqlite::Connection::open(&db_path).unwrap();
    let csqlite_rows = query_sorted(&csqlite_conn);
    let frank = fsqlite::Connection::open(":memory:").await.unwrap();

    // ── FrankenSQLite: sequential execution (same operations) ──
    frank
        .execute("CREATE TABLE concurrent_test (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
        .await
        .unwrap();

    for tid in 0..n_threads {
        let stmts = gen_thread_inserts(tid, ops_per_thread, range_size);
        frank.execute("BEGIN").await.unwrap();
        for sql in &stmts {
            frank.execute(sql).await.unwrap();
        }
        frank.execute("COMMIT").await.unwrap();
    }

    let frank_rows = frank_query_sorted(&frank).await;
    let logically_equivalent = csqlite_rows == frank_rows;

    emit_scenario_completeness_log(
        test_name,
        seed,
        "result",
        json!({
            "scenario_bead_id": BEAD_ID,
            "threads": n_threads,
            "ops_per_thread": ops_per_thread,
            "range_size": range_size,
            "total_expected": total_expected,
            "logical_equivalence": logically_equivalent,
            "csqlite": row_summary(&csqlite_rows),
            "fsqlite": row_summary(&frank_rows),
        }),
    );

    assert_eq!(
        csqlite_rows.len(),
        total_expected,
        "C SQLite row count mismatch"
    );
    assert_eq!(
        frank_rows.len(),
        total_expected,
        "FrankenSQLite row count mismatch"
    );

    // ── Compare logical equivalence ──
    assert_eq!(
        csqlite_rows,
        frank_rows,
        "logical equivalence failed: {n_threads} threads x {ops_per_thread} ops\n  \
         csqlite has {} rows, fsqlite has {} rows",
        csqlite_rows.len(),
        frank_rows.len()
    );
}

#[test]
fn concurrent_writes_verify_no_data_loss() {
    // 4 threads, 200 ops each, verify every single row is present.
    let n_threads = 4;
    let ops_per_thread = 200;
    let range_size = 10_000;

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let db_path = tmp.path().to_str().unwrap().to_owned();

    {
        let setup = rusqlite::Connection::open(&db_path).unwrap();
        setup
            .execute_batch(
                "PRAGMA journal_mode=WAL;\
                 CREATE TABLE concurrent_test (id INTEGER PRIMARY KEY, name TEXT, val INTEGER);",
            )
            .unwrap();
    }

    let barrier = Arc::new(Barrier::new(n_threads));
    let handles: Vec<_> = (0..n_threads)
        .map(|tid| {
            let path = db_path.clone();
            let bar = barrier.clone();
            let stmts = gen_thread_inserts(tid, ops_per_thread, range_size);
            thread::spawn(move || -> rusqlite::Result<()> {
                let conn = rusqlite::Connection::open(&path)?;
                conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")?;
                bar.wait();
                for sql in &stmts {
                    loop {
                        match conn.execute(sql, []) {
                            Ok(_) => break,
                            Err(e) if e.to_string().contains("database is locked") => {
                                thread::sleep(std::time::Duration::from_millis(1));
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
                Ok(())
            })
        })
        .collect();

    for h in handles {
        let r = h.join().unwrap();
        assert!(r.is_ok(), "worker thread error: {r:?}");
    }

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let rows = query_sorted(&conn);

    let mut missing_ids = Vec::new();
    for tid in 0..n_threads {
        for i in 0..ops_per_thread {
            #[allow(clippy::cast_possible_wrap)]
            let expected_id = (tid * range_size + i) as i64;
            if !rows.iter().any(|(id, _, _)| *id == expected_id) {
                missing_ids.push(expected_id);
            }
        }
    }

    emit_scenario_completeness_log(
        "concurrent_writes_verify_no_data_loss",
        SEED_CONCURRENT_WRITES_NO_LOSS,
        "result",
        json!({
            "scenario_bead_id": BEAD_ID,
            "threads": n_threads,
            "ops_per_thread": ops_per_thread,
            "range_size": range_size,
            "row_summary": row_summary(&rows),
            "missing_id_count": missing_ids.len(),
            "missing_id_sample": missing_ids.iter().take(8).copied().collect::<Vec<_>>(),
        }),
    );

    assert!(
        missing_ids.is_empty(),
        "missing row ids detected: {:?}",
        missing_ids.iter().take(8).copied().collect::<Vec<_>>()
    );
    assert_eq!(rows.len(), n_threads * ops_per_thread);
}
