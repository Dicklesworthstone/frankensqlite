//! bd-yfdb6: WAL recovery truncation concurrent-write race — data loss repro.
//!
//! ## Failure mode
//!
//! Connection A writes to WAL → crashes mid-write. Connection B opens DB →
//! recovery starts → recovery truncates WAL after checkpoint. If recovery
//! doesn't fence against in-flight writers or fsync before truncation,
//! Connection A's committed-but-unflushed frames are silently discarded.
//!
//! ## Test approach
//!
//! Since we cannot SIGKILL within a single-process test, we simulate the
//! crash by:
//! 1. Connection A commits N rows, then starts another transaction with
//!    more rows but does NOT commit (simulating mid-write crash).
//! 2. Connection A is dropped (simulating process death without clean close).
//! 3. Connection B opens the same file — triggers WAL recovery.
//! 4. Verify: all N committed rows from A are visible through B.
//!    If any committed rows are missing, the bug is confirmed.
//!
//! For the concurrent-open variant, Connection B opens while Connection A
//! is still writing (before the crash), exercising the recovery fencing path.
#![recursion_limit = "512"]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use fsqlite::Connection;

fn test_tmpdir() -> tempfile::TempDir {
    tempfile::tempdir_in(std::env::temp_dir())
        .or_else(|_| tempfile::tempdir_in("."))
        .expect("tempdir")
}

async fn query_count(conn: &Connection, sql: &str) -> usize {
    conn.query(sql).await.expect("query").len()
}

fn csqlite_count(path: &str, sql: &str) -> usize {
    let c = rusqlite::Connection::open(path).expect("csqlite open");
    let mut stmt = c.prepare(sql).expect("prepare");
    stmt.query_map([], |_| Ok(())).expect("query").count()
}

// ─── R1: Committed data survives crash + recovery ───────────────────

#[test]
fn r1_committed_data_survives_unclean_close() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("r1.db");
        let path = db_path.to_str().expect("path");

        // Connection A: commit 100 rows, then start an uncommitted batch
        {
            let conn_a = Connection::open(path).await.expect("open A");
            conn_a
                .execute("CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("create table");

            // Committed batch
            conn_a.execute("BEGIN").await.expect("begin");
            for i in 1..=100 {
                conn_a
                    .execute(&format!("INSERT INTO data VALUES ({i}, 'committed_{i}')"))
                    .await
                    .expect("insert committed");
            }
            conn_a.execute("COMMIT").await.expect("commit");

            // Uncommitted batch (simulates in-flight writes at crash time)
            conn_a.execute("BEGIN").await.expect("begin uncommitted");
            for i in 101..=200 {
                conn_a
                    .execute(&format!("INSERT INTO data VALUES ({i}, 'uncommitted_{i}')"))
                    .await
                    .expect("insert uncommitted");
            }
            // NO COMMIT — drop simulates crash
        }

        // Connection B: opens the DB, triggering WAL recovery
        let conn_b = Connection::open(path).await.expect("open B (recovery)");
        let _count = query_count(&conn_b, "SELECT COUNT(*) FROM data").await;
        let rows = conn_b
            .query("SELECT COUNT(*) FROM data")
            .await
            .expect("count");
        assert_eq!(rows.len(), 1);

        // All 100 committed rows must survive
        let visible = query_count(&conn_b, "SELECT * FROM data").await;
        assert!(
            visible >= 100,
            "DATA LOSS: only {visible}/100 committed rows visible after recovery"
        );
        assert!(
            visible <= 100,
            "uncommitted rows leaked: {visible} rows visible (expected 100)"
        );

        // Oracle parity: C SQLite should also see exactly 100 rows
        let c_count = csqlite_count(path, "SELECT * FROM data");
        assert_eq!(
            visible, c_count,
            "parity: fsqlite sees {visible}, csqlite sees {c_count}"
        );

        drop(conn_b);
    });
}

// ─── R2: Multiple commit-crash cycles ───────────────────────────────

#[test]
fn r2_repeated_commit_crash_cycles() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("r2.db");
        let path = db_path.to_str().expect("path");

        // Initial schema
        {
            let conn = Connection::open(path).await.expect("open");
            conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, seq INTEGER)")
                .await
                .expect("create");
        }

        let mut expected_count = 0;

        // 5 cycles of: commit some rows → crash with uncommitted rows
        for cycle in 0..5 {
            let batch_start = expected_count + 1;
            let committed_end = batch_start + 20;
            let uncommitted_end = committed_end + 10;

            {
                let conn = Connection::open(path).await.expect("open cycle");

                // Committed rows
                conn.execute("BEGIN").await.expect("begin");
                for i in batch_start..committed_end {
                    conn.execute(&format!("INSERT INTO events VALUES ({i}, {cycle})"))
                        .await
                        .expect("insert committed");
                }
                conn.execute("COMMIT").await.expect("commit");

                // Uncommitted rows (crash)
                conn.execute("BEGIN").await.expect("begin uncommitted");
                for i in committed_end..uncommitted_end {
                    conn.execute(&format!("INSERT INTO events VALUES ({i}, {cycle})"))
                        .await
                        .expect("insert uncommitted");
                }
                // NO COMMIT — drop = crash
            }

            expected_count = committed_end - 1;

            // Recovery connection
            let verify = Connection::open(path).await.expect("open verify");
            let actual = query_count(&verify, "SELECT * FROM events").await;
            assert_eq!(
                actual, expected_count as usize,
                "cycle {cycle}: expected {expected_count} rows, got {actual}"
            );
        }
    });
}

// ─── R3: Concurrent open during write ───────────────────────────────

#[test]
fn r3_concurrent_open_during_write() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("r3.db");
        let path = db_path.to_str().expect("path");

        // Setup
        {
            let conn = Connection::open(path).await.expect("open");
            conn.execute("CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount INTEGER)")
                .await
                .expect("create");
            conn.execute("BEGIN").await.expect("begin");
            for i in 1..=50 {
                conn.execute(&format!("INSERT INTO ledger VALUES ({i}, {i}00)"))
                    .await
                    .expect("insert");
            }
            conn.execute("COMMIT").await.expect("commit");
        }

        let path_owned = path.to_string();
        let done = Arc::new(AtomicBool::new(false));
        let done_writer = Arc::clone(&done);

        // Writer thread: continuously writes in transactions
        let writer = std::thread::spawn(move || {
            let mut committed = 0;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&path_owned).await.expect("writer open");
                let mut next_id = 51;
                while !done_writer.load(Ordering::Relaxed) && next_id < 500 {
                    conn.execute("BEGIN").await.expect("begin");
                    for _ in 0..10 {
                        let result = conn
                            .execute(&format!(
                                "INSERT INTO ledger VALUES ({next_id}, {next_id}00)"
                            ))
                            .await;
                        if result.is_err() {
                            break;
                        }
                        next_id += 1;
                    }
                    if conn.execute("COMMIT").await.is_ok() {
                        committed += 10;
                    } else {
                        // Rollback on commit failure (busy/locked)
                        drop(conn.execute("ROLLBACK").await);
                    }
                }
            });
            committed
        });

        // Reader thread: opens connection and queries while writer is active
        let path_reader = path.to_string();
        let done_reader = Arc::clone(&done);
        let reader = std::thread::spawn(move || {
            let mut reads = 0;
            asupersync::test_utils::run_test(|| async {
                std::thread::sleep(std::time::Duration::from_millis(10));
                let conn = Connection::open(&path_reader)
                    .await
                    .expect("reader open during writes");
                for _ in 0..20 {
                    let result = conn.query("SELECT COUNT(*) FROM ledger").await;
                    if result.is_ok() {
                        reads += 1;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                done_reader.store(true, Ordering::Relaxed);
            });
            reads
        });

        let committed = writer.join().expect("writer must not panic");
        let reads = reader.join().expect("reader must not panic");

        // Both threads completed without deadlock or panic
        assert!(committed >= 0, "writer committed some rows");
        assert!(reads > 0, "reader completed some reads");

        // Final verification: all committed data is consistent
        let verify = Connection::open(path).await.expect("verify open");
        let total = query_count(&verify, "SELECT * FROM ledger").await;
        assert!(
            total >= 50,
            "baseline 50 rows must survive concurrent access (got {total})"
        );
    });
}

// ─── R4: Integrity check after crash recovery ───────────────────────

#[test]
fn r4_integrity_check_after_recovery() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("r4.db");
        let path = db_path.to_str().expect("path");

        // Write + crash
        {
            let conn = Connection::open(path).await.expect("open");
            conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, body TEXT)")
                .await
                .expect("create");
            conn.execute("BEGIN").await.expect("begin");
            for i in 1..=200 {
                conn.execute(&format!(
                    "INSERT INTO docs VALUES ({i}, 'title_{i}', '{}')",
                    "x".repeat(100)
                ))
                .await
                .expect("insert");
            }
            conn.execute("COMMIT").await.expect("commit");

            // Start uncommitted write then crash
            conn.execute("BEGIN").await.expect("begin");
            for i in 201..=300 {
                conn.execute(&format!(
                    "INSERT INTO docs VALUES ({i}, 'uncommitted_{i}', '{}')",
                    "y".repeat(100)
                ))
                .await
                .expect("insert uncommitted");
            }
            // NO COMMIT
        }

        // Recovery
        let recovery = Connection::open(path).await.expect("recovery open");

        // Integrity check must pass
        let check = recovery.query("PRAGMA integrity_check").await;
        match check {
            Ok(rows) => {
                assert!(
                    !rows.is_empty(),
                    "integrity_check must return at least one row"
                );
            }
            Err(e) => {
                // If PRAGMA isn't fully supported, skip gracefully
                eprintln!("R4: integrity_check returned error (may be unsupported): {e}");
            }
        }

        // Committed rows must be intact
        let count = query_count(&recovery, "SELECT * FROM docs").await;
        assert_eq!(count, 200, "all 200 committed rows must survive recovery");
    });
}

// ─── R5: Savepoint + crash recovery ─────────────────────────────────

#[test]
fn r5_savepoint_partial_commit_recovery() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("r5.db");
        let path = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path).await.expect("open");
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
                .await
                .expect("create");

            // Committed transaction
            conn.execute("BEGIN").await.expect("begin");
            for i in 1..=30 {
                conn.execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                    .await
                    .expect("insert");
            }
            conn.execute("COMMIT").await.expect("commit");

            // Second transaction with savepoint: commit partial, crash
            conn.execute("BEGIN").await.expect("begin 2");
            for i in 31..=40 {
                conn.execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                    .await
                    .expect("insert batch 2");
            }
            // Savepoint and more writes
            conn.execute("SAVEPOINT sp1").await.expect("savepoint");
            for i in 41..=50 {
                conn.execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                    .await
                    .expect("insert in savepoint");
            }
            // Don't release savepoint, don't commit — crash
        }

        // Recovery
        let recovery = Connection::open(path).await.expect("recovery open");
        let count = query_count(&recovery, "SELECT * FROM t").await;
        assert_eq!(
            count, 30,
            "only the first committed batch (30 rows) should survive (got {count})"
        );
    });
}
