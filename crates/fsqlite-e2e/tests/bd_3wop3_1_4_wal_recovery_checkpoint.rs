//! bd-3wop3.1.4: WAL recovery, checkpoint, and failure-injection proof pack.
//!
//! Verifies crash recovery, checkpoint correctness, and data durability
//! across restarts for file-backed databases.
//!
//! - W1: Committed data survives close/reopen (basic WAL recovery)
//! - W2: Repeated crash-restart cycles (commit, drop, reopen, verify)
//! - W3: Checkpoint doesn't lose data
//! - W4: Large transaction recovery (1000 rows per txn, 10 txns)
//! - W5: Interleaved commit/rollback recovery
//! - W6: Multi-connection writes survive restart
//! - W7: Data integrity after many small transactions
//! - W8: Oracle parity after recovery cycle
#![recursion_limit = "512"]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn test_tmpdir() -> tempfile::TempDir {
    tempfile::tempdir_in(std::env::temp_dir())
        .or_else(|_| tempfile::tempdir_in("."))
        .expect("tempdir")
}

async fn get_int(conn: &Connection, sql: &str) -> Option<i64> {
    let rows = conn.query(sql).await.ok()?;
    let row = rows.first()?;
    match row.get(0)? {
        SqliteValue::Integer(v) => Some(*v),
        _ => None,
    }
}

// ─── W1: Basic WAL recovery ──────────────────────────────────────

#[test]
fn w1_basic_wal_recovery() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("w1.db");
        let path_str = db_path.to_str().expect("path");

        // Write phase
        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE wal_test (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("create");
            conn.execute("BEGIN").await.expect("begin");
            for i in 1..=100 {
                conn.execute(&format!("INSERT INTO wal_test VALUES ({i}, 'data_{i}')"))
                    .await
                    .expect("insert");
            }
            conn.execute("COMMIT").await.expect("commit");
        }

        // Recovery phase — reopen and verify
        {
            let conn = Connection::open(path_str).await.expect("reopen");
            let count = get_int(&conn, "SELECT COUNT(*) FROM wal_test").await.unwrap();
            assert_eq!(
                count, 100,
                "W1: expected 100 rows after recovery, got {count}"
            );

            let sum_check = get_int(
                &conn,
                "SELECT COUNT(*) FROM wal_test WHERE val LIKE 'data_%'",
            )
            .await
            .unwrap();
            assert_eq!(sum_check, 100, "W1: data corruption detected");
        }

        eprintln!("W1: basic WAL recovery — 100 rows survive close/reopen");
    });
}

// ─── W2: Repeated crash-restart cycles ────────────────────────────

#[test]
fn w2_repeated_crash_restart() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("w2.db");
        let path_str = db_path.to_str().expect("path");

        // Initial schema
        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE cycles (id INTEGER PRIMARY KEY, cycle_num INTEGER)")
                .await
                .expect("create");
        }

        let rows_per_cycle = 50;
        let total_cycles = 10;

        for cycle in 0..total_cycles {
            // Open, write committed data, then simulate crash (just drop)
            {
                let conn = Connection::open(path_str).await.expect("cycle open");

                // Verify previous data survived
                let expected = cycle * rows_per_cycle;
                let actual = get_int(&conn, "SELECT COUNT(*) FROM cycles").await.unwrap();
                assert_eq!(
                    actual, expected as i64,
                    "W2: cycle {cycle} expected {expected} rows, got {actual}"
                );

                // Write new committed data
                conn.execute("BEGIN").await.expect("begin");
                for i in 0..rows_per_cycle {
                    let id = cycle * rows_per_cycle + i + 1;
                    conn.execute(&format!("INSERT INTO cycles VALUES ({id}, {cycle})"))
                        .await
                        .expect("insert");
                }
                conn.execute("COMMIT").await.expect("commit");

                // Start uncommitted data (simulates crash before commit)
                conn.execute("BEGIN").await.expect("begin crash");
                for i in 0..20 {
                    let id = 100_000 + cycle * 20 + i;
                    conn.execute(&format!("INSERT INTO cycles VALUES ({id}, -1)"))
                        .await
                        .expect("uncommitted insert");
                }
                // DROP without commit — crash simulation
            }
        }

        // Final verification
        {
            let conn = Connection::open(path_str).await.expect("final open");
            let total = get_int(&conn, "SELECT COUNT(*) FROM cycles").await.unwrap();
            let expected_total = (total_cycles * rows_per_cycle) as i64;
            assert_eq!(
                total, expected_total,
                "W2: final count {total} != expected {expected_total}"
            );

            // No uncommitted rows should exist
            let crash_rows = get_int(&conn, "SELECT COUNT(*) FROM cycles WHERE cycle_num = -1")
                .await
                .unwrap();
            assert_eq!(crash_rows, 0, "W2: uncommitted crash rows leaked through");
        }

        eprintln!(
            "W2: {total_cycles} crash-restart cycles — all committed data survives, no uncommitted leaks"
        );
    });
}

// ─── W3: Checkpoint doesn't lose data ─────────────────────────────

#[test]
fn w3_checkpoint_data_preservation() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("w3.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        conn.execute("CREATE TABLE ckpt (id INTEGER PRIMARY KEY, val INTEGER)")
            .await
            .expect("create");

        // Write data in phases, triggering checkpoints between
        for phase in 0..5 {
            conn.execute("BEGIN").await.expect("begin");
            for i in 0..200 {
                let id = phase * 200 + i + 1;
                conn.execute(&format!("INSERT INTO ckpt VALUES ({id}, {id})"))
                    .await
                    .expect("insert");
            }
            conn.execute("COMMIT").await.expect("commit");

            // Trigger checkpoint via PRAGMA
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").await.ok();

            // Verify data after checkpoint
            let count = get_int(&conn, "SELECT COUNT(*) FROM ckpt").await.unwrap();
            let expected = ((phase + 1) * 200) as i64;
            assert_eq!(
                count, expected,
                "W3: phase {phase} after checkpoint: expected {expected}, got {count}"
            );
        }

        // Reopen and verify
        drop(conn);
        let conn2 = Connection::open(path_str).await.expect("reopen");
        let final_count = get_int(&conn2, "SELECT COUNT(*) FROM ckpt").await.unwrap();
        assert_eq!(final_count, 1000, "W3: data lost after checkpoint+reopen");

        let sum = get_int(&conn2, "SELECT SUM(val) FROM ckpt").await.unwrap();
        // sum(1..=1000) = 500500
        assert_eq!(sum, 500_500, "W3: data corruption after checkpoint");

        eprintln!("W3: 5 checkpoint cycles, 1000 rows — all data preserved");
    });
}

// ─── W4: Large transaction recovery ───────────────────────────────

#[test]
fn w4_large_transaction_recovery() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("w4.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute(
                "CREATE TABLE large_txn (id INTEGER PRIMARY KEY, batch INTEGER, val INTEGER)",
            )
            .await
            .expect("create");

            for batch in 0..10 {
                conn.execute("BEGIN").await.expect("begin");
                for i in 0..1000 {
                    let id = batch * 1000 + i + 1;
                    conn.execute(&format!(
                        "INSERT INTO large_txn VALUES ({id}, {batch}, {})",
                        id * 3
                    ))
                    .await
                    .expect("insert");
                }
                conn.execute("COMMIT").await.expect("commit");
            }
        }

        // Recovery
        {
            let conn = Connection::open(path_str).await.expect("reopen");
            let count = get_int(&conn, "SELECT COUNT(*) FROM large_txn").await.unwrap();
            assert_eq!(count, 10_000, "W4: expected 10000 rows after recovery");

            // Verify each batch
            for batch in 0..10 {
                let bc = get_int(
                    &conn,
                    &format!("SELECT COUNT(*) FROM large_txn WHERE batch = {batch}"),
                )
                .await
                .unwrap();
                assert_eq!(bc, 1000, "W4: batch {batch} count wrong");
            }

            let sum = get_int(&conn, "SELECT SUM(val) FROM large_txn").await.unwrap();
            // sum(i*3 for i in 1..=10000) = 3 * 10000*10001/2 = 150_015_000
            assert_eq!(
                sum, 150_015_000,
                "W4: data corruption in large txn recovery"
            );
        }

        eprintln!("W4: 10 large transactions (1000 rows each) — full recovery verified");
    });
}

// ─── W5: Interleaved commit/rollback recovery ─────────────────────

#[test]
fn w5_interleaved_commit_rollback() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("w5.db");
        let path_str = db_path.to_str().expect("path");

        let mut committed_ids = Vec::new();

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE interleaved (id INTEGER PRIMARY KEY, status TEXT)")
                .await
                .expect("create");

            for round in 0..20 {
                let id = round + 1;
                conn.execute("BEGIN").await.expect("begin");
                conn.execute(&format!(
                    "INSERT INTO interleaved VALUES ({id}, '{}')",
                    if round % 2 == 0 {
                        "committed"
                    } else {
                        "rolled_back"
                    }
                ))
                .await
                .expect("insert");

                if round % 2 == 0 {
                    conn.execute("COMMIT").await.expect("commit");
                    committed_ids.push(id);
                } else {
                    conn.execute("ROLLBACK").await.expect("rollback");
                }
            }
        }

        // Recovery
        {
            let conn = Connection::open(path_str).await.expect("reopen");
            let count = get_int(&conn, "SELECT COUNT(*) FROM interleaved")
                .await
                .unwrap();
            assert_eq!(
                count,
                committed_ids.len() as i64,
                "W5: expected {} rows, got {count}",
                committed_ids.len()
            );

            // Only committed rows should exist
            let rb_count = get_int(
                &conn,
                "SELECT COUNT(*) FROM interleaved WHERE status = 'rolled_back'",
            )
            .await
            .unwrap();
            assert_eq!(rb_count, 0, "W5: rolled-back rows leaked through recovery");
        }

        eprintln!(
            "W5: 20 rounds (10 commit, 10 rollback) — only {} committed rows survive",
            committed_ids.len()
        );
    });
}

// ─── W6: Multi-connection writes survive restart ──────────────────

#[test]
fn w6_multi_connection_restart() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("w6.db");
        let path_str = db_path.to_str().expect("path");

        // Phase 1: multiple connections write
        {
            let setup = Connection::open(path_str).await.expect("setup");
            setup
                .execute("CREATE TABLE multi_w (id INTEGER PRIMARY KEY, writer INTEGER)")
                .await
                .expect("create");
        }

        let stop = Arc::new(AtomicBool::new(false));
        let threads: Vec<_> = (0..4u64)
            .map(|tid| {
                let path = path_str.to_string();
                let s = Arc::clone(&stop);
                std::thread::spawn(move || {
                    let mut committed = 0u64;
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(&path).await.expect("open");
                        let mut seq = 0u64;
                        while !s.load(Ordering::Relaxed) {
                            let id = tid * 1_000_000 + seq;
                            if conn.execute("BEGIN").await.is_ok()
                                && conn
                                    .execute(&format!("INSERT INTO multi_w VALUES ({id}, {tid})"))
                                    .await
                                    .is_ok()
                                && conn.execute("COMMIT").await.is_ok()
                            {
                                committed += 1;
                            } else {
                                conn.execute("ROLLBACK").await.ok();
                            }
                            seq += 1;
                        }
                    });
                    committed
                })
            })
            .collect();

        std::thread::sleep(Duration::from_millis(500));
        stop.store(true, Ordering::Relaxed);

        let per_thread: Vec<u64> = threads
            .into_iter()
            .map(|t| t.join().expect("must not panic"))
            .collect();
        let total_committed: u64 = per_thread.iter().sum();

        // Phase 2: reopen and verify
        let verify = Connection::open(path_str).await.expect("verify open");
        let actual = get_int(&verify, "SELECT COUNT(*) FROM multi_w").await.unwrap();

        // Allow small tolerance for race at stop boundary
        assert!(
            actual >= total_committed as i64,
            "W6: data loss! actual={actual} < committed={total_committed}"
        );
        assert!(
            actual <= total_committed as i64 + 4,
            "W6: phantom rows! actual={actual} >> committed={total_committed}"
        );

        eprintln!(
            "W6: 4 concurrent writers, restart — {actual} rows survive, per-thread={per_thread:?}"
        );
    });
}

// ─── W7: Many small transactions ──────────────────────────────────

#[test]
fn w7_many_small_transactions() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("w7.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE small_txn (id INTEGER PRIMARY KEY, val INTEGER)")
                .await
                .expect("create");

            // 500 single-row transactions
            for i in 1..=500 {
                conn.execute("BEGIN").await.expect("begin");
                conn.execute(&format!("INSERT INTO small_txn VALUES ({i}, {i})"))
                    .await
                    .expect("insert");
                conn.execute("COMMIT").await.expect("commit");
            }
        }

        // Recovery
        {
            let conn = Connection::open(path_str).await.expect("reopen");
            let count = get_int(&conn, "SELECT COUNT(*) FROM small_txn").await.unwrap();
            assert_eq!(count, 500, "W7: expected 500 rows after recovery");

            let sum = get_int(&conn, "SELECT SUM(val) FROM small_txn").await.unwrap();
            assert_eq!(sum, 125_250, "W7: data corruption");
        }

        eprintln!("W7: 500 single-row transactions — all survive recovery");
    });
}

// ─── W8: Oracle parity after recovery ─────────────────────────────

#[test]
fn w8_oracle_parity_after_recovery() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();

        // C SQLite reference
        let c_path = dir.path().join("w8_c.db");
        {
            let c = rusqlite::Connection::open(&c_path).expect("c open");
            c.execute_batch("CREATE TABLE recovery (id INTEGER PRIMARY KEY, val INTEGER, tag TEXT)")
                .expect("c create");
            for i in 1..=200 {
                c.execute(
                    "INSERT INTO recovery VALUES (?1, ?2, ?3)",
                    rusqlite::params![i, i * 11, format!("tag_{i}")],
                )
                .expect("c insert");
            }
        }
        // Reopen C SQLite
        let c2 = rusqlite::Connection::open(&c_path).expect("c reopen");

        // FrankenSQLite
        let f_path = dir.path().join("w8_f.db");
        let f_path_str = f_path.to_str().expect("path");
        {
            let f = Connection::open(f_path_str).await.expect("f open");
            f.execute("CREATE TABLE recovery (id INTEGER PRIMARY KEY, val INTEGER, tag TEXT)")
                .await
                .expect("f create");
            f.execute("BEGIN").await.expect("f begin");
            for i in 1..=200 {
                f.execute(&format!(
                    "INSERT INTO recovery VALUES ({i}, {}, 'tag_{i}')",
                    i * 11
                ))
                .await
                .expect("f insert");
            }
            f.execute("COMMIT").await.expect("f commit");
        }
        // Reopen FrankenSQLite
        let f2 = Connection::open(f_path_str).await.expect("f reopen");

        // Compare after recovery
        let f_count = get_int(&f2, "SELECT COUNT(*) FROM recovery").await.unwrap();
        let c_count: i64 = c2
            .query_row("SELECT COUNT(*) FROM recovery", [], |r| r.get(0))
            .unwrap();
        assert_eq!(f_count, c_count, "W8: count mismatch after recovery");

        let f_sum = get_int(&f2, "SELECT SUM(val) FROM recovery").await.unwrap();
        let c_sum: i64 = c2
            .query_row("SELECT SUM(val) FROM recovery", [], |r| r.get(0))
            .unwrap();
        assert_eq!(f_sum, c_sum, "W8: sum mismatch after recovery");

        let f_min = get_int(&f2, "SELECT MIN(val) FROM recovery").await.unwrap();
        let c_min: i64 = c2
            .query_row("SELECT MIN(val) FROM recovery", [], |r| r.get(0))
            .unwrap();
        assert_eq!(f_min, c_min, "W8: min mismatch");

        eprintln!("W8: oracle parity after recovery — count={f_count}, sum={f_sum}");
    });
}
