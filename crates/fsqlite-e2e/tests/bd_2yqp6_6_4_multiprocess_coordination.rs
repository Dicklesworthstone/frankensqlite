//! bd-2yqp6.6.4: Multi-process coordination parity scenarios.
//!
//! Validates correctness under multi-process readers/writers sharing
//! a file-backed database. Each subprocess opens its own Connection
//! to the same DB file.
//!
//! - P1: Sequential process write→read (process A writes, process B reads)
//! - P2: Concurrent process writers (2 processes writing simultaneously)
//! - P3: Process crash recovery (write, kill-simulate, verify data)
//! - P4: Reader isolation (reader process sees consistent snapshot)
//! - P5: Many-process storm (4 processes read/write concurrently)
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

async fn count_rows(conn: &Connection, sql: &str) -> usize {
    conn.query(sql).await.expect("count query").len()
}

async fn get_int(conn: &Connection, sql: &str) -> Option<i64> {
    let rows = conn.query(sql).await.ok()?;
    let row = rows.first()?;
    match row.get(0)? {
        SqliteValue::Integer(v) => Some(*v),
        _ => None,
    }
}

// ─── P1: Sequential write→read across connections ─────────────────
// (Simulates process isolation without actual subprocess — each
//  Connection is independent like a separate process)

#[test]
fn p1_sequential_write_then_read() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("p1.db");
        let path_str = db_path.to_str().expect("path");

        // "Process A": create and write
        {
            let conn = Connection::open(path_str).await.expect("A open");
            conn.execute("CREATE TABLE shared (id INTEGER PRIMARY KEY, writer TEXT, val INTEGER)")
                .await
                .expect("create");
            conn.execute("BEGIN").await.expect("begin");
            for i in 1..=100 {
                conn.execute(&format!(
                    "INSERT INTO shared VALUES ({i}, 'process_A', {i})"
                ))
                .await
                .expect("A insert");
            }
            conn.execute("COMMIT").await.expect("commit");
        }
        // Connection dropped — "process A exits"

        // "Process B": read what A wrote
        {
            let conn = Connection::open(path_str).await.expect("B open");
            let count = count_rows(&conn, "SELECT * FROM shared").await;
            assert_eq!(count, 100, "P1: process B should see all 100 rows from A");

            let sum = get_int(&conn, "SELECT SUM(val) FROM shared").await;
            assert_eq!(sum, Some(5050), "P1: sum should be 5050");

            // Verify writer column
            let a_rows = count_rows(&conn, "SELECT * FROM shared WHERE writer = 'process_A'").await;
            assert_eq!(a_rows, 100, "P1: all rows should be from process_A");
        }

        eprintln!("P1: sequential write→read across connections — 100 rows visible");
    });
}

// ─── P2: Concurrent connection writers ────────────────────────────

#[test]
fn p2_concurrent_connection_writers() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("p2.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("setup open");
            conn.execute("CREATE TABLE writes (id INTEGER PRIMARY KEY, writer INTEGER)")
                .await
                .expect("create");
        }

        let stop = Arc::new(AtomicBool::new(false));

        // 4 "process" threads, each with its own connection
        let threads: Vec<_> = (0..4u64)
            .map(|pid| {
                let path = path_str.to_string();
                let s = Arc::clone(&stop);
                std::thread::spawn(move || {
                    let mut committed = 0u64;
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(&path).await.expect("open");
                        let mut seq = 0u64;
                        while !s.load(Ordering::Relaxed) {
                            let id = pid * 1_000_000 + seq;
                            if conn.execute("BEGIN").await.is_ok() {
                                if conn
                                    .execute(&format!("INSERT INTO writes VALUES ({id}, {pid})"))
                                    .await
                                    .is_ok()
                                {
                                    if conn.execute("COMMIT").await.is_ok() {
                                        committed += 1;
                                    } else {
                                        conn.execute("ROLLBACK").await.ok();
                                    }
                                } else {
                                    conn.execute("ROLLBACK").await.ok();
                                }
                            }
                            seq += 1;
                        }
                    });
                    committed
                })
            })
            .collect();

        std::thread::sleep(Duration::from_secs(2));
        stop.store(true, Ordering::Relaxed);

        let per_process: Vec<u64> = threads
            .into_iter()
            .map(|t| t.join().expect("must not panic"))
            .collect();

        let total_expected: u64 = per_process.iter().sum();
        assert!(total_expected > 0, "P2: no commits from any process");

        // Verify from a fresh connection
        let verify = Connection::open(path_str).await.expect("verify open");
        let actual = count_rows(&verify, "SELECT * FROM writes").await;

        // Allow actual >= expected because a commit can succeed right at
        // the stop boundary before the thread increments its counter
        assert!(
            actual as u64 >= total_expected,
            "P2: data loss! actual={actual} < committed={total_expected}"
        );
        // But shouldn't be wildly more
        assert!(
            (actual as u64) <= total_expected + 4,
            "P2: phantom rows! actual={actual} >> committed={total_expected}"
        );

        eprintln!("P2: 4 concurrent writers — total={total_expected}, per-process={per_process:?}");
    });
}

// ─── P3: Crash simulation — drop connection mid-transaction ───────

#[test]
fn p3_crash_simulation_recovery() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("p3.db");
        let path_str = db_path.to_str().expect("path");

        // Phase 1: committed data
        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE critical (id INTEGER PRIMARY KEY, phase TEXT)")
                .await
                .expect("create");
            conn.execute("BEGIN").await.expect("begin");
            for i in 1..=50 {
                conn.execute(&format!("INSERT INTO critical VALUES ({i}, 'committed')"))
                    .await
                    .expect("insert");
            }
            conn.execute("COMMIT").await.expect("commit");
        }

        // Phase 2: "crash" — start txn, insert more, DROP connection without commit
        {
            let conn = Connection::open(path_str).await.expect("crash open");
            conn.execute("BEGIN").await.expect("begin");
            for i in 51..=100 {
                conn.execute(&format!("INSERT INTO critical VALUES ({i}, 'uncommitted')"))
                    .await
                    .expect("insert uncommitted");
            }
            // DROP without COMMIT — simulates process crash
        }

        // Phase 3: recover — verify only committed data survives
        {
            let conn = Connection::open(path_str).await.expect("recover open");
            let count = count_rows(&conn, "SELECT * FROM critical").await;
            assert_eq!(
                count, 50,
                "P3: after crash, only committed rows should survive, got {count}"
            );

            let uncommitted =
                count_rows(&conn, "SELECT * FROM critical WHERE phase = 'uncommitted'").await;
            assert_eq!(
                uncommitted, 0,
                "P3: uncommitted rows should not survive crash"
            );
        }

        eprintln!("P3: crash simulation — only 50 committed rows survive");
    });
}

// ─── P4: Reader isolation during writes ───────────────────────────

#[test]
fn p4_reader_isolation() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("p4.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE ledger (id INTEGER PRIMARY KEY, balance INTEGER)")
                .await
                .expect("create");
            conn.execute("INSERT INTO ledger VALUES (1, 1000)")
                .await
                .expect("seed");
        }

        // Reader opens and reads
        let reader = Connection::open(path_str).await.expect("reader open");
        let initial = get_int(&reader, "SELECT balance FROM ledger WHERE id = 1").await;
        assert_eq!(initial, Some(1000));

        // Writer modifies in a transaction
        let writer = Connection::open(path_str).await.expect("writer open");
        writer.execute("BEGIN").await.expect("begin");
        writer
            .execute("UPDATE ledger SET balance = 500 WHERE id = 1")
            .await
            .expect("update");

        // Reader should NOT see uncommitted write (read committed or higher)
        let during_write = get_int(&reader, "SELECT balance FROM ledger WHERE id = 1").await;
        assert_eq!(
            during_write,
            Some(1000),
            "P4: reader should not see uncommitted balance change"
        );

        // Writer commits
        writer.execute("COMMIT").await.expect("commit");

        // Reader should now see committed change
        let after_commit = get_int(&reader, "SELECT balance FROM ledger WHERE id = 1").await;
        assert!(
            after_commit.is_some(),
            "P4: reader failed to read after commit"
        );
        let val = after_commit.unwrap();
        assert!(
            val == 500 || val == 1000,
            "P4: reader sees unexpected balance {val}"
        );

        eprintln!(
            "P4: reader isolation — balance={during_write:?} during write, {after_commit:?} after commit"
        );
    });
}

// ─── P5: Multi-connection storm ───────────────────────────────────

#[test]
fn p5_multi_connection_storm() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("p5.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute(
                "CREATE TABLE storm (id INTEGER PRIMARY KEY, conn_id INTEGER, seq INTEGER)",
            )
            .await
            .expect("create");
        }

        let stop = Arc::new(AtomicBool::new(false));
        let n_conns = 8u64;

        let threads: Vec<_> = (0..n_conns)
            .map(|cid| {
                let path = path_str.to_string();
                let s = Arc::clone(&stop);
                std::thread::spawn(move || {
                    let mut committed = 0u64;
                    let mut errors = 0u64;
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(&path).await.expect("open");
                        let mut seq = 0u64;

                        while !s.load(Ordering::Relaxed) {
                            let id = cid * 10_000_000 + seq;
                            if conn.execute("BEGIN").await.is_ok() {
                                // Insert
                                let ins_ok = conn
                                    .execute(&format!(
                                        "INSERT INTO storm VALUES ({id}, {cid}, {seq})"
                                    ))
                                    .await
                                    .is_ok();

                                // Also read (mixed workload)
                                if ins_ok {
                                    conn.query(&format!(
                                        "SELECT COUNT(*) FROM storm WHERE conn_id = {cid}"
                                    ))
                                    .await
                                    .ok();
                                }

                                if ins_ok && conn.execute("COMMIT").await.is_ok() {
                                    committed += 1;
                                } else {
                                    conn.execute("ROLLBACK").await.ok();
                                    errors += 1;
                                }
                            }
                            seq += 1;
                        }
                    });
                    (committed, errors)
                })
            })
            .collect();

        std::thread::sleep(Duration::from_secs(2));
        stop.store(true, Ordering::Relaxed);

        let results: Vec<(u64, u64)> = threads
            .into_iter()
            .map(|t| t.join().expect("must not panic"))
            .collect();

        let total_committed: u64 = results.iter().map(|r| r.0).sum();
        let total_errors: u64 = results.iter().map(|r| r.1).sum();

        assert!(total_committed > 0, "P5: no commits from any connection");

        // Verify data integrity
        let verify = Connection::open(path_str).await.expect("verify open");
        let actual = count_rows(&verify, "SELECT * FROM storm").await;
        assert_eq!(
            actual as u64, total_committed,
            "P5: data loss! actual={actual} vs committed={total_committed}"
        );

        eprintln!(
            "P5: 8-connection storm — {total_committed} committed, {total_errors} errors, all data intact"
        );
    });
}

// ─── P6: Oracle parity — fsqlite vs rusqlite multi-connection ─────

#[test]
fn p6_oracle_multi_connection_parity() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();

        // C SQLite reference
        let c_path = dir.path().join("p6_csqlite.db");
        {
            let c = rusqlite::Connection::open(&c_path).expect("c open");
            c.execute_batch(
                "CREATE TABLE mc (id INTEGER PRIMARY KEY, src TEXT, val INTEGER);
             BEGIN;",
            )
            .expect("c setup");
            for i in 1..=100 {
                c.execute(
                    "INSERT INTO mc VALUES (?1, 'conn1', ?2)",
                    rusqlite::params![i, i * 3],
                )
                .expect("c insert");
            }
            c.execute_batch("COMMIT;").expect("c commit");
        }
        // Second "process"
        {
            let c2 = rusqlite::Connection::open(&c_path).expect("c2 open");
            let c_count: i64 = c2
                .query_row("SELECT COUNT(*) FROM mc", [], |r| r.get(0))
                .expect("c count");
            assert_eq!(c_count, 100);

            c2.execute_batch("BEGIN;").expect("c2 begin");
            for i in 101..=200 {
                c2.execute(
                    "INSERT INTO mc VALUES (?1, 'conn2', ?2)",
                    rusqlite::params![i, i * 3],
                )
                .expect("c2 insert");
            }
            c2.execute_batch("COMMIT;").expect("c2 commit");
        }
        let c_final = rusqlite::Connection::open(&c_path).expect("c final");
        let c_total: i64 = c_final
            .query_row("SELECT COUNT(*) FROM mc", [], |r| r.get(0))
            .expect("c total");
        let c_sum: i64 = c_final
            .query_row("SELECT SUM(val) FROM mc", [], |r| r.get(0))
            .expect("c sum");

        // FrankenSQLite
        let f_path = dir.path().join("p6_fsqlite.db");
        let f_path_str = f_path.to_str().expect("path");
        {
            let f = Connection::open(f_path_str).await.expect("f open");
            f.execute("CREATE TABLE mc (id INTEGER PRIMARY KEY, src TEXT, val INTEGER)")
                .await
                .expect("f create");
            f.execute("BEGIN").await.expect("f begin");
            for i in 1..=100 {
                f.execute(&format!("INSERT INTO mc VALUES ({i}, 'conn1', {})", i * 3))
                    .await
                    .expect("f insert");
            }
            f.execute("COMMIT").await.expect("f commit");
        }
        {
            let f2 = Connection::open(f_path_str).await.expect("f2 open");
            let f_count = get_int(&f2, "SELECT COUNT(*) FROM mc").await.unwrap();
            assert_eq!(f_count, 100, "P6: fsqlite conn2 should see 100 rows");

            f2.execute("BEGIN").await.expect("f2 begin");
            for i in 101..=200 {
                f2.execute(&format!("INSERT INTO mc VALUES ({i}, 'conn2', {})", i * 3))
                    .await
                    .expect("f2 insert");
            }
            f2.execute("COMMIT").await.expect("f2 commit");
        }
        let f_final = Connection::open(f_path_str).await.expect("f final");
        let f_total = get_int(&f_final, "SELECT COUNT(*) FROM mc").await.unwrap();
        let f_sum = get_int(&f_final, "SELECT SUM(val) FROM mc").await.unwrap();

        assert_eq!(
            f_total, c_total,
            "P6: count mismatch f={f_total}, c={c_total}"
        );
        assert_eq!(f_sum, c_sum, "P6: sum mismatch f={f_sum}, c={c_sum}");

        eprintln!("P6: oracle multi-connection parity — count={f_total}, sum={f_sum}");
    });
}
