//! bd-1dp9.6.7.6.3: Cross-connection, multi-process, rollback, restart,
//! and backend-identity certification suite.
//!
//! Verifies file-backed storage correctness across connection boundaries:
//! - V1: Cross-connection visibility (write on conn A, read on conn B)
//! - V2: Cross-connection visibility with concurrent readers
//! - R1: ROLLBACK reverts uncommitted writes for same + other connections
//! - R2: SAVEPOINT ROLLBACK partial undo verified cross-connection
//! - R3: Nested savepoint rollback chains
//! - P1: Restart recovery (close all, reopen, data persists)
//! - P2: Repeated open/close cycles with data accumulation
//! - B1: Backend identity — file-backed connections return consistent data
//! - B2: Multiple databases on same connection set (isolation)
//! - S1: Stale reader after writer commits (snapshot advancement)
#![recursion_limit = "512"]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

// ─── V1: Cross-connection write-read visibility ───────────────────

#[test]
fn v1_cross_connection_visibility() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("v1.db");
        let path_str = db_path.to_str().expect("path");

        // Writer connection creates and populates
        let writer = Connection::open(path_str).await.expect("writer open");
        writer
            .execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
            .await
            .expect("create");
        writer.execute("BEGIN").await.expect("begin");
        for i in 1..=50 {
            writer
                .execute(&format!("INSERT INTO items VALUES ({i}, 'item_{i}')"))
                .await
                .expect("insert");
        }
        writer.execute("COMMIT").await.expect("commit");

        // Reader connection (opened AFTER commit) must see all 50 rows
        let reader = Connection::open(path_str).await.expect("reader open");
        let rows = count_rows(&reader, "SELECT * FROM items").await;
        assert_eq!(
            rows, 50,
            "V1: reader should see all 50 committed rows, got {rows}"
        );

        // Writer adds more
        writer.execute("BEGIN").await.expect("begin2");
        for i in 51..=100 {
            writer
                .execute(&format!("INSERT INTO items VALUES ({i}, 'item_{i}')"))
                .await
                .expect("insert2");
        }
        writer.execute("COMMIT").await.expect("commit2");

        // Reader should see 100 rows after new query (no stale cache)
        let rows2 = count_rows(&reader, "SELECT * FROM items").await;
        assert!(
            rows2 >= 100,
            "V1: reader should see at least 100 rows after 2nd commit, got {rows2}"
        );

        eprintln!("V1: cross-connection visibility OK — {rows} then {rows2} rows");
    });
}

// ─── V2: Cross-connection concurrent read+write ───────────────────

#[test]
fn v2_concurrent_cross_connection_readwrite() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("v2.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE counters (id INTEGER PRIMARY KEY, val INTEGER)")
                .await
                .expect("create");
            conn.execute("INSERT INTO counters VALUES (1, 0)")
                .await
                .expect("seed");
        }

        let stop = Arc::new(AtomicBool::new(false));
        let writes_done = Arc::new(AtomicU64::new(0));

        // Writer thread: increments counter
        let w_path = path_str.to_string();
        let w_stop = Arc::clone(&stop);
        let w_done = Arc::clone(&writes_done);
        let writer = std::thread::spawn(move || {
            let mut ops = 0u64;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&w_path).await.expect("w open");
                while !w_stop.load(Ordering::Relaxed) {
                    if conn.execute("BEGIN").await.is_ok() {
                        conn.execute("UPDATE counters SET val = val + 1 WHERE id = 1")
                            .await
                            .ok();
                        if conn.execute("COMMIT").await.is_ok() {
                            ops += 1;
                            w_done.store(ops, Ordering::Relaxed);
                        } else {
                            conn.execute("ROLLBACK").await.ok();
                        }
                    }
                }
            });
            ops
        });

        // Reader thread: reads counter value, asserts monotonic
        let r_path = path_str.to_string();
        let r_stop = Arc::clone(&stop);
        let reader = std::thread::spawn(move || {
            let mut last_val: i64 = 0;
            let mut reads = 0u64;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&r_path).await.expect("r open");
                while !r_stop.load(Ordering::Relaxed) {
                    if let Some(val) = get_int(&conn, "SELECT val FROM counters WHERE id = 1").await
                    {
                        assert!(
                            val >= last_val,
                            "V2: counter went backwards: {last_val} -> {val}"
                        );
                        last_val = val;
                        reads += 1;
                    }
                }
            });
            (reads, last_val)
        });

        std::thread::sleep(Duration::from_secs(2));
        stop.store(true, Ordering::Relaxed);

        let total_writes = writer.join().expect("writer must not panic");
        let (total_reads, final_val) = reader.join().expect("reader must not panic");

        assert!(total_writes > 0, "V2: no writes completed");
        assert!(total_reads > 0, "V2: no reads completed");
        eprintln!("V2: {total_writes} writes, {total_reads} reads, final counter={final_val}");
    });
}

// ─── R1: ROLLBACK reverts uncommitted writes ──────────────────────

#[test]
fn r1_rollback_reverts_uncommitted() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("r1.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .expect("create");

        // Committed batch
        conn.execute("BEGIN").await.expect("begin");
        for i in 1..=10 {
            conn.execute(&format!("INSERT INTO data VALUES ({i}, 'committed')"))
                .await
                .expect("insert committed");
        }
        conn.execute("COMMIT").await.expect("commit");

        assert_eq!(count_rows(&conn, "SELECT * FROM data").await, 10);

        // Rolled-back batch
        conn.execute("BEGIN").await.expect("begin2");
        for i in 11..=20 {
            conn.execute(&format!("INSERT INTO data VALUES ({i}, 'rollback_me')"))
                .await
                .expect("insert rollback");
        }
        // Before rollback, should see 20 inside the txn
        assert_eq!(count_rows(&conn, "SELECT * FROM data").await, 20);

        conn.execute("ROLLBACK").await.expect("rollback");

        // After rollback, back to 10
        assert_eq!(
            count_rows(&conn, "SELECT * FROM data").await,
            10,
            "R1: rollback did not revert uncommitted rows"
        );

        // Cross-connection verification
        let reader = Connection::open(path_str).await.expect("reader");
        assert_eq!(
            count_rows(&reader, "SELECT * FROM data").await,
            10,
            "R1: other connection sees rolled-back rows"
        );

        eprintln!("R1: ROLLBACK correctly reverts uncommitted writes");
    });
}

// ─── R2: SAVEPOINT ROLLBACK partial undo ──────────────────────────

#[test]
fn r2_savepoint_rollback_partial_undo() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("r2.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        conn.execute("CREATE TABLE log (seq INTEGER PRIMARY KEY, msg TEXT)")
            .await
            .expect("create");

        conn.execute("BEGIN").await.expect("begin");

        // Phase 1: insert 1-5
        for i in 1..=5 {
            conn.execute(&format!("INSERT INTO log VALUES ({i}, 'phase1')"))
                .await
                .expect("phase1 insert");
        }
        assert_eq!(count_rows(&conn, "SELECT * FROM log").await, 5);

        // Savepoint
        conn.execute("SAVEPOINT sp1").await.expect("savepoint");

        // Phase 2: insert 6-10
        for i in 6..=10 {
            conn.execute(&format!("INSERT INTO log VALUES ({i}, 'phase2')"))
                .await
                .expect("phase2 insert");
        }
        assert_eq!(count_rows(&conn, "SELECT * FROM log").await, 10);

        // Rollback savepoint — only phase 2 reverted
        conn.execute("ROLLBACK TO sp1")
            .await
            .expect("rollback to sp1");

        let after_sp_rollback = count_rows(&conn, "SELECT * FROM log").await;
        assert_eq!(
            after_sp_rollback, 5,
            "R2: savepoint rollback should leave 5 rows, got {after_sp_rollback}"
        );

        // Commit — phase 1 persists
        conn.execute("COMMIT").await.expect("commit");

        let final_count = count_rows(&conn, "SELECT * FROM log").await;
        assert_eq!(final_count, 5, "R2: final count after commit should be 5");

        // Cross-connection
        let reader = Connection::open(path_str).await.expect("reader");
        assert_eq!(count_rows(&reader, "SELECT * FROM log").await, 5);

        eprintln!("R2: SAVEPOINT ROLLBACK partial undo verified");
    });
}

// ─── R3: Nested savepoint rollback chains ─────────────────────────

#[test]
fn r3_nested_savepoint_chains() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("r3.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        conn.execute("CREATE TABLE nested (id INTEGER PRIMARY KEY, level INTEGER)")
            .await
            .expect("create");

        conn.execute("BEGIN").await.expect("begin");

        // Level 0: insert 1-3
        for i in 1..=3 {
            conn.execute(&format!("INSERT INTO nested VALUES ({i}, 0)"))
                .await
                .expect("l0");
        }

        conn.execute("SAVEPOINT sp_a").await.expect("sp_a");

        // Level 1: insert 4-6
        for i in 4..=6 {
            conn.execute(&format!("INSERT INTO nested VALUES ({i}, 1)"))
                .await
                .expect("l1");
        }

        conn.execute("SAVEPOINT sp_b").await.expect("sp_b");

        // Level 2: insert 7-9
        for i in 7..=9 {
            conn.execute(&format!("INSERT INTO nested VALUES ({i}, 2)"))
                .await
                .expect("l2");
        }
        assert_eq!(count_rows(&conn, "SELECT * FROM nested").await, 9);

        // Rollback sp_b — removes level 2 (7-9)
        conn.execute("ROLLBACK TO sp_b")
            .await
            .expect("rollback sp_b");
        assert_eq!(count_rows(&conn, "SELECT * FROM nested").await, 6);

        // Re-insert at level 2 after rollback
        for i in 7..=8 {
            conn.execute(&format!("INSERT INTO nested VALUES ({i}, 2)"))
                .await
                .expect("l2 redo");
        }
        assert_eq!(count_rows(&conn, "SELECT * FROM nested").await, 8);

        // Release sp_b — merges into sp_a
        conn.execute("RELEASE sp_b").await.expect("release sp_b");

        // Rollback sp_a — removes levels 1+2 (4-8)
        conn.execute("ROLLBACK TO sp_a")
            .await
            .expect("rollback sp_a");
        assert_eq!(
            count_rows(&conn, "SELECT * FROM nested").await,
            3,
            "R3: after rolling back sp_a, only level 0 should remain"
        );

        conn.execute("COMMIT").await.expect("commit");

        let final_count = count_rows(&conn, "SELECT * FROM nested").await;
        assert_eq!(final_count, 3, "R3: final committed count should be 3");

        eprintln!("R3: nested savepoint chains verified");
    });
}

// ─── P1: Restart recovery — close all, reopen ─────────────────────

#[test]
fn p1_restart_recovery() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("p1.db");
        let path_str = db_path.to_str().expect("path");

        // Phase 1: create and populate
        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE persist (id INTEGER PRIMARY KEY, data TEXT)")
                .await
                .expect("create");
            conn.execute("BEGIN").await.expect("begin");
            for i in 1..=100 {
                conn.execute(&format!(
                    "INSERT INTO persist VALUES ({i}, 'row_{i}_phase1')"
                ))
                .await
                .expect("insert");
            }
            conn.execute("COMMIT").await.expect("commit");
        }
        // conn dropped — fully closed

        // Phase 2: reopen, verify, add more
        {
            let conn = Connection::open(path_str).await.expect("reopen");
            let rows = count_rows(&conn, "SELECT * FROM persist").await;
            assert_eq!(rows, 100, "P1: phase1 data lost after reopen, got {rows}");

            conn.execute("BEGIN").await.expect("begin2");
            for i in 101..=200 {
                conn.execute(&format!(
                    "INSERT INTO persist VALUES ({i}, 'row_{i}_phase2')"
                ))
                .await
                .expect("insert2");
            }
            conn.execute("COMMIT").await.expect("commit2");
        }
        // conn dropped again

        // Phase 3: final verification
        {
            let conn = Connection::open(path_str).await.expect("reopen2");
            let rows = count_rows(&conn, "SELECT * FROM persist").await;
            assert_eq!(
                rows, 200,
                "P1: phase1+phase2 data lost after second reopen, got {rows}"
            );

            // Verify specific rows
            let phase1 =
                count_rows(&conn, "SELECT * FROM persist WHERE data LIKE '%phase1%'").await;
            let phase2 =
                count_rows(&conn, "SELECT * FROM persist WHERE data LIKE '%phase2%'").await;
            assert_eq!(phase1, 100, "P1: phase1 rows corrupted");
            assert_eq!(phase2, 100, "P1: phase2 rows corrupted");
        }

        eprintln!("P1: restart recovery — 200 rows survive 2 close/reopen cycles");
    });
}

// ─── P2: Repeated open/close with data accumulation ───────────────

#[test]
fn p2_repeated_open_close_accumulation() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("p2.db");
        let path_str = db_path.to_str().expect("path");

        // Create table on first open
        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE acc (id INTEGER PRIMARY KEY, round INTEGER)")
                .await
                .expect("create");
        }

        let rows_per_round = 10;
        let total_rounds = 20;

        for round in 0..total_rounds {
            let conn = Connection::open(path_str).await.expect("open round");

            // Verify accumulated rows from previous rounds
            let expected = round * rows_per_round;
            let actual = count_rows(&conn, "SELECT * FROM acc").await;
            assert_eq!(
                actual, expected,
                "P2: round {round}: expected {expected} rows, got {actual}"
            );

            // Add this round's rows
            conn.execute("BEGIN").await.expect("begin");
            for i in 0..rows_per_round {
                let id = round * rows_per_round + i;
                conn.execute(&format!("INSERT INTO acc VALUES ({id}, {round})"))
                    .await
                    .expect("insert");
            }
            conn.execute("COMMIT").await.expect("commit");
        }

        // Final verification
        let conn = Connection::open(path_str).await.expect("final open");
        let total = count_rows(&conn, "SELECT * FROM acc").await;
        let expected_total = total_rounds * rows_per_round;
        assert_eq!(
            total, expected_total,
            "P2: final count {total} != expected {expected_total}"
        );

        eprintln!(
            "P2: {total_rounds} open/close cycles, {expected_total} rows accumulated correctly"
        );
    });
}

// ─── B1: Backend identity — file-backed consistency ───────────────

#[test]
fn b1_backend_identity_file_backed() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("b1.db");
        let path_str = db_path.to_str().expect("path");

        // Create and populate via connection 1
        let conn1 = Connection::open(path_str).await.expect("conn1");
        conn1
            .execute("CREATE TABLE ident (id INTEGER PRIMARY KEY, src TEXT)")
            .await
            .expect("create");
        conn1.execute("BEGIN").await.expect("begin");
        for i in 1..=50 {
            conn1
                .execute(&format!("INSERT INTO ident VALUES ({i}, 'conn1')"))
                .await
                .expect("insert");
        }
        conn1.execute("COMMIT").await.expect("commit");

        // 4 concurrent readers all see the same data
        let mut readers: Vec<Connection> = Vec::new();
        for _ in 0..4 {
            readers.push(Connection::open(path_str).await.expect("reader open"));
        }

        let mut counts: Vec<usize> = Vec::new();
        for r in &readers {
            counts.push(count_rows(r, "SELECT * FROM ident").await);
        }

        for (i, &c) in counts.iter().enumerate() {
            assert_eq!(c, 50, "B1: reader {i} sees {c} rows instead of 50");
        }

        // All readers see identical data content
        let reference: Vec<String> = conn1
            .query("SELECT id, src FROM ident ORDER BY id")
            .await
            .expect("ref query")
            .iter()
            .map(|row| {
                let id = match row.get(0) {
                    Some(SqliteValue::Integer(v)) => *v,
                    _ => -1,
                };
                let src = match row.get(1) {
                    Some(SqliteValue::Text(s)) => s.as_str().to_string(),
                    _ => "?".to_string(),
                };
                format!("{id}:{src}")
            })
            .collect();

        for (i, reader) in readers.iter().enumerate() {
            let reader_data: Vec<String> = reader
                .query("SELECT id, src FROM ident ORDER BY id")
                .await
                .expect("reader query")
                .iter()
                .map(|row| {
                    let id = match row.get(0) {
                        Some(SqliteValue::Integer(v)) => *v,
                        _ => -1,
                    };
                    let src = match row.get(1) {
                        Some(SqliteValue::Text(s)) => s.as_str().to_string(),
                        _ => "?".to_string(),
                    };
                    format!("{id}:{src}")
                })
                .collect();
            assert_eq!(
                reader_data, reference,
                "B1: reader {i} has different data than conn1"
            );
        }

        eprintln!("B1: backend identity — 4 readers see identical file-backed data");
    });
}

// ─── B2: Multiple databases isolation ─────────────────────────────

#[test]
fn b2_multiple_database_isolation() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_a = dir.path().join("b2_a.db");
        let db_b = dir.path().join("b2_b.db");

        let conn_a = Connection::open(db_a.to_str().expect("a"))
            .await
            .expect("open a");
        let conn_b = Connection::open(db_b.to_str().expect("b"))
            .await
            .expect("open b");

        // Different schemas in each DB
        conn_a
            .execute("CREATE TABLE tbl (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .expect("create a");
        conn_b
            .execute("CREATE TABLE tbl (id INTEGER PRIMARY KEY, num INTEGER)")
            .await
            .expect("create b");

        // Insert different data
        conn_a.execute("BEGIN").await.expect("begin a");
        for i in 1..=10 {
            conn_a
                .execute(&format!("INSERT INTO tbl VALUES ({i}, 'alpha_{i}')"))
                .await
                .expect("insert a");
        }
        conn_a.execute("COMMIT").await.expect("commit a");

        conn_b.execute("BEGIN").await.expect("begin b");
        for i in 1..=20 {
            conn_b
                .execute(&format!("INSERT INTO tbl VALUES ({i}, {i})"))
                .await
                .expect("insert b");
        }
        conn_b.execute("COMMIT").await.expect("commit b");

        // Verify isolation
        assert_eq!(count_rows(&conn_a, "SELECT * FROM tbl").await, 10);
        assert_eq!(count_rows(&conn_b, "SELECT * FROM tbl").await, 20);

        // Verify data types are correct per DB
        let a_val = conn_a
            .query("SELECT val FROM tbl WHERE id = 1")
            .await
            .expect("query a");
        match a_val[0].get(0) {
            Some(SqliteValue::Text(_)) => {}
            other => panic!("B2: DB A should have Text, got {other:?}"),
        }

        let b_val = conn_b
            .query("SELECT num FROM tbl WHERE id = 1")
            .await
            .expect("query b");
        match b_val[0].get(0) {
            Some(SqliteValue::Integer(1)) => {}
            other => panic!("B2: DB B should have Integer(1), got {other:?}"),
        }

        eprintln!("B2: multiple database isolation — separate schemas and data confirmed");
    });
}

// ─── S1: Stale reader snapshot advancement ────────────────────────

#[test]
fn s1_stale_reader_snapshot_advancement() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("s1.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE evolving (id INTEGER PRIMARY KEY, gen INTEGER)")
                .await
                .expect("create");
            conn.execute("INSERT INTO evolving VALUES (1, 0)")
                .await
                .expect("seed");
        }

        let reader = Connection::open(path_str).await.expect("reader open");

        // Read initial state
        let gen0 = get_int(&reader, "SELECT gen FROM evolving WHERE id = 1").await;
        assert_eq!(gen0, Some(0), "S1: initial gen should be 0");

        // Writer updates 10 times
        for generation in 1..=10 {
            let writer = Connection::open(path_str).await.expect("writer open");
            writer
                .execute(&format!(
                    "UPDATE evolving SET gen = {generation} WHERE id = 1"
                ))
                .await
                .expect("update");
        }

        // Reader should see the latest committed value (no stale snapshot)
        let gen_final = get_int(&reader, "SELECT gen FROM evolving WHERE id = 1").await;
        assert!(
            gen_final.is_some(),
            "S1: reader can't read after writer updates"
        );
        let gf = gen_final.unwrap();
        assert!(
            gf >= 1,
            "S1: reader still sees stale gen 0 after 10 writer updates, got {gf}"
        );

        eprintln!("S1: stale reader sees gen={gf} after 10 writer updates");
    });
}

// ─── C1: Concurrent writers no data loss ──────────────────────────

#[test]
fn c1_concurrent_writers_no_data_loss() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("c1.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE writes (id INTEGER PRIMARY KEY, writer INTEGER)")
                .await
                .expect("create");
        }

        let stop = Arc::new(AtomicBool::new(false));
        let total_committed = Arc::new(AtomicU64::new(0));

        // 4 writer threads, each inserting unique IDs
        let threads: Vec<_> = (0..4u64)
            .map(|tid| {
                let path = path_str.to_string();
                let s = Arc::clone(&stop);
                let tc = Arc::clone(&total_committed);
                std::thread::spawn(move || {
                    let mut committed = 0u64;
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(&path).await.expect("open");
                        let mut seq = 0u64;
                        while !s.load(Ordering::Relaxed) {
                            let id = tid * 1_000_000 + seq;
                            if conn.execute("BEGIN").await.is_ok() {
                                if conn
                                    .execute(&format!("INSERT INTO writes VALUES ({id}, {tid})"))
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
                    tc.fetch_add(committed, Ordering::Relaxed);
                    committed
                })
            })
            .collect();

        std::thread::sleep(Duration::from_secs(2));
        stop.store(true, Ordering::Relaxed);

        let per_thread: Vec<u64> = threads
            .into_iter()
            .map(|t| t.join().expect("writer must not panic"))
            .collect();

        let expected_total: u64 = per_thread.iter().sum();
        assert!(expected_total > 0, "C1: no commits succeeded");

        // Verify actual row count matches committed count
        let verify = Connection::open(path_str).await.expect("verify");
        let actual = count_rows(&verify, "SELECT * FROM writes").await;

        assert_eq!(
            actual as u64, expected_total,
            "C1: data loss! actual={actual} vs committed={expected_total}"
        );

        // Verify per-writer counts
        for (tid, &expected) in per_thread.iter().enumerate() {
            let writer_rows = count_rows(
                &verify,
                &format!("SELECT * FROM writes WHERE writer = {tid}"),
            )
            .await;
            assert_eq!(
                writer_rows as u64, expected,
                "C1: writer {tid}: actual={writer_rows} vs committed={expected}"
            );
        }

        eprintln!(
            "C1: 4 writers, {} total committed, per-thread: {:?}",
            expected_total, per_thread
        );
    });
}

// ─── C2: Concurrent writer fairness (Jain's index) ───────────────

#[test]
fn c2_concurrent_writer_fairness() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("c2.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE fair (id INTEGER PRIMARY KEY, writer INTEGER)")
                .await
                .expect("create");
        }

        let stop = Arc::new(AtomicBool::new(false));
        let n_threads = 4u64;

        let threads: Vec<_> = (0..n_threads)
            .map(|tid| {
                let path = path_str.to_string();
                let s = Arc::clone(&stop);
                std::thread::spawn(move || {
                    let mut committed = 0u64;
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(&path).await.expect("open");
                        let mut seq = 0u64;
                        while !s.load(Ordering::Relaxed) {
                            let id = tid * 10_000_000 + seq;
                            if conn.execute("BEGIN").await.is_ok() {
                                if conn
                                    .execute(&format!("INSERT INTO fair VALUES ({id}, {tid})"))
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

        let per_thread: Vec<u64> = threads
            .into_iter()
            .map(|t| t.join().expect("writer must not panic"))
            .collect();

        let total: u64 = per_thread.iter().sum();
        assert!(total > 0, "C2: no commits completed");

        // Jain's fairness index: J = (sum(x_i))^2 / (n * sum(x_i^2))
        // J = 1.0 means perfectly fair, J = 1/n means maximally unfair
        let sum: f64 = per_thread.iter().map(|&x| x as f64).sum();
        let sum_sq: f64 = per_thread.iter().map(|&x| (x as f64) * (x as f64)).sum();
        let n = n_threads as f64;
        let numerator = sum * sum;
        let denominator = n * sum_sq;
        let jain = numerator / denominator;

        eprintln!(
            "C2: fairness — per-thread: {:?}, total: {}, Jain's index: {:.4}",
            per_thread, total, jain
        );

        // Fairness threshold: Jain's index should be > 0.5 (moderate fairness)
        // A perfectly fair system would be 1.0, totally unfair would be 0.25 for 4 threads
        assert!(
            jain > 0.5,
            "C2: Jain's fairness index {jain:.4} below 0.5 threshold — severe unfairness"
        );
    });
}
