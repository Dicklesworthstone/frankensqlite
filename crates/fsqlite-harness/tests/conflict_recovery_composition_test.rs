//! Conflict/recovery assertions on integrated backend (bd-mblr.2.1.2).
//!
//! Tests that force page-level conflicts, rollback/retry paths, and
//! recovery invariants on real file-backed backend components.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;
use tempfile::TempDir;

const BEAD_ID: &str = "bd-mblr.2.1.2";

fn temp_db() -> (TempDir, String) {
    let dir = tempfile::tempdir().expect("create tempdir");
    let db_path = dir.path().join("test.db");
    let db_str = db_path.to_str().expect("path to str").to_owned();
    (dir, db_str)
}

// ─── Recovery After Unclean Close ───────────────────────────────────────

#[test]
fn drop_mid_transaction_does_not_persist_uncommitted_data() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn = Connection::open(&db_str).await.expect("open");
            conn.execute("CREATE TABLE t1 (x INTEGER)")
                .await
                .expect("ddl");
            conn.execute("INSERT INTO t1 VALUES (1)")
                .await
                .expect("baseline");
        }

        // Open, begin transaction, insert, then drop without commit
        {
            let conn = Connection::open(&db_str).await.expect("reopen");
            conn.execute("BEGIN").await.expect("begin");
            conn.execute("INSERT INTO t1 VALUES (2)")
                .await
                .expect("ins");
            conn.execute("INSERT INTO t1 VALUES (3)")
                .await
                .expect("ins");
            // Connection dropped without COMMIT or ROLLBACK
        }

        // Verify only baseline row exists
        {
            let conn = Connection::open(&db_str).await.expect("reopen2");
            let rows = conn.query("SELECT x FROM t1").await.expect("query");
            assert_eq!(rows.len(), 1, "bead_id={BEAD_ID} case=drop_mid_txn_count");
            assert_eq!(
                rows[0].get(0).unwrap(),
                &SqliteValue::Integer(1),
                "bead_id={BEAD_ID} case=drop_mid_txn_val"
            );
        }
    });
}

#[test]
fn drop_mid_concurrent_transaction_does_not_persist() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn = Connection::open(&db_str).await.expect("open");
            conn.execute("CREATE TABLE t1 (x INTEGER)")
                .await
                .expect("ddl");
            conn.execute("INSERT INTO t1 VALUES (100)")
                .await
                .expect("base");
        }

        // Open, BEGIN CONCURRENT, insert, then drop
        {
            let conn = Connection::open(&db_str).await.expect("reopen");
            conn.execute("BEGIN CONCURRENT")
                .await
                .expect("begin concurrent");
            conn.execute("INSERT INTO t1 VALUES (200)")
                .await
                .expect("ins");
            // Dropped without commit
        }

        {
            let conn = Connection::open(&db_str).await.expect("reopen2");
            let rows = conn.query("SELECT x FROM t1").await.expect("query");
            assert_eq!(
                rows.len(),
                1,
                "bead_id={BEAD_ID} case=drop_concurrent_count"
            );
        }
    });
}

#[test]
fn database_consistent_after_many_abort_cycles() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn = Connection::open(&db_str).await.expect("open");
            conn.execute("CREATE TABLE t1 (seq INTEGER)")
                .await
                .expect("ddl");
        }

        // Alternate between committed and aborted transactions
        for cycle in 0..10 {
            let conn = Connection::open(&db_str).await.expect("reopen");
            conn.execute("BEGIN").await.expect("begin");
            conn.execute_with_params(
                "INSERT INTO t1 VALUES (?)",
                &[SqliteValue::Integer(i64::from(cycle))],
            )
            .await
            .expect("insert");

            if cycle % 2 == 0 {
                conn.execute("COMMIT").await.expect("commit");
            } else {
                conn.execute("ROLLBACK").await.expect("rollback");
            }
        }

        {
            let conn = Connection::open(&db_str).await.expect("final reopen");
            let rows = conn
                .query("SELECT seq FROM t1 ORDER BY seq")
                .await
                .expect("query");
            // Only even cycles committed: 0, 2, 4, 6, 8
            assert_eq!(rows.len(), 5, "bead_id={BEAD_ID} case=abort_cycles_count");
            for (i, row) in rows.iter().enumerate() {
                let expected = i64::try_from(i * 2).expect("i64");
                assert_eq!(
                    row.get(0).unwrap(),
                    &SqliteValue::Integer(expected),
                    "bead_id={BEAD_ID} case=abort_cycle_{i}"
                );
            }
        }
    });
}

// ─── Savepoint Rollback Paths ───────────────────────────────────────────

#[test]
fn savepoint_rollback_to_discards_inner_changes() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn = Connection::open(&db_str).await.expect("open");
            conn.execute("CREATE TABLE t1 (x INTEGER)")
                .await
                .expect("ddl");
            conn.execute("BEGIN").await.expect("begin");
            conn.execute("INSERT INTO t1 VALUES (1)")
                .await
                .expect("outer");
            conn.execute("SAVEPOINT sp1").await.expect("savepoint");
            conn.execute("INSERT INTO t1 VALUES (2)")
                .await
                .expect("inner");
            conn.execute("INSERT INTO t1 VALUES (3)")
                .await
                .expect("inner");
            conn.execute("ROLLBACK TO sp1").await.expect("rollback to");
            // Only row 1 should survive within the transaction
            conn.execute("COMMIT").await.expect("commit");
        }

        {
            let conn = Connection::open(&db_str).await.expect("reopen");
            let rows = conn
                .query("SELECT x FROM t1 ORDER BY x")
                .await
                .expect("query");
            assert_eq!(
                rows.len(),
                1,
                "bead_id={BEAD_ID} case=savepoint_rollback_count"
            );
            assert_eq!(
                rows[0].get(0).unwrap(),
                &SqliteValue::Integer(1),
                "bead_id={BEAD_ID} case=savepoint_rollback_val"
            );
        }
    });
}

#[test]
fn nested_savepoints_partial_rollback() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn = Connection::open(&db_str).await.expect("open");
            conn.execute("CREATE TABLE t1 (x INTEGER)")
                .await
                .expect("ddl");
            conn.execute("BEGIN").await.expect("begin");

            conn.execute("INSERT INTO t1 VALUES (1)").await.expect("l0");

            conn.execute("SAVEPOINT sp1").await.expect("sp1");
            conn.execute("INSERT INTO t1 VALUES (2)").await.expect("l1");

            conn.execute("SAVEPOINT sp2").await.expect("sp2");
            conn.execute("INSERT INTO t1 VALUES (3)").await.expect("l2");
            conn.execute("ROLLBACK TO sp2").await.expect("rollback sp2");
            // Row 3 discarded, row 2 kept

            conn.execute("INSERT INTO t1 VALUES (4)")
                .await
                .expect("after sp2");
            conn.execute("RELEASE sp1").await.expect("release sp1");
            // Rows 1, 2, 4 should remain

            conn.execute("COMMIT").await.expect("commit");
        }

        {
            let conn = Connection::open(&db_str).await.expect("reopen");
            let rows = conn
                .query("SELECT x FROM t1 ORDER BY x")
                .await
                .expect("query");
            assert_eq!(
                rows.len(),
                3,
                "bead_id={BEAD_ID} case=nested_savepoint_count"
            );
            assert_eq!(
                rows[0].get(0).unwrap(),
                &SqliteValue::Integer(1),
                "bead_id={BEAD_ID} case=nested_savepoint_1"
            );
            assert_eq!(
                rows[1].get(0).unwrap(),
                &SqliteValue::Integer(2),
                "bead_id={BEAD_ID} case=nested_savepoint_2"
            );
            assert_eq!(
                rows[2].get(0).unwrap(),
                &SqliteValue::Integer(4),
                "bead_id={BEAD_ID} case=nested_savepoint_4"
            );
        }
    });
}

#[test]
fn savepoint_release_commits_inner_changes() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn = Connection::open(&db_str).await.expect("open");
            conn.execute("CREATE TABLE t1 (x INTEGER)")
                .await
                .expect("ddl");
            conn.execute("BEGIN").await.expect("begin");
            conn.execute("INSERT INTO t1 VALUES (1)")
                .await
                .expect("outer");
            conn.execute("SAVEPOINT sp1").await.expect("savepoint");
            conn.execute("INSERT INTO t1 VALUES (2)")
                .await
                .expect("inner");
            conn.execute("RELEASE sp1").await.expect("release");
            conn.execute("COMMIT").await.expect("commit");
        }

        {
            let conn = Connection::open(&db_str).await.expect("reopen");
            let rows = conn
                .query("SELECT x FROM t1 ORDER BY x")
                .await
                .expect("query");
            assert_eq!(
                rows.len(),
                2,
                "bead_id={BEAD_ID} case=savepoint_release_count"
            );
        }
    });
}

// ─── Constraint Violations ──────────────────────────────────────────────

#[test]
fn unique_constraint_violation_does_not_corrupt_state() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn = Connection::open(&db_str).await.expect("open");
            conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("ddl");
            conn.execute("INSERT INTO t1 VALUES (1, 'first')")
                .await
                .expect("ins1");

            // Attempt duplicate key — should fail
            let result = conn.execute("INSERT INTO t1 VALUES (1, 'duplicate')").await;
            assert!(
                result.is_err(),
                "bead_id={BEAD_ID} case=unique_violation_err"
            );

            // Table should still be intact
            conn.execute("INSERT INTO t1 VALUES (2, 'second')")
                .await
                .expect("ins2");
        }

        {
            let conn = Connection::open(&db_str).await.expect("reopen");
            let rows = conn
                .query("SELECT id, val FROM t1 ORDER BY id")
                .await
                .expect("query");
            assert_eq!(
                rows.len(),
                2,
                "bead_id={BEAD_ID} case=after_violation_count"
            );
            assert_eq!(
                rows[0].get(1).unwrap(),
                &SqliteValue::Text("first".into()),
                "bead_id={BEAD_ID} case=original_preserved"
            );
            assert_eq!(
                rows[1].get(1).unwrap(),
                &SqliteValue::Text("second".into()),
                "bead_id={BEAD_ID} case=after_violation_val"
            );
        }
    });
}

#[test]
fn unique_constraint_in_transaction_allows_retry() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn = Connection::open(&db_str).await.expect("open");
            conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("ddl");
            conn.execute("BEGIN").await.expect("begin");
            conn.execute("INSERT INTO t1 VALUES (1, 'a')")
                .await
                .expect("ins");

            // Duplicate in same transaction
            let result = conn.execute("INSERT INTO t1 VALUES (1, 'dup')").await;
            assert!(result.is_err(), "bead_id={BEAD_ID} case=dup_in_txn");

            // Transaction should still be usable — insert a different key
            conn.execute("INSERT INTO t1 VALUES (2, 'b')")
                .await
                .expect("ins2");
            conn.execute("COMMIT").await.expect("commit");
        }

        {
            let conn = Connection::open(&db_str).await.expect("reopen");
            let rows = conn
                .query("SELECT id FROM t1 ORDER BY id")
                .await
                .expect("query");
            assert_eq!(rows.len(), 2, "bead_id={BEAD_ID} case=txn_after_dup_count");
        }
    });
}

#[test]
fn insert_or_replace_conflict_resolution() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn = Connection::open(&db_str).await.expect("open");
            conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("ddl");
            conn.execute("INSERT INTO t1 VALUES (1, 'original')")
                .await
                .expect("ins");
            conn.execute("INSERT OR REPLACE INTO t1 VALUES (1, 'replaced')")
                .await
                .expect("replace");
        }

        {
            let conn = Connection::open(&db_str).await.expect("reopen");
            let rows = conn
                .query("SELECT val FROM t1 WHERE id = 1")
                .await
                .expect("query");
            assert_eq!(rows.len(), 1, "bead_id={BEAD_ID} case=replace_count");
            assert_eq!(
                rows[0].get(0).unwrap(),
                &SqliteValue::Text("replaced".into()),
                "bead_id={BEAD_ID} case=replace_val"
            );
        }
    });
}

#[test]
fn insert_or_ignore_silently_skips_conflict() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn = Connection::open(&db_str).await.expect("open");
            conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("ddl");
            conn.execute("INSERT INTO t1 VALUES (1, 'kept')")
                .await
                .expect("ins");
            conn.execute("INSERT OR IGNORE INTO t1 VALUES (1, 'ignored')")
                .await
                .expect("ignore");
            conn.execute("INSERT OR IGNORE INTO t1 VALUES (2, 'new')")
                .await
                .expect("new");
        }

        {
            let conn = Connection::open(&db_str).await.expect("reopen");
            let rows = conn
                .query("SELECT id, val FROM t1 ORDER BY id")
                .await
                .expect("query");
            assert_eq!(rows.len(), 2, "bead_id={BEAD_ID} case=ignore_count");
            assert_eq!(
                rows[0].get(1).unwrap(),
                &SqliteValue::Text("kept".into()),
                "bead_id={BEAD_ID} case=ignore_original"
            );
            assert_eq!(
                rows[1].get(1).unwrap(),
                &SqliteValue::Text("new".into()),
                "bead_id={BEAD_ID} case=ignore_new"
            );
        }
    });
}

// ─── WAL Recovery Invariants ────────────────────────────────────────────

#[test]
fn wal_mode_recovery_after_writes_preserves_data() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn = Connection::open(&db_str).await.expect("open");
            conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
            conn.execute("CREATE TABLE t1 (x INTEGER)")
                .await
                .expect("ddl");

            // Multiple committed transactions
            for i in 1..=5 {
                conn.execute("BEGIN").await.expect("begin");
                conn.execute_with_params("INSERT INTO t1 VALUES (?)", &[SqliteValue::Integer(i)])
                    .await
                    .expect("insert");
                conn.execute("COMMIT").await.expect("commit");
            }
            // Close without explicit checkpoint
        }

        {
            // Reopen — WAL should be replayed
            let conn = Connection::open(&db_str).await.expect("reopen");
            let rows = conn
                .query("SELECT x FROM t1 ORDER BY x")
                .await
                .expect("query");
            assert_eq!(rows.len(), 5, "bead_id={BEAD_ID} case=wal_recovery_count");
            for (i, row) in rows.iter().enumerate() {
                let expected = i64::try_from(i + 1).expect("i64");
                assert_eq!(
                    row.get(0).unwrap(),
                    &SqliteValue::Integer(expected),
                    "bead_id={BEAD_ID} case=wal_recovery_val_{i}"
                );
            }
        }
    });
}

#[test]
fn wal_mode_uncommitted_transaction_lost_on_reopen() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn = Connection::open(&db_str).await.expect("open");
            conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
            conn.execute("CREATE TABLE t1 (x INTEGER)")
                .await
                .expect("ddl");
            conn.execute("INSERT INTO t1 VALUES (1)")
                .await
                .expect("committed");
        }

        {
            let conn = Connection::open(&db_str).await.expect("reopen");
            conn.execute("BEGIN").await.expect("begin");
            conn.execute("INSERT INTO t1 VALUES (999)")
                .await
                .expect("uncommitted");
            // Drop without commit — in WAL mode, uncommitted data should be lost
        }

        {
            let conn = Connection::open(&db_str).await.expect("reopen2");
            let rows = conn.query("SELECT x FROM t1").await.expect("query");
            assert_eq!(rows.len(), 1, "bead_id={BEAD_ID} case=wal_uncommitted_lost");
            assert_eq!(
                rows[0].get(0).unwrap(),
                &SqliteValue::Integer(1),
                "bead_id={BEAD_ID} case=wal_committed_preserved"
            );
        }
    });
}

// ─── Sequential Transaction Consistency ─────────────────────────────────

#[test]
fn sequential_transactions_build_consistently() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        let conn = Connection::open(&db_str).await.expect("open");
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
            .await
            .expect("ddl");

        // Transaction 1: insert base rows
        conn.execute("BEGIN").await.expect("begin");
        conn.execute("INSERT INTO t1 VALUES (1, 100)")
            .await
            .expect("ins");
        conn.execute("INSERT INTO t1 VALUES (2, 200)")
            .await
            .expect("ins");
        conn.execute("COMMIT").await.expect("commit");

        // Transaction 2: update based on previous transaction's data
        conn.execute("BEGIN").await.expect("begin");
        conn.execute("UPDATE t1 SET val = val + 50 WHERE id = 1")
            .await
            .expect("update");
        conn.execute("COMMIT").await.expect("commit");

        // Transaction 3: insert derived from existing data
        conn.execute("BEGIN").await.expect("begin");
        let derived = conn
            .query("SELECT val FROM t1 WHERE id = 1")
            .await
            .expect("select derived");
        let derived_val = match derived[0].get(0).unwrap() {
            SqliteValue::Integer(v) => *v,
            other => panic!("expected integer, got {other:?}"),
        };
        conn.execute_with_params(
            "INSERT INTO t1 VALUES (3, ?)",
            &[SqliteValue::Integer(derived_val)],
        )
        .await
        .expect("derived insert");
        conn.execute("COMMIT").await.expect("commit");

        drop(conn);

        // Verify final state after reopen
        let conn = Connection::open(&db_str).await.expect("reopen");
        let rows = conn
            .query("SELECT id, val FROM t1 ORDER BY id")
            .await
            .expect("query");
        assert_eq!(rows.len(), 3, "bead_id={BEAD_ID} case=seq_txn_count");
        assert_eq!(
            rows[0].get(1).unwrap(),
            &SqliteValue::Integer(150),
            "bead_id={BEAD_ID} case=seq_txn_updated"
        );
        assert_eq!(
            rows[1].get(1).unwrap(),
            &SqliteValue::Integer(200),
            "bead_id={BEAD_ID} case=seq_txn_unchanged"
        );
        assert_eq!(
            rows[2].get(1).unwrap(),
            &SqliteValue::Integer(150),
            "bead_id={BEAD_ID} case=seq_txn_derived"
        );
    });
}

// ─── Concurrent Mode Recovery ───────────────────────────────────────────

#[test]
fn concurrent_mode_commit_then_reopen_preserves_data() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn = Connection::open(&db_str).await.expect("open");
            conn.execute("CREATE TABLE t1 (x INTEGER)")
                .await
                .expect("ddl");

            // Multiple concurrent transactions, each committed
            for i in 1..=3 {
                conn.execute("BEGIN CONCURRENT")
                    .await
                    .expect("begin concurrent");
                conn.execute_with_params("INSERT INTO t1 VALUES (?)", &[SqliteValue::Integer(i)])
                    .await
                    .expect("insert");
                conn.execute("COMMIT").await.expect("commit");
            }
        }

        {
            let conn = Connection::open(&db_str).await.expect("reopen");
            let rows = conn
                .query("SELECT x FROM t1 ORDER BY x")
                .await
                .expect("query");
            assert_eq!(
                rows.len(),
                3,
                "bead_id={BEAD_ID} case=concurrent_reopen_count"
            );
        }
    });
}

// ─── Multiple Connections to Same File ──────────────────────────────────

#[test]
fn second_connection_reads_committed_data() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        // Connection 1 creates and populates
        let conn1 = Connection::open(&db_str).await.expect("open1");
        conn1
            .execute("CREATE TABLE t1 (x INTEGER)")
            .await
            .expect("ddl");
        conn1
            .execute("INSERT INTO t1 VALUES (42)")
            .await
            .expect("insert");
        drop(conn1);

        // Connection 2 reads
        let conn2 = Connection::open(&db_str).await.expect("open2");
        let rows = conn2.query("SELECT x FROM t1").await.expect("query");
        assert_eq!(
            rows.len(),
            1,
            "bead_id={BEAD_ID} case=second_conn_read_count"
        );
        assert_eq!(
            rows[0].get(0).unwrap(),
            &SqliteValue::Integer(42),
            "bead_id={BEAD_ID} case=second_conn_read_val"
        );
    });
}

// ─── Schema Recovery After Partial DDL ──────────────────────────────────

#[test]
fn schema_consistent_after_failed_ddl_in_transaction() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn = Connection::open(&db_str).await.expect("open");
            conn.execute("CREATE TABLE t1 (x INTEGER)")
                .await
                .expect("ddl");
            conn.execute("INSERT INTO t1 VALUES (1)")
                .await
                .expect("ins");

            // Try to create a duplicate table — should fail
            conn.execute("BEGIN").await.expect("begin");
            let result = conn.execute("CREATE TABLE t1 (y TEXT)").await;
            assert!(result.is_err(), "bead_id={BEAD_ID} case=dup_table_err");
            conn.execute("ROLLBACK").await.expect("rollback");
        }

        {
            let conn = Connection::open(&db_str).await.expect("reopen");
            // Original table should be intact
            let rows = conn.query("SELECT x FROM t1").await.expect("query");
            assert_eq!(
                rows.len(),
                1,
                "bead_id={BEAD_ID} case=schema_recovery_count"
            );
        }
    });
}

// ─── Stress: Rapid Open-Write-Close Cycles ──────────────────────────────

#[test]
fn rapid_open_write_close_cycles_maintain_consistency() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn = Connection::open(&db_str).await.expect("open");
            conn.execute("CREATE TABLE counter (n INTEGER)")
                .await
                .expect("ddl");
            conn.execute("INSERT INTO counter VALUES (0)")
                .await
                .expect("init");
        }

        for _ in 0..20 {
            let conn = Connection::open(&db_str).await.expect("reopen");
            conn.execute("UPDATE counter SET n = n + 1")
                .await
                .expect("incr");
        }

        {
            let conn = Connection::open(&db_str).await.expect("final reopen");
            let rows = conn.query("SELECT n FROM counter").await.expect("query");
            assert_eq!(
                rows[0].get(0).unwrap(),
                &SqliteValue::Integer(20),
                "bead_id={BEAD_ID} case=rapid_cycle_count"
            );
        }
    });
}

// ─── Error Recovery: Invalid SQL After Valid Ops ────────────────────────

#[test]
fn connection_usable_after_sql_error() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        let conn = Connection::open(&db_str).await.expect("open");
        conn.execute("CREATE TABLE t1 (x INTEGER)")
            .await
            .expect("ddl");
        conn.execute("INSERT INTO t1 VALUES (1)")
            .await
            .expect("ins1");

        // Invalid SQL — should error but not corrupt the connection
        let result = conn.execute("INSERT INTO nonexistent VALUES (99)").await;
        assert!(result.is_err(), "bead_id={BEAD_ID} case=invalid_sql_err");

        // Connection should still work
        conn.execute("INSERT INTO t1 VALUES (2)")
            .await
            .expect("ins2");

        drop(conn);

        let conn = Connection::open(&db_str).await.expect("reopen");
        let rows = conn
            .query("SELECT x FROM t1 ORDER BY x")
            .await
            .expect("query");
        assert_eq!(rows.len(), 2, "bead_id={BEAD_ID} case=after_error_count");
    });
}

#[test]
fn transaction_survives_mid_transaction_sql_error() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        let conn = Connection::open(&db_str).await.expect("open");
        conn.execute("CREATE TABLE t1 (x INTEGER PRIMARY KEY)")
            .await
            .expect("ddl");

        conn.execute("BEGIN").await.expect("begin");
        conn.execute("INSERT INTO t1 VALUES (1)")
            .await
            .expect("ins1");

        // Duplicate primary key error mid-transaction
        let result = conn.execute("INSERT INTO t1 VALUES (1)").await;
        assert!(result.is_err(), "bead_id={BEAD_ID} case=mid_txn_error");

        // Transaction should still be active and committable
        conn.execute("INSERT INTO t1 VALUES (2)")
            .await
            .expect("ins2");
        conn.execute("COMMIT").await.expect("commit");

        drop(conn);

        let conn = Connection::open(&db_str).await.expect("reopen");
        let rows = conn
            .query("SELECT x FROM t1 ORDER BY x")
            .await
            .expect("query");
        assert_eq!(rows.len(), 2, "bead_id={BEAD_ID} case=mid_txn_error_count");
    });
}
