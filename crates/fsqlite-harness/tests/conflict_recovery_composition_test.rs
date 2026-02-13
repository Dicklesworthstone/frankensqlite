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
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (x INTEGER)").expect("ddl");
        conn.execute("INSERT INTO t1 VALUES (1)").expect("baseline");
    }

    // Open, begin transaction, insert, then drop without commit
    {
        let conn = Connection::open(&db_str).expect("reopen");
        conn.execute("BEGIN").expect("begin");
        conn.execute("INSERT INTO t1 VALUES (2)").expect("ins");
        conn.execute("INSERT INTO t1 VALUES (3)").expect("ins");
        // Connection dropped without COMMIT or ROLLBACK
    }

    // Verify only baseline row exists
    {
        let conn = Connection::open(&db_str).expect("reopen2");
        let rows = conn.query("SELECT x FROM t1").expect("query");
        assert_eq!(rows.len(), 1, "bead_id={BEAD_ID} case=drop_mid_txn_count");
        assert_eq!(
            rows[0].get(0).unwrap(),
            &SqliteValue::Integer(1),
            "bead_id={BEAD_ID} case=drop_mid_txn_val"
        );
    }
}

#[test]
fn drop_mid_concurrent_transaction_does_not_persist() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (x INTEGER)").expect("ddl");
        conn.execute("INSERT INTO t1 VALUES (100)").expect("base");
    }

    // Open, BEGIN CONCURRENT, insert, then drop
    {
        let conn = Connection::open(&db_str).expect("reopen");
        conn.execute("BEGIN CONCURRENT").expect("begin concurrent");
        conn.execute("INSERT INTO t1 VALUES (200)").expect("ins");
        // Dropped without commit
    }

    {
        let conn = Connection::open(&db_str).expect("reopen2");
        let rows = conn.query("SELECT x FROM t1").expect("query");
        assert_eq!(
            rows.len(),
            1,
            "bead_id={BEAD_ID} case=drop_concurrent_count"
        );
    }
}

#[test]
fn database_consistent_after_many_abort_cycles() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (seq INTEGER)").expect("ddl");
    }

    // Alternate between committed and aborted transactions
    for cycle in 0..10 {
        let conn = Connection::open(&db_str).expect("reopen");
        conn.execute("BEGIN").expect("begin");
        conn.execute_with_params(
            "INSERT INTO t1 VALUES (?)",
            &[SqliteValue::Integer(i64::from(cycle))],
        )
        .expect("insert");

        if cycle % 2 == 0 {
            conn.execute("COMMIT").expect("commit");
        } else {
            conn.execute("ROLLBACK").expect("rollback");
        }
    }

    {
        let conn = Connection::open(&db_str).expect("final reopen");
        let rows = conn
            .query("SELECT seq FROM t1 ORDER BY seq")
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
}

// ─── Savepoint Rollback Paths ───────────────────────────────────────────

#[test]
fn savepoint_rollback_to_discards_inner_changes() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (x INTEGER)").expect("ddl");
        conn.execute("BEGIN").expect("begin");
        conn.execute("INSERT INTO t1 VALUES (1)").expect("outer");
        conn.execute("SAVEPOINT sp1").expect("savepoint");
        conn.execute("INSERT INTO t1 VALUES (2)").expect("inner");
        conn.execute("INSERT INTO t1 VALUES (3)").expect("inner");
        conn.execute("ROLLBACK TO sp1").expect("rollback to");
        // Only row 1 should survive within the transaction
        conn.execute("COMMIT").expect("commit");
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        let rows = conn.query("SELECT x FROM t1 ORDER BY x").expect("query");
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
}

#[test]
fn nested_savepoints_partial_rollback() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (x INTEGER)").expect("ddl");
        conn.execute("BEGIN").expect("begin");

        conn.execute("INSERT INTO t1 VALUES (1)").expect("l0");

        conn.execute("SAVEPOINT sp1").expect("sp1");
        conn.execute("INSERT INTO t1 VALUES (2)").expect("l1");

        conn.execute("SAVEPOINT sp2").expect("sp2");
        conn.execute("INSERT INTO t1 VALUES (3)").expect("l2");
        conn.execute("ROLLBACK TO sp2").expect("rollback sp2");
        // Row 3 discarded, row 2 kept

        conn.execute("INSERT INTO t1 VALUES (4)")
            .expect("after sp2");
        conn.execute("RELEASE sp1").expect("release sp1");
        // Rows 1, 2, 4 should remain

        conn.execute("COMMIT").expect("commit");
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        let rows = conn.query("SELECT x FROM t1 ORDER BY x").expect("query");
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
}

#[test]
fn savepoint_release_commits_inner_changes() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (x INTEGER)").expect("ddl");
        conn.execute("BEGIN").expect("begin");
        conn.execute("INSERT INTO t1 VALUES (1)").expect("outer");
        conn.execute("SAVEPOINT sp1").expect("savepoint");
        conn.execute("INSERT INTO t1 VALUES (2)").expect("inner");
        conn.execute("RELEASE sp1").expect("release");
        conn.execute("COMMIT").expect("commit");
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        let rows = conn.query("SELECT x FROM t1 ORDER BY x").expect("query");
        assert_eq!(
            rows.len(),
            2,
            "bead_id={BEAD_ID} case=savepoint_release_count"
        );
    }
}

// ─── Constraint Violations ──────────────────────────────────────────────

#[test]
fn unique_constraint_violation_does_not_corrupt_state() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("ddl");
        conn.execute("INSERT INTO t1 VALUES (1, 'first')")
            .expect("ins1");

        // Attempt duplicate key — should fail
        let result = conn.execute("INSERT INTO t1 VALUES (1, 'duplicate')");
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=unique_violation_err"
        );

        // Table should still be intact
        conn.execute("INSERT INTO t1 VALUES (2, 'second')")
            .expect("ins2");
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        let rows = conn
            .query("SELECT id, val FROM t1 ORDER BY id")
            .expect("query");
        assert_eq!(
            rows.len(),
            2,
            "bead_id={BEAD_ID} case=after_violation_count"
        );
        assert_eq!(
            rows[0].get(1).unwrap(),
            &SqliteValue::Text("first".to_owned()),
            "bead_id={BEAD_ID} case=original_preserved"
        );
        assert_eq!(
            rows[1].get(1).unwrap(),
            &SqliteValue::Text("second".to_owned()),
            "bead_id={BEAD_ID} case=after_violation_val"
        );
    }
}

#[test]
fn unique_constraint_in_transaction_allows_retry() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("ddl");
        conn.execute("BEGIN").expect("begin");
        conn.execute("INSERT INTO t1 VALUES (1, 'a')").expect("ins");

        // Duplicate in same transaction
        let result = conn.execute("INSERT INTO t1 VALUES (1, 'dup')");
        assert!(result.is_err(), "bead_id={BEAD_ID} case=dup_in_txn");

        // Transaction should still be usable — insert a different key
        conn.execute("INSERT INTO t1 VALUES (2, 'b')")
            .expect("ins2");
        conn.execute("COMMIT").expect("commit");
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        let rows = conn.query("SELECT id FROM t1 ORDER BY id").expect("query");
        assert_eq!(rows.len(), 2, "bead_id={BEAD_ID} case=txn_after_dup_count");
    }
}

#[test]
fn insert_or_replace_conflict_resolution() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("ddl");
        conn.execute("INSERT INTO t1 VALUES (1, 'original')")
            .expect("ins");
        conn.execute("INSERT OR REPLACE INTO t1 VALUES (1, 'replaced')")
            .expect("replace");
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        let rows = conn
            .query("SELECT val FROM t1 WHERE id = 1")
            .expect("query");
        assert_eq!(rows.len(), 1, "bead_id={BEAD_ID} case=replace_count");
        assert_eq!(
            rows[0].get(0).unwrap(),
            &SqliteValue::Text("replaced".to_owned()),
            "bead_id={BEAD_ID} case=replace_val"
        );
    }
}

#[test]
fn insert_or_ignore_silently_skips_conflict() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("ddl");
        conn.execute("INSERT INTO t1 VALUES (1, 'kept')")
            .expect("ins");
        conn.execute("INSERT OR IGNORE INTO t1 VALUES (1, 'ignored')")
            .expect("ignore");
        conn.execute("INSERT OR IGNORE INTO t1 VALUES (2, 'new')")
            .expect("new");
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        let rows = conn
            .query("SELECT id, val FROM t1 ORDER BY id")
            .expect("query");
        assert_eq!(rows.len(), 2, "bead_id={BEAD_ID} case=ignore_count");
        assert_eq!(
            rows[0].get(1).unwrap(),
            &SqliteValue::Text("kept".to_owned()),
            "bead_id={BEAD_ID} case=ignore_original"
        );
        assert_eq!(
            rows[1].get(1).unwrap(),
            &SqliteValue::Text("new".to_owned()),
            "bead_id={BEAD_ID} case=ignore_new"
        );
    }
}

// ─── WAL Recovery Invariants ────────────────────────────────────────────

#[test]
fn wal_mode_recovery_after_writes_preserves_data() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("PRAGMA journal_mode=WAL").expect("wal");
        conn.execute("CREATE TABLE t1 (x INTEGER)").expect("ddl");

        // Multiple committed transactions
        for i in 1..=5 {
            conn.execute("BEGIN").expect("begin");
            conn.execute_with_params("INSERT INTO t1 VALUES (?)", &[SqliteValue::Integer(i)])
                .expect("insert");
            conn.execute("COMMIT").expect("commit");
        }
        // Close without explicit checkpoint
    }

    {
        // Reopen — WAL should be replayed
        let conn = Connection::open(&db_str).expect("reopen");
        let rows = conn.query("SELECT x FROM t1 ORDER BY x").expect("query");
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
}

#[test]
fn wal_mode_uncommitted_transaction_lost_on_reopen() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("PRAGMA journal_mode=WAL").expect("wal");
        conn.execute("CREATE TABLE t1 (x INTEGER)").expect("ddl");
        conn.execute("INSERT INTO t1 VALUES (1)")
            .expect("committed");
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        conn.execute("BEGIN").expect("begin");
        conn.execute("INSERT INTO t1 VALUES (999)")
            .expect("uncommitted");
        // Drop without commit — in WAL mode, uncommitted data should be lost
    }

    {
        let conn = Connection::open(&db_str).expect("reopen2");
        let rows = conn.query("SELECT x FROM t1").expect("query");
        assert_eq!(rows.len(), 1, "bead_id={BEAD_ID} case=wal_uncommitted_lost");
        assert_eq!(
            rows[0].get(0).unwrap(),
            &SqliteValue::Integer(1),
            "bead_id={BEAD_ID} case=wal_committed_preserved"
        );
    }
}

// ─── Sequential Transaction Consistency ─────────────────────────────────

#[test]
fn sequential_transactions_build_consistently() {
    let (_dir, db_str) = temp_db();

    let conn = Connection::open(&db_str).expect("open");
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
        .expect("ddl");

    // Transaction 1: insert base rows
    conn.execute("BEGIN").expect("begin");
    conn.execute("INSERT INTO t1 VALUES (1, 100)").expect("ins");
    conn.execute("INSERT INTO t1 VALUES (2, 200)").expect("ins");
    conn.execute("COMMIT").expect("commit");

    // Transaction 2: update based on previous transaction's data
    conn.execute("BEGIN").expect("begin");
    conn.execute("UPDATE t1 SET val = val + 50 WHERE id = 1")
        .expect("update");
    conn.execute("COMMIT").expect("commit");

    // Transaction 3: insert derived from existing data
    conn.execute("BEGIN").expect("begin");
    let derived = conn
        .query("SELECT val FROM t1 WHERE id = 1")
        .expect("select derived");
    let derived_val = match derived[0].get(0).unwrap() {
        SqliteValue::Integer(v) => *v,
        other => panic!("expected integer, got {other:?}"),
    };
    conn.execute_with_params(
        "INSERT INTO t1 VALUES (3, ?)",
        &[SqliteValue::Integer(derived_val)],
    )
    .expect("derived insert");
    conn.execute("COMMIT").expect("commit");

    drop(conn);

    // Verify final state after reopen
    let conn = Connection::open(&db_str).expect("reopen");
    let rows = conn
        .query("SELECT id, val FROM t1 ORDER BY id")
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
}

// ─── Concurrent Mode Recovery ───────────────────────────────────────────

#[test]
fn concurrent_mode_commit_then_reopen_preserves_data() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (x INTEGER)").expect("ddl");

        // Multiple concurrent transactions, each committed
        for i in 1..=3 {
            conn.execute("BEGIN CONCURRENT").expect("begin concurrent");
            conn.execute_with_params("INSERT INTO t1 VALUES (?)", &[SqliteValue::Integer(i)])
                .expect("insert");
            conn.execute("COMMIT").expect("commit");
        }
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        let rows = conn.query("SELECT x FROM t1 ORDER BY x").expect("query");
        assert_eq!(
            rows.len(),
            3,
            "bead_id={BEAD_ID} case=concurrent_reopen_count"
        );
    }
}

// ─── Multiple Connections to Same File ──────────────────────────────────

#[test]
fn second_connection_reads_committed_data() {
    let (_dir, db_str) = temp_db();

    // Connection 1 creates and populates
    let conn1 = Connection::open(&db_str).expect("open1");
    conn1.execute("CREATE TABLE t1 (x INTEGER)").expect("ddl");
    conn1.execute("INSERT INTO t1 VALUES (42)").expect("insert");
    drop(conn1);

    // Connection 2 reads
    let conn2 = Connection::open(&db_str).expect("open2");
    let rows = conn2.query("SELECT x FROM t1").expect("query");
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
}

// ─── Schema Recovery After Partial DDL ──────────────────────────────────

#[test]
fn schema_consistent_after_failed_ddl_in_transaction() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (x INTEGER)").expect("ddl");
        conn.execute("INSERT INTO t1 VALUES (1)").expect("ins");

        // Try to create a duplicate table — should fail
        conn.execute("BEGIN").expect("begin");
        let result = conn.execute("CREATE TABLE t1 (y TEXT)");
        assert!(result.is_err(), "bead_id={BEAD_ID} case=dup_table_err");
        conn.execute("ROLLBACK").expect("rollback");
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        // Original table should be intact
        let rows = conn.query("SELECT x FROM t1").expect("query");
        assert_eq!(
            rows.len(),
            1,
            "bead_id={BEAD_ID} case=schema_recovery_count"
        );
    }
}

// ─── Stress: Rapid Open-Write-Close Cycles ──────────────────────────────

#[test]
fn rapid_open_write_close_cycles_maintain_consistency() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE counter (n INTEGER)")
            .expect("ddl");
        conn.execute("INSERT INTO counter VALUES (0)")
            .expect("init");
    }

    for _ in 0..20 {
        let conn = Connection::open(&db_str).expect("reopen");
        conn.execute("UPDATE counter SET n = n + 1").expect("incr");
    }

    {
        let conn = Connection::open(&db_str).expect("final reopen");
        let rows = conn.query("SELECT n FROM counter").expect("query");
        assert_eq!(
            rows[0].get(0).unwrap(),
            &SqliteValue::Integer(20),
            "bead_id={BEAD_ID} case=rapid_cycle_count"
        );
    }
}

// ─── Error Recovery: Invalid SQL After Valid Ops ────────────────────────

#[test]
fn connection_usable_after_sql_error() {
    let (_dir, db_str) = temp_db();

    let conn = Connection::open(&db_str).expect("open");
    conn.execute("CREATE TABLE t1 (x INTEGER)").expect("ddl");
    conn.execute("INSERT INTO t1 VALUES (1)").expect("ins1");

    // Invalid SQL — should error but not corrupt the connection
    let result = conn.execute("INSERT INTO nonexistent VALUES (99)");
    assert!(result.is_err(), "bead_id={BEAD_ID} case=invalid_sql_err");

    // Connection should still work
    conn.execute("INSERT INTO t1 VALUES (2)").expect("ins2");

    drop(conn);

    let conn = Connection::open(&db_str).expect("reopen");
    let rows = conn.query("SELECT x FROM t1 ORDER BY x").expect("query");
    assert_eq!(rows.len(), 2, "bead_id={BEAD_ID} case=after_error_count");
}

#[test]
fn transaction_survives_mid_transaction_sql_error() {
    let (_dir, db_str) = temp_db();

    let conn = Connection::open(&db_str).expect("open");
    conn.execute("CREATE TABLE t1 (x INTEGER PRIMARY KEY)")
        .expect("ddl");

    conn.execute("BEGIN").expect("begin");
    conn.execute("INSERT INTO t1 VALUES (1)").expect("ins1");

    // Duplicate primary key error mid-transaction
    let result = conn.execute("INSERT INTO t1 VALUES (1)");
    assert!(result.is_err(), "bead_id={BEAD_ID} case=mid_txn_error");

    // Transaction should still be active and committable
    conn.execute("INSERT INTO t1 VALUES (2)").expect("ins2");
    conn.execute("COMMIT").expect("commit");

    drop(conn);

    let conn = Connection::open(&db_str).expect("reopen");
    let rows = conn.query("SELECT x FROM t1 ORDER BY x").expect("query");
    assert_eq!(rows.len(), 2, "bead_id={BEAD_ID} case=mid_txn_error_count");
}
