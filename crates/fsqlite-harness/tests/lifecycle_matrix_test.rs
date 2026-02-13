//! File-backed connection lifecycle matrix tests (`bd-mblr.2.2.1`).
//!
//! Systematic permutations of:
//!   create → open → close → reopen → begin → savepoint →
//!   commit → rollback → checkpoint
//!
//! All scenarios run on real files (no in-memory shortcuts).
//! Failures emit reproducible context via `diag_assert_*` macros.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;
use std::path::PathBuf;
use tempfile::TempDir;

const BEAD_ID: &str = "bd-mblr.2.2.1";

fn temp_db() -> (TempDir, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("test.db");
    let path_str = path.to_string_lossy().to_string();
    (dir, path_str)
}

fn wal_path(db: &str) -> PathBuf {
    PathBuf::from(format!("{db}-wal"))
}

/// Helper: create a table and insert a row, returning the connection.
fn setup_table(db_str: &str) -> Connection {
    let conn = Connection::open(db_str).expect("open");
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
        .expect("ddl");
    conn.execute("INSERT INTO t1 VALUES (1, 'baseline')")
        .expect("baseline insert");
    conn
}

/// Helper: count rows in t1.
fn count_rows(conn: &Connection) -> i64 {
    let rows = conn.query("SELECT count(*) FROM t1").expect("count");
    match rows[0].get(0).unwrap() {
        SqliteValue::Integer(n) => *n,
        other => panic!("expected integer, got {other:?}"),
    }
}

/// Helper: read val for given id.
fn read_val(conn: &Connection, id: i64) -> Option<String> {
    let rows = conn
        .query_with_params(
            "SELECT val FROM t1 WHERE id = ?",
            &[SqliteValue::Integer(id)],
        )
        .expect("query");
    if rows.is_empty() {
        None
    } else {
        match rows[0].get(0).unwrap() {
            SqliteValue::Text(s) => Some(s.clone()),
            SqliteValue::Null => None,
            other => panic!("expected text, got {other:?}"),
        }
    }
}

// ─── Basic Lifecycle: Create → Close → Reopen ──────────────────────────

#[test]
fn create_close_reopen_preserves_schema_and_data() {
    let (_dir, db_str) = temp_db();

    {
        let conn = setup_table(&db_str);
        // Implicit close via drop
        drop(conn);
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        assert_eq!(
            count_rows(&conn),
            1,
            "bead_id={BEAD_ID} case=create_close_reopen"
        );
        assert_eq!(
            read_val(&conn, 1).as_deref(),
            Some("baseline"),
            "bead_id={BEAD_ID} case=create_close_reopen_val"
        );
    }
}

#[test]
fn multiple_reopen_cycles_no_data_loss() {
    let (_dir, db_str) = temp_db();

    {
        let conn = setup_table(&db_str);
        drop(conn);
    }

    for cycle in 0..5 {
        let conn = Connection::open(&db_str).expect("reopen");
        let id = i64::try_from(cycle + 2).expect("i64");
        conn.execute_with_params(
            "INSERT INTO t1 VALUES (?, ?)",
            &[
                SqliteValue::Integer(id),
                SqliteValue::Text(format!("cycle_{cycle}")),
            ],
        )
        .expect("insert");
        drop(conn);
    }

    let conn = Connection::open(&db_str).expect("final reopen");
    assert_eq!(
        count_rows(&conn),
        6,
        "bead_id={BEAD_ID} case=multiple_reopen_cycles"
    );
}

// ─── Lifecycle: Begin → Commit → Reopen ────────────────────────────────

#[test]
fn begin_commit_reopen_persists() {
    let (_dir, db_str) = temp_db();

    {
        let conn = setup_table(&db_str);
        conn.execute("BEGIN").expect("begin");
        conn.execute("INSERT INTO t1 VALUES (2, 'committed')")
            .expect("insert");
        conn.execute("COMMIT").expect("commit");
    }

    let conn = Connection::open(&db_str).expect("reopen");
    assert_eq!(
        count_rows(&conn),
        2,
        "bead_id={BEAD_ID} case=begin_commit_reopen"
    );
    assert_eq!(
        read_val(&conn, 2).as_deref(),
        Some("committed"),
        "bead_id={BEAD_ID} case=begin_commit_val"
    );
}

#[test]
fn begin_rollback_reopen_discards() {
    let (_dir, db_str) = temp_db();

    {
        let conn = setup_table(&db_str);
        conn.execute("BEGIN").expect("begin");
        conn.execute("INSERT INTO t1 VALUES (2, 'rolled_back')")
            .expect("insert");
        conn.execute("ROLLBACK").expect("rollback");
    }

    let conn = Connection::open(&db_str).expect("reopen");
    assert_eq!(
        count_rows(&conn),
        1,
        "bead_id={BEAD_ID} case=begin_rollback_reopen"
    );
}

#[test]
fn begin_drop_without_commit_discards() {
    let (_dir, db_str) = temp_db();

    {
        let conn = setup_table(&db_str);
        conn.execute("BEGIN").expect("begin");
        conn.execute("INSERT INTO t1 VALUES (2, 'orphaned')")
            .expect("insert");
        // Drop without commit or rollback
    }

    let conn = Connection::open(&db_str).expect("reopen");
    assert_eq!(
        count_rows(&conn),
        1,
        "bead_id={BEAD_ID} case=begin_drop_discards"
    );
}

// ─── Lifecycle: Savepoint Permutations ─────────────────────────────────

#[test]
fn savepoint_commit_reopen_persists() {
    let (_dir, db_str) = temp_db();

    {
        let conn = setup_table(&db_str);
        conn.execute("BEGIN").expect("begin");
        conn.execute("SAVEPOINT sp1").expect("sp1");
        conn.execute("INSERT INTO t1 VALUES (2, 'in_savepoint')")
            .expect("insert");
        conn.execute("RELEASE sp1").expect("release");
        conn.execute("COMMIT").expect("commit");
    }

    let conn = Connection::open(&db_str).expect("reopen");
    assert_eq!(
        count_rows(&conn),
        2,
        "bead_id={BEAD_ID} case=savepoint_commit"
    );
    assert_eq!(
        read_val(&conn, 2).as_deref(),
        Some("in_savepoint"),
        "bead_id={BEAD_ID} case=savepoint_commit_val"
    );
}

#[test]
fn savepoint_rollback_to_then_commit_reopen() {
    let (_dir, db_str) = temp_db();

    {
        let conn = setup_table(&db_str);
        conn.execute("BEGIN").expect("begin");
        conn.execute("INSERT INTO t1 VALUES (2, 'before_sp')")
            .expect("insert");
        conn.execute("SAVEPOINT sp1").expect("sp1");
        conn.execute("INSERT INTO t1 VALUES (3, 'in_sp')")
            .expect("insert");
        conn.execute("ROLLBACK TO sp1").expect("rollback to");
        // Row 3 discarded, row 2 kept
        conn.execute("COMMIT").expect("commit");
    }

    let conn = Connection::open(&db_str).expect("reopen");
    assert_eq!(
        count_rows(&conn),
        2,
        "bead_id={BEAD_ID} case=savepoint_rollback_to_commit"
    );
    assert!(
        read_val(&conn, 3).is_none(),
        "bead_id={BEAD_ID} case=rolled_back_row_gone"
    );
}

#[test]
fn nested_savepoints_mixed_rollback_release() {
    let (_dir, db_str) = temp_db();

    {
        let conn = setup_table(&db_str);
        conn.execute("BEGIN").expect("begin");

        // Level 1: outer savepoint
        conn.execute("SAVEPOINT sp_outer").expect("sp_outer");
        conn.execute("INSERT INTO t1 VALUES (2, 'outer')")
            .expect("outer insert");

        // Level 2: inner savepoint
        conn.execute("SAVEPOINT sp_inner").expect("sp_inner");
        conn.execute("INSERT INTO t1 VALUES (3, 'inner')")
            .expect("inner insert");
        conn.execute("ROLLBACK TO sp_inner")
            .expect("rollback inner");
        // Row 3 discarded

        // Level 2 again: new inner savepoint after rollback
        conn.execute("SAVEPOINT sp_inner2").expect("sp_inner2");
        conn.execute("INSERT INTO t1 VALUES (4, 'inner2')")
            .expect("inner2 insert");
        conn.execute("RELEASE sp_inner2").expect("release inner2");

        // Release outer
        conn.execute("RELEASE sp_outer").expect("release outer");
        conn.execute("COMMIT").expect("commit");
    }

    let conn = Connection::open(&db_str).expect("reopen");
    assert_eq!(
        count_rows(&conn),
        3,
        "bead_id={BEAD_ID} case=nested_sp_mixed"
    );
    // Rows: 1 (baseline), 2 (outer), 4 (inner2). Row 3 was rolled back.
    assert!(
        read_val(&conn, 3).is_none(),
        "bead_id={BEAD_ID} case=inner_rollback_gone"
    );
    assert_eq!(
        read_val(&conn, 4).as_deref(),
        Some("inner2"),
        "bead_id={BEAD_ID} case=inner2_preserved"
    );
}

#[test]
fn savepoint_rollback_all_then_rollback_transaction() {
    let (_dir, db_str) = temp_db();

    {
        let conn = setup_table(&db_str);
        conn.execute("BEGIN").expect("begin");
        conn.execute("SAVEPOINT sp1").expect("sp1");
        conn.execute("INSERT INTO t1 VALUES (2, 'sp1_data')")
            .expect("insert");
        conn.execute("ROLLBACK TO sp1").expect("rollback sp1");
        conn.execute("ROLLBACK").expect("rollback txn");
    }

    let conn = Connection::open(&db_str).expect("reopen");
    assert_eq!(
        count_rows(&conn),
        1,
        "bead_id={BEAD_ID} case=sp_rollback_then_txn_rollback"
    );
}

// ─── Lifecycle: Checkpoint Permutations ────────────────────────────────

#[test]
fn wal_mode_explicit_checkpoint_persists() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("PRAGMA journal_mode=WAL").expect("wal");
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("ddl");
        conn.execute("INSERT INTO t1 VALUES (1, 'before_ckpt')")
            .expect("insert");
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
            .expect("checkpoint");
    }

    let conn = Connection::open(&db_str).expect("reopen");
    assert_eq!(
        count_rows(&conn),
        1,
        "bead_id={BEAD_ID} case=wal_explicit_checkpoint"
    );
    assert_eq!(
        read_val(&conn, 1).as_deref(),
        Some("before_ckpt"),
        "bead_id={BEAD_ID} case=wal_checkpoint_val"
    );
}

#[test]
fn wal_mode_write_after_checkpoint_persists() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("PRAGMA journal_mode=WAL").expect("wal");
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("ddl");

        // Write, checkpoint, write more
        conn.execute("INSERT INTO t1 VALUES (1, 'pre_ckpt')")
            .expect("insert");
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
            .expect("checkpoint");
        conn.execute("INSERT INTO t1 VALUES (2, 'post_ckpt')")
            .expect("post-ckpt insert");
    }

    let conn = Connection::open(&db_str).expect("reopen");
    assert_eq!(
        count_rows(&conn),
        2,
        "bead_id={BEAD_ID} case=write_after_checkpoint"
    );
}

#[test]
fn wal_mode_multiple_checkpoints_no_data_loss() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("PRAGMA journal_mode=WAL").expect("wal");
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("ddl");

        for i in 1..=10 {
            conn.execute_with_params(
                "INSERT INTO t1 VALUES (?, ?)",
                &[
                    SqliteValue::Integer(i),
                    SqliteValue::Text(format!("val_{i}")),
                ],
            )
            .expect("insert");

            if i % 3 == 0 {
                conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                    .expect("checkpoint");
            }
        }
    }

    let conn = Connection::open(&db_str).expect("reopen");
    assert_eq!(
        count_rows(&conn),
        10,
        "bead_id={BEAD_ID} case=multiple_checkpoints"
    );
}

// ─── Lifecycle: DDL + DML Interleaving ─────────────────────────────────

#[test]
fn create_table_then_drop_then_recreate_reopen() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (x INTEGER)").expect("create");
        conn.execute("INSERT INTO t1 VALUES (1)").expect("insert");
        conn.execute("DROP TABLE t1").expect("drop");
        conn.execute("CREATE TABLE t1 (y TEXT)").expect("recreate");
        conn.execute("INSERT INTO t1 VALUES ('hello')")
            .expect("insert text");
    }

    let conn = Connection::open(&db_str).expect("reopen");
    let rows = conn.query("SELECT y FROM t1").expect("query");
    assert_eq!(
        rows.len(),
        1,
        "bead_id={BEAD_ID} case=create_drop_recreate_count"
    );
    assert_eq!(
        rows[0].get(0).unwrap(),
        &SqliteValue::Text("hello".to_owned()),
        "bead_id={BEAD_ID} case=recreated_table_data"
    );
}

#[test]
fn create_index_persists_through_reopen() {
    let (_dir, db_str) = temp_db();

    {
        let conn = setup_table(&db_str);
        conn.execute("CREATE INDEX idx_val ON t1 (val)")
            .expect("create index");
        // Insert rows that use the index
        for i in 2..=10 {
            conn.execute_with_params(
                "INSERT INTO t1 VALUES (?, ?)",
                &[
                    SqliteValue::Integer(i),
                    SqliteValue::Text(format!("row_{i}")),
                ],
            )
            .expect("insert");
        }
    }

    let conn = Connection::open(&db_str).expect("reopen");
    // Query using indexed column
    let rows = conn
        .query("SELECT id FROM t1 WHERE val = 'row_5'")
        .expect("query");
    assert_eq!(rows.len(), 1, "bead_id={BEAD_ID} case=index_persists");
    assert_eq!(
        rows[0].get(0).unwrap(),
        &SqliteValue::Integer(5),
        "bead_id={BEAD_ID} case=index_persists_val"
    );
}

// ─── Lifecycle: Transaction + Reopen Cycles ────────────────────────────

#[test]
fn alternating_commit_rollback_cycles_with_reopen() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("ddl");
    }

    // 10 cycles: even → commit, odd → rollback. Reopen between each.
    for cycle in 0..10 {
        let conn = Connection::open(&db_str).expect("reopen");
        let id = i64::try_from(cycle + 1).expect("i64");
        conn.execute("BEGIN").expect("begin");
        conn.execute_with_params(
            "INSERT INTO t1 VALUES (?, ?)",
            &[
                SqliteValue::Integer(id),
                SqliteValue::Text(format!("cycle_{cycle}")),
            ],
        )
        .expect("insert");
        if cycle % 2 == 0 {
            conn.execute("COMMIT").expect("commit");
        } else {
            conn.execute("ROLLBACK").expect("rollback");
        }
    }

    let conn = Connection::open(&db_str).expect("final reopen");
    // Even cycles (0,2,4,6,8) committed = 5 rows
    assert_eq!(
        count_rows(&conn),
        5,
        "bead_id={BEAD_ID} case=alternating_commit_rollback_cycles"
    );
}

#[test]
fn transaction_across_schema_and_data_changes() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("BEGIN").expect("begin");
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("ddl");
        conn.execute("INSERT INTO t1 VALUES (1, 'first')")
            .expect("insert");
        conn.execute("CREATE TABLE t2 (x INTEGER)").expect("ddl2");
        conn.execute("INSERT INTO t2 VALUES (99)").expect("insert2");
        conn.execute("COMMIT").expect("commit");
    }

    let conn = Connection::open(&db_str).expect("reopen");
    let rows1 = conn.query("SELECT val FROM t1").expect("t1");
    let rows2 = conn.query("SELECT x FROM t2").expect("t2");
    assert_eq!(rows1.len(), 1, "bead_id={BEAD_ID} case=txn_schema_data_t1");
    assert_eq!(rows2.len(), 1, "bead_id={BEAD_ID} case=txn_schema_data_t2");
    assert_eq!(
        rows2[0].get(0).unwrap(),
        &SqliteValue::Integer(99),
        "bead_id={BEAD_ID} case=txn_schema_data_t2_val"
    );
}

// ─── Lifecycle: WAL + Savepoint + Reopen ───────────────────────────────

#[test]
fn wal_mode_savepoint_rollback_to_with_reopen() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("PRAGMA journal_mode=WAL").expect("wal");
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("ddl");

        conn.execute("BEGIN").expect("begin");
        conn.execute("INSERT INTO t1 VALUES (1, 'kept')")
            .expect("insert");
        conn.execute("SAVEPOINT sp1").expect("sp1");
        conn.execute("INSERT INTO t1 VALUES (2, 'discarded')")
            .expect("insert");
        conn.execute("ROLLBACK TO sp1").expect("rollback to");
        conn.execute("INSERT INTO t1 VALUES (3, 'after_rollback')")
            .expect("insert");
        conn.execute("COMMIT").expect("commit");
    }

    let conn = Connection::open(&db_str).expect("reopen");
    assert_eq!(
        count_rows(&conn),
        2,
        "bead_id={BEAD_ID} case=wal_sp_rollback_reopen"
    );
    assert!(
        read_val(&conn, 2).is_none(),
        "bead_id={BEAD_ID} case=wal_sp_discarded_gone"
    );
    assert_eq!(
        read_val(&conn, 3).as_deref(),
        Some("after_rollback"),
        "bead_id={BEAD_ID} case=wal_sp_after_rollback"
    );
}

// ─── Lifecycle: Concurrent Mode + Lifecycle ────────────────────────────

#[test]
fn concurrent_begin_commit_reopen_cycle() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("ddl");

        for i in 1..=5 {
            conn.execute("BEGIN CONCURRENT").expect("begin concurrent");
            conn.execute_with_params(
                "INSERT INTO t1 VALUES (?, ?)",
                &[
                    SqliteValue::Integer(i),
                    SqliteValue::Text(format!("concurrent_{i}")),
                ],
            )
            .expect("insert");
            conn.execute("COMMIT").expect("commit");
        }
    }

    let conn = Connection::open(&db_str).expect("reopen");
    assert_eq!(
        count_rows(&conn),
        5,
        "bead_id={BEAD_ID} case=concurrent_begin_commit_reopen"
    );
}

#[test]
fn concurrent_begin_rollback_reopen_discards() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("ddl");
        conn.execute("INSERT INTO t1 VALUES (1, 'baseline')")
            .expect("baseline");

        conn.execute("BEGIN CONCURRENT").expect("begin concurrent");
        conn.execute("INSERT INTO t1 VALUES (2, 'rolled_back')")
            .expect("insert");
        conn.execute("ROLLBACK").expect("rollback");
    }

    let conn = Connection::open(&db_str).expect("reopen");
    assert_eq!(
        count_rows(&conn),
        1,
        "bead_id={BEAD_ID} case=concurrent_rollback_discards"
    );
}

// ─── Lifecycle: Update/Delete Through Reopen Cycles ────────────────────

#[test]
fn update_persists_through_reopen() {
    let (_dir, db_str) = temp_db();

    {
        let conn = setup_table(&db_str);
        conn.execute("UPDATE t1 SET val = 'updated' WHERE id = 1")
            .expect("update");
    }

    let conn = Connection::open(&db_str).expect("reopen");
    assert_eq!(
        read_val(&conn, 1).as_deref(),
        Some("updated"),
        "bead_id={BEAD_ID} case=update_persists"
    );
}

#[test]
fn delete_persists_through_reopen() {
    let (_dir, db_str) = temp_db();

    {
        let conn = setup_table(&db_str);
        conn.execute("INSERT INTO t1 VALUES (2, 'to_delete')")
            .expect("insert");
        conn.execute("DELETE FROM t1 WHERE id = 2").expect("delete");
    }

    let conn = Connection::open(&db_str).expect("reopen");
    assert_eq!(
        count_rows(&conn),
        1,
        "bead_id={BEAD_ID} case=delete_persists"
    );
    assert!(
        read_val(&conn, 2).is_none(),
        "bead_id={BEAD_ID} case=deleted_row_gone"
    );
}

// ─── Lifecycle: Database File Properties ───────────────────────────────

#[test]
fn database_file_exists_after_create() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (x INTEGER)").expect("ddl");
    }

    let path = PathBuf::from(&db_str);
    assert!(path.exists(), "bead_id={BEAD_ID} case=db_file_exists");
    let metadata = std::fs::metadata(&path).expect("metadata");
    assert!(metadata.len() > 0, "bead_id={BEAD_ID} case=db_file_nonzero");
}

#[test]
fn wal_file_created_in_wal_mode() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("PRAGMA journal_mode=WAL").expect("wal");
        conn.execute("CREATE TABLE t1 (x INTEGER)").expect("ddl");
        conn.execute("INSERT INTO t1 VALUES (1)").expect("insert");
        // WAL file should exist while connection is open
        let wal = wal_path(&db_str);
        assert!(wal.exists(), "bead_id={BEAD_ID} case=wal_file_exists");
    }
}

// ─── Lifecycle: Autocommit vs Explicit Transaction ─────────────────────

#[test]
fn autocommit_each_statement_persists_independently() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("ddl");
        // Each statement auto-commits
        conn.execute("INSERT INTO t1 VALUES (1, 'auto1')")
            .expect("a1");
        conn.execute("INSERT INTO t1 VALUES (2, 'auto2')")
            .expect("a2");
        conn.execute("INSERT INTO t1 VALUES (3, 'auto3')")
            .expect("a3");
    }

    let conn = Connection::open(&db_str).expect("reopen");
    assert_eq!(
        count_rows(&conn),
        3,
        "bead_id={BEAD_ID} case=autocommit_persists"
    );
}

#[test]
fn explicit_transaction_groups_changes_atomically() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("ddl");
        conn.execute("BEGIN").expect("begin");
        conn.execute("INSERT INTO t1 VALUES (1, 'a')").expect("i1");
        conn.execute("INSERT INTO t1 VALUES (2, 'b')").expect("i2");
        conn.execute("UPDATE t1 SET val = 'updated_a' WHERE id = 1")
            .expect("update");
        conn.execute("DELETE FROM t1 WHERE id = 2").expect("delete");
        conn.execute("COMMIT").expect("commit");
    }

    let conn = Connection::open(&db_str).expect("reopen");
    assert_eq!(
        count_rows(&conn),
        1,
        "bead_id={BEAD_ID} case=explicit_txn_atomic"
    );
    assert_eq!(
        read_val(&conn, 1).as_deref(),
        Some("updated_a"),
        "bead_id={BEAD_ID} case=explicit_txn_val"
    );
}

// ─── Lifecycle: Edge Cases ─────────────────────────────────────────────

#[test]
fn empty_transaction_commit_is_safe() {
    let (_dir, db_str) = temp_db();

    {
        let conn = setup_table(&db_str);
        conn.execute("BEGIN").expect("begin");
        // No changes
        conn.execute("COMMIT").expect("commit empty txn");
    }

    let conn = Connection::open(&db_str).expect("reopen");
    assert_eq!(
        count_rows(&conn),
        1,
        "bead_id={BEAD_ID} case=empty_txn_commit"
    );
}

#[test]
fn empty_transaction_rollback_is_safe() {
    let (_dir, db_str) = temp_db();

    {
        let conn = setup_table(&db_str);
        conn.execute("BEGIN").expect("begin");
        // No changes
        conn.execute("ROLLBACK").expect("rollback empty txn");
    }

    let conn = Connection::open(&db_str).expect("reopen");
    assert_eq!(
        count_rows(&conn),
        1,
        "bead_id={BEAD_ID} case=empty_txn_rollback"
    );
}

#[test]
fn rapid_begin_commit_cycles_same_connection() {
    let (_dir, db_str) = temp_db();

    let conn = Connection::open(&db_str).expect("open");
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
        .expect("ddl");

    for i in 1..=20 {
        conn.execute("BEGIN").expect("begin");
        conn.execute_with_params(
            "INSERT INTO t1 VALUES (?, ?)",
            &[
                SqliteValue::Integer(i),
                SqliteValue::Text(format!("rapid_{i}")),
            ],
        )
        .expect("insert");
        conn.execute("COMMIT").expect("commit");
    }

    drop(conn);

    let conn = Connection::open(&db_str).expect("reopen");
    assert_eq!(
        count_rows(&conn),
        20,
        "bead_id={BEAD_ID} case=rapid_begin_commit"
    );
}

#[test]
fn savepoint_without_begin_acts_as_transaction() {
    let (_dir, db_str) = temp_db();

    {
        let conn = setup_table(&db_str);
        // SAVEPOINT outside explicit BEGIN starts an implicit transaction
        conn.execute("SAVEPOINT sp_auto").expect("sp_auto");
        conn.execute("INSERT INTO t1 VALUES (2, 'sp_auto')")
            .expect("insert");
        conn.execute("RELEASE sp_auto").expect("release");
    }

    let conn = Connection::open(&db_str).expect("reopen");
    assert_eq!(
        count_rows(&conn),
        2,
        "bead_id={BEAD_ID} case=savepoint_without_begin"
    );
}

#[test]
fn multiple_tables_in_single_transaction_persist_atomically() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("BEGIN").expect("begin");
        conn.execute("CREATE TABLE t1 (x INTEGER)").expect("ddl1");
        conn.execute("CREATE TABLE t2 (y TEXT)").expect("ddl2");
        conn.execute("CREATE TABLE t3 (z REAL)").expect("ddl3");
        conn.execute("INSERT INTO t1 VALUES (1)").expect("i1");
        conn.execute("INSERT INTO t2 VALUES ('hello')").expect("i2");
        conn.execute("INSERT INTO t3 VALUES (3.14)").expect("i3");
        conn.execute("COMMIT").expect("commit");
    }

    let conn = Connection::open(&db_str).expect("reopen");
    let r1 = conn.query("SELECT x FROM t1").expect("t1");
    let r2 = conn.query("SELECT y FROM t2").expect("t2");
    let r3 = conn.query("SELECT z FROM t3").expect("t3");
    assert_eq!(r1.len(), 1, "bead_id={BEAD_ID} case=multi_table_t1");
    assert_eq!(r2.len(), 1, "bead_id={BEAD_ID} case=multi_table_t2");
    assert_eq!(r3.len(), 1, "bead_id={BEAD_ID} case=multi_table_t3");
}

// ─── Lifecycle: Journal Mode Transitions ───────────────────────────────

#[test]
fn journal_mode_default_then_wal_then_data_persists() {
    let (_dir, db_str) = temp_db();

    {
        // Start in default (DELETE) journal mode
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("ddl");
        conn.execute("INSERT INTO t1 VALUES (1, 'delete_mode')")
            .expect("insert");

        // Switch to WAL mode
        conn.execute("PRAGMA journal_mode=WAL").expect("wal");
        conn.execute("INSERT INTO t1 VALUES (2, 'wal_mode')")
            .expect("insert");
    }

    let conn = Connection::open(&db_str).expect("reopen");
    assert_eq!(
        count_rows(&conn),
        2,
        "bead_id={BEAD_ID} case=journal_mode_transition"
    );
    assert_eq!(
        read_val(&conn, 1).as_deref(),
        Some("delete_mode"),
        "bead_id={BEAD_ID} case=pre_wal_val"
    );
    assert_eq!(
        read_val(&conn, 2).as_deref(),
        Some("wal_mode"),
        "bead_id={BEAD_ID} case=post_wal_val"
    );
}
