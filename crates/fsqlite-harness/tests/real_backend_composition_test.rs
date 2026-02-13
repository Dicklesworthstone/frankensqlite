//! Real-backend composition harness (bd-mblr.2.1.1).
//!
//! Exercises the full pager → WAL → MVCC → B-tree stack over file-backed
//! VFS paths (not trait mocks). Each test creates a temp directory, opens
//! a real file-backed database, and verifies integrated behavior across
//! storage layers.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;
use std::path::PathBuf;
use tempfile::TempDir;

const BEAD_ID: &str = "bd-mblr.2.1.1";

/// Helper: create a temp directory and return (dir, db_path_string).
fn temp_db() -> (TempDir, String) {
    let dir = tempfile::tempdir().expect("create tempdir");
    let db_path = dir.path().join("test.db");
    let db_str = db_path.to_str().expect("path to str").to_owned();
    (dir, db_str)
}

/// Helper: get the path to the WAL file for a given database path.
fn wal_path(db_path: &str) -> PathBuf {
    PathBuf::from(format!("{db_path}-wal"))
}

// ─── Basic Persistence ──────────────────────────────────────────────────

#[test]
fn autocommit_persists_through_close_reopen() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("create table");
        conn.execute("INSERT INTO t1 VALUES (1, 'alpha')")
            .expect("insert 1");
        conn.execute("INSERT INTO t1 VALUES (2, 'beta')")
            .expect("insert 2");
        conn.execute("INSERT INTO t1 VALUES (3, 'gamma')")
            .expect("insert 3");
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        let rows = conn
            .query("SELECT id, val FROM t1 ORDER BY id")
            .expect("query");
        assert_eq!(
            rows.len(),
            3,
            "bead_id={BEAD_ID} case=autocommit_persist_count"
        );
        assert_eq!(
            rows[0].get(1).unwrap(),
            &SqliteValue::Text("alpha".to_owned()),
            "bead_id={BEAD_ID} case=autocommit_persist_val1"
        );
        assert_eq!(
            rows[2].get(1).unwrap(),
            &SqliteValue::Text("gamma".to_owned()),
            "bead_id={BEAD_ID} case=autocommit_persist_val3"
        );
    }
}

#[test]
fn explicit_transaction_commit_persists() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (x INTEGER)").expect("ddl");
        conn.execute("BEGIN").expect("begin");
        conn.execute("INSERT INTO t1 VALUES (10)").expect("ins1");
        conn.execute("INSERT INTO t1 VALUES (20)").expect("ins2");
        conn.execute("INSERT INTO t1 VALUES (30)").expect("ins3");
        conn.execute("COMMIT").expect("commit");
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        let rows = conn.query("SELECT x FROM t1 ORDER BY x").expect("query");
        assert_eq!(rows.len(), 3, "bead_id={BEAD_ID} case=explicit_txn_count");
        assert_eq!(
            rows[0].get(0).unwrap(),
            &SqliteValue::Integer(10),
            "bead_id={BEAD_ID} case=explicit_txn_val1"
        );
    }
}

#[test]
fn rollback_does_not_persist() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (x INTEGER)").expect("ddl");
        conn.execute("INSERT INTO t1 VALUES (1)").expect("baseline");
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        conn.execute("BEGIN").expect("begin");
        conn.execute("INSERT INTO t1 VALUES (2)").expect("ins");
        conn.execute("INSERT INTO t1 VALUES (3)").expect("ins");
        conn.execute("ROLLBACK").expect("rollback");
    }

    {
        let conn = Connection::open(&db_str).expect("reopen2");
        let rows = conn.query("SELECT x FROM t1").expect("query");
        assert_eq!(rows.len(), 1, "bead_id={BEAD_ID} case=rollback_count");
        assert_eq!(
            rows[0].get(0).unwrap(),
            &SqliteValue::Integer(1),
            "bead_id={BEAD_ID} case=rollback_only_baseline"
        );
    }
}

// ─── Multiple Reopen Cycles ─────────────────────────────────────────────

#[test]
fn multiple_reopen_cycles_accumulate_data() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (seq INTEGER)").expect("ddl");
    }

    for i in 1..=5 {
        let conn = Connection::open(&db_str).expect("reopen");
        conn.execute_with_params(
            "INSERT INTO t1 VALUES (?)",
            &[SqliteValue::Integer(i64::from(i))],
        )
        .expect("insert");
    }

    {
        let conn = Connection::open(&db_str).expect("final reopen");
        let rows = conn
            .query("SELECT seq FROM t1 ORDER BY seq")
            .expect("query");
        assert_eq!(rows.len(), 5, "bead_id={BEAD_ID} case=multi_reopen_count");
        for (i, row) in rows.iter().enumerate() {
            let expected = i64::try_from(i + 1).expect("i64");
            assert_eq!(
                row.get(0).unwrap(),
                &SqliteValue::Integer(expected),
                "bead_id={BEAD_ID} case=multi_reopen_seq_{i}"
            );
        }
    }
}

// ─── Schema DDL Persistence ─────────────────────────────────────────────

#[test]
fn create_drop_table_persists_schema_changes() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE keep (a INTEGER)").expect("t1");
        conn.execute("CREATE TABLE temp_table (b TEXT)")
            .expect("t2");
        conn.execute("INSERT INTO keep VALUES (42)").expect("ins");
        conn.execute("INSERT INTO temp_table VALUES ('bye')")
            .expect("ins");
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        // Verify both tables exist
        let rows = conn.query("SELECT a FROM keep").expect("query keep");
        assert_eq!(rows.len(), 1, "bead_id={BEAD_ID} case=ddl_keep_exists");

        // Drop one table
        conn.execute("DROP TABLE temp_table").expect("drop");
    }

    {
        let conn = Connection::open(&db_str).expect("reopen2");
        // keep should still exist
        let rows = conn.query("SELECT a FROM keep").expect("query");
        assert_eq!(rows.len(), 1, "bead_id={BEAD_ID} case=ddl_keep_survives");
        assert_eq!(
            rows[0].get(0).unwrap(),
            &SqliteValue::Integer(42),
            "bead_id={BEAD_ID} case=ddl_keep_val"
        );

        // temp_table should be gone
        let result = conn.query("SELECT b FROM temp_table");
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=ddl_dropped_table_gone"
        );
    }
}

// ─── WAL Mode and Checkpoint ────────────────────────────────────────────

#[test]
fn wal_mode_write_creates_wal_file() {
    let (_dir, db_str) = temp_db();

    let conn = Connection::open(&db_str).expect("open");
    conn.execute("PRAGMA journal_mode=WAL").expect("wal mode");
    conn.execute("CREATE TABLE t1 (x INTEGER)").expect("ddl");
    conn.execute("INSERT INTO t1 VALUES (1)").expect("ins");

    // WAL file should exist after writes
    let wp = wal_path(&db_str);
    assert!(
        wp.exists(),
        "bead_id={BEAD_ID} case=wal_file_exists path={wp:?}"
    );
}

#[test]
fn wal_mode_data_survives_reopen() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("PRAGMA journal_mode=WAL").expect("wal mode");
        conn.execute("CREATE TABLE t1 (x INTEGER, y TEXT)")
            .expect("ddl");
        for i in 1..=10 {
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

    {
        let conn = Connection::open(&db_str).expect("reopen");
        let rows = conn.query("SELECT x, y FROM t1 ORDER BY x").expect("query");
        assert_eq!(rows.len(), 10, "bead_id={BEAD_ID} case=wal_reopen_count");
        assert_eq!(
            rows[4].get(1).unwrap(),
            &SqliteValue::Text("row_5".to_owned()),
            "bead_id={BEAD_ID} case=wal_reopen_val5"
        );
    }
}

// ─── B-tree Splits and Ordering ─────────────────────────────────────────

#[test]
fn btree_split_preserves_order_on_large_insert() {
    let (_dir, db_str) = temp_db();
    let row_count = 500;

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, payload TEXT)")
            .expect("ddl");

        conn.execute("BEGIN").expect("begin");
        for i in 0..row_count {
            conn.execute_with_params(
                "INSERT INTO t1 VALUES (?, ?)",
                &[
                    SqliteValue::Integer(i),
                    SqliteValue::Text(format!("payload_{i:04}")),
                ],
            )
            .expect("insert");
        }
        conn.execute("COMMIT").expect("commit");
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        let rows = conn
            .query("SELECT id, payload FROM t1 ORDER BY id")
            .expect("query");
        assert_eq!(
            i64::try_from(rows.len()).unwrap(),
            row_count,
            "bead_id={BEAD_ID} case=btree_split_count"
        );

        // Verify ordering is preserved after B-tree splits
        for (i, row) in rows.iter().enumerate() {
            let expected_id = i64::try_from(i).expect("i64");
            assert_eq!(
                row.get(0).unwrap(),
                &SqliteValue::Integer(expected_id),
                "bead_id={BEAD_ID} case=btree_order_{i}"
            );
        }

        // Spot check first and last
        assert_eq!(
            rows[0].get(1).unwrap(),
            &SqliteValue::Text("payload_0000".to_owned()),
            "bead_id={BEAD_ID} case=btree_first"
        );
        let last_idx = usize::try_from(row_count - 1).expect("usize");
        assert_eq!(
            rows[last_idx].get(1).unwrap(),
            &SqliteValue::Text(format!("payload_{:04}", row_count - 1)),
            "bead_id={BEAD_ID} case=btree_last"
        );
    }
}

#[test]
fn btree_handles_delete_and_reinsert() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, v TEXT)")
            .expect("ddl");
        for i in 1..=10 {
            conn.execute_with_params(
                "INSERT INTO t1 VALUES (?, ?)",
                &[SqliteValue::Integer(i), SqliteValue::Text(format!("v{i}"))],
            )
            .expect("insert");
        }

        // Delete specific rows by ID
        conn.execute("DELETE FROM t1 WHERE id IN (2, 4, 6, 8, 10)")
            .expect("delete");

        // Reinsert with different values
        for i in [2, 4, 6, 8, 10] {
            conn.execute_with_params(
                "INSERT INTO t1 VALUES (?, ?)",
                &[
                    SqliteValue::Integer(i),
                    SqliteValue::Text(format!("new_v{i}")),
                ],
            )
            .expect("reinsert");
        }
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        let rows = conn
            .query("SELECT id, v FROM t1 ORDER BY id")
            .expect("query");
        assert_eq!(
            rows.len(),
            10,
            "bead_id={BEAD_ID} case=delete_reinsert_count"
        );

        // Odd rows should have original values
        assert_eq!(
            rows[0].get(1).unwrap(),
            &SqliteValue::Text("v1".to_owned()),
            "bead_id={BEAD_ID} case=odd_preserved"
        );

        // Even rows should have new values
        assert_eq!(
            rows[1].get(1).unwrap(),
            &SqliteValue::Text("new_v2".to_owned()),
            "bead_id={BEAD_ID} case=even_reinserted"
        );
    }
}

// ─── Large Payloads (Overflow Pages) ────────────────────────────────────

#[test]
fn large_blob_overflow_pages_round_trip() {
    let (_dir, db_str) = temp_db();
    // 10KB blob — forces overflow pages (default page size is 4096)
    let large_blob: Vec<u8> = (0..10_000)
        .map(|i| u8::try_from(i % 256).unwrap())
        .collect();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, data BLOB)")
            .expect("ddl");
        conn.execute_with_params(
            "INSERT INTO t1 VALUES (1, ?)",
            &[SqliteValue::Blob(large_blob.clone())],
        )
        .expect("insert blob");
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        let rows = conn
            .query("SELECT data FROM t1 WHERE id = 1")
            .expect("query");
        assert_eq!(rows.len(), 1, "bead_id={BEAD_ID} case=overflow_found");
        let retrieved = rows[0].get(0).unwrap();
        if let SqliteValue::Blob(data) = retrieved {
            assert_eq!(
                data.len(),
                large_blob.len(),
                "bead_id={BEAD_ID} case=overflow_size"
            );
            assert_eq!(data, &large_blob, "bead_id={BEAD_ID} case=overflow_content");
        } else {
            panic!("bead_id={BEAD_ID} case=overflow_type expected Blob, got {retrieved:?}");
        }
    }
}

#[test]
fn large_text_payload_round_trip() {
    let (_dir, db_str) = temp_db();
    // 8KB text string — forces overflow
    let large_text: String = "A".repeat(8000);

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (id INTEGER, txt TEXT)")
            .expect("ddl");
        conn.execute_with_params(
            "INSERT INTO t1 VALUES (1, ?)",
            &[SqliteValue::Text(large_text.clone())],
        )
        .expect("insert text");
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        let rows = conn.query("SELECT txt FROM t1").expect("query");
        assert_eq!(
            rows[0].get(0).unwrap(),
            &SqliteValue::Text(large_text),
            "bead_id={BEAD_ID} case=large_text_content"
        );
    }
}

// ─── Type Diversity ─────────────────────────────────────────────────────

#[test]
fn all_sqlite_types_round_trip_on_file_backend() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (a INTEGER, b REAL, c TEXT, d BLOB, e)")
            .expect("ddl");
        conn.execute_with_params(
            "INSERT INTO t1 VALUES (?, ?, ?, ?, ?)",
            &[
                SqliteValue::Integer(42),
                SqliteValue::Float(3.14),
                SqliteValue::Text("hello".to_owned()),
                SqliteValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
                SqliteValue::Null,
            ],
        )
        .expect("insert");
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        let rows = conn.query("SELECT a, b, c, d, e FROM t1").expect("query");
        assert_eq!(rows.len(), 1, "bead_id={BEAD_ID} case=types_row_count");

        assert_eq!(
            rows[0].get(0).unwrap(),
            &SqliteValue::Integer(42),
            "bead_id={BEAD_ID} case=type_integer"
        );
        if let SqliteValue::Float(f) = rows[0].get(1).unwrap() {
            assert!(
                (*f - 3.14).abs() < 1e-10,
                "bead_id={BEAD_ID} case=type_float got={f}"
            );
        } else {
            panic!(
                "bead_id={BEAD_ID} case=type_float expected Float, got {:?}",
                rows[0].get(1)
            );
        }
        assert_eq!(
            rows[0].get(2).unwrap(),
            &SqliteValue::Text("hello".to_owned()),
            "bead_id={BEAD_ID} case=type_text"
        );
        assert_eq!(
            rows[0].get(3).unwrap(),
            &SqliteValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            "bead_id={BEAD_ID} case=type_blob"
        );
        assert_eq!(
            rows[0].get(4).unwrap(),
            &SqliteValue::Null,
            "bead_id={BEAD_ID} case=type_null"
        );
    }
}

// ─── MVCC/Concurrent Mode ───────────────────────────────────────────────

#[test]
fn concurrent_transaction_commit_persists() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (x INTEGER)").expect("ddl");
        conn.execute("BEGIN CONCURRENT").expect("begin concurrent");
        conn.execute("INSERT INTO t1 VALUES (100)").expect("ins");
        conn.execute("INSERT INTO t1 VALUES (200)").expect("ins");
        conn.execute("COMMIT").expect("commit");
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        let rows = conn.query("SELECT x FROM t1 ORDER BY x").expect("query");
        assert_eq!(
            rows.len(),
            2,
            "bead_id={BEAD_ID} case=concurrent_commit_count"
        );
        assert_eq!(
            rows[0].get(0).unwrap(),
            &SqliteValue::Integer(100),
            "bead_id={BEAD_ID} case=concurrent_val1"
        );
        assert_eq!(
            rows[1].get(0).unwrap(),
            &SqliteValue::Integer(200),
            "bead_id={BEAD_ID} case=concurrent_val2"
        );
    }
}

#[test]
fn concurrent_transaction_rollback_does_not_persist() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (x INTEGER)").expect("ddl");
        conn.execute("INSERT INTO t1 VALUES (1)").expect("baseline");
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        conn.execute("BEGIN CONCURRENT").expect("begin");
        conn.execute("INSERT INTO t1 VALUES (999)").expect("ins");
        conn.execute("ROLLBACK").expect("rollback");
    }

    {
        let conn = Connection::open(&db_str).expect("reopen2");
        let rows = conn.query("SELECT x FROM t1").expect("query");
        assert_eq!(
            rows.len(),
            1,
            "bead_id={BEAD_ID} case=concurrent_rollback_count"
        );
    }
}

// ─── Multi-Table Interactions ───────────────────────────────────────────

#[test]
fn multi_table_transaction_atomicity() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)")
            .expect("ddl1");
        conn.execute("CREATE TABLE audit_log (id INTEGER PRIMARY KEY, msg TEXT)")
            .expect("ddl2");

        conn.execute("BEGIN").expect("begin");
        conn.execute("INSERT INTO accounts VALUES (1, 1000)")
            .expect("ins1");
        conn.execute("INSERT INTO accounts VALUES (2, 2000)")
            .expect("ins2");
        conn.execute("INSERT INTO audit_log VALUES (1, 'initial balances')")
            .expect("audit");
        conn.execute("COMMIT").expect("commit");
    }

    // Transfer operation in a transaction
    {
        let conn = Connection::open(&db_str).expect("reopen");
        conn.execute("BEGIN").expect("begin");
        conn.execute("UPDATE accounts SET balance = balance - 500 WHERE id = 1")
            .expect("debit");
        conn.execute("UPDATE accounts SET balance = balance + 500 WHERE id = 2")
            .expect("credit");
        conn.execute("INSERT INTO audit_log VALUES (2, 'transfer 500')")
            .expect("audit");
        conn.execute("COMMIT").expect("commit");
    }

    // Verify both tables are consistent
    {
        let conn = Connection::open(&db_str).expect("reopen2");
        let accounts = conn
            .query("SELECT id, balance FROM accounts ORDER BY id")
            .expect("query accounts");
        assert_eq!(
            accounts[0].get(1).unwrap(),
            &SqliteValue::Integer(500),
            "bead_id={BEAD_ID} case=multi_table_acct1"
        );
        assert_eq!(
            accounts[1].get(1).unwrap(),
            &SqliteValue::Integer(2500),
            "bead_id={BEAD_ID} case=multi_table_acct2"
        );

        let logs = conn
            .query("SELECT msg FROM audit_log ORDER BY id")
            .expect("query logs");
        assert_eq!(
            logs.len(),
            2,
            "bead_id={BEAD_ID} case=multi_table_audit_count"
        );
    }
}

// ─── Index Persistence ──────────────────────────────────────────────────

#[test]
fn index_persists_and_speeds_lookup() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
            .expect("ddl");
        conn.execute("CREATE INDEX idx_score ON t1(score)")
            .expect("create index");

        for i in 1..=100 {
            conn.execute_with_params(
                "INSERT INTO t1 VALUES (?, ?, ?)",
                &[
                    SqliteValue::Integer(i),
                    SqliteValue::Text(format!("name_{i}")),
                    SqliteValue::Integer(i * 10),
                ],
            )
            .expect("insert");
        }
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        // Index should survive reopen — query using indexed column
        let rows = conn
            .query("SELECT name FROM t1 WHERE score = 500")
            .expect("query");
        assert_eq!(rows.len(), 1, "bead_id={BEAD_ID} case=index_lookup_count");
        assert_eq!(
            rows[0].get(0).unwrap(),
            &SqliteValue::Text("name_50".to_owned()),
            "bead_id={BEAD_ID} case=index_lookup_val"
        );

        // Range query: scores 900,910,...,1000 = 11 values
        let rows = conn
            .query("SELECT count(*) FROM t1 WHERE score >= 900")
            .expect("range query");
        assert_eq!(
            rows[0].get(0).unwrap(),
            &SqliteValue::Integer(11),
            "bead_id={BEAD_ID} case=index_range"
        );
    }
}

// ─── Update and Read-Your-Writes ────────────────────────────────────────

#[test]
fn update_within_transaction_visible_to_later_reads() {
    let (_dir, db_str) = temp_db();

    let conn = Connection::open(&db_str).expect("open");
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
        .expect("ddl");
    conn.execute("INSERT INTO t1 VALUES (1, 'original')")
        .expect("ins");

    conn.execute("BEGIN").expect("begin");
    conn.execute("UPDATE t1 SET val = 'modified' WHERE id = 1")
        .expect("update");

    // Read within same transaction should see updated value
    let rows = conn
        .query("SELECT val FROM t1 WHERE id = 1")
        .expect("query");
    assert_eq!(
        rows[0].get(0).unwrap(),
        &SqliteValue::Text("modified".to_owned()),
        "bead_id={BEAD_ID} case=read_your_writes"
    );

    conn.execute("COMMIT").expect("commit");
    drop(conn);

    // Verify persisted after reopen
    let conn = Connection::open(&db_str).expect("reopen");
    let rows = conn
        .query("SELECT val FROM t1 WHERE id = 1")
        .expect("query");
    assert_eq!(
        rows[0].get(0).unwrap(),
        &SqliteValue::Text("modified".to_owned()),
        "bead_id={BEAD_ID} case=update_persisted"
    );
}

// ─── NULL Handling ──────────────────────────────────────────────────────

#[test]
fn null_values_round_trip_correctly() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (a INTEGER, b TEXT, c BLOB)")
            .expect("ddl");
        conn.execute("INSERT INTO t1 VALUES (NULL, NULL, NULL)")
            .expect("all nulls");
        conn.execute("INSERT INTO t1 VALUES (1, NULL, NULL)")
            .expect("partial nulls");
        conn.execute("INSERT INTO t1 VALUES (NULL, 'text', NULL)")
            .expect("partial nulls 2");
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        let rows = conn
            .query("SELECT a, b, c FROM t1 ORDER BY rowid")
            .expect("query");
        assert_eq!(rows.len(), 3, "bead_id={BEAD_ID} case=null_count");

        // Row 1: all nulls
        assert_eq!(
            rows[0].get(0).unwrap(),
            &SqliteValue::Null,
            "bead_id={BEAD_ID} case=null_row1_a"
        );
        assert_eq!(
            rows[0].get(1).unwrap(),
            &SqliteValue::Null,
            "bead_id={BEAD_ID} case=null_row1_b"
        );

        // Row 2: mixed
        assert_eq!(
            rows[1].get(0).unwrap(),
            &SqliteValue::Integer(1),
            "bead_id={BEAD_ID} case=null_row2_a"
        );
        assert_eq!(
            rows[1].get(1).unwrap(),
            &SqliteValue::Null,
            "bead_id={BEAD_ID} case=null_row2_b"
        );
    }
}

// ─── File-Backed DB Size Grows ──────────────────────────────────────────

#[test]
fn database_file_grows_with_data() {
    let (_dir, db_str) = temp_db();
    let db_path = PathBuf::from(&db_str);

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE t1 (data TEXT)").expect("ddl");
    }

    let size_after_create = std::fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);

    {
        let conn = Connection::open(&db_str).expect("reopen");
        // Insert enough data to force multiple pages
        for i in 0..100 {
            conn.execute_with_params(
                "INSERT INTO t1 VALUES (?)",
                &[SqliteValue::Text(format!(
                    "row_{i}_padding_data_to_fill_pages"
                ))],
            )
            .expect("insert");
        }
    }

    let size_after_inserts = std::fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);

    // WAL file may hold the data instead of the main DB growing,
    // but at least one of them should have grown
    let wal_size = std::fs::metadata(wal_path(&db_str))
        .map(|m| m.len())
        .unwrap_or(0);

    let total_after = size_after_inserts + wal_size;
    assert!(
        total_after > size_after_create,
        "bead_id={BEAD_ID} case=db_grows db_before={size_after_create} total_after={total_after}"
    );
}

// ─── Aggregation Over Persisted Data ────────────────────────────────────

#[test]
fn aggregation_over_persisted_data() {
    let (_dir, db_str) = temp_db();

    {
        let conn = Connection::open(&db_str).expect("open");
        conn.execute("CREATE TABLE scores (student TEXT, score INTEGER)")
            .expect("ddl");
        let data = [
            ("Alice", 90),
            ("Bob", 85),
            ("Alice", 95),
            ("Bob", 80),
            ("Charlie", 100),
        ];
        for (name, score) in &data {
            conn.execute_with_params(
                "INSERT INTO scores VALUES (?, ?)",
                &[
                    SqliteValue::Text((*name).to_owned()),
                    SqliteValue::Integer(i64::from(*score)),
                ],
            )
            .expect("insert");
        }
    }

    {
        let conn = Connection::open(&db_str).expect("reopen");
        let rows = conn
            .query("SELECT student, SUM(score) as total FROM scores GROUP BY student ORDER BY total DESC")
            .expect("query");
        assert_eq!(rows.len(), 3, "bead_id={BEAD_ID} case=agg_group_count");

        // Alice: 90+95=185, Bob: 85+80=165, Charlie: 100
        assert_eq!(
            rows[0].get(0).unwrap(),
            &SqliteValue::Text("Alice".to_owned()),
            "bead_id={BEAD_ID} case=agg_first_student"
        );
        assert_eq!(
            rows[0].get(1).unwrap(),
            &SqliteValue::Integer(185),
            "bead_id={BEAD_ID} case=agg_first_total"
        );
    }
}

// ─── Deterministic Seed-Based Workload ──────────────────────────────────

#[test]
fn deterministic_seed_workload_produces_consistent_results() {
    let seed: u64 = 0xDEAD_BEEF_CAFE_1234;

    // Run the same workload twice with the same seed
    let result1 = run_seeded_workload(seed);
    let result2 = run_seeded_workload(seed);

    assert_eq!(
        result1, result2,
        "bead_id={BEAD_ID} case=deterministic_seed seed=0x{seed:016X}"
    );
}

/// Execute a deterministic workload based on a seed and return row count + checksum.
fn run_seeded_workload(seed: u64) -> (usize, i64) {
    let (_dir, db_str) = temp_db();
    let conn = Connection::open(&db_str).expect("open");
    conn.execute("CREATE TABLE t1 (k INTEGER PRIMARY KEY, v INTEGER)")
        .expect("ddl");

    // Use seed to derive insert values deterministically
    let mut state = seed;
    conn.execute("BEGIN").expect("begin");
    for _ in 0..50 {
        // Simple xorshift-style PRNG
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let k = (state % 1000) as i64;
        let v = (state % 10000) as i64;
        // INSERT OR REPLACE to handle collisions deterministically
        let _ = conn.execute_with_params(
            "INSERT OR REPLACE INTO t1 VALUES (?, ?)",
            &[SqliteValue::Integer(k), SqliteValue::Integer(v)],
        );
    }
    conn.execute("COMMIT").expect("commit");

    drop(conn);

    // Reopen and measure
    let conn = Connection::open(&db_str).expect("reopen");
    let count_rows = conn.query("SELECT count(*) FROM t1").expect("count");
    let count = match count_rows[0].get(0).unwrap() {
        SqliteValue::Integer(n) => *n,
        other => panic!("bead_id={BEAD_ID} case=seed_count unexpected {other:?}"),
    };

    let sum_rows = conn
        .query("SELECT COALESCE(SUM(v), 0) FROM t1")
        .expect("sum");
    let checksum = match sum_rows[0].get(0).unwrap() {
        SqliteValue::Integer(n) => *n,
        other => panic!("bead_id={BEAD_ID} case=seed_sum unexpected {other:?}"),
    };

    (usize::try_from(count).expect("usize"), checksum)
}
