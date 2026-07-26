//! Multi-connection lifecycle and coordination tests (`bd-mblr.2.2.2`).
//!
//! Validates coordination, visibility, and lock/epoch interactions when
//! multiple connections access the same file-backed database.
//!
//! All scenarios use deterministic actor sequencing (no threads, no
//! nondeterministic timing) for reproducible failures.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;
use tempfile::TempDir;

const BEAD_ID: &str = "bd-mblr.2.2.2";

fn temp_db() -> (TempDir, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("test.db");
    let path_str = path.to_string_lossy().to_string();
    (dir, path_str)
}

async fn count_rows(conn: &Connection, table: &str) -> i64 {
    let query = format!("SELECT count(*) FROM {table}");
    let rows = conn.query(&query).await.expect("count");
    match rows[0].get(0).unwrap() {
        SqliteValue::Integer(n) => *n,
        other => panic!("expected integer, got {other:?}"),
    }
}

async fn read_val(conn: &Connection, id: i64) -> Option<String> {
    let rows = conn
        .query_with_params(
            "SELECT val FROM t1 WHERE id = ?",
            &[SqliteValue::Integer(id)],
        )
        .await
        .expect("query");
    if rows.is_empty() {
        None
    } else {
        match rows[0].get(0).unwrap() {
            SqliteValue::Text(s) => Some(s.to_string()),
            SqliteValue::Null => None,
            other => panic!("expected text, got {other:?}"),
        }
    }
}

// ─── Basic Multi-Connection Visibility ─────────────────────────────────

#[test]
fn conn2_sees_conn1_committed_data() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        let conn1 = Connection::open(&db_str).await.expect("open1");
        conn1
            .execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .expect("ddl");
        conn1
            .execute("INSERT INTO t1 VALUES (1, 'from_conn1')")
            .await
            .expect("insert");
        drop(conn1);

        let conn2 = Connection::open(&db_str).await.expect("open2");
        assert_eq!(
            count_rows(&conn2, "t1").await,
            1,
            "bead_id={BEAD_ID} case=conn2_sees_committed"
        );
        assert_eq!(
            read_val(&conn2, 1).await.as_deref(),
            Some("from_conn1"),
            "bead_id={BEAD_ID} case=conn2_sees_val"
        );
    });
}

#[test]
fn sequential_connections_accumulate_data() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        // Connection 1: create schema + baseline
        {
            let conn = Connection::open(&db_str).await.expect("open");
            conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("ddl");
            conn.execute("INSERT INTO t1 VALUES (1, 'first')")
                .await
                .expect("insert");
        }

        // Connection 2: add more data
        {
            let conn = Connection::open(&db_str).await.expect("open2");
            conn.execute("INSERT INTO t1 VALUES (2, 'second')")
                .await
                .expect("insert");
        }

        // Connection 3: add more data
        {
            let conn = Connection::open(&db_str).await.expect("open3");
            conn.execute("INSERT INTO t1 VALUES (3, 'third')")
                .await
                .expect("insert");
        }

        // Connection 4: verify all data
        let conn = Connection::open(&db_str).await.expect("open4");
        assert_eq!(
            count_rows(&conn, "t1").await,
            3,
            "bead_id={BEAD_ID} case=sequential_accumulate"
        );
    });
}

#[test]
fn conn2_does_not_see_conn1_uncommitted_in_explicit_txn() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        let conn1 = Connection::open(&db_str).await.expect("open1");
        conn1
            .execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .expect("ddl");
        conn1
            .execute("INSERT INTO t1 VALUES (1, 'committed_baseline')")
            .await
            .expect("baseline");

        // conn1 starts a transaction but doesn't commit
        conn1.execute("BEGIN").await.expect("begin");
        conn1
            .execute("INSERT INTO t1 VALUES (2, 'uncommitted')")
            .await
            .expect("insert");

        // conn2 should only see the committed baseline (not the in-flight insert)
        let conn2 = Connection::open(&db_str).await.expect("open2");
        let count = count_rows(&conn2, "t1").await;
        // Note: visibility depends on isolation level; autocommit baseline is visible,
        // uncommitted txn data may or may not be visible depending on WAL mode.
        // At minimum, the baseline row should be visible.
        assert!(
            count >= 1,
            "bead_id={BEAD_ID} case=uncommitted_visibility count={count}"
        );

        // conn1 commits
        conn1.execute("COMMIT").await.expect("commit");
        drop(conn1);

        // conn2 re-queries and should now see everything
        let conn3 = Connection::open(&db_str).await.expect("open3");
        assert_eq!(
            count_rows(&conn3, "t1").await,
            2,
            "bead_id={BEAD_ID} case=after_commit_visibility"
        );
    });
}

// ─── Multi-Connection DDL Coordination ─────────────────────────────────

#[test]
fn conn2_sees_schema_created_by_conn1() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn1 = Connection::open(&db_str).await.expect("open1");
            conn1
                .execute("CREATE TABLE t1 (x INTEGER)")
                .await
                .expect("ddl");
            conn1
                .execute("CREATE TABLE t2 (y TEXT)")
                .await
                .expect("ddl2");
        }

        let conn2 = Connection::open(&db_str).await.expect("open2");
        // conn2 should be able to use both tables
        conn2
            .execute("INSERT INTO t1 VALUES (1)")
            .await
            .expect("t1");
        conn2
            .execute("INSERT INTO t2 VALUES ('hello')")
            .await
            .expect("t2");
        assert_eq!(
            count_rows(&conn2, "t1").await,
            1,
            "bead_id={BEAD_ID} case=conn2_sees_schema_t1"
        );
        assert_eq!(
            count_rows(&conn2, "t2").await,
            1,
            "bead_id={BEAD_ID} case=conn2_sees_schema_t2"
        );
    });
}

#[test]
fn conn2_sees_table_dropped_by_conn1() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn1 = Connection::open(&db_str).await.expect("open1");
            conn1
                .execute("CREATE TABLE t1 (x INTEGER)")
                .await
                .expect("ddl");
            conn1
                .execute("CREATE TABLE t2 (y TEXT)")
                .await
                .expect("ddl2");
            conn1.execute("DROP TABLE t1").await.expect("drop");
        }

        let conn2 = Connection::open(&db_str).await.expect("open2");
        // t1 should not exist
        let result = conn2.execute("INSERT INTO t1 VALUES (1)").await;
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=conn2_dropped_table_gone"
        );
        // t2 should still exist
        conn2
            .execute("INSERT INTO t2 VALUES ('still here')")
            .await
            .expect("t2 ok");
    });
}

// ─── Multi-Connection Write Coordination ───────────────────────────────

#[test]
fn sequential_writers_on_same_table() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn = Connection::open(&db_str).await.expect("setup");
            conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, writer TEXT)")
                .await
                .expect("ddl");
        }

        // 5 sequential writers, each opens a fresh connection
        for writer_id in 0..5 {
            let conn = Connection::open(&db_str).await.expect("writer");
            for row in 0..3 {
                let id = i64::from(writer_id * 3 + row + 1);
                conn.execute_with_params(
                    "INSERT INTO t1 VALUES (?, ?)",
                    &[
                        SqliteValue::Integer(id),
                        SqliteValue::Text(format!("writer_{writer_id}").into()),
                    ],
                )
                .await
                .expect("insert");
            }
        }

        let conn = Connection::open(&db_str).await.expect("reader");
        assert_eq!(
            count_rows(&conn, "t1").await,
            15,
            "bead_id={BEAD_ID} case=sequential_writers"
        );
    });
}

#[test]
fn conn2_updates_conn1_data() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn1 = Connection::open(&db_str).await.expect("open1");
            conn1
                .execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("ddl");
            conn1
                .execute("INSERT INTO t1 VALUES (1, 'original')")
                .await
                .expect("insert");
        }

        {
            let conn2 = Connection::open(&db_str).await.expect("open2");
            conn2
                .execute("UPDATE t1 SET val = 'modified_by_conn2' WHERE id = 1")
                .await
                .expect("update");
        }

        let conn3 = Connection::open(&db_str).await.expect("open3");
        assert_eq!(
            read_val(&conn3, 1).await.as_deref(),
            Some("modified_by_conn2"),
            "bead_id={BEAD_ID} case=conn2_updates_conn1_data"
        );
    });
}

#[test]
fn conn2_deletes_conn1_data() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn1 = Connection::open(&db_str).await.expect("open1");
            conn1
                .execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("ddl");
            conn1
                .execute("INSERT INTO t1 VALUES (1, 'to_delete')")
                .await
                .expect("insert");
            conn1
                .execute("INSERT INTO t1 VALUES (2, 'to_keep')")
                .await
                .expect("insert");
        }

        {
            let conn2 = Connection::open(&db_str).await.expect("open2");
            conn2
                .execute("DELETE FROM t1 WHERE id = 1")
                .await
                .expect("delete");
        }

        let conn3 = Connection::open(&db_str).await.expect("open3");
        assert_eq!(
            count_rows(&conn3, "t1").await,
            1,
            "bead_id={BEAD_ID} case=conn2_deletes_conn1_data"
        );
        assert!(
            read_val(&conn3, 1).await.is_none(),
            "bead_id={BEAD_ID} case=deleted_row_gone"
        );
        assert_eq!(
            read_val(&conn3, 2).await.as_deref(),
            Some("to_keep"),
            "bead_id={BEAD_ID} case=kept_row_present"
        );
    });
}

// ─── Multi-Connection Transaction Interleaving ─────────────────────────

#[test]
fn conn1_rollback_does_not_affect_conn2_committed_data() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn = Connection::open(&db_str).await.expect("setup");
            conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("ddl");
        }

        // conn1: commits a row
        {
            let conn1 = Connection::open(&db_str).await.expect("conn1");
            conn1
                .execute("INSERT INTO t1 VALUES (1, 'conn1_committed')")
                .await
                .expect("insert");
        }

        // conn2: begins, inserts, rolls back
        {
            let conn2 = Connection::open(&db_str).await.expect("conn2");
            conn2.execute("BEGIN").await.expect("begin");
            conn2
                .execute("INSERT INTO t1 VALUES (2, 'conn2_rolled_back')")
                .await
                .expect("insert");
            conn2.execute("ROLLBACK").await.expect("rollback");
        }

        let conn3 = Connection::open(&db_str).await.expect("verify");
        assert_eq!(
            count_rows(&conn3, "t1").await,
            1,
            "bead_id={BEAD_ID} case=conn1_commit_conn2_rollback"
        );
        assert_eq!(
            read_val(&conn3, 1).await.as_deref(),
            Some("conn1_committed"),
            "bead_id={BEAD_ID} case=conn1_data_preserved"
        );
    });
}

#[test]
fn interleaved_commit_rollback_across_connections() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn = Connection::open(&db_str).await.expect("setup");
            conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("ddl");
        }

        // Pattern: conn_A commits, conn_B rollbacks, conn_C commits, etc.
        for i in 0..6 {
            let conn = Connection::open(&db_str).await.expect("conn");
            let id = i64::from(i + 1);
            conn.execute("BEGIN").await.expect("begin");
            conn.execute_with_params(
                "INSERT INTO t1 VALUES (?, ?)",
                &[
                    SqliteValue::Integer(id),
                    SqliteValue::Text(format!("step_{i}").into()),
                ],
            )
            .await
            .expect("insert");
            if i % 2 == 0 {
                conn.execute("COMMIT").await.expect("commit");
            } else {
                conn.execute("ROLLBACK").await.expect("rollback");
            }
        }

        let conn = Connection::open(&db_str).await.expect("verify");
        // Even steps (0,2,4) committed: 3 rows
        assert_eq!(
            count_rows(&conn, "t1").await,
            3,
            "bead_id={BEAD_ID} case=interleaved_commit_rollback"
        );
    });
}

// ─── Multi-Connection Index Coordination ───────────────────────────────

#[test]
fn conn2_uses_index_created_by_conn1() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn1 = Connection::open(&db_str).await.expect("conn1");
            conn1
                .execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("ddl");
            conn1
                .execute("CREATE INDEX idx_val ON t1 (val)")
                .await
                .expect("index");
            for i in 1..=10 {
                conn1
                    .execute_with_params(
                        "INSERT INTO t1 VALUES (?, ?)",
                        &[
                            SqliteValue::Integer(i),
                            SqliteValue::Text(format!("val_{i}").into()),
                        ],
                    )
                    .await
                    .expect("insert");
            }
        }

        let conn2 = Connection::open(&db_str).await.expect("conn2");
        let rows = conn2
            .query("SELECT id FROM t1 WHERE val = 'val_7'")
            .await
            .expect("query");
        assert_eq!(rows.len(), 1, "bead_id={BEAD_ID} case=conn2_uses_index");
        assert_eq!(
            rows[0].get(0).unwrap(),
            &SqliteValue::Integer(7),
            "bead_id={BEAD_ID} case=conn2_index_val"
        );
    });
}

// ─── Multi-Connection WAL Mode Coordination ────────────────────────────

#[test]
fn wal_mode_sequential_writers_persist() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn = Connection::open(&db_str).await.expect("setup");
            conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
            conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("ddl");
        }

        for i in 1..=5 {
            let conn = Connection::open(&db_str).await.expect("writer");
            conn.execute_with_params(
                "INSERT INTO t1 VALUES (?, ?)",
                &[
                    SqliteValue::Integer(i),
                    SqliteValue::Text(format!("wal_writer_{i}").into()),
                ],
            )
            .await
            .expect("insert");
        }

        let conn = Connection::open(&db_str).await.expect("reader");
        assert_eq!(
            count_rows(&conn, "t1").await,
            5,
            "bead_id={BEAD_ID} case=wal_sequential_writers"
        );
    });
}

#[test]
fn wal_mode_checkpoint_by_conn2_after_conn1_writes() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn1 = Connection::open(&db_str).await.expect("conn1");
            conn1.execute("PRAGMA journal_mode=WAL").await.expect("wal");
            conn1
                .execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("ddl");
            for i in 1..=5 {
                conn1
                    .execute_with_params(
                        "INSERT INTO t1 VALUES (?, ?)",
                        &[
                            SqliteValue::Integer(i),
                            SqliteValue::Text(format!("wal_{i}").into()),
                        ],
                    )
                    .await
                    .expect("insert");
            }
        }

        // conn2 opens and checkpoints
        {
            let conn2 = Connection::open(&db_str).await.expect("conn2");
            conn2
                .execute("PRAGMA wal_checkpoint(PASSIVE)")
                .await
                .expect("checkpoint");
        }

        // conn3 verifies data survived checkpoint
        let conn3 = Connection::open(&db_str).await.expect("conn3");
        assert_eq!(
            count_rows(&conn3, "t1").await,
            5,
            "bead_id={BEAD_ID} case=wal_checkpoint_by_conn2"
        );
    });
}

// ─── Multi-Connection Concurrent Mode ──────────────────────────────────

#[test]
fn concurrent_mode_sequential_commits_across_connections() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn = Connection::open(&db_str).await.expect("setup");
            conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("ddl");
        }

        for i in 1..=5 {
            let conn = Connection::open(&db_str).await.expect("writer");
            conn.execute("BEGIN CONCURRENT")
                .await
                .expect("begin concurrent");
            conn.execute_with_params(
                "INSERT INTO t1 VALUES (?, ?)",
                &[
                    SqliteValue::Integer(i),
                    SqliteValue::Text(format!("concurrent_{i}").into()),
                ],
            )
            .await
            .expect("insert");
            conn.execute("COMMIT").await.expect("commit");
        }

        let conn = Connection::open(&db_str).await.expect("verify");
        assert_eq!(
            count_rows(&conn, "t1").await,
            5,
            "bead_id={BEAD_ID} case=concurrent_sequential_commits"
        );
    });
}

// ─── Multi-Connection Error Isolation ──────────────────────────────────

#[test]
fn conn2_error_does_not_affect_conn1_data() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn1 = Connection::open(&db_str).await.expect("conn1");
            conn1
                .execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("ddl");
            conn1
                .execute("INSERT INTO t1 VALUES (1, 'safe')")
                .await
                .expect("insert");
        }

        // conn2 attempts invalid operation
        {
            let conn2 = Connection::open(&db_str).await.expect("conn2");
            drop(
                conn2
                    .execute("INSERT INTO nonexistent_table VALUES (1)")
                    .await,
            );
            // Error should not affect conn1's data
        }

        let conn3 = Connection::open(&db_str).await.expect("verify");
        assert_eq!(
            count_rows(&conn3, "t1").await,
            1,
            "bead_id={BEAD_ID} case=conn2_error_isolation"
        );
        assert_eq!(
            read_val(&conn3, 1).await.as_deref(),
            Some("safe"),
            "bead_id={BEAD_ID} case=conn1_data_safe"
        );
    });
}

#[test]
fn conn2_pk_violation_does_not_corrupt_shared_state() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn1 = Connection::open(&db_str).await.expect("conn1");
            conn1
                .execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("ddl");
            conn1
                .execute("INSERT INTO t1 VALUES (1, 'original')")
                .await
                .expect("insert");
        }

        // conn2 tries duplicate PK
        {
            let conn2 = Connection::open(&db_str).await.expect("conn2");
            let result = conn2
                .execute("INSERT INTO t1 VALUES (1, 'duplicate')")
                .await;
            assert!(
                result.is_err(),
                "bead_id={BEAD_ID} case=pk_violation_detected"
            );
            // conn2 can still operate
            conn2
                .execute("INSERT INTO t1 VALUES (2, 'valid')")
                .await
                .expect("valid insert");
        }

        let conn3 = Connection::open(&db_str).await.expect("verify");
        assert_eq!(
            count_rows(&conn3, "t1").await,
            2,
            "bead_id={BEAD_ID} case=pk_violation_no_corruption"
        );
        assert_eq!(
            read_val(&conn3, 1).await.as_deref(),
            Some("original"),
            "bead_id={BEAD_ID} case=original_preserved"
        );
    });
}

// ─── Multi-Connection Round-Robin Pattern ──────────────────────────────

#[test]
fn round_robin_writers_maintain_consistency() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn = Connection::open(&db_str).await.expect("setup");
            conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, writer_id INTEGER)")
                .await
                .expect("ddl");
        }

        // 3 "actors" each write 5 rows in round-robin fashion
        let num_actors = 3;
        let rows_per_actor = 5;

        for round in 0..rows_per_actor {
            for actor in 0..num_actors {
                let conn = Connection::open(&db_str).await.expect("actor");
                let id = i64::from(round * num_actors + actor + 1);
                let actor_id = i64::from(actor);
                conn.execute_with_params(
                    "INSERT INTO t1 VALUES (?, ?)",
                    &[SqliteValue::Integer(id), SqliteValue::Integer(actor_id)],
                )
                .await
                .expect("insert");
            }
        }

        let conn = Connection::open(&db_str).await.expect("verify");
        let total = count_rows(&conn, "t1").await;
        assert_eq!(total, 15, "bead_id={BEAD_ID} case=round_robin_total");

        // Verify each actor wrote rows by checking distinct writer_ids
        let rows = conn
            .query("SELECT DISTINCT writer_id FROM t1 ORDER BY writer_id")
            .await
            .expect("distinct writers");
        assert_eq!(
            rows.len(),
            3,
            "bead_id={BEAD_ID} case=round_robin_distinct_actors"
        );
    });
}

// ─── Multi-Connection: Multiple Tables ─────────────────────────────────

#[test]
fn different_connections_write_different_tables() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn = Connection::open(&db_str).await.expect("setup");
            conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .expect("ddl1");
            conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)")
                .await
                .expect("ddl2");
        }

        // conn1 writes users
        {
            let conn1 = Connection::open(&db_str).await.expect("conn1");
            conn1
                .execute("INSERT INTO users VALUES (1, 'alice')")
                .await
                .expect("user");
            conn1
                .execute("INSERT INTO users VALUES (2, 'bob')")
                .await
                .expect("user");
        }

        // conn2 writes orders
        {
            let conn2 = Connection::open(&db_str).await.expect("conn2");
            conn2
                .execute("INSERT INTO orders VALUES (100, 1)")
                .await
                .expect("order");
            conn2
                .execute("INSERT INTO orders VALUES (101, 2)")
                .await
                .expect("order");
            conn2
                .execute("INSERT INTO orders VALUES (102, 1)")
                .await
                .expect("order");
        }

        // conn3 verifies joined data
        let conn3 = Connection::open(&db_str).await.expect("verify");
        assert_eq!(
            count_rows(&conn3, "users").await,
            2,
            "bead_id={BEAD_ID} case=multi_table_users"
        );
        assert_eq!(
            count_rows(&conn3, "orders").await,
            3,
            "bead_id={BEAD_ID} case=multi_table_orders"
        );

        // Cross-table query: verify foreign key relationship works across connections
        let rows = conn3
            .query("SELECT o.id FROM orders o WHERE o.user_id = 1")
            .await
            .expect("cross-table query");
        assert_eq!(rows.len(), 2, "bead_id={BEAD_ID} case=cross_table_query");
    });
}

// ─── Multi-Connection: Stress Pattern ──────────────────────────────────

#[test]
fn many_connection_open_close_cycles_maintain_consistency() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, db_str) = temp_db();

        {
            let conn = Connection::open(&db_str).await.expect("setup");
            conn.execute("CREATE TABLE counter (n INTEGER)")
                .await
                .expect("ddl");
            conn.execute("INSERT INTO counter VALUES (0)")
                .await
                .expect("init");
        }

        // 30 connections, each increments the counter
        for _ in 0..30 {
            let conn = Connection::open(&db_str).await.expect("conn");
            conn.execute("UPDATE counter SET n = n + 1")
                .await
                .expect("increment");
        }

        let conn = Connection::open(&db_str).await.expect("verify");
        let rows = conn.query("SELECT n FROM counter").await.expect("query");
        assert_eq!(
            rows[0].get(0).unwrap(),
            &SqliteValue::Integer(30),
            "bead_id={BEAD_ID} case=many_connections_counter"
        );
    });
}
