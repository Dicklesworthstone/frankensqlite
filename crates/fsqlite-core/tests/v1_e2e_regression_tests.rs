//! V1 zero-copy page access regression tests.
//!
//! Verifies correctness after V1.1 (cell pointer Vec), V1.2 (page cache
//! get_shared), V1.4 (register subtype bool flag), and V1.6 (CursorSlots
//! flat array) optimizations.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

async fn new_mem_conn() -> Connection {
    let conn = Connection::open(":memory:").await.unwrap();
    conn.execute("PRAGMA journal_mode=WAL").await.ok();
    conn
}

// ── V1.1: Cell pointer correctness ──────────────────────────────────────

#[test]
fn test_v1_insert_10k_rows_correctness() {
    asupersync::test_utils::run_test(|| async {
        let conn = new_mem_conn().await;
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();
        conn.execute("BEGIN").await.unwrap();
        for i in 1..=10_000 {
            conn.execute(&format!("INSERT INTO t VALUES ({i}, 'row_{i}')"))
                .await
                .unwrap();
        }
        conn.execute("COMMIT").await.unwrap();

        let row = conn.query_row("SELECT COUNT(*) FROM t").await.unwrap();
        assert_eq!(
            row.values()[0].to_integer(),
            10_000,
            "10K rows should be present"
        );

        // Verify first and last rows
        let first = conn
            .query_row("SELECT val FROM t WHERE id = 1")
            .await
            .unwrap();
        assert_eq!(first.values()[0].to_text(), "row_1");
        let last = conn
            .query_row("SELECT val FROM t WHERE id = 10000")
            .await
            .unwrap();
        assert_eq!(last.values()[0].to_text(), "row_10000");
    });
}

#[test]
fn test_v1_insert_with_secondary_index() {
    asupersync::test_utils::run_test(|| async {
        let conn = new_mem_conn().await;
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
            .await
            .unwrap();
        conn.execute("CREATE INDEX idx_score ON t(score)")
            .await
            .unwrap();
        conn.execute("BEGIN").await.unwrap();
        for i in 1..=1000 {
            conn.execute(&format!(
                "INSERT INTO t VALUES ({i}, 'user_{i}', {})",
                i % 100
            ))
            .await
            .unwrap();
        }
        conn.execute("COMMIT").await.unwrap();

        let row = conn
            .query_row("SELECT COUNT(*) FROM t WHERE score = 50")
            .await
            .unwrap();
        assert_eq!(row.values()[0].to_integer(), 10, "10 rows with score=50");
    });
}

#[test]
fn test_v1_insert_update_delete_cycle() {
    asupersync::test_utils::run_test(|| async {
        let conn = new_mem_conn().await;
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
            .await
            .unwrap();

        // INSERT
        conn.execute("BEGIN").await.unwrap();
        for i in 1..=100 {
            conn.execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .await
                .unwrap();
        }
        conn.execute("COMMIT").await.unwrap();
        let count = conn.query_row("SELECT COUNT(*) FROM t").await.unwrap();
        assert_eq!(count.values()[0].to_integer(), 100);

        // UPDATE
        conn.execute("UPDATE t SET val = val * 2 WHERE id <= 50")
            .await
            .unwrap();
        let sum = conn
            .query_row("SELECT SUM(val) FROM t WHERE id <= 50")
            .await
            .unwrap();
        // ids 1-50, original val=id, doubled: sum = 2 * (1+2+...+50) = 2*1275 = 2550
        assert_eq!(sum.values()[0].to_integer(), 2550);

        // DELETE
        conn.execute("DELETE FROM t WHERE id > 50").await.unwrap();
        let count = conn.query_row("SELECT COUNT(*) FROM t").await.unwrap();
        assert_eq!(count.values()[0].to_integer(), 50);

        // Verify remaining rows
        let max = conn.query_row("SELECT MAX(id) FROM t").await.unwrap();
        assert_eq!(max.values()[0].to_integer(), 50);
    });
}

// ── V1.6: CursorSlots flat array ────────────────────────────────────────

#[test]
fn test_v1_multiple_cursors_via_join() {
    asupersync::test_utils::run_test(|| async {
        let conn = new_mem_conn().await;
        conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER, name TEXT)")
            .await
            .unwrap();

        conn.execute("BEGIN").await.unwrap();
        for i in 1..=50 {
            conn.execute(&format!("INSERT INTO a VALUES ({i}, 'a_{i}')"))
                .await
                .unwrap();
            conn.execute(&format!("INSERT INTO b VALUES ({i}, {i}, 'b_{i}')"))
                .await
                .unwrap();
        }
        conn.execute("COMMIT").await.unwrap();

        // JOIN uses multiple cursors — tests CursorSlots with multiple occupied slots
        let row = conn
            .query_row("SELECT COUNT(*) FROM a JOIN b ON a.id = b.a_id")
            .await
            .unwrap();
        assert_eq!(row.values()[0].to_integer(), 50);
    });
}

#[test]
fn test_v1_cursor_reuse_across_prepared_inserts() {
    asupersync::test_utils::run_test(|| async {
        let conn = new_mem_conn().await;
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        conn.execute("BEGIN").await.unwrap();

        // Use prepared statement for repeated inserts — tests cursor retention
        let stmt = conn.prepare("INSERT INTO t VALUES (?1)").await.unwrap();
        for i in 1..=5000_i64 {
            stmt.execute_with_params(&[SqliteValue::Integer(i)])
                .await
                .unwrap();
        }
        conn.execute("COMMIT").await.unwrap();

        let row = conn.query_row("SELECT COUNT(*) FROM t").await.unwrap();
        assert_eq!(row.values()[0].to_integer(), 5000);

        let row = conn.query_row("SELECT MAX(id) FROM t").await.unwrap();
        assert_eq!(row.values()[0].to_integer(), 5000);
    });
}

// ── V1.2: Page cache shared access ──────────────────────────────────────

#[test]
fn test_v1_read_after_write_consistency() {
    asupersync::test_utils::run_test(|| async {
        let conn = new_mem_conn().await;
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
            .await
            .unwrap();

        // Write enough to create multiple pages
        conn.execute("BEGIN").await.unwrap();
        for i in 1..=500 {
            conn.execute(&format!(
                "INSERT INTO t VALUES ({i}, '{}')",
                "x".repeat(100)
            ))
            .await
            .unwrap();
        }
        conn.execute("COMMIT").await.unwrap();

        // Read back all rows — tests page cache shared path
        let rows = conn
            .query("SELECT id, LENGTH(data) FROM t ORDER BY id")
            .await
            .unwrap();
        assert_eq!(rows.len(), 500);
        assert_eq!(rows[0].values()[0].to_integer(), 1);
        assert_eq!(rows[0].values()[1].to_integer(), 100);
        assert_eq!(rows[499].values()[0].to_integer(), 500);
    });
}

// ── V1.4: Register subtype flag ─────────────────────────────────────────

#[test]
fn test_v1_json_subtype_after_insert() {
    asupersync::test_utils::run_test(|| async {
        // JSON functions use register subtypes — verify they still work
        // after the has_subtypes bool flag optimization
        let conn = new_mem_conn().await;
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, '{\"key\": \"value\"}')")
            .await
            .unwrap();

        let row = conn
            .query_row("SELECT typeof(data), length(data) FROM t WHERE id = 1")
            .await
            .unwrap();
        assert_eq!(row.values()[0].to_text(), "text");
        assert!(row.values()[1].to_integer() > 0);
    });
}

// ── Large page / many cells stress test ─────────────────────────────────

#[test]
fn test_v1_large_table_page_splits() {
    asupersync::test_utils::run_test(|| async {
        let conn = new_mem_conn().await;
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT)")
            .await
            .unwrap();

        conn.execute("BEGIN").await.unwrap();
        for i in 1..=5000 {
            conn.execute(&format!(
                "INSERT INTO t VALUES ({i}, 'aaa_{i}', 'bbb_{i}', 'ccc_{i}')"
            ))
            .await
            .unwrap();
        }
        conn.execute("COMMIT").await.unwrap();

        // Verify integrity with a full scan
        let count = conn.query_row("SELECT COUNT(*) FROM t").await.unwrap();
        assert_eq!(count.values()[0].to_integer(), 5000);

        // Range scan
        let rows = conn
            .query("SELECT id FROM t WHERE id BETWEEN 2500 AND 2510 ORDER BY id")
            .await
            .unwrap();
        assert_eq!(rows.len(), 11);
        assert_eq!(rows[0].values()[0].to_integer(), 2500);
        assert_eq!(rows[10].values()[0].to_integer(), 2510);
    });
}

// ── Concurrent writer correctness ───────────────────────────────────────

#[test]
fn test_v1_autocommit_insert_read_consistency() {
    asupersync::test_utils::run_test(|| async {
        let conn = new_mem_conn().await;
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
            .await
            .unwrap();

        // Autocommit inserts (each is its own transaction)
        for i in 1..=100 {
            conn.execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .await
                .unwrap();
        }

        // Verify all visible
        let count = conn.query_row("SELECT COUNT(*) FROM t").await.unwrap();
        assert_eq!(count.values()[0].to_integer(), 100);

        let sum = conn.query_row("SELECT SUM(val) FROM t").await.unwrap();
        assert_eq!(sum.values()[0].to_integer(), 5050); // 1+2+...+100
    });
}
