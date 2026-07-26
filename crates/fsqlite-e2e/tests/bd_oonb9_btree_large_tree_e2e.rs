//! bd-oonb9: Track L e2e tests — B-tree large-tree correctness under
//! insertions, deletions, and rebalancing.
//!
//! Verifies data integrity after operations that stress the B-tree:
//! large sequential inserts, random deletes, interleaved operations,
//! and cross-connection verification of tree structure.
//!
//! - L1: 10K sequential inserts, oracle parity
//! - L2: Insert then delete 50%, verify remainder
//! - L3: Random-order inserts (simulates tree splits)
//! - L4: Large tree with secondary index, verify index consistency
//! - L5: Insert→Delete→Reinsert cycles (tree rebalancing stress)
//! - L6: Multi-table large tree interaction
//! - L7: Concurrent insert/delete on large tree
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

// ─── L1: 10K sequential inserts, oracle parity ───────────────────

#[test]
fn l1_large_sequential_insert_oracle() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let f_path = dir.path().join("l1_f.db");
        let c_path = dir.path().join("l1_c.db");

        let f = Connection::open(f_path.to_str().expect("path"))
            .await
            .expect("f open");
        let c = rusqlite::Connection::open(&c_path).expect("c open");

        let ddl = "CREATE TABLE big (id INTEGER PRIMARY KEY, val INTEGER, payload TEXT)";
        f.execute(ddl).await.expect("f create");
        c.execute_batch(ddl).expect("c create");

        f.execute("BEGIN").await.expect("f begin");
        for i in 1..=10_000 {
            let sql = format!(
                "INSERT INTO big VALUES ({i}, {}, 'payload_{i}')",
                i * 13 % 9973
            );
            f.execute(&sql).await.expect("f insert");
            c.execute(&sql, []).expect("c insert");
        }
        f.execute("COMMIT").await.expect("f commit");

        // Compare counts
        let f_count = get_int(&f, "SELECT COUNT(*) FROM big").await.unwrap();
        let c_count: i64 = c
            .query_row("SELECT COUNT(*) FROM big", [], |r| r.get(0))
            .unwrap();
        assert_eq!(f_count, c_count);
        assert_eq!(f_count, 10_000);

        // Compare sums
        let f_sum = get_int(&f, "SELECT SUM(val) FROM big").await.unwrap();
        let c_sum: i64 = c
            .query_row("SELECT SUM(val) FROM big", [], |r| r.get(0))
            .unwrap();
        assert_eq!(f_sum, c_sum, "L1: sum mismatch");

        // Compare min/max
        let f_min = get_int(&f, "SELECT MIN(val) FROM big").await.unwrap();
        let c_min: i64 = c
            .query_row("SELECT MIN(val) FROM big", [], |r| r.get(0))
            .unwrap();
        assert_eq!(f_min, c_min, "L1: min mismatch");

        eprintln!("L1: 10K sequential inserts — oracle parity, sum={f_sum}");
    });
}

// ─── L2: Insert then delete 50% ──────────────────────────────────

#[test]
fn l2_insert_then_delete_half() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("l2.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        conn.execute("CREATE TABLE tree (id INTEGER PRIMARY KEY, val INTEGER)")
            .await
            .expect("create");

        // Insert 5000 rows
        conn.execute("BEGIN").await.expect("begin");
        for i in 1..=5000 {
            conn.execute(&format!("INSERT INTO tree VALUES ({i}, {i})"))
                .await
                .expect("insert");
        }
        conn.execute("COMMIT").await.expect("commit");

        assert_eq!(
            get_int(&conn, "SELECT COUNT(*) FROM tree").await.unwrap(),
            5000
        );

        // Delete even-id rows (half)
        conn.execute("DELETE FROM tree WHERE id % 2 = 0")
            .await
            .expect("delete evens");

        let remaining = get_int(&conn, "SELECT COUNT(*) FROM tree").await.unwrap();
        assert_eq!(remaining, 2500, "L2: should have 2500 after deleting evens");

        // Verify remaining rows are all odd
        let min_id = get_int(&conn, "SELECT MIN(id) FROM tree").await.unwrap();
        let max_id = get_int(&conn, "SELECT MAX(id) FROM tree").await.unwrap();
        assert_eq!(min_id, 1);
        assert_eq!(max_id, 4999);

        let odd_sum = get_int(&conn, "SELECT SUM(id) FROM tree").await.unwrap();
        // Sum of odd numbers 1,3,5,...,4999 = 2500^2 = 6_250_000
        assert_eq!(odd_sum, 6_250_000, "L2: sum of remaining odd IDs wrong");

        // Cross-connection check
        let reader = Connection::open(path_str).await.expect("reader");
        assert_eq!(
            get_int(&reader, "SELECT COUNT(*) FROM tree").await.unwrap(),
            2500
        );

        eprintln!("L2: insert 5000, delete 2500 evens — tree integrity verified");
    });
}

// ─── L3: Random-order inserts ─────────────────────────────────────

#[test]
fn l3_random_order_inserts() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("l3.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        conn.execute("CREATE TABLE shuffled (id INTEGER PRIMARY KEY, val INTEGER)")
            .await
            .expect("create");

        // Generate IDs in pseudo-random order using a simple LCG
        let n = 5000u64;
        let mut ids: Vec<u64> = (1..=n).collect();
        // Fisher-Yates-like shuffle with deterministic seed
        let mut rng_state = 42u64;
        for i in (1..ids.len()).rev() {
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let j = (rng_state >> 33) as usize % (i + 1);
            ids.swap(i, j);
        }

        conn.execute("BEGIN").await.expect("begin");
        for &id in &ids {
            conn.execute(&format!("INSERT INTO shuffled VALUES ({id}, {})", id * 7))
                .await
                .expect("insert");
        }
        conn.execute("COMMIT").await.expect("commit");

        // Despite random insert order, all rows should be present
        let count = get_int(&conn, "SELECT COUNT(*) FROM shuffled").await.unwrap();
        assert_eq!(count, n as i64, "L3: not all rows inserted");

        // ORDER BY should work correctly (tree structure is correct)
        let rows = conn
            .query("SELECT id FROM shuffled ORDER BY id LIMIT 5")
            .await
            .expect("query");
        let first_ids: Vec<i64> = rows
            .iter()
            .map(|r| match r.get(0) {
                Some(SqliteValue::Integer(v)) => *v,
                _ => -1,
            })
            .collect();
        assert_eq!(first_ids, vec![1, 2, 3, 4, 5], "L3: ORDER BY broken");

        // Sum should be correct
        let sum = get_int(&conn, "SELECT SUM(val) FROM shuffled").await.unwrap();
        // sum(i*7 for i in 1..=5000) = 7 * 5000*5001/2 = 87_517_500
        assert_eq!(sum, 87_517_500, "L3: sum wrong after random-order inserts");

        eprintln!("L3: 5000 random-order inserts — tree ordered correctly");
    });
}

// ─── L4: Large tree with secondary index ──────────────────────────

#[test]
fn l4_secondary_index_consistency() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("l4.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        conn.execute("CREATE TABLE idx_tbl (id INTEGER PRIMARY KEY, key TEXT, val INTEGER)")
            .await
            .expect("create");
        conn.execute("CREATE INDEX idx_key ON idx_tbl (key)")
            .await
            .expect("create index");

        conn.execute("BEGIN").await.expect("begin");
        for i in 1..=3000 {
            let key = format!("key_{:05}", i % 100);
            conn.execute(&format!("INSERT INTO idx_tbl VALUES ({i}, '{key}', {i})"))
                .await
                .expect("insert");
        }
        conn.execute("COMMIT").await.expect("commit");

        // Index scan should work
        let key_50_count = get_int(
            &conn,
            "SELECT COUNT(*) FROM idx_tbl WHERE key = 'key_00050'",
        )
        .await
        .unwrap();
        assert_eq!(key_50_count, 30, "L4: key_00050 should have 30 rows");

        // Range scan via index
        let range_count = get_int(
            &conn,
            "SELECT COUNT(*) FROM idx_tbl WHERE key >= 'key_00090'",
        )
        .await
        .unwrap();
        // Keys 90-99 = 10 distinct keys * 30 each = 300
        assert_eq!(range_count, 300, "L4: range scan count wrong");

        // Delete via idx_tbl column
        conn.execute("DELETE FROM idx_tbl WHERE key = 'key_00000'")
            .await
            .expect("delete via index");

        let after_delete = get_int(&conn, "SELECT COUNT(*) FROM idx_tbl").await.unwrap();
        assert_eq!(
            after_delete, 2970,
            "L4: should have 2970 after deleting key_00000"
        );

        // Verify index still consistent
        let zero_count = get_int(
            &conn,
            "SELECT COUNT(*) FROM idx_tbl WHERE key = 'key_00000'",
        )
        .await
        .unwrap();
        assert_eq!(zero_count, 0, "L4: deleted rows still visible via index");

        eprintln!("L4: secondary index on 3000 rows — consistency verified");
    });
}

// ─── L5: Insert→Delete→Reinsert cycles ───────────────────────────

#[test]
fn l5_insert_delete_reinsert_cycles() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("l5.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        conn.execute("CREATE TABLE cycling (id INTEGER PRIMARY KEY, cycle INTEGER)")
            .await
            .expect("create");

        for cycle in 0..10 {
            // Insert 500 rows
            conn.execute("BEGIN").await.expect("begin");
            for i in 1..=500 {
                conn.execute(&format!(
                    "INSERT OR REPLACE INTO cycling VALUES ({i}, {cycle})"
                ))
                .await
                .expect("insert");
            }
            conn.execute("COMMIT").await.expect("commit");

            let count = get_int(&conn, "SELECT COUNT(*) FROM cycling").await.unwrap();
            assert_eq!(count, 500, "L5: cycle {cycle} insert count wrong");

            // Delete half
            conn.execute("DELETE FROM cycling WHERE id > 250")
                .await
                .expect("delete half");

            let after = get_int(&conn, "SELECT COUNT(*) FROM cycling").await.unwrap();
            assert_eq!(after, 250, "L5: cycle {cycle} after delete count wrong");
        }

        // Final state: 250 rows from the last cycle
        let final_count = get_int(&conn, "SELECT COUNT(*) FROM cycling").await.unwrap();
        assert_eq!(final_count, 250);

        // All remaining rows should be from cycle 9
        let cycle_check = get_int(&conn, "SELECT COUNT(*) FROM cycling WHERE cycle = 9")
            .await
            .unwrap();
        assert_eq!(cycle_check, 250, "L5: remaining rows should all be cycle 9");

        eprintln!("L5: 10 insert/delete/reinsert cycles — tree integrity maintained");
    });
}

// ─── L6: Multi-table large tree ───────────────────────────────────

#[test]
fn l6_multi_table_large_tree() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("l6.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");

        // Create 5 tables
        for t in 0..5 {
            conn.execute(&format!(
                "CREATE TABLE t{t} (id INTEGER PRIMARY KEY, val INTEGER)"
            ))
            .await
            .expect("create");
        }

        // Insert 2000 rows into each
        conn.execute("BEGIN").await.expect("begin");
        for t in 0..5 {
            for i in 1..=2000 {
                conn.execute(&format!("INSERT INTO t{t} VALUES ({i}, {})", i + t * 10000))
                    .await
                    .expect("insert");
            }
        }
        conn.execute("COMMIT").await.expect("commit");

        // Verify each table
        for t in 0..5 {
            let count = get_int(&conn, &format!("SELECT COUNT(*) FROM t{t}"))
                .await
                .unwrap();
            assert_eq!(count, 2000, "L6: table t{t} should have 2000 rows");
        }

        // Cross-table JOIN
        let join_count = get_int(
            &conn,
            "SELECT COUNT(*) FROM t0 JOIN t1 ON t0.id = t1.id WHERE t0.id <= 10",
        )
        .await
        .unwrap();
        assert_eq!(join_count, 10, "L6: JOIN should return 10 rows");

        // Delete from one table shouldn't affect others
        conn.execute("DELETE FROM t2").await.expect("delete t2");
        for t in [0, 1, 3, 4] {
            let count = get_int(&conn, &format!("SELECT COUNT(*) FROM t{t}"))
                .await
                .unwrap();
            assert_eq!(count, 2000, "L6: table t{t} affected by t2 delete");
        }

        eprintln!("L6: 5 tables × 2000 rows — multi-table tree isolation verified");
    });
}

// ─── L7: Concurrent insert/delete on large tree ──────────────────

#[test]
fn l7_concurrent_insert_delete_large_tree() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("l7.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE contended (id INTEGER PRIMARY KEY, val INTEGER)")
                .await
                .expect("create");
            // Pre-populate
            conn.execute("BEGIN").await.expect("begin");
            for i in 1..=1000 {
                conn.execute(&format!("INSERT INTO contended VALUES ({i}, {i})"))
                    .await
                    .expect("seed");
            }
            conn.execute("COMMIT").await.expect("commit");
        }

        let stop = Arc::new(AtomicBool::new(false));

        // Inserter: adds new rows with high IDs
        let ins_path = path_str.to_string();
        let ins_stop = Arc::clone(&stop);
        let inserter = std::thread::spawn(move || {
            let mut inserted = 0u64;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&ins_path).await.expect("ins open");
                let mut seq = 10_000u64;
                while !ins_stop.load(Ordering::Relaxed) {
                    if conn.execute("BEGIN").await.is_ok() {
                        if conn
                            .execute(&format!(
                                "INSERT OR IGNORE INTO contended VALUES ({seq}, {seq})"
                            ))
                            .await
                            .is_ok()
                            && conn.execute("COMMIT").await.is_ok()
                        {
                            inserted += 1;
                        } else {
                            conn.execute("ROLLBACK").await.ok();
                        }
                    }
                    seq += 1;
                }
            });
            inserted
        });

        // Deleter: removes high-ID rows
        let del_path = path_str.to_string();
        let del_stop = Arc::clone(&stop);
        let deleter = std::thread::spawn(move || {
            let mut deleted = 0u64;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&del_path).await.expect("del open");
                while !del_stop.load(Ordering::Relaxed) {
                    if conn.execute("BEGIN").await.is_ok() {
                        conn.execute("DELETE FROM contended WHERE id > 5000")
                            .await
                            .ok();
                        if conn.execute("COMMIT").await.is_ok() {
                            deleted += 1;
                        } else {
                            conn.execute("ROLLBACK").await.ok();
                        }
                    }
                }
            });
            deleted
        });

        std::thread::sleep(Duration::from_secs(2));
        stop.store(true, Ordering::Relaxed);

        let ins_ops = inserter.join().expect("inserter must not panic");
        let del_ops = deleter.join().expect("deleter must not panic");

        // Verify original 1000 rows survive
        let verify = Connection::open(path_str).await.expect("verify");
        let original = get_int(&verify, "SELECT COUNT(*) FROM contended WHERE id <= 1000")
            .await
            .unwrap();
        assert_eq!(
            original, 1000,
            "L7: original 1000 rows damaged by concurrent ops"
        );

        eprintln!(
            "L7: concurrent insert/delete — {ins_ops} inserts, {del_ops} delete sweeps, original 1000 rows intact"
        );
    });
}
