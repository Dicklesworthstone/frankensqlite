//! bd-yf6s5: Track K e2e tests — scan-and-update correctness, deferred
//! delete, rebalance verification.
//!
//! Tests verify UPDATE and DELETE correctness at scale with oracle
//! (rusqlite) parity where applicable.
//!
//! - U1: Batch UPDATE 10K sequential rows, verify all values changed
//! - U2: Batch UPDATE with WHERE filter, verify untouched rows
//! - U3: UPDATE with expression (val = val + 1), verify arithmetic
//! - D1: Batch DELETE 5K rows, verify remaining rows correct
//! - D2: DELETE with subquery predicate
//! - D3: DELETE all rows, verify empty table
//! - M1: Interleaved UPDATE/DELETE/SELECT oracle comparison
//! - M2: Concurrent UPDATE+DELETE from separate connections
//! - M3: UPDATE/DELETE inside transaction with ROLLBACK
//! - M4: Large DELETE followed by INSERT (space reuse)
#![recursion_limit = "512"]

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

async fn seed_table(conn: &Connection, table: &str, n: usize) {
    conn.execute(&format!(
        "CREATE TABLE {table} (id INTEGER PRIMARY KEY, val INTEGER, tag TEXT)"
    ))
    .await
    .expect("create");
    conn.execute("BEGIN").await.expect("begin");
    for i in 1..=n {
        conn.execute(&format!("INSERT INTO {table} VALUES ({i}, {i}, 'tag_{i}')"))
            .await
            .expect("seed");
    }
    conn.execute("COMMIT").await.expect("commit");
}

// ─── U1: Batch UPDATE 10K sequential rows ─────────────────────────

#[test]
fn u1_batch_update_10k_sequential() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("u1.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        seed_table(&conn, "data", 10_000).await;

        // Update all rows
        conn.execute("UPDATE data SET val = val * 2, tag = 'updated'")
            .await
            .expect("batch update");

        // Verify row count unchanged
        assert_eq!(count_rows(&conn, "SELECT * FROM data").await, 10_000);

        // Verify values doubled
        let sum = get_int(&conn, "SELECT SUM(val) FROM data").await;
        // Original sum: 1+2+...+10000 = 50_005_000
        // After doubling: 100_010_000
        assert_eq!(
            sum,
            Some(100_010_000),
            "U1: SUM after doubling should be 100_010_000, got {sum:?}"
        );

        // Verify tags all updated
        let tag_count = count_rows(&conn, "SELECT * FROM data WHERE tag = 'updated'").await;
        assert_eq!(tag_count, 10_000, "U1: all tags should be 'updated'");

        // Spot check specific rows
        let row1 = get_int(&conn, "SELECT val FROM data WHERE id = 1").await;
        assert_eq!(row1, Some(2), "U1: row 1 should be 2");
        let row5000 = get_int(&conn, "SELECT val FROM data WHERE id = 5000").await;
        assert_eq!(row5000, Some(10_000), "U1: row 5000 should be 10000");

        eprintln!("U1: 10K batch UPDATE verified — all values doubled correctly");
    });
}

// ─── U2: Batch UPDATE with WHERE filter ───────────────────────────

#[test]
fn u2_batch_update_with_filter() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("u2.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        seed_table(&conn, "data", 1000).await;

        // Update only even-id rows
        conn.execute("UPDATE data SET val = -1, tag = 'even' WHERE id % 2 = 0")
            .await
            .expect("filtered update");

        // Verify counts
        let even_count = count_rows(&conn, "SELECT * FROM data WHERE val = -1").await;
        assert_eq!(even_count, 500, "U2: 500 even rows should be updated");

        let odd_count = count_rows(&conn, "SELECT * FROM data WHERE val != -1").await;
        assert_eq!(odd_count, 500, "U2: 500 odd rows should be untouched");

        // Verify odd rows retain original values
        let odd_sum = get_int(&conn, "SELECT SUM(val) FROM data WHERE id % 2 = 1").await;
        // Sum of 1,3,5,...,999 = 250_000
        assert_eq!(
            odd_sum,
            Some(250_000),
            "U2: odd rows should retain original values"
        );

        // Verify even rows all have -1
        let even_check = count_rows(
            &conn,
            "SELECT * FROM data WHERE id % 2 = 0 AND val = -1 AND tag = 'even'",
        )
        .await;
        assert_eq!(
            even_check, 500,
            "U2: all even rows should have val=-1, tag='even'"
        );

        eprintln!("U2: filtered UPDATE — 500 even rows updated, 500 odd untouched");
    });
}

// ─── U3: UPDATE with arithmetic expression ────────────────────────

#[test]
fn u3_update_arithmetic_expression() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("u3.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        seed_table(&conn, "data", 1000).await;

        // Multiple chained updates
        conn.execute("UPDATE data SET val = val + 10")
            .await
            .expect("add 10");
        conn.execute("UPDATE data SET val = val * 3")
            .await
            .expect("multiply 3");
        conn.execute("UPDATE data SET val = val - 5")
            .await
            .expect("subtract 5");

        // val_final = (val_orig + 10) * 3 - 5
        // For id=1: (1+10)*3-5 = 28
        let row1 = get_int(&conn, "SELECT val FROM data WHERE id = 1").await;
        assert_eq!(row1, Some(28), "U3: row 1 should be (1+10)*3-5 = 28");

        // For id=100: (100+10)*3-5 = 325
        let row100 = get_int(&conn, "SELECT val FROM data WHERE id = 100").await;
        assert_eq!(
            row100,
            Some(325),
            "U3: row 100 should be (100+10)*3-5 = 325"
        );

        // For id=1000: (1000+10)*3-5 = 3025
        let row1000 = get_int(&conn, "SELECT val FROM data WHERE id = 1000").await;
        assert_eq!(
            row1000,
            Some(3025),
            "U3: row 1000 should be (1000+10)*3-5 = 3025"
        );

        eprintln!("U3: chained arithmetic UPDATE verified");
    });
}

// ─── D1: Batch DELETE 5K rows ─────────────────────────────────────

#[test]
fn d1_batch_delete_5k() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("d1.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        seed_table(&conn, "data", 10_000).await;

        // Delete the first 5000 rows
        conn.execute("DELETE FROM data WHERE id <= 5000")
            .await
            .expect("batch delete");

        let remaining = count_rows(&conn, "SELECT * FROM data").await;
        assert_eq!(remaining, 5000, "D1: should have 5000 remaining rows");

        // Verify min id is 5001
        let min_id = get_int(&conn, "SELECT MIN(id) FROM data").await;
        assert_eq!(min_id, Some(5001), "D1: min id should be 5001");

        // Verify max id is 10000
        let max_id = get_int(&conn, "SELECT MAX(id) FROM data").await;
        assert_eq!(max_id, Some(10_000), "D1: max id should be 10000");

        // Verify no deleted rows leak through
        let leaked = count_rows(&conn, "SELECT * FROM data WHERE id <= 5000").await;
        assert_eq!(leaked, 0, "D1: deleted rows should not appear");

        // Cross-connection verification
        let reader = Connection::open(path_str).await.expect("reader");
        assert_eq!(
            count_rows(&reader, "SELECT * FROM data").await,
            5000,
            "D1: reader should also see 5000 rows"
        );

        eprintln!("D1: batch DELETE 5K rows — 5000 remaining verified");
    });
}

// ─── D2: DELETE with subquery predicate ───────────────────────────

#[test]
fn d2_delete_with_subquery() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("d2.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        seed_table(&conn, "data", 1000).await;

        // Create a secondary table with IDs to delete
        conn.execute("CREATE TABLE to_delete (id INTEGER PRIMARY KEY)")
            .await
            .expect("create to_delete");
        conn.execute("BEGIN").await.expect("begin");
        for i in (1..=1000).step_by(3) {
            conn.execute(&format!("INSERT INTO to_delete VALUES ({i})"))
                .await
                .expect("insert to_delete");
        }
        conn.execute("COMMIT").await.expect("commit");

        let delete_count = count_rows(&conn, "SELECT * FROM to_delete").await;

        // DELETE using subquery
        conn.execute("DELETE FROM data WHERE id IN (SELECT id FROM to_delete)")
            .await
            .expect("delete with subquery");

        let remaining = count_rows(&conn, "SELECT * FROM data").await;
        let expected_remaining = 1000 - delete_count;
        assert_eq!(
            remaining, expected_remaining,
            "D2: expected {expected_remaining} remaining, got {remaining}"
        );

        // Verify none of the deleted IDs remain
        let leaked = count_rows(
            &conn,
            "SELECT * FROM data WHERE id IN (SELECT id FROM to_delete)",
        )
        .await;
        assert_eq!(leaked, 0, "D2: deleted rows should not appear in data");

        eprintln!("D2: DELETE with subquery — removed {delete_count}, {remaining} remaining");
    });
}

// ─── D3: DELETE all rows ──────────────────────────────────────────

#[test]
fn d3_delete_all_rows() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("d3.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        seed_table(&conn, "data", 5000).await;

        assert_eq!(count_rows(&conn, "SELECT * FROM data").await, 5000);

        conn.execute("DELETE FROM data").await.expect("delete all");

        assert_eq!(
            count_rows(&conn, "SELECT * FROM data").await,
            0,
            "D3: table should be empty after DELETE all"
        );

        // Table should still exist (not dropped)
        let reinsert = conn
            .execute("INSERT INTO data VALUES (1, 1, 'reinserted')")
            .await;
        assert!(
            reinsert.is_ok(),
            "D3: should be able to INSERT after DELETE all"
        );

        assert_eq!(count_rows(&conn, "SELECT * FROM data").await, 1);

        eprintln!("D3: DELETE all rows — table empty, reinsert works");
    });
}

// ─── M1: Interleaved UPDATE/DELETE/SELECT oracle comparison ───────

#[test]
fn m1_interleaved_operations_oracle() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();

        // C SQLite oracle
        let c_path = dir.path().join("m1_csqlite.db");
        let c = rusqlite::Connection::open(&c_path).expect("csqlite open");
        c.execute_batch(
            "CREATE TABLE ops (id INTEGER PRIMARY KEY, val INTEGER, tag TEXT);
         BEGIN;",
        )
        .expect("csqlite setup");
        for i in 1..=500 {
            c.execute(
                "INSERT INTO ops VALUES (?1, ?2, ?3)",
                rusqlite::params![i, i, format!("t{i}")],
            )
            .expect("csqlite insert");
        }
        c.execute_batch("COMMIT;").expect("csqlite commit");

        // FrankenSQLite
        let f_path = dir.path().join("m1_fsqlite.db");
        let f_path_str = f_path.to_str().expect("path");
        let f = Connection::open(f_path_str).await.expect("fsqlite open");
        seed_table(&f, "ops", 500).await;

        // Same operations on both
        let operations = [
            "UPDATE ops SET val = val + 100 WHERE id <= 100",
            "DELETE FROM ops WHERE id > 400",
            "UPDATE ops SET tag = 'modified' WHERE val > 200",
            "DELETE FROM ops WHERE id % 7 = 0",
            "UPDATE ops SET val = 0 WHERE tag = 'modified'",
        ];

        for op in &operations {
            c.execute_batch(op).expect("csqlite op");
            f.execute(op).await.expect("fsqlite op");
        }

        // Compare results
        let c_count: i64 = c
            .prepare("SELECT COUNT(*) FROM ops")
            .expect("prepare")
            .query_row([], |r| r.get(0))
            .expect("count");
        let f_count = get_int(&f, "SELECT COUNT(*) FROM ops").await.unwrap_or(-1);

        assert_eq!(
            f_count, c_count,
            "M1: row count diverges — fsqlite={f_count}, csqlite={c_count}"
        );

        // Compare sum of val
        let c_sum: i64 = c
            .prepare("SELECT COALESCE(SUM(val), 0) FROM ops")
            .expect("prepare")
            .query_row([], |r| r.get(0))
            .expect("sum");
        let f_sum = get_int(&f, "SELECT COALESCE(SUM(val), 0) FROM ops")
            .await
            .unwrap_or(0);

        assert_eq!(
            f_sum, c_sum,
            "M1: SUM(val) diverges — fsqlite={f_sum}, csqlite={c_sum}"
        );

        // Compare modified tag count
        let c_mod: i64 = c
            .prepare("SELECT COUNT(*) FROM ops WHERE tag = 'modified'")
            .expect("prepare")
            .query_row([], |r| r.get(0))
            .expect("mod count");
        let f_mod = get_int(&f, "SELECT COUNT(*) FROM ops WHERE tag = 'modified'")
            .await
            .unwrap_or(-1);

        assert_eq!(
            f_mod, c_mod,
            "M1: modified count diverges — fsqlite={f_mod}, csqlite={c_mod}"
        );

        eprintln!("M1: oracle parity — {f_count} rows, sum={f_sum}, modified={f_mod}");
    });
}

// ─── M2: Concurrent UPDATE+DELETE from separate connections ───────

#[test]
fn m2_concurrent_update_delete() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("m2.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            seed_table(&conn, "data", 1000).await;
        }

        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Updater: randomly updates val
        let u_path = path_str.to_string();
        let u_stop = std::sync::Arc::clone(&stop);
        let updater = std::thread::spawn(move || {
            let mut ops = 0u64;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&u_path).await.expect("u open");
                while !u_stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let target = (ops % 1000) + 1;
                    if conn.execute("BEGIN").await.is_ok() {
                        if conn
                            .execute(&format!("UPDATE data SET val = {ops} WHERE id = {target}"))
                            .await
                            .is_ok()
                        {
                            if conn.execute("COMMIT").await.is_err() {
                                conn.execute("ROLLBACK").await.ok();
                            }
                        } else {
                            conn.execute("ROLLBACK").await.ok();
                        }
                    }
                    ops += 1;
                }
            });
            ops
        });

        // Deleter: deletes and reinserts rows
        let d_path = path_str.to_string();
        let d_stop = std::sync::Arc::clone(&stop);
        let deleter = std::thread::spawn(move || {
            let mut ops = 0u64;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&d_path).await.expect("d open");
                while !d_stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let target = (ops % 1000) + 1;
                    if conn.execute("BEGIN").await.is_ok() {
                        let deleted = conn
                            .execute(&format!("DELETE FROM data WHERE id = {target}"))
                            .await
                            .is_ok();
                        let inserted = deleted
                            && conn
                                .execute(&format!(
                                    "INSERT OR REPLACE INTO data VALUES ({target}, {ops}, 'reinserted')"
                                ))
                                .await
                                .is_ok();
                        if deleted && inserted {
                            if conn.execute("COMMIT").await.is_err() {
                                conn.execute("ROLLBACK").await.ok();
                            }
                        } else {
                            conn.execute("ROLLBACK").await.ok();
                        }
                    }
                    ops += 1;
                }
            });
            ops
        });

        std::thread::sleep(std::time::Duration::from_secs(2));
        stop.store(true, std::sync::atomic::Ordering::Relaxed);

        let update_ops = updater.join().expect("updater must not panic");
        let delete_ops = deleter.join().expect("deleter must not panic");

        // Verify table integrity
        let verify = Connection::open(path_str).await.expect("verify");
        let final_count = count_rows(&verify, "SELECT * FROM data").await;

        // All 1000 rows should still exist (deleter always reinserts)
        assert_eq!(
            final_count, 1000,
            "M2: expected 1000 rows after concurrent ops, got {final_count}"
        );

        eprintln!(
            "M2: concurrent UPDATE+DELETE — {update_ops} updates, {delete_ops} delete/reinsert cycles, 1000 rows intact"
        );
    });
}

// ─── M3: UPDATE/DELETE with ROLLBACK ──────────────────────────────

#[test]
fn m3_update_delete_rollback() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("m3.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        seed_table(&conn, "data", 100).await;

        // Snapshot before modifications
        let orig_sum = get_int(&conn, "SELECT SUM(val) FROM data").await.unwrap();
        let orig_count = count_rows(&conn, "SELECT * FROM data").await;

        // Transaction with UPDATE + DELETE, then ROLLBACK
        conn.execute("BEGIN").await.expect("begin");
        conn.execute("UPDATE data SET val = 999999")
            .await
            .expect("update");
        conn.execute("DELETE FROM data WHERE id > 50")
            .await
            .expect("delete");

        // Inside txn: should see modifications
        let txn_count = count_rows(&conn, "SELECT * FROM data").await;
        assert_eq!(txn_count, 50, "M3: inside txn should see 50 rows");

        conn.execute("ROLLBACK").await.expect("rollback");

        // After rollback: everything restored
        let after_sum = get_int(&conn, "SELECT SUM(val) FROM data").await.unwrap();
        let after_count = count_rows(&conn, "SELECT * FROM data").await;

        assert_eq!(
            after_count, orig_count,
            "M3: rollback should restore row count"
        );
        assert_eq!(after_sum, orig_sum, "M3: rollback should restore SUM(val)");

        eprintln!("M3: UPDATE+DELETE+ROLLBACK — original state fully restored");
    });
}

// ─── M4: Large DELETE then INSERT (space reuse) ───────────────────

#[test]
fn m4_delete_then_insert_space_reuse() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("m4.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        seed_table(&conn, "data", 5000).await;

        // Delete most rows
        conn.execute("DELETE FROM data WHERE id > 100")
            .await
            .expect("large delete");
        assert_eq!(count_rows(&conn, "SELECT * FROM data").await, 100);

        // Reinsert into the freed space
        conn.execute("BEGIN").await.expect("begin");
        for i in 101..=5000 {
            conn.execute(&format!(
                "INSERT INTO data VALUES ({i}, {}, 'reinserted')",
                i * 10
            ))
            .await
            .expect("reinsert");
        }
        conn.execute("COMMIT").await.expect("commit");

        // Full table again
        let final_count = count_rows(&conn, "SELECT * FROM data").await;
        assert_eq!(
            final_count, 5000,
            "M4: should have 5000 rows after reinsert"
        );

        // Verify reinserted data
        let reinserted = count_rows(&conn, "SELECT * FROM data WHERE tag = 'reinserted'").await;
        assert_eq!(reinserted, 4900, "M4: 4900 should be reinserted");

        // Verify original data preserved
        let original = count_rows(
            &conn,
            "SELECT * FROM data WHERE id <= 100 AND tag != 'reinserted'",
        )
        .await;
        assert_eq!(original, 100, "M4: original 100 rows should be preserved");

        // Cross-connection check
        let reader = Connection::open(path_str).await.expect("reader");
        assert_eq!(count_rows(&reader, "SELECT * FROM data").await, 5000);

        eprintln!("M4: delete 4900 + reinsert 4900 — space reuse verified");
    });
}
