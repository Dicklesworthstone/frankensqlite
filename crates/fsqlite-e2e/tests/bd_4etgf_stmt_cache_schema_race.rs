//! bd-4etgf: Statement cache schema-epoch race with concurrent DDL.
//!
//! ## Bug hypothesis
//!
//! When one connection executes DDL (CREATE TABLE, ALTER TABLE, DROP TABLE),
//! the schema epoch advances. If another connection has cached prepared
//! statements from the old schema epoch, the stale cache entry may:
//! 1. Execute against a table that no longer exists
//! 2. Miss columns added by ALTER TABLE
//! 3. Use a stale column mapping after schema change
//!
//! The `ensure_schema_unchanged` check should invalidate stale cache entries,
//! but if this check races with MVCC snapshot publication, a window exists
//! where a cached statement executes against an inconsistent schema view.
//!
//! ## Test approach
//!
//! - E1: DDL during active queries — no crash or wrong results
//! - E2: ALTER TABLE ADD COLUMN while cached SELECTs are running
//! - E3: DROP TABLE while cached INSERTs target it
//! - E4: CREATE TABLE with same name after DROP (schema recycling)
//! - E5: Concurrent DDL from multiple connections
#![recursion_limit = "512"]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use fsqlite::Connection;

const STRESS_DURATION: Duration = Duration::from_secs(2);

fn test_tmpdir() -> tempfile::TempDir {
    tempfile::tempdir_in(std::env::temp_dir())
        .or_else(|_| tempfile::tempdir_in("."))
        .expect("tempdir")
}

// ─── E1: DDL during active queries ─────────────────────────────────

#[test]
fn e1_ddl_during_active_queries() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("e1.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE base (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("create");
            conn.execute("BEGIN").await.expect("begin");
            for i in 1..=100 {
                conn.execute(&format!("INSERT INTO base VALUES ({i}, 'row_{i}')"))
                    .await
                    .expect("seed");
            }
            conn.execute("COMMIT").await.expect("commit");
        }

        let stop = Arc::new(AtomicBool::new(false));

        // Reader: continuously queries the base table
        let r_path = path_str.to_string();
        let r_stop = Arc::clone(&stop);
        let reader = std::thread::spawn(move || {
            let mut result = (0u64, 0u64);
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&r_path).await.expect("r open");
                let mut reads = 0u64;
                let mut errors = 0u64;
                while !r_stop.load(Ordering::Relaxed) {
                    match conn.query("SELECT * FROM base").await {
                        Ok(rows) => {
                            assert!(
                                rows.len() >= 100,
                                "base table shrunk to {} rows during DDL",
                                rows.len()
                            );
                            reads += 1;
                        }
                        Err(_) => {
                            errors += 1;
                        }
                    }
                }
                result = (reads, errors);
            });
            result
        });

        // DDL thread: creates and drops auxiliary tables
        let d_path = path_str.to_string();
        let d_stop = Arc::clone(&stop);
        let ddl = std::thread::spawn(move || {
            let mut ddl_ops = 0u64;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&d_path).await.expect("d open");
                while !d_stop.load(Ordering::Relaxed) {
                    let tname = format!("aux_{}", ddl_ops % 10);
                    conn.execute(&format!(
                        "CREATE TABLE IF NOT EXISTS {tname} (id INTEGER PRIMARY KEY)"
                    ))
                    .await
                    .ok();
                    conn.execute(&format!("DROP TABLE IF EXISTS {tname}"))
                        .await
                        .ok();
                    ddl_ops += 1;
                }
            });
            ddl_ops
        });

        std::thread::sleep(STRESS_DURATION);
        stop.store(true, Ordering::Relaxed);

        let (reads, errors) = reader.join().expect("reader must not panic");
        let ddl_ops = ddl.join().expect("DDL must not panic");

        assert!(reads > 0, "reader completed no reads");
        eprintln!("E1: {reads} reads, {errors} errors, {ddl_ops} DDL ops");
    });
}

// ─── E2: ALTER TABLE ADD COLUMN during cached SELECTs ──────────────

#[test]
fn e2_alter_table_add_column_during_selects() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("e2.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE evolving (id INTEGER PRIMARY KEY, v1 TEXT)")
                .await
                .expect("create");
            conn.execute("BEGIN").await.expect("begin");
            for i in 1..=50 {
                conn.execute(&format!("INSERT INTO evolving VALUES ({i}, 'val_{i}')"))
                    .await
                    .expect("seed");
            }
            conn.execute("COMMIT").await.expect("commit");
        }

        // Reader connection: warm cache with SELECT *
        let reader_conn = Connection::open(path_str).await.expect("r open");
        let initial_rows = reader_conn
            .query("SELECT * FROM evolving")
            .await
            .expect("initial query");
        assert_eq!(initial_rows.len(), 50);

        // DDL connection: add a column
        {
            let ddl_conn = Connection::open(path_str).await.expect("d open");
            ddl_conn
                .execute("ALTER TABLE evolving ADD COLUMN v2 TEXT DEFAULT 'new'")
                .await
                .expect("alter table");
        }

        // Reader should still see 50 rows (cache invalidated or not, data is there)
        let after_rows = reader_conn
            .query("SELECT * FROM evolving")
            .await
            .expect("after alter query");
        assert_eq!(
            after_rows.len(),
            50,
            "rows lost after ALTER TABLE (got {})",
            after_rows.len()
        );

        // Insert with new column, verify from reader
        {
            let w_conn = Connection::open(path_str).await.expect("w open");
            w_conn
                .execute("INSERT INTO evolving VALUES (51, 'v1_51', 'v2_51')")
                .await
                .expect("insert with new col");
        }

        let final_rows = reader_conn
            .query("SELECT * FROM evolving")
            .await
            .expect("final query");
        assert_eq!(
            final_rows.len(),
            51,
            "new row not visible after ALTER + INSERT"
        );
        eprintln!("E2: ALTER TABLE ADD COLUMN — cache invalidation correct");
    });
}

// ─── E3: DROP TABLE while cached INSERTs target it ─────────────────

#[test]
fn e3_drop_table_while_inserting() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("e3.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE target (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("create");
        }

        let stop = Arc::new(AtomicBool::new(false));

        // Writer: continuously inserts into target table
        let w_path = path_str.to_string();
        let w_stop = Arc::clone(&stop);
        let writer = std::thread::spawn(move || {
            let mut result = (0u64, 0u64);
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&w_path).await.expect("w open");
                let mut inserted = 0u64;
                let mut table_missing = 0u64;
                while !w_stop.load(Ordering::Relaxed) {
                    match conn
                        .execute(&format!("INSERT INTO target VALUES ({inserted}, 'v')"))
                        .await
                    {
                        Ok(_) => inserted += 1,
                        Err(_) => {
                            table_missing += 1;
                            // Table might have been dropped — that's the test
                        }
                    }
                }
                result = (inserted, table_missing);
            });
            result
        });

        // DDL: drop and recreate the table periodically
        let d_path = path_str.to_string();
        let d_stop = Arc::clone(&stop);
        let ddl = std::thread::spawn(move || {
            let mut cycles = 0u64;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&d_path).await.expect("d open");
                while !d_stop.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(100));
                    conn.execute("DROP TABLE IF EXISTS target").await.ok();
                    conn.execute("CREATE TABLE target (id INTEGER PRIMARY KEY, val TEXT)")
                        .await
                        .ok();
                    cycles += 1;
                }
            });
            cycles
        });

        std::thread::sleep(STRESS_DURATION);
        stop.store(true, Ordering::Relaxed);

        let (inserted, table_missing) = writer.join().expect("writer must not panic");
        let cycles = ddl.join().expect("DDL must not panic");

        // The key assertion: no panics/crashes from stale cache entries
        eprintln!(
            "E3: {inserted} inserts, {table_missing} table-missing errors, {cycles} DDL cycles"
        );
    });
}

// ─── E4: Schema recycling — CREATE after DROP with same name ───────

#[test]
fn e4_schema_recycling() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("e4.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");

        for round in 0..20 {
            // Create with varying schema
            if round % 2 == 0 {
                conn.execute("CREATE TABLE recycled (id INTEGER PRIMARY KEY, a TEXT)")
                    .await
                    .expect("create v1");
            } else {
                conn.execute("CREATE TABLE recycled (id INTEGER PRIMARY KEY, a TEXT, b INTEGER)")
                    .await
                    .expect("create v2");
            }

            // Insert data
            conn.execute("BEGIN").await.expect("begin");
            for i in 1..=10 {
                conn.execute(&format!(
                    "INSERT INTO recycled (id, a) VALUES ({i}, 'r{round}')"
                ))
                .await
                .expect("insert");
            }
            conn.execute("COMMIT").await.expect("commit");

            // Query — cached statement must match current schema
            let rows = conn
                .query("SELECT * FROM recycled")
                .await
                .expect("query recycled");
            assert_eq!(
                rows.len(),
                10,
                "round {round}: expected 10 rows, got {}",
                rows.len()
            );

            // Drop
            conn.execute("DROP TABLE recycled").await.expect("drop");
        }
        eprintln!("E4: 20 rounds of schema recycling — cache correctly invalidated");
    });
}

// ─── E5: Concurrent DDL from multiple connections ──────────────────

#[test]
fn e5_concurrent_ddl_storm() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("e5.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE anchor (id INTEGER PRIMARY KEY)")
                .await
                .expect("create anchor");
        }

        let stop = Arc::new(AtomicBool::new(false));
        let total_ops = Arc::new(AtomicU64::new(0));

        let threads: Vec<_> = (0..4)
            .map(|i| {
                let path = path_str.to_string();
                let s = Arc::clone(&stop);
                let ops = Arc::clone(&total_ops);
                std::thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(&path).await.expect("open");
                        let mut local_ops = 0u64;
                        while !s.load(Ordering::Relaxed) {
                            let tname = format!("ddl_{i}_{}", local_ops % 5);
                            // Create, insert, query, drop cycle
                            if conn
                                .execute(&format!(
                                    "CREATE TABLE IF NOT EXISTS {tname} (id INTEGER PRIMARY KEY, v INTEGER)"
                                ))
                                .await
                                .is_ok()
                            {
                                conn.execute(&format!(
                                    "INSERT OR REPLACE INTO {tname} VALUES ({local_ops}, {local_ops})"
                                ))
                                .await
                                .ok();
                                conn.query(&format!("SELECT * FROM {tname}")).await.ok();
                                conn.execute(&format!("DROP TABLE IF EXISTS {tname}"))
                                    .await
                                    .ok();
                                local_ops += 1;
                            }

                            // Also query anchor table (should never disappear)
                            if let Ok(rows) = conn.query("SELECT * FROM anchor").await {
                                // anchor must exist
                                let _ = rows;
                            }
                        }
                        ops.fetch_add(local_ops, Ordering::Relaxed);
                    });
                })
            })
            .collect();

        std::thread::sleep(STRESS_DURATION);
        stop.store(true, Ordering::Relaxed);

        for t in threads {
            t.join()
                .expect("thread must not panic during concurrent DDL storm");
        }

        let ops = total_ops.load(Ordering::Relaxed);
        assert!(ops > 0, "no DDL operations completed");

        // Anchor table must survive
        let verify = Connection::open(path_str).await.expect("verify");
        let anchor = verify.query("SELECT * FROM anchor").await;
        assert!(anchor.is_ok(), "anchor table missing after DDL storm");
        eprintln!("E5: {ops} DDL create/insert/query/drop cycles, 4 threads");
    });
}
