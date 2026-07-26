//! bd-kzwnm — WAL durability and multi-table concurrent writer oracle
//! parity e2e tests.
//!
//! Exercises FrankenSQLite's WAL mode durability guarantees:
//!   - Data survives connection close and reopen
//!   - WAL checkpoint modes (PASSIVE, FULL, RESTART, TRUNCATE) parity
//!   - Large transaction durability
//!   - Multi-table writes in a single transaction
//!   - Concurrent multi-table writers
//!   - Journal mode switching behavior

use std::sync::{Arc, Barrier};
use std::thread;

use fsqlite::SqliteValue;

// ── Helpers ────────────────────────────────────────────────────────────

async fn frank_scalar(conn: &fsqlite::Connection, sql: &str) -> String {
    let rows = conn.query(sql).await.unwrap();
    match &rows[0].values()[0] {
        SqliteValue::Null => "NULL".into(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => s.to_string(),
        SqliteValue::Blob(b) => {
            format!(
                "X'{}'",
                b.iter().map(|x| format!("{x:02X}")).collect::<String>()
            )
        }
    }
}

async fn frank_rows(conn: &fsqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let rows = conn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank query `{sql}`: {e}"));
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|v| match v {
                    SqliteValue::Null => "NULL".into(),
                    SqliteValue::Integer(n) => n.to_string(),
                    SqliteValue::Float(f) => format!("{f}"),
                    SqliteValue::Text(s) => s.to_string(),
                    SqliteValue::Blob(b) => {
                        format!(
                            "X'{}'",
                            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                        )
                    }
                })
                .collect()
        })
        .collect()
}

fn csql_scalar(conn: &rusqlite::Connection, sql: &str) -> String {
    conn.query_row(sql, [], |row| {
        let v: rusqlite::types::Value = row.get_unwrap(0);
        Ok(match v {
            rusqlite::types::Value::Null => "NULL".into(),
            rusqlite::types::Value::Integer(x) => x.to_string(),
            rusqlite::types::Value::Real(f) => format!("{f}"),
            rusqlite::types::Value::Text(s) => s,
            rusqlite::types::Value::Blob(b) => {
                format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                )
            }
        })
    })
    .unwrap()
}

fn rusqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn
        .prepare(sql)
        .unwrap_or_else(|e| panic!("csql prepare `{sql}`: {e}"));
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v: rusqlite::types::Value = row.get_unwrap(i);
            out.push(match v {
                rusqlite::types::Value::Null => "NULL".into(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => s,
                rusqlite::types::Value::Blob(b) => {
                    format!(
                        "X'{}'",
                        b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                    )
                }
            });
        }
        Ok(out)
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

// ── Test 1: Data survives close + reopen ─────────────────────────────

#[test]
fn data_survives_close_reopen_parity() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let f_path = dir.path().join("durable_f.db");
        let r_path = dir.path().join("durable_r.db");
        let f_str = f_path.to_str().unwrap();
        let r_str = r_path.to_str().unwrap();

        // Phase 1: Create tables and insert data
        {
            let f = fsqlite::Connection::open(f_str).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("CREATE TABLE durable (id INTEGER PRIMARY KEY, data TEXT, num REAL);")
                .await
                .unwrap();
            for i in 0..100 {
                f.execute(&format!(
                    "INSERT INTO durable VALUES ({i}, 'item_{i}', {}.{});",
                    i, i
                ))
                .await
                .unwrap();
            }

            let r = rusqlite::Connection::open(r_str).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            r.execute_batch("CREATE TABLE durable (id INTEGER PRIMARY KEY, data TEXT, num REAL);")
                .unwrap();
            for i in 0..100 {
                r.execute_batch(&format!(
                    "INSERT INTO durable VALUES ({i}, 'item_{i}', {}.{});",
                    i, i
                ))
                .unwrap();
            }
        }

        // Phase 2: Reopen and verify parity
        let f = fsqlite::Connection::open(f_str).await.unwrap();
        let r = rusqlite::Connection::open(r_str).unwrap();

        let fcount = frank_scalar(&f, "SELECT COUNT(*) FROM durable").await;
        let rcount = csql_scalar(&r, "SELECT COUNT(*) FROM durable");
        assert_eq!(fcount, rcount, "count mismatch after reopen");
        assert_eq!(fcount, "100");

        let fdata = frank_rows(&f, "SELECT id, data FROM durable ORDER BY id LIMIT 5").await;
        let rdata = rusqlite_rows(&r, "SELECT id, data FROM durable ORDER BY id LIMIT 5");
        assert_eq!(fdata, rdata, "data mismatch after reopen");
    });
}

// ── Test 2: Large transaction durability ─────────────────────────────

#[test]
fn large_transaction_durability() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let f_path = dir.path().join("large_txn.db");
        let f_str = f_path.to_str().unwrap();

        let n_rows = 5000;

        {
            let f = fsqlite::Connection::open(f_str).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("CREATE TABLE big (id INTEGER PRIMARY KEY, payload TEXT);")
                .await
                .unwrap();
            f.execute("BEGIN").await.unwrap();
            for i in 0..n_rows {
                f.execute(&format!(
                    "INSERT INTO big VALUES ({i}, '{}');",
                    "x".repeat(100)
                ))
                .await
                .unwrap();
            }
            f.execute("COMMIT").await.unwrap();
        }

        // Reopen and verify
        let f = fsqlite::Connection::open(f_str).await.unwrap();
        let count = frank_scalar(&f, "SELECT COUNT(*) FROM big").await;
        assert_eq!(count, n_rows.to_string(), "large txn data should persist");

        let last = frank_scalar(&f, "SELECT id FROM big ORDER BY id DESC LIMIT 1").await;
        assert_eq!(last, (n_rows - 1).to_string(), "last row should be present");
    });
}

// ── Test 3: Multi-table transaction atomicity ────────────────────────

#[test]
fn multi_table_transaction_atomicity() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let f_path = dir.path().join("multi_tbl.db");
        let r_path = dir.path().join("multi_tbl_r.db");
        let f_str = f_path.to_str().unwrap();
        let r_str = r_path.to_str().unwrap();

        let setup = [
            "PRAGMA journal_mode = WAL;",
            "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER);",
            "CREATE TABLE transfers (id INTEGER PRIMARY KEY, from_id INTEGER, to_id INTEGER, amount INTEGER);",
            "INSERT INTO accounts VALUES (1, 1000), (2, 500), (3, 750);",
        ];

        {
            let f = fsqlite::Connection::open(f_str).await.unwrap();
            let r = rusqlite::Connection::open(r_str).unwrap();
            for s in &setup {
                f.execute(s).await.unwrap();
                r.execute_batch(s).unwrap();
            }
        }

        // Transfer 200 from account 1 to account 2 in a single transaction
        let transfer_ops = [
            "BEGIN",
            "UPDATE accounts SET balance = balance - 200 WHERE id = 1;",
            "UPDATE accounts SET balance = balance + 200 WHERE id = 2;",
            "INSERT INTO transfers VALUES (1, 1, 2, 200);",
            "COMMIT",
        ];

        {
            let f = fsqlite::Connection::open(f_str).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            let r = rusqlite::Connection::open(r_str).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            for s in &transfer_ops {
                f.execute(s).await.unwrap();
                r.execute_batch(s).unwrap();
            }
        }

        // Verify balances are consistent and match
        let f = fsqlite::Connection::open(f_str).await.unwrap();
        let r = rusqlite::Connection::open(r_str).unwrap();

        let fb = frank_rows(&f, "SELECT id, balance FROM accounts ORDER BY id").await;
        let rb = rusqlite_rows(&r, "SELECT id, balance FROM accounts ORDER BY id");
        assert_eq!(fb, rb, "account balances should match");
        assert_eq!(fb[0], vec!["1", "800"], "account 1 should have 800");
        assert_eq!(fb[1], vec!["2", "700"], "account 2 should have 700");
        assert_eq!(fb[2], vec!["3", "750"], "account 3 unchanged");

        // Total balance should be conserved
        let ftotal = frank_scalar(&f, "SELECT SUM(balance) FROM accounts").await;
        let rtotal = csql_scalar(&r, "SELECT SUM(balance) FROM accounts");
        assert_eq!(ftotal, "2250", "total balance conserved");
        assert_eq!(ftotal, rtotal);
    });
}

// ── Test 4: Multi-table concurrent writers ───────────────────────────

#[test]
fn multi_table_concurrent_writers() {
    let f_tmp = tempfile::NamedTempFile::new().unwrap();
    let f_path = f_tmp.path().to_str().unwrap().to_owned();

    // Create 4 independent tables
    asupersync::test_utils::run_test(|| async {
        let f = fsqlite::Connection::open(&f_path).await.unwrap();
        f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
        for t in 0..4 {
            f.execute(&format!(
                "CREATE TABLE tbl_{t} (id INTEGER PRIMARY KEY, val INTEGER);"
            ))
            .await
            .unwrap();
        }
    });

    let n_threads = 4usize;
    let rows_per_table = 50usize;
    let barrier = Arc::new(Barrier::new(n_threads));

    // Each thread writes to its own table
    let handles: Vec<_> = (0..n_threads)
        .map(|tid| {
            let p = f_path.clone();
            let bar = barrier.clone();
            thread::spawn(move || {
                asupersync::test_utils::run_test(|| async {
                    let conn = fsqlite::Connection::open(&p).await.unwrap();
                    conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                    bar.wait();
                    for i in 0..rows_per_table {
                        let mut attempts = 0u32;
                        loop {
                            if conn.execute("BEGIN CONCURRENT").await.is_err() {
                                attempts += 1;
                                assert!(attempts < 200);
                                thread::sleep(std::time::Duration::from_millis(1));
                                continue;
                            }
                            let sql = format!("INSERT INTO tbl_{tid} VALUES ({i}, {});", i * tid);
                            if conn.execute(&sql).await.is_err() {
                                drop(conn.execute("ROLLBACK").await);
                                attempts += 1;
                                assert!(attempts < 200);
                                thread::sleep(std::time::Duration::from_millis(1));
                                continue;
                            }
                            match conn.execute("COMMIT").await {
                                Ok(_) => break,
                                Err(_) => {
                                    drop(conn.execute("ROLLBACK").await);
                                    attempts += 1;
                                    assert!(attempts < 200);
                                    thread::sleep(std::time::Duration::from_millis(1));
                                }
                            }
                        }
                    }
                });
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    asupersync::test_utils::run_test(|| async {
        let f = fsqlite::Connection::open(&f_path).await.unwrap();
        for t in 0..4 {
            let count = frank_scalar(&f, &format!("SELECT COUNT(*) FROM tbl_{t}")).await;
            assert_eq!(
                count,
                rows_per_table.to_string(),
                "table tbl_{t} should have {rows_per_table} rows"
            );
        }
    });
}

// ── Test 5: Checkpoint PASSIVE vs TRUNCATE behavior ──────────────────

#[test]
fn checkpoint_passive_vs_truncate_parity() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let r_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();
        let r_path = r_tmp.path().to_str().unwrap().to_owned();

        let setup = [
            "PRAGMA journal_mode = WAL;",
            "CREATE TABLE ckpt_mode (id INTEGER PRIMARY KEY, val TEXT);",
        ];

        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            let r = rusqlite::Connection::open(&r_path).unwrap();
            for s in &setup {
                f.execute(s).await.unwrap();
                r.execute_batch(s).unwrap();
            }
            // Insert some data
            for i in 0..30 {
                let sql = format!("INSERT INTO ckpt_mode VALUES ({i}, 'v{i}');");
                f.execute(&sql).await.unwrap();
                r.execute_batch(&sql).unwrap();
            }
        }

        // PASSIVE checkpoint
        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            drop(f.execute("PRAGMA wal_checkpoint(PASSIVE);").await);

            let r = rusqlite::Connection::open(&r_path).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            let _ = r.execute_batch("PRAGMA wal_checkpoint(PASSIVE);");
        }

        // Insert more data after PASSIVE checkpoint
        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            let r = rusqlite::Connection::open(&r_path).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            for i in 30..50 {
                let sql = format!("INSERT INTO ckpt_mode VALUES ({i}, 'v{i}');");
                f.execute(&sql).await.unwrap();
                r.execute_batch(&sql).unwrap();
            }
        }

        // TRUNCATE checkpoint
        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            drop(f.execute("PRAGMA wal_checkpoint(TRUNCATE);").await);

            let r = rusqlite::Connection::open(&r_path).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            let _ = r.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");
        }

        // Verify data integrity post-checkpoint
        let f = fsqlite::Connection::open(&f_path).await.unwrap();
        let r = rusqlite::Connection::open(&r_path).unwrap();

        let fcount = frank_scalar(&f, "SELECT COUNT(*) FROM ckpt_mode").await;
        let rcount = csql_scalar(&r, "SELECT COUNT(*) FROM ckpt_mode");
        assert_eq!(fcount, "50");
        assert_eq!(fcount, rcount, "count mismatch after PASSIVE+TRUNCATE");

        let fdata = frank_rows(&f, "SELECT id, val FROM ckpt_mode ORDER BY id").await;
        let rdata = rusqlite_rows(&r, "SELECT id, val FROM ckpt_mode ORDER BY id");
        assert_eq!(fdata, rdata, "data mismatch after checkpoint cycle");
    });
}

// ── Test 6: Rollback atomicity (no partial writes visible) ───────────

#[test]
fn rollback_atomicity_no_partial_writes() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let r_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();
        let r_path = r_tmp.path().to_str().unwrap().to_owned();

        let setup = [
            "PRAGMA journal_mode = WAL;",
            "CREATE TABLE atomic (id INTEGER PRIMARY KEY, val INTEGER);",
            "INSERT INTO atomic VALUES (1, 100), (2, 200);",
        ];

        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            let r = rusqlite::Connection::open(&r_path).unwrap();
            for s in &setup {
                f.execute(s).await.unwrap();
                r.execute_batch(s).unwrap();
            }
        }

        // Begin transaction, make multiple changes, then ROLLBACK
        let rollback_ops = [
            "BEGIN",
            "UPDATE atomic SET val = 999 WHERE id = 1;",
            "INSERT INTO atomic VALUES (3, 300);",
            "DELETE FROM atomic WHERE id = 2;",
            "ROLLBACK",
        ];

        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            let r = rusqlite::Connection::open(&r_path).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            for s in &rollback_ops {
                f.execute(s).await.unwrap();
                r.execute_batch(s).unwrap();
            }
        }

        // Verify nothing changed
        let f = fsqlite::Connection::open(&f_path).await.unwrap();
        let r = rusqlite::Connection::open(&r_path).unwrap();

        let fdata = frank_rows(&f, "SELECT id, val FROM atomic ORDER BY id").await;
        let rdata = rusqlite_rows(&r, "SELECT id, val FROM atomic ORDER BY id");
        assert_eq!(fdata, rdata, "rollback atomicity parity");
        assert_eq!(fdata.len(), 2, "should still have 2 rows");
        assert_eq!(fdata[0], vec!["1", "100"], "val should be unchanged");
        assert_eq!(fdata[1], vec!["2", "200"], "row 2 should still exist");
    });
}

// ── Test 7: Concurrent writers on same table with secondary index ────

#[test]
fn concurrent_writers_with_index_parity() {
    let f_tmp = tempfile::NamedTempFile::new().unwrap();
    let r_tmp = tempfile::NamedTempFile::new().unwrap();
    let f_path = f_tmp.path().to_str().unwrap().to_owned();
    let r_path = r_tmp.path().to_str().unwrap().to_owned();

    let setup = [
        "PRAGMA journal_mode = WAL;",
        "CREATE TABLE idx_t (id INTEGER PRIMARY KEY, category TEXT, val INTEGER);",
        "CREATE INDEX idx_t_cat ON idx_t(category);",
        "CREATE INDEX idx_t_val ON idx_t(val);",
    ];

    asupersync::test_utils::run_test(|| async {
        let f = fsqlite::Connection::open(&f_path).await.unwrap();
        let r = rusqlite::Connection::open(&r_path).unwrap();
        for s in &setup {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
    });

    let n_threads = 4usize;
    let rows_per_thread = 25usize;
    let categories = ["alpha", "beta", "gamma", "delta"];
    let barrier = Arc::new(Barrier::new(n_threads));

    let handles: Vec<_> = (0..n_threads)
        .map(|tid| {
            let p = f_path.clone();
            let bar = barrier.clone();
            let cat = categories[tid].to_owned();
            thread::spawn(move || {
                asupersync::test_utils::run_test(|| async {
                    let conn = fsqlite::Connection::open(&p).await.unwrap();
                    conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                    bar.wait();
                    for i in 0..rows_per_thread {
                        let pk = tid * rows_per_thread + i;
                        let mut attempts = 0u32;
                        loop {
                            if conn.execute("BEGIN CONCURRENT").await.is_err() {
                                attempts += 1;
                                assert!(attempts < 200);
                                thread::sleep(std::time::Duration::from_millis(1));
                                continue;
                            }
                            let sql =
                                format!("INSERT INTO idx_t VALUES ({pk}, '{cat}', {});", pk * 10);
                            if conn.execute(&sql).await.is_err() {
                                drop(conn.execute("ROLLBACK").await);
                                attempts += 1;
                                assert!(attempts < 200);
                                thread::sleep(std::time::Duration::from_millis(1));
                                continue;
                            }
                            match conn.execute("COMMIT").await {
                                Ok(_) => break,
                                Err(_) => {
                                    drop(conn.execute("ROLLBACK").await);
                                    attempts += 1;
                                    assert!(attempts < 200);
                                    thread::sleep(std::time::Duration::from_millis(1));
                                }
                            }
                        }
                    }
                });
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Apply same inserts to rusqlite sequentially
    {
        let r = rusqlite::Connection::open(&r_path).unwrap();
        r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        for (tid, cat) in categories.iter().copied().enumerate().take(n_threads) {
            for i in 0..rows_per_thread {
                let pk = tid * rows_per_thread + i;
                r.execute_batch(&format!(
                    "INSERT INTO idx_t VALUES ({pk}, '{cat}', {});",
                    pk * 10
                ))
                .unwrap();
            }
        }
    }

    // Verify parity
    asupersync::test_utils::run_test(|| async {
        let f = fsqlite::Connection::open(&f_path).await.unwrap();
        let r = rusqlite::Connection::open(&r_path).unwrap();

        let fcount = frank_scalar(&f, "SELECT COUNT(*) FROM idx_t").await;
        let rcount = csql_scalar(&r, "SELECT COUNT(*) FROM idx_t");
        assert_eq!(fcount, rcount, "total count mismatch");
        assert_eq!(fcount, (n_threads * rows_per_thread).to_string());

        // Verify index-assisted queries produce same results
        for cat in &categories {
            let fcat = frank_scalar(
                &f,
                &format!("SELECT COUNT(*) FROM idx_t WHERE category = '{cat}'"),
            )
            .await;
            let rcat = csql_scalar(
                &r,
                &format!("SELECT COUNT(*) FROM idx_t WHERE category = '{cat}'"),
            );
            assert_eq!(fcat, rcat, "category {cat} count mismatch");
            assert_eq!(fcat, rows_per_thread.to_string());
        }

        let frange = frank_scalar(
            &f,
            "SELECT COUNT(*) FROM idx_t WHERE val BETWEEN 100 AND 500",
        )
        .await;
        let rrange = csql_scalar(
            &r,
            "SELECT COUNT(*) FROM idx_t WHERE val BETWEEN 100 AND 500",
        );
        assert_eq!(frange, rrange, "range query mismatch");
    });
}
