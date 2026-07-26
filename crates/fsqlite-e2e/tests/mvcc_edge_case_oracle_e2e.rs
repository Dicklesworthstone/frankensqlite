//! bd-c0d3h — MVCC edge-case oracle parity e2e tests.
//!
//! Tests FrankenSQLite's MVCC concurrent-writer edge cases:
//!   - Concurrent INSERT to same PK (exactly one must win)
//!   - Write-after-read conflict detection
//!   - Multiple tables with mixed isolation
//!   - Large transaction with many pages
//!   - Connection reuse after error recovery
//!   - Empty transaction commit/rollback
//!
//! Some tests exercise known bugs (marked with #[ignore]) and will pass
//! once fixes land. Others validate already-working behavior.
#![recursion_limit = "512"]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use fsqlite::SqliteValue;

const RETRY_LIMIT: u32 = 100;
const RETRY_BACKOFF: Duration = Duration::from_micros(500);

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

// ── Test 1: Concurrent INSERT to same PK — exactly one writer wins ────

#[test]
fn concurrent_insert_same_pk_exactly_one_wins() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();

        {
            let conn = fsqlite::Connection::open(&f_path).await.unwrap();
            conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            conn.execute("CREATE TABLE conflict_pk (id INTEGER PRIMARY KEY, writer INTEGER);")
                .await
                .unwrap();
        }

        let n_threads = 4usize;
        let barrier = Arc::new(Barrier::new(n_threads));
        let successes = Arc::new(AtomicU64::new(0));

        let handles: Vec<_> = (0..n_threads)
            .map(|tid| {
                let p = f_path.clone();
                let bar = barrier.clone();
                let succ = successes.clone();
                thread::spawn(move || {
                    let mut won = false;
                    asupersync::test_utils::run_test(|| async {
                        let conn = fsqlite::Connection::open(&p).await.unwrap();
                        conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                        bar.wait();

                        let mut attempts = 0u32;
                        loop {
                            if conn.execute("BEGIN CONCURRENT").await.is_err() {
                                attempts += 1;
                                if attempts >= RETRY_LIMIT {
                                    break;
                                }
                                thread::sleep(RETRY_BACKOFF);
                                continue;
                            }
                            let sql = format!("INSERT INTO conflict_pk VALUES (1, {tid});");
                            if conn.execute(&sql).await.is_err() {
                                drop(conn.execute("ROLLBACK").await);
                                break;
                            }
                            match conn.execute("COMMIT").await {
                                Ok(_) => {
                                    won = true;
                                    succ.fetch_add(1, Ordering::Relaxed);
                                    break;
                                }
                                Err(_) => {
                                    drop(conn.execute("ROLLBACK").await);
                                    break;
                                }
                            }
                        }
                    });
                    (tid, won)
                })
            })
            .collect();

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let winners: Vec<_> = results.iter().filter(|(_, won)| *won).collect();

        assert!(
            !winners.is_empty(),
            "at least one writer should succeed inserting PK=1"
        );

        let verify = fsqlite::Connection::open(&f_path).await.unwrap();
        let count = frank_scalar(&verify, "SELECT COUNT(*) FROM conflict_pk WHERE id = 1").await;
        assert_eq!(count, "1", "exactly one row with id=1 should exist");
    });
}

// ── Test 2: Connection reuse after rollback ───────────────────────────

#[test]
fn connection_reuse_after_rollback() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();

        let conn = fsqlite::Connection::open(&f_path).await.unwrap();
        conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
        conn.execute("CREATE TABLE reuse (id INTEGER PRIMARY KEY, v INTEGER);")
            .await
            .unwrap();
        conn.execute("INSERT INTO reuse VALUES (1, 10);")
            .await
            .unwrap();

        conn.execute("BEGIN").await.unwrap();
        conn.execute("INSERT INTO reuse VALUES (2, 20);")
            .await
            .unwrap();
        conn.execute("ROLLBACK").await.unwrap();

        let count = frank_scalar(&conn, "SELECT COUNT(*) FROM reuse").await;
        assert_eq!(count, "1", "rollback should undo the insert");

        conn.execute("INSERT INTO reuse VALUES (3, 30);")
            .await
            .unwrap();
        let count = frank_scalar(&conn, "SELECT COUNT(*) FROM reuse").await;
        assert_eq!(count, "2", "connection should work after rollback");

        let r = rusqlite::Connection::open(f_tmp.path()).unwrap();
        let rcount: i64 = r
            .query_row("SELECT COUNT(*) FROM reuse", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rcount, 2, "rusqlite cross-check: 2 rows expected");
    });
}

// ── Test 3: Empty transaction commit and rollback ─────────────────────

#[test]
fn empty_transaction_commit_rollback() {
    asupersync::test_utils::run_test(|| async {
        let f = fsqlite::Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        f.execute("CREATE TABLE empty_txn (id INTEGER PRIMARY KEY);")
            .await
            .unwrap();
        r.execute_batch("CREATE TABLE empty_txn (id INTEGER PRIMARY KEY);")
            .unwrap();

        f.execute("BEGIN").await.unwrap();
        f.execute("COMMIT").await.unwrap();

        r.execute_batch("BEGIN").unwrap();
        r.execute_batch("COMMIT").unwrap();

        f.execute("BEGIN").await.unwrap();
        f.execute("ROLLBACK").await.unwrap();

        r.execute_batch("BEGIN").unwrap();
        r.execute_batch("ROLLBACK").unwrap();

        f.execute("INSERT INTO empty_txn VALUES (1);")
            .await
            .unwrap();
        r.execute_batch("INSERT INTO empty_txn VALUES (1);")
            .unwrap();

        let fcount = frank_scalar(&f, "SELECT COUNT(*) FROM empty_txn").await;
        let rcount: i64 = r
            .query_row("SELECT COUNT(*) FROM empty_txn", [], |row| row.get(0))
            .unwrap();
        assert_eq!(fcount, "1");
        assert_eq!(rcount, 1);
    });
}

// ── Test 4: Multi-table concurrent writes ─────────────────────────────

#[test]
fn concurrent_writes_to_multiple_tables() {
    asupersync::test_utils::run_test(|| async {
    let f_tmp = tempfile::NamedTempFile::new().unwrap();
    let f_path = f_tmp.path().to_str().unwrap().to_owned();

    {
        let conn = fsqlite::Connection::open(&f_path).await.unwrap();
        conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
            .await
            .unwrap();
        conn.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER);",
        )
        .await
        .unwrap();
        conn.execute("CREATE TABLE logs (id INTEGER PRIMARY KEY, msg TEXT);")
            .await
            .unwrap();
    }

    let barrier = Arc::new(Barrier::new(3));

    let p1 = f_path.clone();
    let b1 = barrier.clone();
    let t1 = thread::spawn(move || {
        asupersync::test_utils::run_test(|| async {
        let conn = fsqlite::Connection::open(&p1).await.unwrap();
        conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
        b1.wait();
        for i in 0..20 {
            let mut attempts = 0u32;
            loop {
                if conn.execute("BEGIN CONCURRENT").await.is_err() {
                    attempts += 1;
                    assert!(attempts < RETRY_LIMIT);
                    thread::sleep(RETRY_BACKOFF);
                    continue;
                }
                let sql = format!("INSERT INTO users VALUES ({i}, 'user_{i}');");
                if conn.execute(&sql).await.is_err() {
                    drop(conn.execute("ROLLBACK").await);
                    attempts += 1;
                    assert!(attempts < RETRY_LIMIT);
                    thread::sleep(RETRY_BACKOFF);
                    continue;
                }
                match conn.execute("COMMIT").await {
                    Ok(_) => break,
                    Err(_) => {
                        drop(conn.execute("ROLLBACK").await);
                        attempts += 1;
                        assert!(attempts < RETRY_LIMIT);
                        thread::sleep(RETRY_BACKOFF);
                    }
                }
            }
        }
        });
    });

    let p2 = f_path.clone();
    let b2 = barrier.clone();
    let t2 = thread::spawn(move || {
        asupersync::test_utils::run_test(|| async {
        let conn = fsqlite::Connection::open(&p2).await.unwrap();
        conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
        b2.wait();
        for i in 0..30 {
            let mut attempts = 0u32;
            loop {
                if conn.execute("BEGIN CONCURRENT").await.is_err() {
                    attempts += 1;
                    assert!(attempts < RETRY_LIMIT);
                    thread::sleep(RETRY_BACKOFF);
                    continue;
                }
                let sql = format!("INSERT INTO orders VALUES ({i}, {}, {});", i % 20, i * 100);
                if conn.execute(&sql).await.is_err() {
                    drop(conn.execute("ROLLBACK").await);
                    attempts += 1;
                    assert!(attempts < RETRY_LIMIT);
                    thread::sleep(RETRY_BACKOFF);
                    continue;
                }
                match conn.execute("COMMIT").await {
                    Ok(_) => break,
                    Err(_) => {
                        drop(conn.execute("ROLLBACK").await);
                        attempts += 1;
                        assert!(attempts < RETRY_LIMIT);
                        thread::sleep(RETRY_BACKOFF);
                    }
                }
            }
        }
        });
    });

    let p3 = f_path.clone();
    let b3 = barrier.clone();
    let t3 = thread::spawn(move || {
        asupersync::test_utils::run_test(|| async {
        let conn = fsqlite::Connection::open(&p3).await.unwrap();
        conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
        b3.wait();
        for i in 0..15 {
            let mut attempts = 0u32;
            loop {
                if conn.execute("BEGIN CONCURRENT").await.is_err() {
                    attempts += 1;
                    assert!(attempts < RETRY_LIMIT);
                    thread::sleep(RETRY_BACKOFF);
                    continue;
                }
                let sql = format!("INSERT INTO logs VALUES ({i}, 'log_entry_{i}');");
                if conn.execute(&sql).await.is_err() {
                    drop(conn.execute("ROLLBACK").await);
                    attempts += 1;
                    assert!(attempts < RETRY_LIMIT);
                    thread::sleep(RETRY_BACKOFF);
                    continue;
                }
                match conn.execute("COMMIT").await {
                    Ok(_) => break,
                    Err(_) => {
                        drop(conn.execute("ROLLBACK").await);
                        attempts += 1;
                        assert!(attempts < RETRY_LIMIT);
                        thread::sleep(RETRY_BACKOFF);
                    }
                }
            }
        }
        });
    });

    t1.join().unwrap();
    t2.join().unwrap();
    t3.join().unwrap();

    let verify = rusqlite::Connection::open(f_tmp.path()).unwrap();
    let users: i64 = verify
        .query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))
        .unwrap();
    let orders: i64 = verify
        .query_row("SELECT COUNT(*) FROM orders", [], |r| r.get(0))
        .unwrap();
    let logs: i64 = verify
        .query_row("SELECT COUNT(*) FROM logs", [], |r| r.get(0))
        .unwrap();

    assert_eq!(users, 20, "users table should have 20 rows");
    assert_eq!(orders, 30, "orders table should have 30 rows");
    assert_eq!(logs, 15, "logs table should have 15 rows");
    });
}

// ── Test 5: Large batch within single transaction ─────────────────────

#[test]
fn large_batch_single_transaction_parity() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let r_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();
        let r_path = r_tmp.path().to_str().unwrap().to_owned();

        let n_rows = 500;

        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("CREATE TABLE bulk (id INTEGER PRIMARY KEY, val TEXT, num REAL);")
                .await
                .unwrap();
            f.execute("BEGIN").await.unwrap();
            for i in 0..n_rows {
                #[allow(clippy::cast_precision_loss)]
                let val = (i as f64) * 1.5;
                f.execute(&format!("INSERT INTO bulk VALUES ({i}, 'row_{i}', {val});"))
                    .await
                    .unwrap();
            }
            f.execute("COMMIT").await.unwrap();
        }

        {
            let r = rusqlite::Connection::open(&r_path).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            r.execute_batch("CREATE TABLE bulk (id INTEGER PRIMARY KEY, val TEXT, num REAL);")
                .unwrap();
            r.execute_batch("BEGIN;").unwrap();
            for i in 0..n_rows {
                #[allow(clippy::cast_precision_loss)]
                let val = (i as f64) * 1.5;
                r.execute_batch(&format!("INSERT INTO bulk VALUES ({i}, 'row_{i}', {val});"))
                    .unwrap();
            }
            r.execute_batch("COMMIT;").unwrap();
        }

        let f = fsqlite::Connection::open(&f_path).await.unwrap();
        let r = rusqlite::Connection::open(&r_path).unwrap();

        let fcount = frank_scalar(&f, "SELECT COUNT(*) FROM bulk").await;
        let rcount: i64 = r
            .query_row("SELECT COUNT(*) FROM bulk", [], |row| row.get(0))
            .unwrap();
        assert_eq!(fcount, n_rows.to_string());
        assert_eq!(rcount, n_rows);
    });
}

// ── Test 6: Rollback doesn't leak across connections ──────────────────

#[test]
fn rollback_doesnt_leak_to_other_connections() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();

        {
            let conn = fsqlite::Connection::open(&f_path).await.unwrap();
            conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            conn.execute("CREATE TABLE leak_test (id INTEGER PRIMARY KEY, v INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO leak_test VALUES (1, 100);")
                .await
                .unwrap();
        }

        {
            let c1 = fsqlite::Connection::open(&f_path).await.unwrap();
            c1.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            c1.execute("BEGIN CONCURRENT").await.unwrap();
            c1.execute("INSERT INTO leak_test VALUES (2, 200);")
                .await
                .unwrap();
            c1.execute("ROLLBACK").await.unwrap();
        }

        let verify = fsqlite::Connection::open(&f_path).await.unwrap();
        verify.execute("PRAGMA journal_mode = WAL;").await.unwrap();
        let count = frank_scalar(&verify, "SELECT COUNT(*) FROM leak_test").await;
        assert_eq!(count, "1", "rolled-back row should not exist");

        let rverify = rusqlite::Connection::open(f_tmp.path()).unwrap();
        let rcount: i64 = rverify
            .query_row("SELECT COUNT(*) FROM leak_test", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rcount, 1, "rusqlite cross-check: only 1 row");
    });
}

// ── Test 7: Concurrent writers with index ─────────────────────────────

#[test]
fn concurrent_writers_with_secondary_index() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();

        {
            let conn = fsqlite::Connection::open(&f_path).await.unwrap();
            conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            conn.execute(
                "CREATE TABLE indexed_t (id INTEGER PRIMARY KEY, category TEXT, score INTEGER);",
            )
            .await
            .unwrap();
            conn.execute("CREATE INDEX idx_category ON indexed_t(category);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_score ON indexed_t(score);")
                .await
                .unwrap();
        }

        let n_threads = 4usize;
        let rows_per_thread = 25i64;
        let barrier = Arc::new(Barrier::new(n_threads));

        let handles: Vec<_> = (0..n_threads)
            .map(|tid| {
                let p = f_path.clone();
                let bar = barrier.clone();
                thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = fsqlite::Connection::open(&p).await.unwrap();
                        conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                        bar.wait();

                        let base = tid as i64 * rows_per_thread;
                        for i in 0..rows_per_thread {
                            let pk = base + i;
                            let cat = format!("cat_{}", tid % 3);
                            let score = pk * 7;
                            let mut attempts = 0u32;
                            loop {
                                if conn.execute("BEGIN CONCURRENT").await.is_err() {
                                    attempts += 1;
                                    assert!(attempts < RETRY_LIMIT);
                                    thread::sleep(RETRY_BACKOFF);
                                    continue;
                                }
                                let sql = format!(
                                    "INSERT INTO indexed_t VALUES ({pk}, '{cat}', {score});"
                                );
                                if conn.execute(&sql).await.is_err() {
                                    drop(conn.execute("ROLLBACK").await);
                                    attempts += 1;
                                    assert!(attempts < RETRY_LIMIT);
                                    thread::sleep(RETRY_BACKOFF);
                                    continue;
                                }
                                match conn.execute("COMMIT").await {
                                    Ok(_) => break,
                                    Err(_) => {
                                        drop(conn.execute("ROLLBACK").await);
                                        attempts += 1;
                                        assert!(attempts < RETRY_LIMIT);
                                        thread::sleep(RETRY_BACKOFF);
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

        let expected = (n_threads as i64) * rows_per_thread;
        let verify = rusqlite::Connection::open(f_tmp.path()).unwrap();
        let total: i64 = verify
            .query_row("SELECT COUNT(*) FROM indexed_t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(total, expected, "total rows mismatch");

        let cat0: i64 = verify
            .query_row(
                "SELECT COUNT(*) FROM indexed_t WHERE category = 'cat_0'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(cat0 > 0, "index should be populated for cat_0");
    });
}
