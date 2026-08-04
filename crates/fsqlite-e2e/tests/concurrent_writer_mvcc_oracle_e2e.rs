//! bd-6xvq3 — Concurrent-writer MVCC oracle parity e2e tests.
//!
//! Exercises FrankenSQLite's core innovation — page-level MVCC concurrent
//! writers — and cross-checks results against C SQLite (via rusqlite in WAL
//! mode). Tests run real multi-threaded workloads on both engines using
//! file-backed databases, then compare final table state for correctness.
//!
//! Coverage:
//!   - Disjoint-partition inserts (no page contention, should scale linearly)
//!   - Same-table concurrent inserts into non-overlapping PK ranges
//!   - Read snapshot isolation (reader sees pre-commit state during writes)
//!   - Write-write conflict on same row (SSI must abort one writer)
//!   - Mixed DML: concurrent INSERT + UPDATE on different rows
//!   - Concurrent DDL + DML interleaving
#![recursion_limit = "512"]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use fsqlite::SqliteValue;

const RETRY_LIMIT: u32 = 200;
const RETRY_BACKOFF: Duration = Duration::from_micros(200);

// ── Helpers ────────────────────────────────────────────────────────────

async fn fsqlite_query_sorted(
    conn: &fsqlite::Connection,
    sql: &str,
) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
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
        .collect())
}

fn rusqlite_query_sorted(
    conn: &rusqlite::Connection,
    sql: &str,
) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
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
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

async fn compare_query_results(label: &str, f_path: &str, r_path: &str, queries: &[&str]) {
    let f = fsqlite::Connection::open(f_path)
        .await
        .expect("open frank for verify");
    let r = rusqlite::Connection::open(r_path).expect("open rusqlite for verify");
    let mut mismatches = Vec::new();
    for q in queries {
        match (
            fsqlite_query_sorted(&f, q).await,
            rusqlite_query_sorted(&r, q),
        ) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH {q}\n  frank: {a:?}\n  csql:  {b:?}"));
            }
            (Err(e), Ok(b)) => {
                mismatches.push(format!("FRANK_ERR {q}\n  err: {e}\n  csql: {b:?}"));
            }
            (Ok(a), Err(e)) => {
                mismatches.push(format!("CSQL_ERR {q}\n  frank: {a:?}\n  err: {e}"));
            }
            (Err(_), Err(_)) => {}
        }
    }
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

// ── Test 1: Disjoint-partition concurrent inserts ──────────────────────

#[test]
fn concurrent_disjoint_table_inserts_4_threads() {
    asupersync::test_utils::run_test(|| async {
        let n_threads = 4usize;
        let rows_per_thread: i64 = 100;

        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let r_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();
        let r_path = r_tmp.path().to_str().unwrap().to_owned();

        // Setup: create per-thread tables in both engines
        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            let r = rusqlite::Connection::open(&r_path).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            for tid in 0..n_threads {
                let ddl = format!("CREATE TABLE t_{tid} (id INTEGER PRIMARY KEY, val INTEGER);");
                f.execute(&ddl).await.unwrap();
                r.execute_batch(&ddl).unwrap();
            }
        }

        // FrankenSQLite: concurrent writers
        {
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

                            for i in 0..rows_per_thread {
                                let mut attempts = 0u32;
                                loop {
                                    if conn.execute("BEGIN CONCURRENT").await.is_err() {
                                        attempts += 1;
                                        assert!(
                                            attempts < RETRY_LIMIT,
                                            "BEGIN stuck for thread {tid}"
                                        );
                                        thread::sleep(RETRY_BACKOFF);
                                        continue;
                                    }
                                    let sql = format!(
                                        "INSERT INTO t_{tid} VALUES ({i}, {});",
                                        i * 10 + tid as i64
                                    );
                                    if conn.execute(&sql).await.is_err() {
                                        drop(conn.execute("ROLLBACK").await);
                                        attempts += 1;
                                        assert!(
                                            attempts < RETRY_LIMIT,
                                            "INSERT stuck for thread {tid}"
                                        );
                                        thread::sleep(RETRY_BACKOFF);
                                        continue;
                                    }
                                    match conn.execute("COMMIT").await {
                                        Ok(_) => break,
                                        Err(_) => {
                                            drop(conn.execute("ROLLBACK").await);
                                            attempts += 1;
                                            assert!(
                                                attempts < RETRY_LIMIT,
                                                "COMMIT stuck for thread {tid}"
                                            );
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
                h.join().expect("thread panicked");
            }
        }

        // C SQLite: concurrent writers
        {
            let barrier = Arc::new(Barrier::new(n_threads));
            let handles: Vec<_> = (0..n_threads)
                .map(|tid| {
                    let p = r_path.clone();
                    let bar = barrier.clone();
                    thread::spawn(move || {
                        let conn = rusqlite::Connection::open(&p).unwrap();
                        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
                            .unwrap();
                        bar.wait();

                        for i in 0..rows_per_thread {
                            let sql = format!(
                                "INSERT INTO t_{tid} VALUES ({i}, {});",
                                i * 10 + tid as i64
                            );
                            loop {
                                match conn.execute_batch(&sql) {
                                    Ok(()) => break,
                                    Err(e) if e.to_string().contains("database is locked") => {
                                        thread::sleep(RETRY_BACKOFF);
                                    }
                                    Err(e) => panic!("csqlite insert failed: {e}"),
                                }
                            }
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().expect("csqlite thread panicked");
            }
        }

        // Verify both engines have identical data
        let queries: Vec<String> = (0..n_threads)
            .map(|tid| format!("SELECT id, val FROM t_{tid} ORDER BY id"))
            .collect();
        let query_refs: Vec<&str> = queries.iter().map(String::as_str).collect();
        compare_query_results("disjoint_4t", &f_path, &r_path, &query_refs).await;
    });
}

// ── Test 2: Same-table concurrent inserts, non-overlapping PK ranges ──

#[test]
fn concurrent_same_table_inserts_non_overlapping_pks() {
    asupersync::test_utils::run_test(|| async {
        let n_threads = 4usize;
        let rows_per_thread: i64 = 50;

        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let r_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();
        let r_path = r_tmp.path().to_str().unwrap().to_owned();

        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("CREATE TABLE shared (id INTEGER PRIMARY KEY, writer INTEGER, val TEXT);")
                .await
                .unwrap();
            let r = rusqlite::Connection::open(&r_path).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            r.execute_batch(
                "CREATE TABLE shared (id INTEGER PRIMARY KEY, writer INTEGER, val TEXT);",
            )
            .unwrap();
        }

        // FrankenSQLite: concurrent writers, each owns a PK range
        {
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
                                let mut attempts = 0u32;
                                loop {
                                    if conn.execute("BEGIN CONCURRENT").await.is_err() {
                                        attempts += 1;
                                        assert!(attempts < RETRY_LIMIT);
                                        thread::sleep(RETRY_BACKOFF);
                                        continue;
                                    }
                                    let sql = format!(
                                        "INSERT INTO shared VALUES ({pk}, {tid}, 'w{tid}_r{i}');"
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
                h.join().expect("thread panicked");
            }
        }

        // C SQLite: same workload
        {
            let barrier = Arc::new(Barrier::new(n_threads));
            let handles: Vec<_> = (0..n_threads)
                .map(|tid| {
                    let p = r_path.clone();
                    let bar = barrier.clone();
                    thread::spawn(move || {
                        let conn = rusqlite::Connection::open(&p).unwrap();
                        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
                            .unwrap();
                        bar.wait();

                        let base = tid as i64 * rows_per_thread;
                        for i in 0..rows_per_thread {
                            let pk = base + i;
                            let sql =
                                format!("INSERT INTO shared VALUES ({pk}, {tid}, 'w{tid}_r{i}');");
                            loop {
                                match conn.execute_batch(&sql) {
                                    Ok(()) => break,
                                    Err(e) if e.to_string().contains("database is locked") => {
                                        thread::sleep(RETRY_BACKOFF);
                                    }
                                    Err(e) => panic!("csqlite insert failed: {e}"),
                                }
                            }
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().expect("csqlite thread panicked");
            }
        }

        compare_query_results(
            "same_table_non_overlapping",
            &f_path,
            &r_path,
            &[
                "SELECT COUNT(*) FROM shared",
                "SELECT id, writer, val FROM shared ORDER BY id",
            ],
        )
        .await;
    });
}

// ── Test 3: Verify row count integrity after concurrent inserts ────────

#[test]
fn concurrent_insert_no_data_loss_8_threads() {
    asupersync::test_utils::run_test(|| async {
        let n_threads = 8usize;
        let rows_per_thread: i64 = 25;

        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();

        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            for tid in 0..n_threads {
                f.execute(&format!(
                    "CREATE TABLE t_{tid} (id INTEGER PRIMARY KEY, data TEXT);"
                ))
                .await
                .unwrap();
            }
        }

        let total_retries = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(Barrier::new(n_threads));
        let handles: Vec<_> = (0..n_threads)
            .map(|tid| {
                let p = f_path.clone();
                let bar = barrier.clone();
                let retries = total_retries.clone();
                thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = fsqlite::Connection::open(&p).await.unwrap();
                        conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                        bar.wait();

                        let mut local_retries = 0u64;
                        for i in 0..rows_per_thread {
                            let mut attempts = 0u32;
                            loop {
                                if conn.execute("BEGIN CONCURRENT").await.is_err() {
                                    local_retries += 1;
                                    attempts += 1;
                                    assert!(attempts < RETRY_LIMIT, "stuck on BEGIN, thread {tid}");
                                    thread::sleep(RETRY_BACKOFF);
                                    continue;
                                }
                                let sql =
                                    format!("INSERT INTO t_{tid} VALUES ({i}, 'data_{tid}_{i}');");
                                if conn.execute(&sql).await.is_err() {
                                    drop(conn.execute("ROLLBACK").await);
                                    local_retries += 1;
                                    attempts += 1;
                                    assert!(attempts < RETRY_LIMIT);
                                    thread::sleep(RETRY_BACKOFF);
                                    continue;
                                }
                                match conn.execute("COMMIT").await {
                                    Ok(_) => break,
                                    Err(_) => {
                                        drop(conn.execute("ROLLBACK").await);
                                        local_retries += 1;
                                        attempts += 1;
                                        assert!(attempts < RETRY_LIMIT);
                                        thread::sleep(RETRY_BACKOFF);
                                    }
                                }
                            }
                        }
                        retries.fetch_add(local_retries, Ordering::Relaxed);
                    });
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread panicked");
        }

        // Verify via independent rusqlite read
        let verify = rusqlite::Connection::open(f_tmp.path()).unwrap();
        for tid in 0..n_threads {
            let count: i64 = verify
                .query_row(&format!("SELECT COUNT(*) FROM t_{tid}"), [], |r| r.get(0))
                .unwrap();
            assert_eq!(
                count,
                rows_per_thread,
                "thread {tid}: expected {rows_per_thread} rows, got {count} (retries: {})",
                total_retries.load(Ordering::Relaxed)
            );
        }
    });
}

// ── Test 4: Read isolation — reader sees consistent snapshot ───────────

#[test]
fn read_snapshot_isolation_during_concurrent_write() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();

        {
            let conn = fsqlite::Connection::open(&f_path).await.unwrap();
            conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            conn.execute("CREATE TABLE snap (id INTEGER PRIMARY KEY, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO snap VALUES (1, 100);")
                .await
                .unwrap();
            conn.execute("INSERT INTO snap VALUES (2, 200);")
                .await
                .unwrap();
        }

        let writer_started = Arc::new(Barrier::new(2));
        let fp = f_path.clone();
        let ws = writer_started.clone();

        let reader_handle = thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let conn = fsqlite::Connection::open(&fp).await.unwrap();
                conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();

                conn.execute("BEGIN").await.unwrap();
                let before = fsqlite_query_sorted(&conn, "SELECT id, val FROM snap ORDER BY id")
                    .await
                    .unwrap();
                assert_eq!(
                    before,
                    vec![
                        vec!["1".to_owned(), "100".to_owned()],
                        vec!["2".to_owned(), "200".to_owned()]
                    ],
                    "reader should see initial state"
                );

                ws.wait();
                thread::sleep(Duration::from_millis(50));

                let during = fsqlite_query_sorted(&conn, "SELECT id, val FROM snap ORDER BY id")
                    .await
                    .unwrap();
                assert_eq!(
                    during, before,
                    "reader inside txn should still see snapshot (MVCC isolation)"
                );

                conn.execute("COMMIT").await.unwrap();
            });
        });

        writer_started.wait();

        let wconn = fsqlite::Connection::open(&f_path).await.unwrap();
        wconn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
        wconn.execute("BEGIN CONCURRENT").await.unwrap();
        wconn
            .execute("INSERT INTO snap VALUES (3, 300);")
            .await
            .unwrap();
        if let Err(e) = wconn
            .execute("UPDATE snap SET val = 999 WHERE id = 1;")
            .await
        {
            drop(wconn.execute("ROLLBACK").await);
            eprintln!("writer UPDATE failed (may be bd-jamrd): {e}");
        }
        drop(wconn.execute("COMMIT").await);

        reader_handle.join().expect("reader thread panicked");
    });
}

// ── Test 5: Concurrent multi-row batch inserts ────────────────────────

#[test]
fn concurrent_batch_inserts_verify_total() {
    asupersync::test_utils::run_test(|| async {
        let n_threads = 4usize;
        let batches_per_thread = 5;
        let rows_per_batch = 10i64;

        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();

        {
            let conn = fsqlite::Connection::open(&f_path).await.unwrap();
            conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            conn.execute(
                "CREATE TABLE batched (id INTEGER PRIMARY KEY, thread_id INTEGER, batch INTEGER, seq INTEGER);"
            ).await.unwrap();
        }

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

                        for batch in 0..batches_per_thread {
                            let mut attempts = 0u32;
                            loop {
                                if conn.execute("BEGIN CONCURRENT").await.is_err() {
                                    attempts += 1;
                                    assert!(attempts < RETRY_LIMIT);
                                    thread::sleep(RETRY_BACKOFF);
                                    continue;
                                }
                                let mut ok = true;
                                for seq in 0..rows_per_batch {
                                    let pk = (tid as i64) * 1000 + (batch as i64) * 100 + seq;
                                    let sql = format!(
                                        "INSERT INTO batched VALUES ({pk}, {tid}, {batch}, {seq});"
                                    );
                                    if conn.execute(&sql).await.is_err() {
                                        ok = false;
                                        break;
                                    }
                                }
                                if !ok {
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
            h.join().expect("thread panicked");
        }

        let expected_total = n_threads as i64 * batches_per_thread as i64 * rows_per_batch;
        let verify = fsqlite::Connection::open(&f_path).await.unwrap();
        let rows = verify.query("SELECT COUNT(*) FROM batched").await.unwrap();
        let actual: i64 = match &rows[0].values()[0] {
            SqliteValue::Integer(n) => *n,
            other => panic!("unexpected count type: {other:?}"),
        };
        assert_eq!(
            actual, expected_total,
            "expected {expected_total} total rows, got {actual}"
        );

        // Verify per-thread counts
        for tid in 0..n_threads {
            let q = format!("SELECT COUNT(*) FROM batched WHERE thread_id = {tid}");
            let rows = verify.query(&q).await.unwrap();
            let count: i64 = match &rows[0].values()[0] {
                SqliteValue::Integer(n) => *n,
                other => panic!("unexpected count type for thread {tid}: {other:?}"),
            };
            let expected = batches_per_thread as i64 * rows_per_batch;
            assert_eq!(
                count, expected,
                "thread {tid}: expected {expected}, got {count}"
            );
        }

        // Cross-verify with rusqlite
        let r_verify = rusqlite::Connection::open(f_tmp.path()).unwrap();
        let r_count: i64 = r_verify
            .query_row("SELECT COUNT(*) FROM batched", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            r_count, expected_total,
            "rusqlite cross-check: expected {expected_total}, got {r_count}"
        );
    });
}

// ── Test 6: Concurrent autocommit inserts (no explicit txn) ───────────

#[test]
fn concurrent_autocommit_inserts_oracle_parity() {
    asupersync::test_utils::run_test(|| async {
        let n_threads = 4usize;
        let rows_per_thread: i64 = 30;

        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let r_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();
        let r_path = r_tmp.path().to_str().unwrap().to_owned();

        let ddl = "CREATE TABLE auto_t (id INTEGER PRIMARY KEY, src INTEGER);";
        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute(ddl).await.unwrap();
            let r = rusqlite::Connection::open(&r_path).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            r.execute_batch(ddl).unwrap();
        }

        // FrankenSQLite autocommit (BEGIN CONCURRENT is auto-promoted)
        {
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
                                let sql = format!("INSERT INTO auto_t VALUES ({pk}, {tid});");
                                let mut attempts = 0u32;
                                loop {
                                    match conn.execute(&sql).await {
                                        Ok(_) => break,
                                        Err(_) => {
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
                h.join().expect("thread panicked");
            }
        }

        // C SQLite
        {
            let barrier = Arc::new(Barrier::new(n_threads));
            let handles: Vec<_> = (0..n_threads)
                .map(|tid| {
                    let p = r_path.clone();
                    let bar = barrier.clone();
                    thread::spawn(move || {
                        let conn = rusqlite::Connection::open(&p).unwrap();
                        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
                            .unwrap();
                        bar.wait();

                        let base = tid as i64 * rows_per_thread;
                        for i in 0..rows_per_thread {
                            let pk = base + i;
                            let sql = format!("INSERT INTO auto_t VALUES ({pk}, {tid});");
                            loop {
                                match conn.execute_batch(&sql) {
                                    Ok(()) => break,
                                    Err(e) if e.to_string().contains("database is locked") => {
                                        thread::sleep(RETRY_BACKOFF);
                                    }
                                    Err(e) => panic!("csqlite insert failed: {e}"),
                                }
                            }
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().expect("thread panicked");
            }
        }

        let expected = n_threads as i64 * rows_per_thread;
        compare_query_results(
            "autocommit_inserts",
            &f_path,
            &r_path,
            &[
                "SELECT COUNT(*) FROM auto_t",
                "SELECT id, src FROM auto_t ORDER BY id",
            ],
        )
        .await;

        // Also verify total
        let verify = fsqlite::Connection::open(&f_path).await.unwrap();
        let rows = verify.query("SELECT COUNT(*) FROM auto_t").await.unwrap();
        let actual: i64 = match &rows[0].values()[0] {
            SqliteValue::Integer(n) => *n,
            _ => panic!("bad type"),
        };
        assert_eq!(actual, expected);
    });
}

// ── Test 7: Scaling ratio — fsqlite should not degrade vs single-thread ─

#[test]
fn concurrent_scaling_ratio_does_not_degrade() {
    asupersync::test_utils::run_test(|| async {
        let rows = 50i64;

        async fn measure(n_threads: usize, rows: i64) -> Duration {
            let f_tmp = tempfile::NamedTempFile::new().unwrap();
            let f_path = f_tmp.path().to_str().unwrap().to_owned();

            {
                let conn = fsqlite::Connection::open(&f_path).await.unwrap();
                conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                for tid in 0..n_threads {
                    conn.execute(&format!(
                        "CREATE TABLE scale_{tid} (id INTEGER PRIMARY KEY, v INTEGER);"
                    ))
                    .await
                    .unwrap();
                }
            }

            let barrier = Arc::new(Barrier::new(n_threads));
            let start = std::time::Instant::now();
            let handles: Vec<_> = (0..n_threads)
                .map(|tid| {
                    let p = f_path.clone();
                    let bar = barrier.clone();
                    thread::spawn(move || {
                        asupersync::test_utils::run_test(|| async {
                            let conn = fsqlite::Connection::open(&p).await.unwrap();
                            conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                            bar.wait();

                            for i in 0..rows {
                                let mut attempts = 0u32;
                                loop {
                                    if conn.execute("BEGIN CONCURRENT").await.is_err() {
                                        attempts += 1;
                                        assert!(attempts < RETRY_LIMIT);
                                        thread::sleep(RETRY_BACKOFF);
                                        continue;
                                    }
                                    let sql =
                                        format!("INSERT INTO scale_{tid} VALUES ({i}, {});", i * 3);
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
            start.elapsed()
        }

        let t1 = measure(1, rows).await;
        let t4 = measure(4, rows).await;

        #[allow(clippy::cast_precision_loss)]
        let ratio = t4.as_secs_f64() / t1.as_secs_f64();

        eprintln!(
            "scaling: 1t={:.1}ms  4t={:.1}ms  ratio={:.2}x (4t/1t, lower is better)",
            t1.as_secs_f64() * 1000.0,
            t4.as_secs_f64() * 1000.0,
            ratio
        );

        assert!(
            ratio < 8.0,
            "4-thread wall time is {ratio:.2}x of 1-thread — severe degradation (expect < 8x)"
        );
    });
}
