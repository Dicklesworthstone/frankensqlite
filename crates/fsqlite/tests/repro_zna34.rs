//! bd-zna34 minimal repro: Bad file descriptor in persistent concurrent WAL COMMIT
//!
//! The Criterion benchmark creates/destroys database files repeatedly via iter_batched.
//! The static GROUP_COMMIT_QUEUES persists across iterations. This test simulates that
//! pattern to reproduce the EBADF.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

const ROWS_PER_THREAD: i64 = 200;
const MAX_RETRIES: u32 = 100;

fn is_retryable(e: &fsqlite::FrankenError) -> bool {
    matches!(
        e,
        fsqlite::FrankenError::Busy
            | fsqlite::FrankenError::BusyRecovery
            | fsqlite::FrankenError::BusySnapshot { .. }
            | fsqlite::FrankenError::SerializationFailure { .. }
    )
}

fn run_one_iteration(n_threads: usize) -> Vec<String> {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_str().unwrap().to_owned();

    // Setup: create tables
    asupersync::test_utils::run_test(|| async {
        let setup = fsqlite::Connection::open(&path).await.unwrap();
        setup.execute("PRAGMA page_size = 4096;").await.unwrap();
        setup.execute("PRAGMA journal_mode = WAL;").await.unwrap();
        setup.execute("PRAGMA synchronous = NORMAL;").await.unwrap();
        setup.execute("PRAGMA cache_size = -64000;").await.unwrap();
        setup
            .execute("PRAGMA fsqlite.concurrent_mode = ON;")
            .await
            .unwrap();
        for tid in 0..n_threads {
            setup
                .execute(&format!(
                    "CREATE TABLE IF NOT EXISTS bench_{tid} (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);"
                ))
                .await
                .unwrap();
        }
    });

    let barrier = Arc::new(Barrier::new(n_threads));
    let errors: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let conflict_count = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..n_threads)
        .map(|tid| {
            let p = path.clone();
            let bar = barrier.clone();
            let errs = errors.clone();
            let conflicts = conflict_count.clone();
            thread::spawn(move || {
                asupersync::test_utils::run_test(|| async {
                let conn = fsqlite::Connection::open(&p).await.unwrap();
                conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                conn.execute("PRAGMA synchronous = NORMAL;").await.unwrap();
                conn.execute("PRAGMA cache_size = -64000;").await.unwrap();
                conn.execute("PRAGMA fsqlite.concurrent_mode = ON;").await.unwrap();
                let insert_sql = format!(
                    "INSERT INTO bench_{tid} VALUES (?1, ('t' || ?1), (?1 * 7));"
                );
                let stmt = conn.prepare(&insert_sql).await.unwrap();
                bar.wait();

                for i in 0..ROWS_PER_THREAD {
                    let mut retries = 0u32;
                    'txn: loop {
                        // BEGIN
                        loop {
                            match conn.execute("BEGIN CONCURRENT").await {
                                Ok(_) => break,
                                Err(e) if is_retryable(&e) => {
                                    conflicts.fetch_add(1, Ordering::Relaxed);
                                    retries += 1;
                                    if retries >= MAX_RETRIES {
                                        errs.lock().unwrap().push(format!(
                                            "[t{tid}] BEGIN failed after {MAX_RETRIES} retries: {e:?}"
                                        ));
                                        return;
                                    }
                                    thread::sleep(Duration::from_micros(
                                        100 * u64::from(retries),
                                    ));
                                }
                                Err(e) => {
                                    errs.lock().unwrap().push(format!(
                                        "[t{tid}] BEGIN non-retryable at row {i}: {e:?}"
                                    ));
                                    return;
                                }
                            }
                        }

                        // INSERT
                        if let Err(e) =
                            stmt.execute_with_params(&[fsqlite::SqliteValue::Integer(i)]).await
                        {
                            drop(conn.execute("ROLLBACK").await);
                            if !is_retryable(&e) {
                                let msg = format!("{e:?}");
                                if msg.contains("Bad file descriptor") {
                                    errs.lock().unwrap().push(format!(
                                        "[t{tid}] INSERT EBADF at row {i}"
                                    ));
                                    return;
                                }
                                // PK violation = row already committed by previous retry
                                if msg.contains("PRIMARY KEY") || msg.contains("UNIQUE") {
                                    break 'txn;
                                }
                                errs.lock().unwrap().push(format!(
                                    "[t{tid}] INSERT non-retryable at row {i}: {e:?}"
                                ));
                                return;
                            }
                            conflicts.fetch_add(1, Ordering::Relaxed);
                            retries += 1;
                            if retries >= MAX_RETRIES {
                                errs.lock().unwrap().push(format!(
                                    "[t{tid}] INSERT failed after {MAX_RETRIES} retries: {e:?}"
                                ));
                                return;
                            }
                            thread::sleep(Duration::from_micros(100 * u64::from(retries)));
                            continue 'txn;
                        }

                        // COMMIT
                        match conn.execute("COMMIT").await {
                            Ok(_) => break 'txn,
                            Err(e) if is_retryable(&e) => {
                                conflicts.fetch_add(1, Ordering::Relaxed);
                                drop(conn.execute("ROLLBACK").await);
                                retries += 1;
                                if retries >= MAX_RETRIES {
                                    errs.lock().unwrap().push(format!(
                                        "[t{tid}] COMMIT failed after {MAX_RETRIES}: {e:?}"
                                    ));
                                    return;
                                }
                                thread::sleep(Duration::from_micros(100 * u64::from(retries)));
                            }
                            Err(e) => {
                                let msg = format!("{e:?}");
                                errs.lock().unwrap().push(format!(
                                    "[t{tid}] COMMIT non-retryable at row {i}: {msg}"
                                ));
                                drop(conn.execute("ROLLBACK").await);
                                return;
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

    let errs = errors.lock().unwrap();
    if !errs.is_empty() {
        eprintln!(
            "  conflicts={}, errors={}",
            conflict_count.load(Ordering::Relaxed),
            errs.len()
        );
        for e in errs.iter() {
            eprintln!("  {e}");
        }
    }
    errs.clone()
}

/// Simulates Criterion's iter_batched: repeated create-use-destroy cycles.
/// The static GROUP_COMMIT_QUEUES accumulates across iterations.
#[test]
fn repro_bad_fd_persistent_concurrent_commit() {
    let mut total_errors = Vec::new();

    // Run 20 iterations (like Criterion's sample_size=10 with warmup)
    for iteration in 0..20 {
        eprintln!("iteration {iteration}...");
        let errs = run_one_iteration(4);
        if !errs.is_empty() {
            for e in &errs {
                total_errors.push(format!("iter {iteration}: {e}"));
            }
        }
    }

    if !total_errors.is_empty() {
        eprintln!("\n=== TOTAL ERRORS ===");
        for e in &total_errors {
            eprintln!("  {e}");
        }
        panic!(
            "bd-zna34 repro: {} errors across iterations",
            total_errors.len()
        );
    }
}

/// Stress variant: asymmetric close timing — thread 0 finishes fast and drops
/// connection (triggering passive checkpoint) while others still commit.
#[test]
fn repro_bad_fd_asymmetric_close_timing() {
    let mut total_errors = Vec::new();

    for iteration in 0..10 {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_owned();

        asupersync::test_utils::run_test(|| async {
            let setup = fsqlite::Connection::open(&path).await.unwrap();
            setup.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            setup.execute("PRAGMA synchronous = NORMAL;").await.unwrap();
            setup
                .execute("PRAGMA fsqlite.concurrent_mode = ON;")
                .await
                .unwrap();
            setup
                .execute("CREATE TABLE t0 (id INTEGER PRIMARY KEY);")
                .await
                .unwrap();
            setup
                .execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY);")
                .await
                .unwrap();
        });

        let barrier = Arc::new(Barrier::new(2));
        let error_msg: Arc<std::sync::Mutex<Option<String>>> =
            Arc::new(std::sync::Mutex::new(None));

        let p1 = path.clone();
        let bar1 = barrier.clone();
        let err1 = error_msg.clone();

        // Thread 0: does 5 rows then DROPS connection immediately
        let t0 = thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let conn = fsqlite::Connection::open(&p1).await.unwrap();
                conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                conn.execute("PRAGMA synchronous = NORMAL;").await.unwrap();
                conn.execute("PRAGMA fsqlite.concurrent_mode = ON;")
                    .await
                    .unwrap();
                bar1.wait();

                for i in 0..5_i64 {
                    drop(conn.execute("BEGIN CONCURRENT").await);
                    drop(conn.execute(&format!("INSERT INTO t0 VALUES ({i});")).await);
                    drop(conn.execute("COMMIT").await);
                }
                // Connection dropped here — triggers passive checkpoint
                drop(conn);
                eprintln!("[t0 iter {iteration}] connection dropped");
            });
        });

        let p2 = path;
        let bar2 = barrier;

        // Thread 1: does 500 rows — runs longer
        let t1 = thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let conn = fsqlite::Connection::open(&p2).await.unwrap();
                conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                conn.execute("PRAGMA synchronous = NORMAL;").await.unwrap();
                conn.execute("PRAGMA fsqlite.concurrent_mode = ON;")
                    .await
                    .unwrap();
                bar2.wait();

                for i in 0..500_i64 {
                    let mut retries = 0u32;
                    loop {
                        match conn.execute("BEGIN CONCURRENT").await {
                            Ok(_) => break,
                            Err(_) => {
                                retries += 1;
                                if retries > 50 {
                                    return;
                                }
                                thread::sleep(Duration::from_micros(200));
                            }
                        }
                    }
                    if conn
                        .execute(&format!("INSERT INTO t1 VALUES ({i});"))
                        .await
                        .is_err()
                    {
                        drop(conn.execute("ROLLBACK").await);
                        continue;
                    }
                    match conn.execute("COMMIT").await {
                        Ok(_) => {}
                        Err(e) => {
                            let msg = format!("{e:?}");
                            if msg.contains("Bad file descriptor") {
                                *err1.lock().unwrap() = Some(format!(
                                    "iter {iteration}: EBADF during COMMIT at row {i}"
                                ));
                                drop(conn.execute("ROLLBACK").await);
                                return;
                            }
                            drop(conn.execute("ROLLBACK").await);
                        }
                    }
                }
            });
        });

        t0.join().unwrap();
        t1.join().unwrap();

        let msg = error_msg.lock().unwrap().take();
        if let Some(msg) = msg {
            total_errors.push(msg);
        }
    }

    if !total_errors.is_empty() {
        for e in &total_errors {
            eprintln!("ERROR: {e}");
        }
        panic!("bd-zna34 asymmetric close: {} errors", total_errors.len());
    }
}
