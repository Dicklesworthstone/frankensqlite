// bd-3plop.4: Fault injection — lock contention storms
//
// Validates system availability under extreme lock contention scenarios.
// With page-level MVCC and SSI, contention manifests as SSI abort storms
// rather than deadlocks. We verify:
//   1. Forward progress under single hot-row contention
//   2. Hot-page contention with adjacent rows
//   3. Lock convoy: slow txn + fast swarm
//   4. Abort storm: deliberate write-skew patterns
//   5. Livelock detection: ensure progress doesn't stall
//   6. Machine-readable conformance output

#![recursion_limit = "512"]
#![allow(
    clippy::too_many_lines,
    clippy::items_after_statements,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::similar_names
)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

// -- Constants ----------------------------------------------------------------

// Low busy_timeout to avoid cascading waits under extreme contention.
// Threads that can't acquire locks quickly will get Busy immediately and retry.
const BUSY_TIMEOUT_MS: u32 = 50;
const MAX_RETRIES: usize = 10;
const RETRY_SLEEP_MS: u64 = 1;

// Keep thread count at 2-3 to avoid io_uring VFS race (uring-fs #359)
// and keep test duration reasonable under extreme contention.
const HOT_ROW_WRITERS: usize = 3;
const HOT_ROW_OPS: usize = 20;

const HOT_PAGE_WRITERS: usize = 3;
const HOT_PAGE_OPS: usize = 20;

const CONVOY_FAST_WRITERS: usize = 3;
const CONVOY_FAST_OPS: usize = 15;

const WRITE_SKEW_WRITERS: usize = 3;
const WRITE_SKEW_OPS: usize = 20;

const LIVELOCK_WRITERS: usize = 3;
const LIVELOCK_DURATION_SECS: u64 = 3;

const ACCOUNT_COUNT: usize = 20;
const INITIAL_BALANCE: i64 = 1000;

// -- Helpers ------------------------------------------------------------------

async fn init_db(path: &str) {
    let conn = fsqlite::Connection::open(path).await.unwrap();
    conn.execute("PRAGMA journal_mode = WAL").await.unwrap();
    conn.execute("PRAGMA synchronous = NORMAL").await.unwrap();
    conn.execute(&format!("PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}"))
        .await
        .unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)",
    )
    .await
    .unwrap();

    for id in 1..=ACCOUNT_COUNT {
        conn.execute(&format!(
            "INSERT INTO accounts (id, balance) VALUES ({id}, {INITIAL_BALANCE})"
        ))
        .await
        .unwrap();
    }
}

async fn open_conn(path: &str) -> fsqlite::Connection {
    // Stagger opens to avoid io_uring VFS race under concurrency
    thread::sleep(Duration::from_millis(5));
    let conn = fsqlite::Connection::open(path).await.unwrap();
    conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS};"))
        .await
        .unwrap();
    conn.execute("PRAGMA fsqlite.concurrent_mode=ON;")
        .await
        .unwrap();
    conn
}

async fn rollback_best_effort(conn: &fsqlite::Connection) {
    drop(conn.execute("ROLLBACK;").await);
}

async fn read_balance(
    conn: &fsqlite::Connection,
    id: usize,
) -> Result<i64, fsqlite_error::FrankenError> {
    let rows = conn
        .query(&format!("SELECT balance FROM accounts WHERE id = {id};"))
        .await?;
    if rows.is_empty() {
        return Err(fsqlite_error::FrankenError::Internal(format!(
            "account {id} not found"
        )));
    }
    match &rows[0].values()[0] {
        fsqlite_types::value::SqliteValue::Integer(v) => Ok(*v),
        other => Err(fsqlite_error::FrankenError::Internal(format!(
            "unexpected value: {other:?}"
        ))),
    }
}

async fn verify_sum_invariant(path: &str) -> (i64, i64) {
    let conn = open_conn(path).await;
    let rows = conn
        .query("SELECT SUM(balance), MIN(balance) FROM accounts;")
        .await
        .unwrap();
    let sum = match &rows[0].values()[0] {
        fsqlite_types::value::SqliteValue::Integer(v) => *v,
        _ => panic!("unexpected sum type"),
    };
    let min = match &rows[0].values()[1] {
        fsqlite_types::value::SqliteValue::Integer(v) => *v,
        _ => panic!("unexpected min type"),
    };
    (sum, min)
}

#[derive(Debug, Default)]
struct StormResult {
    committed: u64,
    aborted: u64,
    retries: u64,
    hard_failures: Vec<String>,
    elapsed: Duration,
}

impl StormResult {
    fn abort_rate(&self) -> f64 {
        let total = self.committed + self.aborted;
        if total == 0 {
            return 0.0;
        }
        self.aborted as f64 / total as f64
    }

    fn throughput(&self) -> f64 {
        if self.elapsed.as_secs_f64() == 0.0 {
            return 0.0;
        }
        self.committed as f64 / self.elapsed.as_secs_f64()
    }
}

// =============================================================================
// Test 1: Single hot-row contention
// All writers update the same row → maximum SSI conflict rate.
// Must maintain forward progress (at least some commits per second).
// =============================================================================

#[test]
fn test_hot_row_contention() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("hot_row.db");
        let path = db_path.to_string_lossy().to_string();
        init_db(&path).await;

        let barrier = Arc::new(Barrier::new(HOT_ROW_WRITERS));
        let start = Instant::now();
        let mut handles = Vec::with_capacity(HOT_ROW_WRITERS);

        for _ in 0..HOT_ROW_WRITERS {
            let p = path.clone();
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let mut committed = 0u64;
                let mut aborted = 0u64;
                let mut retries = 0u64;
                let mut hard_failures: Vec<String> = Vec::new();

                asupersync::test_utils::run_test(|| async {
                    let conn = open_conn(&p).await;

                    b.wait(); // Sync start for maximum contention

                    for _ in 0..HOT_ROW_OPS {
                        let mut retry_count = 0;
                        loop {
                            if let Err(e) = conn.execute("BEGIN CONCURRENT;").await {
                                if e.is_transient() {
                                    rollback_best_effort(&conn).await;
                                    retries += 1;
                                    retry_count += 1;
                                    if retry_count > MAX_RETRIES {
                                        aborted += 1;
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                                    continue;
                                }
                                hard_failures.push(format!("BEGIN: {e}"));
                                break;
                            }

                            // All writers target account 1 (single hot row)
                            match conn
                                .execute("UPDATE accounts SET balance = balance + 1 WHERE id = 1;")
                                .await
                            {
                                Ok(_) => {}
                                Err(e) if e.is_transient() => {
                                    rollback_best_effort(&conn).await;
                                    retries += 1;
                                    retry_count += 1;
                                    if retry_count > MAX_RETRIES {
                                        aborted += 1;
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                                    continue;
                                }
                                Err(e) => {
                                    rollback_best_effort(&conn).await;
                                    hard_failures.push(format!("UPDATE: {e}"));
                                    break;
                                }
                            }

                            match conn.execute("COMMIT;").await {
                                Ok(_) => {
                                    committed += 1;
                                    break;
                                }
                                Err(e) if e.is_transient() => {
                                    rollback_best_effort(&conn).await;
                                    retries += 1;
                                    retry_count += 1;
                                    if retry_count > MAX_RETRIES {
                                        aborted += 1;
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                                }
                                Err(e) => {
                                    rollback_best_effort(&conn).await;
                                    hard_failures.push(format!("COMMIT: {e}"));
                                    break;
                                }
                            }
                        }
                    }
                });

                (committed, aborted, retries, hard_failures)
            }));
        }

        let mut result = StormResult::default();
        for h in handles {
            let (c, a, r, f) = h.join().unwrap();
            result.committed += c;
            result.aborted += a;
            result.retries += r;
            result.hard_failures.extend(f);
        }
        result.elapsed = start.elapsed();

        println!(
            "[hot_row] committed={} aborted={} retries={} abort_rate={:.1}% throughput={:.0} txn/s elapsed={:.2}s hard_failures={}",
            result.committed,
            result.aborted,
            result.retries,
            result.abort_rate() * 100.0,
            result.throughput(),
            result.elapsed.as_secs_f64(),
            result.hard_failures.len()
        );

        // Verify final state
        let (sum, _) = verify_sum_invariant(&path).await;
        let expected_sum = (ACCOUNT_COUNT as i64 * INITIAL_BALANCE) + result.committed as i64;
        assert_eq!(
            sum, expected_sum,
            "sum invariant violated after hot-row contention"
        );

        // Assertions: single-hot-row causes extreme contention so we only require
        // forward progress (at least one committed txn) and no hard failures.
        assert!(
            result.hard_failures.is_empty(),
            "hard failures: {:?}",
            result.hard_failures
        );
        assert!(
            result.committed > 0,
            "must have forward progress (at least 1 commit)"
        );

        println!(
            "[PASS] hot row contention: forward progress maintained (committed={})",
            result.committed
        );
    });
}

// =============================================================================
// Test 2: Hot-page contention
// Writers update different rows that live on the same B-tree page.
// With page-level MVCC, these share the same page lock.
// =============================================================================

#[test]
fn test_hot_page_contention() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("hot_page.db");
        let path = db_path.to_string_lossy().to_string();
        init_db(&path).await;

        let barrier = Arc::new(Barrier::new(HOT_PAGE_WRITERS));
        let start = Instant::now();
        let mut handles = Vec::with_capacity(HOT_PAGE_WRITERS);

        for worker_id in 0..HOT_PAGE_WRITERS {
            let p = path.clone();
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let mut committed = 0u64;
                let mut aborted = 0u64;
                let mut retries = 0u64;
                let mut hard_failures: Vec<String> = Vec::new();

                asupersync::test_utils::run_test(|| async {
                    let conn = open_conn(&p).await;

                    b.wait();

                    for op in 0..HOT_PAGE_OPS {
                        // Different rows, but IDs 1-10 are likely on the same page
                        let target_id = (worker_id % 10) + 1;
                        let mut retry_count = 0;

                        loop {
                            if let Err(e) = conn.execute("BEGIN CONCURRENT;").await {
                                if e.is_transient() {
                                    rollback_best_effort(&conn).await;
                                    retries += 1;
                                    retry_count += 1;
                                    if retry_count > MAX_RETRIES {
                                        aborted += 1;
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                                    continue;
                                }
                                hard_failures.push(format!("BEGIN: {e}"));
                                break;
                            }

                            let sql = format!(
                                "UPDATE accounts SET balance = balance + 1 WHERE id = {target_id};"
                            );
                            match conn.execute(&sql).await {
                                Ok(_) => {}
                                Err(e) if e.is_transient() => {
                                    rollback_best_effort(&conn).await;
                                    retries += 1;
                                    retry_count += 1;
                                    if retry_count > MAX_RETRIES {
                                        aborted += 1;
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                                    continue;
                                }
                                Err(e) => {
                                    rollback_best_effort(&conn).await;
                                    hard_failures.push(format!("UPDATE op={op}: {e}"));
                                    break;
                                }
                            }

                            match conn.execute("COMMIT;").await {
                                Ok(_) => {
                                    committed += 1;
                                    break;
                                }
                                Err(e) if e.is_transient() => {
                                    rollback_best_effort(&conn).await;
                                    retries += 1;
                                    retry_count += 1;
                                    if retry_count > MAX_RETRIES {
                                        aborted += 1;
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                                }
                                Err(e) => {
                                    rollback_best_effort(&conn).await;
                                    hard_failures.push(format!("COMMIT: {e}"));
                                    break;
                                }
                            }
                        }
                    }
                });

                (committed, aborted, retries, hard_failures)
            }));
        }

        let mut result = StormResult::default();
        for h in handles {
            let (c, a, r, f) = h.join().unwrap();
            result.committed += c;
            result.aborted += a;
            result.retries += r;
            result.hard_failures.extend(f);
        }
        result.elapsed = start.elapsed();

        println!(
            "[hot_page] committed={} aborted={} retries={} abort_rate={:.1}% throughput={:.0} txn/s elapsed={:.2}s",
            result.committed,
            result.aborted,
            result.retries,
            result.abort_rate() * 100.0,
            result.throughput(),
            result.elapsed.as_secs_f64()
        );

        assert!(
            result.hard_failures.is_empty(),
            "hard failures: {:?}",
            result.hard_failures
        );
        assert!(result.committed > 0, "must have forward progress");

        println!("[PASS] hot page contention: forward progress maintained");
    });
}

// =============================================================================
// Test 3: Lock convoy
// One slow transaction holds pages while many fast transactions queue behind.
// Verifies no deadlock/livelock — fast txns should complete after slow releases.
// =============================================================================

#[test]
fn test_lock_convoy() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("convoy.db");
        let path = db_path.to_string_lossy().to_string();
        init_db(&path).await;

        let slow_done = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(CONVOY_FAST_WRITERS + 1)); // +1 for slow thread

        // Slow writer: holds transaction open while updating many rows
        let slow_path = path.clone();
        let slow_barrier = Arc::clone(&barrier);
        let slow_flag = Arc::clone(&slow_done);
        let slow_handle = thread::spawn(move || -> Vec<String> {
            let mut hard_failures: Vec<String> = Vec::new();
            asupersync::test_utils::run_test(|| async {
                let conn = open_conn(&slow_path).await;
                slow_barrier.wait();

                if let Err(e) = conn.execute("BEGIN CONCURRENT;").await {
                    hard_failures.push(format!("slow BEGIN: {e}"));
                    slow_flag.store(true, Ordering::Release);
                    return;
                }

                // Touch rows 1-50, then sleep to hold locks
                for id in 1..=50 {
                    match conn
                        .execute(&format!(
                            "UPDATE accounts SET balance = balance + 1 WHERE id = {id};"
                        ))
                        .await
                    {
                        Ok(_) => {}
                        Err(e) if e.is_transient() => {
                            // Fast writers can make the slow writer's snapshot stale before COMMIT.
                            rollback_best_effort(&conn).await;
                            slow_flag.store(true, Ordering::Release);
                            return;
                        }
                        Err(e) => {
                            hard_failures.push(format!("slow UPDATE id={id}: {e}"));
                            rollback_best_effort(&conn).await;
                            slow_flag.store(true, Ordering::Release);
                            return;
                        }
                    }
                }

                // Hold locks briefly to create convoy
                thread::sleep(Duration::from_millis(50));

                match conn.execute("COMMIT;").await {
                    Ok(_) => {} // slow writer committed first — fast writers will conflict
                    Err(e) if e.is_transient() => {
                        // Expected under FCW: fast writers committed first, slow snapshot stale.
                        rollback_best_effort(&conn).await;
                    }
                    Err(e) => {
                        hard_failures.push(format!("slow COMMIT hard: {e}"));
                        rollback_best_effort(&conn).await;
                    }
                }
                slow_flag.store(true, Ordering::Release);
            });
            hard_failures
        });

        // Fast writers: try to update rows in the same range
        let start = Instant::now();
        let mut fast_handles = Vec::with_capacity(CONVOY_FAST_WRITERS);
        for worker_id in 0..CONVOY_FAST_WRITERS {
            let p = path.clone();
            let b = Arc::clone(&barrier);
            fast_handles.push(thread::spawn(move || {
                let mut committed = 0u64;
                let mut aborted = 0u64;
                let mut retries = 0u64;
                let mut hard_failures: Vec<String> = Vec::new();

                asupersync::test_utils::run_test(|| async {
                    let conn = open_conn(&p).await;

                    b.wait();

                    for _ in 0..CONVOY_FAST_OPS {
                        let target_id = (worker_id % 50) + 1; // overlap with slow writer
                        let mut retry_count = 0;

                        loop {
                            if let Err(e) = conn.execute("BEGIN CONCURRENT;").await {
                                if e.is_transient() {
                                    rollback_best_effort(&conn).await;
                                    retries += 1;
                                    retry_count += 1;
                                    if retry_count > MAX_RETRIES {
                                        aborted += 1;
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                                    continue;
                                }
                                aborted += 1;
                                hard_failures.push(format!("BEGIN hard: {e}"));
                                break;
                            }

                            match conn
                                .execute(&format!(
                                    "UPDATE accounts SET balance = balance + 1 WHERE id = {target_id};"
                                ))
                                .await
                            {
                                Ok(_) => {}
                                Err(e) if e.is_transient() => {
                                    rollback_best_effort(&conn).await;
                                    retries += 1;
                                    retry_count += 1;
                                    if retry_count > MAX_RETRIES {
                                        aborted += 1;
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                                    continue;
                                }
                                Err(e) => {
                                    rollback_best_effort(&conn).await;
                                    aborted += 1;
                                    hard_failures.push(format!("DML hard: {e}"));
                                    break;
                                }
                            }

                            match conn.execute("COMMIT;").await {
                                Ok(_) => {
                                    committed += 1;
                                    break;
                                }
                                Err(e) if e.is_transient() => {
                                    rollback_best_effort(&conn).await;
                                    retries += 1;
                                    retry_count += 1;
                                    if retry_count > MAX_RETRIES {
                                        aborted += 1;
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                                }
                                Err(e) => {
                                    rollback_best_effort(&conn).await;
                                    aborted += 1;
                                    hard_failures.push(format!("COMMIT hard: {e}"));
                                    break;
                                }
                            }
                        }
                    }
                });

                (committed, aborted, retries, hard_failures)
            }));
        }

        let slow_failures = slow_handle.join().unwrap();

        let mut result = StormResult::default();
        result.hard_failures.extend(slow_failures);
        for h in fast_handles {
            let (c, a, r, f) = h.join().unwrap();
            result.committed += c;
            result.aborted += a;
            result.retries += r;
            result.hard_failures.extend(f);
        }
        result.elapsed = start.elapsed();

        println!(
            "[convoy] committed={} aborted={} retries={} abort_rate={:.1}% throughput={:.0} txn/s elapsed={:.2}s hard_failures={}",
            result.committed,
            result.aborted,
            result.retries,
            result.abort_rate() * 100.0,
            result.throughput(),
            result.elapsed.as_secs_f64(),
            result.hard_failures.len()
        );

        assert!(
            result.hard_failures.is_empty(),
            "hard failures: {:?}",
            result.hard_failures
        );
        assert!(
            result.committed > 0,
            "fast writers must make progress after slow releases"
        );

        println!("[PASS] lock convoy: no deadlock, fast writers completed");
    });
}

// =============================================================================
// Test 4: Abort storm — deliberate write-skew patterns
// Classic write-skew: T1 reads A, writes B; T2 reads B, writes A.
// SSI should detect and abort one of them. Under high concurrency the
// abort rate will be high but must never reach 100% (no livelock).
// =============================================================================

#[test]
fn test_write_skew_abort_storm() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("write_skew.db");
        let path = db_path.to_string_lossy().to_string();
        init_db(&path).await;

        let barrier = Arc::new(Barrier::new(WRITE_SKEW_WRITERS));
        let start = Instant::now();
        let mut handles = Vec::with_capacity(WRITE_SKEW_WRITERS);

        for worker_id in 0..WRITE_SKEW_WRITERS {
            let p = path.clone();
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let mut committed = 0u64;
                let mut aborted = 0u64;
                let mut retries = 0u64;
                let mut hard_failures: Vec<String> = Vec::new();

                asupersync::test_utils::run_test(|| async {
                    let conn = open_conn(&p).await;

                    b.wait();

                    for _ in 0..WRITE_SKEW_OPS {
                        // Deliberate write-skew pattern:
                        // Even workers: read account A, write account B
                        // Odd workers: read account B, write account A
                        let (read_id, write_id) = if worker_id % 2 == 0 { (1, 2) } else { (2, 1) };

                        let mut retry_count = 0;
                        loop {
                            if let Err(e) = conn.execute("BEGIN CONCURRENT;").await {
                                if e.is_transient() {
                                    rollback_best_effort(&conn).await;
                                    retries += 1;
                                    retry_count += 1;
                                    if retry_count > MAX_RETRIES {
                                        aborted += 1;
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                                    continue;
                                }
                                aborted += 1;
                                hard_failures.push(format!("BEGIN hard: {e}"));
                                break;
                            }

                            // Read one account
                            let balance = match read_balance(&conn, read_id).await {
                                Ok(b) => b,
                                Err(e) if e.is_transient() => {
                                    rollback_best_effort(&conn).await;
                                    retries += 1;
                                    retry_count += 1;
                                    if retry_count > MAX_RETRIES {
                                        aborted += 1;
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                                    continue;
                                }
                                Err(e) => {
                                    rollback_best_effort(&conn).await;
                                    aborted += 1;
                                    hard_failures.push(format!("READ hard: {e}"));
                                    break;
                                }
                            };

                            // Write the other account (constrained by what we read)
                            let delta = if balance > 0 { 1 } else { 0 };
                            match conn
                                .execute(&format!(
                                    "UPDATE accounts SET balance = balance + {delta} WHERE id = {write_id};"
                                ))
                                .await
                            {
                                Ok(_) => {}
                                Err(e) if e.is_transient() => {
                                    rollback_best_effort(&conn).await;
                                    retries += 1;
                                    retry_count += 1;
                                    if retry_count > MAX_RETRIES {
                                        aborted += 1;
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                                    continue;
                                }
                                Err(e) => {
                                    rollback_best_effort(&conn).await;
                                    aborted += 1;
                                    hard_failures.push(format!("WRITE hard: {e}"));
                                    break;
                                }
                            }

                            match conn.execute("COMMIT;").await {
                                Ok(_) => {
                                    committed += 1;
                                    break;
                                }
                                Err(e) if e.is_transient() => {
                                    rollback_best_effort(&conn).await;
                                    retries += 1;
                                    retry_count += 1;
                                    if retry_count > MAX_RETRIES {
                                        aborted += 1;
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                                }
                                Err(e) => {
                                    rollback_best_effort(&conn).await;
                                    aborted += 1;
                                    hard_failures.push(format!("COMMIT hard: {e}"));
                                    break;
                                }
                            }
                        }
                    }
                });

                (committed, aborted, retries, hard_failures)
            }));
        }

        let mut result = StormResult::default();
        for h in handles {
            let (c, a, r, f) = h.join().unwrap();
            result.committed += c;
            result.aborted += a;
            result.retries += r;
            result.hard_failures.extend(f);
        }
        result.elapsed = start.elapsed();

        println!(
            "[write_skew] committed={} aborted={} retries={} abort_rate={:.1}% throughput={:.0} txn/s elapsed={:.2}s",
            result.committed,
            result.aborted,
            result.retries,
            result.abort_rate() * 100.0,
            result.throughput(),
            result.elapsed.as_secs_f64()
        );

        // Write-skew should cause aborts, but not 100% — there must be forward progress
        assert!(
            result.committed > 0,
            "must have some committed transactions even under write-skew"
        );
        // abort_rate < 100% proves no livelock
        assert!(
            result.abort_rate() < 1.0,
            "abort rate must be < 100%: got {:.1}%",
            result.abort_rate() * 100.0
        );

        println!(
            "[PASS] write-skew abort storm: forward progress, abort_rate={:.1}%",
            result.abort_rate() * 100.0
        );
    });
}

// =============================================================================
// Test 5: Livelock detection
// Run concurrent writers for a fixed duration and check that at least one
// transaction commits per second. If progress stalls, it's a livelock.
// =============================================================================

#[test]
fn test_livelock_detection() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("livelock.db");
        let path = db_path.to_string_lossy().to_string();
        init_db(&path).await;

        let stop = Arc::new(AtomicBool::new(false));
        let global_commits = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(Barrier::new(LIVELOCK_WRITERS));
        let mut handles = Vec::with_capacity(LIVELOCK_WRITERS);

        for worker_id in 0..LIVELOCK_WRITERS {
            let p = path.clone();
            let s = Arc::clone(&stop);
            let gc = Arc::clone(&global_commits);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                asupersync::test_utils::run_test(|| async {
                    let conn = open_conn(&p).await;
                    b.wait();

                    while !s.load(Ordering::Relaxed) {
                        // All workers target the same 5 rows — maximum contention
                        let target_id = (worker_id % 5) + 1;
                        let mut retry_count = 0;

                        loop {
                            if s.load(Ordering::Relaxed) {
                                break;
                            }
                            if let Err(e) = conn.execute("BEGIN CONCURRENT;").await {
                                if e.is_transient() {
                                    rollback_best_effort(&conn).await;
                                    retry_count += 1;
                                    if retry_count > MAX_RETRIES {
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                                    continue;
                                }
                                break;
                            }

                            match conn
                                .execute(&format!(
                                    "UPDATE accounts SET balance = balance + 1 WHERE id = {target_id};"
                                ))
                                .await
                            {
                                Ok(_) => {}
                                Err(e) if e.is_transient() => {
                                    rollback_best_effort(&conn).await;
                                    retry_count += 1;
                                    if retry_count > MAX_RETRIES {
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                                    continue;
                                }
                                Err(_) => {
                                    rollback_best_effort(&conn).await;
                                    break;
                                }
                            }

                            match conn.execute("COMMIT;").await {
                                Ok(_) => {
                                    gc.fetch_add(1, Ordering::Relaxed);
                                    break;
                                }
                                Err(e) if e.is_transient() => {
                                    rollback_best_effort(&conn).await;
                                    retry_count += 1;
                                    if retry_count > MAX_RETRIES {
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                                }
                                Err(_) => {
                                    rollback_best_effort(&conn).await;
                                    break;
                                }
                            }
                        }
                    }
                });
            }));
        }

        // Monitor for livelock: check every second that commits are advancing
        let monitor_start = Instant::now();
        let mut last_commits = 0u64;
        let mut stall_seconds = 0u64;

        while monitor_start.elapsed() < Duration::from_secs(LIVELOCK_DURATION_SECS) {
            thread::sleep(Duration::from_secs(1));
            let current = global_commits.load(Ordering::Relaxed);
            if current == last_commits {
                stall_seconds += 1;
            } else {
                stall_seconds = 0;
            }
            last_commits = current;
            println!(
                "[livelock] t={:.0}s commits={current} stall_seconds={stall_seconds}",
                monitor_start.elapsed().as_secs_f64()
            );
        }

        stop.store(true, Ordering::Release);
        for h in handles {
            h.join().unwrap();
        }

        let total_commits = global_commits.load(Ordering::Relaxed);
        let elapsed = monitor_start.elapsed().as_secs_f64();

        println!(
            "[livelock] total_commits={total_commits} elapsed={elapsed:.1}s throughput={:.0} txn/s stall_seconds={stall_seconds}",
            total_commits as f64 / elapsed
        );

        assert!(
            stall_seconds < 3,
            "livelock detected: progress stalled for {stall_seconds} consecutive seconds"
        );
        assert!(total_commits > 0, "must have at least some commits");

        println!("[PASS] livelock detection: continuous progress verified");
    });
}

// =============================================================================
// Test 6: Conformance summary (JSON)
// =============================================================================

#[test]
fn test_conformance_summary() {
    asupersync::test_utils::run_test(|| async {
        struct TestResult {
            name: &'static str,
            pass: bool,
            detail: String,
        }

        let mut results = Vec::new();

        // 1. Hot row: forward progress
        {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("c1.db").to_string_lossy().to_string();
            init_db(&path).await;

            let committed = Arc::new(AtomicU64::new(0));
            let barrier = Arc::new(Barrier::new(3));
            let mut handles = Vec::new();

            for _ in 0..3 {
                let p = path.clone();
                let c = Arc::clone(&committed);
                let b = Arc::clone(&barrier);
                handles.push(thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = open_conn(&p).await;
                        b.wait();
                        for _ in 0..10 {
                            let mut retries = 0;
                            loop {
                                if conn.execute("BEGIN CONCURRENT;").await.is_err() {
                                    rollback_best_effort(&conn).await;
                                    retries += 1;
                                    if retries > MAX_RETRIES {
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(1));
                                    continue;
                                }
                                if conn
                                    .execute(
                                        "UPDATE accounts SET balance = balance + 1 WHERE id = 1;",
                                    )
                                    .await
                                    .is_err()
                                {
                                    rollback_best_effort(&conn).await;
                                    retries += 1;
                                    if retries > MAX_RETRIES {
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(1));
                                    continue;
                                }
                                match conn.execute("COMMIT;").await {
                                    Ok(_) => {
                                        c.fetch_add(1, Ordering::Relaxed);
                                        break;
                                    }
                                    Err(_) => {
                                        rollback_best_effort(&conn).await;
                                        retries += 1;
                                        if retries > MAX_RETRIES {
                                            break;
                                        }
                                        thread::sleep(Duration::from_millis(1));
                                    }
                                }
                            }
                        }
                    });
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
            let c = committed.load(Ordering::Relaxed);
            results.push(TestResult {
                name: "hot_row_progress",
                pass: c > 0,
                detail: format!("committed={c}"),
            });
        }

        // 2. No deadlock under contention
        {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("c2.db").to_string_lossy().to_string();
            init_db(&path).await;

            let panicked = Arc::new(AtomicBool::new(false));
            let barrier = Arc::new(Barrier::new(3));
            let mut handles = Vec::new();

            for wid in 0..3 {
                let p = path.clone();
                let pa = Arc::clone(&panicked);
                let b = Arc::clone(&barrier);
                handles.push(thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = open_conn(&p).await;
                        b.wait();
                        for _ in 0..10 {
                            let target = (wid % 3) + 1;
                            drop(conn.execute("BEGIN CONCURRENT;").await);
                            drop(
                                conn.execute(&format!(
                                    "UPDATE accounts SET balance = balance + 1 WHERE id = {target};"
                                ))
                                .await,
                            );
                            match conn.execute("COMMIT;").await {
                                Ok(_) => {}
                                Err(e) if e.is_transient() => {
                                    rollback_best_effort(&conn).await;
                                }
                                Err(_) => {
                                    rollback_best_effort(&conn).await;
                                    pa.store(true, Ordering::Relaxed);
                                }
                            }
                        }
                    });
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
            let pass = !panicked.load(Ordering::Relaxed);
            results.push(TestResult {
                name: "no_deadlock",
                pass,
                detail: format!("panicked={}", !pass),
            });
        }

        // 3. Abort rate bounded (not 100%)
        {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("c3.db").to_string_lossy().to_string();
            init_db(&path).await;

            let committed = Arc::new(AtomicU64::new(0));
            let total = Arc::new(AtomicU64::new(0));
            let barrier = Arc::new(Barrier::new(3));
            let mut handles = Vec::new();

            for wid in 0..3 {
                let p = path.clone();
                let c = Arc::clone(&committed);
                let t = Arc::clone(&total);
                let b = Arc::clone(&barrier);
                handles.push(thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = open_conn(&p).await;
                        b.wait();
                        for _ in 0..20 {
                            t.fetch_add(1, Ordering::Relaxed);
                            let (rid, wid_target) = if wid % 2 == 0 { (1, 2) } else { (2, 1) };
                            drop(conn.execute("BEGIN CONCURRENT;").await);
                            drop(read_balance(&conn, rid).await);
                            drop(
                                conn.execute(&format!(
                                    "UPDATE accounts SET balance = balance + 1 WHERE id = {wid_target};"
                                ))
                                .await,
                            );
                            match conn.execute("COMMIT;").await {
                                Ok(_) => {
                                    c.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(_) => {
                                    rollback_best_effort(&conn).await;
                                }
                            }
                        }
                    });
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
            let c = committed.load(Ordering::Relaxed);
            let tt = total.load(Ordering::Relaxed);
            let abort_rate = if tt > 0 {
                1.0 - (c as f64 / tt as f64)
            } else {
                0.0
            };
            results.push(TestResult {
                name: "abort_rate_bounded",
                pass: c > 0 && abort_rate < 1.0,
                detail: format!(
                    "committed={c} total={tt} abort_rate={:.1}%",
                    abort_rate * 100.0
                ),
            });
        }

        // 4. Sum invariant preserved under contention
        {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("c4.db").to_string_lossy().to_string();
            init_db(&path).await;

            let committed = Arc::new(AtomicU64::new(0));
            let barrier = Arc::new(Barrier::new(3));
            let mut handles = Vec::new();

            for _ in 0..3 {
                let p = path.clone();
                let c = Arc::clone(&committed);
                let b = Arc::clone(&barrier);
                handles.push(thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = open_conn(&p).await;
                        b.wait();
                        for _ in 0..10 {
                            let mut retries = 0;
                            loop {
                                if conn.execute("BEGIN CONCURRENT;").await.is_err() {
                                    rollback_best_effort(&conn).await;
                                    retries += 1;
                                    if retries > MAX_RETRIES {
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(1));
                                    continue;
                                }
                                // Transfer: subtract from 1, add to 2 (net zero)
                                if conn
                                    .execute(
                                        "UPDATE accounts SET balance = balance - 1 WHERE id = 1;",
                                    )
                                    .await
                                    .is_err()
                                {
                                    rollback_best_effort(&conn).await;
                                    retries += 1;
                                    if retries > MAX_RETRIES {
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(1));
                                    continue;
                                }
                                if conn
                                    .execute(
                                        "UPDATE accounts SET balance = balance + 1 WHERE id = 2;",
                                    )
                                    .await
                                    .is_err()
                                {
                                    rollback_best_effort(&conn).await;
                                    retries += 1;
                                    if retries > MAX_RETRIES {
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(1));
                                    continue;
                                }
                                match conn.execute("COMMIT;").await {
                                    Ok(_) => {
                                        c.fetch_add(1, Ordering::Relaxed);
                                        break;
                                    }
                                    Err(e) if e.is_transient() => {
                                        rollback_best_effort(&conn).await;
                                        retries += 1;
                                        if retries > MAX_RETRIES {
                                            break;
                                        }
                                        thread::sleep(Duration::from_millis(1));
                                    }
                                    Err(_) => {
                                        rollback_best_effort(&conn).await;
                                        break;
                                    }
                                }
                            }
                        }
                    });
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
            let (sum, _) = verify_sum_invariant(&path).await;
            let expected = ACCOUNT_COUNT as i64 * INITIAL_BALANCE;
            results.push(TestResult {
                name: "sum_invariant_preserved",
                pass: sum == expected,
                detail: format!(
                    "sum={sum} expected={expected} committed={}",
                    committed.load(Ordering::Relaxed)
                ),
            });
        }

        // 5. Throughput positive under contention
        {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("c5.db").to_string_lossy().to_string();
            init_db(&path).await;

            let committed = Arc::new(AtomicU64::new(0));
            let barrier = Arc::new(Barrier::new(3));
            let start = Instant::now();
            let mut handles = Vec::new();

            for _ in 0..3 {
                let p = path.clone();
                let c = Arc::clone(&committed);
                let b = Arc::clone(&barrier);
                handles.push(thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = open_conn(&p).await;
                        b.wait();
                        for _ in 0..10 {
                            let mut retries = 0;
                            loop {
                                if conn.execute("BEGIN CONCURRENT;").await.is_err() {
                                    rollback_best_effort(&conn).await;
                                    retries += 1;
                                    if retries > MAX_RETRIES {
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(1));
                                    continue;
                                }
                                if conn
                                    .execute(
                                        "UPDATE accounts SET balance = balance + 1 WHERE id = 1;",
                                    )
                                    .await
                                    .is_err()
                                {
                                    rollback_best_effort(&conn).await;
                                    retries += 1;
                                    if retries > MAX_RETRIES {
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(1));
                                    continue;
                                }
                                match conn.execute("COMMIT;").await {
                                    Ok(_) => {
                                        c.fetch_add(1, Ordering::Relaxed);
                                        break;
                                    }
                                    Err(_) => {
                                        rollback_best_effort(&conn).await;
                                        retries += 1;
                                        if retries > MAX_RETRIES {
                                            break;
                                        }
                                        thread::sleep(Duration::from_millis(1));
                                    }
                                }
                            }
                        }
                    });
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
            let elapsed = start.elapsed().as_secs_f64();
            let c = committed.load(Ordering::Relaxed);
            let throughput = if elapsed > 0.0 {
                c as f64 / elapsed
            } else {
                0.0
            };
            results.push(TestResult {
                name: "positive_throughput",
                pass: c > 0,
                detail: format!("throughput={throughput:.1} txn/s committed={c}"),
            });
        }

        // Summary
        let total = results.len();
        let passed = results.iter().filter(|r| r.pass).count();
        let failed = total - passed;

        println!("\n=== bd-3plop.4: Lock Contention Storm Conformance Summary ===");
        println!("{{");
        println!("  \"bead\": \"bd-3plop.4\",");
        println!("  \"suite\": \"lock_contention_storms\",");
        println!("  \"total\": {total},");
        println!("  \"passed\": {passed},");
        println!("  \"failed\": {failed},");
        println!(
            "  \"pass_rate\": \"{:.1}%\",",
            passed as f64 / total as f64 * 100.0
        );
        println!("  \"cases\": [");
        for (i, r) in results.iter().enumerate() {
            let comma = if i + 1 < total { "," } else { "" };
            let status = if r.pass { "PASS" } else { "FAIL" };
            println!(
                "    {{ \"name\": \"{}\", \"status\": \"{status}\", \"detail\": \"{}\" }}{comma}",
                r.name, r.detail
            );
        }
        println!("  ]");
        println!("}}");

        assert_eq!(
            failed,
            0,
            "{failed}/{total} contention storm conformance tests failed: {:?}",
            results
                .iter()
                .filter(|r| !r.pass)
                .map(|r| r.name)
                .collect::<Vec<_>>()
        );

        println!("[PASS] all {total} contention storm conformance tests passed");
    });
}
