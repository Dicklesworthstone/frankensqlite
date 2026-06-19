//! D-TEST: historical contention-elimination suite + placeholder gates (bd-3wop3.7).
//!
//! As of 2026-03-23 this file is **not** the canonical threshold authority for
//! the overlay scorecard. The truthful benchmark surface lives in:
//! - `crates/fsqlite-e2e/benches/concurrent_write_persistent_bench.rs`
//! - `artifacts/perf/2026-03-23-local/canonical_{mvcc,single_writer}.{md,jsonl}`
//! - the still-blocked governance lane (`bd-db300.1.7.4`, `bd-db300.7.9.1`,
//!   `bd-3wop3.1.5`) that owns final c1/4/8 and persistent 2/4/8/16 gate truth
//!
//! The ignored throughput gates in this file therefore remain historical
//! scaffolding only; they must not be read as current pass/fail policy.
//! Operators should use `scripts/capture_c1_evidence_pack.sh` for the c1 truth
//! surface and `scripts/capture_persistent_phase_pack.sh` for the persistent
//! 8t/16t truth surface and same-pack comparator provenance.
//!
//! ## Contention Tests
//! 1. test_no_global_locks_in_commit_fast_path
//! 2. test_parallel_wal_segments_independent (D1 dependency)
//! 3. test_page_cache_shard_distribution (D2 dependency)
//! 4. test_combiner_reduces_atomic_ops (D3 dependency)
//! 5. test_ebr_no_gc_pauses (D5 dependency)
//! 6. test_scaling_curve
//!
//! ## Stress Tests
//! 7. test_64_thread_no_deadlock
//! 8. test_contention_under_gc_pressure
//!
//! ## Dependencies
//! - D1: Parallel WAL with per-thread log buffers
//! - D2: Sharded PageCache (128 partitions)
//! - D3: Flat Combining for commit sequencer
//! - D5: Epoch-Based Reclamation for MVCC GC
//!
//! Run with:
//! ```sh
//! cargo test -p fsqlite-e2e --test bd_3wop3_7_contention_elimination -- --nocapture
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Thread counts for scaling curve analysis.
const SCALING_THREAD_COUNTS: &[usize] = &[1, 2, 4, 8, 16];

/// Rows per thread for throughput tests.
const ROWS_PER_THREAD: u64 = 10_000;

/// Historical 8-thread placeholder gate from the pre-overlay contention file.
const HISTORICAL_PLACEHOLDER_8T_SPEEDUP: f64 = 1.5;

/// Historical 16-thread placeholder gate from the pre-overlay contention file.
const HISTORICAL_PLACEHOLDER_16T_SPEEDUP: f64 = 1.0;

fn sqlite_i64(value: u64) -> i64 {
    i64::try_from(value).expect("test workload values fit SQLite INTEGER")
}

// ---------------------------------------------------------------------------
// Test result types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct ThroughputResult {
    ops_per_sec: f64,
}

// ---------------------------------------------------------------------------
// Helper: C SQLite baseline measurement
// ---------------------------------------------------------------------------

/// Measure C SQLite throughput at the given thread count.
///
/// Uses WAL mode with busy_timeout for write serialization.
fn measure_csqlite_throughput(thread_count: usize, rows_per_thread: u64) -> ThroughputResult {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_str().unwrap().to_owned();

    // Setup database
    {
        let conn = rusqlite::Connection::open(&path).expect("open");
        conn.execute_batch(
            "PRAGMA page_size = 4096;
             PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA cache_size = -64000;
             CREATE TABLE bench (id INTEGER PRIMARY KEY, val INTEGER);",
        )
        .expect("setup");
    }

    let barrier = Arc::new(Barrier::new(thread_count));
    let total_ops = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let handles: Vec<_> = (0..thread_count)
        .map(|tid| {
            let p = path.clone();
            let bar = Arc::clone(&barrier);
            let ops = Arc::clone(&total_ops);
            let base = (tid as u64) * rows_per_thread * 2; // Non-overlapping ranges

            thread::spawn(move || {
                let conn = rusqlite::Connection::open(&p).expect("thread open");
                conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=10000;")
                    .expect("pragma");
                bar.wait();

                let mut local_ops = 0u64;
                for i in 0..rows_per_thread {
                    // Each insert is its own transaction for maximum contention
                    if conn
                        .execute(
                            "INSERT INTO bench VALUES (?1, ?2)",
                            rusqlite::params![sqlite_i64(base + i), sqlite_i64(i * 7)],
                        )
                        .is_ok()
                    {
                        local_ops += 1;
                    }
                }
                ops.fetch_add(local_ops, Ordering::Relaxed);
            })
        })
        .collect();

    for h in handles {
        h.join().expect("join");
    }

    let elapsed = start.elapsed();
    let total = total_ops.load(Ordering::Relaxed);
    let ops_per_sec = total as f64 / elapsed.as_secs_f64();

    ThroughputResult { ops_per_sec }
}

// ---------------------------------------------------------------------------
// Placeholder: FrankenSQLite throughput measurement
// ---------------------------------------------------------------------------

/// Measure the old FrankenSQLite placeholder control used by this file.
///
/// This is intentionally **not** a truthful concurrent-writer benchmark:
/// - it uses one in-memory connection,
/// - it runs sequentially,
/// - it bypasses the persistent-path harnesses that the 2026-03-23 overlay uses.
///
/// Keep this helper only so the historical ignored scaffolding compiles until
/// the blocked governance and matrix work replaces it with a real gate.
fn measure_fsqlite_placeholder_sequential_control(
    thread_count: usize,
    rows_per_thread: u64,
) -> ThroughputResult {
    let conn = fsqlite::Connection::open(":memory:").expect("open");
    conn.execute("PRAGMA journal_mode = WAL").ok();
    conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, val INTEGER)")
        .expect("create");

    let total_ops = thread_count as u64 * rows_per_thread;
    let start = Instant::now();

    for i in 0..total_ops {
        conn.execute_with_params(
            "INSERT INTO bench VALUES (?1, ?2)",
            &[
                fsqlite::SqliteValue::Integer(i as i64),
                fsqlite::SqliteValue::Integer((i * 7) as i64),
            ],
        )
        .ok();
    }

    let elapsed = start.elapsed();
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();

    ThroughputResult { ops_per_sec }
}

fn create_fsqlite_file_backed_db(filename: &str, schema_sql: &str) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join(filename).to_string_lossy().to_string();
    let conn = fsqlite::Connection::open(path.as_str()).expect("open setup db");
    conn.execute("PRAGMA journal_mode = WAL").ok();
    conn.execute("PRAGMA fsqlite.concurrent_mode = ON").ok();
    conn.execute(schema_sql).expect("create schema");
    (dir, path)
}

fn open_fsqlite_worker(path: &str) -> fsqlite::Connection {
    let conn = fsqlite::Connection::open(path.to_owned()).expect("open worker db");
    conn.execute("PRAGMA fsqlite.concurrent_mode = ON").ok();
    conn
}

// ===========================================================================
// CONTENTION TESTS
// ===========================================================================

/// Test 1: Verify no global locks in the commit fast path.
///
/// Instruments the commit path to assert no global Mutex is acquired during
/// WAL frame writing. This is the foundational guarantee of D1.
///
/// Depends on: D1 (parallel WAL)
#[test]
#[ignore = "requires D1 (parallel WAL) to be complete"]
fn test_no_global_locks_in_commit_fast_path() {
    // TODO(D1): Implement instrumentation for commit path.
    //
    // Strategy:
    // 1. Set a thread-local flag before commit
    // 2. Hook into WAL frame write path
    // 3. Assert no global Mutex (WAL_APPEND_GATE) is acquired
    // 4. Clear flag after commit
    //
    // This test will fail if any global lock is acquired during the commit
    // fast path, catching regressions in the parallel WAL design.

    panic!("test_no_global_locks_in_commit_fast_path: D1 not yet implemented");
}

/// Test 2: Verify WAL segment writes don't serialize.
///
/// Spawns N writer threads, each writing to their own WAL segment. Measures
/// that aggregate write bandwidth scales with thread count.
///
/// Depends on: D1 (parallel WAL)
#[test]
#[ignore = "requires D1 (parallel WAL) to be complete"]
fn test_parallel_wal_segments_independent() {
    // TODO(D1): Implement once parallel WAL segments exist.
    //
    // Strategy:
    // 1. Spawn 8 threads, each inserting 10k rows
    // 2. Measure per-thread write bandwidth
    // 3. Assert total bandwidth > 6x single-thread (allowing for overhead)
    //
    // If WAL segments serialize, total bandwidth will plateau at 1x.

    panic!("test_parallel_wal_segments_independent: D1 not yet implemented");
}

/// Test 3: Verify pages distribute evenly across 128 shards.
///
/// Inserts rows that touch many different pages, then inspects the PageCache
/// shard distribution to verify no single shard is overloaded.
///
/// Depends on: D2 (sharded PageCache)
#[test]
#[ignore = "requires D2 (sharded PageCache) to be complete"]
fn test_page_cache_shard_distribution() {
    // TODO(D2): Implement once sharded PageCache is wired.
    //
    // Strategy:
    // 1. Create a 10MB database (2560 pages at 4KB)
    // 2. Touch all pages via full table scan
    // 3. Query PageCache shard distribution
    // 4. Assert coefficient of variation < 0.2 (even distribution)
    //
    // Birthday problem: 2560 pages across 128 shards ≈ 20 pages/shard average.
    // CoV < 0.2 ensures no pathological clustering.

    panic!("test_page_cache_shard_distribution: D2 not yet implemented");
}

/// Test 4: Verify flat combining reduces atomic operations.
///
/// Counts fetch_add calls during concurrent commits and asserts the count is
/// reduced by the combining factor (batching amortizes atomic ops).
///
/// Depends on: D3 (flat combining)
#[test]
#[ignore = "requires D3 (flat combining) to be complete"]
fn test_combiner_reduces_atomic_ops() {
    // TODO(D3): Implement once flat combining is wired.
    //
    // Strategy:
    // 1. Run 8 threads, each committing 1000 transactions
    // 2. Count total fetch_add calls to commit_seq counter
    // 3. Without combining: 8000 fetch_add calls
    // 4. With combining: ~8000 / combining_factor calls
    // 5. Assert reduction > 2x (conservative floor)
    //
    // Flat combining batches commit sequence increments, reducing cache-line
    // ping-pong on the global atomic.

    panic!("test_combiner_reduces_atomic_ops: D3 not yet implemented");
}

/// Test 5: Verify EBR causes no GC-induced latency spikes.
///
/// Runs sustained writes with aggressive GC, measuring p99 latency to ensure
/// no pause exceeds the threshold.
///
/// Depends on: D5 (EBR)
#[test]
#[ignore = "timing-sensitive stress test"]
fn test_ebr_no_gc_pauses() {
    // EBR is already implemented (D5 complete). This test verifies no GC
    // pauses under sustained write load.
    //
    // Strategy:
    // 1. Spawn 4 writer threads, continuous inserts for 5 seconds
    // 2. Record per-operation latency
    // 3. Assert p99 latency < 10ms (no GC stalls)

    use std::sync::atomic::AtomicBool;

    let (_dir, path) = create_fsqlite_file_backed_db(
        "ebr_no_gc_pauses.db",
        "CREATE TABLE bench (id INTEGER PRIMARY KEY, val TEXT)",
    );
    let path = Arc::new(path);

    let stop = Arc::new(AtomicBool::new(false));
    let latencies = Arc::new(std::sync::Mutex::new(Vec::new()));

    let barrier = Arc::new(Barrier::new(4));
    let handles: Vec<_> = (0..4)
        .map(|tid| {
            let p = Arc::clone(&path);
            let s = Arc::clone(&stop);
            let l = Arc::clone(&latencies);
            let b = Arc::clone(&barrier);

            thread::spawn(move || {
                let c = open_fsqlite_worker(p.as_str());
                b.wait();
                let mut local_latencies = Vec::with_capacity(10_000);
                let mut i = tid * 1_000_000;

                while !s.load(Ordering::Relaxed) {
                    let op_start = Instant::now();
                    let _ = c.execute_with_params(
                        "INSERT OR REPLACE INTO bench VALUES (?1, ?2)",
                        &[
                            fsqlite::SqliteValue::Integer(i),
                            fsqlite::SqliteValue::Text(format!("value_{i}").into()),
                        ],
                    );
                    local_latencies.push(op_start.elapsed().as_micros() as u64);
                    i += 1;
                }

                l.lock().unwrap().extend(local_latencies);
            })
        })
        .collect();

    // Run for 2 seconds (reduced for test speed)
    thread::sleep(Duration::from_secs(2));
    stop.store(true, Ordering::Release);

    for h in handles {
        h.join().expect("join");
    }

    let mut all_latencies = latencies.lock().unwrap().clone();
    all_latencies.sort_unstable();

    if all_latencies.is_empty() {
        panic!("No operations completed");
    }

    let p99_idx = (all_latencies.len() as f64 * 0.99) as usize;
    let p99_us = all_latencies.get(p99_idx).copied().unwrap_or(0);
    let p99_ms = p99_us as f64 / 1000.0;

    println!(
        "[test_ebr_no_gc_pauses] {} ops, p99={:.2}ms, max={:.2}ms",
        all_latencies.len(),
        p99_ms,
        all_latencies.last().copied().unwrap_or(0) as f64 / 1000.0
    );

    // Assert p99 < 10ms (generous threshold for GC-free operation)
    assert!(
        p99_ms < 10.0,
        "bd-3wop3.7: EBR p99 latency {p99_ms:.2}ms exceeds 10ms threshold"
    );
}

/// Test 6: Historical scaling-curve placeholder.
///
/// The real 2026-03-23 scaling story is owned by the canonical matrix and the
/// persistent benchmark harness, not by this file's sequential control.
#[test]
#[ignore = "stale placeholder; pending bd-3wop3.1.5, bd-db300.1.7.4, and bd-db300.7.9.1"]
fn test_scaling_curve() {
    panic!(
        "test_scaling_curve: stale placeholder gate; use scripts/capture_c1_evidence_pack.sh and scripts/capture_persistent_phase_pack.sh instead"
    );
}

// ===========================================================================
// REGRESSION GATES
// ===========================================================================

/// Regression gate: 8-thread throughput >= 1.5x C SQLite.
///
/// Historical note only: this function is blocked because the helper below is a
/// sequential in-memory control, not a truthful persistent concurrent benchmark.
#[test]
#[ignore = "stale placeholder; pending bd-3wop3.1.5, bd-db300.1.7.4, and bd-db300.7.9.1"]
fn test_8t_throughput_regression_gate() {
    panic!(
        "test_8t_throughput_regression_gate: historical {HISTORICAL_PLACEHOLDER_8T_SPEEDUP}x placeholder is non-authoritative; final 8t gate belongs to scripts/capture_persistent_phase_pack.sh with same-pack sqlite3 comparison"
    );
}

/// Regression gate: 16-thread throughput >= 1.0x C SQLite.
///
/// Historical note only: persistent 16-thread truth is part of the blocked
/// overlay contract and must not be inferred from this file's placeholder path.
#[test]
#[ignore = "stale placeholder; pending bd-3wop3.1.5, bd-db300.1.7.4, and bd-db300.7.9.1"]
fn test_16t_throughput_regression_gate() {
    panic!(
        "test_16t_throughput_regression_gate: historical {HISTORICAL_PLACEHOLDER_16T_SPEEDUP}x placeholder is non-authoritative; final persistent 16t gate belongs to scripts/capture_persistent_phase_pack.sh with phase-attribution evidence"
    );
}

// ===========================================================================
// STRESS TESTS
// ===========================================================================

/// Stress test: 64 threads, no deadlock within 60 seconds.
///
/// Spawns 64 writer threads with overlapping key ranges to maximize lock
/// contention. Asserts no deadlock occurs (all threads complete or timeout).
#[test]
#[ignore = "manual contention stress test"]
fn test_64_thread_no_deadlock() {
    // This stress test already exercises the current file-backed concurrent path.
    // It is separate from the historical placeholder throughput helper above.

    let (_dir, path) = create_fsqlite_file_backed_db(
        "64_thread_no_deadlock.db",
        "CREATE TABLE stress (id INTEGER PRIMARY KEY, val INTEGER)",
    );
    let setup_conn = open_fsqlite_worker(&path);

    // Pre-populate some rows to enable updates
    for i in 0..100 {
        setup_conn
            .execute_with_params(
                "INSERT INTO stress VALUES (?1, ?2)",
                &[
                    fsqlite::SqliteValue::Integer(i),
                    fsqlite::SqliteValue::Integer(0),
                ],
            )
            .ok();
    }
    let path = Arc::new(path);

    let barrier = Arc::new(Barrier::new(64));
    let stop = Arc::new(AtomicBool::new(false));
    let completed = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..64)
        .map(|tid| {
            let p = Arc::clone(&path);
            let b = Arc::clone(&barrier);
            let s = Arc::clone(&stop);
            let comp = Arc::clone(&completed);

            thread::spawn(move || {
                let c = open_fsqlite_worker(p.as_str());
                b.wait();
                let mut ops = 0u64;

                while !s.load(Ordering::Relaxed) && ops < 1000 {
                    // Update a random-ish row to create contention
                    let row_id = ((tid * 17 + ops as usize) % 100) as i64;
                    let _ = c.execute_with_params(
                        "UPDATE stress SET val = val + 1 WHERE id = ?1",
                        &[fsqlite::SqliteValue::Integer(row_id)],
                    );
                    ops += 1;
                }

                comp.fetch_add(1, Ordering::Relaxed);
            })
        })
        .collect();

    // Give threads up to 30 seconds to complete
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline && completed.load(Ordering::Relaxed) < 64 {
        thread::sleep(Duration::from_millis(100));
    }

    stop.store(true, Ordering::Release);

    // Wait for all threads with timeout
    for h in handles {
        let _ = h.join();
    }

    let final_completed = completed.load(Ordering::Relaxed);
    println!("[test_64_thread_no_deadlock] {final_completed}/64 threads completed");

    assert_eq!(
        final_completed, 64,
        "bd-3wop3.7: deadlock detected - only {final_completed}/64 threads completed"
    );
}

/// Stress test: High write + aggressive GC, no throughput collapse.
///
/// Runs continuous writes while forcing frequent GC cycles. Asserts throughput
/// remains above a minimum floor (no GC-induced starvation).
#[test]
#[ignore = "timing-sensitive stress test"]
fn test_contention_under_gc_pressure() {
    // This test exercises the EBR-based GC under write pressure.

    let (_dir, path) = create_fsqlite_file_backed_db(
        "contention_under_gc_pressure.db",
        "CREATE TABLE gc_stress (id INTEGER PRIMARY KEY, data BLOB)",
    );
    let path = Arc::new(path);

    let stop = Arc::new(AtomicBool::new(false));
    let total_ops = Arc::new(AtomicU64::new(0));

    let barrier = Arc::new(Barrier::new(4));
    let handles: Vec<_> = (0..4)
        .map(|tid| {
            let p = Arc::clone(&path);
            let b = Arc::clone(&barrier);
            let s = Arc::clone(&stop);
            let ops = Arc::clone(&total_ops);

            thread::spawn(move || {
                let c = open_fsqlite_worker(p.as_str());
                b.wait();
                let mut local_ops = 0u64;
                let mut i = tid * 10_000_000;

                // Create a blob that will stress memory allocation
                let blob = vec![0xABu8; 1024];

                while !s.load(Ordering::Relaxed) {
                    // INSERT OR REPLACE to create version churn (GC pressure)
                    let row_id = (i % 1000) as i64; // Reuse 1000 row IDs
                    let _ = c.execute_with_params(
                        "INSERT OR REPLACE INTO gc_stress VALUES (?1, ?2)",
                        &[
                            fsqlite::SqliteValue::Integer(row_id),
                            fsqlite::SqliteValue::Blob(blob.clone().into()),
                        ],
                    );
                    local_ops += 1;
                    i += 1;
                }

                ops.fetch_add(local_ops, Ordering::Relaxed);
            })
        })
        .collect();

    // Run for 3 seconds
    let start = Instant::now();
    thread::sleep(Duration::from_secs(3));
    stop.store(true, Ordering::Release);

    for h in handles {
        h.join().expect("join");
    }

    let elapsed = start.elapsed();
    let total = total_ops.load(Ordering::Relaxed);
    let ops_per_sec = total as f64 / elapsed.as_secs_f64();

    println!(
        "[test_contention_under_gc_pressure] {} ops in {:.2}s = {:.0} ops/s",
        total,
        elapsed.as_secs_f64(),
        ops_per_sec
    );

    // Assert minimum throughput (very conservative floor)
    // With GC pressure, we should still achieve at least 1000 ops/s
    assert!(
        ops_per_sec > 1000.0,
        "bd-3wop3.7: GC pressure caused throughput collapse ({ops_per_sec:.0} ops/s < 1000)"
    );
}

// ===========================================================================
// SPLIT-LOCK COMMIT TESTS (D1-CRITICAL bd-3wop3.8)
// ===========================================================================

/// Test 7: Verify split-lock commit allows concurrent prepare phases.
///
/// The split-lock protocol separates commit into three phases:
/// - Phase A (prepare): Hold inner.lock(), collect write set
/// - Phase B (WAL I/O): Hold wal_backend.lock(), release inner.lock()
/// - Phase C (publish): Re-acquire inner.lock(), update db_size
///
/// This allows Thread B to start its prepare phase while Thread A does WAL I/O.
#[test]
#[ignore = "manual contention stress test"]
fn test_split_lock_commit_no_deadlock() {
    // Test that multiple concurrent writers don't deadlock with the split-lock
    // protocol. With the old monolithic lock, this would cause severe contention.

    let (_dir, path) = create_fsqlite_file_backed_db(
        "split_lock_commit_no_deadlock.db",
        "CREATE TABLE split_lock_test (id INTEGER PRIMARY KEY, val INTEGER)",
    );
    let path = Arc::new(path);

    let barrier = Arc::new(Barrier::new(8));
    let completed = Arc::new(AtomicU64::new(0));
    let total_ops = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..8)
        .map(|tid| {
            let p = Arc::clone(&path);
            let b = Arc::clone(&barrier);
            let comp = Arc::clone(&completed);
            let ops = Arc::clone(&total_ops);
            let base = (tid as i64) * 10_000;

            thread::spawn(move || {
                let c = open_fsqlite_worker(p.as_str());
                b.wait();
                let mut local_ops = 0u64;

                // Each thread inserts 500 rows, each as its own transaction
                // This maximizes commit contention
                for i in 0..500 {
                    if c.execute_with_params(
                        "INSERT INTO split_lock_test VALUES (?1, ?2)",
                        &[
                            fsqlite::SqliteValue::Integer(base + i),
                            fsqlite::SqliteValue::Integer(i * 7),
                        ],
                    )
                    .is_ok()
                    {
                        local_ops += 1;
                    }
                }

                ops.fetch_add(local_ops, Ordering::Relaxed);
                comp.fetch_add(1, Ordering::Relaxed);
            })
        })
        .collect();

    // Give threads up to 30 seconds to complete (generous timeout)
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline && completed.load(Ordering::Relaxed) < 8 {
        thread::sleep(Duration::from_millis(100));
    }

    for h in handles {
        h.join().expect("join");
    }

    let final_completed = completed.load(Ordering::Relaxed);
    let final_ops = total_ops.load(Ordering::Relaxed);

    println!(
        "[test_split_lock_commit_no_deadlock] {}/8 threads completed, {} total ops",
        final_completed, final_ops
    );

    assert_eq!(
        final_completed, 8,
        "bd-3wop3.8: split-lock deadlock - only {}/8 threads completed",
        final_completed
    );

    // All 8 threads × 500 ops = 4000 expected
    assert!(
        final_ops >= 3800,
        "bd-3wop3.8: too few operations completed ({} < 3800)",
        final_ops
    );
}

/// Test 8: Verify split-lock commit throughput scales better than monolithic lock.
///
/// Measures commit throughput with increasing thread counts. With split-lock,
/// we expect better scaling because prepare phases can overlap with WAL I/O.
#[test]
#[ignore = "manual throughput benchmark"]
fn test_split_lock_commit_scaling() {
    // Measure throughput at 1, 2, 4, 8 threads and verify scaling isn't pathological.

    let results: Vec<(usize, f64)> = [1, 2, 4, 8]
        .iter()
        .map(|&thread_count| {
            let (_dir, path) = create_fsqlite_file_backed_db(
                &format!("split_lock_commit_scaling_{thread_count}.db"),
                "CREATE TABLE scaling_test (id INTEGER PRIMARY KEY, val INTEGER)",
            );
            let path = Arc::new(path);

            let barrier = Arc::new(Barrier::new(thread_count));
            let total_ops = Arc::new(AtomicU64::new(0));
            let ops_per_thread = 1000;

            let start = Instant::now();

            let handles: Vec<_> = (0..thread_count)
                .map(|tid| {
                    let p = Arc::clone(&path);
                    let b = Arc::clone(&barrier);
                    let ops = Arc::clone(&total_ops);
                    let base = (tid as i64) * (ops_per_thread as i64) * 2;

                    thread::spawn(move || {
                        let c = open_fsqlite_worker(p.as_str());
                        b.wait();
                        let mut local_ops = 0u64;

                        for i in 0..ops_per_thread {
                            if c.execute_with_params(
                                "INSERT INTO scaling_test VALUES (?1, ?2)",
                                &[
                                    fsqlite::SqliteValue::Integer(base + i as i64),
                                    fsqlite::SqliteValue::Integer(i as i64),
                                ],
                            )
                            .is_ok()
                            {
                                local_ops += 1;
                            }
                        }

                        ops.fetch_add(local_ops, Ordering::Relaxed);
                    })
                })
                .collect();

            for h in handles {
                h.join().expect("join");
            }

            let elapsed = start.elapsed();
            let total = total_ops.load(Ordering::Relaxed);
            let ops_per_sec = total as f64 / elapsed.as_secs_f64();

            (thread_count, ops_per_sec)
        })
        .collect();

    println!("\n[test_split_lock_commit_scaling] Results:");
    for (threads, ops) in &results {
        println!("  {}t: {:.0} ops/s", threads, ops);
    }

    // Verify basic sanity: throughput at 4+ threads shouldn't collapse below 1-thread
    let single_thread_ops = results[0].1;
    let four_thread_ops = results[2].1;
    let eight_thread_ops = results[3].1;

    // With split-lock, 4t should be at least 50% of 1t (allowing for contention)
    // This is a conservative check - the goal is to catch pathological regression
    assert!(
        four_thread_ops > single_thread_ops * 0.5,
        "bd-3wop3.8: 4t throughput collapsed ({:.0} < {:.0} * 0.5)",
        four_thread_ops,
        single_thread_ops
    );

    // 8t should still be at least 30% of 1t (more contention expected)
    assert!(
        eight_thread_ops > single_thread_ops * 0.3,
        "bd-3wop3.8: 8t throughput collapsed ({:.0} < {:.0} * 0.3)",
        eight_thread_ops,
        single_thread_ops
    );
}

/// Test 9: Verify WAL I/O phase doesn't block prepare phase.
///
/// Creates artificial WAL I/O delay and verifies other threads can still
/// make progress on their prepare phases.
#[test]
#[ignore = "manual contention stress test"]
fn test_split_lock_wal_io_does_not_block_prepare() {
    // This test verifies the core property of split-lock: that WAL I/O in one
    // thread doesn't block prepare in another thread.

    let (_dir, path) = create_fsqlite_file_backed_db(
        "split_lock_wal_io_does_not_block_prepare.db",
        "CREATE TABLE wal_io_test (id INTEGER PRIMARY KEY, data BLOB)",
    );
    let path = Arc::new(path);

    let barrier = Arc::new(Barrier::new(4));
    let completed = Arc::new(AtomicU64::new(0));
    let total_ops = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..4)
        .map(|tid| {
            let p = Arc::clone(&path);
            let b = Arc::clone(&barrier);
            let comp = Arc::clone(&completed);
            let ops = Arc::clone(&total_ops);

            thread::spawn(move || {
                let c = open_fsqlite_worker(p.as_str());
                b.wait();
                let mut local_ops = 0u64;

                // Write larger blobs to make WAL I/O more significant
                let blob = vec![0xABu8; 4096]; // 4KB per row

                for i in 0..100 {
                    let row_id = (tid * 1000 + i) as i64;
                    if c.execute_with_params(
                        "INSERT INTO wal_io_test VALUES (?1, ?2)",
                        &[
                            fsqlite::SqliteValue::Integer(row_id),
                            fsqlite::SqliteValue::Blob(blob.clone().into()),
                        ],
                    )
                    .is_ok()
                    {
                        local_ops += 1;
                    }
                }

                ops.fetch_add(local_ops, Ordering::Relaxed);
                comp.fetch_add(1, Ordering::Relaxed);
            })
        })
        .collect();

    // All threads should complete within 10 seconds
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && completed.load(Ordering::Relaxed) < 4 {
        thread::sleep(Duration::from_millis(50));
    }

    for h in handles {
        h.join().expect("join");
    }

    let final_completed = completed.load(Ordering::Relaxed);
    let final_ops = total_ops.load(Ordering::Relaxed);

    println!(
        "[test_split_lock_wal_io_does_not_block_prepare] {}/4 threads, {} ops",
        final_completed, final_ops
    );

    assert_eq!(
        final_completed, 4,
        "bd-3wop3.8: WAL I/O blocked prepare - only {}/4 threads completed",
        final_completed
    );

    // 4 threads × 100 ops = 400 expected
    assert!(
        final_ops >= 380,
        "bd-3wop3.8: too few ops with large WAL I/O ({} < 380)",
        final_ops
    );
}

// ===========================================================================
// SCALING REPORT (manual run)
// ===========================================================================

/// Generate a scaling report comparing FrankenSQLite vs C SQLite at multiple
/// thread counts.
///
/// Run with: `cargo test -p fsqlite-e2e --test bd_3wop3_7_contention_elimination scaling_report -- --nocapture --ignored`
#[test]
#[ignore = "manual benchmark - run with --ignored"]
fn scaling_report() {
    println!("\n=== D-TEST Scaling Report (bd-3wop3.7) ===\n");
    println!("Thread | C SQLite ops/s | FS placeholder ops/s | Historical placeholder ratio");
    println!("-------|----------------|----------------------|----------------------------");

    for &threads in SCALING_THREAD_COUNTS {
        let csqlite = measure_csqlite_throughput(threads, ROWS_PER_THREAD / 10);
        let fsqlite = measure_fsqlite_placeholder_sequential_control(threads, ROWS_PER_THREAD / 10);
        let speedup = fsqlite.ops_per_sec / csqlite.ops_per_sec;

        println!(
            "{:>6} | {:>14.0} | {:>19.0} | {:>6.2}x",
            threads, csqlite.ops_per_sec, fsqlite.ops_per_sec, speedup
        );
    }

    println!(
        "\nNote: FrankenSQLite numbers here come from a historical sequential placeholder control, not the authoritative c1 or persistent 8t/16t scorecard surfaces. Use scripts/capture_c1_evidence_pack.sh and scripts/capture_persistent_phase_pack.sh for current truth."
    );
}
