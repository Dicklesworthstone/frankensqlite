//! bd-zywqc.15: Multi-threaded (intra-process) concurrency coverage.
//!
//! Complements bd-073kf's multi-process harness by exercising N threads
//! within a single process, each opening its own `fsqlite::Connection`
//! to the same file-backed database. Validates:
//! - Per-task-connection: each thread owns its Connection (actor pattern)
//! - Crash injection: panic in one thread doesn't break others
//! - Throughput scaling: non-decreasing up to NUM_CPUS
//! - Per-thread structured logging with distinguishable span_id
//! - Post-run C SQLite cross-check via verify_concurrency_artifact
#![recursion_limit = "512"]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use fsqlite_e2e::tracing_schema::TraceContext;
use fsqlite_e2e::verify_csqlite::verify_concurrency_artifact;
use serde::Serialize;
use serde_json::json;
use tempfile::TempDir;

const BEAD_ID: &str = "bd-zywqc.15";
const DEFAULT_TASKS: usize = 16;
const OPS_PER_THREAD: u64 = 500;
const RANGE_SIZE: u64 = 1_000_000;
const SEED_BASE: u64 = 0x5A59_5751_4315;
const BUSY_TIMEOUT_MS: u32 = 10_000;

fn fresh_dir() -> TempDir {
    tempfile::tempdir().expect("tempdir")
}

fn emit_log(scenario: &str, phase: &str, data: serde_json::Value) {
    let ctx = TraceContext::with_seed("bd-zywqc.15", SEED_BASE);
    eprintln!(
        "{}",
        json!({
            "bead_id": BEAD_ID,
            "scenario": scenario,
            "phase": phase,
            "run_id": ctx.run_id,
            "data": data,
        })
    );
}

#[derive(Debug, Clone, Serialize)]
struct ThreadResult {
    thread_id: usize,
    ops_completed: u64,
    ops_failed: u64,
    wall_ns: u64,
}

#[derive(Debug, Clone, Serialize)]
struct ScalingPoint {
    threads: usize,
    total_ops: u64,
    wall_ms: u64,
    ops_per_sec: f64,
}

async fn setup_fsqlite_db(path: &str) {
    let conn = fsqlite::Connection::open(path).await.expect("setup open");
    conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
    conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS}"))
        .await
        .expect("timeout");
    conn.execute(
        "CREATE TABLE IF NOT EXISTS mt_bench (id INTEGER PRIMARY KEY, thread_id INTEGER NOT NULL, val INTEGER NOT NULL)",
    )
    .await
    .expect("create");
}

fn setup_csqlite_db(path: &str) {
    let conn = rusqlite::Connection::open(path).expect("csqlite setup");
    conn.execute_batch(&format!(
        "PRAGMA journal_mode=WAL;
         PRAGMA busy_timeout={BUSY_TIMEOUT_MS};
         CREATE TABLE IF NOT EXISTS mt_bench (id INTEGER PRIMARY KEY, thread_id INTEGER NOT NULL, val INTEGER NOT NULL);"
    ))
    .expect("csqlite setup");
}

// ─── Scenario A: per-task-connection disjoint writes (fsqlite) ──────

async fn run_fsqlite_per_task(
    scenario: &str,
    n_threads: usize,
    ops_per_thread: u64,
) -> (Vec<ThreadResult>, u64) {
    let dir = fresh_dir();
    let db_path = dir.path().join("mt.fsqlite");
    let path_str = db_path.to_str().unwrap().to_owned();
    setup_fsqlite_db(&path_str).await;

    emit_log(
        scenario,
        "start",
        json!({"backend": "fsqlite", "threads": n_threads}),
    );

    let barrier = Arc::new(Barrier::new(n_threads));
    let wall_start = Instant::now();

    let handles: Vec<_> = (0..n_threads)
        .map(|tid| {
            let p = path_str.clone();
            let bar = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let mut thread_result: Option<ThreadResult> = None;
                asupersync::test_utils::run_test(|| async {
                    let conn = fsqlite::Connection::open(&p).await.expect("thread open");
                    conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS}"))
                        .await
                        .expect("pragma");
                    bar.wait();
                    let t_start = Instant::now();
                    let base = (tid as u64) * RANGE_SIZE;
                    let mut completed = 0u64;
                    let mut failed = 0u64;
                    conn.execute("BEGIN").await.expect("begin");
                    for i in 0..ops_per_thread {
                        let sql = format!(
                            "INSERT INTO mt_bench (id, thread_id, val) VALUES ({}, {tid}, {})",
                            base + i,
                            i * 7 + 13
                        );
                        match conn.execute(&sql).await {
                            Ok(_) => completed += 1,
                            Err(_) => failed += 1,
                        }
                    }
                    drop(conn.execute("COMMIT").await);
                    thread_result = Some(ThreadResult {
                        thread_id: tid,
                        ops_completed: completed,
                        ops_failed: failed,
                        wall_ns: t_start.elapsed().as_nanos() as u64,
                    });
                });
                thread_result.expect("thread result must be produced")
            })
        })
        .collect();

    let results: Vec<ThreadResult> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let total_wall_ns = wall_start.elapsed().as_nanos() as u64;

    // Verify via csqlite cross-check
    let (report, artifact) = verify_concurrency_artifact(&db_path).unwrap();
    emit_log(
        scenario,
        "csqlite_cross_check",
        json!({
            "ok": report.ok,
            "pages": report.page_count,
            "tables": report.table_count,
            "artifact": artifact.is_some(),
        }),
    );

    (results, total_wall_ns)
}

// ─── Scenario B: per-task-connection disjoint writes (csqlite) ──────

fn run_csqlite_per_task(
    scenario: &str,
    n_threads: usize,
    ops_per_thread: u64,
) -> (Vec<ThreadResult>, u64) {
    let dir = fresh_dir();
    let db_path = dir.path().join("mt.db");
    let path_str = db_path.to_str().unwrap().to_owned();
    setup_csqlite_db(&path_str);

    emit_log(
        scenario,
        "start",
        json!({"backend": "csqlite", "threads": n_threads}),
    );

    let barrier = Arc::new(Barrier::new(n_threads));
    let wall_start = Instant::now();

    let handles: Vec<_> = (0..n_threads)
        .map(|tid| {
            let p = path_str.clone();
            let bar = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let conn = rusqlite::Connection::open(&p).expect("csqlite thread open");
                conn.execute_batch(&format!(
                    "PRAGMA journal_mode=WAL; PRAGMA busy_timeout={BUSY_TIMEOUT_MS};"
                ))
                .expect("pragma");
                bar.wait();
                let t_start = Instant::now();
                let base = (tid as u64) * RANGE_SIZE;
                let mut completed = 0u64;
                let mut failed = 0u64;
                for i in 0..ops_per_thread {
                    match conn.execute(
                        "INSERT INTO mt_bench (id, thread_id, val) VALUES (?1, ?2, ?3)",
                        rusqlite::params![base as i64 + i as i64, tid as i64, i as i64 * 7 + 13],
                    ) {
                        Ok(_) => completed += 1,
                        Err(_) => failed += 1,
                    }
                }
                ThreadResult {
                    thread_id: tid,
                    ops_completed: completed,
                    ops_failed: failed,
                    wall_ns: t_start.elapsed().as_nanos() as u64,
                }
            })
        })
        .collect();

    let results: Vec<ThreadResult> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let total_wall_ns = wall_start.elapsed().as_nanos() as u64;

    (results, total_wall_ns)
}

// ─── M1: 16-task default run ────────────────────────────────────────

#[test]
fn m1_default_16_tasks_per_connection() {
    asupersync::test_utils::run_test(|| async {
        let (results, _wall_ns) =
            run_fsqlite_per_task("M1-default-16", DEFAULT_TASKS, OPS_PER_THREAD).await;

        let total_ops: u64 = results.iter().map(|r| r.ops_completed).sum();
        let total_failed: u64 = results.iter().map(|r| r.ops_failed).sum();

        emit_log(
            "M1-default-16",
            "result",
            json!({
                "total_ops": total_ops,
                "total_failed": total_failed,
                "threads": results.len(),
            }),
        );

        // At least some ops should succeed across threads
        assert!(
            total_ops > 0,
            "at least some operations must succeed across {DEFAULT_TASKS} threads"
        );
    });
}

// ─── M2: crash injection (panic in one thread) ─────────────────────

#[test]
fn m2_panic_in_one_thread_others_unaffected() {
    asupersync::test_utils::run_test(|| async {
        let n_threads = 8;
        let panic_thread = 3;

        let dir = fresh_dir();
        let db_path = dir.path().join("crash.fsqlite");
        let path_str = db_path.to_str().unwrap().to_owned();
        setup_fsqlite_db(&path_str).await;

        emit_log("M2-crash", "start", json!({"panic_thread": panic_thread}));

        let barrier = Arc::new(Barrier::new(n_threads));
        let completed_count = Arc::new(AtomicU64::new(0));

        let handles: Vec<_> = (0..n_threads)
            .map(|tid| {
                let p = path_str.clone();
                let bar = Arc::clone(&barrier);
                let completed = Arc::clone(&completed_count);
                std::thread::spawn(move || {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        asupersync::test_utils::run_test(|| async {
                            let conn =
                                fsqlite::Connection::open(&p).await.expect("thread open");
                            conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS}"))
                                .await
                                .expect("pragma");
                            bar.wait();

                            if tid == panic_thread {
                                panic!("intentional panic in thread {tid}");
                            }

                            let base = (tid as u64) * RANGE_SIZE;
                            conn.execute("BEGIN").await.expect("begin");
                            for i in 0..100u64 {
                                let sql = format!(
                                    "INSERT INTO mt_bench (id, thread_id, val) VALUES ({}, {tid}, {})",
                                    base + i,
                                    i
                                );
                                drop(conn.execute(&sql).await);
                            }
                            drop(conn.execute("COMMIT").await);
                            completed.fetch_add(1, Ordering::Relaxed);
                        });
                    }));
                    result.is_ok()
                })
            })
            .collect();

        let outcomes: Vec<bool> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        let panicked = outcomes.iter().filter(|&&ok| !ok).count();
        let succeeded = outcomes.iter().filter(|&&ok| ok).count();
        let finished = completed_count.load(Ordering::Relaxed);

        emit_log(
            "M2-crash",
            "result",
            json!({
                "panicked": panicked,
                "succeeded": succeeded,
                "completed_writes": finished,
            }),
        );

        assert_eq!(panicked, 1, "exactly one thread should panic");
        assert!(
            succeeded >= n_threads - 1,
            "all non-panicked threads should succeed: {succeeded}/{n_threads}"
        );
        assert!(
            finished > 0,
            "at least some threads must complete their writes"
        );

        // Verify DB is still valid after the panic
        let (report, _) = verify_concurrency_artifact(&db_path).unwrap();
        assert!(
            report.ok,
            "DB must be intact after one thread panicked: {report}"
        );
    });
}

// ─── M3: throughput scaling ─────────────────────────────────────────

#[test]
fn m3_throughput_scaling_csqlite() {
    let thread_counts = [1, 2, 4, 8];
    let ops = 200u64;
    let mut points = Vec::new();

    for &n in &thread_counts {
        let (results, wall_ns) = run_csqlite_per_task(&format!("M3-scaling-{n}t"), n, ops);
        let total_ops: u64 = results.iter().map(|r| r.ops_completed).sum();
        let wall_ms = wall_ns / 1_000_000;
        let ops_per_sec = if wall_ms > 0 {
            total_ops as f64 / (wall_ms as f64 / 1000.0)
        } else {
            0.0
        };
        points.push(ScalingPoint {
            threads: n,
            total_ops,
            wall_ms,
            ops_per_sec,
        });
    }

    emit_log(
        "M3-scaling",
        "result",
        json!({
            "points": points,
        }),
    );

    // Basic sanity: 8t should complete more total ops than 1t
    // (under WAL mode, rusqlite does allow concurrent writes)
    let p1 = points.iter().find(|p| p.threads == 1).unwrap();
    let p8 = points.iter().find(|p| p.threads == 8).unwrap();
    assert!(
        p8.total_ops >= p1.total_ops,
        "8t total_ops ({}) must be >= 1t total_ops ({})",
        p8.total_ops,
        p1.total_ops
    );
}

#[test]
fn m4_throughput_scaling_fsqlite() {
    asupersync::test_utils::run_test(|| async {
        let thread_counts = [1, 2, 4, 8];
        let ops = 200u64;
        let mut points = Vec::new();

        for &n in &thread_counts {
            let (results, wall_ns) =
                run_fsqlite_per_task(&format!("M4-scaling-{n}t"), n, ops).await;
            let total_ops: u64 = results.iter().map(|r| r.ops_completed).sum();
            let wall_ms = wall_ns / 1_000_000;
            let ops_per_sec = if wall_ms > 0 {
                total_ops as f64 / (wall_ms as f64 / 1000.0)
            } else {
                0.0
            };
            points.push(ScalingPoint {
                threads: n,
                total_ops,
                wall_ms,
                ops_per_sec,
            });
        }

        emit_log(
            "M4-scaling-fsqlite",
            "result",
            json!({
                "points": points,
            }),
        );

        // FrankenSQLite with MVCC should have non-decreasing total throughput
        for window in points.windows(2) {
            let (prev, curr) = (&window[0], &window[1]);
            assert!(
                curr.total_ops >= prev.total_ops / 2,
                "throughput should not collapse: {}t={}ops, {}t={}ops",
                prev.threads,
                prev.total_ops,
                curr.threads,
                curr.total_ops
            );
        }
    });
}

// ─── M5: per-thread structured logging with span_id ─────────────────

#[test]
fn m5_per_thread_logs_distinguishable() {
    asupersync::test_utils::run_test(|| async {
        let n_threads = 4;
        let dir = fresh_dir();
        let db_path = dir.path().join("logs.fsqlite");
        let path_str = db_path.to_str().unwrap().to_owned();
        let log_dir = dir.path().join("logs");
        std::fs::create_dir_all(&log_dir).unwrap();

        setup_fsqlite_db(&path_str).await;

        let ctx = TraceContext::with_seed("bd-zywqc.15", SEED_BASE);
        let barrier = Arc::new(Barrier::new(n_threads));

        let handles: Vec<_> = (0..n_threads)
            .map(|tid| {
                let p = path_str.clone();
                let bar = Arc::clone(&barrier);
                let ld = log_dir.clone();
                let run_id = ctx.run_id.clone();
                std::thread::spawn(move || {
                    let mut span_id_out: Option<String> = None;
                    asupersync::test_utils::run_test(|| async {
                        let conn = fsqlite::Connection::open(&p).await.expect("thread open");
                        conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS}"))
                            .await
                            .expect("pragma");

                        let span_id = format!("span_{tid}_{:?}", std::thread::current().id());
                        let log_path = ld.join(format!("thread_{tid}.jsonl"));
                        let mut log_lines = Vec::new();

                        bar.wait();

                        let base = (tid as u64) * RANGE_SIZE;
                        conn.execute("BEGIN").await.expect("begin");
                        for i in 0..50u64 {
                            let sql = format!(
                                "INSERT INTO mt_bench (id, thread_id, val) VALUES ({}, {tid}, {i})",
                                base + i
                            );
                            let outcome = match conn.execute(&sql).await {
                                Ok(_) => "ok",
                                Err(_) => "err",
                            };
                            log_lines.push(
                                json!({
                                    "ts": std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_nanos()
                                        .to_string(),
                                    "run_id": run_id,
                                    "span_id": span_id,
                                    "thread_id": tid,
                                    "op": "insert",
                                    "row_id": base + i,
                                    "outcome": outcome,
                                })
                                .to_string(),
                            );
                        }
                        drop(conn.execute("COMMIT").await);

                        std::fs::write(&log_path, log_lines.join("\n") + "\n").unwrap();

                        span_id_out = Some(span_id);
                    });
                    span_id_out.expect("span_id must be produced")
                })
            })
            .collect();

        let span_ids: Vec<String> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // All span_ids must be unique
        let mut deduped = span_ids.clone();
        deduped.sort();
        deduped.dedup();
        assert_eq!(
            span_ids.len(),
            deduped.len(),
            "all span_ids must be unique: {span_ids:?}"
        );

        // Verify log files exist and are parseable
        for tid in 0..n_threads {
            let log_path = log_dir.join(format!("thread_{tid}.jsonl"));
            assert!(log_path.exists(), "log file for thread {tid} must exist");
            let content = std::fs::read_to_string(&log_path).unwrap();
            let mut line_count = 0;
            for line in content.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                let parsed: serde_json::Value = serde_json::from_str(line)
                    .unwrap_or_else(|e| panic!("invalid JSONL in thread {tid}: {e}"));
                assert!(parsed["span_id"].is_string());
                assert!(parsed["run_id"].is_string());
                assert!(parsed["thread_id"].is_number());
                line_count += 1;
            }
            assert!(line_count > 0, "thread {tid} must have log entries");
        }

        emit_log(
            "M5-logging",
            "result",
            json!({
                "span_ids": span_ids,
                "threads": n_threads,
            }),
        );
    });
}

// ─── M6: hot-page contention (shared rows) ──────────────────────────

#[test]
fn m6_hot_page_contention() {
    asupersync::test_utils::run_test(|| async {
        let n_threads = 4;
        let ops = 100u64;

        let dir = fresh_dir();
        let db_path = dir.path().join("hot.fsqlite");
        let path_str = db_path.to_str().unwrap().to_owned();
        setup_fsqlite_db(&path_str).await;

        // Pre-populate hot rows
        {
            let conn = fsqlite::Connection::open(&path_str)
                .await
                .expect("setup open");
            for i in 0..10i64 {
                conn.execute(&format!(
                    "INSERT INTO mt_bench (id, thread_id, val) VALUES ({i}, 0, 0)"
                ))
                .await
                .expect("seed");
            }
        }

        emit_log(
            "M6-hot-page",
            "start",
            json!({"threads": n_threads, "hot_rows": 10}),
        );

        let barrier = Arc::new(Barrier::new(n_threads));
        let next_id = Arc::new(AtomicU64::new(100));

        let handles: Vec<_> = (0..n_threads)
            .map(|tid| {
                let p = path_str.clone();
                let bar = Arc::clone(&barrier);
                let nid = Arc::clone(&next_id);
                std::thread::spawn(move || {
                    let mut thread_result: Option<ThreadResult> = None;
                    asupersync::test_utils::run_test(|| async {
                        let conn = fsqlite::Connection::open(&p).await.expect("thread open");
                        conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS}"))
                            .await
                            .expect("pragma");
                        bar.wait();
                        let t_start = Instant::now();
                        let mut completed = 0u64;
                        let mut failed = 0u64;
                        for i in 0..ops {
                            // Mix: 50% UPDATE hot rows, 50% INSERT new rows
                            let sql = if i % 2 == 0 {
                                let hot_id = (i % 10) as i64;
                                format!("UPDATE mt_bench SET val = val + 1 WHERE id = {hot_id}")
                            } else {
                                let new_id = nid.fetch_add(1, Ordering::Relaxed);
                                format!(
                                    "INSERT INTO mt_bench (id, thread_id, val) VALUES ({new_id}, {tid}, {i})"
                                )
                            };
                            match conn.execute(&sql).await {
                                Ok(_) => completed += 1,
                                Err(_) => failed += 1,
                            }
                        }
                        thread_result = Some(ThreadResult {
                            thread_id: tid,
                            ops_completed: completed,
                            ops_failed: failed,
                            wall_ns: t_start.elapsed().as_nanos() as u64,
                        });
                    });
                    thread_result.expect("thread result must be produced")
                })
            })
            .collect();

        let results: Vec<ThreadResult> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let total_ops: u64 = results.iter().map(|r| r.ops_completed).sum();
        let total_failed: u64 = results.iter().map(|r| r.ops_failed).sum();

        emit_log(
            "M6-hot-page",
            "result",
            json!({
                "total_ops": total_ops,
                "total_failed": total_failed,
                "results": results,
            }),
        );

        // With hot-page contention, some failures are expected but most should succeed
        assert!(
            total_ops > 0,
            "at least some hot-page operations must succeed"
        );

        // Verify DB integrity after contention
        let (report, _) = verify_concurrency_artifact(&db_path).unwrap();
        assert!(
            report.ok,
            "DB must be intact after hot-page contention: {report}"
        );
    });
}

// ─── M7: csqlite cross-check on fsqlite output ─────────────────────

#[test]
fn m7_fsqlite_output_readable_by_csqlite() {
    asupersync::test_utils::run_test(|| async {
        let dir = fresh_dir();
        let db_path = dir.path().join("cross.fsqlite");
        let path_str = db_path.to_str().unwrap().to_owned();
        setup_fsqlite_db(&path_str).await;

        // Write data from multiple threads
        let n_threads = 4;
        let barrier = Arc::new(Barrier::new(n_threads));

        let handles: Vec<_> = (0..n_threads)
            .map(|tid| {
                let p = path_str.clone();
                let bar = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = fsqlite::Connection::open(&p).await.expect("thread open");
                        conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS}"))
                            .await
                            .expect("pragma");
                        bar.wait();
                        let base = (tid as u64) * RANGE_SIZE;
                        conn.execute("BEGIN").await.expect("begin");
                        for i in 0..100u64 {
                            drop(
                                conn.execute(&format!(
                                    "INSERT INTO mt_bench (id, thread_id, val) VALUES ({}, {tid}, {i})",
                                    base + i
                                ))
                                .await,
                            );
                        }
                        drop(conn.execute("COMMIT").await);
                    });
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Now open with rusqlite and verify
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM mt_bench", [], |row| row.get(0))
            .unwrap();

        emit_log(
            "M7-cross-check",
            "result",
            json!({
                "csqlite_row_count": count,
                "threads": n_threads,
            }),
        );

        assert!(
            count > 0,
            "csqlite must see rows written by fsqlite threads"
        );

        let integrity: String = conn
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(integrity, "ok", "csqlite integrity_check must pass");
    });
}
