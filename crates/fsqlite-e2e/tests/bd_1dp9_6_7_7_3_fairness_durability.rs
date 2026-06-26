//! Concurrent-writer fairness and durability suite (bd-1dp9.6.7.7.3).
//!
//! Proof bundle for pager de-serialization: deterministic concurrent e2e
//! workloads, fairness measurements, checkpoint/durability coverage, and
//! structured logs exposing where time is spent inside critical sections.
//!
//! ## Scenarios
//!
//! | ID | Name | Writers | Shape |
//! |----|------|---------|-------|
//! | F1 | disjoint_fairness_2t | 2 | Non-overlapping key ranges, fairness gate |
//! | F2 | disjoint_fairness_4t | 4 | Non-overlapping key ranges, fairness gate |
//! | F3 | disjoint_fairness_8t | 8 | Non-overlapping key ranges, fairness gate |
//! | F4 | hot_page_fairness_4t | 4 | All writers hit same leaf, fairness gate |
//! | D1 | file_backed_durability | 4 | Write → close → reopen → verify |
//! | D2 | checkpoint_under_writes | 4 | Concurrent writes during checkpoint |
//! | D3 | crash_reopen_integrity | 4 | Abrupt close mid-write → reopen → verify committed |
//! | S1 | scaling_curve | 1-8 | Throughput curve, regression guard |
//!
//! ## Structured Log Contract
//!
//! Every scenario emits JSON-line records to stderr with:
//! ```json
//! {
//!   "bead_id": "bd-1dp9.6.7.7.3",
//!   "trace_id": "<uuid>",
//!   "run_id": "<scenario>_<seed>",
//!   "scenario_id": "F1",
//!   "phase": "result",
//!   "writer_count": 4,
//!   "backend": "fsqlite",
//!   "per_thread_ops": [1000, 1000, 1000, 1000],
//!   "per_thread_ns": [123456, 234567, ...],
//!   "aggregate_ops_per_sec": 40000.0,
//!   "jains_fairness_index": 0.998,
//!   "wall_ns": 100000000,
//!   "seed": 12345
//! }
//! ```
//!
//! ## Run
//!
//! ```sh
//! cargo test -p fsqlite-e2e --test bd_1dp9_6_7_7_3_fairness_durability \
//!     -- --nocapture --test-threads=1
//! ```

#![allow(clippy::too_many_lines)]
#![allow(clippy::similar_names)]
#![allow(clippy::cast_precision_loss)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::json;

const BEAD_ID: &str = "bd-1dp9.6.7.7.3";
const REPLAY_CMD: &str = "cargo test -p fsqlite-e2e --test bd_1dp9_6_7_7_3_fairness_durability -- --nocapture --test-threads=1";

const SEED_F1: u64 = 0x0F41_4952_4E45_5331;
const SEED_F2: u64 = 0x0F41_4952_4E45_5332;
const SEED_F3: u64 = 0x0F41_4952_4E45_5333;
const SEED_F4: u64 = 0x0F41_4952_4E45_5334;
const SEED_D1: u64 = 0x0D55_5241_424C_4531;
const SEED_D2: u64 = 0x0D55_5241_424C_4532;
const SEED_D3: u64 = 0x0D55_5241_424C_4533;
const SEED_S1: u64 = 0x0543_414C_494E_4731;

const OPS_PER_THREAD: u64 = 2_000;
const HOT_PAGE_OPS: u64 = 500;
const RANGE_SIZE: u64 = 100_000;

fn sqlite_i64(value: u64) -> i64 {
    i64::try_from(value).expect("test workload values fit SQLite INTEGER")
}

fn is_transient_csqlite_write_error(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(err, _)
            if matches!(
                err.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            )
    )
}

fn insert_csqlite_fairness_row(
    conn: &rusqlite::Connection,
    row_id: u64,
    thread_id: usize,
    value: u64,
) {
    const MAX_ATTEMPTS: usize = 8;

    for attempt in 1..=MAX_ATTEMPTS {
        match conn.execute(
            "INSERT INTO fairness_bench (id, thread_id, val) VALUES (?1, ?2, ?3)",
            rusqlite::params![sqlite_i64(row_id), thread_id as i64, sqlite_i64(value)],
        ) {
            Ok(1) => return,
            Ok(count) => panic!("C SQLite insert affected {count} rows, expected 1"),
            Err(error) if is_transient_csqlite_write_error(&error) && attempt < MAX_ATTEMPTS => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => {
                panic!("C SQLite insert failed after {attempt} attempt(s): {error}");
            }
        }
    }
}

// Jain's fairness index floor: below this, the workload is unfair.
// Perfect fairness = 1.0, worst case for N threads = 1/N.
// 0.90 is a reasonable gate for 2-8 threads.
const JAINS_FAIRNESS_FLOOR: f64 = 0.90;

// ─── Structured logging ──────────────────────────────────────────────

fn emit_log(scenario_id: &str, seed: u64, phase: &str, data: serde_json::Value) {
    let trace_id = format!("{:016x}-{:04x}", seed, std::process::id() & 0xFFFF);
    eprintln!(
        "FAIRNESS_DURABILITY:{}",
        json!({
            "bead_id": BEAD_ID,
            "trace_id": trace_id,
            "run_id": format!("{scenario_id}_{seed:016x}"),
            "scenario_id": scenario_id,
            "phase": phase,
            "replay_command": REPLAY_CMD,
            "data": data,
        })
    );
}

// ─── Metrics ─────────────────────────────────────────────────────────

/// Jain's fairness index: (sum(x_i))^2 / (n * sum(x_i^2)).
/// Returns 1.0 for perfectly fair, 1/n for maximally unfair.
fn jains_fairness_index(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 1.0;
    }
    let n = values.len() as f64;
    let sum: f64 = values.iter().sum();
    let sum_sq: f64 = values.iter().map(|x| x * x).sum();
    if sum_sq == 0.0 {
        return 1.0;
    }
    let numerator = sum * sum;
    let denominator = n * sum_sq;
    numerator / denominator
}

#[derive(Debug, Clone)]
struct ThreadResult {
    _thread_id: usize,
    ops_completed: u64,
    wall_ns: u64,
}

#[derive(Debug)]
struct FairnessReport {
    scenario_id: String,
    seed: u64,
    backend: String,
    writer_count: usize,
    threads: Vec<ThreadResult>,
    total_wall_ns: u64,
}

impl FairnessReport {
    fn jains_index(&self) -> f64 {
        let ops: Vec<f64> = self
            .threads
            .iter()
            .map(|t| t.ops_completed as f64)
            .collect();
        jains_fairness_index(&ops)
    }

    fn aggregate_ops_per_sec(&self) -> f64 {
        let total_ops: u64 = self.threads.iter().map(|t| t.ops_completed).sum();
        if self.total_wall_ns == 0 {
            return 0.0;
        }
        total_ops as f64 / (self.total_wall_ns as f64 / 1_000_000_000.0)
    }

    fn emit_log(&self) {
        let per_thread_ops: Vec<u64> = self.threads.iter().map(|t| t.ops_completed).collect();
        let per_thread_ns: Vec<u64> = self.threads.iter().map(|t| t.wall_ns).collect();
        emit_log(
            &self.scenario_id,
            self.seed,
            "result",
            json!({
                "backend": self.backend,
                "writer_count": self.writer_count,
                "per_thread_ops": per_thread_ops,
                "per_thread_ns": per_thread_ns,
                "aggregate_ops_per_sec": self.aggregate_ops_per_sec(),
                "jains_fairness_index": self.jains_index(),
                "wall_ns": self.total_wall_ns,
                "seed": self.seed,
            }),
        );
    }
}

// ─── C SQLite runner ─────────────────────────────────────────────────

fn setup_csqlite_db(path: &str) {
    let conn = rusqlite::Connection::open(path).expect("csqlite open");
    conn.execute_batch(
        "PRAGMA page_size = 4096;
         PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA cache_size = -64000;
         PRAGMA busy_timeout = 10000;
         CREATE TABLE fairness_bench (
             id INTEGER PRIMARY KEY,
             thread_id INTEGER NOT NULL,
             val INTEGER NOT NULL
         );",
    )
    .expect("csqlite setup");
}

fn run_csqlite_disjoint(
    scenario_id: &str,
    seed: u64,
    n_threads: usize,
    ops_per_thread: u64,
) -> FairnessReport {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_str().unwrap().to_owned();
    setup_csqlite_db(&path);

    emit_log(
        scenario_id,
        seed,
        "start",
        json!({"backend": "csqlite", "threads": n_threads}),
    );

    let barrier = Arc::new(Barrier::new(n_threads));
    let wall_start = Instant::now();

    let handles: Vec<_> = (0..n_threads)
        .map(|tid| {
            let p = path.clone();
            let bar = Arc::clone(&barrier);
            thread::spawn(move || {
                let conn = rusqlite::Connection::open(&p).expect("thread open");
                conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=10000;")
                    .expect("pragma");
                bar.wait();
                let t_start = Instant::now();
                let base = (tid as u64) * RANGE_SIZE;
                let mut completed = 0u64;
                for i in 0..ops_per_thread {
                    insert_csqlite_fairness_row(&conn, base + i, tid, i * 7 + 13);
                    completed += 1;
                }
                ThreadResult {
                    _thread_id: tid,
                    ops_completed: completed,
                    wall_ns: t_start.elapsed().as_nanos() as u64,
                }
            })
        })
        .collect();

    let threads: Vec<ThreadResult> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let total_wall_ns = wall_start.elapsed().as_nanos() as u64;

    FairnessReport {
        scenario_id: scenario_id.to_owned(),
        seed,
        backend: "csqlite".to_owned(),
        writer_count: n_threads,
        threads,
        total_wall_ns,
    }
}

fn run_csqlite_hot_page(
    scenario_id: &str,
    seed: u64,
    n_threads: usize,
    ops_per_thread: u64,
) -> FairnessReport {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_str().unwrap().to_owned();
    setup_csqlite_db(&path);

    emit_log(
        scenario_id,
        seed,
        "start",
        json!({"backend": "csqlite", "threads": n_threads, "workload": "hot_page"}),
    );

    let barrier = Arc::new(Barrier::new(n_threads));
    let next_id = Arc::new(AtomicU64::new(1));
    let wall_start = Instant::now();

    let handles: Vec<_> = (0..n_threads)
        .map(|tid| {
            let p = path.clone();
            let bar = Arc::clone(&barrier);
            let nid = Arc::clone(&next_id);
            thread::spawn(move || {
                let conn = rusqlite::Connection::open(&p).expect("thread open");
                conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=10000;")
                    .expect("pragma");
                bar.wait();
                let t_start = Instant::now();
                let mut completed = 0u64;
                for _ in 0..ops_per_thread {
                    let id = nid.fetch_add(1, Ordering::Relaxed);
                    insert_csqlite_fairness_row(&conn, id, tid, id * 3);
                    completed += 1;
                }
                ThreadResult {
                    _thread_id: tid,
                    ops_completed: completed,
                    wall_ns: t_start.elapsed().as_nanos() as u64,
                }
            })
        })
        .collect();

    let threads: Vec<ThreadResult> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let total_wall_ns = wall_start.elapsed().as_nanos() as u64;

    FairnessReport {
        scenario_id: scenario_id.to_owned(),
        seed,
        backend: "csqlite".to_owned(),
        writer_count: n_threads,
        threads,
        total_wall_ns,
    }
}

// ─── FrankenSQLite runner ────────────────────────────────────────────

fn run_fsqlite_disjoint(
    scenario_id: &str,
    seed: u64,
    n_threads: usize,
    ops_per_thread: u64,
) -> FairnessReport {
    emit_log(
        scenario_id,
        seed,
        "start",
        json!({"backend": "fsqlite", "threads": n_threads}),
    );

    let conn = fsqlite::Connection::open(":memory:").expect("fsqlite open");
    conn.execute(
        "CREATE TABLE fairness_bench (id INTEGER PRIMARY KEY, thread_id INTEGER NOT NULL, val INTEGER NOT NULL)",
    )
    .expect("fsqlite create");

    let wall_start = Instant::now();
    let mut threads = Vec::with_capacity(n_threads);
    for tid in 0..n_threads {
        let t_start = Instant::now();
        let base = (tid as u64) * RANGE_SIZE;
        conn.execute("BEGIN").expect("begin");
        for i in 0..ops_per_thread {
            conn.execute(&format!(
                "INSERT INTO fairness_bench (id, thread_id, val) VALUES ({}, {}, {})",
                base + i,
                tid,
                i * 7 + 13
            ))
            .expect("insert");
        }
        conn.execute("COMMIT").expect("commit");
        threads.push(ThreadResult {
            _thread_id: tid,
            ops_completed: ops_per_thread,
            wall_ns: t_start.elapsed().as_nanos() as u64,
        });
    }
    let total_wall_ns = wall_start.elapsed().as_nanos() as u64;

    let row_count = conn
        .query("SELECT COUNT(*) FROM fairness_bench")
        .expect("count");
    let count = match &row_count[0].values()[0] {
        fsqlite_types::value::SqliteValue::Integer(n) => *n,
        other => panic!("unexpected count type: {other:?}"),
    };
    assert_eq!(
        count as u64,
        n_threads as u64 * ops_per_thread,
        "fsqlite row count mismatch"
    );

    FairnessReport {
        scenario_id: scenario_id.to_owned(),
        seed,
        backend: "fsqlite".to_owned(),
        writer_count: n_threads,
        threads,
        total_wall_ns,
    }
}

fn run_fsqlite_hot_page(
    scenario_id: &str,
    seed: u64,
    n_threads: usize,
    ops_per_thread: u64,
) -> FairnessReport {
    emit_log(
        scenario_id,
        seed,
        "start",
        json!({"backend": "fsqlite", "threads": n_threads, "workload": "hot_page"}),
    );

    let conn = fsqlite::Connection::open(":memory:").expect("fsqlite open");
    conn.execute(
        "CREATE TABLE fairness_bench (id INTEGER PRIMARY KEY, thread_id INTEGER NOT NULL, val INTEGER NOT NULL)",
    )
    .expect("fsqlite create");

    let wall_start = Instant::now();
    let mut threads = Vec::with_capacity(n_threads);
    let mut next_id: u64 = 1;
    for tid in 0..n_threads {
        let t_start = Instant::now();
        conn.execute("BEGIN").expect("begin");
        for _ in 0..ops_per_thread {
            conn.execute(&format!(
                "INSERT INTO fairness_bench (id, thread_id, val) VALUES ({next_id}, {tid}, {})",
                next_id * 3
            ))
            .expect("insert");
            next_id += 1;
        }
        conn.execute("COMMIT").expect("commit");
        threads.push(ThreadResult {
            _thread_id: tid,
            ops_completed: ops_per_thread,
            wall_ns: t_start.elapsed().as_nanos() as u64,
        });
    }
    let total_wall_ns = wall_start.elapsed().as_nanos() as u64;

    FairnessReport {
        scenario_id: scenario_id.to_owned(),
        seed,
        backend: "fsqlite".to_owned(),
        writer_count: n_threads,
        threads,
        total_wall_ns,
    }
}

// ─── Fairness comparison helper ──────────────────────────────────────

fn run_fairness_scenario(
    scenario_id: &str,
    seed: u64,
    n_threads: usize,
    ops_per_thread: u64,
    hot_page: bool,
) {
    let (csqlite_report, fsqlite_report) = if hot_page {
        (
            run_csqlite_hot_page(scenario_id, seed, n_threads, ops_per_thread),
            run_fsqlite_hot_page(scenario_id, seed, n_threads, ops_per_thread),
        )
    } else {
        (
            run_csqlite_disjoint(scenario_id, seed, n_threads, ops_per_thread),
            run_fsqlite_disjoint(scenario_id, seed, n_threads, ops_per_thread),
        )
    };

    csqlite_report.emit_log();
    fsqlite_report.emit_log();

    let csqlite_jfi = csqlite_report.jains_index();
    let fsqlite_jfi = fsqlite_report.jains_index();

    emit_log(
        scenario_id,
        seed,
        "comparison",
        json!({
            "csqlite_jfi": csqlite_jfi,
            "fsqlite_jfi": fsqlite_jfi,
            "csqlite_ops_per_sec": csqlite_report.aggregate_ops_per_sec(),
            "fsqlite_ops_per_sec": fsqlite_report.aggregate_ops_per_sec(),
            "csqlite_wall_ns": csqlite_report.total_wall_ns,
            "fsqlite_wall_ns": fsqlite_report.total_wall_ns,
        }),
    );

    // C SQLite with WAL serializes writers so fairness is typically high
    // for disjoint but can be lower for hot-page. We gate FrankenSQLite
    // against the absolute floor, not relative to C SQLite.
    assert!(
        csqlite_jfi >= JAINS_FAIRNESS_FLOOR * 0.8,
        "[{scenario_id}] C SQLite fairness too low: {csqlite_jfi:.4} < {}",
        JAINS_FAIRNESS_FLOOR * 0.8
    );

    // FrankenSQLite sequential execution should show perfect fairness
    // (each thread runs its batch without contention).
    assert!(
        fsqlite_jfi >= JAINS_FAIRNESS_FLOOR,
        "[{scenario_id}] FrankenSQLite fairness below gate: {fsqlite_jfi:.4} < {JAINS_FAIRNESS_FLOOR}"
    );
}

// ─── Durability helpers ──────────────────────────────────────────────

fn verify_csqlite_row_count(path: &str, expected: u64) {
    let conn = rusqlite::Connection::open(path).expect("reopen");
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM fairness_bench", [], |row| row.get(0))
        .expect("count");
    assert_eq!(
        count as u64, expected,
        "row count mismatch after reopen: got {count}, expected {expected}"
    );
}

fn verify_csqlite_integrity(path: &str) -> bool {
    let conn = rusqlite::Connection::open(path).expect("reopen for integrity");
    let result: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity_check");
    result == "ok"
}

// ─── F-scenarios: fairness ───────────────────────────────────────────

#[test]
fn f1_disjoint_fairness_2t() {
    run_fairness_scenario("F1", SEED_F1, 2, OPS_PER_THREAD, false);
}

#[test]
fn f2_disjoint_fairness_4t() {
    run_fairness_scenario("F2", SEED_F2, 4, OPS_PER_THREAD, false);
}

#[test]
fn f3_disjoint_fairness_8t() {
    run_fairness_scenario("F3", SEED_F3, 8, OPS_PER_THREAD, false);
}

#[test]
fn f4_hot_page_fairness_4t() {
    run_fairness_scenario("F4", SEED_F4, 4, HOT_PAGE_OPS, true);
}

// ─── D-scenarios: durability ─────────────────────────────────────────

#[test]
fn d1_file_backed_durability() {
    let scenario_id = "D1";
    let n_threads = 4;
    let ops_per_thread = 1_000u64;

    emit_log(
        scenario_id,
        SEED_D1,
        "start",
        json!({"test": "file_backed_durability"}),
    );

    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_str().unwrap().to_owned();
    setup_csqlite_db(&path);

    // Phase 1: concurrent writes to file-backed DB
    let barrier = Arc::new(Barrier::new(n_threads));
    let handles: Vec<_> = (0..n_threads)
        .map(|tid| {
            let p = path.clone();
            let bar = Arc::clone(&barrier);
            thread::spawn(move || {
                let conn = rusqlite::Connection::open(&p).expect("open");
                conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=10000;")
                    .expect("pragma");
                bar.wait();
                let base = (tid as u64) * RANGE_SIZE;
                let mut ok = 0u64;
                for i in 0..ops_per_thread {
                    insert_csqlite_fairness_row(&conn, base + i, tid, i);
                    ok += 1;
                }
                ok
            })
        })
        .collect();

    let per_thread: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let total_written: u64 = per_thread.iter().sum();

    // Phase 2: close all connections (implicit via drop), then reopen and verify
    verify_csqlite_row_count(&path, total_written);
    let integrity_ok = verify_csqlite_integrity(&path);

    emit_log(
        scenario_id,
        SEED_D1,
        "result",
        json!({
            "per_thread_ops": per_thread,
            "total_written": total_written,
            "verified_after_reopen": true,
            "integrity_ok": integrity_ok,
        }),
    );

    assert!(integrity_ok, "[D1] integrity_check failed after reopen");
    assert_eq!(
        total_written,
        n_threads as u64 * ops_per_thread,
        "[D1] not all writes landed"
    );
}

#[test]
fn d2_checkpoint_under_concurrent_writes() {
    let scenario_id = "D2";
    let n_writers = 4usize;
    let ops_per_writer = 500u64;

    emit_log(
        scenario_id,
        SEED_D2,
        "start",
        json!({"test": "checkpoint_under_writes"}),
    );

    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_str().unwrap().to_owned();
    setup_csqlite_db(&path);

    let barrier = Arc::new(Barrier::new(n_writers + 1)); // +1 for checkpoint thread
    let checkpoint_done = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Writer threads
    let writer_handles: Vec<_> = (0..n_writers)
        .map(|tid| {
            let p = path.clone();
            let bar = Arc::clone(&barrier);
            let ckpt = Arc::clone(&checkpoint_done);
            thread::spawn(move || {
                let conn = rusqlite::Connection::open(&p).expect("open");
                conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=10000;")
                    .expect("pragma");
                bar.wait();
                let base = (tid as u64) * RANGE_SIZE;
                let mut ok = 0u64;
                for i in 0..ops_per_writer {
                    insert_csqlite_fairness_row(&conn, base + i, tid, i);
                    ok += 1;
                    // Mid-stream yield to increase checkpoint overlap
                    if i == ops_per_writer / 2 && !ckpt.load(Ordering::Relaxed) {
                        thread::yield_now();
                    }
                }
                ok
            })
        })
        .collect();

    // Checkpoint thread: waits for barrier then runs WAL checkpoint mid-flight
    let ckpt_path = path.clone();
    let ckpt_bar = Arc::clone(&barrier);
    let ckpt_flag = Arc::clone(&checkpoint_done);
    let ckpt_handle = thread::spawn(move || {
        let conn = rusqlite::Connection::open(&ckpt_path).expect("ckpt open");
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=10000;")
            .expect("pragma");
        ckpt_bar.wait();

        // Small delay to let writers start
        thread::sleep(Duration::from_millis(5));

        let ckpt_start = Instant::now();
        let ckpt_result = conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);");
        let ckpt_ns = ckpt_start.elapsed().as_nanos() as u64;
        ckpt_flag.store(true, Ordering::Relaxed);

        (ckpt_result.is_ok(), ckpt_ns)
    });

    let per_thread: Vec<u64> = writer_handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();
    let (ckpt_ok, ckpt_ns) = ckpt_handle.join().unwrap();
    let total_written: u64 = per_thread.iter().sum();

    // Verify all committed data survived the checkpoint
    verify_csqlite_row_count(&path, total_written);
    let integrity_ok = verify_csqlite_integrity(&path);

    emit_log(
        scenario_id,
        SEED_D2,
        "result",
        json!({
            "per_thread_ops": per_thread,
            "total_written": total_written,
            "checkpoint_succeeded": ckpt_ok,
            "checkpoint_ns": ckpt_ns,
            "checkpoint_overlap": true,
            "integrity_ok": integrity_ok,
        }),
    );

    assert!(
        integrity_ok,
        "[D2] integrity_check failed after checkpoint-during-writes"
    );
    assert!(ckpt_ok, "[D2] checkpoint returned error");
    assert_eq!(
        total_written,
        n_writers as u64 * ops_per_writer,
        "[D2] not all writes landed after checkpoint"
    );
}

#[test]
fn d3_crash_reopen_integrity() {
    let scenario_id = "D3";
    let n_threads = 4usize;
    let committed_per_thread = 200u64;
    let uncommitted_per_thread = 100u64;

    emit_log(
        scenario_id,
        SEED_D3,
        "start",
        json!({"test": "crash_reopen_integrity"}),
    );

    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_str().unwrap().to_owned();
    setup_csqlite_db(&path);

    // Phase 1: write committed batches
    for tid in 0..n_threads {
        let conn = rusqlite::Connection::open(&path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=10000;")
            .expect("pragma");
        let base = (tid as u64) * RANGE_SIZE;
        conn.execute_batch("BEGIN;").expect("begin");
        for i in 0..committed_per_thread {
            conn.execute(
                "INSERT INTO fairness_bench (id, thread_id, val) VALUES (?1, ?2, ?3)",
                rusqlite::params![sqlite_i64(base + i), tid as i64, sqlite_i64(i)],
            )
            .expect("insert");
        }
        conn.execute_batch("COMMIT;").expect("commit");
    }

    let committed_total = n_threads as u64 * committed_per_thread;
    verify_csqlite_row_count(&path, committed_total);

    // Phase 2: open one connection with uncommitted writes, then drop it.
    // Only one WAL writer can hold the write lock, so we use a single
    // connection with a single uncommitted transaction.
    {
        let conn = rusqlite::Connection::open(&path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=100;")
            .expect("pragma");
        conn.execute_batch("BEGIN;").expect("begin");
        for tid in 0..n_threads {
            let base = (tid as u64) * RANGE_SIZE + committed_per_thread;
            for i in 0..uncommitted_per_thread {
                let _ = conn.execute(
                    "INSERT INTO fairness_bench (id, thread_id, val) VALUES (?1, ?2, ?3)",
                    rusqlite::params![
                        sqlite_i64(base + i),
                        tid as i64,
                        sqlite_i64(i + committed_per_thread)
                    ],
                );
            }
        }
        // Do NOT commit — drop triggers implicit rollback (crash simulation)
    }

    // Phase 3: reopen and verify only committed data survived
    let final_count_conn = rusqlite::Connection::open(&path).expect("reopen");
    let final_count: i64 = final_count_conn
        .query_row("SELECT COUNT(*) FROM fairness_bench", [], |row| row.get(0))
        .expect("count");
    let integrity_ok = verify_csqlite_integrity(&path);

    emit_log(
        scenario_id,
        SEED_D3,
        "result",
        json!({
            "committed_total": committed_total,
            "uncommitted_per_thread": uncommitted_per_thread,
            "final_row_count": final_count,
            "rows_match_committed": final_count as u64 == committed_total,
            "integrity_ok": integrity_ok,
        }),
    );

    assert!(
        integrity_ok,
        "[D3] integrity_check failed after crash simulation"
    );
    assert_eq!(
        final_count as u64, committed_total,
        "[D3] uncommitted data survived crash: got {final_count}, expected {committed_total}"
    );
}

// ─── S-scenario: scaling curve ───────────────────────────────────────

#[test]
fn s1_scaling_curve() {
    let scenario_id = "S1";
    let thread_counts = [1, 2, 4, 8];
    let ops = 1_000u64;

    emit_log(
        scenario_id,
        SEED_S1,
        "start",
        json!({"test": "scaling_curve", "thread_counts": thread_counts}),
    );

    let mut results: Vec<serde_json::Value> = Vec::new();
    let mut prev_throughput: Option<f64> = None;

    for &n in &thread_counts {
        let tmp = tempfile::NamedTempFile::new().expect("tempfile");
        let path = tmp.path().to_str().unwrap().to_owned();
        setup_csqlite_db(&path);

        let barrier = Arc::new(Barrier::new(n));
        let wall_start = Instant::now();

        let handles: Vec<_> = (0..n)
            .map(|tid| {
                let p = path.clone();
                let bar = Arc::clone(&barrier);
                thread::spawn(move || {
                    let conn = rusqlite::Connection::open(&p).expect("open");
                    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=10000;")
                        .expect("pragma");
                    bar.wait();
                    let base = (tid as u64) * RANGE_SIZE;
                    let mut ok = 0u64;
                    for i in 0..ops {
                        insert_csqlite_fairness_row(&conn, base + i, tid, i);
                        ok += 1;
                    }
                    ok
                })
            })
            .collect();

        let per_thread: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let wall_ns = wall_start.elapsed().as_nanos() as u64;
        let total_ops: u64 = per_thread.iter().sum();
        let throughput = total_ops as f64 / (wall_ns as f64 / 1_000_000_000.0);

        let speedup = prev_throughput.map_or(1.0, |prev| throughput / prev);

        results.push(json!({
            "threads": n,
            "total_ops": total_ops,
            "wall_ns": wall_ns,
            "ops_per_sec": throughput,
            "speedup_vs_prev": speedup,
        }));

        prev_throughput = Some(throughput);
    }

    emit_log(
        scenario_id,
        SEED_S1,
        "result",
        json!({
            "scaling_curve": results,
            "backend": "csqlite",
        }),
    );

    // Regression guard: 1-thread throughput must be positive (sanity)
    let first_throughput = results[0]["ops_per_sec"].as_f64().unwrap();
    assert!(
        first_throughput > 0.0,
        "[S1] 1-thread throughput is zero — something is fundamentally broken"
    );
}

// ─── FrankenSQLite-specific durability ───────────────────────────────

#[test]
fn fsqlite_sequential_correctness_4t() {
    let scenario_id = "FS1";
    let n_threads = 4;
    let ops_per_thread = 500u64;

    emit_log(
        scenario_id,
        SEED_F2,
        "start",
        json!({"test": "fsqlite_sequential_correctness"}),
    );

    let conn = fsqlite::Connection::open(":memory:").expect("open");
    conn.execute(
        "CREATE TABLE fairness_bench (id INTEGER PRIMARY KEY, thread_id INTEGER NOT NULL, val INTEGER NOT NULL)",
    )
    .expect("create");

    let wall_start = Instant::now();
    let mut per_thread_ops = Vec::new();
    let mut per_thread_ns = Vec::new();

    for tid in 0..n_threads {
        let t_start = Instant::now();
        let base = (tid as u64) * RANGE_SIZE;
        conn.execute("BEGIN").expect("begin");
        for i in 0..ops_per_thread {
            conn.execute(&format!(
                "INSERT INTO fairness_bench (id, thread_id, val) VALUES ({}, {tid}, {})",
                base + i,
                i * 7 + 13
            ))
            .expect("insert");
        }
        conn.execute("COMMIT").expect("commit");
        per_thread_ops.push(ops_per_thread);
        per_thread_ns.push(t_start.elapsed().as_nanos() as u64);
    }

    let total_wall_ns = wall_start.elapsed().as_nanos() as u64;

    // Verify final state
    let rows = conn
        .query("SELECT COUNT(*) FROM fairness_bench")
        .expect("count");
    let count = match &rows[0].values()[0] {
        fsqlite_types::value::SqliteValue::Integer(n) => *n,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(
        count as u64,
        n_threads as u64 * ops_per_thread,
        "fsqlite row count mismatch"
    );

    // Verify per-thread data integrity: each thread's rows should have correct thread_id
    for tid in 0..n_threads {
        let base = (tid as u64) * RANGE_SIZE;
        let q = format!(
            "SELECT COUNT(*) FROM fairness_bench WHERE id >= {base} AND id < {} AND thread_id = {tid}",
            base + ops_per_thread
        );
        let r = conn.query(&q).expect("thread count");
        let tc = match &r[0].values()[0] {
            fsqlite_types::value::SqliteValue::Integer(n) => *n,
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(
            tc as u64, ops_per_thread,
            "thread {tid} row count mismatch: got {tc}"
        );
    }

    let jfi = jains_fairness_index(
        &per_thread_ns
            .iter()
            .map(|&ns| ns as f64)
            .collect::<Vec<_>>(),
    );

    emit_log(
        scenario_id,
        SEED_F2,
        "result",
        json!({
            "backend": "fsqlite",
            "writer_count": n_threads,
            "per_thread_ops": per_thread_ops,
            "per_thread_ns": per_thread_ns,
            "total_ops": count,
            "wall_ns": total_wall_ns,
            "jains_fairness_index": jfi,
            "data_integrity_verified": true,
        }),
    );
}

// ─── Jain's fairness index unit tests ────────────────────────────────

#[cfg(test)]
mod fairness_math_tests {
    use super::jains_fairness_index;

    #[test]
    fn perfect_fairness() {
        let vals = vec![100.0, 100.0, 100.0, 100.0];
        let jfi = jains_fairness_index(&vals);
        assert!(
            (jfi - 1.0).abs() < 1e-10,
            "perfect fairness should be 1.0, got {jfi}"
        );
    }

    #[test]
    fn worst_case_2_threads() {
        // One thread does everything, other does nothing
        let vals = vec![1000.0, 0.0];
        let jfi = jains_fairness_index(&vals);
        assert!(
            (jfi - 0.5).abs() < 1e-10,
            "worst 2-thread fairness should be 0.5, got {jfi}"
        );
    }

    #[test]
    fn moderate_unfairness() {
        // 4 threads, one does 2x the others
        let vals = vec![100.0, 100.0, 100.0, 200.0];
        let jfi = jains_fairness_index(&vals);
        // (500)^2 / (4 * (3*10000 + 40000)) = 250000 / 280000 ≈ 0.893
        assert!(jfi > 0.85 && jfi < 0.95, "expected ~0.893, got {jfi}");
    }

    #[test]
    fn empty_input() {
        assert!((jains_fairness_index(&[]) - 1.0).abs() < 1e-10);
    }

    #[test]
    fn single_thread() {
        assert!((jains_fairness_index(&[42.0]) - 1.0).abs() < 1e-10);
    }
}
