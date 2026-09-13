//! bd-odyb1 — GH #335 keeper: a `PRAGMA wal_checkpoint(TRUNCATE)` racing a live
//! writer must not make a freshly-opened connection's FIRST autocommit `SELECT`
//! surface a transient `BusySnapshot`.
//!
//! Root cause (connection.rs:~83884): a fresh connection binds a pager
//! publication at visibility-seq N (pre-TRUNCATE), then its memdb reload
//! observes seq M (post-TRUNCATE); `M != N` raises `FrankenError::BusySnapshot`
//! ("pager publication advanced between metadata bind and reload transaction").
//! This is a LEGITIMATE transient straddle of an external TRUNCATE — the durable
//! image is fine — but pre-fix the autocommit READ path had no retry, so the
//! straddle surfaced to the caller. The GH #333 autocommit retry loop in
//! `execute_statement_after_background_status` was armed only for PRAGMA; the fix
//! (bd-odyb1) extends its arming match to `Statement::Select(_)` so an autocommit
//! SELECT re-executes on the same busy-timeout handoff (retryability keyed on
//! `FrankenError::is_transient()`, covering BusySnapshot | Busy | BusyRecovery |
//! DatabaseLocked | WriteConflict | SerializationFailure |
//! PageBufferCapacityExhausted). Re-executing a SELECT at an autocommit boundary
//! is idempotent (no side effects; the failed dispatch left no partial state).
//!
//! REPRO / EXPECTED SIGNAL:
//!   * pre-fix:  the fresh connection's first SELECT fails with
//!    `Database(BusySnapshot { .. })` on a large fraction of
//!    iterations (the standalone repro reported ~21/30).
//!   * post-fix: 0/N failed reads.
//! Because this is a genuine data race, one green run is not proof — the loop
//! below runs `ITERATIONS` (60) fresh-open→first-SELECT attempts while a writer
//! and a TRUNCATE-checkpointer race the same file.
//!
//! TOPOLOGY (real 3-connection concurrency — this is why the pre-existing
//! `concurrent_checkpoint_oracle_e2e.rs` never reproduced #335: it drives
//! TRUNCATE sequentially on a single connection):
//!   * connection A (writer):       autocommit INSERT in a loop.
//!   * connection B (checkpointer): `PRAGMA wal_checkpoint(TRUNCATE)` in a loop.
//!   * connection C (reader):       per iteration, a FRESHLY-OPENED connection
//!    issues its FIRST statement — a `SELECT` —
//!    and must return `Ok`, never `BusySnapshot`.
//! Each connection lives on its own OS thread with a private current-thread
//! asupersync runtime (mirrors the GH #333 keeper's runtime/spawn pattern).
//!
//! HARNESS NOTE — antagonist pacing: the writer and checkpointer are paced with
//! a sub-millisecond yield between operations. Unpaced tight loops (three
//! CPU-bound OS threads plus libtest) starve the fresh reader for scheduler
//! time and lock acquisition to the point where a single fresh open can stall
//! for tens of seconds — a CPU/lock-starvation artifact of the harness, not the
//! #335 straddle (which is a transient error, not a stall). The pacing keeps the
//! TRUNCATE↔writer↔fresh-open race hot while leaving the reader enough room to
//! make progress, so the test measures the straddle the fix targets.
//!
//! LOAD CAVEAT: the reader absorbs the straddle through the connection
//! `busy_timeout` budget (default 5 s per statement). Under extreme host load a
//! retry window could in principle exhaust, reading as a red without an engine
//! regression; before treating a red run as a regression, rerun this file
//! standalone on an unloaded host. Each iteration is bounded by a wall-clock
//! ceiling and a shared phase marker, so a genuine wedge is reported as a
//! failure naming the phase (open/select/close) rather than stalling CI.

use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use asupersync::runtime::RuntimeBuilder;

/// Fresh-open → first-SELECT attempts.
const ITERATIONS: u32 = 60;

/// Per-iteration wall-clock ceiling. A healthy iteration completes in
/// milliseconds; the worst legitimate case is one busy_timeout window (5 s).
/// A ceiling breach converts a hung phase into a bounded, diagnosed failure
/// instead of a CI stall.
const ITERATION_TIMEOUT: Duration = Duration::from_secs(20);

/// Sub-millisecond antagonist pacing (see HARNESS NOTE above).
const WRITER_PACE: Duration = Duration::from_micros(150);
const CHECKPOINT_PACE: Duration = Duration::from_micros(400);

const CHILD_MODE: &str = "FSQLITE_GH335_CHILD_MODE";
const CHILD_DIR: &str = "FSQLITE_GH335_CHILD_DIR";
const KEEPER_NAME: &str = "gh335_checkpoint_truncate_never_fails_fresh_reader_first_select";
// Covers seed, worker readiness, all 60 receives, and shutdown. The outer
// process owns the fixture, so killing a wedged child cannot leak its threads
// into another libtest case or remove files while those threads still use them.
const LIFECYCLE_TIMEOUT: Duration = Duration::from_mins(21);

fn supervise_campaign(mode: &str, timeout: Duration) -> Result<(), String> {
    let dir = tempfile::tempdir().map_err(|error| error.to_string())?;
    let stdout_path = dir.path().join("stdout");
    let stderr_path = dir.path().join("stderr");
    let stdout = std::fs::File::create(&stdout_path).map_err(|error| error.to_string())?;
    let stderr = std::fs::File::create(&stderr_path).map_err(|error| error.to_string())?;
    let started = Instant::now();
    let mut child = Command::new(std::env::current_exe().map_err(|error| error.to_string())?)
        .args([
            KEEPER_NAME,
            "--exact",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_MODE, mode)
        .env(CHILD_DIR, dir.path())
        // Files avoid pipe-buffer deadlock and keep nested libtest summaries
        // out of the parent test transcript.
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .map_err(|error| error.to_string())?;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if started.elapsed() < timeout => {
                std::thread::sleep(Duration::from_millis(10));
            }
            result => {
                let kill = child.kill();
                let reap = child.wait();
                return Err(format!(
                    "GH335 lifecycle timeout or wait failure: mode={mode} poll={result:?} \
                     child_started={} kill={kill:?} reap={reap:?}; stderr={:?}",
                    dir.path().join("started").is_file(),
                    std::fs::read_to_string(&stderr_path).unwrap_or_default(),
                ));
            }
        }
    };
    let completion = std::fs::read(dir.path().join("complete")).unwrap_or_default();
    if !status.success() || completion != b"gh335:60:complete" {
        return Err(format!(
            "GH335 child failed or did not complete: mode={mode} status={status}; \
             stderr={:?}; stdout={:?}",
            std::fs::read_to_string(&stderr_path).unwrap_or_default(),
            std::fs::read_to_string(&stdout_path).unwrap_or_default(),
        ));
    }
    Ok(())
}

fn require_progress(before: (u64, u64), after: (u64, u64)) -> Result<(), String> {
    if after.0 <= before.0 || after.1 <= before.1 {
        return Err(format!(
            "no antagonist progress: writes {} -> {}, nonempty TRUNCATEs {} -> {}",
            before.0, after.0, before.1, after.1,
        ));
    }
    Ok(())
}

#[test]
fn gh335_supervisor_rejects_failed_or_incomplete_children() {
    for (mode, expected) in [
        ("fail", "intentional child failure"),
        ("return_early", "did not complete"),
        ("no_writer", "no antagonist progress"),
        ("no_checkpoint", "no antagonist progress"),
    ] {
        let error = supervise_campaign(mode, Duration::from_secs(60))
            .expect_err("an incomplete or failed child must not qualify the campaign");
        assert!(error.contains(expected), "{error}");
    }
}

#[test]
fn gh335_supervisor_reaps_timed_out_child() {
    let error = supervise_campaign("hang", Duration::from_secs(5))
        .expect_err("a child stuck in its lifecycle must be terminated");
    assert!(error.contains("lifecycle timeout"), "{error}");
    assert!(error.contains("child_started=true"), "{error}");
    assert!(error.contains("reap=Ok("), "{error}");
}

// Reader phase markers, published to a shared atomic so a wedge names its phase.
const PHASE_IDLE: u8 = 0;
const PHASE_OPEN: u8 = 1;
const PHASE_SELECT: u8 = 2;
const PHASE_CLOSE: u8 = 3;

fn phase_name(p: u8) -> &'static str {
    match p {
        PHASE_OPEN => "open",
        PHASE_SELECT => "first SELECT",
        PHASE_CLOSE => "close",
        _ => "idle",
    }
}

// JOINT #335 + bd-b4u1r guard — committed #[ignore]d (red by design until the
// bd-b4u1r open-path interlock lands). A/B evidence (phase-instrumented, 60 iters):
//   * PRE-FIX (Pragma-only retry): SELECT-phase fails BusySnapshot (iter 1) AND
//     plain Busy (iters 6,8 — the bd-b4u1r symptom), plus a phase=`open` WEDGE.
//   * WITH the #335 read-retry (Statement::Select added to the autocommit retry
//     arming, keyed on is_transient()): 0 SELECT-phase failures — every
//     BusySnapshot/Busy is absorbed. The ONLY residual is the phase=`open` WEDGE
//     (~1/40 fresh Connection::open blocks ~20s during a concurrent
//     wal_checkpoint(TRUNCATE)) — a recovery-fence/header-rewrite contention on
//     the OPEN path, NOT the statement retry loop (which is busy_timeout-bounded).
// That open wedge is bd-b4u1r / GH#367 (recovery-fence contention -> pool
// checkout-validation) and is fixed by that lane's header-rewrite interlock, not
// by this connection-layer read retry. Flip off #[ignore] once the interlock
// lands to prove BOTH fixes compose.
#[ignore = "joint #335+bd-b4u1r guard: #335 read-retry verified to kill SELECT-phase BusySnapshot/Busy (3->0); full green needs the bd-b4u1r fresh-open-during-TRUNCATE interlock to remove the open-phase wedge"]
#[test]
fn gh335_checkpoint_truncate_never_fails_fresh_reader_first_select() {
    let Ok(mode) = std::env::var(CHILD_MODE) else {
        supervise_campaign("campaign", LIFECYCLE_TIMEOUT).unwrap_or_else(|error| panic!("{error}"));
        return;
    };
    let directory = std::env::var_os(CHILD_DIR).expect("supervisor fixture directory");
    let directory = Path::new(&directory);
    std::fs::write(directory.join("started"), b"started").expect("child start marker");
    match mode.as_str() {
        "campaign" => run_campaign(directory, false, false),
        "fail" => panic!("intentional child failure"),
        "return_early" => return,
        "hang" => loop {
            std::thread::sleep(Duration::from_secs(60));
        },
        "no_writer" => run_campaign(directory, true, false),
        "no_checkpoint" => run_campaign(directory, false, true),
        _ => panic!("unknown child mode: {mode}"),
    }
    std::fs::write(directory.join("complete"), b"gh335:60:complete")
        .expect("completed lifecycle marker");
}

fn run_campaign(directory: &Path, suppress_writer: bool, suppress_checkpoint: bool) {
    // Causal negatives exercise the same seed/readiness/reader/cleanup path.
    // Suppressing an antagonist must be caught by campaign progress checks;
    // a successful child without those checks makes the negative test fail.
    let progress_timeout = if suppress_writer || suppress_checkpoint {
        Duration::from_secs(2)
    } else {
        ITERATION_TIMEOUT
    };
    let path = directory
        .join("gh335-ckpt-truncate.db")
        .to_string_lossy()
        .into_owned();

    // ── Seed: WAL mode + a small table with a few rows. ──────────────────
    {
        let rt = RuntimeBuilder::current_thread()
            .build()
            .expect("seed runtime");
        let conn = rt
            .block_on(fsqlite::Connection::open(path.clone()))
            .expect("seed open");
        rt.block_on(conn.execute("PRAGMA journal_mode=WAL;"))
            .expect("seed journal_mode=WAL");
        rt.block_on(
            conn.execute("CREATE TABLE gh335 (id INTEGER PRIMARY KEY, v INTEGER NOT NULL);"),
        )
        .expect("seed create table");
        for v in 0..8 {
            rt.block_on(conn.execute(&format!("INSERT INTO gh335 (v) VALUES ({v});")))
                .expect("seed insert");
        }
        rt.block_on(conn.close()).expect("seed close");
    }

    let stop = Arc::new(AtomicBool::new(false));
    // Both antagonist connections must be open before the reader starts.
    // No database locks are held while waiting at this test-only barrier.
    let ready = Arc::new(Barrier::new(3));
    let writes = Arc::new(AtomicU64::new(0));
    let truncates = Arc::new(AtomicU64::new(0));

    // ── Connection A: live writer (paced autocommit INSERT loop). ────────
    let writer = {
        let path = path.clone();
        let stop = Arc::clone(&stop);
        let ready = Arc::clone(&ready);
        let writes = Arc::clone(&writes);
        std::thread::spawn(move || {
            let rt = RuntimeBuilder::current_thread()
                .build()
                .expect("writer runtime");
            let conn = rt
                .block_on(fsqlite::Connection::open(path))
                .expect("writer open");
            ready.wait();
            let mut n: i64 = 100;
            while !stop.load(Ordering::Relaxed) {
                n += 1;
                if suppress_writer {
                    std::thread::sleep(WRITER_PACE);
                    continue;
                }
                match rt.block_on(conn.execute(&format!("INSERT INTO gh335 (v) VALUES ({n});"))) {
                    Ok(1) => {
                        writes.fetch_add(1, Ordering::Release);
                    }
                    Ok(count) => panic!("writer INSERT affected {count} rows, expected 1"),
                    Err(error) if error.is_transient() => {}
                    Err(error) => panic!("unexpected writer failure: {error}"),
                }
                std::thread::sleep(WRITER_PACE);
            }
            rt.block_on(conn.close()).expect("writer close");
        })
    };

    // ── Connection B: TRUNCATE checkpointer (paced loop). ────────────────
    let checkpointer = {
        let path = path.clone();
        let stop = Arc::clone(&stop);
        let ready = Arc::clone(&ready);
        let truncates = Arc::clone(&truncates);
        std::thread::spawn(move || {
            let rt = RuntimeBuilder::current_thread()
                .build()
                .expect("checkpointer runtime");
            let conn = rt
                .block_on(fsqlite::Connection::open(path))
                .expect("checkpointer open");
            ready.wait();
            while !stop.load(Ordering::Relaxed) {
                if suppress_checkpoint {
                    std::thread::sleep(CHECKPOINT_PACE);
                    continue;
                }
                match rt.block_on(conn.query("PRAGMA wal_checkpoint(TRUNCATE);")) {
                    Ok(rows) => {
                        use fsqlite_types::SqliteValue;

                        assert_eq!(rows.len(), 1, "checkpoint must report one status row");
                        let values = rows[0].values();
                        match values {
                            [
                                SqliteValue::Integer(0),
                                SqliteValue::Integer(total),
                                SqliteValue::Integer(backfilled),
                            ] if *total > 0 && *backfilled == *total => {
                                // The PRAGMA reports busy=1 for incomplete reset.
                                // Its backfill count is cumulative (wal_adapter),
                                // so complete progress also requires total equality.
                                // Empty-WAL success cannot prove the race scenario.
                                truncates.fetch_add(1, Ordering::Release);
                            }
                            [
                                SqliteValue::Integer(0),
                                SqliteValue::Integer(0),
                                SqliteValue::Integer(0),
                            ] => {}
                            [
                                SqliteValue::Integer(1),
                                SqliteValue::Integer(total),
                                SqliteValue::Integer(backfilled),
                            ] if *total >= 0 && *backfilled >= 0 && *backfilled <= *total => {}
                            other => panic!("unexpected TRUNCATE result: {other:?}"),
                        }
                    }
                    Err(error) if error.is_transient() => {}
                    Err(error) => panic!("unexpected checkpoint failure: {error}"),
                }
                std::thread::sleep(CHECKPOINT_PACE);
            }
            rt.block_on(conn.close()).expect("checkpointer close");
        })
    };

    // ── Connection C: per-iteration fresh-open → first SELECT. ───────────
    // Runs on its own thread reporting each outcome through a channel so the
    // main thread can bound each iteration and turn a wedge into a diagnosed
    // failure. The current phase is published so a stall names open/select/close.
    let phase = Arc::new(AtomicU8::new(PHASE_IDLE));
    let (tx, rx) = mpsc::channel::<Result<(), String>>();
    let reader = {
        let path = path.clone();
        let phase = Arc::clone(&phase);
        let ready = Arc::clone(&ready);
        let writes = Arc::clone(&writes);
        let truncates = Arc::clone(&truncates);
        std::thread::spawn(move || {
            let rt = RuntimeBuilder::current_thread()
                .build()
                .expect("reader runtime");
            ready.wait();
            let mut progress = (
                writes.load(Ordering::Acquire),
                truncates.load(Ordering::Acquire),
            );
            let mut previous_count = 8_i64;
            for i in 0..ITERATIONS {
                // Prove renewed antagonist progress in each half of the reader
                // campaign, before another fresh-open/first-SELECT attempt.
                // The supervisor also bounds this wait if either worker died.
                if i == ITERATIONS / 2 || i == ITERATIONS - 1 {
                    let deadline = Instant::now();
                    loop {
                        let current = (
                            writes.load(Ordering::Acquire),
                            truncates.load(Ordering::Acquire),
                        );
                        match require_progress(progress, current) {
                            Ok(()) => {
                                progress = current;
                                break;
                            }
                            Err(error) if deadline.elapsed() >= progress_timeout => {
                                panic!("{error}");
                            }
                            Err(_) => std::thread::sleep(Duration::from_millis(1)),
                        }
                    }
                }
                let outcome = (|| -> Result<(), String> {
                    // A brand-new connection: this open is what binds the pager
                    // publication that #335 straddles.
                    phase.store(PHASE_OPEN, Ordering::Relaxed);
                    let conn = rt
                        .block_on(fsqlite::Connection::open(path.clone()))
                        .map_err(|e| format!("iter {i}: open: {e:?}"))?;
                    // FIRST statement on the fresh connection MUST be the SELECT
                    // (no prior PRAGMA/statement), so it exercises the
                    // metadata-bind → reload straddle.
                    phase.store(PHASE_SELECT, Ordering::Relaxed);
                    let res = rt.block_on(conn.query("SELECT COUNT(*) FROM gh335;"));
                    phase.store(PHASE_CLOSE, Ordering::Relaxed);
                    rt.block_on(conn.close())
                        .map_err(|error| format!("iter {i}: close: {error:?}"))?;
                    phase.store(PHASE_IDLE, Ordering::Relaxed);
                    let rows = res.map_err(|error| format!("iter {i}: first SELECT: {error:?}"))?;
                    if rows.len() != 1 {
                        return Err(format!(
                            "iter {i}: COUNT(*) returned {} rows, expected one",
                            rows.len(),
                        ));
                    }
                    let count = match rows[0].values() {
                        [fsqlite_types::SqliteValue::Integer(count)] => *count,
                        other => return Err(format!("iter {i}: invalid COUNT(*) row: {other:?}")),
                    };
                    // Only committed INSERTs can change this table. A fresh
                    // connection must retain all eight seed rows and cannot
                    // observe fewer rows than an earlier completed read.
                    if count < previous_count {
                        return Err(format!(
                            "iter {i}: COUNT(*) regressed from {previous_count} to {count}",
                        ));
                    }
                    previous_count = count;
                    Ok(())
                })();
                if tx.send(outcome).is_err() {
                    break;
                }
            }
        })
    };

    let mut failures: Vec<String> = Vec::new();
    let mut received = 0u32;
    let mut hang = false;
    while received < ITERATIONS {
        match rx.recv_timeout(ITERATION_TIMEOUT) {
            Ok(Ok(())) => received += 1,
            Ok(Err(msg)) => {
                failures.push(msg);
                received += 1;
            }
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {
                let wedged = phase_name(phase.load(Ordering::Relaxed));
                failures.push(format!(
                    "WEDGE in phase `{wedged}` after {received}/{ITERATIONS} within \
                     {ITERATION_TIMEOUT:?} (a blocking open/select/close, not the retry loop \
                     — the SELECT retry loop is bounded by busy_timeout)"
                ));
                hang = true;
                break;
            }
        }
    }

    // This entire campaign runs in a supervised subprocess. If a reader wedge
    // prevents joining, fail the child; its remaining threads die with it.
    // If a healthy reader finishes but a worker cannot shut down, the parent
    // enforces the whole-lifecycle deadline and kills/reaps the child.
    stop.store(true, Ordering::Relaxed);
    if !hang {
        reader.join().expect("reader thread must not panic");
        writer.join().expect("writer thread must not panic");
        checkpointer
            .join()
            .expect("checkpointer thread must not panic");
    }

    assert!(
        failures.is_empty(),
        "GH #335 keeper: {}/{ITERATIONS} fresh-reader first-SELECT failure(s) \
         (pre-fix the first SELECT fails with BusySnapshot; post-fix must be 0):\n{}",
        failures.len(),
        failures.join("\n")
    );
    assert_eq!(
        received, ITERATIONS,
        "all fresh-reader attempts must finish"
    );
    require_progress(
        (0, 0),
        (
            writes.load(Ordering::Acquire),
            truncates.load(Ordering::Acquire),
        ),
    )
    .expect("successful writes and nonempty TRUNCATEs must accompany reader attempts");
}
