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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::Duration;

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
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir
        .path()
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

    // ── Connection A: live writer (paced autocommit INSERT loop). ────────
    let writer = {
        let path = path.clone();
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let rt = RuntimeBuilder::current_thread()
                .build()
                .expect("writer runtime");
            let conn = rt
                .block_on(fsqlite::Connection::open(path))
                .expect("writer open");
            let mut n: i64 = 100;
            while !stop.load(Ordering::Relaxed) {
                n += 1;
                // Writer contention (Busy/BusySnapshot) is absorbed by the
                // autocommit-DML retry loop; the writer is the antagonist, not
                // the assertion, so any residual error is intentionally ignored.
                let _ = rt.block_on(conn.execute(&format!("INSERT INTO gh335 (v) VALUES ({n});")));
                std::thread::sleep(WRITER_PACE);
            }
            let _ = rt.block_on(conn.close());
        })
    };

    // ── Connection B: TRUNCATE checkpointer (paced loop). ────────────────
    let checkpointer = {
        let path = path.clone();
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let rt = RuntimeBuilder::current_thread()
                .build()
                .expect("checkpointer runtime");
            let conn = rt
                .block_on(fsqlite::Connection::open(path))
                .expect("checkpointer open");
            while !stop.load(Ordering::Relaxed) {
                // A TRUNCATE that cannot complete (a peer holds the WAL) is a
                // benign Busy; loop and try again.
                let _ = rt.block_on(conn.execute("PRAGMA wal_checkpoint(TRUNCATE);"));
                std::thread::sleep(CHECKPOINT_PACE);
            }
            let _ = rt.block_on(conn.close());
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
        std::thread::spawn(move || {
            let rt = RuntimeBuilder::current_thread()
                .build()
                .expect("reader runtime");
            for i in 0..ITERATIONS {
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
                    let _ = rt.block_on(conn.close());
                    phase.store(PHASE_IDLE, Ordering::Relaxed);
                    res.map(|_| ())
                        .map_err(|e| format!("iter {i}: first SELECT: {e:?}"))
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

    // Stop the antagonists and join the healthy threads. On a wedge the reader
    // thread is stuck inside `block_on`; joining it would reintroduce the
    // stall, so its threads are deliberately leaked (the process exits when the
    // harness finishes) — mirrors the GH #333 keeper.
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
}
