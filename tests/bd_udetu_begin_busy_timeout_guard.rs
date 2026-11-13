#![recursion_limit = "512"]

//! bd-udetu — a `BEGIN` refused while a checkpoint holds exclusive access must
//! have spent the connection's `busy_timeout` first.
//!
//! The defect: both begin paths bind the pager publication BEFORE opening the
//! pager transaction, and that bind had no busy retry of its own. Under a
//! `wal_checkpoint(TRUNCATE)` loop a `BEGIN CONCURRENT` was therefore refused in
//! ~0 ms with `busy_timeout=10000` armed — the admission budget in
//! `begin_pager_txn_with_busy_timeout` never applied, because BEGIN never got as
//! far as admission. Measured on the unfixed engine: 577_277 of 577_277 refusals
//! returned in under one second, min / p50 / p90 all 0 ms. A caller that retries
//! without backoff (the ioq6x churn writers do) turns that into a livelock.
//!
//! The contract this guards is the SQLite one: a busy returned to the caller
//! means the deadline was actually spent. So the assertion is not "never busy"
//! (a checkpoint slower than the timeout may legitimately refuse) but "if you
//! refuse, you waited". The threshold is half the timeout, which is ~2000x the
//! observed defect and cannot trip on a slow machine, where waits only grow.
//!
//! GH#423 adds the other side of that contract: persistent `BEGIN IMMEDIATE`
//! contention must not spend a fresh timeout in each retry layer. The probes
//! below use 250 ms and 1000 ms budgets, plus a zero-timeout control. They keep
//! the lock held throughout each refusal, then verify the waiter is reusable.
//! Both connections are opened and configured before locking; opening and
//! cleanup are deliberately outside the measured interval. Run the focused
//! reproducer with:
//!
//! ```text
//! cargo test -p fsqlite-core --test bd_udetu_begin_busy_timeout_guard issue423_ -- --nocapture --test-threads=1
//! ```

// Exercise the actual shared-budget implementation alongside the black-box
// BEGIN probes. These deterministic ownership tests do not replace the real
// contention tests: passing them alone does not establish that dispatch is
// wired to the scope or that BEGIN honors the requested elapsed-time bound.
// Mounted directly under this crate root, the module's `pub(super)` items read
// as `pub(crate)` inside a private module, and its unit test's deliberate
// `Duration::MAX - 1s` is linted here though the library allows it; neither
// lint describes the shared code, and while they failed, `cargo clippy
// --tests` stopped at this target and never linted the rest of the crate.
#[allow(clippy::redundant_pub_crate, clippy::unchecked_time_subtraction)]
#[path = "../src/connection/busy_timeout.rs"]
mod issue423_busy_timeout;

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const BUSY_TIMEOUT_MS: u64 = 4_000;
/// A refusal is only contract-legal if it spent at least this long.
const MIN_LEGAL_REFUSAL_MS: u128 = (BUSY_TIMEOUT_MS / 2) as u128;
const WRITER_RUN_SECS: u64 = 8;

// Do not run the wall-clock upper-bound probes alongside this target's heavy
// checkpoint antagonist. Recover poison so one failed budget still allows the
// other budget's diagnostic to run.
static BEGIN_BUSY_TIMEOUT_TEST_LOCK: Mutex<()> = Mutex::new(());

/// How many BEGINs succeeded, and `(elapsed_ms, message)` for each one refused.
type BeginMeasurements = (u64, Vec<(u128, String)>);

#[test]
fn begin_under_checkpoint_contention_spends_busy_timeout_before_refusing() {
    let _guard = BEGIN_BUSY_TIMEOUT_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("udetu_guard.db");
    let path = path.to_str().expect("utf-8 path").to_owned();

    // Seed enough data that a TRUNCATE checkpoint holds exclusive access for a
    // measurable window rather than completing instantly.
    {
        let p = path.clone();
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&p).await.expect("open seed");
            conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
                .await
                .expect("ddl");
            let payload = "d".repeat(1_500);
            for i in 0..1_200 {
                conn.execute(&format!("INSERT INTO t(id,v) VALUES({i},'{payload}')"))
                    .await
                    .expect("seed insert");
            }
            conn.close().await.expect("close seed");
        });
    }

    let stop = Arc::new(AtomicBool::new(false));

    let checkpointer = {
        let p = path.clone();
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&p).await.expect("open checkpointer");
                conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS}"))
                    .await
                    .expect("busy_timeout");
                while !stop.load(Ordering::Relaxed) {
                    let _ = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").await;
                    let _ = conn.execute("INSERT INTO t(v) VALUES('c')").await;
                }
                conn.close().await.expect("close checkpointer");
            });
        })
    };

    // `run_test` requires the future to resolve to `()`, so the measurements
    // come back through shared state rather than as a return value.
    let measured: Arc<Mutex<BeginMeasurements>> = Arc::new(Mutex::new((0, Vec::new())));

    let writer = {
        let p = path.clone();
        let stop = Arc::clone(&stop);
        let measured = Arc::clone(&measured);
        std::thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&p).await.expect("open writer");
                conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS}"))
                    .await
                    .expect("busy_timeout");
                let deadline = Instant::now() + Duration::from_secs(WRITER_RUN_SECS);
                let mut began = 0u64;
                // (elapsed_ms, message) for every refusal, so a failure names
                // both how fast it came back and what it was.
                let mut refusals: Vec<(u128, String)> = Vec::new();
                while Instant::now() < deadline {
                    let started = Instant::now();
                    match conn.execute("BEGIN CONCURRENT").await {
                        Ok(_) => {
                            began += 1;
                            let _ = conn.execute("INSERT INTO t(v) VALUES('w')").await;
                            let _ = conn.execute("COMMIT").await;
                        }
                        Err(error) => {
                            refusals.push((started.elapsed().as_millis(), format!("{error}")));
                            if conn.in_transaction() {
                                let _ = conn.execute("ROLLBACK").await;
                            }
                        }
                    }
                }
                stop.store(true, Ordering::Relaxed);
                conn.close().await.expect("close writer");
                *measured.lock().expect("measurements") = (began, refusals);
            });
        })
    };

    writer.join().expect("writer thread");
    stop.store(true, Ordering::Relaxed);
    checkpointer.join().expect("checkpointer thread");

    let (began, refusals) = measured.lock().expect("measurements").clone();

    assert!(
        began > 0,
        "writer never began a transaction in {WRITER_RUN_SECS}s; the probe proved nothing"
    );

    let instant: Vec<&(u128, String)> = refusals
        .iter()
        .filter(|(ms, _)| *ms < MIN_LEGAL_REFUSAL_MS)
        .collect();
    let fastest: Vec<&(u128, String)> = instant.iter().take(3).copied().collect();
    assert!(
        instant.is_empty(),
        "{} of {} BEGIN refusals came back in under {MIN_LEGAL_REFUSAL_MS}ms despite \
         busy_timeout={BUSY_TIMEOUT_MS}ms ({began} began OK). A busy returned to the \
         caller must mean the deadline was actually spent. Fastest: {fastest:?}",
        instant.len(),
        refusals.len()
    );
}

fn check_immediate_begin_busy_budget(timeout_ms: u64) {
    let _guard = BEGIN_BUSY_TIMEOUT_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("issue423_begin_budget.db");
    let path = path.to_str().expect("utf-8 path");

    asupersync::test_utils::run_test(|| async {
        let holder = Connection::open(path).await.expect("open holder");
        holder.execute("PRAGMA journal_mode=WAL").await.expect("wal");
        holder
            .execute("PRAGMA wal_autocheckpoint=0")
            .await
            .expect("disable holder auto-checkpoint");
        holder
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .await
            .expect("create table");

        let waiter = Connection::open(path).await.expect("open waiter");
        waiter
            .execute("PRAGMA wal_autocheckpoint=0")
            .await
            .expect("disable waiter auto-checkpoint");
        waiter
            .execute(&format!("PRAGMA busy_timeout={timeout_ms}"))
            .await
            .expect("set waiter busy_timeout");

        holder.execute("BEGIN IMMEDIATE").await.expect("hold writer");
        holder
            .execute("INSERT INTO t(v) VALUES('held')")
            .await
            .expect("write under held transaction");

        // Repeat on the SAME connection: a statement-scoped deadline must be
        // fresh for the next statement, even after the previous one timed out.
        let mut elapsed_samples = Vec::new();
        for attempt in 1..=2 {
            let started = Instant::now();
            let result = waiter.execute("BEGIN IMMEDIATE").await;
            let elapsed = started.elapsed();
            eprintln!(
                "GH#423: busy_timeout={timeout_ms}ms attempt={attempt} \
                 elapsed={elapsed:?} result={result:?}"
            );
            assert!(
                matches!(
                    &result,
                    Err(
                        FrankenError::Busy
                            | FrankenError::BusyRecovery
                            | FrankenError::BusySnapshot { .. }
                    )
                ),
                "held writer must refuse BEGIN with a busy-family error: {result:?}"
            );
            assert!(
                !waiter.in_transaction(),
                "a refused BEGIN must not leave a transaction open"
            );
            assert!(holder.in_transaction(), "waiter must not end holder's txn");
            elapsed_samples.push(elapsed);
        }

        // Finish cleanup and prove reusability BEFORE making timing assertions,
        // so a red reproduction does not rely on Drop to release the writer.
        holder.execute("ROLLBACK").await.expect("release writer");
        waiter
            .execute("BEGIN IMMEDIATE")
            .await
            .expect("waiter begins after release, including with zero timeout");
        assert!(waiter.in_transaction());
        waiter
            .execute("INSERT INTO t(v) VALUES('reused')")
            .await
            .expect("waiter can write after timeout");
        waiter.execute("COMMIT").await.expect("commit reused waiter");
        assert!(!waiter.in_transaction());
        waiter.close().await.expect("close waiter");
        holder.close().await.expect("close holder");

        // Permit 20 ms of clock/timer granularity below the requested timeout,
        // and 50% scheduling slack above it. Neither instant refusal nor a
        // second full timeout fits this interval at either positive budget.
        // Zero disables waiting, not admission; allow a small dispatch margin.
        let minimum = Duration::from_millis(timeout_ms.saturating_sub(20));
        let maximum = Duration::from_millis(if timeout_ms == 0 {
            100
        } else {
            timeout_ms + timeout_ms / 2
        });
        for (attempt, elapsed) in elapsed_samples.into_iter().enumerate() {
            assert!(
                elapsed >= minimum && elapsed <= maximum,
                "GH#423: busy_timeout={timeout_ms}ms attempt={} elapsed={elapsed:?}; \
                 expected {minimum:?}..={maximum:?}. BEGIN admission and outer \
                 statement retry must share ONE budget",
                attempt + 1
            );
        }
    });
}

#[test]
fn issue423_begin_immediate_one_budget_250ms() {
    check_immediate_begin_busy_budget(250);
}

#[test]
fn issue423_begin_immediate_one_budget_1000ms() {
    check_immediate_begin_busy_budget(1_000);
}

#[test]
fn issue423_begin_immediate_zero_timeout() {
    check_immediate_begin_busy_budget(0);
}
