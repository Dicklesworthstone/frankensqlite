//! bd-xva84 — GH #333 keeper: 0.2 regression where two OS threads, each with
//! a private current-thread asupersync runtime, concurrently
//! `Connection::open` the SAME db file and each run one UPDATE.
//!
//! On fsqlite 0.1.19 this topology was tolerated indefinitely (0/60 failures
//! downstream). On the 0.2 line it regressed: 7/60 on registry 0.2.1
//! (asupersync 0.3.10 — so not an asupersync 0.4 issue), 15/60 plus
//! indefinite hangs on master@e6122cc4. Errors surface as
//! `Database(BusyRecovery)` on open, or `Busy` on update/close. This is the
//! documented-intended single-process/multi-Connection MVCC shape — plain
//! concurrent open+UPDATE must not hit busy-class refusals at all
//! (contended-DDL busy semantics from GH #327 are intended; this is not DDL).
//!
//! The workload below is the issue's standalone repro ported verbatim
//! (same seed SQL, same two-worker barrier topology, same per-worker
//! open → UPDATE → close through `block_on`). The only additions are the
//! test harness around it: every worker reports through a channel and the
//! parent uses `recv_timeout`, so the GH #333 hang shape (a worker panics
//! before its Barrier and the sibling waits forever) fails the test with a
//! diagnosis instead of stalling CI.

use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use asupersync::runtime::RuntimeBuilder;

/// The issue matrix showed 14/100 on the 0.2 line; keep the full 100
/// iterations so the keeper has the statistical power to catch a partial
/// regression, not just a total one.
const ITERATIONS: u32 = 100;

/// Per-iteration wall-clock ceiling. A healthy iteration completes in
/// milliseconds; the regression's worst shape is an indefinite stall, which
/// this converts into a failed iteration with a hang diagnosis.
const ITERATION_TIMEOUT: Duration = Duration::from_secs(30);

const WORKERS: usize = 2;

#[test]
fn gh333_concurrent_same_file_open_update_from_two_threads_100_iters() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut failures: Vec<String> = Vec::new();

    for i in 0..ITERATIONS {
        let path_s = dir
            .path()
            .join(format!("race-{i}.db"))
            .to_string_lossy()
            .into_owned();

        // Seed phase, verbatim from the issue repro: create + insert on a
        // private current-thread runtime, then close.
        {
            let rt = RuntimeBuilder::current_thread()
                .build()
                .expect("seed runtime");
            let conn = rt
                .block_on(fsqlite::Connection::open(path_s.clone()))
                .expect("seed open");
            rt.block_on(conn.execute("CREATE TABLE t (k TEXT PRIMARY KEY, v INTEGER)"))
                .expect("seed create table");
            rt.block_on(conn.execute("INSERT INTO t (k, v) VALUES ('a', 0)"))
                .expect("seed insert");
            rt.block_on(conn.close()).expect("seed close");
        }

        let barrier = Arc::new(Barrier::new(WORKERS));
        let (tx, rx) = mpsc::channel::<Result<(), String>>();
        let handles: Vec<_> = (0..WORKERS)
            .map(|w| {
                let path_s = path_s.clone();
                let barrier = Arc::clone(&barrier);
                let tx = tx.clone();
                std::thread::spawn(move || {
                    let body = move || -> Result<(), String> {
                        barrier.wait();
                        let rt = RuntimeBuilder::current_thread()
                            .build()
                            .map_err(|e| format!("runtime: {e}"))?;
                        let conn = rt
                            .block_on(fsqlite::Connection::open(path_s))
                            .map_err(|e| format!("open[{w}]: {e:?}"))?;
                        rt.block_on(conn.execute("UPDATE t SET v = v + 1 WHERE k = 'a'"))
                            .map_err(|e| format!("update[{w}]: {e:?}"))?;
                        rt.block_on(conn.close())
                            .map_err(|e| format!("close[{w}]: {e:?}"))?;
                        Ok(())
                    };
                    // catch_unwind so a panicking worker still reports (the
                    // GH #333 hang shape is a pre-Barrier panic whose sibling
                    // then waits forever; the channel + recv_timeout below
                    // turns that into a bounded failure).
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body))
                        .unwrap_or_else(|panic| {
                            let msg = panic
                                .downcast_ref::<&str>()
                                .map(ToString::to_string)
                                .or_else(|| panic.downcast_ref::<String>().cloned())
                                .unwrap_or_else(|| "non-string panic payload".to_owned());
                            Err(format!("worker[{w}] panicked: {msg}"))
                        });
                    let _ = tx.send(outcome);
                })
            })
            .collect();
        drop(tx);

        let mut reported = 0usize;
        let mut hang = false;
        while reported < WORKERS {
            match rx.recv_timeout(ITERATION_TIMEOUT) {
                Ok(Ok(())) => {
                    reported += 1;
                }
                Ok(Err(msg)) => {
                    failures.push(format!("iter {i}: {msg}"));
                    reported += 1;
                }
                Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {
                    failures.push(format!(
                        "iter {i}: HANG — {} of {WORKERS} workers never reported within \
                         {ITERATION_TIMEOUT:?} (GH #333 stall shape)",
                        WORKERS - reported
                    ));
                    hang = true;
                    break;
                }
            }
        }

        if hang {
            // Wedged worker threads are deliberately leaked (joining would
            // reintroduce the stall); the process exits when the harness
            // finishes. Stop iterating: later iterations would report
            // against a poisoned process state.
            break;
        }
        for h in handles {
            h.join().expect("worker thread already reported via channel");
        }
    }

    assert!(
        failures.is_empty(),
        "GH #333 keeper: failures={}/{ITERATIONS}\n{}",
        failures.len(),
        failures.join("\n")
    );
}
