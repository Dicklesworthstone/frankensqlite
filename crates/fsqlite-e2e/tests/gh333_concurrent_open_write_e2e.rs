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

//! LOAD CAVEAT: the two workers absorb contention through the connection
//! `busy_timeout` budget (default 5 s per statement). Under heavy host load
//! (parallel test binaries, concurrent cargo builds) a retry window can
//! legitimately exhaust, which reads as a keeper failure without being an
//! engine regression — a 2026-08-11 batch-verify red reproduced GREEN when
//! rerun on a quieter host. The tests below therefore serialize against each
//! other via [`gh333_test_serializer`]; before treating a red run as a
//! regression, rerun this test file standalone on an unloaded host.

use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Barrier, Mutex, MutexGuard};
use std::time::Duration;

use asupersync::runtime::RuntimeBuilder;

/// Process-wide serializer so this file's tests never run concurrently with
/// each other (libtest runs them on parallel threads by default): the
/// 100-iteration race keeper and the panic-injection receipt each spawn
/// worker threads, and overlapping them doubles contention and eats into the
/// workers' `busy_timeout` retry budgets. Mirrors the `engine_test_serializer`
/// pattern from bd-pzjg8. Poisoning is ignored: a panicked predecessor must
/// not cascade into unrelated failures.
fn gh333_test_serializer() -> MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

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
    let _serial = gh333_test_serializer();
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
            h.join()
                .expect("worker thread already reported via channel");
        }
    }

    assert!(
        failures.is_empty(),
        "GH #333 keeper: failures={}/{ITERATIONS}\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// An acknowledged autocommit write must already be visible when a peer opens
/// afterward.  Opening the peer before the write is not an equivalent canary:
/// the retained-autocommit optimization can see that already-open peer and
/// choose to publish, while a write parked when the writer was alone remains
/// invisible to a peer that joins after the acknowledgement.
#[test]
fn acknowledged_write_is_visible_to_peer_opened_afterward() {
    let _serial = gh333_test_serializer();
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("peer-after-ack.db");
    let path = path.to_string_lossy().into_owned();
    let runtime = RuntimeBuilder::current_thread().build().expect("runtime");

    let writer = runtime
        .block_on(fsqlite::Connection::open(path.clone()))
        .expect("writer open");
    runtime
        .block_on(writer.execute("PRAGMA fsqlite.concurrent_mode = OFF;"))
        .expect("serialized autocommit mode");
    runtime
        .block_on(writer.execute(
            "CREATE TABLE acknowledged_write (id INTEGER PRIMARY KEY, value TEXT NOT NULL);",
        ))
        .expect("create table");

    runtime
        .block_on(writer.execute("INSERT INTO acknowledged_write VALUES (1, 'visible-after-ack');"))
        .expect("writer must acknowledge the autocommit INSERT");

    // This ordering is the regression boundary: the peer did not exist when
    // execute() returned success for the writer.
    let peer = runtime
        .block_on(fsqlite::Connection::open(path))
        .expect("peer open after writer acknowledgement");
    let rows = runtime
        .block_on(peer.query("SELECT id, value FROM acknowledged_write ORDER BY id;"))
        .expect("peer query after writer acknowledgement");

    assert_eq!(
        rows.len(),
        1,
        "a peer opened after execute() returned must observe that acknowledged autocommit write"
    );
    assert_eq!(
        rows[0].get(0),
        Some(&fsqlite_types::SqliteValue::Integer(1))
    );
    assert_eq!(
        rows[0].get(1),
        Some(&fsqlite_types::SqliteValue::Text(
            "visible-after-ack".into()
        ))
    );

    runtime.block_on(peer.close()).expect("peer close");
    runtime.block_on(writer.close()).expect("writer close");
}

/// bd-tc8u7: the MVCC truth matrix opens 8/16 connections to one file and
/// applies the matched durability PRAGMAs before synchronizing the workload.
/// Before the PRAGMA-boundary retry, `journal_mode=WAL` leaked BusyRecovery
/// from that setup in every retained T8/T16 attempt. Two barriers pin the
/// race: all workers open together, then all dispatch the mode transition
/// together. Every worker must complete the PRAGMA and prove WAL readback.
const JOURNAL_MODE_WORKERS: usize = 16;
const JOURNAL_MODE_ITERATIONS: u32 = 10;

#[test]
fn concurrent_open_journal_mode_wal_never_escapes_busy_recovery() {
    let _serial = gh333_test_serializer();
    let dir = tempfile::tempdir().expect("tempdir");
    let mut failures = Vec::new();

    for iteration in 0..JOURNAL_MODE_ITERATIONS {
        let path = dir
            .path()
            .join(format!("journal-mode-race-{iteration}.db"))
            .to_string_lossy()
            .into_owned();
        {
            let rt = RuntimeBuilder::current_thread()
                .build()
                .expect("seed runtime");
            let conn = rt
                .block_on(fsqlite::Connection::open(path.clone()))
                .expect("seed open");
            rt.block_on(conn.execute("CREATE TABLE seed (id INTEGER PRIMARY KEY);"))
                .expect("seed table");
            rt.block_on(conn.close()).expect("seed close");
        }

        let open_barrier = Arc::new(Barrier::new(JOURNAL_MODE_WORKERS));
        let pragma_barrier = Arc::new(Barrier::new(JOURNAL_MODE_WORKERS));
        let (tx, rx) = mpsc::channel::<Result<(), String>>();
        let handles: Vec<_> = (0..JOURNAL_MODE_WORKERS)
            .map(|worker| {
                let path = path.clone();
                let open_barrier = Arc::clone(&open_barrier);
                let pragma_barrier = Arc::clone(&pragma_barrier);
                let tx = tx.clone();
                std::thread::spawn(move || {
                    let outcome = (|| -> Result<(), String> {
                        let runtime = RuntimeBuilder::current_thread()
                            .build()
                            .map_err(|error| format!("runtime[{worker}]: {error}"));
                        open_barrier.wait();
                        let connection =
                            runtime
                                .as_ref()
                                .map_err(|error| error.clone())
                                .and_then(|rt| {
                                    rt.block_on(fsqlite::Connection::open(path))
                                        .map_err(|error| format!("open[{worker}]: {error:?}"))
                                });
                        // Every worker reaches the PRAGMA barrier even if its
                        // open failed, so a red keeper remains bounded rather
                        // than stranding peers inside `Barrier::wait`.
                        pragma_barrier.wait();
                        let rt = runtime?;
                        let conn = connection?;
                        rt.block_on(conn.execute("PRAGMA journal_mode=WAL;"))
                            .map_err(|error| format!("journal_mode[{worker}]: {error:?}"))?;
                        let rows = rt
                            .block_on(conn.query("PRAGMA journal_mode;"))
                            .map_err(|error| format!("readback[{worker}]: {error:?}"))?;
                        let mode = rows
                            .first()
                            .and_then(|row| row.values().first())
                            .ok_or_else(|| format!("readback[{worker}]: missing mode row"))?;
                        if mode != &fsqlite_types::SqliteValue::Text("wal".into()) {
                            return Err(format!("readback[{worker}]: expected wal, got {mode:?}"));
                        }
                        rt.block_on(conn.close())
                            .map_err(|error| format!("close[{worker}]: {error:?}"))
                    })();
                    let _ = tx.send(outcome);
                })
            })
            .collect();
        drop(tx);

        for _ in 0..JOURNAL_MODE_WORKERS {
            match rx.recv_timeout(ITERATION_TIMEOUT) {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failures.push(format!("iter {iteration}: {error}")),
                Err(error) => {
                    failures.push(format!(
                        "iter {iteration}: {error:?} waiting for {JOURNAL_MODE_WORKERS} workers"
                    ));
                    break;
                }
            }
        }
        for handle in handles {
            handle.join().expect("journal-mode worker must not panic");
        }
    }

    assert!(
        failures.is_empty(),
        "bd-tc8u7: concurrent journal_mode=WAL failures={}/{} worker-attempts\n{}",
        failures.len(),
        JOURNAL_MODE_WORKERS * JOURNAL_MODE_ITERATIONS as usize,
        failures.join("\n")
    );
}

/// GH #333 stall-shape receipt: a worker that panics mid-workload (after
/// `Connection::open`, its handle abandoned to unwind) must not wedge or fail
/// the sibling. The issue's worst arm was an indefinite hang where one worker
/// panicked before reaching its coordination point and the sibling waited
/// forever; at engine level the guarantee is that an abandoned connection
/// (panicked thread, no `close()`) leaves no state behind that blocks or
/// spuriously fails a concurrent open→UPDATE→close on the same file.
const PANIC_ITERATIONS: u32 = 25;

#[test]
fn gh333_worker_panic_after_open_does_not_wedge_or_fail_sibling() {
    let _serial = gh333_test_serializer();
    let dir = tempfile::tempdir().expect("tempdir");
    let mut failures: Vec<String> = Vec::new();

    for i in 0..PANIC_ITERATIONS {
        let path_s = dir
            .path()
            .join(format!("panic-{i}.db"))
            .to_string_lossy()
            .into_owned();
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

        let barrier = Arc::new(Barrier::new(2));
        let (tx, rx) = mpsc::channel::<(usize, Result<(), String>)>();

        // Worker 0: open, then panic with the connection abandoned to unwind.
        let panic_handle = {
            let path_s = path_s.clone();
            let barrier = Arc::clone(&barrier);
            let tx = tx.clone();
            std::thread::spawn(move || {
                let body = move || -> Result<(), String> {
                    barrier.wait();
                    let rt = RuntimeBuilder::current_thread()
                        .build()
                        .map_err(|e| format!("runtime: {e}"))?;
                    let _conn = rt
                        .block_on(fsqlite::Connection::open(path_s))
                        .map_err(|e| format!("open[panicker]: {e:?}"))?;
                    panic!("injected GH #333 worker panic after open");
                };
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body))
                    .unwrap_or_else(|_| Err("panicked (expected)".to_owned()));
                let _ = tx.send((0, outcome));
            })
        };
        // Worker 1: the sibling must complete the full workload.
        let sibling_handle = {
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
                        .map_err(|e| format!("open[sibling]: {e:?}"))?;
                    rt.block_on(conn.execute("UPDATE t SET v = v + 1 WHERE k = 'a'"))
                        .map_err(|e| format!("update[sibling]: {e:?}"))?;
                    rt.block_on(conn.close())
                        .map_err(|e| format!("close[sibling]: {e:?}"))?;
                    Ok(())
                };
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body))
                    .unwrap_or_else(|panic| {
                        let msg = panic
                            .downcast_ref::<&str>()
                            .map(ToString::to_string)
                            .or_else(|| panic.downcast_ref::<String>().cloned())
                            .unwrap_or_else(|| "non-string panic payload".to_owned());
                        Err(format!("sibling panicked: {msg}"))
                    });
                let _ = tx.send((1, outcome));
            })
        };
        drop(tx);

        let mut reported = 0usize;
        let mut hang = false;
        while reported < 2 {
            match rx.recv_timeout(ITERATION_TIMEOUT) {
                Ok((_, Ok(()))) => {
                    reported += 1;
                }
                // The panicker's injected panic is the expected outcome for
                // worker 0; any other error (e.g. its open failing) is real.
                Ok((0, Err(msg))) if msg == "panicked (expected)" => {
                    reported += 1;
                }
                Ok((w, Err(msg))) => {
                    failures.push(format!("iter {i}: worker {w}: {msg}"));
                    reported += 1;
                }
                Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {
                    failures.push(format!(
                        "iter {i}: HANG — sibling (or panicker) never reported within \
                         {ITERATION_TIMEOUT:?} after injected panic (GH #333 stall shape)"
                    ));
                    hang = true;
                    break;
                }
            }
        }
        if hang {
            break;
        }
        panic_handle
            .join()
            .expect("panicker thread reported via channel");
        sibling_handle
            .join()
            .expect("sibling thread reported via channel");
    }

    assert!(
        failures.is_empty(),
        "GH #333 panic-sibling keeper: failures={}/{PANIC_ITERATIONS}\n{}",
        failures.len(),
        failures.join("\n")
    );
}
