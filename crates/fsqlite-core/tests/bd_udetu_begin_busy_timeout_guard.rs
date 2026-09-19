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

use fsqlite_core::connection::Connection;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const BUSY_TIMEOUT_MS: u64 = 4_000;
/// A refusal is only contract-legal if it spent at least this long.
const MIN_LEGAL_REFUSAL_MS: u128 = (BUSY_TIMEOUT_MS / 2) as u128;
const WRITER_RUN_SECS: u64 = 8;

/// How many BEGINs succeeded, and `(elapsed_ms, message)` for each one refused.
type BeginMeasurements = (u64, Vec<(u128, String)>);

#[test]
fn begin_under_checkpoint_contention_spends_busy_timeout_before_refusing() {
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
