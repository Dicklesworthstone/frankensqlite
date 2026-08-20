//! Regression repro for `bd-dhhxp`: bare WAL reads must never surface
//! `Busy`/`BusyRecovery` while a single concurrent writer holds the store.
//!
//! Config mirrors the mcp_agent_mail_rust v0.3.28 stress/load gating that
//! filed the bug:
//!   * WAL journal mode
//!   * generous `busy_timeout`
//!   * a single writer doing repeated `BEGIN IMMEDIATE` / write / `COMMIT`
//!   * `FSQLITE_CONCURRENT_MODE` OFF (`PRAGMA fsqlite.concurrent_mode = OFF`)
//!   * N reader connections, each on its own thread + connection, doing bare
//!     `SELECT`s.
//!
//! The engine-level defect: a bare read's transaction-begin admission can
//! observe `BusyRecovery` when the single writer's checkpoint / group-commit
//! finalization is momentarily in flight (an identity-wide process-root
//! finalization is registered while the writer's maintenance/EXCLUSIVE lock
//! restoration is deferred). Unlike the connection-open retry
//! (`retry_busy_connection_bootstrap`, which retries both `Busy` and
//! `BusyRecovery`), the read/txn-begin retry budget
//! (`begin_pager_txn_with_busy_timeout`) only retries `Busy` — so a transient
//! `BusyRecovery` surfaces immediately, ignoring `busy_timeout`.
//!
//! The MVCC/WAL guarantee under test: WAL readers do not block on writers.
//! A bare read must therefore never observe `Busy`/`BusyRecovery` during
//! normal steady-state concurrent operation (including while the writer
//! checkpoints).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use asupersync::runtime::RuntimeBuilder;
use fsqlite_error::FrankenError;

const READERS: usize = 8;
const WRITER_COMMITS: u64 = 200;
const ROWS_PER_COMMIT: u64 = 8;
const CHECKPOINT_EVERY: u64 = 10;

/// Classify a query error as one of the WAL-reader "should never happen"
/// busy conditions this bug is about, returning a short label for reporting.
fn busy_label(err: &FrankenError) -> Option<&'static str> {
    match err {
        FrankenError::Busy => Some("Busy"),
        FrankenError::BusyRecovery => Some("BusyRecovery"),
        FrankenError::BusySnapshot { .. } => Some("BusySnapshot"),
        _ => None,
    }
}

#[test]
fn bd_dhhxp_bare_wal_reads_never_busy_under_single_writer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir
        .path()
        .join("bd-dhhxp.db")
        .to_string_lossy()
        .into_owned();

    // ── Seed: create the WAL database with a wide-ish table. ──
    {
        let rt = RuntimeBuilder::current_thread().build().expect("seed rt");
        let conn = rt
            .block_on(fsqlite::Connection::open(path.clone()))
            .expect("seed open");
        rt.block_on(conn.execute("PRAGMA journal_mode=WAL"))
            .expect("seed wal");
        rt.block_on(conn.execute("PRAGMA fsqlite.concurrent_mode = OFF"))
            .expect("seed concurrent off");
        rt.block_on(
            conn.execute("CREATE TABLE kv (id INTEGER PRIMARY KEY, payload TEXT, n INTEGER)"),
        )
        .expect("seed create");
        rt.block_on(conn.close()).expect("seed close");
    }

    let stop = Arc::new(AtomicBool::new(false));
    let busy_total = Arc::new(AtomicU64::new(0));
    let busy_recovery_total = Arc::new(AtomicU64::new(0));
    let reads_total = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(READERS + 1));

    // ── Writer thread: single writer, repeated BEGIN IMMEDIATE / write /
    //    COMMIT, with periodic explicit WAL checkpoints (the maintenance
    //    window that arms identity-wide process-root finalizations). Also
    //    keeps a low autocheckpoint threshold so commits themselves trip
    //    checkpoints. ──
    let writer = {
        let path = path.clone();
        let stop = Arc::clone(&stop);
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || -> Result<u64, String> {
            let rt = RuntimeBuilder::current_thread()
                .build()
                .map_err(|e| format!("writer rt: {e}"))?;
            let conn = rt
                .block_on(fsqlite::Connection::open(path))
                .map_err(|e| format!("writer open: {e:?}"))?;
            rt.block_on(conn.execute("PRAGMA journal_mode=WAL"))
                .map_err(|e| format!("writer wal: {e:?}"))?;
            // Keep the WRITER's own busy budget short so a checkpoint that
            // cannot immediately win exclusivity against live readers does not
            // stall the whole run; the reader invariant is what is under test,
            // not writer-side checkpoint starvation.
            rt.block_on(conn.execute("PRAGMA busy_timeout=2000"))
                .map_err(|e| format!("writer busy_timeout: {e:?}"))?;
            rt.block_on(conn.execute("PRAGMA fsqlite.concurrent_mode = OFF"))
                .map_err(|e| format!("writer concurrent off: {e:?}"))?;
            rt.block_on(conn.execute("PRAGMA wal_autocheckpoint=32"))
                .map_err(|e| format!("writer autockpt: {e:?}"))?;
            barrier.wait();
            let mut committed = 0u64;
            let mut next_id = 1u64;
            for iter in 0..WRITER_COMMITS {
                rt.block_on(conn.execute("BEGIN IMMEDIATE"))
                    .map_err(|e| format!("writer begin: {e:?}"))?;
                for _ in 0..ROWS_PER_COMMIT {
                    let sql = format!(
                        "INSERT INTO kv (id, payload, n) VALUES ({next_id}, \
                         'payload-value-for-row-{next_id}-with-some-bulk-text', {next_id})"
                    );
                    rt.block_on(conn.execute(&sql))
                        .map_err(|e| format!("writer insert: {e:?}"))?;
                    next_id += 1;
                }
                rt.block_on(conn.execute("COMMIT"))
                    .map_err(|e| format!("writer commit: {e:?}"))?;
                committed += 1;
                if iter % CHECKPOINT_EVERY == CHECKPOINT_EVERY - 1 {
                    // PASSIVE checkpoints do not block on live readers but still
                    // exercise the maintenance-lock window that arms the
                    // identity-wide process-root finalizations a reader's begin
                    // can observe as BusyRecovery.
                    match rt.block_on(conn.execute("PRAGMA wal_checkpoint(PASSIVE)")) {
                        // A checkpoint completing, or a legitimate Busy when a
                        // reader is mid-snapshot, is the writer's own retry
                        // concern — not the reader invariant under test.
                        Ok(_) | Err(FrankenError::Busy | FrankenError::BusyRecovery) => {}
                        Err(e) => return Err(format!("writer checkpoint: {e:?}")),
                    }
                }
            }
            stop.store(true, Ordering::SeqCst);
            rt.block_on(conn.close())
                .map_err(|e| format!("writer close: {e:?}"))?;
            Ok(committed)
        })
    };

    // ── Reader threads: bare SELECTs on their own connections. ──
    let readers: Vec<_> = (0..READERS)
        .map(|rid| {
            let path = path.clone();
            let stop = Arc::clone(&stop);
            let barrier = Arc::clone(&barrier);
            let busy_total = Arc::clone(&busy_total);
            let busy_recovery_total = Arc::clone(&busy_recovery_total);
            let reads_total = Arc::clone(&reads_total);
            std::thread::spawn(move || -> Result<Vec<String>, String> {
                let rt = RuntimeBuilder::current_thread()
                    .build()
                    .map_err(|e| format!("reader{rid} rt: {e}"))?;
                let conn = rt
                    .block_on(fsqlite::Connection::open(path))
                    .map_err(|e| format!("reader{rid} open: {e:?}"))?;
                rt.block_on(conn.execute("PRAGMA busy_timeout=60000"))
                    .map_err(|e| format!("reader{rid} busy_timeout: {e:?}"))?;
                rt.block_on(conn.execute("PRAGMA fsqlite.concurrent_mode = OFF"))
                    .map_err(|e| format!("reader{rid} concurrent off: {e:?}"))?;
                barrier.wait();
                let mut samples: Vec<String> = Vec::new();
                let mut extra_after_stop = 0u32;
                loop {
                    match rt.block_on(conn.query("SELECT count(*) FROM kv")) {
                        Ok(_rows) => {
                            reads_total.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(err) => {
                            if let Some(label) = busy_label(&err) {
                                if label == "BusyRecovery" {
                                    busy_recovery_total.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    busy_total.fetch_add(1, Ordering::Relaxed);
                                }
                                if samples.len() < 12 {
                                    samples.push(format!("reader{rid}: {label} ({err})"));
                                }
                            } else {
                                return Err(format!("reader{rid} unexpected error: {err:?}"));
                            }
                        }
                    }
                    if stop.load(Ordering::SeqCst) {
                        extra_after_stop += 1;
                        if extra_after_stop > 100 {
                            break;
                        }
                    }
                }
                rt.block_on(conn.close())
                    .map_err(|e| format!("reader{rid} close: {e:?}"))?;
                Ok(samples)
            })
        })
        .collect();

    let started = Instant::now();
    let writer_result = writer.join().expect("writer join");
    let committed = writer_result.expect("writer must complete without error");

    let deadline = started + Duration::from_secs(180);
    let mut all_samples: Vec<String> = Vec::new();
    for (rid, h) in readers.into_iter().enumerate() {
        assert!(
            Instant::now() < deadline,
            "reader{rid} did not finish before the 180s safety deadline"
        );
        let samples = h
            .join()
            .expect("reader join")
            .expect("reader must not hit an unexpected engine error");
        all_samples.extend(samples);
    }

    let busy = busy_total.load(Ordering::Relaxed);
    let busy_recovery = busy_recovery_total.load(Ordering::Relaxed);
    let reads = reads_total.load(Ordering::Relaxed);

    eprintln!(
        "bd-dhhxp repro: writer_commits={committed} reader_reads={reads} \
         busy={busy} busy_recovery={busy_recovery}"
    );
    for s in &all_samples {
        eprintln!("  sample: {s}");
    }

    assert_eq!(
        busy + busy_recovery,
        0,
        "bd-dhhxp: bare WAL readers observed Busy/BusyRecovery under a single \
         concurrent writer (busy={busy}, busy_recovery={busy_recovery}); WAL \
         readers must not block on writers.\nsamples:\n{}",
        all_samples.join("\n")
    );
}
