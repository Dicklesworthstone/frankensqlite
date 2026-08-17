//! bd-ioq6x / GH#346 — oracle-first HARNESS + regression guard for the
//! page-accounting orphan-leak family.
//!
//! GH#346 signature: under sustained delete/replace churn a large CONTIGUOUS
//! region of pages ends up "never used" (in no b-tree and not on the freelist),
//! with `freelist_count` = 0 the whole time. Root-caused to pager.rs in-memory
//! freelist / abandoned-EOF state that lives ABOVE the durable page_count and is
//! dropped by checkpoint-triggered committed-state refresh (see the report at
//! the end of this file's investigation).
//!
//! WHAT THIS EXERCISES: concurrent BEGIN CONCURRENT WAL churn (an expiring
//! marching window, big rows -> EOF splits) with `wal_checkpoint(TRUNCATE)`
//! forced to run in PROVABLY quiesced windows (barrier-synchronised so
//! active_transactions == 0), then cross-checks the resulting file with STOCK
//! sqlite3's own `PRAGMA integrity_check(1000000)` (which labels leaked pages
//! "Page N is never used"). Stock never orphans pages.
//!
//! EMPIRICAL STATUS (be honest): at HEAD this harness runs GREEN — no net
//! orphans. Instrumented tracing (temporary probes at pager.rs:8719
//! `self.freelist = <durable>` and the `abandoned_eof_reservations.clear()`
//! checkpoint sites) confirmed those sites DO discard reclaimable in-memory
//! freelist pages by the thousands during this workload, but the pages recover
//! (get re-freed / re-serialised) in these bounded runs. The production leak is
//! the stochastic residue where a page freed ABOVE committed_db_size (so the
//! `freelist_metadata_dirty` gate never serialises it, keeping freelist_count=0)
//! is dropped and never recovers. Left as a regression guard: if the leak
//! regresses to a reliably-reproducible form, `orphans` goes > 0 and this fails.
//!
//! NOTE: with a secondary index present, this workload ALSO surfaces a distinct
//! stock-confirmed corruption ("wrong # of entries in index") — the bd-84rh4
//! index/table double-grant face — which is why the index is omitted here to
//! isolate the pure page-accounting axis.
#![recursion_limit = "512"]

use fsqlite::Connection;
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

const _BEAD_ID: &str = "bd-ioq6x";

fn stock_orphan_report(path: &Path) -> (usize, Option<u32>, Option<u32>, usize) {
    let conn = rusqlite::Connection::open(path).expect("open stock sqlite3");
    let mut stmt = conn
        .prepare("PRAGMA integrity_check(1000000)")
        .expect("prepare integrity_check");
    let lines: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .expect("run integrity_check")
        .map(|r| r.expect("integrity_check row"))
        .collect();
    let mut orphans: Vec<u32> = Vec::new();
    for line in &lines {
        if let Some(rest) = line.strip_prefix("Page ") {
            if let Some(num) = rest.strip_suffix(" is never used") {
                if let Ok(p) = num.trim().parse::<u32>() {
                    orphans.push(p);
                }
            }
        }
    }
    for l in &lines {
        if !l.ends_with("is never used") && l != "ok" {
            eprintln!("IOQ6X stock integrity (non-orphan) line: {l}");
        }
    }
    orphans.sort_unstable();
    let first = orphans.first().copied();
    let last = orphans.last().copied();
    let mut best = 0usize;
    let mut cur = 0usize;
    let mut prev: Option<u32> = None;
    for &p in &orphans {
        cur = match prev {
            Some(pp) if p == pp + 1 => cur + 1,
            _ => 1,
        };
        best = best.max(cur);
        prev = Some(p);
    }
    (orphans.len(), first, last, best)
}

fn stock_page_stats(path: &Path) -> (u32, u32, i64) {
    let conn = rusqlite::Connection::open(path).expect("open stock sqlite3");
    let page_count: u32 = conn
        .query_row("PRAGMA page_count", [], |r| r.get(0))
        .expect("page_count");
    let freelist_count: u32 = conn
        .query_row("PRAGMA freelist_count", [], |r| r.get(0))
        .expect("freelist_count");
    let rows: i64 = conn
        .query_row("SELECT count(*) FROM items", [], |r| r.get(0))
        .unwrap_or(-1);
    (page_count, freelist_count, rows)
}

const N_THREADS: usize = 4;
const PHASES: usize = 60;
const OPS_PER_PHASE: usize = 40;

#[test]
fn ioq6x_checkpoint_refresh_drops_freelist_pages_into_orphans() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("ioq6x.db");
    let db = db_path.to_string_lossy().into_owned();

    // Setup on the main thread.
    asupersync::test_utils::run_test(|| async {
        let setup = Connection::open(&db).await.unwrap();
        setup
            .execute_batch(
                // NO secondary index: isolates the page-accounting orphan leak
                // from the index/table double-grant desync face (bd-84rh4), so
                // any stock integrity_check error is a genuine orphan.
                "PRAGMA journal_mode=WAL;
                 PRAGMA auto_vacuum=0;
                 PRAGMA busy_timeout=10000;
                 CREATE TABLE items (id INTEGER PRIMARY KEY, category INTEGER, data TEXT);",
            )
            .await
            .unwrap();
        setup.close().await.unwrap();
    });

    // Barriers: N writers + 1 coordinator.
    let done_barrier = Arc::new(Barrier::new(N_THREADS + 1)); // writers idle after a phase
    let resume_barrier = Arc::new(Barrier::new(N_THREADS + 1)); // release next phase
    let expected_rows = Arc::new(AtomicI64::new(0));

    // ── Coordinator thread: checkpoints during the quiescent window ──
    let coordinator = {
        let db = db.clone();
        let done_b = done_barrier.clone();
        let resume_b = resume_barrier.clone();
        thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let ck = Connection::open(&db).await.unwrap();
                let _ = ck.execute("PRAGMA busy_timeout=10000;").await;
                for _phase in 0..PHASES {
                    // Wait until all writers finished the phase's ops and are idle.
                    done_b.wait();
                    // All writer transactions are done -> checkpoint should run
                    // (active_transactions == 0) and clear/refresh the volatile
                    // freelist + abandonment pool.
                    let _ = ck.execute("PRAGMA wal_checkpoint(TRUNCATE);").await;
                    // Release writers into the next phase.
                    resume_b.wait();
                }
                let _ = ck.execute("PRAGMA wal_checkpoint(TRUNCATE);").await;
                ck.close().await.unwrap();
            });
        })
    };

    let mut handles = vec![];
    for tid in 0..N_THREADS {
        let db = db.clone();
        let done_b = done_barrier.clone();
        let resume_b = resume_barrier.clone();
        let expected = expected_rows.clone();
        handles.push(thread::spawn(move || -> Result<(), String> {
            let mut outcome: Result<(), String> = Ok(());
            asupersync::test_utils::run_test(|| async {
                outcome = async {
                    let conn = Connection::open(&db).await.map_err(|e| format!("{e:?}"))?;
                    conn.execute_batch("PRAGMA busy_timeout=10000;")
                        .await
                        .map_err(|e| format!("{e:?}"))?;
                    // ~1.5KB rows force multi-page splits -> EOF allocation.
                    let payload = "d".repeat(1500);

                    for phase in 0..PHASES {
                        for i in 0..OPS_PER_PHASE {
                            let seq = phase * OPS_PER_PHASE + i;
                            let base = tid * 100_000_000;
                            // Expiring-window churn (report's shape): every op
                            // inserts a NEW high-key row and, once the window is
                            // full, deletes the OLDEST (a low key ~WINDOW behind).
                            // The live region marches forward, so freed low pages
                            // are never revisited — a page dropped by
                            // refresh_committed_state can never recover.
                            const WINDOW: usize = 400;
                            // Two statements in one BEGIN CONCURRENT txn.
                            let insert = format!(
                                "INSERT INTO items (id, category, data) VALUES ({}, 42, '{payload}')",
                                (base + seq) as i64
                            );
                            let delete = if seq >= WINDOW {
                                Some(format!(
                                    "DELETE FROM items WHERE id = {}",
                                    (base + seq - WINDOW) as i64
                                ))
                            } else {
                                None
                            };
                            // BEGIN CONCURRENT churn with retry (mirrors
                            // churn_acceptance): conflicts -> rollbacks -> parked
                            // EOF pages in the abandonment pool.
                            let mut attempts = 0;
                            loop {
                                attempts += 1;
                                if attempts > 5000 {
                                    return Err(format!("t{tid} p{phase} op{i}: retry storm"));
                                }
                                if conn.in_transaction() {
                                    let _ = conn.execute("ROLLBACK;").await;
                                }
                                match conn.execute("BEGIN CONCURRENT;").await {
                                    Ok(_) => {}
                                    Err(e) if e.is_transient() => continue,
                                    Err(e) => return Err(format!("t{tid} begin: {e}")),
                                }
                                // INSERT the new row.
                                match conn.execute(&insert).await {
                                    Ok(_) => {}
                                    Err(e) if e.is_transient() => {
                                        let _ = conn.execute("ROLLBACK;").await;
                                        continue;
                                    }
                                    Err(e) => {
                                        let _ = conn.execute("ROLLBACK;").await;
                                        return Err(format!("t{tid} insert: {e}"));
                                    }
                                }
                                // Expire the oldest row (marches the window).
                                let mut deleted = 0i64;
                                if let Some(del) = &delete {
                                    match conn.execute(del).await {
                                        Ok(c) => deleted = c as i64,
                                        Err(e) if e.is_transient() => {
                                            let _ = conn.execute("ROLLBACK;").await;
                                            continue;
                                        }
                                        Err(e) => {
                                            let _ = conn.execute("ROLLBACK;").await;
                                            return Err(format!("t{tid} delete: {e}"));
                                        }
                                    }
                                }
                                match conn.execute("COMMIT;").await {
                                    Ok(_) => {
                                        expected.fetch_add(1 - deleted, Ordering::Relaxed);
                                        break;
                                    }
                                    Err(e) if e.is_transient() => {
                                        let _ = conn.execute("ROLLBACK;").await;
                                        continue;
                                    }
                                    Err(e) => return Err(format!("t{tid} commit: {e}")),
                                }
                            }
                        }
                        // Phase done: this thread is idle (no active txn). Sync
                        // with the coordinator so the checkpoint runs quiesced.
                        done_b.wait();
                        resume_b.wait();
                    }
                    conn.close().await.map_err(|e| format!("{e:?}"))?;
                    Ok(())
                }
                .await;
            });
            outcome
        }));
    }

    let mut first_err = None;
    for h in handles {
        match h.join().unwrap() {
            Ok(()) => {}
            Err(e) => {
                first_err.get_or_insert(e);
            }
        }
    }
    coordinator.join().unwrap();
    if let Some(e) = first_err {
        panic!("writer thread failed: {e}");
    }

    let expected = expected_rows.load(Ordering::SeqCst);
    let (orphans, first, last, run) = stock_orphan_report(&db_path);
    let (pages, free, rows) = stock_page_stats(&db_path);
    eprintln!(
        "IOQ6X RESULT: page_count={pages} freelist_count={free} rows={rows} expected_rows={expected} \
         orphans={orphans} range={first:?}..={last:?} longest_contiguous_run={run} pct_of_file={:.1}%",
        100.0 * orphans as f64 / pages.max(1) as f64,
    );

    assert_eq!(rows, expected, "row count must match arithmetic expectation");
    assert_eq!(
        orphans, 0,
        "GH#346: fsqlite orphaned {orphans} pages (freelist_count={free}, page_count={pages}, \
         contiguous_run={run}, range={first:?}..={last:?}); stock never orphans. \
         Checkpoint-triggered committed-state refresh dropped in-memory freelist / \
         abandoned-EOF pages without freelisting them."
    );
}
