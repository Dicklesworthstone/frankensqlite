//! bd-ioq6x / GH#346 — oracle-first HARNESS + regression guard for the
//! page-accounting orphan-leak / index double-grant churn family.
//!
//! GH#346 signature: under sustained delete/replace churn a large region of
//! pages ends up "never used" (in no b-tree and not on the freelist), with
//! `freelist_count` low the whole time. Root-caused to pager.rs volatile
//! freelist / abandoned-EOF state that lives ABOVE the durable page_count and
//! is dropped by two sites without reconciling into the DURABLE freelist first:
//!   (a) `refresh_committed_state` wholesale-replaces `inner.freelist` from
//!       durable state on every checkpoint/peer-commit (pager.rs ~L8719);
//!   (b) checkpoint `abandoned_eof_reservations.clear()`
//!       (CheckpointGuard/MaintenanceActivityGuard drop, pager.rs ~L25314).
//! The fix reconciles frameless, in-range pages into the durable freelist:
//!   - refresh re-parks in-range in-memory-only freelist pages into the
//!     abandonment pool (which survives refresh) so the next commit folds them;
//!   - checkpoint folds frameless `<= db_size` pool entries into the freelist
//!     BEFORE truncating the WAL (mirroring the commit-time fold).
//!
//! TWO ARMS:
//!  1. `ioq6x_checkpoint_refresh_drops_freelist_pages_into_orphans` — NO
//!     secondary index. Isolates the pure page-accounting axis: any stock
//!     integrity_check error is a genuine orphan. Asserts ZERO orphans AND that
//!     stock integrity_check is fully "ok" (reconciliation: dropped pages must
//!     be recoverable to the durable freelist, so no page is "never used").
//!  2. `ioq6x_secondary_index_churn_stays_integral` — WITH a low-cardinality
//!     secondary index hammered by every writer (the canonical bd-84rh4 /
//!     bd-0shxy shape). This surfaces the double-grant face — a page granted to
//!     both the `items` table and the `idx_category` index, which stock reports
//!     as "wrong # of entries in index". Asserts table COUNT == index COUNT ==
//!     stock COUNT and integrity_check == "ok".
//!
//! HONESTY (measured, do not overclaim): at HEAD the double-grant face is
//! STOCHASTIC (~1/8 at FSQLITE_CHURN_SCALE=large in the sibling
//! `churn_acceptance_corruption_family`), and its root cause is the append-gate
//! double-consumption guard's bd-r82et exemption (pager.rs ~L9530), a DIFFERENT
//! site than the orphan-drop reconciliation this file's fix targets. This arm
//! is a regression guard, not a deterministic lever: a green run does not prove
//! the double-grant is fixed; a red run is a genuine corruption. Scale it up
//! with FSQLITE_CHURN_SCALE=large|full for a real hunt.
#![recursion_limit = "512"]

use fsqlite::Connection;
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

const _BEAD_ID: &str = "bd-ioq6x";

/// (n_threads, phases, ops_per_phase). Default stays CI-viable (~9.6k ops);
/// FSQLITE_CHURN_SCALE=large|full crank the abort-heavy contention that the
/// double-grant / orphan faces need.
fn scale() -> (usize, usize, usize) {
    match std::env::var("FSQLITE_CHURN_SCALE").as_deref() {
        Ok("full") => (16, 120, 80),
        Ok("large") => (8, 100, 60),
        _ => (4, 60, 40),
    }
}

/// Collect stock sqlite3's integrity_check lines (labels leaked pages
/// "Page N is never used", index desync "wrong # of entries in index ...").
fn stock_integrity_lines(path: &Path) -> Vec<String> {
    let conn = rusqlite::Connection::open(path).expect("open stock sqlite3");
    let mut stmt = conn
        .prepare("PRAGMA integrity_check(1000000)")
        .expect("prepare integrity_check");
    stmt.query_map([], |r| r.get::<_, String>(0))
        .expect("run integrity_check")
        .map(|r| r.expect("integrity_check row"))
        .collect()
}

fn stock_orphan_report(path: &Path) -> (usize, Option<u32>, Option<u32>, usize) {
    let lines = stock_integrity_lines(path);
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

/// Stock table COUNT and index-forced COUNT: divergence is the double-grant /
/// index desync fingerprint ("wrong # of entries in index").
fn stock_index_desync(path: &Path) -> (i64, i64) {
    let conn = rusqlite::Connection::open(path).expect("open stock sqlite3");
    let table_total: i64 = conn
        .query_row("SELECT count(*) FROM items", [], |r| r.get(0))
        .unwrap_or(-1);
    let index_total: i64 = conn
        .query_row(
            "SELECT count(*) FROM items INDEXED BY idx_category WHERE category = 42",
            [],
            |r| r.get(0),
        )
        .unwrap_or(-2);
    (table_total, index_total)
}

/// Runs a barrier-synchronised abort-heavy churn workload against `db`,
/// checkpointing (TRUNCATE) in provably-quiescent windows. Every writer hammers
/// ONE index bucket (category=42) — maximum split/merge contention — with big
/// rows (EOF splits). Every writer op is a `BEGIN CONCURRENT` insert plus, once
/// the window fills, a point-DELETE of an older row (feeds the committed
/// freelist; the resurrection/double-grant axis). When `with_index` is true a
/// low-cardinality `idx_category` index shares the same freelist/EOF pool as
/// the table. Returns the arithmetically expected live-row count.
fn run_churn(db: &str, with_index: bool) -> i64 {
    let (n_threads, phases, ops_per_phase) = scale();

    // Setup on the main thread.
    asupersync::test_utils::run_test(|| async {
        let setup = Connection::open(db).await.unwrap();
        setup
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA auto_vacuum=0;
                 PRAGMA busy_timeout=10000;
                 CREATE TABLE items (id INTEGER PRIMARY KEY, category INTEGER, data TEXT);",
            )
            .await
            .unwrap();
        if with_index {
            // Low-cardinality index (all rows category=42): a single deep index
            // b-tree hammered by every writer, splitting/merging on the same
            // EOF reservation + freelist pool as the table — a mis-reconciled
            // or double-granted page aliases an index leaf onto a table page.
            setup
                .execute("CREATE INDEX idx_category ON items(category);")
                .await
                .unwrap();
        }
        setup.close().await.unwrap();
    });

    // Barriers: N writers + 1 coordinator.
    let done_barrier = Arc::new(Barrier::new(n_threads + 1)); // writers idle after a phase
    let resume_barrier = Arc::new(Barrier::new(n_threads + 1)); // release next phase
    let expected_rows = Arc::new(AtomicI64::new(0));

    // ── Coordinator thread: checkpoints during the quiescent window ──
    let coordinator = {
        let db = db.to_owned();
        let done_b = done_barrier.clone();
        let resume_b = resume_barrier.clone();
        thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let ck = Connection::open(&db).await.unwrap();
                let _ = ck.execute("PRAGMA busy_timeout=10000;").await;
                for _phase in 0..phases {
                    // Wait until all writers finished the phase's ops and are idle.
                    done_b.wait();
                    // active_transactions == 0 here -> the checkpoint clears/
                    // refreshes the volatile freelist + abandonment pool.
                    let _ = ck.execute("PRAGMA wal_checkpoint(TRUNCATE);").await;
                    resume_b.wait();
                }
                let _ = ck.execute("PRAGMA wal_checkpoint(TRUNCATE);").await;
                ck.close().await.unwrap();
            });
        })
    };

    let mut handles = vec![];
    for tid in 0..n_threads {
        let db = db.to_owned();
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

                    for phase in 0..phases {
                        for i in 0..ops_per_phase {
                            let seq = phase * ops_per_phase + i;
                            let base = tid * 100_000_000;
                            // Expiring-window churn: insert a NEW high-key row
                            // and, once the window is full, delete an OLDER row
                            // of THIS thread (feeds the committed freelist with
                            // a minimal cross-thread conflict surface). All rows
                            // share category=42 (one hammered index bucket).
                            const WINDOW: usize = 400;
                            let key = (base + seq) as i64;
                            let insert = format!(
                                "INSERT INTO items (id, category, data) VALUES ({key}, 42, '{payload}')"
                            );
                            let delete = if seq >= WINDOW {
                                Some(format!(
                                    "DELETE FROM items WHERE id = {}",
                                    (base + seq - WINDOW) as i64
                                ))
                            } else {
                                None
                            };
                            // BEGIN CONCURRENT churn with retry: conflicts ->
                            // rollbacks -> parked EOF pages in the pool, and the
                            // abort->repop path that exercises the append-gate
                            // double-consumption guard.
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

    expected_rows.load(Ordering::SeqCst)
}

#[test]
fn ioq6x_checkpoint_refresh_drops_freelist_pages_into_orphans() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("ioq6x.db");
    let db = db_path.to_string_lossy().into_owned();

    let expected = run_churn(&db, false);

    let (orphans, first, last, run) = stock_orphan_report(&db_path);
    let (pages, free, rows) = stock_page_stats(&db_path);
    let integrity = stock_integrity_lines(&db_path);
    eprintln!(
        "IOQ6X RESULT: page_count={pages} freelist_count={free} rows={rows} expected_rows={expected} \
         orphans={orphans} range={first:?}..={last:?} longest_contiguous_run={run} pct_of_file={:.1}%",
        100.0 * orphans as f64 / pages.max(1) as f64,
    );

    assert_eq!(
        rows, expected,
        "row count must match arithmetic expectation"
    );
    assert_eq!(
        orphans, 0,
        "GH#346: fsqlite orphaned {orphans} pages (freelist_count={free}, page_count={pages}, \
         contiguous_run={run}, range={first:?}..={last:?}); stock never orphans. \
         Checkpoint-triggered committed-state refresh dropped in-memory freelist / \
         abandoned-EOF pages without freelisting them."
    );
    // RECONCILIATION: pages the pager dropped must be recoverable to the DURABLE
    // freelist — so stock's integrity_check is fully clean (no orphan AND no
    // structural error), every page accounted for as live or on the freelist.
    assert_eq!(
        integrity,
        vec!["ok".to_string()],
        "GH#346: stock integrity_check must be fully 'ok' after churn+checkpoint \
         (freelist_count={free}, page_count={pages}); got: {integrity:?}"
    );
}

#[test]
fn ioq6x_secondary_index_churn_stays_integral() {
    // Double-grant face (bd-84rh4/bd-0shxy/bd-q6ri6): the same churn WITH a
    // hammered low-cardinality secondary index can grant one page to both the
    // table and the index — stock reports "wrong # of entries in index".
    // NOTE: stochastic at HEAD (~1/8 at scale=large); this asserts integrity
    // but is a regression guard, not a deterministic proof of the fix.
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("ioq6x_idx.db");
    let db = db_path.to_string_lossy().into_owned();

    let expected = run_churn(&db, true);

    let (orphans, first, last, run) = stock_orphan_report(&db_path);
    let (pages, free, rows) = stock_page_stats(&db_path);
    let (table_total, index_total) = stock_index_desync(&db_path);
    let integrity = stock_integrity_lines(&db_path);
    eprintln!(
        "IOQ6X INDEX RESULT: page_count={pages} freelist_count={free} rows={rows} \
         expected_rows={expected} table_total={table_total} index_total={index_total} \
         orphans={orphans} range={first:?}..={last:?} longest_contiguous_run={run}"
    );

    assert_eq!(
        rows, expected,
        "row count must match arithmetic expectation"
    );
    assert_eq!(
        table_total, index_total,
        "GH#346/bd-84rh4: table COUNT ({table_total}) != index COUNT ({index_total}) — \
         a page was double-granted to both the items table and idx_category \
         (stock: 'wrong # of entries in index')"
    );
    assert_eq!(
        integrity,
        vec!["ok".to_string()],
        "GH#346/bd-q6ri6: with a hammered secondary index the churn+checkpoint \
         workload must leave stock integrity_check fully 'ok' (freelist_count={free}, \
         page_count={pages}, orphans={orphans}, table_total={table_total}, \
         index_total={index_total}); a 'wrong # of entries in index' or 'never used' \
         line means the accounting reconciliation double-granted or dropped a page. \
         Lines: {integrity:?}"
    );
}
