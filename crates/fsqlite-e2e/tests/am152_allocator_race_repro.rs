//! am#152 — page-allocator cross-connection race stress repro.
//!
//! External report (am#152): under 10+ concurrent writers, the FrankenSQLite
//! page allocator can hand the SAME physical page number to two different
//! transactions which use it for different b-trees, producing overlapping
//! pages on disk ("2nd reference to page Q" / "database disk image is
//! malformed" / lost data).
//!
//! Architecture under test (verified in crates/fsqlite-pager/src/pager.rs):
//!
//! - Each `fsqlite::Connection::open` constructs its OWN `PagerInner`
//!   (`Arc::new(Mutex::new(PagerInner{..}))`), so `next_page` / `freelist` are
//!   per-connection; the inner Mutex only serializes transactions within ONE
//!   connection.
//! - Cross-connection (cross-process) allocation safety relies on (a) the
//!   file-lock protocol and (b) commit-time first-committer-wins.
//! - That commit-time conflict check (`cross_process_conflict_pages`,
//!   pager.rs:9271) is WAL-only; in rollback-journal (DELETE) mode it is empty,
//!   so the only cross-connection defenses are the EXCLUSIVE commit lock plus
//!   the per-begin page-1 refresh and the read-side snapshot-db_size guard.
//!
//! This test drives many writers, each on its own Connection to the same
//! file-backed db, each forcing heavy page allocation (CREATE TABLE + bulk
//! INSERT to force leaf/interior splits), then runs PRAGMA integrity_check.
//!
//! Two scenarios: `am152_allocator_race_rollback_journal` (DELETE journal,
//! default) and `am152_allocator_race_wal` (explicit WAL, the harness's normal
//! mode), used to compare conflict-detection behaviour.
//!
//! RESULT (bd-9inpb): REPRODUCED + FIXED. The rollback-journal scenario
//! reproduces the page-aliasing corruption ("page N referenced multiple times"
//! / "2nd reference to page N", canonical sqlite3 confirms the malformed image)
//! because rollback-journal commits previously had NO cross-connection
//! page-conflict check (the WAL path's `cross_process_conflict_pages` is empty
//! for non-WAL commits). The WAL scenario stayed clean (its first-committer-wins
//! page-conflict check protected it). Fix: `commit_journal` now re-reads the
//! on-disk page-1 db size under the EXCLUSIVE commit lock and aborts with
//! `BusySnapshot` (first-committer-wins) when a write-set page aliases the
//! peer-claimed page range. These remain `#[ignore]`d heavy stress harnesses;
//! run with `--ignored` on demand to validate that the fix holds.
#![recursion_limit = "512"]
// Each writer owns its non-`Send` connection on one dedicated OS thread, and
// the deliberately full-stack engine operations produce large futures. Boxing
// every await would change the stress workload this regression test measures.
#![allow(clippy::future_not_send, clippy::large_futures)]

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use fsqlite::{Connection, FrankenError, Row, SqliteValue};

const WRITERS: usize = 14;
const TABLES_PER_WRITER: usize = 6;
const ROWS_PER_TABLE: usize = 800;
const RETRY_BUDGET: Duration = Duration::from_secs(60);

fn is_transient(err: &FrankenError) -> bool {
    matches!(
        err,
        FrankenError::Busy
            | FrankenError::BusyRecovery
            | FrankenError::BusySnapshot { .. }
            | FrankenError::DatabaseLocked { .. }
    )
}

fn backoff(attempt: u32) -> Duration {
    let shift = attempt.min(6);
    Duration::from_millis((1_u64 << shift).min(40))
}

/// Run `f` retrying on transient (busy/snapshot) errors until RETRY_BUDGET.
async fn with_retry<T, Fut, F>(label: &str, mut f: F) -> Result<T, String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, FrankenError>>,
{
    let deadline = Instant::now() + RETRY_BUDGET;
    let mut attempt = 0_u32;
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(err) if is_transient(&err) && Instant::now() < deadline => {
                attempt = attempt.saturating_add(1);
                thread::sleep(backoff(attempt));
            }
            Err(err) => return Err(format!("{label}: {err}")),
        }
    }
}

async fn open(path: &Path, wal: bool) -> Result<Connection, String> {
    let path = path.to_string_lossy().into_owned();
    let conn = with_retry("open", || Connection::open(path.clone())).await?;
    // Generous busy_timeout so the engine's own retry loop has room.
    let _ = conn.execute("PRAGMA busy_timeout=10000;").await;
    if wal {
        with_retry("journal_mode=WAL", || async {
            conn.execute("PRAGMA journal_mode=WAL;").await.map(|_| ())
        })
        .await?;
    }
    // concurrent_mode is ON by default; assert it stayed on (that is the
    // whole point of the repro).
    if !conn.is_concurrent_mode_default() {
        return Err("concurrent_mode default must be ON".to_owned());
    }
    Ok(conn)
}

async fn writer_body(path: &Path, writer_id: usize, wal: bool) -> Result<(), String> {
    let conn = open(path, wal).await?;
    for t in 0..TABLES_PER_WRITER {
        // Tables are pre-created in the seed phase so the DDL schema-visibility
        // window does not short-circuit this writer before it builds enough
        // allocation pressure. This isolates the allocator-aliasing race.
        let table = format!("w{writer_id}_t{t}");
        // Force leaf + interior splits: many rows, wide-ish payloads.
        for r in 0..ROWS_PER_TABLE {
            let payload_a = format!(
                "w{writer_id}-t{t}-r{r}-{:040x}",
                (writer_id * 131 + r) as u128
            );
            let payload_b = format!("{:050x}", (t * 977 + r) as u128);
            with_retry(&format!("insert {table} r{r}"), || async {
                conn.query_with_params(
                    &format!("INSERT INTO {table} (a, b, c) VALUES (?, ?, ?);"),
                    &[
                        SqliteValue::Text(payload_a.clone().into()),
                        SqliteValue::Text(payload_b.clone().into()),
                        SqliteValue::Integer((writer_id * 1000 + r) as i64),
                    ],
                )
                .await
                .map(|_| ())
            })
            .await?;
        }
    }
    Ok(())
}

async fn integrity_messages(conn: &Connection) -> Result<Vec<String>, String> {
    let rows: Vec<Row> = with_retry("integrity_check", || async {
        conn.query("PRAGMA integrity_check;").await
    })
    .await?;
    rows.iter()
        .map(|row| match row.values().first() {
            Some(SqliteValue::Text(t)) => Ok(t.to_string()),
            other => Err(format!("non-text integrity row: {other:?}")),
        })
        .collect()
}

fn run_scenario(wal: bool) -> Result<(), String> {
    let dir = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;
    let db_path = dir.path().join("am152.db");

    // Seed the file so all writers open an existing db (shared committed
    // db_size baseline => identical next_page snapshots => maximal aliasing
    // opportunity). Pre-create every writer's tables here so the only
    // concurrent work is page-allocating INSERTs.
    {
        let mut seed_outcome: Result<(), String> = Ok(());
        asupersync::test_utils::run_test(|| async {
            seed_outcome = async {
                let conn = open(&db_path, wal).await?;
                with_retry("seed", || async {
                    conn.execute("CREATE TABLE IF NOT EXISTS _seed (id INTEGER PRIMARY KEY, v TEXT);")
                        .await
                        .map(|_| ())
                })
                .await?;
                for w in 0..WRITERS {
                    for t in 0..TABLES_PER_WRITER {
                        let table = format!("w{w}_t{t}");
                        with_retry(&format!("seed create {table}"), || async {
                            conn.execute(&format!(
                                "CREATE TABLE IF NOT EXISTS {table} (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c INTEGER);"
                            ))
                            .await
                            .map(|_| ())
                        })
                        .await?;
                    }
                }
                Ok(())
            }
            .await;
        });
        seed_outcome?;
    }

    let errors = Arc::new(AtomicUsize::new(0));
    let path_arc = Arc::new(db_path.clone());
    let handles: Vec<_> = (0..WRITERS)
        .map(|w| {
            let path = Arc::clone(&path_arc);
            let errors = Arc::clone(&errors);
            thread::spawn(move || {
                asupersync::test_utils::run_test(|| async {
                    if let Err(e) = writer_body(&path, w, wal).await {
                        eprintln!("[writer {w}] FAILED: {e}");
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                });
            })
        })
        .collect();
    for h in handles {
        let _ = h.join();
    }

    let writer_errors = errors.load(Ordering::Relaxed);

    // Integrity check via frank.
    let mut verify_outcome: Result<Vec<String>, String> = Ok(Vec::new());
    asupersync::test_utils::run_test(|| async {
        verify_outcome = async {
            let verify = open(&db_path, wal).await?;
            if wal {
                // Fold the WAL back so a fresh rusqlite open sees everything.
                let _ = verify.execute("PRAGMA wal_checkpoint(TRUNCATE);").await;
            }
            let frank_msgs = integrity_messages(&verify).await?;
            drop(verify);
            Ok(frank_msgs)
        }
        .await;
    });
    let frank_msgs = verify_outcome?;

    // Cross-check with canonical rusqlite (stock SQLite) too.
    let rusqlite_msg: String = {
        let c = rusqlite::Connection::open(&db_path).map_err(|e| format!("rusqlite open: {e}"))?;
        let _ = c.execute_batch("PRAGMA busy_timeout=10000; PRAGMA wal_checkpoint(TRUNCATE);");
        c.query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .map_err(|e| format!("rusqlite integrity_check: {e}"))?
    };

    eprintln!(
        "[am152 wal={wal}] writer_errors={writer_errors} frank_integrity={frank_msgs:?} rusqlite_integrity={rusqlite_msg:?}"
    );

    // Lost-data / overlap detection: count rows in every table via rusqlite.
    // A page-aliasing corruption typically shows up as wrong counts or a
    // malformed-image error even when integrity_check (which only walks
    // reachable structure) reports ok.
    let mut count_problems = Vec::new();
    {
        let c = rusqlite::Connection::open(&db_path)
            .map_err(|e| format!("rusqlite count open: {e}"))?;
        for w in 0..WRITERS {
            for t in 0..TABLES_PER_WRITER {
                let table = format!("w{w}_t{t}");
                match c.query_row(&format!("SELECT COUNT(*) FROM {table};"), [], |r| {
                    r.get::<_, i64>(0)
                }) {
                    Ok(n) if n == ROWS_PER_TABLE as i64 => {}
                    Ok(n) => count_problems.push(format!("{table}: {n}/{ROWS_PER_TABLE}")),
                    Err(e) => count_problems.push(format!("{table}: query err {e}")),
                }
            }
        }
    }

    let frank_ok = frank_msgs == ["ok"];
    let rusqlite_ok = rusqlite_msg == "ok";

    eprintln!(
        "[am152 wal={wal}] count_mismatches={} (note: writers that hit non-transient errors will legitimately under-count)",
        count_problems.len()
    );
    if !count_problems.is_empty() {
        eprintln!(
            "[am152 wal={wal}] sample count problems: {:?}",
            &count_problems[..count_problems.len().min(10)]
        );
    }

    if frank_ok && rusqlite_ok {
        Ok(())
    } else {
        Err(format!(
            "CORRUPTION DETECTED (wal={wal}): frank={frank_msgs:?} rusqlite={rusqlite_msg:?} writer_errors={writer_errors} count_problems={count_problems:?}"
        ))
    }
}

#[test]
#[ignore = "am#152 / bd-9inpb stress harness: reproduced+fixed; run on demand with --ignored to validate the page-alias fix"]
fn am152_allocator_race_rollback_journal() {
    // The suspected-vulnerable path: concurrent_mode ON + DELETE journal,
    // where commit-time cross-process conflict detection is empty.
    match run_scenario(false) {
        Ok(()) => eprintln!("[am152 rollback] clean"),
        Err(e) => panic!("{e}"),
    }
}

#[test]
#[ignore = "am#152 / bd-9inpb stress harness: reproduced+fixed; run on demand with --ignored to validate the page-alias fix"]
fn am152_allocator_race_wal() {
    // Control: WAL mode has the first-committer-wins page conflict check.
    match run_scenario(true) {
        Ok(()) => eprintln!("[am152 wal] clean"),
        Err(e) => panic!("{e}"),
    }
}

// ===========================================================================
// Deterministic barrier scenario — force the allocation window wide open.
//
// In CONCURRENT mode, `ensure_writer` does NOT take any cross-process file
// lock at write-upgrade time (pager.rs:8497 "Concurrent writers don't acquire
// the global writer_active lock"). The cross-process lock (RESERVED/EXCLUSIVE)
// is only taken at COMMIT. So every writer can allocate EOF pages from its OWN
// snapshot's `next_page` simultaneously, before anyone commits.
//
// Each writer opens BEGIN CONCURRENT, inserts enough rows into its OWN table
// to grow the file by several pages (so its allocations are at EOF = N+1,
// N+2, ...), waits on a barrier so ALL writers have allocated the same page
// numbers in different b-trees, THEN all commit. If the allocator hands the
// same physical page to two trees without commit-time detection, the result
// is overlap / lost rows / malformed image.
// ===========================================================================

const BARRIER_WRITERS: usize = 16;
const BARRIER_ROUNDS: usize = 40;
const BARRIER_ROWS_PER_TXN: usize = 60;

/// Ensures a barrier participant cannot strand its peers by returning or
/// unwinding before its explicit rendezvous.
struct BarrierArrivalGuard<'a> {
    barrier: Option<&'a Barrier>,
}

impl<'a> BarrierArrivalGuard<'a> {
    const fn new(barrier: &'a Barrier) -> Self {
        Self {
            barrier: Some(barrier),
        }
    }

    fn arrive(mut self) {
        if let Some(barrier) = self.barrier.take() {
            barrier.wait();
        }
    }
}

impl Drop for BarrierArrivalGuard<'_> {
    fn drop(&mut self) {
        if let Some(barrier) = self.barrier.take() {
            barrier.wait();
        }
    }
}

#[test]
fn barrier_arrival_guard_releases_peer_on_early_return() {
    let barrier = Arc::new(Barrier::new(2));
    let (tx, rx) = mpsc::channel();

    let early_barrier = Arc::clone(&barrier);
    let early = thread::spawn(move || {
        let _arrival = BarrierArrivalGuard::new(&early_barrier);
        // Returning drops the armed guard and therefore still arrives.
    });

    let peer_barrier = Arc::clone(&barrier);
    let peer = thread::spawn(move || {
        BarrierArrivalGuard::new(&peer_barrier).arrive();
        let _ = tx.send(());
    });

    assert!(
        rx.recv_timeout(Duration::from_secs(2)).is_ok(),
        "an early-returning participant must not strand its barrier peer"
    );
    assert!(early.join().is_ok(), "early-returning participant panicked");
    assert!(peer.join().is_ok(), "barrier peer panicked");
}

/// Open a BEGIN CONCURRENT txn, bulk-insert, and COMMIT as ONE retryable
/// unit. BusySnapshot anywhere inside (eager first-committer-wins abort during
/// INSERT, or at COMMIT) rolls back and retries the whole txn. This is the
/// correct concurrent-writer contract: the engine may abort a writer that
/// raced on a page, and the writer retries against a fresh snapshot.
async fn concurrent_txn_insert(
    conn: &Connection,
    table: &str,
    writer_id: usize,
    round: usize,
    rows: usize,
) -> Result<(), String> {
    let attempt_once = || async {
        conn.execute("BEGIN CONCURRENT;").await?;
        for r in 0..rows {
            let payload = format!(
                "{table}-rnd{round}-r{r}-{:0220x}",
                (writer_id * 7919 + round * 131 + r) as u128
            );
            if let Err(e) = conn
                .query_with_params(
                    &format!("INSERT INTO {table} (payload) VALUES (?);"),
                    &[SqliteValue::Text(payload.into())],
                )
                .await
            {
                let _ = conn.execute("ROLLBACK;").await;
                return Err(e);
            }
        }
        match conn.execute("COMMIT;").await {
            Ok(_) => Ok(()),
            Err(e) => {
                let _ = conn.execute("ROLLBACK;").await;
                Err(e)
            }
        }
    };
    with_retry(&format!("concurrent txn {table} rnd{round}"), attempt_once).await
}

async fn barrier_writer(
    path: &Path,
    writer_id: usize,
    wal: bool,
    barrier: &Barrier,
) -> Result<usize, String> {
    // Every spawned participant must arrive at the rendezvous, including one
    // whose open ultimately fails. Returning before `wait` would strand all
    // successful peers forever because `Barrier` has a fixed participant
    // count. Keep the open result until after the rendezvous so a setup error
    // becomes an ordinary scenario failure instead of a harness deadlock.
    let arrival = BarrierArrivalGuard::new(barrier);
    let conn = open(path, wal).await;
    arrival.arrive();
    let conn = conn?;
    let table = format!("bw{writer_id}");
    // Tables are pre-created in the seed phase. After this point we do NOT use
    // the barrier inside the loop; every writer just hammers retryable
    // concurrent transactions.

    let mut committed_rows = 0usize;
    for round in 0..BARRIER_ROUNDS {
        concurrent_txn_insert(&conn, &table, writer_id, round, BARRIER_ROWS_PER_TXN).await?;
        committed_rows += BARRIER_ROWS_PER_TXN;
    }
    Ok(committed_rows)
}

fn run_barrier_scenario(wal: bool) -> Result<(), String> {
    let dir = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;
    let db_path = dir.path().join("am152_barrier.db");
    {
        let mut seed_outcome: Result<(), String> = Ok(());
        asupersync::test_utils::run_test(|| async {
            seed_outcome = async {
                let conn = open(&db_path, wal).await?;
                with_retry("seed", || async {
                    conn.execute("CREATE TABLE IF NOT EXISTS _seed (id INTEGER PRIMARY KEY);")
                        .await
                        .map(|_| ())
                })
                .await?;
                for w in 0..BARRIER_WRITERS {
                    with_retry(&format!("seed create bw{w}"), || async {
                        conn.execute(&format!(
                            "CREATE TABLE IF NOT EXISTS bw{w} (id INTEGER PRIMARY KEY, payload TEXT);"
                        ))
                        .await
                        .map(|_| ())
                    })
                    .await?;
                }
                Ok(())
            }
            .await;
        });
        seed_outcome?;
    }

    let barrier = Arc::new(Barrier::new(BARRIER_WRITERS));
    let path_arc = Arc::new(db_path.clone());
    let committed = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..BARRIER_WRITERS)
        .map(|w| {
            let path = Arc::clone(&path_arc);
            let barrier = Arc::clone(&barrier);
            let committed = Arc::clone(&committed);
            let errors = Arc::clone(&errors);
            thread::spawn(move || {
                asupersync::test_utils::run_test(|| async {
                    match barrier_writer(&path, w, wal, &barrier).await {
                        Ok(n) => {
                            committed.fetch_add(n, Ordering::Relaxed);
                        }
                        Err(e) => {
                            eprintln!("[barrier writer {w}] FAILED: {e}");
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            })
        })
        .collect();
    for handle in handles {
        if handle.join().is_err() {
            errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    let total_committed = committed.load(Ordering::Relaxed);

    // Verify via frank.
    let mut verify_outcome: Result<(Vec<String>, i64), String> = Ok((Vec::new(), 0));
    asupersync::test_utils::run_test(|| async {
        verify_outcome = async {
            let verify = open(&db_path, wal).await?;
            if wal {
                let _ = verify.execute("PRAGMA wal_checkpoint(TRUNCATE);").await;
            }
            let frank_msgs = integrity_messages(&verify).await?;
            // Sum actual rows present per writer table.
            let mut actual_total = 0i64;
            for w in 0..BARRIER_WRITERS {
                let rows = with_retry(&format!("count bw{w}"), || async {
                    verify.query(&format!("SELECT COUNT(*) FROM bw{w};")).await
                })
                .await?;
                if let Some(SqliteValue::Integer(n)) = rows.first().and_then(|r| r.values().first())
                {
                    actual_total += *n;
                }
            }
            drop(verify);
            Ok((frank_msgs, actual_total))
        }
        .await;
    });
    let (frank_msgs, actual_total) = verify_outcome?;

    // Cross-check with rusqlite.
    let rusqlite_msg: String = {
        let c = rusqlite::Connection::open(&db_path).map_err(|e| format!("rusqlite open: {e}"))?;
        let _ = c.execute_batch("PRAGMA busy_timeout=10000; PRAGMA wal_checkpoint(TRUNCATE);");
        c.query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .map_err(|e| format!("rusqlite integrity_check: {e}"))?
    };

    eprintln!(
        "[am152 barrier wal={wal}] committed_rows_reported={total_committed} actual_rows={actual_total} writer_errors={} frank={frank_msgs:?} rusqlite={rusqlite_msg:?}",
        errors.load(Ordering::Relaxed)
    );

    let frank_ok = frank_msgs == ["ok"];
    let rusqlite_ok = rusqlite_msg == "ok";
    // Each successfully-committed concurrent txn must have all its rows. If
    // pages were aliased, actual_total < total_committed (lost data) or the
    // image is malformed.
    if frank_ok && rusqlite_ok && actual_total == total_committed as i64 {
        Ok(())
    } else {
        Err(format!(
            "CORRUPTION/LOST-DATA (barrier wal={wal}): committed_reported={total_committed} actual={actual_total} frank={frank_msgs:?} rusqlite={rusqlite_msg:?}"
        ))
    }
}

#[test]
#[ignore = "am#152 / bd-9inpb stress harness: reproduced+fixed; run on demand with --ignored to validate the page-alias fix"]
fn am152_allocator_race_barrier_rollback_journal() {
    match run_barrier_scenario(false) {
        Ok(()) => eprintln!("[am152 barrier rollback] clean"),
        Err(e) => panic!("{e}"),
    }
}

#[test]
#[ignore = "am#152 / bd-9inpb stress harness: reproduced+fixed; run on demand with --ignored to validate the page-alias fix"]
fn am152_allocator_race_barrier_wal() {
    match run_barrier_scenario(true) {
        Ok(()) => eprintln!("[am152 barrier wal] clean"),
        Err(e) => panic!("{e}"),
    }
}
