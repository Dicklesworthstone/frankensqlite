//! bd-xvv8f — committed CREATE TABLE not visible to the SAME connection's next
//! statement under concurrent commits (DDL schema-cache staleness).
//!
//! Report: under concurrent writers each on their OWN `fsqlite::Connection` to
//! the same file-backed DB, a connection that commits its own
//! `CREATE TABLE wN_tX` can then have its immediately-following
//! `INSERT INTO wN_tX ...` fail with `internal error: no such table: wN_tX`.
//! The DDL is committed but the connection's local schema view doesn't reflect
//! it before the next statement runs, under concurrent-commit interleaving.
//!
//! This is a correctness/usability defect (the writer bails on a NON-transient
//! error), NOT corruption. It was originally observed in the am#152 harness
//! (crates/fsqlite-e2e/tests/am152_allocator_race_repro.rs) before tables were
//! moved into a pre-create seed phase, which masked it. This file isolates the
//! create-then-insert window so the bug fires deterministically.

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use fsqlite::{Connection, FrankenError, SqliteValue};

const WRITERS: usize = 12;
const TABLES_PER_WRITER: usize = 8;
const ROWS_PER_TABLE: usize = 6;
const RETRY_BUDGET: Duration = Duration::from_secs(30);

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
    let conn = Connection::open(path.to_string_lossy().to_string())
        .await
        .map_err(|e| format!("open: {e}"))?;
    let _ = conn.execute("PRAGMA busy_timeout=10000;").await;
    if wal {
        with_retry("journal_mode=WAL", || async {
            conn.execute("PRAGMA journal_mode=WAL;").await.map(|_| ())
        })
        .await?;
    }
    Ok(conn)
}

/// Each writer creates its OWN tables and immediately inserts into each one,
/// on the SAME connection — the exact window where the bug fires. A
/// "no such table" right after a successful CREATE on the same connection is a
/// NON-transient failure and must never happen.
async fn writer_body(
    path: &Path,
    writer_id: usize,
    wal: bool,
    barrier: &Barrier,
) -> Result<(), String> {
    let conn = open(path, wal).await?;
    // Line all writers up so their CREATE/COMMIT calls interleave maximally.
    barrier.wait();

    for t in 0..TABLES_PER_WRITER {
        let table = format!("w{writer_id}_t{t}");
        with_retry(&format!("create {table}"), || async {
            conn.execute(&format!(
                "CREATE TABLE {table} (id INTEGER PRIMARY KEY, a TEXT, b INTEGER);"
            ))
            .await
            .map(|_| ())
        })
        .await?;
        // Immediately insert into the table this connection just created and
        // committed. This is where `no such table: {table}` is reported.
        for r in 0..ROWS_PER_TABLE {
            let payload = format!("w{writer_id}-t{t}-r{r}");
            with_retry(&format!("insert {table} r{r}"), || async {
                conn.query_with_params(
                    &format!("INSERT INTO {table} (a, b) VALUES (?, ?);"),
                    &[
                        SqliteValue::Text(payload.clone().into()),
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

fn run_scenario(wal: bool) -> Result<(), String> {
    let dir = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;
    let db_path = dir.path().join("xvv8f.db");

    // Seed the file so all writers open an existing db.
    {
        let mut seed_outcome: Result<(), String> = Ok(());
        asupersync::test_utils::run_test(|| async {
            let r: Result<(), String> = async {
                let conn = open(&db_path, wal).await?;
                with_retry("seed", || async {
                    conn.execute("CREATE TABLE IF NOT EXISTS _seed (id INTEGER PRIMARY KEY);")
                        .await
                        .map(|_| ())
                })
                .await?;
                Ok(())
            }
            .await;
            seed_outcome = r;
        });
        seed_outcome?;
    }

    let barrier = Arc::new(Barrier::new(WRITERS));
    let path_arc = Arc::new(db_path.clone());
    let no_such_table = Arc::new(AtomicUsize::new(0));
    let other_errors = Arc::new(AtomicUsize::new(0));
    let first_err: Arc<std::sync::Mutex<Option<String>>> = Arc::new(std::sync::Mutex::new(None));

    let handles: Vec<_> = (0..WRITERS)
        .map(|w| {
            let path = Arc::clone(&path_arc);
            let barrier = Arc::clone(&barrier);
            let no_such_table = Arc::clone(&no_such_table);
            let other_errors = Arc::clone(&other_errors);
            let first_err = Arc::clone(&first_err);
            thread::spawn(move || {
                asupersync::test_utils::run_test(|| async {
                    if let Err(e) = writer_body(&path, w, wal, &barrier).await {
                        if e.contains("no such table") {
                            no_such_table.fetch_add(1, Ordering::Relaxed);
                        } else {
                            other_errors.fetch_add(1, Ordering::Relaxed);
                        }
                        let mut slot = first_err.lock().unwrap();
                        if slot.is_none() {
                            *slot = Some(e);
                        }
                    }
                });
            })
        })
        .collect();
    for h in handles {
        let _ = h.join();
    }

    let nst = no_such_table.load(Ordering::Relaxed);
    let other = other_errors.load(Ordering::Relaxed);
    let first = first_err.lock().unwrap().clone();
    eprintln!(
        "[xvv8f wal={wal}] no_such_table_failures={nst} other_failures={other} first_err={first:?}"
    );

    if nst == 0 && other == 0 {
        Ok(())
    } else {
        Err(format!(
            "DDL VISIBILITY BUG (wal={wal}): no_such_table={nst} other={other} first_err={first:?}"
        ))
    }
}

#[test]
fn xvv8f_ddl_visibility_wal() {
    match run_scenario(true) {
        Ok(()) => eprintln!("[xvv8f wal] clean"),
        Err(e) => panic!("{e}"),
    }
}

#[test]
fn xvv8f_ddl_visibility_rollback_journal() {
    match run_scenario(false) {
        Ok(()) => eprintln!("[xvv8f rollback] clean"),
        Err(e) => panic!("{e}"),
    }
}

/// Deterministic, single-threaded root-cause probe.
///
/// Two connections C1 and C2 to the same file DB. C1 creates `foo` and commits.
/// C2 then creates an UNRELATED table `bar` and commits — this bumps the shared
/// on-disk schema_cookie *past* C1's local cookie. When C1 next runs
/// `INSERT INTO foo`, its stale-cache check sees the pager's schema_cookie no
/// longer matches its local cookie and triggers a FULL schema reload from
/// sqlite_master. If that reload drops C1's own committed `foo`, the INSERT
/// fails with "no such table: foo" — the exact bd-xvv8f symptom, reproduced
/// without any thread race.
async fn run_deterministic(wal: bool) -> Result<(), String> {
    let dir = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;
    let db_path = dir.path().join("xvv8f_det.db");

    // Seed so both connections open an existing db.
    {
        let conn = open(&db_path, wal).await?;
        conn.execute("CREATE TABLE IF NOT EXISTS _seed (id INTEGER PRIMARY KEY);")
            .await
            .map_err(|e| format!("seed: {e}"))?;
    }

    let c1 = open(&db_path, wal).await?;
    let c2 = open(&db_path, wal).await?;

    // C1 creates its own table and commits (autocommit).
    with_retry("c1 create foo", || async {
        c1.execute("CREATE TABLE foo (id INTEGER PRIMARY KEY, a TEXT);")
            .await
            .map(|_| ())
    })
    .await?;

    // C2 commits an UNRELATED DDL — bumps the shared schema_cookie past C1's.
    with_retry("c2 create bar", || async {
        c2.execute("CREATE TABLE bar (id INTEGER PRIMARY KEY, a TEXT);")
            .await
            .map(|_| ())
    })
    .await?;

    // C1 now inserts into the table it created. Must NOT see "no such table".
    let res = with_retry("c1 insert foo", || async {
        c1.query_with_params(
            "INSERT INTO foo (a) VALUES (?);",
            &[SqliteValue::Text("hello".into())],
        )
        .await
        .map(|_| ())
    })
    .await;

    match res {
        Ok(()) => Ok(()),
        Err(e) => Err(format!("DETERMINISTIC bd-xvv8f (wal={wal}): {e}")),
    }
}

#[test]
fn xvv8f_deterministic_wal() {
    asupersync::test_utils::run_test(|| async {
        match run_deterministic(true).await {
            Ok(()) => eprintln!("[xvv8f det wal] clean"),
            Err(e) => panic!("{e}"),
        }
    });
}

#[test]
fn xvv8f_deterministic_rollback_journal() {
    asupersync::test_utils::run_test(|| async {
        match run_deterministic(false).await {
            Ok(()) => eprintln!("[xvv8f det rollback] clean"),
            Err(e) => panic!("{e}"),
        }
    });
}
