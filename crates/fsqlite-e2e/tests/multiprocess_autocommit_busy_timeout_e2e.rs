#![recursion_limit = "512"]

//! bd-reality-check-1022-llk6g.5: writers in different processes must honor
//! `busy_timeout`. Stock SQLite queues contended autocommit writers behind its
//! busy handler, so N processes x M autocommit INSERTs into one WAL database
//! with `busy_timeout=5000` commit every row. fsqlite 0.4.12 committed 150 of
//! 400, returning `database is busy` / `snapshot conflict on pages: 2` /
//! `recovery in progress` within ~32 ms instead of spending the budget.
//!
//! Each writer is a real child process (this test binary re-run with
//! `CHILD_ENV` set). Children open their connection, report ready, and start
//! together when the parent creates the `go` file. Stock SQLite (bundled
//! rusqlite) runs the same shape as the oracle.
//!
//! fsqlite writers run three ways: `execute` and `query` on one connection,
//! and a fresh connection per INSERT through `query`, which is what a CLI
//! invocation per statement does. Main on Linux committed every row through
//! `execute` but only 276 of 400 in the CLI shape: `query` sends DML through
//! the statement path, whose autocommit conflict retry excluded DML, so the
//! commit's `snapshot conflict on pages` came back without waiting.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use fsqlite::Connection;

const CHILD_ENV: &str = "FSQLITE_MP_BUSY_CHILD";

#[derive(Debug)]
struct ChildReport {
    committed: usize,
    errors: Vec<String>,
}

#[derive(Debug)]
struct ShapeResult {
    committed: usize,
    errors: Vec<String>,
    elapsed: Duration,
    final_rows: i64,
    integrity: String,
}

fn seed(db: &Path) {
    let stock = rusqlite::Connection::open(db).expect("seed open");
    stock
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE t(id INTEGER PRIMARY KEY, worker INTEGER NOT NULL, seq INTEGER NOT NULL);",
        )
        .expect("seed schema");
}

fn wait_for(path: &Path, limit: Duration) -> bool {
    let started = Instant::now();
    while !path.exists() {
        if started.elapsed() > limit {
            return false;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    true
}

/// The text after `marker` on each line carrying it. libtest prints
/// `test <name> ... ` without a newline, so the child's first line follows it.
fn events<'a>(stdout: &'a str, marker: &'a str) -> impl Iterator<Item = &'a str> {
    stdout
        .lines()
        .filter_map(move |line| line.find(marker).map(|at| &line[at + marker.len()..]))
}

fn parse_report(stdout: &str) -> Option<ChildReport> {
    let committed = events(stdout, "event=mp_busy_child ")
        .next()?
        .split_whitespace()
        .find_map(|field| field.strip_prefix("committed="))?
        .parse()
        .ok()?;
    let errors = events(stdout, "event=mp_busy_error ")
        .map(str::to_owned)
        .collect();
    Some(ChildReport { committed, errors })
}

fn run_shape(backend: &str, workers: usize, rows: usize, busy_timeout_ms: u64) -> ShapeResult {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("mp.db");
    seed(&db);
    let go = dir.path().join("go");
    let exe = std::env::current_exe().expect("test binary path");
    let children: Vec<(usize, Child)> = (0..workers)
        .map(|worker| {
            let child = Command::new(&exe)
                .args([
                    "--exact",
                    "mp_busy_writer_child",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(
                    CHILD_ENV,
                    format!(
                        "{backend}|{}|{}|{worker}|{rows}|{busy_timeout_ms}",
                        db.display(),
                        dir.path().display()
                    ),
                )
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("spawn writer");
            (worker, child)
        })
        .collect();
    for worker in 0..workers {
        assert!(
            wait_for(&ready_path(dir.path(), worker), Duration::from_secs(120)),
            "{backend}: writer {worker} never became ready"
        );
    }
    let started = Instant::now();
    std::fs::write(&go, b"go").expect("start writers");
    let mut committed = 0;
    let mut errors = Vec::new();
    for (worker, child) in children {
        let output = child.wait_with_output().expect("writer output");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let report = parse_report(&stdout).unwrap_or_else(|| {
            panic!(
                "{backend}: writer {worker} produced no report (status {:?}):\n{stdout}\n{}",
                output.status.code(),
                String::from_utf8_lossy(&output.stderr)
            )
        });
        committed += report.committed;
        errors.extend(report.errors);
    }
    let elapsed = started.elapsed();
    let stock = rusqlite::Connection::open(&db).expect("verify open");
    let final_rows: i64 = stock
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .expect("count");
    let integrity: String = stock
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity_check");
    let result = ShapeResult {
        committed,
        errors,
        elapsed,
        final_rows,
        integrity,
    };
    eprintln!(
        "event=mp_busy_shape backend={backend} workers={workers} rows={rows} busy_timeout_ms={busy_timeout_ms} \
         committed={} errors={} elapsed_ms={} final_rows={} integrity={} sample_errors={:?}",
        result.committed,
        result.errors.len(),
        result.elapsed.as_millis(),
        result.final_rows,
        result.integrity,
        result.errors.iter().take(5).collect::<Vec<_>>()
    );
    result
}

fn ready_path(dir: &Path, worker: usize) -> PathBuf {
    dir.join(format!("ready-{worker}"))
}

fn insert_sql(worker: usize, seq: usize) -> String {
    format!("INSERT INTO t(worker, seq) VALUES ({worker}, {seq})")
}

/// The writer process body; a no-op unless the parent set `CHILD_ENV`.
#[test]
fn mp_busy_writer_child() {
    let Ok(spec) = std::env::var(CHILD_ENV) else {
        return;
    };
    let fields: Vec<&str> = spec.split('|').collect();
    let [backend, db, dir, worker, rows, busy_timeout_ms] = fields[..] else {
        panic!("malformed child spec {spec}");
    };
    let (db, dir) = (PathBuf::from(db), PathBuf::from(dir));
    let worker: usize = worker.parse().expect("worker");
    let rows: usize = rows.parse().expect("rows");
    let busy_timeout_ms: u64 = busy_timeout_ms.parse().expect("busy_timeout_ms");
    let mut committed = 0;
    let mut errors = Vec::new();
    match backend {
        "stock" => {
            let conn = rusqlite::Connection::open(&db).expect("stock open");
            conn.busy_timeout(Duration::from_millis(busy_timeout_ms))
                .expect("stock busy_timeout");
            std::fs::write(ready_path(&dir, worker), b"").expect("ready");
            assert!(wait_for(&dir.join("go"), Duration::from_secs(120)), "no go");
            for seq in 0..rows {
                match conn.execute(&insert_sql(worker, seq), []) {
                    Ok(_) => committed += 1,
                    Err(error) => errors.push(error.to_string()),
                }
            }
        }
        "fsqlite" | "fsqlite-query" => {
            let via_query = backend == "fsqlite-query";
            let (committed, errors) = (&mut committed, &mut errors);
            asupersync::test_utils::run_test(|| async move {
                let conn = Connection::open(db.to_str().expect("utf8 path"))
                    .await
                    .expect("fsqlite open");
                conn.execute(&format!("PRAGMA busy_timeout={busy_timeout_ms}"))
                    .await
                    .expect("fsqlite busy_timeout");
                std::fs::write(ready_path(&dir, worker), b"").expect("ready");
                assert!(wait_for(&dir.join("go"), Duration::from_secs(120)), "no go");
                for seq in 0..rows {
                    let sql = insert_sql(worker, seq);
                    let outcome = if via_query {
                        conn.query(&sql).await.map(drop)
                    } else {
                        conn.execute(&sql).await.map(drop)
                    };
                    match outcome {
                        Ok(()) => *committed += 1,
                        Err(error) => errors.push(error.to_string()),
                    }
                }
                let _ = conn.close().await;
            });
        }
        // The CLI shape the bead measured: every INSERT is its own
        // invocation, so it opens a connection, sets busy_timeout, runs the
        // statement through `query` (as the shell does) and closes.
        "fsqlite-fresh" => {
            let (committed, errors) = (&mut committed, &mut errors);
            asupersync::test_utils::run_test(|| async move {
                let path = db.to_str().expect("utf8 path");
                std::fs::write(ready_path(&dir, worker), b"").expect("ready");
                assert!(wait_for(&dir.join("go"), Duration::from_secs(120)), "no go");
                for seq in 0..rows {
                    let outcome = async {
                        let conn = Connection::open(path).await?;
                        conn.execute(&format!("PRAGMA busy_timeout={busy_timeout_ms}"))
                            .await?;
                        let inserted = conn.query(&insert_sql(worker, seq)).await.map(drop);
                        let closed = conn.close().await;
                        inserted.and(closed)
                    }
                    .await;
                    match outcome {
                        Ok(()) => *committed += 1,
                        Err(error) => errors.push(error.to_string()),
                    }
                }
            });
        }
        other => panic!("unknown backend {other}"),
    }
    for error in &errors {
        println!("event=mp_busy_error {error}");
    }
    println!(
        "event=mp_busy_child backend={backend} worker={worker} committed={committed} errors={}",
        errors.len()
    );
}

fn assert_all_committed(label: &str, result: &ShapeResult, expected: usize) {
    assert_eq!(result.integrity, "ok", "{label}: integrity_check");
    assert_eq!(
        usize::try_from(result.final_rows).expect("row count"),
        result.committed,
        "{label}: every reported commit is durable and nothing else is"
    );
    assert_eq!(
        result.committed, expected,
        "{label}: every autocommit INSERT must commit within busy_timeout; errors: {:?}",
        result.errors
    );
}

#[test]
fn cross_process_autocommit_writers_honor_busy_timeout() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    for (workers, rows) in [(4, 100), (8, 50)] {
        let label = format!("{workers}x{rows}");
        let stock = run_shape("stock", workers, rows, 5000);
        assert_all_committed(&format!("{label} stock oracle"), &stock, workers * rows);
        // `execute` and `query` take different statement paths; both must
        // retry an autocommit conflict within busy_timeout.
        for backend in ["fsqlite", "fsqlite-query"] {
            let frank = run_shape(backend, workers, rows, 5000);
            assert_all_committed(&format!("{label} {backend}"), &frank, workers * rows);
            let limit = stock.elapsed * 3 + Duration::from_secs(1);
            assert!(
                frank.elapsed <= limit,
                "{label}: {backend} took {:?}, more than 3x stock's {:?} (+1s)",
                frank.elapsed,
                stock.elapsed
            );
        }
    }
}

/// The shape behind the bead's 150/400: one connection per INSERT, as when
/// each statement is a separate `fsqlite` CLI invocation. Opening a
/// connection costs more than stock's, so this checks only that every
/// INSERT commits, not the wall time.
#[test]
fn cross_process_fresh_connection_writers_honor_busy_timeout() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    let stock = run_shape("stock", 4, 100, 5000);
    assert_all_committed("4x100 stock oracle", &stock, 400);
    let frank = run_shape("fsqlite-fresh", 4, 100, 5000);
    assert_all_committed("4x100 fsqlite-fresh", &frank, 400);
}

const IMMEDIATE_CHILD_ENV: &str = "FSQLITE_MP_IMMEDIATE_CHILD";
const IMMEDIATE_HOLD: Duration = Duration::from_secs(2);

/// bd-reality-check-1022-llk6g.6 child. Role `a` holds `BEGIN IMMEDIATE` with
/// one INSERT for `IMMEDIATE_HOLD`, then commits; role `b` runs one autocommit
/// INSERT once `a` has begun. Reports `committed=0|1` and the role's elapsed
/// time from the go signal.
#[test]
fn mp_immediate_child() {
    let Ok(spec) = std::env::var(IMMEDIATE_CHILD_ENV) else {
        return;
    };
    let fields: Vec<&str> = spec.split('|').collect();
    let [backend, db, dir, role] = fields[..] else {
        panic!("malformed child spec {spec}");
    };
    let (db, dir) = (PathBuf::from(db), PathBuf::from(dir));
    let began = dir.join("a-began");
    let worker = usize::from(role == "b");
    let mut outcome: std::result::Result<(), String> = Ok(());
    let mut elapsed = Duration::ZERO;
    match backend {
        "stock" => {
            let conn = rusqlite::Connection::open(&db).expect("stock open");
            conn.busy_timeout(Duration::from_millis(5000))
                .expect("stock busy_timeout");
            std::fs::write(dir.join(format!("ready-{role}")), b"").expect("ready");
            assert!(wait_for(&dir.join("go"), Duration::from_secs(120)), "no go");
            let started = Instant::now();
            outcome = if role == "a" {
                conn.execute_batch("BEGIN IMMEDIATE")
                    .and_then(|()| conn.execute(&insert_sql(worker, 0), []).map(drop))
                    .map_err(|error| error.to_string())
                    .and_then(|()| {
                        std::fs::write(&began, b"").expect("began");
                        std::thread::sleep(IMMEDIATE_HOLD);
                        conn.execute_batch("COMMIT")
                            .map_err(|error| error.to_string())
                    })
            } else {
                assert!(wait_for(&began, Duration::from_secs(120)), "a never began");
                conn.execute(&insert_sql(worker, 0), [])
                    .map(drop)
                    .map_err(|error| error.to_string())
            };
            elapsed = started.elapsed();
        }
        "fsqlite" => {
            let (outcome, elapsed) = (&mut outcome, &mut elapsed);
            asupersync::test_utils::run_test(|| async move {
                let conn = Connection::open(db.to_str().expect("utf8 path"))
                    .await
                    .expect("fsqlite open");
                conn.execute("PRAGMA busy_timeout=5000")
                    .await
                    .expect("fsqlite busy_timeout");
                std::fs::write(dir.join(format!("ready-{role}")), b"").expect("ready");
                assert!(wait_for(&dir.join("go"), Duration::from_secs(120)), "no go");
                let started = Instant::now();
                *outcome = async {
                    if role == "a" {
                        conn.execute("BEGIN IMMEDIATE").await?;
                        conn.execute(&insert_sql(worker, 0)).await?;
                        std::fs::write(&began, b"").expect("began");
                        std::thread::sleep(IMMEDIATE_HOLD);
                        conn.execute("COMMIT").await?;
                    } else {
                        assert!(wait_for(&began, Duration::from_secs(120)), "a never began");
                        conn.execute(&insert_sql(worker, 0)).await?;
                    }
                    Ok::<(), fsqlite::FrankenError>(())
                }
                .await
                .map_err(|error| error.to_string());
                *elapsed = started.elapsed();
                let _ = conn.close().await;
            });
        }
        other => panic!("unknown backend {other}"),
    }
    println!(
        "event=mp_immediate_child backend={backend} role={role} committed={} elapsed_ms={} error={:?}",
        u8::from(outcome.is_ok()),
        elapsed.as_millis(),
        outcome.err()
    );
}

/// `(committed, elapsed)` for one role, parsed from its child's stdout.
fn parse_immediate(stdout: &str) -> Option<(bool, Duration, String)> {
    let line = events(stdout, "event=mp_immediate_child ").next()?;
    let field = |name: &str| {
        line.split_whitespace()
            .find_map(|field| field.strip_prefix(name))
            .map(str::to_owned)
    };
    let committed = field("committed=")? == "1";
    let elapsed = Duration::from_millis(field("elapsed_ms=")?.parse().ok()?);
    Some((committed, elapsed, line.to_owned()))
}

fn run_immediate(backend: &str) -> [(bool, Duration, String); 2] {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("imm.db");
    seed(&db);
    let exe = std::env::current_exe().expect("test binary path");
    let children: Vec<Child> = ["a", "b"]
        .iter()
        .map(|role| {
            Command::new(&exe)
                .args([
                    "--exact",
                    "mp_immediate_child",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(
                    IMMEDIATE_CHILD_ENV,
                    format!("{backend}|{}|{}|{role}", db.display(), dir.path().display()),
                )
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("spawn role")
        })
        .collect();
    for role in ["a", "b"] {
        assert!(
            wait_for(
                &dir.path().join(format!("ready-{role}")),
                Duration::from_secs(120)
            ),
            "{backend}: role {role} never became ready"
        );
    }
    std::fs::write(dir.path().join("go"), b"go").expect("start roles");
    let reports: Vec<_> = children
        .into_iter()
        .zip(["a", "b"])
        .map(|(child, role)| {
            let output = child.wait_with_output().expect("role output");
            let stdout = String::from_utf8_lossy(&output.stdout);
            parse_immediate(&stdout).unwrap_or_else(|| {
                panic!(
                    "{backend}: role {role} produced no report (status {:?}):\n{stdout}\n{}",
                    output.status.code(),
                    String::from_utf8_lossy(&output.stderr)
                )
            })
        })
        .collect();
    let stock = rusqlite::Connection::open(&db).expect("verify open");
    let rows: Vec<i64> = stock
        .prepare("SELECT worker FROM t ORDER BY worker")
        .expect("prepare")
        .query_map([], |row| row.get(0))
        .expect("query")
        .collect::<rusqlite::Result<_>>()
        .expect("rows");
    let integrity: String = stock
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity_check");
    eprintln!(
        "event=mp_immediate_shape backend={backend} rows={rows:?} integrity={integrity} a={} b={}",
        reports[0].2, reports[1].2
    );
    assert_eq!(integrity, "ok", "{backend}: integrity_check");
    let [a, b]: [(bool, Duration, String); 2] = reports.try_into().expect("two roles");
    let expected: Vec<i64> = [(a.0, 0), (b.0, 1)]
        .into_iter()
        .filter_map(|(committed, worker)| committed.then_some(worker))
        .collect();
    assert_eq!(
        rows, expected,
        "{backend}: exactly the reported commits are durable"
    );
    [a, b]
}

#[test]
fn immediate_transaction_commit_survives_a_cross_process_autocommit_writer() {
    if std::env::var_os(CHILD_ENV).is_some() || std::env::var_os(IMMEDIATE_CHILD_ENV).is_some() {
        return;
    }
    // Stock: A commits after its 2 s hold, B's insert waits behind it.
    // fsqlite 0.4.12: A's COMMIT failed `database is busy` at ~7 s and its row
    // was rolled back while B took ~6.6 s (GH#519 lock-order inversion).
    let shape_failure = |[a, b]: &[(bool, Duration, String); 2]| {
        if !a.0 {
            Some(format!("the IMMEDIATE transaction must commit: {}", a.2))
        } else if !b.0 {
            Some(format!("the autocommit writer must commit: {}", b.2))
        } else if a.1 >= IMMEDIATE_HOLD + Duration::from_millis(1500) {
            Some(format!("A's COMMIT must not wait on B: {}", a.2))
        } else if b.1 >= IMMEDIATE_HOLD + Duration::from_millis(2500) {
            Some(format!("B waits only for A: {}", b.2))
        } else {
            None
        }
    };
    // The bounds assume the host commits within B's 5 s busy_timeout. On an
    // overloaded build worker stock itself once took ~57 s to commit A
    // (probably a stalled WAL fsync), so its B gave up after 5 s; such a run
    // cannot judge fsqlite.
    if let Some(failure) = shape_failure(&run_immediate("stock")) {
        eprintln!("event=mp_immediate_inconclusive stock missed the shape on this host: {failure}");
        return;
    }
    if let Some(failure) = shape_failure(&run_immediate("fsqlite")) {
        panic!("fsqlite: {failure}");
    }
}

#[test]
fn cross_process_writers_without_busy_timeout_stay_consistent() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    // busy_timeout=0: contention may fail statements (as in stock), but every
    // failure is a busy-family error returned promptly, and what reported
    // success is exactly what is durable.
    let frank = run_shape("fsqlite", 4, 100, 0);
    assert_eq!(frank.integrity, "ok", "integrity_check");
    assert_eq!(
        usize::try_from(frank.final_rows).expect("row count"),
        frank.committed,
        "every reported commit is durable and nothing else is"
    );
    assert!(
        frank
            .errors
            .iter()
            .all(|error| error.contains("busy") || error.contains("locked")),
        "only busy-family failures: {:?}",
        frank.errors
    );
    assert!(
        frank.elapsed < Duration::from_secs(60),
        "busy_timeout=0 writers must not wait: {:?}",
        frank.elapsed
    );
}
