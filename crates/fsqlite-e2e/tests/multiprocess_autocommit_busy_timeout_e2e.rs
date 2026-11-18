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
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use fsqlite::Connection;

const CHILD_ENV: &str = "FSQLITE_MP_BUSY_CHILD";

/// bd-bnbdo: every parent test here runs 4-8 writer processes. Run in
/// parallel inside one binary on a loaded worker, they starved stock's own
/// writers past busy_timeout (3-5 of 2400 'database is locked' in the stock
/// oracle arm). Each parent test holds this lock, so the default harness runs
/// them one at a time; the children are separate processes.
static PARENT_TESTS: Mutex<()> = Mutex::new(());

fn one_parent_test_at_a_time() -> MutexGuard<'static, ()> {
    PARENT_TESTS.lock().unwrap_or_else(PoisonError::into_inner)
}

/// bd-bnbdo: how many times a stock oracle arm runs before the test gives up
/// on this host.
const ORACLE_ATTEMPTS: usize = 3;

#[derive(Debug)]
struct ChildReport {
    committed: usize,
    /// `seq` of every row whose transaction reported success (explicit shapes).
    committed_seqs: Vec<usize>,
    errors: Vec<String>,
}

#[derive(Debug)]
struct ShapeResult {
    committed: usize,
    /// `(worker, seq)` of every reported commit (explicit shapes).
    committed_rows: Vec<(i64, i64)>,
    errors: Vec<String>,
    elapsed: Duration,
    final_rows: i64,
    /// `(worker, seq)` of every durable row, read by stock.
    durable_rows: Vec<(i64, i64)>,
    integrity: String,
    /// Writer stderr lines where the pager certified the durable db_size floor
    /// for a non-growing commit (`VNXJD FLOOR-CERTIFIED`, traced with
    /// `DW8OE_TRACE` in the own-table fsqlite shape).
    floor_certified: usize,
}

const FLOOR_CERTIFIED_MARKER: &str = "VNXJD FLOOR-CERTIFIED";

/// A `-own` backend gives every writer its own table `t<worker>`; `t` is then a
/// view over all of them, so the shape's counts and durable-row reads are the
/// same queries.
fn own_tables(backend: &str) -> bool {
    backend.ends_with("-own")
}

fn seed(db: &Path, backend: &str, workers: usize) {
    let stock = rusqlite::Connection::open(db).expect("seed open");
    let schema = if own_tables(backend) {
        let tables: String = (0..workers)
            .map(|worker| {
                format!(
                    "CREATE TABLE t{worker}(id INTEGER PRIMARY KEY, worker INTEGER NOT NULL, \
                     seq INTEGER NOT NULL, pad TEXT NOT NULL);"
                )
            })
            .collect();
        let union = (0..workers)
            .map(|worker| format!("SELECT worker, seq FROM t{worker}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        format!("PRAGMA journal_mode=WAL; {tables} CREATE VIEW t AS {union};")
    } else {
        "PRAGMA journal_mode=WAL;
         CREATE TABLE t(id INTEGER PRIMARY KEY, worker INTEGER NOT NULL, seq INTEGER NOT NULL);"
            .to_owned()
    };
    stock.execute_batch(&schema).expect("seed schema");
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
    let committed_seqs = events(stdout, "event=mp_busy_committed_seq ")
        .map(|seq| seq.trim().parse().expect("committed seq"))
        .collect();
    let errors = events(stdout, "event=mp_busy_error ")
        .map(str::to_owned)
        .collect();
    Some(ChildReport {
        committed,
        committed_seqs,
        errors,
    })
}

fn run_shape(backend: &str, workers: usize, rows: usize, busy_timeout_ms: u64) -> ShapeResult {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("mp.db");
    seed(&db, backend, workers);
    let go = dir.path().join("go");
    let exe = std::env::current_exe().expect("test binary path");
    let children: Vec<(usize, Child)> = (0..workers)
        .map(|worker| {
            let mut command = Command::new(&exe);
            command
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
                .stderr(Stdio::piped());
            if backend == "fsqlite-explicit-own" {
                command.env("DW8OE_TRACE", "1");
            }
            (worker, command.spawn().expect("spawn writer"))
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
    // A writer logs more than a pipe holds, and one blocked on a full pipe can
    // be holding the write lock. Waiting in worker order would then freeze the
    // whole shape, so every writer's pipes are drained while they all run.
    let draining: Vec<_> = children
        .into_iter()
        .map(|(worker, child)| (worker, std::thread::spawn(|| child.wait_with_output())))
        .collect();
    let mut committed = 0;
    let mut committed_rows = Vec::new();
    let mut errors = Vec::new();
    let mut floor_certified = 0;
    for (worker, drain) in draining {
        let output = drain.join().expect("drain thread").expect("writer output");
        let stdout = String::from_utf8_lossy(&output.stdout);
        floor_certified += String::from_utf8_lossy(&output.stderr)
            .lines()
            .filter(|line| line.contains(FLOOR_CERTIFIED_MARKER))
            .count();
        let report = parse_report(&stdout).unwrap_or_else(|| {
            panic!(
                "{backend}: writer {worker} produced no report (status {:?}):\n{stdout}\n{}",
                output.status.code(),
                String::from_utf8_lossy(&output.stderr)
            )
        });
        committed += report.committed;
        let worker = i64::try_from(worker).expect("worker");
        committed_rows.extend(
            report
                .committed_seqs
                .iter()
                .map(|&seq| (worker, i64::try_from(seq).expect("seq"))),
        );
        errors.extend(report.errors);
    }
    committed_rows.sort_unstable();
    let elapsed = started.elapsed();
    let stock = rusqlite::Connection::open(&db).expect("verify open");
    let final_rows: i64 = stock
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .expect("count");
    let durable_rows = stock
        .prepare("SELECT worker, seq FROM t ORDER BY worker, seq")
        .expect("prepare")
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .expect("query")
        .collect::<rusqlite::Result<_>>()
        .expect("durable rows");
    let integrity: String = stock
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity_check");
    let result = ShapeResult {
        committed,
        committed_rows,
        errors,
        elapsed,
        final_rows,
        durable_rows,
        integrity,
        floor_certified,
    };
    eprintln!(
        "event=mp_busy_shape backend={backend} workers={workers} rows={rows} busy_timeout_ms={busy_timeout_ms} \
         committed={} errors={} elapsed_ms={} final_rows={} integrity={} floor_certified={} sample_errors={:?}",
        result.committed,
        result.errors.len(),
        result.elapsed.as_millis(),
        result.final_rows,
        result.integrity,
        result.floor_certified,
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

/// The explicit shapes' INSERT: into `t`, or into the writer's own table. An
/// own-table row carries a 200-byte pad, so each table spans many pages and the
/// writers keep growing the file while others commit into existing pages.
fn explicit_insert_sql(backend: &str, worker: usize, seq: usize) -> String {
    if own_tables(backend) {
        format!(
            "INSERT INTO t{worker}(worker, seq, pad) VALUES ({worker}, {seq}, '{}')",
            "p".repeat(200)
        )
    } else {
        insert_sql(worker, seq)
    }
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
    let mut committed_seqs = Vec::new();
    let mut errors = Vec::new();
    match backend {
        // bd-reality-check-1022-llk6g.5.1: `BEGIN; INSERT; COMMIT` per row,
        // the way a CLI script runs it: every statement runs, an error is
        // recorded, and nothing rolls back. The row counts as committed only
        // when both its INSERT and its COMMIT succeeded.
        "stock-explicit" | "stock-explicit-own" => {
            let conn = rusqlite::Connection::open(&db).expect("stock open");
            conn.busy_timeout(Duration::from_millis(busy_timeout_ms))
                .expect("stock busy_timeout");
            std::fs::write(ready_path(&dir, worker), b"").expect("ready");
            assert!(wait_for(&dir.join("go"), Duration::from_secs(120)), "no go");
            for seq in 0..rows {
                let begin = conn.execute_batch("BEGIN");
                let insert = conn.execute(&explicit_insert_sql(backend, worker, seq), []);
                let commit = conn.execute_batch("COMMIT");
                if let Err(error) = &begin {
                    errors.push(format!("begin: {error}"));
                }
                if let Err(error) = &insert {
                    errors.push(format!("insert: {error}"));
                }
                if let Err(error) = &commit {
                    errors.push(format!(
                        "commit autocommit_after={}: {error}",
                        conn.is_autocommit()
                    ));
                }
                if insert.is_ok() && commit.is_ok() {
                    committed += 1;
                    committed_seqs.push(seq);
                }
            }
        }
        "fsqlite-explicit" | "fsqlite-explicit-own" => {
            let (committed, committed_seqs, errors) =
                (&mut committed, &mut committed_seqs, &mut errors);
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
                    let begin = conn.execute("BEGIN").await;
                    let insert = conn
                        .execute(&explicit_insert_sql(backend, worker, seq))
                        .await;
                    let commit = conn.execute("COMMIT").await;
                    if let Err(error) = &begin {
                        errors.push(format!("begin: {error}"));
                    }
                    if let Err(error) = &insert {
                        errors.push(format!("insert: {error}"));
                    }
                    if let Err(error) = &commit {
                        errors.push(format!(
                            "commit code={} autocommit_after={}: {error}",
                            error.extended_error_code(),
                            !conn.in_transaction()
                        ));
                    }
                    if insert.is_ok() && commit.is_ok() {
                        *committed += 1;
                        committed_seqs.push(seq);
                    }
                }
                let _ = conn.close().await;
            });
        }
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
    for seq in &committed_seqs {
        println!("event=mp_busy_committed_seq {seq}");
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

/// Why a stock oracle arm missed its own answer, if it did: integrity ok,
/// every reported commit durable and nothing else, every row committed, and
/// for the explicit shapes no error line and the durable rows exactly the
/// committed ones.
fn stock_oracle_miss(backend: &str, result: &ShapeResult, expected: usize) -> Option<String> {
    if result.integrity != "ok" {
        return Some(format!("integrity_check: {}", result.integrity));
    }
    if usize::try_from(result.final_rows).ok() != Some(result.committed) {
        return Some(format!(
            "{} durable rows for {} reported commits",
            result.final_rows, result.committed
        ));
    }
    if result.committed != expected || !result.errors.is_empty() {
        return Some(format!(
            "{} of {expected} committed; errors: {:?}",
            result.committed,
            result.errors.iter().take(5).collect::<Vec<_>>()
        ));
    }
    if backend.contains("explicit") && result.durable_rows != result.committed_rows {
        return Some("the durable rows are not the committed rows".to_owned());
    }
    None
}

/// bd-bnbdo: run a stock-only arm as the oracle for a shape. Stock is the
/// reference here, not the subject: a stock arm that misses its own answer
/// means the host starved a writer past busy_timeout, and such a run cannot
/// judge fsqlite. It is rerun, up to [`ORACLE_ATTEMPTS`] times; if every
/// attempt misses, the test fails as "oracle invalid on this host". It never
/// passes without a valid oracle, and the fsqlite arms are never rerun.
fn stock_oracle(backend: &str, workers: usize, rows: usize) -> ShapeResult {
    let expected = workers * rows;
    let mut misses = Vec::new();
    for attempt in 1..=ORACLE_ATTEMPTS {
        let result = run_shape(backend, workers, rows, 5000);
        let Some(miss) = stock_oracle_miss(backend, &result, expected) else {
            return result;
        };
        eprintln!(
            "event=mp_busy_oracle_invalid backend={backend} workers={workers} rows={rows} \
             attempt={attempt} miss={miss}"
        );
        misses.push(miss);
    }
    panic!(
        "{backend} {workers}x{rows}: oracle invalid on this host, rerun: the stock arm missed \
         its own answer in all {ORACLE_ATTEMPTS} attempts: {misses:?}"
    );
}

#[test]
fn cross_process_autocommit_writers_honor_busy_timeout() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    let _one_at_a_time = one_parent_test_at_a_time();
    for (workers, rows) in [(4, 100), (8, 50)] {
        let label = format!("{workers}x{rows}");
        let stock = stock_oracle("stock", workers, rows);
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
    let _one_at_a_time = one_parent_test_at_a_time();
    stock_oracle("stock", 4, 100);
    let frank = run_shape("fsqlite-fresh", 4, 100, 5000);
    assert_all_committed("4x100 fsqlite-fresh", &frank, 400);
}

/// The error text of a COMMIT refused with BusySnapshot (517) after which the
/// connection was back in autocommit mode.
const REFUSED_COMMIT_ENDED: &str = "commit code=517 autocommit_after=true: ";

/// bd-reality-check-1022-llk6g.5.1: processes running explicit `BEGIN; INSERT;
/// COMMIT` transactions on ONE table, CLI-style (no ROLLBACK after an error).
/// Stock commits every row: a write-first transaction waits for the write
/// lock at its INSERT. fsqlite's page MVCC refuses the second committer of a
/// leaf page with BusySnapshot (first-committer-wins, the owner's design), and
/// that refusal must end its transaction, so it costs exactly its own row. On
/// main 714e81184 the refused transaction stayed open: the next BEGIN failed
/// "within a transaction", the next INSERTs landed in the doomed transaction,
/// and 8 CLI writers x 300 committed 300-312 of 2400 rows.
#[test]
fn cross_process_explicit_writers_on_one_table_lose_only_refused_transactions() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    let _one_at_a_time = one_parent_test_at_a_time();
    let (workers, rows) = (4, 100);
    stock_oracle("stock-explicit", workers, rows);

    let frank = run_shape("fsqlite-explicit", workers, rows, 5000);
    assert_eq!(frank.integrity, "ok", "fsqlite-explicit: integrity_check");
    assert_eq!(
        frank.durable_rows, frank.committed_rows,
        "fsqlite-explicit: the durable rows are exactly the transactions whose COMMIT \
         succeeded (no refused row became durable, no later row was lost)"
    );
    let (refused, other): (Vec<&String>, Vec<&String>) = frank
        .errors
        .iter()
        .partition(|error| error.starts_with(REFUSED_COMMIT_ENDED));
    assert!(
        other.is_empty(),
        "fsqlite-explicit: the only error may be a BusySnapshot COMMIT refusal that ended its \
         transaction; got {} others, e.g. {:?}",
        other.len(),
        other.iter().take(5).collect::<Vec<_>>()
    );
    assert_eq!(
        frank.committed + refused.len(),
        workers * rows,
        "fsqlite-explicit: each refused transaction costs exactly its own row"
    );
    assert!(
        !refused.is_empty(),
        "non-vacuity: {workers} writers on one leaf page produced no COMMIT refusal, so this \
         run did not exercise the refused-COMMIT path"
    );
}

/// bd-reality-check-1022-llk6g.5.2: processes running explicit transactions on
/// their OWN tables, CLI-style. A commit that does not grow the file, flushing
/// after a peer published growth between its prep and its append gate, was
/// refused "snapshot conflict on pages: 1" by the bd-vnxjd db_size floor gate
/// (130 per 6 x 2400 transactions once .5.1 ended refused transactions). It now
/// certifies the durable floor. Two writers growing in the same window still
/// alias their EOF pages and the second is refused
/// (bd-reality-check-1022-llk6g.5.3), which with .5.1 costs exactly its own
/// row. Non-vacuity: the pager traces each floor certification under
/// `DW8OE_TRACE`, and the run must have made at least one.
#[test]
fn cross_process_explicit_writers_on_own_tables_certify_the_durable_floor() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    let _one_at_a_time = one_parent_test_at_a_time();
    let (workers, rows) = (8, 300);
    stock_oracle("stock-explicit-own", workers, rows);

    let frank = run_shape("fsqlite-explicit-own", workers, rows, 5000);
    assert_eq!(
        frank.integrity, "ok",
        "fsqlite-explicit-own: integrity_check"
    );
    assert_eq!(
        frank.durable_rows, frank.committed_rows,
        "fsqlite-explicit-own: the durable rows are exactly the transactions whose COMMIT succeeded"
    );
    let floor_refusals: Vec<&String> = frank
        .errors
        .iter()
        .filter(|error| error.contains("(snapshot conflict on pages: 1)"))
        .collect();
    assert!(
        floor_refusals.is_empty(),
        "fsqlite-explicit-own: {} commits refused on the db_size floor; a non-growing commit must \
         certify the durable floor instead: {:?}",
        floor_refusals.len(),
        floor_refusals.iter().take(5).collect::<Vec<_>>()
    );
    let (refused, other): (Vec<&String>, Vec<&String>) = frank
        .errors
        .iter()
        .partition(|error| error.starts_with(REFUSED_COMMIT_ENDED));
    assert!(
        other.is_empty(),
        "fsqlite-explicit-own: the only error may be a BusySnapshot COMMIT refusal that ended its \
         transaction; got {:?}",
        other.iter().take(5).collect::<Vec<_>>()
    );
    assert_eq!(
        frank.committed + refused.len(),
        workers * rows,
        "fsqlite-explicit-own: each refused transaction costs exactly its own row"
    );
    assert!(
        frank.floor_certified > 0,
        "non-vacuity: no commit certified the durable floor, so this run did not reach the \
         prep-to-gate race the fix covers"
    );
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
    seed(&db, backend, 2);
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
    let _one_at_a_time = one_parent_test_at_a_time();
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
    let _one_at_a_time = one_parent_test_at_a_time();
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

const HOLDER_CHILD_ENV: &str = "FSQLITE_MP_WRITE_HOLDER_CHILD";

/// bd-4qocc child: a stock connection holds `BEGIN IMMEDIATE` plus an
/// uncommitted UPDATE, so WAL WRITE, until the parent creates `release`.
#[test]
fn mp_write_holder_child() {
    let Ok(spec) = std::env::var(HOLDER_CHILD_ENV) else {
        return;
    };
    let (db, dir) = spec.split_once('|').expect("holder spec");
    let (db, dir) = (PathBuf::from(db), PathBuf::from(dir));
    let conn = rusqlite::Connection::open(&db).expect("holder open");
    conn.execute_batch("BEGIN IMMEDIATE; UPDATE h SET v = v + 1 WHERE id = 1;")
        .expect("hold WAL WRITE");
    std::fs::write(dir.join("held"), b"").expect("held");
    assert!(
        wait_for(&dir.join("release"), Duration::from_secs(120)),
        "no release"
    );
    conn.execute_batch("COMMIT").expect("holder commit");
}

/// A stock child holding WAL WRITE; `dir` is a fresh directory per holder.
struct WriteHolder {
    child: Child,
    dir: PathBuf,
}

impl WriteHolder {
    fn hold(db: &Path, dir: PathBuf) -> Self {
        std::fs::create_dir(&dir).expect("holder dir");
        let child = Command::new(std::env::current_exe().expect("test binary path"))
            .args([
                "--exact",
                "mp_write_holder_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(
                HOLDER_CHILD_ENV,
                format!("{}|{}", db.display(), dir.display()),
            )
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn holder");
        assert!(
            wait_for(&dir.join("held"), Duration::from_secs(120)),
            "the holder never took WRITE"
        );
        Self { child, dir }
    }

    fn release(mut self) {
        std::fs::write(self.dir.join("release"), b"").expect("release");
        assert!(
            self.child.wait().expect("holder exit").success(),
            "holder failed"
        );
    }
}

impl Drop for WriteHolder {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// bd-4qocc: a write refused behind a stock writer spends `busy_timeout`
/// once, as stock does. Main spent it twice (603 ms at 300, 2003 ms at 1000):
/// the pager's flush ran one clock and the connection's retry a second.
#[test]
fn a_refused_write_behind_a_stock_writer_spends_busy_timeout_once() {
    if std::env::var_os(CHILD_ENV).is_some() || std::env::var_os(HOLDER_CHILD_ENV).is_some() {
        return;
    }
    let _one_at_a_time = one_parent_test_at_a_time();
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("held.db");
    rusqlite::Connection::open(&db)
        .expect("seed open")
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE h(id INTEGER PRIMARY KEY, v INTEGER NOT NULL);
             INSERT INTO h VALUES (1, 0);
             CREATE TABLE t(id INTEGER PRIMARY KEY, worker INTEGER NOT NULL, seq INTEGER NOT NULL);",
        )
        .expect("seed");
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(db.to_str().expect("utf8 path"))
            .await
            .expect("fsqlite open");
        let is_busy = |error: &fsqlite::FrankenError| {
            matches!(
                error,
                fsqlite::FrankenError::Busy | fsqlite::FrankenError::BusyRecovery
            )
        };
        for timeout_ms in [300_u64, 1000] {
            let timeout = Duration::from_millis(timeout_ms);
            let holder = WriteHolder::hold(&db, dir.path().join(format!("hold-{timeout_ms}")));

            // The oracle, live: stock's own refusal behind the same holder.
            let stock = rusqlite::Connection::open(&db).expect("stock open");
            stock.busy_timeout(timeout).expect("stock busy_timeout");
            let started = Instant::now();
            let stock_refusal = stock.execute(&insert_sql(9, 0), []);
            let stock_elapsed = started.elapsed();
            drop(stock);
            assert!(
                stock_refusal.is_err() && stock_elapsed >= timeout,
                "stock oracle: {stock_refusal:?} after {stock_elapsed:?}"
            );
            // Host stalls inflate both arms alike; the bound follows stock.
            let ceiling = timeout.max(stock_elapsed) + Duration::from_millis(100);

            frank
                .execute(&format!("PRAGMA busy_timeout={timeout_ms}"))
                .await
                .expect("fsqlite busy_timeout");
            let started = Instant::now();
            let autocommit = frank.execute(&insert_sql(1, 0)).await;
            let autocommit_elapsed = started.elapsed();
            frank.execute("BEGIN").await.expect("BEGIN");
            frank.execute(&insert_sql(1, 1)).await.expect("INSERT");
            let started = Instant::now();
            let commit = frank.execute("COMMIT").await;
            let commit_elapsed = started.elapsed();
            if commit.is_err() {
                frank.execute("ROLLBACK").await.expect("ROLLBACK");
            }
            holder.release();
            eprintln!(
                "event=bd_4qocc busy_timeout_ms={timeout_ms} stock_ms={} autocommit_ms={} \
                 commit_ms={} autocommit={autocommit:?} commit={commit:?}",
                stock_elapsed.as_millis(),
                autocommit_elapsed.as_millis(),
                commit_elapsed.as_millis()
            );
            for (label, result, elapsed) in [
                (
                    "autocommit INSERT",
                    autocommit.map(drop),
                    autocommit_elapsed,
                ),
                ("COMMIT", commit.map(drop), commit_elapsed),
            ] {
                let error = result.expect_err(label);
                assert!(is_busy(&error), "{label}: {error:?}");
                assert!(
                    elapsed >= timeout,
                    "{label} at busy_timeout {timeout_ms}: refused after {elapsed:?}"
                );
                assert!(
                    elapsed <= ceiling,
                    "{label} at busy_timeout {timeout_ms}: refused after {elapsed:?}, \
                     stock after {stock_elapsed:?}"
                );
            }
        }

        // Negative: the holder commits 150 ms in, so the INSERT commits.
        frank
            .execute("PRAGMA busy_timeout=1000")
            .await
            .expect("fsqlite busy_timeout");
        let holder = WriteHolder::hold(&db, dir.path().join("hold-release"));
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(150));
            holder.release();
        });
        let started = Instant::now();
        frank
            .execute(&insert_sql(2, 0))
            .await
            .expect("the INSERT commits once the holder commits");
        assert!(started.elapsed() < Duration::from_millis(1000));
        releaser.join().expect("releaser");

        // Negative: busy_timeout 0 still refuses at once.
        frank
            .execute("PRAGMA busy_timeout=0")
            .await
            .expect("fsqlite busy_timeout");
        let holder = WriteHolder::hold(&db, dir.path().join("hold-zero"));
        let started = Instant::now();
        let error = frank
            .execute(&insert_sql(3, 0))
            .await
            .expect_err("busy_timeout 0 refuses");
        let elapsed = started.elapsed();
        holder.release();
        assert!(is_busy(&error), "busy_timeout 0: {error:?}");
        assert!(
            elapsed < Duration::from_millis(100),
            "busy_timeout 0 refused after {elapsed:?}"
        );
        frank.close().await.expect("close");
    });
}
