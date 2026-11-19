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
    /// `seq` of every row a successful COMMIT made durable (explicit shapes).
    committed_seqs: Vec<usize>,
    /// `seq` of every inserted row its transaction's end lost, with `ended`
    /// (a failed COMMIT ended the transaction) or `closed` (the connection
    /// closed inside it).
    lost_seqs: Vec<(usize, String)>,
    errors: Vec<String>,
    /// bd-q4o7z: refused COMMITs this fsqlite writer replayed, as
    /// `(committed, refused after all)`.
    replays: (u64, u64),
    /// bd-q4o7z: `(seq, ids)` of every committed transaction of the rowid
    /// shapes, with every id the application received inside it.
    committed_ids: Vec<(usize, Vec<i64>)>,
}

#[derive(Debug)]
struct ShapeResult {
    committed: usize,
    /// `(worker, seq)` of every reported commit (explicit shapes).
    committed_rows: Vec<(i64, i64)>,
    /// `(worker, seq, cause)` of every inserted row its transaction's end lost.
    lost_rows: Vec<(i64, i64, String)>,
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
    /// bd-q4o7z: refused COMMITs the fsqlite writers replayed and committed.
    replayed: u64,
    /// bd-q4o7z: refused COMMITs whose replay diverged or ran out of time.
    replay_refused: u64,
    /// bd-q4o7z: `(worker, seq, ids)` of every committed transaction of the
    /// rowid shapes, with every id the application received inside it.
    committed_ids: Vec<(i64, i64, Vec<i64>)>,
    /// `SELECT *` of `sqlite_master` and of every table, read by stock, in
    /// rowid order, keyed by table name in name order.
    tables: Vec<(String, Vec<Vec<rusqlite::types::Value>>)>,
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
    let lost_seqs = events(stdout, "event=mp_busy_lost_seq ")
        .map(|line| {
            let (seq, how) = line.trim().split_once(' ').expect("lost seq and cause");
            (seq.parse().expect("lost seq"), how.to_owned())
        })
        .collect();
    let errors = events(stdout, "event=mp_busy_error ")
        .map(str::to_owned)
        .collect();
    let replays = events(stdout, "event=mp_busy_replays ")
        .next()
        .map_or((0, 0), |line| {
            let field = |name: &str| {
                line.split_whitespace()
                    .find_map(|field| field.strip_prefix(name))
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(0)
            };
            (field("committed="), field("refused="))
        });
    let committed_ids = events(stdout, "event=mp_busy_committed_id ")
        .map(|line| {
            let mut fields = line.split_whitespace();
            let seq = fields
                .next()
                .expect("committed id seq")
                .parse()
                .expect("seq");
            let ids = fields.map(|id| id.parse().expect("id")).collect();
            (seq, ids)
        })
        .collect();
    Some(ChildReport {
        committed,
        committed_seqs,
        lost_seqs,
        errors,
        replays,
        committed_ids,
    })
}

/// `SELECT *` of `sqlite_master` and of every table, in rowid order.
fn table_snapshot(conn: &rusqlite::Connection) -> Vec<(String, Vec<Vec<rusqlite::types::Value>>)> {
    let mut names: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .expect("prepare table names")
        .query_map([], |row| row.get(0))
        .expect("table names")
        .collect::<rusqlite::Result<_>>()
        .expect("table names");
    names.insert(0, "sqlite_master".to_owned());
    names
        .into_iter()
        .map(|name| {
            let mut stmt = conn
                .prepare(&format!("SELECT * FROM \"{name}\" ORDER BY rowid"))
                .expect("prepare table rows");
            let columns = stmt.column_count();
            let rows = stmt
                .query_map([], |row| {
                    (0..columns)
                        .map(|column| row.get::<_, rusqlite::types::Value>(column))
                        .collect()
                })
                .expect("table rows")
                .collect::<rusqlite::Result<_>>()
                .expect("table rows");
            (name, rows)
        })
        .collect()
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
    let mut lost_rows = Vec::new();
    let mut errors = Vec::new();
    let mut floor_certified = 0;
    let (mut replayed, mut replay_refused) = (0, 0);
    let mut committed_ids = Vec::new();
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
        replayed += report.replays.0;
        replay_refused += report.replays.1;
        let worker = i64::try_from(worker).expect("worker");
        committed_rows.extend(
            report
                .committed_seqs
                .iter()
                .map(|&seq| (worker, i64::try_from(seq).expect("seq"))),
        );
        lost_rows.extend(
            report
                .lost_seqs
                .into_iter()
                .map(|(seq, how)| (worker, i64::try_from(seq).expect("seq"), how)),
        );
        committed_ids.extend(
            report
                .committed_ids
                .into_iter()
                .map(|(seq, ids)| (worker, i64::try_from(seq).expect("seq"), ids)),
        );
        errors.extend(report.errors);
    }
    committed_rows.sort_unstable();
    lost_rows.sort_unstable();
    committed_ids.sort_unstable();
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
    let tables = table_snapshot(&stock);
    let result = ShapeResult {
        committed,
        committed_rows,
        lost_rows,
        errors,
        elapsed,
        final_rows,
        durable_rows,
        integrity,
        floor_certified,
        replayed,
        replay_refused,
        committed_ids,
        tables,
    };
    eprintln!(
        "event=mp_busy_shape backend={backend} workers={workers} rows={rows} busy_timeout_ms={busy_timeout_ms} \
         committed={} errors={} elapsed_ms={} final_rows={} integrity={} floor_certified={} \
         replayed={} replay_refused={} sample_errors={:?}",
        result.committed,
        result.errors.len(),
        result.elapsed.as_millis(),
        result.final_rows,
        result.integrity,
        result.floor_certified,
        result.replayed,
        result.replay_refused,
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

/// bd-igdof: the rows of a CLI-style writer's open transaction. A row whose
/// INSERT succeeded is pending until its transaction ends. A COMMIT that
/// fails with a retryable BUSY leaves the transaction open, as stock's COMMIT
/// BUSY does ("the transaction remains active"); the next row's BEGIN then
/// fails "within a transaction", its INSERT joins the open transaction, and
/// the next COMMIT that succeeds makes every pending row durable at once. A
/// transaction that ends any other way (a refusal that rolled it back, or
/// the close of a connection still inside it) loses its pending rows.
#[derive(Default)]
struct OpenTransactionRows {
    pending: Vec<usize>,
}

impl OpenTransactionRows {
    /// Settle one row's statements: whether its INSERT and its COMMIT
    /// succeeded, and whether the connection is still inside a transaction
    /// afterwards. Returns the rows that COMMIT made durable and the rows the
    /// end of the transaction lost.
    fn settle(
        &mut self,
        seq: usize,
        inserted: bool,
        committed: bool,
        open_after: bool,
    ) -> (Vec<usize>, Vec<usize>) {
        if inserted {
            self.pending.push(seq);
        }
        if committed {
            (std::mem::take(&mut self.pending), Vec::new())
        } else if open_after {
            (Vec::new(), Vec::new())
        } else {
            (Vec::new(), std::mem::take(&mut self.pending))
        }
    }

    /// The rows a close loses: closing inside a transaction rolls it back.
    fn close(self) -> Vec<usize> {
        self.pending
    }
}

/// bd-q4o7z: how many times a rowid-shape writer runs one row's transaction
/// before it gives up on that row (an error the test reports).
const ROWID_ATTEMPTS: usize = 1000;

/// bd-q4o7z: one rowid-shape transaction in stock: `BEGIN`, the INSERT, the
/// id it got (`last_insert_rowid()` through the API and through SQL, or the
/// INSERT's `RETURNING id`), `COMMIT`. Returns every id it received.
fn stock_rowid_transaction(
    conn: &rusqlite::Connection,
    returning: bool,
    worker: usize,
    seq: usize,
) -> Result<Vec<i64>, String> {
    conn.execute_batch("BEGIN")
        .map_err(|error| format!("begin: {error}"))?;
    let ids = if returning {
        let id = conn
            .query_row(
                &format!("{} RETURNING id", insert_sql(worker, seq)),
                [],
                |row| row.get(0),
            )
            .map_err(|error| format!("insert: {error}"))?;
        vec![id]
    } else {
        conn.execute(&insert_sql(worker, seq), [])
            .map_err(|error| format!("insert: {error}"))?;
        let api = conn.last_insert_rowid();
        let sql = conn
            .query_row("SELECT last_insert_rowid()", [], |row| row.get(0))
            .map_err(|error| format!("select: {error}"))?;
        vec![api, sql]
    };
    conn.execute_batch("COMMIT")
        .map_err(|error| format!("commit autocommit_after={}: {error}", conn.is_autocommit()))?;
    Ok(ids)
}

/// The one integer a one-row, one-column result holds.
fn only_integer(rows: &[fsqlite::Row]) -> i64 {
    match rows {
        [row] => match row.values() {
            [fsqlite::SqliteValue::Integer(id)] => *id,
            other => panic!("expected one integer column, got {other:?}"),
        },
        other => panic!("expected one row, got {}", other.len()),
    }
}

/// bd-q4o7z: the same transaction in fsqlite, through `execute` and `query`
/// on one connection. An error comes back with the statement it failed.
async fn fsqlite_rowid_transaction(
    conn: &Connection,
    returning: bool,
    worker: usize,
    seq: usize,
) -> Result<Vec<i64>, (&'static str, fsqlite::FrankenError)> {
    conn.execute("BEGIN")
        .await
        .map_err(|error| ("begin", error))?;
    let ids = if returning {
        let rows = conn
            .query(&format!("{} RETURNING id", insert_sql(worker, seq)))
            .await
            .map_err(|error| ("insert", error))?;
        vec![only_integer(&rows)]
    } else {
        conn.execute(&insert_sql(worker, seq))
            .await
            .map_err(|error| ("insert", error))?;
        let api = conn.last_insert_rowid();
        let rows = conn
            .query("SELECT last_insert_rowid()")
            .await
            .map_err(|error| ("select", error))?;
        vec![api, only_integer(&rows)]
    };
    conn.execute("COMMIT")
        .await
        .map_err(|error| ("commit", error))?;
    Ok(ids)
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
    // bd-q4o7z: the mixed shape alternates stock and fsqlite writers on one
    // table, so fsqlite's replays race real stock commits.
    let backend = match backend {
        "mixed-explicit" if worker.is_multiple_of(2) => "stock-explicit",
        "mixed-explicit" => "fsqlite-explicit",
        other => other,
    };
    let rows: usize = rows.parse().expect("rows");
    let busy_timeout_ms: u64 = busy_timeout_ms.parse().expect("busy_timeout_ms");
    let mut committed = 0;
    let mut committed_seqs = Vec::new();
    let mut errors = Vec::new();
    let mut lost_seqs = Vec::new();
    let mut committed_ids: Vec<(usize, Vec<i64>)> = Vec::new();
    match backend {
        // bd-reality-check-1022-llk6g.5.1: `BEGIN; INSERT; COMMIT` per row,
        // the way a CLI script runs it: every statement runs, an error is
        // recorded, and nothing rolls back. Which rows a COMMIT made durable
        // is `OpenTransactionRows`'s accounting (bd-igdof).
        "stock-explicit" | "stock-explicit-own" => {
            let conn = rusqlite::Connection::open(&db).expect("stock open");
            conn.busy_timeout(Duration::from_millis(busy_timeout_ms))
                .expect("stock busy_timeout");
            std::fs::write(ready_path(&dir, worker), b"").expect("ready");
            assert!(wait_for(&dir.join("go"), Duration::from_secs(120)), "no go");
            let mut open = OpenTransactionRows::default();
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
                let (landed, lost) =
                    open.settle(seq, insert.is_ok(), commit.is_ok(), !conn.is_autocommit());
                committed += landed.len();
                committed_seqs.extend(landed);
                lost_seqs.extend(lost.into_iter().map(|seq| (seq, "ended")));
            }
            lost_seqs.extend(open.close().into_iter().map(|seq| (seq, "closed")));
        }
        // `-query` runs every statement through `query`, as the shell does.
        "fsqlite-explicit" | "fsqlite-explicit-own" | "fsqlite-explicit-query" => {
            let via_query = backend == "fsqlite-explicit-query";
            let (committed, committed_seqs, errors, lost_seqs) = (
                &mut committed,
                &mut committed_seqs,
                &mut errors,
                &mut lost_seqs,
            );
            asupersync::test_utils::run_test(|| async move {
                let conn = Connection::open(db.to_str().expect("utf8 path"))
                    .await
                    .expect("fsqlite open");
                conn.execute(&format!("PRAGMA busy_timeout={busy_timeout_ms}"))
                    .await
                    .expect("fsqlite busy_timeout");
                std::fs::write(ready_path(&dir, worker), b"").expect("ready");
                assert!(wait_for(&dir.join("go"), Duration::from_secs(120)), "no go");
                let mut open = OpenTransactionRows::default();
                let run = |sql: String| {
                    let conn = &conn;
                    async move {
                        if via_query {
                            conn.query(&sql).await.map(drop)
                        } else {
                            conn.execute(&sql).await.map(drop)
                        }
                    }
                };
                for seq in 0..rows {
                    let begin = run("BEGIN".to_owned()).await;
                    let insert = run(explicit_insert_sql(backend, worker, seq)).await;
                    let commit = run("COMMIT".to_owned()).await;
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
                    let (landed, lost) =
                        open.settle(seq, insert.is_ok(), commit.is_ok(), conn.in_transaction());
                    *committed += landed.len();
                    committed_seqs.extend(landed);
                    lost_seqs.extend(lost.into_iter().map(|seq| (seq, "ended")));
                }
                lost_seqs.extend(open.close().into_iter().map(|seq| (seq, "closed")));
                let _ = conn.close().await;
            });
        }
        // bd-q4o7z: each transaction reads the id of the row it inserted, as
        // an ORM does, and a failed transaction is rolled back (if it is still
        // open) and run again, as an application does, until it commits.
        "stock-explicit-rowid" | "stock-explicit-returning" => {
            let returning = backend == "stock-explicit-returning";
            let conn = rusqlite::Connection::open(&db).expect("stock open");
            conn.busy_timeout(Duration::from_millis(busy_timeout_ms))
                .expect("stock busy_timeout");
            std::fs::write(ready_path(&dir, worker), b"").expect("ready");
            assert!(wait_for(&dir.join("go"), Duration::from_secs(120)), "no go");
            for seq in 0..rows {
                for attempt in 1..=ROWID_ATTEMPTS {
                    match stock_rowid_transaction(&conn, returning, worker, seq) {
                        Ok(ids) => {
                            committed += 1;
                            committed_seqs.push(seq);
                            committed_ids.push((seq, ids));
                            break;
                        }
                        Err(error) => {
                            errors.push(error);
                            if !conn.is_autocommit() {
                                let _ = conn.execute_batch("ROLLBACK");
                            }
                            if attempt == ROWID_ATTEMPTS {
                                errors.push(format!("seq {seq}: gave up after {attempt} attempts"));
                            }
                        }
                    }
                }
            }
        }
        "fsqlite-explicit-rowid" | "fsqlite-explicit-returning" => {
            let returning = backend == "fsqlite-explicit-returning";
            let (committed, committed_seqs, committed_ids, errors) = (
                &mut committed,
                &mut committed_seqs,
                &mut committed_ids,
                &mut errors,
            );
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
                    for attempt in 1..=ROWID_ATTEMPTS {
                        match fsqlite_rowid_transaction(&conn, returning, worker, seq).await {
                            Ok(ids) => {
                                *committed += 1;
                                committed_seqs.push(seq);
                                committed_ids.push((seq, ids));
                                break;
                            }
                            Err((stage, error)) => {
                                errors.push(format!(
                                    "{stage} code={} autocommit_after={}: {error}",
                                    error.extended_error_code(),
                                    !conn.in_transaction()
                                ));
                                if conn.in_transaction() {
                                    let _ = conn.execute("ROLLBACK").await;
                                }
                                if attempt == ROWID_ATTEMPTS {
                                    errors.push(format!(
                                        "seq {seq}: gave up after {attempt} attempts"
                                    ));
                                }
                            }
                        }
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
    for (seq, how) in &lost_seqs {
        println!("event=mp_busy_lost_seq {seq} {how}");
    }
    for (seq, ids) in &committed_ids {
        let ids: Vec<String> = ids.iter().map(ToString::to_string).collect();
        println!("event=mp_busy_committed_id {seq} {}", ids.join(" "));
    }
    let replays = fsqlite_core::connection::txn_replay_counts();
    println!(
        "event=mp_busy_replays committed={} refused={}",
        replays.committed, replays.refused
    );
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

/// bd-igdof: the error text of a COMMIT that failed with the retryable BUSY
/// (5) and left its transaction open, as stock's COMMIT BUSY does.
const BUSY_COMMIT_KEPT_OPEN: &str = "commit code=5 autocommit_after=false: ";

/// The next row's BEGIN after such a COMMIT: its transaction is still open.
const BEGIN_WITHIN_TRANSACTION: &str = "begin: cannot start a transaction within a transaction";

/// Whether a writer may end its run still inside a transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpenAtClose {
    /// Every transaction ended by COMMIT or by a refusal.
    Forbidden,
    /// The writer's last COMMITs may have returned BUSY and kept their
    /// transaction open (decision (b1)). The close then rolls that one
    /// transaction back: its rows were reported failed and never land. This
    /// happens when a short busy_timeout runs out on the last rows.
    AfterBusyCommit,
}

/// The explicit shapes' accounting (bd-reality-check-1022-llk6g.5.1, bd-igdof
/// decision (b1)), returning the refused COMMITs:
/// - integrity_check is ok;
/// - the durable rows are exactly the rows a successful COMMIT made durable.
///   That includes the row of a COMMIT that returned BUSY when a later COMMIT
///   of its still-open transaction landed it (stock's contract, shown by
///   `refused_commit_ends_transaction_e2e`'s stock arm). Every committed row
///   is durable and nothing else is;
/// - every inserted row either landed or was lost with a transaction that a
///   BusySnapshot refusal ended. None was lost at close, unless `open_at_close`
///   allows it (see [`OpenAtClose`]);
/// - a refusal costs its own transaction's rows: one, plus at most one per
///   BUSY COMMIT whose row was still pending in it;
/// - the only errors are such a refusal, a BUSY COMMIT that kept its
///   transaction open, and the following BEGIN's "within a transaction".
fn assert_explicit_accounting<'a>(
    label: &str,
    frank: &'a ShapeResult,
    expected: usize,
    open_at_close: OpenAtClose,
) -> Vec<&'a String> {
    assert_eq!(frank.integrity, "ok", "{label}: integrity_check");
    assert_eq!(
        frank.durable_rows, frank.committed_rows,
        "{label}: the durable rows are exactly the rows a successful COMMIT made durable"
    );
    let refused: Vec<&String> = frank
        .errors
        .iter()
        .filter(|error| error.starts_with(REFUSED_COMMIT_ENDED))
        .collect();
    let busy_commits = frank
        .errors
        .iter()
        .filter(|error| error.starts_with(BUSY_COMMIT_KEPT_OPEN))
        .count();
    let begins_within = frank
        .errors
        .iter()
        .filter(|error| error.starts_with(BEGIN_WITHIN_TRANSACTION))
        .count();
    let other: Vec<&String> = frank
        .errors
        .iter()
        .filter(|error| {
            !error.starts_with(REFUSED_COMMIT_ENDED)
                && !error.starts_with(BUSY_COMMIT_KEPT_OPEN)
                && !error.starts_with(BEGIN_WITHIN_TRANSACTION)
        })
        .collect();
    assert!(
        other.is_empty(),
        "{label}: the only errors may be a BusySnapshot COMMIT refusal that ended its \
         transaction, a BUSY COMMIT that kept it open, and the next BEGIN inside it; got {} \
         others, e.g. {:?}",
        other.len(),
        other.iter().take(5).collect::<Vec<_>>()
    );
    assert!(
        begins_within <= busy_commits,
        "{label}: {begins_within} BEGINs inside an open transaction, but only {busy_commits} \
         BUSY COMMITs left one open"
    );
    let closed: Vec<_> = frank
        .lost_rows
        .iter()
        .filter(|(_, _, how)| how != "ended")
        .collect();
    match open_at_close {
        OpenAtClose::Forbidden => assert!(
            closed.is_empty(),
            "{label}: rows lost when a connection closed inside a transaction: {closed:?}"
        ),
        OpenAtClose::AfterBusyCommit => {
            // A writer's rows pending at close are one transaction's: the
            // rows after its last successful COMMIT, consecutive, each
            // INSERTed and then COMMITted with BUSY (any other error fails
            // the error check above), and none of them is durable.
            let mut by_worker: std::collections::BTreeMap<i64, Vec<i64>> =
                std::collections::BTreeMap::new();
            for (worker, seq, _) in &closed {
                by_worker.entry(*worker).or_default().push(*seq);
            }
            for (worker, mut seqs) in by_worker {
                seqs.sort_unstable();
                let last_committed = frank
                    .committed_rows
                    .iter()
                    .filter(|(committed_worker, _)| *committed_worker == worker)
                    .map(|(_, seq)| *seq)
                    .max()
                    .unwrap_or(-1);
                assert!(
                    seqs[0] > last_committed && seqs.windows(2).all(|pair| pair[1] == pair[0] + 1),
                    "{label}: worker {worker} lost rows {seqs:?} at close, which are not the \
                     transaction left open after its last COMMIT (seq {last_committed})"
                );
            }
        }
    }
    assert_eq!(
        frank.committed + frank.lost_rows.len(),
        expected,
        "{label}: every row either landed or was lost with a refused transaction"
    );
    assert!(
        frank.lost_rows.len() >= refused.len()
            && frank.lost_rows.len() <= refused.len() + busy_commits,
        "{label}: {} rows lost to {} refusals with {busy_commits} BUSY COMMITs: a refusal costs \
         its own transaction's rows",
        frank.lost_rows.len(),
        refused.len()
    );
    eprintln!(
        "event=mp_busy_accounting label={label} committed={} lost={} refused={} \
         busy_commits={busy_commits} begins_within={begins_within} lost_at_close={}",
        frank.committed,
        frank.lost_rows.len(),
        refused.len(),
        closed.len()
    );
    refused
}

/// bd-q4o7z: processes running explicit `BEGIN; INSERT; COMMIT` transactions
/// on ONE table, CLI-style (no ROLLBACK after an error), commit every row, as
/// stock does. Stock's write-first transaction waits for the write lock at its
/// INSERT. fsqlite's plain BEGIN is promoted to BEGIN CONCURRENT, so the second
/// committer of a leaf page is refused first-committer-wins; the transaction
/// exposed nothing a replay could change (its INSERT returned 1 row), so the
/// refused COMMIT replays it on the newest snapshot within busy_timeout.
///
/// History: on main 714e81184 a refused transaction stayed open and 8 CLI
/// writers x 300 committed 300-312 of 2400 rows; .5.1 ended it, so each
/// refusal cost exactly its own row (a median 1,910 of 2,400 committed at
/// b1526ddf1). Three fsqlite arms: `execute`, `query` (the shell's path), and
/// stock and fsqlite writers alternating on the same file. The accounting is
/// [`assert_explicit_accounting`]'s; on top of it no COMMIT is refused and no
/// row is lost. Non-vacuity: the fsqlite writers replayed at least one refused
/// COMMIT.
#[test]
fn cross_process_explicit_writers_on_one_table_commit_every_row_as_stock() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    let _one_at_a_time = one_parent_test_at_a_time();
    let (workers, rows) = (8, 300);
    stock_oracle("stock-explicit", workers, rows);

    for backend in [
        "fsqlite-explicit",
        "fsqlite-explicit-query",
        "mixed-explicit",
    ] {
        let frank = run_shape(backend, workers, rows, 5000);
        let refused =
            assert_explicit_accounting(backend, &frank, workers * rows, OpenAtClose::Forbidden);
        assert!(
            refused.is_empty(),
            "{backend}: no COMMIT may be refused, as in stock; got {}, e.g. {:?}",
            refused.len(),
            refused.iter().take(5).collect::<Vec<_>>()
        );
        assert!(
            frank.lost_rows.is_empty(),
            "{backend}: no row may be lost, as in stock; lost {:?}",
            frank.lost_rows.iter().take(5).collect::<Vec<_>>()
        );
        assert_eq!(
            frank.committed,
            workers * rows,
            "{backend}: every transaction commits, as in stock"
        );
        assert!(
            frank.replayed > 0,
            "non-vacuity: {backend}'s fsqlite writers replayed no refused COMMIT, so this run \
             did not exercise the replay"
        );
        // bd-q4o7z probe 4: the commit order is the rows' id order, so stock
        // committing the same rows in that order must build this database:
        // ids 1..N with no gap or duplicate, and every column as committed.
        let commit_order: Vec<(i64, i64)> = snapshot_table(&frank, "t")
            .iter()
            .map(|row| (as_integer(&row[1]), as_integer(&row[2])))
            .collect();
        assert_matches_stock_for_committed_set(backend, backend, workers, &frank, &commit_order);
    }
}

/// The rows of `table` in a shape's snapshot.
fn snapshot_table<'a>(result: &'a ShapeResult, table: &str) -> &'a [Vec<rusqlite::types::Value>] {
    result
        .tables
        .iter()
        .find(|(name, _)| name == table)
        .map_or_else(
            || panic!("no table {table} in the snapshot"),
            |(_, rows)| rows.as_slice(),
        )
}

fn as_integer(value: &rusqlite::types::Value) -> i64 {
    match value {
        rusqlite::types::Value::Integer(value) => *value,
        other => panic!("expected an integer, got {other:?}"),
    }
}

/// bd-q4o7z probe 4: stock commits the same transactions in the same order
/// into a freshly seeded database (one `INSERT INTO t(worker, seq)` per
/// committed row of `commit_order`, ids assigned by stock). That database must
/// read back exactly like the shape's: `SELECT *` of `sqlite_master` and of
/// every table, row by row. The shape's own integrity_check, run by stock, is
/// asserted by its accounting.
fn assert_matches_stock_for_committed_set(
    label: &str,
    backend: &str,
    workers: usize,
    frank: &ShapeResult,
    commit_order: &[(i64, i64)],
) {
    let dir = tempfile::tempdir().expect("reference tempdir");
    let db = dir.path().join("reference.db");
    seed(&db, backend, workers);
    let mut stock = rusqlite::Connection::open(&db).expect("reference open");
    let txn = stock.transaction().expect("reference transaction");
    for (worker, seq) in commit_order {
        txn.execute(
            "INSERT INTO t(worker, seq) VALUES (?1, ?2)",
            rusqlite::params![worker, seq],
        )
        .expect("reference insert");
    }
    txn.commit().expect("reference commit");
    let reference = table_snapshot(&stock);
    let names = |tables: &[(String, Vec<Vec<rusqlite::types::Value>>)]| {
        tables
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        names(&frank.tables),
        names(&reference),
        "{label}: the same tables as stock's database for the same committed set"
    );
    for ((name, rows), (_, expected)) in frank.tables.iter().zip(&reference) {
        if let Some(at) =
            (0..rows.len().max(expected.len())).find(|&at| rows.get(at) != expected.get(at))
        {
            panic!(
                "{label}: {name} differs from stock's database for the same committed set at row \
                 {at}: {:?}, stock {:?} ({} rows, stock {})",
                rows.get(at),
                expected.get(at),
                rows.len(),
                expected.len()
            );
        }
    }
}

/// bd-q4o7z probe 2: the accounting of a rowid shape, whose writers run each
/// row's transaction until it commits. Returns the refused COMMITs.
/// - integrity_check is ok;
/// - every row committed exactly once, both as the writers saw it and in the
///   table: nothing lost, nothing duplicated;
/// - every error is a COMMIT refused with BusySnapshot (517) that ended its
///   transaction;
/// - every id an application received inside a transaction that committed
///   (`last_insert_rowid()` through the API and through SQL, or the INSERT's
///   `RETURNING id`) is the id of that transaction's row in the table;
/// - stock committing the same rows in the order of those ids builds the same
///   database (probe 4).
fn assert_rowid_accounting(
    label: &str,
    backend: &str,
    result: &ShapeResult,
    workers: usize,
    rows: usize,
) -> usize {
    assert_eq!(result.integrity, "ok", "{label}: integrity_check");
    let grid: Vec<(i64, i64)> = (0..workers)
        .flat_map(|worker| (0..rows).map(move |seq| (worker, seq)))
        .map(|(worker, seq)| {
            (
                i64::try_from(worker).expect("worker"),
                i64::try_from(seq).expect("seq"),
            )
        })
        .collect();
    assert_eq!(
        result.durable_rows, grid,
        "{label}: every row is durable exactly once"
    );
    assert_eq!(
        result.committed_rows, grid,
        "{label}: every row's transaction committed exactly once"
    );
    let refused = result
        .errors
        .iter()
        .filter(|error| error.starts_with(REFUSED_COMMIT_ENDED))
        .count();
    let other: Vec<&String> = result
        .errors
        .iter()
        .filter(|error| !error.starts_with(REFUSED_COMMIT_ENDED))
        .collect();
    assert!(
        other.is_empty(),
        "{label}: every error must be a COMMIT refused with BusySnapshot (517) that ended its \
         transaction; got {} others, e.g. {:?}",
        other.len(),
        other.iter().take(5).collect::<Vec<_>>()
    );
    let table_ids: std::collections::BTreeMap<(i64, i64), i64> = snapshot_table(result, "t")
        .iter()
        .map(|row| {
            (
                (as_integer(&row[1]), as_integer(&row[2])),
                as_integer(&row[0]),
            )
        })
        .collect();
    assert_eq!(
        result.committed_ids.len(),
        grid.len(),
        "{label}: every committed transaction reported the ids it received"
    );
    for (worker, seq, ids) in &result.committed_ids {
        let id = table_ids[&(*worker, *seq)];
        assert!(
            !ids.is_empty() && ids.iter().all(|seen| *seen == id),
            "{label}: writer {worker} row {seq} received ids {ids:?} inside the transaction that \
             committed it, but the row's id in the table is {id}"
        );
    }
    let mut by_id: Vec<(i64, (i64, i64))> = result
        .committed_ids
        .iter()
        .map(|(worker, seq, ids)| (ids[0], (*worker, *seq)))
        .collect();
    by_id.sort_unstable();
    let commit_order: Vec<(i64, i64)> = by_id.into_iter().map(|(_, row)| row).collect();
    assert_matches_stock_for_committed_set(label, backend, workers, result, &commit_order);
    eprintln!(
        "event=mp_busy_rowid_accounting label={label} committed={} refused={refused} \
         replayed={} replay_refused={}",
        result.committed, result.replayed, result.replay_refused
    );
    refused
}

/// bd-q4o7z probes 2 and 4: processes on ONE table whose transactions read
/// the id of the row they inserted, as an ORM does: `BEGIN; INSERT;` then
/// `last_insert_rowid()` through the API and through SQL (one arm), or
/// `INSERT ... RETURNING id` (the other), then `COMMIT`. A failed transaction
/// is rolled back if still open and run again until it commits.
///
/// A refused COMMIT's replay must not hand a transaction a different id than
/// the one its application already received: when a peer took that rowid,
/// the replay diverges and the COMMIT is refused with BusySnapshot. So every
/// error is such a refusal, every row commits exactly once, and every id an
/// application received in a transaction that committed is that row's id in
/// the table. Stock committing the same rows in the order of those ids builds
/// the same database. Stock runs the same shape first as the oracle and
/// passes the same accounting with no error.
///
/// Non-vacuity: some COMMIT was refused after its replay diverged, so the
/// replay met transactions whose rowid a peer had taken.
#[test]
fn cross_process_writers_reading_their_rowid_commit_each_row_once_under_the_id_they_saw() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    let _one_at_a_time = one_parent_test_at_a_time();
    let (workers, rows) = (8, 300);
    for (stock_backend, backend) in [
        ("stock-explicit-rowid", "fsqlite-explicit-rowid"),
        ("stock-explicit-returning", "fsqlite-explicit-returning"),
    ] {
        let stock = stock_oracle(stock_backend, workers, rows);
        let stock_refused =
            assert_rowid_accounting(stock_backend, stock_backend, &stock, workers, rows);
        assert_eq!(
            stock_refused, 0,
            "{stock_backend}: the oracle refused a COMMIT"
        );

        let frank = run_shape(backend, workers, rows, 5000);
        let refused = assert_rowid_accounting(backend, backend, &frank, workers, rows);
        assert!(
            refused > 0 && frank.replay_refused > 0,
            "non-vacuity: {backend} had {refused} refused COMMITs and {} diverged replays, so no \
             replay met a transaction whose rowid a peer had taken",
            frank.replay_refused
        );
    }
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
///
/// bd-igdof: a COMMIT that waited out busy_timeout returns BUSY and keeps its
/// transaction open, as stock's does, so the next COMMIT of that transaction
/// lands its row; this test counted that row as failed and then found it
/// durable. The accounting now follows the transaction (decision (b1)), and
/// committed-implies-durable stays exact in both directions.
#[test]
fn cross_process_explicit_writers_on_own_tables_certify_the_durable_floor() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    let _one_at_a_time = one_parent_test_at_a_time();
    let (workers, rows) = (8, 300);
    stock_oracle("stock-explicit-own", workers, rows);

    let frank = run_shape("fsqlite-explicit-own", workers, rows, 5000);
    assert_explicit_accounting(
        "fsqlite-explicit-own",
        &frank,
        workers * rows,
        OpenAtClose::Forbidden,
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
    assert!(
        frank.floor_certified > 0,
        "non-vacuity: no commit certified the durable floor, so this run did not reach the \
         prep-to-gate race the fix covers"
    );
}

/// bd-igdof's own path, made routine. At the default busy_timeout the shape
/// above rarely gets a COMMIT that returns BUSY (none in 80 runs on hz3 and
/// vmi1153651 at 83da909e2), so it seldom checks the case the bead found:
/// a BUSY COMMIT keeps its transaction open (decision (b1)), and the row lands
/// only when a later COMMIT of that transaction succeeds. At 50 ms, commits
/// that wait for the write lock run out of budget. The same accounting must
/// then hold:
/// - committed is exactly durable, in both directions;
/// - every BUSY COMMIT kept its transaction open;
/// - a refusal costs its own row plus at most one pending row per BUSY COMMIT.
/// - a writer may end its run inside the one transaction its last COMMITs
///   left open with BUSY. The close rolls it back, so those rows were reported
///   failed and are not durable; at 50 ms that happens on loaded hosts.
///
/// Non-vacuity: at least one COMMIT returned BUSY, or the run proved nothing.
#[test]
fn cross_process_explicit_writers_on_own_tables_account_busy_commits_at_a_short_busy_timeout() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    let _one_at_a_time = one_parent_test_at_a_time();
    let (workers, rows) = (8, 300);
    let frank = run_shape("fsqlite-explicit-own", workers, rows, 50);
    assert_explicit_accounting(
        "fsqlite-explicit-own-busy-50ms",
        &frank,
        workers * rows,
        OpenAtClose::AfterBusyCommit,
    );
    let busy_commits = frank
        .errors
        .iter()
        .filter(|error| error.starts_with(BUSY_COMMIT_KEPT_OPEN))
        .count();
    assert!(
        busy_commits > 0,
        "non-vacuity: no COMMIT returned BUSY at busy_timeout 50 ms, so this run never checked \
         the accounting of a BUSY COMMIT whose transaction stayed open"
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
    // bd-mah07: each role's pipes are drained from spawn (wait_with_output on
    // its own thread). Reaped in turn, role b could block in a full pipe,
    // holding its lock, while the parent still waited on role a.
    let children: Vec<std::thread::JoinHandle<std::io::Result<std::process::Output>>> = ["a", "b"]
        .iter()
        .map(|role| {
            let child = Command::new(&exe)
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
                .expect("spawn role");
            std::thread::spawn(move || child.wait_with_output())
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
        .map(|(reaper, role)| {
            let output = reaper.join().expect("role reaper").expect("role output");
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
