#![recursion_limit = "512"]

//! bd-reality-check-1022-llk6g.5.7: an fsqlite COMMIT that waits for the WAL
//! write lock must not keep the read mark a stock checkpointer is waiting for.
//!
//! A stock `PRAGMA wal_checkpoint(TRUNCATE)` (or `RESTART`) holds WAL WRITE
//! and waits, under its busy handler, for every reader mark to clear. An open
//! fsqlite transaction holds a mark from BEGIN to the end of COMMIT, and its
//! COMMIT waits for WRITE. On main d39f670d1 each side then spent its whole
//! busy_timeout: the COMMIT returned after the checkpointer gave up, and the
//! checkpoint reported busy. Stock's own writer holds no read lock while it
//! waits for WRITE, so stock never stalls against its checkpointer.
//!
//! Expected, as stock: the checkpoint finishes (not busy) and the COMMIT
//! decides promptly. It commits when the stock writer changed other pages.
//! Its snapshot is refused when stock changed a page it writes, whether that
//! commit landed before the COMMIT began waiting (seen in the WAL) or while
//! it waited (seen only in the database file after the reset). It is also
//! refused when an fsqlite commit may have happened while it waited (its
//! `-wal-cert` sidecar changed), since only the sidecar can prove that none
//! did: a byte comparison cannot see an fsqlite writer's byte-identical
//! structural claim (bd-73pfl).
//!
//! A refused snapshot ends as stock's application retry ends it (bd-q4o7z,
//! a3d329d47): a transaction that returned nothing is replayed inside its
//! COMMIT on a fresh snapshot and commits; one that read is refused at once
//! with a retryable `BusySnapshot`, and its retry commits. Either way the
//! final rows are the ones stock leaves when its own stale writer is refused
//! and retried in the same interleaving, which every such case runs live; a
//! lost update fails that comparison.
//!
//! The close rule: as stock's `sqlite3WalClose`, `Connection::close` folds the
//! WAL into the database file only when no other process is attached to it.
//! Its passive checkpoint holds the WAL checkpoint lock, and a stock
//! TRUNCATE or RESTART that meets that lock answers busy at once. The last
//! closer still checkpoints as before, and one that closes beside an attached
//! process leaves the WAL to whoever closes last, as stock does; both shapes
//! run against stock live.
//!
//! The stock side is a real SQLite library in a child process.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use fsqlite::Connection;
use fsqlite_error::FrankenError;

const CHILD_ENV: &str = "FSQLITE_557_STOCK_CHILD";
/// Both sides' busy_timeout. A stall spends all of it; a prompt decision
/// takes a small fraction.
const BUSY_TIMEOUT_MS: u64 = 2000;
/// A COMMIT or checkpoint this slow waited for the other side's timeout.
const PROMPT: Duration = Duration::from_millis(1000);

struct ChildGuard(Option<Child>);

impl ChildGuard {
    fn finish(mut self) {
        let output = self
            .0
            .take()
            .expect("child spawned")
            .wait_with_output()
            .expect("child output");
        assert!(
            output.status.success(),
            "stock child failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
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

/// What the stock child does before its checkpoint.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Script {
    /// Wait for `go`, commit one row into `table`, then checkpoint.
    CommitAtGo,
    /// Take WRITE (BEGIN IMMEDIATE) and say `held`; after `HOLD_MS`, commit
    /// one row into `table`, then checkpoint.
    HoldWrite,
    /// As `HoldWrite`, and while still holding WRITE, append a copy of the
    /// `-wal-cert` sidecar's last record: what an fsqlite commit would have
    /// done to the sidecar.
    HoldWriteTouchSidecar,
    /// As `HoldWrite`, and while still holding WRITE, create the absent
    /// `-wal-cert` sidecar, as an fsqlite commit would.
    HoldWriteCreateSidecar,
    /// As `HoldWrite`, and while still holding WRITE, truncate the
    /// `-wal-cert` sidecar to empty in place: what an fsqlite peer leaves when
    /// it dies after the truncate that starts a new generation's first
    /// append, before writing the continuity anchor (c9c513a65).
    HoldWriteEmptySidecar,
    /// As `HoldWriteEmptySidecar`, but the peer died part-way through writing
    /// the anchor: the sidecar holds the first half of its newest record.
    HoldWriteTearAnchor,
    /// Take WRITE and say `held`; after `HOLD_MS`, roll back (no stock row),
    /// then checkpoint: only the reset is stock's (bd-30ip9).
    HoldWriteThenRollBack,
}

/// How long a holding child keeps WRITE before it commits: the parent's
/// COMMIT is waiting for WRITE by then.
const HOLD_MS: u64 = 300;

struct StockRun {
    db: PathBuf,
    dir: PathBuf,
    script: Script,
    table: &'static str,
    mode: &'static str,
}

impl StockRun {
    fn spawn(&self) -> ChildGuard {
        let script = match self.script {
            Script::CommitAtGo => "commit",
            Script::HoldWrite => "hold",
            Script::HoldWriteTouchSidecar => "hold-touch",
            Script::HoldWriteCreateSidecar => "hold-create",
            Script::HoldWriteEmptySidecar => "hold-empty",
            Script::HoldWriteTearAnchor => "hold-tear",
            Script::HoldWriteThenRollBack => "hold-rollback",
        };
        let child = Command::new(std::env::current_exe().expect("test binary path"))
            .args([
                "--exact",
                "stock_checkpointer_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(
                CHILD_ENV,
                format!(
                    "{}|{}|{script}|{}|{}",
                    self.db.display(),
                    self.dir.display(),
                    self.table,
                    self.mode
                ),
            )
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn stock child");
        ChildGuard(Some(child))
    }

    /// `(busy, log, checkpointed, elapsed)` of the child's checkpoint.
    fn checkpoint_result(&self) -> (i64, i64, i64, Duration) {
        let text = std::fs::read_to_string(self.dir.join("result")).expect("checkpoint result");
        let fields: Vec<i64> = text
            .split_whitespace()
            .map(|field| field.parse().expect("result field"))
            .collect();
        let millis = u64::try_from(fields[3]).expect("elapsed");
        (
            fields[0],
            fields[1],
            fields[2],
            Duration::from_millis(millis),
        )
    }

    /// When (unix microseconds) the child's checkpoint returned.
    fn checkpoint_done_us(&self) -> u128 {
        let text = std::fs::read_to_string(self.dir.join("result")).expect("checkpoint result");
        text.split_whitespace()
            .nth(4)
            .and_then(|field| field.parse().ok())
            .expect("checkpoint completion time")
    }
}

/// The newest record of a `-wal-cert` sidecar: each record ends in a
/// little-endian u32 footer holding its own length.
fn newest_certificate_record(bytes: &[u8]) -> &[u8] {
    let footer = bytes.len() - 4;
    let record_len = usize::try_from(u32::from_le_bytes(
        bytes[footer..].try_into().expect("footer"),
    ))
    .expect("record length");
    &bytes[bytes.len() - record_len..]
}

/// The sidecar an fsqlite peer leaves when it dies part-way through the
/// first append of a new generation (truncate, write the anchor, append its
/// record; c9c513a65).
#[derive(Clone, Copy)]
enum KilledCompaction {
    Empty,
    TornAnchor,
}

fn killed_compaction_image(script: &str) -> Option<KilledCompaction> {
    match script {
        "hold-empty" => Some(KilledCompaction::Empty),
        "hold-tear" => Some(KilledCompaction::TornAnchor),
        _ => None,
    }
}

#[test]
fn stock_checkpointer_child() {
    let Ok(spec) = std::env::var(CHILD_ENV) else {
        return;
    };
    let parts: Vec<&str> = spec.split('|').collect();
    let [db, dir, script, table, mode] = parts[..] else {
        panic!("child spec {spec}");
    };
    let dir = PathBuf::from(dir);
    let stock = rusqlite::Connection::open(db).expect("stock open");
    stock
        .busy_timeout(Duration::from_millis(BUSY_TIMEOUT_MS))
        .expect("busy_timeout");
    let insert = format!("INSERT INTO {table}(v) VALUES ('stock')");
    if script == "commit" {
        assert!(wait_for(&dir.join("go"), Duration::from_secs(60)), "no go");
        stock.execute_batch(&insert).expect("stock commit");
        std::fs::write(dir.join("committed"), b"").expect("committed");
    } else {
        stock
            .execute_batch("BEGIN IMMEDIATE")
            .expect("stock takes WRITE");
        std::fs::write(dir.join("held"), b"").expect("held");
        std::thread::sleep(Duration::from_millis(HOLD_MS));
        if script == "hold-rollback" {
            stock.execute_batch("ROLLBACK").expect("stock rollback");
        } else {
            stock.execute_batch(&insert).expect("stock insert");
        }
        if script == "hold-touch" {
            let mut cert = PathBuf::from(db).into_os_string();
            cert.push("-wal-cert");
            let bytes = std::fs::read(&cert).expect("read -wal-cert");
            let record = newest_certificate_record(&bytes).to_vec();
            use std::io::Write as _;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&cert)
                .expect("open -wal-cert");
            file.write_all(&record).expect("append a record");
        }
        if script == "hold-create" {
            let mut cert = PathBuf::from(db).into_os_string();
            cert.push("-wal-cert");
            assert!(!Path::new(&cert).exists(), "no sidecar before the wait");
            std::fs::write(&cert, b"").expect("create -wal-cert");
        }
        if let Some(image) = killed_compaction_image(script) {
            let mut cert = PathBuf::from(db).into_os_string();
            cert.push("-wal-cert");
            let bytes = std::fs::read(&cert).expect("read -wal-cert");
            let newest = newest_certificate_record(&bytes);
            // The captured tail is a complete record the image no longer has.
            assert!(
                bytes.len() > newest.len(),
                "{script}: the sidecar must hold more than its newest record ({} bytes)",
                bytes.len()
            );
            let rewritten = match image {
                KilledCompaction::Empty => Vec::new(),
                KilledCompaction::TornAnchor => newest[..newest.len() / 2].to_vec(),
            };
            // In place, as the peer's truncate and write: same file identity.
            std::fs::write(&cert, rewritten).expect("rewrite -wal-cert in place");
        }
        if script != "hold-rollback" {
            stock.execute_batch("COMMIT").expect("stock commit");
        }
    }
    let started = Instant::now();
    let (busy, log, checkpointed): (i64, i64, i64) = stock
        .query_row(&format!("PRAGMA wal_checkpoint({mode})"), [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .expect("stock checkpoint");
    let millis = started.elapsed().as_millis();
    let done_us = unix_us();
    std::fs::write(
        dir.join("result"),
        format!("{busy} {log} {checkpointed} {millis} {done_us}"),
    )
    .expect("result");
}

/// A WAL database with rows in `t` and `s`.
fn seed(dir: &Path) -> PathBuf {
    let db = dir.join("ckpt.db");
    rusqlite::Connection::open(&db)
        .expect("seed open")
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL);
             CREATE TABLE s(id INTEGER PRIMARY KEY, v TEXT NOT NULL);
             INSERT INTO t(v) VALUES ('seed');
             INSERT INTO s(v) VALUES ('seed');",
        )
        .expect("seed");
    db
}

fn stock_values(db: &Path, table: &str) -> Vec<String> {
    let stock = rusqlite::Connection::open(db).expect("stock open");
    let mut stmt = stock
        .prepare(&format!("SELECT v FROM {table} ORDER BY id"))
        .expect("prepare");
    stmt.query_map([], |row| row.get(0))
        .expect("query")
        .collect::<rusqlite::Result<_>>()
        .expect("rows")
}

fn stock_integrity(db: &Path) -> String {
    rusqlite::Connection::open(db)
        .expect("stock open")
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity_check")
}

/// `fsqlite_seeds`: commit that many rows through fsqlite first, so the
/// `-wal-cert` sidecar holds that many records of this file; with none no
/// sidecar exists yet.
async fn open_frank(db: &Path, fsqlite_seeds: usize) -> Connection {
    let conn = Connection::open(db.to_str().expect("utf8 path"))
        .await
        .expect("fsqlite open");
    conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS}"))
        .await
        .expect("busy_timeout");
    for _ in 0..fsqlite_seeds {
        conn.execute("INSERT INTO s(v) VALUES ('frank-seed')")
            .await
            .expect("fsqlite seed commit");
    }
    conn
}

/// What the transaction does before its COMMIT, besides writing `t`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Txn {
    /// `BEGIN; INSERT INTO t`: it returns nothing a newer snapshot would
    /// change, so a refused snapshot is replayed inside its COMMIT.
    WriteOnly,
    /// `BEGIN; SELECT count(*) FROM <the stock child's table>; INSERT INTO
    /// t`: the count it returned is the one a refused snapshot changes, so
    /// no replay can keep it, and the COMMIT is refused.
    ReadsStockTable,
}

/// One shape's outcome on the fsqlite side.
struct ShapeOutcome {
    /// The COMMIT's answer and how long it took.
    commit: Result<usize, FrankenError>,
    took: Duration,
    /// Whether the transaction was still open after the COMMIT answered.
    open_after: bool,
    /// The application's retry (the same statements again) of a
    /// [`Txn::ReadsStockTable`] transaction whose COMMIT was refused with
    /// `BusySnapshot`.
    retry: Option<Result<usize, FrankenError>>,
    /// `(busy, log, checkpointed, elapsed)` of the child's checkpoint.
    checkpoint: (i64, i64, i64, Duration),
    /// When (unix microseconds) the COMMIT answered, and when the child's
    /// checkpoint returned.
    commit_done_us: u128,
    checkpoint_done_us: u128,
    db: PathBuf,
}

/// [`run_shape`] on the test runtime, after one fsqlite commit.
fn shape(dir: &Path, script: Script, table: &'static str, mode: &'static str) -> ShapeOutcome {
    shape_with(dir, script, table, mode, 1, Txn::WriteOnly)
}

/// How many times a holding script's shape is run to get its interleaving.
const SHAPE_ATTEMPTS: usize = 3;

/// [`run_shape`] on the test runtime.
///
/// bd-kcua5: a holding script (every script but [`Script::CommitAtGo`])
/// means the stock TRUNCATE to reset the WAL while the COMMIT waits. Its
/// child commits, which frees WRITE, and only then checkpoints, so the
/// waiting COMMIT can take WRITE in between: it then commits in the old
/// generation, before the reset, and the shape tests nothing it names (seen
/// on hz4, 2026-11-26: a transaction that read `s` beside a changed sidecar
/// answered `Ok(1)` after 308 ms). Such a run is discarded and the shape run
/// again, from a fresh database: when the reset came first, the COMMIT can
/// answer only after the checkpoint returned, so a COMMIT that answered
/// first did not wait for the reset. Only that race is discarded: a COMMIT
/// that answered `Ok` before the checkpoint returned AND promptly (within
/// the hold plus `PROMPT`). A COMMIT that stalled (slow, or `Busy` after its
/// busy_timeout, the bd-reality-check-1022-llk6g.5.7 failure) is never
/// discarded: its assertions run and fail as before. An `Err` answer is
/// never discarded either, however early (coordinator ruling on bd-kcua5,
/// 2026-11-27): it is judged on its merits, so a `BusySnapshot` passes a
/// shape whose transaction read and anything else fails. The rule before
/// that ruling also discarded an early `Err` other than `Busy`; in K1 (hz4,
/// 2026-11-27) its one discard was a `BusySnapshot` answered 2411 us before
/// the child's post-checkpoint timestamp, which this rule judges instead.
/// The test fails if no attempt got the interleaving.
fn shape_with(
    dir: &Path,
    script: Script,
    table: &'static str,
    mode: &'static str,
    fsqlite_seeds: usize,
    txn: Txn,
) -> ShapeOutcome {
    let mut missed = Vec::new();
    for attempt in 1..=SHAPE_ATTEMPTS {
        let attempt_dir = dir.join(format!("attempt-{attempt}"));
        std::fs::create_dir_all(&attempt_dir).expect("attempt dir");
        let slot = std::sync::Arc::new(std::sync::Mutex::new(None));
        let out = std::sync::Arc::clone(&slot);
        asupersync::test_utils::run_test(|| async move {
            let outcome = run_shape(&attempt_dir, script, table, mode, fsqlite_seeds, txn).await;
            *out.lock().expect("outcome slot") = Some(outcome);
        });
        let outcome = slot
            .lock()
            .expect("outcome slot")
            .take()
            .expect("the shape ran");
        let answered_first =
            script != Script::CommitAtGo && outcome.commit_done_us <= outcome.checkpoint_done_us;
        let won_the_race = answered_first
            && outcome.commit.is_ok()
            && outcome.took < PROMPT + Duration::from_millis(HOLD_MS);
        if !won_the_race {
            if answered_first {
                eprintln!(
                    "event=shape_early_answer_judged attempt={attempt} \
                     commit_answered_us_before_reset={} took={:?} commit={:?}",
                    outcome.checkpoint_done_us - outcome.commit_done_us,
                    outcome.took,
                    outcome.commit
                );
            }
            return outcome;
        }
        let early = outcome.checkpoint_done_us - outcome.commit_done_us;
        eprintln!(
            "event=shape_interleaving_missed attempt={attempt} commit_answered_us_before_reset={early} \
             commit={:?}",
            outcome.commit
        );
        missed.push(early);
    }
    panic!(
        "the COMMIT answered before the stock checkpoint returned in all {SHAPE_ATTEMPTS} \
         attempts (by {missed:?} us): the shape never got its interleaving"
    );
}

/// The transaction's statements before its COMMIT.
async fn frank_transaction_body(
    frank: &Connection,
    table: &str,
    txn: Txn,
) -> Result<(), FrankenError> {
    frank.execute("BEGIN").await?;
    if txn == Txn::ReadsStockTable {
        frank
            .query_row(&format!("SELECT count(*) FROM {table}"))
            .await?;
    }
    frank.execute("INSERT INTO t(v) VALUES ('frank')").await?;
    Ok(())
}

/// Let the stock child reach the point where the COMMIT meets it: its commit
/// landed and its checkpoint holds WRITE (`CommitAtGo`), or it holds WRITE.
fn release_stock_child(dir: &Path, script: Script) {
    if script == Script::CommitAtGo {
        std::fs::write(dir.join("go"), b"").expect("go");
        assert!(
            wait_for(&dir.join("committed"), Duration::from_secs(60)),
            "the stock child never committed"
        );
        // Its checkpoint is now waiting for the transaction's read mark,
        // holding WRITE, before the COMMIT.
        std::thread::sleep(Duration::from_millis(300));
    } else {
        assert!(
            wait_for(&dir.join("held"), Duration::from_secs(60)),
            "the stock child never took WRITE"
        );
    }
}

/// Run one shape: the fsqlite transaction writes `t` (and reads `table` for
/// [`Txn::ReadsStockTable`]); the stock child runs `script` on `table` with
/// checkpoint `mode`.
async fn run_shape(
    dir: &Path,
    script: Script,
    table: &'static str,
    mode: &'static str,
    fsqlite_seeds: usize,
    txn: Txn,
) -> ShapeOutcome {
    let db = seed(dir);
    let frank = open_frank(&db, fsqlite_seeds).await;
    let run = StockRun {
        db: db.clone(),
        dir: dir.to_path_buf(),
        script,
        table,
        mode,
    };
    frank_transaction_body(&frank, table, txn)
        .await
        .expect("the transaction before its COMMIT");
    let child = run.spawn();
    release_stock_child(dir, script);
    let started = Instant::now();
    let commit = frank.execute("COMMIT").await;
    let took = started.elapsed();
    let commit_done_us = unix_us();
    let open_after = frank.in_transaction();
    if commit.is_err() && open_after {
        let _ = frank.execute("ROLLBACK").await;
    }
    // A refused snapshot of a transaction that read: the application runs
    // the transaction again.
    let retry = if txn == Txn::ReadsStockTable
        && matches!(commit, Err(FrankenError::BusySnapshot { .. }))
        && !open_after
    {
        let outcome = match frank_transaction_body(&frank, table, txn).await {
            Ok(()) => frank.execute("COMMIT").await,
            Err(error) => Err(error),
        };
        if frank.in_transaction() {
            let _ = frank.execute("ROLLBACK").await;
        }
        Some(outcome)
    } else {
        None
    };
    child.finish();
    frank.close().await.expect("close");
    ShapeOutcome {
        commit,
        took,
        open_after,
        retry,
        checkpoint: run.checkpoint_result(),
        commit_done_us,
        checkpoint_done_us: run.checkpoint_done_us(),
        db,
    }
}

/// The rows stock leaves in the same interleaving: a stock connection plays
/// the fsqlite transaction. It takes its snapshot first (BEGIN and the read
/// the transaction makes, or a read of `t` for a write-only one: a stock
/// write would take WRITE at once and change the interleaving), the stock
/// child then commits as in the fsqlite run, and the stale stock writer is
/// refused (`SQLITE_BUSY` or `SQLITE_BUSY_SNAPSHOT`) and retried by the
/// application. Returns the refusal's extended code and the final `t` and `s`
/// rows.
fn stock_shape(
    script: Script,
    table: &'static str,
    mode: &'static str,
    fsqlite_seeds: usize,
    txn: Txn,
) -> (i32, Vec<String>, Vec<String>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = seed(dir.path());
    if fsqlite_seeds > 0 {
        // The same files as the fsqlite run, `-wal-cert` sidecar included.
        let seed_db = db.clone();
        asupersync::test_utils::run_test(|| async move {
            open_frank(&seed_db, fsqlite_seeds)
                .await
                .close()
                .await
                .expect("close");
        });
    }
    let parent = rusqlite::Connection::open(&db).expect("stock parent open");
    parent
        .busy_timeout(Duration::from_millis(BUSY_TIMEOUT_MS))
        .expect("busy_timeout");
    let read_table = if txn == Txn::ReadsStockTable {
        table
    } else {
        "t"
    };
    let read = format!("SELECT count(*) FROM {read_table}");
    parent.execute_batch("BEGIN").expect("stock BEGIN");
    let _: i64 = parent
        .query_row(&read, [], |row| row.get(0))
        .expect("stock snapshot");
    let run = StockRun {
        db: db.clone(),
        dir: dir.path().to_path_buf(),
        script,
        table,
        mode,
    };
    let child = run.spawn();
    release_stock_child(dir.path(), script);
    let first = parent
        .execute("INSERT INTO t(v) VALUES ('frank')", [])
        .and_then(|_| parent.execute_batch("COMMIT"));
    let refusal = match first {
        Ok(()) => {
            panic!("stock oracle: the stale stock writer committed; not the same interleaving")
        }
        Err(rusqlite::Error::SqliteFailure(error, _)) => error.extended_code,
        Err(error) => panic!("stock oracle: unexpected error {error}"),
    };
    let _ = parent.execute_batch("ROLLBACK");
    parent
        .execute_batch("BEGIN IMMEDIATE")
        .expect("stock retry BEGIN");
    let _: i64 = parent
        .query_row(&read, [], |row| row.get(0))
        .expect("stock retry read");
    parent
        .execute("INSERT INTO t(v) VALUES ('frank')", [])
        .expect("stock retry INSERT");
    parent.execute_batch("COMMIT").expect("stock retry COMMIT");
    child.finish();
    drop(parent);
    (refusal, stock_values(&db, "t"), stock_values(&db, "s"))
}

/// A refused snapshot ends with stock's rows (the stock oracle runs live),
/// which are also `t` and `s` here.
fn assert_rows_as_stock_leaves_them(
    context: &str,
    db: &Path,
    stock: &(i32, Vec<String>, Vec<String>),
    t: &[&str],
    s: &[&str],
) {
    let (refusal, stock_t, stock_s) = stock;
    assert!(
        stock_t.as_slice() == t && stock_s.as_slice() == s,
        "{context}: stock oracle (stale writer refused with {refusal}, then retried): t {stock_t:?} \
         and s {stock_s:?}, expected {t:?} and {s:?}"
    );
    assert_eq!(
        (stock_values(db, "t"), stock_values(db, "s")),
        (stock_t.clone(), stock_s.clone()),
        "{context}: fsqlite's rows against stock's (refused with {refusal}, then retried)"
    );
    assert_eq!(stock_integrity(db), "ok", "{context}");
}

fn assert_checkpoint_finished(context: &str, checkpoint: (i64, i64, i64, Duration)) {
    let (busy, log, checkpointed, took) = checkpoint;
    assert_eq!(
        busy, 0,
        "{context}: the stock checkpoint was blocked (busy) after {took:?}"
    );
    assert_eq!(
        log, checkpointed,
        "{context}: the stock checkpoint was partial"
    );
    assert!(
        took < PROMPT,
        "{context}: the stock checkpoint took {took:?}"
    );
}

/// The main shape: stock commits to another table and checkpoints while the
/// fsqlite transaction is open; its COMMIT then waits for WRITE. Red on main
/// d39f670d1: COMMIT and checkpoint each wait out busy_timeout.
///
/// After TRUNCATE the COMMIT commits. After RESTART its snapshot may instead
/// be refused: stock leaves an index header the WAL file does not match until
/// its next writer rewrites the WAL header, fsqlite's reader admission
/// answers that with an index recovery, and a commit holding WRITE cannot run
/// one. The write-only transaction is then replayed (bd-q4o7z) or, failing
/// that, refused once with BusySnapshot. Either way it decides at once, and
/// the checkpoint finishes.
#[test]
fn a_waiting_commit_lets_a_stock_truncate_or_restart_finish_and_then_decides() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    for mode in ["TRUNCATE", "RESTART"] {
        let dir = tempfile::tempdir().expect("tempdir");
        let outcome = shape(dir.path(), Script::CommitAtGo, "s", mode);
        let (took, db) = (outcome.took, &outcome.db);
        let context = format!("{mode}, stock wrote another table");
        let committed = match outcome.commit {
            Ok(_) => true,
            Err(FrankenError::BusySnapshot { .. }) if mode == "RESTART" => false,
            Err(error) => panic!("{context}: COMMIT failed: {error}"),
        };
        assert!(took < PROMPT, "{context}: COMMIT took {took:?}");
        assert_checkpoint_finished(&context, outcome.checkpoint);
        let expected_t: &[&str] = if committed {
            &["seed", "frank"]
        } else {
            &["seed"]
        };
        assert_eq!(stock_values(db, "t"), expected_t, "{context}");
        assert_eq!(
            stock_values(db, "s"),
            ["seed", "frank-seed", "stock"],
            "{context}"
        );
        assert_eq!(stock_integrity(db), "ok", "{context}");
    }
}

/// The first fsqlite commit a stock-created file ever sees: no `-wal-cert`
/// sidecar exists yet. Still absent after the reset, it proves no fsqlite
/// commit happened, so the COMMIT commits after the TRUNCATE as above (the
/// churn shape's first transactions and the round-4 stall probe).
#[test]
fn a_first_fsqlite_commit_with_no_sidecar_yet_lets_a_truncate_finish_and_commits() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let outcome = shape_with(
        dir.path(),
        Script::CommitAtGo,
        "s",
        "TRUNCATE",
        0,
        Txn::WriteOnly,
    );
    let (took, db) = (outcome.took, &outcome.db);
    let context = "TRUNCATE, no sidecar yet, stock wrote another table";
    if let Err(error) = &outcome.commit {
        panic!("{context}: COMMIT failed: {error}");
    }
    assert!(took < PROMPT, "{context}: COMMIT took {took:?}");
    assert_checkpoint_finished(context, outcome.checkpoint);
    assert_eq!(stock_values(db, "t"), ["seed", "frank"], "{context}");
    assert_eq!(stock_values(db, "s"), ["seed", "stock"], "{context}");
    assert_eq!(stock_integrity(db), "ok", "{context}");
}

/// A shape whose snapshot the transaction cannot keep: the stock child
/// changed a page the transaction writes, or the `-wal-cert` sidecar changed
/// so nothing proves its pages survived the reset. Always TRUNCATE.
struct RefusedShape {
    what: &'static str,
    script: Script,
    /// The table the stock child writes, and the transaction reads.
    table: &'static str,
    fsqlite_seeds: usize,
    /// The final rows of `t` and `s`, as stock leaves them.
    t: &'static [&'static str],
    s: &'static [&'static str],
}

/// Run `shape` for a transaction of kind `txn`, and its stock oracle.
///
/// - Write-only: the COMMIT is replayed on a fresh snapshot and commits,
///   within `PROMPT` of the point it met the stock child.
/// - Reads the stock child's table: refused with `BusySnapshot` within the
///   same bound, the refusal ends the transaction, and the application's
///   retry commits.
///
/// Either way the stock checkpoint finishes, and the final rows are the ones
/// stock leaves when its own stale writer is refused and retried.
fn refused_snapshot_shape(shape: &RefusedShape, txn: Txn) {
    let context = format!(
        "TRUNCATE, {}, {}",
        shape.what,
        match txn {
            Txn::WriteOnly => "write-only transaction",
            Txn::ReadsStockTable => "transaction that read",
        }
    );
    let limit = if shape.script == Script::CommitAtGo {
        PROMPT
    } else {
        PROMPT + Duration::from_millis(HOLD_MS)
    };
    let dir = tempfile::tempdir().expect("tempdir");
    let outcome = shape_with(
        dir.path(),
        shape.script,
        shape.table,
        "TRUNCATE",
        shape.fsqlite_seeds,
        txn,
    );
    let took = outcome.took;
    match txn {
        Txn::WriteOnly => {
            if let Err(error) = &outcome.commit {
                panic!("{context}: COMMIT failed after {took:?}: {error}");
            }
            assert!(took < limit, "{context}: COMMIT took {took:?}");
        }
        Txn::ReadsStockTable => {
            assert!(
                matches!(outcome.commit, Err(FrankenError::BusySnapshot { .. })),
                "{context}: COMMIT answered {:?} after {took:?}",
                outcome.commit
            );
            assert!(took < limit, "{context}: refused after {took:?}");
            assert!(
                !outcome.open_after,
                "{context}: the refusal must end the transaction"
            );
            assert!(
                matches!(outcome.retry, Some(Ok(_))),
                "{context}: the application's retry answered {:?}",
                outcome.retry
            );
        }
    }
    assert_checkpoint_finished(&context, outcome.checkpoint);
    let stock = stock_shape(
        shape.script,
        shape.table,
        "TRUNCATE",
        shape.fsqlite_seeds,
        txn,
    );
    assert_rows_as_stock_leaves_them(&context, &outcome.db, &stock, shape.t, shape.s);
}

/// Stock wrote the page the transaction writes, and committed, before the
/// COMMIT began waiting; its checkpoint then waits for the transaction's
/// read mark. The commit is seen in the WAL while the claim is held.
const SAME_PAGE_FIRST: RefusedShape = RefusedShape {
    what: "stock wrote the same page first",
    script: Script::CommitAtGo,
    table: "t",
    fsqlite_seeds: 1,
    t: &["seed", "stock", "frank"],
    s: &["seed", "frank-seed"],
};

/// Stock writes the same page WHILE the COMMIT waits, then TRUNCATE resets
/// the WAL: the change is visible only in the database file. A commit that
/// rebased across the reset without comparing pages would overwrite stock's
/// row.
const SAME_PAGE_DURING_THE_WAIT: RefusedShape = RefusedShape {
    what: "stock wrote the same page during the wait",
    script: Script::HoldWrite,
    table: "t",
    fsqlite_seeds: 1,
    t: &["seed", "stock", "frank"],
    s: &["seed", "frank-seed"],
};

#[test]
fn a_write_only_transaction_whose_page_stock_wrote_first_lands_after_stocks_row() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    refused_snapshot_shape(&SAME_PAGE_FIRST, Txn::WriteOnly);
}

#[test]
fn a_transaction_that_read_the_page_stock_wrote_first_is_refused_at_once() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    refused_snapshot_shape(&SAME_PAGE_FIRST, Txn::ReadsStockTable);
}

#[test]
fn a_write_only_transaction_whose_page_stock_wrote_during_the_wait_lands_after_stocks_row() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    refused_snapshot_shape(&SAME_PAGE_DURING_THE_WAIT, Txn::WriteOnly);
}

#[test]
fn a_transaction_that_read_the_page_stock_wrote_during_the_wait_is_refused_after_the_reset() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    refused_snapshot_shape(&SAME_PAGE_DURING_THE_WAIT, Txn::ReadsStockTable);
}

/// Stock commits to ANOTHER table while the COMMIT waits, then TRUNCATE: the
/// pages the transaction writes are unchanged in the database file, and no
/// fsqlite commit happened, so the COMMIT commits after the reset.
#[test]
fn another_table_stock_commit_during_the_wait_then_a_reset_commits() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let outcome = shape(dir.path(), Script::HoldWrite, "s", "TRUNCATE");
    let (took, db) = (outcome.took, &outcome.db);
    let context = "TRUNCATE, stock wrote another table during the wait";
    if let Err(error) = &outcome.commit {
        panic!("{context}: COMMIT failed: {error}");
    }
    assert!(
        took < PROMPT + Duration::from_millis(HOLD_MS),
        "{context}: COMMIT took {took:?}"
    );
    assert_checkpoint_finished(context, outcome.checkpoint);
    assert_eq!(stock_values(db, "t"), ["seed", "frank"], "{context}");
    assert_eq!(stock_integrity(db), "ok", "{context}");
}

/// The same as the case above, except that the `-wal-cert` sidecar changes
/// while the COMMIT waits, as an fsqlite commit would change it. Nothing then
/// proves that no fsqlite commit hid behind the reset, so the snapshot is
/// refused even though every page compares equal. The transaction that read
/// `s` is the negative: a commit that rebased across the reset without the
/// sidecar proof would commit it on its stale read.
const SIDECAR_CHANGED: RefusedShape = RefusedShape {
    what: "the sidecar changed during the wait",
    script: Script::HoldWriteTouchSidecar,
    table: "s",
    fsqlite_seeds: 1,
    t: &["seed", "frank"],
    s: &["seed", "frank-seed", "stock"],
};

/// As [`SIDECAR_CHANGED`] for the first fsqlite commit on a stock-created
/// file: the absent sidecar appears while the COMMIT waits, as the first
/// fsqlite commit would create it.
const SIDECAR_CREATED: RefusedShape = RefusedShape {
    what: "the sidecar was created during the wait",
    script: Script::HoldWriteCreateSidecar,
    table: "s",
    fsqlite_seeds: 0,
    t: &["seed", "frank"],
    s: &["seed", "stock"],
};

#[test]
fn a_write_only_transaction_ends_with_stocks_rows_when_the_sidecar_changed_during_the_wait() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    refused_snapshot_shape(&SIDECAR_CHANGED, Txn::WriteOnly);
}

#[test]
fn a_transaction_that_read_is_refused_when_the_sidecar_changed_during_the_wait() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    refused_snapshot_shape(&SIDECAR_CHANGED, Txn::ReadsStockTable);
}

#[test]
fn a_write_only_transaction_ends_with_stocks_rows_when_the_sidecar_appeared_during_the_wait() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    refused_snapshot_shape(&SIDECAR_CREATED, Txn::WriteOnly);
}

#[test]
fn a_transaction_that_read_is_refused_when_the_sidecar_appeared_during_the_wait() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    refused_snapshot_shape(&SIDECAR_CREATED, Txn::ReadsStockTable);
}

// bd-kcua5 (c9c513a65, bd-5eult): an fsqlite peer's first append of a new
// generation truncates `-wal-cert`, writes the reset generation's newest
// record back at offset 0 as a continuity anchor, then appends its own
// record. A COMMIT that waited across the reset is kept only when that
// anchor is byte-identical to the tail it captured and every later record
// is the new generation's. The shapes below are what a peer killed part-way
// through that compaction leaves while the COMMIT waits. The stock child
// rewrites the sidecar in place, holding WRITE, before its own commit and
// TRUNCATE: the checkpoint never touches `-wal-cert`, so the COMMIT finds
// the same sidecar as after a peer's compaction in the new generation.
// Only the fail-closed half is here. A shape that left the anchor alone
// committed even under a mutant that accepts only an unchanged sidecar
// (hz4, 2026-11-26), so it never reached the anchor proof. The admission
// half is the bd-30ip9 keeper below, with a real fsqlite peer process.

/// Killed after the truncate: an empty sidecar. An empty anchor proves no
/// continuity (another fsqlite commit may have hidden behind the reset), so
/// the snapshot is refused as when the sidecar changed.
const COMPACTION_KILLED_AFTER_TRUNCATE: RefusedShape = RefusedShape {
    what: "a peer's compaction died after its truncate during the wait",
    script: Script::HoldWriteEmptySidecar,
    table: "s",
    fsqlite_seeds: 2,
    t: &["seed", "frank"],
    s: &["seed", "frank-seed", "frank-seed", "stock"],
};

/// Killed part-way through the anchor write: a torn anchor proves nothing.
const COMPACTION_KILLED_IN_THE_ANCHOR: RefusedShape = RefusedShape {
    what: "a peer's compaction died writing its anchor during the wait",
    script: Script::HoldWriteTearAnchor,
    table: "s",
    fsqlite_seeds: 2,
    t: &["seed", "frank"],
    s: &["seed", "frank-seed", "frank-seed", "stock"],
};

#[test]
fn a_write_only_transaction_ends_with_stocks_rows_when_a_peer_compaction_left_no_anchor() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    refused_snapshot_shape(&COMPACTION_KILLED_AFTER_TRUNCATE, Txn::WriteOnly);
}

#[test]
fn a_transaction_that_read_is_refused_when_a_peer_compaction_left_no_anchor() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    refused_snapshot_shape(&COMPACTION_KILLED_AFTER_TRUNCATE, Txn::ReadsStockTable);
}

#[test]
fn a_write_only_transaction_ends_with_stocks_rows_when_a_peer_compaction_tore_its_anchor() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    refused_snapshot_shape(&COMPACTION_KILLED_IN_THE_ANCHOR, Txn::WriteOnly);
}

#[test]
fn a_transaction_that_read_is_refused_when_a_peer_compaction_tore_its_anchor() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    refused_snapshot_shape(&COMPACTION_KILLED_IN_THE_ANCHOR, Txn::ReadsStockTable);
}

/// bd-30ip9: make the fresh peer win the new generation's first commit
/// deterministically. The waiting writer is stopped only after it released
/// its read mark, while stock still owns WRITE. No COMMIT result is discarded
/// and no refused transaction can be replayed (`BEGIN CONCURRENT` is explicit).
#[cfg(target_os = "linux")]
mod peer_first_admission {
    use super::*;
    use nix::sys::signal::{Signal, kill};
    use nix::sys::wait::{WaitPidFlag, WaitStatus, waitpid};
    use nix::unistd::Pid;

    const CHILD: &str = "FSQLITE_30IP9_CHILD";
    const LIMIT: Duration = Duration::from_secs(30);

    fn spawn(dir: &Path, role: &str) -> ChildGuard {
        ChildGuard(Some(
            Command::new(std::env::current_exe().expect("test binary path"))
                .args([
                    "--exact",
                    "peer_first_admission::fresh_generation_child",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(CHILD, format!("{}|{role}", dir.display()))
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("spawn fsqlite child"),
        ))
    }

    /// Only the waiting child has fsqlite/OFD reader locks in this shape;
    /// the parent is stock and takes POSIX locks. Bytes 123..=127 are the
    /// stock-visible WAL reader marks. DMS at byte 128 is not a read mark.
    fn has_fsqlite_reader_mark(db: &Path) -> bool {
        file_locks_held(db).split(',').any(|lock| {
            let Some(range) = lock.strip_prefix("shm:OFDLCK:READ:-1:") else {
                return false;
            };
            let Some((start, end)) = range.split_once('-') else {
                return false;
            };
            let start = start.parse::<u64>().expect("lock start");
            let end = if end == "EOF" {
                u64::MAX
            } else {
                end.parse::<u64>().expect("lock end")
            };
            start <= 127 && end >= 123
        })
    }

    #[test]
    fn fresh_generation_child() {
        let Ok(spec) = std::env::var(CHILD) else {
            return;
        };
        let (dir, role) = spec.split_once('|').expect("child specification");
        let dir = PathBuf::from(dir);
        let db = dir.join("ckpt.db");
        asupersync::test_utils::run_test(|| async {
            let conn = open_frank(&db, usize::from(role == "waiting")).await;
            // The parent briefly SIGSTOPs this child to establish ordering;
            // the suspension must not consume the ordinary two-second budget.
            conn.execute("PRAGMA busy_timeout=60000")
                .await
                .expect("child busy timeout");
            conn.execute("PRAGMA wal_autocheckpoint=0")
                .await
                .expect("disable automatic checkpoint");
            if role == "waiting" {
                conn.execute("BEGIN CONCURRENT").await.expect("begin");
                let before = conn
                    .query_row("SELECT count(*) FROM t")
                    .await
                    .expect("read before write");
                assert_eq!(before.values(), &[fsqlite_types::SqliteValue::Integer(1)]);
                conn.execute("INSERT INTO t(v) VALUES ('frank')")
                    .await
                    .expect("private write");
                std::fs::write(dir.join("waiting-ready"), b"").expect("ready");
                assert!(wait_for(&dir.join("waiting-go"), LIMIT), "no COMMIT signal");
                let replays_before = fsqlite_core::connection::txn_replay_counts().attempts;
                let commit = conn.execute("COMMIT").await;
                let replays =
                    fsqlite_core::connection::txn_replay_counts().attempts - replays_before;
                std::fs::write(
                    dir.join("waiting-result"),
                    format!("{commit:?} replay_attempts={replays}"),
                )
                .expect("commit result");
            } else {
                assert_eq!(role, "peer");
                conn.execute("INSERT INTO s(v) VALUES ('peer')")
                    .await
                    .expect("fresh peer's first commit");
                std::fs::write(dir.join("peer-committed"), b"").expect("peer committed");
            }
            assert!(
                wait_for(&dir.join(format!("{role}-close")), LIMIT),
                "no close signal for {role}"
            );
            if conn.in_transaction() {
                conn.execute("ROLLBACK")
                    .await
                    .expect("rollback failed commit");
            }
            conn.close().await.expect("child close");
        });
    }

    #[test]
    fn a_waiting_commit_admits_a_fresh_peers_first_commit_after_stock_truncate() {
        if std::env::var_os(CHILD).is_some() {
            return;
        }
        for attempt in 0..5 {
            let dir = tempfile::tempdir().expect("shape directory");
            let db = seed(dir.path());
            // Keep this stock connection attached throughout: neither child's
            // close can reset the WAL before the assertions below.
            let stock = rusqlite::Connection::open(&db).expect("stock open");
            stock.busy_timeout(LIMIT).expect("stock busy timeout");
            stock
                .execute_batch("PRAGMA wal_autocheckpoint=0")
                .expect("disable automatic checkpoint");
            let waiting = spawn(dir.path(), "waiting");
            assert!(
                wait_for(&dir.path().join("waiting-ready"), LIMIT),
                "waiting writer never began"
            );
            assert!(
                has_fsqlite_reader_mark(&db),
                "the transaction must own an observable read mark: {}",
                file_locks_held(&db)
            );
            stock
                .execute_batch("BEGIN IMMEDIATE; INSERT INTO s(v) VALUES ('stock')")
                .expect("stock owns WRITE");
            std::fs::write(dir.path().join("waiting-go"), b"").expect("start COMMIT");
            let started = Instant::now();
            while has_fsqlite_reader_mark(&db) {
                assert!(started.elapsed() < LIMIT, "COMMIT kept its read mark");
                assert!(
                    !dir.path().join("waiting-result").exists(),
                    "COMMIT answered before releasing its read mark"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            // WRITE is still stock's, so the child's COMMIT cannot pass its
            // wait between the observation above and SIGSTOP taking effect.
            let pid = Pid::from_raw(
                i32::try_from(waiting.0.as_ref().expect("waiting child").id()).expect("child pid"),
            );
            kill(pid, Signal::SIGSTOP).expect("stop waiting writer");
            assert_eq!(
                waitpid(pid, Some(WaitPidFlag::WUNTRACED)).expect("observe stopped writer"),
                WaitStatus::Stopped(pid, Signal::SIGSTOP)
            );
            assert!(
                !dir.path().join("waiting-result").exists(),
                "COMMIT must still be waiting for WRITE"
            );
            stock.execute_batch("COMMIT").expect("stock commit");
            let checkpoint: (i64, i64, i64) = stock
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .expect("stock TRUNCATE");
            assert_eq!(checkpoint, (0, 0, 0), "attempt {attempt}");
            let peer = spawn(dir.path(), "peer");
            assert!(
                wait_for(&dir.path().join("peer-committed"), LIMIT),
                "fresh peer never committed"
            );
            kill(pid, Signal::SIGCONT).expect("resume waiting writer");
            assert!(
                wait_for(&dir.path().join("waiting-result"), LIMIT),
                "admitted COMMIT never answered"
            );
            let result = std::fs::read_to_string(dir.path().join("waiting-result"))
                .expect("waiting writer's result");
            assert_eq!(
                result, "Ok(1) replay_attempts=0",
                "attempt {attempt}: the original COMMIT must succeed without replay"
            );

            // The writes are on disjoint table pages. Their accepted outcome
            // must equal this live stock serial order, including the row read
            // before the waiting writer's private INSERT.
            let oracle_dir = tempfile::tempdir().expect("oracle directory");
            let oracle_db = seed(oracle_dir.path());
            let oracle = rusqlite::Connection::open(&oracle_db).expect("oracle open");
            oracle
                .execute_batch(
                    "INSERT INTO s(v) VALUES ('frank-seed');
                     INSERT INTO s(v) VALUES ('stock');
                     PRAGMA wal_checkpoint(TRUNCATE);
                     INSERT INTO s(v) VALUES ('peer');
                     BEGIN;",
                )
                .expect("oracle preceding commits");
            let before: i64 = oracle
                .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
                .expect("oracle read");
            assert_eq!(before, 1);
            oracle
                .execute_batch("INSERT INTO t(v) VALUES ('frank'); COMMIT;")
                .expect("oracle commit");
            for table in ["t", "s"] {
                assert_eq!(stock_values(&db, table), stock_values(&oracle_db, table));
            }
            assert_eq!(stock_integrity(&db), "ok");
            std::fs::write(dir.path().join("waiting-close"), b"").expect("close writer");
            waiting.finish();
            std::fs::write(dir.path().join("peer-close"), b"").expect("close peer");
            peer.finish();
        }
    }
}

// bd-30ip9 (bd-5eult (p)): the admission half of c9c513a65's continuity
// proof, across processes. Two fsqlite transactions wait for WRITE across a
// stock TRUNCATE: T in this process (it reads `s`, then writes `t`) and P in
// a fresh peer process (it writes `s`). Whichever takes WRITE first after the
// reset makes the new generation's first commit, which compacts `-wal-cert`
// to [anchor, its record], the anchor byte-identical to the record both
// captured. When P is first, T's own first COMMIT must commit: every fsqlite
// commit since its horizon lies in the new generation's WAL, which its
// append's conflict scan reads, and P wrote no page T writes.
//
// T read `s`, which P changes, so a refusal cannot be replayed into `Ok`
// inside the COMMIT (bd-q4o7z replays only when every value the caller saw
// comes out identical): under a mutant that accepts only an unchanged
// sidecar (the rule before c9c513a65) T's COMMIT answers `BusySnapshot`, and
// this keeper is red. On c8112ae72 T's COMMIT answered `Internal("parallel
// WAL authorized tail reconciliation failed: parallel WAL certificate epoch 1
// is not newer than 1")` (bd-30ip9).
//
// Who committed first is read from the WAL (the first frame of either
// table's root page). An attempt in which T took WRITE first after the reset
// never reached the anchor branch and is run again from a fresh database, as
// is one whose COMMIT answered `Ok` before the stock checkpoint returned (the
// HOLD race, under `shape_with`'s rule: an early `Err` is judged). A P commit
// before the reset is a hidden old-generation commit, which c9c513a65
// refuses, so T must then be refused. A stall or an error is never
// discarded, and the keeper fails if no attempt reached the interleaving.

const PEER_CHILD_ENV: &str = "FSQLITE_30IP9_PEER_CHILD";
/// Each attempt reaches the interleaving about half the time: P and T both
/// wait for WRITE across the reset.
const ADMIT_ATTEMPTS: usize = 8;

/// The fsqlite peer P. "armed": BEGIN and write `s`, say `peer-armed`, then
/// COMMIT on `peer-go` (it waits for the stock child's WRITE, as T does).
/// "fresh": open, say `peer-armed`, and on `peer-go` commit one autocommit
/// row into `s`.
#[test]
fn fsqlite_peer_child() {
    let Ok(spec) = std::env::var(PEER_CHILD_ENV) else {
        return;
    };
    let parts: Vec<&str> = spec.split('|').collect();
    let [db, dir, mode] = parts[..] else {
        panic!("peer spec {spec}");
    };
    let db = db.to_owned();
    let dir = PathBuf::from(dir);
    let fresh = mode == "fresh";
    asupersync::test_utils::run_test(|| async move {
        let peer = Connection::open(db.as_str()).await.expect("peer open");
        peer.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS}"))
            .await
            .expect("peer busy_timeout");
        if !fresh {
            peer.execute("BEGIN").await.expect("peer BEGIN");
            peer.execute("INSERT INTO s(v) VALUES ('peer')")
                .await
                .expect("peer INSERT");
        }
        std::fs::write(dir.join("peer-armed"), b"").expect("peer-armed");
        assert!(
            wait_for(&dir.join("peer-go"), Duration::from_secs(60)),
            "the parent never released the peer"
        );
        let commit = if fresh {
            peer.execute("INSERT INTO s(v) VALUES ('peer')").await
        } else {
            peer.execute("COMMIT").await
        };
        let done_us = unix_us();
        std::fs::write(
            dir.join("peer-result"),
            format!("{done_us} {}", commit.is_ok()),
        )
        .expect("peer-result");
        if peer.in_transaction() {
            let _ = peer.execute("ROLLBACK").await;
        }
        commit.expect("the peer's COMMIT");
        peer.close().await.expect("peer close");
    });
}

fn spawn_peer(db: &Path, dir: &Path, mode: &str) -> ChildGuard {
    let child = Command::new(std::env::current_exe().expect("test binary path"))
        .args([
            "--exact",
            "fsqlite_peer_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(
            PEER_CHILD_ENV,
            format!("{}|{}|{mode}", db.display(), dir.display()),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn fsqlite peer");
    ChildGuard(Some(child))
}

/// The root pages of `t` and `s`.
fn root_pages(db: &Path) -> (u32, u32) {
    let stock = rusqlite::Connection::open(db).expect("stock open");
    let root = |name: &str| -> u32 {
        stock
            .query_row(
                "SELECT rootpage FROM sqlite_master WHERE name = ?1",
                [name],
                |row| row.get(0),
            )
            .expect("rootpage")
    };
    (root("t"), root("s"))
}

/// `Some(true)` when the WAL's first frame of either table's root page is
/// `s`'s (P's table), `Some(false)` when it is `t`'s (T's).
fn peer_frame_first(db: &Path, root_t: u32, root_s: u32) -> Option<bool> {
    let wal = std::fs::read(with_suffix(db, "-wal")).ok()?;
    let page_size = usize::try_from(u32::from_be_bytes(wal.get(8..12)?.try_into().ok()?)).ok()?;
    let mut offset = 32;
    while offset + 24 + page_size <= wal.len() {
        let page = u32::from_be_bytes(wal[offset..offset + 4].try_into().ok()?);
        if page == root_s {
            return Some(true);
        }
        if page == root_t {
            return Some(false);
        }
        offset += 24 + page_size;
    }
    None
}

struct AdmitOutcome {
    commit: Result<usize, FrankenError>,
    took: Duration,
    commit_done_us: u128,
    peer_done_us: u128,
    checkpoint: (i64, i64, i64, Duration),
    checkpoint_done_us: u128,
    peer_first: Option<bool>,
    db: PathBuf,
}

async fn run_admit_shape(dir: &Path) -> AdmitOutcome {
    let db = seed(dir);
    let (root_t, root_s) = root_pages(&db);
    // One fsqlite commit: the record T and P capture, and the anchor.
    let frank = open_frank(&db, 1).await;
    let peer = spawn_peer(&db, dir, "armed");
    assert!(
        wait_for(&dir.join("peer-armed"), Duration::from_secs(60)),
        "the fsqlite peer never began its transaction"
    );
    frank_transaction_body(&frank, "s", Txn::ReadsStockTable)
        .await
        .expect("T before its COMMIT");
    let run = StockRun {
        db: db.clone(),
        dir: dir.to_path_buf(),
        script: Script::HoldWriteThenRollBack,
        table: "s",
        mode: "TRUNCATE",
    };
    let child = run.spawn();
    release_stock_child(dir, Script::HoldWriteThenRollBack);
    std::fs::write(dir.join("peer-go"), b"").expect("peer-go");
    let started = Instant::now();
    let commit = frank.execute("COMMIT").await;
    let took = started.elapsed();
    let commit_done_us = unix_us();
    if frank.in_transaction() {
        let _ = frank.execute("ROLLBACK").await;
    }
    child.finish();
    peer.finish();
    // Before this connection's close folds the WAL into the database file.
    let peer_first = peer_frame_first(&db, root_t, root_s);
    frank.close().await.expect("close");
    let peer_done_us = std::fs::read_to_string(dir.join("peer-result"))
        .expect("peer-result")
        .split_whitespace()
        .next()
        .and_then(|field| field.parse().ok())
        .expect("peer completion time");
    AdmitOutcome {
        commit,
        took,
        commit_done_us,
        peer_done_us,
        checkpoint: run.checkpoint_result(),
        checkpoint_done_us: run.checkpoint_done_us(),
        peer_first,
        db,
    }
}

/// The rows stock 3.54.1 leaves for the same transactions: a stock
/// connection plays T (BEGIN and its count of `s`), a stock connection plays
/// P and commits its row, T's stale writer is refused, a stock TRUNCATE
/// resets the WAL, and the application retries T. Returns the refusal's
/// extended code and the final `t` and `s` rows.
fn stock_admit_oracle() -> (i32, Vec<String>, Vec<String>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = seed(dir.path());
    // The same files as the fsqlite run, `-wal-cert` sidecar included.
    let seed_db = db.clone();
    asupersync::test_utils::run_test(|| async move {
        open_frank(&seed_db, 1).await.close().await.expect("close");
    });
    let open = |who: &str| {
        let conn = rusqlite::Connection::open(&db).unwrap_or_else(|error| {
            panic!("stock oracle: {who} open: {error}");
        });
        conn.busy_timeout(Duration::from_millis(BUSY_TIMEOUT_MS))
            .expect("busy_timeout");
        conn
    };
    let read = "SELECT count(*) FROM s";
    let t = open("T");
    t.execute_batch("BEGIN").expect("stock T BEGIN");
    let _: i64 = t
        .query_row(read, [], |row| row.get(0))
        .expect("stock T snapshot");
    let p = open("P");
    p.execute("INSERT INTO s(v) VALUES ('peer')", [])
        .expect("stock P commit");
    let first = t
        .execute("INSERT INTO t(v) VALUES ('frank')", [])
        .and_then(|_| t.execute_batch("COMMIT"));
    let refusal = match first {
        Ok(()) => panic!("stock oracle: the stale stock writer committed"),
        Err(rusqlite::Error::SqliteFailure(error, _)) => error.extended_code,
        Err(error) => panic!("stock oracle: unexpected error {error}"),
    };
    let _ = t.execute_batch("ROLLBACK");
    let busy: i64 = p
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))
        .expect("stock checkpoint");
    assert_eq!(busy, 0, "stock oracle: the TRUNCATE was blocked");
    t.execute_batch("BEGIN IMMEDIATE")
        .expect("stock T retry BEGIN");
    let _: i64 = t
        .query_row(read, [], |row| row.get(0))
        .expect("stock T retry read");
    t.execute("INSERT INTO t(v) VALUES ('frank')", [])
        .expect("stock T retry INSERT");
    t.execute_batch("COMMIT").expect("stock T retry COMMIT");
    drop(t);
    drop(p);
    (refusal, stock_values(&db, "t"), stock_values(&db, "s"))
}

#[test]
fn a_waiting_commit_is_admitted_when_a_peer_made_the_new_generations_first_commit_on_other_pages() {
    if std::env::var_os(CHILD_ENV).is_some() || std::env::var_os(PEER_CHILD_ENV).is_some() {
        return;
    }
    let context = "TRUNCATE while T (read s, write t) and an fsqlite peer (write s) wait; the peer \
                   commits first in the new generation";
    let limit = PROMPT + Duration::from_millis(HOLD_MS);
    let (refusal, stock_t, stock_s) = stock_admit_oracle();
    assert!(
        stock_t == ["seed", "frank"] && stock_s == ["seed", "frank-seed", "peer"],
        "{context}: stock oracle (stale writer refused with {refusal}, then retried): t \
         {stock_t:?} and s {stock_s:?}"
    );
    let dir = tempfile::tempdir().expect("tempdir");
    let mut missed = Vec::new();
    for attempt in 1..=ADMIT_ATTEMPTS {
        let attempt_dir = dir.path().join(format!("attempt-{attempt}"));
        std::fs::create_dir_all(&attempt_dir).expect("attempt dir");
        let slot = std::sync::Arc::new(std::sync::Mutex::new(None));
        let out = std::sync::Arc::clone(&slot);
        asupersync::test_utils::run_test(|| async move {
            let outcome = run_admit_shape(&attempt_dir).await;
            *out.lock().expect("outcome slot") = Some(outcome);
        });
        let outcome = slot
            .lock()
            .expect("outcome slot")
            .take()
            .expect("the shape ran");
        let took = outcome.took;
        assert_checkpoint_finished(context, outcome.checkpoint);
        let t_before_reset = outcome.commit_done_us <= outcome.checkpoint_done_us
            && outcome.commit.is_ok()
            && took < limit;
        let p_before_reset = outcome.peer_done_us <= outcome.checkpoint_done_us;
        let t_first_after_reset = outcome.peer_first == Some(false) && outcome.commit.is_ok();
        if t_before_reset || t_first_after_reset {
            eprintln!(
                "event=admit_interleaving_missed attempt={attempt} t_before_reset={t_before_reset} \
                 peer_first={:?} commit={:?}",
                outcome.peer_first, outcome.commit
            );
            missed.push((t_before_reset, outcome.peer_first));
            continue;
        }
        if p_before_reset {
            // P's commit hid behind the reset in the old generation, so
            // c9c513a65 must refuse T (round-9 keeper n1), and T read `s`.
            assert!(
                matches!(outcome.commit, Err(FrankenError::BusySnapshot { .. })),
                "{context}: the peer committed before the reset, so T must be refused; it \
                 answered {:?}",
                outcome.commit
            );
            eprintln!(
                "event=admit_interleaving_missed attempt={attempt} peer_before_reset=true \
                 (T refused)"
            );
            missed.push((false, outcome.peer_first));
            continue;
        }
        assert_eq!(
            outcome.peer_first,
            Some(true),
            "{context}: the WAL must show the peer's frame first (T answered {:?} after {took:?})",
            outcome.commit
        );
        if let Err(error) = &outcome.commit {
            panic!("{context}: T's own COMMIT answered Err({error:?}) after {took:?}");
        }
        assert!(took < limit, "{context}: T's COMMIT took {took:?}");
        assert_eq!(
            (
                stock_values(&outcome.db, "t"),
                stock_values(&outcome.db, "s")
            ),
            (stock_t.clone(), stock_s.clone()),
            "{context}: fsqlite's rows against stock's (refused with {refusal}, then retried)"
        );
        assert_eq!(stock_integrity(&outcome.db), "ok", "{context}");
        eprintln!(
            "event=admit_interleaving_got attempt={attempt} took_ms={}",
            took.as_millis()
        );
        return;
    }
    panic!(
        "{context}: no attempt of {ADMIT_ATTEMPTS} reached the interleaving \
         (t_before_reset, peer_first): {missed:?}"
    );
}

/// The control for the keeper above, with no waiting COMMIT: this process
/// commits in the old generation, a stock TRUNCATE resets the WAL, a fresh
/// fsqlite peer process makes the new generation's first commit, and this
/// process then commits one autocommit row. Stock commits such a row; so does
/// fsqlite (its begin refreshes past the peer's commit).
#[test]
fn a_commit_after_a_fresh_peers_first_commit_of_the_new_generation_commits() {
    if std::env::var_os(CHILD_ENV).is_some() || std::env::var_os(PEER_CHILD_ENV).is_some() {
        return;
    }
    let context =
        "TRUNCATE, a fresh peer's first commit of the new generation, then this process commits";
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = seed(dir.path());
        let frank = open_frank(&db, 1).await;
        let peer = spawn_peer(&db, dir.path(), "fresh");
        assert!(
            wait_for(&dir.path().join("peer-armed"), Duration::from_secs(60)),
            "the fsqlite peer never opened"
        );
        let run = StockRun {
            db: db.clone(),
            dir: dir.path().to_path_buf(),
            script: Script::CommitAtGo,
            table: "s",
            mode: "TRUNCATE",
        };
        let child = run.spawn();
        release_stock_child(dir.path(), Script::CommitAtGo);
        child.finish();
        assert_checkpoint_finished(context, run.checkpoint_result());
        std::fs::write(dir.path().join("peer-go"), b"").expect("peer-go");
        peer.finish();
        let commit = frank.execute("INSERT INTO t(v) VALUES ('frank')").await;
        assert!(
            matches!(commit, Ok(1)),
            "{context}: the INSERT answered {commit:?}"
        );
        frank.close().await.expect("close");
        assert_eq!(stock_values(&db, "t"), ["seed", "frank"], "{context}");
        assert_eq!(
            stock_values(&db, "s"),
            ["seed", "frank-seed", "stock", "peer"],
            "{context}"
        );
        assert_eq!(stock_integrity(&db), "ok", "{context}");
    });
}

const CHURN_CHILD_ENV: &str = "FSQLITE_557_CHURN_CHILD";
/// The churn shape (scratchpad p8r3_fresh_churn.py, `2 60 300 10 5000`): one
/// stock process commits `CHURN_STOCK_ROWS` autocommit rows into `s` and runs
/// `PRAGMA wal_checkpoint(TRUNCATE)` after every `CHURN_TRUNCATE_EVERY` of
/// them, while fresh-connection loops each run `CHURN_FRESH_ROWS`
/// `BEGIN; INSERT INTO t<k>; COMMIT` transactions, a new connection each, as
/// separate CLI invocations do.
const CHURN_FRESH_ROWS: usize = 60;
const CHURN_STOCK_ROWS: usize = 300;
const CHURN_TRUNCATE_EVERY: usize = 10;
const CHURN_BUSY_MS: u64 = 5000;
/// A COMMIT or stock checkpoint this slow waited out the other side.
const CHURN_STALL: Duration = Duration::from_millis(2500);
/// The stock writer pauses this long after each statement. Without it, its
/// back-to-back autocommits keep WRITE so continuously that on a loaded host
/// even stock's own fresh connections starve past busy_timeout (seen on hz4,
/// 2026-11-18), and the shape can no longer tell an fsqlite stall from host
/// starvation.
const CHURN_STOCK_PAUSE: Duration = Duration::from_millis(2);
/// How many times a stock oracle arm runs before the host is declared unfit
/// to judge (bd-bnbdo's rule).
const CHURN_ORACLE_ATTEMPTS: usize = 3;

/// One churn role in a child process; a no-op unless the parent set
/// `CHURN_CHILD_ENV`. Roles: `stock-writer` (the truncating writer),
/// `stock-fresh` and `frank-fresh` (the fresh-connection loops). Each writes
/// its result to `<dir>/result-<role>-<k>`: one line per row or checkpoint.
#[test]
fn churn_child() {
    let Ok(spec) = std::env::var(CHURN_CHILD_ENV) else {
        return;
    };
    let parts: Vec<&str> = spec.split('|').collect();
    let [role, db, dir, k] = parts[..] else {
        panic!("churn child spec {spec}");
    };
    let dir = PathBuf::from(dir);
    // The pid names this child in the stock writer's lock observations.
    std::fs::write(
        dir.join(format!("ready-{role}-{k}")),
        std::process::id().to_string(),
    )
    .expect("ready");
    assert!(wait_for(&dir.join("go"), Duration::from_secs(120)), "no go");
    let mut lines = Vec::new();
    match role {
        "stock-writer" => {
            let stock = rusqlite::Connection::open(db).expect("stock open");
            stock
                .busy_timeout(Duration::from_millis(CHURN_BUSY_MS))
                .expect("busy_timeout");
            for i in 0..CHURN_STOCK_ROWS {
                let at = unix_ms();
                match stock.execute(&format!("INSERT INTO s(v) VALUES ('{i}')"), []) {
                    Ok(_) => lines.push(format!("row {i} ok at={at}")),
                    Err(error) => lines.push(format!("row {i} err at={at} {error}")),
                }
                std::thread::sleep(CHURN_STOCK_PAUSE);
                if i % CHURN_TRUNCATE_EVERY == CHURN_TRUNCATE_EVERY - 1 {
                    let at = unix_ms();
                    let started = Instant::now();
                    let (busy, log, checkpointed): (i64, i64, i64) = stock
                        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                        })
                        .expect("stock checkpoint");
                    let ms = started.elapsed().as_millis();
                    // A busy checkpoint with log and checkpointed -1 never
                    // got the checkpoint lock: another checkpointer held it.
                    // Which process held what, right after it answered.
                    let locks = if busy == 0 {
                        String::new()
                    } else {
                        format!(" locks={}", file_locks_held(Path::new(db)))
                    };
                    lines.push(format!(
                        "checkpoint busy={busy} log={log} ckpt={checkpointed} ms={ms} at={at}{locks}"
                    ));
                }
            }
        }
        "stock-fresh" => {
            for i in 0..CHURN_FRESH_ROWS {
                let stock = rusqlite::Connection::open(db).expect("stock open");
                stock
                    .busy_timeout(Duration::from_millis(CHURN_BUSY_MS))
                    .expect("busy_timeout");
                let at = unix_ms();
                let started = Instant::now();
                let outcome = stock.execute_batch(&format!(
                    "BEGIN; INSERT INTO t{k}(v) VALUES ('{i}'); COMMIT;"
                ));
                let ms = started.elapsed().as_millis();
                match outcome {
                    Ok(()) => lines.push(format!("txn {i} ok ms={ms} at={at}")),
                    Err(error) => {
                        lines.push(format!("txn {i} err ms={ms} at={at} code=0 {error}"));
                    }
                }
            }
        }
        "frank-fresh" => {
            let path = db.to_owned();
            let out = &mut lines;
            asupersync::test_utils::run_test(|| async move {
                for i in 0..CHURN_FRESH_ROWS {
                    let opened_at = unix_ms();
                    let conn = Connection::open(&path).await.expect("fsqlite open");
                    conn.execute(&format!("PRAGMA busy_timeout={CHURN_BUSY_MS}"))
                        .await
                        .expect("busy_timeout");
                    // Each statement's own duration, so a stall names its
                    // statement.
                    let at = unix_ms();
                    let started = Instant::now();
                    let mut stage = "begin";
                    let mut outcome = conn.execute("BEGIN").await;
                    let begin_ms = started.elapsed().as_millis();
                    if outcome.is_ok() {
                        stage = "insert";
                        outcome = conn
                            .execute(&format!("INSERT INTO t{k}(v) VALUES ('{i}')"))
                            .await;
                    }
                    let insert_ms = started.elapsed().as_millis() - begin_ms;
                    if outcome.is_ok() {
                        stage = "commit";
                        outcome = conn.execute("COMMIT").await;
                    }
                    let ms = started.elapsed().as_millis();
                    let commit_ms = ms - begin_ms - insert_ms;
                    let line = match outcome {
                        Ok(_) => format!(
                            "txn {i} ok ms={ms} at={at} begin={begin_ms} insert={insert_ms} \
                             commit={commit_ms}"
                        ),
                        Err(error) => format!(
                            "txn {i} err ms={ms} at={at} code={} in_txn_after={} stmt={stage} \
                             begin={begin_ms} insert={insert_ms} commit={commit_ms} {error}",
                            error.extended_error_code(),
                            conn.in_transaction()
                        ),
                    };
                    if conn.in_transaction() {
                        let _ = conn.execute("ROLLBACK").await;
                    }
                    // `close`, as a CLI's exit does: on main its passive
                    // checkpoint took the checkpoint lock while the stock
                    // writer was attached, and the stock TRUNCATE that met
                    // it answered busy at once.
                    let close_at = unix_ms();
                    conn.close().await.expect("close");
                    // The connection's whole life, open to closed, so a
                    // blocked stock checkpoint can be laid against it.
                    out.push(format!(
                        "{line} opened_at={opened_at} close_at={close_at} closed_at={}",
                        unix_ms()
                    ));
                }
            });
        }
        other => panic!("unknown churn role {other}"),
    }
    let mut text = lines.join("\n");
    text.push('\n');
    std::fs::write(dir.join(format!("result-{role}-{k}")), text).expect("result");
}

/// The POSIX and OFD locks held now on `db` and its `-shm`, from
/// `/proc/locks`, as `<file>:<kind>:<READ|WRITE>:<pid>:<start>-<end>` (an OFD
/// lock shows pid -1). On the `-shm`, byte 120 is WAL WRITE, 121 the
/// checkpoint lock, 122 RECOVER and 123.. the reader marks. Empty where
/// `/proc/locks` is unavailable.
fn file_locks_held(db: &Path) -> String {
    use std::os::unix::fs::MetadataExt as _;
    let mut shm = db.as_os_str().to_owned();
    shm.push("-shm");
    let files: Vec<(&str, u64)> = [("db", db.to_path_buf()), ("shm", PathBuf::from(shm))]
        .into_iter()
        .filter_map(|(name, path)| Some((name, std::fs::metadata(path).ok()?.ino())))
        .collect();
    let table = std::fs::read_to_string("/proc/locks").unwrap_or_default();
    let mut held = Vec::new();
    for line in table.lines() {
        // "1: POSIX  ADVISORY  WRITE 1234 fd:01:5678 120 120"
        let fields: Vec<&str> = line.split_whitespace().collect();
        let [_, kind, _, mode, pid, device, start, end, ..] = fields[..] else {
            continue;
        };
        let Some(inode) = device
            .rsplit(':')
            .next()
            .and_then(|ino| ino.parse::<u64>().ok())
        else {
            continue;
        };
        if let Some((name, _)) = files.iter().find(|(_, ino)| *ino == inode) {
            held.push(format!("{name}:{kind}:{mode}:{pid}:{start}-{end}"));
        }
    }
    held.join(",")
}

/// Wall-clock milliseconds, comparable across the churn's child processes.
fn unix_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_millis())
}

fn unix_us() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_micros())
}

/// The value of `key=` among a result line's space-separated fields.
fn churn_field<T: std::str::FromStr>(line: &str, key: &str) -> Option<T> {
    line.split(' ')
        .find_map(|field| field.strip_prefix(key)?.strip_prefix('='))
        .and_then(|value| value.parse().ok())
}

/// One churn loop's outcome, read from its result file.
#[derive(Debug, Default)]
struct ChurnLoop {
    committed: Vec<String>,
    /// `(extended code, text)` of each failed transaction.
    errors: Vec<(i64, String)>,
    max_txn: Duration,
    /// `(start unix ms, duration ms, line)` of every transaction.
    txns: Vec<(u128, u64, String)>,
}

#[derive(Debug)]
struct ChurnOutcome {
    loops: Vec<ChurnLoop>,
    stock_rows_ok: usize,
    /// Start unix ms of each stock row that committed.
    stock_rows_at: Vec<u128>,
    /// `(start unix ms, duration ms, busy)` of each stock checkpoint.
    checkpoints_at: Vec<(u128, u64, bool)>,
    checkpoints: usize,
    busy_checkpoints: usize,
    max_checkpoint: Duration,
    /// The stock writer's line for each busy checkpoint: its log and
    /// checkpointed counts, and the file locks held right after it answered.
    busy_checkpoint_lines: Vec<String>,
    /// `<role>-<k>=<pid>` of every child, to name the lock holders.
    pids: Vec<String>,
    /// `t<k>` values stock reads afterwards, per loop.
    durable: Vec<Vec<String>>,
    stock_rows_durable: usize,
    integrity: String,
}

/// Run the churn shape with `fresh` loops of `engine` ("stock" or "frank")
/// beside the stock truncating writer, all in child processes.
fn run_churn(engine: &str, fresh: usize) -> ChurnOutcome {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("churn.db");
    let tables: String = (0..fresh)
        .map(|k| format!("CREATE TABLE t{k}(id INTEGER PRIMARY KEY, v TEXT NOT NULL);"))
        .collect();
    rusqlite::Connection::open(&db)
        .expect("seed open")
        .execute_batch(&format!(
            "PRAGMA journal_mode=WAL; CREATE TABLE s(id INTEGER PRIMARY KEY, v TEXT NOT NULL); {tables}"
        ))
        .expect("seed");
    let spawn = |role: &str, k: usize| {
        let child = Command::new(std::env::current_exe().expect("test binary path"))
            .args(["--exact", "churn_child", "--nocapture", "--test-threads=1"])
            .env(
                CHURN_CHILD_ENV,
                format!("{role}|{}|{}|{k}", db.display(), dir.path().display()),
            )
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn churn child");
        (format!("{role}-{k}"), ChildGuard(Some(child)))
    };
    let fresh_role = format!("{engine}-fresh");
    let mut children = vec![spawn("stock-writer", 0)];
    children.extend((0..fresh).map(|k| spawn(&fresh_role, k)));
    for (name, _) in &children {
        assert!(
            wait_for(
                &dir.path().join(format!("ready-{name}")),
                Duration::from_secs(120)
            ),
            "churn child {name} never became ready"
        );
    }
    std::fs::write(dir.path().join("go"), b"").expect("go");
    // Drain every child's pipes at once. Reaped one after another, a child
    // whose log fills its pipe blocks in that write until the parent reaches
    // it, holding whatever lock it had, and stalls the other children with it.
    let reapers: Vec<_> = children
        .into_iter()
        .map(|(name, child)| (name, std::thread::spawn(move || child.finish())))
        .collect();
    for (name, reaper) in reapers {
        if let Err(panic) = reaper.join() {
            eprintln!("churn child {name} failed");
            std::panic::resume_unwind(panic);
        }
    }
    let read = |name: &str| {
        std::fs::read_to_string(dir.path().join(format!("result-{name}"))).expect("churn result")
    };
    let mut loops = Vec::new();
    for k in 0..fresh {
        let mut outcome = ChurnLoop::default();
        for line in read(&format!("{fresh_role}-{k}")).lines() {
            let fields: Vec<&str> = line.splitn(4, ' ').collect();
            let ms: u64 = churn_field(line, "ms").expect("txn ms");
            let at: u128 = churn_field(line, "at").expect("txn start");
            outcome.max_txn = outcome.max_txn.max(Duration::from_millis(ms));
            outcome.txns.push((at, ms, line.to_owned()));
            if fields[2] == "ok" {
                outcome.committed.push(fields[1].to_owned());
            } else {
                let code = churn_field(line, "code").unwrap_or(-1);
                outcome.errors.push((code, line.to_owned()));
            }
        }
        loops.push(outcome);
    }
    let (mut stock_rows_ok, mut checkpoints, mut busy_checkpoints) = (0, 0, 0);
    let (mut stock_rows_at, mut checkpoints_at) = (Vec::new(), Vec::new());
    let mut busy_checkpoint_lines = Vec::new();
    let mut max_checkpoint = Duration::ZERO;
    for line in read("stock-writer-0").lines() {
        if line.starts_with("row ") && line.split(' ').nth(2) == Some("ok") {
            stock_rows_ok += 1;
            stock_rows_at.push(churn_field(line, "at").expect("row start"));
        } else if line.starts_with("checkpoint ") {
            checkpoints += 1;
            let busy = churn_field::<i64>(line, "busy").expect("checkpoint busy") != 0;
            let ms: u64 = churn_field(line, "ms").expect("checkpoint ms");
            if busy {
                busy_checkpoints += 1;
                busy_checkpoint_lines.push(line.to_owned());
            }
            max_checkpoint = max_checkpoint.max(Duration::from_millis(ms));
            checkpoints_at.push((churn_field(line, "at").expect("checkpoint start"), ms, busy));
        }
    }
    let durable = (0..fresh)
        .map(|k| stock_values(&db, &format!("t{k}")))
        .collect();
    let stock_rows_durable = stock_values(&db, "s").len();
    let pids = std::iter::once("stock-writer-0".to_owned())
        .chain((0..fresh).map(|k| format!("{fresh_role}-{k}")))
        .map(|name| {
            let pid = std::fs::read_to_string(dir.path().join(format!("ready-{name}")))
                .unwrap_or_default();
            format!("{name}={pid}")
        })
        .collect();
    ChurnOutcome {
        loops,
        stock_rows_ok,
        stock_rows_at,
        checkpoints_at,
        checkpoints,
        busy_checkpoints,
        max_checkpoint,
        busy_checkpoint_lines,
        pids,
        durable,
        stock_rows_durable,
        integrity: stock_integrity(&db),
    }
}

/// What the other processes did while loop `k`'s slowest transaction ran:
/// stock rows and checkpoints still completing mean that loop alone was
/// stuck; nothing completing anywhere points at the host.
fn churn_stall_context(outcome: &ChurnOutcome, k: usize) -> String {
    let Some((at, ms, line)) = outcome.loops[k].txns.iter().max_by_key(|(_, ms, _)| *ms) else {
        return "no transactions".to_owned();
    };
    let window = *at..at + u128::from(*ms);
    let rows = outcome
        .stock_rows_at
        .iter()
        .filter(|row_at| window.contains(*row_at))
        .count();
    let checkpoints: Vec<_> = outcome
        .checkpoints_at
        .iter()
        .filter(|(checkpoint_at, _, _)| window.contains(checkpoint_at))
        .map(|(_, ms, busy)| format!("{ms}ms{}", if *busy { " busy" } else { "" }))
        .collect();
    let others: Vec<String> = outcome
        .loops
        .iter()
        .enumerate()
        .filter(|(other, _)| *other != k)
        .map(|(other, lane)| {
            let started = lane
                .txns
                .iter()
                .filter(|(txn_at, _, _)| window.contains(txn_at))
                .count();
            format!("loop {other} began {started} transactions")
        })
        .collect();
    format!(
        "slowest: {line:?}; while it ran the stock writer committed {rows} rows and ran \
         checkpoints {checkpoints:?}; {others:?}"
    )
}

/// What every churn arm must show whatever its row count: integrity ok, the
/// durable rows exactly the committed ones, the stock writer's rows all
/// landed, and no COMMIT or stock checkpoint waiting out the other side.
fn assert_churn_consistent(context: &str, outcome: &ChurnOutcome) {
    assert_eq!(outcome.integrity, "ok", "{context}: integrity_check");
    for (k, (lane, durable)) in outcome.loops.iter().zip(&outcome.durable).enumerate() {
        assert_eq!(
            &lane.committed, durable,
            "{context}: loop {k}: the durable rows are exactly the transactions whose COMMIT \
             succeeded"
        );
        assert!(
            lane.max_txn < CHURN_STALL,
            "{context}: loop {k}: a transaction took {:?} (a stall); {}; errors {:?}",
            lane.max_txn,
            churn_stall_context(outcome, k),
            lane.errors
        );
    }
    assert_eq!(
        (outcome.stock_rows_ok, outcome.stock_rows_durable),
        (CHURN_STOCK_ROWS, CHURN_STOCK_ROWS),
        "{context}: the stock writer's rows"
    );
    assert_eq!(
        outcome.checkpoints,
        CHURN_STOCK_ROWS / CHURN_TRUNCATE_EVERY,
        "{context}: stock checkpoints run"
    );
    assert_eq!(
        outcome.busy_checkpoints, 0,
        "{context}: stock TRUNCATE checkpoints that were blocked (max {:?}): {:?}; children {:?}",
        outcome.max_checkpoint, outcome.busy_checkpoint_lines, outcome.pids
    );
    assert!(
        outcome.max_checkpoint < CHURN_STALL,
        "{context}: a stock checkpoint took {:?}",
        outcome.max_checkpoint
    );
}

/// Why a stock churn arm missed its own answer on this host, if it did:
/// every loop commits every row, no transaction or checkpoint stalls, the
/// stock writer's rows all land, and integrity is ok.
fn churn_oracle_miss(outcome: &ChurnOutcome) -> Option<String> {
    if outcome.integrity != "ok" {
        return Some(format!("integrity_check: {}", outcome.integrity));
    }
    for (k, lane) in outcome.loops.iter().enumerate() {
        if lane.committed.len() != CHURN_FRESH_ROWS || lane.max_txn >= CHURN_STALL {
            return Some(format!(
                "loop {k}: {} of {CHURN_FRESH_ROWS} rows, slowest transaction {:?}, errors {:?}",
                lane.committed.len(),
                lane.max_txn,
                lane.errors.iter().take(3).collect::<Vec<_>>()
            ));
        }
    }
    if outcome.stock_rows_ok != CHURN_STOCK_ROWS
        || outcome.busy_checkpoints != 0
        || outcome.max_checkpoint >= CHURN_STALL
    {
        return Some(format!(
            "stock writer: {} rows, {} busy checkpoints, slowest checkpoint {:?}",
            outcome.stock_rows_ok, outcome.busy_checkpoints, outcome.max_checkpoint
        ));
    }
    None
}

/// bd-bnbdo's rule for a stock oracle: run the stock arm of the churn shape
/// with `fresh` loops; when it misses its own answer, the host starved stock
/// itself and cannot judge fsqlite, so the arm is rerun, up to
/// [`CHURN_ORACLE_ATTEMPTS`] times, and the test fails as "oracle invalid on
/// this host" if every attempt misses. It never passes without a valid
/// oracle, and the fsqlite arm is never rerun.
fn stock_churn_oracle(fresh: usize) -> ChurnOutcome {
    let mut misses = Vec::new();
    for attempt in 1..=CHURN_ORACLE_ATTEMPTS {
        let outcome = run_churn("stock", fresh);
        let Some(miss) = churn_oracle_miss(&outcome) else {
            return outcome;
        };
        eprintln!("event=churn_oracle_invalid fresh={fresh} attempt={attempt} miss={miss}");
        misses.push(miss);
    }
    panic!(
        "stock churn oracle, {fresh} loops: oracle invalid on this host, rerun: stock missed its \
         own answer in all {CHURN_ORACLE_ATTEMPTS} attempts: {misses:?}"
    );
}

/// The churn shape with ONE fresh-connection loop beside the stock truncating
/// writer, measured against stock running the same loop live. Stock commits
/// every row, and so must fsqlite: its first transaction is also the first
/// fsqlite commit on a stock-created file (no `-wal-cert` yet). Red on main
/// d39f670d1: a COMMIT waiting for WRITE kept its read mark, the stock
/// TRUNCATE holding WRITE waited for it, and each side spent busy_timeout.
#[test]
fn a_fresh_connection_loop_beside_a_truncating_stock_writer_commits_every_row_as_stock_does() {
    if std::env::var_os(CHILD_ENV).is_some() || std::env::var_os(CHURN_CHILD_ENV).is_some() {
        return;
    }
    let stock = stock_churn_oracle(1);
    let frank = run_churn("frank", 1);
    assert_churn_consistent("fsqlite, one loop", &frank);
    assert_eq!(
        frank.loops[0].committed.len(),
        stock.loops[0].committed.len(),
        "fsqlite, one loop: rows committed against stock's; errors {:?}",
        frank.loops[0].errors
    );
}

/// The churn shape with TWO fsqlite fresh-connection loops. A COMMIT waiting
/// for WRITE releases its read mark; when the other loop's fsqlite commit
/// changed the `-wal-cert` sidecar during the wait, nothing proves its pages
/// survived the reset, so the COMMIT is refused with BusySnapshot (and the
/// refusal ends its transaction). What must hold: no stall, no lost committed
/// row, no failed row made durable, and no failure other than that refusal.
/// Stock's row count (every row) is not asserted here: with fsqlite peers it
/// needs a commit counter that survives the WAL reset, which
/// bd-reality-check-1022-llk6g.5.7 keeps open. The stock oracle runs first,
/// two stock loops in the same shape: a host that starves stock itself cannot
/// judge a stall.
#[test]
fn two_fsqlite_fresh_loops_beside_a_truncating_stock_writer_never_stall_or_lose_a_committed_row() {
    if std::env::var_os(CHILD_ENV).is_some() || std::env::var_os(CHURN_CHILD_ENV).is_some() {
        return;
    }
    let stock = stock_churn_oracle(2);
    let frank = run_churn("frank", 2);
    // bd-5eult's probe reads these: fsqlite's rows per loop against stock's
    // on the same shape, and each refusal's text.
    eprintln!(
        "event=churn_two_loop_rows fsqlite={:?} stock={:?} refusals={:?}",
        frank
            .loops
            .iter()
            .map(|lane| lane.committed.len())
            .collect::<Vec<_>>(),
        stock
            .loops
            .iter()
            .map(|lane| lane.committed.len())
            .collect::<Vec<_>>(),
        frank
            .loops
            .iter()
            .flat_map(|lane| lane.errors.iter().map(|(_, line)| line.as_str()))
            .collect::<Vec<_>>()
    );
    assert_churn_consistent("fsqlite, two loops", &frank);
    for (k, lane) in frank.loops.iter().enumerate() {
        let other: Vec<&(i64, String)> = lane
            .errors
            .iter()
            .filter(|(code, line)| *code != 517 || !line.contains("in_txn_after=false"))
            .collect();
        assert!(
            other.is_empty(),
            "fsqlite, two loops: loop {k}: the only failure may be a BusySnapshot COMMIT refusal \
             that ended its transaction; got {other:?}"
        );
    }
}

const ATTACHED_CHILD_ENV: &str = "FSQLITE_557_ATTACHED_CHILD";

fn is_a_child() -> bool {
    [CHILD_ENV, CHURN_CHILD_ENV, ATTACHED_CHILD_ENV]
        .iter()
        .any(|name| std::env::var_os(name).is_some())
}

/// A stock connection attached to `<dir>/ckpt.db`. Its read takes the
/// database file's SHARED lock, which a WAL-mode stock connection keeps until
/// it closes; it closes when `<dir>/release` appears. If it is the last
/// connection then, its close folds the WAL in, as every stock close does.
#[test]
fn stock_attached_child() {
    let Ok(dir) = std::env::var(ATTACHED_CHILD_ENV) else {
        return;
    };
    let dir = PathBuf::from(dir);
    let stock = rusqlite::Connection::open(dir.join("ckpt.db")).expect("stock open");
    stock
        .busy_timeout(Duration::from_millis(BUSY_TIMEOUT_MS))
        .expect("busy_timeout");
    let _: i64 = stock
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .expect("stock read");
    std::fs::write(dir.join("attached"), b"").expect("attached");
    assert!(
        wait_for(&dir.join("release"), Duration::from_secs(60)),
        "the attached stock child was never released"
    );
    stock
        .close()
        .map_err(|(_, error)| error)
        .expect("stock close");
}

fn attach_stock_child(dir: &Path) -> ChildGuard {
    let child = Command::new(std::env::current_exe().expect("test binary path"))
        .args([
            "--exact",
            "stock_attached_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(ATTACHED_CHILD_ENV, dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the attached stock child");
    let child = ChildGuard(Some(child));
    assert!(
        wait_for(&dir.join("attached"), Duration::from_secs(60)),
        "the stock child never attached"
    );
    child
}

/// Who commits beside the attached stock child, then closes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Closer {
    Fsqlite,
    Stock,
}

/// When the attached stock child closes: before the writer's close, which
/// makes the writer the last closer, or after it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Detach {
    BeforeTheClose,
    AfterTheClose,
}

/// The rows the writer commits into `t`, one autocommit each.
const CLOSE_ROWS: [&str; 8] = ["w1", "w2", "w3", "w4", "w5", "w6", "w7", "w8"];

/// The WAL as a close left it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WalLeft {
    /// No `-wal`, or an empty one (stock's last close removes it).
    Gone,
    /// Every frame is in the database file (the `-shm` says nBackfill equals
    /// mxFrame), so the next writer restarts the WAL from its start.
    FoldedIn { frames: u32 },
    /// Frames the database file does not hold yet.
    Pending { frames: u32, backfilled: u32 },
}

fn with_suffix(db: &Path, suffix: &str) -> PathBuf {
    let mut path = db.as_os_str().to_owned();
    path.push(suffix);
    PathBuf::from(path)
}

fn wal_left(db: &Path) -> WalLeft {
    let has_frames = std::fs::metadata(with_suffix(db, "-wal")).is_ok_and(|meta| meta.len() > 0);
    if !has_frames {
        return WalLeft::Gone;
    }
    let shm = std::fs::read(with_suffix(db, "-shm")).expect("a WAL with frames has its -shm");
    // mxFrame is at byte 16 of the first WAL-index header copy; nBackfill
    // opens the checkpoint info after both copies, at byte 96.
    let word = |at: usize| {
        u32::from_ne_bytes(
            shm.get(at..at + 4)
                .and_then(|bytes| bytes.try_into().ok())
                .expect("a -shm with an initialized WAL-index header"),
        )
    };
    let (frames, backfilled) = (word(16), word(96));
    if backfilled == frames {
        WalLeft::FoldedIn { frames }
    } else {
        WalLeft::Pending { frames, backfilled }
    }
}

/// `t` in a copy of the database file alone, without the WAL: the rows a
/// checkpoint has folded in.
fn t_in_database_file_alone(db: &Path, step: &str) -> Vec<String> {
    let copy = db.with_file_name(format!("alone-{step}.db"));
    std::fs::copy(db, &copy).expect("copy the database file");
    stock_values(&copy, "t")
}

/// One close shape's observations.
struct CloseOutcome {
    /// `t` in the database file alone just before the writer's close.
    alone_before_close: Vec<String>,
    /// `t` in the database file alone just after it, and the WAL it left.
    alone_after_close: Vec<String>,
    wal_after_close: WalLeft,
    /// `t` in the database file alone once every connection closed.
    alone_at_end: Vec<String>,
    /// `t` through a stock open of the database, WAL included, at the end.
    t_at_end: Vec<String>,
    integrity: String,
}

/// A stock child attaches to a stock-created WAL database, `closer` commits
/// [`CLOSE_ROWS`] into `t` and closes, and the child closes before or after
/// that close, per `detach`.
fn close_shape(closer: Closer, detach: Detach) -> CloseOutcome {
    let temp = tempfile::tempdir().expect("tempdir");
    let dir = temp.path();
    let db = seed(dir);
    let mut child = Some(attach_stock_child(dir));
    let mut detach_at = |when: Detach| {
        if when == detach {
            std::fs::write(dir.join("release"), b"").expect("release");
            child.take().expect("the child is attached").finish();
        }
    };
    let (alone_before_close, alone_after_close, wal_after_close) = match closer {
        Closer::Fsqlite => {
            let mut steps = None;
            let (steps_slot, detach_now, db_path) = (&mut steps, &mut detach_at, &db);
            asupersync::test_utils::run_test(|| async move {
                let frank = open_frank(db_path, 0).await;
                for row in CLOSE_ROWS {
                    frank
                        .execute(&format!("INSERT INTO t(v) VALUES ('{row}')"))
                        .await
                        .expect("fsqlite commit");
                }
                detach_now(Detach::BeforeTheClose);
                let before = t_in_database_file_alone(db_path, "before");
                frank.close().await.expect("fsqlite close");
                *steps_slot = Some((
                    before,
                    t_in_database_file_alone(db_path, "after"),
                    wal_left(db_path),
                ));
            });
            steps.expect("the fsqlite writer ran")
        }
        Closer::Stock => {
            let stock = rusqlite::Connection::open(&db).expect("stock open");
            stock
                .busy_timeout(Duration::from_millis(BUSY_TIMEOUT_MS))
                .expect("busy_timeout");
            for row in CLOSE_ROWS {
                stock
                    .execute_batch(&format!("INSERT INTO t(v) VALUES ('{row}')"))
                    .expect("stock commit");
            }
            detach_at(Detach::BeforeTheClose);
            let before = t_in_database_file_alone(&db, "before");
            stock
                .close()
                .map_err(|(_, error)| error)
                .expect("stock close");
            (
                before,
                t_in_database_file_alone(&db, "after"),
                wal_left(&db),
            )
        }
    };
    detach_at(Detach::AfterTheClose);
    CloseOutcome {
        alone_before_close,
        alone_after_close,
        wal_after_close,
        alone_at_end: t_in_database_file_alone(&db, "end"),
        t_at_end: stock_values(&db, "t"),
        integrity: stock_integrity(&db),
    }
}

fn all_close_rows() -> Vec<String> {
    std::iter::once("seed")
        .chain(CLOSE_ROWS)
        .map(str::to_owned)
        .collect()
}

/// The last closer still checkpoints as before the close rule: the stock
/// child closes first, while the writer is still attached, so its close
/// cannot fold the WAL in; the writer's own close then must. Stock, live:
/// its close folds every row into the database file and removes the WAL.
/// fsqlite's close must fold the same rows in and leave the WAL fully
/// backfilled (or gone), so the next writer restarts it.
#[test]
fn the_last_closer_still_folds_the_wal_into_the_database_file_as_stock_does() {
    if is_a_child() {
        return;
    }
    let all = all_close_rows();
    let stock = close_shape(Closer::Stock, Detach::BeforeTheClose);
    assert!(
        stock.alone_before_close == ["seed"]
            && stock.alone_after_close == all
            && stock.wal_after_close == WalLeft::Gone,
        "stock oracle, last closer: database file alone {:?} before its close and {:?} after, \
         WAL {:?}",
        stock.alone_before_close,
        stock.alone_after_close,
        stock.wal_after_close
    );
    let frank = close_shape(Closer::Fsqlite, Detach::BeforeTheClose);
    assert_eq!(
        frank.alone_before_close, stock.alone_before_close,
        "fsqlite last closer: the stock child's close beside the attached fsqlite writer \
         left the WAL alone, as beside a stock writer"
    );
    assert_eq!(
        frank.alone_after_close, stock.alone_after_close,
        "fsqlite last closer: its close folds every row into the database file, as stock's"
    );
    assert!(
        matches!(
            frank.wal_after_close,
            WalLeft::Gone | WalLeft::FoldedIn { .. }
        ),
        "fsqlite last closer: the WAL after its close is {:?}, not fully backfilled",
        frank.wal_after_close
    );
    assert_eq!(frank.alone_at_end, all, "fsqlite last closer");
    assert_eq!(frank.t_at_end, all, "fsqlite last closer");
    assert_eq!(frank.integrity, "ok", "fsqlite last closer");
}

/// A close beside another attached process leaves the WAL to whoever closes
/// last, as stock's `sqlite3WalClose` does: it cannot take EXCLUSIVE on the
/// database file then. Stock, live: its writer's close folds nothing in and
/// the stock child, closing last, folds every row in. fsqlite's close must
/// leave the same database file, with its frames still pending in the WAL,
/// and the same stock last closer must then fold them in. A close that
/// checkpoints anyway (main before this rule) holds the WAL checkpoint lock
/// that a stock TRUNCATE or RESTART answers busy at once, and fails here.
#[test]
fn a_close_beside_an_attached_process_leaves_the_wal_to_the_last_closer_as_stock_does() {
    if is_a_child() {
        return;
    }
    let all = all_close_rows();
    let stock = close_shape(Closer::Stock, Detach::AfterTheClose);
    assert!(
        stock.alone_after_close == ["seed"]
            && matches!(stock.wal_after_close, WalLeft::Pending { .. })
            && stock.alone_at_end == all,
        "stock oracle, close beside an attached process: database file alone {:?} after its \
         close and {:?} at the end, WAL {:?}",
        stock.alone_after_close,
        stock.alone_at_end,
        stock.wal_after_close
    );
    let frank = close_shape(Closer::Fsqlite, Detach::AfterTheClose);
    assert_eq!(
        frank.alone_after_close, stock.alone_after_close,
        "fsqlite close beside an attached process: the database file after its close, against \
         stock's (WAL {:?})",
        frank.wal_after_close
    );
    assert!(
        matches!(frank.wal_after_close, WalLeft::Pending { .. }),
        "fsqlite close beside an attached process: WAL {:?}",
        frank.wal_after_close
    );
    assert_eq!(
        frank.alone_at_end, stock.alone_at_end,
        "fsqlite close beside an attached process: the stock last closer folds the WAL it left in"
    );
    assert_eq!(
        frank.t_at_end, all,
        "fsqlite close beside an attached process"
    );
    assert_eq!(
        frank.integrity, "ok",
        "fsqlite close beside an attached process"
    );
}
