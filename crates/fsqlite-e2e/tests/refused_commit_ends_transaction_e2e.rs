#![recursion_limit = "512"]

//! bd-reality-check-1022-llk6g.5.1: a COMMIT refused with `BusySnapshot` ends
//! its transaction.
//!
//! On main 714e81184 the refused transaction stayed open on its stale snapshot.
//! It could never commit, so every later statement of the connection landed in
//! it: the next `BEGIN` failed "cannot start a transaction within a
//! transaction", and later rows were lost with it, or a later COMMIT made rows
//! durable whose COMMIT had been reported failed. Eight CLI writers x 300
//! `BEGIN; INSERT; COMMIT` on one table committed 300-312 of 2400 rows; stock
//! commits 2400.
//!
//! Stock documents that an error may roll a transaction back automatically,
//! with autocommit on afterwards. Its own example is `INSERT OR ROLLBACK` on a
//! duplicate key, which this file uses as the oracle for what an automatically
//! rolled-back transaction looks like: autocommit on, `ROLLBACK` reports
//! "cannot rollback - no transaction is active", the next `BEGIN` works, and
//! the transaction's rows are gone.
//!
//! The negative case: a retryable `Busy` at COMMIT (another fsqlite process
//! holds the write lock with BEGIN IMMEDIATE) keeps the transaction open, and a
//! second COMMIT succeeds once the holder is done.
//!
//! bd-igdof: the same with a STOCK process holding the write lock, run the way
//! a CLI script runs it, against stock's own answer to that sequence.
//!
//! bd-reality-check-1022-llk6g.5.5: a transaction open while a STOCK process
//! commits. The file starts with no `-wal`; the fsqlite connection creates one
//! with no frames, and stock's first commit into a frame-less WAL writes a new
//! header with fresh salts on the same inode. On main cdb079ffa every COMMIT
//! of the open transaction then returned "database is busy (recovery in
//! progress)", forever, and its row was lost; 1e0b54d7d made it a single
//! BusySnapshot that ends the transaction. The transaction read no WAL frame,
//! so its COMMIT must decide like any other: commit when the peer wrote other
//! pages, refuse once with BusySnapshot (and end the transaction) when the
//! peer wrote the same page. An fsqlite peer keeps committing.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use fsqlite::Connection;
use fsqlite_error::FrankenError;

const SQLITE_BUSY: i32 = 5;
const SQLITE_BUSY_SNAPSHOT: i32 = 517;
const HOLDER_ENV: &str = "FSQLITE_REFUSED_COMMIT_HOLDER";
const STOCK_HOLDER_ENV: &str = "FSQLITE_REFUSED_COMMIT_STOCK_HOLDER";
const PEER_ENV: &str = "FSQLITE_OPEN_TRANSACTION_PEER";

struct HolderGuard(Option<Child>);

impl HolderGuard {
    fn finish(mut self) -> std::process::Output {
        self.0
            .take()
            .expect("holder spawned")
            .wait_with_output()
            .expect("holder output")
    }
}

impl Drop for HolderGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            // A failing parent assertion must not leave a writer holding the
            // database until the child's handshake timeout expires.
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

/// `(id, s)` of every row of `t`, read by a fresh stock connection.
fn stock_rows(db: &Path) -> Vec<(i64, String)> {
    let stock = rusqlite::Connection::open(db).expect("stock open");
    let mut stmt = stock
        .prepare("SELECT id, s FROM t ORDER BY id")
        .expect("prepare");
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
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

async fn fsqlite_ids(conn: &Connection) -> Vec<i64> {
    conn.query("SELECT id FROM t ORDER BY id")
        .await
        .expect("select ids")
        .iter()
        .map(|row| match row.values()[0] {
            fsqlite::SqliteValue::Integer(id) => id,
            ref other => panic!("id is not an integer: {other:?}"),
        })
        .collect()
}

async fn scalar(conn: &Connection, sql: &str) -> i64 {
    match conn.query(sql).await.expect("scalar query")[0].values()[0] {
        fsqlite::SqliteValue::Integer(value) => value,
        ref other => panic!("{sql} did not return an integer: {other:?}"),
    }
}

/// What an automatically rolled-back transaction looks like in stock, from
/// its documented example: `INSERT OR ROLLBACK` hitting a duplicate key.
struct StockAutoRollback {
    autocommit_after_error: bool,
    rollback_error: String,
    next_begin_ok: bool,
    ids_after: Vec<i64>,
}

fn stock_auto_rollback(db: &Path) -> StockAutoRollback {
    let stock = rusqlite::Connection::open(db).expect("stock open");
    stock
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE t(id INTEGER PRIMARY KEY, w INTEGER NOT NULL, s TEXT NOT NULL);
             INSERT INTO t VALUES (1, 0, 'seed'), (2, 1, 'peer');",
        )
        .expect("stock seed");
    stock.execute_batch("BEGIN").expect("stock BEGIN");
    stock
        .execute("INSERT INTO t VALUES (3, 2, 'rolled back')", [])
        .expect("stock INSERT");
    let error = stock
        .execute("INSERT OR ROLLBACK INTO t VALUES (1, 9, 'dup')", [])
        .expect_err("a duplicate key under OR ROLLBACK fails");
    assert!(
        error.to_string().contains("UNIQUE constraint failed"),
        "stock OR ROLLBACK error: {error}"
    );
    let autocommit_after_error = stock.is_autocommit();
    let rollback_error = stock
        .execute_batch("ROLLBACK")
        .expect_err("stock ROLLBACK after an automatic rollback fails")
        .to_string();
    let next_begin_ok = stock.execute_batch("BEGIN").is_ok();
    stock.execute_batch("ROLLBACK").expect("stock ROLLBACK");
    let ids_after = stock
        .prepare("SELECT id FROM t ORDER BY id")
        .expect("prepare")
        .query_map([], |row| row.get(0))
        .expect("query")
        .collect::<rusqlite::Result<_>>()
        .expect("ids");
    StockAutoRollback {
        autocommit_after_error,
        rollback_error,
        next_begin_ok,
        ids_after,
    }
}

/// Two file-backed BEGIN CONCURRENT transactions in one process form a write
/// skew: `refused` reads `b` and writes `a`, `peer` reads `a` and writes `b`.
/// SSI refuses `refused`'s COMMIT (the pivot) with BusySnapshot; in one process
/// a stale same-page write is refused at the write itself, so the write skew is
/// the deterministic COMMIT-time refusal. The cross-process same-page shape is
/// `cross_process_explicit_writers_on_one_table_lose_only_refused_transactions`.
#[test]
fn busy_snapshot_commit_rolls_the_transaction_back_like_a_stock_automatic_rollback() {
    let dir = tempfile::tempdir().expect("tempdir");
    let stock = stock_auto_rollback(&dir.path().join("stock.db"));
    assert!(
        stock.autocommit_after_error,
        "stock: autocommit after OR ROLLBACK"
    );
    assert!(stock.next_begin_ok, "stock: BEGIN after OR ROLLBACK");
    assert_eq!(
        stock.ids_after,
        vec![1, 2],
        "stock: the transaction's row is gone"
    );

    let db = dir.path().join("frank.db");
    let path = db.to_str().expect("utf8 path").to_owned();
    asupersync::test_utils::run_test(|| async move {
        let peer = Connection::open(&path).await.expect("open peer");
        peer.execute("PRAGMA busy_timeout=1000")
            .await
            .expect("busy_timeout");
        for sql in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, w INTEGER NOT NULL, s TEXT NOT NULL)",
            "CREATE TABLE a(id INTEGER PRIMARY KEY, v INTEGER NOT NULL)",
            "CREATE TABLE b(id INTEGER PRIMARY KEY, v INTEGER NOT NULL)",
            "INSERT INTO t VALUES (1, 0, 'seed')",
            "INSERT INTO a VALUES (1, 10)",
            "INSERT INTO b VALUES (1, 20)",
        ] {
            peer.execute(sql).await.expect("seed");
        }
        let refused = Connection::open(&path).await.expect("open refused");
        refused
            .execute("PRAGMA busy_timeout=1000")
            .await
            .expect("busy_timeout");

        refused.execute("BEGIN").await.expect("BEGIN");
        peer.execute("BEGIN").await.expect("peer BEGIN");
        assert!(
            refused.is_concurrent_transaction() && peer.is_concurrent_transaction(),
            "plain BEGIN must promote to BEGIN CONCURRENT"
        );
        assert_eq!(scalar(&refused, "SELECT v FROM b WHERE id = 1").await, 20);
        assert_eq!(scalar(&peer, "SELECT v FROM a WHERE id = 1").await, 10);
        refused
            .execute("UPDATE a SET v = 11 WHERE id = 1")
            .await
            .expect("refused writes a");
        refused
            .execute("INSERT INTO t VALUES (2, 2, 'refused')")
            .await
            .expect("refused writes t");
        peer.execute("UPDATE b SET v = 21 WHERE id = 1")
            .await
            .expect("peer writes b");
        let error = refused
            .execute("COMMIT")
            .await
            .expect_err("the pivot of a write skew is refused at COMMIT");
        assert!(
            matches!(error, FrankenError::BusySnapshot { .. }),
            "COMMIT must report the snapshot conflict, got {error:?}"
        );
        assert_eq!(error.extended_error_code(), SQLITE_BUSY_SNAPSHOT);

        // The refusal rolled the transaction back, as stock's automatic
        // rollback does.
        assert!(
            !refused.in_transaction(),
            "a COMMIT refused with BusySnapshot must end its transaction (autocommit on)"
        );
        let rollback_error = refused
            .execute("ROLLBACK")
            .await
            .expect_err("nothing is left to roll back")
            .to_string();
        assert_eq!(
            rollback_error, stock.rollback_error,
            "ROLLBACK after the automatic rollback reports what stock reports"
        );
        assert_eq!(
            fsqlite_ids(&refused).await,
            vec![1],
            "the refused transaction's row is absent"
        );
        assert_eq!(
            scalar(&refused, "SELECT v FROM a WHERE id = 1").await,
            10,
            "the refused transaction's update is absent"
        );
        peer.execute("COMMIT")
            .await
            .expect("the peer commits once the pivot is gone");

        // The next transaction works, sees the peer, and nothing is lost.
        refused
            .execute("BEGIN")
            .await
            .expect("BEGIN after a refused COMMIT");
        assert_eq!(scalar(&refused, "SELECT v FROM b WHERE id = 1").await, 21);
        refused
            .execute("UPDATE a SET v = 11 WHERE id = 1")
            .await
            .expect("retried UPDATE");
        refused
            .execute("INSERT INTO t VALUES (2, 2, 'retried')")
            .await
            .expect("retried INSERT");
        refused.execute("COMMIT").await.expect("retried COMMIT");
        refused
            .execute("BEGIN")
            .await
            .expect("BEGIN after the retried COMMIT");
        refused
            .execute("INSERT INTO t VALUES (3, 2, 'later')")
            .await
            .expect("later INSERT");
        refused.execute("COMMIT").await.expect("later COMMIT");
        assert_eq!(fsqlite_ids(&peer).await, vec![1, 2, 3]);
        drop(refused);
        drop(peer);
    });
    assert_eq!(
        stock_rows(&db),
        vec![
            (1, "seed".to_owned()),
            (2, "retried".to_owned()),
            (3, "later".to_owned()),
        ],
        "a fresh stock connection sees every committed row and not the refused one"
    );
    let stock = rusqlite::Connection::open(&db).expect("stock open");
    let a_b: (i64, i64) = stock
        .query_row(
            "SELECT (SELECT v FROM a WHERE id = 1), (SELECT v FROM b WHERE id = 1)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("a and b");
    assert_eq!(
        a_b,
        (11, 21),
        "the peer's and the retried writes are durable"
    );
    drop(stock);
    assert_eq!(stock_integrity(&db), "ok");
}

/// The holder process for the negative case: an fsqlite connection takes the
/// write lock with `BEGIN IMMEDIATE` (an explicit opt-out of the concurrent
/// promotion) plus an UPDATE of a row on an existing page of table `h`,
/// reports `held`, waits for `release`, commits, reports `released`, and
/// closes once the parent reports `done`. The stock variant exercises the
/// same lock lifecycle with an independently implemented WAL writer.
#[test]
fn refused_commit_holder_child() {
    let Ok(spec) = std::env::var(HOLDER_ENV) else {
        return;
    };
    let (db, dir) = spec.split_once('|').expect("holder spec");
    let (db, dir) = (db.to_owned(), PathBuf::from(dir));
    if std::env::var(STOCK_HOLDER_ENV).as_deref() == Ok("1") {
        let holder = rusqlite::Connection::open(&db).expect("stock holder open");
        holder
            .execute_batch("BEGIN IMMEDIATE; UPDATE h SET v = v + 1 WHERE id = 1;")
            .expect("stock holder takes WRITE and updates h");
        std::fs::write(dir.join("held"), b"").expect("held");
        assert!(
            wait_for(&dir.join("release"), Duration::from_secs(120)),
            "stock holder never released"
        );
        holder.execute_batch("COMMIT").expect("stock holder COMMIT");
        std::fs::write(dir.join("released"), b"").expect("released");
        assert!(
            wait_for(&dir.join("done"), Duration::from_secs(120)),
            "stock holder parent never finished"
        );
        return;
    }
    asupersync::test_utils::run_test(|| async move {
        let holder = Connection::open(&db).await.expect("holder open");
        holder
            .execute("BEGIN IMMEDIATE")
            .await
            .expect("holder BEGIN IMMEDIATE");
        assert!(
            !holder.is_concurrent_transaction(),
            "BEGIN IMMEDIATE is not promoted"
        );
        holder
            .execute("UPDATE h SET v = v + 1 WHERE id = 1")
            .await
            .expect("holder UPDATE");
        std::fs::write(dir.join("held"), b"").expect("held");
        assert!(
            wait_for(&dir.join("release"), Duration::from_secs(120)),
            "holder never released"
        );
        holder.execute("COMMIT").await.expect("holder COMMIT");
        std::fs::write(dir.join("released"), b"").expect("released");
        assert!(
            wait_for(&dir.join("done"), Duration::from_secs(120)),
            "parent never finished"
        );
        holder.close().await.expect("holder close");
    });
}

fn spawn_holder(db: &Path, dir: &Path, stock: bool) -> HolderGuard {
    let child = Command::new(std::env::current_exe().expect("test binary path"))
        .args([
            "--exact",
            "refused_commit_holder_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(HOLDER_ENV, format!("{}|{}", db.display(), dir.display()))
        .env(STOCK_HOLDER_ENV, if stock { "1" } else { "0" })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn holder");
    HolderGuard(Some(child))
}

/// Negative case: a COMMIT that gets a retryable `Busy` (another process holds
/// the WAL write lock past busy_timeout) must keep its transaction, and a
/// second COMMIT commits it once the holder is done. An implementation that
/// ends the transaction on every COMMIT error fails here.
#[test]
fn retryable_busy_at_commit_keeps_the_transaction_open() {
    if std::env::var_os(HOLDER_ENV).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("busy.db");
    rusqlite::Connection::open(&db)
        .expect("seed open")
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE t(id INTEGER PRIMARY KEY, w INTEGER NOT NULL, s TEXT NOT NULL);
             CREATE TABLE h(id INTEGER PRIMARY KEY, v INTEGER NOT NULL);
             INSERT INTO t VALUES (1, 0, 'seed');
             INSERT INTO h VALUES (1, 0);",
        )
        .expect("seed");
    let path = db.to_str().expect("utf8 path").to_owned();
    let work = dir.path().to_path_buf();
    let mut holder = None;
    let (holder_slot, holder_db) = (&mut holder, db.clone());
    asupersync::test_utils::run_test(|| async move {
        // Open before the holder takes the write lock, so the open is not
        // part of what this case measures.
        let conn = Connection::open(&path).await.expect("open");
        conn.execute("PRAGMA busy_timeout=200")
            .await
            .expect("busy_timeout");
        *holder_slot = Some(spawn_holder(&holder_db, &work, false));
        assert!(
            wait_for(&work.join("held"), Duration::from_secs(120)),
            "holder never took the write lock"
        );
        conn.execute("BEGIN").await.expect("BEGIN");
        conn.execute("INSERT INTO t VALUES (2, 1, 'waited')")
            .await
            .expect("INSERT while the holder writes");
        let error = conn
            .execute("COMMIT")
            .await
            .expect_err("COMMIT behind a held write lock is refused with Busy");
        assert!(
            matches!(error, FrankenError::Busy),
            "the refusal must be the retryable Busy, got {error:?}"
        );
        assert_eq!(error.extended_error_code(), SQLITE_BUSY);
        assert!(
            conn.in_transaction(),
            "a retryable Busy at COMMIT must leave the transaction open"
        );
        std::fs::write(work.join("release"), b"").expect("release");
        assert!(
            wait_for(&work.join("released"), Duration::from_secs(120)),
            "holder never committed"
        );
        conn.execute("COMMIT")
            .await
            .expect("the second COMMIT succeeds once the holder is done");
        assert!(!conn.in_transaction());
        assert_eq!(fsqlite_ids(&conn).await, vec![1, 2]);
        conn.close().await.expect("close");
        std::fs::write(work.join("done"), b"").expect("done");
    });
    let output = holder.expect("holder spawned").finish();
    assert!(
        output.status.success(),
        "holder failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        stock_rows(&db),
        vec![(1, "seed".to_owned()), (2, "waited".to_owned())]
    );
    let h: i64 = rusqlite::Connection::open(&db)
        .expect("verify open")
        .query_row("SELECT v FROM h WHERE id = 1", [], |row| row.get(0))
        .expect("holder row");
    assert_eq!(h, 1, "the holder's commit is durable");
    assert_eq!(stock_integrity(&db), "ok");
}

/// What a CLI-style writer (`BEGIN; INSERT; COMMIT` per row, every statement
/// run, nothing rolled back) sees around a COMMIT that fails with BUSY.
#[derive(Debug, PartialEq, Eq)]
struct CliAfterBusyCommit {
    /// Extended code of the failed COMMIT.
    commit_code: i32,
    /// Still inside the transaction after it.
    open_after_busy: bool,
    /// The next row's BEGIN error.
    next_begin_error: Option<String>,
    /// The next row's INSERT and COMMIT succeeded.
    next_insert_ok: bool,
    next_commit_ok: bool,
    /// `(id, s)` of the durable rows at the end.
    durable: Vec<(i64, String)>,
}

/// Stock's answer for the CLI sequence after a COMMIT BUSY: "If COMMIT fails
/// with SQLITE_BUSY, the transaction remains active", so the next BEGIN fails
/// "within a transaction", the next INSERT joins the open transaction, and
/// the next COMMIT makes both rows durable. A stock WAL writer takes WRITE at
/// its INSERT and never waits at COMMIT, so stock's COMMIT BUSY is shown in
/// rollback-journal mode: a reader's SHARED lock keeps the writer from the
/// EXCLUSIVE lock its COMMIT needs.
fn stock_cli_after_busy_commit(db: &Path) -> CliAfterBusyCommit {
    let writer = rusqlite::Connection::open(db).expect("stock writer open");
    writer
        .execute_batch(
            "PRAGMA journal_mode=DELETE;
             CREATE TABLE t(id INTEGER PRIMARY KEY, w INTEGER NOT NULL, s TEXT NOT NULL);
             INSERT INTO t VALUES (1, 0, 'seed');",
        )
        .expect("stock seed");
    writer
        .busy_timeout(Duration::from_millis(200))
        .expect("stock busy_timeout");
    let reader = rusqlite::Connection::open(db).expect("stock reader open");
    reader.execute_batch("BEGIN").expect("reader BEGIN");
    let seen: i64 = reader
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .expect("reader takes SHARED");
    assert_eq!(seen, 1);
    writer.execute_batch("BEGIN").expect("stock BEGIN");
    writer
        .execute("INSERT INTO t VALUES (2, 1, 'busy')", [])
        .expect("stock INSERT");
    let commit_code = match writer.execute_batch("COMMIT") {
        Err(rusqlite::Error::SqliteFailure(error, _)) => error.extended_code,
        other => panic!("stock COMMIT behind a SHARED reader must fail with BUSY, got {other:?}"),
    };
    let open_after_busy = !writer.is_autocommit();
    let next_begin_error = writer
        .execute_batch("BEGIN")
        .err()
        .map(|error| error.to_string());
    let next_insert_ok = writer
        .execute("INSERT INTO t VALUES (3, 1, 'next')", [])
        .is_ok();
    reader.execute_batch("COMMIT").expect("reader ends");
    let next_commit_ok = writer.execute_batch("COMMIT").is_ok();
    drop(reader);
    drop(writer);
    CliAfterBusyCommit {
        commit_code,
        open_after_busy,
        next_begin_error,
        next_insert_ok,
        next_commit_ok,
        durable: stock_rows(db),
    }
}

/// bd-igdof, decision (b1): a COMMIT that waits out busy_timeout behind a
/// STOCK process holding the write lock (BEGIN IMMEDIATE) returns the
/// retryable BUSY and writes nothing: stock's durable rows and the WAL are
/// unchanged right after it, and the transaction stays open. Run CLI-style,
/// the next row's BEGIN fails "within a transaction", its INSERT joins the
/// open transaction, and once the holder commits, the next COMMIT makes both
/// rows durable exactly once: what stock does (`stock_cli_after_busy_commit`,
/// compared field by field). An implementation that ends the transaction on
/// a BUSY COMMIT (decision (b2)) fails here: the BEGIN succeeds and the BUSY
/// row never lands.
///
/// Negative: a BusySnapshot refusal is not a BUSY. A stock peer commits to the
/// same page while the transaction is open; the COMMIT is refused with 517,
/// the transaction ends, its row never lands, and the next BEGIN works.
#[test]
fn a_busy_commit_behind_a_stock_writer_stays_open_and_a_later_commit_lands_its_row_once() {
    if std::env::var_os(HOLDER_ENV).is_some() || std::env::var_os(PEER_ENV).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let stock = stock_cli_after_busy_commit(&dir.path().join("stock.db"));
    assert_eq!(
        (stock.commit_code, stock.open_after_busy),
        (SQLITE_BUSY, true),
        "stock: COMMIT BUSY keeps the transaction"
    );
    assert_eq!(
        stock.durable,
        vec![
            (1, "seed".to_owned()),
            (2, "busy".to_owned()),
            (3, "next".to_owned())
        ],
        "stock: the next COMMIT lands both rows"
    );

    let db = dir.path().join("busy.db");
    rusqlite::Connection::open(&db)
        .expect("seed open")
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE t(id INTEGER PRIMARY KEY, w INTEGER NOT NULL, s TEXT NOT NULL);
             CREATE TABLE h(id INTEGER PRIMARY KEY, v INTEGER NOT NULL);
             INSERT INTO t VALUES (1, 0, 'seed');
             INSERT INTO h VALUES (1, 0);",
        )
        .expect("seed");
    let path = db.to_str().expect("utf8 path").to_owned();
    let work = dir.path().to_path_buf();
    let mut holder = None;
    let mut observed = None;
    let (holder_slot, observed_slot, holder_db) = (&mut holder, &mut observed, db.clone());
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(&path).await.expect("open");
        conn.execute("PRAGMA busy_timeout=200")
            .await
            .expect("busy_timeout");
        *holder_slot = Some(spawn_holder(&holder_db, &work, true));
        assert!(
            wait_for(&work.join("held"), Duration::from_secs(120)),
            "the stock holder never took the write lock"
        );
        conn.execute("BEGIN").await.expect("BEGIN");
        conn.execute("INSERT INTO t VALUES (2, 1, 'busy')")
            .await
            .expect("INSERT while stock holds WRITE");
        let wal_before = std::fs::metadata(wal_path(&holder_db)).map_or(0, |meta| meta.len());
        let error = conn
            .execute("COMMIT")
            .await
            .expect_err("COMMIT behind stock's write lock returns BUSY");
        assert!(
            matches!(error, FrankenError::Busy),
            "a COMMIT that waited out busy_timeout is the retryable Busy, got {error:?}"
        );
        let commit_code = error.extended_error_code();
        let open_after_busy = conn.in_transaction();
        assert_eq!(
            stock_rows(&holder_db),
            vec![(1, "seed".to_owned())],
            "the BUSY COMMIT made nothing durable"
        );
        assert_eq!(
            std::fs::metadata(wal_path(&holder_db)).map_or(0, |meta| meta.len()),
            wal_before,
            "the BUSY COMMIT appended no frame"
        );
        let next_begin_error = conn
            .execute("BEGIN")
            .await
            .err()
            .map(|error| error.to_string());
        let next_insert_ok = conn
            .execute("INSERT INTO t VALUES (3, 1, 'next')")
            .await
            .is_ok();
        std::fs::write(work.join("release"), b"").expect("release");
        assert!(
            wait_for(&work.join("released"), Duration::from_secs(120)),
            "the stock holder never committed"
        );
        let next_commit_ok = conn.execute("COMMIT").await.is_ok();
        assert!(!conn.in_transaction());
        conn.close().await.expect("close");
        std::fs::write(work.join("done"), b"").expect("done");
        *observed_slot = Some(CliAfterBusyCommit {
            commit_code,
            open_after_busy,
            next_begin_error,
            next_insert_ok,
            next_commit_ok,
            durable: stock_rows(&holder_db),
        });
    });
    let output = holder.expect("holder spawned").finish();
    assert!(
        output.status.success(),
        "stock holder failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let frank = observed.expect("the fsqlite arm ran");
    assert_eq!(
        frank, stock,
        "fsqlite answers the CLI sequence after a BUSY COMMIT as stock does"
    );
    let h: i64 = rusqlite::Connection::open(&db)
        .expect("verify open")
        .query_row("SELECT v FROM h WHERE id = 1", [], |row| row.get(0))
        .expect("holder row");
    assert_eq!(h, 1, "the stock holder's commit is durable");
    assert_eq!(stock_integrity(&db), "ok");

    // Negative: a same-page stock commit while the transaction is open. The
    // transaction reads `count(*)` after its INSERT, and the peer's row changes
    // that count, so the refused COMMIT cannot be replayed (bd-q4o7z): without
    // the read it would be replayed and commit.
    let neg = dir.path().join("refused.db");
    rusqlite::Connection::open(&neg)
        .expect("seed open")
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE t(id INTEGER PRIMARY KEY, w INTEGER NOT NULL, s TEXT NOT NULL);
             INSERT INTO t VALUES (1, 0, 'seed');",
        )
        .expect("seed");
    let neg_dir = dir.path().join("neg");
    std::fs::create_dir(&neg_dir).expect("negative dir");
    let neg_path = neg.to_str().expect("utf8 path").to_owned();
    let spec = format!(
        "stock|{}|{}|INSERT INTO t VALUES (10, 9, 'peer')",
        neg.display(),
        neg_dir.display()
    );
    let mut peer = None;
    let peer_slot = &mut peer;
    let neg_db = neg.clone();
    asupersync::test_utils::run_test(|| async move {
        let _release = ReleasePeer(neg_dir.clone());
        let conn = Connection::open(&neg_path).await.expect("open");
        conn.execute("PRAGMA busy_timeout=1000")
            .await
            .expect("busy_timeout");
        conn.execute("BEGIN").await.expect("BEGIN");
        conn.execute("INSERT INTO t VALUES (2, 1, 'refused')")
            .await
            .expect("INSERT");
        assert_eq!(
            scalar(&conn, "SELECT count(*) FROM t").await,
            2,
            "the transaction sees its own row"
        );
        let child = Command::new(std::env::current_exe().expect("test binary path"))
            .args([
                "--exact",
                "open_transaction_peer_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(PEER_ENV, &spec)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn stock peer");
        *peer_slot = Some(HolderGuard(Some(child)));
        assert!(
            wait_for(&neg_dir.join("committed"), Duration::from_secs(120)),
            "the stock peer never committed"
        );
        let error = conn
            .execute("COMMIT")
            .await
            .expect_err("a same-page stock commit refuses the stale page");
        assert!(
            matches!(error, FrankenError::BusySnapshot { .. }),
            "the refusal is the snapshot conflict, not BUSY: {error:?}"
        );
        assert_eq!(error.extended_error_code(), SQLITE_BUSY_SNAPSHOT);
        assert!(
            !conn.in_transaction(),
            "a BusySnapshot refusal ends the transaction"
        );
        assert_eq!(
            stock_rows(&neg_db),
            vec![(1, "seed".to_owned()), (10, "peer".to_owned())],
            "the refused row is not durable"
        );
        conn.execute("BEGIN")
            .await
            .expect("the next BEGIN works: no transaction is left open");
        conn.execute("INSERT INTO t VALUES (3, 1, 'next')")
            .await
            .expect("next INSERT");
        conn.execute("COMMIT").await.expect("next COMMIT");
        conn.close().await.expect("close");
    });
    let output = peer.expect("peer spawned").finish();
    assert!(
        output.status.success(),
        "stock peer failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        stock_rows(&neg),
        vec![
            (1, "seed".to_owned()),
            (3, "next".to_owned()),
            (10, "peer".to_owned())
        ],
        "the refused row never lands; the next transaction's does"
    );
    assert_eq!(stock_integrity(&neg), "ok");
}

/// bd-reality-check-1022-llk6g.5.5: a stock writer can restart an empty,
/// fully checkpointed WAL while a concurrent transaction pins only the main
/// database. That transaction must either commit safely or receive a terminal
/// snapshot refusal. Retrying recovery against its obsolete generation can
/// never make progress.
#[test]
fn stock_peer_commit_does_not_leave_a_retryable_recovery_loop() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("stock-peer.db");
    rusqlite::Connection::open(&db)
        .expect("seed open")
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE t(id INTEGER PRIMARY KEY, w INTEGER NOT NULL, s TEXT NOT NULL);
             CREATE TABLE h(id INTEGER PRIMARY KEY, v INTEGER NOT NULL);
             INSERT INTO t VALUES (1, 0, 'seed');
             INSERT INTO h VALUES (1, 0);",
        )
        .expect("seed");
    let path = db.to_str().expect("utf8 path").to_owned();
    let work = dir.path().to_path_buf();
    let peer_db = db.clone();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(&path).await.expect("open");
        conn.execute("PRAGMA busy_timeout=100")
            .await
            .expect("busy_timeout");
        conn.execute("BEGIN").await.expect("BEGIN");
        assert!(conn.is_concurrent_transaction());
        conn.execute("INSERT INTO t VALUES (2, 1, 'retried')")
            .await
            .expect("stage concurrent INSERT before stock writes");

        let holder = spawn_holder(&peer_db, &work, true);
        assert!(
            wait_for(&work.join("held"), Duration::from_secs(20)),
            "stock holder never took WRITE"
        );
        let blocked = conn.execute("COMMIT").await.expect_err("WRITE is held");
        assert!(matches!(blocked, FrankenError::Busy), "{blocked:?}");
        assert!(conn.in_transaction(), "lock contention remains retryable");
        std::fs::write(work.join("release"), b"").expect("release stock");
        assert!(
            wait_for(&work.join("released"), Duration::from_secs(20)),
            "stock holder never committed"
        );

        let started = Instant::now();
        match conn.execute("COMMIT").await {
            Ok(_) => assert!(!conn.in_transaction()),
            Err(error @ FrankenError::BusySnapshot { .. }) => {
                assert_eq!(error.extended_error_code(), SQLITE_BUSY_SNAPSHOT);
                assert!(!conn.in_transaction(), "obsolete transaction must end");
                assert_eq!(fsqlite_ids(&conn).await, vec![1]);
                conn.execute("BEGIN").await.expect("fresh BEGIN");
                conn.execute("INSERT INTO t VALUES (2, 1, 'retried')")
                    .await
                    .expect("retry INSERT on a fresh snapshot");
                conn.execute("COMMIT").await.expect("fresh COMMIT");
            }
            Err(error) => panic!("stock committed; recovery must not loop forever: {error:?}"),
        }
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(fsqlite_ids(&conn).await, vec![1, 2]);
        assert_eq!(scalar(&conn, "SELECT v FROM h WHERE id = 1").await, 1);
        conn.close().await.expect("close");
        std::fs::write(work.join("done"), b"").expect("done");
        let output = holder.finish();
        assert!(
            output.status.success(),
            "stock holder failed: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    });
    assert_eq!(
        stock_rows(&db),
        vec![(1, "seed".to_owned()), (2, "retried".to_owned())]
    );
    assert_eq!(stock_integrity(&db), "ok");
}

fn wal_path(db: &Path) -> PathBuf {
    PathBuf::from(format!("{}-wal", db.display()))
}

/// The WAL header's salts (bytes 16..24); `None` while there is no header.
fn wal_salts(db: &Path) -> Option<[u8; 8]> {
    let bytes = std::fs::read(wal_path(db)).ok()?;
    bytes
        .get(16..24)
        .map(|salts| salts.try_into().expect("eight salt bytes"))
}

fn stock_h(db: &Path) -> i64 {
    rusqlite::Connection::open(db)
        .expect("stock open")
        .query_row("SELECT v FROM h WHERE id = 1", [], |row| row.get(0))
        .expect("h row")
}

/// The peer process for the open-transaction cases: one write transaction,
/// `BEGIN IMMEDIATE; <sql>; COMMIT`, through stock SQLite (rusqlite) or
/// fsqlite, committed while the parent's transaction is open. It reports
/// `committed` and keeps its connection open until the parent reports `done`,
/// so its close cannot checkpoint or reset the WAL under the parent's COMMIT.
#[test]
fn open_transaction_peer_child() {
    let Ok(spec) = std::env::var(PEER_ENV) else {
        return;
    };
    let mut parts = spec.splitn(4, '|');
    let mut field = |name: &str| {
        parts
            .next()
            .unwrap_or_else(|| panic!("peer spec lacks {name}"))
            .to_owned()
    };
    let (engine, db, dir, sql) = (field("engine"), field("db"), field("dir"), field("sql"));
    let dir = PathBuf::from(dir);
    match engine.as_str() {
        "stock" => {
            let peer = rusqlite::Connection::open(&db).expect("stock peer open");
            peer.busy_timeout(Duration::from_secs(5))
                .expect("stock busy_timeout");
            peer.execute_batch(&format!("BEGIN IMMEDIATE; {sql}; COMMIT;"))
                .expect("stock peer commit");
            std::fs::write(dir.join("committed"), b"").expect("committed");
            assert!(
                wait_for(&dir.join("done"), Duration::from_secs(120)),
                "parent never finished"
            );
            drop(peer);
        }
        "fsqlite" => asupersync::test_utils::run_test(|| async move {
            let peer = Connection::open(&db).await.expect("fsqlite peer open");
            peer.execute("PRAGMA busy_timeout=5000")
                .await
                .expect("fsqlite busy_timeout");
            peer.execute("BEGIN IMMEDIATE")
                .await
                .expect("fsqlite peer BEGIN IMMEDIATE");
            peer.execute(&sql).await.expect("fsqlite peer write");
            peer.execute("COMMIT").await.expect("fsqlite peer COMMIT");
            std::fs::write(dir.join("committed"), b"").expect("committed");
            assert!(
                wait_for(&dir.join("done"), Duration::from_secs(120)),
                "parent never finished"
            );
            peer.close().await.expect("fsqlite peer close");
        }),
        other => panic!("unknown peer engine {other}"),
    }
}

/// Tells the peer to close however the parent's run ends, so a failed run
/// does not leave it waiting.
struct ReleasePeer(PathBuf);

impl Drop for ReleasePeer {
    fn drop(&mut self) {
        let _ = std::fs::write(self.0.join("done"), b"");
    }
}

struct OpenAcrossPeerCommit {
    _dir: tempfile::TempDir,
    db: PathBuf,
    /// The WAL header salts just before and just after the peer's commit.
    wal_salts: (Option<[u8; 8]>, Option<[u8; 8]>),
    commit: Result<(), FrankenError>,
    open_after_commit: bool,
    /// What a second COMMIT reported, when the first was refused.
    second_commit: Option<String>,
    fsqlite_ids: Vec<i64>,
    /// What each step of the transaction returned to the caller, in order.
    observed: Vec<String>,
    /// `last_insert_rowid()` after the COMMIT.
    rowid_after_commit: i64,
    /// How many times the application function `app_counter()` ran.
    app_calls: usize,
}

/// One statement or API read of the open transaction, before the peer commits.
#[derive(Clone, Copy)]
enum Step {
    Execute(&'static str),
    Query(&'static str),
    /// `query_with_params_for_each`, counting the streamed rows.
    Stream(&'static str),
    /// `Connection::last_insert_rowid()`.
    ReadRowid,
}

/// The transaction most cases run: one INSERT with an explicit id, which
/// returns nothing but its count.
const INSERT_ID_2: &[Step] = &[Step::Execute("INSERT INTO t VALUES (2, 1, 'fsqlite')")];

/// bd-q4o7z: an application function with a call counter.
struct AppCounter(std::sync::Arc<std::sync::atomic::AtomicUsize>);

impl fsqlite_func::ScalarFunction for AppCounter {
    fn invoke(
        &self,
        _args: &[fsqlite::SqliteValue],
    ) -> fsqlite_error::Result<fsqlite::SqliteValue> {
        let calls = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        Ok(fsqlite::SqliteValue::Integer(
            i64::try_from(calls).expect("call count"),
        ))
    }

    fn num_args(&self) -> i32 {
        0
    }

    fn name(&self) -> &str {
        "app_counter"
    }
}

/// An fsqlite connection opens a WAL database that has no `-wal` (so it
/// creates one with no frames) and runs `BEGIN` plus `steps`. A peer process
/// (`engine`) then commits `peer_sql`; the parent's COMMIT follows. With
/// `app_function`, the connection registers `app_counter()` before BEGIN.
fn open_transaction_across_peer_commit(
    engine: &str,
    peer_sql: &str,
    steps: &'static [Step],
    app_function: bool,
) -> OpenAcrossPeerCommit {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("open.db");
    rusqlite::Connection::open(&db)
        .expect("seed open")
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE t(id INTEGER PRIMARY KEY, w INTEGER NOT NULL, s TEXT NOT NULL);
             CREATE TABLE h(id INTEGER PRIMARY KEY, v INTEGER NOT NULL);
             INSERT INTO t VALUES (1, 0, 'seed');
             INSERT INTO h VALUES (1, 0);",
        )
        .expect("seed");
    assert!(
        !wal_path(&db).exists(),
        "the seed's last close checkpoints and removes its WAL"
    );
    let path = db.to_str().expect("utf8 path").to_owned();
    let work = dir.path().to_path_buf();
    let spec = format!("{engine}|{}|{}|{peer_sql}", db.display(), work.display());
    let mut peer = None;
    let mut observed = None;
    let (peer_slot, observed_slot) = (&mut peer, &mut observed);
    let db_for_salts = db.clone();
    let app_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let app_calls_in = std::sync::Arc::clone(&app_calls);
    asupersync::test_utils::run_test(|| async move {
        let _release = ReleasePeer(work.clone());
        let conn = Connection::open(&path).await.expect("open");
        conn.execute("PRAGMA busy_timeout=1000")
            .await
            .expect("busy_timeout");
        if app_function {
            conn.register_nondeterministic_scalar_function(AppCounter(app_calls_in));
        }
        conn.execute("BEGIN").await.expect("BEGIN");
        let mut observed = Vec::new();
        for step in steps {
            observed.push(match *step {
                Step::Execute(sql) => format!(
                    "{:?}",
                    conn.execute(sql)
                        .await
                        .unwrap_or_else(|error| panic!("{sql} before the peer commits: {error}"))
                ),
                Step::Query(sql) => format!(
                    "{:?}",
                    conn.query(sql)
                        .await
                        .unwrap_or_else(|error| panic!("{sql} before the peer commits: {error}"))
                        .iter()
                        .map(|row| row.values().to_vec())
                        .collect::<Vec<_>>()
                ),
                Step::Stream(sql) => {
                    let mut streamed = 0_usize;
                    conn.query_with_params_for_each(sql, &[], |_| {
                        streamed += 1;
                        Ok(())
                    })
                    .await
                    .unwrap_or_else(|error| panic!("{sql} before the peer commits: {error}"));
                    format!("streamed {streamed}")
                }
                Step::ReadRowid => format!("rowid {}", conn.last_insert_rowid()),
            });
        }
        let before = wal_salts(&db_for_salts);
        let child = peer_slot.insert(
            Command::new(std::env::current_exe().expect("test binary path"))
                .args([
                    "--exact",
                    "open_transaction_peer_child",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(PEER_ENV, &spec)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("spawn peer"),
        );
        let started = Instant::now();
        while !work.join("committed").exists() {
            assert!(
                child.try_wait().expect("peer status").is_none(),
                "the peer exited before committing"
            );
            assert!(
                started.elapsed() < Duration::from_secs(120),
                "the peer never committed"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        let after = wal_salts(&db_for_salts);
        let commit = conn.execute("COMMIT").await.map(|_| ());
        let rowid_after_commit = conn.last_insert_rowid();
        let open_after_commit = conn.in_transaction();
        let second_commit = if commit.is_err() {
            conn.execute("COMMIT")
                .await
                .err()
                .map(|error| error.to_string())
        } else {
            None
        };
        if conn.in_transaction() {
            conn.execute("ROLLBACK")
                .await
                .expect("ROLLBACK the transaction the refusal left open");
        }
        let ids = fsqlite_ids(&conn).await;
        conn.close().await.expect("close");
        *observed_slot = Some((
            (before, after),
            commit,
            open_after_commit,
            second_commit,
            ids,
            observed,
            rowid_after_commit,
        ));
    });
    let (
        wal_salts,
        commit,
        open_after_commit,
        second_commit,
        fsqlite_ids,
        observed,
        rowid_after_commit,
    ) = observed.expect("the parent ran to completion");
    let output = peer
        .expect("peer spawned")
        .wait_with_output()
        .expect("peer output");
    assert!(
        output.status.success(),
        "peer failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    OpenAcrossPeerCommit {
        _dir: dir,
        db,
        wal_salts,
        commit,
        open_after_commit,
        second_commit,
        fsqlite_ids,
        observed,
        rowid_after_commit,
        app_calls: app_calls.load(std::sync::atomic::Ordering::SeqCst),
    }
}

/// The stock peer's commit is what started a new WAL generation in place:
/// without it these cases would not reach the defect.
fn assert_generation_started_in_place(run: &OpenAcrossPeerCommit) {
    let (before, after) = run.wal_salts;
    assert!(
        before.is_some() && after.is_some() && before != after,
        "shape: the fsqlite connection's frame-less WAL got a new header from the stock \
         commit: {before:?} -> {after:?}"
    );
}

/// The stock peer wrote another table: the open transaction read no WAL
/// frame and wrote no page the peer wrote, so its COMMIT commits, at once.
/// 1e0b54d7d answered BusySnapshot here and ended the transaction.
#[test]
fn transaction_open_across_a_stock_commit_on_another_table_commits() {
    if std::env::var_os(PEER_ENV).is_some() || std::env::var_os(HOLDER_ENV).is_some() {
        return;
    }
    let run = open_transaction_across_peer_commit(
        "stock",
        "UPDATE h SET v = v + 1 WHERE id = 1",
        INSERT_ID_2,
        false,
    );
    assert_generation_started_in_place(&run);
    if let Err(error) = &run.commit {
        panic!("COMMIT after a stock commit on another table must commit, got {error:?}");
    }
    assert!(!run.open_after_commit);
    assert_eq!(run.fsqlite_ids, vec![1, 2]);
    assert_eq!(
        stock_rows(&run.db),
        vec![(1, "seed".to_owned()), (2, "fsqlite".to_owned())],
        "a fresh stock connection sees the committed row"
    );
    assert_eq!(stock_h(&run.db), 1, "the stock peer's commit is durable");
    assert_eq!(stock_integrity(&run.db), "ok");
}

/// Negative case: the stock peer wrote the same leaf page of `t`. The
/// generation change must not let the stale page through: COMMIT is refused
/// once with BusySnapshot, the transaction ends (.5.1), and the peer's row is
/// the one that survives. A fix that rebinds without the page check fails
/// here, and so does one whose page check reads the image through the
/// transaction's pinned snapshot (observed: the COMMIT committed over the
/// peer's row).
///
/// bd-q4o7z: the transaction also reads `count(*)` after its INSERT, and the
/// peer's row changes that count, so a replay would show the caller a
/// different answer and the refusal stands. Without that read the refused
/// COMMIT is replayed (the next test).
#[test]
fn transaction_open_across_a_stock_commit_on_the_same_page_is_refused_once() {
    if std::env::var_os(PEER_ENV).is_some() || std::env::var_os(HOLDER_ENV).is_some() {
        return;
    }
    let run = open_transaction_across_peer_commit(
        "stock",
        "INSERT INTO t VALUES (3, 2, 'stock')",
        &[
            Step::Execute("INSERT INTO t VALUES (2, 1, 'fsqlite')"),
            Step::Query("SELECT count(*) FROM t"),
        ],
        false,
    );
    assert_eq!(
        run.observed,
        ["1", "[[Integer(2)]]"],
        "shape: what the caller saw"
    );
    assert_generation_started_in_place(&run);
    let error = match &run.commit {
        Ok(()) => panic!("a same-page stock commit must refuse the stale page"),
        Err(error) => error,
    };
    assert!(
        matches!(error, FrankenError::BusySnapshot { .. }),
        "the refusal must be the snapshot conflict, got {error:?}"
    );
    assert_eq!(error.extended_error_code(), SQLITE_BUSY_SNAPSHOT);
    assert!(
        !run.open_after_commit,
        "a COMMIT refused with BusySnapshot ends its transaction"
    );
    let second = run.second_commit.as_deref().unwrap_or("");
    assert!(
        second.contains("no transaction is active"),
        "the refusal is reported once; a second COMMIT finds no transaction, got {second:?}"
    );
    assert_eq!(run.fsqlite_ids, vec![1, 3]);
    assert_eq!(
        stock_rows(&run.db),
        vec![(1, "seed".to_owned()), (3, "stock".to_owned())],
        "the peer's row survives and the refused row is absent"
    );
    assert_eq!(stock_h(&run.db), 0);
    assert_eq!(stock_integrity(&run.db), "ok");
}

/// `(id, w)` of every row of `t`, read by stock.
fn stock_ws(db: &Path) -> Vec<(i64, i64)> {
    let stock = rusqlite::Connection::open(db).expect("stock open");
    let mut stmt = stock
        .prepare("SELECT id, w FROM t ORDER BY id")
        .expect("prepare");
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .expect("query")
        .collect::<rusqlite::Result<_>>()
        .expect("rows")
}

/// The refusal a bd-q4o7z negative must end with: SQLITE_BUSY_SNAPSHOT once,
/// the transaction ended, and the peer's rows the only new ones.
fn assert_refused_once(label: &str, run: &OpenAcrossPeerCommit, stock_rows_after: &[(i64, &str)]) {
    let error = match &run.commit {
        Ok(()) => {
            panic!("{label}: a replay would change what the caller saw, so COMMIT must refuse")
        }
        Err(error) => error,
    };
    assert!(
        matches!(error, FrankenError::BusySnapshot { .. }),
        "{label}: the refusal must be the snapshot conflict, got {error:?}"
    );
    assert_eq!(error.extended_error_code(), SQLITE_BUSY_SNAPSHOT, "{label}");
    assert!(
        !run.open_after_commit,
        "{label}: the refusal ends the transaction"
    );
    let second = run.second_commit.as_deref().unwrap_or("");
    assert!(
        second.contains("no transaction is active"),
        "{label}: the refusal is reported once, got {second:?}"
    );
    let expected: Vec<(i64, String)> = stock_rows_after
        .iter()
        .map(|(id, s)| (*id, (*s).to_owned()))
        .collect();
    assert_eq!(
        stock_rows(&run.db),
        expected,
        "{label}: nothing of the refused transaction is durable"
    );
    assert_eq!(stock_integrity(&run.db), "ok", "{label}");
}

/// bd-q4o7z positive: the same race on the same leaf page, but nothing the
/// transaction returned depends on the peer's row. It read `h` (which the
/// peer does not write) and inserted a row without an id, and `execute`
/// reported one row. The refused COMMIT replays the transaction on the newest
/// snapshot and commits. Its row takes the id after the peer's, and
/// `last_insert_rowid()` after the COMMIT names that row, so what the caller
/// can still read agrees with the database. Before bd-q4o7z this COMMIT was
/// refused and the row lost.
#[test]
fn transaction_open_across_a_stock_commit_on_the_same_page_is_replayed_when_nothing_it_returned_changes()
 {
    if std::env::var_os(PEER_ENV).is_some() || std::env::var_os(HOLDER_ENV).is_some() {
        return;
    }
    let run = open_transaction_across_peer_commit(
        "stock",
        "INSERT INTO t(w, s) VALUES (2, 'stock')",
        &[
            Step::Query("SELECT v FROM h WHERE id = 1"),
            Step::Execute("INSERT INTO t(w, s) VALUES (1, 'fsqlite')"),
        ],
        false,
    );
    assert_generation_started_in_place(&run);
    assert_eq!(
        run.observed,
        ["[[Integer(0)]]", "1"],
        "shape: what the caller saw"
    );
    if let Err(error) = &run.commit {
        panic!("the refused COMMIT must be replayed and commit, got {error:?}");
    }
    assert!(!run.open_after_commit);
    assert_eq!(
        stock_rows(&run.db),
        vec![
            (1, "seed".to_owned()),
            (2, "stock".to_owned()),
            (3, "fsqlite".to_owned()),
        ],
        "the peer's row and the replayed row are both durable"
    );
    assert_eq!(
        run.rowid_after_commit, 3,
        "last_insert_rowid() after the COMMIT names the replayed row"
    );
    assert_eq!(run.fsqlite_ids, vec![1, 2, 3]);
    assert_eq!(stock_integrity(&run.db), "ok");
}

const ROWID_THROUGH_API: &[Step] = &[
    Step::Execute("INSERT INTO t(w, s) VALUES (1, 'fsqlite')"),
    Step::ReadRowid,
];
const ROWID_THROUGH_RETURNING: &[Step] = &[Step::Query(
    "INSERT INTO t(w, s) VALUES (1, 'fsqlite') RETURNING id",
)];
const ROWID_THROUGH_SQL: &[Step] = &[
    Step::Execute("INSERT INTO t(w, s) VALUES (1, 'fsqlite')"),
    Step::Query("SELECT last_insert_rowid()"),
];

/// bd-q4o7z negatives a naive replay fails: the caller learned its new row's
/// id, through `last_insert_rowid()`, through RETURNING, or through
/// `SELECT last_insert_rowid()`, and the peer's commit took that id. A replay
/// would give the row another id than the one the caller holds, so the
/// refusal stands, and the id the caller saw names the peer's row.
#[test]
fn transaction_that_returned_its_rowid_is_refused_when_a_peer_takes_that_rowid() {
    if std::env::var_os(PEER_ENV).is_some() || std::env::var_os(HOLDER_ENV).is_some() {
        return;
    }
    for (label, steps, saw) in [
        ("api", ROWID_THROUGH_API, &["1", "rowid 2"][..]),
        (
            "returning",
            ROWID_THROUGH_RETURNING,
            &["[[Integer(2)]]"][..],
        ),
        ("sql", ROWID_THROUGH_SQL, &["1", "[[Integer(2)]]"][..]),
    ] {
        let run = open_transaction_across_peer_commit(
            "stock",
            "INSERT INTO t(w, s) VALUES (2, 'stock')",
            steps,
            false,
        );
        assert_eq!(run.observed, saw, "{label}: shape: the caller saw id 2");
        assert_refused_once(label, &run, &[(1, "seed"), (2, "stock")]);
    }
}

/// bd-q4o7z negative: an `UPDATE ... WHERE` reported one changed row; after
/// the peer's commit the same statement changes two. A replay would report a
/// different `changes()`, so the refusal stands and no row is updated.
#[test]
fn transaction_whose_changes_count_would_differ_is_refused() {
    if std::env::var_os(PEER_ENV).is_some() || std::env::var_os(HOLDER_ENV).is_some() {
        return;
    }
    let run = open_transaction_across_peer_commit(
        "stock",
        "INSERT INTO t VALUES (3, 2, 'stock')",
        &[Step::Execute("UPDATE t SET w = w + 10 WHERE w < 5")],
        false,
    );
    assert_eq!(
        run.observed,
        ["1"],
        "shape: the UPDATE changed the seed row"
    );
    assert_refused_once("changes", &run, &[(1, "seed"), (3, "stock")]);
    assert_eq!(
        stock_ws(&run.db),
        vec![(1, 0), (3, 2)],
        "no row was updated"
    );
}

/// bd-q4o7z negative: the replay of an INSERT that succeeded meets a
/// constraint error, because the peer took its primary key. The replay is not
/// committed: the refusal stands and the peer's row keeps the key.
#[test]
fn transaction_whose_replay_meets_a_new_constraint_error_is_refused() {
    if std::env::var_os(PEER_ENV).is_some() || std::env::var_os(HOLDER_ENV).is_some() {
        return;
    }
    let run = open_transaction_across_peer_commit(
        "stock",
        "INSERT INTO t VALUES (2, 2, 'stock')",
        INSERT_ID_2,
        false,
    );
    assert_eq!(run.observed, ["1"]);
    assert_refused_once("constraint", &run, &[(1, "seed"), (2, "stock")]);
}

/// bd-q4o7z negative: a transaction on a connection with an application
/// function is never replayed, so the function never runs twice: the refusal
/// stands and `app_counter()` ran once.
#[test]
fn transaction_that_called_an_application_function_is_never_replayed() {
    if std::env::var_os(PEER_ENV).is_some() || std::env::var_os(HOLDER_ENV).is_some() {
        return;
    }
    let run = open_transaction_across_peer_commit(
        "stock",
        "INSERT INTO t(w, s) VALUES (2, 'stock')",
        &[Step::Execute(
            "INSERT INTO t(w, s) VALUES (app_counter(), 'fsqlite')",
        )],
        true,
    );
    assert_eq!(run.observed, ["1"]);
    assert_refused_once("application function", &run, &[(1, "seed"), (2, "stock")]);
    assert_eq!(
        run.app_calls, 1,
        "the application function ran exactly once"
    );
}

const RAN_A_PRAGMA: &[Step] = &[
    Step::Execute("PRAGMA user_version = 7"),
    Step::Execute("INSERT INTO t(w, s) VALUES (1, 'fsqlite')"),
];
const RAN_SCHEMA_DDL: &[Step] = &[
    Step::Execute("CREATE TABLE u(a)"),
    Step::Execute("INSERT INTO t(w, s) VALUES (1, 'fsqlite')"),
];
const STREAMED_A_READ: &[Step] = &[
    Step::Stream("SELECT id FROM t"),
    Step::Execute("INSERT INTO t(w, s) VALUES (1, 'fsqlite')"),
];

/// bd-q4o7z negative: a transaction that ran a PRAGMA or schema DDL, or read
/// through the streaming API (whose rows the record does not hold), is never
/// replayed, even though nothing it returned would change: the refusal
/// stands, and neither its INSERT nor its PRAGMA or table is durable.
#[test]
fn transaction_that_ran_a_call_a_replay_cannot_repeat_is_never_replayed() {
    if std::env::var_os(PEER_ENV).is_some() || std::env::var_os(HOLDER_ENV).is_some() {
        return;
    }
    for (label, steps) in [
        ("pragma", RAN_A_PRAGMA),
        ("schema ddl", RAN_SCHEMA_DDL),
        ("streamed read", STREAMED_A_READ),
    ] {
        let run = open_transaction_across_peer_commit(
            "stock",
            "INSERT INTO t(w, s) VALUES (2, 'stock')",
            steps,
            false,
        );
        assert_refused_once(label, &run, &[(1, "seed"), (2, "stock")]);
        let stock = rusqlite::Connection::open(&run.db).expect("stock open");
        let user_version: i64 = stock
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("user_version");
        let tables: i64 = stock
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name = 'u'",
                [],
                |row| row.get(0),
            )
            .expect("sqlite_master");
        assert_eq!(
            (user_version, tables),
            (0, 0),
            "{label}: nothing else is durable"
        );
    }
}

/// Negative case: an fsqlite peer in the same shape. Its commit appends to
/// the generation the parent's snapshot already names, and the parent's
/// COMMIT commits, as it did before this change.
#[test]
fn transaction_open_across_an_fsqlite_commit_on_another_table_commits() {
    if std::env::var_os(PEER_ENV).is_some() || std::env::var_os(HOLDER_ENV).is_some() {
        return;
    }
    let run = open_transaction_across_peer_commit(
        "fsqlite",
        "UPDATE h SET v = v + 1 WHERE id = 1",
        INSERT_ID_2,
        false,
    );
    if let Err(error) = &run.commit {
        panic!(
            "COMMIT after an fsqlite commit on another table must commit, got {error:?} \
             (WAL salts {:?})",
            run.wal_salts
        );
    }
    assert!(!run.open_after_commit);
    assert_eq!(run.fsqlite_ids, vec![1, 2]);
    assert_eq!(
        stock_rows(&run.db),
        vec![(1, "seed".to_owned()), (2, "fsqlite".to_owned())]
    );
    assert_eq!(stock_h(&run.db), 1);
    assert_eq!(stock_integrity(&run.db), "ok");
}

/// bd-7dxmt: the child process of the replay-outcome keepers. The counters are
/// process-wide, so each scenario runs alone in its own process.
const REPLAY_OUTCOME_ENV: &str = "FSQLITE_REPLAY_OUTCOME_CHILD";

/// What one public COMMIT recorded, as deltas of the process-wide and
/// connection-local evidence around it, with its wall and CPU cost.
struct CommitOutcomeEvidence {
    commit: Result<(), FrankenError>,
    replay_attempts: u64,
    replays_committed: u64,
    busy_snapshot: u64,
    commits: u64,
    /// Decision types of the SSI cards it added.
    cards: Vec<String>,
    /// commit_events rows it added, retained or not.
    events_added: u64,
    /// `phase:reason` of the retained commit_events rows it added.
    events: Vec<String>,
    in_transaction_after: bool,
    wall_ms: u128,
    cpu_us: u64,
}

impl CommitOutcomeEvidence {
    /// One `key=value` line with no spaces inside a value; the parent reads it
    /// with `outcome_field`.
    fn line(&self, scenario: &str) -> String {
        let commit = match &self.commit {
            Ok(()) => "ok".to_owned(),
            Err(FrankenError::BusySnapshot { .. }) => "busy_snapshot".to_owned(),
            Err(error) => format!("error:{}", error.to_string().replace(' ', "_")),
        };
        format!(
            "event=replay_outcome scenario={scenario} commit={commit} replay_attempts={} \
             replays_committed={} busy_snapshot={} commits={} cards={} card_types={} \
             events_added={} events={} in_transaction_after={} wall_ms={} cpu_us={}",
            self.replay_attempts,
            self.replays_committed,
            self.busy_snapshot,
            self.commits,
            self.cards.len(),
            self.cards.join(","),
            self.events_added,
            self.events.join(","),
            self.in_transaction_after,
            self.wall_ms,
            self.cpu_us,
        )
    }
}

/// CPU time of this process, in ns: every thread's `/proc/self/task/*/schedstat`
/// (Linux; 0 where it is unavailable). The future may be polled on any thread.
fn process_cpu_ns() -> u64 {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return 0;
    };
    tasks
        .filter_map(Result::ok)
        .filter_map(|task| std::fs::read_to_string(task.path().join("schedstat")).ok())
        .filter_map(|text| text.split_whitespace().next()?.parse::<u64>().ok())
        .sum()
}

/// Counts and evidence before one public COMMIT.
struct EvidenceBefore {
    counts: fsqlite_core::connection::TxnReplayCounts,
    busy_snapshot: u64,
    commits: u64,
    /// The ledger's last `decision_epoch` (monotone from 1; these drafts
    /// carry no decision id).
    last_card_epoch: u64,
    last_event_id: u64,
    cpu_ns: u64,
    started: Instant,
}

fn evidence_before(conn: &Connection) -> EvidenceBefore {
    let metrics = fsqlite_observability::metrics::global();
    EvidenceBefore {
        counts: fsqlite_core::connection::txn_replay_counts(),
        busy_snapshot: metrics.conflicts_busy_snapshot_total.get(),
        commits: metrics.commits_total.get(),
        last_card_epoch: conn
            .ssi_decisions_snapshot()
            .iter()
            .map(|card| card.decision_epoch)
            .max()
            .unwrap_or(0),
        last_event_id: conn
            .commit_execution_snapshot()
            .events
            .iter()
            .map(|event| event.event_id)
            .max()
            .unwrap_or(0),
        cpu_ns: process_cpu_ns(),
        started: Instant::now(),
    }
}

fn evidence_after(
    conn: &Connection,
    before: &EvidenceBefore,
    commit: Result<(), FrankenError>,
) -> CommitOutcomeEvidence {
    let wall_ms = before.started.elapsed().as_millis();
    let cpu_us = process_cpu_ns().saturating_sub(before.cpu_ns) / 1_000;
    let metrics = fsqlite_observability::metrics::global();
    let counts = fsqlite_core::connection::txn_replay_counts();
    let events = conn.commit_execution_snapshot();
    // Event ids are monotone, so the last one counts the rows this COMMIT
    // added even when the 64-row ring dropped some of them.
    let events_added = events
        .events
        .iter()
        .map(|event| event.event_id)
        .max()
        .unwrap_or(before.last_event_id)
        .saturating_sub(before.last_event_id);
    CommitOutcomeEvidence {
        events_added,
        commit,
        replay_attempts: counts.attempts - before.counts.attempts,
        replays_committed: counts.committed - before.counts.committed,
        busy_snapshot: metrics.conflicts_busy_snapshot_total.get() - before.busy_snapshot,
        commits: metrics.commits_total.get() - before.commits,
        cards: conn
            .ssi_decisions_snapshot()
            .iter()
            .filter(|card| card.decision_epoch > before.last_card_epoch)
            .map(|card| format!("{:?}", card.decision_type))
            .collect(),
        events: events
            .events
            .iter()
            .filter(|event| event.event_id > before.last_event_id)
            .map(|event| format!("{}:{}", event.phase, event.reason))
            .collect(),
        in_transaction_after: conn.in_transaction(),
        wall_ms,
        cpu_us,
    }
}

/// An SSI write skew between `first` and `second` on one file: each reads
/// the table the other updates, and `second` is still open when `first`
/// commits, so `first`'s COMMIT is refused as the pivot.
async fn open_write_skew(first: &Connection, second: &Connection) {
    first.execute("BEGIN").await.expect("first BEGIN");
    second.execute("BEGIN").await.expect("second BEGIN");
    first
        .query("SELECT value FROM b WHERE id = 1")
        .await
        .expect("first reads b");
    second
        .query("SELECT value FROM a WHERE id = 1")
        .await
        .expect("second reads a");
    first
        .execute("UPDATE a SET value = 11 WHERE id = 1")
        .await
        .expect("first writes a");
    second
        .execute("UPDATE b SET value = 21 WHERE id = 1")
        .await
        .expect("second writes b");
}

async fn seed_write_skew(path: &str) {
    let seed = Connection::open(path).await.expect("seed open");
    seed.query("PRAGMA journal_mode=WAL").await.expect("WAL");
    seed.execute("CREATE TABLE a(id INTEGER PRIMARY KEY, value INTEGER)")
        .await
        .expect("a");
    seed.execute("CREATE TABLE b(id INTEGER PRIMARY KEY, value INTEGER)")
        .await
        .expect("b");
    seed.execute("INSERT INTO a VALUES(1, 10)")
        .await
        .expect("seed a");
    seed.execute("INSERT INTO b VALUES(1, 20)")
        .await
        .expect("seed b");
    seed.close().await.expect("seed close");
}

fn run_replay_outcome_child(scenario: &str) -> String {
    let exe = std::env::current_exe().expect("test binary path");
    let output = Command::new(exe)
        .args([
            "--exact",
            "replay_outcome_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(REPLAY_OUTCOME_ENV, scenario)
        .env("FRANKENSQLITE_METRICS_DISABLE", "0")
        .output()
        .expect("spawn the replay-outcome child");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "replay-outcome child {scenario} failed:\n{stdout}\n{stderr}"
    );
    assert!(
        stdout.contains(&format!(
            "event=replay_outcome_verified scenario={scenario}"
        )),
        "the {scenario} child must run its scenario:\n{stdout}\n{stderr}"
    );
    // The evidence lines, for the run log (attempts, wall and CPU per COMMIT).
    for line in stdout
        .lines()
        .filter(|line| line.starts_with("event=replay_outcome"))
    {
        println!("{line}");
    }
    stdout
}

/// The child body; a no-op unless the parent set `REPLAY_OUTCOME_ENV`.
#[test]
fn replay_outcome_child() {
    let Ok(scenario) = std::env::var(REPLAY_OUTCOME_ENV) else {
        return;
    };
    assert!(
        !fsqlite_observability::metrics::metrics_disabled(),
        "the outcome counts need the metrics registry"
    );
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("skew.db");
    let path = path.to_str().expect("utf8 path").to_owned();
    asupersync::test_utils::run_test(|| seed_write_skew(&path));
    let outcome = match scenario.as_str() {
        "refused" => {
            let mut outcome = None;
            asupersync::test_utils::run_test(|| async {
                let first = Connection::open(&path).await.expect("first open");
                let second = Connection::open(&path).await.expect("second open");
                first.set_diagnostic_capture_enabled(true);
                // The explain-concurrency contract's busy_timeout. The peer
                // stays open through every replay attempt.
                first
                    .execute("PRAGMA busy_timeout=5000")
                    .await
                    .expect("busy_timeout");
                open_write_skew(&first, &second).await;
                let before = evidence_before(&first);
                let commit = first.execute("COMMIT").await.map(drop);
                outcome = Some(evidence_after(&first, &before, commit));
                second
                    .execute("COMMIT")
                    .await
                    .expect("the survivor commits");
                first.close().await.expect("first close");
                second.close().await.expect("second close");
            });
            outcome.expect("refused scenario ran")
        }
        "peer_rolls_back" => {
            // The peer runs on its own thread and rolls back while `first`'s
            // COMMIT is waiting in its replay.
            let steps = std::sync::Arc::new(std::sync::Barrier::new(2));
            let peer_steps = std::sync::Arc::clone(&steps);
            let peer_path = path.clone();
            // When the peer's ROLLBACK returned: how long after it the replay
            // noticed is the owner proposal's latency figure (bd-7dxmt).
            let peer_ended = std::sync::Arc::new(std::sync::Mutex::new(None::<Instant>));
            let peer_ended_at = std::sync::Arc::clone(&peer_ended);
            let peer = std::thread::spawn(move || {
                asupersync::test_utils::run_test(|| async move {
                    let second = Connection::open(&peer_path).await.expect("second open");
                    peer_steps.wait(); // 1: first has begun and read b
                    second.execute("BEGIN").await.expect("second BEGIN");
                    second
                        .query("SELECT value FROM a WHERE id = 1")
                        .await
                        .expect("second reads a");
                    peer_steps.wait(); // 2
                    peer_steps.wait(); // 3: first has written a
                    second
                        .execute("UPDATE b SET value = 21 WHERE id = 1")
                        .await
                        .expect("second writes b");
                    peer_steps.wait(); // 4: first is about to COMMIT
                    std::thread::sleep(Duration::from_millis(200));
                    second.execute("ROLLBACK").await.expect("second rolls back");
                    *peer_ended_at.lock().expect("peer clock") = Some(Instant::now());
                    second.close().await.expect("second close");
                });
            });
            let mut outcome = None;
            let mut commit_returned = None;
            asupersync::test_utils::run_test(|| async {
                let first = Connection::open(&path).await.expect("first open");
                first.set_diagnostic_capture_enabled(true);
                first
                    .execute("PRAGMA busy_timeout=5000")
                    .await
                    .expect("busy_timeout");
                first.execute("BEGIN").await.expect("first BEGIN");
                first
                    .query("SELECT value FROM b WHERE id = 1")
                    .await
                    .expect("first reads b");
                steps.wait(); // 1
                steps.wait(); // 2: second has begun and read a
                first
                    .execute("UPDATE a SET value = 11 WHERE id = 1")
                    .await
                    .expect("first writes a");
                steps.wait(); // 3
                steps.wait(); // 4: second has written b
                let before = evidence_before(&first);
                let commit = first.execute("COMMIT").await.map(drop);
                commit_returned = Some(Instant::now());
                outcome = Some(evidence_after(&first, &before, commit));
                first.close().await.expect("first close");
            });
            peer.join().expect("peer thread");
            let a: i64 = rusqlite::Connection::open(&path)
                .expect("verify open")
                .query_row("SELECT value FROM a WHERE id = 1", [], |row| row.get(0))
                .expect("a");
            let peer_ended = peer_ended
                .lock()
                .expect("peer clock")
                .expect("the peer rolled back");
            let commit_returned = commit_returned.expect("the COMMIT returned");
            // Signed: the replay may commit before the peer's ROLLBACK call
            // returns to it.
            let lag_us = if commit_returned >= peer_ended {
                i128::try_from(commit_returned.duration_since(peer_ended).as_micros())
                    .unwrap_or(i128::MAX)
            } else {
                -i128::try_from(peer_ended.duration_since(commit_returned).as_micros())
                    .unwrap_or(i128::MAX)
            };
            println!("event=replay_outcome_durable a={a} peer_end_to_commit_us={lag_us}");
            outcome.expect("peer scenario ran")
        }
        other => panic!("unknown replay-outcome scenario {other}"),
    };
    println!("{}", outcome.line(&scenario));
    println!("event=replay_outcome_verified scenario={scenario}");
}

/// The value of `name` on the child's `event=replay_outcome` line.
fn outcome_field(stdout: &str, name: &str) -> String {
    let line = stdout
        .lines()
        .find(|line| line.starts_with("event=replay_outcome scenario="))
        .expect("outcome line");
    let prefix = format!("{name}=");
    line.split(' ')
        .find_map(|token| token.strip_prefix(prefix.as_str()))
        .unwrap_or_else(|| panic!("no {name} in {line}"))
        .to_owned()
}

fn outcome_count(stdout: &str, name: &str) -> u64 {
    outcome_field(stdout, name)
        .parse()
        .unwrap_or_else(|_| panic!("{name} is a count: {stdout}"))
}

/// bd-7dxmt (a): an SSI-refused COMMIT whose replay is refused again N >= 2
/// times, while the peer stays open, returns 517 and reports ONE outcome: busy
/// +1, one SSI decision card, one commit_events row (`validation_rejected`),
/// no commit. Before the fix each internal attempt counted as a public outcome
/// (the explain-concurrency contract saw 104-106 per COMMIT).
#[test]
fn ssi_refused_commit_replayed_and_refused_again_reports_one_outcome() {
    if std::env::var_os(REPLAY_OUTCOME_ENV).is_some() {
        return;
    }
    let stdout = run_replay_outcome_child("refused");
    let count = |name| outcome_count(&stdout, name);
    assert_eq!(
        outcome_field(&stdout, "commit"),
        "busy_snapshot",
        "the COMMIT is refused with BusySnapshot: {stdout}"
    );
    assert!(
        count("replay_attempts") >= 2,
        "non-vacuity: the replay must be refused again at least twice: {stdout}"
    );
    assert_eq!(count("busy_snapshot"), 1, "one public refusal: {stdout}");
    assert_eq!(count("commits"), 0, "nothing committed: {stdout}");
    assert_eq!(count("replays_committed"), 0, "{stdout}");
    assert_eq!(outcome_field(&stdout, "in_transaction_after"), "false");
    assert_eq!(
        count("cards"),
        1,
        "one SSI decision card for one public COMMIT: {stdout}"
    );
    assert!(
        outcome_field(&stdout, "card_types").starts_with("Abort"),
        "the card is the refusal: {stdout}"
    );
    assert_eq!(
        count("events_added"),
        1,
        "one commit_events row for one public COMMIT: {stdout}"
    );
    assert!(
        outcome_field(&stdout, "events").starts_with("validation_rejected:"),
        "the row is the refusal: {stdout}"
    );
}

/// bd-7dxmt (b): an SSI-refused COMMIT whose peer rolls back while the replay
/// waits. The replay then commits within busy_timeout: the COMMIT succeeds,
/// its row is durable, and it reports one outcome, the commit: commits +1,
/// busy +0 (the attempts before it are internal), and no rejected
/// commit_events row. A fix that stopped replaying SSI refusals would return
/// 517 here.
#[test]
fn ssi_refused_commit_whose_peer_rolls_back_commits_once_through_its_replay() {
    if std::env::var_os(REPLAY_OUTCOME_ENV).is_some() {
        return;
    }
    let stdout = run_replay_outcome_child("peer_rolls_back");
    let count = |name| outcome_count(&stdout, name);
    assert_eq!(
        outcome_field(&stdout, "commit"),
        "ok",
        "the replay commits within busy_timeout: {stdout}"
    );
    assert!(
        stdout.contains("event=replay_outcome_durable a=11"),
        "the row is durable: {stdout}"
    );
    assert_eq!(
        count("replays_committed"),
        1,
        "non-vacuity: a replay committed: {stdout}"
    );
    assert!(
        count("replay_attempts") >= 2,
        "non-vacuity: the replay was refused before the peer rolled back: {stdout}"
    );
    assert_eq!(count("commits"), 1, "one public commit: {stdout}");
    assert_eq!(
        count("busy_snapshot"),
        0,
        "the refused attempts are internal: {stdout}"
    );
    assert!(
        !outcome_field(&stdout, "events").contains("validation_rejected"),
        "the COMMIT reports its commit, not the refusals before it: {stdout}"
    );
    assert!(
        !outcome_field(&stdout, "card_types").contains("Abort"),
        "no refusal card for a COMMIT that committed: {stdout}"
    );
}

/// bd-29qkj: the child process of the replay-contention keepers.
const REPLAY_CONTENTION_ENV: &str = "FSQLITE_REPLAY_CONTENTION_CHILD";

/// The shared conflict log's length (every event, all connections).
async fn conflict_log_len(monitor: &Connection) -> usize {
    monitor
        .query("PRAGMA fsqlite.conflict_log;")
        .await
        .expect("conflict log")
        .len()
}

/// The shared conflict log's page-lock contention events from entry `from`
/// on, as `requester:holder` transaction ids.
async fn contention_events_since(monitor: &Connection, from: usize) -> Vec<String> {
    let rows = monitor
        .query("PRAGMA fsqlite.conflict_log;")
        .await
        .expect("conflict log");
    rows[from.min(rows.len())..]
        .iter()
        .filter_map(|row| match row.values().get(2) {
            Some(fsqlite::SqliteValue::Text(event)) if event.contains("PageLockContention") => {
                Some(format!(
                    "{}:{}",
                    event_txn(event, "requester"),
                    event_txn(event, "holder")
                ))
            }
            _ => None,
        })
        .collect()
}

/// `name: TxnId(N)` in a conflict-log event.
fn event_txn(event: &str, name: &str) -> u64 {
    let key = format!("{name}: TxnId(");
    let at = event
        .find(&key)
        .unwrap_or_else(|| panic!("no {name} in {event}"))
        + key.len();
    event[at..]
        .split(')')
        .next()
        .and_then(|digits| digits.parse().ok())
        .unwrap_or_else(|| panic!("no {name} id in {event}"))
}

/// The transaction id of `conn`'s last COMMIT, from its commit_events rows
/// (commit capture must be on). This is the id conflict events name.
fn committed_txn_id(conn: &Connection) -> u64 {
    conn.commit_execution_snapshot()
        .events
        .last()
        .map(|event| event.transaction_id)
        .expect("the COMMIT recorded its commit_events rows")
}

fn run_replay_contention_child(scenario: &str) -> String {
    let exe = std::env::current_exe().expect("test binary path");
    let output = Command::new(exe)
        .args([
            "--exact",
            "replay_contention_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(REPLAY_CONTENTION_ENV, scenario)
        .output()
        .expect("spawn the replay-contention child");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "replay-contention child {scenario} failed:\n{stdout}\n{stderr}"
    );
    let line = stdout
        .lines()
        .find(|line| line.starts_with(&format!("event=replay_contention scenario={scenario} ")))
        .unwrap_or_else(|| panic!("the {scenario} child must report:\n{stdout}\n{stderr}"))
        .to_owned();
    println!("{line}");
    line
}

fn contention_field<'a>(line: &'a str, name: &str) -> &'a str {
    let prefix = format!("{name}=");
    line.split(' ')
        .find_map(|token| token.strip_prefix(prefix.as_str()))
        .unwrap_or_else(|| panic!("no {name} in {line}"))
}

fn contention_count(line: &str, name: &str) -> u64 {
    contention_field(line, name)
        .parse()
        .unwrap_or_else(|_| panic!("{name} is a count: {line}"))
}

/// The shape-2 replay (bd-29qkj step 1): an SSI write skew with the peer open
/// whose transaction also updates `u`, while a third connection takes `u`'s
/// page after the first refusal and keeps its transaction open. Every replay
/// attempt that starts after that waits on the third connection's lock.
fn third_holds_page_scenario(path: &str) -> String {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    let stop = Arc::new(AtomicBool::new(false));
    let attempts_at_hold = Arc::new(AtomicU64::new(u64::MAX));
    let third_txn = Arc::new(AtomicU64::new(0));
    let attempts_before = fsqlite_core::connection::txn_replay_counts().attempts;
    let third = {
        let path = path.to_owned();
        let stop = Arc::clone(&stop);
        let attempts_at_hold = Arc::clone(&attempts_at_hold);
        let third_txn = Arc::clone(&third_txn);
        std::thread::spawn(move || {
            asupersync::test_utils::run_test(|| async move {
                let conn = Connection::open(&path).await.expect("third open");
                conn.set_diagnostic_capture_enabled(true);
                while fsqlite_core::connection::txn_replay_counts().attempts == attempts_before {
                    std::thread::sleep(Duration::from_micros(20));
                }
                loop {
                    conn.execute("BEGIN").await.expect("third BEGIN");
                    if conn
                        .execute("UPDATE u SET value = value + 100 WHERE id = 1")
                        .await
                        .is_ok()
                    {
                        break;
                    }
                    let _ = conn.execute("ROLLBACK").await;
                }
                attempts_at_hold.store(
                    fsqlite_core::connection::txn_replay_counts().attempts,
                    Ordering::Release,
                );
                while !stop.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(5));
                }
                // Committing names the third connection's transaction id.
                conn.execute("COMMIT").await.expect("third commits");
                third_txn.store(committed_txn_id(&conn), Ordering::Release);
                conn.close().await.expect("third close");
            });
        })
    };
    let mut measured = None;
    asupersync::test_utils::run_test(|| async {
        let monitor = Connection::open(path).await.expect("monitor open");
        let first = Connection::open(path).await.expect("first open");
        let second = Connection::open(path).await.expect("second open");
        first
            .execute("PRAGMA busy_timeout=2000")
            .await
            .expect("busy_timeout");
        open_write_skew(&first, &second).await;
        first
            .execute("UPDATE u SET value = value + 1 WHERE id = 1")
            .await
            .expect("first writes u");
        let log_from = conflict_log_len(&monitor).await;
        let commit = first.execute("COMMIT").await;
        let attempts_after = fsqlite_core::connection::txn_replay_counts().attempts;
        let events = contention_events_since(&monitor, log_from).await;
        stop.store(true, Ordering::Release);
        let commit = match commit {
            Ok(_) => "ok".to_owned(),
            Err(FrankenError::BusySnapshot { .. }) => "busy_snapshot".to_owned(),
            Err(error) => format!("error:{}", error.to_string().replace(' ', "_")),
        };
        measured = Some((commit, attempts_after, events));
        second
            .execute("COMMIT")
            .await
            .expect("the survivor commits");
        first.close().await.expect("first close");
        second.close().await.expect("second close");
        monitor.close().await.expect("monitor close");
    });
    third.join().expect("third thread");
    let (commit, attempts_after, events) = measured.expect("the COMMIT ran");
    format!(
        "commit={commit} attempts={} attempts_after_hold={} third_txn={} \
         contention_events={} events={}",
        attempts_after - attempts_before,
        attempts_after.saturating_sub(attempts_at_hold.load(Ordering::Acquire)),
        third_txn.load(Ordering::Acquire),
        events.len(),
        events.join(","),
    )
}

/// The negative: a plain statement (no replay) contending on a held page.
fn plain_contention_scenario(path: &str) -> String {
    let mut report = String::new();
    asupersync::test_utils::run_test(|| async {
        let monitor = Connection::open(path).await.expect("monitor open");
        let holder = Connection::open(path).await.expect("holder open");
        let requester = Connection::open(path).await.expect("requester open");
        holder.set_diagnostic_capture_enabled(true);
        requester
            .execute("PRAGMA busy_timeout=0")
            .await
            .expect("busy_timeout");
        holder.execute("BEGIN").await.expect("holder BEGIN");
        holder
            .execute("UPDATE u SET value = value + 1 WHERE id = 1")
            .await
            .expect("holder writes u");
        requester.execute("BEGIN").await.expect("requester BEGIN");
        let log_from = conflict_log_len(&monitor).await;
        let denied = requester
            .execute("UPDATE u SET value = value + 2 WHERE id = 1")
            .await
            .is_err();
        let events = contention_events_since(&monitor, log_from).await;
        if requester.in_transaction() {
            requester
                .execute("ROLLBACK")
                .await
                .expect("requester rolls back");
        }
        // Committing names the holder's transaction id.
        holder.execute("COMMIT").await.expect("holder commits");
        report = format!(
            "denied={denied} holder_txn={} contention_events={} events={}",
            committed_txn_id(&holder),
            events.len(),
            events.join(","),
        );
        requester.close().await.expect("requester close");
        holder.close().await.expect("holder close");
        monitor.close().await.expect("monitor close");
    });
    report
}

/// The child body; a no-op unless the parent set `REPLAY_CONTENTION_ENV`.
#[test]
fn replay_contention_child() {
    let Ok(scenario) = std::env::var(REPLAY_CONTENTION_ENV) else {
        return;
    };
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("contention.db");
    let path = path.to_str().expect("utf8 path").to_owned();
    asupersync::test_utils::run_test(|| async {
        seed_write_skew(&path).await;
        let seed = Connection::open(&path).await.expect("seed open");
        seed.execute("CREATE TABLE u(id INTEGER PRIMARY KEY, value INTEGER)")
            .await
            .expect("u");
        seed.execute("INSERT INTO u VALUES(1, 30)")
            .await
            .expect("seed u");
        seed.close().await.expect("seed close");
    });
    let report = match scenario.as_str() {
        "third_holds_page" => third_holds_page_scenario(&path),
        "plain_contention" => plain_contention_scenario(&path),
        other => panic!("unknown replay-contention scenario {other}"),
    };
    println!("event=replay_contention scenario={scenario} {report}");
}

/// bd-29qkj: a refused COMMIT whose replay attempts each wait on a page lock
/// held by a third connection publishes none of those attempts' contention
/// to the shared conflict log; the caller-visible attempt raised none
/// either. Only the third connection's own waits may appear. Before the fix
/// (and with the attempts not quiet) each attempt that met the lock added an
/// event whose requester is the attempt.
#[test]
fn replayed_commit_publishes_no_page_lock_contention_from_its_internal_attempts() {
    if std::env::var_os(REPLAY_CONTENTION_ENV).is_some() {
        return;
    }
    let line = run_replay_contention_child("third_holds_page");
    let third_txn = contention_count(&line, "third_txn");
    assert_ne!(
        third_txn, 0,
        "the third connection must hold u's page: {line}"
    );
    // Non-vacuity: a replay attempt that meets the third connection's lock
    // waits on it for the rest of busy_timeout. Without that wait the replay
    // cycles about 25 attempts per second (50 in 2 s in bd-29qkj step 1), so
    // a handful of attempts means an attempt waited on the held page.
    assert!(
        contention_count(&line, "attempts") <= 5,
        "non-vacuity: a replay attempt must wait on the third connection's lock: {line}"
    );
    assert_eq!(
        contention_field(&line, "commit"),
        "busy_snapshot",
        "the peer stays open, so the COMMIT is refused at busy_timeout: {line}"
    );
    let events = contention_field(&line, "events");
    for event in events.split(',').filter(|event| !event.is_empty()) {
        let requester: u64 = event
            .split(':')
            .next()
            .and_then(|id| id.parse().ok())
            .unwrap_or_else(|| panic!("bad event {event} in {line}"));
        assert_eq!(
            requester, third_txn,
            "only the third connection's own waits are public; {event} is a replay attempt's: {line}"
        );
    }
}

/// The negative: a statement that contends on a held page with no replay
/// still logs exactly one event naming its requester and the holder. A fix
/// that silenced page-lock contention everywhere fails here.
#[test]
fn plain_statement_contending_on_a_held_page_still_logs_its_contention() {
    if std::env::var_os(REPLAY_CONTENTION_ENV).is_some() {
        return;
    }
    let line = run_replay_contention_child("plain_contention");
    assert_eq!(contention_field(&line, "denied"), "true", "{line}");
    assert_eq!(contention_count(&line, "contention_events"), 1, "{line}");
    let event = contention_field(&line, "events");
    let (requester, holder) = event
        .split_once(':')
        .unwrap_or_else(|| panic!("bad event {event} in {line}"));
    assert_eq!(
        holder,
        contention_count(&line, "holder_txn").to_string(),
        "the event names the holder: {line}"
    );
    assert_ne!(
        requester, holder,
        "the requester is the other transaction: {line}"
    );
}
