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

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use fsqlite::Connection;
use fsqlite_error::FrankenError;

const SQLITE_BUSY: i32 = 5;
const SQLITE_BUSY_SNAPSHOT: i32 = 517;
const HOLDER_ENV: &str = "FSQLITE_REFUSED_COMMIT_HOLDER";
const STOCK_HOLDER_ENV: &str = "FSQLITE_REFUSED_COMMIT_STOCK_HOLDER";

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
