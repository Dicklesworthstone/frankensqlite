#![recursion_limit = "512"]

//! bd-reality-check-1022-llk6g.5.6: a writable fsqlite connection and a WAL
//! shorter than its 32-byte header, with stock SQLite live in another process.
//!
//! Stock reads such a WAL as one with no frames: the database file alone is
//! current, and its next writer writes a header under WAL_WRITE_LOCK. A stock
//! connection leaves one behind whenever it creates the WAL (it writes the
//! header only with its first frame) or runs `wal_checkpoint(TRUNCATE)`.
//!
//! On main b081f59ea, beside any stock connection with a transaction open (a
//! read transaction is enough):
//! - a fresh fsqlite open waited about 35 s, ignoring busy_timeout, and then
//!   failed "database is busy": the open initialized the WAL under an
//!   EXCLUSIVE fence on the main file, which every stock SHARED lock blocks;
//! - an fsqlite connection already open across a stock TRUNCATE failed every
//!   later read and write ("busy", "recovery in progress") while any stock
//!   connection stayed open, and lost its rows.
//!
//! Stock is the oracle for each answer below: its reader returns at once and
//! its writer commits. Negative cases a naive fix fails:
//! - nothing writes a WAL header outside the append gate: a reader, and a
//!   write refused with BUSY behind a stock writer, leave the WAL as it was;
//! - the header the first fsqlite writer writes is one stock accepts, both by
//!   writing on top of it and by recovering a copy of the WAL from scratch;
//! - a WAL with frames reads as before, and a stale index over a zero-byte
//!   WAL, or a torn short WAL with no `-shm`, with no live peer, still reads
//!   the database file's rows.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use fsqlite::Connection;

const STOCK_ENV: &str = "FSQLITE_HEADERLESS_WAL_STOCK_SCRIPT";
/// Separates the stock child's steps; no SQL here contains it.
const STEP_SEPARATOR: char = '\u{1e}';
const SQLITE_BUSY: i32 = 5;
/// The bead's bound for an open plus a read that stock answers in about 3 ms.
/// The defect waited about 35 s.
const READ_BOUND: Duration = Duration::from_secs(1);

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

fn wal_path(db: &Path) -> PathBuf {
    PathBuf::from(format!("{}-wal", db.display()))
}

fn shm_path(db: &Path) -> PathBuf {
    PathBuf::from(format!("{}-shm", db.display()))
}

fn wal_len(db: &Path) -> Option<u64> {
    std::fs::metadata(wal_path(db)).ok().map(|meta| meta.len())
}

/// A stock process running `steps` in order on one connection, kept open
/// until the last step. `wait:NAME` waits for the parent's file `NAME`,
/// `signal:NAME` creates one, `ckpt:MODE` runs `PRAGMA wal_checkpoint(MODE)`
/// and requires it to complete, and anything else is SQL for `execute_batch`.
#[test]
fn stock_script_child() {
    let Ok(spec) = std::env::var(STOCK_ENV) else {
        return;
    };
    let mut parts = spec.splitn(3, '|');
    let db = parts.next().expect("db").to_owned();
    let dir = PathBuf::from(parts.next().expect("dir"));
    let steps = parts.next().expect("steps").to_owned();
    let stock = rusqlite::Connection::open(&db).expect("stock open");
    stock
        .busy_timeout(Duration::from_secs(5))
        .expect("stock busy_timeout");
    for step in steps.split(STEP_SEPARATOR) {
        if let Some(name) = step.strip_prefix("wait:") {
            assert!(
                wait_for(&dir.join(name), Duration::from_secs(120)),
                "parent never signalled {name}"
            );
        } else if let Some(name) = step.strip_prefix("signal:") {
            std::fs::write(dir.join(name), b"").expect("signal");
        } else if let Some(mode) = step.strip_prefix("ckpt:") {
            let (busy, log, checkpointed): (i64, i64, i64) = stock
                .query_row(&format!("PRAGMA wal_checkpoint({mode})"), [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .expect("stock checkpoint");
            assert_eq!(
                busy, 0,
                "stock {mode} checkpoint did not complete: log {log}, checkpointed {checkpointed}"
            );
        } else {
            stock
                .execute_batch(step)
                .unwrap_or_else(|error| panic!("stock step {step:?}: {error}"));
        }
    }
    drop(stock);
}

struct StockScript {
    child: Option<Child>,
    dir: PathBuf,
}

impl StockScript {
    fn spawn(db: &Path, dir: &Path, steps: &[&str]) -> Self {
        let child = Command::new(std::env::current_exe().expect("test binary path"))
            .args([
                "--exact",
                "stock_script_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(
                STOCK_ENV,
                format!(
                    "{}|{}|{}",
                    db.display(),
                    dir.display(),
                    steps.join(&STEP_SEPARATOR.to_string())
                ),
            )
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn stock");
        Self {
            child: Some(child),
            dir: dir.to_path_buf(),
        }
    }

    /// Wait for the child's `signal:NAME`, failing fast if it died first.
    fn wait(&mut self, name: &str) {
        let started = Instant::now();
        let path = self.dir.join(name);
        while !path.exists() {
            let child = self.child.as_mut().expect("stock child");
            if child.try_wait().expect("stock status").is_some() {
                let output = self
                    .child
                    .take()
                    .expect("child")
                    .wait_with_output()
                    .expect("output");
                panic!(
                    "the stock child exited before {name}: {}\n{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            assert!(
                started.elapsed() < Duration::from_secs(120),
                "stock never reached {name}"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn signal(&self, name: &str) {
        std::fs::write(self.dir.join(name), b"").expect("signal stock");
    }

    fn finish(mut self) {
        let output = self
            .child
            .take()
            .expect("child")
            .wait_with_output()
            .expect("stock output");
        assert!(
            output.status.success(),
            "stock child failed: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

impl Drop for StockScript {
    fn drop(&mut self) {
        // A failing parent assertion must not leave a stock connection
        // holding the database until its handshake timeout expires.
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn seed(db: &Path) {
    rusqlite::Connection::open(db)
        .expect("seed open")
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE t(id INTEGER PRIMARY KEY, s TEXT NOT NULL);
             CREATE TABLE h(id INTEGER PRIMARY KEY, v INTEGER NOT NULL);
             INSERT INTO t VALUES (1, 'seed');
             INSERT INTO h VALUES (1, 0);",
        )
        .expect("seed");
    assert_eq!(wal_len(db), None, "the seed's last close removes its WAL");
}

fn stock_ids(db: &Path) -> Vec<i64> {
    let stock = rusqlite::Connection::open(db).expect("stock open");
    let mut stmt = stock
        .prepare("SELECT id FROM t ORDER BY id")
        .expect("prepare");
    stmt.query_map([], |row| row.get(0))
        .expect("query")
        .collect::<rusqlite::Result<_>>()
        .expect("ids")
}

fn stock_h(db: &Path) -> i64 {
    rusqlite::Connection::open(db)
        .expect("stock open")
        .query_row("SELECT v FROM h WHERE id = 1", [], |row| row.get(0))
        .expect("h")
}

fn stock_integrity(db: &Path) -> String {
    rusqlite::Connection::open(db)
        .expect("stock open")
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity_check")
}

/// Stock recovers a copy of the database and its WAL, with no `-shm`, from
/// scratch: it validates the WAL header, its salts and every frame checksum.
fn stock_ids_recovered_from_a_copy(db: &Path, copy_dir: &Path) -> Vec<i64> {
    std::fs::create_dir_all(copy_dir).expect("copy dir");
    let copy = copy_dir.join("copy.db");
    std::fs::copy(db, &copy).expect("copy db");
    std::fs::copy(wal_path(db), wal_path(&copy)).expect("copy wal");
    assert!(!shm_path(&copy).exists(), "the copy has no index to trust");
    stock_ids(&copy)
}

async fn ids_and_h(conn: &Connection) -> (Vec<i64>, i64) {
    let ids = conn
        .query("SELECT id FROM t ORDER BY id")
        .await
        .expect("select ids")
        .iter()
        .map(|row| match row.values()[0] {
            fsqlite::SqliteValue::Integer(id) => id,
            ref other => panic!("id is not an integer: {other:?}"),
        })
        .collect();
    let h = match conn
        .query("SELECT v FROM h WHERE id = 1")
        .await
        .expect("select h")[0]
        .values()[0]
    {
        fsqlite::SqliteValue::Integer(v) => v,
        ref other => panic!("h is not an integer: {other:?}"),
    };
    (ids, h)
}

/// Open, set busy_timeout 1000, read; report the elapsed time with the rows.
async fn open_and_read(path: &str) -> (Connection, Duration, Vec<i64>, i64) {
    let started = Instant::now();
    let conn = Connection::open(path)
        .await
        .expect("open beside a live stock connection");
    conn.execute("PRAGMA busy_timeout=1000")
        .await
        .expect("busy_timeout");
    let (ids, h) = ids_and_h(&conn).await;
    (conn, started.elapsed(), ids, h)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    /// The seed left no `-wal`; the stock holder's own transaction created a
    /// zero-byte one.
    Cold,
    /// A stock commit, then `wal_checkpoint(TRUNCATE)` on the holder's
    /// connection: a zero-byte WAL whose index carries the next salts.
    Truncated,
    /// Control: a stock commit left a WAL with a header and a frame.
    WithFrames,
}

impl Shape {
    /// Steps run before the holder's transaction, and `h` once they commit.
    fn setup(self) -> (&'static [&'static str], i64) {
        match self {
            Self::Cold => (&[], 0),
            Self::Truncated => (
                &["UPDATE h SET v = v + 1 WHERE id = 1;", "ckpt:TRUNCATE"],
                1,
            ),
            Self::WithFrames => (&["UPDATE h SET v = v + 1 WHERE id = 1;"], 1),
        }
    }

    fn assert_wal(self, len: Option<u64>) {
        if self == Self::WithFrames {
            assert!(
                len.is_some_and(|len| len > 32),
                "shape {self:?}: the WAL has a frame: {len:?}"
            );
        } else {
            assert_eq!(len, Some(0), "shape {self:?}: the WAL has no header");
        }
    }
}

const SHAPES: [Shape; 3] = [Shape::Cold, Shape::Truncated, Shape::WithFrames];

/// A stock writer holds BEGIN IMMEDIATE plus an uncommitted UPDATE. A fresh
/// fsqlite connection opens and reads the committed rows at once. Its write
/// while the stock writer holds WAL_WRITE is refused with BUSY, as stock's
/// own is, and writes nothing. Once the writer commits, the same connection
/// writes in both transaction shapes, a stock write lands on top, and each
/// engine sees every row.
#[test]
fn fresh_open_reads_at_once_beside_a_stock_writer() {
    if std::env::var_os(STOCK_ENV).is_some() {
        return;
    }
    for shape in SHAPES {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("writer.db");
        seed(&db);
        let (setup, committed_h) = shape.setup();
        let mut steps = setup.to_vec();
        steps.extend([
            "BEGIN IMMEDIATE; UPDATE h SET v = v + 100 WHERE id = 1;",
            "signal:held",
            "wait:release",
            "COMMIT;",
            "signal:released",
            "wait:write",
            "INSERT INTO t VALUES (4, 'stock');",
            "signal:written",
            "wait:done",
        ]);
        let mut stock = StockScript::spawn(&db, dir.path(), &steps);
        stock.wait("held");
        let held_wal = wal_len(&db);
        shape.assert_wal(held_wal);
        let path = db.to_str().expect("utf8").to_owned();
        let db_in = db.clone();
        let stock_slot = &mut stock;
        asupersync::test_utils::run_test(|| async move {
            let (conn, elapsed, ids, h) = open_and_read(&path).await;
            assert!(
                elapsed < READ_BOUND,
                "{shape:?}: open + read took {elapsed:?} beside a stock writer"
            );
            assert_eq!(
                (ids, h),
                (vec![1], committed_h),
                "{shape:?}: the committed rows, without the uncommitted UPDATE"
            );
            assert_eq!(
                wal_len(&db_in),
                held_wal,
                "{shape:?}: a reader writes no WAL header"
            );
            let refused = conn
                .execute("INSERT INTO t VALUES (2, 'behind the stock writer')")
                .await
                .expect_err("a write behind a stock writer that holds WAL_WRITE");
            assert_eq!(
                refused.extended_error_code(),
                SQLITE_BUSY,
                "{shape:?}: stock answers SQLITE_BUSY here: {refused}"
            );
            assert_eq!(
                wal_len(&db_in),
                held_wal,
                "{shape:?}: the refused write wrote no WAL header"
            );
            stock_slot.signal("release");
            stock_slot.wait("released");
            conn.execute("BEGIN").await.expect("BEGIN");
            conn.execute("INSERT INTO t VALUES (2, 'explicit')")
                .await
                .expect("INSERT");
            conn.execute("COMMIT")
                .await
                .expect("COMMIT after the stock writer committed");
            conn.execute("INSERT INTO t VALUES (3, 'autocommit')")
                .await
                .expect("autocommit INSERT");
            stock_slot.signal("write");
            stock_slot.wait("written");
            assert_eq!(
                ids_and_h(&conn).await,
                (vec![1, 2, 3, 4], committed_h + 100),
                "{shape:?}: fsqlite sees every row"
            );
            conn.close().await.expect("close");
        });
        stock.signal("done");
        stock.finish();
        assert_eq!(stock_ids(&db), vec![1, 2, 3, 4], "{shape:?}");
        assert_eq!(stock_h(&db), committed_h + 100, "{shape:?}");
        assert_eq!(stock_integrity(&db), "ok", "{shape:?}");
    }
}

/// A stock connection holds a READ transaction. A fresh fsqlite connection
/// opens and reads at once and commits while that reader stays open (a WAL
/// reader never blocks a writer). On a header-less WAL its first commit
/// writes the WAL header: stock recovers a copy of that WAL from scratch,
/// and a stock write lands on top of its frames.
#[test]
fn fresh_open_reads_and_writes_beside_a_stock_reader() {
    if std::env::var_os(STOCK_ENV).is_some() {
        return;
    }
    for shape in SHAPES {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("reader.db");
        seed(&db);
        let (setup, committed_h) = shape.setup();
        let mut steps = setup.to_vec();
        steps.extend([
            "BEGIN; SELECT count(*) FROM h;",
            "signal:held",
            "wait:release",
            "COMMIT;",
            "signal:released",
            "wait:write",
            "INSERT INTO t VALUES (4, 'stock');",
            "signal:written",
            "wait:done",
        ]);
        let mut stock = StockScript::spawn(&db, dir.path(), &steps);
        stock.wait("held");
        let held_wal = wal_len(&db);
        shape.assert_wal(held_wal);
        let path = db.to_str().expect("utf8").to_owned();
        let db_in = db.clone();
        let copy_dir = dir.path().join("recovered");
        let stock_slot = &mut stock;
        asupersync::test_utils::run_test(|| async move {
            let (conn, elapsed, ids, h) = open_and_read(&path).await;
            assert!(
                elapsed < READ_BOUND,
                "{shape:?}: open + read took {elapsed:?} beside a stock reader"
            );
            assert_eq!((ids, h), (vec![1], committed_h), "{shape:?}");
            assert_eq!(
                wal_len(&db_in),
                held_wal,
                "{shape:?}: a reader writes no WAL header"
            );
            conn.execute("BEGIN").await.expect("BEGIN");
            conn.execute("INSERT INTO t VALUES (2, 'explicit')")
                .await
                .expect("INSERT");
            conn.execute("COMMIT")
                .await
                .expect("COMMIT beside an open stock reader");
            conn.execute("INSERT INTO t VALUES (3, 'autocommit')")
                .await
                .expect("autocommit INSERT beside an open stock reader");
            assert!(
                wal_len(&db_in).is_some_and(|len| len > 32),
                "{shape:?}: the commits are in the WAL"
            );
            assert_eq!(
                stock_ids_recovered_from_a_copy(&db_in, &copy_dir),
                vec![1, 2, 3],
                "{shape:?}: stock recovers the WAL fsqlite wrote"
            );
            stock_slot.signal("release");
            stock_slot.wait("released");
            stock_slot.signal("write");
            stock_slot.wait("written");
            assert_eq!(
                ids_and_h(&conn).await,
                (vec![1, 2, 3, 4], committed_h),
                "{shape:?}: fsqlite sees the stock write on top of its own"
            );
            conn.close().await.expect("close");
        });
        stock.signal("done");
        stock.finish();
        assert_eq!(stock_ids(&db), vec![1, 2, 3, 4], "{shape:?}");
        assert_eq!(stock_integrity(&db), "ok", "{shape:?}");
    }
}

/// An fsqlite connection is already open, on a WAL with a header, when a
/// stock connection commits, checkpoints and stays open, idle or inside a
/// READ transaction. TRUNCATE leaves a zero-byte WAL; RESTART keeps the
/// header and its stock's next writer restarts the log. The fsqlite
/// connection keeps reading and committing, its rows are durable, and a stock
/// write afterwards lands on top of them.
#[test]
fn open_connection_reads_and_writes_after_a_stock_truncate_or_restart() {
    if std::env::var_os(STOCK_ENV).is_some() {
        return;
    }
    for mode in ["TRUNCATE", "RESTART"] {
        for stock_reader in [false, true] {
            let case = format!("{mode}, stock reader open: {stock_reader}");
            let dir = tempfile::tempdir().expect("tempdir");
            let db = dir.path().join("open.db");
            seed(&db);
            let path = db.to_str().expect("utf8").to_owned();
            let work = dir.path().to_path_buf();
            let db_in = db.clone();
            let mut stock = None;
            let stock_slot = &mut stock;
            let case_in = case.clone();
            asupersync::test_utils::run_test(|| async move {
                let case = case_in;
                let conn = Connection::open(&path).await.expect("open");
                conn.execute("PRAGMA busy_timeout=1000")
                    .await
                    .expect("busy_timeout");
                assert_eq!(ids_and_h(&conn).await, (vec![1], 0), "{case}");
                let ckpt = format!("ckpt:{mode}");
                let mut steps = vec!["UPDATE h SET v = v + 1 WHERE id = 1;", ckpt.as_str()];
                if stock_reader {
                    steps.push("BEGIN; SELECT count(*) FROM h;");
                }
                steps.extend(["signal:checkpointed", "wait:release"]);
                if stock_reader {
                    steps.push("COMMIT;");
                }
                steps.extend([
                    "signal:released",
                    "wait:write",
                    "INSERT INTO t VALUES (4, 'stock');",
                    "signal:written",
                    "wait:done",
                ]);
                let script = stock_slot.insert(StockScript::spawn(&db_in, &work, &steps));
                script.wait("checkpointed");
                let len = wal_len(&db_in);
                if mode == "TRUNCATE" {
                    assert_eq!(len, Some(0), "{case}: shape: a zero-byte WAL");
                } else {
                    assert!(
                        len.is_some_and(|len| len > 32),
                        "{case}: shape: RESTART keeps the WAL: {len:?}"
                    );
                }
                assert_eq!(
                    ids_and_h(&conn).await,
                    (vec![1], 1),
                    "{case}: the read after the checkpoint sees the stock commit"
                );
                conn.execute("INSERT INTO t VALUES (2, 'autocommit')")
                    .await
                    .expect("autocommit INSERT after the stock checkpoint");
                conn.execute("BEGIN").await.expect("BEGIN");
                conn.execute("INSERT INTO t VALUES (3, 'explicit')")
                    .await
                    .expect("INSERT");
                conn.execute("COMMIT")
                    .await
                    .expect("COMMIT after the stock checkpoint");
                assert_eq!(ids_and_h(&conn).await, (vec![1, 2, 3], 1), "{case}");
                script.signal("release");
                script.wait("released");
                script.signal("write");
                script.wait("written");
                assert_eq!(
                    ids_and_h(&conn).await,
                    (vec![1, 2, 3, 4], 1),
                    "{case}: fsqlite sees the stock write on top of its own"
                );
                conn.close().await.expect("close");
            });
            let stock = stock.expect("stock spawned");
            stock.signal("done");
            stock.finish();
            assert_eq!(stock_ids(&db), vec![1, 2, 3, 4], "{case}");
            assert_eq!(stock_h(&db), 1, "{case}");
            assert_eq!(stock_integrity(&db), "ok", "{case}");
        }
    }
}

/// Negative cases with no live peer. A stale `-shm` whose index publishes
/// frames over a zero-byte WAL, and a torn WAL shorter than its header with
/// no `-shm`, both read as the database file alone, as stock reads them (its
/// answer, on a copy, is the oracle); a write then commits. Binding the
/// zero-byte WAL to the stale index's frames would read pages that are not
/// there.
#[test]
fn headerless_wal_with_no_live_peer_reads_the_database_file() {
    if std::env::var_os(STOCK_ENV).is_some() {
        return;
    }
    for stale_index in [true, false] {
        let case = if stale_index {
            "stale index over a zero-byte WAL"
        } else {
            "torn short WAL, no -shm"
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("idle.db");
        seed(&db);
        if stale_index {
            let stock = rusqlite::Connection::open(&db).expect("stock open");
            stock
                .execute_batch("INSERT INTO t VALUES (2, 'wal'); INSERT INTO t VALUES (3, 'wal');")
                .expect("stock commits into the WAL");
            let index = std::fs::read(shm_path(&db)).expect("read the live index");
            let busy: i64 = stock
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))
                .expect("stock TRUNCATE");
            assert_eq!(busy, 0, "{case}: the checkpoint completed");
            drop(stock);
            assert_eq!(
                wal_len(&db),
                None,
                "{case}: the last stock close removes the WAL"
            );
            std::fs::write(wal_path(&db), b"").expect("zero-byte WAL");
            std::fs::write(shm_path(&db), &index).expect("stale index");
        } else {
            std::fs::write(wal_path(&db), [0xA5_u8; 10]).expect("torn WAL");
            assert!(!shm_path(&db).exists(), "{case}: no -shm");
        }
        let oracle_dir = dir.path().join("oracle");
        std::fs::create_dir_all(&oracle_dir).expect("oracle dir");
        let oracle = oracle_dir.join("idle.db");
        std::fs::copy(&db, &oracle).expect("copy db");
        std::fs::copy(wal_path(&db), wal_path(&oracle)).expect("copy wal");
        if stale_index {
            std::fs::copy(shm_path(&db), shm_path(&oracle)).expect("copy shm");
        }
        let expected = stock_ids(&oracle);
        assert_eq!(
            expected,
            if stale_index { vec![1, 2, 3] } else { vec![1] },
            "{case}: stock's answer"
        );
        let path = db.to_str().expect("utf8").to_owned();
        let next = expected.last().copied().expect("rows") + 1;
        let expected_in = expected.clone();
        asupersync::test_utils::run_test(|| async move {
            let (conn, _, ids, _) = open_and_read(&path).await;
            assert_eq!(ids, expected_in, "{case}: fsqlite reads what stock reads");
            conn.execute(&format!("INSERT INTO t VALUES ({next}, 'after')"))
                .await
                .expect("INSERT");
            conn.close().await.expect("close");
        });
        let mut after = expected;
        after.push(next);
        assert_eq!(stock_ids(&db), after, "{case}");
        assert_eq!(stock_integrity(&db), "ok", "{case}");
    }
}
