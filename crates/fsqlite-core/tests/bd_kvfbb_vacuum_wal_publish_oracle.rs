#![recursion_limit = "512"]
#![cfg(unix)]

//! bd-kvfbb: In-place `VACUUM` on a WAL-mode database must publish the rebuilt
//! image through the WAL as one transaction (as stock SQLite does), so it
//! succeeds beside idle and active-reading peers — both `fsqlite` and stock
//! SQLite — while preserving active reader snapshots, shrinking the database
//! file once checkpointed, and rejecting stale concurrent MVCC writers.

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

const PEER_MODE_ENV: &str = "FSQLITE_BD_KVFBB_PEER_MODE";
const PEER_DB_ENV: &str = "FSQLITE_BD_KVFBB_PEER_DB";
const PEER_TEST_IDLE: &str = "vacuum_in_place_succeeds_beside_idle_fsqlite_peer_process";
const PEER_TEST_READER: &str = "vacuum_in_place_preserves_active_cross_process_reader_snapshot";
const READY_PREFIX: &str = "BD_KVFBB_READY";
const REPLY_PREFIX: &str = "BD_KVFBB_REPLY";

fn int_cell(rows: &[Row]) -> i64 {
    match rows[0].values()[0] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected integer cell, got {other:?}"),
    }
}

fn int_pair(rows: &[Row]) -> (i64, i64) {
    let a = match rows[0].values()[0] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected first integer cell, got {other:?}"),
    };
    let b = match rows[0].values()[1] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected second integer cell, got {other:?}"),
    };
    (a, b)
}

async fn seed_sparse_wal_db(db_str: &str, total_rows: i64, keep_rows: i64) -> (Connection, i64) {
    let conn = Connection::open(db_str).await.expect("open db");
    conn.query("PRAGMA journal_mode = WAL;")
        .await
        .expect("enable WAL");
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT);")
        .await
        .expect("create table");
    conn.execute(&format!(
        "INSERT INTO t \
         WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {total_rows}) \
         SELECT i, printf('%0900d', i) FROM n;"
    ))
    .await
    .expect("seed rows");
    conn.execute(&format!("DELETE FROM t WHERE id > {keep_rows};"))
        .await
        .expect("delete tail rows");
    conn.query("PRAGMA wal_checkpoint(TRUNCATE);")
        .await
        .expect("checkpoint after seed");
    let pre_pages = int_cell(
        &conn
            .query("PRAGMA page_count;")
            .await
            .expect("pre page_count"),
    );
    assert!(
        pre_pages >= 40,
        "expected bloated pre-vacuum database (>= 40 pages), got {pre_pages}"
    );
    (conn, pre_pages)
}

fn verify_with_stock(db_path: &Path, expected_count: i64, expected_auto_vacuum: Option<i64>) {
    let stock = rusqlite::Connection::open(db_path).expect("stock open");
    let integrity: String = stock
        .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
        .expect("stock integrity_check");
    assert_eq!(integrity, "ok", "stock SQLite integrity_check failed");
    let count: i64 = stock
        .query_row("SELECT count(*) FROM t;", [], |row| row.get(0))
        .expect("stock row count");
    assert_eq!(count, expected_count, "stock SQLite row count mismatch");
    if let Some(expected_av) = expected_auto_vacuum {
        let actual_av: i64 = stock
            .query_row("PRAGMA auto_vacuum;", [], |row| row.get(0))
            .expect("stock auto_vacuum");
        assert_eq!(actual_av, expected_av, "stock auto_vacuum mismatch");
    }
}

struct PeerHandle {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl PeerHandle {
    fn spawn(test_name: &str, mode: &str, db_str: &str) -> (Self, String) {
        let mut child = Command::new(std::env::current_exe().expect("current_exe"))
            .args([test_name, "--exact", "--nocapture", "--test-threads=1"])
            .env(PEER_MODE_ENV, mode)
            .env(PEER_DB_ENV, db_str)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn peer process");
        let stdin = child.stdin.take().expect("peer stdin");
        let mut stdout = BufReader::new(child.stdout.take().expect("peer stdout"));
        let ready = Self::wait_for_prefix(&mut stdout, READY_PREFIX);
        (
            Self {
                child,
                stdin,
                stdout,
            },
            ready,
        )
    }

    fn wait_for_prefix(stdout: &mut BufReader<ChildStdout>, prefix: &str) -> String {
        let started = Instant::now();
        loop {
            let mut line = String::new();
            let read = stdout.read_line(&mut line).expect("read peer stdout");
            assert!(read > 0, "peer exited before sending {prefix}");
            if let Some(at) = line.find(prefix) {
                return line[at + prefix.len()..].trim().to_owned();
            }
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "timed out waiting for peer {prefix}"
            );
        }
    }

    fn command(&mut self, cmd: &str) -> String {
        writeln!(self.stdin, "{cmd}").expect("write peer command");
        self.stdin.flush().expect("flush peer command");
        Self::wait_for_prefix(&mut self.stdout, REPLY_PREFIX)
    }

    fn finish(mut self) {
        let _ = writeln!(self.stdin, "QUIT");
        let _ = self.stdin.flush();
        drop(self.stdin);
        let status = self.child.wait().expect("wait peer");
        assert!(status.success(), "peer exited with failure: {status}");
    }
}

fn run_idle_peer(db_str: &str) {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(db_str).await.expect("peer open");
        let count = int_cell(
            &conn
                .query("SELECT count(*) FROM t;")
                .await
                .expect("peer initial read"),
        );
        let pages = int_cell(
            &conn
                .query("PRAGMA page_count;")
                .await
                .expect("peer initial page_count"),
        );
        println!("{READY_PREFIX} {count} {pages}");
        std::io::stdout().flush().expect("flush ready");

        let mut lines = BufReader::new(std::io::stdin()).lines();
        while let Some(Ok(cmd)) = lines.next() {
            match cmd.trim() {
                "CHECK_AND_WRITE" => {
                    let after_count = int_cell(
                        &conn
                            .query("SELECT count(*) FROM t;")
                            .await
                            .expect("peer post-vacuum count"),
                    );
                    let after_pages = int_cell(
                        &conn
                            .query("PRAGMA page_count;")
                            .await
                            .expect("peer post-vacuum page_count"),
                    );
                    conn.execute("INSERT INTO t VALUES (999999, 'from-idle-peer');")
                        .await
                        .expect("peer post-vacuum insert");
                    let final_count = int_cell(
                        &conn
                            .query("SELECT count(*) FROM t;")
                            .await
                            .expect("peer final count"),
                    );
                    println!("{REPLY_PREFIX} {after_count} {after_pages} {final_count}");
                    std::io::stdout().flush().expect("flush reply");
                }
                "QUIT" => break,
                other => panic!("unexpected idle peer command: {other}"),
            }
        }
        conn.close().await.expect("peer close");
    });
}

fn run_active_reader_peer(db_str: &str) {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(db_str).await.expect("reader peer open");
        conn.execute("BEGIN;").await.expect("reader BEGIN");
        let (count, sum) = int_pair(
            &conn
                .query("SELECT count(*), sum(id) FROM t;")
                .await
                .expect("reader initial snapshot query"),
        );
        println!("{READY_PREFIX} {count} {sum}");
        std::io::stdout().flush().expect("flush ready");

        let mut lines = BufReader::new(std::io::stdin()).lines();
        while let Some(Ok(cmd)) = lines.next() {
            match cmd.trim() {
                "REREAD_IN_TXN" => {
                    let (still_count, still_sum) = int_pair(
                        &conn
                            .query("SELECT count(*), sum(id) FROM t;")
                            .await
                            .expect("reader snapshot query after vacuum"),
                    );
                    println!("{REPLY_PREFIX} {still_count} {still_sum}");
                    std::io::stdout().flush().expect("flush reply");
                }
                "COMMIT_AND_READ" => {
                    conn.execute("COMMIT;").await.expect("reader COMMIT");
                    let (new_count, new_sum) = int_pair(
                        &conn
                            .query("SELECT count(*), sum(id) FROM t;")
                            .await
                            .expect("reader post-commit query"),
                    );
                    let new_pages = int_cell(
                        &conn
                            .query("PRAGMA page_count;")
                            .await
                            .expect("reader post-commit page_count"),
                    );
                    println!("{REPLY_PREFIX} {new_count} {new_sum} {new_pages}");
                    std::io::stdout().flush().expect("flush reply");
                }
                "QUIT" => break,
                other => panic!("unexpected reader peer command: {other}"),
            }
        }
        conn.close().await.expect("reader peer close");
    });
}

#[test]
fn vacuum_in_place_succeeds_beside_idle_fsqlite_peer_process() {
    if std::env::var(PEER_MODE_ENV).ok().as_deref() == Some("idle") {
        let db = std::env::var(PEER_DB_ENV).expect("peer db env");
        run_idle_peer(&db);
        return;
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("idle_peer.db");
    let db_str = db.to_str().expect("utf-8 path").to_owned();

    asupersync::test_utils::run_test(|| async {
        let (conn, pre_pages) = seed_sparse_wal_db(&db_str, 400, 10).await;

        let (mut peer, ready) = PeerHandle::spawn(PEER_TEST_IDLE, "idle", &db_str);
        assert_eq!(ready, format!("10 {pre_pages}"));

        conn.execute("PRAGMA busy_timeout = 3000;")
            .await
            .expect("busy_timeout");
        let started = Instant::now();
        conn.execute("VACUUM;").await.unwrap_or_else(|err| {
            panic!(
                "bd-kvfbb: in-place VACUUM beside idle fsqlite peer failed after {:?}: {err}",
                started.elapsed()
            )
        });

        let post_pages = int_cell(
            &conn
                .query("PRAGMA page_count;")
                .await
                .expect("post page_count"),
        );
        let post_freelist = int_cell(
            &conn
                .query("PRAGMA freelist_count;")
                .await
                .expect("post freelist_count"),
        );
        assert!(
            post_pages < pre_pages / 2,
            "expected compacted page_count (< {}), got {post_pages}",
            pre_pages / 2
        );
        assert_eq!(post_freelist, 0, "expected 0 freelist pages after VACUUM");

        // While both connections remain open, the idle peer must observe the
        // compacted extent and commit a subsequent write cleanly.
        let reply = peer.command("CHECK_AND_WRITE");
        assert_eq!(
            reply,
            format!("10 {post_pages} 11"),
            "idle peer did not observe compacted extent or failed post-vacuum write"
        );
        peer.finish();

        let main_count = int_cell(
            &conn
                .query("SELECT count(*) FROM t;")
                .await
                .expect("main sees peer write"),
        );
        assert_eq!(main_count, 11);
        conn.query("PRAGMA wal_checkpoint(TRUNCATE);")
            .await
            .expect("final truncate checkpoint");
        let final_pages = int_cell(
            &conn
                .query("PRAGMA page_count;")
                .await
                .expect("final page_count"),
        );
        let final_len = std::fs::metadata(&db).expect("db metadata").len();
        assert_eq!(
            final_len,
            u64::try_from(final_pages).expect("non-negative pages") * 4096,
            "expected db file truncated to compacted extent on disk"
        );

        conn.close().await.expect("close main");
        verify_with_stock(&db, 11, None);
    });
}

#[test]
fn vacuum_in_place_succeeds_beside_idle_stock_rusqlite_peer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("idle_stock.db");
    let db_str = db.to_str().expect("utf-8 path").to_owned();

    asupersync::test_utils::run_test(|| async {
        let (conn, pre_pages) = seed_sparse_wal_db(&db_str, 400, 10).await;

        let stock_peer = rusqlite::Connection::open(&db).expect("open stock peer");
        let stock_pre_count: i64 = stock_peer
            .query_row("SELECT count(*) FROM t;", [], |r| r.get(0))
            .expect("stock pre read");
        assert_eq!(stock_pre_count, 10);

        conn.execute("PRAGMA busy_timeout = 3000;")
            .await
            .expect("busy_timeout");
        conn.execute("VACUUM;").await.unwrap_or_else(|err| {
            panic!("bd-kvfbb: in-place VACUUM beside idle stock rusqlite peer failed: {err}")
        });

        let post_pages = int_cell(
            &conn
                .query("PRAGMA page_count;")
                .await
                .expect("post page_count"),
        );
        assert!(
            post_pages < pre_pages / 2,
            "expected compacted page_count (< {}), got {post_pages}",
            pre_pages / 2
        );

        // Already-open stock SQLite peer must see the compacted image cleanly.
        let stock_pages: i64 = stock_peer
            .query_row("PRAGMA page_count;", [], |r| r.get(0))
            .expect("stock post page_count");
        let stock_count: i64 = stock_peer
            .query_row("SELECT count(*) FROM t;", [], |r| r.get(0))
            .expect("stock post count");
        let stock_integrity: String = stock_peer
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .expect("stock integrity_check");
        assert_eq!(stock_pages, post_pages);
        assert_eq!(stock_count, 10);
        assert_eq!(stock_integrity, "ok");

        stock_peer
            .execute("INSERT INTO t VALUES (777, 'from-stock');", [])
            .expect("stock write after fsqlite vacuum");
        drop(stock_peer);

        let after_stock_write = int_cell(
            &conn
                .query("SELECT count(*) FROM t;")
                .await
                .expect("fsqlite sees stock write"),
        );
        assert_eq!(after_stock_write, 11);
        conn.close().await.expect("close main");
        verify_with_stock(&db, 11, None);
    });
}

#[test]
fn vacuum_in_place_preserves_active_cross_process_reader_snapshot() {
    if std::env::var(PEER_MODE_ENV).ok().as_deref() == Some("reader") {
        let db = std::env::var(PEER_DB_ENV).expect("peer db env");
        run_active_reader_peer(&db);
        return;
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("active_reader.db");
    let db_str = db.to_str().expect("utf-8 path").to_owned();

    asupersync::test_utils::run_test(|| async {
        let (conn, pre_pages) = seed_sparse_wal_db(&db_str, 400, 10).await;
        let pre_file_len = std::fs::metadata(&db).expect("pre db metadata").len();

        let (mut peer, ready) = PeerHandle::spawn(PEER_TEST_READER, "reader", &db_str);
        assert_eq!(ready, "10 55");

        conn.execute("PRAGMA busy_timeout = 3000;")
            .await
            .expect("busy_timeout");
        conn.execute("VACUUM;").await.unwrap_or_else(|err| {
            panic!("bd-kvfbb: in-place VACUUM beside active reading fsqlite peer failed: {err}")
        });

        let post_pages = int_cell(
            &conn
                .query("PRAGMA page_count;")
                .await
                .expect("post page_count"),
        );
        assert!(post_pages < pre_pages / 2);

        // Main-file bytes must NOT be truncated underneath the pinned reader.
        let during_reader_len = std::fs::metadata(&db).expect("during metadata").len();
        assert_eq!(
            during_reader_len, pre_file_len,
            "db file must stay untruncated while active reader pins pre-vacuum snapshot"
        );

        // Commit another row after VACUUM while the reader still holds its snapshot.
        conn.execute("INSERT INTO t VALUES (1000, 'post-vacuum');")
            .await
            .expect("post-vacuum insert");

        // The active reader must still observe its exact pre-VACUUM snapshot.
        let still = peer.command("REREAD_IN_TXN");
        assert_eq!(still, "10 55", "active reader snapshot was disturbed");

        // After COMMIT, the reader must see both the compacted page_count and new row.
        let post_commit_pages = int_cell(
            &conn
                .query("PRAGMA page_count;")
                .await
                .expect("post-insert page_count"),
        );
        let after = peer.command("COMMIT_AND_READ");
        assert_eq!(after, format!("11 1055 {post_commit_pages}"));
        peer.finish();

        // Now that the reader released its read mark, TRUNCATE checkpoint must
        // backfill the compacted image and shrink `.db` on disk.
        conn.query("PRAGMA wal_checkpoint(TRUNCATE);")
            .await
            .expect("truncate checkpoint after reader released");
        let shrunk_len = std::fs::metadata(&db).expect("shrunk metadata").len();
        assert_eq!(
            shrunk_len,
            u64::try_from(post_commit_pages).expect("non-negative") * 4096,
            "expected db file shrunk to compacted size after reader released"
        );

        conn.close().await.expect("close main");
        verify_with_stock(&db, 11, None);
    });
}

#[test]
fn vacuum_in_place_preserves_active_stock_rusqlite_reader_snapshot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("active_stock_reader.db");
    let db_str = db.to_str().expect("utf-8 path").to_owned();

    asupersync::test_utils::run_test(|| async {
        let (conn, pre_pages) = seed_sparse_wal_db(&db_str, 400, 10).await;
        let pre_file_len = std::fs::metadata(&db).expect("pre db metadata").len();

        let stock_reader = rusqlite::Connection::open(&db).expect("open stock reader");
        stock_reader.execute_batch("BEGIN;").expect("stock BEGIN");
        let (pre_count, pre_sum): (i64, i64) = stock_reader
            .query_row("SELECT count(*), sum(id) FROM t;", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .expect("stock snapshot read");
        assert_eq!((pre_count, pre_sum), (10, 55));

        conn.execute("PRAGMA busy_timeout = 3000;")
            .await
            .expect("busy_timeout");
        conn.execute("VACUUM;").await.unwrap_or_else(|err| {
            panic!("bd-kvfbb: in-place VACUUM beside active stock SQLite reader failed: {err}")
        });
        let post_pages = int_cell(
            &conn
                .query("PRAGMA page_count;")
                .await
                .expect("post page_count"),
        );
        assert!(post_pages < pre_pages / 2);

        // Main file must not have been truncated underneath the active stock reader.
        let during_len = std::fs::metadata(&db).expect("during metadata").len();
        assert_eq!(during_len, pre_file_len);

        conn.execute("INSERT INTO t VALUES (2000, 'after-vacuum');")
            .await
            .expect("insert after vacuum");

        // Stock reader must still see its exact pre-VACUUM snapshot cleanly.
        let (still_count, still_sum): (i64, i64) = stock_reader
            .query_row("SELECT count(*), sum(id) FROM t;", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .expect("stock snapshot reread after vacuum");
        assert_eq!((still_count, still_sum), (10, 55));

        stock_reader.execute_batch("COMMIT;").expect("stock COMMIT");
        let (after_count, after_sum): (i64, i64) = stock_reader
            .query_row("SELECT count(*), sum(id) FROM t;", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .expect("stock post-commit read");
        assert_eq!((after_count, after_sum), (11, 2055));
        let post_insert_pages = int_cell(
            &conn
                .query("PRAGMA page_count;")
                .await
                .expect("post-insert page_count"),
        );
        let stock_pages: i64 = stock_reader
            .query_row("PRAGMA page_count;", [], |r| r.get(0))
            .expect("stock post-commit page_count");
        assert_eq!(stock_pages, post_insert_pages);
        drop(stock_reader);

        conn.query("PRAGMA wal_checkpoint(TRUNCATE);")
            .await
            .expect("truncate checkpoint");
        let shrunk_len = std::fs::metadata(&db).expect("shrunk metadata").len();
        assert_eq!(
            shrunk_len,
            u64::try_from(post_insert_pages).expect("non-negative") * 4096
        );

        conn.close().await.expect("close main");
        verify_with_stock(&db, 11, None);
    });
}

#[test]
fn vacuum_in_place_refuses_stale_concurrent_mvcc_writer_on_high_page_after_shrink() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("mvcc_conflict.db");
    let db_str = db.to_str().expect("utf-8 path").to_owned();

    asupersync::test_utils::run_test(|| async {
        let (w1, _pre_pages) = seed_sparse_wal_db(&db_str, 500, 10).await;
        // Create a second table AFTER `t` grew large so `t_high`'s root page sits
        // well above the post-VACUUM compacted page count.
        w1.execute("CREATE TABLE t_high(id INTEGER PRIMARY KEY, val TEXT);")
            .await
            .expect("create t_high");
        w1.execute("INSERT INTO t_high VALUES (1, 'orig');")
            .await
            .expect("seed t_high");
        w1.query("PRAGMA wal_checkpoint(TRUNCATE);")
            .await
            .expect("checkpoint");
        let pre_pages = int_cell(&w1.query("PRAGMA page_count;").await.expect("pre pages"));

        // `w2` starts a concurrent writer at the pre-VACUUM snapshot and modifies
        // ONLY `t_high` (a page well above the post-VACUUM `new_page_count`).
        let w2 = Connection::open(&db_str).await.expect("open w2");
        w2.execute("BEGIN CONCURRENT;").await.expect("w2 begin");
        w2.execute("UPDATE t_high SET val = 'stale-high-page' WHERE id = 1;")
            .await
            .expect("w2 stage update on high page");

        // `w1` compacts the entire database through the WAL.
        w1.execute("VACUUM;")
            .await
            .expect("w1 VACUUM through WAL beside concurrent writer");
        let post_pages = int_cell(&w1.query("PRAGMA page_count;").await.expect("post pages"));
        assert!(
            post_pages < pre_pages / 2,
            "expected shrink: pre={pre_pages}, post={post_pages}"
        );

        // `w2`'s stale commit MUST fail closed rather than writing a frame past
        // the shrunk `new_page_count` or resurrecting pre-VACUUM pages.
        let commit_err = w2
            .execute("COMMIT;")
            .await
            .expect_err("stale concurrent writer committing after VACUUM shrink must fail");
        let _ = w2.execute("ROLLBACK;").await;
        assert!(
            format!("{commit_err:?}").contains("Busy"),
            "expected Busy/BusySnapshot refusal, got {commit_err:?}"
        );

        let val_rows = w1
            .query("SELECT val FROM t_high WHERE id = 1;")
            .await
            .expect("read t_high");
        assert_eq!(
            val_rows[0].values()[0],
            SqliteValue::Text("orig".into()),
            "VACUUMed image must not be corrupted by stale concurrent writer"
        );

        w2.close().await.expect("close w2");
        w1.close().await.expect("close w1");
        verify_with_stock(&db, 10, None);
    });
}

#[test]
fn vacuum_in_place_wal_auto_vacuum_full_transition_beside_idle_peer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("auto_vacuum_wal.db");
    let db_str = db.to_str().expect("utf-8 path").to_owned();

    asupersync::test_utils::run_test(|| async {
        let (conn, pre_pages) = seed_sparse_wal_db(&db_str, 400, 10).await;
        let peer = Connection::open(&db_str).await.expect("open idle peer");
        let _ = peer
            .query("SELECT count(*) FROM t;")
            .await
            .expect("peer initial read");

        conn.execute("PRAGMA auto_vacuum = FULL;")
            .await
            .expect("set pending auto_vacuum=FULL");
        conn.execute("VACUUM;")
            .await
            .expect("VACUUM applying auto_vacuum=FULL in WAL mode beside idle peer");

        let av = int_cell(
            &conn
                .query("PRAGMA auto_vacuum;")
                .await
                .expect("conn auto_vacuum"),
        );
        let post_pages = int_cell(
            &conn
                .query("PRAGMA page_count;")
                .await
                .expect("conn page_count"),
        );
        assert_eq!(av, 1, "expected auto_vacuum=FULL (1)");
        assert!(post_pages < pre_pages / 2);

        let peer_av = int_cell(
            &peer
                .query("PRAGMA auto_vacuum;")
                .await
                .expect("peer auto_vacuum"),
        );
        let peer_pages = int_cell(
            &peer
                .query("PRAGMA page_count;")
                .await
                .expect("peer page_count"),
        );
        assert_eq!(peer_av, 1);
        assert_eq!(peer_pages, post_pages);

        peer.close().await.expect("close peer");
        conn.close().await.expect("close conn");
        verify_with_stock(&db, 10, Some(1));
    });
}

#[test]
fn vacuum_in_place_succeeds_with_uncheckpointed_wal_pinned_by_reader() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("pinned_wal.db");
    let db_str = db.to_str().expect("utf-8 path").to_owned();

    asupersync::test_utils::run_test(|| async {
        let (conn, pre_pages) = seed_sparse_wal_db(&db_str, 400, 10).await;
        // Leave an uncheckpointed commit in the WAL so the WAL is non-empty
        // when the reading peer pins its snapshot and VACUUM begins.
        conn.execute("UPDATE t SET payload = 'pinned-in-wal' WHERE id = 1;")
            .await
            .expect("update in WAL");

        let stock_reader = rusqlite::Connection::open(&db).expect("open stock reader");
        stock_reader.execute_batch("BEGIN;").expect("stock BEGIN");
        let reader_val: String = stock_reader
            .query_row("SELECT payload FROM t WHERE id = 1;", [], |r| r.get(0))
            .expect("stock read WAL value");
        assert_eq!(reader_val, "pinned-in-wal");

        conn.execute("PRAGMA busy_timeout = 3000;")
            .await
            .expect("busy_timeout");
        conn.execute("VACUUM;").await.unwrap_or_else(|err| {
            panic!("bd-kvfbb: VACUUM with non-empty WAL pinned by reader failed: {err}")
        });

        let post_pages = int_cell(&conn.query("PRAGMA page_count;").await.expect("post pages"));
        assert!(post_pages < pre_pages / 2);
        let after_val = conn
            .query("SELECT payload FROM t WHERE id = 1;")
            .await
            .expect("read after vacuum");
        assert_eq!(
            after_val[0].values()[0],
            SqliteValue::Text("pinned-in-wal".into()),
            "VACUUM must preserve uncheckpointed WAL commits"
        );

        // Update id=1 AFTER VACUUM; pinned reader must still see 'pinned-in-wal'.
        conn.execute("UPDATE t SET payload = 'post-vacuum-wal' WHERE id = 1;")
            .await
            .expect("post-vacuum update");
        let still_val: String = stock_reader
            .query_row("SELECT payload FROM t WHERE id = 1;", [], |r| r.get(0))
            .expect("stock reread in txn");
        assert_eq!(still_val, "pinned-in-wal");

        stock_reader.execute_batch("COMMIT;").expect("stock COMMIT");
        let latest_val: String = stock_reader
            .query_row("SELECT payload FROM t WHERE id = 1;", [], |r| r.get(0))
            .expect("stock read after commit");
        assert_eq!(latest_val, "post-vacuum-wal");
        drop(stock_reader);

        conn.query("PRAGMA wal_checkpoint(TRUNCATE);")
            .await
            .expect("final truncate checkpoint");
        let final_pages = int_cell(&conn.query("PRAGMA page_count;").await.expect("final pages"));
        let final_len = std::fs::metadata(&db).expect("final metadata").len();
        assert_eq!(
            final_len,
            u64::try_from(final_pages).expect("non-negative") * 4096
        );

        conn.close().await.expect("close conn");
        verify_with_stock(&db, 10, None);
    });
}

#[test]
fn vacuum_in_place_preserves_same_process_active_reader_snapshot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("same_proc_reader.db");
    let db_str = db.to_str().expect("utf-8 path").to_owned();

    asupersync::test_utils::run_test(|| async {
        let (conn, pre_pages) = seed_sparse_wal_db(&db_str, 400, 10).await;
        let pre_file_len = std::fs::metadata(&db).expect("pre db metadata").len();

        let reader = Connection::open(&db_str).await.expect("open same-proc reader");
        reader.execute("BEGIN;").await.expect("reader BEGIN");
        let (pre_count, pre_sum) = int_pair(
            &reader
                .query("SELECT count(*), sum(id) FROM t;")
                .await
                .expect("reader initial query"),
        );
        assert_eq!((pre_count, pre_sum), (10, 55));

        conn.execute("VACUUM;")
            .await
            .expect("in-place WAL VACUUM beside same-process active reader");
        let post_pages = int_cell(&conn.query("PRAGMA page_count;").await.expect("post pages"));
        assert!(post_pages < pre_pages / 2);

        // Main file must remain untruncated while same-process reader pins its snapshot.
        let during_len = std::fs::metadata(&db).expect("during metadata").len();
        assert_eq!(during_len, pre_file_len);

        conn.execute("INSERT INTO t VALUES (3000, 'after-same-proc-vacuum');")
            .await
            .expect("insert after vacuum");

        // Same-process active reader must retain its exact pre-VACUUM snapshot.
        let (still_count, still_sum) = int_pair(
            &reader
                .query("SELECT count(*), sum(id) FROM t;")
                .await
                .expect("reader snapshot reread"),
        );
        assert_eq!((still_count, still_sum), (10, 55));

        reader.execute("COMMIT;").await.expect("reader COMMIT");
        let (after_count, after_sum) = int_pair(
            &reader
                .query("SELECT count(*), sum(id) FROM t;")
                .await
                .expect("reader post-commit read"),
        );
        let reader_pages = int_cell(
            &reader
                .query("PRAGMA page_count;")
                .await
                .expect("reader post-commit page_count"),
        );
        let post_insert_pages = int_cell(
            &conn
                .query("PRAGMA page_count;")
                .await
                .expect("conn post-insert page_count"),
        );
        assert_eq!((after_count, after_sum), (11, 3055));
        assert_eq!(reader_pages, post_insert_pages);

        reader.close().await.expect("close reader");
        conn.query("PRAGMA wal_checkpoint(TRUNCATE);")
            .await
            .expect("truncate checkpoint");
        let shrunk_len = std::fs::metadata(&db).expect("shrunk metadata").len();
        assert_eq!(
            shrunk_len,
            u64::try_from(post_insert_pages).expect("non-negative") * 4096
        );
        conn.close().await.expect("close conn");
        verify_with_stock(&db, 11, None);
    });
}


