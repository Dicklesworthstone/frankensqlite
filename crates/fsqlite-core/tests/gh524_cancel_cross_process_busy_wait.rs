#![recursion_limit = "512"]
#![cfg(unix)]

//! GH#524: a cancelled waiter must stop waiting for a writer lock that ANOTHER
//! PROCESS holds, instead of sleeping out its whole `busy_timeout`.
//!
//! A blocker in the same process wakes a cancelled waiter through the
//! in-process lock. A lock held by another process is only probed: the
//! admission attempt fails `Busy` at once and the busy-retry loop sleeps and
//! retries. On the 0.4.12 release a `BEGIN IMMEDIATE`, an autocommit INSERT
//! and a COMMIT cancelled 300 ms into such a wait each returned only at the
//! 10 s `busy_timeout` (this keeper: 10.00 s, all three; cass GH#515: SIGTERM
//! of `cass index` while the `cass daemon` held the writer). Main at
//! ad5d47395 ends all three promptly (the COMMIT at 0.33 s); this keeper
//! guards that. A cancellation check in the admission busy-retry loops alone
//! did not help on 0.4.12: the wait there sits in the cross-process lock
//! acquisition itself.
//!
//! Cancellation here is exactly what an actor facade does: a child of the
//! connection root with a local cancel relay, bound with
//! `bind_operation_cx`, and cancelled from another thread.
//!
//! The test re-executes itself as the peer: in peer mode it takes the writer
//! with `BEGIN IMMEDIATE` (serialized mode), inserts a row, reports readiness on
//! stdout and holds the transaction until its stdin closes, then rolls back.

use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::time::{Duration, Instant};

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::cx::CancelReason;
use fsqlite_types::value::SqliteValue;

const PEER_ENV: &str = "FSQLITE_GH524_PEER_DB";
const PEER_TEST: &str = "gh524_peer_holds_the_writer";
const READY: &str = "GH524_PEER_HOLDING";
/// Long enough that a waiter which ignores cancellation is unmistakable.
const WAITER_BUSY_TIMEOUT_MS: u64 = 10_000;
const CANCEL_AFTER: Duration = Duration::from_millis(300);
/// Far below the busy timeout, far above one back-off step (50 ms).
const PROMPT: Duration = Duration::from_secs(3);

/// Peer mode only: hold the writer in this process until stdin closes.
#[test]
fn gh524_peer_holds_the_writer() {
    let Some(db) = std::env::var_os(PEER_ENV) else {
        return;
    };
    let db = db.into_string().expect("utf-8 peer path");
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&db).await.expect("peer open");
        conn.execute("PRAGMA fsqlite.concurrent_mode = OFF;")
            .await
            .expect("peer serialized mode");
        conn.execute("BEGIN IMMEDIATE;").await.expect("peer BEGIN IMMEDIATE");
        conn.execute("INSERT INTO t VALUES (100);")
            .await
            .expect("peer insert");
        println!("{READY}");
        std::io::stdout().flush().expect("flush readiness");
        let mut sink = Vec::new();
        let _ = std::io::stdin().read_to_end(&mut sink);
        conn.execute("ROLLBACK;").await.expect("peer rollback");
        conn.close().await.expect("peer close");
    });
}

/// A peer process holding the database's writer until [`Peer::release`].
struct Peer {
    child: Child,
    stdin: Option<ChildStdin>,
}

impl Peer {
    fn hold_writer(db: &str) -> Self {
        let mut child = Command::new(std::env::current_exe().expect("test binary"))
            .args([PEER_TEST, "--exact", "--nocapture", "--test-threads=1"])
            .env(PEER_ENV, db)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn peer process");
        let stdin = child.stdin.take();
        let mut stdout = BufReader::new(child.stdout.take().expect("peer stdout"));
        let started = Instant::now();
        loop {
            let mut line = String::new();
            let read = stdout.read_line(&mut line).expect("read peer stdout");
            assert!(read > 0, "peer exited before taking the writer");
            if line.trim() == READY {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "peer did not take the writer within 60 s"
            );
        }
        // Keep draining the peer's stdout so it can never block on a full pipe.
        std::thread::spawn(move || {
            let mut sink = Vec::new();
            let _ = stdout.read_to_end(&mut sink);
        });
        Self { child, stdin }
    }

    /// Close the peer's stdin: it rolls back, closes, and exits.
    fn release(mut self) {
        drop(self.stdin.take());
        let status = self.child.wait().expect("wait for peer");
        assert!(status.success(), "peer process failed: {status}");
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        if self.stdin.is_some() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn seeded_database(dir: &tempfile::TempDir) -> String {
    let db = dir.path().join("gh524.db");
    let db = db.to_str().expect("utf-8 path").to_owned();
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&db).await.expect("open seed connection");
        conn.query("PRAGMA journal_mode = WAL;").await.expect("WAL");
        conn.execute("CREATE TABLE t(x INTEGER);").await.expect("create");
        conn.execute("INSERT INTO t VALUES (1);").await.expect("seed row");
        conn.close().await.expect("close seed connection");
    });
    db
}

fn count_rows(conn_rows: &[fsqlite_core::connection::Row]) -> i64 {
    match conn_rows[0].values()[0] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected an integer count, got {other:?}"),
    }
}

/// Run `sql` on a fresh connection to `db` under a cancellable operation
/// context that another thread cancels after [`CANCEL_AFTER`]. Returns the
/// statement's result and how long it took.
fn run_cancelled(db: &str, setup: &[&str], sql: &str) -> (Result<usize, FrankenError>, Duration) {
    let mut outcome = None;
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(db).await.expect("open waiter");
        conn.execute(&format!("PRAGMA busy_timeout = {WAITER_BUSY_TIMEOUT_MS};"))
            .await
            .expect("busy_timeout");
        for statement in setup {
            conn.execute(statement).await.expect("waiter setup statement");
        }
        let (operation, relay) = conn.root_cx().create_child_with_local_cancel_relay();
        let _binding = conn.bind_operation_cx(&operation);
        let canceller = std::thread::spawn(move || {
            std::thread::sleep(CANCEL_AFTER);
            relay.cancel_local(CancelReason::UserInterrupt)
        });
        let started = Instant::now();
        let result = conn.execute(sql).await;
        let elapsed = started.elapsed();
        assert!(
            canceller.join().expect("canceller thread"),
            "the cancel request must reach a live operation context"
        );
        outcome = Some((result, elapsed));
    });
    outcome.expect("waiter ran")
}

#[test]
fn cancelled_begin_immediate_stops_waiting_for_another_process_writer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = seeded_database(&dir);
    let peer = Peer::hold_writer(&db);
    let (result, elapsed) = run_cancelled(&db, &[], "BEGIN IMMEDIATE;");
    peer.release();
    assert!(
        matches!(result, Err(FrankenError::Abort)),
        "a cancelled BEGIN IMMEDIATE must end with Abort, got {result:?} after {elapsed:?}"
    );
    assert!(
        elapsed >= CANCEL_AFTER.saturating_sub(Duration::from_millis(50)),
        "BEGIN IMMEDIATE ended before the cancel ({elapsed:?}): it was not waiting on the peer"
    );
    assert!(
        elapsed < PROMPT,
        "a cancelled BEGIN IMMEDIATE waited {elapsed:?} for another process's writer \
         (busy_timeout {WAITER_BUSY_TIMEOUT_MS} ms)"
    );
}

#[test]
fn cancelled_autocommit_insert_stops_waiting_for_another_process_writer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = seeded_database(&dir);
    let peer = Peer::hold_writer(&db);
    let (result, elapsed) = run_cancelled(&db, &[], "INSERT INTO t VALUES (2);");
    peer.release();
    assert!(
        matches!(result, Err(FrankenError::Abort)),
        "a cancelled autocommit INSERT must end with Abort, got {result:?} after {elapsed:?}"
    );
    assert!(
        elapsed < PROMPT,
        "a cancelled autocommit INSERT waited {elapsed:?} for another process's writer"
    );
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&db).await.expect("reopen");
        let rows = conn
            .query("SELECT count(*) FROM t WHERE x = 2;")
            .await
            .expect("count");
        assert_eq!(count_rows(&rows), 0, "the cancelled INSERT must not have landed");
    });
}

/// The waiter's transaction is open (and has written) before the peer takes
/// the writer, so the wait happens at COMMIT. Cancellation ends that wait
/// promptly with a refusal (`Abort`, or `Busy` as on an expired
/// `busy_timeout`), and nothing is committed.
#[test]
fn cancelled_commit_stops_waiting_for_another_process_writer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = seeded_database(&dir);
    let mut outcome = None;
    let mut held = None;
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&db).await.expect("open waiter");
        conn.execute(&format!("PRAGMA busy_timeout = {WAITER_BUSY_TIMEOUT_MS};"))
            .await
            .expect("busy_timeout");
        conn.execute("BEGIN;").await.expect("waiter BEGIN");
        conn.execute("INSERT INTO t VALUES (4);")
            .await
            .expect("waiter insert");
        let peer = Peer::hold_writer(&db);
        let (operation, relay) = conn.root_cx().create_child_with_local_cancel_relay();
        let binding = conn.bind_operation_cx(&operation);
        let canceller = std::thread::spawn(move || {
            std::thread::sleep(CANCEL_AFTER);
            relay.cancel_local(CancelReason::UserInterrupt)
        });
        let started = Instant::now();
        let commit = conn.execute("COMMIT;").await;
        let elapsed = started.elapsed();
        assert!(
            canceller.join().expect("canceller thread"),
            "the cancel request must reach a live operation context"
        );
        drop(binding);
        if conn.in_transaction() {
            conn.execute("ROLLBACK;").await.expect("rollback after the refused commit");
        }
        outcome = Some((commit, elapsed));
        held = Some(peer);
    });
    held.expect("peer started").release();
    let (commit, elapsed) = outcome.expect("waiter ran");
    assert!(
        matches!(
            commit,
            Err(FrankenError::Abort | FrankenError::Busy | FrankenError::BusyRecovery)
        ),
        "a cancelled COMMIT behind another process's writer must refuse (Abort or Busy), \
         got {commit:?} after {elapsed:?}"
    );
    assert!(
        elapsed >= CANCEL_AFTER.saturating_sub(Duration::from_millis(50)),
        "COMMIT refused after {elapsed:?}, before the cancel: it was not waiting on the peer"
    );
    assert!(
        elapsed < PROMPT,
        "a cancelled COMMIT waited {elapsed:?} for another process's writer"
    );
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&db).await.expect("reopen");
        let rows = conn
            .query("SELECT count(*) FROM t WHERE x = 4;")
            .await
            .expect("count");
        assert_eq!(count_rows(&rows), 0, "the refused COMMIT must not have landed");
    });
}

/// Negative control: cancellation support must not cut a normal wait short.
/// An uncancelled `BEGIN IMMEDIATE` keeps waiting while the peer holds the
/// writer and is admitted once the peer lets go. A loop that gave up on its
/// first `Busy`, or mistook an uncancelled context for a cancelled one, fails.
#[test]
fn uncancelled_begin_immediate_still_waits_and_acquires_when_the_peer_releases() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = seeded_database(&dir);
    let peer = Peer::hold_writer(&db);
    let release_after = Duration::from_millis(800);
    let releaser = std::thread::spawn(move || {
        std::thread::sleep(release_after);
        peer.release();
    });
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&db).await.expect("open waiter");
        conn.execute(&format!("PRAGMA busy_timeout = {WAITER_BUSY_TIMEOUT_MS};"))
            .await
            .expect("busy_timeout");
        let started = Instant::now();
        let begin = conn.execute("BEGIN IMMEDIATE;").await;
        let elapsed = started.elapsed();
        assert!(
            begin.is_ok(),
            "an uncancelled BEGIN IMMEDIATE must be admitted once the peer releases, \
             got {begin:?} after {elapsed:?}"
        );
        assert!(
            elapsed >= release_after.saturating_sub(Duration::from_millis(100)),
            "BEGIN IMMEDIATE was admitted after {elapsed:?}, before the peer released"
        );
        conn.execute("INSERT INTO t VALUES (3);").await.expect("insert");
        conn.execute("COMMIT;").await.expect("commit");
        let rows = conn.query("SELECT count(*) FROM t;").await.expect("count");
        assert_eq!(count_rows(&rows), 2, "seed row plus the waiter's row");
    });
    releaser.join().expect("releaser thread");
}

/// Negative control: an uncancelled waiter still refuses at its own
/// `busy_timeout`, not before it.
#[test]
fn uncancelled_begin_immediate_refuses_busy_only_at_its_timeout() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = seeded_database(&dir);
    let peer = Peer::hold_writer(&db);
    let timeout = Duration::from_millis(1_000);
    let mut outcome = None;
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&db).await.expect("open waiter");
        conn.execute(&format!("PRAGMA busy_timeout = {};", timeout.as_millis()))
            .await
            .expect("busy_timeout");
        let started = Instant::now();
        let begin = conn.execute("BEGIN IMMEDIATE;").await;
        outcome = Some((begin, started.elapsed()));
    });
    peer.release();
    let (begin, elapsed) = outcome.expect("waiter ran");
    assert!(
        matches!(begin, Err(FrankenError::Busy | FrankenError::BusyRecovery)),
        "an uncancelled BEGIN IMMEDIATE behind another process's writer must refuse Busy, \
         got {begin:?} after {elapsed:?}"
    );
    assert!(
        elapsed >= timeout.saturating_sub(Duration::from_millis(100)),
        "Busy came after {elapsed:?}, before the {timeout:?} busy_timeout was spent"
    );
}
