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

#[cfg(feature = "native")]
use std::future::Future;
use std::io::{BufRead, BufReader, Read, Write};
#[cfg(feature = "native")]
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use fsqlite_core::connection::Connection;
#[cfg(feature = "native")]
use fsqlite_core::connection::ConnectionEnv;
use fsqlite_error::FrankenError;
#[cfg(feature = "native")]
use fsqlite_pager::{ConnectionPagerOpenMode, SimplePager};
#[cfg(feature = "native")]
use fsqlite_types::{LockLevel, PageSize};
use fsqlite_types::cx::CancelReason;
#[cfg(feature = "native")]
use fsqlite_types::cx::{Cx, LocalCancelRelay};
#[cfg(feature = "native")]
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_types::value::SqliteValue;
#[cfg(feature = "native")]
use fsqlite_vfs::shm::{
    SHM_SEGMENT_SIZE, SQLITE_SHM_EXCLUSIVE, SQLITE_SHM_LOCK, SQLITE_SHM_SHARED,
    SQLITE_SHM_UNLOCK, WAL_NREADER, WAL_READ_LOCK_BASE, WAL_WRITE_LOCK,
};
#[cfg(feature = "native")]
use fsqlite_vfs::unix::UnixVfs;
#[cfg(feature = "native")]
use fsqlite_vfs::{Vfs, VfsFile};

const PEER_ENV: &str = "FSQLITE_GH524_PEER_DB";
const PEER_TEST: &str = "gh524_peer_holds_the_writer";
#[cfg(feature = "native")]
const EXCLUSIVE_PEER_TEST: &str = "gh524_peer_holds_an_exclusive_main_file_lock";
#[cfg(feature = "native")]
const READER_PEER_TEST: &str = "gh524_peer_holds_all_wal_reader_slots";
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

/// Block every bootstrap mode, including read-only schema loading, at the
/// real main-file lock boundary. A WAL-only writer does not provide that
/// guarantee: a reader can legitimately open behind it.
#[cfg(feature = "native")]
#[test]
fn gh524_peer_holds_an_exclusive_main_file_lock() {
    let Some(db) = std::env::var_os(PEER_ENV) else {
        return;
    };
    let cx = Cx::new();
    let vfs = UnixVfs::new();
    let (mut file, _) = vfs
        .open(
            &cx,
            Some(Path::new(&db)),
            VfsOpenFlags::MAIN_DB | VfsOpenFlags::READWRITE,
        )
        .expect("peer opens the existing main file");
    file.lock(&cx, LockLevel::Exclusive)
        .expect("peer takes the exclusive main-file lock");
    println!("{READY}");
    std::io::stdout().flush().expect("flush readiness");
    let mut sink = Vec::new();
    let _ = std::io::stdin().read_to_end(&mut sink);
    file.unlock(&cx, LockLevel::None).expect("peer unlock");
    file.close(&cx).expect("peer close");
}

/// Preserve a real, nonempty WAL and its valid publication while excluding
/// readers only. The idle stock connection keeps the WAL/DMS lifetime alive;
/// its completed SELECT has released its reader slot before we claim them.
#[cfg(feature = "native")]
#[test]
fn gh524_peer_holds_all_wal_reader_slots() {
    let Some(db) = std::env::var_os(PEER_ENV) else {
        return;
    };
    let stock = rusqlite::Connection::open(&db).expect("stock peer open");
    let count: i64 = stock
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .expect("stock peer validates committed WAL");
    assert_eq!(count, 2);

    let cx = Cx::new();
    let vfs = UnixVfs::new();
    let (mut file, _) = vfs
        .open(
            &cx,
            Some(Path::new(&db)),
            VfsOpenFlags::MAIN_DB | VfsOpenFlags::READWRITE,
        )
        .expect("peer opens existing main file");
    file.lock(&cx, LockLevel::Shared).expect("peer main-file SHARED");
    let region = file
        .shm_map(&cx, 0, SHM_SEGMENT_SIZE, false)
        .expect("map the existing WAL publication");
    let header = fsqlite_wal::wal_index::read_shared_wal_index_header(&region)
        .expect("validate WAL publication")
        .expect("stock peer must have published a valid WAL index");
    assert!(header.mx_frame > 0, "fixture must retain committed WAL frames");
    file.shm_lock(
        &cx,
        WAL_READ_LOCK_BASE,
        WAL_NREADER,
        SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE,
    )
    .expect("claim all WAL reader slots without a writer or recovery lock");
    println!("{READY}");
    std::io::stdout().flush().expect("flush readiness");
    let mut sink = Vec::new();
    let _ = std::io::stdin().read_to_end(&mut sink);
    file.shm_lock(
        &cx,
        WAL_READ_LOCK_BASE,
        WAL_NREADER,
        SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE,
    )
    .expect("release reader slots");
    drop(region);
    file.unlock(&cx, LockLevel::None).expect("peer unlock");
    file.close(&cx).expect("peer close");
    stock.close().expect("stock peer closes only after releasing the slots");
}

/// A peer process holding the database's writer until [`Peer::release`].
struct Peer {
    child: Child,
    stdin: Option<ChildStdin>,
}

impl Peer {
    fn hold_writer(db: &str) -> Self {
        Self::hold(db, PEER_TEST)
    }

    #[cfg(feature = "native")]
    fn hold_exclusive(db: &str) -> Self {
        Self::hold(db, EXCLUSIVE_PEER_TEST)
    }

    #[cfg(feature = "native")]
    fn hold_wal_readers(db: &str) -> Self {
        Self::hold(db, READER_PEER_TEST)
    }

    fn hold(db: &str, test: &str) -> Self {
        let mut child = Command::new(std::env::current_exe().expect("test binary"))
            .args([test, "--exact", "--nocapture", "--test-threads=1"])
            .env(PEER_ENV, db)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn peer process");
        let stdin = child.stdin.take();
        let mut stdout = BufReader::new(child.stdout.take().expect("peer stdout"));
        let peer = Self { child, stdin };
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        // Readiness needs a real deadline even when the child emits no line.
        // Continue draining after readiness so a full pipe cannot stall it.
        std::thread::spawn(move || {
            loop {
                let mut line = String::new();
                if stdout.read_line(&mut line).unwrap_or(0) == 0 {
                    return;
                }
                if line.trim_end().ends_with(READY) {
                    let _ = ready_tx.send(());
                    break;
                }
            }
            let mut sink = Vec::new();
            let _ = stdout.read_to_end(&mut sink);
        });
        ready_rx
            .recv_timeout(Duration::from_secs(60))
            .expect("peer must acquire its lock before the readiness deadline");
        peer
    }

    /// Close the peer's stdin: it rolls back, closes, and exits.
    fn release(mut self) {
        drop(self.stdin.take());
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.child.try_wait().expect("wait for peer") {
                assert!(status.success(), "peer process failed: {status}");
                return;
            }
            assert!(Instant::now() < deadline, "peer did not release its lock");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        if !self.child.try_wait().is_ok_and(|status| status.is_some()) {
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

#[cfg(feature = "native")]
#[derive(Clone, Copy, Debug)]
enum BootstrapOpen {
    Ordinary,
    Existing,
    ReadOnlySchema,
    ExistingSchema,
    DeferredFtsSchema,
    RecoverWalIndex,
}

#[cfg(feature = "native")]
impl BootstrapOpen {
    const ALL: [Self; 6] = [
        Self::Ordinary,
        Self::Existing,
        Self::ReadOnlySchema,
        Self::ExistingSchema,
        Self::DeferredFtsSchema,
        Self::RecoverWalIndex,
    ];

    async fn open(self, path: &str, env: ConnectionEnv) -> Result<Connection, FrankenError> {
        match self {
            Self::Ordinary => Connection::open_with_env(path, env).await,
            Self::Existing => Connection::open_existing_with_env(path, env).await,
            Self::ReadOnlySchema => Connection::open_schema_only_with_env(path, env).await,
            Self::ExistingSchema => Connection::open_existing_schema_only_with_env(path, env).await,
            Self::DeferredFtsSchema => {
                Connection::open_existing_schema_only_deferred_fts5_with_env(path, env).await
            }
            Self::RecoverWalIndex => {
                Connection::open_schema_only_with_wal_index_recovery_and_env(path, env).await
            }
        }
    }
}

/// Start the timer before the first poll: the bug can block that very poll.
/// Requiring completion after the actual request also proves that the peer's
/// lock blocked the operation, rather than testing an already-finished open.
#[cfg(feature = "native")]
async fn cancel_after_first_poll<F: Future>(
    relay: LocalCancelRelay,
    future: F,
) -> (F::Output, Duration) {
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let canceller = std::thread::spawn(move || {
        started_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("operation must be polled");
        std::thread::sleep(CANCEL_AFTER);
        let requested = Instant::now();
        let delivered = relay.cancel_local(CancelReason::UserInterrupt);
        (requested, delivered)
    });
    let mut started_tx = Some(started_tx);
    let mut future = std::pin::pin!(future);
    let result = std::future::poll_fn(|task_cx| {
        if let Some(started) = started_tx.take() {
            started.send(()).expect("canceller is listening");
        }
        future.as_mut().poll(task_cx)
    })
    .await;
    let finished = Instant::now();
    let (requested, delivered) = canceller.join().expect("canceller thread");
    assert!(delivered, "cancellation must reach the live operation context");
    let latency = finished
        .checked_duration_since(requested)
        .expect("the operation finished before cancellation: the peer did not block it");
    (result, latency)
}

#[cfg(feature = "native")]
#[test]
fn cancelled_bootstrap_stops_waiting_for_another_process_lock() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = seeded_database(&dir);
    let peer = Peer::hold_exclusive(&db);
    asupersync::test_utils::run_test(|| async {
        let env = ConnectionEnv::default();
        for mode in BootstrapOpen::ALL {
            let bootstrap_parent = env.bootstrap_cx();
            let (operation, relay) = bootstrap_parent.create_child_with_local_cancel_relay();
            let (result, latency) = cancel_after_first_poll(
                relay,
                mode.open(&db, env.clone().with_bootstrap_cx(operation.clone())),
            )
            .await;
            eprintln!("{mode:?} bootstrap cancellation latency: {latency:?}");
            assert!(
                matches!(result, Err(FrankenError::Abort)),
                "{mode:?} must abort while the peer still holds the main file: {result:?}"
            );
            assert!(
                latency < Duration::from_secs(1),
                "{mode:?} ignored bootstrap cancellation for {latency:?}"
            );
            assert!(operation.checkpoint().is_err());
            assert!(
                env.bootstrap_cx().checkpoint().is_ok(),
                "a cancelled bootstrap must not cancel the shared runtime"
            );
        }
    });
    peer.release();
    asupersync::test_utils::run_test(|| async {
        for mode in BootstrapOpen::ALL {
            let conn = mode
                .open(&db, ConnectionEnv::default())
                .await
                .expect("a fresh bootstrap must recover after cancellation");
            let rows = conn.query("SELECT count(*) FROM t").await.expect("read seed");
            assert_eq!(count_rows(&rows), 1, "{mode:?} must preserve committed data");
            conn.close().await.expect("close recovered connection");
        }
    });
}

#[cfg(feature = "native")]
#[test]
fn uncancelled_bootstrap_acquires_after_the_peer_releases() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = seeded_database(&dir);
    let peer = Peer::hold_exclusive(&db);
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let releaser = std::thread::spawn(move || {
        started_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("bootstrap must be polled");
        std::thread::sleep(Duration::from_millis(800));
        let released = Instant::now();
        peer.release();
        released
    });
    asupersync::test_utils::run_test(|| async {
        let env = ConnectionEnv::default();
        let operation = env.bootstrap_cx();
        let mut open = std::pin::pin!(Connection::open_with_env(
            &db,
            env.with_bootstrap_cx(operation),
        ));
        let mut started_tx = Some(started_tx);
        let conn = std::future::poll_fn(|task_cx| {
            if let Some(started) = started_tx.take() {
                started.send(()).expect("releaser is listening");
            }
            open.as_mut().poll(task_cx)
        })
        .await
        .expect("uncancelled bootstrap must wait for the peer to release");
        let finished = Instant::now();
        let released = releaser.join().expect("releaser thread");
        assert!(
            finished.checked_duration_since(released).is_some(),
            "bootstrap completed before the conflicting lock was released"
        );
        conn.execute("INSERT INTO t VALUES (2)").await.expect("write after open");
        let rows = conn.query("SELECT count(*) FROM t").await.expect("count");
        assert_eq!(count_rows(&rows), 2);
        conn.close().await.expect("close");
    });
}

#[cfg(feature = "native")]
#[test]
fn precancelled_bootstrap_does_not_create_database_or_sidecars() {
    let dir = tempfile::tempdir().expect("tempdir");
    let missing = dir.path().join("not-admitted.db");
    let missing = missing.to_str().expect("utf-8 path");
    asupersync::test_utils::run_test(|| async {
        let env = ConnectionEnv::default();
        let bootstrap_parent = env.bootstrap_cx();
        let (operation, relay) = bootstrap_parent.create_child_with_local_cancel_relay();
        assert!(relay.cancel_local(CancelReason::UserInterrupt));
        for mode in BootstrapOpen::ALL {
            let result = mode
                .open(missing, env.clone().with_bootstrap_cx(operation.clone()))
                .await;
            assert!(
                matches!(result, Err(FrankenError::Abort)),
                "{mode:?} must reject a precancelled bootstrap: {result:?}"
            );
            assert!(
                std::fs::read_dir(dir.path()).expect("read directory").next().is_none(),
                "{mode:?} created a database or companion before cancellation admission"
            );
        }
        let conn = Connection::open_with_env(missing, env)
            .await
            .expect("the runtime remains usable after refused opens");
        conn.execute("CREATE TABLE t(x INTEGER)").await.expect("create");
        conn.close().await.expect("close");
    });
}

#[cfg(feature = "native")]
#[test]
fn completed_bootstrap_cancellation_does_not_poison_connection_or_attach() {
    for mode in [BootstrapOpen::Ordinary, BootstrapOpen::ExistingSchema] {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = seeded_database(&dir);
        let auxiliary = dir.path().join("attached.db");
        asupersync::test_utils::run_test(|| async {
            let env = ConnectionEnv::default();
            let bootstrap_parent = env.bootstrap_cx();
            let (operation, relay) = bootstrap_parent.create_child_with_local_cancel_relay();
            let conn = mode
                .open(&db, env.with_bootstrap_cx(operation.clone()))
                .await
                .expect("open under a live bootstrap scope");
            assert!(relay.cancel_local(CancelReason::UserInterrupt));
            assert!(operation.checkpoint().is_err());
            assert!(
                conn.root_cx().checkpoint().is_ok(),
                "the returned connection must own an independent lifetime"
            );
            conn.execute("INSERT INTO t VALUES (2)").await.expect("write after late cancel");
            let rows = conn.query("SELECT count(*) FROM t").await.expect("read after late cancel");
            assert_eq!(count_rows(&rows), 2);
            conn.execute(&format!(
                "ATTACH DATABASE '{}' AS aux",
                auxiliary.to_string_lossy().replace('\'', "''"),
            ))
            .await
            .expect("ATTACH must not inherit the expired opening scope");
            conn.execute("CREATE TABLE aux.child(x INTEGER)").await.expect("attached DDL");
            conn.execute("INSERT INTO aux.child VALUES (7)").await.expect("attached insert");
            let rows = conn.query("SELECT count(*) FROM aux.child").await.expect("attached read");
            assert_eq!(count_rows(&rows), 1);
            conn.execute("DETACH DATABASE aux").await.expect("detach");
            conn.close().await.expect("late bootstrap cancellation must not poison close");
        });
    }
}

#[cfg(feature = "native")]
#[test]
fn cancelled_attach_bootstrap_leaves_the_main_connection_usable() {
    let main_dir = tempfile::tempdir().expect("main tempdir");
    let main_db = seeded_database(&main_dir);
    let auxiliary_dir = tempfile::tempdir().expect("auxiliary tempdir");
    let auxiliary_db = seeded_database(&auxiliary_dir);
    let peer = Peer::hold_exclusive(&auxiliary_db);
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&main_db).await.expect("open main");
        let sql = format!("ATTACH DATABASE '{}' AS aux", auxiliary_db.replace('\'', "''"));
        let (operation, relay) = conn.root_cx().create_child_with_local_cancel_relay();
        let binding = conn.bind_operation_cx(&operation);
        let (result, latency) = cancel_after_first_poll(relay, conn.execute(&sql)).await;
        drop(binding);
        assert!(
            matches!(result, Err(FrankenError::Abort)),
            "ATTACH must pass its operation cancellation into the nested open: {result:?}"
        );
        assert!(
            latency < Duration::from_secs(1),
            "ATTACH ignored cancellation for {latency:?}"
        );
        assert!(conn.root_cx().checkpoint().is_ok());
        let databases = conn.query("PRAGMA database_list").await.expect("database list");
        assert!(
            !databases.iter().any(|row| {
                matches!(row.values().get(1), Some(SqliteValue::Text(name)) if name.as_str() == "aux")
            }),
            "a cancelled ATTACH must not publish its schema"
        );
        conn.execute("INSERT INTO t VALUES (2)").await.expect("main write after cancelled ATTACH");
        let rows = conn.query("SELECT count(*) FROM t").await.expect("main count");
        assert_eq!(count_rows(&rows), 2);
        peer.release();
        conn.execute(&sql).await.expect("ATTACH can retry after the peer releases");
        let rows = conn.query("SELECT count(*) FROM aux.t").await.expect("attached count");
        assert_eq!(count_rows(&rows), 1);
        conn.execute("INSERT INTO aux.t VALUES (3)").await.expect("attached write");
        conn.close().await.expect("close after recovered ATTACH");
    });
}

/// Establish that only reader admission is contended, both before the open
/// and after it aborts. This also proves cancellation did not unlock the peer.
#[cfg(feature = "native")]
fn assert_only_wal_readers_are_locked(db: &str) {
    let cx = Cx::new();
    let vfs = UnixVfs::new();
    let (mut file, _) = vfs
        .open(
            &cx,
            Some(Path::new(db)),
            VfsOpenFlags::MAIN_DB | VfsOpenFlags::READWRITE,
        )
        .expect("open fixture probe");
    file.lock(&cx, LockLevel::Shared).expect("main-file admission is free");
    file.shm_lock(
        &cx,
        WAL_WRITE_LOCK,
        WAL_READ_LOCK_BASE,
        SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE,
    )
    .expect("write, checkpoint, and recovery locks must remain free");
    file.shm_lock(
        &cx,
        WAL_WRITE_LOCK,
        WAL_READ_LOCK_BASE,
        SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE,
    )
    .expect("release fixture probe locks");
    assert!(matches!(
        file.shm_lock(
            &cx,
            WAL_READ_LOCK_BASE,
            WAL_NREADER,
            SQLITE_SHM_LOCK | SQLITE_SHM_SHARED,
        ),
        Err(FrankenError::Busy)
    ));
    file.unlock(&cx, LockLevel::None).expect("probe unlock");
    file.close(&cx).expect("probe close");
}

#[cfg(feature = "native")]
#[test]
fn cancelled_bootstrap_after_pager_open_stops_waiting_for_wal_readers() {
    let dir = tempfile::tempdir().expect("tempdir");
    // Establish a coherent main header first, then retain an extra committed
    // row in the WAL so the connection must bind a real WAL reader snapshot.
    let db = seeded_database(&dir);
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&db).await.expect("open WAL fixture");
        conn.execute("PRAGMA wal_autocheckpoint = 0").await.expect("retain WAL");
        conn.execute("INSERT INTO t VALUES (2)").await.expect("WAL row");
        conn.close_without_checkpoint().await.expect("preserve committed WAL");
    });
    let peer = Peer::hold_wal_readers(&db);
    assert_only_wal_readers_are_locked(&db);
    asupersync::test_utils::run_test(|| async {
        let env = ConnectionEnv::default();
        let probe_cx = env.bootstrap_cx();
        let pager = SimplePager::open_for_connection_with_cx_and_page_buffer_max(
            &probe_cx,
            UnixVfs::new(),
            Path::new(&db),
            PageSize::DEFAULT,
            None,
            ConnectionPagerOpenMode::ExistingOnly(None),
        )
        .await
        .expect("pager construction must succeed while reader slots are held");
        pager.finish_namespace_bootstrap().expect("finish the probe's namespace lease");
        drop(pager);

        let operation = env.bootstrap_cx();
        let relay = operation.local_cancel_relay();
        let (result, latency) = cancel_after_first_poll(
            relay,
            Connection::open_existing_with_env(
                &db,
                env.clone().with_bootstrap_cx(operation.clone()),
            ),
        )
        .await;
        eprintln!("post-pager bootstrap cancellation latency: {latency:?}");
        assert!(
            matches!(result, Err(FrankenError::Abort)),
            "the connection must retain bootstrap cancellation after its pager opens: {result:?}"
        );
        assert!(latency < Duration::from_secs(1), "post-pager wait lasted {latency:?}");
        assert!(operation.checkpoint().is_err());
        assert!(env.bootstrap_cx().checkpoint().is_ok());
    });
    assert_only_wal_readers_are_locked(&db);
    peer.release();
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&db).await.expect("recover after the reader peer releases");
        let rows = conn.query("SELECT count(*) FROM t").await.expect("read committed rows");
        assert_eq!(count_rows(&rows), 2);
        conn.execute("INSERT INTO t VALUES (3)").await.expect("write after aborted bootstrap");
        conn.close().await.expect("close recovered connection");
    });
    let stock = rusqlite::Connection::open(&db).expect("stock integrity oracle");
    let integrity: String = stock
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity check");
    assert_eq!(integrity, "ok");
    let count: i64 = stock.query_row("SELECT count(*) FROM t", [], |row| row.get(0)).expect("stock count");
    assert_eq!(count, 3);
}
