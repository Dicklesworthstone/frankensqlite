//! GH#534: a writer killed after its WAL commit frame is durable but before
//! it publishes the shared WAL index (`-shm`) leaves a committed WAL tail that
//! no live connection will ever publish. Every later opener bound its
//! snapshot from the stale index, and every COMMIT on it then spent the whole
//! busy timeout (5 s) and failed with `BusyRecovery`, in every new process,
//! forever. Stock SQLite reads the same files with the dead writer's commit.
//!
//! Expected, matching stock SQLite:
//! - the next connection's first COMMIT succeeds at once, on top of every
//!   commit the WAL holds (the same rows stock reads from a copy of the
//!   post-crash files), and `PRAGMA integrity_check` is `ok`;
//! - a crash at any step of a commit leaves the commit either wholly present
//!   or wholly absent, the same way stock reads it;
//! - a writer killed within its first commits after a checkpointing close,
//!   with a second database open beside it in both processes, leaves a state
//!   the next opener commits on at once (the shape of the downstream repro,
//!   `kill_during_commit_recovers`);
//! - a connection that stayed open in another process across the crash still
//!   commits, retrying at most a transaction refused with `BusySnapshot`;
//! - a writer that is alive between its WAL sync and its publication is never
//!   mistaken for a dead one: readers keep the published prefix meanwhile;
//! - a recovery that dies partway is redone by the next opener;
//! - a transaction whose snapshot predates the dead writer's commit is
//!   refused at once with a retryable `BusySnapshot`, and its retry commits.
//!
//! The writers are child processes killed for real (SIGKILL), or stopped at a
//! named commit step by the `fault-injection` crash points, which abort the
//! process the same way. POSIX locks are per process, so the dead writer must
//! not share this process.

#![cfg(all(unix, feature = "native"))]
// The async engine futures nest deeply; match the other integration suites.
#![recursion_limit = "512"]

use std::io::{BufRead as _, Write as _};
use std::os::unix::process::ExitStatusExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

const CHILD_TEST: &str = "gh534_child_process";
const CHILD_ROLE_ENV: &str = "FSQLITE_GH534_CHILD_ROLE";
const CHILD_DB_ENV: &str = "FSQLITE_GH534_CHILD_DB";
const CHILD_COMMITS_ENV: &str = "FSQLITE_GH534_CHILD_COMMITS";
const WITNESS: &str = "gh534:";
/// Process crash point environment, as `fsqlite_wal::fault_hooks` reads it.
#[cfg(feature = "fault-injection")]
const CRASH_POINT_ENV: &str = "FSQLITE_CRASH_POINT";
#[cfg(feature = "fault-injection")]
const CRASH_AFTER_ENV: &str = "FSQLITE_CRASH_AFTER";
#[cfg(feature = "fault-injection")]
const CRASH_ACTION_ENV: &str = "FSQLITE_CRASH_ACTION";
const CRASH_PAUSED_WITNESS: &str = "fsqlite-crash-point-paused:";
/// The commit window GH#534 is about: durable in the WAL, not yet indexed.
#[cfg(feature = "fault-injection")]
const SYNC_BEFORE_PUBLISH: &str = "commit_after_wal_sync_before_publish";
/// A first COMMIT after the crash must not come near the 5 s busy timeout.
const PROMPT: Duration = Duration::from_secs(2);

fn sidecar(db: &Path, suffix: &str) -> PathBuf {
    let mut path = db.as_os_str().to_owned();
    path.push(suffix);
    PathBuf::from(path)
}

fn db_str(db: &Path) -> &str {
    db.to_str().expect("utf-8 path")
}

/// The reporter's connection settings.
async fn open(db: &Path) -> Connection {
    let conn = Connection::open(db_str(db)).await.expect("open");
    conn.execute("PRAGMA journal_mode = WAL").await.expect("wal");
    conn.execute("PRAGMA synchronous = FULL").await.expect("synchronous");
    conn.execute("PRAGMA fsqlite.stmt_microbatch = OFF")
        .await
        .expect("microbatch off");
    conn
}

/// A fresh WAL database holding `items(1)`, closed without a checkpoint.
async fn create(dir: &Path) -> PathBuf {
    let db = dir.join("b.db");
    let conn = open(&db).await;
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .await
        .expect("create");
    conn.execute("INSERT INTO items VALUES (1)")
        .await
        .expect("seed");
    conn.close_without_checkpoint().await.expect("close");
    db
}

async fn count(conn: &Connection) -> Result<i64, FrankenError> {
    match conn.query_row("SELECT count(*) FROM items").await?.values() {
        [SqliteValue::Integer(rows)] => Ok(*rows),
        other => panic!("unexpected count row {other:?}"),
    }
}

async fn integrity(conn: &Connection) -> String {
    let row = conn
        .query_row("PRAGMA integrity_check")
        .await
        .expect("integrity_check");
    match row.values() {
        [SqliteValue::Text(text)] => text.to_string(),
        other => panic!("unexpected integrity row {other:?}"),
    }
}

/// BEGIN / INSERT `id` / COMMIT once, timing the COMMIT.
async fn commit_one(conn: &Connection, id: i64) -> (Result<usize, FrankenError>, Duration) {
    conn.execute("BEGIN").await.expect("begin");
    conn.execute(&format!("INSERT INTO items VALUES ({id})"))
        .await
        .expect("insert");
    let started = Instant::now();
    let result = conn.execute("COMMIT").await;
    (result, started.elapsed())
}

/// Copy the post-crash main/WAL/`-shm` files and count `items` there with
/// stock SQLite. Stock rebuilds the index from every valid WAL commit frame.
fn stock_count_of_copy(db: &Path, into: &Path) -> i64 {
    std::fs::create_dir_all(into).expect("copy dir");
    let copy = into.join("b.db");
    for suffix in ["", "-wal", "-shm"] {
        let source = sidecar(db, suffix);
        if source.exists() {
            std::fs::copy(&source, sidecar(&copy, suffix)).expect("copy post-crash file");
        }
    }
    let stock = rusqlite::Connection::open(&copy).expect("stock open copy");
    let rows: i64 = stock
        .query_row("SELECT count(*) FROM items", [], |row| row.get(0))
        .expect("stock count");
    let check: String = stock
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("stock integrity");
    assert_eq!(check, "ok", "stock reads the post-crash files as intact");
    rows
}

/// The checks every crash scenario ends with: the next writer commits at
/// once on top of exactly what stock reads, and stock reads the result.
async fn assert_next_writer_commits_on_top(db: &Path, stock_rows: i64, context: &str) {
    let conn = open(db).await;
    let before = count(&conn).await.expect("count after crash");
    assert_eq!(before, stock_rows, "{context}: fsqlite reads what stock reads");
    let (result, took) = commit_one(&conn, 1_000_000).await;
    result.unwrap_or_else(|error| panic!("{context}: first COMMIT after the crash: {error}"));
    assert!(took < PROMPT, "{context}: first COMMIT took {took:?}");
    assert_eq!(count(&conn).await.expect("count"), stock_rows + 1, "{context}");
    assert_eq!(integrity(&conn).await, "ok", "{context}");
    conn.close_without_checkpoint().await.expect("close");
    let after = db.parent().expect("dir").join(format!("stock-after-{}", context.replace(' ', "-")));
    assert_eq!(
        stock_count_of_copy(db, &after),
        stock_rows + 1,
        "{context}: stock reads the new commit and every earlier one"
    );
}

#[test]
fn sigkill_during_a_commit_loop_never_blocks_the_next_commit() {
    let trials: u64 = std::env::var("GH534_TRIALS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20);
    asupersync::test_utils::run_test(|| async move {
        for trial in 0..trials {
            let dir = tempfile::tempdir().expect("tempdir");
            let db = create(dir.path()).await;
            let mut writer = ChildProcess::spawn("writer", &db, &[]);
            writer.witness("committing");
            // Land the kill at varied points of the commit cycle.
            std::thread::sleep(Duration::from_millis(1 + (trial * 7_919) % 40));
            writer.sigkill();
            let stock_rows = stock_count_of_copy(&db, &dir.path().join("stock"));
            assert!(stock_rows >= 1);
            assert_next_writer_commits_on_top(&db, stock_rows, &format!("trial {trial}")).await;
        }
    });
}

/// Two fresh WAL databases, `a.db` and `b.db`, each holding `items(1)` and each
/// closed WITH its checkpoint, so the next writer's first commit starts a new
/// WAL. Returns `(a, b)`.
async fn create_pair_checkpointed(dir: &Path) -> (PathBuf, PathBuf) {
    let mut made = Vec::new();
    for name in ["a.db", "b.db"] {
        let db = dir.join(name);
        let conn = open(&db).await;
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
            .await
            .expect("create");
        conn.execute("INSERT INTO items VALUES (1)")
            .await
            .expect("seed");
        conn.close().await.expect("close with checkpoint");
        made.push(db);
    }
    let b = made.pop().expect("b.db");
    let a = made.pop().expect("a.db");
    (a, b)
}

/// The downstream repro of GH#534: a writer holding two databases open commits
/// single rows to the second; it is killed a few milliseconds into its first
/// commits; the reopening process opens both databases again and commits.
/// Recovery must complete within [`PROMPT`], on top of exactly the rows stock
/// reads from a copy of the post-crash files, with `integrity_check` ok. Unlike
/// the loop above, the databases were closed with a checkpoint (so the kill can
/// land while the writer starts a new WAL) and a second database is open in both
/// processes.
#[test]
fn kill_during_commit_recovers() {
    let trials: u64 = std::env::var("GH534_TRIALS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20);
    asupersync::test_utils::run_test(|| async move {
        for trial in 0..trials {
            let dir = tempfile::tempdir().expect("tempdir");
            let (peer, db) = create_pair_checkpointed(dir.path()).await;
            let mut writer = ChildProcess::spawn("writer_beside_peer", &db, &[]);
            writer.witness("committing");
            std::thread::sleep(Duration::from_millis(1 + trial % 8));
            writer.sigkill();
            let stock_rows = stock_count_of_copy(&db, &dir.path().join("stock"));
            assert!(stock_rows >= 1, "trial {trial}: stock reads {stock_rows} rows");
            let _peer = open(&peer).await;
            assert_next_writer_commits_on_top(&db, stock_rows, &format!("pair trial {trial}")).await;
        }
    });
}

#[cfg(feature = "fault-injection")]
#[test]
fn a_crash_at_any_commit_step_leaves_the_commit_whole_or_absent_like_stock() {
    // The fifth commit (id 6) dies at `point`; four committed before it.
    for (point, expected) in [
        ("commit_after_certificate", &[5][..]),
        // The appended frames may or may not have reached the file.
        ("commit_after_frame_append", &[5, 6][..]),
        (SYNC_BEFORE_PUBLISH, &[6][..]),
        ("commit_after_publish_before_unlock", &[6][..]),
    ] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().expect("tempdir");
            let db = create(dir.path()).await;
            let mut writer = ChildProcess::spawn(
                "writer",
                &db,
                &[(CRASH_POINT_ENV, point), (CRASH_AFTER_ENV, "5")],
            );
            writer.wait_aborted();
            let stock_rows = stock_count_of_copy(&db, &dir.path().join("stock"));
            assert!(
                expected.contains(&stock_rows),
                "{point}: stock reads {stock_rows} rows, expected one of {expected:?}"
            );
            assert_next_writer_commits_on_top(&db, stock_rows, point).await;
        });
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn a_connection_open_across_the_crash_keeps_committing() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = create(dir.path()).await;
        let mut survivor = ChildProcess::spawn("survivor", &db, &[]);
        assert_eq!(survivor.witness("ready"), "1");
        // The third commit (id 4) is durable but never published.
        let mut writer = ChildProcess::spawn(
            "writer",
            &db,
            &[(CRASH_POINT_ENV, SYNC_BEFORE_PUBLISH), (CRASH_AFTER_ENV, "3")],
        );
        writer.wait_aborted();
        survivor.signal("commit");
        let report = survivor.witness("committed");
        let (attempts, millis) = report.split_once(' ').expect("attempts and millis");
        let attempts: u32 = attempts.parse().expect("attempts");
        let millis: u64 = millis.parse().expect("millis");
        assert!(attempts <= 2, "the survivor needed {attempts} attempts");
        assert!(millis < 2_000, "the survivor took {millis} ms");
        assert_eq!(survivor.witness("rows"), "5", "1 + 3 recovered + its own");
        survivor.wait_success();
        let stock_rows = stock_count_of_copy(&db, &dir.path().join("stock"));
        assert_eq!(stock_rows, 5);
        assert_next_writer_commits_on_top(&db, stock_rows, "after survivor").await;
    });
}

#[cfg(feature = "fault-injection")]
#[test]
fn a_live_writer_between_sync_and_publish_is_not_taken_for_dead() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = create(dir.path()).await;
        let mut writer = ChildProcess::spawn(
            "writer",
            &db,
            &[
                (CRASH_POINT_ENV, SYNC_BEFORE_PUBLISH),
                (CRASH_AFTER_ENV, "3"),
                (CRASH_ACTION_ENV, "pause"),
                (CHILD_COMMITS_ENV, "3"),
            ],
        );
        writer.wait_paused();
        // The third commit is durable and its writer alive: a reader keeps
        // the published prefix and must not try to rebuild the index under
        // the live publisher (which would also fail busy against its WRITE).
        let reader = Connection::open(db_str(&db)).await.expect("open reader");
        let started = Instant::now();
        assert_eq!(count(&reader).await.expect("read beside a live writer"), 3);
        assert!(started.elapsed() < PROMPT);
        writer.signal("resume");
        assert_eq!(writer.witness("done"), "3");
        assert_eq!(count(&reader).await.expect("read after publication"), 4);
        writer.signal("exit");
        writer.wait_success();
        reader.close_without_checkpoint().await.expect("close reader");
        let stock_rows = stock_count_of_copy(&db, &dir.path().join("stock"));
        assert_eq!(stock_rows, 4);
        assert_next_writer_commits_on_top(&db, stock_rows, "after live writer").await;
    });
}

#[cfg(feature = "fault-injection")]
#[test]
fn reopening_repeatedly_after_the_crash_stays_consistent() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = create(dir.path()).await;
        let mut writer = ChildProcess::spawn(
            "writer",
            &db,
            &[(CRASH_POINT_ENV, SYNC_BEFORE_PUBLISH), (CRASH_AFTER_ENV, "3")],
        );
        writer.wait_aborted();
        let stock_rows = stock_count_of_copy(&db, &dir.path().join("stock"));
        assert_eq!(stock_rows, 4);
        for reopen in 0..3 {
            let conn = Connection::open(db_str(&db)).await.expect("reopen");
            assert_eq!(count(&conn).await.expect("count"), stock_rows, "reopen {reopen}");
            conn.close_without_checkpoint().await.expect("close");
        }
        assert_next_writer_commits_on_top(&db, stock_rows, "after reopens").await;
    });
}

#[cfg(feature = "fault-injection")]
#[test]
fn a_recovery_that_dies_partway_is_redone_by_the_next_opener() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = create(dir.path()).await;
        let mut writer = ChildProcess::spawn(
            "writer",
            &db,
            &[(CRASH_POINT_ENV, SYNC_BEFORE_PUBLISH), (CRASH_AFTER_ENV, "3")],
        );
        writer.wait_aborted();
        let mut recoverer = ChildProcess::spawn(
            "reader",
            &db,
            &[
                (CRASH_POINT_ENV, "index_recovery_before_header_publish"),
                (CRASH_AFTER_ENV, "1"),
            ],
        );
        recoverer.wait_aborted();
        let stock_rows = stock_count_of_copy(&db, &dir.path().join("stock"));
        assert_eq!(stock_rows, 4, "a half-done index rebuild loses no commit");
        assert_next_writer_commits_on_top(&db, stock_rows, "after dead recovery").await;
    });
}

#[cfg(feature = "fault-injection")]
#[test]
fn a_transaction_begun_before_the_crash_gets_a_prompt_retryable_conflict() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = create(dir.path()).await;
        let mut writer = ChildProcess::spawn(
            "writer",
            &db,
            &[
                (CRASH_POINT_ENV, SYNC_BEFORE_PUBLISH),
                (CRASH_AFTER_ENV, "3"),
                (CRASH_ACTION_ENV, "pause"),
            ],
        );
        writer.wait_paused();
        // Bind a snapshot while the writer is alive and holds WRITE.
        let conn = open(&db).await;
        conn.execute("BEGIN").await.expect("begin");
        conn.execute("INSERT INTO items VALUES (500)")
            .await
            .expect("insert on the published prefix");
        writer.sigkill();
        let started = Instant::now();
        let error = conn
            .execute("COMMIT")
            .await
            .expect_err("the snapshot predates a durable commit");
        let took = started.elapsed();
        assert!(
            matches!(error, FrankenError::BusySnapshot { .. }),
            "refused with {error:?}"
        );
        assert!(took < Duration::from_secs(1), "refused after {took:?}");
        let _ = conn.execute("ROLLBACK").await;
        let (result, took) = commit_one(&conn, 500).await;
        result.expect("the retried transaction commits");
        assert!(took < PROMPT, "retry took {took:?}");
        assert_eq!(count(&conn).await.expect("count"), 5, "1 + 3 + 1");
        assert_eq!(integrity(&conn).await, "ok");
        conn.close_without_checkpoint().await.expect("close");
        assert_eq!(stock_count_of_copy(&db, &dir.path().join("stock")), 5);
    });
}

/// Child roles; a no-op unless the parent set the role environment.
#[test]
fn gh534_child_process() {
    let (Some(role), Some(db)) = (
        std::env::var_os(CHILD_ROLE_ENV),
        std::env::var_os(CHILD_DB_ENV),
    ) else {
        return;
    };
    let db = PathBuf::from(db);
    let commits: Option<i64> = std::env::var(CHILD_COMMITS_ENV)
        .ok()
        .map(|value| value.parse().expect("commit count"));
    asupersync::test_utils::run_test(|| async move {
        match role.to_str().expect("utf-8 role") {
            // The reporter's writer: BEGIN / INSERT / COMMIT in a loop.
            "writer" => {
                let conn = open(&db).await;
                witness("committing", "");
                let last = commits.map_or(i64::MAX, |n| n + 1);
                for id in 2..=last {
                    conn.execute("BEGIN").await.expect("begin");
                    conn.execute(&format!("INSERT INTO items VALUES ({id})"))
                        .await
                        .expect("insert");
                    conn.execute("COMMIT").await.expect("commit");
                }
                witness("done", &(last - 1).to_string());
                expect_line("exit");
            }
            // The downstream repro's writer: a second database open beside the
            // one it commits single rows to, until it is killed.
            "writer_beside_peer" => {
                let _peer = open(&db.with_file_name("a.db")).await;
                let conn = open(&db).await;
                witness("committing", "");
                for id in 2..=i64::MAX {
                    conn.execute("BEGIN").await.expect("begin");
                    conn.execute(&format!("INSERT INTO items VALUES ({id})"))
                        .await
                        .expect("insert");
                    conn.execute("COMMIT").await.expect("commit");
                }
            }
            "survivor" => {
                let conn = open(&db).await;
                witness("ready", &count(&conn).await.expect("count").to_string());
                expect_line("commit");
                let started = Instant::now();
                let mut attempts = 0;
                loop {
                    attempts += 1;
                    let (result, _) = commit_one(&conn, 900).await;
                    match result {
                        Ok(_) => break,
                        Err(FrankenError::BusySnapshot { .. }) if attempts < 3 => {
                            let _ = conn.execute("ROLLBACK").await;
                        }
                        Err(error) => panic!("survivor commit attempt {attempts}: {error}"),
                    }
                }
                let millis = started.elapsed().as_millis();
                witness("committed", &format!("{attempts} {millis}"));
                witness("rows", &count(&conn).await.expect("count").to_string());
                conn.close_without_checkpoint().await.expect("close");
            }
            "reader" => {
                let conn = Connection::open(db_str(&db)).await.expect("open");
                witness("rows", &count(&conn).await.expect("count").to_string());
            }
            other => panic!("unknown child role {other}"),
        }
    });
}

fn witness(tag: &str, value: &str) {
    let mut stdout = std::io::stdout().lock();
    writeln!(stdout, "{WITNESS}{tag} {value}").expect("witness");
    stdout.flush().expect("flush witness");
}

fn expect_line(expected: &str) {
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).expect("read parent line");
    assert_eq!(line.trim(), expected);
}

/// One child running [`gh534_child_process`], killed if still alive on drop.
struct ChildProcess {
    child: std::process::Child,
    output: Receiver<String>,
    role: &'static str,
}

impl ChildProcess {
    fn spawn(role: &'static str, db: &Path, env: &[(&str, &str)]) -> Self {
        let mut command = Command::new(std::env::current_exe().expect("test binary"));
        command
            .args([CHILD_TEST, "--exact", "--nocapture", "--test-threads=1"])
            .env(CHILD_ROLE_ENV, role)
            .env(CHILD_DB_ENV, db.as_os_str())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());
        for (key, value) in env {
            command.env(key, value);
        }
        let mut child = command.spawn().expect("spawn child");
        let stdout = child.stdout.take().expect("child stdout");
        let (sender, output) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                // The harness's `test ... ` banner may precede a witness.
                let witness = if let Some((_, witness)) = line.split_once(WITNESS) {
                    witness.to_owned()
                } else if let Some((_, point)) = line.split_once(CRASH_PAUSED_WITNESS) {
                    format!("paused {point}")
                } else {
                    continue;
                };
                if sender.send(witness).is_err() {
                    break;
                }
            }
        });
        Self {
            child,
            output,
            role,
        }
    }

    #[cfg(feature = "fault-injection")]
    fn signal(&mut self, line: &str) {
        let stdin = self.child.stdin.as_mut().expect("child stdin");
        writeln!(stdin, "{line}").expect("signal child");
        stdin.flush().expect("flush child stdin");
    }

    /// The value of the child's next `tag` witness.
    fn witness(&self, tag: &str) -> String {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let line = self.output.recv_timeout(remaining).unwrap_or_else(|error| {
                panic!("{} child produced no {tag} witness: {error}", self.role)
            });
            if let Some(value) = line.strip_prefix(tag) {
                return value.trim().to_owned();
            }
        }
    }

    #[cfg(feature = "fault-injection")]
    fn wait_paused(&self) {
        let point = self.witness("paused");
        assert_eq!(point, SYNC_BEFORE_PUBLISH, "{} paused elsewhere", self.role);
    }

    fn sigkill(&mut self) {
        self.child.kill().expect("SIGKILL child");
        let status = self.child.wait().expect("reap child");
        assert_eq!(status.signal(), Some(9), "{} child died by SIGKILL", self.role);
    }

    #[cfg(feature = "fault-injection")]
    fn wait_status(&mut self) -> std::process::ExitStatus {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = self.child.try_wait().expect("poll child") {
                return status;
            }
            assert!(Instant::now() < deadline, "{} child did not exit", self.role);
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// The crash point aborted the child (SIGABRT, no cleanup).
    #[cfg(feature = "fault-injection")]
    fn wait_aborted(&mut self) {
        let status = self.wait_status();
        assert_eq!(
            status.signal(),
            Some(6),
            "{} child should die at its crash point, exited with {status}",
            self.role
        );
    }

    #[cfg(feature = "fault-injection")]
    fn wait_success(&mut self) {
        let status = self.wait_status();
        assert!(status.success(), "{} child exited with {status}", self.role);
    }
}

impl Drop for ChildProcess {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}
