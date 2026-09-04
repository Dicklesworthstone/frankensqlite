//! GH #411 keeper — a committed transaction must not be destroyed when a
//! FOREIGN (stock-SQLite, separate process) connection closes its handle on
//! the same WAL-mode database.
//!
//! # The bug
//!
//! fsqlite appends committed frames to `-wal` and fsyncs them, but never
//! advances `mxFrame` in the shared `-shm` WAL-index, and holds no SHARED lock
//! on the main database file while the WAL is open. So:
//!
//! 1. every attached stock connection still reads the pre-commit `mxFrame` and
//!    cannot see the commit (GH #19 — `write_shm_header` and
//!    `update_legacy_shm` have no production callers); and
//! 2. stock's `sqlite3WalClose` succeeds in taking an EXCLUSIVE lock on the
//!    main file, concludes it is the last connection, checkpoints only up to
//!    that stale `mxFrame`, and unlinks `-wal`/`-shm`.
//!
//! The committed frames go with it. `execute` returned `Ok`, the writer's own
//! `SELECT` returned the new row, and then the row vanishes — for fsqlite as
//! well as for stock — because an unrelated process closed a handle.
//!
//! Reported from `Dicklesworthstone/mcp_agent_mail_rust`, where it is the
//! remaining failure in
//! `queries::tests::commit_tx_does_not_wait_for_external_reader_checkpoint`
//! (`crates/mcp-agent-mail-db`): a write through the fsqlite pool returns `Ok`
//! while an external canonical reader holds `BEGIN; SELECT …`, and the row is
//! afterwards absent, with `-wal` back to a bare 32-byte header and a passive
//! checkpoint reporting 0 frames published.
//!
//! # Why the reader is a child PROCESS
//!
//! POSIX advisory locks are per-process. An in-process stock reader shares
//! fsqlite's lock ownership and never exercises the cross-process WAL close
//! protocol this bug lives in — with an in-process reader the scenario passes.
//!
//! # Status
//!
//! Committed `#[ignore]`d: red by design until GH #411 / GH #19 land. It is a
//! local-only keeper (nothing under `crates/fsqlite-core/tests/` is named by a
//! workflow allowlist), so run it explicitly with `--ignored`:
//!
//! ```text
//! cargo test -p fsqlite-core --test wal_reset_with_foreign_reader -- --ignored --nocapture
//! ```
//!
//! The `eprintln!` probes are deliberate: `(mxFrame, nBackfill, aReadMark)`
//! read straight out of `-shm` plus the `-wal` length at each step is what
//! separates the two defects, and a future regression will want them.

use std::io::{BufRead as _, Write as _};
use std::path::Path;
use std::process::Stdio;

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

const READER_PATH_ENV: &str = "FSQLITE_GH411_FOREIGN_READER_DB";
const READER_READY: &str = "foreign-reader-ready";
const READER_ROLLED_BACK: &str = "foreign-reader-rolled-back";
const TEST_NAME: &str = "commit_survives_foreign_canonical_reader_pinning_a_snapshot";

fn scalar_i64(rows: &[Row]) -> i64 {
    match rows[0].values()[0] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected an integer, got {other:?}"),
    }
}

fn sidecar(db: &Path, suffix: &str) -> std::path::PathBuf {
    let mut path = db.as_os_str().to_owned();
    path.push(suffix);
    std::path::PathBuf::from(path)
}

fn wal_len(db: &Path) -> u64 {
    std::fs::metadata(sidecar(db, "-wal")).map_or(0, |meta| meta.len())
}

/// `(mxFrame, nBackfill, aReadMark[0..5])` straight out of the `-shm`
/// WAL-index header, so a probe can tell what the shared index publishes.
///
/// Layout (C SQLite): `[0..48)` header copy 1, `[48..96)` copy 2,
/// `[96..100)` `nBackfill`, `[100..120)` `aReadMark`. `mxFrame` sits at
/// offset 16 of a copy. All native-endian.
fn shm_header(db: &Path) -> (u32, u32, Vec<u32>) {
    let Ok(bytes) = std::fs::read(sidecar(db, "-shm")) else {
        return (0, 0, Vec::new());
    };
    if bytes.len() < 120 {
        return (0, 0, Vec::new());
    }
    let word = |off: usize| {
        u32::from_ne_bytes([bytes[off], bytes[off + 1], bytes[off + 2], bytes[off + 3]])
    };
    (
        word(16),
        word(96),
        (0..5).map(|i| word(100 + i * 4)).collect(),
    )
}

fn canonical_count(db: &Path) -> i64 {
    let conn = rusqlite::Connection::open(db).expect("stock handle opens the database");
    conn.query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
        .expect("stock handle counts rows")
}

fn expect_line(expected: &str) {
    let mut line = String::new();
    std::io::stdin()
        .lock()
        .read_line(&mut line)
        .expect("child reads a control line");
    assert_eq!(line.trim(), expected, "child control protocol");
}

/// The child half: pin a stock read snapshot, announce readiness, roll back on
/// `release`, and only exit on `exit`. Splitting rollback from process exit is
/// what proves the destroyer is the CLOSE path and not the rollback.
fn run_as_foreign_reader(db_path: &std::ffi::OsStr) {
    let reader = rusqlite::Connection::open(Path::new(db_path)).expect("child opens stock reader");
    reader
        .execute_batch("BEGIN;")
        .expect("child begins a read transaction");
    let pinned: i64 = reader
        .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
        .expect("child pins its read snapshot");
    println!("{READER_READY} {pinned}");
    std::io::stdout().flush().expect("flush readiness witness");

    expect_line("release");
    reader
        .execute_batch("ROLLBACK;")
        .expect("child releases its read transaction");
    println!("{READER_ROLLED_BACK}");
    std::io::stdout().flush().expect("flush rollback witness");

    expect_line("exit");
}

fn signal_child(child: &mut std::process::Child, line: &str) {
    writeln!(
        child.stdin.as_mut().expect("child stdin stays available"),
        "{line}"
    )
    .expect("signal the foreign reader");
}

#[ignore = "GH#411 keeper: red by design — a foreign stock-SQLite connection's close deletes the -wal and destroys committed frames; needs the GH#19 shm WAL-index publication (and the WAL-open db-file SHARED fence) to go green"]
#[test]
fn commit_survives_foreign_canonical_reader_pinning_a_snapshot() {
    if let Some(db_path) = std::env::var_os(READER_PATH_ENV) {
        run_as_foreign_reader(&db_path);
        return;
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("gh411-foreign-reader.db");
    let db_str = db_path.to_string_lossy().into_owned();

    // One long-lived fsqlite connection throughout, the way a pooled writer
    // lives: seed, write while the foreign reader is pinned, checkpoint, close.
    asupersync::test_utils::run_test(move || {
        let db_path = db_path.clone();
        let db_str = db_str.clone();
        async move {
            let conn = Connection::open(&db_str).await.expect("open writer");
            conn.execute("PRAGMA journal_mode=WAL;")
                .await
                .expect("wal mode");
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT UNIQUE);")
                .await
                .expect("create table");
            conn.execute("INSERT INTO t(k) VALUES('baseline');")
                .await
                .expect("baseline insert");

            // ── A foreign canonical reader pins a snapshot. ───────────────
            let mut child = std::process::Command::new(
                std::env::current_exe().expect("resolve the test executable"),
            )
            .arg(TEST_NAME)
            .arg("--exact")
            .arg("--ignored")
            .arg("--nocapture")
            .env(READER_PATH_ENV, db_path.as_os_str())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn the foreign canonical reader");
            let mut child_out =
                std::io::BufReader::new(child.stdout.take().expect("capture child stdout"));
            let mut ready = false;
            loop {
                let mut line = String::new();
                if child_out.read_line(&mut line).expect("read child stdout") == 0 {
                    break;
                }
                if line.contains(READER_READY) {
                    ready = true;
                    break;
                }
            }
            assert!(ready, "the foreign reader exited before pinning a snapshot");

            // ── fsqlite commits while that snapshot is pinned. ────────────
            let wal_before = wal_len(&db_path);
            conn.execute("INSERT INTO t(k) VALUES('while-reader-pinned');")
                .await
                .expect("insert commits while a foreign reader is active");
            assert_eq!(
                scalar_i64(&conn.query("SELECT COUNT(*) FROM t;").await.expect("count")),
                2,
                "the writer sees its own committed row"
            );
            let wal_after_commit = wal_len(&db_path);
            assert!(
                wal_after_commit > wal_before,
                "the commit appended frames to the WAL ({wal_before} -> {wal_after_commit})"
            );
            // Safe to open a throwaway stock handle here: the child still
            // pins its read transaction, so this handle's own close cannot
            // take the EXCLUSIVE lock that would delete the WAL.
            eprintln!(
                "gh411 after-commit, reader pinned: wal={wal_after_commit} stock={} shm={:?}",
                canonical_count(&db_path),
                shm_header(&db_path)
            );

            // A passive checkpoint may legitimately publish nothing — the
            // foreign reader pins the tail — but must never discard frames.
            conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
                .await
                .expect("passive checkpoint");
            eprintln!(
                "gh411 after-passive-checkpoint: wal={} shm={:?}",
                wal_len(&db_path),
                shm_header(&db_path)
            );

            // ── Release the read transaction, child process still alive. ──
            signal_child(&mut child, "release");
            let mut rolled = String::new();
            child_out
                .read_line(&mut rolled)
                .expect("read the rollback witness");
            assert!(
                rolled.contains(READER_ROLLED_BACK),
                "the foreign reader rolled back, got {rolled:?}"
            );
            eprintln!(
                "gh411 after-rollback, child alive: wal={} shm={:?} fsqlite={}",
                wal_len(&db_path),
                shm_header(&db_path),
                scalar_i64(&conn.query("SELECT COUNT(*) FROM t;").await.expect("count"))
            );

            // ── Now let the child PROCESS exit: this is the destroyer. ────
            signal_child(&mut child, "exit");
            assert!(
                child.wait().expect("wait for the foreign reader").success(),
                "the foreign reader child failed"
            );
            let wal_after_close = wal_len(&db_path);
            eprintln!(
                "gh411 after the child process exits: wal={wal_after_close} shm={:?}",
                shm_header(&db_path)
            );

            assert_eq!(
                scalar_i64(&conn.query("SELECT COUNT(*) FROM t;").await.expect("count")),
                2,
                "a foreign connection's close must not discard committed frames \
                 (wal: {wal_before} -> {wal_after_commit} -> {wal_after_close})"
            );
            conn.close().await.expect("close writer");

            assert_eq!(
                canonical_count(&db_path),
                2,
                "the committed row must be visible to a fresh stock-SQLite handle"
            );

            let conn = Connection::open(&db_str).await.expect("reopen");
            assert_eq!(
                scalar_i64(&conn.query("SELECT COUNT(*) FROM t;").await.expect("count")),
                2,
                "the committed row survives close and reopen"
            );
            conn.close().await.expect("close");
        }
    });
}
