//! bd-v0t32 adversarial corpus — kill-9 during durability (crash before
//! checkpoint), and the first-open repair pass on the recovered image.
//!
//! A child process opens the database, commits, and calls `std::process::abort()`
//! (SIGABRT) *inside* the live async runtime — no `Drop`, no checkpoint, no
//! clean fsync-on-close, the closest a test gets to `kill -9` mid-durability.
//! The parent reopens and verifies crash-recovery semantics, cross-checked
//! against stock C SQLite (rusqlite): the committed prefix survives, the image
//! is `integrity_check == ok` in both engines, the two engines agree on the
//! recovered rows, recovery is deterministic across repeated reopens, and the
//! first-open repair pass (bd-zywqc.5) certifies the crash-recovered image
//! rather than silently corrupting it.

use std::env;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;

use fsqlite::Connection;
use fsqlite_types::SqliteValue;
use tempfile::tempdir;

const HELPER_MODE_ENV: &str = "FSQLITE_KILL9_HELPER_MODE";
const HELPER_DB_PATH_ENV: &str = "FSQLITE_KILL9_HELPER_DB_PATH";
const HELPER_TEST_NAME: &str = "kill9_helper_entrypoint";
const MIGRATION_MARKER_SUFFIX: &str = ".fsqlite-migration-state";

fn migration_marker_path(db_path: &Path) -> PathBuf {
    let mut p: OsString = db_path.as_os_str().to_owned();
    p.push(MIGRATION_MARKER_SUFFIX);
    PathBuf::from(p)
}

async fn ordered_values_fsqlite(conn: &Connection) -> Vec<i64> {
    conn.query("SELECT x FROM t ORDER BY x;")
        .await
        .expect("query ordered values")
        .into_iter()
        .map(|row| match row.get(0) {
            Some(SqliteValue::Integer(v)) => *v,
            other => panic!("expected integer row, got {other:?}"),
        })
        .collect()
}

fn ordered_values_stock(db: &Path) -> Vec<i64> {
    let conn = rusqlite::Connection::open(db).expect("stock open");
    let mut stmt = conn.prepare("SELECT x FROM t ORDER BY x;").expect("prepare");
    stmt.query_map([], |r| r.get::<_, i64>(0))
        .expect("query")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect")
}

fn stock_integrity(db: &Path) -> String {
    let conn = rusqlite::Connection::open(db).expect("stock open");
    conn.query_row("PRAGMA integrity_check;", [], |r| r.get::<_, String>(0))
        .expect("stock integrity_check")
}

/// Reopen the crash-recovered database and require both engines to agree that it
/// holds exactly `expected` and is structurally sound.
async fn assert_recovered_matches(db: &Path, expected: &[i64], label: &str) {
    assert_eq!(
        stock_integrity(db),
        "ok",
        "[{label}] stock integrity_check on the recovered image must be ok"
    );
    let stock_rows = ordered_values_stock(db);
    assert_eq!(stock_rows, expected, "[{label}] stock recovered rows diverged");

    let conn = Connection::open(db.to_string_lossy().as_ref())
        .await
        .expect("reopen recovered db");
    let f_rows = ordered_values_fsqlite(&conn).await;
    conn.close().await.ok();
    assert_eq!(f_rows, expected, "[{label}] fsqlite recovered rows diverged");
    assert_eq!(
        f_rows, stock_rows,
        "[{label}] fsqlite and stock disagree on the recovered rows"
    );
}

fn spawn_kill9_helper(mode: &str, db: &Path) {
    let status = Command::new(env::current_exe().expect("current_exe"))
        .arg("--exact")
        .arg(HELPER_TEST_NAME)
        .arg("--ignored")
        .arg("--nocapture")
        .env(HELPER_MODE_ENV, mode)
        .env(HELPER_DB_PATH_ENV, db.as_os_str())
        .status()
        .expect("spawn kill9 helper");
    assert!(
        !status.success(),
        "the helper must die via abort() (mode={mode}), not exit cleanly"
    );
}

async fn insert_range(conn: &Connection, start: i64, end_exclusive: i64) {
    for v in start..end_exclusive {
        conn.execute_with_params("INSERT INTO t VALUES (?1);", &[SqliteValue::Integer(v)])
            .await
            .expect("insert");
    }
}

async fn setup_table(conn: &Connection) {
    conn.execute("PRAGMA journal_mode = WAL;")
        .await
        .expect("WAL mode");
    conn.execute("CREATE TABLE IF NOT EXISTS t(x INTEGER);")
        .await
        .expect("create table");
}

/// Commit many transactions into an uncheckpointed WAL, then die. The whole
/// committed prefix must replay on reopen.
#[test]
fn kill9_after_many_commits_replays_full_wal() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempdir().expect("tempdir");
        let db = dir.path().join("kill9_many.db");
        spawn_kill9_helper("many_commits", &db);
        assert_recovered_matches(&db, &(0..200).collect::<Vec<_>>(), "many_commits").await;
    });
}

/// An open transaction that never committed must be discarded, leaving the
/// previously committed prefix.
#[test]
fn kill9_mid_uncommitted_discards_partial_batch() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempdir().expect("tempdir");
        let db = dir.path().join("kill9_uncommitted.db");
        // Seed a durable committed prefix first (clean connection, closed).
        {
            let conn = Connection::open(db.to_string_lossy().as_ref())
                .await
                .expect("seed open");
            setup_table(&conn).await;
            conn.execute("BEGIN;").await.expect("begin seed");
            insert_range(&conn, 0, 50).await;
            conn.execute("COMMIT;").await.expect("commit seed");
            conn.close().await.expect("close seed");
        }
        spawn_kill9_helper("uncommitted", &db);
        assert_recovered_matches(&db, &(0..50).collect::<Vec<_>>(), "uncommitted").await;
    });
}

/// Recovery must be deterministic: reopening the crash image repeatedly yields
/// the identical committed prefix every time.
#[test]
fn kill9_recovery_is_deterministic_across_reopens() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempdir().expect("tempdir");
        let db = dir.path().join("kill9_determinism.db");
        spawn_kill9_helper("many_commits", &db);
        let expected: Vec<i64> = (0..200).collect();
        for _ in 0..3 {
            assert_recovered_matches(&db, &expected, "determinism").await;
        }
    });
}

/// The first-open repair pass (bd-zywqc.5) must certify a crash-recovered image,
/// not silently corrupt it. Deleting the marker forces the pass to run on the
/// recovered database as if it were a pre-fix upgrader.
#[test]
fn kill9_recovered_image_passes_first_open_repair() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempdir().expect("tempdir");
        let db = dir.path().join("kill9_repair.db");
        spawn_kill9_helper("many_commits", &db);

        // Force the first-open repair pass to run on the recovered image.
        let _ = std::fs::remove_file(migration_marker_path(&db));
        {
            let conn = Connection::open(db.to_string_lossy().as_ref())
                .await
                .expect("reopen runs the repair pass");
            let lines: Vec<String> = conn
                .query("PRAGMA integrity_check;")
                .await
                .expect("integrity_check")
                .iter()
                .filter_map(|r| match &r.values()[0] {
                    SqliteValue::Text(s) => Some(s.as_ref().to_owned()),
                    _ => None,
                })
                .collect();
            conn.close().await.ok();
            assert_eq!(
                lines,
                vec!["ok".to_owned()],
                "repair pass must certify the crash-recovered image, not corrupt it"
            );
        }
        assert_recovered_matches(&db, &(0..200).collect::<Vec<_>>(), "post_repair").await;
    });
}

// ── crash helper subprocess ────────────────────────────────────────────────

fn helper_many_commits(db: &Path) -> ! {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(db.to_string_lossy().as_ref())
            .await
            .expect("helper open");
        setup_table(&conn).await;
        // 10 committed transactions of 20 rows each -> 200 rows in an
        // uncheckpointed WAL, then die before any checkpoint.
        for batch in 0..10 {
            conn.execute("BEGIN;").await.expect("begin");
            insert_range(&conn, batch * 20, batch * 20 + 20).await;
            conn.execute("COMMIT;").await.expect("commit");
        }
        std::process::abort();
    });
    unreachable!("helper aborts inside the runtime");
}

fn helper_uncommitted(db: &Path) -> ! {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(db.to_string_lossy().as_ref())
            .await
            .expect("helper open");
        setup_table(&conn).await;
        conn.execute("BEGIN;").await.expect("begin uncommitted");
        insert_range(&conn, 50, 100).await;
        std::process::abort();
    });
    unreachable!("helper aborts inside the runtime");
}

#[test]
#[ignore = "invoked via subprocess by the kill9 corpus tests"]
fn kill9_helper_entrypoint() {
    let Some(mode) = env::var_os(HELPER_MODE_ENV) else {
        return;
    };
    let Some(db) = env::var_os(HELPER_DB_PATH_ENV) else {
        return;
    };
    let db = PathBuf::from(db);
    match mode.to_string_lossy().as_ref() {
        "many_commits" => helper_many_commits(&db),
        "uncommitted" => helper_uncommitted(&db),
        other => panic!("unknown kill9 helper mode: {other}"),
    }
}
