//! Crash recovery and WAL replay correctness tests.
//!
//! Bead: bd-3tc7 (5F.4)
//!
//! These tests simulate unclean shutdown via `std::process::abort()` in a
//! subprocess so `Drop`-time checkpointing is skipped. The parent process then
//! reopens the database and verifies crash-recovery semantics.

use std::env;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;

use fsqlite::Connection;
use fsqlite_types::SqliteValue;
use tempfile::tempdir;

const HELPER_MODE_ENV: &str = "FSQLITE_CRASH_HELPER_MODE";
const HELPER_DB_PATH_ENV: &str = "FSQLITE_CRASH_HELPER_DB_PATH";
const HELPER_TEST_NAME: &str = "crash_helper_entrypoint";

fn wal_path_for_db(db_path: &Path) -> PathBuf {
    let mut wal: OsString = db_path.as_os_str().to_owned();
    wal.push("-wal");
    PathBuf::from(wal)
}

fn row_count(conn: &Connection) -> i64 {
    let row = conn
        .query_row("SELECT COUNT(*) FROM t;")
        .expect("count query");
    match row.get(0) {
        Some(SqliteValue::Integer(count)) => *count,
        other => panic!("expected integer COUNT(*), got {other:?}"),
    }
}

fn insert_range(conn: &Connection, start: i64, end_exclusive: i64) {
    for value in start..end_exclusive {
        conn.execute_with_params("INSERT INTO t VALUES (?1);", &[SqliteValue::Integer(value)])
            .expect("insert value");
    }
}

fn setup_table(conn: &Connection) {
    conn.execute("PRAGMA journal_mode = WAL;")
        .expect("enable WAL mode");
    conn.execute("CREATE TABLE IF NOT EXISTS t(x INTEGER);")
        .expect("create table");
}

fn spawn_crash_helper(mode: &str, db_path: &Path) {
    let helper_status = Command::new(env::current_exe().expect("current_exe"))
        .arg("--exact")
        .arg(HELPER_TEST_NAME)
        .arg("--ignored")
        .arg("--nocapture")
        .env(HELPER_MODE_ENV, mode)
        .env(HELPER_DB_PATH_ENV, db_path.as_os_str())
        .status()
        .expect("spawn crash helper");

    assert!(
        !helper_status.success(),
        "helper should abort for mode={mode}"
    );
}

fn helper_mode_committed(db_path: &Path) -> ! {
    let conn = Connection::open(db_path.to_string_lossy().as_ref()).expect("open helper db");
    setup_table(&conn);
    conn.execute("BEGIN;").expect("begin committed helper txn");
    insert_range(&conn, 0, 100);
    conn.execute("COMMIT;").expect("commit helper txn");
    std::process::abort();
}

fn helper_mode_uncommitted(db_path: &Path) -> ! {
    let conn = Connection::open(db_path.to_string_lossy().as_ref()).expect("open helper db");
    setup_table(&conn);
    conn.execute("BEGIN;")
        .expect("begin uncommitted helper txn");
    insert_range(&conn, 50, 100);
    std::process::abort();
}

fn helper_mode_mixed(db_path: &Path) -> ! {
    let conn = Connection::open(db_path.to_string_lossy().as_ref()).expect("open helper db");
    setup_table(&conn);

    conn.execute("BEGIN;").expect("begin txn1");
    insert_range(&conn, 0, 10);
    conn.execute("COMMIT;").expect("commit txn1");

    conn.execute("BEGIN;").expect("begin txn2");
    insert_range(&conn, 10, 20);
    conn.execute("COMMIT;").expect("commit txn2");

    conn.execute("BEGIN;").expect("begin txn3");
    insert_range(&conn, 20, 30);
    std::process::abort();
}

#[test]
fn committed_transaction_survives_crash_recovery() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("committed_survives.db");
    let wal_path = wal_path_for_db(&db_path);

    spawn_crash_helper("committed", &db_path);

    let wal_meta = std::fs::metadata(&wal_path).expect("wal exists after crash");
    assert!(
        wal_meta.len() > 32,
        "expected non-empty WAL after crash, len={}",
        wal_meta.len()
    );

    let conn = Connection::open(db_path.to_string_lossy().as_ref()).expect("open recovered db");
    assert_eq!(row_count(&conn), 100, "committed rows must survive crash");
}

#[test]
fn uncommitted_transaction_is_discarded_after_crash() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("uncommitted_discarded.db");

    {
        let conn = Connection::open(db_path.to_string_lossy().as_ref()).expect("open seed db");
        setup_table(&conn);
        conn.execute("BEGIN;").expect("begin seed txn");
        insert_range(&conn, 0, 50);
        conn.execute("COMMIT;").expect("commit seed txn");
        conn.close().expect("close seed connection");
    }

    spawn_crash_helper("uncommitted", &db_path);

    let conn = Connection::open(db_path.to_string_lossy().as_ref()).expect("open recovered db");
    assert_eq!(
        row_count(&conn),
        50,
        "uncommitted rows must be discarded after crash"
    );
}

#[test]
fn only_committed_prefix_survives_multi_transaction_crash() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("multi_commit_prefix.db");

    spawn_crash_helper("mixed", &db_path);

    let conn = Connection::open(db_path.to_string_lossy().as_ref()).expect("open recovered db");
    assert_eq!(
        row_count(&conn),
        20,
        "two committed transactions should survive while trailing uncommitted writes are discarded"
    );
}

#[test]
#[ignore = "invoked via subprocess by crash recovery tests"]
fn crash_helper_entrypoint() {
    let Some(mode) = env::var_os(HELPER_MODE_ENV) else {
        return;
    };
    let Some(db_path) = env::var_os(HELPER_DB_PATH_ENV) else {
        return;
    };
    let db_path = PathBuf::from(db_path);

    match mode.to_string_lossy().as_ref() {
        "committed" => helper_mode_committed(&db_path),
        "uncommitted" => helper_mode_uncommitted(&db_path),
        "mixed" => helper_mode_mixed(&db_path),
        other => panic!("unknown crash helper mode: {other}"),
    }
}
