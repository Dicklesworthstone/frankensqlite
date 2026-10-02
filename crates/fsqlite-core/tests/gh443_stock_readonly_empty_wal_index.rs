//! GH#443: a stock read-only connection opened on a WAL database that has no
//! sidecars leaves a 32 KiB `-shm` holding stock's initialized, unindexed empty
//! WAL-index header (`mxFrame = 0`, `szPage = 0`, zero salts) beside a 0-byte
//! `-wal`. A read-write fsqlite COMMIT on those files returned retryable
//! `BusyRecovery`, and a retry in a new process (now beside the 32-byte WAL
//! header the failed attempt wrote) stalled.
//!
//! Stock SQLite's first frame binds such an index to the WAL generation it
//! writes. Expected: both the first COMMIT and a COMMIT beside a header-only
//! WAL of another generation succeed promptly, and stock reads the result.

// The async engine futures nest deeply; match the other integration suites.
#![recursion_limit = "512"]

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use fsqlite_wal::checksum::{
    SqliteWalChecksum, WAL_FORMAT_VERSION, WAL_MAGIC_LE, WalHeader, WalSalts,
};

const SEED_ROWS: i64 = 3;

fn sidecar(db: &Path, suffix: &str) -> PathBuf {
    let mut path = db.as_os_str().to_owned();
    path.push(suffix);
    PathBuf::from(path)
}

fn word(bytes: &[u8], offset: usize) -> u32 {
    u32::from_ne_bytes(bytes[offset..offset + 4].try_into().expect("four bytes"))
}

/// Stock creates a WAL database, closes it (removing both sidecars), then a
/// stock read-only connection reads it and leaves the GH#443 sidecars.
fn stage_stock_readonly_sidecars(db: &Path) {
    {
        let conn = rusqlite::Connection::open(db).expect("stock open");
        let mode: String = conn
            .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
            .expect("wal");
        assert_eq!(mode, "wal");
        conn.execute_batch(
            "CREATE TABLE some_table(id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO some_table(v) VALUES ('a'), ('b'), ('c');",
        )
        .expect("seed");
    }
    assert!(!sidecar(db, "-wal").exists(), "stock's last close removes the WAL");
    {
        let conn = rusqlite::Connection::open_with_flags(
            db,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .expect("stock read-only open");
        let count: i64 = conn
            .query_row("SELECT count(*) FROM some_table", [], |row| row.get(0))
            .expect("stock read-only count");
        assert_eq!(count, SEED_ROWS);
    }
    let shm = std::fs::read(sidecar(db, "-shm")).expect("stock leaves -shm");
    assert_eq!(shm.len(), 32_768);
    assert_eq!(word(&shm, 12) & 0xff, 1, "isInit");
    assert_eq!(word(&shm, 16), 0, "mxFrame");
    let wal_len = std::fs::metadata(sidecar(db, "-wal")).expect("stock leaves -wal").len();
    assert_eq!(wal_len, 0, "stock leaves a 0-byte WAL");
}

async fn commit_ddl_and_row(path: &str, value: i64) {
    let started = Instant::now();
    let conn = Connection::open(path).await.expect("fsqlite read-write open");
    conn.execute("BEGIN").await.expect("begin");
    conn.execute("CREATE TABLE IF NOT EXISTS gh443(x)")
        .await
        .expect("ddl");
    conn.execute(&format!("INSERT INTO gh443 VALUES ({value})"))
        .await
        .expect("insert");
    conn.execute("COMMIT")
        .await
        .expect("COMMIT beside stock's unindexed empty WAL index");
    let rows = conn.query("SELECT count(*) FROM some_table").await.expect("read");
    assert_eq!(rows[0].values()[0], SqliteValue::Integer(SEED_ROWS));
    drop(conn);
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "the commit must not wait out a busy loop"
    );
}

fn stock_rows(db: &Path) -> Vec<i64> {
    let conn = rusqlite::Connection::open(db).expect("stock reopen");
    let check: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity_check");
    assert_eq!(check, "ok");
    let mut stmt = conn.prepare("SELECT x FROM gh443 ORDER BY x").expect("prepare");
    stmt.query_map([], |row| row.get(0))
        .expect("query")
        .collect::<Result<_, _>>()
        .expect("rows")
}

#[test]
fn commit_binds_stock_unindexed_empty_index_beside_empty_wal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("gh443.db");
    stage_stock_readonly_sidecars(&db);
    let path = db.to_str().expect("utf-8").to_owned();
    asupersync::test_utils::run_test(|| async {
        commit_ddl_and_row(&path, 1).await;
        // A second process-like open finds the index fsqlite published.
        commit_ddl_and_row(&path, 2).await;
    });
    assert_eq!(stock_rows(&db), vec![1, 2]);
}

/// The state a failed 0.4.7 attempt left behind: a header-only WAL of a fresh
/// generation beside stock's unindexed empty index. The retry used to stall.
#[test]
fn commit_binds_stock_unindexed_empty_index_beside_header_only_wal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("gh443_retry.db");
    stage_stock_readonly_sidecars(&db);
    let header = WalHeader {
        magic: WAL_MAGIC_LE,
        format_version: WAL_FORMAT_VERSION,
        page_size: 4096,
        checkpoint_seq: 0,
        salts: WalSalts {
            salt1: 0x4a43_1c55,
            salt2: 0x0123_9e77,
        },
        checksum: SqliteWalChecksum { s1: 0, s2: 0 },
    };
    std::fs::write(
        sidecar(&db, "-wal"),
        header.to_bytes().expect("WAL header bytes"),
    )
    .expect("write header-only WAL");
    let path = db.to_str().expect("utf-8").to_owned();
    asupersync::test_utils::run_test(|| async {
        commit_ddl_and_row(&path, 7).await;
    });
    assert_eq!(stock_rows(&db), vec![7]);
}
