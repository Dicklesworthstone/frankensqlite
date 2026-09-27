//! GH#430: a read-only open of a WAL database whose `-shm` header is older than
//! its reader marks (every in-use mark above `mxFrame`, nothing holding the
//! files) returned retryable `BusyRecovery` forever. Readers only publish marks
//! at or below the `mxFrame` they saw, so such marks prove the header is stale;
//! a read-only opener cannot publish a mark to escape it.
//!
//! Expected, matching stock SQLite:
//! - the WAL-index-recovery read-only open rebuilds the index (resetting the
//!   marks) and reads every committed row;
//! - a plain read-only open, which may not rebuild it, fails fast with a
//!   non-retryable read-only error (SQLITE_READONLY_RECOVERY) instead of a
//!   busy that callers spin on.
//!
//! GH#431 (same admission path): the initialized, empty index stock SQLite
//! writes beside a settled, frame-free WAL (`szPage = 0`, zero salts, valid
//! checksum) must be accepted by a read-only open, not reported as recovery.

// The async engine futures nest deeply; match the other integration suites.
#![recursion_limit = "512"]

use std::path::{Path, PathBuf};

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

const ROWS: i64 = 12;
/// Byte offset of the header's `mxFrame` and of reader mark `slot` in `-shm`.
const MX_FRAME_OFFSET: usize = 16;
fn read_mark_offset(slot: usize) -> usize {
    100 + slot * 4
}

fn shm_path(db: &Path) -> PathBuf {
    let mut path = db.as_os_str().to_owned();
    path.push("-shm");
    PathBuf::from(path)
}

fn word(bytes: &[u8], offset: usize) -> u32 {
    u32::from_ne_bytes(bytes[offset..offset + 4].try_into().expect("four bytes"))
}

/// Leave `path` as a WAL database with ROWS committed rows still in the WAL,
/// then raise reader marks 1 and 2 above the header's `mxFrame`.
async fn stage_stale_reader_marks(path: &str) -> u32 {
    let conn = Connection::open(path).await.expect("open writer");
    conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
    conn.execute("PRAGMA wal_autocheckpoint=0").await.expect("no autocheckpoint");
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .await
        .expect("create");
    for id in 1..=ROWS {
        conn.execute(&format!("INSERT INTO t VALUES ({id}, 'v{id}')"))
            .await
            .expect("insert");
    }
    conn.close_without_checkpoint().await.expect("close without checkpoint");

    let shm = shm_path(Path::new(path));
    let mut bytes = std::fs::read(&shm).expect("the -shm sidecar survives the close");
    assert!(bytes.len() >= 136, "-shm holds the header and checkpoint info");
    let mx_frame = word(&bytes, MX_FRAME_OFFSET);
    assert!(mx_frame > 0, "the committed rows are still in the WAL");
    for (slot, mark) in [(1, mx_frame + 13), (2, mx_frame + 12)] {
        let offset = read_mark_offset(slot);
        bytes[offset..offset + 4].copy_from_slice(&mark.to_ne_bytes());
    }
    std::fs::write(&shm, &bytes).expect("rewrite -shm");
    mx_frame
}

async fn count_rows(conn: &Connection) -> Result<i64, FrankenError> {
    let rows = conn.query("SELECT count(*) FROM t").await?;
    match rows.first().map(|row| row.values()[0].clone()) {
        Some(SqliteValue::Integer(count)) => Ok(count),
        other => panic!("count(*) returned {other:?}"),
    }
}

#[test]
fn stale_reader_marks_recover_or_fail_fast_instead_of_busy_forever() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("gh430.db").to_str().expect("utf-8").to_owned();
        let mx_frame = stage_stale_reader_marks(&path).await;

        // Plain read-only: may not rebuild the index, so it must fail fast and
        // non-retryably rather than returning BusyRecovery forever.
        let started = std::time::Instant::now();
        let plain = match Connection::open_schema_only(path.clone()).await {
            Ok(conn) => count_rows(&conn).await,
            Err(error) => Err(error),
        };
        let error = plain.expect_err("a stale index cannot be read without rebuilding it");
        assert!(
            matches!(error, FrankenError::ReadOnly),
            "expected a read-only (recovery needed) error, got {error:?}"
        );
        assert!(!error.is_transient(), "the condition never clears; do not invite retries");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "the plain open must not spin until a busy timeout"
        );

        // WAL-index-recovery read-only: rebuilds the index and sees every row.
        let recovering = Connection::open_schema_only_with_wal_index_recovery(path.clone())
            .await
            .expect("recovery-mode open succeeds");
        assert_eq!(count_rows(&recovering).await.expect("read after recovery"), ROWS);
        drop(recovering);

        // The rebuild reset the marks like stock recovery, so the header and
        // marks agree again and a plain read-only open now works too.
        let bytes = std::fs::read(shm_path(Path::new(&path))).expect("read -shm");
        assert!(word(&bytes, MX_FRAME_OFFSET) >= mx_frame);
        let plain = Connection::open_schema_only(path.clone())
            .await
            .expect("plain read-only open after the rebuild");
        assert_eq!(count_rows(&plain).await.expect("plain read after rebuild"), ROWS);
    });
}

/// The 48-byte WAL-index header stock SQLite 3.51 writes beside a header-only
/// WAL (native little-endian, from the GH#431 report): initialized, empty,
/// `szPage = 0`, zero salts, valid checksum.
#[cfg(target_endian = "little")]
const STOCK_UNINDEXED_EMPTY_HEADER: [u8; 48] = [
    0x18, 0xe2, 0x2d, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x38, 0x07, 0x18, 0x06, 0x35,
    0x93, 0xdb, 0x09,
];

#[cfg(target_endian = "little")]
#[test]
fn read_only_open_accepts_stock_unindexed_empty_index() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("gh431.db").to_str().expect("utf-8").to_owned();
        let conn = Connection::open(&path).await.expect("open writer");
        conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .await
            .expect("create");
        for id in 1..=ROWS {
            conn.execute(&format!("INSERT INTO t VALUES ({id}, 'v{id}')"))
                .await
                .expect("insert");
        }
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").await.expect("settle the WAL");
        conn.close_without_checkpoint().await.expect("close");

        let wal = {
            let mut p = std::ffi::OsString::from(&path);
            p.push("-wal");
            PathBuf::from(p)
        };
        let wal_len = std::fs::metadata(&wal).map_or(0, |meta| meta.len());
        assert!(wal_len <= 32, "the settled WAL holds no frames (len {wal_len})");
        // The reported shape: a valid 32-byte header-only WAL with real salts.
        let header = fsqlite_wal::WalHeader {
            magic: fsqlite_wal::WAL_MAGIC_LE,
            format_version: fsqlite_wal::WAL_FORMAT_VERSION,
            page_size: 4096,
            checkpoint_seq: 1,
            salts: fsqlite_wal::WalSalts {
                salt1: 0x1234_5678,
                salt2: 0x9abc_def0,
            },
            checksum: fsqlite_wal::SqliteWalChecksum { s1: 0, s2: 0 },
        };
        std::fs::write(&wal, header.to_bytes().expect("encode WAL header"))
            .expect("write header-only WAL");

        let shm = shm_path(Path::new(&path));
        let mut bytes = std::fs::read(&shm).expect("the -shm sidecar survives the close");
        bytes[..48].copy_from_slice(&STOCK_UNINDEXED_EMPTY_HEADER);
        bytes[48..96].copy_from_slice(&STOCK_UNINDEXED_EMPTY_HEADER);
        std::fs::write(&shm, &bytes).expect("install stock's empty index");

        let reader = Connection::open_schema_only(path.clone())
            .await
            .expect("read-only open accepts stock's empty index");
        assert_eq!(count_rows(&reader).await.expect("read-only count"), ROWS);
    });
}
