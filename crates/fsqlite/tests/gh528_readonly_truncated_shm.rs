//! GH#528 through the compat read-only open beads_rust (`br`) reads with:
//! a stock first attacher that dies after truncating `-shm` to 3 bytes left
//! every later read-only open answering `BusyRecovery`, with nothing holding
//! the database. Both WAL shapes from the field are covered: committed frames,
//! and the 32-byte header-only WAL of the reported tracker.
//!
//! The rebuild and its fail-closed boundaries are covered in
//! `fsqlite-core/tests/gh528_truncated_shm_first_attacher.rs`.

#![cfg(all(unix, feature = "native"))]
#![recursion_limit = "512"]

use fsqlite::compat::{OpenFlags, open_with_flags};
use fsqlite_types::SqliteValue;

const ROWS: i64 = 30;

fn sidecar(path: &std::path::Path, suffix: &str) -> std::path::PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    name.into()
}

fn expected_rows() -> Vec<(i64, String)> {
    (1..=ROWS).map(|id| (id, format!("issue-{id}"))).collect()
}

async fn frank_rows(conn: &fsqlite::Connection, context: &str) -> Vec<(i64, String)> {
    conn.query("SELECT id, v FROM t ORDER BY id")
        .await
        .unwrap_or_else(|error| panic!("{context}: read-only query failed: {error}"))
        .iter()
        .map(|row| match row.values() {
            [SqliteValue::Integer(id), SqliteValue::Text(v)] => (*id, v.to_string()),
            other => panic!("{context}: unexpected row {other:?}"),
        })
        .collect()
}

#[test]
fn compat_read_only_open_rebuilds_a_three_byte_shm() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        for shape in ["frames", "header-only"] {
            let path = dir.path().join(format!("{shape}.db"));
            let writer = fsqlite_core::connection::Connection::open(path.to_str().unwrap())
                .await
                .expect("open writer");
            for sql in [
                "PRAGMA journal_mode=WAL",
                "PRAGMA wal_autocheckpoint=0",
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)",
            ] {
                writer.execute(sql).await.expect(sql);
            }
            for (id, v) in expected_rows() {
                writer
                    .execute(&format!("INSERT INTO t VALUES ({id}, '{v}')"))
                    .await
                    .expect("insert");
            }
            if shape == "header-only" {
                writer
                    .execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    .await
                    .expect("settle the WAL");
            }
            writer
                .close_without_checkpoint()
                .await
                .expect("close writer");
            let wal = sidecar(&path, "-wal");
            if shape == "header-only" {
                let header = fsqlite_wal::WalHeader {
                    magic: fsqlite_wal::WAL_MAGIC_LE,
                    format_version: fsqlite_wal::WAL_FORMAT_VERSION,
                    page_size: 4096,
                    checkpoint_seq: 1,
                    salts: fsqlite_wal::WalSalts {
                        salt1: 0x0528_0528,
                        salt2: 0x1234_abcd,
                    },
                    checksum: fsqlite_wal::SqliteWalChecksum { s1: 0, s2: 0 },
                };
                std::fs::write(&wal, header.to_bytes().unwrap()).unwrap();
            }
            let wal_bytes = std::fs::read(&wal).unwrap();
            let shm = sidecar(&path, "-shm");
            std::fs::OpenOptions::new()
                .write(true)
                .open(&shm)
                .unwrap()
                .set_len(3)
                .unwrap();

            for attempt in 0..3 {
                let context = format!("{shape} WAL, open {attempt}");
                let reader =
                    open_with_flags(path.to_str().unwrap(), OpenFlags::SQLITE_OPEN_READ_ONLY)
                        .await
                        .unwrap_or_else(|error| {
                            panic!("{context}: read-only open failed: {error}")
                        });
                assert_eq!(
                    frank_rows(&reader, &context).await,
                    expected_rows(),
                    "{context}"
                );
                reader.close().await.expect("close reader");
            }
            assert_eq!(
                std::fs::read(&wal).unwrap(),
                wal_bytes,
                "{shape}: WAL unchanged"
            );
            let rebuilt = std::fs::read(&shm).unwrap();
            assert_eq!(rebuilt.len(), 32 * 1024, "{shape}: one full index region");
            assert!(
                fsqlite_wal::wal_index::parse_shm_header(&rebuilt)
                    .unwrap()
                    .is_some(),
                "{shape}: the rebuilt index has a valid header"
            );

            let stock = rusqlite::Connection::open_with_flags(
                &path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .expect("stock read-only open");
            let count: i64 = stock
                .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
                .expect("stock count");
            assert_eq!(count, ROWS, "{shape}: stock agrees");
        }
    });
}
