//! GH#515: a read-only open of a WAL database whose `-wal` is shorter than its
//! 32-byte header and whose `-shm` is missing must read the main file, as
//! 0.4.10 and stock SQLite do. 0.4.12 bound such a WAL header-less (bd-yxqs9)
//! and then asked for a WAL-index rebuild that a read-only pager may not
//! perform, so every read answered `BusyRecovery` (beads_rust could not adopt
//! 0.4.12). With no `-shm` no WAL-mode connection is live, and the WAL has no
//! frames, so the main file is the whole database.
//!
//! The bd-yxqs9 case, where a live stock writer truncated its WAL and still
//! holds `-shm`, is covered by `gh519_stock_truncated_wal_reader.rs`; this file
//! also checks that a fresh reader sees commits once a writer is live again.

#![cfg(all(unix, feature = "native"))]

use fsqlite::compat::{OpenFlags, open_with_flags};
use fsqlite_types::SqliteValue;

fn stock_rows(path: &std::path::Path) -> Vec<(i64, String)> {
    let stock =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("stock read-only open");
    stock
        .prepare("SELECT id, v FROM t ORDER BY id")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .expect("stock rows")
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

fn sidecar(path: &std::path::Path, suffix: &str) -> std::path::PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    name.into()
}

#[test]
fn readonly_open_reads_main_file_under_headerless_wal_without_shm() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let source = dir.path().join("source.db");
        {
            let stock = rusqlite::Connection::open(&source).expect("stock writer");
            stock
                .execute_batch(
                    "PRAGMA journal_mode=WAL;
                     CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL);
                     INSERT INTO t(v) VALUES ('alpha'), ('beta'), ('gamma');",
                )
                .expect("seed stock database");
        }
        assert!(
            !sidecar(&source, "-wal").exists(),
            "stock close removes the WAL"
        );
        let main_bytes = std::fs::read(&source).unwrap();
        assert_eq!(&main_bytes[18..20], &[2, 2], "database stays in WAL format");

        for (kind, wal_bytes) in [("zero-byte", Vec::new()), ("short", vec![0x37; 16])] {
            let path = dir.path().join(format!("{kind}.db"));
            std::fs::copy(&source, &path).unwrap();
            let wal = sidecar(&path, "-wal");
            let shm = sidecar(&path, "-shm");
            std::fs::write(&wal, &wal_bytes).unwrap();

            // The oracle reads a copy: stock creates `-shm` beside what it opens.
            let oracle = dir.path().join(format!("{kind}-oracle.db"));
            std::fs::copy(&path, &oracle).unwrap();
            std::fs::write(sidecar(&oracle, "-wal"), &wal_bytes).unwrap();
            let expected = stock_rows(&oracle);
            assert_eq!(expected.len(), 3, "{kind}: stock reads the main file");

            let reader = open_with_flags(path.to_str().unwrap(), OpenFlags::SQLITE_OPEN_READ_ONLY)
                .await
                .unwrap_or_else(|error| panic!("{kind}: read-only open failed: {error}"));
            for attempt in 0..3 {
                let context = format!("{kind} WAL, read {attempt}");
                assert_eq!(frank_rows(&reader, &context).await, expected, "{context}");
            }
            let refused = reader
                .execute("INSERT INTO t(v) VALUES ('refused')")
                .await
                .expect_err("read-only connection must refuse writes");
            assert!(
                refused.to_string().contains("readonly"),
                "{kind}: write refused as read-only, got {refused}"
            );
            reader.close().await.expect("close reader");

            assert_eq!(
                std::fs::read(&path).unwrap(),
                main_bytes,
                "{kind}: main file unchanged"
            );
            assert_eq!(
                std::fs::read(&wal).unwrap(),
                wal_bytes,
                "{kind}: WAL unchanged"
            );
            assert!(!shm.exists(), "{kind}: read-only open must not create -shm");

            // Once a writer is live again its commits reach a fresh reader.
            let stock = rusqlite::Connection::open(&path).expect("stock writer reopens");
            stock
                .execute("INSERT INTO t(v) VALUES ('delta')", [])
                .expect("stock commit after reopen");
            let fresh = open_with_flags(path.to_str().unwrap(), OpenFlags::SQLITE_OPEN_READ_ONLY)
                .await
                .unwrap_or_else(|error| panic!("{kind}: fresh read-only open failed: {error}"));
            let context = format!("{kind} WAL, fresh reader after a live commit");
            let rows = frank_rows(&fresh, &context).await;
            assert_eq!(rows.len(), 4, "{context}: {rows:?}");
            assert_eq!(rows[3], (4, "delta".to_owned()), "{context}");
            fresh.close().await.expect("close fresh reader");
            drop(stock);
        }
    });
}
