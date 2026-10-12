//! bd-yxqs9 (cass GH#519): a stock SQLite writer that truncates its WAL to
//! zero bytes (`PRAGMA wal_checkpoint(TRUNCATE)`) must not make a
//! FrankenSQLite read-only connection report `WalCorrupt`.
//!
//! The incident: cass exports an OpenCode session through a read-only fsqlite
//! connection while OpenCode's bun:sqlite writer keeps committing and
//! truncating its WAL. Some exports failed with "WAL file is corrupt: WAL file
//! too small for header during rebuild: read 0, need 32". Stock SQLite reads a
//! WAL shorter than its 32-byte header as one with no frames, so the database
//! file alone is current.
//!
//! Two negative cases a naive fix fails:
//! - rounds without a truncation leave committed rows only in the WAL, so a
//!   reader that ignores the WAL ("treat anything odd as empty") under-counts;
//! - runs of consecutive truncating rounds change the database file while
//!   the WAL is zero bytes before and after, so a reader that keeps its old
//!   view because "the WAL is still empty" serves stale rows.

#![cfg(feature = "native")]

use fsqlite::compat::{OpenFlags, open_with_flags};
use fsqlite_types::SqliteValue;

const ROUNDS: i64 = 48;

/// Rounds 3, 5, 6 and 7 of every 8 truncate: one isolated truncation and one
/// run of three.
fn truncates(round: i64) -> bool {
    matches!(round % 8, 3 | 5 | 6 | 7)
}

async fn committed_rows(conn: &fsqlite::Connection, context: &str) -> (i64, i64) {
    let rows = conn
        .query("SELECT COUNT(*), MAX(id) FROM t")
        .await
        .unwrap_or_else(|error| panic!("{context}: read-only query failed: {error}"));
    let values = rows[0].values();
    match (&values[0], &values[1]) {
        (SqliteValue::Integer(count), SqliteValue::Integer(max_id)) => (*count, *max_id),
        other => panic!("{context}: unexpected row {other:?}"),
    }
}

#[test]
fn read_only_reader_survives_stock_truncate_checkpoints() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("gh519.db");
        let wal_path = dir.path().join("gh519.db-wal");
        let path_str = path.to_str().unwrap().to_owned();

        let stock = rusqlite::Connection::open(&path).expect("stock writer");
        stock
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL);
                 INSERT INTO t(v) VALUES ('seed');",
            )
            .expect("seed stock database");

        let long_lived = open_with_flags(&path_str, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .await
            .expect("long-lived read-only open");

        let mut expected = 1_i64;
        let mut zero_length_reads = 0;
        let mut wal_only_reads = 0;
        for round in 0..ROUNDS {
            stock
                .execute("INSERT INTO t(v) VALUES (?1)", [format!("row{round}")])
                .expect("stock commit");
            expected += 1;
            if truncates(round) {
                let busy: i64 = stock
                    .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))
                    .expect("stock TRUNCATE checkpoint");
                assert_eq!(busy, 0, "round {round}: no reader holds a snapshot here");
            }
            let wal_len = std::fs::metadata(&wal_path).map_or(0, |meta| meta.len());
            if wal_len == 0 {
                zero_length_reads += 1;
            } else {
                wal_only_reads += 1;
            }

            let context = format!("round {round}, long-lived reader, WAL {wal_len} bytes");
            assert_eq!(
                committed_rows(&long_lived, &context).await,
                (expected, expected),
                "{context}: must see every committed row"
            );

            let fresh = open_with_flags(&path_str, OpenFlags::SQLITE_OPEN_READ_ONLY)
                .await
                .unwrap_or_else(|error| {
                    panic!("round {round}: fresh read-only open, WAL {wal_len} bytes: {error}")
                });
            let context = format!("round {round}, fresh reader, WAL {wal_len} bytes");
            assert_eq!(
                committed_rows(&fresh, &context).await,
                (expected, expected),
                "{context}: must see every committed row"
            );
            fresh.close().await.expect("close fresh reader");
        }

        // Oracle for the scenario itself: both shapes were exercised.
        assert_eq!(
            zero_length_reads,
            ROUNDS / 2,
            "every truncating round left 0 bytes"
        );
        assert!(wal_only_reads > 0, "{wal_only_reads}");
        long_lived.close().await.expect("close long-lived reader");
    });
}

/// A read-only connection opened while the WAL is zero bytes has no WAL
/// generation to bind yet. It must still see the commits the stock writer
/// makes afterwards, which live only in the WAL until the next truncation.
#[test]
fn reader_opened_on_a_truncated_wal_sees_later_commits() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("gh519_open.db");
        let wal_path = dir.path().join("gh519_open.db-wal");
        let path_str = path.to_str().unwrap().to_owned();

        let stock = rusqlite::Connection::open(&path).expect("stock writer");
        stock
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL);
                 INSERT INTO t(v) VALUES ('seed');
                 PRAGMA wal_checkpoint(TRUNCATE);",
            )
            .expect("seed and truncate");
        assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), 0);

        let reader = open_with_flags(&path_str, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .await
            .expect("read-only open on a zero-length WAL");
        assert_eq!(committed_rows(&reader, "after open").await, (1, 1));

        let mut expected = 1_i64;
        for round in 0..12 {
            stock
                .execute("INSERT INTO t(v) VALUES (?1)", [format!("row{round}")])
                .expect("stock commit");
            expected += 1;
            if round % 3 == 2 {
                stock
                    .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                    .expect("stock TRUNCATE checkpoint");
            }
            let wal_len = std::fs::metadata(&wal_path).map_or(0, |meta| meta.len());
            let context = format!("round {round}, reader opened on 0 bytes, WAL {wal_len} bytes");
            assert_eq!(
                committed_rows(&reader, &context).await,
                (expected, expected),
                "{context}: must see every committed row"
            );
        }
        reader.close().await.expect("close reader");
    });
}
