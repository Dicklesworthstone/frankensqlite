//! Regression for GH #292: a WAL sidecar whose 32-byte header fails
//! magic/page-size/checksum validation must be treated as EMPTY — exactly what
//! stock SQLite's `walIndexRecover` does — instead of failing the whole
//! connection open with `WalCorrupt`.
//!
//! The reported incident: a healthy ~9 GB archive (native `PRAGMA
//! integrity_check` = ok) carried an unusable `-wal` sidecar; FrankenSQLite
//! logged "WAL header checksum mismatch — file may be corrupt", refused every
//! open, and the embedding application (CASS) stalled in its rebuild pipeline
//! until an RSS watchdog killed it. Stock SQLite opens the same database and
//! silently ignores the sidecar.

#![cfg(all(feature = "native", unix))]

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

/// Build a committed, checkpointed database with rusqlite (stock SQLite),
/// so all rows live in the main database file, then return its path.
fn build_stock_database(dir: &tempfile::TempDir) -> std::path::PathBuf {
    let path = dir.path().join("gh292.db");
    let conn = rusqlite::Connection::open(&path).expect("create stock database");
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         CREATE TABLE probe(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
         INSERT INTO probe(value) VALUES ('alpha'), ('beta');
         PRAGMA wal_checkpoint(TRUNCATE);",
    )
    .expect("seed and checkpoint stock database");
    drop(conn);
    path
}

async fn assert_fsqlite_reads_rows(path: &std::path::Path) {
    let conn = Connection::open(path.to_str().unwrap())
        .await
        .expect("open must treat the unusable WAL sidecar as empty (GH #292)");
    let rows = conn
        .query("SELECT value FROM probe ORDER BY id")
        .await
        .expect("query rows from the main database");
    let values: Vec<_> = rows.iter().map(|row| row.values()[0].clone()).collect();
    assert_eq!(
        values,
        vec![
            SqliteValue::Text("alpha".to_owned().into()),
            SqliteValue::Text("beta".to_owned().into()),
        ]
    );
}

#[test]
fn open_treats_garbage_wal_sidecar_as_empty() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let path = build_stock_database(&dir);

        // A garbage sidecar (invalid magic) as left behind by an unrelated crash.
        let wal_path = dir.path().join("gh292.db-wal");
        std::fs::write(&wal_path, vec![0xA5_u8; 4096]).expect("plant garbage WAL sidecar");

        // Stock SQLite accepts this database as-is.
        let stock = rusqlite::Connection::open(&path).expect("stock open");
        let ok: String = stock
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect("stock integrity_check");
        assert_eq!(
            ok, "ok",
            "oracle: stock SQLite treats the database as healthy"
        );
        drop(stock);
        std::fs::write(&wal_path, vec![0xA5_u8; 4096]).expect("re-plant garbage WAL sidecar");

        assert_fsqlite_reads_rows(&path).await;
    });
}

#[test]
fn open_treats_checksum_flipped_wal_header_as_empty() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let path = build_stock_database(&dir);

        // The torn-header shape from the issue: a `-wal` whose header checksum
        // does not match its contents. Stock SQLite drops such a WAL silently
        // (treat-as-empty), so FrankenSQLite must too.
        let wal_path = dir.path().join("gh292.db-wal");
        // A stock-shaped header (valid magic, format version, page size) whose
        // checksum words do not match the covered bytes.
        let mut header = vec![0_u8; 32];
        header[..4].copy_from_slice(&0x377f_0682_u32.to_be_bytes()); // magic (LE checksums)
        header[4..8].copy_from_slice(&3_007_000_u32.to_be_bytes()); // format version
        header[8..12].copy_from_slice(&4096_u32.to_be_bytes()); // page size
        header[24..28].copy_from_slice(&0xDEAD_BEEF_u32.to_be_bytes()); // checksum-1: wrong
        std::fs::write(&wal_path, header).expect("plant checksum-mismatched WAL header");

        // Oracle: stock SQLite still opens and reads the database.
        let stock = rusqlite::Connection::open(&path).expect("stock open with bad-checksum WAL");
        let n: i64 = stock
            .query_row("SELECT COUNT(*) FROM probe", [], |row| row.get(0))
            .expect("stock read");
        assert_eq!(n, 2);
        drop(stock);
        // Re-plant: the stock open may have rewritten/removed the sidecar.
        let mut header = vec![0_u8; 32];
        header[..4].copy_from_slice(&0x377f_0682_u32.to_be_bytes());
        header[4..8].copy_from_slice(&3_007_000_u32.to_be_bytes());
        header[8..12].copy_from_slice(&4096_u32.to_be_bytes());
        header[24..28].copy_from_slice(&0xDEAD_BEEF_u32.to_be_bytes());
        std::fs::write(&wal_path, header).expect("re-plant checksum-mismatched WAL header");

        assert_fsqlite_reads_rows(&path).await;
    });
}
