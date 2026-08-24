//! GH#303 (bd-qgkoi) keeper: the typed verified-compaction + exact-copy backup
//! facade on `Connection`.
//!
//! - `Connection::backup_exact_to(&Path) -> BackupReport`: a byte-exact copy of
//!   the main database, verified against the source image receipt (logical
//!   hash + geometry) before it is reported.
//! - `Connection::compact_verified_into(&Path) -> CompactionReport`: a
//!   `VACUUM INTO`-backed compaction whose output passed `quick_check` +
//!   `integrity_check`, content-equivalent to the source but not byte-identical.
//!
//! Every produced image is cross-checked with the rusqlite C oracle
//! (integrity_check + row counts + freelist_count), and byte-exactness of the
//! backup is asserted directly against the source main-file bytes.

use asupersync::runtime::{Runtime, RuntimeBuilder};
use fsqlite::Connection;

fn rt() -> Runtime {
    RuntimeBuilder::current_thread().build().expect("runtime")
}

/// C-oracle helpers over a produced image.
fn oracle_integrity(path: &str) -> String {
    let c = rusqlite::Connection::open(path).expect("oracle open");
    c.query_row("PRAGMA integrity_check;", [], |r| r.get::<_, String>(0))
        .expect("oracle integrity_check")
}

fn oracle_count(path: &str, table: &str) -> i64 {
    let c = rusqlite::Connection::open(path).expect("oracle open");
    c.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get::<_, i64>(0))
        .expect("oracle count")
}

fn oracle_freelist_count(path: &str) -> i64 {
    let c = rusqlite::Connection::open(path).expect("oracle open");
    c.query_row("PRAGMA freelist_count;", [], |r| r.get::<_, i64>(0))
        .expect("oracle freelist_count")
}

#[test]
fn backup_exact_to_produces_byte_identical_verified_copy() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.db");
    let copy = dir.path().join("copy.db");
    let src_s = src.to_string_lossy().into_owned();
    let copy_s = copy.to_string_lossy().into_owned();

    let rt = rt();
    let report = rt.block_on(async {
        let conn = Connection::open(src_s.clone()).await.unwrap();
        conn.execute("PRAGMA journal_mode=WAL;").await.unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);")
            .await
            .unwrap();
        for i in 0..64 {
            conn.execute(&format!("INSERT INTO t (id, v) VALUES ({i}, 'row-{i}');"))
                .await
                .unwrap();
        }
        let report = conn.backup_exact_to(&copy).await.unwrap();
        // A second backup of the unchanged db must yield the SAME logical hash
        // (exactness/determinism); target must be a fresh path.
        let copy2 = dir.path().join("copy2.db");
        let report2 = conn.backup_exact_to(&copy2).await.unwrap();
        assert_eq!(
            report.logical_hash_hex, report2.logical_hash_hex,
            "byte-exact backup must be deterministic"
        );
        conn.close().await.unwrap();
        report
    });

    // Report sanity.
    assert!(report.page_count >= 1, "page_count {}", report.page_count);
    assert!(report.page_size >= 512 && report.page_size.is_power_of_two());
    assert_eq!(report.byte_len, u64::from(report.page_count) * u64::from(report.page_size));
    assert_eq!(report.logical_hash_hex.len(), 64, "blake3 hex is 64 chars");

    // Byte-exactness: the copy equals the source main file byte-for-byte
    // (backup checkpointed the source to Truncate, so the main file is the
    // authoritative image).
    let src_bytes = std::fs::read(&src).expect("read source main file");
    let copy_bytes = std::fs::read(&copy).expect("read copy");
    assert_eq!(src_bytes, copy_bytes, "backup must be byte-identical to source");

    // C-oracle: the copy is a valid, integrity-clean database with all rows.
    assert_eq!(oracle_integrity(&copy_s), "ok");
    assert_eq!(oracle_count(&copy_s, "t"), 64);
}

#[test]
fn compact_verified_into_reclaims_free_pages_and_preserves_content() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.db");
    let out = dir.path().join("compact.db");
    let src_s = src.to_string_lossy().into_owned();
    let out_s = out.to_string_lossy().into_owned();

    let rt = rt();
    let (report, surviving) = rt.block_on(async {
        let conn = Connection::open(src_s.clone()).await.unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);")
            .await
            .unwrap();
        // Wide rows so deletions free whole pages.
        for i in 0..400 {
            conn.execute(&format!(
                "INSERT INTO t (id, v) VALUES ({i}, '{}');",
                "x".repeat(200)
            ))
            .await
            .unwrap();
        }
        // Delete ~3/4 of the rows to create a substantial freelist.
        conn.execute("DELETE FROM t WHERE id % 4 <> 0;").await.unwrap();
        let surviving = 100_i64; // ids 0,4,8,... < 400 => 100 rows
        let report = conn.compact_verified_into(&out).await.unwrap();
        conn.close().await.unwrap();
        (report, surviving)
    });

    // Compaction must not grow the image and should reclaim pages after the
    // heavy delete.
    assert!(
        report.compacted_page_count <= report.source_page_count,
        "compacted {} > source {}",
        report.compacted_page_count,
        report.source_page_count
    );
    assert!(
        report.reclaimed_pages > 0,
        "expected reclaimed pages after deleting 3/4 of rows, got {}",
        report.reclaimed_pages
    );
    assert_eq!(report.source_logical_hash_hex.len(), 64);
    assert_eq!(report.compacted_logical_hash_hex.len(), 64);
    assert_ne!(
        report.source_logical_hash_hex, report.compacted_logical_hash_hex,
        "compaction rewrites layout, hashes must differ"
    );

    // C-oracle: compacted image is integrity-clean, fully compact (no free
    // pages), and content-equivalent (surviving rows preserved).
    assert_eq!(oracle_integrity(&out_s), "ok");
    assert_eq!(oracle_freelist_count(&out_s), 0, "compacted image must have no free pages");
    assert_eq!(oracle_count(&out_s, "t"), surviving);

    // The source is left untouched and readable with the same surviving rows.
    assert_eq!(oracle_integrity(&src_s), "ok");
    assert_eq!(oracle_count(&src_s, "t"), surviving);
}

#[test]
fn backup_exact_to_rejects_existing_target_and_memory() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.db");
    let existing = dir.path().join("existing.db");
    std::fs::write(&existing, b"not empty").unwrap();
    let src_s = src.to_string_lossy().into_owned();

    let rt = rt();
    rt.block_on(async {
        let conn = Connection::open(src_s).await.unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY);")
            .await
            .unwrap();
        // Existing target is refused (create-new only).
        let err = conn.backup_exact_to(&existing).await;
        assert!(err.is_err(), "existing target must be refused, got {err:?}");
        conn.close().await.unwrap();

        // Memory-backed connection is unsupported.
        let mem = Connection::open(":memory:".to_owned()).await.unwrap();
        mem.execute("CREATE TABLE t (id INTEGER PRIMARY KEY);")
            .await
            .unwrap();
        let mem_backup = mem.backup_exact_to(dir.path().join("mem_copy.db")).await;
        assert!(mem_backup.is_err(), "memory backup must be unsupported");
        let mem_compact = mem.compact_verified_into(dir.path().join("mem_compact.db")).await;
        assert!(mem_compact.is_err(), "memory compaction must be unsupported");
        mem.close().await.unwrap();
    });
}
