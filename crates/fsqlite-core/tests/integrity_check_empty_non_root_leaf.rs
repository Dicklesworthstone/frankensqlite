//! bd-y5urj: `PRAGMA integrity_check` must flag an empty non-root B-tree leaf
//! that is still referenced by its parent — stock sqlite3 3.46.1 calls that
//! shape malformed (it cannot even parse the page: `integrity_check`,
//! `quick_check`, and a plain `SELECT` all raise "database disk image is
//! malformed"). The invariant is enforced in the integrity b-tree walk
//! (`connection.rs::walk_integrity_btree_pages`: `!is_root && cell_count == 0`
//! -> `DatabaseCorrupt`).
//!
//! Root-cause of the earlier "inconclusive" investigation (EmeraldOsprey,
//! 2026-08-15): the previous fixture set `PRAGMA page_size = 512` *after*
//! `Connection::open`, but fsqlite fixes the page size when it initialises the
//! file at open, so that pragma was a no-op and the database stayed at the
//! default page size. A handful of small rows then fit on a single leaf (the
//! table root stayed `is_root` with no non-root leaf at all), and the fixture's
//! hard-coded 512-byte stride corrupted arbitrary interior bytes rather than a
//! real leaf's cell count — so the checker correctly read "ok" because there was
//! no empty non-root leaf to find. The checker was never the bug; the fixture
//! never built the shape it claimed to test.
//!
//! This version forces a genuine split: enough moderate rows that the table
//! root becomes an interior page with several non-root leaves. It reads the
//! real page size from the file header, corrupts one genuine non-root
//! table-leaf's cell count to zero, and asserts fsqlite self-reports instead of
//! reading it as "ok".

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Read the database page size from the file header (bytes 16..18, big-endian;
/// the value `1` encodes 65536 per the SQLite format).
fn header_page_size(bytes: &[u8]) -> usize {
    let raw = u16::from_be_bytes([bytes[16], bytes[17]]);
    if raw == 1 { 65_536 } else { usize::from(raw) }
}

/// B-tree page-type flag byte for a 1-based page. Page 1 carries the 100-byte
/// database header before its b-tree header; every other page starts with it.
fn page_type(bytes: &[u8], page_size: usize, page_1based: usize) -> u8 {
    let base = (page_1based - 1) * page_size;
    let hdr = if page_1based == 1 { base + 100 } else { base };
    bytes[hdr]
}

/// Zero the b-tree cell count (u16 at header offset 3..5) of a 1-based page.
fn set_cell_count_zero(bytes: &mut [u8], page_size: usize, page_1based: usize) {
    let base = (page_1based - 1) * page_size;
    let hdr = if page_1based == 1 { base + 100 } else { base };
    bytes[hdr + 3] = 0;
    bytes[hdr + 4] = 0;
}

#[test]
fn integrity_check_flags_empty_non_root_leaf() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir
            .path()
            .join("empty_leaf.db")
            .to_string_lossy()
            .into_owned();

        {
            let conn = Connection::open(&db).await.expect("open");
            // Keep the committed image in the main database file (no WAL
            // sidecar), so a byte edit to the file lands on the read path.
            conn.execute("PRAGMA journal_mode=DELETE;")
                .await
                .expect("journal_mode");
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);")
                .await
                .expect("create");
            // Enough moderate rows to split the table root into an interior page
            // with several non-root leaf children at the default page size. Each
            // ~40-byte value keeps every row inline (no overflow pages to
            // confuse the leaf scan) while still overflowing a single leaf.
            for i in 0..400 {
                conn.execute(&format!(
                    "INSERT INTO t VALUES ({i}, 'row-{i:05}-padding-abcdefghijklmnopqrstuvwxyz');"
                ))
                .await
                .expect("insert");
            }
            let ic = conn.query("PRAGMA integrity_check;").await.expect("ic");
            assert!(
                matches!(ic[0].values()[0], SqliteValue::Text(ref s) if s.as_ref() == "ok"),
                "premise: the freshly-built table must be integrity_check-ok, got {:?}",
                ic[0].values()[0]
            );
            conn.close().await.expect("close");
        }

        let mut bytes = std::fs::read(&db).expect("read db");
        let page_size = header_page_size(&bytes);
        let page_count = bytes.len() / page_size;
        assert!(
            page_count >= 3,
            "db must have split into >= 3 pages (got {page_count} pages of {page_size} bytes)"
        );

        // The table root is page 2. After the split it is an interior page
        // (0x05), so any table-leaf (0x0D) page beyond it is a genuine non-root
        // leaf. Overflow pages begin with a next-page number whose high byte is
        // 0x00 for a database this small, so they never masquerade as 0x0D.
        assert_eq!(
            page_type(&bytes, page_size, 2),
            0x05,
            "premise: 400 rows must split the table root into an interior page"
        );
        let mut target = None;
        for p in 3..=page_count {
            if page_type(&bytes, page_size, p) == 0x0D {
                target = Some(p);
                break;
            }
        }
        let target = target.expect("expected a non-root table-leaf page after the root split");
        set_cell_count_zero(&mut bytes, page_size, target);
        std::fs::write(&db, &bytes).expect("write corrupted db");

        // Reopen and check: fsqlite must now self-report the malformed page
        // rather than reading the empty non-root leaf as "ok".
        let conn = Connection::open(&db).await.expect("reopen");
        let report = match conn.query("PRAGMA integrity_check;").await {
            Ok(rows) => match &rows[0].values()[0] {
                SqliteValue::Text(s) => s.as_ref().to_owned(),
                other => panic!("integrity_check did not return text: {other:?}"),
            },
            // A hard DatabaseCorrupt error surfaced through the query is also an
            // acceptable self-report (the checker refused the malformed image).
            Err(e) => format!("{e:?}"),
        };
        assert_ne!(
            report, "ok",
            "bd-y5urj: integrity_check must not read an empty non-root leaf as ok"
        );
        assert!(
            report.contains("empty non-root")
                || report.contains("never used")
                || report.to_lowercase().contains("malformed")
                || report.to_lowercase().contains("corrupt"),
            "expected an empty-non-root / malformed self-report, got: {report}"
        );
    });
}
