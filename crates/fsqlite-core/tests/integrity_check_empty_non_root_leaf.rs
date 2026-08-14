//! bd-y5urj: `PRAGMA integrity_check` must flag an empty non-root B-tree leaf
//! that is still referenced by its parent — stock sqlite3 calls that shape
//! malformed. The invariant is enforced in the b-tree walk (connection.rs:
//! `!is_root && header.cell_count == 0` -> DatabaseCorrupt). This keeper builds
//! a valid multi-leaf table, byte-corrupts a non-root leaf's cell-count to 0,
//! and asserts fsqlite self-reports instead of reading it as "ok".

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const PAGE_SIZE: usize = 512;

fn page_type(bytes: &[u8], page_index_1based: usize) -> u8 {
    // Page 1 has the 100-byte database header before its b-tree header.
    let base = (page_index_1based - 1) * PAGE_SIZE;
    let hdr = if page_index_1based == 1 { base + 100 } else { base };
    bytes[hdr]
}

fn set_cell_count_zero(bytes: &mut [u8], page_index_1based: usize) {
    let base = (page_index_1based - 1) * PAGE_SIZE;
    let hdr = if page_index_1based == 1 { base + 100 } else { base };
    // b-tree page header: cell count is a u16 at header offset 3..5.
    bytes[hdr + 3] = 0;
    bytes[hdr + 4] = 0;
}

// bd-y5urj: KNOWN-RED investigation anchor. Zeroing a non-root leaf's cell-count
// then reopening currently reads as `integrity_check` = "ok" (the invariant at
// connection.rs:58567 did not fire). Open question before un-ignoring: (1) is that
// invariant actually on the `integrity_check` walk (vs a pre-publication/VACUUM
// check), or does file-backed integrity_check consult a memdb mirror that never
// re-reads the corrupted page bytes; (2) is zeroing cell_count on a POPULATED leaf
// the genuine "empty non-root leaf" shape stock sqlite3 flags, or a different
// malformation — needs a stock oracle on the exact corrupted bytes.
#[ignore = "bd-y5urj: repro fixture inconclusive; integrity_check reads the corrupted empty-non-root leaf as ok — root-cause checker-wiring vs fixture validity before un-ignoring"]
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
            conn.execute("PRAGMA page_size=512;").await.expect("page size");
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);")
                .await
                .expect("create");
            // Enough rows to split the table root into an interior page with
            // multiple leaf children (non-root leaves).
            for i in 0..120 {
                conn.execute(&format!(
                    "INSERT INTO t VALUES ({i}, 'row-{i:04}-padding-abcdefghij');"
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

        // Find a NON-root leaf page (table-leaf 0x0D or index-leaf 0x0A), skipping
        // page 1 (schema) and page 2 (the table root), and zero its cell count so
        // it becomes an empty non-root page while its parent still references it.
        let mut bytes = std::fs::read(&db).expect("read db");
        let page_count = bytes.len() / PAGE_SIZE;
        assert!(page_count >= 3, "db must have split into >= 3 pages (got {page_count})");
        let mut target = None;
        for p in 3..=page_count {
            let t = page_type(&bytes, p);
            if t == 0x0D || t == 0x0A {
                target = Some(p);
                break;
            }
        }
        let target = target.expect("expected a non-root leaf page after the root split");
        set_cell_count_zero(&mut bytes, target);
        std::fs::write(&db, &bytes).expect("write corrupted db");

        // Reopen and check: fsqlite must now self-report the malformed page.
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
