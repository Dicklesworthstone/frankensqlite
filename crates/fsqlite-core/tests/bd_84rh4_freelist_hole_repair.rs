//! bd-84rh4 (RELEASE-P0): large-scale churn leaves an in-range page reachable
//! from neither a b-tree nor the durable freelist — `integrity_check` reports
//! "Page N: never used" with freelist_count=0. The definitive diagnosis is a
//! freed/abandoned page whose free never reached the durable page-1 freelist
//! (a per-connection abandonment pool cleared at checkpoint; see bd-84rh4).
//!
//! The stochastic churn reproduces it only ~1/20. This DETERMINISTIC reproducer
//! crafts the exact corrupt on-disk state — real pages freed into the durable
//! freelist, then erased from the page-1 freelist header — so the leak class is
//! reproducible in one shot. It drives the reachability repair (Option B):
//! walk b-trees from sqlite_master + the freelist, re-free any in-range page in
//! neither set via a normal maintenance commit.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Build a database file whose page-1 freelist header has been zeroed after a
/// DROP freed real pages, leaving those pages in-range (`<= page_count`) but
/// recorded free nowhere and reachable from no b-tree — the bd-84rh4 "Page N:
/// never used" corrupt state. Returns `(db_path, freed_before_erase)`.
async fn craft_freelist_hole_db(dir: &std::path::Path) -> (String, u32) {
    let db = dir.join("bd84rh4_hole.db").to_string_lossy().into_owned();
    {
        let conn = Connection::open(&db).await.expect("open");
        // Rollback-journal mode: the freelist is written directly into the main
        // file at commit, so the crafted image is self-contained (no WAL). The
        // corrupt state produced is journal-mode-agnostic — the repair must heal
        // any such hole however it arose.
        conn.execute("PRAGMA journal_mode=DELETE;")
            .await
            .expect("journal_mode");
        conn.execute("CREATE TABLE keep(x INTEGER PRIMARY KEY, v TEXT);")
            .await
            .expect("create keep");
        conn.execute("CREATE TABLE dropme(x INTEGER PRIMARY KEY, v TEXT);")
            .await
            .expect("create dropme");
        // Populate both so dropme owns real pages beyond its root.
        for i in 0..64 {
            conn.execute(&format!("INSERT INTO keep VALUES ({i}, 'k{i}');"))
                .await
                .expect("insert keep");
            conn.execute(&format!("INSERT INTO dropme VALUES ({i}, 'd{i}');"))
                .await
                .expect("insert dropme");
        }
        // Free dropme's pages into the durable freelist.
        conn.execute("DROP TABLE dropme;")
            .await
            .expect("drop dropme");
        conn.close().await.expect("close");
    }

    let mut bytes = std::fs::read(&db).expect("read db image");
    assert!(bytes.len() >= 100, "database header must be present");
    let page_count = u32::from_be_bytes([bytes[28], bytes[29], bytes[30], bytes[31]]);
    let freelist_head = u32::from_be_bytes([bytes[32], bytes[33], bytes[34], bytes[35]]);
    let freelist_count = u32::from_be_bytes([bytes[36], bytes[37], bytes[38], bytes[39]]);
    assert!(
        freelist_count > 0 && freelist_head > 0 && freelist_head <= page_count,
        "DROP must durably free pages into the page-1 freelist \
         (head={freelist_head} count={freelist_count} page_count={page_count})"
    );
    // Erase the durable freelist header: the freed pages are now in-range but
    // recorded free nowhere -> the exact bd-84rh4 orphan state.
    for b in &mut bytes[32..40] {
        *b = 0;
    }
    std::fs::write(&db, &bytes).expect("write corrupted image");
    (db, freelist_count)
}

/// The crafted hole is a deterministic, one-shot reproduction of the bd-84rh4
/// leak class (replacing the ~1/20 stochastic churn hunt): `integrity_check`
/// must report an in-range page reachable from neither a b-tree nor the
/// freelist.
#[test]
fn bd_84rh4_freelist_hole_is_deterministically_reproducible() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let (db, freed) = craft_freelist_hole_db(dir.path()).await;
        assert!(
            freed > 0,
            "reproducer requires durably-freed pages to erase"
        );

        let conn = Connection::open(&db).await.expect("reopen crafted image");
        let integrity = conn
            .query("PRAGMA integrity_check;")
            .await
            .expect("integrity_check");
        let report: Vec<String> = integrity
            .iter()
            .filter_map(|row| match &row.values()[0] {
                SqliteValue::Text(s) => Some(s.as_ref().to_owned()),
                _ => None,
            })
            .collect();
        assert!(
            report
                .iter()
                .any(|line| line.to_ascii_lowercase().contains("never used")),
            "bd-84rh4: crafted freelist hole must surface as 'Page N: never used', got {report:?}"
        );
        conn.close().await.expect("close");
    });
}

/// After the reachability repair (Option B) lands, opening (or a maintenance
/// pass on) the crafted image must re-free the in-range unreachable pages so
/// `integrity_check` is `ok`. Un-ignore together with the repair.
#[test]
fn bd_84rh4_reachability_repair_heals_freelist_hole() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let (db, freed) = craft_freelist_hole_db(dir.path()).await;

        let conn = Connection::open(&db).await.expect("reopen crafted image");

        // Sanity: the hole is present before the repair.
        let before = conn
            .query("PRAGMA integrity_check;")
            .await
            .expect("integrity_check");
        assert!(
            before.iter().any(|row| matches!(&row.values()[0],
                SqliteValue::Text(s) if s.to_ascii_lowercase().contains("never used"))),
            "precondition: the crafted hole must be present, got {before:?}"
        );

        // Option-B reachability repair re-frees the in-range unreachable pages.
        let repaired = conn
            .repair_orphaned_pages()
            .await
            .expect("repair_orphaned_pages");
        assert!(
            repaired >= freed as usize,
            "repair must re-free at least the {freed} erased pages, re-freed {repaired}"
        );

        let after = conn
            .query("PRAGMA integrity_check;")
            .await
            .expect("integrity_check");
        assert!(
            matches!(after[0].values()[0], SqliteValue::Text(ref s) if s.as_ref() == "ok"),
            "bd-84rh4: after repair integrity_check must be ok, got {after:?}"
        );
        conn.close().await.expect("close");
    });
}
