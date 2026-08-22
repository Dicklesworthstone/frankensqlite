//! bd-zywqc.14 adversarial corpus — orphaned-page corruption healed by the
//! first-open repair pass (bd-zywqc.5).
//!
//! The #70 "Page N: never used" class: a DROP frees real pages into the durable
//! freelist, then the page-1 freelist header is erased, leaving in-range pages
//! reachable from neither a b-tree nor the freelist. This is the exact class the
//! migration/repair pass heals via `repair_orphaned_pages`.
//!
//! Each scenario models a pre-fix database (marker stripped, so the first-open
//! pass runs), asserts the hole is present beforehand, opens it once (which runs
//! the repair pass), and requires the healed image to be certified `ok` by BOTH
//! fsqlite and stock C SQLite (rusqlite) with every surviving row intact. The
//! stock oracle before/after is the guard against a fixture that is secretly
//! already clean, or a "repair" that only satisfies our own checker.

use fsqlite_core::connection::Connection;
use fsqlite_core::migration::migration_marker_path;
use fsqlite_types::value::SqliteValue;

/// Build a pre-fix database with an orphaned-page hole: `keep` survives, `drop_n`
/// throwaway tables are populated then dropped (freeing real pages), and the
/// page-1 freelist header is zeroed. Returns `(db_path, keep_rows)`.
async fn craft_orphan_hole_db(dir: &std::path::Path, name: &str, drop_n: usize) -> (String, usize) {
    let db = dir.join(name).to_string_lossy().into_owned();
    let keep_rows = 48usize;
    {
        let conn = Connection::open(&db).await.expect("open");
        conn.execute("PRAGMA journal_mode=DELETE;")
            .await
            .expect("journal_mode");
        conn.execute("CREATE TABLE keep(x INTEGER PRIMARY KEY, v TEXT);")
            .await
            .expect("create keep");
        for i in 0..keep_rows {
            conn.execute(&format!("INSERT INTO keep VALUES ({i}, 'k{i}');"))
                .await
                .expect("insert keep");
        }
        for t in 0..drop_n {
            conn.execute(&format!(
                "CREATE TABLE dropme{t}(x INTEGER PRIMARY KEY, v TEXT);"
            ))
            .await
            .expect("create dropme");
            for i in 0..64 {
                conn.execute(&format!("INSERT INTO dropme{t} VALUES ({i}, 'd{i}');"))
                    .await
                    .expect("insert dropme");
            }
            conn.execute(&format!("DROP TABLE dropme{t};"))
                .await
                .expect("drop dropme");
        }
        conn.close().await.expect("close");
    }
    // Model a pre-fix database: strip the birth marker so the pass runs.
    let _ = std::fs::remove_file(migration_marker_path(&db));
    // Erase the durable freelist header -> orphaned in-range pages.
    let mut bytes = std::fs::read(&db).expect("read image");
    let freelist_count = u32::from_be_bytes([bytes[36], bytes[37], bytes[38], bytes[39]]);
    assert!(
        freelist_count > 0,
        "{name}: the DROP(s) must durably free pages to erase (got 0)"
    );
    for b in &mut bytes[32..40] {
        *b = 0;
    }
    std::fs::write(&db, &bytes).expect("write corrupted image");
    (db, keep_rows)
}

async fn fsqlite_integrity(conn: &Connection) -> Vec<String> {
    conn.query("PRAGMA integrity_check;")
        .await
        .expect("integrity_check")
        .iter()
        .filter_map(|r| match &r.values()[0] {
            SqliteValue::Text(s) => Some(s.as_ref().to_owned()),
            _ => None,
        })
        .collect()
}

fn stock_integrity(db: &str) -> String {
    let c = rusqlite::Connection::open(db).expect("stock open");
    c.query_row("PRAGMA integrity_check;", [], |r| r.get::<_, String>(0))
        .expect("stock integrity_check")
}

fn stock_keep_count(db: &str) -> i64 {
    let c = rusqlite::Connection::open(db).expect("stock open");
    c.query_row("SELECT count(*) FROM keep;", [], |r| r.get::<_, i64>(0))
        .expect("stock count")
}

/// The shared scenario: a pre-fix orphan hole is present, and the first-open
/// pass heals it into an image both engines certify `ok`, with all rows intact.
async fn assert_orphan_hole_healed(name: &str, drop_n: usize) {
    let dir = tempfile::tempdir().expect("tempdir");
    let (db, keep_rows) = craft_orphan_hole_db(dir.path(), &format!("{name}.db"), drop_n).await;

    // Before: stock C SQLite sees the hole (the fixture genuinely bites, and
    // stock cannot run fsqlite's repair pass, so it observes the raw image).
    let before = stock_integrity(&db);
    assert_ne!(
        before, "ok",
        "{name}: precondition — the crafted hole must be visible to stock, got {before:?}"
    );

    // Open once: the first-open repair pass runs and heals the hole.
    {
        let conn = Connection::open(&db)
            .await
            .expect("open triggers repair pass");
        let after = fsqlite_integrity(&conn).await;
        conn.close().await.ok();
        assert_eq!(
            after,
            vec!["ok".to_owned()],
            "{name}: after the repair pass fsqlite integrity_check must be ok, got {after:?}"
        );
    }

    // Stock oracle: the healed image is valid and every kept row survived.
    assert_eq!(
        stock_integrity(&db),
        "ok",
        "{name}: stock C SQLite must read the healed image as ok"
    );
    assert_eq!(
        stock_keep_count(&db),
        keep_rows as i64,
        "{name}: repair must preserve all committed rows"
    );
}

#[test]
fn orphan_hole_single_drop_is_healed_and_stock_clean() {
    asupersync::test_utils::run_test(|| async {
        assert_orphan_hole_healed("orphan_single", 1).await;
    });
}

#[test]
fn orphan_hole_two_drops_is_healed_and_stock_clean() {
    asupersync::test_utils::run_test(|| async {
        assert_orphan_hole_healed("orphan_double", 2).await;
    });
}

#[test]
fn orphan_hole_three_drops_is_healed_and_stock_clean() {
    asupersync::test_utils::run_test(|| async {
        assert_orphan_hole_healed("orphan_triple", 3).await;
    });
}
