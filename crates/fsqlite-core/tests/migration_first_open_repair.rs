//! bd-zywqc.5: first-open-after-upgrade one-time idempotent repair pass.
//!
//! A database created by a version predating this migration logic has no
//! marker; opening it under the current code runs a bounded, idempotent,
//! interrupt-safe repair pass and stamps a marker so it never re-runs for the
//! same `(database, version)`. These tests exercise the acceptance criteria:
//! runs-once, idempotent, interrupt-safe, backup-preserved, opt-out, message.
//!
//! The corruption fixture reuses the deterministic bd-84rh4 freelist-hole (an
//! in-range page reachable from neither a b-tree nor the durable freelist —
//! `integrity_check`'s "Page N: never used" class, healed by
//! `repair_orphaned_pages`). Since the current code stamps a marker at birth,
//! the fixture deletes that marker to model a genuine pre-fix upgrader.

use std::path::Path;

use fsqlite_core::connection::Connection;
use fsqlite_core::migration::{
    CURRENT_MIGRATION_VERSION, migration_marker_path, pre_migration_backup_path,
    read_migration_marker,
};
use fsqlite_types::value::SqliteValue;

/// Build an on-disk database with the bd-84rh4 freelist hole and **no** marker,
/// modelling a database written by a pre-migration version. Returns
/// `(db_path, freed_pages)`.
async fn craft_prefix_corrupt_db(dir: &Path, name: &str) -> (String, u32) {
    let db = dir.join(name).to_string_lossy().into_owned();
    {
        // The first open stamps a birth marker; DELETE journal mode keeps the
        // crafted image self-contained (no WAL sidecar).
        let conn = Connection::open(&db).await.expect("open");
        conn.execute("PRAGMA journal_mode=DELETE;")
            .await
            .expect("journal_mode");
        conn.execute("CREATE TABLE keep(x INTEGER PRIMARY KEY, v TEXT);")
            .await
            .expect("create keep");
        conn.execute("CREATE TABLE dropme(x INTEGER PRIMARY KEY, v TEXT);")
            .await
            .expect("create dropme");
        for i in 0..64 {
            conn.execute(&format!("INSERT INTO keep VALUES ({i}, 'k{i}');"))
                .await
                .expect("insert keep");
            conn.execute(&format!("INSERT INTO dropme VALUES ({i}, 'd{i}');"))
                .await
                .expect("insert dropme");
        }
        conn.execute("DROP TABLE dropme;").await.expect("drop");
        conn.close().await.expect("close");
    }
    // Model a pre-fix database: strip the birth marker.
    let _ = std::fs::remove_file(migration_marker_path(&db));
    // Erase the durable freelist header -> the orphan hole.
    let mut bytes = std::fs::read(&db).expect("read image");
    let freelist_count = u32::from_be_bytes([bytes[36], bytes[37], bytes[38], bytes[39]]);
    assert!(freelist_count > 0, "DROP must durably free pages to erase");
    for b in &mut bytes[32..40] {
        *b = 0;
    }
    std::fs::write(&db, &bytes).expect("write corrupted image");
    (db, freelist_count)
}

async fn integrity_lines(conn: &Connection) -> Vec<String> {
    conn.query("PRAGMA integrity_check;")
        .await
        .expect("integrity_check")
        .iter()
        .filter_map(|row| match &row.values()[0] {
            SqliteValue::Text(s) => Some(s.as_ref().to_owned()),
            _ => None,
        })
        .collect()
}

/// Stock C SQLite (rusqlite) integrity_check verdict — the oracle.
fn stock_integrity(db: &str) -> String {
    let c = rusqlite::Connection::open(db).expect("stock open");
    c.query_row("PRAGMA integrity_check;", [], |r| r.get(0))
        .expect("stock integrity_check")
}

/// AC precondition: a database created by the current code is stamped at birth
/// (so reopens short-circuit) and needs no repair or backup.
#[test]
fn clean_database_is_stamped_at_birth_without_repair() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("clean.db").to_string_lossy().into_owned();

        let conn = Connection::open(&db).await.expect("open");
        conn.execute("CREATE TABLE t(a);").await.unwrap();
        conn.execute("INSERT INTO t VALUES (1),(2);").await.unwrap();
        conn.close().await.unwrap();

        let marker = read_migration_marker(&db).expect("marker stamped at birth");
        assert_eq!(marker.last_upgrade_version, CURRENT_MIGRATION_VERSION);
        assert!(
            marker.repairs_applied.is_empty(),
            "a born-clean database records no repairs: {marker:?}"
        );
        assert!(
            !pre_migration_backup_path(&db).exists(),
            "no backup for a clean database"
        );

        let conn2 = Connection::open(&db).await.expect("reopen (AlreadyMigrated)");
        assert_eq!(integrity_lines(&conn2).await, vec!["ok".to_owned()]);
        conn2.close().await.unwrap();
    });
}

/// AC: a pre-fix corrupt database is repaired on first open; the original is
/// preserved at the backup; stock C SQLite reads the repaired image as `ok`.
#[test]
fn prefix_corrupt_database_is_repaired_on_first_open() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let (db, _freed) = craft_prefix_corrupt_db(dir.path(), "prefix.db").await;
        assert!(
            read_migration_marker(&db).is_none(),
            "precondition: a pre-fix database has no marker"
        );

        // First open under current code triggers the migration/repair pass.
        let conn = Connection::open(&db).await.expect("open triggers migration");
        assert_eq!(
            integrity_lines(&conn).await,
            vec!["ok".to_owned()],
            "migration must heal the freelist hole"
        );
        conn.close().await.unwrap();

        let marker = read_migration_marker(&db).expect("marker written after repair");
        assert_eq!(marker.last_upgrade_version, CURRENT_MIGRATION_VERSION);
        assert!(
            !marker.repairs_applied.is_empty(),
            "repairs_applied records the applied repair: {marker:?}"
        );

        let backup = pre_migration_backup_path(&db);
        assert!(backup.exists(), "original preserved at the pre-migration backup");
        assert_ne!(
            std::fs::read(&backup).unwrap(),
            std::fs::read(&db).unwrap(),
            "backup holds the pre-repair image, distinct from the repaired database"
        );
        assert_eq!(
            stock_integrity(&db),
            "ok",
            "stock oracle: the repaired database is clean"
        );
    });
}

/// AC (1): the migration runs once and only once per (db, version). A second
/// open sees the marker and does not re-run the repair.
#[test]
fn migration_runs_once_per_version() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let (db, _freed) = craft_prefix_corrupt_db(dir.path(), "once.db").await;

        Connection::open(&db).await.unwrap().close().await.unwrap();
        let marker1 = read_migration_marker(&db).expect("marker after first open");

        // Remove the backup to observe whether a second open re-runs repair.
        std::fs::remove_file(pre_migration_backup_path(&db)).unwrap();

        Connection::open(&db).await.unwrap().close().await.unwrap();
        assert!(
            !pre_migration_backup_path(&db).exists(),
            "second open must not re-run the repair (no new backup)"
        );
        let marker2 = read_migration_marker(&db).expect("marker after second open");
        assert_eq!(marker1, marker2, "marker unchanged: migration did not re-run");
    });
}

/// AC (2): forcing a re-run (marker removed) produces identical state — the
/// repair is idempotent (re-freeing already-free pages is a no-op).
#[test]
fn forced_rerun_is_idempotent() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let (db, _freed) = craft_prefix_corrupt_db(dir.path(), "idem.db").await;

        Connection::open(&db).await.unwrap().close().await.unwrap();
        let repaired = std::fs::read(&db).unwrap();

        // Force a re-run: drop the marker as if opened again by an unmarked path.
        std::fs::remove_file(migration_marker_path(&db)).unwrap();
        Connection::open(&db).await.unwrap().close().await.unwrap();

        assert_eq!(
            std::fs::read(&db).unwrap(),
            repaired,
            "a forced re-run leaves the database byte-identical"
        );
        assert_eq!(stock_integrity(&db), "ok");
        assert!(read_migration_marker(&db).is_some(), "marker re-written");
    });
}

/// AC (3): an interruption after the repair but before the marker write leaves
/// a valid (repaired) database; the next open completes the pass cleanly. This
/// simulates the interrupt by deleting the marker after a successful pass.
#[test]
fn interrupted_before_marker_completes_on_next_open() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let (db, _freed) = craft_prefix_corrupt_db(dir.path(), "interrupt.db").await;

        Connection::open(&db).await.unwrap().close().await.unwrap();
        // Simulate a kill that repaired the DB but lost the marker write.
        std::fs::remove_file(migration_marker_path(&db)).unwrap();

        let conn = Connection::open(&db).await.expect("reopen completes the pass");
        assert_eq!(
            integrity_lines(&conn).await,
            vec!["ok".to_owned()],
            "post-interrupt state is valid and clean"
        );
        conn.close().await.unwrap();
        assert!(
            read_migration_marker(&db).is_some(),
            "marker restored after the interrupted run"
        );
    });
}

/// AC (5): opt-out. Run this test with `FRANKENSQLITE_SKIP_MIGRATION=1` in the
/// environment; it is a no-op otherwise (so the normal suite is unaffected and
/// no `unsafe` `set_var` is needed). With opt-out set, a corrupt database is
/// left untouched — no repair, no marker, no backup.
#[test]
fn opt_out_leaves_corrupt_database_untouched() {
    if std::env::var("FRANKENSQLITE_SKIP_MIGRATION").as_deref() != Ok("1") {
        eprintln!(
            "opt_out_leaves_corrupt_database_untouched: skipped; \
             set FRANKENSQLITE_SKIP_MIGRATION=1 to exercise it"
        );
        return;
    }
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let (db, _freed) = craft_prefix_corrupt_db(dir.path(), "optout.db").await;

        let conn = Connection::open(&db).await.expect("open with opt-out");
        let lines = integrity_lines(&conn).await;
        assert!(
            lines
                .iter()
                .any(|l| l.to_ascii_lowercase().contains("never used")),
            "opt-out: the hole must remain (no repair), got {lines:?}"
        );
        conn.close().await.unwrap();
        assert!(
            read_migration_marker(&db).is_none(),
            "opt-out: no marker written"
        );
        assert!(
            !pre_migration_backup_path(&db).exists(),
            "opt-out: no backup written"
        );
    });
}
