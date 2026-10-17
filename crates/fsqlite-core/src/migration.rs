//! bd-zywqc.5 — one-time idempotent repair pass at first open after upgrade.
//!
//! Databases created by a FrankenSQLite version predating the issue-#70 recovery
//! work may carry latent corruption that `integrity_check` exposes but the old
//! code kept writing over (for example the "Page N: never used" orphan-page
//! class healed by [`Connection::repair_orphaned_pages`]). This module is the
//! operability bridge for those upgraders: on the first open of such a database
//! it runs a bounded, idempotent, interrupt-safe repair pass and records a
//! marker so it never runs again for the same `(database, version)` pair.
//!
//! ## Marker-at-birth
//!
//! Every on-disk database *created* by the current code is stamped with the
//! marker at birth (`storage_was_empty == true`). A database that lacks the
//! marker was therefore created by code without this migration logic — exactly
//! the pre-fix population we want to repair. This also keeps the pass from
//! interfering with a database the current code created and merely reopened
//! (its marker is already present), and from re-running the repair on every
//! open.
//!
//! ## Interrupt safety
//!
//! Repairs commit individually; completion is not inferred from an attempted
//! pass. The completion marker is published only after all required steps and
//! the final integrity check succeed:
//! 1. The original main/WAL files are copied to a fresh private generation
//!    **before** mutation, with native VFS source fences held during copying.
//!    A checksummed receipt selects the complete set;
//!    retries retain earlier generations rather than overwriting the original.
//! 2. Each repair is its own atomic (WAL/journal-backed) commit, so an
//!    interruption leaves the database at a valid inter-commit state.
//! 3. The marker is written **last**, via a temp file + atomic rename, so its
//!    presence means "fully migrated to this version". An interruption before
//!    the marker write leaves the pass eligible for retry on the next open.
//! Earlier successful repairs are not rolled back when a later step fails.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use fsqlite_error::{FrankenError, Result};
use fsqlite_vfs::host_fs;
use serde::{Deserialize, Serialize};

use crate::connection::Connection;

pub mod backup;
#[cfg(any(unix, windows))]
pub use backup::capture_migration_backup;
pub use backup::{MigrationBackup, migration_backup_receipt_path, read_migration_backup};

/// Sidecar suffix for the migration-state marker.
pub const MIGRATION_MARKER_SUFFIX: &str = ".fsqlite-migration-state";

/// Sidecar suffix for the pre-migration backup of the main database file.
pub const PRE_MIGRATION_BACKUP_SUFFIX: &str = ".pre-migration-bak";

/// Environment variable that opts a process out of the automatic migration pass
/// (for users who prefer to handle migration themselves). Set it to `1`.
pub const SKIP_MIGRATION_ENV: &str = "FRANKENSQLITE_SKIP_MIGRATION";

/// Version of the migration *logic*.
///
/// A database whose marker records a smaller value (or has no marker at all) is
/// (re)migrated; a database already at this version is left untouched. Bump this
/// when a new repairable corruption class is added so upgraders re-run the pass.
pub const CURRENT_MIGRATION_VERSION: u32 = 1;

/// Persisted migration marker (`<db>.fsqlite-migration-state`, JSON).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationMarker {
    /// Migration-logic version that last ran to completion on this database.
    pub last_upgrade_version: u32,
    /// Unix seconds when the marker was last written (informational).
    pub last_run_at: u64,
    /// Names of the repairs the pass applied (empty when the database was
    /// already clean or freshly created).
    pub repairs_applied: Vec<String>,
}

/// The operation that prevented a migration from completing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationFailureStage {
    /// The initial check did not establish a corruption verdict.
    IntegrityCheck,
    /// A mandatory pre-repair backup failed.
    Backup,
    /// Repairing invalid freelist entries failed.
    FreelistRepair,
    /// Reclaiming orphaned pages failed.
    OrphanRepair,
    /// Reclaiming legacy contentless FTS5 shadows failed.
    ContentShadowRepair,
    /// The post-repair integrity check failed.
    IntegrityRecheck,
    /// The completion marker could not be published.
    MarkerPublication,
}

/// What the pass did, for callers/tests that want to observe it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationOutcome {
    /// In-memory database — nothing to migrate.
    SkippedMemory,
    /// `FRANKENSQLITE_SKIP_MIGRATION=1` — the user opted out.
    SkippedOptOut,
    /// Marker already at (or beyond) the current version.
    AlreadyMigrated,
    /// Freshly created database — stamped with the marker at birth.
    MarkedAtBirth,
    /// Pre-existing database whose `integrity_check` was already clean.
    CleanNoRepair,
    /// Pre-existing database that was repaired; carries the applied-repair names.
    Repaired { repairs: Vec<String> },
    /// The pass did not complete and did not publish a completion marker.
    /// Earlier successful repairs remain committed and are listed here. This
    /// is not a claim that the database is clean or that those repairs rolled
    /// back. The existing marker, if any, is left unchanged.
    Incomplete {
        stage: MigrationFailureStage,
        repairs: Vec<String>,
        error: String,
    },
}

fn migration_incomplete(
    db_path: &str,
    stage: MigrationFailureStage,
    repairs: Vec<String>,
    error: FrankenError,
) -> MigrationOutcome {
    tracing::warn!(
        target: "fsqlite.migration",
        db = %db_path,
        ?stage,
        %error,
        "migration incomplete; completion marker was not published"
    );
    MigrationOutcome::Incomplete {
        stage,
        repairs,
        error: error.to_string(),
    }
}

/// An inability to inspect the database is not permission to repair it.
/// In particular, BUSY, cancellation, I/O and internal errors must leave it
/// untouched instead of entering a mutation path intended for corruption.
fn permits_migration_repair(error: &FrankenError) -> bool {
    matches!(error, FrankenError::DatabaseCorrupt { .. })
}

/// Append `suffix` to a database path (mirrors `wal_path_for_db_path` /
/// `db_fec_path_for_db`: operate on the raw `OsString`, not a UTF-8 boundary).
fn sidecar_path(db_path: &str, suffix: &str) -> PathBuf {
    let mut s = OsString::from(db_path);
    s.push(suffix);
    PathBuf::from(s)
}

/// Path of the migration marker for `db_path`.
#[must_use]
pub fn migration_marker_path(db_path: &str) -> PathBuf {
    sidecar_path(db_path, MIGRATION_MARKER_SUFFIX)
}

/// Path selected by a well-formed generation receipt, or the legacy backup
/// pathname when there is no readable receipt. This convenience lookup does
/// not verify data bytes. Use [`read_migration_backup`] for explicit errors
/// and checksum verification; do not open retained evidence for writing.
#[must_use]
pub fn pre_migration_backup_path(db_path: &str) -> PathBuf {
    backup::selected_path(db_path)
        .ok()
        .flatten()
        .unwrap_or_else(|| sidecar_path(db_path, PRE_MIGRATION_BACKUP_SUFFIX))
}

/// Read and parse the migration marker for `db_path`, if present and valid.
#[must_use]
pub fn read_migration_marker(db_path: &str) -> Option<MigrationMarker> {
    let bytes = host_fs::read(&migration_marker_path(db_path)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Decide whether the value of [`SKIP_MIGRATION_ENV`] opts the process out.
/// Extracted as a pure function so the policy is unit-testable without mutating
/// the process environment (`std::env::set_var` is `unsafe`, forbidden here).
fn opt_out_from_env_value(value: Option<&str>) -> bool {
    value == Some("1")
}

/// The user-facing stderr line emitted after repairs are applied. Pure so its
/// wording/format is unit-testable.
fn migration_repair_message(elapsed_secs: f64, backup_path: &Path) -> String {
    format!(
        "fsqlite: applied migration repairs (took {elapsed_secs:.1}s). Original DB preserved at {}",
        backup_path.display()
    )
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Serialize `marker` via private, exclusively reserved staging and atomic
/// rename. Never truncate a stale `.tmp` file or an alias at that pathname.
fn write_marker_atomic(db_path: &str, marker: &MigrationMarker) -> Result<()> {
    let final_path = migration_marker_path(db_path);
    let json = serde_json::to_vec_pretty(marker)
        .map_err(|e| FrankenError::internal(format!("serialize migration marker: {e}")))?;
    host_fs::write_private_atomic(&final_path, &json)
}

/// Copy through private staging. Only an absent source is optional; alias,
/// read, destination, sync and publication failures must stop required backups.
fn backup_file_atomic(from: &Path, to: &Path) -> Result<bool> {
    host_fs::copy_private_atomic_if_exists(from, to)
}

/// Offline file-set fixtures retain the original raw-copy failure seam. This
/// helper is not available to production migration or live capture.
#[cfg(test)]
fn backup_original(db_path: &str) -> Result<PathBuf> {
    Ok(backup::capture(db_path)?.main_path())
}

/// Both automatic repair paths require the same coherent native capture as
/// the public backup API. Contention, cancellation, and unsupported platforms
/// must stop before repair, never fall back to independent raw-file copies.
async fn backup_before_repair(conn: &Connection) -> Result<PathBuf> {
    #[cfg(any(unix, windows))]
    {
        Ok(backup::capture_migration_backup(conn).await?.main_path())
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = conn;
        Err(FrankenError::Unsupported)
    }
}

/// Run the one-time first-open migration/repair pass for `conn`.
///
/// Best-effort from the opener's perspective: failures are logged and reported
/// as `Incomplete`, never as successful repair. Every required operation must
/// succeed before the completion marker is written. `storage_was_empty` is
/// `true` when this open created the file.
pub(crate) async fn run_first_open_migration(
    conn: &Connection,
    storage_was_empty: bool,
) -> MigrationOutcome {
    let db_path = conn.path().to_owned();

    // In-memory databases have no on-disk state to migrate.
    if db_path == ":memory:" {
        return MigrationOutcome::SkippedMemory;
    }
    // Explicit user opt-out.
    if opt_out_from_env_value(std::env::var(SKIP_MIGRATION_ENV).ok().as_deref()) {
        return MigrationOutcome::SkippedOptOut;
    }
    // Already migrated to (or beyond) the current version — the common path,
    // checked before any I/O-heavy integrity walk.
    if let Some(marker) = read_migration_marker(&db_path)
        && marker.last_upgrade_version >= CURRENT_MIGRATION_VERSION
    {
        return MigrationOutcome::AlreadyMigrated;
    }

    // A database created by the current code is clean by construction: stamp it
    // at birth so reopens short-circuit and the repair pass never touches it.
    if storage_was_empty {
        let marker = MigrationMarker {
            last_upgrade_version: CURRENT_MIGRATION_VERSION,
            last_run_at: now_unix_secs(),
            repairs_applied: Vec::new(),
        };
        if let Err(err) = write_marker_atomic(&db_path, &marker) {
            return migration_incomplete(
                &db_path,
                MigrationFailureStage::MarkerPublication,
                Vec::new(),
                err,
            );
        }
        return MigrationOutcome::MarkedAtBirth;
    }

    let started = Instant::now();

    // A pre-existing, unmarked database: check it, and repair the repairable
    // corruption classes if any are present.
    match conn.validate_database_integrity(false).await {
        Ok(()) => {
            // bd-7o1vu (GH#370): a legacy CONTENTLESS FTS5 table can carry an
            // orphaned `%_content` corpus shadow. It is integrity-CLEAN, so it
            // reaches THIS Ok branch, not the repair branch below. Reclaim it —
            // but, because the clean path does not otherwise mutate the
            // database, take the pre-migration backup HERE first (the Err branch
            // already has one). If the backup fails, skip the reclaim and leave
            // the database untouched — and do not stamp the marker, so a later
            // open retries — exactly as the Err branch does on backup failure.
            let mut repairs_applied = Vec::new();
            if !conn.orphaned_fts5_content_shadow_names().is_empty() {
                if let Err(err) = backup_before_repair(conn).await {
                    return migration_incomplete(
                        &db_path,
                        MigrationFailureStage::Backup,
                        repairs_applied,
                        err,
                    );
                }
                match conn.reclaim_orphaned_fts5_content_shadows().await {
                    Ok(dropped) if !dropped.is_empty() => {
                        repairs_applied
                            .push(format!("reclaim_orphaned_fts5_content:{}", dropped.len()));
                    }
                    Ok(_) => {}
                    Err(err) => {
                        return migration_incomplete(
                            &db_path,
                            MigrationFailureStage::ContentShadowRepair,
                            repairs_applied,
                            err,
                        );
                    }
                }
                // The initial check predates the shadow-table mutation.
                if let Err(err) = conn.validate_database_integrity(false).await {
                    return migration_incomplete(
                        &db_path,
                        MigrationFailureStage::IntegrityRecheck,
                        repairs_applied,
                        err,
                    );
                }
            }
            // Record the marker so the walk runs at most once.
            let marker = MigrationMarker {
                last_upgrade_version: CURRENT_MIGRATION_VERSION,
                last_run_at: now_unix_secs(),
                repairs_applied: repairs_applied.clone(),
            };
            if let Err(err) = write_marker_atomic(&db_path, &marker) {
                return migration_incomplete(
                    &db_path,
                    MigrationFailureStage::MarkerPublication,
                    repairs_applied,
                    err,
                );
            }
            if repairs_applied.is_empty() {
                MigrationOutcome::CleanNoRepair
            } else {
                MigrationOutcome::Repaired {
                    repairs: repairs_applied,
                }
            }
        }
        Err(integrity_err) if !permits_migration_repair(&integrity_err) => {
            migration_incomplete(
                &db_path,
                MigrationFailureStage::IntegrityCheck,
                Vec::new(),
                integrity_err,
            )
        }
        Err(_) => {
            // Preserve the original before any mutation.
            let backup_path = match backup_before_repair(conn).await {
                Ok(path) => path,
                Err(err) => {
                    return migration_incomplete(
                        &db_path,
                        MigrationFailureStage::Backup,
                        Vec::new(),
                        err,
                    );
                }
            };

            // Apply the repairable-class repairs. `repair_orphaned_pages` only
            // re-frees genuinely-orphaned in-range pages (a no-op otherwise), so
            // running it is safe even when the corruption is a different class.
            let mut repairs_applied = Vec::new();
            // GH#410: drop entries the durable freelist names but that can
            // never legally be free — the reserved lock-byte page above all,
            // which 0.3.13/0.3.14 writers left on a freelist leaf of archives
            // past 1 GiB. Runs FIRST: the orphaned-page walk below refuses to
            // run at all while the freelist names the reserved page.
            match conn.repair_freelist().await {
                Ok(dropped) if dropped > 0 => {
                    repairs_applied.push(format!("repair_freelist:{dropped}"));
                }
                Ok(_) => {}
                Err(err) => {
                    return migration_incomplete(
                        &db_path,
                        MigrationFailureStage::FreelistRepair,
                        repairs_applied,
                        err,
                    );
                }
            }
            match conn.repair_orphaned_pages().await {
                Ok(freed) if freed > 0 => {
                    repairs_applied.push(format!("repair_orphaned_pages:{freed}"));
                }
                Ok(_) => {}
                Err(err) => {
                    return migration_incomplete(
                        &db_path,
                        MigrationFailureStage::OrphanRepair,
                        repairs_applied,
                        err,
                    );
                }
            }

            // bd-7o1vu (GH#370): also reclaim any orphaned CONTENTLESS FTS5
            // `%_content` shadows here — the pre-migration backup was already
            // taken above, so no additional backup is needed.
            match conn.reclaim_orphaned_fts5_content_shadows().await {
                Ok(dropped) if !dropped.is_empty() => {
                    repairs_applied
                        .push(format!("reclaim_orphaned_fts5_content:{}", dropped.len()));
                }
                Ok(_) => {}
                Err(err) => {
                    return migration_incomplete(
                        &db_path,
                        MigrationFailureStage::ContentShadowRepair,
                        repairs_applied,
                        err,
                    );
                }
            }

            // A failed recheck is not successful migration. Do not suppress
            // future attempts by marking an unresolved database complete.
            if let Err(err) = conn.validate_database_integrity(false).await {
                return migration_incomplete(
                    &db_path,
                    MigrationFailureStage::IntegrityRecheck,
                    repairs_applied,
                    err,
                );
            }

            // Record the marker last (atomic), so its presence means done.
            let marker = MigrationMarker {
                last_upgrade_version: CURRENT_MIGRATION_VERSION,
                last_run_at: now_unix_secs(),
                repairs_applied: repairs_applied.clone(),
            };
            if let Err(err) = write_marker_atomic(&db_path, &marker) {
                return migration_incomplete(
                    &db_path,
                    MigrationFailureStage::MarkerPublication,
                    repairs_applied,
                    err,
                );
            }

            let elapsed = started.elapsed().as_secs_f64();
            eprintln!("{}", migration_repair_message(elapsed, &backup_path));

            MigrationOutcome::Repaired {
                repairs: repairs_applied,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Stock-created, fully checkpointed input with a genuine freelist hole.
    /// No FrankenSQLite connection or migration marker exists before damage.
    #[cfg(any(unix, windows))]
    fn orphaned_upgrade_fixture(wal_mode: bool) -> (PathBuf, String, Vec<u8>, u32) {
        let directory = tempfile::tempdir().unwrap().keep();
        let path = directory.join("upgrade.db");
        let db = path.to_str().unwrap().to_owned();
        let stock = rusqlite::Connection::open(&path).unwrap();
        if wal_mode {
            stock.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        }
        stock.execute_batch(
            "CREATE TABLE payload(id INTEGER PRIMARY KEY, value TEXT, bytes BLOB);
             INSERT INTO payload VALUES(1, 'retained', zeroblob(180000));
             CREATE TABLE discarded(bytes BLOB);
             INSERT INTO discarded VALUES(zeroblob(250000));
             DROP TABLE discarded;",
        ).unwrap();
        drop(stock);
        let mut before = host_fs::read(&path).unwrap();
        let freed = u32::from_be_bytes(before[36..40].try_into().unwrap());
        assert!(freed > 0, "stock DROP must free real pages");
        before[32..40].fill(0);
        host_fs::write(&path, &before).unwrap();
        let oracle = rusqlite::Connection::open(&path).unwrap();
        let verdict: String = oracle.query_row("PRAGMA integrity_check(1)", [], |row| row.get(0))
            .unwrap();
        assert!(verdict.contains("never used"), "{verdict}");
        drop(oracle);
        assert_eq!(host_fs::read(&path).unwrap(), before);
        assert!(!migration_marker_path(&db).exists());
        (directory, db, before, freed)
    }

    #[cfg(any(unix, windows))]
    fn assert_upgrade_rows(path: &Path, expect_clean: bool) {
        let oracle = rusqlite::Connection::open(path).unwrap();
        let row: (i64, String, i64) = oracle.query_row(
            "SELECT id, value, length(bytes) FROM payload", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            },
        ).unwrap();
        assert_eq!(row, (1, "retained".to_owned(), 180000));
        let verdict: String = oracle.query_row("PRAGMA integrity_check(1)", [], |row| row.get(0))
            .unwrap();
        assert_eq!(verdict == "ok", expect_clean, "{verdict}");
    }

    #[test]
    #[cfg(any(unix, windows))]
    fn ordinary_open_repairs_only_after_preserving_the_original_native_image() {
        asupersync::test_utils::run_test(|| async {
            for wal_mode in [false, true] {
                let (directory, db, before, freed) = orphaned_upgrade_fixture(wal_mode);
                // Exercise the actual default opener, not only the backup API.
                let conn = Connection::open(&db).await.unwrap();
                assert!(conn.is_concurrent_mode_default());
                conn.validate_database_integrity(false).await.unwrap();
                conn.close().await.unwrap();
                let marker = read_migration_marker(&db).expect("completed automatic upgrade");
                assert!(marker.repairs_applied.contains(&format!("repair_orphaned_pages:{freed}")),
                    "{marker:?}");
                let backup = read_migration_backup(&db).unwrap().unwrap();
                assert_eq!(host_fs::read(&backup.main_path()).unwrap(), before);
                let restored = backup.restore_to_new_directory(&directory).unwrap();
                assert_upgrade_rows(&restored, false);
                assert_upgrade_rows(Path::new(&db), true);
                backup.verify().unwrap();
                let selector = host_fs::read(&migration_backup_receipt_path(&db)).unwrap();
                let reopened = Connection::open(&db).await.unwrap();
                reopened.close().await.unwrap();
                assert_eq!(host_fs::read(&migration_backup_receipt_path(&db)).unwrap(), selector);
                backup.verify().unwrap();
            }
        });
    }

    #[test]
    #[cfg(any(unix, windows))]
    fn automatic_upgrade_selection_failure_leaves_damage_retryable_without_repair() {
        asupersync::test_utils::run_test(|| async {
            let (directory, db, before, _) = orphaned_upgrade_fixture(false);
            let selector = migration_backup_receipt_path(&db);
            host_fs::create_dir_all(&selector).unwrap();
            host_fs::write(&selector.join("sentinel"), b"preserve blocked selector").unwrap();
            let conn = Connection::open(&db).await.unwrap();
            assert!(conn.validate_database_integrity(false).await.is_err());
            assert!(!migration_marker_path(&db).exists());
            conn.close().await.unwrap();
            assert_eq!(host_fs::read(Path::new(&db)).unwrap(), before);
            let retained = directory.join("retained-selector-blocker");
            host_fs::rename(&selector, &retained).unwrap();
            let retry = Connection::open(&db).await.unwrap();
            retry.validate_database_integrity(false).await.unwrap();
            retry.close().await.unwrap();
            let backup = read_migration_backup(&db).unwrap().unwrap();
            assert_eq!(host_fs::read(&backup.main_path()).unwrap(), before);
            assert_upgrade_rows(Path::new(&db), true);
            assert_eq!(host_fs::read(&retained.join("sentinel")).unwrap(), b"preserve blocked selector");
            backup.verify().unwrap();
        });
    }

    #[test]
    #[cfg(unix)]
    fn migration_refuses_busy_native_capture_without_selecting_a_raw_fallback() {
        use fsqlite_types::flags::VfsOpenFlags;
        use fsqlite_vfs::{UnixVfs, Vfs as _, VfsFile as _};

        asupersync::test_utils::run_test(|| async {
            let (_directory, db, before, _) = orphaned_upgrade_fixture(true);
            let conn = Connection::open_schema_only(&db).await.unwrap();
            assert!(conn.is_concurrent_mode_default());
            let cx = conn.root_cx().create_child();
            let vfs = UnixVfs::new();
            let (mut peer, _) = vfs.open(
                &cx, Some(Path::new(&db)), VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB,
            ).unwrap();
            // Another VFS owner holds a real physical checkpoint attempt.
            // Read-only integrity inspection can still diagnose the hole,
            // but backup cannot acquire its own WRITE/CKPT ownership.
            peer.lock_external_wal_checkpoint(&cx).unwrap();
            let outcome = run_first_open_migration(&conn, false).await;
            let cleanup = cx.create_child();
            {
                let _mask = cleanup.masked();
                peer.restore_external_maintenance_attempt(&cleanup).unwrap();
                peer.close(&cleanup).unwrap();
            }
            assert!(matches!(outcome, MigrationOutcome::Incomplete {
                stage: MigrationFailureStage::Backup, ref repairs, ..
            } if repairs.is_empty()), "{outcome:?}");
            assert!(!migration_backup_receipt_path(&db).exists());
            assert!(!migration_marker_path(&db).exists());
            assert!(conn.validate_database_integrity(false).await.is_err());
            let retried = run_first_open_migration(&conn, false).await;
            assert!(matches!(retried, MigrationOutcome::Repaired { .. }), "{retried:?}");
            conn.validate_database_integrity(false).await.unwrap();
            conn.close().await.unwrap();
            let backup = read_migration_backup(&db).unwrap().unwrap();
            assert_eq!(host_fs::read(&backup.main_path()).unwrap(), before);
            assert_upgrade_rows(Path::new(&db), true);
            backup.verify().unwrap();
        });
    }

    #[test]
    fn only_explicit_corruption_authorizes_migration_repair() {
        let errors = [
            FrankenError::BusyRecovery,
            FrankenError::Io(std::io::Error::other("inspection failed")),
            FrankenError::internal("validation implementation failed"),
            FrankenError::DatabaseNotFound {
                path: PathBuf::from("missing.db"),
            },
            FrankenError::DatabaseFull,
        ];
        for error in errors {
            assert!(!permits_migration_repair(&error), "{error}");
        }
        assert!(permits_migration_repair(&FrankenError::DatabaseCorrupt {
            detail: "Page 3: never used".to_owned(),
        }));
    }

    /// A structurally valid index with keys for the wrong declared column is
    /// corruption that freelist/orphan/content-shadow repairs cannot fix.
    /// Stock SQLite creates the fixture; no expected query rows are injected
    /// into the engine or its migration driver.
    fn migration_sqlite_fixture(inconsistent_index: bool) -> (PathBuf, String) {
        let directory = tempfile::tempdir().unwrap().keep();
        let path = directory.join("migration.db");
        let db_path = path.to_str().unwrap().to_owned();
        let source = rusqlite::Connection::open(&path).unwrap();
        source
            .execute_batch(
                "CREATE TABLE payload(id INTEGER PRIMARY KEY, v TEXT, w TEXT);
                 INSERT INTO payload VALUES(1, 'original-v', 'original-w');
                 CREATE INDEX payload_idx ON payload(v);",
            )
            .unwrap();
        if inconsistent_index {
            source
                .execute_batch(
                    "PRAGMA writable_schema=ON;
                     UPDATE sqlite_master
                     SET sql='CREATE INDEX payload_idx ON payload(w)'
                     WHERE type='index' AND name='payload_idx';
                     PRAGMA writable_schema=OFF;",
                )
                .unwrap();
        }
        drop(source);
        // Reopen to invalidate the stock connection's cached original schema.
        let oracle = rusqlite::Connection::open(&path).unwrap();
        let verdict: String = oracle
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(verdict == "ok", !inconsistent_index, "{verdict}");
        drop(oracle);
        (directory, db_path)
    }

    #[test]
    fn clean_marker_publication_failure_is_incomplete_and_can_retry() {
        asupersync::test_utils::run_test(|| async {
            let (directory, db_path) = migration_sqlite_fixture(false);
            let marker_path = migration_marker_path(&db_path);
            host_fs::create_dir_all(&marker_path).unwrap();
            host_fs::write(&marker_path.join("sentinel"), b"retained blocker").unwrap();
            let conn = Connection::open_schema_only(&db_path).await.unwrap();
            assert!(matches!(
                run_first_open_migration(&conn, false).await,
                MigrationOutcome::Incomplete {
                    stage: MigrationFailureStage::MarkerPublication,
                    repairs,
                    ..
                } if repairs.is_empty()
            ));
            assert!(read_migration_marker(&db_path).is_none());
            assert!(!pre_migration_backup_path(&db_path).exists());

            // Retain, rather than delete, the blocker and retry the same pass.
            let retained = directory.join("retained-marker-blocker");
            host_fs::rename(&marker_path, &retained).unwrap();
            assert_eq!(
                run_first_open_migration(&conn, false).await,
                MigrationOutcome::CleanNoRepair
            );
            let marker_bytes = host_fs::read(&marker_path).unwrap();
            assert_eq!(
                run_first_open_migration(&conn, false).await,
                MigrationOutcome::AlreadyMigrated
            );
            assert_eq!(host_fs::read(&marker_path).unwrap(), marker_bytes);
            assert_eq!(
                host_fs::read(&retained.join("sentinel")).unwrap(),
                b"retained blocker"
            );
            conn.close().await.unwrap();
        });
    }

    #[test]
    fn failed_birth_marker_is_not_reported_as_marked_at_birth() {
        asupersync::test_utils::run_test(|| async {
            let directory = tempfile::tempdir().unwrap().keep();
            let db_path = directory.join("new.db").to_str().unwrap().to_owned();
            let marker_path = migration_marker_path(&db_path);
            host_fs::create_dir_all(&marker_path).unwrap();
            let conn = Connection::open_schema_only(&db_path).await.unwrap();
            assert!(matches!(
                run_first_open_migration(&conn, true).await,
                MigrationOutcome::Incomplete {
                    stage: MigrationFailureStage::MarkerPublication,
                    ..
                }
            ));
            assert!(read_migration_marker(&db_path).is_none());
            host_fs::rename(&marker_path, &directory.join("retained-blocker")).unwrap();
            assert_eq!(
                run_first_open_migration(&conn, true).await,
                MigrationOutcome::MarkedAtBirth
            );
            assert!(read_migration_marker(&db_path).is_some());
            conn.close().await.unwrap();
        });
    }

    #[test]
    fn unresolved_index_corruption_does_not_publish_completion() {
        asupersync::test_utils::run_test(|| async {
            let (_directory, db_path) = migration_sqlite_fixture(true);
            let conn = Connection::open_schema_only(&db_path).await.unwrap();
            let before = conn.validate_database_integrity(false).await.unwrap_err();
            assert!(permits_migration_repair(&before), "{before}");
            assert!(matches!(
                run_first_open_migration(&conn, false).await,
                MigrationOutcome::Incomplete { .. }
            ));
            assert!(read_migration_marker(&db_path).is_none());
            assert!(pre_migration_backup_path(&db_path).exists());
            assert!(conn.validate_database_integrity(false).await.is_err());
            conn.close().await.unwrap();

            let oracle = rusqlite::Connection::open(&db_path).unwrap();
            let values: (String, String) = oracle
                .query_row("SELECT v, w FROM payload WHERE id=1", [], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .unwrap();
            assert_eq!(values, ("original-v".to_owned(), "original-w".to_owned()));
            let verdict: String = oracle
                .query_row("PRAGMA integrity_check", [], |row| row.get(0))
                .unwrap();
            assert_ne!(verdict, "ok");
        });
    }

    #[test]
    fn required_backup_failure_is_incomplete_not_clean() {
        asupersync::test_utils::run_test(|| async {
            let (_directory, db_path) = migration_sqlite_fixture(true);
            let backup = migration_backup_receipt_path(&db_path);
            host_fs::create_dir_all(&backup).unwrap();
            host_fs::write(&backup.join("sentinel"), b"retain destination").unwrap();
            let before = host_fs::read(Path::new(&db_path)).unwrap();
            let conn = Connection::open_schema_only(&db_path).await.unwrap();
            assert!(matches!(
                run_first_open_migration(&conn, false).await,
                MigrationOutcome::Incomplete {
                    stage: MigrationFailureStage::Backup,
                    repairs,
                    ..
                } if repairs.is_empty()
            ));
            assert!(read_migration_marker(&db_path).is_none());
            conn.close().await.unwrap();
            assert_eq!(host_fs::read(Path::new(&db_path)).unwrap(), before);
            assert_eq!(
                host_fs::read(&backup.join("sentinel")).unwrap(),
                b"retain destination"
            );
        });
    }

    fn backup_fixture() -> (PathBuf, String) {
        let directory = tempfile::tempdir().expect("create backup fixture").keep();
        let path = directory.join("original.db");
        host_fs::write(&path, b"original main image").expect("write main image");
        let db_path = path.to_str().expect("UTF-8 fixture path").to_owned();
        (directory, db_path)
    }

    #[test]
    fn migration_marker_publication_preserves_stale_temporary_contents() {
        let (_directory, db_path) = backup_fixture();
        let stale = sidecar_path(&db_path, &format!("{MIGRATION_MARKER_SUFFIX}.tmp"));
        host_fs::write(&stale, b"retained interrupted marker").unwrap();
        let marker = MigrationMarker {
            last_upgrade_version: CURRENT_MIGRATION_VERSION,
            last_run_at: 1_700_000_000,
            repairs_applied: vec!["repair_orphaned_pages:3".to_owned()],
        };
        write_marker_atomic(&db_path, &marker).unwrap();
        assert_eq!(read_migration_marker(&db_path), Some(marker));
        assert_eq!(host_fs::read(&stale).unwrap(), b"retained interrupted marker");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let permissions = host_fs::metadata(&migration_marker_path(&db_path))
                .unwrap()
                .permissions();
            assert_eq!(permissions.mode() & 0o077, 0);
        }
    }

    #[test]
    fn migration_marker_failure_preserves_the_existing_destination() {
        let (_directory, db_path) = backup_fixture();
        let final_path = migration_marker_path(&db_path);
        host_fs::create_dir_all(&final_path).unwrap();
        host_fs::write(&final_path.join("sentinel"), b"preserve destination").unwrap();
        let marker = MigrationMarker {
            last_upgrade_version: CURRENT_MIGRATION_VERSION,
            last_run_at: 0,
            repairs_applied: Vec::new(),
        };
        assert!(write_marker_atomic(&db_path, &marker).is_err());
        assert_eq!(
            host_fs::read(&final_path.join("sentinel")).unwrap(),
            b"preserve destination"
        );
        assert!(read_migration_marker(&db_path).is_none());
    }

    #[test]
    #[cfg(unix)]
    fn migration_backup_family_has_no_group_or_other_access() {
        use std::os::unix::fs::PermissionsExt as _;

        let (_directory, db_path) = backup_fixture();
        host_fs::write(&sidecar_path(&db_path, "-wal"), b"private WAL").unwrap();
        host_fs::write(&sidecar_path(&db_path, "-shm"), b"private SHM").unwrap();
        let main = backup_original(&db_path).unwrap();
        for suffix in ["", "-wal", "-shm"] {
            let path = sidecar_path(main.to_str().unwrap(), suffix);
            assert_eq!(host_fs::metadata(&path).unwrap().permissions().mode() & 0o077, 0);
        }
    }

    #[test]
    fn backup_requires_the_main_database() {
        let directory = tempfile::tempdir().expect("create fixture").keep();
        let path = directory.join("missing.db");
        let db_path = path.to_str().expect("UTF-8 fixture path");
        host_fs::write(&sidecar_path(db_path, "-wal"), b"orphaned WAL").unwrap();
        assert!(matches!(
            backup_original(db_path),
            Err(FrankenError::DatabaseNotFound { path: missing }) if missing == path
        ));
        assert!(!pre_migration_backup_path(db_path).exists());
        assert!(!migration_marker_path(db_path).exists());
    }

    #[test]
    fn backup_accepts_an_absent_wal_but_preserves_an_existing_one() {
        let (_directory, db_path) = backup_fixture();
        let main_backup = backup_original(&db_path).expect("main-only backup");
        assert_eq!(host_fs::read(&main_backup).unwrap(), b"original main image");
        let wal = sidecar_path(&db_path, "-wal");
        let wal_backup = sidecar_path(main_backup.to_str().unwrap(), "-wal");
        assert!(!wal_backup.exists());
        host_fs::create_empty_file(&wal).unwrap();
        let empty = backup_original(&db_path).expect("an existing frame-free WAL is still preserved");
        let empty_wal = sidecar_path(empty.to_str().unwrap(), "-wal");
        assert_eq!(host_fs::metadata(&empty_wal).unwrap().len(), 0);
        host_fs::write(&wal, b"committed rows not checkpointed").unwrap();
        let full = backup_original(&db_path).expect("main plus WAL backup");
        let full_wal = sidecar_path(full.to_str().unwrap(), "-wal");
        assert_eq!(
            host_fs::read(&full_wal).unwrap(),
            b"committed rows not checkpointed"
        );
        assert_eq!(
            host_fs::read(&wal).unwrap(),
            host_fs::read(&full_wal).unwrap()
        );
        assert!(!wal_backup.exists(), "retry must not add a WAL to the first generation");
        assert_eq!(host_fs::metadata(&empty_wal).unwrap().len(), 0);
        assert_ne!(main_backup, empty);
        assert_ne!(empty, full);
    }

    #[test]
    fn backup_refuses_wal_publication_failure() {
        let (_directory, db_path) = backup_fixture();
        let wal = sidecar_path(&db_path, "-wal");
        host_fs::write(&wal, b"only committed copy").unwrap();
        let mut blocked = None;
        let result = backup::capture_with(&db_path, |from, to| {
            if from == wal.as_path() {
                host_fs::create_dir_all(to)?;
                host_fs::write(&to.join("sentinel"), b"do not replace")?;
                blocked = Some(to.to_path_buf());
            }
            backup_file_atomic(from, to)
        });
        assert!(result.is_err());
        let wal_backup = blocked.expect("real WAL publication was attempted");
        assert_eq!(
            host_fs::read(Path::new(&db_path)).unwrap(),
            b"original main image"
        );
        assert_eq!(host_fs::read(&wal).unwrap(), b"only committed copy");
        assert_eq!(
            host_fs::read(&wal_backup.join("sentinel")).unwrap(),
            b"do not replace"
        );
        assert!(!migration_marker_path(&db_path).exists());
        assert!(!migration_backup_receipt_path(&db_path).exists());
    }

    #[test]
    fn backup_refuses_an_unreadable_wal_object() {
        let (_directory, db_path) = backup_fixture();
        let wal = sidecar_path(&db_path, "-wal");
        host_fs::create_dir_all(&wal).unwrap();
        host_fs::write(&wal.join("sentinel"), b"not a regular WAL").unwrap();
        assert!(backup_original(&db_path).is_err());
        assert_eq!(
            host_fs::read(&wal.join("sentinel")).unwrap(),
            b"not a regular WAL"
        );
        assert!(!migration_marker_path(&db_path).exists());
    }

    #[test]
    fn backup_keeps_only_shm_failure_best_effort() {
        let (_directory, db_path) = backup_fixture();
        let wal = sidecar_path(&db_path, "-wal");
        let shm = sidecar_path(&db_path, "-shm");
        host_fs::write(&wal, b"required WAL").unwrap();
        host_fs::create_dir_all(&shm).unwrap();
        let main = backup_original(&db_path)
            .expect("reconstructible SHM cannot veto preserved main and WAL");
        let wal_backup = sidecar_path(main.to_str().unwrap(), "-wal");
        assert_eq!(
            host_fs::read(&wal_backup).unwrap(),
            b"required WAL"
        );
        assert!(!sidecar_path(main.to_str().unwrap(), "-shm").exists());
    }

    #[test]
    #[cfg(unix)]
    fn backup_does_not_classify_metadata_errors_as_missing() {
        let (directory, db_path) = backup_fixture();
        // Traversing through a regular file produces ENOTDIR, not ENOENT.
        // This remains a real error even when the test runs as root.
        let from = Path::new(&db_path).join("not-a-child");
        assert!(backup_file_atomic(&from, &directory.join("backup")).is_err());
        assert!(!directory.join("backup").exists());
    }

    #[test]
    fn backup_restores_rows_present_only_in_the_stock_sqlite_wal() {
        let directory = tempfile::tempdir().unwrap().keep();
        let path = directory.join("source.db");
        let db_path = path.to_str().unwrap();
        let source = rusqlite::Connection::open(&path).unwrap();
        source
            .execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; \
                 CREATE TABLE payload(id INTEGER PRIMARY KEY, value TEXT); \
                 PRAGMA wal_checkpoint(TRUNCATE); \
                 INSERT INTO payload VALUES(1, 'committed only in WAL');",
            )
            .unwrap();
        assert!(host_fs::metadata(&sidecar_path(db_path, "-wal")).unwrap().len() > 32);

        // Keep the source connection alive: closing the last stock connection
        // would checkpoint the row and make a main-only backup appear sound.
        let main_only = directory.join("main-only.db");
        host_fs::copy_file(&path, &main_only).unwrap();
        let incomplete = rusqlite::Connection::open(&main_only).unwrap();
        let missing_rows: i64 = incomplete
            .query_row("SELECT count(*) FROM payload", [], |row| row.get(0))
            .unwrap();
        assert_eq!(missing_rows, 0);

        let backup = backup_original(db_path).expect("preserve main and WAL");
        let retained = read_migration_backup(db_path).unwrap().unwrap();
        assert_eq!(retained.main_path(), backup);
        let restore_path = retained.restore_to_new_directory(&directory).unwrap();
        let restored = rusqlite::Connection::open(&restore_path).unwrap();
        let value: String = restored
            .query_row("SELECT value FROM payload WHERE id=1", [], |row| row.get(0))
            .unwrap();
        assert_eq!(value, "committed only in WAL");
        let integrity: String = restored
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(integrity, "ok");
        let source_rows: i64 = source
            .query_row("SELECT count(*) FROM payload", [], |row| row.get(0))
            .unwrap();
        assert_eq!(source_rows, 1);
        drop(restored);
        retained.verify().expect("stock recovery must not change retained evidence");
    }

    #[test]
    fn sidecar_paths_append_suffix_to_raw_db_path() {
        assert_eq!(
            migration_marker_path("/tmp/foo.db"),
            PathBuf::from("/tmp/foo.db.fsqlite-migration-state")
        );
        assert_eq!(
            pre_migration_backup_path("/tmp/foo.db"),
            PathBuf::from("/tmp/foo.db.pre-migration-bak")
        );
    }

    #[test]
    fn marker_roundtrips_through_json() {
        let marker = MigrationMarker {
            last_upgrade_version: CURRENT_MIGRATION_VERSION,
            last_run_at: 1_700_000_000,
            repairs_applied: vec!["repair_orphaned_pages:3".to_owned()],
        };
        let json = serde_json::to_vec(&marker).expect("serialize");
        let back: MigrationMarker = serde_json::from_slice(&json).expect("deserialize");
        assert_eq!(marker, back);
    }

    #[test]
    fn read_missing_marker_is_none() {
        assert!(read_migration_marker("/nonexistent/path/to/db-xyzzy").is_none());
    }

    #[test]
    fn opt_out_only_for_exactly_one() {
        assert!(opt_out_from_env_value(Some("1")));
        assert!(!opt_out_from_env_value(Some("0")));
        assert!(!opt_out_from_env_value(Some("true")));
        assert!(!opt_out_from_env_value(Some("")));
        assert!(!opt_out_from_env_value(None));
    }

    #[test]
    fn repair_message_names_time_and_backup_path() {
        let msg = migration_repair_message(2.34, Path::new("/tmp/foo.db.pre-migration-bak"));
        assert!(msg.contains("applied migration repairs"), "got: {msg}");
        assert!(
            msg.contains("2.3s"),
            "one-decimal elapsed seconds; got: {msg}"
        );
        assert!(
            msg.contains("/tmp/foo.db.pre-migration-bak"),
            "names the backup path; got: {msg}"
        );
    }
}
