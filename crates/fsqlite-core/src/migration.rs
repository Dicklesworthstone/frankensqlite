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
//! 1. The original files are copied to `<db>.pre-migration-bak*` **before** any
//!    mutation, each via a temp file + atomic rename.
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

/// Path of the pre-migration backup of the main database file for `db_path`.
#[must_use]
pub fn pre_migration_backup_path(db_path: &str) -> PathBuf {
    sidecar_path(db_path, PRE_MIGRATION_BACKUP_SUFFIX)
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

/// Serialize `marker` to its sidecar via a temp file + atomic rename, so a
/// reader never observes a half-written marker.
fn write_marker_atomic(db_path: &str, marker: &MigrationMarker) -> Result<()> {
    let final_path = migration_marker_path(db_path);
    let tmp_path = sidecar_path(db_path, &format!("{MIGRATION_MARKER_SUFFIX}.tmp"));
    let json = serde_json::to_vec_pretty(marker)
        .map_err(|e| FrankenError::internal(format!("serialize migration marker: {e}")))?;
    host_fs::write(&tmp_path, &json)?;
    host_fs::rename(&tmp_path, &final_path)
}

/// Copy `from` to `<to>.tmp` then atomically rename to `to`. A missing source
/// is not an error (the companion simply does not exist). Other inspection
/// failures must not be mistaken for an absent WAL.
fn backup_file_atomic(from: &Path, to: &Path) -> Result<bool> {
    match host_fs::metadata(from) {
        Ok(_) => {}
        Err(FrankenError::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(false);
        }
        Err(err) => return Err(err),
    }
    let mut tmp = to.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp_path = PathBuf::from(tmp);
    host_fs::copy_file(from, &tmp_path)?;
    host_fs::rename(&tmp_path, to)?;
    Ok(true)
}

/// Back up the main database file and any `-wal`/`-shm` companions, so the
/// original is preserved before any repair mutation. The main file and any
/// existing WAL are mandatory: committed rows may exist only in the WAL. Only
/// the reconstructible SHM index is best-effort. Both migration repair paths
/// already stop before mutation and marker publication when this returns an
/// error. Returns the main backup path on success.
fn backup_original(db_path: &str) -> Result<PathBuf> {
    let main_backup = pre_migration_backup_path(db_path);
    if !backup_file_atomic(Path::new(db_path), &main_backup)? {
        return Err(FrankenError::DatabaseNotFound {
            path: PathBuf::from(db_path),
        });
    }

    let wal = sidecar_path(db_path, "-wal");
    let wal_backup = sidecar_path(db_path, &format!("{PRE_MIGRATION_BACKUP_SUFFIX}-wal"));
    backup_file_atomic(&wal, &wal_backup)?;

    let shm = sidecar_path(db_path, "-shm");
    let shm_backup = sidecar_path(db_path, &format!("{PRE_MIGRATION_BACKUP_SUFFIX}-shm"));
    if let Err(err) = backup_file_atomic(&shm, &shm_backup) {
        tracing::warn!(target: "fsqlite.migration", %err, db = %db_path, "could not back up reconstructible SHM index; main database and WAL were preserved");
    }
    Ok(main_backup)
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
                if let Err(err) = backup_original(&db_path) {
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
            let backup_path = match backup_original(&db_path) {
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
            let backup = pre_migration_backup_path(&db_path);
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
        let wal_backup = sidecar_path(&db_path, &format!("{PRE_MIGRATION_BACKUP_SUFFIX}-wal"));
        assert!(!wal_backup.exists());
        host_fs::write(&wal, b"committed rows not checkpointed").unwrap();
        backup_original(&db_path).expect("main plus WAL backup");
        assert_eq!(
            host_fs::read(&wal_backup).unwrap(),
            b"committed rows not checkpointed"
        );
        assert_eq!(
            host_fs::read(&wal).unwrap(),
            host_fs::read(&wal_backup).unwrap()
        );
    }

    #[test]
    fn backup_refuses_wal_publication_failure() {
        let (_directory, db_path) = backup_fixture();
        let wal = sidecar_path(&db_path, "-wal");
        let wal_backup = sidecar_path(&db_path, &format!("{PRE_MIGRATION_BACKUP_SUFFIX}-wal"));
        host_fs::write(&wal, b"only committed copy").unwrap();
        host_fs::create_dir_all(&wal_backup).unwrap();
        host_fs::write(&wal_backup.join("sentinel"), b"do not replace").unwrap();
        assert!(backup_original(&db_path).is_err());
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
        backup_original(&db_path).expect("reconstructible SHM cannot veto preserved main and WAL");
        let wal_backup = sidecar_path(&db_path, &format!("{PRE_MIGRATION_BACKUP_SUFFIX}-wal"));
        assert_eq!(
            host_fs::read(&wal_backup).unwrap(),
            b"required WAL"
        );
        assert!(!sidecar_path(&db_path, &format!("{PRE_MIGRATION_BACKUP_SUFFIX}-shm")).exists());
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
        let restored = rusqlite::Connection::open(&backup).unwrap();
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
