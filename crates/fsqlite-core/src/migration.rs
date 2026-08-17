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
//! Ordering guarantees "either the pre-migration state or the post-migration
//! state, never a partial one":
//! 1. The original files are copied to `<db>.pre-migration-bak*` **before** any
//!    mutation, each via a temp file + atomic rename.
//! 2. Each repair is its own atomic (WAL/journal-backed) commit, so an
//!    interruption leaves the database at a valid inter-commit state.
//! 3. The marker is written **last**, via a temp file + atomic rename, so its
//!    presence means "fully migrated to this version". An interruption before
//!    the marker write simply re-runs the (idempotent) pass on the next open.

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

/// Companion suffixes copied alongside the main file into the pre-migration
/// backup, so a WAL-mode database can be restored faithfully.
const BACKUP_COMPANION_SUFFIXES: [&str; 2] = ["-wal", "-shm"];

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
/// is not an error (the companion simply does not exist).
fn backup_file_atomic(from: &Path, to: &Path) -> Result<bool> {
    if host_fs::metadata(from).is_err() {
        return Ok(false);
    }
    let mut tmp = to.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp_path = PathBuf::from(tmp);
    host_fs::copy_file(from, &tmp_path)?;
    host_fs::rename(&tmp_path, to)?;
    Ok(true)
}

/// Back up the main database file and any `-wal`/`-shm` companions, so the
/// original is preserved before any repair mutation. Returns the main backup
/// path on success.
fn backup_original(db_path: &str) -> Result<PathBuf> {
    let main_backup = pre_migration_backup_path(db_path);
    backup_file_atomic(Path::new(db_path), &main_backup)?;
    for suffix in BACKUP_COMPANION_SUFFIXES {
        let from = sidecar_path(db_path, suffix);
        let to = sidecar_path(db_path, &format!("{PRE_MIGRATION_BACKUP_SUFFIX}{suffix}"));
        // Companions are best-effort: a missing/uncopyable -shm must not abort
        // the migration (it is rebuilt on next open).
        let _ = backup_file_atomic(&from, &to);
    }
    Ok(main_backup)
}

/// Run the one-time first-open migration/repair pass for `conn`.
///
/// Infallible from the caller's perspective: any internal error is logged and
/// the database is left no worse than it was found (the backup preserves the
/// original). `storage_was_empty` is `true` when this open created the file.
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
            tracing::warn!(target: "fsqlite.migration", %err, db = %db_path, "failed to stamp migration marker at birth");
        }
        return MigrationOutcome::MarkedAtBirth;
    }

    let started = Instant::now();

    // A pre-existing, unmarked database: check it, and repair the repairable
    // corruption classes if any are present.
    match conn.validate_database_integrity(false).await {
        Ok(()) => {
            // Clean — record the marker so the walk runs at most once.
            let marker = MigrationMarker {
                last_upgrade_version: CURRENT_MIGRATION_VERSION,
                last_run_at: now_unix_secs(),
                repairs_applied: Vec::new(),
            };
            if let Err(err) = write_marker_atomic(&db_path, &marker) {
                tracing::warn!(target: "fsqlite.migration", %err, db = %db_path, "failed to write migration marker for a clean database");
            }
            MigrationOutcome::CleanNoRepair
        }
        Err(integrity_err) => {
            // Preserve the original before any mutation.
            let backup_path = match backup_original(&db_path) {
                Ok(path) => path,
                Err(err) => {
                    tracing::warn!(target: "fsqlite.migration", %err, db = %db_path, "could not back up database before repair; leaving it untouched");
                    return MigrationOutcome::CleanNoRepair;
                }
            };

            // Apply the repairable-class repairs. `repair_orphaned_pages` only
            // re-frees genuinely-orphaned in-range pages (a no-op otherwise), so
            // running it is safe even when the corruption is a different class.
            let mut repairs_applied = Vec::new();
            match conn.repair_orphaned_pages().await {
                Ok(freed) if freed > 0 => {
                    repairs_applied.push(format!("repair_orphaned_pages:{freed}"));
                }
                Ok(_) => {}
                Err(err) => {
                    tracing::warn!(target: "fsqlite.migration", %err, db = %db_path, "repair_orphaned_pages failed during migration");
                }
            }

            // Best-effort confirmation (informational only).
            let integrity_ok_after = conn.validate_database_integrity(false).await.is_ok();
            if !integrity_ok_after {
                tracing::warn!(
                    target: "fsqlite.migration",
                    db = %db_path,
                    original = %integrity_err,
                    "database still fails integrity_check after the migration repair pass; original preserved at the backup"
                );
            }

            // Record the marker last (atomic), so its presence means done.
            let marker = MigrationMarker {
                last_upgrade_version: CURRENT_MIGRATION_VERSION,
                last_run_at: now_unix_secs(),
                repairs_applied: repairs_applied.clone(),
            };
            if let Err(err) = write_marker_atomic(&db_path, &marker) {
                tracing::warn!(target: "fsqlite.migration", %err, db = %db_path, "failed to write migration marker after repair");
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
        assert!(msg.contains("2.3s"), "one-decimal elapsed seconds; got: {msg}");
        assert!(
            msg.contains("/tmp/foo.db.pre-migration-bak"),
            "names the backup path; got: {msg}"
        );
    }
}
