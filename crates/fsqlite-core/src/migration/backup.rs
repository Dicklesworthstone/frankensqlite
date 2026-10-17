//! Retained, receipt-selected pre-migration backup generations.
//!
//! A retry never writes into a prior generation. Main and WAL are copied into
//! a fresh private directory; a bounded, checksummed receipt selects that
//! directory only after its required files are complete. Failed generations
//! are retained but never selected. Legacy `.pre-migration-bak*` files are not
//! rewritten, adopted, or combined with a new generation.
//!
//! `capture_migration_backup` captures native main/WAL through VFS handles
//! under the existing physical-copy fences. Receipt selection and verification
//! happen after the source handles release those fences. Capture preserves
//! existing corruption; a coherent file set is not an SQL-integrity verdict.

use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};

use fsqlite_error::{FrankenError, Result};
use fsqlite_vfs::host_fs;
use serde::{Deserialize, Serialize};

use super::{CURRENT_MIGRATION_VERSION, backup_file_atomic, sidecar_path};

#[cfg(any(unix, windows))]
mod live;
#[cfg(any(unix, windows))]
pub use live::capture_migration_backup;

const RECEIPT_VERSION: u32 = 1;
const MAX_RECEIPT_BYTES: u64 = 16 * 1024;
const GENERATION_PREFIX: &str = ".fsqlite-migration-";
const RESTORE_PREFIX: &str = ".fsqlite-restored-";
const GENERATION_RECEIPT: &str = "receipt.json";
const MAIN_FILE: &str = "database.db";
const WAL_FILE: &str = "database.db-wal";
#[cfg(test)]
const SHM_FILE: &str = "database.db-shm";

/// The atomically replaced selector, not the backup data itself.
#[must_use]
pub fn migration_backup_receipt_path(db_path: &str) -> PathBuf {
    sidecar_path(db_path, ".pre-migration-bak.receipt")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileDigest {
    bytes: u64,
    blake3: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    format_version: u32,
    migration_version: u32,
    source_name: String,
    generation: String,
    previous_generation: Option<String>,
    main: FileDigest,
    wal: Option<FileDigest>,
}

/// A retained backup generation. Paths are resolved only from validated local
/// directory names, never from arbitrary paths in a receipt.
///
/// Do not open the retained main file with a database engine: WAL recovery or
/// checkpoint could change the evidence. Work on a separate restore copy.
#[derive(Debug, Clone)]
pub struct MigrationBackup {
    directory: PathBuf,
    receipt: Receipt,
}

impl MigrationBackup {
    /// Directory containing this generation and its immutable receipt.
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Main file; any required WAL is adjacent using SQLite's usual suffix.
    #[must_use]
    pub fn main_path(&self) -> PathBuf {
        self.directory.join(MAIN_FILE)
    }

    /// An existing zero-byte WAL is distinct from an absent WAL.
    #[must_use]
    pub fn wal_path(&self) -> Option<PathBuf> {
        self.receipt.wal.as_ref().map(|_| self.directory.join(WAL_FILE))
    }

    /// Recheck the retained main/WAL byte counts and digests without opening
    /// either through a database engine. SHM is reconstructible and not an
    /// authority for restoration. This is byte integrity, not SQL integrity.
    ///
    /// # Errors
    /// Refuses missing, changed, aliased, or unexpected required files.
    pub fn verify(&self) -> Result<()> {
        if file_digest(&self.main_path())? != self.receipt.main {
            return Err(invalid_backup("main file does not match its receipt"));
        }
        let wal = self.directory.join(WAL_FILE);
        match &self.receipt.wal {
            Some(expected) if &file_digest(&wal)? != expected => {
                Err(invalid_backup("WAL does not match its receipt"))
            }
            Some(_) => Ok(()),
            None => require_absent(&wal),
        }
    }

    /// Return the previously selected generation observed during capture.
    /// Concurrent captures can observe the same predecessor; this is not a
    /// total ordering of all capture attempts. All directories are retained.
    ///
    /// # Errors
    /// Refuses malformed receipts or altered required backup bytes.
    pub fn previous(&self) -> Result<Option<Self>> {
        let Some(name) = &self.receipt.previous_generation else {
            return Ok(None);
        };
        let parent = self.directory.parent().ok_or_else(|| invalid_backup("missing parent"))?;
        let receipt = read_receipt(&parent.join(name).join(GENERATION_RECEIPT))?
            .ok_or_else(|| invalid_backup("previous generation receipt is missing"))?;
        validate_receipt(&receipt, &self.receipt.source_name)?;
        if &receipt.generation != name {
            return Err(invalid_backup("previous generation name mismatch"));
        }
        let previous = Self { directory: parent.join(name), receipt };
        previous.verify()?;
        Ok(Some(previous))
    }

    /// Materialize this restore point in a fresh private child of `parent`.
    /// Returns the new main database path, with exactly the required WAL next
    /// to it. The caller may open or modify this copy without checkpointing or
    /// changing the retained generation. Existing paths are never reused.
    ///
    /// Main and WAL are verified before any destination is created, and their
    /// copied bytes must independently match this handle's receipt. SHM,
    /// journals, migration markers, and unlisted files are not restored.
    /// Files are synced before return, with directory syncs on Unix. As with
    /// capture, Windows has no portable directory-fsync guarantee here.
    ///
    /// `parent` must already exist in a trusted, cooperative namespace. This
    /// method neither replaces the live source nor changes its selector.
    /// Restoring a pre-repair image may restore its original corruption: byte
    /// verification is not an SQL-integrity verdict or a repair operation.
    ///
    /// # Errors
    /// Refuses invalid source bytes before destination creation. Later copy,
    /// verification, or sync errors retain partial output for inspection but
    /// never return its path as a successful restore.
    pub fn restore_to_new_directory(&self, parent: &Path) -> Result<PathBuf> {
        self.restore_with(parent, backup_file_atomic)
    }

    fn restore_with(
        &self,
        parent: &Path,
        mut copy: impl FnMut(&Path, &Path) -> Result<bool>,
    ) -> Result<PathBuf> {
        self.verify()?;
        let directory = tempfile::Builder::new()
            .prefix(RESTORE_PREFIX)
            .tempdir_in(parent)?
            .keep();
        let main = directory.join(MAIN_FILE);
        copy_verified(&self.main_path(), &main, &self.receipt.main, &mut copy)?;
        let wal = directory.join(WAL_FILE);
        if let Some(expected) = &self.receipt.wal {
            copy_verified(&self.directory.join(WAL_FILE), &wal, expected, &mut copy)?;
        } else {
            require_absent(&wal)?;
        }
        // The copy helper syncs each data file before its atomic rename.
        // Only a complete, independently verified family reaches this point.
        sync_directory(&directory)?;
        sync_directory(parent)?;
        Ok(main)
    }
}

fn copy_verified(
    from: &Path,
    to: &Path,
    expected: &FileDigest,
    copy: &mut impl FnMut(&Path, &Path) -> Result<bool>,
) -> Result<()> {
    if !copy(from, to)? {
        return Err(invalid_backup("required restore file disappeared during copy"));
    }
    if &file_digest(to)? != expected {
        return Err(invalid_backup("restored bytes do not match their receipt"));
    }
    Ok(())
}

/// Load and verify the selected backup. Absence is distinct from an unreadable
/// or malformed selector, and no legacy raw backup is silently trusted.
///
/// # Errors
/// Refuses bad receipts, path traversal, and altered or missing main/WAL bytes.
pub fn read_migration_backup(db_path: &str) -> Result<Option<MigrationBackup>> {
    let Some(backup) = selected_backup(db_path)? else {
        return Ok(None);
    };
    backup.verify()?;
    Ok(Some(backup))
}

/// Lightweight path lookup for the legacy path helper. The explicit read API
/// above is the authority when the caller needs error or integrity reporting.
pub(super) fn selected_path(db_path: &str) -> Result<Option<PathBuf>> {
    Ok(selected_backup(db_path)?.map(|backup| backup.main_path()))
}

fn selected_backup(db_path: &str) -> Result<Option<MigrationBackup>> {
    let Some(receipt) = read_receipt(&migration_backup_receipt_path(db_path))? else {
        return Ok(None);
    };
    validate_receipt(&receipt, source_name(db_path)?)?;
    let directory = source_parent(db_path).join(&receipt.generation);
    let retained = read_receipt(&directory.join(GENERATION_RECEIPT))?
        .ok_or_else(|| invalid_backup("selected generation receipt is missing"))?;
    if retained != receipt {
        return Err(invalid_backup("selector and retained generation disagree"));
    }
    Ok(Some(MigrationBackup { directory, receipt }))
}

// Raw copying is deliberately restricted to stable, offline test fixtures.
// Every production capture uses the native VFS source-fence path.
#[cfg(test)]
pub(super) fn capture(db_path: &str) -> Result<MigrationBackup> {
    capture_with(db_path, backup_file_atomic)
}

// The copy seam allows real filesystem failure injection without changing
// production storage behavior or manufacturing successful source bytes.
#[cfg(test)]
pub(super) fn capture_with(
    db_path: &str,
    mut copy: impl FnMut(&Path, &Path) -> Result<bool>,
) -> Result<MigrationBackup> {
    let previous = read_migration_backup(db_path)?;
    let parent = source_parent(db_path);
    // Keep immediately: every error below preserves the partial evidence.
    let directory = tempfile::Builder::new()
        .prefix(GENERATION_PREFIX)
        .tempdir_in(parent)?
        .keep();
    let main_path = directory.join(MAIN_FILE);
    if !copy(Path::new(db_path), &main_path)? {
        return Err(FrankenError::DatabaseNotFound { path: PathBuf::from(db_path) });
    }
    let main = file_digest(&main_path)?;
    let wal_path = directory.join(WAL_FILE);
    let wal = if copy(&sidecar_path(db_path, "-wal"), &wal_path)? {
        Some(file_digest(&wal_path)?)
    } else {
        require_absent(&wal_path)?;
        None
    };
    if let Err(error) = copy(&sidecar_path(db_path, "-shm"), &directory.join(SHM_FILE)) {
        tracing::warn!(target: "fsqlite.migration", db = %db_path, %error,
            "could not preserve reconstructible SHM; main and WAL are required");
    }
    publish_generation(db_path, directory, previous, main, wal)
}

// Both coordinated native capture and offline filesystem fixtures select
// generations through the same byte-verification and publication protocol.
fn publish_generation(
    db_path: &str,
    directory: PathBuf,
    previous: Option<MigrationBackup>,
    main: FileDigest,
    wal: Option<FileDigest>,
) -> Result<MigrationBackup> {
    let parent = source_parent(db_path);
    let generation = directory.file_name().and_then(|name| name.to_str())
        .ok_or_else(|| invalid_backup("invalid reserved generation name"))?.to_owned();
    let receipt = Receipt {
        format_version: RECEIPT_VERSION,
        migration_version: CURRENT_MIGRATION_VERSION,
        source_name: source_name(db_path)?.to_owned(),
        generation,
        previous_generation: previous.map(|backup| backup.receipt.generation),
        main,
        wal,
    };
    validate_receipt(&receipt, source_name(db_path)?)?;
    let bytes = serde_json::to_vec(&receipt)
        .map_err(|error| FrankenError::internal(format!("serialize backup receipt: {error}")))?;
    if bytes.len() as u64 > MAX_RECEIPT_BYTES {
        return Err(FrankenError::TooBig);
    }
    host_fs::write_private_atomic(&directory.join(GENERATION_RECEIPT), &bytes)?;
    let backup = MigrationBackup { directory, receipt };
    backup.verify()?;
    // Persist the complete generation and its parent entry before selection.
    sync_directory(backup.directory())?;
    sync_directory(parent)?;
    host_fs::write_private_atomic(&migration_backup_receipt_path(db_path), &bytes)?;
    // A failure here may leave a visible selector; it is NOT a rollback claim.
    // No repair is authorized by this function until all required syncs pass.
    sync_directory(parent)?;
    Ok(backup)
}

fn read_receipt(path: &Path) -> Result<Option<Receipt>> {
    let file = match host_fs::open_existing_regular_file_no_follow(path) {
        Ok(file) => file,
        Err(FrankenError::Io(error)) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut bytes = Vec::new();
    file.take(MAX_RECEIPT_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_RECEIPT_BYTES {
        return Err(FrankenError::TooBig);
    }
    serde_json::from_slice(&bytes).map(Some)
        .map_err(|error| invalid_backup(&format!("invalid receipt: {error}")))
}

fn validate_receipt(receipt: &Receipt, source: &str) -> Result<()> {
    if receipt.format_version != RECEIPT_VERSION
        || receipt.migration_version == 0
        || receipt.source_name != source
        || !valid_generation(&receipt.generation)
        || receipt.previous_generation.as_ref().is_some_and(|name| {
            !valid_generation(name) || name == &receipt.generation
        })
    {
        return Err(invalid_backup("unsupported or inconsistent receipt"));
    }
    Ok(())
}

fn valid_generation(name: &str) -> bool {
    name.strip_prefix(GENERATION_PREFIX).is_some_and(|suffix| {
        !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_alphanumeric())
    })
}

fn file_digest(path: &Path) -> Result<FileDigest> {
    let file = host_fs::open_existing_regular_file_no_follow(path)?;
    let expected = file.metadata()?.len();
    let limit = expected.checked_add(1).ok_or(FrankenError::TooBig)?;
    let mut input = (&file).take(limit);
    let mut buffer = [0_u8; 8192];
    let mut hasher = blake3::Hasher::new();
    let mut bytes = 0_u64;
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 { break; }
        bytes = bytes.checked_add(count as u64).ok_or(FrankenError::TooBig)?;
        hasher.update(&buffer[..count]);
    }
    if bytes != expected || file.metadata()?.len() != expected {
        return Err(invalid_backup("backup changed length during verification"));
    }
    Ok(FileDigest { bytes, blake3: *hasher.finalize().as_bytes() })
}

fn require_absent(path: &Path) -> Result<()> {
    match host_fs::open_existing_regular_file_no_follow(path) {
        Err(FrankenError::Io(error)) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
        Ok(_) => Err(invalid_backup("receipt excludes a present WAL")),
    }
}

fn source_parent(db_path: &str) -> &Path {
    Path::new(db_path).parent().filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn source_name(db_path: &str) -> Result<&str> {
    Path::new(db_path).file_name().and_then(|name| name.to_str())
        .ok_or_else(|| invalid_backup("database path has no filename"))
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    host_fs::open_file(path)?.sync_all()?;
    // Match the native Windows VFS boundary: there is no portable directory
    // fsync there. Per-file sync/atomic visibility is not a power-loss proof.
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn invalid_backup(detail: &str) -> FrankenError {
    FrankenError::DatabaseCorrupt { detail: format!("migration backup: {detail}") }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (PathBuf, String) {
        let directory = tempfile::tempdir().unwrap().keep();
        let path = directory.join("source.db");
        host_fs::write(&path, b"original main").unwrap();
        let db = path.to_str().unwrap().to_owned();
        (directory, db)
    }

    #[test]
    fn restored_sqlite_generations_reopen_without_mutating_retained_evidence() {
        let directory = tempfile::tempdir().unwrap().keep();
        let path = directory.join("source.db");
        let db = path.to_str().unwrap();
        let source = rusqlite::Connection::open(&path).unwrap();
        source.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
             CREATE TABLE payload(id INTEGER PRIMARY KEY, value TEXT);
             PRAGMA wal_checkpoint(TRUNCATE);
             INSERT INTO payload VALUES(1, 'original');",
        ).unwrap();
        let first = capture(db).unwrap();
        assert!(first.wal_path().is_some());
        let first_receipt = host_fs::read(&first.directory.join(GENERATION_RECEIPT)).unwrap();
        source.execute_batch("UPDATE payload SET value='after repair' WHERE id=1;").unwrap();
        drop(source);
        assert!(!sidecar_path(db, "-wal").exists(), "last stock close checkpoints WAL");
        let second = capture(db).unwrap();
        assert!(second.wal_path().is_none());
        let selected = read_migration_backup(db).unwrap().unwrap();
        let historical = selected.previous().unwrap().unwrap();
        assert_eq!(historical.directory(), first.directory());
        let selector = host_fs::read(&migration_backup_receipt_path(db)).unwrap();

        for (backup, expected) in [(&historical, "original"), (&selected, "after repair")] {
            let restored_path = backup.restore_to_new_directory(&directory).unwrap();
            assert_ne!(restored_path, backup.main_path());
            let restored = rusqlite::Connection::open(&restored_path).unwrap();
            let value: String = restored.query_row(
                "SELECT value FROM payload WHERE id=1", [], |row| row.get(0),
            ).unwrap();
            assert_eq!(value, expected);
            let integrity: String = restored.query_row(
                "PRAGMA integrity_check", [], |row| row.get(0),
            ).unwrap();
            assert_eq!(integrity, "ok");
            restored.execute_batch(
                "UPDATE payload SET value='modified restore' WHERE id=1;
                 PRAGMA wal_checkpoint(TRUNCATE);",
            ).unwrap();
            drop(restored);
            backup.verify().unwrap();
        }
        first.verify().unwrap();
        second.verify().unwrap();
        assert_eq!(host_fs::read(&first.directory.join(GENERATION_RECEIPT)).unwrap(), first_receipt);
        assert_eq!(host_fs::read(&migration_backup_receipt_path(db)).unwrap(), selector);
        let untouched = rusqlite::Connection::open(&path).unwrap();
        let value: String = untouched.query_row(
            "SELECT value FROM payload WHERE id=1", [], |row| row.get(0),
        ).unwrap();
        assert_eq!(value, "after repair");
    }

    #[test]
    fn restore_uses_fresh_directories_and_excludes_unlisted_sidecars() {
        let (directory, db) = fixture();
        host_fs::write(&sidecar_path(&db, "-wal"), b"required WAL").unwrap();
        host_fs::write(&sidecar_path(&db, "-shm"), b"stale SHM index").unwrap();
        let backup = capture(&db).unwrap();
        host_fs::write(&backup.directory.join("database.db-journal"), b"unlisted journal").unwrap();
        host_fs::write(&backup.directory.join("database.db.fsqlite-migration-state"), b"old marker").unwrap();
        let parent = directory.join("restores");
        host_fs::create_dir_all(&parent).unwrap();
        host_fs::write(&parent.join(MAIN_FILE), b"existing destination").unwrap();
        let first = backup.restore_to_new_directory(&parent).unwrap();
        let second = backup.restore_to_new_directory(&parent).unwrap();
        assert_ne!(first.parent(), second.parent());
        for main in [&first, &second] {
            let entries = host_fs::read_dir_paths(main.parent().unwrap()).unwrap();
            assert_eq!(entries.len(), 2, "only main and WAL belong in a restore");
            assert!(entries.contains(main));
            assert!(entries.contains(&main.parent().unwrap().join(WAL_FILE)));
            assert_eq!(file_digest(main).unwrap(), backup.receipt.main);
            assert_eq!(file_digest(&main.parent().unwrap().join(WAL_FILE)).unwrap(),
                *backup.receipt.wal.as_ref().unwrap());
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                for entry in std::iter::once(main.parent().unwrap().to_path_buf()).chain(entries) {
                    assert_eq!(host_fs::metadata(&entry).unwrap().permissions().mode() & 0o077, 0);
                }
            }
        }
        assert_eq!(host_fs::read(&parent.join(MAIN_FILE)).unwrap(), b"existing destination");
        assert_eq!(host_fs::read(&backup.directory.join(SHM_FILE)).unwrap(), b"stale SHM index");
        backup.verify().unwrap();
    }

    #[test]
    fn restore_distinguishes_absent_from_empty_wal() {
        let (directory, db) = fixture();
        let without_wal = capture(&db).unwrap();
        host_fs::create_empty_file(&sidecar_path(&db, "-wal")).unwrap();
        let empty_wal = capture(&db).unwrap();
        let absent = without_wal.restore_to_new_directory(&directory).unwrap();
        let empty = empty_wal.restore_to_new_directory(&directory).unwrap();
        assert!(!absent.parent().unwrap().join(WAL_FILE).exists());
        assert_eq!(host_fs::metadata(&empty.parent().unwrap().join(WAL_FILE)).unwrap().len(), 0);
        without_wal.verify().unwrap();
        empty_wal.verify().unwrap();
    }

    #[test]
    fn restore_rejects_tampered_evidence_before_creating_output() {
        for file in [MAIN_FILE, WAL_FILE] {
            let (directory, db) = fixture();
            host_fs::write(&sidecar_path(&db, "-wal"), b"required WAL").unwrap();
            let backup = capture(&db).unwrap();
            let path = backup.directory.join(file);
            let mut bytes = host_fs::read(&path).unwrap();
            bytes[0] ^= 1;
            host_fs::write(&path, bytes).unwrap();
            let mut before = host_fs::read_dir_paths(&directory).unwrap();
            before.sort();
            assert!(backup.restore_to_new_directory(&directory).is_err());
            let mut after = host_fs::read_dir_paths(&directory).unwrap();
            after.sort();
            assert_eq!(before, after, "invalid {file} must fail before creating a restore");
        }
    }

    #[test]
    fn restore_required_wal_failure_preserves_partial_output_and_originals() {
        let (directory, db) = fixture();
        host_fs::write(&sidecar_path(&db, "-wal"), b"required WAL").unwrap();
        let backup = capture(&db).unwrap();
        let selector = host_fs::read(&migration_backup_receipt_path(&db)).unwrap();
        let mut partial = None;
        let result = backup.restore_with(&directory, |from, to| {
            if to.file_name() == Some(std::ffi::OsStr::new(WAL_FILE)) {
                partial = Some(to.parent().unwrap().to_path_buf());
                host_fs::create_dir_all(to)?;
                host_fs::write(&to.join("sentinel"), b"blocked WAL publication")?;
            }
            backup_file_atomic(from, to)
        });
        assert!(result.is_err());
        let partial = partial.unwrap();
        assert_eq!(host_fs::read(&partial.join(MAIN_FILE)).unwrap(), b"original main");
        assert_eq!(host_fs::read(&partial.join(WAL_FILE).join("sentinel")).unwrap(), b"blocked WAL publication");
        backup.verify().unwrap();
        assert_eq!(host_fs::read(&migration_backup_receipt_path(&db)).unwrap(), selector);
        let retry = backup.restore_to_new_directory(&directory).unwrap();
        assert_ne!(retry.parent().unwrap(), partial.as_path());
        backup.verify().unwrap();
    }

    #[test]
    fn restore_detects_changes_between_verification_and_copy() {
        for target in [MAIN_FILE, WAL_FILE] {
            let (directory, db) = fixture();
            host_fs::write(&sidecar_path(&db, "-wal"), b"required WAL").unwrap();
            let backup = capture(&db).unwrap();
            let mut changed = false;
            let result = backup.restore_with(&directory, |from, to| {
                if from.file_name() == Some(std::ffi::OsStr::new(target)) {
                    // Simulate an external alteration after the initial
                    // verification using real source bytes of the same size.
                    let mut bytes = host_fs::read(from)?;
                    bytes[0] ^= 1;
                    host_fs::write(from, bytes)?;
                    changed = true;
                }
                backup_file_atomic(from, to)
            });
            assert!(changed, "the initial receipt validation must pass");
            assert!(matches!(result, Err(FrankenError::DatabaseCorrupt { .. })), "{target}");
        }
    }

    #[test]
    fn restore_refuses_disappearing_required_wal_and_missing_parent() {
        let (directory, db) = fixture();
        host_fs::write(&sidecar_path(&db, "-wal"), b"required WAL").unwrap();
        let backup = capture(&db).unwrap();
        let missing_parent = directory.join("nonexistent-parent");
        assert!(backup.restore_to_new_directory(&missing_parent).is_err());
        assert!(!missing_parent.exists(), "restoration must not create parents");
        backup.verify().unwrap();
        let retained_wal = directory.join("retained-displaced-wal");
        let result = backup.restore_with(&directory, |from, to| {
            if from.file_name() == Some(std::ffi::OsStr::new(WAL_FILE)) {
                host_fs::rename(from, &retained_wal)?;
            }
            backup_file_atomic(from, to)
        });
        assert!(matches!(result, Err(FrankenError::DatabaseCorrupt { .. })));
        assert_eq!(host_fs::read(&retained_wal).unwrap(), b"required WAL");
        assert_eq!(host_fs::read(&backup.main_path()).unwrap(), b"original main");
    }

    #[test]
    fn retries_preserve_prior_generations_and_legacy_backups() {
        let (_directory, db) = fixture();
        let legacy = sidecar_path(&db, ".pre-migration-bak");
        let legacy_wal = sidecar_path(&db, ".pre-migration-bak-wal");
        host_fs::write(&legacy, b"legacy main").unwrap();
        host_fs::write(&legacy_wal, b"legacy WAL").unwrap();
        host_fs::write(&sidecar_path(&db, "-wal"), b"original WAL").unwrap();
        let first = capture(&db).unwrap();
        let first_receipt = host_fs::read(&first.directory.join(GENERATION_RECEIPT)).unwrap();
        host_fs::write(Path::new(&db), b"partially repaired main").unwrap();
        host_fs::write(&sidecar_path(&db, "-wal"), b"partially repaired WAL").unwrap();
        let second = capture(&db).unwrap();
        assert_ne!(first.directory(), second.directory());
        first.verify().unwrap();
        second.verify().unwrap();
        assert_eq!(host_fs::read(&first.main_path()).unwrap(), b"original main");
        assert_eq!(host_fs::read(&first.wal_path().unwrap()).unwrap(), b"original WAL");
        assert_eq!(host_fs::read(&first.directory.join(GENERATION_RECEIPT)).unwrap(), first_receipt);
        assert_eq!(host_fs::read(&second.main_path()).unwrap(), b"partially repaired main");
        assert_eq!(second.previous().unwrap().unwrap().directory(), first.directory());
        assert!(first.previous().unwrap().is_none());
        assert_eq!(read_migration_backup(&db).unwrap().unwrap().directory(), second.directory());
        assert_eq!(host_fs::read(&legacy).unwrap(), b"legacy main");
        assert_eq!(host_fs::read(&legacy_wal).unwrap(), b"legacy WAL");
    }

    #[test]
    fn absent_wal_on_retry_does_not_replay_an_earlier_generations_wal() {
        let (directory, db) = fixture();
        let wal = sidecar_path(&db, "-wal");
        host_fs::write(&wal, b"earlier committed frames").unwrap();
        let first = capture(&db).unwrap();
        host_fs::rename(&wal, &directory.join("retained-source-wal")).unwrap();
        host_fs::write(Path::new(&db), b"later standalone main").unwrap();
        let second = capture(&db).unwrap();
        assert!(second.wal_path().is_none());
        assert!(!second.directory.join(WAL_FILE).exists());
        second.verify().unwrap();
        first.verify().unwrap();
        assert_eq!(host_fs::read(&first.wal_path().unwrap()).unwrap(), b"earlier committed frames");
        assert_eq!(host_fs::read(&second.main_path()).unwrap(), b"later standalone main");
    }

    #[test]
    fn failed_wal_copy_retains_previous_selection_and_partial_generation() {
        let (_directory, db) = fixture();
        host_fs::write(&sidecar_path(&db, "-wal"), b"original WAL").unwrap();
        let first = capture(&db).unwrap();
        let selector = migration_backup_receipt_path(&db);
        let before = host_fs::read(&selector).unwrap();
        host_fs::write(Path::new(&db), b"retry main").unwrap();
        let mut partial = None;
        let result = capture_with(&db, |from, to| {
            if to.file_name() == Some(std::ffi::OsStr::new(WAL_FILE)) {
                partial = Some(to.parent().unwrap().to_path_buf());
                host_fs::create_dir_all(to)?;
                host_fs::write(&to.join("sentinel"), b"publication blocked")?;
            }
            backup_file_atomic(from, to)
        });
        assert!(result.is_err());
        let partial = partial.unwrap();
        assert_eq!(host_fs::read(&partial.join(MAIN_FILE)).unwrap(), b"retry main");
        assert!(!partial.join(GENERATION_RECEIPT).exists());
        assert_eq!(host_fs::read(&selector).unwrap(), before);
        assert_eq!(read_migration_backup(&db).unwrap().unwrap().directory(), first.directory());
        first.verify().unwrap();
        let retry = capture(&db).unwrap();
        assert_ne!(retry.directory(), partial.as_path());
        assert_eq!(retry.previous().unwrap().unwrap().directory(), first.directory());
    }

    #[test]
    fn generation_receipt_failure_never_selects_partial_data() {
        let (_directory, db) = fixture();
        let first = capture(&db).unwrap();
        let selector = migration_backup_receipt_path(&db);
        let before = host_fs::read(&selector).unwrap();
        let result = capture_with(&db, |from, to| {
            if to.file_name() == Some(std::ffi::OsStr::new(MAIN_FILE)) {
                let blocker = to.parent().unwrap().join(GENERATION_RECEIPT);
                host_fs::create_dir_all(&blocker)?;
                host_fs::write(&blocker.join("sentinel"), b"retain blocker")?;
            }
            backup_file_atomic(from, to)
        });
        assert!(result.is_err());
        assert_eq!(host_fs::read(&selector).unwrap(), before);
        first.verify().unwrap();
        assert_eq!(read_migration_backup(&db).unwrap().unwrap().directory(), first.directory());
    }

    #[test]
    fn malformed_or_escaping_selectors_are_errors_not_missing_backups() {
        let (_directory, db) = fixture();
        let original = capture(&db).unwrap();
        let selector = migration_backup_receipt_path(&db);
        for generation in ["../outside", "/absolute", ".fsqlite-migration-../outside",
            ".fsqlite-migration-..\\outside", GENERATION_PREFIX] {
            let mut invalid = original.receipt.clone();
            invalid.generation = generation.to_owned();
            host_fs::write_private_atomic(&selector, &serde_json::to_vec(&invalid).unwrap()).unwrap();
            assert!(read_migration_backup(&db).is_err(), "{generation}");
            assert!(capture(&db).is_err(), "must not hide an invalid prior selector");
        }
        for bad_version in [0, RECEIPT_VERSION + 1] {
            let mut invalid = original.receipt.clone();
            invalid.format_version = bad_version;
            host_fs::write_private_atomic(&selector, &serde_json::to_vec(&invalid).unwrap()).unwrap();
            assert!(read_migration_backup(&db).is_err());
        }
        let mut wrong_source = original.receipt.clone();
        wrong_source.source_name = "another.db".to_owned();
        host_fs::write_private_atomic(&selector, &serde_json::to_vec(&wrong_source).unwrap()).unwrap();
        assert!(read_migration_backup(&db).is_err());
        assert_eq!(host_fs::read(&original.main_path()).unwrap(), b"original main");
    }

    #[test]
    fn checksum_verification_detects_same_length_tampering_and_missing_wal() {
        for target in [MAIN_FILE, WAL_FILE] {
            let (_directory, db) = fixture();
            host_fs::write(&sidecar_path(&db, "-wal"), b"original WAL").unwrap();
            let backup = capture(&db).unwrap();
            let path = backup.directory.join(target);
            let mut bytes = host_fs::read(&path).unwrap();
            bytes[0] ^= 1;
            host_fs::write(&path, bytes).unwrap();
            assert!(backup.verify().is_err(), "{target}");
            assert!(read_migration_backup(&db).is_err(), "{target}");
        }
        let (directory, db) = fixture();
        host_fs::write(&sidecar_path(&db, "-wal"), b"required WAL").unwrap();
        let backup = capture(&db).unwrap();
        host_fs::rename(&backup.wal_path().unwrap(), &directory.join("retained-backup-wal")).unwrap();
        assert!(backup.verify().is_err());
    }

    #[test]
    fn unexpected_wal_and_disagreeing_receipt_are_refused() {
        let (_directory, db) = fixture();
        let backup = capture(&db).unwrap();
        host_fs::write(&backup.directory.join(WAL_FILE), b"stale WAL").unwrap();
        assert!(backup.verify().is_err());
        let mut changed = backup.receipt.clone();
        changed.main.bytes += 1;
        host_fs::write_private_atomic(
            &backup.directory.join(GENERATION_RECEIPT),
            &serde_json::to_vec(&changed).unwrap(),
        ).unwrap();
        assert!(selected_backup(&db).is_err());
    }

    #[test]
    fn receipt_reads_are_bounded_and_io_failures_are_not_absence() {
        let (directory, db) = fixture();
        let selector = migration_backup_receipt_path(&db);
        host_fs::write(&selector, vec![b'x'; MAX_RECEIPT_BYTES as usize + 1]).unwrap();
        assert!(matches!(read_migration_backup(&db), Err(FrankenError::TooBig)));
        host_fs::rename(&selector, &directory.join("retained-oversized-receipt")).unwrap();
        host_fs::create_dir_all(&selector).unwrap();
        assert!(read_migration_backup(&db).is_err());
        assert!(capture(&db).is_err());
        assert_eq!(host_fs::read(Path::new(&db)).unwrap(), b"original main");
    }

    #[test]
    fn concurrent_captures_select_complete_independent_generations() {
        let (_directory, db) = fixture();
        host_fs::write(&sidecar_path(&db, "-wal"), b"stable WAL").unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
        let threads: Vec<_> = (0..4).map(|_| {
            let db = db.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                capture(&db).unwrap()
            })
        }).collect();
        let backups: Vec<_> = threads.into_iter().map(|thread| thread.join().unwrap()).collect();
        let unique: std::collections::HashSet<_> = backups.iter().map(MigrationBackup::directory).collect();
        assert_eq!(unique.len(), 4);
        for backup in &backups {
            backup.verify().unwrap();
        }
        let selected = read_migration_backup(&db).unwrap().unwrap();
        assert!(backups.iter().any(|backup| backup.directory() == selected.directory()));
    }

    #[test]
    #[cfg(unix)]
    fn generation_and_receipts_are_private_from_creation() {
        use std::os::unix::fs::PermissionsExt as _;
        let (_directory, db) = fixture();
        host_fs::write(&sidecar_path(&db, "-wal"), b"private WAL").unwrap();
        let backup = capture(&db).unwrap();
        for path in [backup.directory.clone(), backup.main_path(), backup.wal_path().unwrap(),
            backup.directory.join(GENERATION_RECEIPT), migration_backup_receipt_path(&db)] {
            assert_eq!(host_fs::metadata(&path).unwrap().permissions().mode() & 0o077, 0);
        }
    }
}
