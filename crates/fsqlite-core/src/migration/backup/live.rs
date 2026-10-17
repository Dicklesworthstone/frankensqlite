//! Coherent native capture without raw main/WAL descriptors.
//!
//! WAL capture reuses the VFS checkpoint attempt (WRITE + CKPT + native
//! appender fence). Rollback capture reuses the shared-snapshot attempt and
//! refuses a non-inert journal. These are physical-copy fences, not transaction
//! writer serialization or a lock-free online backup algorithm. Source file
//! ownership and cancellation cleanup follow native WAL recovery's protocol.

use std::io::Write as _;
use std::path::Path;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_vfs::{FileIdentity, Vfs, VfsFile, host_fs};

use crate::connection::Connection;

use super::{
    FileDigest, GENERATION_PREFIX, MAIN_FILE, MigrationBackup, WAL_FILE, invalid_backup,
    publish_generation, read_migration_backup, sidecar_path, source_parent,
};

#[cfg(unix)]
type NativeVfs = fsqlite_vfs::unix::UnixVfs;
#[cfg(windows)]
type NativeVfs = fsqlite_vfs::windows::WindowsVfs;
type NativeFile = <NativeVfs as Vfs>::File;

const COPY_BYTES: usize = 64 * 1024;
const HEADER_BYTES: usize = 100;

/// Capture the committed native on-disk state into a retained backup generation.
///
/// Use `MigrationBackup::restore_to_new_directory` before opening the result.
/// This does not copy transaction-local pending writes or certify SQL integrity.
/// The connection supplies context/cancellation lineage; no runtime is created.
///
/// WAL appenders and checkpoints are excluded only during the streamed source
/// copy. Rollback mode uses a shared main-file fence. Acquisition is fallible:
/// contention, unsupported locks, or cancellation never fall back to raw copies.
/// Existing reader attachments may remain, subject to the platform VFS's fence
/// admission rules. Main/WAL reads and closes use VFS ownership throughout, so
/// no independent raw main descriptor can release another connection's locks.
///
/// The source must be a native database in a trusted, cooperative namespace.
/// Each data file uses a 64 KiB application copy buffer; no full image is loaded.
/// Destination writes/syncs and receipt verification are synchronous filesystem
/// work. This is a maintenance capture, not a nonblocking or deadline-guaranteed
/// backup API. Unsupported native targets have no implementation. Existing
/// receipt, restoration, and Windows directory-durability limitations still apply.
///
/// # Errors
/// Refuses unknown database modes, non-inert rollback journals, changed source
/// identities/sizes, required-file failures, and failed lock restoration. Partial
/// generations are retained but never selected after a failed source capture.
pub async fn capture_migration_backup(conn: &Connection) -> Result<MigrationBackup> {
    let cx = conn.root_cx().create_child();
    capture_with_probe(&cx, conn.path(), |_| Ok(())).await
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Rollback,
    Wal,
}

/// Arm the cleanup owner before any lock side effect, including a failed
/// acquisition. Native backend close/Drop retains its own unresolved ledger.
struct SourceFile {
    file: NativeFile,
    cleanup_cx: Cx,
    attempt: Option<Mode>,
    closed: bool,
}

impl SourceFile {
    fn new(file: NativeFile, cx: &Cx) -> Self {
        Self {
            file,
            cleanup_cx: cx.create_child(),
            attempt: None,
            closed: false,
        }
    }

    fn acquire(&mut self, cx: &Cx, mode: Mode) -> Result<()> {
        self.attempt = Some(mode);
        match mode {
            Mode::Wal => self.file.lock_external_wal_checkpoint(cx)?,
            Mode::Rollback => self.file.lock_external_shared_snapshot(cx)?,
        }
        if self.file.locking_downgraded_to_whole_file_flock() {
            return Err(FrankenError::Unsupported);
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        let _mask = self.cleanup_cx.masked();
        match self.attempt {
            Some(Mode::Wal) => self
                .file
                .restore_external_maintenance_attempt(&self.cleanup_cx)?,
            Some(Mode::Rollback) => self
                .file
                .restore_external_shared_snapshot_attempt(&self.cleanup_cx)?,
            None => {}
        }
        self.attempt = None;
        self.file.close(&self.cleanup_cx)?;
        self.closed = true;
        Ok(())
    }
}

impl Drop for SourceFile {
    fn drop(&mut self) {
        if let Err(error) = self.finish() {
            tracing::warn!(target: "fsqlite.migration", %error,
                "capture source cleanup requires native backend retry");
        }
    }
}

fn checkpoint(cx: &Cx) -> Result<()> {
    cx.checkpoint().map_err(|_| FrankenError::Interrupt)
}

fn header_mode(header: &[u8; HEADER_BYTES]) -> Result<Mode> {
    if !header.starts_with(b"SQLite format 3\0") {
        return Err(invalid_backup("source does not have a SQLite header"));
    }
    match (header[18], header[19]) {
        (1, 1) => Ok(Mode::Rollback),
        (2, 2) => Ok(Mode::Wal),
        _ => Err(invalid_backup("source has an unknown journal mode")),
    }
}

async fn read_exact_at(file: &NativeFile, cx: &Cx, bytes: &mut [u8], offset: u64) -> Result<()> {
    let mut read = 0;
    while read < bytes.len() {
        checkpoint(cx)?;
        let position = offset.checked_add(read as u64).ok_or(FrankenError::TooBig)?;
        let count = file.read(cx, &mut bytes[read..], position).await?;
        if count == 0 || count > bytes.len() - read {
            return Err(FrankenError::ShortRead { expected: bytes.len(), actual: read });
        }
        read += count;
    }
    Ok(())
}

async fn read_mode(file: &NativeFile, cx: &Cx) -> Result<Mode> {
    let mut header = [0_u8; HEADER_BYTES];
    read_exact_at(file, cx, &mut header, 0).await?;
    header_mode(&header)
}

fn identity(file: &NativeFile) -> Result<FileIdentity> {
    file.file_identity()?.ok_or(FrankenError::Unsupported)
}

fn require_regular(path: &Path) -> Result<()> {
    if !host_fs::metadata(path)?.is_file() {
        return Err(FrankenError::CannotOpen { path: path.to_path_buf() });
    }
    Ok(())
}

fn revalidate(
    vfs: &NativeVfs,
    cx: &Cx,
    path: &Path,
    flags: VfsOpenFlags,
    expected: FileIdentity,
) -> Result<()> {
    let (probe, _) = vfs.open_with_expected_identity(cx, path, flags, expected)?;
    SourceFile::new(probe, cx).finish()
}

/// A hot or ambiguous rollback journal cannot be omitted from a restorable
/// main-only image. Zeroed PERSIST headers and empty TRUNCATE journals are inert.
/// Open even the journal through VFS: a bad alias must not close a raw main fd.
async fn require_inert_journal(
    vfs: &NativeVfs,
    cx: &Cx,
    db_path: &str,
    main_identity: FileIdentity,
) -> Result<()> {
    let path = sidecar_path(db_path, "-journal");
    if !vfs.path_entry_exists(cx, &path)? {
        return Ok(());
    }
    require_regular(&path)?;
    let (file, _) = vfs.open(cx, Some(&path), VfsOpenFlags::READONLY)?;
    let mut journal = SourceFile::new(file, cx);
    if identity(&journal.file)? == main_identity {
        return Err(FrankenError::BusyRecovery);
    }
    let length = usize::try_from(journal.file.file_size(cx)?.min(28))
        .map_err(|_| FrankenError::TooBig)?;
    let mut header = [0_u8; 28];
    read_exact_at(&journal.file, cx, &mut header[..length], 0).await?;
    if header[..length].iter().any(|byte| *byte != 0) {
        return Err(FrankenError::BusyRecovery);
    }
    journal.finish()
}

async fn copy_file(file: &NativeFile, cx: &Cx, path: &Path) -> Result<FileDigest> {
    checkpoint(cx)?;
    let length = file.file_size(cx)?;
    // This file is inside an unselected fresh generation. A failed write leaves
    // evidence there; no partially written data pathname is ever selected.
    let mut destination = host_fs::reserve_new_file(path)?;
    let mut buffer = Vec::new();
    buffer.try_reserve_exact(COPY_BYTES).map_err(|_| FrankenError::OutOfMemory)?;
    buffer.resize(COPY_BYTES, 0);
    let mut hasher = blake3::Hasher::new();
    let mut offset = 0_u64;
    while offset < length {
        let count = usize::try_from((length - offset).min(COPY_BYTES as u64))
            .map_err(|_| FrankenError::TooBig)?;
        read_exact_at(file, cx, &mut buffer[..count], offset).await?;
        destination.write_all(&buffer[..count])?;
        hasher.update(&buffer[..count]);
        offset += count as u64;
    }
    checkpoint(cx)?;
    if file.file_size(cx)? != length {
        return Err(FrankenError::BusyRecovery);
    }
    destination.sync_all()?;
    Ok(FileDigest { bytes: length, blake3: *hasher.finalize().as_bytes() })
}

// The probe runs after main is copied but before WAL is read, while the actual
// source fence is held. Tests use real peer processes and real output failures.
async fn capture_with_probe(
    cx: &Cx,
    db_path: &str,
    after_main: impl FnOnce(&Path) -> Result<()>,
) -> Result<MigrationBackup> {
    checkpoint(cx)?;
    if db_path == ":memory:" {
        return Err(FrankenError::Unsupported);
    }
    let previous = read_migration_backup(db_path)?;
    let vfs = NativeVfs::new();
    let main_flags = VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB;
    require_regular(Path::new(db_path))?;
    let (file, actual_flags) = vfs.open(cx, Some(Path::new(db_path)), main_flags)?;
    let mut main = SourceFile::new(file, cx);
    if !actual_flags.contains(VfsOpenFlags::READWRITE) {
        return Err(FrankenError::ReadOnly);
    }
    let mode = read_mode(&main.file, cx).await?;
    main.acquire(cx, mode)?;
    // Mode discovery preceded acquisition. Never use a rollback-only fence
    // for a source which switched to WAL in that interval (or vice versa).
    if read_mode(&main.file, cx).await? != mode {
        return Err(FrankenError::BusyRecovery);
    }
    let main_identity = identity(&main.file)?;
    // A rollback journal is never part of the restore receipt. Refuse a
    // non-inert one even with a WAL header instead of guessing at recovery.
    require_inert_journal(&vfs, cx, db_path, main_identity).await?;
    let directory = tempfile::Builder::new()
        .prefix(GENERATION_PREFIX)
        .tempdir_in(source_parent(db_path))?
        .keep();
    let main_digest = copy_file(&main.file, cx, &directory.join(MAIN_FILE)).await?;
    after_main(&directory)?;
    checkpoint(cx)?;

    // Declared after main, so error unwinding also closes WAL before main.
    let wal_path = sidecar_path(db_path, "-wal");
    let wal_flags = VfsOpenFlags::READONLY | VfsOpenFlags::WAL;
    let mut wal = if mode == Mode::Wal && vfs.path_entry_exists(cx, &wal_path)? {
        require_regular(&wal_path)?;
        let (file, _) = vfs.open(cx, Some(&wal_path), wal_flags)?;
        Some(SourceFile::new(file, cx))
    } else {
        None
    };
    let wal_digest = if let Some(wal) = wal.as_mut() {
        let wal_identity = identity(&wal.file)?;
        if wal_identity == main_identity {
            return Err(FrankenError::BusyRecovery);
        }
        let digest = copy_file(&wal.file, cx, &directory.join(WAL_FILE)).await?;
        revalidate(&vfs, cx, &wal_path, wal_flags, wal_identity)?;
        Some(digest)
    } else {
        // Rollback-mode WAL leftovers are not recovery authority. For WAL
        // mode, absence itself must remain stable under the source fence.
        if mode == Mode::Wal && vfs.path_entry_exists(cx, &wal_path)? {
            return Err(FrankenError::BusyRecovery);
        }
        None
    };
    if read_mode(&main.file, cx).await? != mode
        || main.file.file_size(cx)? != main_digest.bytes
    {
        return Err(FrankenError::BusyRecovery);
    }
    revalidate(&vfs, cx, Path::new(db_path), main_flags, main_identity)?;
    checkpoint(cx)?;
    if let Some(wal) = wal.as_mut() {
        wal.finish()?;
    }
    main.finish()?;
    checkpoint(cx)?;
    // No source descriptor or physical-copy fence is used for the expensive
    // retained-file verification and receipt publication below.
    publish_generation(db_path, directory, previous, main_digest, wal_digest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::{GENERATION_RECEIPT, migration_backup_receipt_path};
    use std::path::PathBuf;
    use std::time::Duration;

    const PEER_PATH: &str = "FSQLITE_MIGRATION_CAPTURE_PEER_PATH";
    const PEER_ACTION: &str = "FSQLITE_MIGRATION_CAPTURE_PEER_ACTION";
    const PEER_DONE: &str = "FSQLITE_CAPTURE_PEER_ASSERTIONS_PASSED";

    fn is_busy(error: &rusqlite::Error) -> bool {
        matches!(error, rusqlite::Error::SqliteFailure(code, _)
            if code.code == rusqlite::ErrorCode::DatabaseBusy)
    }

    // Subprocess endpoint, dispatched by the real Rust test executable. The
    // parent requires a completion token, so zero matched tests cannot pass.
    #[test]
    fn stock_peer_process() {
        let Some(path) = std::env::var_os(PEER_PATH) else { return; };
        let action = std::env::var(PEER_ACTION).unwrap();
        let stock = rusqlite::Connection::open(path).unwrap();
        stock.busy_timeout(Duration::ZERO).unwrap();
        if action == "mode-blocked" {
            match stock.query_row("PRAGMA journal_mode=DELETE", [], |row| row.get::<_, String>(0)) {
                Ok(mode) => assert_eq!(mode, "wal"),
                Err(error) => assert!(is_busy(&error), "{error}"),
            }
        } else if action == "write" {
            stock.execute("UPDATE payload SET value='after' WHERE id=1", []).unwrap();
            let result: (i64, i64, i64) = stock.query_row(
                "PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                },
            ).unwrap();
            assert_eq!(result.0, 0, "checkpoint must be admitted after capture cleanup");
        } else {
            let value: String = stock.query_row(
                "SELECT value FROM payload WHERE id=1", [], |row| row.get(0),
            ).unwrap();
            assert_eq!(value, "before", "existing readers still see committed data");
            let error = stock.execute("UPDATE payload SET value='forbidden' WHERE id=1", [])
                .expect_err("physical capture must exclude a peer commit");
            assert!(is_busy(&error), "expected BUSY, not a broken SQL fixture: {error}");
            if action == "fenced-wal" {
                let result: (i64, i64, i64) = stock.query_row(
                    "PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    },
                ).unwrap();
                assert_eq!(result.0, 1, "capture must also exclude checkpoints");
            } else {
                assert_eq!(action, "fenced-rollback");
            }
        }
        println!("{PEER_DONE}");
    }

    fn run_peer(db: &str, action: &str) {
        let module = module_path!().split_once("::").unwrap().1;
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(format!("{module}::stock_peer_process"))
            .arg("--nocapture")
            .env(PEER_PATH, db)
            .env(PEER_ACTION, action)
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success() && stdout.contains(PEER_DONE),
            "peer {action}: {}\n{stdout}\n{stderr}", output.status);
    }

    fn fixture(wal: bool) -> (PathBuf, String, rusqlite::Connection) {
        let directory = tempfile::tempdir().unwrap().keep();
        let db = directory.join("source.db").to_str().unwrap().to_owned();
        let stock = rusqlite::Connection::open(&db).unwrap();
        if wal {
            stock.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;").unwrap();
        }
        stock.execute_batch(
            "CREATE TABLE payload(id INTEGER PRIMARY KEY, value TEXT, bytes BLOB);
             PRAGMA wal_checkpoint(TRUNCATE);
             INSERT INTO payload VALUES(1, 'before', zeroblob(180000));",
        ).unwrap();
        (directory, db, stock)
    }

    fn assert_restores(backup: &MigrationBackup, parent: &Path, expected: &str) {
        let path = backup.restore_to_new_directory(parent).unwrap();
        let restored = rusqlite::Connection::open(path).unwrap();
        let row: (String, i64) = restored.query_row(
            "SELECT value, length(bytes) FROM payload WHERE id=1", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            },
        ).unwrap();
        assert_eq!(row, (expected.to_owned(), 180000));
        let verdict: String = restored.query_row("PRAGMA integrity_check", [], |row| row.get(0)).unwrap();
        assert_eq!(verdict, "ok");
        drop(restored);
        backup.verify().unwrap();
    }

    #[test]
    fn wal_capture_excludes_peer_writes_and_checkpoints_but_preserves_readers() {
        asupersync::test_utils::run_test(|| async {
            let (directory, db, stock) = fixture(true);
            let conn = Connection::open_schema_only(&db).await.unwrap();
            let cx = conn.root_cx().create_child();
            let first = capture_with_probe(&cx, &db, |_| {
                run_peer(&db, "fenced-wal");
                Ok(())
            }).await.unwrap();
            assert!(first.wal_path().is_some());
            assert_restores(&first, &directory, "before");
            // Closing capture's VFS probes must not remove the original
            // connection's main-file WAL lifetime read claim.
            run_peer(&db, "mode-blocked");
            run_peer(&db, "write");
            let second = capture_migration_backup(&conn).await.unwrap();
            assert_restores(&second, &directory, "after");
            assert_restores(&first, &directory, "before");
            assert_eq!(second.previous().unwrap().unwrap().directory(), first.directory());
            conn.close().await.unwrap();
            drop(stock);
        });
    }

    #[test]
    fn rollback_capture_excludes_peer_commits_and_restores_a_standalone_main() {
        asupersync::test_utils::run_test(|| async {
            let (directory, db, stock) = fixture(false);
            let conn = Connection::open_schema_only(&db).await.unwrap();
            let cx = conn.root_cx().create_child();
            let backup = capture_with_probe(&cx, &db, |_| {
                run_peer(&db, "fenced-rollback");
                Ok(())
            }).await.unwrap();
            assert!(backup.wal_path().is_none());
            assert_restores(&backup, &directory, "before");
            run_peer(&db, "write");
            assert_restores(&backup, &directory, "before");
            conn.close().await.unwrap();
            drop(stock);
        });
    }

    #[test]
    fn failed_wal_output_retains_selection_and_releases_source_fences() {
        asupersync::test_utils::run_test(|| async {
            let (directory, db, stock) = fixture(true);
            let conn = Connection::open_schema_only(&db).await.unwrap();
            let first = capture_migration_backup(&conn).await.unwrap();
            let selector = host_fs::read(&migration_backup_receipt_path(&db)).unwrap();
            let cx = conn.root_cx().create_child();
            let mut partial = None;
            let result = capture_with_probe(&cx, &db, |generation| {
                run_peer(&db, "fenced-wal");
                partial = Some(generation.to_path_buf());
                host_fs::create_dir_all(&generation.join(WAL_FILE))?;
                Ok(())
            }).await;
            assert!(result.is_err());
            let partial = partial.unwrap();
            assert!(partial.join(MAIN_FILE).exists());
            assert!(!partial.join(GENERATION_RECEIPT).exists());
            assert_eq!(host_fs::read(&migration_backup_receipt_path(&db)).unwrap(), selector);
            run_peer(&db, "write");
            let retry = capture_migration_backup(&conn).await.unwrap();
            assert_restores(&retry, &directory, "after");
            assert_restores(&first, &directory, "before");
            conn.close().await.unwrap();
            drop(stock);
        });
    }

    #[test]
    fn cancellation_after_main_retains_selection_and_releases_fences() {
        asupersync::test_utils::run_test(|| async {
            let (directory, db, stock) = fixture(true);
            let conn = Connection::open_schema_only(&db).await.unwrap();
            let first = capture_migration_backup(&conn).await.unwrap();
            let selector = host_fs::read(&migration_backup_receipt_path(&db)).unwrap();
            let cx = conn.root_cx().create_child();
            let mut partial = None;
            let result = capture_with_probe(&cx, &db, |generation| {
                partial = Some(generation.to_path_buf());
                cx.cancel();
                Ok(())
            }).await;
            assert!(matches!(result, Err(FrankenError::Interrupt)));
            assert!(!partial.unwrap().join(GENERATION_RECEIPT).exists());
            assert_eq!(host_fs::read(&migration_backup_receipt_path(&db)).unwrap(), selector);
            run_peer(&db, "write");
            assert_restores(&first, &directory, "before");
            conn.close().await.unwrap();
            drop(stock);
        });
    }

    #[test]
    fn rollback_capture_refuses_noninert_journal_and_accepts_zeroed_header() {
        asupersync::test_utils::run_test(|| async {
            let (directory, db, stock) = fixture(false);
            let conn = Connection::open_schema_only(&db).await.unwrap();
            let journal = sidecar_path(&db, "-journal");
            let mut bytes = vec![0_u8; 512];
            bytes[..8].copy_from_slice(&[0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7]);
            host_fs::write(&journal, &bytes).unwrap();
            assert!(capture_migration_backup(&conn).await.is_err());
            assert!(!migration_backup_receipt_path(&db).exists());
            assert_eq!(host_fs::read(&journal).unwrap(), bytes);
            host_fs::rename(&journal, &directory.join("retained-hot-journal")).unwrap();
            // PERSIST mode zeroes its header instead of unlinking the journal.
            host_fs::write(&journal, vec![0_u8; 512]).unwrap();
            let backup = capture_migration_backup(&conn).await.unwrap();
            assert!(backup.wal_path().is_none());
            assert_restores(&backup, &directory, "before");
            conn.close().await.unwrap();
            drop(stock);
        });
    }

    #[test]
    fn frame_free_wal_is_preserved_and_pre_cancelled_capture_creates_no_generation() {
        asupersync::test_utils::run_test(|| async {
            let (directory, db, stock) = fixture(true);
            stock.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            let conn = Connection::open_schema_only(&db).await.unwrap();
            let backup = capture_migration_backup(&conn).await.unwrap();
            assert_eq!(backup.receipt.wal.as_ref().unwrap().bytes, 0);
            assert_restores(&backup, &directory, "before");
            let mut before = host_fs::read_dir_paths(&directory).unwrap();
            before.sort();
            let cx = conn.root_cx().create_child();
            cx.cancel();
            assert!(matches!(capture_with_probe(&cx, &db, |_| Ok(())).await,
                Err(FrankenError::Interrupt)));
            let mut after = host_fs::read_dir_paths(&directory).unwrap();
            after.sort();
            assert_eq!(after, before);
            conn.close().await.unwrap();
            drop(stock);
        });
    }

    #[test]
    fn unknown_or_mixed_header_modes_are_never_assumed_to_be_rollback() {
        let mut header = [0_u8; HEADER_BYTES];
        assert!(header_mode(&header).is_err());
        header[..16].copy_from_slice(b"SQLite format 3\0");
        for (read, write, expected) in [
            (1, 1, Some(Mode::Rollback)), (2, 2, Some(Mode::Wal)),
            (1, 2, None), (2, 1, None), (0, 0, None), (3, 3, None),
        ] {
            header[18] = read;
            header[19] = write;
            assert_eq!(header_mode(&header).ok(), expected);
        }
    }
}
