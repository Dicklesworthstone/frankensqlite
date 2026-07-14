//! Lifetime binding between an opened database and its pathname namespace.
//!
//! Native file VFSes use two persistent sidecars.  The `gate` lock serializes
//! admission, while the `use` lock is shared by every connection bound to the
//! same file identity.  A new generation may replace the identity record only
//! while it owns `use` exclusively.  Reserved-empty bootstrap retains both
//! locks exclusively until [`DatabaseNamespaceBinding::finish_bootstrap`].
//! Sidecars are deliberately never unlinked for ordinary database lifetimes:
//! unlinking a locked file would split the advisory-lock domain on Unix. The
//! sole exception is [`cleanup_abandoned_private_database`], which is limited
//! to a caller-reserved transient candidate after all pager bindings have
//! closed and requires exclusive ownership of both namespace locks.
//!
//! This is a cooperative, trusted-parent protocol.  Native processes that
//! bypass FrankenSQLite can ignore advisory locks, and Unix permits a raw
//! unlink/rename despite an open descriptor.  Callers must not mutate the
//! database namespace or these sidecars outside the library while a binding
//! is live, and must not open one database through multiple hard-link aliases.

use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use advisory_lock::{AdvisoryFileLock, FileLockError, FileLockMode};
use fsqlite_error::{FrankenError, Result};

use crate::traits::FileIdentity;

const GATE_SUFFIX: &str = "-fsqlite-ns-gate";
const USE_SUFFIX: &str = "-fsqlite-ns-use";
const RECORD_MAGIC: [u8; 8] = *b"FSQLNS01";
const RECORD_VERSION: u8 = 1;
const IDENTITY_BYTES: usize = 25;
const RECORD_BYTES: usize = 40;

/// Admission mode for a database namespace.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NamespaceOpenIntent {
    /// Join the live generation, or establish a new shared generation when no
    /// connection currently owns the namespace.
    Shared,
    /// Exclusively reserve the namespace through empty-database bootstrap.
    ReservedExclusive,
}

#[derive(Debug)]
enum PendingLease {
    NewShared {
        gate: File,
        use_file: File,
    },
    JoinShared {
        gate: File,
        use_file: File,
        generation_identity: FileIdentity,
    },
    BootstrapExclusive {
        gate: File,
        use_file: File,
    },
}

/// Admission guard held while the caller opens and verifies the main file.
///
/// Dropping this value at any error point releases every acquired lock.
#[derive(Debug)]
pub struct PendingNamespaceOpen {
    stable_path: PathBuf,
    lease: Option<PendingLease>,
}

impl PendingNamespaceOpen {
    /// Begin namespace admission for an already-resolved absolute database
    /// path.  This operation is non-blocking; lock contention returns BUSY.
    pub fn begin(stable_path: &Path, intent: NamespaceOpenIntent) -> Result<Self> {
        validate_stable_path(stable_path)?;
        let gate = open_secure_lock_file(&sidecar_path(stable_path, GATE_SUFFIX))?;
        let mut use_file = open_secure_lock_file(&sidecar_path(stable_path, USE_SUFFIX))?;
        try_lock(&gate, FileLockMode::Exclusive)?;

        let lease = match intent {
            NamespaceOpenIntent::ReservedExclusive => {
                try_lock(&use_file, FileLockMode::Exclusive)?;
                PendingLease::BootstrapExclusive { gate, use_file }
            }
            NamespaceOpenIntent::Shared => {
                match AdvisoryFileLock::try_lock(&use_file, FileLockMode::Exclusive) {
                    Ok(()) => PendingLease::NewShared { gate, use_file },
                    Err(FileLockError::AlreadyLocked) => {
                        try_lock(&use_file, FileLockMode::Shared)?;
                        let generation_identity = read_identity_record(&mut use_file, stable_path)?;
                        PendingLease::JoinShared {
                            gate,
                            use_file,
                            generation_identity,
                        }
                    }
                    Err(FileLockError::Io(error)) => return Err(error.into()),
                }
            }
        };

        Ok(Self {
            stable_path: stable_path.to_owned(),
            lease: Some(lease),
        })
    }

    /// Identity of the live generation this admission must join.  When this
    /// returns `Some`, callers must strip CREATE/EXCLUSIVE and open that exact
    /// existing identity before calling [`Self::bind`].
    #[must_use]
    pub fn expected_identity(&self) -> Option<FileIdentity> {
        match self.lease.as_ref() {
            Some(PendingLease::JoinShared {
                generation_identity,
                ..
            }) => Some(*generation_identity),
            _ => None,
        }
    }

    /// Bind admission to the identity obtained from the opened main-file
    /// descriptor.  No recovery artifact may be inspected before this step.
    pub fn bind(mut self, identity: FileIdentity) -> Result<Arc<DatabaseNamespaceBinding>> {
        let lease = self
            .lease
            .take()
            .ok_or_else(|| FrankenError::internal("namespace admission already consumed"))?;

        let state = match lease {
            PendingLease::NewShared { gate, mut use_file } => {
                write_identity_record(&mut use_file, identity)?;
                // Keep a new generation exclusive through pager
                // initialization.  Otherwise a peer could join a freshly
                // created zero-length file before page 1 is durable.
                BindingLease::BootstrapExclusive { gate, use_file }
            }
            PendingLease::JoinShared {
                gate,
                mut use_file,
                generation_identity,
            } => {
                if read_identity_record(&mut use_file, &self.stable_path)? != generation_identity
                    || identity != generation_identity
                {
                    return Err(cannot_open(&self.stable_path));
                }
                release_gate(gate)?;
                BindingLease::Shared {
                    _use_file: use_file,
                }
            }
            PendingLease::BootstrapExclusive { gate, mut use_file } => {
                write_identity_record(&mut use_file, identity)?;
                BindingLease::BootstrapExclusive { gate, use_file }
            }
        };

        Ok(Arc::new(DatabaseNamespaceBinding {
            stable_path: self.stable_path,
            identity,
            lease: Mutex::new(state),
        }))
    }
}

#[derive(Debug)]
enum BindingLease {
    Shared { _use_file: File },
    BootstrapExclusive { gate: File, use_file: File },
    Transitioning,
}

/// Lifetime lease binding all path-derived companions to one main-file
/// identity.  Keep this value alive for the full connection lifetime.
#[derive(Debug)]
pub struct DatabaseNamespaceBinding {
    stable_path: PathBuf,
    identity: FileIdentity,
    lease: Mutex<BindingLease>,
}

impl DatabaseNamespaceBinding {
    /// The single absolute path from which all companion names must derive.
    #[must_use]
    pub fn stable_path(&self) -> &Path {
        &self.stable_path
    }

    /// The main-file identity to which this lease is bound.
    #[must_use]
    pub const fn identity(&self) -> FileIdentity {
        self.identity
    }

    /// Side-effect-free identity validation for operation boundaries.  The
    /// caller obtains the current pathname identity through its VFS first.
    pub fn validate_identity(&self, current: Option<FileIdentity>) -> Result<()> {
        if current == Some(self.identity) {
            Ok(())
        } else {
            Err(cannot_open(&self.stable_path))
        }
    }

    /// Open the stable main pathname without following its final symlink and
    /// verify that it still names this binding's file identity.  The probe is
    /// read-only and never creates database or companion files.
    pub fn validate_path_identity(&self) -> Result<()> {
        let file = open_identity_probe(&self.stable_path)?;
        self.validate_identity(FileIdentity::from_file(&file)?)
    }

    /// Complete reserved bootstrap by converting `use` to shared and then
    /// releasing `gate`.  The transition is idempotent.
    pub fn finish_bootstrap(&self) -> Result<()> {
        let mut lease = self
            .lease
            .lock()
            .map_err(|_| FrankenError::internal("namespace lease mutex poisoned"))?;
        if matches!(*lease, BindingLease::Shared { .. }) {
            return Ok(());
        }
        let old = std::mem::replace(&mut *lease, BindingLease::Transitioning);
        let (gate, use_file) = match old {
            BindingLease::BootstrapExclusive { gate, use_file } => (gate, use_file),
            other => {
                *lease = other;
                return Err(FrankenError::internal(
                    "namespace bootstrap transition re-entered",
                ));
            }
        };

        if let Err(error) = downgrade_to_shared(&use_file) {
            *lease = BindingLease::BootstrapExclusive { gate, use_file };
            return Err(error);
        }
        if let Err(error) = AdvisoryFileLock::unlock(&gate).map_err(lock_error) {
            *lease = BindingLease::BootstrapExclusive { gate, use_file };
            return Err(error);
        }
        *lease = BindingLease::Shared {
            _use_file: use_file,
        };
        Ok(())
    }

    /// Whether bootstrap still owns the namespace exclusively.
    #[must_use]
    pub fn bootstrap_is_exclusive(&self) -> bool {
        self.lease
            .lock()
            .is_ok_and(|lease| matches!(*lease, BindingLease::BootstrapExclusive { .. }))
    }
}

fn validate_stable_path(path: &Path) -> Result<()> {
    if !path.is_absolute() || path.file_name().is_none() {
        return Err(cannot_open(path));
    }
    Ok(())
}

fn sidecar_path(database_path: &Path, suffix: &str) -> PathBuf {
    let mut path: OsString = database_path.as_os_str().to_owned();
    path.push(suffix);
    PathBuf::from(path)
}

fn open_secure_lock_file(path: &Path) -> Result<File> {
    let file = match configured_open_options(true).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            configured_open_options(false)
                .open(path)
                .map_err(|_| cannot_open(path))?
        }
        Err(_) => return Err(cannot_open(path)),
    };
    validate_secure_lock_file(path, &file)?;
    Ok(file)
}

fn configured_open_options(create_new: bool) -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(create_new);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE,
        };
        options
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    options
}

/// Open an existing namespace lock for transient-candidate cleanup.
///
/// Windows cleanup must be able to unlink the two namespace records while the
/// exclusive lock handles are retained. This special-purpose open therefore
/// includes `FILE_SHARE_DELETE`; ordinary namespace opens deliberately keep
/// their stronger no-delete sharing policy.
fn cleanup_open_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true).write(true);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
        };
        options
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    options
}

fn open_cleanup_lock_file(path: &Path) -> Result<Option<File>> {
    let file = match cleanup_open_options().open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(cannot_open(path)),
    };
    validate_secure_lock_file(path, &file)?;
    Ok(Some(file))
}

fn existing_regular_cleanup_entry(database_path: &Path, path: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(true),
        Ok(_) => Err(cannot_open(database_path)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// Remove one abandoned caller-reserved transient database and its exact
/// namespace/recovery companions while holding the namespace exclusively.
///
/// This is **not** general database deletion. It exists only for private
/// `VACUUM` discard/rebuild candidates and failed caller-reserved outputs after
/// every pager/validation connection has closed. The parent directory is a
/// trusted cooperative namespace. Contention, a missing namespace record, a
/// generation-record mismatch, pathname identity drift, symlinks, or any
/// non-regular companion all fail closed without removing the main file.
///
/// The caller must retain the descriptor from which `expected_identity` was
/// derived until this function returns. `Ok(false)` means ownership could not
/// be proven and every entry was preserved.
pub fn cleanup_abandoned_private_database(
    database_path: &Path,
    expected_identity: FileIdentity,
) -> Result<bool> {
    validate_stable_path(database_path)?;
    let gate_path = sidecar_path(database_path, GATE_SUFFIX);
    let use_path = sidecar_path(database_path, USE_SUFFIX);
    let Some(gate) = open_cleanup_lock_file(&gate_path)? else {
        return Ok(false);
    };
    let Some(mut use_file) = open_cleanup_lock_file(&use_path)? else {
        return Ok(false);
    };

    match AdvisoryFileLock::try_lock(&gate, FileLockMode::Exclusive) {
        Ok(()) => {}
        Err(FileLockError::AlreadyLocked) => return Ok(false),
        Err(FileLockError::Io(error)) => return Err(error.into()),
    }
    match AdvisoryFileLock::try_lock(&use_file, FileLockMode::Exclusive) {
        Ok(()) => {}
        Err(FileLockError::AlreadyLocked) => return Ok(false),
        Err(FileLockError::Io(error)) => return Err(error.into()),
    }

    if read_identity_record(&mut use_file, database_path)? != expected_identity {
        return Ok(false);
    }
    let main_probe = match open_cleanup_identity_probe(database_path) {
        Ok(file) => file,
        Err(FrankenError::CannotOpen { .. }) => return Ok(false),
        Err(error) => return Err(error),
    };
    if FileIdentity::from_file(&main_probe)? != Some(expected_identity) {
        return Ok(false);
    }

    // Preflight the complete fixed companion set before removing anything.
    // Dynamic WAL segment cleanup is intentionally absent: transient VACUUM
    // candidates never enter WAL mode, and broad prefix deletion would violate
    // the exact-entry ownership boundary of this function.
    let companion_paths = [
        sidecar_path(database_path, "-journal"),
        sidecar_path(database_path, "-wal"),
        sidecar_path(database_path, "-wal-fec"),
        sidecar_path(database_path, "-wal-fec").with_extension("wal-fec.tmp"),
        sidecar_path(database_path, "-shm"),
        sidecar_path(database_path, "-lock-shared"),
        sidecar_path(database_path, "-lock-reserved"),
        sidecar_path(database_path, "-lock-pending"),
    ];
    let companion_exists = companion_paths
        .iter()
        .map(|path| existing_regular_cleanup_entry(database_path, path))
        .collect::<Result<Vec<_>>>()?;

    // Revalidate immediately before the first removal while both namespace
    // locks and the expected main descriptor are still live.
    let final_main_probe = match open_cleanup_identity_probe(database_path) {
        Ok(file) => file,
        Err(FrankenError::CannotOpen { .. }) => return Ok(false),
        Err(error) => return Err(error),
    };
    if FileIdentity::from_file(&final_main_probe)? != Some(expected_identity) {
        return Ok(false);
    }

    for (path, exists) in companion_paths.iter().zip(companion_exists) {
        if exists {
            std::fs::remove_file(path)?;
        }
    }
    std::fs::remove_file(database_path)?;
    std::fs::remove_file(&use_path)?;
    std::fs::remove_file(&gate_path)?;

    #[cfg(unix)]
    {
        let parent = database_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        File::open(parent)?.sync_all()?;
    }
    // Win32 has no portable directory fsync. The caller still invokes the
    // platform VFS namespace-sync hook, whose Windows contract is an explicit
    // no-op matching SQLite's own Windows VFS durability boundary.

    Ok(true)
}

fn open_identity_probe(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE,
        };
        options
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }

    let file = options.open(path).map_err(|_| cannot_open(path))?;
    let metadata = file.metadata().map_err(|_| cannot_open(path))?;
    if !metadata.is_file() {
        return Err(cannot_open(path));
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt as _;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(cannot_open(path));
        }
    }
    Ok(file)
}

fn open_cleanup_identity_probe(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
        };
        options
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }

    let file = options.open(path).map_err(|_| cannot_open(path))?;
    let metadata = file.metadata().map_err(|_| cannot_open(path))?;
    if !metadata.is_file() {
        return Err(cannot_open(path));
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt as _;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(cannot_open(path));
        }
    }
    Ok(file)
}

fn validate_secure_lock_file(path: &Path, file: &File) -> Result<()> {
    let metadata = file.metadata().map_err(|_| cannot_open(path))?;
    if !metadata.is_file() {
        return Err(cannot_open(path));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        // SAFETY: `geteuid` has no preconditions and does not dereference data.
        let effective_uid = unsafe { libc::geteuid() };
        if metadata.uid() != effective_uid || metadata.nlink() != 1 || metadata.mode() & 0o077 != 0
        {
            return Err(cannot_open(path));
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt as _;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(cannot_open(path));
        }
    }
    Ok(())
}

fn write_identity_record(file: &mut File, identity: FileIdentity) -> Result<()> {
    let mut record = [0_u8; RECORD_BYTES];
    record[..8].copy_from_slice(&RECORD_MAGIC);
    record[8] = RECORD_VERSION;
    record[9..9 + IDENTITY_BYTES].copy_from_slice(&identity.to_namespace_bytes());
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&record)?;
    file.flush()?;
    file.sync_data()?;
    Ok(())
}

fn read_identity_record(file: &mut File, database_path: &Path) -> Result<FileIdentity> {
    if file.metadata()?.len() != RECORD_BYTES as u64 {
        return Err(cannot_open(database_path));
    }
    let mut record = [0_u8; RECORD_BYTES];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut record)?;
    if record[..8] != RECORD_MAGIC
        || record[8] != RECORD_VERSION
        || record[9 + IDENTITY_BYTES..].iter().any(|byte| *byte != 0)
    {
        return Err(cannot_open(database_path));
    }
    let mut encoded = [0_u8; IDENTITY_BYTES];
    encoded.copy_from_slice(&record[9..9 + IDENTITY_BYTES]);
    FileIdentity::from_namespace_bytes(encoded).ok_or_else(|| cannot_open(database_path))
}

fn try_lock(file: &File, mode: FileLockMode) -> Result<()> {
    AdvisoryFileLock::try_lock(file, mode).map_err(lock_error)
}

fn lock_error(error: FileLockError) -> FrankenError {
    match error {
        FileLockError::AlreadyLocked => FrankenError::Busy,
        FileLockError::Io(error) => FrankenError::Io(error),
    }
}

#[cfg(unix)]
fn downgrade_to_shared(file: &File) -> Result<()> {
    // `flock(LOCK_SH)` atomically converts this open file description's
    // exclusive lock to shared.
    try_lock(file, FileLockMode::Shared)
}

#[cfg(windows)]
fn downgrade_to_shared(file: &File) -> Result<()> {
    // LockFileEx has no atomic conversion operation.  `gate` remains exclusive
    // around this call, so no cooperating opener can observe the short gap.
    AdvisoryFileLock::unlock(file).map_err(lock_error)?;
    try_lock(file, FileLockMode::Shared)
}

fn release_gate(gate: File) -> Result<()> {
    AdvisoryFileLock::unlock(&gate).map_err(lock_error)?;
    drop(gate);
    Ok(())
}

fn cannot_open(path: &Path) -> FrankenError {
    FrankenError::CannotOpen {
        path: path.to_owned(),
    }
}

/// Windows advisory-lock sidecar policy for reserved-empty validation.
///
/// The post-main-open check permits the three sidecars that opening a Windows
/// VFS handle necessarily creates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowsLockSidecarPolicy {
    /// Reject every advisory-lock sidecar before the main handle is opened.
    RejectAll,
    /// Allow only the sidecars created by the accepted main-file handle.
    AllowExpected,
}

/// Validate that no recovery artifact belongs to a caller-reserved empty DB.
/// This function performs reads only and never creates or removes entries.
pub fn validate_reserved_database_artifacts(
    database_path: &Path,
    windows_lock_sidecars: WindowsLockSidecarPolicy,
) -> Result<()> {
    validate_stable_path(database_path)?;
    for suffix in ["-journal", "-wal", "-wal-fec", "-shm"] {
        reject_existing_entry(database_path, &sidecar_path(database_path, suffix))?;
    }

    #[cfg(windows)]
    if windows_lock_sidecars == WindowsLockSidecarPolicy::RejectAll {
        for suffix in ["-lock-shared", "-lock-reserved", "-lock-pending"] {
            reject_existing_entry(database_path, &sidecar_path(database_path, suffix))?;
        }
    }
    #[cfg(not(windows))]
    let _ = windows_lock_sidecars;

    let wal_fec_temp = sidecar_path(database_path, "-wal-fec").with_extension("wal-fec.tmp");
    reject_existing_entry(database_path, &wal_fec_temp)?;

    let parent = database_path
        .parent()
        .ok_or_else(|| cannot_open(database_path))?;
    let db_name = database_path
        .file_name()
        .ok_or_else(|| cannot_open(database_path))?
        .to_string_lossy();
    let segment_prefix = format!("{db_name}-wal-seg-");
    for entry in std::fs::read_dir(parent)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(&segment_prefix)
        {
            return Err(cannot_open(database_path));
        }
    }
    Ok(())
}

fn reject_existing_entry(database_path: &Path, candidate: &Path) -> Result<()> {
    match std::fs::symlink_metadata(candidate) {
        Ok(_) => Err(cannot_open(database_path)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    fn create_database(path: &Path, bytes: &[u8]) -> FileIdentity {
        fs::write(path, bytes).expect("create test database");
        let file = File::open(path).expect("open test database");
        FileIdentity::from_file(&file)
            .expect("query test database identity")
            .expect("native filesystem identity")
    }

    #[test]
    fn new_generation_stays_exclusive_until_bootstrap_finishes() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("bootstrap.db");
        let identity = create_database(&database, b"");

        let pending = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("admit new generation");
        assert_eq!(pending.expected_identity(), None);
        let binding = pending.bind(identity).expect("bind new generation");
        assert!(binding.bootstrap_is_exclusive());
        assert!(matches!(
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared),
            Err(FrankenError::Busy)
        ));

        binding.finish_bootstrap().expect("finish bootstrap");
        assert!(!binding.bootstrap_is_exclusive());
        binding
            .finish_bootstrap()
            .expect("finishing bootstrap twice is harmless");

        let join = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("join live generation");
        assert_eq!(join.expected_identity(), Some(identity));
        let peer = join.bind(identity).expect("bind peer");
        assert!(!peer.bootstrap_is_exclusive());
    }

    #[test]
    fn live_generation_rejects_replacement_identity_then_allows_new_generation() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("replace.db");
        let displaced = dir.path().join("replace.displaced.db");
        let first_identity = create_database(&database, b"first");
        let first = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("admit first")
            .bind(first_identity)
            .expect("bind first");
        first.finish_bootstrap().expect("finish first bootstrap");

        fs::rename(&database, &displaced).expect("displace main path");
        let replacement_identity = create_database(&database, b"replacement");
        assert!(matches!(
            first.validate_path_identity(),
            Err(FrankenError::CannotOpen { .. })
        ));

        let stale_join = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("admission reads live record");
        assert_eq!(stale_join.expected_identity(), Some(first_identity));
        assert!(matches!(
            stale_join.bind(replacement_identity),
            Err(FrankenError::CannotOpen { .. })
        ));

        drop(first);
        let replacement = PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("admit replacement generation");
        assert_eq!(replacement.expected_identity(), None);
        let replacement = replacement
            .bind(replacement_identity)
            .expect("bind replacement generation");
        replacement
            .finish_bootstrap()
            .expect("finish replacement bootstrap");
        replacement
            .validate_path_identity()
            .expect("replacement remains bound");
    }

    #[test]
    fn reserved_bootstrap_and_pending_drop_are_raii_exclusive() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("reserved.db");
        let identity = create_database(&database, b"");

        let abandoned =
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::ReservedExclusive)
                .expect("reserve namespace");
        drop(abandoned);

        let reserved =
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::ReservedExclusive)
                .expect("reserve after unwind")
                .bind(identity)
                .expect("bind reservation");
        assert!(matches!(
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared),
            Err(FrankenError::Busy)
        ));
        reserved.finish_bootstrap().expect("finish reservation");
        PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared)
            .expect("shared admission after reservation")
            .bind(identity)
            .expect("join reserved generation");

        assert!(sidecar_path(&database, GATE_SUFFIX).exists());
        assert!(sidecar_path(&database, USE_SUFFIX).exists());
    }

    #[test]
    fn abandoned_private_cleanup_requires_exclusive_namespace_and_removes_exact_artifacts() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("transient.db");
        let identity = create_database(&database, b"candidate");
        let binding =
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::ReservedExclusive)
                .expect("reserve namespace")
                .bind(identity)
                .expect("bind reservation");
        binding.finish_bootstrap().expect("finish bootstrap");
        for suffix in [
            "-journal",
            "-wal",
            "-wal-fec",
            "-shm",
            "-lock-shared",
            "-lock-reserved",
            "-lock-pending",
        ] {
            fs::write(sidecar_path(&database, suffix), b"candidate artifact")
                .expect("seed exact candidate companion");
        }
        let wal_fec_temp = sidecar_path(&database, "-wal-fec").with_extension("wal-fec.tmp");
        fs::write(&wal_fec_temp, b"candidate rewrite artifact")
            .expect("seed exact WAL-FEC rewrite companion");

        assert!(
            !cleanup_abandoned_private_database(&database, identity)
                .expect("contention must fail closed"),
            "a live namespace binding must prevent transient cleanup"
        );
        assert!(database.exists());
        drop(binding);

        assert!(
            cleanup_abandoned_private_database(&database, identity)
                .expect("exclusive abandoned-candidate cleanup")
        );
        assert!(!database.exists());
        for suffix in [
            "-journal",
            "-wal",
            "-wal-fec",
            "-shm",
            "-lock-shared",
            "-lock-reserved",
            "-lock-pending",
            GATE_SUFFIX,
            USE_SUFFIX,
        ] {
            assert!(
                !sidecar_path(&database, suffix).exists(),
                "cleanup left exact companion {suffix}"
            );
        }
        assert!(!wal_fec_temp.exists());
    }

    #[test]
    fn abandoned_private_cleanup_preserves_replacement_and_namespace_on_identity_drift() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("drift.db");
        let displaced = dir.path().join("drift-owned.db");
        let identity = create_database(&database, b"owned candidate");
        let binding =
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::ReservedExclusive)
                .expect("reserve namespace")
                .bind(identity)
                .expect("bind reservation");
        binding.finish_bootstrap().expect("finish bootstrap");
        drop(binding);

        fs::rename(&database, &displaced).expect("displace owned candidate");
        fs::write(&database, b"replacement").expect("seed replacement");
        assert!(
            !cleanup_abandoned_private_database(&database, identity)
                .expect("identity drift must fail closed")
        );
        assert_eq!(
            fs::read(&database).expect("read replacement"),
            b"replacement"
        );
        assert_eq!(
            fs::read(&displaced).expect("read owned candidate"),
            b"owned candidate"
        );
        assert!(sidecar_path(&database, GATE_SUFFIX).exists());
        assert!(sidecar_path(&database, USE_SUFFIX).exists());
    }

    #[test]
    fn artifact_validation_rejects_segments_and_wal_fec_rewrite_temp() {
        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("artifacts.db");
        create_database(&database, b"");
        validate_reserved_database_artifacts(&database, WindowsLockSidecarPolicy::RejectAll)
            .expect("artifact-free reservation");

        fs::write(
            dir.path().join("artifacts.db-wal-seg-not-an-epoch"),
            b"segment",
        )
        .expect("seed segment");
        assert!(matches!(
            validate_reserved_database_artifacts(&database, WindowsLockSidecarPolicy::RejectAll),
            Err(FrankenError::CannotOpen { .. })
        ));

        let second = dir.path().join("rewrite.db");
        create_database(&second, b"");
        let temp = sidecar_path(&second, "-wal-fec").with_extension("wal-fec.tmp");
        fs::write(temp, b"partial rewrite").expect("seed WAL-FEC rewrite temp");
        assert!(matches!(
            validate_reserved_database_artifacts(&second, WindowsLockSidecarPolicy::RejectAll),
            Err(FrankenError::CannotOpen { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn namespace_lockfile_symlink_is_rejected_without_following_it() {
        use std::os::unix::fs::symlink;

        let dir = tempdir().expect("tempdir");
        let database = dir.path().join("nofollow.db");
        create_database(&database, b"");
        let target = dir.path().join("attacker-target");
        fs::write(&target, b"unchanged").expect("seed target");
        symlink(&target, sidecar_path(&database, GATE_SUFFIX)).expect("seed malicious symlink");

        assert!(matches!(
            PendingNamespaceOpen::begin(&database, NamespaceOpenIntent::Shared),
            Err(FrankenError::CannotOpen { .. })
        ));
        assert_eq!(fs::read(target).expect("read target"), b"unchanged");
    }
}
