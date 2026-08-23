//! Windows VFS implementation.
//!
//! This backend provides the same `Vfs` / `VfsFile` surface as `UnixVfs`,
//! using Windows-friendly file APIs and lock sidecars backed by OS advisory
//! locks (`LockFileEx` via `advisory-lock`) that mirror SQLite lock-level
//! transitions (`NONE` → `SHARED` → `RESERVED` → `PENDING` → `EXCLUSIVE`).

use std::collections::HashMap;
use std::env;
use std::ffi::{OsString, c_void};
use std::fs::{self, File, OpenOptions};
use std::os::windows::fs::{FileExt, OpenOptionsExt};
use std::os::windows::io::AsRawHandle as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering, fence};
use std::sync::{Arc, Mutex, OnceLock};

use advisory_lock::{AdvisoryFileLock, FileLockError, FileLockMode};
use asupersync::runtime::spawn_blocking_io;
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::LockLevel;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};
use tracing::{debug, info, warn};

use crate::shm::{
    SQLITE_SHM_EXCLUSIVE, SQLITE_SHM_LOCK, SQLITE_SHM_SHARED, SQLITE_SHM_UNLOCK, ShmRegion,
    WAL_CKPT_LOCK, WAL_TOTAL_LOCKS, WAL_WRITE_LOCK,
};
use crate::traits::{FileIdentity, Vfs, VfsFile, VfsWriteCompletion, VfsWriteCompletionSource};

/// SQLite I/O capability bit indicating files cannot be deleted while open.
const SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN: u32 = 0x0000_0800;
const WINDOWS_FILE_SHARE_READ: u32 = 0x0000_0001;
const WINDOWS_FILE_SHARE_WRITE: u32 = 0x0000_0002;
const WINDOWS_FILE_SHARE_DELETE: u32 = 0x0000_0004;
// bd-h5oaj / GH#355: stock SQLite's winOpen always includes
// FILE_SHARE_DELETE alongside READ|WRITE. Omitting it here was a
// deliberate deviation to close a preflight-to-final-open TOCTOU window,
// but the reserved-builder path holds two overlapping handles on the same
// path (fsqlite-core's `DatabaseBuilderReservation` retains the first for
// the reservation's whole lifetime while the pager opens a second), and an
// external delete-access opener (AV real-time scanning, indexing, backup
// software commonly requests DELETE access even for a read) racing either
// handle can hit NTFS's mandatory share-mode arbitration and fail with a
// sharing violation -- observed as a flaky (not deterministic)
// `CannotOpen`/`store.disk` refusal, Windows-only. FILE_SHARE_DELETE does
// NOT make the file deletable-while-open in the POSIX sense: Windows still
// enters "delete pending" on a `DeleteFile`/`FILE_DISPOSITION_INFO` call
// against an open handle, which blocks reopening the same name until the
// last handle closes -- exactly what `SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN`
// (below) already declares to callers, so that contract is unaffected.
// The TOCTOU the omission guarded is already covered independently by the
// `FileIdentity` re-checks after every final open (see
// `open_with_expected_identity` / `open_reserved_with_expected_identity`
// below and `pager.rs`'s post-open identity verification), so matching
// stock SQLite's share mode here is safe.
const WINDOWS_SHARE_READ_WRITE_DELETE: u32 =
    WINDOWS_FILE_SHARE_READ | WINDOWS_FILE_SHARE_WRITE | WINDOWS_FILE_SHARE_DELETE;

// Stock SQLite's Windows VFS coordinates main-database access through these
// byte ranges on the *database file itself*. They intentionally match the
// constants in SQLite's os_win.c and the Unix VFS in this crate.
const STOCK_SQLITE_PENDING_BYTE: u64 = 0x4000_0000;
const STOCK_SQLITE_RESERVED_BYTE: u64 = STOCK_SQLITE_PENDING_BYTE + 1;
const STOCK_SQLITE_SHARED_FIRST: u64 = STOCK_SQLITE_PENDING_BYTE + 2;
const STOCK_SQLITE_SHARED_SIZE: u64 = 510;

// SQLite's Windows WAL VFS places the eight WAL lock bytes at offsets 120..128
// of the real `-shm` file. WAL_WRITE_LOCK and WAL_CKPT_LOCK are slots 0 and 1.
const STOCK_SQLITE_SHM_LOCK_BASE: u64 = 120;
#[cfg(test)]
const STOCK_SQLITE_WAL_WRITE_BYTE: u64 = STOCK_SQLITE_SHM_LOCK_BASE;
#[cfg(test)]
const STOCK_SQLITE_WAL_CKPT_BYTE: u64 = STOCK_SQLITE_SHM_LOCK_BASE + 1;

const LOCKFILE_FAIL_IMMEDIATELY: u32 = 0x0000_0001;
const LOCKFILE_EXCLUSIVE_LOCK: u32 = 0x0000_0002;
const ERROR_LOCK_VIOLATION: i32 = 33;
const ERROR_NOT_LOCKED: i32 = 158;

fn blocking_io_offset(offset: u64, total: usize, op: &'static str) -> std::io::Result<u64> {
    let total = u64::try_from(total).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "I/O offset is too large")
    })?;
    offset.checked_add(total).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("offset overflow during async windows vfs {op}"),
        )
    })
}

fn read_owned_at(file: &File, len: usize, offset: u64) -> std::io::Result<(Vec<u8>, usize)> {
    let mut data = vec![0_u8; len];
    let mut total = 0_usize;
    while total < data.len() {
        let current = blocking_io_offset(offset, total, "read")?;
        match file.seek_read(&mut data[total..], current) {
            Ok(0) => break,
            Ok(read) => total += read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok((data, total))
}

fn write_owned_at(file: &File, data: &[u8], offset: u64) -> std::io::Result<()> {
    let mut total = 0_usize;
    while total < data.len() {
        let current = blocking_io_offset(offset, total, "write")?;
        match file.seek_write(&data[total..], current) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "async windows vfs seek_write returned 0",
                ));
            }
            Ok(written) => total += written,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn write_owned_at_tracked(
    file: &File,
    data: &[u8],
    offset: u64,
    mut completion: VfsWriteCompletionSource,
) -> std::io::Result<()> {
    let result = write_owned_at(file, data, offset);
    if result.is_ok() {
        completion.complete_success();
    } else {
        completion.complete_error();
    }
    result
}

/// Layout-compatible subset of Win32 `OVERLAPPED` used for byte-range locks.
///
/// `windows-sys` gates `LockFileEx` behind an additional feature that this
/// crate does not otherwise need, so the two small FFI calls live here at the
/// platform boundary. The anonymous union in the SDK is represented by its
/// two 32-bit offset fields; that has the same layout on 32-bit and 64-bit
/// Windows.
#[repr(C)]
struct WindowsOverlapped {
    internal: usize,
    internal_high: usize,
    offset: u32,
    offset_high: u32,
    event: *mut c_void,
}

impl WindowsOverlapped {
    fn at(offset: u64) -> Self {
        Self {
            internal: 0,
            internal_high: 0,
            offset: offset as u32,
            offset_high: (offset >> 32) as u32,
            event: std::ptr::null_mut(),
        }
    }
}

#[link(name = "kernel32")]
unsafe extern "system" {
    #[link_name = "LockFileEx"]
    fn windows_lock_file_ex(
        file: *mut c_void,
        flags: u32,
        reserved: u32,
        bytes_low: u32,
        bytes_high: u32,
        overlapped: *mut WindowsOverlapped,
    ) -> i32;

    #[link_name = "UnlockFileEx"]
    fn windows_unlock_file_ex(
        file: *mut c_void,
        reserved: u32,
        bytes_low: u32,
        bytes_high: u32,
        overlapped: *mut WindowsOverlapped,
    ) -> i32;
}

fn split_u64(value: u64) -> (u32, u32) {
    (value as u32, (value >> 32) as u32)
}

#[derive(Clone, Copy, Debug)]
enum WindowsRangeLockMode {
    Shared,
    Exclusive,
}

fn try_lock_stock_sqlite_range_with_mode(
    file: &File,
    offset: u64,
    len: u64,
    mode: WindowsRangeLockMode,
) -> Result<()> {
    if len == 0 {
        return Err(FrankenError::internal(
            "cannot acquire a zero-length Windows byte-range lock",
        ));
    }
    let (bytes_low, bytes_high) = split_u64(len);
    let mut overlapped = WindowsOverlapped::at(offset);
    // SAFETY: `file` owns a live Windows handle, `overlapped` is layout-
    // compatible with Win32 OVERLAPPED and remains live for the synchronous
    // nonblocking call, and the requested range is non-empty.
    let flags = LOCKFILE_FAIL_IMMEDIATELY
        | match mode {
            WindowsRangeLockMode::Shared => 0,
            WindowsRangeLockMode::Exclusive => LOCKFILE_EXCLUSIVE_LOCK,
        };
    let locked = unsafe {
        windows_lock_file_ex(
            file.as_raw_handle().cast(),
            flags,
            0,
            bytes_low,
            bytes_high,
            &raw mut overlapped,
        )
    };
    if locked != 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(ERROR_LOCK_VIOLATION) {
        Err(FrankenError::Busy)
    } else {
        Err(FrankenError::Io(error))
    }
}

fn try_lock_stock_sqlite_range(file: &File, offset: u64, len: u64) -> Result<()> {
    try_lock_stock_sqlite_range_with_mode(file, offset, len, WindowsRangeLockMode::Exclusive)
}

fn try_lock_stock_sqlite_shared_range(file: &File, offset: u64, len: u64) -> Result<()> {
    try_lock_stock_sqlite_range_with_mode(file, offset, len, WindowsRangeLockMode::Shared)
}

fn unlock_stock_sqlite_range(file: &File, offset: u64, len: u64) -> Result<()> {
    unlock_stock_sqlite_range_impl(file, offset, len, true)
}

fn unlock_stock_sqlite_range_strict(file: &File, offset: u64, len: u64) -> Result<()> {
    unlock_stock_sqlite_range_impl(file, offset, len, false)
}

fn unlock_stock_sqlite_range_impl(
    file: &File,
    offset: u64,
    len: u64,
    missing_is_unlocked: bool,
) -> Result<()> {
    let (bytes_low, bytes_high) = split_u64(len);
    let mut overlapped = WindowsOverlapped::at(offset);
    // SAFETY: the handle and OVERLAPPED invariants are the same as in
    // `try_lock_stock_sqlite_range`; this exact range was previously locked
    // through the same handle.
    let unlocked = unsafe {
        windows_unlock_file_ex(
            file.as_raw_handle().cast(),
            0,
            bytes_low,
            bytes_high,
            &raw mut overlapped,
        )
    };
    if unlocked != 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if missing_is_unlocked && error.raw_os_error() == Some(ERROR_NOT_LOCKED) {
        Ok(())
    } else {
        Err(FrankenError::Io(error))
    }
}

fn restore_missing_stock_sqlite_range_fence(
    file: &File,
    offset: u64,
    len: u64,
    mode: WindowsRangeLockMode,
    operation: &str,
    unlock_error: &FrankenError,
) -> Option<String> {
    let FrankenError::Io(error) = unlock_error else {
        return None;
    };
    if error.raw_os_error() != Some(ERROR_NOT_LOCKED) {
        return None;
    }

    let restore_result = try_lock_stock_sqlite_range_with_mode(file, offset, len, mode);
    Some(format!(
        "{operation} observed ERROR_NOT_LOCKED for stock SQLite range offset={offset} len={len}; attempted {mode:?} re-fence: {restore_result:?}"
    ))
}

fn checkpoint_or_abort(cx: &Cx) -> Result<()> {
    cx.checkpoint().map_err(|_| FrankenError::Abort)
}

fn lock_poisoned(name: &str) -> FrankenError {
    FrankenError::internal(format!("{name} lock poisoned"))
}

fn windows_open_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.share_mode(WINDOWS_SHARE_READ_WRITE_DELETE);
    options
}

fn resolve_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(env::current_dir()?.join(path))
    }
}

fn stable_full_path(path: &Path) -> Result<PathBuf> {
    let absolute = resolve_path(path)?;

    match absolute.canonicalize() {
        Ok(canonical) => Ok(canonical),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = absolute.parent().ok_or_else(|| FrankenError::CannotOpen {
                path: absolute.clone(),
            })?;
            let file_name = absolute
                .file_name()
                .ok_or_else(|| FrankenError::CannotOpen {
                    path: absolute.clone(),
                })?;
            let canonical_parent = parent.canonicalize()?;
            Ok(canonical_parent.join(file_name))
        }
        Err(error) => Err(FrankenError::Io(error)),
    }
}

fn sqlite_shm_path(path: &Path) -> PathBuf {
    let mut shm: OsString = path.as_os_str().to_owned();
    shm.push("-shm");
    PathBuf::from(shm)
}

fn sqlite_companion_path(path: &Path, suffix: &str) -> PathBuf {
    let mut companion: OsString = path.as_os_str().to_owned();
    companion.push(suffix);
    PathBuf::from(companion)
}

fn sqlite_shared_lock_path(path: &Path) -> PathBuf {
    let mut p: OsString = path.as_os_str().to_owned();
    p.push("-lock-shared");
    PathBuf::from(p)
}

fn sqlite_reserved_lock_path(path: &Path) -> PathBuf {
    let mut p: OsString = path.as_os_str().to_owned();
    p.push("-lock-reserved");
    PathBuf::from(p)
}

fn sqlite_pending_lock_path(path: &Path) -> PathBuf {
    let mut p: OsString = path.as_os_str().to_owned();
    p.push("-lock-pending");
    PathBuf::from(p)
}

// The three advisory-lock sidecars `WindowsOsLockFiles::open` writes next to
// every DB it touches. Returned as an array so callers can iterate uniformly.
fn windows_lock_sidecar_paths(path: &Path) -> [PathBuf; 3] {
    [
        sqlite_shared_lock_path(path),
        sqlite_reserved_lock_path(path),
        sqlite_pending_lock_path(path),
    ]
}

fn reserved_database_artifact_paths(path: &Path) -> [PathBuf; 7] {
    let [shared, reserved, pending] = windows_lock_sidecar_paths(path);
    [
        sqlite_companion_path(path, "-journal"),
        sqlite_companion_path(path, "-wal"),
        sqlite_companion_path(path, "-wal-fec"),
        sqlite_shm_path(path),
        shared,
        reserved,
        pending,
    ]
}

fn filesystem_entry_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(FrankenError::Io(err)),
    }
}

// Best-effort removal of the three advisory-lock sidecars alongside `path`.
// Errors are intentionally swallowed: sidecars are advisory and may be missing,
// in use by a racing handle, or already cleaned up. Without this, every
// transient DB file (e.g. VACUUM INTO backups) leaks three zero-byte files,
// and a downstream caller that re-enumerates the dir can mistake an orphan
// sidecar for a backup root and chain a fresh set on top.
fn try_remove_windows_lock_sidecars(path: &Path) {
    for sidecar in windows_lock_sidecar_paths(path) {
        let _ = fs::remove_file(sidecar);
    }
}

fn ensure_shm_file_len(path: &Path, min_len: u64) -> Result<()> {
    let mut options = windows_open_options();
    let file = options
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    let current = file.metadata()?.len();
    if current < min_len {
        file.set_len(min_len)?;
    }
    Ok(())
}

fn open_windows_lock_sidecar(path: &Path) -> Result<(File, bool)> {
    loop {
        let mut create_options = windows_open_options();
        match create_options
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
        {
            Ok(file) => return Ok((file, true)),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                let mut open_options = windows_open_options();
                match open_options.read(true).write(true).open(path) {
                    Ok(file) => return Ok((file, false)),
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => return Err(FrankenError::Io(err)),
                }
            }
            Err(err) => return Err(FrankenError::Io(err)),
        }
    }
}

#[derive(Debug, Default)]
struct WindowsVfsInner {
    next_temp_id: u64,
}

/// Windows filesystem-backed VFS implementation.
#[derive(Debug, Clone, Default)]
pub struct WindowsVfs {
    inner: Arc<Mutex<WindowsVfsInner>>,
}

impl WindowsVfs {
    /// Create a new Windows VFS instance.
    #[must_use]
    pub fn new() -> Self {
        info!(
            target: "fsqlite_vfs::windows",
            sector_size = 4096_u32,
            "windows vfs initialized"
        );
        Self::default()
    }

    fn next_temp_path(&self) -> Result<PathBuf> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| lock_poisoned("windows vfs inner"))?;
        let id = inner.next_temp_id.max(next_temp_id());
        inner.next_temp_id = id
            .checked_add(1)
            .ok_or_else(|| FrankenError::internal("temp file id overflow"))?;
        Ok(env::temp_dir().join(format!("fsqlite-windows-{id}.tmp")))
    }
}

#[derive(Debug, Clone, Default)]
struct ShmSlotState {
    shared_holders: HashMap<u64, u32>,
    exclusive_owner: Option<u64>,
}

#[derive(Debug)]
struct WindowsShmState {
    regions: HashMap<u32, ShmRegion>,
    slots: Vec<ShmSlotState>,
    owner_refs: HashMap<u64, u32>,
    stock_shm_file: Option<File>,
    poisoned: Option<String>,
}

impl Default for WindowsShmState {
    fn default() -> Self {
        let slot_count = usize::try_from(WAL_TOTAL_LOCKS).expect("WAL_TOTAL_LOCKS must fit usize");
        Self {
            regions: HashMap::new(),
            slots: vec![ShmSlotState::default(); slot_count],
            owner_refs: HashMap::new(),
            stock_shm_file: None,
            poisoned: None,
        }
    }
}

#[derive(Debug)]
struct WindowsShmTable {
    map: Mutex<HashMap<PathBuf, Arc<Mutex<WindowsShmState>>>>,
}

impl WindowsShmTable {
    fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
        }
    }

    #[cfg(test)]
    fn get(&self, path: &Path) -> Result<Option<Arc<Mutex<WindowsShmState>>>> {
        let map = self
            .map
            .lock()
            .map_err(|_| lock_poisoned("windows shm table"))?;
        Ok(map.get(path).map(Arc::clone))
    }

    fn get_existing_and_register_with<T>(
        &self,
        path: &Path,
        owner_id: u64,
        before_register: impl FnOnce(),
        inspect: impl FnOnce(&WindowsShmState) -> Result<T>,
    ) -> Result<Option<(Arc<Mutex<WindowsShmState>>, T)>> {
        let map = self
            .map
            .lock()
            .map_err(|_| lock_poisoned("windows shm table"))?;
        let Some(state) = map.get(path).map(Arc::clone) else {
            return Ok(None);
        };

        // Match get_or_create_and_register's map -> state lock order and keep
        // the owner registration in the same table critical section.  The
        // non-extending xShmMap path used to clone the state, release `map`,
        // and only then increment owner_refs.  A concurrent last close could
        // remove the table entry in that gap and split later handles across
        // two independent lock domains.
        before_register();
        let inspected = {
            let mut guard = state
                .lock()
                .map_err(|_| lock_poisoned("windows shm state"))?;
            let inspected = inspect(&guard)?;
            *guard.owner_refs.entry(owner_id).or_insert(0) += 1;
            inspected
        };
        drop(map);
        Ok(Some((state, inspected)))
    }

    fn get_or_create_and_register(
        &self,
        path: &Path,
        owner_id: u64,
    ) -> Result<Arc<Mutex<WindowsShmState>>> {
        self.get_or_create_and_register_with(path, owner_id, || {})
    }

    fn get_or_create_and_register_with(
        &self,
        path: &Path,
        owner_id: u64,
        before_register: impl FnOnce(),
    ) -> Result<Arc<Mutex<WindowsShmState>>> {
        let mut map = self
            .map
            .lock()
            .map_err(|_| lock_poisoned("windows shm table"))?;
        let state = Arc::clone(
            map.entry(path.to_path_buf())
                .or_insert_with(|| Arc::new(Mutex::new(WindowsShmState::default()))),
        );

        // Keep registration inside the table critical section. Otherwise the
        // last old owner could remove the entry after a new opener cloned it
        // but before that opener made its ownership visible, leaving the new
        // handle on a detached SHM lock domain.
        before_register();
        {
            let mut guard = state
                .lock()
                .map_err(|_| lock_poisoned("windows shm state"))?;
            *guard.owner_refs.entry(owner_id).or_insert(0) += 1;
        }
        drop(map);
        Ok(state)
    }

    fn remove_if_orphaned(
        &self,
        path: &Path,
        expected: &Arc<Mutex<WindowsShmState>>,
    ) -> Result<()> {
        let mut map = self
            .map
            .lock()
            .map_err(|_| lock_poisoned("windows shm table"))?;
        if let Some(state) = map.get(path) {
            if !Arc::ptr_eq(state, expected) {
                return Ok(());
            }
            let orphaned = state
                .lock()
                .map_err(|_| lock_poisoned("windows shm state"))?
                .owner_refs
                .is_empty();
            if orphaned {
                map.remove(path);
            }
        }
        Ok(())
    }
}

fn windows_shm_table() -> &'static WindowsShmTable {
    static TABLE: OnceLock<WindowsShmTable> = OnceLock::new();
    TABLE.get_or_init(WindowsShmTable::new)
}

fn next_owner_id() -> u64 {
    static OWNER_SEQ: AtomicU64 = AtomicU64::new(1);
    OWNER_SEQ.fetch_add(1, Ordering::Relaxed)
}

fn next_temp_id() -> u64 {
    static TEMP_SEQ: AtomicU64 = AtomicU64::new(1);
    TEMP_SEQ.fetch_add(1, Ordering::Relaxed)
}

fn to_slot_index(slot: u32) -> Result<usize> {
    usize::try_from(slot).map_err(|_| FrankenError::OutOfRange {
        what: "shm slot index".to_string(),
        value: slot.to_string(),
    })
}

fn next_lock_level(level: LockLevel) -> Option<LockLevel> {
    match level {
        LockLevel::None => Some(LockLevel::Shared),
        LockLevel::Shared => Some(LockLevel::Reserved),
        LockLevel::Reserved => Some(LockLevel::Pending),
        LockLevel::Pending => Some(LockLevel::Exclusive),
        LockLevel::Exclusive => None,
    }
}

fn lock_level_slot(level: LockLevel) -> Option<usize> {
    match level {
        LockLevel::None => None,
        LockLevel::Shared => Some(0),
        LockLevel::Reserved => Some(1),
        LockLevel::Pending => Some(2),
        LockLevel::Exclusive => Some(3),
    }
}

#[derive(Debug)]
struct WindowsOsLockFiles {
    shared_file: File,
    reserved_file: File,
    pending_file: File,
    held_levels: [bool; 4],
    #[cfg(test)]
    fail_next_unlock: bool,
}

impl WindowsOsLockFiles {
    fn open(path: &Path) -> Result<Self> {
        let shared_path = sqlite_shared_lock_path(path);
        let reserved_path = sqlite_reserved_lock_path(path);
        let pending_path = sqlite_pending_lock_path(path);
        let (shared_file, shared_created) = open_windows_lock_sidecar(&shared_path)?;
        let (reserved_file, reserved_created) = match open_windows_lock_sidecar(&reserved_path) {
            Ok(opened) => opened,
            Err(err) => {
                drop(shared_file);
                if shared_created {
                    let _ = fs::remove_file(&shared_path);
                }
                return Err(err);
            }
        };
        let (pending_file, _) = match open_windows_lock_sidecar(&pending_path) {
            Ok(opened) => opened,
            Err(err) => {
                drop(reserved_file);
                drop(shared_file);
                if reserved_created {
                    let _ = fs::remove_file(&reserved_path);
                }
                if shared_created {
                    let _ = fs::remove_file(&shared_path);
                }
                return Err(err);
            }
        };
        Ok(Self {
            shared_file,
            reserved_file,
            pending_file,
            held_levels: [false; 4],
            #[cfg(test)]
            fail_next_unlock: false,
        })
    }

    fn try_lock_shared(file: &File) -> Result<()> {
        match AdvisoryFileLock::try_lock(file, FileLockMode::Shared) {
            Ok(()) => Ok(()),
            Err(FileLockError::AlreadyLocked) => Err(FrankenError::Busy),
            Err(FileLockError::Io(err)) => Err(FrankenError::Io(err)),
        }
    }

    fn try_lock_exclusive(file: &File) -> Result<()> {
        match AdvisoryFileLock::try_lock(file, FileLockMode::Exclusive) {
            Ok(()) => Ok(()),
            Err(FileLockError::AlreadyLocked) => Err(FrankenError::Busy),
            Err(FileLockError::Io(err)) => Err(FrankenError::Io(err)),
        }
    }

    fn unlock_file(file: &File) -> Result<()> {
        match AdvisoryFileLock::unlock(file) {
            Ok(()) => Ok(()),
            Err(FileLockError::AlreadyLocked) => Err(FrankenError::LockFailed {
                detail: "unlock called for contended lock".to_string(),
            }),
            Err(FileLockError::Io(err)) => Err(FrankenError::Io(err)),
        }
    }

    fn lock_file_for_level(&self, level: LockLevel) -> Option<&File> {
        match level {
            LockLevel::None => None,
            LockLevel::Shared | LockLevel::Exclusive => Some(&self.shared_file),
            LockLevel::Reserved => Some(&self.reserved_file),
            LockLevel::Pending => Some(&self.pending_file),
        }
    }

    fn lock_held(&self, level: LockLevel) -> bool {
        lock_level_slot(level).is_some_and(|slot| self.held_levels[slot])
    }

    fn set_lock_held(&mut self, level: LockLevel, held: bool) {
        if let Some(slot) = lock_level_slot(level) {
            self.held_levels[slot] = held;
        }
    }

    fn try_lock_level(&mut self, level: LockLevel) -> Result<()> {
        if level == LockLevel::None {
            return Ok(());
        }

        if self.lock_held(level) {
            return Ok(());
        }

        if level == LockLevel::Shared {
            // Match SQLite's pending-byte protocol: readers briefly take a
            // shared lock on the pending sidecar before acquiring the shared
            // range. A pending writer holds this sidecar exclusively, blocking
            // new readers while existing readers drain.
            Self::try_lock_shared(&self.pending_file)?;
            let shared_result = Self::try_lock_shared(&self.shared_file);
            let pending_unlock = Self::unlock_file(&self.pending_file);
            if let Err(err) = shared_result {
                pending_unlock?;
                return Err(err);
            }
            if let Err(err) = pending_unlock {
                let _ = Self::unlock_file(&self.shared_file);
                return Err(err);
            }
            self.set_lock_held(LockLevel::Shared, true);
            return Ok(());
        }

        if level == LockLevel::Exclusive {
            // EXCLUSIVE conflicts with SHARED by upgrading the same shared
            // sidecar from shared to exclusive. Locking a separate
            // "exclusive" sidecar would only exclude other writers and would
            // allow readers through.
            let had_shared = self.lock_held(LockLevel::Shared);
            if had_shared {
                Self::unlock_file(&self.shared_file)?;
                self.set_lock_held(LockLevel::Shared, false);
            }
            if let Err(err) = Self::try_lock_exclusive(&self.shared_file) {
                if had_shared && Self::try_lock_shared(&self.shared_file).is_ok() {
                    self.set_lock_held(LockLevel::Shared, true);
                }
                return Err(err);
            }
            self.set_lock_held(LockLevel::Exclusive, true);
            return Ok(());
        }

        let file = self
            .lock_file_for_level(level)
            .ok_or_else(|| FrankenError::internal("invalid lock level"))?;
        Self::try_lock_exclusive(file)?;
        self.set_lock_held(level, true);
        Ok(())
    }

    fn unlock_to(&mut self, level: LockLevel) -> Result<()> {
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_unlock) {
            return Err(FrankenError::Io(std::io::Error::other(
                "injected Windows cooperative ordinary-lock unlock failure",
            )));
        }

        if self.lock_held(LockLevel::Exclusive) && level < LockLevel::Exclusive {
            Self::unlock_file(&self.shared_file)?;
            self.set_lock_held(LockLevel::Exclusive, false);
            if level >= LockLevel::Shared {
                Self::try_lock_shared(&self.shared_file)?;
                self.set_lock_held(LockLevel::Shared, true);
            }
        }

        for held_level in [LockLevel::Pending, LockLevel::Reserved, LockLevel::Shared] {
            if level < held_level && self.lock_held(held_level) {
                let file = self
                    .lock_file_for_level(held_level)
                    .ok_or_else(|| FrankenError::internal("invalid lock level"))?;
                Self::unlock_file(file)?;
                self.set_lock_held(held_level, false);
            }
        }
        Ok(())
    }

    fn is_exactly_at(&self, level: LockLevel) -> bool {
        match level {
            LockLevel::None => self.held_levels == [false; 4],
            LockLevel::Shared => self.held_levels == [true, false, false, false],
            LockLevel::Reserved => self.held_levels == [true, true, false, false],
            LockLevel::Pending => self.held_levels == [true, true, true, false],
            // EXCLUSIVE replaces this handle's SHARED lock on the shared
            // sidecar while retaining the RESERVED and PENDING prefix.
            LockLevel::Exclusive => self.held_levels == [false, true, true, true],
        }
    }

    fn restore_to_exact(&mut self, level: LockLevel) -> Result<()> {
        self.unlock_to(level)?;

        // A previous partial restoration can leave an upper sidecar held while
        // a lower prefix member is absent. Rebuild only the missing members,
        // then verify the complete per-surface state instead of trusting the
        // file-level aggregate lock value.
        if level >= LockLevel::Shared
            && !self.lock_held(LockLevel::Shared)
            && !self.lock_held(LockLevel::Exclusive)
        {
            self.try_lock_level(LockLevel::Shared)?;
        }
        if level >= LockLevel::Reserved && !self.lock_held(LockLevel::Reserved) {
            self.try_lock_level(LockLevel::Reserved)?;
        }
        if level >= LockLevel::Pending && !self.lock_held(LockLevel::Pending) {
            self.try_lock_level(LockLevel::Pending)?;
        }
        if level == LockLevel::Exclusive && !self.lock_held(LockLevel::Exclusive) {
            self.try_lock_level(LockLevel::Exclusive)?;
        }

        if self.is_exactly_at(level) {
            Ok(())
        } else {
            Err(FrankenError::internal(format!(
                "Windows cooperative ordinary locks did not restore exactly to {level:?}: held={:?}",
                self.held_levels
            )))
        }
    }

    fn reserved_locked_by_other(&self) -> Result<bool> {
        if self.lock_held(LockLevel::Reserved) {
            return Ok(false);
        }

        match AdvisoryFileLock::try_lock(&self.reserved_file, FileLockMode::Exclusive) {
            Ok(()) => {
                Self::unlock_file(&self.reserved_file)?;
                Ok(false)
            }
            Err(FileLockError::AlreadyLocked) => Ok(true),
            Err(FileLockError::Io(err)) => Err(FrankenError::Io(err)),
        }
    }
}

/// Stock-SQLite-visible main-database lock state for an ordinary connection.
///
/// The dedicated handle makes handle closure a synchronous final unlock if an
/// explicit `UnlockFileEx` reports an error during cleanup.
#[derive(Debug)]
#[allow(clippy::struct_excessive_bools)]
struct WindowsStockMainLocks {
    main_file: File,
    pending: bool,
    reserved: bool,
    shared_range: bool,
    shared_range_exclusive: bool,
    lock_level: LockLevel,
}

impl WindowsStockMainLocks {
    const fn new(main_file: File) -> Self {
        Self {
            main_file,
            pending: false,
            reserved: false,
            shared_range: false,
            shared_range_exclusive: false,
            lock_level: LockLevel::None,
        }
    }

    fn try_lock_level(&mut self, level: LockLevel) -> Result<()> {
        if level <= self.lock_level {
            return Ok(());
        }

        match level {
            LockLevel::None => {}
            LockLevel::Shared => {
                // Match winReadLock() in stock SQLite: briefly participate in
                // PENDING, retain a shared lock over the reader range, then
                // release PENDING so later readers can join the snapshot.
                try_lock_stock_sqlite_shared_range(&self.main_file, STOCK_SQLITE_PENDING_BYTE, 1)?;
                self.pending = true;
                if let Err(lock_error) = try_lock_stock_sqlite_shared_range(
                    &self.main_file,
                    STOCK_SQLITE_SHARED_FIRST,
                    STOCK_SQLITE_SHARED_SIZE,
                ) {
                    let unlock_result =
                        unlock_stock_sqlite_range(&self.main_file, STOCK_SQLITE_PENDING_BYTE, 1);
                    if unlock_result.is_ok() {
                        self.pending = false;
                    }
                    return match unlock_result {
                        Ok(()) => Err(lock_error),
                        Err(unlock_error) => Err(FrankenError::internal(format!(
                            "stock SQLite Windows SHARED acquisition failed and could not release transient PENDING: lock={lock_error}; unlock={unlock_error}"
                        ))),
                    };
                }
                self.shared_range = true;
                if let Err(unlock_error) =
                    unlock_stock_sqlite_range(&self.main_file, STOCK_SQLITE_PENDING_BYTE, 1)
                {
                    let shared_cleanup = unlock_stock_sqlite_range(
                        &self.main_file,
                        STOCK_SQLITE_SHARED_FIRST,
                        STOCK_SQLITE_SHARED_SIZE,
                    );
                    if shared_cleanup.is_ok() {
                        self.shared_range = false;
                    }
                    return match shared_cleanup {
                        Ok(()) => Err(unlock_error),
                        Err(shared_error) => Err(FrankenError::internal(format!(
                            "stock SQLite Windows SHARED acquisition could not release transient PENDING or unwind SHARED: pending={unlock_error}; shared={shared_error}"
                        ))),
                    };
                }
                self.pending = false;
            }
            LockLevel::Reserved => {
                try_lock_stock_sqlite_range(&self.main_file, STOCK_SQLITE_RESERVED_BYTE, 1)?;
                self.reserved = true;
            }
            LockLevel::Pending => {
                try_lock_stock_sqlite_range(&self.main_file, STOCK_SQLITE_PENDING_BYTE, 1)?;
                self.pending = true;
            }
            LockLevel::Exclusive => {
                // LockFileEx cannot atomically promote a shared range. Keep
                // PENDING held while dropping our reader lock so no new stock
                // SQLite reader can enter before the exclusive attempt.
                unlock_stock_sqlite_range(
                    &self.main_file,
                    STOCK_SQLITE_SHARED_FIRST,
                    STOCK_SQLITE_SHARED_SIZE,
                )?;
                self.shared_range = false;
                if let Err(lock_error) = try_lock_stock_sqlite_range(
                    &self.main_file,
                    STOCK_SQLITE_SHARED_FIRST,
                    STOCK_SQLITE_SHARED_SIZE,
                ) {
                    let restore_result = try_lock_stock_sqlite_shared_range(
                        &self.main_file,
                        STOCK_SQLITE_SHARED_FIRST,
                        STOCK_SQLITE_SHARED_SIZE,
                    );
                    if restore_result.is_ok() {
                        self.shared_range = true;
                    }
                    return match restore_result {
                        Ok(()) => Err(lock_error),
                        Err(restore_error) => Err(FrankenError::internal(format!(
                            "stock SQLite Windows EXCLUSIVE acquisition failed and could not restore SHARED: lock={lock_error}; restore={restore_error}"
                        ))),
                    };
                }
                self.shared_range = true;
                self.shared_range_exclusive = true;
            }
        }
        self.lock_level = level;
        Ok(())
    }

    fn highest_held_level(&self) -> LockLevel {
        // Only a complete SQLite lock prefix is a reusable level. A failed
        // promotion/unwind can transiently leave an upper byte held without
        // the SHARED range beneath it; reporting that partial state as
        // RESERVED/PENDING would let a later SHARED request become a false
        // no-op. The caller drops this dedicated handle whenever the requested
        // prefix could not be reconstructed.
        if self.shared_range && self.shared_range_exclusive && self.reserved && self.pending {
            LockLevel::Exclusive
        } else if self.shared_range && self.reserved && self.pending {
            LockLevel::Pending
        } else if self.shared_range && self.reserved {
            LockLevel::Reserved
        } else if self.shared_range {
            LockLevel::Shared
        } else {
            LockLevel::None
        }
    }

    fn recompute_lock_level(&mut self) {
        self.lock_level = self.highest_held_level();
    }

    fn is_exactly_at(&self, level: LockLevel) -> bool {
        match level {
            LockLevel::None => {
                !self.pending
                    && !self.reserved
                    && !self.shared_range
                    && !self.shared_range_exclusive
            }
            LockLevel::Shared => {
                !self.pending && !self.reserved && self.shared_range && !self.shared_range_exclusive
            }
            LockLevel::Reserved => {
                !self.pending && self.reserved && self.shared_range && !self.shared_range_exclusive
            }
            LockLevel::Pending => {
                self.pending && self.reserved && self.shared_range && !self.shared_range_exclusive
            }
            LockLevel::Exclusive => {
                self.pending && self.reserved && self.shared_range && self.shared_range_exclusive
            }
        }
    }

    fn restore_to_exact(&mut self, level: LockLevel) -> Result<()> {
        let mut failures = Vec::new();

        if self.shared_range_exclusive && level < LockLevel::Exclusive {
            match unlock_stock_sqlite_range(
                &self.main_file,
                STOCK_SQLITE_SHARED_FIRST,
                STOCK_SQLITE_SHARED_SIZE,
            ) {
                Ok(()) => {
                    self.shared_range = false;
                    self.shared_range_exclusive = false;
                    if level >= LockLevel::Shared {
                        match try_lock_stock_sqlite_shared_range(
                            &self.main_file,
                            STOCK_SQLITE_SHARED_FIRST,
                            STOCK_SQLITE_SHARED_SIZE,
                        ) {
                            Ok(()) => self.shared_range = true,
                            Err(error) => {
                                failures.push(format!("restore main shared range: {error}"));
                            }
                        }
                    }
                }
                Err(error) => failures.push(format!("main exclusive range: {error}")),
            }
        }

        if self.pending && level < LockLevel::Pending {
            match unlock_stock_sqlite_range(&self.main_file, STOCK_SQLITE_PENDING_BYTE, 1) {
                Ok(()) => self.pending = false,
                Err(error) => failures.push(format!("main pending byte: {error}")),
            }
        }
        if self.reserved && level < LockLevel::Reserved {
            match unlock_stock_sqlite_range(&self.main_file, STOCK_SQLITE_RESERVED_BYTE, 1) {
                Ok(()) => self.reserved = false,
                Err(error) => failures.push(format!("main reserved byte: {error}")),
            }
        }
        if self.shared_range && level < LockLevel::Shared {
            match unlock_stock_sqlite_range(
                &self.main_file,
                STOCK_SQLITE_SHARED_FIRST,
                STOCK_SQLITE_SHARED_SIZE,
            ) {
                Ok(()) => self.shared_range = false,
                Err(error) => failures.push(format!("main shared range: {error}")),
            }
        }

        self.recompute_lock_level();
        if failures.is_empty() {
            // A prior partial restoration can leave one of the lower stock
            // ranges absent while an upper range remains held. Reconstruct the
            // exact requested prefix without treating the derived lock level as
            // proof that every constituent range is present.
            if level >= LockLevel::Shared && !self.shared_range {
                self.try_lock_level(LockLevel::Shared)?;
            }
            if level >= LockLevel::Reserved && !self.reserved {
                self.try_lock_level(LockLevel::Reserved)?;
            }
            if level >= LockLevel::Pending && !self.pending {
                self.try_lock_level(LockLevel::Pending)?;
            }
            if level == LockLevel::Exclusive && !self.shared_range_exclusive {
                self.try_lock_level(LockLevel::Exclusive)?;
            }
            self.recompute_lock_level();
            if !self.is_exactly_at(level) {
                failures.push(format!(
                    "requested {level:?} but exact stock lock state was not restored: pending={} reserved={} shared={} exclusive={}",
                    self.pending,
                    self.reserved,
                    self.shared_range,
                    self.shared_range_exclusive
                ));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(FrankenError::internal(format!(
                "could not release stock SQLite Windows ordinary lock ranges: {}",
                failures.join("; ")
            )))
        }
    }
}

/// Retry state for one external-maintenance acquisition attempt.
///
/// Ordinary SHM locking mirrors the WAL slots onto stock SQLite's real `-shm`
/// bytes. The attempt records only slots newly acquired by maintenance, so
/// restoration cannot release same-owner locks that predated the attempt.
#[derive(Debug)]
#[allow(clippy::struct_excessive_bools)]
struct WindowsExternalMaintenanceLocks {
    wal_mode: bool,
    prior_main_level: LockLevel,
    main_restore_pending: bool,
    wal_write_acquired: bool,
    wal_checkpoint_acquired: bool,
}

impl WindowsExternalMaintenanceLocks {
    const fn new(wal_mode: bool, prior_main_level: LockLevel) -> Self {
        Self {
            wal_mode,
            prior_main_level,
            main_restore_pending: true,
            wal_write_acquired: false,
            wal_checkpoint_acquired: false,
        }
    }

    const fn restoration_complete(&self) -> bool {
        !self.main_restore_pending && !self.wal_write_acquired && !self.wal_checkpoint_acquired
    }
}

impl Vfs for WindowsVfs {
    type File = WindowsFile;

    fn name(&self) -> &'static str {
        "windows"
    }

    #[allow(clippy::significant_drop_tightening)]
    fn open(
        &self,
        cx: &Cx,
        path: Option<&Path>,
        flags: VfsOpenFlags,
    ) -> Result<(Self::File, VfsOpenFlags)> {
        checkpoint_or_abort(cx)?;

        let is_temp = path.is_none();
        let mut resolved = if let Some(path) = path {
            resolve_path(path)?
        } else {
            self.next_temp_path()?
        };

        let is_create = path.is_none() || flags.contains(VfsOpenFlags::CREATE);
        let is_rw = path.is_none() || flags.contains(VfsOpenFlags::READWRITE) || is_create;
        let is_exclusive_create = is_create && flags.contains(VfsOpenFlags::EXCLUSIVE);

        if !is_create && !resolved.exists() {
            return Err(FrankenError::CannotOpen { path: resolved });
        }

        let mut created_db_file = false;
        let file = loop {
            let mut options = windows_open_options();
            options.read(true);
            if is_rw {
                options.write(true);
            }
            if is_create {
                options.create_new(true);
            }

            match options.open(&resolved) {
                Ok(file) => {
                    created_db_file = is_create;
                    break file;
                }
                Err(err) if is_temp && err.kind() == std::io::ErrorKind::AlreadyExists => {
                    resolved = self.next_temp_path()?;
                }
                Err(err)
                    if is_create
                        && !is_temp
                        && !is_exclusive_create
                        && err.kind() == std::io::ErrorKind::AlreadyExists =>
                {
                    let mut open_options = windows_open_options();
                    open_options.read(true);
                    if is_rw {
                        open_options.write(true);
                    }
                    match open_options.open(&resolved) {
                        Ok(file) => break file,
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                        Err(err) => return Err(FrankenError::Io(err)),
                    }
                }
                Err(err) => {
                    return Err(if err.kind() == std::io::ErrorKind::NotFound {
                        FrankenError::CannotOpen { path: resolved }
                    } else {
                        FrankenError::Io(err)
                    });
                }
            }
        };

        let owner_id = next_owner_id();
        let shm_path = sqlite_shm_path(&resolved);

        let delete_on_close = flags.contains(VfsOpenFlags::DELETEONCLOSE) || is_temp;
        let out_flags = if is_create {
            flags | VfsOpenFlags::READWRITE
        } else {
            flags
        };
        // bd-ypl7b / GH#140 (Windows residual): a read-only open must NOT create
        // the -lock-shared/-reserved/-pending advisory sidecars — doing so mutates
        // the database directory and cannot open a clean DB on read-only media,
        // exactly the defect the unix sidecar-less ReadOnlyExisting admission
        // already fixed (bd-daqmp, a410c2735). A read-only binding takes no
        // cooperative file locks: reads are MVCC snapshots, and the pager only
        // calls VfsFile::lock at Reserved/Exclusive on write/maintenance paths
        // that a read-only open never reaches (writes are rejected up front), so
        // `os_locks` stays None and the lock methods are never invoked on it.
        let os_locks = if is_rw {
            match WindowsOsLockFiles::open(&resolved) {
                Ok(os_locks) => Some(os_locks),
                Err(err) => {
                    drop(file);
                    if created_db_file {
                        let _ = fs::remove_file(&resolved);
                    }
                    return Err(err);
                }
            }
        } else {
            None
        };

        Ok((
            WindowsFile {
                path: resolved,
                file: Some(file),
                os_locks,
                stock_main_locks: None,
                #[cfg(test)]
                fail_next_stock_main_clone: false,
                external_shared_snapshot_prior_level: None,
                external_maintenance_locks: None,
                owner_id,
                lock_level: LockLevel::None,
                delete_on_close,
                shm_path,
                shm_state: None,
            },
            out_flags,
        ))
    }

    fn open_with_expected_identity(
        &self,
        cx: &Cx,
        path: &Path,
        flags: VfsOpenFlags,
        expected_identity: FileIdentity,
    ) -> Result<(Self::File, VfsOpenFlags)> {
        checkpoint_or_abort(cx)?;
        let resolved = resolve_path(path)?;

        // Query through a read-only handle before `open` reaches
        // `WindowsOsLockFiles::open`, which creates the advisory sidecars.
        // Our share mode now matches stock SQLite (READ|WRITE|DELETE, see
        // bd-h5oaj / GH#355 above `windows_open_options`); the pathname
        // -replacement TOCTOU this guard also used to lean on is covered
        // independently by the `final_identity` re-check below.
        let mut options = windows_open_options();
        let identity_guard = options.read(true).open(&resolved).map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                // GH#355 (bd-h5oaj) diagnostic (identity-bound reopen path).
                warn!(
                    target: "fsqlite_vfs::reserved",
                    path = %resolved.display(),
                    "identity-bound open: main file not found (CannotOpen)"
                );
                FrankenError::CannotOpen {
                    path: resolved.clone(),
                }
            } else {
                FrankenError::Io(err)
            }
        })?;
        let guard_identity = FileIdentity::from_file(&identity_guard)?;
        if guard_identity != Some(expected_identity) {
            warn!(
                target: "fsqlite_vfs::reserved",
                path = %resolved.display(),
                actual_present = guard_identity.is_some(),
                "identity-bound open: preflight identity mismatch (CannotOpen)"
            );
            return Err(FrankenError::CannotOpen { path: resolved });
        }

        let mut existing_flags = flags;
        existing_flags.remove(VfsOpenFlags::CREATE | VfsOpenFlags::EXCLUSIVE);
        let (file, actual_flags) = self.open(cx, Some(&resolved), existing_flags)?;
        let final_identity = file.file_identity()?;
        if final_identity != Some(expected_identity) {
            warn!(
                target: "fsqlite_vfs::reserved",
                path = %resolved.display(),
                actual_present = final_identity.is_some(),
                "identity-bound open: final handle identity mismatch (CannotOpen)"
            );
            return Err(FrankenError::CannotOpen { path: resolved });
        }
        drop(identity_guard);
        Ok((file, actual_flags))
    }

    fn open_reserved_with_expected_identity(
        &self,
        cx: &Cx,
        path: &Path,
        flags: VfsOpenFlags,
        expected_identity: FileIdentity,
    ) -> Result<(Self::File, VfsOpenFlags)> {
        checkpoint_or_abort(cx)?;
        let resolved = resolve_path(path)?;
        let mut existing_flags = flags;
        existing_flags.remove(VfsOpenFlags::CREATE | VfsOpenFlags::EXCLUSIVE);

        // Open the final main-database handle directly. Ordinary `open`
        // cannot be used here because it creates the advisory lock sidecars
        // before returning the handle to its caller.
        let mut options = windows_open_options();
        options.read(true);
        if existing_flags.contains(VfsOpenFlags::READWRITE) {
            options.write(true);
        }
        let file = options.open(&resolved).map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                // GH#355 (bd-h5oaj) diagnostic: the reserved bootstrap returns an
                // unsourced CannotOpen, which forced a black-box elimination
                // sweep on Windows. Name each failing stage so an instrumented
                // run pinpoints the site.
                warn!(
                    target: "fsqlite_vfs::reserved",
                    path = %resolved.display(),
                    "reserved open: main file not found (CannotOpen)"
                );
                FrankenError::CannotOpen {
                    path: resolved.clone(),
                }
            } else {
                FrankenError::Io(err)
            }
        })?;
        let actual_identity = FileIdentity::from_file(&file)?;
        if actual_identity != Some(expected_identity) {
            warn!(
                target: "fsqlite_vfs::reserved",
                path = %resolved.display(),
                actual_present = actual_identity.is_some(),
                "reserved open: file identity mismatch vs the reservation (CannotOpen)"
            );
            return Err(FrankenError::CannotOpen { path: resolved });
        }
        let actual_len = file.metadata()?.len();
        if actual_len != 0 {
            warn!(
                target: "fsqlite_vfs::reserved",
                path = %resolved.display(),
                len = actual_len,
                "reserved open: reserved target is not a 0-byte file (CannotOpen)"
            );
            return Err(FrankenError::CannotOpen { path: resolved });
        }
        for artifact_path in reserved_database_artifact_paths(&resolved) {
            if filesystem_entry_exists(&artifact_path)? {
                warn!(
                    target: "fsqlite_vfs::reserved",
                    path = %resolved.display(),
                    artifact = %artifact_path.display(),
                    "reserved open: reserved-database artifact present (CannotOpen)"
                );
                return Err(FrankenError::CannotOpen { path: resolved });
            }
        }

        // Only an accepted reservation may create the Windows advisory-lock
        // sidecars. The main handle above now shares FILE_SHARE_DELETE like
        // stock SQLite (bd-h5oaj / GH#355); the identity and zero-length
        // checks just above already re-verify against the live handle, so a
        // pathname replacement between verification and construction is
        // still caught rather than silently admitted.
        let os_locks = WindowsOsLockFiles::open(&resolved)?;
        let owner_id = next_owner_id();
        let shm_path = sqlite_shm_path(&resolved);
        let delete_on_close = existing_flags.contains(VfsOpenFlags::DELETEONCLOSE);
        Ok((
            WindowsFile {
                path: resolved,
                file: Some(file),
                os_locks: Some(os_locks),
                stock_main_locks: None,
                #[cfg(test)]
                fail_next_stock_main_clone: false,
                external_shared_snapshot_prior_level: None,
                external_maintenance_locks: None,
                owner_id,
                lock_level: LockLevel::None,
                delete_on_close,
                shm_path,
                shm_state: None,
            },
            existing_flags,
        ))
    }

    fn delete(&self, cx: &Cx, path: &Path, sync_dir: bool) -> Result<()> {
        let resolved = resolve_path(path)?;
        if resolved.exists() {
            fs::remove_file(&resolved)?;
        }
        let shm_path = sqlite_shm_path(&resolved);
        if shm_path.exists() {
            fs::remove_file(shm_path)?;
        }
        try_remove_windows_lock_sidecars(&resolved);
        if sync_dir {
            self.sync_parent_directory(cx, &resolved)?;
        }
        Ok(())
    }

    fn sync_parent_directory(&self, _cx: &Cx, _path: &Path) -> Result<()> {
        // Win32 does not expose a portable fsync-directory operation. SQLite's
        // own Windows VFS establishes journal create/delete durability through
        // FlushFileBuffers on the journal handle, which `WindowsFile::sync_all`
        // performs before this hook is reached. Keep the namespace hook an
        // explicit no-op rather than attempting FlushFileBuffers on a directory
        // handle (unsupported on common filesystems and liable to return
        // ERROR_INVALID_HANDLE).
        Ok(())
    }

    fn access(&self, _cx: &Cx, path: &Path, flags: AccessFlags) -> Result<bool> {
        let resolved = resolve_path(path)?;
        if !resolved.exists() {
            return Ok(false);
        }
        match flags {
            f if f == AccessFlags::EXISTS => Ok(true),
            f if f == AccessFlags::READ => {
                let mut options = windows_open_options();
                Ok(options.read(true).open(resolved).is_ok())
            }
            _ => {
                let mut options = windows_open_options();
                Ok(options.read(true).write(true).open(resolved).is_ok())
            }
        }
    }

    fn path_entry_exists(&self, _cx: &Cx, path: &Path) -> Result<bool> {
        filesystem_entry_exists(&resolve_path(path)?)
    }

    fn full_pathname(&self, _cx: &Cx, path: &Path) -> Result<PathBuf> {
        stable_full_path(path)
    }
}

/// A file handle opened by [`WindowsVfs`].
#[derive(Debug)]
pub struct WindowsFile {
    path: PathBuf,
    file: Option<File>,
    os_locks: Option<WindowsOsLockFiles>,
    stock_main_locks: Option<WindowsStockMainLocks>,
    #[cfg(test)]
    fail_next_stock_main_clone: bool,
    external_shared_snapshot_prior_level: Option<LockLevel>,
    external_maintenance_locks: Option<WindowsExternalMaintenanceLocks>,
    owner_id: u64,
    lock_level: LockLevel,
    delete_on_close: bool,
    shm_path: PathBuf,
    shm_state: Option<Arc<Mutex<WindowsShmState>>>,
}

impl WindowsFile {
    fn is_closed(&self) -> bool {
        self.file.is_none() && self.os_locks.is_none()
    }

    fn ensure_open(&self) -> Result<()> {
        if self.is_closed() {
            Err(FrankenError::internal("windows file is closed"))
        } else {
            Ok(())
        }
    }

    fn file_ref(&self) -> Result<&File> {
        self.file
            .as_ref()
            .ok_or_else(|| FrankenError::internal("windows file is closed"))
    }

    fn file_mut(&mut self) -> Result<&mut File> {
        self.file
            .as_mut()
            .ok_or_else(|| FrankenError::internal("windows file is closed"))
    }

    fn os_locks_ref(&self) -> Result<&WindowsOsLockFiles> {
        self.os_locks
            .as_ref()
            .ok_or_else(|| FrankenError::internal("windows lock files are closed"))
    }

    fn os_locks_mut(&mut self) -> Result<&mut WindowsOsLockFiles> {
        self.os_locks
            .as_mut()
            .ok_or_else(|| FrankenError::internal("windows lock files are closed"))
    }

    /// Open the advisory lock sidecars on demand for a binding that was
    /// admitted sidecar-less (bd-ypl7b: read-only opens leave `os_locks`
    /// `None`).
    ///
    /// An external fence (`lock_external_shared_snapshot` /
    /// `lock_external_maintenance`) is an explicit cross-process coordination
    /// request, not a plain read, so materializing the cooperative sidecars
    /// here is correct even on a read-only binding — the protocol needs the
    /// shared artifacts, exactly as a read-write open would have created
    /// them. Plain read-only opens stay sidecar-less. On read-only media the
    /// sidecar creation surfaces the real filesystem error instead of the
    /// misleading `windows lock files are closed` internal error that made
    /// every `br` command fail on Windows (beads_rust GH#438; the regression
    /// class bd-ypl7b's plan predicted for lock ops reached on an RO
    /// binding).
    fn ensure_os_locks(&mut self) -> Result<()> {
        if self.os_locks.is_some() {
            return Ok(());
        }
        if self.file.is_none() {
            return Err(FrankenError::internal("windows file is closed"));
        }
        self.os_locks = Some(WindowsOsLockFiles::open(&self.path)?);
        Ok(())
    }

    fn ensure_stock_main_locks(&mut self) -> Result<()> {
        if self.stock_main_locks.is_none() {
            #[cfg(test)]
            if std::mem::take(&mut self.fail_next_stock_main_clone) {
                return Err(FrankenError::Io(std::io::Error::other(
                    "injected Windows main-handle clone failure",
                )));
            }
            self.stock_main_locks = Some(WindowsStockMainLocks::new(self.file_ref()?.try_clone()?));
        }
        Ok(())
    }

    fn stock_main_locks_mut(&mut self) -> Result<&mut WindowsStockMainLocks> {
        self.ensure_stock_main_locks()?;
        self.stock_main_locks
            .as_mut()
            .ok_or_else(|| FrankenError::internal("windows stock main lock handle is closed"))
    }

    fn ensure_shm_state(&mut self) -> Result<Arc<Mutex<WindowsShmState>>> {
        if let Some(state) = &self.shm_state {
            return Ok(Arc::clone(state));
        }
        let state =
            windows_shm_table().get_or_create_and_register(&self.shm_path, self.owner_id)?;
        self.shm_state = Some(Arc::clone(&state));
        Ok(state)
    }

    fn ensure_stock_shm_file<'a>(
        state: &'a mut WindowsShmState,
        shm_path: &Path,
    ) -> Result<&'a File> {
        if let Some(detail) = &state.poisoned {
            return Err(FrankenError::internal(format!(
                "Windows SHM lock state is poisoned: {detail}"
            )));
        }
        if state.stock_shm_file.is_none() {
            let (file, _) = open_windows_lock_sidecar(shm_path)?;
            state.stock_shm_file = Some(file);
        }
        state
            .stock_shm_file
            .as_ref()
            .ok_or_else(|| FrankenError::internal("Windows stock SHM lock handle is closed"))
    }

    fn stock_shm_lock_byte(slot: u32) -> Result<u64> {
        if slot >= WAL_TOTAL_LOCKS {
            return Err(FrankenError::LockFailed {
                detail: format!("invalid SHM slot {slot}"),
            });
        }
        Ok(STOCK_SQLITE_SHM_LOCK_BASE + u64::from(slot))
    }

    #[allow(clippy::too_many_lines)]
    fn release_shm_owner_state(&mut self, delete: bool) -> Result<()> {
        let Some(state_arc) = self.shm_state.as_ref().map(Arc::clone) else {
            if delete {
                drop(fs::remove_file(&self.shm_path));
            }
            return Ok(());
        };

        let mut owner_detached = false;
        let release_result = (|| -> Result<bool> {
            let mut state = state_arc
                .lock()
                .map_err(|_| lock_poisoned("windows shm state"))?;
            let owner_ref_count = state.owner_refs.get(&self.owner_id).copied().unwrap_or(0);
            if owner_ref_count == 0 {
                return Err(FrankenError::internal(format!(
                    "owner {} is not registered in Windows SHM state",
                    self.owner_id
                )));
            }
            if state.poisoned.is_some() {
                // A failed promotion/downgrade restoration means the local
                // slot table can no longer prove which ranges the OS retains.
                // Keep the process-scoped handle and every possibly-live range
                // until the whole poisoned cohort drains. Closing it for one
                // departing owner would silently unfence other owners that may
                // already be inside WAL critical sections. Future SHM calls
                // still fail closed through the poison check.
                if owner_ref_count > 1 {
                    state.owner_refs.insert(self.owner_id, owner_ref_count - 1);
                } else {
                    let _ = state.owner_refs.remove(&self.owner_id);
                }
                let orphaned = state.owner_refs.is_empty();
                if orphaned {
                    drop(state.stock_shm_file.take());
                    for slot in &mut state.slots {
                        slot.shared_holders.clear();
                        slot.exclusive_owner = None;
                    }
                    state.poisoned = None;
                }
                return Ok(orphaned);
            }
            if owner_ref_count > 1 {
                state.owner_refs.insert(self.owner_id, owner_ref_count - 1);
                return Ok(false);
            }

            let last_process_owner = state.owner_refs.len() == 1;
            if last_process_owner {
                // Closing the process-scoped handle is the final synchronous
                // release for every outstanding range. Keep the state mutex
                // held until CloseHandle completes so no local opener can
                // observe the slots as free before the OS does.
                drop(state.stock_shm_file.take());
                for slot in &mut state.slots {
                    let _ = slot.shared_holders.remove(&self.owner_id);
                    if slot.exclusive_owner == Some(self.owner_id) {
                        slot.exclusive_owner = None;
                    }
                }
                let _ = state.owner_refs.remove(&self.owner_id);
                state.poisoned = None;
                return Ok(true);
            }

            for slot in 0..WAL_TOTAL_LOCKS {
                let idx = to_slot_index(slot)?;
                let owns_exclusive = state.slots[idx].exclusive_owner == Some(self.owner_id);
                let owns_shared = state.slots[idx].shared_holders.contains_key(&self.owner_id);
                if !owns_exclusive && !owns_shared {
                    continue;
                }
                let other_shared = state.slots[idx]
                    .shared_holders
                    .iter()
                    .any(|(owner, count)| *owner != self.owner_id && *count > 0);
                let lock_byte = Self::stock_shm_lock_byte(slot)?;
                let Some(shm_file) = state.stock_shm_file.as_ref() else {
                    let detail = format!(
                        "owner {} has SHM slot {slot} state without a stock -shm handle",
                        self.owner_id
                    );
                    state.poisoned = Some(detail.clone());
                    let _ = state.owner_refs.remove(&self.owner_id);
                    owner_detached = true;
                    return Err(FrankenError::internal(detail));
                };

                if owns_exclusive {
                    if other_shared {
                        if let Err(overlay_error) =
                            try_lock_stock_sqlite_shared_range(shm_file, lock_byte, 1)
                        {
                            state.poisoned = Some(format!(
                                "owner-close shared overlay failed for slot {slot}: {overlay_error}"
                            ));
                            let _ = state.owner_refs.remove(&self.owner_id);
                            owner_detached = true;
                            return Err(FrankenError::internal(format!(
                                "Windows SHM owner-close could not preserve surviving shared slot {slot}: {overlay_error}"
                            )));
                        }
                        if let Err(unlock_error) =
                            unlock_stock_sqlite_range_strict(shm_file, lock_byte, 1)
                        {
                            let detail = restore_missing_stock_sqlite_range_fence(
                                shm_file,
                                lock_byte,
                                1,
                                WindowsRangeLockMode::Exclusive,
                                "owner-close exclusive downgrade",
                                &unlock_error,
                            )
                            .unwrap_or_else(|| {
                                format!(
                                    "owner-close exclusive unlock failed after installing shared overlay for slot {slot}: {unlock_error}"
                                )
                            });
                            state.poisoned = Some(detail);
                            let _ = state.owner_refs.remove(&self.owner_id);
                            owner_detached = true;
                            return Err(FrankenError::internal(format!(
                                "Windows SHM owner-close installed a shared overlay but could not release exclusive slot {slot}: {unlock_error}"
                            )));
                        }
                    } else if let Err(unlock_error) =
                        unlock_stock_sqlite_range_strict(shm_file, lock_byte, 1)
                    {
                        let detail = restore_missing_stock_sqlite_range_fence(
                            shm_file,
                            lock_byte,
                            1,
                            WindowsRangeLockMode::Exclusive,
                            "owner-close exclusive unlock",
                            &unlock_error,
                        )
                        .unwrap_or_else(|| {
                            format!(
                                "owner-close exclusive unlock failed for slot {slot}: {unlock_error}"
                            )
                        });
                        state.poisoned = Some(detail);
                        let _ = state.owner_refs.remove(&self.owner_id);
                        owner_detached = true;
                        return Err(FrankenError::internal(format!(
                            "Windows SHM owner-close could not release exclusive slot {slot}: {unlock_error}"
                        )));
                    }
                    state.slots[idx].exclusive_owner = None;
                } else if owns_shared
                    && !other_shared
                    && let Err(unlock_error) =
                        unlock_stock_sqlite_range_strict(shm_file, lock_byte, 1)
                {
                    let detail = restore_missing_stock_sqlite_range_fence(
                        shm_file,
                        lock_byte,
                        1,
                        WindowsRangeLockMode::Shared,
                        "owner-close shared unlock",
                        &unlock_error,
                    )
                    .unwrap_or_else(|| {
                        format!("owner-close shared unlock failed for slot {slot}: {unlock_error}")
                    });
                    state.poisoned = Some(detail);
                    let _ = state.owner_refs.remove(&self.owner_id);
                    owner_detached = true;
                    return Err(FrankenError::internal(format!(
                        "Windows SHM owner-close could not release shared slot {slot}: {unlock_error}"
                    )));
                }
                let _ = state.slots[idx].shared_holders.remove(&self.owner_id);
            }

            let _ = state.owner_refs.remove(&self.owner_id);
            Ok(false)
        })();

        let orphaned = match release_result {
            Ok(orphaned) => orphaned,
            Err(error) => {
                if owner_detached {
                    drop(self.shm_state.take());
                }
                return Err(error);
            }
        };
        drop(self.shm_state.take());
        if orphaned {
            windows_shm_table().remove_if_orphaned(&self.shm_path, &state_arc)?;
        }

        if delete {
            drop(fs::remove_file(&self.shm_path));
        }

        Ok(())
    }

    fn ordinary_locks_are_exactly_at(&self, level: LockLevel) -> Result<bool> {
        let stock_exact = self
            .stock_main_locks
            .as_ref()
            .map_or(level == LockLevel::None, |locks| locks.is_exactly_at(level));
        // A sidecar-less binding (bd-ypl7b read-only open) never opened the
        // cooperative sidecars, so its cooperative state is vacuously NONE —
        // mirror the stock-handle treatment above instead of erroring with
        // "windows lock files are closed" (beads_rust GH#438: that error
        // fired on every read-only open+close cycle).
        let cooperative_exact = self
            .os_locks
            .as_ref()
            .map_or(level == LockLevel::None, |locks| locks.is_exactly_at(level));
        Ok(stock_exact && cooperative_exact)
    }

    fn rollback_ordinary_locks_to(&mut self, level: LockLevel) -> Result<()> {
        let stock_result = if self.stock_main_locks.is_none() && level == LockLevel::None {
            Ok(())
        } else {
            self.stock_main_locks_mut()
                .and_then(|locks| locks.restore_to_exact(level))
        };
        if stock_result.is_err() {
            // A partial UnlockFileEx failure can leave a non-prefix subset of
            // SQLite lock levels (for example RESERVED without SHARED). Drop
            // the dedicated handle before returning the error: CloseHandle is
            // the synchronous final release and the next attempt must rebuild
            // its stock-visible state from NONE rather than trust a stale or
            // structurally incomplete lock ladder.
            drop(self.stock_main_locks.take());
        }
        let cooperative_result = if self.os_locks.is_none() && level == LockLevel::None {
            // Sidecar-less binding (bd-ypl7b): no cooperative sidecars were
            // ever opened, so restoring to NONE is vacuously complete. The
            // unlock/close path must never materialize sidecars — a read-only
            // open on read-only media has to close cleanly.
            Ok(())
        } else {
            self.os_locks_mut()
                .and_then(|locks| locks.restore_to_exact(level))
        };
        match (stock_result, cooperative_result) {
            (Ok(()), Ok(())) => {
                if !self.ordinary_locks_are_exactly_at(level)? {
                    return Err(FrankenError::internal(format!(
                        "Windows ordinary lock restoration reported success without exact {level:?} ownership on both surfaces"
                    )));
                }
                // The aggregate is publication state, not a recovery oracle.
                // Advance it only after both independent surfaces prove the
                // exact recorded target.
                self.lock_level = level;
                Ok(())
            }
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(stock_error), Err(cooperative_error)) => Err(FrankenError::internal(format!(
                "Windows ordinary lock rollback failed on both lock surfaces: stock={stock_error}; cooperative={cooperative_error}"
            ))),
        }
    }

    fn restore_ordinary_lock_level(&mut self, _cx: &Cx, level: LockLevel) -> Result<()> {
        self.rollback_ordinary_locks_to(level)
    }

    fn acquire_cooperative_wal_maintenance_locks(
        &mut self,
        cx: &Cx,
        wal_mode: bool,
        write_preheld: bool,
        checkpoint_preheld: bool,
    ) -> Result<()> {
        if !wal_mode {
            return Ok(());
        }

        if !write_preheld {
            self.external_maintenance_locks
                .as_mut()
                .ok_or_else(|| {
                    FrankenError::internal(
                        "Windows maintenance has no attempt marker before acquiring WAL write",
                    )
                })?
                .wal_write_acquired = true;
            let result = self.shm_lock(
                cx,
                WAL_WRITE_LOCK,
                1,
                SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE,
            );
            if result.is_err()
                && let Some(attempt) = self.external_maintenance_locks.as_mut()
            {
                attempt.wal_write_acquired = false;
            }
            result?;
        }

        if !checkpoint_preheld {
            self.external_maintenance_locks
                .as_mut()
                .ok_or_else(|| {
                    FrankenError::internal(
                        "Windows maintenance has no attempt marker before acquiring WAL checkpoint",
                    )
                })?
                .wal_checkpoint_acquired = true;
            let result =
                self.shm_lock(cx, WAL_CKPT_LOCK, 1, SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE);
            if result.is_err()
                && let Some(attempt) = self.external_maintenance_locks.as_mut()
            {
                attempt.wal_checkpoint_acquired = false;
            }
            result?;
        }
        Ok(())
    }

    fn owns_exclusive_shm_slot(&self, slot: u32) -> Result<bool> {
        let Some(state) = &self.shm_state else {
            return Ok(false);
        };
        let state = state
            .lock()
            .map_err(|_| lock_poisoned("windows shm state"))?;
        let idx = to_slot_index(slot)?;
        Ok(state
            .slots
            .get(idx)
            .is_some_and(|slot| slot.exclusive_owner == Some(self.owner_id)))
    }

    fn verify_stock_sqlite_maintenance_locks(&mut self, wal_mode: bool) -> Result<()> {
        self.ensure_open()?;
        if wal_mode {
            let state = self.ensure_shm_state()?;
            let state = state
                .lock()
                .map_err(|_| lock_poisoned("windows shm state"))?;
            if let Some(detail) = &state.poisoned {
                return Err(FrankenError::internal(format!(
                    "Windows maintenance found poisoned SHM state: {detail}"
                )));
            }
            for slot in WAL_WRITE_LOCK..=WAL_CKPT_LOCK {
                let idx = to_slot_index(slot)?;
                let slot_state = state
                    .slots
                    .get(idx)
                    .ok_or_else(|| FrankenError::internal("shm slot index out of bounds"))?;
                if slot_state.exclusive_owner != Some(self.owner_id) {
                    return Err(FrankenError::internal(format!(
                        "Windows maintenance lost its ordinary exclusive SHM slot {slot}"
                    )));
                }
            }
            if state.stock_shm_file.is_none() {
                return Err(FrankenError::internal(
                    "Windows maintenance has SHM state without a stock -shm handle",
                ));
            }
        }

        Ok(())
    }

    fn validate_shm_request(offset: u32, n: u32) -> Result<u32> {
        if n == 0 {
            return Err(FrankenError::LockFailed {
                detail: "shm_lock called with n=0".to_string(),
            });
        }
        let end = offset
            .checked_add(n)
            .ok_or_else(|| FrankenError::LockFailed {
                detail: "shm_lock range overflow".to_string(),
            })?;
        if end > WAL_TOTAL_LOCKS {
            return Err(FrankenError::LockFailed {
                detail: format!("shm_lock range {offset}..{end} exceeds WAL lock table"),
            });
        }
        Ok(end)
    }

    fn acquire_shared_slot(&self, state: &mut WindowsShmState, slot: u32) -> Result<()> {
        if let Some(detail) = &state.poisoned {
            return Err(FrankenError::internal(format!(
                "Windows SHM lock state is poisoned: {detail}"
            )));
        }
        let idx = to_slot_index(slot)?;
        let slot_state = state
            .slots
            .get_mut(idx)
            .ok_or_else(|| FrankenError::internal("shm slot index out of bounds"))?;
        if let Some(exclusive_owner) = slot_state.exclusive_owner
            && exclusive_owner != self.owner_id
        {
            return Err(FrankenError::Busy);
        }
        let has_shared = slot_state
            .shared_holders
            .values()
            .copied()
            .any(|count| count > 0);
        let prior_count = slot_state
            .shared_holders
            .get(&self.owner_id)
            .copied()
            .unwrap_or(0);
        let next_count = prior_count
            .checked_add(1)
            .ok_or_else(|| FrankenError::internal("Windows SHM shared lock count overflow"))?;
        if !has_shared && slot_state.exclusive_owner.is_none() {
            let lock_byte = Self::stock_shm_lock_byte(slot)?;
            let shm_path = self.shm_path.clone();
            try_lock_stock_sqlite_shared_range(
                Self::ensure_stock_shm_file(state, &shm_path)?,
                lock_byte,
                1,
            )?;
        }
        state.slots[idx]
            .shared_holders
            .insert(self.owner_id, next_count);
        Ok(())
    }

    /// Acquire an exclusive slot and report whether this call changed state.
    ///
    /// An already-held exclusive lock is an idempotent success. The caller
    /// must not unwind that pre-existing hold if a later slot in the same
    /// multi-slot request fails.
    fn acquire_exclusive_slot(&self, state: &mut WindowsShmState, slot: u32) -> Result<bool> {
        if let Some(detail) = &state.poisoned {
            return Err(FrankenError::internal(format!(
                "Windows SHM lock state is poisoned: {detail}"
            )));
        }
        let idx = to_slot_index(slot)?;
        let slot_state = state
            .slots
            .get_mut(idx)
            .ok_or_else(|| FrankenError::internal("shm slot index out of bounds"))?;

        if slot_state.exclusive_owner == Some(self.owner_id) {
            return Ok(false);
        }

        if slot_state.exclusive_owner.is_some() {
            return Err(FrankenError::Busy);
        }

        if slot_state
            .shared_holders
            .iter()
            .any(|(owner, count)| *owner != self.owner_id && *count > 0)
        {
            return Err(FrankenError::Busy);
        }

        let lock_byte = Self::stock_shm_lock_byte(slot)?;
        let has_shared = slot_state
            .shared_holders
            .values()
            .copied()
            .any(|count| count > 0);
        let shm_path = self.shm_path.clone();
        let shm_file = Self::ensure_stock_shm_file(state, &shm_path)?;
        if has_shared {
            // LockFileEx cannot atomically promote a shared range. Drop the
            // process-scoped shared lock, attempt EXCLUSIVE, and restore the
            // shared lock before returning if promotion loses a race.
            if let Err(unlock_error) = unlock_stock_sqlite_range_strict(shm_file, lock_byte, 1) {
                if let Some(detail) = restore_missing_stock_sqlite_range_fence(
                    shm_file,
                    lock_byte,
                    1,
                    WindowsRangeLockMode::Shared,
                    "exclusive promotion",
                    &unlock_error,
                ) {
                    state.poisoned = Some(detail.clone());
                    return Err(FrankenError::internal(detail));
                }
                return Err(unlock_error);
            }
            if let Err(lock_error) = try_lock_stock_sqlite_range(shm_file, lock_byte, 1) {
                let restore_result = try_lock_stock_sqlite_shared_range(shm_file, lock_byte, 1);
                if restore_result.is_err() {
                    state.poisoned = Some(format!(
                        "exclusive promotion failed for slot {slot}: lock={lock_error}; restore={restore_result:?}"
                    ));
                }
                return match restore_result {
                    Ok(()) => Err(lock_error),
                    Err(restore_error) => Err(FrankenError::internal(format!(
                        "Windows SHM exclusive promotion failed and could not restore shared slot {slot}: lock={lock_error}; restore={restore_error}"
                    ))),
                };
            }
        } else {
            try_lock_stock_sqlite_range(shm_file, lock_byte, 1)?;
        }
        state.slots[idx].exclusive_owner = Some(self.owner_id);
        Ok(true)
    }

    fn release_shared_slot(&self, state: &mut WindowsShmState, slot: u32) -> Result<()> {
        if let Some(detail) = &state.poisoned {
            return Err(FrankenError::internal(format!(
                "Windows SHM lock state is poisoned: {detail}"
            )));
        }
        let idx = to_slot_index(slot)?;
        let slot_state = state
            .slots
            .get_mut(idx)
            .ok_or_else(|| FrankenError::internal("shm slot index out of bounds"))?;
        let Some(holder_count) = slot_state.shared_holders.get(&self.owner_id).copied() else {
            return Err(FrankenError::LockFailed {
                detail: format!(
                    "owner {} does not hold shared SHM slot {slot}",
                    self.owner_id
                ),
            });
        };
        if holder_count > 1 {
            state.slots[idx]
                .shared_holders
                .insert(self.owner_id, holder_count - 1);
            return Ok(());
        }
        let other_shared = slot_state
            .shared_holders
            .iter()
            .any(|(owner, count)| *owner != self.owner_id && *count > 0);
        if slot_state.exclusive_owner.is_none() && !other_shared {
            let lock_byte = Self::stock_shm_lock_byte(slot)?;
            let shm_file = state.stock_shm_file.as_ref().ok_or_else(|| {
                FrankenError::internal(format!(
                    "owner {} has shared SHM slot {slot} without a stock -shm handle",
                    self.owner_id
                ))
            })?;
            if let Err(unlock_error) = unlock_stock_sqlite_range_strict(shm_file, lock_byte, 1) {
                if let Some(detail) = restore_missing_stock_sqlite_range_fence(
                    shm_file,
                    lock_byte,
                    1,
                    WindowsRangeLockMode::Shared,
                    "shared unlock",
                    &unlock_error,
                ) {
                    state.poisoned = Some(detail.clone());
                    return Err(FrankenError::internal(detail));
                }
                return Err(unlock_error);
            }
        }
        let _ = state.slots[idx].shared_holders.remove(&self.owner_id);
        Ok(())
    }

    fn release_exclusive_slot(&self, state: &mut WindowsShmState, slot: u32) -> Result<()> {
        if let Some(detail) = &state.poisoned {
            return Err(FrankenError::internal(format!(
                "Windows SHM lock state is poisoned: {detail}"
            )));
        }
        let idx = to_slot_index(slot)?;
        let slot_state = state
            .slots
            .get_mut(idx)
            .ok_or_else(|| FrankenError::internal("shm slot index out of bounds"))?;
        if slot_state.exclusive_owner != Some(self.owner_id) {
            return Err(FrankenError::LockFailed {
                detail: format!(
                    "owner {} does not hold exclusive SHM slot {slot}",
                    self.owner_id
                ),
            });
        }
        let lock_byte = Self::stock_shm_lock_byte(slot)?;
        let preserve_shared = slot_state
            .shared_holders
            .values()
            .copied()
            .any(|count| count > 0);
        let shm_file = state.stock_shm_file.as_ref().ok_or_else(|| {
            FrankenError::internal(format!(
                "owner {} has exclusive SHM slot {slot} without a stock -shm handle",
                self.owner_id
            ))
        })?;
        if preserve_shared {
            // Win32 permits a SHARED range to overlap an EXCLUSIVE range on
            // the same handle. Its first matching UnlockFileEx then removes
            // EXCLUSIVE while leaving SHARED, which makes this downgrade
            // continuous from the perspective of every other process.
            try_lock_stock_sqlite_shared_range(shm_file, lock_byte, 1)?;
            if let Err(unlock_error) = unlock_stock_sqlite_range_strict(shm_file, lock_byte, 1) {
                let detail = restore_missing_stock_sqlite_range_fence(
                    shm_file,
                    lock_byte,
                    1,
                    WindowsRangeLockMode::Exclusive,
                    "exclusive downgrade",
                    &unlock_error,
                )
                .unwrap_or_else(|| {
                    format!(
                        "exclusive unlock failed after installing shared overlay for slot {slot}: {unlock_error}"
                    )
                });
                state.poisoned = Some(detail);
                return Err(FrankenError::internal(format!(
                    "Windows SHM installed a shared overlay but could not release exclusive slot {slot}: {unlock_error}"
                )));
            }
        } else if let Err(unlock_error) = unlock_stock_sqlite_range_strict(shm_file, lock_byte, 1) {
            if let Some(detail) = restore_missing_stock_sqlite_range_fence(
                shm_file,
                lock_byte,
                1,
                WindowsRangeLockMode::Exclusive,
                "exclusive unlock",
                &unlock_error,
            ) {
                state.poisoned = Some(detail.clone());
                return Err(FrankenError::internal(detail));
            }
            return Err(unlock_error);
        }
        state.slots[idx].exclusive_owner = None;
        Ok(())
    }
}

impl VfsFile for WindowsFile {
    fn close(&mut self, cx: &Cx) -> Result<()> {
        if self.is_closed() && self.shm_state.is_none() {
            return Ok(());
        }

        let mut first_error = if self.external_shared_snapshot_prior_level.is_some() {
            self.restore_external_shared_snapshot_attempt(cx).err()
        } else {
            None
        };
        if self.external_maintenance_locks.is_some()
            && let Err(error) = self.restore_external_maintenance_attempt(cx)
            && first_error.is_none()
        {
            first_error = Some(error);
        }

        if !self.is_closed()
            && let Err(err) = self.unlock(cx, LockLevel::None)
            && first_error.is_none()
        {
            first_error = Some(err);
        }

        let release_result = if self.shm_state.is_some() || self.delete_on_close {
            self.release_shm_owner_state(self.delete_on_close)
        } else {
            Ok(())
        };
        if first_error.is_none() {
            first_error = release_result.err();
        }

        // Closing the underlying handles is the final Win32 lock-release
        // fallback even if an explicit UnlockFileEx call above failed.
        drop(self.stock_main_locks.take());
        self.external_shared_snapshot_prior_level = None;
        let _ = self.external_maintenance_locks.take();
        drop(self.os_locks.take());
        drop(self.file.take());
        self.lock_level = LockLevel::None;

        if self.delete_on_close {
            drop(fs::remove_file(&self.path));
            try_remove_windows_lock_sidecars(&self.path);
        }

        first_error.map_or(Ok(()), Err)
    }

    fn file_identity(&self) -> Result<Option<FileIdentity>> {
        Ok(FileIdentity::from_file(self.file_ref()?)?)
    }

    async fn read(&self, cx: &Cx, buf: &mut [u8], offset: u64) -> Result<usize> {
        checkpoint_or_abort(cx)?;
        let file = self.file_ref()?.try_clone().map_err(FrankenError::Io)?;
        let requested = buf.len();
        let (data, total) = spawn_blocking_io(move || read_owned_at(&file, requested, offset))
            .await
            .map_err(FrankenError::Io)?;
        checkpoint_or_abort(cx)?;
        buf.copy_from_slice(&data);
        Ok(total)
    }

    async fn write(&self, cx: &Cx, buf: &[u8], offset: u64) -> Result<()> {
        checkpoint_or_abort(cx)?;
        let file = self.file_ref()?.try_clone().map_err(FrankenError::Io)?;
        let data = buf.to_vec();
        spawn_blocking_io(move || write_owned_at(&file, &data, offset))
            .await
            .map_err(FrankenError::Io)?;
        checkpoint_or_abort(cx)
    }

    async fn write_tracked(
        &self,
        cx: &Cx,
        buf: &[u8],
        offset: u64,
        completion: VfsWriteCompletion,
    ) -> Result<()> {
        let file = match (|| {
            checkpoint_or_abort(cx)?;
            self.file_ref()?.try_clone().map_err(FrankenError::Io)
        })() {
            Ok(file) => file,
            Err(error) => {
                completion.complete_error();
                return Err(error);
            }
        };
        let data = buf.to_vec();
        let source_completion = VfsWriteCompletionSource::new(completion.clone());
        spawn_blocking_io(move || write_owned_at_tracked(&file, &data, offset, source_completion))
            .await
            .map_err(FrankenError::Io)?;
        checkpoint_or_abort(cx)
    }

    fn truncate(&mut self, _cx: &Cx, size: u64) -> Result<()> {
        self.file_mut()?.set_len(size)?;
        Ok(())
    }

    fn sync(&mut self, _cx: &Cx, flags: SyncFlags) -> Result<()> {
        if flags.contains(SyncFlags::DATAONLY) {
            self.file_mut()?.sync_data()?;
        } else {
            self.file_mut()?.sync_all()?;
        }
        Ok(())
    }

    fn file_size(&self, _cx: &Cx) -> Result<u64> {
        Ok(self.file_ref()?.metadata()?.len())
    }

    fn lock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
        if !self.ordinary_locks_are_exactly_at(self.lock_level)? {
            self.restore_ordinary_lock_level(cx, self.lock_level)?;
        }
        if level <= self.lock_level {
            return Ok(());
        }

        // A sidecar-less read-only binding (bd-ypl7b) materializes the
        // cooperative sidecars on demand the moment something actually
        // acquires a lock through it; plain reads never reach this point.
        self.ensure_os_locks()?;
        // Materialize the stock-visible handle before acquiring any
        // cooperative sidecar lock. If duplicating the main handle fails,
        // no partial lock acquisition is left hidden behind the unchanged
        // logical lock level.
        self.ensure_stock_main_locks()?;
        let prior_level = self.lock_level;
        while self.lock_level < level {
            let next = next_lock_level(self.lock_level)
                .ok_or_else(|| FrankenError::internal("invalid lock escalation"))?;
            if let Err(lock_error) = self.os_locks_mut()?.try_lock_level(next) {
                let rollback_result = self.rollback_ordinary_locks_to(prior_level);
                return match rollback_result {
                    Ok(()) => Err(lock_error),
                    Err(rollback_error) => Err(FrankenError::internal(format!(
                        "Windows cooperative lock acquisition failed and rollback also failed: lock={lock_error}; rollback={rollback_error}"
                    ))),
                };
            }
            if let Err(lock_error) = self.stock_main_locks_mut()?.try_lock_level(next) {
                let rollback_result = self.rollback_ordinary_locks_to(prior_level);
                return match rollback_result {
                    Ok(()) => Err(lock_error),
                    Err(rollback_error) => Err(FrankenError::internal(format!(
                        "Windows stock-visible lock acquisition failed and rollback also failed: lock={lock_error}; rollback={rollback_error}"
                    ))),
                };
            }
            self.lock_level = next;
        }
        Ok(())
    }

    fn unlock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
        if level >= self.lock_level {
            if self.ordinary_locks_are_exactly_at(self.lock_level)? {
                return Ok(());
            }
            return self.restore_ordinary_lock_level(cx, self.lock_level);
        }
        self.restore_ordinary_lock_level(cx, level)
    }

    fn lock_external_shared_snapshot(&mut self, cx: &Cx) -> Result<()> {
        if self.external_shared_snapshot_prior_level.is_some() {
            return Err(FrankenError::internal(
                "Windows external shared-snapshot fence is already held",
            ));
        }
        if self.external_maintenance_locks.is_some() {
            return Err(FrankenError::internal(
                "cannot acquire a Windows shared-snapshot fence during external maintenance",
            ));
        }

        // A read-only binding is admitted sidecar-less (bd-ypl7b); the fence
        // needs the cooperative sidecars, so materialize them on demand
        // before arming anything.
        self.ensure_os_locks()?;
        self.ensure_stock_main_locks()?;
        let prior_level = self.lock_level;
        self.external_shared_snapshot_prior_level = Some(prior_level);
        self.lock(cx, LockLevel::Shared)
    }

    fn restore_external_shared_snapshot_attempt(&mut self, cx: &Cx) -> Result<()> {
        let Some(prior_level) = self.external_shared_snapshot_prior_level else {
            return Ok(());
        };
        self.restore_ordinary_lock_level(cx, prior_level)?;
        self.external_shared_snapshot_prior_level = None;
        Ok(())
    }

    fn lock_external_maintenance(&mut self, cx: &Cx, wal_mode: bool) -> Result<()> {
        if self.external_shared_snapshot_prior_level.is_some() {
            return Err(FrankenError::internal(
                "cannot acquire Windows external maintenance while a shared-snapshot fence is held",
            ));
        }
        if self.external_maintenance_locks.is_some() {
            return Err(FrankenError::internal(
                "Windows external maintenance fence is already held",
            ));
        }

        // A read-only binding is admitted sidecar-less (bd-ypl7b); the fence
        // needs the cooperative sidecars, so materialize them on demand
        // before the attempt owns anything.
        self.ensure_os_locks()?;
        // The dedicated stock-main handle is duplicated before any cooperative
        // slot is taken so an OS duplication failure is reported while the
        // attempt still owns nothing at all.
        let stock_main_locks_preheld = self.stock_main_locks.is_some();
        self.ensure_stock_main_locks()?;
        let (write_preheld, checkpoint_preheld) = if wal_mode {
            (
                self.owns_exclusive_shm_slot(WAL_WRITE_LOCK)?,
                self.owns_exclusive_shm_slot(WAL_CKPT_LOCK)?,
            )
        } else {
            (false, false)
        };
        self.external_maintenance_locks = Some(WindowsExternalMaintenanceLocks::new(
            wal_mode,
            self.lock_level,
        ));

        // Every participant uses the same deadlock-free order: WAL
        // writer/checkpointer slots first, then main-file EXCLUSIVE. Ordinary
        // Windows SHM acquisition publishes the process-local slot and its
        // matching real `-shm` byte together under the shared state mutex.
        let mut wal_fence = self.acquire_cooperative_wal_maintenance_locks(
            cx,
            wal_mode,
            write_preheld,
            checkpoint_preheld,
        );
        if wal_fence.is_ok() {
            wal_fence = self.verify_stock_sqlite_maintenance_locks(wal_mode);
        }
        if let Err(error) = wal_fence {
            // Real WAL contention leaves the attempt holding no main-file lock,
            // so the handle this call duplicated must not outlive the failure:
            // Windows keeps a file undeletable while any handle stays open, and
            // the retained attempt is restored through the WAL slots alone. A
            // handle that predates this call belongs to an earlier lock level
            // and is left exactly as it was found.
            if !stock_main_locks_preheld {
                drop(self.stock_main_locks.take());
            }
            return Err(error);
        }
        // Past this point the WAL fence is settled, so the main-file EXCLUSIVE
        // step legitimately owns the handle; a failure there is unwound by
        // `restore_external_maintenance_attempt` via `main_restore_pending`.
        self.lock(cx, LockLevel::Exclusive)?;
        Ok(())
    }

    fn restore_external_maintenance_attempt(&mut self, cx: &Cx) -> Result<()> {
        let Some((actual_wal_mode, prior_main_level, main_restore_pending)) =
            self.external_maintenance_locks.as_ref().map(|attempt| {
                (
                    attempt.wal_mode,
                    attempt.prior_main_level,
                    attempt.main_restore_pending,
                )
            })
        else {
            return Ok(());
        };
        let mut failures = Vec::new();

        if main_restore_pending {
            match self.restore_ordinary_lock_level(cx, prior_main_level) {
                Ok(()) => {
                    if let Some(attempt) = self.external_maintenance_locks.as_mut() {
                        attempt.main_restore_pending = false;
                    }
                }
                Err(error) => failures.push(format!("main lock level: {error}")),
            }
        }

        if actual_wal_mode {
            for (slot, checkpoint_slot) in [(WAL_CKPT_LOCK, true), (WAL_WRITE_LOCK, false)] {
                let attempt = self.external_maintenance_locks.as_ref().ok_or_else(|| {
                    FrankenError::internal(
                        "Windows maintenance lost its attempt marker during restoration",
                    )
                })?;
                let acquired = if checkpoint_slot {
                    attempt.wal_checkpoint_acquired
                } else {
                    attempt.wal_write_acquired
                };
                if !acquired {
                    continue;
                }
                match self.shm_lock(cx, slot, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE) {
                    Ok(()) => {
                        if let Some(attempt) = self.external_maintenance_locks.as_mut() {
                            if checkpoint_slot {
                                attempt.wal_checkpoint_acquired = false;
                            } else {
                                attempt.wal_write_acquired = false;
                            }
                        }
                    }
                    Err(error) => failures.push(format!("WAL slot {slot}: {error}")),
                }
            }
        }

        let restoration_complete = self
            .external_maintenance_locks
            .as_ref()
            .is_some_and(WindowsExternalMaintenanceLocks::restoration_complete);
        if restoration_complete && failures.is_empty() {
            let _ = self.external_maintenance_locks.take();
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(FrankenError::internal(format!(
                "Windows external maintenance restoration was incomplete: {}",
                failures.join("; ")
            )))
        }
    }

    fn check_reserved_lock(&self, _cx: &Cx) -> Result<bool> {
        match self.os_locks.as_ref() {
            Some(os_locks) => {
                if os_locks.reserved_locked_by_other()? {
                    return Ok(true);
                }
            }
            None => {
                // Sidecar-less read-only binding (bd-ypl7b): this binding
                // retains no sidecar handles, but another process may still
                // hold the cooperative RESERVED lock through the on-disk
                // sidecar. Probe it transiently without creating anything —
                // an absent sidecar means no cooperative holder exists
                // (holders create the sidecars when they lock), and
                // LockFileEx works on a read-only handle, so this stays
                // usable on read-only media.
                self.ensure_open()?;
                let reserved_path = sqlite_reserved_lock_path(&self.path);
                let mut open_options = windows_open_options();
                match open_options.read(true).open(&reserved_path) {
                    Ok(reserved_file) => {
                        match AdvisoryFileLock::try_lock(&reserved_file, FileLockMode::Exclusive) {
                            Ok(()) => WindowsOsLockFiles::unlock_file(&reserved_file)?,
                            Err(FileLockError::AlreadyLocked) => return Ok(true),
                            Err(FileLockError::Io(err)) => return Err(FrankenError::Io(err)),
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => return Err(FrankenError::Io(err)),
                }
            }
        }
        if self.lock_level >= LockLevel::Reserved {
            return Ok(false);
        }

        let probe = self.file_ref()?.try_clone()?;
        match try_lock_stock_sqlite_range(&probe, STOCK_SQLITE_RESERVED_BYTE, 1) {
            Ok(()) => {
                unlock_stock_sqlite_range(&probe, STOCK_SQLITE_RESERVED_BYTE, 1)?;
                Ok(false)
            }
            Err(FrankenError::Busy) => Ok(true),
            Err(error) => Err(error),
        }
    }

    fn sector_size(&self) -> u32 {
        4096
    }

    fn device_characteristics(&self) -> u32 {
        SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN
    }

    #[allow(clippy::significant_drop_tightening)]
    fn shm_map(&mut self, _cx: &Cx, region: u32, size: u32, extend: bool) -> Result<ShmRegion> {
        self.ensure_open()?;
        if size == 0 {
            return Err(FrankenError::LockFailed {
                detail: "shm_map size must be > 0".to_string(),
            });
        }

        let size_usize = usize::try_from(size).map_err(|_| FrankenError::OutOfRange {
            what: "shm region size".to_string(),
            value: size.to_string(),
        })?;

        let min_len = u64::from(region)
            .checked_add(1)
            .and_then(|value| value.checked_mul(u64::from(size)))
            .ok_or_else(|| FrankenError::OutOfRange {
                what: "shm file length".to_string(),
                value: format!("region={region}, size={size}"),
            })?;

        if !extend {
            let (shm_state, mapped_region) = if let Some(state) = &self.shm_state {
                let shm_state = Arc::clone(state);
                let mapped_region = {
                    let state = shm_state
                        .lock()
                        .map_err(|_| lock_poisoned("windows shm state"))?;
                    let existing = state
                        .regions
                        .get(&region)
                        .map(ShmRegion::share)
                        .ok_or_else(|| FrankenError::CannotOpen {
                            path: self.shm_path.clone(),
                        })?;
                    if existing.len() < size_usize {
                        return Err(FrankenError::LockFailed {
                            detail: format!(
                                "shm region {region} is {} bytes, requested {size_usize} bytes without extend",
                                existing.len()
                            ),
                        });
                    }
                    existing
                };
                (shm_state, mapped_region)
            } else {
                windows_shm_table()
                    .get_existing_and_register_with(
                        &self.shm_path,
                        self.owner_id,
                        || {},
                        |state| {
                            let existing = state
                                .regions
                                .get(&region)
                                .map(ShmRegion::share)
                                .ok_or_else(|| FrankenError::CannotOpen {
                                    path: self.shm_path.clone(),
                                })?;
                            if existing.len() < size_usize {
                                return Err(FrankenError::LockFailed {
                                    detail: format!(
                                        "shm region {region} is {} bytes, requested {size_usize} bytes without extend",
                                        existing.len()
                                    ),
                                });
                            }
                            Ok(existing)
                        },
                    )?
                    .ok_or_else(|| FrankenError::CannotOpen {
                        path: self.shm_path.clone(),
                    })?
            };

            if self.shm_state.is_none() {
                self.shm_state = Some(Arc::clone(&shm_state));
            }

            debug!(
                target: "fsqlite_vfs::windows",
                region,
                size,
                path = %self.shm_path.display(),
                "mapped windows shm region"
            );

            return Ok(mapped_region);
        }

        ensure_shm_file_len(&self.shm_path, min_len)?;
        let shm_state = self.ensure_shm_state()?;
        let mapped_region = {
            let mut state = shm_state
                .lock()
                .map_err(|_| lock_poisoned("windows shm state"))?;

            let entry = state.regions.entry(region);
            let region_ref = match entry {
                std::collections::hash_map::Entry::Occupied(occupied) => {
                    let region_ref = occupied.into_mut();
                    if region_ref.len() < size_usize {
                        region_ref.try_resize_heap(size_usize)?;
                    }
                    region_ref
                }
                std::collections::hash_map::Entry::Vacant(vacant) => {
                    vacant.insert(ShmRegion::new(size_usize))
                }
            };
            region_ref.share()
        };

        debug!(
            target: "fsqlite_vfs::windows",
            region,
            size,
            path = %self.shm_path.display(),
            "mapped windows shm region"
        );

        Ok(mapped_region)
    }

    fn shm_lock(&mut self, _cx: &Cx, offset: u32, n: u32, flags: u32) -> Result<()> {
        self.ensure_open()?;
        let end = Self::validate_shm_request(offset, n)?;
        let lock_requested = flags & SQLITE_SHM_LOCK != 0;
        let unlock_requested = flags & SQLITE_SHM_UNLOCK != 0;
        if lock_requested == unlock_requested {
            return Err(FrankenError::LockFailed {
                detail: "invalid shm_lock flags (must set exactly one of LOCK/UNLOCK)".to_string(),
            });
        }

        let shared_requested = flags & SQLITE_SHM_SHARED != 0;
        let exclusive_requested = flags & SQLITE_SHM_EXCLUSIVE != 0;
        if shared_requested == exclusive_requested {
            return Err(FrankenError::LockFailed {
                detail: "invalid shm_lock flags (must set exactly one of SHARED/EXCLUSIVE)"
                    .to_string(),
            });
        }

        let shm_state = self.ensure_shm_state()?;
        let mut state = shm_state
            .lock()
            .map_err(|_| lock_poisoned("windows shm state"))?;

        if lock_requested {
            let mut acquired: Vec<u32> = Vec::new();
            for slot in offset..end {
                let changed = if exclusive_requested {
                    self.acquire_exclusive_slot(&mut state, slot)
                } else {
                    self.acquire_shared_slot(&mut state, slot).map(|()| true)
                };

                let changed = match changed {
                    Ok(changed) => changed,
                    Err(err) => {
                        let mut rollback_errors = Vec::new();
                        for acquired_slot in acquired.into_iter().rev() {
                            let rollback = if exclusive_requested {
                                self.release_exclusive_slot(&mut state, acquired_slot)
                            } else {
                                self.release_shared_slot(&mut state, acquired_slot)
                            };
                            if let Err(rollback_error) = rollback {
                                rollback_errors
                                    .push(format!("slot {acquired_slot}: {rollback_error}"));
                            }
                        }
                        if rollback_errors.is_empty() {
                            return Err(err);
                        }
                        return Err(FrankenError::internal(format!(
                            "Windows SHM acquisition failed and reverse-order unwind was incomplete: lock={err}; unwind={}",
                            rollback_errors.join("; ")
                        )));
                    }
                };
                if changed {
                    acquired.push(slot);
                }
            }
            return Ok(());
        }

        let mut release_errors = Vec::new();
        for slot in (offset..end).rev() {
            let release = if exclusive_requested {
                self.release_exclusive_slot(&mut state, slot)
            } else {
                self.release_shared_slot(&mut state, slot)
            };
            if let Err(error) = release {
                release_errors.push(format!("slot {slot}: {error}"));
            }
        }
        if release_errors.is_empty() {
            Ok(())
        } else {
            Err(FrankenError::internal(format!(
                "Windows SHM range release was incomplete: {}",
                release_errors.join("; ")
            )))
        }
    }

    fn shm_barrier(&self) {
        fence(Ordering::SeqCst);
    }

    fn shm_unmap(&mut self, _cx: &Cx, delete: bool) -> Result<()> {
        self.ensure_open()?;
        self.release_shm_owner_state(delete)
    }
}

impl Drop for WindowsFile {
    fn drop(&mut self) {
        if !self.is_closed() || self.shm_state.is_some() {
            let cx = Cx::new();
            if let Err(error) = self.close(&cx) {
                warn!(
                    path = %self.path.display(),
                    error = %error,
                    "Windows VFS cleanup failed during file drop"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future as _;
    use std::io::{BufRead as _, BufReader, Write as _};
    use std::process::{Child, ChildStdin, Command, Stdio};
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};
    use tempfile::tempdir;

    struct TempPathCleanup(PathBuf);

    impl Drop for TempPathCleanup {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    struct SqliteWalKeeper {
        child: Option<Child>,
        stdin: Option<ChildStdin>,
    }

    impl SqliteWalKeeper {
        fn start(path: &Path) -> Self {
            let mut child = Command::new("sqlite3")
                .arg(path)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .expect("spawn sqlite3 WAL keeper");
            let mut stdin = child.stdin.take().expect("keeper stdin");
            let stdout = child.stdout.take().expect("keeper stdout");
            let (ready_tx, ready_rx) = mpsc::channel();
            let reader = thread::spawn(move || {
                let mut stdout = BufReader::new(stdout);
                let mut line = String::new();
                let mut saw_wal = false;
                loop {
                    line.clear();
                    match stdout.read_line(&mut line) {
                        Ok(0) => {
                            let _ = ready_tx
                                .send(Err("sqlite3 keeper exited before readiness".to_string()));
                            return;
                        }
                        Ok(_) if line.trim().eq_ignore_ascii_case("wal") => saw_wal = true,
                        Ok(_) if line.trim() == "FSQLITE_WAL_READY" => {
                            let _ = ready_tx.send(Ok(saw_wal));
                            return;
                        }
                        Ok(_) => {}
                        Err(error) => {
                            let _ = ready_tx.send(Err(format!(
                                "could not read sqlite3 keeper readiness: {error}"
                            )));
                            return;
                        }
                    }
                }
            });

            let write_result = writeln!(
                stdin,
                ".bail on\nPRAGMA journal_mode=WAL;\nSELECT 'FSQLITE_WAL_READY';"
            )
            .and_then(|()| stdin.flush());
            let readiness = write_result
                .map_err(|error| format!("could not initialize sqlite3 keeper: {error}"))
                .and_then(|()| {
                    ready_rx
                        .recv_timeout(Duration::from_secs(10))
                        .map_err(|error| format!("sqlite3 keeper readiness timed out: {error}"))?
                });
            match readiness {
                Ok(true) => {
                    reader.join().expect("join sqlite3 readiness reader");
                    Self {
                        child: Some(child),
                        stdin: Some(stdin),
                    }
                }
                Ok(false) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    reader.join().expect("join sqlite3 readiness reader");
                    panic!("sqlite3 keeper did not confirm WAL mode");
                }
                Err(error) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    reader.join().expect("join sqlite3 readiness reader");
                    panic!("{error}");
                }
            }
        }

        fn shutdown(mut self) {
            if let Some(mut stdin) = self.stdin.take() {
                let _ = writeln!(stdin, ".quit");
                let _ = stdin.flush();
            }
            let Some(mut child) = self.child.take() else {
                return;
            };
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        assert!(status.success(), "sqlite3 WAL keeper exited with {status}");
                        return;
                    }
                    Ok(None) => thread::sleep(Duration::from_millis(10)),
                    Err(error) => {
                        let _ = child.kill();
                        let _ = child.wait();
                        panic!("could not wait for sqlite3 WAL keeper: {error}");
                    }
                }
            }
            let _ = child.kill();
            let _ = child.wait();
            panic!("sqlite3 WAL keeper did not exit within five seconds");
        }
    }

    impl Drop for SqliteWalKeeper {
        fn drop(&mut self) {
            drop(self.stdin.take());
            if let Some(mut child) = self.child.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }

    fn open_flags_create() -> VfsOpenFlags {
        VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE
    }

    fn open_stock_shm_probe(path: &Path) -> File {
        let (file, _) = open_windows_lock_sidecar(path).expect("open stock -shm lock probe");
        file
    }

    fn inject_missing_stock_shm_range(
        file: &WindowsFile,
        slot: u32,
    ) -> Arc<Mutex<WindowsShmState>> {
        let state = file.shm_state.as_ref().map(Arc::clone).expect("SHM state");
        let lock_byte = WindowsFile::stock_shm_lock_byte(slot).expect("stock SHM lock byte");
        {
            let state_guard = state.lock().expect("lock SHM state for fault injection");
            let shm_file = state_guard
                .stock_shm_file
                .as_ref()
                .expect("aggregate stock -shm handle");
            unlock_stock_sqlite_range_strict(shm_file, lock_byte, 1)
                .expect("inject missing aggregate SHM range");
        }
        state
    }

    fn path_with_suffix(path: &Path, suffix: &str) -> PathBuf {
        let mut suffixed = path.as_os_str().to_owned();
        suffixed.push(suffix);
        PathBuf::from(suffixed)
    }

    #[test]
    fn shm_table_registration_and_orphan_removal_preserve_one_lock_domain() {
        let path = PathBuf::from("registration-race-shm");
        let table = WindowsShmTable::new();

        let old = table
            .get_or_create_and_register(&path, 1)
            .expect("register initial owner");
        old.lock().expect("old SHM state").owner_refs.remove(&1);

        // Model the exact last-close/new-open interleaving: the old owner has
        // become invisible but has not removed the table entry yet. The new
        // opener must publish its owner while the table mutex is still held,
        // so orphan removal cannot pass between lookup and registration.
        let reopened = table
            .get_or_create_and_register_with(&path, 2, || {
                assert!(matches!(
                    table.map.try_lock(),
                    Err(std::sync::TryLockError::WouldBlock)
                ));
            })
            .expect("register replacement owner");
        assert!(Arc::ptr_eq(&old, &reopened));
        table
            .remove_if_orphaned(&path, &old)
            .expect("retain registered state");

        let mapped = table
            .get(&path)
            .expect("read SHM table")
            .expect("registered state remains mapped");
        assert!(Arc::ptr_eq(&mapped, &reopened));
        assert_eq!(
            mapped.lock().expect("mapped SHM state").owner_refs.get(&2),
            Some(&1)
        );

        // Also prove that a delayed cleanup from an older file generation can
        // never erase a newer generation that reused the same pathname.
        mapped
            .lock()
            .expect("mapped SHM state")
            .owner_refs
            .remove(&2);
        table
            .remove_if_orphaned(&path, &mapped)
            .expect("remove old generation");
        let replacement = table
            .get_or_create_and_register(&path, 3)
            .expect("register new generation");
        assert!(!Arc::ptr_eq(&old, &replacement));
        replacement
            .lock()
            .expect("replacement SHM state")
            .owner_refs
            .remove(&3);
        table
            .remove_if_orphaned(&path, &old)
            .expect("ignore stale cleanup");

        let current = table
            .get(&path)
            .expect("read SHM table")
            .expect("new generation remains mapped");
        assert!(Arc::ptr_eq(&current, &replacement));
    }

    #[test]
    fn shm_table_nonextending_registration_is_atomic_with_last_close() {
        let path = PathBuf::from("nonextending-registration-race-shm");
        let table = WindowsShmTable::new();
        let old = table
            .get_or_create_and_register(&path, 1)
            .expect("register initial owner");
        {
            let mut state = old.lock().expect("old SHM state");
            state.regions.insert(0, ShmRegion::new(64));
            state.owner_refs.remove(&1);
        }

        // Model xShmMap(extend=false) racing the old generation's final
        // cleanup.  Lookup, region validation, and the replacement owner ref
        // must all happen while the table mutex excludes orphan removal.
        let (reopened, mapped) = table
            .get_existing_and_register_with(
                &path,
                2,
                || {
                    assert!(matches!(
                        table.map.try_lock(),
                        Err(std::sync::TryLockError::WouldBlock)
                    ));
                },
                |state| {
                    state
                        .regions
                        .get(&0)
                        .map(ShmRegion::share)
                        .ok_or_else(|| FrankenError::CannotOpen { path: path.clone() })
                },
            )
            .expect("register non-extending owner")
            .expect("existing state");
        assert_eq!(mapped.len(), 64);
        assert!(Arc::ptr_eq(&old, &reopened));

        table
            .remove_if_orphaned(&path, &old)
            .expect("registered replacement prevents orphan removal");
        let current = table
            .get(&path)
            .expect("read SHM table")
            .expect("replacement registration remains mapped");
        assert!(Arc::ptr_eq(&current, &reopened));
        assert_eq!(
            current
                .lock()
                .expect("current SHM state")
                .owner_refs
                .get(&2),
            Some(&1)
        );
    }

    #[test]
    fn test_windowsvfs_create_and_write() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("create_write.db");
        let vfs = WindowsVfs::new();
        let (file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open file");

        crate::block_on_test_io(&cx, file.write(&cx, b"hello windows", 0)).expect("write");
        let mut buf = [0_u8; 13];
        let n = crate::block_on_test_io(&cx, file.read(&cx, &mut buf, 0)).expect("read");
        assert_eq!(n, 13);
        assert_eq!(&buf, b"hello windows");
    }

    #[test]
    fn test_read_only_binding_materializes_sidecars_for_external_snapshot_fence() {
        // beads_rust GH#438: a sidecar-less read-only binding (bd-ypl7b) must
        // still be able to take the external shared-snapshot fence — the
        // fence opens the advisory sidecars on demand instead of failing
        // with "windows lock files are closed".
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("ro-fence.db");
        {
            let vfs = WindowsVfs::new();
            let (file, _) = vfs
                .open(&cx, Some(&path), open_flags_create())
                .expect("create db rw");
            crate::block_on_test_io(&cx, file.write(&cx, b"seed", 0)).expect("seed write");
        }
        // The rw open created sidecars; remove them so the RO open starts
        // from the sidecar-less on-disk state a fresh checkout has.
        for sidecar in [
            sqlite_shared_lock_path(&path),
            sqlite_reserved_lock_path(&path),
            sqlite_pending_lock_path(&path),
        ] {
            let _ = fs::remove_file(&sidecar);
        }

        let vfs = WindowsVfs::new();
        let (mut file, _) = vfs
            .open(&cx, Some(&path), VfsOpenFlags::MAIN_DB)
            .expect("open read-only");
        assert!(
            !sqlite_shared_lock_path(&path).exists(),
            "read-only open must stay sidecar-less (bd-ypl7b)"
        );

        file.lock_external_shared_snapshot(&cx)
            .expect("external snapshot fence on a read-only binding");
        assert!(
            sqlite_shared_lock_path(&path).exists(),
            "fence acquisition materializes the advisory sidecars on demand"
        );
        file.restore_external_shared_snapshot_attempt(&cx)
            .expect("restore fence");

        let mut buf = [0_u8; 4];
        let n = crate::block_on_test_io(&cx, file.read(&cx, &mut buf, 0)).expect("read");
        assert_eq!(n, 4);
        assert_eq!(&buf, b"seed");
    }

    #[test]
    fn test_windowsvfs_full_file_identity_is_handle_bound() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("identity.db");
        let alias_path = dir.path().join("identity-alias.db");
        let other_path = dir.path().join("other.db");
        let vfs = WindowsVfs::new();
        let (file_a, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open first handle");
        fs::hard_link(&path, &alias_path).expect("create hard-link alias");
        let (file_b, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open second handle");
        let (alias_file, _) = vfs
            .open(&cx, Some(&alias_path), open_flags_create())
            .expect("open hard-link alias");
        let (other_file, _) = vfs
            .open(&cx, Some(&other_path), open_flags_create())
            .expect("open distinct file");

        let identity_a = file_a
            .file_identity()
            .expect("read first identity")
            .expect("Windows file identity should be available");
        let identity_b = file_b
            .file_identity()
            .expect("read second identity")
            .expect("Windows file identity should be available");
        let alias_identity = alias_file
            .file_identity()
            .expect("read alias identity")
            .expect("Windows file identity should be available");
        let other_identity = other_file
            .file_identity()
            .expect("read distinct identity")
            .expect("Windows file identity should be available");

        assert_eq!(identity_a, identity_b);
        assert_eq!(identity_a, alias_identity);
        assert_ne!(identity_a, other_identity);
    }

    #[test]
    fn test_windowsvfs_expected_identity_mismatch_precedes_side_effects() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("identity-guard.db");
        let other_path = dir.path().join("other.db");
        let journal_path = path_with_suffix(&path, "-journal");
        let wal_path = path_with_suffix(&path, "-wal");
        let shm_path = sqlite_shm_path(&path);

        fs::write(&path, b"main sentinel").expect("seed main sentinel");
        fs::write(&other_path, b"other sentinel").expect("seed other file");
        fs::write(&journal_path, b"journal sentinel").expect("seed journal sentinel");
        fs::write(&wal_path, b"wal sentinel").expect("seed WAL sentinel");
        fs::write(&shm_path, b"shm sentinel").expect("seed SHM sentinel");

        let expected_identity =
            FileIdentity::from_file(&File::open(&other_path).expect("open other identity handle"))
                .expect("query other identity")
                .expect("Windows file identity must be available");
        let actual_identity =
            FileIdentity::from_file(&File::open(&path).expect("open main identity handle"))
                .expect("query main identity")
                .expect("Windows file identity must be available");
        assert_ne!(expected_identity, actual_identity);

        let main_before = fs::read(&path).expect("snapshot main sentinel");
        let journal_before = fs::read(&journal_path).expect("snapshot journal sentinel");
        let wal_before = fs::read(&wal_path).expect("snapshot WAL sentinel");
        let shm_before = fs::read(&shm_path).expect("snapshot SHM sentinel");
        for sidecar in windows_lock_sidecar_paths(&path) {
            assert!(!sidecar.exists(), "lock sidecar must start absent");
        }

        let error = WindowsVfs::new()
            .open_with_expected_identity(
                &cx,
                &path,
                VfsOpenFlags::MAIN_DB | VfsOpenFlags::READWRITE,
                expected_identity,
            )
            .expect_err("wrong expected identity must refuse the open");

        assert!(matches!(error, FrankenError::CannotOpen { .. }));
        assert_eq!(fs::read(&path).unwrap(), main_before);
        assert_eq!(fs::read(&journal_path).unwrap(), journal_before);
        assert_eq!(fs::read(&wal_path).unwrap(), wal_before);
        assert_eq!(fs::read(&shm_path).unwrap(), shm_before);
        for sidecar in windows_lock_sidecar_paths(&path) {
            assert!(
                !sidecar.exists(),
                "identity refusal must not create {}",
                sidecar.display()
            );
        }
    }

    #[test]
    fn test_windowsvfs_read_exact_at() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("read_at.db");
        let vfs = WindowsVfs::new();
        let (file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open file");
        crate::block_on_test_io(&cx, file.write(&cx, b"0123456789", 0)).expect("write");

        let mut buf = [0_u8; 4];
        let n = crate::block_on_test_io(&cx, file.read(&cx, &mut buf, 3)).expect("read");
        assert_eq!(n, 4);
        assert_eq!(&buf, b"3456");
    }

    #[test]
    fn test_windowsvfs_write_all_at() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("write_at.db");
        let vfs = WindowsVfs::new();
        let (file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open file");
        crate::block_on_test_io(&cx, file.write(&cx, b"abcdefghij", 0)).expect("write base");
        crate::block_on_test_io(&cx, file.write(&cx, b"WXYZ", 2)).expect("write overlay");

        let mut buf = [0_u8; 10];
        let n = crate::block_on_test_io(&cx, file.read(&cx, &mut buf, 0)).expect("read");
        assert_eq!(n, 10);
        assert_eq!(&buf, b"abWXYZghij");
    }

    #[test]
    fn test_windowsvfs_file_size_and_truncate() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("size.db");
        let vfs = WindowsVfs::new();
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open file");
        crate::block_on_test_io(&cx, file.write(&cx, &[7_u8; 4096], 0)).expect("write");
        assert_eq!(file.file_size(&cx).expect("size"), 4096);

        file.truncate(&cx, 1024).expect("truncate");
        assert_eq!(file.file_size(&cx).expect("size"), 1024);
    }

    #[test]
    fn test_windowsvfs_file_size() {
        test_windowsvfs_file_size_and_truncate();
    }

    #[test]
    fn test_windowsvfs_truncate() {
        test_windowsvfs_file_size_and_truncate();
    }

    #[test]
    fn test_windowsvfs_shared_memory_create_and_cross_handle() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("shm.db");
        let vfs = WindowsVfs::new();
        let (mut file_a, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open A");
        let (mut file_b, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open B");

        let region_a = file_a.shm_map(&cx, 0, 32 * 1024, true).expect("map A");
        {
            let mut guard = region_a.lock();
            guard[0] = 0xAA;
            guard[1] = 0x55;
        }

        let region_b = file_b.shm_map(&cx, 0, 32 * 1024, false).expect("map B");
        let guard = region_b.lock();
        assert_eq!(guard[0], 0xAA);
        assert_eq!(guard[1], 0x55);
        drop(guard);
    }

    #[test]
    fn test_windowsvfs_shared_memory_create() {
        test_windowsvfs_shared_memory_create_and_cross_handle();
    }

    #[test]
    fn test_windowsvfs_shared_memory_cross_handle() {
        test_windowsvfs_shared_memory_create_and_cross_handle();
    }

    #[test]
    fn test_windowsvfs_shm_resize_preserves_existing_mappings() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("shm_resize.db");
        let vfs = WindowsVfs::new();
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open file");

        let region_small = file.shm_map(&cx, 0, 32, true).expect("initial map");
        region_small.write_u32_le(0, 0x1122_3344).unwrap();

        let region_large = file.shm_map(&cx, 0, 64, true).expect("resized map");
        region_large.write_u32_le(0, 0x5566_7788).unwrap();
        region_large.write_u32_le(32, 0xAABB_CCDD).unwrap();

        assert_eq!(
            region_small.read_u32_le(0).unwrap(),
            0x5566_7788,
            "resizing must preserve shared backing for existing mappings"
        );
        assert_eq!(region_large.read_u32_le(32).unwrap(), 0xAABB_CCDD);
    }

    #[test]
    fn test_windowsvfs_shm_map_extend_false_rejects_missing_without_side_effects() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("shm_missing_no_extend.db");
        let vfs = WindowsVfs::new();
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open file");
        let shm_path = file.shm_path.clone();

        let err = file.shm_map(&cx, 2, 64, false).unwrap_err();
        assert!(
            matches!(err, FrankenError::CannotOpen { .. }),
            "missing non-extend shm_map should report CannotOpen, got {err:?}"
        );
        assert!(
            file.shm_state.is_none(),
            "failed non-extend shm_map must not register shm owner state"
        );
        assert!(
            windows_shm_table().get(&shm_path).unwrap().is_none(),
            "failed non-extend shm_map must not create a shared state entry"
        );
        assert!(
            !shm_path.exists(),
            "failed non-extend shm_map must not create a -shm file"
        );
    }

    #[test]
    fn test_windowsvfs_reserved_lock_conflicts_across_handles() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("reserved_lock.db");
        let vfs = WindowsVfs::new();
        let (mut file_a, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open A");
        let (mut file_b, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open B");

        file_a.lock(&cx, LockLevel::Shared).expect("A shared");
        file_a.lock(&cx, LockLevel::Reserved).expect("A reserved");
        assert!(
            !file_a.check_reserved_lock(&cx).unwrap(),
            "a handle should not report its own RESERVED lock as external"
        );
        assert!(
            file_b.check_reserved_lock(&cx).unwrap(),
            "other handles must observe a RESERVED-or-higher sidecar lock"
        );
        assert!(
            matches!(
                file_b.lock(&cx, LockLevel::Reserved),
                Err(FrankenError::Busy)
            ),
            "second RESERVED locker must be rejected"
        );

        file_a.unlock(&cx, LockLevel::None).expect("release A");
        file_b.lock(&cx, LockLevel::Reserved).expect("B reserved");
    }

    #[test]
    fn test_windowsvfs_exclusive_lock_conflicts_with_other_shared_handle() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("exclusive_vs_shared.db");
        let vfs = WindowsVfs::new();
        let (mut file_a, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open A");
        let (mut file_b, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open B");

        file_a.lock(&cx, LockLevel::Shared).expect("A shared");
        file_b.lock(&cx, LockLevel::Shared).expect("B shared");
        assert!(
            matches!(
                file_a.lock(&cx, LockLevel::Exclusive),
                Err(FrankenError::Busy)
            ),
            "EXCLUSIVE must upgrade the shared sidecar and conflict with another SHARED holder"
        );
        assert_eq!(
            file_a.lock_level,
            LockLevel::Shared,
            "failed EXCLUSIVE upgrade should roll back to the prior lock level"
        );
        file_a
            .lock(&cx, LockLevel::Reserved)
            .expect("failed exclusive upgrade must not strand RESERVED/PENDING sidecars");
    }

    #[test]
    fn test_stock_main_lock_level_is_recomputed_from_held_ranges() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("stock_level_recompute.db");
        let mut options = windows_open_options();
        let file = options
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .expect("open lock-state test file");
        let mut locks = WindowsStockMainLocks::new(file);

        locks.lock_level = LockLevel::Exclusive;
        locks.recompute_lock_level();
        assert_eq!(locks.lock_level, LockLevel::None);

        locks.shared_range = true;
        locks.recompute_lock_level();
        assert_eq!(locks.lock_level, LockLevel::Shared);

        locks.reserved = true;
        locks.recompute_lock_level();
        assert_eq!(locks.lock_level, LockLevel::Reserved);

        locks.pending = true;
        locks.recompute_lock_level();
        assert_eq!(locks.lock_level, LockLevel::Pending);

        locks.shared_range_exclusive = true;
        locks.recompute_lock_level();
        assert_eq!(locks.lock_level, LockLevel::Exclusive);

        locks.shared_range = false;
        locks.recompute_lock_level();
        assert_eq!(
            locks.lock_level,
            LockLevel::None,
            "upper bytes without the SHARED range are not a reusable SQLite lock prefix"
        );

        locks.shared_range = true;
        locks.reserved = false;
        locks.recompute_lock_level();
        assert_eq!(
            locks.lock_level,
            LockLevel::Shared,
            "PENDING/EXCLUSIVE remnants without RESERVED must not overstate the reusable level"
        );
    }

    #[test]
    fn test_ordinary_writer_levels_contend_on_stock_sqlite_bytes() {
        let cx = Cx::new();
        let dir = tempdir().unwrap();
        let path = dir.path().join("ordinary_stock_writer_levels.db");
        let vfs = WindowsVfs::new();
        let (mut writer, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open FrankenSQLite writer");
        let mut blocker_options = windows_open_options();
        let blocker = blocker_options
            .read(true)
            .write(true)
            .open(&path)
            .expect("open independent stock-lock blocker");

        writer.lock(&cx, LockLevel::Shared).expect("shared");
        try_lock_stock_sqlite_range(&blocker, STOCK_SQLITE_RESERVED_BYTE, 1)
            .expect("hold stock RESERVED byte");
        assert!(matches!(
            writer.lock(&cx, LockLevel::Reserved),
            Err(FrankenError::Busy)
        ));
        assert_eq!(writer.lock_level, LockLevel::Shared);
        assert_eq!(
            writer.stock_main_locks.as_ref().unwrap().lock_level,
            LockLevel::Shared
        );
        unlock_stock_sqlite_range(&blocker, STOCK_SQLITE_RESERVED_BYTE, 1)
            .expect("release stock RESERVED blocker");

        writer.lock(&cx, LockLevel::Reserved).expect("reserved");
        try_lock_stock_sqlite_range(&blocker, STOCK_SQLITE_PENDING_BYTE, 1)
            .expect("hold stock PENDING byte");
        assert!(matches!(
            writer.lock(&cx, LockLevel::Pending),
            Err(FrankenError::Busy)
        ));
        assert_eq!(writer.lock_level, LockLevel::Reserved);
        let stock = writer.stock_main_locks.as_ref().unwrap();
        assert_eq!(stock.lock_level, LockLevel::Reserved);
        assert!(stock.reserved);
        assert!(!stock.pending);
        unlock_stock_sqlite_range(&blocker, STOCK_SQLITE_PENDING_BYTE, 1)
            .expect("release stock PENDING blocker");

        writer.lock(&cx, LockLevel::Pending).expect("pending");
        assert!(matches!(
            try_lock_stock_sqlite_range(&blocker, STOCK_SQLITE_RESERVED_BYTE, 1),
            Err(FrankenError::Busy)
        ));
        assert!(matches!(
            try_lock_stock_sqlite_range(&blocker, STOCK_SQLITE_PENDING_BYTE, 1),
            Err(FrankenError::Busy)
        ));
        writer.unlock(&cx, LockLevel::None).expect("unlock writer");
        try_lock_stock_sqlite_range(&blocker, STOCK_SQLITE_RESERVED_BYTE, 1)
            .expect("ordinary unlock must release stock RESERVED");
        unlock_stock_sqlite_range(&blocker, STOCK_SQLITE_RESERVED_BYTE, 1).unwrap();
        try_lock_stock_sqlite_range(&blocker, STOCK_SQLITE_PENDING_BYTE, 1)
            .expect("ordinary unlock must release stock PENDING");
        unlock_stock_sqlite_range(&blocker, STOCK_SQLITE_PENDING_BYTE, 1).unwrap();
        writer.close(&cx).unwrap();
    }

    #[test]
    fn test_ordinary_exclusive_conflict_restores_stock_reserved_snapshot() {
        let cx = Cx::new();
        let dir = tempdir().unwrap();
        let path = dir.path().join("ordinary_stock_exclusive_unwind.db");
        let vfs = WindowsVfs::new();
        let (mut writer, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open FrankenSQLite writer");
        let mut reader_options = windows_open_options();
        let stock_reader = reader_options
            .read(true)
            .write(true)
            .open(&path)
            .expect("open independent stock reader");

        writer
            .lock(&cx, LockLevel::Reserved)
            .expect("acquire FrankenSQLite RESERVED");
        try_lock_stock_sqlite_shared_range(
            &stock_reader,
            STOCK_SQLITE_SHARED_FIRST,
            STOCK_SQLITE_SHARED_SIZE,
        )
        .expect("hold stock SHARED reader range");

        assert!(matches!(
            writer.lock(&cx, LockLevel::Exclusive),
            Err(FrankenError::Busy)
        ));
        assert_eq!(writer.lock_level, LockLevel::Reserved);
        let stock = writer.stock_main_locks.as_ref().unwrap();
        assert_eq!(stock.lock_level, LockLevel::Reserved);
        assert!(stock.shared_range);
        assert!(!stock.shared_range_exclusive);
        assert!(stock.reserved);
        assert!(!stock.pending);
        try_lock_stock_sqlite_range(&stock_reader, STOCK_SQLITE_PENDING_BYTE, 1)
            .expect("failed EXCLUSIVE must release stock PENDING");
        unlock_stock_sqlite_range(&stock_reader, STOCK_SQLITE_PENDING_BYTE, 1).unwrap();

        unlock_stock_sqlite_range(
            &stock_reader,
            STOCK_SQLITE_SHARED_FIRST,
            STOCK_SQLITE_SHARED_SIZE,
        )
        .expect("release stock reader");
        writer
            .lock(&cx, LockLevel::Exclusive)
            .expect("exclusive retry after reader drains");
        let stock = writer.stock_main_locks.as_ref().unwrap();
        assert_eq!(stock.lock_level, LockLevel::Exclusive);
        assert!(stock.pending);
        assert!(stock.reserved);
        assert!(stock.shared_range_exclusive);
        assert!(matches!(
            try_lock_stock_sqlite_range(
                &stock_reader,
                STOCK_SQLITE_SHARED_FIRST,
                STOCK_SQLITE_SHARED_SIZE,
            ),
            Err(FrankenError::Busy)
        ));

        writer.unlock(&cx, LockLevel::None).expect("unlock writer");
        try_lock_stock_sqlite_range(
            &stock_reader,
            STOCK_SQLITE_SHARED_FIRST,
            STOCK_SQLITE_SHARED_SIZE,
        )
        .expect("ordinary unlock must release stock EXCLUSIVE range");
        unlock_stock_sqlite_range(
            &stock_reader,
            STOCK_SQLITE_SHARED_FIRST,
            STOCK_SQLITE_SHARED_SIZE,
        )
        .unwrap();
        writer.close(&cx).unwrap();
    }

    #[test]
    fn test_external_attempt_restore_before_acquisition_is_noop() {
        let cx = Cx::new();
        let dir = tempdir().unwrap();
        let path = dir.path().join("external_restore_before_attempt.db");
        let vfs = WindowsVfs::new();
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open external-fence handle");

        file.restore_external_shared_snapshot_attempt(&cx)
            .expect("snapshot restoration before acquisition");
        file.restore_external_maintenance_attempt(&cx)
            .expect("maintenance restoration before acquisition");

        assert_eq!(file.lock_level, LockLevel::None);
        assert!(file.external_shared_snapshot_prior_level.is_none());
        assert!(file.external_maintenance_locks.is_none());
        assert!(
            file.shm_state.is_none(),
            "a restoration before acquisition must not create SHM state"
        );
        file.close(&cx).unwrap();
    }

    #[test]
    fn test_stock_main_clone_failure_precedes_cooperative_external_locks() {
        let cx = Cx::new();
        let dir = tempdir().unwrap();
        let path = dir.path().join("external_clone_failure.db");
        let vfs = WindowsVfs::new();
        let (mut attempted, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open attempted external-fence handle");
        let (mut probe, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open independent lock probe");

        attempted.fail_next_stock_main_clone = true;
        let snapshot_error = attempted
            .lock_external_shared_snapshot(&cx)
            .expect_err("injected stock-main handle duplication must fail");
        assert!(
            snapshot_error
                .to_string()
                .contains("injected Windows main-handle clone failure")
        );
        assert_eq!(attempted.lock_level, LockLevel::None);
        assert!(attempted.stock_main_locks.is_none());
        assert!(attempted.external_shared_snapshot_prior_level.is_none());
        assert!(
            attempted
                .os_locks
                .as_ref()
                .is_some_and(|locks| locks.held_levels == [false; 4])
        );
        assert!(attempted.shm_state.is_none());

        probe
            .lock(&cx, LockLevel::Exclusive)
            .expect("failed snapshot preflight must leave no cooperative lock");
        probe
            .unlock(&cx, LockLevel::None)
            .expect("release snapshot probe");
        attempted
            .restore_external_shared_snapshot_attempt(&cx)
            .expect("preflight failure leaves restoration as an idempotent no-op");

        attempted.fail_next_stock_main_clone = true;
        let maintenance_error = attempted
            .lock_external_maintenance(&cx, true)
            .expect_err("injected stock-main handle duplication must fail");
        assert!(
            maintenance_error
                .to_string()
                .contains("injected Windows main-handle clone failure")
        );
        assert_eq!(attempted.lock_level, LockLevel::None);
        assert!(attempted.stock_main_locks.is_none());
        assert!(attempted.external_maintenance_locks.is_none());
        assert!(
            attempted
                .os_locks
                .as_ref()
                .is_some_and(|locks| locks.held_levels == [false; 4])
        );
        assert!(
            attempted.shm_state.is_none(),
            "maintenance preflight must fail before opening or locking -shm"
        );
        assert!(
            !sqlite_shm_path(&path).exists(),
            "maintenance preflight must not create a stock-visible -shm file"
        );

        probe
            .lock(&cx, LockLevel::Exclusive)
            .expect("failed maintenance preflight must leave no cooperative lock");
        probe
            .unlock(&cx, LockLevel::None)
            .expect("release maintenance probe");
        attempted
            .restore_external_maintenance_attempt(&cx)
            .expect("preflight failure leaves restoration as an idempotent no-op");
        attempted
            .lock_external_shared_snapshot(&cx)
            .expect("one-shot clone failures must permit a clean snapshot retry");
        attempted
            .restore_external_shared_snapshot_attempt(&cx)
            .expect("restore snapshot retry");
        attempted
            .lock_external_maintenance(&cx, true)
            .expect("one-shot clone failure must permit a clean maintenance retry");
        attempted
            .restore_external_maintenance_attempt(&cx)
            .expect("restore maintenance retry");

        attempted.close(&cx).unwrap();
        probe.close(&cx).unwrap();
    }

    #[test]
    fn test_external_attempt_restore_is_idempotent_after_success() {
        let cx = Cx::new();
        let dir = tempdir().unwrap();
        let path = dir.path().join("external_double_restore.db");
        let vfs = WindowsVfs::new();
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open external-fence handle");

        file.lock_external_shared_snapshot(&cx)
            .expect("acquire snapshot fence");
        file.restore_external_shared_snapshot_attempt(&cx)
            .expect("first snapshot restoration");
        file.restore_external_shared_snapshot_attempt(&cx)
            .expect("second snapshot restoration");

        file.lock_external_maintenance(&cx, false)
            .expect("acquire maintenance fence");
        file.restore_external_maintenance_attempt(&cx)
            .expect("first maintenance restoration");
        file.restore_external_maintenance_attempt(&cx)
            .expect("second maintenance restoration");

        assert_eq!(file.lock_level, LockLevel::None);
        assert!(file.external_shared_snapshot_prior_level.is_none());
        assert!(file.external_maintenance_locks.is_none());
        file.close(&cx).unwrap();
    }

    #[test]
    fn test_dropped_pending_external_maintenance_retry_releases_cooperative_surface() {
        let cx = Cx::new();
        let dir = tempdir().unwrap();
        let path = dir.path().join("external_dropped_restore_retry.db");
        let vfs = WindowsVfs::new();
        let (mut attempted, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open external-maintenance handle");

        attempted
            .lock_external_maintenance(&cx, false)
            .expect("acquire external-maintenance fence");
        attempted
            .os_locks
            .as_mut()
            .expect("open handle has cooperative ordinary locks")
            .fail_next_unlock = true;

        let mut restoration = Box::pin(async {
            let error = attempted
                .restore_external_maintenance_attempt(&cx)
                .expect_err("first cooperative restoration must fail once");
            assert!(
                error
                    .to_string()
                    .contains("injected Windows cooperative ordinary-lock unlock failure"),
                "unexpected injected restoration error: {error}"
            );
            std::future::pending::<()>().await;
        });
        let waker = std::task::Waker::noop();
        let mut task_cx = std::task::Context::from_waker(waker);
        assert!(
            matches!(
                restoration.as_mut().poll(&mut task_cx),
                std::task::Poll::Pending
            ),
            "the restoration attempt must truly become pending before cancellation"
        );
        drop(restoration);

        assert!(
            attempted.external_maintenance_locks.is_some(),
            "a failed restoration must retain its retry marker"
        );
        assert!(
            attempted
                .stock_main_locks
                .as_ref()
                .is_some_and(|locks| locks.is_exactly_at(LockLevel::None)),
            "the first attempt should have restored the stock-visible surface"
        );
        assert!(
            attempted
                .os_locks
                .as_ref()
                .is_some_and(|locks| locks.is_exactly_at(LockLevel::Exclusive)),
            "the injected failure should leave the cooperative surface outstanding"
        );
        assert_eq!(
            attempted.lock_level,
            LockLevel::Exclusive,
            "the aggregate must remain at the last state proven exact on both surfaces"
        );

        attempted
            .restore_external_maintenance_attempt(&cx)
            .expect("retry exact restoration after the dropped future");
        assert!(attempted.external_maintenance_locks.is_none());
        assert!(
            attempted
                .ordinary_locks_are_exactly_at(LockLevel::None)
                .expect("inspect exact restored surfaces")
        );
        assert_eq!(attempted.lock_level, LockLevel::None);

        let (mut probe, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open independent post-retry probe");
        probe
            .lock_external_maintenance(&cx, false)
            .expect("the retry must leave no cooperative lock stranded");
        probe
            .restore_external_maintenance_attempt(&cx)
            .expect("restore independent probe");

        probe.close(&cx).unwrap();
        attempted.close(&cx).unwrap();
    }

    #[test]
    fn test_external_maintenance_restore_uses_recorded_wal_mode() {
        let cx = Cx::new();
        let dir = tempdir().unwrap();
        let path = dir.path().join("external_maintenance_recorded_mode.db");
        let vfs = WindowsVfs::new();
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open external-fence handle");

        file.lock_external_maintenance(&cx, true)
            .expect("acquire WAL maintenance fence");
        file.restore_external_maintenance_attempt(&cx)
            .expect("restore every surface recorded by the backend attempt");
        assert_eq!(file.lock_level, LockLevel::None);
        assert!(
            !file
                .owns_exclusive_shm_slot(WAL_WRITE_LOCK)
                .expect("inspect restored write slot")
        );
        assert!(
            !file
                .owns_exclusive_shm_slot(WAL_CKPT_LOCK)
                .expect("inspect restored checkpoint slot")
        );
        assert!(file.external_maintenance_locks.is_none());
        file.restore_external_maintenance_attempt(&cx)
            .expect("repeated restoration remains a no-op");
        file.close(&cx).unwrap();
    }

    #[test]
    fn test_external_shared_snapshot_uses_stock_sqlite_main_range() {
        let cx = Cx::new();
        let dir = tempdir().unwrap();
        let path = dir.path().join("stock_shared_snapshot.db");
        let vfs = WindowsVfs::new();
        let (mut snapshot, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open snapshot handle");
        let mut probe_options = windows_open_options();
        let probe = probe_options
            .read(true)
            .write(true)
            .open(&path)
            .expect("open independent stock-lock probe");

        snapshot
            .lock_external_shared_snapshot(&cx)
            .expect("acquire external shared-snapshot fence");
        assert!(matches!(
            try_lock_stock_sqlite_range(
                &probe,
                STOCK_SQLITE_SHARED_FIRST,
                STOCK_SQLITE_SHARED_SIZE,
            ),
            Err(FrankenError::Busy)
        ));

        // Stock SQLite releases the transient PENDING participation after the
        // shared range is held, allowing a writer to enter PENDING while it
        // waits for readers to drain.
        try_lock_stock_sqlite_range(&probe, STOCK_SQLITE_PENDING_BYTE, 1)
            .expect("snapshot must not retain the transient pending byte");
        unlock_stock_sqlite_range(&probe, STOCK_SQLITE_PENDING_BYTE, 1)
            .expect("unlock pending probe");

        snapshot
            .restore_external_shared_snapshot_attempt(&cx)
            .expect("release external shared-snapshot fence");
        try_lock_stock_sqlite_range(&probe, STOCK_SQLITE_SHARED_FIRST, STOCK_SQLITE_SHARED_SIZE)
            .expect("released shared range must be exclusively acquirable");
        unlock_stock_sqlite_range(&probe, STOCK_SQLITE_SHARED_FIRST, STOCK_SQLITE_SHARED_SIZE)
            .expect("unlock shared-range probe");
        snapshot.close(&cx).unwrap();
    }

    #[test]
    fn test_external_shared_snapshot_partial_acquisition_unwinds_all_locks() {
        let cx = Cx::new();
        let dir = tempdir().unwrap();
        let path = dir.path().join("stock_shared_snapshot_unwind.db");
        let vfs = WindowsVfs::new();
        let (mut snapshot, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open snapshot handle");
        let mut blocker_options = windows_open_options();
        let blocker = blocker_options
            .read(true)
            .write(true)
            .open(&path)
            .expect("open independent stock-lock blocker");
        try_lock_stock_sqlite_range(
            &blocker,
            STOCK_SQLITE_SHARED_FIRST,
            STOCK_SQLITE_SHARED_SIZE,
        )
        .expect("hold stock exclusive shared range");

        assert!(matches!(
            snapshot.lock_external_shared_snapshot(&cx),
            Err(FrankenError::Busy)
        ));
        assert_eq!(snapshot.lock_level, LockLevel::None);
        assert_eq!(
            snapshot.external_shared_snapshot_prior_level,
            Some(LockLevel::None),
            "a failed acquisition must retain its exact pre-attempt baseline until restoration succeeds"
        );
        let stock = snapshot
            .stock_main_locks
            .as_ref()
            .expect("failed acquisition keeps a reusable dedicated handle");
        assert_eq!(stock.lock_level, LockLevel::None);
        assert!(!stock.pending);
        assert!(!stock.reserved);
        assert!(!stock.shared_range);
        try_lock_stock_sqlite_range(&blocker, STOCK_SQLITE_PENDING_BYTE, 1)
            .expect("failed snapshot acquisition must release pending");
        unlock_stock_sqlite_range(&blocker, STOCK_SQLITE_PENDING_BYTE, 1)
            .expect("unlock pending probe");

        snapshot
            .restore_external_shared_snapshot_attempt(&cx)
            .expect("restore failed snapshot attempt");
        assert!(
            snapshot.external_shared_snapshot_prior_level.is_none(),
            "successful restoration must disarm the acquisition-attempt marker"
        );

        unlock_stock_sqlite_range(
            &blocker,
            STOCK_SQLITE_SHARED_FIRST,
            STOCK_SQLITE_SHARED_SIZE,
        )
        .expect("release shared-range blocker");
        snapshot
            .lock_external_shared_snapshot(&cx)
            .expect("retry after contention must succeed");
        snapshot
            .restore_external_shared_snapshot_attempt(&cx)
            .expect("retry fence must release cleanly");
        snapshot.close(&cx).unwrap();
    }

    #[test]
    fn test_external_shared_snapshot_conflicts_with_external_maintenance() {
        let cx = Cx::new();
        let dir = tempdir().unwrap();
        let path = dir.path().join("shared_snapshot_vs_maintenance.db");
        let vfs = WindowsVfs::new();
        let (mut snapshot, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open snapshot handle");
        let (mut maintenance, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open maintenance handle");

        snapshot
            .lock_external_shared_snapshot(&cx)
            .expect("acquire shared snapshot");
        assert!(matches!(
            maintenance.lock_external_maintenance(&cx, false),
            Err(FrankenError::Busy)
        ));
        maintenance
            .restore_external_maintenance_attempt(&cx)
            .expect("restore failed maintenance attempt");
        snapshot
            .restore_external_shared_snapshot_attempt(&cx)
            .expect("release shared snapshot");

        maintenance
            .lock_external_maintenance(&cx, false)
            .expect("maintenance must succeed after shared snapshot releases");
        maintenance
            .restore_external_maintenance_attempt(&cx)
            .expect("release maintenance");
        snapshot.close(&cx).unwrap();
        maintenance.close(&cx).unwrap();
    }

    #[test]
    fn test_external_maintenance_fence_uses_stock_sqlite_main_ranges() {
        let cx = Cx::new();
        let dir = tempdir().unwrap();
        let path = dir.path().join("stock_maintenance_main.db");
        let vfs = WindowsVfs::new();
        let (mut maintenance, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open maintenance handle");
        let mut probe_options = windows_open_options();
        let probe = probe_options
            .read(true)
            .write(true)
            .open(&path)
            .expect("open independent stock-lock probe");

        maintenance
            .lock_external_maintenance(&cx, false)
            .expect("acquire external maintenance fence");
        for (offset, len) in [
            (STOCK_SQLITE_PENDING_BYTE, 1),
            (STOCK_SQLITE_RESERVED_BYTE, 1),
            (STOCK_SQLITE_SHARED_FIRST, STOCK_SQLITE_SHARED_SIZE),
        ] {
            assert!(
                matches!(
                    try_lock_stock_sqlite_range(&probe, offset, len),
                    Err(FrankenError::Busy)
                ),
                "stock SQLite range {offset}..{} must be fenced",
                offset + len
            );
        }

        maintenance
            .restore_external_maintenance_attempt(&cx)
            .expect("release external maintenance fence");
        for (offset, len) in [
            (STOCK_SQLITE_PENDING_BYTE, 1),
            (STOCK_SQLITE_RESERVED_BYTE, 1),
            (STOCK_SQLITE_SHARED_FIRST, STOCK_SQLITE_SHARED_SIZE),
        ] {
            try_lock_stock_sqlite_range(&probe, offset, len)
                .expect("released range must be acquirable");
            unlock_stock_sqlite_range(&probe, offset, len).expect("unlock probe range");
        }
        maintenance.close(&cx).unwrap();
    }

    #[test]
    fn test_external_maintenance_wal_fence_uses_real_shm_bytes() {
        let cx = Cx::new();
        let dir = tempdir().unwrap();
        let path = dir.path().join("stock_maintenance_wal.db");
        let shm_path = sqlite_shm_path(&path);
        let vfs = WindowsVfs::new();
        let (mut maintenance, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open maintenance handle");

        maintenance
            .lock_external_maintenance(&cx, true)
            .expect("acquire WAL external maintenance fence");
        let mut probe_options = windows_open_options();
        let probe = probe_options
            .read(true)
            .write(true)
            .open(&shm_path)
            .expect("open real -shm probe");
        for offset in [STOCK_SQLITE_WAL_WRITE_BYTE, STOCK_SQLITE_WAL_CKPT_BYTE] {
            assert!(
                matches!(
                    try_lock_stock_sqlite_range(&probe, offset, 1),
                    Err(FrankenError::Busy)
                ),
                "stock SQLite WAL byte {offset} must be fenced"
            );
        }

        maintenance
            .restore_external_maintenance_attempt(&cx)
            .expect("release WAL external maintenance fence");
        for offset in [STOCK_SQLITE_WAL_WRITE_BYTE, STOCK_SQLITE_WAL_CKPT_BYTE] {
            try_lock_stock_sqlite_range(&probe, offset, 1)
                .expect("released WAL byte must be acquirable");
            unlock_stock_sqlite_range(&probe, offset, 1).expect("unlock WAL probe byte");
        }
        maintenance.close(&cx).unwrap();
    }

    #[test]
    fn test_external_maintenance_restore_preserves_prior_main_level() {
        let cx = Cx::new();
        let dir = tempdir().unwrap();
        let path = dir.path().join("stock_maintenance_prior_main.db");
        let vfs = WindowsVfs::new();
        let (mut maintenance, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open maintenance handle");

        maintenance
            .lock(&cx, LockLevel::Reserved)
            .expect("acquire preexisting RESERVED level");
        maintenance
            .lock_external_maintenance(&cx, false)
            .expect("upgrade to external maintenance");
        assert_eq!(maintenance.lock_level, LockLevel::Exclusive);
        assert_eq!(
            maintenance
                .external_maintenance_locks
                .as_ref()
                .expect("maintenance marker")
                .prior_main_level,
            LockLevel::Reserved
        );

        maintenance
            .restore_external_maintenance_attempt(&cx)
            .expect("restore exact preexisting main level");
        assert_eq!(maintenance.lock_level, LockLevel::Reserved);
        assert_eq!(
            maintenance
                .stock_main_locks
                .as_ref()
                .expect("stock main lock state")
                .lock_level,
            LockLevel::Reserved
        );
        maintenance
            .restore_external_maintenance_attempt(&cx)
            .expect("second restoration must preserve the prior level");
        assert_eq!(maintenance.lock_level, LockLevel::Reserved);

        maintenance
            .unlock(&cx, LockLevel::None)
            .expect("release preexisting RESERVED level");
        maintenance.close(&cx).unwrap();
    }

    #[test]
    fn test_external_maintenance_restore_preserves_preheld_wal_slot() {
        let cx = Cx::new();
        let dir = tempdir().unwrap();
        let path = dir.path().join("stock_maintenance_preheld_wal.db");
        let shm_path = sqlite_shm_path(&path);
        let vfs = WindowsVfs::new();
        let (mut maintenance, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open maintenance handle");

        maintenance
            .shm_lock(
                &cx,
                WAL_WRITE_LOCK,
                1,
                SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE,
            )
            .expect("acquire preexisting WAL write slot");
        maintenance
            .lock_external_maintenance(&cx, true)
            .expect("acquire maintenance around preheld WAL write");
        let attempt = maintenance
            .external_maintenance_locks
            .as_ref()
            .expect("maintenance marker");
        assert!(!attempt.wal_write_acquired);
        assert!(attempt.wal_checkpoint_acquired);

        maintenance
            .restore_external_maintenance_attempt(&cx)
            .expect("restore only attempt-owned WAL slots");
        assert!(maintenance.external_maintenance_locks.is_none());
        assert!(
            maintenance
                .owns_exclusive_shm_slot(WAL_WRITE_LOCK)
                .expect("inspect preheld write slot"),
            "restoration must preserve same-owner preexisting ownership"
        );
        assert!(
            !maintenance
                .owns_exclusive_shm_slot(WAL_CKPT_LOCK)
                .expect("inspect attempt-owned checkpoint slot")
        );

        let probe = open_stock_shm_probe(&shm_path);
        assert!(matches!(
            try_lock_stock_sqlite_range(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1),
            Err(FrankenError::Busy)
        ));
        try_lock_stock_sqlite_range(&probe, STOCK_SQLITE_WAL_CKPT_BYTE, 1)
            .expect("attempt-owned checkpoint byte must be released");
        unlock_stock_sqlite_range(&probe, STOCK_SQLITE_WAL_CKPT_BYTE, 1)
            .expect("unlock checkpoint probe");

        maintenance
            .shm_lock(
                &cx,
                WAL_WRITE_LOCK,
                1,
                SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE,
            )
            .expect("release preexisting WAL write slot");
        maintenance.close(&cx).unwrap();
    }

    #[test]
    fn test_external_maintenance_partial_wal_acquisition_is_retry_restorable() {
        let cx = Cx::new();
        let dir = tempdir().unwrap();
        let path = dir.path().join("stock_maintenance_unwind.db");
        let shm_path = sqlite_shm_path(&path);
        let vfs = WindowsVfs::new();
        let (mut maintenance, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open maintenance handle");
        let (blocker, _) = open_windows_lock_sidecar(&shm_path).expect("open -shm blocker");
        try_lock_stock_sqlite_range(&blocker, STOCK_SQLITE_WAL_CKPT_BYTE, 1)
            .expect("hold stock checkpoint byte");

        assert!(matches!(
            maintenance.lock_external_maintenance(&cx, true),
            Err(FrankenError::Busy)
        ));

        // The write byte was acquired before checkpoint contention. The armed
        // attempt must remember exactly that new ownership while preserving
        // the still-unacquired checkpoint slot for a retry-safe restoration.
        assert_eq!(maintenance.lock_level, LockLevel::None);
        let attempt = maintenance
            .external_maintenance_locks
            .as_ref()
            .expect("failed attempt retains its restoration marker");
        assert!(attempt.main_restore_pending);
        assert!(attempt.wal_write_acquired);
        assert!(!attempt.wal_checkpoint_acquired);
        assert!(
            maintenance.stock_main_locks.is_none(),
            "real WAL contention must be resolved before opening the dedicated stock-main lock handle"
        );
        assert!(matches!(
            try_lock_stock_sqlite_range(&blocker, STOCK_SQLITE_WAL_WRITE_BYTE, 1),
            Err(FrankenError::Busy)
        ));

        maintenance
            .restore_external_maintenance_attempt(&cx)
            .expect("restore partial WAL acquisition");
        assert!(maintenance.external_maintenance_locks.is_none());
        try_lock_stock_sqlite_range(&blocker, STOCK_SQLITE_WAL_WRITE_BYTE, 1)
            .expect("restoration must release the attempt-owned WAL write byte");
        unlock_stock_sqlite_range(&blocker, STOCK_SQLITE_WAL_WRITE_BYTE, 1)
            .expect("unlock WAL write probe");
        unlock_stock_sqlite_range(&blocker, STOCK_SQLITE_WAL_CKPT_BYTE, 1)
            .expect("release checkpoint blocker");

        maintenance
            .lock_external_maintenance(&cx, true)
            .expect("retry after contention must succeed");
        maintenance
            .restore_external_maintenance_attempt(&cx)
            .expect("retry fence must release cleanly");
        maintenance.close(&cx).unwrap();
    }

    #[test]
    fn test_external_maintenance_failed_main_acquisition_is_retry_restorable() {
        let cx = Cx::new();
        let dir = tempdir().unwrap();
        let path = dir.path().join("stock_maintenance_main_unwind.db");
        let shm_path = sqlite_shm_path(&path);
        let vfs = WindowsVfs::new();
        let (mut maintenance, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open maintenance handle");
        let mut blocker_options = windows_open_options();
        let blocker = blocker_options
            .read(true)
            .write(true)
            .open(&path)
            .expect("open main-range blocker");
        try_lock_stock_sqlite_range(
            &blocker,
            STOCK_SQLITE_SHARED_FIRST,
            STOCK_SQLITE_SHARED_SIZE,
        )
        .expect("hold stock shared range");

        assert!(matches!(
            maintenance.lock_external_maintenance(&cx, true),
            Err(FrankenError::Busy)
        ));

        // Ordinary main-lock acquisition rolls its own partial prefix back,
        // while the external-attempt marker retains both newly acquired WAL
        // slots until the caller performs the required restoration.
        let attempt = maintenance
            .external_maintenance_locks
            .as_ref()
            .expect("failed main acquisition retains its restoration marker");
        assert!(attempt.main_restore_pending);
        assert!(attempt.wal_write_acquired);
        assert!(attempt.wal_checkpoint_acquired);
        for offset in [STOCK_SQLITE_PENDING_BYTE, STOCK_SQLITE_RESERVED_BYTE] {
            try_lock_stock_sqlite_range(&blocker, offset, 1)
                .expect("partial main lock must be unwound");
            unlock_stock_sqlite_range(&blocker, offset, 1).expect("unlock main-range probe");
        }
        let mut shm_probe_options = windows_open_options();
        let shm_probe = shm_probe_options
            .read(true)
            .write(true)
            .open(&shm_path)
            .expect("open -shm lock probe");
        for offset in [STOCK_SQLITE_WAL_WRITE_BYTE, STOCK_SQLITE_WAL_CKPT_BYTE] {
            assert!(matches!(
                try_lock_stock_sqlite_range(&shm_probe, offset, 1),
                Err(FrankenError::Busy)
            ));
        }

        maintenance
            .restore_external_maintenance_attempt(&cx)
            .expect("restore failed main acquisition");
        assert!(maintenance.external_maintenance_locks.is_none());
        for offset in [STOCK_SQLITE_WAL_WRITE_BYTE, STOCK_SQLITE_WAL_CKPT_BYTE] {
            try_lock_stock_sqlite_range(&shm_probe, offset, 1)
                .expect("restoration must release the earlier real WAL fence");
            unlock_stock_sqlite_range(&shm_probe, offset, 1).expect("unlock WAL-range probe");
        }
        unlock_stock_sqlite_range(
            &blocker,
            STOCK_SQLITE_SHARED_FIRST,
            STOCK_SQLITE_SHARED_SIZE,
        )
        .expect("release shared-range blocker");

        maintenance
            .lock_external_maintenance(&cx, true)
            .expect("retry after contention must succeed");
        maintenance
            .restore_external_maintenance_attempt(&cx)
            .expect("retry fence must release cleanly");
        maintenance.close(&cx).unwrap();
    }

    #[test]
    fn test_external_maintenance_restore_retries_each_failed_surface() {
        let cx = Cx::new();
        let dir = tempdir().unwrap();
        let path = dir.path().join("stock_maintenance_partial_restore.db");
        let vfs = WindowsVfs::new();
        let (mut maintenance, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open maintenance handle");

        maintenance
            .lock_external_maintenance(&cx, true)
            .expect("acquire WAL maintenance fence");
        let state = maintenance
            .shm_state
            .as_ref()
            .map(Arc::clone)
            .expect("maintenance SHM state");
        state
            .lock()
            .expect("inject SHM restoration failure")
            .poisoned = Some("injected maintenance restoration failure".to_string());

        assert!(
            maintenance
                .restore_external_maintenance_attempt(&cx)
                .is_err(),
            "poisoned WAL surfaces must make the first restoration fail"
        );
        assert_eq!(
            maintenance.lock_level,
            LockLevel::None,
            "main restoration must still run when both WAL surfaces fail"
        );
        let attempt = maintenance
            .external_maintenance_locks
            .as_ref()
            .expect("failed restoration retains its exact attempt marker");
        assert!(!attempt.main_restore_pending);
        assert!(attempt.wal_write_acquired);
        assert!(attempt.wal_checkpoint_acquired);

        state.lock().expect("clear injected SHM failure").poisoned = None;
        maintenance
            .restore_external_maintenance_attempt(&cx)
            .expect("retry every still-owned WAL surface");
        assert!(maintenance.external_maintenance_locks.is_none());
        assert!(
            !maintenance
                .owns_exclusive_shm_slot(WAL_WRITE_LOCK)
                .expect("inspect restored write slot")
        );
        assert!(
            !maintenance
                .owns_exclusive_shm_slot(WAL_CKPT_LOCK)
                .expect("inspect restored checkpoint slot")
        );
        maintenance.close(&cx).unwrap();
    }

    #[test]
    fn test_windowsvfs_shm_exclusive_unlock_preserves_prior_shared_lock() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("shm_lock_downgrade.db");
        let vfs = WindowsVfs::new();
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open file");

        file.shm_lock(&cx, 0, 1, SQLITE_SHM_LOCK | SQLITE_SHM_SHARED)
            .expect("acquire shared");
        file.shm_lock(&cx, 0, 1, SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)
            .expect("upgrade to exclusive");
        let probe = open_stock_shm_probe(&file.shm_path);
        assert!(matches!(
            try_lock_stock_sqlite_range(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1),
            Err(FrankenError::Busy)
        ));
        file.shm_lock(&cx, 0, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE)
            .expect("downgrade from exclusive");
        assert!(
            matches!(
                try_lock_stock_sqlite_range(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1),
                Err(FrankenError::Busy)
            ),
            "exclusive unlock must restore the owner's earlier shared OS lock"
        );
        file.shm_lock(&cx, 0, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED)
            .expect("release preserved shared lock");
        try_lock_stock_sqlite_range(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1)
            .expect("final shared unlock must release the real WAL byte");
        unlock_stock_sqlite_range_strict(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1)
            .expect("unlock final probe");
    }

    #[test]
    fn test_windowsvfs_shm_failed_promotion_restores_shared_lock() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("shm_failed_promotion.db");
        let vfs = WindowsVfs::new();
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open file");
        file.shm_lock(&cx, 0, 1, SQLITE_SHM_LOCK | SQLITE_SHM_SHARED)
            .expect("acquire FrankenSQLite shared lock");
        let probe = open_stock_shm_probe(&file.shm_path);
        try_lock_stock_sqlite_shared_range(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1)
            .expect("acquire independent stock shared lock");

        assert!(matches!(
            file.shm_lock(&cx, 0, 1, SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE),
            Err(FrankenError::Busy)
        ));
        unlock_stock_sqlite_range_strict(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1)
            .expect("release independent shared blocker");
        assert!(
            matches!(
                try_lock_stock_sqlite_range(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1),
                Err(FrankenError::Busy)
            ),
            "failed promotion must restore FrankenSQLite's aggregate shared lock"
        );

        file.shm_lock(&cx, 0, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED)
            .expect("release restored FrankenSQLite shared lock");
        try_lock_stock_sqlite_range(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1)
            .expect("shared byte must release after the original owner unlocks");
        unlock_stock_sqlite_range_strict(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1)
            .expect("unlock final probe");
    }

    #[test]
    fn test_windowsvfs_missing_ordinary_shm_unlock_restores_fence_and_poison() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let vfs = WindowsVfs::new();

        for (label, mode_flag) in [
            ("shared", SQLITE_SHM_SHARED),
            ("exclusive", SQLITE_SHM_EXCLUSIVE),
        ] {
            let path = dir.path().join(format!("shm_missing_{label}_unlock.db"));
            let (mut file, _) = vfs
                .open(&cx, Some(&path), open_flags_create())
                .expect("open file");
            file.shm_lock(&cx, WAL_WRITE_LOCK, 1, SQLITE_SHM_LOCK | mode_flag)
                .expect("acquire SHM lock");
            let state = inject_missing_stock_shm_range(&file, WAL_WRITE_LOCK);

            assert!(matches!(
                file.shm_lock(&cx, WAL_WRITE_LOCK, 1, SQLITE_SHM_UNLOCK | mode_flag,),
                Err(FrankenError::Internal(_))
            ));
            {
                let state = state.lock().expect("inspect poisoned SHM state");
                assert!(
                    state
                        .poisoned
                        .as_deref()
                        .is_some_and(|detail| detail.contains("ERROR_NOT_LOCKED")),
                    "missing {label} unlock must poison the process lock domain"
                );
            }

            let probe = open_stock_shm_probe(&file.shm_path);
            assert!(
                matches!(
                    try_lock_stock_sqlite_range(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1),
                    Err(FrankenError::Busy)
                ),
                "missing {label} unlock must restore a stock-visible fence before failing"
            );
            file.shm_unmap(&cx, false)
                .expect("final poisoned owner drains the domain");
            try_lock_stock_sqlite_range(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1)
                .expect("final poison teardown releases the restored fence");
            unlock_stock_sqlite_range_strict(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1)
                .expect("unlock final probe");
        }
    }

    #[test]
    fn test_windowsvfs_missing_promotion_unlock_restores_shared_fence_and_poison() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("shm_missing_promotion_unlock.db");
        let vfs = WindowsVfs::new();
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open file");
        file.shm_lock(&cx, WAL_WRITE_LOCK, 1, SQLITE_SHM_LOCK | SQLITE_SHM_SHARED)
            .expect("acquire shared SHM lock");
        let state = inject_missing_stock_shm_range(&file, WAL_WRITE_LOCK);

        assert!(matches!(
            file.shm_lock(
                &cx,
                WAL_WRITE_LOCK,
                1,
                SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE,
            ),
            Err(FrankenError::Internal(_))
        ));
        assert!(
            state
                .lock()
                .expect("inspect poisoned SHM state")
                .poisoned
                .as_deref()
                .is_some_and(|detail| detail.contains("ERROR_NOT_LOCKED")),
            "a missing promotion unlock must poison the process lock domain"
        );

        let probe = open_stock_shm_probe(&file.shm_path);
        assert!(
            matches!(
                try_lock_stock_sqlite_range(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1),
                Err(FrankenError::Busy)
            ),
            "failed promotion must restore the aggregate shared fence"
        );
        file.shm_unmap(&cx, false)
            .expect("final poisoned owner drains the domain");
        try_lock_stock_sqlite_range(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1)
            .expect("final poison teardown releases the restored fence");
        unlock_stock_sqlite_range_strict(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1)
            .expect("unlock final probe");
    }

    #[test]
    fn test_windowsvfs_shm_shared_lock_is_process_aggregated_and_refcounted() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("shm_shared_aggregate.db");
        let vfs = WindowsVfs::new();
        let (mut file_a, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open A");
        let (mut file_b, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open B");

        file_a
            .shm_lock(&cx, 0, 1, SQLITE_SHM_LOCK | SQLITE_SHM_SHARED)
            .expect("A first shared");
        file_a
            .shm_lock(&cx, 0, 1, SQLITE_SHM_LOCK | SQLITE_SHM_SHARED)
            .expect("A repeated shared");
        file_b
            .shm_lock(&cx, 0, 1, SQLITE_SHM_LOCK | SQLITE_SHM_SHARED)
            .expect("B shared");

        let probe = open_stock_shm_probe(&file_a.shm_path);
        try_lock_stock_sqlite_shared_range(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1)
            .expect("independent stock shared lock must coexist");
        unlock_stock_sqlite_range_strict(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1)
            .expect("unlock shared probe");
        assert!(matches!(
            try_lock_stock_sqlite_range(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1),
            Err(FrankenError::Busy)
        ));

        file_a
            .shm_lock(&cx, 0, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED)
            .expect("A first shared release");
        assert!(
            matches!(
                try_lock_stock_sqlite_range(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1),
                Err(FrankenError::Busy)
            ),
            "A refcount and B's hold must keep the aggregate byte locked"
        );
        file_a
            .shm_lock(&cx, 0, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED)
            .expect("A final shared release");
        assert!(
            matches!(
                try_lock_stock_sqlite_range(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1),
                Err(FrankenError::Busy)
            ),
            "B's shared hold must survive A's final release"
        );
        file_b
            .shm_lock(&cx, 0, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED)
            .expect("B shared release");
        try_lock_stock_sqlite_range(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1)
            .expect("last process-local shared release must unlock the real byte");
        unlock_stock_sqlite_range_strict(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1)
            .expect("unlock exclusive probe");
    }

    #[test]
    fn test_windowsvfs_shm_exclusive_locks_all_stock_wal_bytes() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("shm_all_slots.db");
        let vfs = WindowsVfs::new();
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open file");

        for slot in 0..WAL_TOTAL_LOCKS {
            let lock_byte = STOCK_SQLITE_SHM_LOCK_BASE + u64::from(slot);
            file.shm_lock(&cx, slot, 1, SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)
                .expect("acquire exclusive slot");
            let probe = open_stock_shm_probe(&file.shm_path);
            assert!(
                matches!(
                    try_lock_stock_sqlite_shared_range(&probe, lock_byte, 1),
                    Err(FrankenError::Busy)
                ),
                "slot {slot} must exclude an independent shared probe"
            );
            assert!(
                matches!(
                    try_lock_stock_sqlite_range(&probe, lock_byte, 1),
                    Err(FrankenError::Busy)
                ),
                "slot {slot} must exclude an independent exclusive probe"
            );
            file.shm_lock(&cx, slot, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE)
                .expect("release exclusive slot");
            try_lock_stock_sqlite_range(&probe, lock_byte, 1)
                .expect("released slot must be independently acquirable");
            unlock_stock_sqlite_range_strict(&probe, lock_byte, 1)
                .expect("unlock independent probe");
        }
    }

    #[test]
    fn test_windowsvfs_shm_multislot_failure_unwinds_real_and_local_locks() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("shm_multislot_unwind.db");
        let vfs = WindowsVfs::new();
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open file");
        let blocker = open_stock_shm_probe(&file.shm_path);
        try_lock_stock_sqlite_range(&blocker, STOCK_SQLITE_WAL_CKPT_BYTE, 1)
            .expect("block checkpoint slot");

        assert!(matches!(
            file.shm_lock(&cx, 0, 3, SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE),
            Err(FrankenError::Busy)
        ));

        for slot in [0_u32, 2] {
            let lock_byte = STOCK_SQLITE_SHM_LOCK_BASE + u64::from(slot);
            try_lock_stock_sqlite_range(&blocker, lock_byte, 1)
                .expect("partial multi-slot acquisition must unwind real byte");
            unlock_stock_sqlite_range_strict(&blocker, lock_byte, 1).expect("unlock unwind probe");
        }
        unlock_stock_sqlite_range_strict(&blocker, STOCK_SQLITE_WAL_CKPT_BYTE, 1)
            .expect("release checkpoint blocker");

        file.shm_lock(&cx, 0, 3, SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)
            .expect("retry must prove local slot state was also unwound");
        file.shm_lock(&cx, 0, 3, SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE)
            .expect("release retry");
    }

    #[test]
    fn test_windowsvfs_shm_multislot_unlock_attempts_every_slot_after_error() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("shm_multislot_unlock_error.db");
        let vfs = WindowsVfs::new();
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open file");
        file.shm_lock(&cx, 0, 2, SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)
            .expect("acquire two exclusive slots");

        let state = file.shm_state.as_ref().map(Arc::clone).expect("SHM state");
        {
            let mut state = state.lock().expect("lock SHM state");
            state.slots[1].exclusive_owner = None;
        }
        assert!(matches!(
            file.shm_lock(&cx, 0, 2, SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE),
            Err(FrankenError::Internal(_))
        ));

        let probe = open_stock_shm_probe(&file.shm_path);
        try_lock_stock_sqlite_range(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1)
            .expect("slot 0 must release even after slot 1 reports an error");
        unlock_stock_sqlite_range_strict(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1)
            .expect("unlock slot 0 probe");
        assert!(
            matches!(
                try_lock_stock_sqlite_range(&probe, STOCK_SQLITE_WAL_CKPT_BYTE, 1),
                Err(FrankenError::Busy)
            ),
            "the deliberately mismatched slot 1 real lock must remain fail-closed"
        );

        {
            let mut state = state.lock().expect("lock SHM state for cleanup");
            state.slots[1].exclusive_owner = Some(file.owner_id);
        }
        file.shm_lock(
            &cx,
            WAL_CKPT_LOCK,
            1,
            SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE,
        )
        .expect("release deliberately mismatched slot");
    }

    #[test]
    fn test_windowsvfs_shm_unmap_close_and_drop_release_stock_bytes() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let vfs = WindowsVfs::new();

        for action in ["unmap", "close", "drop"] {
            let path = dir.path().join(format!("shm_release_{action}.db"));
            let (mut file, _) = vfs
                .open(&cx, Some(&path), open_flags_create())
                .expect("open file");
            file.shm_lock(&cx, 0, 2, SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)
                .expect("acquire two slots");
            let shm_path = file.shm_path.clone();
            match action {
                "unmap" => file.shm_unmap(&cx, false).expect("unmap"),
                "close" => file.close(&cx).expect("close"),
                "drop" => drop(file),
                _ => unreachable!(),
            }

            let probe = open_stock_shm_probe(&shm_path);
            for slot in 0_u32..2 {
                let lock_byte = STOCK_SQLITE_SHM_LOCK_BASE + u64::from(slot);
                try_lock_stock_sqlite_range(&probe, lock_byte, 1)
                    .expect("lifecycle release must unlock real byte");
                unlock_stock_sqlite_range_strict(&probe, lock_byte, 1)
                    .expect("unlock lifecycle probe");
            }
        }
    }

    #[test]
    fn test_windowsvfs_poisoned_shm_handle_stays_fenced_until_all_owners_drain() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("shm_poisoned_cohort.db");
        let vfs = WindowsVfs::new();
        let (mut file_a, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open A");
        let (mut file_b, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open B");
        file_a
            .shm_lock(
                &cx,
                WAL_WRITE_LOCK,
                1,
                SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE,
            )
            .expect("A acquires WAL write slot");
        file_b
            .shm_lock(
                &cx,
                WAL_CKPT_LOCK,
                1,
                SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE,
            )
            .expect("B acquires WAL checkpoint slot");

        let state = file_a
            .shm_state
            .as_ref()
            .map(Arc::clone)
            .expect("SHM state");
        state.lock().expect("poison SHM state").poisoned =
            Some("injected restoration failure".to_string());

        file_a
            .shm_unmap(&cx, false)
            .expect("first poisoned owner detaches");
        let probe = open_stock_shm_probe(&file_b.shm_path);
        for lock_byte in [STOCK_SQLITE_WAL_WRITE_BYTE, STOCK_SQLITE_WAL_CKPT_BYTE] {
            assert!(
                matches!(
                    try_lock_stock_sqlite_range(&probe, lock_byte, 1),
                    Err(FrankenError::Busy)
                ),
                "a poisoned cohort must retain every possibly-live range until its last owner drains"
            );
        }
        assert!(
            state
                .lock()
                .expect("inspect poisoned state")
                .poisoned
                .is_some(),
            "the surviving owner must remain attached to a fail-closed domain"
        );
        assert!(matches!(
            file_b.shm_lock(
                &cx,
                WAL_CKPT_LOCK,
                1,
                SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE,
            ),
            Err(FrankenError::Internal(_))
        ));

        file_b
            .shm_unmap(&cx, false)
            .expect("last poisoned owner detaches");
        for lock_byte in [STOCK_SQLITE_WAL_WRITE_BYTE, STOCK_SQLITE_WAL_CKPT_BYTE] {
            try_lock_stock_sqlite_range(&probe, lock_byte, 1)
                .expect("last poisoned owner must close the aggregate handle");
            unlock_stock_sqlite_range_strict(&probe, lock_byte, 1)
                .expect("unlock final poison-teardown probe");
        }
        assert!(
            windows_shm_table()
                .get(&file_b.shm_path)
                .expect("inspect SHM table")
                .is_none(),
            "the fully drained poisoned domain must leave the global table"
        );
    }

    #[test]
    fn test_windowsvfs_owner_close_unlock_failure_detaches_into_poisoned_cohort() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("shm_owner_close_unlock_failure.db");
        let vfs = WindowsVfs::new();
        let (mut file_a, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open A");
        let (mut file_b, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open B");
        file_a
            .shm_lock(&cx, WAL_WRITE_LOCK, 1, SQLITE_SHM_LOCK | SQLITE_SHM_SHARED)
            .expect("A acquires WAL write slot");
        let state = file_b.ensure_shm_state().expect("register B");

        // Fault injection: release the aggregate OS range behind the local
        // slot table. The next strict owner-close unlock must see
        // ERROR_NOT_LOCKED, poison the shared domain, and detach A rather than
        // strand an owner ref that can never be drained after A is dropped.
        {
            let state = state.lock().expect("lock SHM state for fault injection");
            let shm_file = state
                .stock_shm_file
                .as_ref()
                .expect("aggregate stock -shm handle");
            unlock_stock_sqlite_range_strict(shm_file, STOCK_SQLITE_WAL_WRITE_BYTE, 1)
                .expect("inject missing aggregate shared range");
        }

        assert!(matches!(
            file_a.shm_unmap(&cx, false),
            Err(FrankenError::Internal(_))
        ));
        let probe = open_stock_shm_probe(&file_b.shm_path);
        assert!(
            matches!(
                try_lock_stock_sqlite_range(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1),
                Err(FrankenError::Busy)
            ),
            "owner-close ERROR_NOT_LOCKED recovery must restore the stock-visible fence"
        );
        assert!(
            file_a.shm_state.is_none(),
            "an owner detached into a poisoned cohort must not retain a retry-only SHM ref"
        );
        {
            let state = state.lock().expect("inspect poisoned cohort");
            assert!(state.poisoned.is_some());
            assert!(!state.owner_refs.contains_key(&file_a.owner_id));
            assert!(state.owner_refs.contains_key(&file_b.owner_id));
            assert!(
                state.slots[to_slot_index(WAL_WRITE_LOCK).expect("slot index")]
                    .shared_holders
                    .contains_key(&file_a.owner_id),
                "possibly-live claims stay recorded until the final cohort owner drains"
            );
        }
        assert!(matches!(
            file_b.shm_lock(
                &cx,
                WAL_CKPT_LOCK,
                1,
                SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE,
            ),
            Err(FrankenError::Internal(_))
        ));

        file_b
            .shm_unmap(&cx, false)
            .expect("final poisoned owner drains the cohort");
        try_lock_stock_sqlite_range(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1)
            .expect("final poisoned owner must release the restored fence");
        unlock_stock_sqlite_range_strict(&probe, STOCK_SQLITE_WAL_WRITE_BYTE, 1)
            .expect("unlock final owner-close probe");
        assert!(
            windows_shm_table()
                .get(&file_b.shm_path)
                .expect("inspect SHM table")
                .is_none(),
            "final poison teardown must remove the stale owner claim and global domain"
        );
    }

    #[test]
    fn test_windowsvfs_temp_file_auto_delete() {
        let cx = Cx::new();
        let vfs = WindowsVfs::new();
        let flags = VfsOpenFlags::TEMP_DB
            | VfsOpenFlags::CREATE
            | VfsOpenFlags::READWRITE
            | VfsOpenFlags::DELETEONCLOSE;
        let (mut file, _) = vfs.open(&cx, None, flags).expect("open temp");
        let temp_path = file.path.clone();
        let lock_sidecars = windows_lock_sidecar_paths(&temp_path);
        assert!(temp_path.exists());
        for sidecar in &lock_sidecars {
            assert!(
                sidecar.exists(),
                "temporary Windows VFS handle should create {}",
                sidecar.display()
            );
        }
        file.close(&cx).expect("close");
        assert!(!temp_path.exists());
        for sidecar in &lock_sidecars {
            assert!(
                !sidecar.exists(),
                "temporary close should remove advisory lock sidecar {}",
                sidecar.display()
            );
        }
    }

    #[test]
    fn test_windowsvfs_temp_file_skips_existing_candidate() {
        let cx = Cx::new();
        let seed_base = 1_000_000_000_000_u64 + u64::from(std::process::id()) * 1_024;
        let (seed, blocker, blocker_file) = (0_u64..1_024)
            .find_map(|offset| {
                let seed = seed_base + offset;
                let blocker = env::temp_dir().join(format!("fsqlite-windows-{seed}.tmp"));
                let mut blocker_options = windows_open_options();
                blocker_options
                    .write(true)
                    .create_new(true)
                    .open(&blocker)
                    .ok()
                    .map(|file| (seed, blocker, file))
            })
            .expect("available temp candidate");
        let _blocker_cleanup = TempPathCleanup(blocker.clone());
        let mut blocker_file = blocker_file;
        blocker_file
            .write_all(b"existing temp candidate")
            .expect("write existing temp candidate");
        drop(blocker_file);
        let vfs = WindowsVfs {
            inner: Arc::new(Mutex::new(WindowsVfsInner { next_temp_id: seed })),
        };
        let flags = VfsOpenFlags::TEMP_DB
            | VfsOpenFlags::CREATE
            | VfsOpenFlags::READWRITE
            | VfsOpenFlags::DELETEONCLOSE;

        let (mut file, _) = vfs.open(&cx, None, flags).expect("open temp");
        let opened_path = file.path.clone();
        assert_ne!(
            opened_path, blocker,
            "anonymous temp open must not reuse an existing candidate path"
        );
        assert!(
            blocker.exists(),
            "temp collision handling must preserve the existing candidate file"
        );

        file.close(&cx).expect("close temp");
        assert!(
            !opened_path.exists(),
            "delete-on-close should remove the actual temp file"
        );
    }

    #[test]
    fn test_windowsvfs_open_handles_block_delete_sharing() {
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("delete_sharing.db");
        let mut options = windows_open_options();
        let _file = options
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .expect("open file without delete sharing");

        assert!(
            fs::remove_file(&path).is_err(),
            "Windows VFS files must reject unlink while an open handle exists"
        );
    }

    #[test]
    fn test_windowsvfs_lock_open_failure_cleans_created_shared_sidecar() {
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("partial_lock_open.db");
        let shared_path = sqlite_shared_lock_path(&path);
        let reserved_path = sqlite_reserved_lock_path(&path);
        fs::create_dir(&reserved_path).expect("reserved sidecar blocker");

        assert!(WindowsOsLockFiles::open(&path).is_err());
        assert!(
            !shared_path.exists(),
            "failed lock setup should remove the shared sidecar it just created"
        );
        assert!(
            reserved_path.is_dir(),
            "cleanup must not disturb the path that caused the open failure"
        );
    }

    #[test]
    fn test_windowsvfs_open_failure_cleans_created_db_file() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("partial_vfs_open.db");
        let shared_path = sqlite_shared_lock_path(&path);
        let reserved_path = sqlite_reserved_lock_path(&path);
        fs::create_dir(&reserved_path).expect("reserved sidecar blocker");
        let vfs = WindowsVfs::new();
        let flags = open_flags_create() | VfsOpenFlags::EXCLUSIVE | VfsOpenFlags::DELETEONCLOSE;

        assert!(vfs.open(&cx, Some(&path), flags).is_err());
        assert!(
            !path.exists(),
            "failed exclusive create should remove the DB file it just created"
        );
        assert!(
            !shared_path.exists(),
            "failed lock setup should remove the shared sidecar it just created"
        );
        assert!(
            reserved_path.is_dir(),
            "cleanup must not disturb the path that caused the open failure"
        );
    }

    #[test]
    fn test_windowsvfs_plain_create_failure_cleans_created_db_file() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("partial_plain_create.db");
        let shared_path = sqlite_shared_lock_path(&path);
        let reserved_path = sqlite_reserved_lock_path(&path);
        fs::create_dir(&reserved_path).expect("reserved sidecar blocker");
        let vfs = WindowsVfs::new();

        assert!(vfs.open(&cx, Some(&path), open_flags_create()).is_err());
        assert!(
            !path.exists(),
            "failed plain create should remove the DB file it just created"
        );
        assert!(
            !shared_path.exists(),
            "failed lock setup should remove the shared sidecar it just created"
        );
        assert!(
            reserved_path.is_dir(),
            "cleanup must not disturb the path that caused the open failure"
        );
    }

    #[test]
    fn test_windowsvfs_plain_create_failure_preserves_existing_db_file() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("existing_plain_create.db");
        let shared_path = sqlite_shared_lock_path(&path);
        let reserved_path = sqlite_reserved_lock_path(&path);
        fs::write(&path, b"existing db").expect("existing db");
        fs::create_dir(&reserved_path).expect("reserved sidecar blocker");
        let vfs = WindowsVfs::new();

        assert!(vfs.open(&cx, Some(&path), open_flags_create()).is_err());
        assert_eq!(
            fs::read(&path).expect("read existing db"),
            b"existing db",
            "failed plain create must preserve an existing DB file"
        );
        assert!(
            !shared_path.exists(),
            "failed lock setup should remove only the shared sidecar it just created"
        );
        assert!(
            reserved_path.is_dir(),
            "cleanup must not disturb the path that caused the open failure"
        );
    }

    #[test]
    fn test_windowsvfs_open_failure_preserves_existing_sidecar() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("partial_vfs_open_existing_sidecar.db");
        let shared_path = sqlite_shared_lock_path(&path);
        let reserved_path = sqlite_reserved_lock_path(&path);
        fs::write(&shared_path, b"existing shared sidecar").expect("existing shared sidecar");
        fs::create_dir(&reserved_path).expect("reserved sidecar blocker");
        let vfs = WindowsVfs::new();
        let flags = open_flags_create() | VfsOpenFlags::EXCLUSIVE | VfsOpenFlags::DELETEONCLOSE;

        assert!(vfs.open(&cx, Some(&path), flags).is_err());
        assert!(
            !path.exists(),
            "failed exclusive create should remove the DB file it just created"
        );
        assert!(
            shared_path.exists(),
            "failed VFS open must preserve a sidecar it did not create"
        );
        assert!(
            reserved_path.is_dir(),
            "cleanup must not disturb the path that caused the open failure"
        );
    }

    #[test]
    fn test_windowsvfs_lock_open_failure_preserves_existing_sidecars() {
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("existing_partial_lock_open.db");
        let shared_path = sqlite_shared_lock_path(&path);
        let reserved_path = sqlite_reserved_lock_path(&path);
        let pending_path = sqlite_pending_lock_path(&path);
        fs::write(&shared_path, b"existing shared sidecar").expect("existing shared sidecar");
        fs::create_dir(&pending_path).expect("pending sidecar blocker");

        assert!(WindowsOsLockFiles::open(&path).is_err());
        assert!(
            shared_path.exists(),
            "failed lock setup must not remove a sidecar it did not create"
        );
        assert!(
            !reserved_path.exists(),
            "failed lock setup should remove the reserved sidecar it just created"
        );
        assert!(
            pending_path.is_dir(),
            "cleanup must not disturb the path that caused the open failure"
        );
    }

    #[test]
    fn test_windowsvfs_delete_on_close_is_idempotent() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("idempotent_close.db");
        let vfs = WindowsVfs::new();
        let flags = open_flags_create() | VfsOpenFlags::DELETEONCLOSE;
        let (mut file, _) = vfs
            .open(&cx, Some(&path), flags)
            .expect("open delete-on-close file");
        let shm_path = file.shm_path.clone();
        let lock_sidecars = windows_lock_sidecar_paths(&path);

        file.close(&cx).expect("first close");
        assert!(!path.exists(), "first close should delete the DB file");

        fs::write(&path, b"replacement db").expect("replacement db");
        fs::write(&shm_path, b"replacement shm").expect("replacement shm");
        for sidecar in &lock_sidecars {
            fs::write(sidecar, b"replacement lock").expect("replacement sidecar");
        }

        file.close(&cx).expect("second close");
        assert!(path.exists(), "second close must be a no-op");
        assert!(
            shm_path.exists(),
            "second close must not delete replacement SHM"
        );
        for sidecar in &lock_sidecars {
            assert!(
                sidecar.exists(),
                "second close must not delete replacement sidecar {}",
                sidecar.display()
            );
        }
    }

    #[test]
    fn test_windowsvfs_shm_rejects_use_after_close() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("closed_shm.db");
        let vfs = WindowsVfs::new();
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open file");

        file.close(&cx).expect("close file");
        assert!(
            matches!(
                file.shm_map(&cx, 0, 32 * 1024, true),
                Err(FrankenError::Internal(_))
            ),
            "closed Windows handles must not recreate SHM state"
        );
        assert!(
            matches!(
                file.shm_lock(&cx, 0, 1, SQLITE_SHM_LOCK | SQLITE_SHM_SHARED),
                Err(FrankenError::Internal(_))
            ),
            "closed Windows handles must reject SHM locks"
        );
        assert!(
            matches!(file.shm_unmap(&cx, false), Err(FrankenError::Internal(_))),
            "closed Windows handles must reject SHM unmap"
        );
    }

    #[test]
    fn test_windowsvfs_delete_removes_lock_sidecars() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("delete_sidecars.db");
        let vfs = WindowsVfs::new();
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open file");
        let lock_sidecars = windows_lock_sidecar_paths(&path);

        for sidecar in &lock_sidecars {
            assert!(
                sidecar.exists(),
                "opening the Windows VFS handle should create {}",
                sidecar.display()
            );
        }

        file.close(&cx).expect("close file");

        vfs.delete(&cx, &path, false).expect("delete file");
        assert!(!path.exists(), "Vfs::delete should remove the main DB");
        for sidecar in &lock_sidecars {
            assert!(
                !sidecar.exists(),
                "Vfs::delete should remove advisory lock sidecar {}",
                sidecar.display()
            );
        }
    }

    #[test]
    fn test_windowsvfs_sector_size_detection() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("sector.db");
        let vfs = WindowsVfs::new();
        let (file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open file");

        let size = file.sector_size();
        assert!(size.is_power_of_two());
        assert!(size >= 512);
    }

    #[test]
    fn test_windowsvfs_device_characteristics() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("iocap.db");
        let vfs = WindowsVfs::new();
        let (file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open file");

        assert_eq!(
            file.device_characteristics() & SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN,
            SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_e2e_windowsvfs_c_sqlite_interop() {
        let sqlite_available = Command::new("sqlite3")
            .arg("--version")
            .output()
            .is_ok_and(|output| output.status.success());
        if !sqlite_available {
            return;
        }

        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("interop.db");
        let path_str = path.to_str().expect("path utf8");

        let create_status = Command::new("sqlite3")
            .arg(path_str)
            .arg(
                "PRAGMA journal_mode=WAL; \
                 CREATE TABLE t(x INTEGER); \
                 INSERT INTO t(x) VALUES (1),(2),(3);",
            )
            .status()
            .expect("run sqlite3 create");
        assert!(create_status.success());

        // Keep a stock SQLite connection open so its real file-backed WAL
        // index remains initialized while FrankenSQLite exercises only the
        // lock protocol. Windows shm_map is still heap-backed, so this test
        // intentionally makes no broader WAL-index visibility claim.
        let keeper = SqliteWalKeeper::start(&path);

        let vfs = WindowsVfs::new();
        let (mut file, _) = vfs
            .open(
                &cx,
                Some(&path),
                VfsOpenFlags::MAIN_DB | VfsOpenFlags::READWRITE,
            )
            .expect("open via windows vfs");
        let mut header = [0_u8; 16];
        let read = crate::block_on_test_io(&cx, file.read(&cx, &mut header, 0))
            .expect("read sqlite header");
        assert_eq!(read, 16);
        assert_eq!(&header, b"SQLite format 3\0");

        file.shm_lock(
            &cx,
            WAL_WRITE_LOCK,
            1,
            SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE,
        )
        .expect("FrankenSQLite acquires stock-visible WAL write slot");
        let blocked_writer = Command::new("sqlite3")
            .arg(path_str)
            .arg("PRAGMA busy_timeout=0; INSERT INTO t(x) VALUES (4);")
            .output()
            .expect("run contending sqlite3 writer");
        assert!(
            !blocked_writer.status.success(),
            "stock SQLite writer must not enter while FrankenSQLite holds WAL_WRITE_LOCK"
        );
        let blocked_stderr = String::from_utf8_lossy(&blocked_writer.stderr).to_ascii_lowercase();
        assert!(
            blocked_stderr.contains("locked") || blocked_stderr.contains("busy"),
            "stock writer should report lock contention, stderr={blocked_stderr:?}"
        );

        file.shm_lock(
            &cx,
            WAL_WRITE_LOCK,
            1,
            SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE,
        )
        .expect("release stock-visible WAL write slot");
        let writer_after_release = Command::new("sqlite3")
            .arg(path_str)
            .arg("PRAGMA busy_timeout=0; INSERT INTO t(x) VALUES (4);")
            .output()
            .expect("run sqlite3 writer after release");
        assert!(
            writer_after_release.status.success(),
            "stock SQLite writer must succeed after WAL_WRITE_LOCK release: {}",
            String::from_utf8_lossy(&writer_after_release.stderr)
        );

        file.shm_lock(
            &cx,
            WAL_CKPT_LOCK,
            1,
            SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE,
        )
        .expect("FrankenSQLite acquires stock-visible WAL checkpoint slot");
        let blocked_checkpoint = Command::new("sqlite3")
            .arg(path_str)
            .arg("PRAGMA busy_timeout=0; PRAGMA wal_checkpoint(TRUNCATE);")
            .output()
            .expect("run contending sqlite3 checkpoint");
        assert!(blocked_checkpoint.status.success());
        let blocked_checkpoint_stdout =
            String::from_utf8(blocked_checkpoint.stdout).expect("checkpoint stdout utf8");
        assert!(
            blocked_checkpoint_stdout
                .lines()
                .any(|line| line.starts_with("1|")),
            "stock checkpoint must report BUSY while FrankenSQLite holds WAL_CKPT_LOCK, stdout={blocked_checkpoint_stdout:?}"
        );
        file.shm_lock(
            &cx,
            WAL_CKPT_LOCK,
            1,
            SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE,
        )
        .expect("release stock-visible WAL checkpoint slot");
        let checkpoint_after_release = Command::new("sqlite3")
            .arg(path_str)
            .arg("PRAGMA busy_timeout=0; PRAGMA wal_checkpoint(TRUNCATE);")
            .output()
            .expect("run sqlite3 checkpoint after release");
        assert!(checkpoint_after_release.status.success());
        let checkpoint_after_release_stdout =
            String::from_utf8(checkpoint_after_release.stdout).expect("checkpoint stdout utf8");
        assert!(
            checkpoint_after_release_stdout
                .lines()
                .any(|line| line.starts_with("0|")),
            "stock checkpoint must succeed after WAL_CKPT_LOCK release, stdout={checkpoint_after_release_stdout:?}"
        );
        file.close(&cx).expect("close vfs file");

        keeper.shutdown();

        let query_output = Command::new("sqlite3")
            .arg(path_str)
            .arg("SELECT count(*) FROM t;")
            .output()
            .expect("run sqlite3 query");
        assert!(query_output.status.success());
        let stdout = String::from_utf8(query_output.stdout).expect("utf8");
        assert_eq!(stdout.trim(), "4");
    }

    #[test]
    fn test_windowsvfs_cfg_gate() {
        let _ = std::any::type_name::<WindowsVfs>();
        let _ = std::any::type_name::<WindowsFile>();
    }
}
