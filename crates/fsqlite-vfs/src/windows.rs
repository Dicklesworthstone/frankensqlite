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
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::LockLevel;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};
use tracing::{debug, info};

use crate::shm::{
    SQLITE_SHM_EXCLUSIVE, SQLITE_SHM_LOCK, SQLITE_SHM_SHARED, SQLITE_SHM_UNLOCK, ShmRegion,
    WAL_CKPT_LOCK, WAL_TOTAL_LOCKS, WAL_WRITE_LOCK,
};
use crate::traits::{FileIdentity, Vfs, VfsFile};

/// SQLite I/O capability bit indicating files cannot be deleted while open.
const SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN: u32 = 0x0000_0800;
const WINDOWS_FILE_SHARE_READ: u32 = 0x0000_0001;
const WINDOWS_FILE_SHARE_WRITE: u32 = 0x0000_0002;
const WINDOWS_SHARE_READ_WRITE: u32 = WINDOWS_FILE_SHARE_READ | WINDOWS_FILE_SHARE_WRITE;

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
const STOCK_SQLITE_WAL_WRITE_BYTE: u64 = STOCK_SQLITE_SHM_LOCK_BASE;
const STOCK_SQLITE_WAL_CKPT_BYTE: u64 = STOCK_SQLITE_SHM_LOCK_BASE + 1;

const LOCKFILE_FAIL_IMMEDIATELY: u32 = 0x0000_0001;
const LOCKFILE_EXCLUSIVE_LOCK: u32 = 0x0000_0002;
const ERROR_LOCK_VIOLATION: i32 = 33;
const ERROR_NOT_LOCKED: i32 = 158;

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

#[derive(Clone, Copy)]
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
    if error.raw_os_error() == Some(ERROR_NOT_LOCKED) {
        Ok(())
    } else {
        Err(FrankenError::Io(error))
    }
}

fn checkpoint_or_abort(cx: &Cx) -> Result<()> {
    cx.checkpoint().map_err(|_| FrankenError::Abort)
}

fn lock_poisoned(name: &str) -> FrankenError {
    FrankenError::internal(format!("{name} lock poisoned"))
}

fn windows_open_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.share_mode(WINDOWS_SHARE_READ_WRITE);
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
}

impl Default for WindowsShmState {
    fn default() -> Self {
        let slot_count = usize::try_from(WAL_TOTAL_LOCKS).expect("WAL_TOTAL_LOCKS must fit usize");
        Self {
            regions: HashMap::new(),
            slots: vec![ShmSlotState::default(); slot_count],
            owner_refs: HashMap::new(),
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

    fn highest_held_level(&self) -> LockLevel {
        [
            LockLevel::Exclusive,
            LockLevel::Pending,
            LockLevel::Reserved,
            LockLevel::Shared,
        ]
        .into_iter()
        .find(|level| self.lock_held(*level))
        .unwrap_or(LockLevel::None)
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

    fn unlock_to(&mut self, level: LockLevel) -> Result<()> {
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
        if failures.is_empty() && self.lock_level != level {
            failures.push(format!(
                "requested {level:?} but only a partial/non-prefix stock lock state remained ({:?})",
                self.lock_level
            ));
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

/// Stock-SQLite-visible WAL portion of an external maintenance fence.
///
/// Ordinary main-file locking already mirrors every level onto stock SQLite's
/// real database bytes. Whole-image maintenance additionally retains the real
/// WAL write/checkpoint slots tracked here.
#[derive(Debug)]
struct WindowsExternalMaintenanceLocks {
    wal_mode: bool,
    shm_file: Option<File>,
    shm_write: bool,
    shm_checkpoint: bool,
}

impl WindowsExternalMaintenanceLocks {
    const fn new(wal_mode: bool) -> Self {
        Self {
            wal_mode,
            shm_file: None,
            shm_write: false,
            shm_checkpoint: false,
        }
    }

    /// Release every acquired range in strict reverse order. A failed explicit
    /// unlock leaves its bit set until this state is dropped; closing the
    /// dedicated handles is Win32's guaranteed final release path.
    fn release(&mut self) -> Result<()> {
        let mut failures = Vec::new();

        if self.shm_checkpoint {
            match self.shm_file.as_ref() {
                Some(shm_file) => {
                    match unlock_stock_sqlite_range(shm_file, STOCK_SQLITE_WAL_CKPT_BYTE, 1) {
                        Ok(()) => self.shm_checkpoint = false,
                        Err(error) => failures.push(format!("WAL checkpoint byte: {error}")),
                    }
                }
                None => failures.push("WAL checkpoint byte: missing -shm handle".to_owned()),
            }
        }
        if self.shm_write {
            match self.shm_file.as_ref() {
                Some(shm_file) => {
                    match unlock_stock_sqlite_range(shm_file, STOCK_SQLITE_WAL_WRITE_BYTE, 1) {
                        Ok(()) => self.shm_write = false,
                        Err(error) => failures.push(format!("WAL write byte: {error}")),
                    }
                }
                None => failures.push("WAL write byte: missing -shm handle".to_owned()),
            }
        }

        if !self.shm_write && !self.shm_checkpoint {
            self.shm_file.take();
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(FrankenError::internal(format!(
                "could not release stock SQLite Windows maintenance ranges: {}",
                failures.join("; ")
            )))
        }
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
        let os_locks = match WindowsOsLockFiles::open(&resolved) {
            Ok(os_locks) => os_locks,
            Err(err) => {
                drop(file);
                if created_db_file {
                    let _ = fs::remove_file(&resolved);
                }
                return Err(err);
            }
        };

        Ok((
            WindowsFile {
                path: resolved,
                file: Some(file),
                os_locks: Some(os_locks),
                stock_main_locks: None,
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
        // Our share mode deliberately omits FILE_SHARE_DELETE, so retaining
        // this guard also prevents a pathname replacement between the
        // preflight and the final read-write handle verification.
        let mut options = windows_open_options();
        let identity_guard = options.read(true).open(&resolved).map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                FrankenError::CannotOpen {
                    path: resolved.clone(),
                }
            } else {
                FrankenError::Io(err)
            }
        })?;
        if FileIdentity::from_file(&identity_guard)? != Some(expected_identity) {
            return Err(FrankenError::CannotOpen { path: resolved });
        }

        let mut existing_flags = flags;
        existing_flags.remove(VfsOpenFlags::CREATE | VfsOpenFlags::EXCLUSIVE);
        let (file, actual_flags) = self.open(cx, Some(&resolved), existing_flags)?;
        if file.file_identity()? != Some(expected_identity) {
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
                FrankenError::CannotOpen {
                    path: resolved.clone(),
                }
            } else {
                FrankenError::Io(err)
            }
        })?;
        if FileIdentity::from_file(&file)? != Some(expected_identity) || file.metadata()?.len() != 0
        {
            return Err(FrankenError::CannotOpen { path: resolved });
        }
        for artifact_path in reserved_database_artifact_paths(&resolved) {
            if filesystem_entry_exists(&artifact_path)? {
                return Err(FrankenError::CannotOpen { path: resolved });
            }
        }

        // Only an accepted reservation may create the Windows advisory-lock
        // sidecars. The live main handle above omits FILE_SHARE_DELETE, so the
        // pathname cannot be replaced between verification and construction.
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

    fn stock_main_locks_mut(&mut self) -> Result<&mut WindowsStockMainLocks> {
        if self.stock_main_locks.is_none() {
            self.stock_main_locks = Some(WindowsStockMainLocks::new(self.file_ref()?.try_clone()?));
        }
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

    fn release_shm_owner_state(&mut self, delete: bool) -> Result<()> {
        let Some(state_arc) = self.shm_state.take() else {
            if delete {
                drop(fs::remove_file(&self.shm_path));
            }
            return Ok(());
        };

        let orphaned = {
            let mut state = state_arc
                .lock()
                .map_err(|_| lock_poisoned("windows shm state"))?;

            for slot in &mut state.slots {
                slot.shared_holders.remove(&self.owner_id);
                if slot.exclusive_owner == Some(self.owner_id) {
                    slot.exclusive_owner = None;
                }
            }

            if let Some(count) = state.owner_refs.get_mut(&self.owner_id) {
                if *count > 1 {
                    *count -= 1;
                } else {
                    state.owner_refs.remove(&self.owner_id);
                }
            }
            state.owner_refs.is_empty()
        };

        if orphaned {
            windows_shm_table().remove_if_orphaned(&self.shm_path, &state_arc)?;
        }

        if delete {
            drop(fs::remove_file(&self.shm_path));
        }

        Ok(())
    }

    fn rollback_ordinary_locks_to(&mut self, level: LockLevel) -> Result<()> {
        let stock_result = self
            .stock_main_locks
            .as_mut()
            .map_or(Ok(()), |locks| locks.unlock_to(level));
        if stock_result.is_err() {
            // A partial UnlockFileEx failure can leave a non-prefix subset of
            // SQLite lock levels (for example RESERVED without SHARED). Drop
            // the dedicated handle before returning the error: CloseHandle is
            // the synchronous final release and the next attempt must rebuild
            // its stock-visible state from NONE rather than trust a stale or
            // structurally incomplete lock ladder.
            drop(self.stock_main_locks.take());
        }
        let cooperative_result = self.os_locks_mut()?.unlock_to(level);
        self.lock_level = std::cmp::min(
            self.stock_main_locks
                .as_ref()
                .map_or(LockLevel::None, |locks| locks.lock_level),
            self.os_locks_ref()?.highest_held_level(),
        );
        match (stock_result, cooperative_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(stock_error), Err(cooperative_error)) => Err(FrankenError::internal(format!(
                "Windows ordinary lock rollback failed on both lock surfaces: stock={stock_error}; cooperative={cooperative_error}"
            ))),
        }
    }

    fn acquire_cooperative_wal_maintenance_locks(&mut self, cx: &Cx, wal_mode: bool) -> Result<()> {
        if wal_mode {
            self.shm_lock(
                cx,
                WAL_WRITE_LOCK,
                WAL_CKPT_LOCK - WAL_WRITE_LOCK + 1,
                SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE,
            )?;
        }
        Ok(())
    }

    fn release_cooperative_wal_maintenance_locks(&mut self, cx: &Cx, wal_mode: bool) -> Result<()> {
        if wal_mode {
            self.shm_lock(
                cx,
                WAL_WRITE_LOCK,
                WAL_CKPT_LOCK - WAL_WRITE_LOCK + 1,
                SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE,
            )
        } else {
            Ok(())
        }
    }

    fn acquire_stock_sqlite_maintenance_locks(&mut self, wal_mode: bool) -> Result<()> {
        if self.external_maintenance_locks.is_some() {
            return Err(FrankenError::internal(
                "stock SQLite Windows maintenance fence is already held",
            ));
        }
        self.ensure_open()?;
        let mut locks = WindowsExternalMaintenanceLocks::new(wal_mode);

        let acquisition_result = (|| -> Result<()> {
            if wal_mode {
                let (shm_file, _) = open_windows_lock_sidecar(&self.shm_path)?;
                locks.shm_file = Some(shm_file);
                let shm_file = locks.shm_file.as_ref().ok_or_else(|| {
                    FrankenError::internal("Windows maintenance lost its -shm handle")
                })?;
                try_lock_stock_sqlite_range(shm_file, STOCK_SQLITE_WAL_WRITE_BYTE, 1)?;
                locks.shm_write = true;
                try_lock_stock_sqlite_range(shm_file, STOCK_SQLITE_WAL_CKPT_BYTE, 1)?;
                locks.shm_checkpoint = true;
            }

            // The ordinary main-file lock path mirrors every SQLite lock level
            // onto the real database bytes. This dedicated state intentionally
            // acquires only the stock-visible WAL slots; the caller acquires
            // main EXCLUSIVE afterward to preserve WAL-then-main ordering.
            Ok(())
        })();

        if let Err(acquisition_error) = acquisition_result {
            let cleanup_result = locks.release();
            // Even if an explicit UnlockFileEx failed, dropping these
            // dedicated handles releases every range before CloseHandle
            // returns. Never strand a partial acquisition on `self.file`.
            drop(locks);
            return match cleanup_result {
                Ok(()) => Err(acquisition_error),
                Err(cleanup_error) => Err(FrankenError::internal(format!(
                    "stock SQLite Windows maintenance lock failed and partial-acquisition unwind also failed: lock={acquisition_error}; unwind={cleanup_error}"
                ))),
            };
        }

        self.external_maintenance_locks = Some(locks);
        Ok(())
    }

    fn release_stock_sqlite_maintenance_locks(&mut self) -> Result<()> {
        let Some(mut locks) = self.external_maintenance_locks.take() else {
            return Ok(());
        };
        let release_result = locks.release();
        // See the acquisition unwind above: handle closure is the final,
        // synchronous lock-release fallback for any range whose explicit
        // UnlockFileEx call failed.
        drop(locks);
        release_result
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

    fn acquire_shared_slot(state: &mut WindowsShmState, slot: u32, owner_id: u64) -> Result<()> {
        let idx = to_slot_index(slot)?;
        let slot_state = state
            .slots
            .get_mut(idx)
            .ok_or_else(|| FrankenError::internal("shm slot index out of bounds"))?;
        if let Some(exclusive_owner) = slot_state.exclusive_owner {
            if exclusive_owner != owner_id {
                return Err(FrankenError::Busy);
            }
        }
        *slot_state.shared_holders.entry(owner_id).or_insert(0) += 1;
        Ok(())
    }

    fn acquire_exclusive_slot(state: &mut WindowsShmState, slot: u32, owner_id: u64) -> Result<()> {
        let idx = to_slot_index(slot)?;
        let slot_state = state
            .slots
            .get_mut(idx)
            .ok_or_else(|| FrankenError::internal("shm slot index out of bounds"))?;

        if slot_state.exclusive_owner == Some(owner_id) {
            return Ok(());
        }

        if slot_state.exclusive_owner.is_some() {
            return Err(FrankenError::Busy);
        }

        if slot_state
            .shared_holders
            .iter()
            .any(|(owner, count)| *owner != owner_id && *count > 0)
        {
            return Err(FrankenError::Busy);
        }

        slot_state.exclusive_owner = Some(owner_id);
        Ok(())
    }

    fn release_shared_slot(state: &mut WindowsShmState, slot: u32, owner_id: u64) -> Result<()> {
        let idx = to_slot_index(slot)?;
        let slot_state = state
            .slots
            .get_mut(idx)
            .ok_or_else(|| FrankenError::internal("shm slot index out of bounds"))?;
        let Some(holder_count) = slot_state.shared_holders.get_mut(&owner_id) else {
            return Err(FrankenError::LockFailed {
                detail: format!("owner {owner_id} does not hold shared SHM slot {slot}"),
            });
        };
        if *holder_count > 1 {
            *holder_count -= 1;
        } else {
            slot_state.shared_holders.remove(&owner_id);
        }
        Ok(())
    }

    fn release_exclusive_slot(state: &mut WindowsShmState, slot: u32, owner_id: u64) -> Result<()> {
        let idx = to_slot_index(slot)?;
        let slot_state = state
            .slots
            .get_mut(idx)
            .ok_or_else(|| FrankenError::internal("shm slot index out of bounds"))?;
        if slot_state.exclusive_owner != Some(owner_id) {
            return Err(FrankenError::LockFailed {
                detail: format!("owner {owner_id} does not hold exclusive SHM slot {slot}"),
            });
        }
        slot_state.exclusive_owner = None;
        Ok(())
    }
}

impl VfsFile for WindowsFile {
    fn close(&mut self, cx: &Cx) -> Result<()> {
        if self.is_closed() && self.shm_state.is_none() {
            return Ok(());
        }

        let mut first_error = if self.external_shared_snapshot_prior_level.is_some() {
            self.unlock_external_shared_snapshot(cx).err()
        } else {
            None
        };
        let external_wal_mode = self
            .external_maintenance_locks
            .as_ref()
            .map(|locks| locks.wal_mode);
        if let Some(wal_mode) = external_wal_mode
            && let Err(error) = self.unlock_external_maintenance(cx, wal_mode)
            && first_error.is_none()
        {
            first_error = Some(error);
        }

        if !self.is_closed() {
            if let Err(err) = self.unlock(cx, LockLevel::None) {
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
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
        drop(self.external_maintenance_locks.take());
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

    fn read(&self, cx: &Cx, buf: &mut [u8], offset: u64) -> Result<usize> {
        checkpoint_or_abort(cx)?;
        let mut total = 0_usize;
        while total < buf.len() {
            let read_offset = offset
                .checked_add(u64::try_from(total).map_err(|_| FrankenError::OutOfRange {
                    what: "read offset".to_string(),
                    value: total.to_string(),
                })?)
                .ok_or_else(|| FrankenError::OutOfRange {
                    what: "read offset".to_string(),
                    value: "overflow".to_string(),
                })?;
            let n = self.file_ref()?.seek_read(&mut buf[total..], read_offset)?;
            if n == 0 {
                break;
            }
            total += n;
        }
        if total < buf.len() {
            buf[total..].fill(0);
        }
        Ok(total)
    }

    fn write(&mut self, cx: &Cx, buf: &[u8], offset: u64) -> Result<()> {
        checkpoint_or_abort(cx)?;
        let mut total = 0_usize;
        while total < buf.len() {
            let write_offset = offset
                .checked_add(u64::try_from(total).map_err(|_| FrankenError::OutOfRange {
                    what: "write offset".to_string(),
                    value: total.to_string(),
                })?)
                .ok_or_else(|| FrankenError::OutOfRange {
                    what: "write offset".to_string(),
                    value: "overflow".to_string(),
                })?;
            let n = self.file_mut()?.seek_write(&buf[total..], write_offset)?;
            if n == 0 {
                return Err(FrankenError::Io(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "seek_write returned 0",
                )));
            }
            total += n;
        }
        Ok(())
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

    fn lock(&mut self, _cx: &Cx, level: LockLevel) -> Result<()> {
        if level <= self.lock_level {
            return Ok(());
        }

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

    fn unlock(&mut self, _cx: &Cx, level: LockLevel) -> Result<()> {
        if level >= self.lock_level {
            return Ok(());
        }
        self.rollback_ordinary_locks_to(level)
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

        let prior_level = self.lock_level;
        self.lock(cx, LockLevel::Shared)?;
        self.external_shared_snapshot_prior_level = Some(prior_level);
        Ok(())
    }

    fn unlock_external_shared_snapshot(&mut self, cx: &Cx) -> Result<()> {
        let Some(prior_level) = self.external_shared_snapshot_prior_level.take() else {
            return Ok(());
        };
        self.unlock(cx, prior_level)
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
        // Every participant uses the same deadlock-free order: WAL
        // writer/checkpointer slots first, then main-file EXCLUSIVE. Acquire
        // the cooperative slots before the matching real `-shm` bytes so a
        // FrankenSQLite peer cannot enter between the two surfaces.
        self.acquire_cooperative_wal_maintenance_locks(cx, wal_mode)?;
        if let Err(stock_error) = self.acquire_stock_sqlite_maintenance_locks(wal_mode) {
            let cooperative_cleanup = self.release_cooperative_wal_maintenance_locks(cx, wal_mode);
            return match cooperative_cleanup {
                Ok(()) => Err(stock_error),
                Err(cleanup_error) => Err(FrankenError::internal(format!(
                    "Windows external maintenance fence failed and cooperative cleanup also failed: stock={stock_error}; cleanup={cleanup_error}"
                ))),
            };
        }
        if let Err(main_error) = self.lock(cx, LockLevel::Exclusive) {
            let stock_cleanup = self.release_stock_sqlite_maintenance_locks();
            let cooperative_cleanup = self.release_cooperative_wal_maintenance_locks(cx, wal_mode);
            return match (stock_cleanup, cooperative_cleanup) {
                (Ok(()), Ok(())) => Err(main_error),
                (stock_result, cooperative_result) => Err(FrankenError::internal(format!(
                    "Windows external maintenance could not acquire main EXCLUSIVE or release its WAL fences: main={main_error}; stock_cleanup={stock_result:?}; cooperative_cleanup={cooperative_result:?}"
                ))),
            };
        }
        Ok(())
    }

    fn unlock_external_maintenance(&mut self, cx: &Cx, wal_mode: bool) -> Result<()> {
        let actual_wal_mode = self
            .external_maintenance_locks
            .as_ref()
            .map_or(wal_mode, |locks| locks.wal_mode);
        let mode_mismatch =
            self.external_maintenance_locks.is_some() && actual_wal_mode != wal_mode;
        // Release in the exact reverse of acquisition: main, real WAL bytes,
        // then the in-process/cooperative WAL slots.
        let main_result = self.unlock(cx, LockLevel::None);
        let stock_result = self.release_stock_sqlite_maintenance_locks();
        let cooperative_result =
            self.release_cooperative_wal_maintenance_locks(cx, actual_wal_mode);

        let release_result = if main_result.is_ok()
            && stock_result.is_ok()
            && cooperative_result.is_ok()
        {
            Ok(())
        } else {
            Err(FrankenError::internal(format!(
                "Windows external maintenance could not release every lock: main={main_result:?}; stock={stock_result:?}; cooperative={cooperative_result:?}"
            )))
        };
        if mode_mismatch {
            return match release_result {
                Ok(()) => Err(FrankenError::internal(format!(
                    "Windows external maintenance unlock mode mismatch: acquired wal_mode={actual_wal_mode}, requested wal_mode={wal_mode}"
                ))),
                Err(release_error) => Err(FrankenError::internal(format!(
                    "Windows external maintenance unlock mode mismatch and cleanup failed: acquired wal_mode={actual_wal_mode}, requested wal_mode={wal_mode}; cleanup={release_error}"
                ))),
            };
        }
        release_result
    }

    fn check_reserved_lock(&self, _cx: &Cx) -> Result<bool> {
        if self.os_locks_ref()?.reserved_locked_by_other()? {
            return Ok(true);
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
                let result = if exclusive_requested {
                    Self::acquire_exclusive_slot(&mut state, slot, self.owner_id)
                } else {
                    Self::acquire_shared_slot(&mut state, slot, self.owner_id)
                };

                if let Err(err) = result {
                    for acquired_slot in acquired.into_iter().rev() {
                        let rollback = if exclusive_requested {
                            Self::release_exclusive_slot(&mut state, acquired_slot, self.owner_id)
                        } else {
                            Self::release_shared_slot(&mut state, acquired_slot, self.owner_id)
                        };
                        if rollback.is_err() {
                            break;
                        }
                    }
                    return Err(err);
                }
                acquired.push(slot);
            }
            return Ok(());
        }

        for slot in offset..end {
            if exclusive_requested {
                Self::release_exclusive_slot(&mut state, slot, self.owner_id)?;
            } else {
                Self::release_shared_slot(&mut state, slot, self.owner_id)?;
            }
        }
        Ok(())
    }

    fn shm_barrier(&self) {
        fence(Ordering::SeqCst);
    }

    fn shm_unmap(&mut self, _cx: &Cx, delete: bool) -> Result<()> {
        self.ensure_open()?;
        self.release_shm_owner_state(delete)
    }
}

impl crate::traits::AsyncVfsDataPath for WindowsFile {}

impl Drop for WindowsFile {
    fn drop(&mut self) {
        if !self.is_closed() || self.shm_state.is_some() {
            let cx = Cx::new();
            let _ = self.close(&cx);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use std::process::Command;
    use tempfile::tempdir;

    struct TempPathCleanup(PathBuf);

    impl Drop for TempPathCleanup {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    fn open_flags_create() -> VfsOpenFlags {
        VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE
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
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open file");

        file.write(&cx, b"hello windows", 0).expect("write");
        let mut buf = [0_u8; 13];
        let n = file.read(&cx, &mut buf, 0).expect("read");
        assert_eq!(n, 13);
        assert_eq!(&buf, b"hello windows");
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
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open file");
        file.write(&cx, b"0123456789", 0).expect("write");

        let mut buf = [0_u8; 4];
        let n = file.read(&cx, &mut buf, 3).expect("read");
        assert_eq!(n, 4);
        assert_eq!(&buf, b"3456");
    }

    #[test]
    fn test_windowsvfs_write_all_at() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("write_at.db");
        let vfs = WindowsVfs::new();
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open file");
        file.write(&cx, b"abcdefghij", 0).expect("write base");
        file.write(&cx, b"WXYZ", 2).expect("write overlay");

        let mut buf = [0_u8; 10];
        let n = file.read(&cx, &mut buf, 0).expect("read");
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
        file.write(&cx, &[7_u8; 4096], 0).expect("write");
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
            .unlock_external_shared_snapshot(&cx)
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
        assert!(snapshot.external_shared_snapshot_prior_level.is_none());
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
            .unlock_external_shared_snapshot(&cx)
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
        snapshot
            .unlock_external_shared_snapshot(&cx)
            .expect("release shared snapshot");

        maintenance
            .lock_external_maintenance(&cx, false)
            .expect("maintenance must succeed after shared snapshot releases");
        maintenance
            .unlock_external_maintenance(&cx, false)
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
            .unlock_external_maintenance(&cx, false)
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
            .unlock_external_maintenance(&cx, true)
            .expect("release WAL external maintenance fence");
        for offset in [STOCK_SQLITE_WAL_WRITE_BYTE, STOCK_SQLITE_WAL_CKPT_BYTE] {
            try_lock_stock_sqlite_range(&probe, offset, 1)
                .expect("released WAL byte must be acquirable");
            unlock_stock_sqlite_range(&probe, offset, 1).expect("unlock WAL probe byte");
        }
        maintenance.close(&cx).unwrap();
    }

    #[test]
    fn test_external_maintenance_partial_wal_acquisition_unwinds_every_lock() {
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

        // The write byte was acquired before checkpoint contention and must
        // have been unwound. Main ranges were never reached and the local
        // cooperative locks must also have been released so a retry can work.
        assert_eq!(maintenance.lock_level, LockLevel::None);
        assert!(
            maintenance.stock_main_locks.is_none(),
            "real WAL contention must be resolved before opening the dedicated stock-main lock handle"
        );
        try_lock_stock_sqlite_range(&blocker, STOCK_SQLITE_WAL_WRITE_BYTE, 1)
            .expect("partial WAL write lock must be unwound");
        unlock_stock_sqlite_range(&blocker, STOCK_SQLITE_WAL_WRITE_BYTE, 1)
            .expect("unlock WAL write probe");
        unlock_stock_sqlite_range(&blocker, STOCK_SQLITE_WAL_CKPT_BYTE, 1)
            .expect("release checkpoint blocker");

        maintenance
            .lock_external_maintenance(&cx, true)
            .expect("retry after contention must succeed");
        maintenance
            .unlock_external_maintenance(&cx, true)
            .expect("retry fence must release cleanly");
        maintenance.close(&cx).unwrap();
    }

    #[test]
    fn test_external_maintenance_partial_main_acquisition_unwinds_every_lock() {
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

        // PENDING and RESERVED were acquired before the SHARED-range
        // contention. Both must have been unwound, as must the FrankenSQLite
        // sidecar locks, so a retry can succeed after the blocker leaves.
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
            .expect("open released -shm lock probe");
        for offset in [STOCK_SQLITE_WAL_WRITE_BYTE, STOCK_SQLITE_WAL_CKPT_BYTE] {
            try_lock_stock_sqlite_range(&shm_probe, offset, 1)
                .expect("main-lock failure must unwind the earlier real WAL fence");
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
            .unlock_external_maintenance(&cx, true)
            .expect("retry fence must release cleanly");
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
        file.shm_lock(&cx, 0, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE)
            .expect("downgrade from exclusive");
        file.shm_lock(&cx, 0, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED)
            .expect("release preserved shared lock");
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
            .arg("CREATE TABLE t(x INTEGER); INSERT INTO t(x) VALUES (1),(2),(3);")
            .status()
            .expect("run sqlite3 create");
        assert!(create_status.success());

        let vfs = WindowsVfs::new();
        let (mut file, _) = vfs
            .open(
                &cx,
                Some(&path),
                VfsOpenFlags::MAIN_DB | VfsOpenFlags::READWRITE,
            )
            .expect("open via windows vfs");
        let mut header = [0_u8; 16];
        let read = file.read(&cx, &mut header, 0).expect("read sqlite header");
        assert_eq!(read, 16);
        assert_eq!(&header, b"SQLite format 3\0");
        file.close(&cx).expect("close vfs file");

        let query_output = Command::new("sqlite3")
            .arg(path_str)
            .arg("SELECT count(*) FROM t;")
            .output()
            .expect("run sqlite3 query");
        assert!(query_output.status.success());
        let stdout = String::from_utf8(query_output.stdout).expect("utf8");
        assert_eq!(stdout.trim(), "3");
    }

    #[test]
    fn test_windowsvfs_cfg_gate() {
        let _ = std::any::type_name::<WindowsVfs>();
        let _ = std::any::type_name::<WindowsFile>();
    }
}
