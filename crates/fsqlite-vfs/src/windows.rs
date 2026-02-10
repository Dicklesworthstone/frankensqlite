//! Windows VFS implementation.
//!
//! This backend provides the same `Vfs` / `VfsFile` surface as `UnixVfs`,
//! using Windows-friendly file APIs and lock sidecars backed by OS advisory
//! locks (`LockFileEx` via `advisory-lock`) that mirror SQLite lock-level
//! transitions (`NONE` → `SHARED` → `RESERVED` → `PENDING` → `EXCLUSIVE`).

use std::collections::HashMap;
use std::env;
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::os::windows::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering, fence};
use std::sync::{Arc, Mutex, OnceLock};

use advisory_lock::{AdvisoryFileLock, FileLockError, FileLockMode};
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::LockLevel;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};
use tracing::{debug, error, info, warn};

use crate::shm::{
    SQLITE_SHM_EXCLUSIVE, SQLITE_SHM_LOCK, SQLITE_SHM_SHARED, SQLITE_SHM_UNLOCK, ShmRegion,
    WAL_TOTAL_LOCKS,
};
use crate::traits::{Vfs, VfsFile};

/// SQLite I/O capability bit indicating files cannot be deleted while open.
const SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN: u32 = 0x0000_0800;
const PENDING_BYTE: u64 = 0x4000_0000;
const RESERVED_BYTE: u64 = PENDING_BYTE + 1;
const SHARED_FIRST: u64 = PENDING_BYTE + 2;
const SHARED_SIZE: u64 = 510;

fn checkpoint_or_abort(cx: &Cx) -> Result<()> {
    cx.checkpoint().map_err(|_| FrankenError::Abort)
}

fn lock_poisoned(name: &str) -> FrankenError {
    FrankenError::internal(format!("{name} lock poisoned"))
}

fn resolve_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(env::current_dir()?.join(path))
    }
}

fn sqlite_shm_path(path: &Path) -> PathBuf {
    let mut shm: OsString = path.as_os_str().to_owned();
    shm.push("-shm");
    PathBuf::from(shm)
}

fn append_suffix_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value: OsString = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

fn sqlite_shared_lock_path(path: &Path) -> PathBuf {
    append_suffix_path(path, &format!(".lock.{SHARED_FIRST}.{SHARED_SIZE}.shared"))
}

fn sqlite_reserved_lock_path(path: &Path) -> PathBuf {
    append_suffix_path(path, &format!(".lock.{RESERVED_BYTE}.reserved"))
}

fn sqlite_pending_lock_path(path: &Path) -> PathBuf {
    append_suffix_path(path, &format!(".lock.{PENDING_BYTE}.pending"))
}

fn sqlite_exclusive_lock_path(path: &Path) -> PathBuf {
    append_suffix_path(
        path,
        &format!(".lock.{}.exclusive", PENDING_BYTE + SHARED_SIZE + 1),
    )
}

fn sqlite_lock_sidecar_paths(path: &Path) -> [PathBuf; 4] {
    [
        sqlite_shared_lock_path(path),
        sqlite_reserved_lock_path(path),
        sqlite_pending_lock_path(path),
        sqlite_exclusive_lock_path(path),
    ]
}

fn cleanup_lock_sidecars(path: &Path) -> Result<()> {
    for sidecar in sqlite_lock_sidecar_paths(path) {
        if sidecar.exists() {
            fs::remove_file(sidecar)?;
        }
    }
    Ok(())
}

fn ensure_shm_file_len(path: &Path, min_len: u64) -> Result<()> {
    let file = OpenOptions::new()
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
}

#[derive(Debug, Default)]
struct WindowsLockState {
    holders: HashMap<u64, LockLevel>,
}

#[derive(Debug)]
struct WindowsLockTable {
    map: Mutex<HashMap<PathBuf, Arc<Mutex<WindowsLockState>>>>,
}

impl WindowsLockTable {
    fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
        }
    }

    fn get_or_create(&self, path: &Path) -> Result<Arc<Mutex<WindowsLockState>>> {
        let mut map = self
            .map
            .lock()
            .map_err(|_| lock_poisoned("windows lock table"))?;
        Ok(Arc::clone(map.entry(path.to_path_buf()).or_insert_with(
            || Arc::new(Mutex::new(WindowsLockState::default())),
        )))
    }

    fn remove_if_empty(&self, path: &Path) -> Result<()> {
        let state = {
            let map = self
                .map
                .lock()
                .map_err(|_| lock_poisoned("windows lock table"))?;
            map.get(path).cloned()
        };
        let Some(state) = state else {
            return Ok(());
        };
        let empty = state
            .lock()
            .map_err(|_| lock_poisoned("windows lock state"))?
            .holders
            .is_empty();
        if empty {
            let mut map = self
                .map
                .lock()
                .map_err(|_| lock_poisoned("windows lock table"))?;
            map.remove(path);
        }
        Ok(())
    }
}

fn windows_lock_table() -> &'static WindowsLockTable {
    static TABLE: OnceLock<WindowsLockTable> = OnceLock::new();
    TABLE.get_or_init(WindowsLockTable::new)
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

    fn get_or_create(&self, path: &Path) -> Result<Arc<Mutex<WindowsShmState>>> {
        let mut map = self
            .map
            .lock()
            .map_err(|_| lock_poisoned("windows shm table"))?;
        Ok(Arc::clone(map.entry(path.to_path_buf()).or_insert_with(
            || Arc::new(Mutex::new(WindowsShmState::default())),
        )))
    }

    fn remove_if_orphaned(&self, path: &Path) -> Result<()> {
        let state = {
            let map = self
                .map
                .lock()
                .map_err(|_| lock_poisoned("windows shm table"))?;
            map.get(path).cloned()
        };
        let Some(state) = state else {
            return Ok(());
        };
        let orphaned = state
            .lock()
            .map_err(|_| lock_poisoned("windows shm state"))?
            .owner_refs
            .is_empty();
        if orphaned {
            let mut map = self
                .map
                .lock()
                .map_err(|_| lock_poisoned("windows shm table"))?;
            map.remove(path);
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
    exclusive_file: File,
    held_levels: [bool; 4],
}

impl WindowsOsLockFiles {
    fn open(path: &Path) -> Result<Self> {
        let open_sidecar = |sidecar: &Path| -> Result<File> {
            Ok(OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(sidecar)?)
        };

        Ok(Self {
            shared_file: open_sidecar(&sqlite_shared_lock_path(path))?,
            reserved_file: open_sidecar(&sqlite_reserved_lock_path(path))?,
            pending_file: open_sidecar(&sqlite_pending_lock_path(path))?,
            exclusive_file: open_sidecar(&sqlite_exclusive_lock_path(path))?,
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
            LockLevel::Shared => Some(&self.shared_file),
            LockLevel::Reserved => Some(&self.reserved_file),
            LockLevel::Pending => Some(&self.pending_file),
            LockLevel::Exclusive => Some(&self.exclusive_file),
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

        let file = self
            .lock_file_for_level(level)
            .ok_or_else(|| FrankenError::internal("invalid lock level"))?;
        if level == LockLevel::Shared {
            Self::try_lock_shared(file)?;
        } else {
            Self::try_lock_exclusive(file)?;
        }
        self.set_lock_held(level, true);
        Ok(())
    }

    fn unlock_to(&mut self, level: LockLevel) -> Result<()> {
        for held_level in [
            LockLevel::Exclusive,
            LockLevel::Pending,
            LockLevel::Reserved,
            LockLevel::Shared,
        ] {
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

        let resolved = if let Some(path) = path {
            resolve_path(path)?
        } else {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| lock_poisoned("windows vfs inner"))?;
            let id = inner.next_temp_id.max(next_temp_id());
            inner.next_temp_id = id
                .checked_add(1)
                .ok_or_else(|| FrankenError::internal("temp file id overflow"))?;
            env::temp_dir().join(format!("fsqlite-windows-{id}.tmp"))
        };

        let is_create = path.is_none() || flags.contains(VfsOpenFlags::CREATE);
        let is_rw = path.is_none() || flags.contains(VfsOpenFlags::READWRITE) || is_create;
        let is_exclusive_create = is_create && flags.contains(VfsOpenFlags::EXCLUSIVE);

        if !is_create && !resolved.exists() {
            return Err(FrankenError::CannotOpen { path: resolved });
        }

        let mut options = OpenOptions::new();
        options.read(true);
        if is_rw {
            options.write(true);
        }
        if is_exclusive_create {
            options.create_new(true);
        } else if is_create {
            options.create(true);
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

        let lock_state = windows_lock_table().get_or_create(&resolved)?;
        let os_locks = WindowsOsLockFiles::open(&resolved)?;
        let owner_id = next_owner_id();
        let shm_path = sqlite_shm_path(&resolved);

        let delete_on_close = flags.contains(VfsOpenFlags::DELETEONCLOSE) || path.is_none();
        let out_flags = if is_create {
            flags | VfsOpenFlags::READWRITE
        } else {
            flags
        };

        Ok((
            WindowsFile {
                path: resolved,
                file,
                lock_state,
                os_locks,
                owner_id,
                lock_level: LockLevel::None,
                delete_on_close,
                shm_path,
                shm_state: None,
            },
            out_flags,
        ))
    }

    fn delete(&self, _cx: &Cx, path: &Path, _sync_dir: bool) -> Result<()> {
        let resolved = resolve_path(path)?;
        if resolved.exists() {
            fs::remove_file(&resolved)?;
        }
        let shm_path = sqlite_shm_path(&resolved);
        if shm_path.exists() {
            fs::remove_file(shm_path)?;
        }
        cleanup_lock_sidecars(&resolved)?;
        Ok(())
    }

    fn access(&self, _cx: &Cx, path: &Path, flags: AccessFlags) -> Result<bool> {
        let resolved = resolve_path(path)?;
        if !resolved.exists() {
            return Ok(false);
        }
        match flags {
            f if f == AccessFlags::EXISTS => Ok(true),
            f if f == AccessFlags::READ => Ok(File::open(resolved).is_ok()),
            _ => Ok(OpenOptions::new()
                .read(true)
                .write(true)
                .open(resolved)
                .is_ok()),
        }
    }

    fn full_pathname(&self, _cx: &Cx, path: &Path) -> Result<PathBuf> {
        resolve_path(path)
    }
}

/// A file handle opened by [`WindowsVfs`].
#[derive(Debug)]
pub struct WindowsFile {
    path: PathBuf,
    file: File,
    lock_state: Arc<Mutex<WindowsLockState>>,
    os_locks: WindowsOsLockFiles,
    owner_id: u64,
    lock_level: LockLevel,
    delete_on_close: bool,
    shm_path: PathBuf,
    shm_state: Option<Arc<Mutex<WindowsShmState>>>,
}

impl WindowsFile {
    fn can_acquire(state: &WindowsLockState, owner_id: u64, target: LockLevel) -> bool {
        let mut has_shared = false;
        let mut has_reserved = false;
        let mut has_pending = false;
        let mut has_exclusive = false;
        for (owner, level) in &state.holders {
            if *owner == owner_id {
                continue;
            }
            if *level >= LockLevel::Shared {
                has_shared = true;
            }
            if *level >= LockLevel::Reserved {
                has_reserved = true;
            }
            if *level >= LockLevel::Pending {
                has_pending = true;
            }
            if *level >= LockLevel::Exclusive {
                has_exclusive = true;
            }
        }

        match target {
            LockLevel::None => true,
            LockLevel::Shared | LockLevel::Pending => !has_pending && !has_exclusive,
            LockLevel::Reserved => !has_reserved && !has_pending && !has_exclusive,
            LockLevel::Exclusive => !has_shared && !has_reserved && !has_pending && !has_exclusive,
        }
    }

    fn ensure_shm_state(&mut self) -> Result<Arc<Mutex<WindowsShmState>>> {
        if let Some(state) = &self.shm_state {
            return Ok(Arc::clone(state));
        }
        let state = windows_shm_table().get_or_create(&self.shm_path)?;
        {
            let mut guard = state
                .lock()
                .map_err(|_| lock_poisoned("windows shm state"))?;
            *guard.owner_refs.entry(self.owner_id).or_insert(0) += 1;
        }
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
            windows_shm_table().remove_if_orphaned(&self.shm_path)?;
        }

        if delete {
            drop(fs::remove_file(&self.shm_path));
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

        slot_state.shared_holders.remove(&owner_id);
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
        self.unlock(cx, LockLevel::None)?;
        self.release_shm_owner_state(self.delete_on_close)?;
        if self.delete_on_close {
            drop(fs::remove_file(&self.path));
            cleanup_lock_sidecars(&self.path)?;
        }
        windows_lock_table().remove_if_empty(&self.path)?;
        Ok(())
    }

    fn read(&mut self, cx: &Cx, buf: &mut [u8], offset: u64) -> Result<usize> {
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
            let n = self.file.seek_read(&mut buf[total..], read_offset)?;
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
            let n = self.file.seek_write(&buf[total..], write_offset)?;
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
        self.file.set_len(size)?;
        Ok(())
    }

    fn sync(&mut self, _cx: &Cx, flags: SyncFlags) -> Result<()> {
        if flags.contains(SyncFlags::DATAONLY) {
            self.file.sync_data()?;
        } else {
            self.file.sync_all()?;
        }
        Ok(())
    }

    fn file_size(&self, _cx: &Cx) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn lock(&mut self, _cx: &Cx, level: LockLevel) -> Result<()> {
        if level <= self.lock_level {
            return Ok(());
        }

        let mut state = self
            .lock_state
            .lock()
            .map_err(|_| lock_poisoned("windows lock state"))?;

        let mut current = self.lock_level;
        while current < level {
            let next = next_lock_level(current).ok_or_else(|| FrankenError::LockFailed {
                detail: format!("cannot promote lock from {current:?}"),
            })?;

            if !Self::can_acquire(&state, self.owner_id, next) {
                warn!(
                    target: "fsqlite_vfs::windows",
                    requested = ?next,
                    path = %self.path.display(),
                    "windows lock contention"
                );
                return Err(FrankenError::Busy);
            }

            if let Err(err) = self.os_locks.try_lock_level(next) {
                if matches!(err, FrankenError::Busy) {
                    warn!(
                        target: "fsqlite_vfs::windows",
                        requested = ?next,
                        path = %self.path.display(),
                        "windows os lock contention"
                    );
                }
                return Err(err);
            }

            let old_level = current;
            state.holders.insert(self.owner_id, next);
            current = next;
            self.lock_level = current;
            debug!(
                target: "fsqlite_vfs::windows",
                old_level = ?old_level,
                new_level = ?next,
                path = %self.path.display(),
                "windows lock transition"
            );
        }

        drop(state);
        Ok(())
    }

    fn unlock(&mut self, _cx: &Cx, level: LockLevel) -> Result<()> {
        if level >= self.lock_level {
            return Ok(());
        }

        let mut state = self
            .lock_state
            .lock()
            .map_err(|_| lock_poisoned("windows lock state"))?;

        self.os_locks.unlock_to(level)?;

        if level == LockLevel::None {
            state.holders.remove(&self.owner_id);
        } else {
            state.holders.insert(self.owner_id, level);
        }
        debug!(
            target: "fsqlite_vfs::windows",
            old_level = ?self.lock_level,
            new_level = ?level,
            path = %self.path.display(),
            "windows unlock transition"
        );
        self.lock_level = level;
        drop(state);
        windows_lock_table().remove_if_empty(&self.path)?;
        Ok(())
    }

    fn check_reserved_lock(&self, _cx: &Cx) -> Result<bool> {
        if self.lock_level >= LockLevel::Reserved {
            return Ok(false);
        }
        let reserved_path = sqlite_reserved_lock_path(&self.path);

        let state = self
            .lock_state
            .lock()
            .map_err(|_| lock_poisoned("windows lock state"))?;
        if state
            .holders
            .iter()
            .any(|(owner, level)| *owner != self.owner_id && *level >= LockLevel::Reserved)
        {
            return Ok(true);
        }
        drop(state);

        match AdvisoryFileLock::try_lock(&self.os_locks.reserved_file, FileLockMode::Exclusive) {
            Ok(()) => {
                WindowsOsLockFiles::unlock_file(&self.os_locks.reserved_file)?;
                Ok(false)
            }
            Err(FileLockError::AlreadyLocked) => Ok(true),
            Err(FileLockError::Io(err)) => {
                error!(
                    target: "fsqlite_vfs::windows",
                    function_name = "check_reserved_lock",
                    error_code = err.raw_os_error().unwrap_or_default(),
                    file_path = %reserved_path.display(),
                    "windows reserved lock probe failed"
                );
                Err(FrankenError::Io(err))
            }
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
        ensure_shm_file_len(&self.shm_path, min_len)?;

        let shm_state = self.ensure_shm_state()?;
        let mapped_region = {
            let mut state = shm_state
                .lock()
                .map_err(|_| lock_poisoned("windows shm state"))?;

            let entry = state.regions.entry(region);
            let region_ref = match entry {
                std::collections::hash_map::Entry::Occupied(mut occupied) => {
                    if occupied.get().len() < size_usize {
                        let replacement = ShmRegion::new(size_usize);
                        {
                            let current = occupied.get();
                            let old_guard = current.lock();
                            let mut new_guard = replacement.lock();
                            let copy_len = old_guard.len().min(new_guard.len());
                            new_guard[..copy_len].copy_from_slice(&old_guard[..copy_len]);
                        }
                        occupied.insert(replacement);
                    }
                    occupied.into_mut()
                }
                std::collections::hash_map::Entry::Vacant(vacant) => {
                    if !extend {
                        return Err(FrankenError::CannotOpen {
                            path: self.shm_path.clone(),
                        });
                    }
                    vacant.insert(ShmRegion::new(size_usize))
                }
            };
            region_ref.clone()
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
        self.release_shm_owner_state(delete)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use tempfile::tempdir;

    fn open_flags_create() -> VfsOpenFlags {
        VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE
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
    fn test_windowsvfs_lock_escalation_and_contention() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("locks.db");
        let vfs = WindowsVfs::new();
        let (mut file_a, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open A");
        let (mut file_b, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open B");

        file_a.lock(&cx, LockLevel::Shared).expect("shared");
        file_a.lock(&cx, LockLevel::Reserved).expect("reserved");
        assert!(file_b.lock(&cx, LockLevel::Reserved).is_err());
        assert!(file_b.check_reserved_lock(&cx).expect("check"));

        file_a.lock(&cx, LockLevel::Pending).expect("pending");
        assert!(file_b.lock(&cx, LockLevel::Shared).is_err());

        file_b.unlock(&cx, LockLevel::None).expect("unlock B");
        file_a.lock(&cx, LockLevel::Exclusive).expect("exclusive");
        file_a.unlock(&cx, LockLevel::None).expect("unlock A");
        assert!(!file_b.check_reserved_lock(&cx).expect("check"));
    }

    #[test]
    fn test_windowsvfs_lock_escalation() {
        test_windowsvfs_lock_escalation_and_contention();
    }

    #[test]
    fn test_windowsvfs_lock_contention() {
        test_windowsvfs_lock_escalation_and_contention();
    }

    #[test]
    fn test_windowsvfs_reserved_lock_probe_observes_os_lock() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("reserved_probe.db");
        let vfs = WindowsVfs::new();
        let (file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open file");

        let reserved_sidecar = sqlite_reserved_lock_path(&path);
        let reserved_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&reserved_sidecar)
            .expect("open reserved sidecar");
        AdvisoryFileLock::try_lock(&reserved_file, FileLockMode::Exclusive)
            .expect("acquire reserved sidecar lock");

        assert!(file.check_reserved_lock(&cx).expect("reserved probe"));

        AdvisoryFileLock::unlock(&reserved_file).expect("release reserved sidecar lock");
        assert!(!file.check_reserved_lock(&cx).expect("reserved probe clear"));
    }

    #[test]
    fn test_windowsvfs_delete_removes_lock_sidecars() {
        let cx = Cx::new();
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("sidecars.db");
        let vfs = WindowsVfs::new();

        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open file");
        file.lock(&cx, LockLevel::Shared).expect("shared");
        file.unlock(&cx, LockLevel::None).expect("unlock");
        file.close(&cx).expect("close");

        let sidecars = sqlite_lock_sidecar_paths(&path);
        assert!(
            sidecars.iter().all(|path| path.exists()),
            "expected lock sidecars to exist before delete"
        );

        vfs.delete(&cx, &path, false).expect("delete");
        assert!(!path.exists());
        assert!(
            sidecars.iter().all(|path| !path.exists()),
            "expected lock sidecars removed by delete"
        );
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
    fn test_windowsvfs_temp_file_auto_delete() {
        let cx = Cx::new();
        let vfs = WindowsVfs::new();
        let flags = VfsOpenFlags::TEMP_DB
            | VfsOpenFlags::CREATE
            | VfsOpenFlags::READWRITE
            | VfsOpenFlags::DELETEONCLOSE;
        let (mut file, _) = vfs.open(&cx, None, flags).expect("open temp");
        let temp_path = file.path.clone();
        assert!(temp_path.exists());
        file.close(&cx).expect("close");
        assert!(!temp_path.exists());
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
