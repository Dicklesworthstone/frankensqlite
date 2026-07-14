use std::collections::HashMap;
use std::env;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::shm::{
    SQLITE_SHM_EXCLUSIVE, SQLITE_SHM_LOCK, SQLITE_SHM_SHARED, SQLITE_SHM_UNLOCK, ShmRegion,
    WAL_TOTAL_LOCKS,
};
use crate::traits::{FileIdentity, Vfs, VfsFile};
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::LockLevel;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};

const WASM_LINEAR_MEMORY_PAGE_BYTES: usize = 64 * 1024;

/// Shared storage for all files in the memory VFS.
///
/// Each file is stored as a named byte vector. Multiple `MemoryFile` handles
/// can reference the same underlying storage via `Arc<Mutex<..>>`.
#[derive(Debug)]
struct FileStorage {
    data: Vec<u8>,
    identity_id: u64,
}

impl FileStorage {
    fn with_reserved_capacity(bytes: usize) -> Result<Self> {
        let mut data = Vec::new();
        if bytes > 0 {
            data.try_reserve_exact(bytes)
                .map_err(|_| FrankenError::OutOfMemory)?;
        }
        Ok(Self {
            data,
            identity_id: next_memory_identity_id(),
        })
    }
}

/// Configuration for heap growth inside [`MemoryVfs`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryVfsConfig {
    /// Bytes to reserve for a newly created main database file.
    pub initial_reserve_bytes: usize,
    /// Bytes to grow allocations by when reserving additional capacity.
    pub growth_chunk_bytes: usize,
    /// Hard cap for tracked heap bytes inside this VFS.
    pub max_bytes: Option<usize>,
}

impl Default for MemoryVfsConfig {
    fn default() -> Self {
        Self {
            initial_reserve_bytes: 0,
            growth_chunk_bytes: WASM_LINEAR_MEMORY_PAGE_BYTES,
            max_bytes: None,
        }
    }
}

/// Point-in-time tracked heap usage for [`MemoryVfs`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemoryVfsUsageSnapshot {
    pub file_bytes: usize,
    pub file_reserved_bytes: usize,
    pub shm_bytes: usize,
    pub shm_reserved_bytes: usize,
    pub peak_reserved_bytes: usize,
    pub growth_events: u64,
    pub file_count: usize,
    pub shm_region_count: usize,
    pub initial_reserve_bytes: usize,
    pub growth_chunk_bytes: usize,
    pub max_bytes: Option<usize>,
}

impl MemoryVfsUsageSnapshot {
    #[must_use]
    pub const fn live_bytes(self) -> usize {
        self.file_bytes.saturating_add(self.shm_bytes)
    }

    #[must_use]
    pub const fn reserved_bytes(self) -> usize {
        self.file_reserved_bytes
            .saturating_add(self.shm_reserved_bytes)
    }

    #[must_use]
    pub const fn fragmentation_bytes(self) -> usize {
        self.reserved_bytes().saturating_sub(self.live_bytes())
    }
}

#[derive(Debug, Default)]
struct MemoryVfsUsageState {
    file_bytes: usize,
    file_reserved_bytes: usize,
    shm_bytes: usize,
    shm_reserved_bytes: usize,
    peak_reserved_bytes: usize,
    growth_events: u64,
}

impl MemoryVfsUsageState {
    fn total_reserved_bytes(&self) -> usize {
        self.file_reserved_bytes
            .saturating_add(self.shm_reserved_bytes)
    }

    fn refresh_peak(&mut self) {
        self.peak_reserved_bytes = self.peak_reserved_bytes.max(self.total_reserved_bytes());
    }

    fn apply_file_change(
        &mut self,
        old_len: usize,
        old_reserved: usize,
        new_len: usize,
        new_reserved: usize,
    ) {
        self.file_bytes = self
            .file_bytes
            .saturating_sub(old_len)
            .saturating_add(new_len);
        self.file_reserved_bytes = self
            .file_reserved_bytes
            .saturating_sub(old_reserved)
            .saturating_add(new_reserved);
        if new_reserved > old_reserved {
            self.growth_events = self.growth_events.saturating_add(1);
        }
        self.refresh_peak();
    }

    fn apply_shm_change(
        &mut self,
        old_len: usize,
        old_reserved: usize,
        new_len: usize,
        new_reserved: usize,
    ) {
        self.shm_bytes = self
            .shm_bytes
            .saturating_sub(old_len)
            .saturating_add(new_len);
        self.shm_reserved_bytes = self
            .shm_reserved_bytes
            .saturating_sub(old_reserved)
            .saturating_add(new_reserved);
        if new_reserved > old_reserved {
            self.growth_events = self.growth_events.saturating_add(1);
        }
        self.refresh_peak();
    }
}

/// Shared state for the entire memory VFS.
#[derive(Debug)]
struct MemoryVfsInner {
    files: HashMap<PathBuf, Arc<Mutex<FileStorage>>>,
    shm: HashMap<PathBuf, Arc<Mutex<MemoryShmInfo>>>,
    next_temp_id: u64,
    namespace_id: u64,
    config: MemoryVfsConfig,
    usage: MemoryVfsUsageState,
}

// ---------------------------------------------------------------------------
// SHM table — in-process SHM region + lock coordination for MemoryVfs
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct ShmSlotState {
    shared_holders: HashMap<u64, u32>,
    exclusive_owner: Option<u64>,
}

#[derive(Debug)]
struct MemoryShmInfo {
    regions: HashMap<u32, ShmRegion>,
    slots: Vec<ShmSlotState>,
    owner_refs: HashMap<u64, u32>,
}

impl MemoryShmInfo {
    fn new() -> Self {
        // Slots 0..WAL_TOTAL_LOCKS are the legacy WAL lock slots.
        // Slot WAL_TOTAL_LOCKS is reserved (deadman switch in C SQLite).
        let slot_count =
            usize::try_from(WAL_TOTAL_LOCKS.saturating_add(1)).expect("WAL lock count fits usize");
        Self {
            regions: HashMap::new(),
            slots: std::iter::repeat_with(ShmSlotState::default)
                .take(slot_count)
                .collect(),
            owner_refs: HashMap::new(),
        }
    }
}

static SHM_OWNER_SEQ: AtomicU64 = AtomicU64::new(1);
static MEMORY_IDENTITY_SEQ: AtomicU64 = AtomicU64::new(1);

fn next_shm_owner_id() -> u64 {
    SHM_OWNER_SEQ.fetch_add(1, Ordering::Relaxed)
}

fn next_memory_identity_id() -> u64 {
    MEMORY_IDENTITY_SEQ.fetch_add(1, Ordering::Relaxed)
}

fn sqlite_shm_path(path: &Path) -> PathBuf {
    let mut shm = OsString::from(path.as_os_str());
    shm.push("-shm");
    PathBuf::from(shm)
}

/// An in-memory VFS for testing and in-memory databases.
///
/// All files are stored in memory with no persistence. Multiple connections
/// can share the same `MemoryVfs` instance to access the same files.
#[derive(Debug, Clone)]
pub struct MemoryVfs {
    inner: Arc<Mutex<MemoryVfsInner>>,
}

impl Default for MemoryVfs {
    fn default() -> Self {
        Self::new_with_config(MemoryVfsConfig::default())
    }
}

impl MemoryVfs {
    /// Create a new empty in-memory VFS.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new empty in-memory VFS with explicit growth limits.
    pub fn new_with_config(config: MemoryVfsConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(MemoryVfsInner {
                files: HashMap::new(),
                shm: HashMap::new(),
                next_temp_id: 0,
                namespace_id: next_memory_identity_id(),
                config,
                usage: MemoryVfsUsageState::default(),
            })),
        }
    }

    /// Return the configured growth policy.
    pub fn config(&self) -> Result<MemoryVfsConfig> {
        Ok(self.inner.lock().map_err(|_| lock_err())?.config)
    }

    /// Snapshot tracked heap usage.
    pub fn usage_snapshot(&self) -> Result<MemoryVfsUsageSnapshot> {
        let inner = self.inner.lock().map_err(|_| lock_err())?;
        Ok(MemoryVfsUsageSnapshot {
            file_bytes: inner.usage.file_bytes,
            file_reserved_bytes: inner.usage.file_reserved_bytes,
            shm_bytes: inner.usage.shm_bytes,
            shm_reserved_bytes: inner.usage.shm_reserved_bytes,
            peak_reserved_bytes: inner.usage.peak_reserved_bytes,
            growth_events: inner.usage.growth_events,
            file_count: inner.files.len(),
            shm_region_count: inner.shm.values().fold(0, |count, info_arc| {
                count.saturating_add(info_arc.lock().map_or(0, |info| info.regions.len()))
            }),
            initial_reserve_bytes: inner.config.initial_reserve_bytes,
            growth_chunk_bytes: inner.config.growth_chunk_bytes,
            max_bytes: inner.config.max_bytes,
        })
    }
}

fn lock_err() -> FrankenError {
    FrankenError::internal("MemoryVfs lock poisoned")
}

fn checkpoint_or_abort(cx: &Cx) -> Result<()> {
    cx.checkpoint().map_err(|_| FrankenError::Abort)
}

fn u64_to_usize(value: u64, what: &str) -> Result<usize> {
    usize::try_from(value).map_err(|_| FrankenError::OutOfRange {
        what: what.to_string(),
        value: value.to_string(),
    })
}

fn checked_memory_io_range(offset: u64, len: usize, what: &str) -> Result<(usize, usize)> {
    let offset = u64_to_usize(offset, what)?;
    let end = offset
        .checked_add(len)
        .ok_or_else(|| FrankenError::OutOfRange {
            what: format!("{what} + length"),
            value: format!("{offset}+{len}"),
        })?;
    Ok((offset, end))
}

fn round_up_to_chunk(required: usize, chunk: usize) -> usize {
    if chunk <= 1 {
        return required;
    }
    let remainder = required % chunk;
    if remainder == 0 {
        required
    } else {
        required.saturating_add(chunk - remainder)
    }
}

fn next_growth_target(
    current_reserved: usize,
    required_len: usize,
    initial_reserve_bytes: usize,
    growth_chunk_bytes: usize,
) -> usize {
    let chunk = growth_chunk_bytes.max(1);
    let rounded_required = round_up_to_chunk(required_len, chunk);
    let doubled = current_reserved.saturating_mul(2).max(chunk);
    rounded_required.max(doubled).max(initial_reserve_bytes)
}

fn ensure_total_reserved_within_limit(
    usage: &MemoryVfsUsageState,
    current_reserved: usize,
    proposed_reserved: usize,
    max_bytes: Option<usize>,
) -> Result<()> {
    let Some(max_bytes) = max_bytes else {
        return Ok(());
    };
    let total_without_current = usage
        .total_reserved_bytes()
        .saturating_sub(current_reserved);
    let proposed_total = total_without_current.saturating_add(proposed_reserved);
    if proposed_total > max_bytes {
        return Err(FrankenError::OutOfMemory);
    }
    Ok(())
}

fn shm_region_accounting(info: &MemoryShmInfo) -> (usize, usize) {
    info.regions
        .values()
        .fold((0, 0), |(live, reserved), region| {
            match region.heap_len_and_capacity() {
                Some((len, cap)) => (live.saturating_add(len), reserved.saturating_add(cap)),
                None => (live, reserved),
            }
        })
}

impl Vfs for MemoryVfs {
    type File = MemoryFile;

    fn name(&self) -> &'static str {
        "memory"
    }

    #[allow(clippy::significant_drop_tightening)]
    fn open(
        &self,
        cx: &Cx,
        path: Option<&Path>,
        flags: VfsOpenFlags,
    ) -> Result<(Self::File, VfsOpenFlags)> {
        checkpoint_or_abort(cx)?;
        let mut inner = self.inner.lock().map_err(|_| lock_err())?;

        let is_anonymous_temp = path.is_none();
        let resolved_path = if let Some(p) = path {
            p.to_path_buf()
        } else {
            // Generate a unique temporary filename.
            let id = inner.next_temp_id;
            inner.next_temp_id += 1;
            PathBuf::from(format!("__temp_{id}__"))
        };

        let is_create = flags.contains(VfsOpenFlags::CREATE);
        let storage = if let Some(existing) = inner.files.get(&resolved_path) {
            Arc::clone(existing)
        } else if is_create {
            let initial_reserve_bytes = if flags.contains(VfsOpenFlags::MAIN_DB)
                && !flags.contains(VfsOpenFlags::TEMP_DB)
            {
                inner.config.initial_reserve_bytes
            } else {
                0
            };
            ensure_total_reserved_within_limit(
                &inner.usage,
                0,
                initial_reserve_bytes,
                inner.config.max_bytes,
            )?;
            let storage = Arc::new(Mutex::new(FileStorage::with_reserved_capacity(
                initial_reserve_bytes,
            )?));
            inner
                .files
                .insert(resolved_path.clone(), Arc::clone(&storage));
            inner
                .usage
                .apply_file_change(0, 0, 0, initial_reserve_bytes);
            storage
        } else {
            return Err(FrankenError::CannotOpen {
                path: resolved_path,
            });
        };

        drop(inner);

        let shm_owner_id = next_shm_owner_id();
        let shm_path = sqlite_shm_path(&resolved_path);

        let file = MemoryFile {
            path: resolved_path,
            storage,
            lock_level: LockLevel::None,
            // SQLite temp opens pass a null path and expect delete-on-close semantics.
            delete_on_close: flags.contains(VfsOpenFlags::DELETEONCLOSE) || is_anonymous_temp,
            vfs: Arc::clone(&self.inner),
            shm_owner_id,
            shm_path,
            shm_info: None,
        };

        let mut out_flags = flags;
        if is_create {
            out_flags |= VfsOpenFlags::READWRITE;
        }

        Ok((file, out_flags))
    }

    fn delete(&self, _cx: &Cx, path: &Path, _sync_dir: bool) -> Result<()> {
        let mut inner = self.inner.lock().map_err(|_| lock_err())?;
        if let Some(storage) = inner.files.remove(path) {
            let storage = storage.lock().map_err(|_| lock_err())?;
            inner
                .usage
                .apply_file_change(storage.data.len(), storage.data.capacity(), 0, 0);
        }
        // `xDelete` may target either the main database path or the explicit
        // `*-shm` sidecar. Clear both the exact path and the derived SHM sidecar
        // key so delete/recreate cycles do not inherit stale SHM regions/locks.
        for shm_path in [path.to_path_buf(), sqlite_shm_path(path)] {
            if let Some(info_arc) = inner.shm.remove(&shm_path) {
                let info = info_arc.lock().map_err(|_| lock_err())?;
                let (live_bytes, reserved_bytes) = shm_region_accounting(&info);
                inner
                    .usage
                    .apply_shm_change(live_bytes, reserved_bytes, 0, 0);
            }
        }
        Ok(())
    }

    fn access(&self, _cx: &Cx, path: &Path, _flags: AccessFlags) -> Result<bool> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| lock_err())?
            .files
            .contains_key(path))
    }

    fn path_entry_exists(&self, cx: &Cx, path: &Path) -> Result<bool> {
        self.access(cx, path, AccessFlags::EXISTS)
    }

    fn full_pathname(&self, _cx: &Cx, path: &Path) -> Result<PathBuf> {
        if path.is_absolute() {
            Ok(path.to_path_buf())
        } else {
            Ok(env::current_dir()?.join(path))
        }
    }

    fn is_memory(&self) -> bool {
        true
    }
}

/// A file handle in the memory VFS.
///
/// Reads and writes operate on a shared `Vec<u8>` protected by a mutex.
#[derive(Debug)]
pub struct MemoryFile {
    path: PathBuf,
    storage: Arc<Mutex<FileStorage>>,
    lock_level: LockLevel,
    delete_on_close: bool,
    vfs: Arc<Mutex<MemoryVfsInner>>,
    shm_owner_id: u64,
    shm_path: PathBuf,
    shm_info: Option<Arc<Mutex<MemoryShmInfo>>>,
}

impl MemoryFile {
    fn write_into_storage(
        inner: &mut MemoryVfsInner,
        storage: &mut FileStorage,
        buf: &[u8],
        offset: u64,
    ) -> Result<()> {
        if buf.is_empty() {
            return Ok(());
        }

        let (offset, end) = checked_memory_io_range(offset, buf.len(), "write offset")?;

        let old_len = storage.data.len();
        let old_reserved = storage.data.capacity();
        if end > old_reserved {
            let proposed_reserved = next_growth_target(
                old_reserved,
                end,
                inner.config.initial_reserve_bytes,
                inner.config.growth_chunk_bytes,
            );
            ensure_total_reserved_within_limit(
                &inner.usage,
                old_reserved,
                proposed_reserved,
                inner.config.max_bytes,
            )?;
            storage
                .data
                .try_reserve_exact(proposed_reserved.saturating_sub(old_reserved))
                .map_err(|_| FrankenError::OutOfMemory)?;
        }

        if offset == storage.data.len() {
            // Fast path: appending exactly at the end. Skips zero-initialization.
            storage.data.extend_from_slice(buf);
        } else {
            if end > storage.data.len() {
                storage.data.resize(end, 0);
            }

            storage.data[offset..end].copy_from_slice(buf);
        }

        inner.usage.apply_file_change(
            old_len,
            old_reserved,
            storage.data.len(),
            storage.data.capacity(),
        );
        Ok(())
    }

    fn ensure_shm_info(&mut self) -> Result<Arc<Mutex<MemoryShmInfo>>> {
        if let Some(info) = &self.shm_info {
            return Ok(Arc::clone(info));
        }

        let info = {
            let mut inner = self.vfs.lock().map_err(|_| lock_err())?;
            let info_arc = Arc::clone(
                inner
                    .shm
                    .entry(self.shm_path.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(MemoryShmInfo::new()))),
            );

            {
                let mut guard = info_arc.lock().map_err(|_| lock_err())?;
                *guard.owner_refs.entry(self.shm_owner_id).or_insert(0) += 1;
            }
            info_arc
        };

        self.shm_info = Some(Arc::clone(&info));
        Ok(info)
    }

    fn release_shm_owner_state(&mut self, delete: bool) -> Result<()> {
        let Some(info_arc) = self.shm_info.take() else {
            return Ok(());
        };

        let mut inner = self.vfs.lock().map_err(|_| lock_err())?;
        let orphaned_accounting = {
            let mut info = info_arc.lock().map_err(|_| lock_err())?;

            for slot_state in &mut info.slots {
                if slot_state.exclusive_owner == Some(self.shm_owner_id) {
                    slot_state.exclusive_owner = None;
                }
                slot_state.shared_holders.remove(&self.shm_owner_id);
            }

            if let Some(count) = info.owner_refs.get_mut(&self.shm_owner_id) {
                if *count > 1 {
                    *count -= 1;
                } else {
                    info.owner_refs.remove(&self.shm_owner_id);
                }
            }

            info.owner_refs
                .is_empty()
                .then(|| shm_region_accounting(&info))
        };

        let still_names_this_generation = inner
            .shm
            .get(&self.shm_path)
            .is_some_and(|mapped| Arc::ptr_eq(mapped, &info_arc));
        if let Some((live_bytes, reserved_bytes)) = orphaned_accounting {
            if still_names_this_generation {
                inner.shm.remove(&self.shm_path);
                inner
                    .usage
                    .apply_shm_change(live_bytes, reserved_bytes, 0, 0);
            }
        } else if delete && still_names_this_generation {
            // Match SQLite's xShmUnmap(deleteFlag): the shared backing only
            // disappears when the last reference goes away.
            debug_assert!(inner.shm.contains_key(&self.shm_path));
        }

        Ok(())
    }

    fn remove_delete_on_close_storage(&mut self) -> Result<()> {
        if !self.delete_on_close {
            return Ok(());
        }

        let mut inner = self.vfs.lock().map_err(|_| lock_err())?;
        let still_names_this_file = inner
            .files
            .get(&self.path)
            .is_some_and(|storage| Arc::ptr_eq(storage, &self.storage));
        if still_names_this_file && let Some(storage) = inner.files.remove(&self.path) {
            let storage = storage.lock().map_err(|_| lock_err())?;
            inner
                .usage
                .apply_file_change(storage.data.len(), storage.data.capacity(), 0, 0);
        }
        self.delete_on_close = false;
        Ok(())
    }

    fn validate_shm_request(offset: u32, n: u32) -> Result<()> {
        if n == 0 {
            return Err(FrankenError::LockFailed {
                detail: "shm_lock called with n=0".to_string(),
            });
        }
        let Some(end) = offset.checked_add(n) else {
            return Err(FrankenError::LockFailed {
                detail: "shm_lock range overflow".to_string(),
            });
        };
        if end > WAL_TOTAL_LOCKS {
            return Err(FrankenError::LockFailed {
                detail: format!("shm_lock range {offset}..{end} exceeds WAL lock table"),
            });
        }
        Ok(())
    }

    fn acquire_shm_shared_slot(&self, info: &mut MemoryShmInfo, slot: u32) -> Result<()> {
        let slot_idx = usize::try_from(slot).map_err(|_| FrankenError::LockFailed {
            detail: format!("invalid SHM slot {slot}"),
        })?;
        let slot_state = info
            .slots
            .get_mut(slot_idx)
            .ok_or_else(|| FrankenError::LockFailed {
                detail: format!("invalid SHM slot {slot}"),
            })?;

        if let Some(owner) = slot_state.exclusive_owner {
            if owner != self.shm_owner_id {
                return Err(FrankenError::Busy);
            }
            return Ok(());
        }

        *slot_state
            .shared_holders
            .entry(self.shm_owner_id)
            .or_insert(0) += 1;
        Ok(())
    }

    fn release_shm_shared_slot(&self, info: &mut MemoryShmInfo, slot: u32) -> Result<()> {
        let slot_idx = usize::try_from(slot).map_err(|_| FrankenError::LockFailed {
            detail: format!("invalid SHM slot {slot}"),
        })?;
        let slot_state = info
            .slots
            .get_mut(slot_idx)
            .ok_or_else(|| FrankenError::LockFailed {
                detail: format!("invalid SHM slot {slot}"),
            })?;

        let Some(holder_count) = slot_state.shared_holders.get_mut(&self.shm_owner_id) else {
            return Err(FrankenError::LockFailed {
                detail: format!(
                    "owner {} does not hold shared slot {slot}",
                    self.shm_owner_id
                ),
            });
        };

        if *holder_count > 1 {
            *holder_count -= 1;
        } else {
            slot_state.shared_holders.remove(&self.shm_owner_id);
        }
        Ok(())
    }

    fn acquire_shm_exclusive_slot(&self, info: &mut MemoryShmInfo, slot: u32) -> Result<()> {
        let slot_idx = usize::try_from(slot).map_err(|_| FrankenError::LockFailed {
            detail: format!("invalid SHM slot {slot}"),
        })?;
        let slot_state = info
            .slots
            .get_mut(slot_idx)
            .ok_or_else(|| FrankenError::LockFailed {
                detail: format!("invalid SHM slot {slot}"),
            })?;

        if slot_state.exclusive_owner == Some(self.shm_owner_id) {
            return Ok(());
        }
        if slot_state.exclusive_owner.is_some() {
            return Err(FrankenError::Busy);
        }

        let shared_from_others = slot_state
            .shared_holders
            .iter()
            .any(|(owner, count)| *owner != self.shm_owner_id && *count > 0);
        if shared_from_others {
            return Err(FrankenError::Busy);
        }

        // Exclusive lock does NOT clear any shared lock held by this owner,
        // allowing it to be downgraded properly later.
        slot_state.exclusive_owner = Some(self.shm_owner_id);
        Ok(())
    }

    fn release_shm_exclusive_slot(&self, info: &mut MemoryShmInfo, slot: u32) -> Result<()> {
        let slot_idx = usize::try_from(slot).map_err(|_| FrankenError::LockFailed {
            detail: format!("invalid SHM slot {slot}"),
        })?;
        let slot_state = info
            .slots
            .get_mut(slot_idx)
            .ok_or_else(|| FrankenError::LockFailed {
                detail: format!("invalid SHM slot {slot}"),
            })?;

        if slot_state.exclusive_owner != Some(self.shm_owner_id) {
            return Err(FrankenError::LockFailed {
                detail: format!(
                    "owner {} does not hold exclusive slot {slot}",
                    self.shm_owner_id
                ),
            });
        }

        slot_state.exclusive_owner = None;
        Ok(())
    }
}

impl Drop for MemoryFile {
    fn drop(&mut self) {
        // `VfsFile::close` is explicit because native backends can report I/O
        // failures, but Rust callers are not required to invoke it before a
        // handle leaves scope. Never strand WAL-index ownership or SHM locks
        // in that common path. Drop cannot return an error, so cleanup is
        // deliberately best-effort and non-panicking.
        self.lock_level = LockLevel::None;
        let _ = self.release_shm_owner_state(self.delete_on_close);
        let _ = self.remove_delete_on_close_storage();
    }
}

impl VfsFile for MemoryFile {
    fn close(&mut self, cx: &Cx) -> Result<()> {
        // Release any file locks.
        self.unlock(cx, LockLevel::None)?;

        self.release_shm_owner_state(self.delete_on_close)?;
        self.remove_delete_on_close_storage()?;
        Ok(())
    }

    fn file_identity(&self) -> Result<Option<FileIdentity>> {
        // Cloned VFS handles share monotonic process-local namespace/object
        // tokens, while independent VFS instances and delete/recreate cycles
        // receive fresh tokens. Unlike pointer-derived identities, these
        // cannot alias after allocator address reuse. They remain strictly
        // in-process coordination values.
        let namespace = self.vfs.lock().map_err(|_| lock_err())?.namespace_id;
        let object = self.storage.lock().map_err(|_| lock_err())?.identity_id;
        Ok(Some(FileIdentity::from_memory_parts(namespace, object)))
    }

    fn read(&self, cx: &Cx, buf: &mut [u8], offset: u64) -> Result<usize> {
        checkpoint_or_abort(cx)?;
        let storage = self.storage.lock().map_err(|_| lock_err())?;

        let (offset, _) = checked_memory_io_range(offset, buf.len(), "read offset")?;
        let file_len = storage.data.len();

        if offset >= file_len {
            drop(storage);
            buf.fill(0);
            return Ok(0);
        }

        let available = file_len - offset;
        let to_read = buf.len().min(available);
        buf[..to_read].copy_from_slice(&storage.data[offset..offset + to_read]);
        drop(storage);

        // Zero-fill the rest if short read.
        if to_read < buf.len() {
            buf[to_read..].fill(0);
        }

        Ok(to_read)
    }

    #[allow(clippy::significant_drop_tightening)]
    fn write(&mut self, cx: &Cx, buf: &[u8], offset: u64) -> Result<()> {
        checkpoint_or_abort(cx)?;
        let mut inner = self.vfs.lock().map_err(|_| lock_err())?;
        let mut storage = self.storage.lock().map_err(|_| lock_err())?;
        Self::write_into_storage(&mut inner, &mut storage, buf, offset)
    }

    #[allow(clippy::significant_drop_tightening)]
    fn write_page_batch(&mut self, cx: &Cx, writes: &[(u64, &[u8])]) -> Result<()> {
        checkpoint_or_abort(cx)?;
        if writes.is_empty() {
            return Ok(());
        }
        let mut inner = self.vfs.lock().map_err(|_| lock_err())?;
        let mut storage = self.storage.lock().map_err(|_| lock_err())?;

        if writes.len() == 1 {
            let (offset, data) = writes[0];
            return Self::write_into_storage(&mut inner, &mut storage, data, offset);
        }

        let mut normalized_writes = Vec::with_capacity(writes.len());
        let mut required_len = storage.data.len();
        for &(offset, data) in writes {
            if data.is_empty() {
                continue;
            }
            let (offset, end) = checked_memory_io_range(offset, data.len(), "write offset")?;
            required_len = required_len.max(end);
            normalized_writes.push((offset, data));
        }

        if normalized_writes.is_empty() {
            return Ok(());
        }

        let old_len = storage.data.len();
        let old_reserved = storage.data.capacity();
        if required_len > old_reserved {
            let proposed_reserved = next_growth_target(
                old_reserved,
                required_len,
                inner.config.initial_reserve_bytes,
                inner.config.growth_chunk_bytes,
            );
            ensure_total_reserved_within_limit(
                &inner.usage,
                old_reserved,
                proposed_reserved,
                inner.config.max_bytes,
            )?;
            storage
                .data
                .try_reserve_exact(proposed_reserved.saturating_sub(old_reserved))
                .map_err(|_| FrankenError::OutOfMemory)?;
        }

        if required_len > storage.data.len() {
            storage.data.resize(required_len, 0);
        }

        for (offset, data) in normalized_writes {
            let end = offset + data.len();
            if end == storage.data.len() && offset == storage.data.len() - data.len() {
                storage.data[offset..].copy_from_slice(data);
            } else {
                storage.data[offset..end].copy_from_slice(data);
            }
        }
        inner.usage.apply_file_change(
            old_len,
            old_reserved,
            storage.data.len(),
            storage.data.capacity(),
        );
        Ok(())
    }

    fn truncate(&mut self, _cx: &Cx, size: u64) -> Result<()> {
        let size = u64_to_usize(size, "truncate size")?;
        let mut inner = self.vfs.lock().map_err(|_| lock_err())?;
        let mut storage = self.storage.lock().map_err(|_| lock_err())?;
        let old_len = storage.data.len();
        let old_reserved = storage.data.capacity();
        // SQLite xTruncate semantics: only shrink, never extend.
        if size < storage.data.len() {
            storage.data.truncate(size);

            // Opportunistically shrink allocation if we've truncated significantly.
            if storage.data.capacity() > 1024 && size < storage.data.capacity() / 2 {
                storage.data.shrink_to_fit();
            }
        }
        inner.usage.apply_file_change(
            old_len,
            old_reserved,
            storage.data.len(),
            storage.data.capacity(),
        );
        drop(storage);
        Ok(())
    }

    fn sync(&mut self, _cx: &Cx, _flags: SyncFlags) -> Result<()> {
        Ok(())
    }

    fn file_size(&self, _cx: &Cx) -> Result<u64> {
        Ok(self.storage.lock().map_err(|_| lock_err())?.data.len() as u64)
    }

    fn lock(&mut self, _cx: &Cx, level: LockLevel) -> Result<()> {
        // MemoryVfs deliberately does not serialize writers at file scope.
        // FrankenSQLite's page-level MVCC/WAL layer owns conflict detection
        // and concurrent publication; these hooks only preserve the VFS state
        // machine expected by pager call sites.
        if self.lock_level < level {
            self.lock_level = level;
        }
        Ok(())
    }

    fn unlock(&mut self, _cx: &Cx, level: LockLevel) -> Result<()> {
        if self.lock_level > level {
            self.lock_level = level;
        }
        Ok(())
    }

    fn check_reserved_lock(&self, _cx: &Cx) -> Result<bool> {
        Ok(false)
    }

    fn shm_map(
        &mut self,
        cx: &Cx,
        region: u32,
        size: u32,
        extend: bool,
    ) -> Result<crate::shm::ShmRegion> {
        checkpoint_or_abort(cx)?;
        if size == 0 {
            return Err(FrankenError::LockFailed {
                detail: "shm_map size must be > 0".to_string(),
            });
        }

        let shm_info = self.ensure_shm_info()?;
        let mut info = shm_info.lock().map_err(|_| lock_err())?;
        if let Some(existing) = info.regions.get(&region).map(ShmRegion::share) {
            let requested_size = usize::try_from(size).map_err(|_| FrankenError::LockFailed {
                detail: format!("shm_map size too large: {size}"),
            })?;
            if existing.len() >= requested_size {
                drop(info);
                return Ok(existing);
            }
            if !extend {
                drop(info);
                return Err(FrankenError::LockFailed {
                    detail: format!(
                        "shm region {region} is {} bytes, requested {requested_size} bytes without extend",
                        existing.len()
                    ),
                });
            }

            // Grow the region in place to match WAL-index expectations
            // when a caller remaps with a larger size.
            let mut updated_region = existing;
            updated_region.try_resize_heap(requested_size)?;
            info.regions.insert(region, updated_region.share());
            drop(info);
            return Ok(updated_region);
        }
        if !extend {
            drop(info);
            return Err(FrankenError::CannotOpen {
                path: self.shm_path.clone(),
            });
        }

        let map_size = usize::try_from(size).map_err(|_| FrankenError::LockFailed {
            detail: format!("shm_map size too large: {size}"),
        })?;
        let new_region = ShmRegion::new(map_size);
        info.regions.insert(region, new_region.share());
        drop(info);
        Ok(new_region)
    }

    fn shm_lock(&mut self, cx: &Cx, offset: u32, n: u32, flags: u32) -> Result<()> {
        checkpoint_or_abort(cx)?;
        Self::validate_shm_request(offset, n)?;

        let lock_requested = flags & SQLITE_SHM_LOCK != 0;
        let unlock_requested = flags & SQLITE_SHM_UNLOCK != 0;
        if lock_requested == unlock_requested {
            return Err(FrankenError::LockFailed {
                detail: "invalid shm_lock flags (must set exactly one of LOCK/UNLOCK)".to_string(),
            });
        }

        let shared_mode = flags & SQLITE_SHM_SHARED != 0;
        let exclusive_mode = flags & SQLITE_SHM_EXCLUSIVE != 0;
        if shared_mode == exclusive_mode {
            return Err(FrankenError::LockFailed {
                detail: "invalid shm_lock flags (must set exactly one of SHARED/EXCLUSIVE)"
                    .to_string(),
            });
        }

        let shm_info = self.ensure_shm_info()?;
        let mut info = shm_info.lock().map_err(|_| lock_err())?;

        if lock_requested {
            let mut acquired = Vec::new();
            for slot in offset..offset + n {
                let result = if exclusive_mode {
                    self.acquire_shm_exclusive_slot(&mut info, slot)
                } else {
                    self.acquire_shm_shared_slot(&mut info, slot)
                };
                match result {
                    Ok(()) => acquired.push(slot),
                    Err(err) => {
                        for acquired_slot in acquired.into_iter().rev() {
                            if exclusive_mode {
                                let _ = self.release_shm_exclusive_slot(&mut info, acquired_slot);
                            } else {
                                let _ = self.release_shm_shared_slot(&mut info, acquired_slot);
                            }
                        }
                        return Err(err);
                    }
                }
            }
            return Ok(());
        }

        for slot in offset..offset + n {
            if exclusive_mode {
                self.release_shm_exclusive_slot(&mut info, slot)?;
            } else {
                self.release_shm_shared_slot(&mut info, slot)?;
            }
        }

        Ok(())
    }

    fn shm_barrier(&self) {}

    fn shm_unmap(&mut self, cx: &Cx, delete: bool) -> Result<()> {
        checkpoint_or_abort(cx)?;
        self.release_shm_owner_state(delete)
    }
}

impl crate::traits::AsyncVfsDataPath for MemoryFile {}

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
mod tests {
    use super::*;

    fn make_vfs() -> MemoryVfs {
        MemoryVfs::new()
    }

    #[test]
    fn create_and_read_file() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let path = Path::new("test.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut file, _) = vfs.open(&cx, Some(path), flags).unwrap();

        file.write(&cx, b"hello", 0).unwrap();
        assert_eq!(file.file_size(&cx).unwrap(), 5);

        let mut buf = [0u8; 5];
        let n = file.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn test_memory_vfs_write_read_1mb() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let path = Path::new("one_mb.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let payload = (0..(1024 * 1024))
            .map(|idx| u8::try_from(idx % 251).expect("mod value must fit in u8"))
            .collect::<Vec<_>>();

        let (mut file, _) = vfs.open(&cx, Some(path), flags).unwrap();
        file.write(&cx, &payload, 0).unwrap();
        assert_eq!(file.file_size(&cx).unwrap(), payload.len() as u64);

        let mut roundtrip = vec![0_u8; payload.len()];
        let read = file.read(&cx, &mut roundtrip, 0).unwrap();
        assert_eq!(read, payload.len());
        assert_eq!(roundtrip, payload);
    }

    #[test]
    fn read_past_end_zeroes() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let path = Path::new("test.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut file, _) = vfs.open(&cx, Some(path), flags).unwrap();
        file.write(&cx, b"hi", 0).unwrap();

        let mut buf = [0xFFu8; 10];
        let n = file.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf[..2], b"hi");
        assert!(buf[2..].iter().all(|&b| b == 0));
    }

    #[test]
    fn read_from_empty_file() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let path = Path::new("empty.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (file, _) = vfs.open(&cx, Some(path), flags).unwrap();

        let mut buf = [0xFFu8; 4];
        let n = file.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(n, 0);
        assert!(buf.iter().all(|&b| b == 0));
    }

    #[test]
    fn write_extends_file() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut file, _) = vfs.open(&cx, Some(Path::new("test.db")), flags).unwrap();

        file.write(&cx, b"world", 10).unwrap();
        assert_eq!(file.file_size(&cx).unwrap(), 15);

        let mut buf = [0xFFu8; 15];
        file.read(&cx, &mut buf, 0).unwrap();
        assert!(buf[..10].iter().all(|&b| b == 0));
        assert_eq!(&buf[10..], b"world");
    }

    #[test]
    fn truncate() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut file, _) = vfs.open(&cx, Some(Path::new("test.db")), flags).unwrap();
        file.write(&cx, b"hello world", 0).unwrap();
        assert_eq!(file.file_size(&cx).unwrap(), 11);

        file.truncate(&cx, 5).unwrap();
        assert_eq!(file.file_size(&cx).unwrap(), 5);

        let mut buf = [0u8; 5];
        file.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn test_memory_vfs_truncate() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let payload = vec![0xAB_u8; 1024 * 1024];
        let (mut file, _) = vfs
            .open(&cx, Some(Path::new("truncate_1mb.db")), flags)
            .unwrap();
        file.write(&cx, &payload, 0).unwrap();

        file.truncate(&cx, 512 * 1024).unwrap();
        assert_eq!(file.file_size(&cx).unwrap(), 512 * 1024);

        let mut buf = vec![0_u8; 512 * 1024];
        let n = file.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(n, 512 * 1024);
        assert!(buf.iter().all(|byte| *byte == 0xAB));
    }

    #[test]
    fn open_nonexistent_without_create_fails() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::READWRITE;

        let result = vfs.open(&cx, Some(Path::new("nope.db")), flags);
        assert!(result.is_err());
    }

    #[test]
    fn delete_file() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let path = Path::new("test.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut file, _) = vfs.open(&cx, Some(path), flags).unwrap();
        file.write(&cx, b"data", 0).unwrap();
        file.close(&cx).unwrap();

        assert!(vfs.access(&cx, path, AccessFlags::EXISTS).unwrap());
        vfs.delete(&cx, path, false).unwrap();
        assert!(!vfs.access(&cx, path, AccessFlags::EXISTS).unwrap());
    }

    #[test]
    fn delete_on_close() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let path = Path::new("temp.db");
        let flags = VfsOpenFlags::MAIN_DB
            | VfsOpenFlags::CREATE
            | VfsOpenFlags::READWRITE
            | VfsOpenFlags::DELETEONCLOSE;

        let (mut file, _) = vfs.open(&cx, Some(path), flags).unwrap();
        file.write(&cx, b"temp data", 0).unwrap();
        assert!(vfs.access(&cx, path, AccessFlags::EXISTS).unwrap());

        file.close(&cx).unwrap();
        assert!(!vfs.access(&cx, path, AccessFlags::EXISTS).unwrap());
    }

    #[test]
    fn temp_file_auto_naming() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::TEMP_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut file1, _) = vfs.open(&cx, None, flags).unwrap();
        let (mut file2, _) = vfs.open(&cx, None, flags).unwrap();

        file1.write(&cx, b"file1", 0).unwrap();
        file2.write(&cx, b"file2", 0).unwrap();

        let mut buf = [0u8; 5];
        file1.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(&buf, b"file1");

        file2.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(&buf, b"file2");

        assert_eq!(vfs.inner.lock().unwrap().files.len(), 2);
        file1.close(&cx).unwrap();
        file2.close(&cx).unwrap();
        assert_eq!(
            vfs.inner.lock().unwrap().files.len(),
            0,
            "anonymous temp files must be deleted on close"
        );
    }

    #[test]
    fn shared_file_across_handles() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let path = Path::new("shared.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut file1, _) = vfs.open(&cx, Some(path), flags).unwrap();
        file1.write(&cx, b"shared data", 0).unwrap();

        let open_flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::READWRITE;
        let (file2, _) = vfs.open(&cx, Some(path), open_flags).unwrap();
        let mut buf = [0u8; 11];
        let n = file2.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(n, 11);
        assert_eq!(&buf, b"shared data");
    }

    #[test]
    fn full_pathname() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let resolved = vfs.full_pathname(&cx, Path::new("test.db")).unwrap();
        assert!(resolved.is_absolute());

        let already_abs_input = std::env::current_dir().unwrap().join("tmp").join("test.db");
        let already_abs = vfs.full_pathname(&cx, &already_abs_input).unwrap();
        assert_eq!(already_abs, already_abs_input);
    }

    #[test]
    fn locking() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut file, _) = vfs.open(&cx, Some(Path::new("lock.db")), flags).unwrap();

        file.lock(&cx, LockLevel::Shared).unwrap();
        file.lock(&cx, LockLevel::Reserved).unwrap();
        file.lock(&cx, LockLevel::Exclusive).unwrap();

        assert!(!file.check_reserved_lock(&cx).unwrap());

        file.unlock(&cx, LockLevel::Shared).unwrap();
        file.unlock(&cx, LockLevel::None).unwrap();
    }

    #[test]
    fn sync_is_noop() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(&cx, Some(Path::new("sync.db")), flags).unwrap();
        file.sync(&cx, SyncFlags::FULL).unwrap();
    }

    #[test]
    fn write_overwrite() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(&cx, Some(Path::new("test.db")), flags).unwrap();

        file.write(&cx, b"AAAAAAAAAA", 0).unwrap();
        file.write(&cx, b"BB", 3).unwrap();

        let mut buf = [0u8; 10];
        file.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(&buf, b"AAABBAAAAA");
    }

    #[test]
    fn vfs_name() {
        let vfs = make_vfs();
        assert_eq!(vfs.name(), "memory");
    }

    #[test]
    fn vfs_default_current_time() {
        let cx = Cx::new();
        let vfs = make_vfs();
        // Use deterministic time from Cx (no ambient authority).
        cx.set_unix_millis_for_testing(1_700_000_000_000);
        let time = vfs.current_time(&cx);
        #[allow(clippy::cast_precision_loss)]
        let expected = 2_440_587.5 + ((1_700_000_000_000_f64 / 1000.0) / 86_400.0);
        assert!(
            (time - expected).abs() < 1e-9,
            "unexpected julian day: got={time} expected≈{expected}"
        );
    }

    #[test]
    fn vfs_default_randomness() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let mut buf1 = [0u8; 16];
        let mut buf2 = [0u8; 16];
        vfs.randomness(&cx, &mut buf1);
        vfs.randomness(&cx, &mut buf2);
        assert_ne!(buf1, buf2);
    }

    #[test]
    fn page_read_write_roundtrip() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(&cx, Some(Path::new("pages.db")), flags).unwrap();

        let page1 = vec![0xAA_u8; 4096];
        let page2 = vec![0xBB_u8; 4096];

        file.write(&cx, &page1, 0).unwrap();
        file.write(&cx, &page2, 4096).unwrap();
        assert_eq!(file.file_size(&cx).unwrap(), 8192);

        let mut buf = vec![0u8; 4096];
        file.read(&cx, &mut buf, 0).unwrap();
        assert!(buf.iter().all(|&b| b == 0xAA));

        file.read(&cx, &mut buf, 4096).unwrap();
        assert!(buf.iter().all(|&b| b == 0xBB));
    }

    #[test]
    fn clone_vfs_shares_state() {
        let cx = Cx::new();
        let vfs1 = make_vfs();
        let vfs2 = vfs1.clone();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut file, _) = vfs1.open(&cx, Some(Path::new("shared.db")), flags).unwrap();
        file.write(&cx, b"from vfs1", 0).unwrap();

        // vfs2 should see the same file since they share inner state.
        let open_flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::READWRITE;
        let (file2, _) = vfs2
            .open(&cx, Some(Path::new("shared.db")), open_flags)
            .unwrap();
        let mut buf = [0u8; 9];
        let n = file2.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(n, 9);
        assert_eq!(&buf, b"from vfs1");
    }

    #[test]
    fn write_zero_bytes() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(&cx, Some(Path::new("zero.db")), flags).unwrap();

        file.write(&cx, b"abc", 0).unwrap();
        file.write(&cx, b"", 0).unwrap(); // zero-length write
        assert_eq!(file.file_size(&cx).unwrap(), 3);

        let mut buf = [0u8; 3];
        file.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(&buf, b"abc");
    }

    #[test]
    fn write_offset_overflow_returns_error() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs
            .open(&cx, Some(Path::new("overflow_offset.db")), flags)
            .unwrap();

        let err = file.write(&cx, b"ab", u64::MAX).unwrap_err();
        assert!(matches!(err, FrankenError::OutOfRange { .. }));
    }

    #[test]
    fn read_offset_overflow_returns_error() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (file, _) = vfs
            .open(&cx, Some(Path::new("read_overflow_offset.db")), flags)
            .unwrap();

        let mut buf = [0_u8; 2];
        let err = file.read(&cx, &mut buf, u64::MAX).unwrap_err();
        assert!(matches!(err, FrankenError::OutOfRange { .. }));
    }

    #[test]
    fn write_page_batch_offset_overflow_returns_error() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs
            .open(&cx, Some(Path::new("batch_overflow_offset.db")), flags)
            .unwrap();

        let err = file
            .write_page_batch(&cx, &[(u64::MAX, &[1_u8, 2_u8][..])])
            .unwrap_err();
        assert!(matches!(err, FrankenError::OutOfRange { .. }));
    }

    #[test]
    fn read_zero_bytes() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(&cx, Some(Path::new("rz.db")), flags).unwrap();

        file.write(&cx, b"data", 0).unwrap();
        let mut buf = [];
        let n = file.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn read_at_exact_end() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(&cx, Some(Path::new("end.db")), flags).unwrap();

        file.write(&cx, b"12345", 0).unwrap();
        let mut buf = [0xFFu8; 4];
        let n = file.read(&cx, &mut buf, 5).unwrap();
        assert_eq!(n, 0);
        assert!(buf.iter().all(|&b| b == 0));
    }

    #[test]
    fn truncate_to_zero() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(&cx, Some(Path::new("tz.db")), flags).unwrap();

        file.write(&cx, b"content", 0).unwrap();
        file.truncate(&cx, 0).unwrap();
        assert_eq!(file.file_size(&cx).unwrap(), 0);
    }

    #[test]
    fn truncate_beyond_size_is_noop() {
        // Vec::truncate with a length larger than current is a no-op.
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(&cx, Some(Path::new("tb.db")), flags).unwrap();

        file.write(&cx, b"hi", 0).unwrap();
        file.truncate(&cx, 100).unwrap();
        assert_eq!(file.file_size(&cx).unwrap(), 2);
    }

    #[test]
    fn close_without_delete_preserves_file() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let path = Path::new("keep.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut file, _) = vfs.open(&cx, Some(path), flags).unwrap();
        file.write(&cx, b"persist", 0).unwrap();
        file.close(&cx).unwrap();

        assert!(vfs.access(&cx, path, AccessFlags::EXISTS).unwrap());
    }

    #[test]
    fn shm_map_roundtrip_shared_between_handles() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file1, _) = vfs.open(&cx, Some(Path::new("shm.db")), flags).unwrap();
        let (mut file2, _) = vfs.open(&cx, Some(Path::new("shm.db")), flags).unwrap();

        let region1 = file1.shm_map(&cx, 0, 64, true).unwrap();
        {
            let mut g = region1.lock();
            g[0] = 0xAA;
            g[1] = 0xBB;
        }

        let region2 = file2.shm_map(&cx, 0, 64, true).unwrap();
        let (b0, b1) = {
            let g2 = region2.lock();
            (g2[0], g2[1])
        };
        assert_eq!(b0, 0xAA);
        assert_eq!(b1, 0xBB);
    }

    #[test]
    fn shm_map_extend_false_rejects_missing_region() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs
            .open(&cx, Some(Path::new("shm_missing.db")), flags)
            .unwrap();

        let err = file.shm_map(&cx, 2, 64, false).unwrap_err();
        assert!(matches!(err, FrankenError::CannotOpen { .. }));
    }

    #[test]
    fn shm_map_existing_region_grows_on_larger_extend_request() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs
            .open(&cx, Some(Path::new("shm_grow.db")), flags)
            .unwrap();

        let region_small = file.shm_map(&cx, 0, 64, true).unwrap();
        {
            let mut guard = region_small.lock();
            guard[0] = 0x11;
            guard[63] = 0x22;
        }

        let region_big = file.shm_map(&cx, 0, 128, true).unwrap();
        assert_eq!(region_big.len(), 128);
        {
            let guard = region_big.lock();
            assert_eq!(guard[0], 0x11);
            assert_eq!(guard[63], 0x22);
            assert_eq!(guard[127], 0x00);
            drop(guard);
        }
    }

    #[test]
    fn shm_lock_shared_and_exclusive_conflict() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file1, _) = vfs
            .open(&cx, Some(Path::new("shm_lock.db")), flags)
            .unwrap();
        let (mut file2, _) = vfs
            .open(&cx, Some(Path::new("shm_lock.db")), flags)
            .unwrap();

        file1
            .shm_lock(&cx, 0, 1, SQLITE_SHM_LOCK | SQLITE_SHM_SHARED)
            .unwrap();
        file2
            .shm_lock(&cx, 0, 1, SQLITE_SHM_LOCK | SQLITE_SHM_SHARED)
            .unwrap();

        let err = file2
            .shm_lock(&cx, 0, 1, SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)
            .unwrap_err();
        assert!(matches!(err, FrankenError::Busy));

        file2
            .shm_lock(&cx, 0, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED)
            .unwrap();
        file1
            .shm_lock(&cx, 0, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED)
            .unwrap();

        file2
            .shm_lock(&cx, 0, 1, SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)
            .unwrap();
        file2
            .shm_lock(&cx, 0, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE)
            .unwrap();
    }

    #[test]
    fn close_releases_shm_locks() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file1, _) = vfs
            .open(&cx, Some(Path::new("shm_close.db")), flags)
            .unwrap();
        let (mut file2, _) = vfs
            .open(&cx, Some(Path::new("shm_close.db")), flags)
            .unwrap();

        file1
            .shm_lock(&cx, 0, 1, SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)
            .unwrap();
        file1.close(&cx).unwrap();

        file2
            .shm_lock(&cx, 0, 1, SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)
            .unwrap();
    }

    #[test]
    fn drop_releases_shm_locks_and_owner_registration() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let path = Path::new("shm_drop.db");
        let shm_path = sqlite_shm_path(path);
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file1, _) = vfs.open(&cx, Some(path), flags).unwrap();
        let (mut file2, _) = vfs.open(&cx, Some(path), flags).unwrap();

        let _region1 = file1.shm_map(&cx, 0, 64, true).unwrap();
        file1
            .shm_lock(&cx, 0, 1, SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)
            .unwrap();
        drop(file1);

        // A handle that leaves scope without xClose must not strand either
        // its exclusive slot or its owner reference.
        let _region2 = file2.shm_map(&cx, 0, 64, true).unwrap();
        file2
            .shm_lock(&cx, 0, 1, SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)
            .unwrap();
        file2
            .shm_lock(&cx, 0, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE)
            .unwrap();
        drop(file2);

        assert!(
            !vfs.inner.lock().unwrap().shm.contains_key(&shm_path),
            "dropping the last mapped handle must remove orphaned SHM state"
        );
    }

    #[test]
    fn dropping_detached_old_handle_does_not_remove_recreated_shm_generation() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let path = Path::new("shm_recreate_drop_old.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut old_file, _) = vfs.open(&cx, Some(path), flags).unwrap();
        let _old_region = old_file.shm_map(&cx, 0, 64, true).unwrap();
        old_file
            .shm_lock(&cx, 0, 1, SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)
            .unwrap();

        // Detach the old database/SHM generation, then create a replacement
        // at the same pathname before the final old handle is dropped.
        vfs.delete(&cx, path, false).unwrap();
        let (mut replacement, _) = vfs.open(&cx, Some(path), flags).unwrap();
        let _replacement_region = replacement.shm_map(&cx, 0, 64, true).unwrap();
        replacement
            .shm_lock(&cx, 0, 1, SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)
            .unwrap();

        drop(old_file);

        let (mut third, _) = vfs.open(&cx, Some(path), flags).unwrap();
        let _third_region = third.shm_map(&cx, 0, 64, true).unwrap();
        assert!(matches!(
            third.shm_lock(&cx, 0, 1, SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE),
            Err(FrankenError::Busy)
        ));

        replacement
            .shm_lock(&cx, 0, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE)
            .unwrap();
        third
            .shm_lock(&cx, 0, 1, SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)
            .unwrap();
    }

    #[test]
    fn shm_barrier_is_noop() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (file, _) = vfs.open(&cx, Some(Path::new("shmb.db")), flags).unwrap();

        file.shm_barrier(); // should not panic
    }

    #[test]
    fn shm_unmap_succeeds() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(&cx, Some(Path::new("shmu.db")), flags).unwrap();

        file.shm_unmap(&cx, false).unwrap();
        file.shm_unmap(&cx, true).unwrap(); // idempotent
    }

    #[test]
    fn shm_unmap_delete_clears_registered_shm_state() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let path = Path::new("shm_unmap_delete.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(&cx, Some(path), flags).unwrap();

        let shm_path = sqlite_shm_path(path);
        let _region = file.shm_map(&cx, 0, 64, true).unwrap();
        assert!(
            vfs.inner.lock().unwrap().shm.contains_key(&shm_path),
            "mapping SHM should register per-path SHM state"
        );

        file.shm_unmap(&cx, true).unwrap();
        assert!(
            !vfs.inner.lock().unwrap().shm.contains_key(&shm_path),
            "shm_unmap(delete=true) should clear the registered SHM state entry"
        );

        file.close(&cx).unwrap();
    }

    #[test]
    fn shm_unmap_delete_keeps_shared_state_while_other_owner_remains() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let path = Path::new("shm_unmap_shared_delete.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file1, _) = vfs.open(&cx, Some(path), flags).unwrap();
        let (mut file2, _) = vfs.open(&cx, Some(path), flags).unwrap();

        let shm_path = sqlite_shm_path(path);
        let region1 = file1.shm_map(&cx, 0, 64, true).unwrap();
        {
            let mut guard = region1.lock();
            guard[0] = 0x5A;
        }
        let region2 = file2.shm_map(&cx, 0, 64, true).unwrap();
        assert_eq!(region2.lock()[0], 0x5A);

        file1.shm_unmap(&cx, true).unwrap();
        assert!(
            vfs.inner.lock().unwrap().shm.contains_key(&shm_path),
            "delete=true must not clear SHM state while another owner still references it"
        );

        let (mut file3, _) = vfs.open(&cx, Some(path), flags).unwrap();
        let region3 = file3.shm_map(&cx, 0, 64, true).unwrap();
        assert_eq!(
            region3.lock()[0],
            0x5A,
            "new handles must join the existing SHM node rather than a split replacement"
        );

        file2.shm_unmap(&cx, false).unwrap();
        assert!(
            vfs.inner.lock().unwrap().shm.contains_key(&shm_path),
            "shared SHM state should persist until the final owner unmaps"
        );

        file3.shm_unmap(&cx, false).unwrap();
        assert!(
            !vfs.inner.lock().unwrap().shm.contains_key(&shm_path),
            "shared SHM state should clear once the final owner releases it"
        );
    }

    #[test]
    fn delete_nonexistent_is_silent() {
        let cx = Cx::new();
        let vfs = make_vfs();
        // Deleting a path that doesn't exist should not error (HashMap::remove is a no-op).
        vfs.delete(&cx, Path::new("ghost.db"), false).unwrap();
    }

    #[test]
    fn delete_shm_path_clears_shm_state() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let path = Path::new("shm_delete.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(&cx, Some(path), flags).unwrap();

        let shm_path = sqlite_shm_path(path);
        let _region = file.shm_map(&cx, 0, 64, true).unwrap();
        assert!(
            vfs.inner.lock().unwrap().shm.contains_key(&shm_path),
            "mapping SHM should register per-path SHM state"
        );

        vfs.delete(&cx, &shm_path, false).unwrap();
        assert!(
            !vfs.inner.lock().unwrap().shm.contains_key(&shm_path),
            "explicit SHM delete should clear the SHM state table entry"
        );

        file.close(&cx).unwrap();
    }

    #[test]
    fn delete_db_path_clears_derived_shm_state() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let path = Path::new("db_delete.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(&cx, Some(path), flags).unwrap();

        let shm_path = sqlite_shm_path(path);
        let _region = file.shm_map(&cx, 0, 64, true).unwrap();
        assert!(
            vfs.inner.lock().unwrap().shm.contains_key(&shm_path),
            "mapping SHM should register per-path SHM state"
        );

        vfs.delete(&cx, path, false).unwrap();
        assert!(
            !vfs.inner.lock().unwrap().shm.contains_key(&shm_path),
            "deleting the main database path should also clear the derived SHM state entry"
        );

        file.close(&cx).unwrap();
    }

    #[test]
    fn new_file_size_is_zero() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (file, _) = vfs
            .open(&cx, Some(Path::new("empty_sz.db")), flags)
            .unwrap();
        assert_eq!(file.file_size(&cx).unwrap(), 0);
    }

    #[test]
    fn open_with_create_adds_readwrite_flag() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE;
        let (_, out_flags) = vfs.open(&cx, Some(Path::new("flag.db")), flags).unwrap();
        assert!(out_flags.contains(VfsOpenFlags::READWRITE));
    }

    #[test]
    fn access_readwrite_on_existing_file() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut file, _) = vfs.open(&cx, Some(Path::new("acc.db")), flags).unwrap();
        file.write(&cx, b"test", 0).unwrap();

        // MemoryVfs always returns true for access if file exists.
        assert!(
            vfs.access(&cx, Path::new("acc.db"), AccessFlags::READWRITE)
                .unwrap()
        );
    }

    #[test]
    fn access_nonexistent() {
        let cx = Cx::new();
        let vfs = make_vfs();
        assert!(
            !vfs.access(&cx, Path::new("nope.db"), AccessFlags::EXISTS)
                .unwrap()
        );
    }

    #[test]
    fn unlock_below_current_level_is_noop() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(&cx, Some(Path::new("unlock.db")), flags).unwrap();

        file.lock(&cx, LockLevel::Shared).unwrap();
        // Unlocking to a higher level should be a no-op.
        file.unlock(&cx, LockLevel::Exclusive).unwrap();
        // The lock level should remain at Shared (unlock doesn't escalate).
    }

    #[test]
    fn multiple_temp_files_distinct() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::TEMP_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut f1, _) = vfs.open(&cx, None, flags).unwrap();
        let (mut f2, _) = vfs.open(&cx, None, flags).unwrap();
        let (mut f3, _) = vfs.open(&cx, None, flags).unwrap();

        f1.write(&cx, b"one", 0).unwrap();
        f2.write(&cx, b"two", 0).unwrap();
        f3.write(&cx, b"three", 0).unwrap();

        let mut buf = [0u8; 5];
        f1.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(&buf[..3], b"one");

        f2.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(&buf[..3], b"two");

        f3.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(&buf[..5], b"three");
    }

    #[test]
    fn full_pathname_relative_gets_root_prefix() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let result = vfs.full_pathname(&cx, Path::new("foo/bar.db")).unwrap();
        let expected = std::env::current_dir().unwrap().join("foo/bar.db");
        assert_eq!(result, expected);
    }

    #[test]
    fn memory_vfs_default_trait() {
        let vfs = MemoryVfs::default();
        assert_eq!(vfs.name(), "memory");
    }

    #[test]
    fn concurrent_write_via_shared_handle() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut f1, _) = vfs.open(&cx, Some(Path::new("conc.db")), flags).unwrap();
        f1.write(&cx, b"AAAA", 0).unwrap();

        // Open a second handle to the same file.
        let open_flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::READWRITE;
        let (mut f2, _) = vfs
            .open(&cx, Some(Path::new("conc.db")), open_flags)
            .unwrap();
        f2.write(&cx, b"BB", 1).unwrap();

        // Both handles should see the merged result.
        let mut buf = [0u8; 4];
        f1.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(&buf, b"ABBA");
    }

    #[test]
    fn close_resets_lock_to_none() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(&cx, Some(Path::new("clr.db")), flags).unwrap();

        file.lock(&cx, LockLevel::Exclusive).unwrap();
        file.close(&cx).unwrap();
        // Internal lock_level should be reset to None after close.
        assert_eq!(file.lock_level, LockLevel::None);
    }

    #[test]
    fn lock_full_escalation_and_downgrade() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(&cx, Some(Path::new("esc.db")), flags).unwrap();

        // Full SQLite lock escalation: None → Shared → Reserved → Pending → Exclusive
        file.lock(&cx, LockLevel::Shared).unwrap();
        file.lock(&cx, LockLevel::Reserved).unwrap();
        file.lock(&cx, LockLevel::Pending).unwrap();
        file.lock(&cx, LockLevel::Exclusive).unwrap();

        // Full downgrade: Exclusive → Shared → None
        file.unlock(&cx, LockLevel::Shared).unwrap();
        file.unlock(&cx, LockLevel::None).unwrap();

        // The reservation disappears once the handle downgrades below it.
        assert!(!file.check_reserved_lock(&cx).unwrap());
    }

    #[test]
    fn sector_size_and_device_characteristics_defaults() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (file, _) = vfs.open(&cx, Some(Path::new("dev.db")), flags).unwrap();

        assert_eq!(file.sector_size(), 4096);
        assert_eq!(file.device_characteristics(), 0);
    }

    #[test]
    fn sync_with_different_flags() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(&cx, Some(Path::new("sync2.db")), flags).unwrap();

        // All sync flag variants should be no-ops for MemoryVfs.
        file.sync(&cx, SyncFlags::NORMAL).unwrap();
        file.sync(&cx, SyncFlags::FULL).unwrap();
        file.sync(&cx, SyncFlags::DATAONLY).unwrap();
    }

    #[test]
    fn write_page_batch_applies_multiple_writes() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs
            .open(&cx, Some(Path::new("batch_write.db")), flags)
            .unwrap();

        file.write(&cx, b"01234567", 0).unwrap();
        file.write_page_batch(&cx, &[(2, &b"AB"[..]), (8, &b"XYZ"[..])])
            .unwrap();

        let mut buf = [0_u8; 11];
        file.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(&buf, b"01AB4567XYZ");
    }

    #[test]
    fn test_locking_contention() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let path = Path::new("contention.db");

        let (mut f1, _) = vfs.open(&cx, Some(path), flags).unwrap();
        let (mut f2, _) = vfs.open(&cx, Some(path), flags).unwrap();

        // MemoryVfs locking is intentionally non-serializing: locks are local
        // compatibility state while page-level MVCC owns writer conflicts.
        f1.lock(&cx, LockLevel::Shared).unwrap();
        f2.lock(&cx, LockLevel::Shared).unwrap();

        f1.lock(&cx, LockLevel::Reserved).unwrap();
        f2.lock(&cx, LockLevel::Reserved).unwrap();
        assert!(!f2.check_reserved_lock(&cx).unwrap());
        assert!(!f1.check_reserved_lock(&cx).unwrap());

        f1.lock(&cx, LockLevel::Exclusive).unwrap();
        f2.lock(&cx, LockLevel::Exclusive).unwrap();

        // Lock() should not downgrade.
        f1.lock(&cx, LockLevel::Shared).unwrap();
        assert_eq!(f1.lock_level, LockLevel::Exclusive);

        // Unlock() should downgrade.
        f1.unlock(&cx, LockLevel::None).unwrap();
        assert_eq!(f1.lock_level, LockLevel::None);
        f2.unlock(&cx, LockLevel::Shared).unwrap();
        assert_eq!(f2.lock_level, LockLevel::Shared);
    }

    #[test]
    fn file_identity_tracks_shared_storage_not_path_text() {
        let cx = Cx::new();
        let shared_vfs = make_vfs();
        let independent_vfs = make_vfs();
        let path = Path::new("identity.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (shared_first, _) = shared_vfs.open(&cx, Some(path), flags).unwrap();
        let (shared_second, _) = shared_vfs.open(&cx, Some(path), flags).unwrap();
        let (independent, _) = independent_vfs.open(&cx, Some(path), flags).unwrap();

        assert_eq!(
            shared_first.file_identity().unwrap(),
            shared_second.file_identity().unwrap()
        );
        assert_ne!(
            shared_first.file_identity().unwrap(),
            independent.file_identity().unwrap()
        );
    }

    // =========================================================================
    // bd-3u7.4 VFS contract tests: systematic trait method coverage
    // =========================================================================

    const VFS_BEAD: &str = "bd-3u7.4";

    #[test]
    fn test_vfs_contract_sync_idempotency() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs
            .open(&cx, Some(Path::new("sync_idem.db")), flags)
            .unwrap();

        file.write(&cx, b"data", 0).unwrap();
        file.sync(&cx, SyncFlags::NORMAL).unwrap();
        file.sync(&cx, SyncFlags::NORMAL).unwrap();
        file.sync(&cx, SyncFlags::FULL).unwrap();

        let mut buf = [0u8; 4];
        let n = file.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(n, 4);
        assert_eq!(&buf, b"data", "bead_id={VFS_BEAD} case=sync_idempotency");
    }

    #[test]
    fn test_vfs_contract_write_page_batch() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs
            .open(&cx, Some(Path::new("batch_write.db")), flags)
            .unwrap();

        let page_a = vec![0xAA_u8; 4096];
        let page_b = vec![0xBB_u8; 4096];
        let page_c = vec![0xCC_u8; 4096];

        let writes: Vec<(u64, &[u8])> = vec![(0, &page_a), (4096, &page_b), (8192, &page_c)];
        file.write_page_batch(&cx, &writes).unwrap();

        assert_eq!(
            file.file_size(&cx).unwrap(),
            12288,
            "bead_id={VFS_BEAD} case=batch_write_file_size"
        );

        for (offset, expected_fill) in [(0_u64, 0xAA), (4096, 0xBB), (8192, 0xCC)] {
            let mut buf = [0_u8; 4096];
            file.read(&cx, &mut buf, offset).unwrap();
            assert!(
                buf.iter().all(|&b| b == expected_fill),
                "bead_id={VFS_BEAD} case=batch_write_content offset={offset}"
            );
        }
    }

    #[test]
    fn test_vfs_contract_lock_escalation_full_ladder() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs
            .open(&cx, Some(Path::new("lock_ladder.db")), flags)
            .unwrap();

        for level in [LockLevel::Shared, LockLevel::Reserved, LockLevel::Exclusive] {
            file.lock(&cx, level).unwrap();
        }

        for level in [LockLevel::Reserved, LockLevel::Shared, LockLevel::None] {
            file.unlock(&cx, level).unwrap();
        }
    }

    #[test]
    fn test_vfs_contract_close_reopen_persistence() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let path = Path::new("persist.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        {
            let (mut file, _) = vfs.open(&cx, Some(path), flags).unwrap();
            file.write(&cx, b"persisted data", 0).unwrap();
            file.close(&cx).unwrap();
        }

        {
            let (file, _) = vfs.open(&cx, Some(path), flags).unwrap();
            let mut buf = [0u8; 14];
            let n = file.read(&cx, &mut buf, 0).unwrap();
            assert_eq!(n, 14);
            assert_eq!(
                &buf, b"persisted data",
                "bead_id={VFS_BEAD} case=close_reopen_persistence"
            );
        }
    }

    #[test]
    fn test_vfs_contract_access_checks() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let path = Path::new("access_check.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        assert!(
            !vfs.access(&cx, path, AccessFlags::EXISTS).unwrap(),
            "bead_id={VFS_BEAD} case=access_before_create"
        );

        let (mut file, _) = vfs.open(&cx, Some(path), flags).unwrap();
        file.write(&cx, b"x", 0).unwrap();
        file.close(&cx).unwrap();

        assert!(
            vfs.access(&cx, path, AccessFlags::EXISTS).unwrap(),
            "bead_id={VFS_BEAD} case=access_after_create"
        );
        assert!(
            vfs.access(&cx, path, AccessFlags::READWRITE).unwrap(),
            "bead_id={VFS_BEAD} case=access_readwrite"
        );
    }

    #[test]
    fn test_vfs_contract_full_pathname() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let result = vfs.full_pathname(&cx, Path::new("relative.db")).unwrap();
        assert!(
            !result.as_os_str().is_empty(),
            "bead_id={VFS_BEAD} case=full_pathname_non_empty"
        );
    }

    #[test]
    fn test_vfs_contract_randomness_determinism() {
        let cx = Cx::new();
        let vfs_a = make_vfs();
        let vfs_b = make_vfs();

        let mut buf_a = [0u8; 32];
        let mut buf_b = [0u8; 32];
        vfs_a.randomness(&cx, &mut buf_a);
        vfs_b.randomness(&cx, &mut buf_b);

        assert_ne!(
            buf_a, buf_b,
            "bead_id={VFS_BEAD} case=randomness_unique — \
             sequential calls must produce different output"
        );
        assert!(
            buf_a.iter().any(|&b| b != 0),
            "bead_id={VFS_BEAD} case=randomness_nonzero"
        );
    }

    #[test]
    fn test_vfs_contract_overwrite_in_place() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs
            .open(&cx, Some(Path::new("overwrite.db")), flags)
            .unwrap();

        file.write(&cx, b"AAAA", 0).unwrap();
        file.write(&cx, b"BB", 1).unwrap();

        let mut buf = [0u8; 4];
        file.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(&buf, b"ABBA", "bead_id={VFS_BEAD} case=overwrite_in_place");
    }

    #[test]
    fn test_vfs_contract_sector_size_and_characteristics() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (file, _) = vfs.open(&cx, Some(Path::new("props.db")), flags).unwrap();

        assert!(
            file.sector_size() > 0,
            "bead_id={VFS_BEAD} case=sector_size_positive"
        );
        let _ = file.device_characteristics();
    }

    #[test]
    fn test_vfs_contract_is_memory() {
        let vfs = make_vfs();
        assert!(
            vfs.is_memory(),
            "bead_id={VFS_BEAD} case=is_memory_true_for_memory_vfs"
        );
    }

    #[test]
    fn test_vfs_contract_truncate_then_read_zeros() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs
            .open(&cx, Some(Path::new("trunc_zero.db")), flags)
            .unwrap();

        file.write(&cx, &[0xFF; 8192], 0).unwrap();
        file.truncate(&cx, 4096).unwrap();

        let size = file.file_size(&cx).unwrap();
        assert_eq!(size, 4096, "bead_id={VFS_BEAD} case=truncate_size");

        let mut buf = [0xFF_u8; 4096];
        let n = file.read(&cx, &mut buf, 4096).unwrap();
        assert_eq!(
            n, 0,
            "bead_id={VFS_BEAD} case=read_past_truncation_returns_zero_bytes"
        );
    }

    #[test]
    fn test_vfs_contract_vfs_name() {
        let vfs = make_vfs();
        let name = vfs.name();
        assert!(
            !name.is_empty(),
            "bead_id={VFS_BEAD} case=vfs_name_non_empty"
        );
    }

    #[test]
    fn test_vfs_contract_delete_then_access_is_gone() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let path = Path::new("delete_then_access.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut file, _) = vfs.open(&cx, Some(path), flags).unwrap();
        file.write(&cx, b"data", 0).unwrap();
        file.close(&cx).unwrap();

        assert!(vfs.access(&cx, path, AccessFlags::EXISTS).unwrap());
        vfs.delete(&cx, path, false).unwrap();
        assert!(
            !vfs.access(&cx, path, AccessFlags::EXISTS).unwrap(),
            "bead_id={VFS_BEAD} case=delete_then_access_is_gone"
        );
    }

    #[test]
    fn test_vfs_contract_file_size_tracks_writes_and_truncates() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let path = Path::new("size_track.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(&cx, Some(path), flags).unwrap();

        assert_eq!(file.file_size(&cx).unwrap(), 0, "empty file");

        file.write(&cx, &[0xAA; 100], 0).unwrap();
        assert_eq!(
            file.file_size(&cx).unwrap(),
            100,
            "after 100-byte write at 0"
        );

        file.write(&cx, &[0xBB; 50], 200).unwrap();
        assert_eq!(
            file.file_size(&cx).unwrap(),
            250,
            "sparse write at 200 extends file to 250"
        );

        file.truncate(&cx, 80).unwrap();
        assert_eq!(file.file_size(&cx).unwrap(), 80, "truncate to 80");

        file.truncate(&cx, 0).unwrap();
        assert_eq!(file.file_size(&cx).unwrap(), 0, "truncate to 0");
    }

    #[test]
    fn test_vfs_contract_read_beyond_eof_zeros_remainder() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let path = Path::new("read_eof.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(&cx, Some(path), flags).unwrap();

        file.write(&cx, &[0xCC; 10], 0).unwrap();

        let mut buf = [0xFF_u8; 20];
        let n = file.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(n, 10, "short read returns actual bytes read");
        assert_eq!(&buf[..10], &[0xCC; 10], "data portion correct");
        assert!(
            buf[10..].iter().all(|&b| b == 0),
            "remainder zeroed on short read"
        );

        let mut buf2 = [0xFF_u8; 10];
        let n2 = file.read(&cx, &mut buf2, 100).unwrap();
        assert_eq!(n2, 0, "read entirely past EOF returns 0 bytes");
        assert!(
            buf2.iter().all(|&b| b == 0),
            "buffer zeroed when read past EOF"
        );
    }

    #[test]
    fn test_vfs_contract_sparse_write_fills_gap_with_zeros() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let path = Path::new("sparse.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(&cx, Some(path), flags).unwrap();

        file.write(&cx, &[0xDD; 4], 100).unwrap();
        assert_eq!(file.file_size(&cx).unwrap(), 104);

        let mut buf = [0xFF_u8; 104];
        let n = file.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(n, 104);
        assert!(
            buf[..100].iter().all(|&b| b == 0),
            "gap before sparse write must be zeros"
        );
        assert_eq!(&buf[100..], &[0xDD; 4], "sparse-written data intact");
    }

    #[test]
    fn test_vfs_contract_two_handles_share_state() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let path = Path::new("shared.db");
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;

        let (mut f1, _) = vfs.open(&cx, Some(path), flags).unwrap();
        f1.write(&cx, &[0x11; 16], 0).unwrap();

        let (f2, _) = vfs.open(&cx, Some(path), flags).unwrap();
        let mut buf = [0u8; 16];
        let n = f2.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(n, 16);
        assert_eq!(
            &buf, &[0x11; 16],
            "second handle sees first handle's writes"
        );

        assert_eq!(
            f2.file_size(&cx).unwrap(),
            16,
            "second handle reports correct file size"
        );
    }

    #[test]
    fn test_usage_snapshot_live_reserved_fragmentation() {
        let snap = MemoryVfsUsageSnapshot {
            file_bytes: 100,
            file_reserved_bytes: 200,
            shm_bytes: 50,
            shm_reserved_bytes: 80,
            peak_reserved_bytes: 300,
            growth_events: 2,
            file_count: 1,
            shm_region_count: 1,
            initial_reserve_bytes: 0,
            growth_chunk_bytes: 64 * 1024,
            max_bytes: None,
        };
        assert_eq!(snap.live_bytes(), 150);
        assert_eq!(snap.reserved_bytes(), 280);
        assert_eq!(snap.fragmentation_bytes(), 130);
    }

    #[test]
    fn test_memory_vfs_config_default() {
        let cfg = MemoryVfsConfig::default();
        assert_eq!(cfg.initial_reserve_bytes, 0);
        assert_eq!(cfg.growth_chunk_bytes, WASM_LINEAR_MEMORY_PAGE_BYTES);
        assert_eq!(cfg.max_bytes, None);
        let cfg2 = cfg;
        assert_eq!(cfg, cfg2);
    }

    #[test]
    fn test_new_with_config_and_config_accessor() {
        let cfg = MemoryVfsConfig {
            initial_reserve_bytes: 8192,
            growth_chunk_bytes: 4096,
            max_bytes: Some(1_000_000),
        };
        let vfs = MemoryVfs::new_with_config(cfg);
        let retrieved = vfs.config().unwrap();
        assert_eq!(retrieved, cfg);
    }

    #[test]
    fn test_usage_snapshot_default_all_zero() {
        let snap = MemoryVfsUsageSnapshot::default();
        assert_eq!(snap.live_bytes(), 0);
        assert_eq!(snap.reserved_bytes(), 0);
        assert_eq!(snap.fragmentation_bytes(), 0);
        assert_eq!(snap.file_count, 0);
        assert_eq!(snap.growth_events, 0);
        assert_eq!(snap.peak_reserved_bytes, 0);
    }

    #[test]
    fn memory_vfs_config_default_values() {
        let cfg = MemoryVfsConfig::default();
        assert_eq!(cfg.initial_reserve_bytes, 0);
        assert_eq!(cfg.growth_chunk_bytes, WASM_LINEAR_MEMORY_PAGE_BYTES);
        assert!(cfg.max_bytes.is_none());
    }

    #[test]
    fn usage_snapshot_computed_methods_with_data() {
        let snap = MemoryVfsUsageSnapshot {
            file_bytes: 100,
            file_reserved_bytes: 200,
            shm_bytes: 50,
            shm_reserved_bytes: 80,
            peak_reserved_bytes: 300,
            growth_events: 3,
            file_count: 2,
            shm_region_count: 1,
            initial_reserve_bytes: 0,
            growth_chunk_bytes: 65536,
            max_bytes: Some(1_000_000),
        };
        assert_eq!(snap.live_bytes(), 150);
        assert_eq!(snap.reserved_bytes(), 280);
        assert_eq!(snap.fragmentation_bytes(), 130);
    }

    #[test]
    fn usage_snapshot_debug_clone_copy_eq() {
        let a = MemoryVfsUsageSnapshot {
            file_bytes: 10,
            file_reserved_bytes: 20,
            shm_bytes: 5,
            shm_reserved_bytes: 8,
            peak_reserved_bytes: 28,
            growth_events: 1,
            file_count: 1,
            shm_region_count: 0,
            initial_reserve_bytes: 0,
            growth_chunk_bytes: 65536,
            max_bytes: None,
        };
        let copied = a;
        assert_eq!(copied, a);
        let b = MemoryVfsUsageSnapshot {
            file_bytes: 99,
            ..a
        };
        assert_ne!(a, b);
        let dbg = format!("{a:?}");
        assert!(dbg.contains("MemoryVfsUsageSnapshot"));
    }

    #[test]
    fn memory_vfs_with_config_respects_settings() {
        let cfg = MemoryVfsConfig {
            initial_reserve_bytes: 4096,
            growth_chunk_bytes: 8192,
            max_bytes: Some(1_048_576),
        };
        let vfs = MemoryVfs::new_with_config(cfg);
        let snap = vfs.usage_snapshot().unwrap();
        assert_eq!(snap.initial_reserve_bytes, 4096);
        assert_eq!(snap.growth_chunk_bytes, 8192);
        assert_eq!(snap.max_bytes, Some(1_048_576));
    }
}
