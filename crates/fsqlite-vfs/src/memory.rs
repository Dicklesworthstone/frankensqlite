use std::collections::HashMap;
use std::env;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::LockLevel;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};

use crate::shm::{
    SQLITE_SHM_EXCLUSIVE, SQLITE_SHM_LOCK, SQLITE_SHM_SHARED, SQLITE_SHM_UNLOCK, ShmRegion,
    WAL_TOTAL_LOCKS,
};
use crate::traits::{Vfs, VfsFile};

/// Shared storage for all files in the memory VFS.
///
/// Each file is stored as a named byte vector. Multiple `MemoryFile` handles
/// can reference the same underlying storage via `Arc<Mutex<..>>`.
#[derive(Debug, Default)]
struct FileStorage {
    data: Vec<u8>,
}

/// Shared state for the entire memory VFS.
#[derive(Debug, Default)]
struct MemoryVfsInner {
    files: HashMap<PathBuf, Arc<Mutex<FileStorage>>>,
    shm: HashMap<PathBuf, Arc<Mutex<MemoryShmInfo>>>,
    next_temp_id: u64,
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

fn next_shm_owner_id() -> u64 {
    SHM_OWNER_SEQ.fetch_add(1, Ordering::Relaxed)
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
#[derive(Debug, Clone, Default)]
pub struct MemoryVfs {
    inner: Arc<Mutex<MemoryVfsInner>>,
}

impl MemoryVfs {
    /// Create a new empty in-memory VFS.
    pub fn new() -> Self {
        Self::default()
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
            let storage = Arc::new(Mutex::new(FileStorage::default()));
            inner
                .files
                .insert(resolved_path.clone(), Arc::clone(&storage));
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
        self.inner
            .lock()
            .map_err(|_| lock_err())?
            .files
            .remove(path);
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

    fn full_pathname(&self, _cx: &Cx, path: &Path) -> Result<PathBuf> {
        if path.is_absolute() {
            Ok(path.to_path_buf())
        } else {
            Ok(env::current_dir()?.join(path))
        }
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
    fn ensure_shm_info(&mut self) -> Result<Arc<Mutex<MemoryShmInfo>>> {
        if let Some(info) = &self.shm_info {
            return Ok(Arc::clone(info));
        }

        let info = {
            let mut inner = self.vfs.lock().map_err(|_| lock_err())?;
            Arc::clone(
                inner
                    .shm
                    .entry(self.shm_path.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(MemoryShmInfo::new()))),
            )
        };

        {
            let mut guard = info.lock().map_err(|_| lock_err())?;
            *guard.owner_refs.entry(self.shm_owner_id).or_insert(0) += 1;
        }

        self.shm_info = Some(Arc::clone(&info));
        Ok(info)
    }

    fn release_shm_owner_state(&mut self, _delete: bool) -> Result<()> {
        let Some(info_arc) = self.shm_info.take() else {
            return Ok(());
        };

        let orphaned = {
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

            info.owner_refs.is_empty()
        };

        if orphaned {
            let mut inner = self.vfs.lock().map_err(|_| lock_err())?;
            inner.shm.remove(&self.shm_path);
        }

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

        // Exclusive lock clears any shared lock held by this owner.
        slot_state.shared_holders.remove(&self.shm_owner_id);
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

impl VfsFile for MemoryFile {
    fn close(&mut self, cx: &Cx) -> Result<()> {
        // Release any file locks.
        self.unlock(cx, LockLevel::None)?;

        self.release_shm_owner_state(self.delete_on_close)?;
        if self.delete_on_close {
            self.vfs
                .lock()
                .map_err(|_| lock_err())?
                .files
                .remove(&self.path);
        }
        Ok(())
    }

    fn read(&mut self, cx: &Cx, buf: &mut [u8], offset: u64) -> Result<usize> {
        checkpoint_or_abort(cx)?;
        let storage = self.storage.lock().map_err(|_| lock_err())?;

        let offset = u64_to_usize(offset, "read offset")?;
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
        let mut storage = self.storage.lock().map_err(|_| lock_err())?;

        let offset = u64_to_usize(offset, "write offset")?;
        let end = offset
            .checked_add(buf.len())
            .ok_or_else(|| FrankenError::OutOfRange {
                what: "write end offset".to_string(),
                value: format!("offset={offset}, len={}", buf.len()),
            })?;

        if end > storage.data.len() {
            storage.data.resize(end, 0);
        }

        storage.data[offset..end].copy_from_slice(buf);
        Ok(())
    }

    fn truncate(&mut self, _cx: &Cx, size: u64) -> Result<()> {
        let size = u64_to_usize(size, "truncate size")?;
        let mut storage = self.storage.lock().map_err(|_| lock_err())?;
        storage.data.resize(size, 0);

        // Opportunistically shrink allocation if we've truncated significantly.
        // Heuristic: if size < capacity / 2, shrink.
        if storage.data.capacity() > 1024 && size < storage.data.capacity() / 2 {
            storage.data.shrink_to_fit();
        }
        Ok(())
    }

    fn sync(&mut self, _cx: &Cx, _flags: SyncFlags) -> Result<()> {
        Ok(())
    }

    fn file_size(&self, _cx: &Cx) -> Result<u64> {
        Ok(self.storage.lock().map_err(|_| lock_err())?.data.len() as u64)
    }

    fn lock(&mut self, _cx: &Cx, level: LockLevel) -> Result<()> {
        // FrankenSQLite does not use SQLite-style file-lock escalation to serialize writers.
        // MVCC provides concurrency at the page level; MemoryVfs file locks are intentionally
        // minimal stubs for compatibility with call sites that expect lock/unlock hooks.
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
        // MemoryVfs does not coordinate cross-handle RESERVED locks.
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
        if let Some(existing) = info.regions.get(&region).cloned() {
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

            // Grow the region while preserving existing bytes to match WAL-index
            // expectations when a caller remaps with a larger size.
            let replacement = ShmRegion::new(requested_size);
            {
                let existing_guard = existing.lock();
                let mut replacement_guard = replacement.lock();
                let copy_len = existing_guard.len().min(replacement_guard.len());
                replacement_guard[..copy_len].copy_from_slice(&existing_guard[..copy_len]);
            }
            info.regions.insert(region, replacement.clone());
            drop(info);
            return Ok(replacement);
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
        info.regions.insert(region, new_region.clone());
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

        let (mut file, _) = vfs.open(&cx, Some(path), flags).unwrap();

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
        let (mut file2, _) = vfs.open(&cx, Some(path), open_flags).unwrap();
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
        let (mut file2, _) = vfs2
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
    fn delete_nonexistent_is_silent() {
        let cx = Cx::new();
        let vfs = make_vfs();
        // Deleting a path that doesn't exist should not error (HashMap::remove is a no-op).
        vfs.delete(&cx, Path::new("ghost.db"), false).unwrap();
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

        // check_reserved_lock still false (MemoryVfs has no cross-connection locking).
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
    fn test_locking_contention() {
        let cx = Cx::new();
        let vfs = make_vfs();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let path = Path::new("contention.db");

        let (mut f1, _) = vfs.open(&cx, Some(path), flags).unwrap();
        let (mut f2, _) = vfs.open(&cx, Some(path), flags).unwrap();

        // MemoryVfs locking is intentionally non-serializing: locks are local stubs.
        f1.lock(&cx, LockLevel::Shared).unwrap();
        f2.lock(&cx, LockLevel::Shared).unwrap();

        f1.lock(&cx, LockLevel::Reserved).unwrap();
        f2.lock(&cx, LockLevel::Reserved).unwrap();

        // check_reserved_lock is always false (no cross-handle RESERVED coordination).
        assert!(!f2.check_reserved_lock(&cx).unwrap());
        assert!(!f1.check_reserved_lock(&cx).unwrap());

        // Locks never block other handles.
        f1.lock(&cx, LockLevel::Exclusive).unwrap();
        f2.lock(&cx, LockLevel::Exclusive).unwrap();

        // Lock() should not downgrade.
        f1.lock(&cx, LockLevel::Shared).unwrap();
        assert_eq!(f1.lock_level, LockLevel::Exclusive);

        f1.unlock(&cx, LockLevel::None).unwrap();
        assert_eq!(f1.lock_level, LockLevel::None);

        // Unlock() should downgrade.
        f2.lock(&cx, LockLevel::Shared).unwrap();
        assert_eq!(f2.lock_level, LockLevel::Exclusive);

        f2.unlock(&cx, LockLevel::Shared).unwrap();
        assert_eq!(f2.lock_level, LockLevel::Shared);
    }
}
