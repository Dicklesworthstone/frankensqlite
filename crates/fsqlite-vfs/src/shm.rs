//! Safe shared-memory region handle for WAL index coordination.
//!
//! This replaces raw `*mut u8` pointers in the [`VfsFile`] SHM API with a safe,
//! bounds-checked wrapper.
//!
//! Note: this type is intentionally backend-agnostic. Concrete VFS backends can
//! construct `ShmRegion` from their own backing storage (in-process heap buffers
//! for `MemoryVfs`, mmap-backed regions for `UnixVfs`, etc.).
//!
//! [`VfsFile`]: crate::traits::VfsFile

use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, MutexGuard};

/// `xShmLock` flag: unlock the requested slot range.
pub const SQLITE_SHM_UNLOCK: u32 = 0x01;
/// `xShmLock` flag: lock the requested slot range.
pub const SQLITE_SHM_LOCK: u32 = 0x02;
/// `xShmLock` flag: shared lock mode for the requested slot range.
pub const SQLITE_SHM_SHARED: u32 = 0x04;
/// `xShmLock` flag: exclusive lock mode for the requested slot range.
pub const SQLITE_SHM_EXCLUSIVE: u32 = 0x08;

/// Legacy SQLite WAL lock slot for writer coordination.
pub const WAL_WRITE_LOCK: u32 = 0;
/// Legacy SQLite WAL lock slot for checkpoint coordination.
pub const WAL_CKPT_LOCK: u32 = 1;
/// Legacy SQLite WAL lock slot for recovery coordination.
pub const WAL_RECOVER_LOCK: u32 = 2;
/// Legacy SQLite WAL lock slot base for reader slots.
pub const WAL_READ_LOCK_BASE: u32 = 3;
/// Number of legacy SQLite WAL reader lock slots.
pub const WAL_NREADER: u32 = 5;
/// Number of legacy SQLite WAL reader lock slots (`usize` form).
pub const WAL_NREADER_USIZE: usize = 5;
/// Total number of legacy SQLite WAL lock slots.
pub const WAL_TOTAL_LOCKS: u32 = WAL_READ_LOCK_BASE + WAL_NREADER;

/// Legacy SQLite POSIX SHM lock-byte base offset in the `*-shm` file.
const SQLITE_SHM_LOCK_BASE: u64 = 120;

/// Return the lock slot for `WAL_READ_LOCK(i)`.
#[must_use]
pub const fn wal_read_lock_slot(index: u32) -> Option<u32> {
    if index < WAL_NREADER {
        Some(WAL_READ_LOCK_BASE + index)
    } else {
        None
    }
}

/// Return the byte offset in the `*-shm` lock area for a WAL lock slot.
#[must_use]
pub const fn wal_lock_byte(slot: u32) -> Option<u64> {
    if slot < WAL_TOTAL_LOCKS {
        Some(SQLITE_SHM_LOCK_BASE + slot as u64)
    } else {
        None
    }
}

/// A handle to a mapped shared-memory region.
///
/// Provides safe, bounds-checked byte-level access to SHM regions used for
/// WAL index coordination. No raw pointers in the public API.
///
/// # Region semantics
///
/// Each region is a fixed-size chunk (typically 32 KB) of the SHM file.
/// Regions are 0-indexed and grow on demand when `VfsFile::shm_map` is
/// called with `extend = true`.
#[derive(Debug, Clone)]
pub struct ShmRegion {
    len: usize,
    data: Arc<Mutex<Vec<u8>>>,
}

impl ShmRegion {
    /// Create a new zeroed SHM region of the given size.
    #[must_use]
    pub fn new(size: usize) -> Self {
        Self {
            len: size,
            data: Arc::new(Mutex::new(vec![0; size])),
        }
    }

    /// Create a region from existing data.
    #[must_use]
    pub fn from_vec(data: Vec<u8>) -> Self {
        let len = data.len();
        Self {
            len,
            data: Arc::new(Mutex::new(data)),
        }
    }

    /// The size of this region in bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether this region is empty (zero-length).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Acquire a lock and borrow the region as a byte slice.
    ///
    /// The returned guard derefs to `&[u8]` / `&mut [u8]` and releases the lock
    /// on drop.
    #[must_use]
    pub fn lock(&self) -> ShmRegionGuard<'_> {
        ShmRegionGuard {
            guard: self.data.lock().unwrap_or_else(|e| e.into_inner()),
        }
    }

    /// Read a little-endian `u32` at the given byte offset.
    ///
    /// # Panics
    ///
    /// Panics if `offset + 4 > self.len()`.
    #[must_use]
    pub fn read_u32_le(&self, offset: usize) -> u32 {
        let bytes: [u8; 4] = {
            let guard = self.lock();
            guard[offset..offset + 4]
                .try_into()
                .expect("slice is exactly 4 bytes")
        };
        u32::from_le_bytes(bytes)
    }

    /// Write a little-endian `u32` at the given byte offset.
    ///
    /// # Panics
    ///
    /// Panics if `offset + 4 > self.len()`.
    pub fn write_u32_le(&self, offset: usize, val: u32) {
        let mut guard = self.lock();
        guard[offset..offset + 4].copy_from_slice(&val.to_le_bytes());
    }

    /// Read a little-endian `u64` at the given byte offset.
    ///
    /// # Panics
    ///
    /// Panics if `offset + 8 > self.len()`.
    #[must_use]
    pub fn read_u64_le(&self, offset: usize) -> u64 {
        let bytes: [u8; 8] = {
            let guard = self.lock();
            guard[offset..offset + 8]
                .try_into()
                .expect("slice is exactly 8 bytes")
        };
        u64::from_le_bytes(bytes)
    }

    /// Write a little-endian `u64` at the given byte offset.
    ///
    /// # Panics
    ///
    /// Panics if `offset + 8 > self.len()`.
    pub fn write_u64_le(&self, offset: usize, val: u64) {
        let mut guard = self.lock();
        guard[offset..offset + 8].copy_from_slice(&val.to_le_bytes());
    }
}

/// Locked SHM region access guard.
pub struct ShmRegionGuard<'a> {
    guard: MutexGuard<'a, Vec<u8>>,
}

impl Deref for ShmRegionGuard<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.guard.as_slice()
    }
}

impl DerefMut for ShmRegionGuard<'_> {
    fn deref_mut(&mut self) -> &mut [u8] {
        self.guard.as_mut_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shm_region_new_zeroed() {
        let region = ShmRegion::new(4096);
        assert_eq!(region.len(), 4096);
        assert!(region.lock().iter().all(|&b| b == 0));
    }

    #[test]
    fn test_shm_region_read_write_u32() {
        let region = ShmRegion::new(64);
        region.write_u32_le(0, 0xDEAD_BEEF);
        region.write_u32_le(4, 42);
        assert_eq!(region.read_u32_le(0), 0xDEAD_BEEF);
        assert_eq!(region.read_u32_le(4), 42);
    }

    #[test]
    fn test_shm_region_read_write_u64() {
        let region = ShmRegion::new(64);
        region.write_u64_le(0, 0x0102_0304_0506_0708);
        assert_eq!(region.read_u64_le(0), 0x0102_0304_0506_0708);
    }

    #[test]
    fn test_shm_region_deref() {
        let region = ShmRegion::new(8);
        {
            let mut g = region.lock();
            g[0] = 0xFF;
        }
        assert_eq!(region.lock()[0], 0xFF);
    }

    #[test]
    fn test_shm_region_from_vec() {
        let data = vec![1, 2, 3, 4];
        let region = ShmRegion::from_vec(data);
        assert_eq!(region.len(), 4);
        assert_eq!(&*region.lock(), &[1, 2, 3, 4]);
    }

    #[test]
    fn test_wal_lock_slots_and_bytes() {
        assert_eq!(WAL_WRITE_LOCK, 0);
        assert_eq!(WAL_CKPT_LOCK, 1);
        assert_eq!(WAL_RECOVER_LOCK, 2);
        assert_eq!(wal_read_lock_slot(0), Some(3));
        assert_eq!(wal_read_lock_slot(4), Some(7));
        assert_eq!(wal_read_lock_slot(5), None);

        assert_eq!(wal_lock_byte(WAL_WRITE_LOCK), Some(120));
        assert_eq!(wal_lock_byte(7), Some(127));
        assert_eq!(wal_lock_byte(8), None);
    }

    #[test]
    fn test_shm_region_is_empty() {
        let empty = ShmRegion::new(0);
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);

        let non_empty = ShmRegion::new(1);
        assert!(!non_empty.is_empty());
    }

    #[test]
    fn test_shm_region_from_vec_empty() {
        let region = ShmRegion::from_vec(vec![]);
        assert!(region.is_empty());
        assert_eq!(region.len(), 0);
        assert!(region.lock().is_empty());
    }

    #[test]
    fn test_shm_region_clone_shares_data() {
        let r1 = ShmRegion::new(16);
        let r2 = r1.clone();

        r1.write_u32_le(0, 0x1234_5678);
        assert_eq!(r2.read_u32_le(0), 0x1234_5678);
    }

    #[test]
    fn test_shm_region_guard_deref_mut() {
        let region = ShmRegion::new(8);
        {
            let mut guard = region.lock();
            guard[0] = 0xAA;
            guard[7] = 0xBB;
        }
        let guard = region.lock();
        assert_eq!(guard[0], 0xAA);
        assert_eq!(guard[7], 0xBB);
        drop(guard);
    }

    #[test]
    fn test_shm_region_u32_at_nonzero_offset() {
        let region = ShmRegion::new(32);
        region.write_u32_le(12, 999);
        region.write_u32_le(28, u32::MAX);
        assert_eq!(region.read_u32_le(12), 999);
        assert_eq!(region.read_u32_le(28), u32::MAX);
        // Bytes in between should still be zero.
        assert_eq!(region.read_u32_le(16), 0);
    }

    #[test]
    fn test_shm_region_u64_at_nonzero_offset() {
        let region = ShmRegion::new(32);
        region.write_u64_le(8, u64::MAX);
        assert_eq!(region.read_u64_le(8), u64::MAX);
        assert_eq!(region.read_u64_le(0), 0);
    }

    #[test]
    fn test_shm_region_u32_min_max() {
        let region = ShmRegion::new(8);
        region.write_u32_le(0, 0);
        assert_eq!(region.read_u32_le(0), 0);
        region.write_u32_le(0, u32::MAX);
        assert_eq!(region.read_u32_le(0), u32::MAX);
    }

    #[test]
    fn test_shm_region_u64_min_max() {
        let region = ShmRegion::new(16);
        region.write_u64_le(0, 0);
        assert_eq!(region.read_u64_le(0), 0);
        region.write_u64_le(0, u64::MAX);
        assert_eq!(region.read_u64_le(0), u64::MAX);
    }

    #[test]
    fn test_shm_flag_constants() {
        assert_eq!(SQLITE_SHM_UNLOCK, 0x01);
        assert_eq!(SQLITE_SHM_LOCK, 0x02);
        assert_eq!(SQLITE_SHM_SHARED, 0x04);
        assert_eq!(SQLITE_SHM_EXCLUSIVE, 0x08);

        // Lock + shared and unlock + exclusive are distinct flag combos.
        assert_ne!(
            SQLITE_SHM_LOCK | SQLITE_SHM_SHARED,
            SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE
        );
    }

    #[test]
    fn test_wal_read_lock_slot_all_valid() {
        for i in 0..WAL_NREADER {
            assert_eq!(wal_read_lock_slot(i), Some(WAL_READ_LOCK_BASE + i));
        }
    }

    #[test]
    fn test_wal_lock_byte_all_valid() {
        for slot in 0..WAL_TOTAL_LOCKS {
            let byte = wal_lock_byte(slot);
            assert!(byte.is_some());
            assert_eq!(byte.unwrap(), 120 + u64::from(slot));
        }
    }

    #[test]
    fn test_wal_total_locks_consistent() {
        assert_eq!(WAL_TOTAL_LOCKS, WAL_READ_LOCK_BASE + WAL_NREADER);
        assert_eq!(WAL_NREADER_USIZE, WAL_NREADER as usize);
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn test_shm_region_read_u32_out_of_bounds() {
        let region = ShmRegion::new(4);
        let _ = region.read_u32_le(2); // offset 2 + 4 = 6 > 4
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn test_shm_region_read_u64_out_of_bounds() {
        let region = ShmRegion::new(8);
        let _ = region.read_u64_le(4); // offset 4 + 8 = 12 > 8
    }

    #[test]
    fn test_shm_region_debug() {
        let region = ShmRegion::new(4);
        let debug_str = format!("{region:?}");
        assert!(debug_str.contains("ShmRegion"));
    }

    #[test]
    fn test_shm_region_interleaved_u32_u64() {
        let region = ShmRegion::new(16);
        region.write_u32_le(0, 42);
        region.write_u64_le(8, 0xCAFE_BABE_DEAD_BEEF);
        assert_eq!(region.read_u32_le(0), 42);
        assert_eq!(region.read_u64_le(8), 0xCAFE_BABE_DEAD_BEEF);
    }
}
