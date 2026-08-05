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

use std::ops::{Deref, DerefMut, Range};
#[cfg(unix)]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard};

use fsqlite_error::{FrankenError, Result};

// ---------------------------------------------------------------------------
// Mmap-backed SHM region support (Unix only)
// ---------------------------------------------------------------------------

/// Raw mmap-backed shared memory region.
///
/// This is the actual backing storage for `ShmRegion` when using the Unix VFS.
/// It maps a region of the `*-shm` file via `mmap(MAP_SHARED)`, so writes are
/// visible to all processes that map the same file region.
///
/// # Safety
///
/// The `ptr` must point to a valid `mmap`-allocated region of `len` bytes.
/// The region must not be unmapped while any `MmapBacking` referring to it
/// is alive.
#[cfg(unix)]
struct MmapBacking {
    ptr: *mut u8,
    len: usize,
    mutex: Mutex<()>,
}

#[cfg(unix)]
impl Drop for MmapBacking {
    fn drop(&mut self) {
        if !self.ptr.is_null() && self.len > 0 {
            // SAFETY: `ptr` and `len` were returned by a successful `mmap` call.
            unsafe {
                libc::munmap(self.ptr.cast::<libc::c_void>(), self.len);
            }
        }
    }
}

// SAFETY: The mmap region is backed by a `MAP_SHARED` file mapping.
// Multiple processes/threads can safely access it via the POSIX shared memory
// contract (coordinated by fcntl locks and memory barriers). The raw pointer
// is only dereferenced through the `ShmRegionGuard` which holds a mutex lock.
#[cfg(unix)]
unsafe impl Send for MmapBacking {}
#[cfg(unix)]
unsafe impl Sync for MmapBacking {}

/// The backing storage for a `ShmRegion`.
enum ShmRegionBacking {
    /// Heap-allocated storage (used by `MemoryVfs` and tests).
    Heap(Arc<Mutex<Vec<u8>>>),
    /// Mmap-backed storage (used by `UnixVfs` for real multi-process SHM).
    #[cfg(unix)]
    Mmap(Arc<MmapBacking>),
}

impl std::fmt::Debug for ShmRegionBacking {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Heap(v) => f
                .debug_tuple("Heap")
                .field(&format_args!(
                    "Vec<u8>[{}]",
                    v.lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .len()
                ))
                .finish(),
            #[cfg(unix)]
            Self::Mmap(m) => f
                .debug_tuple("Mmap")
                .field(&format_args!("ptr={:?}, len={}", m.ptr, m.len))
                .finish(),
        }
    }
}

impl Clone for ShmRegionBacking {
    fn clone(&self) -> Self {
        match self {
            // Deep-copy heap-backed regions so `Clone` produces an independent
            // buffer (matching Rust convention that `Clone` is a value copy,
            // not an `Arc`-style alias). Multi-process sharing of mmap-backed
            // regions still aliases by design — that's the whole point of
            // `MAP_SHARED`.
            Self::Heap(v) => {
                let guard = v.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                Self::Heap(Arc::new(Mutex::new(guard.clone())))
            }
            #[cfg(unix)]
            Self::Mmap(m) => Self::Mmap(Arc::clone(m)),
        }
    }
}

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

/// Standard SHM segment size in bytes (32 KiB, matching C SQLite).
pub const SHM_SEGMENT_SIZE: u32 = 32 * 1024;

/// Absolute byte offset of `aReadMark[0]` in SHM segment 0.
///
/// The SHM header layout is:
/// - `[0..48)`:   `WalIndexHdr` copy 1
/// - `[48..96)`:  `WalIndexHdr` copy 2
/// - `[96..100)`: `nBackfill` (u32)
/// - `[100..120)`: `aReadMark[0..5]` (5 × u32, native byte order)
/// - `[120..128)`: `aLock[0..8]` (lock slot bytes)
/// - `[128..132)`: `nBackfillAttempted` (u32)
/// - `[132..136)`: reserved
pub const SHM_READ_MARK_OFFSET: usize = 100;

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
///
/// # Backing types
///
/// - **Heap**: In-process `Vec<u8>` behind a `Mutex`. Used by `MemoryVfs` and
///   tests. Changes are only visible within the same process.
/// - **Mmap** (Unix only): `MAP_SHARED` mapping of the `*-shm` file. Changes
///   are visible across processes. Coordinated by `fcntl` locks and memory
///   barriers (`shm_barrier`).
#[derive(Debug, Clone)]
pub struct ShmRegion {
    len: usize,
    backing: ShmRegionBacking,
}

impl ShmRegion {
    /// Create a new zeroed SHM region of the given size (heap-backed).
    #[must_use]
    pub fn new(size: usize) -> Self {
        Self {
            len: size,
            backing: ShmRegionBacking::Heap(Arc::new(Mutex::new(vec![0; size]))),
        }
    }

    /// Create a region from existing data (heap-backed).
    #[must_use]
    pub fn from_vec(data: Vec<u8>) -> Self {
        let len = data.len();
        Self {
            len,
            backing: ShmRegionBacking::Heap(Arc::new(Mutex::new(data))),
        }
    }

    /// Create a region backed by an existing `mmap(MAP_SHARED)` mapping.
    ///
    /// # Safety
    ///
    /// - `ptr` must have been returned by a successful `mmap` call with
    ///   `MAP_SHARED` and `PROT_READ | PROT_WRITE`.
    /// - The mapped region must be exactly `len` bytes.
    /// - The caller must not `munmap` the region; `ShmRegion` will do it on
    ///   drop (when all clones are dropped).
    #[cfg(unix)]
    pub unsafe fn from_mmap(ptr: *mut u8, len: usize) -> Self {
        Self {
            len,
            backing: ShmRegionBacking::Mmap(Arc::new(MmapBacking {
                ptr,
                len,
                mutex: Mutex::new(()),
            })),
        }
    }

    /// Return an aliased view of this region — a handle that shares the
    /// underlying buffer with `self`.
    ///
    /// Use this for SHM coordination across handles to the same physical
    /// region (multi-connection WAL-index, mvcc txn counters, etc.). Writes
    /// through one handle are visible to all others via the shared `Arc`.
    ///
    /// Use [`Clone`] when you want a deep, independent copy of the buffer.
    #[must_use]
    pub fn share(&self) -> Self {
        Self {
            len: self.len,
            backing: match &self.backing {
                ShmRegionBacking::Heap(v) => ShmRegionBacking::Heap(Arc::clone(v)),
                #[cfg(unix)]
                ShmRegionBacking::Mmap(m) => ShmRegionBacking::Mmap(Arc::clone(m)),
            },
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
    /// For heap-backed regions, this acquires the inner mutex.
    /// For mmap-backed regions, this acquires an in-process mutex to coordinate
    /// concurrent access from multiple threads in the same process.
    ///
    /// The returned guard derefs to `&[u8]` / `&mut [u8]` and releases the lock
    /// on drop.
    #[must_use]
    pub fn lock(&self) -> ShmRegionGuard<'_> {
        match &self.backing {
            ShmRegionBacking::Heap(data) => ShmRegionGuard {
                inner: ShmRegionGuardInner::Heap {
                    guard: data
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner),
                    visible_len: self.len,
                },
            },
            #[cfg(unix)]
            ShmRegionBacking::Mmap(m) => ShmRegionGuard {
                inner: ShmRegionGuardInner::Mmap {
                    ptr: m.ptr,
                    visible_len: self.len.min(m.len),
                    _guard: m
                        .mutex
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner),
                    _backing: m,
                },
            },
        }
    }

    /// Read a little-endian `u32` at the given byte offset.
    ///
    /// # Errors
    ///
    /// Returns [`FrankenError::OutOfRange`] if the offset would overflow or
    /// `offset + 4 > self.len()`.
    pub fn read_u32_le(&self, offset: usize) -> Result<u32> {
        let bytes: [u8; 4] = {
            let guard = self.lock();
            let range = self.checked_range(offset, 4, guard.len(), "SHM u32 LE read")?;
            guard[range].try_into().expect("slice is exactly 4 bytes")
        };
        Ok(u32::from_le_bytes(bytes))
    }

    /// Write a little-endian `u32` at the given byte offset.
    ///
    /// # Errors
    ///
    /// Returns [`FrankenError::OutOfRange`] if the offset would overflow or
    /// `offset + 4 > self.len()`.
    pub fn write_u32_le(&self, offset: usize, val: u32) -> Result<()> {
        let mut guard = self.lock();
        let range = self.checked_range(offset, 4, guard.len(), "SHM u32 LE write")?;
        guard[range].copy_from_slice(&val.to_le_bytes());
        Ok(())
    }

    /// Read a native-endian `u32` at the given byte offset.
    ///
    /// SHM WAL-index fields use native byte order (the SHM file is not
    /// portable across architectures — it is reconstructed from the WAL
    /// on startup).
    ///
    /// # Errors
    ///
    /// Returns [`FrankenError::OutOfRange`] if the offset would overflow or
    /// `offset + 4 > self.len()`.
    pub fn read_u32_ne(&self, offset: usize) -> Result<u32> {
        let bytes: [u8; 4] = {
            let guard = self.lock();
            let range = self.checked_range(offset, 4, guard.len(), "SHM u32 native-endian read")?;
            guard[range].try_into().expect("slice is exactly 4 bytes")
        };
        Ok(u32::from_ne_bytes(bytes))
    }

    /// Write a native-endian `u32` at the given byte offset.
    ///
    /// SHM WAL-index fields use native byte order.
    ///
    /// # Errors
    ///
    /// Returns [`FrankenError::OutOfRange`] if the offset would overflow or
    /// `offset + 4 > self.len()`.
    pub fn write_u32_ne(&self, offset: usize, val: u32) -> Result<()> {
        let mut guard = self.lock();
        let range = self.checked_range(offset, 4, guard.len(), "SHM u32 native-endian write")?;
        guard[range].copy_from_slice(&val.to_ne_bytes());
        Ok(())
    }

    /// Read a little-endian `u64` at the given byte offset.
    ///
    /// # Errors
    ///
    /// Returns [`FrankenError::OutOfRange`] if the offset would overflow or
    /// `offset + 8 > self.len()`.
    pub fn read_u64_le(&self, offset: usize) -> Result<u64> {
        let bytes: [u8; 8] = {
            let guard = self.lock();
            let range = self.checked_range(offset, 8, guard.len(), "SHM u64 LE read")?;
            guard[range].try_into().expect("slice is exactly 8 bytes")
        };
        Ok(u64::from_le_bytes(bytes))
    }

    /// Write a little-endian `u64` at the given byte offset.
    ///
    /// # Errors
    ///
    /// Returns [`FrankenError::OutOfRange`] if the offset would overflow or
    /// `offset + 8 > self.len()`.
    pub fn write_u64_le(&self, offset: usize, val: u64) -> Result<()> {
        let mut guard = self.lock();
        let range = self.checked_range(offset, 8, guard.len(), "SHM u64 LE write")?;
        guard[range].copy_from_slice(&val.to_le_bytes());
        Ok(())
    }

    /// Atomically load a little-endian `u64` at the given byte offset.
    ///
    /// Heap-backed regions emulate the atomic through the region mutex.
    /// Mmap-backed regions use a native `AtomicU64` over the shared mapping so
    /// updates are visible across processes that map the same file.
    ///
    /// # Errors
    ///
    /// Returns [`FrankenError::OutOfRange`] if the offset is unaligned, would
    /// overflow, or `offset + 8 > self.len()`.
    pub fn atomic_load_u64_le(&self, offset: usize, ordering: Ordering) -> Result<u64> {
        #[cfg(not(unix))]
        let _ = ordering;
        match &self.backing {
            ShmRegionBacking::Heap(data) => {
                let guard = data
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let range = self.checked_aligned_u64_offset(offset, guard.len())?;
                let bytes: [u8; 8] = guard[range].try_into().expect("slice is exactly 8 bytes");
                Ok(u64::from_le_bytes(bytes))
            }
            #[cfg(unix)]
            ShmRegionBacking::Mmap(m) => {
                self.checked_aligned_u64_offset(offset, m.len)?;
                let _guard = m
                    .mutex
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                // SAFETY: `offset` bounds/alignment were validated above and
                // the mapping stays alive for the duration of the guard.
                let raw = unsafe { atomic_u64_at(m.ptr, offset) }.load(ordering);
                Ok(u64::from_le(raw))
            }
        }
    }

    /// Atomically store a little-endian `u64` at the given byte offset.
    ///
    /// # Errors
    ///
    /// Returns [`FrankenError::OutOfRange`] if the offset is unaligned, would
    /// overflow, or `offset + 8 > self.len()`.
    pub fn atomic_store_u64_le(&self, offset: usize, val: u64, ordering: Ordering) -> Result<()> {
        #[cfg(not(unix))]
        let _ = ordering;
        match &self.backing {
            ShmRegionBacking::Heap(data) => {
                let mut guard = data
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let range = self.checked_aligned_u64_offset(offset, guard.len())?;
                guard[range].copy_from_slice(&val.to_le_bytes());
                Ok(())
            }
            #[cfg(unix)]
            ShmRegionBacking::Mmap(m) => {
                self.checked_aligned_u64_offset(offset, m.len)?;
                let _guard = m
                    .mutex
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                // SAFETY: `offset` bounds/alignment were validated above and
                // the mapping stays alive for the duration of the guard.
                unsafe { atomic_u64_at(m.ptr, offset) }.store(val.to_le(), ordering);
                Ok(())
            }
        }
    }

    /// Atomically add `delta` to a little-endian `u64`, returning the previous value.
    ///
    /// # Errors
    ///
    /// Returns [`FrankenError::OutOfRange`] if the offset is unaligned, would
    /// overflow, or `offset + 8 > self.len()`.
    pub fn atomic_fetch_add_u64_le(
        &self,
        offset: usize,
        delta: u64,
        ordering: Ordering,
    ) -> Result<u64> {
        #[cfg(not(unix))]
        let _ = ordering;
        match &self.backing {
            ShmRegionBacking::Heap(data) => {
                let mut guard = data
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let range = self.checked_aligned_u64_offset(offset, guard.len())?;
                let current = u64::from_le_bytes(
                    guard[range.clone()]
                        .try_into()
                        .expect("slice is exactly 8 bytes"),
                );
                let next = current.wrapping_add(delta);
                guard[range].copy_from_slice(&next.to_le_bytes());
                Ok(current)
            }
            #[cfg(unix)]
            ShmRegionBacking::Mmap(m) => {
                self.checked_aligned_u64_offset(offset, m.len)?;
                let _guard = m
                    .mutex
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                loop {
                    // SAFETY: `offset` bounds/alignment were validated above and
                    // the mapping stays alive for the duration of the guard.
                    let atom = unsafe { atomic_u64_at(m.ptr, offset) };
                    let current = u64::from_le(atom.load(Ordering::Acquire));
                    let next = current.wrapping_add(delta);
                    if atom
                        .compare_exchange(
                            current.to_le(),
                            next.to_le(),
                            ordering,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return Ok(current);
                    }
                }
            }
        }
    }

    /// Atomically compare-exchange a little-endian `u64`.
    ///
    /// Returns `Ok(Ok(previous))` on success and `Ok(Err(actual))` on compare
    /// failure, matching `AtomicU64::compare_exchange`.
    ///
    /// # Errors
    ///
    /// Returns [`FrankenError::OutOfRange`] if the offset is unaligned, would
    /// overflow, or `offset + 8 > self.len()`.
    pub fn atomic_compare_exchange_u64_le(
        &self,
        offset: usize,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<std::result::Result<u64, u64>> {
        #[cfg(not(unix))]
        let _ = (success, failure);
        match &self.backing {
            ShmRegionBacking::Heap(data) => {
                let mut guard = data
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let range = self.checked_aligned_u64_offset(offset, guard.len())?;
                let actual = u64::from_le_bytes(
                    guard[range.clone()]
                        .try_into()
                        .expect("slice is exactly 8 bytes"),
                );
                if actual == current {
                    guard[range].copy_from_slice(&new.to_le_bytes());
                    Ok(Ok(actual))
                } else {
                    Ok(Err(actual))
                }
            }
            #[cfg(unix)]
            ShmRegionBacking::Mmap(m) => {
                self.checked_aligned_u64_offset(offset, m.len)?;
                let _guard = m
                    .mutex
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                // SAFETY: `offset` bounds/alignment were validated above and
                // the mapping stays alive for the duration of the guard.
                Ok(unsafe { atomic_u64_at(m.ptr, offset) }
                    .compare_exchange(current.to_le(), new.to_le(), success, failure)
                    .map(u64::from_le)
                    .map_err(u64::from_le))
            }
        }
    }

    /// Resize a heap-backed shared-memory region without panicking on OOM.
    ///
    /// Note that `Clone` on a heap-backed `ShmRegion` deep-copies the buffer,
    /// so existing clones are independent and unaffected by this resize.
    /// Mmap-backed regions (when present) still alias by design and would
    /// share the new size if they could be resized — but mmap-backed resizing
    /// is not supported here and must go through `shm_map` again.
    ///
    /// # Errors
    ///
    /// Returns [`FrankenError::Unsupported`] for mmap-backed regions, which
    /// must be remapped by calling `shm_map` again with the new size.
    pub fn try_resize_heap(&mut self, new_size: usize) -> Result<()> {
        match &self.backing {
            ShmRegionBacking::Heap(data) => {
                let mut guard = data
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if new_size > guard.len() {
                    let current_len = guard.len();
                    guard
                        .try_reserve_exact(new_size.saturating_sub(current_len))
                        .map_err(|_| FrankenError::OutOfMemory)?;
                    guard.resize(new_size, 0);
                } else if new_size < guard.len() {
                    guard.truncate(new_size);
                }
                self.len = new_size;
                Ok(())
            }
            #[cfg(unix)]
            ShmRegionBacking::Mmap(_) => Err(FrankenError::Unsupported),
        }
    }

    /// Heap-backed storage accounting, when available.
    #[must_use]
    pub fn heap_len_and_capacity(&self) -> Option<(usize, usize)> {
        match &self.backing {
            ShmRegionBacking::Heap(data) => {
                let guard = data
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                Some((guard.len(), guard.capacity()))
            }
            #[cfg(unix)]
            ShmRegionBacking::Mmap(_) => None,
        }
    }

    /// Returns `true` if this region is backed by mmap (multi-process visible).
    #[must_use]
    pub fn is_mmap_backed(&self) -> bool {
        match &self.backing {
            ShmRegionBacking::Heap(_) => false,
            #[cfg(unix)]
            ShmRegionBacking::Mmap(_) => true,
        }
    }

    fn checked_range(
        &self,
        offset: usize,
        width: usize,
        actual_len: usize,
        what: &'static str,
    ) -> Result<Range<usize>> {
        Self::checked_range_for_len(offset, width, self.len.min(actual_len), what)
    }

    fn checked_aligned_u64_offset(&self, offset: usize, actual_len: usize) -> Result<Range<usize>> {
        if !offset.is_multiple_of(std::mem::align_of::<u64>()) {
            return Err(FrankenError::OutOfRange {
                what: "SHM atomic u64 access".to_owned(),
                value: format!("unaligned offset={offset}"),
            });
        }
        self.checked_range(offset, 8, actual_len, "SHM atomic u64 access")
    }

    fn checked_range_for_len(
        offset: usize,
        width: usize,
        len: usize,
        what: &'static str,
    ) -> Result<Range<usize>> {
        let end = offset
            .checked_add(width)
            .ok_or_else(|| FrankenError::OutOfRange {
                what: what.to_owned(),
                value: format!("offset={offset} width={width} len={len}"),
            })?;
        if end > len {
            return Err(FrankenError::OutOfRange {
                what: what.to_owned(),
                value: format!("offset={offset} width={width} len={len}"),
            });
        }
        Ok(offset..end)
    }
}

#[cfg(unix)]
#[allow(clippy::cast_ptr_alignment)]
unsafe fn atomic_u64_at<'a>(ptr: *mut u8, offset: usize) -> &'a AtomicU64 {
    // SAFETY: callers only use this on MAP_SHARED mmap regions. mmap returns
    // page-aligned base addresses, and `checked_aligned_u64_offset()` ensures
    // the final byte offset stays 8-byte aligned and in-bounds.
    unsafe { &*ptr.add(offset).cast::<AtomicU64>() }
}

/// Locked SHM region access guard.
///
/// For heap-backed regions, holds the inner `MutexGuard`.
/// For mmap-backed regions, provides direct access to the mapped memory
/// while keeping a reference to the `MmapBacking` to prevent unmapping.
pub struct ShmRegionGuard<'a> {
    inner: ShmRegionGuardInner<'a>,
}

enum ShmRegionGuardInner<'a> {
    Heap {
        guard: MutexGuard<'a, Vec<u8>>,
        visible_len: usize,
    },
    #[cfg(unix)]
    Mmap {
        ptr: *mut u8,
        visible_len: usize,
        /// The in-process lock protecting this region.
        _guard: MutexGuard<'a, ()>,
        /// Prevent the `MmapBacking` from being dropped while we hold a
        /// reference to the mapped memory.
        _backing: &'a Arc<MmapBacking>,
    },
}

impl Deref for ShmRegionGuard<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match &self.inner {
            ShmRegionGuardInner::Heap { guard, visible_len } => {
                &guard[..(*visible_len).min(guard.len())]
            }
            #[cfg(unix)]
            ShmRegionGuardInner::Mmap {
                ptr, visible_len, ..
            } => {
                // SAFETY: The MmapBacking reference guarantees the region is
                // still mapped. `visible_len` is capped to the successful
                // `mmap` length when the guard is created.
                unsafe { std::slice::from_raw_parts(*ptr, *visible_len) }
            }
        }
    }
}

impl DerefMut for ShmRegionGuard<'_> {
    fn deref_mut(&mut self) -> &mut [u8] {
        match &mut self.inner {
            ShmRegionGuardInner::Heap { guard, visible_len } => {
                let end = (*visible_len).min(guard.len());
                &mut guard[..end]
            }
            #[cfg(unix)]
            ShmRegionGuardInner::Mmap {
                ptr, visible_len, ..
            } => {
                // SAFETY: Same invariants as Deref. The mmap was created with
                // PROT_READ | PROT_WRITE.
                unsafe { std::slice::from_raw_parts_mut(*ptr, *visible_len) }
            }
        }
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
        region.write_u32_le(0, 0xDEAD_BEEF).unwrap();
        region.write_u32_le(4, 42).unwrap();
        assert_eq!(region.read_u32_le(0).unwrap(), 0xDEAD_BEEF);
        assert_eq!(region.read_u32_le(4).unwrap(), 42);
    }

    #[test]
    fn test_shm_region_read_write_u64() {
        let region = ShmRegion::new(64);
        region.write_u64_le(0, 0x0102_0304_0506_0708).unwrap();
        assert_eq!(region.read_u64_le(0).unwrap(), 0x0102_0304_0506_0708);
    }

    #[test]
    fn test_shm_region_atomic_u64_round_trip() {
        let region = ShmRegion::new(64);

        region.atomic_store_u64_le(8, 41, Ordering::SeqCst).unwrap();
        assert_eq!(region.atomic_load_u64_le(8, Ordering::SeqCst).unwrap(), 41);

        let before = region
            .atomic_fetch_add_u64_le(8, 1, Ordering::SeqCst)
            .unwrap();
        assert_eq!(before, 41);
        assert_eq!(region.atomic_load_u64_le(8, Ordering::SeqCst).unwrap(), 42);

        assert_eq!(
            region
                .atomic_compare_exchange_u64_le(8, 42, 99, Ordering::SeqCst, Ordering::SeqCst)
                .unwrap(),
            Ok(42)
        );
        assert_eq!(region.atomic_load_u64_le(8, Ordering::SeqCst).unwrap(), 99);
    }

    #[test]
    fn test_shm_region_atomic_compare_exchange_reports_actual_value() {
        let region = ShmRegion::new(64);
        region.atomic_store_u64_le(16, 7, Ordering::SeqCst).unwrap();

        assert_eq!(
            region
                .atomic_compare_exchange_u64_le(16, 8, 9, Ordering::SeqCst, Ordering::SeqCst)
                .unwrap(),
            Err(7)
        );
        assert_eq!(region.atomic_load_u64_le(16, Ordering::SeqCst).unwrap(), 7);
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
    fn test_shm_region_clone_copies_existing_data() {
        // `Clone` produces an independent buffer that initially mirrors the
        // source's contents (see also `test_shm_region_clone_is_independent`
        // for the post-clone divergence invariant).
        let r1 = ShmRegion::new(16);
        r1.write_u32_le(0, 0x1234_5678).unwrap();
        let r2 = r1.clone();
        assert_eq!(r2.read_u32_le(0).unwrap(), 0x1234_5678);
        // r1 must remain readable (clone copies, doesn't move) — using the
        // original here also keeps clippy from flagging the clone as
        // redundant under `-D clippy::nursery`.
        assert_eq!(r1.read_u32_le(0).unwrap(), 0x1234_5678);
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
        region.write_u32_le(12, 999).unwrap();
        region.write_u32_le(28, u32::MAX).unwrap();
        assert_eq!(region.read_u32_le(12).unwrap(), 999);
        assert_eq!(region.read_u32_le(28).unwrap(), u32::MAX);
        // Bytes in between should still be zero.
        assert_eq!(region.read_u32_le(16).unwrap(), 0);
    }

    #[test]
    fn test_shm_region_u64_at_nonzero_offset() {
        let region = ShmRegion::new(32);
        region.write_u64_le(8, u64::MAX).unwrap();
        assert_eq!(region.read_u64_le(8).unwrap(), u64::MAX);
        assert_eq!(region.read_u64_le(0).unwrap(), 0);
    }

    #[test]
    fn test_shm_region_u32_min_max() {
        let region = ShmRegion::new(8);
        region.write_u32_le(0, 0).unwrap();
        assert_eq!(region.read_u32_le(0).unwrap(), 0);
        region.write_u32_le(0, u32::MAX).unwrap();
        assert_eq!(region.read_u32_le(0).unwrap(), u32::MAX);
    }

    #[test]
    fn test_shm_region_u64_min_max() {
        let region = ShmRegion::new(16);
        region.write_u64_le(0, 0).unwrap();
        assert_eq!(region.read_u64_le(0).unwrap(), 0);
        region.write_u64_le(0, u64::MAX).unwrap();
        assert_eq!(region.read_u64_le(0).unwrap(), u64::MAX);
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
    fn test_shm_region_read_u32_out_of_bounds() {
        let region = ShmRegion::new(4);
        let err = region.read_u32_le(2).unwrap_err(); // offset 2 + 4 = 6 > 4
        assert!(matches!(err, FrankenError::OutOfRange { .. }));
    }

    #[test]
    fn test_shm_region_read_u64_out_of_bounds() {
        let region = ShmRegion::new(8);
        let err = region.read_u64_le(4).unwrap_err(); // offset 4 + 8 = 12 > 8
        assert!(matches!(err, FrankenError::OutOfRange { .. }));
    }

    #[test]
    fn test_shm_region_write_out_of_bounds_returns_error() {
        let region = ShmRegion::new(4);
        let err = region.write_u32_le(2, 99).unwrap_err();
        assert!(matches!(err, FrankenError::OutOfRange { .. }));

        let region = ShmRegion::new(8);
        let err = region.write_u64_le(4, 99).unwrap_err();
        assert!(matches!(err, FrankenError::OutOfRange { .. }));
    }

    #[test]
    fn test_shm_region_offset_overflow_returns_error() {
        let region = ShmRegion::new(64);

        let err = region.read_u32_le(usize::MAX - 1).unwrap_err();
        assert!(matches!(err, FrankenError::OutOfRange { .. }));

        let err = region.write_u64_le(usize::MAX - 7, 99).unwrap_err();
        assert!(matches!(err, FrankenError::OutOfRange { .. }));
    }

    #[test]
    fn test_shm_region_atomic_bad_offsets_return_error() {
        let region = ShmRegion::new(16);

        let err = region.atomic_load_u64_le(4, Ordering::SeqCst).unwrap_err();
        assert!(matches!(err, FrankenError::OutOfRange { .. }));

        let err = region
            .atomic_store_u64_le(12, 99, Ordering::SeqCst)
            .unwrap_err();
        assert!(matches!(err, FrankenError::OutOfRange { .. }));

        let err = region
            .atomic_compare_exchange_u64_le(usize::MAX, 0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .unwrap_err();
        assert!(matches!(err, FrankenError::OutOfRange { .. }));
    }

    #[test]
    fn test_shm_region_clone_unaffected_by_original_shrink() {
        // Since `Clone` deep-copies heap-backed regions, shrinking the
        // original must not invalidate reads on the clone — the clone owns
        // an independent 16-byte buffer.
        let mut region = ShmRegion::new(16);
        let clone = region.clone();
        region.try_resize_heap(4).unwrap();

        assert_eq!(clone.len(), 16);
        assert_eq!(clone.lock().len(), 16);
        // Reading the full 16 bytes of the clone must succeed.
        clone.read_u64_le(8).unwrap();
        // The shrunk original, however, must reject the out-of-range read.
        let err = region.read_u64_le(8).unwrap_err();
        assert!(matches!(err, FrankenError::OutOfRange { .. }));
    }

    #[test]
    fn test_shm_region_stale_clone_after_grow_lock_respects_cached_len() {
        let mut region = ShmRegion::new(4);
        let clone = region.clone();
        region.try_resize_heap(8).unwrap();

        assert_eq!(region.len(), 8);
        assert_eq!(region.lock().len(), 8);
        assert_eq!(clone.len(), 4);
        assert_eq!(
            clone.lock().len(),
            4,
            "stale clones must not see newly mapped bytes until remapped"
        );
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
        region.write_u32_le(0, 42).unwrap();
        region.write_u64_le(8, 0xCAFE_BABE_DEAD_BEEF).unwrap();
        assert_eq!(region.read_u32_le(0).unwrap(), 42);
        assert_eq!(region.read_u64_le(8).unwrap(), 0xCAFE_BABE_DEAD_BEEF);
    }

    #[test]
    fn test_shm_region_read_write_u32_ne() {
        let region = ShmRegion::new(64);
        region.write_u32_ne(0, 0xDEAD_BEEF).unwrap();
        region.write_u32_ne(4, 42).unwrap();
        assert_eq!(region.read_u32_ne(0).unwrap(), 0xDEAD_BEEF);
        assert_eq!(region.read_u32_ne(4).unwrap(), 42);
    }

    #[test]
    fn test_shm_region_native_endian_consistency() {
        let region = ShmRegion::new(16);
        let value = 0x1234_5678_u32;
        region.write_u32_ne(0, value).unwrap();
        // Native endian read should round-trip.
        assert_eq!(region.read_u32_ne(0).unwrap(), value);
        // On little-endian platforms, ne == le.
        if cfg!(target_endian = "little") {
            assert_eq!(region.read_u32_le(0).unwrap(), value);
        }
    }

    #[test]
    fn test_shm_read_mark_offset_constant() {
        // aReadMark starts at absolute SHM offset 100 (after 2×48B headers + 4B nBackfill).
        assert_eq!(SHM_READ_MARK_OFFSET, 100);
        assert_eq!(SHM_SEGMENT_SIZE, 32 * 1024);
    }

    #[test]
    fn test_shm_region_try_resize_heap_preserves_data() {
        let mut region = ShmRegion::new(8);
        region.write_u32_le(0, 0xDEAD).unwrap();
        region.write_u32_le(4, 0xBEEF).unwrap();

        region.try_resize_heap(16).unwrap();
        assert_eq!(region.len(), 16);
        assert_eq!(region.read_u32_le(0).unwrap(), 0xDEAD);
        assert_eq!(region.read_u32_le(4).unwrap(), 0xBEEF);
        assert_eq!(
            region.read_u32_le(8).unwrap(),
            0,
            "grown region zero-filled"
        );

        region.try_resize_heap(4).unwrap();
        assert_eq!(region.len(), 4);
        assert_eq!(region.read_u32_le(0).unwrap(), 0xDEAD);
    }

    #[test]
    fn test_shm_region_heap_len_and_capacity() {
        let region = ShmRegion::new(32);
        let (len, cap) = region.heap_len_and_capacity().expect("heap-backed");
        assert_eq!(len, 32);
        assert!(cap >= 32);
    }

    #[test]
    fn test_shm_region_is_mmap_backed_false_for_heap() {
        let region = ShmRegion::new(16);
        assert!(!region.is_mmap_backed());
    }

    #[test]
    fn test_shm_region_atomic_fetch_add_u64_le() {
        let region = ShmRegion::new(16);
        region.write_u64_le(0, 100).unwrap();

        let prev = region
            .atomic_fetch_add_u64_le(0, 42, Ordering::SeqCst)
            .unwrap();
        assert_eq!(prev, 100);
        assert_eq!(region.read_u64_le(0).unwrap(), 142);

        let prev2 = region
            .atomic_fetch_add_u64_le(0, u64::MAX, Ordering::SeqCst)
            .unwrap();
        assert_eq!(prev2, 142);
        assert_eq!(region.read_u64_le(0).unwrap(), 141, "wrapping add");
    }

    #[test]
    fn test_shm_region_clone_is_independent() {
        let region = ShmRegion::new(16);
        region.write_u32_le(0, 0xDEAD).unwrap();
        let cloned = region.clone();
        assert_eq!(cloned.read_u32_le(0).unwrap(), 0xDEAD);
        assert_eq!(cloned.len(), 16);
        region.write_u32_le(0, 0xBEEF).unwrap();
        assert_eq!(
            cloned.read_u32_le(0).unwrap(),
            0xDEAD,
            "clone must be independent"
        );
    }

    #[test]
    fn test_shm_region_share_aliases_buffer() {
        let region = ShmRegion::new(16);
        region.write_u32_le(0, 0xDEAD).unwrap();
        let shared = region.share();
        assert_eq!(
            shared.read_u32_le(0).unwrap(),
            0xDEAD,
            "share() handle sees post-write state",
        );
        region.write_u32_le(0, 0xBEEF).unwrap();
        assert_eq!(
            shared.read_u32_le(0).unwrap(),
            0xBEEF,
            "share() handle aliases the original buffer — subsequent writes through one are visible to the other",
        );
        shared.write_u32_le(4, 0xCAFE).unwrap();
        assert_eq!(
            region.read_u32_le(4).unwrap(),
            0xCAFE,
            "writes through the shared handle are also visible to the original",
        );
    }

    #[test]
    fn test_wal_read_lock_slot_out_of_range() {
        assert!(wal_read_lock_slot(WAL_NREADER).is_none());
        assert!(wal_read_lock_slot(WAL_NREADER + 1).is_none());
        assert!(wal_read_lock_slot(u32::MAX).is_none());
    }

    #[test]
    fn test_wal_lock_byte_out_of_range() {
        assert!(wal_lock_byte(WAL_TOTAL_LOCKS).is_none());
        assert!(wal_lock_byte(WAL_TOTAL_LOCKS + 1).is_none());
        assert!(wal_lock_byte(u32::MAX).is_none());
    }

    #[test]
    fn test_shm_segment_size_is_32kib() {
        assert_eq!(SHM_SEGMENT_SIZE, 32 * 1024);
        assert_eq!(WAL_TOTAL_LOCKS, WAL_READ_LOCK_BASE + WAL_NREADER);
    }
}
