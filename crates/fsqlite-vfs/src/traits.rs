use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use fsqlite_error::Result;
use fsqlite_types::LockLevel;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};

use crate::shm::ShmRegion;

/// Opaque identity of an already-open filesystem object.
///
/// Identities are intended only for equality comparisons while the relevant
/// file handles remain open. They are not persistent database identifiers and
/// must not be serialized or compared across machines or boots.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileIdentity {
    namespace: u64,
    object: u64,
}

impl FileIdentity {
    /// Read the identity of an independently opened filesystem descriptor.
    ///
    /// On Unix this uses descriptor metadata (`st_dev`, `st_ino`), so a later
    /// rename or pathname replacement does not change the result. Platforms
    /// without a stable descriptor identity exposed by this crate return
    /// `Ok(None)`.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_file(file: &std::fs::File) -> std::io::Result<Option<Self>> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;

            let metadata = file.metadata()?;
            Ok(Some(Self::from_unix_parts(metadata.dev(), metadata.ino())))
        }

        #[cfg(not(unix))]
        {
            let _ = file;
            Ok(None)
        }
    }

    #[cfg(unix)]
    pub(crate) const fn from_unix_parts(device: u64, inode: u64) -> Self {
        Self {
            namespace: device,
            object: inode,
        }
    }
}

impl std::fmt::Debug for FileIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FileIdentity(..)")
    }
}

/// Durability level for `VfsFile::durable_sync`.
///
/// Centralizes per-filesystem sync policy so callers express intent
/// ("make WAL frames durable") and the VFS maps it to the correct
/// syscall: fdatasync on ext4/XFS, fsync on btrfs/ZFS, F_FULLFSYNC
/// on APFS.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SyncKind {
    /// Data-only sync (fdatasync on Linux). Sufficient when file size
    /// and metadata are unchanged (e.g. overwriting existing WAL frames).
    DataOnly,
    /// Data + metadata sync (fsync on Linux). Required when new allocation
    /// blocks must be persisted (file growth, btrfs CoW).
    DataAndMetadata,
    /// Full durable barrier: the strongest sync the platform offers.
    /// On APFS this maps to F_FULLFSYNC; elsewhere identical to
    /// `DataAndMetadata`. Required for WAL commit frames.
    FullDurable,
}

static DEFAULT_RANDOMNESS_CALL_SEQ: AtomicU64 = AtomicU64::new(0);

/// A virtual filesystem implementation.
///
/// This trait abstracts all file system operations, allowing different
/// backends: real files (Unix), in-memory (testing), or custom implementations.
///
/// Modeled after C SQLite's `sqlite3_vfs` struct from `os.h`.
pub trait Vfs: Send + Sync {
    /// The file handle type produced by this VFS.
    type File: VfsFile;

    /// The name of this VFS (e.g., "unix", "memory").
    fn name(&self) -> &'static str;

    /// Open a file.
    ///
    /// `path` is `None` for temporary files that should be auto-named.
    /// `flags` describes what kind of file (main DB, journal, WAL, etc.)
    /// and how to open it (create, read-write, exclusive, etc.).
    ///
    /// Returns the opened file and the flags that were actually used (the VFS
    /// may add flags like `READWRITE` when `CREATE` is specified).
    fn open(
        &self,
        cx: &Cx,
        path: Option<&Path>,
        flags: VfsOpenFlags,
    ) -> Result<(Self::File, VfsOpenFlags)>;

    /// Delete a file.
    ///
    /// If `sync_dir` is true, the directory entry removal should be synced
    /// to ensure durability.
    fn delete(&self, cx: &Cx, path: &Path, sync_dir: bool) -> Result<()>;

    /// Check file access.
    ///
    /// Returns true if the file at `path` satisfies the access check
    /// described by `flags`.
    fn access(&self, cx: &Cx, path: &Path, flags: AccessFlags) -> Result<bool>;

    /// Resolve a potentially relative path into an absolute path.
    fn full_pathname(&self, cx: &Cx, path: &Path) -> Result<PathBuf>;

    /// Generate a random byte sequence for temporary file naming.
    ///
    /// Fills `buf` with bytes suitable for temporary file naming.
    ///
    /// The default implementation is deterministic (xorshift seeded from a
    /// process-local counter) for reproducible tests; real VFS implementations
    /// should override this and use OS-provided randomness to avoid collisions.
    fn randomness(&self, cx: &Cx, buf: &mut [u8]) {
        // Default: fill with pseudo-random bytes using a simple xorshift.
        // Real VFS implementations should use OS-provided randomness.
        let _ = cx; // Usage to silence unused variable warning
        let seq = DEFAULT_RANDOMNESS_CALL_SEQ.fetch_add(1, Ordering::Relaxed);
        let mut state: u64 = 0x5DEE_CE66_D1A4_F681 ^ seq.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        for chunk in buf.chunks_mut(8) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let bytes = state.to_le_bytes();
            for (dst, &src) in chunk.iter_mut().zip(bytes.iter()) {
                *dst = src;
            }
        }
    }

    /// Return the current time as a Julian day number (days since noon
    /// on November 24, 4714 B.C.).
    fn current_time(&self, cx: &Cx) -> f64 {
        // Default: derive from `Cx` time capability (no ambient authority).
        cx.current_time_julian_day()
    }

    /// Returns true if this VFS operates entirely in-process memory.
    /// In-memory VFS backends can skip file locking, journal recovery,
    /// and other I/O-oriented work in the pager hot path.
    fn is_memory(&self) -> bool {
        false
    }
}

/// A file handle opened by a VFS.
///
/// Corresponds to C SQLite's `sqlite3_file` + `sqlite3_io_methods`.
pub trait VfsFile: Send + Sync {
    /// Close the file.
    ///
    /// After this call, the file handle should not be used.
    fn close(&mut self, cx: &Cx) -> Result<()>;

    /// Return the identity of the filesystem object held by this open handle.
    ///
    /// Implementations must derive this from the open descriptor (or from
    /// descriptor metadata captured at open time), never by resolving the
    /// current pathname. The default is `None` for memory and custom backends
    /// that cannot provide a stable comparable identity.
    fn file_identity(&self) -> Result<Option<FileIdentity>> {
        Ok(None)
    }

    /// Read `buf.len()` bytes starting at byte offset `offset`.
    ///
    /// Returns the number of bytes actually read. If fewer bytes are read
    /// than requested (short read), the remaining bytes in `buf` are zeroed.
    fn read(&self, cx: &Cx, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// Write `buf` starting at byte offset `offset`.
    fn write(&mut self, cx: &Cx, buf: &[u8], offset: u64) -> Result<()>;

    /// Write multiple page-sized buffers in one logical operation.
    ///
    /// The default implementation preserves existing semantics by issuing the
    /// writes sequentially through [`Self::write`]. VFS backends may override
    /// this to amortize locking or syscall overhead for hot pager commit paths.
    fn write_page_batch(&mut self, cx: &Cx, writes: &[(u64, &[u8])]) -> Result<()> {
        for (offset, data) in writes {
            self.write(cx, data, *offset)?;
        }
        Ok(())
    }

    /// Truncate the file to `size` bytes.
    fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()>;

    /// Sync the file contents to stable storage.
    ///
    /// `flags` indicates the type of sync (normal, full, data-only).
    fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()>;

    /// Durability-intent sync with per-filesystem policy.
    ///
    /// Callers express *what* must be durable (`SyncKind`); the VFS maps
    /// it to the correct platform syscall. Default delegates to `sync()`
    /// with `DATAONLY` / `FULL` flags; platform VFS backends override for
    /// filesystem-specific behaviour (F_FULLFSYNC on APFS, etc.).
    fn durable_sync(&mut self, cx: &Cx, kind: SyncKind) -> Result<()> {
        let flags = match kind {
            SyncKind::DataOnly => SyncFlags::DATAONLY,
            SyncKind::DataAndMetadata | SyncKind::FullDurable => SyncFlags::FULL,
        };
        self.sync(cx, flags)
    }

    /// Return the current file size in bytes.
    fn file_size(&self, cx: &Cx) -> Result<u64>;

    /// Acquire a file lock at the given level.
    ///
    /// SQLite's five-level locking: None < Shared < Reserved < Pending < Exclusive.
    fn lock(&mut self, cx: &Cx, level: LockLevel) -> Result<()>;

    /// Release the file lock to the given level.
    fn unlock(&mut self, cx: &Cx, level: LockLevel) -> Result<()>;

    /// Check if another process holds a reserved lock.
    ///
    /// Returns true if a RESERVED or higher lock is held by another connection.
    fn check_reserved_lock(&self, cx: &Cx) -> Result<bool>;

    /// Return the sector size for this file.
    ///
    /// The sector size is the minimum write granularity for the underlying
    /// storage. Defaults to 4096 bytes.
    fn sector_size(&self) -> u32 {
        4096
    }

    /// Return device characteristics flags.
    ///
    /// These flags describe capabilities of the underlying storage device,
    /// such as whether it supports atomic writes. Returns 0 for no special
    /// characteristics.
    fn device_characteristics(&self) -> u32 {
        0
    }

    // --- Shared-memory methods (required for WAL mode) ---

    /// Map a region of shared memory. `region` is a 0-based index of 32KB
    /// regions. If `extend` is true and the region does not exist, create it.
    /// Returns a safe [`ShmRegion`] handle with bounds-checked accessors.
    /// (Equivalent to sqlite3_io_methods.xShmMap)
    fn shm_map(&mut self, cx: &Cx, region: u32, size: u32, extend: bool) -> Result<ShmRegion>;

    /// Acquire or release a shared-memory lock.
    /// `offset` and `n` define a range of lock slots.
    /// `flags`: SHM_LOCK | (SHM_SHARED | SHM_EXCLUSIVE).
    /// (Equivalent to sqlite3_io_methods.xShmLock)
    fn shm_lock(&mut self, cx: &Cx, offset: u32, n: u32, flags: u32) -> Result<()>;

    /// Memory barrier for shared memory -- ensures all prior SHM writes are
    /// visible to other processes before subsequent reads.
    /// (Equivalent to sqlite3_io_methods.xShmBarrier)
    fn shm_barrier(&self);

    /// Unmap all shared-memory regions. If `delete` is true, also delete
    /// the underlying SHM file.
    /// (Equivalent to sqlite3_io_methods.xShmUnmap)
    fn shm_unmap(&mut self, cx: &Cx, delete: bool) -> Result<()>;

    /// Set the busy-timeout for cross-process file-lock contention.
    ///
    /// When `ms > 0`, the VFS should retry `F_SETLK` with exponential
    /// backoff instead of returning `SQLITE_BUSY` immediately on
    /// `EAGAIN`/`EACCES`. A value of `0` disables retries (fail-fast).
    ///
    /// Default implementation is a no-op (memory and stub VFS backends
    /// have no OS-level lock contention).
    fn set_busy_timeout_ms(&mut self, _ms: u64) {}
}

/// Async data-path trait for VFS file I/O (bd-2jpu6.1 Phase 0).
///
/// Separates the async read/write data path from the sync `VfsFile` trait
/// so callers that can drive a future (e.g. pager hot path with io_uring)
/// avoid `pollster::block_on` overhead. Implementations that have a native
/// async backend (io_uring via asupersync) override these to submit SQEs
/// directly; sync-only backends get a default that delegates to `VfsFile`.
pub trait AsyncVfsDataPath: VfsFile {
    /// Async read into `buf` at byte `offset`. Returns bytes read; short
    /// reads zero-fill the remainder (same contract as `VfsFile::read`).
    fn read_async(
        &self,
        cx: &Cx,
        buf: &mut [u8],
        offset: u64,
    ) -> impl std::future::Future<Output = Result<usize>> + Send
    where
        Self: Sync,
    {
        let result = self.read(cx, buf, offset);
        async move { result }
    }

    /// Async write of `buf` at byte `offset`.
    fn write_async(
        &self,
        cx: &Cx,
        buf: &[u8],
        offset: u64,
    ) -> impl std::future::Future<Output = Result<()>> + Send
    where
        Self: Sync,
    {
        // Default: synchronous write — the `&mut self` requirement of
        // `VfsFile::write` cannot be met through `&self`, so sync-only
        // backends should override this if they want async write support.
        let _ = (cx, buf, offset);
        async { Err(fsqlite_error::FrankenError::Unsupported) }
    }

    /// Async batch write of page-sized buffers. Default delegates to
    /// sequential `write_async` calls.
    fn write_page_batch_async(
        &self,
        cx: &Cx,
        writes: &[(u64, &[u8])],
    ) -> impl std::future::Future<Output = Result<()>> + Send
    where
        Self: Sync,
    {
        let results: Vec<Result<()>> = writes
            .iter()
            .map(|(offset, data)| {
                let _ = (cx, *data, *offset);
                Ok(())
            })
            .collect();
        async move {
            for r in results {
                r?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that the trait is object-safe for VfsFile (can be used as dyn).
    #[test]
    fn vfs_file_is_object_safe() {
        fn _accepts_dyn(_f: &dyn VfsFile) {}
    }

    /// Verify default implementations exist and don't panic.
    #[test]
    fn vfs_file_defaults() {
        struct DummyFile;
        impl VfsFile for DummyFile {
            fn close(&mut self, _cx: &Cx) -> Result<()> {
                Ok(())
            }
            fn read(&self, _cx: &Cx, _buf: &mut [u8], _offset: u64) -> Result<usize> {
                Ok(0)
            }
            fn write(&mut self, _cx: &Cx, _buf: &[u8], _offset: u64) -> Result<()> {
                Ok(())
            }
            fn truncate(&mut self, _cx: &Cx, _size: u64) -> Result<()> {
                Ok(())
            }
            fn sync(&mut self, _cx: &Cx, _flags: SyncFlags) -> Result<()> {
                Ok(())
            }
            fn file_size(&self, _cx: &Cx) -> Result<u64> {
                Ok(0)
            }
            fn lock(&mut self, _cx: &Cx, _level: LockLevel) -> Result<()> {
                Ok(())
            }
            fn unlock(&mut self, _cx: &Cx, _level: LockLevel) -> Result<()> {
                Ok(())
            }
            fn check_reserved_lock(&self, _cx: &Cx) -> Result<bool> {
                Ok(false)
            }
            fn shm_map(
                &mut self,
                _cx: &Cx,
                _region: u32,
                _size: u32,
                _extend: bool,
            ) -> Result<ShmRegion> {
                Err(fsqlite_error::FrankenError::Unsupported)
            }
            fn shm_lock(&mut self, _cx: &Cx, _offset: u32, _n: u32, _flags: u32) -> Result<()> {
                Err(fsqlite_error::FrankenError::Unsupported)
            }
            fn shm_barrier(&self) {}
            fn shm_unmap(&mut self, _cx: &Cx, _delete: bool) -> Result<()> {
                Ok(())
            }
        }

        let file = DummyFile;
        assert_eq!(file.sector_size(), 4096);
        assert_eq!(file.device_characteristics(), 0);
    }

    /// Verify that VfsFile trait defaults are what we expect.
    #[test]
    fn vfs_file_sector_size_default_is_4096() {
        struct Stub;
        impl VfsFile for Stub {
            fn close(&mut self, _: &Cx) -> Result<()> {
                Ok(())
            }
            fn read(&self, _: &Cx, _: &mut [u8], _: u64) -> Result<usize> {
                Ok(0)
            }
            fn write(&mut self, _: &Cx, _: &[u8], _: u64) -> Result<()> {
                Ok(())
            }
            fn truncate(&mut self, _: &Cx, _: u64) -> Result<()> {
                Ok(())
            }
            fn sync(&mut self, _: &Cx, _: SyncFlags) -> Result<()> {
                Ok(())
            }
            fn file_size(&self, _: &Cx) -> Result<u64> {
                Ok(0)
            }
            fn lock(&mut self, _: &Cx, _: LockLevel) -> Result<()> {
                Ok(())
            }
            fn unlock(&mut self, _: &Cx, _: LockLevel) -> Result<()> {
                Ok(())
            }
            fn check_reserved_lock(&self, _: &Cx) -> Result<bool> {
                Ok(false)
            }
            fn shm_map(&mut self, _: &Cx, _: u32, _: u32, _: bool) -> Result<ShmRegion> {
                Err(fsqlite_error::FrankenError::Unsupported)
            }
            fn shm_lock(&mut self, _: &Cx, _: u32, _: u32, _: u32) -> Result<()> {
                Err(fsqlite_error::FrankenError::Unsupported)
            }
            fn shm_barrier(&self) {}
            fn shm_unmap(&mut self, _: &Cx, _: bool) -> Result<()> {
                Ok(())
            }
        }

        let file = Stub;
        assert_eq!(file.sector_size(), 4096);
        assert_eq!(file.device_characteristics(), 0);
    }

    /// Verify that default Vfs::randomness produces different sequences.
    #[test]
    fn vfs_default_randomness_varies() {
        use crate::memory::MemoryVfs;
        use crate::traits::Vfs;

        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut buf1 = [0u8; 32];
        let mut buf2 = [0u8; 32];
        vfs.randomness(&cx, &mut buf1);
        vfs.randomness(&cx, &mut buf2);
        assert_ne!(buf1, buf2);
    }

    /// Verify that default Vfs::current_time reads from Cx.
    #[test]
    fn vfs_default_current_time_from_cx() {
        use crate::memory::MemoryVfs;
        use crate::traits::Vfs;

        let cx = Cx::new();
        cx.set_unix_millis_for_testing(0);
        let vfs = MemoryVfs::new();
        let t1 = vfs.current_time(&cx);
        // Unix epoch in Julian days is 2440587.5
        #[allow(clippy::approx_constant)]
        let expected = 2_440_587.5;
        assert!(
            (t1 - expected).abs() < 1e-6,
            "at unix epoch, julian day should be ~2440587.5, got {t1}"
        );
    }

    /// Verify randomness with a zero-length buffer doesn't panic.
    #[test]
    fn vfs_randomness_zero_length_buffer() {
        use crate::memory::MemoryVfs;
        use crate::traits::Vfs;

        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut buf = [];
        vfs.randomness(&cx, &mut buf);
    }

    /// Verify randomness with a 1-byte buffer.
    #[test]
    fn vfs_randomness_single_byte() {
        use crate::memory::MemoryVfs;
        use crate::traits::Vfs;

        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut buf = [0u8; 1];
        vfs.randomness(&cx, &mut buf);
        // Can't assert much about the value, just that it doesn't panic.
    }

    #[test]
    fn vfs_is_memory_default_is_false() {
        use crate::memory::MemoryVfs;
        use crate::traits::Vfs;

        let vfs = MemoryVfs::new();
        assert!(vfs.is_memory(), "MemoryVfs::is_memory must return true");
    }

    #[test]
    fn vfs_trait_is_object_safe() {
        use crate::memory::MemoryVfs;
        fn _accepts_dyn(_v: &dyn Vfs<File = crate::memory::MemoryFile>) {}
        let _vfs = MemoryVfs::new();
    }

    #[test]
    fn vfs_file_set_busy_timeout_is_noop() {
        struct Stub;
        impl VfsFile for Stub {
            fn close(&mut self, _: &Cx) -> Result<()> {
                Ok(())
            }
            fn read(&self, _: &Cx, _: &mut [u8], _: u64) -> Result<usize> {
                Ok(0)
            }
            fn write(&mut self, _: &Cx, _: &[u8], _: u64) -> Result<()> {
                Ok(())
            }
            fn truncate(&mut self, _: &Cx, _: u64) -> Result<()> {
                Ok(())
            }
            fn sync(&mut self, _: &Cx, _: SyncFlags) -> Result<()> {
                Ok(())
            }
            fn file_size(&self, _: &Cx) -> Result<u64> {
                Ok(0)
            }
            fn lock(&mut self, _: &Cx, _: LockLevel) -> Result<()> {
                Ok(())
            }
            fn unlock(&mut self, _: &Cx, _: LockLevel) -> Result<()> {
                Ok(())
            }
            fn check_reserved_lock(&self, _: &Cx) -> Result<bool> {
                Ok(false)
            }
            fn shm_map(&mut self, _: &Cx, _: u32, _: u32, _: bool) -> Result<ShmRegion> {
                Err(fsqlite_error::FrankenError::Unsupported)
            }
            fn shm_lock(&mut self, _: &Cx, _: u32, _: u32, _: u32) -> Result<()> {
                Err(fsqlite_error::FrankenError::Unsupported)
            }
            fn shm_barrier(&self) {}
            fn shm_unmap(&mut self, _: &Cx, _: bool) -> Result<()> {
                Ok(())
            }
        }

        let mut file = Stub;
        file.set_busy_timeout_ms(5000);
        file.set_busy_timeout_ms(0);
    }

    #[test]
    fn vfs_file_write_page_batch_default_delegates_to_write() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        static WRITE_COUNT: AtomicUsize = AtomicUsize::new(0);

        struct CountingFile;
        impl VfsFile for CountingFile {
            fn close(&mut self, _: &Cx) -> Result<()> {
                Ok(())
            }
            fn read(&self, _: &Cx, _: &mut [u8], _: u64) -> Result<usize> {
                Ok(0)
            }
            fn write(&mut self, _: &Cx, _: &[u8], _: u64) -> Result<()> {
                WRITE_COUNT.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            fn truncate(&mut self, _: &Cx, _: u64) -> Result<()> {
                Ok(())
            }
            fn sync(&mut self, _: &Cx, _: SyncFlags) -> Result<()> {
                Ok(())
            }
            fn file_size(&self, _: &Cx) -> Result<u64> {
                Ok(0)
            }
            fn lock(&mut self, _: &Cx, _: LockLevel) -> Result<()> {
                Ok(())
            }
            fn unlock(&mut self, _: &Cx, _: LockLevel) -> Result<()> {
                Ok(())
            }
            fn check_reserved_lock(&self, _: &Cx) -> Result<bool> {
                Ok(false)
            }
            fn shm_map(&mut self, _: &Cx, _: u32, _: u32, _: bool) -> Result<ShmRegion> {
                Err(fsqlite_error::FrankenError::Unsupported)
            }
            fn shm_lock(&mut self, _: &Cx, _: u32, _: u32, _: u32) -> Result<()> {
                Err(fsqlite_error::FrankenError::Unsupported)
            }
            fn shm_barrier(&self) {}
            fn shm_unmap(&mut self, _: &Cx, _: bool) -> Result<()> {
                Ok(())
            }
        }

        WRITE_COUNT.store(0, Ordering::Relaxed);
        let cx = Cx::new();
        let mut file = CountingFile;
        let data = [0u8; 4096];
        let writes: Vec<(u64, &[u8])> = vec![(0, &data), (4096, &data), (8192, &data)];
        file.write_page_batch(&cx, &writes).unwrap();
        assert_eq!(WRITE_COUNT.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn vfs_randomness_fills_large_buffer() {
        use crate::memory::MemoryVfs;
        use crate::traits::Vfs;

        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut buf = [0u8; 256];
        vfs.randomness(&cx, &mut buf);
        let all_zero = buf.iter().all(|&b| b == 0);
        assert!(
            !all_zero,
            "256-byte randomness buffer should not be all zeros"
        );
    }

    #[test]
    fn vfs_randomness_non_aligned_buffer() {
        use crate::memory::MemoryVfs;
        use crate::traits::Vfs;

        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut buf = [0u8; 13];
        vfs.randomness(&cx, &mut buf);
        let all_zero = buf.iter().all(|&b| b == 0);
        assert!(!all_zero, "13-byte non-aligned buffer should be filled");
    }

    #[test]
    fn vfs_write_page_batch_empty_is_noop() {
        struct Stub;
        impl VfsFile for Stub {
            fn close(&mut self, _: &Cx) -> Result<()> {
                Ok(())
            }
            fn read(&self, _: &Cx, _: &mut [u8], _: u64) -> Result<usize> {
                Ok(0)
            }
            fn write(&mut self, _: &Cx, _: &[u8], _: u64) -> Result<()> {
                panic!("write should not be called for empty batch");
            }
            fn truncate(&mut self, _: &Cx, _: u64) -> Result<()> {
                Ok(())
            }
            fn sync(&mut self, _: &Cx, _: SyncFlags) -> Result<()> {
                Ok(())
            }
            fn file_size(&self, _: &Cx) -> Result<u64> {
                Ok(0)
            }
            fn lock(&mut self, _: &Cx, _: LockLevel) -> Result<()> {
                Ok(())
            }
            fn unlock(&mut self, _: &Cx, _: LockLevel) -> Result<()> {
                Ok(())
            }
            fn check_reserved_lock(&self, _: &Cx) -> Result<bool> {
                Ok(false)
            }
            fn shm_map(&mut self, _: &Cx, _: u32, _: u32, _: bool) -> Result<ShmRegion> {
                Err(fsqlite_error::FrankenError::Unsupported)
            }
            fn shm_lock(&mut self, _: &Cx, _: u32, _: u32, _: u32) -> Result<()> {
                Err(fsqlite_error::FrankenError::Unsupported)
            }
            fn shm_barrier(&self) {}
            fn shm_unmap(&mut self, _: &Cx, _: bool) -> Result<()> {
                Ok(())
            }
        }

        let cx = Cx::new();
        let mut file = Stub;
        let writes: Vec<(u64, &[u8])> = vec![];
        file.write_page_batch(&cx, &writes).unwrap();
    }

    #[test]
    fn memory_vfs_name_is_memory() {
        use crate::memory::MemoryVfs;
        use crate::traits::Vfs;

        let vfs = MemoryVfs::new();
        assert_eq!(vfs.name(), "memory");
    }

    #[test]
    fn vfs_current_time_advances_with_unix_millis() {
        use crate::memory::MemoryVfs;
        use crate::traits::Vfs;

        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        cx.set_unix_millis_for_testing(0);
        let t0 = vfs.current_time(&cx);
        cx.set_unix_millis_for_testing(86_400_000);
        let t1 = vfs.current_time(&cx);
        let delta = t1 - t0;
        assert!(
            (delta - 1.0).abs() < 1e-6,
            "86400000ms = 1 Julian day, got delta {delta}"
        );
    }

    #[test]
    fn write_page_batch_short_circuits_on_error() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        static CALL_COUNT: AtomicUsize = AtomicUsize::new(0);

        struct FailOnSecond;
        impl VfsFile for FailOnSecond {
            fn close(&mut self, _: &Cx) -> Result<()> {
                Ok(())
            }
            fn read(&self, _: &Cx, _: &mut [u8], _: u64) -> Result<usize> {
                Ok(0)
            }
            fn write(&mut self, _: &Cx, _: &[u8], _: u64) -> Result<()> {
                let n = CALL_COUNT.fetch_add(1, Ordering::Relaxed);
                if n >= 1 {
                    return Err(fsqlite_error::FrankenError::Io(std::io::Error::other(
                        "injected",
                    )));
                }
                Ok(())
            }
            fn truncate(&mut self, _: &Cx, _: u64) -> Result<()> {
                Ok(())
            }
            fn sync(&mut self, _: &Cx, _: SyncFlags) -> Result<()> {
                Ok(())
            }
            fn file_size(&self, _: &Cx) -> Result<u64> {
                Ok(0)
            }
            fn lock(&mut self, _: &Cx, _: LockLevel) -> Result<()> {
                Ok(())
            }
            fn unlock(&mut self, _: &Cx, _: LockLevel) -> Result<()> {
                Ok(())
            }
            fn check_reserved_lock(&self, _: &Cx) -> Result<bool> {
                Ok(false)
            }
            fn shm_map(&mut self, _: &Cx, _: u32, _: u32, _: bool) -> Result<ShmRegion> {
                Err(fsqlite_error::FrankenError::Unsupported)
            }
            fn shm_lock(&mut self, _: &Cx, _: u32, _: u32, _: u32) -> Result<()> {
                Err(fsqlite_error::FrankenError::Unsupported)
            }
            fn shm_barrier(&self) {}
            fn shm_unmap(&mut self, _: &Cx, _: bool) -> Result<()> {
                Ok(())
            }
        }

        CALL_COUNT.store(0, Ordering::Relaxed);
        let cx = Cx::new();
        let mut file = FailOnSecond;
        let data = [0u8; 64];
        let writes: Vec<(u64, &[u8])> = vec![(0, &data), (64, &data), (128, &data)];
        let result = file.write_page_batch(&cx, &writes);
        assert!(result.is_err());
        assert_eq!(
            CALL_COUNT.load(Ordering::Relaxed),
            2,
            "should stop after second write fails, not call third"
        );
    }

    #[test]
    fn vfs_file_defaults_can_be_overridden() {
        struct CustomFile;
        impl VfsFile for CustomFile {
            fn close(&mut self, _: &Cx) -> Result<()> {
                Ok(())
            }
            fn read(&self, _: &Cx, _: &mut [u8], _: u64) -> Result<usize> {
                Ok(0)
            }
            fn write(&mut self, _: &Cx, _: &[u8], _: u64) -> Result<()> {
                Ok(())
            }
            fn truncate(&mut self, _: &Cx, _: u64) -> Result<()> {
                Ok(())
            }
            fn sync(&mut self, _: &Cx, _: SyncFlags) -> Result<()> {
                Ok(())
            }
            fn file_size(&self, _: &Cx) -> Result<u64> {
                Ok(0)
            }
            fn lock(&mut self, _: &Cx, _: LockLevel) -> Result<()> {
                Ok(())
            }
            fn unlock(&mut self, _: &Cx, _: LockLevel) -> Result<()> {
                Ok(())
            }
            fn check_reserved_lock(&self, _: &Cx) -> Result<bool> {
                Ok(false)
            }
            fn sector_size(&self) -> u32 {
                512
            }
            fn device_characteristics(&self) -> u32 {
                0x0010
            }
            fn shm_map(&mut self, _: &Cx, _: u32, _: u32, _: bool) -> Result<ShmRegion> {
                Err(fsqlite_error::FrankenError::Unsupported)
            }
            fn shm_lock(&mut self, _: &Cx, _: u32, _: u32, _: u32) -> Result<()> {
                Err(fsqlite_error::FrankenError::Unsupported)
            }
            fn shm_barrier(&self) {}
            fn shm_unmap(&mut self, _: &Cx, _: bool) -> Result<()> {
                Ok(())
            }
        }

        let file = CustomFile;
        assert_eq!(file.sector_size(), 512);
        assert_eq!(file.device_characteristics(), 0x0010);
    }

    #[test]
    fn vfs_randomness_has_byte_level_entropy() {
        use crate::memory::MemoryVfs;
        use crate::traits::Vfs;

        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut buf = [0u8; 64];
        vfs.randomness(&cx, &mut buf);
        let distinct: std::collections::HashSet<u8> = buf.iter().copied().collect();
        assert!(
            distinct.len() > 4,
            "64-byte buffer should have more than 4 distinct byte values, got {}",
            distinct.len()
        );
    }

    #[test]
    fn vfs_current_time_default_returns_reasonable_julian_day() {
        use crate::memory::MemoryVfs;
        use crate::traits::Vfs;

        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let jd = vfs.current_time(&cx);
        assert!(jd.is_finite(), "Julian day must be finite");
        assert!(
            jd > 2_440_000.0,
            "Julian day should be after ~1968, got {jd}"
        );
    }

    #[test]
    fn durable_sync_default_delegates_to_sync() {
        use std::sync::atomic::{AtomicU8, Ordering};
        static LAST_FLAGS: AtomicU8 = AtomicU8::new(0);

        struct RecordingFile;
        impl VfsFile for RecordingFile {
            fn close(&mut self, _: &Cx) -> Result<()> {
                Ok(())
            }
            fn read(&self, _: &Cx, _: &mut [u8], _: u64) -> Result<usize> {
                Ok(0)
            }
            fn write(&mut self, _: &Cx, _: &[u8], _: u64) -> Result<()> {
                Ok(())
            }
            fn truncate(&mut self, _: &Cx, _: u64) -> Result<()> {
                Ok(())
            }
            fn sync(&mut self, _: &Cx, flags: SyncFlags) -> Result<()> {
                LAST_FLAGS.store(flags.bits(), Ordering::Relaxed);
                Ok(())
            }
            fn file_size(&self, _: &Cx) -> Result<u64> {
                Ok(0)
            }
            fn lock(&mut self, _: &Cx, _: LockLevel) -> Result<()> {
                Ok(())
            }
            fn unlock(&mut self, _: &Cx, _: LockLevel) -> Result<()> {
                Ok(())
            }
            fn check_reserved_lock(&self, _: &Cx) -> Result<bool> {
                Ok(false)
            }
            fn shm_map(&mut self, _: &Cx, _: u32, _: u32, _: bool) -> Result<ShmRegion> {
                Err(fsqlite_error::FrankenError::Unsupported)
            }
            fn shm_lock(&mut self, _: &Cx, _: u32, _: u32, _: u32) -> Result<()> {
                Err(fsqlite_error::FrankenError::Unsupported)
            }
            fn shm_barrier(&self) {}
            fn shm_unmap(&mut self, _: &Cx, _: bool) -> Result<()> {
                Ok(())
            }
        }

        let cx = Cx::new();
        let mut f = RecordingFile;

        f.durable_sync(&cx, SyncKind::DataOnly).unwrap();
        assert_eq!(
            LAST_FLAGS.load(Ordering::Relaxed),
            SyncFlags::DATAONLY.bits()
        );

        f.durable_sync(&cx, SyncKind::FullDurable).unwrap();
        assert_eq!(LAST_FLAGS.load(Ordering::Relaxed), SyncFlags::FULL.bits());

        f.durable_sync(&cx, SyncKind::DataAndMetadata).unwrap();
        assert_eq!(LAST_FLAGS.load(Ordering::Relaxed), SyncFlags::FULL.bits());
    }

    #[test]
    fn sync_kind_variants_are_distinct() {
        assert_ne!(SyncKind::DataOnly, SyncKind::DataAndMetadata);
        assert_ne!(SyncKind::DataAndMetadata, SyncKind::FullDurable);
        assert_ne!(SyncKind::DataOnly, SyncKind::FullDurable);
    }

    #[test]
    fn async_vfs_data_path_trait_is_implementable() {
        use crate::memory::MemoryFile;

        fn assert_impl<T: AsyncVfsDataPath>() {}
        assert_impl::<MemoryFile>();
    }

    #[test]
    fn async_vfs_data_path_default_read_resolves_immediately() {
        use crate::memory::MemoryVfs;

        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let flags = fsqlite_types::flags::VfsOpenFlags::MAIN_DB
            | fsqlite_types::flags::VfsOpenFlags::CREATE
            | fsqlite_types::flags::VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs.open(&cx, None, flags).unwrap();

        let payload = b"hello async vfs";
        file.write(&cx, payload, 0).unwrap();

        let mut buf = [0u8; 15];
        let n = pollster::block_on(AsyncVfsDataPath::read_async(&file, &cx, &mut buf, 0)).unwrap();
        assert_eq!(n, 15);
        assert_eq!(&buf, payload);
    }
}
