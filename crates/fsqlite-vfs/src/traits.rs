use std::path::{Path, PathBuf};

use fsqlite_error::Result;
use fsqlite_types::LockLevel;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};

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
        use std::sync::atomic::{AtomicU64, Ordering};

        static CALL_SEQ: AtomicU64 = AtomicU64::new(0);

        // Default: fill with pseudo-random bytes using a simple xorshift.
        // Real VFS implementations should use OS-provided randomness.
        let _ = cx; // Usage to silence unused variable warning
        let seq = CALL_SEQ.fetch_add(1, Ordering::Relaxed);
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
        // Default: use system time.
        let _ = cx;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        // Unix epoch in Julian days: 2440587.5
        #[allow(clippy::cast_precision_loss)]
        let julian = 2_440_587.5 + (now.as_secs_f64() / 86400.0);
        julian
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

    /// Read `buf.len()` bytes starting at byte offset `offset`.
    ///
    /// Returns the number of bytes actually read. If fewer bytes are read
    /// than requested (short read), the remaining bytes in `buf` are zeroed.
    fn read(&mut self, cx: &Cx, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// Write `buf` starting at byte offset `offset`.
    fn write(&mut self, cx: &Cx, buf: &[u8], offset: u64) -> Result<()>;

    /// Truncate the file to `size` bytes.
    fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()>;

    /// Sync the file contents to stable storage.
    ///
    /// `flags` indicates the type of sync (normal, full, data-only).
    fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()>;

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
    /// Returns a mutable pointer to the mapped region.
    /// (Equivalent to sqlite3_io_methods.xShmMap)
    fn shm_map(&mut self, cx: &Cx, region: u32, size: u32, extend: bool) -> Result<*mut u8>;

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
            fn read(&mut self, _cx: &Cx, _buf: &mut [u8], _offset: u64) -> Result<usize> {
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
            ) -> Result<*mut u8> {
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
}
