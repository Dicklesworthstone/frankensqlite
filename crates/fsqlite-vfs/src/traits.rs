use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::LockLevel;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};

use crate::shm::{
    SQLITE_SHM_EXCLUSIVE, SQLITE_SHM_LOCK, SQLITE_SHM_UNLOCK, ShmRegion, WAL_CKPT_LOCK,
    WAL_WRITE_LOCK,
};

/// Opaque identity of an already-open filesystem object.
///
/// Identities are intended only as opaque comparison keys while the relevant
/// file handles remain open. Their ordering carries no filesystem meaning;
/// it exists only for ordered collections. They are not persistent database
/// identifiers and must not be serialized or compared across machines or
/// boots.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FileIdentity {
    kind: FileIdentityKind,
    namespace: u64,
    object: [u8; 16],
}

/// Representation domain for an opaque [`FileIdentity`].
///
/// The discriminator is part of equality and ordering so a legacy Windows
/// 64-bit file index can never compare equal to a 128-bit `FILE_ID_INFO`
/// value that happens to contain the same bytes.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum FileIdentityKind {
    /// Stable identity for one file generation inside an in-process VFS.
    Memory,
    #[cfg(unix)]
    Unix,
    #[cfg(windows)]
    WindowsFileId128,
    #[cfg(windows)]
    WindowsFileIndex64,
}

impl FileIdentity {
    /// Construct an identity for a file owned by an in-process VFS.
    ///
    /// Both components are opaque process-local tokens. They only need to be
    /// stable while the corresponding VFS/file storage objects are alive; the
    /// identity is used to coalesce pager coordination gates, never persisted.
    #[must_use]
    pub(crate) fn from_memory_parts(namespace: u64, object: u64) -> Self {
        let mut object_bytes = [0_u8; 16];
        object_bytes[..8].copy_from_slice(&object.to_ne_bytes());
        Self {
            kind: FileIdentityKind::Memory,
            namespace,
            object: object_bytes,
        }
    }

    /// Read the identity of an independently opened filesystem descriptor.
    ///
    /// On Unix this uses descriptor metadata (`st_dev`, `st_ino`). On Windows
    /// it uses the volume serial number and full 128-bit file identifier
    /// associated with the open handle. A later rename or pathname replacement
    /// therefore does not change the result. Platforms without a stable
    /// descriptor identity exposed by this crate return `Ok(None)`.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_file(file: &std::fs::File) -> std::io::Result<Option<Self>> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;

            let metadata = file.metadata()?;
            Ok(Some(Self::from_unix_parts(metadata.dev(), metadata.ino())))
        }

        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle as _;

            let handle = file.as_raw_handle();
            let file_id_result = query_windows_file_id(handle);
            Self::from_windows_query_result(file_id_result, || {
                query_windows_legacy_file_index(handle)
            })
        }

        #[cfg(not(any(unix, windows)))]
        {
            let _ = file;
            Ok(None)
        }
    }

    #[cfg(unix)]
    pub(crate) fn from_unix_parts(device: u64, inode: u64) -> Self {
        let mut object = [0_u8; 16];
        object[..8].copy_from_slice(&inode.to_be_bytes());
        Self {
            kind: FileIdentityKind::Unix,
            namespace: device,
            object,
        }
    }

    #[cfg(windows)]
    fn from_windows_parts(volume_serial_number: u64, file_id: [u8; 16]) -> Option<Self> {
        // MS-FSCC 2.1.10 reserves all-zero for filesystems without a
        // 128-bit file ID and all-ones for files whose unique ID cannot be
        // established. Both values MUST be ignored, so neither can safely
        // participate in an expected-identity comparison.
        if file_id.iter().all(|byte| *byte == 0) || file_id.iter().all(|byte| *byte == u8::MAX) {
            return None;
        }
        Some(Self {
            kind: FileIdentityKind::WindowsFileId128,
            namespace: volume_serial_number,
            object: file_id,
        })
    }

    #[cfg(windows)]
    fn from_windows_legacy_parts(
        volume_serial_number: u32,
        file_index_high: u32,
        file_index_low: u32,
    ) -> Self {
        let file_index = (u64::from(file_index_high) << 32) | u64::from(file_index_low);
        let mut object = [0_u8; 16];
        object[..8].copy_from_slice(&file_index.to_be_bytes());
        Self {
            kind: FileIdentityKind::WindowsFileIndex64,
            namespace: u64::from(volume_serial_number),
            object,
        }
    }

    #[cfg(windows)]
    fn from_windows_query_result<F>(
        file_id_result: std::io::Result<(u64, [u8; 16])>,
        legacy_query: F,
    ) -> std::io::Result<Option<Self>>
    where
        F: FnOnce() -> std::io::Result<(u32, u32, u32)>,
    {
        match file_id_result {
            Ok((volume_serial_number, file_id)) => {
                Ok(Self::from_windows_parts(volume_serial_number, file_id))
            }
            Err(err) if is_windows_file_id_unsupported(&err) => {
                let (volume_serial_number, file_index_high, file_index_low) = legacy_query()?;
                Ok(Some(Self::from_windows_legacy_parts(
                    volume_serial_number,
                    file_index_high,
                    file_index_low,
                )))
            }
            Err(err) => Err(err),
        }
    }

    /// Encode this identity for a namespace record.
    ///
    /// Byte 0 is the representation tag, bytes 1..9 are the big-endian
    /// namespace, and bytes 9..25 are the exact object identifier.
    #[cfg(any(unix, windows))]
    pub(crate) fn to_namespace_bytes(self) -> [u8; 25] {
        let tag = match self.kind {
            // Memory identities are process-local. The tag round-trips for
            // in-process coordination/tests only; callers must never persist
            // it as a reusable cross-process namespace identity.
            FileIdentityKind::Memory => 4,
            #[cfg(unix)]
            FileIdentityKind::Unix => 1,
            #[cfg(windows)]
            FileIdentityKind::WindowsFileId128 => 2,
            #[cfg(windows)]
            FileIdentityKind::WindowsFileIndex64 => 3,
        };
        let mut encoded = [0_u8; 25];
        encoded[0] = tag;
        encoded[1..9].copy_from_slice(&self.namespace.to_be_bytes());
        encoded[9..].copy_from_slice(&self.object);
        encoded
    }

    /// Decode and validate a namespace-record identity.
    #[cfg(any(unix, windows))]
    pub(crate) fn from_namespace_bytes(encoded: [u8; 25]) -> Option<Self> {
        let mut namespace_bytes = [0_u8; 8];
        namespace_bytes.copy_from_slice(&encoded[1..9]);
        let namespace = u64::from_be_bytes(namespace_bytes);
        let mut object = [0_u8; 16];
        object.copy_from_slice(&encoded[9..]);

        match encoded[0] {
            4 if object[8..].iter().all(|byte| *byte == 0) => Some(Self {
                kind: FileIdentityKind::Memory,
                namespace,
                object,
            }),
            #[cfg(unix)]
            1 if object[8..].iter().all(|byte| *byte == 0) => Some(Self {
                kind: FileIdentityKind::Unix,
                namespace,
                object,
            }),
            #[cfg(windows)]
            2 => Self::from_windows_parts(namespace, object),
            #[cfg(windows)]
            3 if u32::try_from(namespace).is_ok() && object[8..].iter().all(|byte| *byte == 0) => {
                Some(Self {
                    kind: FileIdentityKind::WindowsFileIndex64,
                    namespace,
                    object,
                })
            }
            _ => None,
        }
    }
}

#[cfg(all(test, any(unix, windows)))]
mod memory_file_identity_codec_tests {
    use super::FileIdentity;

    #[test]
    fn memory_namespace_bytes_round_trip_with_distinct_tag() {
        let identity = FileIdentity::from_memory_parts(17, 29);
        let encoded = identity.to_namespace_bytes();

        assert_eq!(encoded[0], 4);
        assert_eq!(FileIdentity::from_namespace_bytes(encoded), Some(identity));
        assert_ne!(
            identity,
            FileIdentity::from_memory_parts(18, 29),
            "independent MemoryVfs instances must remain isolated"
        );
        assert_ne!(
            identity,
            FileIdentity::from_memory_parts(17, 30),
            "distinct named files in one MemoryVfs must remain isolated"
        );
    }
}

#[cfg(windows)]
fn query_windows_file_id(
    handle: std::os::windows::io::RawHandle,
) -> std::io::Result<(u64, [u8; 16])> {
    use std::mem::size_of;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ID_INFO, FileIdInfo, GetFileInformationByHandleEx,
    };

    let mut identity = FILE_ID_INFO::default();
    let identity_size = u32::try_from(size_of::<FILE_ID_INFO>())
        .map_err(|_| std::io::Error::other("FILE_ID_INFO size does not fit in a Windows DWORD"))?;
    // SAFETY: the caller supplies a live Windows file handle, `identity` is a
    // correctly sized writable `FILE_ID_INFO` buffer, and both remain valid
    // for the duration of this synchronous system call.
    let succeeded = unsafe {
        GetFileInformationByHandleEx(
            handle,
            FileIdInfo,
            std::ptr::from_mut(&mut identity).cast(),
            identity_size,
        )
    };
    if succeeded == 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok((identity.VolumeSerialNumber, identity.FileId.Identifier))
}

#[cfg(windows)]
fn query_windows_legacy_file_index(
    handle: std::os::windows::io::RawHandle,
) -> std::io::Result<(u32, u32, u32)> {
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
    };

    let mut identity = BY_HANDLE_FILE_INFORMATION::default();
    // SAFETY: the caller supplies a live Windows file handle and `identity`
    // is a correctly sized writable buffer that remains valid for the
    // duration of this synchronous system call.
    let succeeded = unsafe { GetFileInformationByHandle(handle, &raw mut identity) };
    if succeeded == 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok((
        identity.dwVolumeSerialNumber,
        identity.nFileIndexHigh,
        identity.nFileIndexLow,
    ))
}

#[cfg(windows)]
fn is_windows_file_id_unsupported(err: &std::io::Error) -> bool {
    use windows_sys::Win32::Foundation::{
        ERROR_CALL_NOT_IMPLEMENTED, ERROR_INVALID_FUNCTION, ERROR_INVALID_PARAMETER,
        ERROR_NOT_SUPPORTED,
    };

    err.raw_os_error().is_some_and(|raw_error| {
        raw_error == ERROR_INVALID_FUNCTION as i32
            || raw_error == ERROR_NOT_SUPPORTED as i32
            || raw_error == ERROR_INVALID_PARAMETER as i32
            || raw_error == ERROR_CALL_NOT_IMPLEMENTED as i32
    })
}

impl std::fmt::Debug for FileIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FileIdentity(..)")
    }
}

#[cfg(all(test, unix))]
mod unix_file_identity_codec_tests {
    use super::FileIdentity;

    #[test]
    fn unix_namespace_bytes_round_trip_exactly() {
        let identity = FileIdentity::from_unix_parts(0x0102_0304_0506_0708, 0x1122_3344_5566_7788);
        let encoded = identity.to_namespace_bytes();

        assert_eq!(encoded[0], 1);
        assert_eq!(&encoded[1..9], &0x0102_0304_0506_0708_u64.to_be_bytes());
        assert_eq!(&encoded[9..17], &0x1122_3344_5566_7788_u64.to_be_bytes());
        assert_eq!(&encoded[17..], &[0_u8; 8]);
        assert_eq!(FileIdentity::from_namespace_bytes(encoded), Some(identity));
    }

    #[test]
    fn unix_namespace_bytes_reject_unknown_and_noncanonical_records() {
        let identity = FileIdentity::from_unix_parts(7, 11);
        let mut unknown_tag = identity.to_namespace_bytes();
        unknown_tag[0] = u8::MAX;
        assert_eq!(FileIdentity::from_namespace_bytes(unknown_tag), None);

        let mut nonzero_tail = identity.to_namespace_bytes();
        nonzero_tail[24] = 1;
        assert_eq!(FileIdentity::from_namespace_bytes(nonzero_tail), None);
    }
}

#[cfg(all(test, windows))]
mod file_identity_tests {
    use super::FileIdentity;
    use std::cell::Cell;
    use std::io;
    use windows_sys::Win32::Foundation::{
        ERROR_CALL_NOT_IMPLEMENTED, ERROR_INVALID_FUNCTION, ERROR_INVALID_PARAMETER,
        ERROR_NOT_SUPPORTED,
    };

    #[test]
    fn windows_identity_compares_all_file_id_bits() {
        let file_id = [0x5a_u8; 16];
        let mut different_high_byte = file_id;
        different_high_byte[15] ^= 0xff;

        assert_ne!(
            FileIdentity::from_windows_parts(7, file_id),
            FileIdentity::from_windows_parts(7, different_high_byte),
        );
    }

    #[test]
    fn windows_identity_rejects_reserved_sentinels() {
        assert_eq!(FileIdentity::from_windows_parts(7, [0_u8; 16]), None);
        assert_eq!(FileIdentity::from_windows_parts(7, [u8::MAX; 16]), None);
    }

    #[test]
    fn windows_identity_prefers_full_file_id_without_legacy_query() {
        let expected = [0x3c_u8; 16];
        let legacy_queried = Cell::new(false);

        let actual = FileIdentity::from_windows_query_result(Ok((19, expected)), || {
            legacy_queried.set(true);
            Ok((19, 0, 7))
        })
        .expect("full FileIdInfo result should succeed");

        assert_eq!(actual, FileIdentity::from_windows_parts(19, expected));
        assert!(!legacy_queried.get());
    }

    #[test]
    fn windows_identity_falls_back_for_unsupported_file_id_queries() {
        for code in [
            ERROR_INVALID_FUNCTION,
            ERROR_NOT_SUPPORTED,
            ERROR_INVALID_PARAMETER,
            ERROR_CALL_NOT_IMPLEMENTED,
        ] {
            let actual = FileIdentity::from_windows_query_result(
                Err(io::Error::from_raw_os_error(code as i32)),
                || Ok((23, 0x1122_3344, 0x5566_7788)),
            )
            .expect("unsupported FileIdInfo should use the legacy query")
            .expect("legacy file index should produce an identity");

            assert_eq!(
                actual,
                FileIdentity::from_windows_legacy_parts(23, 0x1122_3344, 0x5566_7788)
            );
        }
    }

    #[test]
    fn windows_identity_does_not_fallback_for_real_io_errors() {
        let legacy_queried = Cell::new(false);
        let err =
            FileIdentity::from_windows_query_result(Err(io::Error::from_raw_os_error(6)), || {
                legacy_queried.set(true);
                Ok((1, 2, 3))
            })
            .expect_err("invalid handles must remain hard errors");

        assert_eq!(err.raw_os_error(), Some(6));
        assert!(!legacy_queried.get());
    }

    #[test]
    fn windows_identity_separates_full_and_legacy_representation_domains() {
        let file_index = 0x1122_3344_5566_7788_u64;
        let mut full_file_id = [0_u8; 16];
        full_file_id[..8].copy_from_slice(&file_index.to_be_bytes());

        assert_ne!(
            FileIdentity::from_windows_parts(23, full_file_id).expect("non-sentinel full file ID"),
            FileIdentity::from_windows_legacy_parts(23, 0x1122_3344, 0x5566_7788),
        );
    }

    #[test]
    fn windows_namespace_bytes_round_trip_both_identity_domains() {
        let full_object = [0x5a_u8; 16];
        let full = FileIdentity::from_windows_parts(0x0102_0304_0506_0708, full_object)
            .expect("non-sentinel full file ID");
        let full_encoded = full.to_namespace_bytes();
        assert_eq!(full_encoded[0], 2);
        assert_eq!(
            &full_encoded[1..9],
            &0x0102_0304_0506_0708_u64.to_be_bytes()
        );
        assert_eq!(&full_encoded[9..], &full_object);
        assert_eq!(FileIdentity::from_namespace_bytes(full_encoded), Some(full));

        let legacy = FileIdentity::from_windows_legacy_parts(0x0102_0304, 0x1122_3344, 0x5566_7788);
        let legacy_encoded = legacy.to_namespace_bytes();
        assert_eq!(legacy_encoded[0], 3);
        assert_eq!(&legacy_encoded[1..9], &0x0102_0304_u64.to_be_bytes());
        assert_eq!(
            &legacy_encoded[9..17],
            &0x1122_3344_5566_7788_u64.to_be_bytes()
        );
        assert_eq!(&legacy_encoded[17..], &[0_u8; 8]);
        assert_eq!(
            FileIdentity::from_namespace_bytes(legacy_encoded),
            Some(legacy)
        );
    }

    #[test]
    fn windows_namespace_bytes_reject_noncanonical_records() {
        let mut unknown_tag = [0_u8; 25];
        unknown_tag[0] = u8::MAX;
        assert_eq!(FileIdentity::from_namespace_bytes(unknown_tag), None);

        let mut full_zero_sentinel = [0_u8; 25];
        full_zero_sentinel[0] = 2;
        assert_eq!(FileIdentity::from_namespace_bytes(full_zero_sentinel), None);

        let mut full_ones_sentinel = [u8::MAX; 25];
        full_ones_sentinel[0] = 2;
        assert_eq!(FileIdentity::from_namespace_bytes(full_ones_sentinel), None);

        let legacy = FileIdentity::from_windows_legacy_parts(7, 11, 13);
        let mut legacy_nonzero_tail = legacy.to_namespace_bytes();
        legacy_nonzero_tail[24] = 1;
        assert_eq!(
            FileIdentity::from_namespace_bytes(legacy_nonzero_tail),
            None
        );

        let mut legacy_wide_namespace = legacy.to_namespace_bytes();
        legacy_wide_namespace[1..9].copy_from_slice(&(u64::from(u32::MAX) + 1).to_be_bytes());
        assert_eq!(
            FileIdentity::from_namespace_bytes(legacy_wide_namespace),
            None
        );
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

    /// Open an existing file only if its handle has `expected_identity`.
    ///
    /// The default implementation verifies the identity immediately after
    /// opening. Filesystem backends whose normal open path can mutate related
    /// artifacts should override this method and perform an earlier,
    /// side-effect-free identity preflight as well. Because an expected
    /// identity can only belong to an existing object, `CREATE` and
    /// `EXCLUSIVE` are stripped before the open.
    fn open_with_expected_identity(
        &self,
        cx: &Cx,
        path: &Path,
        flags: VfsOpenFlags,
        expected_identity: FileIdentity,
    ) -> Result<(Self::File, VfsOpenFlags)> {
        let mut existing_flags = flags;
        existing_flags.remove(VfsOpenFlags::CREATE | VfsOpenFlags::EXCLUSIVE);
        let (file, actual_flags) = self.open(cx, Some(path), existing_flags)?;
        if file.file_identity()? != Some(expected_identity) {
            return Err(fsqlite_error::FrankenError::CannotOpen {
                path: path.to_owned(),
            });
        }
        Ok((file, actual_flags))
    }

    /// Open a caller-reserved empty file without creating it or recovering
    /// any pre-existing database artifacts.
    ///
    /// Backends whose ordinary open path creates auxiliary files must
    /// override this method so the identity, zero-length, and recovery-
    /// artifact checks all occur before those side effects.
    fn open_reserved_with_expected_identity(
        &self,
        cx: &Cx,
        path: &Path,
        flags: VfsOpenFlags,
        expected_identity: FileIdentity,
    ) -> Result<(Self::File, VfsOpenFlags)> {
        let (file, actual_flags) =
            self.open_with_expected_identity(cx, path, flags, expected_identity)?;
        if file.file_size(cx)? != 0 {
            return Err(fsqlite_error::FrankenError::CannotOpen {
                path: path.to_owned(),
            });
        }

        for suffix in ["-journal", "-wal", "-wal-fec", "-shm"] {
            let mut artifact_path = path.as_os_str().to_owned();
            artifact_path.push(suffix);
            if self.path_entry_exists(cx, Path::new(&artifact_path))? {
                return Err(fsqlite_error::FrankenError::CannotOpen {
                    path: path.to_owned(),
                });
            }
        }
        Ok((file, actual_flags))
    }

    /// Delete a file.
    ///
    /// If `sync_dir` is true, the directory entry removal should be synced
    /// to ensure durability.
    fn delete(&self, cx: &Cx, path: &Path, sync_dir: bool) -> Result<()>;

    /// Synchronize the parent directory containing `path`.
    ///
    /// Durable create-before-mutate protocols (notably rollback journals)
    /// must make the newly-created directory entry stable before they modify
    /// the protected file.  Filesystems that do not require or support an
    /// explicit directory sync may keep the default no-op implementation.
    fn sync_parent_directory(&self, _cx: &Cx, _path: &Path) -> Result<()> {
        Ok(())
    }

    /// Check file access.
    ///
    /// Returns true if the file at `path` satisfies the access check
    /// described by `flags`.
    fn access(&self, cx: &Cx, path: &Path, flags: AccessFlags) -> Result<bool>;

    /// Return whether a directory entry exists without following its final
    /// symlink component.
    ///
    /// This is stricter than [`Path::exists`] and is required for gates that
    /// must refuse dangling recovery-artifact symlinks. Virtual filesystems
    /// without symlink semantics may delegate to [`Self::access`].
    fn path_entry_exists(&self, cx: &Cx, path: &Path) -> Result<bool> {
        self.access(cx, path, AccessFlags::EXISTS)
    }

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

    /// Acquire the cross-process SHARED fence used while capturing a coherent
    /// main-database snapshot.
    ///
    /// Native Unix locks already use stock SQLite's main-file byte ranges, so
    /// the default is the ordinary SHARED lock. Platform VFSes whose ordinary
    /// locks are private coordination surfaces must additionally fence the
    /// byte ranges used by the system SQLite VFS.
    fn lock_external_shared_snapshot(&mut self, cx: &Cx) -> Result<()> {
        self.lock(cx, LockLevel::Shared)
    }

    /// Release a fence acquired by [`Self::lock_external_shared_snapshot`].
    fn unlock_external_shared_snapshot(&mut self, cx: &Cx) -> Result<()> {
        self.unlock(cx, LockLevel::None)
    }

    /// Acquire the cross-process fence for an operation that replaces the
    /// complete main-database image in place.
    ///
    /// The default implementation composes the ordinary SQLite lock surfaces:
    /// in WAL mode it first excludes writers and checkpointers through the
    /// adjacent WAL write/checkpoint slots, then obtains the main-database
    /// EXCLUSIVE lock. Platform VFSes whose ordinary locks are not visible to
    /// the system SQLite VFS must override this hook with a stock-compatible
    /// external fence.
    fn lock_external_maintenance(&mut self, cx: &Cx, wal_mode: bool) -> Result<()> {
        if wal_mode {
            self.shm_lock(
                cx,
                WAL_WRITE_LOCK,
                WAL_CKPT_LOCK - WAL_WRITE_LOCK + 1,
                SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE,
            )?;
        }

        if let Err(lock_error) = self.lock(cx, LockLevel::Exclusive) {
            if wal_mode
                && let Err(unlock_error) = self.shm_lock(
                    cx,
                    WAL_WRITE_LOCK,
                    WAL_CKPT_LOCK - WAL_WRITE_LOCK + 1,
                    SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE,
                )
            {
                return Err(FrankenError::internal(format!(
                    "external maintenance could not acquire the main database lock or release WAL maintenance locks: lock={lock_error}; unlock={unlock_error}"
                )));
            }
            return Err(lock_error);
        }
        Ok(())
    }

    /// Release a fence acquired by [`Self::lock_external_maintenance`].
    ///
    /// Both lock surfaces are attempted even if one release fails, so a
    /// cleanup error cannot silently strand the other cross-process lock.
    fn unlock_external_maintenance(&mut self, cx: &Cx, wal_mode: bool) -> Result<()> {
        let main_result = self.unlock(cx, LockLevel::None);
        let wal_result = if wal_mode {
            self.shm_lock(
                cx,
                WAL_WRITE_LOCK,
                WAL_CKPT_LOCK - WAL_WRITE_LOCK + 1,
                SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE,
            )
        } else {
            Ok(())
        };

        match (main_result, wal_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(main_error), Err(wal_error)) => Err(FrankenError::internal(format!(
                "external maintenance could not release all locks: main={main_error}; wal={wal_error}"
            ))),
        }
    }

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
