//! Storage allocation for writable Unix shared-memory mappings.
//!
//! File length is not an allocation receipt. In particular, a failed growth
//! attempt may leave a full-sized sparse file behind. Where the platform can
//! allocate natively beside live mappings (Linux and Android), every uncached
//! mapping establishes backing for the complete prefix it makes available to
//! a stock SQLite peer before publishing a writable alias, so a full
//! filesystem is an I/O error instead of a SIGBUS (GH #506).
//!
//! That protection is best effort. Releases up to 0.4.10 extended the file
//! sparsely and mapped it, and that still has to work wherever it worked
//! before: on a filesystem with no `fallocate` (NFS before 4.2, 9p, many FUSE
//! mounts), on btrfs extents shared by a snapshot or a reflink copy, through a
//! descriptor that cannot allocate, and on every Unix without a native
//! allocator that is safe beside live mappings (macOS included). In all of
//! those the mapping sites fall back to the sparse extension. Only an error
//! that says the storage itself failed (see [`is_storage_failure`]) refuses
//! the mapping.

use std::fs::File;
use std::io;
#[cfg(any(target_os = "linux", target_os = "android"))]
use std::os::fd::AsRawFd as _;
#[cfg(test)]
use std::os::unix::fs::FileExt as _;
use std::sync::atomic::{AtomicBool, Ordering};

/// What an allocation attempt established for the requested prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShmBacking {
    /// The platform allocated storage for the whole prefix, or proved that it
    /// already had it.
    Allocated,
    /// The platform, the filesystem or the descriptor cannot preallocate.
    /// Nothing was allocated; the mapping behaves as it did up to 0.4.10.
    Sparse,
}

/// Check allocation without changing file contents, length, or timestamps.
///
/// `false` means that storage is missing or that the filesystem cannot prove
/// allocation. Linux may flush already dirty pages when FIEMAP reports
/// delayed allocation; writeback errors propagate instead of being treated as
/// allocation receipts. Targets without a native allocator never prove it.
pub fn shm_backing_is_allocated(file: &File, target_len: u64) -> io::Result<bool> {
    if target_len == 0 {
        return Ok(true);
    }
    checked_length(target_len)?;
    if file.metadata()?.len() < target_len {
        return Ok(false);
    }
    platform_backing_is_allocated(file, target_len)
}

/// Allocate backing natively, without truncating or rewriting contents.
///
/// Linux/Android use native allocation, which preserves bytes and is safe
/// alongside existing mappings. Nothing truncates on error: another alias or
/// process may depend on an already extended prefix, and a retry rechecks
/// storage even when the file has the requested size.
///
/// `Ok(ShmBacking::Sparse)` means preallocation is not possible here and
/// nothing was changed; the caller decides whether to extend the file. An
/// `Err` is always a storage failure the caller must surface.
pub fn ensure_shm_backing(file: &File, target_len: u64) -> io::Result<ShmBacking> {
    if target_len == 0 {
        return Ok(ShmBacking::Allocated);
    }
    checked_length(target_len)?;
    match allocate_shm_backing(file, target_len) {
        Ok(()) => Ok(ShmBacking::Allocated),
        Err(error) if is_storage_failure(&error) => Err(error),
        Err(error) => {
            note_sparse_fallback(&error);
            Ok(ShmBacking::Sparse)
        }
    }
}

/// The policy for a site that grows a SHM file before mapping it: prove or
/// allocate the prefix, and where that is not possible extend the file
/// sparsely, exactly as releases up to 0.4.10 did at the same site.
pub fn ensure_shm_backing_or_sparse(file: &File, target_len: u64) -> io::Result<ShmBacking> {
    let backing = ensure_proven_or_allocated(file, target_len)?;
    if backing == ShmBacking::Sparse && file.metadata()?.len() < target_len {
        file.set_len(target_len)?;
    }
    Ok(backing)
}

/// The policy for an existing payload that peers may already be using: prove
/// or allocate the prefix where the platform can, and otherwise leave the
/// file exactly as it is. The fallback never changes its length.
pub fn ensure_existing_shm_backing(file: &File, target_len: u64) -> io::Result<ShmBacking> {
    ensure_proven_or_allocated(file, target_len)
}

fn ensure_proven_or_allocated(file: &File, target_len: u64) -> io::Result<ShmBacking> {
    match shm_backing_is_allocated(file, target_len) {
        Ok(true) => Ok(ShmBacking::Allocated),
        Err(error) if is_storage_failure(&error) => Err(error),
        // Unproven, or a probe that cannot answer: neither refuses a mapping.
        Ok(false) | Err(_) => ensure_shm_backing(file, target_len),
    }
}

/// Errors that say the storage itself failed: no space, quota exhausted, an
/// I/O error, or a size the file may not reach. These are the failures GH #506
/// is about, and they refuse the mapping. Every other error from a probe or an
/// allocation attempt (`EOPNOTSUPP`, `ENOSYS`, `EINVAL`, `EBADF`, `EPERM`, ...)
/// means that preallocation is unavailable for this file, which is not a
/// reason to fail an open that worked before preallocation existed.
fn is_storage_failure(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::ENOSPC | libc::EDQUOT | libc::EIO | libc::EFBIG)
    )
}

/// Say once per process that SHM preallocation is unavailable, so an operator
/// can tell that a full filesystem can still raise SIGBUS on this mount.
fn note_sparse_fallback(error: &io::Error) {
    static NOTED: AtomicBool = AtomicBool::new(false);
    if !NOTED.swap(true, Ordering::Relaxed) {
        tracing::warn!(
            %error,
            "SHM preallocation is unavailable here; mapping sparsely as releases up to 0.4.10 did"
        );
    }
}

fn allocate_shm_backing(file: &File, target_len: u64) -> io::Result<()> {
    #[cfg(test)]
    if let Some(fault) = ALLOCATION_FAULT.with(std::cell::Cell::get) {
        if let Some(prefix) = fault.allocated_prefix {
            // A target without a native allocator still gets the sparse
            // remainder below, which is what the fixture is for.
            if let Err(error) = platform_ensure_shm_backing(file, prefix.min(target_len))
                && is_storage_failure(&error)
            {
                return Err(error);
            }
            // Deliberately reproduce a legacy/failed allocation's sparse
            // remainder. Production allocation never uses this set_len.
            if file.metadata()?.len() < target_len {
                file.set_len(target_len)?;
            }
        }
        return Err(io::Error::from_raw_os_error(fault.errno));
    }
    platform_ensure_shm_backing(file, target_len)
}

fn checked_length(target_len: u64) -> io::Result<libc::off_t> {
    libc::off_t::try_from(target_len).map_err(|_| io::Error::from_raw_os_error(libc::EFBIG))
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn platform_ensure_shm_backing(file: &File, target_len: u64) -> io::Result<()> {
    if target_len == 0 {
        return Ok(());
    }
    linux_fallocate(file, 0, target_len)?;
    if linux_is_tmpfs(file)? {
        // shmem_fallocate initially leaves newly reserved folios !uptodate so
        // a failed call can roll them back. A second native pass materializes
        // only those still-logically-zero folios, without rewriting live data.
        // This also lets subsequent SEEK_HOLE queries prove allocation without
        // touching contents or timestamps on a cold reader's admission path.
        return linux_fallocate(file, 0, target_len);
    }
    if linux_fiemap_backing(file, target_len)? == LinuxBacking::Shared {
        // Ordinary fallocate leaves existing reflink extents shared. Reserve
        // their private copy now, or a later mapped write could still SIGBUS
        // when CoW needs storage. btrfs has no UNSHARE_RANGE: its EOPNOTSUPP
        // goes back to the caller, which reports the prefix as sparse instead
        // of refusing a database that a snapshot or `cp --reflink` touched.
        linux_fallocate(file, libc::FALLOC_FL_UNSHARE_RANGE, target_len)?;
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn linux_fallocate(file: &File, mode: libc::c_int, target_len: u64) -> io::Result<()> {
    let length = checked_length(target_len)?;
    loop {
        // SAFETY: the descriptor is live, the nonnegative range fits off_t,
        // and mode zero allocates without changing data or shrinking a file.
        // Unlike glibc's posix_fallocate fallback, this never emulates the
        // operation with racy zero writes or a potentially shrinking truncate.
        if unsafe { libc::fallocate(file.as_raw_fd(), mode, 0, length) } == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn platform_ensure_shm_backing(_file: &File, _target_len: u64) -> io::Result<()> {
    // There is no safe general emulation alongside mapped readers/writers.
    // Even FreeBSD's native posix_fallocate may use vop_stdallocate's full
    // buffer read/writeback, which can clobber concurrent mmap stores.
    //
    // macOS is in this group too. Its only route is to write a byte into
    // every allocation unit of the live index, which needs the WAL WRITE lock
    // (so a reader would wait behind, or be refused by, an active writer) and
    // then an F_LOG2PHYS_EXT receipt that APFS does not give until the bytes
    // reach the disk: a fresh `-shm` reported no physical extent in 20 of 20
    // probes on macOS 27 until an fsync or about ten seconds of writeback.
    // Wired in, that refused new WAL databases outright.
    //
    // "Unsupported" is not a refusal: the caller maps sparsely, as releases
    // up to 0.4.10 did. Add a backend proven against live peers before
    // admitting another target here.
    Err(io::Error::from_raw_os_error(libc::EOPNOTSUPP))
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn platform_backing_is_allocated(file: &File, target_len: u64) -> io::Result<bool> {
    // tmpfs has no disk FIEMAP, but its SEEK_HOLE implementation reports
    // actual initialized shmem pages, including swap-backed pages. The native
    // allocator's second pass initializes newly reserved fallocate pages.
    // Do not generalize this proof to filesystems permitted to report every
    // byte as data even when their files contain holes.
    if linux_is_tmpfs(file)? {
        loop {
            // SAFETY: this is a live descriptor and a valid seek operation.
            // SHM I/O is positional, so changing its cursor has no data effect.
            let hole = unsafe { libc::lseek(file.as_raw_fd(), 0, libc::SEEK_HOLE) };
            if hole >= 0 {
                return Ok(u64::try_from(hole).is_ok_and(|hole| hole >= target_len));
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return if allocation_query_unsupported(&error) {
                    Ok(false)
                } else {
                    Err(error)
                };
            }
        }
    }
    Ok(linux_fiemap_backing(file, target_len)? == LinuxBacking::Allocated)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn linux_is_tmpfs(file: &File) -> io::Result<bool> {
    let mut filesystem = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: fstatfs initializes the complete statfs on successful return.
    if unsafe { libc::fstatfs(file.as_raw_fd(), filesystem.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the preceding syscall succeeded.
    Ok(unsafe { filesystem.assume_init() }.f_type == 0x0102_1994)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[derive(Clone, Copy, PartialEq, Eq)]
enum LinuxBacking {
    Allocated,
    Unproven,
    Shared,
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn allocation_query_unsupported(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::ENOTTY | libc::EOPNOTSUPP | libc::ENOSYS | libc::EINVAL)
    )
}

// These targets use the asm-generic ioctl encoding. Unknown Linux ioctl ABIs
// still use native fallocate; they simply do not claim an FIEMAP fast path.
#[cfg(all(
    any(target_os = "linux", target_os = "android"),
    any(
        target_arch = "x86",
        target_arch = "x86_64",
        target_arch = "arm",
        target_arch = "aarch64",
        target_arch = "riscv32",
        target_arch = "riscv64",
        target_arch = "s390x",
        target_arch = "loongarch64"
    )
))]
fn linux_fiemap_backing(file: &File, target_len: u64) -> io::Result<LinuxBacking> {
    #[repr(C)]
    #[derive(Default)]
    struct Extent {
        logical: u64,
        physical: u64,
        length: u64,
        reserved64: [u64; 2],
        flags: u32,
        reserved: [u32; 3],
    }
    #[repr(C)]
    struct Query {
        start: u64,
        length: u64,
        flags: u32,
        mapped_extents: u32,
        extent_count: u32,
        reserved: u32,
        extent: Extent,
    }

    // Linux UAPI linux/fs.h: _IOWR('f', 11, struct fiemap), whose header is
    // 32 bytes. The trailing single extent is accounted for by extent_count.
    // libc types ioctl's request as c_ulong on glibc and uClibc but as c_int
    // on musl, OpenHarmony and Android, so the same 32 bits are spelled twice.
    // With a single c_ulong constant the musl targets, which are the Linux
    // release targets, fail to compile (E0308 at the ioctl call below).
    #[cfg(all(
        target_os = "linux",
        not(any(target_env = "musl", target_env = "ohos"))
    ))]
    const FS_IOC_FIEMAP: libc::c_ulong = 0xC020_660B;
    #[cfg(any(
        target_os = "android",
        all(target_os = "linux", any(target_env = "musl", target_env = "ohos"))
    ))]
    const FS_IOC_FIEMAP: libc::c_int = -1_071_618_549;
    // UNKNOWN, DELALLOC, ENCODED, DATA_ENCRYPTED, DATA_INLINE, and DATA_TAIL
    // do not prove independently writable, allocated file storage.
    // UNWRITTEN is allocated reservation and is deliberately accepted.
    const UNPROVEN_FLAGS: u32 = 0x0000_068E;
    const SHARED: u32 = 0x0000_2000;
    const DELALLOC: u32 = 0x0000_0004;
    const SYNC: u32 = 0x0000_0001;
    let mut cursor = 0;
    let mut allocated = true;
    let mut tried_sync = false;
    let mut sync_next_query = false;
    while cursor < target_len {
        let mut query = Query {
            start: cursor,
            length: target_len - cursor,
            flags: if sync_next_query { SYNC } else { 0 },
            mapped_extents: 0,
            extent_count: 1,
            reserved: 0,
            extent: Extent::default(),
        };
        loop {
            // SAFETY: Query has the exact Linux fiemap header layout followed
            // by storage for the one extent advertised in extent_count.
            if unsafe { libc::ioctl(file.as_raw_fd(), FS_IOC_FIEMAP, &raw mut query) } == 0 {
                break;
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return if allocation_query_unsupported(&error) {
                    Ok(LinuxBacking::Unproven)
                } else {
                    Err(error)
                };
            }
        }
        sync_next_query = false;
        let extent = query.extent;
        let Some(end) = extent.logical.checked_add(extent.length) else {
            return Ok(LinuxBacking::Unproven);
        };
        if query.mapped_extents != 1 || end <= cursor {
            return Ok(LinuxBacking::Unproven);
        }
        if extent.flags & SHARED != 0 {
            return Ok(LinuxBacking::Shared);
        }
        if extent.flags & DELALLOC != 0 && !tried_sync {
            // Ordinary dirty stock-SQLite SHM can be fully populated while
            // its physical allocation is still delayed. fiemap_prep handles
            // SYNC with filemap_write_and_wait, preserving logical contents
            // and mtime/ctime while surfacing writeback allocation failures.
            // Retry this extent once, avoiding an unnecessary fallocate that
            // would mutate timestamps on a cold reader's admission path.
            tried_sync = true;
            sync_next_query = true;
            continue;
        }
        // Keep scanning past holes and uncertain extents: a later shared
        // extent still requires UNSHARE_RANGE before writable publication.
        allocated &= extent.logical <= cursor && extent.flags & UNPROVEN_FLAGS == 0;
        cursor = end;
    }
    Ok(if allocated {
        LinuxBacking::Allocated
    } else {
        LinuxBacking::Unproven
    })
}

#[cfg(all(
    any(target_os = "linux", target_os = "android"),
    not(any(
        target_arch = "x86",
        target_arch = "x86_64",
        target_arch = "arm",
        target_arch = "aarch64",
        target_arch = "riscv32",
        target_arch = "riscv64",
        target_arch = "s390x",
        target_arch = "loongarch64"
    ))
))]
fn linux_fiemap_backing(_file: &File, _target_len: u64) -> io::Result<LinuxBacking> {
    Ok(LinuxBacking::Unproven)
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
// Same signature as the Linux probe, which does fail.
#[allow(clippy::unnecessary_wraps)]
fn platform_backing_is_allocated(_file: &File, _target_len: u64) -> io::Result<bool> {
    // No allocator, so no receipt. On macOS an F_LOG2PHYS_EXT query was tried
    // as the receipt and removed: it reports no physical extent for freshly
    // written bytes until they are flushed (see platform_ensure_shm_backing).
    Ok(false)
}

// The byte-per-allocation-unit materializer is not wired into any target. It
// stays, with its tests, as the starting point for a macOS backend; see
// platform_ensure_shm_backing for what such a backend still has to solve.
#[cfg(test)]
fn materialize_shm_pages(file: &File, target_len: u64) -> io::Result<()> {
    materialize_shm_pages_with(file, target_len, |byte, offset| file.write_at(byte, offset))
}

#[cfg(test)]
fn materialize_shm_pages_with(
    file: &File,
    target_len: u64,
    mut write_at: impl FnMut(&[u8], u64) -> io::Result<usize>,
) -> io::Result<()> {
    // The first existing native reader/checkpoint fields end before byte 511.
    // Do not rewrite a whole page: WRITE does not exclude reader-mark stores.
    const ALLOCATION_UNIT: u64 = 512;
    if target_len != 0 && target_len < ALLOCATION_UNIT {
        // A short final unit would put its tail inside the mutable header.
        // Refuse without writing, rather than clobber a concurrent read mark.
        return Err(io::Error::from_raw_os_error(libc::EINVAL));
    }
    let mut start = 0_u64;
    while start < target_len {
        let end = start.saturating_add(ALLOCATION_UNIT).min(target_len);
        let offset = end - 1;
        let mut byte = [0_u8];
        loop {
            match file.read_at(&mut byte, offset) {
                Ok(_) => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        loop {
            match write_at(&byte, offset) {
                Ok(1) => break,
                Ok(_) => return Err(io::Error::from(io::ErrorKind::WriteZero)),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        start = end;
    }
    Ok(())
}

#[cfg(test)]
#[derive(Clone, Copy)]
struct AllocationFault {
    errno: i32,
    allocated_prefix: Option<u64>,
}

#[cfg(test)]
thread_local! {
    static ALLOCATION_FAULT: std::cell::Cell<Option<AllocationFault>> = const {
        std::cell::Cell::new(None)
    };
}

#[cfg(test)]
pub struct AllocationFaultGuard(Option<AllocationFault>);

#[cfg(test)]
impl Drop for AllocationFaultGuard {
    fn drop(&mut self) {
        ALLOCATION_FAULT.with(|fault| fault.set(self.0));
    }
}

/// Inject a repeatable allocation failure only on this test's current thread.
/// An optional prefix is really allocated before leaving a target-sized sparse
/// remainder, so VFS-level tests can exercise retry and alias preservation.
#[cfg(test)]
pub fn fail_allocations(errno: i32, allocated_prefix: Option<u64>) -> AllocationFaultGuard {
    AllocationFaultGuard(ALLOCATION_FAULT.with(|fault| {
        fault.replace(Some(AllocationFault {
            errno,
            allocated_prefix,
        }))
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    fn tmpfs_fixture() -> File {
        use std::os::fd::FromRawFd as _;

        // SAFETY: a valid name and CLOEXEC request one small private fixture.
        let descriptor =
            unsafe { libc::memfd_create(c"fsqlite-shm-proof".as_ptr(), libc::MFD_CLOEXEC) };
        assert_ne!(
            descriptor,
            -1,
            "memfd_create: {}",
            io::Error::last_os_error()
        );
        // SAFETY: the successful syscall returned a new owned descriptor.
        unsafe { File::from_raw_fd(descriptor) }
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn allocation_metadata(file: &File) -> (u64, i64, i64, i64, i64) {
        use std::os::unix::fs::MetadataExt as _;

        let metadata = file.metadata().unwrap();
        (
            metadata.len(),
            metadata.mtime(),
            metadata.mtime_nsec(),
            metadata.ctime(),
            metadata.ctime_nsec(),
        )
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    fn shm_allocation_proof_preserves_bytes_length_and_timestamps() {
        let file = tempfile::tempfile().unwrap();
        assert!(!shm_backing_is_allocated(&file, 32_768).unwrap());
        file.set_len(32_768).unwrap();
        assert!(!shm_backing_is_allocated(&file, 32_768).unwrap());
        file.write_at(b"wal-index sentinel", 0).unwrap();
        assert!(!shm_backing_is_allocated(&file, 32_768).unwrap());
        ensure_shm_backing(&file, 32_768).unwrap();
        // No reads or writes between native allocation and its first receipt.
        assert!(shm_backing_is_allocated(&file, 32_768).unwrap());
        let metadata = allocation_metadata(&file);
        let mut before = [0_u8; 18];
        file.read_exact_at(&mut before, 0).unwrap();
        for _ in 0..3 {
            assert!(shm_backing_is_allocated(&file, 32_768).unwrap());
            assert!(shm_backing_is_allocated(&file, 4096).unwrap());
            assert_eq!(allocation_metadata(&file), metadata);
        }
        let mut after = [0_u8; 18];
        file.read_exact_at(&mut after, 0).unwrap();
        assert_eq!(before, after);
        let error = shm_backing_is_allocated(&file, u64::MAX).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EFBIG));
        assert_eq!(allocation_metadata(&file), metadata);
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    fn shm_dirty_file_proof_preserves_bytes_length_and_timestamps() {
        let file = tempfile::tempfile().unwrap();
        let before = vec![0x5A; 32_768];
        // Deliberately omit allocation/fsync: this mirrors an ordinary dirty
        // index published by a stock SQLite writer before our first mapping.
        file.write_all_at(&before, 0).unwrap();
        let metadata = allocation_metadata(&file);
        assert!(shm_backing_is_allocated(&file, 32_768).unwrap());
        assert_eq!(allocation_metadata(&file), metadata);
        let mut after = vec![0; before.len()];
        file.read_exact_at(&mut after, 0).unwrap();
        assert_eq!(after, before);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn shm_tmpfs_proof_distinguishes_sparse_partial_and_reserved_storage() {
        use std::os::unix::fs::MetadataExt as _;

        let file = tmpfs_fixture();
        file.set_len(32_768).unwrap();
        assert!(!shm_backing_is_allocated(&file, 32_768).unwrap());
        // Reserve as much storage as the requested prefix, but beyond EOF.
        // st_blocks is therefore sufficient numerically and still no proof.
        // SAFETY: the live private descriptor and small checked range are valid.
        assert_eq!(
            unsafe { libc::fallocate(file.as_raw_fd(), libc::FALLOC_FL_KEEP_SIZE, 32_768, 32_768) },
            0
        );
        assert!(file.metadata().unwrap().blocks() * 512 >= 32_768);
        assert_eq!(file.metadata().unwrap().len(), 32_768);
        assert!(!shm_backing_is_allocated(&file, 32_768).unwrap());
        file.write_at(b"retained", 0).unwrap();
        assert!(!shm_backing_is_allocated(&file, 32_768).unwrap());
        ensure_shm_backing(&file, 32_768).unwrap();
        // The second native pass must make its receipt available immediately,
        // without requiring a caller to touch a potentially unsafe mapping.
        assert!(shm_backing_is_allocated(&file, 32_768).unwrap());
        let metadata = allocation_metadata(&file);
        for _ in 0..3 {
            assert!(shm_backing_is_allocated(&file, 32_768).unwrap());
            assert_eq!(allocation_metadata(&file), metadata);
        }
        let mut bytes = [0_u8; 8];
        file.read_exact_at(&mut bytes, 0).unwrap();
        assert_eq!(&bytes, b"retained");

        let new_file = tmpfs_fixture();
        ensure_shm_backing(&new_file, 32_768).unwrap();
        assert!(shm_backing_is_allocated(&new_file, 32_768).unwrap());
    }

    #[test]
    fn shm_allocation_preserves_sparse_file_bytes_and_larger_extent() {
        let file = tempfile::tempfile().unwrap();
        file.set_len(12_288).unwrap();
        file.write_at(b"wal-index sentinel", 0).unwrap();
        file.write_at(&[0xA5], 4095).unwrap();
        file.write_at(&[0x5A], 8191).unwrap();
        let mut before = vec![0; 12_288];
        file.read_exact_at(&mut before, 0).unwrap();
        ensure_shm_backing(&file, 12_288).unwrap();
        ensure_shm_backing(&file, 4096).unwrap();
        assert_eq!(file.metadata().unwrap().len(), 12_288);
        let mut after = vec![0; 12_288];
        file.read_exact_at(&mut after, 0).unwrap();
        assert_eq!(after, before);
    }

    #[test]
    fn shm_allocation_fault_keeps_errno_and_can_recover() {
        for errno in [libc::ENOSPC, libc::EDQUOT, libc::EIO, libc::EFBIG] {
            let file = tempfile::tempfile().unwrap();
            file.write_at(b"retained", 0).unwrap();
            {
                let _fault = fail_allocations(errno, Some(4096));
                for _ in 0..2 {
                    let error = ensure_shm_backing(&file, 12_288).unwrap_err();
                    assert_eq!(error.raw_os_error(), Some(errno));
                    assert_eq!(file.metadata().unwrap().len(), 12_288);
                }
            }
            ensure_shm_backing(&file, 12_288).unwrap();
            let mut bytes = [0_u8; 8];
            file.read_exact_at(&mut bytes, 0).unwrap();
            assert_eq!(&bytes, b"retained");
        }
    }

    /// A filesystem, descriptor or platform that cannot preallocate must not
    /// refuse the mapping: the allocator reports the prefix as sparse, a
    /// mapping site then extends the file as releases up to 0.4.10 did, and an
    /// existing payload is left exactly as it was.
    #[test]
    fn shm_unsupported_allocation_falls_back_to_a_sparse_extension() {
        for errno in [
            libc::EOPNOTSUPP,
            libc::ENOSYS,
            libc::EINVAL,
            libc::EBADF,
            libc::EPERM,
        ] {
            let file = tempfile::tempfile().unwrap();
            file.write_at(b"retained", 0).unwrap();
            let _fault = fail_allocations(errno, None);

            assert_eq!(
                ensure_shm_backing(&file, 12_288).unwrap(),
                ShmBacking::Sparse,
                "errno {errno} is not a storage failure"
            );
            assert_eq!(file.metadata().unwrap().len(), 8);

            assert_eq!(
                ensure_existing_shm_backing(&file, 12_288).unwrap(),
                ShmBacking::Sparse
            );
            assert_eq!(
                file.metadata().unwrap().len(),
                8,
                "the fallback never resizes an existing payload"
            );

            assert_eq!(
                ensure_shm_backing_or_sparse(&file, 12_288).unwrap(),
                ShmBacking::Sparse
            );
            assert_eq!(file.metadata().unwrap().len(), 12_288);
            // A shorter request never shrinks the file, whatever it proves.
            ensure_shm_backing_or_sparse(&file, 4096).unwrap();
            assert_eq!(file.metadata().unwrap().len(), 12_288);

            let mut bytes = [0_u8; 8];
            file.read_exact_at(&mut bytes, 0).unwrap();
            assert_eq!(&bytes, b"retained");
        }
    }

    /// The fallback is only for "cannot preallocate". A storage failure stays
    /// an error at every entry point and leaves the file alone.
    #[test]
    fn shm_storage_failure_is_never_downgraded_to_the_sparse_fallback() {
        for errno in [libc::ENOSPC, libc::EDQUOT, libc::EIO, libc::EFBIG] {
            let file = tempfile::tempfile().unwrap();
            file.write_at(b"retained", 0).unwrap();
            let _fault = fail_allocations(errno, None);
            for attempt in [
                ensure_shm_backing(&file, 12_288),
                ensure_shm_backing_or_sparse(&file, 12_288),
                ensure_existing_shm_backing(&file, 12_288),
            ] {
                assert_eq!(attempt.unwrap_err().raw_os_error(), Some(errno));
                assert_eq!(file.metadata().unwrap().len(), 8);
            }
        }
    }

    #[test]
    fn shm_byte_allocation_retries_interruption_and_partial_progress() {
        let file = tempfile::tempfile().unwrap();
        file.write_at(b"preserve", 0).unwrap();
        let mut calls = 0;
        let error = materialize_shm_pages_with(&file, 4096, |byte, offset| {
            calls += 1;
            match calls {
                1 => Err(io::Error::from(io::ErrorKind::Interrupted)),
                4 => Err(io::Error::from_raw_os_error(libc::ENOSPC)),
                _ => file.write_at(byte, offset),
            }
        })
        .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::ENOSPC));
        assert_eq!(file.metadata().unwrap().len(), 1024);
        materialize_shm_pages(&file, 4096).unwrap();
        assert_eq!(file.metadata().unwrap().len(), 4096);
        let mut bytes = [0_u8; 8];
        file.read_exact_at(&mut bytes, 0).unwrap();
        assert_eq!(&bytes, b"preserve");
        let error = materialize_shm_pages_with(&file, 4096, |_, _| Ok(0)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WriteZero);
    }

    #[test]
    fn shm_byte_allocation_refuses_a_tail_in_the_mutable_header() {
        let file = tempfile::tempfile().unwrap();
        file.write_at(b"reader marks", 0).unwrap();
        let mut writes = 0;
        let error = materialize_shm_pages_with(&file, 128, |_, _| {
            writes += 1;
            Ok(1)
        })
        .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
        assert_eq!(writes, 0);
        assert_eq!(file.metadata().unwrap().len(), 12);
        let mut bytes = [0_u8; 12];
        file.read_exact_at(&mut bytes, 0).unwrap();
        assert_eq!(&bytes, b"reader marks");
    }

    #[test]
    fn shm_allocation_rejects_unrepresentable_extent_without_mutation() {
        let file = tempfile::tempfile().unwrap();
        let error = ensure_shm_backing(&file, u64::MAX).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EFBIG));
        assert_eq!(file.metadata().unwrap().len(), 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn shm_allocation_reports_real_enospc_on_empty_hugepage_pool() {
        use std::os::fd::FromRawFd as _;

        const CHILD: &str = "FSQLITE_SHM_HUGETLB_ALLOC_CHILD";
        const HUGE_PAGE: u64 = 2 * 1024 * 1024;
        if std::env::var_os(CHILD).is_none() {
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "shm_allocation_reports_real_enospc_on_empty_hugepage_pool",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
            loop {
                if child.try_wait().unwrap().is_some() {
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("hugetlb allocation child exceeded its 15-second deadline");
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            let output = child.wait_with_output().unwrap();
            let transcript = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                output.status.success(),
                "hugetlb child failed: {transcript}"
            );
            assert!(
                transcript.contains("REAL_HUGETLB_ENOSPC")
                    || transcript.contains("SKIP_HUGETLB_ENOSPC"),
                "the child must execute its allocation body: {transcript}"
            );
            eprintln!("{transcript}");
            return;
        }

        let read_count = |name: &str| {
            std::fs::read_to_string(format!("/sys/kernel/mm/hugepages/hugepages-2048kB/{name}"))
                .ok()?
                .trim()
                .parse::<u64>()
                .ok()
        };
        let Some((free, reserved)) = read_count("free_hugepages").zip(read_count("resv_hugepages"))
        else {
            eprintln!("SKIP_HUGETLB_ENOSPC: 2 MiB hugepage accounting unavailable");
            return;
        };
        if free > reserved {
            eprintln!("SKIP_HUGETLB_ENOSPC: the existing hugepage pool has capacity");
            return;
        }

        // SAFETY: this valid NUL-terminated name and supported flag combination
        // request one bounded, anonymous hugetlb fixture; no mount is changed.
        let descriptor = unsafe {
            libc::memfd_create(
                c"fsqlite-shm-enospc".as_ptr(),
                libc::MFD_CLOEXEC | libc::MFD_HUGETLB | libc::MFD_HUGE_2MB,
            )
        };
        if descriptor == -1 {
            let error = io::Error::last_os_error();
            if matches!(
                error.raw_os_error(),
                Some(libc::ENOSYS | libc::EINVAL | libc::EPERM | libc::EACCES)
            ) {
                eprintln!("SKIP_HUGETLB_ENOSPC: memfd fixture unavailable: {error}");
                return;
            }
            panic!("hugetlb memfd failed unexpectedly: {error}");
        }
        // SAFETY: the successful syscall returned a new owned descriptor.
        let file = unsafe { File::from_raw_fd(descriptor) };
        file.set_len(HUGE_PAGE).unwrap();
        let Err(error) = ensure_shm_backing(&file, HUGE_PAGE) else {
            // A surplus/overcommit policy or a concurrently returned page
            // can add capacity after the accounting check. This bounded
            // fixture must not fail a healthy host for having space.
            eprintln!("SKIP_HUGETLB_ENOSPC: one hugepage became available");
            return;
        };
        assert_eq!(error.raw_os_error(), Some(libc::ENOSPC));
        assert_eq!(file.metadata().unwrap().len(), HUGE_PAGE);
        eprintln!("REAL_HUGETLB_ENOSPC: native fallocate refused one 2 MiB page");
    }
}
