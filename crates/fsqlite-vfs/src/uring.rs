//! Linux `io_uring`-backed VFS.
//!
//! This backend preserves Unix lock and SHM semantics by delegating lock/SHM
//! operations to [`UnixFile`]. Data-path read/write can use `io_uring` when it
//! is available at runtime, and transparently falls back to the Unix path when
//! `io_uring` initialization fails.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

#[cfg(all(feature = "linux-uring-fs", not(feature = "linux-asupersync-uring")))]
use std::sync::Mutex;

#[cfg(feature = "linux-asupersync-uring")]
use asupersync::fs::IoUringFile as AsupersyncIoUringFile;
#[cfg(all(feature = "linux-uring-fs", not(feature = "linux-asupersync-uring")))]
use std::fs::File;
#[cfg(all(feature = "linux-uring-fs", not(feature = "linux-asupersync-uring")))]
use std::os::fd::AsRawFd;

use fsqlite_error::{FrankenError, Result};
use fsqlite_observability::{
    io_uring_latency_snapshot, record_io_uring_read_latency, record_io_uring_unix_fallback,
    record_io_uring_write_latency,
};
use fsqlite_types::LockLevel;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};
#[cfg(all(feature = "linux-uring-fs", not(feature = "linux-asupersync-uring")))]
use nix::unistd::{Whence, lseek};
use tracing::warn;

use crate::shm::ShmRegion;
use crate::traits::{Vfs, VfsFile};
use crate::unix::{UnixFile, UnixVfs};

#[cfg(feature = "linux-uring-fs")]
compile_error!(
    "legacy `linux-uring-fs` backend is disabled; use `linux-asupersync-uring` (homegrown runtime path)"
);
#[cfg(not(feature = "linux-asupersync-uring"))]
compile_error!("fsqlite-vfs on Linux requires `linux-asupersync-uring`");

#[cfg(all(feature = "linux-uring-fs", not(feature = "linux-asupersync-uring")))]
const IO_URING_LOCK_POISONED_MSG: &str = "io_uring runtime lock poisoned";
const IO_URING_READ_PANICKED_MSG: &str = "io_uring read panicked";
const IO_URING_WRITE_PANICKED_MSG: &str = "io_uring write panicked";
const IO_URING_READ_CONFORMAL_BREACH_MSG: &str = "io_uring read conformal tail breach";
const IO_URING_WRITE_CONFORMAL_BREACH_MSG: &str = "io_uring write conformal tail breach";
const IO_URING_MAX_RW_CHUNK_BYTES: usize = 64 * 1024;
#[cfg(feature = "linux-asupersync-uring")]
const IO_URING_ASUPERSYNC_INIT_FAILED_MSG: &str = "asupersync io_uring backend init failed";
#[cfg(all(test, feature = "linux-asupersync-uring"))]
static FORCE_ASUPERSYNC_INIT_FAIL: AtomicBool = AtomicBool::new(false);

fn checkpoint_or_abort(cx: &Cx) -> Result<()> {
    cx.checkpoint().map_err(|_| FrankenError::Abort)
}

fn duration_to_micros_saturated(duration: std::time::Duration) -> u64 {
    #[allow(clippy::cast_possible_truncation)] // clamped to u64::MAX first
    {
        duration.as_micros().min(u128::from(u64::MAX)) as u64
    }
}

fn next_chunk_end(total: usize, len: usize) -> usize {
    let remaining = len - total;
    total + remaining.min(IO_URING_MAX_RW_CHUNK_BYTES)
}

fn enforce_conformal_breach_policy(
    runtime: &IoUringRuntime,
    operation: &'static str,
    observed: Duration,
    conformal_upper_bound_us: u64,
    disable_reason: &'static str,
) {
    runtime.disable(disable_reason);
    warn!(
        operation,
        observed_latency_us = duration_to_micros_saturated(observed),
        conformal_upper_bound_us,
        "io_uring latency exceeded conformal upper bound; backend disabled and unix path will be used"
    );
}

#[cfg(all(feature = "linux-uring-fs", not(feature = "linux-asupersync-uring")))]
fn seek_to(file: &File, offset: u64) -> Result<()> {
    let off = i64::try_from(offset).map_err(|_| {
        FrankenError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("io_uring offset too large: {offset}"),
        ))
    })?;
    lseek(file.as_raw_fd(), off, Whence::SeekSet).map_err(|err| FrankenError::Io(err.into()))?;
    Ok(())
}

#[cfg(all(feature = "linux-uring-fs", not(feature = "linux-asupersync-uring")))]
fn current_offset(file: &File) -> Result<u64> {
    let off =
        lseek(file.as_raw_fd(), 0, Whence::SeekCur).map_err(|err| FrankenError::Io(err.into()))?;
    u64::try_from(off).map_err(|_| {
        FrankenError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("negative seek position returned by kernel: {off}"),
        ))
    })
}

#[cfg(all(feature = "linux-uring-fs", not(feature = "linux-asupersync-uring")))]
fn lock_mutex_or_io<T>(mutex: &Mutex<T>) -> io::Result<std::sync::MutexGuard<'_, T>> {
    mutex
        .lock()
        .map_err(|_| io::Error::other(IO_URING_LOCK_POISONED_MSG))
}

#[cfg(all(feature = "linux-uring-fs", not(feature = "linux-asupersync-uring")))]
fn is_lock_poison_error(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::Other && err.to_string() == IO_URING_LOCK_POISONED_MSG
}

#[cfg(feature = "linux-asupersync-uring")]
fn open_asupersync_backend(path: &Path, flags: VfsOpenFlags) -> io::Result<AsupersyncIoUringFile> {
    #[cfg(test)]
    if FORCE_ASUPERSYNC_INIT_FAIL.load(Ordering::Acquire) {
        return Err(io::Error::other("forced asupersync init failure"));
    }

    let open_flags = if flags.contains(VfsOpenFlags::READWRITE) {
        libc::O_RDWR
    } else {
        libc::O_RDONLY
    };
    AsupersyncIoUringFile::open_with_flags(path, open_flags, 0)
}

struct IoUringRuntime {
    #[cfg(all(feature = "linux-uring-fs", not(feature = "linux-asupersync-uring")))]
    ring: Option<Mutex<uring_fs::IoUring>>,
    status: String,
    disabled: AtomicBool,
}

impl fmt::Debug for IoUringRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[cfg(all(feature = "linux-uring-fs", not(feature = "linux-asupersync-uring")))]
        let backend_available = self.ring.is_some();
        #[cfg(feature = "linux-asupersync-uring")]
        let backend_available = true;

        f.debug_struct("IoUringRuntime")
            .field("backend", &Self::backend_name())
            .field("backend_available", &backend_available)
            .field("disabled", &self.disabled.load(Ordering::Relaxed))
            .field("status", &self.status)
            .finish_non_exhaustive()
    }
}

impl IoUringRuntime {
    fn new() -> Self {
        #[cfg(all(feature = "linux-uring-fs", not(feature = "linux-asupersync-uring")))]
        {
            let init_result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(uring_fs::IoUring::new));
            match init_result {
                Ok(Ok(ring)) => Self {
                    ring: Some(Mutex::new(ring)),
                    status: "available:uring-fs".to_owned(),
                    disabled: AtomicBool::new(false),
                },
                Ok(Err(error)) => Self {
                    ring: None,
                    status: format!("unavailable:uring-fs:{error}"),
                    disabled: AtomicBool::new(false),
                },
                Err(_) => Self {
                    ring: None,
                    status: "unavailable:uring-fs:init-panicked".to_owned(),
                    disabled: AtomicBool::new(false),
                },
            }
        }

        #[cfg(feature = "linux-asupersync-uring")]
        {
            Self {
                status: "available:asupersync".to_owned(),
                disabled: AtomicBool::new(false),
            }
        }
    }

    const fn backend_name() -> &'static str {
        #[cfg(all(feature = "linux-uring-fs", not(feature = "linux-asupersync-uring")))]
        {
            "uring-fs"
        }
        #[cfg(feature = "linux-asupersync-uring")]
        {
            "asupersync"
        }
    }

    fn disable(&self, reason: &'static str) {
        if !self.disabled.swap(true, Ordering::AcqRel) {
            warn!(
                backend = Self::backend_name(),
                reason, "io_uring backend disabled; falling back to unix path"
            );
        }
    }

    #[cfg(test)]
    fn is_disabled(&self) -> bool {
        self.disabled.load(Ordering::Acquire)
    }

    fn is_available(&self) -> bool {
        #[cfg(all(feature = "linux-uring-fs", not(feature = "linux-asupersync-uring")))]
        {
            self.ring.is_some() && !self.disabled.load(Ordering::Acquire)
        }
        #[cfg(feature = "linux-asupersync-uring")]
        {
            !self.disabled.load(Ordering::Acquire)
        }
    }
}

/// Linux VFS that prefers `io_uring` for the data path.
#[derive(Debug)]
pub struct IoUringVfs {
    unix: UnixVfs,
    runtime: Arc<IoUringRuntime>,
}

impl IoUringVfs {
    /// Create a new `io_uring` VFS.
    #[must_use]
    pub fn new() -> Self {
        Self {
            unix: UnixVfs::new(),
            runtime: Arc::new(IoUringRuntime::new()),
        }
    }

    /// Returns whether `io_uring` was successfully initialized.
    #[must_use]
    pub fn is_available(&self) -> bool {
        self.runtime.is_available()
    }

    /// Human-readable runtime status.
    #[must_use]
    pub fn status(&self) -> &str {
        &self.runtime.status
    }
}

impl Default for IoUringVfs {
    fn default() -> Self {
        Self::new()
    }
}

/// File handle for [`IoUringVfs`].
#[derive(Debug)]
pub struct IoUringFile {
    inner: UnixFile,
    runtime: Arc<IoUringRuntime>,
    #[cfg(feature = "linux-asupersync-uring")]
    asupersync_backend: Option<AsupersyncIoUringFile>,
}

impl IoUringFile {
    #[cfg(all(feature = "linux-uring-fs", not(feature = "linux-asupersync-uring")))]
    fn read_via_uring(&self, cx: &Cx, buf: &mut [u8], offset: u64) -> Result<usize> {
        let ring_mutex = self.runtime.ring.as_ref().ok_or_else(|| {
            FrankenError::Io(io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring runtime unavailable",
            ))
        })?;

        self.inner.with_inode_io_file(|file| {
            let mut total = 0_usize;
            while total < buf.len() {
                checkpoint_or_abort(cx)?;
                let off = offset
                    .checked_add(u64::try_from(total).expect("usize must fit into u64"))
                    .ok_or_else(|| {
                        FrankenError::Io(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "offset overflow during io_uring read",
                        ))
                    })?;

                let chunk_end = next_chunk_end(total, buf.len());
                let requested = u32::try_from(chunk_end - total).map_err(|_| {
                    FrankenError::Io(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("read size too large for io_uring: {}", chunk_end - total),
                    ))
                })?;

                seek_to(file, off)?;
                let read_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let ring = lock_mutex_or_io(ring_mutex)?;
                    pollster::block_on(ring.read(file, requested))
                }));

                let data = match read_result {
                    Ok(Ok(data)) => data,
                    Ok(Err(err)) => {
                        if is_lock_poison_error(&err) {
                            self.runtime.disable(IO_URING_LOCK_POISONED_MSG);
                        }
                        return Err(FrankenError::Io(err));
                    }
                    Err(_) => {
                        self.runtime.disable(IO_URING_READ_PANICKED_MSG);
                        return Err(FrankenError::Io(io::Error::other(
                            IO_URING_READ_PANICKED_MSG,
                        )));
                    }
                };

                if data.is_empty() {
                    break; // EOF
                }

                let bytes_read = data.len();
                buf[total..total + bytes_read].copy_from_slice(&data);
                total += bytes_read;
            }

            if total < buf.len() {
                buf[total..].fill(0);
            }
            Ok(total)
        })
    }

    #[cfg(feature = "linux-asupersync-uring")]
    fn read_via_uring(&self, cx: &Cx, buf: &mut [u8], offset: u64) -> Result<usize> {
        let backend = self.asupersync_backend.as_ref().ok_or_else(|| {
            FrankenError::Io(io::Error::new(
                io::ErrorKind::Unsupported,
                "asupersync io_uring backend unavailable",
            ))
        })?;

        let mut total = 0_usize;
        while total < buf.len() {
            checkpoint_or_abort(cx)?;
            let chunk_end = next_chunk_end(total, buf.len());
            let off = offset
                .checked_add(u64::try_from(total).expect("usize must fit into u64"))
                .ok_or_else(|| {
                    FrankenError::Io(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "offset overflow during io_uring read",
                    ))
                })?;

            let read_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                pollster::block_on(backend.read_at(&mut buf[total..chunk_end], off))
            }));

            let bytes_read = match read_result {
                Ok(Ok(n)) => n,
                Ok(Err(err)) => return Err(FrankenError::Io(err)),
                Err(_) => {
                    self.runtime.disable(IO_URING_READ_PANICKED_MSG);
                    return Err(FrankenError::Io(io::Error::other(
                        IO_URING_READ_PANICKED_MSG,
                    )));
                }
            };

            if bytes_read == 0 {
                break; // EOF
            }
            total += bytes_read;
        }

        if total < buf.len() {
            buf[total..].fill(0);
        }
        Ok(total)
    }

    #[cfg(all(feature = "linux-uring-fs", not(feature = "linux-asupersync-uring")))]
    fn write_via_uring(&self, cx: &Cx, buf: &[u8], offset: u64) -> Result<()> {
        let ring_mutex = self.runtime.ring.as_ref().ok_or_else(|| {
            FrankenError::Io(io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring runtime unavailable",
            ))
        })?;

        self.inner.with_inode_io_file(|file| {
            let mut total = 0_usize;
            while total < buf.len() {
                checkpoint_or_abort(cx)?;
                let chunk_end = next_chunk_end(total, buf.len());
                let off = offset
                    .checked_add(u64::try_from(total).expect("usize must fit into u64"))
                    .ok_or_else(|| {
                        FrankenError::Io(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "offset overflow during io_uring write",
                        ))
                    })?;
                seek_to(file, off)?;
                let before = current_offset(file)?;
                // uring-fs currently requires owning the payload for submission; chunking
                // bounds this copy size while preserving forward progress semantics.
                let payload = buf[total..chunk_end].to_vec();
                let write_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let ring = lock_mutex_or_io(ring_mutex)?;
                    pollster::block_on(ring.write(file, payload))
                }));
                match write_result {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        if is_lock_poison_error(&err) {
                            self.runtime.disable(IO_URING_LOCK_POISONED_MSG);
                        }
                        return Err(FrankenError::Io(err));
                    }
                    Err(_) => {
                        self.runtime.disable(IO_URING_WRITE_PANICKED_MSG);
                        return Err(FrankenError::Io(io::Error::other(
                            IO_URING_WRITE_PANICKED_MSG,
                        )));
                    }
                }
                let after = current_offset(file)?;
                let advanced_u64 = after.checked_sub(before).ok_or_else(|| {
                    FrankenError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "io_uring write moved cursor backwards: before={before} after={after}"
                        ),
                    ))
                })?;
                let advanced = usize::try_from(advanced_u64).map_err(|_| {
                    FrankenError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("io_uring write advanced too far: {advanced_u64}"),
                    ))
                })?;
                if advanced == 0 {
                    return Err(FrankenError::Io(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "io_uring write advanced by 0 bytes",
                    )));
                }
                let remaining = chunk_end - total;
                total += advanced.min(remaining);
            }
            Ok(())
        })
    }

    #[cfg(feature = "linux-asupersync-uring")]
    fn write_via_uring(&self, cx: &Cx, buf: &[u8], offset: u64) -> Result<()> {
        let backend = self.asupersync_backend.as_ref().ok_or_else(|| {
            FrankenError::Io(io::Error::new(
                io::ErrorKind::Unsupported,
                "asupersync io_uring backend unavailable",
            ))
        })?;

        let mut total = 0_usize;
        while total < buf.len() {
            checkpoint_or_abort(cx)?;
            let chunk_end = next_chunk_end(total, buf.len());
            let off = offset
                .checked_add(u64::try_from(total).expect("usize must fit into u64"))
                .ok_or_else(|| {
                    FrankenError::Io(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "offset overflow during io_uring write",
                    ))
                })?;
            let write_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                pollster::block_on(backend.write_at(&buf[total..chunk_end], off))
            }));
            let advanced: usize = match write_result {
                Ok(Ok(advanced)) => advanced,
                Ok(Err(err)) => return Err(FrankenError::Io(err)),
                Err(_) => {
                    self.runtime.disable(IO_URING_WRITE_PANICKED_MSG);
                    return Err(FrankenError::Io(io::Error::other(
                        IO_URING_WRITE_PANICKED_MSG,
                    )));
                }
            };
            if advanced == 0 {
                return Err(FrankenError::Io(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "io_uring write advanced by 0 bytes",
                )));
            }
            let remaining = chunk_end - total;
            total += advanced.min(remaining);
        }
        Ok(())
    }
}

impl Vfs for IoUringVfs {
    type File = IoUringFile;

    fn name(&self) -> &'static str {
        "io_uring"
    }

    fn open(
        &self,
        cx: &Cx,
        path: Option<&Path>,
        flags: VfsOpenFlags,
    ) -> Result<(Self::File, VfsOpenFlags)> {
        let (file, out_flags) = self.unix.open(cx, path, flags)?;

        #[cfg(feature = "linux-asupersync-uring")]
        let asupersync_backend = if self.runtime.is_available() {
            if let Some(requested_path) = path {
                let full_path = self.unix.full_pathname(cx, requested_path)?;
                match open_asupersync_backend(&full_path, out_flags) {
                    Ok(backend) => Some(backend),
                    Err(err) => {
                        self.runtime.disable(IO_URING_ASUPERSYNC_INIT_FAILED_MSG);
                        warn!(
                            path = %full_path.display(),
                            error = %err,
                            "asupersync io_uring backend init failed; falling back to unix path"
                        );
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        Ok((
            IoUringFile {
                inner: file,
                runtime: Arc::clone(&self.runtime),
                #[cfg(feature = "linux-asupersync-uring")]
                asupersync_backend,
            },
            out_flags,
        ))
    }

    fn delete(&self, cx: &Cx, path: &Path, sync_dir: bool) -> Result<()> {
        self.unix.delete(cx, path, sync_dir)
    }

    fn access(&self, cx: &Cx, path: &Path, flags: AccessFlags) -> Result<bool> {
        self.unix.access(cx, path, flags)
    }

    fn full_pathname(&self, cx: &Cx, path: &Path) -> Result<PathBuf> {
        self.unix.full_pathname(cx, path)
    }

    fn randomness(&self, cx: &Cx, buf: &mut [u8]) {
        self.unix.randomness(cx, buf);
    }

    fn current_time(&self, cx: &Cx) -> f64 {
        self.unix.current_time(cx)
    }
}

impl VfsFile for IoUringFile {
    fn close(&mut self, cx: &Cx) -> Result<()> {
        self.inner.close(cx)
    }

    fn read(&mut self, cx: &Cx, buf: &mut [u8], offset: u64) -> Result<usize> {
        checkpoint_or_abort(cx)?;
        if self.runtime.is_available() {
            let start = Instant::now();
            match self.read_via_uring(cx, buf, offset) {
                Ok(bytes) => {
                    let elapsed = start.elapsed();
                    if record_io_uring_read_latency(elapsed) {
                        let snapshot = io_uring_latency_snapshot();
                        enforce_conformal_breach_policy(
                            &self.runtime,
                            "read",
                            elapsed,
                            snapshot.read_conformal_upper_bound_us,
                            IO_URING_READ_CONFORMAL_BREACH_MSG,
                        );
                    }
                    return Ok(bytes);
                }
                Err(_) => {
                    record_io_uring_unix_fallback();
                }
            }
        }
        self.inner.read(cx, buf, offset)
    }

    fn write(&mut self, cx: &Cx, buf: &[u8], offset: u64) -> Result<()> {
        checkpoint_or_abort(cx)?;
        if self.runtime.is_available() {
            let start = Instant::now();
            match self.write_via_uring(cx, buf, offset) {
                Ok(()) => {
                    let elapsed = start.elapsed();
                    if record_io_uring_write_latency(elapsed) {
                        let snapshot = io_uring_latency_snapshot();
                        enforce_conformal_breach_policy(
                            &self.runtime,
                            "write",
                            elapsed,
                            snapshot.write_conformal_upper_bound_us,
                            IO_URING_WRITE_CONFORMAL_BREACH_MSG,
                        );
                    }
                    return Ok(());
                }
                Err(_) => {
                    record_io_uring_unix_fallback();
                }
            }
        }
        self.inner.write(cx, buf, offset)
    }

    fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
        self.inner.truncate(cx, size)
    }

    fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
        self.inner.sync(cx, flags)
    }

    fn file_size(&self, cx: &Cx) -> Result<u64> {
        self.inner.file_size(cx)
    }

    fn lock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
        self.inner.lock(cx, level)
    }

    fn unlock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
        self.inner.unlock(cx, level)
    }

    fn check_reserved_lock(&self, cx: &Cx) -> Result<bool> {
        self.inner.check_reserved_lock(cx)
    }

    fn sector_size(&self) -> u32 {
        self.inner.sector_size()
    }

    fn device_characteristics(&self) -> u32 {
        self.inner.device_characteristics()
    }

    fn shm_map(&mut self, cx: &Cx, region: u32, size: u32, extend: bool) -> Result<ShmRegion> {
        self.inner.shm_map(cx, region, size, extend)
    }

    fn shm_lock(&mut self, cx: &Cx, offset: u32, n: u32, flags: u32) -> Result<()> {
        self.inner.shm_lock(cx, offset, n, flags)
    }

    fn shm_barrier(&self) {
        self.inner.shm_barrier();
    }

    fn shm_unmap(&mut self, cx: &Cx, delete: bool) -> Result<()> {
        self.inner.shm_unmap(cx, delete)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use fsqlite_observability::{io_uring_latency_snapshot, reset_io_uring_latency_metrics};
    use fsqlite_types::flags::VfsOpenFlags;

    fn open_flags_create() -> VfsOpenFlags {
        VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE
    }

    #[test]
    fn test_io_uring_vfs_name_and_status() {
        let vfs = IoUringVfs::new();
        assert_eq!(vfs.name(), "io_uring");
        assert!(!vfs.status().is_empty());
    }

    #[test]
    fn test_io_uring_vfs_roundtrip_write_read() {
        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("uring_roundtrip.db");

        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open should succeed");
        file.write(&cx, b"hello io_uring", 0)
            .expect("write should succeed");

        let mut buf = [0_u8; 14];
        let n = file.read(&cx, &mut buf, 0).expect("read should succeed");
        assert_eq!(n, 14);
        assert_eq!(&buf, b"hello io_uring");
        file.close(&cx).expect("close should succeed");
    }

    #[test]
    fn test_io_uring_paths_emit_latency_or_fallback_metrics() {
        reset_io_uring_latency_metrics();

        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("uring_metrics.db");

        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open should succeed");
        file.write(&cx, b"metrics", 0)
            .expect("write should succeed");

        let mut buf = [0_u8; 7];
        let _ = file.read(&cx, &mut buf, 0).expect("read should succeed");

        let snapshot = io_uring_latency_snapshot();
        if vfs.is_available() {
            assert!(
                snapshot.write_samples_total >= 1 || snapshot.unix_fallbacks_total >= 1,
                "write path should either record io_uring latency or fallback"
            );
            assert!(
                snapshot.read_samples_total >= 1 || snapshot.unix_fallbacks_total >= 1,
                "read path should either record io_uring latency or fallback"
            );
        }
    }

    #[test]
    fn test_runtime_disable_is_sticky() {
        let runtime = IoUringRuntime::new();
        assert!(!runtime.is_disabled());
        runtime.disable("test disable");
        assert!(runtime.is_disabled());
        runtime.disable("test disable again");
        assert!(runtime.is_disabled());
    }

    #[test]
    fn test_conformal_breach_policy_disables_runtime() {
        let runtime = IoUringRuntime::new();
        assert!(!runtime.is_disabled());

        enforce_conformal_breach_policy(
            &runtime,
            "read",
            Duration::from_micros(250),
            100,
            IO_URING_READ_CONFORMAL_BREACH_MSG,
        );

        assert!(runtime.is_disabled());
    }

    #[cfg(all(feature = "linux-uring-fs", not(feature = "linux-asupersync-uring")))]
    #[test]
    fn test_lock_mutex_or_io_handles_poison_without_panicking() {
        let mutex = Mutex::new(7_u8);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = mutex.lock().unwrap_or_else(|e| e.into_inner());
            panic!("poison mutex");
        }));
        let err = lock_mutex_or_io(&mutex).expect_err("lock should fail");
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert_eq!(err.to_string(), IO_URING_LOCK_POISONED_MSG);
    }

    #[cfg(all(feature = "linux-uring-fs", not(feature = "linux-asupersync-uring")))]
    #[test]
    fn test_poisoned_runtime_falls_back_to_unix_path_and_disables_backend() {
        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        if !vfs.is_available() {
            return;
        }
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("uring_poison_fallback.db");
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open should succeed");

        if let Some(ring_mutex) = file.runtime.ring.as_ref() {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _guard = ring_mutex.lock().unwrap_or_else(|e| e.into_inner());
                panic!("poison io_uring runtime lock");
            }));
        }

        file.write(&cx, b"fallback", 0)
            .expect("write should fall back and succeed");
        let mut buf = [0_u8; 8];
        let n = file
            .read(&cx, &mut buf, 0)
            .expect("read should fall back and succeed");
        assert_eq!(n, 8);
        assert_eq!(&buf, b"fallback");
        assert!(vfs.runtime.is_disabled());
        assert!(!vfs.is_available());
    }

    #[cfg(feature = "linux-asupersync-uring")]
    #[test]
    fn test_asupersync_init_failure_disables_backend_and_falls_back() {
        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("asupersync_forced_init_failure.db");

        FORCE_ASUPERSYNC_INIT_FAIL.store(true, Ordering::Release);
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open should succeed via unix fallback");
        FORCE_ASUPERSYNC_INIT_FAIL.store(false, Ordering::Release);

        assert!(vfs.runtime.is_disabled());
        assert!(!vfs.is_available());

        file.write(&cx, b"fallback", 0)
            .expect("write should succeed via unix fallback");
        let mut buf = [0_u8; 8];
        let n = file
            .read(&cx, &mut buf, 0)
            .expect("read should succeed via unix fallback");
        assert_eq!(n, 8);
        assert_eq!(&buf, b"fallback");
    }
}
