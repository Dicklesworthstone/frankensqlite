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
use std::sync::OnceLock;
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
    io_uring_latency_snapshot, record_io_uring_read_latency, record_io_uring_read_unix_fallback,
    record_io_uring_write_latency, record_io_uring_write_unix_fallback,
};
use fsqlite_types::LockLevel;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};
#[cfg(all(feature = "linux-uring-fs", not(feature = "linux-asupersync-uring")))]
use nix::unistd::{Whence, lseek};
use tracing::{info, warn};

use crate::shm::ShmRegion;
use crate::traits::{FileIdentity, Vfs, VfsFile};
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
const IO_URING_READ_ERROR_FALLBACK_MSG: &str = "io_uring read error fallback";
const IO_URING_WRITE_ERROR_FALLBACK_MSG: &str = "io_uring write error fallback";
const IO_URING_READ_CONFORMAL_BREACH_MSG: &str = "io_uring read conformal tail breach";
const IO_URING_WRITE_CONFORMAL_BREACH_MSG: &str = "io_uring write conformal tail breach";
const IO_URING_MAX_RW_CHUNK_BYTES: usize = 64 * 1024;
#[cfg(feature = "linux-asupersync-uring")]
const IO_URING_ASUPERSYNC_INIT_FAILED_MSG: &str = "asupersync io_uring backend init failed";
#[cfg(all(test, feature = "linux-asupersync-uring"))]
static FORCE_ASUPERSYNC_INIT_FAIL: AtomicBool = AtomicBool::new(false);
#[cfg(all(test, feature = "linux-asupersync-uring"))]
static FORCE_ASUPERSYNC_READ_FAIL: AtomicBool = AtomicBool::new(false);
#[cfg(all(test, feature = "linux-asupersync-uring"))]
static FORCE_ASUPERSYNC_READ_ABORT: AtomicBool = AtomicBool::new(false);
#[cfg(all(test, feature = "linux-asupersync-uring"))]
static FORCE_ASUPERSYNC_WRITE_FAIL: AtomicBool = AtomicBool::new(false);
#[cfg(all(test, feature = "linux-asupersync-uring"))]
static FORCE_ASUPERSYNC_WRITE_ABORT: AtomicBool = AtomicBool::new(false);

fn checkpoint_or_abort(cx: &Cx) -> Result<()> {
    cx.checkpoint().map_err(|_| FrankenError::Abort)
}

fn should_fallback_to_unix_on_uring_error(err: &FrankenError) -> bool {
    match err {
        FrankenError::Abort => false,
        FrankenError::Io(io_err) if io_err.kind() == io::ErrorKind::InvalidInput => false,
        _ => true,
    }
}

fn should_disable_runtime_on_uring_fallback(err: &FrankenError) -> bool {
    match err {
        FrankenError::Abort => false,
        FrankenError::Io(io_err)
            if matches!(
                io_err.kind(),
                io::ErrorKind::Unsupported | io::ErrorKind::InvalidInput
            ) =>
        {
            false
        }
        _ => true,
    }
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

/// Returns whether this open participates in SQLite's lock-sensitive fd topology.
///
/// `UnixVfs` preserves SQLite/FrankenSQLite locking semantics by ensuring the
/// process keeps a single canonical fd per inode. The asupersync io_uring path
/// opens an independent fd, which is not safe for these files because closing
/// any same-inode fd can perturb process-scoped POSIX locks.
fn uses_sqlite_lock_sensitive_fd_topology(flags: VfsOpenFlags) -> bool {
    flags.intersects(
        VfsOpenFlags::MAIN_DB
            | VfsOpenFlags::MAIN_JOURNAL
            | VfsOpenFlags::TEMP_DB
            | VfsOpenFlags::TEMP_JOURNAL
            | VfsOpenFlags::SUBJOURNAL
            | VfsOpenFlags::SUPER_JOURNAL
            | VfsOpenFlags::WAL
            | VfsOpenFlags::DELETEONCLOSE,
    )
}

fn enforce_conformal_breach_policy(
    runtime: &IoUringRuntime,
    operation: &'static str,
    observed: Duration,
    conformal_upper_bound_us: u64,
    disable_reason: &'static str,
) {
    runtime.disable(disable_reason);
    info!(
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
    initial_status: String,
    disabled: AtomicBool,
    disable_reason: OnceLock<&'static str>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IoUringRuntimeStatus {
    pub backend: &'static str,
    pub available: bool,
    pub disabled: bool,
    pub initial_status: String,
    pub status: String,
    pub disable_reason: Option<&'static str>,
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
            .field("status", &self.status())
            .field("disable_reason", &self.disable_reason())
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
                    initial_status: "available:uring-fs".to_owned(),
                    disabled: AtomicBool::new(false),
                    disable_reason: OnceLock::new(),
                },
                Ok(Err(error)) => Self {
                    ring: None,
                    initial_status: format!("unavailable:uring-fs:{error}"),
                    disabled: AtomicBool::new(false),
                    disable_reason: OnceLock::new(),
                },
                Err(_) => Self {
                    ring: None,
                    initial_status: "unavailable:uring-fs:init-panicked".to_owned(),
                    disabled: AtomicBool::new(false),
                    disable_reason: OnceLock::new(),
                },
            }
        }

        #[cfg(feature = "linux-asupersync-uring")]
        {
            Self {
                initial_status: "available:asupersync".to_owned(),
                disabled: AtomicBool::new(false),
                disable_reason: OnceLock::new(),
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
            let _ = self.disable_reason.set(reason);
            if matches!(
                reason,
                IO_URING_READ_CONFORMAL_BREACH_MSG | IO_URING_WRITE_CONFORMAL_BREACH_MSG
            ) {
                info!(
                    backend = Self::backend_name(),
                    reason, "io_uring backend disabled; falling back to unix path"
                );
            } else {
                warn!(
                    backend = Self::backend_name(),
                    reason, "io_uring backend disabled; falling back to unix path"
                );
            }
        }
    }

    fn disable_reason(&self) -> Option<&'static str> {
        self.disable_reason.get().copied()
    }

    fn status(&self) -> String {
        match self.disable_reason() {
            Some(reason) => format!("disabled:{}:{reason}", Self::backend_name()),
            None => self.initial_status.clone(),
        }
    }

    fn snapshot(&self) -> IoUringRuntimeStatus {
        IoUringRuntimeStatus {
            backend: Self::backend_name(),
            available: self.is_available(),
            disabled: self.disabled.load(Ordering::Acquire),
            initial_status: self.initial_status.clone(),
            status: self.status(),
            disable_reason: self.disable_reason(),
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
    pub fn status(&self) -> String {
        self.runtime.status()
    }

    /// Runtime status snapshot including disable reason.
    #[must_use]
    pub fn status_snapshot(&self) -> IoUringRuntimeStatus {
        self.runtime.snapshot()
    }

    fn wrap_unix_file(
        &self,
        cx: &Cx,
        path: Option<&Path>,
        requested_flags: VfsOpenFlags,
        file: UnixFile,
        out_flags: VfsOpenFlags,
    ) -> Result<(IoUringFile, VfsOpenFlags)> {
        #[cfg(feature = "linux-asupersync-uring")]
        // Lock-sensitive SQLite opens must stay on the canonical Unix fd.
        // Opening a second same-inode fd for the io_uring backend breaks the
        // invariant that `UnixVfs` relies on to keep process-scoped locks
        // stable across multiple file handles.
        let asupersync_backend = if self.runtime.is_available()
            && path.is_some()
            && !uses_sqlite_lock_sensitive_fd_topology(requested_flags)
        {
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
    #[cfg(feature = "linux-asupersync-uring")]
    fn has_uring_data_path(&self) -> bool {
        self.runtime.is_available() && self.asupersync_backend.is_some()
    }

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

            #[cfg(test)]
            if FORCE_ASUPERSYNC_READ_FAIL.load(Ordering::Acquire) {
                return Err(FrankenError::Io(io::Error::other(
                    "forced asupersync read failure",
                )));
            }
            #[cfg(test)]
            if FORCE_ASUPERSYNC_READ_ABORT.load(Ordering::Acquire) {
                return Err(FrankenError::Abort);
            }

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
            #[cfg(test)]
            if FORCE_ASUPERSYNC_WRITE_FAIL.load(Ordering::Acquire) {
                return Err(FrankenError::Io(io::Error::other(
                    "forced asupersync write failure",
                )));
            }
            #[cfg(test)]
            if FORCE_ASUPERSYNC_WRITE_ABORT.load(Ordering::Acquire) {
                return Err(FrankenError::Abort);
            }
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
        self.wrap_unix_file(cx, path, flags, file, out_flags)
    }

    fn open_with_expected_identity(
        &self,
        cx: &Cx,
        path: &Path,
        flags: VfsOpenFlags,
        expected_identity: FileIdentity,
    ) -> Result<(Self::File, VfsOpenFlags)> {
        let (file, out_flags) =
            self.unix
                .open_with_expected_identity(cx, path, flags, expected_identity)?;
        self.wrap_unix_file(cx, Some(path), flags, file, out_flags)
    }

    fn open_reserved_with_expected_identity(
        &self,
        cx: &Cx,
        path: &Path,
        flags: VfsOpenFlags,
        expected_identity: FileIdentity,
    ) -> Result<(Self::File, VfsOpenFlags)> {
        let (file, out_flags) =
            self.unix
                .open_reserved_with_expected_identity(cx, path, flags, expected_identity)?;
        self.wrap_unix_file(cx, Some(path), flags, file, out_flags)
    }

    fn delete(&self, cx: &Cx, path: &Path, sync_dir: bool) -> Result<()> {
        self.unix.delete(cx, path, sync_dir)
    }

    fn sync_parent_directory(&self, cx: &Cx, path: &Path) -> Result<()> {
        self.unix.sync_parent_directory(cx, path)
    }

    fn access(&self, cx: &Cx, path: &Path, flags: AccessFlags) -> Result<bool> {
        self.unix.access(cx, path, flags)
    }

    fn path_entry_exists(&self, cx: &Cx, path: &Path) -> Result<bool> {
        self.unix.path_entry_exists(cx, path)
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

    fn is_memory(&self) -> bool {
        self.unix.is_memory()
    }
}

impl VfsFile for IoUringFile {
    fn close(&mut self, cx: &Cx) -> Result<()> {
        self.inner.close(cx)
    }

    fn file_identity(&self) -> Result<Option<FileIdentity>> {
        self.inner.file_identity()
    }

    fn read(&self, cx: &Cx, buf: &mut [u8], offset: u64) -> Result<usize> {
        checkpoint_or_abort(cx)?;
        #[cfg(feature = "linux-asupersync-uring")]
        if self.has_uring_data_path() {
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
                Err(err) => {
                    if !should_fallback_to_unix_on_uring_error(&err) {
                        return Err(err);
                    }
                    if should_disable_runtime_on_uring_fallback(&err) {
                        self.runtime.disable(IO_URING_READ_ERROR_FALLBACK_MSG);
                    }
                }
            }
        }
        record_io_uring_read_unix_fallback();
        self.inner.read(cx, buf, offset)
    }

    fn write(&mut self, cx: &Cx, buf: &[u8], offset: u64) -> Result<()> {
        checkpoint_or_abort(cx)?;
        #[cfg(feature = "linux-asupersync-uring")]
        if self.has_uring_data_path() {
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
                Err(err) => {
                    if !should_fallback_to_unix_on_uring_error(&err) {
                        return Err(err);
                    }
                    if should_disable_runtime_on_uring_fallback(&err) {
                        self.runtime.disable(IO_URING_WRITE_ERROR_FALLBACK_MSG);
                    }
                }
            }
        }
        record_io_uring_write_unix_fallback();
        self.inner.write(cx, buf, offset)
    }

    fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
        self.inner.truncate(cx, size)
    }

    fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
        self.inner.sync(cx, flags)
    }

    fn durable_sync(&mut self, cx: &Cx, kind: crate::SyncKind) -> Result<()> {
        self.inner.durable_sync(cx, kind)
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

    fn set_busy_timeout_ms(&mut self, ms: u64) {
        self.inner.set_busy_timeout_ms(ms);
    }
}

#[cfg(feature = "linux-asupersync-uring")]
impl crate::traits::AsyncVfsDataPath for IoUringFile {
    async fn read_async(&self, cx: &Cx, buf: &mut [u8], offset: u64) -> Result<usize> {
        checkpoint_or_abort(cx)?;
        let backend = match &self.asupersync_backend {
            Some(b) if self.runtime.is_available() => b,
            _ => return self.inner.read(cx, buf, offset),
        };

        let start = Instant::now();
        let mut total = 0_usize;
        while total < buf.len() {
            checkpoint_or_abort(cx)?;
            let chunk_end = next_chunk_end(total, buf.len());
            let off = offset
                .checked_add(u64::try_from(total).expect("usize must fit into u64"))
                .ok_or_else(|| {
                    FrankenError::Io(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "offset overflow during async io_uring read",
                    ))
                })?;

            #[cfg(test)]
            if FORCE_ASUPERSYNC_READ_FAIL.load(Ordering::Acquire) {
                return Err(FrankenError::Io(io::Error::other(
                    "forced asupersync read failure",
                )));
            }
            #[cfg(test)]
            if FORCE_ASUPERSYNC_READ_ABORT.load(Ordering::Acquire) {
                return Err(FrankenError::Abort);
            }

            let bytes_read = backend
                .read_at(&mut buf[total..chunk_end], off)
                .await
                .map_err(FrankenError::Io)?;

            if bytes_read == 0 {
                break;
            }
            total += bytes_read;
        }

        if total < buf.len() {
            buf[total..].fill(0);
        }

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
        Ok(total)
    }

    async fn write_async(&self, cx: &Cx, buf: &[u8], offset: u64) -> Result<()> {
        checkpoint_or_abort(cx)?;
        let backend = match &self.asupersync_backend {
            Some(b) if self.runtime.is_available() => b,
            _ => {
                record_io_uring_write_unix_fallback();
                return Err(FrankenError::Unsupported);
            }
        };

        let start = Instant::now();
        let mut total = 0_usize;
        while total < buf.len() {
            checkpoint_or_abort(cx)?;
            let chunk_end = next_chunk_end(total, buf.len());
            let off = offset
                .checked_add(u64::try_from(total).expect("usize must fit into u64"))
                .ok_or_else(|| {
                    FrankenError::Io(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "offset overflow during async io_uring write",
                    ))
                })?;

            #[cfg(test)]
            if FORCE_ASUPERSYNC_WRITE_FAIL.load(Ordering::Acquire) {
                return Err(FrankenError::Io(io::Error::other(
                    "forced asupersync write failure",
                )));
            }
            #[cfg(test)]
            if FORCE_ASUPERSYNC_WRITE_ABORT.load(Ordering::Acquire) {
                return Err(FrankenError::Abort);
            }

            let advanced = backend
                .write_at(&buf[total..chunk_end], off)
                .await
                .map_err(FrankenError::Io)?;

            if advanced == 0 {
                return Err(FrankenError::Io(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "async io_uring write advanced by 0 bytes",
                )));
            }
            let remaining = chunk_end - total;
            total += advanced.min(remaining);
        }

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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use fsqlite_observability::{io_uring_latency_snapshot, reset_io_uring_latency_metrics};
    use fsqlite_types::flags::VfsOpenFlags;
    use std::sync::{Mutex as StdMutex, MutexGuard as StdMutexGuard};

    static IO_URING_TEST_LOCK: StdMutex<()> = StdMutex::new(());

    fn open_flags_create() -> VfsOpenFlags {
        VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE
    }

    fn open_flags_create_unlocked() -> VfsOpenFlags {
        VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE
    }

    #[cfg(feature = "linux-asupersync-uring")]
    struct ScopedAtomicFlag<'a> {
        flag: &'a AtomicBool,
    }

    #[cfg(feature = "linux-asupersync-uring")]
    impl<'a> ScopedAtomicFlag<'a> {
        fn enable(flag: &'a AtomicBool) -> Self {
            flag.store(true, Ordering::Release);
            Self { flag }
        }
    }

    #[cfg(feature = "linux-asupersync-uring")]
    impl Drop for ScopedAtomicFlag<'_> {
        fn drop(&mut self) {
            self.flag.store(false, Ordering::Release);
        }
    }

    fn io_uring_test_guard() -> StdMutexGuard<'static, ()> {
        IO_URING_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[test]
    fn test_io_uring_vfs_name_and_status() {
        let vfs = IoUringVfs::new();
        assert_eq!(vfs.name(), "io_uring");
        assert!(!vfs.status().is_empty());
        assert_eq!(vfs.is_memory(), vfs.unix.is_memory());
    }

    #[test]
    fn test_io_uring_vfs_forwards_expected_identity_open() {
        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("uring_identity.db");
        std::fs::write(&path, b"existing database bytes").expect("seed existing file");
        let identity_source = std::fs::File::open(&path).expect("open identity source");
        let expected_identity = FileIdentity::from_file(&identity_source)
            .expect("query identity")
            .expect("Unix files have stable identities");

        let requested_flags = VfsOpenFlags::MAIN_DB
            | VfsOpenFlags::READWRITE
            | VfsOpenFlags::CREATE
            | VfsOpenFlags::EXCLUSIVE;
        let (mut file, out_flags) = vfs
            .open_with_expected_identity(&cx, &path, requested_flags, expected_identity)
            .expect("matching identity should open through io_uring decorator");

        assert_eq!(
            file.file_identity().expect("wrapped identity"),
            Some(expected_identity)
        );
        assert!(!out_flags.intersects(VfsOpenFlags::CREATE | VfsOpenFlags::EXCLUSIVE));
        file.close(&cx).expect("close decorated file");
    }

    #[test]
    fn test_io_uring_vfs_forwards_reserved_identity_open() {
        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("uring_reserved.db");
        let identity_source = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .expect("create reservation");
        let expected_identity = FileIdentity::from_file(&identity_source)
            .expect("query identity")
            .expect("Unix files have stable identities");
        drop(identity_source);

        let requested_flags = VfsOpenFlags::MAIN_DB
            | VfsOpenFlags::READWRITE
            | VfsOpenFlags::CREATE
            | VfsOpenFlags::EXCLUSIVE;
        let (mut file, out_flags) = vfs
            .open_reserved_with_expected_identity(&cx, &path, requested_flags, expected_identity)
            .expect("matching empty reservation should open through io_uring decorator");

        assert_eq!(
            file.file_identity().expect("wrapped identity"),
            Some(expected_identity)
        );
        assert!(!out_flags.intersects(VfsOpenFlags::CREATE | VfsOpenFlags::EXCLUSIVE));
        file.close(&cx).expect("close decorated file");
    }

    #[test]
    fn test_io_uring_vfs_forwards_no_follow_path_entry_check() {
        use std::os::unix::fs::symlink;

        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        let dir = tempfile::tempdir().expect("tempdir");
        let dangling = dir.path().join("dangling-wal");
        symlink(dir.path().join("missing-target"), &dangling).expect("create dangling symlink");

        assert!(
            vfs.path_entry_exists(&cx, &dangling)
                .expect("forward no-follow entry check")
        );
    }

    #[test]
    fn test_io_uring_vfs_roundtrip_write_read() {
        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("uring_roundtrip.db");

        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create_unlocked())
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
        let _guard = io_uring_test_guard();
        reset_io_uring_latency_metrics();

        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("uring_metrics.db");

        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create_unlocked())
            .expect("open should succeed");
        file.write(&cx, b"metrics", 0)
            .expect("write should succeed");

        let mut buf = [0_u8; 7];
        let _ = file.read(&cx, &mut buf, 0).expect("read should succeed");

        let snapshot = io_uring_latency_snapshot();
        if vfs.is_available() {
            assert!(
                snapshot.write_samples_total >= 1 || snapshot.write_unix_fallbacks_total >= 1,
                "write path should either record io_uring latency or fallback"
            );
            assert!(
                snapshot.read_samples_total >= 1 || snapshot.read_unix_fallbacks_total >= 1,
                "read path should either record io_uring latency or fallback"
            );
        }
    }

    #[test]
    fn test_disabled_runtime_records_unix_fallback_metrics() {
        let _guard = io_uring_test_guard();
        reset_io_uring_latency_metrics();

        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        vfs.runtime.disable("test disable before io");
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("uring_disabled_runtime_metrics.db");

        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create_unlocked())
            .expect("open should succeed via unix fallback");
        file.write(&cx, b"fallback-metrics", 0)
            .expect("write should succeed via unix path");
        let mut buf = [0_u8; 16];
        let _ = file
            .read(&cx, &mut buf, 0)
            .expect("read should succeed via unix path");

        let snapshot = io_uring_latency_snapshot();
        assert!(
            snapshot.unix_fallbacks_total >= 2,
            "disabled runtime should record fallback for both write/read ops"
        );
        assert!(
            snapshot.write_unix_fallbacks_total >= 1,
            "disabled runtime should record write fallback"
        );
        assert!(
            snapshot.read_unix_fallbacks_total >= 1,
            "disabled runtime should record read fallback"
        );
    }

    #[test]
    fn test_runtime_disable_is_sticky() {
        let runtime = IoUringRuntime::new();
        assert!(!runtime.is_disabled());
        assert_eq!(runtime.disable_reason(), None);
        runtime.disable("test disable");
        assert!(runtime.is_disabled());
        assert_eq!(runtime.disable_reason(), Some("test disable"));
        assert_eq!(runtime.status(), "disabled:asupersync:test disable");
        runtime.disable("test disable again");
        assert!(runtime.is_disabled());
        assert_eq!(runtime.disable_reason(), Some("test disable"));
    }

    #[test]
    fn test_invalid_input_errors_propagate_without_fallback_or_disable() {
        let err = FrankenError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "offset overflow during io_uring write",
        ));

        assert!(!should_fallback_to_unix_on_uring_error(&err));
        assert!(!should_disable_runtime_on_uring_fallback(&err));
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
        assert_eq!(
            runtime.disable_reason(),
            Some(IO_URING_READ_CONFORMAL_BREACH_MSG)
        );
        assert_eq!(
            runtime.status(),
            format!("disabled:asupersync:{IO_URING_READ_CONFORMAL_BREACH_MSG}")
        );
    }

    #[test]
    fn test_vfs_status_snapshot_reflects_disable_reason() {
        let vfs = IoUringVfs::new();
        let initial = vfs.status_snapshot();
        assert_eq!(initial.backend, "asupersync");
        assert_eq!(initial.initial_status, "available:asupersync");
        assert_eq!(initial.status, "available:asupersync");
        assert_eq!(initial.disable_reason, None);

        vfs.runtime.disable("manual test disable");

        let disabled = vfs.status_snapshot();
        assert!(disabled.disabled);
        assert!(!disabled.available);
        assert_eq!(disabled.disable_reason, Some("manual test disable"));
        assert_eq!(disabled.status, "disabled:asupersync:manual test disable");
    }

    #[cfg(feature = "linux-asupersync-uring")]
    #[test]
    fn test_temp_file_fallback_does_not_disable_runtime() {
        let _guard = io_uring_test_guard();
        reset_io_uring_latency_metrics();

        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        if !vfs.is_available() {
            return;
        }

        let flags = VfsOpenFlags::TEMP_DB
            | VfsOpenFlags::CREATE
            | VfsOpenFlags::READWRITE
            | VfsOpenFlags::DELETEONCLOSE;
        let (mut file, _) = vfs.open(&cx, None, flags).expect("open temp file");

        file.write(&cx, b"temp data", 0)
            .expect("write should fall back without disabling runtime");
        let mut buf = [0_u8; 9];
        let n = file
            .read(&cx, &mut buf, 0)
            .expect("read should fall back without disabling runtime");

        assert_eq!(n, 9);
        assert_eq!(&buf, b"temp data");
        assert!(
            vfs.is_available(),
            "temp-file fallback should not disable io_uring"
        );
        assert!(!vfs.runtime.is_disabled());

        let snapshot = io_uring_latency_snapshot();
        assert!(
            snapshot.unix_fallbacks_total >= 2,
            "temp-file fallback should record unix fallback metrics"
        );
    }

    #[cfg(feature = "linux-asupersync-uring")]
    #[test]
    fn test_main_db_open_skips_secondary_io_uring_fd() {
        let _guard = io_uring_test_guard();
        reset_io_uring_latency_metrics();

        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        if !vfs.is_available() {
            return;
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("main_db_direct_unix.db");
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create())
            .expect("open should succeed");

        assert!(
            file.asupersync_backend.is_none(),
            "main-db opens must not create a second fd while unix lock coalescing is active"
        );

        file.write(&cx, b"main-db", 0)
            .expect("write should succeed via unix path");
        let mut buf = [0_u8; 7];
        let n = file.read(&cx, &mut buf, 0).expect("read should succeed");
        assert_eq!(n, 7);
        assert_eq!(&buf, b"main-db");
        assert!(
            vfs.is_available(),
            "skipping io_uring fd should not disable runtime"
        );

        let snapshot = io_uring_latency_snapshot();
        assert!(
            snapshot.unix_fallbacks_total >= 2,
            "main-db direct unix path should avoid io_uring and record unix-path ops"
        );
    }

    #[cfg(feature = "linux-asupersync-uring")]
    #[test]
    fn test_wal_open_skips_secondary_io_uring_fd() {
        let _guard = io_uring_test_guard();
        reset_io_uring_latency_metrics();

        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        if !vfs.is_available() {
            return;
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("main.db-wal");
        let wal_flags = VfsOpenFlags::WAL | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (mut file, _) = vfs
            .open(&cx, Some(&path), wal_flags)
            .expect("open should succeed");

        assert!(
            file.asupersync_backend.is_none(),
            "wal opens must not create a second fd while unix lock coalescing is active"
        );

        file.write(&cx, b"wal", 0)
            .expect("write should succeed via unix path");
        let mut buf = [0_u8; 3];
        let n = file.read(&cx, &mut buf, 0).expect("read should succeed");
        assert_eq!(n, 3);
        assert_eq!(&buf, b"wal");
        assert!(
            vfs.is_available(),
            "skipping io_uring fd should not disable runtime"
        );

        let snapshot = io_uring_latency_snapshot();
        assert!(
            snapshot.unix_fallbacks_total >= 2,
            "wal direct unix path should avoid io_uring and record unix-path ops"
        );
    }

    #[cfg(feature = "linux-asupersync-uring")]
    #[test]
    fn test_write_abort_propagates_without_disabling_runtime_or_fallback() {
        let _guard = io_uring_test_guard();
        reset_io_uring_latency_metrics();

        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        if !vfs.is_available() {
            return;
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("asupersync_abort_propagation.db");
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create_unlocked())
            .expect("open should succeed");

        let _force_abort = ScopedAtomicFlag::enable(&FORCE_ASUPERSYNC_WRITE_ABORT);
        let err = file
            .write(&cx, b"abort", 0)
            .expect_err("write should propagate abort");

        assert!(matches!(err, FrankenError::Abort));
        assert!(vfs.is_available(), "abort should not disable io_uring");
        assert!(!vfs.runtime.is_disabled());

        let snapshot = io_uring_latency_snapshot();
        assert_eq!(
            snapshot.unix_fallbacks_total, 0,
            "abort should not fall back to unix or record fallback metrics"
        );
    }

    #[cfg(feature = "linux-asupersync-uring")]
    #[test]
    fn test_read_abort_propagates_without_disabling_runtime_or_fallback() -> Result<()> {
        let _guard = io_uring_test_guard();
        reset_io_uring_latency_metrics();

        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        if !vfs.is_available() {
            return Ok(());
        }

        let dir = tempfile::tempdir().map_err(FrankenError::Io)?;
        let path = dir.path().join("asupersync_read_abort_propagation.db");
        let (file, _) = vfs.open(&cx, Some(&path), open_flags_create_unlocked())?;
        reset_io_uring_latency_metrics();

        let _force_abort = ScopedAtomicFlag::enable(&FORCE_ASUPERSYNC_READ_ABORT);
        let mut buf = [0_u8; 4];
        let err = match file.read(&cx, &mut buf, 0) {
            Ok(bytes) => {
                return Err(FrankenError::Io(io::Error::other(format!(
                    "read should propagate abort, read {bytes} bytes"
                ))));
            }
            Err(err) => err,
        };

        assert!(matches!(err, FrankenError::Abort));
        assert!(vfs.is_available(), "abort should not disable io_uring");
        assert!(!vfs.runtime.is_disabled());

        let snapshot = io_uring_latency_snapshot();
        assert_eq!(
            snapshot.unix_fallbacks_total, 0,
            "abort should not fall back to unix or record fallback metrics"
        );
        Ok(())
    }

    #[cfg(all(feature = "linux-uring-fs", not(feature = "linux-asupersync-uring")))]
    #[test]
    fn test_lock_mutex_or_io_handles_poison_without_panicking() {
        let mutex = Mutex::new(7_u8);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = mutex.lock().unwrap_or_else(|e| e.into_inner());
            std::panic::panic_any("poison mutex");
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
                std::panic::panic_any("poison io_uring runtime lock");
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
        let _guard = io_uring_test_guard();
        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("asupersync_forced_init_failure.db");

        let _force_init_fail = ScopedAtomicFlag::enable(&FORCE_ASUPERSYNC_INIT_FAIL);
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create_unlocked())
            .expect("open should succeed via unix fallback");

        assert!(vfs.runtime.is_disabled());
        assert!(!vfs.is_available());
        let status = vfs.status_snapshot();
        assert_eq!(
            status.disable_reason,
            Some(IO_URING_ASUPERSYNC_INIT_FAILED_MSG)
        );
        assert_eq!(
            status.status,
            format!("disabled:asupersync:{IO_URING_ASUPERSYNC_INIT_FAILED_MSG}")
        );

        file.write(&cx, b"fallback", 0)
            .expect("write should succeed via unix fallback");
        let mut buf = [0_u8; 8];
        let n = file
            .read(&cx, &mut buf, 0)
            .expect("read should succeed via unix fallback");
        assert_eq!(n, 8);
        assert_eq!(&buf, b"fallback");
    }

    #[cfg(feature = "linux-asupersync-uring")]
    #[test]
    fn test_asupersync_write_error_disables_runtime_and_falls_back() {
        let _guard = io_uring_test_guard();
        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        if !vfs.is_available() {
            return;
        }
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("asupersync_forced_write_failure.db");
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create_unlocked())
            .expect("open should succeed");

        let _force_write_fail = ScopedAtomicFlag::enable(&FORCE_ASUPERSYNC_WRITE_FAIL);
        file.write(&cx, b"fallback", 0)
            .expect("write should succeed via unix fallback");

        assert!(vfs.runtime.is_disabled());
        assert!(!vfs.is_available());

        let mut buf = [0_u8; 8];
        let n = file
            .read(&cx, &mut buf, 0)
            .expect("read should use unix path after runtime disable");
        assert_eq!(n, 8);
        assert_eq!(&buf, b"fallback");
    }

    #[cfg(feature = "linux-asupersync-uring")]
    #[test]
    fn test_asupersync_read_error_disables_runtime_and_falls_back() {
        let _guard = io_uring_test_guard();
        let cx = Cx::new();
        let vfs = IoUringVfs::new();
        if !vfs.is_available() {
            return;
        }
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("asupersync_forced_read_failure.db");
        let (mut file, _) = vfs
            .open(&cx, Some(&path), open_flags_create_unlocked())
            .expect("open should succeed");

        file.write(&cx, b"fallback", 0)
            .expect("write should seed data");

        let _force_read_fail = ScopedAtomicFlag::enable(&FORCE_ASUPERSYNC_READ_FAIL);
        let mut buf = [0_u8; 8];
        let n = file
            .read(&cx, &mut buf, 0)
            .expect("read should succeed via unix fallback");

        assert_eq!(n, 8);
        assert_eq!(&buf, b"fallback");
        assert!(vfs.runtime.is_disabled());
        assert!(!vfs.is_available());
    }

    #[test]
    fn io_uring_runtime_status_clone_eq_debug() {
        let vfs = IoUringVfs::new();
        let snap = vfs.status_snapshot();
        let cloned = snap.clone();
        assert_eq!(cloned, snap);
        let dbg = format!("{snap:?}");
        assert!(dbg.contains("IoUringRuntimeStatus"));
        assert!(dbg.contains(snap.backend));
    }

    #[test]
    fn io_uring_vfs_default_equals_new() {
        let from_new = IoUringVfs::new();
        let from_default = IoUringVfs::default();
        assert_eq!(from_new.name(), from_default.name());
        assert_eq!(from_new.status(), from_default.status());
    }

    #[test]
    fn io_uring_vfs_status_snapshot_fresh_fields() {
        let vfs = IoUringVfs::new();
        let snap = vfs.status_snapshot();
        assert!(!snap.backend.is_empty());
        assert!(!snap.disabled);
        assert!(snap.disable_reason.is_none());
        assert!(!snap.initial_status.is_empty());
        assert!(!snap.status.is_empty());
    }

    #[test]
    fn io_uring_vfs_status_contains_backend() {
        let vfs = IoUringVfs::new();
        let status = vfs.status();
        let snap = vfs.status_snapshot();
        assert!(status.contains(snap.backend));
    }
}
