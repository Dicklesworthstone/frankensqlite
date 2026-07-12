//! VFS observability: tracing spans and operation counters (bd-3u7.1).
//!
//! Provides [`TracingFile`], a wrapper around any [`VfsFile`] that emits
//! structured tracing spans for every I/O operation.
//!
//! ## Span format
//!
//! Each operation emits a `vfs_op` span at DEBUG level with fields:
//! - `op_type`: read, write, sync, lock, unlock, truncate, etc.
//! - `file_path`: path of the file (if known)
//! - `bytes`: number of bytes read or written (for read/write ops)
//! - `duration_us`: duration of the operation in microseconds
//!
//! Lock acquisition emits at TRACE level for finer granularity.
//!
//! ## Metrics
//!
//! [`VfsMetrics`] provides global atomic counters:
//! - `operations_total`: counter by operation type
//! - `read_bytes_total` / `write_bytes_total`: byte counters
//! - `sync_count`: total syncs

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use fsqlite_error::Result;
use fsqlite_types::LockLevel;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::SyncFlags;

use crate::shm::ShmRegion;
use crate::traits::{FileIdentity, VfsFile};

// ---------------------------------------------------------------------------
// Global metrics counters
// ---------------------------------------------------------------------------

/// Global VFS operation metrics.
///
/// All counters are monotonically increasing atomic u64 values.
/// Thread-safe and lock-free for minimal overhead.
pub struct VfsMetrics {
    pub read_ops: AtomicU64,
    pub write_ops: AtomicU64,
    pub sync_ops: AtomicU64,
    pub lock_ops: AtomicU64,
    pub unlock_ops: AtomicU64,
    pub truncate_ops: AtomicU64,
    pub close_ops: AtomicU64,
    pub file_size_ops: AtomicU64,
    pub read_bytes_total: AtomicU64,
    pub write_bytes_total: AtomicU64,
}

impl VfsMetrics {
    /// Create a new zeroed metrics instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            read_ops: AtomicU64::new(0),
            write_ops: AtomicU64::new(0),
            sync_ops: AtomicU64::new(0),
            lock_ops: AtomicU64::new(0),
            unlock_ops: AtomicU64::new(0),
            truncate_ops: AtomicU64::new(0),
            close_ops: AtomicU64::new(0),
            file_size_ops: AtomicU64::new(0),
            read_bytes_total: AtomicU64::new(0),
            write_bytes_total: AtomicU64::new(0),
        }
    }

    /// Snapshot all counters.
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            read_ops: self.read_ops.load(Ordering::Relaxed),
            write_ops: self.write_ops.load(Ordering::Relaxed),
            sync_ops: self.sync_ops.load(Ordering::Relaxed),
            lock_ops: self.lock_ops.load(Ordering::Relaxed),
            unlock_ops: self.unlock_ops.load(Ordering::Relaxed),
            truncate_ops: self.truncate_ops.load(Ordering::Relaxed),
            close_ops: self.close_ops.load(Ordering::Relaxed),
            file_size_ops: self.file_size_ops.load(Ordering::Relaxed),
            read_bytes_total: self.read_bytes_total.load(Ordering::Relaxed),
            write_bytes_total: self.write_bytes_total.load(Ordering::Relaxed),
        }
    }

    /// Total operations across all types.
    #[must_use]
    pub fn total_ops(&self) -> u64 {
        self.read_ops.load(Ordering::Relaxed)
            + self.write_ops.load(Ordering::Relaxed)
            + self.sync_ops.load(Ordering::Relaxed)
            + self.lock_ops.load(Ordering::Relaxed)
            + self.unlock_ops.load(Ordering::Relaxed)
            + self.truncate_ops.load(Ordering::Relaxed)
            + self.close_ops.load(Ordering::Relaxed)
            + self.file_size_ops.load(Ordering::Relaxed)
    }
}

impl Default for VfsMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// A point-in-time snapshot of VFS metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub read_ops: u64,
    pub write_ops: u64,
    pub sync_ops: u64,
    pub lock_ops: u64,
    pub unlock_ops: u64,
    pub truncate_ops: u64,
    pub close_ops: u64,
    pub file_size_ops: u64,
    pub read_bytes_total: u64,
    pub write_bytes_total: u64,
}

impl MetricsSnapshot {
    /// Total operations.
    #[must_use]
    pub fn total_ops(&self) -> u64 {
        self.read_ops
            + self.write_ops
            + self.sync_ops
            + self.lock_ops
            + self.unlock_ops
            + self.truncate_ops
            + self.close_ops
            + self.file_size_ops
    }
}

impl std::fmt::Display for MetricsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "VfsMetrics {{ read: {} ({} B), write: {} ({} B), sync: {}, lock: {}, unlock: {}, \
             truncate: {}, close: {}, file_size: {} }}",
            self.read_ops,
            self.read_bytes_total,
            self.write_ops,
            self.write_bytes_total,
            self.sync_ops,
            self.lock_ops,
            self.unlock_ops,
            self.truncate_ops,
            self.close_ops,
            self.file_size_ops,
        )
    }
}

/// Global VFS metrics singleton.
pub static GLOBAL_VFS_METRICS: VfsMetrics = VfsMetrics::new();

// ---------------------------------------------------------------------------
// Tracing wrapper for VfsFile
// ---------------------------------------------------------------------------

/// A wrapper around any [`VfsFile`] that emits `vfs_op` tracing spans
/// and increments [`GLOBAL_VFS_METRICS`] counters.
///
/// ## Usage
///
/// ```ignore
/// let raw_file = vfs.open(cx, path, flags)?;
/// let traced = TracingFile::new(raw_file, "/path/to/db");
/// traced.read(cx, &mut buf, 0)?; // emits vfs_op span + increments read counter
/// ```
pub struct TracingFile<F: VfsFile> {
    inner: F,
    path: String,
}

impl<F: VfsFile> TracingFile<F> {
    /// Wrap a file with tracing instrumentation.
    #[must_use]
    pub fn new(inner: F, path: impl Into<String>) -> Self {
        Self {
            inner,
            path: path.into(),
        }
    }

    /// Access the inner file.
    #[must_use]
    pub fn inner(&self) -> &F {
        &self.inner
    }

    /// Access the inner file mutably.
    pub fn inner_mut(&mut self) -> &mut F {
        &mut self.inner
    }

    /// The file path used for tracing.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }
}

/// Convert elapsed micros to u64, saturating at `u64::MAX`.
#[inline]
fn duration_us_saturating(start: Instant) -> u64 {
    let micros = start.elapsed().as_micros();
    u64::try_from(micros).unwrap_or(u64::MAX)
}

/// Measure duration and log a vfs_op span.
macro_rules! vfs_trace_op {
    ($op:expr, $path:expr, $bytes:expr, $body:expr) => {{
        let start = Instant::now();
        let result = $body;
        let duration_us = duration_us_saturating(start);
        tracing::debug!(
            op_type = $op,
            file_path = $path,
            bytes = $bytes,
            duration_us = duration_us,
            ok = result.is_ok(),
            "vfs_op"
        );
        result
    }};
}

/// Trace a lock operation at TRACE level for finer granularity.
macro_rules! vfs_trace_lock {
    ($op:expr, $path:expr, $level:expr, $body:expr) => {{
        let start = Instant::now();
        let result = $body;
        let duration_us = duration_us_saturating(start);
        tracing::trace!(
            op_type = $op,
            file_path = $path,
            lock_level = ?$level,
            duration_us = duration_us,
            ok = result.is_ok(),
            "vfs_op"
        );
        result
    }};
}

impl<F: VfsFile> VfsFile for TracingFile<F> {
    fn close(&mut self, cx: &Cx) -> Result<()> {
        GLOBAL_VFS_METRICS.close_ops.fetch_add(1, Ordering::Relaxed);
        vfs_trace_op!("close", &*self.path, 0_u64, self.inner.close(cx))
    }

    fn file_identity(&self) -> Result<Option<FileIdentity>> {
        self.inner.file_identity()
    }

    fn read(&self, cx: &Cx, buf: &mut [u8], offset: u64) -> Result<usize> {
        GLOBAL_VFS_METRICS.read_ops.fetch_add(1, Ordering::Relaxed);
        let bytes_requested = buf.len() as u64;
        let result = vfs_trace_op!(
            "read",
            &*self.path,
            bytes_requested,
            self.inner.read(cx, buf, offset)
        );
        if let Ok(n) = &result {
            GLOBAL_VFS_METRICS
                .read_bytes_total
                .fetch_add(*n as u64, Ordering::Relaxed);
        }
        result
    }

    fn write(&mut self, cx: &Cx, buf: &[u8], offset: u64) -> Result<()> {
        GLOBAL_VFS_METRICS.write_ops.fetch_add(1, Ordering::Relaxed);
        let bytes = buf.len() as u64;
        GLOBAL_VFS_METRICS
            .write_bytes_total
            .fetch_add(bytes, Ordering::Relaxed);
        vfs_trace_op!(
            "write",
            &*self.path,
            bytes,
            self.inner.write(cx, buf, offset)
        )
    }

    fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
        GLOBAL_VFS_METRICS
            .truncate_ops
            .fetch_add(1, Ordering::Relaxed);
        vfs_trace_op!("truncate", &*self.path, size, self.inner.truncate(cx, size))
    }

    fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
        GLOBAL_VFS_METRICS.sync_ops.fetch_add(1, Ordering::Relaxed);
        vfs_trace_op!("sync", &*self.path, 0_u64, self.inner.sync(cx, flags))
    }

    fn file_size(&self, cx: &Cx) -> Result<u64> {
        GLOBAL_VFS_METRICS
            .file_size_ops
            .fetch_add(1, Ordering::Relaxed);
        // file_size is read-only and very frequent; use trace! not debug!
        let start = Instant::now();
        let result = self.inner.file_size(cx);
        let duration_us = duration_us_saturating(start);
        tracing::trace!(
            op_type = "file_size",
            file_path = &*self.path,
            duration_us = duration_us,
            "vfs_op"
        );
        result
    }

    fn lock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
        GLOBAL_VFS_METRICS.lock_ops.fetch_add(1, Ordering::Relaxed);
        vfs_trace_lock!("lock", &*self.path, level, self.inner.lock(cx, level))
    }

    fn unlock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
        GLOBAL_VFS_METRICS
            .unlock_ops
            .fetch_add(1, Ordering::Relaxed);
        vfs_trace_lock!("unlock", &*self.path, level, self.inner.unlock(cx, level))
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

impl<F: VfsFile + crate::traits::AsyncVfsDataPath> crate::traits::AsyncVfsDataPath
    for TracingFile<F>
{
}

// TracingFile is Send+Sync automatically because F: VfsFile requires Send+Sync
// and String is Send+Sync.

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::memory::MemoryVfs;
    use crate::traits::Vfs;
    use fsqlite_types::flags::VfsOpenFlags;

    #[test]
    fn tracing_file_wraps_operations() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let (file, _) = vfs
            .open(
                &cx,
                Some(Path::new("test.db")),
                VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE,
            )
            .unwrap();

        let mut traced = TracingFile::new(file, "test.db");

        // Write some data.
        traced.write(&cx, b"hello world", 0).unwrap();

        // Read it back.
        let mut buf = [0u8; 11];
        let n = traced.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(n, 11);
        assert_eq!(&buf, b"hello world");

        // File size.
        let size = traced.file_size(&cx).unwrap();
        assert_eq!(size, 11);

        // Truncate.
        traced.truncate(&cx, 5).unwrap();
        let size = traced.file_size(&cx).unwrap();
        assert_eq!(size, 5);

        // Lock/unlock.
        traced.lock(&cx, LockLevel::Shared).unwrap();
        traced.unlock(&cx, LockLevel::None).unwrap();

        // Close.
        traced.close(&cx).unwrap();
    }

    #[test]
    fn global_metrics_increment() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let (file, _) = vfs
            .open(
                &cx,
                Some(Path::new("metrics_test.db")),
                VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE,
            )
            .unwrap();

        let before = GLOBAL_VFS_METRICS.snapshot();
        let mut traced = TracingFile::new(file, "metrics_test.db");

        traced.write(&cx, b"data", 0).unwrap();
        let mut buf = [0u8; 4];
        traced.read(&cx, &mut buf, 0).unwrap();
        traced.lock(&cx, LockLevel::Shared).unwrap();
        traced.unlock(&cx, LockLevel::None).unwrap();

        let after = GLOBAL_VFS_METRICS.snapshot();

        assert!(after.write_ops > before.write_ops);
        assert!(after.read_ops > before.read_ops);
        assert!(after.lock_ops > before.lock_ops);
        assert!(after.unlock_ops > before.unlock_ops);
        assert!(after.write_bytes_total >= before.write_bytes_total + 4);
        assert!(after.read_bytes_total >= before.read_bytes_total + 4);

        traced.close(&cx).unwrap();
    }

    #[test]
    fn metrics_snapshot_display() {
        let snap = MetricsSnapshot {
            read_ops: 10,
            write_ops: 5,
            sync_ops: 2,
            lock_ops: 3,
            unlock_ops: 3,
            truncate_ops: 1,
            close_ops: 1,
            file_size_ops: 20,
            read_bytes_total: 40960,
            write_bytes_total: 20480,
        };
        let display = format!("{snap}");
        assert!(display.contains("read: 10"));
        assert!(display.contains("write: 5"));
        assert!(display.contains("40960 B"));
    }

    #[test]
    fn metrics_total_ops() {
        let snap = MetricsSnapshot {
            read_ops: 1,
            write_ops: 2,
            sync_ops: 3,
            lock_ops: 4,
            unlock_ops: 5,
            truncate_ops: 6,
            close_ops: 7,
            file_size_ops: 8,
            read_bytes_total: 0,
            write_bytes_total: 0,
        };
        assert_eq!(snap.total_ops(), 36);
    }

    #[test]
    fn tracing_file_path_accessor() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let (file, _) = vfs
            .open(
                &cx,
                Some(Path::new("accessor.db")),
                VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE,
            )
            .unwrap();

        let traced = TracingFile::new(file, "accessor.db");
        assert_eq!(traced.path(), "accessor.db");
    }

    #[test]
    fn vfs_metrics_new_is_zeroed() {
        let m = VfsMetrics::new();
        assert_eq!(m.read_ops.load(Ordering::Relaxed), 0);
        assert_eq!(m.write_ops.load(Ordering::Relaxed), 0);
        assert_eq!(m.sync_ops.load(Ordering::Relaxed), 0);
        assert_eq!(m.lock_ops.load(Ordering::Relaxed), 0);
        assert_eq!(m.unlock_ops.load(Ordering::Relaxed), 0);
        assert_eq!(m.truncate_ops.load(Ordering::Relaxed), 0);
        assert_eq!(m.close_ops.load(Ordering::Relaxed), 0);
        assert_eq!(m.file_size_ops.load(Ordering::Relaxed), 0);
        assert_eq!(m.read_bytes_total.load(Ordering::Relaxed), 0);
        assert_eq!(m.write_bytes_total.load(Ordering::Relaxed), 0);
        assert_eq!(m.total_ops(), 0);
    }

    #[test]
    fn vfs_metrics_total_ops_matches_snapshot() {
        let m = VfsMetrics::new();
        m.read_ops.store(10, Ordering::Relaxed);
        m.write_ops.store(5, Ordering::Relaxed);
        m.sync_ops.store(3, Ordering::Relaxed);
        m.lock_ops.store(2, Ordering::Relaxed);
        m.unlock_ops.store(2, Ordering::Relaxed);
        m.truncate_ops.store(1, Ordering::Relaxed);
        m.close_ops.store(1, Ordering::Relaxed);
        m.file_size_ops.store(7, Ordering::Relaxed);

        let snap = m.snapshot();
        assert_eq!(m.total_ops(), snap.total_ops());
        assert_eq!(snap.total_ops(), 31);
    }

    #[test]
    fn metrics_snapshot_default_is_zeroed() {
        let snap = MetricsSnapshot::default();
        assert_eq!(snap.total_ops(), 0);
        assert_eq!(snap.read_bytes_total, 0);
        assert_eq!(snap.write_bytes_total, 0);
    }

    #[test]
    fn global_metrics_sync_truncate_file_size_close_increment() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let (file, _) = vfs
            .open(
                &cx,
                Some(Path::new("ops_test.db")),
                VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE,
            )
            .unwrap();

        let before = GLOBAL_VFS_METRICS.snapshot();
        let mut traced = TracingFile::new(file, "ops_test.db");

        traced.write(&cx, &[0u8; 100], 0).unwrap();
        traced.sync(&cx, SyncFlags::NORMAL).unwrap();
        traced.truncate(&cx, 50).unwrap();
        let _size = traced.file_size(&cx).unwrap();
        traced.close(&cx).unwrap();

        let after = GLOBAL_VFS_METRICS.snapshot();
        assert!(after.sync_ops > before.sync_ops);
        assert!(after.truncate_ops > before.truncate_ops);
        assert!(after.file_size_ops > before.file_size_ops);
        assert!(after.close_ops > before.close_ops);
    }

    #[test]
    fn metrics_snapshot_equality() {
        let a = MetricsSnapshot {
            read_ops: 1,
            write_ops: 2,
            sync_ops: 3,
            lock_ops: 4,
            unlock_ops: 5,
            truncate_ops: 6,
            close_ops: 7,
            file_size_ops: 8,
            read_bytes_total: 100,
            write_bytes_total: 200,
        };
        let b = a;
        assert_eq!(a, b);

        let c = MetricsSnapshot { read_ops: 99, ..a };
        assert_ne!(a, c);
    }

    #[test]
    fn tracing_file_inner_access() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let (file, _) = vfs
            .open(
                &cx,
                Some(Path::new("inner.db")),
                VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE,
            )
            .unwrap();

        let mut traced = TracingFile::new(file, "inner.db");

        // Write through wrapper.
        traced.write(&cx, b"test", 0).unwrap();

        // Read through inner.
        let mut buf = [0u8; 4];
        let n = traced.inner_mut().read(&cx, &mut buf, 0).unwrap();
        assert_eq!(n, 4);
        assert_eq!(&buf, b"test");
    }

    #[test]
    fn vfs_metrics_default_equals_new() {
        let d = VfsMetrics::default();
        let n = VfsMetrics::new();
        assert_eq!(d.total_ops(), n.total_ops());
        assert_eq!(
            d.read_bytes_total.load(Ordering::Relaxed),
            n.read_bytes_total.load(Ordering::Relaxed)
        );
        assert_eq!(
            d.write_bytes_total.load(Ordering::Relaxed),
            n.write_bytes_total.load(Ordering::Relaxed)
        );
    }

    #[test]
    fn snapshot_captures_byte_counters() {
        let m = VfsMetrics::new();
        m.read_bytes_total.store(12345, Ordering::Relaxed);
        m.write_bytes_total.store(67890, Ordering::Relaxed);
        let snap = m.snapshot();
        assert_eq!(snap.read_bytes_total, 12345);
        assert_eq!(snap.write_bytes_total, 67890);
    }

    #[test]
    fn tracing_file_delegates_sector_size_and_device_characteristics() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let (file, _) = vfs
            .open(
                &cx,
                Some(Path::new("delegate.db")),
                VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE,
            )
            .unwrap();

        let inner_sector = file.sector_size();
        let inner_devchar = file.device_characteristics();

        let traced = TracingFile::new(file, "delegate.db");
        assert_eq!(traced.sector_size(), inner_sector);
        assert_eq!(traced.device_characteristics(), inner_devchar);
    }

    #[test]
    fn tracing_file_delegates_check_reserved_lock() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let (file, _) = vfs
            .open(
                &cx,
                Some(Path::new("reserved.db")),
                VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE,
            )
            .unwrap();

        let traced = TracingFile::new(file, "reserved.db");
        let reserved = traced.check_reserved_lock(&cx).unwrap();
        assert!(!reserved);
    }

    #[test]
    fn metrics_snapshot_display_contains_all_op_types() {
        let snap = MetricsSnapshot {
            read_ops: 1,
            write_ops: 2,
            sync_ops: 3,
            lock_ops: 4,
            unlock_ops: 5,
            truncate_ops: 6,
            close_ops: 7,
            file_size_ops: 8,
            read_bytes_total: 100,
            write_bytes_total: 200,
        };
        let s = format!("{snap}");
        assert!(s.contains("sync: 3"), "missing sync");
        assert!(s.contains("lock: 4"), "missing lock");
        assert!(s.contains("unlock: 5"), "missing unlock");
        assert!(s.contains("truncate: 6"), "missing truncate");
        assert!(s.contains("close: 7"), "missing close");
        assert!(s.contains("file_size: 8"), "missing file_size");
    }

    #[test]
    fn metrics_snapshot_total_ops_sums_all_fields() {
        let snap = MetricsSnapshot {
            read_ops: 10,
            write_ops: 20,
            sync_ops: 5,
            lock_ops: 3,
            unlock_ops: 3,
            truncate_ops: 1,
            close_ops: 1,
            file_size_ops: 2,
            read_bytes_total: 999,
            write_bytes_total: 888,
        };
        assert_eq!(snap.total_ops(), 10 + 20 + 5 + 3 + 3 + 1 + 1 + 2);
    }

    #[test]
    fn metrics_snapshot_display_contains_byte_counters() {
        let snap = MetricsSnapshot {
            read_ops: 1,
            write_ops: 1,
            read_bytes_total: 4096,
            write_bytes_total: 8192,
            ..MetricsSnapshot::default()
        };
        let s = format!("{snap}");
        assert!(s.contains("4096"), "missing read byte count");
        assert!(s.contains("8192"), "missing write byte count");
    }

    #[test]
    fn metrics_snapshot_debug_and_clone() {
        let snap = MetricsSnapshot {
            read_ops: 42,
            write_ops: 7,
            ..MetricsSnapshot::default()
        };
        let cloned = snap;
        assert_eq!(snap, cloned);
        let dbg = format!("{snap:?}");
        assert!(dbg.contains("MetricsSnapshot"));
        assert!(dbg.contains("read_ops"));
        assert!(dbg.contains("42"));
    }

    #[test]
    fn total_ops_excludes_byte_counters() {
        let snap = MetricsSnapshot {
            read_ops: 0,
            write_ops: 0,
            sync_ops: 0,
            lock_ops: 0,
            unlock_ops: 0,
            truncate_ops: 0,
            close_ops: 0,
            file_size_ops: 0,
            read_bytes_total: 999_999,
            write_bytes_total: 888_888,
        };
        assert_eq!(
            snap.total_ops(),
            0,
            "byte counters must not inflate total_ops"
        );
    }

    #[test]
    fn tracing_file_delegates_shm_barrier() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let (file, _) = vfs
            .open(
                &cx,
                Some(Path::new("barrier.db")),
                VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE,
            )
            .unwrap();
        let traced = TracingFile::new(file, "barrier.db");
        traced.shm_barrier();
    }

    #[test]
    fn tracing_file_delegates_set_busy_timeout() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let (file, _) = vfs
            .open(
                &cx,
                Some(Path::new("busy.db")),
                VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE,
            )
            .unwrap();
        let mut traced = TracingFile::new(file, "busy.db");
        traced.set_busy_timeout_ms(5000);
        traced.set_busy_timeout_ms(0);
    }
}
