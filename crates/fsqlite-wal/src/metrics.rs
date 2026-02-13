//! WAL observability metrics.
//!
//! Global `AtomicU64` counters for frame writes, checkpoint operations, and WAL
//! size tracking.  Thread-safe, lock-free, suitable for concurrent writers.
//!
//! Metrics are recorded by [`WalFile::append_frame`](crate::wal::WalFile) and
//! [`execute_checkpoint`](crate::checkpoint_executor::execute_checkpoint) when
//! the corresponding instrumentation hooks fire.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// Metric counters
// ---------------------------------------------------------------------------

/// Global WAL metrics singleton.
pub static GLOBAL_WAL_METRICS: WalMetrics = WalMetrics::new();

/// Atomic counters tracking WAL write and checkpoint activity.
pub struct WalMetrics {
    /// Total WAL frames written (monotonic counter).
    pub frames_written_total: AtomicU64,
    /// Total bytes written to the WAL (frame headers + page data).
    pub bytes_written_total: AtomicU64,
    /// Total number of checkpoint operations executed.
    pub checkpoint_count: AtomicU64,
    /// Total frames backfilled to the database during checkpoints.
    pub checkpoint_frames_backfilled_total: AtomicU64,
    /// Cumulative checkpoint wall-clock time in microseconds.
    pub checkpoint_duration_us_total: AtomicU64,
    /// Total WAL reset operations (after restart/truncate checkpoints).
    pub wal_resets_total: AtomicU64,
}

impl WalMetrics {
    /// Create a zeroed metrics instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            frames_written_total: AtomicU64::new(0),
            bytes_written_total: AtomicU64::new(0),
            checkpoint_count: AtomicU64::new(0),
            checkpoint_frames_backfilled_total: AtomicU64::new(0),
            checkpoint_duration_us_total: AtomicU64::new(0),
            wal_resets_total: AtomicU64::new(0),
        }
    }

    /// Record a frame write.
    pub fn record_frame_write(&self, frame_bytes: u64) {
        self.frames_written_total.fetch_add(1, Ordering::Relaxed);
        self.bytes_written_total
            .fetch_add(frame_bytes, Ordering::Relaxed);
    }

    /// Record a completed checkpoint.
    pub fn record_checkpoint(&self, frames_backfilled: u64, duration_us: u64) {
        self.checkpoint_count.fetch_add(1, Ordering::Relaxed);
        self.checkpoint_frames_backfilled_total
            .fetch_add(frames_backfilled, Ordering::Relaxed);
        self.checkpoint_duration_us_total
            .fetch_add(duration_us, Ordering::Relaxed);
    }

    /// Record a WAL reset.
    pub fn record_wal_reset(&self) {
        self.wal_resets_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a consistent snapshot of all counters.
    #[must_use]
    pub fn snapshot(&self) -> WalMetricsSnapshot {
        WalMetricsSnapshot {
            frames_written_total: self.frames_written_total.load(Ordering::Relaxed),
            bytes_written_total: self.bytes_written_total.load(Ordering::Relaxed),
            checkpoint_count: self.checkpoint_count.load(Ordering::Relaxed),
            checkpoint_frames_backfilled_total: self
                .checkpoint_frames_backfilled_total
                .load(Ordering::Relaxed),
            checkpoint_duration_us_total: self.checkpoint_duration_us_total.load(Ordering::Relaxed),
            wal_resets_total: self.wal_resets_total.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.frames_written_total.store(0, Ordering::Relaxed);
        self.bytes_written_total.store(0, Ordering::Relaxed);
        self.checkpoint_count.store(0, Ordering::Relaxed);
        self.checkpoint_frames_backfilled_total
            .store(0, Ordering::Relaxed);
        self.checkpoint_duration_us_total
            .store(0, Ordering::Relaxed);
        self.wal_resets_total.store(0, Ordering::Relaxed);
    }
}

impl Default for WalMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

/// Point-in-time snapshot of WAL metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalMetricsSnapshot {
    pub frames_written_total: u64,
    pub bytes_written_total: u64,
    pub checkpoint_count: u64,
    pub checkpoint_frames_backfilled_total: u64,
    pub checkpoint_duration_us_total: u64,
    pub wal_resets_total: u64,
}

impl WalMetricsSnapshot {
    /// Average checkpoint duration in microseconds, or 0 if no checkpoints.
    #[must_use]
    pub fn avg_checkpoint_duration_us(&self) -> u64 {
        self.checkpoint_duration_us_total
            .checked_div(self.checkpoint_count)
            .unwrap_or(0)
    }
}

impl fmt::Display for WalMetricsSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "wal_frames_written={} wal_bytes_written={} checkpoints={} \
             ckpt_frames_backfilled={} ckpt_duration_us={} wal_resets={}",
            self.frames_written_total,
            self.bytes_written_total,
            self.checkpoint_count,
            self.checkpoint_frames_backfilled_total,
            self.checkpoint_duration_us_total,
            self.wal_resets_total,
        )
    }
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

/// Convert a `Duration` to microseconds, saturating at `u64::MAX`.
pub(crate) fn duration_us_saturating(d: std::time::Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX)
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_frame_write_counting() {
        let m = WalMetrics::new();
        assert_eq!(m.snapshot().frames_written_total, 0);
        m.record_frame_write(4120);
        m.record_frame_write(4120);
        let snap = m.snapshot();
        assert_eq!(snap.frames_written_total, 2);
        assert_eq!(snap.bytes_written_total, 8240);
    }

    #[test]
    fn metrics_checkpoint_recording() {
        let m = WalMetrics::new();
        m.record_checkpoint(10, 5000);
        m.record_checkpoint(5, 3000);
        let snap = m.snapshot();
        assert_eq!(snap.checkpoint_count, 2);
        assert_eq!(snap.checkpoint_frames_backfilled_total, 15);
        assert_eq!(snap.checkpoint_duration_us_total, 8000);
        assert_eq!(snap.avg_checkpoint_duration_us(), 4000);
    }

    #[test]
    fn metrics_avg_checkpoint_duration_zero_checkpoints() {
        let m = WalMetrics::new();
        assert_eq!(m.snapshot().avg_checkpoint_duration_us(), 0);
    }

    #[test]
    fn metrics_wal_reset_counting() {
        let m = WalMetrics::new();
        m.record_wal_reset();
        m.record_wal_reset();
        m.record_wal_reset();
        assert_eq!(m.snapshot().wal_resets_total, 3);
    }

    #[test]
    fn metrics_reset() {
        let m = WalMetrics::new();
        m.record_frame_write(100);
        m.record_checkpoint(5, 2000);
        m.record_wal_reset();
        m.reset();
        let snap = m.snapshot();
        assert_eq!(snap.frames_written_total, 0);
        assert_eq!(snap.bytes_written_total, 0);
        assert_eq!(snap.checkpoint_count, 0);
        assert_eq!(snap.checkpoint_frames_backfilled_total, 0);
        assert_eq!(snap.checkpoint_duration_us_total, 0);
        assert_eq!(snap.wal_resets_total, 0);
    }

    #[test]
    fn metrics_display() {
        let m = WalMetrics::new();
        m.record_frame_write(4096);
        m.record_checkpoint(3, 1500);
        let s = m.snapshot().to_string();
        assert!(s.contains("wal_frames_written=1"));
        assert!(s.contains("wal_bytes_written=4096"));
        assert!(s.contains("checkpoints=1"));
        assert!(s.contains("ckpt_frames_backfilled=3"));
        assert!(s.contains("ckpt_duration_us=1500"));
        assert!(s.contains("wal_resets=0"));
    }

    #[test]
    fn metrics_default() {
        let m = WalMetrics::default();
        assert_eq!(m.snapshot().frames_written_total, 0);
    }
}
