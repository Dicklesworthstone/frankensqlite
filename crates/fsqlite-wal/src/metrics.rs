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

use serde::Serialize;

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
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
// WAL FEC repair counters
// ---------------------------------------------------------------------------

/// Global WAL FEC repair metrics singleton.
pub static GLOBAL_WAL_FEC_REPAIR_METRICS: WalFecRepairCounters = WalFecRepairCounters::new();

/// Atomic counters tracking WAL FEC (RaptorQ) repair operations.
pub struct WalFecRepairCounters {
    /// Total repair attempts (successful + failed).
    pub repairs_total: AtomicU64,
    /// Total successful repairs.
    pub repairs_succeeded: AtomicU64,
    /// Total failed repairs.
    pub repairs_failed: AtomicU64,
    /// Cumulative repair latency in microseconds.
    pub repair_duration_us_total: AtomicU64,
    /// Total repair symbol encoding operations.
    pub encode_ops: AtomicU64,
}

impl WalFecRepairCounters {
    /// Create a zeroed counters instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            repairs_total: AtomicU64::new(0),
            repairs_succeeded: AtomicU64::new(0),
            repairs_failed: AtomicU64::new(0),
            repair_duration_us_total: AtomicU64::new(0),
            encode_ops: AtomicU64::new(0),
        }
    }

    /// Record a repair attempt.
    pub fn record_repair(&self, succeeded: bool, duration_us: u64) {
        self.repairs_total.fetch_add(1, Ordering::Relaxed);
        if succeeded {
            self.repairs_succeeded.fetch_add(1, Ordering::Relaxed);
        } else {
            self.repairs_failed.fetch_add(1, Ordering::Relaxed);
        }
        self.repair_duration_us_total
            .fetch_add(duration_us, Ordering::Relaxed);
    }

    /// Record a repair symbol encoding operation.
    pub fn record_encode(&self) {
        self.encode_ops.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a snapshot.
    #[must_use]
    pub fn snapshot(&self) -> WalFecRepairCountersSnapshot {
        WalFecRepairCountersSnapshot {
            repairs_total: self.repairs_total.load(Ordering::Relaxed),
            repairs_succeeded: self.repairs_succeeded.load(Ordering::Relaxed),
            repairs_failed: self.repairs_failed.load(Ordering::Relaxed),
            repair_duration_us_total: self.repair_duration_us_total.load(Ordering::Relaxed),
            encode_ops: self.encode_ops.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.repairs_total.store(0, Ordering::Relaxed);
        self.repairs_succeeded.store(0, Ordering::Relaxed);
        self.repairs_failed.store(0, Ordering::Relaxed);
        self.repair_duration_us_total.store(0, Ordering::Relaxed);
        self.encode_ops.store(0, Ordering::Relaxed);
    }
}

impl Default for WalFecRepairCounters {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time snapshot of WAL FEC repair counters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WalFecRepairCountersSnapshot {
    pub repairs_total: u64,
    pub repairs_succeeded: u64,
    pub repairs_failed: u64,
    pub repair_duration_us_total: u64,
    pub encode_ops: u64,
}

impl WalFecRepairCountersSnapshot {
    /// Average repair latency in microseconds, or 0 if no repairs.
    #[must_use]
    pub fn avg_repair_duration_us(&self) -> u64 {
        self.repair_duration_us_total
            .checked_div(self.repairs_total)
            .unwrap_or(0)
    }
}

impl fmt::Display for WalFecRepairCountersSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "wal_fec_repairs={} succeeded={} failed={} repair_duration_us={} encode_ops={}",
            self.repairs_total,
            self.repairs_succeeded,
            self.repairs_failed,
            self.repair_duration_us_total,
            self.encode_ops,
        )
    }
}

// ---------------------------------------------------------------------------
// WAL recovery counters
// ---------------------------------------------------------------------------

/// Global WAL recovery metrics singleton.
pub static GLOBAL_WAL_RECOVERY_METRICS: WalRecoveryCounters = WalRecoveryCounters::new();

/// Atomic counters tracking WAL crash recovery operations.
pub struct WalRecoveryCounters {
    /// Total frames replayed during recovery.
    pub recovery_frames_total: AtomicU64,
    /// Total corruption events detected.
    pub corruption_detected_total: AtomicU64,
    /// Total frames successfully repaired (RaptorQ).
    pub frames_repaired_total: AtomicU64,
    /// Total recovery operations completed.
    pub recovery_ops_total: AtomicU64,
}

impl WalRecoveryCounters {
    /// Create a zeroed counters instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            recovery_frames_total: AtomicU64::new(0),
            corruption_detected_total: AtomicU64::new(0),
            frames_repaired_total: AtomicU64::new(0),
            recovery_ops_total: AtomicU64::new(0),
        }
    }

    /// Record frames replayed during a recovery.
    pub fn record_recovery(&self, frames_replayed: u64, corrupted: u64, repaired: u64) {
        self.recovery_ops_total.fetch_add(1, Ordering::Relaxed);
        self.recovery_frames_total
            .fetch_add(frames_replayed, Ordering::Relaxed);
        self.corruption_detected_total
            .fetch_add(corrupted, Ordering::Relaxed);
        self.frames_repaired_total
            .fetch_add(repaired, Ordering::Relaxed);
    }

    /// Take a snapshot.
    #[must_use]
    pub fn snapshot(&self) -> WalRecoveryCountersSnapshot {
        WalRecoveryCountersSnapshot {
            recovery_frames_total: self.recovery_frames_total.load(Ordering::Relaxed),
            corruption_detected_total: self.corruption_detected_total.load(Ordering::Relaxed),
            frames_repaired_total: self.frames_repaired_total.load(Ordering::Relaxed),
            recovery_ops_total: self.recovery_ops_total.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.recovery_frames_total.store(0, Ordering::Relaxed);
        self.corruption_detected_total.store(0, Ordering::Relaxed);
        self.frames_repaired_total.store(0, Ordering::Relaxed);
        self.recovery_ops_total.store(0, Ordering::Relaxed);
    }
}

impl Default for WalRecoveryCounters {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time snapshot of WAL recovery counters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WalRecoveryCountersSnapshot {
    pub recovery_frames_total: u64,
    pub corruption_detected_total: u64,
    pub frames_repaired_total: u64,
    pub recovery_ops_total: u64,
}

impl fmt::Display for WalRecoveryCountersSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "wal_recovery_frames={} corruption_detected={} frames_repaired={} recovery_ops={}",
            self.recovery_frames_total,
            self.corruption_detected_total,
            self.frames_repaired_total,
            self.recovery_ops_total,
        )
    }
}

// ---------------------------------------------------------------------------
// Group commit metrics
// ---------------------------------------------------------------------------

/// Global group commit metrics singleton.
pub static GLOBAL_GROUP_COMMIT_METRICS: GroupCommitMetrics = GroupCommitMetrics::new();

/// Atomic counters tracking parallel WAL group commit activity.
pub struct GroupCommitMetrics {
    /// Total group commit flushes (each flush = 1 fsync1 + 1 fsync2).
    pub group_commits_total: AtomicU64,
    /// Sum of batch sizes across all group commits (for computing average).
    pub group_commit_size_sum: AtomicU64,
    /// Total individual commit submissions processed.
    pub submissions_total: AtomicU64,
    /// Cumulative group commit latency in microseconds (submit → drain).
    pub commit_latency_us_total: AtomicU64,
    /// Total FSYNC_1 (pre-marker) barrier completions.
    pub fsync1_total: AtomicU64,
    /// Total FSYNC_2 (post-marker) barrier completions.
    pub fsync2_total: AtomicU64,
    /// Total first-committer-wins conflict rejections.
    pub fcw_conflicts_total: AtomicU64,
    /// Total SSI conflict rejections.
    pub ssi_conflicts_total: AtomicU64,
    /// Total shutdown rejections.
    pub shutdown_rejections_total: AtomicU64,
}

impl GroupCommitMetrics {
    /// Create a zeroed metrics instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            group_commits_total: AtomicU64::new(0),
            group_commit_size_sum: AtomicU64::new(0),
            submissions_total: AtomicU64::new(0),
            commit_latency_us_total: AtomicU64::new(0),
            fsync1_total: AtomicU64::new(0),
            fsync2_total: AtomicU64::new(0),
            fcw_conflicts_total: AtomicU64::new(0),
            ssi_conflicts_total: AtomicU64::new(0),
            shutdown_rejections_total: AtomicU64::new(0),
        }
    }

    /// Record a group commit flush with the given batch size and latency.
    pub fn record_group_commit(&self, batch_size: u64, latency_us: u64) {
        self.group_commits_total.fetch_add(1, Ordering::Relaxed);
        self.group_commit_size_sum
            .fetch_add(batch_size, Ordering::Relaxed);
        self.commit_latency_us_total
            .fetch_add(latency_us, Ordering::Relaxed);
    }

    /// Record an individual submission.
    pub fn record_submission(&self) {
        self.submissions_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an FSYNC_1 completion.
    pub fn record_fsync1(&self) {
        self.fsync1_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an FSYNC_2 completion.
    pub fn record_fsync2(&self) {
        self.fsync2_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an FCW conflict.
    pub fn record_fcw_conflict(&self) {
        self.fcw_conflicts_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an SSI conflict.
    pub fn record_ssi_conflict(&self) {
        self.ssi_conflicts_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a shutdown rejection.
    pub fn record_shutdown_rejection(&self) {
        self.shutdown_rejections_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Take a consistent snapshot of all counters.
    #[must_use]
    pub fn snapshot(&self) -> GroupCommitMetricsSnapshot {
        GroupCommitMetricsSnapshot {
            group_commits_total: self.group_commits_total.load(Ordering::Relaxed),
            group_commit_size_sum: self.group_commit_size_sum.load(Ordering::Relaxed),
            submissions_total: self.submissions_total.load(Ordering::Relaxed),
            commit_latency_us_total: self.commit_latency_us_total.load(Ordering::Relaxed),
            fsync1_total: self.fsync1_total.load(Ordering::Relaxed),
            fsync2_total: self.fsync2_total.load(Ordering::Relaxed),
            fcw_conflicts_total: self.fcw_conflicts_total.load(Ordering::Relaxed),
            ssi_conflicts_total: self.ssi_conflicts_total.load(Ordering::Relaxed),
            shutdown_rejections_total: self.shutdown_rejections_total.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.group_commits_total.store(0, Ordering::Relaxed);
        self.group_commit_size_sum.store(0, Ordering::Relaxed);
        self.submissions_total.store(0, Ordering::Relaxed);
        self.commit_latency_us_total.store(0, Ordering::Relaxed);
        self.fsync1_total.store(0, Ordering::Relaxed);
        self.fsync2_total.store(0, Ordering::Relaxed);
        self.fcw_conflicts_total.store(0, Ordering::Relaxed);
        self.ssi_conflicts_total.store(0, Ordering::Relaxed);
        self.shutdown_rejections_total.store(0, Ordering::Relaxed);
    }
}

impl Default for GroupCommitMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time snapshot of group commit metrics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GroupCommitMetricsSnapshot {
    pub group_commits_total: u64,
    pub group_commit_size_sum: u64,
    pub submissions_total: u64,
    pub commit_latency_us_total: u64,
    pub fsync1_total: u64,
    pub fsync2_total: u64,
    pub fcw_conflicts_total: u64,
    pub ssi_conflicts_total: u64,
    pub shutdown_rejections_total: u64,
}

impl GroupCommitMetricsSnapshot {
    /// Average group commit batch size, or 0 if no group commits.
    #[must_use]
    pub fn avg_group_size(&self) -> u64 {
        self.group_commit_size_sum
            .checked_div(self.group_commits_total)
            .unwrap_or(0)
    }

    /// Average commit latency in microseconds, or 0 if no group commits.
    #[must_use]
    pub fn avg_commit_latency_us(&self) -> u64 {
        self.commit_latency_us_total
            .checked_div(self.group_commits_total)
            .unwrap_or(0)
    }

    /// Fsync reduction ratio: submissions / (fsync1 + fsync2), or 0 if none.
    /// A value of N means N submissions per fsync operation.
    #[must_use]
    pub fn fsync_reduction_ratio(&self) -> u64 {
        let total_fsyncs = self.fsync1_total + self.fsync2_total;
        self.submissions_total
            .checked_div(total_fsyncs)
            .unwrap_or(0)
    }
}

impl fmt::Display for GroupCommitMetricsSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "group_commits={} size_sum={} submissions={} latency_us={} \
             fsync1={} fsync2={} fcw_conflicts={} ssi_conflicts={} shutdown_rejections={}",
            self.group_commits_total,
            self.group_commit_size_sum,
            self.submissions_total,
            self.commit_latency_us_total,
            self.fsync1_total,
            self.fsync2_total,
            self.fcw_conflicts_total,
            self.ssi_conflicts_total,
            self.shutdown_rejections_total,
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

    // ── WAL FEC repair counters ──

    #[test]
    fn fec_repair_counting() {
        let c = WalFecRepairCounters::new();
        c.record_repair(true, 500);
        c.record_repair(false, 1200);
        c.record_repair(true, 300);
        let snap = c.snapshot();
        assert_eq!(snap.repairs_total, 3);
        assert_eq!(snap.repairs_succeeded, 2);
        assert_eq!(snap.repairs_failed, 1);
        assert_eq!(snap.repair_duration_us_total, 2000);
        assert_eq!(snap.avg_repair_duration_us(), 666);
    }

    #[test]
    fn fec_repair_avg_zero() {
        let c = WalFecRepairCounters::new();
        assert_eq!(c.snapshot().avg_repair_duration_us(), 0);
    }

    #[test]
    fn fec_encode_ops() {
        let c = WalFecRepairCounters::new();
        c.record_encode();
        c.record_encode();
        assert_eq!(c.snapshot().encode_ops, 2);
    }

    #[test]
    fn fec_repair_reset() {
        let c = WalFecRepairCounters::new();
        c.record_repair(true, 100);
        c.record_encode();
        c.reset();
        let snap = c.snapshot();
        assert_eq!(snap.repairs_total, 0);
        assert_eq!(snap.repairs_succeeded, 0);
        assert_eq!(snap.repairs_failed, 0);
        assert_eq!(snap.repair_duration_us_total, 0);
        assert_eq!(snap.encode_ops, 0);
    }

    #[test]
    fn fec_repair_display() {
        let c = WalFecRepairCounters::new();
        c.record_repair(true, 800);
        c.record_encode();
        let s = c.snapshot().to_string();
        assert!(s.contains("wal_fec_repairs=1"));
        assert!(s.contains("succeeded=1"));
        assert!(s.contains("failed=0"));
        assert!(s.contains("repair_duration_us=800"));
        assert!(s.contains("encode_ops=1"));
    }

    #[test]
    fn fec_repair_default() {
        let c = WalFecRepairCounters::default();
        assert_eq!(c.snapshot().repairs_total, 0);
    }

    // ── WAL recovery counters ──

    #[test]
    fn recovery_counting() {
        let r = WalRecoveryCounters::new();
        r.record_recovery(100, 3, 2);
        r.record_recovery(50, 1, 1);
        let snap = r.snapshot();
        assert_eq!(snap.recovery_ops_total, 2);
        assert_eq!(snap.recovery_frames_total, 150);
        assert_eq!(snap.corruption_detected_total, 4);
        assert_eq!(snap.frames_repaired_total, 3);
    }

    #[test]
    fn recovery_reset() {
        let r = WalRecoveryCounters::new();
        r.record_recovery(10, 1, 1);
        r.reset();
        let snap = r.snapshot();
        assert_eq!(snap.recovery_ops_total, 0);
        assert_eq!(snap.recovery_frames_total, 0);
        assert_eq!(snap.corruption_detected_total, 0);
        assert_eq!(snap.frames_repaired_total, 0);
    }

    #[test]
    fn recovery_display() {
        let r = WalRecoveryCounters::new();
        r.record_recovery(20, 2, 1);
        let s = r.snapshot().to_string();
        assert!(s.contains("wal_recovery_frames=20"));
        assert!(s.contains("corruption_detected=2"));
        assert!(s.contains("frames_repaired=1"));
        assert!(s.contains("recovery_ops=1"));
    }

    #[test]
    fn recovery_default() {
        let r = WalRecoveryCounters::default();
        assert_eq!(r.snapshot().recovery_frames_total, 0);
    }
}
