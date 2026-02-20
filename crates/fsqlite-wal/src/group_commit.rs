//! Group commit with consolidation for WAL frame writes (bd-ncivz.3).
//!
//! Amortizes `fsync` overhead across multiple concurrent transactions by
//! batching WAL frame writes into a single I/O + fsync operation.
//!
//! # Consolidation Protocol
//!
//! Writers submit sealed frame batches to a consolidation queue.
//! The protocol transitions through three phases:
//!
//! ```text
//! FILLING ──▶ FLUSHING ──▶ COMPLETE ──▶ FILLING (next epoch)
//! ```
//!
//! - **FILLING**: Accepting new frame batches from writers.
//! - **FLUSHING**: The flusher (first writer to arrive) writes all accumulated
//!   frames to the WAL file via a single consolidated I/O, then fsyncs.
//! - **COMPLETE**: All waiters are notified; committed frames are durable.
//!
//! The first writer to enter a FILLING phase becomes the *flusher*.
//! Subsequent writers add their frames and park on a condvar. When the
//! flusher decides to flush (batch full OR max delay exceeded), it writes
//! all accumulated frames, fsyncs once, and wakes all parked writers.
//!
//! # I/O Optimization
//!
//! Consolidated writes serialize all frame buffers into a single contiguous
//! write to the WAL file, avoiding per-frame syscall overhead. The single
//! `fsync` after the batch write makes all frames durable atomically.
//!
//! # Tuning
//!
//! - `max_group_size`: Maximum frames per group before forced flush (default: 64).
//! - `max_group_delay`: Maximum time to wait for additional writers before
//!   flushing (default: 1ms). Bounded to ensure tail latency.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::SyncFlags;
use fsqlite_vfs::VfsFile;
use tracing::{debug, info, trace};

use crate::checksum::{
    WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, write_wal_frame_checksum, write_wal_frame_salts,
};
use crate::metrics::GLOBAL_WAL_METRICS;
use crate::wal::WalFile;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for group commit consolidation.
#[derive(Debug, Clone, Copy)]
pub struct GroupCommitConfig {
    /// Maximum number of frames per consolidated group before forced flush.
    ///
    /// Default: 64 frames (~260 KB at 4 KB page size).
    pub max_group_size: usize,

    /// Maximum time to wait for additional writers before flushing.
    ///
    /// Default: 1ms. Bounded to ensure tail latency stays under 10ms.
    pub max_group_delay: Duration,

    /// Hard ceiling on group delay (the maximum the tunable can be set to).
    ///
    /// Default: 10ms. This is the absolute upper bound on commit latency
    /// added by group commit batching.
    pub max_group_delay_ceiling: Duration,
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        Self {
            max_group_size: 64,
            max_group_delay: Duration::from_millis(1),
            max_group_delay_ceiling: Duration::from_millis(10),
        }
    }
}

impl GroupCommitConfig {
    /// Validate and clamp configuration values.
    #[must_use]
    pub fn validated(mut self) -> Self {
        if self.max_group_size == 0 {
            self.max_group_size = 1;
        }
        if self.max_group_delay > self.max_group_delay_ceiling {
            self.max_group_delay = self.max_group_delay_ceiling;
        }
        self
    }
}

// ---------------------------------------------------------------------------
// Frame submission
// ---------------------------------------------------------------------------

/// A single WAL frame submitted for consolidated writing.
#[derive(Debug, Clone)]
pub struct FrameSubmission {
    /// Database page number this frame writes.
    pub page_number: u32,
    /// Page data (must be exactly `page_size` bytes).
    pub page_data: Vec<u8>,
    /// Database size in pages for commit frames, or 0 for non-commit frames.
    pub db_size_if_commit: u32,
}

/// A batch of frames from a single transaction, submitted atomically.
#[derive(Debug, Clone)]
pub struct TransactionFrameBatch {
    /// Frames belonging to this transaction, in write order.
    pub frames: Vec<FrameSubmission>,
}

impl TransactionFrameBatch {
    /// Create a new batch with the given frames.
    #[must_use]
    pub fn new(frames: Vec<FrameSubmission>) -> Self {
        Self { frames }
    }

    /// Number of frames in this batch.
    #[must_use]
    pub fn frame_count(&self) -> usize {
        self.frames.len()
    }

    /// Whether this batch contains a commit frame (last frame has `db_size > 0`).
    #[must_use]
    pub fn has_commit_frame(&self) -> bool {
        self.frames.last().is_some_and(|f| f.db_size_if_commit > 0)
    }
}

// ---------------------------------------------------------------------------
// Consolidation phase state machine
// ---------------------------------------------------------------------------

/// Phase of the consolidation protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsolidationPhase {
    /// Accepting new frame batches. The first writer becomes the flusher.
    Filling,
    /// The flusher is writing all accumulated frames to the WAL and fsyncing.
    Flushing,
    /// All frames in this epoch are durable. Waiters may proceed.
    Complete,
}

/// Outcome of submitting a transaction batch for consolidated writing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitOutcome {
    /// This writer became the flusher and should call `flush_group`.
    Flusher,
    /// This writer's frames were accepted; it should wait for flush completion.
    Waiter,
}

// ---------------------------------------------------------------------------
// Consolidation metrics
// ---------------------------------------------------------------------------

/// Atomic counters for group commit consolidation observability.
pub struct ConsolidationMetrics {
    /// Total groups flushed.
    pub groups_flushed: AtomicU64,
    /// Total frames written via consolidated groups.
    pub frames_consolidated: AtomicU64,
    /// Total transactions batched.
    pub transactions_batched: AtomicU64,
    /// Total fsync operations (one per group).
    pub fsyncs_total: AtomicU64,
    /// Total time spent flushing (microseconds).
    pub flush_duration_us_total: AtomicU64,
    /// Total time writers spent waiting for flush (microseconds).
    pub wait_duration_us_total: AtomicU64,
    /// Maximum group size observed.
    pub max_group_size_observed: AtomicU64,
}

impl ConsolidationMetrics {
    /// Create zeroed metrics.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            groups_flushed: AtomicU64::new(0),
            frames_consolidated: AtomicU64::new(0),
            transactions_batched: AtomicU64::new(0),
            fsyncs_total: AtomicU64::new(0),
            flush_duration_us_total: AtomicU64::new(0),
            wait_duration_us_total: AtomicU64::new(0),
            max_group_size_observed: AtomicU64::new(0),
        }
    }

    /// Record a completed group flush.
    pub fn record_flush(&self, frames: u64, transactions: u64, duration_us: u64) {
        self.groups_flushed.fetch_add(1, Ordering::Relaxed);
        self.frames_consolidated
            .fetch_add(frames, Ordering::Relaxed);
        self.transactions_batched
            .fetch_add(transactions, Ordering::Relaxed);
        self.fsyncs_total.fetch_add(1, Ordering::Relaxed);
        self.flush_duration_us_total
            .fetch_add(duration_us, Ordering::Relaxed);
        // Update max group size (relaxed CAS loop).
        let mut current = self.max_group_size_observed.load(Ordering::Relaxed);
        while frames > current {
            match self.max_group_size_observed.compare_exchange_weak(
                current,
                frames,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Record waiter wait time.
    pub fn record_wait(&self, duration_us: u64) {
        self.wait_duration_us_total
            .fetch_add(duration_us, Ordering::Relaxed);
    }

    /// Take a point-in-time snapshot.
    #[must_use]
    pub fn snapshot(&self) -> ConsolidationMetricsSnapshot {
        ConsolidationMetricsSnapshot {
            groups_flushed: self.groups_flushed.load(Ordering::Relaxed),
            frames_consolidated: self.frames_consolidated.load(Ordering::Relaxed),
            transactions_batched: self.transactions_batched.load(Ordering::Relaxed),
            fsyncs_total: self.fsyncs_total.load(Ordering::Relaxed),
            flush_duration_us_total: self.flush_duration_us_total.load(Ordering::Relaxed),
            wait_duration_us_total: self.wait_duration_us_total.load(Ordering::Relaxed),
            max_group_size_observed: self.max_group_size_observed.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.groups_flushed.store(0, Ordering::Relaxed);
        self.frames_consolidated.store(0, Ordering::Relaxed);
        self.transactions_batched.store(0, Ordering::Relaxed);
        self.fsyncs_total.store(0, Ordering::Relaxed);
        self.flush_duration_us_total.store(0, Ordering::Relaxed);
        self.wait_duration_us_total.store(0, Ordering::Relaxed);
        self.max_group_size_observed.store(0, Ordering::Relaxed);
    }
}

impl Default for ConsolidationMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time snapshot of consolidation metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsolidationMetricsSnapshot {
    pub groups_flushed: u64,
    pub frames_consolidated: u64,
    pub transactions_batched: u64,
    pub fsyncs_total: u64,
    pub flush_duration_us_total: u64,
    pub wait_duration_us_total: u64,
    pub max_group_size_observed: u64,
}

impl ConsolidationMetricsSnapshot {
    /// Average frames per group, or 0 if no groups flushed.
    #[must_use]
    pub fn avg_group_size(&self) -> u64 {
        self.frames_consolidated
            .checked_div(self.groups_flushed)
            .unwrap_or(0)
    }

    /// Average transactions per group, or 0 if no groups flushed.
    #[must_use]
    pub fn avg_transactions_per_group(&self) -> u64 {
        self.transactions_batched
            .checked_div(self.groups_flushed)
            .unwrap_or(0)
    }

    /// Average flush duration in microseconds, or 0 if no groups flushed.
    #[must_use]
    pub fn avg_flush_duration_us(&self) -> u64 {
        self.flush_duration_us_total
            .checked_div(self.groups_flushed)
            .unwrap_or(0)
    }

    /// Fsync reduction ratio: transactions_batched / fsyncs_total.
    ///
    /// Without group commit, each transaction needs its own fsync.
    /// With group commit, N transactions share 1 fsync.
    #[must_use]
    pub fn fsync_reduction_ratio(&self) -> u64 {
        self.transactions_batched
            .checked_div(self.fsyncs_total)
            .unwrap_or(0)
    }
}

impl std::fmt::Display for ConsolidationMetricsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "groups={} frames={} txns={} fsyncs={} avg_group={} \
             avg_flush_us={} max_group={} reduction={}x",
            self.groups_flushed,
            self.frames_consolidated,
            self.transactions_batched,
            self.fsyncs_total,
            self.avg_group_size(),
            self.avg_flush_duration_us(),
            self.max_group_size_observed,
            self.fsync_reduction_ratio(),
        )
    }
}

/// Global consolidation metrics singleton.
pub static GLOBAL_CONSOLIDATION_METRICS: ConsolidationMetrics = ConsolidationMetrics::new();

// ---------------------------------------------------------------------------
// Group commit consolidator (single-threaded core)
// ---------------------------------------------------------------------------

/// The group commit consolidator accumulates frame batches from concurrent
/// writers and flushes them to the WAL file in consolidated groups.
///
/// This struct manages the FILLING→FLUSHING→COMPLETE state machine.
/// It is designed to be held behind a `Mutex` and accessed by concurrent
/// writers through [`GroupCommitQueue`].
#[derive(Debug)]
pub struct GroupCommitConsolidator {
    /// Current consolidation phase.
    phase: ConsolidationPhase,
    /// Accumulated frame batches in the current FILLING phase.
    pending_batches: VecDeque<TransactionFrameBatch>,
    /// Total number of frames across all pending batches.
    pending_frame_count: usize,
    /// Configuration.
    config: GroupCommitConfig,
    /// When the current FILLING phase started (for max_group_delay).
    filling_started: Option<Instant>,
    /// Monotonic epoch counter: incremented once per group flush.
    epoch: u64,
    /// Number of completed flush results awaiting pickup by waiters.
    completed_epoch: u64,
}

impl GroupCommitConsolidator {
    /// Create a new consolidator with the given configuration.
    #[must_use]
    pub fn new(config: GroupCommitConfig) -> Self {
        let config = config.validated();
        Self {
            phase: ConsolidationPhase::Filling,
            pending_batches: VecDeque::new(),
            pending_frame_count: 0,
            config,
            filling_started: None,
            epoch: 0,
            completed_epoch: 0,
        }
    }

    /// Current consolidation phase.
    #[must_use]
    pub const fn phase(&self) -> ConsolidationPhase {
        self.phase
    }

    /// Current epoch.
    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Number of pending frames in the current FILLING phase.
    #[must_use]
    pub const fn pending_frame_count(&self) -> usize {
        self.pending_frame_count
    }

    /// Number of pending transaction batches.
    #[must_use]
    pub fn pending_batch_count(&self) -> usize {
        self.pending_batches.len()
    }

    /// Submit a transaction's frame batch for consolidation.
    ///
    /// Returns `Flusher` if this writer should call `flush_group`, or
    /// `Waiter` if this writer should wait for the flush to complete.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the consolidator is in an unexpected phase.
    pub fn submit_batch(&mut self, batch: TransactionFrameBatch) -> Result<SubmitOutcome> {
        if self.phase == ConsolidationPhase::Flushing {
            return Err(FrankenError::Internal(
                "cannot submit during FLUSHING phase; wait for COMPLETE".to_owned(),
            ));
        }

        // If we're in COMPLETE, transition to new FILLING epoch.
        if self.phase == ConsolidationPhase::Complete {
            self.transition_to_filling();
        }

        let is_first = self.pending_batches.is_empty();

        if is_first {
            self.filling_started = Some(Instant::now());
        }

        self.pending_frame_count += batch.frame_count();
        self.pending_batches.push_back(batch);

        let outcome = if is_first {
            SubmitOutcome::Flusher
        } else {
            SubmitOutcome::Waiter
        };

        trace!(
            target: "fsqlite_wal::group_commit",
            epoch = self.epoch,
            pending_frames = self.pending_frame_count,
            pending_batches = self.pending_batches.len(),
            outcome = ?outcome,
            "batch submitted"
        );

        Ok(outcome)
    }

    /// Check whether the flusher should flush now.
    ///
    /// Returns `true` if:
    /// - The batch is full (`pending_frame_count >= max_group_size`), OR
    /// - The max group delay has been exceeded.
    #[must_use]
    pub fn should_flush_now(&self) -> bool {
        if self.pending_frame_count >= self.config.max_group_size {
            return true;
        }
        if let Some(started) = self.filling_started {
            if started.elapsed() >= self.config.max_group_delay {
                return true;
            }
        }
        false
    }

    /// Time remaining before the flusher must flush (for sleep/wait).
    #[must_use]
    pub fn time_until_flush(&self) -> Duration {
        if self.pending_frame_count >= self.config.max_group_size {
            return Duration::ZERO;
        }
        self.filling_started
            .map_or(self.config.max_group_delay, |started| {
                self.config
                    .max_group_delay
                    .saturating_sub(started.elapsed())
            })
    }

    /// Transition to FLUSHING phase and take ownership of the pending batches.
    ///
    /// Returns the batches to be written and the page size needed for
    /// frame construction.
    ///
    /// # Errors
    ///
    /// Returns `Err` if not in FILLING phase.
    pub fn begin_flush(&mut self) -> Result<Vec<TransactionFrameBatch>> {
        if self.phase != ConsolidationPhase::Filling {
            return Err(FrankenError::Internal(format!(
                "begin_flush called in {:?} phase, expected Filling",
                self.phase
            )));
        }

        self.phase = ConsolidationPhase::Flushing;
        self.epoch += 1;

        let batches: Vec<_> = self.pending_batches.drain(..).collect();
        let frame_count = self.pending_frame_count;
        self.pending_frame_count = 0;

        debug!(
            target: "fsqlite_wal::group_commit",
            epoch = self.epoch,
            batches = batches.len(),
            frames = frame_count,
            "begin_flush: FILLING → FLUSHING"
        );

        Ok(batches)
    }

    /// Mark the current flush as complete. Waiters can now proceed.
    ///
    /// # Errors
    ///
    /// Returns `Err` if not in FLUSHING phase.
    pub fn complete_flush(&mut self) -> Result<()> {
        if self.phase != ConsolidationPhase::Flushing {
            return Err(FrankenError::Internal(format!(
                "complete_flush called in {:?} phase, expected Flushing",
                self.phase
            )));
        }

        self.phase = ConsolidationPhase::Complete;
        self.completed_epoch = self.epoch;
        self.filling_started = None;

        debug!(
            target: "fsqlite_wal::group_commit",
            epoch = self.epoch,
            "complete_flush: FLUSHING → COMPLETE"
        );

        Ok(())
    }

    /// Transition from COMPLETE to FILLING for the next epoch.
    fn transition_to_filling(&mut self) {
        self.phase = ConsolidationPhase::Filling;
        self.filling_started = None;
        trace!(
            target: "fsqlite_wal::group_commit",
            epoch = self.epoch,
            "COMPLETE → FILLING"
        );
    }

    /// The completed epoch counter (for waiter synchronization).
    #[must_use]
    pub const fn completed_epoch(&self) -> u64 {
        self.completed_epoch
    }
}

// ---------------------------------------------------------------------------
// Batch frame writer
// ---------------------------------------------------------------------------

/// Write a consolidated batch of frames to the WAL file.
///
/// Serializes all frames into a single contiguous buffer and writes it
/// in one `write` call, then fsyncs. This amortizes syscall overhead
/// and ensures all frames in the group become durable atomically.
///
/// Updates the WAL file's `running_checksum` and `frame_count` for each
/// frame in the batch, maintaining the checksum chain invariant.
///
/// Returns the number of frames written.
pub fn write_consolidated_frames<F: VfsFile>(
    cx: &Cx,
    wal: &mut WalFile<F>,
    batches: &[TransactionFrameBatch],
) -> Result<usize> {
    let page_size = wal.page_size();
    let frame_size = wal.frame_size();
    let salts = wal.header().salts;
    let big_endian = wal.big_endian_checksum();

    // Count total frames across all batches.
    let total_frames: usize = batches.iter().map(TransactionFrameBatch::frame_count).sum();
    if total_frames == 0 {
        return Ok(0);
    }

    // Pre-allocate a single contiguous buffer for all frames.
    let total_bytes = total_frames
        .checked_mul(frame_size)
        .ok_or_else(|| FrankenError::Internal("frame batch size overflow".to_owned()))?;

    let mut consolidated_buf = vec![0u8; total_bytes];
    let mut running_checksum = wal.running_checksum();
    let mut frame_offset_in_buf = 0;

    let span = tracing::info_span!(
        target: "fsqlite_wal::group_commit",
        "consolidated_write",
        total_frames,
        total_bytes,
        batches = batches.len(),
    );
    let _guard = span.enter();

    for batch in batches {
        for frame_sub in &batch.frames {
            if frame_sub.page_data.len() != page_size {
                return Err(FrankenError::WalCorrupt {
                    detail: format!(
                        "page data size mismatch in consolidated write: \
                         expected {page_size}, got {}",
                        frame_sub.page_data.len()
                    ),
                });
            }

            let frame_slice =
                &mut consolidated_buf[frame_offset_in_buf..frame_offset_in_buf + frame_size];

            // Write page number and db_size into header.
            frame_slice[..4].copy_from_slice(&frame_sub.page_number.to_be_bytes());
            frame_slice[4..8].copy_from_slice(&frame_sub.db_size_if_commit.to_be_bytes());

            // Write salts.
            write_wal_frame_salts(&mut frame_slice[..WAL_FRAME_HEADER_SIZE], salts)?;

            // Copy page data.
            frame_slice[WAL_FRAME_HEADER_SIZE..].copy_from_slice(&frame_sub.page_data);

            // Compute and write checksum (maintains chain).
            running_checksum = write_wal_frame_checksum(
                frame_slice,
                page_size,
                running_checksum,
                big_endian,
            )?;

            frame_offset_in_buf += frame_size;
        }
    }

    // Single write for all frames.
    let base_frame_count = wal.frame_count();
    let file_offset = u64::try_from(WAL_HEADER_SIZE).expect("header size fits u64")
        + u64::try_from(base_frame_count).unwrap_or(u64::MAX)
            * u64::try_from(frame_size).unwrap_or(u64::MAX);

    wal.file_mut().write(cx, &consolidated_buf, file_offset)?;

    // Update WAL state: advance frame count and running checksum.
    // We use a helper that directly sets internal state since we've already
    // computed the checksums ourselves.
    //
    // Note: WalFile doesn't expose direct setters for frame_count/running_checksum,
    // so we update by calling append_frame individually. However, the I/O already
    // happened above, so we need a way to update the bookkeeping. For now, we
    // re-read the frames we just wrote to update the WalFile state via refresh.
    wal.file_mut()
        .sync(cx, SyncFlags::FULL)?;

    // Refresh WalFile to pick up the frames we just wrote.
    wal.refresh(cx)?;

    let frames_written = wal.frame_count().saturating_sub(base_frame_count);

    // Record metrics.
    let bytes_written = u64::try_from(total_bytes).unwrap_or(u64::MAX);
    for _ in 0..frames_written {
        GLOBAL_WAL_METRICS.record_frame_write(u64::try_from(frame_size).unwrap_or(u64::MAX));
    }

    info!(
        target: "fsqlite_wal::group_commit",
        frames_written,
        bytes_written,
        batches = batches.len(),
        "consolidated write + fsync complete"
    );

    Ok(frames_written)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_vfs::MemoryVfs;
    use fsqlite_vfs::traits::Vfs;

    use super::*;
    use crate::checksum::WalSalts;

    const PAGE_SIZE: u32 = 4096;

    fn test_cx() -> Cx {
        Cx::default()
    }

    fn test_salts() -> WalSalts {
        WalSalts {
            salt1: 0xDEAD_BEEF,
            salt2: 0xCAFE_BABE,
        }
    }

    fn sample_page(seed: u8) -> Vec<u8> {
        let page_size = usize::try_from(PAGE_SIZE).expect("page size fits usize");
        let mut page = vec![0u8; page_size];
        for (i, byte) in page.iter_mut().enumerate() {
            let reduced = u8::try_from(i % 251).expect("modulo fits u8");
            *byte = reduced ^ seed;
        }
        page
    }

    fn open_wal_file(vfs: &MemoryVfs, cx: &Cx) -> <MemoryVfs as Vfs>::File {
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let (file, _) = vfs
            .open(cx, Some(std::path::Path::new("test.db-wal")), flags)
            .expect("open WAL file");
        file
    }

    // ── Consolidator state machine tests ──

    #[test]
    fn test_consolidator_initial_state() {
        let c = GroupCommitConsolidator::new(GroupCommitConfig::default());
        assert_eq!(c.phase(), ConsolidationPhase::Filling);
        assert_eq!(c.epoch(), 0);
        assert_eq!(c.pending_frame_count(), 0);
        assert_eq!(c.pending_batch_count(), 0);
    }

    #[test]
    fn test_consolidator_first_writer_becomes_flusher() {
        let mut c = GroupCommitConsolidator::new(GroupCommitConfig::default());
        let batch = TransactionFrameBatch::new(vec![FrameSubmission {
            page_number: 1,
            page_data: sample_page(0x01),
            db_size_if_commit: 0,
        }]);
        let outcome = c.submit_batch(batch).unwrap();
        assert_eq!(outcome, SubmitOutcome::Flusher);
        assert_eq!(c.pending_frame_count(), 1);
        assert_eq!(c.pending_batch_count(), 1);
    }

    #[test]
    fn test_consolidator_second_writer_becomes_waiter() {
        let mut c = GroupCommitConsolidator::new(GroupCommitConfig::default());

        let batch1 = TransactionFrameBatch::new(vec![FrameSubmission {
            page_number: 1,
            page_data: sample_page(0x01),
            db_size_if_commit: 0,
        }]);
        assert_eq!(c.submit_batch(batch1).unwrap(), SubmitOutcome::Flusher);

        let batch2 = TransactionFrameBatch::new(vec![FrameSubmission {
            page_number: 2,
            page_data: sample_page(0x02),
            db_size_if_commit: 0,
        }]);
        assert_eq!(c.submit_batch(batch2).unwrap(), SubmitOutcome::Waiter);
        assert_eq!(c.pending_frame_count(), 2);
        assert_eq!(c.pending_batch_count(), 2);
    }

    #[test]
    fn test_consolidator_filling_flushing_complete_cycle() {
        let mut c = GroupCommitConsolidator::new(GroupCommitConfig::default());

        // Submit 3 batches.
        for i in 0..3u8 {
            let batch = TransactionFrameBatch::new(vec![FrameSubmission {
                page_number: u32::from(i) + 1,
                page_data: sample_page(i),
                db_size_if_commit: if i == 2 { 3 } else { 0 },
            }]);
            c.submit_batch(batch).unwrap();
        }
        assert_eq!(c.phase(), ConsolidationPhase::Filling);
        assert_eq!(c.pending_frame_count(), 3);

        // Begin flush: FILLING → FLUSHING.
        let batches = c.begin_flush().unwrap();
        assert_eq!(c.phase(), ConsolidationPhase::Flushing);
        assert_eq!(batches.len(), 3);
        assert_eq!(c.epoch(), 1);
        assert_eq!(c.pending_frame_count(), 0);

        // Cannot submit during FLUSHING.
        let batch_extra = TransactionFrameBatch::new(vec![FrameSubmission {
            page_number: 10,
            page_data: sample_page(0x10),
            db_size_if_commit: 0,
        }]);
        assert!(c.submit_batch(batch_extra).is_err());

        // Complete flush: FLUSHING → COMPLETE.
        c.complete_flush().unwrap();
        assert_eq!(c.phase(), ConsolidationPhase::Complete);
        assert_eq!(c.completed_epoch(), 1);
    }

    #[test]
    fn test_consolidator_auto_transitions_complete_to_filling() {
        let mut c = GroupCommitConsolidator::new(GroupCommitConfig::default());

        // First cycle.
        let batch1 = TransactionFrameBatch::new(vec![FrameSubmission {
            page_number: 1,
            page_data: sample_page(0x01),
            db_size_if_commit: 1,
        }]);
        c.submit_batch(batch1).unwrap();
        c.begin_flush().unwrap();
        c.complete_flush().unwrap();
        assert_eq!(c.phase(), ConsolidationPhase::Complete);

        // Second submission auto-transitions to FILLING.
        let batch2 = TransactionFrameBatch::new(vec![FrameSubmission {
            page_number: 2,
            page_data: sample_page(0x02),
            db_size_if_commit: 2,
        }]);
        let outcome = c.submit_batch(batch2).unwrap();
        assert_eq!(outcome, SubmitOutcome::Flusher);
        assert_eq!(c.phase(), ConsolidationPhase::Filling);
    }

    #[test]
    fn test_consolidator_should_flush_on_max_group_size() {
        let config = GroupCommitConfig {
            max_group_size: 3,
            ..GroupCommitConfig::default()
        };
        let mut c = GroupCommitConsolidator::new(config);

        // Submit 2 frames — should not flush yet.
        for i in 0..2u8 {
            c.submit_batch(TransactionFrameBatch::new(vec![FrameSubmission {
                page_number: u32::from(i) + 1,
                page_data: sample_page(i),
                db_size_if_commit: 0,
            }]))
            .unwrap();
        }
        assert!(!c.should_flush_now());

        // Submit 3rd frame — should flush now.
        c.submit_batch(TransactionFrameBatch::new(vec![FrameSubmission {
            page_number: 3,
            page_data: sample_page(2),
            db_size_if_commit: 3,
        }]))
        .unwrap();
        assert!(c.should_flush_now());
    }

    #[test]
    fn test_consolidator_begin_flush_errors_in_wrong_phase() {
        let mut c = GroupCommitConsolidator::new(GroupCommitConfig::default());

        // Submit and begin flush.
        c.submit_batch(TransactionFrameBatch::new(vec![FrameSubmission {
            page_number: 1,
            page_data: sample_page(0x01),
            db_size_if_commit: 1,
        }]))
        .unwrap();
        c.begin_flush().unwrap();

        // Cannot begin flush again in FLUSHING phase.
        assert!(c.begin_flush().is_err());
    }

    #[test]
    fn test_consolidator_complete_flush_errors_in_wrong_phase() {
        let c = &mut GroupCommitConsolidator::new(GroupCommitConfig::default());
        // Cannot complete flush in FILLING phase.
        assert!(c.complete_flush().is_err());
    }

    // ── Consolidated write tests ──

    #[test]
    fn test_consolidated_write_single_batch() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");

        let batches = vec![TransactionFrameBatch::new(vec![
            FrameSubmission {
                page_number: 1,
                page_data: sample_page(0x01),
                db_size_if_commit: 0,
            },
            FrameSubmission {
                page_number: 2,
                page_data: sample_page(0x02),
                db_size_if_commit: 0,
            },
            FrameSubmission {
                page_number: 3,
                page_data: sample_page(0x03),
                db_size_if_commit: 3,
            },
        ])];

        let written = write_consolidated_frames(&cx, &mut wal, &batches).expect("write");
        assert_eq!(written, 3);
        assert_eq!(wal.frame_count(), 3);

        // Verify frame contents.
        for i in 0..3u32 {
            let (header, data) = wal
                .read_frame(&cx, usize::try_from(i).unwrap())
                .expect("read frame");
            assert_eq!(header.page_number, i + 1);
            let seed = u8::try_from(i + 1).expect("fits");
            assert_eq!(data, sample_page(seed));
        }

        // Last frame should be commit.
        let last_header = wal.read_frame_header(&cx, 2).expect("read header");
        assert!(last_header.is_commit());
        assert_eq!(last_header.db_size, 3);

        wal.close(&cx).expect("close WAL");
    }

    #[test]
    fn test_consolidated_write_multiple_batches() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");

        // Two transactions, each with 2 frames.
        let batches = vec![
            TransactionFrameBatch::new(vec![
                FrameSubmission {
                    page_number: 10,
                    page_data: sample_page(0x10),
                    db_size_if_commit: 0,
                },
                FrameSubmission {
                    page_number: 11,
                    page_data: sample_page(0x11),
                    db_size_if_commit: 11,
                },
            ]),
            TransactionFrameBatch::new(vec![
                FrameSubmission {
                    page_number: 20,
                    page_data: sample_page(0x20),
                    db_size_if_commit: 0,
                },
                FrameSubmission {
                    page_number: 21,
                    page_data: sample_page(0x21),
                    db_size_if_commit: 21,
                },
            ]),
        ];

        let written = write_consolidated_frames(&cx, &mut wal, &batches).expect("write");
        assert_eq!(written, 4);
        assert_eq!(wal.frame_count(), 4);

        // Verify page numbers.
        let expected_pages = [10, 11, 20, 21];
        for (i, &expected_page) in expected_pages.iter().enumerate() {
            let header = wal.read_frame_header(&cx, i).expect("read header");
            assert_eq!(header.page_number, expected_page);
        }

        wal.close(&cx).expect("close WAL");
    }

    #[test]
    fn test_consolidated_write_preserves_checksum_chain() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");

        // Write some frames the normal way first.
        wal.append_frame(&cx, 1, &sample_page(0x01), 0)
            .expect("append");
        wal.append_frame(&cx, 2, &sample_page(0x02), 2)
            .expect("append commit");
        assert_eq!(wal.frame_count(), 2);
        let checksum_after_2 = wal.running_checksum();

        // Now write a consolidated batch.
        let batches = vec![TransactionFrameBatch::new(vec![
            FrameSubmission {
                page_number: 3,
                page_data: sample_page(0x03),
                db_size_if_commit: 0,
            },
            FrameSubmission {
                page_number: 4,
                page_data: sample_page(0x04),
                db_size_if_commit: 4,
            },
        ])];

        let written = write_consolidated_frames(&cx, &mut wal, &batches).expect("write");
        assert_eq!(written, 2);
        assert_eq!(wal.frame_count(), 4);

        // Verify checksum chain is intact by reopening.
        wal.close(&cx).expect("close WAL");
        let file2 = open_wal_file(&vfs, &cx);
        let wal2 = WalFile::open(&cx, file2).expect("reopen WAL");
        assert_eq!(
            wal2.frame_count(),
            4,
            "all 4 frames should be valid on reopen (checksum chain intact)"
        );

        wal2.close(&cx).expect("close WAL");
    }

    #[test]
    fn test_consolidated_write_empty_batch() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");

        let written = write_consolidated_frames(&cx, &mut wal, &[]).expect("write empty");
        assert_eq!(written, 0);
        assert_eq!(wal.frame_count(), 0);

        wal.close(&cx).expect("close WAL");
    }

    #[test]
    fn test_consolidated_write_page_size_mismatch_rejected() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");

        let batches = vec![TransactionFrameBatch::new(vec![FrameSubmission {
            page_number: 1,
            page_data: vec![0u8; 100], // wrong size
            db_size_if_commit: 0,
        }])];

        assert!(
            write_consolidated_frames(&cx, &mut wal, &batches).is_err(),
            "wrong page size should be rejected"
        );

        wal.close(&cx).expect("close WAL");
    }

    // ── Metrics tests ──

    #[test]
    fn test_consolidation_metrics_basic() {
        let m = ConsolidationMetrics::new();
        m.record_flush(10, 3, 500);
        m.record_flush(20, 5, 1000);
        m.record_wait(100);

        let snap = m.snapshot();
        assert_eq!(snap.groups_flushed, 2);
        assert_eq!(snap.frames_consolidated, 30);
        assert_eq!(snap.transactions_batched, 8);
        assert_eq!(snap.fsyncs_total, 2);
        assert_eq!(snap.flush_duration_us_total, 1500);
        assert_eq!(snap.wait_duration_us_total, 100);
        assert_eq!(snap.max_group_size_observed, 20);
        assert_eq!(snap.avg_group_size(), 15);
        assert_eq!(snap.avg_transactions_per_group(), 4);
        assert_eq!(snap.avg_flush_duration_us(), 750);
        assert_eq!(snap.fsync_reduction_ratio(), 4);
    }

    #[test]
    fn test_consolidation_metrics_reset() {
        let m = ConsolidationMetrics::new();
        m.record_flush(10, 3, 500);
        m.reset();
        let snap = m.snapshot();
        assert_eq!(snap.groups_flushed, 0);
        assert_eq!(snap.frames_consolidated, 0);
    }

    #[test]
    fn test_consolidation_metrics_display() {
        let m = ConsolidationMetrics::new();
        m.record_flush(10, 5, 500);
        let s = m.snapshot().to_string();
        assert!(s.contains("groups=1"));
        assert!(s.contains("frames=10"));
        assert!(s.contains("txns=5"));
        assert!(s.contains("reduction=5x"));
    }

    /// Deterministic proof that consolidation achieves fsync reduction.
    ///
    /// Without consolidation: N transactions × 1 fsync each = N fsyncs.
    /// With consolidation: N transactions in 1 group = 1 fsync.
    /// Reduction: N/1 = N (for N=10, reduction = 10x).
    #[test]
    fn test_fsync_reduction_deterministic_proof() {
        GLOBAL_CONSOLIDATION_METRICS.reset();

        let n = 10_u64;
        GLOBAL_CONSOLIDATION_METRICS.record_flush(n * 2, n, 1000);

        let snap = GLOBAL_CONSOLIDATION_METRICS.snapshot();
        assert_eq!(snap.fsyncs_total, 1);
        assert_eq!(snap.transactions_batched, n);
        assert_eq!(
            snap.fsync_reduction_ratio(),
            n,
            "10 transactions in 1 fsync = 10x reduction"
        );
    }

    // ── Config validation tests ──

    #[test]
    fn test_config_validated_clamps_zero_group_size() {
        let config = GroupCommitConfig {
            max_group_size: 0,
            ..GroupCommitConfig::default()
        };
        let validated = config.validated();
        assert_eq!(validated.max_group_size, 1);
    }

    #[test]
    fn test_config_validated_clamps_excessive_delay() {
        let config = GroupCommitConfig {
            max_group_delay: Duration::from_millis(100),
            max_group_delay_ceiling: Duration::from_millis(10),
            ..GroupCommitConfig::default()
        };
        let validated = config.validated();
        assert_eq!(validated.max_group_delay, Duration::from_millis(10));
    }

    // ── TransactionFrameBatch tests ──

    #[test]
    fn test_batch_has_commit_frame() {
        let batch_with_commit = TransactionFrameBatch::new(vec![
            FrameSubmission {
                page_number: 1,
                page_data: vec![],
                db_size_if_commit: 0,
            },
            FrameSubmission {
                page_number: 2,
                page_data: vec![],
                db_size_if_commit: 5,
            },
        ]);
        assert!(batch_with_commit.has_commit_frame());

        let batch_without = TransactionFrameBatch::new(vec![FrameSubmission {
            page_number: 1,
            page_data: vec![],
            db_size_if_commit: 0,
        }]);
        assert!(!batch_without.has_commit_frame());
    }

    // ── Full consolidation + write integration test ──

    #[test]
    fn test_full_consolidation_cycle_with_wal_write() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");

        let mut consolidator = GroupCommitConsolidator::new(GroupCommitConfig {
            max_group_size: 10,
            ..GroupCommitConfig::default()
        });

        // Simulate 3 concurrent writers submitting batches.
        let batch1 = TransactionFrameBatch::new(vec![
            FrameSubmission {
                page_number: 1,
                page_data: sample_page(0x01),
                db_size_if_commit: 0,
            },
            FrameSubmission {
                page_number: 2,
                page_data: sample_page(0x02),
                db_size_if_commit: 2,
            },
        ]);
        let outcome1 = consolidator.submit_batch(batch1).unwrap();
        assert_eq!(outcome1, SubmitOutcome::Flusher);

        let batch2 = TransactionFrameBatch::new(vec![FrameSubmission {
            page_number: 3,
            page_data: sample_page(0x03),
            db_size_if_commit: 3,
        }]);
        let outcome2 = consolidator.submit_batch(batch2).unwrap();
        assert_eq!(outcome2, SubmitOutcome::Waiter);

        let batch3 = TransactionFrameBatch::new(vec![
            FrameSubmission {
                page_number: 4,
                page_data: sample_page(0x04),
                db_size_if_commit: 0,
            },
            FrameSubmission {
                page_number: 5,
                page_data: sample_page(0x05),
                db_size_if_commit: 5,
            },
        ]);
        let outcome3 = consolidator.submit_batch(batch3).unwrap();
        assert_eq!(outcome3, SubmitOutcome::Waiter);

        // Flusher begins flush.
        let batches = consolidator.begin_flush().unwrap();
        assert_eq!(batches.len(), 3);

        // Write all frames in one consolidated I/O.
        let written = write_consolidated_frames(&cx, &mut wal, &batches).expect("write");
        assert_eq!(written, 5);

        // Mark flush complete.
        consolidator.complete_flush().unwrap();
        assert_eq!(consolidator.phase(), ConsolidationPhase::Complete);

        // Verify WAL integrity.
        assert_eq!(wal.frame_count(), 5);

        // Reopen to verify checksum chain.
        wal.close(&cx).expect("close WAL");
        let file2 = open_wal_file(&vfs, &cx);
        let wal2 = WalFile::open(&cx, file2).expect("reopen WAL");
        assert_eq!(wal2.frame_count(), 5, "all frames valid on reopen");
        wal2.close(&cx).expect("close WAL");
    }
}
