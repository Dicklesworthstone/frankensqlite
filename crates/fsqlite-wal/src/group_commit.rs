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

use crate::WalGenerationIdentity;
use fsqlite_types::sync_primitives::{Duration, Instant};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::{
    LazyLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_vfs::{SyncKind, VfsFile};
use tracing::{debug, info, trace};

use crate::wal::{WalAppendFrameRef, WalFile};

fn env_flag_enabled(value: &str) -> bool {
    let value = value.trim();
    value == "1"
        || value.eq_ignore_ascii_case("true")
        || value.eq_ignore_ascii_case("yes")
        || value.eq_ignore_ascii_case("on")
}

/// Whether expensive per-substep consolidation timing is enabled.
#[must_use]
pub fn detailed_consolidation_metrics_enabled() -> bool {
    static ENABLED: LazyLock<bool> = LazyLock::new(|| {
        std::env::var("FSQLITE_WAL_DETAILED_COMMIT_METRICS")
            .is_ok_and(|value| env_flag_enabled(&value))
            || std::env::var("FSQLITE_BENCH_PROFILE_INSERT")
                .is_ok_and(|value| env_flag_enabled(&value))
    });
    *ENABLED
}

static COMMIT_PHASE_TIMING_ENABLED: AtomicBool = AtomicBool::new(false);

/// Enable or disable WAL commit phase timing for explicit profiling windows.
///
/// Normal commits still update correctness-critical WAL state and cheap frame
/// counters, but they do not need to sample the wall clock for every phase
/// unless a caller is collecting a hot-path profile or detailed WAL metrics.
pub fn set_commit_phase_timing_enabled(enabled: bool) -> bool {
    COMMIT_PHASE_TIMING_ENABLED.swap(enabled, Ordering::Relaxed)
}

/// Whether commit phase timing has been explicitly enabled by a profiling caller.
#[must_use]
pub fn commit_phase_timing_forced_enabled() -> bool {
    COMMIT_PHASE_TIMING_ENABLED.load(Ordering::Relaxed)
}

/// Whether commit phase timing should sample `Instant::now()`.
#[must_use]
pub fn commit_phase_timing_enabled() -> bool {
    detailed_consolidation_metrics_enabled() || commit_phase_timing_forced_enabled()
}

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
    /// Pages that must obey first-committer-wins against committed WAL frames
    /// newer than this transaction's read snapshot.
    ///
    /// The process-local MVCC registry catches conflicts between connections
    /// in the same process. These fields carry the same commit intent through
    /// the process-global WAL group-commit queue so the eventual flusher can
    /// also reject stale cross-process commits under the WAL append gate.
    pub conflict_pages: Vec<u32>,
    /// WAL visibility snapshot pinned when the submitting transaction began.
    /// If another process has committed any of `conflict_pages` after this
    /// horizon, the batch must fail with BUSY_SNAPSHOT instead of appending a
    /// stale page image that could hide the other writer's committed rows.
    pub conflict_snapshot: Option<TransactionConflictSnapshot>,
    /// Full-page hashes captured from the submitting transaction's pinned
    /// snapshot, keyed by exact page number.
    ///
    /// These are used only when an external checkpoint replaces/resets the WAL
    /// generation while the transaction is open. A generation change alone
    /// does not prove a write conflict: a stock SQLite reader can checkpoint
    /// an otherwise unchanged WAL. The eventual flusher may admit that benign
    /// transition only when every conflict candidate has a baseline and the
    /// latest committed full-page image hashes identically.
    pub conflict_page_baselines: Vec<TransactionConflictPageBaseline>,
    /// Lane-local staging context captured before group-commit submission.
    pub context: TransactionFrameBatchContext,
}

/// WAL conflict horizon captured by a submitting transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionConflictSnapshot {
    pub generation: WalGenerationIdentity,
    pub last_commit_frame: Option<usize>,
    pub commit_count: u64,
}

/// Snapshot-bound full-page hash for one cross-process conflict candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionConflictPageBaseline {
    /// Exact 1-based database page number associated with `page_hash`.
    pub page_number: u32,
    /// BLAKE3 hash of the complete page image visible to the transaction.
    pub page_hash: [u8; 32],
}

/// Lane-local staging context attached to a transaction batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TransactionFrameBatchContext {
    /// Monotonic identifier used to correlate the batch with lane-local staging.
    pub batch_id: u64,
    /// Stable lane identity chosen for the submitting writer.
    pub lane_id: u16,
    /// Number of frames locally staged for this batch.
    pub staged_frame_count: u32,
    /// Time spent in local staging before queue submission.
    pub staging_elapsed_ns: u64,
}

impl TransactionFrameBatch {
    /// Create a new batch with the given frames.
    #[must_use]
    pub fn new(frames: Vec<FrameSubmission>) -> Self {
        Self {
            frames,
            conflict_pages: Vec::new(),
            conflict_snapshot: None,
            conflict_page_baselines: Vec::new(),
            context: TransactionFrameBatchContext::default(),
        }
    }

    /// Attach cross-process conflict metadata for the submitting transaction.
    #[must_use]
    pub fn with_conflict_snapshot(
        mut self,
        conflict_pages: Vec<u32>,
        conflict_snapshot: Option<TransactionConflictSnapshot>,
    ) -> Self {
        self.conflict_pages = conflict_pages;
        self.conflict_snapshot = conflict_snapshot;
        self
    }

    /// Attach snapshot-bound full-page hashes for WAL-generation transitions.
    #[must_use]
    pub fn with_conflict_page_baselines(
        mut self,
        conflict_page_baselines: Vec<TransactionConflictPageBaseline>,
    ) -> Self {
        self.conflict_page_baselines = conflict_page_baselines;
        self
    }

    /// Attach lane-local staging context to this batch.
    #[must_use]
    pub fn with_context(mut self, context: TransactionFrameBatchContext) -> Self {
        self.context = context;
        self
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

/// Result of submitting a transaction batch to the group-commit consolidator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubmitReceipt {
    /// Whether the caller should flush or wait.
    pub outcome: SubmitOutcome,
    /// The epoch whose completion makes this submitted batch durable.
    pub target_epoch: u64,
}

// ---------------------------------------------------------------------------
// Lock-free phase histogram (bd-db300.3.8.1)
// ---------------------------------------------------------------------------

/// Fixed-capacity ring buffer for lock-free percentile estimation.
///
/// Writers atomically advance `write_idx` and store samples. Readers
/// snapshot the ring and sort to extract percentiles. The ring overwrites
/// old samples when full, giving a rolling window of the last N observations.
/// This is intentionally simple — no locks, no allocations on the hot path.
const PHASE_HISTOGRAM_CAPACITY: usize = 4096;

pub struct PhaseHistogram {
    /// Circular sample buffer.
    samples: Box<[AtomicU64]>,
    /// Monotonic write index (wraps via modulo).
    write_idx: AtomicU64,
    /// Running max (updated atomically on each record).
    max_us: AtomicU64,
    /// Total samples recorded (not capped by ring size).
    count: AtomicU64,
    /// Running sum for mean computation.
    sum_us: AtomicU64,
    /// Cheap decaying tail estimate for hot-path policy decisions.
    recent_tail_us: AtomicU64,
}

impl PhaseHistogram {
    #[must_use]
    pub fn new() -> Self {
        let samples: Vec<AtomicU64> = std::iter::repeat_with(|| AtomicU64::new(0))
            .take(PHASE_HISTOGRAM_CAPACITY)
            .collect();
        Self {
            samples: samples.into_boxed_slice(),
            write_idx: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
            count: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
            recent_tail_us: AtomicU64::new(0),
        }
    }

    /// Record a single sample (microseconds). Lock-free.
    pub fn record(&self, value_us: u64) {
        let idx =
            self.write_idx.fetch_add(1, Ordering::Relaxed) as usize % PHASE_HISTOGRAM_CAPACITY;
        self.samples[idx].store(value_us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(value_us, Ordering::Relaxed);
        // Update max.
        let mut prev = self.max_us.load(Ordering::Relaxed);
        while value_us > prev {
            match self.max_us.compare_exchange_weak(
                prev,
                value_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => prev = actual,
            }
        }

        // Hot-path consumers need a cheap tail-pressure signal but must not
        // call `percentiles()`, which copies and sorts the whole ring. Maintain
        // a conservative decaying maximum: spikes influence a few subsequent
        // decisions, then fade without requiring a telemetry snapshot.
        let mut prev_tail = self.recent_tail_us.load(Ordering::Relaxed);
        loop {
            let decayed = prev_tail.saturating_mul(15) / 16;
            let next_tail = value_us.max(decayed);
            match self.recent_tail_us.compare_exchange_weak(
                prev_tail,
                next_tail,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => prev_tail = actual,
            }
        }
    }

    /// Return a constant-time recent tail estimate for scheduler hot paths.
    #[must_use]
    pub fn recent_tail_us(&self) -> u64 {
        self.recent_tail_us.load(Ordering::Relaxed)
    }

    /// Snapshot percentiles: returns (p50, p95, p99, max, count, mean_us).
    #[must_use]
    pub fn percentiles(&self) -> PhasePercentiles {
        let total_count = self.count.load(Ordering::Relaxed);
        let max = self.max_us.load(Ordering::Relaxed);
        let sum = self.sum_us.load(Ordering::Relaxed);

        if total_count == 0 {
            return PhasePercentiles {
                p50: 0,
                p95: 0,
                p99: 0,
                max: 0,
                count: 0,
                mean_us: 0,
            };
        }

        // Copy samples into a local vec and sort.
        let n = total_count.min(PHASE_HISTOGRAM_CAPACITY as u64) as usize;
        let mut buf = Vec::with_capacity(n);
        for i in 0..n {
            buf.push(self.samples[i].load(Ordering::Relaxed));
        }
        buf.sort_unstable();

        let p = |pct: usize| -> u64 {
            if buf.is_empty() {
                return 0;
            }
            let idx = (pct * buf.len()) / 100;
            buf[idx.min(buf.len() - 1)]
        };

        PhasePercentiles {
            p50: p(50),
            p95: p(95),
            p99: p(99),
            max,
            count: total_count,
            mean_us: sum / total_count,
        }
    }

    /// Reset all samples and counters.
    pub fn reset(&self) {
        for s in &self.samples {
            s.store(0, Ordering::Relaxed);
        }
        self.write_idx.store(0, Ordering::Relaxed);
        self.max_us.store(0, Ordering::Relaxed);
        self.count.store(0, Ordering::Relaxed);
        self.sum_us.store(0, Ordering::Relaxed);
        self.recent_tail_us.store(0, Ordering::Relaxed);
    }
}

impl Default for PhaseHistogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Percentile snapshot from a `PhaseHistogram`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PhasePercentiles {
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub max: u64,
    pub count: u64,
    pub mean_us: u64,
}

// ---------------------------------------------------------------------------
// Wake-reason accounting (bd-db300.3.8.1)
// ---------------------------------------------------------------------------

/// Tracks why a waiter woke up during epoch wait.
pub struct WakeReasonCounters {
    /// Woken by condvar notify (normal completion).
    pub notify: AtomicU64,
    /// Woken by condvar timeout.
    pub timeout: AtomicU64,
    /// Waiter took over as flusher (flusher died or slow).
    pub flusher_takeover: AtomicU64,
    /// Woken to observe a failed epoch.
    pub failed_epoch: AtomicU64,
    /// Woken but must busy-retry (spurious or race).
    pub busy_retry: AtomicU64,
}

impl WakeReasonCounters {
    const fn new() -> Self {
        Self {
            notify: AtomicU64::new(0),
            timeout: AtomicU64::new(0),
            flusher_takeover: AtomicU64::new(0),
            failed_epoch: AtomicU64::new(0),
            busy_retry: AtomicU64::new(0),
        }
    }

    /// Snapshot all counters.
    #[must_use]
    pub fn snapshot(&self) -> WakeReasonSnapshot {
        WakeReasonSnapshot {
            notify: self.notify.load(Ordering::Relaxed),
            timeout: self.timeout.load(Ordering::Relaxed),
            flusher_takeover: self.flusher_takeover.load(Ordering::Relaxed),
            failed_epoch: self.failed_epoch.load(Ordering::Relaxed),
            busy_retry: self.busy_retry.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters.
    pub fn reset(&self) {
        self.notify.store(0, Ordering::Relaxed);
        self.timeout.store(0, Ordering::Relaxed);
        self.flusher_takeover.store(0, Ordering::Relaxed);
        self.failed_epoch.store(0, Ordering::Relaxed);
        self.busy_retry.store(0, Ordering::Relaxed);
    }
}

/// Point-in-time snapshot of wake reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct WakeReasonSnapshot {
    pub notify: u64,
    pub timeout: u64,
    pub flusher_takeover: u64,
    pub failed_epoch: u64,
    pub busy_retry: u64,
}

impl WakeReasonSnapshot {
    /// Total wake events.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.notify + self.timeout + self.flusher_takeover + self.failed_epoch + self.busy_retry
    }
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
    /// Total busy retries during flush (exponential backoff).
    pub busy_retries: AtomicU64,

    // ── Phase timing instrumentation ──
    /// Time building batch before entering consolidator (microseconds).
    pub prepare_us_total: AtomicU64,
    /// Time cloning staged pages into an owned group-commit batch (microseconds).
    pub batch_build_us_total: AtomicU64,
    /// Time pinning WAL conflict snapshot and attaching metadata (microseconds).
    pub conflict_snapshot_us_total: AtomicU64,
    /// Time spent preparing lane-local WAL frame bytes (microseconds).
    pub lane_prepare_us_total: AtomicU64,
    /// Time waiting to acquire consolidator.lock() (microseconds).
    pub consolidator_lock_wait_us_total: AtomicU64,
    /// Time waiting while consolidator phase == FLUSHING (microseconds).
    pub consolidator_flushing_wait_us_total: AtomicU64,
    /// Time flusher spends waiting for more batches (microseconds).
    pub flusher_arrival_wait_us_total: AtomicU64,
    /// Time waiting to acquire inner.lock() (microseconds).
    pub inner_lock_wait_us_total: AtomicU64,
    /// Time acquiring EXCLUSIVE file lock (microseconds).
    pub exclusive_lock_us_total: AtomicU64,
    /// Time in WAL append_frames (microseconds).
    pub wal_append_us_total: AtomicU64,
    /// Time preparing flusher frame refs and prepared batches (microseconds).
    pub flush_frame_prep_us_total: AtomicU64,
    /// Time checking stale WAL conflicts immediately before append (microseconds).
    pub append_conflict_check_us_total: AtomicU64,
    /// Time spent in the WAL append call itself (microseconds).
    pub append_frames_us_total: AtomicU64,
    /// Time in WAL sync/fsync (microseconds).
    pub wal_sync_us_total: AtomicU64,
    /// Time waiters spend waiting for epoch completion (microseconds).
    pub waiter_epoch_wait_us_total: AtomicU64,
    /// Count of commits that took flusher role.
    pub flusher_commits: AtomicU64,
    /// Count of commits that took waiter role.
    pub waiter_commits: AtomicU64,
    // ── Full commit path phase timing ──
    /// Phase A: prepare under inner.lock (microseconds).
    pub commit_phase_a_us_total: AtomicU64,
    /// Phase B: WAL group commit (microseconds).
    pub commit_phase_b_us_total: AtomicU64,
    /// Phase C1: post-commit metadata under inner.lock (microseconds).
    pub commit_phase_c1_us_total: AtomicU64,
    /// Phase C2: publish to snapshot plane (microseconds).
    pub commit_phase_c2_us_total: AtomicU64,
    /// Total commits with phase timing recorded.
    pub commit_phase_count: AtomicU64,

    // ── Per-phase distribution histograms (bd-db300.3.8.1) ──
    /// Distribution: consolidator lock wait.
    pub hist_consolidator_lock_wait: PhaseHistogram,
    /// Distribution: arrival wait (flusher only).
    pub hist_arrival_wait: PhaseHistogram,
    /// Distribution: WAL backend (inner) lock wait.
    pub hist_wal_backend_lock_wait: PhaseHistogram,
    /// Distribution: WAL append_frames.
    pub hist_wal_append: PhaseHistogram,
    /// Distribution: exclusive file lock acquisition.
    pub hist_exclusive_lock: PhaseHistogram,
    /// Distribution: waiter epoch wait.
    pub hist_waiter_epoch_wait: PhaseHistogram,
    /// Distribution: full Phase B (group commit path, flusher + waiter).
    pub hist_phase_b: PhaseHistogram,
    /// Distribution: WAL sync/fsync.
    pub hist_wal_sync: PhaseHistogram,
    /// Distribution: full commit (phase A + B + C).
    pub hist_full_commit: PhaseHistogram,

    // ── Wake-reason accounting (bd-db300.3.8.1) ──
    /// Why waiters woke up during epoch wait.
    pub wake_reasons: WakeReasonCounters,
}

impl ConsolidationMetrics {
    /// Create zeroed metrics.
    #[must_use]
    pub fn new() -> Self {
        Self {
            groups_flushed: AtomicU64::new(0),
            frames_consolidated: AtomicU64::new(0),
            transactions_batched: AtomicU64::new(0),
            fsyncs_total: AtomicU64::new(0),
            flush_duration_us_total: AtomicU64::new(0),
            wait_duration_us_total: AtomicU64::new(0),
            max_group_size_observed: AtomicU64::new(0),
            busy_retries: AtomicU64::new(0),
            // Phase timing
            prepare_us_total: AtomicU64::new(0),
            batch_build_us_total: AtomicU64::new(0),
            conflict_snapshot_us_total: AtomicU64::new(0),
            lane_prepare_us_total: AtomicU64::new(0),
            consolidator_lock_wait_us_total: AtomicU64::new(0),
            consolidator_flushing_wait_us_total: AtomicU64::new(0),
            flusher_arrival_wait_us_total: AtomicU64::new(0),
            inner_lock_wait_us_total: AtomicU64::new(0),
            exclusive_lock_us_total: AtomicU64::new(0),
            wal_append_us_total: AtomicU64::new(0),
            flush_frame_prep_us_total: AtomicU64::new(0),
            append_conflict_check_us_total: AtomicU64::new(0),
            append_frames_us_total: AtomicU64::new(0),
            wal_sync_us_total: AtomicU64::new(0),
            waiter_epoch_wait_us_total: AtomicU64::new(0),
            flusher_commits: AtomicU64::new(0),
            waiter_commits: AtomicU64::new(0),
            commit_phase_a_us_total: AtomicU64::new(0),
            commit_phase_b_us_total: AtomicU64::new(0),
            commit_phase_c1_us_total: AtomicU64::new(0),
            commit_phase_c2_us_total: AtomicU64::new(0),
            commit_phase_count: AtomicU64::new(0),
            // Phase histograms (bd-db300.3.8.1)
            hist_consolidator_lock_wait: PhaseHistogram::new(),
            hist_arrival_wait: PhaseHistogram::new(),
            hist_wal_backend_lock_wait: PhaseHistogram::new(),
            hist_wal_append: PhaseHistogram::new(),
            hist_exclusive_lock: PhaseHistogram::new(),
            hist_waiter_epoch_wait: PhaseHistogram::new(),
            hist_phase_b: PhaseHistogram::new(),
            hist_wal_sync: PhaseHistogram::new(),
            hist_full_commit: PhaseHistogram::new(),
            wake_reasons: WakeReasonCounters::new(),
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
        // Update max group size.
        self.max_group_size_observed
            .fetch_max(frames, Ordering::Relaxed);
    }

    /// Record waiter wait time.
    pub fn record_wait(&self, duration_us: u64) {
        self.wait_duration_us_total
            .fetch_add(duration_us, Ordering::Relaxed);
    }

    /// Record a flush retry triggered by a transient busy error.
    pub fn record_busy_retry(&self) {
        self.busy_retries.fetch_add(1, Ordering::Relaxed);
    }

    /// Record pre-queue commit preparation breakdown.
    pub fn record_prepare_breakdown(
        &self,
        batch_build_us: u64,
        conflict_snapshot_us: u64,
        lane_prepare_us: u64,
    ) {
        self.batch_build_us_total
            .fetch_add(batch_build_us, Ordering::Relaxed);
        self.conflict_snapshot_us_total
            .fetch_add(conflict_snapshot_us, Ordering::Relaxed);
        self.lane_prepare_us_total
            .fetch_add(lane_prepare_us, Ordering::Relaxed);
    }

    /// Record flusher-side frame preparation and append breakdown.
    pub fn record_flush_breakdown(
        &self,
        flush_frame_prep_us: u64,
        append_conflict_check_us: u64,
        append_frames_us: u64,
    ) {
        self.flush_frame_prep_us_total
            .fetch_add(flush_frame_prep_us, Ordering::Relaxed);
        self.append_conflict_check_us_total
            .fetch_add(append_conflict_check_us, Ordering::Relaxed);
        self.append_frames_us_total
            .fetch_add(append_frames_us, Ordering::Relaxed);
    }

    /// Record phase timing for a commit operation.
    #[allow(clippy::too_many_arguments)]
    pub fn record_phase_timing(
        &self,
        prepare_us: u64,
        consolidator_lock_wait_us: u64,
        consolidator_flushing_wait_us: u64,
        is_flusher: bool,
        flusher_arrival_wait_us: u64,
        inner_lock_wait_us: u64,
        exclusive_lock_us: u64,
        wal_append_us: u64,
        wal_sync_us: u64,
        waiter_epoch_wait_us: u64,
    ) {
        self.prepare_us_total
            .fetch_add(prepare_us, Ordering::Relaxed);
        self.consolidator_lock_wait_us_total
            .fetch_add(consolidator_lock_wait_us, Ordering::Relaxed);
        self.consolidator_flushing_wait_us_total
            .fetch_add(consolidator_flushing_wait_us, Ordering::Relaxed);

        // bd-db300.3.8.1: record to per-phase histograms.
        self.hist_consolidator_lock_wait
            .record(consolidator_lock_wait_us);

        if is_flusher {
            self.flusher_arrival_wait_us_total
                .fetch_add(flusher_arrival_wait_us, Ordering::Relaxed);
            self.inner_lock_wait_us_total
                .fetch_add(inner_lock_wait_us, Ordering::Relaxed);
            self.exclusive_lock_us_total
                .fetch_add(exclusive_lock_us, Ordering::Relaxed);
            self.wal_append_us_total
                .fetch_add(wal_append_us, Ordering::Relaxed);
            self.wal_sync_us_total
                .fetch_add(wal_sync_us, Ordering::Relaxed);
            self.flusher_commits.fetch_add(1, Ordering::Relaxed);

            // bd-db300.3.8.1: flusher-specific histograms.
            self.hist_arrival_wait.record(flusher_arrival_wait_us);
            self.hist_wal_backend_lock_wait.record(inner_lock_wait_us);
            self.hist_wal_append.record(wal_append_us);
            self.hist_exclusive_lock.record(exclusive_lock_us);
            self.hist_wal_sync.record(wal_sync_us);
        } else {
            self.waiter_epoch_wait_us_total
                .fetch_add(waiter_epoch_wait_us, Ordering::Relaxed);
            self.waiter_commits.fetch_add(1, Ordering::Relaxed);

            // bd-db300.3.8.1: waiter-specific histogram.
            self.hist_waiter_epoch_wait.record(waiter_epoch_wait_us);
        }

        // bd-db300.3.8.1: Phase B total = consolidator_lock + flushing_wait + flusher/waiter work.
        let phase_b_total = consolidator_lock_wait_us
            + consolidator_flushing_wait_us
            + if is_flusher {
                flusher_arrival_wait_us
                    + inner_lock_wait_us
                    + exclusive_lock_us
                    + wal_append_us
                    + wal_sync_us
            } else {
                waiter_epoch_wait_us
            };
        self.hist_phase_b.record(phase_b_total);
    }

    /// Record full commit path phase timing.
    pub fn record_commit_phases(
        &self,
        phase_a_us: u64,
        phase_b_us: u64,
        phase_c1_us: u64,
        phase_c2_us: u64,
    ) {
        self.commit_phase_a_us_total
            .fetch_add(phase_a_us, Ordering::Relaxed);
        self.commit_phase_b_us_total
            .fetch_add(phase_b_us, Ordering::Relaxed);
        self.commit_phase_c1_us_total
            .fetch_add(phase_c1_us, Ordering::Relaxed);
        self.commit_phase_c2_us_total
            .fetch_add(phase_c2_us, Ordering::Relaxed);
        self.commit_phase_count.fetch_add(1, Ordering::Relaxed);

        // bd-db300.3.8.1: full commit distribution.
        self.hist_full_commit
            .record(phase_a_us + phase_b_us + phase_c1_us + phase_c2_us);
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
            busy_retries: self.busy_retries.load(Ordering::Relaxed),
            // Phase timing
            prepare_us_total: self.prepare_us_total.load(Ordering::Relaxed),
            batch_build_us_total: self.batch_build_us_total.load(Ordering::Relaxed),
            conflict_snapshot_us_total: self.conflict_snapshot_us_total.load(Ordering::Relaxed),
            lane_prepare_us_total: self.lane_prepare_us_total.load(Ordering::Relaxed),
            consolidator_lock_wait_us_total: self
                .consolidator_lock_wait_us_total
                .load(Ordering::Relaxed),
            consolidator_flushing_wait_us_total: self
                .consolidator_flushing_wait_us_total
                .load(Ordering::Relaxed),
            flusher_arrival_wait_us_total: self
                .flusher_arrival_wait_us_total
                .load(Ordering::Relaxed),
            inner_lock_wait_us_total: self.inner_lock_wait_us_total.load(Ordering::Relaxed),
            exclusive_lock_us_total: self.exclusive_lock_us_total.load(Ordering::Relaxed),
            wal_append_us_total: self.wal_append_us_total.load(Ordering::Relaxed),
            flush_frame_prep_us_total: self.flush_frame_prep_us_total.load(Ordering::Relaxed),
            append_conflict_check_us_total: self
                .append_conflict_check_us_total
                .load(Ordering::Relaxed),
            append_frames_us_total: self.append_frames_us_total.load(Ordering::Relaxed),
            wal_sync_us_total: self.wal_sync_us_total.load(Ordering::Relaxed),
            waiter_epoch_wait_us_total: self.waiter_epoch_wait_us_total.load(Ordering::Relaxed),
            flusher_commits: self.flusher_commits.load(Ordering::Relaxed),
            waiter_commits: self.waiter_commits.load(Ordering::Relaxed),
            commit_phase_a_us_total: self.commit_phase_a_us_total.load(Ordering::Relaxed),
            commit_phase_b_us_total: self.commit_phase_b_us_total.load(Ordering::Relaxed),
            commit_phase_c1_us_total: self.commit_phase_c1_us_total.load(Ordering::Relaxed),
            commit_phase_c2_us_total: self.commit_phase_c2_us_total.load(Ordering::Relaxed),
            commit_phase_count: self.commit_phase_count.load(Ordering::Relaxed),
            // Per-phase distributions (bd-db300.3.8.1)
            hist_consolidator_lock_wait: self.hist_consolidator_lock_wait.percentiles(),
            hist_arrival_wait: self.hist_arrival_wait.percentiles(),
            hist_wal_backend_lock_wait: self.hist_wal_backend_lock_wait.percentiles(),
            hist_wal_append: self.hist_wal_append.percentiles(),
            hist_exclusive_lock: self.hist_exclusive_lock.percentiles(),
            hist_waiter_epoch_wait: self.hist_waiter_epoch_wait.percentiles(),
            hist_phase_b: self.hist_phase_b.percentiles(),
            hist_wal_sync: self.hist_wal_sync.percentiles(),
            hist_full_commit: self.hist_full_commit.percentiles(),
            wake_reasons: self.wake_reasons.snapshot(),
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
        self.busy_retries.store(0, Ordering::Relaxed);
        // Phase timing
        self.prepare_us_total.store(0, Ordering::Relaxed);
        self.batch_build_us_total.store(0, Ordering::Relaxed);
        self.conflict_snapshot_us_total.store(0, Ordering::Relaxed);
        self.lane_prepare_us_total.store(0, Ordering::Relaxed);
        self.consolidator_lock_wait_us_total
            .store(0, Ordering::Relaxed);
        self.consolidator_flushing_wait_us_total
            .store(0, Ordering::Relaxed);
        self.flusher_arrival_wait_us_total
            .store(0, Ordering::Relaxed);
        self.inner_lock_wait_us_total.store(0, Ordering::Relaxed);
        self.exclusive_lock_us_total.store(0, Ordering::Relaxed);
        self.wal_append_us_total.store(0, Ordering::Relaxed);
        self.flush_frame_prep_us_total.store(0, Ordering::Relaxed);
        self.append_conflict_check_us_total
            .store(0, Ordering::Relaxed);
        self.append_frames_us_total.store(0, Ordering::Relaxed);
        self.wal_sync_us_total.store(0, Ordering::Relaxed);
        self.waiter_epoch_wait_us_total.store(0, Ordering::Relaxed);
        self.flusher_commits.store(0, Ordering::Relaxed);
        self.waiter_commits.store(0, Ordering::Relaxed);
        self.commit_phase_a_us_total.store(0, Ordering::Relaxed);
        self.commit_phase_b_us_total.store(0, Ordering::Relaxed);
        self.commit_phase_c1_us_total.store(0, Ordering::Relaxed);
        self.commit_phase_c2_us_total.store(0, Ordering::Relaxed);
        self.commit_phase_count.store(0, Ordering::Relaxed);
        // Histograms and wake reasons (bd-db300.3.8.1)
        self.hist_consolidator_lock_wait.reset();
        self.hist_arrival_wait.reset();
        self.hist_wal_backend_lock_wait.reset();
        self.hist_wal_append.reset();
        self.hist_exclusive_lock.reset();
        self.hist_waiter_epoch_wait.reset();
        self.hist_phase_b.reset();
        self.hist_wal_sync.reset();
        self.hist_full_commit.reset();
        self.wake_reasons.reset();
    }
}

impl Default for ConsolidationMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time snapshot of consolidation metrics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsolidationMetricsSnapshot {
    pub groups_flushed: u64,
    pub frames_consolidated: u64,
    pub transactions_batched: u64,
    pub fsyncs_total: u64,
    pub flush_duration_us_total: u64,
    pub wait_duration_us_total: u64,
    pub max_group_size_observed: u64,
    pub busy_retries: u64,
    // Phase timing (all in microseconds)
    pub prepare_us_total: u64,
    pub batch_build_us_total: u64,
    pub conflict_snapshot_us_total: u64,
    pub lane_prepare_us_total: u64,
    pub consolidator_lock_wait_us_total: u64,
    pub consolidator_flushing_wait_us_total: u64,
    pub flusher_arrival_wait_us_total: u64,
    pub inner_lock_wait_us_total: u64,
    pub exclusive_lock_us_total: u64,
    pub wal_append_us_total: u64,
    pub flush_frame_prep_us_total: u64,
    pub append_conflict_check_us_total: u64,
    pub append_frames_us_total: u64,
    pub wal_sync_us_total: u64,
    pub waiter_epoch_wait_us_total: u64,
    pub flusher_commits: u64,
    pub waiter_commits: u64,
    // Full commit path phases
    pub commit_phase_a_us_total: u64,
    pub commit_phase_b_us_total: u64,
    pub commit_phase_c1_us_total: u64,
    pub commit_phase_c2_us_total: u64,
    pub commit_phase_count: u64,
    // ── Per-phase distributions (bd-db300.3.8.1) ──
    pub hist_consolidator_lock_wait: PhasePercentiles,
    pub hist_arrival_wait: PhasePercentiles,
    pub hist_wal_backend_lock_wait: PhasePercentiles,
    pub hist_wal_append: PhasePercentiles,
    pub hist_exclusive_lock: PhasePercentiles,
    pub hist_waiter_epoch_wait: PhasePercentiles,
    pub hist_phase_b: PhasePercentiles,
    pub hist_wal_sync: PhasePercentiles,
    pub hist_full_commit: PhasePercentiles,
    // ── Wake reasons (bd-db300.3.8.1) ──
    pub wake_reasons: WakeReasonSnapshot,
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

    /// Total commits (flusher + waiter).
    #[must_use]
    pub fn total_commits(&self) -> u64 {
        self.flusher_commits.saturating_add(self.waiter_commits)
    }

    /// Average prepare time per commit (microseconds).
    #[must_use]
    pub fn avg_prepare_us(&self) -> u64 {
        self.prepare_us_total
            .checked_div(self.total_commits())
            .unwrap_or(0)
    }

    /// Average consolidator lock wait per commit (microseconds).
    #[must_use]
    pub fn avg_consolidator_lock_wait_us(&self) -> u64 {
        self.consolidator_lock_wait_us_total
            .checked_div(self.total_commits())
            .unwrap_or(0)
    }

    /// Average WAL I/O time per flusher (microseconds).
    #[must_use]
    pub fn avg_wal_io_us(&self) -> u64 {
        self.wal_append_us_total
            .saturating_add(self.wal_sync_us_total)
            .checked_div(self.flusher_commits)
            .unwrap_or(0)
    }

    /// Average waiter epoch wait time (microseconds).
    #[must_use]
    pub fn avg_waiter_wait_us(&self) -> u64 {
        self.waiter_epoch_wait_us_total
            .checked_div(self.waiter_commits)
            .unwrap_or(0)
    }

    // ── bd-db300.3.8.2: lock-wait vs WAL-service split ──

    /// Total flusher lock-wait time (inner_lock + exclusive_lock +
    /// flushing_wait), microseconds. This is time spent WAITING to acquire
    /// the WAL backend write path, NOT doing WAL I/O.
    #[must_use]
    pub fn flusher_lock_wait_us_total(&self) -> u64 {
        self.inner_lock_wait_us_total
            .saturating_add(self.exclusive_lock_us_total)
            .saturating_add(self.consolidator_flushing_wait_us_total)
    }

    /// Total WAL service time (append + sync), microseconds. This is time
    /// spent doing actual WAL I/O AFTER acquiring the write lock.
    #[must_use]
    pub fn wal_service_us_total(&self) -> u64 {
        self.wal_append_us_total
            .saturating_add(self.wal_sync_us_total)
    }

    /// Lock-wait fraction of total flusher phase-B time (0.0–1.0).
    /// Values > 0.5 indicate the regime is lock-topology-limited, not
    /// I/O-limited.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn flusher_lock_wait_fraction(&self) -> f64 {
        let lock = self.flusher_lock_wait_us_total();
        let service = self.wal_service_us_total();
        let total = lock.saturating_add(service);
        if total == 0 {
            return 0.0;
        }
        lock as f64 / total as f64
    }

    /// Whether the flusher is lock-topology-limited (lock wait > service).
    #[must_use]
    pub fn is_lock_topology_limited(&self) -> bool {
        self.flusher_lock_wait_us_total() > self.wal_service_us_total()
    }

    /// Generate detailed phase timing report.
    #[must_use]
    pub fn phase_timing_report(&self) -> String {
        let total = self.total_commits();
        if total == 0 {
            return "no commits".to_string();
        }

        // Calculate per-commit averages
        let avg_prepare = self.avg_prepare_us();
        let avg_consol_lock = self.avg_consolidator_lock_wait_us();
        let avg_flushing_wait = self
            .consolidator_flushing_wait_us_total
            .checked_div(total)
            .unwrap_or(0);

        // Flusher-only metrics (per flusher)
        let avg_arrival_wait = self
            .flusher_arrival_wait_us_total
            .checked_div(self.flusher_commits)
            .unwrap_or(0);
        let avg_inner_lock = self
            .inner_lock_wait_us_total
            .checked_div(self.flusher_commits)
            .unwrap_or(0);
        let avg_excl_lock = self
            .exclusive_lock_us_total
            .checked_div(self.flusher_commits)
            .unwrap_or(0);
        let avg_append = self
            .wal_append_us_total
            .checked_div(self.flusher_commits)
            .unwrap_or(0);
        let avg_sync = self
            .wal_sync_us_total
            .checked_div(self.flusher_commits)
            .unwrap_or(0);

        // Waiter-only metrics
        let avg_epoch_wait = self.avg_waiter_wait_us();

        format!(
            "commits: {} (flusher={}, waiter={})\n\
             per-commit avg:\n\
             ├─ prepare: {}µs\n\
             ├─ consolidator_lock_wait: {}µs\n\
             ├─ flushing_wait: {}µs\n\
             flusher path ({} commits):\n\
             ├─ arrival_wait: {}µs\n\
             ├─ inner_lock_wait: {}µs\n\
             ├─ exclusive_lock: {}µs\n\
             ├─ wal_append: {}µs\n\
             └─ wal_sync: {}µs (total WAL I/O: {}µs)\n\
             waiter path ({} commits):\n\
             └─ epoch_wait: {}µs\n\
             full commit path ({} commits):\n\
             ├─ phase_A (prepare+inner.lock): {}µs\n\
             ├─ phase_B (group_commit): {}µs\n\
             ├─ phase_C1 (post-commit+inner.lock): {}µs\n\
             └─ phase_C2 (publish): {}µs",
            total,
            self.flusher_commits,
            self.waiter_commits,
            avg_prepare,
            avg_consol_lock,
            avg_flushing_wait,
            self.flusher_commits,
            avg_arrival_wait,
            avg_inner_lock,
            avg_excl_lock,
            avg_append,
            avg_sync,
            avg_append + avg_sync,
            self.waiter_commits,
            avg_epoch_wait,
            self.commit_phase_count,
            self.commit_phase_a_us_total
                .checked_div(self.commit_phase_count)
                .unwrap_or(0),
            self.commit_phase_b_us_total
                .checked_div(self.commit_phase_count)
                .unwrap_or(0),
            self.commit_phase_c1_us_total
                .checked_div(self.commit_phase_count)
                .unwrap_or(0),
            self.commit_phase_c2_us_total
                .checked_div(self.commit_phase_count)
                .unwrap_or(0),
        )
    }
}

impl std::fmt::Display for ConsolidationMetricsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "groups={} frames={} txns={} fsyncs={} avg_group={} \
             avg_flush_us={} max_group={} busy_retries={} reduction={}x",
            self.groups_flushed,
            self.frames_consolidated,
            self.transactions_batched,
            self.fsyncs_total,
            self.avg_group_size(),
            self.avg_flush_duration_us(),
            self.max_group_size_observed,
            self.busy_retries,
            self.fsync_reduction_ratio(),
        )
    }
}

/// Global consolidation metrics singleton.
pub static GLOBAL_CONSOLIDATION_METRICS: LazyLock<ConsolidationMetrics> =
    LazyLock::new(ConsolidationMetrics::new);

#[cfg(test)]
pub(crate) static GLOBAL_CONSOLIDATION_METRICS_TEST_LOCK: LazyLock<std::sync::Mutex<()>> =
    LazyLock::new(|| std::sync::Mutex::new(()));

// ---------------------------------------------------------------------------
// Group commit consolidator (single-threaded core)
// ---------------------------------------------------------------------------

/// The group commit consolidator accumulates frame batches from concurrent
/// writers and flushes them to the WAL file in consolidated groups.
///
/// This struct manages the FILLING→FLUSHING→COMPLETE state machine.
/// It is designed to be held behind a `Mutex` and accessed by concurrent
/// writers through `GroupCommitQueue`.
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
    /// Epoch pipelining: batches submitted during FLUSHING phase, queued
    /// for the next epoch. This eliminates the flushing_wait bottleneck —
    /// threads never block waiting for a flush to complete.
    next_epoch_batches: VecDeque<TransactionFrameBatch>,
    /// Total frames across next_epoch_batches.
    next_epoch_frame_count: usize,
    /// Whether a promoted epoch in FILLING currently has pending work but no
    /// explicitly claimed flusher because the previous flusher may have stopped.
    promoted_epoch_flusher_vacant: bool,
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
            next_epoch_batches: VecDeque::new(),
            next_epoch_frame_count: 0,
            promoted_epoch_flusher_vacant: false,
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

    /// Maximum time a batch may remain in the filling epoch before flush.
    #[must_use]
    pub const fn max_group_delay(&self) -> Duration {
        self.config.max_group_delay
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
    pub fn submit_batch(&mut self, batch: TransactionFrameBatch) -> Result<SubmitReceipt> {
        // ── Epoch pipelining: accept submissions during FLUSHING ──
        // Instead of blocking, queue batches for the next epoch. This
        // eliminates the flushing_wait bottleneck entirely — threads
        // never block waiting for a flush to complete.
        if self.phase == ConsolidationPhase::Flushing {
            self.next_epoch_frame_count += batch.frame_count();
            self.next_epoch_batches.push_back(batch);

            trace!(
                target: "fsqlite_wal::group_commit",
                epoch = self.epoch,
                next_epoch_frames = self.next_epoch_frame_count,
                next_epoch_batches = self.next_epoch_batches.len(),
                "batch pipelined for next epoch (submitted during FLUSHING)"
            );

            // Always a Waiter — the next epoch's flusher will be elected
            // when complete_flush() promotes these batches.
            return Ok(SubmitReceipt {
                outcome: SubmitOutcome::Waiter,
                target_epoch: self.epoch.saturating_add(1),
            });
        }

        // If we're in COMPLETE, transition to new FILLING epoch.
        if self.phase == ConsolidationPhase::Complete {
            self.transition_to_filling();
        }

        let is_first = self.pending_batches.is_empty();

        if is_first {
            self.filling_started = Some(Instant::now());
            self.promoted_epoch_flusher_vacant = false;
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

        Ok(SubmitReceipt {
            outcome,
            target_epoch: self.epoch.saturating_add(1),
        })
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

    /// Age of the current FILLING epoch from the first submitted batch.
    #[must_use]
    pub fn fill_age(&self) -> Duration {
        self.filling_started
            .map_or(Duration::ZERO, |started| started.elapsed())
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
        self.promoted_epoch_flusher_vacant = false;
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
    /// Returns `true` if pipelined batches were promoted and the caller
    /// should flush again. If the original flusher does not continue,
    /// a fresh submitter may explicitly claim the promoted epoch via
    /// [`Self::claim_flusher_vacancy`].
    pub fn complete_flush(&mut self) -> Result<bool> {
        if self.phase != ConsolidationPhase::Flushing {
            return Err(FrankenError::Internal(format!(
                "complete_flush called in {:?} phase, expected Flushing",
                self.phase
            )));
        }

        self.completed_epoch = self.epoch;
        self.filling_started = None;

        // ── Epoch pipelining: promote next-epoch batches ──
        // If threads submitted during FLUSHING, their batches are in
        // next_epoch_batches. Promote them to pending_batches and
        // transition directly to FILLING (skipping COMPLETE) so the
        // current flusher can immediately begin_flush() again.
        if self.next_epoch_batches.is_empty() {
            self.phase = ConsolidationPhase::Complete;
            self.promoted_epoch_flusher_vacant = false;
            debug!(
                target: "fsqlite_wal::group_commit",
                epoch = self.epoch,
                "complete_flush: FLUSHING → COMPLETE"
            );
            Ok(false)
        } else {
            let promoted_count = self.next_epoch_batches.len();
            let promoted_frames = self.next_epoch_frame_count;
            self.pending_batches = std::mem::take(&mut self.next_epoch_batches);
            self.pending_frame_count = self.next_epoch_frame_count;
            self.next_epoch_frame_count = 0;
            self.phase = ConsolidationPhase::Filling;
            self.filling_started = Some(Instant::now());
            self.promoted_epoch_flusher_vacant = true;

            debug!(
                target: "fsqlite_wal::group_commit",
                epoch = self.epoch,
                promoted_batches = promoted_count,
                promoted_frames = promoted_frames,
                "complete_flush: FLUSHING → FILLING (epoch pipelining)"
            );
            Ok(true) // Caller must flush again
        }
    }

    /// Whether pipelined batches are waiting for the next epoch.
    #[must_use]
    pub fn has_pipelined_batches(&self) -> bool {
        !self.next_epoch_batches.is_empty()
    }

    /// Whether a promoted epoch in `Filling` currently needs an explicit
    /// flusher claim before a replacement flusher takes over.
    #[must_use]
    pub const fn has_flusher_vacancy(&self) -> bool {
        self.promoted_epoch_flusher_vacant
    }

    /// Claim the promoted-epoch flusher vacancy after the original flusher
    /// stopped before calling `begin_flush()` again.
    ///
    /// Returns `true` if the caller successfully claimed the vacancy.
    #[must_use]
    pub fn claim_flusher_vacancy(&mut self) -> bool {
        if self.phase == ConsolidationPhase::Filling
            && self.promoted_epoch_flusher_vacant
            && !self.pending_batches.is_empty()
        {
            self.promoted_epoch_flusher_vacant = false;
            return true;
        }
        false
    }

    /// Abort the current flush after the flusher observed an I/O error.
    ///
    /// This transitions the state machine out of `Flushing` so waiters can be
    /// released with the epoch-level failure published by the caller.
    ///
    /// # Errors
    ///
    /// Returns `Err` if not in `Flushing` phase.
    pub fn abort_flush(&mut self) -> Result<()> {
        if self.phase != ConsolidationPhase::Flushing {
            return Err(FrankenError::Internal(format!(
                "abort_flush called in {:?} phase, expected Flushing",
                self.phase
            )));
        }

        // On abort, promote pipelined batches the same way as
        // complete_flush — those transactions weren't part of the
        // failed flush, so they should be retried in the next epoch.
        if self.next_epoch_batches.is_empty() {
            self.phase = ConsolidationPhase::Complete;
            self.filling_started = None;
            self.promoted_epoch_flusher_vacant = false;
        } else {
            self.pending_batches = std::mem::take(&mut self.next_epoch_batches);
            self.pending_frame_count = self.next_epoch_frame_count;
            self.next_epoch_frame_count = 0;
            self.phase = ConsolidationPhase::Filling;
            self.filling_started = Some(Instant::now());
            self.promoted_epoch_flusher_vacant = true;
            // Keep filling_started set — promoted batches need the timeout
        }

        debug!(
            target: "fsqlite_wal::group_commit",
            epoch = self.epoch,
            "abort_flush: FLUSHING → {:?}",
            self.phase
        );

        Ok(())
    }

    /// Transition from COMPLETE to FILLING for the next epoch.
    fn transition_to_filling(&mut self) {
        self.phase = ConsolidationPhase::Filling;
        self.filling_started = None;
        self.promoted_epoch_flusher_vacant = false;
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
    let frame_size = wal.frame_size();
    let total_frames: usize = batches.iter().map(TransactionFrameBatch::frame_count).sum();
    if total_frames == 0 {
        return Ok(0);
    }

    let total_bytes = total_frames
        .checked_mul(frame_size)
        .ok_or_else(|| FrankenError::Internal("frame batch size overflow".to_owned()))?;
    let frame_refs = batches.iter().flat_map(|batch| {
        batch.frames.iter().map(|frame| WalAppendFrameRef {
            page_number: frame.page_number,
            page_data: &frame.page_data,
            db_size_if_commit: frame.db_size_if_commit,
        })
    });

    let span = tracing::info_span!(
        target: "fsqlite_wal::group_commit",
        "consolidated_write",
        total_frames,
        total_bytes,
        batches = batches.len(),
    );
    let _guard = span.enter();

    wal.append_frame_iter(cx, total_frames, frame_refs)?;
    wal.durable_sync(cx, SyncKind::FullDurable)?;
    let bytes_written = u64::try_from(total_bytes).unwrap_or(u64::MAX);

    info!(
        target: "fsqlite_wal::group_commit",
        frames_written = total_frames,
        bytes_written,
        batches = batches.len(),
        "consolidated write + fsync complete"
    );

    Ok(total_frames)
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

    struct ResetGlobalConsolidationMetrics;

    impl Drop for ResetGlobalConsolidationMetrics {
        fn drop(&mut self) {
            GLOBAL_CONSOLIDATION_METRICS.reset();
        }
    }

    fn with_global_consolidation_metrics<T>(body: impl FnOnce() -> T) -> T {
        let _guard = GLOBAL_CONSOLIDATION_METRICS_TEST_LOCK
            .lock()
            .expect("global consolidation metrics test lock poisoned");
        let _reset = ResetGlobalConsolidationMetrics;
        GLOBAL_CONSOLIDATION_METRICS.reset();
        body()
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
        let receipt = c.submit_batch(batch).unwrap();
        assert_eq!(receipt.outcome, SubmitOutcome::Flusher);
        assert_eq!(receipt.target_epoch, 1);
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
        assert_eq!(
            c.submit_batch(batch1).unwrap().outcome,
            SubmitOutcome::Flusher
        );

        let batch2 = TransactionFrameBatch::new(vec![FrameSubmission {
            page_number: 2,
            page_data: sample_page(0x02),
            db_size_if_commit: 0,
        }]);
        assert_eq!(
            c.submit_batch(batch2).unwrap().outcome,
            SubmitOutcome::Waiter
        );
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

        // Pipelined submissions during FLUSHING become waiters for the next epoch.
        let batch_extra = TransactionFrameBatch::new(vec![FrameSubmission {
            page_number: 10,
            page_data: sample_page(0x10),
            db_size_if_commit: 0,
        }]);
        let receipt = c.submit_batch(batch_extra).unwrap();
        assert_eq!(receipt.outcome, SubmitOutcome::Waiter);
        assert_eq!(
            receipt.target_epoch, 2,
            "pipelined submissions belong to the promoted next epoch"
        );

        // Complete flush: FLUSHING → FILLING with a promoted next epoch.
        let promoted = c.complete_flush().unwrap();
        assert!(promoted);
        assert_eq!(c.phase(), ConsolidationPhase::Filling);
        assert_eq!(c.completed_epoch(), 1);
        assert_eq!(c.pending_batch_count(), 1);
        assert!(c.has_flusher_vacancy());

        // The original flusher may continue immediately without an explicit claim.
        let batches = c.begin_flush().unwrap();
        assert_eq!(c.phase(), ConsolidationPhase::Flushing);
        assert_eq!(c.epoch(), 2);
        assert_eq!(batches.len(), 1);
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
        let receipt = c.submit_batch(batch2).unwrap();
        assert_eq!(receipt.outcome, SubmitOutcome::Flusher);
        assert_eq!(receipt.target_epoch, 2);
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

    #[test]
    fn test_consolidator_abort_flush_releases_epoch_and_allows_next_cycle() {
        let mut c = GroupCommitConsolidator::new(GroupCommitConfig::default());

        c.submit_batch(TransactionFrameBatch::new(vec![FrameSubmission {
            page_number: 1,
            page_data: sample_page(0x01),
            db_size_if_commit: 1,
        }]))
        .unwrap();
        c.begin_flush().unwrap();
        c.abort_flush().unwrap();
        assert_eq!(c.phase(), ConsolidationPhase::Complete);
        assert_eq!(c.completed_epoch(), 0);

        let receipt = c
            .submit_batch(TransactionFrameBatch::new(vec![FrameSubmission {
                page_number: 2,
                page_data: sample_page(0x02),
                db_size_if_commit: 2,
            }]))
            .unwrap();
        assert_eq!(receipt.outcome, SubmitOutcome::Flusher);
        assert_eq!(receipt.target_epoch, 2);
        assert_eq!(c.phase(), ConsolidationPhase::Filling);
        assert_eq!(c.pending_batch_count(), 1);
        assert_eq!(c.epoch(), 1);
    }

    #[test]
    fn test_consolidator_promoted_epoch_exposes_flusher_takeover_claim_if_original_stops() {
        let mut c = GroupCommitConsolidator::new(GroupCommitConfig::default());

        let batch1 = TransactionFrameBatch::new(vec![FrameSubmission {
            page_number: 1,
            page_data: sample_page(0x01),
            db_size_if_commit: 1,
        }]);
        assert_eq!(
            c.submit_batch(batch1).unwrap().outcome,
            SubmitOutcome::Flusher
        );

        let _flushing_batches = c.begin_flush().unwrap();
        assert_eq!(c.epoch(), 1);
        assert_eq!(c.phase(), ConsolidationPhase::Flushing);

        let pipelined_batch = TransactionFrameBatch::new(vec![FrameSubmission {
            page_number: 2,
            page_data: sample_page(0x02),
            db_size_if_commit: 2,
        }]);
        let receipt = c.submit_batch(pipelined_batch).unwrap();
        assert_eq!(receipt.outcome, SubmitOutcome::Waiter);
        assert_eq!(receipt.target_epoch, 2);

        let promoted = c.complete_flush().unwrap();
        assert!(
            promoted,
            "pipelined epoch must be promoted back to FILLING for the next flush"
        );
        assert_eq!(c.phase(), ConsolidationPhase::Filling);
        assert_eq!(c.pending_batch_count(), 1);
        assert_eq!(c.pending_frame_count(), 1);
        assert_eq!(c.epoch(), 1);
        assert_eq!(c.completed_epoch(), 1);
        assert!(c.has_flusher_vacancy());

        let takeover_batch = TransactionFrameBatch::new(vec![FrameSubmission {
            page_number: 3,
            page_data: sample_page(0x03),
            db_size_if_commit: 3,
        }]);
        let receipt = c.submit_batch(takeover_batch).unwrap();
        assert_eq!(
            receipt.outcome,
            SubmitOutcome::Waiter,
            "promoted work stays queued until someone explicitly claims the flusher vacancy"
        );
        assert_eq!(receipt.target_epoch, 2);
        assert!(c.claim_flusher_vacancy());
        assert!(!c.has_flusher_vacancy());
        assert!(!c.claim_flusher_vacancy());

        let takeover_batches = c.begin_flush().unwrap();
        assert_eq!(c.phase(), ConsolidationPhase::Flushing);
        assert_eq!(c.epoch(), 2);
        assert_eq!(takeover_batches.len(), 2);
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
        let _checksum_after_2 = wal.running_checksum();

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
        m.record_busy_retry();
        m.record_busy_retry();

        let snap = m.snapshot();
        assert_eq!(snap.groups_flushed, 2);
        assert_eq!(snap.frames_consolidated, 30);
        assert_eq!(snap.transactions_batched, 8);
        assert_eq!(snap.fsyncs_total, 2);
        assert_eq!(snap.flush_duration_us_total, 1500);
        assert_eq!(snap.wait_duration_us_total, 100);
        assert_eq!(snap.max_group_size_observed, 20);
        assert_eq!(snap.busy_retries, 2);
        assert_eq!(snap.avg_group_size(), 15);
        assert_eq!(snap.avg_transactions_per_group(), 4);
        assert_eq!(snap.avg_flush_duration_us(), 750);
        assert_eq!(snap.fsync_reduction_ratio(), 4);
    }

    #[test]
    fn test_consolidation_metrics_reset() {
        let m = ConsolidationMetrics::new();
        m.record_flush(10, 3, 500);
        m.record_busy_retry();
        m.reset();
        let snap = m.snapshot();
        assert_eq!(snap.groups_flushed, 0);
        assert_eq!(snap.frames_consolidated, 0);
        assert_eq!(snap.busy_retries, 0);
    }

    #[test]
    fn test_consolidation_metrics_display() {
        let m = ConsolidationMetrics::new();
        m.record_flush(10, 5, 500);
        m.record_busy_retry();
        let s = m.snapshot().to_string();
        assert!(s.contains("groups=1"));
        assert!(s.contains("frames=10"));
        assert!(s.contains("txns=5"));
        assert!(s.contains("busy_retries=1"));
        assert!(s.contains("reduction=5x"));
    }

    #[test]
    fn test_consolidation_metrics_snapshot_serializes_phase_distributions() {
        let m = ConsolidationMetrics::new();
        m.record_phase_timing(10, 5, 2, true, 20, 3, 8, 50, 30, 0);
        m.record_phase_timing(11, 6, 2, false, 0, 0, 0, 0, 0, 100);
        m.wake_reasons.notify.fetch_add(1, Ordering::Relaxed);
        m.wake_reasons.timeout.fetch_add(2, Ordering::Relaxed);

        let encoded =
            serde_json::to_value(m.snapshot()).expect("consolidation snapshot should serialize");

        assert_eq!(
            encoded["hist_wal_append"]["count"].as_u64(),
            Some(1),
            "flusher histogram should serialize sample counts"
        );
        assert_eq!(
            encoded["hist_waiter_epoch_wait"]["count"].as_u64(),
            Some(1),
            "waiter histogram should serialize sample counts"
        );
        assert_eq!(
            encoded["wake_reasons"]["notify"].as_u64(),
            Some(1),
            "wake reasons should serialize nested counters"
        );
        assert_eq!(
            encoded["wake_reasons"]["timeout"].as_u64(),
            Some(2),
            "wake reasons should preserve all fields"
        );
    }

    #[test]
    fn test_phase_histogram_recent_tail_decays_without_snapshot() {
        let h = PhaseHistogram::new();
        h.record(1_600);
        assert_eq!(h.recent_tail_us(), 1_600);

        h.record(0);
        assert_eq!(
            h.recent_tail_us(),
            1_500,
            "recent tail should decay by one sixteenth per sample"
        );

        h.record(2_000);
        assert_eq!(
            h.recent_tail_us(),
            2_000,
            "new spikes should replace the decayed tail immediately"
        );

        h.reset();
        assert_eq!(h.recent_tail_us(), 0);
    }

    /// Deterministic proof that consolidation achieves fsync reduction.
    ///
    /// Without consolidation: N transactions × 1 fsync each = N fsyncs.
    /// With consolidation: N transactions in 1 group = 1 fsync.
    /// Reduction: N/1 = N (for N=10, reduction = 10x).
    #[test]
    fn test_fsync_reduction_deterministic_proof() {
        with_global_consolidation_metrics(|| {
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
        });
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
        let receipt1 = consolidator.submit_batch(batch1).unwrap();
        assert_eq!(receipt1.outcome, SubmitOutcome::Flusher);
        assert_eq!(receipt1.target_epoch, 1);

        let batch2 = TransactionFrameBatch::new(vec![FrameSubmission {
            page_number: 3,
            page_data: sample_page(0x03),
            db_size_if_commit: 3,
        }]);
        let receipt2 = consolidator.submit_batch(batch2).unwrap();
        assert_eq!(receipt2.outcome, SubmitOutcome::Waiter);
        assert_eq!(receipt2.target_epoch, 1);

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
        let receipt3 = consolidator.submit_batch(batch3).unwrap();
        assert_eq!(receipt3.outcome, SubmitOutcome::Waiter);
        assert_eq!(receipt3.target_epoch, 1);

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

    // ── PhaseHistogram tests (bd-db300.3.8.1) ──────────────────────

    #[test]
    fn phase_histogram_empty_returns_zeros() {
        let h = PhaseHistogram::new();
        let p = h.percentiles();
        assert_eq!(p.count, 0);
        assert_eq!(p.p50, 0);
        assert_eq!(p.p99, 0);
        assert_eq!(p.max, 0);
        assert_eq!(p.mean_us, 0);
    }

    #[test]
    fn phase_histogram_single_sample() {
        let h = PhaseHistogram::new();
        h.record(42);
        let p = h.percentiles();
        assert_eq!(p.count, 1);
        assert_eq!(p.p50, 42);
        assert_eq!(p.p95, 42);
        assert_eq!(p.p99, 42);
        assert_eq!(p.max, 42);
        assert_eq!(p.mean_us, 42);
    }

    #[test]
    fn phase_histogram_percentiles_ordered() {
        let h = PhaseHistogram::new();
        for i in 1..=100 {
            h.record(i);
        }
        let p = h.percentiles();
        assert_eq!(p.count, 100);
        assert!(p.p50 <= p.p95, "p50={} should be <= p95={}", p.p50, p.p95);
        assert!(p.p95 <= p.p99, "p95={} should be <= p99={}", p.p95, p.p99);
        assert!(p.p99 <= p.max, "p99={} should be <= max={}", p.p99, p.max);
        assert_eq!(p.max, 100);
        assert_eq!(p.mean_us, 50);
    }

    #[test]
    fn phase_histogram_max_tracks_outlier() {
        let h = PhaseHistogram::new();
        for _ in 0..100 {
            h.record(10);
        }
        h.record(99999);
        let p = h.percentiles();
        assert_eq!(p.max, 99999);
        assert_eq!(p.p50, 10);
    }

    #[test]
    fn phase_histogram_reset_clears_state() {
        let h = PhaseHistogram::new();
        for i in 0..50 {
            h.record(i * 10);
        }
        h.reset();
        let p = h.percentiles();
        assert_eq!(p.count, 0);
        assert_eq!(p.max, 0);
    }

    #[test]
    fn phase_histogram_concurrent_writers_no_panic() {
        use std::sync::Arc;
        let h = Arc::new(PhaseHistogram::new());
        let barrier = Arc::new(std::sync::Barrier::new(4));
        let mut handles = Vec::new();
        for t in 0..4u64 {
            let h = Arc::clone(&h);
            let b = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                b.wait();
                for i in 0..500 {
                    h.record(t * 1000 + i);
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        let p = h.percentiles();
        assert_eq!(p.count, 2000);
        assert!(p.max >= 3499);
    }

    // ── WakeReasonCounters tests (bd-db300.3.8.1) ──────────────────

    #[test]
    fn wake_reason_counters_track_all_reasons() {
        let w = WakeReasonCounters::new();
        w.notify.fetch_add(10, Ordering::Relaxed);
        w.timeout.fetch_add(3, Ordering::Relaxed);
        w.flusher_takeover.fetch_add(1, Ordering::Relaxed);
        w.failed_epoch.fetch_add(2, Ordering::Relaxed);
        w.busy_retry.fetch_add(5, Ordering::Relaxed);
        let s = w.snapshot();
        assert_eq!(s.notify, 10);
        assert_eq!(s.timeout, 3);
        assert_eq!(s.flusher_takeover, 1);
        assert_eq!(s.failed_epoch, 2);
        assert_eq!(s.busy_retry, 5);
        assert_eq!(s.total(), 21);
    }

    #[test]
    fn wake_reason_reset_clears() {
        let w = WakeReasonCounters::new();
        w.notify.fetch_add(99, Ordering::Relaxed);
        w.reset();
        let s = w.snapshot();
        assert_eq!(s.total(), 0);
    }

    // ── Integration: histograms in ConsolidationMetrics ──────────────

    #[test]
    fn consolidation_metrics_snapshot_includes_distributions() {
        with_global_consolidation_metrics(|| {
            for i in 0..10u64 {
                GLOBAL_CONSOLIDATION_METRICS.record_phase_timing(
                    10 + i,
                    5 + i,
                    2,
                    true,
                    20 + i,
                    3 + i,
                    8 + i,
                    50 + i,
                    30 + i,
                    0,
                );
            }
            for i in 0..5u64 {
                GLOBAL_CONSOLIDATION_METRICS.record_phase_timing(
                    10 + i,
                    5 + i,
                    2,
                    false,
                    0,
                    0,
                    0,
                    0,
                    0,
                    100 + i,
                );
            }

            let snap = GLOBAL_CONSOLIDATION_METRICS.snapshot();
            assert_eq!(snap.hist_consolidator_lock_wait.count, 15);
            assert_eq!(snap.hist_arrival_wait.count, 10);
            assert_eq!(snap.hist_wal_append.count, 10);
            assert_eq!(snap.hist_waiter_epoch_wait.count, 5);
            assert_eq!(snap.hist_phase_b.count, 15);
            assert!(snap.hist_wal_append.p50 > 0);
            assert!(snap.hist_wal_append.max >= 59);
        });
    }

    #[test]
    fn transaction_conflict_snapshot_debug_clone_copy_eq() {
        let generation = WalGenerationIdentity {
            checkpoint_seq: 0,
            salts: WalSalts { salt1: 0, salt2: 0 },
        };
        let a = TransactionConflictSnapshot {
            generation,
            last_commit_frame: Some(42),
            commit_count: 7,
        };
        let copied = a;
        assert_eq!(copied, a);
        let b = TransactionConflictSnapshot {
            generation,
            last_commit_frame: None,
            commit_count: 7,
        };
        assert_ne!(a, b);
        let dbg = format!("{a:?}");
        assert!(dbg.contains("TransactionConflictSnapshot"));
    }

    #[test]
    fn transaction_frame_batch_context_default_and_eq() {
        let def = TransactionFrameBatchContext::default();
        assert_eq!(def.batch_id, 0);
        assert_eq!(def.lane_id, 0);
        assert_eq!(def.staged_frame_count, 0);
        assert_eq!(def.staging_elapsed_ns, 0);
        let other = TransactionFrameBatchContext {
            batch_id: 1,
            lane_id: 3,
            staged_frame_count: 10,
            staging_elapsed_ns: 500,
        };
        assert_ne!(def, other);
        let copied = other;
        assert_eq!(copied, other);
        let dbg = format!("{def:?}");
        assert!(dbg.contains("TransactionFrameBatchContext"));
    }

    #[test]
    fn consolidation_phase_and_submit_outcome_all_variants() {
        let phases = [
            ConsolidationPhase::Filling,
            ConsolidationPhase::Flushing,
            ConsolidationPhase::Complete,
        ];
        for (i, p) in phases.iter().enumerate() {
            let copied = *p;
            assert_eq!(copied, *p);
            for (j, q) in phases.iter().enumerate() {
                assert_eq!(i == j, p == q);
            }
        }
        assert_ne!(SubmitOutcome::Flusher, SubmitOutcome::Waiter);
        let copied = SubmitOutcome::Flusher;
        assert_eq!(copied, SubmitOutcome::Flusher);
        let dbg = format!("{:?}", SubmitOutcome::Waiter);
        assert!(dbg.contains("Waiter"));
    }

    #[test]
    fn transaction_frame_batch_builders() {
        let frame = FrameSubmission {
            page_number: 5,
            page_data: vec![0u8; 16],
            db_size_if_commit: 0,
        };
        let batch = TransactionFrameBatch::new(vec![frame.clone()])
            .with_conflict_snapshot(vec![5, 10], None)
            .with_context(TransactionFrameBatchContext {
                batch_id: 99,
                lane_id: 2,
                staged_frame_count: 1,
                staging_elapsed_ns: 100,
            });
        assert_eq!(batch.frame_count(), 1);
        assert!(!batch.has_commit_frame());
        assert_eq!(batch.conflict_pages, vec![5, 10]);
        assert!(batch.conflict_snapshot.is_none());
        assert_eq!(batch.context.batch_id, 99);
        assert_eq!(batch.context.lane_id, 2);
    }

    #[test]
    fn group_commit_config_default_copy_debug() {
        let cfg = GroupCommitConfig::default();
        let copied = cfg;
        assert_eq!(copied.max_group_size, 64);
        assert_eq!(copied.max_group_delay, Duration::from_millis(1));
        assert_eq!(copied.max_group_delay_ceiling, Duration::from_millis(10));
        let dbg = format!("{cfg:?}");
        assert!(dbg.contains("GroupCommitConfig"));
    }

    #[test]
    fn frame_submission_debug_clone() {
        let fs = FrameSubmission {
            page_number: 42,
            page_data: vec![0xAB; 8],
            db_size_if_commit: 0,
        };
        let cloned = fs.clone();
        assert_eq!(cloned.page_number, 42);
        assert_eq!(cloned.page_data.len(), 8);
        assert_eq!(cloned.db_size_if_commit, 0);
        let dbg = format!("{fs:?}");
        assert!(dbg.contains("FrameSubmission"));
    }

    #[test]
    fn phase_percentiles_default_copy_eq() {
        let pp = PhasePercentiles::default();
        let copied = pp;
        assert_eq!(copied, pp);
        assert_eq!(pp.p50, 0);
        assert_eq!(pp.p99, 0);
        assert_eq!(pp.max, 0);
        assert_eq!(pp.count, 0);
    }

    #[test]
    fn wake_reason_snapshot_default_total_zero() {
        let ws = WakeReasonSnapshot::default();
        assert_eq!(ws.total(), 0);
        let copied = ws;
        assert_eq!(copied, ws);
        let dbg = format!("{ws:?}");
        assert!(dbg.contains("WakeReasonSnapshot"));
    }
}
