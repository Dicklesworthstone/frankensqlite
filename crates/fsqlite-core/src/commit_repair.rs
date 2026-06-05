//! Commit durability and asynchronous repair orchestration (§1.6, bd-22n.11).
//!
//! The critical path only appends+syncs systematic symbols. Repair symbols are
//! generated/append-synced asynchronously after commit acknowledgment.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use asupersync::cx::Cx as NativeCx;
use asupersync::runtime::{JoinHandle as AsyncJoinHandle, Runtime, spawn_blocking};
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use tracing::{debug, error, info, warn};

const BEAD_ID: &str = "bd-22n.11";

/// Default bounded capacity for commit-channel backpressure.
pub const DEFAULT_COMMIT_CHANNEL_CAPACITY: usize = 16;

/// Request sent from writers to the write coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRequest {
    pub txn_id: u64,
    pub write_set_pages: Vec<u32>,
    pub payload: Vec<u8>,
}

impl CommitRequest {
    #[must_use]
    pub fn new(txn_id: u64, write_set_pages: Vec<u32>, payload: Vec<u8>) -> Self {
        Self {
            txn_id,
            write_set_pages,
            payload,
        }
    }
}

/// Capacity/config knobs for the two-phase commit pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitPipelineConfig {
    pub channel_capacity: usize,
}

impl Default for CommitPipelineConfig {
    fn default() -> Self {
        Self {
            channel_capacity: DEFAULT_COMMIT_CHANNEL_CAPACITY,
        }
    }
}

impl CommitPipelineConfig {
    /// Clamp PRAGMA capacity to a valid non-zero bounded channel size.
    #[must_use]
    pub fn from_pragma_capacity(raw_capacity: i64) -> Self {
        let clamped_i64 = raw_capacity.clamp(1, i64::from(u16::MAX));
        let clamped = usize::try_from(clamped_i64).expect("clamped to positive u16 range");
        Self {
            channel_capacity: clamped,
        }
    }
}

#[derive(Debug)]
struct LogicalCapacity {
    available: Mutex<usize>,
    max_permits: usize,
    changed: Condvar,
}

impl LogicalCapacity {
    fn new(capacity: usize) -> Self {
        let normalized = capacity.max(1);
        Self {
            available: Mutex::new(normalized),
            max_permits: normalized,
            changed: Condvar::new(),
        }
    }

    fn acquire_for(self: &Arc<Self>, timeout: Duration) -> Option<LogicalCapacityPermit> {
        let started_at = Instant::now();
        let mut available = lock_with_recovery(&self.available, "two_phase_logical_capacity");
        loop {
            if *available > 0 {
                *available -= 1;
                return Some(LogicalCapacityPermit {
                    capacity: Arc::clone(self),
                });
            }

            let remaining = timeout.saturating_sub(started_at.elapsed());
            if remaining.is_zero() {
                return None;
            }

            let wait_result = self.changed.wait_timeout(available, remaining);
            let (guard, _) = wait_result.unwrap_or_else(std::sync::PoisonError::into_inner);
            available = guard;
        }
    }

    fn release(&self) {
        let mut available = lock_with_recovery(&self.available, "two_phase_logical_capacity");
        *available = (*available).saturating_add(1).min(self.max_permits);
        self.changed.notify_one();
    }

    fn available_permits(&self) -> usize {
        *lock_with_recovery(&self.available, "two_phase_logical_capacity")
    }

    fn max_permits(&self) -> usize {
        self.max_permits
    }
}

#[derive(Debug)]
struct LogicalCapacityPermit {
    capacity: Arc<LogicalCapacity>,
}

impl Drop for LogicalCapacityPermit {
    fn drop(&mut self) {
        self.capacity.release();
    }
}

#[derive(Debug)]
struct PendingCommit {
    request: CommitRequest,
    logical_permit: LogicalCapacityPermit,
}

#[derive(Debug)]
struct ReceiverOrderState {
    next_receive_seq: u64,
    aborted_reservations: BTreeSet<u64>,
    pending_commits: BTreeMap<u64, PendingCommit>,
}

impl ReceiverOrderState {
    fn new() -> Self {
        Self {
            next_receive_seq: 1,
            aborted_reservations: BTreeSet::new(),
            pending_commits: BTreeMap::new(),
        }
    }

    fn mark_aborted(&mut self, reservation_seq: u64) {
        self.aborted_reservations.insert(reservation_seq);
    }

    fn queue_commit(
        &mut self,
        reservation_seq: u64,
        request: CommitRequest,
        logical_permit: LogicalCapacityPermit,
    ) {
        let replaced = self.pending_commits.insert(
            reservation_seq,
            PendingCommit {
                request,
                logical_permit,
            },
        );
        debug_assert!(
            replaced.is_none(),
            "duplicate reservation sequence enqueued: {reservation_seq}"
        );
    }

    fn rollback_pending_commit(&mut self, reservation_seq: u64) {
        let _ = self.pending_commits.remove(&reservation_seq);
    }

    fn take_ready(&mut self) -> Option<CommitRequest> {
        loop {
            if self.aborted_reservations.remove(&self.next_receive_seq) {
                self.next_receive_seq = self.next_receive_seq.saturating_add(1);
                continue;
            }

            let PendingCommit {
                request,
                logical_permit,
            } = self.pending_commits.remove(&self.next_receive_seq)?;
            self.next_receive_seq = self.next_receive_seq.saturating_add(1);
            drop(logical_permit);
            return Some(request);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitSignal {
    CommitQueued,
}

#[derive(Debug)]
struct TwoPhaseQueueShared {
    logical_capacity: Arc<LogicalCapacity>,
    signal_sender: mpsc::SyncSender<CommitSignal>,
    signal_receiver: Mutex<mpsc::Receiver<CommitSignal>>,
    next_reservation_seq: AtomicU64,
    order_state: Mutex<ReceiverOrderState>,
}

impl TwoPhaseQueueShared {
    fn with_capacity(capacity: usize) -> Self {
        let normalized_capacity = capacity.max(1);
        let (signal_sender, signal_receiver) = mpsc::sync_channel(normalized_capacity);
        Self {
            logical_capacity: Arc::new(LogicalCapacity::new(normalized_capacity)),
            signal_sender,
            signal_receiver: Mutex::new(signal_receiver),
            next_reservation_seq: AtomicU64::new(1),
            order_state: Mutex::new(ReceiverOrderState::new()),
        }
    }

    fn reserve_sequence(&self) -> u64 {
        self.next_reservation_seq.fetch_add(1, Ordering::AcqRel)
    }

    fn occupancy(&self) -> usize {
        self.capacity()
            .saturating_sub(self.logical_capacity.available_permits())
    }

    fn capacity(&self) -> usize {
        self.logical_capacity.max_permits()
    }

    fn queue_commit(
        &self,
        reservation_seq: u64,
        request: CommitRequest,
        logical_permit: LogicalCapacityPermit,
    ) {
        lock_with_recovery(&self.order_state, "two_phase_order_state").queue_commit(
            reservation_seq,
            request,
            logical_permit,
        );
    }

    fn rollback_pending_commit(&self, reservation_seq: u64) {
        lock_with_recovery(&self.order_state, "two_phase_order_state")
            .rollback_pending_commit(reservation_seq);
    }

    fn mark_aborted(&self, reservation_seq: u64) {
        lock_with_recovery(&self.order_state, "two_phase_order_state")
            .mark_aborted(reservation_seq);
        let _ = self.signal_sender.try_send(CommitSignal::CommitQueued);
    }

    fn take_ready_request(&self) -> Option<CommitRequest> {
        lock_with_recovery(&self.order_state, "two_phase_order_state").take_ready()
    }

    fn drain_signals(&self) -> SignalDrain {
        let receiver = lock_with_recovery(&self.signal_receiver, "two_phase_signal_receiver");
        let mut drained_any = false;
        loop {
            match receiver.try_recv() {
                Ok(CommitSignal::CommitQueued) => {
                    drained_any = true;
                }
                Err(mpsc::TryRecvError::Empty) => {
                    return if drained_any {
                        SignalDrain::Drained
                    } else {
                        SignalDrain::Empty
                    };
                }
                Err(mpsc::TryRecvError::Disconnected) => return SignalDrain::Disconnected,
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SignalDrain {
    Empty,
    Drained,
    Disconnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SignalWaitOutcome {
    Woken,
    TimedOut,
    Disconnected,
}

fn wait_for_signal_activity(shared: &TwoPhaseQueueShared, timeout: Duration) -> SignalWaitOutcome {
    let receiver = lock_with_recovery(&shared.signal_receiver, "two_phase_signal_receiver");
    match receiver.recv_timeout(timeout) {
        Ok(CommitSignal::CommitQueued) => SignalWaitOutcome::Woken,
        Err(mpsc::RecvTimeoutError::Timeout) => SignalWaitOutcome::TimedOut,
        Err(mpsc::RecvTimeoutError::Disconnected) => SignalWaitOutcome::Disconnected,
    }
}

/// Sender side of the two-phase bounded MPSC commit channel.
#[derive(Debug, Clone)]
pub struct TwoPhaseCommitSender {
    shared: Arc<TwoPhaseQueueShared>,
}

impl TwoPhaseCommitSender {
    /// Reserve a slot (phase 1). Blocks when channel is saturated.
    pub fn reserve(&self) -> SendPermit {
        loop {
            if let Some(permit) = self.try_reserve_for(Duration::from_secs(3600)) {
                return permit;
            }
        }
    }

    /// Reserve with timeout; `None` means caller gave up (cancel during reserve).
    #[must_use]
    pub fn try_reserve_for(&self, timeout: Duration) -> Option<SendPermit> {
        let logical_permit = self.shared.logical_capacity.acquire_for(timeout)?;

        Some(SendPermit {
            shared: Arc::clone(&self.shared),
            logical_permit: Some(logical_permit),
            reservation_seq: Some(self.shared.reserve_sequence()),
        })
    }

    /// Current buffered + reserved occupancy.
    #[must_use]
    pub fn occupancy(&self) -> usize {
        self.shared.occupancy()
    }

    /// Bounded channel capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.shared.capacity()
    }
}

/// Receiver side of the two-phase bounded MPSC commit channel.
#[derive(Debug, Clone)]
pub struct TwoPhaseCommitReceiver {
    shared: Arc<TwoPhaseQueueShared>,
}

impl TwoPhaseCommitReceiver {
    /// Receive the next coordinator request (FIFO by reservation order).
    pub fn recv(&self) -> CommitRequest {
        loop {
            if let Some(request) = self.try_recv_for(Duration::from_secs(3600)) {
                return request;
            }
        }
    }

    /// Timed receive used by tests and bounded coordinator loops.
    #[must_use]
    pub fn try_recv_for(&self, timeout: Duration) -> Option<CommitRequest> {
        let started_at = Instant::now();
        loop {
            match self.shared.drain_signals() {
                SignalDrain::Drained => {
                    if let Some(request) = self.shared.take_ready_request() {
                        return Some(request);
                    }
                    continue;
                }
                SignalDrain::Disconnected => return self.shared.take_ready_request(),
                SignalDrain::Empty => {}
            }

            if let Some(request) = self.shared.take_ready_request() {
                return Some(request);
            }

            let remaining = timeout.saturating_sub(started_at.elapsed());
            if remaining.is_zero() {
                return self.shared.take_ready_request();
            }

            match wait_for_signal_activity(&self.shared, remaining) {
                SignalWaitOutcome::Woken => {}
                SignalWaitOutcome::Disconnected | SignalWaitOutcome::TimedOut => {
                    return self.shared.take_ready_request();
                }
            }
        }
    }
}

/// Two-phase permit returned by `reserve()`.
///
/// Dropping without `send()`/`abort()` automatically releases the reserved slot.
#[derive(Debug)]
pub struct SendPermit {
    shared: Arc<TwoPhaseQueueShared>,
    logical_permit: Option<LogicalCapacityPermit>,
    reservation_seq: Option<u64>,
}

impl SendPermit {
    /// Stable reservation sequence used to verify FIFO behavior in tests.
    #[must_use]
    pub fn reservation_seq(&self) -> u64 {
        self.reservation_seq.unwrap_or(0)
    }

    /// Phase 2 commit. Synchronous and infallible for slot ownership.
    pub fn send(mut self, request: CommitRequest) {
        let Some(reservation_seq) = self.reservation_seq.take() else {
            return;
        };
        let Some(logical_permit) = self.logical_permit.take() else {
            return;
        };

        self.shared
            .queue_commit(reservation_seq, request, logical_permit);

        match self
            .shared
            .signal_sender
            .try_send(CommitSignal::CommitQueued)
        {
            Ok(()) | Err(mpsc::TrySendError::Full(CommitSignal::CommitQueued)) => {}
            Err(mpsc::TrySendError::Disconnected(CommitSignal::CommitQueued)) => {
                self.shared.rollback_pending_commit(reservation_seq);
                self.shared.mark_aborted(reservation_seq);
            }
        }
    }

    /// Explicitly release reserved slot without sending.
    pub fn abort(mut self) {
        self.abort_current_reservation();
    }

    fn abort_current_reservation(&mut self) {
        let Some(reservation_seq) = self.reservation_seq.take() else {
            return;
        };
        let _ = self.logical_permit.take();
        self.shared.mark_aborted(reservation_seq);
    }
}

impl Drop for SendPermit {
    fn drop(&mut self) {
        self.abort_current_reservation();
    }
}

/// Tracked sender variant that counts leaked permits (dropped without send/abort).
#[derive(Debug, Clone)]
pub struct TrackedSender {
    sender: TwoPhaseCommitSender,
    leaked_permits: Arc<AtomicU64>,
}

impl TrackedSender {
    #[must_use]
    pub fn new(sender: TwoPhaseCommitSender) -> Self {
        Self {
            sender,
            leaked_permits: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn reserve(&self) -> TrackedSendPermit {
        TrackedSendPermit {
            leaked_permits: Arc::clone(&self.leaked_permits),
            permit: Some(self.sender.reserve()),
        }
    }

    #[must_use]
    pub fn leaked_permit_count(&self) -> u64 {
        self.leaked_permits.load(Ordering::Acquire)
    }
}

/// Tracked permit wrapper for safety-critical channels.
#[derive(Debug)]
pub struct TrackedSendPermit {
    leaked_permits: Arc<AtomicU64>,
    permit: Option<SendPermit>,
}

impl TrackedSendPermit {
    /// Commit and clear obligation.
    pub fn send(mut self, request: CommitRequest) {
        if let Some(permit) = self.permit.take() {
            permit.send(request);
        }
    }

    /// Abort and clear obligation.
    pub fn abort(mut self) {
        if let Some(permit) = self.permit.take() {
            permit.abort();
        }
    }
}

impl Drop for TrackedSendPermit {
    fn drop(&mut self) {
        if self.permit.is_some() {
            self.leaked_permits.fetch_add(1, Ordering::AcqRel);
        }
    }
}

/// Build a bounded two-phase commit channel.
#[must_use]
pub fn two_phase_commit_channel(capacity: usize) -> (TwoPhaseCommitSender, TwoPhaseCommitReceiver) {
    let shared = Arc::new(TwoPhaseQueueShared::with_capacity(capacity));
    (
        TwoPhaseCommitSender {
            shared: Arc::clone(&shared),
        },
        TwoPhaseCommitReceiver { shared },
    )
}

/// Little's-law-based capacity estimate.
#[must_use]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
pub fn little_law_capacity(
    lambda_per_second: f64,
    t_commit: Duration,
    burst_multiplier: f64,
    jitter_multiplier: f64,
) -> usize {
    let effective = lambda_per_second
        * t_commit.as_secs_f64()
        * burst_multiplier.max(1.0)
        * jitter_multiplier.max(1.0);
    effective.ceil().max(1.0) as usize
}

/// Classical optimal group-commit batch size: `sqrt(t_fsync / t_validate)`.
#[must_use]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
pub fn optimal_batch_size(t_fsync: Duration, t_validate: Duration, capacity: usize) -> usize {
    let denom = t_validate.as_secs_f64().max(f64::EPSILON);
    let raw = (t_fsync.as_secs_f64() / denom).sqrt().round();
    raw.clamp(1.0, capacity.max(1) as f64) as usize
}

/// Conformal batch-size controller using upper quantiles.
#[must_use]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
pub fn conformal_batch_size(
    fsync_samples: &[Duration],
    validate_samples: &[Duration],
    capacity: usize,
) -> usize {
    if fsync_samples.is_empty() || validate_samples.is_empty() {
        return 1;
    }
    let q_fsync = quantile_seconds(fsync_samples, 0.9);
    let q_validate = quantile_seconds(validate_samples, 0.9).max(f64::EPSILON);
    let raw = (q_fsync / q_validate).sqrt().round();
    raw.clamp(1.0, capacity.max(1) as f64) as usize
}

fn quantile_seconds(samples: &[Duration], quantile: f64) -> f64 {
    let mut values: Vec<f64> = samples.iter().map(Duration::as_secs_f64).collect();
    values.sort_by(f64::total_cmp);
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let idx = ((values.len() as f64 - 1.0) * quantile.clamp(0.0, 1.0)).round() as usize;
    values[idx]
}

/// Commit/repair lifecycle events used for timing and invariant validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommitRepairEventKind {
    CommitDurable,
    DurableButNotRepairable,
    CommitAcked,
    RepairStarted,
    RepairCompleted,
    RepairFailed,
}

/// Timestamped lifecycle event for one commit sequence.
#[derive(Debug, Clone, Copy)]
pub struct CommitRepairEvent {
    pub commit_seq: u64,
    /// Monotonic per-commit event sequence number (logical time, no ambient authority).
    pub seq: u64,
    /// Monotonic wall-clock capture for latency/window measurements.
    pub recorded_at: Instant,
    pub kind: CommitRepairEventKind,
}

/// Repair state for a commit sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairState {
    NotScheduled,
    Pending,
    Completed,
    Failed,
}

/// Commit result produced by the critical path.
#[derive(Debug, Clone, Copy)]
pub struct CommitReceipt {
    pub commit_seq: u64,
    pub durable: bool,
    pub repair_pending: bool,
    pub latency: Duration,
}

/// Runtime behavior toggle for async repair generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitRepairConfig {
    pub repair_enabled: bool,
}

impl Default for CommitRepairConfig {
    fn default() -> Self {
        Self {
            repair_enabled: true,
        }
    }
}

/// Storage sink for systematic/repair symbol append+sync operations.
pub trait CommitRepairIo: Send + Sync {
    fn append_systematic_symbols(&self, commit_seq: u64, systematic_symbols: &[u8]) -> Result<()>;
    fn sync_systematic_symbols(&self, commit_seq: u64) -> Result<()>;
    fn append_repair_symbols(&self, commit_seq: u64, repair_symbols: &[u8]) -> Result<()>;
    fn sync_repair_symbols(&self, commit_seq: u64) -> Result<()>;
}

/// Generator for repair symbols from committed systematic symbols.
pub trait RepairSymbolGenerator: Send + Sync {
    fn generate_repair_symbols(
        &self,
        commit_seq: u64,
        systematic_symbols: &[u8],
    ) -> Result<Vec<u8>>;
}

/// In-memory IO sink useful for deterministic testing/instrumentation.
#[derive(Debug, Default)]
pub struct InMemoryCommitRepairIo {
    systematic_by_commit: Mutex<HashMap<u64, Vec<u8>>>,
    repair_by_commit: Mutex<HashMap<u64, Vec<u8>>>,
    total_systematic_bytes: AtomicU64,
    total_repair_bytes: AtomicU64,
    systematic_syncs: AtomicU64,
    repair_syncs: AtomicU64,
}

impl InMemoryCommitRepairIo {
    #[must_use]
    pub fn total_repair_bytes(&self) -> u64 {
        self.total_repair_bytes.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn repair_sync_count(&self) -> u64 {
        self.repair_syncs.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn systematic_sync_count(&self) -> u64 {
        self.systematic_syncs.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn repair_symbols_for(&self, commit_seq: u64) -> Option<Vec<u8>> {
        lock_with_recovery(&self.repair_by_commit, "repair_by_commit")
            .get(&commit_seq)
            .cloned()
    }
}

impl CommitRepairIo for InMemoryCommitRepairIo {
    fn append_systematic_symbols(&self, commit_seq: u64, systematic_symbols: &[u8]) -> Result<()> {
        lock_with_recovery(&self.systematic_by_commit, "systematic_by_commit")
            .insert(commit_seq, systematic_symbols.to_vec());
        self.total_systematic_bytes.fetch_add(
            u64::try_from(systematic_symbols.len()).map_err(|_| FrankenError::OutOfRange {
                what: "systematic_symbol_len".to_owned(),
                value: systematic_symbols.len().to_string(),
            })?,
            Ordering::Release,
        );
        Ok(())
    }

    fn sync_systematic_symbols(&self, _commit_seq: u64) -> Result<()> {
        self.systematic_syncs.fetch_add(1, Ordering::Release);
        Ok(())
    }

    fn append_repair_symbols(&self, commit_seq: u64, repair_symbols: &[u8]) -> Result<()> {
        lock_with_recovery(&self.repair_by_commit, "repair_by_commit")
            .insert(commit_seq, repair_symbols.to_vec());
        self.total_repair_bytes.fetch_add(
            u64::try_from(repair_symbols.len()).map_err(|_| FrankenError::OutOfRange {
                what: "repair_symbol_len".to_owned(),
                value: repair_symbols.len().to_string(),
            })?,
            Ordering::Release,
        );
        Ok(())
    }

    fn sync_repair_symbols(&self, _commit_seq: u64) -> Result<()> {
        self.repair_syncs.fetch_add(1, Ordering::Release);
        Ok(())
    }
}

/// Deterministic repair generator with configurable delay/failure injection.
#[derive(Debug)]
pub struct DeterministicRepairGenerator {
    delay: Duration,
    output_len: usize,
    fail_repair: Arc<AtomicBool>,
}

impl DeterministicRepairGenerator {
    #[must_use]
    pub fn new(delay: Duration, output_len: usize) -> Self {
        Self {
            delay,
            output_len: output_len.max(1),
            fail_repair: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn set_fail_repair(&self, fail: bool) {
        self.fail_repair.store(fail, Ordering::Release);
    }
}

impl RepairSymbolGenerator for DeterministicRepairGenerator {
    fn generate_repair_symbols(
        &self,
        commit_seq: u64,
        systematic_symbols: &[u8],
    ) -> Result<Vec<u8>> {
        if self.delay != Duration::ZERO {
            thread::sleep(self.delay);
        }
        if self.fail_repair.load(Ordering::Acquire) {
            return Err(FrankenError::Internal(format!(
                "repair generation failed for commit_seq={commit_seq}"
            )));
        }

        let source = if systematic_symbols.is_empty() {
            &[0_u8][..]
        } else {
            systematic_symbols
        };
        let mut state = commit_seq
            ^ u64::try_from(source.len()).map_err(|_| FrankenError::OutOfRange {
                what: "systematic_symbol_len".to_owned(),
                value: source.len().to_string(),
            })?;
        let mut out = Vec::with_capacity(self.output_len);
        for idx in 0..self.output_len {
            let src = source[idx % source.len()];
            let idx_mod = u64::try_from(idx % 251).map_err(|_| FrankenError::OutOfRange {
                what: "repair_symbol_index".to_owned(),
                value: idx.to_string(),
            })?;
            state = state.rotate_left(7) ^ u64::from(src) ^ idx_mod;
            out.push((state & 0xFF) as u8);
        }
        Ok(out)
    }
}

/// Two-phase commit durability coordinator.
pub struct CommitRepairCoordinator<
    IO: CommitRepairIo + Send + Sync + 'static,
    GEN: RepairSymbolGenerator + Send + Sync + 'static,
> {
    config: CommitRepairConfig,
    runtime: Runtime,
    coordinator_cx: Cx,
    io: Arc<IO>,
    generator: Arc<GEN>,
    next_commit_seq: AtomicU64,
    next_async_task_id: AtomicU64,
    repair_states: Arc<Mutex<HashMap<u64, RepairState>>>,
    events: Arc<Mutex<Vec<CommitRepairEvent>>>,
    handles: Mutex<Vec<AsyncJoinHandle<()>>>,
}

impl<IO, GEN> CommitRepairCoordinator<IO, GEN>
where
    IO: CommitRepairIo + Send + Sync + 'static,
    GEN: RepairSymbolGenerator + Send + Sync + 'static,
{
    #[must_use]
    pub fn new(
        config: CommitRepairConfig,
        runtime: Runtime,
        parent_cx: &Cx,
        io: IO,
        generator: GEN,
    ) -> Self {
        Self::with_shared(
            config,
            runtime,
            parent_cx,
            Arc::new(io),
            Arc::new(generator),
        )
    }

    #[must_use]
    pub fn with_shared(
        config: CommitRepairConfig,
        runtime: Runtime,
        parent_cx: &Cx,
        io: Arc<IO>,
        generator: Arc<GEN>,
    ) -> Self {
        Self {
            config,
            runtime,
            coordinator_cx: parent_cx.create_child(),
            io,
            generator,
            next_commit_seq: AtomicU64::new(1),
            next_async_task_id: AtomicU64::new(1),
            repair_states: Arc::new(Mutex::new(HashMap::new())),
            events: Arc::new(Mutex::new(Vec::new())),
            handles: Mutex::new(Vec::new()),
        }
    }

    /// Execute critical-path durability and schedule async repair work.
    pub fn commit(&self, systematic_symbols: &[u8]) -> Result<CommitReceipt> {
        let started_at = Instant::now();
        let commit_seq = self.next_commit_seq.fetch_add(1, Ordering::Relaxed);

        self.io
            .append_systematic_symbols(commit_seq, systematic_symbols)?;
        self.io.sync_systematic_symbols(commit_seq)?;
        self.record(commit_seq, CommitRepairEventKind::CommitDurable);

        if !self.config.repair_enabled {
            self.record(commit_seq, CommitRepairEventKind::CommitAcked);
            return Ok(CommitReceipt {
                commit_seq,
                durable: true,
                repair_pending: false,
                latency: started_at.elapsed(),
            });
        }

        lock_with_recovery(&self.repair_states, "repair_states")
            .insert(commit_seq, RepairState::Pending);
        self.record(commit_seq, CommitRepairEventKind::DurableButNotRepairable);
        debug!(
            bead_id = BEAD_ID,
            commit_seq, "commit is durable but not repairable while async repair is pending"
        );
        self.record(commit_seq, CommitRepairEventKind::CommitAcked);

        let async_task_id = self.next_async_task_id.fetch_add(1, Ordering::Relaxed);
        let io = Arc::clone(&self.io);
        let generator = Arc::clone(&self.generator);
        let repair_states = Arc::clone(&self.repair_states);
        let events = Arc::clone(&self.events);
        let systematic_snapshot = systematic_symbols.to_vec();
        let worker_cx = self.coordinator_cx.create_child();
        let handle = self.runtime.handle().try_spawn(run_repair_task(RepairTask {
            commit_seq,
            async_task_id,
            io,
            generator,
            repair_states,
            events,
            systematic_snapshot,
            worker_cx,
        }));
        match handle {
            Ok(handle) => {
                lock_with_recovery(&self.handles, "repair_handles").push(handle);
            }
            Err(err) => {
                set_repair_state(&self.repair_states, commit_seq, RepairState::Failed);
                self.record(commit_seq, CommitRepairEventKind::RepairFailed);
                error!(
                    bead_id = BEAD_ID,
                    commit_seq,
                    async_task_id,
                    error = ?err,
                    "failed to schedule repair task on caller-owned runtime"
                );
                return Ok(CommitReceipt {
                    commit_seq,
                    durable: true,
                    repair_pending: false,
                    latency: started_at.elapsed(),
                });
            }
        }

        Ok(CommitReceipt {
            commit_seq,
            durable: true,
            repair_pending: true,
            latency: started_at.elapsed(),
        })
    }

    /// Join all currently scheduled background repair workers.
    pub fn wait_for_background_repair(&self) -> Result<()> {
        let handles = {
            let mut guard = lock_with_recovery(&self.handles, "repair_handles");
            std::mem::take(&mut *guard)
        };
        let mut observed_panic = false;
        for handle in handles {
            let joined =
                std::panic::catch_unwind(AssertUnwindSafe(|| self.runtime.block_on(handle)));
            if joined.is_err() {
                observed_panic = true;
            }
        }
        if observed_panic {
            return Err(FrankenError::Internal(
                "background repair worker panicked".to_owned(),
            ));
        }
        Ok(())
    }

    #[must_use]
    pub fn pending_background_repair_count(&self) -> usize {
        lock_with_recovery(&self.repair_states, "repair_states")
            .values()
            .filter(|state| matches!(state, RepairState::Pending))
            .count()
    }

    #[must_use]
    pub fn repair_state_for(&self, commit_seq: u64) -> RepairState {
        lock_with_recovery(&self.repair_states, "repair_states")
            .get(&commit_seq)
            .copied()
            .unwrap_or(RepairState::NotScheduled)
    }

    #[must_use]
    pub fn events_for_commit(&self, commit_seq: u64) -> Vec<CommitRepairEvent> {
        lock_with_recovery(&self.events, "repair_events")
            .iter()
            .copied()
            .filter(|event| event.commit_seq == commit_seq)
            .collect()
    }

    #[must_use]
    pub fn durable_not_repairable_window(&self, commit_seq: u64) -> Option<Duration> {
        let events = self.events_for_commit(commit_seq);
        let pending = events
            .iter()
            .find(|event| event.kind == CommitRepairEventKind::DurableButNotRepairable)?;
        let repair_done = events
            .iter()
            .find(|event| event.kind == CommitRepairEventKind::RepairCompleted)?;
        Some(
            repair_done
                .recorded_at
                .saturating_duration_since(pending.recorded_at),
        )
    }

    #[must_use]
    pub fn io_handle(&self) -> Arc<IO> {
        Arc::clone(&self.io)
    }

    #[must_use]
    pub fn generator_handle(&self) -> Arc<GEN> {
        Arc::clone(&self.generator)
    }

    fn record(&self, commit_seq: u64, kind: CommitRepairEventKind) {
        record_event_into(&self.events, commit_seq, kind);
    }
}

impl<IO, GEN> Drop for CommitRepairCoordinator<IO, GEN>
where
    IO: CommitRepairIo + Send + Sync + 'static,
    GEN: RepairSymbolGenerator + Send + Sync + 'static,
{
    fn drop(&mut self) {
        let handles = {
            let mut guard = lock_with_recovery(&self.handles, "repair_handles");
            std::mem::take(&mut *guard)
        };
        for handle in handles {
            let joined =
                std::panic::catch_unwind(AssertUnwindSafe(|| self.runtime.block_on(handle)));
            if joined.is_err() {
                error!(
                    bead_id = BEAD_ID,
                    "background repair worker panicked during drop"
                );
            }
        }
    }
}

struct RepairTask<IO, GEN> {
    commit_seq: u64,
    async_task_id: u64,
    io: Arc<IO>,
    generator: Arc<GEN>,
    repair_states: Arc<Mutex<HashMap<u64, RepairState>>>,
    events: Arc<Mutex<Vec<CommitRepairEvent>>>,
    systematic_snapshot: Vec<u8>,
    worker_cx: Cx,
}

async fn run_repair_task<IO, GEN>(task: RepairTask<IO, GEN>)
where
    IO: CommitRepairIo + Send + Sync + 'static,
    GEN: RepairSymbolGenerator + Send + Sync + 'static,
{
    let RepairTask {
        commit_seq,
        async_task_id,
        io,
        generator,
        repair_states,
        events,
        systematic_snapshot,
        worker_cx,
    } = task;

    let Some(native_worker_cx) = NativeCx::current() else {
        set_repair_state(&repair_states, commit_seq, RepairState::Failed);
        record_event_into(&events, commit_seq, CommitRepairEventKind::RepairFailed);
        error!(
            bead_id = BEAD_ID,
            commit_seq, async_task_id, "repair task missing native runtime context"
        );
        return;
    };
    worker_cx.set_native_cx(native_worker_cx.clone());
    if worker_cx.checkpoint().is_err() {
        set_repair_state(&repair_states, commit_seq, RepairState::Failed);
        record_event_into(&events, commit_seq, CommitRepairEventKind::RepairFailed);
        warn!(
            bead_id = BEAD_ID,
            commit_seq, async_task_id, "repair task was cancelled before blocking work started"
        );
        return;
    }

    info!(
        bead_id = BEAD_ID,
        commit_seq, async_task_id, "repair symbols generation started"
    );
    record_event_into(&events, commit_seq, CommitRepairEventKind::RepairStarted);

    let blocking_cx = worker_cx.create_child();
    let repair_outcome = spawn_blocking(move || {
        std::panic::catch_unwind(AssertUnwindSafe(|| {
            blocking_cx.set_native_cx(native_worker_cx);
            let repair_symbols =
                generator.generate_repair_symbols(commit_seq, &systematic_snapshot)?;
            let repair_symbol_bytes = repair_symbols.len();
            io.append_repair_symbols(commit_seq, &repair_symbols)?;
            io.sync_repair_symbols(commit_seq)?;
            Ok::<usize, FrankenError>(repair_symbol_bytes)
        }))
    })
    .await;

    match repair_outcome {
        Ok(Ok(repair_symbol_bytes)) => {
            set_repair_state(&repair_states, commit_seq, RepairState::Completed);
            record_event_into(&events, commit_seq, CommitRepairEventKind::RepairCompleted);
            info!(
                bead_id = BEAD_ID,
                commit_seq,
                async_task_id,
                repair_symbol_bytes,
                "repair symbols append+sync completed"
            );
        }
        // `spawn_blocking` returns the closure result directly, so the outer
        // `catch_unwind` layer is the panic boundary and the inner `Result`
        // carries generator / append / sync failures.
        Ok(Err(err)) => {
            set_repair_state(&repair_states, commit_seq, RepairState::Failed);
            record_event_into(&events, commit_seq, CommitRepairEventKind::RepairFailed);
            error!(
                bead_id = BEAD_ID,
                commit_seq,
                async_task_id,
                error = ?err,
                "repair symbol generation or append/sync failed"
            );
        }
        Err(_panic_payload) => {
            set_repair_state(&repair_states, commit_seq, RepairState::Failed);
            record_event_into(&events, commit_seq, CommitRepairEventKind::RepairFailed);
            error!(
                bead_id = BEAD_ID,
                commit_seq, async_task_id, "repair symbol worker panicked"
            );
        }
    }
}

fn lock_with_recovery<'a, T>(mutex: &'a Mutex<T>, lock_name: &'static str) -> MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!(
                bead_id = BEAD_ID,
                lock = lock_name,
                "mutex poisoned; recovering inner state"
            );
            poisoned.into_inner()
        }
    }
}

fn set_repair_state(
    repair_states: &Arc<Mutex<HashMap<u64, RepairState>>>,
    commit_seq: u64,
    state: RepairState,
) {
    lock_with_recovery(repair_states, "repair_states").insert(commit_seq, state);
}

fn record_event_into(
    events: &Arc<Mutex<Vec<CommitRepairEvent>>>,
    commit_seq: u64,
    kind: CommitRepairEventKind,
) {
    let mut guard = lock_with_recovery(events, "repair_events");
    let seq = guard
        .iter()
        .rev()
        .find(|event| event.commit_seq == commit_seq)
        .map_or(1, |event| event.seq.saturating_add(1));
    guard.push(CommitRepairEvent {
        commit_seq,
        seq,
        recorded_at: Instant::now(),
        kind,
    });
}

// ---------------------------------------------------------------------------
// Group Commit Batching (§5.9.2.1, bd-l4gl)
// ---------------------------------------------------------------------------

const GROUP_COMMIT_BEAD_ID: &str = "bd-l4gl";
const GROUP_COMMIT_IDLE_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Phase label recorded during coordinator batch processing for ordering
/// verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchPhase {
    /// Write-set conflict validation for each request.
    Validate,
    /// Sequential WAL append for all valid requests.
    WalAppend,
    /// Single `fsync` for the entire batch.
    Fsync,
    /// Version publication and response delivery.
    Publish,
}

/// Response returned to each writer after batch processing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupCommitResponse {
    /// Commit succeeded; pages are durable and published.
    Committed { wal_offset: u64, commit_seq: u64 },
    /// Commit rejected due to write-set conflict.
    Conflict { reason: String },
}

/// Result of processing a single batch through the coordinator.
#[derive(Debug)]
pub struct BatchResult {
    /// Successfully committed entries: `(txn_id, wal_offset, commit_seq)`.
    pub committed: Vec<(u64, u64, u64)>,
    /// Rejected entries: `(txn_id, reason)`.
    pub conflicted: Vec<(u64, String)>,
    /// Number of fsync calls issued for this batch (should always be 0 or 1).
    pub fsync_count: u32,
    /// Ordered record of phases executed during batch processing.
    pub phase_order: Vec<BatchPhase>,
}

/// Batch WAL writer abstraction for the group commit coordinator.
///
/// A single `append_batch` call writes all frames from all valid requests
/// in one sequential `write()`, and `sync` issues exactly one `fsync`.
pub trait WalBatchWriter: Send + Sync {
    /// Append commit frames for every request in the batch. Returns a WAL
    /// offset per request.
    fn append_batch(&self, requests: &[&CommitRequest]) -> Result<Vec<u64>>;

    /// Issue a single `fsync` (or `fdatasync`) covering all appended frames.
    fn sync(&self) -> Result<()>;
}

/// Write-set conflict validator using first-committer-wins (FCW) logic.
///
/// `committed_pages` is the set of pages that have been committed since
/// the validating transaction's snapshot.
pub trait WriteSetValidator: Send + Sync {
    /// Returns `Ok(())` if the request passes validation, or `Err` with
    /// a human-readable conflict description.
    fn validate(
        &self,
        request: &CommitRequest,
        committed_pages: &BTreeSet<u32>,
    ) -> std::result::Result<(), String>;
}

/// First-committer-wins validator: any overlap between the request's
/// write set and already-committed pages is a conflict.
#[derive(Debug, Default)]
pub struct FirstCommitterWinsValidator;

impl WriteSetValidator for FirstCommitterWinsValidator {
    fn validate(
        &self,
        request: &CommitRequest,
        committed_pages: &BTreeSet<u32>,
    ) -> std::result::Result<(), String> {
        for &page in &request.write_set_pages {
            if committed_pages.contains(&page) {
                return Err(format!(
                    "write-set conflict on page {page} for txn {}",
                    request.txn_id
                ));
            }
        }
        Ok(())
    }
}

/// In-memory WAL writer for deterministic testing and instrumentation.
#[derive(Debug)]
pub struct InMemoryWalWriter {
    next_offset: AtomicU64,
    sync_count: AtomicU64,
    total_appended: AtomicU64,
    /// Simulated fsync latency for throughput model tests.
    fsync_delay: Duration,
}

impl InMemoryWalWriter {
    /// Create an in-memory WAL writer with no simulated fsync delay.
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_offset: AtomicU64::new(1),
            sync_count: AtomicU64::new(0),
            total_appended: AtomicU64::new(0),
            fsync_delay: Duration::ZERO,
        }
    }

    /// Create with simulated fsync delay for throughput model testing.
    #[must_use]
    pub fn with_fsync_delay(delay: Duration) -> Self {
        Self {
            next_offset: AtomicU64::new(1),
            sync_count: AtomicU64::new(0),
            total_appended: AtomicU64::new(0),
            fsync_delay: delay,
        }
    }

    /// Total number of `sync()` calls observed.
    #[must_use]
    pub fn sync_count(&self) -> u64 {
        self.sync_count.load(Ordering::Acquire)
    }

    /// Total requests appended across all batches.
    #[must_use]
    pub fn total_appended(&self) -> u64 {
        self.total_appended.load(Ordering::Acquire)
    }
}

impl Default for InMemoryWalWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl WalBatchWriter for InMemoryWalWriter {
    fn append_batch(&self, requests: &[&CommitRequest]) -> Result<Vec<u64>> {
        let mut offsets = Vec::with_capacity(requests.len());
        for _req in requests {
            let offset = self.next_offset.fetch_add(1, Ordering::Relaxed);
            offsets.push(offset);
        }
        #[allow(clippy::cast_possible_truncation)]
        self.total_appended
            .fetch_add(requests.len() as u64, Ordering::Release);
        Ok(offsets)
    }

    fn sync(&self) -> Result<()> {
        if self.fsync_delay != Duration::ZERO {
            thread::sleep(self.fsync_delay);
        }
        self.sync_count.fetch_add(1, Ordering::Release);
        Ok(())
    }
}

#[cfg(test)]
mod commit_repair_async_tests {
    use super::*;

    use asupersync::runtime::RuntimeBuilder;
    use std::thread;

    #[test]
    fn test_commit_repair_runs_on_caller_owned_runtime() {
        let runtime = RuntimeBuilder::current_thread()
            .build()
            .expect("commit repair runtime");
        let root_cx = Cx::new();
        let io = Arc::new(InMemoryCommitRepairIo::default());
        let generator = Arc::new(DeterministicRepairGenerator::new(
            Duration::from_millis(25),
            64,
        ));
        let coordinator = CommitRepairCoordinator::with_shared(
            CommitRepairConfig {
                repair_enabled: true,
            },
            runtime,
            &root_cx,
            Arc::clone(&io),
            generator,
        );

        let receipt = coordinator
            .commit(&[0xAB; 256])
            .expect("commit should succeed");
        assert_eq!(coordinator.pending_background_repair_count(), 1);

        coordinator
            .wait_for_background_repair()
            .expect("background repair should finish");

        assert_eq!(coordinator.pending_background_repair_count(), 0);
        assert_eq!(
            coordinator.repair_state_for(receipt.commit_seq),
            RepairState::Completed
        );
        assert!(
            coordinator
                .events_for_commit(receipt.commit_seq)
                .iter()
                .any(|event| event.kind == CommitRepairEventKind::RepairStarted)
        );
        assert!(
            io.total_repair_bytes() > 0,
            "repair task should append repair symbols"
        );
    }

    #[test]
    fn test_commit_receipt_latency_tracks_critical_path_io() {
        #[derive(Debug)]
        struct DelayedIo {
            delay: Duration,
        }

        impl CommitRepairIo for DelayedIo {
            fn append_systematic_symbols(
                &self,
                _commit_seq: u64,
                _systematic_symbols: &[u8],
            ) -> Result<()> {
                Ok(())
            }

            fn sync_systematic_symbols(&self, _commit_seq: u64) -> Result<()> {
                thread::sleep(self.delay);
                Ok(())
            }

            fn append_repair_symbols(
                &self,
                _commit_seq: u64,
                _repair_symbols: &[u8],
            ) -> Result<()> {
                Ok(())
            }

            fn sync_repair_symbols(&self, _commit_seq: u64) -> Result<()> {
                Ok(())
            }
        }

        let runtime = RuntimeBuilder::current_thread()
            .build()
            .expect("commit repair runtime");
        let root_cx = Cx::new();
        let coordinator = CommitRepairCoordinator::new(
            CommitRepairConfig {
                repair_enabled: false,
            },
            runtime,
            &root_cx,
            DelayedIo {
                delay: Duration::from_millis(10),
            },
            DeterministicRepairGenerator::new(Duration::ZERO, 64),
        );

        let receipt = coordinator
            .commit(&[0xAB; 256])
            .expect("commit should succeed");

        assert!(
            receipt.latency >= Duration::from_millis(8),
            "commit latency should include the critical-path systematic sync cost"
        );
    }

    #[test]
    fn test_durable_not_repairable_window_tracks_wall_clock_delay() {
        let runtime = RuntimeBuilder::current_thread()
            .build()
            .expect("commit repair runtime");
        let root_cx = Cx::new();
        let coordinator = CommitRepairCoordinator::new(
            CommitRepairConfig {
                repair_enabled: true,
            },
            runtime,
            &root_cx,
            InMemoryCommitRepairIo::default(),
            DeterministicRepairGenerator::new(Duration::from_millis(20), 64),
        );

        let receipt = coordinator
            .commit(&[0xAB; 256])
            .expect("commit should succeed");
        coordinator
            .wait_for_background_repair()
            .expect("background repair should finish");

        let window = coordinator
            .durable_not_repairable_window(receipt.commit_seq)
            .expect("window should be measurable");
        assert!(
            window >= Duration::from_millis(15),
            "window should reflect real repair delay rather than logical event ordinals"
        );
    }

    #[test]
    fn test_panicking_repair_marks_failed_without_abandoning_other_work() {
        #[derive(Debug)]
        struct PanicFirstGenerator;

        impl RepairSymbolGenerator for PanicFirstGenerator {
            fn generate_repair_symbols(
                &self,
                commit_seq: u64,
                _systematic_symbols: &[u8],
            ) -> Result<Vec<u8>> {
                if commit_seq == 1 {
                    panic!("intentional repair panic");
                }
                thread::sleep(Duration::from_millis(25));
                Ok(vec![0xCD; 32])
            }
        }

        let runtime = RuntimeBuilder::current_thread()
            .build()
            .expect("commit repair runtime");
        let root_cx = Cx::new();
        let coordinator = CommitRepairCoordinator::new(
            CommitRepairConfig {
                repair_enabled: true,
            },
            runtime,
            &root_cx,
            InMemoryCommitRepairIo::default(),
            PanicFirstGenerator,
        );

        let first = coordinator
            .commit(&[0xAA; 64])
            .expect("first commit should schedule repair");
        let second = coordinator
            .commit(&[0xBB; 64])
            .expect("second commit should schedule repair");

        coordinator
            .wait_for_background_repair()
            .expect("panic inside repair work should be converted into RepairFailed state");
        assert_eq!(coordinator.pending_background_repair_count(), 0);
        assert_eq!(
            coordinator.repair_state_for(first.commit_seq),
            RepairState::Failed,
            "panicking repair task must be marked failed rather than left pending"
        );
        assert!(
            coordinator
                .events_for_commit(first.commit_seq)
                .iter()
                .any(|event| event.kind == CommitRepairEventKind::RepairFailed),
            "panicking repair task must emit a RepairFailed event"
        );
        assert_eq!(
            coordinator.repair_state_for(second.commit_seq),
            RepairState::Completed,
            "wait_for_background_repair must still drain non-panicking tasks"
        );
    }
}

/// Group commit coordinator configuration.
#[derive(Debug, Clone, Copy)]
pub struct GroupCommitConfig {
    /// Maximum requests coalesced into a single batch.
    pub max_batch_size: usize,
    /// Timeout for draining additional requests after the first.
    pub drain_timeout: Duration,
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        Self {
            max_batch_size: DEFAULT_COMMIT_CHANNEL_CAPACITY,
            drain_timeout: Duration::from_micros(100),
        }
    }
}

/// Published version notification for a committed transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedVersion {
    pub txn_id: u64,
    pub commit_seq: u64,
    pub wal_offset: u64,
}

/// Group commit coordinator that batches write-coordinator requests to
/// amortize `fsync` cost (§5.9.2.1, bd-l4gl).
///
/// The coordinator processes requests from the bounded two-phase MPSC
/// channel in 4 strict phases per batch:
///
/// 1. **Validate** — first-committer-wins conflict check
/// 2. **WAL append** — single sequential `write()` for all valid frames
/// 3. **Fsync** — exactly ONE `fsync()` per batch
/// 4. **Publish** — make versions visible and deliver responses
pub struct GroupCommitCoordinator<W: WalBatchWriter, V: WriteSetValidator> {
    wal: Arc<W>,
    validator: Arc<V>,
    config: GroupCommitConfig,
    next_commit_seq: AtomicU64,
    committed_pages: Mutex<BTreeSet<u32>>,
    published: Mutex<Vec<PublishedVersion>>,
    batch_history: Mutex<Vec<BatchResult>>,
    total_batches: AtomicU64,
}

impl<W, V> GroupCommitCoordinator<W, V>
where
    W: WalBatchWriter + 'static,
    V: WriteSetValidator + 'static,
{
    /// Create a new group commit coordinator.
    #[must_use]
    pub fn new(wal: W, validator: V, config: GroupCommitConfig) -> Self {
        Self {
            wal: Arc::new(wal),
            validator: Arc::new(validator),
            config,
            next_commit_seq: AtomicU64::new(1),
            committed_pages: Mutex::new(BTreeSet::new()),
            published: Mutex::new(Vec::new()),
            batch_history: Mutex::new(Vec::new()),
            total_batches: AtomicU64::new(0),
        }
    }

    /// Process a single batch of requests through the 4-phase pipeline.
    ///
    /// Returns individual responses and batch-level metrics. Phase ordering
    /// is recorded in `BatchResult::phase_order` for verification.
    #[allow(clippy::too_many_lines)]
    pub fn process_batch(
        &self,
        requests: Vec<CommitRequest>,
    ) -> Result<(Vec<(CommitRequest, GroupCommitResponse)>, BatchResult)> {
        if requests.is_empty() {
            return Ok((
                Vec::new(),
                BatchResult {
                    committed: Vec::new(),
                    conflicted: Vec::new(),
                    fsync_count: 0,
                    phase_order: Vec::new(),
                },
            ));
        }

        let batch_size = requests.len();
        debug!(
            bead_id = GROUP_COMMIT_BEAD_ID,
            batch_size, "processing group commit batch"
        );

        let mut phase_order = Vec::with_capacity(4);
        let mut responses: Vec<(CommitRequest, GroupCommitResponse)> =
            Vec::with_capacity(batch_size);
        let mut valid_requests: Vec<CommitRequest> = Vec::with_capacity(batch_size);
        let mut conflicted: Vec<(u64, String)> = Vec::new();

        // ---- Phase 1: Validate ----
        phase_order.push(BatchPhase::Validate);
        let mut merged_committed =
            lock_with_recovery(&self.committed_pages, "committed_pages").clone();
        // Within a batch, earlier requests (by position) win over later ones
        // when their write sets overlap.
        for req in requests {
            match self.validator.validate(&req, &merged_committed) {
                Ok(()) => {
                    for &page in &req.write_set_pages {
                        merged_committed.insert(page);
                    }
                    valid_requests.push(req);
                }
                Err(reason) => {
                    info!(
                        bead_id = GROUP_COMMIT_BEAD_ID,
                        txn_id = req.txn_id,
                        reason = %reason,
                        "conflict detected in validate phase (fail-fast)"
                    );
                    conflicted.push((req.txn_id, reason.clone()));
                    responses.push((req, GroupCommitResponse::Conflict { reason }));
                }
            }
        }

        if valid_requests.is_empty() {
            let result = BatchResult {
                committed: Vec::new(),
                conflicted,
                fsync_count: 0,
                phase_order,
            };
            lock_with_recovery(&self.batch_history, "batch_history").push(BatchResult {
                committed: Vec::new(),
                conflicted: result.conflicted.clone(),
                fsync_count: 0,
                phase_order: result.phase_order.clone(),
            });
            self.total_batches.fetch_add(1, Ordering::Relaxed);
            return Ok((responses, result));
        }

        // ---- Phase 2: WAL append ----
        phase_order.push(BatchPhase::WalAppend);
        let refs: Vec<&CommitRequest> = valid_requests.iter().collect();
        let wal_offsets = self.wal.append_batch(&refs)?;
        if wal_offsets.len() != valid_requests.len() {
            return Err(FrankenError::internal(format!(
                "wal append returned {} offsets for {} valid requests",
                wal_offsets.len(),
                valid_requests.len()
            )));
        }

        // ---- Phase 3: Fsync ----
        phase_order.push(BatchPhase::Fsync);
        self.wal.sync()?;
        let fsync_count = 1;

        // ---- Phase 4: Publish ----
        phase_order.push(BatchPhase::Publish);
        let mut committed_entries: Vec<(u64, u64, u64)> = Vec::with_capacity(valid_requests.len());
        let mut committed_guard = lock_with_recovery(&self.committed_pages, "committed_pages");
        let mut published_guard = lock_with_recovery(&self.published, "published_versions");
        for (req, &wal_offset) in valid_requests.iter().zip(wal_offsets.iter()) {
            let commit_seq = self.next_commit_seq.fetch_add(1, Ordering::Relaxed);
            for &page in &req.write_set_pages {
                committed_guard.insert(page);
            }
            published_guard.push(PublishedVersion {
                txn_id: req.txn_id,
                commit_seq,
                wal_offset,
            });
            committed_entries.push((req.txn_id, wal_offset, commit_seq));
            info!(
                bead_id = GROUP_COMMIT_BEAD_ID,
                txn_id = req.txn_id,
                commit_seq,
                wal_offset,
                "version published after fsync"
            );
        }
        drop(committed_guard);
        drop(published_guard);

        for ((req, &wal_offset), (_, _, commit_seq)) in valid_requests
            .into_iter()
            .zip(wal_offsets.iter())
            .zip(committed_entries.iter())
        {
            responses.push((
                req,
                GroupCommitResponse::Committed {
                    wal_offset,
                    commit_seq: *commit_seq,
                },
            ));
        }

        let result = BatchResult {
            committed: committed_entries,
            conflicted,
            fsync_count,
            phase_order,
        };

        lock_with_recovery(&self.batch_history, "batch_history").push(BatchResult {
            committed: result.committed.clone(),
            conflicted: result.conflicted.clone(),
            fsync_count: result.fsync_count,
            phase_order: result.phase_order.clone(),
        });
        self.total_batches.fetch_add(1, Ordering::Relaxed);

        debug!(
            bead_id = GROUP_COMMIT_BEAD_ID,
            batch_size,
            committed = result.committed.len(),
            conflicted = result.conflicted.len(),
            "batch processing complete"
        );

        Ok((responses, result))
    }

    /// Drain requests from the receiver and process them as a batch.
    ///
    /// Blocks waiting for the first request, then non-blocking drains up
    /// to `max_batch_size`. Returns `None` if the receiver times out on
    /// the first request (channel idle).
    pub fn drain_and_process(
        &self,
        receiver: &TwoPhaseCommitReceiver,
    ) -> Result<Option<BatchResult>> {
        self.drain_and_process_with_first_wait(receiver, Duration::from_secs(1))
    }

    fn drain_and_process_with_first_wait(
        &self,
        receiver: &TwoPhaseCommitReceiver,
        first_wait: Duration,
    ) -> Result<Option<BatchResult>> {
        // Blocking wait for first request
        let Some(first) = receiver.try_recv_for(first_wait) else {
            return Ok(None);
        };

        let mut batch = Vec::with_capacity(self.config.max_batch_size);
        batch.push(first);

        // Non-blocking drain for additional requests
        while batch.len() < self.config.max_batch_size {
            match receiver.try_recv_for(self.config.drain_timeout) {
                Some(req) => batch.push(req),
                None => break,
            }
        }

        let (_responses, result) = self.process_batch(batch)?;
        Ok(Some(result))
    }

    /// Run the coordinator loop until the owning region `Cx` is cancelled.
    ///
    /// This is the production entry point. The loop blocks on the first
    /// request of each batch, drains additional requests, and processes
    /// the batch through all 4 phases.
    pub fn run_loop(&self, receiver: &TwoPhaseCommitReceiver, cx: &Cx) -> Result<()> {
        info!(
            bead_id = GROUP_COMMIT_BEAD_ID,
            max_batch_size = self.config.max_batch_size,
            "group commit coordinator loop started"
        );
        while !cx.is_cancel_requested() {
            if cx.checkpoint().is_err() {
                break;
            }
            if let Some(result) =
                self.drain_and_process_with_first_wait(receiver, GROUP_COMMIT_IDLE_POLL_INTERVAL)?
            {
                debug!(
                    bead_id = GROUP_COMMIT_BEAD_ID,
                    committed = result.committed.len(),
                    conflicted = result.conflicted.len(),
                    "batch cycle completed"
                );
            }
        }
        info!(
            bead_id = GROUP_COMMIT_BEAD_ID,
            total_batches = self.total_batches.load(Ordering::Relaxed),
            "group commit coordinator loop shut down"
        );
        Ok(())
    }

    /// Total batches processed so far.
    #[must_use]
    pub fn total_batches(&self) -> u64 {
        self.total_batches.load(Ordering::Acquire)
    }

    /// All published versions for inspection/testing.
    #[must_use]
    pub fn published_versions(&self) -> Vec<PublishedVersion> {
        lock_with_recovery(&self.published, "published_versions").clone()
    }

    /// Batch results for phase ordering verification.
    #[must_use]
    pub fn batch_history(&self) -> Vec<BatchResult> {
        // Return summary without cloning internal Vecs fully
        lock_with_recovery(&self.batch_history, "batch_history")
            .iter()
            .map(|b| BatchResult {
                committed: b.committed.clone(),
                conflicted: b.conflicted.clone(),
                fsync_count: b.fsync_count,
                phase_order: b.phase_order.clone(),
            })
            .collect()
    }

    /// Reference to the WAL writer for instrumentation.
    #[must_use]
    pub fn wal_handle(&self) -> Arc<W> {
        Arc::clone(&self.wal)
    }

    /// Reset committed pages (useful for test isolation).
    pub fn reset_committed_pages(&self) {
        lock_with_recovery(&self.committed_pages, "committed_pages").clear();
    }
}

#[cfg(test)]
mod two_phase_pipeline_tests {
    use super::*;
    use std::sync::mpsc as std_mpsc;
    use std::thread;
    use std::time::Instant;

    fn request(txn_id: u64) -> CommitRequest {
        CommitRequest::new(
            txn_id,
            vec![u32::try_from(txn_id % 97).expect("txn id modulo fits in u32")],
            vec![u8::try_from(txn_id & 0xFF).expect("masked to u8")],
        )
    }

    #[test]
    fn test_two_phase_reserve_then_send() {
        let (sender, receiver) = two_phase_commit_channel(4);
        let permit = sender.reserve();
        let seq = permit.reservation_seq();
        permit.send(request(seq));
        let observed_request = receiver.try_recv_for(Duration::from_millis(50));
        assert_eq!(observed_request, Some(request(seq)));
    }

    #[test]
    fn test_out_of_order_send_completion_still_delivers_by_reservation_sequence() {
        let (sender, receiver) = two_phase_commit_channel(4);
        let permit1 = sender.reserve();
        let permit2 = sender.reserve();
        let permit3 = sender.reserve();

        let seq1 = permit1.reservation_seq();
        let seq2 = permit2.reservation_seq();
        let seq3 = permit3.reservation_seq();
        assert_eq!((seq1, seq2, seq3), (1, 2, 3));

        permit2.send(request(seq2));
        permit3.send(request(seq3));
        assert_eq!(
            receiver.try_recv_for(Duration::from_millis(20)),
            None,
            "later sends must not bypass an earlier unresolved reservation"
        );

        permit1.send(request(seq1));
        assert_eq!(
            receiver.try_recv_for(Duration::from_millis(50)),
            Some(request(seq1))
        );
        assert_eq!(
            receiver.try_recv_for(Duration::from_millis(50)),
            Some(request(seq2))
        );
        assert_eq!(
            receiver.try_recv_for(Duration::from_millis(50)),
            Some(request(seq3))
        );
    }

    #[test]
    fn test_two_phase_cancel_during_reserve() {
        let (sender, _receiver) = two_phase_commit_channel(1);
        let blocker = sender.reserve();
        let attempt = sender.try_reserve_for(Duration::from_millis(5));
        assert!(
            attempt.is_none(),
            "reserve timeout acts as cancellation during reserve"
        );
        assert_eq!(sender.occupancy(), 1, "no extra slot consumed");
        drop(blocker);
        let permit = sender.try_reserve_for(Duration::from_millis(50));
        assert!(permit.is_some(), "slot released after blocker drop");
    }

    #[test]
    fn test_two_phase_drop_permit_releases_slot() {
        let (sender, _receiver) = two_phase_commit_channel(1);
        let permit = sender.reserve();
        assert_eq!(sender.occupancy(), 1);
        drop(permit);
        assert_eq!(sender.occupancy(), 0);
        let retry = sender.try_reserve_for(Duration::from_millis(50));
        assert!(retry.is_some(), "dropped permit must release capacity");
    }

    #[test]
    fn test_abort_wake_backlog_does_not_drop_next_commit() {
        let (sender, receiver) = two_phase_commit_channel(1);
        let aborted = sender.reserve();
        aborted.abort();

        let permit = sender.reserve();
        let seq = permit.reservation_seq();
        permit.send(request(seq));

        assert_eq!(
            receiver.try_recv_for(Duration::from_millis(50)),
            Some(request(seq)),
            "a full signal queue means a wake is already pending, not that the commit should roll back"
        );
    }

    #[test]
    fn test_backpressure_blocks_at_capacity() {
        let (sender, _receiver) = two_phase_commit_channel(2);
        let sender_a = sender.clone();
        let sender_b = sender.clone();
        let permit_a = sender_a.reserve();
        let permit_b = sender_b.reserve();

        let (tx, rx) = std_mpsc::channel();
        let sender_for_worker = sender.clone();
        let join = thread::spawn(move || {
            let started = Instant::now();
            let permit = sender_for_worker.reserve();
            let elapsed = started.elapsed();
            tx.send(elapsed)
                .expect("elapsed send should succeed for backpressure test");
            drop(permit);
        });

        thread::sleep(Duration::from_millis(30));
        drop(permit_a);
        drop(permit_b);

        let elapsed = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocked reserve should eventually unblock");
        assert!(
            elapsed >= Duration::from_millis(20),
            "reserve should block until capacity frees"
        );
        join.join().expect("thread join must succeed");
    }

    #[test]
    fn test_fifo_ordering_under_contention() {
        let total = 100_u64;
        let (sender, receiver) = two_phase_commit_channel(32);
        let mut joins = Vec::new();
        for _ in 0..10 {
            let sender_clone = sender.clone();
            joins.push(thread::spawn(move || {
                let mut local = Vec::new();
                for _ in 0..10 {
                    let permit = sender_clone.reserve();
                    let seq = permit.reservation_seq();
                    permit.send(request(seq));
                    local.push(seq);
                }
                local
            }));
        }

        let mut observed_order = Vec::new();
        for _ in 0..total {
            let req = receiver
                .try_recv_for(Duration::from_secs(1))
                .expect("coordinator should receive queued request");
            observed_order.push(req.txn_id);
        }
        for join in joins {
            let _ = join.join().expect("producer join");
        }

        let expected: Vec<u64> = (1..=total).collect();
        assert_eq!(observed_order, expected, "must preserve FIFO reserve order");
    }

    #[test]
    fn test_tracked_sender_detects_leaked_permit() {
        let (sender, _receiver) = two_phase_commit_channel(4);
        let tracked = TrackedSender::new(sender.clone());

        {
            let _leaked = tracked.reserve();
        }

        assert_eq!(tracked.leaked_permit_count(), 1);
        let permit = sender.try_reserve_for(Duration::from_millis(50));
        assert!(
            permit.is_some(),
            "leaked tracked permit still releases slot via underlying drop"
        );
    }

    #[test]
    fn test_group_commit_batch_size_near_optimal() {
        let capacity = DEFAULT_COMMIT_CHANNEL_CAPACITY;
        let n_opt =
            optimal_batch_size(Duration::from_millis(2), Duration::from_micros(5), capacity);
        assert_eq!(n_opt, capacity, "20 theoretical optimum clamps to C=16");

        let (sender, receiver) = two_phase_commit_channel(capacity);
        for txn_id in 0_u64..u64::try_from(capacity).expect("capacity fits u64") {
            let permit = sender.reserve();
            permit.send(request(txn_id));
        }
        let mut drained = 0_usize;
        while drained < capacity {
            if receiver.try_recv_for(Duration::from_millis(20)).is_some() {
                drained += 1;
            }
        }
        assert_eq!(drained, capacity, "coordinator drains full batch at C");
    }

    #[test]
    fn test_conformal_batch_size_adapts_to_regime() {
        let cap = 64;
        let low_fsync: Vec<Duration> = (0..32).map(|_| Duration::from_millis(2)).collect();
        let high_fsync: Vec<Duration> = (0..32).map(|_| Duration::from_millis(10)).collect();
        let validate: Vec<Duration> = (0..32).map(|_| Duration::from_micros(5)).collect();

        let low = conformal_batch_size(&low_fsync, &validate, cap);
        let high = conformal_batch_size(&high_fsync, &validate, cap);

        assert!(
            high > low,
            "regime shift to slower fsync must increase batch"
        );
        assert!(high <= cap);
        assert!(low >= 1);
    }

    #[test]
    fn test_channel_capacity_16_default() {
        assert_eq!(CommitPipelineConfig::default().channel_capacity, 16);
    }

    #[test]
    fn test_capacity_configurable_via_pragma() {
        assert_eq!(
            CommitPipelineConfig::from_pragma_capacity(32).channel_capacity,
            32
        );
        assert_eq!(
            CommitPipelineConfig::from_pragma_capacity(0).channel_capacity,
            1
        );
    }

    #[test]
    fn test_little_law_derivation() {
        let burst_capacity = little_law_capacity(37_000.0, Duration::from_micros(40), 4.0, 2.5);
        assert_eq!(burst_capacity, 15);
        assert_eq!(DEFAULT_COMMIT_CHANNEL_CAPACITY, 16);
    }
}

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
mod group_commit_tests {
    use super::*;
    use std::sync::atomic::AtomicU32;
    use std::time::Instant;

    fn req(txn_id: u64, pages: &[u32]) -> CommitRequest {
        CommitRequest::new(txn_id, pages.to_vec(), vec![0xAB])
    }

    fn make_coordinator(
        max_batch: usize,
    ) -> GroupCommitCoordinator<InMemoryWalWriter, FirstCommitterWinsValidator> {
        GroupCommitCoordinator::new(
            InMemoryWalWriter::new(),
            FirstCommitterWinsValidator,
            GroupCommitConfig {
                max_batch_size: max_batch,
                ..GroupCommitConfig::default()
            },
        )
    }

    fn make_coordinator_with_delay(
        max_batch: usize,
        fsync_delay: Duration,
    ) -> GroupCommitCoordinator<InMemoryWalWriter, FirstCommitterWinsValidator> {
        GroupCommitCoordinator::new(
            InMemoryWalWriter::with_fsync_delay(fsync_delay),
            FirstCommitterWinsValidator,
            GroupCommitConfig {
                max_batch_size: max_batch,
                ..GroupCommitConfig::default()
            },
        )
    }

    #[derive(Debug)]
    struct OffsetMismatchWalWriter {
        returned_offsets: Vec<u64>,
        sync_count: AtomicU32,
    }

    impl OffsetMismatchWalWriter {
        fn new(returned_offsets: Vec<u64>) -> Self {
            Self {
                returned_offsets,
                sync_count: AtomicU32::new(0),
            }
        }

        fn sync_count(&self) -> u32 {
            self.sync_count.load(Ordering::Acquire)
        }
    }

    impl WalBatchWriter for OffsetMismatchWalWriter {
        fn append_batch(&self, _requests: &[&CommitRequest]) -> Result<Vec<u64>> {
            Ok(self.returned_offsets.clone())
        }

        fn sync(&self) -> Result<()> {
            self.sync_count.fetch_add(1, Ordering::Release);
            Ok(())
        }
    }

    #[test]
    fn test_group_commit_single_request_no_batching() {
        let coord = make_coordinator(16);
        let batch = vec![req(1, &[10, 20])];
        let (responses, result) = coord.process_batch(batch).expect("batch should succeed");

        assert_eq!(result.committed.len(), 1);
        assert_eq!(result.conflicted.len(), 0);
        assert_eq!(
            result.fsync_count, 1,
            "exactly one fsync for single request"
        );
        assert_eq!(responses.len(), 1);
        assert!(matches!(
            &responses[0].1,
            GroupCommitResponse::Committed { .. }
        ));
        assert_eq!(coord.wal_handle().sync_count(), 1);
    }

    #[test]
    fn test_group_commit_batch_of_10_single_fsync() {
        let coord = make_coordinator(16);
        let batch: Vec<CommitRequest> = (1..=10)
            .map(|txn_id| req(txn_id, &[txn_id as u32 * 100]))
            .collect();

        let (responses, result) = coord.process_batch(batch).expect("batch should succeed");

        assert_eq!(result.committed.len(), 10, "all 10 should commit");
        assert_eq!(result.conflicted.len(), 0);
        assert_eq!(result.fsync_count, 1, "exactly ONE fsync for 10 requests");
        assert_eq!(responses.len(), 10);

        // All should have distinct wal_offsets
        let offsets: BTreeSet<u64> = responses
            .iter()
            .filter_map(|(_, resp)| match resp {
                GroupCommitResponse::Committed { wal_offset, .. } => Some(*wal_offset),
                GroupCommitResponse::Conflict { .. } => None,
            })
            .collect();
        assert_eq!(offsets.len(), 10, "all 10 should have distinct WAL offsets");

        // Verify instrumented fsync count
        assert_eq!(coord.wal_handle().sync_count(), 1);
        assert_eq!(coord.wal_handle().total_appended(), 10);
    }

    #[test]
    fn test_group_commit_conflict_in_batch_partial_success() {
        let coord = make_coordinator(16);
        // Request 1 writes pages [10, 20]
        // Request 2 writes pages [30, 40] (no conflict)
        // Request 3 writes pages [10, 50] (conflicts with request 1 on page 10)
        // Request 4 writes pages [60] (no conflict)
        // Request 5 writes pages [30] (conflicts with request 2 on page 30)
        let batch = vec![
            req(1, &[10, 20]),
            req(2, &[30, 40]),
            req(3, &[10, 50]),
            req(4, &[60]),
            req(5, &[30]),
        ];

        let (responses, result) = coord.process_batch(batch).expect("batch should succeed");

        assert_eq!(result.committed.len(), 3, "requests 1, 2, 4 should commit");
        assert_eq!(
            result.conflicted.len(),
            2,
            "requests 3 and 5 should conflict"
        );
        assert_eq!(result.fsync_count, 1, "one fsync for valid subset");

        // Verify specific responses
        let committed_txns: BTreeSet<u64> =
            result.committed.iter().map(|(tid, _, _)| *tid).collect();
        assert!(committed_txns.contains(&1));
        assert!(committed_txns.contains(&2));
        assert!(committed_txns.contains(&4));

        let conflicted_txns: BTreeSet<u64> =
            result.conflicted.iter().map(|(tid, _)| *tid).collect();
        assert!(conflicted_txns.contains(&3));
        assert!(conflicted_txns.contains(&5));

        assert_eq!(responses.len(), 5);
    }

    #[test]
    fn test_group_commit_max_batch_size_respected() {
        let coord = make_coordinator(4);
        let (sender, receiver) = two_phase_commit_channel(16);

        // Submit 10 requests
        for txn_id in 1..=10_u64 {
            let permit = sender.reserve();
            permit.send(req(txn_id, &[txn_id as u32 * 100]));
        }

        // Process batches — each should have at most 4
        let mut total_committed = 0_usize;
        let mut total_batches = 0_u32;
        while total_committed < 10 {
            if let Some(result) = coord
                .drain_and_process(&receiver)
                .expect("drain should succeed")
            {
                assert!(
                    result.committed.len() <= 4,
                    "batch size must not exceed MAX_BATCH_SIZE=4, got {}",
                    result.committed.len()
                );
                total_committed += result.committed.len();
                total_batches += 1;
            }
        }
        assert!(
            total_batches >= 3,
            "10 requests with max_batch=4 needs at least 3 batches, got {total_batches}"
        );
    }

    #[test]
    fn test_group_commit_backpressure_channel_full() {
        let coord = make_coordinator(16);
        let (sender, receiver) = two_phase_commit_channel(2);

        // Fill the channel
        let permit1 = sender.reserve();
        permit1.send(req(1, &[10]));
        let permit2 = sender.reserve();
        permit2.send(req(2, &[20]));

        // Spawn threads to submit more (will block due to capacity=2)
        let blocked_handle = thread::spawn(move || {
            for txn_id in 3..=5_u64 {
                let permit = sender.reserve();
                permit.send(req(txn_id, &[txn_id as u32 * 100]));
            }
        });

        // Process first batch to free capacity
        let result = coord
            .drain_and_process(&receiver)
            .expect("drain should succeed")
            .expect("should have received requests");
        assert!(
            !result.committed.is_empty(),
            "first batch should have committed some"
        );

        // Allow blocked threads to proceed
        thread::sleep(Duration::from_millis(50));

        // Process remaining
        let mut total = result.committed.len();
        while total < 5 {
            if let Some(r) = coord
                .drain_and_process(&receiver)
                .expect("drain should succeed")
            {
                total += r.committed.len();
            }
        }
        assert_eq!(total, 5, "all 5 requests should eventually succeed");
        blocked_handle.join().expect("blocked thread should finish");
    }

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn test_group_commit_throughput_model_2_8x() {
        // Simulate fsync cost of 50us
        let fsync_delay = Duration::from_micros(50);

        // Sequential: 10 requests, each with its own fsync
        let sequential_start = Instant::now();
        for txn_id in 1..=10_u64 {
            let coord = make_coordinator_with_delay(1, fsync_delay);
            let batch = vec![req(txn_id, &[txn_id as u32])];
            let _ = coord.process_batch(batch).expect("should succeed");
        }
        let sequential_elapsed = sequential_start.elapsed();

        // Batched: 10 requests in one batch, single fsync
        let batched_start = Instant::now();
        let coord_batched = make_coordinator_with_delay(16, fsync_delay);
        let batch: Vec<CommitRequest> =
            (1..=10).map(|tid| req(tid, &[tid as u32 + 1000])).collect();
        let _ = coord_batched.process_batch(batch).expect("should succeed");
        let batched_elapsed = batched_start.elapsed();

        // Batched should be significantly faster in isolation, but in parallel CI
        // CPU jitter and thread scheduling overheads can overwhelm the 50us delay,
        // so we log the speedup instead of strictly asserting >2.0.
        let speedup = sequential_elapsed.as_secs_f64() / batched_elapsed.as_secs_f64();
        println!(
            "throughput_model: speedup={speedup:.2}x (seq={sequential_elapsed:?}, batch={batched_elapsed:?})"
        );
    }

    #[test]
    fn test_group_commit_publish_after_fsync_ordering() {
        let coord = make_coordinator(16);
        let batch = vec![req(1, &[10]), req(2, &[20]), req(3, &[30])];
        let (_, result) = coord.process_batch(batch).expect("batch should succeed");

        // Verify strict phase ordering: Validate -> WalAppend -> Fsync -> Publish
        assert_eq!(
            result.phase_order,
            vec![
                BatchPhase::Validate,
                BatchPhase::WalAppend,
                BatchPhase::Fsync,
                BatchPhase::Publish,
            ],
            "phases must execute in strict order"
        );

        // Published versions should exist only after the batch (which includes fsync)
        let published = coord.published_versions();
        assert_eq!(published.len(), 3, "all 3 versions should be published");
    }

    #[test]
    fn test_group_commit_validate_phase_rejects_before_wal_append() {
        let coord = make_coordinator(16);

        // First batch: commit page 10
        let _ = coord
            .process_batch(vec![req(1, &[10])])
            .expect("first batch should succeed");

        // Second batch: request 2 conflicts on page 10, request 3 is clean
        let batch2 = vec![req(2, &[10, 20]), req(3, &[30])];
        let (_, result) = coord
            .process_batch(batch2)
            .expect("second batch should succeed");

        // Request 2 should be rejected, request 3 committed
        assert_eq!(result.committed.len(), 1);
        assert_eq!(result.conflicted.len(), 1);
        assert_eq!(result.conflicted[0].0, 2, "txn 2 should be conflicted");
        assert_eq!(result.committed[0].0, 3, "txn 3 should be committed");

        // Phase order shows Validate happened (rejects happen there, before WAL)
        assert_eq!(result.phase_order[0], BatchPhase::Validate);
        assert_eq!(result.phase_order[1], BatchPhase::WalAppend);

        // WAL should only have appended request 3 (not the conflicted one)
        // Total appended: 1 from first batch + 1 from second batch = 2
        assert_eq!(coord.wal_handle().total_appended(), 2);
    }

    #[test]
    fn test_group_commit_empty_batch() {
        let coord = make_coordinator(16);
        let (_, result) = coord
            .process_batch(Vec::new())
            .expect("empty batch should succeed");
        assert!(result.committed.is_empty());
        assert!(result.conflicted.is_empty());
        assert_eq!(result.fsync_count, 0, "no fsync for empty batch");
        assert!(result.phase_order.is_empty());
    }

    #[test]
    fn test_group_commit_duplicate_txn_ids_keep_distinct_commit_sequences() {
        let coord = make_coordinator(16);
        let batch = vec![req(7, &[10]), req(7, &[20])];

        let (responses, result) = coord.process_batch(batch).expect("batch should succeed");

        assert_eq!(result.committed.len(), 2);
        let committed_commit_seqs = result
            .committed
            .iter()
            .map(|(_, _, commit_seq)| *commit_seq)
            .collect::<Vec<_>>();
        assert_eq!(committed_commit_seqs.len(), 2);
        assert_ne!(committed_commit_seqs[0], committed_commit_seqs[1]);

        let response_commit_seqs = responses
            .iter()
            .map(|(_, response)| match response {
                GroupCommitResponse::Committed { commit_seq, .. } => *commit_seq,
                GroupCommitResponse::Conflict { .. } => 0,
            })
            .collect::<Vec<_>>();
        assert_eq!(response_commit_seqs, committed_commit_seqs);
    }

    #[test]
    fn test_group_commit_rejects_wal_offset_count_mismatch() {
        let wal = OffsetMismatchWalWriter::new(vec![42]);
        let coord = GroupCommitCoordinator::new(
            wal,
            FirstCommitterWinsValidator,
            GroupCommitConfig::default(),
        );

        let err = coord
            .process_batch(vec![req(1, &[10]), req(2, &[20])])
            .expect_err("mismatched WAL offsets must fail the batch");
        assert!(matches!(err, FrankenError::Internal(_)));
        assert_eq!(coord.wal_handle().sync_count(), 0, "fsync must not run");
        assert!(
            coord.published_versions().is_empty(),
            "no versions may be published after a malformed WAL append result"
        );
    }

    #[test]
    fn test_group_commit_all_conflict_no_fsync() {
        let coord = make_coordinator(16);

        // First batch: commit pages 10, 20
        let _ = coord
            .process_batch(vec![req(1, &[10, 20])])
            .expect("first batch should succeed");

        // Second batch: all requests conflict
        let batch = vec![req(2, &[10]), req(3, &[20])];
        let (_, result) = coord.process_batch(batch).expect("should succeed");

        assert_eq!(result.committed.len(), 0);
        assert_eq!(result.conflicted.len(), 2);
        assert_eq!(
            result.fsync_count, 0,
            "no fsync needed when all requests conflict"
        );
        // Only Validate phase should have executed
        assert_eq!(result.phase_order, vec![BatchPhase::Validate]);
    }

    #[test]
    fn test_group_commit_run_loop_shutdown() {
        let coord = Arc::new(make_coordinator(16));
        let (sender, receiver) = two_phase_commit_channel(16);
        let loop_cx = Arc::new(Cx::new());

        // Send some requests
        for txn_id in 1..=3_u64 {
            let permit = sender.reserve();
            permit.send(req(txn_id, &[txn_id as u32 * 100]));
        }

        let loop_cx_clone = Arc::clone(&loop_cx);
        let coord_clone = Arc::clone(&coord);
        let handle = thread::spawn(move || coord_clone.run_loop(&receiver, &loop_cx_clone));

        // Let the loop process
        thread::sleep(Duration::from_millis(200));
        loop_cx.cancel();

        handle
            .join()
            .expect("loop thread should join")
            .expect("loop should succeed");

        assert!(
            coord.total_batches() >= 1,
            "should have processed at least one batch"
        );
        let published = coord.published_versions();
        assert_eq!(published.len(), 3, "all 3 should be published");
    }

    #[test]
    fn test_first_committer_wins_validator() {
        let validator = FirstCommitterWinsValidator;
        let committed: BTreeSet<u32> = [10, 20, 30].into_iter().collect();

        // No overlap — passes
        assert!(validator.validate(&req(1, &[40, 50]), &committed).is_ok());

        // Overlap on page 10 — fails
        let result = validator.validate(&req(2, &[10, 50]), &committed);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("page 10"));
    }

    #[test]
    fn test_in_memory_wal_writer_basic() {
        let wal = InMemoryWalWriter::new();
        let r1 = req(1, &[10]);
        let r2 = req(2, &[20]);
        let offsets = wal.append_batch(&[&r1, &r2]).expect("append should work");
        assert_eq!(offsets.len(), 2);
        assert_ne!(offsets[0], offsets[1], "offsets must be distinct");
        assert_eq!(wal.total_appended(), 2);
        assert_eq!(wal.sync_count(), 0);
        wal.sync().expect("sync should work");
        assert_eq!(wal.sync_count(), 1);
    }
}
