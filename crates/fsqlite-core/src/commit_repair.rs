//! Commit durability and asynchronous repair orchestration (§1.6, bd-22n.11).
//!
//! The critical path only appends+syncs systematic symbols. Repair symbols are
//! generated/append-synced asynchronously after commit acknowledgment.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use fsqlite_error::{FrankenError, Result};
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
struct TwoPhaseQueueState {
    capacity: usize,
    next_wait_ticket: u64,
    wait_queue: VecDeque<u64>,
    next_reservation_seq: u64,
    next_receive_seq: u64,
    open_reservations: BTreeSet<u64>,
    aborted_reservations: BTreeSet<u64>,
    pending_commits: BTreeMap<u64, CommitRequest>,
    ready_commits: VecDeque<CommitRequest>,
}

impl TwoPhaseQueueState {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            next_wait_ticket: 1,
            wait_queue: VecDeque::new(),
            next_reservation_seq: 1,
            next_receive_seq: 1,
            open_reservations: BTreeSet::new(),
            aborted_reservations: BTreeSet::new(),
            pending_commits: BTreeMap::new(),
            ready_commits: VecDeque::new(),
        }
    }

    fn occupancy(&self) -> usize {
        self.open_reservations.len() + self.pending_commits.len() + self.ready_commits.len()
    }

    fn can_admit(&self, ticket: u64) -> bool {
        self.wait_queue.front().copied() == Some(ticket) && self.occupancy() < self.capacity
    }

    fn reserve_slot(&mut self) -> u64 {
        let seq = self.next_reservation_seq;
        self.next_reservation_seq = self.next_reservation_seq.saturating_add(1);
        self.open_reservations.insert(seq);
        seq
    }

    fn close_reservation_with_send(&mut self, reservation_seq: u64, request: CommitRequest) {
        if self.open_reservations.remove(&reservation_seq) {
            self.pending_commits.insert(reservation_seq, request);
        }
        self.promote_ready();
    }

    fn close_reservation_with_abort(&mut self, reservation_seq: u64) {
        if self.open_reservations.remove(&reservation_seq) {
            self.aborted_reservations.insert(reservation_seq);
        }
        self.promote_ready();
    }

    fn remove_wait_ticket(&mut self, ticket: u64) {
        if let Some(pos) = self.wait_queue.iter().position(|queued| *queued == ticket) {
            let _ = self.wait_queue.remove(pos);
        }
    }

    fn promote_ready(&mut self) {
        loop {
            if self.aborted_reservations.remove(&self.next_receive_seq) {
                self.next_receive_seq = self.next_receive_seq.saturating_add(1);
                continue;
            }
            let Some(request) = self.pending_commits.remove(&self.next_receive_seq) else {
                break;
            };
            self.ready_commits.push_back(request);
            self.next_receive_seq = self.next_receive_seq.saturating_add(1);
        }
    }
}

#[derive(Debug)]
struct TwoPhaseQueueShared {
    state: Mutex<TwoPhaseQueueState>,
    cv: Condvar,
}

impl TwoPhaseQueueShared {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            state: Mutex::new(TwoPhaseQueueState::new(capacity)),
            cv: Condvar::new(),
        }
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
    #[allow(clippy::significant_drop_tightening)]
    pub fn try_reserve_for(&self, timeout: Duration) -> Option<SendPermit> {
        let mut state_guard = lock_with_recovery(&self.shared.state, "two_phase_state");
        let ticket = state_guard.next_wait_ticket;
        state_guard.next_wait_ticket = state_guard.next_wait_ticket.saturating_add(1);
        state_guard.wait_queue.push_back(ticket);

        let (mut guard, _) =
            match self
                .shared
                .cv
                .wait_timeout_while(state_guard, timeout, |state| !state.can_admit(ticket))
            {
                Ok(pair) => pair,
                Err(poisoned) => poisoned.into_inner(),
            };

        if !guard.can_admit(ticket) {
            guard.remove_wait_ticket(ticket);
            drop(guard);
            self.shared.cv.notify_all();
            return None;
        }

        let _ = guard.wait_queue.pop_front();
        let reservation_seq = guard.reserve_slot();
        drop(guard);
        self.shared.cv.notify_all();
        Some(SendPermit {
            shared: Arc::clone(&self.shared),
            reservation_seq: Some(reservation_seq),
        })
    }

    /// Current buffered + reserved occupancy.
    #[must_use]
    pub fn occupancy(&self) -> usize {
        lock_with_recovery(&self.shared.state, "two_phase_state").occupancy()
    }

    /// Bounded channel capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        lock_with_recovery(&self.shared.state, "two_phase_state").capacity
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
    #[allow(clippy::significant_drop_tightening)]
    pub fn try_recv_for(&self, timeout: Duration) -> Option<CommitRequest> {
        let (mut guard, _) = match self.shared.cv.wait_timeout_while(
            lock_with_recovery(&self.shared.state, "two_phase_state"),
            timeout,
            |state| state.ready_commits.is_empty(),
        ) {
            Ok(pair) => pair,
            Err(poisoned) => poisoned.into_inner(),
        };

        let request = guard.ready_commits.pop_front()?;
        drop(guard);
        self.shared.cv.notify_all();
        Some(request)
    }
}

/// Two-phase permit returned by `reserve()`.
///
/// Dropping without `send()`/`abort()` automatically releases the reserved slot.
#[derive(Debug)]
pub struct SendPermit {
    shared: Arc<TwoPhaseQueueShared>,
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
        if let Some(reservation_seq) = self.reservation_seq.take() {
            {
                let mut guard = lock_with_recovery(&self.shared.state, "two_phase_state");
                guard.close_reservation_with_send(reservation_seq, request);
            }
            self.shared.cv.notify_all();
        }
    }

    /// Explicitly release reserved slot without sending.
    pub fn abort(mut self) {
        if let Some(reservation_seq) = self.reservation_seq.take() {
            {
                let mut guard = lock_with_recovery(&self.shared.state, "two_phase_state");
                guard.close_reservation_with_abort(reservation_seq);
            }
            self.shared.cv.notify_all();
        }
    }
}

impl Drop for SendPermit {
    fn drop(&mut self) {
        if let Some(reservation_seq) = self.reservation_seq.take() {
            {
                let mut guard = lock_with_recovery(&self.shared.state, "two_phase_state");
                guard.close_reservation_with_abort(reservation_seq);
            }
            self.shared.cv.notify_all();
        }
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
    io: Arc<IO>,
    generator: Arc<GEN>,
    next_commit_seq: AtomicU64,
    next_async_task_id: AtomicU64,
    repair_states: Arc<Mutex<HashMap<u64, RepairState>>>,
    events: Arc<Mutex<Vec<CommitRepairEvent>>>,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

impl<IO, GEN> CommitRepairCoordinator<IO, GEN>
where
    IO: CommitRepairIo + Send + Sync + 'static,
    GEN: RepairSymbolGenerator + Send + Sync + 'static,
{
    #[must_use]
    pub fn new(config: CommitRepairConfig, io: IO, generator: GEN) -> Self {
        Self::with_shared(config, Arc::new(io), Arc::new(generator))
    }

    #[must_use]
    pub fn with_shared(config: CommitRepairConfig, io: Arc<IO>, generator: Arc<GEN>) -> Self {
        Self {
            config,
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
                latency: Duration::ZERO,
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
        let handle = thread::spawn(move || {
            info!(
                bead_id = BEAD_ID,
                commit_seq, async_task_id, "repair symbols generation started"
            );
            record_event_into(&events, commit_seq, CommitRepairEventKind::RepairStarted);

            let repair_outcome =
                generator.generate_repair_symbols(commit_seq, &systematic_snapshot);
            match repair_outcome {
                Ok(repair_symbols) => {
                    let append_sync = io
                        .append_repair_symbols(commit_seq, &repair_symbols)
                        .and_then(|()| io.sync_repair_symbols(commit_seq));
                    match append_sync {
                        Ok(()) => {
                            set_repair_state(&repair_states, commit_seq, RepairState::Completed);
                            record_event_into(
                                &events,
                                commit_seq,
                                CommitRepairEventKind::RepairCompleted,
                            );
                            info!(
                                bead_id = BEAD_ID,
                                commit_seq,
                                async_task_id,
                                repair_symbol_bytes = repair_symbols.len(),
                                "repair symbols append+sync completed"
                            );
                        }
                        Err(err) => {
                            set_repair_state(&repair_states, commit_seq, RepairState::Failed);
                            record_event_into(
                                &events,
                                commit_seq,
                                CommitRepairEventKind::RepairFailed,
                            );
                            error!(
                                bead_id = BEAD_ID,
                                commit_seq,
                                async_task_id,
                                error = %err,
                                "repair symbol append/sync failed"
                            );
                        }
                    }
                }
                Err(err) => {
                    set_repair_state(&repair_states, commit_seq, RepairState::Failed);
                    record_event_into(&events, commit_seq, CommitRepairEventKind::RepairFailed);
                    error!(
                        bead_id = BEAD_ID,
                        commit_seq,
                        async_task_id,
                        error = %err,
                        "repair symbol generation failed"
                    );
                }
            }
        });
        lock_with_recovery(&self.handles, "repair_handles").push(handle);

        Ok(CommitReceipt {
            commit_seq,
            durable: true,
            repair_pending: true,
            latency: Duration::ZERO,
        })
    }

    /// Join all currently scheduled background repair workers.
    pub fn wait_for_background_repair(&self) -> Result<()> {
        let mut handles = lock_with_recovery(&self.handles, "repair_handles");
        while let Some(handle) = handles.pop() {
            if handle.join().is_err() {
                return Err(FrankenError::Internal(
                    "background repair worker panicked".to_owned(),
                ));
            }
        }
        Ok(())
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
        let ack = events
            .iter()
            .find(|event| event.kind == CommitRepairEventKind::CommitAcked)?;
        let repair_done = events
            .iter()
            .find(|event| event.kind == CommitRepairEventKind::RepairCompleted)?;
        Some(Duration::from_millis(
            repair_done.seq.saturating_sub(ack.seq),
        ))
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
        let mut handles = lock_with_recovery(&self.handles, "repair_handles");
        while let Some(handle) = handles.pop() {
            if handle.join().is_err() {
                error!(
                    bead_id = BEAD_ID,
                    "background repair worker panicked during drop"
                );
            }
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
        kind,
    });
}

// ---------------------------------------------------------------------------
// Group Commit Batching (§5.9.2.1, bd-l4gl)
// ---------------------------------------------------------------------------

const GROUP_COMMIT_BEAD_ID: &str = "bd-l4gl";

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

        for (req, &wal_offset) in valid_requests.into_iter().zip(wal_offsets.iter()) {
            let commit_seq = committed_entries
                .iter()
                .find(|(tid, _, _)| *tid == req.txn_id)
                .map_or(0, |entry| entry.2);
            responses.push((
                req,
                GroupCommitResponse::Committed {
                    wal_offset,
                    commit_seq,
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
        // Blocking wait for first request
        let Some(first) = receiver.try_recv_for(Duration::from_secs(1)) else {
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

    /// Run the coordinator loop, processing batches until `shutdown` is set.
    ///
    /// This is the production entry point. The loop blocks on the first
    /// request of each batch, drains additional requests, and processes
    /// the batch through all 4 phases.
    pub fn run_loop(&self, receiver: &TwoPhaseCommitReceiver, shutdown: &AtomicBool) -> Result<()> {
        info!(
            bead_id = GROUP_COMMIT_BEAD_ID,
            max_batch_size = self.config.max_batch_size,
            "group commit coordinator loop started"
        );
        while !shutdown.load(Ordering::Acquire) {
            if let Some(result) = self.drain_and_process(receiver)? {
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
    fn test_backpressure_blocks_at_capacity() {
        let (sender, _receiver) = two_phase_commit_channel(2);
        let permit_a = sender.reserve();
        let permit_b = sender.reserve();

        let (tx, rx) = std_mpsc::channel();
        let join = thread::spawn(move || {
            let started = Instant::now();
            let permit = sender.reserve();
            let elapsed = started.elapsed();
            tx.send(elapsed)
                .expect("elapsed send should succeed for backpressure test");
            permit
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
        let _permit = join.join().expect("thread join must succeed");
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

        // Batched should be significantly faster
        let speedup = sequential_elapsed.as_secs_f64() / batched_elapsed.as_secs_f64();
        assert!(
            speedup > 2.0,
            "expected >2x speedup from batching, got {speedup:.2}x \
             (seq={sequential_elapsed:?}, batch={batched_elapsed:?})"
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
        let shutdown = Arc::new(AtomicBool::new(false));

        // Send some requests
        for txn_id in 1..=3_u64 {
            let permit = sender.reserve();
            permit.send(req(txn_id, &[txn_id as u32 * 100]));
        }

        let shutdown_clone = Arc::clone(&shutdown);
        let coord_clone = Arc::clone(&coord);
        let handle = thread::spawn(move || coord_clone.run_loop(&receiver, &shutdown_clone));

        // Let the loop process
        thread::sleep(Duration::from_millis(200));
        shutdown.store(true, Ordering::Release);

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
