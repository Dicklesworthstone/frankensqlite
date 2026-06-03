use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, TryLockError};
use std::time::{Duration, Instant};

#[cfg(test)]
use asupersync::runtime::{Runtime, RuntimeBuilder, spawn_blocking};
#[cfg(test)]
use asupersync::time::{sleep, wall_now};
use fsqlite_types::{CommitSeq, PageNumber, TxnEpoch, TxnId, TxnToken};

/// Per-core WAL buffer capacity in bytes. With N CPU cores, the total
/// memory commitment is N × 4 MiB. On memory-constrained systems, reduce
/// this via `PerCoreBuffer::with_capacity()`. On servers with large L3
/// caches, increasing this can reduce flush frequency.
const DEFAULT_BUFFER_CAPACITY_BYTES: usize = 4 * 1024 * 1024;
/// Overflow fallback buffer size when the per-core buffer is full.
const DEFAULT_OVERFLOW_FALLBACK_BYTES: usize = 8 * 1024 * 1024;
const RECORD_FIXED_OVERHEAD_BYTES: usize = 48;
/// Epoch advance interval for flushing batched WAL entries.
const DEFAULT_EPOCH_ADVANCE_INTERVAL_MS: u64 = 10;

/// Default number of buffer slots.
///
/// With 128 slots, 16 threads have only 12.5% probability of two threads
/// landing on the same slot. For systems with fewer cores, a smaller slot
/// count reduces memory overhead.
pub const DEFAULT_BUFFER_SLOT_COUNT: usize = 128;

/// Global counter for assigning thread-local buffer slot indices.
/// Each thread gets a unique slot on first access via `thread_buffer_slot()`.
static NEXT_BUFFER_SLOT: AtomicUsize = AtomicUsize::new(0);

std::thread_local! {
    /// Each thread's assigned buffer slot index. Assigned on first access
    /// via atomic increment of `NEXT_BUFFER_SLOT`, then modulo slot count.
    static THREAD_BUFFER_SLOT: usize =
        NEXT_BUFFER_SLOT.fetch_add(1, Ordering::Relaxed);
}

/// Returns the current thread's assigned buffer slot index, modulo `slot_count`.
///
/// This slot is stable for the lifetime of the thread. Different threads
/// get different slots (until wrap-around at `slot_count`).
#[inline]
pub fn thread_buffer_slot(slot_count: usize) -> usize {
    THREAD_BUFFER_SLOT.with(|&slot| slot % slot_count)
}

/// Resets the global slot counter. Only for testing.
#[cfg(test)]
pub fn reset_slot_counter() {
    NEXT_BUFFER_SLOT.store(0, Ordering::SeqCst);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    BlockWriter,
    AllocateOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferConfig {
    pub capacity_bytes: usize,
    pub overflow_policy: OverflowPolicy,
    pub overflow_fallback_bytes: usize,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            capacity_bytes: DEFAULT_BUFFER_CAPACITY_BYTES,
            overflow_policy: OverflowPolicy::AllocateOverflow,
            overflow_fallback_bytes: DEFAULT_OVERFLOW_FALLBACK_BYTES,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferState {
    Writable,
    Sealed { epoch: u64 },
    Flushing { epoch: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendOutcome {
    Appended,
    QueuedOverflow,
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallbackDecision {
    ContinueParallel,
    ForceSerializedDrain,
}

#[derive(Debug, Clone)]
pub struct WalRecord {
    pub txn_token: TxnToken,
    pub epoch: u64,
    pub page_id: PageNumber,
    pub begin_seq: CommitSeq,
    pub end_seq: Option<CommitSeq>,
    pub before_image: Vec<u8>,
    pub after_image: Vec<u8>,
}

impl WalRecord {
    fn encoded_len(&self) -> usize {
        let metadata_guard = self.txn_token.id.get()
            ^ u64::from(self.txn_token.epoch.get())
            ^ u64::from(self.page_id.get())
            ^ self.epoch
            ^ self.begin_seq.get()
            ^ self.end_seq.map_or(0, CommitSeq::get);

        let metadata_bytes = if metadata_guard == u64::MAX {
            RECORD_FIXED_OVERHEAD_BYTES + 1
        } else {
            RECORD_FIXED_OVERHEAD_BYTES
        };

        metadata_bytes
            .saturating_add(self.before_image.len())
            .saturating_add(self.after_image.len())
    }
}

#[derive(Debug, Clone)]
struct BufferLane {
    state: BufferState,
    bytes_used: usize,
    records: Vec<WalRecord>,
}

impl BufferLane {
    fn new_writable() -> Self {
        Self {
            state: BufferState::Writable,
            bytes_used: 0,
            records: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct PerCoreWalBuffer {
    config: BufferConfig,
    active: BufferLane,
    flush_lane: BufferLane,
    overflow: VecDeque<WalRecord>,
    overflow_bytes: usize,
    fallback_latched: bool,
}

impl PerCoreWalBuffer {
    pub fn new(_core_id: usize, config: BufferConfig) -> Self {
        Self {
            config,
            active: BufferLane::new_writable(),
            flush_lane: BufferLane::new_writable(),
            overflow: VecDeque::new(),
            overflow_bytes: 0,
            fallback_latched: false,
        }
    }

    pub fn append(&mut self, record: WalRecord) -> AppendOutcome {
        if self.active.state != BufferState::Writable {
            return AppendOutcome::Blocked;
        }

        let needed = record.encoded_len();
        if needed > self.config.capacity_bytes {
            self.fallback_latched = true;
            return AppendOutcome::Blocked;
        }

        if self.active.bytes_used.saturating_add(needed) <= self.config.capacity_bytes {
            self.active.bytes_used = self.active.bytes_used.saturating_add(needed);
            self.active.records.push(record);
            return AppendOutcome::Appended;
        }

        match self.config.overflow_policy {
            OverflowPolicy::BlockWriter => AppendOutcome::Blocked,
            OverflowPolicy::AllocateOverflow => {
                self.overflow_bytes = self.overflow_bytes.saturating_add(needed);
                self.overflow.push_back(record);
                if self.overflow_bytes > self.config.overflow_fallback_bytes {
                    self.fallback_latched = true;
                }
                AppendOutcome::QueuedOverflow
            }
        }
    }

    pub fn append_batch(&mut self, records: Vec<WalRecord>) -> AppendOutcome {
        if self.active.state != BufferState::Writable {
            return AppendOutcome::Blocked;
        }

        let mut total_needed = 0usize;
        for record in &records {
            let needed = record.encoded_len();
            if needed > self.config.capacity_bytes {
                self.fallback_latched = true;
                return AppendOutcome::Blocked;
            }
            total_needed = total_needed.saturating_add(needed);
        }

        if self.config.overflow_policy == OverflowPolicy::BlockWriter
            && self.active.bytes_used.saturating_add(total_needed) > self.config.capacity_bytes
        {
            return AppendOutcome::Blocked;
        }

        let mut queued_overflow = false;
        for record in records {
            match self.append(record) {
                AppendOutcome::Appended => {}
                AppendOutcome::QueuedOverflow => queued_overflow = true,
                AppendOutcome::Blocked => return AppendOutcome::Blocked,
            }
        }

        if queued_overflow {
            AppendOutcome::QueuedOverflow
        } else {
            AppendOutcome::Appended
        }
    }

    pub fn seal_active(&mut self, epoch: u64) -> Result<(), &'static str> {
        if self.active.state != BufferState::Writable {
            return Err("active lane is not writable");
        }
        self.active.state = BufferState::Sealed { epoch };
        Ok(())
    }

    pub fn begin_flush(&mut self) -> Result<usize, &'static str> {
        let BufferState::Sealed { epoch } = self.active.state else {
            return Err("active lane must be sealed before flush");
        };

        if self.flush_lane.state != BufferState::Writable {
            return Err("flush lane must be writable before flush");
        }
        if !self.flush_lane.records.is_empty() || self.flush_lane.bytes_used != 0 {
            return Err("flush lane must be empty before flush");
        }

        std::mem::swap(&mut self.active, &mut self.flush_lane);
        self.flush_lane.state = BufferState::Flushing { epoch };
        Ok(self.flush_lane.records.len())
    }

    pub fn complete_flush(&mut self) -> Result<(), &'static str> {
        if !matches!(self.flush_lane.state, BufferState::Flushing { .. }) {
            return Err("flush lane is not in flushing state");
        }

        self.flush_lane.records.clear();
        self.flush_lane.bytes_used = 0;
        self.flush_lane.state = BufferState::Writable;
        self.drain_overflow_into_active();
        Ok(())
    }

    pub fn fallback_decision(&self) -> FallbackDecision {
        if self.fallback_latched {
            FallbackDecision::ForceSerializedDrain
        } else {
            FallbackDecision::ContinueParallel
        }
    }

    pub fn force_serialized_drain(&mut self) -> Vec<WalRecord> {
        let mut drained = Vec::with_capacity(
            self.active
                .records
                .len()
                .saturating_add(self.flush_lane.records.len())
                .saturating_add(self.overflow.len()),
        );
        drained.append(&mut self.active.records);
        drained.append(&mut self.flush_lane.records);
        drained.extend(self.overflow.drain(..));

        self.active.bytes_used = 0;
        self.active.state = BufferState::Writable;

        self.flush_lane.bytes_used = 0;
        self.flush_lane.state = BufferState::Writable;

        self.overflow_bytes = 0;
        self.fallback_latched = false;
        drained
    }

    pub fn active_state(&self) -> BufferState {
        self.active.state
    }

    pub fn flush_state(&self) -> BufferState {
        self.flush_lane.state
    }

    pub fn active_len(&self) -> usize {
        self.active.records.len()
    }

    pub fn flush_len(&self) -> usize {
        self.flush_lane.records.len()
    }

    pub fn overflow_len(&self) -> usize {
        self.overflow.len()
    }

    fn drain_overflow_into_active(&mut self) {
        if self.active.state != BufferState::Writable {
            return;
        }

        while let Some(front) = self.overflow.front() {
            let needed = front.encoded_len();
            if self.active.bytes_used.saturating_add(needed) > self.config.capacity_bytes {
                break;
            }

            let Some(record) = self.overflow.pop_front() else {
                break;
            };
            self.overflow_bytes = self.overflow_bytes.saturating_sub(needed);
            self.active.bytes_used = self.active.bytes_used.saturating_add(needed);
            self.active.records.push(record);
        }

        if self.overflow.is_empty() {
            self.fallback_latched = false;
        }
    }
}

#[derive(Debug)]
struct BufferCell {
    inner: Mutex<PerCoreWalBuffer>,
    contention_events: AtomicU64,
}

impl BufferCell {
    fn new(core_id: usize, config: BufferConfig) -> Self {
        Self {
            inner: Mutex::new(PerCoreWalBuffer::new(core_id, config)),
            contention_events: AtomicU64::new(0),
        }
    }

    fn append(&self, record: WalRecord) -> AppendOutcome {
        match self.inner.try_lock() {
            Ok(mut guard) => guard.append(record),
            Err(TryLockError::WouldBlock) => {
                self.contention_events.fetch_add(1, Ordering::Relaxed);
                let mut guard = self
                    .inner
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                guard.append(record)
            }
            Err(TryLockError::Poisoned(poisoned)) => {
                let mut guard = poisoned.into_inner();
                guard.append(record)
            }
        }
    }

    fn append_batch(&self, records: Vec<WalRecord>) -> AppendOutcome {
        match self.inner.try_lock() {
            Ok(mut guard) => guard.append_batch(records),
            Err(TryLockError::WouldBlock) => {
                self.contention_events.fetch_add(1, Ordering::Relaxed);
                let mut guard = self
                    .inner
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                guard.append_batch(records)
            }
            Err(TryLockError::Poisoned(poisoned)) => {
                let mut guard = poisoned.into_inner();
                guard.append_batch(records)
            }
        }
    }

    fn contention_events(&self) -> u64 {
        self.contention_events.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub struct PerCoreWalBufferPool {
    cells: Vec<BufferCell>,
}

impl PerCoreWalBufferPool {
    pub fn new(core_count: usize, config: BufferConfig) -> Self {
        assert!(core_count > 0, "core_count must be > 0");
        let mut cells = Vec::with_capacity(core_count);
        for core_id in 0..core_count {
            cells.push(BufferCell::new(core_id, config));
        }
        Self { cells }
    }

    pub fn append_to_core(
        &self,
        core_id: usize,
        record: WalRecord,
    ) -> Result<AppendOutcome, String> {
        let Some(cell) = self.cells.get(core_id) else {
            return Err(format!(
                "invalid core_id={core_id}; available cores={}",
                self.cells.len()
            ));
        };
        Ok(cell.append(record))
    }

    pub fn append_batch_to_core(
        &self,
        core_id: usize,
        records: Vec<WalRecord>,
    ) -> Result<AppendOutcome, String> {
        let Some(cell) = self.cells.get(core_id) else {
            return Err(format!(
                "invalid core_id={core_id}; available cores={}",
                self.cells.len()
            ));
        };
        Ok(cell.append_batch(records))
    }

    pub fn contention_events_total(&self) -> u64 {
        self.cells
            .iter()
            .map(BufferCell::contention_events)
            .sum::<u64>()
    }

    pub fn core_count(&self) -> usize {
        self.cells.len()
    }

    pub fn seal_active_for_epoch(&self, epoch: u64) -> Result<usize, String> {
        let mut sealed_lanes = 0_usize;

        for (core_id, cell) in self.cells.iter().enumerate() {
            let did_seal = {
                let mut guard = cell
                    .inner
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if guard.active_state() == BufferState::Writable && guard.active_len() > 0 {
                    guard.seal_active(epoch).map_err(|error| {
                        format!(
                            "core {core_id}: failed to seal active lane for epoch {epoch}: {error}"
                        )
                    })?;
                    true
                } else {
                    false
                }
            };

            if did_seal {
                sealed_lanes = sealed_lanes.saturating_add(1);
            }
        }

        Ok(sealed_lanes)
    }

    pub fn flush_epoch_batches(&self, epoch: u64) -> Result<(Vec<WalRecord>, Vec<usize>), String> {
        let mut all_records = Vec::new();
        let mut records_per_core = Vec::with_capacity(self.cells.len());

        for (core_id, cell) in self.cells.iter().enumerate() {
            let core_records = {
                let mut guard = cell
                    .inner
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);

                if matches!(guard.active_state(), BufferState::Sealed { epoch: sealed_epoch } if sealed_epoch == epoch)
                {
                    guard.begin_flush().map_err(|error| {
                        format!("core {core_id}: begin_flush failed for epoch {epoch}: {error}")
                    })?;
                }

                let should_drain = matches!(
                    guard.flush_state(),
                    BufferState::Flushing {
                        epoch: flushing_epoch
                    } if flushing_epoch == epoch
                );

                let core_records = if should_drain {
                    // Records without an end_seq represent aborted writes and must never
                    // participate in durable replay ordering.
                    guard
                        .flush_lane
                        .records
                        .retain(|record| record.end_seq.is_some());

                    if guard
                        .flush_lane
                        .records
                        .iter()
                        .any(|record| record.epoch != epoch)
                    {
                        return Err(format!(
                            "core {core_id}: epoch boundary straddle detected in flush lane for epoch {epoch}"
                        ));
                    }

                    let core_records = std::mem::take(&mut guard.flush_lane.records);
                    guard.flush_lane.bytes_used = 0;
                    guard.flush_lane.state = BufferState::Writable;
                    guard.drain_overflow_into_active();
                    Some(core_records)
                } else {
                    None
                };

                drop(guard);
                core_records
            };

            if let Some(core_records) = core_records {
                records_per_core.push(core_records.len());
                all_records.extend(core_records);
            } else {
                records_per_core.push(0);
            }
        }

        Ok((all_records, records_per_core))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochConfig {
    pub advance_interval_ms: u64,
}

impl Default for EpochConfig {
    fn default() -> Self {
        Self {
            advance_interval_ms: DEFAULT_EPOCH_ADVANCE_INTERVAL_MS,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EpochFlushBatch {
    pub epoch: u64,
    pub records: Vec<WalRecord>,
    pub records_per_core: Vec<usize>,
}

impl EpochFlushBatch {
    pub fn total_records(&self) -> usize {
        self.records.len()
    }
}

#[derive(Debug)]
struct EpochWaitState {
    observed_epochs: Vec<u64>,
    durable_epoch: Option<u64>,
}

#[derive(Debug)]
pub struct EpochOrderCoordinator {
    pool: Arc<PerCoreWalBufferPool>,
    current_epoch: AtomicU64,
    append_epoch: AtomicU64,
    advance_lock: Mutex<()>,
    wait_state: Mutex<EpochWaitState>,
    wait_cv: Condvar,
    config: EpochConfig,
}

impl EpochOrderCoordinator {
    pub fn new(core_count: usize, buffer_config: BufferConfig, config: EpochConfig) -> Self {
        let pool = Arc::new(PerCoreWalBufferPool::new(core_count, buffer_config));
        let wait_state = EpochWaitState {
            observed_epochs: vec![0; core_count],
            durable_epoch: None,
        };
        Self {
            pool,
            current_epoch: AtomicU64::new(0),
            append_epoch: AtomicU64::new(0),
            advance_lock: Mutex::new(()),
            wait_state: Mutex::new(wait_state),
            wait_cv: Condvar::new(),
            config,
        }
    }

    pub fn current_epoch(&self) -> u64 {
        self.current_epoch.load(Ordering::SeqCst)
    }

    pub fn current_append_epoch(&self) -> u64 {
        self.append_epoch.load(Ordering::SeqCst)
    }

    pub fn durable_epoch(&self) -> Option<u64> {
        let guard = self
            .wait_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.durable_epoch
    }

    pub fn epoch_advance_interval(&self) -> Duration {
        Duration::from_millis(self.config.advance_interval_ms)
    }

    pub fn observe_epoch(&self, core_id: usize) -> Result<u64, String> {
        let observed_epoch = self.current_epoch();
        let mut guard = self
            .wait_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(slot) = guard.observed_epochs.get_mut(core_id) else {
            return Err(format!(
                "invalid core_id={core_id}; available cores={}",
                guard.observed_epochs.len()
            ));
        };
        *slot = (*slot).max(observed_epoch);
        drop(guard);
        self.wait_cv.notify_all();
        Ok(observed_epoch)
    }

    pub fn append_to_core(
        &self,
        core_id: usize,
        begin_seq: u64,
        payload_len: usize,
    ) -> Result<AppendOutcome, String> {
        let mut record = make_record(core_id, begin_seq, payload_len);
        record.epoch = self.current_append_epoch();
        self.pool.append_to_core(core_id, record)
    }

    pub fn append_records_to_core(
        &self,
        core_id: usize,
        records: Vec<WalRecord>,
    ) -> Result<AppendOutcome, String> {
        self.pool.append_batch_to_core(core_id, records)
    }

    pub fn advance_epoch_and_wait(
        &self,
        active_cores: &[usize],
        timeout: Duration,
    ) -> Result<u64, String> {
        let _advance_guard = self
            .advance_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let core_count = self.pool.core_count();
        for &core_id in active_cores {
            if core_id >= core_count {
                return Err(format!(
                    "invalid active core_id={core_id}; available cores={core_count}"
                ));
            }
        }

        let previous_epoch = self.current_epoch.fetch_add(1, Ordering::SeqCst);
        let next_epoch = previous_epoch.saturating_add(1);
        let deadline = Instant::now() + timeout;
        let mut guard = self
            .wait_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        while active_cores
            .iter()
            .any(|core_id| guard.observed_epochs[*core_id] < next_epoch)
        {
            let now = Instant::now();
            if now >= deadline {
                self.current_epoch.store(previous_epoch, Ordering::SeqCst);
                return Err(format!(
                    "epoch fence timed out waiting for active cores to observe epoch {next_epoch}"
                ));
            }

            let remaining = deadline.saturating_duration_since(now);
            let (next_guard, wait_result) = self
                .wait_cv
                .wait_timeout(guard, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard = next_guard;
            if wait_result.timed_out()
                && active_cores
                    .iter()
                    .any(|core_id| guard.observed_epochs[*core_id] < next_epoch)
            {
                self.current_epoch.store(previous_epoch, Ordering::SeqCst);
                return Err(format!(
                    "epoch fence timed out waiting for active cores to observe epoch {next_epoch}"
                ));
            }
        }

        drop(guard);
        self.pool.seal_active_for_epoch(previous_epoch)?;
        self.append_epoch.store(next_epoch, Ordering::SeqCst);
        Ok(next_epoch)
    }

    pub fn flush_epoch(&self, epoch: u64) -> Result<EpochFlushBatch, String> {
        let (records, records_per_core) = self.pool.flush_epoch_batches(epoch)?;
        Ok(EpochFlushBatch {
            epoch,
            records,
            records_per_core,
        })
    }

    pub fn mark_epoch_durable(&self, epoch: u64) {
        let mut guard = self
            .wait_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.durable_epoch = Some(
            guard
                .durable_epoch
                .map_or(epoch, |existing| existing.max(epoch)),
        );
        drop(guard);
        self.wait_cv.notify_all();
    }

    pub fn wait_until_epoch_durable(&self, epoch: u64, timeout: Duration) -> Result<(), String> {
        let deadline = Instant::now() + timeout;
        let mut guard = self
            .wait_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while guard
            .durable_epoch
            .is_none_or(|durable_epoch| durable_epoch < epoch)
        {
            let now = Instant::now();
            if now >= deadline {
                return Err(format!("timeout while waiting for durable epoch {epoch}"));
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next_guard, wait_result) = self
                .wait_cv
                .wait_timeout(guard, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard = next_guard;
            if wait_result.timed_out()
                && guard
                    .durable_epoch
                    .is_none_or(|durable_epoch| durable_epoch < epoch)
            {
                return Err(format!("timeout while waiting for durable epoch {epoch}"));
            }
        }
        Ok(())
    }

    pub fn recovery_order(records: &[WalRecord]) -> Vec<WalRecord> {
        let mut ordered = records.to_vec();
        ordered.sort_by_key(|record| {
            (
                record.epoch,
                record.begin_seq.get(),
                record.txn_token.id.get(),
                record.page_id.get(),
            )
        });
        ordered
    }
}

fn make_record(core_id: usize, seq: u64, payload_len: usize) -> WalRecord {
    let core_u64 = u64::try_from(core_id).expect("core id should fit into u64");
    let txn_id_raw = core_u64.saturating_mul(1_000_000).saturating_add(seq + 1);
    let txn_id = TxnId::new(txn_id_raw).expect("txn id should be non-zero");

    let page_raw = u32::try_from(core_id + 1).expect("core id should fit into u32");
    let page_id = PageNumber::new(page_raw).expect("page id should be non-zero");

    WalRecord {
        txn_token: TxnToken::new(txn_id, TxnEpoch::new(1)),
        epoch: seq,
        page_id,
        begin_seq: CommitSeq::new(seq),
        end_seq: Some(CommitSeq::new(seq.saturating_add(1))),
        before_image: vec![0x10; payload_len],
        after_image: vec![0x20; payload_len],
    }
}

#[cfg(test)]
fn test_runtime() -> Runtime {
    RuntimeBuilder::current_thread()
        .blocking_threads(8, 8)
        .build()
        .expect("runtime build")
}

#[test]
fn bd_ncivz_1_state_machine_double_buffering() {
    let config = BufferConfig {
        capacity_bytes: 640,
        ..BufferConfig::default()
    };
    let mut buffer = PerCoreWalBuffer::new(0, config);

    assert_eq!(buffer.active_state(), BufferState::Writable);
    assert_eq!(buffer.flush_state(), BufferState::Writable);

    assert_eq!(
        buffer.append(make_record(0, 1, 64)),
        AppendOutcome::Appended
    );
    assert_eq!(
        buffer.append(make_record(0, 2, 64)),
        AppendOutcome::Appended
    );
    assert_eq!(buffer.active_len(), 2);

    buffer.seal_active(7).expect("active lane should seal");
    assert_eq!(buffer.active_state(), BufferState::Sealed { epoch: 7 });

    let flushed_records = buffer.begin_flush().expect("sealed lane should flush");
    assert_eq!(flushed_records, 2);
    assert_eq!(buffer.flush_state(), BufferState::Flushing { epoch: 7 });
    assert_eq!(buffer.active_state(), BufferState::Writable);

    assert_eq!(
        buffer.append(make_record(0, 3, 64)),
        AppendOutcome::Appended
    );
    assert_eq!(buffer.active_len(), 1);

    buffer
        .complete_flush()
        .expect("flushing lane should complete");
    assert_eq!(buffer.flush_state(), BufferState::Writable);
    assert_eq!(buffer.flush_len(), 0);
    assert_eq!(buffer.active_len(), 1);
    assert_eq!(
        buffer.fallback_decision(),
        FallbackDecision::ContinueParallel
    );
}

#[test]
fn bd_ncivz_1_sealed_lane_rejects_mutating_appends() {
    let config = BufferConfig {
        capacity_bytes: 640,
        ..BufferConfig::default()
    };
    let mut buffer = PerCoreWalBuffer::new(0, config);

    let seeded = make_record(0, 1, 64);
    assert_eq!(buffer.append(seeded.clone()), AppendOutcome::Appended);
    buffer
        .seal_active(3)
        .expect("active lane should seal before flush");

    assert_eq!(
        buffer.append(make_record(0, 2, 64)),
        AppendOutcome::Blocked,
        "sealed lane must reject mutating appends"
    );

    let flushed_records = buffer
        .begin_flush()
        .expect("sealed records should move into flush lane");
    assert_eq!(flushed_records, 1);
    assert_eq!(buffer.flush_lane.records.len(), 1);

    let flushed = &buffer.flush_lane.records[0];
    assert_eq!(
        flushed.txn_token.id.get(),
        seeded.txn_token.id.get(),
        "flushed lane contents must remain unchanged after blocked append"
    );
    assert_eq!(
        flushed.end_seq, seeded.end_seq,
        "commit metadata must remain intact while lane is sealed"
    );
}

#[test]
fn bd_ncivz_1_overflow_block_writer_policy() {
    let config = BufferConfig {
        capacity_bytes: 160,
        overflow_policy: OverflowPolicy::BlockWriter,
        overflow_fallback_bytes: 320,
    };
    let mut buffer = PerCoreWalBuffer::new(1, config);

    assert_eq!(
        buffer.append(make_record(1, 1, 48)),
        AppendOutcome::Appended
    );
    assert_eq!(buffer.append(make_record(1, 2, 48)), AppendOutcome::Blocked);
    assert_eq!(buffer.overflow_len(), 0);
    assert_eq!(
        buffer.fallback_decision(),
        FallbackDecision::ContinueParallel
    );
}

#[test]
fn bd_ncivz_1_overflow_allocate_triggers_deterministic_fallback() {
    let config = BufferConfig {
        capacity_bytes: 192,
        overflow_policy: OverflowPolicy::AllocateOverflow,
        overflow_fallback_bytes: 170,
    };
    let mut buffer = PerCoreWalBuffer::new(2, config);

    assert_eq!(
        buffer.append(make_record(2, 1, 64)),
        AppendOutcome::Appended
    );
    assert_eq!(
        buffer.append(make_record(2, 2, 64)),
        AppendOutcome::QueuedOverflow
    );
    assert_eq!(
        buffer.append(make_record(2, 3, 64)),
        AppendOutcome::QueuedOverflow
    );

    assert_eq!(
        buffer.fallback_decision(),
        FallbackDecision::ForceSerializedDrain
    );
    assert_eq!(buffer.overflow_len(), 2);

    let drained = buffer.force_serialized_drain();
    assert_eq!(drained.len(), 3);
    assert_eq!(buffer.active_len(), 0);
    assert_eq!(buffer.flush_len(), 0);
    assert_eq!(buffer.overflow_len(), 0);
    assert_eq!(
        buffer.fallback_decision(),
        FallbackDecision::ContinueParallel
    );
}

#[test]
fn bd_ncivz_1_per_core_pool_concurrent_writers_no_contention() {
    let pool = Arc::new(PerCoreWalBufferPool::new(8, BufferConfig::default()));
    let records_per_core = 400_u64;
    let runtime = test_runtime();
    let handle = runtime.handle();

    runtime.block_on(async {
        let mut writer_tasks = Vec::new();
        for core_id in 0..8_usize {
            let pool_ref = Arc::clone(&pool);
            writer_tasks.push(
                handle
                    .try_spawn(async move {
                        spawn_blocking(move || {
                            for seq in 0..records_per_core {
                                let record = make_record(core_id, seq, 64);
                                let outcome = pool_ref
                                    .append_to_core(core_id, record)
                                    .expect("core index should exist");
                                assert!(
                                    matches!(
                                        outcome,
                                        AppendOutcome::Appended | AppendOutcome::QueuedOverflow
                                    ),
                                    "append outcome should not block"
                                );
                            }
                        })
                        .await;
                    })
                    .expect("writer task should spawn"),
            );
        }

        for writer_task in writer_tasks {
            writer_task.await;
        }
    });

    assert_eq!(pool.contention_events_total(), 0);
}

#[test]
fn bd_ncivz_2_epoch_counter_defaults_to_10ms_interval() {
    let coordinator =
        EpochOrderCoordinator::new(2, BufferConfig::default(), EpochConfig::default());
    assert_eq!(coordinator.current_epoch(), 0);
    assert_eq!(
        coordinator.epoch_advance_interval(),
        Duration::from_millis(10)
    );
    assert_eq!(coordinator.pool.core_count(), 2);
}

#[test]
fn bd_ncivz_2_epoch_fence_waits_for_active_core_observation() {
    let coordinator = Arc::new(EpochOrderCoordinator::new(
        2,
        BufferConfig::default(),
        EpochConfig::default(),
    ));
    coordinator
        .observe_epoch(0)
        .expect("core 0 should be valid");
    coordinator
        .observe_epoch(1)
        .expect("core 1 should be valid");
    let runtime = test_runtime();
    let handle = runtime.handle();

    let next_epoch = runtime.block_on(async {
        let observer_ref = Arc::clone(&coordinator);
        let observer = handle
            .try_spawn(async move {
                sleep(wall_now(), Duration::from_millis(8)).await;
                observer_ref
                    .observe_epoch(0)
                    .expect("core 0 observation should succeed");
                observer_ref
                    .observe_epoch(1)
                    .expect("core 1 observation should succeed");
            })
            .expect("observer task should spawn");

        let fence_ref = Arc::clone(&coordinator);
        let next_epoch = spawn_blocking(move || {
            fence_ref.advance_epoch_and_wait(&[0, 1], Duration::from_millis(200))
        })
        .await
        .expect("fence should complete after observations");
        observer.await;
        next_epoch
    });

    assert_eq!(next_epoch, 1);
    assert_eq!(coordinator.current_epoch(), 1);
}

#[test]
fn bd_ncivz_2_append_during_epoch_fence_stays_in_previous_epoch_lane() {
    let coordinator = Arc::new(EpochOrderCoordinator::new(
        1,
        BufferConfig::default(),
        EpochConfig::default(),
    ));
    coordinator
        .observe_epoch(0)
        .expect("core 0 observation should succeed");
    coordinator
        .append_to_core(0, 1, 64)
        .expect("initial append should succeed");

    let runtime = test_runtime();
    let handle = runtime.handle();

    runtime.block_on(async {
        let advance_ref = Arc::clone(&coordinator);
        let advance = handle
            .try_spawn(async move {
                spawn_blocking(move || {
                    advance_ref.advance_epoch_and_wait(&[0], Duration::from_millis(200))
                })
                .await
            })
            .expect("advance task should spawn");

        let deadline = Instant::now() + Duration::from_millis(200);
        while coordinator.current_epoch() != 1 {
            assert!(
                Instant::now() < deadline,
                "advance should publish epoch 1 before timing out"
            );
            std::thread::yield_now();
        }
        assert_eq!(
            coordinator.current_append_epoch(),
            0,
            "writers must keep appending to the previous epoch until sealing completes"
        );

        coordinator
            .append_to_core(0, 2, 64)
            .expect("append during fence window should still succeed");
        coordinator
            .observe_epoch(0)
            .expect("core 0 observation should release the fence");

        let next_epoch = advance.await.expect("advance should complete");
        assert_eq!(next_epoch, 1);
    });

    let batch = coordinator
        .flush_epoch(0)
        .expect("flush should not see an epoch straddle");
    assert_eq!(batch.total_records(), 2);
    assert_eq!(batch.records_per_core, vec![2]);
    assert!(
        batch.records.iter().all(|record| record.epoch == 0),
        "records appended during the fence window must stay in epoch 0: {:?}",
        batch
            .records
            .iter()
            .map(|record| record.epoch)
            .collect::<Vec<_>>()
    );
}

#[test]
fn bd_ncivz_2_group_commit_flushes_epoch_across_cores() {
    let coordinator = Arc::new(EpochOrderCoordinator::new(
        2,
        BufferConfig::default(),
        EpochConfig::default(),
    ));

    coordinator
        .observe_epoch(0)
        .expect("core 0 should be valid");
    coordinator
        .observe_epoch(1)
        .expect("core 1 should be valid");

    assert_eq!(
        coordinator
            .append_to_core(0, 1, 64)
            .expect("append on core 0 should succeed"),
        AppendOutcome::Appended
    );
    assert_eq!(
        coordinator
            .append_to_core(0, 2, 64)
            .expect("append on core 0 should succeed"),
        AppendOutcome::Appended
    );
    assert_eq!(
        coordinator
            .append_to_core(1, 3, 64)
            .expect("append on core 1 should succeed"),
        AppendOutcome::Appended
    );
    let runtime = test_runtime();
    let handle = runtime.handle();

    runtime.block_on(async {
        let observer_ref = Arc::clone(&coordinator);
        let observer = handle
            .try_spawn(async move {
                sleep(wall_now(), Duration::from_millis(6)).await;
                observer_ref
                    .observe_epoch(0)
                    .expect("core 0 observation should succeed");
                observer_ref
                    .observe_epoch(1)
                    .expect("core 1 observation should succeed");
            })
            .expect("observer task should spawn");

        let advance_ref = Arc::clone(&coordinator);
        spawn_blocking(move || {
            advance_ref.advance_epoch_and_wait(&[0, 1], Duration::from_millis(200))
        })
        .await
        .expect("epoch advance should succeed");
        observer.await;

        let batch = coordinator
            .flush_epoch(0)
            .expect("epoch 0 flush should succeed");
        assert_eq!(batch.epoch, 0);
        assert_eq!(batch.records_per_core, vec![2, 1]);
        assert_eq!(batch.total_records(), 3);
        assert!(batch.records.iter().all(|record| record.epoch == 0));

        assert_eq!(coordinator.durable_epoch(), None);
        coordinator.mark_epoch_durable(0);
        assert_eq!(coordinator.durable_epoch(), Some(0));
        coordinator
            .wait_until_epoch_durable(0, Duration::from_millis(25))
            .expect("durability wait should pass");
    });
}

#[test]
fn bd_ncivz_2_writers_block_until_epoch_is_durable() {
    let coordinator = Arc::new(EpochOrderCoordinator::new(
        1,
        BufferConfig::default(),
        EpochConfig::default(),
    ));
    coordinator
        .observe_epoch(0)
        .expect("core 0 observation should succeed");
    coordinator
        .append_to_core(0, 1, 64)
        .expect("append should succeed");
    let runtime = test_runtime();
    let handle = runtime.handle();

    runtime.block_on(async {
        let waiter_ref = Arc::clone(&coordinator);
        let waiter = handle
            .try_spawn(async move {
                spawn_blocking(move || {
                    waiter_ref.wait_until_epoch_durable(0, Duration::from_millis(600))
                })
                .await
            })
            .expect("waiter task should spawn");

        sleep(wall_now(), Duration::from_millis(30)).await;
        assert!(
            !waiter.is_finished(),
            "writer should still be waiting before flush"
        );

        let advance_ref = Arc::clone(&coordinator);
        spawn_blocking(move || advance_ref.advance_epoch_and_wait(&[], Duration::from_millis(25)))
            .await
            .expect("advancing with no active fence set should succeed");
        assert_eq!(
            coordinator
                .flush_epoch(0)
                .expect("flush should succeed")
                .total_records(),
            1
        );
        assert_eq!(coordinator.durable_epoch(), None);
        coordinator.mark_epoch_durable(0);

        waiter.await.expect("epoch should become durable");
    });
}

#[test]
fn bd_ncivz_2_recovery_replays_in_epoch_order() {
    let mut r1 = make_record(0, 10, 16);
    r1.epoch = 2;
    let mut r2 = make_record(1, 2, 16);
    r2.epoch = 1;
    let mut r3 = make_record(0, 3, 16);
    r3.epoch = 1;
    let mut r4 = make_record(1, 9, 16);
    r4.epoch = 2;

    let ordered = EpochOrderCoordinator::recovery_order(&[r1, r2, r3, r4]);
    assert!(
        ordered
            .windows(2)
            .all(|pair| pair[0].epoch <= pair[1].epoch)
    );
    assert_eq!(ordered[0].epoch, 1);
    assert_eq!(ordered[1].epoch, 1);
    assert_eq!(ordered[2].epoch, 2);
    assert_eq!(ordered[3].epoch, 2);
}

#[test]
fn bd_ncivz_2_epoch_fence_timeout_when_active_core_not_observed() {
    let coordinator =
        EpochOrderCoordinator::new(2, BufferConfig::default(), EpochConfig::default());
    let error = coordinator
        .advance_epoch_and_wait(&[1], Duration::from_millis(20))
        .expect_err("fence must timeout without active core observation");
    assert!(
        error.contains("timed out"),
        "error should describe fence timeout: {error}"
    );
}

#[test]
fn bd_ncivz_2_epoch_fence_timeout_does_not_poison_next_advance() {
    let coordinator =
        EpochOrderCoordinator::new(1, BufferConfig::default(), EpochConfig::default());

    coordinator
        .append_to_core(0, 1, 64)
        .expect("initial append should succeed");

    let error = coordinator
        .advance_epoch_and_wait(&[0], Duration::from_millis(20))
        .expect_err("fence must timeout without active core observation");
    assert!(
        error.contains("timed out"),
        "error should describe fence timeout: {error}"
    );
    assert_eq!(
        coordinator.current_epoch(),
        0,
        "timed-out epoch advance must roll back the published epoch"
    );
    assert_eq!(
        coordinator.current_append_epoch(),
        0,
        "timed-out epoch advance must not move the append epoch"
    );

    coordinator
        .append_to_core(0, 2, 64)
        .expect("append after timeout should still use epoch 0");
    coordinator
        .advance_epoch_and_wait(&[], Duration::from_millis(25))
        .expect("retry advance should succeed");

    let batch = coordinator
        .flush_epoch(0)
        .expect("retry advance should seal the original epoch");
    assert_eq!(batch.total_records(), 2);
    assert!(
        batch.records.iter().all(|record| record.epoch == 0),
        "timed-out fence must not relabel epoch-0 records: {:?}",
        batch
            .records
            .iter()
            .map(|record| record.epoch)
            .collect::<Vec<_>>()
    );
}

#[test]
fn bd_ncivz_2_fence_detects_epoch_boundary_straddle() {
    let pool = PerCoreWalBufferPool::new(1, BufferConfig::default());
    let mut epoch0 = make_record(0, 1, 64);
    epoch0.epoch = 0;
    let mut epoch1 = make_record(0, 2, 64);
    epoch1.epoch = 1;

    assert_eq!(
        pool.append_to_core(0, epoch0)
            .expect("append should succeed"),
        AppendOutcome::Appended
    );
    assert_eq!(
        pool.append_to_core(0, epoch1)
            .expect("append should succeed"),
        AppendOutcome::Appended
    );
    pool.seal_active_for_epoch(0)
        .expect("sealing should succeed");

    let error = pool
        .flush_epoch_batches(0)
        .expect_err("mixed epochs in one flush lane must fail");
    assert!(
        error.contains("straddle"),
        "error should report epoch straddle: {error}"
    );
}

#[test]
fn bd_ncivz_2_commits_across_epochs_preserve_serial_epoch_order() {
    let coordinator =
        EpochOrderCoordinator::new(1, BufferConfig::default(), EpochConfig::default());
    coordinator
        .observe_epoch(0)
        .expect("core 0 observation should succeed");

    coordinator
        .append_to_core(0, 1, 64)
        .expect("append should succeed");
    coordinator
        .advance_epoch_and_wait(&[], Duration::from_millis(25))
        .expect("advance should succeed");
    let first_batch = coordinator
        .flush_epoch(0)
        .expect("epoch 0 flush should succeed");

    coordinator
        .observe_epoch(0)
        .expect("core 0 observation should succeed");
    coordinator
        .append_to_core(0, 2, 64)
        .expect("append should succeed");
    coordinator
        .advance_epoch_and_wait(&[], Duration::from_millis(25))
        .expect("advance should succeed");
    let second_batch = coordinator
        .flush_epoch(1)
        .expect("epoch 1 flush should succeed");

    let mut combined = first_batch.records;
    combined.extend(second_batch.records);
    let ordered = EpochOrderCoordinator::recovery_order(&combined);
    assert!(
        ordered.windows(2).all(|pair| {
            pair[0].epoch < pair[1].epoch
                || (pair[0].epoch == pair[1].epoch
                    && pair[0].begin_seq.get() <= pair[1].begin_seq.get())
        }),
        "recovery ordering should be monotonic by epoch and begin_seq"
    );
}

#[test]
fn bd_ncivz_2_abort_cleanup_drops_non_committed_records() {
    let coordinator =
        EpochOrderCoordinator::new(1, BufferConfig::default(), EpochConfig::default());
    coordinator
        .observe_epoch(0)
        .expect("core 0 observation should succeed");

    coordinator
        .append_to_core(0, 1, 64)
        .expect("committed append should succeed");

    let mut aborted = make_record(0, 2, 64);
    aborted.epoch = coordinator.current_epoch();
    aborted.end_seq = None;
    let aborted_txn_id = aborted.txn_token.id.get();
    assert_eq!(
        coordinator
            .pool
            .append_to_core(0, aborted)
            .expect("aborted append should still enter active lane"),
        AppendOutcome::Appended
    );

    coordinator
        .advance_epoch_and_wait(&[], Duration::from_millis(25))
        .expect("epoch advance should succeed");
    let batch = coordinator
        .flush_epoch(0)
        .expect("flush should prune aborted records");

    assert_eq!(batch.total_records(), 1);
    assert!(
        batch.records.iter().all(|record| record.end_seq.is_some()),
        "all flushed records must be committed"
    );
    assert!(
        batch
            .records
            .iter()
            .all(|record| record.txn_token.id.get() != aborted_txn_id),
        "aborted record must not survive cleanup"
    );

    let ordered = EpochOrderCoordinator::recovery_order(&batch.records);
    assert_eq!(ordered.len(), 1);
    assert!(
        ordered[0].end_seq.is_some(),
        "recovery input should only contain committed records"
    );
}

// -- PerCoreWalBuffer unit-level state machine tests --

#[test]
fn seal_active_on_already_sealed_returns_error() {
    let config = BufferConfig {
        capacity_bytes: 640,
        ..BufferConfig::default()
    };
    let mut buf = PerCoreWalBuffer::new(0, config);
    buf.seal_active(1).expect("first seal");
    assert!(buf.seal_active(2).is_err());
}

#[test]
fn begin_flush_on_writable_returns_error() {
    let config = BufferConfig {
        capacity_bytes: 640,
        ..BufferConfig::default()
    };
    let mut buf = PerCoreWalBuffer::new(0, config);
    assert!(buf.begin_flush().is_err());
}

#[test]
fn complete_flush_on_non_flushing_returns_error() {
    let config = BufferConfig {
        capacity_bytes: 640,
        ..BufferConfig::default()
    };
    let mut buf = PerCoreWalBuffer::new(0, config);
    assert!(buf.complete_flush().is_err());
}

#[test]
fn force_serialized_drain_collects_all_lanes() {
    let config = BufferConfig {
        capacity_bytes: 640,
        overflow_policy: OverflowPolicy::AllocateOverflow,
        overflow_fallback_bytes: 4096,
    };
    let mut buf = PerCoreWalBuffer::new(0, config);

    buf.append(make_record(0, 1, 64));
    buf.seal_active(1).unwrap();
    buf.begin_flush().unwrap();

    buf.append(make_record(0, 2, 64));

    let drained = buf.force_serialized_drain();
    assert_eq!(drained.len(), 2);
    assert_eq!(buf.active_state(), BufferState::Writable);
    assert_eq!(buf.flush_state(), BufferState::Writable);
    assert_eq!(buf.active_len(), 0);
    assert_eq!(buf.flush_len(), 0);
    assert_eq!(buf.overflow_len(), 0);
    assert_eq!(buf.fallback_decision(), FallbackDecision::ContinueParallel);
}

#[test]
fn overflow_drains_into_active_after_complete_flush() {
    let config = BufferConfig {
        capacity_bytes: 160,
        overflow_policy: OverflowPolicy::AllocateOverflow,
        overflow_fallback_bytes: 4096,
    };
    let mut buf = PerCoreWalBuffer::new(0, config);

    buf.append(make_record(0, 1, 48));
    assert_eq!(
        buf.append(make_record(0, 2, 48)),
        AppendOutcome::QueuedOverflow
    );
    assert_eq!(buf.active_len(), 1);
    assert_eq!(buf.overflow_len(), 1);

    buf.seal_active(1).unwrap();
    buf.begin_flush().unwrap();
    buf.complete_flush().unwrap();

    assert!(buf.overflow_len() == 0 || buf.active_len() > 0);
    assert_eq!(buf.active_state(), BufferState::Writable);
}

#[test]
fn append_record_exceeding_capacity_latches_fallback() {
    let config = BufferConfig {
        capacity_bytes: 32,
        ..BufferConfig::default()
    };
    let mut buf = PerCoreWalBuffer::new(0, config);
    let outcome = buf.append(make_record(0, 1, 128));
    assert_eq!(outcome, AppendOutcome::Blocked);
    assert_eq!(
        buf.fallback_decision(),
        FallbackDecision::ForceSerializedDrain
    );
}

#[test]
fn append_batch_with_oversized_single_record_latches_fallback() {
    let config = BufferConfig {
        capacity_bytes: 32,
        ..BufferConfig::default()
    };
    let mut buf = PerCoreWalBuffer::new(0, config);
    let outcome = buf.append_batch(vec![make_record(0, 1, 128)]);
    assert_eq!(outcome, AppendOutcome::Blocked);
    assert_eq!(
        buf.fallback_decision(),
        FallbackDecision::ForceSerializedDrain
    );
}

#[test]
fn append_batch_all_fit_returns_appended() {
    let config = BufferConfig {
        capacity_bytes: 4096,
        ..BufferConfig::default()
    };
    let mut buf = PerCoreWalBuffer::new(0, config);
    let records = vec![make_record(0, 1, 32), make_record(0, 2, 32)];
    assert_eq!(buf.append_batch(records), AppendOutcome::Appended);
    assert_eq!(buf.active_len(), 2);
}

#[test]
fn recovery_order_sorts_by_epoch_then_seq() {
    let mut r1 = make_record(0, 10, 16);
    r1.epoch = 2;
    let mut r2 = make_record(0, 5, 16);
    r2.epoch = 1;
    let mut r3 = make_record(0, 7, 16);
    r3.epoch = 2;

    let ordered = EpochOrderCoordinator::recovery_order(&[r1, r2.clone(), r3]);
    assert_eq!(ordered[0].epoch, 1);
    assert_eq!(ordered[0].begin_seq.get(), r2.begin_seq.get());
    assert!(ordered[1].epoch <= ordered[2].epoch);
}

#[test]
fn buffer_config_default_values() {
    let config = BufferConfig::default();
    assert_eq!(config.capacity_bytes, DEFAULT_BUFFER_CAPACITY_BYTES);
    assert_eq!(
        config.overflow_fallback_bytes,
        DEFAULT_OVERFLOW_FALLBACK_BYTES
    );
    assert_eq!(config.overflow_policy, OverflowPolicy::AllocateOverflow);
}

#[test]
fn thread_buffer_slot_stable_within_thread() {
    let slot1 = thread_buffer_slot(16);
    let slot2 = thread_buffer_slot(16);
    assert_eq!(slot1, slot2);
    assert!(slot1 < 16);
}

#[test]
fn pool_invalid_core_id_returns_error() {
    let pool = PerCoreWalBufferPool::new(2, BufferConfig::default());
    assert!(pool.append_to_core(2, make_record(0, 1, 16)).is_err());
    assert!(pool.append_to_core(99, make_record(0, 1, 16)).is_err());
    assert!(pool.append_to_core(0, make_record(0, 1, 16)).is_ok());
}

#[test]
fn pool_core_count_and_contention_start_at_zero() {
    let pool = PerCoreWalBufferPool::new(4, BufferConfig::default());
    assert_eq!(pool.core_count(), 4);
    assert_eq!(pool.contention_events_total(), 0);
}

#[test]
fn epoch_config_default_values() {
    let cfg = EpochConfig::default();
    assert_eq!(cfg.advance_interval_ms, DEFAULT_EPOCH_ADVANCE_INTERVAL_MS);
}

#[test]
fn epoch_flush_batch_total_records() {
    let batch = EpochFlushBatch {
        epoch: 1,
        records: vec![make_record(0, 1, 8), make_record(0, 2, 8)],
        records_per_core: vec![2],
    };
    assert_eq!(batch.total_records(), 2);

    let empty = EpochFlushBatch {
        epoch: 0,
        records: Vec::new(),
        records_per_core: Vec::new(),
    };
    assert_eq!(empty.total_records(), 0);
}

#[test]
fn wal_record_encoded_len_includes_images() {
    let r = make_record(0, 1, 100);
    let len = r.encoded_len();
    assert!(len >= RECORD_FIXED_OVERHEAD_BYTES + 200);
    let small = make_record(0, 1, 0);
    assert!(small.encoded_len() >= RECORD_FIXED_OVERHEAD_BYTES);
    assert!(small.encoded_len() < len);
}

#[test]
fn buffer_state_variant_equality() {
    assert_eq!(BufferState::Writable, BufferState::Writable);
    assert_eq!(
        BufferState::Sealed { epoch: 5 },
        BufferState::Sealed { epoch: 5 }
    );
    assert_ne!(
        BufferState::Sealed { epoch: 5 },
        BufferState::Sealed { epoch: 6 }
    );
    assert_ne!(
        BufferState::Sealed { epoch: 1 },
        BufferState::Flushing { epoch: 1 }
    );
    let dbg = format!("{:?}", BufferState::Flushing { epoch: 3 });
    assert!(dbg.contains("Flushing"));
}

#[test]
fn pool_append_batch_to_core_works() {
    let pool = PerCoreWalBufferPool::new(2, BufferConfig::default());
    let records = vec![make_record(0, 1, 16), make_record(0, 2, 16)];
    let outcome = pool.append_batch_to_core(0, records).unwrap();
    assert_eq!(outcome, AppendOutcome::Appended);
    assert!(
        pool.append_batch_to_core(99, vec![make_record(0, 1, 16)])
            .is_err()
    );
}

#[test]
fn overflow_policy_debug_clone_copy_eq() {
    let a = OverflowPolicy::BlockWriter;
    let b = OverflowPolicy::AllocateOverflow;
    assert_ne!(a, b);
    let copied = a;
    assert_eq!(copied, a);
    let dbg = format!("{a:?}");
    assert!(dbg.contains("BlockWriter"));
    let dbg_b = format!("{b:?}");
    assert!(dbg_b.contains("AllocateOverflow"));
}

#[test]
fn append_outcome_debug_clone_copy_eq() {
    let variants = [
        AppendOutcome::Appended,
        AppendOutcome::QueuedOverflow,
        AppendOutcome::Blocked,
    ];
    for v in &variants {
        let copied = *v;
        assert_eq!(copied, *v);
    }
    let dbg = format!("{:?}", AppendOutcome::QueuedOverflow);
    assert!(dbg.contains("QueuedOverflow"));
    assert_ne!(AppendOutcome::Appended, AppendOutcome::Blocked);
}

#[test]
fn fallback_decision_debug_clone_copy_eq() {
    let a = FallbackDecision::ContinueParallel;
    let b = FallbackDecision::ForceSerializedDrain;
    assert_ne!(a, b);
    let copied = a;
    assert_eq!(copied, a);
    let dbg = format!("{b:?}");
    assert!(dbg.contains("ForceSerializedDrain"));
}

#[test]
fn wal_record_debug_and_clone() {
    let r = make_record(0, 5, 32);
    let dbg = format!("{r:?}");
    assert!(dbg.contains("WalRecord"));
    let cloned = r.clone();
    assert_eq!(cloned.epoch, r.epoch);
    assert_eq!(cloned.page_id, r.page_id);
    assert_eq!(cloned.before_image.len(), r.before_image.len());
    assert_eq!(cloned.after_image, r.after_image);
}

#[test]
fn epoch_coordinator_mark_and_query_durable_epoch() {
    let coord = EpochOrderCoordinator::new(2, BufferConfig::default(), EpochConfig::default());
    assert_eq!(coord.durable_epoch(), None);
    coord.mark_epoch_durable(5);
    assert_eq!(coord.durable_epoch(), Some(5));
    coord.mark_epoch_durable(3);
    assert_eq!(coord.durable_epoch(), Some(5), "should keep max");
    coord.mark_epoch_durable(10);
    assert_eq!(coord.durable_epoch(), Some(10));
}

#[test]
fn buffer_config_clone_copy_debug() {
    let cfg = BufferConfig::default();
    let copied = cfg;
    assert_eq!(copied, cfg);
    let dbg = format!("{cfg:?}");
    assert!(dbg.contains("BufferConfig"));
}

#[test]
fn epoch_config_default_and_epoch_flush_batch_total() {
    let ecfg = EpochConfig::default();
    assert_eq!(ecfg.advance_interval_ms, DEFAULT_EPOCH_ADVANCE_INTERVAL_MS);
    let batch = EpochFlushBatch {
        epoch: 5,
        records: Vec::new(),
        records_per_core: vec![0, 0],
    };
    assert_eq!(batch.total_records(), 0);
    assert_eq!(batch.epoch, 5);
}

#[test]
fn epoch_order_coordinator_current_and_append_epoch() {
    let coord = EpochOrderCoordinator::new(2, BufferConfig::default(), EpochConfig::default());
    assert_eq!(coord.current_epoch(), 0);
    assert_eq!(coord.current_append_epoch(), 0);
}

#[test]
fn per_core_buffer_pool_core_count() {
    let pool = PerCoreWalBufferPool::new(4, BufferConfig::default());
    assert_eq!(pool.core_count(), 4);
}
