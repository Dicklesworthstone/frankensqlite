use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, TryLockError};
use std::thread;

use fsqlite_types::{CommitSeq, PageNumber, TxnEpoch, TxnId, TxnToken};

const DEFAULT_BUFFER_CAPACITY_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_OVERFLOW_FALLBACK_BYTES: usize = 8 * 1024 * 1024;
const RECORD_FIXED_OVERHEAD_BYTES: usize = 48;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OverflowPolicy {
    BlockWriter,
    AllocateOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BufferConfig {
    capacity_bytes: usize,
    overflow_policy: OverflowPolicy,
    overflow_fallback_bytes: usize,
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
enum BufferState {
    Writable,
    Sealed { epoch: u64 },
    Flushing { epoch: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppendOutcome {
    Appended,
    QueuedOverflow,
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FallbackDecision {
    ContinueParallel,
    ForceSerializedDrain,
}

#[derive(Debug, Clone)]
struct WalRecord {
    txn_token: TxnToken,
    epoch: u64,
    page_id: PageNumber,
    begin_seq: CommitSeq,
    end_seq: Option<CommitSeq>,
    before_image: Vec<u8>,
    after_image: Vec<u8>,
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
struct PerCoreWalBuffer {
    config: BufferConfig,
    active: BufferLane,
    flush_lane: BufferLane,
    overflow: VecDeque<WalRecord>,
    overflow_bytes: usize,
    fallback_latched: bool,
}

impl PerCoreWalBuffer {
    fn new(_core_id: usize, config: BufferConfig) -> Self {
        Self {
            config,
            active: BufferLane::new_writable(),
            flush_lane: BufferLane::new_writable(),
            overflow: VecDeque::new(),
            overflow_bytes: 0,
            fallback_latched: false,
        }
    }

    fn append(&mut self, record: WalRecord) -> AppendOutcome {
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

    fn seal_active(&mut self, epoch: u64) -> Result<(), &'static str> {
        if self.active.state != BufferState::Writable {
            return Err("active lane is not writable");
        }
        self.active.state = BufferState::Sealed { epoch };
        Ok(())
    }

    fn begin_flush(&mut self) -> Result<usize, &'static str> {
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

    fn complete_flush(&mut self) -> Result<(), &'static str> {
        if !matches!(self.flush_lane.state, BufferState::Flushing { .. }) {
            return Err("flush lane is not in flushing state");
        }

        self.flush_lane.records.clear();
        self.flush_lane.bytes_used = 0;
        self.flush_lane.state = BufferState::Writable;
        self.drain_overflow_into_active();
        Ok(())
    }

    fn fallback_decision(&self) -> FallbackDecision {
        if self.fallback_latched {
            FallbackDecision::ForceSerializedDrain
        } else {
            FallbackDecision::ContinueParallel
        }
    }

    fn force_serialized_drain(&mut self) -> usize {
        let drained = self
            .active
            .records
            .len()
            .saturating_add(self.flush_lane.records.len())
            .saturating_add(self.overflow.len());

        self.active.records.clear();
        self.active.bytes_used = 0;
        self.active.state = BufferState::Writable;

        self.flush_lane.records.clear();
        self.flush_lane.bytes_used = 0;
        self.flush_lane.state = BufferState::Writable;

        self.overflow.clear();
        self.overflow_bytes = 0;
        self.fallback_latched = false;
        drained
    }

    fn active_state(&self) -> BufferState {
        self.active.state
    }

    fn flush_state(&self) -> BufferState {
        self.flush_lane.state
    }

    fn active_len(&self) -> usize {
        self.active.records.len()
    }

    fn flush_len(&self) -> usize {
        self.flush_lane.records.len()
    }

    fn overflow_len(&self) -> usize {
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

    fn contention_events(&self) -> u64 {
        self.contention_events.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
struct PerCoreWalBufferPool {
    cells: Vec<BufferCell>,
}

impl PerCoreWalBufferPool {
    fn new(core_count: usize, config: BufferConfig) -> Self {
        assert!(core_count > 0, "core_count must be > 0");
        let mut cells = Vec::with_capacity(core_count);
        for core_id in 0..core_count {
            cells.push(BufferCell::new(core_id, config));
        }
        Self { cells }
    }

    fn append_to_core(&self, core_id: usize, record: WalRecord) -> Result<AppendOutcome, String> {
        let Some(cell) = self.cells.get(core_id) else {
            return Err(format!(
                "invalid core_id={core_id}; available cores={}",
                self.cells.len()
            ));
        };
        Ok(cell.append(record))
    }

    fn contention_events_total(&self) -> u64 {
        self.cells
            .iter()
            .map(BufferCell::contention_events)
            .sum::<u64>()
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
        end_seq: None,
        before_image: vec![0x10; payload_len],
        after_image: vec![0x20; payload_len],
    }
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
    assert_eq!(drained, 3);
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

    let mut handles = Vec::new();
    for core_id in 0..8_usize {
        let pool_ref = Arc::clone(&pool);
        handles.push(thread::spawn(move || {
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
        }));
    }

    for handle in handles {
        handle.join().expect("writer thread should complete");
    }

    assert_eq!(pool.contention_events_total(), 0);
}
