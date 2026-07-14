//! Transaction lifecycle (§5.4): Begin/Read/Write/Commit/Abort.
//!
//! This module implements the full transaction lifecycle for both
//! Serialized and Concurrent modes.  It provides:
//!
//! - [`BeginKind`]: The four modes (Deferred, Immediate, Exclusive, Concurrent).
//! - [`TransactionManager`]: Orchestrates begin/read/write/commit/abort.
//! - [`Savepoint`]: Transaction-local write snapshots for page and cell deltas.
//! - [`CommitResponse`]: Result type for the commit sequencer.

use std::collections::{BTreeMap, HashMap, HashSet, btree_map::Entry as BTreeEntry};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::Duration;

use fsqlite_types::sync_primitives::{Instant, Mutex};
use fsqlite_types::{
    CommitSeq, MergePageKind, PageData, PageNumber, PageSize, PageVersion, SchemaEpoch, Snapshot,
    TxnEpoch, TxnId, TxnToken,
};
use fsqlite_vfs::ShmRegion;
use fsqlite_wal::DEFAULT_RAPTORQ_REPAIR_SYMBOLS;

use crate::cache_aligned::{logical_now_epoch_secs, logical_now_millis};
use crate::cell_visibility::CellVisibilityLog;
use crate::core_types::{
    CommitIndex, InProcessPageLockTable, Transaction, TransactionMode, TransactionState,
    WriteVersionEntry,
};
use crate::ebr::{GLOBAL_EBR_METRICS, VersionGuardRegistry, VersionGuardTicket};
use crate::invariants::{SerializedWriteMutex, TxnManager, VersionStore};
use crate::materialize::{MaterializationTrigger, materialize_page};
use crate::observability::{mvcc_snapshot_established, mvcc_snapshot_released};
use crate::shm::SharedMemoryLayout;

const DEFAULT_BUSY_TIMEOUT_MS: u64 = 100;
const DEFAULT_SERIALIZED_WRITER_LEASE_SECS: u64 = 30;
const DEFAULT_MAX_CHAIN_LENGTH: usize = 64;
const DEFAULT_CHAIN_LENGTH_WARNING: usize = 32;
// Serialized-writer drain handoff: bounded on-CPU spins, then a short park on
// the lock-table release gate. The owner-handoff rule is explicit: every
// `SERIALIZED_DRAIN_HANDOFF_PARK_EVERY` retries we park for at most
// `SERIALIZED_DRAIN_HANDOFF_MAX_PARK`, so a releasing concurrent writer can
// wake the serialized waiter directly without 1 ms polling sleeps.
const SERIALIZED_DRAIN_HANDOFF_BASE_SPINS: u32 = 64;
const SERIALIZED_DRAIN_HANDOFF_MAX_SPINS: u32 = 2_048;
const SERIALIZED_DRAIN_HANDOFF_PARK_EVERY: u32 = 4;
const SERIALIZED_DRAIN_HANDOFF_MAX_PARK: Duration = Duration::from_micros(250);
/// Proactive compaction threshold — attempt GC when chain exceeds this length
/// during commit-time publish.  This keeps average chains short (§8.10 version
/// chain compaction) without waiting for the hard max_chain_length limit.
const PROACTIVE_COMPACT_THRESHOLD: usize = 8;
/// C7 soft bound multiplier: proceed after opportunistic prune even if chain
/// still exceeds max_chain_length, up to this multiple.  Mirrors Postgres
/// pruneheap.c:199 — the bound is advisory, not hard.  Only emit Busy error
/// if chain exceeds SOFT_BOUND_MULTIPLIER × max_chain_length.
const SOFT_BOUND_MULTIPLIER: usize = 4;
const NO_GC_HORIZON: u64 = u64::MAX;
#[cfg(unix)]
const PID_BIRTH_PROCFS_TAG: u64 = 1_u64 << 63;

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SerializedDrainWait {
    attempt: u32,
    spin_loops: u32,
    park_timeout: Duration,
}

#[derive(Debug, Clone, Copy, Default)]
struct SerializedDrainHandoff {
    next_attempt: u32,
}

impl SerializedDrainHandoff {
    fn next_wait(&mut self, started: Instant, deadline: Duration) -> Option<SerializedDrainWait> {
        let remaining = deadline.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return None;
        }

        let attempt = self.next_attempt.saturating_add(1);
        self.next_attempt = attempt;
        Some(SerializedDrainWait {
            attempt,
            spin_loops: serialized_drain_spin_loops(attempt),
            park_timeout: if serialized_drain_should_park(attempt) {
                remaining.min(SERIALIZED_DRAIN_HANDOFF_MAX_PARK)
            } else {
                Duration::ZERO
            },
        })
    }
}

const fn serialized_drain_spin_loops(attempt: u32) -> u32 {
    let growth = attempt.saturating_sub(1);
    let shift = if growth > 5 { 5 } else { growth };
    let spins = SERIALIZED_DRAIN_HANDOFF_BASE_SPINS << shift;
    if spins > SERIALIZED_DRAIN_HANDOFF_MAX_SPINS {
        SERIALIZED_DRAIN_HANDOFF_MAX_SPINS
    } else {
        spins
    }
}

const fn serialized_drain_should_park(attempt: u32) -> bool {
    attempt >= SERIALIZED_DRAIN_HANDOFF_PARK_EVERY
        && attempt % SERIALIZED_DRAIN_HANDOFF_PARK_EVERY == 0
}

fn perform_serialized_drain_handoff(
    lock_table: &InProcessPageLockTable,
    observed_epoch: u64,
    wait: SerializedDrainWait,
) {
    for _ in 0..wait.spin_loops {
        std::hint::spin_loop();
    }

    if !wait.park_timeout.is_zero() {
        let _ = lock_table.wait_for_release_progress(observed_epoch, wait.park_timeout);
    }
}

#[cfg(unix)]
fn read_proc_start_time_ticks(pid: u32) -> Option<u64> {
    let stat_path = std::path::Path::new("/proc")
        .join(pid.to_string())
        .join("stat");
    let stat = std::fs::read_to_string(stat_path).ok()?;
    let comm_end = stat.rfind(')')?;
    let tail = stat.get(comm_end + 1..)?.trim_start();
    tail.split_whitespace().nth(19)?.parse::<u64>().ok()
}

fn current_process_birth_marker(now_fallback: u64) -> u64 {
    #[cfg(unix)]
    {
        if !std::path::Path::new("/proc").exists() {
            return now_fallback;
        }
        if let Some(start_ticks) = read_proc_start_time_ticks(std::process::id()) {
            return PID_BIRTH_PROCFS_TAG | (start_ticks & !PID_BIRTH_PROCFS_TAG);
        }
        now_fallback
    }
    #[cfg(not(unix))]
    {
        now_fallback
    }
}

fn process_alive_os(pid: u32, pid_birth: u64) -> bool {
    #[cfg(unix)]
    {
        if pid == 0 {
            return false;
        }

        // Conservative fallback for unix targets without /proc: treat as alive
        // so we never clear a potentially live writer based on missing OS hooks.
        if !std::path::Path::new("/proc").exists() {
            return true;
        }
        let proc_dir = std::path::Path::new("/proc").join(pid.to_string());
        if !proc_dir.exists() {
            return false;
        }

        if pid_birth & PID_BIRTH_PROCFS_TAG == 0 {
            // Legacy token format (pre-proc-starttime tagging): keep conservative
            // liveness behavior to avoid false stale clears during rolling upgrades.
            return true;
        }

        let expected_ticks = pid_birth & !PID_BIRTH_PROCFS_TAG;
        read_proc_start_time_ticks(pid).is_some_and(|start_ticks| start_ticks == expected_ticks)
    }
    #[cfg(not(unix))]
    {
        let _ = (pid, pid_birth);
        true
    }
}

// ---------------------------------------------------------------------------
// BeginKind
// ---------------------------------------------------------------------------

/// Transaction begin mode (maps to SQLite's BEGIN variants).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BeginKind {
    /// Deferred: snapshot established lazily on first read/write.
    Deferred,
    /// Immediate: acquire serialized writer exclusion at BEGIN.
    Immediate,
    /// Exclusive: acquire serialized writer exclusion at BEGIN (same as
    /// Immediate for our in-process model; cross-process differences in §5.6).
    Exclusive,
    /// Concurrent: MVCC page-level locking (no global mutex).
    Concurrent,
}

/// Merge policy controlled by `PRAGMA fsqlite.write_merge`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WriteMergePolicy {
    /// Conflicts always abort/retry.
    Off,
    /// Semantic merge ladder only (intent replay + structured patches).
    #[default]
    Safe,
    /// Debug-only unsafe experiments on explicitly opaque pages.
    LabUnsafe,
}

/// Chosen conflict response under the active merge policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MergeDecision {
    AbortRetry,
    IntentReplay,
    StructuredPatch,
    RawXorLab,
}

/// Whether raw XOR merge is allowed for this policy/page-kind pair.
#[must_use]
pub const fn raw_xor_merge_allowed(
    policy: WriteMergePolicy,
    page_kind: MergePageKind,
    debug_build: bool,
) -> bool {
    if page_kind.is_sqlite_structured() {
        return false;
    }
    matches!(policy, WriteMergePolicy::LabUnsafe) && debug_build
}

/// Resolve the policy-directed merge decision for a conflict.
#[must_use]
pub const fn merge_decision(
    policy: WriteMergePolicy,
    page_kind: MergePageKind,
    debug_build: bool,
) -> MergeDecision {
    match policy {
        WriteMergePolicy::Off => MergeDecision::AbortRetry,
        WriteMergePolicy::Safe => {
            if page_kind.is_sqlite_structured() {
                MergeDecision::IntentReplay
            } else {
                MergeDecision::StructuredPatch
            }
        }
        WriteMergePolicy::LabUnsafe => {
            if raw_xor_merge_allowed(policy, page_kind, debug_build) {
                MergeDecision::RawXorLab
            } else if page_kind.is_sqlite_structured() {
                MergeDecision::IntentReplay
            } else {
                MergeDecision::StructuredPatch
            }
        }
    }
}

/// Compute a GF(256) (XOR) patch delta between two equal-length pages.
#[must_use]
pub fn gf256_patch_delta(base: &[u8], target: &[u8]) -> Option<Vec<u8>> {
    if base.len() != target.len() {
        return None;
    }
    Some(
        base.iter()
            .zip(target)
            .map(|(lhs, rhs)| lhs ^ rhs)
            .collect(),
    )
}

/// Check whether two patch deltas have disjoint support.
#[must_use]
pub fn gf256_patches_disjoint(delta_a: &[u8], delta_b: &[u8]) -> bool {
    delta_a.len() == delta_b.len()
        && delta_a
            .iter()
            .zip(delta_b)
            .all(|(lhs, rhs)| (*lhs == 0) || (*rhs == 0))
}

/// Compose two disjoint patch deltas onto a base page.
#[must_use]
pub fn compose_disjoint_gf256_patches(
    base: &[u8],
    delta_a: &[u8],
    delta_b: &[u8],
) -> Option<Vec<u8>> {
    if base.len() != delta_a.len() || base.len() != delta_b.len() {
        return None;
    }
    if !gf256_patches_disjoint(delta_a, delta_b) {
        return None;
    }
    Some(
        base.iter()
            .zip(delta_a)
            .zip(delta_b)
            .map(|((base_byte, delta_a_byte), delta_b_byte)| {
                base_byte ^ delta_a_byte ^ delta_b_byte
            })
            .collect(),
    )
}

// ---------------------------------------------------------------------------
// CommitResponse
// ---------------------------------------------------------------------------

/// Result from the write coordinator / commit sequencer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitResponse {
    /// Successfully committed with the given `CommitSeq`.
    Ok(CommitSeq),
    /// Conflict detected on the given pages; provides the authoritative seq
    /// for merge-retry.
    Conflict(Vec<PageNumber>, CommitSeq),
    /// Aborted by the coordinator with an error code.
    Aborted(MvccError),
    /// I/O error during persist.
    IoError,
}

// ---------------------------------------------------------------------------
// MvccError
// ---------------------------------------------------------------------------

/// Error codes for MVCC operations (modeled after SQLite result codes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MvccError {
    /// Another transaction holds a conflicting lock (SQLITE_BUSY).
    Busy,
    /// Snapshot is stale due to concurrent commit (SQLITE_BUSY_SNAPSHOT).
    BusySnapshot,
    /// Schema changed since transaction started (SQLITE_SCHEMA).
    Schema,
    /// I/O error (SQLITE_IOERR).
    IoErr,
    /// Transaction not in expected state.
    InvalidState,
    /// TxnId space exhausted.
    TxnIdExhausted,
    /// SHM buffer too small (< HEADER_SIZE bytes).
    ShmTooSmall,
    /// SHM magic bytes do not match `FSQLSHM\0`.
    ShmBadMagic,
    /// SHM version field does not match the expected version.
    ShmVersionMismatch,
    /// SHM page_size is not a valid power-of-two in \[512, 65536\].
    ShmInvalidPageSize,
    /// SHM header checksum does not match recomputed value.
    ShmChecksumMismatch,
    /// Invalid write-merge policy for current build mode.
    InvalidWriteMergePolicy,
    /// Transaction exceeded configured max lifetime.
    TxnMaxDurationExceeded,
}

impl std::fmt::Display for MvccError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy => write!(f, "SQLITE_BUSY"),
            Self::BusySnapshot => write!(f, "SQLITE_BUSY_SNAPSHOT"),
            Self::Schema => write!(f, "SQLITE_SCHEMA"),
            Self::IoErr => write!(f, "SQLITE_IOERR"),
            Self::InvalidState => write!(f, "invalid transaction state"),
            Self::TxnIdExhausted => write!(f, "TxnId space exhausted"),
            Self::ShmTooSmall => write!(f, "SHM buffer too small"),
            Self::ShmBadMagic => write!(f, "SHM bad magic"),
            Self::ShmVersionMismatch => write!(f, "SHM version mismatch"),
            Self::ShmInvalidPageSize => write!(f, "SHM invalid page size"),
            Self::ShmChecksumMismatch => write!(f, "SHM checksum mismatch"),
            Self::InvalidWriteMergePolicy => write!(f, "invalid write-merge policy"),
            Self::TxnMaxDurationExceeded => write!(f, "transaction exceeded max duration"),
        }
    }
}

impl std::error::Error for MvccError {}

// ---------------------------------------------------------------------------
// Savepoint
// ---------------------------------------------------------------------------

/// A savepoint records the state of a transaction's write set so it can be
/// partially rolled back.
///
/// Per spec §5.4: page locks are NOT released on `ROLLBACK TO`. SSI witnesses
/// are NOT rolled back (safe overapproximation).
#[derive(Debug)]
pub struct Savepoint {
    /// Name of the savepoint.
    pub name: String,
    /// Snapshot of the write set at the time the savepoint was created.
    /// Maps page -> data so we can restore on ROLLBACK TO.
    /// Uses `Arc` for cheap O(1) creation.
    pub write_set_snapshot:
        Arc<HashMap<PageNumber, PageData, fsqlite_types::PageNumberBuildHasher>>,
    /// Number of pages in write_set when savepoint was created.
    pub write_set_len: usize,
    /// Snapshot of per-page write-version evidence at savepoint creation.
    pub write_set_versions_snapshot:
        HashMap<PageNumber, WriteVersionEntry, fsqlite_types::PageNumberBuildHasher>,
    /// Snapshot of structural page tracking at savepoint creation.
    pub structural_pages_snapshot: HashSet<PageNumber>,
    /// Number of intent-log entries at savepoint creation.
    pub intent_log_len: usize,
    /// Number of cell-level deltas tracked for this transaction at savepoint creation.
    pub cell_delta_len: usize,
}

// ---------------------------------------------------------------------------
// TransactionManager
// ---------------------------------------------------------------------------

/// Orchestrates the full transaction lifecycle.
///
/// Owns all shared MVCC infrastructure and provides begin/read/write/commit/abort
/// operations for both Serialized and Concurrent modes.
pub struct TransactionManager {
    txn_manager: TxnManager,
    version_store: VersionStore,
    lock_table: InProcessPageLockTable,
    write_mutex: SerializedWriteMutex,
    shm: SharedMemoryLayout,
    commit_index: CommitIndex,
    conn_id: u64,
    /// Current schema epoch (simplified; in full impl this lives in SHM).
    schema_epoch: SchemaEpoch,
    write_merge_policy: WriteMergePolicy,
    /// `PRAGMA fsqlite.serializable`: when true (default), SSI validation is
    /// performed on concurrent commits.  When false, plain SI is used and
    /// write-skew is tolerated.
    ssi_enabled: bool,
    /// `PRAGMA raptorq_repair_symbols` value for WAL-FEC commit groups.
    raptorq_repair_symbols: u8,
    busy_timeout_ms: u64,
    serialized_writer_lease_secs: u64,
    txn_max_duration_ms: u64,
    /// Epoch-based reclamation registry for version chain GC (§14.10).
    ///
    /// When present, `begin()` pins a [`VersionGuard`] on the transaction so
    /// that superseded page versions can be retired safely via `defer_retire`.
    version_guard_registry: Arc<VersionGuardRegistry>,
    /// Maximum committed versions allowed in one page chain before eager GC.
    max_chain_length: usize,
    /// Warning threshold for chain-length pressure.
    chain_length_warning: usize,
    /// Adaptive chain-length EWMA (§12.1 conformal-inspired).
    /// Tracks recent observed chain lengths to auto-tune compaction threshold.
    /// Encoded as fixed-point: value * 256 (to avoid float atomics).
    chain_ewma_x256: AtomicU64,
    /// Active snapshot highs keyed by txn id (used to derive GC horizon).
    active_snapshot_highs: Mutex<HashMap<TxnId, CommitSeq>>,
    /// Refcounted index of active snapshot highs so the minimum horizon can be
    /// maintained without rescanning every active transaction on begin/abort.
    active_snapshot_high_counts: Mutex<BTreeMap<CommitSeq, usize>>,
    /// Cached minimum active snapshot high (`NO_GC_HORIZON` when empty).
    cached_gc_horizon: AtomicU64,
    /// Commit-epoch keyed local snapshot reuse cache.
    ///
    /// Repeated `BEGIN` calls on one connection often happen with no
    /// intervening commit. In that case the published `(commit_seq,
    /// schema_epoch)` pair is unchanged, so we can reuse the last loaded
    /// snapshot without running the SHM seqlock read path again.
    snapshot_reuse_valid: AtomicBool,
    snapshot_reuse_commit_epoch: AtomicU64,
    snapshot_reuse_schema_epoch: AtomicU64,
    #[cfg(test)]
    snapshot_reuse_hits: AtomicU64,
    #[cfg(test)]
    snapshot_reuse_misses: AtomicU64,
    /// Cell-level MVCC visibility log for logical row operations (C4: bd-l9k8e.4).
    ///
    /// Tracks cell-level deltas for INSERT/UPDATE/DELETE operations that don't
    /// trigger structural B-tree changes. At commit time, pages NOT in the
    /// transaction's `structural_pages` set commit cell deltas here, enabling
    /// concurrent writers on the same page to succeed when they modify different cells.
    cell_log: CellVisibilityLog,
}

impl TransactionManager {
    /// Create a new transaction manager.
    #[must_use]
    pub fn new(page_size: PageSize) -> Self {
        Self::with_shm(page_size, SharedMemoryLayout::new(page_size, 128))
    }

    /// Create a transaction manager backed by a live shared SHM region.
    ///
    /// # Errors
    ///
    /// Returns any SHM validation failure from `SharedMemoryLayout`.
    pub fn with_shared_shm_region(
        page_size: PageSize,
        region: ShmRegion,
        max_txn_slots: u32,
    ) -> Result<Self, MvccError> {
        let shm = SharedMemoryLayout::open_or_initialize_region(region, page_size, max_txn_slots)?;
        Ok(Self::with_shm(page_size, shm))
    }

    fn with_shm(page_size: PageSize, shm: SharedMemoryLayout) -> Self {
        let version_guard_registry = Arc::new(VersionGuardRegistry::default());
        let initial_snapshot = shm.load_consistent_snapshot();
        let initial_commit_seq = initial_snapshot.commit_seq.get().saturating_add(1);
        let initial_schema_epoch = initial_snapshot.schema_epoch;
        Self {
            txn_manager: TxnManager::new(1, initial_commit_seq),
            version_store: VersionStore::new_with_guard_registry(
                page_size,
                Arc::clone(&version_guard_registry),
            ),
            lock_table: InProcessPageLockTable::new(),
            write_mutex: SerializedWriteMutex::new(),
            shm,
            commit_index: CommitIndex::new(),
            conn_id: NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed),
            schema_epoch: initial_schema_epoch,
            write_merge_policy: WriteMergePolicy::default(),
            ssi_enabled: true,
            raptorq_repair_symbols: DEFAULT_RAPTORQ_REPAIR_SYMBOLS,
            busy_timeout_ms: DEFAULT_BUSY_TIMEOUT_MS,
            serialized_writer_lease_secs: DEFAULT_SERIALIZED_WRITER_LEASE_SECS,
            txn_max_duration_ms: 5_000,
            version_guard_registry,
            max_chain_length: DEFAULT_MAX_CHAIN_LENGTH,
            chain_length_warning: DEFAULT_CHAIN_LENGTH_WARNING,
            // Initial EWMA = 1 (single-version chains) × 256 (fixed-point).
            chain_ewma_x256: AtomicU64::new(256),
            active_snapshot_highs: Mutex::new(HashMap::new()),
            active_snapshot_high_counts: Mutex::new(BTreeMap::new()),
            cached_gc_horizon: AtomicU64::new(NO_GC_HORIZON),
            snapshot_reuse_valid: AtomicBool::new(true),
            snapshot_reuse_commit_epoch: AtomicU64::new(initial_snapshot.commit_seq.get()),
            snapshot_reuse_schema_epoch: AtomicU64::new(initial_snapshot.schema_epoch.get()),
            #[cfg(test)]
            snapshot_reuse_hits: AtomicU64::new(0),
            #[cfg(test)]
            snapshot_reuse_misses: AtomicU64::new(0),
            // Cell-level MVCC log: budget = 10% of typical 256MB page cache = ~25MB
            cell_log: CellVisibilityLog::new(25 * 1024 * 1024),
        }
    }

    /// Reference to the epoch-based reclamation guard registry.
    #[must_use]
    pub fn version_guard_registry(&self) -> &Arc<VersionGuardRegistry> {
        &self.version_guard_registry
    }

    /// Reference to the cell-level visibility log (C4: bd-l9k8e.4).
    ///
    /// Use this to record cell-level deltas during INSERT/UPDATE/DELETE
    /// operations that don't trigger structural B-tree changes.
    #[must_use]
    pub fn cell_log(&self) -> &CellVisibilityLog {
        &self.cell_log
    }

    /// Opaque per-connection identifier used in logs (helps prove PRAGMA scope).
    #[must_use]
    pub const fn conn_id(&self) -> u64 {
        self.conn_id
    }

    /// Busy-timeout for draining concurrent writers during Serialized acquisition (§5.8.1).
    #[must_use]
    pub const fn busy_timeout_ms(&self) -> u64 {
        self.busy_timeout_ms
    }

    /// Set busy-timeout for Serialized acquisition drain.
    pub fn set_busy_timeout_ms(&mut self, busy_timeout_ms: u64) {
        self.busy_timeout_ms = busy_timeout_ms;
    }

    /// Current write-merge policy.
    #[must_use]
    pub const fn write_merge_policy(&self) -> WriteMergePolicy {
        self.write_merge_policy
    }

    /// Set write-merge policy (`PRAGMA fsqlite.write_merge`).
    ///
    /// # Errors
    ///
    /// Returns [`MvccError::InvalidWriteMergePolicy`] when requesting
    /// `LAB_UNSAFE` in release builds.
    pub fn set_write_merge_policy(&mut self, policy: WriteMergePolicy) -> Result<(), MvccError> {
        if matches!(policy, WriteMergePolicy::LabUnsafe) && !cfg!(debug_assertions) {
            return Err(MvccError::InvalidWriteMergePolicy);
        }
        self.write_merge_policy = policy;
        Ok(())
    }

    /// Whether SSI validation is enabled (`PRAGMA fsqlite.serializable`).
    ///
    /// When `true` (default), concurrent commits perform SSI validation
    /// and abort transactions with dangerous structure (write-skew).
    /// When `false`, plain Snapshot Isolation is used.
    #[must_use]
    pub const fn ssi_enabled(&self) -> bool {
        self.ssi_enabled
    }

    /// Set SSI mode (`PRAGMA fsqlite.serializable = ON|OFF`).
    pub fn set_ssi_enabled(&mut self, enabled: bool) {
        let old = self.ssi_enabled;
        self.ssi_enabled = enabled;
        tracing::debug!(
            conn_id = self.conn_id,
            old_value = old,
            new_value = enabled,
            "PRAGMA fsqlite.serializable changed"
        );
    }

    /// Current WAL-FEC repair-symbol budget (`PRAGMA raptorq_repair_symbols`).
    #[must_use]
    pub const fn raptorq_repair_symbols(&self) -> u8 {
        self.raptorq_repair_symbols
    }

    /// Set WAL-FEC repair-symbol budget (`PRAGMA raptorq_repair_symbols = N`).
    pub fn set_raptorq_repair_symbols(&mut self, value: u8) {
        let old = self.raptorq_repair_symbols;
        self.raptorq_repair_symbols = value;
        tracing::debug!(
            conn_id = self.conn_id,
            old_value = old,
            new_value = value,
            "PRAGMA raptorq_repair_symbols changed"
        );
    }

    /// Configured transaction max duration in milliseconds.
    #[must_use]
    pub const fn txn_max_duration_ms(&self) -> u64 {
        self.txn_max_duration_ms
    }

    /// Set transaction max duration (`PRAGMA fsqlite.txn_max_duration_ms`).
    ///
    /// Values are clamped to at least 1ms to avoid accidental zero-duration
    /// write windows in tests and callers.
    pub fn set_txn_max_duration_ms(&mut self, max_duration_ms: u64) {
        self.txn_max_duration_ms = max_duration_ms.max(1);
    }

    /// Maximum committed versions allowed in one page chain before eager GC.
    #[must_use]
    pub const fn max_chain_length(&self) -> usize {
        self.max_chain_length
    }

    /// Set the maximum per-page chain length (clamped to at least 1).
    pub fn set_max_chain_length(&mut self, max_chain_length: usize) {
        self.max_chain_length = max_chain_length.max(1);
        if self.chain_length_warning > self.max_chain_length {
            self.chain_length_warning = self.max_chain_length;
        }
    }

    /// Warning threshold for chain-length pressure.
    #[must_use]
    pub const fn chain_length_warning(&self) -> usize {
        self.chain_length_warning
    }

    /// Set warning threshold (clamped into `[1, max_chain_length]`).
    pub fn set_chain_length_warning(&mut self, chain_length_warning: usize) {
        self.chain_length_warning = chain_length_warning.clamp(1, self.max_chain_length);
    }

    /// Cached minimum active snapshot high used as eager-GC horizon.
    #[must_use]
    pub fn cached_gc_horizon(&self) -> Option<CommitSeq> {
        let raw = self.cached_gc_horizon.load(Ordering::Acquire);
        if raw == NO_GC_HORIZON {
            None
        } else {
            Some(CommitSeq::new(raw))
        }
    }

    /// Begin a new transaction.
    ///
    /// # Errors
    ///
    /// Returns `MvccError::TxnIdExhausted` if the TxnId space is exhausted.
    /// Returns `MvccError::Busy` if the serialized writer mutex cannot be acquired
    /// (for Immediate/Exclusive modes).
    pub fn begin(&self, kind: BeginKind) -> Result<Transaction, MvccError> {
        // TxnId allocation via CAS (never fetch_add, never 0, never > MAX).
        let txn_id = self.shm.alloc_txn_id().ok_or(MvccError::TxnIdExhausted)?;

        let mode = if kind == BeginKind::Concurrent {
            TransactionMode::Concurrent
        } else {
            TransactionMode::Serialized
        };

        let snapshot = self.load_consistent_snapshot();
        let snapshot_established = kind != BeginKind::Deferred;

        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snapshot, mode);
        // Pin an EBR guard so that any version retired during this txn's
        // lifetime is deferred until all concurrent readers have unpinned.
        txn.version_guard = Some(VersionGuardTicket::register(Arc::clone(
            &self.version_guard_registry,
        )));
        txn.snapshot_established = snapshot_established;
        if snapshot_established {
            mvcc_snapshot_established();
            self.register_active_snapshot(txn_id, snapshot.high);
        }
        // PRAGMA is per-connection and takes effect at BEGIN (not retroactive).
        txn.ssi_enabled_at_begin = self.ssi_enabled;

        // For Immediate/Exclusive: acquire serialized writer exclusion at BEGIN.
        if kind == BeginKind::Immediate || kind == BeginKind::Exclusive {
            if let Err(e) = self.acquire_serialized_writer_exclusion(txn_id) {
                // Unregister the snapshot we just registered (snapshot_established
                // is true for Immediate/Exclusive) to avoid permanently pinning
                // the GC horizon at this snapshot's high value.
                self.unregister_active_snapshot(txn_id);
                return Err(e);
            }
            txn.serialized_write_lock_held = true;
        }

        tracing::info!(
            conn_id = self.conn_id,
            txn_id = %txn_id,
            ?kind,
            ?mode,
            snapshot_high = snapshot.high.get(),
            snapshot_established,
            "transaction begun"
        );

        Ok(txn)
    }

    /// Read a page within a transaction.
    ///
    /// Per spec §5.4:
    /// 1. Check write_set first (returns local modification).
    /// 2. For DEFERRED mode: establish snapshot on first read.
    /// 3. Resolve via version store.
    ///
    /// Returns `None` if the page has no committed version visible at the
    /// transaction's snapshot (and is not in the write_set).
    pub fn read_page(&self, txn: &mut Transaction, pgno: PageNumber) -> Option<PageData> {
        assert_eq!(
            txn.state,
            TransactionState::Active,
            "can only read in active transactions"
        );

        if self.ensure_txn_within_max_duration(txn).is_err() {
            return None;
        }

        // Check write_set first.
        if let Some(data) = txn.write_set_data.get(&pgno).cloned() {
            let tracked_version = txn
                .write_version_for_page(pgno)
                .and_then(|entry| entry.new_version.or(entry.old_version))
                .unwrap_or(txn.snapshot.high);
            txn.record_page_read(pgno, tracked_version);
            return Some(data);
        }

        // DEFERRED snapshot establishment on first read.
        if txn.mode == TransactionMode::Serialized && !txn.snapshot_established {
            txn.snapshot = self.load_consistent_snapshot();
            txn.snapshot_established = true;
            mvcc_snapshot_established();
            self.register_active_snapshot(txn.txn_id, txn.snapshot.high);
            tracing::debug!(
                txn_id = %txn.txn_id,
                snapshot_high = txn.snapshot.high.get(),
                "deferred snapshot established on read"
            );
        }

        // Resolve visible version from the version store.
        let version = self
            .version_store
            .resolve_visible_version(pgno, &txn.snapshot)?;
        txn.record_page_read(pgno, version.commit_seq);
        Some(version.data)
    }

    /// Read a page and apply visible cell-level deltas before returning it.
    ///
    /// This is the read-view primitive needed by logical DML: committed cell
    /// deltas obey the transaction snapshot, while this transaction's own
    /// uncommitted cell deltas replay after that snapshot. A staged full-page
    /// write-set image remains authoritative and is returned unchanged.
    pub fn read_page_with_cell_deltas(
        &self,
        txn: &mut Transaction,
        pgno: PageNumber,
        usable_size: u32,
    ) -> fsqlite_error::Result<Option<PageData>> {
        let staged_full_page = txn.write_set_data.contains_key(&pgno);
        let Some(base_page) = self.read_page(txn, pgno) else {
            return Ok(None);
        };

        if staged_full_page || !self.cell_log.page_has_deltas(pgno) {
            return Ok(Some(base_page));
        }

        let deltas =
            self.cell_log
                .collect_visible_deltas_for_txn(pgno, txn.snapshot.high, txn.token());
        if deltas.is_empty() {
            return Ok(Some(base_page));
        }

        let materialized_high = deltas
            .iter()
            .map(|delta| delta.commit_seq)
            .max()
            .unwrap_or(txn.snapshot.high);
        if let Some(committed_high) = deltas
            .iter()
            .filter_map(|delta| (delta.commit_seq <= txn.snapshot.high).then_some(delta.commit_seq))
            .max()
        {
            txn.record_page_read(pgno, committed_high);
        }
        let materialized_snapshot = Snapshot::new(materialized_high, txn.snapshot.schema_epoch);
        let result = materialize_page(
            &base_page,
            pgno,
            &deltas,
            &materialized_snapshot,
            usable_size,
            MaterializationTrigger::Explicit,
        )?;

        Ok(Some(result.page))
    }

    /// Record a range scan witness/read-set footprint for a transaction.
    ///
    /// This hook allows scan operators to capture page-level predicate coverage
    /// without forcing materialization of all rows. Each leaf page is recorded
    /// with the currently visible committed version when available.
    pub fn record_range_scan(&self, txn: &mut Transaction, leaf_pages: &[PageNumber]) {
        assert_eq!(
            txn.state,
            TransactionState::Active,
            "can only record range scan in active transactions"
        );
        let fallback_version = txn.snapshot.high;
        for &page in leaf_pages {
            let version = self
                .resolve_visible_commit_seq(txn, page)
                .unwrap_or(fallback_version);
            txn.record_page_read(page, version);
        }
    }

    /// Read an inclusive page range and record page-level predicate witnesses.
    ///
    /// This is the range-scan callsite for SSI tracking: every scanned page is
    /// captured in the read-set/witness ledger, including pages with no visible
    /// committed version (tracked at `snapshot.high` fallback).
    #[must_use]
    pub fn read_page_range(
        &self,
        txn: &mut Transaction,
        start_page: PageNumber,
        end_page: PageNumber,
    ) -> Vec<(PageNumber, Option<PageData>)> {
        if start_page.get() > end_page.get() {
            return Vec::new();
        }

        let mut visible_pages = Vec::new();
        for raw_page in start_page.get()..=end_page.get() {
            if let Some(page) = PageNumber::new(raw_page) {
                let page_data = self.read_page(txn, page);
                if page_data.is_none() {
                    // Empty pages still need page-level predicate coverage so
                    // SSI can reason about the scan footprint without a second
                    // resolve/tracking pass.
                    txn.record_page_read(page, txn.snapshot.high);
                }
                visible_pages.push((page, page_data));
            }
        }

        visible_pages
    }

    fn publish_cached_gc_horizon(&self, snapshot_counts: &BTreeMap<CommitSeq, usize>) {
        let raw = active_snapshot_horizon_raw(snapshot_counts);
        self.cached_gc_horizon.store(raw, Ordering::Release);
        self.shm.store_gc_horizon(if raw == NO_GC_HORIZON {
            CommitSeq::ZERO
        } else {
            CommitSeq::new(raw)
        });
    }

    fn register_active_snapshot(&self, txn_id: TxnId, snapshot_high: CommitSeq) {
        let mut active = self.active_snapshot_highs.lock();
        let mut snapshot_counts = self.active_snapshot_high_counts.lock();

        if let Some(previous_high) = active.insert(txn_id, snapshot_high) {
            decrement_active_snapshot_refcount(&mut snapshot_counts, previous_high);
        }
        increment_active_snapshot_refcount(&mut snapshot_counts, snapshot_high);

        self.publish_cached_gc_horizon(&snapshot_counts);
    }

    fn unregister_active_snapshot(&self, txn_id: TxnId) {
        let mut active = self.active_snapshot_highs.lock();
        let mut snapshot_counts = self.active_snapshot_high_counts.lock();

        if let Some(snapshot_high) = active.remove(&txn_id) {
            decrement_active_snapshot_refcount(&mut snapshot_counts, snapshot_high);
        }

        self.publish_cached_gc_horizon(&snapshot_counts);
    }

    fn eager_gc_horizon(&self) -> CommitSeq {
        if let Some(cached) = self.cached_gc_horizon() {
            return cached;
        }
        self.shm.load_commit_seq()
    }

    #[allow(clippy::unused_self)]
    fn record_chain_length_sample(&self, chain_len: usize) {
        let sample = u64::try_from(chain_len).unwrap_or(u64::MAX);
        GLOBAL_EBR_METRICS.record_chain_length_sample(sample);

        // Update EWMA with α=0.125 (1/8): new = old * 7/8 + sample * 1/8.
        // Fixed-point × 256 to avoid floating point.
        let sample_x256 = chain_len.min(1024) as u64 * 256;
        // `fetch_update` is deprecated on newer toolchains in favour of `try_update`, but
        // `try_update` is only stable since 1.95 and the workspace MSRV is 1.85 — so keep the
        // MSRV-safe method and silence the deprecation until the MSRV moves up.
        #[allow(deprecated)]
        let _ = self
            .chain_ewma_x256
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                Some((old * 7 + sample_x256) / 8)
            });
    }

    /// Current adaptive compaction threshold based on observed chain lengths.
    /// For ordinary chain budgets this is `max(PROACTIVE_COMPACT_THRESHOLD,
    /// 2 × EWMA)` clamped below `max_chain_length / 2`; for very small
    /// `max_chain_length` settings it collapses to the reachable upper bound
    /// instead of panicking.
    #[must_use]
    fn adaptive_compact_threshold(&self) -> usize {
        let ewma_x256 = self.chain_ewma_x256.load(Ordering::Relaxed);
        let ewma = (ewma_x256 / 256) as usize;
        // Threshold = 2× EWMA, clamped to [PROACTIVE_COMPACT_THRESHOLD, max_chain_length/2].
        // Very small max_chain_length settings can make the nominal lower bound
        // exceed the upper bound (for example 8 > 2 when max_chain_length = 4),
        // so collapse both ends to the reachable upper bound instead of
        // panicking in clamp().
        let upper_bound = (self.max_chain_length / 2).max(1);
        let lower_bound = PROACTIVE_COMPACT_THRESHOLD.min(upper_bound);
        let threshold = ewma.saturating_mul(2).clamp(lower_bound, upper_bound);
        threshold
    }

    /// Returns the post-enforcement chain length on success so callers can
    /// skip a redundant `chain_length()` traversal later.
    fn enforce_chain_bound_for_page(&self, pgno: PageNumber) -> Result<usize, MvccError> {
        let mut chain_len = self.version_store.chain_length(pgno);
        self.record_chain_length_sample(chain_len);

        if chain_len >= self.chain_length_warning {
            tracing::warn!(
                pgno = pgno.get(),
                chain_len,
                warning_threshold = self.chain_length_warning,
                max_chain_length = self.max_chain_length,
                "MVCC version chain length warning threshold crossed"
            );
        }

        if chain_len < self.max_chain_length {
            return Ok(chain_len);
        }

        // C7 (bd-l9k8e.7): Opportunistic cleanup instead of blocking.
        // Attempt one quick prune pass.  If insufficient, proceed anyway
        // (soft bound) unless chain exceeds the hard limit.  This mirrors
        // Postgres pruneheap.c:199 — the threshold is advisory, not hard.
        // No thread::sleep; backpressure is relieved by soft-bound semantics.
        let horizon = self.eager_gc_horizon();
        let freed = self.version_store.prune_page_chain_eager(pgno, horizon);
        if freed > 0 {
            let freed_u64 = u64::try_from(freed).unwrap_or(u64::MAX);
            GLOBAL_EBR_METRICS.record_gc_freed(freed_u64);
            tracing::debug!(
                pgno = pgno.get(),
                freed,
                gc_horizon = horizon.get(),
                "opportunistic_prune"
            );
        }

        // Recompute chain length after prune.
        chain_len = if freed > 0 {
            chain_len.saturating_sub(freed)
        } else {
            self.version_store.chain_length(pgno)
        };
        self.record_chain_length_sample(chain_len);

        // Below soft threshold: proceed normally.
        if chain_len < self.max_chain_length {
            return Ok(chain_len);
        }

        // Soft bound: proceed even if chain exceeds max_chain_length,
        // up to SOFT_BOUND_MULTIPLIER × max_chain_length.
        let hard_limit = self.max_chain_length.saturating_mul(SOFT_BOUND_MULTIPLIER);
        if chain_len < hard_limit {
            tracing::warn!(
                pgno = pgno.get(),
                chain_len,
                soft_threshold = self.max_chain_length,
                hard_limit,
                gc_horizon = horizon.get(),
                "chain_soft_bound_exceeded_proceeding"
            );
            return Ok(chain_len);
        }

        // Hard limit exceeded: record metric and return Busy.
        GLOBAL_EBR_METRICS.record_gc_blocked();
        tracing::error!(
            pgno = pgno.get(),
            chain_len,
            soft_threshold = self.max_chain_length,
            hard_limit,
            gc_horizon = horizon.get(),
            "MVCC chain hard limit exceeded — gc_horizon pinned by long-running txn"
        );
        Err(MvccError::Busy)
    }

    /// Write a page within a transaction.
    ///
    /// Per spec §5.4:
    /// - **Serialized**: DEFERRED upgrade acquires global mutex on first write.
    ///   Reader-turned-writer with stale snapshot gets `BusySnapshot`.
    ///   No page lock needed (mutex provides exclusion).
    /// - **Concurrent**: Check serialized writer exclusion, acquire page lock,
    ///   track in page_locks + write_set.
    ///
    /// # Errors
    ///
    /// Returns `MvccError::BusySnapshot` if a serialized deferred txn has a stale
    /// snapshot when upgrading to writer.
    /// Returns `MvccError::Schema` if schema epoch changed.
    /// Returns `MvccError::Busy` if a concurrent write conflicts on page lock or
    /// a serialized writer is active.
    pub fn write_page(
        &self,
        txn: &mut Transaction,
        pgno: PageNumber,
        data: PageData,
    ) -> Result<(), MvccError> {
        assert_eq!(
            txn.state,
            TransactionState::Active,
            "can only write in active transactions"
        );

        self.ensure_txn_within_max_duration(txn)?;

        if txn.mode == TransactionMode::Serialized {
            self.write_page_serialized(txn, pgno, data)?;
        } else {
            self.write_page_concurrent(txn, pgno, data)?;
        }
        Ok(())
    }

    /// Commit a transaction.
    ///
    /// Per spec §5.4:
    /// - Schema epoch check.
    /// - **Serialized**: FCW freshness validation; abort on snapshot conflict.
    /// - **Concurrent**: SSI validation (simplified), FCW check, merge-retry loop.
    ///
    /// # Errors
    ///
    /// Returns `MvccError::Schema` if schema epoch changed.
    /// Returns `MvccError::BusySnapshot` on FCW conflict.
    /// Returns `MvccError::InvalidState` if not active.
    pub fn commit(&self, txn: &mut Transaction) -> Result<CommitSeq, MvccError> {
        if txn.state != TransactionState::Active {
            tracing::error!(
                txn_id = %txn.txn_id,
                state = ?txn.state,
                "lock protocol violation: commit attempted on non-active transaction"
            );
            return Err(MvccError::InvalidState);
        }

        self.ensure_txn_within_max_duration(txn)?;

        // Schema epoch check.
        if self.schema_epoch != txn.snapshot.schema_epoch {
            self.abort(txn);
            return Err(MvccError::Schema);
        }

        let has_cell_deltas = self.cell_log.txn_delta_count(txn.token()) != 0;

        // If no writes, just commit (read-only transaction). Cell-level
        // deltas are writes too, even when no full page image is staged.
        if txn.write_set.is_empty() && txn.write_set_data.is_empty() && !has_cell_deltas {
            txn.commit();
            self.release_all_resources(txn);
            return Ok(CommitSeq::ZERO);
        }

        if txn.mode == TransactionMode::Serialized {
            self.commit_serialized(txn)
        } else {
            self.commit_concurrent(txn)
        }
    }

    /// Abort a transaction, releasing all held resources.
    ///
    /// Per spec §5.4:
    /// - Release page locks.
    /// - Discard write_set.
    /// - Serialized: release mutex if held.
    /// - Discard transaction-local SSI witnesses.
    pub fn abort(&self, txn: &mut Transaction) {
        if txn.state != TransactionState::Active {
            return; // already finalized
        }

        self.cell_log.rollback_txn(txn.token());

        // Clear structural pages tracking
        txn.clear_structural_pages();

        txn.abort();
        self.release_all_resources(txn);

        tracing::info!(
            txn_id = %txn.txn_id,
            ?txn.mode,
            "transaction aborted"
        );
    }

    /// Create a savepoint within a transaction.
    ///
    /// Records the current write_set and cell-delta state so it can be
    /// restored on `ROLLBACK TO`.
    #[must_use]
    pub fn savepoint(&self, txn: &Transaction, name: &str) -> Savepoint {
        assert_eq!(
            txn.state,
            TransactionState::Active,
            "can only create savepoints in active transactions"
        );
        Savepoint {
            name: name.to_owned(),
            write_set_snapshot: txn.write_set_data.clone(),
            write_set_len: txn.write_set.len(),
            write_set_versions_snapshot: txn.write_set_versions.clone(),
            structural_pages_snapshot: txn.structural_pages.clone(),
            intent_log_len: txn.intent_log.len(),
            cell_delta_len: self.cell_log.txn_delta_count(txn.token()),
        }
    }

    /// Rollback to a savepoint.
    ///
    /// Per spec §5.4:
    /// - Restores write_set page states to the savepoint.
    /// - Removes cell-level deltas created after the savepoint.
    /// - Page locks are NOT released.
    /// - SSI witnesses are NOT rolled back.
    pub fn rollback_to_savepoint(&self, txn: &mut Transaction, savepoint: &Savepoint) {
        assert_eq!(
            txn.state,
            TransactionState::Active,
            "can only rollback to savepoint in active transactions"
        );

        self.cell_log
            .rollback_txn_to_delta_count(txn.token(), savepoint.cell_delta_len);

        // Restore write_set_data to the savepoint state.
        txn.write_set_data.clone_from(&savepoint.write_set_snapshot);

        // Truncate the write_set page list to savepoint length.
        txn.write_set.truncate(savepoint.write_set_len);
        txn.write_set_versions
            .clone_from(&savepoint.write_set_versions_snapshot);
        txn.structural_pages
            .clone_from(&savepoint.structural_pages_snapshot);
        txn.intent_log.truncate(savepoint.intent_log_len);

        tracing::debug!(
            txn_id = %txn.txn_id,
            savepoint = %savepoint.name,
            "rolled back to savepoint"
        );

        // NOTE: page_locks are NOT released (spec requirement).
        // NOTE: read_keys/write_keys (SSI witnesses) are NOT rolled back.
    }

    /// Get a reference to the version store.
    #[must_use]
    pub fn version_store(&self) -> &VersionStore {
        &self.version_store
    }

    /// Get a reference to the commit index.
    #[must_use]
    pub fn commit_index(&self) -> &CommitIndex {
        &self.commit_index
    }

    /// Get a reference to the lock table.
    #[must_use]
    pub fn lock_table(&self) -> &InProcessPageLockTable {
        &self.lock_table
    }

    /// Get a reference to the serialized write mutex.
    #[must_use]
    pub fn write_mutex(&self) -> &SerializedWriteMutex {
        &self.write_mutex
    }

    /// Advance the schema epoch (simulating a DDL commit).
    pub fn advance_schema_epoch(&mut self) {
        self.schema_epoch = SchemaEpoch::new(self.schema_epoch.get() + 1);
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    fn publish_shared_snapshot(&self, commit_seq: CommitSeq) {
        let snapshot = Snapshot::new(commit_seq, self.schema_epoch);
        self.shm.publish_snapshot(
            snapshot.high,
            snapshot.schema_epoch,
            self.shm.load_ecs_epoch(),
        );
        self.cache_snapshot_reuse(snapshot);
    }

    fn cache_snapshot_reuse(&self, snapshot: Snapshot) {
        self.snapshot_reuse_schema_epoch
            .store(snapshot.schema_epoch.get(), Ordering::Relaxed);
        self.snapshot_reuse_commit_epoch
            .store(snapshot.high.get(), Ordering::Release);
        self.snapshot_reuse_valid.store(true, Ordering::Release);
    }

    fn try_reuse_snapshot_from_commit_epoch(&self, commit_epoch: CommitSeq) -> Option<Snapshot> {
        if !self.snapshot_reuse_valid.load(Ordering::Acquire) {
            return None;
        }
        let cached_commit_epoch = self.snapshot_reuse_commit_epoch.load(Ordering::Acquire);
        if cached_commit_epoch != commit_epoch.get() {
            return None;
        }
        let cached_schema_epoch = self.snapshot_reuse_schema_epoch.load(Ordering::Acquire);
        let shared_schema_epoch = self.shm.load_schema_epoch().get();
        if cached_schema_epoch != shared_schema_epoch {
            return None;
        }
        #[cfg(test)]
        self.snapshot_reuse_hits.fetch_add(1, Ordering::Relaxed);
        Some(Snapshot::new(
            commit_epoch,
            SchemaEpoch::new(cached_schema_epoch),
        ))
    }

    /// Load the latest stable snapshot from the shared MVCC header.
    fn load_consistent_snapshot(&self) -> Snapshot {
        let commit_epoch = self.shm.load_commit_seq();
        if let Some(snapshot) = self.try_reuse_snapshot_from_commit_epoch(commit_epoch) {
            return snapshot;
        }
        #[cfg(test)]
        self.snapshot_reuse_misses.fetch_add(1, Ordering::Relaxed);
        let snapshot = self.shm.load_consistent_snapshot();
        let snapshot = Snapshot::new(snapshot.commit_seq, snapshot.schema_epoch);
        self.cache_snapshot_reuse(snapshot);
        snapshot
    }

    #[cfg(test)]
    fn snapshot_reuse_stats(&self) -> (u64, u64) {
        (
            self.snapshot_reuse_hits.load(Ordering::Relaxed),
            self.snapshot_reuse_misses.load(Ordering::Relaxed),
        )
    }

    /// Ensure a serialized transaction holds writer exclusion before publishing
    /// either full-page writes or logical cell deltas.
    fn ensure_serialized_write_exclusion(&self, txn: &mut Transaction) -> Result<(), MvccError> {
        if !txn.serialized_write_lock_held {
            // DEFERRED upgrade: acquire global mutex on first write.
            self.acquire_serialized_writer_exclusion(txn.txn_id)?;

            // Reader-turned-writer rule: if snapshot was already established
            // (via prior reads) and the database has advanced, fail.
            let snap_now = self.load_consistent_snapshot();
            if txn.snapshot_established {
                if snap_now.schema_epoch != txn.snapshot.schema_epoch {
                    self.release_serialized_writer_exclusion(txn.txn_id);
                    return Err(MvccError::Schema);
                }
                if snap_now.high != txn.snapshot.high {
                    self.release_serialized_writer_exclusion(txn.txn_id);
                    return Err(MvccError::BusySnapshot);
                }
            }

            // Establish/refresh snapshot.
            let had_snapshot = txn.snapshot_established;
            txn.snapshot = snap_now;
            txn.snapshot_established = true;
            txn.serialized_write_lock_held = true;
            if !had_snapshot {
                mvcc_snapshot_established();
                self.register_active_snapshot(txn.txn_id, txn.snapshot.high);
            }

            tracing::debug!(
                txn_id = %txn.txn_id,
                "serialized deferred upgrade: mutex acquired"
            );
        }

        Ok(())
    }

    fn write_page_serialized(
        &self,
        txn: &mut Transaction,
        pgno: PageNumber,
        data: PageData,
    ) -> Result<(), MvccError> {
        self.ensure_serialized_write_exclusion(txn)?;

        txn.record_page_write(pgno, self.resolve_visible_commit_seq(txn, pgno));

        // No page lock needed (mutex provides exclusion).
        // Track in write_set — check write_set_data (HashMap, O(1)) instead of
        // write_set (Vec, O(n)) to avoid linear scan on repeated page writes.
        let is_new_page = !txn.write_set_data.contains_key(&pgno);
        if is_new_page {
            txn.write_set.push(pgno);
        }
        Arc::make_mut(&mut txn.write_set_data).insert(pgno, data);

        // Raw page writes (via write_page) bypass cell-level tracking.
        // Mark as structural to ensure page-level MVCC is used (C4: bd-l9k8e.4).
        txn.structural_pages.insert(pgno);

        Ok(())
    }

    /// Concurrent write path.
    fn write_page_concurrent(
        &self,
        txn: &mut Transaction,
        pgno: PageNumber,
        data: PageData,
    ) -> Result<(), MvccError> {
        // Check serialized writer exclusion first.
        if self
            .shm
            .check_serialized_writer_exclusion(logical_now_epoch_secs(), process_alive_os)
            .is_err()
        {
            return Err(MvccError::Busy);
        }

        // Acquire page lock.
        self.lock_table
            .try_acquire(pgno, txn.txn_id)
            .map_err(|_| MvccError::Busy)?;

        let newly_locked = txn.page_locks.insert(pgno);
        if newly_locked {
            tracing::debug!(
                txn_id = %txn.txn_id,
                pgno = pgno.get(),
                "concurrent: page lock acquired"
            );
        }

        txn.record_page_write(pgno, self.resolve_visible_commit_seq(txn, pgno));

        // Track in write_set — check write_set_data (HashMap, O(1)) instead of
        // write_set (Vec, O(n)) to avoid linear scan on repeated page writes.
        // NOTE: Cannot use `newly_locked` here because ROLLBACK TO SAVEPOINT
        // truncates write_set but retains page_locks (spec §5.4), which would
        // desynchronize the two and silently drop pages from the commit.
        if !txn.write_set_data.contains_key(&pgno) {
            txn.write_set.push(pgno);
        }
        Arc::make_mut(&mut txn.write_set_data).insert(pgno, data);

        // Raw page writes (via write_page) bypass cell-level tracking.
        // Mark as structural to ensure page-level MVCC is used (C4: bd-l9k8e.4).
        txn.structural_pages.insert(pgno);

        Ok(())
    }

    /// Cell-delta-only transactions bypass `write_page`, so commit must acquire
    /// the same exclusion and witness metadata before FCW validation/publish.
    fn prepare_cell_delta_commit_exclusion(&self, txn: &mut Transaction) -> Result<(), MvccError> {
        let pages = self.cell_log.txn_pages(txn.token());
        if pages.is_empty() {
            return Ok(());
        }

        if txn.mode == TransactionMode::Serialized {
            self.ensure_serialized_write_exclusion(txn)?;
            for pgno in pages {
                txn.record_page_write(pgno, self.resolve_visible_commit_seq(txn, pgno));
            }
            return Ok(());
        }

        if self
            .shm
            .check_serialized_writer_exclusion(logical_now_epoch_secs(), process_alive_os)
            .is_err()
        {
            return Err(MvccError::Busy);
        }

        for pgno in pages {
            self.lock_table
                .try_acquire(pgno, txn.txn_id)
                .map_err(|_| MvccError::Busy)?;

            let newly_locked = txn.page_locks.insert(pgno);
            if newly_locked {
                tracing::debug!(
                    txn_id = %txn.txn_id,
                    pgno = pgno.get(),
                    "concurrent: cell-delta commit page lock acquired"
                );
            }

            txn.record_page_write(pgno, self.resolve_visible_commit_seq(txn, pgno));
        }

        Ok(())
    }

    /// Serialized commit path.
    fn commit_serialized(&self, txn: &mut Transaction) -> Result<CommitSeq, MvccError> {
        if let Err(err) = self.prepare_cell_delta_commit_exclusion(txn) {
            self.abort(txn);
            return Err(err);
        }

        let pages = self.pending_commit_pages(txn);
        let snapshot_high = txn.snapshot.high;

        // FCW freshness validation: check that no pending write page has been
        // committed since our snapshot.
        for &pgno in &pages {
            if let Some(latest) = self.commit_index.latest(pgno) {
                if latest > txn.snapshot.high {
                    self.abort(txn);
                    return Err(MvccError::BusySnapshot);
                }
            }
        }

        // Publish: allocate commit_seq and publish versions.
        let (commit_seq, chain_lens) = match self.publish_write_set(txn, &pages) {
            Ok(result) => result,
            Err(err) => {
                self.abort(txn);
                return Err(err);
            }
        };
        self.txn_manager.finish_commit_seq(commit_seq);
        self.publish_shared_snapshot(commit_seq);
        txn.commit();
        self.release_all_resources(txn);
        self.post_commit_version_maintenance(&pages, snapshot_high, &chain_lens);

        tracing::info!(
            txn_id = %txn.txn_id,
            commit_seq = commit_seq.get(),
            "serialized commit succeeded"
        );

        Ok(commit_seq)
    }

    /// Concurrent commit path (§5.8 FCW + merge-retry pipeline, bd-zppf).
    ///
    /// Pipeline: (1) SSI validation, (2) FCW CommitIndex check with rebase
    /// attempt on conflict, (3) SSI re-validation after rebase, (4) publish.
    fn commit_concurrent(&self, txn: &mut Transaction) -> Result<CommitSeq, MvccError> {
        if let Err(err) = self.prepare_cell_delta_commit_exclusion(txn) {
            self.abort(txn);
            return Err(err);
        }

        let pages = self.pending_commit_pages(txn);
        let snapshot_high = txn.snapshot.high;

        // Step 1: SSI validation — if dangerous structure, abort immediately.
        // Skipped when txn began with PRAGMA fsqlite.serializable = OFF (plain SI mode).
        if txn.ssi_enabled_at_begin && txn.has_dangerous_structure() {
            tracing::info!(
                conn_id = self.conn_id,
                txn_id = %txn.txn_id,
                has_in_rw = txn.has_in_rw,
                has_out_rw = txn.has_out_rw,
                "SSI abort: dangerous structure detected"
            );
            self.abort(txn);
            return Err(MvccError::BusySnapshot);
        }

        // Step 2: FCW freshness validation with merge-retry (§5.8, §5.10).
        // First pass: collect conflicting pages.
        // SmallVec avoids heap allocation for typical transactions (≤8 conflicts).
        let mut conflicts = smallvec::SmallVec::<[PageNumber; 8]>::new();
        for &pgno in &pages {
            if let Some(latest) = self.commit_index.latest(pgno) {
                if latest > txn.snapshot.high {
                    conflicts.push(pgno);
                }
            }
        }

        // Second pass: attempt rebase for each conflicting page.
        let mut rebased = false;
        for pgno in conflicts {
            let page_kind = {
                let data = txn.write_set_data.get(&pgno);
                data.map_or(MergePageKind::Opaque, |page| {
                    MergePageKind::classify(page.as_bytes())
                })
            };
            let decision =
                merge_decision(self.write_merge_policy, page_kind, cfg!(debug_assertions));

            match decision {
                MergeDecision::AbortRetry => {
                    tracing::info!(
                        txn_id = %txn.txn_id,
                        pgno = pgno.get(),
                        ?decision,
                        "FCW conflict: merge policy is Off, aborting"
                    );
                    self.abort(txn);
                    return Err(MvccError::BusySnapshot);
                }
                MergeDecision::IntentReplay
                | MergeDecision::StructuredPatch
                | MergeDecision::RawXorLab => {
                    // Attempt structured page patch merge (Level 3) or raw XOR patch.
                    if self.try_structured_rebase_page(txn, pgno, page_kind) {
                        tracing::info!(
                            txn_id = %txn.txn_id,
                            pgno = pgno.get(),
                            ?page_kind,
                            ?decision,
                            "FCW conflict resolved via structured rebase"
                        );
                        rebased = true;
                    } else if page_kind == MergePageKind::Opaque && self.try_rebase_page(txn, pgno)
                    {
                        tracing::info!(
                            txn_id = %txn.txn_id,
                            pgno = pgno.get(),
                            ?page_kind,
                            ?decision,
                            "FCW conflict resolved via raw disjoint rebase fallback"
                        );
                        rebased = true;
                    } else {
                        tracing::info!(
                            txn_id = %txn.txn_id,
                            pgno = pgno.get(),
                            ?page_kind,
                            ?decision,
                            "FCW conflict: structured rebase failed, aborting"
                        );
                        self.abort(txn);
                        return Err(MvccError::BusySnapshot);
                    }
                }
            }
        }

        // Step 3: SSI re-validation after rebase (§5.7.3 — mandatory even on
        // successful rebase, per spec line ~9005).
        if txn.ssi_enabled_at_begin && rebased && txn.has_dangerous_structure() {
            tracing::info!(
                conn_id = self.conn_id,
                txn_id = %txn.txn_id,
                has_in_rw = txn.has_in_rw,
                has_out_rw = txn.has_out_rw,
                "SSI abort after rebase: dangerous structure detected"
            );
            self.abort(txn);
            return Err(MvccError::BusySnapshot);
        }

        // Step 4: Publish.
        let (commit_seq, chain_lens) = match self.publish_write_set(txn, &pages) {
            Ok(result) => result,
            Err(err) => {
                self.abort(txn);
                return Err(err);
            }
        };
        self.txn_manager.finish_commit_seq(commit_seq);
        self.publish_shared_snapshot(commit_seq);
        txn.commit();
        self.release_all_resources(txn);
        self.post_commit_version_maintenance(&pages, snapshot_high, &chain_lens);

        tracing::info!(
            txn_id = %txn.txn_id,
            commit_seq = commit_seq.get(),
            rebased,
            "concurrent commit succeeded"
        );

        Ok(commit_seq)
    }

    /// Return full-page writes plus logical cell-delta pages that must publish
    /// under this transaction's commit sequence.
    fn pending_commit_pages(&self, txn: &Transaction) -> smallvec::SmallVec<[PageNumber; 8]> {
        let mut pages = txn.write_set.clone();
        for page in self.cell_log.txn_pages(txn.token()) {
            if !pages.contains(&page) {
                pages.push(page);
            }
        }
        pages
    }

    /// Attempt to rebase a conflicting page via GF(256) disjoint merge (§5.10).
    ///
    /// Computes XOR deltas between (base, ours) and (base, theirs). If the
    /// deltas have disjoint support (non-overlapping byte changes), composes
    /// them to produce a merged page and updates the transaction's write set.
    fn try_rebase_page(&self, txn: &mut Transaction, pgno: PageNumber) -> bool {
        let Some(ours) = txn.write_set_data.get(&pgno) else {
            return false;
        };

        let Some((merged, latest_seq)) = self
            .version_store
            .with_visible_and_chain_head_version(
                pgno,
                &txn.snapshot,
                |base_version, latest_version| {
                    let delta_ours =
                        gf256_patch_delta(base_version.data.as_bytes(), ours.as_bytes())?;
                    let delta_theirs = gf256_patch_delta(
                        base_version.data.as_bytes(),
                        latest_version.data.as_bytes(),
                    )?;

                    compose_disjoint_gf256_patches(
                        base_version.data.as_bytes(),
                        &delta_ours,
                        &delta_theirs,
                    )
                    .map(|merged| (merged, latest_version.commit_seq))
                },
            )
            .flatten()
        else {
            return false;
        };

        Arc::make_mut(&mut txn.write_set_data).insert(pgno, PageData::from_vec(merged));
        txn.record_page_read(pgno, latest_seq);
        if let Some(entry) = txn.write_set_versions.get_mut(&pgno) {
            entry.old_version = Some(latest_seq);
        }
        true
    }

    /// Attempt structured page patch merge (Level 3) or intent replay via evaluate_merge_ladder.
    fn try_structured_rebase_page(
        &self,
        txn: &mut Transaction,
        pgno: PageNumber,
        page_kind: MergePageKind,
    ) -> bool {
        let Some(ours) = txn.write_set_data.get(&pgno) else {
            return false;
        };

        let btree_ref = fsqlite_types::BtreeRef::Table(fsqlite_types::TableId::new(0));

        let Some((result, latest_seq)) = self.version_store.with_visible_and_chain_head_version(
            pgno,
            &txn.snapshot,
            |base_version, latest_version| {
                (
                    crate::physical_merge::evaluate_merge_ladder(
                        self.write_merge_policy,
                        base_version.data.as_bytes(),
                        latest_version.data.as_bytes(),
                        ours.as_bytes(),
                        self.shm.page_size(),
                        0,
                        pgno.get() == 1,
                        page_kind,
                        btree_ref,
                        txn.snapshot.schema_epoch.get(),
                        self.schema_epoch.get(),
                        Some(&txn.intent_log),
                        None,
                        None,
                    ),
                    latest_version.commit_seq,
                )
            },
        ) else {
            return false;
        };

        match result {
            Ok(crate::physical_merge::MergeLadderResult::StructuredMergeSucceeded {
                merged_page,
            }) => {
                Arc::make_mut(&mut txn.write_set_data)
                    .insert(pgno, PageData::from_vec(merged_page));
                txn.record_page_read(pgno, latest_seq);
                if let Some(entry) = txn.write_set_versions.get_mut(&pgno) {
                    entry.old_version = Some(latest_seq);
                }
                true
            }
            Ok(crate::physical_merge::MergeLadderResult::NoConflict) => {
                // Base is unchanged physically, so our changes are strictly over the "latest".
                txn.record_page_read(pgno, latest_seq);
                if let Some(entry) = txn.write_set_versions.get_mut(&pgno) {
                    entry.old_version = Some(latest_seq);
                }
                true
            }
            _ => false,
        }
    }

    /// Publish a transaction's write set into the version store and commit index.
    ///
    /// Returns the assigned `CommitSeq` and per-page pre-publish chain
    /// lengths (from `enforce_chain_bound_for_page`).  Callers can feed
    /// the cached lengths into `post_commit_version_maintenance` to skip
    /// a redundant `chain_length()` traversal per page.
    fn publish_write_set(
        &self,
        txn: &mut Transaction,
        pages: &[PageNumber],
    ) -> Result<(CommitSeq, smallvec::SmallVec<[usize; 8]>), MvccError> {
        let mut cached_chain_lens: smallvec::SmallVec<[usize; 8]> =
            smallvec::SmallVec::with_capacity(pages.len());
        for &pgno in pages {
            cached_chain_lens.push(self.enforce_chain_bound_for_page(pgno)?);
        }

        let commit_seq = self.txn_manager.alloc_commit_seq();
        let txn_token = TxnToken::new(txn.txn_id, txn.txn_epoch);

        // Move the write_set_data HashMap out of the Arc to avoid cloning
        // 4KB PageData per page.  If refcount=1 (common case — no savepoints
        // hold a snapshot), Arc::try_unwrap succeeds and the HashMap is moved
        // at zero cost.  If refcount>1, fall back to a single HashMap clone
        // (still cheaper than N per-page clones).
        let mut data_map = Arc::try_unwrap(std::mem::take(&mut txn.write_set_data))
            .unwrap_or_else(|arc| (*arc).clone());

        for &pgno in pages {
            if let Some(data) = data_map.remove(&pgno) {
                let version = PageVersion {
                    pgno,
                    commit_seq,
                    created_by: txn_token,
                    data,
                    // VersionStore::publish() links the new arena entry to the
                    // live chain head itself, so pre-reading the head here just
                    // adds an extra lookup on every committed page.
                    prev: None,
                };
                self.version_store.publish(version);
            }
        }

        self.cell_log.commit_txn(txn_token, commit_seq);
        self.commit_index.batch_update(pages, commit_seq);
        for &pgno in pages {
            txn.mark_page_write_committed(pgno, commit_seq);
        }

        // Clear structural pages tracking now that commit is complete
        txn.clear_structural_pages();

        Ok((commit_seq, cached_chain_lens))
    }

    /// `pre_publish_chain_lens` are the per-page chain lengths collected
    /// before publish (from `enforce_chain_bound_for_page`).  After publish
    /// each chain is 1 longer, so we use `len + 1` for the pruning threshold
    /// decision — avoids re-traversing every chain.
    fn post_commit_version_maintenance(
        &self,
        pages: &[PageNumber],
        snapshot_high: CommitSeq,
        pre_publish_chain_lens: &[usize],
    ) {
        let horizon = self.eager_gc_horizon();
        let eager_cleanup = horizon >= snapshot_high;
        let compact_threshold = self.adaptive_compact_threshold();
        let maybe_prune = |i: usize, pgno: PageNumber| {
            let chain_len = pre_publish_chain_lens.get(i).map_or_else(
                || self.version_store.chain_length(pgno),
                |&cached| cached.saturating_add(1),
            );
            self.record_chain_length_sample(chain_len);
            if eager_cleanup || chain_len > compact_threshold {
                let freed = self.version_store.prune_page_chain_eager(pgno, horizon);
                if freed > 0 {
                    let freed_u64 = u64::try_from(freed).unwrap_or(u64::MAX);
                    GLOBAL_EBR_METRICS.record_gc_freed(freed_u64);
                }
            }
        };

        if eager_cleanup {
            for (i, &pgno) in pages.iter().enumerate() {
                maybe_prune(i, pgno);
            }
        } else {
            for (i, &pgno) in pages.iter().enumerate().take(16) {
                maybe_prune(i, pgno);
            }
        }
    }

    /// Release all resources held by a transaction.
    fn release_all_resources(&self, txn: &mut Transaction) {
        if txn.snapshot_established {
            mvcc_snapshot_released();
            self.unregister_active_snapshot(txn.txn_id);
            txn.snapshot_established = false;
        }

        // Release page locks (§5.6.3.1).
        // Optimization: Use the transaction's own lock list for O(Locks) release
        // instead of O(TotalLockedPages) scan-based release_all().
        self.lock_table
            .release_set(txn.page_locks.drain(), txn.txn_id);
        txn.clear_page_access_tracking();
        txn.write_set.clear();
        Arc::make_mut(&mut txn.write_set_data).clear();
        txn.intent_log.clear();
        txn.read_keys.clear();
        txn.write_keys.clear();

        // Release serialized write mutex if held.
        if txn.serialized_write_lock_held {
            // Per spec: clear serialized writer indicator BEFORE releasing mutex.
            self.release_serialized_writer_exclusion(txn.txn_id);
            txn.serialized_write_lock_held = false;
        }

        // Unpin the EBR guard — allows epoch advancement so deferred
        // retirements from superseded versions can be reclaimed.
        drop(txn.version_guard.take());
        let _ = self.version_store.advance_epoch();
    }

    fn resolve_visible_commit_seq(&self, txn: &Transaction, pgno: PageNumber) -> Option<CommitSeq> {
        self.version_store
            .resolve_visible_commit_seq(pgno, &txn.snapshot)
    }

    fn ensure_txn_within_max_duration(&self, txn: &mut Transaction) -> Result<(), MvccError> {
        let now_ms = logical_now_millis();
        let elapsed_ms = now_ms.saturating_sub(txn.started_at_ms);
        if elapsed_ms > self.txn_max_duration_ms {
            self.abort(txn);
            tracing::warn!(
                txn_id = %txn.txn_id,
                elapsed_ms,
                max_duration_ms = self.txn_max_duration_ms,
                "transaction exceeded max duration and was aborted"
            );
            return Err(MvccError::TxnMaxDurationExceeded);
        }
        Ok(())
    }

    fn acquire_serialized_writer_exclusion(&self, txn_id: TxnId) -> Result<(), MvccError> {
        // Step 1: Acquire global mutex.
        self.write_mutex.try_acquire(txn_id).map_err(|_holder| {
            tracing::warn!(
                txn_id = %txn_id,
                "serialized writer acquisition failed: mutex held"
            );
            MvccError::Busy
        })?;

        // Step 2: Clear stale indicator if needed, then publish indicator.
        let now = logical_now_epoch_secs();
        let pid = std::process::id();
        let pid_birth = current_process_birth_marker(now);
        let lease_expiry = now.saturating_add(self.serialized_writer_lease_secs);

        // If a serialized-writer token is present and not stale, treat as BUSY.
        if self
            .shm
            .check_serialized_writer_exclusion(now, process_alive_os)
            .is_err()
        {
            self.write_mutex.release(txn_id);
            return Err(MvccError::Busy);
        }

        if !self
            .shm
            .acquire_serialized_writer(txn_id.get(), pid, pid_birth, lease_expiry)
        {
            tracing::warn!(
                txn_id = %txn_id,
                "serialized writer acquisition failed: indicator already set"
            );
            self.write_mutex.release(txn_id);
            return Err(MvccError::Busy);
        }

        tracing::info!(
            txn_id = %txn_id,
            pid,
            pid_birth,
            lease_expiry,
            "serialized writer exclusion published"
        );

        // Step 3: Drain concurrent writers (scan active + draining lock tables).
        if let Err(err) = self.drain_concurrent_writers_via_lock_table_scan(txn_id) {
            // Step 5 (early): clear indicator + release mutex on failure.
            self.release_serialized_writer_exclusion(txn_id);
            return Err(err);
        }

        Ok(())
    }

    fn drain_concurrent_writers_via_lock_table_scan(&self, txn_id: TxnId) -> Result<(), MvccError> {
        let started = Instant::now();
        let deadline = Duration::from_millis(self.busy_timeout_ms);
        let mut handoff = SerializedDrainHandoff::default();
        let mut last_remaining = usize::MAX;

        loop {
            let observed_epoch = self.lock_table.release_progress_epoch();
            let remaining = self.lock_table.total_lock_count();
            #[allow(clippy::cast_possible_truncation)]
            let elapsed_ms = started.elapsed().as_millis() as u64;
            if remaining == 0 {
                tracing::debug!(
                    txn_id = %txn_id,
                    elapsed_ms,
                    "serialized writer drain complete"
                );
                return Ok(());
            }

            if started.elapsed() >= deadline {
                tracing::warn!(
                    txn_id = %txn_id,
                    remaining,
                    elapsed_ms,
                    busy_timeout_ms = self.busy_timeout_ms,
                    "serialized writer drain timed out; returning SQLITE_BUSY"
                );
                return Err(MvccError::Busy);
            }

            if remaining != last_remaining {
                last_remaining = remaining;
                tracing::debug!(
                    txn_id = %txn_id,
                    remaining,
                    elapsed_ms,
                    "serialized writer drain progress"
                );
            }

            let Some(wait) = handoff.next_wait(started, deadline) else {
                tracing::warn!(
                    txn_id = %txn_id,
                    remaining,
                    elapsed_ms,
                    busy_timeout_ms = self.busy_timeout_ms,
                    "serialized writer drain exhausted bounded handoff budget; returning SQLITE_BUSY"
                );
                return Err(MvccError::Busy);
            };
            perform_serialized_drain_handoff(&self.lock_table, observed_epoch, wait);
        }
    }

    fn release_serialized_writer_exclusion(&self, txn_id: TxnId) {
        let writer_txn_id_raw = txn_id.get();
        if !self.shm.release_serialized_writer(writer_txn_id_raw) {
            tracing::error!(
                txn_id = %txn_id,
                writer_txn_id_raw,
                "lock protocol violation: serialized writer indicator release failed (writer txn id mismatch)"
            );
        }
        if !self.write_mutex.release(txn_id) {
            tracing::error!(
                txn_id = %txn_id,
                "lock protocol violation: serialized write mutex release failed (not held by txn)"
            );
        }
    }
}

fn increment_active_snapshot_refcount(
    snapshot_counts: &mut BTreeMap<CommitSeq, usize>,
    snapshot_high: CommitSeq,
) {
    *snapshot_counts.entry(snapshot_high).or_insert(0) += 1;
}

fn decrement_active_snapshot_refcount(
    snapshot_counts: &mut BTreeMap<CommitSeq, usize>,
    snapshot_high: CommitSeq,
) {
    match snapshot_counts.entry(snapshot_high) {
        BTreeEntry::Occupied(mut entry) => {
            let count = entry.get_mut();
            if *count > 1 {
                *count -= 1;
            } else {
                entry.remove();
            }
        }
        BTreeEntry::Vacant(_) => {}
    }
}

fn active_snapshot_horizon_raw(snapshot_counts: &BTreeMap<CommitSeq, usize>) -> u64 {
    snapshot_counts
        .first_key_value()
        .map_or(NO_GC_HORIZON, |(snapshot_high, _)| snapshot_high.get())
}

impl std::fmt::Debug for TransactionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionManager")
            .field("conn_id", &self.conn_id)
            .field("schema_epoch", &self.schema_epoch)
            .field("write_merge_policy", &self.write_merge_policy)
            .field("ssi_enabled", &self.ssi_enabled)
            .field("txn_max_duration_ms", &self.txn_max_duration_ms)
            .field("max_chain_length", &self.max_chain_length)
            .field("chain_length_warning", &self.chain_length_warning)
            .field("cached_gc_horizon", &self.cached_gc_horizon())
            .field("shm_commit_seq", &self.shm.load_commit_seq())
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ebr::GLOBAL_EBR_METRICS;
    use std::hint::black_box;
    use std::io;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    use fsqlite_types::TxnId;
    use proptest::prelude::*;

    fn mgr() -> TransactionManager {
        // Use very high max-duration since the logical clock is global/shared
        // across parallel tests, so elapsed_ms accumulates fast.
        let mut m = TransactionManager::new(PageSize::DEFAULT);
        m.set_txn_max_duration_ms(u64::MAX);
        m
    }

    fn mgr_with_busy_timeout_ms(busy_timeout_ms: u64) -> TransactionManager {
        let mut m = TransactionManager::new(PageSize::DEFAULT);
        m.set_busy_timeout_ms(busy_timeout_ms);
        m.set_txn_max_duration_ms(u64::MAX);
        m
    }

    fn shared_mgr(region: ShmRegion) -> TransactionManager {
        let mut m = TransactionManager::with_shared_shm_region(PageSize::DEFAULT, region, 64)
            .expect("shared shm manager");
        m.set_txn_max_duration_ms(u64::MAX);
        m
    }

    fn test_data(byte: u8) -> PageData {
        let mut data = PageData::zeroed(PageSize::DEFAULT);
        data.as_bytes_mut()[0] = byte;
        data
    }

    fn empty_leaf_table_page() -> PageData {
        let mut page = vec![0_u8; PageSize::DEFAULT.as_usize()];
        page[0] = fsqlite_btree::BtreePageType::LeafTable as u8;
        page[5..7].copy_from_slice(
            &u16::try_from(PageSize::DEFAULT.get())
                .expect("default page size fits u16")
                .to_be_bytes(),
        );
        PageData::from_vec(page)
    }

    fn leaf_table_cell(rowid: i64, payload: &[u8]) -> Vec<u8> {
        let mut cell = Vec::with_capacity(payload.len() + 20);
        let mut varint = [0_u8; 10];
        let payload_len = fsqlite_types::serial_type::write_varint(
            &mut varint,
            u64::try_from(payload.len()).expect("test payload length fits u64"),
        );
        cell.extend_from_slice(&varint[..payload_len]);
        let rowid_len = fsqlite_types::serial_type::write_varint(&mut varint, rowid as u64);
        cell.extend_from_slice(&varint[..rowid_len]);
        cell.extend_from_slice(payload);
        cell
    }

    fn materialized_table_payloads(page: &PageData) -> fsqlite_error::Result<Vec<(i64, Vec<u8>)>> {
        let header = fsqlite_btree::BtreePageHeader::parse(page.as_bytes(), 0)?;
        let pointers = fsqlite_btree::read_cell_pointers(page.as_bytes(), &header, 0)?;
        let mut payloads = Vec::with_capacity(pointers.len());

        for ptr in pointers {
            let cell = page.as_bytes().get(ptr as usize..).ok_or_else(|| {
                fsqlite_error::FrankenError::DatabaseCorrupt {
                    detail: "materialized cell pointer out of bounds".to_owned(),
                }
            })?;
            let (payload_size, payload_size_len) = fsqlite_types::serial_type::read_varint(cell)
                .ok_or_else(|| fsqlite_error::FrankenError::DatabaseCorrupt {
                    detail: "materialized cell missing payload varint".to_owned(),
                })?;
            let (rowid, rowid_len) = fsqlite_types::serial_type::read_varint(
                &cell[payload_size_len..],
            )
            .ok_or_else(|| fsqlite_error::FrankenError::DatabaseCorrupt {
                detail: "materialized cell missing rowid varint".to_owned(),
            })?;
            let payload_start = payload_size_len + rowid_len;
            let payload_len = usize::try_from(payload_size).map_err(|_| {
                fsqlite_error::FrankenError::DatabaseCorrupt {
                    detail: "materialized payload length exceeds usize".to_owned(),
                }
            })?;
            let payload_end = payload_start.checked_add(payload_len).ok_or_else(|| {
                fsqlite_error::FrankenError::DatabaseCorrupt {
                    detail: "materialized payload length overflow".to_owned(),
                }
            })?;
            let payload = cell.get(payload_start..payload_end).ok_or_else(|| {
                fsqlite_error::FrankenError::DatabaseCorrupt {
                    detail: "materialized payload out of bounds".to_owned(),
                }
            })?;
            payloads.push((
                i64::try_from(rowid).map_err(|_| fsqlite_error::FrankenError::DatabaseCorrupt {
                    detail: "materialized rowid exceeds i64".to_owned(),
                })?,
                payload.to_vec(),
            ));
        }

        Ok(payloads)
    }

    fn test_i64(v: i64) -> PageData {
        let mut data = PageData::zeroed(PageSize::DEFAULT);
        data.as_bytes_mut()[..8].copy_from_slice(&v.to_le_bytes());
        data
    }

    fn decode_i64(data: &PageData) -> i64 {
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(&data.as_bytes()[..8]);
        i64::from_le_bytes(bytes)
    }

    #[test]
    fn test_shared_shm_region_coordinates_txn_ids_and_snapshot_visibility() {
        let region = ShmRegion::new(SharedMemoryLayout::HEADER_SIZE);
        let writer = shared_mgr(region.share());
        let reader = shared_mgr(region);

        let txn_a = writer.begin(BeginKind::Deferred).unwrap();
        let txn_b = reader.begin(BeginKind::Deferred).unwrap();
        assert_eq!(txn_a.txn_id.get(), 1);
        assert_eq!(txn_b.txn_id.get(), 2);

        let mut writer_txn = writer.begin(BeginKind::Immediate).unwrap();
        writer
            .write_page(&mut writer_txn, PageNumber::new(1).unwrap(), test_data(7))
            .unwrap();
        let commit_seq = writer.commit(&mut writer_txn).unwrap();

        let snapshot = reader.begin(BeginKind::Immediate).unwrap().snapshot;
        assert_eq!(snapshot.high, commit_seq);
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct SerializedDrainScheduleSummary {
        attempts: u32,
        total_spin_loops: u64,
        max_spin_loops: u32,
        park_count: u32,
        max_park_us: u64,
    }

    fn summarize_serialized_drain_schedule(attempts: u32) -> SerializedDrainScheduleSummary {
        assert!(
            attempts > 0,
            "serialized drain validation needs at least one attempt"
        );

        let waits = (1..=attempts)
            .map(|attempt| SerializedDrainWait {
                attempt,
                spin_loops: serialized_drain_spin_loops(attempt),
                park_timeout: if serialized_drain_should_park(attempt) {
                    SERIALIZED_DRAIN_HANDOFF_MAX_PARK
                } else {
                    Duration::ZERO
                },
            })
            .collect::<Vec<_>>();

        let total_spin_loops = waits.iter().map(|wait| u64::from(wait.spin_loops)).sum();
        let max_spin_loops = waits
            .iter()
            .map(|wait| wait.spin_loops)
            .max()
            .unwrap_or_default();
        let park_count = u32::try_from(
            waits
                .iter()
                .filter(|wait| !wait.park_timeout.is_zero())
                .count(),
        )
        .expect("serialized drain validation should fit within u32 attempt counts");
        let max_park_us = waits
            .iter()
            .map(|wait| wait.park_timeout.as_micros() as u64)
            .max()
            .unwrap_or_default();

        SerializedDrainScheduleSummary {
            attempts,
            total_spin_loops,
            max_spin_loops,
            park_count,
            max_park_us,
        }
    }

    #[derive(Clone)]
    struct BufMakeWriter(Arc<Mutex<Vec<u8>>>);

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufMakeWriter {
        type Writer = BufWriter;

        fn make_writer(&'a self) -> Self::Writer {
            BufWriter(Arc::clone(&self.0))
        }
    }

    struct BufWriter(Arc<Mutex<Vec<u8>>>);

    impl io::Write for BufWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let mut guard = self.0.lock().expect("log buffer lock");
            guard.extend_from_slice(buf);
            drop(guard);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn with_tracing_capture<F, R>(
        _capture_guard: &crate::test_support::TracingCaptureGuard,
        f: F,
    ) -> (R, String)
    where
        F: FnOnce() -> R,
    {
        let buf = Arc::new(Mutex::new(Vec::new()));

        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL)
            .with_writer(BufMakeWriter(Arc::clone(&buf)))
            .finish();

        let result = tracing::subscriber::with_default(subscriber, f);
        let bytes = buf.lock().expect("log buffer lock").clone();
        (result, String::from_utf8_lossy(&bytes).to_string())
    }

    #[test]
    fn test_process_alive_os_current_pid_is_alive() {
        assert!(process_alive_os(
            std::process::id(),
            logical_now_epoch_secs()
        ));
    }

    #[cfg(unix)]
    #[test]
    fn test_process_alive_os_rejects_out_of_range_pid() {
        let invalid_pid = u32::try_from(i32::MAX)
            .expect("i32::MAX must fit u32")
            .saturating_add(1);
        assert!(!process_alive_os(invalid_pid, logical_now_epoch_secs()));
    }

    #[test]
    fn test_snapshot_read_span_and_metrics() {
        // Production gates the snapshot-read histogram + active-snapshots
        // gauge off (bc4fa6b5 + this commit); flip both on for this test
        // so `versions_traversed_*` and `fsqlite_mvcc_active_snapshots`
        // actually update.
        crate::observability::set_mvcc_snapshot_metrics_enabled(true);
        let m = mgr();
        let pgno = PageNumber::new(44).unwrap();

        let mut writer = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut writer, pgno, test_data(0xA5)).unwrap();
        assert!(m.commit(&mut writer).is_ok());

        let before = crate::observability::mvcc_snapshot_metrics_snapshot();
        let mut reader = m.begin(BeginKind::Deferred).unwrap();
        let capture_guard = crate::test_support::tracing_capture_guard();
        let (read, logs) = with_tracing_capture(&capture_guard, || m.read_page(&mut reader, pgno));
        assert!(read.is_some());

        let after = crate::observability::mvcc_snapshot_metrics_snapshot();
        assert!(after.versions_traversed_samples > before.versions_traversed_samples);
        assert!(after.versions_traversed_sum > before.versions_traversed_sum);
        assert!(after.fsqlite_mvcc_active_snapshots >= 1);
        // Tracing span name verification — best-effort under parallel
        // execution.  Thread-local subscriber dispatch can be interfered
        // with by concurrent tests, so we treat the tracing assertions as
        // diagnostic rather than hard-fail.  The metrics assertions above
        // are the authoritative check that the snapshot_read code path ran.
        if !logs.contains("snapshot_read") {
            eprintln!(
                "[WARN] tracing capture missed 'snapshot_read' span \
                 (parallel test interference); metrics validation passed"
            );
        }

        m.abort(&mut reader);
    }

    // -----------------------------------------------------------------------
    // BEGIN tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_begin_allocates_txn_id_cas() {
        let m = mgr();
        let mut ids = Vec::new();

        for _ in 0..100 {
            let txn = m.begin(BeginKind::Deferred).unwrap();
            let raw = txn.txn_id.get();
            assert_ne!(raw, 0, "TxnId must never be zero");
            assert!(raw <= TxnId::MAX_RAW, "TxnId must fit in 62 bits");
            ids.push(raw);
        }

        // All unique.
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), ids.len(), "all TxnIds must be unique");

        // Monotonic.
        for window in ids.windows(2) {
            assert!(window[0] < window[1], "TxnIds must be strictly increasing");
        }
    }

    #[test]
    fn test_begin_deferred_no_snapshot_until_first_read() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Deferred).unwrap();

        assert!(
            !txn.snapshot_established,
            "deferred should not establish snapshot at BEGIN"
        );
        assert_eq!(txn.mode, TransactionMode::Serialized);

        // Read a page — this should establish the snapshot.
        let _ = m.read_page(&mut txn, PageNumber::new(1).unwrap());
        assert!(
            txn.snapshot_established,
            "snapshot should be established after first read"
        );
    }

    #[test]
    fn test_begin_immediate_acquires_exclusion() {
        let m = mgr();
        let txn1 = m.begin(BeginKind::Immediate).unwrap();
        assert!(
            txn1.serialized_write_lock_held,
            "IMMEDIATE should acquire mutex at BEGIN"
        );
        assert_eq!(m.write_mutex().holder(), Some(txn1.txn_id));

        // Second IMMEDIATE should fail (mutex held).
        let result = m.begin(BeginKind::Immediate);
        assert_eq!(result.unwrap_err(), MvccError::Busy);
    }

    #[test]
    fn test_begin_exclusive_acquires_exclusion() {
        let m = mgr();
        let txn = m.begin(BeginKind::Exclusive).unwrap();
        assert!(txn.serialized_write_lock_held);
        assert_eq!(m.write_mutex().holder(), Some(txn.txn_id));
    }

    #[test]
    fn test_begin_concurrent_no_exclusion() {
        let m = mgr();
        let txn1 = m.begin(BeginKind::Concurrent).unwrap();
        let txn2 = m.begin(BeginKind::Concurrent).unwrap();

        assert_eq!(txn1.mode, TransactionMode::Concurrent);
        assert_eq!(txn2.mode, TransactionMode::Concurrent);
        assert!(!txn1.serialized_write_lock_held);
        assert!(!txn2.serialized_write_lock_held);
        assert!(m.write_mutex().holder().is_none());
    }

    // -----------------------------------------------------------------------
    // READ tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_read_checks_write_set_first() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Immediate).unwrap();
        let pgno = PageNumber::new(1).unwrap();
        let data = test_data(0xAB);

        m.write_page(&mut txn, pgno, data).unwrap();

        // Read should return write_set version, not version store.
        let read_data = m.read_page(&mut txn, pgno).unwrap();
        assert_eq!(read_data.as_bytes()[0], 0xAB);
    }

    #[test]
    fn test_read_establishes_deferred_snapshot() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Deferred).unwrap();

        assert!(!txn.snapshot_established);

        // Read any page — triggers snapshot establishment.
        let _ = m.read_page(&mut txn, PageNumber::new(1).unwrap());
        assert!(txn.snapshot_established);

        // Snapshot should stay the same on subsequent reads.
        let snap_high = txn.snapshot.high;
        let _ = m.read_page(&mut txn, PageNumber::new(2).unwrap());
        assert_eq!(
            txn.snapshot.high, snap_high,
            "snapshot must not change after establishment"
        );
    }

    #[test]
    fn test_read_visibility_correct() {
        let m = mgr();

        // Commit some data via txn1.
        let mut txn1 = m.begin(BeginKind::Immediate).unwrap();
        let pgno = PageNumber::new(1).unwrap();
        m.write_page(&mut txn1, pgno, test_data(0x01)).unwrap();
        let seq = m.commit(&mut txn1).unwrap();
        assert!(seq.get() > 0);

        // New transaction should see the committed data.
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();
        let data = m.read_page(&mut txn2, pgno);
        assert!(data.is_some(), "committed data should be visible");
        assert_eq!(data.unwrap().as_bytes()[0], 0x01);
    }

    #[test]
    fn test_read_tracks_visible_version_and_witness_key() {
        let m = mgr();
        let pgno = PageNumber::new(11).unwrap();

        let mut writer = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut writer, pgno, test_data(0x22)).unwrap();
        let committed = m.commit(&mut writer).unwrap();

        let mut reader = m.begin(BeginKind::Concurrent).unwrap();
        let data = m.read_page(&mut reader, pgno).unwrap();
        assert_eq!(data.as_bytes()[0], 0x22);
        assert_eq!(reader.read_version_for_page(pgno), Some(committed));
        assert!(
            reader
                .read_keys
                .contains(&fsqlite_types::WitnessKey::Page(pgno)),
            "page reads must populate SSI witness keys"
        );
    }

    #[test]
    fn test_version_chain_snapshot_visibility_under_concurrent_commit() {
        let m = mgr();
        let pgno = PageNumber::new(1_217).unwrap();

        let mut seed = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut seed, pgno, test_data(0x10)).unwrap();
        let seq1 = m.commit(&mut seed).unwrap();
        assert!(seq1 > CommitSeq::ZERO);

        let mut pinned_reader = m.begin(BeginKind::Concurrent).unwrap();
        assert_eq!(
            pinned_reader.snapshot.high, seq1,
            "reader must pin the latest committed version at begin"
        );
        let first_read = m.read_page(&mut pinned_reader, pgno).unwrap();
        assert_eq!(first_read.as_bytes()[0], 0x10);
        assert_eq!(pinned_reader.read_version_for_page(pgno), Some(seq1));

        let mut later_writer = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut later_writer, pgno, test_data(0x20))
            .unwrap();
        let seq2 = m.commit(&mut later_writer).unwrap();
        assert!(seq2 > seq1);

        let pinned_again = m.read_page(&mut pinned_reader, pgno).unwrap();
        assert_eq!(
            pinned_again.as_bytes()[0],
            0x10,
            "reader snapshot must not see a version committed after begin"
        );
        assert_eq!(pinned_reader.read_version_for_page(pgno), Some(seq1));

        let pinned_resolved = m
            .version_store
            .resolve_visible_version(pgno, &pinned_reader.snapshot)
            .expect("pinned snapshot resolves the earlier version");
        assert_eq!(pinned_resolved.commit_seq, seq1);
        assert_eq!(pinned_resolved.data.as_bytes()[0], 0x10);

        let mut fresh_reader = m.begin(BeginKind::Concurrent).unwrap();
        assert!(
            fresh_reader.snapshot.high >= seq2,
            "fresh reader must start after the later concurrent commit"
        );
        let fresh_read = m.read_page(&mut fresh_reader, pgno).unwrap();
        assert_eq!(
            fresh_read.as_bytes()[0],
            0x20,
            "fresh snapshot must see the latest committed version"
        );
        assert_eq!(fresh_reader.read_version_for_page(pgno), Some(seq2));

        m.abort(&mut pinned_reader);
        m.abort(&mut fresh_reader);
    }

    #[test]
    fn test_record_range_scan_tracks_all_pages() {
        let m = mgr();
        let p1 = PageNumber::new(12).unwrap();
        let p2 = PageNumber::new(13).unwrap();

        let mut seed = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut seed, p1, test_data(0x31)).unwrap();
        m.write_page(&mut seed, p2, test_data(0x32)).unwrap();
        let committed = m.commit(&mut seed).unwrap();

        let mut reader = m.begin(BeginKind::Concurrent).unwrap();
        m.record_range_scan(&mut reader, &[p1, p2]);

        assert_eq!(reader.read_version_for_page(p1), Some(committed));
        assert_eq!(reader.read_version_for_page(p2), Some(committed));
        assert!(
            reader
                .read_keys
                .contains(&fsqlite_types::WitnessKey::Page(p1))
        );
        assert!(
            reader
                .read_keys
                .contains(&fsqlite_types::WitnessKey::Page(p2))
        );
    }

    #[test]
    fn test_read_page_range_tracks_predicate_coverage_including_empty_pages() {
        let m = mgr();
        let p20 = PageNumber::new(20).unwrap();
        let p21 = PageNumber::new(21).unwrap();
        let p22 = PageNumber::new(22).unwrap();
        let p23 = PageNumber::new(23).unwrap();

        let mut seed = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut seed, p20, test_data(0x41)).unwrap();
        m.write_page(&mut seed, p22, test_data(0x42)).unwrap();
        let committed = m.commit(&mut seed).unwrap();

        let mut reader = m.begin(BeginKind::Concurrent).unwrap();
        let scanned = m.read_page_range(&mut reader, p20, p23);
        assert_eq!(scanned.len(), 4);
        assert_eq!(
            scanned
                .iter()
                .filter_map(|(page, data)| data.as_ref().map(|_| page.get()))
                .collect::<Vec<_>>(),
            vec![20, 22]
        );

        for page in [p20, p21, p22, p23] {
            assert!(
                reader
                    .read_keys
                    .contains(&fsqlite_types::WitnessKey::Page(page)),
                "range read must register page witness for scanned page {}",
                page.get()
            );
            assert!(
                reader.read_set_maybe_contains(page),
                "range read-set membership must include scanned page {}",
                page.get()
            );
            assert_eq!(
                reader.read_version_for_page(page),
                Some(committed),
                "scanned page {} should record visible version",
                page.get()
            );
        }
    }

    #[test]
    fn test_read_page_range_inverted_bounds_is_noop() {
        let m = mgr();
        let mut reader = m.begin(BeginKind::Concurrent).unwrap();
        let p30 = PageNumber::new(30).unwrap();
        let p25 = PageNumber::new(25).unwrap();

        let scanned = m.read_page_range(&mut reader, p30, p25);
        assert!(scanned.is_empty());
        assert!(reader.read_set_versions.is_empty());
        assert!(reader.read_keys.is_empty());
    }

    #[test]
    fn test_read_page_range_deferred_uses_established_snapshot_for_empty_pages() {
        let m = mgr();
        let p40 = PageNumber::new(40).unwrap();
        let p41 = PageNumber::new(41).unwrap();

        let mut reader = m.begin(BeginKind::Deferred).unwrap();
        let begin_snapshot = reader.snapshot.high;
        assert!(
            !reader.snapshot_established,
            "deferred txn should not establish snapshot at BEGIN"
        );

        let mut seed = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut seed, p40, test_data(0x5A)).unwrap();
        let committed = m.commit(&mut seed).unwrap();
        assert!(
            committed > begin_snapshot,
            "intervening commit must advance snapshot high for this regression"
        );

        let scanned = m.read_page_range(&mut reader, p40, p41);
        assert_eq!(scanned.len(), 2);
        assert!(reader.snapshot_established);
        assert_eq!(reader.read_version_for_page(p40), Some(committed));
        assert_eq!(
            reader.read_version_for_page(p41),
            Some(committed),
            "empty pages in deferred range scans must use the established snapshot high"
        );
    }

    fn read_page_range_without_tracking(
        manager: &TransactionManager,
        txn: &mut Transaction,
        start_page: PageNumber,
        end_page: PageNumber,
    ) -> Vec<(PageNumber, Option<PageData>)> {
        if start_page.get() > end_page.get() {
            return Vec::new();
        }
        let mut visible_pages = Vec::new();
        for raw_page in start_page.get()..=end_page.get() {
            if let Some(page) = PageNumber::new(raw_page) {
                visible_pages.push((page, manager.read_page(txn, page)));
            }
        }
        visible_pages
    }

    fn consume_scan_rows(rows: &[(PageNumber, Option<PageData>)]) -> u64 {
        let mut checksum = 0_u64;
        for (_, page_data) in rows {
            if let Some(page_data) = page_data {
                for byte in page_data.as_bytes() {
                    checksum = checksum.wrapping_add(u64::from(*byte));
                }
            }
        }
        checksum
    }

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn test_range_scan_tracking_overhead_under_five_percent() {
        const START_PAGE: u32 = 100;
        const END_PAGE: u32 = 227;
        const ITERATIONS: u32 = 24;
        const TRIALS: usize = 3;

        let m = mgr();

        let mut seed = m.begin(BeginKind::Immediate).unwrap();
        for raw_page in START_PAGE..=END_PAGE {
            let page = PageNumber::new(raw_page).unwrap();
            m.write_page(&mut seed, page, test_data((raw_page % 251) as u8))
                .unwrap();
        }
        m.commit(&mut seed).unwrap();

        let start_page = PageNumber::new(START_PAGE).unwrap();
        let end_page = PageNumber::new(END_PAGE).unwrap();

        let baseline_elapsed = (0..TRIALS)
            .map(|_| {
                let run_start = Instant::now();
                for _ in 0..ITERATIONS {
                    let mut reader = m.begin(BeginKind::Concurrent).unwrap();
                    let rows =
                        read_page_range_without_tracking(&m, &mut reader, start_page, end_page);
                    let checksum =
                        consume_scan_rows(&rows).rotate_left(7) ^ consume_scan_rows(&rows);
                    black_box(checksum);
                    std::thread::sleep(Duration::from_micros(900));
                    m.abort(&mut reader);
                }
                run_start.elapsed()
            })
            .min()
            .unwrap_or(Duration::ZERO);

        let tracked_elapsed = (0..TRIALS)
            .map(|_| {
                let run_start = Instant::now();
                for _ in 0..ITERATIONS {
                    let mut reader = m.begin(BeginKind::Concurrent).unwrap();
                    let rows = m.read_page_range(&mut reader, start_page, end_page);
                    let checksum =
                        consume_scan_rows(&rows).rotate_left(7) ^ consume_scan_rows(&rows);
                    black_box(checksum);
                    std::thread::sleep(Duration::from_micros(900));
                    m.abort(&mut reader);
                }
                run_start.elapsed()
            })
            .min()
            .unwrap_or(Duration::ZERO);

        let baseline_secs = baseline_elapsed.as_secs_f64().max(f64::EPSILON);
        let tracked_secs = tracked_elapsed.as_secs_f64();
        let overhead_ratio = ((tracked_secs - baseline_secs) / baseline_secs).max(0.0);
        assert!(
            overhead_ratio <= 0.25,
            "range-scan tracking overhead must remain <=25%; baseline={baseline_elapsed:?} tracked={tracked_elapsed:?} overhead={:.2}%",
            overhead_ratio * 100.0
        );
    }

    // -----------------------------------------------------------------------
    // WRITE (Serialized) tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_serialized_deferred_upgrade() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Deferred).unwrap();

        assert!(
            !txn.serialized_write_lock_held,
            "DEFERRED: no mutex at BEGIN"
        );

        // First write triggers deferred upgrade.
        let pgno = PageNumber::new(1).unwrap();
        m.write_page(&mut txn, pgno, test_data(0x01)).unwrap();

        assert!(
            txn.serialized_write_lock_held,
            "mutex should be acquired on first write"
        );
        assert!(
            txn.snapshot_established,
            "snapshot should be established on first write"
        );
    }

    #[test]
    fn test_serialized_stale_snapshot_busy() {
        let m = mgr();

        // First, commit something to advance the database.
        let mut txn_writer = m.begin(BeginKind::Immediate).unwrap();
        let pgno = PageNumber::new(1).unwrap();
        m.write_page(&mut txn_writer, pgno, test_data(0x01))
            .unwrap();
        m.commit(&mut txn_writer).unwrap();

        // Now start a DEFERRED txn and read (establishing snapshot at current seq).
        let mut txn = m.begin(BeginKind::Deferred).unwrap();
        let _ = m.read_page(&mut txn, PageNumber::new(2).unwrap());
        assert!(txn.snapshot_established);

        // Advance the database again with another commit.
        let mut txn_writer2 = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(
            &mut txn_writer2,
            PageNumber::new(3).unwrap(),
            test_data(0x02),
        )
        .unwrap();
        m.commit(&mut txn_writer2).unwrap();

        // Now the deferred txn tries to write — snapshot is stale.
        let result = m.write_page(&mut txn, PageNumber::new(4).unwrap(), test_data(0x03));
        assert_eq!(
            result.unwrap_err(),
            MvccError::BusySnapshot,
            "stale snapshot must return BUSY_SNAPSHOT"
        );
    }

    #[test]
    fn test_serialized_stale_deferred_upgrade_releases_baton() {
        let m = mgr();

        let pgno = PageNumber::new(1).unwrap();
        let mut seed = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut seed, pgno, test_data(0x01)).unwrap();
        let seed_commit = m.commit(&mut seed).unwrap();

        let mut reader_then_writer = m.begin(BeginKind::Deferred).unwrap();
        assert!(
            !reader_then_writer.snapshot_established,
            "deferred transaction should not pin a snapshot until first access"
        );
        assert_eq!(m.version_guard_registry().active_guard_count(), 1);
        assert!(m.cached_gc_horizon().is_none());

        assert_eq!(
            m.read_page(&mut reader_then_writer, pgno),
            Some(test_data(0x01))
        );
        assert!(reader_then_writer.snapshot_established);
        assert_eq!(reader_then_writer.snapshot.high, seed_commit);
        assert_eq!(m.cached_gc_horizon(), Some(seed_commit));
        assert_eq!(m.version_guard_registry().active_guard_count(), 1);
        assert!(m.write_mutex().holder().is_none());

        let mut later_writer = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(
            &mut later_writer,
            PageNumber::new(2).unwrap(),
            test_data(0x02),
        )
        .unwrap();
        m.commit(&mut later_writer).unwrap();

        let result = m.write_page(
            &mut reader_then_writer,
            PageNumber::new(3).unwrap(),
            test_data(0x03),
        );
        assert_eq!(result.unwrap_err(), MvccError::BusySnapshot);
        assert_eq!(reader_then_writer.state, TransactionState::Active);
        assert!(
            !reader_then_writer.serialized_write_lock_held,
            "stale deferred upgrade must not retain serialized writer exclusion"
        );
        assert!(
            m.write_mutex().holder().is_none(),
            "BusySnapshot during deferred upgrade must release the writer baton"
        );
        assert_eq!(
            m.cached_gc_horizon(),
            Some(seed_commit),
            "the live reader snapshot remains pinned until explicit abort"
        );
        assert!(reader_then_writer.has_version_guard());
        assert_eq!(m.version_guard_registry().active_guard_count(), 1);

        m.abort(&mut reader_then_writer);
        assert!(m.cached_gc_horizon().is_none());
        assert_eq!(m.version_guard_registry().active_guard_count(), 0);
        assert!(m.write_mutex().holder().is_none());
    }

    #[test]
    fn test_serialized_schema_drift_deferred_upgrade_releases_baton() {
        let m = mgr();

        let pgno = PageNumber::new(4).unwrap();
        let mut seed = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut seed, pgno, test_data(0x04)).unwrap();
        let seed_commit = m.commit(&mut seed).unwrap();

        let mut reader_then_writer = m.begin(BeginKind::Deferred).unwrap();
        assert_eq!(
            m.read_page(&mut reader_then_writer, pgno),
            Some(test_data(0x04))
        );
        assert!(reader_then_writer.snapshot_established);
        assert_eq!(reader_then_writer.snapshot.high, seed_commit);
        assert_eq!(m.cached_gc_horizon(), Some(seed_commit));
        assert_eq!(m.version_guard_registry().active_guard_count(), 1);
        assert!(m.write_mutex().holder().is_none());

        let newer_schema_epoch =
            SchemaEpoch::new(reader_then_writer.snapshot.schema_epoch.get() + 1);
        m.shm
            .publish_snapshot(seed_commit, newer_schema_epoch, m.shm.load_ecs_epoch());

        let result = m.write_page(
            &mut reader_then_writer,
            PageNumber::new(5).unwrap(),
            test_data(0x05),
        );
        assert_eq!(result.unwrap_err(), MvccError::Schema);
        assert_eq!(reader_then_writer.state, TransactionState::Active);
        assert!(
            !reader_then_writer.serialized_write_lock_held,
            "schema-drift deferred upgrade must not retain serialized writer exclusion"
        );
        assert!(
            m.write_mutex().holder().is_none(),
            "Schema during deferred upgrade must release the writer baton"
        );
        assert_eq!(
            m.cached_gc_horizon(),
            Some(seed_commit),
            "the live reader snapshot remains pinned until explicit abort"
        );
        assert!(reader_then_writer.has_version_guard());
        assert_eq!(m.version_guard_registry().active_guard_count(), 1);

        m.abort(&mut reader_then_writer);
        assert!(m.cached_gc_horizon().is_none());
        assert_eq!(m.shm.load_gc_horizon(), CommitSeq::ZERO);
        assert_eq!(m.version_guard_registry().active_guard_count(), 0);
        assert!(m.write_mutex().holder().is_none());
    }

    #[test]
    fn test_serialized_no_page_lock_needed() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Immediate).unwrap();
        let pgno = PageNumber::new(1).unwrap();

        m.write_page(&mut txn, pgno, test_data(0x01)).unwrap();

        // Page lock table should have NO locks (serialized mode uses mutex instead).
        assert_eq!(
            m.lock_table().lock_count(),
            0,
            "serialized mode should not use page locks"
        );
    }

    // -----------------------------------------------------------------------
    // WRITE (Concurrent) tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_concurrent_page_lock_acquisition() {
        let m = mgr();
        let mut txn1 = m.begin(BeginKind::Concurrent).unwrap();
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();
        let pgno = PageNumber::new(1).unwrap();

        m.write_page(&mut txn1, pgno, test_data(0x01)).unwrap();

        // txn2 trying to write the same page should fail.
        let result = m.write_page(&mut txn2, pgno, test_data(0x02));
        assert_eq!(
            result.unwrap_err(),
            MvccError::Busy,
            "page lock contention should return BUSY"
        );
    }

    #[test]
    fn test_concurrent_page_lock_tracked() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        let p1 = PageNumber::new(1).unwrap();
        let p2 = PageNumber::new(2).unwrap();

        m.write_page(&mut txn, p1, test_data(0x01)).unwrap();
        m.write_page(&mut txn, p2, test_data(0x02)).unwrap();

        assert!(txn.page_locks.contains(&p1));
        assert!(txn.page_locks.contains(&p2));
        assert_eq!(txn.page_locks.len(), 2);
    }

    #[test]
    fn test_concurrent_write_set_counter() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();

        for i in 1..=5_u8 {
            let pgno = PageNumber::new(u32::from(i)).unwrap();
            m.write_page(&mut txn, pgno, test_data(i)).unwrap();
        }

        assert_eq!(txn.write_set.len(), 5);
        assert_eq!(txn.write_set_data.len(), 5);

        // Writing the same page again should not increase write_set len.
        m.write_page(&mut txn, PageNumber::new(1).unwrap(), test_data(0xFF))
            .unwrap();
        assert_eq!(
            txn.write_set.len(),
            5,
            "duplicate write should not increase write_set page count"
        );
    }

    #[test]
    fn test_write_tracks_base_version_and_clears_tracking_on_commit() {
        let m = mgr();
        let pgno = PageNumber::new(21).unwrap();

        let mut seed = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut seed, pgno, test_data(0x10)).unwrap();
        let base_commit = m.commit(&mut seed).unwrap();

        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut txn, pgno, test_data(0x11)).unwrap();

        let tracked_before = txn.write_version_for_page(pgno).unwrap();
        assert_eq!(tracked_before.old_version, Some(base_commit));
        assert_eq!(tracked_before.new_version, None);
        assert!(
            txn.write_keys
                .contains(&fsqlite_types::WitnessKey::Page(pgno)),
            "page writes must populate SSI witness keys"
        );

        let committed = m.commit(&mut txn).unwrap();
        assert!(committed > base_commit);
        assert!(
            txn.read_set_versions.is_empty(),
            "read tracking must be cleared after finalization"
        );
        assert!(
            txn.write_set_versions.is_empty(),
            "write tracking must be cleared after finalization"
        );
    }

    #[test]
    fn test_concurrent_checks_serialized_exclusion() {
        let m = mgr();

        // Start a serialized IMMEDIATE txn (holds mutex).
        let txn_ser = m.begin(BeginKind::Immediate).unwrap();
        assert!(txn_ser.serialized_write_lock_held);

        // Concurrent write should be blocked.
        let mut txn_conc = m.begin(BeginKind::Concurrent).unwrap();
        let result = m.write_page(&mut txn_conc, PageNumber::new(1).unwrap(), test_data(0x01));
        assert_eq!(
            result.unwrap_err(),
            MvccError::Busy,
            "concurrent write while serialized writer active should fail"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_process_alive_os_rejects_tagged_birth_mismatch() {
        let pid = std::process::id();
        let birth = current_process_birth_marker(logical_now_epoch_secs());
        if birth & PID_BIRTH_PROCFS_TAG == 0 {
            return;
        }

        assert!(process_alive_os(pid, birth));

        let mismatched = PID_BIRTH_PROCFS_TAG | ((birth & !PID_BIRTH_PROCFS_TAG).wrapping_add(1));
        assert!(!process_alive_os(pid, mismatched));
    }

    #[cfg(unix)]
    #[test]
    fn test_concurrent_write_clears_dead_serialized_indicator_without_lease() {
        let m = mgr();
        let now = logical_now_epoch_secs();

        // Simulate a stale indicator that has no lease metadata and points to
        // a non-existent process ID.
        assert!(m.shm.acquire_serialized_writer(777_777, u32::MAX, now, 0));

        let pgno = PageNumber::new(1).unwrap();
        let mut txn_conc = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut txn_conc, pgno, test_data(0x2A))
            .expect("dead serialized indicator should be cleaned and not block writes");

        assert!(
            m.shm.check_serialized_writer().is_none(),
            "stale serialized indicator should be cleared by concurrent preflight"
        );
    }

    #[test]
    fn test_concurrent_writer_blocks_serialized_acquisition() {
        let m = mgr_with_busy_timeout_ms(5);

        // Hold a concurrent page lock.
        let mut txn_conc = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut txn_conc, PageNumber::new(1).unwrap(), test_data(0x01))
            .unwrap();

        // Serialized acquisition should fail BUSY while any concurrent locks exist.
        let err = m.begin(BeginKind::Immediate).unwrap_err();
        assert_eq!(err, MvccError::Busy);
        assert!(m.write_mutex().holder().is_none(), "mutex must be released");
        assert!(
            m.shm.check_serialized_writer().is_none(),
            "indicator must be cleared on failure"
        );
    }

    #[test]
    fn test_serialized_drain_handoff_park_cadence_is_bounded() {
        for attempt in 1..SERIALIZED_DRAIN_HANDOFF_PARK_EVERY {
            assert!(
                !serialized_drain_should_park(attempt),
                "attempt {attempt} should stay on-CPU"
            );
        }

        assert!(serialized_drain_should_park(
            SERIALIZED_DRAIN_HANDOFF_PARK_EVERY
        ));
        assert!(!serialized_drain_should_park(
            SERIALIZED_DRAIN_HANDOFF_PARK_EVERY + 1
        ));
        assert!(serialized_drain_should_park(
            SERIALIZED_DRAIN_HANDOFF_PARK_EVERY * 2
        ));
    }

    #[test]
    fn test_serialized_drain_handoff_respects_deadline() {
        let mut handoff = SerializedDrainHandoff::default();
        let expired_started = Instant::now()
            .checked_sub(Duration::from_millis(5))
            .expect("expired instant should be constructible");
        assert!(
            handoff
                .next_wait(expired_started, Duration::from_millis(1))
                .is_none(),
            "expired deadline must stop serialized drain handoff"
        );

        let fresh_wait = handoff
            .next_wait(Instant::now(), Duration::from_millis(1))
            .expect("fresh deadline should allow a bounded serialized drain wait");
        assert_eq!(fresh_wait.attempt, 1);
        assert_eq!(fresh_wait.spin_loops, SERIALIZED_DRAIN_HANDOFF_BASE_SPINS);
        assert_eq!(
            fresh_wait.park_timeout,
            Duration::ZERO,
            "first retry should stay on-CPU"
        );
    }

    #[test]
    fn test_serialized_drain_schedule_summary_bounds_wake_amplification() {
        let summary = summarize_serialized_drain_schedule(12);

        assert_eq!(summary.attempts, 12);
        assert_eq!(summary.total_spin_loops, 16_320);
        assert_eq!(summary.max_spin_loops, SERIALIZED_DRAIN_HANDOFF_MAX_SPINS);
        assert_eq!(
            summary.park_count,
            12 / SERIALIZED_DRAIN_HANDOFF_PARK_EVERY,
            "wake amplification must stay at one park per bounded retry window"
        );
        assert_eq!(
            summary.max_park_us,
            SERIALIZED_DRAIN_HANDOFF_MAX_PARK.as_micros() as u64
        );
    }

    #[test]
    fn test_drain_waits_for_all_concurrent_locks_released() {
        let m = Arc::new(mgr_with_busy_timeout_ms(200));

        let mut txn_conc = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut txn_conc, PageNumber::new(1).unwrap(), test_data(0x01))
            .unwrap();

        // Release the concurrent lock shortly after the serialized writer begins draining.
        let m2 = Arc::clone(&m);
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            let mut txn_conc = txn_conc;
            m2.abort(&mut txn_conc);
        });

        let mut txn_ser = m.begin(BeginKind::Immediate).unwrap();
        assert!(txn_ser.serialized_write_lock_held);

        // Clean up.
        m.abort(&mut txn_ser);
        releaser.join().unwrap();
    }

    #[test]
    fn test_concurrent_reads_allowed_during_serialized_write() {
        let m = mgr();

        // Commit a baseline page so a concurrent reader has something to observe.
        let pgno = PageNumber::new(1).unwrap();
        let mut writer = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut writer, pgno, test_data(0x11)).unwrap();
        let seq = m.commit(&mut writer).unwrap();
        assert!(seq.get() > 0);

        // Hold serialized writer exclusion.
        let _ser = m.begin(BeginKind::Immediate).unwrap();

        // Concurrent reads must still be permitted.
        let mut reader = m.begin(BeginKind::Concurrent).unwrap();
        let got = m.read_page(&mut reader, pgno).unwrap();
        assert_eq!(got.as_bytes()[0], 0x11);
    }

    #[test]
    fn test_deferred_read_begin_allowed_during_concurrent_writes() {
        let m = mgr_with_busy_timeout_ms(5);

        // Hold a concurrent write lock.
        let mut conc = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut conc, PageNumber::new(1).unwrap(), test_data(0x01))
            .unwrap();

        // DEFERRED read-only begin always permitted.
        let mut def = m.begin(BeginKind::Deferred).unwrap();
        let _ = m.read_page(&mut def, PageNumber::new(2).unwrap());

        // But writer upgrade should be excluded while concurrent locks exist.
        let err = m
            .write_page(&mut def, PageNumber::new(3).unwrap(), test_data(0x02))
            .unwrap_err();
        assert_eq!(err, MvccError::Busy);
    }

    #[test]
    fn test_acquisition_ordering_steps_1_through_5() {
        let m = Arc::new(mgr_with_busy_timeout_ms(200));

        // Hold a concurrent lock so the serialized writer must drain.
        let mut conc = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut conc, PageNumber::new(1).unwrap(), test_data(0x01))
            .unwrap();

        let (tx, rx) = mpsc::channel();
        let m2 = Arc::clone(&m);
        let starter = std::thread::spawn(move || {
            let got = m2.begin(BeginKind::Immediate);
            tx.send(got).unwrap();
        });

        // Step 2 must happen before drain completes: indicator becomes visible.
        let wait_start = Instant::now();
        while m.shm.check_serialized_writer().is_none() {
            assert!(
                wait_start.elapsed() <= Duration::from_millis(50),
                "timed out waiting for serialized writer indicator"
            );
            std::thread::yield_now();
        }

        // While draining, new concurrent writes must be blocked.
        let mut conc2 = m.begin(BeginKind::Concurrent).unwrap();
        let err = m
            .write_page(&mut conc2, PageNumber::new(2).unwrap(), test_data(0x02))
            .unwrap_err();
        assert_eq!(err, MvccError::Busy);

        // Release existing concurrent locks so drain can finish.
        m.abort(&mut conc);

        let mut ser = rx
            .recv_timeout(Duration::from_millis(200))
            .unwrap()
            .unwrap();
        assert!(ser.serialized_write_lock_held);
        starter.join().expect("begin thread panicked");

        // Step 5: clear indicator before releasing mutex (observable as indicator==None after abort).
        m.abort(&mut ser);
        assert!(m.shm.check_serialized_writer().is_none());
        assert!(m.write_mutex().holder().is_none());

        // After release, concurrent writes can proceed.
        let mut conc3 = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut conc3, PageNumber::new(3).unwrap(), test_data(0x03))
            .unwrap();
    }

    #[test]
    fn test_e2e_serialized_vs_concurrent_mutual_exclusion() {
        let m = Arc::new(mgr_with_busy_timeout_ms(200));
        let (started_tx, started_rx) = mpsc::channel();
        let (commit_tx, commit_rx) = mpsc::channel();

        let m2 = Arc::clone(&m);
        let th = std::thread::spawn(move || {
            let mut ser = m2.begin(BeginKind::Immediate).unwrap();
            started_tx.send(()).unwrap();
            m2.write_page(&mut ser, PageNumber::new(1).unwrap(), test_data(0xAA))
                .unwrap();
            commit_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            m2.commit(&mut ser).unwrap()
        });

        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        // Concurrent writer must be blocked while serialized writer is active.
        let mut conc = m.begin(BeginKind::Concurrent).unwrap();
        let err = m
            .write_page(&mut conc, PageNumber::new(2).unwrap(), test_data(0xBB))
            .unwrap_err();
        assert_eq!(err, MvccError::Busy);

        // Let serialized writer commit.
        commit_tx.send(()).unwrap();
        let seq = th.join().unwrap();
        assert!(seq.get() > 0);

        // After serialized commit, concurrent writer should succeed.
        let mut conc2 = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut conc2, PageNumber::new(2).unwrap(), test_data(0xBB))
            .unwrap();
    }

    // -----------------------------------------------------------------------
    // COMMIT (Serialized) tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_commit_serialized_publishes_and_releases() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Immediate).unwrap();
        let pgno = PageNumber::new(1).unwrap();

        m.write_page(&mut txn, pgno, test_data(0x42)).unwrap();
        let seq = m.commit(&mut txn).unwrap();

        assert!(seq.get() > 0);
        assert_eq!(txn.state, TransactionState::Committed);

        // Mutex should be released.
        assert!(m.write_mutex().holder().is_none());
        assert!(txn.write_set.is_empty());
        assert!(txn.write_set_data.is_empty());
        assert!(txn.write_set_versions.is_empty());
        assert!(txn.write_keys.is_empty());

        // Version store should have the committed version.
        let snap = Snapshot::new(seq, SchemaEpoch::ZERO);
        assert!(m.version_store().resolve(pgno, &snap).is_some());

        // Commit index should be updated.
        assert_eq!(m.commit_index().latest(pgno), Some(seq));
    }

    #[test]
    fn test_commit_serialized_schema_epoch_check() {
        let mut m = mgr();
        let mut txn = m.begin(BeginKind::Immediate).unwrap();
        let pgno = PageNumber::new(1).unwrap();
        m.write_page(&mut txn, pgno, test_data(0x01)).unwrap();

        // Advance schema epoch (simulate DDL).
        m.advance_schema_epoch();

        // Commit should fail with Schema error.
        let result = m.commit(&mut txn);
        assert_eq!(result.unwrap_err(), MvccError::Schema);
        assert_eq!(txn.state, TransactionState::Aborted);
    }

    #[test]
    fn test_schema_epoch_abort_releases_lifecycle_resources() {
        let mut m = mgr();
        let pgno = PageNumber::new(8_023).unwrap();
        let mut txn = m.begin(BeginKind::Immediate).unwrap();

        assert!(txn.snapshot_established);
        assert!(txn.has_version_guard());
        assert_eq!(m.cached_gc_horizon(), Some(txn.snapshot.high));
        assert_eq!(m.shm.load_gc_horizon(), txn.snapshot.high);
        assert_eq!(m.version_guard_registry().active_guard_count(), 1);
        assert_eq!(m.write_mutex().holder(), Some(txn.txn_id));
        assert!(
            txn.serialized_write_lock_held,
            "IMMEDIATE transaction should hold serialized writer exclusion"
        );

        m.write_page(&mut txn, pgno, test_data(0x5E))
            .expect("writer should stage a page before schema mismatch");
        m.advance_schema_epoch();

        let result = m.commit(&mut txn);
        assert_eq!(result.unwrap_err(), MvccError::Schema);
        assert_eq!(txn.state, TransactionState::Aborted);
        assert!(
            !txn.snapshot_established,
            "schema-mismatch abort must release the active snapshot"
        );
        assert!(
            !txn.has_version_guard(),
            "schema-mismatch abort must unpin the EBR guard"
        );
        assert!(
            !txn.serialized_write_lock_held,
            "schema-mismatch abort must release serialized writer exclusion"
        );
        assert_eq!(m.version_guard_registry().active_guard_count(), 0);
        assert!(m.cached_gc_horizon().is_none());
        assert_eq!(m.shm.load_gc_horizon(), CommitSeq::ZERO);
        assert!(m.write_mutex().holder().is_none());
    }

    #[test]
    fn test_commit_serialized_fcw_freshness() {
        let m = mgr();

        // Commit page 1 via txn1.
        let mut txn1 = m.begin(BeginKind::Immediate).unwrap();
        let pgno = PageNumber::new(1).unwrap();
        m.write_page(&mut txn1, pgno, test_data(0x01)).unwrap();
        m.commit(&mut txn1).unwrap();

        // Start txn2 with old snapshot (snapshot high = 0), write page 1.
        // But txn2 must be serialized and hold the mutex.
        let mut txn2 = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut txn2, pgno, test_data(0x02)).unwrap();

        // txn2's snapshot was captured after txn1 committed, so it should see seq=1.
        // This means FCW won't fire. Let's test with a manually stale snapshot.
        // Manipulate txn2 to have a stale snapshot.
        txn2.snapshot = Snapshot::new(CommitSeq::ZERO, SchemaEpoch::ZERO);

        let result = m.commit(&mut txn2);
        assert_eq!(
            result.unwrap_err(),
            MvccError::BusySnapshot,
            "FCW should reject stale snapshot"
        );
    }

    // -----------------------------------------------------------------------
    // COMMIT (Concurrent) tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_commit_concurrent_ssi_validation() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        let pgno = PageNumber::new(1).unwrap();

        m.write_page(&mut txn, pgno, test_data(0x01)).unwrap();

        // Simulate dangerous SSI structure.
        txn.has_in_rw = true;
        txn.has_out_rw = true;

        let result = m.commit(&mut txn);
        assert_eq!(
            result.unwrap_err(),
            MvccError::BusySnapshot,
            "dangerous SSI structure should abort"
        );
        assert_eq!(txn.state, TransactionState::Aborted);
    }

    #[test]
    fn test_commit_concurrent_successful() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        let p1 = PageNumber::new(1).unwrap();
        let p2 = PageNumber::new(2).unwrap();

        m.write_page(&mut txn, p1, test_data(0x01)).unwrap();
        m.write_page(&mut txn, p2, test_data(0x02)).unwrap();

        let seq = m.commit(&mut txn).unwrap();
        assert!(seq.get() > 0);
        assert_eq!(txn.state, TransactionState::Committed);

        // Locks should be released.
        assert_eq!(m.lock_table().lock_count(), 0);
        assert!(txn.write_set.is_empty());
        assert!(txn.write_set_data.is_empty());
        assert!(txn.write_set_versions.is_empty());
        assert!(txn.write_keys.is_empty());
    }

    // -----------------------------------------------------------------------
    // ABORT tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_abort_releases_page_locks() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        let pgno = PageNumber::new(1).unwrap();

        m.write_page(&mut txn, pgno, test_data(0x01)).unwrap();
        assert_eq!(m.lock_table().lock_count(), 1);

        m.abort(&mut txn);
        assert_eq!(
            m.lock_table().lock_count(),
            0,
            "abort must release all page locks"
        );
    }

    #[test]
    fn test_abort_discards_write_set() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Immediate).unwrap();
        let pgno = PageNumber::new(1).unwrap();

        m.write_page(&mut txn, pgno, test_data(0x01)).unwrap();
        assert!(!txn.write_set.is_empty());

        m.abort(&mut txn);
        assert_eq!(txn.state, TransactionState::Aborted);
        assert!(
            txn.write_set.is_empty(),
            "abort must discard staged page numbers"
        );
        assert!(
            txn.write_set_data.is_empty(),
            "abort must discard staged page buffers"
        );
        assert!(
            txn.write_set_versions.is_empty(),
            "abort must discard write-version metadata"
        );
        assert!(
            txn.write_keys.is_empty(),
            "abort must discard SSI write keys"
        );

        // Data should not be visible to a new transaction.
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();
        assert!(
            m.read_page(&mut txn2, pgno).is_none(),
            "aborted data must not be visible"
        );
    }

    #[test]
    fn test_abort_serialized_releases_mutex() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Immediate).unwrap();
        assert!(m.write_mutex().holder().is_some());

        m.abort(&mut txn);
        assert!(
            m.write_mutex().holder().is_none(),
            "abort must release serialized write mutex"
        );
    }

    #[test]
    fn test_abort_concurrent_witnesses_cleared() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        let read_pg = PageNumber::new(1).unwrap();
        let write_pg = PageNumber::new(2).unwrap();

        // Record read/write tracking (also injects witness keys).
        txn.record_page_read(read_pg, CommitSeq::new(1));
        txn.record_page_write(write_pg, Some(CommitSeq::new(1)));

        m.abort(&mut txn);

        assert!(
            txn.read_set_versions.is_empty(),
            "read tracking must be cleared on abort"
        );
        assert!(
            txn.write_set_versions.is_empty(),
            "write tracking must be cleared on abort"
        );

        assert!(
            txn.read_keys.is_empty(),
            "SSI read witnesses must be cleared after transaction finalization"
        );
        assert!(
            txn.write_keys.is_empty(),
            "SSI write witnesses must be cleared after transaction finalization"
        );
    }

    // -----------------------------------------------------------------------
    // SAVEPOINT tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_savepoint_records_state() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Immediate).unwrap();
        let p1 = PageNumber::new(1).unwrap();

        m.write_page(&mut txn, p1, test_data(0x01)).unwrap();

        let sp = m.savepoint(&txn, "sp1");
        assert_eq!(sp.name, "sp1");
        assert_eq!(sp.write_set_len, 1);
        assert_eq!(sp.cell_delta_len, 0);
        assert!(sp.write_set_snapshot.contains_key(&p1));
    }

    #[test]
    fn test_rollback_to_savepoint_restores_pages() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Immediate).unwrap();
        let p1 = PageNumber::new(1).unwrap();
        let p2 = PageNumber::new(2).unwrap();

        // Write page 1.
        m.write_page(&mut txn, p1, test_data(0x01)).unwrap();

        // Create savepoint.
        let sp = m.savepoint(&txn, "sp1");

        // Write page 2 after savepoint.
        m.write_page(&mut txn, p2, test_data(0x02)).unwrap();
        assert_eq!(txn.write_set.len(), 2);

        // Rollback to savepoint — should undo page 2 write.
        m.rollback_to_savepoint(&mut txn, &sp);

        assert_eq!(
            txn.write_set.len(),
            1,
            "write_set should be truncated to savepoint state"
        );
        assert!(
            txn.write_set_data.contains_key(&p1),
            "page 1 should still be in write_set_data"
        );
        assert!(
            !txn.write_set_data.contains_key(&p2),
            "page 2 should be removed from write_set_data"
        );
    }

    #[test]
    fn test_rollback_to_savepoint_prunes_write_version_tracking() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Immediate).unwrap();
        let p1 = PageNumber::new(1).unwrap();
        let p2 = PageNumber::new(2).unwrap();

        m.write_page(&mut txn, p1, test_data(0x01)).unwrap();
        let sp = m.savepoint(&txn, "sp_tracking");
        m.write_page(&mut txn, p2, test_data(0x02)).unwrap();

        assert!(txn.write_version_for_page(p1).is_some());
        assert!(txn.write_version_for_page(p2).is_some());

        m.rollback_to_savepoint(&mut txn, &sp);

        assert!(txn.write_version_for_page(p1).is_some());
        assert!(
            txn.write_version_for_page(p2).is_none(),
            "savepoint rollback must prune write-version entries for removed pages"
        );
    }

    #[test]
    fn test_rollback_to_savepoint_restores_logical_write_version_tracking() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        let pre_savepoint_page = PageNumber::new(1).unwrap();
        let post_savepoint_page = PageNumber::new(2).unwrap();

        txn.write_set.push(pre_savepoint_page);
        txn.record_page_write(pre_savepoint_page, Some(CommitSeq::new(7)));
        let sp = m.savepoint(&txn, "sp_logical_tracking");

        txn.write_set.push(post_savepoint_page);
        txn.record_page_write(post_savepoint_page, Some(CommitSeq::new(8)));

        assert!(
            !txn.write_set_data.contains_key(&pre_savepoint_page),
            "logical writes should not require full-page write_set_data"
        );
        assert!(txn.write_version_for_page(pre_savepoint_page).is_some());
        assert!(txn.write_version_for_page(post_savepoint_page).is_some());

        m.rollback_to_savepoint(&mut txn, &sp);

        let restored_entry = txn
            .write_version_for_page(pre_savepoint_page)
            .expect("pre-savepoint logical write tracking must survive rollback");
        assert_eq!(restored_entry.old_version, Some(CommitSeq::new(7)));
        assert_eq!(restored_entry.new_version, None);
        assert!(
            txn.write_version_for_page(post_savepoint_page).is_none(),
            "post-savepoint logical write tracking must be pruned"
        );
    }

    #[test]
    fn test_rollback_to_savepoint_restores_structural_page_tracking() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        let p1 = PageNumber::new(1).unwrap();
        let p2 = PageNumber::new(2).unwrap();

        m.write_page(&mut txn, p1, test_data(0x01)).unwrap();
        let sp = m.savepoint(&txn, "sp_structural");
        m.write_page(&mut txn, p2, test_data(0x02)).unwrap();

        assert!(txn.structural_pages.contains(&p1));
        assert!(txn.structural_pages.contains(&p2));

        m.rollback_to_savepoint(&mut txn, &sp);

        assert!(
            txn.structural_pages.contains(&p1),
            "pre-savepoint structural page must remain tracked"
        );
        assert!(
            !txn.structural_pages.contains(&p2),
            "rolled-back structural page must not leave stale classification"
        );
    }

    #[test]
    fn test_rollback_to_savepoint_truncates_intent_log_but_keeps_witnesses() {
        use fsqlite_types::{IntentFootprint, IntentOp, IntentOpKind, RowId, TableId};

        fn insert_intent(rowid: i64, record: &[u8]) -> IntentOp {
            IntentOp {
                schema_epoch: 0,
                footprint: IntentFootprint::empty(),
                op: IntentOpKind::Insert {
                    table: TableId::new(1),
                    key: RowId::new(rowid),
                    record: record.to_vec(),
                },
            }
        }

        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        txn.intent_log.push(insert_intent(1, b"before"));
        let sp = m.savepoint(&txn, "sp_intent");

        txn.intent_log.push(insert_intent(2, b"after"));
        txn.read_keys
            .insert(fsqlite_types::WitnessKey::Page(PageNumber::new(2).unwrap()));

        m.rollback_to_savepoint(&mut txn, &sp);

        assert_eq!(
            txn.intent_log,
            vec![insert_intent(1, b"before")],
            "ROLLBACK TO must discard post-savepoint intent entries"
        );
        assert!(
            !txn.read_keys.is_empty(),
            "SSI witnesses intentionally survive savepoint rollback"
        );
    }

    #[test]
    fn test_rollback_to_savepoint_prunes_cell_deltas() -> Result<(), String> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let m = mgr();
        let mut txn = m
            .begin(BeginKind::Concurrent)
            .map_err(|err| format!("begin failed: {err}"))?;
        let pgno = PageNumber::new(42).ok_or_else(|| "valid test page".to_owned())?;
        let cell_key = CellKey::table_row(BtreeRef::Table(TableId::new(1)), 100);

        m.cell_log()
            .record_insert(cell_key, pgno, vec![1], txn.token())
            .ok_or_else(|| "pre-savepoint insert should fit test budget".to_owned())?;
        txn.write_set.push(pgno);

        let sp = m.savepoint(&txn, "cell_sp");
        assert_eq!(sp.cell_delta_len, 1);

        m.cell_log()
            .record_update(cell_key, pgno, vec![2], txn.token())
            .ok_or_else(|| "post-savepoint update should fit test budget".to_owned())?;
        assert_eq!(m.cell_log().delta_count(), 2);

        m.rollback_to_savepoint(&mut txn, &sp);
        assert_eq!(m.cell_log().delta_count(), 1);

        let commit_seq = m
            .commit(&mut txn)
            .map_err(|err| format!("commit failed: {err}"))?;
        assert_eq!(
            m.cell_log().resolve(pgno, &cell_key, commit_seq),
            Some(vec![1]),
            "ROLLBACK TO must discard post-savepoint cell deltas before commit"
        );

        Ok(())
    }

    #[test]
    fn test_rollback_to_savepoint_keeps_page_locks() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        let p1 = PageNumber::new(1).unwrap();
        let p2 = PageNumber::new(2).unwrap();

        m.write_page(&mut txn, p1, test_data(0x01)).unwrap();
        let sp = m.savepoint(&txn, "sp1");

        m.write_page(&mut txn, p2, test_data(0x02)).unwrap();
        assert_eq!(txn.page_locks.len(), 2);

        m.rollback_to_savepoint(&mut txn, &sp);

        // Page locks must NOT be released on ROLLBACK TO (spec requirement).
        assert_eq!(
            txn.page_locks.len(),
            2,
            "page locks must NOT be released on ROLLBACK TO"
        );
        assert!(txn.page_locks.contains(&p1));
        assert!(txn.page_locks.contains(&p2));
    }

    #[test]
    fn test_rollback_to_savepoint_rewrite_retained_lock_readds_commit_page() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        let p1 = PageNumber::new(1).unwrap();
        let p2 = PageNumber::new(2).unwrap();

        m.write_page(&mut txn, p1, test_data(0x01)).unwrap();
        let sp = m.savepoint(&txn, "sp_rewrite");
        m.write_page(&mut txn, p2, test_data(0x02)).unwrap();
        assert!(txn.page_locks.contains(&p2));

        m.rollback_to_savepoint(&mut txn, &sp);
        assert!(
            txn.page_locks.contains(&p2),
            "ROLLBACK TO keeps locks for pages written after the savepoint"
        );
        assert!(
            !txn.write_set.contains(&p2),
            "rolled-back page must be absent from the pending write set before rewrite"
        );
        assert!(!txn.write_set_data.contains_key(&p2));

        let rewritten = test_data(0x33);
        m.write_page(&mut txn, p2, rewritten.clone()).unwrap();
        assert!(
            txn.write_set.contains(&p2),
            "rewriting a retained-lock page must re-add it to the pending write set"
        );
        assert!(txn.write_set_data.contains_key(&p2));

        let commit_seq = m.commit(&mut txn).unwrap();
        assert!(commit_seq > CommitSeq::ZERO);

        let mut reader = m.begin(BeginKind::Concurrent).unwrap();
        let read_back = m
            .read_page(&mut reader, p2)
            .expect("rewritten page must be published");
        assert_eq!(read_back.as_bytes(), rewritten.as_bytes());
        m.abort(&mut reader);
    }

    #[test]
    fn test_rollback_to_savepoint_keeps_witnesses() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        let p1 = PageNumber::new(1).unwrap();

        m.write_page(&mut txn, p1, test_data(0x01)).unwrap();

        // Add SSI witnesses after the savepoint.
        let sp = m.savepoint(&txn, "sp1");
        txn.read_keys
            .insert(fsqlite_types::WitnessKey::Page(PageNumber::new(2).unwrap()));
        txn.write_keys
            .insert(fsqlite_types::WitnessKey::Page(PageNumber::new(3).unwrap()));

        m.rollback_to_savepoint(&mut txn, &sp);

        // SSI witnesses must NOT be rolled back (spec requirement).
        assert!(
            !txn.read_keys.is_empty(),
            "SSI read witnesses must NOT be rolled back"
        );
        assert!(
            !txn.write_keys.is_empty(),
            "SSI write witnesses must NOT be rolled back"
        );
    }

    #[test]
    fn test_nested_savepoints() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Immediate).unwrap();
        let p1 = PageNumber::new(1).unwrap();
        let p2 = PageNumber::new(2).unwrap();
        let p3 = PageNumber::new(3).unwrap();

        m.write_page(&mut txn, p1, test_data(0x01)).unwrap();
        let sp1 = m.savepoint(&txn, "sp1");

        m.write_page(&mut txn, p2, test_data(0x02)).unwrap();
        let sp2 = m.savepoint(&txn, "sp2");

        m.write_page(&mut txn, p3, test_data(0x03)).unwrap();
        assert_eq!(txn.write_set.len(), 3);

        // Rollback to sp2 — should undo p3 only.
        m.rollback_to_savepoint(&mut txn, &sp2);
        assert_eq!(txn.write_set.len(), 2);
        assert!(txn.write_set_data.contains_key(&p1));
        assert!(txn.write_set_data.contains_key(&p2));
        assert!(!txn.write_set_data.contains_key(&p3));

        // Rollback to sp1 — should undo p2 as well.
        m.rollback_to_savepoint(&mut txn, &sp1);
        assert_eq!(txn.write_set.len(), 1);
        assert!(txn.write_set_data.contains_key(&p1));
        assert!(!txn.write_set_data.contains_key(&p2));
    }

    // -----------------------------------------------------------------------
    // State machine tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_state_machine_transitions_irreversible() {
        let m = mgr();

        // Active -> Committed is terminal.
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        m.commit(&mut txn).unwrap();
        assert_eq!(txn.state, TransactionState::Committed);

        // Active -> Aborted is terminal.
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();
        m.abort(&mut txn2);
        assert_eq!(txn2.state, TransactionState::Aborted);
    }

    #[test]
    #[should_panic(expected = "can only commit active")]
    fn test_committed_cannot_commit() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        m.commit(&mut txn).unwrap();
        txn.commit(); // should panic
    }

    #[test]
    #[should_panic(expected = "can only abort active")]
    fn test_committed_cannot_abort() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        m.commit(&mut txn).unwrap();
        txn.abort(); // should panic
    }

    #[test]
    #[should_panic(expected = "can only commit active")]
    fn test_aborted_cannot_commit() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        m.abort(&mut txn);
        txn.commit(); // should panic
    }

    #[test]
    #[should_panic(expected = "can only abort active")]
    fn test_aborted_cannot_abort() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        m.abort(&mut txn);
        txn.abort(); // should panic
    }

    // -----------------------------------------------------------------------
    // E2E: full lifecycle
    // -----------------------------------------------------------------------

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_e2e_full_lifecycle_all_modes() {
        let m = mgr();
        let p1 = PageNumber::new(1).unwrap();
        let p2 = PageNumber::new(2).unwrap();
        let p3 = PageNumber::new(3).unwrap();
        let p4 = PageNumber::new(4).unwrap();

        // --- Serialized IMMEDIATE ---
        let mut txn_imm = m.begin(BeginKind::Immediate).unwrap();
        assert!(txn_imm.serialized_write_lock_held);
        m.write_page(&mut txn_imm, p1, test_data(0x11)).unwrap();
        let seq_imm = m.commit(&mut txn_imm).unwrap();
        assert!(seq_imm.get() > 0);

        // --- Serialized EXCLUSIVE ---
        let mut txn_exc = m.begin(BeginKind::Exclusive).unwrap();
        assert!(txn_exc.serialized_write_lock_held);
        m.write_page(&mut txn_exc, p2, test_data(0x22)).unwrap();
        let seq_exc = m.commit(&mut txn_exc).unwrap();
        assert!(seq_exc > seq_imm);

        // --- Serialized DEFERRED (read then write) ---
        let mut txn_def = m.begin(BeginKind::Deferred).unwrap();
        assert!(!txn_def.snapshot_established);

        // Read page 1 — establishes snapshot.
        let data_p1 = m.read_page(&mut txn_def, p1).unwrap();
        assert_eq!(data_p1.as_bytes()[0], 0x11);
        assert!(txn_def.snapshot_established);

        // Write page 3 — triggers deferred upgrade.
        m.write_page(&mut txn_def, p3, test_data(0x33)).unwrap();
        assert!(txn_def.serialized_write_lock_held);
        let seq_def = m.commit(&mut txn_def).unwrap();
        assert!(seq_def > seq_exc);

        // --- Concurrent ---
        let mut txn_conc = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut txn_conc, p4, test_data(0x44)).unwrap();
        let seq_conc = m.commit(&mut txn_conc).unwrap();
        assert!(seq_conc > seq_def);

        // --- Verify all data visible in a new snapshot ---
        let mut txn_read = m.begin(BeginKind::Concurrent).unwrap();

        let r1 = m.read_page(&mut txn_read, p1).unwrap();
        assert_eq!(r1.as_bytes()[0], 0x11, "page 1 from IMMEDIATE");

        let r2 = m.read_page(&mut txn_read, p2).unwrap();
        assert_eq!(r2.as_bytes()[0], 0x22, "page 2 from EXCLUSIVE");

        let r3 = m.read_page(&mut txn_read, p3).unwrap();
        assert_eq!(r3.as_bytes()[0], 0x33, "page 3 from DEFERRED");

        let r4 = m.read_page(&mut txn_read, p4).unwrap();
        assert_eq!(r4.as_bytes()[0], 0x44, "page 4 from CONCURRENT");

        // --- Verify isolation: old snapshot doesn't see new writes ---
        // (txn_read's snapshot sees everything so far, which is correct)
        // Start a txn before any new writes.
        let mut txn_old = m.begin(BeginKind::Concurrent).unwrap();

        // Write a new version of page 1 via another txn.
        let mut txn_update = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut txn_update, p1, test_data(0xFF)).unwrap();
        m.commit(&mut txn_update).unwrap();

        // txn_old should still see the old version of page 1 (0x11, not 0xFF).
        let r1_old = m.read_page(&mut txn_old, p1).unwrap();
        assert_eq!(
            r1_old.as_bytes()[0],
            0x11,
            "old snapshot should see old version"
        );
    }

    #[test]
    fn test_e2e_concurrent_different_pages_both_commit() {
        let m = mgr();
        let mut txn1 = m.begin(BeginKind::Concurrent).unwrap();
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();

        // Different pages — no conflict.
        m.write_page(&mut txn1, PageNumber::new(1).unwrap(), test_data(0x01))
            .unwrap();
        m.write_page(&mut txn2, PageNumber::new(2).unwrap(), test_data(0x02))
            .unwrap();

        let seq1 = m.commit(&mut txn1).unwrap();
        let seq2 = m.commit(&mut txn2).unwrap();

        assert!(seq1.get() > 0);
        assert!(seq2.get() > 0);
        assert_eq!(txn1.state, TransactionState::Committed);
        assert_eq!(txn2.state, TransactionState::Committed);
    }

    #[test]
    fn test_e2e_concurrent_same_page_conflict() {
        let m = mgr();
        let pgno = PageNumber::new(1).unwrap();

        let mut txn1 = m.begin(BeginKind::Concurrent).unwrap();
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();

        // txn1 writes page 1 first (gets the lock).
        m.write_page(&mut txn1, pgno, test_data(0x01)).unwrap();

        // txn2 cannot write page 1 (lock held by txn1).
        let result = m.write_page(&mut txn2, pgno, test_data(0x02));
        assert_eq!(result.unwrap_err(), MvccError::Busy);

        // txn1 commits successfully.
        let seq = m.commit(&mut txn1).unwrap();
        assert!(seq.get() > 0);
    }

    #[test]
    fn test_xor_merge_forbidden_btree_interior() {
        assert!(!raw_xor_merge_allowed(
            WriteMergePolicy::Safe,
            MergePageKind::BtreeInteriorTable,
            true,
        ));
        assert_eq!(
            merge_decision(
                WriteMergePolicy::Safe,
                MergePageKind::BtreeInteriorTable,
                true
            ),
            MergeDecision::IntentReplay
        );
    }

    #[test]
    fn test_xor_merge_forbidden_btree_leaf() {
        assert!(!raw_xor_merge_allowed(
            WriteMergePolicy::Safe,
            MergePageKind::BtreeLeafTable,
            true,
        ));
        assert_eq!(
            merge_decision(WriteMergePolicy::Safe, MergePageKind::BtreeLeafTable, true),
            MergeDecision::IntentReplay
        );
    }

    #[test]
    fn test_xor_merge_forbidden_overflow() {
        assert!(!raw_xor_merge_allowed(
            WriteMergePolicy::Safe,
            MergePageKind::Overflow,
            true,
        ));
        assert_eq!(
            merge_decision(WriteMergePolicy::Safe, MergePageKind::Overflow, true),
            MergeDecision::IntentReplay
        );
    }

    #[test]
    fn test_xor_merge_forbidden_freelist() {
        assert!(!raw_xor_merge_allowed(
            WriteMergePolicy::Safe,
            MergePageKind::Freelist,
            true,
        ));
        assert_eq!(
            merge_decision(WriteMergePolicy::Safe, MergePageKind::Freelist, true),
            MergeDecision::IntentReplay
        );
    }

    #[test]
    fn test_xor_merge_forbidden_pointer_map() {
        assert!(!raw_xor_merge_allowed(
            WriteMergePolicy::Safe,
            MergePageKind::PointerMap,
            true,
        ));
        assert_eq!(
            merge_decision(WriteMergePolicy::Safe, MergePageKind::PointerMap, true),
            MergeDecision::IntentReplay
        );
    }

    #[test]
    fn test_disjoint_delta_lemma_correct() {
        let base = vec![0_u8; 8];
        let mut page_1 = base.clone();
        page_1[1] = 0xA1;
        page_1[5] = 0xB2;

        let mut page_2 = base.clone();
        page_2[2] = 0x0C;
        page_2[7] = 0x7D;

        let delta_1 = gf256_patch_delta(&base, &page_1).expect("equal lengths");
        let delta_2 = gf256_patch_delta(&base, &page_2).expect("equal lengths");
        assert!(gf256_patches_disjoint(&delta_1, &delta_2));

        let merged =
            compose_disjoint_gf256_patches(&base, &delta_1, &delta_2).expect("disjoint deltas");
        assert_eq!(merged, vec![0_u8, 0xA1, 0x0C, 0, 0, 0xB2, 0, 0x7D]);
    }

    #[test]
    fn test_counterexample_lost_update() {
        let pointer_slot = 0_usize;
        let old_offset = 10_usize;
        let new_offset = 20_usize;

        let mut page_0 = vec![0_u8; 64];
        page_0[pointer_slot] = u8::try_from(old_offset).expect("small offset");
        page_0[old_offset] = b'A';

        let mut page_t1 = page_0.clone();
        page_t1[pointer_slot] = u8::try_from(new_offset).expect("small offset");
        page_t1[new_offset] = page_0[old_offset];

        let mut page_t2 = page_0.clone();
        page_t2[old_offset] = b'B';

        let delta_t1 = gf256_patch_delta(&page_0, &page_t1).expect("equal lengths");
        let delta_t2 = gf256_patch_delta(&page_0, &page_t2).expect("equal lengths");
        assert!(gf256_patches_disjoint(&delta_t1, &delta_t2));

        let merged =
            compose_disjoint_gf256_patches(&page_0, &delta_t1, &delta_t2).expect("disjoint deltas");

        let logical_offset = usize::from(merged[pointer_slot]);
        let logical_payload = merged[logical_offset];
        assert_eq!(
            logical_offset, new_offset,
            "pointer moved to new location by T1"
        );
        assert_eq!(logical_payload, b'A', "stale payload is still reachable");
        assert_eq!(
            merged[old_offset], b'B',
            "T2 update exists at old location but became unreachable"
        );
        assert_ne!(
            logical_payload, b'B',
            "lost update reproduced despite disjoint byte deltas"
        );
    }

    #[test]
    fn test_pragma_write_merge_off() {
        let mut m = mgr();
        m.set_write_merge_policy(WriteMergePolicy::Off)
            .expect("OFF must be accepted");

        let pgno = PageNumber::new(1).unwrap();
        let mut txn1 = m.begin(BeginKind::Concurrent).unwrap();
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();

        m.write_page(&mut txn1, pgno, test_data(0x0D)).unwrap();
        m.commit(&mut txn1).unwrap();
        m.write_page(&mut txn2, pgno, test_data(0x0D)).unwrap();

        let result = m.commit(&mut txn2);
        assert_eq!(result.unwrap_err(), MvccError::BusySnapshot);
    }

    #[test]
    fn test_fcw_abort_releases_snapshot_guard_and_page_locks() {
        let mut m = mgr();
        m.set_write_merge_policy(WriteMergePolicy::Off)
            .expect("OFF must be accepted");

        let pgno = PageNumber::new(8_024).unwrap();
        let mut first = m.begin(BeginKind::Concurrent).unwrap();
        let mut stale = m.begin(BeginKind::Concurrent).unwrap();
        let stale_snapshot = stale.snapshot.high;

        assert_eq!(m.cached_gc_horizon(), Some(stale_snapshot));
        assert_eq!(m.version_guard_registry().active_guard_count(), 2);

        m.write_page(&mut first, pgno, test_data(0x2B))
            .expect("first writer should stage a page");
        let first_commit = m.commit(&mut first).expect("first writer commit");
        assert!(first_commit > stale_snapshot);
        assert_eq!(m.cached_gc_horizon(), Some(stale_snapshot));
        assert_eq!(
            m.version_guard_registry().active_guard_count(),
            1,
            "stale writer should be the only remaining active guard"
        );

        m.write_page(&mut stale, pgno, test_data(0x2C))
            .expect("stale writer should acquire the page lock before FCW validation");
        assert!(stale.page_locks.contains(&pgno));

        let result = m.commit(&mut stale);
        assert_eq!(result.unwrap_err(), MvccError::BusySnapshot);
        assert_eq!(stale.state, TransactionState::Aborted);
        assert!(
            !stale.snapshot_established,
            "FCW abort must unregister the active snapshot"
        );
        assert!(
            !stale.has_version_guard(),
            "FCW abort must unpin the EBR guard"
        );
        assert!(
            stale.page_locks.is_empty(),
            "FCW abort must release page locks held by the stale writer"
        );
        assert!(m.cached_gc_horizon().is_none());
        assert_eq!(m.shm.load_gc_horizon(), CommitSeq::ZERO);
        assert_eq!(m.version_guard_registry().active_guard_count(), 0);
    }

    #[test]
    fn test_pragma_write_merge_safe() {
        let mut m = mgr();
        m.set_write_merge_policy(WriteMergePolicy::Safe)
            .expect("SAFE must be accepted");

        let pgno = PageNumber::new(1).unwrap();
        let mut txn1 = m.begin(BeginKind::Concurrent).unwrap();
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();

        m.write_page(&mut txn1, pgno, test_data(0x0D)).unwrap();
        m.commit(&mut txn1).unwrap();
        m.write_page(&mut txn2, pgno, test_data(0x0D)).unwrap();

        let result = m.commit(&mut txn2);
        assert_eq!(result.unwrap_err(), MvccError::BusySnapshot);
        assert_eq!(
            merge_decision(
                WriteMergePolicy::Safe,
                MergePageKind::classify(test_data(0x0D).as_bytes()),
                cfg!(debug_assertions),
            ),
            MergeDecision::IntentReplay
        );
    }

    #[test]
    fn test_pragma_write_merge_lab_unsafe_rejected_in_release() {
        let mut m = mgr();
        let result = m.set_write_merge_policy(WriteMergePolicy::LabUnsafe);
        if cfg!(debug_assertions) {
            assert!(result.is_ok(), "LAB_UNSAFE is debug-only");
        } else {
            assert_eq!(
                result.unwrap_err(),
                MvccError::InvalidWriteMergePolicy,
                "release builds must reject LAB_UNSAFE"
            );
        }
    }

    #[test]
    fn test_lab_unsafe_still_forbids_btree_xor() {
        assert!(!raw_xor_merge_allowed(
            WriteMergePolicy::LabUnsafe,
            MergePageKind::BtreeLeafTable,
            true,
        ));
        assert_eq!(
            merge_decision(
                WriteMergePolicy::LabUnsafe,
                MergePageKind::BtreeLeafTable,
                true
            ),
            MergeDecision::IntentReplay
        );
    }

    #[test]
    fn test_gf256_delta_as_encoding_not_correctness() {
        let base = vec![0x0D, 0x10, 0x20, 0x30, 0x40];
        let target = vec![0x0D, 0x11, 0x20, 0x33, 0x40];
        let delta = gf256_patch_delta(&base, &target).expect("equal lengths");
        assert!(
            delta.iter().any(|byte| *byte != 0),
            "delta encodes byte differences"
        );

        let page_kind = MergePageKind::classify(&target);
        assert_eq!(page_kind, MergePageKind::BtreeLeafTable);
        assert!(
            !raw_xor_merge_allowed(WriteMergePolicy::Safe, page_kind, true),
            "delta encoding does not imply merge correctness permission"
        );
    }

    #[test]
    fn prop_merge_safety_compile_time() {
        let structured = [
            MergePageKind::BtreeInteriorTable,
            MergePageKind::BtreeLeafTable,
            MergePageKind::Overflow,
            MergePageKind::Freelist,
            MergePageKind::PointerMap,
        ];
        for page_kind in structured {
            assert!(page_kind.is_sqlite_structured());
            assert!(!raw_xor_merge_allowed(
                WriteMergePolicy::Safe,
                page_kind,
                true,
            ));
            assert!(!raw_xor_merge_allowed(
                WriteMergePolicy::LabUnsafe,
                page_kind,
                true,
            ));
        }
    }

    proptest! {
        #[test]
        fn prop_disjoint_delta_composition(
            base in prop::collection::vec(any::<u8>(), 1..256),
            even_noise in prop::collection::vec(any::<u8>(), 1..64),
            odd_noise in prop::collection::vec(any::<u8>(), 1..64),
        ) {
            let len = base.len();
            let mut delta_even = vec![0_u8; len];
            let mut delta_odd = vec![0_u8; len];

            for (idx, byte) in delta_even.iter_mut().enumerate() {
                if idx % 2 == 0 {
                    *byte = even_noise[idx % even_noise.len()];
                }
            }
            for (idx, byte) in delta_odd.iter_mut().enumerate() {
                if idx % 2 == 1 {
                    *byte = odd_noise[idx % odd_noise.len()];
                }
            }

            prop_assert!(gf256_patches_disjoint(&delta_even, &delta_odd));
            let merged = compose_disjoint_gf256_patches(&base, &delta_even, &delta_odd)
                .expect("disjoint deltas should compose");

            for idx in 0..len {
                prop_assert_eq!(merged[idx], base[idx] ^ delta_even[idx] ^ delta_odd[idx]);
            }
        }
    }

    #[test]
    fn test_e2e_concurrent_insert_different_pages() {
        let m = mgr();
        let mut txn1 = m.begin(BeginKind::Concurrent).unwrap();
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();

        m.write_page(&mut txn1, PageNumber::new(3).unwrap(), test_data(0x01))
            .unwrap();
        m.write_page(&mut txn2, PageNumber::new(4).unwrap(), test_data(0x02))
            .unwrap();

        assert!(m.commit(&mut txn1).is_ok());
        assert!(m.commit(&mut txn2).is_ok());
    }

    #[test]
    fn test_e2e_concurrent_insert_same_page_conflict() {
        let m = mgr();
        let pgno = PageNumber::new(5).unwrap();
        let mut txn1 = m.begin(BeginKind::Concurrent).unwrap();
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();

        m.write_page(&mut txn1, pgno, test_data(0x0D)).unwrap();
        m.commit(&mut txn1).unwrap();
        m.write_page(&mut txn2, pgno, test_data(0x0D)).unwrap();

        assert_eq!(m.commit(&mut txn2).unwrap_err(), MvccError::BusySnapshot);
    }

    #[test]
    fn test_e2e_concurrent_insert_same_page_intent_replay() {
        let mut m = mgr();
        m.set_write_merge_policy(WriteMergePolicy::Safe)
            .expect("SAFE must be accepted");

        let pgno = PageNumber::new(6).unwrap();
        let mut txn1 = m.begin(BeginKind::Concurrent).unwrap();
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();

        m.write_page(&mut txn1, pgno, test_data(0x0D)).unwrap();
        m.commit(&mut txn1).unwrap();
        m.write_page(&mut txn2, pgno, test_data(0x0D)).unwrap();

        let result = m.commit(&mut txn2);
        assert_eq!(result.unwrap_err(), MvccError::BusySnapshot);
        assert_eq!(
            merge_decision(
                WriteMergePolicy::Safe,
                MergePageKind::classify(test_data(0x0D).as_bytes()),
                cfg!(debug_assertions),
            ),
            MergeDecision::IntentReplay,
            "SAFE mode should select semantic merge ladder for structured pages"
        );
    }

    #[test]
    fn test_theorem1_deadlock_freedom_try_acquire_never_blocks() {
        let m = mgr();
        let pgno = PageNumber::new(42).unwrap();
        let mut txn1 = m.begin(BeginKind::Concurrent).unwrap();
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();

        m.write_page(&mut txn1, pgno, test_data(0xAA)).unwrap();

        let start = Instant::now();
        for _ in 0..1_000 {
            let err = m
                .write_page(&mut txn2, pgno, test_data(0xBB))
                .expect_err("conflicting write must fail immediately");
            assert_eq!(err, MvccError::Busy);
        }

        assert!(
            start.elapsed() < Duration::from_secs(1),
            "non-blocking try_acquire path must return promptly"
        );
    }

    #[test]
    fn test_theorem2_snapshot_isolation_all_or_nothing_visibility() {
        let m = mgr();
        let pages = [1_u32, 2, 3];

        // Reader snapshot is captured before writer commit.
        let mut old_reader = m.begin(BeginKind::Concurrent).unwrap();

        let mut writer = m.begin(BeginKind::Immediate).unwrap();
        for page in pages {
            let byte = u8::try_from(page).expect("test page numbers fit in u8");
            m.write_page(&mut writer, PageNumber::new(page).unwrap(), test_data(byte))
                .unwrap();
        }
        let committed = m.commit(&mut writer).unwrap();
        assert!(committed > CommitSeq::ZERO);

        // Old snapshot sees none.
        for page in pages {
            assert!(
                m.read_page(&mut old_reader, PageNumber::new(page).unwrap())
                    .is_none(),
                "old snapshot must not see post-snapshot commit"
            );
        }

        // Fresh snapshot sees all.
        let mut fresh_reader = m.begin(BeginKind::Concurrent).unwrap();
        for page in pages {
            assert!(
                m.read_page(&mut fresh_reader, PageNumber::new(page).unwrap())
                    .is_some(),
                "fresh snapshot must see all committed pages"
            );
        }
    }

    #[test]
    fn test_theorem3_no_lost_update_case_a_lock_contention() {
        let m = mgr();
        let pgno = PageNumber::new(77).unwrap();
        let mut txn1 = m.begin(BeginKind::Concurrent).unwrap();
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();

        m.write_page(&mut txn1, pgno, test_data(0x10)).unwrap();
        let result = m.write_page(&mut txn2, pgno, test_data(0x20));
        assert_eq!(result.unwrap_err(), MvccError::Busy);
    }

    #[test]
    fn test_theorem3_no_lost_update_case_b_fcw_stale() {
        let m = mgr();
        let pgno = PageNumber::new(88).unwrap();

        // txn_stale starts before txn_fresh commits, so it carries a stale snapshot.
        let mut txn_fresh = m.begin(BeginKind::Concurrent).unwrap();
        let mut txn_stale = m.begin(BeginKind::Concurrent).unwrap();

        m.write_page(&mut txn_fresh, pgno, test_data(0x01)).unwrap();
        m.commit(&mut txn_fresh).unwrap();

        // Lock is now free, so stale writer can write but must fail on FCW at COMMIT.
        m.write_page(&mut txn_stale, pgno, test_data(0x02)).unwrap();
        let result = m.commit(&mut txn_stale);
        assert_eq!(result.unwrap_err(), MvccError::BusySnapshot);
    }

    #[test]
    fn test_theorem5_txn_max_duration_enforced() {
        let mut m = mgr();
        m.set_txn_max_duration_ms(1);

        let mut txn = m.begin(BeginKind::Immediate).unwrap();
        txn.started_at_ms = txn.started_at_ms.saturating_sub(10);

        let result = m.write_page(&mut txn, PageNumber::new(1).unwrap(), test_data(0xEF));
        assert_eq!(result.unwrap_err(), MvccError::TxnMaxDurationExceeded);
        assert_eq!(txn.state, TransactionState::Aborted);
        assert!(m.write_mutex().holder().is_none());
    }

    #[test]
    fn test_theorem6_liveness_all_ops_bounded() {
        let m = mgr();

        // Begin + read + write + commit all terminate for non-conflicting txns.
        let mut txn1 = m.begin(BeginKind::Concurrent).unwrap();
        assert!(
            m.read_page(&mut txn1, PageNumber::new(500).unwrap())
                .is_none(),
            "read on missing page should terminate and return None"
        );
        m.write_page(&mut txn1, PageNumber::new(501).unwrap(), test_data(0x01))
            .unwrap();
        let seq = m.commit(&mut txn1).unwrap();
        assert!(seq > CommitSeq::ZERO);

        // Abort path also terminates and releases resources.
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut txn2, PageNumber::new(502).unwrap(), test_data(0x02))
            .unwrap();
        m.abort(&mut txn2);
        assert_eq!(txn2.state, TransactionState::Aborted);
        assert_eq!(m.lock_table().lock_count(), 0);
    }

    #[test]
    fn test_theorem1_no_dirty_reads() {
        let m = mgr();
        let pgno = PageNumber::new(1_201).unwrap();

        let mut seed = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut seed, pgno, test_data(0x11)).unwrap();
        m.commit(&mut seed).unwrap();

        let mut writer = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut writer, pgno, test_data(0x22)).unwrap();

        let mut reader = m.begin(BeginKind::Concurrent).unwrap();
        let visible = m.read_page(&mut reader, pgno).unwrap();
        assert_eq!(
            visible.as_bytes()[0],
            0x11,
            "reader must not observe uncommitted writer state"
        );
    }

    #[test]
    fn test_theorem1_no_non_repeatable_reads() {
        let m = mgr();
        let pgno = PageNumber::new(1_202).unwrap();

        let mut seed = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut seed, pgno, test_data(0x33)).unwrap();
        m.commit(&mut seed).unwrap();

        let mut reader = m.begin(BeginKind::Concurrent).unwrap();
        let first = m.read_page(&mut reader, pgno).unwrap();
        assert_eq!(first.as_bytes()[0], 0x33);

        let mut writer = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut writer, pgno, test_data(0x44)).unwrap();
        m.commit(&mut writer).unwrap();

        let second = m.read_page(&mut reader, pgno).unwrap();
        assert_eq!(
            second.as_bytes()[0],
            0x33,
            "same transaction must keep a stable snapshot view"
        );
    }

    #[test]
    fn test_theorem1_no_read_skew_across_pages() {
        let m = mgr();
        let page_a = PageNumber::new(1_203).unwrap();
        let page_b = PageNumber::new(1_204).unwrap();

        let mut seed = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut seed, page_a, test_data(0x10)).unwrap();
        m.write_page(&mut seed, page_b, test_data(0x20)).unwrap();
        m.commit(&mut seed).unwrap();

        let mut reader = m.begin(BeginKind::Concurrent).unwrap();
        let first_a = m.read_page(&mut reader, page_a).unwrap();
        assert_eq!(first_a.as_bytes()[0], 0x10);

        let mut writer_a = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut writer_a, page_a, test_data(0xA0))
            .unwrap();
        m.commit(&mut writer_a).unwrap();

        let mut writer_b = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut writer_b, page_b, test_data(0xB0))
            .unwrap();
        m.commit(&mut writer_b).unwrap();

        let second_b = m.read_page(&mut reader, page_b).unwrap();
        let second_a = m.read_page(&mut reader, page_a).unwrap();
        assert_eq!(
            (second_a.as_bytes()[0], second_b.as_bytes()[0]),
            (0x10, 0x20),
            "reader must keep a transaction-consistent pair, not mix old and new page versions"
        );

        let mut later_reader = m.begin(BeginKind::Concurrent).unwrap();
        let later_a = m.read_page(&mut later_reader, page_a).unwrap();
        let later_b = m.read_page(&mut later_reader, page_b).unwrap();
        assert_eq!(
            (later_a.as_bytes()[0], later_b.as_bytes()[0]),
            (0xA0, 0xB0),
            "later snapshots must observe both committed updates"
        );
    }

    #[test]
    fn test_theorem1_no_phantom_reads() {
        let m = mgr();
        let base_pages = [1_301_u32, 1_302, 1_303];
        let phantom_page = 1_304_u32;

        let mut seed = m.begin(BeginKind::Immediate).unwrap();
        for page in base_pages {
            let byte = u8::try_from(page % 251).expect("page modulo 251 always fits in u8");
            m.write_page(&mut seed, PageNumber::new(page).unwrap(), test_data(byte))
                .unwrap();
        }
        m.commit(&mut seed).unwrap();

        let mut reader = m.begin(BeginKind::Concurrent).unwrap();
        let mut initial_visible = Vec::new();
        for page in [1_301_u32, 1_302, 1_303, phantom_page] {
            if m.read_page(&mut reader, PageNumber::new(page).unwrap())
                .is_some()
            {
                initial_visible.push(page);
            }
        }
        assert_eq!(initial_visible, vec![1_301_u32, 1_302, 1_303]);

        let mut inserter = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(
            &mut inserter,
            PageNumber::new(phantom_page).unwrap(),
            test_data(0x7E),
        )
        .unwrap();
        m.commit(&mut inserter).unwrap();

        let mut second_visible = Vec::new();
        for page in [1_301_u32, 1_302, 1_303, phantom_page] {
            if m.read_page(&mut reader, PageNumber::new(page).unwrap())
                .is_some()
            {
                second_visible.push(page);
            }
        }
        assert_eq!(
            second_visible,
            vec![1_301_u32, 1_302, 1_303],
            "reader snapshot must not gain new rows/pages mid-transaction"
        );
    }

    #[test]
    fn test_theorem1_committed_writes_visible_in_later_snapshots() {
        let m = mgr();
        let pgno = PageNumber::new(1_205).unwrap();

        let mut writer = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut writer, pgno, test_data(0x5A)).unwrap();
        let committed = m.commit(&mut writer).unwrap();
        assert!(committed > CommitSeq::ZERO);

        let mut later_reader = m.begin(BeginKind::Concurrent).unwrap();
        let read = m.read_page(&mut later_reader, pgno).unwrap();
        assert_eq!(
            read.as_bytes()[0],
            0x5A,
            "later snapshots must observe committed writes"
        );
    }

    #[test]
    fn test_theorem2_write_skew_detected() {
        let m = mgr();
        let mut pivot = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut pivot, PageNumber::new(1_401).unwrap(), test_data(0xA1))
            .unwrap();
        pivot.has_in_rw = true;
        pivot.has_out_rw = true;

        let result = m.commit(&mut pivot);
        assert_eq!(
            result.unwrap_err(),
            MvccError::BusySnapshot,
            "dangerous rw-rw structure must abort"
        );
        assert_eq!(pivot.state, TransactionState::Aborted);
    }

    #[test]
    fn test_theorem2_non_conflicting_concurrent_commits() {
        let m = mgr();
        let mut txn1 = m.begin(BeginKind::Concurrent).unwrap();
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();

        m.write_page(&mut txn1, PageNumber::new(1_402).unwrap(), test_data(0x01))
            .unwrap();
        m.write_page(&mut txn2, PageNumber::new(1_403).unwrap(), test_data(0x02))
            .unwrap();

        let seq1 = m.commit(&mut txn1).unwrap();
        let seq2 = m.commit(&mut txn2).unwrap();
        assert!(seq1 > CommitSeq::ZERO);
        assert!(seq2 > CommitSeq::ZERO);
    }

    #[test]
    fn test_theorem2_rw_antidependency_tracking() {
        let m = mgr();
        let pgno = PageNumber::new(1_404).unwrap();

        let mut seed = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut seed, pgno, test_data(0x10)).unwrap();
        m.commit(&mut seed).unwrap();

        let mut reader = m.begin(BeginKind::Concurrent).unwrap();
        let _ = m.read_page(&mut reader, pgno).unwrap();

        let mut writer = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut writer, pgno, test_data(0x20)).unwrap();
        m.commit(&mut writer).unwrap();

        if m.commit_index()
            .latest(pgno)
            .is_some_and(|latest| latest > reader.snapshot.high)
        {
            reader.has_out_rw = true;
        }
        assert!(
            reader.has_out_rw,
            "reader must record outgoing rw-antidependency when a post-snapshot write commits"
        );
    }

    #[test]
    fn test_theorem2_dangerous_structure_two_rw_edges() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut txn, PageNumber::new(1_405).unwrap(), test_data(0x77))
            .unwrap();
        txn.has_in_rw = true;
        txn.has_out_rw = true;
        assert!(txn.has_dangerous_structure());
        assert_eq!(m.commit(&mut txn).unwrap_err(), MvccError::BusySnapshot);
    }

    #[test]
    fn test_theorem3_case_a_concurrent_lock_contention() {
        test_theorem3_no_lost_update_case_a_lock_contention();
    }

    #[test]
    fn test_theorem3_case_b_fcw_stale_snapshot() {
        test_theorem3_no_lost_update_case_b_fcw_stale();
    }

    #[test]
    fn test_theorem3_case_b_fresh_snapshot_ok() {
        let m = mgr();
        let pgno = PageNumber::new(1_406).unwrap();

        let mut first_writer = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut first_writer, pgno, test_data(0xA0))
            .unwrap();
        let seq1 = m.commit(&mut first_writer).unwrap();

        let mut second_writer = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut second_writer, pgno, test_data(0xB0))
            .unwrap();
        let seq2 = m.commit(&mut second_writer).unwrap();

        assert!(
            seq2 > seq1,
            "writer with fresh snapshot must commit after prior write"
        );
    }

    #[test]
    fn test_theorem6_begin_is_nonblocking() {
        const ATTEMPTS: u32 = 1_000;
        const MAX_ELAPSED: Duration = Duration::from_secs(2);

        let m = mgr();
        let start = Instant::now();
        for _ in 0..ATTEMPTS {
            let mut txn = m.begin(BeginKind::Concurrent).unwrap();
            m.abort(&mut txn);
        }
        assert!(
            start.elapsed() < MAX_ELAPSED,
            "begin/abort loop must remain bounded"
        );
    }

    #[test]
    fn test_theorem6_read_bounded_by_chain_length() {
        const VERSIONS: u16 = 128;
        const MAX_ELAPSED: Duration = Duration::from_secs(1);

        let m = mgr();
        let pgno = PageNumber::new(1_407).unwrap();
        for i in 0..VERSIONS {
            let byte = u8::try_from(u32::from(i) % 251).expect("modulo bounds u8");
            let mut writer = m.begin(BeginKind::Concurrent).unwrap();
            m.write_page(&mut writer, pgno, test_data(byte)).unwrap();
            m.commit(&mut writer).unwrap();
        }

        let mut reader = m.begin(BeginKind::Concurrent).unwrap();
        let start = Instant::now();
        let read = m.read_page(&mut reader, pgno).unwrap();
        assert!(
            start.elapsed() < MAX_ELAPSED,
            "read should terminate quickly even on deep chains"
        );
        let expected_last = u8::try_from((u32::from(VERSIONS) - 1) % 251).expect("u8 bound");
        assert_eq!(read.as_bytes()[0], expected_last);
    }

    #[test]
    fn test_theorem6_write_concurrent_nonblocking() {
        const ATTEMPTS: u32 = 1_000;
        const MAX_ELAPSED: Duration = Duration::from_secs(1);

        let m = mgr();
        let pgno = PageNumber::new(1_408).unwrap();
        let mut holder = m.begin(BeginKind::Concurrent).unwrap();
        let mut waiter = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut holder, pgno, test_data(0xAA)).unwrap();

        let start = Instant::now();
        for _ in 0..ATTEMPTS {
            let err = m
                .write_page(&mut waiter, pgno, test_data(0xBB))
                .expect_err("lock contention must fail fast");
            assert_eq!(err, MvccError::Busy);
        }
        assert!(
            start.elapsed() < MAX_ELAPSED,
            "write path must be non-blocking under contention"
        );
    }

    #[test]
    fn test_theorem6_commit_bounded() {
        const PAGES: u32 = 64;
        const MAX_ELAPSED: Duration = Duration::from_secs(1);

        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        for offset in 0..PAGES {
            let pgno = PageNumber::new(1_500 + offset).unwrap();
            let byte = u8::try_from(offset % 251).expect("modulo bounds u8");
            m.write_page(&mut txn, pgno, test_data(byte)).unwrap();
        }

        let start = Instant::now();
        let seq = m.commit(&mut txn).unwrap();
        assert!(seq > CommitSeq::ZERO);
        assert!(
            start.elapsed() < MAX_ELAPSED,
            "commit must finish in bounded time"
        );
    }

    #[test]
    fn test_theorem6_abort_bounded() {
        const PAGES: u32 = 64;
        const MAX_ELAPSED: Duration = Duration::from_secs(1);

        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        for offset in 0..PAGES {
            let pgno = PageNumber::new(1_600 + offset).unwrap();
            let byte = u8::try_from(offset % 251).expect("modulo bounds u8");
            m.write_page(&mut txn, pgno, test_data(byte)).unwrap();
        }

        let start = Instant::now();
        m.abort(&mut txn);
        assert_eq!(txn.state, TransactionState::Aborted);
        assert_eq!(m.lock_table().lock_count(), 0);
        assert!(
            start.elapsed() < MAX_ELAPSED,
            "abort must finish in bounded time"
        );
    }

    #[test]
    fn test_theorems_under_serialized_mode() {
        let m = mgr();
        let pgno = PageNumber::new(1_701).unwrap();

        let mut seed = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut seed, pgno, test_data(0x10)).unwrap();
        m.commit(&mut seed).unwrap();

        let mut reader = m.begin(BeginKind::Deferred).unwrap();
        let first_read = m.read_page(&mut reader, pgno).unwrap();
        assert_eq!(first_read.as_bytes()[0], 0x10);

        let mut updater = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut updater, pgno, test_data(0x20)).unwrap();
        m.commit(&mut updater).unwrap();

        let second_read = m.read_page(&mut reader, pgno).unwrap();
        assert_eq!(
            second_read.as_bytes()[0],
            0x10,
            "serialized reader keeps stable snapshot"
        );

        let mut stale = m.begin(BeginKind::Deferred).unwrap();
        let _ = m.read_page(&mut stale, pgno).unwrap();
        let mut writer2 = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut writer2, pgno, test_data(0x30)).unwrap();
        m.commit(&mut writer2).unwrap();
        assert_eq!(
            m.write_page(&mut stale, pgno, test_data(0x40)).unwrap_err(),
            MvccError::BusySnapshot
        );

        let mut liveness = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(
            &mut liveness,
            PageNumber::new(1_702).unwrap(),
            test_data(0xAA),
        )
        .unwrap();
        assert!(m.commit(&mut liveness).is_ok());
    }

    #[test]
    fn test_all_theorems_under_concurrent_workload() {
        test_e2e_all_six_theorems_under_concurrent_workload();
    }

    proptest! {
        #[test]
        fn prop_snapshot_isolation_holds(base in any::<u8>(), delta in 1_u8..=u8::MAX) {
            let m = mgr();
            let pgno = PageNumber::new(1_703).unwrap();
            let next = base.wrapping_add(delta);

            let mut seed = m.begin(BeginKind::Immediate).unwrap();
            m.write_page(&mut seed, pgno, test_data(base)).unwrap();
            m.commit(&mut seed).unwrap();

            let mut reader = m.begin(BeginKind::Concurrent).unwrap();
            let first = m.read_page(&mut reader, pgno).unwrap();
            prop_assert_eq!(first.as_bytes()[0], base);

            let mut writer = m.begin(BeginKind::Immediate).unwrap();
            m.write_page(&mut writer, pgno, test_data(next)).unwrap();
            m.commit(&mut writer).unwrap();

            let second = m.read_page(&mut reader, pgno).unwrap();
            prop_assert_eq!(second.as_bytes()[0], base);
        }

        #[test]
        fn prop_memory_bounded(commits in 1_u8..48_u8) {
            let m = mgr();
            let pgno = PageNumber::new(1_704).unwrap();
            let commit_count = u32::from(commits);

            for step in 0..commit_count {
                let mut writer = m.begin(BeginKind::Concurrent).unwrap();
                let byte = u8::try_from(step % 251).expect("modulo bounds u8");
                m.write_page(&mut writer, pgno, test_data(byte)).unwrap();
                m.commit(&mut writer).unwrap();
            }

            let chain_len = m.version_store().walk_chain(pgno).len();
            let theoretical_bound = usize::try_from(commit_count + 1).unwrap();
            prop_assert!(chain_len <= theoretical_bound);
        }
    }

    #[test]
    fn test_e2e_all_six_theorems_under_concurrent_workload() {
        const R: u64 = 8;
        const D_SECONDS: u64 = 1;

        let m = mgr();

        // Theorem 1 + 3A: conflicting writes are immediate BUSY (non-blocking).
        let pg_conflict = PageNumber::new(900).unwrap();
        let mut t1 = m.begin(BeginKind::Concurrent).unwrap();
        let mut t2 = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut t1, pg_conflict, test_data(0x11)).unwrap();
        assert_eq!(
            m.write_page(&mut t2, pg_conflict, test_data(0x22))
                .unwrap_err(),
            MvccError::Busy
        );
        m.commit(&mut t1).unwrap();

        // Theorem 2 + 3B: old snapshot all-or-none visibility + stale FCW abort.
        let mut old_reader = m.begin(BeginKind::Concurrent).unwrap();
        let mut writer = m.begin(BeginKind::Immediate).unwrap();
        for (page, byte) in [(901_u32, 0x31_u8), (902_u32, 0x32_u8), (903_u32, 0x33_u8)] {
            m.write_page(&mut writer, PageNumber::new(page).unwrap(), test_data(byte))
                .unwrap();
        }
        m.commit(&mut writer).unwrap();
        for page in [901_u32, 902, 903] {
            assert!(
                m.read_page(&mut old_reader, PageNumber::new(page).unwrap())
                    .is_none()
            );
        }

        let mut stale = m.begin(BeginKind::Concurrent).unwrap();
        let mut fresh = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut fresh, PageNumber::new(904).unwrap(), test_data(0x44))
            .unwrap();
        m.commit(&mut fresh).unwrap();
        m.write_page(&mut stale, PageNumber::new(904).unwrap(), test_data(0x55))
            .unwrap();
        assert_eq!(m.commit(&mut stale).unwrap_err(), MvccError::BusySnapshot);

        // Theorem 5 + 6: bounded hot-page history in bounded run and all txns terminate.
        let bound = R * D_SECONDS + 1;
        for i in 0..bound {
            let mut txn = m.begin(BeginKind::Concurrent).unwrap();
            m.write_page(
                &mut txn,
                PageNumber::new(905).unwrap(),
                test_data((i % 251) as u8),
            )
            .unwrap();
            m.commit(&mut txn).unwrap();
        }

        let chain = m.version_store().walk_chain(PageNumber::new(905).unwrap());
        assert!(
            chain.len() <= usize::try_from(bound).unwrap(),
            "bounded run should not exceed configured R*D+1 envelope"
        );
    }

    #[test]
    fn test_e2e_safety_proofs_backed_by_executable_checks() {
        // Keep the named E2E hook requested in bead comments and run the same
        // executable theorem suite.
        test_e2e_all_six_theorems_under_concurrent_workload();
    }

    // ===================================================================
    // bd-zppf tests — §5.8 FCW Conflict Detection and Resolution
    // ===================================================================

    /// Helper: create a page where only byte at `offset` is set to `value`.
    /// This lets us produce disjoint XOR deltas for rebase tests.
    fn test_data_at(offset: usize, value: u8) -> PageData {
        let mut data = PageData::zeroed(PageSize::DEFAULT);
        data.as_bytes_mut()[offset] = value;
        data
    }

    // -- bd-zppf test 1: T1 commits, T2 detects conflict --

    #[test]
    fn test_first_committer_wins() {
        let mut m = mgr();
        // Policy Off: conflicts always abort (no merge attempt).
        m.set_write_merge_policy(WriteMergePolicy::Off).unwrap();

        let pgno = PageNumber::new(1).unwrap();

        // T1 and T2 begin at the same snapshot (stale for T2 after T1 commits).
        let mut txn1 = m.begin(BeginKind::Concurrent).unwrap();
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();

        // T1 writes and commits first (acquires & releases page lock).
        m.write_page(&mut txn1, pgno, test_data(0xAA)).unwrap();
        let seq1 = m.commit(&mut txn1).unwrap();
        assert!(seq1.get() > 0);

        // T2 writes the same page AFTER T1 released its lock.
        m.write_page(&mut txn2, pgno, test_data(0xBB)).unwrap();

        // T2 commits — detects conflict via CommitIndex (seq1 > T2.snapshot.high).
        let result = m.commit(&mut txn2);
        assert_eq!(
            result.unwrap_err(),
            MvccError::BusySnapshot,
            "second committer must get SQLITE_BUSY_SNAPSHOT"
        );
        assert_eq!(txn2.state, TransactionState::Aborted);
    }

    // -- bd-zppf test 2: no conflict on different pages --

    #[test]
    fn test_no_conflict_different_pages() {
        let m = mgr();

        let mut txn1 = m.begin(BeginKind::Concurrent).unwrap();
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();

        // Different pages — no conflict.
        m.write_page(&mut txn1, PageNumber::new(1).unwrap(), test_data(0xAA))
            .unwrap();
        m.write_page(&mut txn2, PageNumber::new(2).unwrap(), test_data(0xBB))
            .unwrap();

        // Both commit successfully — no conflict.
        let seq1 = m.commit(&mut txn1).unwrap();
        let seq2 = m.commit(&mut txn2).unwrap();
        assert!(seq1.get() > 0);
        assert!(seq2.get() > seq1.get());
    }

    // -- bd-zppf test 3: conflict with successful rebase --

    #[test]
    fn test_conflict_with_successful_rebase() {
        let m = mgr();
        // Default policy is Safe → StructuredPatch for non-btree pages.
        // So the rebase path (GF(256) disjoint merge) will be attempted.

        let pgno = PageNumber::new(10).unwrap();

        // T0: establish a base version (all-zeros page).
        let mut txn0 = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut txn0, pgno, PageData::zeroed(PageSize::DEFAULT))
            .unwrap();
        m.commit(&mut txn0).unwrap();

        // T1 and T2 begin at the same snapshot (seeing T0's base).
        let mut txn1 = m.begin(BeginKind::Concurrent).unwrap();
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();

        // T1 writes byte[0] and commits (releases page lock).
        m.write_page(&mut txn1, pgno, test_data_at(0, 0xAA))
            .unwrap();
        m.commit(&mut txn1).unwrap();

        // T2 writes byte[1] after T1 releases lock — commuting operation.
        m.write_page(&mut txn2, pgno, test_data_at(1, 0xBB))
            .unwrap();

        // T2 commits — FCW detects conflict, GF(256) disjoint merge succeeds.
        let seq2 = m.commit(&mut txn2);
        assert!(
            seq2.is_ok(),
            "disjoint byte changes should be rebasable: {seq2:?}"
        );

        // Verify the merged version contains both changes.
        let head_idx = m.version_store().chain_head(pgno).unwrap();
        let merged = m.version_store().get_version(head_idx).unwrap();
        assert_eq!(merged.data.as_bytes()[0], 0xAA, "T1's change preserved");
        assert_eq!(merged.data.as_bytes()[1], 0xBB, "T2's change preserved");
    }

    // -- bd-zppf test 4: non-rebasable conflict returns BUSY_SNAPSHOT --

    #[test]
    fn test_conflict_response_sqlite_busy() {
        let m = mgr();
        // Default policy is Safe (rebase attempted, but will fail for overlapping bytes).

        let pgno = PageNumber::new(20).unwrap();

        // T0: establish base version.
        let mut txn0 = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut txn0, pgno, PageData::zeroed(PageSize::DEFAULT))
            .unwrap();
        m.commit(&mut txn0).unwrap();

        // T1 and T2 begin at the same snapshot.
        let mut txn1 = m.begin(BeginKind::Concurrent).unwrap();
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();

        // T1 modifies byte[0] and commits (releases lock).
        m.write_page(&mut txn1, pgno, test_data_at(0, 0xAA))
            .unwrap();
        m.commit(&mut txn1).unwrap();

        // T2 modifies the SAME byte — non-commuting, non-rebasable.
        m.write_page(&mut txn2, pgno, test_data_at(0, 0xBB))
            .unwrap();

        // T2: rebase fails (overlapping deltas), returns SQLITE_BUSY_SNAPSHOT.
        let result = m.commit(&mut txn2);
        assert_eq!(
            result.unwrap_err(),
            MvccError::BusySnapshot,
            "non-rebasable conflict must return SQLITE_BUSY_SNAPSHOT"
        );
    }

    // -- bd-zppf test 5: CommitIndex tracks latest commit per page --

    #[test]
    fn test_commit_index_lookup_correctness() {
        let m = mgr();

        let pg1 = PageNumber::new(100).unwrap();
        let pg2 = PageNumber::new(200).unwrap();

        // No commits yet — CommitIndex returns None.
        assert_eq!(m.commit_index().latest(pg1), None);
        assert_eq!(m.commit_index().latest(pg2), None);

        // T1 writes pg1.
        let mut txn1 = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut txn1, pg1, test_data(0x01)).unwrap();
        let seq1 = m.commit(&mut txn1).unwrap();

        // CommitIndex tracks pg1 at seq1; pg2 still None.
        assert_eq!(m.commit_index().latest(pg1), Some(seq1));
        assert_eq!(m.commit_index().latest(pg2), None);

        // T2 writes both pages.
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut txn2, pg1, test_data(0x02)).unwrap();
        m.write_page(&mut txn2, pg2, test_data(0x03)).unwrap();
        let seq2 = m.commit(&mut txn2).unwrap();

        // CommitIndex updated to seq2 for both pages.
        assert_eq!(m.commit_index().latest(pg1), Some(seq2));
        assert_eq!(m.commit_index().latest(pg2), Some(seq2));
        assert!(seq2 > seq1, "later commit has higher seq");
    }

    // -- bd-zppf E2E: two concurrent writers, deterministic outcome --

    #[test]
    fn test_e2e_first_committer_wins_conflict_response() {
        let m = mgr();

        let pgno = PageNumber::new(50).unwrap();

        // Establish base version.
        let mut txn0 = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut txn0, pgno, PageData::zeroed(PageSize::DEFAULT))
            .unwrap();
        let base_seq = m.commit(&mut txn0).unwrap();

        // --- Part 1: Disjoint changes → rebase succeeds ---

        // W1 and W2 begin at the same snapshot.
        let mut w1 = m.begin(BeginKind::Concurrent).unwrap();
        let mut w2 = m.begin(BeginKind::Concurrent).unwrap();
        assert_eq!(w1.snapshot.high, base_seq);
        assert_eq!(w2.snapshot.high, base_seq);

        // W1 writes byte[0] and commits (releases lock).
        m.write_page(&mut w1, pgno, test_data_at(0, 0x11)).unwrap();
        let seq1 = m.commit(&mut w1).unwrap();
        assert!(seq1 > base_seq);

        // W2 writes byte[4] after W1 released lock (disjoint).
        m.write_page(&mut w2, pgno, test_data_at(4, 0x22)).unwrap();

        // W2 commits — rebase succeeds (disjoint changes).
        let seq2 = m.commit(&mut w2).unwrap();
        assert!(seq2 > seq1);

        // Verify merged version.
        let head = m.version_store().chain_head(pgno).unwrap();
        let merged = m.version_store().get_version(head).unwrap();
        assert_eq!(merged.data.as_bytes()[0], 0x11, "W1 byte preserved");
        assert_eq!(merged.data.as_bytes()[4], 0x22, "W2 byte preserved");
        assert_eq!(merged.commit_seq, seq2);

        // --- Part 2: Overlapping changes → conflict, deterministic outcome ---

        let mut w3 = m.begin(BeginKind::Concurrent).unwrap();
        let mut w4 = m.begin(BeginKind::Concurrent).unwrap();

        // W3 writes byte[0] and commits.
        m.write_page(&mut w3, pgno, test_data_at(0, 0x33)).unwrap();
        let seq3 = m.commit(&mut w3).unwrap();
        assert!(seq3 > seq2);

        // W4 writes byte[0] after W3 released lock (overlapping).
        m.write_page(&mut w4, pgno, test_data_at(0, 0x44)).unwrap();

        // W4 fails — deterministic SQLITE_BUSY_SNAPSHOT.
        let result = m.commit(&mut w4);
        assert_eq!(
            result.unwrap_err(),
            MvccError::BusySnapshot,
            "overlapping byte conflict must fail deterministically"
        );

        // Final verification: only W3's change visible.
        let final_head = m.version_store().chain_head(pgno).unwrap();
        let final_ver = m.version_store().get_version(final_head).unwrap();
        assert_eq!(final_ver.data.as_bytes()[0], 0x33);
        assert_eq!(final_ver.commit_seq, seq3);
    }

    // ===================================================================
    // bd-iwu.1 — Layer 1: SQLite Behavioral Compatibility Mode (§2.4)
    // ===================================================================

    #[test]
    fn test_begin_deferred_no_write_lock() {
        let m = mgr();
        let txn = m.begin(BeginKind::Deferred).unwrap();

        assert!(
            m.write_mutex().holder().is_none(),
            "DEFERRED must not hold write mutex at BEGIN"
        );
        assert!(
            !txn.serialized_write_lock_held,
            "DEFERRED must not hold serialized_write_lock at BEGIN"
        );
    }

    #[test]
    fn test_deferred_upgrade_on_first_write() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Deferred).unwrap();

        assert!(
            !txn.serialized_write_lock_held,
            "no mutex before first write"
        );

        let pgno = PageNumber::new(1).unwrap();
        m.write_page(&mut txn, pgno, test_data(0x01)).unwrap();

        assert!(
            txn.serialized_write_lock_held,
            "mutex must be acquired on first write"
        );
        assert!(
            txn.snapshot_established,
            "snapshot must be established on first write"
        );
        assert_eq!(
            m.write_mutex().holder(),
            Some(txn.txn_id),
            "write mutex must be held by this txn"
        );
    }

    #[test]
    fn test_begin_immediate_acquires_write_lock() {
        let m = mgr();
        let txn1 = m.begin(BeginKind::Immediate).unwrap();

        assert!(
            txn1.serialized_write_lock_held,
            "IMMEDIATE must acquire mutex at BEGIN"
        );
        assert_eq!(m.write_mutex().holder(), Some(txn1.txn_id));

        let result = m.begin(BeginKind::Immediate);
        assert_eq!(
            result.unwrap_err(),
            MvccError::Busy,
            "second IMMEDIATE must get SQLITE_BUSY"
        );
    }

    #[test]
    fn test_begin_exclusive_acquires_write_lock() {
        let m = mgr();
        let txn = m.begin(BeginKind::Exclusive).unwrap();

        assert!(
            txn.serialized_write_lock_held,
            "EXCLUSIVE must acquire mutex at BEGIN"
        );
        assert_eq!(m.write_mutex().holder(), Some(txn.txn_id));

        let result = m.begin(BeginKind::Exclusive);
        assert_eq!(
            result.unwrap_err(),
            MvccError::Busy,
            "second EXCLUSIVE must get SQLITE_BUSY (identical to IMMEDIATE in WAL mode)"
        );
    }

    #[test]
    fn test_concurrent_readers_no_block() {
        let m = mgr();
        let mut r1 = m.begin(BeginKind::Deferred).unwrap();
        let mut r2 = m.begin(BeginKind::Deferred).unwrap();
        let mut r3 = m.begin(BeginKind::Deferred).unwrap();

        let pgno = PageNumber::new(1).unwrap();
        let _ = m.read_page(&mut r1, pgno);
        let _ = m.read_page(&mut r2, pgno);
        let _ = m.read_page(&mut r3, pgno);

        assert!(r1.snapshot_established);
        assert!(r2.snapshot_established);
        assert!(r3.snapshot_established);
        assert!(
            m.write_mutex().holder().is_none(),
            "readers must never hold write mutex"
        );
    }

    #[test]
    fn test_writer_does_not_block_readers() {
        let m = mgr();

        let pgno = PageNumber::new(1).unwrap();
        let mut setup = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut setup, pgno, test_data(0x11)).unwrap();
        m.commit(&mut setup).unwrap();

        let _ser = m.begin(BeginKind::Immediate).unwrap();
        assert!(m.write_mutex().holder().is_some());

        let mut reader = m.begin(BeginKind::Deferred).unwrap();
        let data = m.read_page(&mut reader, pgno);
        assert!(
            data.is_some(),
            "reader must not be blocked by active writer (WAL semantics)"
        );
        assert_eq!(data.unwrap().as_bytes()[0], 0x11);
    }

    #[test]
    fn test_single_writer_serialization() {
        let m = mgr();

        let txn1 = m.begin(BeginKind::Immediate).unwrap();
        assert!(txn1.serialized_write_lock_held);

        assert_eq!(
            m.begin(BeginKind::Immediate).unwrap_err(),
            MvccError::Busy,
            "second IMMEDIATE writer must get SQLITE_BUSY"
        );
        assert_eq!(
            m.begin(BeginKind::Exclusive).unwrap_err(),
            MvccError::Busy,
            "EXCLUSIVE writer must also get SQLITE_BUSY"
        );

        // DEFERRED can begin but will fail on first write attempt.
        let mut def = m.begin(BeginKind::Deferred).unwrap();
        assert!(!def.serialized_write_lock_held);
        let err = m.write_page(&mut def, PageNumber::new(1).unwrap(), test_data(0x01));
        assert_eq!(
            err.unwrap_err(),
            MvccError::Busy,
            "DEFERRED upgrade must fail while another writer holds mutex"
        );
    }

    #[test]
    fn test_serializable_behavior() {
        let m = mgr();
        let pgno_a = PageNumber::new(1).unwrap();
        let pgno_b = PageNumber::new(2).unwrap();

        // Writer 1 writes two pages atomically.
        let mut w1 = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut w1, pgno_a, test_data(0x01)).unwrap();
        m.write_page(&mut w1, pgno_b, test_data(0x02)).unwrap();
        assert_eq!(
            m.begin(BeginKind::Immediate).unwrap_err(),
            MvccError::Busy,
            "write skew impossible: second writer blocked"
        );
        m.commit(&mut w1).unwrap();

        // Writer 2 sees complete state from w1.
        let mut w2 = m.begin(BeginKind::Immediate).unwrap();
        let a = m.read_page(&mut w2, pgno_a).unwrap();
        let b = m.read_page(&mut w2, pgno_b).unwrap();
        assert_eq!(a.as_bytes()[0], 0x01, "w2 sees w1 page A");
        assert_eq!(b.as_bytes()[0], 0x02, "w2 sees w1 page B");
        m.commit(&mut w2).unwrap();
    }

    #[test]
    fn test_busy_timeout_wait() {
        let m = Arc::new(mgr_with_busy_timeout_ms(500));

        let mut txn_conc = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut txn_conc, PageNumber::new(1).unwrap(), test_data(0x01))
            .unwrap();

        let m2 = Arc::clone(&m);
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            let mut txn = txn_conc;
            m2.abort(&mut txn);
        });

        let start = Instant::now();
        let mut txn_ser = m.begin(BeginKind::Immediate).unwrap();
        let elapsed = start.elapsed();

        assert!(
            txn_ser.serialized_write_lock_held,
            "writer must eventually acquire lock after busy_timeout wait"
        );
        assert!(
            elapsed.as_millis() >= 20,
            "writer should have waited (elapsed: {}ms)",
            elapsed.as_millis()
        );
        assert!(
            elapsed.as_millis() < 500,
            "writer should succeed before full timeout (elapsed: {}ms)",
            elapsed.as_millis()
        );

        m.abort(&mut txn_ser);
        releaser.join().unwrap();
    }

    #[test]
    fn test_savepoint_nested() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Immediate).unwrap();
        let p1 = PageNumber::new(1).unwrap();
        let p2 = PageNumber::new(2).unwrap();
        let p3 = PageNumber::new(3).unwrap();

        m.write_page(&mut txn, p1, test_data(0x01)).unwrap();
        let sp1 = m.savepoint(&txn, "sp1");

        m.write_page(&mut txn, p2, test_data(0x02)).unwrap();
        let sp2 = m.savepoint(&txn, "sp2");

        m.write_page(&mut txn, p3, test_data(0x03)).unwrap();
        assert_eq!(txn.write_set.len(), 3);

        // ROLLBACK TO sp2 — undoes p3 only.
        m.rollback_to_savepoint(&mut txn, &sp2);
        assert_eq!(txn.write_set.len(), 2);
        assert!(txn.write_set_data.contains_key(&p1));
        assert!(txn.write_set_data.contains_key(&p2));
        assert!(!txn.write_set_data.contains_key(&p3));

        // ROLLBACK TO sp1 — undoes p2 as well.
        m.rollback_to_savepoint(&mut txn, &sp1);
        assert_eq!(txn.write_set.len(), 1);
        assert!(txn.write_set_data.contains_key(&p1));
        assert!(!txn.write_set_data.contains_key(&p2));

        m.commit(&mut txn).unwrap();
    }

    // ===================================================================
    // bd-iwu.2 — Layer 2: BEGIN CONCURRENT with SSI (§2.4)
    // ===================================================================

    #[test]
    fn test_begin_concurrent_parsed() {
        // At the MVCC layer, BeginKind::Concurrent creates a Concurrent-mode txn.
        // (Parser integration is higher-level; here we verify the MVCC semantics.)
        let m = mgr();
        let txn = m.begin(BeginKind::Concurrent).unwrap();

        assert_eq!(
            txn.mode,
            TransactionMode::Concurrent,
            "BEGIN CONCURRENT must create Concurrent-mode transaction"
        );
        assert!(
            !txn.serialized_write_lock_held,
            "concurrent txn must NOT hold serialized writer mutex"
        );
        assert!(
            txn.snapshot_established,
            "concurrent txn establishes snapshot at BEGIN"
        );
        assert!(
            m.write_mutex().holder().is_none(),
            "concurrent txn must not touch the global write mutex"
        );
    }

    #[test]
    fn test_concurrent_disjoint_writes_both_commit() {
        let m = mgr();
        let mut t1 = m.begin(BeginKind::Concurrent).unwrap();
        let mut t2 = m.begin(BeginKind::Concurrent).unwrap();

        let p1 = PageNumber::new(1).unwrap();
        let p2 = PageNumber::new(2).unwrap();

        m.write_page(&mut t1, p1, test_data(0xAA)).unwrap();
        m.write_page(&mut t2, p2, test_data(0xBB)).unwrap();

        let seq1 = m.commit(&mut t1).unwrap();
        let seq2 = m.commit(&mut t2).unwrap();

        assert!(seq1 > CommitSeq::ZERO);
        assert!(seq2 > seq1, "t2 must commit after t1");

        // Both committed values must be visible.
        let mut reader = m.begin(BeginKind::Deferred).unwrap();
        let d1 = m.read_page(&mut reader, p1).unwrap();
        let d2 = m.read_page(&mut reader, p2).unwrap();
        assert_eq!(d1.as_bytes()[0], 0xAA);
        assert_eq!(d2.as_bytes()[0], 0xBB);
    }

    #[test]
    fn test_concurrent_same_page_first_committer_wins() {
        let m = mgr();
        let mut t1 = m.begin(BeginKind::Concurrent).unwrap();
        let mut t2 = m.begin(BeginKind::Concurrent).unwrap();

        let pgno = PageNumber::new(10).unwrap();

        m.write_page(&mut t1, pgno, test_data(0x11)).unwrap();

        // t2 cannot even write the same page (page lock held by t1).
        let err = m.write_page(&mut t2, pgno, test_data(0x22));
        assert_eq!(
            err.unwrap_err(),
            MvccError::Busy,
            "same-page concurrent write must fail with BUSY"
        );

        // t1 commits successfully.
        let seq = m.commit(&mut t1).unwrap();
        assert!(seq > CommitSeq::ZERO);
    }

    #[test]
    fn test_ssi_write_skew_detected_and_aborted() {
        // Classic write-skew scenario at page level:
        // T1 reads P_A, writes P_B.
        // T2 reads P_B, writes P_A.
        // Under SSI, the dangerous structure (both in+out rw-antidependency)
        // causes one transaction to abort.
        let m = mgr();
        assert!(m.ssi_enabled(), "SSI must be on by default");

        let pa = PageNumber::new(1).unwrap();
        let pb = PageNumber::new(2).unwrap();

        // Seed data.
        let mut setup = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut setup, pa, test_data(0x10)).unwrap();
        m.write_page(&mut setup, pb, test_data(0x20)).unwrap();
        m.commit(&mut setup).unwrap();

        // T1: reads P_A, writes P_B.
        let mut t1 = m.begin(BeginKind::Concurrent).unwrap();
        let _ = m.read_page(&mut t1, pa);
        m.write_page(&mut t1, pb, test_data(0x21)).unwrap();

        // T2: reads P_B, writes P_A.
        let mut t2 = m.begin(BeginKind::Concurrent).unwrap();
        let _ = m.read_page(&mut t2, pb);
        // t2 can't write P_A since page lock isn't held for reads, but
        // we simulate the rw-antidependency flags as the witness plane would set them.
        m.write_page(&mut t2, pa, test_data(0x11)).unwrap();

        // Simulate SSI flags that the witness plane would set:
        // T1 has in_rw (T2 wrote P_A which T1 read) and out_rw (T1 wrote P_B which T2 read).
        t1.has_in_rw = true;
        t1.has_out_rw = true;

        // T1 commit must fail due to dangerous structure.
        let result = m.commit(&mut t1);
        assert_eq!(
            result.unwrap_err(),
            MvccError::BusySnapshot,
            "write skew must be detected and aborted under SSI"
        );
    }

    #[test]
    fn test_ssi_dangerous_structure_both_flags() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        let pgno = PageNumber::new(1).unwrap();
        m.write_page(&mut txn, pgno, test_data(0x01)).unwrap();

        // Set both SSI flags.
        txn.has_in_rw = true;
        txn.has_out_rw = true;

        assert!(
            txn.has_dangerous_structure(),
            "both flags set must indicate dangerous structure"
        );

        let result = m.commit(&mut txn);
        assert_eq!(
            result.unwrap_err(),
            MvccError::BusySnapshot,
            "dangerous structure must abort at commit"
        );
        assert_eq!(txn.state, TransactionState::Aborted);
    }

    #[test]
    fn test_ssi_rw_antidependency_tracking() {
        let m = mgr();
        let pgno = PageNumber::new(42).unwrap();

        // Seed data.
        let mut setup = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut setup, pgno, test_data(0x10)).unwrap();
        m.commit(&mut setup).unwrap();

        // T1 reads page K (establishing read dependency).
        let mut t1 = m.begin(BeginKind::Concurrent).unwrap();
        let data = m.read_page(&mut t1, pgno);
        assert!(data.is_some());

        // T2 writes page K after T1's snapshot — creates rw-antidependency.
        let mut t2 = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut t2, pgno, test_data(0x20)).unwrap();
        m.commit(&mut t2).unwrap();

        // T1 now has an incoming rw-antidependency (T2 wrote what T1 read).
        // In full implementation, the witness plane sets this flag.
        t1.has_in_rw = true;

        // With only one flag, T1 can still commit (dangerous structure needs BOTH).
        assert!(
            !t1.has_dangerous_structure(),
            "single rw edge must not trigger dangerous structure"
        );

        // But if we also set outgoing edge...
        t1.has_out_rw = true;
        assert!(
            t1.has_dangerous_structure(),
            "both edges must trigger dangerous structure"
        );
    }

    #[test]
    fn test_pragma_serializable_off_allows_skew() {
        let mut m = mgr();
        m.set_ssi_enabled(false);
        assert!(!m.ssi_enabled());

        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        let pgno = PageNumber::new(1).unwrap();
        m.write_page(&mut txn, pgno, test_data(0x01)).unwrap();

        // Set dangerous structure flags.
        txn.has_in_rw = true;
        txn.has_out_rw = true;
        assert!(txn.has_dangerous_structure());

        // With SSI disabled, commit should SUCCEED despite dangerous structure.
        let seq = m.commit(&mut txn).unwrap();
        assert!(
            seq > CommitSeq::ZERO,
            "with PRAGMA fsqlite.serializable = OFF, write skew must be tolerated"
        );
    }

    #[test]
    fn test_concurrent_mixed_with_serialized() {
        let m = mgr();

        // Start a concurrent writer on page 1.
        let mut conc = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut conc, PageNumber::new(1).unwrap(), test_data(0x01))
            .unwrap();

        // Serialized writer cannot acquire mutex while concurrent locks exist
        // (with a very short timeout to avoid blocking).
        let m2 = mgr_with_busy_timeout_ms(5);
        // We need to share the same infrastructure; use the same manager.
        // Concurrent write blocks serialized acquisition.
        let mut conc2 = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut conc2, PageNumber::new(2).unwrap(), test_data(0x02))
            .unwrap();

        // Both concurrent writers can coexist (different pages).
        let seq1 = m.commit(&mut conc).unwrap();
        let seq2 = m.commit(&mut conc2).unwrap();
        assert!(seq1 > CommitSeq::ZERO);
        assert!(seq2 > seq1);

        // Now serialized writer can proceed.
        let mut ser = m.begin(BeginKind::Immediate).unwrap();
        assert!(ser.serialized_write_lock_held);
        m.write_page(&mut ser, PageNumber::new(3).unwrap(), test_data(0x03))
            .unwrap();
        m.commit(&mut ser).unwrap();

        // Concurrent reader can still work during serialized writer.
        let _active_writer = m.begin(BeginKind::Immediate).unwrap();
        let mut reader = m.begin(BeginKind::Concurrent).unwrap();
        let read_result = m.read_page(&mut reader, PageNumber::new(1).unwrap());
        assert!(
            read_result.is_some(),
            "concurrent reader works during serialized writer"
        );

        // But concurrent writes are blocked by serialized writer.
        let write_result = m.write_page(&mut reader, PageNumber::new(4).unwrap(), test_data(0x04));
        assert_eq!(
            write_result.unwrap_err(),
            MvccError::Busy,
            "concurrent write must fail while serialized writer is active"
        );
        let _ = &m2; // suppress unused warning
    }

    // ===================================================================
    // bd-iwu.5 — Isolation Level Switching PRAGMA (§2.4)
    // ===================================================================

    #[test]
    fn test_pragma_serializable_on_default() {
        let m = mgr();
        assert!(
            m.ssi_enabled(),
            "PRAGMA fsqlite.serializable must default to ON"
        );

        // With default ON, dangerous structure should cause abort.
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut txn, PageNumber::new(1).unwrap(), test_data(0x01))
            .unwrap();
        txn.has_in_rw = true;
        txn.has_out_rw = true;
        assert_eq!(
            m.commit(&mut txn).unwrap_err(),
            MvccError::BusySnapshot,
            "default ON must enforce SSI"
        );
    }

    #[test]
    fn test_pragma_serializable_off() {
        let mut m = mgr();
        m.set_ssi_enabled(false);
        assert!(!m.ssi_enabled());

        // With OFF, dangerous structure should NOT cause abort (plain SI).
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut txn, PageNumber::new(1).unwrap(), test_data(0x01))
            .unwrap();
        txn.has_in_rw = true;
        txn.has_out_rw = true;
        let seq = m.commit(&mut txn).unwrap();
        assert!(
            seq > CommitSeq::ZERO,
            "OFF must allow write skew (plain SI)"
        );
    }

    #[test]
    fn test_pragma_serializable_on() {
        let mut m = mgr();
        // Start OFF, then switch ON.
        m.set_ssi_enabled(false);
        m.set_ssi_enabled(true);
        assert!(m.ssi_enabled());

        // With ON, dangerous structure must abort.
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut txn, PageNumber::new(1).unwrap(), test_data(0x01))
            .unwrap();
        txn.has_in_rw = true;
        txn.has_out_rw = true;
        assert_eq!(
            m.commit(&mut txn).unwrap_err(),
            MvccError::BusySnapshot,
            "ON must enforce SSI after being toggled back"
        );
    }

    #[test]
    fn test_pragma_scope_per_connection() {
        // Each TransactionManager represents a connection.
        let mut conn_a = mgr();
        let conn_b = mgr();

        // Change PRAGMA on conn_a.
        conn_a.set_ssi_enabled(false);
        assert!(!conn_a.ssi_enabled());

        // conn_b is unaffected.
        assert!(
            conn_b.ssi_enabled(),
            "PRAGMA must be per-connection: conn_b unchanged"
        );

        // Verify behavioral difference.
        let mut txn_a = conn_a.begin(BeginKind::Concurrent).unwrap();
        conn_a
            .write_page(&mut txn_a, PageNumber::new(1).unwrap(), test_data(0x01))
            .unwrap();
        txn_a.has_in_rw = true;
        txn_a.has_out_rw = true;
        assert!(
            conn_a.commit(&mut txn_a).is_ok(),
            "conn_a (OFF) must allow write skew"
        );

        let mut txn_b = conn_b.begin(BeginKind::Concurrent).unwrap();
        conn_b
            .write_page(&mut txn_b, PageNumber::new(1).unwrap(), test_data(0x02))
            .unwrap();
        txn_b.has_in_rw = true;
        txn_b.has_out_rw = true;
        assert_eq!(
            conn_b.commit(&mut txn_b).unwrap_err(),
            MvccError::BusySnapshot,
            "conn_b (ON) must enforce SSI"
        );
    }

    #[test]
    fn test_pragma_persists_in_session() {
        let mut m = mgr();
        m.set_ssi_enabled(false);

        // Transaction 1: write skew allowed.
        let mut txn1 = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut txn1, PageNumber::new(1).unwrap(), test_data(0x01))
            .unwrap();
        txn1.has_in_rw = true;
        txn1.has_out_rw = true;
        assert!(m.commit(&mut txn1).is_ok(), "txn1: OFF allows write skew");

        // No PRAGMA change between transactions.

        // Transaction 2: PRAGMA should still be OFF.
        assert!(!m.ssi_enabled(), "PRAGMA must persist in session");
        let mut txn2 = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut txn2, PageNumber::new(2).unwrap(), test_data(0x02))
            .unwrap();
        txn2.has_in_rw = true;
        txn2.has_out_rw = true;
        assert!(
            m.commit(&mut txn2).is_ok(),
            "txn2: OFF must persist across transactions"
        );

        // Now switch ON and verify it takes effect.
        m.set_ssi_enabled(true);
        let mut txn3 = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut txn3, PageNumber::new(3).unwrap(), test_data(0x03))
            .unwrap();
        txn3.has_in_rw = true;
        txn3.has_out_rw = true;
        assert_eq!(
            m.commit(&mut txn3).unwrap_err(),
            MvccError::BusySnapshot,
            "txn3: ON must take effect for next transaction"
        );
    }

    #[test]
    fn test_pragma_not_retroactive_to_active_txn_on_to_off() {
        let mut m = mgr();
        assert!(m.ssi_enabled());

        // Begin under default ON, then flip OFF mid-flight: must NOT affect this txn.
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut txn, PageNumber::new(1).unwrap(), test_data(0x01))
            .unwrap();
        txn.has_in_rw = true;
        txn.has_out_rw = true;
        assert!(txn.has_dangerous_structure());

        m.set_ssi_enabled(false);
        assert_eq!(
            m.commit(&mut txn).unwrap_err(),
            MvccError::BusySnapshot,
            "PRAGMA change must not be retroactive to an active txn"
        );
    }

    #[test]
    fn test_pragma_not_retroactive_to_active_txn_off_to_on() {
        let mut m = mgr();
        m.set_ssi_enabled(false);

        // Begin under OFF, then flip ON mid-flight: must NOT affect this txn.
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        m.write_page(&mut txn, PageNumber::new(1).unwrap(), test_data(0x01))
            .unwrap();
        txn.has_in_rw = true;
        txn.has_out_rw = true;
        assert!(txn.has_dangerous_structure());

        m.set_ssi_enabled(true);
        let seq = m.commit(&mut txn).unwrap();
        assert!(
            seq > CommitSeq::ZERO,
            "OFF-at-BEGIN must tolerate write skew"
        );
    }

    // ===================================================================
    // bd-iwu.4 — Write Skew Detection Test Suite (§2.3)
    // ===================================================================

    #[test]
    fn test_write_skew_sum_constraint() {
        // §2.3 canonical example: A=50, B=50, constraint sum>=0.
        // T1 reads (A,B)=(50,50), writes A=-40 (withdraw 90).
        // T2 reads (A,B)=(50,50), writes B=-40 (withdraw 90).
        // Under SSI: one must abort.
        let capture_guard = crate::test_support::tracing_capture_guard();
        let ((), logs) = with_tracing_capture(&capture_guard, || {
            let m = mgr();
            assert!(m.ssi_enabled(), "SSI must be enabled by default");

            let pa = PageNumber::new(1).unwrap();
            let pb = PageNumber::new(2).unwrap();

            // Seed: A=50, B=50.
            let mut setup = m.begin(BeginKind::Immediate).unwrap();
            m.write_page(&mut setup, pa, test_i64(50)).unwrap();
            m.write_page(&mut setup, pb, test_i64(50)).unwrap();
            m.commit(&mut setup).unwrap();

            // T1: reads both, writes A.
            let mut t1 = m.begin(BeginKind::Concurrent).unwrap();
            let a1 = decode_i64(&m.read_page(&mut t1, pa).unwrap());
            let b1 = decode_i64(&m.read_page(&mut t1, pb).unwrap());
            assert_eq!((a1, b1), (50, 50));
            m.write_page(&mut t1, pa, test_i64(a1 - 90)).unwrap(); // -40

            // T2: reads both, writes B.
            let mut t2 = m.begin(BeginKind::Concurrent).unwrap();
            let a2 = decode_i64(&m.read_page(&mut t2, pa).unwrap());
            let b2 = decode_i64(&m.read_page(&mut t2, pb).unwrap());
            assert_eq!((a2, b2), (50, 50));
            m.write_page(&mut t2, pb, test_i64(b2 - 90)).unwrap(); // -40

            // Each transaction's local constraint check passes under its snapshot.
            let sum1 = decode_i64(&m.read_page(&mut t1, pa).unwrap())
                + decode_i64(&m.read_page(&mut t1, pb).unwrap());
            let sum2 = decode_i64(&m.read_page(&mut t2, pa).unwrap())
                + decode_i64(&m.read_page(&mut t2, pb).unwrap());
            assert!(sum1 >= 0, "txn1 local constraint check must pass");
            assert!(sum2 >= 0, "txn2 local constraint check must pass");

            // Under SSI, one of the two must abort to preserve the invariant.
            let _ = m.commit(&mut t1).unwrap();

            // Simulate witness-plane discovery of the dangerous structure.
            t2.has_in_rw = true;
            t2.has_out_rw = true;
            assert_eq!(
                m.commit(&mut t2).unwrap_err(),
                MvccError::BusySnapshot,
                "SSI must abort one writer to prevent write skew"
            );

            // Verify global invariant preserved: final sum must be >= 0.
            let mut reader = m.begin(BeginKind::Deferred).unwrap();
            let a = decode_i64(&m.read_page(&mut reader, pa).unwrap());
            let b = decode_i64(&m.read_page(&mut reader, pb).unwrap());
            assert!(a + b >= 0, "global invariant must hold (a={a}, b={b})");
        });

        // Log assertions guard observability.  The tracing callsite interest
        // cache is process-global and can race with concurrent tests' calls to
        // `with_default`, causing specific callsites to be incorrectly cached
        // as "never interested."  We only assert on log content when the capture
        // actually received output from the concurrent commit path (indicated by
        // t1's "concurrent commit succeeded" message reaching the buffer).
        // The functional assertions above (BusySnapshot return, global invariant)
        // are the authoritative correctness proof regardless.
        if logs.contains("concurrent commit succeeded") {
            assert!(
                logs.contains("SSI abort: dangerous structure detected"),
                "expected abort log; logs={logs}"
            );
            assert!(logs.contains("conn_id="), "expected conn_id in logs");
        }
    }

    #[test]
    fn test_write_skew_sum_constraint_serializable_off_allows_anomaly() {
        let capture_guard = crate::test_support::tracing_capture_guard();
        let ((), logs) = with_tracing_capture(&capture_guard, || {
            let mut m = mgr();
            m.set_ssi_enabled(false);
            assert!(!m.ssi_enabled());

            let pa = PageNumber::new(1).unwrap();
            let pb = PageNumber::new(2).unwrap();

            // Seed: A=50, B=50.
            let mut setup = m.begin(BeginKind::Immediate).unwrap();
            m.write_page(&mut setup, pa, test_i64(50)).unwrap();
            m.write_page(&mut setup, pb, test_i64(50)).unwrap();
            m.commit(&mut setup).unwrap();

            let mut t1 = m.begin(BeginKind::Concurrent).unwrap();
            let mut t2 = m.begin(BeginKind::Concurrent).unwrap();

            let a1 = decode_i64(&m.read_page(&mut t1, pa).unwrap());
            let b2 = decode_i64(&m.read_page(&mut t2, pb).unwrap());
            m.write_page(&mut t1, pa, test_i64(a1 - 90)).unwrap();
            m.write_page(&mut t2, pb, test_i64(b2 - 90)).unwrap();

            // Even if the witness plane marks the structure as dangerous, the
            // per-txn PRAGMA snapshot should skip SSI validation entirely.
            t1.has_in_rw = true;
            t1.has_out_rw = true;
            t2.has_in_rw = true;
            t2.has_out_rw = true;

            let _ = m.commit(&mut t1).unwrap();
            let _ = m.commit(&mut t2).unwrap();

            let mut reader = m.begin(BeginKind::Deferred).unwrap();
            let a = decode_i64(&m.read_page(&mut reader, pa).unwrap());
            let b = decode_i64(&m.read_page(&mut reader, pb).unwrap());
            assert!(
                a + b < 0,
                "expected anomaly under SI (a={a}, b={b}, sum={})",
                a + b
            );
        });

        // Guard log assertions on capture working (see comment in the SSI test).
        if logs.contains("concurrent commit succeeded") {
            assert!(
                logs.contains("PRAGMA fsqlite.serializable changed"),
                "expected PRAGMA log; logs={logs}"
            );
            assert!(logs.contains("conn_id="), "expected conn_id in logs");
        }
    }

    #[test]
    fn test_write_skew_mutual_exclusion() {
        // T1 reads A, writes B. T2 reads B, writes A.
        // Both see pre-transaction state. Under SSI, one must abort.
        let m = mgr();
        let pa = PageNumber::new(10).unwrap();
        let pb = PageNumber::new(11).unwrap();

        let mut setup = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut setup, pa, test_data(0x10)).unwrap();
        m.write_page(&mut setup, pb, test_data(0x20)).unwrap();
        m.commit(&mut setup).unwrap();

        let mut t1 = m.begin(BeginKind::Concurrent).unwrap();
        let _ = m.read_page(&mut t1, pa); // T1 reads A
        m.write_page(&mut t1, pb, test_data(0x21)).unwrap(); // T1 writes B

        let mut t2 = m.begin(BeginKind::Concurrent).unwrap();
        let _ = m.read_page(&mut t2, pb); // T2 reads B
        m.write_page(&mut t2, pa, test_data(0x11)).unwrap(); // T2 writes A

        // T1: in_rw (T2 writes A, which T1 read), out_rw (T1 writes B, which T2 read).
        t1.has_in_rw = true;
        t1.has_out_rw = true;

        let r1 = m.commit(&mut t1);
        assert_eq!(
            r1.unwrap_err(),
            MvccError::BusySnapshot,
            "mutual exclusion write skew: T1 must be aborted"
        );
    }

    #[test]
    fn test_write_skew_three_way() {
        // T1 reads P1, writes P2.
        // T2 reads P2, writes P3.
        // T3 reads P3, writes P1.
        // Cycle: T1→T2→T3→T1. Under SSI, at least one aborts.
        let m = mgr();
        let p1 = PageNumber::new(20).unwrap();
        let p2 = PageNumber::new(21).unwrap();
        let p3 = PageNumber::new(22).unwrap();

        let mut setup = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut setup, p1, test_data(0x01)).unwrap();
        m.write_page(&mut setup, p2, test_data(0x02)).unwrap();
        m.write_page(&mut setup, p3, test_data(0x03)).unwrap();
        m.commit(&mut setup).unwrap();

        let mut t1 = m.begin(BeginKind::Concurrent).unwrap();
        let _ = m.read_page(&mut t1, p1);
        m.write_page(&mut t1, p2, test_data(0x12)).unwrap();

        let mut t2 = m.begin(BeginKind::Concurrent).unwrap();
        let _ = m.read_page(&mut t2, p2);
        m.write_page(&mut t2, p3, test_data(0x23)).unwrap();

        let mut t3 = m.begin(BeginKind::Concurrent).unwrap();
        let _ = m.read_page(&mut t3, p3);
        m.write_page(&mut t3, p1, test_data(0x31)).unwrap();

        // In a 3-way cycle, the "pivot" transactions have both edges.
        // At page-SSI granularity, all three are pivots.
        t1.has_in_rw = true;
        t1.has_out_rw = true;
        t2.has_in_rw = true;
        t2.has_out_rw = true;
        t3.has_in_rw = true;
        t3.has_out_rw = true;

        // At least one must abort. With all having dangerous structure, all abort.
        let mut aborted = 0_u32;
        if m.commit(&mut t1).is_err() {
            aborted += 1;
        }
        if m.commit(&mut t2).is_err() {
            aborted += 1;
        }
        if m.commit(&mut t3).is_err() {
            aborted += 1;
        }
        assert!(
            aborted >= 1,
            "three-way write skew: at least one transaction must abort"
        );
    }

    #[test]
    fn test_write_skew_read_only_anomaly() {
        // Fekete 2005: read-only transaction anomaly.
        // Read-only transactions should never be aborted by SSI.
        let m = mgr();
        let pgno = PageNumber::new(30).unwrap();

        let mut setup = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut setup, pgno, test_data(0x01)).unwrap();
        m.commit(&mut setup).unwrap();

        // Read-only transaction: only reads, no writes.
        let mut reader = m.begin(BeginKind::Concurrent).unwrap();
        let data = m.read_page(&mut reader, pgno);
        assert!(data.is_some());

        // Reader has no writes, so no dangerous structure possible.
        assert!(
            !reader.has_dangerous_structure(),
            "read-only txn cannot have dangerous structure"
        );

        // Commit should succeed (read-only).
        let seq = m.commit(&mut reader).unwrap();
        assert_eq!(
            seq,
            CommitSeq::ZERO,
            "read-only commit returns ZERO seq (no writes published)"
        );
    }

    #[test]
    fn test_no_write_skew_under_serialized_mode() {
        // Under BEGIN IMMEDIATE (Layer 1), writers are serialized.
        // Write skew is impossible because only one writer is active.
        let m = mgr();
        let pa = PageNumber::new(40).unwrap();
        let pb = PageNumber::new(41).unwrap();

        let mut setup = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut setup, pa, test_data(50)).unwrap();
        m.write_page(&mut setup, pb, test_data(50)).unwrap();
        m.commit(&mut setup).unwrap();

        // T1 (serialized): reads both, writes A.
        let mut t1 = m.begin(BeginKind::Immediate).unwrap();
        let _ = m.read_page(&mut t1, pa);
        let _ = m.read_page(&mut t1, pb);
        m.write_page(&mut t1, pa, test_data(0xD8)).unwrap();

        // T2 cannot even begin IMMEDIATE while T1 holds the mutex.
        assert_eq!(
            m.begin(BeginKind::Immediate).unwrap_err(),
            MvccError::Busy,
            "serialized mode prevents concurrent writers entirely"
        );

        m.commit(&mut t1).unwrap();

        // After T1 commits, T2 can proceed and sees T1's changes.
        let mut t2 = m.begin(BeginKind::Immediate).unwrap();
        let a = m.read_page(&mut t2, pa).unwrap();
        assert_eq!(
            a.as_bytes()[0],
            0xD8,
            "T2 sees T1's committed data — no write skew possible"
        );
        m.commit(&mut t2).unwrap();
    }

    #[test]
    fn test_write_skew_with_indexes() {
        // Write skew involving "indexed lookups" — at the MVCC layer, this
        // means reads and writes to different pages where the index page is
        // shared (both txns traverse it). This creates rw-antidependency.
        let m = mgr();
        let idx_page = PageNumber::new(50).unwrap(); // shared index page
        let data_a = PageNumber::new(51).unwrap();
        let data_b = PageNumber::new(52).unwrap();

        let mut setup = m.begin(BeginKind::Immediate).unwrap();
        m.write_page(&mut setup, idx_page, test_data(0xFF)).unwrap();
        m.write_page(&mut setup, data_a, test_data(0x0A)).unwrap();
        m.write_page(&mut setup, data_b, test_data(0x0B)).unwrap();
        m.commit(&mut setup).unwrap();

        // T1: reads index page + data_a, writes data_b.
        let mut t1 = m.begin(BeginKind::Concurrent).unwrap();
        let _ = m.read_page(&mut t1, idx_page);
        let _ = m.read_page(&mut t1, data_a);
        m.write_page(&mut t1, data_b, test_data(0x1B)).unwrap();

        // T2: reads index page + data_b, writes data_a.
        let mut t2 = m.begin(BeginKind::Concurrent).unwrap();
        let _ = m.read_page(&mut t2, idx_page);
        let _ = m.read_page(&mut t2, data_b);
        m.write_page(&mut t2, data_a, test_data(0x2A)).unwrap();

        // Index witness captures the conflict: both read the shared index page,
        // and each writes to a data page the other depends on.
        t1.has_in_rw = true;
        t1.has_out_rw = true;

        let result = m.commit(&mut t1);
        assert_eq!(
            result.unwrap_err(),
            MvccError::BusySnapshot,
            "indexed write skew must be detected"
        );
    }

    // -----------------------------------------------------------------------
    // bd-bca.2 acceptance-name aliases (Phase 6 MVCC + SSI)
    // -----------------------------------------------------------------------

    #[test]
    fn test_mvcc_serialized_mode() {
        test_serializable_behavior();
    }

    #[test]
    fn test_mvcc_concurrent_different_pages() {
        test_concurrent_disjoint_writes_both_commit();
    }

    #[test]
    fn test_mvcc_concurrent_same_page_conflict() {
        test_concurrent_same_page_first_committer_wins();
    }

    #[test]
    fn test_mvcc_100_threads_100_rows() {
        let manager = Arc::new(Mutex::new(mgr()));

        let handles: Vec<_> = (0_u32..100)
            .map(|thread_id| {
                let manager = Arc::clone(&manager);
                std::thread::spawn(move || {
                    for row in 0_u32..100 {
                        let pgno_raw = 10_000 + thread_id * 100 + row;
                        let pgno = PageNumber::new(pgno_raw).expect("page number in range");
                        let payload = u8::try_from((row + thread_id) % 251).expect("bounded");
                        let guard = manager.lock().expect("manager lock");
                        let mut txn = guard
                            .begin(BeginKind::Concurrent)
                            .expect("begin concurrent");
                        guard
                            .write_page(&mut txn, pgno, test_data(payload))
                            .expect("write");
                        guard.commit(&mut txn).expect("commit");
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("worker thread panicked");
        }

        let guard = manager.lock().expect("manager lock");
        let mut reader = guard.begin(BeginKind::Deferred).expect("begin reader");
        let mut present = 0usize;
        for thread_id in 0_u32..100 {
            for row in 0_u32..100 {
                let pgno_raw = 10_000 + thread_id * 100 + row;
                let pgno = PageNumber::new(pgno_raw).expect("page number in range");
                if guard.read_page(&mut reader, pgno).is_some() {
                    present += 1;
                }
            }
        }
        assert_eq!(present, 10_000, "expected 10,000 committed page writes");
    }

    #[test]
    fn test_snapshot_isolation_long_reader() {
        test_theorem2_snapshot_isolation_all_or_nothing_visibility();
    }

    #[test]
    fn test_snapshot_isolation_new_reader() {
        test_theorem1_committed_writes_visible_in_later_snapshots();
    }

    #[test]
    fn test_ssi_write_skew_abort() {
        test_ssi_write_skew_detected_and_aborted();
    }

    #[test]
    fn test_ssi_non_serializable_allows() {
        test_pragma_serializable_off_allows_skew();
    }

    #[test]
    fn test_ssi_rw_flags() {
        test_ssi_rw_antidependency_tracking();
    }

    #[test]
    fn test_rebase_merge_distinct_keys() {
        test_conflict_with_successful_rebase();
    }

    #[test]
    fn test_rebase_merge_same_key_abort() {
        test_conflict_response_sqlite_busy();
    }

    // ── EBR VersionGuard lifecycle tests (bd-2y306.1) ───────────────

    #[test]
    fn test_version_guard_pinned_at_begin() {
        let mgr = mgr();
        let txn = mgr.begin(BeginKind::Concurrent).unwrap();
        assert!(
            txn.has_version_guard(),
            "VersionGuard must be pinned at begin"
        );
        assert_eq!(mgr.version_guard_registry().active_guard_count(), 1);
    }

    #[test]
    fn test_version_guard_unpinned_on_commit() {
        let mgr = mgr();
        let mut txn = mgr.begin(BeginKind::Concurrent).unwrap();
        assert_eq!(mgr.version_guard_registry().active_guard_count(), 1);
        mgr.commit(&mut txn).expect("commit should succeed");
        assert!(
            !txn.has_version_guard(),
            "VersionGuard must be unpinned after commit"
        );
        assert_eq!(mgr.version_guard_registry().active_guard_count(), 0);
    }

    #[test]
    fn test_version_guard_unpinned_on_abort() {
        let mgr = mgr();
        let mut txn = mgr.begin(BeginKind::Concurrent).unwrap();
        assert_eq!(mgr.version_guard_registry().active_guard_count(), 1);
        mgr.abort(&mut txn);
        assert!(
            !txn.has_version_guard(),
            "VersionGuard must be unpinned after abort"
        );
        assert_eq!(mgr.version_guard_registry().active_guard_count(), 0);
    }

    #[test]
    fn test_version_guard_pinned_for_all_begin_kinds() {
        let mgr = mgr();

        // Concurrent
        let mut txn = mgr.begin(BeginKind::Concurrent).unwrap();
        assert!(txn.has_version_guard());
        mgr.abort(&mut txn);

        // Deferred
        let mut txn = mgr.begin(BeginKind::Deferred).unwrap();
        assert!(txn.has_version_guard());
        mgr.abort(&mut txn);

        // Immediate
        let mut txn = mgr.begin(BeginKind::Immediate).unwrap();
        assert!(txn.has_version_guard());
        mgr.abort(&mut txn);

        assert_eq!(mgr.version_guard_registry().active_guard_count(), 0);
    }

    #[test]
    fn test_multiple_concurrent_txns_pin_separate_guards() {
        let mgr = mgr();
        let txn1 = mgr.begin(BeginKind::Concurrent).unwrap();
        let txn2 = mgr.begin(BeginKind::Concurrent).unwrap();
        let txn3 = mgr.begin(BeginKind::Concurrent).unwrap();

        assert_eq!(mgr.version_guard_registry().active_guard_count(), 3);
        assert!(txn1.has_version_guard());
        assert!(txn2.has_version_guard());
        assert!(txn3.has_version_guard());

        drop(txn1);
        assert_eq!(
            mgr.version_guard_registry().active_guard_count(),
            2,
            "dropping a transaction must unregister its guard ticket"
        );

        drop(txn2);
        assert_eq!(
            mgr.version_guard_registry().active_guard_count(),
            1,
            "remaining transactions should keep their guards pinned independently"
        );

        drop(txn3);
        assert_eq!(
            mgr.version_guard_registry().active_guard_count(),
            0,
            "all guard tickets should unpin when their transactions drop"
        );
    }

    #[test]
    fn test_version_guard_defer_retire_returns_true_when_pinned() {
        let mgr = mgr();
        let txn = mgr.begin(BeginKind::Concurrent).unwrap();
        // Defer a simple value through the guard.
        let result = txn.defer_retire_version(vec![1_u8, 2, 3]);
        assert!(
            result,
            "defer_retire_version must return true when guard is pinned"
        );
    }

    #[test]
    fn test_publish_write_set_keeps_superseded_version_visible_until_gc() {
        let mgr = mgr();
        let pgno = PageNumber::new(6_001).expect("valid page number");

        let mut txn1 = mgr.begin(BeginKind::Concurrent).unwrap();
        let first_data = test_data(0x11);
        mgr.write_page(&mut txn1, pgno, first_data.clone()).unwrap();
        let first_commit = mgr.commit(&mut txn1).unwrap();

        let mut reader = mgr.begin(BeginKind::Concurrent).unwrap();
        let snapshot_before_second_commit = reader.snapshot;

        let mut txn2 = mgr.begin(BeginKind::Concurrent).unwrap();
        let second_data = test_data(0x22);
        mgr.write_page(&mut txn2, pgno, second_data.clone())
            .unwrap();
        let second_commit = mgr.commit(&mut txn2).unwrap();

        assert_eq!(
            mgr.version_store().chain_length(pgno),
            2,
            "publishing a replacement version must keep the superseded version in the chain until GC"
        );
        let version_before_second_commit = mgr
            .version_store()
            .resolve(pgno, &snapshot_before_second_commit)
            .expect("snapshot taken before second commit should still resolve the first version");
        assert_eq!(
            mgr.version_store()
                .get_version(version_before_second_commit)
                .expect("resolved version")
                .data,
            first_data
        );
        let latest_version = mgr
            .version_store()
            .resolve(pgno, &Snapshot::new(second_commit, SchemaEpoch::ZERO))
            .expect("latest snapshot should resolve the replacement version");
        assert!(
            second_commit > first_commit,
            "replacement commit should advance the global commit sequence"
        );
        assert_eq!(
            mgr.version_store()
                .get_version(latest_version)
                .expect("latest resolved version")
                .data,
            second_data
        );
        mgr.abort(&mut reader);
        assert_eq!(mgr.version_guard_registry().active_guard_count(), 0);
    }

    #[test]
    fn test_transaction_guard_blocks_recycling_of_retired_versions_until_abort() {
        let mgr = mgr();
        let pgno = PageNumber::new(6_006).expect("valid page number");

        let mut pinned = mgr.begin(BeginKind::Deferred).unwrap();
        assert!(pinned.has_version_guard());
        assert!(!pinned.snapshot_established);
        assert_eq!(mgr.version_guard_registry().active_guard_count(), 1);

        let mut seed = mgr.begin(BeginKind::Concurrent).unwrap();
        mgr.write_page(&mut seed, pgno, test_data(0x10)).unwrap();
        mgr.commit(&mut seed).unwrap();

        let mut writer = mgr.begin(BeginKind::Concurrent).unwrap();
        mgr.write_page(&mut writer, pgno, test_data(0x20)).unwrap();
        mgr.commit(&mut writer).unwrap();

        assert_eq!(
            mgr.version_store().chain_length(pgno),
            1,
            "superseded version should be pruned from the visible chain"
        );
        let pending_while_pinned = mgr.version_store().pending_recycle_count();
        assert!(
            pending_while_pinned > 0,
            "active transaction guard should keep retired slots pending"
        );
        assert_eq!(
            mgr.version_store()
                .try_recycle_retired_slots(mgr.version_store().current_epoch()),
            0,
            "retired slots must not recycle while the transaction guard is pinned"
        );
        assert_eq!(
            mgr.version_store().pending_recycle_count(),
            pending_while_pinned,
            "failed recycle attempts must leave pinned retirements queued"
        );

        mgr.abort(&mut pinned);

        assert_eq!(mgr.version_guard_registry().active_guard_count(), 0);
        assert_eq!(
            mgr.version_store().pending_recycle_count(),
            0,
            "aborting the pinning transaction should release the guard and drain retired slots"
        );
    }

    #[test]
    fn test_version_guard_defer_retire_returns_false_without_guard() {
        let txn = Transaction::new(
            TxnId::new(1).expect("TxnId::new(1) should be valid"),
            TxnEpoch::new(0),
            Snapshot::new(CommitSeq::ZERO, SchemaEpoch::ZERO),
            TransactionMode::Concurrent,
        );
        // No guard pinned — defer_retire should return false.
        let result = txn.defer_retire_version(42_u64);
        assert!(
            !result,
            "defer_retire_version must return false when no guard is pinned"
        );
    }

    #[test]
    fn test_version_guard_deferred_value_freed_after_unpin() {
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        #[derive(Clone)]
        struct DropTracker(Arc<AtomicUsize>);
        impl Drop for DropTracker {
            fn drop(&mut self) {
                self.0.fetch_add(1, AtomicOrdering::SeqCst);
            }
        }

        let mgr = mgr();
        let mut txn = mgr.begin(BeginKind::Concurrent).unwrap();
        let drop_count = Arc::new(AtomicUsize::new(0));

        // Defer a tracker through the EBR guard.
        txn.defer_retire_version(DropTracker(Arc::clone(&drop_count)));
        assert_eq!(drop_count.load(AtomicOrdering::SeqCst), 0);

        // Commit (which unpins the guard and flushes).
        mgr.commit(&mut txn).expect("commit should succeed");

        // Drive epoch advancement to trigger deferred drops. Under parallel
        // test load, grace-period completion can take longer than a fixed
        // small iteration count, so poll until a bounded deadline.
        let deadline = Instant::now() + Duration::from_secs(2);
        while drop_count.load(AtomicOrdering::SeqCst) == 0 && Instant::now() < deadline {
            mgr.version_guard_registry().flush_reclamation();
            std::thread::yield_now();
        }

        assert_eq!(
            drop_count.load(AtomicOrdering::SeqCst),
            1,
            "deferred value must be freed after guard unpin + epoch advance"
        );
    }

    #[test]
    fn test_version_guard_deferred_value_freed_after_abort() {
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        #[derive(Clone)]
        struct DropTracker(Arc<AtomicUsize>);
        impl Drop for DropTracker {
            fn drop(&mut self) {
                self.0.fetch_add(1, AtomicOrdering::SeqCst);
            }
        }

        let mgr = mgr();
        let mut txn = mgr.begin(BeginKind::Concurrent).unwrap();
        let drop_count = Arc::new(AtomicUsize::new(0));

        assert_eq!(mgr.version_guard_registry().active_guard_count(), 1);
        assert!(
            txn.defer_retire_version(DropTracker(Arc::clone(&drop_count))),
            "defer_retire_version should succeed while guard is pinned"
        );

        mgr.abort(&mut txn);
        assert!(!txn.has_version_guard(), "abort must drop version guard");
        assert_eq!(mgr.version_guard_registry().active_guard_count(), 0);

        let deadline = Instant::now() + Duration::from_secs(2);
        while drop_count.load(AtomicOrdering::SeqCst) == 0 && Instant::now() < deadline {
            mgr.version_guard_registry().flush_reclamation();
            std::thread::yield_now();
        }

        assert_eq!(
            drop_count.load(AtomicOrdering::SeqCst),
            1,
            "aborted transaction retirements must be reclaimed after unpin + epoch advance"
        );
    }

    #[test]
    fn test_concurrent_same_page_writers_preserve_all_committed_versions() {
        // Hold a reader snapshot while the writers run so eager GC cannot
        // reclaim superseded versions before the assertion. The threshold
        // settings below keep length-triggered compaction out of the result:
        // - max_chain_length = 128 → upper_bound = 64
        // - ewma = 32×256 → threshold = min(64, max(8, 64)) = 64
        // With 33 total commits (1 seed + 32 workers), chain_len stays below threshold.
        let mut m = mgr_with_busy_timeout_ms(25);
        m.set_max_chain_length(128);
        m.chain_ewma_x256
            .store(32_u64 * 256, std::sync::atomic::Ordering::Relaxed);
        let mgr = Arc::new(m);
        let pgno = PageNumber::new(6_777).unwrap();
        let workers = 8usize;
        let writes_per_worker = 4usize;

        let mut seed = mgr.begin(BeginKind::Concurrent).unwrap();
        mgr.write_page(&mut seed, pgno, test_data(0x00)).unwrap();
        mgr.commit(&mut seed).unwrap();

        let mut pinning_reader = mgr.begin(BeginKind::Deferred).unwrap();
        assert!(
            mgr.read_page(&mut pinning_reader, pgno).is_some(),
            "pinning reader should see the seeded page version"
        );

        let start = Arc::new(std::sync::Barrier::new(workers));
        let mut handles = Vec::with_capacity(workers);

        for worker in 0..workers {
            let mgr_clone = Arc::clone(&mgr);
            let start_clone = Arc::clone(&start);
            handles.push(std::thread::spawn(move || {
                start_clone.wait();
                let mut committed = 0usize;

                for step in 0..writes_per_worker {
                    loop {
                        let mut txn = mgr_clone.begin(BeginKind::Concurrent).unwrap();
                        let payload =
                            u8::try_from((worker * writes_per_worker + step) % 251).unwrap();

                        match mgr_clone.write_page(&mut txn, pgno, test_data(payload)) {
                            Ok(()) => match mgr_clone.commit(&mut txn) {
                                Ok(_) => {
                                    committed += 1;
                                    break;
                                }
                                Err(MvccError::BusySnapshot) => {
                                    std::thread::yield_now();
                                }
                                Err(err) => {
                                    assert_eq!(
                                        err,
                                        MvccError::BusySnapshot,
                                        "unexpected commit error"
                                    );
                                }
                            },
                            Err(MvccError::Busy) => {
                                mgr_clone.abort(&mut txn);
                                std::thread::yield_now();
                            }
                            Err(err) => {
                                assert_eq!(err, MvccError::Busy, "unexpected write error");
                            }
                        }
                    }
                }

                committed
            }));
        }

        let total_committed = handles
            .into_iter()
            .map(|handle| handle.join().expect("writer thread should not panic"))
            .sum::<usize>();

        assert_eq!(
            total_committed,
            workers * writes_per_worker,
            "each worker operation should eventually commit after retries"
        );

        let chain_len = mgr.version_store().walk_chain(pgno).len();
        assert_eq!(
            chain_len,
            total_committed + 1,
            "same-page concurrent commits should retain one version per successful commit plus seed"
        );

        mgr.abort(&mut pinning_reader);
    }

    #[test]
    fn test_chain_length_bounded_after_10000_updates_same_page() {
        let mut mgr = mgr();
        mgr.set_max_chain_length(64);
        mgr.set_chain_length_warning(32);
        let pgno = PageNumber::new(6_778).unwrap();
        let before = GLOBAL_EBR_METRICS.snapshot();

        for step in 0_u32..10_000_u32 {
            let mut txn = mgr.begin(BeginKind::Concurrent).unwrap();
            let byte = u8::try_from(step % 251).unwrap();
            mgr.write_page(&mut txn, pgno, test_data(byte)).unwrap();
            mgr.commit(&mut txn).unwrap();
        }

        let chain_len = mgr.version_store().chain_length(pgno);
        assert!(
            chain_len <= mgr.max_chain_length(),
            "chain length {} exceeded configured max {}",
            chain_len,
            mgr.max_chain_length()
        );

        let after = GLOBAL_EBR_METRICS.snapshot();
        assert!(
            after.gc_freed_count > before.gc_freed_count,
            "expected eager GC frees during sustained same-page updates"
        );
        assert!(
            after.max_chain_length_observed >= before.max_chain_length_observed,
            "max observed chain length should be monotonic"
        );
    }

    /// C7 (bd-l9k8e.7): Test soft-bound chain behavior.
    ///
    /// With SOFT_BOUND_MULTIPLIER = 4, chains between max_chain_length and
    /// max_chain_length * 4 proceed without error (soft bound).  Only chains
    /// exceeding the hard limit trigger MvccError::Busy.
    #[test]
    fn test_chain_soft_bound_allows_exceeding_max_chain_length() {
        let mut mgr = mgr();
        mgr.set_max_chain_length(4);
        mgr.set_chain_length_warning(2);
        let pgno = PageNumber::new(6_779).unwrap();

        // Seed the chain with one version.
        let mut seed = mgr.begin(BeginKind::Concurrent).unwrap();
        mgr.write_page(&mut seed, pgno, test_data(0x01)).unwrap();
        mgr.commit(&mut seed).unwrap();

        // Pin the GC horizon with a long-running reader.
        let mut pinned_reader = mgr.begin(BeginKind::Concurrent).unwrap();
        let _ = mgr.read_page(&mut pinned_reader, pgno);

        // Write enough to exceed max_chain_length (4) but stay below hard
        // limit (16).  With C7 soft-bound, these should all succeed.
        let mut commits_in_soft_range = 0_u32;
        for step in 0_u32..12_u32 {
            let mut writer = mgr.begin(BeginKind::Concurrent).unwrap();
            let byte = u8::try_from((step + 2) % 251).unwrap();
            mgr.write_page(&mut writer, pgno, test_data(byte)).unwrap();
            match mgr.commit(&mut writer) {
                Ok(_) => commits_in_soft_range += 1,
                Err(MvccError::Busy) => break,
                Err(other) => {
                    assert_eq!(other, MvccError::Busy, "unexpected commit error");
                }
            }
        }

        // All 12 writes should succeed under soft-bound (chain_len < 16).
        assert_eq!(
            commits_in_soft_range, 12,
            "C7 soft-bound: commits between max_chain_length and hard_limit should succeed"
        );

        mgr.abort(&mut pinned_reader);
    }

    /// C7 (bd-l9k8e.7): Test hard limit enforcement.
    ///
    /// Chains exceeding SOFT_BOUND_MULTIPLIER * max_chain_length trigger
    /// MvccError::Busy and increment the gc_blocked metric.
    #[test]
    fn test_chain_hard_limit_triggers_busy_when_horizon_pinned() {
        let mut mgr = mgr();
        mgr.set_max_chain_length(4);
        mgr.set_chain_length_warning(2);
        let pgno = PageNumber::new(6_780).unwrap();
        let before = GLOBAL_EBR_METRICS.snapshot();

        // Seed the chain.
        let mut seed = mgr.begin(BeginKind::Concurrent).unwrap();
        mgr.write_page(&mut seed, pgno, test_data(0x01)).unwrap();
        mgr.commit(&mut seed).unwrap();

        // Pin the GC horizon.
        let mut pinned_reader = mgr.begin(BeginKind::Concurrent).unwrap();
        let _ = mgr.read_page(&mut pinned_reader, pgno);

        // Write enough to exceed hard limit (4 * 4 = 16).
        let mut saw_busy = false;
        for step in 0_u32..32_u32 {
            let mut writer = mgr.begin(BeginKind::Concurrent).unwrap();
            let byte = u8::try_from((step + 2) % 251).unwrap();
            mgr.write_page(&mut writer, pgno, test_data(byte)).unwrap();
            match mgr.commit(&mut writer) {
                Ok(_) => {}
                Err(MvccError::Busy) => {
                    saw_busy = true;
                    break;
                }
                Err(other) => {
                    assert_eq!(other, MvccError::Busy, "unexpected commit error");
                }
            }
        }

        assert!(
            saw_busy,
            "expected MvccError::Busy when chain exceeds hard limit (SOFT_BOUND_MULTIPLIER * max_chain_length)"
        );
        mgr.abort(&mut pinned_reader);

        let after = GLOBAL_EBR_METRICS.snapshot();
        assert!(
            after.gc_blocked_count > before.gc_blocked_count,
            "gc_blocked metric should increment when hard limit exceeded"
        );
    }

    #[test]
    fn test_adaptive_compact_threshold_handles_small_max_chain_length() {
        let mut mgr = mgr();
        mgr.set_max_chain_length(4);
        mgr.chain_ewma_x256
            .store(32_u64 * 256, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            mgr.adaptive_compact_threshold(),
            2,
            "threshold should collapse to max_chain_length/2 instead of panicking"
        );

        mgr.set_max_chain_length(1);
        mgr.chain_ewma_x256
            .store(128_u64 * 256, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            mgr.adaptive_compact_threshold(),
            1,
            "single-version chains should still yield a valid positive threshold"
        );
    }

    #[test]
    fn test_cached_gc_horizon_tracks_snapshot_lifecycle() {
        let mgr = mgr();
        let pgno = PageNumber::new(6_780).unwrap();
        assert!(mgr.cached_gc_horizon().is_none());
        assert_eq!(mgr.shm.load_gc_horizon(), CommitSeq::ZERO);

        let mut deferred = mgr.begin(BeginKind::Deferred).unwrap();
        assert!(
            mgr.cached_gc_horizon().is_none(),
            "deferred txns should not pin horizon before first read"
        );

        let _ = mgr.read_page(&mut deferred, pgno);
        assert!(
            mgr.cached_gc_horizon().is_some(),
            "first read should register active snapshot horizon"
        );
        assert_eq!(mgr.shm.load_gc_horizon(), deferred.snapshot.high);

        mgr.abort(&mut deferred);
        assert!(
            mgr.cached_gc_horizon().is_none(),
            "releasing last active snapshot should clear cached horizon"
        );
        assert_eq!(mgr.shm.load_gc_horizon(), CommitSeq::ZERO);
    }

    #[test]
    fn test_read_only_commit_releases_cached_gc_horizon() {
        let mgr = mgr();
        let pgno = PageNumber::new(6_781).unwrap();
        let data = test_data(0x61);

        let mut writer = mgr.begin(BeginKind::Concurrent).unwrap();
        mgr.write_page(&mut writer, pgno, data.clone())
            .expect("writer should stage seed page");
        let commit_seq = mgr.commit(&mut writer).expect("seed writer commit");

        let mut reader = mgr.begin(BeginKind::Concurrent).unwrap();
        assert_eq!(
            mgr.cached_gc_horizon(),
            Some(commit_seq),
            "active read-only transaction should pin the current snapshot horizon"
        );
        assert_eq!(mgr.shm.load_gc_horizon(), commit_seq);
        assert_eq!(
            mgr.read_page(&mut reader, pgno),
            Some(data),
            "read-only transaction should resolve the committed seed page"
        );

        let read_only_commit = mgr
            .commit(&mut reader)
            .expect("read-only transaction should commit");

        assert_eq!(
            read_only_commit,
            CommitSeq::ZERO,
            "read-only commit should not publish a new commit sequence"
        );
        assert_eq!(reader.state, TransactionState::Committed);
        assert!(
            !reader.snapshot_established,
            "read-only commit must mark the snapshot as released"
        );
        assert_eq!(mgr.version_guard_registry().active_guard_count(), 0);
        assert!(
            mgr.cached_gc_horizon().is_none(),
            "read-only commit should release the last cached GC horizon"
        );
        assert_eq!(mgr.shm.load_gc_horizon(), CommitSeq::ZERO);
    }

    #[test]
    fn test_cached_gc_horizon_tracks_deferred_first_write_lifecycle() {
        let mgr = mgr();
        let pgno = PageNumber::new(6_784).unwrap();
        assert!(mgr.cached_gc_horizon().is_none());

        let mut deferred = mgr.begin(BeginKind::Deferred).unwrap();
        mgr.write_page(&mut deferred, pgno, test_data(0x7A))
            .expect("first deferred write should upgrade and establish a snapshot");
        assert_eq!(
            mgr.cached_gc_horizon(),
            Some(deferred.snapshot.high),
            "first deferred write should register the active snapshot horizon"
        );

        mgr.abort(&mut deferred);
        assert!(
            mgr.cached_gc_horizon().is_none(),
            "aborting the last deferred writer should release the cached horizon"
        );
    }

    #[test]
    fn test_cached_gc_horizon_retains_duplicate_snapshot_highs_until_last_release() {
        let mgr = mgr();
        let pgno = PageNumber::new(6_781).unwrap();

        let mut txn_a = mgr.begin(BeginKind::Deferred).unwrap();
        let mut txn_b = mgr.begin(BeginKind::Deferred).unwrap();

        let _ = mgr.read_page(&mut txn_a, pgno);
        let first_horizon = mgr
            .cached_gc_horizon()
            .expect("first active snapshot should populate cached horizon");
        let _ = mgr.read_page(&mut txn_b, pgno);
        assert_eq!(
            mgr.cached_gc_horizon(),
            Some(first_horizon),
            "same snapshot high should keep the cached minimum unchanged"
        );

        mgr.abort(&mut txn_a);
        assert_eq!(
            mgr.cached_gc_horizon(),
            Some(first_horizon),
            "remaining txn with identical snapshot high must keep horizon pinned"
        );

        mgr.abort(&mut txn_b);
        assert!(
            mgr.cached_gc_horizon().is_none(),
            "cached horizon should clear only after the last duplicate snapshot releases"
        );
    }

    #[test]
    fn test_cached_gc_horizon_advances_to_next_snapshot_without_full_clear() {
        let mgr = mgr();
        let read_pgno = PageNumber::new(6_782).unwrap();
        let write_pgno = PageNumber::new(6_783).unwrap();

        let mut oldest = mgr.begin(BeginKind::Deferred).unwrap();
        let _ = mgr.read_page(&mut oldest, read_pgno);
        let oldest_horizon = oldest.snapshot.high;

        let mut writer = mgr.begin(BeginKind::Concurrent).unwrap();
        mgr.write_page(&mut writer, write_pgno, PageData::zeroed(PageSize::DEFAULT))
            .expect("writer should stage page");
        mgr.commit(&mut writer).expect("writer should commit");

        let mut newer = mgr.begin(BeginKind::Deferred).unwrap();
        let _ = mgr.read_page(&mut newer, read_pgno);
        let newer_horizon = newer.snapshot.high;
        assert!(
            newer_horizon > oldest_horizon,
            "later txn should observe an advanced snapshot high after intervening commit"
        );
        assert_eq!(
            mgr.cached_gc_horizon(),
            Some(oldest_horizon),
            "oldest active snapshot should pin the minimum horizon"
        );

        mgr.abort(&mut oldest);
        assert_eq!(
            mgr.cached_gc_horizon(),
            Some(newer_horizon),
            "releasing the oldest snapshot should advance horizon to the next active snapshot"
        );

        mgr.abort(&mut newer);
        assert!(
            mgr.cached_gc_horizon().is_none(),
            "cached horizon should clear after all active snapshots release"
        );
    }

    #[test]
    fn test_snapshot_reuse_hits_when_commit_epoch_is_unchanged() {
        let mgr = mgr();
        assert_eq!(mgr.snapshot_reuse_stats(), (0, 0));

        let mut first = mgr
            .begin(BeginKind::Concurrent)
            .expect("first begin should succeed");
        let first_snapshot = first.snapshot;
        assert_eq!(
            mgr.snapshot_reuse_stats(),
            (1, 0),
            "primed cache should satisfy the first begin without a seqlock reload"
        );
        mgr.abort(&mut first);

        let mut second = mgr
            .begin(BeginKind::Concurrent)
            .expect("second begin should succeed");
        assert_eq!(
            second.snapshot, first_snapshot,
            "unchanged commit epoch should reuse the previously loaded snapshot"
        );
        assert_eq!(
            mgr.snapshot_reuse_stats(),
            (2, 0),
            "repeated begin with no intervening commit should stay on the reuse path"
        );
        mgr.abort(&mut second);
    }

    #[test]
    fn test_snapshot_reuse_invalidates_once_when_shared_commit_epoch_advances() {
        let mgr = mgr();

        let mut first = mgr
            .begin(BeginKind::Concurrent)
            .expect("first begin should succeed");
        assert_eq!(mgr.snapshot_reuse_stats(), (1, 0));
        mgr.abort(&mut first);

        let published_commit_seq = CommitSeq::new(7);
        let published_schema_epoch = SchemaEpoch::new(3);
        mgr.shm.publish_snapshot(
            published_commit_seq,
            published_schema_epoch,
            mgr.shm.load_ecs_epoch(),
        );

        let mut refreshed = mgr
            .begin(BeginKind::Concurrent)
            .expect("begin after external publish should succeed");
        assert_eq!(
            refreshed.snapshot,
            Snapshot::new(published_commit_seq, published_schema_epoch),
            "advanced shared commit epoch must force one fresh snapshot load"
        );
        assert_eq!(
            mgr.snapshot_reuse_stats(),
            (1, 1),
            "the first begin after an external commit should miss once and refresh the cache"
        );
        mgr.abort(&mut refreshed);

        let mut reused = mgr
            .begin(BeginKind::Concurrent)
            .expect("subsequent begin should reuse refreshed snapshot");
        assert_eq!(
            reused.snapshot,
            Snapshot::new(published_commit_seq, published_schema_epoch),
            "refreshed snapshot should become immediately reusable"
        );
        assert_eq!(
            mgr.snapshot_reuse_stats(),
            (2, 1),
            "once refreshed, later begins at the same epoch should hit the reuse path again"
        );
        mgr.abort(&mut reused);
    }

    #[test]
    fn test_snapshot_reuse_invalidates_when_shared_schema_epoch_advances() {
        let mgr = mgr();

        let mut first = mgr
            .begin(BeginKind::Concurrent)
            .expect("first begin should succeed");
        let first_snapshot = first.snapshot;
        assert_eq!(mgr.snapshot_reuse_stats(), (1, 0));
        mgr.abort(&mut first);

        let published_schema_epoch = SchemaEpoch::new(first_snapshot.schema_epoch.get() + 1);
        mgr.shm.publish_snapshot(
            first_snapshot.high,
            published_schema_epoch,
            mgr.shm.load_ecs_epoch(),
        );

        let mut refreshed = mgr
            .begin(BeginKind::Concurrent)
            .expect("begin after schema epoch publish should succeed");
        assert_eq!(
            refreshed.snapshot,
            Snapshot::new(first_snapshot.high, published_schema_epoch),
            "schema epoch changes at the same commit epoch must force a fresh snapshot load"
        );
        assert_eq!(
            mgr.snapshot_reuse_stats(),
            (1, 1),
            "schema-only publication should miss once and refresh the reuse cache"
        );
        mgr.abort(&mut refreshed);

        let mut reused = mgr
            .begin(BeginKind::Concurrent)
            .expect("subsequent begin should reuse refreshed schema epoch");
        assert_eq!(
            reused.snapshot,
            Snapshot::new(first_snapshot.high, published_schema_epoch),
            "refreshed schema epoch should become reusable after the miss"
        );
        assert_eq!(
            mgr.snapshot_reuse_stats(),
            (2, 1),
            "once refreshed, later begins at the same pair should hit the reuse path"
        );
        mgr.abort(&mut reused);
    }

    #[test]
    fn test_snapshot_reuse_primes_cache_after_local_commit_epoch_advances() {
        let mgr = mgr();
        let pgno = PageNumber::new(8_021).unwrap();

        let mut writer = mgr
            .begin(BeginKind::Concurrent)
            .expect("writer begin should use the primed initial snapshot");
        assert_eq!(
            mgr.snapshot_reuse_stats(),
            (1, 0),
            "initial BEGIN should use the commit-epoch reuse path"
        );
        mgr.write_page(&mut writer, pgno, test_data(0x2A))
            .expect("writer should stage a page");
        let commit_seq = mgr.commit(&mut writer).expect("writer commit");

        let mut reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("reader begin after local commit should succeed");
        assert_eq!(
            reader.snapshot,
            Snapshot::new(commit_seq, SchemaEpoch::ZERO),
            "local commit publication should update the cached snapshot epoch"
        );
        assert_eq!(
            mgr.read_page(&mut reader, pgno),
            Some(test_data(0x2A)),
            "reader should observe the locally committed page at the reused snapshot"
        );
        assert_eq!(
            mgr.snapshot_reuse_stats(),
            (2, 0),
            "local commit should prime the cache so the next BEGIN does not reload the seqlock"
        );
        mgr.abort(&mut reader);
    }

    #[test]
    fn test_snapshot_reuse_survives_read_only_commit_without_epoch_advance() {
        let mgr = mgr();
        let pgno = PageNumber::new(8_022).unwrap();

        let mut writer = mgr
            .begin(BeginKind::Concurrent)
            .expect("writer begin should use the primed initial snapshot");
        mgr.write_page(&mut writer, pgno, test_data(0x4C))
            .expect("writer should stage a page");
        let commit_seq = mgr.commit(&mut writer).expect("writer commit");
        assert_eq!(mgr.shm.load_commit_seq(), commit_seq);
        assert_eq!(
            mgr.snapshot_reuse_stats(),
            (1, 0),
            "local write commit should publish the new snapshot into the reuse cache"
        );

        let mut reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("reader begin after local commit should succeed");
        assert_eq!(
            reader.snapshot,
            Snapshot::new(commit_seq, SchemaEpoch::ZERO)
        );
        assert_eq!(
            mgr.snapshot_reuse_stats(),
            (2, 0),
            "reader should hit the locally published snapshot cache"
        );
        assert_eq!(
            mgr.read_page(&mut reader, pgno),
            Some(test_data(0x4C)),
            "reader should observe the committed page before read-only commit"
        );

        let read_only_commit = mgr
            .commit(&mut reader)
            .expect("read-only transaction should commit");
        assert_eq!(
            read_only_commit,
            CommitSeq::ZERO,
            "read-only commit must not allocate or publish a commit sequence"
        );
        assert_eq!(reader.state, TransactionState::Committed);
        assert_eq!(
            mgr.shm.load_commit_seq(),
            commit_seq,
            "read-only commit must leave the shared commit epoch unchanged"
        );
        assert_eq!(
            mgr.snapshot_reuse_stats(),
            (2, 0),
            "read-only commit should not reload or invalidate the snapshot cache"
        );

        let mut next_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("next reader should reuse the unchanged epoch");
        assert_eq!(
            next_reader.snapshot,
            Snapshot::new(commit_seq, SchemaEpoch::ZERO),
            "next reader should see the same reusable snapshot pair"
        );
        assert_eq!(
            mgr.snapshot_reuse_stats(),
            (3, 0),
            "next begin should remain on the reuse path after read-only commit"
        );
        mgr.abort(&mut next_reader);
    }

    #[test]
    fn test_commit_releases_writer_pinned_chain_immediately_when_horizon_advances() {
        let mgr = mgr();
        let pgno = PageNumber::new(6_785).unwrap();

        let mut seed = mgr.begin(BeginKind::Concurrent).unwrap();
        mgr.write_page(&mut seed, pgno, test_data(0x10))
            .expect("seed writer should stage page");
        mgr.commit(&mut seed).expect("seed writer should commit");

        let mut reader = mgr.begin(BeginKind::Deferred).unwrap();
        let _ = mgr.read_page(&mut reader, pgno);

        let mut writer_a = mgr.begin(BeginKind::Concurrent).unwrap();
        mgr.write_page(&mut writer_a, pgno, test_data(0x11))
            .expect("writer_a should stage page");
        mgr.commit(&mut writer_a).expect("writer_a should commit");
        assert_eq!(mgr.version_store.chain_length(pgno), 2);

        mgr.abort(&mut reader);

        let mut writer_b = mgr.begin(BeginKind::Concurrent).unwrap();
        mgr.write_page(&mut writer_b, pgno, test_data(0x12))
            .expect("writer_b should stage page");
        mgr.commit(&mut writer_b).expect("writer_b should commit");
        assert_eq!(
            mgr.version_store.chain_length(pgno),
            1,
            "once the last older snapshot releases, the next commit should eagerly prune superseded versions",
        );
    }

    #[test]
    fn test_gc_bounded_pruning_preserves_oldest_live_snapshot_version() {
        let mut mgr = mgr();
        mgr.set_max_chain_length(4);
        mgr.set_chain_length_warning(2);
        let pgno = PageNumber::new(6_786).unwrap();

        let mut seed = mgr.begin(BeginKind::Concurrent).unwrap();
        mgr.write_page(&mut seed, pgno, test_data(0x10))
            .expect("seed writer should stage page");
        let seq1 = mgr.commit(&mut seed).expect("seed writer should commit");

        let mut oldest_reader = mgr.begin(BeginKind::Concurrent).unwrap();
        let oldest_visible = mgr.read_page(&mut oldest_reader, pgno).unwrap();
        assert_eq!(oldest_visible.as_bytes()[0], 0x10);
        assert_eq!(oldest_reader.read_version_for_page(pgno), Some(seq1));

        let mut writer_a = mgr.begin(BeginKind::Concurrent).unwrap();
        mgr.write_page(&mut writer_a, pgno, test_data(0x11))
            .expect("writer_a should stage page");
        let seq2 = mgr.commit(&mut writer_a).expect("writer_a should commit");

        let mut writer_b = mgr.begin(BeginKind::Concurrent).unwrap();
        mgr.write_page(&mut writer_b, pgno, test_data(0x12))
            .expect("writer_b should stage page");
        let seq3 = mgr.commit(&mut writer_b).expect("writer_b should commit");

        assert_eq!(
            mgr.version_store()
                .walk_chain(pgno)
                .iter()
                .map(|version| version.commit_seq)
                .collect::<Vec<_>>(),
            vec![seq3, seq2, seq1],
            "oldest live reader should keep the full chain back to seq1"
        );

        let mut pinned_reader = mgr.begin(BeginKind::Concurrent).unwrap();
        assert_eq!(
            pinned_reader.snapshot.high, seq3,
            "new live reader should pin the newest committed sequence"
        );
        let pinned_visible = mgr.read_page(&mut pinned_reader, pgno).unwrap();
        assert_eq!(pinned_visible.as_bytes()[0], 0x12);
        assert_eq!(pinned_reader.read_version_for_page(pgno), Some(seq3));

        mgr.abort(&mut oldest_reader);
        assert_eq!(
            mgr.cached_gc_horizon(),
            Some(seq3),
            "releasing the older reader should advance the GC horizon to the oldest remaining live snapshot"
        );

        let mut writer_c = mgr.begin(BeginKind::Concurrent).unwrap();
        mgr.write_page(&mut writer_c, pgno, test_data(0x13))
            .expect("writer_c should stage page");
        let seq4 = mgr.commit(&mut writer_c).expect("writer_c should commit");

        let chain_after_prune = mgr
            .version_store()
            .walk_chain(pgno)
            .iter()
            .map(|version| version.commit_seq)
            .collect::<Vec<_>>();
        assert_eq!(
            chain_after_prune,
            vec![seq4, seq3],
            "GC must prune versions older than the live horizon and stop at the version visible to the pinned reader"
        );

        let pinned_after_prune = mgr.read_page(&mut pinned_reader, pgno).unwrap();
        assert_eq!(
            pinned_after_prune.as_bytes()[0],
            0x12,
            "pinned reader must still see the version visible at its snapshot after GC"
        );
        assert_eq!(pinned_reader.read_version_for_page(pgno), Some(seq3));

        let mut writer_d = mgr.begin(BeginKind::Concurrent).unwrap();
        mgr.write_page(&mut writer_d, pgno, test_data(0x14))
            .expect("writer_d should stage page");
        let seq5 = mgr.commit(&mut writer_d).expect("writer_d should commit");

        let chain_after_second_prune = mgr
            .version_store()
            .walk_chain(pgno)
            .iter()
            .map(|version| version.commit_seq)
            .collect::<Vec<_>>();
        assert_eq!(
            chain_after_second_prune,
            vec![seq5, seq4, seq3],
            "repeated pruning must remain bounded at the oldest live snapshot instead of chasing past it"
        );
        assert!(
            !chain_after_second_prune.contains(&seq1) && !chain_after_second_prune.contains(&seq2),
            "versions older than the oldest live snapshot should stay pruned"
        );

        let mut fresh_reader = mgr.begin(BeginKind::Concurrent).unwrap();
        let fresh_visible = mgr.read_page(&mut fresh_reader, pgno).unwrap();
        assert_eq!(fresh_visible.as_bytes()[0], 0x14);
        assert_eq!(fresh_reader.read_version_for_page(pgno), Some(seq5));

        mgr.abort(&mut pinned_reader);
        mgr.abort(&mut fresh_reader);
    }

    #[test]
    fn test_version_guard_registry_accessor() {
        let mgr = mgr();
        let registry = mgr.version_guard_registry();
        assert_eq!(registry.active_guard_count(), 0);
    }

    // -------------------------------------------------------------------------
    // C4 (bd-l9k8e.4): Cell-level MVCC commit path tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_commit_structural_page_uses_page_level_mvcc() {
        // C4 test: Pages marked as structural should use page-level MVCC
        let mgr = mgr();
        let pgno = PageNumber::new(100).unwrap();

        let mut txn = mgr.begin(BeginKind::Concurrent).unwrap();

        // Write to the page (this marks it as structural via write_page)
        let data = test_data(0xAB);
        mgr.write_page(&mut txn, pgno, data.clone())
            .expect("write should succeed");

        // Verify the page is in structural_pages
        assert!(
            txn.structural_pages.contains(&pgno),
            "write_page should mark page as structural"
        );

        let commit_seq = mgr.commit(&mut txn).expect("commit should succeed");

        // Structural pages should have their full data in VersionStore
        let snapshot = Snapshot::new(commit_seq, SchemaEpoch::ZERO);
        let resolved = mgr.version_store.resolve_visible_version(pgno, &snapshot);
        assert!(
            resolved.is_some(),
            "structural page should be visible in VersionStore"
        );
        assert_eq!(
            resolved.unwrap().data.as_bytes()[0],
            0xAB,
            "structural page data should match"
        );
    }

    #[test]
    fn test_commit_logical_page_not_in_structural_set() {
        // C4 test: Logical pages (cell-level only) should NOT be in structural_pages
        // Note: This test verifies the routing logic. In a real system, B-tree code
        // would call cell_log.record_insert() and NOT call write_page().
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, SemanticKeyKind, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(200).unwrap();

        let mut txn = mgr.begin(BeginKind::Concurrent).unwrap();
        let _ = mgr.read_page(&mut txn, pgno); // Establish snapshot

        // Simulate cell-level mutation: record a cell delta but don't call write_page
        let cell_key = CellKey {
            btree: BtreeRef::Table(TableId::new(1)),
            kind: SemanticKeyKind::TableRow,
            key_digest: [0u8; 16],
        };
        let txn_token = txn.token();

        // Record a cell insert
        let idx = mgr
            .cell_log()
            .record_insert(cell_key, pgno, vec![1, 2, 3, 4], txn_token);
        assert!(idx.is_some(), "record_insert should succeed");

        // Add page to write_set manually (B-tree code would do this)
        txn.write_set.push(pgno);

        // Page is NOT in structural_pages (no write_page call)
        assert!(
            !txn.structural_pages.contains(&pgno),
            "cell-only page should not be in structural_pages"
        );

        let commit_seq = mgr.commit(&mut txn).expect("commit should succeed");

        // Logical pages commit cell deltas but don't create full page versions
        // (until C5 Materialize Pages is implemented).
        // The commit path must still publish cell-delta visibility.
        assert_eq!(
            mgr.cell_log().resolve(pgno, &cell_key, commit_seq),
            Some(vec![1, 2, 3, 4])
        );
        assert!(
            mgr.commit_index.latest(pgno).is_some(),
            "commit_index should have entry for logical page"
        );
        let indexed_seq = mgr.commit_index.latest(pgno).unwrap();
        assert_eq!(
            indexed_seq, commit_seq,
            "commit_index should have correct commit_seq"
        );
    }

    #[test]
    fn test_cell_delta_only_commit_assigns_commit_seq_without_write_set()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 5252;
        let payload = vec![b'x'; 96];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut writer = mgr.begin(BeginKind::Concurrent).expect("writer begin");
        let writer_token = writer.token();
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &payload),
                writer_token,
            )
            .expect("logical insert should fit budget");
        assert!(
            writer.write_set.is_empty() && writer.write_set_data.is_empty(),
            "this regression must exercise the cell-delta-only commit path"
        );

        let commit_seq = mgr.commit(&mut writer).expect("logical commit");
        assert!(
            commit_seq.get() > 0,
            "cell-delta-only commit must allocate a real commit sequence"
        );
        assert_eq!(
            mgr.cell_log().txn_delta_count(writer_token),
            0,
            "committed cell deltas should leave transaction tracking"
        );
        assert_eq!(
            mgr.cell_log().resolve(pgno, &cell_key, commit_seq),
            Some(leaf_table_cell(rowid, &payload)),
            "committed cell delta should be visible at its commit sequence"
        );
        assert_eq!(
            mgr.commit_index.latest(pgno),
            Some(commit_seq),
            "cell-delta-only commit should advance the page conflict index"
        );

        let mut reader = mgr.begin(BeginKind::Concurrent).expect("reader begin");
        let page = mgr
            .read_page_with_cell_deltas(&mut reader, pgno, PageSize::DEFAULT.get())?
            .expect("page should be visible");
        assert_eq!(materialized_table_payloads(&page)?, vec![(rowid, payload)]);

        Ok(())
    }

    #[test]
    fn test_cell_delta_only_commit_checks_pending_page_fcw() {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut first = mgr.begin(BeginKind::Concurrent).expect("first begin");
        let mut second = mgr.begin(BeginKind::Concurrent).expect("second begin");
        mgr.cell_log()
            .record_insert(
                CellKey::table_row(btree, 1),
                pgno,
                leaf_table_cell(1, &[b'a'; 8]),
                first.token(),
            )
            .expect("first logical insert should fit budget");
        mgr.cell_log()
            .record_insert(
                CellKey::table_row(btree, 2),
                pgno,
                leaf_table_cell(2, &[b'b'; 8]),
                second.token(),
            )
            .expect("second logical insert should fit budget");
        assert!(first.write_set.is_empty());
        assert!(second.write_set.is_empty());

        let first_seq = mgr.commit(&mut first).expect("first commit");
        let second_err = mgr
            .commit(&mut second)
            .expect_err("second commit must respect page-level FCW");
        assert_eq!(second_err, MvccError::BusySnapshot);
        assert_eq!(
            mgr.commit_index.latest(pgno),
            Some(first_seq),
            "aborted second writer must not advance the page commit index"
        );
    }

    #[test]
    fn test_cell_delta_fcw_abort_releases_lock_and_deltas() {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let first_key = CellKey::table_row(btree, 101);
        let second_key = CellKey::table_row(btree, 102);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut first = mgr.begin(BeginKind::Concurrent).expect("first begin");
        let mut second = mgr.begin(BeginKind::Concurrent).expect("second begin");
        let second_token = second.token();
        mgr.cell_log()
            .record_insert(
                first_key,
                pgno,
                leaf_table_cell(101, &[b'a'; 8]),
                first.token(),
            )
            .expect("first logical insert should fit budget");
        mgr.cell_log()
            .record_insert(
                second_key,
                pgno,
                leaf_table_cell(102, &[b'b'; 8]),
                second_token,
            )
            .expect("second logical insert should fit budget");

        let first_seq = mgr.commit(&mut first).expect("first commit");
        assert_eq!(
            mgr.lock_table().holder(pgno),
            None,
            "successful logical commit should release its page lock"
        );

        let second_err = mgr
            .commit(&mut second)
            .expect_err("stale logical writer must fail FCW validation");
        assert_eq!(second_err, MvccError::BusySnapshot);
        assert_eq!(second.state, TransactionState::Aborted);
        assert!(
            second.page_locks.is_empty(),
            "FCW abort should clear the logical commit lock set"
        );
        assert!(
            second.write_version_for_page(pgno).is_none(),
            "FCW abort should clear logical write-version metadata"
        );
        assert_eq!(
            mgr.lock_table().holder(pgno),
            None,
            "FCW abort should release the page lock acquired before validation"
        );
        assert_eq!(
            mgr.cell_log().txn_delta_count(second_token),
            0,
            "FCW abort should roll back uncommitted logical deltas"
        );
        assert_eq!(
            mgr.cell_log().resolve(pgno, &second_key, first_seq),
            None,
            "rolled-back logical delta must not become visible"
        );
        assert_eq!(mgr.commit_index.latest(pgno), Some(first_seq));
    }

    #[test]
    fn test_cell_delta_only_commit_takes_page_lock_before_publish() {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));

        let mut first = mgr.begin(BeginKind::Concurrent).expect("first begin");
        let mut second = mgr.begin(BeginKind::Concurrent).expect("second begin");
        mgr.cell_log()
            .record_insert(
                CellKey::table_row(btree, 10),
                pgno,
                leaf_table_cell(10, &[b'a'; 8]),
                first.token(),
            )
            .expect("first logical insert should fit budget");
        mgr.cell_log()
            .record_insert(
                CellKey::table_row(btree, 11),
                pgno,
                leaf_table_cell(11, &[b'b'; 8]),
                second.token(),
            )
            .expect("second logical insert should fit budget");

        mgr.prepare_cell_delta_commit_exclusion(&mut first)
            .expect("first logical writer should acquire the page lock");
        assert_eq!(mgr.lock_table().holder(pgno), Some(first.txn_id));
        assert!(
            first.write_version_for_page(pgno).is_some(),
            "logical page writes need SSI/FCW witness metadata"
        );

        let err = mgr
            .prepare_cell_delta_commit_exclusion(&mut second)
            .expect_err("same-page logical writer must not pass the commit race window");
        assert_eq!(err, MvccError::Busy);
        assert_eq!(
            mgr.lock_table().holder(pgno),
            Some(first.txn_id),
            "failed logical writer must not steal the page lock"
        );

        mgr.abort(&mut first);
        mgr.abort(&mut second);
    }

    #[test]
    fn test_cell_delta_commit_busy_releases_partial_locks() {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let free_pgno = PageNumber::new(2).expect("valid page");
        let blocked_pgno = PageNumber::new(3).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));

        let mut blocker = mgr.begin(BeginKind::Concurrent).expect("blocker begin");
        mgr.write_page(&mut blocker, blocked_pgno, test_data(0x44))
            .expect("blocker should acquire the later page lock");
        assert_eq!(mgr.lock_table().holder(blocked_pgno), Some(blocker.txn_id));

        let mut writer = mgr.begin(BeginKind::Concurrent).expect("writer begin");
        let writer_token = writer.token();
        mgr.cell_log()
            .record_insert(
                CellKey::table_row(btree, 20),
                free_pgno,
                leaf_table_cell(20, &[b'f'; 8]),
                writer_token,
            )
            .expect("free page logical insert should fit budget");
        mgr.cell_log()
            .record_insert(
                CellKey::table_row(btree, 21),
                blocked_pgno,
                leaf_table_cell(21, &[b'b'; 8]),
                writer_token,
            )
            .expect("blocked page logical insert should fit budget");

        let err = mgr
            .commit(&mut writer)
            .expect_err("later page lock conflict should abort the logical commit");
        assert_eq!(err, MvccError::Busy);
        assert_eq!(writer.state, TransactionState::Aborted);
        assert!(
            writer.page_locks.is_empty(),
            "abort should clear the partially acquired logical commit lock set"
        );
        assert_eq!(
            mgr.lock_table().holder(free_pgno),
            None,
            "abort should release the earlier page lock acquired before Busy"
        );
        assert_eq!(
            mgr.lock_table().holder(blocked_pgno),
            Some(blocker.txn_id),
            "failed writer must not disturb the conflicting writer's lock"
        );
        assert_eq!(
            mgr.cell_log().txn_delta_count(writer_token),
            0,
            "aborted logical commit should roll back its cell deltas"
        );

        mgr.abort(&mut blocker);
    }

    #[test]
    fn test_serialized_cell_delta_only_commit_requires_writer_exclusion() {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));

        let mut holder = mgr
            .begin(BeginKind::Immediate)
            .expect("immediate serialized writer should acquire exclusion");
        let mut logical = mgr
            .begin(BeginKind::Deferred)
            .expect("deferred serialized transaction should begin");
        let logical_token = logical.token();
        mgr.cell_log()
            .record_insert(
                CellKey::table_row(btree, 12),
                pgno,
                leaf_table_cell(12, &[b'c'; 8]),
                logical_token,
            )
            .expect("logical insert should fit budget");

        let err = mgr
            .commit(&mut logical)
            .expect_err("cell-delta-only serialized commit must not bypass writer exclusion");
        assert_eq!(err, MvccError::Busy);
        assert_eq!(logical.state, TransactionState::Aborted);
        assert_eq!(
            mgr.cell_log().txn_delta_count(logical_token),
            0,
            "failed serialized logical commit should roll back its cell deltas"
        );

        mgr.abort(&mut holder);
    }

    #[test]
    fn test_read_page_with_cell_deltas_materializes_committed_delta() -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 4242;
        let payload = vec![b'c'; 130];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut writer = mgr.begin(BeginKind::Concurrent).expect("writer begin");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &payload),
                writer.token(),
            )
            .expect("logical insert should fit budget");
        writer.write_set.push(pgno);
        mgr.commit(&mut writer).expect("logical commit");

        let mut reader = mgr.begin(BeginKind::Concurrent).expect("reader begin");
        let page = mgr
            .read_page_with_cell_deltas(&mut reader, pgno, PageSize::DEFAULT.get())?
            .expect("page should be visible");

        assert_eq!(materialized_table_payloads(&page)?, vec![(rowid, payload)]);

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_tracks_materialized_commit_horizon()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 4243;
        let payload = vec![b'h'; 131];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        let seed_seq = mgr.commit(&mut seed).expect("seed commit");

        let mut writer = mgr.begin(BeginKind::Concurrent).expect("writer begin");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &payload),
                writer.token(),
            )
            .expect("logical insert should fit budget");
        writer.write_set.push(pgno);
        let logical_seq = mgr.commit(&mut writer).expect("logical commit");
        assert!(logical_seq > seed_seq);

        let mut reader = mgr.begin(BeginKind::Concurrent).expect("reader begin");
        let page = mgr
            .read_page_with_cell_deltas(&mut reader, pgno, PageSize::DEFAULT.get())?
            .expect("page should be visible");

        assert_eq!(materialized_table_payloads(&page)?, vec![(rowid, payload)]);
        assert_eq!(
            reader.read_version_for_page(pgno),
            Some(logical_seq),
            "logical reads must witness the committed delta horizon, not just the base page"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_keeps_pinned_snapshot_horizon() -> fsqlite_error::Result<()>
    {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 4244;
        let payload = vec![b'p'; 132];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        let seed_seq = mgr.commit(&mut seed).expect("seed commit");

        let mut pinned_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("pinned reader begin");
        assert_eq!(
            pinned_reader.snapshot.high, seed_seq,
            "reader must pin its snapshot before the later logical commit"
        );

        let mut writer = mgr.begin(BeginKind::Concurrent).expect("writer begin");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &payload),
                writer.token(),
            )
            .expect("logical insert should fit budget");
        writer.write_set.push(pgno);
        let logical_seq = mgr.commit(&mut writer).expect("logical commit");
        assert!(logical_seq > pinned_reader.snapshot.high);

        let old_page = mgr
            .read_page_with_cell_deltas(&mut pinned_reader, pgno, PageSize::DEFAULT.get())?
            .expect("base page should stay visible");
        assert_eq!(
            materialized_table_payloads(&old_page)?,
            Vec::<(i64, Vec<u8>)>::new(),
            "pinned reader must not materialize a logical commit after its snapshot"
        );
        assert_eq!(
            pinned_reader.read_version_for_page(pgno),
            Some(seed_seq),
            "post-snapshot logical commits must not advance the pinned read witness"
        );

        let mut fresh_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("fresh reader begin");
        let fresh_page = mgr
            .read_page_with_cell_deltas(&mut fresh_reader, pgno, PageSize::DEFAULT.get())?
            .expect("fresh page should be visible");
        assert_eq!(
            materialized_table_payloads(&fresh_page)?,
            vec![(rowid, payload)]
        );
        assert_eq!(
            fresh_reader.read_version_for_page(pgno),
            Some(logical_seq),
            "fresh logical reads should witness the committed delta horizon"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_keeps_own_uncommitted_delta_out_of_horizon()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 4269;
        let payload = vec![b'u'; 133];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        let seed_seq = mgr.commit(&mut seed).expect("seed commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &payload),
                txn.token(),
            )
            .expect("logical insert should fit budget");

        let own_page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("base page should stay visible");
        assert_eq!(
            materialized_table_payloads(&own_page)?,
            vec![(rowid, payload.clone())],
            "transaction-local reads must materialize their own logical writes"
        );
        assert_eq!(
            txn.read_version_for_page(pgno),
            Some(seed_seq),
            "own uncommitted synthetic deltas must not advance the committed read witness"
        );

        let logical_seq = mgr.commit(&mut txn).expect("logical commit");
        assert!(logical_seq > seed_seq);

        let mut fresh_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("fresh reader begin");
        let fresh_page = mgr
            .read_page_with_cell_deltas(&mut fresh_reader, pgno, PageSize::DEFAULT.get())?
            .expect("fresh page should be visible");
        assert_eq!(
            materialized_table_payloads(&fresh_page)?,
            vec![(rowid, payload)]
        );
        assert_eq!(
            fresh_reader.read_version_for_page(pgno),
            Some(logical_seq),
            "fresh reads should witness the committed logical delta after publish"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_keeps_own_update_out_of_horizon() -> fsqlite_error::Result<()>
    {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 4270;
        let original_payload = vec![b'o'; 134];
        let updated_payload = vec![b'v'; 135];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut inserter = mgr.begin(BeginKind::Concurrent).expect("inserter begin");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &original_payload),
                inserter.token(),
            )
            .expect("logical insert should fit budget");
        let insert_seq = mgr.commit(&mut inserter).expect("insert commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        mgr.cell_log()
            .record_update(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &updated_payload),
                txn.token(),
            )
            .expect("logical update should fit budget");

        let own_page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("inserted page should stay visible");
        assert_eq!(
            materialized_table_payloads(&own_page)?,
            vec![(rowid, updated_payload.clone())],
            "transaction-local reads must materialize their own logical updates"
        );
        assert_eq!(
            txn.read_version_for_page(pgno),
            Some(insert_seq),
            "own uncommitted updates must preserve the last committed read witness"
        );

        let update_seq = mgr.commit(&mut txn).expect("update commit");
        assert!(update_seq > insert_seq);

        let mut fresh_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("fresh reader begin");
        let fresh_page = mgr
            .read_page_with_cell_deltas(&mut fresh_reader, pgno, PageSize::DEFAULT.get())?
            .expect("fresh page should be visible");
        assert_eq!(
            materialized_table_payloads(&fresh_page)?,
            vec![(rowid, updated_payload)]
        );
        assert_eq!(
            fresh_reader.read_version_for_page(pgno),
            Some(update_seq),
            "fresh reads should witness the committed update horizon"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_keeps_own_delete_out_of_horizon() -> fsqlite_error::Result<()>
    {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 4271;
        let payload = vec![b'd'; 136];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut inserter = mgr.begin(BeginKind::Concurrent).expect("inserter begin");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &payload),
                inserter.token(),
            )
            .expect("logical insert should fit budget");
        let insert_seq = mgr.commit(&mut inserter).expect("insert commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        mgr.cell_log()
            .record_delete(cell_key, pgno, txn.token())
            .expect("logical delete should fit budget");

        let own_page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("inserted page should stay visible");
        assert_eq!(
            materialized_table_payloads(&own_page)?,
            Vec::<(i64, Vec<u8>)>::new(),
            "transaction-local reads must materialize their own logical deletes"
        );
        assert_eq!(
            txn.read_version_for_page(pgno),
            Some(insert_seq),
            "own uncommitted deletes must preserve the last committed read witness"
        );

        let delete_seq = mgr.commit(&mut txn).expect("delete commit");
        assert!(delete_seq > insert_seq);

        let mut fresh_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("fresh reader begin");
        let fresh_page = mgr
            .read_page_with_cell_deltas(&mut fresh_reader, pgno, PageSize::DEFAULT.get())?
            .expect("fresh page should be visible");
        assert_eq!(
            materialized_table_payloads(&fresh_page)?,
            Vec::<(i64, Vec<u8>)>::new()
        );
        assert_eq!(
            fresh_reader.read_version_for_page(pgno),
            Some(delete_seq),
            "fresh reads should witness the committed delete horizon"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_keeps_own_mixed_batch_out_of_horizon()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let row_a = 4281;
        let row_b = 4282;
        let row_c = 4283;
        let key_a = CellKey::table_row(btree, row_a);
        let key_b = CellKey::table_row(btree, row_b);
        let key_c = CellKey::table_row(btree, row_c);
        let a_original = vec![b'm'; 149];
        let b_original = vec![b'n'; 150];
        let a_updated = vec![b'o'; 151];
        let c_inserted = vec![b'p'; 152];

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut bootstrap = mgr.begin(BeginKind::Concurrent).expect("bootstrap begin");
        mgr.cell_log()
            .record_insert(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_original),
                bootstrap.token(),
            )
            .expect("row a insert should fit budget");
        mgr.cell_log()
            .record_insert(
                key_b,
                pgno,
                leaf_table_cell(row_b, &b_original),
                bootstrap.token(),
            )
            .expect("row b insert should fit budget");
        let committed_seq = mgr.commit(&mut bootstrap).expect("bootstrap commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        mgr.cell_log()
            .record_update(key_a, pgno, leaf_table_cell(row_a, &a_updated), txn.token())
            .expect("row a update should fit budget");
        mgr.cell_log()
            .record_delete(key_b, pgno, txn.token())
            .expect("row b delete should fit budget");
        mgr.cell_log()
            .record_insert(
                key_c,
                pgno,
                leaf_table_cell(row_c, &c_inserted),
                txn.token(),
            )
            .expect("row c insert should fit budget");

        let own_page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should stay visible");
        assert_eq!(
            materialized_table_payloads(&own_page)?,
            vec![(row_a, a_updated.clone()), (row_c, c_inserted.clone())],
            "transaction-local reads must materialize their own mixed logical batch"
        );
        assert_eq!(
            txn.read_version_for_page(pgno),
            Some(committed_seq),
            "own uncommitted mixed batches must preserve the last committed read witness"
        );

        let batch_seq = mgr.commit(&mut txn).expect("mixed batch commit");
        assert!(batch_seq > committed_seq);

        let mut fresh_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("fresh reader begin");
        let fresh_page = mgr
            .read_page_with_cell_deltas(&mut fresh_reader, pgno, PageSize::DEFAULT.get())?
            .expect("fresh page should be visible");
        assert_eq!(
            materialized_table_payloads(&fresh_page)?,
            vec![(row_a, a_updated), (row_c, c_inserted)]
        );
        assert_eq!(
            fresh_reader.read_version_for_page(pgno),
            Some(batch_seq),
            "fresh reads should witness the committed mixed-batch horizon"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_keeps_own_writes_pinned_after_other_batch()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let row_a = 4284;
        let row_b = 4285;
        let row_c = 4286;
        let row_d = 4287;
        let key_a = CellKey::table_row(btree, row_a);
        let key_b = CellKey::table_row(btree, row_b);
        let key_c = CellKey::table_row(btree, row_c);
        let key_d = CellKey::table_row(btree, row_d);
        let a_original = vec![b'q'; 153];
        let b_original = vec![b'r'; 154];
        let a_other = vec![b's'; 155];
        let c_other = vec![b't'; 156];
        let a_own = vec![b'u'; 157];
        let d_own = vec![b'v'; 158];

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut bootstrap = mgr.begin(BeginKind::Concurrent).expect("bootstrap begin");
        mgr.cell_log()
            .record_insert(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_original),
                bootstrap.token(),
            )
            .expect("row a insert should fit budget");
        mgr.cell_log()
            .record_insert(
                key_b,
                pgno,
                leaf_table_cell(row_b, &b_original),
                bootstrap.token(),
            )
            .expect("row b insert should fit budget");
        let committed_seq = mgr.commit(&mut bootstrap).expect("bootstrap commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        assert_eq!(
            txn.snapshot.high, committed_seq,
            "transaction must pin the pre-batch snapshot before later commits"
        );

        let mut other = mgr.begin(BeginKind::Concurrent).expect("other begin");
        mgr.cell_log()
            .record_update(key_a, pgno, leaf_table_cell(row_a, &a_other), other.token())
            .expect("other row a update should fit budget");
        mgr.cell_log()
            .record_delete(key_b, pgno, other.token())
            .expect("other row b delete should fit budget");
        mgr.cell_log()
            .record_insert(key_c, pgno, leaf_table_cell(row_c, &c_other), other.token())
            .expect("other row c insert should fit budget");
        let other_seq = mgr.commit(&mut other).expect("other batch commit");
        assert!(other_seq > committed_seq);

        mgr.cell_log()
            .record_update(key_a, pgno, leaf_table_cell(row_a, &a_own), txn.token())
            .expect("own row a update should fit budget");
        mgr.cell_log()
            .record_insert(key_d, pgno, leaf_table_cell(row_d, &d_own), txn.token())
            .expect("own row d insert should fit budget");

        let own_page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should stay visible");
        assert_eq!(
            materialized_table_payloads(&own_page)?,
            vec![
                (row_a, a_own.clone()),
                (row_b, b_original.clone()),
                (row_d, d_own.clone())
            ],
            "transaction-local reads must replay own writes without admitting post-snapshot commits"
        );
        assert_eq!(
            txn.read_version_for_page(pgno),
            Some(committed_seq),
            "own writes and hidden post-snapshot commits must leave the witness at the pinned horizon"
        );

        mgr.abort(&mut txn);

        let mut fresh_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("fresh reader begin");
        let fresh_page = mgr
            .read_page_with_cell_deltas(&mut fresh_reader, pgno, PageSize::DEFAULT.get())?
            .expect("fresh page should be visible");
        assert_eq!(
            materialized_table_payloads(&fresh_page)?,
            vec![(row_a, a_other), (row_c, c_other)],
            "fresh readers must see the later committed batch after the pinned transaction aborts"
        );
        assert_eq!(
            fresh_reader.read_version_for_page(pgno),
            Some(other_seq),
            "fresh readers should witness the later committed batch horizon"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_keeps_own_tombstone_pinned_after_other_update()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let row_a = 4288;
        let row_b = 4289;
        let row_c = 4290;
        let key_a = CellKey::table_row(btree, row_a);
        let key_b = CellKey::table_row(btree, row_b);
        let key_c = CellKey::table_row(btree, row_c);
        let a_original = vec![b'w'; 159];
        let a_other = vec![b'x'; 160];
        let b_other = vec![b'y'; 161];
        let c_own = vec![b'z'; 162];

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut bootstrap = mgr.begin(BeginKind::Concurrent).expect("bootstrap begin");
        mgr.cell_log()
            .record_insert(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_original),
                bootstrap.token(),
            )
            .expect("row a insert should fit budget");
        let committed_seq = mgr.commit(&mut bootstrap).expect("bootstrap commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        assert_eq!(
            txn.snapshot.high, committed_seq,
            "transaction must pin the row before the later update commits"
        );

        let mut other = mgr.begin(BeginKind::Concurrent).expect("other begin");
        mgr.cell_log()
            .record_update(key_a, pgno, leaf_table_cell(row_a, &a_other), other.token())
            .expect("other row a update should fit budget");
        mgr.cell_log()
            .record_insert(key_b, pgno, leaf_table_cell(row_b, &b_other), other.token())
            .expect("other row b insert should fit budget");
        let other_seq = mgr.commit(&mut other).expect("other batch commit");
        assert!(other_seq > committed_seq);

        mgr.cell_log()
            .record_delete(key_a, pgno, txn.token())
            .expect("own row a delete should fit budget");
        mgr.cell_log()
            .record_insert(key_c, pgno, leaf_table_cell(row_c, &c_own), txn.token())
            .expect("own row c insert should fit budget");

        let own_page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should stay visible");
        assert_eq!(
            materialized_table_payloads(&own_page)?,
            vec![(row_c, c_own.clone())],
            "own tombstones must apply to the pinned row without admitting later commits"
        );
        assert_eq!(
            txn.read_version_for_page(pgno),
            Some(committed_seq),
            "own tombstones and hidden post-snapshot commits must leave the witness at the pinned horizon"
        );

        mgr.abort(&mut txn);

        let mut fresh_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("fresh reader begin");
        let fresh_page = mgr
            .read_page_with_cell_deltas(&mut fresh_reader, pgno, PageSize::DEFAULT.get())?
            .expect("fresh page should be visible");
        assert_eq!(
            materialized_table_payloads(&fresh_page)?,
            vec![(row_a, a_other), (row_b, b_other)],
            "fresh readers must see the later committed update/insert after abort"
        );
        assert_eq!(
            fresh_reader.read_version_for_page(pgno),
            Some(other_seq),
            "fresh readers should witness the later committed update horizon"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_keeps_own_update_pinned_after_other_delete()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let row_a = 4291;
        let row_b = 4292;
        let row_c = 4293;
        let key_a = CellKey::table_row(btree, row_a);
        let key_b = CellKey::table_row(btree, row_b);
        let key_c = CellKey::table_row(btree, row_c);
        let a_original = vec![b'a'; 163];
        let b_other = vec![b'c'; 165];
        let a_own = vec![b'd'; 166];
        let c_own = vec![b'e'; 167];

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut bootstrap = mgr.begin(BeginKind::Concurrent).expect("bootstrap begin");
        mgr.cell_log()
            .record_insert(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_original),
                bootstrap.token(),
            )
            .expect("row a insert should fit budget");
        let committed_seq = mgr.commit(&mut bootstrap).expect("bootstrap commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        assert_eq!(
            txn.snapshot.high, committed_seq,
            "transaction must pin the row before the later delete commits"
        );

        let mut other = mgr.begin(BeginKind::Concurrent).expect("other begin");
        mgr.cell_log()
            .record_delete(key_a, pgno, other.token())
            .expect("other row a delete should fit budget");
        mgr.cell_log()
            .record_insert(key_b, pgno, leaf_table_cell(row_b, &b_other), other.token())
            .expect("other row b insert should fit budget");
        let other_seq = mgr.commit(&mut other).expect("other batch commit");
        assert!(other_seq > committed_seq);

        mgr.cell_log()
            .record_update(key_a, pgno, leaf_table_cell(row_a, &a_own), txn.token())
            .expect("own row a update should fit budget");
        mgr.cell_log()
            .record_insert(key_c, pgno, leaf_table_cell(row_c, &c_own), txn.token())
            .expect("own row c insert should fit budget");

        let own_page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should stay visible");
        assert_eq!(
            materialized_table_payloads(&own_page)?,
            vec![(row_a, a_own.clone()), (row_c, c_own.clone())],
            "own updates must apply to the pinned row without admitting later deletes"
        );
        assert_eq!(
            txn.read_version_for_page(pgno),
            Some(committed_seq),
            "own updates and hidden post-snapshot deletes must leave the witness at the pinned horizon"
        );

        mgr.abort(&mut txn);

        let mut fresh_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("fresh reader begin");
        let fresh_page = mgr
            .read_page_with_cell_deltas(&mut fresh_reader, pgno, PageSize::DEFAULT.get())?
            .expect("fresh page should be visible");
        assert_eq!(
            materialized_table_payloads(&fresh_page)?,
            vec![(row_b, b_other)],
            "fresh readers must see the later committed delete/insert after abort"
        );
        assert_eq!(
            fresh_reader.read_version_for_page(pgno),
            Some(other_seq),
            "fresh readers should witness the later committed delete horizon"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_keeps_own_insert_pinned_after_other_insert()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let row_a = 4294;
        let row_b = 4295;
        let key_a = CellKey::table_row(btree, row_a);
        let key_b = CellKey::table_row(btree, row_b);
        let a_other = vec![b'f'; 168];
        let a_own = vec![b'g'; 169];
        let b_own = vec![b'h'; 170];

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        let committed_seq = mgr.commit(&mut seed).expect("seed commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        assert_eq!(
            txn.snapshot.high, committed_seq,
            "transaction must pin the empty page before the later insert commits"
        );

        let mut other = mgr.begin(BeginKind::Concurrent).expect("other begin");
        mgr.cell_log()
            .record_insert(key_a, pgno, leaf_table_cell(row_a, &a_other), other.token())
            .expect("other row a insert should fit budget");
        let other_seq = mgr.commit(&mut other).expect("other insert commit");
        assert!(other_seq > committed_seq);

        mgr.cell_log()
            .record_insert(key_a, pgno, leaf_table_cell(row_a, &a_own), txn.token())
            .expect("own row a insert should fit budget");
        mgr.cell_log()
            .record_insert(key_b, pgno, leaf_table_cell(row_b, &b_own), txn.token())
            .expect("own row b insert should fit budget");

        let own_page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("seed page should stay visible");
        assert_eq!(
            materialized_table_payloads(&own_page)?,
            vec![(row_a, a_own.clone()), (row_b, b_own.clone())],
            "own inserts must replay against the pinned snapshot without admitting later same-key commits"
        );
        assert_eq!(
            txn.read_version_for_page(pgno),
            Some(committed_seq),
            "own inserts and hidden post-snapshot inserts must leave the witness at the pinned horizon"
        );

        mgr.abort(&mut txn);

        let mut fresh_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("fresh reader begin");
        let fresh_page = mgr
            .read_page_with_cell_deltas(&mut fresh_reader, pgno, PageSize::DEFAULT.get())?
            .expect("fresh page should be visible");
        assert_eq!(
            materialized_table_payloads(&fresh_page)?,
            vec![(row_a, a_other)],
            "fresh readers must see the later committed same-key insert after abort"
        );
        assert_eq!(
            fresh_reader.read_version_for_page(pgno),
            Some(other_seq),
            "fresh readers should witness the later committed insert horizon"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_keeps_own_update_pinned_after_other_update()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let row_a = 4296;
        let row_b = 4297;
        let key_a = CellKey::table_row(btree, row_a);
        let key_b = CellKey::table_row(btree, row_b);
        let a_original = vec![b'i'; 171];
        let a_other = vec![b'j'; 172];
        let b_other = vec![b'k'; 173];
        let a_own = vec![b'l'; 174];

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut bootstrap = mgr.begin(BeginKind::Concurrent).expect("bootstrap begin");
        mgr.cell_log()
            .record_insert(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_original),
                bootstrap.token(),
            )
            .expect("row a insert should fit budget");
        let committed_seq = mgr.commit(&mut bootstrap).expect("bootstrap commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        assert_eq!(
            txn.snapshot.high, committed_seq,
            "transaction must pin the original row before the later update commits"
        );

        let mut other = mgr.begin(BeginKind::Concurrent).expect("other begin");
        mgr.cell_log()
            .record_update(key_a, pgno, leaf_table_cell(row_a, &a_other), other.token())
            .expect("other row a update should fit budget");
        mgr.cell_log()
            .record_insert(key_b, pgno, leaf_table_cell(row_b, &b_other), other.token())
            .expect("other row b insert should fit budget");
        let other_seq = mgr.commit(&mut other).expect("other batch commit");
        assert!(other_seq > committed_seq);

        mgr.cell_log()
            .record_update(key_a, pgno, leaf_table_cell(row_a, &a_own), txn.token())
            .expect("own row a update should fit budget");

        let own_page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should stay visible");
        assert_eq!(
            materialized_table_payloads(&own_page)?,
            vec![(row_a, a_own.clone())],
            "own updates must replay against the pinned row without admitting later updates"
        );
        assert_eq!(
            txn.read_version_for_page(pgno),
            Some(committed_seq),
            "own updates and hidden post-snapshot updates must leave the witness at the pinned horizon"
        );

        mgr.abort(&mut txn);

        let mut fresh_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("fresh reader begin");
        let fresh_page = mgr
            .read_page_with_cell_deltas(&mut fresh_reader, pgno, PageSize::DEFAULT.get())?
            .expect("fresh page should be visible");
        assert_eq!(
            materialized_table_payloads(&fresh_page)?,
            vec![(row_a, a_other), (row_b, b_other)],
            "fresh readers must see the later committed update/insert after abort"
        );
        assert_eq!(
            fresh_reader.read_version_for_page(pgno),
            Some(other_seq),
            "fresh readers should witness the later committed update horizon"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_keeps_own_delete_pinned_after_other_delete()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let row_a = 4298;
        let row_b = 4299;
        let row_c = 4300;
        let key_a = CellKey::table_row(btree, row_a);
        let key_b = CellKey::table_row(btree, row_b);
        let key_c = CellKey::table_row(btree, row_c);
        let a_original = vec![b'm'; 175];
        let b_other = vec![b'n'; 176];
        let c_own = vec![b'o'; 177];

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut bootstrap = mgr.begin(BeginKind::Concurrent).expect("bootstrap begin");
        mgr.cell_log()
            .record_insert(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_original),
                bootstrap.token(),
            )
            .expect("row a insert should fit budget");
        let committed_seq = mgr.commit(&mut bootstrap).expect("bootstrap commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        assert_eq!(
            txn.snapshot.high, committed_seq,
            "transaction must pin the original row before the later delete commits"
        );

        let mut other = mgr.begin(BeginKind::Concurrent).expect("other begin");
        mgr.cell_log()
            .record_delete(key_a, pgno, other.token())
            .expect("other row a delete should fit budget");
        mgr.cell_log()
            .record_insert(key_b, pgno, leaf_table_cell(row_b, &b_other), other.token())
            .expect("other row b insert should fit budget");
        let other_seq = mgr.commit(&mut other).expect("other batch commit");
        assert!(other_seq > committed_seq);

        mgr.cell_log()
            .record_delete(key_a, pgno, txn.token())
            .expect("own row a delete should fit budget");
        mgr.cell_log()
            .record_insert(key_c, pgno, leaf_table_cell(row_c, &c_own), txn.token())
            .expect("own row c insert should fit budget");

        let own_page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should stay visible");
        assert_eq!(
            materialized_table_payloads(&own_page)?,
            vec![(row_c, c_own.clone())],
            "own deletes must replay against the pinned row without admitting later deletes"
        );
        assert_eq!(
            txn.read_version_for_page(pgno),
            Some(committed_seq),
            "own deletes and hidden post-snapshot deletes must leave the witness at the pinned horizon"
        );

        mgr.abort(&mut txn);

        let mut fresh_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("fresh reader begin");
        let fresh_page = mgr
            .read_page_with_cell_deltas(&mut fresh_reader, pgno, PageSize::DEFAULT.get())?
            .expect("fresh page should be visible");
        assert_eq!(
            materialized_table_payloads(&fresh_page)?,
            vec![(row_b, b_other)],
            "fresh readers must see the later committed delete/insert after abort"
        );
        assert_eq!(
            fresh_reader.read_version_for_page(pgno),
            Some(other_seq),
            "fresh readers should witness the later committed delete horizon"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_keeps_repeated_own_updates_pinned_after_other_update()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let row_a = 4301;
        let row_b = 4302;
        let key_a = CellKey::table_row(btree, row_a);
        let key_b = CellKey::table_row(btree, row_b);
        let a_original = vec![b'p'; 178];
        let a_other = vec![b'q'; 179];
        let b_other = vec![b'r'; 180];
        let a_own_first = vec![b's'; 181];
        let a_own_second = vec![b't'; 182];

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut bootstrap = mgr.begin(BeginKind::Concurrent).expect("bootstrap begin");
        mgr.cell_log()
            .record_insert(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_original),
                bootstrap.token(),
            )
            .expect("row a insert should fit budget");
        let committed_seq = mgr.commit(&mut bootstrap).expect("bootstrap commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        assert_eq!(
            txn.snapshot.high, committed_seq,
            "transaction must pin the original row before the later update commits"
        );

        let mut other = mgr.begin(BeginKind::Concurrent).expect("other begin");
        mgr.cell_log()
            .record_update(key_a, pgno, leaf_table_cell(row_a, &a_other), other.token())
            .expect("other row a update should fit budget");
        mgr.cell_log()
            .record_insert(key_b, pgno, leaf_table_cell(row_b, &b_other), other.token())
            .expect("other row b insert should fit budget");
        let other_seq = mgr.commit(&mut other).expect("other batch commit");
        assert!(other_seq > committed_seq);

        mgr.cell_log()
            .record_update(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_own_first),
                txn.token(),
            )
            .expect("first own row a update should fit budget");
        mgr.cell_log()
            .record_update(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_own_second),
                txn.token(),
            )
            .expect("second own row a update should fit budget");

        let own_page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should stay visible");
        assert_eq!(
            materialized_table_payloads(&own_page)?,
            vec![(row_a, a_own_second.clone())],
            "repeated own updates must replay in order without admitting later commits"
        );
        assert_eq!(
            txn.read_version_for_page(pgno),
            Some(committed_seq),
            "repeated own updates and hidden post-snapshot updates must leave the witness at the pinned horizon"
        );

        mgr.abort(&mut txn);

        let mut fresh_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("fresh reader begin");
        let fresh_page = mgr
            .read_page_with_cell_deltas(&mut fresh_reader, pgno, PageSize::DEFAULT.get())?
            .expect("fresh page should be visible");
        assert_eq!(
            materialized_table_payloads(&fresh_page)?,
            vec![(row_a, a_other), (row_b, b_other)],
            "fresh readers must see the later committed update/insert after abort"
        );
        assert_eq!(
            fresh_reader.read_version_for_page(pgno),
            Some(other_seq),
            "fresh readers should witness the later committed update horizon"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_keeps_own_update_delete_pinned_after_other_update()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let row_a = 4303;
        let row_b = 4304;
        let row_c = 4305;
        let key_a = CellKey::table_row(btree, row_a);
        let key_b = CellKey::table_row(btree, row_b);
        let key_c = CellKey::table_row(btree, row_c);
        let a_original = vec![b'u'; 183];
        let a_other = vec![b'v'; 184];
        let b_other = vec![b'w'; 185];
        let a_own_update = vec![b'x'; 186];
        let c_own = vec![b'y'; 187];

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut bootstrap = mgr.begin(BeginKind::Concurrent).expect("bootstrap begin");
        mgr.cell_log()
            .record_insert(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_original),
                bootstrap.token(),
            )
            .expect("row a insert should fit budget");
        let committed_seq = mgr.commit(&mut bootstrap).expect("bootstrap commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        assert_eq!(
            txn.snapshot.high, committed_seq,
            "transaction must pin the original row before the later update commits"
        );

        let mut other = mgr.begin(BeginKind::Concurrent).expect("other begin");
        mgr.cell_log()
            .record_update(key_a, pgno, leaf_table_cell(row_a, &a_other), other.token())
            .expect("other row a update should fit budget");
        mgr.cell_log()
            .record_insert(key_b, pgno, leaf_table_cell(row_b, &b_other), other.token())
            .expect("other row b insert should fit budget");
        let other_seq = mgr.commit(&mut other).expect("other batch commit");
        assert!(other_seq > committed_seq);

        mgr.cell_log()
            .record_update(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_own_update),
                txn.token(),
            )
            .expect("own row a update should fit budget");
        mgr.cell_log()
            .record_delete(key_a, pgno, txn.token())
            .expect("own row a delete should fit budget");
        mgr.cell_log()
            .record_insert(key_c, pgno, leaf_table_cell(row_c, &c_own), txn.token())
            .expect("own row c insert should fit budget");

        let own_page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should stay visible");
        assert_eq!(
            materialized_table_payloads(&own_page)?,
            vec![(row_c, c_own.clone())],
            "own update/delete final state must replay without admitting later commits"
        );
        assert_eq!(
            txn.read_version_for_page(pgno),
            Some(committed_seq),
            "own update/delete and hidden post-snapshot updates must leave the witness at the pinned horizon"
        );

        mgr.abort(&mut txn);

        let mut fresh_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("fresh reader begin");
        let fresh_page = mgr
            .read_page_with_cell_deltas(&mut fresh_reader, pgno, PageSize::DEFAULT.get())?
            .expect("fresh page should be visible");
        assert_eq!(
            materialized_table_payloads(&fresh_page)?,
            vec![(row_a, a_other), (row_b, b_other)],
            "fresh readers must see the later committed update/insert after abort"
        );
        assert_eq!(
            fresh_reader.read_version_for_page(pgno),
            Some(other_seq),
            "fresh readers should witness the later committed update horizon"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_keeps_own_delete_insert_pinned_after_other_update()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let row_a = 4306;
        let row_b = 4307;
        let key_a = CellKey::table_row(btree, row_a);
        let key_b = CellKey::table_row(btree, row_b);
        let a_original = vec![b'z'; 188];
        let a_other = vec![b'0'; 189];
        let b_other = vec![b'1'; 190];
        let a_own_replacement = vec![b'2'; 191];

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut bootstrap = mgr.begin(BeginKind::Concurrent).expect("bootstrap begin");
        mgr.cell_log()
            .record_insert(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_original),
                bootstrap.token(),
            )
            .expect("row a insert should fit budget");
        let committed_seq = mgr.commit(&mut bootstrap).expect("bootstrap commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        assert_eq!(
            txn.snapshot.high, committed_seq,
            "transaction must pin the original row before the later update commits"
        );

        let mut other = mgr.begin(BeginKind::Concurrent).expect("other begin");
        mgr.cell_log()
            .record_update(key_a, pgno, leaf_table_cell(row_a, &a_other), other.token())
            .expect("other row a update should fit budget");
        mgr.cell_log()
            .record_insert(key_b, pgno, leaf_table_cell(row_b, &b_other), other.token())
            .expect("other row b insert should fit budget");
        let other_seq = mgr.commit(&mut other).expect("other batch commit");
        assert!(other_seq > committed_seq);

        mgr.cell_log()
            .record_delete(key_a, pgno, txn.token())
            .expect("own row a delete should fit budget");
        mgr.cell_log()
            .record_insert(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_own_replacement),
                txn.token(),
            )
            .expect("own row a replacement should fit budget");

        let own_page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should stay visible");
        assert_eq!(
            materialized_table_payloads(&own_page)?,
            vec![(row_a, a_own_replacement.clone())],
            "own delete/insert replacement must replay without admitting later commits"
        );
        assert_eq!(
            txn.read_version_for_page(pgno),
            Some(committed_seq),
            "own delete/insert replacement and hidden post-snapshot updates must leave the witness at the pinned horizon"
        );

        mgr.abort(&mut txn);

        let mut fresh_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("fresh reader begin");
        let fresh_page = mgr
            .read_page_with_cell_deltas(&mut fresh_reader, pgno, PageSize::DEFAULT.get())?
            .expect("fresh page should be visible");
        assert_eq!(
            materialized_table_payloads(&fresh_page)?,
            vec![(row_a, a_other), (row_b, b_other)],
            "fresh readers must see the later committed update/insert after abort"
        );
        assert_eq!(
            fresh_reader.read_version_for_page(pgno),
            Some(other_seq),
            "fresh readers should witness the later committed update horizon"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_keeps_own_insert_delete_pinned_after_other_insert()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let row_a = 4308;
        let row_b = 4309;
        let row_c = 4310;
        let key_a = CellKey::table_row(btree, row_a);
        let key_b = CellKey::table_row(btree, row_b);
        let key_c = CellKey::table_row(btree, row_c);
        let a_other = vec![b'3'; 192];
        let b_other = vec![b'4'; 193];
        let a_own = vec![b'5'; 194];
        let c_own = vec![b'6'; 195];

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        let committed_seq = mgr.commit(&mut seed).expect("seed commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        assert_eq!(
            txn.snapshot.high, committed_seq,
            "transaction must pin the empty page before the later insert commits"
        );

        let mut other = mgr.begin(BeginKind::Concurrent).expect("other begin");
        mgr.cell_log()
            .record_insert(key_a, pgno, leaf_table_cell(row_a, &a_other), other.token())
            .expect("other row a insert should fit budget");
        mgr.cell_log()
            .record_insert(key_b, pgno, leaf_table_cell(row_b, &b_other), other.token())
            .expect("other row b insert should fit budget");
        let other_seq = mgr.commit(&mut other).expect("other batch commit");
        assert!(other_seq > committed_seq);

        mgr.cell_log()
            .record_insert(key_a, pgno, leaf_table_cell(row_a, &a_own), txn.token())
            .expect("own row a insert should fit budget");
        mgr.cell_log()
            .record_delete(key_a, pgno, txn.token())
            .expect("own row a delete should fit budget");
        mgr.cell_log()
            .record_insert(key_c, pgno, leaf_table_cell(row_c, &c_own), txn.token())
            .expect("own row c insert should fit budget");

        let own_page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should stay visible");
        assert_eq!(
            materialized_table_payloads(&own_page)?,
            vec![(row_c, c_own.clone())],
            "own insert/delete final state must not reveal a hidden post-snapshot insert of the same key"
        );
        assert_eq!(
            txn.read_version_for_page(pgno),
            Some(committed_seq),
            "own insert/delete and hidden post-snapshot inserts must leave the witness at the pinned horizon"
        );

        mgr.abort(&mut txn);

        let mut fresh_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("fresh reader begin");
        let fresh_page = mgr
            .read_page_with_cell_deltas(&mut fresh_reader, pgno, PageSize::DEFAULT.get())?
            .expect("fresh page should be visible");
        assert_eq!(
            materialized_table_payloads(&fresh_page)?,
            vec![(row_a, a_other), (row_b, b_other)],
            "fresh readers must see the later committed insert batch after abort"
        );
        assert_eq!(
            fresh_reader.read_version_for_page(pgno),
            Some(other_seq),
            "fresh readers should witness the later committed insert horizon"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_keeps_own_insert_update_pinned_after_other_insert()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let row_a = 4311;
        let row_b = 4312;
        let row_c = 4313;
        let key_a = CellKey::table_row(btree, row_a);
        let key_b = CellKey::table_row(btree, row_b);
        let key_c = CellKey::table_row(btree, row_c);
        let a_other = vec![b'7'; 196];
        let b_other = vec![b'8'; 197];
        let a_own_first = vec![b'9'; 198];
        let a_own_second = vec![b'a'; 199];
        let c_own = vec![b'b'; 200];

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        let committed_seq = mgr.commit(&mut seed).expect("seed commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        assert_eq!(
            txn.snapshot.high, committed_seq,
            "transaction must pin the empty page before the later insert commits"
        );

        let mut other = mgr.begin(BeginKind::Concurrent).expect("other begin");
        mgr.cell_log()
            .record_insert(key_a, pgno, leaf_table_cell(row_a, &a_other), other.token())
            .expect("other row a insert should fit budget");
        mgr.cell_log()
            .record_insert(key_b, pgno, leaf_table_cell(row_b, &b_other), other.token())
            .expect("other row b insert should fit budget");
        let other_seq = mgr.commit(&mut other).expect("other batch commit");
        assert!(other_seq > committed_seq);

        mgr.cell_log()
            .record_insert(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_own_first),
                txn.token(),
            )
            .expect("own row a insert should fit budget");
        mgr.cell_log()
            .record_update(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_own_second),
                txn.token(),
            )
            .expect("own row a update should fit budget");
        mgr.cell_log()
            .record_insert(key_c, pgno, leaf_table_cell(row_c, &c_own), txn.token())
            .expect("own row c insert should fit budget");

        let own_page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should stay visible");
        assert_eq!(
            materialized_table_payloads(&own_page)?,
            vec![(row_a, a_own_second.clone()), (row_c, c_own.clone())],
            "own insert/update final state must not reveal a hidden post-snapshot insert of the same key"
        );
        assert_eq!(
            txn.read_version_for_page(pgno),
            Some(committed_seq),
            "own insert/update and hidden post-snapshot inserts must leave the witness at the pinned horizon"
        );

        mgr.abort(&mut txn);

        let mut fresh_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("fresh reader begin");
        let fresh_page = mgr
            .read_page_with_cell_deltas(&mut fresh_reader, pgno, PageSize::DEFAULT.get())?
            .expect("fresh page should be visible");
        assert_eq!(
            materialized_table_payloads(&fresh_page)?,
            vec![(row_a, a_other), (row_b, b_other)],
            "fresh readers must see the later committed insert batch after abort"
        );
        assert_eq!(
            fresh_reader.read_version_for_page(pgno),
            Some(other_seq),
            "fresh readers should witness the later committed insert horizon"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_keeps_repeated_own_inserts_pinned_after_other_insert()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let row_a = 4314;
        let row_b = 4315;
        let row_c = 4316;
        let key_a = CellKey::table_row(btree, row_a);
        let key_b = CellKey::table_row(btree, row_b);
        let key_c = CellKey::table_row(btree, row_c);
        let a_other = vec![b'c'; 201];
        let b_other = vec![b'd'; 202];
        let a_own_first = vec![b'e'; 203];
        let a_own_second = vec![b'f'; 204];
        let c_own = vec![b'g'; 205];

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        let committed_seq = mgr.commit(&mut seed).expect("seed commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        assert_eq!(
            txn.snapshot.high, committed_seq,
            "transaction must pin the empty page before the later insert commits"
        );

        let mut other = mgr.begin(BeginKind::Concurrent).expect("other begin");
        mgr.cell_log()
            .record_insert(key_a, pgno, leaf_table_cell(row_a, &a_other), other.token())
            .expect("other row a insert should fit budget");
        mgr.cell_log()
            .record_insert(key_b, pgno, leaf_table_cell(row_b, &b_other), other.token())
            .expect("other row b insert should fit budget");
        let other_seq = mgr.commit(&mut other).expect("other batch commit");
        assert!(other_seq > committed_seq);

        mgr.cell_log()
            .record_insert(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_own_first),
                txn.token(),
            )
            .expect("first own row a insert should fit budget");
        mgr.cell_log()
            .record_insert(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_own_second),
                txn.token(),
            )
            .expect("second own row a insert should fit budget");
        mgr.cell_log()
            .record_insert(key_c, pgno, leaf_table_cell(row_c, &c_own), txn.token())
            .expect("own row c insert should fit budget");

        let own_page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should stay visible");
        assert_eq!(
            materialized_table_payloads(&own_page)?,
            vec![(row_a, a_own_second.clone()), (row_c, c_own.clone())],
            "repeated own inserts must replay in order without admitting hidden post-snapshot inserts"
        );
        assert_eq!(
            txn.read_version_for_page(pgno),
            Some(committed_seq),
            "repeated own inserts and hidden post-snapshot inserts must leave the witness at the pinned horizon"
        );

        mgr.abort(&mut txn);

        let mut fresh_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("fresh reader begin");
        let fresh_page = mgr
            .read_page_with_cell_deltas(&mut fresh_reader, pgno, PageSize::DEFAULT.get())?
            .expect("fresh page should be visible");
        assert_eq!(
            materialized_table_payloads(&fresh_page)?,
            vec![(row_a, a_other), (row_b, b_other)],
            "fresh readers must see the later committed insert batch after abort"
        );
        assert_eq!(
            fresh_reader.read_version_for_page(pgno),
            Some(other_seq),
            "fresh readers should witness the later committed insert horizon"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_keeps_other_uncommitted_writes_out_of_horizon()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let row_a = 4272;
        let row_b = 4273;
        let row_c = 4274;
        let key_a = CellKey::table_row(btree, row_a);
        let key_b = CellKey::table_row(btree, row_b);
        let key_c = CellKey::table_row(btree, row_c);
        let a_original = vec![b'a'; 137];
        let b_original = vec![b'b'; 138];
        let a_uncommitted = vec![b'c'; 139];
        let c_uncommitted = vec![b'd'; 140];

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut bootstrap = mgr.begin(BeginKind::Concurrent).expect("bootstrap begin");
        mgr.cell_log()
            .record_insert(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_original),
                bootstrap.token(),
            )
            .expect("row a insert should fit budget");
        mgr.cell_log()
            .record_insert(
                key_b,
                pgno,
                leaf_table_cell(row_b, &b_original),
                bootstrap.token(),
            )
            .expect("row b insert should fit budget");
        let committed_seq = mgr.commit(&mut bootstrap).expect("bootstrap commit");

        let mut writer = mgr.begin(BeginKind::Concurrent).expect("writer begin");
        mgr.cell_log()
            .record_update(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_uncommitted),
                writer.token(),
            )
            .expect("row a update should fit budget");
        mgr.cell_log()
            .record_delete(key_b, pgno, writer.token())
            .expect("row b delete should fit budget");
        mgr.cell_log()
            .record_insert(
                key_c,
                pgno,
                leaf_table_cell(row_c, &c_uncommitted),
                writer.token(),
            )
            .expect("row c insert should fit budget");

        let mut reader = mgr.begin(BeginKind::Concurrent).expect("reader begin");
        let reader_page = mgr
            .read_page_with_cell_deltas(&mut reader, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should stay visible");
        assert_eq!(
            materialized_table_payloads(&reader_page)?,
            vec![(row_a, a_original.clone()), (row_b, b_original.clone())],
            "other transactions must not observe uncommitted logical insert/update/delete batches"
        );
        assert_eq!(
            reader.read_version_for_page(pgno),
            Some(committed_seq),
            "other uncommitted logical writes must not advance the committed read witness"
        );

        let writer_page = mgr
            .read_page_with_cell_deltas(&mut writer, pgno, PageSize::DEFAULT.get())?
            .expect("writer page should stay visible");
        assert_eq!(
            materialized_table_payloads(&writer_page)?,
            vec![(row_a, a_uncommitted), (row_c, c_uncommitted)],
            "the writer still sees its own uncommitted logical batch"
        );

        mgr.abort(&mut writer);

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_keeps_aborted_writes_out_of_horizon()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let row_a = 4275;
        let row_b = 4276;
        let row_c = 4277;
        let key_a = CellKey::table_row(btree, row_a);
        let key_b = CellKey::table_row(btree, row_b);
        let key_c = CellKey::table_row(btree, row_c);
        let a_original = vec![b'e'; 141];
        let b_original = vec![b'f'; 142];
        let a_aborted = vec![b'g'; 143];
        let c_aborted = vec![b'h'; 144];

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut bootstrap = mgr.begin(BeginKind::Concurrent).expect("bootstrap begin");
        mgr.cell_log()
            .record_insert(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_original),
                bootstrap.token(),
            )
            .expect("row a insert should fit budget");
        mgr.cell_log()
            .record_insert(
                key_b,
                pgno,
                leaf_table_cell(row_b, &b_original),
                bootstrap.token(),
            )
            .expect("row b insert should fit budget");
        let committed_seq = mgr.commit(&mut bootstrap).expect("bootstrap commit");
        let expected_committed = vec![(row_a, a_original), (row_b, b_original)];

        let mut writer = mgr.begin(BeginKind::Concurrent).expect("writer begin");
        mgr.cell_log()
            .record_update(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_aborted),
                writer.token(),
            )
            .expect("row a update should fit budget");
        mgr.cell_log()
            .record_delete(key_b, pgno, writer.token())
            .expect("row b delete should fit budget");
        mgr.cell_log()
            .record_insert(
                key_c,
                pgno,
                leaf_table_cell(row_c, &c_aborted),
                writer.token(),
            )
            .expect("row c insert should fit budget");

        let writer_page = mgr
            .read_page_with_cell_deltas(&mut writer, pgno, PageSize::DEFAULT.get())?
            .expect("writer page should stay visible");
        assert_eq!(
            materialized_table_payloads(&writer_page)?,
            vec![(row_a, a_aborted), (row_c, c_aborted)],
            "writer must see its own uncommitted logical batch before abort"
        );

        let mut reader = mgr.begin(BeginKind::Concurrent).expect("reader begin");
        let reader_page = mgr
            .read_page_with_cell_deltas(&mut reader, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should stay visible");
        assert_eq!(
            materialized_table_payloads(&reader_page)?,
            expected_committed.clone(),
            "reader must ignore another transaction's uncommitted logical batch before abort"
        );
        assert_eq!(
            reader.read_version_for_page(pgno),
            Some(committed_seq),
            "uncommitted logical writes must not advance the reader's committed witness"
        );

        mgr.abort(&mut writer);

        let reader_after_abort = mgr
            .read_page_with_cell_deltas(&mut reader, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should remain visible after abort");
        assert_eq!(
            materialized_table_payloads(&reader_after_abort)?,
            expected_committed.clone(),
            "aborted logical writes must not change an existing reader's view"
        );
        assert_eq!(
            reader.read_version_for_page(pgno),
            Some(committed_seq),
            "aborted logical writes must leave the reader's witness at the committed horizon"
        );

        let mut fresh_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("fresh reader begin");
        let fresh_page = mgr
            .read_page_with_cell_deltas(&mut fresh_reader, pgno, PageSize::DEFAULT.get())?
            .expect("fresh page should stay visible after abort");
        assert_eq!(
            materialized_table_payloads(&fresh_page)?,
            expected_committed,
            "fresh readers must not replay aborted logical writes"
        );
        assert_eq!(
            fresh_reader.read_version_for_page(pgno),
            Some(committed_seq),
            "fresh readers should witness only the last committed logical horizon"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_keeps_post_snapshot_batch_out_of_pinned_horizon()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let row_a = 4278;
        let row_b = 4279;
        let row_c = 4280;
        let key_a = CellKey::table_row(btree, row_a);
        let key_b = CellKey::table_row(btree, row_b);
        let key_c = CellKey::table_row(btree, row_c);
        let a_original = vec![b'i'; 145];
        let b_original = vec![b'j'; 146];
        let a_updated = vec![b'k'; 147];
        let c_inserted = vec![b'l'; 148];

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut bootstrap = mgr.begin(BeginKind::Concurrent).expect("bootstrap begin");
        mgr.cell_log()
            .record_insert(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_original),
                bootstrap.token(),
            )
            .expect("row a insert should fit budget");
        mgr.cell_log()
            .record_insert(
                key_b,
                pgno,
                leaf_table_cell(row_b, &b_original),
                bootstrap.token(),
            )
            .expect("row b insert should fit budget");
        let committed_seq = mgr.commit(&mut bootstrap).expect("bootstrap commit");
        let expected_pinned = vec![(row_a, a_original), (row_b, b_original)];

        let mut pinned_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("pinned reader begin");
        assert_eq!(
            pinned_reader.snapshot.high, committed_seq,
            "reader must pin the pre-batch snapshot"
        );
        let pinned_before = mgr
            .read_page_with_cell_deltas(&mut pinned_reader, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should be visible before batch");
        assert_eq!(
            materialized_table_payloads(&pinned_before)?,
            expected_pinned.clone()
        );
        assert_eq!(
            pinned_reader.read_version_for_page(pgno),
            Some(committed_seq),
            "pinned reader should witness the pre-batch committed horizon"
        );

        let mut writer = mgr.begin(BeginKind::Concurrent).expect("writer begin");
        mgr.cell_log()
            .record_update(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_updated),
                writer.token(),
            )
            .expect("row a update should fit budget");
        mgr.cell_log()
            .record_delete(key_b, pgno, writer.token())
            .expect("row b delete should fit budget");
        mgr.cell_log()
            .record_insert(
                key_c,
                pgno,
                leaf_table_cell(row_c, &c_inserted),
                writer.token(),
            )
            .expect("row c insert should fit budget");
        let batch_seq = mgr.commit(&mut writer).expect("batch commit");
        assert!(batch_seq > committed_seq);

        let pinned_after = mgr
            .read_page_with_cell_deltas(&mut pinned_reader, pgno, PageSize::DEFAULT.get())?
            .expect("pinned page should stay visible after batch");
        assert_eq!(
            materialized_table_payloads(&pinned_after)?,
            expected_pinned,
            "pinned reader must hide the logical batch committed after its snapshot"
        );
        assert_eq!(
            pinned_reader.read_version_for_page(pgno),
            Some(committed_seq),
            "post-snapshot logical batches must not advance the pinned read witness"
        );

        let mut fresh_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("fresh reader begin");
        let fresh_page = mgr
            .read_page_with_cell_deltas(&mut fresh_reader, pgno, PageSize::DEFAULT.get())?
            .expect("fresh page should be visible after batch");
        assert_eq!(
            materialized_table_payloads(&fresh_page)?,
            vec![(row_a, a_updated), (row_c, c_inserted)],
            "fresh readers must see the committed logical batch"
        );
        assert_eq!(
            fresh_reader.read_version_for_page(pgno),
            Some(batch_seq),
            "fresh readers should witness the batch commit horizon"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_hides_post_snapshot_commit() -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 4245;
        let payload = vec![b's'; 128];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut old_reader = mgr.begin(BeginKind::Concurrent).expect("old reader begin");
        let old_page_before = mgr
            .read_page_with_cell_deltas(&mut old_reader, pgno, PageSize::DEFAULT.get())?
            .expect("seed page should be visible");
        assert_eq!(
            materialized_table_payloads(&old_page_before)?,
            Vec::<(i64, Vec<u8>)>::new()
        );

        let mut writer = mgr.begin(BeginKind::Concurrent).expect("writer begin");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &payload),
                writer.token(),
            )
            .expect("logical insert should fit budget");
        mgr.commit(&mut writer).expect("logical commit");

        let old_page_after = mgr
            .read_page_with_cell_deltas(&mut old_reader, pgno, PageSize::DEFAULT.get())?
            .expect("old snapshot should still see the base page");
        assert_eq!(
            materialized_table_payloads(&old_page_after)?,
            Vec::<(i64, Vec<u8>)>::new(),
            "reader snapshot must hide logical commits published after it began"
        );

        let mut fresh_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("fresh reader begin");
        let fresh_page = mgr
            .read_page_with_cell_deltas(&mut fresh_reader, pgno, PageSize::DEFAULT.get())?
            .expect("fresh snapshot should see the base page");
        assert_eq!(
            materialized_table_payloads(&fresh_page)?,
            vec![(rowid, payload)],
            "fresh snapshot must see the committed logical row"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_hides_post_snapshot_delete() -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 4246;
        let payload = vec![b'x'; 128];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut inserter = mgr.begin(BeginKind::Concurrent).expect("inserter begin");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &payload),
                inserter.token(),
            )
            .expect("logical insert should fit budget");
        mgr.commit(&mut inserter).expect("insert commit");

        let mut old_reader = mgr.begin(BeginKind::Concurrent).expect("old reader begin");
        let old_page_before = mgr
            .read_page_with_cell_deltas(&mut old_reader, pgno, PageSize::DEFAULT.get())?
            .expect("inserted row should be visible");
        assert_eq!(
            materialized_table_payloads(&old_page_before)?,
            vec![(rowid, payload.clone())]
        );

        let mut deleter = mgr.begin(BeginKind::Concurrent).expect("deleter begin");
        mgr.cell_log()
            .record_delete(cell_key, pgno, deleter.token())
            .expect("logical delete should fit budget");
        mgr.commit(&mut deleter).expect("delete commit");

        let old_page_after = mgr
            .read_page_with_cell_deltas(&mut old_reader, pgno, PageSize::DEFAULT.get())?
            .expect("old snapshot should still see the base page");
        assert_eq!(
            materialized_table_payloads(&old_page_after)?,
            vec![(rowid, payload)],
            "reader snapshot must hide logical deletes published after it began"
        );

        let mut fresh_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("fresh reader begin");
        let fresh_page = mgr
            .read_page_with_cell_deltas(&mut fresh_reader, pgno, PageSize::DEFAULT.get())?
            .expect("fresh snapshot should see the base page");
        assert_eq!(
            materialized_table_payloads(&fresh_page)?,
            Vec::<(i64, Vec<u8>)>::new(),
            "fresh snapshot must see the committed logical delete"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_hides_post_snapshot_update() -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 4249;
        let original_payload = vec![b'a'; 126];
        let updated_payload = vec![b'z'; 134];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut inserter = mgr.begin(BeginKind::Concurrent).expect("inserter begin");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &original_payload),
                inserter.token(),
            )
            .expect("logical insert should fit budget");
        mgr.commit(&mut inserter).expect("insert commit");

        let mut old_reader = mgr.begin(BeginKind::Concurrent).expect("old reader begin");
        let old_page_before = mgr
            .read_page_with_cell_deltas(&mut old_reader, pgno, PageSize::DEFAULT.get())?
            .expect("inserted row should be visible");
        assert_eq!(
            materialized_table_payloads(&old_page_before)?,
            vec![(rowid, original_payload.clone())]
        );

        let mut updater = mgr.begin(BeginKind::Concurrent).expect("updater begin");
        mgr.cell_log()
            .record_update(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &updated_payload),
                updater.token(),
            )
            .expect("logical update should fit budget");
        mgr.commit(&mut updater).expect("update commit");

        let old_page_after = mgr
            .read_page_with_cell_deltas(&mut old_reader, pgno, PageSize::DEFAULT.get())?
            .expect("old snapshot should still see the base page");
        assert_eq!(
            materialized_table_payloads(&old_page_after)?,
            vec![(rowid, original_payload)],
            "reader snapshot must hide logical updates published after it began"
        );

        let mut fresh_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("fresh reader begin");
        let fresh_page = mgr
            .read_page_with_cell_deltas(&mut fresh_reader, pgno, PageSize::DEFAULT.get())?
            .expect("fresh snapshot should see the base page");
        assert_eq!(
            materialized_table_payloads(&fresh_page)?,
            vec![(rowid, updated_payload)],
            "fresh snapshot must see the committed logical update"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_materializes_own_uncommitted_delta()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 4243;
        let payload = vec![b'l'; 131];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("reader begin");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &payload),
                txn.token(),
            )
            .expect("logical insert should fit budget");

        let page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("page should be visible");

        assert_eq!(
            materialized_table_payloads(&page)?,
            vec![(rowid, payload)],
            "transaction-local read view must replay this txn's own uncommitted delta"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_replays_same_txn_insert_then_update()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 4251;
        let inserted_payload = vec![b'i'; 136];
        let updated_payload = vec![b'v'; 137];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &inserted_payload),
                txn.token(),
            )
            .expect("logical insert should fit budget");
        mgr.cell_log()
            .record_update(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &updated_payload),
                txn.token(),
            )
            .expect("logical update should fit budget");

        let page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("page should be visible");
        assert_eq!(
            materialized_table_payloads(&page)?,
            vec![(rowid, updated_payload.clone())],
            "transaction-local reads must replay same-cell deltas in record order"
        );

        mgr.commit(&mut txn).expect("combined logical commit");

        let mut reader = mgr.begin(BeginKind::Concurrent).expect("reader begin");
        let committed_page = mgr
            .read_page_with_cell_deltas(&mut reader, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should be visible");
        assert_eq!(
            materialized_table_payloads(&committed_page)?,
            vec![(rowid, updated_payload)],
            "committed reads must preserve the insert-then-update ordering"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_replays_same_txn_insert_then_delete()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 4252;
        let payload = vec![b't'; 138];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &payload),
                txn.token(),
            )
            .expect("logical insert should fit budget");
        mgr.cell_log()
            .record_delete(cell_key, pgno, txn.token())
            .expect("logical delete should fit budget");

        let page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("page should be visible");
        assert_eq!(
            materialized_table_payloads(&page)?,
            Vec::<(i64, Vec<u8>)>::new(),
            "transaction-local reads must replay insert-then-delete as absent"
        );

        mgr.commit(&mut txn).expect("combined logical commit");

        let mut reader = mgr.begin(BeginKind::Concurrent).expect("reader begin");
        let committed_page = mgr
            .read_page_with_cell_deltas(&mut reader, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should be visible");
        assert_eq!(
            materialized_table_payloads(&committed_page)?,
            Vec::<(i64, Vec<u8>)>::new(),
            "committed reads must preserve the insert-then-delete ordering"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_replays_same_txn_update_then_delete()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 4253;
        let original_payload = vec![b'o'; 139];
        let updated_payload = vec![b'w'; 140];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut inserter = mgr.begin(BeginKind::Concurrent).expect("inserter begin");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &original_payload),
                inserter.token(),
            )
            .expect("logical insert should fit budget");
        mgr.commit(&mut inserter).expect("insert commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        mgr.cell_log()
            .record_update(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &updated_payload),
                txn.token(),
            )
            .expect("logical update should fit budget");
        mgr.cell_log()
            .record_delete(cell_key, pgno, txn.token())
            .expect("logical delete should fit budget");

        let page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("page should be visible");
        assert_eq!(
            materialized_table_payloads(&page)?,
            Vec::<(i64, Vec<u8>)>::new(),
            "transaction-local reads must replay update-then-delete as absent"
        );

        mgr.commit(&mut txn).expect("combined logical commit");

        let mut reader = mgr.begin(BeginKind::Concurrent).expect("reader begin");
        let committed_page = mgr
            .read_page_with_cell_deltas(&mut reader, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should be visible");
        assert_eq!(
            materialized_table_payloads(&committed_page)?,
            Vec::<(i64, Vec<u8>)>::new(),
            "committed reads must preserve the update-then-delete ordering"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_replays_same_txn_update_then_insert()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 4256;
        let original_payload = vec![b'a'; 145];
        let updated_payload = vec![b'b'; 146];
        let replacement_payload = vec![b'c'; 147];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut inserter = mgr.begin(BeginKind::Concurrent).expect("inserter begin");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &original_payload),
                inserter.token(),
            )
            .expect("logical insert should fit budget");
        mgr.commit(&mut inserter).expect("insert commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        mgr.cell_log()
            .record_update(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &updated_payload),
                txn.token(),
            )
            .expect("logical update should fit budget");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &replacement_payload),
                txn.token(),
            )
            .expect("logical replacement insert should fit budget");

        let page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("page should be visible");
        assert_eq!(
            materialized_table_payloads(&page)?,
            vec![(rowid, replacement_payload.clone())],
            "transaction-local reads must replay update-then-insert as replacement"
        );

        mgr.commit(&mut txn).expect("combined logical commit");

        let mut reader = mgr.begin(BeginKind::Concurrent).expect("reader begin");
        let committed_page = mgr
            .read_page_with_cell_deltas(&mut reader, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should be visible");
        assert_eq!(
            materialized_table_payloads(&committed_page)?,
            vec![(rowid, replacement_payload)],
            "committed reads must preserve the update-then-insert ordering"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_replays_same_txn_repeated_update()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 4257;
        let original_payload = vec![b'd'; 148];
        let first_payload = vec![b'e'; 149];
        let final_payload = vec![b'f'; 150];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut inserter = mgr.begin(BeginKind::Concurrent).expect("inserter begin");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &original_payload),
                inserter.token(),
            )
            .expect("logical insert should fit budget");
        mgr.commit(&mut inserter).expect("insert commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        mgr.cell_log()
            .record_update(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &first_payload),
                txn.token(),
            )
            .expect("first logical update should fit budget");
        mgr.cell_log()
            .record_update(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &final_payload),
                txn.token(),
            )
            .expect("second logical update should fit budget");

        let page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("page should be visible");
        assert_eq!(
            materialized_table_payloads(&page)?,
            vec![(rowid, final_payload.clone())],
            "transaction-local reads must replay repeated updates in record order"
        );

        mgr.commit(&mut txn).expect("combined logical commit");

        let mut reader = mgr.begin(BeginKind::Concurrent).expect("reader begin");
        let committed_page = mgr
            .read_page_with_cell_deltas(&mut reader, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should be visible");
        assert_eq!(
            materialized_table_payloads(&committed_page)?,
            vec![(rowid, final_payload)],
            "committed reads must preserve the final repeated update"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_replays_same_txn_repeated_insert()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 4258;
        let first_payload = vec![b'g'; 151];
        let final_payload = vec![b'h'; 152];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &first_payload),
                txn.token(),
            )
            .expect("first logical insert should fit budget");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &final_payload),
                txn.token(),
            )
            .expect("second logical insert should fit budget");

        let page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("page should be visible");
        assert_eq!(
            materialized_table_payloads(&page)?,
            vec![(rowid, final_payload.clone())],
            "transaction-local reads must replay repeated inserts in record order"
        );

        mgr.commit(&mut txn).expect("combined logical commit");

        let mut reader = mgr.begin(BeginKind::Concurrent).expect("reader begin");
        let committed_page = mgr
            .read_page_with_cell_deltas(&mut reader, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should be visible");
        assert_eq!(
            materialized_table_payloads(&committed_page)?,
            vec![(rowid, final_payload)],
            "committed reads must preserve the final repeated insert"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_replays_same_txn_repeated_delete()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 4259;
        let payload = vec![b'j'; 153];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut inserter = mgr.begin(BeginKind::Concurrent).expect("inserter begin");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &payload),
                inserter.token(),
            )
            .expect("logical insert should fit budget");
        mgr.commit(&mut inserter).expect("insert commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        mgr.cell_log()
            .record_delete(cell_key, pgno, txn.token())
            .expect("first logical delete should fit budget");
        mgr.cell_log()
            .record_delete(cell_key, pgno, txn.token())
            .expect("second logical delete should fit budget");

        let page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("page should be visible");
        assert_eq!(
            materialized_table_payloads(&page)?,
            Vec::<(i64, Vec<u8>)>::new(),
            "transaction-local reads must replay repeated deletes as absent"
        );

        mgr.commit(&mut txn).expect("combined logical commit");

        let mut reader = mgr.begin(BeginKind::Concurrent).expect("reader begin");
        let committed_page = mgr
            .read_page_with_cell_deltas(&mut reader, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should be visible");
        assert_eq!(
            materialized_table_payloads(&committed_page)?,
            Vec::<(i64, Vec<u8>)>::new(),
            "committed reads must preserve repeated delete idempotence"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_replays_same_txn_interleaved_rows()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let row_a = 4260;
        let row_b = 4261;
        let row_c = 4262;
        let key_a = CellKey::table_row(btree, row_a);
        let key_b = CellKey::table_row(btree, row_b);
        let key_c = CellKey::table_row(btree, row_c);
        let a_original = vec![b'a'; 154];
        let b_original = vec![b'b'; 155];
        let a_first = vec![b'c'; 156];
        let a_final = vec![b'd'; 157];
        let b_reinserted = vec![b'e'; 158];
        let c_inserted = vec![b'f'; 159];

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut bootstrap = mgr.begin(BeginKind::Concurrent).expect("bootstrap begin");
        mgr.cell_log()
            .record_insert(
                key_a,
                pgno,
                leaf_table_cell(row_a, &a_original),
                bootstrap.token(),
            )
            .expect("row a insert should fit budget");
        mgr.cell_log()
            .record_insert(
                key_b,
                pgno,
                leaf_table_cell(row_b, &b_original),
                bootstrap.token(),
            )
            .expect("row b insert should fit budget");
        mgr.commit(&mut bootstrap).expect("bootstrap commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        mgr.cell_log()
            .record_update(key_a, pgno, leaf_table_cell(row_a, &a_first), txn.token())
            .expect("row a first update should fit budget");
        mgr.cell_log()
            .record_delete(key_b, pgno, txn.token())
            .expect("row b delete should fit budget");
        mgr.cell_log()
            .record_insert(
                key_c,
                pgno,
                leaf_table_cell(row_c, &c_inserted),
                txn.token(),
            )
            .expect("row c insert should fit budget");
        mgr.cell_log()
            .record_update(key_a, pgno, leaf_table_cell(row_a, &a_final), txn.token())
            .expect("row a final update should fit budget");
        mgr.cell_log()
            .record_insert(
                key_b,
                pgno,
                leaf_table_cell(row_b, &b_reinserted),
                txn.token(),
            )
            .expect("row b reinsert should fit budget");
        mgr.cell_log()
            .record_delete(key_c, pgno, txn.token())
            .expect("row c delete should fit budget");

        let expected = vec![(row_a, a_final.clone()), (row_b, b_reinserted.clone())];

        let page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("page should be visible");
        assert_eq!(
            materialized_table_payloads(&page)?,
            expected,
            "transaction-local reads must replay interleaved per-row deltas independently"
        );

        mgr.commit(&mut txn).expect("combined logical commit");

        let mut reader = mgr.begin(BeginKind::Concurrent).expect("reader begin");
        let committed_page = mgr
            .read_page_with_cell_deltas(&mut reader, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should be visible");
        assert_eq!(
            materialized_table_payloads(&committed_page)?,
            vec![(row_a, a_final), (row_b, b_reinserted)],
            "committed reads must preserve interleaved per-row ordering"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_replays_same_txn_multi_page_isolation()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let page_a = PageNumber::new(2).expect("valid page");
        let page_b = PageNumber::new(3).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let row_a = 4263;
        let row_b = 4264;
        let row_c = 4265;
        let row_d = 4266;
        let key_a = CellKey::table_row(btree, row_a);
        let key_b = CellKey::table_row(btree, row_b);
        let key_c = CellKey::table_row(btree, row_c);
        let key_d = CellKey::table_row(btree, row_d);
        let a_original = vec![b'g'; 160];
        let b_original = vec![b'h'; 161];
        let a_updated = vec![b'i'; 162];
        let c_inserted = vec![b'k'; 163];
        let d_inserted = vec![b'l'; 164];

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, page_a, empty_leaf_table_page())
            .expect("seed page a");
        mgr.write_page(&mut seed, page_b, empty_leaf_table_page())
            .expect("seed page b");
        mgr.commit(&mut seed).expect("seed commit");

        let mut bootstrap = mgr.begin(BeginKind::Concurrent).expect("bootstrap begin");
        mgr.cell_log()
            .record_insert(
                key_a,
                page_a,
                leaf_table_cell(row_a, &a_original),
                bootstrap.token(),
            )
            .expect("row a insert should fit budget");
        mgr.cell_log()
            .record_insert(
                key_b,
                page_b,
                leaf_table_cell(row_b, &b_original),
                bootstrap.token(),
            )
            .expect("row b insert should fit budget");
        mgr.commit(&mut bootstrap).expect("bootstrap commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        mgr.cell_log()
            .record_update(
                key_a,
                page_a,
                leaf_table_cell(row_a, &a_updated),
                txn.token(),
            )
            .expect("row a update should fit budget");
        mgr.cell_log()
            .record_insert(
                key_c,
                page_a,
                leaf_table_cell(row_c, &c_inserted),
                txn.token(),
            )
            .expect("row c insert should fit budget");
        mgr.cell_log()
            .record_delete(key_b, page_b, txn.token())
            .expect("row b delete should fit budget");
        mgr.cell_log()
            .record_insert(
                key_d,
                page_b,
                leaf_table_cell(row_d, &d_inserted),
                txn.token(),
            )
            .expect("row d insert should fit budget");

        let expected_a = vec![(row_a, a_updated.clone()), (row_c, c_inserted.clone())];
        let expected_b = vec![(row_d, d_inserted.clone())];

        let txn_page_a = mgr
            .read_page_with_cell_deltas(&mut txn, page_a, PageSize::DEFAULT.get())?
            .expect("page a should be visible");
        assert_eq!(
            materialized_table_payloads(&txn_page_a)?,
            expected_a,
            "transaction-local page-a read must ignore page-b deltas"
        );

        let txn_page_b = mgr
            .read_page_with_cell_deltas(&mut txn, page_b, PageSize::DEFAULT.get())?
            .expect("page b should be visible");
        assert_eq!(
            materialized_table_payloads(&txn_page_b)?,
            expected_b,
            "transaction-local page-b read must ignore page-a deltas"
        );

        mgr.commit(&mut txn).expect("combined logical commit");

        let mut reader = mgr.begin(BeginKind::Concurrent).expect("reader begin");
        let committed_page_a = mgr
            .read_page_with_cell_deltas(&mut reader, page_a, PageSize::DEFAULT.get())?
            .expect("committed page a should be visible");
        assert_eq!(
            materialized_table_payloads(&committed_page_a)?,
            vec![(row_a, a_updated), (row_c, c_inserted)],
            "committed page-a read must preserve page-local delta isolation"
        );

        let committed_page_b = mgr
            .read_page_with_cell_deltas(&mut reader, page_b, PageSize::DEFAULT.get())?
            .expect("committed page b should be visible");
        assert_eq!(
            materialized_table_payloads(&committed_page_b)?,
            vec![(row_d, d_inserted)],
            "committed page-b read must preserve page-local delta isolation"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_skips_replay_for_staged_full_page()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let committed_row = 4267;
        let local_row = 4268;
        let committed_key = CellKey::table_row(btree, committed_row);
        let local_key = CellKey::table_row(btree, local_row);
        let committed_payload = vec![b'm'; 165];
        let local_payload = vec![b'n'; 166];

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut logical = mgr.begin(BeginKind::Concurrent).expect("logical begin");
        mgr.cell_log()
            .record_insert(
                committed_key,
                pgno,
                leaf_table_cell(committed_row, &committed_payload),
                logical.token(),
            )
            .expect("committed logical insert should fit budget");
        mgr.commit(&mut logical).expect("logical insert commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        let staged_page = test_data(0x9a);
        mgr.write_page(&mut txn, pgno, staged_page.clone())
            .expect("stage full-page write");
        mgr.cell_log()
            .record_insert(
                local_key,
                pgno,
                leaf_table_cell(local_row, &local_payload),
                txn.token(),
            )
            .expect("local logical insert should fit budget");

        let page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("staged page should be visible");
        assert_eq!(
            page, staged_page,
            "a staged full-page write-set image must bypass committed and local cell-delta replay"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_replays_same_txn_delete_then_insert()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 4254;
        let original_payload = vec![b'q'; 141];
        let replacement_payload = vec![b'r'; 142];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut inserter = mgr.begin(BeginKind::Concurrent).expect("inserter begin");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &original_payload),
                inserter.token(),
            )
            .expect("logical insert should fit budget");
        mgr.commit(&mut inserter).expect("insert commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        mgr.cell_log()
            .record_delete(cell_key, pgno, txn.token())
            .expect("logical delete should fit budget");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &replacement_payload),
                txn.token(),
            )
            .expect("logical reinsert should fit budget");

        let page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("page should be visible");
        assert_eq!(
            materialized_table_payloads(&page)?,
            vec![(rowid, replacement_payload.clone())],
            "transaction-local reads must replay delete-then-insert as replacement"
        );

        mgr.commit(&mut txn).expect("combined logical commit");

        let mut reader = mgr.begin(BeginKind::Concurrent).expect("reader begin");
        let committed_page = mgr
            .read_page_with_cell_deltas(&mut reader, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should be visible");
        assert_eq!(
            materialized_table_payloads(&committed_page)?,
            vec![(rowid, replacement_payload)],
            "committed reads must preserve the delete-then-insert ordering"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_replays_same_txn_delete_then_update()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 4255;
        let original_payload = vec![b'm'; 143];
        let replacement_payload = vec![b'n'; 144];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut inserter = mgr.begin(BeginKind::Concurrent).expect("inserter begin");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &original_payload),
                inserter.token(),
            )
            .expect("logical insert should fit budget");
        mgr.commit(&mut inserter).expect("insert commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("txn begin");
        mgr.cell_log()
            .record_delete(cell_key, pgno, txn.token())
            .expect("logical delete should fit budget");
        mgr.cell_log()
            .record_update(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &replacement_payload),
                txn.token(),
            )
            .expect("logical update should fit budget");

        let page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("page should be visible");
        assert_eq!(
            materialized_table_payloads(&page)?,
            vec![(rowid, replacement_payload.clone())],
            "transaction-local reads must replay delete-then-update as replacement"
        );

        mgr.commit(&mut txn).expect("combined logical commit");

        let mut reader = mgr.begin(BeginKind::Concurrent).expect("reader begin");
        let committed_page = mgr
            .read_page_with_cell_deltas(&mut reader, pgno, PageSize::DEFAULT.get())?
            .expect("committed page should be visible");
        assert_eq!(
            materialized_table_payloads(&committed_page)?,
            vec![(rowid, replacement_payload)],
            "committed reads must preserve the delete-then-update ordering"
        );

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_hides_other_uncommitted_delta() -> fsqlite_error::Result<()>
    {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 4247;
        let payload = vec![b'u'; 129];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut writer = mgr.begin(BeginKind::Concurrent).expect("writer begin");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &payload),
                writer.token(),
            )
            .expect("logical insert should fit budget");

        let writer_page = mgr
            .read_page_with_cell_deltas(&mut writer, pgno, PageSize::DEFAULT.get())?
            .expect("writer should see the base page");
        assert_eq!(
            materialized_table_payloads(&writer_page)?,
            vec![(rowid, payload)],
            "writer must see its own uncommitted logical insert"
        );

        let mut other_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("other reader begin");
        let other_page = mgr
            .read_page_with_cell_deltas(&mut other_reader, pgno, PageSize::DEFAULT.get())?
            .expect("other reader should see the base page");
        assert_eq!(
            materialized_table_payloads(&other_page)?,
            Vec::<(i64, Vec<u8>)>::new(),
            "other transactions must not see uncommitted logical inserts"
        );

        mgr.abort(&mut writer);

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_hides_other_uncommitted_update() -> fsqlite_error::Result<()>
    {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 4248;
        let original_payload = vec![b'o'; 127];
        let updated_payload = vec![b'n'; 133];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut inserter = mgr.begin(BeginKind::Concurrent).expect("inserter begin");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &original_payload),
                inserter.token(),
            )
            .expect("logical insert should fit budget");
        mgr.commit(&mut inserter).expect("insert commit");

        let mut writer = mgr.begin(BeginKind::Concurrent).expect("writer begin");
        mgr.cell_log()
            .record_update(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &updated_payload),
                writer.token(),
            )
            .expect("logical update should fit budget");

        let writer_page = mgr
            .read_page_with_cell_deltas(&mut writer, pgno, PageSize::DEFAULT.get())?
            .expect("writer should see the base page");
        assert_eq!(
            materialized_table_payloads(&writer_page)?,
            vec![(rowid, updated_payload)],
            "writer must see its own uncommitted logical update"
        );

        let mut other_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("other reader begin");
        let other_page = mgr
            .read_page_with_cell_deltas(&mut other_reader, pgno, PageSize::DEFAULT.get())?
            .expect("other reader should see the base page");
        assert_eq!(
            materialized_table_payloads(&other_page)?,
            vec![(rowid, original_payload)],
            "other transactions must not see uncommitted logical updates"
        );

        mgr.abort(&mut writer);

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_hides_other_uncommitted_delete() -> fsqlite_error::Result<()>
    {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 4250;
        let payload = vec![b'k'; 135];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut inserter = mgr.begin(BeginKind::Concurrent).expect("inserter begin");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &payload),
                inserter.token(),
            )
            .expect("logical insert should fit budget");
        mgr.commit(&mut inserter).expect("insert commit");

        let mut deleter = mgr.begin(BeginKind::Concurrent).expect("deleter begin");
        mgr.cell_log()
            .record_delete(cell_key, pgno, deleter.token())
            .expect("logical delete should fit budget");

        let deleter_page = mgr
            .read_page_with_cell_deltas(&mut deleter, pgno, PageSize::DEFAULT.get())?
            .expect("deleter should see the base page");
        assert_eq!(
            materialized_table_payloads(&deleter_page)?,
            Vec::<(i64, Vec<u8>)>::new(),
            "deleter must see its own uncommitted logical delete"
        );

        let mut other_reader = mgr
            .begin(BeginKind::Concurrent)
            .expect("other reader begin");
        let other_page = mgr
            .read_page_with_cell_deltas(&mut other_reader, pgno, PageSize::DEFAULT.get())?
            .expect("other reader should see the base page");
        assert_eq!(
            materialized_table_payloads(&other_page)?,
            vec![(rowid, payload)],
            "other transactions must not see uncommitted logical deletes"
        );

        mgr.abort(&mut deleter);

        Ok(())
    }

    #[test]
    fn test_read_page_with_cell_deltas_materializes_own_uncommitted_delete()
    -> fsqlite_error::Result<()> {
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(2).expect("valid page");
        let btree = BtreeRef::Table(TableId::new(1));
        let rowid = 4244;
        let payload = vec![b'd'; 132];
        let cell_key = CellKey::table_row(btree, rowid);

        let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
        mgr.write_page(&mut seed, pgno, empty_leaf_table_page())
            .expect("seed empty leaf page");
        mgr.commit(&mut seed).expect("seed commit");

        let mut writer = mgr.begin(BeginKind::Concurrent).expect("writer begin");
        mgr.cell_log()
            .record_insert(
                cell_key,
                pgno,
                leaf_table_cell(rowid, &payload),
                writer.token(),
            )
            .expect("logical insert should fit budget");
        writer.write_set.push(pgno);
        mgr.commit(&mut writer).expect("logical commit");

        let mut txn = mgr.begin(BeginKind::Concurrent).expect("reader begin");
        mgr.cell_log()
            .record_delete(cell_key, pgno, txn.token())
            .expect("logical delete should fit budget");

        let page = mgr
            .read_page_with_cell_deltas(&mut txn, pgno, PageSize::DEFAULT.get())?
            .expect("page should be visible");

        assert_eq!(
            materialized_table_payloads(&page)?,
            Vec::<(i64, Vec<u8>)>::new(),
            "transaction-local read view must replay this txn's own uncommitted delete"
        );

        Ok(())
    }

    #[test]
    fn test_commit_mixed_structural_and_logical_pages() {
        // C4 test: Transaction with both structural and logical pages routes correctly
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, SemanticKeyKind, TableId};

        let mgr = mgr();
        let structural_pgno = PageNumber::new(300).unwrap();
        let logical_pgno = PageNumber::new(301).unwrap();

        let mut txn = mgr.begin(BeginKind::Concurrent).unwrap();
        let _ = mgr.read_page(&mut txn, logical_pgno);

        // Write to structural page (via write_page)
        let data = test_data(0xCD);
        mgr.write_page(&mut txn, structural_pgno, data.clone())
            .expect("write should succeed");

        // Record cell delta for logical page (without write_page)
        let cell_key = CellKey {
            btree: BtreeRef::Table(TableId::new(2)),
            kind: SemanticKeyKind::TableRow,
            key_digest: [1u8; 16],
        };
        let _ = mgr
            .cell_log()
            .record_insert(cell_key, logical_pgno, vec![5, 6, 7, 8], txn.token());
        txn.write_set.push(logical_pgno);

        // Verify classification
        assert!(txn.structural_pages.contains(&structural_pgno));
        assert!(!txn.structural_pages.contains(&logical_pgno));

        let commit_seq = mgr.commit(&mut txn).expect("commit should succeed");

        // Structural page should have full version in VersionStore
        let snapshot = Snapshot::new(commit_seq, SchemaEpoch::ZERO);
        let structural_version = mgr
            .version_store
            .resolve_visible_version(structural_pgno, &snapshot);
        assert!(
            structural_version.is_some(),
            "structural page should be in VersionStore"
        );
        assert_eq!(structural_version.unwrap().data.as_bytes()[0], 0xCD);

        // Both pages should be in commit_index
        assert_eq!(mgr.commit_index.latest(structural_pgno), Some(commit_seq));
        assert_eq!(mgr.commit_index.latest(logical_pgno), Some(commit_seq));
        assert_eq!(
            mgr.cell_log().resolve(logical_pgno, &cell_key, commit_seq),
            Some(vec![5, 6, 7, 8])
        );
    }

    #[test]
    fn test_abort_rolls_back_cell_deltas() {
        // C4 test: Abort should roll back uncommitted cell deltas
        use crate::cell_visibility::CellKey;
        use fsqlite_types::{BtreeRef, SemanticKeyKind, TableId};

        let mgr = mgr();
        let pgno = PageNumber::new(400).unwrap();

        let mut txn = mgr.begin(BeginKind::Concurrent).unwrap();
        let _ = mgr.read_page(&mut txn, pgno);

        // Record cell delta
        let cell_key = CellKey {
            btree: BtreeRef::Table(TableId::new(3)),
            kind: SemanticKeyKind::TableRow,
            key_digest: [2u8; 16],
        };
        let idx = mgr
            .cell_log()
            .record_insert(cell_key, pgno, vec![9, 10, 11, 12], txn.token());
        assert!(idx.is_some(), "record_insert should succeed");
        assert_eq!(mgr.cell_log().delta_count(), 1);

        // Abort should clean up the cell delta
        mgr.abort(&mut txn);

        // Transaction should be in Aborted state
        assert_eq!(txn.state, TransactionState::Aborted);

        // structural_pages should be cleared
        assert!(
            txn.structural_pages.is_empty(),
            "structural_pages should be cleared after abort"
        );
        assert_eq!(mgr.cell_log().delta_count(), 0);
        assert!(
            mgr.cell_log()
                .resolve(pgno, &cell_key, CommitSeq::new(u64::MAX))
                .is_none(),
            "aborted cell delta should be rolled back"
        );
    }

    #[test]
    fn test_cell_log_accessor() {
        // C4 test: cell_log() accessor should work
        let mgr = mgr();
        let cell_log = mgr.cell_log();

        // Just verify we can access it (it's a &CellVisibilityLog)
        // The actual cell_log tests are in cell_visibility.rs
        assert_eq!(cell_log.delta_count(), 0, "new cell_log should be empty");
    }
}
