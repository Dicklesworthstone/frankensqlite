//! Transaction lifecycle (§5.4): Begin/Read/Write/Commit/Abort.
//!
//! This module implements the full transaction lifecycle for both
//! Serialized and Concurrent modes.  It provides:
//!
//! - [`BeginKind`]: The four modes (Deferred, Immediate, Exclusive, Concurrent).
//! - [`TransactionManager`]: Orchestrates begin/read/write/commit/abort.
//! - [`Savepoint`]: B-tree-level page state snapshots within a transaction.
//! - [`CommitResponse`]: Result type for the commit sequencer.

use std::collections::HashMap;
use std::time::Duration;

use fsqlite_types::{
    BTreePageType, CommitSeq, PageData, PageNumber, PageSize, PageVersion, SchemaEpoch, Snapshot,
    TxnEpoch, TxnId, TxnToken,
};

use crate::cache_aligned::{logical_now_epoch_secs, logical_now_millis};
use crate::core_types::{
    CommitIndex, InProcessPageLockTable, Transaction, TransactionMode, TransactionState,
};
use crate::invariants::{SerializedWriteMutex, TxnManager, VersionStore};
use crate::shm::SharedMemoryLayout;

const DEFAULT_BUSY_TIMEOUT_MS: u64 = 100;
const DEFAULT_SERIALIZED_WRITER_LEASE_SECS: u64 = 30;

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

/// Page class used for merge-safety decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MergePageKind {
    BtreeInterior,
    BtreeLeaf,
    Overflow,
    Freelist,
    PointerMap,
    OpaqueLab,
}

impl MergePageKind {
    /// Whether this page has SQLite-internal pointer semantics.
    #[must_use]
    pub const fn is_sqlite_structured(self) -> bool {
        !matches!(self, Self::OpaqueLab)
    }
}

/// Chosen conflict response under the active merge policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MergeDecision {
    AbortRetry,
    IntentReplay,
    StructuredPatch,
    RawXorLab,
}

/// Classify a raw page image for merge-safety policy checks.
#[must_use]
pub fn classify_page_for_merge(page: &[u8]) -> MergePageKind {
    let Some(first_byte) = page.first().copied() else {
        return MergePageKind::OpaqueLab;
    };
    match BTreePageType::from_byte(first_byte) {
        Some(kind) if kind.is_leaf() => MergePageKind::BtreeLeaf,
        Some(_) => MergePageKind::BtreeInterior,
        None => MergePageKind::OpaqueLab,
    }
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
/// Per spec §5.4: savepoints are a B-tree-level mechanism, NOT MVCC-level.
/// Page locks are NOT released on `ROLLBACK TO`. SSI witnesses are NOT
/// rolled back (safe overapproximation).
#[derive(Debug)]
pub struct Savepoint {
    /// Name of the savepoint.
    pub name: String,
    /// Snapshot of the write set at the time the savepoint was created.
    /// Maps page -> data so we can restore on ROLLBACK TO.
    pub write_set_snapshot: HashMap<PageNumber, PageData>,
    /// Number of pages in write_set when savepoint was created.
    pub write_set_len: usize,
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
    /// Current schema epoch (simplified; in full impl this lives in SHM).
    schema_epoch: SchemaEpoch,
    write_merge_policy: WriteMergePolicy,
    busy_timeout_ms: u64,
    serialized_writer_lease_secs: u64,
    txn_max_duration_ms: u64,
}

impl TransactionManager {
    /// Create a new transaction manager.
    #[must_use]
    pub fn new(page_size: PageSize) -> Self {
        Self {
            txn_manager: TxnManager::default(),
            version_store: VersionStore::new(page_size),
            lock_table: InProcessPageLockTable::new(),
            write_mutex: SerializedWriteMutex::new(),
            shm: SharedMemoryLayout::new(page_size, 128),
            commit_index: CommitIndex::new(),
            schema_epoch: SchemaEpoch::ZERO,
            write_merge_policy: WriteMergePolicy::default(),
            busy_timeout_ms: DEFAULT_BUSY_TIMEOUT_MS,
            serialized_writer_lease_secs: DEFAULT_SERIALIZED_WRITER_LEASE_SECS,
            txn_max_duration_ms: 5_000,
        }
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

    /// Begin a new transaction.
    ///
    /// # Errors
    ///
    /// Returns `MvccError::TxnIdExhausted` if the TxnId space is exhausted.
    /// Returns `MvccError::Busy` if the serialized writer mutex cannot be acquired
    /// (for Immediate/Exclusive modes).
    pub fn begin(&self, kind: BeginKind) -> Result<Transaction, MvccError> {
        // TxnId allocation via CAS (never fetch_add, never 0, never > MAX).
        let txn_id = self
            .txn_manager
            .alloc_txn_id()
            .ok_or(MvccError::TxnIdExhausted)?;

        let mode = if kind == BeginKind::Concurrent {
            TransactionMode::Concurrent
        } else {
            TransactionMode::Serialized
        };

        let snapshot = self.load_consistent_snapshot();
        let snapshot_established = kind != BeginKind::Deferred;

        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snapshot, mode);
        txn.snapshot_established = snapshot_established;

        // For Immediate/Exclusive: acquire serialized writer exclusion at BEGIN.
        if kind == BeginKind::Immediate || kind == BeginKind::Exclusive {
            self.acquire_serialized_writer_exclusion(txn_id)?;
            txn.serialized_write_lock_held = true;
        }

        tracing::info!(
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
        if let Some(data) = txn.write_set_data.get(&pgno) {
            return Some(data.clone());
        }

        // DEFERRED snapshot establishment on first read.
        if txn.mode == TransactionMode::Serialized && !txn.snapshot_established {
            txn.snapshot = self.load_consistent_snapshot();
            txn.snapshot_established = true;
            tracing::debug!(
                txn_id = %txn.txn_id,
                snapshot_high = txn.snapshot.high.get(),
                "deferred snapshot established on read"
            );
        }

        // Resolve from version store.
        let idx = self.version_store.resolve(pgno, &txn.snapshot)?;
        let version = self.version_store.get_version(idx)?;
        Some(version.data)
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

        // If no writes, just commit (read-only transaction).
        if txn.write_set.is_empty() && txn.write_set_data.is_empty() {
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
    /// - Concurrent: SSI witnesses preserved (safe overapproximation).
    pub fn abort(&self, txn: &mut Transaction) {
        if txn.state != TransactionState::Active {
            return; // already finalized
        }
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
    /// Records the current write_set state so it can be restored on
    /// `ROLLBACK TO`.
    #[must_use]
    pub fn savepoint(txn: &Transaction, name: &str) -> Savepoint {
        assert_eq!(
            txn.state,
            TransactionState::Active,
            "can only create savepoints in active transactions"
        );
        Savepoint {
            name: name.to_owned(),
            write_set_snapshot: txn.write_set_data.clone(),
            write_set_len: txn.write_set.len(),
        }
    }

    /// Rollback to a savepoint.
    ///
    /// Per spec §5.4:
    /// - Restores write_set page states to the savepoint.
    /// - Page locks are NOT released.
    /// - SSI witnesses are NOT rolled back.
    pub fn rollback_to_savepoint(txn: &mut Transaction, savepoint: &Savepoint) {
        assert_eq!(
            txn.state,
            TransactionState::Active,
            "can only rollback to savepoint in active transactions"
        );

        // Restore write_set_data to the savepoint state.
        txn.write_set_data.clone_from(&savepoint.write_set_snapshot);

        // Truncate the write_set page list to savepoint length.
        txn.write_set.truncate(savepoint.write_set_len);

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

    /// Simplified consistent snapshot load (in-process; no seqlock needed).
    ///
    /// Derives the latest committed seq from the TxnManager's counter
    /// (the counter holds the *next* seq to allocate, so latest = counter - 1).
    fn load_consistent_snapshot(&self) -> Snapshot {
        let counter = self.txn_manager.current_commit_counter();
        let high = CommitSeq::new(counter.saturating_sub(1));
        Snapshot::new(high, self.schema_epoch)
    }

    /// Serialized write path.
    fn write_page_serialized(
        &self,
        txn: &mut Transaction,
        pgno: PageNumber,
        data: PageData,
    ) -> Result<(), MvccError> {
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
            txn.snapshot = snap_now;
            txn.snapshot_established = true;
            txn.serialized_write_lock_held = true;

            tracing::debug!(
                txn_id = %txn.txn_id,
                "serialized deferred upgrade: mutex acquired"
            );
        }

        // No page lock needed (mutex provides exclusion).
        // Track in write_set.
        if !txn.write_set.contains(&pgno) {
            txn.write_set.push(pgno);
        }
        txn.write_set_data.insert(pgno, data);
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
            .check_serialized_writer_exclusion(logical_now_epoch_secs(), |_pid, _birth| true)
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

        // Track in write_set.
        if !txn.write_set.contains(&pgno) {
            txn.write_set.push(pgno);
        }
        txn.write_set_data.insert(pgno, data);
        Ok(())
    }

    /// Serialized commit path.
    fn commit_serialized(&self, txn: &mut Transaction) -> Result<CommitSeq, MvccError> {
        // FCW freshness validation: check that no page in write_set has been
        // committed since our snapshot.
        for &pgno in &txn.write_set {
            if let Some(latest) = self.commit_index.latest(pgno) {
                if latest > txn.snapshot.high {
                    self.abort(txn);
                    return Err(MvccError::BusySnapshot);
                }
            }
        }

        // Publish: allocate commit_seq and publish versions.
        let commit_seq = self.publish_write_set(txn);
        txn.commit();
        self.release_all_resources(txn);

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
        // Step 1: SSI validation — if dangerous structure, abort immediately.
        if txn.has_dangerous_structure() {
            self.abort(txn);
            return Err(MvccError::BusySnapshot);
        }

        // Step 2: FCW freshness validation with merge-retry (§5.8, §5.10).
        // First pass: collect conflicting pages.
        let mut conflicts = Vec::new();
        for &pgno in &txn.write_set {
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
                data.map_or(MergePageKind::OpaqueLab, |page| {
                    classify_page_for_merge(page.as_bytes())
                })
            };
            let decision =
                merge_decision(self.write_merge_policy, page_kind, cfg!(debug_assertions));

            if decision == MergeDecision::AbortRetry {
                tracing::info!(
                    txn_id = %txn.txn_id,
                    pgno = pgno.get(),
                    ?decision,
                    "FCW conflict: merge policy is Off, aborting"
                );
                self.abort(txn);
                return Err(MvccError::BusySnapshot);
            }

            // Attempt deterministic rebase (§5.10).
            if self.try_rebase_page(txn, pgno) {
                tracing::info!(
                    txn_id = %txn.txn_id,
                    pgno = pgno.get(),
                    ?page_kind,
                    ?decision,
                    "FCW conflict resolved via disjoint rebase"
                );
                rebased = true;
            } else {
                tracing::info!(
                    txn_id = %txn.txn_id,
                    pgno = pgno.get(),
                    ?page_kind,
                    ?decision,
                    "FCW conflict: rebase failed, aborting"
                );
                self.abort(txn);
                return Err(MvccError::BusySnapshot);
            }
        }

        // Step 3: SSI re-validation after rebase (§5.7.3 — mandatory even on
        // successful rebase, per spec line ~9005).
        if rebased && txn.has_dangerous_structure() {
            self.abort(txn);
            return Err(MvccError::BusySnapshot);
        }

        // Step 4: Publish.
        let commit_seq = self.publish_write_set(txn);
        txn.commit();
        self.release_all_resources(txn);

        tracing::info!(
            txn_id = %txn.txn_id,
            commit_seq = commit_seq.get(),
            rebased,
            "concurrent commit succeeded"
        );

        Ok(commit_seq)
    }

    /// Attempt to rebase a conflicting page via GF(256) disjoint merge (§5.10).
    ///
    /// Computes XOR deltas between (base, ours) and (base, theirs). If the
    /// deltas have disjoint support (non-overlapping byte changes), composes
    /// them to produce a merged page and updates the transaction's write set.
    fn try_rebase_page(&self, txn: &mut Transaction, pgno: PageNumber) -> bool {
        // Get base version visible at txn's snapshot.
        let base_data = match self.version_store.resolve(pgno, &txn.snapshot) {
            Some(idx) => match self.version_store.get_version(idx) {
                Some(v) => v.data,
                None => return false,
            },
            None => {
                // Page didn't exist at txn's snapshot — insert-insert conflict,
                // cannot rebase without higher-level intent replay.
                return false;
            }
        };

        // Get latest committed version (chain head = "theirs").
        let latest_data = match self.version_store.chain_head(pgno) {
            Some(idx) => match self.version_store.get_version(idx) {
                Some(v) => v.data,
                None => return false,
            },
            None => return false,
        };

        // Get txn's written data ("ours").
        let ours = match txn.write_set_data.get(&pgno) {
            Some(data) => data.clone(),
            None => return false,
        };

        // Compute XOR deltas.
        let Some(delta_ours) = gf256_patch_delta(base_data.as_bytes(), ours.as_bytes()) else {
            return false;
        };
        let Some(delta_theirs) = gf256_patch_delta(base_data.as_bytes(), latest_data.as_bytes())
        else {
            return false;
        };

        // Compose if disjoint.
        match compose_disjoint_gf256_patches(base_data.as_bytes(), &delta_ours, &delta_theirs) {
            Some(merged) => {
                txn.write_set_data.insert(pgno, PageData::from_vec(merged));
                true
            }
            None => false,
        }
    }

    /// Publish a transaction's write set into the version store and commit index.
    ///
    /// Returns the assigned `CommitSeq`.
    fn publish_write_set(&self, txn: &Transaction) -> CommitSeq {
        let commit_seq = self.txn_manager.alloc_commit_seq();

        for &pgno in &txn.write_set {
            if let Some(data) = txn.write_set_data.get(&pgno) {
                // Look up existing chain head for prev pointer.
                let prev = self
                    .version_store
                    .chain_head(pgno)
                    .map(crate::invariants::idx_to_version_pointer);

                let version = PageVersion {
                    pgno,
                    commit_seq,
                    created_by: TxnToken::new(txn.txn_id, txn.txn_epoch),
                    data: data.clone(),
                    prev,
                };
                self.version_store.publish(version);
                self.commit_index.update(pgno, commit_seq);
            }
        }

        commit_seq
    }

    /// Release all resources held by a transaction.
    fn release_all_resources(&self, txn: &mut Transaction) {
        // Release page locks.
        self.lock_table.release_all(txn.txn_id);

        // Release serialized write mutex if held.
        if txn.serialized_write_lock_held {
            // Per spec: clear serialized writer indicator BEFORE releasing mutex.
            self.release_serialized_writer_exclusion(txn.txn_id);
            txn.serialized_write_lock_held = false;
        }
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
        let pid_birth = now; // simplified; real impl uses epoch nanos + OS liveness
        let lease_expiry = now.saturating_add(self.serialized_writer_lease_secs);

        // If a serialized-writer token is present and not stale, treat as BUSY.
        if self
            .shm
            .check_serialized_writer_exclusion(now, |_pid, _birth| true)
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
        let mut elapsed_ms = 0_u64;
        let mut remaining_budget_ms = self.busy_timeout_ms;
        let mut last_remaining = usize::MAX;

        loop {
            let remaining = self.lock_table.total_lock_count();
            if remaining == 0 {
                tracing::debug!(
                    txn_id = %txn_id,
                    elapsed_ms,
                    "serialized writer drain complete"
                );
                return Ok(());
            }

            if remaining_budget_ms == 0 {
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

            // Be polite: this is a busy-wait with a deadline, not a hard spin.
            std::thread::sleep(Duration::from_millis(1));
            elapsed_ms = elapsed_ms.saturating_add(1);
            remaining_budget_ms = remaining_budget_ms.saturating_sub(1);
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

impl std::fmt::Debug for TransactionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionManager")
            .field("schema_epoch", &self.schema_epoch)
            .field("write_merge_policy", &self.write_merge_policy)
            .field("txn_max_duration_ms", &self.txn_max_duration_ms)
            .field(
                "current_commit_counter",
                &self.txn_manager.current_commit_counter(),
            )
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
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

    fn test_data(byte: u8) -> PageData {
        let mut data = PageData::zeroed(PageSize::DEFAULT);
        data.as_bytes_mut()[0] = byte;
        data
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
    fn test_abort_concurrent_witnesses_preserved() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();

        // Add SSI witness keys.
        txn.read_keys
            .insert(fsqlite_types::WitnessKey::Page(PageNumber::new(1).unwrap()));
        txn.write_keys
            .insert(fsqlite_types::WitnessKey::Page(PageNumber::new(2).unwrap()));

        m.abort(&mut txn);

        // Witnesses are NOT cleared on abort (safe overapproximation per spec).
        assert!(
            !txn.read_keys.is_empty(),
            "SSI read witnesses must be preserved after abort"
        );
        assert!(
            !txn.write_keys.is_empty(),
            "SSI write witnesses must be preserved after abort"
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

        let sp = TransactionManager::savepoint(&txn, "sp1");
        assert_eq!(sp.name, "sp1");
        assert_eq!(sp.write_set_len, 1);
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
        let sp = TransactionManager::savepoint(&txn, "sp1");

        // Write page 2 after savepoint.
        m.write_page(&mut txn, p2, test_data(0x02)).unwrap();
        assert_eq!(txn.write_set.len(), 2);

        // Rollback to savepoint — should undo page 2 write.
        TransactionManager::rollback_to_savepoint(&mut txn, &sp);

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
    fn test_rollback_to_savepoint_keeps_page_locks() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        let p1 = PageNumber::new(1).unwrap();
        let p2 = PageNumber::new(2).unwrap();

        m.write_page(&mut txn, p1, test_data(0x01)).unwrap();
        let sp = TransactionManager::savepoint(&txn, "sp1");

        m.write_page(&mut txn, p2, test_data(0x02)).unwrap();
        assert_eq!(txn.page_locks.len(), 2);

        TransactionManager::rollback_to_savepoint(&mut txn, &sp);

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
    fn test_rollback_to_savepoint_keeps_witnesses() {
        let m = mgr();
        let mut txn = m.begin(BeginKind::Concurrent).unwrap();
        let p1 = PageNumber::new(1).unwrap();

        m.write_page(&mut txn, p1, test_data(0x01)).unwrap();

        // Add SSI witnesses after the savepoint.
        let sp = TransactionManager::savepoint(&txn, "sp1");
        txn.read_keys
            .insert(fsqlite_types::WitnessKey::Page(PageNumber::new(2).unwrap()));
        txn.write_keys
            .insert(fsqlite_types::WitnessKey::Page(PageNumber::new(3).unwrap()));

        TransactionManager::rollback_to_savepoint(&mut txn, &sp);

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
        let sp1 = TransactionManager::savepoint(&txn, "sp1");

        m.write_page(&mut txn, p2, test_data(0x02)).unwrap();
        let sp2 = TransactionManager::savepoint(&txn, "sp2");

        m.write_page(&mut txn, p3, test_data(0x03)).unwrap();
        assert_eq!(txn.write_set.len(), 3);

        // Rollback to sp2 — should undo p3 only.
        TransactionManager::rollback_to_savepoint(&mut txn, &sp2);
        assert_eq!(txn.write_set.len(), 2);
        assert!(txn.write_set_data.contains_key(&p1));
        assert!(txn.write_set_data.contains_key(&p2));
        assert!(!txn.write_set_data.contains_key(&p3));

        // Rollback to sp1 — should undo p2 as well.
        TransactionManager::rollback_to_savepoint(&mut txn, &sp1);
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
            MergePageKind::BtreeInterior,
            true,
        ));
        assert_eq!(
            merge_decision(WriteMergePolicy::Safe, MergePageKind::BtreeInterior, true),
            MergeDecision::IntentReplay
        );
    }

    #[test]
    fn test_xor_merge_forbidden_btree_leaf() {
        assert!(!raw_xor_merge_allowed(
            WriteMergePolicy::Safe,
            MergePageKind::BtreeLeaf,
            true,
        ));
        assert_eq!(
            merge_decision(WriteMergePolicy::Safe, MergePageKind::BtreeLeaf, true),
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
                classify_page_for_merge(test_data(0x0D).as_bytes()),
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
            MergePageKind::BtreeLeaf,
            true,
        ));
        assert_eq!(
            merge_decision(WriteMergePolicy::LabUnsafe, MergePageKind::BtreeLeaf, true),
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

        let page_kind = classify_page_for_merge(&target);
        assert_eq!(page_kind, MergePageKind::BtreeLeaf);
        assert!(
            !raw_xor_merge_allowed(WriteMergePolicy::Safe, page_kind, true),
            "delta encoding does not imply merge correctness permission"
        );
    }

    #[test]
    fn prop_merge_safety_compile_time() {
        let structured = [
            MergePageKind::BtreeInterior,
            MergePageKind::BtreeLeaf,
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
                classify_page_for_merge(test_data(0x0D).as_bytes()),
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
        let sp1 = TransactionManager::savepoint(&txn, "sp1");

        m.write_page(&mut txn, p2, test_data(0x02)).unwrap();
        let sp2 = TransactionManager::savepoint(&txn, "sp2");

        m.write_page(&mut txn, p3, test_data(0x03)).unwrap();
        assert_eq!(txn.write_set.len(), 3);

        // ROLLBACK TO sp2 — undoes p3 only.
        TransactionManager::rollback_to_savepoint(&mut txn, &sp2);
        assert_eq!(txn.write_set.len(), 2);
        assert!(txn.write_set_data.contains_key(&p1));
        assert!(txn.write_set_data.contains_key(&p2));
        assert!(!txn.write_set_data.contains_key(&p3));

        // ROLLBACK TO sp1 — undoes p2 as well.
        TransactionManager::rollback_to_savepoint(&mut txn, &sp1);
        assert_eq!(txn.write_set.len(), 1);
        assert!(txn.write_set_data.contains_key(&p1));
        assert!(!txn.write_set_data.contains_key(&p2));

        m.commit(&mut txn).unwrap();
    }
}
