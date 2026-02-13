//! BEGIN CONCURRENT transaction protocol (§12.10).
//!
//! Implements MVCC concurrent-writer mode where multiple transactions can
//! write simultaneously to different pages.  Page-level conflict detection
//! uses first-committer-wins: if two CONCURRENT transactions modify the same
//! page, the second committer receives `SQLITE_BUSY_SNAPSHOT`.
//!
//! # Protocol
//!
//! 1. `BEGIN CONCURRENT` establishes a read snapshot without acquiring the
//!    global write mutex.
//! 2. Reads resolve through MVCC: `resolve(page, snapshot)` returns the
//!    newest committed version with `commit_seq <= snapshot.high`.
//! 3. Writes acquire per-page locks (not a global mutex).
//! 4. At commit time, the write set is validated against the commit index:
//!    any page modified by another transaction since the snapshot was taken
//!    triggers `BusySnapshot`.
//! 5. Savepoints within concurrent transactions work normally; `ROLLBACK TO`
//!    reverts write-set state but preserves page locks and the snapshot.

use std::cell::Cell;
use std::collections::{HashMap, HashSet};

use fsqlite_types::{
    CommitSeq, PageData, PageNumber, Snapshot, TxnEpoch, TxnId, TxnToken, WitnessKey,
};

use crate::core_types::{CommitIndex, InProcessPageLockTable, TransactionMode, TransactionState};
use crate::lifecycle::MvccError;
use crate::ssi_validation::{
    ActiveTxnView, CommittedReaderInfo, CommittedWriterInfo, SsiAbortReason,
    ssi_validate_and_publish,
};

/// Maximum number of concurrent writers that can be active simultaneously.
///
/// This is a soft limit enforced at `begin_concurrent` time to prevent
/// unbounded resource consumption.
pub const MAX_CONCURRENT_WRITERS: usize = 128;

/// Result of first-committer-wins validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FcwResult {
    /// No conflicts: the write set is clean relative to the snapshot.
    Clean,
    /// One or more pages were modified by a concurrent transaction after
    /// the snapshot was established.
    Conflict {
        /// Pages that conflict (modified by another committer since snapshot).
        conflicting_pages: Vec<PageNumber>,
        /// The authoritative commit sequence that caused the conflict.
        conflicting_commit_seq: CommitSeq,
    },
}

/// Lightweight handle representing one active concurrent session.
///
/// A `ConcurrentHandle` tracks the write set, page locks, snapshot, and SSI
/// state for a single `BEGIN CONCURRENT` transaction.
#[derive(Debug)]
pub struct ConcurrentHandle {
    /// Read snapshot established at `BEGIN CONCURRENT` time.
    snapshot: Snapshot,
    /// Pages written by this transaction, keyed by page number.
    write_set: HashMap<PageNumber, PageData>,
    /// Set of page-level locks held by this transaction.
    page_locks: HashSet<PageNumber>,
    /// Transaction state (Active / Committed / Aborted).
    state: TransactionState,
    /// Pages read by this transaction (for SSI rw-antidependency detection).
    read_set: HashSet<PageNumber>,
    /// Transaction token for SSI tracking.
    txn_token: TxnToken,
    /// Whether this transaction has an incoming rw-antidependency edge (SSI).
    has_in_rw: Cell<bool>,
    /// Whether this transaction has an outgoing rw-antidependency edge (SSI).
    has_out_rw: Cell<bool>,
    /// Whether this transaction was marked for abort by another committer.
    marked_for_abort: Cell<bool>,
}

impl ConcurrentHandle {
    /// Create a new concurrent handle with the given snapshot and token.
    #[must_use]
    pub fn new(snapshot: Snapshot, txn_token: TxnToken) -> Self {
        Self {
            snapshot,
            write_set: HashMap::new(),
            page_locks: HashSet::new(),
            state: TransactionState::Active,
            read_set: HashSet::new(),
            txn_token,
            has_in_rw: Cell::new(false),
            has_out_rw: Cell::new(false),
            marked_for_abort: Cell::new(false),
        }
    }

    /// Returns the read snapshot for this concurrent transaction.
    #[must_use]
    pub const fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// Returns the current transaction state.
    #[must_use]
    pub const fn state(&self) -> TransactionState {
        self.state
    }

    /// Returns the set of pages in the write set.
    #[must_use]
    pub fn write_set_pages(&self) -> Vec<PageNumber> {
        self.write_set.keys().copied().collect()
    }

    /// Returns the number of pages in the write set.
    #[must_use]
    pub fn write_set_len(&self) -> usize {
        self.write_set.len()
    }

    /// Returns the set of page locks held.
    #[must_use]
    pub fn held_locks(&self) -> &HashSet<PageNumber> {
        &self.page_locks
    }

    /// Check whether this transaction is still active.
    #[must_use]
    pub const fn is_active(&self) -> bool {
        matches!(self.state, TransactionState::Active)
    }

    /// Mark the transaction as committed.
    pub fn mark_committed(&mut self) {
        self.state = TransactionState::Committed;
    }

    /// Mark the transaction as aborted.
    pub fn mark_aborted(&mut self) {
        self.state = TransactionState::Aborted;
    }

    /// Record a page read (for SSI rw-antidependency detection).
    pub fn record_read(&mut self, page: PageNumber) {
        self.read_set.insert(page);
    }

    /// Returns the set of pages that were read.
    #[must_use]
    pub fn read_set(&self) -> &HashSet<PageNumber> {
        &self.read_set
    }

    /// Returns the number of pages in the read set.
    #[must_use]
    pub fn read_set_len(&self) -> usize {
        self.read_set.len()
    }

    /// Returns the transaction token.
    #[must_use]
    pub const fn txn_token(&self) -> TxnToken {
        self.txn_token
    }

    /// Returns witness keys for all read pages (for SSI validation).
    #[must_use]
    pub fn read_witness_keys(&self) -> Vec<WitnessKey> {
        self.read_set.iter().map(|&p| WitnessKey::Page(p)).collect()
    }

    /// Returns witness keys for all written pages (for SSI validation).
    #[must_use]
    pub fn write_witness_keys(&self) -> Vec<WitnessKey> {
        self.write_set
            .keys()
            .map(|&p| WitnessKey::Page(p))
            .collect()
    }

    /// Whether this handle has incoming rw-antidependency (SSI).
    #[must_use]
    pub fn has_in_rw(&self) -> bool {
        self.has_in_rw.get()
    }

    /// Whether this handle has outgoing rw-antidependency (SSI).
    #[must_use]
    pub fn has_out_rw(&self) -> bool {
        self.has_out_rw.get()
    }

    /// Whether this transaction was marked for abort by another committer.
    #[must_use]
    pub fn is_marked_for_abort(&self) -> bool {
        self.marked_for_abort.get()
    }
}

/// Implement ActiveTxnView for ConcurrentHandle to enable SSI validation.
impl ActiveTxnView for ConcurrentHandle {
    fn token(&self) -> TxnToken {
        self.txn_token
    }

    fn begin_seq(&self) -> CommitSeq {
        self.snapshot.high
    }

    fn is_active(&self) -> bool {
        matches!(self.state, TransactionState::Active)
    }

    fn read_keys(&self) -> &[WitnessKey] {
        // NOTE: We can't return a slice directly since read_witness_keys() allocates.
        // The SSI validation code will call read_set() directly instead.
        &[]
    }

    fn write_keys(&self) -> &[WitnessKey] {
        // NOTE: We can't return a slice directly since write_witness_keys() allocates.
        // The SSI validation code will call write_set_pages() directly instead.
        &[]
    }

    fn has_in_rw(&self) -> bool {
        self.has_in_rw.get()
    }

    fn has_out_rw(&self) -> bool {
        self.has_out_rw.get()
    }

    fn set_has_out_rw(&self, val: bool) {
        self.has_out_rw.set(val);
    }

    fn set_has_in_rw(&self, val: bool) {
        self.has_in_rw.set(val);
    }

    fn set_marked_for_abort(&self, val: bool) {
        self.marked_for_abort.set(val);
    }
}

/// Savepoint within a concurrent transaction.
///
/// Per spec §5.4: page locks are NOT released on `ROLLBACK TO`.
/// SSI witnesses are NOT rolled back (safe overapproximation).
#[derive(Debug)]
pub struct ConcurrentSavepoint {
    /// Savepoint name.
    pub name: String,
    /// Snapshot of the write set at savepoint creation time.
    write_set_snapshot: HashMap<PageNumber, PageData>,
    /// Number of pages in write_set at savepoint creation.
    write_set_len: usize,
}

impl ConcurrentSavepoint {
    /// Returns the number of pages captured in this savepoint.
    #[must_use]
    pub fn captured_len(&self) -> usize {
        self.write_set_len
    }
}

/// Registry tracking all active concurrent writers for a database.
///
/// Enforces the soft limit on concurrent writers and provides the shared
/// state needed for first-committer-wins and SSI validation.
#[derive(Debug)]
pub struct ConcurrentRegistry {
    /// Active concurrent handles, keyed by an opaque session id.
    active: HashMap<u64, ConcurrentHandle>,
    /// Committed-reader history (RCRI-like) for SSI edge discovery.
    committed_readers: Vec<CommittedReaderInfo>,
    /// Committed-writer history (commit-log-like) for SSI edge discovery.
    committed_writers: Vec<CommittedWriterInfo>,
    /// Next session id to assign.
    next_session_id: u64,
    /// Epoch counter for TxnToken generation (increments on each session).
    epoch_counter: u32,
}

impl ConcurrentRegistry {
    /// Create a new, empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            active: HashMap::new(),
            committed_readers: Vec::new(),
            committed_writers: Vec::new(),
            next_session_id: 1,
            epoch_counter: 0,
        }
    }

    /// Number of currently active concurrent writers.
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.active.len()
    }

    /// Begin a new concurrent transaction.
    ///
    /// Establishes a read snapshot and registers a new concurrent handle.
    /// Returns the session id and handle, or an error if the soft limit
    /// is reached.
    pub fn begin_concurrent(&mut self, snapshot: Snapshot) -> Result<u64, MvccError> {
        if self.active.len() >= MAX_CONCURRENT_WRITERS {
            return Err(MvccError::Busy);
        }
        let session_id = self.next_session_id;
        self.next_session_id = self.next_session_id.wrapping_add(1);
        self.epoch_counter = self.epoch_counter.wrapping_add(1);

        // Create TxnToken for SSI tracking.
        let txn_id = TxnId::new(session_id).ok_or(MvccError::InvalidState)?;
        let txn_token = TxnToken::new(txn_id, TxnEpoch::new(self.epoch_counter));

        let handle = ConcurrentHandle::new(snapshot, txn_token);
        self.active.insert(session_id, handle);
        Ok(session_id)
    }

    /// Returns an iterator over all active handles (for SSI validation).
    pub fn iter_active(&self) -> impl Iterator<Item = (u64, &ConcurrentHandle)> {
        self.active.iter().map(|(&id, h)| (id, h))
    }

    /// Look up a concurrent handle by session id.
    #[must_use]
    pub fn get(&self, session_id: u64) -> Option<&ConcurrentHandle> {
        self.active.get(&session_id)
    }

    /// Look up a mutable concurrent handle by session id.
    pub fn get_mut(&mut self, session_id: u64) -> Option<&mut ConcurrentHandle> {
        self.active.get_mut(&session_id)
    }

    /// Remove a session (after commit or abort).
    pub fn remove(&mut self, session_id: u64) -> Option<ConcurrentHandle> {
        self.active.remove(&session_id)
    }

    /// Prune committed SSI history that cannot overlap any active transaction.
    fn prune_committed_conflict_history(&mut self) {
        let Some(min_active_begin) = self.gc_horizon() else {
            self.committed_readers.clear();
            self.committed_writers.clear();
            return;
        };
        self.committed_readers
            .retain(|reader| reader.commit_seq > min_active_begin);
        self.committed_writers
            .retain(|writer| writer.commit_seq > min_active_begin);
    }

    /// Compute the GC horizon: the minimum `snapshot.high` across all active
    /// concurrent transactions.
    ///
    /// Versions with `commit_seq <= horizon` that are superseded by a newer
    /// version are safe to reclaim (no active snapshot can see them).
    ///
    /// Returns `None` if no active transactions exist (caller should use the
    /// current commit_seq as the horizon, meaning all old versions can be pruned
    /// except the latest).
    #[must_use]
    pub fn gc_horizon(&self) -> Option<CommitSeq> {
        self.active
            .values()
            .filter(|h| h.is_active())
            .map(|h| h.snapshot.high)
            .min()
    }
}

impl Default for ConcurrentRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Write a page within a concurrent transaction.
///
/// Acquires a page-level lock if not already held, then records the page
/// data in the write set.  Returns an error if the lock is held by another
/// concurrent transaction.
pub fn concurrent_write_page(
    handle: &mut ConcurrentHandle,
    lock_table: &InProcessPageLockTable,
    session_id: u64,
    page: PageNumber,
    data: PageData,
) -> Result<(), MvccError> {
    if !handle.is_active() {
        return Err(MvccError::InvalidState);
    }
    let txn_id = TxnId::new(session_id).ok_or(MvccError::InvalidState)?;
    // Acquire page lock if not already held.
    if handle.page_locks.insert(page) && lock_table.try_acquire(page, txn_id).is_err() {
        handle.page_locks.remove(&page);
        return Err(MvccError::Busy);
    }
    handle.write_set.insert(page, data);
    Ok(())
}

/// Read a page within a concurrent transaction.
///
/// Returns the page from the local write set if it was modified by this
/// transaction, otherwise returns `None` (caller should resolve via MVCC
/// version store using the handle's snapshot).
#[must_use]
pub fn concurrent_read_page(handle: &ConcurrentHandle, page: PageNumber) -> Option<&PageData> {
    handle.write_set.get(&page)
}

/// Validate the write set against the commit index using first-committer-wins.
///
/// For each page in the write set, checks whether any other transaction
/// committed a newer version since the snapshot was established.  If so,
/// the conflicting pages and the authoritative commit sequence are returned.
pub fn validate_first_committer_wins(
    handle: &ConcurrentHandle,
    commit_index: &CommitIndex,
) -> FcwResult {
    let snapshot_seq = handle.snapshot.high;
    let mut conflicting_pages = Vec::new();
    let mut max_conflicting_seq = CommitSeq::ZERO;

    for &page in handle.write_set.keys() {
        if let Some(committed_seq) = commit_index.latest(page) {
            if committed_seq > snapshot_seq {
                conflicting_pages.push(page);
                if committed_seq > max_conflicting_seq {
                    max_conflicting_seq = committed_seq;
                }
            }
        }
    }

    if conflicting_pages.is_empty() {
        tracing::debug!(
            write_set_size = handle.write_set.len(),
            snapshot_seq = snapshot_seq.get(),
            "fcw_validation: clean (no base drift)"
        );
        FcwResult::Clean
    } else {
        // Sort for deterministic output.
        conflicting_pages.sort();
        tracing::warn!(
            conflicting_page_count = conflicting_pages.len(),
            max_conflicting_seq = max_conflicting_seq.get(),
            snapshot_seq = snapshot_seq.get(),
            "fcw_validation: base drift detected"
        );
        FcwResult::Conflict {
            conflicting_pages,
            conflicting_commit_seq: max_conflicting_seq,
        }
    }
}

/// SSI validation result for concurrent commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SsiResult {
    /// SSI validation passed.
    Clean,
    /// SSI validation failed — dangerous structure detected.
    Abort { reason: SsiAbortReason },
}

/// Borrowed active-transaction view with materialized witness keys.
struct HandleView<'a> {
    handle: &'a ConcurrentHandle,
    read_keys: Vec<WitnessKey>,
    write_keys: Vec<WitnessKey>,
}

impl<'a> HandleView<'a> {
    fn new(handle: &'a ConcurrentHandle) -> Self {
        Self {
            handle,
            read_keys: handle.read_witness_keys(),
            write_keys: handle.write_witness_keys(),
        }
    }
}

impl ActiveTxnView for HandleView<'_> {
    fn token(&self) -> TxnToken {
        self.handle.txn_token
    }

    fn begin_seq(&self) -> CommitSeq {
        self.handle.snapshot.high
    }

    fn is_active(&self) -> bool {
        self.handle.is_active()
    }

    fn read_keys(&self) -> &[WitnessKey] {
        &self.read_keys
    }

    fn write_keys(&self) -> &[WitnessKey] {
        &self.write_keys
    }

    fn has_in_rw(&self) -> bool {
        self.handle.has_in_rw()
    }

    fn has_out_rw(&self) -> bool {
        self.handle.has_out_rw()
    }

    fn set_has_out_rw(&self, val: bool) {
        self.handle.has_out_rw.set(val);
    }

    fn set_has_in_rw(&self, val: bool) {
        self.handle.has_in_rw.set(val);
    }

    fn set_marked_for_abort(&self, val: bool) {
        self.handle.marked_for_abort.set(val);
    }
}

/// Commit a concurrent transaction.
///
/// Validates with first-committer-wins, then SSI (Serializable Snapshot
/// Isolation), then either commits (returning the assigned sequence) or
/// returns `BusySnapshot` on conflict.
pub fn concurrent_commit(
    handle: &mut ConcurrentHandle,
    commit_index: &CommitIndex,
    lock_table: &InProcessPageLockTable,
    session_id: u64,
    assign_commit_seq: CommitSeq,
) -> Result<CommitSeq, (MvccError, FcwResult)> {
    if !handle.is_active() {
        return Err((MvccError::InvalidState, FcwResult::Clean));
    }
    let txn_id = TxnId::new(session_id).ok_or((MvccError::InvalidState, FcwResult::Clean))?;

    // Step 1: First-committer-wins validation.
    let fcw_result = validate_first_committer_wins(handle, commit_index);
    match &fcw_result {
        FcwResult::Clean => {
            // FCW passed. Now run SSI validation.
            // NOTE: For now, we use a simplified SSI check that only looks at
            // the local handle's has_in_rw/has_out_rw flags and marked_for_abort.
            // Full SSI with cross-transaction edge discovery requires the
            // registry to be passed in (done in concurrent_commit_with_ssi).

            // Check if marked for abort by another committer.
            if handle.is_marked_for_abort() {
                tracing::warn!(
                    txn = %txn_id,
                    "concurrent_commit: SSI marked_for_abort"
                );
                lock_table.release_all(txn_id);
                handle.mark_aborted();
                return Err((MvccError::BusySnapshot, FcwResult::Clean));
            }

            // Check for dangerous structure (both in + out rw edges).
            if handle.has_in_rw() && handle.has_out_rw() {
                tracing::warn!(
                    txn = %txn_id,
                    "concurrent_commit: SSI pivot (in+out rw edges)"
                );
                lock_table.release_all(txn_id);
                handle.mark_aborted();
                return Err((MvccError::BusySnapshot, FcwResult::Clean));
            }

            // Commit: update commit index for all written pages.
            for &page in handle.write_set.keys() {
                commit_index.update(page, assign_commit_seq);
            }
            // Release all page locks.
            lock_table.release_all(txn_id);
            handle.mark_committed();
            Ok(assign_commit_seq)
        }
        FcwResult::Conflict { .. } => {
            // Release all page locks on conflict.
            lock_table.release_all(txn_id);
            handle.mark_aborted();
            Err((MvccError::BusySnapshot, fcw_result))
        }
    }
}

/// Commit a concurrent transaction with full SSI validation.
///
/// This version takes the registry mutably to perform cross-transaction SSI
/// edge discovery. It handles getting the handle internally.
#[allow(clippy::too_many_lines)]
pub fn concurrent_commit_with_ssi(
    registry: &mut ConcurrentRegistry,
    commit_index: &CommitIndex,
    lock_table: &InProcessPageLockTable,
    session_id: u64,
    assign_commit_seq: CommitSeq,
) -> Result<CommitSeq, (MvccError, FcwResult)> {
    let txn_id = TxnId::new(session_id).ok_or((MvccError::InvalidState, FcwResult::Clean))?;

    // First, verify the session exists and is active.
    {
        let handle = registry
            .get(session_id)
            .ok_or((MvccError::InvalidState, FcwResult::Clean))?;
        if !handle.is_active() {
            return Err((MvccError::InvalidState, FcwResult::Clean));
        }

        // Step 1: First-committer-wins validation.
        let fcw_result = validate_first_committer_wins(handle, commit_index);
        if !matches!(fcw_result, FcwResult::Clean) {
            lock_table.release_all(txn_id);
            let handle = registry.get_mut(session_id).unwrap();
            handle.mark_aborted();
            return Err((MvccError::BusySnapshot, fcw_result));
        }
    }

    // Step 2: Run full SSI validation using active + committed history.
    let validation = {
        let handle = registry
            .get(session_id)
            .ok_or((MvccError::InvalidState, FcwResult::Clean))?;
        let txn = handle.txn_token();
        let begin_seq = handle.snapshot().high;
        let read_keys = handle.read_witness_keys();
        let write_keys = handle.write_witness_keys();
        let marked_for_abort = handle.is_marked_for_abort();

        let views: Vec<HandleView<'_>> = registry
            .iter_active()
            .filter(|(_, other)| other.is_active())
            .map(|(_, other)| HandleView::new(other))
            .collect();
        let active_views: Vec<&dyn ActiveTxnView> = views
            .iter()
            .map(|view| view as &dyn ActiveTxnView)
            .collect();

        ssi_validate_and_publish(
            txn,
            begin_seq,
            assign_commit_seq,
            &read_keys,
            &write_keys,
            &active_views,
            &active_views,
            &registry.committed_readers,
            &registry.committed_writers,
            marked_for_abort,
        )
    };

    let validation = match validation {
        Ok(ok) => ok,
        Err(err) => {
            tracing::warn!(
                txn = %txn_id,
                reason = ?err.reason,
                "concurrent_commit_with_ssi: SSI abort"
            );
            lock_table.release_all(txn_id);
            let handle = registry
                .get_mut(session_id)
                .ok_or((MvccError::InvalidState, FcwResult::Clean))?;
            handle.mark_aborted();
            return Err((MvccError::BusySnapshot, FcwResult::Clean));
        }
    };

    // Step 3: SSI passed. Commit and persist conflict history for future overlap checks.
    let (txn_token, begin_seq, had_in_rw, read_pages, write_pages) = {
        let handle = registry
            .get_mut(session_id)
            .ok_or((MvccError::InvalidState, FcwResult::Clean))?;

        handle.has_in_rw.set(validation.ssi_state.has_in_rw);
        handle.has_out_rw.set(validation.ssi_state.has_out_rw);

        let mut read_pages: Vec<PageNumber> = handle.read_set().iter().copied().collect();
        read_pages.sort_unstable();
        let mut write_pages: Vec<PageNumber> = handle.write_set.keys().copied().collect();
        write_pages.sort_unstable();

        for &page in &write_pages {
            commit_index.update(page, assign_commit_seq);
        }
        lock_table.release_all(txn_id);
        handle.mark_committed();

        (
            handle.txn_token(),
            handle.snapshot().high,
            handle.has_in_rw(),
            read_pages,
            write_pages,
        )
    };

    if !read_pages.is_empty() {
        registry.committed_readers.push(CommittedReaderInfo {
            token: txn_token,
            begin_seq,
            commit_seq: assign_commit_seq,
            had_in_rw,
            pages: read_pages,
        });
    }
    if !write_pages.is_empty() {
        registry.committed_writers.push(CommittedWriterInfo {
            token: txn_token,
            commit_seq: assign_commit_seq,
            pages: write_pages,
        });
    }
    registry.prune_committed_conflict_history();

    Ok(assign_commit_seq)
}

/// Abort a concurrent transaction, releasing all page locks.
pub fn concurrent_abort(
    handle: &mut ConcurrentHandle,
    lock_table: &InProcessPageLockTable,
    session_id: u64,
) {
    if let Some(txn_id) = TxnId::new(session_id) {
        lock_table.release_all(txn_id);
    }
    handle.mark_aborted();
}

/// Create a savepoint within a concurrent transaction.
///
/// Captures the current write set state so it can be restored on
/// `ROLLBACK TO`.  Page locks are NOT captured (they persist across
/// rollback).
pub fn concurrent_savepoint(
    handle: &ConcurrentHandle,
    name: &str,
) -> Result<ConcurrentSavepoint, MvccError> {
    if !handle.is_active() {
        return Err(MvccError::InvalidState);
    }
    Ok(ConcurrentSavepoint {
        name: name.to_owned(),
        write_set_snapshot: handle.write_set.clone(),
        write_set_len: handle.write_set.len(),
    })
}

/// Rollback to a savepoint within a concurrent transaction.
///
/// Restores the write set to the state captured by the savepoint.
/// Page locks are NOT released (per spec §5.4).
/// The snapshot remains active for continued operations.
pub fn concurrent_rollback_to_savepoint(
    handle: &mut ConcurrentHandle,
    savepoint: &ConcurrentSavepoint,
) -> Result<(), MvccError> {
    if !handle.is_active() {
        return Err(MvccError::InvalidState);
    }
    handle.write_set.clone_from(&savepoint.write_set_snapshot);
    Ok(())
}

/// Check whether a transaction mode supports concurrent writers.
#[must_use]
pub const fn is_concurrent_mode(mode: TransactionMode) -> bool {
    matches!(mode, TransactionMode::Concurrent)
}

#[cfg(test)]
mod tests {
    use fsqlite_types::{CommitSeq, PageData, PageNumber, PageSize, SchemaEpoch, Snapshot};

    use crate::core_types::{CommitIndex, InProcessPageLockTable};
    use crate::lifecycle::MvccError;

    use super::{
        ConcurrentRegistry, FcwResult, MAX_CONCURRENT_WRITERS, concurrent_abort, concurrent_commit,
        concurrent_read_page, concurrent_rollback_to_savepoint, concurrent_savepoint,
        concurrent_write_page, validate_first_committer_wins,
    };

    fn test_snapshot(high: u64) -> Snapshot {
        Snapshot {
            high: CommitSeq::new(high),
            schema_epoch: SchemaEpoch::ZERO,
        }
    }

    fn test_page(n: u32) -> PageNumber {
        PageNumber::new(n).expect("page number must be nonzero")
    }

    fn test_data() -> PageData {
        PageData::zeroed(PageSize::DEFAULT)
    }

    // -----------------------------------------------------------------------
    // Test 1: Two connections both BEGIN CONCURRENT; insert into different
    //         pages; both commit successfully.
    // -----------------------------------------------------------------------
    #[test]
    fn test_begin_concurrent_multiple_writers() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        // Two concurrent sessions with the same snapshot.
        let s1 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session 1");
        let s2 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session 2");

        // Session 1 writes page 5.
        let h1 = registry.get_mut(s1).expect("handle 1");
        concurrent_write_page(h1, &lock_table, s1, test_page(5), test_data())
            .expect("write page 5");

        // Session 2 writes page 10 (different page => no conflict).
        let h2 = registry.get_mut(s2).expect("handle 2");
        concurrent_write_page(h2, &lock_table, s2, test_page(10), test_data())
            .expect("write page 10");

        // Both commit successfully.
        let h1 = registry.get_mut(s1).expect("handle 1");
        let seq1 = concurrent_commit(h1, &commit_index, &lock_table, s1, CommitSeq::new(11))
            .expect("commit 1");
        assert_eq!(seq1, CommitSeq::new(11));

        let h2 = registry.get_mut(s2).expect("handle 2");
        let seq2 = concurrent_commit(h2, &commit_index, &lock_table, s2, CommitSeq::new(12))
            .expect("commit 2");
        assert_eq!(seq2, CommitSeq::new(12));
    }

    // -----------------------------------------------------------------------
    // Test 2: Page conflict triggers SQLITE_BUSY_SNAPSHOT.
    // -----------------------------------------------------------------------
    #[test]
    fn test_begin_concurrent_page_conflict_busy_snapshot() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session 1");
        let s2 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session 2");

        // Both write to page 5, but lock contention prevents s2 from
        // acquiring the same lock.  In our model, each session uses its
        // session_id as the lock holder.
        let h1 = registry.get_mut(s1).expect("handle 1");
        concurrent_write_page(h1, &lock_table, s1, test_page(5), test_data())
            .expect("s1 write page 5");

        // s1 commits first (first-committer-wins).
        let h1 = registry.get_mut(s1).expect("handle 1");
        concurrent_commit(h1, &commit_index, &lock_table, s1, CommitSeq::new(11))
            .expect("s1 commits first");

        // Now s2 tries to write and commit the same page.  The lock was
        // released by s1's commit, so s2 can acquire it.
        let h2 = registry.get_mut(s2).expect("handle 2");
        concurrent_write_page(h2, &lock_table, s2, test_page(5), test_data())
            .expect("s2 write page 5");

        let h2 = registry.get_mut(s2).expect("handle 2");
        let result = concurrent_commit(h2, &commit_index, &lock_table, s2, CommitSeq::new(12));
        assert!(result.is_err());
        let (err, fcw) = result.unwrap_err();
        assert_eq!(err, MvccError::BusySnapshot);
        assert!(matches!(fcw, FcwResult::Conflict { .. }));
    }

    // -----------------------------------------------------------------------
    // Test 3: First-committer-wins with three concurrent transactions.
    // -----------------------------------------------------------------------
    #[test]
    fn test_begin_concurrent_first_committer_wins() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session 1");
        let s2 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session 2");
        let s3 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session 3");

        // s1 writes page 5, s3 writes page 10 (no overlap).
        let h1 = registry.get_mut(s1).expect("h1");
        concurrent_write_page(h1, &lock_table, s1, test_page(5), test_data()).unwrap();

        let h3 = registry.get_mut(s3).expect("h3");
        concurrent_write_page(h3, &lock_table, s3, test_page(10), test_data()).unwrap();

        // s1 commits first on page 5.
        let h1 = registry.get_mut(s1).expect("h1");
        concurrent_commit(h1, &commit_index, &lock_table, s1, CommitSeq::new(11))
            .expect("s1 commits");

        // s2 now tries page 5 (same as s1, but s1 already committed).
        let h2 = registry.get_mut(s2).expect("h2");
        concurrent_write_page(h2, &lock_table, s2, test_page(5), test_data()).unwrap();

        let h2 = registry.get_mut(s2).expect("h2");
        let result = concurrent_commit(h2, &commit_index, &lock_table, s2, CommitSeq::new(12));
        assert!(result.is_err());
        let (err, _) = result.unwrap_err();
        assert_eq!(err, MvccError::BusySnapshot);

        // s3 commits on page 10 (no conflict with s1's page 5).
        let h3 = registry.get_mut(s3).expect("h3");
        let seq3 = concurrent_commit(h3, &commit_index, &lock_table, s3, CommitSeq::new(13))
            .expect("s3 commits");
        assert_eq!(seq3, CommitSeq::new(13));
    }

    // -----------------------------------------------------------------------
    // Test 4: Savepoint within a concurrent transaction.
    // -----------------------------------------------------------------------
    #[test]
    fn test_savepoint_within_concurrent() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session");

        // Write page 1 (INSERT A).
        let handle = registry.get_mut(s1).expect("handle");
        concurrent_write_page(handle, &lock_table, s1, test_page(1), test_data()).unwrap();

        // Create savepoint.
        let handle = registry.get(s1).expect("handle");
        let sp = concurrent_savepoint(handle, "sp1").unwrap();
        assert_eq!(sp.captured_len(), 1);

        // Write page 2 (INSERT B).
        let handle = registry.get_mut(s1).expect("handle");
        concurrent_write_page(handle, &lock_table, s1, test_page(2), test_data()).unwrap();
        assert_eq!(handle.write_set_len(), 2);

        // Rollback to savepoint: page 2 should be removed from write set,
        // but its lock should still be held.
        let handle = registry.get_mut(s1).expect("handle");
        concurrent_rollback_to_savepoint(handle, &sp).unwrap();
        assert_eq!(handle.write_set_len(), 1);
        assert!(handle.held_locks().contains(&test_page(2))); // Lock preserved.

        // Write page 3 (INSERT C).
        let handle = registry.get_mut(s1).expect("handle");
        concurrent_write_page(handle, &lock_table, s1, test_page(3), test_data()).unwrap();

        // Commit: pages 1 and 3 are in the write set (not page 2).
        let handle = registry.get_mut(s1).expect("handle");
        let mut pages = handle.write_set_pages();
        pages.sort();
        assert_eq!(pages, vec![test_page(1), test_page(3)]);

        let handle = registry.get_mut(s1).expect("handle");
        concurrent_commit(handle, &commit_index, &lock_table, s1, CommitSeq::new(11))
            .expect("commit succeeds");
    }

    // -----------------------------------------------------------------------
    // Test 5: Read from local write set vs MVCC fallback.
    // -----------------------------------------------------------------------
    #[test]
    fn test_concurrent_read_local_vs_mvcc() {
        let lock_table = InProcessPageLockTable::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session");

        // Before writing, local read returns None (would fall through to MVCC).
        let handle = registry.get(s1).expect("handle");
        assert!(concurrent_read_page(handle, test_page(5)).is_none());

        // After writing, local read returns the written data.
        let handle = registry.get_mut(s1).expect("handle");
        concurrent_write_page(handle, &lock_table, s1, test_page(5), test_data()).unwrap();

        let handle = registry.get(s1).expect("handle");
        assert!(concurrent_read_page(handle, test_page(5)).is_some());
        assert!(concurrent_read_page(handle, test_page(6)).is_none());
    }

    // -----------------------------------------------------------------------
    // Test 6: Abort releases all page locks.
    // -----------------------------------------------------------------------
    #[test]
    fn test_concurrent_abort_releases_locks() {
        let lock_table = InProcessPageLockTable::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session");

        let handle = registry.get_mut(s1).expect("handle");
        concurrent_write_page(handle, &lock_table, s1, test_page(5), test_data()).unwrap();
        concurrent_write_page(handle, &lock_table, s1, test_page(6), test_data()).unwrap();
        assert_eq!(handle.held_locks().len(), 2);

        // Abort: locks released.
        let handle = registry.get_mut(s1).expect("handle");
        concurrent_abort(handle, &lock_table, s1);
        assert!(!handle.is_active());

        // Another session can now acquire the same locks.
        let s2 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session 2");
        let handle2 = registry.get_mut(s2).expect("handle 2");
        concurrent_write_page(handle2, &lock_table, s2, test_page(5), test_data())
            .expect("lock should be available after abort");
    }

    // -----------------------------------------------------------------------
    // Test 7: Registry enforces max concurrent writers.
    // -----------------------------------------------------------------------
    #[test]
    fn test_registry_max_concurrent_writers() {
        let mut registry = ConcurrentRegistry::new();
        for _ in 0..MAX_CONCURRENT_WRITERS {
            registry
                .begin_concurrent(test_snapshot(1))
                .expect("should succeed");
        }
        let result = registry.begin_concurrent(test_snapshot(1));
        assert_eq!(result.unwrap_err(), MvccError::Busy);
    }

    // -----------------------------------------------------------------------
    // Test 8: FCW validation with clean write set.
    // -----------------------------------------------------------------------
    #[test]
    fn test_fcw_validation_clean() {
        let commit_index = CommitIndex::new();
        let lock_table = InProcessPageLockTable::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session");
        let handle = registry.get_mut(s1).expect("handle");
        concurrent_write_page(handle, &lock_table, s1, test_page(5), test_data()).unwrap();

        let handle = registry.get(s1).expect("handle");
        assert_eq!(
            validate_first_committer_wins(handle, &commit_index),
            FcwResult::Clean
        );
    }

    // -----------------------------------------------------------------------
    // Test 9: FCW validation detects conflicts.
    // -----------------------------------------------------------------------
    #[test]
    fn test_fcw_validation_conflict() {
        let commit_index = CommitIndex::new();
        let lock_table = InProcessPageLockTable::new();
        let mut registry = ConcurrentRegistry::new();

        // Pre-populate commit index: page 5 was committed at seq 15.
        commit_index.update(test_page(5), CommitSeq::new(15));

        // Session with snapshot at seq 10 writes page 5.
        let s1 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session");
        let handle = registry.get_mut(s1).expect("handle");
        concurrent_write_page(handle, &lock_table, s1, test_page(5), test_data()).unwrap();

        let handle = registry.get(s1).expect("handle");
        let result = validate_first_committer_wins(handle, &commit_index);
        match result {
            FcwResult::Conflict {
                conflicting_pages,
                conflicting_commit_seq,
            } => {
                assert_eq!(conflicting_pages, vec![test_page(5)]);
                assert_eq!(conflicting_commit_seq, CommitSeq::new(15));
            }
            FcwResult::Clean => panic!("expected conflict"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 10: BUSY_SNAPSHOT is distinguishable from BUSY.
    // -----------------------------------------------------------------------
    #[test]
    fn test_busy_snapshot_vs_busy() {
        // BusySnapshot (stale snapshot) vs Busy (lock contention) are different
        // error codes for application retry logic.
        assert_ne!(MvccError::BusySnapshot, MvccError::Busy);

        // Display representations are distinct.
        assert_eq!(
            format!("{}", MvccError::BusySnapshot),
            "SQLITE_BUSY_SNAPSHOT"
        );
        assert_eq!(format!("{}", MvccError::Busy), "SQLITE_BUSY");
    }

    // -----------------------------------------------------------------------
    // Test 11: Concurrent session lifecycle.
    // -----------------------------------------------------------------------
    #[test]
    fn test_concurrent_session_lifecycle() {
        let mut registry = ConcurrentRegistry::new();
        assert_eq!(registry.active_count(), 0);

        let s1 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session");
        assert_eq!(registry.active_count(), 1);

        let handle = registry.get(s1).expect("handle");
        assert!(handle.is_active());

        let removed = registry.remove(s1);
        assert!(removed.is_some());
        assert_eq!(registry.active_count(), 0);
    }

    // -----------------------------------------------------------------------
    // Test 12: Operations on non-active handle return InvalidState.
    // -----------------------------------------------------------------------
    #[test]
    fn test_operations_on_inactive_handle() {
        let lock_table = InProcessPageLockTable::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session");

        // Abort the handle.
        let handle = registry.get_mut(s1).expect("handle");
        concurrent_abort(handle, &lock_table, s1);

        // Write should fail on aborted handle.
        let handle = registry.get_mut(s1).expect("handle");
        let result = concurrent_write_page(handle, &lock_table, s1, test_page(1), test_data());
        assert_eq!(result.unwrap_err(), MvccError::InvalidState);

        // Savepoint should fail on aborted handle.
        let handle = registry.get(s1).expect("handle");
        let result = concurrent_savepoint(handle, "sp1");
        assert_eq!(result.unwrap_err(), MvccError::InvalidState);
    }

    // -----------------------------------------------------------------------
    // SSI Tests (bd-1xo1)
    // -----------------------------------------------------------------------

    // Test 13: SSI - read tracking records pages.
    #[test]
    fn test_ssi_read_tracking() {
        let mut registry = ConcurrentRegistry::new();
        let s1 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session");

        let handle = registry.get_mut(s1).expect("handle");
        assert_eq!(handle.read_set_len(), 0);

        handle.record_read(test_page(5));
        handle.record_read(test_page(10));
        handle.record_read(test_page(5)); // Duplicate, should be deduplicated.

        assert_eq!(handle.read_set_len(), 2);
        assert!(handle.read_set().contains(&test_page(5)));
        assert!(handle.read_set().contains(&test_page(10)));
    }

    // Test 14: SSI - no conflict when reads/writes don't overlap.
    #[test]
    fn test_ssi_no_conflict_disjoint() {
        use super::concurrent_commit_with_ssi;

        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        // T1: reads page 5, writes page 10.
        // T2: reads page 20, writes page 30.
        // No overlap → both commit.
        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        // T1 reads and writes.
        {
            let h1 = registry.get_mut(s1).unwrap();
            h1.record_read(test_page(5));
            concurrent_write_page(h1, &lock_table, s1, test_page(10), test_data()).unwrap();
        }

        // T2 reads and writes.
        {
            let h2 = registry.get_mut(s2).unwrap();
            h2.record_read(test_page(20));
            concurrent_write_page(h2, &lock_table, s2, test_page(30), test_data()).unwrap();
        }

        // Both should commit successfully.
        let seq1 = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s1,
            CommitSeq::new(11),
        )
        .expect("T1 commits");
        assert_eq!(seq1, CommitSeq::new(11));

        let seq2 = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s2,
            CommitSeq::new(12),
        )
        .expect("T2 commits");
        assert_eq!(seq2, CommitSeq::new(12));
    }

    // Test 14b: committed SSI history is pruned on transaction completion.
    #[test]
    fn test_ssi_committed_history_pruned_on_completion() {
        use super::concurrent_commit_with_ssi;

        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        {
            let h1 = registry.get_mut(s1).unwrap();
            h1.record_read(test_page(5));
            concurrent_write_page(h1, &lock_table, s1, test_page(10), test_data()).unwrap();
        }
        {
            let h2 = registry.get_mut(s2).unwrap();
            h2.record_read(test_page(10));
            concurrent_write_page(h2, &lock_table, s2, test_page(20), test_data()).unwrap();
        }

        concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s1,
            CommitSeq::new(11),
        )
        .expect("first txn commits while second is still active");

        assert_eq!(
            registry.committed_readers.len(),
            1,
            "reader history retained while overlapping txn is active"
        );
        assert_eq!(
            registry.committed_writers.len(),
            1,
            "writer history retained while overlapping txn is active"
        );

        concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s2,
            CommitSeq::new(12),
        )
        .expect("second txn commits");

        assert!(
            registry.committed_readers.is_empty(),
            "reader history pruned once no active transactions remain"
        );
        assert!(
            registry.committed_writers.is_empty(),
            "writer history pruned once no active transactions remain"
        );
    }

    // Test 15: SSI - pivot detection aborts transaction with both in/out edges.
    #[test]
    fn test_ssi_pivot_abort() {
        use super::concurrent_commit_with_ssi;

        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        // Classic write skew setup:
        // T1 reads A, writes B.
        // T2 reads B, writes A.
        //
        // When T1 tries to commit:
        // - Incoming: T2 read B, T1 writes B → incoming edge from T2
        // - Outgoing: T1 read A, T2 writes A → outgoing edge to T2
        // T1 has BOTH → T1 is pivot → T1 must abort.

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        // T1: reads page 5 (A), writes page 10 (B).
        {
            let h1 = registry.get_mut(s1).unwrap();
            h1.record_read(test_page(5)); // A
            concurrent_write_page(h1, &lock_table, s1, test_page(10), test_data()).unwrap();
            // B
        }

        // T2: reads page 10 (B), writes page 5 (A).
        {
            let h2 = registry.get_mut(s2).unwrap();
            h2.record_read(test_page(10)); // B
            concurrent_write_page(h2, &lock_table, s2, test_page(5), test_data()).unwrap();
            // A
        }

        // T1 tries to commit first: it has both incoming and outgoing edges.
        // Incoming: T2 read B (page 10), T1 writes B → edge from T2 to T1
        // Outgoing: T1 read A (page 5), T2 writes A → edge from T1 to T2
        // T1 is pivot and must abort.
        let result1 = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s1,
            CommitSeq::new(11),
        );
        assert!(
            result1.is_err(),
            "T1 should abort as pivot (both in and out edges)"
        );
        let (err, _) = result1.unwrap_err();
        assert_eq!(err, MvccError::BusySnapshot);

        // After T1 aborts, T2 can now commit (T1 is no longer active, so no edges).
        let result2 = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s2,
            CommitSeq::new(11),
        );
        assert!(result2.is_ok(), "T2 should commit after T1 aborted");
    }

    // Test 16: SSI - transaction marked for abort fails.
    #[test]
    fn test_ssi_marked_for_abort() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        let h1 = registry.get_mut(s1).unwrap();
        concurrent_write_page(h1, &lock_table, s1, test_page(5), test_data()).unwrap();

        // Manually mark for abort.
        h1.marked_for_abort.set(true);

        // Commit should fail.
        let result = concurrent_commit(h1, &commit_index, &lock_table, s1, CommitSeq::new(11));
        assert!(result.is_err());
        let (err, _) = result.unwrap_err();
        assert_eq!(err, MvccError::BusySnapshot);
    }

    // Test 17: SSI - only incoming edge allows commit.
    #[test]
    fn test_ssi_only_incoming_edge_commits() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        let h1 = registry.get_mut(s1).unwrap();
        concurrent_write_page(h1, &lock_table, s1, test_page(5), test_data()).unwrap();

        // Set only incoming edge (no outgoing).
        h1.has_in_rw.set(true);
        h1.has_out_rw.set(false);

        // Commit should succeed (not a pivot).
        let result = concurrent_commit(h1, &commit_index, &lock_table, s1, CommitSeq::new(11));
        assert!(result.is_ok(), "only incoming edge should allow commit");
    }

    // Test 18: SSI - only outgoing edge allows commit.
    #[test]
    fn test_ssi_only_outgoing_edge_commits() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        let h1 = registry.get_mut(s1).unwrap();
        concurrent_write_page(h1, &lock_table, s1, test_page(5), test_data()).unwrap();

        // Set only outgoing edge (no incoming).
        h1.has_in_rw.set(false);
        h1.has_out_rw.set(true);

        // Commit should succeed (not a pivot).
        let result = concurrent_commit(h1, &commit_index, &lock_table, s1, CommitSeq::new(11));
        assert!(result.is_ok(), "only outgoing edge should allow commit");
    }

    // Test 19: SSI - both edges trigger abort.
    #[test]
    fn test_ssi_both_edges_aborts() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        let h1 = registry.get_mut(s1).unwrap();
        concurrent_write_page(h1, &lock_table, s1, test_page(5), test_data()).unwrap();

        // Set both edges → dangerous structure.
        h1.has_in_rw.set(true);
        h1.has_out_rw.set(true);

        // Commit should fail (pivot).
        let result = concurrent_commit(h1, &commit_index, &lock_table, s1, CommitSeq::new(11));
        assert!(result.is_err());
        let (err, _) = result.unwrap_err();
        assert_eq!(err, MvccError::BusySnapshot);
    }

    // Test 20: SSI - witness keys generated correctly.
    #[test]
    fn test_ssi_witness_keys() {
        let mut registry = ConcurrentRegistry::new();
        let lock_table = InProcessPageLockTable::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        let h1 = registry.get_mut(s1).unwrap();
        h1.record_read(test_page(5));
        h1.record_read(test_page(10));
        concurrent_write_page(h1, &lock_table, s1, test_page(15), test_data()).unwrap();
        concurrent_write_page(h1, &lock_table, s1, test_page(20), test_data()).unwrap();

        let read_keys = h1.read_witness_keys();
        let write_keys = h1.write_witness_keys();

        assert_eq!(read_keys.len(), 2);
        assert_eq!(write_keys.len(), 2);
    }

    // -----------------------------------------------------------------------
    // bd-mblr.6.7: Critical-path no-mock unit assertions
    //
    // The tests below exercise critical SSI invariants using ONLY real
    // components (ConcurrentHandle, ConcurrentRegistry, InProcessPageLockTable,
    // CommitIndex) — no MockActiveTxn, no manual flag setting.
    // -----------------------------------------------------------------------

    // Test 21 (bd-mblr.6.7): Three-transaction SSI marked-for-abort
    // propagation via real edge detection through concurrent_commit_with_ssi.
    //
    // Scenario:
    //   T1: reads D, writes C      (T1 will commit second)
    //   T2: reads C, writes D      (T2 will get marked_for_abort)
    //   T3: reads A, writes B      (T3 commits first, no overlap)
    //
    // When T3 commits first (disjoint pages → clean):
    //   - No edges, T3 commits cleanly.
    //
    // When T1 commits second:
    //   - Scans T2: T2 reads C, T1 writes C → incoming edge for T1.
    //     T2.has_out_rw = true.
    //   - Scans T2: T2 writes D, T1 reads D → outgoing edge for T1.
    //     T2.has_in_rw = true.
    //   - T1 has both in+out → T1 is pivot → T1 aborts.
    //   - T2 has both flags set, but was NOT marked_for_abort because
    //     has_in_rw was set in the outgoing-check AFTER the incoming-check
    //     tested it (false at that point).
    //
    // After T1 aborts, T2 can commit because the pivot (T1) is gone.
    //
    // This test exercises the full real-component SSI path without any
    // mock objects or manual flag manipulation.
    #[test]
    fn test_ssi_three_txn_pivot_abort_real_components() {
        use super::concurrent_commit_with_ssi;

        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        // T1: reads D (page 40), writes C (page 30).
        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        // T2: reads C (page 30), writes D (page 40).
        let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        // T3: reads A (page 5), writes B (page 10) — disjoint.
        let s3 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        // T1 operations.
        {
            let h1 = registry.get_mut(s1).unwrap();
            h1.record_read(test_page(40)); // reads D
            concurrent_write_page(h1, &lock_table, s1, test_page(30), test_data()).unwrap();
            // writes C
        }
        // T2 operations.
        {
            let h2 = registry.get_mut(s2).unwrap();
            h2.record_read(test_page(30)); // reads C
            concurrent_write_page(h2, &lock_table, s2, test_page(40), test_data()).unwrap();
            // writes D
        }
        // T3 operations (disjoint).
        {
            let h3 = registry.get_mut(s3).unwrap();
            h3.record_read(test_page(5)); // reads A
            concurrent_write_page(h3, &lock_table, s3, test_page(10), test_data()).unwrap();
            // writes B
        }

        // T3 commits first (disjoint, no edges).
        let result3 = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s3,
            CommitSeq::new(11),
        );
        assert!(result3.is_ok(), "T3 disjoint commit must succeed");

        // T1 commits second: classic write-skew with T2, T1 is pivot.
        let result1 = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s1,
            CommitSeq::new(12),
        );
        assert!(
            result1.is_err(),
            "T1 must abort as pivot (both in+out edges with T2)"
        );
        let (err, _) = result1.unwrap_err();
        assert_eq!(err, MvccError::BusySnapshot);

        // After T1 aborted, T2 can now commit (only remaining active handle).
        let result2 = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s2,
            CommitSeq::new(12),
        );
        assert!(result2.is_ok(), "T2 must commit after pivot T1 aborted");
    }

    // Test 22 (bd-mblr.6.7): SSI marked-for-abort propagation through
    // real edge detection — three transactions where sequential commits
    // progressively set SSI flags on T1 until T1 is marked_for_abort.
    //
    // The marked_for_abort path fires when the INCOMING edge check finds
    // that the other handle already has has_in_rw set. The incoming check
    // runs before the outgoing check, so has_in_rw must be set by a PRIOR
    // commit's scan.
    //
    // T1: reads pages {10, 20}, writes page 30
    // T2: reads page 50, writes page 10
    // T3: reads page 30, writes page 40
    //
    // Step 1: T3 commits. Scans T1:
    //   Outgoing check: T1 writes 30, T3 reads 30. Match!
    //   T1.has_in_rw = true. T3 commits (only outgoing, safe).
    //
    // Step 2: T2 commits. Scans T1:
    //   Incoming check: T2 writes 10, T1 reads {10,20}. Match on 10!
    //   T1.has_out_rw = true. T1.has_in_rw already true => T1 marked_for_abort!
    //   T2 has only incoming edge => T2 commits.
    //
    // Step 3: T1 tries to commit => fails (marked_for_abort).
    #[test]
    fn test_ssi_marked_for_abort_via_real_edge_detection() {
        use super::concurrent_commit_with_ssi;

        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        // T1: reads pages 10+20, writes page 30.
        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        // T2: reads page 50, writes page 10.
        let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        // T3: reads page 30, writes page 40.
        let s3 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        // T1 operations.
        {
            let h1 = registry.get_mut(s1).unwrap();
            h1.record_read(test_page(10));
            h1.record_read(test_page(20));
            concurrent_write_page(h1, &lock_table, s1, test_page(30), test_data()).unwrap();
        }
        // T2 operations.
        {
            let h2 = registry.get_mut(s2).unwrap();
            h2.record_read(test_page(50));
            concurrent_write_page(h2, &lock_table, s2, test_page(10), test_data()).unwrap();
        }
        // T3 operations.
        {
            let h3 = registry.get_mut(s3).unwrap();
            h3.record_read(test_page(30));
            concurrent_write_page(h3, &lock_table, s3, test_page(40), test_data()).unwrap();
        }

        // Step 1: T3 commits. T1 writes page 30, T3 reads page 30
        // (outgoing check: T1 wrote what T3 read). T1.has_in_rw = true.
        let result3 = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s3,
            CommitSeq::new(11),
        );
        assert!(result3.is_ok(), "T3 commits (only outgoing edge)");

        // Verify T1's has_in_rw was set by T3's outgoing edge detection.
        {
            let h1 = registry.get(s1).unwrap();
            assert!(
                h1.has_in_rw(),
                "T1 must have has_in_rw: T1 writes 30, T3 reads 30"
            );
            assert!(!h1.has_out_rw(), "T1 must NOT have has_out_rw yet");
            assert!(
                !h1.is_marked_for_abort(),
                "T1 must NOT be marked_for_abort yet"
            );
        }

        // Step 2: T2 commits. T2 writes page 10, T1 reads page 10
        // (incoming check: T1 read what T2 writes). T1.has_out_rw = true.
        // T1.has_in_rw is already true (from step 1) => T1 marked_for_abort!
        let result2 = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s2,
            CommitSeq::new(12),
        );
        assert!(
            result2.is_ok(),
            "T2 commits (only incoming edge, not pivot)"
        );

        // Verify T1 is now marked_for_abort.
        {
            let h1 = registry.get(s1).unwrap();
            assert!(h1.has_in_rw(), "T1 still has has_in_rw (from T3's commit)");
            assert!(
                h1.has_out_rw(),
                "T1 now has has_out_rw (T2's incoming edge scan set it)"
            );
            assert!(
                h1.is_marked_for_abort(),
                "T1 must be marked_for_abort: T2 found incoming edge from T1, \
                 and T1 already had has_in_rw from T3's commit"
            );
        }

        // Step 3: T1 tries to commit → fails with BusySnapshot
        // (marked_for_abort).
        let result1 = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s1,
            CommitSeq::new(13),
        );
        assert!(
            result1.is_err(),
            "T1 must abort: marked_for_abort by T2's commit scan"
        );
        let (err, _) = result1.unwrap_err();
        assert_eq!(err, MvccError::BusySnapshot);
    }

    // Test 23 (bd-mblr.6.7): SSI edge propagation — verify that a
    // commit's edge scan correctly sets other transactions' SSI flags
    // without any manual flag manipulation.
    #[test]
    fn test_ssi_edge_propagation_sets_flags_automatically() {
        use super::concurrent_commit_with_ssi;

        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        // T1: reads page 100, writes page 200.
        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        // T2: reads page 200, writes page 300.
        let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        {
            let h1 = registry.get_mut(s1).unwrap();
            h1.record_read(test_page(100));
            concurrent_write_page(h1, &lock_table, s1, test_page(200), test_data()).unwrap();
        }
        {
            let h2 = registry.get_mut(s2).unwrap();
            h2.record_read(test_page(200));
            concurrent_write_page(h2, &lock_table, s2, test_page(300), test_data()).unwrap();
        }

        // Before any commit: both T1 and T2 have no SSI flags.
        {
            let h1 = registry.get(s1).unwrap();
            let h2 = registry.get(s2).unwrap();
            assert!(!h1.has_in_rw());
            assert!(!h1.has_out_rw());
            assert!(!h2.has_in_rw());
            assert!(!h2.has_out_rw());
        }

        // T1 commits: T2 reads page 200, T1 writes page 200 →
        // incoming edge from T2→T1. T2.has_out_rw = true.
        // No outgoing edge (T1 reads 100, T2 writes 300 → no overlap).
        let result1 = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s1,
            CommitSeq::new(11),
        );
        assert!(result1.is_ok(), "T1 commits (only incoming edge)");

        // Verify T2's flags were set by the edge scan.
        {
            let h2 = registry.get(s2).unwrap();
            assert!(
                h2.has_out_rw(),
                "T2.has_out_rw must be set: T2 read page 200 that T1 wrote"
            );
            assert!(
                !h2.has_in_rw(),
                "T2.has_in_rw must NOT be set: no outgoing edge from T1 to T2"
            );
        }

        // T2 can commit (only has outgoing edge, not pivot).
        let result2 = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s2,
            CommitSeq::new(12),
        );
        assert!(
            result2.is_ok(),
            "T2 commits (only outgoing edge, not pivot)"
        );
    }

    // Test 24 (bd-mblr.6.7): FCW conflict detection with real CommitIndex
    // — verifies first-committer-wins uses real commit_index state.
    //
    // T1 and T2 both start with snapshot at seq 10.
    // T1 writes page 42, commits at seq 11 → commit_index[page42] = 11.
    // After T1 commits, the page lock is released.
    // T2 then writes page 42 (lock now available), and tries to commit.
    // FCW detects: commit_index[page42] = 11 > snapshot.high = 10 → conflict.
    #[test]
    fn test_fcw_real_commit_index_conflict() {
        use super::concurrent_commit_with_ssi;

        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        // T1 writes page 42 and commits first.
        {
            let h1 = registry.get_mut(s1).unwrap();
            concurrent_write_page(h1, &lock_table, s1, test_page(42), test_data()).unwrap();
        }
        let result1 = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s1,
            CommitSeq::new(11),
        );
        assert!(result1.is_ok(), "T1 first-committer wins");

        // After T1 committed, the page lock on page 42 is released.
        // T2 now writes the same page.
        {
            let h2 = registry.get_mut(s2).unwrap();
            concurrent_write_page(h2, &lock_table, s2, test_page(42), test_data()).unwrap();
        }

        // T2 commits second — FCW detects page 42 was modified after
        // T2's snapshot (commit_seq 11 > snapshot high 10).
        let result2 = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s2,
            CommitSeq::new(12),
        );
        assert!(result2.is_err(), "T2 must fail: FCW conflict on page 42");
        let (err, fcw) = result2.unwrap_err();
        assert_eq!(err, MvccError::BusySnapshot);
        assert!(
            matches!(fcw, FcwResult::Conflict { .. }),
            "FCW must report conflict"
        );
    }

    // Test 25 (bd-2kkn4.5): Deterministic FCW tiebreaker policy.
    //
    // Simultaneous commit window (same commit_seq) is resolved deterministically
    // by ordering commit attempts on txn_id. Lower txn_id commits first and
    // therefore wins; higher txn_id sees BusySnapshot.
    #[test]
    fn test_fcw_deterministic_tiebreak_lower_txn_id_wins() {
        use super::concurrent_commit_with_ssi;

        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        let token1 = registry.get(s1).unwrap().txn_token();
        let token2 = registry.get(s2).unwrap().txn_token();
        let (winner_session, loser_session, winner_token) = if token1.id <= token2.id {
            (s1, s2, token1)
        } else {
            (s2, s1, token2)
        };

        {
            let winner_handle = registry.get_mut(winner_session).unwrap();
            concurrent_write_page(
                winner_handle,
                &lock_table,
                winner_session,
                test_page(77),
                test_data(),
            )
            .unwrap();
        }

        // Same commit sequence for both contenders (simultaneous window).
        let winner = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            winner_session,
            CommitSeq::new(11),
        );
        assert!(
            winner.is_ok(),
            "lower txn_id should deterministically win tie window"
        );

        {
            let loser_handle = registry.get_mut(loser_session).unwrap();
            concurrent_write_page(
                loser_handle,
                &lock_table,
                loser_session,
                test_page(77),
                test_data(),
            )
            .unwrap();
        }

        let loser = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            loser_session,
            CommitSeq::new(11),
        );
        assert!(
            loser.is_err(),
            "higher txn_id should lose deterministic tie"
        );
        let (err, fcw) = loser.unwrap_err();
        assert_eq!(err, MvccError::BusySnapshot);
        assert!(matches!(fcw, FcwResult::Conflict { .. }));

        let committed = registry
            .committed_writers
            .iter()
            .find(|writer| writer.pages.contains(&test_page(77)))
            .map(|writer| writer.token)
            .expect("winning writer should be recorded");
        assert_eq!(committed, winner_token);
    }
}
