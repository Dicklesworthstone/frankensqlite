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

use fsqlite_types::{CommitSeq, PageData, PageNumber, Snapshot, TxnEpoch, TxnId, TxnToken, WitnessKey};

use crate::core_types::{CommitIndex, InProcessPageLockTable, TransactionMode, TransactionState};
use crate::lifecycle::MvccError;
use crate::ssi_validation::{ActiveTxnView, SsiAbortReason};

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
        self.write_set.keys().map(|&p| WitnessKey::Page(p)).collect()
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
        FcwResult::Clean
    } else {
        // Sort for deterministic output.
        conflicting_pages.sort();
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
                lock_table.release_all(txn_id);
                handle.mark_aborted();
                return Err((MvccError::BusySnapshot, FcwResult::Clean));
            }

            // Check for dangerous structure (both in + out rw edges).
            if handle.has_in_rw() && handle.has_out_rw() {
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

    // Step 2: Discover SSI edges by iterating over all handles immutably.
    // The Cell-based SSI flags allow mutation through shared references.
    let (has_in_rw, has_out_rw, is_marked_for_abort) = {
        let handles: Vec<_> = registry.iter_active().collect();
        let our_handle = handles
            .iter()
            .find(|(id, _)| *id == session_id)
            .map(|(_, h)| *h);

        let our_handle = match our_handle {
            Some(h) => h,
            None => return Err((MvccError::InvalidState, FcwResult::Clean)),
        };

        let write_keys = our_handle.write_witness_keys();

        // Skip SSI for write-only transactions.
        if write_keys.is_empty() {
            lock_table.release_all(txn_id);
            let handle = registry.get_mut(session_id).unwrap();
            handle.mark_committed();
            return Ok(assign_commit_seq);
        }

        let mut has_in_rw = false;
        let mut has_out_rw = false;

        for &(other_id, other_handle) in &handles {
            if other_id == session_id {
                continue;
            }
            if !other_handle.is_active() {
                continue;
            }

            // Incoming edge: other read a page we're writing.
            let other_reads = other_handle.read_set();
            for write_page in our_handle.write_set.keys() {
                if other_reads.contains(write_page) {
                    has_in_rw = true;
                    other_handle.has_out_rw.set(true);
                    if other_handle.has_in_rw() {
                        other_handle.marked_for_abort.set(true);
                    }
                    break;
                }
            }

            // Outgoing edge: other wrote a page we've read.
            let other_writes: Vec<_> = other_handle.write_set.keys().collect();
            for read_page in our_handle.read_set() {
                if other_writes.iter().any(|&&p| p == *read_page) {
                    has_out_rw = true;
                    other_handle.has_in_rw.set(true);
                    break;
                }
            }

            if has_in_rw && has_out_rw {
                break;
            }
        }

        // Update our flags.
        our_handle.has_in_rw.set(has_in_rw);
        our_handle.has_out_rw.set(has_out_rw);

        (has_in_rw, has_out_rw, our_handle.is_marked_for_abort())
    };

    // Check abort conditions.
    if is_marked_for_abort || (has_in_rw && has_out_rw) {
        lock_table.release_all(txn_id);
        let handle = registry.get_mut(session_id).unwrap();
        handle.mark_aborted();
        return Err((MvccError::BusySnapshot, FcwResult::Clean));
    }

    // SSI passed. Commit.
    let handle = registry.get_mut(session_id).unwrap();
    for &page in handle.write_set.keys() {
        commit_index.update(page, assign_commit_seq);
    }
    lock_table.release_all(txn_id);
    handle.mark_committed();
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
        let seq1 = concurrent_commit_with_ssi(&mut registry, &commit_index, &lock_table, s1, CommitSeq::new(11))
            .expect("T1 commits");
        assert_eq!(seq1, CommitSeq::new(11));

        let seq2 = concurrent_commit_with_ssi(&mut registry, &commit_index, &lock_table, s2, CommitSeq::new(12))
            .expect("T2 commits");
        assert_eq!(seq2, CommitSeq::new(12));
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
            concurrent_write_page(h1, &lock_table, s1, test_page(10), test_data()).unwrap(); // B
        }

        // T2: reads page 10 (B), writes page 5 (A).
        {
            let h2 = registry.get_mut(s2).unwrap();
            h2.record_read(test_page(10)); // B
            concurrent_write_page(h2, &lock_table, s2, test_page(5), test_data()).unwrap(); // A
        }

        // T1 tries to commit first: it has both incoming and outgoing edges.
        // Incoming: T2 read B (page 10), T1 writes B → edge from T2 to T1
        // Outgoing: T1 read A (page 5), T2 writes A → edge from T1 to T2
        // T1 is pivot and must abort.
        let result1 = concurrent_commit_with_ssi(&mut registry, &commit_index, &lock_table, s1, CommitSeq::new(11));
        assert!(result1.is_err(), "T1 should abort as pivot (both in and out edges)");
        let (err, _) = result1.unwrap_err();
        assert_eq!(err, MvccError::BusySnapshot);

        // After T1 aborts, T2 can now commit (T1 is no longer active, so no edges).
        let result2 = concurrent_commit_with_ssi(&mut registry, &commit_index, &lock_table, s2, CommitSeq::new(11));
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
}
