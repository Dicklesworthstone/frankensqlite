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
//!    any page modified by a concurrent transaction since the snapshot was taken
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
    ActiveTxnView, CommittedReaderInfo, CommittedWriterInfo, DiscoveredEdge, SsiAbortReason,
    discover_incoming_edges, discover_outgoing_edges, evaluate_t3_dro, witness_key_page,
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
    /// SSI abort due to anomalous access patterns.
    Abort {
        reason: crate::ssi_validation::SsiAbortReason,
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
    /// Granular read witnesses bucketed by page for O(1) page-level lookup.
    /// Uses SmallVec to avoid allocations for the common case of 1-4 witnesses per page.
    read_index: HashMap<PageNumber, smallvec::SmallVec<[WitnessKey; 4]>>,
    /// Witnesses that are not bound to a specific page (global or custom).
    global_read_witnesses: Vec<WitnessKey>,
    /// Granular write witnesses bucketed by page.
    write_index: HashMap<PageNumber, smallvec::SmallVec<[WitnessKey; 4]>>,
    /// Global write witnesses.
    global_write_witnesses: Vec<WitnessKey>,
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
            read_index: HashMap::new(),
            global_read_witnesses: Vec::new(),
            write_index: HashMap::new(),
            global_write_witnesses: Vec::new(),
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

    /// Access the raw write set (for `is_dirty` checks).
    #[must_use]
    pub fn write_set(&self) -> &HashMap<PageNumber, PageData> {
        &self.write_set
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

    /// Return the transaction token used by SSI and commit tracking.
    #[must_use]
    pub const fn txn_token(&self) -> TxnToken {
        self.txn_token
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
        if self.read_set.insert(page) {
            self.read_index
                .entry(page)
                .or_default()
                .push(WitnessKey::Page(page));
        }
    }

    /// Record a granular read witness for fine-grained SSI.
    pub fn record_read_witness(&mut self, key: WitnessKey) {
        if let Some(p) = witness_key_page(&key) {
            self.read_set.insert(p);
            self.read_index.entry(p).or_default().push(key);
        } else {
            self.global_read_witnesses.push(key);
        }
    }

    /// Record a granular write witness for fine-grained SSI.
    pub fn record_write_witness(&mut self, key: WitnessKey) {
        if let Some(p) = witness_key_page(&key) {
            self.page_locks.insert(p);
            self.write_index.entry(p).or_default().push(key);
        } else {
            self.global_write_witnesses.push(key);
        }
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

    /// Returns witness keys for all read pages (for SSI validation).
    #[must_use]
    pub fn read_witness_keys(&self) -> Vec<WitnessKey> {
        let mut keys: Vec<_> = self.read_index.values().flatten().cloned().collect();
        keys.extend(self.global_read_witnesses.iter().cloned());
        keys
    }

    /// Returns witness keys for all written pages (for SSI validation).
    #[must_use]
    pub fn write_witness_keys(&self) -> Vec<WitnessKey> {
        let mut keys: Vec<_> = self.write_index.values().flatten().cloned().collect();
        keys.extend(self.global_write_witnesses.iter().cloned());
        keys
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
        // NOTE: We can't return a slice directly since indices are non-contiguous.
        &[]
    }

    fn write_keys(&self) -> &[WitnessKey] {
        &[]
    }

    fn check_read_overlap(&self, key: &WitnessKey) -> bool {
        // First check global witnesses.
        if self
            .global_read_witnesses
            .iter()
            .any(|w| crate::witness_plane::witness_keys_overlap(w, key))
        {
            return true;
        }

        // Level 0: Fast reject if the page isn't in our read set at all.
        let page = match witness_key_page(key) {
            Some(p) => p,
            None => return !self.read_set.is_empty() || !self.global_read_witnesses.is_empty(),
        };

        if !self.read_set.contains(&page) {
            return false;
        }

        // Level 1: Refined check against witnesses for THIS page only.
        if let Some(witnesses) = self.read_index.get(&page) {
            return witnesses
                .iter()
                .any(|w| crate::witness_plane::witness_keys_overlap(w, key));
        }

        // Fallback: If no granular index but page is in read_set, assume overlap.
        true
    }

    fn check_write_overlap(&self, key: &WitnessKey) -> bool {
        if self
            .global_write_witnesses
            .iter()
            .any(|w| crate::witness_plane::witness_keys_overlap(w, key))
        {
            return true;
        }

        let page = match witness_key_page(key) {
            Some(p) => p,
            None => return !self.write_set.is_empty() || !self.global_write_witnesses.is_empty(),
        };

        if !self.write_set.contains_key(&page) {
            return false;
        }

        if let Some(witnesses) = self.write_index.get(&page) {
            return witnesses
                .iter()
                .any(|w| crate::witness_plane::witness_keys_overlap(w, key));
        }

        true
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
#[derive(Debug, Clone)]
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

        // Safety bound: prevent unbounded memory growth if a long-running
        // transaction pins the horizon.
        const MAX_HISTORY_ENTRIES: usize = 16384;
        while self.committed_readers.len() + self.committed_writers.len() > MAX_HISTORY_ENTRIES {
            // Find the oldest active transaction and mark it for abort to unpin the horizon.
            let mut oldest_id = None;
            let mut oldest_seq = CommitSeq::new(u64::MAX);
            for (&id, handle) in &self.active {
                if handle.is_active()
                    && !handle.is_marked_for_abort()
                    && handle.snapshot.high < oldest_seq
                {
                    oldest_seq = handle.snapshot.high;
                    oldest_id = Some(id);
                }
            }

            if let Some(id) = oldest_id {
                tracing::warn!(
                    session_id = id,
                    snapshot_high = oldest_seq.get(),
                    "prune_committed_conflict_history: marking long-running transaction for abort due to SSI history limit"
                );
                if let Some(handle) = self.active.get_mut(&id) {
                    handle.set_marked_for_abort(true);
                }

                // Recompute horizon and prune again now that the oldest is marked for abort
                let new_horizon = self.gc_horizon().unwrap_or(CommitSeq::new(u64::MAX));
                self.committed_readers
                    .retain(|reader| reader.commit_seq > new_horizon);
                self.committed_writers
                    .retain(|writer| writer.commit_seq > new_horizon);
            } else {
                // If we couldn't find an active transaction to abort (should be impossible if
                // history is bounded by active transactions), just clear everything.
                tracing::warn!("prune_committed_conflict_history: forced to clear SSI history");
                self.committed_readers.clear();
                self.committed_writers.clear();
                break;
            }
        }
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
            .filter(|h| h.is_active() && !h.is_marked_for_abort())
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
    handle.record_write_witness(fsqlite_types::WitnessKey::Page(page));
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

/// Prepared concurrent commit plan produced by SSI validation.
///
/// This captures the pre-commit validation result without publishing commit
/// side effects. Callers should run physical pager commit first, then pass
/// this plan to [`finalize_prepared_concurrent_commit_with_ssi`] to publish
/// conflict history and edge propagation.
#[derive(Debug, Clone)]
pub struct PreparedConcurrentCommit {
    session_id: u64,
    assigned_commit_seq: CommitSeq,
    txn_token: TxnToken,
    begin_seq: CommitSeq,
    read_keys: Vec<WitnessKey>,
    write_keys: Vec<WitnessKey>,
    write_set_pages: Vec<PageNumber>,
    has_in_rw: bool,
    has_out_rw: bool,
    incoming_edges: Vec<DiscoveredEdge>,
    outgoing_edges: Vec<DiscoveredEdge>,
    dro_t3_decision: Option<crate::ssi_abort_policy::DroHotPathDecision>,
}

impl PreparedConcurrentCommit {
    #[must_use]
    pub const fn session_id(&self) -> u64 {
        self.session_id
    }

    #[must_use]
    pub const fn assigned_commit_seq(&self) -> CommitSeq {
        self.assigned_commit_seq
    }

    #[must_use]
    pub const fn txn_token(&self) -> TxnToken {
        self.txn_token
    }

    #[must_use]
    pub const fn begin_seq(&self) -> CommitSeq {
        self.begin_seq
    }

    #[must_use]
    pub const fn has_in_rw(&self) -> bool {
        self.has_in_rw
    }

    #[must_use]
    pub const fn has_out_rw(&self) -> bool {
        self.has_out_rw
    }

    #[must_use]
    pub fn read_keys(&self) -> &[WitnessKey] {
        &self.read_keys
    }

    #[must_use]
    pub fn write_keys(&self) -> &[WitnessKey] {
        &self.write_keys
    }

    #[must_use]
    pub const fn dro_t3_decision(&self) -> Option<crate::ssi_abort_policy::DroHotPathDecision> {
        self.dro_t3_decision
    }
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
        self.handle.token()
    }

    fn begin_seq(&self) -> CommitSeq {
        self.handle.begin_seq()
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

    fn check_read_overlap(&self, key: &WitnessKey) -> bool {
        match key {
            WitnessKey::Page(p)
            | WitnessKey::Cell { btree_root: p, .. }
            | WitnessKey::ByteRange { page: p, .. }
            | WitnessKey::KeyRange { btree_root: p, .. } => self.handle.read_set.contains(p),
            WitnessKey::Custom { .. } => !self.handle.read_set.is_empty(), // Conservative fallback
        }
    }

    fn check_write_overlap(&self, key: &WitnessKey) -> bool {
        match key {
            WitnessKey::Page(p)
            | WitnessKey::Cell { btree_root: p, .. }
            | WitnessKey::ByteRange { page: p, .. }
            | WitnessKey::KeyRange { btree_root: p, .. } => self.handle.write_set.contains_key(p),
            WitnessKey::Custom { .. } => !self.handle.write_set.is_empty(), // Conservative fallback
        }
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

fn evaluate_prepare_t3_dro(
    txn: TxnToken,
    incoming_edges: &[DiscoveredEdge],
    outgoing_edges: &[DiscoveredEdge],
    active_txn_count: usize,
    committed_reader_count: usize,
    committed_writer_count: usize,
) -> Option<crate::ssi_abort_policy::DroHotPathDecision> {
    if incoming_edges.is_empty() && outgoing_edges.is_empty() {
        return None;
    }

    let active_other_txns = active_txn_count.saturating_sub(1);
    let active_reader_population = active_other_txns
        .max(incoming_edges.len())
        .saturating_add(committed_reader_count);
    let active_writer_population = active_other_txns
        .max(outgoing_edges.len())
        .saturating_add(committed_writer_count);

    Some(evaluate_t3_dro(
        txn,
        active_reader_population,
        active_writer_population,
    ))
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
        FcwResult::Conflict { .. } | FcwResult::Abort { .. } => {
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
pub fn prepare_concurrent_commit_with_ssi(
    registry: &mut ConcurrentRegistry,
    commit_index: &CommitIndex,
    lock_table: &InProcessPageLockTable,
    session_id: u64,
    assign_commit_seq: CommitSeq,
) -> Result<PreparedConcurrentCommit, (MvccError, FcwResult)> {
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
            if let Some(handle) = registry.get_mut(session_id) {
                handle.mark_aborted();
            }
            return Err((MvccError::BusySnapshot, fcw_result));
        }
    }

    // Build view of committing txn state.
    let (txn, begin_seq, read_keys, write_keys, write_set_pages, marked_for_abort) = {
        let handle = registry
            .get(session_id)
            .ok_or((MvccError::InvalidState, FcwResult::Clean))?;

        let read_keys = handle.read_witness_keys();
        let write_keys = handle.write_witness_keys();
        let write_set_pages = handle.write_set.keys().copied().collect::<Vec<_>>();

        (
            handle.token(),
            handle.begin_seq(),
            read_keys,
            write_keys,
            write_set_pages,
            handle.marked_for_abort.get(),
        )
    };

    if marked_for_abort {
        tracing::warn!(
            txn = %txn_id,
            "prepare_concurrent_commit_with_ssi: marked_for_abort"
        );
        lock_table.release_all(txn_id);
        if let Some(handle) = registry.get_mut(session_id) {
            handle.mark_aborted();
        }
        return Err((MvccError::BusySnapshot, FcwResult::Clean));
    }

    // Sort keys for deterministic evidence ordering.
    let mut sorted_read_keys = read_keys.clone();
    sorted_read_keys.sort_unstable();
    let mut sorted_write_keys = write_keys.clone();
    sorted_write_keys.sort_unstable();

    // Step 2: Discover SSI edges without publishing side effects yet.
    let views: Vec<HandleView<'_>> = registry
        .iter_active()
        .filter(|(_, other)| other.is_active())
        .map(|(_, other)| HandleView::new(other))
        .collect();
    let active_views: Vec<&dyn ActiveTxnView> = views
        .iter()
        .map(|view| view as &dyn ActiveTxnView)
        .collect();

    let incoming_edges = discover_incoming_edges(
        txn,
        begin_seq,
        assign_commit_seq,
        &sorted_write_keys,
        &active_views,
        &registry.committed_readers,
    );
    let outgoing_edges = discover_outgoing_edges(
        txn,
        begin_seq,
        assign_commit_seq,
        &sorted_read_keys,
        &active_views,
        &registry.committed_writers,
    );

    let has_in_rw = !incoming_edges.is_empty();
    let has_out_rw = !outgoing_edges.is_empty();
    let dro_t3_decision = evaluate_prepare_t3_dro(
        txn,
        &incoming_edges,
        &outgoing_edges,
        registry.active.len(),
        registry.committed_readers.len(),
        registry.committed_writers.len(),
    );

    if has_in_rw && has_out_rw {
        let reason = SsiAbortReason::Pivot;
        tracing::warn!(
            ?txn,
            ?reason,
            dro_penalty = dro_t3_decision.map_or(0.0, |decision| decision.cvar_penalty),
            dro_threshold = dro_t3_decision.map_or(0.0, |decision| decision.threshold),
            "SSI validation aborted"
        );
        return Err((MvccError::BusySnapshot, FcwResult::Abort { reason }));
    }

    Ok(PreparedConcurrentCommit {
        session_id,
        assigned_commit_seq: assign_commit_seq,
        txn_token: txn,
        begin_seq,
        read_keys: sorted_read_keys,
        write_keys: sorted_write_keys,
        write_set_pages,
        has_in_rw,
        has_out_rw,
        incoming_edges,
        outgoing_edges,
        dro_t3_decision,
    })
}

/// Publish a previously prepared concurrent commit after physical pager commit.
///
/// Applies SSI side effects (T3 propagation), records committed conflict
/// history, updates the commit index, and releases page locks.
#[allow(clippy::too_many_lines)]
pub fn finalize_prepared_concurrent_commit_with_ssi(
    registry: &mut ConcurrentRegistry,
    commit_index: &CommitIndex,
    lock_table: &InProcessPageLockTable,
    prepared: &PreparedConcurrentCommit,
    committed_seq: CommitSeq,
) {
    debug_assert_eq!(
        committed_seq, prepared.assigned_commit_seq,
        "prepared commit sequence mismatch"
    );

    let Some(txn_id) = TxnId::new(prepared.session_id) else {
        return;
    };

    // Re-scan against current active state to capture overlap edges that may
    // appear after prepare but before finalize. This keeps committed pivot
    // decisions deterministic.
    let active_views: Vec<HandleView> = registry.active.values().map(HandleView::new).collect();
    let active_refs: Vec<&dyn ActiveTxnView> = active_views
        .iter()
        .map(|view| view as &dyn ActiveTxnView)
        .collect();

    let mut incoming_edges = prepared.incoming_edges.clone();
    for edge in discover_incoming_edges(
        prepared.txn_token,
        prepared.begin_seq,
        committed_seq,
        &prepared.write_keys,
        &active_refs,
        &[],
    ) {
        if incoming_edges
            .iter()
            .all(|existing| existing.from != edge.from)
        {
            incoming_edges.push(edge);
        }
    }

    let mut outgoing_edges = prepared.outgoing_edges.clone();
    for edge in discover_outgoing_edges(
        prepared.txn_token,
        prepared.begin_seq,
        committed_seq,
        &prepared.read_keys,
        &active_refs,
        &[],
    ) {
        if outgoing_edges.iter().all(|existing| existing.to != edge.to) {
            outgoing_edges.push(edge);
        }
    }

    let has_in_rw = !incoming_edges.is_empty();
    let has_out_rw = !outgoing_edges.is_empty();
    let dro_t3_decision = evaluate_prepare_t3_dro(
        prepared.txn_token,
        &incoming_edges,
        &outgoing_edges,
        registry.active.len(),
        registry.committed_readers.len(),
        registry.committed_writers.len(),
    );
    let should_abort_active_pivot = dro_t3_decision.is_none_or(|decision| decision.should_abort());

    // T3 propagation for active readers on incoming edges.
    for edge in &incoming_edges {
        if !edge.source_is_active {
            continue;
        }
        if let Some(reader) = registry
            .active
            .values_mut()
            .find(|reader| reader.is_active() && reader.token() == edge.from)
        {
            reader.set_has_out_rw(true);
            if reader.has_in_rw() {
                if should_abort_active_pivot {
                    tracing::debug!(
                        pivot = ?edge.from,
                        dro_penalty = dro_t3_decision.map_or(0.0, |decision| decision.cvar_penalty),
                        dro_threshold = dro_t3_decision.map_or(0.0, |decision| decision.threshold),
                        "prepare/finalize T3 rule: active reader is pivot, marking for abort"
                    );
                    reader.set_marked_for_abort(true);
                } else {
                    tracing::debug!(
                        pivot = ?edge.from,
                        dro_penalty = dro_t3_decision.map_or(0.0, |decision| decision.cvar_penalty),
                        dro_threshold = dro_t3_decision.map_or(0.0, |decision| decision.threshold),
                        "prepare/finalize T3 rule: active reader is pivot, DRO allows it to continue"
                    );
                }
            }
        }
    }

    // T3 propagation for active writers on outgoing edges.
    for edge in &outgoing_edges {
        if !edge.source_is_active {
            continue;
        }
        if let Some(writer) = registry
            .active
            .values_mut()
            .find(|writer| writer.is_active() && writer.token() == edge.to)
        {
            writer.set_has_in_rw(true);
            if writer.has_out_rw() {
                if should_abort_active_pivot {
                    tracing::debug!(
                        pivot = ?edge.to,
                        dro_penalty = dro_t3_decision.map_or(0.0, |decision| decision.cvar_penalty),
                        dro_threshold = dro_t3_decision.map_or(0.0, |decision| decision.threshold),
                        "prepare/finalize T3 rule: active writer is pivot, marking for abort"
                    );
                    writer.set_marked_for_abort(true);
                } else {
                    tracing::debug!(
                        pivot = ?edge.to,
                        dro_penalty = dro_t3_decision.map_or(0.0, |decision| decision.cvar_penalty),
                        dro_threshold = dro_t3_decision.map_or(0.0, |decision| decision.threshold),
                        "prepare/finalize T3 rule: active writer is pivot, DRO allows it to continue"
                    );
                }
            }
        }
    }

    let mut mark_committed = false;
    if let Some(handle) = registry.get_mut(prepared.session_id) {
        if handle.is_active() {
            handle.has_in_rw.set(has_in_rw);
            handle.has_out_rw.set(has_out_rw);
            mark_committed = true;
        } else {
            tracing::warn!(
                session_id = prepared.session_id,
                "finalize_prepared_concurrent_commit_with_ssi: session inactive during finalize; applying commit-index/lock-table side effects"
            );
        }
    } else {
        tracing::warn!(
            session_id = prepared.session_id,
            "finalize_prepared_concurrent_commit_with_ssi: session missing during finalize; applying commit-index/lock-table side effects"
        );
    }

    for &page in &prepared.write_set_pages {
        commit_index.update(page, committed_seq);
    }
    lock_table.release_all(txn_id);
    if mark_committed {
        if let Some(handle) = registry.get_mut(prepared.session_id) {
            if handle.is_active() {
                handle.mark_committed();
            }
        }
    }

    if !prepared.read_keys.is_empty() {
        registry.committed_readers.push(CommittedReaderInfo {
            token: prepared.txn_token,
            begin_seq: prepared.begin_seq,
            commit_seq: committed_seq,
            had_in_rw: has_in_rw,
            keys: prepared.read_keys.clone(),
        });
    }
    if !prepared.write_keys.is_empty() {
        registry.committed_writers.push(CommittedWriterInfo {
            token: prepared.txn_token,
            commit_seq: committed_seq,
            had_out_rw: has_out_rw,
            keys: prepared.write_keys.clone(),
        });
    }
    registry.prune_committed_conflict_history();
}

#[allow(clippy::too_many_lines)]
pub fn concurrent_commit_with_ssi(
    registry: &mut ConcurrentRegistry,
    commit_index: &CommitIndex,
    lock_table: &InProcessPageLockTable,
    session_id: u64,
    assign_commit_seq: CommitSeq,
) -> Result<CommitSeq, (MvccError, FcwResult)> {
    let prepared = match prepare_concurrent_commit_with_ssi(
        registry,
        commit_index,
        lock_table,
        session_id,
        assign_commit_seq,
    ) {
        Ok(p) => p,
        Err(e) => {
            if let Some(mut handle) = registry.remove(session_id) {
                concurrent_abort(&mut handle, lock_table, session_id);
            }
            return Err(e);
        }
    };
    finalize_prepared_concurrent_commit_with_ssi(
        registry,
        commit_index,
        lock_table,
        &prepared,
        assign_commit_seq,
    );
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
        concurrent_write_page, finalize_prepared_concurrent_commit_with_ssi,
        prepare_concurrent_commit_with_ssi, validate_first_committer_wins,
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
            FcwResult::Abort { .. } => panic!("expected conflict, got abort"),
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

    #[test]
    fn test_finalize_releases_locks_and_updates_commit_index_when_session_missing() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session 1");
        {
            let h1 = registry.get_mut(s1).expect("handle 1");
            concurrent_write_page(h1, &lock_table, s1, test_page(5), test_data())
                .expect("session 1 writes page 5");
        }

        let prepared = prepare_concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s1,
            CommitSeq::new(11),
        )
        .expect("prepare should succeed");

        let removed = registry.remove(s1);
        assert!(
            removed.is_some(),
            "session should be removable to simulate handle disappearance"
        );

        finalize_prepared_concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            &prepared,
            CommitSeq::new(11),
        );

        assert_eq!(
            commit_index.latest(test_page(5)),
            Some(CommitSeq::new(11)),
            "finalize must update commit index even if handle is missing"
        );

        let s2 = registry
            .begin_concurrent(test_snapshot(11))
            .expect("session 2");
        let h2 = registry.get_mut(s2).expect("handle 2");
        concurrent_write_page(h2, &lock_table, s2, test_page(5), test_data())
            .expect("page lock should be released during finalize");
    }

    #[test]
    fn test_prepare_materializes_dro_decision_for_edgeful_commit() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        {
            let h1 = registry.get_mut(s1).unwrap();
            h1.record_read(test_page(20));
            concurrent_write_page(h1, &lock_table, s1, test_page(10), test_data()).unwrap();
        }
        {
            let h2 = registry.get_mut(s2).unwrap();
            h2.record_read(test_page(30));
            concurrent_write_page(h2, &lock_table, s2, test_page(20), test_data()).unwrap();
        }

        let prepared = prepare_concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s1,
            CommitSeq::new(11),
        )
        .expect("prepare should succeed");

        let decision = prepared
            .dro_t3_decision()
            .expect("edgeful prepare should materialize a DRO decision");
        assert_eq!(decision.active_readers, 1);
        assert_eq!(decision.active_writers, 1);
        assert!(decision.threshold >= 0.0);
    }

    #[test]
    fn test_prepare_skips_dro_decision_for_edge_free_commit() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        {
            let h1 = registry.get_mut(s1).unwrap();
            concurrent_write_page(h1, &lock_table, s1, test_page(5), test_data()).unwrap();
        }

        let prepared = prepare_concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s1,
            CommitSeq::new(11),
        )
        .expect("prepare should succeed");

        assert!(
            prepared.dro_t3_decision().is_none(),
            "edge-free prepare should not emit a DRO decision"
        );
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
            // B
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
            CommitSeq::new(11),
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
        // T1: reads A, writes B.
        // T2: reads B, writes A.
        //
        // When T1 tries to commit:
        // - Incoming: T2 read B, T1 writes B → incoming edge from T2 to T1
        // - Outgoing: T1 read A, T2 writes A → outgoing edge to T2
        // T1 has BOTH → T1 must abort.

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

        // After T1 aborted, T2 can now commit (T1 is no longer active, so no edges).
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
    // After T1 aborts, T2 can now commit because the pivot (T1) is gone.
    //
    // This test exercises the full real-component SSI path without any
    // mock objects or manual flag manipulation.
    #[test]
    fn test_ssi_three_txn_pivot_abort_real_components() {
        use super::concurrent_commit_with_ssi;

        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        // T1: reads pages {10, 20}, writes page 30.
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
        // T1 keeps running because the DRO hot-path decision stays below the
        // abort threshold at this contention level.
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

        // Verify T1 is still live even though it now carries both SSI flags.
        {
            let h1 = registry.get(s1).unwrap();
            assert!(h1.has_in_rw(), "T1 still has has_in_rw (from T3's commit)");
            assert!(
                h1.has_out_rw(),
                "T1 now has has_out_rw (T2's incoming edge scan set it)"
            );
            assert!(
                !h1.is_marked_for_abort(),
                "low-contention DRO should defer the active-pivot abort mark"
            );
        }

        // Step 3: T1 tries to commit → fails with BusySnapshot
        // (actual pivot detected during T1's own commit-time scan).
        let result1 = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s1,
            CommitSeq::new(13),
        );
        assert!(
            result1.is_err(),
            "T1 must still abort when its own commit observes the full pivot"
        );
        let (err, _) = result1.unwrap_err();
        assert_eq!(err, MvccError::BusySnapshot);
    }

    // Test 22 (bd-mblr.6.7): SSI edge propagation through real edge detection
    // with the DRO gate left open under low contention.
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
    //   T1.has_out_rw = true. T1 has both flags now, but the default
    //   low-contention DRO matrix keeps the active pivot running.
    //   T2 has only incoming edge => T2 commits.
    //
    // Step 3: T1 tries to commit → still fails with BusySnapshot
    // (actual pivot detected during T1's own commit-time scan).
    #[test]
    fn test_ssi_low_contention_dro_defers_marked_for_abort() {
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
        // T1 keeps running because the DRO hot-path decision stays below the
        // abort threshold at this contention level.
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

        // Verify T1 is still live even though it now carries both SSI flags.
        {
            let h1 = registry.get(s1).unwrap();
            assert!(h1.has_in_rw(), "T1 still has has_in_rw (from T3's commit)");
            assert!(
                h1.has_out_rw(),
                "T1 now has has_out_rw (T2's incoming edge scan set it)"
            );
            assert!(
                !h1.is_marked_for_abort(),
                "low-contention DRO should defer the active-pivot abort mark"
            );
        }

        // Step 3: T1 tries to commit → still fails with BusySnapshot
        // (actual pivot detected during T1's own commit-time scan).
        let result1 = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s1,
            CommitSeq::new(13),
        );
        assert!(
            result1.is_err(),
            "T1 must still abort when its own commit observes the full pivot"
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

    // Test 24 (bd-2kkn4.5): FCW conflict detection with real CommitIndex
    // — verifies first-committer-wins uses real commit_index state.
    //
    // T1 and T2 both start with snapshot at seq 10.
    // T1 writes page 42, commits at seq 11 → commit_index[page42] = 11.
    // After T1 commits, the page lock is released.
    // T2 then writes page 42 (lock now available), and tries to commit.
    // FCW detects: commit_index[page42] = 11 > snapshot high = 10 → conflict.
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
            CommitSeq::new(11),
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
            .find(|entry| entry.token.id.get() == winner_session)
            .map(|entry| entry.token)
            .expect("winning writer should be recorded");
        assert_eq!(committed, winner_token);
    }

    // Test 26: committed writer pivot forces abort in prepare path.
    //
    // Scenario:
    // - T1 reads B, writes A.
    // - T2 reads C, writes B (active while T1 commits), so T1 has outgoing rw.
    // - T3 reads A, writes D from an old snapshot.
    //
    // T1 commits and should carry had_out_rw=true in committed writer history.
    // T3 then discovers outgoing edge T3 -> T1. Because T1 was already a
    // committed writer pivot source, T3 must abort.
    #[test]
    fn test_prepare_aborts_on_committed_writer_pivot() {
        use super::concurrent_commit_with_ssi;

        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let s3 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        // T1: reads B (20), writes A (10)
        {
            let h1 = registry.get_mut(s1).unwrap();
            h1.record_read(test_page(20));
            concurrent_write_page(h1, &lock_table, s1, test_page(10), test_data()).unwrap();
        }

        // T2: reads C (30), writes B (20)
        {
            let h2 = registry.get_mut(s2).unwrap();
            h2.record_read(test_page(30));
            concurrent_write_page(h2, &lock_table, s2, test_page(20), test_data()).unwrap();
        }

        // T1 commits and should carry had_out_rw=true due to T2 writing B.
        let result1 = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s1,
            CommitSeq::new(11),
        );
        assert!(
            result1.is_ok(),
            "T1 should commit with only outgoing rw edge"
        );
        let t1_writer = registry
            .committed_writers
            .iter()
            .find(|entry| entry.token.id.get() == s1)
            .expect("T1 writer history should be present");
        assert!(
            t1_writer.had_out_rw,
            "T1 should be recorded with had_out_rw"
        );

        // T3 now performs its workload using the earlier snapshot.
        {
            let h3 = registry.get_mut(s3).unwrap();
            h3.record_read(test_page(10));
            concurrent_write_page(h3, &lock_table, s3, test_page(40), test_data()).unwrap();
        }

        // T3 must abort due to outgoing edge to committed writer pivot T1.
        let result3 = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s3,
            CommitSeq::new(12),
        );
        assert!(
            result3.is_err(),
            "T3 must abort when it depends on committed writer pivot T1"
        );
        let (err3, _) = result3.unwrap_err();
        assert_eq!(err3, MvccError::BusySnapshot);
    }
}
