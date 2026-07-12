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
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use fsqlite_types::{
    CommitSeq, PageData, PageNumber, PageNumberBuildHasher, Snapshot, TxnEpoch, TxnId, TxnToken,
    WitnessKey,
};

/// HashMap keyed by [`PageNumber`] with identity hashing.
///
/// Per §M5.2: PageNumber keys are already well-distributed u32 values; using
/// std `RandomState` (SipHash13) on PageNumber lookups costs ~70 cycles per
/// probe and shows up at 3-5% of wall time in UPDATE/DELETE hot paths.
/// `PageNumberBuildHasher` reduces this to a single mov+finish.
type PageMap<V> = HashMap<PageNumber, V, PageNumberBuildHasher>;
type PageSet = HashSet<PageNumber, PageNumberBuildHasher>;
use parking_lot::{Mutex, MutexGuard};

use crate::core_types::{CommitIndex, InProcessPageLockTable, TransactionMode, TransactionState};
use crate::lifecycle::MvccError;
use crate::observability::{
    ConflictHeatContext, ConflictHeatEdge, ConflictHeatObservation, ConflictOverlapDirection,
    conflict_heat_telemetry_enabled, record_conflict_heat_observation,
};
use crate::rcu::{ActiveTxnSnapshotEntry, QsbrHandle, RcuActiveTxnSnapshotTable};
use crate::ssi_validation::{
    ActiveTxnView, CommittedReaderInfo, CommittedWriterInfo, DiscoveredEdge, SsiAbortReason,
    discover_incoming_edges, discover_outgoing_edges, evaluate_t3_dro, witness_key_page,
};

/// Maximum number of concurrent writers that can be active simultaneously.
///
/// This is a soft limit enforced at `begin_concurrent` time to prevent
/// unbounded resource consumption.
pub const MAX_CONCURRENT_WRITERS: usize = 128;

#[inline]
fn same_txn_identity(left: TxnToken, right: TxnToken) -> bool {
    left.id.get() == right.id.get() && left.epoch.get() == right.epoch.get()
}

#[inline]
#[cfg(test)]
fn witness_is_page(witness: &WitnessKey, page: PageNumber) -> bool {
    match witness {
        WitnessKey::Page(candidate) => candidate.get().cmp(&page.get()).is_eq(),
        WitnessKey::Cell { .. }
        | WitnessKey::ByteRange { .. }
        | WitnessKey::KeyRange { .. }
        | WitnessKey::Custom { .. } => false,
    }
}

/// Stable shared handle for one active concurrent transaction.
pub type SharedConcurrentHandle = Arc<Mutex<ConcurrentHandle>>;

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
    /// Per-page transactional state for staged writes, frees, synthetic
    /// conflict tracking, and held page locks.
    page_states: PageMap<PageTxnState>,
    /// Transaction state (Active / Committed / Aborted).
    state: TransactionState,
    /// Pages read by this transaction (for SSI rw-antidependency detection).
    read_set: PageSet,
    /// Granular read witnesses bucketed by page for O(1) page-level lookup.
    /// Uses SmallVec to avoid allocations for the common case of 1-4 witnesses per page.
    read_index: PageMap<smallvec::SmallVec<[WitnessKey; 4]>>,
    /// Witnesses that are not bound to a specific page (global or custom).
    global_read_witnesses: Vec<WitnessKey>,
    /// Granular write witnesses bucketed by page.
    write_index: PageMap<smallvec::SmallVec<[WitnessKey; 4]>>,
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

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Default)]
struct PageTxnState {
    staged_data: Option<PageData>,
    has_staged_write: bool,
    is_freed: bool,
    is_conflict_only: bool,
    held_lock: bool,
    /// E3 (bd-wwqen Track E): Metadata pages (e.g., page 1 freelist) can be
    /// marked exempt from conflict checking. These pages are still written,
    /// but don't trigger FCW conflicts. This eliminates false conflicts when
    /// disjoint inserts both update freelist metadata on page 1.
    metadata_exempt: bool,
}

impl PageTxnState {
    #[must_use]
    fn tracks_write_conflict(&self) -> bool {
        // E3: Skip conflict tracking for metadata-exempt pages.
        if self.metadata_exempt {
            return false;
        }
        self.has_staged_write || self.is_freed || self.is_conflict_only
    }

    #[must_use]
    fn is_empty(&self) -> bool {
        !self.held_lock && !self.tracks_write_conflict()
    }
}

pub struct WriteSetView<'a> {
    page_states: &'a PageMap<PageTxnState>,
}

impl WriteSetView<'_> {
    #[must_use]
    pub fn contains_key(&self, page: &PageNumber) -> bool {
        self.page_states
            .get(page)
            .is_some_and(|state| state.has_staged_write)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.page_states
            .values()
            .all(|state| !state.has_staged_write)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.page_states
            .values()
            .filter(|state| state.has_staged_write)
            .count()
    }
}

pub struct HeldLocksView<'a> {
    page_states: &'a PageMap<PageTxnState>,
}

impl HeldLocksView<'_> {
    #[must_use]
    pub fn contains(&self, page: &PageNumber) -> bool {
        self.page_states
            .get(page)
            .is_some_and(|state| state.held_lock)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.page_states.values().all(|state| !state.held_lock)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.page_states
            .values()
            .filter(|state| state.held_lock)
            .count()
    }
}

/// Snapshot of one page's local concurrent tracking state.
///
/// This is used to restore the in-memory MVCC bookkeeping if the underlying
/// pager rejects a write/free after we already updated the concurrent handle.
/// SSI witnesses are intentionally not rolled back; that remains a safe
/// overapproximation just like savepoint rollback.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone)]
pub struct ConcurrentPageState {
    page: PageNumber,
    staged_data: Option<PageData>,
    had_staged_write: bool,
    was_freed: bool,
    was_conflict_only: bool,
    held_lock: bool,
}

impl ConcurrentPageState {
    #[must_use]
    pub fn is_synthetic_conflict_only(&self) -> bool {
        self.was_conflict_only && !self.had_staged_write && !self.was_freed
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Default)]
struct SavepointPageState {
    staged_data: Option<PageData>,
    has_staged_write: bool,
    is_freed: bool,
    is_conflict_only: bool,
    metadata_exempt: bool,
}

impl ConcurrentHandle {
    /// Create a new concurrent handle with the given snapshot and token.
    #[must_use]
    pub fn new(snapshot: Snapshot, txn_token: TxnToken) -> Self {
        Self {
            snapshot,
            page_states: PageMap::default(),
            state: TransactionState::Active,
            read_set: PageSet::default(),
            read_index: PageMap::default(),
            global_read_witnesses: Vec::new(),
            write_index: PageMap::default(),
            global_write_witnesses: Vec::new(),
            txn_token,
            has_in_rw: Cell::new(false),
            has_out_rw: Cell::new(false),
            marked_for_abort: Cell::new(false),
        }
    }

    /// Reset this handle for a new concurrent transaction while preserving
    /// allocation capacity across hot autocommit begin/commit cycles.
    pub fn reset_for_new_transaction(&mut self, snapshot: Snapshot, txn_token: TxnToken) {
        self.snapshot = snapshot;
        self.page_states.clear();
        self.state = TransactionState::Active;
        self.read_set.clear();
        self.read_index.clear();
        self.global_read_witnesses.clear();
        self.write_index.clear();
        self.global_write_witnesses.clear();
        self.txn_token = txn_token;
        self.has_in_rw.set(false);
        self.has_out_rw.set(false);
        self.marked_for_abort.set(false);
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
    /// Uses SmallVec to avoid heap allocation for typical transactions (≤16 pages).
    #[must_use]
    pub fn write_set_pages(&self) -> smallvec::SmallVec<[PageNumber; 16]> {
        let mut pages: smallvec::SmallVec<[PageNumber; 16]> =
            self.tracked_write_conflict_pages_iter().collect();
        pages.sort_unstable();
        pages
    }

    /// Iterate tracked write-conflict pages without rebuilding a sorted copy.
    fn tracked_write_conflict_pages_iter(&self) -> impl Iterator<Item = PageNumber> + '_ {
        self.page_states
            .iter()
            .filter_map(|(&page, state)| state.tracks_write_conflict().then_some(page))
    }

    /// Iterate the exact page locks currently held by this handle.
    fn held_lock_pages_iter(&self) -> impl Iterator<Item = PageNumber> + '_ {
        self.page_states
            .iter()
            .filter_map(|(&page, state)| state.held_lock.then_some(page))
    }

    /// Returns the number of pages in the write set.
    #[must_use]
    pub fn write_set_len(&self) -> usize {
        self.page_states
            .values()
            .filter(|state| state.has_staged_write)
            .count()
    }

    /// Access the raw write set (for `is_dirty` checks).
    #[must_use]
    pub fn write_set(&self) -> WriteSetView<'_> {
        WriteSetView {
            page_states: &self.page_states,
        }
    }

    #[must_use]
    pub fn is_page_freed(&self, page: PageNumber) -> bool {
        self.page_states
            .get(&page)
            .is_some_and(|state| state.is_freed)
    }

    #[must_use]
    pub fn tracks_write_conflict_page(&self, page: PageNumber) -> bool {
        self.page_states
            .get(&page)
            .is_some_and(PageTxnState::tracks_write_conflict)
    }

    /// Returns the set of page locks held.
    #[must_use]
    pub fn held_locks(&self) -> HeldLocksView<'_> {
        HeldLocksView {
            page_states: &self.page_states,
        }
    }

    /// Returns the exact set of page locks currently tracked by this handle.
    #[must_use]
    pub fn held_lock_pages(&self) -> Vec<PageNumber> {
        let mut pages = self.held_lock_pages_iter().collect::<Vec<_>>();
        pages.sort_unstable();
        pages
    }

    #[must_use]
    pub fn holds_page_lock(&self, page: PageNumber) -> bool {
        self.page_states
            .get(&page)
            .is_some_and(|state| state.held_lock)
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

    /// Record a metadata page read that does NOT participate in SSI tracking.
    ///
    /// E4 enhancement (bd-wwqen Track E): Page 1 (database header with freelist
    /// metadata) is read by every transaction that allocates pages. Tracking
    /// these reads in the SIREAD set causes massive false rw-antidependency
    /// conflicts between disjoint inserts. By skipping SSI registration for
    /// metadata-only reads, we eliminate this bottleneck.
    ///
    /// # Safety Guarantee
    ///
    /// This is safe because page 1 freelist reads are commutative operations:
    /// multiple transactions can read the freelist pointer, allocate different
    /// pages, and commit without conflict. The write-side is protected by FCW
    /// (and E3's metadata_exempt for the actual freelist pointer update).
    pub fn record_metadata_read(&mut self, _page: PageNumber) {
        // Intentionally skip read_set and read_index registration.
        // The page is still read, but won't trigger rw-antidependency edges.
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
            let witnesses = self.write_index.entry(p).or_default();
            if !witnesses.iter().any(|existing| existing == &key) {
                witnesses.push(key);
            }
        } else if !self
            .global_write_witnesses
            .iter()
            .any(|existing| existing == &key)
        {
            self.global_write_witnesses.push(key);
        }
    }

    /// Returns the set of pages that were read.
    #[must_use]
    pub fn read_set(&self) -> &PageSet {
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
        let capacity = self.global_read_witnesses.len()
            + self
                .read_index
                .values()
                .map(smallvec::SmallVec::len)
                .sum::<usize>();
        let mut keys = Vec::with_capacity(capacity);
        for witnesses in self.read_index.values() {
            keys.extend(witnesses.iter().cloned());
        }
        keys.extend(self.global_read_witnesses.iter().cloned());
        keys
    }

    /// Returns witness keys for all written pages (for SSI validation).
    #[must_use]
    pub fn write_witness_keys(&self) -> Vec<WitnessKey> {
        let capacity = self.global_write_witnesses.len()
            + self
                .write_index
                .values()
                .map(smallvec::SmallVec::len)
                .sum::<usize>();
        let mut keys = Vec::with_capacity(capacity);
        for witnesses in self.write_index.values() {
            keys.extend(witnesses.iter().cloned());
        }
        keys.extend(self.global_write_witnesses.iter().cloned());
        keys
    }

    #[must_use]
    pub fn has_global_read_witnesses(&self) -> bool {
        !self.global_read_witnesses.is_empty()
    }

    #[must_use]
    pub fn has_global_write_witnesses(&self) -> bool {
        !self.global_write_witnesses.is_empty()
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

    fn page_state(&self, page: PageNumber) -> Option<&PageTxnState> {
        self.page_states.get(&page)
    }

    fn ensure_page_state(&mut self, page: PageNumber) -> &mut PageTxnState {
        self.page_states.entry(page).or_default()
    }

    fn remove_page_state_if_empty(&mut self, page: PageNumber) {
        if self
            .page_states
            .get(&page)
            .is_some_and(PageTxnState::is_empty)
        {
            self.page_states.remove(&page);
        }
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
            None => {
                return !self.write_index.is_empty()
                    || self
                        .page_states
                        .values()
                        .any(PageTxnState::tracks_write_conflict)
                    || !self.global_write_witnesses.is_empty();
            }
        };

        if let Some(witnesses) = self.write_index.get(&page) {
            return witnesses
                .iter()
                .any(|w| crate::witness_plane::witness_keys_overlap(w, key));
        }

        self.tracks_write_conflict_page(page)
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
    /// Snapshot of per-page tracking state at savepoint creation time.
    page_states_snapshot: HashMap<PageNumber, SavepointPageState>,
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
    active: HashMap<u64, SharedConcurrentHandle>,
    /// Cached `snapshot.high` per registered session so GC-horizon queries do
    /// not need to lock and scan every handle.
    active_snapshot_highs: HashMap<u64, CommitSeq>,
    /// Refcounted index of registered session snapshot highs.
    gc_horizon_counts: BTreeMap<CommitSeq, usize>,
    /// Committed-reader history (RCRI-like) for SSI edge discovery.
    committed_readers: Vec<CommittedReaderInfo>,
    /// Page-local index into committed reader history (identity-hashed).
    committed_readers_by_page: PageMap<Vec<usize>>,
    /// Page witness-local index into committed reader history (identity-hashed).
    committed_readers_by_page_witness: PageMap<Vec<usize>>,
    /// Exact cell-local index into committed reader history.
    committed_readers_by_exact_cell: HashMap<(PageNumber, u64), Vec<usize>>,
    /// Committed reader entries that include at least one global witness.
    committed_readers_with_global_keys: Vec<usize>,
    /// Committed-writer history (commit-log-like) for SSI edge discovery.
    committed_writers: Vec<CommittedWriterInfo>,
    /// Page-local index into committed writer history (identity-hashed).
    committed_writers_by_page: PageMap<Vec<usize>>,
    /// Page witness-local index into committed writer history (identity-hashed).
    committed_writers_by_page_witness: PageMap<Vec<usize>>,
    /// Exact cell-local index into committed writer history.
    committed_writers_by_exact_cell: HashMap<(PageNumber, u64), Vec<usize>>,
    /// Committed writer entries that include at least one global witness.
    committed_writers_with_global_keys: Vec<usize>,
    /// Detached handles kept for reuse so hot autocommit loops do not hit the
    /// general allocator for a fresh handle every statement.
    recycled_handles: Vec<SharedConcurrentHandle>,
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
            active_snapshot_highs: HashMap::new(),
            gc_horizon_counts: BTreeMap::new(),
            committed_readers: Vec::new(),
            committed_readers_by_page: PageMap::default(),
            committed_readers_by_page_witness: PageMap::default(),
            committed_readers_by_exact_cell: HashMap::new(),
            committed_readers_with_global_keys: Vec::new(),
            committed_writers: Vec::new(),
            committed_writers_by_page: PageMap::default(),
            committed_writers_by_page_witness: PageMap::default(),
            committed_writers_by_exact_cell: HashMap::new(),
            committed_writers_with_global_keys: Vec::new(),
            recycled_handles: Vec::new(),
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

        let handle = if let Some(handle) = self.recycled_handles.pop() {
            handle.lock().reset_for_new_transaction(snapshot, txn_token);
            handle
        } else {
            Arc::new(Mutex::new(ConcurrentHandle::new(snapshot, txn_token)))
        };
        self.active.insert(session_id, handle);
        self.active_snapshot_highs.insert(session_id, snapshot.high);
        self.increment_gc_horizon_count(snapshot.high);
        Ok(session_id)
    }

    /// Returns an iterator over all active handles (for SSI validation).
    pub fn iter_active(&self) -> impl Iterator<Item = (u64, SharedConcurrentHandle)> + '_ {
        self.active
            .iter()
            .map(|(&id, handle)| (id, Arc::clone(handle)))
    }

    /// Build the immutable active-session image selected by Track E3's RCU
    /// publication mapping.
    ///
    /// This surfaces the active transaction snapshot table as sorted
    /// `(session_id, snapshot_high)` pairs plus the current GC horizon, leaving
    /// the per-session mutable leaves in the existing handle objects.
    #[must_use]
    pub fn active_snapshot_entries(&self) -> smallvec::SmallVec<[ActiveTxnSnapshotEntry; 8]> {
        let mut entries = self
            .active_snapshot_highs
            .iter()
            .map(|(&session_id, &snapshot_high)| ActiveTxnSnapshotEntry {
                session_id,
                snapshot_high,
            })
            .collect::<smallvec::SmallVec<[ActiveTxnSnapshotEntry; 8]>>();
        entries.sort_unstable_by_key(|entry| entry.session_id);
        entries
    }

    /// Publish the current active-session image through the E3.3 RCU/QSBR
    /// prototype without changing the current registry ownership model.
    pub fn publish_active_snapshot_prototype(
        &self,
        table: &RcuActiveTxnSnapshotTable,
        handle: &QsbrHandle<'_>,
    ) {
        let entries = self.active_snapshot_entries();
        table.publish(entries.as_slice(), self.gc_horizon(), handle);
    }

    /// Look up a shared concurrent handle by session id.
    #[must_use]
    pub fn handle(&self, session_id: u64) -> Option<SharedConcurrentHandle> {
        self.active.get(&session_id).map(Arc::clone)
    }

    /// Lock a concurrent handle by session id.
    #[must_use]
    pub fn get(&self, session_id: u64) -> Option<MutexGuard<'_, ConcurrentHandle>> {
        self.active.get(&session_id).map(|handle| handle.lock())
    }

    /// Lock a concurrent handle by session id for mutation.
    pub fn get_mut(&self, session_id: u64) -> Option<MutexGuard<'_, ConcurrentHandle>> {
        self.active.get(&session_id).map(|handle| handle.lock())
    }

    /// Remove a session (after commit or abort).
    pub fn remove(&mut self, session_id: u64) -> Option<SharedConcurrentHandle> {
        let handle = self.active.remove(&session_id)?;
        if let Some(snapshot_high) = self.active_snapshot_highs.remove(&session_id) {
            self.decrement_gc_horizon_count(snapshot_high);
        } else {
            debug_assert!(
                false,
                "active concurrent session {session_id} missing cached snapshot.high"
            );
        }
        Some(handle)
    }

    /// Remove a session and recycle its handle when the caller no longer
    /// needs to inspect it.
    pub fn remove_and_recycle(&mut self, session_id: u64) -> bool {
        self.remove(session_id)
            .map(|handle| self.recycle_handle(handle))
            .is_some()
    }

    /// Return an idle handle to the local recycle pool when no other strong
    /// references remain.
    pub fn recycle_handle(&mut self, handle: SharedConcurrentHandle) {
        const MAX_RECYCLED_HANDLES: usize = 8;
        if self.recycled_handles.len() >= MAX_RECYCLED_HANDLES || Arc::strong_count(&handle) != 1 {
            return;
        }
        self.recycled_handles.push(handle);
    }

    fn can_use_uncontended_prepare_fast_path(&self, session_id: u64, begin_seq: CommitSeq) -> bool {
        self.active.len() == 1
            && self.active.contains_key(&session_id)
            && self
                .committed_readers
                .last()
                .is_none_or(|reader| reader.commit_seq <= begin_seq)
            && self
                .committed_writers
                .last()
                .is_none_or(|writer| writer.commit_seq <= begin_seq)
    }

    fn can_use_uncontended_finalize_fast_path(
        &self,
        session_id: u64,
        begin_seq: CommitSeq,
    ) -> bool {
        (self.active.is_empty()
            || (self.active.len() == 1 && self.active.contains_key(&session_id)))
            && self
                .committed_readers
                .last()
                .is_none_or(|reader| reader.commit_seq <= begin_seq)
            && self
                .committed_writers
                .last()
                .is_none_or(|writer| writer.commit_seq <= begin_seq)
    }

    /// Prune committed SSI history that cannot overlap any active transaction.
    fn prune_committed_conflict_history(&mut self) {
        let Some(min_active_begin) = self.history_retention_horizon() else {
            self.committed_readers.clear();
            self.committed_readers_by_page.clear();
            self.committed_readers_by_page_witness.clear();
            self.committed_readers_by_exact_cell.clear();
            self.committed_readers_with_global_keys.clear();
            self.committed_writers.clear();
            self.committed_writers_by_page.clear();
            self.committed_writers_by_page_witness.clear();
            self.committed_writers_by_exact_cell.clear();
            self.committed_writers_with_global_keys.clear();
            return;
        };
        self.committed_readers
            .retain(|reader| reader.commit_seq > min_active_begin);
        self.committed_writers
            .retain(|writer| writer.commit_seq > min_active_begin);

        // C7 (bd-l9k8e.7): Safety bound for SSI history memory.
        // Reduced from 16384 to 4096 for tighter memory control.
        // Each entry is ~128 bytes, so 4096 entries = ~512KB max SSI history.
        const MAX_HISTORY_ENTRIES: usize = 4096;
        while self.committed_readers.len() + self.committed_writers.len() > MAX_HISTORY_ENTRIES {
            // Find the oldest active transaction and mark it for abort to unpin the horizon.
            let mut oldest_id = None;
            let mut oldest_seq = CommitSeq::new(u64::MAX);
            for (&id, handle) in &self.active {
                let handle = handle.lock();
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
                    let handle = handle.lock();
                    handle.set_marked_for_abort(true);
                }

                // Recompute the retained-history horizon and prune again now
                // that the oldest transaction is doomed to abort.
                let new_horizon = self
                    .history_retention_horizon()
                    .unwrap_or(CommitSeq::new(u64::MAX));
                self.committed_readers
                    .retain(|reader| reader.commit_seq > new_horizon);
                self.committed_writers
                    .retain(|writer| writer.commit_seq > new_horizon);
            } else {
                // If we couldn't find an active transaction to abort (should be impossible if
                // history is bounded by active transactions), just clear everything.
                tracing::warn!("prune_committed_conflict_history: forced to clear SSI history");
                self.committed_readers.clear();
                self.committed_readers_by_page.clear();
                self.committed_readers_by_page_witness.clear();
                self.committed_readers_by_exact_cell.clear();
                self.committed_readers_with_global_keys.clear();
                self.committed_writers.clear();
                self.committed_writers_by_page.clear();
                self.committed_writers_by_page_witness.clear();
                self.committed_writers_by_exact_cell.clear();
                self.committed_writers_with_global_keys.clear();
                break;
            }
        }

        // C7 (bd-l9k8e.7): Log SSI history size for observability.
        let reader_count = self.committed_readers.len();
        let writer_count = self.committed_writers.len();
        if reader_count + writer_count > 0 {
            tracing::debug!(
                reader_entries = reader_count,
                writer_entries = writer_count,
                total = reader_count + writer_count,
                active_txns = self.active.len(),
                "ssi_history_status"
            );
        }

        self.rebuild_committed_history_indexes();
    }

    fn rebuild_committed_history_indexes(&mut self) {
        self.committed_readers_by_page.clear();
        self.committed_readers_by_page_witness.clear();
        self.committed_readers_by_exact_cell.clear();
        self.committed_readers_with_global_keys.clear();
        for (idx, reader) in self.committed_readers.iter().enumerate() {
            let summary = summarize_witness_keys(&reader.keys);
            if summary.has_global_keys {
                self.committed_readers_with_global_keys.push(idx);
            }
            for page in summary.pages {
                self.committed_readers_by_page
                    .entry(page)
                    .or_default()
                    .push(idx);
            }
            for page in summary.page_witness_pages {
                self.committed_readers_by_page_witness
                    .entry(page)
                    .or_default()
                    .push(idx);
            }
            for cell in summary.cell_witnesses {
                self.committed_readers_by_exact_cell
                    .entry(cell.exact_key())
                    .or_default()
                    .push(idx);
            }
        }

        self.committed_writers_by_page.clear();
        self.committed_writers_by_page_witness.clear();
        self.committed_writers_by_exact_cell.clear();
        self.committed_writers_with_global_keys.clear();
        for (idx, writer) in self.committed_writers.iter().enumerate() {
            let summary = summarize_witness_keys(&writer.keys);
            if summary.has_global_keys {
                self.committed_writers_with_global_keys.push(idx);
            }
            for page in summary.pages {
                self.committed_writers_by_page
                    .entry(page)
                    .or_default()
                    .push(idx);
            }
            for page in summary.page_witness_pages {
                self.committed_writers_by_page_witness
                    .entry(page)
                    .or_default()
                    .push(idx);
            }
            for cell in summary.cell_witnesses {
                self.committed_writers_by_exact_cell
                    .entry(cell.exact_key())
                    .or_default()
                    .push(idx);
            }
        }
    }

    fn increment_gc_horizon_count(&mut self, snapshot_high: CommitSeq) {
        *self.gc_horizon_counts.entry(snapshot_high).or_default() += 1;
    }

    fn decrement_gc_horizon_count(&mut self, snapshot_high: CommitSeq) {
        let Some(count) = self.gc_horizon_counts.get_mut(&snapshot_high) else {
            debug_assert!(
                false,
                "missing gc-horizon count for snapshot.high={}",
                snapshot_high.get()
            );
            return;
        };
        let should_remove = if *count == 1 {
            true
        } else {
            *count -= 1;
            false
        };
        if should_remove {
            self.gc_horizon_counts.remove(&snapshot_high);
        }
    }

    fn index_committed_reader(&mut self, entry_idx: usize) {
        let Some(reader) = self.committed_readers.get(entry_idx) else {
            return;
        };
        let summary = summarize_witness_keys(&reader.keys);
        if summary.has_global_keys {
            self.committed_readers_with_global_keys.push(entry_idx);
        }
        for page in summary.pages {
            self.committed_readers_by_page
                .entry(page)
                .or_default()
                .push(entry_idx);
        }
        for page in summary.page_witness_pages {
            self.committed_readers_by_page_witness
                .entry(page)
                .or_default()
                .push(entry_idx);
        }
        for cell in summary.cell_witnesses {
            self.committed_readers_by_exact_cell
                .entry(cell.exact_key())
                .or_default()
                .push(entry_idx);
        }
    }

    fn index_committed_writer(&mut self, entry_idx: usize) {
        let Some(writer) = self.committed_writers.get(entry_idx) else {
            return;
        };
        let summary = summarize_witness_keys(&writer.keys);
        if summary.has_global_keys {
            self.committed_writers_with_global_keys.push(entry_idx);
        }
        for page in summary.pages {
            self.committed_writers_by_page
                .entry(page)
                .or_default()
                .push(entry_idx);
        }
        for page in summary.page_witness_pages {
            self.committed_writers_by_page_witness
                .entry(page)
                .or_default()
                .push(entry_idx);
        }
        for cell in summary.cell_witnesses {
            self.committed_writers_by_exact_cell
                .entry(cell.exact_key())
                .or_default()
                .push(entry_idx);
        }
    }

    fn committed_reader_candidates(
        &self,
        committing_txn: TxnToken,
        committing_begin_seq: CommitSeq,
        committing_commit_seq: CommitSeq,
        write_key_summary: &WitnessKeySummary,
    ) -> Vec<CommittedReaderInfo> {
        if self.committed_readers.is_empty() {
            return Vec::new();
        }
        let candidate_indexes = if write_key_summary.has_global_keys {
            (0..self.committed_readers.len()).collect::<Vec<_>>()
        } else {
            collect_precise_candidates(
                &self.committed_readers_with_global_keys,
                &self.committed_readers_by_page,
                &self.committed_readers_by_page_witness,
                &self.committed_readers_by_exact_cell,
                write_key_summary,
            )
        };
        let committing_begin = committing_begin_seq.get();
        let committing_end = committing_commit_seq.get();
        candidate_indexes
            .into_iter()
            .filter_map(|idx| self.committed_readers.get(idx))
            .filter(|reader| {
                !same_txn_identity(reader.token, committing_txn)
                    && committing_begin < reader.commit_seq.get()
                    && reader.begin_seq.get() < committing_end
            })
            .cloned()
            .collect()
    }

    fn committed_writer_candidates(
        &self,
        committing_txn: TxnToken,
        committing_begin_seq: CommitSeq,
        _committing_commit_seq: CommitSeq,
        read_key_summary: &WitnessKeySummary,
    ) -> Vec<CommittedWriterInfo> {
        if self.committed_writers.is_empty() {
            return Vec::new();
        }
        let candidate_indexes = if read_key_summary.has_global_keys {
            (0..self.committed_writers.len()).collect::<Vec<_>>()
        } else {
            collect_precise_candidates(
                &self.committed_writers_with_global_keys,
                &self.committed_writers_by_page,
                &self.committed_writers_by_page_witness,
                &self.committed_writers_by_exact_cell,
                read_key_summary,
            )
        };
        let committing_begin = committing_begin_seq.get();
        candidate_indexes
            .into_iter()
            .filter_map(|idx| self.committed_writers.get(idx))
            .filter(|writer| {
                !same_txn_identity(writer.token, committing_txn)
                    && committing_begin < writer.commit_seq.get()
            })
            .cloned()
            .collect()
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
        self.gc_horizon_counts.keys().next().copied()
    }

    /// Compute the retention horizon for committed SSI history.
    ///
    /// Unlike page-version GC, committed conflict history only needs to be
    /// retained for active transactions that may still commit. Transactions
    /// already marked for abort still pin MVCC visibility, but they are
    /// excluded here so history can be bounded once they are guaranteed to
    /// fail at commit time.
    #[must_use]
    fn history_retention_horizon(&self) -> Option<CommitSeq> {
        self.active
            .values()
            .filter_map(|handle| {
                let handle = handle.lock();
                (handle.is_active() && !handle.is_marked_for_abort())
                    .then_some(handle.snapshot.high)
            })
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
pub fn concurrent_prepare_write_page(
    handle: &mut ConcurrentHandle,
    lock_table: &InProcessPageLockTable,
    session_id: u64,
    page: PageNumber,
) -> Result<(), MvccError> {
    if !handle.is_active() {
        return Err(MvccError::InvalidState);
    }
    let (holds_lock, was_freed, was_conflict_only, has_staged_write) = handle
        .page_state(page)
        .map_or((false, false, false, false), |state| {
            (
                state.held_lock,
                state.is_freed,
                state.is_conflict_only,
                state.has_staged_write,
            )
        });

    if has_staged_write {
        debug_assert!(holds_lock, "staged write pages must retain their page lock");
        debug_assert!(!was_freed, "staged write pages cannot also be marked freed");
        debug_assert!(
            !was_conflict_only,
            "staged write pages cannot also be conflict-only"
        );
        return Ok(());
    }

    if holds_lock && (was_freed || was_conflict_only) {
        let state = handle.ensure_page_state(page);
        state.is_freed = false;
        state.is_conflict_only = false;
        handle.remove_page_state_if_empty(page);
        return Ok(());
    }

    let already_tracked = handle.tracks_write_conflict_page(page);
    if !holds_lock {
        let txn_id = TxnId::new(session_id).ok_or(MvccError::InvalidState)?;
        if lock_table.try_acquire(page, txn_id).is_err() {
            handle.remove_page_state_if_empty(page);
            return Err(MvccError::Busy);
        }
        handle.ensure_page_state(page).held_lock = true;
    }
    let state = handle.ensure_page_state(page);
    state.is_freed = false;
    state.is_conflict_only = false;
    if !already_tracked {
        handle.record_write_witness(fsqlite_types::WitnessKey::Page(page));
    }
    handle.remove_page_state_if_empty(page);
    Ok(())
}

fn concurrent_stage_prepared_write_state(
    handle: &mut ConcurrentHandle,
    page: PageNumber,
    data: Option<PageData>,
) -> Result<(), MvccError> {
    if !handle.is_active() {
        return Err(MvccError::InvalidState);
    }

    debug_assert!(
        handle.holds_page_lock(page),
        "prepared page writes must retain their page lock before staging bytes"
    );
    debug_assert!(
        !handle
            .page_state(page)
            .is_some_and(|state| state.is_conflict_only),
        "prepared write staging must clear conflict-only tracking first"
    );
    debug_assert!(
        !handle.page_state(page).is_some_and(|state| state.is_freed),
        "prepared write staging must clear freed-page tracking first"
    );

    let state = handle.ensure_page_state(page);
    state.staged_data = data;
    state.has_staged_write = true;
    Ok(())
}

/// Stage payload bytes for a page that is already prepared for writing.
pub fn concurrent_stage_prepared_write_page(
    handle: &mut ConcurrentHandle,
    page: PageNumber,
    data: PageData,
) -> Result<(), MvccError> {
    concurrent_stage_prepared_write_state(handle, page, Some(data))
}

/// Mark a page as staged for conflict/read-status tracking without copying its bytes.
///
/// This is for upper layers that keep the authoritative same-transaction page
/// image in the pager write-set. The page remains in the MVCC write-conflict
/// surface, but `concurrent_read_page` intentionally returns `None` because
/// MVCC does not own a payload snapshot for marker-only writes.
pub fn concurrent_stage_prepared_write_marker(
    handle: &mut ConcurrentHandle,
    page: PageNumber,
) -> Result<(), MvccError> {
    concurrent_stage_prepared_write_state(handle, page, None)
}

/// Write a page within a concurrent transaction.
///
/// Acquires a page-level lock if not already held, then records the page
/// data in the write set. Returns an error if the lock is held by another
/// concurrent transaction.
pub fn concurrent_write_page(
    handle: &mut ConcurrentHandle,
    lock_table: &InProcessPageLockTable,
    session_id: u64,
    page: PageNumber,
    data: PageData,
) -> Result<(), MvccError> {
    concurrent_prepare_write_page(handle, lock_table, session_id, page)?;
    concurrent_stage_prepared_write_page(handle, page, data)
}

/// Capture the local concurrent tracking state for a page.
#[must_use]
pub fn concurrent_page_state(handle: &ConcurrentHandle, page: PageNumber) -> ConcurrentPageState {
    let state = handle.page_state(page);
    ConcurrentPageState {
        page,
        staged_data: state.and_then(|state| state.staged_data.clone()),
        had_staged_write: state.is_some_and(|state| state.has_staged_write),
        was_freed: state.is_some_and(|state| state.is_freed),
        was_conflict_only: state.is_some_and(|state| state.is_conflict_only),
        held_lock: state.is_some_and(|state| state.held_lock),
    }
}

/// Return whether a page is tracked only as a synthetic conflict surface.
///
/// This is the predicate-only companion to [`concurrent_page_state`]. Hot
/// cleanup paths use it when they only need the boolean answer, avoiding a
/// full `ConcurrentPageState` construction and possible staged-page clone.
#[must_use]
pub fn concurrent_page_is_synthetic_conflict_only(
    handle: &ConcurrentHandle,
    page: PageNumber,
) -> bool {
    handle
        .page_state(page)
        .is_some_and(|state| state.is_conflict_only && !state.has_staged_write && !state.is_freed)
}

/// Restore a page's local concurrent tracking state after a failed pager write.
pub fn concurrent_restore_page_state(
    handle: &mut ConcurrentHandle,
    lock_table: &InProcessPageLockTable,
    session_id: u64,
    state: &ConcurrentPageState,
) -> Result<(), MvccError> {
    if !handle.is_active() {
        return Err(MvccError::InvalidState);
    }
    let txn_id = TxnId::new(session_id).ok_or(MvccError::InvalidState)?;

    {
        let restored = handle.ensure_page_state(state.page);
        restored.staged_data.clone_from(&state.staged_data);
        restored.has_staged_write = state.had_staged_write;
        restored.is_freed = state.was_freed;
        restored.is_conflict_only = state.was_conflict_only;
        restored.held_lock = state.held_lock;
    }

    if !state.held_lock {
        lock_table.release(state.page, txn_id);
    }
    handle.remove_page_state_if_empty(state.page);
    Ok(())
}

/// Clear all local concurrent tracking for a page.
///
/// Upper layers use this when a synthetic conflict surface is no longer part
/// of the transaction's pending commit state. SSI witnesses are intentionally
/// retained as a safe overapproximation.
pub fn concurrent_clear_page_state(
    handle: &mut ConcurrentHandle,
    lock_table: &InProcessPageLockTable,
    session_id: u64,
    page: PageNumber,
) -> Result<(), MvccError> {
    if !handle.is_active() {
        return Err(MvccError::InvalidState);
    }
    let txn_id = TxnId::new(session_id).ok_or(MvccError::InvalidState)?;

    if let Some(state) = handle.page_state(page)
        && state.held_lock
    {
        lock_table.release(page, txn_id);
    }

    handle.page_states.remove(&page);
    Ok(())
}

/// Track a page in the write-conflict surface without staging payload bytes.
pub fn concurrent_track_write_conflict_page(
    handle: &mut ConcurrentHandle,
    lock_table: &InProcessPageLockTable,
    session_id: u64,
    page: PageNumber,
) -> Result<(), MvccError> {
    if !handle.is_active() {
        return Err(MvccError::InvalidState);
    }
    let already_tracked = handle.tracks_write_conflict_page(page);
    let holds_lock = handle.holds_page_lock(page);
    if already_tracked && holds_lock {
        debug_assert!(
            holds_lock,
            "tracked conflict pages must retain their page lock"
        );
        return Ok(());
    }
    if holds_lock {
        let state = handle.ensure_page_state(page);
        if !state.has_staged_write && !state.is_freed {
            state.is_conflict_only = true;
        }
        if !already_tracked {
            handle.record_write_witness(fsqlite_types::WitnessKey::Page(page));
        }
        return Ok(());
    }

    let txn_id = TxnId::new(session_id).ok_or(MvccError::InvalidState)?;
    if lock_table.try_acquire(page, txn_id).is_err() {
        handle.remove_page_state_if_empty(page);
        return Err(MvccError::Busy);
    }
    let state = handle.ensure_page_state(page);
    state.held_lock = true;
    if !state.has_staged_write && !state.is_freed {
        state.is_conflict_only = true;
    }
    if !already_tracked {
        handle.record_write_witness(fsqlite_types::WitnessKey::Page(page));
    }
    Ok(())
}

/// Record that a page was freed within a concurrent transaction.
///
/// The page remains part of the write-conflict surface even though it no
/// longer has staged payload bytes.
pub fn concurrent_free_page(
    handle: &mut ConcurrentHandle,
    lock_table: &InProcessPageLockTable,
    session_id: u64,
    page: PageNumber,
) -> Result<(), MvccError> {
    if !handle.is_active() {
        return Err(MvccError::InvalidState);
    }
    if handle.is_page_freed(page) {
        debug_assert!(
            handle.holds_page_lock(page),
            "freed pages must retain their page lock"
        );
        debug_assert!(
            handle
                .page_state(page)
                .is_none_or(|state| !state.has_staged_write && state.staged_data.is_none()),
            "freed pages cannot retain staged write state"
        );
        return Ok(());
    }
    if handle.holds_page_lock(page) {
        let already_tracked = handle.tracks_write_conflict_page(page);
        let state = handle.ensure_page_state(page);
        state.staged_data = None;
        state.has_staged_write = false;
        state.is_conflict_only = false;
        state.is_freed = true;
        if !already_tracked {
            handle.record_write_witness(fsqlite_types::WitnessKey::Page(page));
        }
        return Ok(());
    }

    let txn_id = TxnId::new(session_id).ok_or(MvccError::InvalidState)?;
    let already_tracked = handle.tracks_write_conflict_page(page);
    if lock_table.try_acquire(page, txn_id).is_err() {
        handle.remove_page_state_if_empty(page);
        return Err(MvccError::Busy);
    }
    let state = handle.ensure_page_state(page);
    state.held_lock = true;
    state.staged_data = None;
    state.has_staged_write = false;
    state.is_conflict_only = false;
    state.is_freed = true;
    if !already_tracked {
        handle.record_write_witness(fsqlite_types::WitnessKey::Page(page));
    }
    Ok(())
}

/// Read a page within a concurrent transaction.
///
/// Returns locally staged page bytes when MVCC owns a payload snapshot.
///
/// Marker-only staged writes still participate in write-conflict tracking and
/// [`concurrent_page_read_status`], but return `None` here because their
/// authoritative read-your-writes image lives in the caller's pager write-set.
#[must_use]
pub fn concurrent_read_page(handle: &ConcurrentHandle, page: PageNumber) -> Option<&PageData> {
    handle
        .page_state(page)
        .and_then(|state| state.staged_data.as_ref())
}

/// Whether the page has been freed by this concurrent transaction.
#[must_use]
pub fn concurrent_page_is_freed(handle: &ConcurrentHandle, page: PageNumber) -> bool {
    handle.is_page_freed(page)
}

/// Compound lookup: returns `(is_freed, staged_data)` from a single
/// `page_states` HashMap probe.
///
/// The VDBE hot read path calls `concurrent_page_is_freed` followed by
/// `concurrent_read_page` on every page read, producing two independent
/// lookups on the same key. This helper collapses them into one so read
/// workloads stop paying the double-lookup cost per page.
///
/// `staged_data` is cloned (cheap after first clone because `PageData`
/// lazily promotes to `Arc<[u8]>` on the first clone).
#[must_use]
pub fn concurrent_page_read_state(
    handle: &ConcurrentHandle,
    page: PageNumber,
) -> (bool, Option<PageData>) {
    match handle.page_state(page) {
        Some(state) => (state.is_freed, state.staged_data.clone()),
        None => (false, None),
    }
}

/// Compound status lookup for callers that need write-set visibility without
/// cloning staged page bytes.
#[must_use]
pub fn concurrent_page_read_status(handle: &ConcurrentHandle, page: PageNumber) -> (bool, bool) {
    match handle.page_state(page) {
        Some(state) => (state.is_freed, state.has_staged_write),
        None => (false, false),
    }
}

/// Fast predicate used by read callers to skip compound page-state lookups
/// on read-only transactions (where `page_states` is empty).
#[must_use]
pub fn concurrent_has_page_state(handle: &ConcurrentHandle) -> bool {
    !handle.page_states.is_empty()
}

/// Mark a page as metadata-exempt for MVCC conflict tracking (E3 — bd-wwqen Track E).
///
/// Metadata pages (e.g., page 1 with freelist metadata) are still written but
/// don't participate in first-committer-wins conflict detection. This eliminates
/// false conflicts when disjoint inserts both update structural metadata on the
/// same page (e.g., both bump the freelist pointer on page 1).
///
/// # Safety Guarantee
///
/// The page is still locked and written — only conflict *detection* is skipped.
/// The caller must ensure that concurrent modifications to metadata pages are
/// semantically safe (e.g., freelist operations are commutative).
///
/// # Use Cases
///
/// - Page 1 freelist metadata updates during INSERT
/// - Private page allocation counters that reconcile at commit
/// - Structural B-tree metadata that can be merged safely
pub fn concurrent_mark_metadata_exempt(handle: &mut ConcurrentHandle, page: PageNumber) {
    if let Some(state) = handle.page_states.get_mut(&page) {
        state.metadata_exempt = true;
    }
}

/// Check if a page is marked as metadata-exempt.
#[must_use]
pub fn concurrent_is_metadata_exempt(handle: &ConcurrentHandle, page: PageNumber) -> bool {
    handle
        .page_state(page)
        .is_some_and(|state| state.metadata_exempt)
}

/// Write a page and mark it as metadata-exempt for MVCC conflict tracking.
///
/// This is a convenience function combining `concurrent_write_page` with
/// `concurrent_mark_metadata_exempt`. Use this for freelist/structural metadata
/// pages that should not trigger FCW conflicts with disjoint operations.
pub fn concurrent_write_metadata_page(
    handle: &mut ConcurrentHandle,
    lock_table: &InProcessPageLockTable,
    session_id: u64,
    page: PageNumber,
    data: PageData,
) -> Result<(), MvccError> {
    concurrent_write_page(handle, lock_table, session_id, page, data)?;
    concurrent_mark_metadata_exempt(handle, page);
    Ok(())
}

/// Record a metadata page read without SSI tracking (E4 — bd-wwqen Track E).
///
/// Page 1 (database header with freelist metadata) is read by every transaction
/// that allocates pages. Tracking these reads in the SSI SIREAD set causes
/// massive false rw-antidependency conflicts between disjoint inserts.
///
/// By calling this function instead of the normal read tracking, the read
/// is not registered in the SIREAD set and won't trigger conflicts with
/// concurrent writers to page 1.
///
/// # Use Cases
///
/// - Reading freelist metadata from page 1 during page allocation
/// - Reading structural B-tree metadata that doesn't affect user data semantics
///
/// # Safety
///
/// Safe because freelist reads are commutative: multiple transactions can
/// read the freelist, allocate different pages, and commit without conflict.
pub fn concurrent_record_metadata_read(handle: &mut ConcurrentHandle, page: PageNumber) {
    handle.record_metadata_read(page);
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
    let mut conflicting_pages = smallvec::SmallVec::<[PageNumber; 8]>::new();
    let mut max_conflicting_seq = CommitSeq::ZERO;

    for page in handle.tracked_write_conflict_pages_iter() {
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
            write_set_size = handle.write_set_len(),
            snapshot_seq = snapshot_seq.get(),
            "fcw_validation: clean (no base drift)"
        );
        FcwResult::Clean
    } else {
        if fsqlite_btree::conflict_topology_policy_enabled() {
            let _updated = fsqlite_btree::record_conflict_topology_heat_batch(
                conflicting_pages.iter().copied(),
                1,
                2,
            );
        }
        tracing::warn!(
            conflicting_page_count = conflicting_pages.len(),
            max_conflicting_seq = max_conflicting_seq.get(),
            snapshot_seq = snapshot_seq.get(),
            "fcw_validation: base drift detected"
        );
        FcwResult::Conflict {
            conflicting_pages: conflicting_pages.into_vec(),
            conflicting_commit_seq: max_conflicting_seq,
        }
    }
}

fn release_tracked_page_locks(
    lock_table: &InProcessPageLockTable,
    handle: &ConcurrentHandle,
    txn_id: TxnId,
) {
    lock_table.release_set(handle.held_lock_pages_iter(), txn_id);
}

fn finalize_prepared_session_state(
    registry: &ConcurrentRegistry,
    session_id: u64,
    has_in_rw: bool,
    has_out_rw: bool,
    finalize_path: &'static str,
) {
    if let Some(mut handle) = registry.get_mut(session_id) {
        if handle.is_active() {
            handle.has_in_rw.set(has_in_rw);
            handle.has_out_rw.set(has_out_rw);
            handle.mark_committed();
        } else {
            tracing::warn!(
                session_id,
                finalize_path,
                "finalize_prepared_concurrent_commit_with_ssi: session inactive during finalize; applying commit-index/lock-table side effects"
            );
        }
    } else {
        tracing::warn!(
            session_id,
            finalize_path,
            "finalize_prepared_concurrent_commit_with_ssi: session missing during finalize; applying commit-index/lock-table side effects"
        );
    }
}

fn merge_unique_incoming_edges(
    incoming_edges: &mut Vec<DiscoveredEdge>,
    seen_sources: &mut HashSet<TxnToken>,
    discovered_edges: impl IntoIterator<Item = DiscoveredEdge>,
) {
    for edge in discovered_edges {
        if seen_sources.insert(edge.from) {
            incoming_edges.push(edge);
        }
    }
}

fn merge_unique_outgoing_edges(
    outgoing_edges: &mut Vec<DiscoveredEdge>,
    seen_targets: &mut HashSet<TxnToken>,
    discovered_edges: impl IntoIterator<Item = DiscoveredEdge>,
) {
    for edge in discovered_edges {
        if seen_targets.insert(edge.to) {
            outgoing_edges.push(edge);
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
/// conflict history and edge propagation. `planned_commit_seq` is an
/// optimistic planning frontier, not necessarily the final published commit
/// sequence if another commit slips in between prepare and finalize.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone)]
pub struct PreparedConcurrentCommit {
    session_id: u64,
    planned_commit_seq: CommitSeq,
    txn_token: TxnToken,
    begin_seq: CommitSeq,
    read_keys: Vec<WitnessKey>,
    read_key_summary: WitnessKeySummary,
    write_keys: Vec<WitnessKey>,
    write_key_summary: WitnessKeySummary,
    write_set_pages: Vec<PageNumber>,
    held_lock_pages: Vec<PageNumber>,
    has_in_rw: bool,
    has_out_rw: bool,
    incoming_edges: Vec<DiscoveredEdge>,
    outgoing_edges: Vec<DiscoveredEdge>,
    dro_t3_decision: Option<crate::ssi_abort_policy::DroHotPathDecision>,
    used_uncontended_prepare_fast_path: bool,
    used_candidate_free_prepare_fast_path: bool,
}

impl PreparedConcurrentCommit {
    #[must_use]
    pub const fn session_id(&self) -> u64 {
        self.session_id
    }

    #[must_use]
    pub const fn planned_commit_seq(&self) -> CommitSeq {
        self.planned_commit_seq
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
    pub const fn used_uncontended_prepare_fast_path(&self) -> bool {
        self.used_uncontended_prepare_fast_path
    }

    #[must_use]
    pub const fn used_candidate_free_prepare_fast_path(&self) -> bool {
        self.used_candidate_free_prepare_fast_path
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
    pub fn read_pages(&self) -> Vec<PageNumber> {
        self.read_key_summary.pages.clone()
    }

    #[must_use]
    pub fn write_set_pages(&self) -> &[PageNumber] {
        &self.write_set_pages
    }

    #[must_use]
    pub fn held_lock_pages(&self) -> &[PageNumber] {
        &self.held_lock_pages
    }

    #[must_use]
    pub fn conflicting_txns(&self) -> Vec<TxnToken> {
        let mut txns = self
            .incoming_edges
            .iter()
            .map(|edge| edge.from)
            .chain(self.outgoing_edges.iter().map(|edge| edge.to))
            .collect::<Vec<_>>();
        txns.sort_by(|left, right| {
            left.id
                .get()
                .cmp(&right.id.get())
                .then_with(|| left.epoch.get().cmp(&right.epoch.get()))
        });
        txns.dedup();
        txns
    }

    #[must_use]
    pub fn conflict_pages(&self) -> Vec<PageNumber> {
        let mut pages = self
            .incoming_edges
            .iter()
            .chain(self.outgoing_edges.iter())
            .filter_map(|edge| witness_key_page(&edge.overlap_key))
            .collect::<Vec<_>>();
        if pages.is_empty() {
            pages.extend(self.write_set_pages.iter().copied());
        }
        pages.sort_unstable();
        pages.dedup();
        pages
    }

    #[must_use]
    pub const fn dro_t3_decision(&self) -> Option<crate::ssi_abort_policy::DroHotPathDecision> {
        self.dro_t3_decision
    }
}

/// Snapshot of one active transaction used during SSI edge discovery.
#[derive(Debug, Clone, Copy, Default)]
struct HandleViewFlags(u8);

impl HandleViewFlags {
    const ACTIVE: u8 = 1 << 0;
    const GLOBAL_READ_WITNESSES: u8 = 1 << 1;
    const GLOBAL_WRITE_WITNESSES: u8 = 1 << 2;

    fn from_handle(handle: &ConcurrentHandle) -> Self {
        let mut bits = 0;
        if handle.is_active() {
            bits |= Self::ACTIVE;
        }
        if handle.has_global_read_witnesses() {
            bits |= Self::GLOBAL_READ_WITNESSES;
        }
        if handle.has_global_write_witnesses() {
            bits |= Self::GLOBAL_WRITE_WITNESSES;
        }
        Self(bits)
    }

    const fn contains(self, flag: u8) -> bool {
        self.0 & flag != 0
    }

    const fn is_active(self) -> bool {
        self.contains(Self::ACTIVE)
    }

    const fn has_global_read_witnesses(self) -> bool {
        self.contains(Self::GLOBAL_READ_WITNESSES)
    }

    const fn has_global_write_witnesses(self) -> bool {
        self.contains(Self::GLOBAL_WRITE_WITNESSES)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct ExactCellWitness {
    btree_root: PageNumber,
    leaf_page: PageNumber,
    tag: u64,
}

impl ExactCellWitness {
    const fn new(btree_root: PageNumber, leaf_page: PageNumber, tag: u64) -> Self {
        Self {
            btree_root,
            leaf_page,
            tag,
        }
    }

    const fn exact_key(self) -> (PageNumber, u64) {
        (self.btree_root, self.tag)
    }
}

struct HandleView {
    token: TxnToken,
    begin_seq: CommitSeq,
    flags: HandleViewFlags,
    // Sorted, dedup'd page sets queried via `binary_search`. Storing these
    // as `HashSet<PageNumber>` (default `RandomState` hasher) cost
    // SipHash13 on every insert + every overlap probe — `HandleView::new`
    // showed 0.10% MT8 self-time as the residual SSI hot path, dominated
    // by the six-HashSet build. The summaries handed to us by
    // `summarize_witness_keys` are already sorted+dedup'd, so the page
    // fields move in directly without a re-sort.
    read_pages: Vec<PageNumber>,
    read_page_witness_pages: Vec<PageNumber>,
    read_exact_cells: Vec<(PageNumber, u64)>,
    write_pages: Vec<PageNumber>,
    write_page_witness_pages: Vec<PageNumber>,
    write_exact_cells: Vec<(PageNumber, u64)>,
    has_in_rw: Cell<bool>,
    has_out_rw: Cell<bool>,
}

impl HandleView {
    fn new(handle: &ConcurrentHandle) -> Self {
        let read_summary = summarize_witness_keys(&handle.read_witness_keys());
        let write_summary = summarize_witness_keys(&handle.write_witness_keys());

        // Union tracked-write-conflict pages with witness pages. The tracked
        // iter yields HashMap-iteration order; the witness side is already
        // sorted. Sort+dedup the combined Vec once instead of paying
        // per-insert hashing.
        let mut write_pages: Vec<PageNumber> = handle.tracked_write_conflict_pages_iter().collect();
        write_pages.extend_from_slice(&write_summary.pages);
        write_pages.sort_unstable();
        write_pages.dedup();

        // `summary.cell_witnesses` is sorted by full (root, leaf, tag);
        // projecting to (root, tag) drops `leaf_page`, so the result may
        // hold dup (root, tag) pairs across distinct leaves — sort+dedup.
        let mut read_exact_cells: Vec<(PageNumber, u64)> = read_summary
            .cell_witnesses
            .iter()
            .map(|cell| cell.exact_key())
            .collect();
        read_exact_cells.sort_unstable();
        read_exact_cells.dedup();

        let mut write_exact_cells: Vec<(PageNumber, u64)> = write_summary
            .cell_witnesses
            .iter()
            .map(|cell| cell.exact_key())
            .collect();
        write_exact_cells.sort_unstable();
        write_exact_cells.dedup();

        Self {
            token: handle.token(),
            begin_seq: handle.begin_seq(),
            flags: HandleViewFlags::from_handle(handle),
            read_pages: read_summary.pages,
            read_page_witness_pages: read_summary.page_witness_pages,
            read_exact_cells,
            write_pages,
            write_page_witness_pages: write_summary.page_witness_pages,
            write_exact_cells,
            has_in_rw: Cell::new(handle.has_in_rw()),
            has_out_rw: Cell::new(handle.has_out_rw()),
        }
    }

    const fn is_currently_active(&self) -> bool {
        self.flags.is_active()
    }

    fn has_read_witnesses(&self) -> bool {
        !self.read_pages.is_empty() || self.has_global_read_witnesses()
    }

    fn has_write_witnesses(&self) -> bool {
        !self.write_pages.is_empty() || self.has_global_write_witnesses()
    }

    const fn has_global_read_witnesses(&self) -> bool {
        self.flags.has_global_read_witnesses()
    }

    const fn has_global_write_witnesses(&self) -> bool {
        self.flags.has_global_write_witnesses()
    }
}

#[derive(Debug, Clone, Default)]
struct WitnessKeySummary {
    pages: Vec<PageNumber>,
    page_witness_pages: Vec<PageNumber>,
    cell_witnesses: Vec<ExactCellWitness>,
    has_global_keys: bool,
}

fn summarize_witness_keys(keys: &[WitnessKey]) -> WitnessKeySummary {
    let mut pages = Vec::new();
    let mut page_witness_pages = Vec::new();
    let mut cell_witnesses = Vec::new();
    let mut has_global_keys = false;
    for key in keys {
        match key {
            WitnessKey::Page(page) | WitnessKey::ByteRange { page, .. } => {
                pages.push(*page);
                page_witness_pages.push(*page);
            }
            WitnessKey::KeyRange { btree_root, .. } => {
                pages.push(*btree_root);
                page_witness_pages.push(*btree_root);
            }
            WitnessKey::Cell {
                btree_root,
                leaf_page,
                tag,
            } => {
                pages.push(*btree_root);
                pages.push(*leaf_page);
                cell_witnesses.push(ExactCellWitness::new(*btree_root, *leaf_page, *tag));
            }
            WitnessKey::Custom { .. } => {
                has_global_keys = true;
            }
        }
    }
    pages.sort_unstable();
    pages.dedup();
    page_witness_pages.sort_unstable();
    page_witness_pages.dedup();
    cell_witnesses.sort_unstable();
    cell_witnesses.dedup();
    WitnessKeySummary {
        pages,
        page_witness_pages,
        cell_witnesses,
        has_global_keys,
    }
}

fn hydrate_finalize_witness_state(
    registry: &ConcurrentRegistry,
    session_id: u64,
) -> Option<(
    Vec<WitnessKey>,
    WitnessKeySummary,
    Vec<WitnessKey>,
    WitnessKeySummary,
)> {
    let handle = registry.get(session_id)?;
    if !handle.is_active() {
        return None;
    }

    let mut read_keys = handle.read_witness_keys();
    read_keys.sort_unstable();
    let read_key_summary = summarize_witness_keys(&read_keys);

    let mut write_keys = handle.write_witness_keys();
    write_keys.sort_unstable();
    let write_key_summary = summarize_witness_keys(&write_keys);

    Some((read_keys, read_key_summary, write_keys, write_key_summary))
}

fn collect_precise_candidates(
    global_indexes: &[usize],
    indexes_by_page: &PageMap<Vec<usize>>,
    indexes_by_page_witness: &PageMap<Vec<usize>>,
    indexes_by_exact_cell: &HashMap<(PageNumber, u64), Vec<usize>>,
    summary: &WitnessKeySummary,
) -> Vec<usize> {
    // Short-circuit: when every committed-history index is empty there is
    // nothing to probe per summary page/cell. This is the hot shape when
    // no committed SSI history exists yet — every commit otherwise walks
    // `summary.page_witness_pages` doing wasted HashMap probes.
    if global_indexes.is_empty()
        && indexes_by_page.is_empty()
        && indexes_by_page_witness.is_empty()
        && indexes_by_exact_cell.is_empty()
    {
        return Vec::new();
    }
    let mut candidate_indexes = global_indexes.to_vec();
    for &page in &summary.page_witness_pages {
        if let Some(indexes) = indexes_by_page.get(&page) {
            candidate_indexes.extend(indexes.iter().copied());
        }
    }
    for cell in &summary.cell_witnesses {
        for page in [cell.btree_root, cell.leaf_page] {
            if let Some(indexes) = indexes_by_page_witness.get(&page) {
                candidate_indexes.extend(indexes.iter().copied());
            }
        }
        if let Some(indexes) = indexes_by_exact_cell.get(&cell.exact_key()) {
            candidate_indexes.extend(indexes.iter().copied());
        }
    }
    candidate_indexes.sort_unstable();
    candidate_indexes.dedup();
    candidate_indexes
}

#[derive(Debug, Default)]
struct ActiveEdgeDiscoveryIndex {
    all_readers: Vec<usize>,
    all_writers: Vec<usize>,
    readers_with_global_keys: Vec<usize>,
    writers_with_global_keys: Vec<usize>,
    // Identity-hashed by PageNumber via `PageNumberBuildHasher`. The default
    // `RandomState` was the `reserve_rehash` hotspot under
    // `ActiveEdgeDiscoveryIndex::build` in the MT 16t profile (1.03%
    // self-time — see
    // tests/artifacts/perf/bd-m4s2c-20260423T013614Z/perf-report-mt_writers_16x500.txt).
    readers_by_page: PageMap<Vec<usize>>,
    writers_by_page: PageMap<Vec<usize>>,
    readers_by_page_witness: PageMap<Vec<usize>>,
    writers_by_page_witness: PageMap<Vec<usize>>,
    readers_by_exact_cell: HashMap<(PageNumber, u64), Vec<usize>>,
    writers_by_exact_cell: HashMap<(PageNumber, u64), Vec<usize>>,
}

impl ActiveEdgeDiscoveryIndex {
    fn build(views: &[HandleView]) -> Self {
        // Pre-size the PageNumber-keyed maps to avoid the reserve_rehash
        // chain that otherwise doubles capacity on every insert
        // (0 → 3 → 7 → 15 → ...). Four slots per view is a conservative
        // lower bound for typical MT workloads; overshooting is cheap
        // because each bucket is pointer-sized.
        let page_cap = views.len().saturating_mul(4);
        let mut index = Self {
            readers_by_page: PageMap::with_capacity_and_hasher(
                page_cap,
                PageNumberBuildHasher::default(),
            ),
            writers_by_page: PageMap::with_capacity_and_hasher(
                page_cap,
                PageNumberBuildHasher::default(),
            ),
            readers_by_page_witness: PageMap::with_capacity_and_hasher(
                page_cap,
                PageNumberBuildHasher::default(),
            ),
            writers_by_page_witness: PageMap::with_capacity_and_hasher(
                page_cap,
                PageNumberBuildHasher::default(),
            ),
            ..Self::default()
        };
        for (idx, view) in views.iter().enumerate() {
            if view.has_read_witnesses() {
                index.all_readers.push(idx);
                if view.has_global_read_witnesses() {
                    index.readers_with_global_keys.push(idx);
                }
                for &page in &view.read_pages {
                    index.readers_by_page.entry(page).or_default().push(idx);
                }
                for &page in &view.read_page_witness_pages {
                    index
                        .readers_by_page_witness
                        .entry(page)
                        .or_default()
                        .push(idx);
                }
                for &cell in &view.read_exact_cells {
                    index
                        .readers_by_exact_cell
                        .entry(cell)
                        .or_default()
                        .push(idx);
                }
            }
            if view.has_write_witnesses() {
                index.all_writers.push(idx);
                if view.has_global_write_witnesses() {
                    index.writers_with_global_keys.push(idx);
                }
                for &page in &view.write_pages {
                    index.writers_by_page.entry(page).or_default().push(idx);
                }
                for &page in &view.write_page_witness_pages {
                    index
                        .writers_by_page_witness
                        .entry(page)
                        .or_default()
                        .push(idx);
                }
                for &cell in &view.write_exact_cells {
                    index
                        .writers_by_exact_cell
                        .entry(cell)
                        .or_default()
                        .push(idx);
                }
            }
        }
        index
    }

    fn incoming_candidate_refs<'a>(
        &'a self,
        views: &'a [HandleView],
        committing_txn: TxnToken,
        _committing_begin_seq: CommitSeq,
        committing_commit_seq: CommitSeq,
        write_key_summary: &WitnessKeySummary,
    ) -> Vec<&'a dyn ActiveTxnView> {
        // Short-circuit: if no active view has any read witnesses, the
        // SireadTable-equivalent indexes are all empty and there can be no
        // incoming rw-antidependency edges regardless of how many pages the
        // committer writes. Skip the per-page probe loop + sort/dedup. This
        // is a very common case on write-heavy workloads where the active
        // set is dominated by writers that have not yet read anything.
        if self.all_readers.is_empty() {
            return Vec::new();
        }
        let mut candidate_indexes = if write_key_summary.has_global_keys {
            self.all_readers.clone()
        } else {
            self.readers_with_global_keys.clone()
        };
        for &page in &write_key_summary.page_witness_pages {
            if let Some(indexes) = self.readers_by_page.get(&page) {
                candidate_indexes.extend(indexes.iter().copied());
            }
        }
        for cell in &write_key_summary.cell_witnesses {
            for page in [cell.btree_root, cell.leaf_page] {
                if let Some(indexes) = self.readers_by_page_witness.get(&page) {
                    candidate_indexes.extend(indexes.iter().copied());
                }
            }
            if let Some(indexes) = self.readers_by_exact_cell.get(&cell.exact_key()) {
                candidate_indexes.extend(indexes.iter().copied());
            }
        }
        candidate_indexes.sort_unstable();
        candidate_indexes.dedup();
        let committing_end = committing_commit_seq.get();
        candidate_indexes
            .into_iter()
            .filter_map(|idx| views.get(idx))
            .filter(|view| {
                !same_txn_identity(view.token, committing_txn)
                    && view.is_currently_active()
                    && view.begin_seq.get() < committing_end
            })
            .map(|view| view as &dyn ActiveTxnView)
            .collect()
    }

    fn outgoing_candidate_refs<'a>(
        &'a self,
        views: &'a [HandleView],
        committing_txn: TxnToken,
        _committing_begin_seq: CommitSeq,
        committing_commit_seq: CommitSeq,
        read_key_summary: &WitnessKeySummary,
    ) -> Vec<&'a dyn ActiveTxnView> {
        // Symmetric short-circuit to `incoming_candidate_refs`: when no
        // active view holds any write witnesses, no outgoing
        // rw-antidependency edge can exist no matter what the reader read.
        // This is the expected shape on read-heavy MT workloads where
        // only a handful of the active sessions are writers.
        if self.all_writers.is_empty() {
            return Vec::new();
        }
        let mut candidate_indexes = if read_key_summary.has_global_keys {
            self.all_writers.clone()
        } else {
            self.writers_with_global_keys.clone()
        };
        for &page in &read_key_summary.page_witness_pages {
            if let Some(indexes) = self.writers_by_page.get(&page) {
                candidate_indexes.extend(indexes.iter().copied());
            }
        }
        for cell in &read_key_summary.cell_witnesses {
            for page in [cell.btree_root, cell.leaf_page] {
                if let Some(indexes) = self.writers_by_page_witness.get(&page) {
                    candidate_indexes.extend(indexes.iter().copied());
                }
            }
            if let Some(indexes) = self.writers_by_exact_cell.get(&cell.exact_key()) {
                candidate_indexes.extend(indexes.iter().copied());
            }
        }
        candidate_indexes.sort_unstable();
        candidate_indexes.dedup();
        let committing_end = committing_commit_seq.get();
        candidate_indexes
            .into_iter()
            .filter_map(|idx| views.get(idx))
            .filter(|view| {
                !same_txn_identity(view.token, committing_txn)
                    && view.is_currently_active()
                    && view.begin_seq.get() < committing_end
            })
            .map(|view| view as &dyn ActiveTxnView)
            .collect()
    }
}

impl ActiveTxnView for HandleView {
    fn token(&self) -> TxnToken {
        self.token
    }

    fn begin_seq(&self) -> CommitSeq {
        self.begin_seq
    }

    fn is_active(&self) -> bool {
        self.is_currently_active()
    }

    fn read_keys(&self) -> &[WitnessKey] {
        &[]
    }

    fn write_keys(&self) -> &[WitnessKey] {
        &[]
    }

    fn check_read_overlap(&self, key: &WitnessKey) -> bool {
        match key {
            WitnessKey::Page(p) | WitnessKey::ByteRange { page: p, .. } => {
                self.read_pages.binary_search(p).is_ok()
            }
            WitnessKey::Cell {
                btree_root,
                leaf_page,
                tag,
            } => {
                self.read_page_witness_pages
                    .binary_search(btree_root)
                    .is_ok()
                    || self
                        .read_page_witness_pages
                        .binary_search(leaf_page)
                        .is_ok()
                    || self
                        .read_exact_cells
                        .binary_search(&(*btree_root, *tag))
                        .is_ok()
            }
            WitnessKey::KeyRange { btree_root, .. } => {
                self.read_pages.binary_search(btree_root).is_ok()
            }
            WitnessKey::Custom { .. } => {
                !self.read_pages.is_empty() || self.has_global_read_witnesses()
            }
        }
    }

    fn check_write_overlap(&self, key: &WitnessKey) -> bool {
        match key {
            WitnessKey::Page(p) | WitnessKey::ByteRange { page: p, .. } => {
                self.write_pages.binary_search(p).is_ok()
            }
            WitnessKey::Cell {
                btree_root,
                leaf_page,
                tag,
            } => {
                self.write_page_witness_pages
                    .binary_search(btree_root)
                    .is_ok()
                    || self
                        .write_page_witness_pages
                        .binary_search(leaf_page)
                        .is_ok()
                    || self
                        .write_exact_cells
                        .binary_search(&(*btree_root, *tag))
                        .is_ok()
            }
            WitnessKey::KeyRange { btree_root, .. } => {
                self.write_pages.binary_search(btree_root).is_ok()
            }
            WitnessKey::Custom { .. } => {
                !self.write_pages.is_empty() || self.has_global_write_witnesses()
            }
        }
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

    fn set_marked_for_abort(&self, _val: bool) {}
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

fn record_prepare_conflict_heat(
    txn: TxnToken,
    planned_commit_seq: CommitSeq,
    incoming_edges: &[DiscoveredEdge],
    outgoing_edges: &[DiscoveredEdge],
    write_set_pages: &[PageNumber],
) {
    let telemetry_enabled = conflict_heat_telemetry_enabled();
    let topology_policy_enabled = fsqlite_btree::conflict_topology_policy_enabled();
    if !telemetry_enabled && !topology_policy_enabled {
        return;
    }
    let edge_count = incoming_edges.len().saturating_add(outgoing_edges.len());
    if edge_count == 0 {
        return;
    }

    let mut edges = Vec::with_capacity(edge_count);
    let mut peer_tokens = Vec::with_capacity(edge_count);
    for edge in incoming_edges {
        edges.push(ConflictHeatEdge {
            from: edge.from,
            to: edge.to,
            overlap_page: witness_key_page(&edge.overlap_key),
            direction: ConflictOverlapDirection::Incoming,
            source_is_active: edge.source_is_active,
        });
        peer_tokens.push((
            edge.from.id.get(),
            edge.from.epoch.get(),
            edge.source_is_active,
        ));
    }
    for edge in outgoing_edges {
        edges.push(ConflictHeatEdge {
            from: edge.from,
            to: edge.to,
            overlap_page: witness_key_page(&edge.overlap_key),
            direction: ConflictOverlapDirection::Outgoing,
            source_is_active: edge.source_is_active,
        });
        peer_tokens.push((edge.to.id.get(), edge.to.epoch.get(), edge.source_is_active));
    }
    peer_tokens.sort_unstable();
    peer_tokens.dedup();

    let writer_overlap_estimate = u32::try_from(peer_tokens.len()).unwrap_or(u32::MAX);
    let conflict_heat = u64::try_from(edge_count).unwrap_or(u64::MAX);
    if topology_policy_enabled {
        let updated = fsqlite_btree::record_conflict_topology_heat_batch(
            edges.iter().filter_map(|edge| edge.overlap_page),
            1,
            writer_overlap_estimate,
        );
        if updated == 0 {
            let _updated = fsqlite_btree::record_conflict_topology_heat_batch(
                write_set_pages.iter().copied(),
                conflict_heat.max(1),
                writer_overlap_estimate,
            );
        }
    }
    let observation = ConflictHeatObservation {
        context: ConflictHeatContext {
            trace_id: "mvcc-ssi-prepare",
            run_id: "local",
            scenario_id: "prepare_concurrent_commit_with_ssi",
            btree_id: 0,
            table_or_index_role: "unknown",
            split_pressure: 0,
            allocation_class: "unknown",
            elapsed_ns: 0,
            first_failure_diag: "none",
        },
        commit_seq: planned_commit_seq,
        writer_overlap_estimate,
        conflict_heat,
        overlap_edges: &edges,
        fallback_pages: write_set_pages,
    };
    tracing::debug!(
        target: "fsqlite.mvcc.conflict_heat",
        txn_id = txn.id.get(),
        txn_epoch = txn.epoch.get(),
        commit_seq = planned_commit_seq.get(),
        writer_overlap_estimate,
        conflict_heat,
        edge_count,
        "mvcc conflict heat prepare observation"
    );
    if telemetry_enabled {
        record_conflict_heat_observation(&observation);
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
                release_tracked_page_locks(lock_table, handle, txn_id);
                handle.mark_aborted();
                return Err((MvccError::BusySnapshot, FcwResult::Clean));
            }

            // Check for dangerous structure (both in + out rw edges).
            if handle.has_in_rw() && handle.has_out_rw() {
                tracing::warn!(
                    txn = %txn_id,
                    "concurrent_commit: SSI pivot (in+out rw edges)"
                );
                release_tracked_page_locks(lock_table, handle, txn_id);
                handle.mark_aborted();
                return Err((MvccError::BusySnapshot, FcwResult::Clean));
            }

            // Commit: update commit index for every tracked write-conflict page,
            // including structural frees that no longer have staged bytes.
            let conflict_pages: smallvec::SmallVec<[PageNumber; 16]> =
                handle.tracked_write_conflict_pages_iter().collect();
            commit_index.batch_update(&conflict_pages, assign_commit_seq);
            // Release only the pages this transaction actually locked.
            release_tracked_page_locks(lock_table, handle, txn_id);
            handle.mark_committed();
            Ok(assign_commit_seq)
        }
        FcwResult::Conflict { .. } | FcwResult::Abort { .. } => {
            // Release only the pages this transaction actually locked.
            release_tracked_page_locks(lock_table, handle, txn_id);
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
    planned_commit_seq: CommitSeq,
) -> Result<PreparedConcurrentCommit, (MvccError, FcwResult)> {
    let txn_id = TxnId::new(session_id).ok_or((MvccError::InvalidState, FcwResult::Clean))?;

    let commit_view = {
        let handle = registry
            .get(session_id)
            .ok_or((MvccError::InvalidState, FcwResult::Clean))?;
        if !handle.is_active() {
            return Err((MvccError::InvalidState, FcwResult::Clean));
        }

        // Step 1: First-committer-wins validation.
        let fcw_result = validate_first_committer_wins(&handle, commit_index);
        if !matches!(fcw_result, FcwResult::Clean) {
            Err(fcw_result)
        } else {
            Ok((
                handle.token(),
                handle.begin_seq(),
                handle.write_set_pages().into_vec(),
                handle.held_lock_pages(),
                handle.is_marked_for_abort(),
            ))
        }
    };
    let (txn, begin_seq, write_set_pages, held_lock_pages, marked_for_abort) = match commit_view {
        Ok(view) => view,
        Err(fcw_result) => {
            if let Some(mut handle) = registry.get_mut(session_id) {
                release_tracked_page_locks(lock_table, &handle, txn_id);
                handle.mark_aborted();
            } else {
                lock_table.release_all(txn_id);
            }
            return Err((MvccError::BusySnapshot, fcw_result));
        }
    };

    if marked_for_abort {
        tracing::warn!(
            txn = %txn_id,
            "prepare_concurrent_commit_with_ssi: marked_for_abort"
        );
        if let Some(mut handle) = registry.get_mut(session_id) {
            release_tracked_page_locks(lock_table, &handle, txn_id);
            handle.mark_aborted();
        } else {
            lock_table.release_all(txn_id);
        }
        return Err((MvccError::BusySnapshot, FcwResult::Clean));
    }

    if registry.can_use_uncontended_prepare_fast_path(session_id, begin_seq) {
        if write_set_pages.is_empty() {
            let Some((sorted_read_keys, read_key_summary, sorted_write_keys, write_key_summary)) =
                hydrate_finalize_witness_state(registry, session_id)
            else {
                if let Some(mut handle) = registry.get_mut(session_id) {
                    release_tracked_page_locks(lock_table, &handle, txn_id);
                    handle.mark_aborted();
                } else {
                    lock_table.release_all(txn_id);
                }
                return Err((MvccError::InvalidState, FcwResult::Clean));
            };
            return Ok(PreparedConcurrentCommit {
                session_id,
                planned_commit_seq,
                txn_token: txn,
                begin_seq,
                read_keys: sorted_read_keys,
                read_key_summary,
                write_keys: sorted_write_keys,
                write_key_summary,
                write_set_pages,
                held_lock_pages,
                has_in_rw: false,
                has_out_rw: false,
                incoming_edges: Vec::new(),
                outgoing_edges: Vec::new(),
                dro_t3_decision: None,
                used_uncontended_prepare_fast_path: true,
                used_candidate_free_prepare_fast_path: false,
            });
        }
        return Ok(PreparedConcurrentCommit {
            session_id,
            planned_commit_seq,
            txn_token: txn,
            begin_seq,
            read_keys: Vec::new(),
            read_key_summary: WitnessKeySummary::default(),
            write_keys: Vec::new(),
            write_key_summary: WitnessKeySummary::default(),
            write_set_pages,
            held_lock_pages,
            has_in_rw: false,
            has_out_rw: false,
            incoming_edges: Vec::new(),
            outgoing_edges: Vec::new(),
            dro_t3_decision: None,
            used_uncontended_prepare_fast_path: true,
            used_candidate_free_prepare_fast_path: false,
        });
    }

    let Some((sorted_read_keys, _read_key_summary, sorted_write_keys, _write_key_summary)) =
        hydrate_finalize_witness_state(registry, session_id)
    else {
        if let Some(mut handle) = registry.get_mut(session_id) {
            release_tracked_page_locks(lock_table, &handle, txn_id);
            handle.mark_aborted();
        } else {
            lock_table.release_all(txn_id);
        }
        return Err((MvccError::InvalidState, FcwResult::Clean));
    };

    let read_key_summary = summarize_witness_keys(&sorted_read_keys);
    let write_key_summary = summarize_witness_keys(&sorted_write_keys);
    if sorted_read_keys.is_empty() && sorted_write_keys.is_empty() {
        return Ok(PreparedConcurrentCommit {
            session_id,
            planned_commit_seq,
            txn_token: txn,
            begin_seq,
            read_keys: sorted_read_keys,
            read_key_summary,
            write_keys: sorted_write_keys,
            write_key_summary,
            write_set_pages,
            held_lock_pages,
            has_in_rw: false,
            has_out_rw: false,
            incoming_edges: Vec::new(),
            outgoing_edges: Vec::new(),
            dro_t3_decision: None,
            used_uncontended_prepare_fast_path: false,
            used_candidate_free_prepare_fast_path: true,
        });
    }

    if sorted_read_keys.is_empty() {
        let views = registry
            .iter_active()
            .filter_map(|(_, handle)| {
                let guard = handle.lock();
                guard.is_active().then_some(HandleView::new(&guard))
            })
            .collect::<Vec<_>>();
        let active_index = ActiveEdgeDiscoveryIndex::build(&views);
        let active_reader_candidates = active_index.incoming_candidate_refs(
            &views,
            txn,
            begin_seq,
            planned_commit_seq,
            &write_key_summary,
        );
        let committed_reader_candidates = registry.committed_reader_candidates(
            txn,
            begin_seq,
            planned_commit_seq,
            &write_key_summary,
        );
        if active_reader_candidates.is_empty() && committed_reader_candidates.is_empty() {
            return Ok(PreparedConcurrentCommit {
                session_id,
                planned_commit_seq,
                txn_token: txn,
                begin_seq,
                read_keys: sorted_read_keys,
                read_key_summary,
                write_keys: sorted_write_keys,
                write_key_summary,
                write_set_pages,
                held_lock_pages,
                has_in_rw: false,
                has_out_rw: false,
                incoming_edges: Vec::new(),
                outgoing_edges: Vec::new(),
                dro_t3_decision: None,
                used_uncontended_prepare_fast_path: false,
                used_candidate_free_prepare_fast_path: true,
            });
        }

        let incoming_edges = discover_incoming_edges(
            txn,
            begin_seq,
            planned_commit_seq,
            &sorted_write_keys,
            &active_reader_candidates,
            &committed_reader_candidates,
        );
        record_prepare_conflict_heat(
            txn,
            planned_commit_seq,
            &incoming_edges,
            &[],
            &write_set_pages,
        );
        let dro_t3_decision = evaluate_prepare_t3_dro(
            txn,
            &incoming_edges,
            &[],
            registry.active.len(),
            registry.committed_readers.len(),
            registry.committed_writers.len(),
        );
        if incoming_edges
            .iter()
            .any(|edge| !edge.source_is_active && edge.source_has_in_rw)
        {
            tracing::warn!(
                ?txn,
                dro_penalty = dro_t3_decision.map_or(0.0, |decision| decision.cvar_penalty),
                dro_threshold = dro_t3_decision.map_or(0.0, |decision| decision.threshold),
                "SSI validation aborted"
            );
            return Err((
                MvccError::BusySnapshot,
                FcwResult::Abort {
                    reason: SsiAbortReason::CommittedPivot,
                },
            ));
        }

        return Ok(PreparedConcurrentCommit {
            session_id,
            planned_commit_seq,
            txn_token: txn,
            begin_seq,
            read_keys: sorted_read_keys,
            read_key_summary,
            write_keys: sorted_write_keys,
            write_key_summary,
            write_set_pages,
            held_lock_pages,
            has_in_rw: !incoming_edges.is_empty(),
            has_out_rw: false,
            incoming_edges,
            outgoing_edges: Vec::new(),
            dro_t3_decision,
            used_uncontended_prepare_fast_path: false,
            used_candidate_free_prepare_fast_path: false,
        });
    }

    // Step 2: Discover SSI edges without publishing side effects yet.
    let views = registry
        .iter_active()
        .filter_map(|(_, handle)| {
            let guard = handle.lock();
            guard.is_active().then_some(HandleView::new(&guard))
        })
        .collect::<Vec<_>>();
    let active_index = ActiveEdgeDiscoveryIndex::build(&views);
    let active_reader_candidates = active_index.incoming_candidate_refs(
        &views,
        txn,
        begin_seq,
        planned_commit_seq,
        &write_key_summary,
    );
    let active_writer_candidates = active_index.outgoing_candidate_refs(
        &views,
        txn,
        begin_seq,
        planned_commit_seq,
        &read_key_summary,
    );
    let committed_reader_candidates = registry.committed_reader_candidates(
        txn,
        begin_seq,
        planned_commit_seq,
        &write_key_summary,
    );
    let committed_writer_candidates =
        registry.committed_writer_candidates(txn, begin_seq, planned_commit_seq, &read_key_summary);
    if active_reader_candidates.is_empty()
        && active_writer_candidates.is_empty()
        && committed_reader_candidates.is_empty()
        && committed_writer_candidates.is_empty()
    {
        return Ok(PreparedConcurrentCommit {
            session_id,
            planned_commit_seq,
            txn_token: txn,
            begin_seq,
            read_keys: sorted_read_keys,
            read_key_summary,
            write_keys: sorted_write_keys,
            write_key_summary,
            write_set_pages,
            held_lock_pages,
            has_in_rw: false,
            has_out_rw: false,
            incoming_edges: Vec::new(),
            outgoing_edges: Vec::new(),
            dro_t3_decision: None,
            used_uncontended_prepare_fast_path: false,
            used_candidate_free_prepare_fast_path: true,
        });
    }

    let incoming_edges = discover_incoming_edges(
        txn,
        begin_seq,
        planned_commit_seq,
        &sorted_write_keys,
        &active_reader_candidates,
        &committed_reader_candidates,
    );
    let outgoing_edges = discover_outgoing_edges(
        txn,
        begin_seq,
        planned_commit_seq,
        &sorted_read_keys,
        &active_writer_candidates,
        &committed_writer_candidates,
    );
    record_prepare_conflict_heat(
        txn,
        planned_commit_seq,
        &incoming_edges,
        &outgoing_edges,
        &write_set_pages,
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

    let abort_reason = if has_in_rw && has_out_rw {
        Some(SsiAbortReason::Pivot)
    } else if incoming_edges
        .iter()
        .chain(&outgoing_edges)
        .any(|edge| !edge.source_is_active && edge.source_has_in_rw)
    {
        Some(SsiAbortReason::CommittedPivot)
    } else {
        None
    };

    if let Some(reason) = abort_reason {
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
        planned_commit_seq,
        txn_token: txn,
        begin_seq,
        read_keys: sorted_read_keys,
        read_key_summary,
        write_keys: sorted_write_keys,
        write_key_summary,
        write_set_pages,
        held_lock_pages,
        has_in_rw,
        has_out_rw,
        incoming_edges,
        outgoing_edges,
        dro_t3_decision,
        used_uncontended_prepare_fast_path: false,
        used_candidate_free_prepare_fast_path: false,
    })
}

/// Commit a concurrent transaction, running only first-committer-wins and
/// skipping the Cahill/Fekete SSI edge discovery + pivot check.
///
/// This is the "fast path" entry point used when the `SsiEProcessGate` (see
/// [`crate::ssi_eprocess_gate::SsiEProcessGate`]) certifies that the current
/// commit's coarse conflict pattern is inside a pivot-free safe region.
///
/// **Safety:** FCW is always preserved — it is required for MVCC invariants
/// INV-1/2/3 (first-committer-wins blocks write-write conflicts regardless
/// of SSI). Only the edge-discovery + pivot-detection step is skipped. The
/// returned plan is structurally indistinguishable from the
/// `used_candidate_free_prepare_fast_path` branch of
/// [`prepare_concurrent_commit_with_ssi`] — `finalize` treats it as having
/// no incoming/outgoing edges.
///
/// This function must only be called under `write_merge = LAB_UNSAFE`. The
/// gate consultation happens at the caller; this entry point just implements
/// the FCW-only shape.
///
/// # Errors
///
/// Returns `Err` if FCW detects a base-drift conflict (the conservative
/// write-write check), or if the handle is missing / marked for abort.
pub fn prepare_concurrent_commit_fcw_only(
    registry: &mut ConcurrentRegistry,
    commit_index: &CommitIndex,
    lock_table: &InProcessPageLockTable,
    session_id: u64,
    planned_commit_seq: CommitSeq,
) -> Result<PreparedConcurrentCommit, (MvccError, FcwResult)> {
    let txn_id = TxnId::new(session_id).ok_or((MvccError::InvalidState, FcwResult::Clean))?;

    let commit_view = {
        let handle = registry
            .get(session_id)
            .ok_or((MvccError::InvalidState, FcwResult::Clean))?;
        if !handle.is_active() {
            return Err((MvccError::InvalidState, FcwResult::Clean));
        }

        // FCW (Step 1) — mandatory for MVCC write-write invariants.
        let fcw_result = validate_first_committer_wins(&handle, commit_index);
        if !matches!(fcw_result, FcwResult::Clean) {
            Err(fcw_result)
        } else {
            Ok((
                handle.token(),
                handle.begin_seq(),
                handle.write_set_pages().into_vec(),
                handle.held_lock_pages(),
                handle.is_marked_for_abort(),
            ))
        }
    };
    let (txn, begin_seq, write_set_pages, held_lock_pages, marked_for_abort) = match commit_view {
        Ok(view) => view,
        Err(fcw_result) => {
            if let Some(mut handle) = registry.get_mut(session_id) {
                release_tracked_page_locks(lock_table, &handle, txn_id);
                handle.mark_aborted();
            } else {
                lock_table.release_all(txn_id);
            }
            return Err((MvccError::BusySnapshot, fcw_result));
        }
    };

    if marked_for_abort {
        tracing::warn!(
            txn = %txn_id,
            "prepare_concurrent_commit_fcw_only: marked_for_abort"
        );
        if let Some(mut handle) = registry.get_mut(session_id) {
            release_tracked_page_locks(lock_table, &handle, txn_id);
            handle.mark_aborted();
        } else {
            lock_table.release_all(txn_id);
        }
        return Err((MvccError::BusySnapshot, FcwResult::Clean));
    }

    // Hydrate the witness state so finalize can emit the committed-conflict
    // history the same way it would on the candidate-free fast path — this
    // keeps the committed-readers/writers ledger honest even when the gate
    // skips validation on the *current* commit.
    let Some((sorted_read_keys, read_key_summary, sorted_write_keys, write_key_summary)) =
        hydrate_finalize_witness_state(registry, session_id)
    else {
        if let Some(mut handle) = registry.get_mut(session_id) {
            release_tracked_page_locks(lock_table, &handle, txn_id);
            handle.mark_aborted();
        } else {
            lock_table.release_all(txn_id);
        }
        return Err((MvccError::InvalidState, FcwResult::Clean));
    };

    Ok(PreparedConcurrentCommit {
        session_id,
        planned_commit_seq,
        txn_token: txn,
        begin_seq,
        read_keys: sorted_read_keys,
        read_key_summary,
        write_keys: sorted_write_keys,
        write_key_summary,
        write_set_pages,
        held_lock_pages,
        has_in_rw: false,
        has_out_rw: false,
        incoming_edges: Vec::new(),
        outgoing_edges: Vec::new(),
        dro_t3_decision: None,
        used_uncontended_prepare_fast_path: false,
        used_candidate_free_prepare_fast_path: true,
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
    debug_assert!(
        committed_seq >= prepared.planned_commit_seq,
        "final commit sequence must not move backwards from the planning frontier"
    );

    let Some(txn_id) = TxnId::new(prepared.session_id) else {
        return;
    };

    if prepared.used_uncontended_prepare_fast_path()
        && registry.can_use_uncontended_finalize_fast_path(prepared.session_id, prepared.begin_seq)
    {
        commit_index.batch_update(&prepared.write_set_pages, committed_seq);
        lock_table.release_set(prepared.held_lock_pages.iter().copied(), txn_id);
        finalize_prepared_session_state(registry, prepared.session_id, false, false, "uncontended");
        registry.prune_committed_conflict_history();
        return;
    }

    let hydrated_witness_state;
    let (read_keys, read_key_summary, write_keys, write_key_summary): (
        &[WitnessKey],
        &WitnessKeySummary,
        &[WitnessKey],
        &WitnessKeySummary,
    ) = if prepared.used_uncontended_prepare_fast_path() {
        hydrated_witness_state = hydrate_finalize_witness_state(registry, prepared.session_id)
            .unwrap_or_else(|| {
                (
                    prepared.read_keys.clone(),
                    prepared.read_key_summary.clone(),
                    prepared.write_keys.clone(),
                    prepared.write_key_summary.clone(),
                )
            });
        (
            &hydrated_witness_state.0,
            &hydrated_witness_state.1,
            &hydrated_witness_state.2,
            &hydrated_witness_state.3,
        )
    } else {
        (
            &prepared.read_keys,
            &prepared.read_key_summary,
            &prepared.write_keys,
            &prepared.write_key_summary,
        )
    };

    if prepared.incoming_edges.is_empty()
        && prepared.outgoing_edges.is_empty()
        && read_keys.is_empty()
        && write_keys.is_empty()
    {
        commit_index.batch_update(&prepared.write_set_pages, committed_seq);
        lock_table.release_set(prepared.held_lock_pages.iter().copied(), txn_id);
        if let Some(mut handle) = registry.get_mut(prepared.session_id)
            && handle.is_active()
        {
            handle.has_in_rw.set(false);
            handle.has_out_rw.set(false);
            handle.mark_committed();
        }
        registry.prune_committed_conflict_history();
        return;
    }

    if prepared.incoming_edges.is_empty()
        && prepared.outgoing_edges.is_empty()
        && read_keys.is_empty()
    {
        let active_views: Vec<HandleView> = registry
            .active
            .values()
            .map(|handle| {
                let guard = handle.lock();
                HandleView::new(&guard)
            })
            .collect();
        let active_index = ActiveEdgeDiscoveryIndex::build(&active_views);
        let active_reader_candidates = active_index.incoming_candidate_refs(
            &active_views,
            prepared.txn_token,
            prepared.begin_seq,
            committed_seq,
            write_key_summary,
        );
        let committed_reader_candidates = registry.committed_reader_candidates(
            prepared.txn_token,
            prepared.begin_seq,
            committed_seq,
            write_key_summary,
        );
        if active_reader_candidates.is_empty() && committed_reader_candidates.is_empty() {
            commit_index.batch_update(&prepared.write_set_pages, committed_seq);
            lock_table.release_set(prepared.held_lock_pages.iter().copied(), txn_id);
            if let Some(mut handle) = registry.get_mut(prepared.session_id)
                && handle.is_active()
            {
                handle.has_in_rw.set(false);
                handle.has_out_rw.set(false);
                handle.mark_committed();
            }
            if !write_keys.is_empty() {
                registry.committed_writers.push(CommittedWriterInfo {
                    token: prepared.txn_token,
                    commit_seq: committed_seq,
                    had_out_rw: false,
                    keys: write_keys.to_vec(),
                });
                registry.index_committed_writer(registry.committed_writers.len() - 1);
            }
            registry.prune_committed_conflict_history();
            return;
        }
    }

    // Re-scan against current active state to capture overlap edges that may
    // appear after prepare but before finalize. This keeps committed pivot
    // decisions deterministic.
    let active_views: Vec<HandleView> = registry
        .active
        .values()
        .map(|handle| {
            let guard = handle.lock();
            HandleView::new(&guard)
        })
        .collect();
    let active_index = ActiveEdgeDiscoveryIndex::build(&active_views);
    let active_reader_candidates = active_index.incoming_candidate_refs(
        &active_views,
        prepared.txn_token,
        prepared.begin_seq,
        committed_seq,
        write_key_summary,
    );
    let active_writer_candidates = active_index.outgoing_candidate_refs(
        &active_views,
        prepared.txn_token,
        prepared.begin_seq,
        committed_seq,
        read_key_summary,
    );
    let committed_reader_candidates = registry.committed_reader_candidates(
        prepared.txn_token,
        prepared.begin_seq,
        committed_seq,
        write_key_summary,
    );
    let committed_writer_candidates = registry.committed_writer_candidates(
        prepared.txn_token,
        prepared.begin_seq,
        committed_seq,
        read_key_summary,
    );
    if prepared.incoming_edges.is_empty()
        && prepared.outgoing_edges.is_empty()
        && active_reader_candidates.is_empty()
        && active_writer_candidates.is_empty()
        && committed_reader_candidates.is_empty()
        && committed_writer_candidates.is_empty()
    {
        commit_index.batch_update(&prepared.write_set_pages, committed_seq);
        lock_table.release_set(prepared.held_lock_pages.iter().copied(), txn_id);
        if let Some(mut handle) = registry.get_mut(prepared.session_id)
            && handle.is_active()
        {
            handle.has_in_rw.set(false);
            handle.has_out_rw.set(false);
            handle.mark_committed();
        }
        if !read_keys.is_empty() {
            registry.committed_readers.push(CommittedReaderInfo {
                token: prepared.txn_token,
                begin_seq: prepared.begin_seq,
                commit_seq: committed_seq,
                had_in_rw: false,
                keys: read_keys.to_vec(),
            });
            registry.index_committed_reader(registry.committed_readers.len() - 1);
        }
        if !write_keys.is_empty() {
            registry.committed_writers.push(CommittedWriterInfo {
                token: prepared.txn_token,
                commit_seq: committed_seq,
                had_out_rw: false,
                keys: write_keys.to_vec(),
            });
            registry.index_committed_writer(registry.committed_writers.len() - 1);
        }
        registry.prune_committed_conflict_history();
        return;
    }

    let mut incoming_edges = prepared.incoming_edges.clone();
    let mut incoming_sources = incoming_edges.iter().map(|edge| edge.from).collect();
    merge_unique_incoming_edges(
        &mut incoming_edges,
        &mut incoming_sources,
        discover_incoming_edges(
            prepared.txn_token,
            prepared.begin_seq,
            committed_seq,
            write_keys,
            &active_reader_candidates,
            &committed_reader_candidates,
        ),
    );

    let mut outgoing_edges = prepared.outgoing_edges.clone();
    let mut outgoing_targets = outgoing_edges.iter().map(|edge| edge.to).collect();
    merge_unique_outgoing_edges(
        &mut outgoing_edges,
        &mut outgoing_targets,
        discover_outgoing_edges(
            prepared.txn_token,
            prepared.begin_seq,
            committed_seq,
            read_keys,
            &active_writer_candidates,
            &committed_writer_candidates,
        ),
    );

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
        for reader in registry.active.values() {
            let reader = reader.lock();
            if !reader.is_active() || !same_txn_identity(reader.token(), edge.from) {
                continue;
            }
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
            break;
        }
    }

    // T3 propagation for active writers on outgoing edges.
    for edge in &outgoing_edges {
        if !edge.source_is_active {
            continue;
        }
        for writer in registry.active.values() {
            let writer = writer.lock();
            if !writer.is_active() || !same_txn_identity(writer.token(), edge.to) {
                continue;
            }
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
            break;
        }
    }

    commit_index.batch_update(&prepared.write_set_pages, committed_seq);
    lock_table.release_set(prepared.held_lock_pages.iter().copied(), txn_id);
    finalize_prepared_session_state(
        registry,
        prepared.session_id,
        has_in_rw,
        has_out_rw,
        "edgeful",
    );

    if !read_keys.is_empty() {
        registry.committed_readers.push(CommittedReaderInfo {
            token: prepared.txn_token,
            begin_seq: prepared.begin_seq,
            commit_seq: committed_seq,
            had_in_rw: has_in_rw,
            keys: read_keys.to_vec(),
        });
        registry.index_committed_reader(registry.committed_readers.len() - 1);
    }
    if !write_keys.is_empty() {
        registry.committed_writers.push(CommittedWriterInfo {
            token: prepared.txn_token,
            commit_seq: committed_seq,
            had_out_rw: has_out_rw,
            keys: write_keys.to_vec(),
        });
        registry.index_committed_writer(registry.committed_writers.len() - 1);
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
            if let Some(shared_handle) = registry.remove(session_id) {
                {
                    let mut handle = shared_handle.lock();
                    concurrent_abort(&mut handle, lock_table, session_id);
                }
                registry.recycle_handle(shared_handle);
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
        release_tracked_page_locks(lock_table, handle, txn_id);
    }
    handle.mark_aborted();
}

/// Finalize a concurrent transaction that has no remaining staged writes.
///
/// `ROLLBACK TO` deliberately preserves page locks for the outer transaction
/// window.  If those writes are all rolled back, the commit path may become
/// logically read-only while still owning locks.  This helper releases that
/// preserved lock surface and marks the handle committed.
pub fn concurrent_commit_read_only(
    handle: &mut ConcurrentHandle,
    lock_table: &InProcessPageLockTable,
    session_id: u64,
) {
    debug_assert!(
        handle.write_set_pages().is_empty(),
        "read-only concurrent commit cannot finalize tracked write/conflict pages"
    );
    if let Some(txn_id) = TxnId::new(session_id) {
        release_tracked_page_locks(lock_table, handle, txn_id);
    }
    handle.mark_committed();
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
    let page_states_snapshot = handle
        .page_states
        .iter()
        .filter_map(|(&page, state)| {
            state.tracks_write_conflict().then_some((
                page,
                SavepointPageState {
                    staged_data: state.staged_data.clone(),
                    has_staged_write: state.has_staged_write,
                    is_freed: state.is_freed,
                    is_conflict_only: state.is_conflict_only,
                    metadata_exempt: state.metadata_exempt,
                },
            ))
        })
        .collect();
    Ok(ConcurrentSavepoint {
        name: name.to_owned(),
        page_states_snapshot,
        write_set_len: handle.write_set_len(),
    })
}

/// Rollback to a savepoint within a concurrent transaction.
///
/// Restores the write set to the state captured by the savepoint.
/// Page locks are NOT released (per spec §5.4).
/// The snapshot remains active for continued operations.
pub fn concurrent_rollback_to_savepoint(
    handle: &mut ConcurrentHandle,
    lock_table: &InProcessPageLockTable,
    session_id: u64,
    savepoint: &ConcurrentSavepoint,
) -> Result<(), MvccError> {
    if !handle.is_active() {
        return Err(MvccError::InvalidState);
    }
    let txn_id = TxnId::new(session_id).ok_or(MvccError::InvalidState)?;
    let mut reacquired_pages = Vec::new();
    for &page in savepoint.page_states_snapshot.keys() {
        if !handle.page_state(page).is_some_and(|state| state.held_lock) {
            if lock_table.try_acquire(page, txn_id).is_err() {
                for reacquired in reacquired_pages {
                    lock_table.release(reacquired, txn_id);
                }
                return Err(MvccError::Busy);
            }
            reacquired_pages.push(page);
        }
    }

    let mut restored: PageMap<PageTxnState> = PageMap::with_capacity_and_hasher(
        handle
            .page_states
            .len()
            .max(savepoint.page_states_snapshot.len()),
        PageNumberBuildHasher::default(),
    );

    for (&page, snapshot_state) in &savepoint.page_states_snapshot {
        restored.insert(
            page,
            PageTxnState {
                staged_data: snapshot_state.staged_data.clone(),
                has_staged_write: snapshot_state.has_staged_write,
                is_freed: snapshot_state.is_freed,
                is_conflict_only: snapshot_state.is_conflict_only,
                held_lock: true,
                metadata_exempt: snapshot_state.metadata_exempt,
            },
        );
    }

    for (&page, state) in &handle.page_states {
        if state.held_lock {
            restored.entry(page).or_insert_with(|| PageTxnState {
                held_lock: true,
                ..PageTxnState::default()
            });
        }
    }

    handle.page_states = restored;
    Ok(())
}

/// Check whether a transaction mode supports concurrent writers.
#[must_use]
pub const fn is_concurrent_mode(mode: TransactionMode) -> bool {
    matches!(mode, TransactionMode::Concurrent)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::Arc,
    };

    use fsqlite_types::{
        CommitSeq, PageData, PageNumber, PageSize, SchemaEpoch, Snapshot, TxnEpoch, TxnId,
        TxnToken, WitnessKey,
    };
    use proptest::{
        collection::vec as prop_vec,
        prelude::*,
        test_runner::{Config as ProptestConfig, TestCaseError},
    };

    use crate::core_types::{CommitIndex, InProcessPageLockTable, TransactionState};
    use crate::lifecycle::MvccError;
    use crate::observability::{
        conflict_heat_telemetry_snapshot, reset_conflict_heat_telemetry,
        set_conflict_heat_telemetry_enabled, set_conflict_heat_test_capture_enabled,
    };
    use crate::ssi_validation::{ActiveTxnView, SsiAbortReason};

    use super::{
        ActiveEdgeDiscoveryIndex, CommittedReaderInfo, CommittedWriterInfo, ConcurrentHandle,
        ConcurrentRegistry, FcwResult, HandleView, MAX_CONCURRENT_WRITERS, concurrent_abort,
        concurrent_clear_page_state, concurrent_commit, concurrent_commit_read_only,
        concurrent_commit_with_ssi, concurrent_free_page, concurrent_is_metadata_exempt,
        concurrent_mark_metadata_exempt, concurrent_page_is_freed,
        concurrent_page_is_synthetic_conflict_only, concurrent_page_read_status,
        concurrent_page_state, concurrent_prepare_write_page, concurrent_read_page,
        concurrent_restore_page_state, concurrent_rollback_to_savepoint, concurrent_savepoint,
        concurrent_stage_prepared_write_marker, concurrent_track_write_conflict_page,
        concurrent_write_metadata_page, concurrent_write_page,
        finalize_prepared_concurrent_commit_with_ssi, prepare_concurrent_commit_with_ssi,
        same_txn_identity, summarize_witness_keys, validate_first_committer_wins, witness_is_page,
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

    fn test_token(id: u64) -> TxnToken {
        TxnToken::new(
            TxnId::new(id).expect("test transaction id"),
            TxnEpoch::new(id as u32 + 1),
        )
    }

    const MVCC_METAMORPHIC_PROPTEST_CASES: u32 = 64;

    fn tagged_test_data(tag: u8) -> PageData {
        let mut data = test_data();
        data.as_bytes_mut()[0] = tag;
        data
    }

    fn unique_test_pages(raw_pages: &[u32]) -> Vec<PageNumber> {
        let mut seen = BTreeSet::new();
        raw_pages
            .iter()
            .filter_map(|raw| PageNumber::new(*raw))
            .filter(|page| seen.insert(*page))
            .collect()
    }

    fn run_disjoint_commit_schedule(
        pages: &[PageNumber],
        commit_order: &[usize],
    ) -> Result<BTreeMap<PageNumber, u8>, String> {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();
        let mut sessions = Vec::with_capacity(pages.len());

        for _ in pages {
            sessions.push(
                registry
                    .begin_concurrent(test_snapshot(10))
                    .map_err(|err| format!("begin_concurrent failed: {err:?}"))?,
            );
        }

        for (idx, (session_id, page)) in sessions.iter().zip(pages).enumerate() {
            let mut handle = registry
                .get_mut(*session_id)
                .ok_or_else(|| format!("missing session handle {session_id}"))?;
            let tag = u8::try_from(idx + 1).expect("proptest limits writers to <= 8");
            concurrent_write_page(
                &mut handle,
                &lock_table,
                *session_id,
                *page,
                tagged_test_data(tag),
            )
            .map_err(|err| {
                format!(
                    "write page {} in session {session_id} failed: {err:?}",
                    page.get()
                )
            })?;
        }

        for (order_idx, writer_idx) in commit_order.iter().copied().enumerate() {
            let session_id = sessions
                .get(writer_idx)
                .copied()
                .ok_or_else(|| format!("commit order references writer {writer_idx}"))?;
            let mut handle = registry
                .get_mut(session_id)
                .ok_or_else(|| format!("missing session handle {session_id}"))?;
            let commit_seq = CommitSeq::new(11 + order_idx as u64);
            concurrent_commit(
                &mut handle,
                &commit_index,
                &lock_table,
                session_id,
                commit_seq,
            )
            .map_err(|(err, fcw)| {
                format!("commit writer {writer_idx} session {session_id} failed: {err:?} {fcw:?}")
            })?;
        }

        if lock_table.lock_count() != 0 {
            return Err(format!(
                "expected all page locks released, found {}",
                lock_table.lock_count()
            ));
        }

        let mut final_state = BTreeMap::new();
        for (idx, (session_id, page)) in sessions.iter().zip(pages).enumerate() {
            let tag = u8::try_from(idx + 1).expect("proptest limits writers to <= 8");
            if commit_index.latest(*page).is_none() {
                return Err(format!("page {} missing committed sequence", page.get()));
            }
            final_state.insert(*page, tag);
            let removed = registry.remove_and_recycle(*session_id);
            if !removed {
                return Err(format!("session {session_id} was not removed"));
            }
        }

        Ok(final_state)
    }

    fn reader_snapshot_view(
        handle: &ConcurrentHandle,
        commit_index: &CommitIndex,
        pages: &[PageNumber],
    ) -> BTreeMap<PageNumber, (Option<u64>, bool, Option<u8>)> {
        let snapshot_high = handle.snapshot().high;
        let mut view = BTreeMap::new();
        for page in pages {
            let committed = commit_index
                .latest(*page)
                .filter(|seq| *seq <= snapshot_high)
                .map(CommitSeq::get);
            let (is_freed, has_staged_write) = concurrent_page_read_status(handle, *page);
            let local_tag = has_staged_write
                .then(|| concurrent_read_page(handle, *page).map(|data| data.as_bytes()[0]))
                .flatten();
            view.insert(*page, (committed, is_freed, local_tag));
        }
        view
    }

    fn union_pages(left: &[PageNumber], right: &[PageNumber]) -> Vec<PageNumber> {
        let mut pages = BTreeSet::new();
        pages.extend(left.iter().copied());
        pages.extend(right.iter().copied());
        pages.into_iter().collect()
    }

    fn write_set_with_extras(overlap: &[PageNumber], extras: &[PageNumber]) -> Vec<PageNumber> {
        union_pages(overlap, extras)
    }

    fn stage_tagged_pages(
        handle: &mut ConcurrentHandle,
        lock_table: &InProcessPageLockTable,
        session_id: u64,
        pages: &[PageNumber],
    ) -> Result<(), String> {
        for (idx, page) in pages.iter().enumerate() {
            let tag = u8::try_from(idx + 1)
                .map_err(|err| format!("tag conversion for page {} failed: {err}", page.get()))?;
            concurrent_write_page(handle, lock_table, session_id, *page, tagged_test_data(tag))
                .map_err(|err| {
                    format!(
                        "write page {} in session {session_id} failed: {err:?}",
                        page.get()
                    )
                })?;
        }
        Ok(())
    }

    fn run_conflict_direction(
        first_pages: &[PageNumber],
        second_pages: &[PageNumber],
    ) -> Result<BTreeSet<PageNumber>, String> {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();
        let first_session = registry
            .begin_concurrent(test_snapshot(10))
            .map_err(|err| format!("begin first failed: {err:?}"))?;
        let second_session = registry
            .begin_concurrent(test_snapshot(10))
            .map_err(|err| format!("begin second failed: {err:?}"))?;

        {
            let mut first = registry
                .get_mut(first_session)
                .ok_or_else(|| "missing first handle".to_string())?;
            stage_tagged_pages(&mut first, &lock_table, first_session, first_pages)?;
            concurrent_commit(
                &mut first,
                &commit_index,
                &lock_table,
                first_session,
                CommitSeq::new(11),
            )
            .map_err(|(err, fcw)| format!("first commit failed: {err:?} {fcw:?}"))?;
        }

        let conflict_pages = {
            let mut second = registry
                .get_mut(second_session)
                .ok_or_else(|| "missing second handle".to_string())?;
            stage_tagged_pages(&mut second, &lock_table, second_session, second_pages)?;
            match concurrent_commit(
                &mut second,
                &commit_index,
                &lock_table,
                second_session,
                CommitSeq::new(12),
            ) {
                Err((
                    MvccError::BusySnapshot,
                    FcwResult::Conflict {
                        conflicting_pages, ..
                    },
                )) => conflicting_pages.into_iter().collect::<BTreeSet<_>>(),
                Err((err, fcw)) => {
                    return Err(format!(
                        "second commit failed without FCW conflict: {err:?} {fcw:?}"
                    ));
                }
                Ok(seq) => {
                    return Err(format!("second commit unexpectedly succeeded at {seq}"));
                }
            }
        };

        if lock_table.lock_count() != 0 {
            return Err(format!(
                "expected conflict path to release locks, found {}",
                lock_table.lock_count()
            ));
        }
        if !registry.remove_and_recycle(first_session) {
            return Err(format!("first session {first_session} was not removed"));
        }
        if !registry.remove_and_recycle(second_session) {
            return Err(format!("second session {second_session} was not removed"));
        }

        Ok(conflict_pages)
    }

    // Metamorphic strength matrix:
    // - MR1 commit-order independence: fault sensitivity 5, independence 4,
    //   execution cost 1. It detects false global writer serialization or
    //   order-dependent FCW failures for disjoint page writes.
    // - MR2 snapshot isolation: fault sensitivity 5, independence 5,
    //   execution cost 1. It detects leaked uncommitted writer state in a
    //   reader's already-established snapshot view.
    // - MR3 conflict symmetry: fault sensitivity 4, independence 4,
    //   execution cost 1. It detects asymmetric FCW overlap accounting when
    //   the same two write sets are committed in opposite directions.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(MVCC_METAMORPHIC_PROPTEST_CASES))]

        #[test]
        fn metamorphic_commit_order_independence_for_disjoint_writers(
            raw_pages in prop_vec(1_u32..=2048, 1..=8),
        ) {
            let pages = unique_test_pages(&raw_pages);
            prop_assert!(!pages.is_empty());

            let forward_order = (0..pages.len()).collect::<Vec<_>>();
            let mut reverse_order = forward_order.clone();
            reverse_order.reverse();

            let forward_state = run_disjoint_commit_schedule(&pages, &forward_order)
                .map_err(TestCaseError::fail)?;
            let reverse_state = run_disjoint_commit_schedule(&pages, &reverse_order)
                .map_err(TestCaseError::fail)?;

            prop_assert_eq!(forward_state, reverse_state);
        }

        #[test]
        fn metamorphic_snapshot_isolation_hides_uncommitted_writes(
            raw_base_pages in prop_vec(1_u32..=2048, 1..=8),
            raw_writer_pages in prop_vec(1_u32..=2048, 1..=8),
        ) {
            let base_pages = unique_test_pages(&raw_base_pages);
            let writer_pages = unique_test_pages(&raw_writer_pages);
            prop_assert!(!base_pages.is_empty());
            prop_assert!(!writer_pages.is_empty());

            let lock_table = InProcessPageLockTable::new();
            let commit_index = CommitIndex::new();
            for (idx, page) in base_pages.iter().enumerate() {
                commit_index.update(*page, CommitSeq::new(2 + idx as u64));
            }

            let all_pages = union_pages(&base_pages, &writer_pages);
            let mut registry = ConcurrentRegistry::new();
            let reader_session = registry
                .begin_concurrent(test_snapshot(10))
                .map_err(|err| TestCaseError::fail(format!("begin reader failed: {err:?}")))?;
            let writer_session = registry
                .begin_concurrent(test_snapshot(10))
                .map_err(|err| TestCaseError::fail(format!("begin writer failed: {err:?}")))?;

            let before = {
                let reader = registry
                    .get(reader_session)
                    .ok_or_else(|| TestCaseError::fail("missing reader handle"))?;
                reader_snapshot_view(&reader, &commit_index, &all_pages)
            };

            {
                let mut writer = registry
                    .get_mut(writer_session)
                    .ok_or_else(|| TestCaseError::fail("missing writer handle"))?;
                for (idx, page) in writer_pages.iter().enumerate() {
                    let tag = u8::try_from(idx + 1).expect("proptest limits writers to <= 8");
                    concurrent_write_page(
                        &mut writer,
                        &lock_table,
                        writer_session,
                        *page,
                        tagged_test_data(tag),
                    )
                    .map_err(|err| {
                        TestCaseError::fail(format!(
                            "uncommitted writer page {} failed: {err:?}",
                            page.get()
                        ))
                    })?;
                    let writer_tag = concurrent_read_page(&writer, *page)
                        .map(|data| data.as_bytes()[0]);
                    prop_assert_eq!(writer_tag, Some(tag));
                }
            }

            let after = {
                let reader = registry
                    .get(reader_session)
                    .ok_or_else(|| TestCaseError::fail("missing reader handle"))?;
                reader_snapshot_view(&reader, &commit_index, &all_pages)
            };
            prop_assert_eq!(before, after);

            {
                let mut writer = registry
                    .get_mut(writer_session)
                    .ok_or_else(|| TestCaseError::fail("missing writer handle"))?;
                concurrent_abort(&mut writer, &lock_table, writer_session);
            }
            prop_assert_eq!(lock_table.lock_count(), 0);
            prop_assert!(registry.remove_and_recycle(reader_session));
            prop_assert!(registry.remove_and_recycle(writer_session));
        }

        #[test]
        fn metamorphic_conflict_symmetry_reports_same_overlap(
            raw_overlap_pages in prop_vec(1_u32..=256, 1..=8),
            raw_left_extra_pages in prop_vec(1001_u32..=2048, 0..=4),
            raw_right_extra_pages in prop_vec(3001_u32..=4096, 0..=4),
        ) {
            let overlap_pages = unique_test_pages(&raw_overlap_pages);
            prop_assert!(!overlap_pages.is_empty());
            let left_extra_pages = unique_test_pages(&raw_left_extra_pages);
            let right_extra_pages = unique_test_pages(&raw_right_extra_pages);
            let left_pages = write_set_with_extras(&overlap_pages, &left_extra_pages);
            let right_pages = write_set_with_extras(&overlap_pages, &right_extra_pages);
            let expected_overlap = overlap_pages.iter().copied().collect::<BTreeSet<_>>();

            let left_then_right = run_conflict_direction(&left_pages, &right_pages)
                .map_err(TestCaseError::fail)?;
            let right_then_left = run_conflict_direction(&right_pages, &left_pages)
                .map_err(TestCaseError::fail)?;

            prop_assert_eq!(&left_then_right, &expected_overlap);
            prop_assert_eq!(&right_then_left, &expected_overlap);
            prop_assert_eq!(left_then_right, right_then_left);
        }
    }

    struct ConflictHeatTelemetryGuard;

    impl Drop for ConflictHeatTelemetryGuard {
        fn drop(&mut self) {
            set_conflict_heat_telemetry_enabled(false);
            set_conflict_heat_test_capture_enabled(false);
            reset_conflict_heat_telemetry();
            fsqlite_btree::reset_conflict_topology_policy_state();
            fsqlite_btree::set_conflict_topology_policy_mode(
                fsqlite_btree::ConflictTopologyPolicyMode::Enforced,
            );
        }
    }

    fn enable_conflict_heat_telemetry_for_test() -> ConflictHeatTelemetryGuard {
        reset_conflict_heat_telemetry();
        fsqlite_btree::reset_conflict_topology_policy_state();
        fsqlite_btree::set_conflict_topology_policy_mode(
            fsqlite_btree::ConflictTopologyPolicyMode::Enforced,
        );
        set_conflict_heat_test_capture_enabled(true);
        set_conflict_heat_telemetry_enabled(true);
        ConflictHeatTelemetryGuard
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
        {
            let mut h1 = registry.get_mut(s1).expect("handle 1");
            concurrent_write_page(&mut h1, &lock_table, s1, test_page(5), test_data())
                .expect("write page 5");
        }

        // Session 2 writes page 10 (different page => no conflict).
        {
            let mut h2 = registry.get_mut(s2).expect("handle 2");
            concurrent_write_page(&mut h2, &lock_table, s2, test_page(10), test_data())
                .expect("write page 10");
        }

        // Both commit successfully.
        {
            let mut h1 = registry.get_mut(s1).expect("handle 1");
            let seq1 =
                concurrent_commit(&mut h1, &commit_index, &lock_table, s1, CommitSeq::new(11))
                    .expect("commit 1");
            assert_eq!(seq1, CommitSeq::new(11));
        }

        {
            let mut h2 = registry.get_mut(s2).expect("handle 2");
            let seq2 =
                concurrent_commit(&mut h2, &commit_index, &lock_table, s2, CommitSeq::new(12))
                    .expect("commit 2");
            assert_eq!(seq2, CommitSeq::new(12));
        }
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
        {
            let mut h1 = registry.get_mut(s1).expect("handle 1");
            concurrent_write_page(&mut h1, &lock_table, s1, test_page(5), test_data())
                .expect("s1 write page 5");
        }

        // s1 commits first (first-committer-wins).
        {
            let mut h1 = registry.get_mut(s1).expect("handle 1");
            concurrent_commit(&mut h1, &commit_index, &lock_table, s1, CommitSeq::new(11))
                .expect("s1 commits first");
        }

        // Now s2 tries to write and commit the same page.  The lock was
        // released by s1's commit, so s2 can acquire it.
        {
            let mut h2 = registry.get_mut(s2).expect("handle 2");
            concurrent_write_page(&mut h2, &lock_table, s2, test_page(5), test_data())
                .expect("s2 write page 5");
        }

        {
            let mut h2 = registry.get_mut(s2).expect("handle 2");
            let result =
                concurrent_commit(&mut h2, &commit_index, &lock_table, s2, CommitSeq::new(12));
            assert!(result.is_err());
            let (err, fcw) = result.unwrap_err();
            assert_eq!(err, MvccError::BusySnapshot);
            assert!(matches!(fcw, FcwResult::Conflict { .. }));
        }
    }

    #[test]
    fn test_same_page_stale_writer_hits_fcw_after_lock_busy_retry() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();
        let page = test_page(77);

        let s1 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session 1");
        let s2 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session 2");

        {
            let mut h1 = registry.get_mut(s1).expect("handle 1");
            concurrent_write_page(&mut h1, &lock_table, s1, page, tagged_test_data(1))
                .expect("s1 write page");
        }

        {
            let mut h2 = registry.get_mut(s2).expect("handle 2");
            let result = concurrent_write_page(&mut h2, &lock_table, s2, page, tagged_test_data(2));
            assert_eq!(result, Err(MvccError::Busy));
        }

        let seq1 = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s1,
            CommitSeq::new(11),
        )
        .expect("s1 commits first");
        assert_eq!(seq1, CommitSeq::new(11));
        assert_eq!(commit_index.latest(page), Some(CommitSeq::new(11)));

        {
            let mut h2 = registry.get_mut(s2).expect("handle 2");
            concurrent_write_page(&mut h2, &lock_table, s2, page, tagged_test_data(2))
                .expect("s2 write page after s1 releases lock");
        }

        let result = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s2,
            CommitSeq::new(12),
        );
        let (err, fcw) = result.expect_err("stale same-page writer must abort");
        assert_eq!(err, MvccError::BusySnapshot);
        assert_eq!(
            fcw,
            FcwResult::Conflict {
                conflicting_pages: vec![page],
                conflicting_commit_seq: CommitSeq::new(11),
            }
        );
        assert_eq!(commit_index.latest(page), Some(CommitSeq::new(11)));
        assert_eq!(lock_table.lock_count(), 0);
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
        {
            let mut h1 = registry.get_mut(s1).expect("h1");
            concurrent_write_page(&mut h1, &lock_table, s1, test_page(5), test_data()).unwrap();
        }

        {
            let mut h3 = registry.get_mut(s3).expect("h3");
            concurrent_write_page(&mut h3, &lock_table, s3, test_page(10), test_data()).unwrap();
        }

        // s1 commits first on page 5.
        {
            let mut h1 = registry.get_mut(s1).expect("h1");
            concurrent_commit(&mut h1, &commit_index, &lock_table, s1, CommitSeq::new(11))
                .expect("s1 commits");
        }

        // s2 now tries page 5 (same as s1, but s1 already committed).
        {
            let mut h2 = registry.get_mut(s2).expect("h2");
            concurrent_write_page(&mut h2, &lock_table, s2, test_page(5), test_data()).unwrap();
        }

        {
            let mut h2 = registry.get_mut(s2).expect("h2");
            let result =
                concurrent_commit(&mut h2, &commit_index, &lock_table, s2, CommitSeq::new(12));
            assert!(result.is_err());
            let (err, _) = result.unwrap_err();
            assert_eq!(err, MvccError::BusySnapshot);
        }

        // s3 commits on page 10 (no conflict with s1's page 5).
        {
            let mut h3 = registry.get_mut(s3).expect("h3");
            let seq3 =
                concurrent_commit(&mut h3, &commit_index, &lock_table, s3, CommitSeq::new(13))
                    .expect("s3 commits");
            assert_eq!(seq3, CommitSeq::new(13));
        }
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
        {
            let mut handle = registry.get_mut(s1).expect("handle");
            concurrent_write_page(&mut handle, &lock_table, s1, test_page(1), test_data()).unwrap();
        }

        // Create savepoint.
        let sp = {
            let handle = registry.get(s1).expect("handle");
            concurrent_savepoint(&handle, "sp1").unwrap()
        };
        assert_eq!(sp.captured_len(), 1);

        // Write page 2 (INSERT B).
        {
            let mut handle = registry.get_mut(s1).expect("handle");
            concurrent_write_page(&mut handle, &lock_table, s1, test_page(2), test_data()).unwrap();
            assert_eq!(handle.write_set_len(), 2);
        }

        // Rollback to savepoint: page 2 should be removed from write set,
        // but its lock should still be held.
        {
            let mut handle = registry.get_mut(s1).expect("handle");
            concurrent_rollback_to_savepoint(&mut handle, &lock_table, s1, &sp).unwrap();
            assert_eq!(handle.write_set_len(), 1);
            assert!(handle.held_locks().contains(&test_page(2))); // Lock preserved.
        }

        // Write page 3 (INSERT C).
        {
            let mut handle = registry.get_mut(s1).expect("handle");
            concurrent_write_page(&mut handle, &lock_table, s1, test_page(3), test_data()).unwrap();
        }

        // Commit: pages 1 and 3 are in the write set (not page 2).
        {
            let handle = registry.get_mut(s1).expect("handle");
            let mut pages = handle.write_set_pages();
            pages.sort();
            assert_eq!(pages.as_slice(), &[test_page(1), test_page(3)]);
        }

        {
            let mut handle = registry.get_mut(s1).expect("handle");
            concurrent_commit(
                &mut handle,
                &commit_index,
                &lock_table,
                s1,
                CommitSeq::new(11),
            )
            .expect("commit succeeds");
        }

        let s2 = registry
            .begin_concurrent(test_snapshot(11))
            .expect("session 2");
        let mut handle2 = registry.get_mut(s2).expect("handle 2");
        concurrent_write_page(&mut handle2, &lock_table, s2, test_page(2), test_data())
            .expect("savepoint-preserved lock must be released on commit");
    }

    #[test]
    fn test_savepoint_within_concurrent_restores_freed_pages() {
        let lock_table = InProcessPageLockTable::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        {
            let mut handle = registry.get_mut(s1).expect("handle");
            concurrent_write_page(&mut handle, &lock_table, s1, test_page(1), test_data()).unwrap();
        }

        let sp = {
            let handle = registry.get(s1).expect("handle");
            concurrent_savepoint(&handle, "sp1").unwrap()
        };

        {
            let mut handle = registry.get_mut(s1).expect("handle");
            concurrent_free_page(&mut handle, &lock_table, s1, test_page(1)).unwrap();
            assert!(concurrent_page_is_freed(&handle, test_page(1)));

            concurrent_rollback_to_savepoint(&mut handle, &lock_table, s1, &sp).unwrap();
            assert!(!concurrent_page_is_freed(&handle, test_page(1)));
            assert!(concurrent_read_page(&handle, test_page(1)).is_some());
            assert_eq!(handle.write_set_pages().as_slice(), &[test_page(1)]);
        }
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
        {
            let handle = registry.get(s1).expect("handle");
            assert!(concurrent_read_page(&handle, test_page(5)).is_none());
        }

        // After writing, local read returns the written data.
        {
            let mut handle = registry.get_mut(s1).expect("handle");
            concurrent_write_page(&mut handle, &lock_table, s1, test_page(5), test_data()).unwrap();
        }

        {
            let handle = registry.get(s1).expect("handle");
            assert!(concurrent_read_page(&handle, test_page(5)).is_some());
            assert!(concurrent_read_page(&handle, test_page(6)).is_none());
        }
    }

    #[test]
    fn test_marker_staged_write_tracks_conflict_without_payload_bytes() -> Result<(), MvccError> {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let session_id = registry.begin_concurrent(test_snapshot(10))?;
        let page = test_page(5);
        let mut handle = registry
            .get_mut(session_id)
            .ok_or(MvccError::InvalidState)?;

        concurrent_prepare_write_page(&mut handle, &lock_table, session_id, page)?;
        concurrent_stage_prepared_write_marker(&mut handle, page)?;

        assert_eq!(concurrent_page_read_status(&handle, page), (false, true));
        assert!(
            concurrent_read_page(&handle, page).is_none(),
            "marker-only writes must not manufacture an MVCC payload snapshot"
        );
        assert_eq!(handle.write_set_len(), 1);
        assert_eq!(handle.write_set_pages().as_slice(), &[page]);
        assert!(handle.write_set().contains_key(&page));
        assert!(handle.held_locks().contains(&page));
        assert_eq!(
            validate_first_committer_wins(&handle, &commit_index),
            FcwResult::Clean
        );

        commit_index.update(page, CommitSeq::new(11));
        assert_eq!(
            validate_first_committer_wins(&handle, &commit_index),
            FcwResult::Conflict {
                conflicting_pages: vec![page],
                conflicting_commit_seq: CommitSeq::new(11),
            },
            "marker-only writes must remain in the FCW conflict surface"
        );
        Ok(())
    }

    #[test]
    fn test_marker_staged_write_survives_savepoint_rollback() -> Result<(), MvccError> {
        let lock_table = InProcessPageLockTable::new();
        let mut registry = ConcurrentRegistry::new();

        let session_id = registry.begin_concurrent(test_snapshot(10))?;
        let page = test_page(5);
        let other_page = test_page(6);
        let savepoint = {
            let mut handle = registry
                .get_mut(session_id)
                .ok_or(MvccError::InvalidState)?;
            concurrent_prepare_write_page(&mut handle, &lock_table, session_id, page)?;
            concurrent_stage_prepared_write_marker(&mut handle, page)?;
            concurrent_savepoint(&handle, "sp1")?
        };

        {
            let mut handle = registry
                .get_mut(session_id)
                .ok_or(MvccError::InvalidState)?;
            concurrent_prepare_write_page(&mut handle, &lock_table, session_id, other_page)?;
            concurrent_stage_prepared_write_marker(&mut handle, other_page)?;
            concurrent_free_page(&mut handle, &lock_table, session_id, page)?;

            concurrent_rollback_to_savepoint(&mut handle, &lock_table, session_id, &savepoint)?;

            assert_eq!(concurrent_page_read_status(&handle, page), (false, true));
            assert!(concurrent_read_page(&handle, page).is_none());
            assert!(!handle.write_set().contains_key(&other_page));
            assert!(
                handle.held_locks().contains(&other_page),
                "rollback preserves locks acquired after the savepoint"
            );
            assert_eq!(handle.write_set_pages().as_slice(), &[page]);
        }
        Ok(())
    }

    #[test]
    fn test_concurrent_free_page_removes_local_read_and_still_tracks_conflict_page() {
        let lock_table = InProcessPageLockTable::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        let mut handle = registry.get_mut(s1).expect("handle");
        concurrent_write_page(&mut handle, &lock_table, s1, test_page(5), test_data()).unwrap();
        concurrent_free_page(&mut handle, &lock_table, s1, test_page(5)).unwrap();

        assert!(concurrent_page_is_freed(&handle, test_page(5)));
        assert!(concurrent_read_page(&handle, test_page(5)).is_none());
        assert_eq!(handle.write_set_len(), 0);
        assert_eq!(handle.write_set_pages().as_slice(), &[test_page(5)]);
    }

    #[test]
    fn test_validate_first_committer_wins_considers_freed_pages() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let mut handle = registry.get_mut(s1).expect("handle");
        concurrent_free_page(&mut handle, &lock_table, s1, test_page(5)).unwrap();

        commit_index.update(test_page(5), CommitSeq::new(11));
        assert_eq!(
            validate_first_committer_wins(&handle, &commit_index),
            FcwResult::Conflict {
                conflicting_pages: vec![test_page(5)],
                conflicting_commit_seq: CommitSeq::new(11),
            }
        );
    }

    #[test]
    fn test_restore_page_state_releases_new_lock_and_clears_failed_free() {
        let lock_table = InProcessPageLockTable::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let mut handle = registry.get_mut(s1).expect("handle");
        let saved = concurrent_page_state(&handle, test_page(8));

        concurrent_free_page(&mut handle, &lock_table, s1, test_page(8)).unwrap();
        assert!(concurrent_page_is_freed(&handle, test_page(8)));

        concurrent_restore_page_state(&mut handle, &lock_table, s1, &saved).unwrap();
        assert!(!concurrent_page_is_freed(&handle, test_page(8)));
        assert!(concurrent_read_page(&handle, test_page(8)).is_none());
        assert!(!handle.held_locks().contains(&test_page(8)));

        let other_txn = fsqlite_types::TxnId::new(999).unwrap();
        assert!(
            lock_table.try_acquire(test_page(8), other_txn).is_ok(),
            "restoring clean state must release the transient page lock"
        );
        assert!(lock_table.release(test_page(8), other_txn));
    }

    #[test]
    fn test_clear_page_state_releases_lock_and_conflict_tracking() {
        let lock_table = InProcessPageLockTable::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let mut handle = registry.get_mut(s1).expect("handle");
        concurrent_track_write_conflict_page(&mut handle, &lock_table, s1, PageNumber::ONE)
            .unwrap();

        assert!(handle.tracks_write_conflict_page(PageNumber::ONE));
        assert!(handle.held_locks().contains(&PageNumber::ONE));
        assert!(
            concurrent_page_is_synthetic_conflict_only(&handle, PageNumber::ONE),
            "a conflict-only page with no staged data or free marker is synthetic"
        );

        concurrent_clear_page_state(&mut handle, &lock_table, s1, PageNumber::ONE).unwrap();

        assert!(
            !handle.tracks_write_conflict_page(PageNumber::ONE),
            "clearing the page state must remove the synthetic conflict surface"
        );
        assert!(
            !concurrent_page_is_synthetic_conflict_only(&handle, PageNumber::ONE),
            "clearing the page state must clear the synthetic predicate"
        );
        assert!(
            !handle.held_locks().contains(&PageNumber::ONE),
            "clearing the page state must release the page lock"
        );

        let other_txn = fsqlite_types::TxnId::new(999).unwrap();
        assert!(
            lock_table.try_acquire(PageNumber::ONE, other_txn).is_ok(),
            "clearing the page state must make the page lock available to other writers"
        );
        assert!(lock_table.release(PageNumber::ONE, other_txn));
    }

    #[test]
    fn test_rollback_to_savepoint_reacquires_cleared_page_state_lock() {
        let lock_table = InProcessPageLockTable::new();
        let mut registry = ConcurrentRegistry::new();
        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        let savepoint = {
            let mut handle = registry.get_mut(s1).expect("handle");
            concurrent_track_write_conflict_page(&mut handle, &lock_table, s1, PageNumber::ONE)
                .unwrap();
            concurrent_savepoint(&handle, "sp1").unwrap()
        };

        {
            let mut handle = registry.get_mut(s1).expect("handle");
            concurrent_clear_page_state(&mut handle, &lock_table, s1, PageNumber::ONE).unwrap();
            assert!(!handle.tracks_write_conflict_page(PageNumber::ONE));
            assert!(!handle.holds_page_lock(PageNumber::ONE));

            concurrent_rollback_to_savepoint(&mut handle, &lock_table, s1, &savepoint).unwrap();
            assert!(handle.tracks_write_conflict_page(PageNumber::ONE));
            assert!(handle.holds_page_lock(PageNumber::ONE));
        }

        let other_txn = fsqlite_types::TxnId::new(999).unwrap();
        assert!(
            lock_table.try_acquire(PageNumber::ONE, other_txn).is_err(),
            "rollback-to-savepoint must reacquire the cleared page lock"
        );
    }

    #[test]
    fn test_concurrent_commit_updates_commit_index_for_freed_pages() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let mut handle = registry.get_mut(s1).expect("handle");
        concurrent_free_page(&mut handle, &lock_table, s1, test_page(11)).unwrap();

        concurrent_commit(
            &mut handle,
            &commit_index,
            &lock_table,
            s1,
            CommitSeq::new(11),
        )
        .expect("commit should succeed");

        assert_eq!(commit_index.latest(test_page(11)), Some(CommitSeq::new(11)));
    }

    #[test]
    fn test_concurrent_commit_updates_commit_index_for_conflict_only_pages() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let mut handle = registry.get_mut(s1).expect("handle");
        concurrent_track_write_conflict_page(&mut handle, &lock_table, s1, PageNumber::ONE)
            .unwrap();

        concurrent_commit(
            &mut handle,
            &commit_index,
            &lock_table,
            s1,
            CommitSeq::new(11),
        )
        .expect("commit should succeed");

        assert_eq!(
            commit_index.latest(PageNumber::ONE),
            Some(CommitSeq::new(11))
        );
    }

    #[test]
    fn test_concurrent_write_page_fast_path_reuses_owned_page_without_duplicate_witnesses() {
        let lock_table = InProcessPageLockTable::new();
        let mut registry = ConcurrentRegistry::new();
        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let page = test_page(5);
        let updated = PageData::from_vec(vec![0x7A; PageSize::DEFAULT.as_usize()]);

        let mut handle = registry.get_mut(s1).expect("handle");
        concurrent_write_page(&mut handle, &lock_table, s1, page, test_data()).unwrap();
        concurrent_write_page(&mut handle, &lock_table, s1, page, updated.clone()).unwrap();

        assert_eq!(concurrent_read_page(&handle, page), Some(&updated));
        assert_eq!(handle.held_locks().len(), 1);
        assert_eq!(
            handle
                .write_witness_keys()
                .iter()
                .filter(|witness| witness_is_page(witness, page))
                .count(),
            1,
            "rewriting an already-owned page should not duplicate page witnesses"
        );
    }

    #[test]
    fn test_concurrent_track_write_conflict_page_fast_path_reuses_tracked_page_without_duplicate_witnesses()
     {
        let lock_table = InProcessPageLockTable::new();
        let mut registry = ConcurrentRegistry::new();
        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let page = PageNumber::ONE;

        let mut handle = registry.get_mut(s1).expect("handle");
        concurrent_track_write_conflict_page(&mut handle, &lock_table, s1, page).unwrap();
        concurrent_track_write_conflict_page(&mut handle, &lock_table, s1, page).unwrap();

        assert!(handle.tracks_write_conflict_page(page));
        assert_eq!(handle.held_locks().len(), 1);
        assert_eq!(
            handle
                .write_witness_keys()
                .iter()
                .filter(|witness| witness_is_page(witness, page))
                .count(),
            1,
            "retracking an already-owned conflict-only page should not duplicate page witnesses"
        );
    }

    #[test]
    fn test_concurrent_write_page_reuses_savepoint_preserved_lock_without_duplicate_witnesses() {
        let lock_table = InProcessPageLockTable::new();
        let mut registry = ConcurrentRegistry::new();
        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let page = test_page(11);
        let expected = test_data();

        let mut handle = registry.get_mut(s1).expect("handle");
        concurrent_write_page(&mut handle, &lock_table, s1, page, expected.clone()).unwrap();
        let savepoint = concurrent_savepoint(&handle, "sp1").unwrap();
        concurrent_write_page(&mut handle, &lock_table, s1, test_page(12), test_data()).unwrap();
        concurrent_rollback_to_savepoint(&mut handle, &lock_table, s1, &savepoint).unwrap();
        assert!(handle.held_locks().contains(&test_page(12)));
        assert!(!handle.tracks_write_conflict_page(test_page(12)));

        concurrent_write_page(
            &mut handle,
            &lock_table,
            s1,
            test_page(12),
            expected.clone(),
        )
        .unwrap();
        assert_eq!(
            concurrent_read_page(&handle, test_page(12)),
            Some(&expected)
        );
        assert_eq!(
            handle
                .write_witness_keys()
                .iter()
                .filter(|witness| witness_is_page(witness, test_page(12)))
                .count(),
            1,
            "reusing a savepoint-preserved lock should not duplicate page witnesses"
        );
    }

    #[test]
    fn test_concurrent_write_page_promotes_conflict_only_page_without_duplicate_witnesses() {
        let lock_table = InProcessPageLockTable::new();
        let mut registry = ConcurrentRegistry::new();
        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let page = PageNumber::ONE;
        let expected = test_data();

        let mut handle = registry.get_mut(s1).expect("handle");
        concurrent_track_write_conflict_page(&mut handle, &lock_table, s1, page).unwrap();
        concurrent_write_page(&mut handle, &lock_table, s1, page, expected.clone()).unwrap();

        assert_eq!(concurrent_read_page(&handle, page), Some(&expected));
        assert!(
            !handle
                .page_state(page)
                .is_some_and(|state| state.is_conflict_only)
        );
        assert_eq!(
            handle
                .write_witness_keys()
                .iter()
                .filter(|witness| witness_is_page(witness, page))
                .count(),
            1,
            "promoting a conflict-only page into the write set should not duplicate page witnesses"
        );
    }

    #[test]
    fn test_concurrent_write_page_rewrites_freed_page_without_duplicate_witnesses() {
        let lock_table = InProcessPageLockTable::new();
        let mut registry = ConcurrentRegistry::new();
        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let page = test_page(13);
        let expected = test_data();

        let mut handle = registry.get_mut(s1).expect("handle");
        concurrent_write_page(&mut handle, &lock_table, s1, page, expected.clone()).unwrap();
        concurrent_free_page(&mut handle, &lock_table, s1, page).unwrap();
        concurrent_write_page(&mut handle, &lock_table, s1, page, expected.clone()).unwrap();

        assert_eq!(concurrent_read_page(&handle, page), Some(&expected));
        assert!(!concurrent_page_is_freed(&handle, page));
        assert_eq!(
            handle
                .write_witness_keys()
                .iter()
                .filter(|witness| witness_is_page(witness, page))
                .count(),
            1,
            "rewriting a previously freed page should not duplicate page witnesses"
        );
    }

    #[test]
    fn test_concurrent_registry_remove_keeps_shared_handle_alive_for_existing_clones() {
        let mut registry = ConcurrentRegistry::new();
        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        let shared = registry.handle(s1).expect("shared handle");
        let removed = registry.remove(s1).expect("removed handle");

        assert!(Arc::ptr_eq(&shared, &removed));
        assert_eq!(registry.active_count(), 0);
        assert!(removed.lock().is_active());
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

        let mut handle = registry.get_mut(s1).expect("handle");
        concurrent_write_page(&mut handle, &lock_table, s1, test_page(5), test_data()).unwrap();
        concurrent_write_page(&mut handle, &lock_table, s1, test_page(6), test_data()).unwrap();
        assert_eq!(handle.held_locks().len(), 2);
        drop(handle);

        // Abort: locks released.
        let mut handle = registry.get_mut(s1).expect("handle");
        concurrent_abort(&mut handle, &lock_table, s1);
        assert!(!handle.is_active());
        drop(handle);

        // Another session can now acquire the same locks.
        let s2 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session 2");
        let mut handle2 = registry.get_mut(s2).expect("handle 2");
        concurrent_write_page(&mut handle2, &lock_table, s2, test_page(5), test_data())
            .expect("lock should be available after abort");
    }

    #[test]
    fn test_concurrent_abort_releases_savepoint_preserved_lock() {
        let lock_table = InProcessPageLockTable::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session");
        {
            let mut handle = registry.get_mut(s1).expect("handle");
            concurrent_write_page(&mut handle, &lock_table, s1, test_page(5), test_data()).unwrap();
            let savepoint = concurrent_savepoint(&handle, "sp1").unwrap();
            concurrent_write_page(&mut handle, &lock_table, s1, test_page(6), test_data()).unwrap();
            concurrent_rollback_to_savepoint(&mut handle, &lock_table, s1, &savepoint).unwrap();
            assert!(handle.held_locks().contains(&test_page(6)));
            assert!(!handle.tracks_write_conflict_page(test_page(6)));
            concurrent_abort(&mut handle, &lock_table, s1);
            assert!(!handle.is_active());
        }

        let s2 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session 2");
        let mut handle2 = registry.get_mut(s2).expect("handle 2");
        concurrent_write_page(&mut handle2, &lock_table, s2, test_page(6), test_data())
            .expect("savepoint-preserved lock should be available after abort");
    }

    #[test]
    fn test_read_only_commit_releases_savepoint_preserved_lock() {
        let lock_table = InProcessPageLockTable::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session");
        {
            let mut handle = registry.get_mut(s1).expect("handle");
            let savepoint = concurrent_savepoint(&handle, "sp1").unwrap();
            concurrent_write_page(&mut handle, &lock_table, s1, test_page(6), test_data()).unwrap();
            concurrent_rollback_to_savepoint(&mut handle, &lock_table, s1, &savepoint).unwrap();
            assert!(
                handle.write_set_pages().is_empty(),
                "ROLLBACK TO removed every staged write/conflict page"
            );
            assert!(
                handle.held_locks().contains(&test_page(6)),
                "ROLLBACK TO keeps post-savepoint page locks until transaction end"
            );
            concurrent_commit_read_only(&mut handle, &lock_table, s1);
            assert!(!handle.is_active());
        }

        let s2 = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session 2");
        let mut handle2 = registry.get_mut(s2).expect("handle 2");
        concurrent_write_page(&mut handle2, &lock_table, s2, test_page(6), test_data())
            .expect("savepoint-preserved lock should be available after read-only commit");
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
        {
            let mut handle = registry.get_mut(s1).expect("handle");
            concurrent_write_page(&mut handle, &lock_table, s1, test_page(5), test_data()).unwrap();
        }

        let handle = registry.get(s1).expect("handle");
        assert_eq!(
            validate_first_committer_wins(&handle, &commit_index),
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
        {
            let mut handle = registry.get_mut(s1).expect("handle");
            concurrent_write_page(&mut handle, &lock_table, s1, test_page(5), test_data()).unwrap();
        }

        let handle = registry.get(s1).expect("handle");
        let result = validate_first_committer_wins(&handle, &commit_index);
        match result {
            FcwResult::Conflict {
                conflicting_pages,
                conflicting_commit_seq,
            } => {
                assert_eq!(conflicting_pages, vec![test_page(5)]);
                assert_eq!(conflicting_commit_seq, CommitSeq::new(15));
            }
            other => assert!(
                matches!(other, FcwResult::Conflict { .. }),
                "expected conflict, got {other:?}"
            ),
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
        drop(handle);

        let removed = registry.remove(s1);
        assert!(removed.is_some());
        assert_eq!(registry.active_count(), 0);
    }

    #[test]
    fn test_marked_for_abort_txn_still_pins_gc_but_not_history_retention() {
        let mut registry = ConcurrentRegistry::new();
        let doomed = registry
            .begin_concurrent(test_snapshot(10))
            .expect("doomed session");
        let survivor = registry
            .begin_concurrent(test_snapshot(20))
            .expect("survivor session");

        registry
            .get_mut(doomed)
            .expect("doomed handle")
            .set_marked_for_abort(true);
        let survivor_token = registry.get(survivor).expect("survivor handle").txn_token();
        registry
            .committed_readers
            .push(crate::ssi_validation::CommittedReaderInfo {
                token: survivor_token,
                begin_seq: CommitSeq::new(20),
                commit_seq: CommitSeq::new(15),
                had_in_rw: false,
                keys: vec![WitnessKey::Page(test_page(7))],
            });

        assert_eq!(
            registry.gc_horizon(),
            Some(CommitSeq::new(10)),
            "marked-for-abort transactions remain active until they actually abort"
        );
        assert_eq!(
            registry.history_retention_horizon(),
            Some(CommitSeq::new(20)),
            "committed SSI history may ignore transactions already doomed to abort"
        );

        registry.prune_committed_conflict_history();
        assert!(
            registry.committed_readers.is_empty(),
            "history older than the surviving retention horizon should be pruned"
        );
    }

    #[test]
    fn test_gc_horizon_cache_advances_on_remove_and_is_conservative_until_then() {
        let mut registry = ConcurrentRegistry::new();
        let earlier = registry
            .begin_concurrent(test_snapshot(10))
            .expect("earlier session");
        let later = registry
            .begin_concurrent(test_snapshot(20))
            .expect("later session");

        assert_eq!(registry.gc_horizon(), Some(CommitSeq::new(10)));

        registry
            .get_mut(earlier)
            .expect("earlier handle")
            .mark_committed();
        assert_eq!(
            registry.gc_horizon(),
            Some(CommitSeq::new(10)),
            "cached horizon remains conservative until the committed session is removed"
        );

        let removed = registry.remove(earlier);
        assert!(removed.is_some());
        assert_eq!(registry.gc_horizon(), Some(CommitSeq::new(20)));

        let removed = registry.remove(later);
        assert!(removed.is_some());
        assert_eq!(registry.gc_horizon(), None);
    }

    #[test]
    fn test_can_use_uncontended_prepare_fast_path_only_for_single_active_session_without_overlap() {
        let mut registry = ConcurrentRegistry::new();
        let session_id = registry
            .begin_concurrent(test_snapshot(10))
            .expect("session");

        assert!(
            registry.can_use_uncontended_prepare_fast_path(session_id, CommitSeq::new(10)),
            "single active session with no newer committed history should use the fast path"
        );

        let other_session_id = registry
            .begin_concurrent(test_snapshot(10))
            .expect("other session");
        assert!(
            !registry.can_use_uncontended_prepare_fast_path(session_id, CommitSeq::new(10)),
            "multiple active sessions must force full SSI edge discovery"
        );
        registry
            .remove(other_session_id)
            .expect("remove other session");
        let session_token = registry.get(session_id).expect("handle").txn_token();

        registry
            .committed_writers
            .push(crate::ssi_validation::CommittedWriterInfo {
                token: session_token,
                commit_seq: CommitSeq::new(11),
                had_out_rw: false,
                keys: vec![WitnessKey::Page(test_page(7))],
            });

        assert!(
            !registry.can_use_uncontended_prepare_fast_path(session_id, CommitSeq::new(10)),
            "committed history newer than the begin sequence must disable the fast path"
        );
    }

    #[test]
    fn test_begin_concurrent_reuses_recycled_handle_and_clears_state() {
        let mut registry = ConcurrentRegistry::new();
        let session1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        {
            let mut handle = registry.get_mut(session1).unwrap();
            handle.record_read(test_page(5));
            handle.record_write_witness(WitnessKey::Page(test_page(7)));
            handle.has_in_rw.set(true);
            handle.has_out_rw.set(true);
            handle.set_marked_for_abort(true);
            handle.mark_aborted();
        }

        let removed = registry.remove(session1).unwrap();
        let recycled_ptr = Arc::as_ptr(&removed);
        registry.recycle_handle(removed);

        let session2 = registry.begin_concurrent(test_snapshot(20)).unwrap();
        let handle = registry.handle(session2).unwrap();
        assert_eq!(Arc::as_ptr(&handle), recycled_ptr);

        let handle = handle.lock();
        assert_eq!(handle.snapshot().high, CommitSeq::new(20));
        assert!(handle.is_active());
        assert!(handle.read_set().is_empty());
        assert!(handle.write_set_pages().is_empty());
        assert!(!handle.has_in_rw());
        assert!(!handle.has_out_rw());
        assert!(!handle.is_marked_for_abort());
        assert_eq!(handle.txn_token().id.get(), session2);
    }

    #[test]
    fn test_prepare_concurrent_commit_with_ssi_uses_uncontended_fast_path_after_stale_history_is_pruned()
     {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let seed_session = registry.begin_concurrent(test_snapshot(10)).unwrap();
        {
            let mut seed = registry.get_mut(seed_session).unwrap();
            seed.record_read(test_page(3));
            concurrent_write_page(
                &mut seed,
                &lock_table,
                seed_session,
                test_page(5),
                test_data(),
            )
            .unwrap();
        }
        concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            seed_session,
            CommitSeq::new(11),
        )
        .unwrap();
        registry
            .remove(seed_session)
            .expect("remove committed seed");
        assert!(
            registry.committed_writers.is_empty(),
            "once no active sessions remain, stale committed history should already be pruned"
        );

        let session_id = registry.begin_concurrent(test_snapshot(11)).unwrap();
        {
            let mut handle = registry.get_mut(session_id).unwrap();
            handle.record_read(test_page(7));
            concurrent_write_page(
                &mut handle,
                &lock_table,
                session_id,
                test_page(9),
                test_data(),
            )
            .unwrap();
        }

        let prepared = prepare_concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            session_id,
            CommitSeq::new(12),
        )
        .expect("prepare should succeed");

        assert!(
            prepared.conflicting_txns().is_empty(),
            "the uncontended fast path should not manufacture SSI conflicts"
        );
        assert!(
            prepared.conflict_pages().contains(&test_page(9)),
            "the plan should still carry the write set pages forward to finalize"
        );
        assert_eq!(
            prepared.dro_t3_decision(),
            None,
            "uncontended fast path should bypass DRO evaluation"
        );
        assert!(
            prepared.used_uncontended_prepare_fast_path(),
            "prepare should tag uncontended plans so finalize can re-check for the matching fast path"
        );
        assert!(
            prepared.read_keys().is_empty() && prepared.write_keys().is_empty(),
            "uncontended prepare should defer witness materialization until slow finalize actually needs it"
        );
    }

    #[test]
    fn test_finalize_uncontended_fast_path_skips_ssi_history_when_still_uncontended() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let session_id = registry.begin_concurrent(test_snapshot(11)).unwrap();
        {
            let mut handle = registry.get_mut(session_id).unwrap();
            handle.record_read(test_page(7));
            concurrent_write_page(
                &mut handle,
                &lock_table,
                session_id,
                test_page(9),
                test_data(),
            )
            .unwrap();
        }

        let prepared = prepare_concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            session_id,
            CommitSeq::new(12),
        )
        .expect("prepare should succeed");

        assert!(
            prepared.used_uncontended_prepare_fast_path(),
            "setup should hit the uncontended prepare fast path"
        );

        finalize_prepared_concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            &prepared,
            CommitSeq::new(12),
        );

        let committed = registry.get(session_id).expect("committed handle");
        assert_eq!(
            committed.state(),
            TransactionState::Committed,
            "uncontended finalize should mark the prepared session committed"
        );
        assert!(
            !committed.has_in_rw() && !committed.has_out_rw(),
            "uncontended finalize should not manufacture SSI edges"
        );
        drop(committed);

        assert_eq!(
            commit_index.latest(test_page(9)),
            Some(CommitSeq::new(12)),
            "finalize must still publish the committed page version"
        );
        assert!(
            registry.committed_readers.is_empty(),
            "uncontended finalize should skip committed reader history publication"
        );
        assert!(
            registry.committed_writers.is_empty(),
            "uncontended finalize should skip committed writer history publication"
        );

        let next_session = registry.begin_concurrent(test_snapshot(12)).unwrap();
        let mut next_handle = registry.get_mut(next_session).unwrap();
        concurrent_write_page(
            &mut next_handle,
            &lock_table,
            next_session,
            test_page(9),
            test_data(),
        )
        .expect("uncontended finalize must still release the page lock");
    }

    #[test]
    fn test_finalize_rehydrates_deferred_witnesses_when_fast_path_plan_loses_eligibility() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let session_id = registry.begin_concurrent(test_snapshot(11)).unwrap();
        {
            let mut handle = registry.get_mut(session_id).unwrap();
            handle.record_read(test_page(7));
            concurrent_write_page(
                &mut handle,
                &lock_table,
                session_id,
                test_page(9),
                test_data(),
            )
            .unwrap();
        }

        let prepared = prepare_concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            session_id,
            CommitSeq::new(12),
        )
        .expect("prepare should succeed");

        assert!(
            prepared.used_uncontended_prepare_fast_path(),
            "setup should hit the uncontended prepare fast path"
        );
        assert!(
            prepared.read_keys().is_empty() && prepared.write_keys().is_empty(),
            "prepare should not materialize witness vectors on the uncontended path"
        );

        let blocker = registry.begin_concurrent(test_snapshot(11)).unwrap();
        {
            let blocker_handle = registry.get(blocker).unwrap();
            assert!(blocker_handle.is_active(), "blocker txn should stay active");
        }

        finalize_prepared_concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            &prepared,
            CommitSeq::new(12),
        );

        assert_eq!(
            registry.committed_readers.len(),
            1,
            "slow finalize fallback should rehydrate deferred read witnesses before publishing history"
        );
        assert_eq!(
            registry.committed_writers.len(),
            1,
            "slow finalize fallback should rehydrate deferred write witnesses before publishing history"
        );
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
            let mut h1 = registry.get_mut(s1).expect("handle 1");
            concurrent_write_page(&mut h1, &lock_table, s1, test_page(5), test_data())
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
        let mut h2 = registry.get_mut(s2).expect("handle 2");
        concurrent_write_page(&mut h2, &lock_table, s2, test_page(5), test_data())
            .expect("page lock should be released during finalize");
    }

    #[test]
    fn test_finalize_rechecks_committed_writers_that_commit_after_prepare() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        {
            let mut h1 = registry.get_mut(s1).unwrap();
            h1.record_read(test_page(7));
            concurrent_write_page(&mut h1, &lock_table, s1, test_page(9), test_data()).unwrap();
        }

        let prepared = prepare_concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s1,
            CommitSeq::new(11),
        )
        .expect("prepare should succeed before the late writer commits");

        let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        {
            let mut h2 = registry.get_mut(s2).unwrap();
            concurrent_write_page(&mut h2, &lock_table, s2, test_page(7), test_data()).unwrap();
        }
        concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s2,
            CommitSeq::new(11),
        )
        .expect("late writer should commit before the prepared txn finalizes");

        finalize_prepared_concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            &prepared,
            CommitSeq::new(12),
        );

        let committed = registry
            .get(s1)
            .expect("prepared txn handle should remain present until explicit removal");
        assert_eq!(
            committed.state(),
            TransactionState::Committed,
            "edgeful finalize should mark the prepared session committed"
        );
        assert!(
            committed.has_out_rw(),
            "finalize must discover committed writers that were not present during prepare"
        );
    }

    #[test]
    fn test_finalize_rechecks_committed_readers_that_commit_after_prepare() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        {
            let mut h1 = registry.get_mut(s1).unwrap();
            h1.record_read(test_page(3));
            concurrent_write_page(&mut h1, &lock_table, s1, test_page(9), test_data()).unwrap();
        }

        let prepared = prepare_concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s1,
            CommitSeq::new(11),
        )
        .expect("prepare should succeed before the late reader commits");

        let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        {
            let mut h2 = registry.get_mut(s2).unwrap();
            h2.record_read(test_page(9));
            concurrent_write_page(&mut h2, &lock_table, s2, test_page(11), test_data()).unwrap();
        }
        concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s2,
            CommitSeq::new(11),
        )
        .expect("late reader should commit before the prepared txn finalizes");

        finalize_prepared_concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            &prepared,
            CommitSeq::new(12),
        );

        let committed = registry
            .get(s1)
            .expect("prepared txn handle should remain present until explicit removal");
        assert_eq!(
            committed.state(),
            TransactionState::Committed,
            "edgeful finalize should mark the prepared session committed"
        );
        assert!(
            committed.has_in_rw(),
            "finalize must discover committed readers that were not present during prepare"
        );
    }

    #[test]
    fn test_committed_reader_candidates_include_matching_page_and_global_entries_only() {
        let mut registry = ConcurrentRegistry::new();
        let relevant = test_token(101);
        let global = test_token(102);
        let unrelated = test_token(103);

        registry.committed_readers.push(CommittedReaderInfo {
            token: unrelated,
            begin_seq: CommitSeq::new(5),
            commit_seq: CommitSeq::new(12),
            had_in_rw: false,
            keys: vec![WitnessKey::Page(test_page(99))],
        });
        registry.index_committed_reader(0);
        registry.committed_readers.push(CommittedReaderInfo {
            token: relevant,
            begin_seq: CommitSeq::new(5),
            commit_seq: CommitSeq::new(12),
            had_in_rw: false,
            keys: vec![WitnessKey::Page(test_page(7))],
        });
        registry.index_committed_reader(1);
        registry.committed_readers.push(CommittedReaderInfo {
            token: global,
            begin_seq: CommitSeq::new(5),
            commit_seq: CommitSeq::new(12),
            had_in_rw: false,
            keys: vec![WitnessKey::Custom {
                namespace: 7,
                bytes: b"global-reader".to_vec(),
            }],
        });
        registry.index_committed_reader(2);

        let summary = summarize_witness_keys(&[WitnessKey::Page(test_page(7))]);
        let mut candidates = registry
            .committed_reader_candidates(
                test_token(999),
                CommitSeq::new(10),
                CommitSeq::new(20),
                &summary,
            )
            .into_iter()
            .map(|reader| reader.token)
            .collect::<Vec<_>>();
        candidates.sort_by_key(|token| token.id.get());

        assert_eq!(candidates, vec![relevant, global]);
    }

    #[test]
    fn test_committed_writer_candidates_include_matching_page_and_global_entries_only() {
        let mut registry = ConcurrentRegistry::new();
        let relevant = test_token(201);
        let global = test_token(202);
        let unrelated = test_token(203);

        registry.committed_writers.push(CommittedWriterInfo {
            token: unrelated,
            commit_seq: CommitSeq::new(12),
            had_out_rw: false,
            keys: vec![WitnessKey::Page(test_page(88))],
        });
        registry.index_committed_writer(0);
        registry.committed_writers.push(CommittedWriterInfo {
            token: relevant,
            commit_seq: CommitSeq::new(12),
            had_out_rw: false,
            keys: vec![WitnessKey::Page(test_page(9))],
        });
        registry.index_committed_writer(1);
        registry.committed_writers.push(CommittedWriterInfo {
            token: global,
            commit_seq: CommitSeq::new(12),
            had_out_rw: false,
            keys: vec![WitnessKey::Custom {
                namespace: 9,
                bytes: b"global-writer".to_vec(),
            }],
        });
        registry.index_committed_writer(2);

        let summary = summarize_witness_keys(&[WitnessKey::Page(test_page(9))]);
        let mut candidates = registry
            .committed_writer_candidates(
                test_token(999),
                CommitSeq::new(10),
                CommitSeq::new(20),
                &summary,
            )
            .into_iter()
            .map(|writer| writer.token)
            .collect::<Vec<_>>();
        candidates.sort_by_key(|token| token.id.get());

        assert_eq!(candidates, vec![relevant, global]);
    }

    #[test]
    fn test_committed_reader_candidates_skip_disjoint_point_keys_on_same_root() {
        let mut registry = ConcurrentRegistry::new();
        let global = test_token(212);

        registry.committed_readers.push(CommittedReaderInfo {
            token: test_token(211),
            begin_seq: CommitSeq::new(5),
            commit_seq: CommitSeq::new(12),
            had_in_rw: false,
            keys: vec![WitnessKey::for_cell_read(
                test_page(70),
                test_page(701),
                b"key-b",
            )],
        });
        registry.index_committed_reader(0);
        registry.committed_readers.push(CommittedReaderInfo {
            token: global,
            begin_seq: CommitSeq::new(5),
            commit_seq: CommitSeq::new(12),
            had_in_rw: false,
            keys: vec![WitnessKey::Custom {
                namespace: 70,
                bytes: b"global-reader".to_vec(),
            }],
        });
        registry.index_committed_reader(1);

        let (write_cell, write_page) =
            WitnessKey::for_point_write(test_page(70), b"key-a", test_page(700));
        let write_keys: [WitnessKey; 2] = (write_cell, write_page).into();
        let mut candidates = registry
            .committed_reader_candidates(
                test_token(999),
                CommitSeq::new(10),
                CommitSeq::new(20),
                &summarize_witness_keys(write_keys.as_slice()),
            )
            .into_iter()
            .map(|reader| reader.token)
            .collect::<Vec<_>>();
        candidates.sort_by_key(|token| token.id.get());

        assert_eq!(candidates, vec![global]);
    }

    #[test]
    fn test_committed_writer_candidates_skip_disjoint_point_keys_on_same_root() {
        let mut registry = ConcurrentRegistry::new();
        let global = test_token(222);
        let (writer_cell, writer_page) =
            WitnessKey::for_point_write(test_page(71), b"key-b", test_page(711));

        registry.committed_writers.push(CommittedWriterInfo {
            token: test_token(221),
            commit_seq: CommitSeq::new(12),
            had_out_rw: false,
            keys: vec![writer_cell, writer_page],
        });
        registry.index_committed_writer(0);
        registry.committed_writers.push(CommittedWriterInfo {
            token: global,
            commit_seq: CommitSeq::new(12),
            had_out_rw: false,
            keys: vec![WitnessKey::Custom {
                namespace: 71,
                bytes: b"global-writer".to_vec(),
            }],
        });
        registry.index_committed_writer(1);

        let read_cell = WitnessKey::for_cell_read(test_page(71), test_page(710), b"key-a");
        let mut candidates = registry
            .committed_writer_candidates(
                test_token(999),
                CommitSeq::new(10),
                CommitSeq::new(20),
                &summarize_witness_keys(&[read_cell]),
            )
            .into_iter()
            .map(|writer| writer.token)
            .collect::<Vec<_>>();
        candidates.sort_by_key(|token| token.id.get());

        assert_eq!(candidates, vec![global]);
    }

    #[test]
    fn test_handle_view_custom_overlap_respects_global_witnesses() {
        let mut handle = ConcurrentHandle::new(test_snapshot(10), test_token(301));
        handle.record_read_witness(WitnessKey::Custom {
            namespace: 3,
            bytes: b"read-global".to_vec(),
        });
        handle.record_write_witness(WitnessKey::Custom {
            namespace: 4,
            bytes: b"write-global".to_vec(),
        });
        let view = HandleView::new(&handle);

        assert!(view.check_read_overlap(&WitnessKey::Custom {
            namespace: 5,
            bytes: b"candidate".to_vec(),
        }));
        assert!(view.check_write_overlap(&WitnessKey::Custom {
            namespace: 6,
            bytes: b"candidate".to_vec(),
        }));
    }

    #[test]
    fn test_handle_view_disjoint_cell_witnesses_do_not_overlap_on_same_root() {
        let root = test_page(41);
        let leaf_a = test_page(410);
        let leaf_b = test_page(411);

        let mut handle = ConcurrentHandle::new(test_snapshot(10), test_token(3011));
        handle.record_read_witness(WitnessKey::for_cell_read(root, leaf_a, b"key-a"));
        let view = HandleView::new(&handle);

        assert!(view.check_read_overlap(&WitnessKey::for_cell_read(root, leaf_a, b"key-a")));
        assert!(
            !view.check_read_overlap(&WitnessKey::for_cell_read(root, leaf_b, b"key-b")),
            "same-root point witnesses must not overlap when the key tag differs"
        );
    }

    #[test]
    fn test_page_write_witness_preserves_overlap_without_claiming_lock() {
        let page = test_page(17);
        let mut handle = ConcurrentHandle::new(test_snapshot(10), test_token(302));
        handle.record_write_witness(WitnessKey::Page(page));

        assert!(
            !handle.holds_page_lock(page),
            "page-scoped SSI witnesses must not claim physical page-lock ownership"
        );
        assert!(handle.check_write_overlap(&WitnessKey::Page(page)));
        assert!(!handle.check_write_overlap(&WitnessKey::Page(test_page(18))));
    }

    #[test]
    fn test_active_edge_discovery_index_indexes_page_write_witnesses_without_page_state() {
        let mut page_writer = ConcurrentHandle::new(test_snapshot(10), test_token(303));
        page_writer.record_write_witness(WitnessKey::Page(test_page(7)));
        assert!(
            !page_writer.holds_page_lock(test_page(7)),
            "page-scoped SSI witnesses must not manufacture a held page lock"
        );

        let mut global_writer = ConcurrentHandle::new(test_snapshot(10), test_token(304));
        global_writer.record_write_witness(WitnessKey::Custom {
            namespace: 8,
            bytes: b"global-writer".to_vec(),
        });

        let mut unrelated_writer = ConcurrentHandle::new(test_snapshot(10), test_token(305));
        unrelated_writer.record_write_witness(WitnessKey::Page(test_page(9)));

        let views = vec![
            HandleView::new(&page_writer),
            HandleView::new(&global_writer),
            HandleView::new(&unrelated_writer),
        ];
        let index = ActiveEdgeDiscoveryIndex::build(&views);
        let read_summary = summarize_witness_keys(&[WitnessKey::Page(test_page(7))]);

        let mut candidate_tokens = index
            .outgoing_candidate_refs(
                &views,
                test_token(399),
                CommitSeq::new(10),
                CommitSeq::new(20),
                &read_summary,
            )
            .into_iter()
            .map(|candidate| candidate.token())
            .collect::<Vec<_>>();
        candidate_tokens.sort_by_key(|token| token.id.get());

        assert_eq!(candidate_tokens, vec![test_token(303), test_token(304)]);
    }

    #[test]
    fn test_active_edge_discovery_index_keeps_tracked_write_pages_without_write_index() {
        let mut tracked_writer = ConcurrentHandle::new(test_snapshot(10), test_token(306));
        tracked_writer
            .ensure_page_state(test_page(11))
            .is_conflict_only = true;
        assert!(tracked_writer.tracks_write_conflict_page(test_page(11)));
        assert!(tracked_writer.write_index.is_empty());

        let views = vec![HandleView::new(&tracked_writer)];
        let index = ActiveEdgeDiscoveryIndex::build(&views);
        let read_summary = summarize_witness_keys(&[WitnessKey::Page(test_page(11))]);

        let candidate_tokens = index
            .outgoing_candidate_refs(
                &views,
                test_token(399),
                CommitSeq::new(10),
                CommitSeq::new(20),
                &read_summary,
            )
            .into_iter()
            .map(|candidate| candidate.token())
            .collect::<Vec<_>>();

        assert_eq!(candidate_tokens, vec![test_token(306)]);
    }

    #[test]
    fn test_active_edge_discovery_index_skips_disjoint_point_keys_on_same_root() {
        let root = test_page(51);
        let leaf_a = test_page(510);
        let leaf_b = test_page(511);

        let mut reader = ConcurrentHandle::new(test_snapshot(10), test_token(307));
        reader.record_read_witness(WitnessKey::for_cell_read(root, leaf_a, b"key-a"));

        let mut writer = ConcurrentHandle::new(test_snapshot(10), test_token(308));
        let (write_cell, write_page) = WitnessKey::for_point_write(root, b"key-b", leaf_b);
        writer.record_write_witness(write_cell.clone());
        writer.record_write_witness(write_page.clone());

        let views = vec![HandleView::new(&reader), HandleView::new(&writer)];
        let index = ActiveEdgeDiscoveryIndex::build(&views);
        let write_summary = summarize_witness_keys(&[write_cell.clone(), write_page.clone()]);
        let read_summary =
            summarize_witness_keys(&[WitnessKey::for_cell_read(root, leaf_a, b"key-a")]);

        let incoming = index
            .incoming_candidate_refs(
                &views,
                test_token(399),
                CommitSeq::new(10),
                CommitSeq::new(20),
                &write_summary,
            )
            .into_iter()
            .map(|candidate| candidate.token())
            .collect::<Vec<_>>();
        let outgoing = index
            .outgoing_candidate_refs(
                &views,
                test_token(399),
                CommitSeq::new(10),
                CommitSeq::new(20),
                &read_summary,
            )
            .into_iter()
            .map(|candidate| candidate.token())
            .collect::<Vec<_>>();

        assert!(incoming.is_empty());
        assert!(outgoing.is_empty());
    }

    #[test]
    fn test_active_edge_discovery_index_uses_presence_flags_without_materialized_keys() {
        let mut page_reader = ConcurrentHandle::new(test_snapshot(10), test_token(311));
        page_reader.record_read(test_page(7));

        let mut global_reader = ConcurrentHandle::new(test_snapshot(10), test_token(312));
        global_reader.record_read_witness(WitnessKey::Custom {
            namespace: 7,
            bytes: b"global-reader".to_vec(),
        });

        let mut unrelated_reader = ConcurrentHandle::new(test_snapshot(10), test_token(313));
        unrelated_reader.record_read(test_page(9));

        let views = vec![
            HandleView::new(&page_reader),
            HandleView::new(&global_reader),
            HandleView::new(&unrelated_reader),
        ];
        let index = ActiveEdgeDiscoveryIndex::build(&views);
        let write_summary = summarize_witness_keys(&[WitnessKey::Page(test_page(7))]);

        let mut candidate_tokens = index
            .incoming_candidate_refs(
                &views,
                test_token(399),
                CommitSeq::new(10),
                CommitSeq::new(20),
                &write_summary,
            )
            .into_iter()
            .map(|candidate| candidate.token())
            .collect::<Vec<_>>();
        candidate_tokens.sort_by_key(|token| token.id.get());

        assert_eq!(candidate_tokens, vec![test_token(311), test_token(312)]);
    }

    #[test]
    fn test_prepare_materializes_dro_decision_for_edgeful_commit() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        {
            let mut h1 = registry.get_mut(s1).unwrap();
            h1.record_read(test_page(20));
            concurrent_write_page(&mut h1, &lock_table, s1, test_page(10), test_data()).unwrap();
        }
        {
            let mut h2 = registry.get_mut(s2).unwrap();
            h2.record_read(test_page(30));
            concurrent_write_page(&mut h2, &lock_table, s2, test_page(20), test_data()).unwrap();
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
    fn test_conflict_heat_adversarial_overlap_workload_records_page_heat_graph() {
        let _guard = enable_conflict_heat_telemetry_for_test();
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let s3 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        {
            let mut h1 = registry.get_mut(s1).unwrap();
            h1.record_read(test_page(10));
            concurrent_write_page(&mut h1, &lock_table, s1, test_page(20), test_data()).unwrap();
        }
        {
            let mut h2 = registry.get_mut(s2).unwrap();
            h2.record_read(test_page(20));
            concurrent_write_page(&mut h2, &lock_table, s2, test_page(10), test_data()).unwrap();
        }
        {
            let mut h3 = registry.get_mut(s3).unwrap();
            h3.record_read(test_page(20));
            concurrent_write_page(&mut h3, &lock_table, s3, test_page(30), test_data()).unwrap();
        }

        let result = prepare_concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s1,
            CommitSeq::new(11),
        );

        assert!(matches!(
            result,
            Err((
                MvccError::BusySnapshot,
                FcwResult::Abort {
                    reason: SsiAbortReason::Pivot
                }
            ))
        ));

        let snapshot = conflict_heat_telemetry_snapshot();
        assert_eq!(snapshot.schema_version, "fsqlite.mvcc.conflict_heat.v1");
        assert_eq!(snapshot.observations_total, 1);
        assert_eq!(snapshot.max_writer_overlap_estimate, 2);
        assert_eq!(snapshot.max_conflict_heat, 3);
        assert_eq!(snapshot.overlap_edges.len(), 3);

        let hot_page = snapshot
            .top_pages
            .iter()
            .find(|page| page.page_no == 20)
            .expect("page 20 should be the incoming overlap hotspot");
        assert_eq!(hot_page.conflict_heat, 2);
        assert_eq!(hot_page.writer_overlap_estimate, 2);

        let outgoing_page = snapshot
            .top_pages
            .iter()
            .find(|page| page.page_no == 10)
            .expect("page 10 should record the outgoing overlap");
        assert_eq!(outgoing_page.conflict_heat, 1);

        let split_advice =
            fsqlite_btree::conflict_topology_split_advice(test_page(20), "right_edge", 6_500);
        assert!(split_advice.topology_hot);
        assert!(split_advice.applied);
        assert_eq!(split_advice.effective_target_left_basis_points, 8_000);
        assert_eq!(split_advice.split_reason(), "mvcc_conflict_heat_hot_page");
    }

    #[test]
    fn test_prepare_uses_candidate_free_fast_path_for_disjoint_point_inserts() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();
        let root = test_page(61);
        let leaf_a = test_page(610);
        let leaf_b = test_page(611);

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        {
            let mut h1 = registry.get_mut(s1).unwrap();
            h1.record_read_witness(WitnessKey::for_cell_read(root, leaf_a, b"key-a"));
            let (write_cell, _) = WitnessKey::for_point_write(root, b"key-a", leaf_a);
            h1.record_write_witness(write_cell);
            concurrent_write_page(&mut h1, &lock_table, s1, leaf_a, test_data()).unwrap();
        }

        {
            let mut h2 = registry.get_mut(s2).unwrap();
            h2.record_read_witness(WitnessKey::for_cell_read(root, leaf_b, b"key-b"));
            let (write_cell, _) = WitnessKey::for_point_write(root, b"key-b", leaf_b);
            h2.record_write_witness(write_cell);
            concurrent_write_page(&mut h2, &lock_table, s2, leaf_b, test_data()).unwrap();
        }

        let prepared = prepare_concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s1,
            CommitSeq::new(11),
        )
        .expect("disjoint point inserts should prepare cleanly");

        assert!(prepared.used_candidate_free_prepare_fast_path());
        assert!(!prepared.has_in_rw());
        assert!(!prepared.has_out_rw());
        assert!(prepared.conflicting_txns().is_empty());
    }

    #[test]
    fn test_prepare_uses_candidate_free_fast_path_with_disjoint_committed_history() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();
        let root = test_page(62);
        let leaf_a = test_page(620);
        let leaf_b = test_page(621);

        registry.committed_readers.push(CommittedReaderInfo {
            token: test_token(631),
            begin_seq: CommitSeq::new(5),
            commit_seq: CommitSeq::new(11),
            had_in_rw: false,
            keys: vec![WitnessKey::for_cell_read(root, leaf_b, b"key-b")],
        });
        registry.index_committed_reader(0);
        let (writer_cell, writer_page) = WitnessKey::for_point_write(root, b"key-b", leaf_b);
        registry.committed_writers.push(CommittedWriterInfo {
            token: test_token(632),
            commit_seq: CommitSeq::new(11),
            had_out_rw: false,
            keys: vec![writer_cell, writer_page],
        });
        registry.index_committed_writer(0);

        let session_id = registry.begin_concurrent(test_snapshot(10)).unwrap();
        {
            let mut handle = registry.get_mut(session_id).unwrap();
            handle.record_read_witness(WitnessKey::for_cell_read(root, leaf_a, b"key-a"));
            let (write_cell, _) = WitnessKey::for_point_write(root, b"key-a", leaf_a);
            handle.record_write_witness(write_cell);
            concurrent_write_page(&mut handle, &lock_table, session_id, leaf_a, test_data())
                .unwrap();
        }

        let prepared = prepare_concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            session_id,
            CommitSeq::new(12),
        )
        .expect("disjoint committed history should not force validation");

        assert!(prepared.used_candidate_free_prepare_fast_path());
        assert!(!prepared.has_in_rw());
        assert!(!prepared.has_out_rw());
        assert!(prepared.conflicting_txns().is_empty());
    }

    #[test]
    fn test_prepare_skips_dro_decision_for_edge_free_commit() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        {
            let mut h1 = registry.get_mut(s1).unwrap();
            concurrent_write_page(&mut h1, &lock_table, s1, test_page(5), test_data()).unwrap();
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

    #[test]
    fn test_zero_witness_commit_stays_candidate_free_with_active_peer() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        let prepared = prepare_concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s1,
            CommitSeq::new(11),
        )
        .expect("zero-witness write should not need SSI candidate scans");

        assert!(prepared.used_candidate_free_prepare_fast_path());
        assert!(prepared.read_keys().is_empty());
        assert!(prepared.write_keys().is_empty());

        finalize_prepared_concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            &prepared,
            CommitSeq::new(11),
        );

        assert!(
            registry.committed_readers.is_empty() && registry.committed_writers.is_empty(),
            "zero-witness commits must not add empty SSI history rows"
        );
        assert!(registry.get(s2).is_some_and(|peer| peer.is_active()));
    }

    #[test]
    fn test_prepare_captures_held_lock_pages_separately_from_write_set() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let session_id = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let txn_id = TxnId::new(session_id).unwrap();
        let extra_lock_page = test_page(99);

        {
            let mut handle = registry.get_mut(session_id).unwrap();
            concurrent_write_page(
                &mut handle,
                &lock_table,
                session_id,
                test_page(5),
                test_data(),
            )
            .unwrap();
            lock_table.try_acquire(extra_lock_page, txn_id).unwrap();
            handle.ensure_page_state(extra_lock_page).held_lock = true;
        }

        let prepared = prepare_concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            session_id,
            CommitSeq::new(11),
        )
        .expect("prepare should succeed");

        assert_eq!(prepared.write_set_pages(), &[test_page(5)]);
        assert_eq!(prepared.held_lock_pages(), &[test_page(5), extra_lock_page]);
    }

    #[test]
    fn test_finalize_releases_prepared_held_lock_pages_when_session_missing() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let session_id = registry.begin_concurrent(test_snapshot(10)).unwrap();
        let txn_id = TxnId::new(session_id).unwrap();
        let extra_lock_page = test_page(101);

        {
            let mut handle = registry.get_mut(session_id).unwrap();
            concurrent_write_page(
                &mut handle,
                &lock_table,
                session_id,
                test_page(7),
                test_data(),
            )
            .unwrap();
            lock_table.try_acquire(extra_lock_page, txn_id).unwrap();
            handle.ensure_page_state(extra_lock_page).held_lock = true;
        }

        let prepared = prepare_concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            session_id,
            CommitSeq::new(11),
        )
        .expect("prepare should succeed");

        let shared_handle = registry.remove(session_id).expect("active handle");
        registry.recycle_handle(shared_handle);

        let other_txn = TxnId::new(999).unwrap();
        assert!(
            lock_table.try_acquire(extra_lock_page, other_txn).is_err(),
            "extra held lock should still be owned before finalize"
        );

        finalize_prepared_concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            &prepared,
            CommitSeq::new(11),
        );

        assert!(
            lock_table.try_acquire(extra_lock_page, other_txn).is_ok(),
            "finalize should release held locks captured at prepare time"
        );
        assert!(
            lock_table.try_acquire(test_page(7), other_txn).is_ok(),
            "finalize should also release normal write-set locks"
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
        {
            let mut handle = registry.get_mut(s1).expect("handle");
            concurrent_abort(&mut handle, &lock_table, s1);
        }

        // Write should fail on aborted handle.
        {
            let mut handle = registry.get_mut(s1).expect("handle");
            let result =
                concurrent_write_page(&mut handle, &lock_table, s1, test_page(1), test_data());
            assert_eq!(result.unwrap_err(), MvccError::InvalidState);
        }

        // Savepoint should fail on aborted handle.
        {
            let handle = registry.get(s1).expect("handle");
            let result = concurrent_savepoint(&handle, "sp1");
            assert_eq!(result.unwrap_err(), MvccError::InvalidState);
        }
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

        let mut handle = registry.get_mut(s1).expect("handle");
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
            let mut h1 = registry.get_mut(s1).unwrap();
            h1.record_read(test_page(5));
            concurrent_write_page(&mut h1, &lock_table, s1, test_page(10), test_data()).unwrap();
            // B
        }

        // T2 reads and writes.
        {
            let mut h2 = registry.get_mut(s2).unwrap();
            h2.record_read(test_page(20));
            concurrent_write_page(&mut h2, &lock_table, s2, test_page(30), test_data()).unwrap();
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
            let mut h1 = registry.get_mut(s1).unwrap();
            h1.record_read(test_page(5));
            concurrent_write_page(&mut h1, &lock_table, s1, test_page(10), test_data()).unwrap();
        }
        {
            let mut h2 = registry.get_mut(s2).unwrap();
            h2.record_read(test_page(10));
            concurrent_write_page(&mut h2, &lock_table, s2, test_page(20), test_data()).unwrap();
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
            let mut h1 = registry.get_mut(s1).unwrap();
            h1.record_read(test_page(5)); // A
            concurrent_write_page(&mut h1, &lock_table, s1, test_page(10), test_data()).unwrap();
            // B
        }

        // T2: reads page 10 (B), writes page 5 (A).
        {
            let mut h2 = registry.get_mut(s2).unwrap();
            h2.record_read(test_page(10)); // B
            concurrent_write_page(&mut h2, &lock_table, s2, test_page(5), test_data()).unwrap();
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

        let mut h1 = registry.get_mut(s1).unwrap();
        concurrent_write_page(&mut h1, &lock_table, s1, test_page(5), test_data()).unwrap();

        // Manually mark for abort.
        h1.marked_for_abort.set(true);

        // Commit should fail.
        let result = concurrent_commit(&mut h1, &commit_index, &lock_table, s1, CommitSeq::new(11));
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

        let mut h1 = registry.get_mut(s1).unwrap();
        concurrent_write_page(&mut h1, &lock_table, s1, test_page(5), test_data()).unwrap();

        // Set only incoming edge (no outgoing).
        h1.has_in_rw.set(true);
        h1.has_out_rw.set(false);

        // Commit should succeed (not a pivot).
        let result = concurrent_commit(&mut h1, &commit_index, &lock_table, s1, CommitSeq::new(11));
        assert!(result.is_ok(), "only incoming edge should allow commit");
    }

    // Test 18: SSI - only outgoing edge allows commit.
    #[test]
    fn test_ssi_only_outgoing_edge_commits() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        let mut h1 = registry.get_mut(s1).unwrap();
        concurrent_write_page(&mut h1, &lock_table, s1, test_page(5), test_data()).unwrap();

        // Set only outgoing edge (no incoming).
        h1.has_in_rw.set(false);
        h1.has_out_rw.set(true);

        // Commit should succeed (not a pivot).
        let result = concurrent_commit(&mut h1, &commit_index, &lock_table, s1, CommitSeq::new(11));
        assert!(result.is_ok(), "only outgoing edge should allow commit");
    }

    // Test 19: SSI - both edges trigger abort.
    #[test]
    fn test_ssi_both_edges_aborts() {
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();

        let mut h1 = registry.get_mut(s1).unwrap();
        concurrent_write_page(&mut h1, &lock_table, s1, test_page(5), test_data()).unwrap();

        // Set both edges → dangerous structure.
        h1.has_in_rw.set(true);
        h1.has_out_rw.set(true);

        // Commit should fail (pivot).
        let result = concurrent_commit(&mut h1, &commit_index, &lock_table, s1, CommitSeq::new(11));
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

        let mut h1 = registry.get_mut(s1).unwrap();
        h1.record_read(test_page(5));
        h1.record_read(test_page(10));
        concurrent_write_page(&mut h1, &lock_table, s1, test_page(15), test_data()).unwrap();
        concurrent_write_page(&mut h1, &lock_table, s1, test_page(20), test_data()).unwrap();

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
            let mut h1 = registry.get_mut(s1).unwrap();
            h1.record_read(test_page(10));
            h1.record_read(test_page(20));
            concurrent_write_page(&mut h1, &lock_table, s1, test_page(30), test_data()).unwrap();
        }
        // T2 operations.
        {
            let mut h2 = registry.get_mut(s2).unwrap();
            h2.record_read(test_page(50));
            concurrent_write_page(&mut h2, &lock_table, s2, test_page(10), test_data()).unwrap();
        }
        // T3 operations.
        {
            let mut h3 = registry.get_mut(s3).unwrap();
            h3.record_read(test_page(30));
            concurrent_write_page(&mut h3, &lock_table, s3, test_page(40), test_data()).unwrap();
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
            let mut h1 = registry.get_mut(s1).unwrap();
            h1.record_read(test_page(10));
            h1.record_read(test_page(20));
            concurrent_write_page(&mut h1, &lock_table, s1, test_page(30), test_data()).unwrap();
        }
        // T2 operations.
        {
            let mut h2 = registry.get_mut(s2).unwrap();
            h2.record_read(test_page(50));
            concurrent_write_page(&mut h2, &lock_table, s2, test_page(10), test_data()).unwrap();
        }
        // T3 operations.
        {
            let mut h3 = registry.get_mut(s3).unwrap();
            h3.record_read(test_page(30));
            concurrent_write_page(&mut h3, &lock_table, s3, test_page(40), test_data()).unwrap();
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
            let mut h1 = registry.get_mut(s1).unwrap();
            h1.record_read(test_page(100));
            concurrent_write_page(&mut h1, &lock_table, s1, test_page(200), test_data()).unwrap();
        }
        {
            let mut h2 = registry.get_mut(s2).unwrap();
            h2.record_read(test_page(200));
            concurrent_write_page(&mut h2, &lock_table, s2, test_page(300), test_data()).unwrap();
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
            let mut h1 = registry.get_mut(s1).unwrap();
            concurrent_write_page(&mut h1, &lock_table, s1, test_page(42), test_data()).unwrap();
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
            let mut h2 = registry.get_mut(s2).unwrap();
            concurrent_write_page(&mut h2, &lock_table, s2, test_page(42), test_data()).unwrap();
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
            let mut winner_handle = registry.get_mut(winner_session).unwrap();
            concurrent_write_page(
                &mut winner_handle,
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
            let mut loser_handle = registry.get_mut(loser_session).unwrap();
            concurrent_write_page(
                &mut loser_handle,
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
            .find(|entry| same_txn_identity(entry.token, winner_token))
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
        let t1_token = registry.get(s1).unwrap().txn_token();

        // T1: reads B (20), writes A (10)
        {
            let mut h1 = registry.get_mut(s1).unwrap();
            h1.record_read(test_page(20));
            concurrent_write_page(&mut h1, &lock_table, s1, test_page(10), test_data()).unwrap();
        }

        // T2: reads C (30), writes B (20)
        {
            let mut h2 = registry.get_mut(s2).unwrap();
            h2.record_read(test_page(30));
            concurrent_write_page(&mut h2, &lock_table, s2, test_page(20), test_data()).unwrap();
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
            .find(|entry| same_txn_identity(entry.token, t1_token))
            .expect("T1 writer history should be present");
        assert!(
            t1_writer.had_out_rw,
            "T1 should be recorded with had_out_rw"
        );

        // T3 now performs its workload using the earlier snapshot.
        {
            let mut h3 = registry.get_mut(s3).unwrap();
            h3.record_read(test_page(10));
            concurrent_write_page(&mut h3, &lock_table, s3, test_page(40), test_data()).unwrap();
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

    // -----------------------------------------------------------------------
    // E3 (bd-wwqen Track E): Metadata-exempt pages skip conflict detection.
    // -----------------------------------------------------------------------

    #[test]
    fn test_metadata_exempt_skips_conflict_detection() {
        // Two sessions both write to page 1 (metadata) and different data pages.
        // Without metadata exemption, s2 would conflict on page 1.
        // With exemption, only the data pages are conflict-checked.
        let lock_table = InProcessPageLockTable::new();
        let commit_index = CommitIndex::new();
        let mut registry = ConcurrentRegistry::new();

        // s1 writes page 1 (metadata, exempt) and page 5 (data).
        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        {
            let mut h1 = registry.get_mut(s1).unwrap();
            concurrent_write_metadata_page(&mut h1, &lock_table, s1, test_page(1), test_data())
                .unwrap();
            concurrent_write_page(&mut h1, &lock_table, s1, test_page(5), test_data()).unwrap();
            assert!(concurrent_is_metadata_exempt(&h1, test_page(1)));
            assert!(!concurrent_is_metadata_exempt(&h1, test_page(5)));
        }

        // s1 commits.
        {
            let mut h1 = registry.get_mut(s1).expect("handle s1");
            concurrent_commit(&mut h1, &commit_index, &lock_table, s1, CommitSeq::new(11))
                .expect("s1 commits");
        }

        // s2 also writes page 1 (metadata, exempt) and page 10 (different data page).
        let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        {
            let mut h2 = registry.get_mut(s2).unwrap();
            concurrent_write_metadata_page(&mut h2, &lock_table, s2, test_page(1), test_data())
                .unwrap();
            concurrent_write_page(&mut h2, &lock_table, s2, test_page(10), test_data()).unwrap();
        }

        // s2 should NOT conflict on page 1 (exempt), only page 10 is checked.
        // Since page 10 wasn't written by s1, s2 should commit successfully.
        {
            let mut h2 = registry.get_mut(s2).expect("handle s2");
            let result =
                concurrent_commit(&mut h2, &commit_index, &lock_table, s2, CommitSeq::new(12));
            assert!(
                result.is_ok(),
                "s2 should commit: page 1 is metadata-exempt"
            );
        }
    }

    #[test]
    fn test_metadata_exempt_manual_marking() {
        let lock_table = InProcessPageLockTable::new();
        let mut registry = ConcurrentRegistry::new();

        let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
        {
            let mut h1 = registry.get_mut(s1).unwrap();
            // Write normally first.
            concurrent_write_page(&mut h1, &lock_table, s1, test_page(1), test_data()).unwrap();
            assert!(!concurrent_is_metadata_exempt(&h1, test_page(1)));

            // Mark as metadata-exempt.
            concurrent_mark_metadata_exempt(&mut h1, test_page(1));
            assert!(concurrent_is_metadata_exempt(&h1, test_page(1)));

            // Verify it no longer tracks write conflict.
            assert!(
                !h1.tracks_write_conflict_page(test_page(1)),
                "metadata-exempt pages should not track write conflicts"
            );
        }
    }

    /// Microbench for `ActiveEdgeDiscoveryIndex::build`. Constructs N handle
    /// views with M disjoint read/write pages each and times the build loop.
    /// Reports ns/build averaged over trials. Not a correctness test; ignored
    /// by default.
    ///
    /// Run with:
    /// `cargo test -p fsqlite-mvcc --lib --release -- \
    ///    bench_active_edge_index_build --ignored --nocapture`
    #[test]
    #[ignore = "microbench — run manually"]
    fn bench_active_edge_index_build() {
        use std::cell::Cell;
        use std::time::Instant;

        use super::{ActiveEdgeDiscoveryIndex, HandleView, HandleViewFlags};

        fn make_view(token_id: u64, page_base: u32, pages_per_view: u32) -> HandleView {
            let mut read_pages: Vec<PageNumber> = (0..pages_per_view)
                .map(|i| test_page(page_base + i))
                .collect();
            read_pages.sort_unstable();
            let mut write_pages: Vec<PageNumber> = (0..pages_per_view)
                .map(|i| test_page(page_base + pages_per_view + i))
                .collect();
            write_pages.sort_unstable();
            HandleView {
                token: test_token(token_id),
                begin_seq: CommitSeq::new(1),
                flags: HandleViewFlags(HandleViewFlags::ACTIVE),
                read_pages: read_pages.clone(),
                read_page_witness_pages: read_pages,
                read_exact_cells: Vec::new(),
                write_pages: write_pages.clone(),
                write_page_witness_pages: write_pages,
                write_exact_cells: Vec::new(),
                has_in_rw: Cell::new(false),
                has_out_rw: Cell::new(false),
            }
        }

        // MT 16t with ~10 pages-per-txn is the profile scenario the hasher
        // swap targets.
        const VIEWS: usize = 16;
        const PAGES_PER_VIEW: u32 = 10;
        const BUILDS_PER_TRIAL: u32 = 20_000;
        const TRIALS: usize = 7;

        let views: Vec<HandleView> = (0..VIEWS)
            .map(|i| {
                make_view(
                    (i as u64) + 1,
                    (i as u32) * PAGES_PER_VIEW * 2 + 1,
                    PAGES_PER_VIEW,
                )
            })
            .collect();

        // Warm-up.
        for _ in 0..BUILDS_PER_TRIAL {
            std::hint::black_box(ActiveEdgeDiscoveryIndex::build(&views));
        }

        let mut samples = Vec::with_capacity(TRIALS);
        for _ in 0..TRIALS {
            let start = Instant::now();
            for _ in 0..BUILDS_PER_TRIAL {
                std::hint::black_box(ActiveEdgeDiscoveryIndex::build(&views));
            }
            let elapsed = start.elapsed();
            let ns_per_build = elapsed.as_nanos() as f64 / f64::from(BUILDS_PER_TRIAL);
            eprintln!("  trial: {ns_per_build:.0} ns/build ({elapsed:?} total)");
            samples.push(ns_per_build);
        }
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = samples[TRIALS / 2];
        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
        eprintln!(
            "bench_active_edge_index_build: median={median:.0} ns/build, mean={mean:.0} \
             ns/build (n={TRIALS}, views={VIEWS}, pages_per_view={PAGES_PER_VIEW})"
        );
    }

    /// Microbench comparing the VDBE read hot-path's current double
    /// `page_states` HashMap lookup (`concurrent_page_is_freed` +
    /// `concurrent_read_page`) to the compound `concurrent_page_read_state`
    /// that does a single lookup.
    ///
    /// Not a correctness test; ignored by default. Run with:
    /// `cargo test -p fsqlite-mvcc --lib --release -- \
    ///    bench_read_path_page_state_lookup --ignored --nocapture`
    #[test]
    #[ignore = "microbench — run manually"]
    fn bench_read_path_page_state_lookup() {
        use std::time::Instant;

        use super::{concurrent_page_is_freed, concurrent_page_read_state, concurrent_read_page};

        const STAGED_PAGES: u32 = 64;
        const READS_PER_TRIAL: u32 = 200_000;
        const TRIALS: usize = 7;

        // Populate the handle with STAGED_PAGES staged writes so
        // `page_states` is hot and realistic (mix of hits + misses when the
        // read probes pages outside the staged set).
        let lock_table = InProcessPageLockTable::new();
        let mut registry = ConcurrentRegistry::new();
        let session_id = registry.begin_concurrent(test_snapshot(10)).unwrap();
        {
            let mut handle = registry.get_mut(session_id).unwrap();
            for i in 1..=STAGED_PAGES {
                concurrent_write_page(
                    &mut handle,
                    &lock_table,
                    session_id,
                    test_page(i),
                    test_data(),
                )
                .unwrap();
            }
        }

        fn run_trial<F: Fn(PageNumber) -> (bool, bool)>(reads: u32, probe: F) -> f64 {
            let start = Instant::now();
            for i in 0..reads {
                // Alternate hits (pages 1..=STAGED_PAGES) and misses
                // (pages STAGED_PAGES+1..2*STAGED_PAGES) to exercise both
                // the Some/None branches of `page_states.get`.
                let page = test_page((i % (STAGED_PAGES * 2)) + 1);
                std::hint::black_box(probe(page));
            }
            start.elapsed().as_nanos() as f64 / f64::from(reads)
        }

        let handle = registry.get(session_id).unwrap();

        // Baseline: the current engine.rs pattern — two independent
        // `page_states` probes per read.
        let baseline = |page: PageNumber| -> (bool, bool) {
            let is_freed = concurrent_page_is_freed(&handle, page);
            let hit = concurrent_read_page(&handle, page).is_some();
            (is_freed, hit)
        };
        // Optimised: single compound probe returning both flags.
        let optimised = |page: PageNumber| -> (bool, bool) {
            let (is_freed, staged) = concurrent_page_read_state(&handle, page);
            (is_freed, staged.is_some())
        };

        // Warm-up.
        run_trial(READS_PER_TRIAL, baseline);
        run_trial(READS_PER_TRIAL, optimised);

        let mut baseline_samples = Vec::with_capacity(TRIALS);
        let mut optimised_samples = Vec::with_capacity(TRIALS);
        for _ in 0..TRIALS {
            let b = run_trial(READS_PER_TRIAL, baseline);
            let o = run_trial(READS_PER_TRIAL, optimised);
            eprintln!("  baseline: {b:.1} ns/probe   optimised: {o:.1} ns/probe");
            baseline_samples.push(b);
            optimised_samples.push(o);
        }
        baseline_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        optimised_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let b_med = baseline_samples[TRIALS / 2];
        let o_med = optimised_samples[TRIALS / 2];
        let delta_pct = (o_med - b_med) / b_med * 100.0;
        eprintln!(
            "bench_read_path_page_state_lookup: baseline median={b_med:.1} ns/probe; \
             optimised median={o_med:.1} ns/probe; delta={delta_pct:+.1}% \
             (n={TRIALS} trials, {READS_PER_TRIAL} reads/trial, staged={STAGED_PAGES})"
        );
    }

    /// Microbench for the new `all_readers.is_empty()` short-circuit in
    /// `ActiveEdgeDiscoveryIndex::incoming_candidate_refs`. Builds an index
    /// from write-only views (readers vec empty) and a committer's write
    /// summary that covers N pages, then times repeated
    /// `incoming_candidate_refs` calls. Without the short-circuit each
    /// call clones `self.readers_with_global_keys`, probes
    /// `readers_by_page` / `readers_by_page_witness` N times each, and
    /// runs sort+dedup; with the short-circuit each call is a single
    /// atomic-length check.
    #[test]
    #[ignore = "microbench — run manually"]
    fn bench_incoming_candidate_refs_empty_readers() {
        use std::cell::Cell;
        use std::time::Instant;

        use super::{
            ActiveEdgeDiscoveryIndex, HandleView, HandleViewFlags, summarize_witness_keys,
        };

        fn make_writer_view(token_id: u64, pages: &[u32]) -> HandleView {
            let mut write_pages: Vec<PageNumber> = pages.iter().copied().map(test_page).collect();
            write_pages.sort_unstable();
            HandleView {
                token: test_token(token_id),
                begin_seq: CommitSeq::new(1),
                flags: HandleViewFlags(HandleViewFlags::ACTIVE),
                // Read side intentionally empty — all_readers stays empty
                // so the short-circuit fires.
                read_pages: Vec::new(),
                read_page_witness_pages: Vec::new(),
                read_exact_cells: Vec::new(),
                write_pages: write_pages.clone(),
                write_page_witness_pages: write_pages,
                write_exact_cells: Vec::new(),
                has_in_rw: Cell::new(false),
                has_out_rw: Cell::new(false),
            }
        }

        // MT 16t shape again: 16 writer-only views, 10 pages each. A
        // committing txn writes 10 pages and asks for incoming edges.
        const VIEWS: usize = 16;
        const PAGES_PER_VIEW: u32 = 10;
        const CALLS_PER_TRIAL: u32 = 500_000;
        const TRIALS: usize = 7;

        let views: Vec<HandleView> = (0..VIEWS)
            .map(|i| {
                let base = (i as u32) * PAGES_PER_VIEW + 1;
                let pages: Vec<u32> = (base..base + PAGES_PER_VIEW).collect();
                make_writer_view((i as u64) + 1, &pages)
            })
            .collect();

        let index = ActiveEdgeDiscoveryIndex::build(&views);
        let committer_token = test_token(999);
        let committer_begin = CommitSeq::new(1);
        let committer_end = CommitSeq::new(2);
        let committer_write_keys: Vec<WitnessKey> = (1..=PAGES_PER_VIEW)
            .map(|i| WitnessKey::Page(test_page(i)))
            .collect();
        let write_summary = summarize_witness_keys(&committer_write_keys);

        // Warm-up.
        for _ in 0..CALLS_PER_TRIAL {
            std::hint::black_box(index.incoming_candidate_refs(
                &views,
                committer_token,
                committer_begin,
                committer_end,
                &write_summary,
            ));
        }

        let mut samples = Vec::with_capacity(TRIALS);
        for _ in 0..TRIALS {
            let start = Instant::now();
            for _ in 0..CALLS_PER_TRIAL {
                std::hint::black_box(index.incoming_candidate_refs(
                    &views,
                    committer_token,
                    committer_begin,
                    committer_end,
                    &write_summary,
                ));
            }
            let elapsed = start.elapsed();
            let ns_per_call = elapsed.as_nanos() as f64 / f64::from(CALLS_PER_TRIAL);
            eprintln!("  trial: {ns_per_call:.1} ns/call ({elapsed:?} total)");
            samples.push(ns_per_call);
        }
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = samples[TRIALS / 2];
        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
        eprintln!(
            "bench_incoming_candidate_refs_empty_readers: median={median:.1} ns/call, \
             mean={mean:.1} ns/call (n={TRIALS}, views={VIEWS}, write_pages={PAGES_PER_VIEW})"
        );
    }

    /// Microbench for `HandleView::new` on the typical MT writer shape:
    /// 10 read-witness pages + 10 staged write pages (which feed both the
    /// `tracked_write_conflict_pages_iter` chain and the write-witness
    /// summary). Six set-shaped fields are built per call; switching the
    /// representation from `HashSet<PageNumber>` (default `RandomState`)
    /// to sorted `Vec<PageNumber>` with `binary_search`-backed
    /// `check_*_overlap` removes per-insert SipHash13 cost and lets the
    /// summary's already-sorted Vecs move in directly.
    ///
    /// Run with:
    /// `cargo test -p fsqlite-mvcc --lib --release -- \
    ///    bench_handle_view_new_typical --ignored --nocapture`
    #[test]
    #[ignore = "microbench — run manually"]
    fn bench_handle_view_new_typical() {
        use std::time::Instant;

        use super::HandleView;

        const READ_PAGES: u32 = 10;
        const WRITE_PAGES: u32 = 10;
        const BUILDS_PER_TRIAL: u32 = 200_000;
        const TRIALS: usize = 7;

        let lock_table = InProcessPageLockTable::new();
        let mut registry = ConcurrentRegistry::new();
        let session_id = registry.begin_concurrent(test_snapshot(10)).unwrap();
        {
            let mut handle = registry.get_mut(session_id).unwrap();
            for i in 1..=READ_PAGES {
                handle.record_read_witness(WitnessKey::Page(test_page(i)));
            }
            for i in 1..=WRITE_PAGES {
                concurrent_write_page(
                    &mut handle,
                    &lock_table,
                    session_id,
                    test_page(READ_PAGES + i),
                    test_data(),
                )
                .unwrap();
            }
        }

        let handle = registry.get(session_id).unwrap();

        // Warm-up.
        for _ in 0..BUILDS_PER_TRIAL {
            std::hint::black_box(HandleView::new(&handle));
        }

        let mut samples = Vec::with_capacity(TRIALS);
        for _ in 0..TRIALS {
            let start = Instant::now();
            for _ in 0..BUILDS_PER_TRIAL {
                std::hint::black_box(HandleView::new(&handle));
            }
            let elapsed = start.elapsed();
            let ns_per_build = elapsed.as_nanos() as f64 / f64::from(BUILDS_PER_TRIAL);
            eprintln!("  trial: {ns_per_build:.1} ns/build ({elapsed:?} total)");
            samples.push(ns_per_build);
        }
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = samples[TRIALS / 2];
        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
        eprintln!(
            "bench_handle_view_new_typical: median={median:.1} ns/build, \
             mean={mean:.1} ns/build (n={TRIALS}, read={READ_PAGES}, write={WRITE_PAGES})"
        );
    }
}
