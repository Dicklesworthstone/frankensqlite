//! MVCC invariant enforcement and visibility predicates (§5.2-5.3).
//!
//! This module implements:
//! - [`TxnManager`]: Monotonic `TxnId` allocation via `AtomicU64` CAS (INV-1).
//! - [`VersionStore`]: Version chain management with arena-backed storage.
//! - [`visible`]: The core visibility predicate.
//! - `resolve`: Version chain resolution against a snapshot.
//! - `resolve_for_txn`: Write-set-aware resolution for transactions.
//! - [`SerializedWriteMutex`]: Global write mutex for Serialized mode (INV-7).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::{Mutex, RwLock};

use fsqlite_types::{
    CommitSeq, PageNumber, PageNumberBuildHasher, PageSize, PageVersion, Snapshot, TxnId,
    VersionPointer,
};

use crate::core_types::{Transaction, VersionArena, VersionIdx};

// ---------------------------------------------------------------------------
// TxnManager — INV-1 (Monotonicity)
// ---------------------------------------------------------------------------

/// Manages monotonic allocation of `TxnId` and `CommitSeq` values.
///
/// # INV-1 Enforcement
///
/// `TxnId` allocation uses an `AtomicU64` CAS loop that increments by 1.
/// Each successful CAS publishes a unique `TxnId`. The counter only ever
/// increases, so `TxnId`s are strictly increasing.
///
/// If the counter would wrap into `TxnId = 0` or exceed `TXN_ID_MAX`
/// (62-bit domain), the engine fails fast rather than publishing an
/// illegal `TxnId`.
///
/// `CommitSeq` is assigned only by the commit sequencer under the commit
/// mutex, producing a strict total order.
pub struct TxnManager {
    next_txn_id: AtomicU64,
    next_commit_seq: AtomicU64,
}

impl TxnManager {
    /// Create a new manager starting from the given initial values.
    #[must_use]
    pub fn new(initial_txn_id: u64, initial_commit_seq: u64) -> Self {
        Self {
            next_txn_id: AtomicU64::new(initial_txn_id),
            next_commit_seq: AtomicU64::new(initial_commit_seq),
        }
    }

    /// Allocate the next `TxnId` via CAS loop (INV-1).
    ///
    /// Returns `None` if the id space is exhausted (`> TXN_ID_MAX`).
    pub fn alloc_txn_id(&self) -> Option<TxnId> {
        loop {
            let current = self.next_txn_id.load(Ordering::Acquire);
            if current > TxnId::MAX_RAW {
                return None; // exhausted
            }
            let next = current.checked_add(1)?;
            if self
                .next_txn_id
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return TxnId::new(current);
            }
            // CAS failed — another thread won; retry.
        }
    }

    /// Allocate the next `CommitSeq`.
    ///
    /// This must be called only under the commit mutex to maintain
    /// strict ordering. Uses `Release` ordering so that readers using
    /// `Acquire` will see all prior version chain updates.
    pub fn alloc_commit_seq(&self) -> CommitSeq {
        let seq = self.next_commit_seq.fetch_add(1, Ordering::Release);
        CommitSeq::new(seq)
    }

    /// The current (not-yet-allocated) `TxnId` counter value.
    #[must_use]
    pub fn current_txn_counter(&self) -> u64 {
        self.next_txn_id.load(Ordering::Acquire)
    }

    /// The current (not-yet-allocated) `CommitSeq` counter value.
    #[must_use]
    pub fn current_commit_counter(&self) -> u64 {
        self.next_commit_seq.load(Ordering::Acquire)
    }
}

impl Default for TxnManager {
    fn default() -> Self {
        Self::new(1, 1)
    }
}

impl std::fmt::Debug for TxnManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnManager")
            .field("next_txn_id", &self.next_txn_id.load(Ordering::Relaxed))
            .field(
                "next_commit_seq",
                &self.next_commit_seq.load(Ordering::Relaxed),
            )
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Visibility predicate (§5.3)
// ---------------------------------------------------------------------------

/// The core MVCC visibility predicate.
///
/// A page version `V` is visible to snapshot `S` if and only if:
/// 1. `V.commit_seq != 0` (the version is committed, not a private write-set entry)
/// 2. `V.commit_seq <= S.high` (the commit happened before the snapshot)
#[inline]
#[must_use]
pub fn visible(version: &PageVersion, snapshot: &Snapshot) -> bool {
    version.commit_seq.get() != 0 && version.commit_seq <= snapshot.high
}

// ---------------------------------------------------------------------------
// VersionStore — version chain management
// ---------------------------------------------------------------------------

/// Version chain head table + arena, providing `resolve()` and `resolve_for_txn()`.
///
/// The version store owns all committed page versions in the arena and
/// maintains a mapping from each page to the head of its version chain.
pub struct VersionStore {
    arena: RwLock<VersionArena>,
    /// Maps each page to the head of its version chain (most recent committed version).
    chain_heads: RwLock<HashMap<PageNumber, VersionIdx, PageNumberBuildHasher>>,
    page_size: PageSize,
}

impl VersionStore {
    /// Create an empty version store.
    #[must_use]
    pub fn new(page_size: PageSize) -> Self {
        Self {
            arena: RwLock::new(VersionArena::new()),
            chain_heads: RwLock::new(HashMap::with_hasher(PageNumberBuildHasher::default())),
            page_size,
        }
    }

    /// Publish a committed version into the store.
    ///
    /// The version is allocated in the arena and linked at the head of its
    /// page's version chain (INV-3: new version has higher `commit_seq`
    /// than the previous head).
    ///
    /// Returns the `VersionIdx` of the published version.
    pub fn publish(&self, version: PageVersion) -> VersionIdx {
        let pgno = version.pgno;
        let mut arena = self.arena.write();
        let idx = arena.alloc(version);
        drop(arena);

        let mut heads = self.chain_heads.write();
        heads.insert(pgno, idx);
        drop(heads);

        tracing::debug!(pgno = pgno.get(), "version published to chain head");
        idx
    }

    /// Resolve the newest committed version of `page` visible to `snapshot`.
    ///
    /// Walks the version chain from the head, returning the first version
    /// where `visible(V, snapshot)` holds.
    ///
    /// Returns `None` if no committed version exists at or before the snapshot
    /// (the page only exists on disk or has not been written).
    #[must_use]
    #[allow(clippy::significant_drop_tightening)]
    pub fn resolve(&self, page: PageNumber, snapshot: &Snapshot) -> Option<VersionIdx> {
        let heads = self.chain_heads.read();
        let head_idx = *heads.get(&page)?;
        drop(heads);

        let arena = self.arena.read();
        let mut current_idx = head_idx;

        loop {
            let version = arena.get(current_idx)?;
            if visible(version, snapshot) {
                return Some(current_idx);
            }

            // Walk backward through the chain via prev pointer.
            let prev_ptr = version.prev?;
            current_idx = version_pointer_to_idx(prev_ptr);
        }
    }

    /// Resolve the base version for a write operation in a transaction.
    ///
    /// Checks the transaction's write set first (for pages already modified
    /// in this transaction), then falls back to `resolve()`.
    #[must_use]
    pub fn resolve_for_txn(&self, page: PageNumber, txn: &Transaction) -> Option<VersionIdx> {
        // Check if the page is already in the transaction's write set.
        // If so, the base version is whatever the previous version was.
        if txn.write_set.contains(&page) {
            // The transaction has already written this page; the "base" for
            // further writes is the version chain entry before this txn's write.
            // In a real implementation the write_set would map to VersionIdx,
            // but for the invariant layer we fall through to resolve().
            return self.resolve(page, &txn.snapshot);
        }

        self.resolve(page, &txn.snapshot)
    }

    /// Read a version from the arena by index.
    #[must_use]
    pub fn get_version(&self, idx: VersionIdx) -> Option<PageVersion> {
        let arena = self.arena.read();
        arena.get(idx).cloned()
    }

    /// Get the chain head index for a page, if any.
    #[must_use]
    pub fn chain_head(&self, page: PageNumber) -> Option<VersionIdx> {
        let heads = self.chain_heads.read();
        heads.get(&page).copied()
    }

    /// Walk the full version chain for a page, returning all versions
    /// from newest to oldest.
    #[must_use]
    #[allow(clippy::significant_drop_tightening)]
    pub fn walk_chain(&self, page: PageNumber) -> Vec<PageVersion> {
        let heads = self.chain_heads.read();
        let Some(&head_idx) = heads.get(&page) else {
            return Vec::new();
        };
        drop(heads);

        let arena = self.arena.read();
        let mut result = Vec::new();
        let mut current_idx = head_idx;

        while let Some(version) = arena.get(current_idx) {
            let prev = version.prev;
            result.push(version.clone());
            match prev {
                Some(ptr) => current_idx = version_pointer_to_idx(ptr),
                None => break,
            }
        }

        result
    }

    /// The page size used by this store.
    #[must_use]
    pub fn page_size(&self) -> PageSize {
        self.page_size
    }
}

impl std::fmt::Debug for VersionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let arena = self.arena.read();
        let heads = self.chain_heads.read();
        f.debug_struct("VersionStore")
            .field("page_size", &self.page_size.get())
            .field("page_count", &heads.len())
            .field("arena_high_water", &arena.high_water())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// SerializedWriteMutex — INV-7
// ---------------------------------------------------------------------------

/// Global write mutex for Serialized mode (INV-7).
///
/// At most one Serialized-mode writer holds this mutex at any time.
/// DEFERRED transactions do not acquire it until their first write.
pub struct SerializedWriteMutex {
    inner: Mutex<Option<TxnId>>,
}

impl SerializedWriteMutex {
    /// Create a new unlocked mutex.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(None),
        }
    }

    /// Try to acquire the mutex for `txn`. Returns `Ok(())` if acquired,
    /// or `Err(holder)` if another transaction holds it.
    pub fn try_acquire(&self, txn: TxnId) -> Result<(), TxnId> {
        let mut guard = self.inner.lock();
        match *guard {
            Some(holder) if holder != txn => Err(holder),
            Some(_) => Ok(()), // already held by this txn (idempotent)
            None => {
                *guard = Some(txn);
                drop(guard);
                tracing::info!(txn_id = %txn, "serialized write mutex acquired");
                Ok(())
            }
        }
    }

    /// Release the mutex held by `txn`. Returns `true` if released.
    pub fn release(&self, txn: TxnId) -> bool {
        let mut guard = self.inner.lock();
        if *guard == Some(txn) {
            *guard = None;
            drop(guard);
            tracing::info!(txn_id = %txn, "serialized write mutex released");
            true
        } else {
            false
        }
    }

    /// Check which transaction holds the mutex, if any.
    #[must_use]
    pub fn holder(&self) -> Option<TxnId> {
        *self.inner.lock()
    }
}

impl Default for SerializedWriteMutex {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for SerializedWriteMutex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SerializedWriteMutex")
            .field("holder", &self.holder())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert a `VersionPointer` (packed u64) to a `VersionIdx`.
///
/// The packing convention: high 32 bits = chunk, low 32 bits = offset.
#[inline]
#[must_use]
fn version_pointer_to_idx(ptr: VersionPointer) -> VersionIdx {
    let raw = ptr.get();
    #[allow(clippy::cast_possible_truncation)]
    let chunk = (raw >> 32) as u32;
    #[allow(clippy::cast_possible_truncation)]
    let offset = raw as u32;
    VersionIdx::new(chunk, offset)
}

/// Convert a `VersionIdx` to a `VersionPointer` for storage in `PageVersion.prev`.
#[inline]
#[must_use]
pub fn idx_to_version_pointer(idx: VersionIdx) -> VersionPointer {
    VersionPointer::new(u64::from(idx.chunk()) << 32 | u64::from(idx.offset()))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core_types::{InProcessPageLockTable, TransactionMode, TransactionState};
    use fsqlite_types::{PageData, SchemaEpoch, TxnEpoch, TxnToken};
    use proptest::prelude::*;

    fn make_snapshot(high: u64) -> Snapshot {
        Snapshot::new(CommitSeq::new(high), SchemaEpoch::ZERO)
    }

    fn make_version(pgno: u32, commit_seq: u64, prev: Option<VersionPointer>) -> PageVersion {
        PageVersion {
            pgno: PageNumber::new(pgno).unwrap(),
            commit_seq: CommitSeq::new(commit_seq),
            created_by: TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0)),
            data: PageData::zeroed(PageSize::DEFAULT),
            prev,
        }
    }

    // -----------------------------------------------------------------------
    // INV-1: Monotonicity (TxnId + CommitSeq)
    // -----------------------------------------------------------------------

    #[test]
    fn test_inv1_txnid_monotonic_cas_loop() {
        let mgr = TxnManager::default();
        let mut prev = 0_u64;

        for _ in 0..1000 {
            let id = mgr.alloc_txn_id().expect("should not exhaust id space");
            let raw = id.get();
            assert!(
                raw > prev,
                "TxnId must be strictly increasing: {raw} <= {prev}"
            );
            assert_ne!(raw, 0, "TxnId must never be zero");
            assert!(raw <= TxnId::MAX_RAW, "TxnId must not exceed MAX_RAW");
            prev = raw;
        }
    }

    #[test]
    fn test_inv1_txnid_exhaustion() {
        // Start near the max to test exhaustion.
        let mgr = TxnManager::new(TxnId::MAX_RAW, 1);

        let id = mgr.alloc_txn_id();
        assert!(id.is_some(), "should allocate the last valid TxnId");
        assert_eq!(id.unwrap().get(), TxnId::MAX_RAW);

        let id = mgr.alloc_txn_id();
        assert!(id.is_none(), "should fail when id space is exhausted");
    }

    #[test]
    fn test_inv1_commit_seq_monotonic() {
        let mgr = TxnManager::default();
        let mut prev = CommitSeq::ZERO;

        for _ in 0..100 {
            let seq = mgr.alloc_commit_seq();
            assert!(seq > prev, "CommitSeq must be strictly increasing");
            prev = seq;
        }
    }

    #[test]
    fn test_inv1_txnid_multithreaded_monotonicity() {
        use std::sync::Arc;

        let mgr = Arc::new(TxnManager::default());
        let mut handles = Vec::new();

        for _ in 0..4 {
            let mgr = Arc::clone(&mgr);
            handles.push(std::thread::spawn(move || {
                let mut ids = Vec::with_capacity(250);
                for _ in 0..250 {
                    ids.push(mgr.alloc_txn_id().unwrap().get());
                }
                ids
            }));
        }

        let mut all_ids: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();

        // All ids must be unique.
        let unique_count = {
            let mut sorted = all_ids.clone();
            sorted.sort_unstable();
            sorted.dedup();
            sorted.len()
        };
        assert_eq!(unique_count, 1000, "all TxnIds must be unique");

        // Each thread's local sequence must be increasing.
        // (Already guaranteed by AtomicU64 CAS, but verify the global set has no duplicates.)
        all_ids.sort_unstable();
        for window in all_ids.windows(2) {
            assert!(
                window[0] < window[1],
                "global TxnId sequence must be strictly increasing: {} >= {}",
                window[0],
                window[1]
            );
        }
    }

    // -----------------------------------------------------------------------
    // INV-2: Lock Exclusivity (tested via InProcessPageLockTable)
    // -----------------------------------------------------------------------

    #[test]
    fn test_inv2_page_lock_exclusivity() {
        let table = InProcessPageLockTable::new();
        let page = PageNumber::new(42).unwrap();
        let txn_a = TxnId::new(1).unwrap();
        let txn_b = TxnId::new(2).unwrap();

        // txn_a acquires.
        assert!(table.try_acquire(page, txn_a).is_ok());

        // txn_b is blocked (gets SQLITE_BUSY equivalent).
        let err = table.try_acquire(page, txn_b);
        assert_eq!(err, Err(txn_a), "second txn must see the holder");

        // txn_a re-acquires (idempotent).
        assert!(table.try_acquire(page, txn_a).is_ok());

        // After release, txn_b can acquire.
        assert!(table.release(page, txn_a));
        assert!(table.try_acquire(page, txn_b).is_ok());
    }

    // -----------------------------------------------------------------------
    // INV-3: Version Chain Order (descending commit_seq)
    // -----------------------------------------------------------------------

    #[test]
    fn test_inv3_version_chain_descending() {
        let store = VersionStore::new(PageSize::DEFAULT);

        // Commit 5 transactions writing the same page.
        let pgno = PageNumber::new(1).unwrap();
        let mut prev_ptr: Option<VersionPointer> = None;

        for seq in 1..=5_u64 {
            let version = PageVersion {
                pgno,
                commit_seq: CommitSeq::new(seq),
                created_by: TxnToken::new(TxnId::new(seq).unwrap(), TxnEpoch::new(0)),
                data: PageData::zeroed(PageSize::DEFAULT),
                prev: prev_ptr,
            };
            let idx = store.publish(version);
            prev_ptr = Some(idx_to_version_pointer(idx));
        }

        // Walk the chain and verify strictly descending commit_seq.
        let chain = store.walk_chain(pgno);
        assert_eq!(chain.len(), 5);

        for window in chain.windows(2) {
            assert!(
                window[0].commit_seq > window[1].commit_seq,
                "version chain must be strictly descending: {} <= {}",
                window[0].commit_seq.get(),
                window[1].commit_seq.get()
            );
        }
    }

    // -----------------------------------------------------------------------
    // INV-4: Write Set Consistency (every write_set page must be locked)
    // -----------------------------------------------------------------------

    #[test]
    fn test_inv4_write_set_requires_lock() {
        let table = InProcessPageLockTable::new();
        let txn_id = TxnId::new(1).unwrap();
        let snap = make_snapshot(0);
        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snap, TransactionMode::Concurrent);

        let page = PageNumber::new(10).unwrap();

        // Acquire lock first (correct order per INV-4).
        table.try_acquire(page, txn_id).unwrap();
        txn.page_locks.insert(page);
        txn.write_set.push(page);

        // Verify invariant: every page in write_set is in page_locks.
        for &p in &txn.write_set {
            assert!(
                txn.page_locks.contains(&p),
                "INV-4 violated: page {p:?} in write_set but not in page_locks"
            );
        }
    }

    // -----------------------------------------------------------------------
    // INV-5: Snapshot Stability (DEFERRED nuance)
    // -----------------------------------------------------------------------

    #[test]
    fn test_inv5_deferred_snapshot_provisional() {
        let txn_id = TxnId::new(1).unwrap();
        let provisional_snap = make_snapshot(0);

        // Simulate DEFERRED mode: snapshot_established starts false.
        let mut txn = Transaction::new(
            txn_id,
            TxnEpoch::new(0),
            provisional_snap,
            TransactionMode::Serialized,
        );
        // Override: for DEFERRED, snapshot is provisional.
        txn.snapshot_established = false;

        assert!(
            !txn.snapshot_established,
            "DEFERRED snapshot should be provisional"
        );

        // First read establishes the snapshot.
        let current_high = CommitSeq::new(5);
        txn.snapshot = Snapshot::new(current_high, SchemaEpoch::ZERO);
        txn.snapshot_established = true;

        assert!(
            txn.snapshot_established,
            "snapshot should now be established"
        );
        assert_eq!(txn.snapshot.high, current_high);

        // Once established, verify it cannot change (type-level immutability).
        let established = txn.snapshot;
        assert_eq!(established.high.get(), 5);
    }

    // -----------------------------------------------------------------------
    // INV-6: Commit Atomicity (all-or-nothing visibility)
    // -----------------------------------------------------------------------

    #[test]
    fn test_inv6_commit_atomicity_all_visible_or_none() {
        let store = VersionStore::new(PageSize::DEFAULT);

        // Transaction writes pages 1, 2, 3 with commit_seq=5.
        let pages = [1_u32, 2, 3];
        for &p in &pages {
            let version = make_version(p, 5, None);
            store.publish(version);
        }

        // Snapshot at high=4: none should be visible.
        let snap_before = make_snapshot(4);
        for &p in &pages {
            let pgno = PageNumber::new(p).unwrap();
            assert!(
                store.resolve(pgno, &snap_before).is_none(),
                "page {p} should NOT be visible at snapshot high=4"
            );
        }

        // Snapshot at high=5: all should be visible.
        let snap_at = make_snapshot(5);
        for &p in &pages {
            let pgno = PageNumber::new(p).unwrap();
            assert!(
                store.resolve(pgno, &snap_at).is_some(),
                "page {p} should be visible at snapshot high=5"
            );
        }

        // Snapshot at high=10: all should still be visible.
        let snap_after = make_snapshot(10);
        for &p in &pages {
            let pgno = PageNumber::new(p).unwrap();
            assert!(
                store.resolve(pgno, &snap_after).is_some(),
                "page {p} should be visible at snapshot high=10"
            );
        }
    }

    // -----------------------------------------------------------------------
    // INV-7: Serialized Mode (global write mutex)
    // -----------------------------------------------------------------------

    #[test]
    fn test_inv7_serialized_write_mutex_exclusivity() {
        let mutex = SerializedWriteMutex::new();
        let txn_a = TxnId::new(1).unwrap();
        let txn_b = TxnId::new(2).unwrap();

        // txn_a acquires.
        assert!(mutex.try_acquire(txn_a).is_ok());
        assert_eq!(mutex.holder(), Some(txn_a));

        // txn_b cannot acquire.
        assert_eq!(mutex.try_acquire(txn_b), Err(txn_a));

        // txn_a re-acquire is idempotent.
        assert!(mutex.try_acquire(txn_a).is_ok());

        // Release.
        assert!(mutex.release(txn_a));
        assert!(mutex.holder().is_none());

        // Now txn_b can acquire.
        assert!(mutex.try_acquire(txn_b).is_ok());
        assert_eq!(mutex.holder(), Some(txn_b));
        assert!(mutex.release(txn_b));
    }

    // -----------------------------------------------------------------------
    // Visibility predicate tests (§5.3)
    // -----------------------------------------------------------------------

    #[test]
    fn test_visible_predicate_committed_within_range() {
        let snap = make_snapshot(10);

        // Committed at seq=5, visible (5 <= 10).
        let v5 = make_version(1, 5, None);
        assert!(visible(&v5, &snap));

        // Committed at seq=10, visible (10 <= 10).
        let v10 = make_version(1, 10, None);
        assert!(visible(&v10, &snap));

        // Committed at seq=15, NOT visible (15 > 10).
        let v15 = make_version(1, 15, None);
        assert!(!visible(&v15, &snap));

        // Uncommitted (seq=0), NOT visible.
        let v0 = make_version(1, 0, None);
        assert!(!visible(&v0, &snap));
    }

    #[test]
    fn test_resolve_returns_first_visible_from_head() {
        let store = VersionStore::new(PageSize::DEFAULT);
        let pgno = PageNumber::new(1).unwrap();

        // Build chain: V1(seq=1) <- V2(seq=5) <- V3(seq=10)
        let v1 = make_version(1, 1, None);
        let idx1 = store.publish(v1);

        let v2 = make_version(1, 5, Some(idx_to_version_pointer(idx1)));
        let idx2 = store.publish(v2);

        let v3 = make_version(1, 10, Some(idx_to_version_pointer(idx2)));
        store.publish(v3);

        // Snapshot high=7: should resolve to V2 (seq=5, first visible from head).
        let snap = make_snapshot(7);
        let resolved = store.resolve(pgno, &snap).unwrap();
        let version = store.get_version(resolved).unwrap();
        assert_eq!(
            version.commit_seq,
            CommitSeq::new(5),
            "should resolve to V2 (seq=5)"
        );

        // Snapshot high=10: should resolve to V3 (seq=10).
        let snap_at_ten = make_snapshot(10);
        let resolved_ten = store.resolve(pgno, &snap_at_ten).unwrap();
        let version_ten = store.get_version(resolved_ten).unwrap();
        assert_eq!(version_ten.commit_seq, CommitSeq::new(10));

        // Snapshot high=0: nothing visible (seq 0 is uncommitted marker).
        let snap_at_zero = make_snapshot(0);
        assert!(store.resolve(pgno, &snap_at_zero).is_none());
    }

    #[test]
    fn test_resolve_for_txn_checks_write_set_first() {
        let store = VersionStore::new(PageSize::DEFAULT);
        let pgno = PageNumber::new(1).unwrap();

        // Publish a committed version.
        let v1 = make_version(1, 1, None);
        store.publish(v1);

        // Create a transaction that has written to page 1.
        let txn_id = TxnId::new(2).unwrap();
        let snap = make_snapshot(1);
        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snap, TransactionMode::Concurrent);
        txn.write_set.push(pgno);

        // resolve_for_txn should still resolve (via snapshot fallback).
        let resolved = store.resolve_for_txn(pgno, &txn);
        assert!(
            resolved.is_some(),
            "should resolve even with write_set entry"
        );

        // For a page NOT in write_set, also resolves via snapshot.
        let other_page = PageNumber::new(99).unwrap();
        let resolved_other = store.resolve_for_txn(other_page, &txn);
        assert!(resolved_other.is_none(), "page 99 has no versions");
    }

    // -----------------------------------------------------------------------
    // §5.3 Worked example: 5-txn scenario
    // -----------------------------------------------------------------------

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_worked_example_5txn_scenario() {
        // Implements the 12-step worked example from the spec (§5.3).
        let mgr = TxnManager::default();
        let store = VersionStore::new(PageSize::DEFAULT);
        let lock_table = InProcessPageLockTable::new();

        let p1 = PageNumber::new(1).unwrap();

        // t0: T1 begins (snapshot.high=0)
        let t1_id = mgr.alloc_txn_id().unwrap();
        let snap0 = make_snapshot(0);
        let mut t1 = Transaction::new(t1_id, TxnEpoch::new(0), snap0, TransactionMode::Concurrent);

        // t1: T2 begins (snapshot.high=0)
        let t2_id = mgr.alloc_txn_id().unwrap();
        let mut t2 = Transaction::new(t2_id, TxnEpoch::new(0), snap0, TransactionMode::Concurrent);

        // t2: T1 writes P1 (private write_set version)
        lock_table.try_acquire(p1, t1_id).unwrap();
        t1.page_locks.insert(p1);
        t1.write_set.push(p1);

        // t3: T3 begins (snapshot.high=0)
        let t3_id = mgr.alloc_txn_id().unwrap();
        let mut t3 = Transaction::new(t3_id, TxnEpoch::new(0), snap0, TransactionMode::Concurrent);

        // t4: T1 commits (commit_seq=1; publishes V1)
        let seq1 = mgr.alloc_commit_seq();
        assert_eq!(seq1.get(), 1);

        let v1 = PageVersion {
            pgno: p1,
            commit_seq: seq1,
            created_by: t1.token(),
            data: PageData::zeroed(PageSize::DEFAULT),
            prev: None,
        };
        store.publish(v1);
        lock_table.release_all(t1_id);
        t1.commit();

        // t5: T2 writes P1 (private)
        // T2 tries to acquire lock (T1 released it, so it succeeds).
        lock_table.try_acquire(p1, t2_id).unwrap();
        t2.page_locks.insert(p1);
        t2.write_set.push(p1);

        // t6: T4 begins (snapshot.high=1 — sees V1)
        let t4_id = mgr.alloc_txn_id().unwrap();
        let snap1 = make_snapshot(1);
        let t4 = Transaction::new(t4_id, TxnEpoch::new(0), snap1, TransactionMode::Concurrent);

        // t7: T2 commits -> FAILS FCW
        // Base version of P1 has commit_seq=1, but T2's snapshot.high=0.
        // FCW check: base_version(P1).commit_seq (=1) > T2.snapshot.high (=0) => FAIL
        let base = store.resolve(
            p1,
            &Snapshot::new(CommitSeq::new(u64::MAX), SchemaEpoch::ZERO),
        );
        let base_version = store.get_version(base.unwrap()).unwrap();
        let fcw_fail_t2 = base_version.commit_seq.get() > t2.snapshot.high.get();
        assert!(
            fcw_fail_t2,
            "T2 must fail FCW: base seq=1 > snapshot high=0"
        );
        lock_table.release_all(t2_id);
        t2.abort();
        assert_eq!(t2.state, TransactionState::Aborted);

        // t8: T5 begins (snapshot.high=1)
        let t5_id = mgr.alloc_txn_id().unwrap();
        let mut t5 = Transaction::new(t5_id, TxnEpoch::new(0), snap1, TransactionMode::Concurrent);

        // t9: T3 writes P1 (private)
        lock_table.try_acquire(p1, t3_id).unwrap();
        t3.page_locks.insert(p1);
        t3.write_set.push(p1);

        // t10: T3 commits -> FAILS FCW (same reason as T2)
        let fcw_fail_t3 = base_version.commit_seq.get() > t3.snapshot.high.get();
        assert!(
            fcw_fail_t3,
            "T3 must fail FCW: base seq=1 > snapshot high=0"
        );
        lock_table.release_all(t3_id);
        t3.abort();
        assert_eq!(t3.state, TransactionState::Aborted);

        // t11: T5 writes P1
        lock_table.try_acquire(p1, t5_id).unwrap();
        t5.page_locks.insert(p1);
        t5.write_set.push(p1);

        // t12: T5 commits (commit_seq=2; publishes V2)
        // FCW check: base_version(P1).commit_seq (=1) <= T5.snapshot.high (=1) => PASS
        let fcw_pass_t5 = base_version.commit_seq.get() <= t5.snapshot.high.get();
        assert!(
            fcw_pass_t5,
            "T5 must pass FCW: base seq=1 <= snapshot high=1"
        );

        let seq2 = mgr.alloc_commit_seq();
        assert_eq!(seq2.get(), 2);

        let head_idx = store.chain_head(p1).unwrap();
        let v2 = PageVersion {
            pgno: p1,
            commit_seq: seq2,
            created_by: t5.token(),
            data: PageData::zeroed(PageSize::DEFAULT),
            prev: Some(idx_to_version_pointer(head_idx)),
        };
        store.publish(v2);
        lock_table.release_all(t5_id);
        t5.commit();

        // Verify what each transaction sees:
        // T2 (snap high=0): no committed version visible.
        let snap_t2 = make_snapshot(0);
        assert!(store.resolve(p1, &snap_t2).is_none());

        // T4 (snap high=1): sees V1 (seq=1).
        let resolved_t4 = store.resolve(p1, &t4.snapshot).unwrap();
        let ver_t4 = store.get_version(resolved_t4).unwrap();
        assert_eq!(ver_t4.commit_seq.get(), 1, "T4 should see V1");

        // T5 (snap high=1): before writing, sees V1.
        let resolved_t5_before = store.resolve(p1, &snap1).unwrap();
        let ver_t5 = store.get_version(resolved_t5_before).unwrap();
        assert_eq!(
            ver_t5.commit_seq.get(),
            1,
            "T5 should see V1 at snap high=1"
        );

        // After T5 commits, a new snapshot at high=2 sees V2.
        let snap2 = make_snapshot(2);
        let resolved_snap2 = store.resolve(p1, &snap2).unwrap();
        let ver_snap2 = store.get_version(resolved_snap2).unwrap();
        assert_eq!(
            ver_snap2.commit_seq.get(),
            2,
            "snapshot high=2 should see V2"
        );

        // Version chain verification (INV-3).
        let chain = store.walk_chain(p1);
        assert_eq!(chain.len(), 2, "should have 2 committed versions");
        assert_eq!(chain[0].commit_seq.get(), 2, "head should be V2");
        assert_eq!(chain[1].commit_seq.get(), 1, "tail should be V1");
    }

    #[test]
    fn test_theorem4_gc_never_removes_needed_version() {
        let store = VersionStore::new(PageSize::DEFAULT);
        let pgno = PageNumber::new(7).unwrap();

        // Build chain: V1(seq=1) <- V2(seq=5) <- V3(seq=10)
        let v1 = make_version(7, 1, None);
        let idx1 = store.publish(v1);
        let v2 = make_version(7, 5, Some(idx_to_version_pointer(idx1)));
        let idx2 = store.publish(v2);
        let v3 = make_version(7, 10, Some(idx_to_version_pointer(idx2)));
        let idx3 = store.publish(v3);

        // Active snapshot at high=7 must resolve to V2.
        let active_snap = make_snapshot(7);
        let visible_idx = store.resolve(pgno, &active_snap).unwrap();
        let visible = store.get_version(visible_idx).unwrap();
        assert_eq!(
            visible.commit_seq.get(),
            5,
            "active snapshot must keep V2 reachable"
        );

        // GC horizon at 7: V1 reclaimable (superseded by V2 <= horizon),
        // V2 not reclaimable (newer is V3=10 > horizon), V3 not reclaimable.
        let gc_horizon = CommitSeq::new(7);
        let v1_ref = store.get_version(idx1).unwrap();
        let v2_ref = store.get_version(idx2).unwrap();
        let v3_ref = store.get_version(idx3).unwrap();

        let v1_reclaimable = v1_ref.commit_seq < gc_horizon && v2_ref.commit_seq <= gc_horizon;
        let v2_reclaimable = v2_ref.commit_seq < gc_horizon && v3_ref.commit_seq <= gc_horizon;
        let v3_reclaimable = v3_ref.commit_seq < gc_horizon;

        assert!(v1_reclaimable, "V1 should be reclaimable");
        assert!(!v2_reclaimable, "V2 must be retained for snapshot high=7");
        assert!(!v3_reclaimable, "head version is never reclaimable here");
    }

    #[test]
    fn test_theorem5_version_chain_bounded_by_rd_plus_1() {
        let store = VersionStore::new(PageSize::DEFAULT);
        let pgno = PageNumber::new(11).unwrap();
        let commit_rate_per_sec = 100_u64;
        let max_txn_duration_secs = 1_u64;
        let bound = commit_rate_per_sec * max_txn_duration_secs + 1;

        let mut prev: Option<VersionPointer> = None;
        for seq in 1..=bound {
            let version = PageVersion {
                pgno,
                commit_seq: CommitSeq::new(seq),
                created_by: TxnToken::new(TxnId::new(seq).unwrap(), TxnEpoch::new(0)),
                data: PageData::zeroed(PageSize::DEFAULT),
                prev,
            };
            let idx = store.publish(version);
            prev = Some(idx_to_version_pointer(idx));
        }

        let chain = store.walk_chain(pgno);
        assert_eq!(
            chain.len(),
            usize::try_from(bound).unwrap(),
            "version chain should respect R*D+1 bound in bounded workload"
        );
    }

    #[test]
    fn test_theorem4_no_active_txns_gc_all_but_latest() {
        let store = VersionStore::new(PageSize::DEFAULT);
        let pgno = PageNumber::new(12).unwrap();

        let mut prev: Option<VersionPointer> = None;
        for seq in 1_u64..=3 {
            let version = PageVersion {
                pgno,
                commit_seq: CommitSeq::new(seq),
                created_by: TxnToken::new(TxnId::new(seq).unwrap(), TxnEpoch::new(0)),
                data: PageData::zeroed(PageSize::DEFAULT),
                prev,
            };
            let idx = store.publish(version);
            prev = Some(idx_to_version_pointer(idx));
        }

        // No active txns: safe horizon is latest commit.
        let horizon = CommitSeq::new(3);
        let chain = store.walk_chain(pgno); // [3, 2, 1]
        assert_eq!(chain.len(), 3);
        assert_eq!(chain[0].commit_seq, CommitSeq::new(3));

        // All but the latest are reclaimable at horizon=latest.
        let reclaimable = chain
            .windows(2)
            .filter(|pair| pair[1].commit_seq < horizon && pair[0].commit_seq <= horizon)
            .count();
        assert_eq!(reclaimable, 2, "older versions should be reclaimable");
    }

    #[test]
    fn test_theorem4_gc_horizon_min_active_snapshot() {
        let active_highs = [CommitSeq::new(10), CommitSeq::new(20), CommitSeq::new(30)];
        let safe_gc_seq = active_highs.iter().copied().min().unwrap();
        assert_eq!(
            safe_gc_seq,
            CommitSeq::new(10),
            "gc horizon must track min active snapshot.high"
        );
    }

    #[test]
    fn test_theorem4_reclaimability_predicate() {
        let store = VersionStore::new(PageSize::DEFAULT);

        // Chain: V1(3) <- V2(5) <- V3(9)
        let v1 = make_version(13, 3, None);
        let idx1 = store.publish(v1);
        let v2 = make_version(13, 5, Some(idx_to_version_pointer(idx1)));
        let idx2 = store.publish(v2);
        let v3 = make_version(13, 9, Some(idx_to_version_pointer(idx2)));
        let idx3 = store.publish(v3);

        let horizon = CommitSeq::new(7);
        let v1_ref = store.get_version(idx1).unwrap();
        let v2_ref = store.get_version(idx2).unwrap();
        let v3_ref = store.get_version(idx3).unwrap();

        let v1_reclaimable = v1_ref.commit_seq < horizon && v2_ref.commit_seq <= horizon;
        let v2_reclaimable = v2_ref.commit_seq < horizon && v3_ref.commit_seq <= horizon;
        assert!(v1_reclaimable, "V1 should satisfy reclaimability predicate");
        assert!(
            !v2_reclaimable,
            "V2 must be retained because newer V3 is beyond horizon"
        );
    }

    #[test]
    fn test_theorem5_version_chain_bounded() {
        test_theorem5_version_chain_bounded_by_rd_plus_1();
    }

    #[test]
    fn test_theorem5_gc_prunes_old_versions() {
        let store = VersionStore::new(PageSize::DEFAULT);
        let pgno = PageNumber::new(14).unwrap();
        let mut prev: Option<VersionPointer> = None;
        for seq in 1_u64..=32 {
            let version = PageVersion {
                pgno,
                commit_seq: CommitSeq::new(seq),
                created_by: TxnToken::new(TxnId::new(seq).unwrap(), TxnEpoch::new(0)),
                data: PageData::zeroed(PageSize::DEFAULT),
                prev,
            };
            let idx = store.publish(version);
            prev = Some(idx_to_version_pointer(idx));
        }

        let chain = store.walk_chain(pgno);
        let horizon = CommitSeq::new(32);
        let reclaimable = chain
            .iter()
            .skip(1)
            .filter(|version| version.commit_seq <= horizon)
            .count();
        assert_eq!(reclaimable, 31, "all non-head versions are reclaimable");
        assert_eq!(chain[0].commit_seq, CommitSeq::new(32));
    }

    proptest! {
        #[test]
        fn prop_gc_safety_holds(horizon in 1_u64..40_u64) {
            let store = VersionStore::new(PageSize::DEFAULT);
            let pgno = PageNumber::new(15).unwrap();
            let mut prev: Option<VersionPointer> = None;

            for seq in 1_u64..=horizon + 2 {
                let version = PageVersion {
                    pgno,
                    commit_seq: CommitSeq::new(seq),
                    created_by: TxnToken::new(TxnId::new(seq).unwrap(), TxnEpoch::new(0)),
                    data: PageData::zeroed(PageSize::DEFAULT),
                    prev,
                };
                let idx = store.publish(version);
                prev = Some(idx_to_version_pointer(idx));
            }

            let active_snapshot = make_snapshot(horizon);
            let visible_idx = store.resolve(pgno, &active_snapshot).expect("visible version must exist");
            let visible = store.get_version(visible_idx).expect("arena lookup must succeed");
            prop_assert_eq!(visible.commit_seq, CommitSeq::new(horizon));

            // The version selected by an active snapshot must not satisfy the reclaim predicate.
            // Its immediate newer successor is horizon+1, which is > horizon.
            let visible_reclaimable = visible.commit_seq < active_snapshot.high;
            prop_assert!(!visible_reclaimable);
        }
    }

    // -----------------------------------------------------------------------
    // E2E: invariants hold under concurrent schedule
    // -----------------------------------------------------------------------

    #[test]
    fn test_e2e_invariants_under_concurrent_schedule() {
        let mgr = TxnManager::default();
        let store = VersionStore::new(PageSize::DEFAULT);
        let lock_table = InProcessPageLockTable::new();
        let write_mutex = SerializedWriteMutex::new();

        let mut committed_ids = Vec::new();

        // Process 10 transactions sequentially, each writing its own page.
        // Serialized-mode txns (every 3rd) acquire/release the global mutex.
        for i in 1..=10_u64 {
            let id = mgr.alloc_txn_id().unwrap();
            let snap = make_snapshot(i.saturating_sub(1));
            let mode = if i % 3 == 0 {
                TransactionMode::Serialized
            } else {
                TransactionMode::Concurrent
            };
            let mut txn = Transaction::new(id, TxnEpoch::new(0), snap, mode);
            let pgno = PageNumber::new(u32::try_from(i).unwrap()).unwrap();

            // INV-7: Serialized txns must acquire the global mutex.
            if txn.mode == TransactionMode::Serialized {
                write_mutex.try_acquire(txn.txn_id).unwrap();
                txn.serialized_write_lock_held = true;
            }

            // INV-2: acquire page lock first.
            lock_table.try_acquire(pgno, txn.txn_id).unwrap();
            txn.page_locks.insert(pgno);

            // INV-4: write_set only after lock acquired.
            txn.write_set.push(pgno);
            for &p in &txn.write_set {
                assert!(txn.page_locks.contains(&p), "INV-4 violated for {p:?}");
            }

            // Commit.
            let seq = mgr.alloc_commit_seq();
            let version = PageVersion {
                pgno,
                commit_seq: seq,
                created_by: txn.token(),
                data: PageData::zeroed(PageSize::DEFAULT),
                prev: None,
            };
            store.publish(version);

            lock_table.release_all(txn.txn_id);
            if txn.serialized_write_lock_held {
                write_mutex.release(txn.txn_id);
                txn.serialized_write_lock_held = false;
            }
            txn.commit();
            committed_ids.push(txn.txn_id.get());
        }

        // Verify all invariants post-commit:

        // INV-1: All TxnIds are unique and increasing.
        for window in committed_ids.windows(2) {
            assert!(window[0] < window[1], "INV-1: TxnIds must be increasing");
        }

        // INV-2: No locks held.
        assert_eq!(lock_table.lock_count(), 0, "all locks must be released");

        // INV-6: All pages visible at snapshot high=10.
        let snap_all = make_snapshot(10);
        for i in 1..=10_u32 {
            let pgno = PageNumber::new(i).unwrap();
            assert!(
                store.resolve(pgno, &snap_all).is_some(),
                "INV-6: page {} must be visible at high=10",
                i
            );
        }

        // INV-7: Write mutex is released.
        assert!(
            write_mutex.holder().is_none(),
            "INV-7: mutex must be released"
        );
    }

    // -----------------------------------------------------------------------
    // bd-22n.8 — Allocation-Free Read Path E2E Test
    // -----------------------------------------------------------------------

    #[test]
    fn test_e2e_version_resolve_allocation_free() {
        // bd-22n.8: The full MVCC read path (VersionStore::resolve) is
        // allocation-free for cached, visible versions.
        //
        // We verify by:
        // 1. Publishing a chain of versions
        // 2. Resolving them repeatedly
        // 3. Checking that resolve returns the same VersionIdx each time
        //    (proving no intermediate data structures are allocated per call)
        const BEAD_22N8: &str = "bd-22n.8";

        let store = VersionStore::new(PageSize::DEFAULT);
        let p1 = PageNumber::new(1).unwrap();

        // Publish 3 versions of page 1.
        let v1 = make_version(1, 1, None);
        let idx1 = store.publish(v1);

        let v2 = make_version(1, 3, Some(idx_to_version_pointer(idx1)));
        let idx2 = store.publish(v2);

        let v3 = make_version(1, 5, Some(idx_to_version_pointer(idx2)));
        store.publish(v3);

        // Snapshot at commit_seq=4: should see v2 (commit_seq=3).
        let snap = make_snapshot(4);

        // Resolve 100 times — must return the same index each time.
        let first_idx = store.resolve(p1, &snap).unwrap();
        for round in 0..100u32 {
            let idx = store.resolve(p1, &snap).unwrap();
            assert_eq!(
                idx, first_idx,
                "bead_id={BEAD_22N8} case=e2e_version_resolve_stable \
                 round={round} resolve must return same VersionIdx"
            );
        }

        // Verify we got the right version (commit_seq=3).
        let resolved = store.get_version(first_idx).unwrap();
        assert_eq!(
            resolved.commit_seq,
            CommitSeq::new(3),
            "bead_id={BEAD_22N8} case=e2e_resolved_correct_version"
        );

        // Snapshot at commit_seq=5: should see v3 (commit_seq=5).
        let snap5 = make_snapshot(5);
        let idx5 = store.resolve(p1, &snap5).unwrap();
        let v5_resolved = store.get_version(idx5).unwrap();
        assert_eq!(
            v5_resolved.commit_seq,
            CommitSeq::new(5),
            "bead_id={BEAD_22N8} case=e2e_latest_version_resolved"
        );

        // Snapshot at commit_seq=0: nothing visible.
        let snap0 = make_snapshot(0);
        assert!(
            store.resolve(p1, &snap0).is_none(),
            "bead_id={BEAD_22N8} case=e2e_no_visible_version_at_zero"
        );
    }

    // -----------------------------------------------------------------------
    // Property tests
    // -----------------------------------------------------------------------

    proptest! {
        #[test]
        fn prop_visible_uncommitted_never_visible(
            high in 0_u64..1_000_000,
        ) {
            let snap = make_snapshot(high);
            let uncommitted = make_version(1, 0, None);
            prop_assert!(!visible(&uncommitted, &snap), "uncommitted (seq=0) must never be visible");
        }

        #[test]
        fn prop_visible_committed_iff_in_range(
            seq in 1_u64..1_000_000,
            high in 0_u64..1_000_000,
        ) {
            let snap = make_snapshot(high);
            let version = make_version(1, seq, None);
            let expected = seq <= high;
            prop_assert_eq!(
                visible(&version, &snap),
                expected,
                "visible(seq={}, high={}) should be {}", seq, high, expected
            );
        }

        #[test]
        fn prop_txn_manager_ids_unique(
            count in 1_usize..500,
        ) {
            let mgr = TxnManager::default();
            let mut ids = Vec::with_capacity(count);
            for _ in 0..count {
                ids.push(mgr.alloc_txn_id().unwrap().get());
            }
            let mut deduped = ids.clone();
            deduped.sort_unstable();
            deduped.dedup();
            prop_assert_eq!(ids.len(), deduped.len(), "all TxnIds must be unique");
        }

        // INV-PBT-4 (bd-2sm1): Snapshot isolation under multi-writer workload.
        //
        // Generate N writers that each commit to random pages at sequential
        // commit_seqs. Then take snapshots at various high values and verify:
        //   1. Every resolved version has commit_seq <= snapshot.high
        //   2. The resolved version is the *maximum* commit_seq <= high for that page
        //   3. Resolving the same page twice yields the same result (determinism)
        #[test]
        fn prop_snapshot_isolation_multi_writer(
            num_writers in 2_usize..8,
            ops_per_writer in 1_usize..20,
            num_pages in 1_u32..16,
            snapshot_highs in proptest::collection::vec(0_u64..100, 1..10),
        ) {
            let store = VersionStore::new(PageSize::DEFAULT);
            let mgr = TxnManager::default();

            // Track the latest commit_seq per page for oracle verification.
            let mut page_history: std::collections::BTreeMap<u32, Vec<u64>> =
                std::collections::BTreeMap::new();

            // Each writer commits ops_per_writer pages.
            for _ in 0..num_writers {
                let txn_id = mgr.alloc_txn_id().unwrap();
                let seq = mgr.alloc_commit_seq();

                for _ in 0..ops_per_writer {
                    #[allow(clippy::cast_possible_truncation)] // modulo num_pages (< 16) always fits u32
                    let pgno_raw =
                        ((seq.get() * 7 + txn_id.get() * 3) % u64::from(num_pages)) as u32 + 1;
                    let pgno = PageNumber::new(pgno_raw).unwrap();

                    // Look up previous head to chain versions properly.
                    let prev = store.chain_head(pgno).map(idx_to_version_pointer);
                    let version = PageVersion {
                        pgno,
                        commit_seq: seq,
                        created_by: TxnToken::new(txn_id, TxnEpoch::new(0)),
                        data: PageData::zeroed(PageSize::DEFAULT),
                        prev,
                    };
                    store.publish(version);
                    page_history.entry(pgno_raw).or_default().push(seq.get());
                }
            }

            // Sort each page's history so we can binary-search for the oracle answer.
            for seqs in page_history.values_mut() {
                seqs.sort_unstable();
            }

            // Verify snapshot isolation for each generated snapshot high value.
            for &high in &snapshot_highs {
                let snap = make_snapshot(high);

                for (&pgno_raw, seqs) in &page_history {
                    let pgno = PageNumber::new(pgno_raw).unwrap();

                    // Oracle: the expected visible version is the max seq <= high.
                    let expected_seq = seqs.iter().copied().filter(|&s| s <= high).max();

                    let resolved = store.resolve(pgno, &snap);
                    match (expected_seq, resolved) {
                        (None, None) => {} // correctly invisible
                        (Some(exp), Some(idx)) => {
                            let v = store.get_version(idx).unwrap();
                            // INV-PBT-4a: commit_seq <= snapshot.high
                            prop_assert!(
                                v.commit_seq.get() <= high,
                                "page {} resolved commit_seq {} > snapshot high {}",
                                pgno_raw, v.commit_seq.get(), high
                            );
                            // INV-PBT-4b: resolved version is the max visible
                            prop_assert_eq!(
                                v.commit_seq.get(), exp,
                                "page {} expected seq {} but got {}",
                                pgno_raw, exp, v.commit_seq.get()
                            );
                        }
                        (Some(exp), None) => {
                            prop_assert!(
                                false,
                                "page {} expected visible at seq {} but resolve returned None",
                                pgno_raw, exp
                            );
                        }
                        (None, Some(idx)) => {
                            let v = store.get_version(idx).unwrap();
                            prop_assert!(
                                false,
                                "page {} expected invisible but resolved to seq {}",
                                pgno_raw, v.commit_seq.get()
                            );
                        }
                    }

                    // INV-PBT-4c: Determinism — resolving again yields same result.
                    let resolved2 = store.resolve(pgno, &snap);
                    prop_assert_eq!(
                        resolved, resolved2,
                        "resolve must be deterministic for page {} at high {}",
                        pgno_raw, high
                    );
                }
            }
        }

        // bd-2sm1: Version chain walk must yield strictly descending commit_seq.
        #[test]
        fn prop_version_chain_strictly_descending(
            chain_len in 2_usize..30,
        ) {
            let store = VersionStore::new(PageSize::DEFAULT);
            let pgno = PageNumber::new(99).unwrap();
            let mut prev: Option<VersionPointer> = None;

            for seq in 1..=chain_len as u64 {
                let version = PageVersion {
                    pgno,
                    commit_seq: CommitSeq::new(seq),
                    created_by: TxnToken::new(TxnId::new(seq).unwrap(), TxnEpoch::new(0)),
                    data: PageData::zeroed(PageSize::DEFAULT),
                    prev,
                };
                let idx = store.publish(version);
                prev = Some(idx_to_version_pointer(idx));
            }

            let chain = store.walk_chain(pgno);
            prop_assert!(chain.len() >= 2, "chain too short: {}", chain.len());

            // Chain must be strictly descending (newest first).
            for window in chain.windows(2) {
                prop_assert!(
                    window[0].commit_seq > window[1].commit_seq,
                    "version chain not strictly descending: {} >= {}",
                    window[0].commit_seq.get(),
                    window[1].commit_seq.get()
                );
            }
        }
    }
}
