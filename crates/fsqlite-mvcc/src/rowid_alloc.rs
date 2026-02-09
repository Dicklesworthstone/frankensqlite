//! Coordinator-owned RowId allocator for `BEGIN CONCURRENT` mode (§5.10.1.1).
//!
//! In serialized mode, `OP_NewRowid` uses `max(rowid)+1` because writers hold
//! `WAL_WRITE_LOCK`. In concurrent mode, multiple writers share a snapshot, so
//! a **global per-table allocator** prevents duplicate RowIds.
//!
//! The allocator state lives in the coordinator (§5.9) and is **not** stored in
//! the SQLite file format. Single-process deployments use an in-memory map;
//! multi-process deployments serve reservations over IPC (`ROWID_RESERVE`).

use std::collections::HashMap;

use fsqlite_types::{RowId, RowIdMode, SchemaEpoch, TableId};
use parking_lot::Mutex;
use tracing::{debug, error, info, warn};

use crate::coordinator_ipc::{RowidReservePayload, RowidReserveResponse};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default range size for range reservations (§5.10.1.1 recommends 32–64).
pub const DEFAULT_RANGE_SIZE: u32 = 64;

/// SQLite error code: database or object is full.
pub const SQLITE_FULL: u32 = 13;

/// SQLite error code: schema changed.
pub const SQLITE_SCHEMA: u32 = 17;

// ---------------------------------------------------------------------------
// Key + per-table state
// ---------------------------------------------------------------------------

/// Composite key for the per-table allocator map.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AllocatorKey {
    pub schema_epoch: SchemaEpoch,
    pub table_id: TableId,
}

/// Per-table allocator state owned by the coordinator.
#[derive(Debug, Clone)]
struct TableAllocatorState {
    /// Next rowid to hand out (always ≥ 1).
    next_rowid: i64,
    /// Normal vs AUTOINCREMENT.
    mode: RowIdMode,
    /// High-water mark for AUTOINCREMENT `sqlite_sequence` persistence.
    autoincrement_high_water: i64,
}

impl TableAllocatorState {
    /// Initialise from the durable tip (§5.10.1.1 "Coordinator Initialization").
    fn new(max_committed_rowid: Option<RowId>, sqlite_sequence_seq: i64, mode: RowIdMode) -> Self {
        let max_committed = max_committed_rowid.map_or(0, RowId::get);
        let next = match mode {
            RowIdMode::Normal => max_committed.saturating_add(1).max(1),
            RowIdMode::AutoIncrement => {
                let base = max_committed.max(sqlite_sequence_seq);
                base.saturating_add(1).max(1)
            }
        };

        info!(
            max_committed_rowid = max_committed,
            sqlite_sequence_seq,
            next_rowid = next,
            source = "durable_tip",
            "allocator init from durable tip"
        );

        Self {
            next_rowid: next,
            mode,
            autoincrement_high_water: if mode == RowIdMode::AutoIncrement {
                sqlite_sequence_seq
            } else {
                0
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Error from the concurrent RowId allocator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowIdAllocError {
    /// RowId space exhausted (`SQLITE_FULL`).
    Exhausted,
    /// Schema epoch mismatch (`SQLITE_SCHEMA`).
    SchemaMismatch {
        requested: SchemaEpoch,
        current: SchemaEpoch,
    },
    /// Table not yet initialised — call `init_table` first.
    NotInitialized(AllocatorKey),
}

impl std::fmt::Display for RowIdAllocError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exhausted => f.write_str("rowid space exhausted (SQLITE_FULL)"),
            Self::SchemaMismatch { requested, current } => {
                write!(
                    f,
                    "schema epoch mismatch: requested {}, current {} (SQLITE_SCHEMA)",
                    requested.get(),
                    current.get()
                )
            }
            Self::NotInitialized(key) => {
                write!(
                    f,
                    "table allocator not initialized: epoch={}, table={}",
                    key.schema_epoch.get(),
                    key.table_id.get()
                )
            }
        }
    }
}

impl std::error::Error for RowIdAllocError {}

// ---------------------------------------------------------------------------
// Range reservation
// ---------------------------------------------------------------------------

/// A reserved range of RowIds returned to a connection.
///
/// Represents the half-open interval `[start_rowid, start_rowid + count)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeReservation {
    /// First rowid in the range (inclusive).
    pub start_rowid: RowId,
    /// Number of rowids in the range.
    pub count: u32,
}

impl RangeReservation {
    /// The last rowid in the range (inclusive).
    #[must_use]
    pub fn end_rowid_inclusive(&self) -> RowId {
        RowId::new(self.start_rowid.get() + i64::from(self.count) - 1)
    }
}

// ---------------------------------------------------------------------------
// Per-connection local cache
// ---------------------------------------------------------------------------

/// Per-connection local RowId cache backed by a range reservation.
///
/// Connections draw from this cache to avoid an atomic op per row. When the
/// cache is exhausted, a new range is reserved from the coordinator.
#[derive(Debug, Clone)]
pub struct LocalRowIdCache {
    /// Current position within the reserved range.
    next: i64,
    /// End of the reserved range (exclusive).
    end: i64,
    /// Table key for re-reservation.
    key: AllocatorKey,
}

impl LocalRowIdCache {
    /// Create a new local cache from a range reservation.
    #[must_use]
    pub fn new(reservation: RangeReservation, key: AllocatorKey) -> Self {
        Self {
            next: reservation.start_rowid.get(),
            end: reservation.start_rowid.get() + i64::from(reservation.count),
            key,
        }
    }

    /// Allocate the next rowid from the local cache.
    ///
    /// Returns `None` if the cache is exhausted (caller should reserve a new range).
    pub fn allocate(&mut self) -> Option<RowId> {
        if self.next < self.end {
            let rowid = RowId::new(self.next);
            self.next += 1;
            Some(rowid)
        } else {
            None
        }
    }

    /// Number of remaining rowids in the cache.
    #[must_use]
    pub fn remaining(&self) -> u32 {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let r = (self.end - self.next).max(0) as u32;
        r
    }

    /// The allocator key this cache is associated with.
    #[must_use]
    pub const fn key(&self) -> AllocatorKey {
        self.key
    }
}

// ---------------------------------------------------------------------------
// Coordinator-owned allocator
// ---------------------------------------------------------------------------

/// Coordinator-owned concurrent RowId allocator (§5.10.1.1).
///
/// Thread-safe. Maintains a per-`(SchemaEpoch, TableId)` allocator map.
/// Used by both in-process connections and cross-process IPC.
#[derive(Debug)]
pub struct ConcurrentRowIdAllocator {
    /// Current durable schema epoch (for cross-process validation).
    current_epoch: Mutex<SchemaEpoch>,
    /// Per-table allocator states.
    tables: Mutex<HashMap<AllocatorKey, TableAllocatorState>>,
}

impl ConcurrentRowIdAllocator {
    /// Create a new allocator with the given current schema epoch.
    #[must_use]
    pub fn new(current_epoch: SchemaEpoch) -> Self {
        Self {
            current_epoch: Mutex::new(current_epoch),
            tables: Mutex::new(HashMap::new()),
        }
    }

    /// Initialise (or re-initialise) a table's allocator from the durable tip.
    ///
    /// Called lazily on first use of a `(schema_epoch, table_id)` key, or
    /// after coordinator restart. Idempotent: a later call overwrites the
    /// previous state.
    pub fn init_table(
        &self,
        key: AllocatorKey,
        max_committed_rowid: Option<RowId>,
        sqlite_sequence_seq: i64,
        mode: RowIdMode,
    ) {
        let state = TableAllocatorState::new(max_committed_rowid, sqlite_sequence_seq, mode);
        self.tables.lock().insert(key, state);
    }

    /// Update the current durable schema epoch.
    pub fn set_current_epoch(&self, epoch: SchemaEpoch) {
        *self.current_epoch.lock() = epoch;
    }

    /// Get the current durable schema epoch.
    #[must_use]
    pub fn current_epoch(&self) -> SchemaEpoch {
        *self.current_epoch.lock()
    }

    /// Reserve a range of RowIds for a table.
    ///
    /// The coordinator advances the allocator by `count` even if the caller
    /// later aborts (gaps are permitted and intentional per §5.10.1.1).
    pub fn reserve_range(
        &self,
        key: AllocatorKey,
        count: u32,
    ) -> Result<RangeReservation, RowIdAllocError> {
        if count == 0 {
            return Ok(RangeReservation {
                start_rowid: RowId::new(1),
                count: 0,
            });
        }

        let mut tables = self.tables.lock();
        let state = tables
            .get_mut(&key)
            .ok_or(RowIdAllocError::NotInitialized(key))?;

        let start = state.next_rowid;

        // In concurrent mode, rowids are always in [1, MAX].  If the allocator
        // wrapped past MAX (via `wrapping_add`), `start` becomes negative and
        // we are exhausted.
        if start < 1 {
            error!(
                attempted_next = start,
                max_rowid = RowId::MAX.get(),
                "MAX_ROWID saturation: SQLITE_FULL"
            );
            return Err(RowIdAllocError::Exhausted);
        }

        let count_i64 = i64::from(count);

        // Remaining capacity: how many rowids [start, MAX] are available.
        // Safe because start >= 1 and MAX = i64::MAX.
        let remaining_capacity = RowId::MAX.get() - start + 1;
        if count_i64 > remaining_capacity {
            error!(
                attempted_next = start,
                count,
                remaining_capacity,
                max_rowid = RowId::MAX.get(),
                "MAX_ROWID saturation: SQLITE_FULL"
            );
            return Err(RowIdAllocError::Exhausted);
        }

        // Advance.  When we allocate exactly through MAX, wrapping_add sends
        // next_rowid to i64::MIN, which the `start < 1` guard catches next time.
        state.next_rowid = start.wrapping_add(count_i64);

        // Update AUTOINCREMENT high-water (last allocated rowid in range).
        if state.mode == RowIdMode::AutoIncrement {
            let last_in_range = start + count_i64 - 1;
            state.autoincrement_high_water = state.autoincrement_high_water.max(last_in_range);
        }

        let next_after = state.next_rowid;
        drop(tables);

        debug!(
            schema_epoch = key.schema_epoch.get(),
            table_id = key.table_id.get(),
            start_rowid = start,
            count,
            next_rowid_after = next_after,
            "range reservation"
        );

        Ok(RangeReservation {
            start_rowid: RowId::new(start),
            count,
        })
    }

    /// Allocate a single RowId (convenience wrapper over `reserve_range`).
    pub fn allocate_one(&self, key: AllocatorKey) -> Result<RowId, RowIdAllocError> {
        self.reserve_range(key, 1).map(|r| r.start_rowid)
    }

    /// Bump the allocator past an explicit rowid value (§5.10.1.1).
    ///
    /// If a statement inserts an explicit rowid `r`, the allocator's next value
    /// must be at least `r + 1`. This preserves `max(rowid)+1` behaviour and
    /// AUTOINCREMENT's "highest ever" rule under mixed explicit/implicit inserts.
    pub fn bump_explicit(
        &self,
        key: AllocatorKey,
        explicit_rowid: RowId,
    ) -> Result<(), RowIdAllocError> {
        let mut tables = self.tables.lock();
        let state = tables
            .get_mut(&key)
            .ok_or(RowIdAllocError::NotInitialized(key))?;

        let r = explicit_rowid.get();
        let before = state.next_rowid;

        if r >= state.next_rowid {
            // wrapping_add(1) at MAX → i64::MIN, caught by `start < 1` guard
            // in reserve_range.
            state.next_rowid = r.wrapping_add(1);
        }

        // Update AUTOINCREMENT high-water for explicit inserts too.
        if state.mode == RowIdMode::AutoIncrement {
            state.autoincrement_high_water = state.autoincrement_high_water.max(r);
        }

        let next_after = state.next_rowid;
        drop(tables);

        info!(
            explicit_rowid = r,
            allocator_next_before = before,
            allocator_next_after = next_after,
            "bump-on-explicit-rowid"
        );

        Ok(())
    }

    /// Get the AUTOINCREMENT high-water mark for `sqlite_sequence` persistence.
    ///
    /// The committing transaction MUST update `sqlite_sequence` to at least
    /// this value via a monotone-max `UpdateExpression`.
    #[must_use]
    pub fn autoincrement_high_water(&self, key: &AllocatorKey) -> Option<i64> {
        self.tables
            .lock()
            .get(key)
            .map(|s| s.autoincrement_high_water)
    }

    /// Handle a cross-process `ROWID_RESERVE` IPC request (§5.9.0).
    ///
    /// Validates schema epoch, then delegates to `reserve_range`.
    pub fn handle_rowid_reserve(&self, payload: &RowidReservePayload) -> RowidReserveResponse {
        let current_epoch = self.current_epoch();
        let requested_epoch = SchemaEpoch::new(payload.schema_epoch);

        if requested_epoch != current_epoch {
            warn!(
                requested_epoch = requested_epoch.get(),
                current_epoch = current_epoch.get(),
                "schema epoch mismatch on ROWID_RESERVE"
            );
            return RowidReserveResponse::Err {
                code: SQLITE_SCHEMA,
            };
        }

        let key = AllocatorKey {
            schema_epoch: requested_epoch,
            table_id: TableId::new(payload.table_id),
        };

        match self.reserve_range(key, payload.count) {
            Ok(reservation) => {
                // In concurrent mode, rowids are always positive (≥ 1), so the
                // i64 → u64 cast is safe.
                #[allow(clippy::cast_sign_loss)]
                let start = reservation.start_rowid.get() as u64;
                RowidReserveResponse::Ok {
                    start_rowid: start,
                    count: reservation.count,
                }
            }
            Err(RowIdAllocError::Exhausted | RowIdAllocError::NotInitialized(_)) => {
                RowidReserveResponse::Err { code: SQLITE_FULL }
            }
            Err(RowIdAllocError::SchemaMismatch { .. }) => RowidReserveResponse::Err {
                code: SQLITE_SCHEMA,
            },
        }
    }

    /// Check whether a table allocator has been initialised.
    #[must_use]
    pub fn is_initialized(&self, key: &AllocatorKey) -> bool {
        self.tables.lock().contains_key(key)
    }

    /// Current `next_rowid` for a table (testing / debugging).
    #[must_use]
    pub fn next_rowid(&self, key: &AllocatorKey) -> Option<i64> {
        self.tables.lock().get(key).map(|s| s.next_rowid)
    }
}

// ===========================================================================
// Tests (bd-3t3.11 §5.10.1.1)
// ===========================================================================

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn epoch(n: u64) -> SchemaEpoch {
        SchemaEpoch::new(n)
    }

    fn table(n: u32) -> TableId {
        TableId::new(n)
    }

    fn key(e: u64, t: u32) -> AllocatorKey {
        AllocatorKey {
            schema_epoch: epoch(e),
            table_id: table(t),
        }
    }

    // ── 1. test_rowid_allocator_basic ──

    #[test]
    fn test_rowid_allocator_basic() {
        let alloc = ConcurrentRowIdAllocator::new(epoch(1));
        let k = key(1, 42);

        // Table with max committed rowid = 5 → allocator starts at 6.
        alloc.init_table(k, Some(RowId::new(5)), 0, RowIdMode::Normal);

        let mut ids = Vec::new();
        for _ in 0..10 {
            ids.push(alloc.allocate_one(k).unwrap());
        }

        // Verify monotonically increasing starting from 6.
        assert_eq!(ids[0].get(), 6);
        for window in ids.windows(2) {
            assert!(window[1].get() > window[0].get(), "must be monotone");
        }
        assert_eq!(ids[9].get(), 15);
    }

    // ── 2. test_rowid_allocator_concurrent_uniqueness ──

    #[test]
    fn test_rowid_allocator_concurrent_uniqueness() {
        use std::sync::Arc;

        let alloc = Arc::new(ConcurrentRowIdAllocator::new(epoch(1)));
        let k = key(1, 100);
        alloc.init_table(k, None, 0, RowIdMode::Normal);

        let mut handles = Vec::new();
        for _ in 0..2 {
            let alloc = Arc::clone(&alloc);
            handles.push(std::thread::spawn(move || {
                let mut ids = Vec::with_capacity(100);
                for _ in 0..100 {
                    ids.push(alloc.allocate_one(k).unwrap());
                }
                ids
            }));
        }

        let mut all_ids = HashSet::new();
        for h in handles {
            for id in h.join().unwrap() {
                assert!(all_ids.insert(id.get()), "duplicate rowid {}", id.get());
            }
        }
        assert_eq!(all_ids.len(), 200);
    }

    // ── 3. test_rowid_allocator_gap_on_abort ──

    #[test]
    fn test_rowid_allocator_gap_on_abort() {
        let alloc = ConcurrentRowIdAllocator::new(epoch(1));
        let k = key(1, 1);
        alloc.init_table(k, None, 0, RowIdMode::Normal);

        // Writer 1 allocates 5 rowids then "aborts" (discards them).
        let first_batch: Vec<_> = (0..5).map(|_| alloc.allocate_one(k).unwrap()).collect();
        assert_eq!(first_batch[0].get(), 1);
        assert_eq!(first_batch[4].get(), 5);

        // Writer 2 gets rowids AFTER the gap — no reuse.
        let next = alloc.allocate_one(k).unwrap();
        assert_eq!(next.get(), 6, "must not reuse aborted rowids");
    }

    // ── 4. test_rowid_bump_on_explicit ──

    #[test]
    fn test_rowid_bump_on_explicit() {
        let alloc = ConcurrentRowIdAllocator::new(epoch(1));
        let k = key(1, 1);
        alloc.init_table(k, None, 0, RowIdMode::Normal);

        // Allocate a few.
        let _ = alloc.allocate_one(k).unwrap(); // 1
        let _ = alloc.allocate_one(k).unwrap(); // 2

        // Explicit insert at rowid 1000.
        alloc.bump_explicit(k, RowId::new(1000)).unwrap();

        // Next auto-allocated rowid must be >= 1001.
        let next = alloc.allocate_one(k).unwrap();
        assert!(next.get() >= 1001, "got {}", next.get());
    }

    // ── 5. test_rowid_autoincrement_init ──

    #[test]
    fn test_rowid_autoincrement_init() {
        let alloc = ConcurrentRowIdAllocator::new(epoch(1));
        let k = key(1, 1);

        // sqlite_sequence.seq=500, max_committed_rowid=400.
        // Allocator must start at max(400, 500) + 1 = 501.
        alloc.init_table(k, Some(RowId::new(400)), 500, RowIdMode::AutoIncrement);

        let r = alloc.allocate_one(k).unwrap();
        assert_eq!(r.get(), 501);
    }

    // ── 6. test_rowid_autoincrement_persist ──

    #[test]
    fn test_rowid_autoincrement_persist() {
        let alloc = ConcurrentRowIdAllocator::new(epoch(1));
        let k = key(1, 1);
        alloc.init_table(k, None, 0, RowIdMode::AutoIncrement);

        // Allocate 5 rowids (1..5).
        for _ in 0..5 {
            let _ = alloc.allocate_one(k).unwrap();
        }

        // High-water should be at least the max allocated rowid (5).
        let hw = alloc.autoincrement_high_water(&k).unwrap();
        assert!(hw >= 5, "high_water={hw}, expected >= 5");
    }

    // ── 7. test_rowid_autoincrement_monotone_max_merge ──

    #[test]
    fn test_rowid_autoincrement_monotone_max_merge() {
        let alloc = ConcurrentRowIdAllocator::new(epoch(1));
        let k = key(1, 1);
        alloc.init_table(k, None, 0, RowIdMode::AutoIncrement);

        // Txn A: allocate range of 10 → rowids 1..10.
        let range_a = alloc.reserve_range(k, 10).unwrap();
        assert_eq!(range_a.start_rowid.get(), 1);

        // Txn B: allocate range of 20 → rowids 11..30.
        let range_b = alloc.reserve_range(k, 20).unwrap();
        assert_eq!(range_b.start_rowid.get(), 11);

        // The high-water reflects the max of both (30).
        let hw = alloc.autoincrement_high_water(&k).unwrap();
        assert_eq!(hw, 30, "monotone max must reflect both txns");
    }

    // ── 8. test_rowid_range_reservation ──

    #[test]
    fn test_rowid_range_reservation() {
        let alloc = ConcurrentRowIdAllocator::new(epoch(1));
        let k = key(1, 1);
        alloc.init_table(k, Some(RowId::new(100)), 0, RowIdMode::Normal);

        // Reserve range of 64 → [101, 164].
        let range = alloc.reserve_range(k, 64).unwrap();
        assert_eq!(range.start_rowid.get(), 101);
        assert_eq!(range.count, 64);
        assert_eq!(range.end_rowid_inclusive().get(), 164);

        // Build a local cache and allocate from it.
        let mut cache = LocalRowIdCache::new(range, k);
        assert_eq!(cache.remaining(), 64);

        let first = cache.allocate().unwrap();
        assert_eq!(first.get(), 101);
        assert_eq!(cache.remaining(), 63);

        // Exhaust the cache.
        for _ in 0..63 {
            assert!(cache.allocate().is_some());
        }
        assert_eq!(cache.remaining(), 0);
        assert!(cache.allocate().is_none(), "cache must be exhausted");

        // Next range from coordinator continues from 165.
        let range2 = alloc.reserve_range(k, 32).unwrap();
        assert_eq!(range2.start_rowid.get(), 165);
    }

    // ── 9. test_rowid_max_saturation ──

    #[test]
    fn test_rowid_max_saturation() {
        let alloc = ConcurrentRowIdAllocator::new(epoch(1));
        let k = key(1, 1);

        // Set allocator near MAX_ROWID.
        alloc.init_table(k, Some(RowId::new(i64::MAX - 5)), 0, RowIdMode::Normal);

        // Can allocate 5 more.
        for i in 0..5 {
            let r = alloc.allocate_one(k);
            assert!(r.is_ok(), "allocation {i} should succeed");
        }

        // Next allocation must fail with Exhausted.
        let r = alloc.allocate_one(k);
        assert_eq!(r, Err(RowIdAllocError::Exhausted));

        // Range reservation that would overflow also fails.
        alloc.init_table(k, Some(RowId::new(i64::MAX - 10)), 0, RowIdMode::Normal);
        let r = alloc.reserve_range(k, 20);
        assert_eq!(r, Err(RowIdAllocError::Exhausted));

        // But a range that fits still works.
        let r = alloc.reserve_range(k, 10);
        assert!(r.is_ok());
    }

    // ── 10. test_rowid_schema_epoch_mismatch ──

    #[test]
    fn test_rowid_schema_epoch_mismatch() {
        let alloc = ConcurrentRowIdAllocator::new(epoch(5));
        let k = key(5, 1);
        alloc.init_table(k, None, 0, RowIdMode::Normal);

        // Build a ROWID_RESERVE payload with stale epoch (3 ≠ 5).
        let payload = RowidReservePayload {
            txn: crate::coordinator_ipc::WireTxnToken {
                txn_id: 1,
                txn_epoch: 1,
            },
            schema_epoch: 3,
            table_id: 1,
            count: 10,
        };

        let resp = alloc.handle_rowid_reserve(&payload);
        assert_eq!(
            resp,
            RowidReserveResponse::Err {
                code: SQLITE_SCHEMA
            }
        );

        // Correct epoch works.
        let payload_ok = RowidReservePayload {
            txn: crate::coordinator_ipc::WireTxnToken {
                txn_id: 1,
                txn_epoch: 1,
            },
            schema_epoch: 5,
            table_id: 1,
            count: 10,
        };
        let resp_ok = alloc.handle_rowid_reserve(&payload_ok);
        assert!(matches!(resp_ok, RowidReserveResponse::Ok { .. }));
    }

    // ── 11. test_rowid_coordinator_restart_init ──

    #[test]
    fn test_rowid_coordinator_restart_init() {
        let k = key(1, 1);

        // Coordinator 1: allocate some rowids.
        let alloc1 = ConcurrentRowIdAllocator::new(epoch(1));
        alloc1.init_table(k, Some(RowId::new(100)), 0, RowIdMode::Normal);
        for _ in 0..50 {
            let _ = alloc1.allocate_one(k).unwrap();
        }
        // alloc1.next_rowid is now 151.

        // Simulate restart: new coordinator, re-init from durable tip.
        // The durable tip still shows max_committed = 100 (the 50 allocated
        // rowids were from a writer that committed, pushing max to e.g. 150).
        // Use max_committed = 150 as the new tip.
        let alloc2 = ConcurrentRowIdAllocator::new(epoch(1));
        alloc2.init_table(k, Some(RowId::new(150)), 0, RowIdMode::Normal);

        let r = alloc2.allocate_one(k).unwrap();
        assert_eq!(
            r.get(),
            151,
            "re-init from durable tip must be non-duplicate"
        );

        // Also verify AUTOINCREMENT re-init from sqlite_sequence.
        let ka = key(1, 2);
        alloc2.init_table(ka, Some(RowId::new(80)), 200, RowIdMode::AutoIncrement);
        let ra = alloc2.allocate_one(ka).unwrap();
        assert_eq!(
            ra.get(),
            201,
            "AUTOINCREMENT re-init: max(80, 200) + 1 = 201"
        );
    }

    // ── Additional: test_handle_rowid_reserve_ok ──

    #[test]
    fn test_handle_rowid_reserve_ok() {
        let alloc = ConcurrentRowIdAllocator::new(epoch(1));
        let k = key(1, 42);
        alloc.init_table(k, Some(RowId::new(10)), 0, RowIdMode::Normal);

        let payload = RowidReservePayload {
            txn: crate::coordinator_ipc::WireTxnToken {
                txn_id: 99,
                txn_epoch: 1,
            },
            schema_epoch: 1,
            table_id: 42,
            count: 32,
        };

        let resp = alloc.handle_rowid_reserve(&payload);
        match resp {
            RowidReserveResponse::Ok { start_rowid, count } => {
                assert_eq!(start_rowid, 11);
                assert_eq!(count, 32);
            }
            RowidReserveResponse::Err { code } => panic!("unexpected error: {code}"),
        }
    }

    // ── Additional: test_empty_range_reservation ──

    #[test]
    fn test_empty_range_reservation() {
        let alloc = ConcurrentRowIdAllocator::new(epoch(1));
        let k = key(1, 1);
        alloc.init_table(k, None, 0, RowIdMode::Normal);

        // Zero-count reservation is a no-op.
        let r = alloc.reserve_range(k, 0).unwrap();
        assert_eq!(r.count, 0);

        // Allocator position unchanged.
        let next = alloc.allocate_one(k).unwrap();
        assert_eq!(next.get(), 1);
    }

    // ── Additional: test_bump_explicit_no_retreat ──

    #[test]
    fn test_bump_explicit_no_retreat() {
        let alloc = ConcurrentRowIdAllocator::new(epoch(1));
        let k = key(1, 1);
        alloc.init_table(k, Some(RowId::new(1000)), 0, RowIdMode::Normal);

        // Bump to 500 — should NOT move allocator backwards.
        alloc.bump_explicit(k, RowId::new(500)).unwrap();
        let next = alloc.allocate_one(k).unwrap();
        assert_eq!(next.get(), 1001, "bump below current must not retreat");
    }

    // ── Additional: test_not_initialized_error ──

    #[test]
    fn test_not_initialized_error() {
        let alloc = ConcurrentRowIdAllocator::new(epoch(1));
        let k = key(1, 999);

        let r = alloc.allocate_one(k);
        assert_eq!(r, Err(RowIdAllocError::NotInitialized(k)));

        let r = alloc.bump_explicit(k, RowId::new(5));
        assert_eq!(r, Err(RowIdAllocError::NotInitialized(k)));
    }

    // ── Additional: test_bump_explicit_at_max_rowid ──

    #[test]
    fn test_bump_explicit_at_max_rowid() {
        let alloc = ConcurrentRowIdAllocator::new(epoch(1));
        let k = key(1, 1);
        alloc.init_table(k, None, 0, RowIdMode::Normal);

        // Bump to MAX_ROWID.
        alloc.bump_explicit(k, RowId::MAX).unwrap();

        // Next allocation must fail.
        let r = alloc.allocate_one(k);
        assert_eq!(r, Err(RowIdAllocError::Exhausted));
    }
}
