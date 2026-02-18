//! B-tree operation observability counters.
//!
//! This module exposes lightweight process-local counters used by the
//! `btree_op` tracing lane and bead-level telemetry verification.

use std::sync::atomic::{AtomicU64, Ordering};

/// Supported B-tree operation types for observability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BtreeOpType {
    /// Cursor seek operation (`table_move_to` / `index_move_to`).
    Seek,
    /// Mutation insert operation.
    Insert,
    /// Mutation delete operation.
    Delete,
}

impl BtreeOpType {
    /// Stable label used in logs and metrics dimensions.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Seek => "seek",
            Self::Insert => "insert",
            Self::Delete => "delete",
        }
    }
}

/// Snapshot of per-operation totals for `fsqlite_btree_operations_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BtreeOperationTotals {
    /// Number of seek operations.
    pub seek: u64,
    /// Number of insert operations.
    pub insert: u64,
    /// Number of delete operations.
    pub delete: u64,
}

/// Snapshot of B-tree observability metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BtreeMetricsSnapshot {
    /// Counter by operation type.
    pub fsqlite_btree_operations_total: BtreeOperationTotals,
    /// Total number of split events observed.
    pub fsqlite_btree_page_splits_total: u64,
    /// Current B-tree depth gauge.
    pub fsqlite_btree_depth: u64,
    /// Total number of Swiss Table probes (lookups/inserts/removes).
    pub fsqlite_swiss_table_probes_total: u64,
    /// Current Swiss Table load factor (scaled by 1000, e.g. 875 = 0.875).
    pub fsqlite_swiss_table_load_factor: u64,
    /// Swizzle ratio gauge (0–1000, where 1000 = 100% swizzled).
    pub fsqlite_swizzle_ratio: u64,
    /// Total swizzle faults (CAS failures).
    pub fsqlite_swizzle_faults_total: u64,
    /// Total successful swizzle-in operations.
    pub fsqlite_swizzle_in_total: u64,
    /// Total successful unswizzle-out operations.
    pub fsqlite_swizzle_out_total: u64,
}

/// Per-operation mutable stats while a `btree_op` span is active.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct BtreeOpRuntimeStats {
    pub(crate) pages_visited: u64,
    pub(crate) splits: u64,
    pub(crate) merges: u64,
}

impl BtreeOpRuntimeStats {
    pub(crate) fn record_page_visit(&mut self) {
        self.pages_visited = self.pages_visited.saturating_add(1);
    }

    pub(crate) fn record_split(&mut self) {
        self.splits = self.splits.saturating_add(1);
    }

    pub(crate) fn record_merge(&mut self) {
        self.merges = self.merges.saturating_add(1);
    }
}

static BTREE_OP_SEEK_TOTAL: AtomicU64 = AtomicU64::new(0);
static BTREE_OP_INSERT_TOTAL: AtomicU64 = AtomicU64::new(0);
static BTREE_OP_DELETE_TOTAL: AtomicU64 = AtomicU64::new(0);
static BTREE_PAGE_SPLITS_TOTAL: AtomicU64 = AtomicU64::new(0);
static BTREE_DEPTH_GAUGE: AtomicU64 = AtomicU64::new(0);
static SWISS_TABLE_PROBES_TOTAL: AtomicU64 = AtomicU64::new(0);
static SWISS_TABLE_LOAD_FACTOR: AtomicU64 = AtomicU64::new(0);

// ── Swizzle metrics (bd-3ta.3) ──────────────────────────────────────────────

/// Swizzle ratio gauge: (swizzled_count / total_tracked) * 1000.
static SWIZZLE_RATIO_GAUGE: AtomicU64 = AtomicU64::new(0);
/// Total swizzle faults (CAS failures + retry attempts).
static SWIZZLE_FAULTS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Total successful swizzle operations.
static SWIZZLE_IN_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Total successful unswizzle operations.
static SWIZZLE_OUT_TOTAL: AtomicU64 = AtomicU64::new(0);

pub(crate) fn record_operation(op_type: BtreeOpType) {
    let counter = match op_type {
        BtreeOpType::Seek => &BTREE_OP_SEEK_TOTAL,
        BtreeOpType::Insert => &BTREE_OP_INSERT_TOTAL,
        BtreeOpType::Delete => &BTREE_OP_DELETE_TOTAL,
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_split_event() {
    BTREE_PAGE_SPLITS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn set_depth_gauge(depth: usize) {
    let depth_u64 = u64::try_from(depth).unwrap_or(u64::MAX);
    BTREE_DEPTH_GAUGE.store(depth_u64, Ordering::Relaxed);
}

/// Record a Swiss Table probe (lookup/insert/remove).
pub fn record_swiss_probe() {
    SWISS_TABLE_PROBES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Set Swiss Table load factor (scaled by 1000).
pub fn set_swiss_load_factor(load_factor_milli: u64) {
    SWISS_TABLE_LOAD_FACTOR.store(load_factor_milli, Ordering::Relaxed);
}

/// Record a successful swizzle-in event and emit a tracing span.
pub fn record_swizzle_in(page_id: u64) {
    SWIZZLE_IN_TOTAL.fetch_add(1, Ordering::Relaxed);
    let _span = tracing::trace_span!(
        "swizzle",
        page_id,
        swizzled_in = true,
        unswizzled_out = false,
    )
    .entered();
}

/// Record a successful unswizzle-out event and emit a tracing span.
pub fn record_swizzle_out(page_id: u64) {
    SWIZZLE_OUT_TOTAL.fetch_add(1, Ordering::Relaxed);
    let _span = tracing::trace_span!(
        "swizzle",
        page_id,
        swizzled_in = false,
        unswizzled_out = true,
    )
    .entered();
}

/// Record a swizzle fault (CAS failure or retry).
pub fn record_swizzle_fault() {
    SWIZZLE_FAULTS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Update the swizzle ratio gauge (0–1000, where 1000 = 100% swizzled).
pub fn set_swizzle_ratio(ratio_milli: u64) {
    SWIZZLE_RATIO_GAUGE.store(ratio_milli, Ordering::Relaxed);
}

/// Return a snapshot of B-tree observability counters.
#[must_use]
pub fn btree_metrics_snapshot() -> BtreeMetricsSnapshot {
    BtreeMetricsSnapshot {
        fsqlite_btree_operations_total: BtreeOperationTotals {
            seek: BTREE_OP_SEEK_TOTAL.load(Ordering::Relaxed),
            insert: BTREE_OP_INSERT_TOTAL.load(Ordering::Relaxed),
            delete: BTREE_OP_DELETE_TOTAL.load(Ordering::Relaxed),
        },
        fsqlite_btree_page_splits_total: BTREE_PAGE_SPLITS_TOTAL.load(Ordering::Relaxed),
        fsqlite_btree_depth: BTREE_DEPTH_GAUGE.load(Ordering::Relaxed),
        fsqlite_swiss_table_probes_total: SWISS_TABLE_PROBES_TOTAL.load(Ordering::Relaxed),
        fsqlite_swiss_table_load_factor: SWISS_TABLE_LOAD_FACTOR.load(Ordering::Relaxed),
        fsqlite_swizzle_ratio: SWIZZLE_RATIO_GAUGE.load(Ordering::Relaxed),
        fsqlite_swizzle_faults_total: SWIZZLE_FAULTS_TOTAL.load(Ordering::Relaxed),
        fsqlite_swizzle_in_total: SWIZZLE_IN_TOTAL.load(Ordering::Relaxed),
        fsqlite_swizzle_out_total: SWIZZLE_OUT_TOTAL.load(Ordering::Relaxed),
    }
}

/// Reset all B-tree observability counters.
pub fn reset_btree_metrics() {
    BTREE_OP_SEEK_TOTAL.store(0, Ordering::Relaxed);
    BTREE_OP_INSERT_TOTAL.store(0, Ordering::Relaxed);
    BTREE_OP_DELETE_TOTAL.store(0, Ordering::Relaxed);
    BTREE_PAGE_SPLITS_TOTAL.store(0, Ordering::Relaxed);
    BTREE_DEPTH_GAUGE.store(0, Ordering::Relaxed);
    SWISS_TABLE_PROBES_TOTAL.store(0, Ordering::Relaxed);
    SWISS_TABLE_LOAD_FACTOR.store(0, Ordering::Relaxed);
    SWIZZLE_RATIO_GAUGE.store(0, Ordering::Relaxed);
    SWIZZLE_FAULTS_TOTAL.store(0, Ordering::Relaxed);
    SWIZZLE_IN_TOTAL.store(0, Ordering::Relaxed);
    SWIZZLE_OUT_TOTAL.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::{BtreeOpType, btree_metrics_snapshot, record_operation};

    #[test]
    fn metrics_snapshot_tracks_operation_buckets() {
        let before = btree_metrics_snapshot();
        record_operation(BtreeOpType::Seek);
        record_operation(BtreeOpType::Seek);
        record_operation(BtreeOpType::Insert);

        let after = btree_metrics_snapshot();
        assert!(
            after.fsqlite_btree_operations_total.seek
                >= before.fsqlite_btree_operations_total.seek.saturating_add(2)
        );
        assert!(
            after.fsqlite_btree_operations_total.insert
                >= before
                    .fsqlite_btree_operations_total
                    .insert
                    .saturating_add(1)
        );
        assert!(
            after.fsqlite_btree_operations_total.delete
                >= before.fsqlite_btree_operations_total.delete
        );
    }
}
