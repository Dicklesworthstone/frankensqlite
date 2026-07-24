//! Cell-Level MVCC Dual-Path Routing (C-TRANSITION: bd-l9k8e.11)
//!
//! This module implements runtime routing between cell-level and page-level MVCC paths.
//! It provides:
//! - A routing predicate that decides which path to use for each operation
//! - Escalation logic when cell-level operations trigger structural changes
//! - PRAGMA control for enabling/disabling cell-level MVCC
//! - Comprehensive tracing for all routing decisions
//!
//! # Design Philosophy
//!
//! The cell-level MVCC path is new and may have bugs. This module acts as a safety
//! net, ensuring:
//! 1. Clean fallback to page-level if cell-level fails or is disabled
//! 2. Per-page escalation when a logical op turns structural mid-flight
//! 3. PRAGMA control for debugging and gradual rollout
//! 4. Correct handling of mixed cell/page-level pages across concurrent transactions
//!
//! # Routing Predicate
//!
//! The [`should_use_cell_path`] function implements the core decision:
//!
//! 1. Is cell_mvcc_enabled? If not, return false
//! 2. Is this a leaf page? Interior pages ALWAYS use page-level
//! 3. Is this a logical operation per C2 classification? If structural, return false
//! 4. Is this page already page-level tracked by another active txn? If so, return false
//! 5. Has this page hit the materialization threshold? If so, return false
//! 6. Would this txn's cell delta memory exceed budget? If so, return false
//!
//! # Escalation
//!
//! When a transaction starts cell-level on a page but discovers it needs a structural
//! operation (e.g., INSERT fills page and triggers split), we must escalate:
//!
//! 1. Materialize page from base + committed deltas at snapshot
//! 2. Apply this txn's uncommitted cell deltas to the materialized page
//! 3. Discard this txn's cell deltas for this page
//! 4. Switch to page-level for the rest of this txn on this page
//!
//! Escalation is one-way: cell->page, never page->cell within a transaction.

use std::collections::HashSet;
use std::sync::atomic::Ordering;

use fsqlite_types::sync_primitives::RwLock;
use fsqlite_types::{CommitSeq, PageData, PageNumber, Snapshot, TxnToken};
use tracing::{debug, info, trace, warn};

use crate::cell_mvcc_boundary::{BtreeOp, MvccOpClass, PageMetadata, classify_btree_op};
use crate::cell_visibility::{CellDelta, CellKey, CellVisibilityLog};
use crate::materialize::{MaterializationTrigger, materialize_page, should_materialize_eagerly};

// ---------------------------------------------------------------------------
// PRAGMA Control (§11.1)
// ---------------------------------------------------------------------------

/// Cell-level MVCC operating mode.
///
/// Controls how the routing predicate decides between cell-level and page-level paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CellMvccMode {
    /// Always try cell-level first (default after Track C ships).
    /// Cell-level is used for all leaf page operations that classify as LOGICAL.
    On,
    /// Always use page-level (safe fallback for debugging).
    /// All operations use the existing page-level MVCC path.
    Off,
    /// Cell-level for table leaf pages, page-level for index and interior pages.
    /// This is a conservative mode for initial rollout.
    #[default]
    Auto,
}

impl CellMvccMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::On => "on",
            Self::Off => "off",
            Self::Auto => "auto",
        }
    }
}

impl std::str::FromStr for CellMvccMode {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "on" | "true" | "1" => Ok(Self::On),
            "off" | "false" | "0" => Ok(Self::Off),
            "auto" => Ok(Self::Auto),
            _ => Err(()),
        }
    }
}

impl std::fmt::Display for CellMvccMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// ---------------------------------------------------------------------------
// Routing Decision Types (§11.2)
// ---------------------------------------------------------------------------

/// Why the routing predicate chose a particular path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingReason {
    /// Cell MVCC is disabled via PRAGMA.
    CellMvccDisabled,
    /// The target page is an interior page (always page-level).
    InteriorPage,
    /// The operation is structural (split, merge, etc.).
    StructuralOperation,
    /// Another transaction already has page-level tracking on this page.
    PageTrackedByOtherTxn,
    /// This page has exceeded the materialization threshold.
    MaterializationThresholdExceeded,
    /// This transaction would exceed its cell delta memory budget.
    MemoryBudgetExceeded,
    /// Index pages use page-level in AUTO mode.
    IndexPageAutoMode,
    /// Cell-level is eligible and enabled.
    CellLevelEligible,
}

impl RoutingReason {
    #[allow(dead_code)] // Used for tracing/logging
    fn as_str(self) -> &'static str {
        match self {
            Self::CellMvccDisabled => "cell_mvcc_disabled",
            Self::InteriorPage => "interior_page",
            Self::StructuralOperation => "structural_operation",
            Self::PageTrackedByOtherTxn => "page_tracked_by_other_txn",
            Self::MaterializationThresholdExceeded => "materialization_threshold",
            Self::MemoryBudgetExceeded => "memory_budget_exceeded",
            Self::IndexPageAutoMode => "index_page_auto_mode",
            Self::CellLevelEligible => "cell_level_eligible",
        }
    }
}

/// The result of routing a B-tree operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoutingDecision {
    /// Whether to use cell-level MVCC (true) or page-level (false).
    pub use_cell_level: bool,
    /// Why this decision was made.
    pub reason: RoutingReason,
}

impl RoutingDecision {
    /// Create a decision to use cell-level MVCC.
    #[must_use]
    pub const fn cell_level(reason: RoutingReason) -> Self {
        Self {
            use_cell_level: true,
            reason,
        }
    }

    /// Create a decision to use page-level MVCC.
    #[must_use]
    pub const fn page_level(reason: RoutingReason) -> Self {
        Self {
            use_cell_level: false,
            reason,
        }
    }
}

// ---------------------------------------------------------------------------
// Escalation Types (§11.3)
// ---------------------------------------------------------------------------

/// Result of an escalation from cell-level to page-level.
#[derive(Debug, Clone)]
pub struct EscalationResult {
    /// The materialized page incorporating all cell deltas.
    pub materialized_page: PageData,
    /// Number of cell deltas that were applied.
    pub deltas_applied: usize,
    /// Cell keys that were discarded from this transaction's tracking.
    pub discarded_cells: Vec<CellKey>,
}

/// Tracks which pages a transaction has escalated from cell-level to page-level.
///
/// Once escalated, a page stays page-level for the remainder of the transaction.
#[derive(Debug, Default)]
pub struct TxnEscalationTracker {
    /// Pages that have been escalated to page-level.
    escalated_pages: HashSet<PageNumber>,
    /// Pages currently using cell-level tracking.
    cell_level_pages: HashSet<PageNumber>,
}

impl TxnEscalationTracker {
    /// Create a new tracker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark a page as using cell-level tracking.
    pub fn add_cell_level(&mut self, page: PageNumber) {
        if !self.escalated_pages.contains(&page) {
            self.cell_level_pages.insert(page);
        }
    }

    /// Check if a page is using cell-level tracking.
    #[must_use]
    pub fn is_cell_level(&self, page: PageNumber) -> bool {
        self.cell_level_pages.contains(&page) && !self.escalated_pages.contains(&page)
    }

    /// Mark a page as escalated to page-level.
    ///
    /// After this call, [`Self::is_escalated`] returns true for this page.
    pub fn escalate(&mut self, page: PageNumber) {
        self.cell_level_pages.remove(&page);
        self.escalated_pages.insert(page);

        debug!(pgno = page.get(), "page_escalated_to_page_level");
    }

    /// Check if a page has been escalated to page-level.
    #[must_use]
    pub fn is_escalated(&self, page: PageNumber) -> bool {
        self.escalated_pages.contains(&page)
    }

    /// Get all pages currently using cell-level tracking.
    pub fn cell_level_pages(&self) -> impl Iterator<Item = PageNumber> + '_ {
        self.cell_level_pages.iter().copied()
    }

    /// Get all pages that have been escalated.
    pub fn escalated_pages(&self) -> impl Iterator<Item = PageNumber> + '_ {
        self.escalated_pages.iter().copied()
    }

    /// Reset the tracker (called on transaction commit/rollback).
    pub fn clear(&mut self) {
        self.cell_level_pages.clear();
        self.escalated_pages.clear();
    }
}

// ---------------------------------------------------------------------------
// Page Tracking State (§11.4)
// ---------------------------------------------------------------------------

/// Tracks which transactions have page-level locks on which pages.
///
/// This is used to prevent mixing cell-level and page-level tracking on the
/// same page across concurrent transactions.
#[derive(Debug, Default)]
pub struct PageTrackingState {
    /// Maps pages to the set of transactions with page-level tracking.
    page_level_txns: RwLock<std::collections::HashMap<PageNumber, HashSet<TxnToken>>>,
}

impl PageTrackingState {
    /// Create a new tracking state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register that a transaction has page-level tracking on a page.
    pub fn register_page_level(&self, page: PageNumber, txn: TxnToken) {
        let mut map = self.page_level_txns.write();
        map.entry(page).or_default().insert(txn);

        trace!(
            pgno = page.get(),
            txn_id = txn.id.get(),
            "registered_page_level_tracking"
        );
    }

    /// Unregister a transaction's page-level tracking on a page.
    pub fn unregister_page_level(&self, page: PageNumber, txn: TxnToken) {
        let mut map = self.page_level_txns.write();
        if let Some(txns) = map.get_mut(&page) {
            txns.remove(&txn);
            if txns.is_empty() {
                map.remove(&page);
            }
        }
    }

    /// Unregister all page-level tracking for a transaction.
    pub fn unregister_all(&self, txn: TxnToken) {
        let mut map = self.page_level_txns.write();
        map.retain(|_, txns| {
            txns.remove(&txn);
            !txns.is_empty()
        });
    }

    /// Check if any OTHER transaction has page-level tracking on a page.
    #[must_use]
    pub fn has_other_page_level_txn(&self, page: PageNumber, current_txn: TxnToken) -> bool {
        let map = self.page_level_txns.read();
        map.get(&page)
            .is_some_and(|txns| txns.iter().any(|t| *t != current_txn))
    }

    /// Get the count of transactions with page-level tracking on a page.
    #[must_use]
    pub fn page_level_txn_count(&self, page: PageNumber) -> usize {
        let map = self.page_level_txns.read();
        map.get(&page).map_or(0, HashSet::len)
    }
}

// ---------------------------------------------------------------------------
// Routing Context (§11.5)
// ---------------------------------------------------------------------------

/// Context for making routing decisions.
///
/// This bundles all the state needed by the routing predicate:
/// - Current MVCC mode (PRAGMA setting)
/// - Cell visibility log (for delta counts and memory tracking)
/// - Page tracking state (for detecting mixed cross-transaction tracking)
///
/// # Important Usage Note
///
/// This context does NOT include the per-transaction [`TxnEscalationTracker`].
/// The caller MUST check `TxnEscalationTracker::is_escalated(page)` BEFORE
/// calling [`should_use_cell_path`]. If the page is already escalated for this
/// transaction, use page-level directly without calling the routing predicate.
///
/// The two-level check is:
/// 1. Per-transaction: `TxnEscalationTracker::is_escalated()` (caller's responsibility)
/// 2. Cross-transaction: `PageTrackingState` (handled by `should_use_cell_path`)
#[derive(Debug)]
pub struct RoutingContext<'a> {
    /// Current cell MVCC mode.
    pub mode: CellMvccMode,
    /// Reference to the cell visibility log.
    pub cell_log: &'a CellVisibilityLog,
    /// Reference to the page tracking state.
    pub page_tracking: &'a PageTrackingState,
    /// The current transaction.
    pub current_txn: TxnToken,
    /// Materialization threshold (delta count per page).
    pub materialization_threshold: usize,
}

// ---------------------------------------------------------------------------
// Routing Predicate (§11.6)
// ---------------------------------------------------------------------------

/// Determine whether to use cell-level or page-level MVCC for an operation.
///
/// This is the core routing predicate that implements the decision tree:
///
/// 1. Is cell_mvcc enabled? If not, use page-level.
/// 2. Is this a leaf page? Interior pages always use page-level.
/// 3. Is this a logical operation? Structural operations use page-level.
/// 4. Is another txn using page-level on this page? If so, use page-level.
/// 5. Has this page hit the materialization threshold? If so, use page-level.
/// 6. Would this txn exceed its memory budget? If so, use page-level.
///
/// # Prerequisites
///
/// **The caller MUST check `TxnEscalationTracker::is_escalated(page)` BEFORE
/// calling this function.** If the page is already escalated for this transaction,
/// use page-level directly. This function only checks cross-transaction state,
/// not per-transaction escalation.
///
/// # Arguments
///
/// * `ctx` - The routing context with all necessary state
/// * `page` - Metadata about the target page
/// * `op` - The B-tree operation being performed
///
/// # Returns
///
/// A [`RoutingDecision`] indicating which path to use and why.
#[must_use]
pub fn should_use_cell_path(
    ctx: &RoutingContext<'_>,
    page: &PageMetadata,
    op: &BtreeOp,
) -> RoutingDecision {
    // 1. Check mode
    match ctx.mode {
        CellMvccMode::Off => {
            trace!(
                pgno = page.page_no.get(),
                mode = "off",
                "routing_cell_mvcc_disabled"
            );
            return RoutingDecision::page_level(RoutingReason::CellMvccDisabled);
        }
        CellMvccMode::Auto if !page.is_table => {
            // In AUTO mode, index pages use page-level
            trace!(
                pgno = page.page_no.get(),
                mode = "auto",
                is_index = true,
                "routing_index_page_auto_mode"
            );
            return RoutingDecision::page_level(RoutingReason::IndexPageAutoMode);
        }
        _ => {}
    }

    // 2. Check if interior page (always page-level)
    if !page.is_leaf {
        trace!(
            pgno = page.page_no.get(),
            is_leaf = false,
            "routing_interior_page"
        );
        return RoutingDecision::page_level(RoutingReason::InteriorPage);
    }

    // 3. Check operation classification
    let op_class = classify_btree_op(op, page);
    if op_class == MvccOpClass::Structural {
        trace!(
            pgno = page.page_no.get(),
            op = ?op,
            "routing_structural_operation"
        );
        return RoutingDecision::page_level(RoutingReason::StructuralOperation);
    }

    // 4. Check if another transaction has page-level tracking
    if ctx
        .page_tracking
        .has_other_page_level_txn(page.page_no, ctx.current_txn)
    {
        debug!(
            pgno = page.page_no.get(),
            txn_id = ctx.current_txn.id.get(),
            "routing_page_tracked_by_other_txn"
        );
        return RoutingDecision::page_level(RoutingReason::PageTrackedByOtherTxn);
    }

    // 5. Check materialization threshold
    let delta_count = ctx.cell_log.page_delta_count(page.page_no);
    if should_materialize_eagerly(delta_count, ctx.materialization_threshold) {
        debug!(
            pgno = page.page_no.get(),
            delta_count,
            threshold = ctx.materialization_threshold,
            "routing_threshold_exceeded"
        );
        return RoutingDecision::page_level(RoutingReason::MaterializationThresholdExceeded);
    }

    // 6. Check per-transaction memory budget
    let txn_bytes = ctx.cell_log.txn_bytes(ctx.current_txn);
    let per_txn_budget = ctx.cell_log.per_txn_budget_bytes();
    if txn_bytes >= per_txn_budget {
        debug!(
            pgno = page.page_no.get(),
            txn_bytes,
            budget = per_txn_budget,
            "routing_memory_budget_exceeded"
        );
        return RoutingDecision::page_level(RoutingReason::MemoryBudgetExceeded);
    }

    // All checks passed — use cell-level
    trace!(
        pgno = page.page_no.get(),
        mode = ctx.mode.as_str(),
        delta_count,
        "routing_cell_level_eligible"
    );
    RoutingDecision::cell_level(RoutingReason::CellLevelEligible)
}

// ---------------------------------------------------------------------------
// Escalation Logic (§11.7)
// ---------------------------------------------------------------------------

/// Escalate a page from cell-level to page-level tracking within a transaction.
///
/// This is called when an operation that started as cell-level discovers it
/// needs to become structural (e.g., INSERT fills page and triggers split).
///
/// # Steps
///
/// 1. Materialize the page from base + committed deltas at snapshot
/// 2. Apply this transaction's uncommitted cell deltas to the materialized page
/// 3. Discard this transaction's cell deltas for this page
/// 4. Mark the page as escalated (stays page-level for rest of transaction)
///
/// # Arguments
///
/// * `base_page` - The base page data (last committed version)
/// * `page_number` - The page number
/// * `committed_deltas` - Deltas visible to the current snapshot
/// * `uncommitted_deltas` - This transaction's uncommitted deltas
/// * `snapshot` - The current transaction's snapshot
/// * `usable_size` - The usable page size
/// * `escalation_tracker` - Tracks which pages have been escalated
///
/// # Returns
///
/// An [`EscalationResult`] containing the materialized page.
///
/// # Errors
///
/// Returns an error if materialization fails.
pub fn escalate_to_page_level(
    base_page: &PageData,
    page_number: PageNumber,
    committed_deltas: &[CellDelta],
    uncommitted_deltas: &[CellDelta],
    snapshot: &Snapshot,
    usable_size: u32,
    escalation_tracker: &mut TxnEscalationTracker,
) -> fsqlite_error::Result<EscalationResult> {
    warn!(
        pgno = page_number.get(),
        committed_delta_count = committed_deltas.len(),
        uncommitted_delta_count = uncommitted_deltas.len(),
        "escalating_to_page_level"
    );

    // First, materialize with committed deltas
    let committed_result = materialize_page(
        base_page,
        page_number,
        committed_deltas,
        snapshot,
        usable_size,
        MaterializationTrigger::Structural,
    )?;

    // Then apply uncommitted deltas on top
    // Create a high snapshot to make all uncommitted deltas "visible"
    let uncommitted_snapshot = Snapshot {
        high: CommitSeq::new(u64::MAX),
        schema_epoch: snapshot.schema_epoch,
    };

    let committed_deltas_applied = committed_result.deltas_applied;
    let final_result = if uncommitted_deltas.is_empty() {
        committed_result
    } else {
        // `materialize_page()` only applies deltas visible to the supplied
        // snapshot, and commit_seq=0 is reserved for uncommitted changes. When
        // a transaction escalates mid-flight, we still need to replay its own
        // local deltas on top of the committed materialization result, so map
        // them onto a synthetic in-transaction order for this one-shot pass.
        let visible_uncommitted = uncommitted_deltas
            .iter()
            .enumerate()
            .map(|(idx, delta)| {
                let mut visible_delta = delta.clone();
                visible_delta.commit_seq =
                    CommitSeq::new(u64::try_from(idx + 1).unwrap_or(u64::MAX));
                visible_delta
            })
            .collect::<Vec<_>>();
        materialize_page(
            &committed_result.page,
            page_number,
            &visible_uncommitted,
            &uncommitted_snapshot,
            usable_size,
            MaterializationTrigger::Structural,
        )?
    };

    // Track discarded cell keys
    let discarded_cells: Vec<CellKey> = uncommitted_deltas.iter().map(|d| d.cell_key).collect();

    // Mark page as escalated
    escalation_tracker.escalate(page_number);

    let total_deltas_applied = committed_deltas_applied + final_result.deltas_applied;
    info!(
        pgno = page_number.get(),
        total_deltas_applied,
        discarded_cells = discarded_cells.len(),
        "escalation_complete"
    );

    Ok(EscalationResult {
        materialized_page: final_result.page,
        deltas_applied: total_deltas_applied,
        discarded_cells,
    })
}

// ---------------------------------------------------------------------------
// Global Mode Control (§11.8)
// ---------------------------------------------------------------------------

/// Global cell MVCC mode.
///
/// This is set by `PRAGMA fsqlite.cell_mvcc = ON|OFF|AUTO`.
///
/// # WARNING: Process-Wide Global
///
/// This is a **process-wide static**, not per-connection state. In a multi-database
/// scenario (multiple connections to different databases in the same process), ALL
/// connections share this mode setting. Changing the mode in one connection affects
/// all other connections in the process.
///
/// # Limitation: Process-Global (Track D — bead bd-bldc5.3.9)
///
/// This is process-global, not per-connection. Changing the mode via PRAGMA
/// on one connection affects all connections in the process. A proper fix
/// requires:
/// 1. Adding `cell_mvcc_mode: CellMvccMode` field to `ConnectionHandle`
/// 2. Threading the connection handle through the routing predicate
///    (`should_use_cell_path()`, `CellRoutingDecision::decide()`)
/// 3. Updating PRAGMA handling to modify connection state, not global state
///
/// The current design was chosen for simplicity during initial Track C
/// implementation. It is functionally correct — just has wrong isolation
/// scope. Most workloads use a single mode process-wide anyway.
static GLOBAL_CELL_MVCC_MODE: AtomicCellMvccMode = AtomicCellMvccMode::new(CellMvccMode::Auto);

/// Atomic wrapper for [`CellMvccMode`].
struct AtomicCellMvccMode(std::sync::atomic::AtomicU8);

impl AtomicCellMvccMode {
    const fn new(mode: CellMvccMode) -> Self {
        Self(std::sync::atomic::AtomicU8::new(mode as u8))
    }

    fn load(&self, ordering: Ordering) -> CellMvccMode {
        match self.0.load(ordering) {
            0 => CellMvccMode::On,
            1 => CellMvccMode::Off,
            _ => CellMvccMode::Auto,
        }
    }

    fn store(&self, mode: CellMvccMode, ordering: Ordering) {
        self.0.store(mode as u8, ordering);
    }
}

/// Get the current global cell MVCC mode.
#[must_use]
pub fn get_cell_mvcc_mode() -> CellMvccMode {
    GLOBAL_CELL_MVCC_MODE.load(Ordering::Acquire)
}

/// Set the global cell MVCC mode.
///
/// This is called by `PRAGMA fsqlite.cell_mvcc = ON|OFF|AUTO`.
pub fn set_cell_mvcc_mode(mode: CellMvccMode) {
    let old_mode = GLOBAL_CELL_MVCC_MODE.load(Ordering::Acquire);
    GLOBAL_CELL_MVCC_MODE.store(mode, Ordering::Release);

    info!(
        old_mode = old_mode.as_str(),
        new_mode = mode.as_str(),
        "cell_mvcc_mode_changed"
    );
}

// ---------------------------------------------------------------------------
// Tests (§11.10)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_types::{BtreeRef, TxnEpoch, TxnId};

    fn test_txn(id: u64) -> TxnToken {
        TxnToken::new(TxnId::new(id).unwrap(), TxnEpoch::new(0))
    }

    fn leaf_table_page(page_no: PageNumber, cell_count: u16, available: u32) -> PageMetadata {
        PageMetadata {
            page_no,
            is_leaf: true,
            is_table: true,
            cell_count,
            content_offset: available + 8 + (u32::from(cell_count) + 1) * 2,
            usable_size: 4096,
            header_offset: 0,
            header_size: 8,
        }
    }

    fn interior_table_page(page_no: PageNumber) -> PageMetadata {
        PageMetadata {
            page_no,
            is_leaf: false,
            is_table: true,
            cell_count: 5,
            content_offset: 4000,
            usable_size: 4096,
            header_offset: 0,
            header_size: 12,
        }
    }

    fn index_leaf_page(page_no: PageNumber) -> PageMetadata {
        PageMetadata {
            page_no,
            is_leaf: true,
            is_table: false, // Index page
            cell_count: 10,
            content_offset: 3000,
            usable_size: 4096,
            header_offset: 0,
            header_size: 8,
        }
    }

    #[test]
    fn test_route_logical_insert_to_cell_path() {
        let cell_log = CellVisibilityLog::new(1024 * 1024);
        let page_tracking = PageTrackingState::new();
        let txn = test_txn(1);
        let page = leaf_table_page(PageNumber::new(2).unwrap(), 10, 1000);

        let ctx = RoutingContext {
            mode: CellMvccMode::On,
            cell_log: &cell_log,
            page_tracking: &page_tracking,
            current_txn: txn,
            materialization_threshold: 32,
        };

        let op = BtreeOp::TableInsert {
            rowid: 100,
            payload_size: 50,
        };

        let decision = should_use_cell_path(&ctx, &page, &op);
        assert!(decision.use_cell_level);
        assert_eq!(decision.reason, RoutingReason::CellLevelEligible);
    }

    #[test]
    fn test_route_structural_split_to_page_path() {
        let cell_log = CellVisibilityLog::new(1024 * 1024);
        let page_tracking = PageTrackingState::new();
        let txn = test_txn(1);
        let page = leaf_table_page(PageNumber::new(2).unwrap(), 100, 30); // Nearly full

        let ctx = RoutingContext {
            mode: CellMvccMode::On,
            cell_log: &cell_log,
            page_tracking: &page_tracking,
            current_txn: txn,
            materialization_threshold: 32,
        };

        // Large insert that won't fit
        let op = BtreeOp::TableInsert {
            rowid: 100,
            payload_size: 100,
        };

        let decision = should_use_cell_path(&ctx, &page, &op);
        assert!(!decision.use_cell_level);
        assert_eq!(decision.reason, RoutingReason::StructuralOperation);
    }

    #[test]
    fn test_route_overflowing_insert_to_page_path_even_when_empty_page_has_space() {
        let cell_log = CellVisibilityLog::new(1024 * 1024);
        let page_tracking = PageTrackingState::new();
        let txn = test_txn(1);
        let page = leaf_table_page(PageNumber::new(2).unwrap(), 0, 4086);

        let ctx = RoutingContext {
            mode: CellMvccMode::On,
            cell_log: &cell_log,
            page_tracking: &page_tracking,
            current_txn: txn,
            materialization_threshold: 32,
        };

        let op = BtreeOp::TableInsert {
            rowid: 100,
            payload_size: 4062,
        };

        let decision = should_use_cell_path(&ctx, &page, &op);
        assert!(!decision.use_cell_level);
        assert_eq!(decision.reason, RoutingReason::StructuralOperation);
    }

    #[test]
    fn test_route_interior_page_always_page_path() {
        let cell_log = CellVisibilityLog::new(1024 * 1024);
        let page_tracking = PageTrackingState::new();
        let txn = test_txn(1);
        let page = interior_table_page(PageNumber::new(2).unwrap());

        let ctx = RoutingContext {
            mode: CellMvccMode::On,
            cell_log: &cell_log,
            page_tracking: &page_tracking,
            current_txn: txn,
            materialization_threshold: 32,
        };

        let op = BtreeOp::TableInsert {
            rowid: 100,
            payload_size: 10,
        };

        let decision = should_use_cell_path(&ctx, &page, &op);
        assert!(!decision.use_cell_level);
        assert_eq!(decision.reason, RoutingReason::InteriorPage);
    }

    #[test]
    fn test_route_pragma_off_forces_page_path() {
        let cell_log = CellVisibilityLog::new(1024 * 1024);
        let page_tracking = PageTrackingState::new();
        let txn = test_txn(1);
        let page = leaf_table_page(PageNumber::new(2).unwrap(), 10, 1000);

        let ctx = RoutingContext {
            mode: CellMvccMode::Off, // Disabled
            cell_log: &cell_log,
            page_tracking: &page_tracking,
            current_txn: txn,
            materialization_threshold: 32,
        };

        let op = BtreeOp::TableInsert {
            rowid: 100,
            payload_size: 50,
        };

        let decision = should_use_cell_path(&ctx, &page, &op);
        assert!(!decision.use_cell_level);
        assert_eq!(decision.reason, RoutingReason::CellMvccDisabled);
    }

    #[test]
    fn test_route_pragma_auto_tables_cell_indexes_page() {
        let cell_log = CellVisibilityLog::new(1024 * 1024);
        let page_tracking = PageTrackingState::new();
        let txn = test_txn(1);

        let ctx = RoutingContext {
            mode: CellMvccMode::Auto,
            cell_log: &cell_log,
            page_tracking: &page_tracking,
            current_txn: txn,
            materialization_threshold: 32,
        };

        // Table leaf page -> cell level
        let table_page = leaf_table_page(PageNumber::new(2).unwrap(), 10, 1000);
        let op = BtreeOp::TableInsert {
            rowid: 100,
            payload_size: 50,
        };
        let table_decision = should_use_cell_path(&ctx, &table_page, &op);
        assert!(table_decision.use_cell_level);

        // Index leaf page -> page level in AUTO mode
        let index_page = index_leaf_page(PageNumber::new(3).unwrap());
        let index_op = BtreeOp::IndexInsert { key_size: 50 };
        let index_decision = should_use_cell_path(&ctx, &index_page, &index_op);
        assert!(!index_decision.use_cell_level);
        assert_eq!(index_decision.reason, RoutingReason::IndexPageAutoMode);
    }

    #[test]
    fn test_route_page_tracked_by_other_txn_forces_page() {
        let cell_log = CellVisibilityLog::new(1024 * 1024);
        let page_tracking = PageTrackingState::new();
        let txn1 = test_txn(1);
        let txn2 = test_txn(2);
        let page = leaf_table_page(PageNumber::new(2).unwrap(), 10, 1000);

        // Register txn1 as having page-level tracking
        page_tracking.register_page_level(page.page_no, txn1);

        // Now txn2 tries to route
        let ctx = RoutingContext {
            mode: CellMvccMode::On,
            cell_log: &cell_log,
            page_tracking: &page_tracking,
            current_txn: txn2,
            materialization_threshold: 32,
        };

        let op = BtreeOp::TableInsert {
            rowid: 100,
            payload_size: 50,
        };

        let decision = should_use_cell_path(&ctx, &page, &op);
        assert!(!decision.use_cell_level);
        assert_eq!(decision.reason, RoutingReason::PageTrackedByOtherTxn);
    }

    #[test]
    fn test_route_same_txn_page_level_allows_cell() {
        let cell_log = CellVisibilityLog::new(1024 * 1024);
        let page_tracking = PageTrackingState::new();
        let txn = test_txn(1);
        let page = leaf_table_page(PageNumber::new(2).unwrap(), 10, 1000);

        // Same txn has page-level tracking - this doesn't block cell-level
        page_tracking.register_page_level(page.page_no, txn);

        let ctx = RoutingContext {
            mode: CellMvccMode::On,
            cell_log: &cell_log,
            page_tracking: &page_tracking,
            current_txn: txn, // Same txn
            materialization_threshold: 32,
        };

        let op = BtreeOp::TableInsert {
            rowid: 100,
            payload_size: 50,
        };

        let decision = should_use_cell_path(&ctx, &page, &op);
        // Same txn's page-level tracking doesn't block itself
        assert!(decision.use_cell_level);
    }

    #[test]
    fn test_route_threshold_exceeded_forces_page() {
        use fsqlite_types::TableId;

        let cell_log = CellVisibilityLog::new(1024 * 1024);
        let page_tracking = PageTrackingState::new();
        let txn = test_txn(1);
        let page = leaf_table_page(PageNumber::new(2).unwrap(), 10, 1000);

        // Set a low threshold
        let ctx = RoutingContext {
            mode: CellMvccMode::On,
            cell_log: &cell_log,
            page_tracking: &page_tracking,
            current_txn: txn,
            materialization_threshold: 32,
        };

        // Simulate many deltas on this page by adding them to the cell log
        let btree = BtreeRef::Table(TableId::new(1));
        for i in 0i64..35 {
            let cell_key = CellKey::table_row(btree, i);
            let delta_txn = test_txn(100 + i as u64);
            let idx = cell_log
                .record_insert(cell_key, page.page_no, vec![i as u8; 20], delta_txn)
                .expect("insert should succeed");
            cell_log.commit_delta(idx, CommitSeq::new((i + 1) as u64));
        }

        let op = BtreeOp::TableInsert {
            rowid: 100,
            payload_size: 50,
        };

        let decision = should_use_cell_path(&ctx, &page, &op);
        assert!(!decision.use_cell_level);
        assert_eq!(
            decision.reason,
            RoutingReason::MaterializationThresholdExceeded
        );
    }

    #[test]
    fn test_escalation_is_one_way() {
        let mut tracker = TxnEscalationTracker::new();
        let page = PageNumber::new(2).unwrap();

        // Start with cell-level
        tracker.add_cell_level(page);
        assert!(tracker.is_cell_level(page));
        assert!(!tracker.is_escalated(page));

        // Escalate
        tracker.escalate(page);
        assert!(!tracker.is_cell_level(page));
        assert!(tracker.is_escalated(page));

        // Try to add cell-level again - should stay escalated
        tracker.add_cell_level(page);
        assert!(!tracker.is_cell_level(page));
        assert!(tracker.is_escalated(page));
    }

    #[test]
    fn test_escalation_only_affects_target_page() {
        let mut tracker = TxnEscalationTracker::new();
        let page47 = PageNumber::new(47).unwrap();
        let page48 = PageNumber::new(48).unwrap();

        tracker.add_cell_level(page47);
        tracker.add_cell_level(page48);

        // Escalate only page 47
        tracker.escalate(page47);

        assert!(!tracker.is_cell_level(page47));
        assert!(tracker.is_escalated(page47));
        assert!(tracker.is_cell_level(page48));
        assert!(!tracker.is_escalated(page48));
    }

    #[test]
    fn test_cell_mvcc_mode_parsing() {
        assert_eq!("on".parse::<CellMvccMode>(), Ok(CellMvccMode::On));
        assert_eq!("ON".parse::<CellMvccMode>(), Ok(CellMvccMode::On));
        assert_eq!("true".parse::<CellMvccMode>(), Ok(CellMvccMode::On));
        assert_eq!("1".parse::<CellMvccMode>(), Ok(CellMvccMode::On));

        assert_eq!("off".parse::<CellMvccMode>(), Ok(CellMvccMode::Off));
        assert_eq!("OFF".parse::<CellMvccMode>(), Ok(CellMvccMode::Off));
        assert_eq!("false".parse::<CellMvccMode>(), Ok(CellMvccMode::Off));
        assert_eq!("0".parse::<CellMvccMode>(), Ok(CellMvccMode::Off));

        assert_eq!("auto".parse::<CellMvccMode>(), Ok(CellMvccMode::Auto));
        assert_eq!("AUTO".parse::<CellMvccMode>(), Ok(CellMvccMode::Auto));

        assert!("invalid".parse::<CellMvccMode>().is_err());
    }

    #[test]
    fn test_page_tracking_state() {
        let state = PageTrackingState::new();
        let page = PageNumber::new(2).unwrap();
        let txn1 = test_txn(1);
        let txn2 = test_txn(2);

        assert_eq!(state.page_level_txn_count(page), 0);
        assert!(!state.has_other_page_level_txn(page, txn1));

        state.register_page_level(page, txn1);
        assert_eq!(state.page_level_txn_count(page), 1);
        assert!(!state.has_other_page_level_txn(page, txn1)); // Self doesn't count
        assert!(state.has_other_page_level_txn(page, txn2)); // txn2 sees txn1

        state.register_page_level(page, txn2);
        assert_eq!(state.page_level_txn_count(page), 2);
        assert!(state.has_other_page_level_txn(page, txn1)); // Now txn1 sees txn2

        state.unregister_page_level(page, txn1);
        assert_eq!(state.page_level_txn_count(page), 1);
        assert!(!state.has_other_page_level_txn(page, txn2)); // Only txn2 left

        state.unregister_all(txn2);
        assert_eq!(state.page_level_txn_count(page), 0);
    }

    #[test]
    fn test_global_mode_control() {
        // Save current mode
        let saved = get_cell_mvcc_mode();

        set_cell_mvcc_mode(CellMvccMode::Off);
        assert_eq!(get_cell_mvcc_mode(), CellMvccMode::Off);

        set_cell_mvcc_mode(CellMvccMode::On);
        assert_eq!(get_cell_mvcc_mode(), CellMvccMode::On);

        set_cell_mvcc_mode(CellMvccMode::Auto);
        assert_eq!(get_cell_mvcc_mode(), CellMvccMode::Auto);

        // Restore
        set_cell_mvcc_mode(saved);
    }

    // =========================================================================
    // Additional tests required by C-TRANSITION (bd-l9k8e.11)
    // =========================================================================

    #[test]
    fn test_route_memory_budget_exceeded_forces_page() {
        use fsqlite_types::TableId;

        // CellDelta has ~100 byte fixed overhead + cell_data.
        // We use a budget of 120 bytes so exactly one delta fits (~114 bytes).
        // After one insert, txn_bytes will be ~114, and we set budget to 114
        // so the routing predicate sees txn_bytes >= budget.
        //
        // Actually, we need to first measure the exact delta size, then set
        // budget = delta_size so routing sees txn_bytes >= budget.
        let btree = BtreeRef::Table(TableId::new(1));
        let txn = test_txn(1);

        // First, measure delta memory size with a separate log
        let measure_log = CellVisibilityLog::with_per_txn_budget(1024 * 1024, 10_000);
        let cell_key = CellKey::table_row(btree, 0);
        let _ =
            measure_log.record_insert(cell_key, PageNumber::new(99).unwrap(), vec![0u8; 10], txn);
        let delta_size = measure_log.txn_bytes(txn);

        // Now create the real log with budget = delta_size so after one insert,
        // txn_bytes == budget, triggering the routing check.
        let cell_log = CellVisibilityLog::with_per_txn_budget(1024 * 1024, delta_size);
        let page_tracking = PageTrackingState::new();
        let page = leaf_table_page(PageNumber::new(2).unwrap(), 10, 1000);

        // Insert one delta - should succeed and bring us exactly to budget
        let cell_key1 = CellKey::table_row(btree, 1);
        let result =
            cell_log.record_insert(cell_key1, PageNumber::new(99).unwrap(), vec![0u8; 10], txn);
        assert!(result.is_some(), "First insert should succeed");

        // Verify we're at the budget limit
        let tracked_bytes = cell_log.txn_bytes(txn);
        assert_eq!(
            tracked_bytes, delta_size,
            "txn_bytes should equal budget after one insert"
        );

        let ctx = RoutingContext {
            mode: CellMvccMode::On,
            cell_log: &cell_log,
            page_tracking: &page_tracking,
            current_txn: txn,
            materialization_threshold: 32,
        };

        let op = BtreeOp::TableInsert {
            rowid: 100,
            payload_size: 50,
        };

        let decision = should_use_cell_path(&ctx, &page, &op);
        assert!(!decision.use_cell_level);
        assert_eq!(decision.reason, RoutingReason::MemoryBudgetExceeded);
    }

    #[test]
    fn test_escalation_preserves_uncommitted_deltas() {
        use crate::cell_visibility::CellDeltaKind;
        use fsqlite_types::{PageSize, SchemaEpoch, TableId};

        // Create a minimal page (leaf page header + some space)
        let mut base_page = PageData::zeroed(PageSize::new(4096).unwrap());
        // Set leaf table page header: flag byte 0x0D (leaf|table), freeblock=0, cell_count=0
        let page_bytes = base_page.as_bytes_mut();
        page_bytes[0] = 0x0D; // Leaf table page flag
        page_bytes[3] = 0; // Cell count high byte
        page_bytes[4] = 0; // Cell count low byte
        page_bytes[5] = 0x10; // Content offset high (4096 - 4080 = 16)
        page_bytes[6] = 0x00; // Content offset low

        let page_number = PageNumber::new(5).unwrap();
        let btree = BtreeRef::Table(TableId::new(1));
        let txn = test_txn(42);
        let snapshot = Snapshot {
            high: CommitSeq::new(0),
            schema_epoch: SchemaEpoch::new(0),
        };

        // Create uncommitted deltas (not yet committed)
        let uncommitted_deltas = vec![
            CellDelta {
                cell_key: CellKey::table_row(btree, 1),
                commit_seq: CommitSeq::new(0), // Not committed
                created_by: txn,
                kind: CellDeltaKind::Insert,
                page_number,
                cell_data: vec![1, 2, 3, 4, 5],
                prev_idx: None,
            },
            CellDelta {
                cell_key: CellKey::table_row(btree, 2),
                commit_seq: CommitSeq::new(0), // Not committed
                created_by: txn,
                kind: CellDeltaKind::Insert,
                page_number,
                cell_data: vec![6, 7, 8, 9, 10],
                prev_idx: None,
            },
        ];

        let mut tracker = TxnEscalationTracker::new();
        tracker.add_cell_level(page_number);

        // Escalate with no committed deltas but 2 uncommitted
        let result = escalate_to_page_level(
            &base_page,
            page_number,
            &[],                 // No committed deltas
            &uncommitted_deltas, // 2 uncommitted deltas
            &snapshot,
            4096,
            &mut tracker,
        )
        .expect("escalation should succeed");

        // Verify all uncommitted deltas were applied
        assert_eq!(result.deltas_applied, 2);
        assert_eq!(result.discarded_cells.len(), 2);
        assert!(tracker.is_escalated(page_number));
    }

    #[test]
    #[ignore = "requires B-tree split integration (C-TRANSITION integration)"]
    fn test_escalation_insert_triggers_split() {
        // This test would verify:
        // 1. Start cell-level tracking on a nearly-full page
        // 2. Insert that fills the page
        // 3. Next insert triggers split detection
        // 4. System escalates to page-level before split
        // 5. Split proceeds correctly with materialized page
        //
        // Requires full B-tree integration to detect split conditions.
    }

    #[test]
    #[ignore = "requires full SQL execution stack (C-TRANSITION integration)"]
    fn test_cell_path_result_identical_to_page_path() {
        // This test would verify:
        // 1. Execute same sequence of operations with cell_mvcc = ON
        // 2. Execute same sequence of operations with cell_mvcc = OFF
        // 3. Compare final database bytes - must be identical
        //
        // Requires full fsqlite-core Connection to execute SQL.
    }

    #[test]
    #[ignore = "requires concurrent transaction infrastructure (C-TRANSITION integration)"]
    fn test_mixed_concurrent_txns() {
        // This test would verify:
        // 1. txn A uses cell-level on pages 1, 2
        // 2. txn B uses page-level on pages 3, 4
        // 3. Both commit successfully
        // 4. No interference between paths
        //
        // Requires transaction manager integration.
    }

    #[test]
    #[ignore = "requires threading and contention testing (C-TRANSITION integration)"]
    fn test_escalation_under_contention() {
        // This test would verify:
        // 1. 4 threads operating concurrently
        // 2. One thread triggers escalation on its page
        // 3. Other threads continue cell-level on other pages
        // 4. No deadlock, no corruption
        //
        // Requires multi-threaded test harness.
    }

    #[test]
    fn test_pragma_toggle_mid_session() {
        // Save current mode
        let saved = get_cell_mvcc_mode();

        let cell_log = CellVisibilityLog::new(1024 * 1024);
        let page_tracking = PageTrackingState::new();
        let txn = test_txn(1);
        let page = leaf_table_page(PageNumber::new(2).unwrap(), 10, 1000);
        let op = BtreeOp::TableInsert {
            rowid: 100,
            payload_size: 50,
        };

        // Start with ON
        set_cell_mvcc_mode(CellMvccMode::On);
        let ctx_on = RoutingContext {
            mode: get_cell_mvcc_mode(),
            cell_log: &cell_log,
            page_tracking: &page_tracking,
            current_txn: txn,
            materialization_threshold: 32,
        };
        let decision_on = should_use_cell_path(&ctx_on, &page, &op);
        assert!(decision_on.use_cell_level);

        // Toggle to OFF
        set_cell_mvcc_mode(CellMvccMode::Off);
        let ctx_off = RoutingContext {
            mode: get_cell_mvcc_mode(),
            cell_log: &cell_log,
            page_tracking: &page_tracking,
            current_txn: txn,
            materialization_threshold: 32,
        };
        let decision_off = should_use_cell_path(&ctx_off, &page, &op);
        assert!(!decision_off.use_cell_level);
        assert_eq!(decision_off.reason, RoutingReason::CellMvccDisabled);

        // Toggle to AUTO
        set_cell_mvcc_mode(CellMvccMode::Auto);
        let ctx_auto = RoutingContext {
            mode: get_cell_mvcc_mode(),
            cell_log: &cell_log,
            page_tracking: &page_tracking,
            current_txn: txn,
            materialization_threshold: 32,
        };
        let decision_auto = should_use_cell_path(&ctx_auto, &page, &op);
        // Table leaf page in AUTO mode should use cell-level
        assert!(decision_auto.use_cell_level);

        // Restore
        set_cell_mvcc_mode(saved);
    }

    #[test]
    #[ignore = "requires proptest infrastructure (C-TRANSITION stress)"]
    fn test_random_routing_1000_ops() {
        // Property-based test:
        // Given: 1000 random operations with random page sizes/fill levels
        // Property: All routing decisions are correct per the decision tree
        //
        // Requires proptest configuration.
    }

    #[test]
    #[ignore = "requires repeated fill/split cycle (C-TRANSITION stress)"]
    fn test_rapid_escalation_cycle() {
        // Stress test:
        // 1. Repeatedly fill page with cell deltas to trigger split
        // 2. Each split triggers escalation
        // 3. Verify escalation/materialization cycle is stable
        // 4. No memory leaks, no corruption
        //
        // Requires B-tree split/merge integration.
    }
}
