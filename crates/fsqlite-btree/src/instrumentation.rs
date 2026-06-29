//! B-tree operation observability counters.
//!
//! This module exposes lightweight process-local counters used by the
//! `btree_op` tracing lane and bead-level telemetry verification.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};

use fsqlite_types::PageNumber;
use serde::{Deserialize, Serialize};

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

/// Snapshot of copy-heavy B-tree payload and cell-assembly kernels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BtreeCopyProfileSnapshot {
    /// Local payload copied into a caller-owned scratch buffer.
    pub local_payload_copy_calls: u64,
    pub local_payload_copy_bytes: u64,
    /// Fresh owned payload materializations (for example `payload()` or helper APIs).
    pub owned_payload_materialization_calls: u64,
    pub owned_payload_materialization_bytes: u64,
    /// Overflow payload reassembly activity.
    pub overflow_chain_reassembly_calls: u64,
    pub overflow_chain_local_bytes: u64,
    pub overflow_chain_overflow_bytes: u64,
    pub overflow_page_reads: u64,
    /// On-page cell assembly helpers.
    pub table_leaf_cell_assembly_calls: u64,
    pub table_leaf_cell_assembly_bytes: u64,
    pub index_leaf_cell_assembly_calls: u64,
    pub index_leaf_cell_assembly_bytes: u64,
    pub interior_cell_rebuild_calls: u64,
    pub interior_cell_rebuild_bytes: u64,
}

/// Snapshot of residual leaf-state reuse counters for W3-style insert paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BtreeLeafReuseSnapshot {
    /// Successful no-split leaf inserts that reused the in-memory stack entry.
    pub no_split_reuse_hits: u64,
    /// Cases that fell back to the conservative balance/reload path.
    pub conservative_reload_fallbacks: u64,
    /// Full header + cell-pointer rebuilds performed via `reload_page_fresh`.
    pub page_header_rebuild_count: u64,
    /// No-overflow table-leaf append calls that wrote payload bytes directly.
    pub fast_table_leaf_payload_appends: u64,
    /// Time spent mutating the in-memory page image for the no-overflow append path.
    pub fast_table_leaf_payload_mutate_time_ns: u64,
    /// Time spent handing the no-overflow append image to the pager write-set.
    pub fast_table_leaf_payload_stage_time_ns: u64,
    /// Full-cell append calls used when the payload fast path was unavailable.
    pub fast_table_leaf_full_cell_appends: u64,
    /// Time spent mutating the in-memory page image for the full-cell append path.
    pub fast_table_leaf_full_cell_mutate_time_ns: u64,
    /// Time spent handing the full-cell append image to the pager write-set.
    pub fast_table_leaf_full_cell_stage_time_ns: u64,
    /// Attempts to use the right-edge quick-balance split path.
    pub quick_balance_attempts: u64,
    /// Successful right-edge quick-balance split path hits.
    pub quick_balance_hits: u64,
    /// Time spent in the right-edge quick-balance attempt path.
    pub quick_balance_time_ns: u64,
    /// Attempts to use the table-leaf local split path.
    pub local_split_attempts: u64,
    /// Successful table-leaf local split path hits.
    pub local_split_hits: u64,
    /// Time spent in the table-leaf local split attempt path.
    pub local_split_time_ns: u64,
    /// Calls into the generic nonroot rebalance path.
    pub nonroot_balance_calls: u64,
    /// Time spent in the generic nonroot rebalance path.
    pub nonroot_balance_time_ns: u64,
    /// Invocations of the deferred-delete rebalance fixup (`balance_for_delete`).
    ///
    /// Per the K2 deferred-delete strategy (bd-yywuv), DELETE removes a leaf
    /// cell in place and only triggers `balance_for_delete` when the leaf
    /// becomes empty — never per DELETE. A run of DELETEs that does not empty a
    /// leaf therefore leaves this counter unchanged, so it stays far below the
    /// `fsqlite_btree_operations_total.delete` count.
    pub balance_for_delete_calls: u64,
    /// Retained same-leaf DELETE run materializations.
    pub delete_leaf_run_materialize_calls: u64,
    /// Time spent materializing retained same-leaf DELETE runs into page images.
    pub delete_leaf_run_materialize_time_ns: u64,
    /// Retained same-leaf DELETE run page writes.
    pub delete_leaf_run_write_calls: u64,
    /// Time spent handing retained same-leaf DELETE page images to the pager.
    pub delete_leaf_run_write_time_ns: u64,
    /// Rowid searches inside retained same-leaf DELETE runs.
    pub delete_leaf_run_search_calls: u64,
    /// Time spent searching retained same-leaf DELETE run rowids.
    pub delete_leaf_run_search_time_ns: u64,
    /// Duplicate-index checks inside retained same-leaf DELETE runs.
    pub delete_leaf_run_duplicate_check_calls: u64,
    /// Time spent checking duplicate indexes in retained same-leaf DELETE runs.
    pub delete_leaf_run_duplicate_check_time_ns: u64,
    /// Compact-page-shape checks inside retained same-leaf DELETE runs.
    pub delete_leaf_run_compact_check_calls: u64,
    /// Time spent checking compact page shape in retained same-leaf DELETE runs.
    pub delete_leaf_run_compact_check_time_ns: u64,
    /// Cell parse/shape checks inside retained same-leaf DELETE runs.
    pub delete_leaf_run_cell_parse_calls: u64,
    /// Time spent parsing cells in retained same-leaf DELETE runs.
    pub delete_leaf_run_cell_parse_time_ns: u64,
    /// Bulk table INSERT page-run grouping calls.
    pub bulk_table_grouping_calls: u64,
    /// Time spent grouping bulk table INSERT records into page ranges.
    pub bulk_table_grouping_time_ns: u64,
    /// Bulk table INSERT leaf page builds.
    pub bulk_table_leaf_page_build_calls: u64,
    /// Time spent building bulk table INSERT leaf page images.
    pub bulk_table_leaf_page_build_time_ns: u64,
    /// Bulk table INSERT leaf page writes.
    pub bulk_table_leaf_page_write_calls: u64,
    /// Time spent handing bulk table INSERT leaf pages to the pager.
    pub bulk_table_leaf_page_write_time_ns: u64,
    /// Bulk table INSERT interior/root page builds.
    pub bulk_table_interior_page_build_calls: u64,
    /// Time spent building bulk table INSERT interior/root page images.
    pub bulk_table_interior_page_build_time_ns: u64,
    /// Bulk table INSERT interior/root page writes.
    pub bulk_table_interior_page_write_calls: u64,
    /// Time spent handing bulk table INSERT interior/root pages to the pager.
    pub bulk_table_interior_page_write_time_ns: u64,
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

// ── B-tree hot-path metrics gate (bd-perf cc_3 2026-04-25) ────────────────
//
// The per-cursor-op counters below (`record_operation`, `set_depth_gauge`,
// `record_split_event`, `record_no_split_reuse_hit`,
// `record_conservative_reload_fallback`, `record_page_header_rebuild`) are
// consumed only by `fsqlite-e2e` profiling captures and a handful of
// observability tests — production reads nothing here. Each unconditional
// `fetch_add` / `store` on a shared static atomic is a cross-core cache-line
// invalidation under MT load. Mirror the MVCC pattern (bc4fa6b5 / d2156302 /
// 03c49886): default the gate off so the hot path pays one shared-cache
// `AtomicBool::load(Relaxed)` instead of a `lock xadd` to a contended line.
//
// Callers flip the gate explicitly: `fsqlite-e2e/src/fsqlite_executor.rs`
// when `HotPathMetricsCapture` is enabled, and tests that assert on these
// counters before reading the snapshot.
static FSQLITE_BTREE_METRICS_ENABLED: AtomicBool = AtomicBool::new(false);

static BTREE_OP_SEEK_TOTAL: AtomicU64 = AtomicU64::new(0);
static BTREE_OP_INSERT_TOTAL: AtomicU64 = AtomicU64::new(0);
static BTREE_OP_DELETE_TOTAL: AtomicU64 = AtomicU64::new(0);
static BTREE_PAGE_SPLITS_TOTAL: AtomicU64 = AtomicU64::new(0);
static BTREE_DEPTH_GAUGE: AtomicU64 = AtomicU64::new(0);
static SWISS_TABLE_PROBES_TOTAL: AtomicU64 = AtomicU64::new(0);
static SWISS_TABLE_LOAD_FACTOR: AtomicU64 = AtomicU64::new(0);

// ── Conflict-topology allocator/split policy (bd-1dp9.6.7.13.2) ──────────

const CONFLICT_TOPOLOGY_POLICY_ID: &str = "btree.conflict_topology_split.v1";
const CONFLICT_TOPOLOGY_POLICY_ENV: &str = "FSQLITE_CONFLICT_TOPOLOGY_POLICY";
const CONFLICT_TOPOLOGY_HOT_HEAT_THRESHOLD: u64 = 2;
const CONFLICT_TOPOLOGY_HOT_OVERLAP_THRESHOLD: u32 = 2;
const CONFLICT_TOPOLOGY_TARGET_SHIFT_BPS: usize = 1_500;
const HOT_PAGE_DEFLECTION_POLICY_ID: &str = "btree.hot_page_deflection.v1";
const HOT_PAGE_DEFLECTION_HEAT_THRESHOLD: u64 = 64;
const HOT_PAGE_DEFLECTION_OVERLAP_THRESHOLD: u32 = 4;
const HOT_PAGE_DEFLECTION_TARGET_SHIFT_BPS: usize = 1_000;
const HOT_PAGE_DEFLECTION_BUDGET_PAGES: u8 = 2;
const HOT_PAGE_DEFLECTION_BUDGET_NS: u64 = 0;

// ── Adaptive fill-factor control (bd-1dp9.6.7.13.2) ──────────────────────
//
// The topology policy above applies a *flat* fill-factor shift the moment a
// page is judged "topology hot". Adaptive fill-factor control instead scales an
// additional, bounded shift in proportion to the accumulated conflict heat, so
// a page that keeps getting hotter cedes progressively more slack to the
// contended side — never exceeding explicit clamps. It is opt-in and
// default-disabled, so the baseline/topology split policy is byte-identical
// until an operator enables it (rollout discipline: observe -> advise ->
// enforce, with a reversible kill switch).
const ADAPTIVE_FILL_FACTOR_POLICY_ID: &str = "btree.adaptive_fill_factor.v1";
const ADAPTIVE_FILL_FACTOR_ENV: &str = "FSQLITE_ADAPTIVE_FILL_FACTOR";
// Heat at/below which the adaptive ramp adds zero extra shift, preserving the
// flat topology behavior at first contact. Anchored to the hot threshold.
const ADAPTIVE_FILL_FACTOR_KNEE_HEAT: u64 = CONFLICT_TOPOLOGY_HOT_HEAT_THRESHOLD;
// Heat at which the adaptive ramp saturates at its maximum extra shift. Anchored
// to the pathological-hotspot threshold so the ramp spans the topology-hot band.
const ADAPTIVE_FILL_FACTOR_SATURATION_HEAT: u64 = HOT_PAGE_DEFLECTION_HEAT_THRESHOLD;
// Maximum *additional* fill-factor shift the ramp may add on top of the flat
// topology shift, in basis points.
const ADAPTIVE_FILL_FACTOR_MAX_EXTRA_SHIFT_BPS: usize = 1_500;
// Explicit clamps for the adaptive path: wider than the flat topology clamps,
// but still bounded and operator-visible.
const ADAPTIVE_FILL_FACTOR_LEFT_FLOOR_BPS: usize = 1_500;
const ADAPTIVE_FILL_FACTOR_RIGHT_CEIL_BPS: usize = 9_000;

static CONFLICT_TOPOLOGY_POLICY_MODE: AtomicU64 = AtomicU64::new(2);
static CONFLICT_TOPOLOGY_POLICY_ENV_APPLIED: AtomicBool = AtomicBool::new(false);
static ADAPTIVE_FILL_FACTOR_ENABLED: AtomicBool = AtomicBool::new(false);
static ADAPTIVE_FILL_FACTOR_ENV_APPLIED: AtomicBool = AtomicBool::new(false);
static CONFLICT_TOPOLOGY_STATE: LazyLock<Mutex<ConflictTopologyState>> =
    LazyLock::new(|| Mutex::new(ConflictTopologyState::default()));

/// Rollout mode for conflict-topology placement/split policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictTopologyPolicyMode {
    /// Ignore conflict heat and keep the baseline B-tree split policy.
    Baseline,
    /// Compute and log what the topology-aware policy would do, but do not apply it.
    Advisory,
    /// Apply topology-aware target fill adjustments for hot pages.
    Enforced,
}

impl ConflictTopologyPolicyMode {
    /// Stable log label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::Advisory => "advisory",
            Self::Enforced => "enforced",
        }
    }

    const fn to_raw(self) -> u64 {
        match self {
            Self::Baseline => 0,
            Self::Advisory => 1,
            Self::Enforced => 2,
        }
    }

    const fn from_raw(raw: u64) -> Self {
        match raw {
            0 => Self::Baseline,
            1 => Self::Advisory,
            _ => Self::Enforced,
        }
    }
}

/// Bounded hotspot-deflection state for a split decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum HotPageDeflectionStatus {
    /// No pathological hotspot signal selected the deflection path.
    Inactive,
    /// Advisory mode observed a hotspot but did not mutate split placement.
    AdvisoryOnly,
    /// Enforced mode consumed one bounded split-deflection credit.
    Applied,
    /// The page remains hot but its bounded deflection budget is exhausted.
    BudgetExhausted,
    /// Operator policy forced baseline placement.
    OperatorOverride,
}

impl HotPageDeflectionStatus {
    /// Stable log label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inactive => "inactive",
            Self::AdvisoryOnly => "advisory_only",
            Self::Applied => "applied",
            Self::BudgetExhausted => "budget_exhausted",
            Self::OperatorOverride => "operator_override",
        }
    }

    /// Whether this status represents an active pathological-hotspot signal.
    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(
            self,
            Self::AdvisoryOnly | Self::Applied | Self::BudgetExhausted
        )
    }

    /// Whether this status consumed a split-deflection credit.
    #[must_use]
    pub const fn is_applied(self) -> bool {
        matches!(self, Self::Applied)
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct ConflictTopologyPageState {
    heat: u64,
    max_writer_overlap_estimate: u32,
    deflection_armed: bool,
    deflection_credits_remaining: u8,
}

#[derive(Debug, Default)]
struct ConflictTopologyState {
    pages: BTreeMap<PageNumber, ConflictTopologyPageState>,
}

/// Split-policy advice derived from MVCC conflict heat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ConflictTopologySplitAdvice {
    /// Stable policy identifier for logs and artifacts.
    pub policy_id: &'static str,
    /// Stable mitigation policy identifier for hotspot escape hatches.
    pub mitigation_policy_id: &'static str,
    /// Current rollout mode.
    pub policy_mode: ConflictTopologyPolicyMode,
    /// Page selected by the heat-map signal, or zero when no page is known.
    pub hot_page_id: u32,
    /// Baseline split target before topology adjustment.
    pub baseline_target_left_basis_points: usize,
    /// Advisory topology-aware target, even when mode is advisory.
    pub advised_target_left_basis_points: usize,
    /// Target actually applied to the split.
    pub effective_target_left_basis_points: usize,
    /// Conflict heat observed for the page.
    pub conflict_heat: u64,
    /// Maximum peer-writer overlap observed for the page.
    pub writer_overlap_estimate: u32,
    /// Whether the heat thresholds selected the topology-aware policy.
    pub topology_hot: bool,
    /// Whether the effective target differs from baseline.
    pub applied: bool,
    /// Bounded deflection outcome for this split decision.
    pub deflection_status: HotPageDeflectionStatus,
    /// Deflection credits available before this advice was produced.
    pub deflection_credits_before: u8,
    /// Deflection credits remaining after this advice was produced.
    pub deflection_credits_after: u8,
    /// Maximum synchronous budget consumed by this split-time mitigation.
    pub budget_ns: u64,
    /// Maximum page-level deflections allowed for this armed hotspot.
    pub budget_pages: u8,
    /// Snapshot generation published by a physical recluster path, if any.
    pub publication_generation: u64,
    /// Conflict heat before the mitigation estimate.
    pub heat_before: u64,
    /// Conflict heat after the mitigation estimate.
    pub heat_after: u64,
    /// Why the pathological-hotspot path did or did not trigger.
    pub trigger_reason: &'static str,
    /// Operator-facing outcome for the bounded mitigation path.
    pub migration_outcome: &'static str,
    /// Reversal or rollback reason, when the mitigation is not applied.
    pub rollback_reason: &'static str,
    /// Positive means the policy predicts fewer future hot-page overlaps.
    pub predicted_overlap_delta: i64,
    /// Whether an operator override forced baseline behavior.
    pub operator_override_active: bool,
}

impl ConflictTopologySplitAdvice {
    /// Log label for the chosen placement policy.
    #[must_use]
    pub const fn placement_policy(self) -> &'static str {
        if self.applied {
            if self.deflection_applied() {
                "hot_page_deflection_fill_factor"
            } else {
                "topology_aware_fill_factor"
            }
        } else {
            "baseline"
        }
    }

    /// Log label for why the policy did or did not adjust the split.
    #[must_use]
    pub const fn split_reason(self) -> &'static str {
        if self.operator_override_active {
            "operator_override_baseline"
        } else if self.deflection_applied() {
            "bounded_hot_page_deflection"
        } else if self.topology_hot {
            "mvcc_conflict_heat_hot_page"
        } else {
            "no_hot_page_signal"
        }
    }

    /// Whether this page crossed the stronger pathological-hotspot threshold.
    #[must_use]
    pub const fn deflection_active(self) -> bool {
        self.deflection_status.is_active()
    }

    /// Whether a bounded split-time deflection credit was consumed.
    #[must_use]
    pub const fn deflection_applied(self) -> bool {
        self.deflection_status.is_applied()
    }
}

/// Set the process-local rollout mode for conflict-topology split policy.
pub fn set_conflict_topology_policy_mode(mode: ConflictTopologyPolicyMode) {
    CONFLICT_TOPOLOGY_POLICY_ENV_APPLIED.store(true, Ordering::Release);
    CONFLICT_TOPOLOGY_POLICY_MODE.store(mode.to_raw(), Ordering::Relaxed);
}

/// Current rollout mode for conflict-topology split policy.
#[must_use]
pub fn conflict_topology_policy_mode() -> ConflictTopologyPolicyMode {
    apply_conflict_topology_policy_env_once();
    ConflictTopologyPolicyMode::from_raw(CONFLICT_TOPOLOGY_POLICY_MODE.load(Ordering::Relaxed))
}

/// Whether MVCC should feed conflict heat into the B-tree policy cache.
#[must_use]
pub fn conflict_topology_policy_enabled() -> bool {
    conflict_topology_policy_mode() != ConflictTopologyPolicyMode::Baseline
}

/// Stable policy identifier for adaptive fill-factor control.
#[must_use]
pub const fn adaptive_fill_factor_policy_id() -> &'static str {
    ADAPTIVE_FILL_FACTOR_POLICY_ID
}

fn apply_adaptive_fill_factor_env_once() {
    if ADAPTIVE_FILL_FACTOR_ENV_APPLIED
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }
    let Ok(raw) = std::env::var(ADAPTIVE_FILL_FACTOR_ENV) else {
        return;
    };
    if let Some(enabled) = parse_adaptive_fill_factor_flag(raw.as_str()) {
        ADAPTIVE_FILL_FACTOR_ENABLED.store(enabled, Ordering::Relaxed);
    }
}

fn parse_adaptive_fill_factor_flag(raw: &str) -> Option<bool> {
    let raw = raw.trim();
    if raw.eq_ignore_ascii_case("on")
        || raw.eq_ignore_ascii_case("true")
        || raw.eq_ignore_ascii_case("enforced")
        || raw == "1"
    {
        Some(true)
    } else if raw.eq_ignore_ascii_case("off")
        || raw.eq_ignore_ascii_case("false")
        || raw.eq_ignore_ascii_case("baseline")
        || raw == "0"
    {
        Some(false)
    } else {
        None
    }
}

/// Whether adaptive fill-factor control is enabled (default: disabled).
///
/// Honors the `FSQLITE_ADAPTIVE_FILL_FACTOR` operator override once per process.
#[must_use]
pub fn adaptive_fill_factor_enabled() -> bool {
    apply_adaptive_fill_factor_env_once();
    ADAPTIVE_FILL_FACTOR_ENABLED.load(Ordering::Relaxed)
}

/// Set the process-local adaptive fill-factor enable flag (reversible kill switch).
pub fn set_adaptive_fill_factor_enabled(enabled: bool) {
    ADAPTIVE_FILL_FACTOR_ENV_APPLIED.store(true, Ordering::Release);
    ADAPTIVE_FILL_FACTOR_ENABLED.store(enabled, Ordering::Relaxed);
}

/// Additional fill-factor shift (basis points) the adaptive ramp adds for a
/// given accumulated conflict heat.
///
/// Zero at or below the knee heat (so first contact matches the flat topology
/// policy), increasing linearly to `ADAPTIVE_FILL_FACTOR_MAX_EXTRA_SHIFT_BPS` at
/// the saturation heat, and clamped flat above it. Pure and deterministic.
#[must_use]
fn adaptive_fill_factor_extra_shift_bps(conflict_heat: u64) -> usize {
    if conflict_heat <= ADAPTIVE_FILL_FACTOR_KNEE_HEAT {
        return 0;
    }
    let span = ADAPTIVE_FILL_FACTOR_SATURATION_HEAT.saturating_sub(ADAPTIVE_FILL_FACTOR_KNEE_HEAT);
    if span == 0 {
        return ADAPTIVE_FILL_FACTOR_MAX_EXTRA_SHIFT_BPS;
    }
    let pos = (conflict_heat - ADAPTIVE_FILL_FACTOR_KNEE_HEAT).min(span);
    let max = ADAPTIVE_FILL_FACTOR_MAX_EXTRA_SHIFT_BPS as u64;
    let extra = max * pos / span;
    usize::try_from(extra).unwrap_or(ADAPTIVE_FILL_FACTOR_MAX_EXTRA_SHIFT_BPS)
}

/// Refine a topology-aware split target with adaptive fill-factor control.
///
/// When adaptive control is disabled this returns `topology_target_left_basis_points`
/// unchanged, guaranteeing the baseline/topology split policy is byte-identical.
/// When enabled it biases the target further toward the contended side in
/// proportion to `conflict_heat`, clamped to explicit bounds. Interior splits
/// carry no directional bias and are returned unchanged.
#[must_use]
pub fn adaptive_fill_factor_target(
    predicted_hot_side: &str,
    topology_target_left_basis_points: usize,
    conflict_heat: u64,
) -> usize {
    if !adaptive_fill_factor_enabled() {
        return topology_target_left_basis_points;
    }
    let extra = adaptive_fill_factor_extra_shift_bps(conflict_heat);
    if extra == 0 {
        return topology_target_left_basis_points;
    }
    match predicted_hot_side {
        "left_edge" => topology_target_left_basis_points
            .saturating_sub(extra)
            .max(ADAPTIVE_FILL_FACTOR_LEFT_FLOOR_BPS),
        "right_edge" => topology_target_left_basis_points
            .saturating_add(extra)
            .min(ADAPTIVE_FILL_FACTOR_RIGHT_CEIL_BPS),
        _ => topology_target_left_basis_points,
    }
}

/// Clear accumulated conflict-topology heat.
pub fn reset_conflict_topology_policy_state() {
    *CONFLICT_TOPOLOGY_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = ConflictTopologyState::default();
}

/// Feed one MVCC conflict-heat observation into the B-tree split policy cache.
pub fn record_conflict_topology_heat(
    page: PageNumber,
    conflict_heat: u64,
    writer_overlap_estimate: u32,
) {
    let _updated = record_conflict_topology_heat_batch(
        std::iter::once(page),
        conflict_heat,
        writer_overlap_estimate,
    );
}

/// Feed several MVCC conflict-heat observations under one policy-cache lock.
///
/// Returns the number of pages updated.
#[must_use]
pub fn record_conflict_topology_heat_batch(
    pages: impl IntoIterator<Item = PageNumber>,
    conflict_heat: u64,
    writer_overlap_estimate: u32,
) -> usize {
    if !conflict_topology_policy_enabled() {
        return 0;
    }
    let mut state = CONFLICT_TOPOLOGY_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let conflict_heat = conflict_heat.max(1);
    let mut updated = 0usize;
    for page in pages {
        let page_state = state.pages.entry(page).or_default();
        page_state.heat = page_state.heat.saturating_add(conflict_heat);
        page_state.max_writer_overlap_estimate = page_state
            .max_writer_overlap_estimate
            .max(writer_overlap_estimate);
        if !page_state.deflection_armed
            && page_state.heat >= HOT_PAGE_DEFLECTION_HEAT_THRESHOLD
            && page_state.max_writer_overlap_estimate >= HOT_PAGE_DEFLECTION_OVERLAP_THRESHOLD
        {
            page_state.deflection_armed = true;
            page_state.deflection_credits_remaining = HOT_PAGE_DEFLECTION_BUDGET_PAGES;
        }
        updated += 1;
    }
    updated
}

/// Return split advice for a page about to split.
#[must_use]
pub fn conflict_topology_split_advice(
    page: PageNumber,
    predicted_hot_side: &str,
    baseline_target_left_basis_points: usize,
) -> ConflictTopologySplitAdvice {
    let mode = conflict_topology_policy_mode();
    let mut deflection_credits_before = 0;
    let mut deflection_credits_after = 0;
    let mut deflection_applied = false;

    let page_state = if mode == ConflictTopologyPolicyMode::Baseline {
        ConflictTopologyPageState::default()
    } else {
        let mut state = CONFLICT_TOPOLOGY_STATE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(page_state) = state.pages.get_mut(&page) else {
            return baseline_conflict_topology_split_advice(
                page,
                mode,
                baseline_target_left_basis_points,
            );
        };
        let deflection_active = page_state.deflection_armed
            && page_state.heat >= HOT_PAGE_DEFLECTION_HEAT_THRESHOLD
            && page_state.max_writer_overlap_estimate >= HOT_PAGE_DEFLECTION_OVERLAP_THRESHOLD;
        deflection_credits_before = page_state.deflection_credits_remaining;
        if mode == ConflictTopologyPolicyMode::Enforced
            && deflection_active
            && page_state.deflection_credits_remaining > 0
        {
            page_state.deflection_credits_remaining =
                page_state.deflection_credits_remaining.saturating_sub(1);
            deflection_applied = true;
        }
        deflection_credits_after = page_state.deflection_credits_remaining;
        *page_state
    };
    let topology_hot = page_state.heat >= CONFLICT_TOPOLOGY_HOT_HEAT_THRESHOLD
        && page_state.max_writer_overlap_estimate >= CONFLICT_TOPOLOGY_HOT_OVERLAP_THRESHOLD;
    let topology_target_left_basis_points = if topology_hot {
        topology_adjusted_target(predicted_hot_side, baseline_target_left_basis_points)
    } else {
        baseline_target_left_basis_points
    };
    let deflection_active = page_state.deflection_armed
        && page_state.heat >= HOT_PAGE_DEFLECTION_HEAT_THRESHOLD
        && page_state.max_writer_overlap_estimate >= HOT_PAGE_DEFLECTION_OVERLAP_THRESHOLD;
    let advised_target_left_basis_points = if deflection_active {
        hot_page_deflection_adjusted_target(predicted_hot_side, topology_target_left_basis_points)
    } else {
        topology_target_left_basis_points
    };
    let effective_target_left_basis_points =
        if mode == ConflictTopologyPolicyMode::Enforced && deflection_applied {
            advised_target_left_basis_points
        } else if mode == ConflictTopologyPolicyMode::Enforced && topology_hot {
            topology_target_left_basis_points
        } else {
            baseline_target_left_basis_points
        };
    let applied = effective_target_left_basis_points != baseline_target_left_basis_points;
    let overlap_estimate = i64::from(page_state.max_writer_overlap_estimate.max(1));
    let predicted_overlap_delta = if deflection_applied {
        overlap_estimate * 2
    } else if topology_hot {
        overlap_estimate
    } else {
        0
    };
    let heat_after = if deflection_applied {
        page_state
            .heat
            .saturating_sub(u64::from(page_state.max_writer_overlap_estimate.max(1)))
    } else {
        page_state.heat
    };

    ConflictTopologySplitAdvice {
        policy_id: CONFLICT_TOPOLOGY_POLICY_ID,
        mitigation_policy_id: HOT_PAGE_DEFLECTION_POLICY_ID,
        policy_mode: mode,
        hot_page_id: page.get(),
        baseline_target_left_basis_points,
        advised_target_left_basis_points,
        effective_target_left_basis_points,
        conflict_heat: page_state.heat,
        writer_overlap_estimate: page_state.max_writer_overlap_estimate,
        topology_hot,
        applied,
        deflection_status: hot_page_deflection_status(
            mode,
            deflection_active,
            deflection_applied,
            deflection_credits_before,
        ),
        deflection_credits_before,
        deflection_credits_after,
        budget_ns: HOT_PAGE_DEFLECTION_BUDGET_NS,
        budget_pages: HOT_PAGE_DEFLECTION_BUDGET_PAGES,
        publication_generation: 0,
        heat_before: page_state.heat,
        heat_after,
        trigger_reason: hot_page_deflection_trigger_reason(
            mode,
            topology_hot,
            deflection_active,
            deflection_credits_before,
        ),
        migration_outcome: hot_page_deflection_migration_outcome(
            mode,
            deflection_active,
            deflection_applied,
            deflection_credits_before,
        ),
        rollback_reason: hot_page_deflection_rollback_reason(
            mode,
            deflection_active,
            deflection_applied,
            deflection_credits_before,
        ),
        predicted_overlap_delta,
        operator_override_active: mode == ConflictTopologyPolicyMode::Baseline,
    }
}

fn baseline_conflict_topology_split_advice(
    page: PageNumber,
    mode: ConflictTopologyPolicyMode,
    baseline_target_left_basis_points: usize,
) -> ConflictTopologySplitAdvice {
    ConflictTopologySplitAdvice {
        policy_id: CONFLICT_TOPOLOGY_POLICY_ID,
        mitigation_policy_id: HOT_PAGE_DEFLECTION_POLICY_ID,
        policy_mode: mode,
        hot_page_id: page.get(),
        baseline_target_left_basis_points,
        advised_target_left_basis_points: baseline_target_left_basis_points,
        effective_target_left_basis_points: baseline_target_left_basis_points,
        conflict_heat: 0,
        writer_overlap_estimate: 0,
        topology_hot: false,
        applied: false,
        deflection_status: hot_page_deflection_status(mode, false, false, 0),
        deflection_credits_before: 0,
        deflection_credits_after: 0,
        budget_ns: HOT_PAGE_DEFLECTION_BUDGET_NS,
        budget_pages: HOT_PAGE_DEFLECTION_BUDGET_PAGES,
        publication_generation: 0,
        heat_before: 0,
        heat_after: 0,
        trigger_reason: hot_page_deflection_trigger_reason(mode, false, false, 0),
        migration_outcome: hot_page_deflection_migration_outcome(mode, false, false, 0),
        rollback_reason: hot_page_deflection_rollback_reason(mode, false, false, 0),
        predicted_overlap_delta: 0,
        operator_override_active: mode == ConflictTopologyPolicyMode::Baseline,
    }
}

fn topology_adjusted_target(predicted_hot_side: &str, baseline: usize) -> usize {
    match predicted_hot_side {
        "left_edge" => baseline
            .saturating_sub(CONFLICT_TOPOLOGY_TARGET_SHIFT_BPS)
            .max(2_500),
        "right_edge" => baseline
            .saturating_add(CONFLICT_TOPOLOGY_TARGET_SHIFT_BPS)
            .min(8_000),
        _ => 5_000,
    }
}

fn hot_page_deflection_adjusted_target(predicted_hot_side: &str, baseline: usize) -> usize {
    match predicted_hot_side {
        "left_edge" => baseline
            .saturating_sub(HOT_PAGE_DEFLECTION_TARGET_SHIFT_BPS)
            .max(1_500),
        "right_edge" => baseline
            .saturating_add(HOT_PAGE_DEFLECTION_TARGET_SHIFT_BPS)
            .min(9_000),
        _ => baseline,
    }
}

fn hot_page_deflection_trigger_reason(
    mode: ConflictTopologyPolicyMode,
    topology_hot: bool,
    deflection_active: bool,
    deflection_credits_before: u8,
) -> &'static str {
    if mode == ConflictTopologyPolicyMode::Baseline {
        "operator_override_baseline"
    } else if deflection_active && deflection_credits_before > 0 {
        "pathological_hot_page"
    } else if deflection_active {
        "pathological_hot_page_budget_exhausted"
    } else if topology_hot {
        "below_deflection_threshold"
    } else {
        "no_hot_page_signal"
    }
}

fn hot_page_deflection_status(
    mode: ConflictTopologyPolicyMode,
    deflection_active: bool,
    deflection_applied: bool,
    deflection_credits_before: u8,
) -> HotPageDeflectionStatus {
    if mode == ConflictTopologyPolicyMode::Baseline {
        HotPageDeflectionStatus::OperatorOverride
    } else if deflection_applied {
        HotPageDeflectionStatus::Applied
    } else if deflection_active && deflection_credits_before == 0 {
        HotPageDeflectionStatus::BudgetExhausted
    } else if deflection_active && mode == ConflictTopologyPolicyMode::Advisory {
        HotPageDeflectionStatus::AdvisoryOnly
    } else {
        HotPageDeflectionStatus::Inactive
    }
}

fn hot_page_deflection_migration_outcome(
    mode: ConflictTopologyPolicyMode,
    deflection_active: bool,
    deflection_applied: bool,
    deflection_credits_before: u8,
) -> &'static str {
    if mode == ConflictTopologyPolicyMode::Baseline {
        "operator_override_baseline"
    } else if deflection_applied {
        "split_deflected"
    } else if deflection_active && deflection_credits_before == 0 {
        "budget_exhausted"
    } else if deflection_active && mode == ConflictTopologyPolicyMode::Advisory {
        "advisory_only"
    } else {
        "not_triggered"
    }
}

fn hot_page_deflection_rollback_reason(
    mode: ConflictTopologyPolicyMode,
    deflection_active: bool,
    deflection_applied: bool,
    deflection_credits_before: u8,
) -> &'static str {
    if mode == ConflictTopologyPolicyMode::Baseline {
        "operator_override_baseline"
    } else if deflection_applied {
        "none"
    } else if deflection_active && deflection_credits_before == 0 {
        "budget_exhausted"
    } else if deflection_active && mode == ConflictTopologyPolicyMode::Advisory {
        "advisory_only"
    } else {
        "no_hotspot"
    }
}

fn apply_conflict_topology_policy_env_once() {
    if CONFLICT_TOPOLOGY_POLICY_ENV_APPLIED
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }
    let Ok(raw) = std::env::var(CONFLICT_TOPOLOGY_POLICY_ENV) else {
        return;
    };
    if let Some(mode) = parse_conflict_topology_policy_mode(raw.as_str()) {
        CONFLICT_TOPOLOGY_POLICY_MODE.store(mode.to_raw(), Ordering::Relaxed);
    }
}

fn parse_conflict_topology_policy_mode(raw: &str) -> Option<ConflictTopologyPolicyMode> {
    let raw = raw.trim();
    if raw.eq_ignore_ascii_case("baseline")
        || raw.eq_ignore_ascii_case("off")
        || raw.eq_ignore_ascii_case("false")
        || raw == "0"
    {
        Some(ConflictTopologyPolicyMode::Baseline)
    } else if raw.eq_ignore_ascii_case("advisory")
        || raw.eq_ignore_ascii_case("shadow")
        || raw == "1"
    {
        Some(ConflictTopologyPolicyMode::Advisory)
    } else if raw.eq_ignore_ascii_case("enforced")
        || raw.eq_ignore_ascii_case("on")
        || raw.eq_ignore_ascii_case("true")
        || raw == "2"
    {
        Some(ConflictTopologyPolicyMode::Enforced)
    } else {
        None
    }
}

// ── Swizzle metrics (bd-3ta.3) ──────────────────────────────────────────────

/// Swizzle ratio gauge: (swizzled_count / total_tracked) * 1000.
static SWIZZLE_RATIO_GAUGE: AtomicU64 = AtomicU64::new(0);
/// Total swizzle faults (CAS failures + retry attempts).
static SWIZZLE_FAULTS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Total successful swizzle operations.
static SWIZZLE_IN_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Total successful unswizzle operations.
static SWIZZLE_OUT_TOTAL: AtomicU64 = AtomicU64::new(0);

// ── Copy-heavy payload/cell kernel metrics (bd-db300.4.4.1) ─────────────────

static BTREE_COPY_PROFILE_ENABLED: AtomicBool = AtomicBool::new(false);
static BTREE_LOCAL_PAYLOAD_COPY_CALLS: AtomicU64 = AtomicU64::new(0);
static BTREE_LOCAL_PAYLOAD_COPY_BYTES: AtomicU64 = AtomicU64::new(0);
static BTREE_OWNED_PAYLOAD_MATERIALIZATION_CALLS: AtomicU64 = AtomicU64::new(0);
static BTREE_OWNED_PAYLOAD_MATERIALIZATION_BYTES: AtomicU64 = AtomicU64::new(0);
static BTREE_OVERFLOW_REASSEMBLY_CALLS: AtomicU64 = AtomicU64::new(0);
static BTREE_OVERFLOW_LOCAL_BYTES: AtomicU64 = AtomicU64::new(0);
static BTREE_OVERFLOW_BYTES: AtomicU64 = AtomicU64::new(0);
static BTREE_OVERFLOW_PAGE_READS: AtomicU64 = AtomicU64::new(0);
static BTREE_TABLE_LEAF_CELL_ASSEMBLY_CALLS: AtomicU64 = AtomicU64::new(0);
static BTREE_TABLE_LEAF_CELL_ASSEMBLY_BYTES: AtomicU64 = AtomicU64::new(0);
static BTREE_INDEX_LEAF_CELL_ASSEMBLY_CALLS: AtomicU64 = AtomicU64::new(0);
static BTREE_INDEX_LEAF_CELL_ASSEMBLY_BYTES: AtomicU64 = AtomicU64::new(0);
static BTREE_INTERIOR_CELL_REBUILD_CALLS: AtomicU64 = AtomicU64::new(0);
static BTREE_INTERIOR_CELL_REBUILD_BYTES: AtomicU64 = AtomicU64::new(0);
static BTREE_NO_SPLIT_REUSE_HITS: AtomicU64 = AtomicU64::new(0);
static BTREE_CONSERVATIVE_RELOAD_FALLBACKS: AtomicU64 = AtomicU64::new(0);
static BTREE_PAGE_HEADER_REBUILD_COUNT: AtomicU64 = AtomicU64::new(0);
static BTREE_FAST_TABLE_LEAF_PAYLOAD_APPEND_CALLS: AtomicU64 = AtomicU64::new(0);
static BTREE_FAST_TABLE_LEAF_PAYLOAD_MUTATE_TIME_NS: AtomicU64 = AtomicU64::new(0);
static BTREE_FAST_TABLE_LEAF_PAYLOAD_STAGE_TIME_NS: AtomicU64 = AtomicU64::new(0);
static BTREE_FAST_TABLE_LEAF_FULL_CELL_APPEND_CALLS: AtomicU64 = AtomicU64::new(0);
static BTREE_FAST_TABLE_LEAF_FULL_CELL_MUTATE_TIME_NS: AtomicU64 = AtomicU64::new(0);
static BTREE_FAST_TABLE_LEAF_FULL_CELL_STAGE_TIME_NS: AtomicU64 = AtomicU64::new(0);
static BTREE_QUICK_BALANCE_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
static BTREE_QUICK_BALANCE_HITS: AtomicU64 = AtomicU64::new(0);
static BTREE_QUICK_BALANCE_TIME_NS: AtomicU64 = AtomicU64::new(0);
static BTREE_LOCAL_SPLIT_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
static BTREE_LOCAL_SPLIT_HITS: AtomicU64 = AtomicU64::new(0);
static BTREE_LOCAL_SPLIT_TIME_NS: AtomicU64 = AtomicU64::new(0);
static BTREE_NONROOT_BALANCE_CALLS: AtomicU64 = AtomicU64::new(0);
static BTREE_NONROOT_BALANCE_TIME_NS: AtomicU64 = AtomicU64::new(0);
static BTREE_BALANCE_FOR_DELETE_CALLS: AtomicU64 = AtomicU64::new(0);
static BTREE_DELETE_LEAF_RUN_MATERIALIZE_CALLS: AtomicU64 = AtomicU64::new(0);
static BTREE_DELETE_LEAF_RUN_MATERIALIZE_TIME_NS: AtomicU64 = AtomicU64::new(0);
static BTREE_DELETE_LEAF_RUN_WRITE_CALLS: AtomicU64 = AtomicU64::new(0);
static BTREE_DELETE_LEAF_RUN_WRITE_TIME_NS: AtomicU64 = AtomicU64::new(0);
static BTREE_DELETE_LEAF_RUN_SEARCH_CALLS: AtomicU64 = AtomicU64::new(0);
static BTREE_DELETE_LEAF_RUN_SEARCH_TIME_NS: AtomicU64 = AtomicU64::new(0);
static BTREE_DELETE_LEAF_RUN_DUPLICATE_CHECK_CALLS: AtomicU64 = AtomicU64::new(0);
static BTREE_DELETE_LEAF_RUN_DUPLICATE_CHECK_TIME_NS: AtomicU64 = AtomicU64::new(0);
static BTREE_DELETE_LEAF_RUN_COMPACT_CHECK_CALLS: AtomicU64 = AtomicU64::new(0);
static BTREE_DELETE_LEAF_RUN_COMPACT_CHECK_TIME_NS: AtomicU64 = AtomicU64::new(0);
static BTREE_DELETE_LEAF_RUN_CELL_PARSE_CALLS: AtomicU64 = AtomicU64::new(0);
static BTREE_DELETE_LEAF_RUN_CELL_PARSE_TIME_NS: AtomicU64 = AtomicU64::new(0);
static BTREE_BULK_TABLE_GROUPING_CALLS: AtomicU64 = AtomicU64::new(0);
static BTREE_BULK_TABLE_GROUPING_TIME_NS: AtomicU64 = AtomicU64::new(0);
static BTREE_BULK_TABLE_LEAF_PAGE_BUILD_CALLS: AtomicU64 = AtomicU64::new(0);
static BTREE_BULK_TABLE_LEAF_PAGE_BUILD_TIME_NS: AtomicU64 = AtomicU64::new(0);
static BTREE_BULK_TABLE_LEAF_PAGE_WRITE_CALLS: AtomicU64 = AtomicU64::new(0);
static BTREE_BULK_TABLE_LEAF_PAGE_WRITE_TIME_NS: AtomicU64 = AtomicU64::new(0);
static BTREE_BULK_TABLE_INTERIOR_PAGE_BUILD_CALLS: AtomicU64 = AtomicU64::new(0);
static BTREE_BULK_TABLE_INTERIOR_PAGE_BUILD_TIME_NS: AtomicU64 = AtomicU64::new(0);
static BTREE_BULK_TABLE_INTERIOR_PAGE_WRITE_CALLS: AtomicU64 = AtomicU64::new(0);
static BTREE_BULK_TABLE_INTERIOR_PAGE_WRITE_TIME_NS: AtomicU64 = AtomicU64::new(0);

#[inline]
pub(crate) fn copy_profile_enabled() -> bool {
    BTREE_COPY_PROFILE_ENABLED.load(Ordering::Relaxed)
}

/// Start a profiling timer for a hot-path segment.
///
/// Returns `Some(Instant::now())` only when the copy profile gate is enabled;
/// otherwise returns `None`. Call sites capture this once at segment start,
/// run their work unconditionally, then pass the sentinel to the paired
/// `record_*` recorder — which is a no-op when the sentinel is `None`.
///
/// Purpose: avoid paying `clock_gettime` on every invocation when profiling
/// is off. Previously the pattern `let t = Instant::now(); /* work */;
/// record_x(t.elapsed())` captured the clock unconditionally and the gate
/// was checked only inside the recorder — wasting two `clock_gettime`
/// syscalls per hot-path op.
#[inline]
pub(crate) fn profile_start() -> Option<std::time::Instant> {
    copy_profile_enabled().then(std::time::Instant::now)
}

#[inline]
fn profile_elapsed_ns(start: Option<std::time::Instant>) -> Option<u64> {
    let s = start?;
    Some(u64::try_from(s.elapsed().as_nanos()).unwrap_or(u64::MAX))
}

#[inline]
fn saturating_add_bytes(counter: &AtomicU64, bytes: usize) {
    counter.fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
}

#[inline]
pub(crate) fn record_operation(op_type: BtreeOpType) {
    if !btree_metrics_enabled() {
        return;
    }
    record_operation_cold(op_type);
}

#[cold]
#[inline(never)]
fn record_operation_cold(op_type: BtreeOpType) {
    let counter = match op_type {
        BtreeOpType::Seek => &BTREE_OP_SEEK_TOTAL,
        BtreeOpType::Insert => &BTREE_OP_INSERT_TOTAL,
        BtreeOpType::Delete => &BTREE_OP_DELETE_TOTAL,
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

pub fn set_btree_copy_profile_enabled(enabled: bool) {
    BTREE_COPY_PROFILE_ENABLED.store(enabled, Ordering::Relaxed);
}

/// Toggle the B-tree hot-path metrics gate.
///
/// When `false` (the default), `record_operation`, `set_depth_gauge`,
/// `record_split_event`, `record_no_split_reuse_hit`,
/// `record_conservative_reload_fallback`, and `record_page_header_rebuild`
/// each early-exit on an `AtomicBool` load instead of paying a `fetch_add` /
/// `store` on a contended global atomic. Diagnostic snapshots (`E2E
/// `HotPathMetricsCapture`, observability tests) flip this on.
pub fn set_btree_metrics_enabled(enabled: bool) {
    FSQLITE_BTREE_METRICS_ENABLED.store(enabled, Ordering::Relaxed);
}

/// Read the B-tree hot-path metrics gate.
#[inline]
#[must_use]
pub fn btree_metrics_enabled() -> bool {
    FSQLITE_BTREE_METRICS_ENABLED.load(Ordering::Relaxed)
}

pub(crate) fn record_local_payload_copy(bytes: usize) {
    if !copy_profile_enabled() {
        return;
    }
    BTREE_LOCAL_PAYLOAD_COPY_CALLS.fetch_add(1, Ordering::Relaxed);
    saturating_add_bytes(&BTREE_LOCAL_PAYLOAD_COPY_BYTES, bytes);
}

pub(crate) fn record_owned_payload_materialization(bytes: usize) {
    if !copy_profile_enabled() {
        return;
    }
    BTREE_OWNED_PAYLOAD_MATERIALIZATION_CALLS.fetch_add(1, Ordering::Relaxed);
    saturating_add_bytes(&BTREE_OWNED_PAYLOAD_MATERIALIZATION_BYTES, bytes);
}

pub(crate) fn record_overflow_chain_reassembly(
    local_bytes: usize,
    overflow_bytes: usize,
    overflow_page_reads: usize,
) {
    if !copy_profile_enabled() {
        return;
    }
    BTREE_OVERFLOW_REASSEMBLY_CALLS.fetch_add(1, Ordering::Relaxed);
    saturating_add_bytes(&BTREE_OVERFLOW_LOCAL_BYTES, local_bytes);
    saturating_add_bytes(&BTREE_OVERFLOW_BYTES, overflow_bytes);
    saturating_add_bytes(&BTREE_OVERFLOW_PAGE_READS, overflow_page_reads);
}

pub(crate) fn record_table_leaf_cell_assembly(bytes: usize) {
    if !copy_profile_enabled() {
        return;
    }
    BTREE_TABLE_LEAF_CELL_ASSEMBLY_CALLS.fetch_add(1, Ordering::Relaxed);
    saturating_add_bytes(&BTREE_TABLE_LEAF_CELL_ASSEMBLY_BYTES, bytes);
}

pub(crate) fn record_index_leaf_cell_assembly(bytes: usize) {
    if !copy_profile_enabled() {
        return;
    }
    BTREE_INDEX_LEAF_CELL_ASSEMBLY_CALLS.fetch_add(1, Ordering::Relaxed);
    saturating_add_bytes(&BTREE_INDEX_LEAF_CELL_ASSEMBLY_BYTES, bytes);
}

pub(crate) fn record_interior_cell_rebuild(bytes: usize) {
    if !copy_profile_enabled() {
        return;
    }
    BTREE_INTERIOR_CELL_REBUILD_CALLS.fetch_add(1, Ordering::Relaxed);
    saturating_add_bytes(&BTREE_INTERIOR_CELL_REBUILD_BYTES, bytes);
}

#[inline]
pub(crate) fn record_no_split_reuse_hit() {
    if !btree_metrics_enabled() {
        return;
    }
    BTREE_NO_SPLIT_REUSE_HITS.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub(crate) fn record_conservative_reload_fallback() {
    if !btree_metrics_enabled() {
        return;
    }
    BTREE_CONSERVATIVE_RELOAD_FALLBACKS.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub(crate) fn record_page_header_rebuild() {
    if !btree_metrics_enabled() {
        return;
    }
    BTREE_PAGE_HEADER_REBUILD_COUNT.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_fast_table_leaf_payload_append_mutate(start: Option<std::time::Instant>) {
    let Some(duration_ns) = profile_elapsed_ns(start) else {
        return;
    };
    BTREE_FAST_TABLE_LEAF_PAYLOAD_APPEND_CALLS.fetch_add(1, Ordering::Relaxed);
    BTREE_FAST_TABLE_LEAF_PAYLOAD_MUTATE_TIME_NS.fetch_add(duration_ns, Ordering::Relaxed);
}

pub(crate) fn record_fast_table_leaf_payload_append_stage(start: Option<std::time::Instant>) {
    let Some(duration_ns) = profile_elapsed_ns(start) else {
        return;
    };
    BTREE_FAST_TABLE_LEAF_PAYLOAD_STAGE_TIME_NS.fetch_add(duration_ns, Ordering::Relaxed);
}

pub(crate) fn record_fast_table_leaf_full_cell_append_mutate(start: Option<std::time::Instant>) {
    let Some(duration_ns) = profile_elapsed_ns(start) else {
        return;
    };
    BTREE_FAST_TABLE_LEAF_FULL_CELL_APPEND_CALLS.fetch_add(1, Ordering::Relaxed);
    BTREE_FAST_TABLE_LEAF_FULL_CELL_MUTATE_TIME_NS.fetch_add(duration_ns, Ordering::Relaxed);
}

pub(crate) fn record_fast_table_leaf_full_cell_append_stage(start: Option<std::time::Instant>) {
    let Some(duration_ns) = profile_elapsed_ns(start) else {
        return;
    };
    BTREE_FAST_TABLE_LEAF_FULL_CELL_STAGE_TIME_NS.fetch_add(duration_ns, Ordering::Relaxed);
}

pub(crate) fn record_quick_balance_attempt(start: Option<std::time::Instant>, hit: bool) {
    let Some(duration_ns) = profile_elapsed_ns(start) else {
        return;
    };
    BTREE_QUICK_BALANCE_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
    if hit {
        BTREE_QUICK_BALANCE_HITS.fetch_add(1, Ordering::Relaxed);
    }
    BTREE_QUICK_BALANCE_TIME_NS.fetch_add(duration_ns, Ordering::Relaxed);
}

pub(crate) fn record_local_split_attempt(start: Option<std::time::Instant>, hit: bool) {
    let Some(duration_ns) = profile_elapsed_ns(start) else {
        return;
    };
    BTREE_LOCAL_SPLIT_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
    if hit {
        BTREE_LOCAL_SPLIT_HITS.fetch_add(1, Ordering::Relaxed);
    }
    BTREE_LOCAL_SPLIT_TIME_NS.fetch_add(duration_ns, Ordering::Relaxed);
}

pub(crate) fn record_nonroot_balance(start: Option<std::time::Instant>) {
    let Some(duration_ns) = profile_elapsed_ns(start) else {
        return;
    };
    BTREE_NONROOT_BALANCE_CALLS.fetch_add(1, Ordering::Relaxed);
    BTREE_NONROOT_BALANCE_TIME_NS.fetch_add(duration_ns, Ordering::Relaxed);
}

/// Count one invocation of the deferred-delete rebalance fixup.
///
/// Gated on the general B-tree metrics flag (not the copy-profile flag) so the
/// production hot path pays a single `AtomicBool` load when metrics are off.
pub(crate) fn record_balance_for_delete() {
    if !btree_metrics_enabled() {
        return;
    }
    BTREE_BALANCE_FOR_DELETE_CALLS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_delete_leaf_run_materialize(start: Option<std::time::Instant>) {
    let Some(duration_ns) = profile_elapsed_ns(start) else {
        return;
    };
    BTREE_DELETE_LEAF_RUN_MATERIALIZE_CALLS.fetch_add(1, Ordering::Relaxed);
    BTREE_DELETE_LEAF_RUN_MATERIALIZE_TIME_NS.fetch_add(duration_ns, Ordering::Relaxed);
}

pub(crate) fn record_delete_leaf_run_write(start: Option<std::time::Instant>) {
    let Some(duration_ns) = profile_elapsed_ns(start) else {
        return;
    };
    BTREE_DELETE_LEAF_RUN_WRITE_CALLS.fetch_add(1, Ordering::Relaxed);
    BTREE_DELETE_LEAF_RUN_WRITE_TIME_NS.fetch_add(duration_ns, Ordering::Relaxed);
}

pub(crate) fn record_delete_leaf_run_search(start: Option<std::time::Instant>) {
    let Some(duration_ns) = profile_elapsed_ns(start) else {
        return;
    };
    BTREE_DELETE_LEAF_RUN_SEARCH_CALLS.fetch_add(1, Ordering::Relaxed);
    BTREE_DELETE_LEAF_RUN_SEARCH_TIME_NS.fetch_add(duration_ns, Ordering::Relaxed);
}

pub(crate) fn record_delete_leaf_run_duplicate_check(start: Option<std::time::Instant>) {
    let Some(duration_ns) = profile_elapsed_ns(start) else {
        return;
    };
    BTREE_DELETE_LEAF_RUN_DUPLICATE_CHECK_CALLS.fetch_add(1, Ordering::Relaxed);
    BTREE_DELETE_LEAF_RUN_DUPLICATE_CHECK_TIME_NS.fetch_add(duration_ns, Ordering::Relaxed);
}

pub(crate) fn record_delete_leaf_run_compact_check(start: Option<std::time::Instant>) {
    let Some(duration_ns) = profile_elapsed_ns(start) else {
        return;
    };
    BTREE_DELETE_LEAF_RUN_COMPACT_CHECK_CALLS.fetch_add(1, Ordering::Relaxed);
    BTREE_DELETE_LEAF_RUN_COMPACT_CHECK_TIME_NS.fetch_add(duration_ns, Ordering::Relaxed);
}

pub(crate) fn record_delete_leaf_run_cell_parse(start: Option<std::time::Instant>) {
    let Some(duration_ns) = profile_elapsed_ns(start) else {
        return;
    };
    BTREE_DELETE_LEAF_RUN_CELL_PARSE_CALLS.fetch_add(1, Ordering::Relaxed);
    BTREE_DELETE_LEAF_RUN_CELL_PARSE_TIME_NS.fetch_add(duration_ns, Ordering::Relaxed);
}

pub(crate) fn record_bulk_table_grouping(start: Option<std::time::Instant>) {
    let Some(duration_ns) = profile_elapsed_ns(start) else {
        return;
    };
    BTREE_BULK_TABLE_GROUPING_CALLS.fetch_add(1, Ordering::Relaxed);
    BTREE_BULK_TABLE_GROUPING_TIME_NS.fetch_add(duration_ns, Ordering::Relaxed);
}

pub(crate) fn record_bulk_table_leaf_page_build(start: Option<std::time::Instant>) {
    let Some(duration_ns) = profile_elapsed_ns(start) else {
        return;
    };
    BTREE_BULK_TABLE_LEAF_PAGE_BUILD_CALLS.fetch_add(1, Ordering::Relaxed);
    BTREE_BULK_TABLE_LEAF_PAGE_BUILD_TIME_NS.fetch_add(duration_ns, Ordering::Relaxed);
}

pub(crate) fn record_bulk_table_leaf_page_write(start: Option<std::time::Instant>) {
    let Some(duration_ns) = profile_elapsed_ns(start) else {
        return;
    };
    BTREE_BULK_TABLE_LEAF_PAGE_WRITE_CALLS.fetch_add(1, Ordering::Relaxed);
    BTREE_BULK_TABLE_LEAF_PAGE_WRITE_TIME_NS.fetch_add(duration_ns, Ordering::Relaxed);
}

pub(crate) fn record_bulk_table_interior_page_build(start: Option<std::time::Instant>) {
    let Some(duration_ns) = profile_elapsed_ns(start) else {
        return;
    };
    BTREE_BULK_TABLE_INTERIOR_PAGE_BUILD_CALLS.fetch_add(1, Ordering::Relaxed);
    BTREE_BULK_TABLE_INTERIOR_PAGE_BUILD_TIME_NS.fetch_add(duration_ns, Ordering::Relaxed);
}

pub(crate) fn record_bulk_table_interior_page_write(start: Option<std::time::Instant>) {
    let Some(duration_ns) = profile_elapsed_ns(start) else {
        return;
    };
    BTREE_BULK_TABLE_INTERIOR_PAGE_WRITE_CALLS.fetch_add(1, Ordering::Relaxed);
    BTREE_BULK_TABLE_INTERIOR_PAGE_WRITE_TIME_NS.fetch_add(duration_ns, Ordering::Relaxed);
}

#[inline]
pub(crate) fn record_split_event() {
    if !btree_metrics_enabled() {
        return;
    }
    BTREE_PAGE_SPLITS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub(crate) fn set_depth_gauge(depth: usize) {
    if !btree_metrics_enabled() {
        return;
    }
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

#[must_use]
pub fn btree_copy_profile_snapshot() -> BtreeCopyProfileSnapshot {
    BtreeCopyProfileSnapshot {
        local_payload_copy_calls: BTREE_LOCAL_PAYLOAD_COPY_CALLS.load(Ordering::Relaxed),
        local_payload_copy_bytes: BTREE_LOCAL_PAYLOAD_COPY_BYTES.load(Ordering::Relaxed),
        owned_payload_materialization_calls: BTREE_OWNED_PAYLOAD_MATERIALIZATION_CALLS
            .load(Ordering::Relaxed),
        owned_payload_materialization_bytes: BTREE_OWNED_PAYLOAD_MATERIALIZATION_BYTES
            .load(Ordering::Relaxed),
        overflow_chain_reassembly_calls: BTREE_OVERFLOW_REASSEMBLY_CALLS.load(Ordering::Relaxed),
        overflow_chain_local_bytes: BTREE_OVERFLOW_LOCAL_BYTES.load(Ordering::Relaxed),
        overflow_chain_overflow_bytes: BTREE_OVERFLOW_BYTES.load(Ordering::Relaxed),
        overflow_page_reads: BTREE_OVERFLOW_PAGE_READS.load(Ordering::Relaxed),
        table_leaf_cell_assembly_calls: BTREE_TABLE_LEAF_CELL_ASSEMBLY_CALLS
            .load(Ordering::Relaxed),
        table_leaf_cell_assembly_bytes: BTREE_TABLE_LEAF_CELL_ASSEMBLY_BYTES
            .load(Ordering::Relaxed),
        index_leaf_cell_assembly_calls: BTREE_INDEX_LEAF_CELL_ASSEMBLY_CALLS
            .load(Ordering::Relaxed),
        index_leaf_cell_assembly_bytes: BTREE_INDEX_LEAF_CELL_ASSEMBLY_BYTES
            .load(Ordering::Relaxed),
        interior_cell_rebuild_calls: BTREE_INTERIOR_CELL_REBUILD_CALLS.load(Ordering::Relaxed),
        interior_cell_rebuild_bytes: BTREE_INTERIOR_CELL_REBUILD_BYTES.load(Ordering::Relaxed),
    }
}

#[must_use]
pub fn btree_leaf_reuse_snapshot() -> BtreeLeafReuseSnapshot {
    BtreeLeafReuseSnapshot {
        no_split_reuse_hits: BTREE_NO_SPLIT_REUSE_HITS.load(Ordering::Relaxed),
        conservative_reload_fallbacks: BTREE_CONSERVATIVE_RELOAD_FALLBACKS.load(Ordering::Relaxed),
        page_header_rebuild_count: BTREE_PAGE_HEADER_REBUILD_COUNT.load(Ordering::Relaxed),
        fast_table_leaf_payload_appends: BTREE_FAST_TABLE_LEAF_PAYLOAD_APPEND_CALLS
            .load(Ordering::Relaxed),
        fast_table_leaf_payload_mutate_time_ns: BTREE_FAST_TABLE_LEAF_PAYLOAD_MUTATE_TIME_NS
            .load(Ordering::Relaxed),
        fast_table_leaf_payload_stage_time_ns: BTREE_FAST_TABLE_LEAF_PAYLOAD_STAGE_TIME_NS
            .load(Ordering::Relaxed),
        fast_table_leaf_full_cell_appends: BTREE_FAST_TABLE_LEAF_FULL_CELL_APPEND_CALLS
            .load(Ordering::Relaxed),
        fast_table_leaf_full_cell_mutate_time_ns: BTREE_FAST_TABLE_LEAF_FULL_CELL_MUTATE_TIME_NS
            .load(Ordering::Relaxed),
        fast_table_leaf_full_cell_stage_time_ns: BTREE_FAST_TABLE_LEAF_FULL_CELL_STAGE_TIME_NS
            .load(Ordering::Relaxed),
        quick_balance_attempts: BTREE_QUICK_BALANCE_ATTEMPTS.load(Ordering::Relaxed),
        quick_balance_hits: BTREE_QUICK_BALANCE_HITS.load(Ordering::Relaxed),
        quick_balance_time_ns: BTREE_QUICK_BALANCE_TIME_NS.load(Ordering::Relaxed),
        local_split_attempts: BTREE_LOCAL_SPLIT_ATTEMPTS.load(Ordering::Relaxed),
        local_split_hits: BTREE_LOCAL_SPLIT_HITS.load(Ordering::Relaxed),
        local_split_time_ns: BTREE_LOCAL_SPLIT_TIME_NS.load(Ordering::Relaxed),
        nonroot_balance_calls: BTREE_NONROOT_BALANCE_CALLS.load(Ordering::Relaxed),
        nonroot_balance_time_ns: BTREE_NONROOT_BALANCE_TIME_NS.load(Ordering::Relaxed),
        balance_for_delete_calls: BTREE_BALANCE_FOR_DELETE_CALLS.load(Ordering::Relaxed),
        delete_leaf_run_materialize_calls: BTREE_DELETE_LEAF_RUN_MATERIALIZE_CALLS
            .load(Ordering::Relaxed),
        delete_leaf_run_materialize_time_ns: BTREE_DELETE_LEAF_RUN_MATERIALIZE_TIME_NS
            .load(Ordering::Relaxed),
        delete_leaf_run_write_calls: BTREE_DELETE_LEAF_RUN_WRITE_CALLS.load(Ordering::Relaxed),
        delete_leaf_run_write_time_ns: BTREE_DELETE_LEAF_RUN_WRITE_TIME_NS.load(Ordering::Relaxed),
        delete_leaf_run_search_calls: BTREE_DELETE_LEAF_RUN_SEARCH_CALLS.load(Ordering::Relaxed),
        delete_leaf_run_search_time_ns: BTREE_DELETE_LEAF_RUN_SEARCH_TIME_NS
            .load(Ordering::Relaxed),
        delete_leaf_run_duplicate_check_calls: BTREE_DELETE_LEAF_RUN_DUPLICATE_CHECK_CALLS
            .load(Ordering::Relaxed),
        delete_leaf_run_duplicate_check_time_ns: BTREE_DELETE_LEAF_RUN_DUPLICATE_CHECK_TIME_NS
            .load(Ordering::Relaxed),
        delete_leaf_run_compact_check_calls: BTREE_DELETE_LEAF_RUN_COMPACT_CHECK_CALLS
            .load(Ordering::Relaxed),
        delete_leaf_run_compact_check_time_ns: BTREE_DELETE_LEAF_RUN_COMPACT_CHECK_TIME_NS
            .load(Ordering::Relaxed),
        delete_leaf_run_cell_parse_calls: BTREE_DELETE_LEAF_RUN_CELL_PARSE_CALLS
            .load(Ordering::Relaxed),
        delete_leaf_run_cell_parse_time_ns: BTREE_DELETE_LEAF_RUN_CELL_PARSE_TIME_NS
            .load(Ordering::Relaxed),
        bulk_table_grouping_calls: BTREE_BULK_TABLE_GROUPING_CALLS.load(Ordering::Relaxed),
        bulk_table_grouping_time_ns: BTREE_BULK_TABLE_GROUPING_TIME_NS.load(Ordering::Relaxed),
        bulk_table_leaf_page_build_calls: BTREE_BULK_TABLE_LEAF_PAGE_BUILD_CALLS
            .load(Ordering::Relaxed),
        bulk_table_leaf_page_build_time_ns: BTREE_BULK_TABLE_LEAF_PAGE_BUILD_TIME_NS
            .load(Ordering::Relaxed),
        bulk_table_leaf_page_write_calls: BTREE_BULK_TABLE_LEAF_PAGE_WRITE_CALLS
            .load(Ordering::Relaxed),
        bulk_table_leaf_page_write_time_ns: BTREE_BULK_TABLE_LEAF_PAGE_WRITE_TIME_NS
            .load(Ordering::Relaxed),
        bulk_table_interior_page_build_calls: BTREE_BULK_TABLE_INTERIOR_PAGE_BUILD_CALLS
            .load(Ordering::Relaxed),
        bulk_table_interior_page_build_time_ns: BTREE_BULK_TABLE_INTERIOR_PAGE_BUILD_TIME_NS
            .load(Ordering::Relaxed),
        bulk_table_interior_page_write_calls: BTREE_BULK_TABLE_INTERIOR_PAGE_WRITE_CALLS
            .load(Ordering::Relaxed),
        bulk_table_interior_page_write_time_ns: BTREE_BULK_TABLE_INTERIOR_PAGE_WRITE_TIME_NS
            .load(Ordering::Relaxed),
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

pub fn reset_btree_copy_profile() {
    BTREE_LOCAL_PAYLOAD_COPY_CALLS.store(0, Ordering::Relaxed);
    BTREE_LOCAL_PAYLOAD_COPY_BYTES.store(0, Ordering::Relaxed);
    BTREE_OWNED_PAYLOAD_MATERIALIZATION_CALLS.store(0, Ordering::Relaxed);
    BTREE_OWNED_PAYLOAD_MATERIALIZATION_BYTES.store(0, Ordering::Relaxed);
    BTREE_OVERFLOW_REASSEMBLY_CALLS.store(0, Ordering::Relaxed);
    BTREE_OVERFLOW_LOCAL_BYTES.store(0, Ordering::Relaxed);
    BTREE_OVERFLOW_BYTES.store(0, Ordering::Relaxed);
    BTREE_OVERFLOW_PAGE_READS.store(0, Ordering::Relaxed);
    BTREE_TABLE_LEAF_CELL_ASSEMBLY_CALLS.store(0, Ordering::Relaxed);
    BTREE_TABLE_LEAF_CELL_ASSEMBLY_BYTES.store(0, Ordering::Relaxed);
    BTREE_INDEX_LEAF_CELL_ASSEMBLY_CALLS.store(0, Ordering::Relaxed);
    BTREE_INDEX_LEAF_CELL_ASSEMBLY_BYTES.store(0, Ordering::Relaxed);
    BTREE_INTERIOR_CELL_REBUILD_CALLS.store(0, Ordering::Relaxed);
    BTREE_INTERIOR_CELL_REBUILD_BYTES.store(0, Ordering::Relaxed);
}

pub fn reset_btree_leaf_reuse_profile() {
    BTREE_NO_SPLIT_REUSE_HITS.store(0, Ordering::Relaxed);
    BTREE_CONSERVATIVE_RELOAD_FALLBACKS.store(0, Ordering::Relaxed);
    BTREE_PAGE_HEADER_REBUILD_COUNT.store(0, Ordering::Relaxed);
    BTREE_FAST_TABLE_LEAF_PAYLOAD_APPEND_CALLS.store(0, Ordering::Relaxed);
    BTREE_FAST_TABLE_LEAF_PAYLOAD_MUTATE_TIME_NS.store(0, Ordering::Relaxed);
    BTREE_FAST_TABLE_LEAF_PAYLOAD_STAGE_TIME_NS.store(0, Ordering::Relaxed);
    BTREE_FAST_TABLE_LEAF_FULL_CELL_APPEND_CALLS.store(0, Ordering::Relaxed);
    BTREE_FAST_TABLE_LEAF_FULL_CELL_MUTATE_TIME_NS.store(0, Ordering::Relaxed);
    BTREE_FAST_TABLE_LEAF_FULL_CELL_STAGE_TIME_NS.store(0, Ordering::Relaxed);
    BTREE_QUICK_BALANCE_ATTEMPTS.store(0, Ordering::Relaxed);
    BTREE_QUICK_BALANCE_HITS.store(0, Ordering::Relaxed);
    BTREE_QUICK_BALANCE_TIME_NS.store(0, Ordering::Relaxed);
    BTREE_LOCAL_SPLIT_ATTEMPTS.store(0, Ordering::Relaxed);
    BTREE_LOCAL_SPLIT_HITS.store(0, Ordering::Relaxed);
    BTREE_LOCAL_SPLIT_TIME_NS.store(0, Ordering::Relaxed);
    BTREE_NONROOT_BALANCE_CALLS.store(0, Ordering::Relaxed);
    BTREE_NONROOT_BALANCE_TIME_NS.store(0, Ordering::Relaxed);
    BTREE_BALANCE_FOR_DELETE_CALLS.store(0, Ordering::Relaxed);
    BTREE_DELETE_LEAF_RUN_MATERIALIZE_CALLS.store(0, Ordering::Relaxed);
    BTREE_DELETE_LEAF_RUN_MATERIALIZE_TIME_NS.store(0, Ordering::Relaxed);
    BTREE_DELETE_LEAF_RUN_WRITE_CALLS.store(0, Ordering::Relaxed);
    BTREE_DELETE_LEAF_RUN_WRITE_TIME_NS.store(0, Ordering::Relaxed);
    BTREE_DELETE_LEAF_RUN_SEARCH_CALLS.store(0, Ordering::Relaxed);
    BTREE_DELETE_LEAF_RUN_SEARCH_TIME_NS.store(0, Ordering::Relaxed);
    BTREE_DELETE_LEAF_RUN_DUPLICATE_CHECK_CALLS.store(0, Ordering::Relaxed);
    BTREE_DELETE_LEAF_RUN_DUPLICATE_CHECK_TIME_NS.store(0, Ordering::Relaxed);
    BTREE_DELETE_LEAF_RUN_COMPACT_CHECK_CALLS.store(0, Ordering::Relaxed);
    BTREE_DELETE_LEAF_RUN_COMPACT_CHECK_TIME_NS.store(0, Ordering::Relaxed);
    BTREE_DELETE_LEAF_RUN_CELL_PARSE_CALLS.store(0, Ordering::Relaxed);
    BTREE_DELETE_LEAF_RUN_CELL_PARSE_TIME_NS.store(0, Ordering::Relaxed);
    BTREE_BULK_TABLE_GROUPING_CALLS.store(0, Ordering::Relaxed);
    BTREE_BULK_TABLE_GROUPING_TIME_NS.store(0, Ordering::Relaxed);
    BTREE_BULK_TABLE_LEAF_PAGE_BUILD_CALLS.store(0, Ordering::Relaxed);
    BTREE_BULK_TABLE_LEAF_PAGE_BUILD_TIME_NS.store(0, Ordering::Relaxed);
    BTREE_BULK_TABLE_LEAF_PAGE_WRITE_CALLS.store(0, Ordering::Relaxed);
    BTREE_BULK_TABLE_LEAF_PAGE_WRITE_TIME_NS.store(0, Ordering::Relaxed);
    BTREE_BULK_TABLE_INTERIOR_PAGE_BUILD_CALLS.store(0, Ordering::Relaxed);
    BTREE_BULK_TABLE_INTERIOR_PAGE_BUILD_TIME_NS.store(0, Ordering::Relaxed);
    BTREE_BULK_TABLE_INTERIOR_PAGE_WRITE_CALLS.store(0, Ordering::Relaxed);
    BTREE_BULK_TABLE_INTERIOR_PAGE_WRITE_TIME_NS.store(0, Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) static LEAF_REUSE_TEST_LOCK: std::sync::LazyLock<std::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(()));

/// Serializes any test that flips `FSQLITE_BTREE_METRICS_ENABLED`.
///
/// The gate is process-global, so two parallel tests that flip it would race
/// — one disabling the gate mid-run of another, causing the latter's expected
/// counter advances to be lost. All tests that depend on a specific
/// gate-on/gate-off state must hold this mutex for the full enable→work→read
/// span.
#[cfg(test)]
pub(crate) static BTREE_METRICS_TEST_LOCK: std::sync::LazyLock<std::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(()));

#[cfg(test)]
pub(crate) static CONFLICT_TOPOLOGY_POLICY_TEST_LOCK: std::sync::LazyLock<std::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(()));

#[cfg(test)]
mod tests {
    use super::{
        BtreeOpType, ConflictTopologyPolicyMode, adaptive_fill_factor_enabled,
        adaptive_fill_factor_policy_id, adaptive_fill_factor_target, btree_copy_profile_snapshot,
        btree_leaf_reuse_snapshot, btree_metrics_snapshot, conflict_topology_split_advice,
        record_conflict_topology_heat, record_conservative_reload_fallback,
        record_no_split_reuse_hit, record_operation, record_page_header_rebuild,
        reset_btree_copy_profile, reset_btree_metrics, reset_conflict_topology_policy_state,
        set_adaptive_fill_factor_enabled, set_btree_copy_profile_enabled,
        set_btree_metrics_enabled, set_conflict_topology_policy_mode,
    };
    use crate::{BtCursor, BtreeCursorOps, MemPageStore};
    use fsqlite_types::PageNumber;
    use fsqlite_types::cx::Cx;
    use std::collections::BTreeSet;
    use std::sync::{LazyLock, Mutex};

    const TEST_USABLE: u32 = 4096;
    static COPY_PROFILE_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    #[test]
    fn adaptive_fill_factor_disabled_is_a_noop() {
        let _guard = super::CONFLICT_TOPOLOGY_POLICY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        set_adaptive_fill_factor_enabled(false);

        // Disabled: every side/heat returns the topology target verbatim, so the
        // baseline/topology split policy is byte-identical to pre-adaptive code.
        for heat in [0_u64, 2, 3, 32, 64, 4096] {
            assert_eq!(
                adaptive_fill_factor_target("right_edge", 6_500, heat),
                6_500
            );
            assert_eq!(adaptive_fill_factor_target("left_edge", 4_500, heat), 4_500);
            assert_eq!(adaptive_fill_factor_target("interior", 5_000, heat), 5_000);
        }

        set_adaptive_fill_factor_enabled(false);
    }

    #[test]
    fn adaptive_fill_factor_ramps_monotonically_within_bounds() {
        let _guard = super::CONFLICT_TOPOLOGY_POLICY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        set_adaptive_fill_factor_enabled(true);

        // At/below the knee heat the ramp adds nothing (matches the flat topology shift).
        assert_eq!(adaptive_fill_factor_target("right_edge", 8_000, 0), 8_000);
        assert_eq!(adaptive_fill_factor_target("right_edge", 8_000, 2), 8_000);

        // Right-edge: extra shift grows with heat, monotonically, clamped to the ceiling.
        let mut prev = 8_000;
        for heat in [3_u64, 16, 33, 50, 64] {
            let target = adaptive_fill_factor_target("right_edge", 8_000, heat);
            assert!(
                target >= prev,
                "right-edge target must be monotonic in heat"
            );
            assert!(
                target <= 9_000,
                "right-edge target must respect the ceiling"
            );
            prev = target;
        }
        // Saturation heat yields the full extra shift, then clamps flat above it.
        assert_eq!(adaptive_fill_factor_target("right_edge", 8_000, 64), 9_000);
        assert_eq!(
            adaptive_fill_factor_target("right_edge", 8_000, 10_000),
            9_000
        );

        // Left-edge: target descends with heat, clamped to the floor.
        let mut prev = 4_500;
        for heat in [3_u64, 16, 33, 50, 64] {
            let target = adaptive_fill_factor_target("left_edge", 4_500, heat);
            assert!(target <= prev, "left-edge target must be monotonic in heat");
            assert!(target >= 1_500, "left-edge target must respect the floor");
            prev = target;
        }
        assert_eq!(adaptive_fill_factor_target("left_edge", 4_500, 64), 3_000);

        // Interior carries no directional bias regardless of heat.
        assert_eq!(adaptive_fill_factor_target("interior", 5_000, 64), 5_000);

        set_adaptive_fill_factor_enabled(false);
    }

    #[test]
    fn adaptive_fill_factor_extra_shift_is_bounded_and_proportional() {
        // Pure ramp function: zero at/below the knee, full at saturation, clamped above.
        assert_eq!(super::adaptive_fill_factor_extra_shift_bps(0), 0);
        assert_eq!(super::adaptive_fill_factor_extra_shift_bps(2), 0);
        assert!(super::adaptive_fill_factor_extra_shift_bps(3) > 0);
        assert_eq!(super::adaptive_fill_factor_extra_shift_bps(64), 1_500);
        assert_eq!(super::adaptive_fill_factor_extra_shift_bps(u64::MAX), 1_500);
        // Midpoint heat lands near half the maximum extra shift.
        let mid = super::adaptive_fill_factor_extra_shift_bps(33);
        assert!((720..=780).contains(&mid), "midpoint extra shift was {mid}");
    }

    #[test]
    fn adaptive_fill_factor_kill_switch_reverts_to_baseline() {
        let _guard = super::CONFLICT_TOPOLOGY_POLICY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        set_adaptive_fill_factor_enabled(true);
        assert!(adaptive_fill_factor_enabled());
        let hot = adaptive_fill_factor_target("right_edge", 8_000, 64);
        assert_eq!(hot, 9_000);

        // Operator override flips it off: behavior reverts to the topology target.
        set_adaptive_fill_factor_enabled(false);
        assert!(!adaptive_fill_factor_enabled());
        assert_eq!(adaptive_fill_factor_target("right_edge", 8_000, 64), 8_000);

        assert_eq!(
            adaptive_fill_factor_policy_id(),
            "btree.adaptive_fill_factor.v1"
        );
    }

    #[test]
    fn adaptive_fill_factor_env_flag_parses_operator_labels() {
        // Affirmative operator labels enable adaptive control (case- and
        // whitespace-insensitive).
        for raw in ["on", "ON", "true", "True", "1", "enforced", "  enforced  "] {
            assert_eq!(
                super::parse_adaptive_fill_factor_flag(raw),
                Some(true),
                "{raw:?} should enable"
            );
        }
        // Kill-switch / negative labels disable it.
        for raw in [
            "off",
            "OFF",
            "false",
            "False",
            "0",
            "baseline",
            "  baseline  ",
        ] {
            assert_eq!(
                super::parse_adaptive_fill_factor_flag(raw),
                Some(false),
                "{raw:?} should disable"
            );
        }
        // Unrecognized values are ignored (None => leave the current setting untouched),
        // including the topology mode's "2"/"advisory"/"shadow" which are not flags here.
        for raw in ["", "yes", "2", "advisory", "shadow", "maybe", "enable"] {
            assert_eq!(
                super::parse_adaptive_fill_factor_flag(raw),
                None,
                "{raw:?} should be ignored"
            );
        }
    }

    #[test]
    fn hot_page_deflection_status_labels_and_predicates() {
        // as_str / is_active / is_applied are exercised via the split-advice
        // wrapper, but the enum's own per-variant contract was never directly
        // asserted.
        use crate::HotPageDeflectionStatus::{
            AdvisoryOnly, Applied, BudgetExhausted, Inactive, OperatorOverride,
        };

        assert_eq!(Inactive.as_str(), "inactive");
        assert_eq!(AdvisoryOnly.as_str(), "advisory_only");
        assert_eq!(Applied.as_str(), "applied");
        assert_eq!(BudgetExhausted.as_str(), "budget_exhausted");
        assert_eq!(OperatorOverride.as_str(), "operator_override");

        // is_active: the three pathological-hotspot states are active; Inactive
        // and OperatorOverride (forced baseline) are not.
        assert!(AdvisoryOnly.is_active() && Applied.is_active() && BudgetExhausted.is_active());
        assert!(!Inactive.is_active() && !OperatorOverride.is_active());

        // is_applied: only Applied consumed a bounded deflection credit.
        assert!(Applied.is_applied());
        for s in [Inactive, AdvisoryOnly, BudgetExhausted, OperatorOverride] {
            assert!(!s.is_applied());
        }
    }

    #[test]
    fn btree_op_type_as_str_labels() {
        // Metric-dimension labels for fsqlite_btree_operations_total; the enum's
        // as_str was never directly asserted.
        use crate::BtreeOpType::{Delete, Insert, Seek};
        assert_eq!(Seek.as_str(), "seek");
        assert_eq!(Insert.as_str(), "insert");
        assert_eq!(Delete.as_str(), "delete");
    }

    #[test]
    fn conflict_topology_policy_mode_labels_and_raw_round_trip() {
        // as_str labels (pub) plus the to_raw/from_raw codec that stashes the
        // mode in a u64 atomic. as_str was never directly asserted, and
        // from_raw's out-of-range catch-all (-> Enforced) guards a corrupt
        // stored value.
        use crate::ConflictTopologyPolicyMode::{Advisory, Baseline, Enforced};

        assert_eq!(Baseline.as_str(), "baseline");
        assert_eq!(Advisory.as_str(), "advisory");
        assert_eq!(Enforced.as_str(), "enforced");

        // Exact raw codes and a clean round-trip for every variant.
        assert_eq!(Baseline.to_raw(), 0);
        assert_eq!(Advisory.to_raw(), 1);
        assert_eq!(Enforced.to_raw(), 2);
        for mode in [Baseline, Advisory, Enforced] {
            assert_eq!(ConflictTopologyPolicyMode::from_raw(mode.to_raw()), mode);
        }

        // from_raw maps any out-of-range value to Enforced (defensive default).
        assert_eq!(ConflictTopologyPolicyMode::from_raw(99), Enforced);
    }

    #[test]
    fn conflict_topology_advice_applies_hot_right_edge_fill_shift() {
        let _guard = super::CONFLICT_TOPOLOGY_POLICY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let page = PageNumber::new(42).expect("page");
        set_conflict_topology_policy_mode(ConflictTopologyPolicyMode::Enforced);
        reset_conflict_topology_policy_state();

        record_conflict_topology_heat(page, 2, 3);
        let advice = conflict_topology_split_advice(page, "right_edge", 6_500);

        assert!(advice.topology_hot);
        assert!(advice.applied);
        assert_eq!(advice.policy_mode, ConflictTopologyPolicyMode::Enforced);
        assert_eq!(advice.effective_target_left_basis_points, 8_000);
        assert_eq!(advice.placement_policy(), "topology_aware_fill_factor");
        assert_eq!(advice.predicted_overlap_delta, 3);

        reset_conflict_topology_policy_state();
        set_conflict_topology_policy_mode(ConflictTopologyPolicyMode::Enforced);
    }

    #[test]
    fn conflict_topology_baseline_mode_is_kill_switch() {
        let _guard = super::CONFLICT_TOPOLOGY_POLICY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let page = PageNumber::new(43).expect("page");
        set_conflict_topology_policy_mode(ConflictTopologyPolicyMode::Enforced);
        reset_conflict_topology_policy_state();
        record_conflict_topology_heat(page, 10, 8);

        set_conflict_topology_policy_mode(ConflictTopologyPolicyMode::Baseline);
        let advice = conflict_topology_split_advice(page, "right_edge", 6_500);

        assert_eq!(advice.policy_mode, ConflictTopologyPolicyMode::Baseline);
        assert!(!advice.applied);
        assert_eq!(advice.effective_target_left_basis_points, 6_500);
        assert!(advice.operator_override_active);

        reset_conflict_topology_policy_state();
        set_conflict_topology_policy_mode(ConflictTopologyPolicyMode::Enforced);
    }

    #[test]
    fn hot_page_deflection_synthetic_pathological_hotspot_reduces_projected_overlap() {
        let _guard = super::CONFLICT_TOPOLOGY_POLICY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let page = PageNumber::new(45).expect("page");
        set_conflict_topology_policy_mode(ConflictTopologyPolicyMode::Enforced);
        reset_conflict_topology_policy_state();

        for _ in 0..64 {
            record_conflict_topology_heat(page, 1, 4);
        }

        let first = conflict_topology_split_advice(page, "right_edge", 6_500);
        assert!(first.topology_hot);
        assert!(first.deflection_active());
        assert!(first.deflection_applied());
        assert_eq!(first.deflection_credits_before, 2);
        assert_eq!(first.deflection_credits_after, 1);
        assert_eq!(first.effective_target_left_basis_points, 9_000);
        assert_eq!(first.trigger_reason, "pathological_hot_page");
        assert_eq!(first.migration_outcome, "split_deflected");
        assert_eq!(first.rollback_reason, "none");
        assert_eq!(first.budget_pages, 2);
        assert_eq!(first.budget_ns, 0);
        assert!(first.heat_after < first.heat_before);
        assert_eq!(first.predicted_overlap_delta, 8);

        let second = conflict_topology_split_advice(page, "right_edge", 6_500);
        assert!(second.deflection_applied());
        assert_eq!(second.deflection_credits_before, 1);
        assert_eq!(second.deflection_credits_after, 0);
        assert_eq!(second.effective_target_left_basis_points, 9_000);

        let exhausted = conflict_topology_split_advice(page, "right_edge", 6_500);
        assert!(exhausted.deflection_active());
        assert!(!exhausted.deflection_applied());
        assert_eq!(exhausted.deflection_credits_before, 0);
        assert_eq!(exhausted.deflection_credits_after, 0);
        assert_eq!(exhausted.effective_target_left_basis_points, 8_000);
        assert_eq!(
            exhausted.trigger_reason,
            "pathological_hot_page_budget_exhausted"
        );
        assert_eq!(exhausted.migration_outcome, "budget_exhausted");
        assert_eq!(exhausted.rollback_reason, "budget_exhausted");

        reset_conflict_topology_policy_state();
        set_conflict_topology_policy_mode(ConflictTopologyPolicyMode::Enforced);
    }

    #[test]
    fn hot_page_deflection_baseline_mode_is_reversible_kill_switch() {
        let _guard = super::CONFLICT_TOPOLOGY_POLICY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let page = PageNumber::new(46).expect("page");
        set_conflict_topology_policy_mode(ConflictTopologyPolicyMode::Enforced);
        reset_conflict_topology_policy_state();

        for _ in 0..64 {
            record_conflict_topology_heat(page, 1, 4);
        }

        set_conflict_topology_policy_mode(ConflictTopologyPolicyMode::Baseline);
        let advice = conflict_topology_split_advice(page, "right_edge", 6_500);

        assert!(!advice.applied);
        assert!(!advice.deflection_active());
        assert_eq!(advice.effective_target_left_basis_points, 6_500);
        assert!(advice.operator_override_active);
        assert_eq!(advice.trigger_reason, "operator_override_baseline");
        assert_eq!(advice.migration_outcome, "operator_override_baseline");
        assert_eq!(advice.rollback_reason, "operator_override_baseline");

        reset_conflict_topology_policy_state();
        set_conflict_topology_policy_mode(ConflictTopologyPolicyMode::Enforced);
    }

    fn certification_log_fields(
        scenario_id: &'static str,
        conflict_topology_class: &'static str,
        advice: super::ConflictTopologySplitAdvice,
        throughput_rows_per_s: u64,
        latency_p95_ns: u64,
    ) -> [(&'static str, String); 12] {
        [
            ("trace_id", "bd-1dp9.6.7.13.4-cert".to_owned()),
            ("run_id", "bd-1dp9.6.7.13.4-static-replay".to_owned()),
            ("scenario_id", scenario_id.to_owned()),
            (
                "conflict_topology_class",
                conflict_topology_class.to_owned(),
            ),
            ("policy_mode", advice.policy_mode.as_str().to_owned()),
            ("backend_identity", "file_backed_mvcc".to_owned()),
            ("abort_rate", "0".to_owned()),
            ("latency_p95_ns", latency_p95_ns.to_string()),
            ("throughput_rows_per_s", throughput_rows_per_s.to_string()),
            ("semantic_diff_status", "no_divergence".to_owned()),
            ("artifact_hash", "0".repeat(64)),
            ("first_failure_diag", "none".to_owned()),
        ]
    }

    fn assert_certification_log_fields_complete(fields: &[(&str, String)]) {
        let names = fields
            .iter()
            .map(|(field_name, _value)| *field_name)
            .collect::<BTreeSet<_>>();
        for required in [
            "trace_id",
            "run_id",
            "scenario_id",
            "conflict_topology_class",
            "policy_mode",
            "backend_identity",
            "abort_rate",
            "latency_p95_ns",
            "throughput_rows_per_s",
            "semantic_diff_status",
            "artifact_hash",
            "first_failure_diag",
        ] {
            assert!(names.contains(required), "missing field {required}");
        }

        let artifact_hash = fields
            .iter()
            .find_map(|(field_name, value)| (*field_name == "artifact_hash").then_some(value))
            .expect("artifact hash field");
        assert_eq!(artifact_hash.len(), 64);
        assert!(
            artifact_hash.chars().all(|ch| ch.is_ascii_hexdigit()),
            "artifact hash must be hex"
        );
    }

    #[test]
    fn conflict_topology_certification_matrix_covers_rollout_and_hotspot_states() {
        let _guard = super::CONFLICT_TOPOLOGY_POLICY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let page = PageNumber::new(47).expect("page");

        set_conflict_topology_policy_mode(ConflictTopologyPolicyMode::Enforced);
        reset_conflict_topology_policy_state();
        let cold = conflict_topology_split_advice(page, "right_edge", 6_500);
        assert!(!cold.topology_hot);
        assert!(!cold.applied);
        assert_eq!(cold.effective_target_left_basis_points, 6_500);
        assert_eq!(
            cold.deflection_status,
            super::HotPageDeflectionStatus::Inactive
        );

        record_conflict_topology_heat(page, 2, 2);
        let topology_hot = conflict_topology_split_advice(page, "right_edge", 6_500);
        assert!(topology_hot.topology_hot);
        assert!(topology_hot.applied);
        assert_eq!(topology_hot.effective_target_left_basis_points, 8_000);
        assert_eq!(
            topology_hot.deflection_status,
            super::HotPageDeflectionStatus::Inactive
        );

        reset_conflict_topology_policy_state();
        for _ in 0..64 {
            record_conflict_topology_heat(page, 1, 4);
        }
        set_conflict_topology_policy_mode(ConflictTopologyPolicyMode::Advisory);
        let advisory = conflict_topology_split_advice(page, "right_edge", 6_500);
        assert!(advisory.deflection_active());
        assert!(!advisory.applied);
        assert_eq!(advisory.effective_target_left_basis_points, 6_500);
        assert_eq!(advisory.advised_target_left_basis_points, 9_000);
        assert_eq!(
            advisory.deflection_status,
            super::HotPageDeflectionStatus::AdvisoryOnly
        );

        set_conflict_topology_policy_mode(ConflictTopologyPolicyMode::Baseline);
        let baseline = conflict_topology_split_advice(page, "right_edge", 6_500);
        assert!(baseline.operator_override_active);
        assert_eq!(baseline.effective_target_left_basis_points, 6_500);
        assert_eq!(
            baseline.deflection_status,
            super::HotPageDeflectionStatus::OperatorOverride
        );

        reset_conflict_topology_policy_state();
        set_conflict_topology_policy_mode(ConflictTopologyPolicyMode::Enforced);
        for _ in 0..64 {
            record_conflict_topology_heat(page, 1, 4);
        }
        let first = conflict_topology_split_advice(page, "right_edge", 6_500);
        let second = conflict_topology_split_advice(page, "right_edge", 6_500);
        let exhausted = conflict_topology_split_advice(page, "right_edge", 6_500);
        assert!(first.deflection_applied());
        assert_eq!(first.effective_target_left_basis_points, 9_000);
        assert!(second.deflection_applied());
        assert_eq!(second.deflection_credits_after, 0);
        assert_eq!(
            exhausted.deflection_status,
            super::HotPageDeflectionStatus::BudgetExhausted
        );
        assert_eq!(exhausted.effective_target_left_basis_points, 8_000);

        for fields in [
            certification_log_fields("cold-shared-table", "cold_page", cold, 120_000, 9_000_000),
            certification_log_fields(
                "topology-hot-shared-table",
                "topology_hot_page",
                topology_hot,
                140_000,
                8_000_000,
            ),
            certification_log_fields(
                "advisory-pathological-hot-page",
                "pathological_hot_page",
                advisory,
                140_000,
                8_000_000,
            ),
            certification_log_fields(
                "baseline-operator-override",
                "operator_override",
                baseline,
                120_000,
                9_000_000,
            ),
            certification_log_fields(
                "enforced-bounded-deflection",
                "pathological_hot_page",
                first,
                150_000,
                7_000_000,
            ),
            certification_log_fields(
                "budget-exhausted-fallback",
                "budget_exhausted",
                exhausted,
                130_000,
                8_000_000,
            ),
        ] {
            assert_certification_log_fields_complete(&fields);
        }

        reset_conflict_topology_policy_state();
        set_conflict_topology_policy_mode(ConflictTopologyPolicyMode::Enforced);
    }

    #[test]
    fn conflict_topology_policy_parse_accepts_operator_labels() {
        assert_eq!(
            super::parse_conflict_topology_policy_mode("baseline"),
            Some(ConflictTopologyPolicyMode::Baseline)
        );
        assert_eq!(
            super::parse_conflict_topology_policy_mode("shadow"),
            Some(ConflictTopologyPolicyMode::Advisory)
        );
        assert_eq!(
            super::parse_conflict_topology_policy_mode("true"),
            Some(ConflictTopologyPolicyMode::Enforced)
        );
        assert_eq!(super::parse_conflict_topology_policy_mode("bogus"), None);
    }

    #[test]
    fn metrics_snapshot_tracks_operation_buckets() {
        let _gate_guard = super::BTREE_METRICS_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        set_btree_metrics_enabled(true);
        let before = btree_metrics_snapshot();
        record_operation(BtreeOpType::Seek);
        record_operation(BtreeOpType::Seek);
        record_operation(BtreeOpType::Insert);

        let after = btree_metrics_snapshot();
        set_btree_metrics_enabled(false);
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

    #[test]
    fn copy_profile_tracks_owned_materialization_and_cell_assembly() {
        let _guard = COPY_PROFILE_TEST_LOCK
            .lock()
            .expect("copy-profile test lock");
        reset_btree_metrics();
        reset_btree_copy_profile();
        set_btree_copy_profile_enabled(true);

        let before = btree_copy_profile_snapshot();
        let cx = Cx::new();
        let root = PageNumber::new(2).expect("root page");
        let store = MemPageStore::with_empty_table(root, TEST_USABLE);
        let mut cursor = BtCursor::new(store, root, TEST_USABLE, true);

        cursor
            .table_insert(&cx, 1, b"copy-kernel-row")
            .expect("insert should succeed");
        assert!(
            cursor
                .table_move_to(&cx, 1)
                .expect("seek should succeed")
                .is_found()
        );
        let payload = cursor.payload(&cx).expect("payload should decode");
        assert_eq!(payload, b"copy-kernel-row");

        let after = btree_copy_profile_snapshot();
        set_btree_copy_profile_enabled(false);

        assert!(
            after.table_leaf_cell_assembly_calls
                >= before.table_leaf_cell_assembly_calls.saturating_add(1)
        );
        assert!(
            after.table_leaf_cell_assembly_bytes
                >= before
                    .table_leaf_cell_assembly_bytes
                    .saturating_add(payload.len() as u64)
        );
        assert!(
            after.owned_payload_materialization_calls
                >= before.owned_payload_materialization_calls.saturating_add(1)
        );
        assert!(
            after.owned_payload_materialization_bytes
                >= before
                    .owned_payload_materialization_bytes
                    .saturating_add(payload.len() as u64)
        );
    }

    #[test]
    fn copy_profile_tracks_overflow_reassembly() {
        let _guard = COPY_PROFILE_TEST_LOCK
            .lock()
            .expect("copy-profile test lock");
        reset_btree_copy_profile();
        set_btree_copy_profile_enabled(true);

        let before = btree_copy_profile_snapshot();
        let cx = Cx::new();
        let root = PageNumber::new(2).expect("root page");
        let store = MemPageStore::with_empty_table(root, TEST_USABLE);
        let mut cursor = BtCursor::new(store, root, TEST_USABLE, true);
        let payload = vec![b'X'; 8_000];

        cursor
            .table_insert(&cx, 1, &payload)
            .expect("overflow insert should succeed");
        assert!(
            cursor
                .table_move_to(&cx, 1)
                .expect("seek should succeed")
                .is_found()
        );
        let mut scratch = Vec::new();
        cursor
            .payload_into(&cx, &mut scratch)
            .expect("payload_into should decode overflow");
        set_btree_copy_profile_enabled(false);

        let after = btree_copy_profile_snapshot();
        assert_eq!(scratch, payload);
        assert!(
            after.overflow_chain_reassembly_calls
                >= before.overflow_chain_reassembly_calls.saturating_add(1)
        );
        assert!(after.overflow_chain_overflow_bytes > before.overflow_chain_overflow_bytes);
        assert!(after.overflow_page_reads > before.overflow_page_reads);
    }

    #[test]
    fn leaf_reuse_profile_tracks_reuse_fallback_and_rebuilds() {
        let _guard = super::LEAF_REUSE_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _gate_guard = super::BTREE_METRICS_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        set_btree_metrics_enabled(true);
        let before = btree_leaf_reuse_snapshot();

        record_no_split_reuse_hit();
        record_conservative_reload_fallback();
        record_page_header_rebuild();

        let after = btree_leaf_reuse_snapshot();
        set_btree_metrics_enabled(false);
        assert!(
            after.no_split_reuse_hits >= before.no_split_reuse_hits.saturating_add(1),
            "no-split reuse counter should advance"
        );
        assert!(
            after.conservative_reload_fallbacks
                >= before.conservative_reload_fallbacks.saturating_add(1),
            "fallback counter should advance"
        );
        assert!(
            after.page_header_rebuild_count >= before.page_header_rebuild_count.saturating_add(1),
            "page-header rebuild counter should advance"
        );
    }

    /// Microbench: gate-off vs gate-on cost of `record_operation` and
    /// `set_depth_gauge` — the two recorders that fire on every cursor op.
    ///
    /// Cross-core contention isn't exercised here (single thread), but the
    /// per-call atomic fetch_add / store still costs ~5–10 ns even when the
    /// counter isn't contended. With the gate off we expect a single
    /// `AtomicBool::load(Relaxed)` per call (~1 ns).
    ///
    /// Run via:
    /// ```text
    /// cargo test -p fsqlite-btree --lib --release -- --ignored --nocapture \
    ///   bench_btree_metrics_gate_per_op_cost
    /// ```
    /// Wrapper that prevents LLVM from hoisting the gate-load out of the
    /// bench loop or folding away the body. `record_operation` is `#[inline]`
    /// — without an inline boundary here the compiler sees a constant-folded
    /// loop body and elides the work entirely.
    #[inline(never)]
    fn bench_record_op_pair(op: BtreeOpType, depth: usize) {
        record_operation(op);
        super::set_depth_gauge(depth);
    }

    #[test]
    #[ignore = "microbench — run with --ignored --nocapture"]
    fn bench_btree_metrics_gate_per_op_cost() {
        use std::hint::black_box;
        use std::time::Instant;

        let _gate_guard = super::BTREE_METRICS_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        const ITERATIONS: u64 = 50_000_000;

        // Gate-off path (default).
        set_btree_metrics_enabled(false);
        for _ in 0..100_000 {
            bench_record_op_pair(black_box(BtreeOpType::Seek), black_box(3));
        }
        let off_start = Instant::now();
        for _ in 0..ITERATIONS {
            bench_record_op_pair(black_box(BtreeOpType::Seek), black_box(3));
        }
        let off_elapsed = off_start.elapsed();

        // Gate-on path.
        set_btree_metrics_enabled(true);
        for _ in 0..100_000 {
            bench_record_op_pair(black_box(BtreeOpType::Seek), black_box(3));
        }
        let on_start = Instant::now();
        for _ in 0..ITERATIONS {
            bench_record_op_pair(black_box(BtreeOpType::Seek), black_box(3));
        }
        let on_elapsed = on_start.elapsed();
        set_btree_metrics_enabled(false);

        let off_ns = off_elapsed.as_nanos() as f64 / ITERATIONS as f64;
        let on_ns = on_elapsed.as_nanos() as f64 / ITERATIONS as f64;
        println!(
            "btree-metrics gate per-op (record_operation + set_depth_gauge): \
             gate-off={off_ns:.2} ns/pair  gate-on={on_ns:.2} ns/pair  iterations={ITERATIONS}"
        );
        // Gate-off must be no slower than gate-on. An `AtomicBool::load(Relaxed)`
        // is ~1 ns while the gated `fetch_add` + `store` cost ~5–10 ns even
        // uncontended (and far more under multi-thread cache-line ping-pong).
        assert!(
            off_ns <= on_ns,
            "gate-off ({off_ns:.2} ns) should not exceed gate-on ({on_ns:.2} ns)"
        );
    }
}
