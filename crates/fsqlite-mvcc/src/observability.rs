//! MVCC observability integration.
//!
//! This module wires the `fsqlite-observability` event types into the MVCC
//! layer. It provides helper functions that emit conflict events through
//! both `tracing` (for structured logging) and an optional observer callback
//! (for programmatic access via PRAGMAs).
//!
//! **Invariant:** All functions in this module are non-blocking. They must
//! never acquire page locks or block writers.

use fsqlite_types::sync_primitives::{Instant, Mutex};
#[cfg(test)]
use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use fsqlite_observability::{ConflictEvent, ConflictObserver, SsiAbortCategory};
use fsqlite_types::{CommitSeq, PageNumber, TxnId, TxnToken};

/// Optional observer handle. When `None`, no callback overhead.
pub type SharedObserver = Option<Arc<dyn ConflictObserver>>;

/// Histogram buckets for `fsqlite_mvcc_versions_traversed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct VersionsTraversedHistogram {
    pub le_1: u64,
    pub le_2: u64,
    pub le_4: u64,
    pub le_8: u64,
    pub le_16: u64,
    pub gt_16: u64,
}

/// Snapshot of MVCC snapshot-read metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SnapshotReadMetricsSnapshot {
    /// Histogram of versions traversed during snapshot reads.
    pub fsqlite_mvcc_versions_traversed: VersionsTraversedHistogram,
    /// Number of recorded snapshot-read samples.
    pub versions_traversed_samples: u64,
    /// Sum of traversed-version counts across samples.
    pub versions_traversed_sum: u64,
    /// Gauge of active snapshot-bearing transactions.
    pub fsqlite_mvcc_active_snapshots: u64,
}

static MVCC_VERSIONS_TRAVERSED_LE_1: AtomicU64 = AtomicU64::new(0);
static MVCC_VERSIONS_TRAVERSED_LE_2: AtomicU64 = AtomicU64::new(0);
static MVCC_VERSIONS_TRAVERSED_LE_4: AtomicU64 = AtomicU64::new(0);
static MVCC_VERSIONS_TRAVERSED_LE_8: AtomicU64 = AtomicU64::new(0);
static MVCC_VERSIONS_TRAVERSED_LE_16: AtomicU64 = AtomicU64::new(0);
static MVCC_VERSIONS_TRAVERSED_GT_16: AtomicU64 = AtomicU64::new(0);
static MVCC_VERSIONS_TRAVERSED_SAMPLES: AtomicU64 = AtomicU64::new(0);
static MVCC_VERSIONS_TRAVERSED_SUM: AtomicU64 = AtomicU64::new(0);
static MVCC_ACTIVE_SNAPSHOTS: AtomicU64 = AtomicU64::new(0);

/// Gate for the snapshot-read observability histogram. When `false` (the
/// default) every `record_snapshot_read_versions_traversed` call returns
/// immediately after a single relaxed atomic-bool load — skipping the three
/// `fetch_add`s the histogram otherwise performs on every MVCC resolve. The
/// runtime metrics probe (PRAGMA / admin tooling) flips this on when it
/// wants to observe the histogram. Matches the `FSQLITE_VDBE_METRICS_ENABLED`
/// pattern in the VDBE engine.
static MVCC_SNAPSHOT_METRICS_ENABLED: AtomicBool = AtomicBool::new(false);

/// Gate for the CAS-attempts observability histogram. `record_cas_attempt`
/// is called from `VersionStore::publish` on every version-chain append
/// and was doing two unconditional relaxed `fetch_add`s regardless of
/// whether anyone ever reads the histogram. Production consumers are
/// diagnostic-only, so the default is off — same pattern as
/// `MVCC_SNAPSHOT_METRICS_ENABLED`.
static MVCC_CAS_METRICS_ENABLED: AtomicBool = AtomicBool::new(false);

/// Enable or disable the MVCC snapshot-read histogram collection.
/// Defaults to disabled so hot-path resolves do not pay three relaxed
/// atomic increments per call.
pub fn set_mvcc_snapshot_metrics_enabled(enabled: bool) {
    MVCC_SNAPSHOT_METRICS_ENABLED.store(enabled, Ordering::Relaxed);
}

/// Current MVCC snapshot-read metrics collection flag.
#[must_use]
pub fn mvcc_snapshot_metrics_enabled() -> bool {
    MVCC_SNAPSHOT_METRICS_ENABLED.load(Ordering::Relaxed)
}

/// Enable or disable the MVCC CAS-attempts histogram collection.
/// Defaults to disabled so hot-path publishes do not pay two relaxed
/// atomic increments per call.
pub fn set_mvcc_cas_metrics_enabled(enabled: bool) {
    MVCC_CAS_METRICS_ENABLED.store(enabled, Ordering::Relaxed);
}

/// Current MVCC CAS-attempts metrics collection flag.
#[must_use]
pub fn mvcc_cas_metrics_enabled() -> bool {
    MVCC_CAS_METRICS_ENABLED.load(Ordering::Relaxed)
}

/// Monotonic nanosecond timestamp relative to process start.
fn now_ns() -> u64 {
    // Use a single, consistent epoch for all events in this process.
    static EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let epoch = EPOCH.get_or_init(Instant::now);
    #[allow(clippy::cast_possible_truncation)] // clamped to u64::MAX
    {
        epoch.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
    }
}

/// Emit to observer if present.
#[inline]
fn emit(observer: &SharedObserver, event: &ConflictEvent) {
    if let Some(obs) = observer {
        obs.on_event(event);
    }
}

/// Record one snapshot-read traversal into the
/// `fsqlite_mvcc_versions_traversed` histogram.
///
/// No-op when `mvcc_snapshot_metrics_enabled()` is false (the default).
/// This is called from every `VersionStore::resolve_visible_version` /
/// `with_visible_version` / `resolve_visible_commit_seq` exit path, so
/// the gate is an important hot-path saver — see
/// `bench_resolve_visible_version_metric_gate`.
#[inline]
pub fn record_snapshot_read_versions_traversed(versions_traversed: u64) {
    if !MVCC_SNAPSHOT_METRICS_ENABLED.load(Ordering::Relaxed) {
        return;
    }
    record_snapshot_read_versions_traversed_slow(versions_traversed);
}

#[cold]
#[inline(never)]
fn record_snapshot_read_versions_traversed_slow(versions_traversed: u64) {
    MVCC_VERSIONS_TRAVERSED_SAMPLES.fetch_add(1, Ordering::Relaxed);
    MVCC_VERSIONS_TRAVERSED_SUM.fetch_add(versions_traversed, Ordering::Relaxed);

    let bucket = match versions_traversed {
        0 | 1 => &MVCC_VERSIONS_TRAVERSED_LE_1,
        2 => &MVCC_VERSIONS_TRAVERSED_LE_2,
        3 | 4 => &MVCC_VERSIONS_TRAVERSED_LE_4,
        5..=8 => &MVCC_VERSIONS_TRAVERSED_LE_8,
        9..=16 => &MVCC_VERSIONS_TRAVERSED_LE_16,
        _ => &MVCC_VERSIONS_TRAVERSED_GT_16,
    };
    bucket.fetch_add(1, Ordering::Relaxed);
}

/// Increment the `fsqlite_mvcc_active_snapshots` gauge.
///
/// No-op when `mvcc_snapshot_metrics_enabled()` is false (the default).
/// Called from every `TransactionManager::begin` that establishes a read
/// snapshot, so the gate keeps the hot path to one relaxed bool load
/// instead of a `fetch_add` on a process-wide contended cache line.
#[inline]
pub fn mvcc_snapshot_established() {
    if !MVCC_SNAPSHOT_METRICS_ENABLED.load(Ordering::Relaxed) {
        return;
    }
    MVCC_ACTIVE_SNAPSHOTS.fetch_add(1, Ordering::Relaxed);
}

/// Decrement the `fsqlite_mvcc_active_snapshots` gauge (saturating at zero).
///
/// No-op when `mvcc_snapshot_metrics_enabled()` is false (the default).
/// Called from every transaction finalization path, so the gate avoids
/// the CAS loop on the commit/rollback hot path.
#[inline]
pub fn mvcc_snapshot_released() {
    if !MVCC_SNAPSHOT_METRICS_ENABLED.load(Ordering::Relaxed) {
        return;
    }
    mvcc_snapshot_released_slow();
}

#[cold]
#[inline(never)]
fn mvcc_snapshot_released_slow() {
    loop {
        let current = MVCC_ACTIVE_SNAPSHOTS.load(Ordering::Relaxed);
        if current == 0 {
            return;
        }
        if MVCC_ACTIVE_SNAPSHOTS
            .compare_exchange_weak(current, current - 1, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return;
        }
    }
}

/// Snapshot MVCC snapshot-read metrics.
#[must_use]
pub fn mvcc_snapshot_metrics_snapshot() -> SnapshotReadMetricsSnapshot {
    SnapshotReadMetricsSnapshot {
        fsqlite_mvcc_versions_traversed: VersionsTraversedHistogram {
            le_1: MVCC_VERSIONS_TRAVERSED_LE_1.load(Ordering::Relaxed),
            le_2: MVCC_VERSIONS_TRAVERSED_LE_2.load(Ordering::Relaxed),
            le_4: MVCC_VERSIONS_TRAVERSED_LE_4.load(Ordering::Relaxed),
            le_8: MVCC_VERSIONS_TRAVERSED_LE_8.load(Ordering::Relaxed),
            le_16: MVCC_VERSIONS_TRAVERSED_LE_16.load(Ordering::Relaxed),
            gt_16: MVCC_VERSIONS_TRAVERSED_GT_16.load(Ordering::Relaxed),
        },
        versions_traversed_samples: MVCC_VERSIONS_TRAVERSED_SAMPLES.load(Ordering::Relaxed),
        versions_traversed_sum: MVCC_VERSIONS_TRAVERSED_SUM.load(Ordering::Relaxed),
        fsqlite_mvcc_active_snapshots: MVCC_ACTIVE_SNAPSHOTS.load(Ordering::Relaxed),
    }
}

/// Reset MVCC snapshot-read metrics.
pub fn reset_mvcc_snapshot_metrics() {
    MVCC_VERSIONS_TRAVERSED_LE_1.store(0, Ordering::Relaxed);
    MVCC_VERSIONS_TRAVERSED_LE_2.store(0, Ordering::Relaxed);
    MVCC_VERSIONS_TRAVERSED_LE_4.store(0, Ordering::Relaxed);
    MVCC_VERSIONS_TRAVERSED_LE_8.store(0, Ordering::Relaxed);
    MVCC_VERSIONS_TRAVERSED_LE_16.store(0, Ordering::Relaxed);
    MVCC_VERSIONS_TRAVERSED_GT_16.store(0, Ordering::Relaxed);
    MVCC_VERSIONS_TRAVERSED_SAMPLES.store(0, Ordering::Relaxed);
    MVCC_VERSIONS_TRAVERSED_SUM.store(0, Ordering::Relaxed);
    MVCC_ACTIVE_SNAPSHOTS.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// CAS Metrics (bd-688.3)
// ---------------------------------------------------------------------------

static FSQLITE_MVCC_CAS_ATTEMPTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static FSQLITE_MVCC_CAS_RETRIES_LE_1: AtomicU64 = AtomicU64::new(0);
static FSQLITE_MVCC_CAS_RETRIES_LE_2: AtomicU64 = AtomicU64::new(0);
static FSQLITE_MVCC_CAS_RETRIES_LE_4: AtomicU64 = AtomicU64::new(0);
static FSQLITE_MVCC_CAS_RETRIES_GT_4: AtomicU64 = AtomicU64::new(0);

/// Histogram buckets for CAS retry counts during chain head installation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
pub struct CasRetriesHistogram {
    pub le_1: u64,
    pub le_2: u64,
    pub le_4: u64,
    pub gt_4: u64,
}

/// Point-in-time snapshot of CAS chain head installation metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
pub struct CasMetricsSnapshot {
    /// Total number of CAS install operations attempted.
    pub attempts_total: u64,
    /// Histogram of CAS attempt counts per install operation.
    pub retries: CasRetriesHistogram,
}

impl CasMetricsSnapshot {
    /// Number of installs that succeeded on the first CAS attempt.
    #[must_use]
    pub fn first_attempt_count(&self) -> u64 {
        self.retries.le_1
    }

    /// Fraction of installs that succeeded on the first attempt.
    ///
    /// Returns `0.0` when no samples have been recorded.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn first_attempt_ratio(&self) -> f64 {
        if self.attempts_total == 0 {
            return 0.0;
        }
        self.first_attempt_count() as f64 / self.attempts_total as f64
    }
}

/// Record one CAS install operation with the given number of CAS attempts.
///
/// No-op when `mvcc_cas_metrics_enabled()` is false (the default). Called
/// from `VersionStore::publish` on every version-chain append; the gate
/// keeps the hot path down to one relaxed bool load instead of two
/// `fetch_add`s. See `bench_publish_visibility_ranges_gate` / the gate
/// pattern landed in bc4fa6b5.
#[inline]
pub fn record_cas_attempt(attempts: u32) {
    if !MVCC_CAS_METRICS_ENABLED.load(Ordering::Relaxed) {
        return;
    }
    record_cas_attempt_slow(attempts);
}

#[cold]
#[inline(never)]
fn record_cas_attempt_slow(attempts: u32) {
    FSQLITE_MVCC_CAS_ATTEMPTS_TOTAL.fetch_add(1, Ordering::Relaxed);
    let bucket = match attempts {
        0 | 1 => &FSQLITE_MVCC_CAS_RETRIES_LE_1,
        2 => &FSQLITE_MVCC_CAS_RETRIES_LE_2,
        3 | 4 => &FSQLITE_MVCC_CAS_RETRIES_LE_4,
        _ => &FSQLITE_MVCC_CAS_RETRIES_GT_4,
    };
    bucket.fetch_add(1, Ordering::Relaxed);
}

/// Take a point-in-time snapshot of CAS metrics.
#[must_use]
pub fn cas_metrics_snapshot() -> CasMetricsSnapshot {
    CasMetricsSnapshot {
        attempts_total: FSQLITE_MVCC_CAS_ATTEMPTS_TOTAL.load(Ordering::Relaxed),
        retries: CasRetriesHistogram {
            le_1: FSQLITE_MVCC_CAS_RETRIES_LE_1.load(Ordering::Relaxed),
            le_2: FSQLITE_MVCC_CAS_RETRIES_LE_2.load(Ordering::Relaxed),
            le_4: FSQLITE_MVCC_CAS_RETRIES_LE_4.load(Ordering::Relaxed),
            gt_4: FSQLITE_MVCC_CAS_RETRIES_GT_4.load(Ordering::Relaxed),
        },
    }
}

/// Reset CAS metrics to zero (tests/diagnostics).
pub fn reset_cas_metrics() {
    FSQLITE_MVCC_CAS_ATTEMPTS_TOTAL.store(0, Ordering::Relaxed);
    FSQLITE_MVCC_CAS_RETRIES_LE_1.store(0, Ordering::Relaxed);
    FSQLITE_MVCC_CAS_RETRIES_LE_2.store(0, Ordering::Relaxed);
    FSQLITE_MVCC_CAS_RETRIES_LE_4.store(0, Ordering::Relaxed);
    FSQLITE_MVCC_CAS_RETRIES_GT_4.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// SSI Metrics (bd-688.2)
// ---------------------------------------------------------------------------

static FSQLITE_SSI_COMMITS_TOTAL: AtomicU64 = AtomicU64::new(0);
static FSQLITE_SSI_ABORTS_PIVOT: AtomicU64 = AtomicU64::new(0);
static FSQLITE_SSI_ABORTS_COMMITTED_PIVOT: AtomicU64 = AtomicU64::new(0);
static FSQLITE_SSI_ABORTS_MARKED_FOR_ABORT: AtomicU64 = AtomicU64::new(0);
// `validations_total` was previously a separate `AtomicU64` incremented on
// every commit/abort. It is now derived at snapshot time as
// `commits_total + aborts_total()` — the two are mathematically identical
// because every SSI validation resolves into exactly one commit or one
// categorised abort. Eliminating the redundant counter halves the atomic
// store traffic on the SSI commit hot path.

/// Record a successful SSI commit.
pub fn record_ssi_commit() {
    FSQLITE_SSI_COMMITS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Record an SSI abort with reason label.
pub fn record_ssi_abort(reason: SsiAbortCategory) {
    let bucket = match reason {
        SsiAbortCategory::Pivot => &FSQLITE_SSI_ABORTS_PIVOT,
        SsiAbortCategory::CommittedPivot => &FSQLITE_SSI_ABORTS_COMMITTED_PIVOT,
        SsiAbortCategory::MarkedForAbort => &FSQLITE_SSI_ABORTS_MARKED_FOR_ABORT,
    };
    bucket.fetch_add(1, Ordering::Relaxed);
}

/// Point-in-time snapshot of SSI metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SsiMetricsSnapshot {
    pub commits_total: u64,
    pub aborts_pivot: u64,
    pub aborts_committed_pivot: u64,
    pub aborts_marked_for_abort: u64,
    pub validations_total: u64,
}

impl SsiMetricsSnapshot {
    /// Total SSI aborts across all reasons.
    #[must_use]
    pub fn aborts_total(&self) -> u64 {
        self.aborts_pivot + self.aborts_committed_pivot + self.aborts_marked_for_abort
    }

    /// SSI conflict rate as aborts / validations.  Returns 0.0 if no
    /// validations have occurred.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn conflict_rate(&self) -> f64 {
        if self.validations_total == 0 {
            return 0.0;
        }
        self.aborts_total() as f64 / self.validations_total as f64
    }
}

impl std::fmt::Display for SsiMetricsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ssi: {} commits, {} aborts (pivot={}, committed_pivot={}, marked={}), rate={:.4}",
            self.commits_total,
            self.aborts_total(),
            self.aborts_pivot,
            self.aborts_committed_pivot,
            self.aborts_marked_for_abort,
            self.conflict_rate(),
        )
    }
}

/// Take a point-in-time snapshot of SSI metrics.
#[must_use]
pub fn ssi_metrics_snapshot() -> SsiMetricsSnapshot {
    let commits_total = FSQLITE_SSI_COMMITS_TOTAL.load(Ordering::Relaxed);
    let aborts_pivot = FSQLITE_SSI_ABORTS_PIVOT.load(Ordering::Relaxed);
    let aborts_committed_pivot = FSQLITE_SSI_ABORTS_COMMITTED_PIVOT.load(Ordering::Relaxed);
    let aborts_marked_for_abort = FSQLITE_SSI_ABORTS_MARKED_FOR_ABORT.load(Ordering::Relaxed);
    let validations_total = commits_total
        .saturating_add(aborts_pivot)
        .saturating_add(aborts_committed_pivot)
        .saturating_add(aborts_marked_for_abort);
    SsiMetricsSnapshot {
        commits_total,
        aborts_pivot,
        aborts_committed_pivot,
        aborts_marked_for_abort,
        validations_total,
    }
}

/// Reset SSI metrics to zero (tests/diagnostics).
pub fn reset_ssi_metrics() {
    FSQLITE_SSI_COMMITS_TOTAL.store(0, Ordering::Relaxed);
    FSQLITE_SSI_ABORTS_PIVOT.store(0, Ordering::Relaxed);
    FSQLITE_SSI_ABORTS_COMMITTED_PIVOT.store(0, Ordering::Relaxed);
    FSQLITE_SSI_ABORTS_MARKED_FOR_ABORT.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// Conflict heat telemetry (bd-1dp9.6.7.13.1)
// ---------------------------------------------------------------------------

const CONFLICT_HEAT_SCHEMA_VERSION: &str = "fsqlite.mvcc.conflict_heat.v1";
const CONFLICT_HEAT_TOP_PAGE_LIMIT: usize = 16;
const CONFLICT_HEAT_OVERLAP_EDGE_LIMIT: usize = 64;

static CONFLICT_HEAT_TELEMETRY_ENABLED: AtomicBool = AtomicBool::new(false);
static CONFLICT_HEAT_STATE: LazyLock<Mutex<ConflictHeatState>> =
    LazyLock::new(|| Mutex::new(ConflictHeatState::default()));

// Unit tests share this process-global telemetry sink while the Rust test
// harness executes unrelated MVCC workloads in parallel. Restrict recording
// to the test thread that explicitly owns the capture window so those
// workloads cannot pollute an exact telemetry assertion. Production builds
// retain the process-wide opt-in behavior above.
#[cfg(test)]
thread_local! {
    static CONFLICT_HEAT_TEST_CAPTURE_ENABLED: Cell<bool> = const { Cell::new(false) };
}

/// Direction of an observed SSI overlap edge relative to the committing
/// transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub enum ConflictOverlapDirection {
    /// Another transaction read a witness this transaction writes.
    Incoming,
    /// This transaction read a witness another transaction writes.
    Outgoing,
}

impl ConflictOverlapDirection {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Incoming => "incoming",
            Self::Outgoing => "outgoing",
        }
    }
}

/// One edge in the conflict-topology overlap graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConflictHeatEdge {
    pub from: TxnToken,
    pub to: TxnToken,
    pub overlap_page: Option<PageNumber>,
    pub direction: ConflictOverlapDirection,
    pub source_is_active: bool,
}

/// Operator-facing context attached to conflict heat observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConflictHeatContext<'a> {
    pub trace_id: &'a str,
    pub run_id: &'a str,
    pub scenario_id: &'a str,
    pub btree_id: u32,
    pub table_or_index_role: &'a str,
    pub split_pressure: u32,
    pub allocation_class: &'a str,
    pub elapsed_ns: u64,
    pub first_failure_diag: &'a str,
}

impl Default for ConflictHeatContext<'_> {
    fn default() -> Self {
        Self {
            trace_id: "mvcc-conflict-heat",
            run_id: "local",
            scenario_id: "unknown",
            btree_id: 0,
            table_or_index_role: "unknown",
            split_pressure: 0,
            allocation_class: "unknown",
            elapsed_ns: 0,
            first_failure_diag: "none",
        }
    }
}

/// Single conflict heat observation produced by MVCC validation.
#[derive(Debug, Clone, Copy)]
pub struct ConflictHeatObservation<'a> {
    pub context: ConflictHeatContext<'a>,
    pub commit_seq: CommitSeq,
    pub writer_overlap_estimate: u32,
    pub conflict_heat: u64,
    pub overlap_edges: &'a [ConflictHeatEdge],
    pub fallback_pages: &'a [PageNumber],
}

/// Snapshot row for one hot conflict page.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ConflictHeatPageSummary {
    pub page_no: u32,
    pub conflict_heat: u64,
    pub writer_overlap_estimate: u32,
}

/// Snapshot row for one conflict overlap edge.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ConflictOverlapSummary {
    pub from_txn_id: u64,
    pub from_txn_epoch: u64,
    pub to_txn_id: u64,
    pub to_txn_epoch: u64,
    pub direction: &'static str,
    pub overlap_count: u64,
    pub conflict_heat: u64,
    pub last_page_no: Option<u32>,
    pub source_is_active: bool,
}

/// Deterministic conflict heat snapshot for PRAGMAs, verification scripts, and
/// later placement/deflection policy beads.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ConflictHeatSnapshot {
    pub schema_version: &'static str,
    pub observations_total: u64,
    pub max_writer_overlap_estimate: u32,
    pub max_conflict_heat: u64,
    pub top_pages: Vec<ConflictHeatPageSummary>,
    pub overlap_edges: Vec<ConflictOverlapSummary>,
    pub first_failure_diag: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ConflictOverlapKey {
    from_txn_id: u64,
    from_txn_epoch: u64,
    to_txn_id: u64,
    to_txn_epoch: u64,
    direction: ConflictOverlapDirection,
}

impl ConflictOverlapKey {
    fn new(edge: &ConflictHeatEdge) -> Self {
        Self {
            from_txn_id: edge.from.id.get(),
            from_txn_epoch: u64::from(edge.from.epoch.get()),
            to_txn_id: edge.to.id.get(),
            to_txn_epoch: u64::from(edge.to.epoch.get()),
            direction: edge.direction,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct ConflictHeatPageState {
    heat: u64,
    max_writer_overlap_estimate: u32,
}

#[derive(Debug, Clone, Copy, Default)]
struct ConflictOverlapState {
    overlap_count: u64,
    heat: u64,
    last_page: Option<PageNumber>,
    source_is_active: bool,
}

#[derive(Debug, Default)]
struct ConflictHeatState {
    observations_total: u64,
    max_writer_overlap_estimate: u32,
    max_conflict_heat: u64,
    pages: BTreeMap<PageNumber, ConflictHeatPageState>,
    overlap_edges: BTreeMap<ConflictOverlapKey, ConflictOverlapState>,
    first_failure_diag: Option<String>,
}

/// Enable or disable conflict heat telemetry.
///
/// Defaults to disabled. When disabled, record sites pay one relaxed bool load
/// and return before taking locks or allocating.
pub fn set_conflict_heat_telemetry_enabled(enabled: bool) {
    CONFLICT_HEAT_TELEMETRY_ENABLED.store(enabled, Ordering::Relaxed);
}

/// Current conflict heat telemetry flag.
#[must_use]
pub fn conflict_heat_telemetry_enabled() -> bool {
    CONFLICT_HEAT_TELEMETRY_ENABLED.load(Ordering::Relaxed)
}

#[cfg(test)]
pub(crate) fn set_conflict_heat_test_capture_enabled(enabled: bool) {
    CONFLICT_HEAT_TEST_CAPTURE_ENABLED.set(enabled);
}

fn conflict_heat_recording_enabled() -> bool {
    if !CONFLICT_HEAT_TELEMETRY_ENABLED.load(Ordering::Relaxed) {
        return false;
    }

    #[cfg(test)]
    {
        CONFLICT_HEAT_TEST_CAPTURE_ENABLED.get()
    }
    #[cfg(not(test))]
    {
        true
    }
}

/// Reset conflict heat telemetry state.
pub fn reset_conflict_heat_telemetry() {
    *CONFLICT_HEAT_STATE.lock() = ConflictHeatState::default();
}

/// Record one conflict heat observation.
pub fn record_conflict_heat_observation(observation: &ConflictHeatObservation<'_>) {
    if !conflict_heat_recording_enabled() {
        return;
    }
    record_conflict_heat_observation_slow(observation);
}

#[cold]
#[inline(never)]
fn record_conflict_heat_observation_slow(observation: &ConflictHeatObservation<'_>) {
    let mut state = CONFLICT_HEAT_STATE.lock();
    state.observations_total = state.observations_total.saturating_add(1);
    state.max_writer_overlap_estimate = state
        .max_writer_overlap_estimate
        .max(observation.writer_overlap_estimate);
    state.max_conflict_heat = state.max_conflict_heat.max(observation.conflict_heat);
    if observation.context.first_failure_diag != "none" && state.first_failure_diag.is_none() {
        state.first_failure_diag = Some(observation.context.first_failure_diag.to_owned());
    }

    let per_edge_heat = 1_u64;
    let mut emitted_page_event = false;
    for edge in observation.overlap_edges {
        if let Some(page) = edge.overlap_page {
            emitted_page_event = true;
            record_conflict_heat_page(&mut state, page, 1, observation.writer_overlap_estimate);
            emit_conflict_heat_page_event(observation, page, 1);
        }

        let key = ConflictOverlapKey::new(edge);
        let entry = state.overlap_edges.entry(key).or_default();
        entry.overlap_count = entry.overlap_count.saturating_add(1);
        entry.heat = entry.heat.saturating_add(per_edge_heat);
        entry.last_page = edge.overlap_page;
        entry.source_is_active = edge.source_is_active;
    }

    if !emitted_page_event {
        for &page in observation.fallback_pages {
            record_conflict_heat_page(
                &mut state,
                page,
                observation.conflict_heat.max(1),
                observation.writer_overlap_estimate,
            );
            emit_conflict_heat_page_event(observation, page, observation.conflict_heat.max(1));
        }
    }
}

fn record_conflict_heat_page(
    state: &mut ConflictHeatState,
    page: PageNumber,
    heat: u64,
    writer_overlap_estimate: u32,
) {
    let page_state = state.pages.entry(page).or_default();
    page_state.heat = page_state.heat.saturating_add(heat);
    page_state.max_writer_overlap_estimate = page_state
        .max_writer_overlap_estimate
        .max(writer_overlap_estimate);
}

fn emit_conflict_heat_page_event(
    observation: &ConflictHeatObservation<'_>,
    page: PageNumber,
    page_heat: u64,
) {
    tracing::info!(
        target: "fsqlite.mvcc.conflict_heat",
        trace_id = observation.context.trace_id,
        run_id = observation.context.run_id,
        scenario_id = observation.context.scenario_id,
        btree_id = observation.context.btree_id,
        table_or_index_role = observation.context.table_or_index_role,
        page_no = page.get(),
        commit_seq = observation.commit_seq.get(),
        writer_overlap_estimate = observation.writer_overlap_estimate,
        conflict_heat = page_heat,
        split_pressure = observation.context.split_pressure,
        allocation_class = observation.context.allocation_class,
        elapsed_ns = observation.context.elapsed_ns,
        first_failure_diag = observation.context.first_failure_diag,
        "mvcc conflict heat observation"
    );
}

/// Take a deterministic conflict heat telemetry snapshot.
#[must_use]
pub fn conflict_heat_telemetry_snapshot() -> ConflictHeatSnapshot {
    let state = CONFLICT_HEAT_STATE.lock();
    let mut top_pages = state
        .pages
        .iter()
        .map(|(&page, page_state)| ConflictHeatPageSummary {
            page_no: page.get(),
            conflict_heat: page_state.heat,
            writer_overlap_estimate: page_state.max_writer_overlap_estimate,
        })
        .collect::<Vec<_>>();
    top_pages.sort_by(|left, right| {
        right
            .conflict_heat
            .cmp(&left.conflict_heat)
            .then_with(|| left.page_no.cmp(&right.page_no))
    });
    top_pages.truncate(CONFLICT_HEAT_TOP_PAGE_LIMIT);

    let mut overlap_edges = state
        .overlap_edges
        .iter()
        .map(|(key, edge_state)| ConflictOverlapSummary {
            from_txn_id: key.from_txn_id,
            from_txn_epoch: key.from_txn_epoch,
            to_txn_id: key.to_txn_id,
            to_txn_epoch: key.to_txn_epoch,
            direction: key.direction.as_str(),
            overlap_count: edge_state.overlap_count,
            conflict_heat: edge_state.heat,
            last_page_no: edge_state.last_page.map(PageNumber::get),
            source_is_active: edge_state.source_is_active,
        })
        .collect::<Vec<_>>();
    overlap_edges.sort_by(|left, right| {
        right
            .conflict_heat
            .cmp(&left.conflict_heat)
            .then_with(|| left.from_txn_id.cmp(&right.from_txn_id))
            .then_with(|| left.to_txn_id.cmp(&right.to_txn_id))
            .then_with(|| left.direction.cmp(right.direction))
    });
    overlap_edges.truncate(CONFLICT_HEAT_OVERLAP_EDGE_LIMIT);

    ConflictHeatSnapshot {
        schema_version: CONFLICT_HEAT_SCHEMA_VERSION,
        observations_total: state.observations_total,
        max_writer_overlap_estimate: state.max_writer_overlap_estimate,
        max_conflict_heat: state.max_conflict_heat,
        top_pages,
        overlap_edges,
        first_failure_diag: state.first_failure_diag.clone(),
    }
}

// ---------------------------------------------------------------------------
// Emit helpers for each event kind
// ---------------------------------------------------------------------------

/// Emit a page lock contention event.
///
/// Called when a page lock is held by another transaction and the requester
/// receives `Busy`.
pub fn emit_page_lock_contention(
    observer: &SharedObserver,
    page: PageNumber,
    requester: TxnId,
    holder: TxnId,
) {
    let event = ConflictEvent::PageLockContention {
        page,
        requester,
        holder,
        timestamp_ns: now_ns(),
    };
    tracing::info!(
        page = page.get(),
        requester = %requester,
        holder = %holder,
        "mvcc::page_lock_contention"
    );
    emit(observer, &event);
}

/// Emit a first-committer-wins base drift event.
///
/// Called from `concurrent_commit` when FCW validation detects that another
/// transaction committed to the same page after the snapshot.
pub fn emit_fcw_base_drift(
    observer: &SharedObserver,
    page: PageNumber,
    loser: TxnId,
    winner_commit_seq: CommitSeq,
    merge_attempted: bool,
    merge_succeeded: bool,
) {
    let event = ConflictEvent::FcwBaseDrift {
        page,
        loser,
        winner_commit_seq,
        merge_attempted,
        merge_succeeded,
        timestamp_ns: now_ns(),
    };
    tracing::warn!(
        page = page.get(),
        loser = %loser,
        winner_seq = winner_commit_seq.get(),
        merge_attempted,
        merge_succeeded,
        "mvcc::fcw_base_drift"
    );
    emit(observer, &event);
}

/// Emit an SSI abort event.
///
/// Called when SSI validation detects a dangerous structure (write skew)
/// and the transaction must abort.
pub fn emit_ssi_abort(
    observer: &SharedObserver,
    txn: TxnToken,
    reason: SsiAbortCategory,
    in_edge_count: usize,
    out_edge_count: usize,
) {
    let reason_str = match reason {
        SsiAbortCategory::Pivot => "pivot",
        SsiAbortCategory::CommittedPivot => "committed_pivot",
        SsiAbortCategory::MarkedForAbort => "marked_for_abort",
    };
    let event = ConflictEvent::SsiAbort {
        txn,
        reason,
        in_edge_count,
        out_edge_count,
        timestamp_ns: now_ns(),
    };
    tracing::warn!(
        txn_id = txn.id.get(),
        reason = reason_str,
        in_edges = in_edge_count,
        out_edges = out_edge_count,
        "mvcc::ssi_abort"
    );
    emit(observer, &event);
}

/// Emit a conflict-resolved event (merge succeeded).
pub fn emit_conflict_resolved(
    observer: &SharedObserver,
    txn: TxnId,
    pages_merged: usize,
    commit_seq: CommitSeq,
) {
    let event = ConflictEvent::ConflictResolved {
        txn,
        pages_merged,
        commit_seq,
        timestamp_ns: now_ns(),
    };
    tracing::info!(
        txn = %txn,
        pages_merged,
        commit_seq = commit_seq.get(),
        "mvcc::conflict_resolved"
    );
    emit(observer, &event);
}

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_observability::MetricsObserver;
    use fsqlite_types::TxnEpoch;

    fn make_page(n: u32) -> PageNumber {
        PageNumber::new(n).unwrap()
    }

    fn make_txn(n: u64) -> TxnId {
        TxnId::new(n).unwrap()
    }

    fn make_token(n: u64) -> TxnToken {
        TxnToken::new(TxnId::new(n).unwrap(), TxnEpoch::new(1))
    }

    #[test]
    fn emit_fcw_records_to_observer() {
        let obs = Arc::new(MetricsObserver::new(100));
        let shared: SharedObserver = Some(obs.clone() as Arc<dyn ConflictObserver>);

        emit_fcw_base_drift(
            &shared,
            make_page(10),
            make_txn(2),
            CommitSeq::new(5),
            false,
            false,
        );

        let snap = obs.metrics().snapshot();
        assert_eq!(snap.fcw_drifts, 1);
        assert_eq!(snap.conflicts_total, 1);

        let events = obs.log().snapshot();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            ConflictEvent::FcwBaseDrift { page, loser, .. }
                if page.get() == 10 && loser.get() == 2
        ));
    }

    #[test]
    fn emit_ssi_abort_records_to_observer() {
        let obs = Arc::new(MetricsObserver::new(100));
        let shared: SharedObserver = Some(obs.clone() as Arc<dyn ConflictObserver>);

        emit_ssi_abort(&shared, make_token(3), SsiAbortCategory::Pivot, 1, 1);

        let snap = obs.metrics().snapshot();
        assert_eq!(snap.ssi_aborts, 1);
    }

    #[test]
    fn emit_contention_records_to_observer() {
        let obs = Arc::new(MetricsObserver::new(100));
        let shared: SharedObserver = Some(obs.clone() as Arc<dyn ConflictObserver>);

        emit_page_lock_contention(&shared, make_page(42), make_txn(1), make_txn(2));

        let snap = obs.metrics().snapshot();
        assert_eq!(snap.page_contentions, 1);
    }

    #[test]
    fn emit_conflict_resolved_records_to_observer() {
        let obs = Arc::new(MetricsObserver::new(100));
        let shared: SharedObserver = Some(obs.clone() as Arc<dyn ConflictObserver>);

        emit_conflict_resolved(&shared, make_txn(1), 2, CommitSeq::new(10));

        let snap = obs.metrics().snapshot();
        assert_eq!(snap.conflicts_resolved, 1);
        // ConflictResolved is not a conflict, so total should stay 0.
        assert_eq!(snap.conflicts_total, 0);
    }

    #[test]
    fn no_observer_no_panic() {
        let shared: SharedObserver = None;
        emit_fcw_base_drift(
            &shared,
            make_page(1),
            make_txn(1),
            CommitSeq::new(1),
            false,
            false,
        );
        emit_ssi_abort(
            &shared,
            make_token(1),
            SsiAbortCategory::MarkedForAbort,
            0,
            0,
        );
        emit_page_lock_contention(&shared, make_page(1), make_txn(1), make_txn(2));
        emit_conflict_resolved(&shared, make_txn(1), 0, CommitSeq::new(1));
    }

    #[test]
    fn snapshot_metrics_record_histogram_and_gauge() {
        // The histogram defaults to disabled in production; enable it here
        // so the record_* calls below actually mutate the counters.
        set_mvcc_snapshot_metrics_enabled(true);
        let before = mvcc_snapshot_metrics_snapshot();

        mvcc_snapshot_established();
        mvcc_snapshot_established();
        record_snapshot_read_versions_traversed(1);
        record_snapshot_read_versions_traversed(4);
        record_snapshot_read_versions_traversed(20);
        mvcc_snapshot_released();

        let after = mvcc_snapshot_metrics_snapshot();
        assert!(after.versions_traversed_samples >= before.versions_traversed_samples + 3);
        assert!(after.versions_traversed_sum >= before.versions_traversed_sum + 25);
        assert!(
            after.fsqlite_mvcc_versions_traversed.le_1
                > before.fsqlite_mvcc_versions_traversed.le_1
        );
        assert!(
            after.fsqlite_mvcc_versions_traversed.le_4
                > before.fsqlite_mvcc_versions_traversed.le_4
        );
        assert!(
            after.fsqlite_mvcc_versions_traversed.gt_16
                > before.fsqlite_mvcc_versions_traversed.gt_16
        );
        assert!(after.fsqlite_mvcc_active_snapshots >= 1);
    }

    #[test]
    fn snapshot_gauge_release_saturates() {
        // Saturating release must never underflow/panic, even when gauge is zero.
        mvcc_snapshot_released();
    }

    #[test]
    fn cas_metrics_recording_buckets_progress() {
        // The CAS histogram defaults to disabled in production; enable it
        // here so the record_cas_attempt calls below actually mutate the
        // counters.
        set_mvcc_cas_metrics_enabled(true);
        let before = cas_metrics_snapshot();
        record_cas_attempt(1);
        record_cas_attempt(2);
        record_cas_attempt(4);
        record_cas_attempt(6);
        let after = cas_metrics_snapshot();

        let total_delta = after.attempts_total.saturating_sub(before.attempts_total);
        let le_1_delta = after.retries.le_1.saturating_sub(before.retries.le_1);
        let le_2_delta = after.retries.le_2.saturating_sub(before.retries.le_2);
        let le_4_delta = after.retries.le_4.saturating_sub(before.retries.le_4);
        let gt_4_delta = after.retries.gt_4.saturating_sub(before.retries.gt_4);

        assert!(
            total_delta >= 4,
            "expected >=4 new samples, got {total_delta}"
        );
        assert!(
            le_1_delta >= 1,
            "expected >=1 le_1 sample, got {le_1_delta}"
        );
        assert!(
            le_2_delta >= 1,
            "expected >=1 le_2 sample, got {le_2_delta}"
        );
        assert!(
            le_4_delta >= 1,
            "expected >=1 le_4 sample, got {le_4_delta}"
        );
        assert!(
            gt_4_delta >= 1,
            "expected >=1 gt_4 sample, got {gt_4_delta}"
        );
    }

    #[test]
    fn cas_metrics_first_attempt_ratio_helper() {
        let empty = CasMetricsSnapshot::default();
        assert!((empty.first_attempt_ratio() - 0.0).abs() < f64::EPSILON);

        let snapshot = CasMetricsSnapshot {
            attempts_total: 20,
            retries: CasRetriesHistogram {
                le_1: 19,
                le_2: 1,
                le_4: 0,
                gt_4: 0,
            },
        };
        assert_eq!(snapshot.first_attempt_count(), 19);
        assert!((snapshot.first_attempt_ratio() - 0.95).abs() < 1e-12);
    }

    // -----------------------------------------------------------------------
    // bd-688.2: SSI Metrics Tests
    // -----------------------------------------------------------------------

    #[test]
    fn ssi_metrics_commit_counting() {
        // Use a local snapshot-delta pattern (global shared across tests).
        let before = ssi_metrics_snapshot();
        record_ssi_commit();
        record_ssi_commit();
        let after = ssi_metrics_snapshot();
        assert!(after.commits_total >= before.commits_total + 2);
        assert!(after.validations_total >= before.validations_total + 2);
    }

    #[test]
    fn ssi_metrics_abort_by_reason() {
        let before = ssi_metrics_snapshot();
        record_ssi_abort(SsiAbortCategory::Pivot);
        record_ssi_abort(SsiAbortCategory::CommittedPivot);
        record_ssi_abort(SsiAbortCategory::MarkedForAbort);
        let after = ssi_metrics_snapshot();
        assert!(after.aborts_pivot > before.aborts_pivot);
        assert!(after.aborts_committed_pivot > before.aborts_committed_pivot);
        assert!(after.aborts_marked_for_abort > before.aborts_marked_for_abort);
        assert!(after.aborts_total() >= before.aborts_total() + 3);
        assert!(after.validations_total >= before.validations_total + 3);
    }

    #[test]
    fn ssi_metrics_conflict_rate() {
        let m = SsiMetricsSnapshot {
            commits_total: 90,
            aborts_pivot: 5,
            aborts_committed_pivot: 3,
            aborts_marked_for_abort: 2,
            validations_total: 100,
        };
        assert!((m.conflict_rate() - 0.10).abs() < 1e-10);
        assert_eq!(m.aborts_total(), 10);
    }

    #[test]
    fn ssi_metrics_conflict_rate_zero_validations() {
        let m = SsiMetricsSnapshot::default();
        assert!((m.conflict_rate() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn ssi_metrics_display() {
        let m = SsiMetricsSnapshot {
            commits_total: 50,
            aborts_pivot: 2,
            aborts_committed_pivot: 1,
            aborts_marked_for_abort: 0,
            validations_total: 53,
        };
        let display = format!("{m}");
        assert!(display.contains("50 commits"), "display: {display}");
        assert!(display.contains("3 aborts"), "display: {display}");
        assert!(display.contains("pivot=2"), "display: {display}");
    }

    #[test]
    fn ssi_metrics_reset() {
        let before = ssi_metrics_snapshot();
        record_ssi_commit();
        record_ssi_abort(SsiAbortCategory::Pivot);
        let after = ssi_metrics_snapshot();
        let commits_delta = after.commits_total - before.commits_total;
        let aborts_delta = after.aborts_pivot - before.aborts_pivot;
        assert!(
            commits_delta >= 1,
            "expected at least 1 commit delta, got {commits_delta}"
        );
        assert!(
            aborts_delta >= 1,
            "expected at least 1 abort delta, got {aborts_delta}"
        );
    }

    /// Microbench for the snapshot established/released gate extension.
    /// Each transaction begin/finalize calls `mvcc_snapshot_established`
    /// and `mvcc_snapshot_released`; the pre-gate path did one relaxed
    /// `fetch_add` + a CAS loop on a process-wide contended cache line.
    /// With the gate off (the new production default) both collapse to
    /// one relaxed bool load each.
    #[test]
    #[ignore = "microbench — run manually"]
    fn bench_snapshot_established_released_gate() {
        use std::time::Instant;

        const CYCLES_PER_TRIAL: u32 = 4_000_000;
        const TRIALS: usize = 9;

        fn run_trial(cycles: u32, enabled: bool) -> f64 {
            set_mvcc_snapshot_metrics_enabled(enabled);
            let start = Instant::now();
            for _ in 0..cycles {
                mvcc_snapshot_established();
                mvcc_snapshot_released();
            }
            start.elapsed().as_nanos() as f64 / f64::from(cycles)
        }

        run_trial(CYCLES_PER_TRIAL, true);
        run_trial(CYCLES_PER_TRIAL, false);

        let mut on_samples = Vec::with_capacity(TRIALS);
        let mut off_samples = Vec::with_capacity(TRIALS);
        for _ in 0..TRIALS {
            let on = run_trial(CYCLES_PER_TRIAL, true);
            let off = run_trial(CYCLES_PER_TRIAL, false);
            eprintln!("  enabled: {on:.2} ns/cycle   disabled: {off:.2} ns/cycle");
            on_samples.push(on);
            off_samples.push(off);
        }
        on_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        off_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let on_med = on_samples[TRIALS / 2];
        let off_med = off_samples[TRIALS / 2];
        let delta_pct = (off_med - on_med) / on_med * 100.0;
        eprintln!(
            "bench_snapshot_established_released_gate: enabled median={on_med:.2} ns/cycle; \
             disabled median={off_med:.2} ns/cycle; delta={delta_pct:+.1}% \
             (n={TRIALS}, {CYCLES_PER_TRIAL} est+rel cycles/trial)"
        );
        set_mvcc_snapshot_metrics_enabled(false);
    }

    /// Microbench for the redundant `validations_total` counter elimination.
    ///
    /// Pre-change `record_ssi_commit` did two unconditional relaxed
    /// `fetch_add`s — one on `FSQLITE_SSI_COMMITS_TOTAL`, one on
    /// `FSQLITE_SSI_VALIDATIONS_TOTAL`. The latter was redundant because
    /// every validation resolves into one categorised commit/abort, so
    /// `validations_total = commits_total + aborts_total` by definition.
    /// Post-change derives `validations_total` at snapshot time and the
    /// hot path drops to a single `fetch_add`.
    ///
    /// The bench reproduces the old shape locally so before/after numbers
    /// land in the same run without bringing the static back.
    #[test]
    #[ignore = "microbench — run manually"]
    fn bench_record_ssi_commit_validations_pruning() {
        use std::time::Instant;

        const CYCLES_PER_TRIAL: u32 = 4_000_000;
        const TRIALS: usize = 9;

        // Local stand-in for the deleted FSQLITE_SSI_VALIDATIONS_TOTAL.
        // Defined in the bench so the optimised path can stay clean.
        let baseline_validations = AtomicU64::new(0);

        fn run_old(extra: &AtomicU64, cycles: u32) -> f64 {
            let start = Instant::now();
            for _ in 0..cycles {
                FSQLITE_SSI_COMMITS_TOTAL.fetch_add(1, Ordering::Relaxed);
                extra.fetch_add(1, Ordering::Relaxed);
            }
            start.elapsed().as_nanos() as f64 / f64::from(cycles)
        }

        fn run_new(cycles: u32) -> f64 {
            let start = Instant::now();
            for _ in 0..cycles {
                record_ssi_commit();
            }
            start.elapsed().as_nanos() as f64 / f64::from(cycles)
        }

        // Warmups (caches, branch predictor).
        run_old(&baseline_validations, CYCLES_PER_TRIAL);
        run_new(CYCLES_PER_TRIAL);

        let mut old_samples = Vec::with_capacity(TRIALS);
        let mut new_samples = Vec::with_capacity(TRIALS);
        for _ in 0..TRIALS {
            let old = run_old(&baseline_validations, CYCLES_PER_TRIAL);
            let new_ = run_new(CYCLES_PER_TRIAL);
            eprintln!(
                "  old (2 fetch_adds): {old:.2} ns/call   new (1 fetch_add): {new_:.2} ns/call"
            );
            old_samples.push(old);
            new_samples.push(new_);
        }
        old_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        new_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let old_med = old_samples[TRIALS / 2];
        let new_med = new_samples[TRIALS / 2];
        let delta_pct = (new_med - old_med) / old_med * 100.0;
        eprintln!(
            "bench_record_ssi_commit_validations_pruning: old median={old_med:.2} ns/call; \
             new median={new_med:.2} ns/call; delta={delta_pct:+.1}% \
             (n={TRIALS}, {CYCLES_PER_TRIAL} cycles/trial)"
        );
    }
}
