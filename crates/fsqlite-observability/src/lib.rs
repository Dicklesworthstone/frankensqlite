//! MVCC conflict analytics and observability infrastructure.
//!
//! Provides shared types and utilities for conflict tracing, metrics
//! aggregation, and diagnostic logging across the FrankenSQLite MVCC layer.
//!
//! # Design Principles
//!
//! - **Zero-cost when unused:** All observation is opt-in via the
//!   [`ConflictObserver`] trait. When no observer is registered, conflict
//!   emission compiles to nothing (the default [`NoOpObserver`] is inlined).
//! - **Non-blocking:** Observers MUST NOT acquire page locks or block writers.
//!   Conflict tracing is purely diagnostic.
//! - **Shared foundation:** Types defined here are reused by downstream
//!   observability beads (bd-t6sv2.2, .3, .5, .6, .8, .12).

use fsqlite_types::sync_primitives::{Duration, Instant, Mutex};
use fsqlite_types::{CommitSeq, PageNumber, TxnId, TxnToken};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// Structured trace metrics (bd-19u.1)
// ---------------------------------------------------------------------------

static FSQLITE_TRACE_SPANS_TOTAL: AtomicU64 = AtomicU64::new(0);
static FSQLITE_TRACE_EXPORT_ERRORS_TOTAL: AtomicU64 = AtomicU64::new(0);
static FSQLITE_COMPAT_TRACE_CALLBACKS_TOTAL: AtomicU64 = AtomicU64::new(0);
static TRACE_ID_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static DECISION_ID_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Snapshot of structured tracing counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct TraceMetricsSnapshot {
    pub fsqlite_trace_spans_total: u64,
    pub fsqlite_trace_export_errors_total: u64,
    pub fsqlite_compat_trace_callbacks_total: u64,
}

/// Allocate the next trace identifier.
#[must_use]
pub fn next_trace_id() -> u64 {
    TRACE_ID_SEQUENCE.fetch_add(1, Ordering::Relaxed)
}

/// Allocate the next decision identifier.
#[must_use]
pub fn next_decision_id() -> u64 {
    DECISION_ID_SEQUENCE.fetch_add(1, Ordering::Relaxed)
}

/// Record creation of a tracing span in the SQL pipeline.
pub fn record_trace_span_created() {
    FSQLITE_TRACE_SPANS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Record an export batch for tracing spans.
pub fn record_trace_export(spans_exported: u64, export_latency_us: u64) {
    let span = tracing::span!(
        target: "fsqlite.trace_export",
        tracing::Level::DEBUG,
        "trace_export",
        spans_exported,
        export_latency_us
    );
    let _guard = span.enter();
    tracing::debug!("trace export observed");
}

/// Record a failed span-export attempt.
pub fn record_trace_export_error() {
    FSQLITE_TRACE_EXPORT_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Record a sqlite3_trace_v2 compatibility callback invocation.
pub fn record_compat_trace_callback() {
    FSQLITE_COMPAT_TRACE_CALLBACKS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Read a point-in-time snapshot of trace counters.
#[must_use]
pub fn trace_metrics_snapshot() -> TraceMetricsSnapshot {
    TraceMetricsSnapshot {
        fsqlite_trace_spans_total: FSQLITE_TRACE_SPANS_TOTAL.load(Ordering::Relaxed),
        fsqlite_trace_export_errors_total: FSQLITE_TRACE_EXPORT_ERRORS_TOTAL
            .load(Ordering::Relaxed),
        fsqlite_compat_trace_callbacks_total: FSQLITE_COMPAT_TRACE_CALLBACKS_TOTAL
            .load(Ordering::Relaxed),
    }
}

/// Reset trace counters to zero (tests/diagnostics).
pub fn reset_trace_metrics() {
    FSQLITE_TRACE_SPANS_TOTAL.store(0, Ordering::Relaxed);
    FSQLITE_TRACE_EXPORT_ERRORS_TOTAL.store(0, Ordering::Relaxed);
    FSQLITE_COMPAT_TRACE_CALLBACKS_TOTAL.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// io_uring latency telemetry + conformal bound signal (bd-al1)
// ---------------------------------------------------------------------------

const IO_URING_LATENCY_WINDOW_CAPACITY: usize = 1024;
const P99_NUMERATOR: usize = 99;
const P99_DENOMINATOR: usize = 100;

/// Global io_uring latency metrics singleton.
pub static GLOBAL_IO_URING_LATENCY_METRICS: LazyLock<IoUringLatencyMetrics> =
    LazyLock::new(|| IoUringLatencyMetrics::new(IO_URING_LATENCY_WINDOW_CAPACITY));

#[derive(Debug, Clone, Serialize)]
pub struct IoUringLatencySnapshot {
    pub read_samples_total: u64,
    pub write_samples_total: u64,
    pub unix_fallbacks_total: u64,
    pub read_tail_violations_total: u64,
    pub write_tail_violations_total: u64,
    pub window_capacity: usize,
    pub read_window_len: usize,
    pub write_window_len: usize,
    pub read_p99_latency_us: u64,
    pub write_p99_latency_us: u64,
    pub read_conformal_upper_bound_us: u64,
    pub write_conformal_upper_bound_us: u64,
}

#[derive(Default)]
struct IoLatencySeries {
    latencies_ns: VecDeque<u64>,
    nonconformity_ns: VecDeque<u64>,
}

impl IoLatencySeries {
    fn push(&mut self, sample_ns: u64, sample_capacity: usize) {
        let baseline = self.p99_latency_ns().unwrap_or(sample_ns);
        let score = sample_ns.saturating_sub(baseline);
        push_bounded(&mut self.latencies_ns, sample_ns, sample_capacity);
        push_bounded(&mut self.nonconformity_ns, score, sample_capacity);
    }

    fn p99_latency_ns(&self) -> Option<u64> {
        quantile_from_deque(&self.latencies_ns, P99_NUMERATOR, P99_DENOMINATOR)
    }

    fn conformal_upper_bound_ns(&self) -> Option<u64> {
        let baseline = self.p99_latency_ns()?;
        let q = conformal_quantile(&self.nonconformity_ns)?;
        Some(baseline.saturating_add(q))
    }

    fn reset(&mut self) {
        self.latencies_ns.clear();
        self.nonconformity_ns.clear();
    }
}

#[derive(Default)]
struct IoUringLatencyWindow {
    read: IoLatencySeries,
    write: IoLatencySeries,
}

pub struct IoUringLatencyMetrics {
    pub read_samples_total: AtomicU64,
    pub write_samples_total: AtomicU64,
    pub unix_fallbacks_total: AtomicU64,
    pub read_tail_violations_total: AtomicU64,
    pub write_tail_violations_total: AtomicU64,
    sample_capacity: usize,
    window: Mutex<IoUringLatencyWindow>,
}

impl IoUringLatencyMetrics {
    #[must_use]
    pub const fn new(sample_capacity: usize) -> Self {
        Self {
            read_samples_total: AtomicU64::new(0),
            write_samples_total: AtomicU64::new(0),
            unix_fallbacks_total: AtomicU64::new(0),
            read_tail_violations_total: AtomicU64::new(0),
            write_tail_violations_total: AtomicU64::new(0),
            sample_capacity,
            window: Mutex::new(IoUringLatencyWindow {
                read: IoLatencySeries {
                    latencies_ns: VecDeque::new(),
                    nonconformity_ns: VecDeque::new(),
                },
                write: IoLatencySeries {
                    latencies_ns: VecDeque::new(),
                    nonconformity_ns: VecDeque::new(),
                },
            }),
        }
    }

    pub fn record_read_latency(&self, latency: Duration) -> bool {
        self.read_samples_total.fetch_add(1, Ordering::Relaxed);
        let sample_ns = duration_to_nanos_saturated(latency);
        let mut window = self.window.lock();
        let prior_bound = window.read.conformal_upper_bound_ns();
        window.read.push(sample_ns, self.sample_capacity);
        let is_tail_violation = prior_bound.is_some_and(|bound| sample_ns > bound);
        if is_tail_violation {
            self.read_tail_violations_total
                .fetch_add(1, Ordering::Relaxed);
        }
        is_tail_violation
    }

    pub fn record_write_latency(&self, latency: Duration) -> bool {
        self.write_samples_total.fetch_add(1, Ordering::Relaxed);
        let sample_ns = duration_to_nanos_saturated(latency);
        let mut window = self.window.lock();
        let prior_bound = window.write.conformal_upper_bound_ns();
        window.write.push(sample_ns, self.sample_capacity);
        let is_tail_violation = prior_bound.is_some_and(|bound| sample_ns > bound);
        if is_tail_violation {
            self.write_tail_violations_total
                .fetch_add(1, Ordering::Relaxed);
        }
        is_tail_violation
    }

    pub fn record_unix_fallback(&self) {
        self.unix_fallbacks_total.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> IoUringLatencySnapshot {
        let window = self.window.lock();

        IoUringLatencySnapshot {
            read_samples_total: self.read_samples_total.load(Ordering::Relaxed),
            write_samples_total: self.write_samples_total.load(Ordering::Relaxed),
            unix_fallbacks_total: self.unix_fallbacks_total.load(Ordering::Relaxed),
            read_tail_violations_total: self.read_tail_violations_total.load(Ordering::Relaxed),
            write_tail_violations_total: self.write_tail_violations_total.load(Ordering::Relaxed),
            window_capacity: self.sample_capacity,
            read_window_len: window.read.latencies_ns.len(),
            write_window_len: window.write.latencies_ns.len(),
            read_p99_latency_us: nanos_to_micros(window.read.p99_latency_ns().unwrap_or(0)),
            write_p99_latency_us: nanos_to_micros(window.write.p99_latency_ns().unwrap_or(0)),
            read_conformal_upper_bound_us: nanos_to_micros(
                window.read.conformal_upper_bound_ns().unwrap_or(0),
            ),
            write_conformal_upper_bound_us: nanos_to_micros(
                window.write.conformal_upper_bound_ns().unwrap_or(0),
            ),
        }
    }

    pub fn reset(&self) {
        self.read_samples_total.store(0, Ordering::Relaxed);
        self.write_samples_total.store(0, Ordering::Relaxed);
        self.unix_fallbacks_total.store(0, Ordering::Relaxed);
        self.read_tail_violations_total.store(0, Ordering::Relaxed);
        self.write_tail_violations_total.store(0, Ordering::Relaxed);
        let mut window = self.window.lock();
        window.read.reset();
        window.write.reset();
    }
}

impl Default for IoUringLatencyMetrics {
    fn default() -> Self {
        Self::new(IO_URING_LATENCY_WINDOW_CAPACITY)
    }
}

pub fn record_io_uring_read_latency(latency: Duration) -> bool {
    GLOBAL_IO_URING_LATENCY_METRICS.record_read_latency(latency)
}

pub fn record_io_uring_write_latency(latency: Duration) -> bool {
    GLOBAL_IO_URING_LATENCY_METRICS.record_write_latency(latency)
}

pub fn record_io_uring_unix_fallback() {
    GLOBAL_IO_URING_LATENCY_METRICS.record_unix_fallback();
}

#[must_use]
pub fn io_uring_latency_snapshot() -> IoUringLatencySnapshot {
    GLOBAL_IO_URING_LATENCY_METRICS.snapshot()
}

pub fn reset_io_uring_latency_metrics() {
    GLOBAL_IO_URING_LATENCY_METRICS.reset();
}

fn push_bounded(buffer: &mut VecDeque<u64>, value: u64, sample_capacity: usize) {
    if sample_capacity == 0 {
        return;
    }
    if buffer.len() == sample_capacity {
        let _ = buffer.pop_front();
    }
    buffer.push_back(value);
}

fn quantile_from_deque(
    values: &VecDeque<u64>,
    numerator: usize,
    denominator: usize,
) -> Option<u64> {
    if values.is_empty() || denominator == 0 {
        return None;
    }

    let mut sorted: Vec<u64> = values.iter().copied().collect();
    sorted.sort_unstable();

    let n = sorted.len();
    let rank = numerator
        .saturating_mul(n)
        .div_ceil(denominator)
        .saturating_sub(1)
        .min(n.saturating_sub(1));
    sorted.get(rank).copied()
}

fn conformal_quantile(nonconformity: &VecDeque<u64>) -> Option<u64> {
    if nonconformity.is_empty() {
        return None;
    }

    let mut sorted: Vec<u64> = nonconformity.iter().copied().collect();
    sorted.sort_unstable();

    let n = sorted.len();
    let rank = P99_NUMERATOR
        .saturating_mul(n.saturating_add(1))
        .div_ceil(P99_DENOMINATOR)
        .saturating_sub(1)
        .min(n.saturating_sub(1));
    sorted.get(rank).copied()
}

fn nanos_to_micros(nanos: u64) -> u64 {
    nanos / 1_000
}

fn duration_to_nanos_saturated(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

// ---------------------------------------------------------------------------
// Cx propagation telemetry (bd-2g5.6.1)
// ---------------------------------------------------------------------------

/// Global Cx propagation metrics singleton.
///
/// Tracks how well the capability context (`Cx`) is threaded through
/// connection and transaction paths. Incremented on the code paths that
/// detect missing or invalid propagation, as well as on successful
/// propagation checkpoints, cancellation cleanup outcomes, and trace
/// linkage establishments.
pub static GLOBAL_CX_PROPAGATION_METRICS: CxPropagationMetrics = CxPropagationMetrics::new();

/// Atomic counters for Cx propagation telemetry.
///
/// Counters follow the same lock-free `Relaxed` ordering convention as
/// the rest of the observability crate — callers may observe stale reads
/// but never torn values.
pub struct CxPropagationMetrics {
    /// Number of successful Cx propagation checkpoints.
    pub propagation_successes_total: AtomicU64,
    /// Number of detected missing or invalid Cx propagation.
    pub propagation_failures_total: AtomicU64,
    /// Number of cancellation cleanup operations completed.
    pub cancellation_cleanups_total: AtomicU64,
    /// Number of trace-ID linkage establishments (Cx → span).
    pub trace_linkages_total: AtomicU64,
    /// Number of Cx instances created for transaction scopes.
    pub cx_created_total: AtomicU64,
    /// Number of Cx cancel propagations observed.
    pub cancel_propagations_total: AtomicU64,
}

impl Default for CxPropagationMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl CxPropagationMetrics {
    /// Create a new metrics instance with all counters at zero.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            propagation_successes_total: AtomicU64::new(0),
            propagation_failures_total: AtomicU64::new(0),
            cancellation_cleanups_total: AtomicU64::new(0),
            trace_linkages_total: AtomicU64::new(0),
            cx_created_total: AtomicU64::new(0),
            cancel_propagations_total: AtomicU64::new(0),
        }
    }

    /// Record a successful Cx propagation checkpoint.
    pub fn record_propagation_success(&self) {
        self.propagation_successes_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a missing or invalid Cx propagation and emit a WARN diagnostic.
    pub fn record_propagation_failure(&self, site: &str) {
        self.propagation_failures_total
            .fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            target: "fsqlite.cx_propagation",
            site,
            "missing or invalid Cx propagation detected"
        );
    }

    /// Record a cancellation cleanup completion.
    pub fn record_cancellation_cleanup(&self) {
        self.cancellation_cleanups_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a trace-ID linkage establishment.
    pub fn record_trace_linkage(&self) {
        self.trace_linkages_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record creation of a Cx for a transaction scope.
    pub fn record_cx_created(&self) {
        self.cx_created_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cancel propagation event.
    pub fn record_cancel_propagation(&self) {
        self.cancel_propagations_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Read a point-in-time snapshot.
    #[must_use]
    pub fn snapshot(&self) -> CxPropagationMetricsSnapshot {
        CxPropagationMetricsSnapshot {
            propagation_successes_total: self.propagation_successes_total.load(Ordering::Relaxed),
            propagation_failures_total: self.propagation_failures_total.load(Ordering::Relaxed),
            cancellation_cleanups_total: self.cancellation_cleanups_total.load(Ordering::Relaxed),
            trace_linkages_total: self.trace_linkages_total.load(Ordering::Relaxed),
            cx_created_total: self.cx_created_total.load(Ordering::Relaxed),
            cancel_propagations_total: self.cancel_propagations_total.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero (tests/diagnostics).
    pub fn reset(&self) {
        self.propagation_successes_total.store(0, Ordering::Relaxed);
        self.propagation_failures_total.store(0, Ordering::Relaxed);
        self.cancellation_cleanups_total.store(0, Ordering::Relaxed);
        self.trace_linkages_total.store(0, Ordering::Relaxed);
        self.cx_created_total.store(0, Ordering::Relaxed);
        self.cancel_propagations_total.store(0, Ordering::Relaxed);
    }
}

/// Serializable snapshot of Cx propagation metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CxPropagationMetricsSnapshot {
    pub propagation_successes_total: u64,
    pub propagation_failures_total: u64,
    pub cancellation_cleanups_total: u64,
    pub trace_linkages_total: u64,
    pub cx_created_total: u64,
    pub cancel_propagations_total: u64,
}

impl CxPropagationMetricsSnapshot {
    /// Propagation failure ratio (failures / total attempts). Returns 0.0
    /// when no attempts have been recorded.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn failure_ratio(&self) -> f64 {
        let total = self.propagation_successes_total + self.propagation_failures_total;
        if total == 0 {
            return 0.0;
        }
        self.propagation_failures_total as f64 / total as f64
    }
}

impl std::fmt::Display for CxPropagationMetricsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cx_propagation(ok={} fail={} cancel_cleanup={} trace_link={} cx_new={} cancel_prop={} fail_ratio={:.4})",
            self.propagation_successes_total,
            self.propagation_failures_total,
            self.cancellation_cleanups_total,
            self.trace_linkages_total,
            self.cx_created_total,
            self.cancel_propagations_total,
            self.failure_ratio(),
        )
    }
}

// ---------------------------------------------------------------------------
// TxnSlot crash/occupancy telemetry (bd-2g5.1)
// ---------------------------------------------------------------------------

/// Sentinel slot-id value used when the caller cannot map a concrete index.
const UNKNOWN_SLOT_ID: usize = usize::MAX;

/// Global TxnSlot observability metrics singleton.
///
/// Tracks active TxnSlot occupancy (`fsqlite_txn_slots_active`) and crash
/// detections (`fsqlite_txn_slot_crashes_detected_total`).
pub static GLOBAL_TXN_SLOT_METRICS: TxnSlotMetrics = TxnSlotMetrics::new();

/// Atomic counters for TxnSlot lifecycle telemetry.
///
/// Counters follow the same lock-free `Relaxed` ordering convention used by the
/// rest of this crate.
pub struct TxnSlotMetrics {
    /// Gauge: number of currently active (published) transaction slots.
    pub fsqlite_txn_slots_active: AtomicU64,
    /// Counter: number of detected orphan/crashed transaction slots.
    pub fsqlite_txn_slot_crashes_detected_total: AtomicU64,
}

impl Default for TxnSlotMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl TxnSlotMetrics {
    /// Create a new metrics instance with all counters at zero.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            fsqlite_txn_slots_active: AtomicU64::new(0),
            fsqlite_txn_slot_crashes_detected_total: AtomicU64::new(0),
        }
    }

    #[inline]
    const fn normalize_slot_id(slot_id: Option<usize>) -> usize {
        match slot_id {
            Some(value) => value,
            None => UNKNOWN_SLOT_ID,
        }
    }

    fn log_context_from_env() -> (String, u64, String) {
        let run_id = std::env::var("RUN_ID").unwrap_or_else(|_| "(none)".to_owned());
        let trace_id = std::env::var("TRACE_ID")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        let scenario_id = std::env::var("SCENARIO_ID").unwrap_or_else(|_| "(none)".to_owned());
        (run_id, trace_id, scenario_id)
    }

    fn decrement_active_slots_saturating(&self) -> u64 {
        loop {
            let prev = self.fsqlite_txn_slots_active.load(Ordering::Relaxed);
            let next = prev.saturating_sub(1);
            if self
                .fsqlite_txn_slots_active
                .compare_exchange_weak(prev, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return next;
            }
        }
    }

    /// Record a successful slot allocation/publish.
    pub fn record_slot_allocated(&self, slot_id: usize, process_id: u32) {
        let started_at = Instant::now();
        let active_after = self
            .fsqlite_txn_slots_active
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let operation_elapsed_us = started_at.elapsed().as_micros().max(1);
        let (run_id, trace_id, scenario_id) = Self::log_context_from_env();
        let span = tracing::span!(
            target: "fsqlite.txn_slot",
            tracing::Level::INFO,
            "txn_slot",
            slot_id,
            process_id,
            run_id = %run_id.as_str(),
            trace_id,
            scenario_id = %scenario_id.as_str(),
            operation = "alloc"
        );
        let _guard = span.enter();
        tracing::info!(
            fsqlite_txn_slots_active = active_after,
            operation_elapsed_us,
            run_id = %run_id.as_str(),
            trace_id,
            scenario_id = %scenario_id.as_str(),
            failure_context = "none",
            "transaction slot allocated"
        );
    }

    /// Record a slot release/free operation.
    pub fn record_slot_released(&self, slot_id: Option<usize>, process_id: u32) {
        let started_at = Instant::now();
        let active_after = self.decrement_active_slots_saturating();
        let slot_id = Self::normalize_slot_id(slot_id);
        let operation_elapsed_us = started_at.elapsed().as_micros().max(1);
        let (run_id, trace_id, scenario_id) = Self::log_context_from_env();
        let span = tracing::span!(
            target: "fsqlite.txn_slot",
            tracing::Level::INFO,
            "txn_slot",
            slot_id,
            process_id,
            run_id = %run_id.as_str(),
            trace_id,
            scenario_id = %scenario_id.as_str(),
            operation = "release"
        );
        let _guard = span.enter();
        tracing::info!(
            fsqlite_txn_slots_active = active_after,
            operation_elapsed_us,
            run_id = %run_id.as_str(),
            trace_id,
            scenario_id = %scenario_id.as_str(),
            failure_context = "none",
            "transaction slot released"
        );
    }

    /// Record detection/reclamation of a crashed/orphaned slot.
    pub fn record_crash_detected(
        &self,
        slot_id: Option<usize>,
        process_id: u32,
        orphan_txn_id: u64,
    ) {
        let started_at = Instant::now();
        let total = self
            .fsqlite_txn_slot_crashes_detected_total
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let slot_id = Self::normalize_slot_id(slot_id);
        let operation_elapsed_us = started_at.elapsed().as_micros().max(1);
        let (run_id, trace_id, scenario_id) = Self::log_context_from_env();
        let span = tracing::span!(
            target: "fsqlite.txn_slot",
            tracing::Level::WARN,
            "txn_slot",
            slot_id,
            process_id,
            run_id = %run_id.as_str(),
            trace_id,
            scenario_id = %scenario_id.as_str(),
            operation = "crash_detect"
        );
        let _guard = span.enter();
        tracing::warn!(
            orphan_txn_id,
            fsqlite_txn_slot_crashes_detected_total = total,
            operation_elapsed_us,
            run_id = %run_id.as_str(),
            trace_id,
            scenario_id = %scenario_id.as_str(),
            failure_context = "orphan_slot_reclaimed_during_cleanup",
            "orphaned transaction slot crash detected"
        );
    }

    /// Read a point-in-time snapshot.
    #[must_use]
    pub fn snapshot(&self) -> TxnSlotMetricsSnapshot {
        TxnSlotMetricsSnapshot {
            fsqlite_txn_slots_active: self.fsqlite_txn_slots_active.load(Ordering::Relaxed),
            fsqlite_txn_slot_crashes_detected_total: self
                .fsqlite_txn_slot_crashes_detected_total
                .load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero (tests/diagnostics).
    pub fn reset(&self) {
        self.fsqlite_txn_slots_active.store(0, Ordering::Relaxed);
        self.fsqlite_txn_slot_crashes_detected_total
            .store(0, Ordering::Relaxed);
    }
}

/// Serializable snapshot of TxnSlot telemetry counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct TxnSlotMetricsSnapshot {
    pub fsqlite_txn_slots_active: u64,
    pub fsqlite_txn_slot_crashes_detected_total: u64,
}

impl std::fmt::Display for TxnSlotMetricsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "txn_slots(active={} crashes={})",
            self.fsqlite_txn_slots_active, self.fsqlite_txn_slot_crashes_detected_total
        )
    }
}

// ---------------------------------------------------------------------------
// ConflictEvent — the core event type
// ---------------------------------------------------------------------------

/// A single conflict event emitted by the MVCC layer.
///
/// Each variant carries enough context to reconstruct what happened
/// without access to internal MVCC state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ConflictEvent {
    /// A page lock acquisition was denied because another txn holds it.
    PageLockContention {
        /// The page that was contended.
        page: PageNumber,
        /// The transaction that tried to acquire the lock.
        requester: TxnId,
        /// The transaction currently holding the lock.
        holder: TxnId,
        /// Monotonic event timestamp (nanoseconds since observer creation).
        timestamp_ns: u64,
    },

    /// First-Committer-Wins (FCW) detected base drift on a page.
    FcwBaseDrift {
        /// The page where drift was detected.
        page: PageNumber,
        /// The transaction that lost the FCW race.
        loser: TxnId,
        /// The transaction that committed first (winner).
        winner_commit_seq: CommitSeq,
        /// Whether merge was attempted.
        merge_attempted: bool,
        /// Whether merge succeeded (if attempted).
        merge_succeeded: bool,
        /// Monotonic event timestamp.
        timestamp_ns: u64,
    },

    /// SSI validation detected a dangerous structure (write skew).
    SsiAbort {
        /// The transaction that was aborted.
        txn: TxnToken,
        /// The reason for the abort.
        reason: SsiAbortCategory,
        /// Number of incoming rw-antidependency edges.
        in_edge_count: usize,
        /// Number of outgoing rw-antidependency edges.
        out_edge_count: usize,
        /// Monotonic event timestamp.
        timestamp_ns: u64,
    },

    /// A transaction committed successfully after resolving conflicts.
    ConflictResolved {
        /// The transaction that committed.
        txn: TxnId,
        /// Number of page conflicts resolved via merge.
        pages_merged: usize,
        /// Commit sequence assigned.
        commit_seq: CommitSeq,
        /// Monotonic event timestamp.
        timestamp_ns: u64,
    },
}

/// Categorized SSI abort reason (serialization-friendly).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum SsiAbortCategory {
    /// Transaction is the pivot (has both in + out rw edges).
    Pivot,
    /// A committed reader has an incoming rw edge.
    CommittedPivot,
    /// Transaction was eagerly marked for abort.
    MarkedForAbort,
}

impl ConflictEvent {
    /// Extract the monotonic timestamp from any event variant.
    #[must_use]
    pub fn timestamp_ns(&self) -> u64 {
        match self {
            Self::PageLockContention { timestamp_ns, .. }
            | Self::FcwBaseDrift { timestamp_ns, .. }
            | Self::SsiAbort { timestamp_ns, .. }
            | Self::ConflictResolved { timestamp_ns, .. } => *timestamp_ns,
        }
    }

    /// Whether this event represents a conflict (contention/drift/abort).
    #[must_use]
    pub fn is_conflict(&self) -> bool {
        !matches!(self, Self::ConflictResolved { .. })
    }
}

// ---------------------------------------------------------------------------
// ConflictObserver — trait for zero-cost opt-in observation
// ---------------------------------------------------------------------------

/// Observer trait for conflict events.
///
/// Implementations MUST be non-blocking and MUST NOT acquire page locks.
/// The observer is called on the hot path during lock acquisition and
/// commit validation; expensive work should be deferred.
pub trait ConflictObserver: Send + Sync {
    /// Called when a conflict event occurs.
    fn on_event(&self, event: &ConflictEvent);
}

/// No-op observer that compiles to nothing. Default when observability is
/// not configured.
#[derive(Debug, Clone, Copy)]
pub struct NoOpObserver;

impl ConflictObserver for NoOpObserver {
    #[inline(always)]
    fn on_event(&self, _event: &ConflictEvent) {}
}

// ---------------------------------------------------------------------------
// RingBuffer — bounded event storage
// ---------------------------------------------------------------------------

/// Fixed-capacity ring buffer for storing recent conflict events.
///
/// When the buffer is full, the oldest event is overwritten. Thread-safe
/// via internal `Mutex` (not on the hot path — only accessed via PRAGMA).
pub struct ConflictRingBuffer {
    events: Mutex<RingBuf>,
}

struct RingBuf {
    buf: Vec<ConflictEvent>,
    capacity: usize,
    head: usize,
    len: usize,
}

impl RingBuf {
    fn new(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
            capacity,
            head: 0,
            len: 0,
        }
    }

    fn push(&mut self, event: ConflictEvent) {
        if self.capacity == 0 {
            return;
        }
        let idx = (self.head + self.len) % self.capacity;
        if self.buf.len() < self.capacity {
            self.buf.push(event);
        } else {
            self.buf[idx] = event;
        }
        if self.len == self.capacity {
            self.head = (self.head + 1) % self.capacity;
        } else {
            self.len += 1;
        }
    }

    fn drain_ordered(&self) -> Vec<ConflictEvent> {
        let mut result = Vec::with_capacity(self.len);
        for i in 0..self.len {
            let idx = (self.head + i) % self.capacity;
            result.push(self.buf[idx].clone());
        }
        result
    }

    fn clear(&mut self) {
        self.buf.clear();
        self.head = 0;
        self.len = 0;
    }
}

impl ConflictRingBuffer {
    /// Create a new ring buffer with the given capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            events: Mutex::new(RingBuf::new(capacity)),
        }
    }

    /// Push an event into the ring buffer.
    pub fn push(&self, event: ConflictEvent) {
        self.events.lock().push(event);
    }

    /// Return all events in chronological order.
    #[must_use]
    pub fn snapshot(&self) -> Vec<ConflictEvent> {
        self.events.lock().drain_ordered()
    }

    /// Clear all stored events.
    pub fn clear(&self) {
        self.events.lock().clear();
    }

    /// Current number of stored events.
    #[must_use]
    pub fn len(&self) -> usize {
        self.events.lock().len
    }

    /// Whether the buffer is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Configured capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.events.lock().capacity
    }
}

// ---------------------------------------------------------------------------
// ConflictMetrics — aggregated statistics
// ---------------------------------------------------------------------------

/// Aggregated conflict statistics exposed via PRAGMA.
///
/// All counters are atomic for lock-free updates from the hot path.
/// Statistics are per-connection (not global).
pub struct ConflictMetrics {
    /// Total conflict events (contention + drift + abort).
    pub conflicts_total: AtomicU64,
    /// Page lock contention events.
    pub page_contentions: AtomicU64,
    /// FCW base drift events.
    pub fcw_drifts: AtomicU64,
    /// FCW merge attempts.
    pub fcw_merge_attempts: AtomicU64,
    /// FCW merge successes.
    pub fcw_merge_successes: AtomicU64,
    /// SSI abort events.
    pub ssi_aborts: AtomicU64,
    /// Successful conflict resolutions via merge.
    pub conflicts_resolved: AtomicU64,
    /// Per-page contention counts (behind mutex, not hot path).
    page_hotspots: Mutex<HashMap<PageNumber, u64>>,
    /// Creation time for rate calculations.
    created_at: Instant,
}

impl ConflictMetrics {
    /// Create a new metrics instance with all counters at zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            conflicts_total: AtomicU64::new(0),
            page_contentions: AtomicU64::new(0),
            fcw_drifts: AtomicU64::new(0),
            fcw_merge_attempts: AtomicU64::new(0),
            fcw_merge_successes: AtomicU64::new(0),
            ssi_aborts: AtomicU64::new(0),
            conflicts_resolved: AtomicU64::new(0),
            page_hotspots: Mutex::new(HashMap::new()),
            created_at: Instant::now(),
        }
    }

    /// Record a conflict event, updating all relevant counters.
    pub fn record(&self, event: &ConflictEvent) {
        match event {
            ConflictEvent::PageLockContention { page, .. } => {
                self.conflicts_total.fetch_add(1, Ordering::Relaxed);
                self.page_contentions.fetch_add(1, Ordering::Relaxed);
                *self.page_hotspots.lock().entry(*page).or_insert(0) += 1;
            }
            ConflictEvent::FcwBaseDrift {
                page,
                merge_attempted,
                merge_succeeded,
                ..
            } => {
                self.conflicts_total.fetch_add(1, Ordering::Relaxed);
                self.fcw_drifts.fetch_add(1, Ordering::Relaxed);
                if *merge_attempted {
                    self.fcw_merge_attempts.fetch_add(1, Ordering::Relaxed);
                    if *merge_succeeded {
                        self.fcw_merge_successes.fetch_add(1, Ordering::Relaxed);
                    }
                }
                *self.page_hotspots.lock().entry(*page).or_insert(0) += 1;
            }
            ConflictEvent::SsiAbort { .. } => {
                self.conflicts_total.fetch_add(1, Ordering::Relaxed);
                self.ssi_aborts.fetch_add(1, Ordering::Relaxed);
            }
            ConflictEvent::ConflictResolved { .. } => {
                self.conflicts_resolved.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.conflicts_total.store(0, Ordering::Relaxed);
        self.page_contentions.store(0, Ordering::Relaxed);
        self.fcw_drifts.store(0, Ordering::Relaxed);
        self.fcw_merge_attempts.store(0, Ordering::Relaxed);
        self.fcw_merge_successes.store(0, Ordering::Relaxed);
        self.ssi_aborts.store(0, Ordering::Relaxed);
        self.conflicts_resolved.store(0, Ordering::Relaxed);
        self.page_hotspots.lock().clear();
    }

    /// Elapsed time since metrics creation.
    #[must_use]
    pub fn elapsed(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }

    /// Conflicts per second since creation.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn conflicts_per_second(&self) -> f64 {
        let elapsed_secs = self.created_at.elapsed().as_secs_f64();
        if elapsed_secs < f64::EPSILON {
            return 0.0;
        }
        self.conflicts_total.load(Ordering::Relaxed) as f64 / elapsed_secs
    }

    /// Top N pages by contention count.
    #[must_use]
    pub fn top_hotspots(&self, n: usize) -> Vec<(PageNumber, u64)> {
        let mut entries: Vec<(PageNumber, u64)> = {
            let map = self.page_hotspots.lock();
            map.iter().map(|(&k, &v)| (k, v)).collect()
        };
        entries.sort_by_key(|e| std::cmp::Reverse(e.1));
        entries.truncate(n);
        entries
    }

    /// Snapshot all metrics as a serializable summary.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn snapshot(&self) -> ConflictMetricsSnapshot {
        ConflictMetricsSnapshot {
            conflicts_total: self.conflicts_total.load(Ordering::Relaxed),
            page_contentions: self.page_contentions.load(Ordering::Relaxed),
            fcw_drifts: self.fcw_drifts.load(Ordering::Relaxed),
            fcw_merge_attempts: self.fcw_merge_attempts.load(Ordering::Relaxed),
            fcw_merge_successes: self.fcw_merge_successes.load(Ordering::Relaxed),
            ssi_aborts: self.ssi_aborts.load(Ordering::Relaxed),
            conflicts_resolved: self.conflicts_resolved.load(Ordering::Relaxed),
            conflicts_per_second: self.conflicts_per_second(),
            elapsed_secs: self.created_at.elapsed().as_secs_f64(),
            top_hotspots: self.top_hotspots(10),
        }
    }
}

impl Default for ConflictMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Serializable snapshot of conflict metrics.
#[derive(Debug, Clone, Serialize)]
pub struct ConflictMetricsSnapshot {
    pub conflicts_total: u64,
    pub page_contentions: u64,
    pub fcw_drifts: u64,
    pub fcw_merge_attempts: u64,
    pub fcw_merge_successes: u64,
    pub ssi_aborts: u64,
    pub conflicts_resolved: u64,
    pub conflicts_per_second: f64,
    pub elapsed_secs: f64,
    pub top_hotspots: Vec<(PageNumber, u64)>,
}

// ---------------------------------------------------------------------------
// MetricsObserver — observer that records to both metrics and ring buffer
// ---------------------------------------------------------------------------

/// Combined observer that records events to both a [`ConflictMetrics`]
/// aggregator and a [`ConflictRingBuffer`] for detailed logging.
pub struct MetricsObserver {
    metrics: ConflictMetrics,
    log: ConflictRingBuffer,
    epoch: Instant,
}

impl MetricsObserver {
    /// Create a new metrics observer with the given ring buffer capacity.
    #[must_use]
    pub fn new(log_capacity: usize) -> Self {
        Self {
            metrics: ConflictMetrics::new(),
            log: ConflictRingBuffer::new(log_capacity),
            epoch: Instant::now(),
        }
    }

    /// Access the aggregated metrics.
    #[must_use]
    pub fn metrics(&self) -> &ConflictMetrics {
        &self.metrics
    }

    /// Access the conflict log ring buffer.
    #[must_use]
    pub fn log(&self) -> &ConflictRingBuffer {
        &self.log
    }

    /// Elapsed nanoseconds since observer creation (for timestamps).
    #[must_use]
    pub fn elapsed_ns(&self) -> u64 {
        #[allow(clippy::cast_possible_truncation)] // clamped to u64::MAX
        {
            self.epoch.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
        }
    }

    /// Reset both metrics and log.
    pub fn reset(&self) {
        self.metrics.reset();
        self.log.clear();
    }
}

impl ConflictObserver for MetricsObserver {
    fn on_event(&self, event: &ConflictEvent) {
        self.metrics.record(event);
        self.log.push(event.clone());
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn page(n: u32) -> PageNumber {
        PageNumber::new(n).unwrap()
    }

    fn txn(n: u64) -> TxnId {
        TxnId::new(n).unwrap()
    }

    fn make_contention_event(pg: u32, req: u64, hold: u64) -> ConflictEvent {
        ConflictEvent::PageLockContention {
            page: page(pg),
            requester: txn(req),
            holder: txn(hold),
            timestamp_ns: 1000,
        }
    }

    #[test]
    fn noop_observer_compiles_away() {
        let obs = NoOpObserver;
        let event = make_contention_event(1, 2, 3);
        obs.on_event(&event);
        // If this compiles and runs, it proves the no-op path works.
    }

    #[test]
    fn ring_buffer_basic_push_and_snapshot() {
        let rb = ConflictRingBuffer::new(3);
        assert!(rb.is_empty());

        rb.push(make_contention_event(1, 10, 20));
        rb.push(make_contention_event(2, 11, 21));
        assert_eq!(rb.len(), 2);

        let snap = rb.snapshot();
        assert_eq!(snap.len(), 2);
        assert!(
            matches!(&snap[0], ConflictEvent::PageLockContention { page, .. } if page.get() == 1)
        );
        assert!(
            matches!(&snap[1], ConflictEvent::PageLockContention { page, .. } if page.get() == 2)
        );
    }

    #[test]
    fn ring_buffer_wraps_on_overflow() {
        let rb = ConflictRingBuffer::new(2);

        rb.push(make_contention_event(1, 10, 20));
        rb.push(make_contention_event(2, 11, 21));
        rb.push(make_contention_event(3, 12, 22)); // overwrites first

        assert_eq!(rb.len(), 2);
        let snap = rb.snapshot();
        // Should contain events for pages 2 and 3 (oldest evicted)
        assert!(
            matches!(&snap[0], ConflictEvent::PageLockContention { page, .. } if page.get() == 2)
        );
        assert!(
            matches!(&snap[1], ConflictEvent::PageLockContention { page, .. } if page.get() == 3)
        );
    }

    #[test]
    fn ring_buffer_clear() {
        let rb = ConflictRingBuffer::new(10);
        rb.push(make_contention_event(1, 10, 20));
        rb.push(make_contention_event(2, 11, 21));
        assert_eq!(rb.len(), 2);

        rb.clear();
        assert!(rb.is_empty());
        assert!(rb.snapshot().is_empty());
    }

    #[test]
    fn ring_buffer_zero_capacity() {
        let rb = ConflictRingBuffer::new(0);
        rb.push(make_contention_event(1, 10, 20));
        assert!(rb.is_empty());
    }

    #[test]
    fn conflict_metrics_basic_recording() {
        let m = ConflictMetrics::new();

        m.record(&make_contention_event(1, 10, 20));
        m.record(&make_contention_event(1, 11, 20)); // same page
        m.record(&make_contention_event(2, 12, 20));

        assert_eq!(m.conflicts_total.load(Ordering::Relaxed), 3);
        assert_eq!(m.page_contentions.load(Ordering::Relaxed), 3);

        let hotspots = m.top_hotspots(5);
        assert_eq!(hotspots.len(), 2);
        assert_eq!(hotspots[0].0, page(1));
        assert_eq!(hotspots[0].1, 2);
    }

    #[test]
    fn conflict_metrics_fcw_recording() {
        let m = ConflictMetrics::new();

        m.record(&ConflictEvent::FcwBaseDrift {
            page: page(5),
            loser: txn(10),
            winner_commit_seq: CommitSeq::new(100),
            merge_attempted: true,
            merge_succeeded: true,
            timestamp_ns: 2000,
        });

        assert_eq!(m.fcw_drifts.load(Ordering::Relaxed), 1);
        assert_eq!(m.fcw_merge_attempts.load(Ordering::Relaxed), 1);
        assert_eq!(m.fcw_merge_successes.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn conflict_metrics_ssi_recording() {
        let m = ConflictMetrics::new();

        m.record(&ConflictEvent::SsiAbort {
            txn: TxnToken::new(txn(10), fsqlite_types::TxnEpoch::new(1)),
            reason: SsiAbortCategory::Pivot,
            in_edge_count: 1,
            out_edge_count: 1,
            timestamp_ns: 3000,
        });

        assert_eq!(m.ssi_aborts.load(Ordering::Relaxed), 1);
        assert_eq!(m.conflicts_total.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn conflict_metrics_reset() {
        let m = ConflictMetrics::new();
        m.record(&make_contention_event(1, 10, 20));
        assert_eq!(m.conflicts_total.load(Ordering::Relaxed), 1);

        m.reset();
        assert_eq!(m.conflicts_total.load(Ordering::Relaxed), 0);
        assert_eq!(m.page_contentions.load(Ordering::Relaxed), 0);
        assert!(m.top_hotspots(5).is_empty());
    }

    #[test]
    fn metrics_observer_records_both() {
        let obs = MetricsObserver::new(100);
        let event = make_contention_event(1, 10, 20);
        obs.on_event(&event);

        assert_eq!(obs.metrics().conflicts_total.load(Ordering::Relaxed), 1);
        assert_eq!(obs.log().len(), 1);
    }

    #[test]
    fn conflict_event_timestamp() {
        let event = make_contention_event(1, 10, 20);
        assert_eq!(event.timestamp_ns(), 1000);
    }

    #[test]
    fn conflict_event_is_conflict() {
        assert!(make_contention_event(1, 10, 20).is_conflict());
        assert!(
            !ConflictEvent::ConflictResolved {
                txn: txn(1),
                pages_merged: 0,
                commit_seq: CommitSeq::new(1),
                timestamp_ns: 0,
            }
            .is_conflict()
        );
    }

    #[test]
    fn metrics_snapshot_serializable() {
        let m = ConflictMetrics::new();
        m.record(&make_contention_event(1, 10, 20));
        let snap = m.snapshot();
        let json = serde_json::to_string(&snap).unwrap();
        assert!(json.contains("\"conflicts_total\":1"));
    }

    // ===================================================================
    // bd-t6sv2.1: Additional observability tests
    // ===================================================================

    #[test]
    fn ring_buffer_stress_many_pushes() {
        // Push far more events than capacity; verify only the last N survive.
        let cap = 10;
        let rb = ConflictRingBuffer::new(cap);
        for i in 1..=200_u32 {
            rb.push(make_contention_event(i, u64::from(i), u64::from(i) + 1));
        }
        assert_eq!(rb.len(), cap);
        let snap = rb.snapshot();
        assert_eq!(snap.len(), cap);
        // Oldest surviving event should be page 191.
        assert!(matches!(
            &snap[0],
            ConflictEvent::PageLockContention { page, .. } if page.get() == 191
        ),);
        // Newest should be page 200.
        assert!(matches!(
            &snap[cap - 1],
            ConflictEvent::PageLockContention { page, .. } if page.get() == 200
        ),);
    }

    #[test]
    fn ring_buffer_capacity_one() {
        // Edge case: capacity of 1 always holds the latest event.
        let rb = ConflictRingBuffer::new(1);
        rb.push(make_contention_event(1, 10, 20));
        rb.push(make_contention_event(2, 11, 21));
        rb.push(make_contention_event(3, 12, 22));
        assert_eq!(rb.len(), 1);
        let snap = rb.snapshot();
        assert!(
            matches!(&snap[0], ConflictEvent::PageLockContention { page, .. } if page.get() == 3)
        );
    }

    #[test]
    fn ring_buffer_clear_after_wrap() {
        // Ensure clear works correctly after the buffer has wrapped.
        let rb = ConflictRingBuffer::new(2);
        rb.push(make_contention_event(1, 10, 20));
        rb.push(make_contention_event(2, 11, 21));
        rb.push(make_contention_event(3, 12, 22)); // wrap
        assert_eq!(rb.len(), 2);

        rb.clear();
        assert!(rb.is_empty());
        assert_eq!(rb.capacity(), 2);

        // Re-use after clear.
        rb.push(make_contention_event(4, 13, 23));
        assert_eq!(rb.len(), 1);
        let snap = rb.snapshot();
        assert!(
            matches!(&snap[0], ConflictEvent::PageLockContention { page, .. } if page.get() == 4)
        );
    }

    #[test]
    fn metrics_all_fcw_merge_combinations() {
        // Test all four combinations of merge_attempted x merge_succeeded.
        let m = ConflictMetrics::new();

        let cases = [
            (false, false),
            (true, false),
            (true, true),
            (false, false), // duplicate no-merge
        ];
        for (attempted, succeeded) in cases {
            m.record(&ConflictEvent::FcwBaseDrift {
                page: page(1),
                loser: txn(1),
                winner_commit_seq: CommitSeq::new(1),
                merge_attempted: attempted,
                merge_succeeded: succeeded,
                timestamp_ns: 0,
            });
        }

        assert_eq!(m.fcw_drifts.load(Ordering::Relaxed), 4);
        assert_eq!(m.fcw_merge_attempts.load(Ordering::Relaxed), 2);
        assert_eq!(m.fcw_merge_successes.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn metrics_all_ssi_abort_categories() {
        let m = ConflictMetrics::new();

        for reason in [
            SsiAbortCategory::Pivot,
            SsiAbortCategory::CommittedPivot,
            SsiAbortCategory::MarkedForAbort,
        ] {
            m.record(&ConflictEvent::SsiAbort {
                txn: TxnToken::new(txn(1), fsqlite_types::TxnEpoch::new(1)),
                reason,
                in_edge_count: 1,
                out_edge_count: 1,
                timestamp_ns: 0,
            });
        }

        assert_eq!(m.ssi_aborts.load(Ordering::Relaxed), 3);
        assert_eq!(m.conflicts_total.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn trace_metrics_snapshot_and_reset() {
        reset_trace_metrics();
        record_trace_span_created();
        record_trace_span_created();
        record_trace_export(2, 17);
        record_trace_export_error();
        record_compat_trace_callback();

        let snapshot = trace_metrics_snapshot();
        assert_eq!(snapshot.fsqlite_trace_spans_total, 2);
        assert_eq!(snapshot.fsqlite_trace_export_errors_total, 1);
        assert_eq!(snapshot.fsqlite_compat_trace_callbacks_total, 1);

        reset_trace_metrics();
        let reset = trace_metrics_snapshot();
        assert_eq!(reset.fsqlite_trace_spans_total, 0);
        assert_eq!(reset.fsqlite_trace_export_errors_total, 0);
        assert_eq!(reset.fsqlite_compat_trace_callbacks_total, 0);
    }

    #[test]
    fn io_uring_latency_snapshot_and_reset() {
        reset_io_uring_latency_metrics();

        record_io_uring_read_latency(Duration::from_micros(40));
        record_io_uring_read_latency(Duration::from_micros(125));
        record_io_uring_write_latency(Duration::from_micros(55));
        record_io_uring_unix_fallback();

        let snapshot = io_uring_latency_snapshot();
        assert_eq!(snapshot.read_samples_total, 2);
        assert_eq!(snapshot.write_samples_total, 1);
        assert_eq!(snapshot.unix_fallbacks_total, 1);
        assert!(snapshot.read_tail_violations_total <= snapshot.read_samples_total);
        assert!(snapshot.write_tail_violations_total <= snapshot.write_samples_total);
        assert!(snapshot.read_p99_latency_us >= 125);
        assert!(snapshot.write_p99_latency_us >= 55);
        assert!(snapshot.read_conformal_upper_bound_us >= snapshot.read_p99_latency_us);
        assert!(snapshot.write_conformal_upper_bound_us >= snapshot.write_p99_latency_us);

        reset_io_uring_latency_metrics();
        let reset = io_uring_latency_snapshot();
        assert_eq!(reset.read_samples_total, 0);
        assert_eq!(reset.write_samples_total, 0);
        assert_eq!(reset.unix_fallbacks_total, 0);
        assert_eq!(reset.read_tail_violations_total, 0);
        assert_eq!(reset.write_tail_violations_total, 0);
        assert_eq!(reset.read_window_len, 0);
        assert_eq!(reset.write_window_len, 0);
    }

    #[test]
    fn io_uring_latency_conformal_upper_bound_is_tail_safe() {
        let metrics = IoUringLatencyMetrics::new(16);
        let mut saw_violation = false;
        for latency in [20_u64, 22, 21, 23, 20, 24, 26, 200] {
            if metrics.record_read_latency(Duration::from_micros(latency)) {
                saw_violation = true;
            }
        }

        let snapshot = metrics.snapshot();
        assert!(snapshot.read_p99_latency_us >= 200);
        assert!(snapshot.read_conformal_upper_bound_us >= snapshot.read_p99_latency_us);
        assert!(saw_violation);
        assert!(snapshot.read_tail_violations_total >= 1);
    }

    #[test]
    fn trace_and_decision_ids_are_monotonic() {
        let first_trace = next_trace_id();
        let second_trace = next_trace_id();
        assert!(second_trace > first_trace);

        let first_decision = next_decision_id();
        let second_decision = next_decision_id();
        assert!(second_decision > first_decision);
    }

    #[test]
    fn metrics_conflict_resolved_not_counted_as_conflict() {
        // ConflictResolved increments resolved counter but NOT conflicts_total.
        let m = ConflictMetrics::new();
        for i in 1..=5_u64 {
            m.record(&ConflictEvent::ConflictResolved {
                txn: txn(i),
                pages_merged: 2,
                commit_seq: CommitSeq::new(i * 10),
                timestamp_ns: 0,
            });
        }

        assert_eq!(m.conflicts_resolved.load(Ordering::Relaxed), 5);
        assert_eq!(m.conflicts_total.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn metrics_hotspot_ordering() {
        // Verify top_hotspots returns pages sorted by descending frequency.
        let m = ConflictMetrics::new();
        // Page 5: 3 contentions, page 10: 1, page 15: 2.
        for _ in 0..3 {
            m.record(&make_contention_event(5, 1, 2));
        }
        m.record(&make_contention_event(10, 1, 2));
        for _ in 0..2 {
            m.record(&make_contention_event(15, 1, 2));
        }

        let hotspots = m.top_hotspots(3);
        assert_eq!(hotspots.len(), 3);
        assert_eq!(hotspots[0], (page(5), 3));
        assert_eq!(hotspots[1], (page(15), 2));
        assert_eq!(hotspots[2], (page(10), 1));
    }

    #[test]
    fn metrics_hotspot_truncation() {
        // top_hotspots(N) should return at most N entries.
        let m = ConflictMetrics::new();
        for i in 1..=20_u32 {
            m.record(&make_contention_event(i, 1, 2));
        }
        assert_eq!(m.top_hotspots(5).len(), 5);
        assert_eq!(m.top_hotspots(0).len(), 0);
    }

    #[test]
    fn metrics_snapshot_all_fields() {
        // Verify snapshot captures all counter types accurately.
        let m = ConflictMetrics::new();
        m.record(&make_contention_event(1, 10, 20));
        m.record(&ConflictEvent::FcwBaseDrift {
            page: page(2),
            loser: txn(3),
            winner_commit_seq: CommitSeq::new(100),
            merge_attempted: true,
            merge_succeeded: false,
            timestamp_ns: 0,
        });
        m.record(&ConflictEvent::SsiAbort {
            txn: TxnToken::new(txn(4), fsqlite_types::TxnEpoch::new(1)),
            reason: SsiAbortCategory::Pivot,
            in_edge_count: 2,
            out_edge_count: 3,
            timestamp_ns: 0,
        });
        m.record(&ConflictEvent::ConflictResolved {
            txn: txn(5),
            pages_merged: 1,
            commit_seq: CommitSeq::new(200),
            timestamp_ns: 0,
        });

        let snap = m.snapshot();
        assert_eq!(snap.conflicts_total, 3); // contention + drift + abort
        assert_eq!(snap.page_contentions, 1);
        assert_eq!(snap.fcw_drifts, 1);
        assert_eq!(snap.fcw_merge_attempts, 1);
        assert_eq!(snap.fcw_merge_successes, 0);
        assert_eq!(snap.ssi_aborts, 1);
        assert_eq!(snap.conflicts_resolved, 1);
        assert!(snap.elapsed_secs >= 0.0);
    }

    #[test]
    fn metrics_observer_log_preserves_order() {
        // Events in the ring buffer are in chronological order.
        let obs = MetricsObserver::new(100);
        for i in 1..=5_u32 {
            obs.on_event(&make_contention_event(i, u64::from(i), u64::from(i) + 10));
        }

        let events = obs.log().snapshot();
        assert_eq!(events.len(), 5);
        for (idx, event) in events.iter().enumerate() {
            let expected_page = u32::try_from(idx + 1).unwrap();
            assert!(matches!(
                event,
                ConflictEvent::PageLockContention { page, .. } if page.get() == expected_page
            ),);
        }
    }

    #[test]
    fn metrics_observer_elapsed_ns_monotonic() {
        let obs = MetricsObserver::new(10);
        let t1 = obs.elapsed_ns();
        // Busy-wait briefly to ensure some time passes.
        std::thread::yield_now();
        let t2 = obs.elapsed_ns();
        assert!(t2 >= t1, "elapsed_ns must be monotonically non-decreasing");
    }

    #[test]
    fn conflict_event_serde_roundtrip() {
        // All event variants should serialize to JSON and back.
        let events = vec![
            make_contention_event(1, 2, 3),
            ConflictEvent::FcwBaseDrift {
                page: page(4),
                loser: txn(5),
                winner_commit_seq: CommitSeq::new(100),
                merge_attempted: true,
                merge_succeeded: true,
                timestamp_ns: 42,
            },
            ConflictEvent::SsiAbort {
                txn: TxnToken::new(txn(6), fsqlite_types::TxnEpoch::new(2)),
                reason: SsiAbortCategory::CommittedPivot,
                in_edge_count: 3,
                out_edge_count: 4,
                timestamp_ns: 99,
            },
            ConflictEvent::ConflictResolved {
                txn: txn(7),
                pages_merged: 5,
                commit_seq: CommitSeq::new(200),
                timestamp_ns: 123,
            },
        ];

        for event in &events {
            let json = serde_json::to_string(event).unwrap();
            assert!(!json.is_empty(), "serialization should produce output");
        }
    }

    #[test]
    fn conflict_event_is_conflict_all_variants() {
        assert!(make_contention_event(1, 2, 3).is_conflict());

        assert!(
            ConflictEvent::FcwBaseDrift {
                page: page(1),
                loser: txn(1),
                winner_commit_seq: CommitSeq::new(1),
                merge_attempted: false,
                merge_succeeded: false,
                timestamp_ns: 0,
            }
            .is_conflict()
        );

        assert!(
            ConflictEvent::SsiAbort {
                txn: TxnToken::new(txn(1), fsqlite_types::TxnEpoch::new(1)),
                reason: SsiAbortCategory::Pivot,
                in_edge_count: 0,
                out_edge_count: 0,
                timestamp_ns: 0,
            }
            .is_conflict()
        );

        assert!(
            !ConflictEvent::ConflictResolved {
                txn: txn(1),
                pages_merged: 0,
                commit_seq: CommitSeq::new(1),
                timestamp_ns: 0,
            }
            .is_conflict()
        );
    }

    // ===================================================================
    // bd-2g5.6.1: Cx propagation telemetry tests
    // ===================================================================

    #[test]
    fn cx_propagation_metrics_basic() {
        GLOBAL_CX_PROPAGATION_METRICS.reset();

        GLOBAL_CX_PROPAGATION_METRICS.record_propagation_success();
        GLOBAL_CX_PROPAGATION_METRICS.record_propagation_success();
        GLOBAL_CX_PROPAGATION_METRICS.record_propagation_failure("test_site_1");
        GLOBAL_CX_PROPAGATION_METRICS.record_cancellation_cleanup();
        GLOBAL_CX_PROPAGATION_METRICS.record_trace_linkage();
        GLOBAL_CX_PROPAGATION_METRICS.record_cx_created();
        GLOBAL_CX_PROPAGATION_METRICS.record_cancel_propagation();

        let snap = GLOBAL_CX_PROPAGATION_METRICS.snapshot();
        assert_eq!(snap.propagation_successes_total, 2);
        assert_eq!(snap.propagation_failures_total, 1);
        assert_eq!(snap.cancellation_cleanups_total, 1);
        assert_eq!(snap.trace_linkages_total, 1);
        assert_eq!(snap.cx_created_total, 1);
        assert_eq!(snap.cancel_propagations_total, 1);
    }

    #[test]
    fn cx_propagation_metrics_reset() {
        GLOBAL_CX_PROPAGATION_METRICS.reset();
        GLOBAL_CX_PROPAGATION_METRICS.record_propagation_success();
        GLOBAL_CX_PROPAGATION_METRICS.record_propagation_failure("reset_test");
        assert!(
            GLOBAL_CX_PROPAGATION_METRICS
                .propagation_successes_total
                .load(Ordering::Relaxed)
                > 0
        );

        GLOBAL_CX_PROPAGATION_METRICS.reset();
        let snap = GLOBAL_CX_PROPAGATION_METRICS.snapshot();
        assert_eq!(snap.propagation_successes_total, 0);
        assert_eq!(snap.propagation_failures_total, 0);
        assert_eq!(snap.cancellation_cleanups_total, 0);
        assert_eq!(snap.trace_linkages_total, 0);
        assert_eq!(snap.cx_created_total, 0);
        assert_eq!(snap.cancel_propagations_total, 0);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn cx_propagation_failure_ratio() {
        let m = CxPropagationMetrics::new();

        // Zero total → 0.0 ratio.
        assert_eq!(m.snapshot().failure_ratio(), 0.0);

        // 1 success, 0 failures → 0.0 ratio.
        m.record_propagation_success();
        assert!((m.snapshot().failure_ratio() - 0.0).abs() < f64::EPSILON);

        // 1 success, 1 failure → 0.5 ratio.
        m.record_propagation_failure("ratio_test");
        assert!((m.snapshot().failure_ratio() - 0.5).abs() < f64::EPSILON);

        // 1 success, 3 failures → 0.75 ratio.
        m.record_propagation_failure("ratio_test");
        m.record_propagation_failure("ratio_test");
        assert!((m.snapshot().failure_ratio() - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn cx_propagation_snapshot_display() {
        let m = CxPropagationMetrics::new();
        m.record_propagation_success();
        m.record_propagation_success();
        m.record_propagation_failure("display_test");
        let display = format!("{}", m.snapshot());
        assert!(display.contains("ok=2"));
        assert!(display.contains("fail=1"));
        assert!(display.contains("fail_ratio="));
    }

    #[test]
    fn cx_propagation_snapshot_serializable() {
        let m = CxPropagationMetrics::new();
        m.record_propagation_success();
        m.record_trace_linkage();
        let snap = m.snapshot();
        let json = serde_json::to_string(&snap).unwrap();
        assert!(json.contains("\"propagation_successes_total\":1"));
        assert!(json.contains("\"trace_linkages_total\":1"));
    }

    #[test]
    fn cx_propagation_independent_counters() {
        // Each counter increments independently.
        let m = CxPropagationMetrics::new();
        for _ in 0..5 {
            m.record_propagation_success();
        }
        for _ in 0..3 {
            m.record_cancellation_cleanup();
        }
        m.record_cx_created();
        m.record_cx_created();

        let snap = m.snapshot();
        assert_eq!(snap.propagation_successes_total, 5);
        assert_eq!(snap.propagation_failures_total, 0);
        assert_eq!(snap.cancellation_cleanups_total, 3);
        assert_eq!(snap.trace_linkages_total, 0);
        assert_eq!(snap.cx_created_total, 2);
        assert_eq!(snap.cancel_propagations_total, 0);
    }

    #[test]
    fn cx_propagation_concurrent_safety() {
        // Multiple threads can record concurrently without panicking.
        let m = &CxPropagationMetrics::new();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
        std::thread::scope(|s| {
            for _ in 0..4 {
                let b = barrier.clone();
                s.spawn(move || {
                    b.wait();
                    for _ in 0..100 {
                        m.record_propagation_success();
                        m.record_propagation_failure("concurrent_test");
                        m.record_cancellation_cleanup();
                        m.record_trace_linkage();
                        m.record_cx_created();
                        m.record_cancel_propagation();
                    }
                });
            }
        });

        let snap = m.snapshot();
        assert_eq!(snap.propagation_successes_total, 400);
        assert_eq!(snap.propagation_failures_total, 400);
        assert_eq!(snap.cancellation_cleanups_total, 400);
        assert_eq!(snap.trace_linkages_total, 400);
        assert_eq!(snap.cx_created_total, 400);
        assert_eq!(snap.cancel_propagations_total, 400);
    }

    // ===================================================================
    // bd-2g5.1: TxnSlot telemetry tests
    // ===================================================================

    #[test]
    fn txn_slot_metrics_alloc_release_and_crash() {
        let m = TxnSlotMetrics::new();

        m.record_slot_allocated(3, 1001);
        m.record_slot_allocated(4, 1001);
        m.record_crash_detected(Some(4), 1001, 42);
        m.record_slot_released(Some(4), 1001);

        let snap = m.snapshot();
        assert_eq!(snap.fsqlite_txn_slots_active, 1);
        assert_eq!(snap.fsqlite_txn_slot_crashes_detected_total, 1);
    }

    #[test]
    fn txn_slot_metrics_release_saturates_at_zero() {
        let m = TxnSlotMetrics::new();

        // Releasing without prior alloc should never underflow.
        m.record_slot_released(None, 0);
        m.record_slot_released(None, 0);

        let snap = m.snapshot();
        assert_eq!(snap.fsqlite_txn_slots_active, 0);
        assert_eq!(snap.fsqlite_txn_slot_crashes_detected_total, 0);
    }

    #[test]
    fn txn_slot_metrics_snapshot_display_and_serde() {
        let m = TxnSlotMetrics::new();
        m.record_slot_allocated(7, 2222);
        m.record_crash_detected(None, 2222, 9001);

        let snap = m.snapshot();
        let display = format!("{snap}");
        assert!(display.contains("txn_slots(active=1 crashes=1)"));

        let json = serde_json::to_string(&snap).unwrap();
        assert!(json.contains("\"fsqlite_txn_slots_active\":1"));
        assert!(json.contains("\"fsqlite_txn_slot_crashes_detected_total\":1"));
    }
}
