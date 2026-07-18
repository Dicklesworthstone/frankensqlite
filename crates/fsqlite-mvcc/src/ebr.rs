//! Epoch-based reclamation scaffolding for MVCC version-chain GC.
//!
//! This module provides a safe wrapper around `crossbeam-epoch` pin/unpin
//! semantics so transaction lifecycle hooks can carry a `VersionGuard` without
//! exposing raw epoch internals.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use crossbeam_epoch::{Collector, Guard, LocalHandle};
use fsqlite_types::sync_primitives::Mutex;
use serde::Serialize;

// ---------------------------------------------------------------------------
// EBR metrics (bd-688.4)
// ---------------------------------------------------------------------------

/// Global EBR metrics singleton.
///
/// Tracks epoch-based reclamation activity across all `VersionGuard` and
/// `VersionGuardTicket` instances. Counters are lock-free `AtomicU64` with
/// `Relaxed` ordering — callers may observe stale reads but never torn values.
pub static GLOBAL_EBR_METRICS: EbrMetrics = EbrMetrics::new();

/// Atomic counters for EBR version-chain garbage collection telemetry.
pub struct EbrMetrics {
    /// Total version objects actually deferred through Crossbeam EBR via
    /// `defer_retire` / `defer_retire_with`.
    pub retirements_deferred_total: AtomicU64,
    /// Total explicit `flush()` calls that push deferred retirements toward
    /// execution.
    pub flush_calls_total: AtomicU64,
    /// Total epoch pins created (`VersionGuard::pin` + ticket-scoped pins).
    pub guards_pinned_total: AtomicU64,
    /// Total epoch pins dropped (guards unpinned).
    pub guards_unpinned_total: AtomicU64,
    /// Total stale-reader warnings emitted.
    pub stale_reader_warnings_total: AtomicU64,
    /// High-water mark of concurrently active guards observed.
    pub active_guards_high_water: AtomicU64,
    /// Maximum version-chain length observed at write-time.
    pub max_chain_length_observed: AtomicU64,
    /// Number of recorded chain-length samples.
    pub chain_length_samples_total: AtomicU64,
    /// Sum of recorded chain-length samples.
    pub chain_length_sum_total: AtomicU64,
    /// Total versions freed by eager chain-bound GC passes.
    pub gc_freed_count: AtomicU64,
    /// Number of times chain-bound backpressure could not be relieved in time.
    pub gc_blocked_count: AtomicU64,
}

impl EbrMetrics {
    /// Create a new metrics instance with all counters at zero.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            retirements_deferred_total: AtomicU64::new(0),
            flush_calls_total: AtomicU64::new(0),
            guards_pinned_total: AtomicU64::new(0),
            guards_unpinned_total: AtomicU64::new(0),
            stale_reader_warnings_total: AtomicU64::new(0),
            active_guards_high_water: AtomicU64::new(0),
            max_chain_length_observed: AtomicU64::new(0),
            chain_length_samples_total: AtomicU64::new(0),
            chain_length_sum_total: AtomicU64::new(0),
            gc_freed_count: AtomicU64::new(0),
            gc_blocked_count: AtomicU64::new(0),
        }
    }

    /// Record an actual Crossbeam deferred retirement.
    pub fn record_retirement_deferred(&self) {
        self.retirements_deferred_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a flush call.
    pub fn record_flush(&self) {
        self.flush_calls_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a guard pin event and update the high-water mark.
    pub fn record_guard_pinned(&self, current_active: u64) {
        self.guards_pinned_total.fetch_add(1, Ordering::Relaxed);
        self.active_guards_high_water
            .fetch_max(current_active, Ordering::Relaxed);
    }

    /// Record a guard unpin event.
    pub fn record_guard_unpinned(&self) {
        self.guards_unpinned_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record stale-reader warnings emitted.
    pub fn record_stale_warnings(&self, count: u64) {
        self.stale_reader_warnings_total
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Record a version-chain length sample.
    pub fn record_chain_length_sample(&self, chain_len: u64) {
        self.chain_length_samples_total
            .fetch_add(1, Ordering::Relaxed);
        self.chain_length_sum_total
            .fetch_add(chain_len, Ordering::Relaxed);
        self.max_chain_length_observed
            .fetch_max(chain_len, Ordering::Relaxed);
    }

    /// Record versions freed during eager chain-bound GC.
    pub fn record_gc_freed(&self, count: u64) {
        self.gc_freed_count.fetch_add(count, Ordering::Relaxed);
    }

    /// Record a chain-bound backpressure event.
    pub fn record_gc_blocked(&self) {
        self.gc_blocked_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Read a point-in-time snapshot.
    #[must_use]
    pub fn snapshot(&self) -> EbrMetricsSnapshot {
        EbrMetricsSnapshot {
            retirements_deferred_total: self.retirements_deferred_total.load(Ordering::Relaxed),
            flush_calls_total: self.flush_calls_total.load(Ordering::Relaxed),
            guards_pinned_total: self.guards_pinned_total.load(Ordering::Relaxed),
            guards_unpinned_total: self.guards_unpinned_total.load(Ordering::Relaxed),
            stale_reader_warnings_total: self.stale_reader_warnings_total.load(Ordering::Relaxed),
            active_guards_high_water: self.active_guards_high_water.load(Ordering::Relaxed),
            max_chain_length_observed: self.max_chain_length_observed.load(Ordering::Relaxed),
            chain_length_samples_total: self.chain_length_samples_total.load(Ordering::Relaxed),
            chain_length_sum_total: self.chain_length_sum_total.load(Ordering::Relaxed),
            gc_freed_count: self.gc_freed_count.load(Ordering::Relaxed),
            gc_blocked_count: self.gc_blocked_count.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero (tests/diagnostics).
    pub fn reset(&self) {
        self.retirements_deferred_total.store(0, Ordering::Relaxed);
        self.flush_calls_total.store(0, Ordering::Relaxed);
        self.guards_pinned_total.store(0, Ordering::Relaxed);
        self.guards_unpinned_total.store(0, Ordering::Relaxed);
        self.stale_reader_warnings_total.store(0, Ordering::Relaxed);
        self.active_guards_high_water.store(0, Ordering::Relaxed);
        self.max_chain_length_observed.store(0, Ordering::Relaxed);
        self.chain_length_samples_total.store(0, Ordering::Relaxed);
        self.chain_length_sum_total.store(0, Ordering::Relaxed);
        self.gc_freed_count.store(0, Ordering::Relaxed);
        self.gc_blocked_count.store(0, Ordering::Relaxed);
    }
}

impl Default for EbrMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Serializable snapshot of EBR metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct EbrMetricsSnapshot {
    pub retirements_deferred_total: u64,
    pub flush_calls_total: u64,
    pub guards_pinned_total: u64,
    pub guards_unpinned_total: u64,
    pub stale_reader_warnings_total: u64,
    pub active_guards_high_water: u64,
    pub max_chain_length_observed: u64,
    pub chain_length_samples_total: u64,
    pub chain_length_sum_total: u64,
    pub gc_freed_count: u64,
    pub gc_blocked_count: u64,
}

impl EbrMetricsSnapshot {
    /// Average sampled chain length.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn avg_chain_length(self) -> f64 {
        if self.chain_length_samples_total == 0 {
            0.0
        } else {
            self.chain_length_sum_total as f64 / self.chain_length_samples_total as f64
        }
    }
}

impl std::fmt::Display for EbrMetricsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ebr(retired={} flushed={} pinned={} unpinned={} stale_warn={} hw={} chain_max={} chain_avg={:.2} gc_freed={} gc_blocked={})",
            self.retirements_deferred_total,
            self.flush_calls_total,
            self.guards_pinned_total,
            self.guards_unpinned_total,
            self.stale_reader_warnings_total,
            self.active_guards_high_water,
            self.max_chain_length_observed,
            self.avg_chain_length(),
            self.gc_freed_count,
            self.gc_blocked_count,
        )
    }
}

/// Default cap on the number of pending versions queued for a single page
/// while a stale reader still pins an older snapshot.
///
/// When a version chain for a page exceeds this cap with at least one pinned
/// reader holding an older `commit_seq`, the MVCC layer force-aborts the
/// pinned reader via [`VersionGuardRegistry::mark_force_abort`], making the
/// force-abort status observable on the reader's next access.
/// This prevents the OOM failure mode documented in `bd-wt4uu` (unbounded
/// version-chain growth under long-lived readers at 3am prod load).
pub const DEFAULT_MAX_PENDING_VERSIONS_PER_PAGE: usize = 4096;

/// Configuration for stale-reader detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StaleReaderConfig {
    /// Reader pins older than this duration are considered stale.
    pub warn_after: Duration,
    /// Minimum interval between repeated warnings for the same guard.
    pub warn_every: Duration,
    /// Maximum number of versions pending in a single page's chain while a
    /// stale reader still pins an older `commit_seq`.
    ///
    /// When exceeded, the offending reader is force-aborted rather than
    /// letting the chain grow to OOM. See the `bd-wt4uu` design doc.
    pub max_pending_versions_per_page: usize,
}

impl Default for StaleReaderConfig {
    fn default() -> Self {
        Self {
            warn_after: Duration::from_secs(30),
            warn_every: Duration::from_secs(5),
            max_pending_versions_per_page: DEFAULT_MAX_PENDING_VERSIONS_PER_PAGE,
        }
    }
}

/// Snapshot of an active stale reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReaderPinSnapshot {
    /// Stable ID assigned to the pinned guard.
    pub guard_id: u64,
    /// Elapsed pin duration.
    pub pinned_for: Duration,
    /// EBR epoch pinned by this reader (monotonic counter; not a `CommitSeq`).
    pub pinned_epoch: u64,
    /// `CommitSeq` captured by the reader at snapshot time, if known.
    ///
    /// When present, writers MUST NOT advance the GC horizon past this value
    /// without force-aborting this reader first. A `None` value means the
    /// reader has not reported its snapshot seq yet (e.g. transient pins that
    /// don't participate in MVCC horizon gating).
    pub pinned_commit_seq: Option<u64>,
    /// Whether this reader has been marked for force-abort due to stale-reader
    /// pressure (bd-wt4uu).
    pub force_abort: bool,
}

#[derive(Debug, Clone, Copy)]
struct ReaderPinState {
    pinned_at: Instant,
    last_warned_at: Option<Instant>,
    pinned_epoch: u64,
    pinned_commit_seq: Option<u64>,
    force_abort: bool,
}

/// Registry for active epoch pins (`VersionGuard`s).
///
/// The registry is intentionally lock-based and simple for the initial
/// integration slice; cardinality is bounded by active transactions.
#[derive(Debug)]
pub struct VersionGuardRegistry {
    /// Reclamation domain for this MVCC registry. Separate databases/stores
    /// must not delay one another's grace periods through Crossbeam's
    /// process-global default collector.
    collector: Collector,
    stale_reader: StaleReaderConfig,
    next_guard_id: AtomicU64,
    global_epoch: AtomicU64,
    active: Mutex<HashMap<u64, ReaderPinState>>,
}

impl VersionGuardRegistry {
    /// Construct a registry with the provided stale-reader policy.
    #[must_use]
    pub fn new(stale_reader: StaleReaderConfig) -> Self {
        Self {
            collector: Collector::new(),
            stale_reader,
            next_guard_id: AtomicU64::new(1),
            global_epoch: AtomicU64::new(0),
            active: Mutex::new(HashMap::new()),
        }
    }

    /// Stale-reader policy currently in use.
    #[must_use]
    pub const fn stale_reader_config(&self) -> StaleReaderConfig {
        self.stale_reader
    }

    /// Number of currently pinned guards.
    #[must_use]
    pub fn active_guard_count(&self) -> usize {
        self.active.lock().len()
    }

    /// Current global EBR epoch.
    #[must_use]
    pub fn current_epoch(&self) -> u64 {
        self.global_epoch.load(Ordering::Acquire)
    }

    /// Advance the global EBR epoch and return the new value.
    pub fn advance_epoch(&self) -> u64 {
        self.global_epoch.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Ensure the global EBR epoch is at least `target`, returning the observed value.
    pub fn advance_epoch_to(&self, target: u64) -> u64 {
        let mut observed = self.current_epoch();
        while observed < target {
            match self.global_epoch.compare_exchange_weak(
                observed,
                target,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return target,
                Err(actual) => observed = actual,
            }
        }
        observed
    }

    /// Prompt this registry's private Crossbeam collector to advance and
    /// flush locally deferred retirements.
    #[cfg(test)]
    pub(crate) fn flush_reclamation(&self) {
        let handle = self.collector.register();
        let guard = handle.pin();
        guard.flush();
    }

    /// Minimum pinned epoch among currently active guards.
    #[must_use]
    pub fn min_pinned_epoch(&self) -> Option<u64> {
        self.active
            .lock()
            .values()
            .map(|state| state.pinned_epoch)
            .min()
    }

    /// Snapshot all stale readers as of `now`.
    ///
    /// Returns one entry per currently pinned reader whose pin age is at
    /// least [`StaleReaderConfig::warn_after`]. The entry carries the reader's
    /// `pinned_epoch`, optional `pinned_commit_seq`, and `force_abort` flag
    /// so that writers can synchronously consult pin state before advancing
    /// the GC horizon (bd-wt4uu).
    #[must_use]
    pub fn stale_reader_snapshots(&self, now: Instant) -> Vec<ReaderPinSnapshot> {
        self.active
            .lock()
            .iter()
            .filter_map(|(&guard_id, state)| {
                let pinned_for = now.saturating_duration_since(state.pinned_at);
                if pinned_for >= self.stale_reader.warn_after {
                    Some(ReaderPinSnapshot {
                        guard_id,
                        pinned_for,
                        pinned_epoch: state.pinned_epoch,
                        pinned_commit_seq: state.pinned_commit_seq,
                        force_abort: state.force_abort,
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    /// Snapshot every currently pinned reader (regardless of staleness age).
    ///
    /// Unlike [`Self::stale_reader_snapshots`], this returns every active pin.
    /// Used by the horizon-cap path in `cleanup_and_raise_gc_horizon` so that
    /// even short-lived pins can clamp the horizon when they declared a
    /// snapshot seq. Returns an empty `Vec` if no readers are pinned.
    #[must_use]
    pub fn all_reader_pins(&self) -> Vec<ReaderPinSnapshot> {
        let now = Instant::now();
        self.active
            .lock()
            .iter()
            .map(|(&guard_id, state)| ReaderPinSnapshot {
                guard_id,
                pinned_for: now.saturating_duration_since(state.pinned_at),
                pinned_epoch: state.pinned_epoch,
                pinned_commit_seq: state.pinned_commit_seq,
                force_abort: state.force_abort,
            })
            .collect()
    }

    /// Minimum `pinned_commit_seq` across currently active readers.
    ///
    /// Returns `None` if no reader has declared a snapshot seq. A `Some(seq)`
    /// return means: any version visible only at `CommitSeq < seq` is still
    /// reachable; the GC horizon MUST NOT advance past `seq - 1` without
    /// force-aborting the corresponding reader (bd-wt4uu).
    #[must_use]
    pub fn min_pinned_commit_seq(&self) -> Option<u64> {
        self.active
            .lock()
            .values()
            .filter_map(|state| state.pinned_commit_seq)
            .min()
    }

    /// Attach a snapshot `CommitSeq` to an already-pinned guard.
    ///
    /// Called by [`VersionGuard`] / [`VersionGuardTicket`] owners once their
    /// transaction's `begin_seq` is known. Idempotent: later calls with the
    /// same or newer seq are no-ops; monotonic behaviour is not enforced here
    /// because a single guard corresponds to one transaction's lifetime.
    pub fn set_pinned_commit_seq(&self, guard_id: u64, commit_seq: u64) {
        if let Some(state) = self.active.lock().get_mut(&guard_id) {
            state.pinned_commit_seq = Some(commit_seq);
        }
    }

    /// Mark a pinned guard as force-aborted due to stale-reader pressure.
    ///
    /// The reader can observe this flag via [`VersionGuard::is_force_aborted`]
    /// or [`VersionGuardTicket::is_force_aborted`] and surface an error to
    /// the caller. Idempotent. Returns `true` if the guard was present and
    /// transitioned from `false` to `true`, `false` otherwise.
    pub fn mark_force_abort(&self, guard_id: u64) -> bool {
        self.active.lock().get_mut(&guard_id).is_some_and(|state| {
            if state.force_abort {
                false
            } else {
                state.force_abort = true;
                true
            }
        })
    }

    /// Query whether a pinned guard has been force-aborted.
    #[must_use]
    pub fn is_force_aborted(&self, guard_id: u64) -> bool {
        self.active
            .lock()
            .get(&guard_id)
            .is_some_and(|state| state.force_abort)
    }

    /// Emit stale-reader warnings as of `now`.
    ///
    /// Returns the number of warnings emitted.
    pub fn warn_on_stale_readers(&self, now: Instant) -> usize {
        let mut warned = 0_usize;
        let mut active = self.active.lock();
        for (&guard_id, state) in active.iter_mut() {
            let pinned_for = now.saturating_duration_since(state.pinned_at);
            if pinned_for < self.stale_reader.warn_after {
                continue;
            }

            let should_warn = state.last_warned_at.is_none_or(|last| {
                now.saturating_duration_since(last) >= self.stale_reader.warn_every
            });
            if should_warn {
                tracing::warn!(
                    guard_id,
                    pinned_for_ms = pinned_for.as_millis(),
                    stale_warn_after_ms = self.stale_reader.warn_after.as_millis(),
                    "stale MVCC reader pin is blocking epoch advancement"
                );
                state.last_warned_at = Some(now);
                warned += 1;
            }
        }
        drop(active);
        if warned > 0 {
            GLOBAL_EBR_METRICS.record_stale_warnings(warned as u64);
        }
        warned
    }

    fn register_guard(&self, pinned_at: Instant) -> u64 {
        let guard_id = self.next_guard_id.fetch_add(1, Ordering::Relaxed);
        let mut active = self.active.lock();
        let pinned_epoch = self.current_epoch();
        active.insert(
            guard_id,
            ReaderPinState {
                pinned_at,
                last_warned_at: None,
                pinned_epoch,
                pinned_commit_seq: None,
                force_abort: false,
            },
        );
        guard_id
    }

    fn unregister_guard(&self, guard_id: u64) -> Option<Duration> {
        self.active
            .lock()
            .remove(&guard_id)
            .map(|state| state.pinned_at.elapsed())
    }
}

impl Default for VersionGuardRegistry {
    fn default() -> Self {
        Self::new(StaleReaderConfig::default())
    }
}

/// Transaction-scoped epoch pin.
///
/// Construct at transaction begin and drop at transaction end (commit or
/// abort). Retirements deferred through this guard are only reclaimed after all
/// currently pinned readers have unpinned.
#[derive(Debug)]
pub struct VersionGuard {
    // Field order is deliberate: the Guard must unpin before its LocalHandle
    // unregisters, and both must drop before the registry-owned Collector.
    guard: Guard,
    _epoch_handle: LocalHandle,
    registry: Arc<VersionGuardRegistry>,
    guard_id: u64,
    pinned_at: Instant,
}

impl VersionGuard {
    /// Pin the current thread into the epoch domain.
    #[must_use]
    pub fn pin(registry: Arc<VersionGuardRegistry>) -> Self {
        let pinned_at = Instant::now();
        let guard_id = registry.register_guard(pinned_at);
        let epoch_handle = registry.collector.register();
        let guard = epoch_handle.pin();
        let active_count = registry.active_guard_count() as u64;
        GLOBAL_EBR_METRICS.record_guard_pinned(active_count);
        tracing::trace!(
            target: "fsqlite_mvcc::ebr",
            guard_id,
            active_guards = active_count,
            "epoch guard pinned"
        );
        Self {
            guard,
            _epoch_handle: epoch_handle,
            registry,
            guard_id,
            pinned_at,
        }
    }

    /// Stable ID for diagnostics and stale-reader reporting.
    #[must_use]
    pub const fn guard_id(&self) -> u64 {
        self.guard_id
    }

    /// Elapsed pin duration.
    #[must_use]
    pub fn pinned_for(&self) -> Duration {
        self.pinned_at.elapsed()
    }

    /// Attach a snapshot `CommitSeq` so writers can clamp the GC horizon at
    /// commit finalization (bd-wt4uu).
    pub fn set_pinned_commit_seq(&self, commit_seq: u64) {
        self.registry
            .set_pinned_commit_seq(self.guard_id, commit_seq);
    }

    /// Returns `true` if this guard has been force-aborted by the writer
    /// due to stale-reader pressure (bd-wt4uu).
    #[must_use]
    pub fn is_force_aborted(&self) -> bool {
        self.registry.is_force_aborted(self.guard_id)
    }

    /// Defer retirement of an owned value until it is safe to reclaim.
    pub fn defer_retire<T>(&self, retired: T)
    where
        T: Send + 'static,
    {
        GLOBAL_EBR_METRICS.record_retirement_deferred();
        self.guard.defer(move || drop(retired));
    }

    /// Defer an arbitrary retirement closure.
    pub fn defer_retire_with<F, R>(&self, retire: F)
    where
        F: FnOnce() -> R + Send + 'static,
    {
        GLOBAL_EBR_METRICS.record_retirement_deferred();
        self.guard.defer(retire);
    }

    /// Flush local deferred-retirement queue toward execution.
    ///
    /// Actual execution still depends on epoch advancement and active readers.
    pub fn flush(&self) {
        GLOBAL_EBR_METRICS.record_flush();
        self.guard.flush();
    }
}

impl Drop for VersionGuard {
    fn drop(&mut self) {
        GLOBAL_EBR_METRICS.record_guard_unpinned();
        let pinned_for = self
            .registry
            .unregister_guard(self.guard_id)
            .unwrap_or_else(|| self.pinned_at.elapsed());
        tracing::trace!(
            target: "fsqlite_mvcc::ebr",
            guard_id = self.guard_id,
            pinned_for_us = pinned_for.as_micros(),
            "epoch guard unpinned"
        );
        if pinned_for >= self.registry.stale_reader_config().warn_after {
            tracing::warn!(
                guard_id = self.guard_id,
                pinned_for_ms = pinned_for.as_millis(),
                stale_warn_after_ms = self.registry.stale_reader_config().warn_after.as_millis(),
                "MVCC reader pin ended after stale threshold"
            );
        }
    }
}

/// Send-safe transaction-scoped epoch registration.
///
/// Unlike [`VersionGuard`], a ticket does not hold a thread-local
/// `crossbeam-epoch::Guard`.  This makes it `Send + Sync` so it can live
/// inside a [`crate::Transaction`] that may be moved between threads (async
/// workloads, thread pools, etc.).
///
/// Stale-reader detection and epoch-advancement tracking still work because the
/// ticket is registered in the [`VersionGuardRegistry`] for its entire
/// lifetime.  Actual epoch pinning for deferred retirement happens via
/// short-lived [`VersionGuard`]s at the point where version chains are
/// traversed or old versions are freed.
#[derive(Debug)]
pub struct VersionGuardTicket {
    registry: Arc<VersionGuardRegistry>,
    guard_id: u64,
    pinned_at: Instant,
}

impl VersionGuardTicket {
    /// Register a transaction-scoped ticket.
    #[must_use]
    pub fn register(registry: Arc<VersionGuardRegistry>) -> Self {
        let pinned_at = Instant::now();
        let guard_id = registry.register_guard(pinned_at);
        let active_count = registry.active_guard_count() as u64;
        GLOBAL_EBR_METRICS.record_guard_pinned(active_count);
        tracing::trace!(
            target: "fsqlite_mvcc::ebr",
            guard_id,
            active_guards = active_count,
            "epoch ticket registered"
        );
        Self {
            registry,
            guard_id,
            pinned_at,
        }
    }

    /// Stable ID for diagnostics and stale-reader reporting.
    #[must_use]
    pub const fn guard_id(&self) -> u64 {
        self.guard_id
    }

    /// Elapsed registration duration.
    #[must_use]
    pub fn registered_for(&self) -> Duration {
        self.pinned_at.elapsed()
    }

    /// Reference to the owning registry.
    #[must_use]
    pub fn registry(&self) -> &Arc<VersionGuardRegistry> {
        &self.registry
    }

    /// Attach a snapshot `CommitSeq` so writers can clamp the GC horizon at
    /// commit finalization (bd-wt4uu).
    pub fn set_pinned_commit_seq(&self, commit_seq: u64) {
        self.registry
            .set_pinned_commit_seq(self.guard_id, commit_seq);
    }

    /// Returns `true` if this ticket has been force-aborted by the writer
    /// due to stale-reader pressure (bd-wt4uu).
    #[must_use]
    pub fn is_force_aborted(&self) -> bool {
        self.registry.is_force_aborted(self.guard_id)
    }

    /// Pin the current thread's epoch and defer retirement of a value.
    ///
    /// The short-lived epoch pin ensures correctness: the deferred value is
    /// only reclaimed after all concurrently pinned readers have advanced
    /// past the current epoch.
    pub fn defer_retire<T: Send + 'static>(&self, retired: T) {
        GLOBAL_EBR_METRICS.record_retirement_deferred();
        let handle = self.registry.collector.register();
        let guard = handle.pin();
        guard.defer(move || drop(retired));
        guard.flush();
    }

    /// Pin the current thread's epoch and defer an arbitrary closure.
    pub fn defer_retire_with<F, R>(&self, retire: F)
    where
        F: FnOnce() -> R + Send + 'static,
    {
        GLOBAL_EBR_METRICS.record_retirement_deferred();
        let handle = self.registry.collector.register();
        let guard = handle.pin();
        guard.defer(retire);
        guard.flush();
    }
}

impl Drop for VersionGuardTicket {
    fn drop(&mut self) {
        GLOBAL_EBR_METRICS.record_guard_unpinned();
        let pinned_for = self
            .registry
            .unregister_guard(self.guard_id)
            .unwrap_or_else(|| self.pinned_at.elapsed());
        tracing::trace!(
            target: "fsqlite_mvcc::ebr",
            guard_id = self.guard_id,
            registered_for_us = pinned_for.as_micros(),
            "epoch ticket unregistered"
        );
        if pinned_for >= self.registry.stale_reader_config().warn_after {
            tracing::warn!(
                guard_id = self.guard_id,
                pinned_for_ms = pinned_for.as_millis(),
                stale_warn_after_ms = self.registry.stale_reader_config().warn_after.as_millis(),
                "MVCC reader registration ended after stale threshold"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// EbrRetireQueue (D5: Epoch-Based Reclamation for version chain GC)
// ---------------------------------------------------------------------------

use crate::core_types::VersionIdx;

/// Queue of version slots pending reclamation after epoch advancement.
///
/// When GC retires a version via `VersionArena::take_for_retirement()`, the
/// slot index is added to this queue. After sufficient epoch advancement
/// (all readers have unpinned since the retirement), the indices are passed
/// to `VersionArena::recycle_slots()` to make them available for reallocation.
///
/// # Thread Safety
///
/// The queue uses internal locking for thread-safe append and drain operations.
/// This is acceptable because:
/// 1. Append is O(1) and the lock is held very briefly
/// 2. Drain happens infrequently (after epoch advancement)
/// 3. The contention is much lower than holding the arena write lock during
///    the entire GC pass
///
/// # Memory Bounds
///
/// The queue grows proportionally to:
/// - Thread count (more concurrent GC = more pending retirements)
/// - Epoch interval (longer intervals = more accumulation before drain)
///
/// Under normal operation with ~1ms epochs and 8-16 threads, the queue should
/// contain at most a few thousand entries.
#[derive(Debug)]
pub struct EbrRetireQueue {
    /// Pending retired batches grouped by retire epoch.
    pending: Mutex<Vec<RetiredBatch>>,
    /// Counter for total slots retired through this queue.
    total_retired: AtomicU64,
    /// Counter for total slots recycled (drained and returned to arena).
    total_recycled: AtomicU64,
}

#[derive(Debug)]
struct RetiredBatch {
    retire_epoch: u64,
    indices: Vec<VersionIdx>,
}

impl EbrRetireQueue {
    /// Create an empty retire queue.
    #[must_use]
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(Vec::new()),
            total_retired: AtomicU64::new(0),
            total_recycled: AtomicU64::new(0),
        }
    }

    /// Add a slot index to the retire queue.
    ///
    /// Call this after `VersionArena::take_for_retirement()` to track the slot
    /// for later recycling. The slot will be recycled when `drain_if_safe()`
    /// is called after epoch advancement.
    pub fn retire(&self, idx: VersionIdx, current_epoch: u64) {
        let mut pending = self.pending.lock();
        append_to_epoch_batch(&mut pending, current_epoch, idx);
        drop(pending);
        self.total_retired.fetch_add(1, Ordering::Relaxed);
    }

    /// Batch-retire multiple slot indices.
    pub fn retire_batch(&self, indices: impl IntoIterator<Item = VersionIdx>, current_epoch: u64) {
        let mut pending = self.pending.lock();
        let mut count = 0_u64;
        for idx in indices {
            append_to_epoch_batch(&mut pending, current_epoch, idx);
            count += 1;
        }
        drop(pending);
        if count > 0 {
            self.total_retired.fetch_add(count, Ordering::Relaxed);
        }
    }

    /// Drain all pending retirements if it's safe to recycle them.
    ///
    /// Returns the drained indices if all active guards have advanced past the
    /// retired batches being reclaimed.
    /// Returns an empty Vec if not safe yet or if the queue is empty.
    ///
    /// # Arguments
    ///
    /// * `current_epoch` - Current global epoch after any advancement.
    /// * `min_pinned_epoch` - Minimum pinned epoch across active guards. `None`
    ///   means there are no active guards, so the current epoch alone gates reclamation.
    #[must_use]
    pub fn drain_if_safe(
        &self,
        current_epoch: u64,
        min_pinned_epoch: Option<u64>,
    ) -> Vec<VersionIdx> {
        let mut pending = self.pending.lock();
        if pending.is_empty() {
            return Vec::new();
        }

        let safe_epoch = reclaim_safe_epoch(current_epoch, min_pinned_epoch);
        let drain_batches = reclaimable_batch_count(&pending, safe_epoch);
        if drain_batches == 0 {
            return Vec::new();
        }

        let retained_batches = pending.split_off(drain_batches);
        let drained_batches = std::mem::replace(&mut *pending, retained_batches);
        drop(pending);

        let drained_len = drained_batches
            .iter()
            .map(|batch| batch.indices.len())
            .sum::<usize>();
        let mut drained = Vec::with_capacity(drained_len);
        for mut batch in drained_batches {
            drained.append(&mut batch.indices);
        }

        let count = u64::try_from(drained.len()).unwrap_or(u64::MAX);
        if count > 0 {
            self.total_recycled.fetch_add(count, Ordering::Relaxed);
            GLOBAL_EBR_METRICS.record_gc_freed(count);
        }

        drained
    }

    /// Force-drain all pending retirements regardless of epoch.
    ///
    /// Use only during shutdown or when epoch safety is guaranteed externally.
    #[must_use]
    pub fn force_drain(&self) -> Vec<VersionIdx> {
        let mut pending = self.pending.lock();
        let drained_batches = std::mem::take(&mut *pending);
        drop(pending);

        let mut drained = Vec::new();
        for batch in drained_batches {
            drained.extend(batch.indices);
        }

        let count = u64::try_from(drained.len()).unwrap_or(u64::MAX);
        if count > 0 {
            self.total_recycled.fetch_add(count, Ordering::Relaxed);
            GLOBAL_EBR_METRICS.record_gc_freed(count);
        }

        drained
    }

    /// Number of slots currently pending recycle.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending
            .lock()
            .iter()
            .map(|batch| batch.indices.len())
            .sum()
    }

    /// Total slots retired through this queue (lifetime counter).
    #[must_use]
    pub fn total_retired(&self) -> u64 {
        self.total_retired.load(Ordering::Relaxed)
    }

    /// Total slots recycled through this queue (lifetime counter).
    #[must_use]
    pub fn total_recycled(&self) -> u64 {
        self.total_recycled.load(Ordering::Relaxed)
    }
}

/// Append a single `VersionIdx` to the batch for `epoch`.
///
/// Epochs advance monotonically, so the common case is that `epoch`
/// matches the last batch or is newer.  The fast path avoids the
/// `O(log n)` binary search and `O(n)` `Vec::insert` that the old
/// code paid on every call.
#[inline]
fn append_to_epoch_batch(pending: &mut Vec<RetiredBatch>, epoch: u64, idx: VersionIdx) {
    if let Some(last) = pending.last_mut() {
        if last.retire_epoch == epoch {
            last.indices.push(idx);
            return;
        }
        if last.retire_epoch < epoch {
            pending.push(RetiredBatch {
                retire_epoch: epoch,
                indices: vec![idx],
            });
            return;
        }
    } else {
        pending.push(RetiredBatch {
            retire_epoch: epoch,
            indices: vec![idx],
        });
        return;
    }
    // Rare: out-of-order epoch (should not happen with monotonic epochs,
    // but preserve correctness).
    match pending.binary_search_by_key(&epoch, |batch| batch.retire_epoch) {
        Ok(existing) => pending[existing].indices.push(idx),
        Err(insert_at) => pending.insert(
            insert_at,
            RetiredBatch {
                retire_epoch: epoch,
                indices: vec![idx],
            },
        ),
    }
}

#[inline]
fn reclaim_safe_epoch(current_epoch: u64, min_pinned_epoch: Option<u64>) -> u64 {
    min_pinned_epoch.map_or(current_epoch, |min_epoch| {
        std::cmp::min(current_epoch, min_epoch)
    })
}

#[inline]
fn reclaimable_batch_count(pending: &[RetiredBatch], safe_epoch: u64) -> usize {
    pending.partition_point(|batch| batch.retire_epoch < safe_epoch)
}

impl Default for EbrRetireQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };

    use proptest::{prelude::*, test_runner::Config as ProptestConfig};

    use super::{
        DEFAULT_MAX_PENDING_VERSIONS_PER_PAGE, EbrMetrics, GLOBAL_EBR_METRICS, StaleReaderConfig,
        VersionGuard, VersionGuardRegistry, VersionGuardTicket,
    };

    #[test]
    fn version_guard_registers_and_unregisters() {
        let registry = Arc::new(VersionGuardRegistry::new(StaleReaderConfig {
            warn_after: Duration::from_secs(60),
            warn_every: Duration::from_secs(10),
            ..StaleReaderConfig::default()
        }));
        assert_eq!(registry.active_guard_count(), 0);

        {
            let guard = VersionGuard::pin(Arc::clone(&registry));
            assert_eq!(registry.active_guard_count(), 1);
            assert!(guard.pinned_for() < Duration::from_secs(1));
        }

        assert_eq!(registry.active_guard_count(), 0);
    }

    #[test]
    fn nested_version_guards_pin_and_unpin_independently() {
        let registry = Arc::new(VersionGuardRegistry::default());
        let before = GLOBAL_EBR_METRICS.snapshot();

        {
            let outer = VersionGuard::pin(Arc::clone(&registry));
            assert_eq!(registry.active_guard_count(), 1);
            assert!(outer.pinned_for() < Duration::from_secs(1));

            {
                let inner = VersionGuard::pin(Arc::clone(&registry));
                assert_eq!(registry.active_guard_count(), 2);
                assert!(inner.pinned_for() < Duration::from_secs(1));
            }

            assert_eq!(
                registry.active_guard_count(),
                1,
                "dropping inner guard must keep outer guard active"
            );
        }

        assert_eq!(registry.active_guard_count(), 0);
        let after = GLOBAL_EBR_METRICS.snapshot();
        assert!(
            after.guards_pinned_total >= before.guards_pinned_total + 2,
            "nested guards should register two pin events"
        );
        assert!(
            after.guards_unpinned_total >= before.guards_unpinned_total + 2,
            "nested guards should register two unpin events"
        );
    }

    #[test]
    fn stale_reader_snapshots_report_long_pins() {
        let registry = Arc::new(VersionGuardRegistry::new(StaleReaderConfig {
            warn_after: Duration::from_millis(5),
            warn_every: Duration::from_millis(5),
            ..StaleReaderConfig::default()
        }));

        let _guard = VersionGuard::pin(Arc::clone(&registry));
        thread::sleep(Duration::from_millis(10));

        let stale = registry.stale_reader_snapshots(Instant::now());
        assert_eq!(stale.len(), 1);
        assert!(stale[0].pinned_for >= Duration::from_millis(5));
    }

    #[test]
    fn stale_reader_warning_is_rate_limited() {
        let registry = Arc::new(VersionGuardRegistry::new(StaleReaderConfig {
            warn_after: Duration::ZERO,
            warn_every: Duration::from_millis(5),
            ..StaleReaderConfig::default()
        }));
        let _guard = VersionGuard::pin(Arc::clone(&registry));

        let base = Instant::now();
        assert_eq!(registry.warn_on_stale_readers(base), 1);
        assert_eq!(
            registry.warn_on_stale_readers(base + Duration::from_millis(1)),
            0
        );
        assert_eq!(
            registry.warn_on_stale_readers(base + Duration::from_millis(6)),
            1
        );
    }

    #[derive(Clone)]
    struct DropCounter(Arc<AtomicUsize>);

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn deferred_retirement_executes_after_unpin() {
        let registry = Arc::new(VersionGuardRegistry::default());
        let dropped = Arc::new(AtomicUsize::new(0));

        {
            let guard = VersionGuard::pin(Arc::clone(&registry));
            guard.defer_retire(DropCounter(Arc::clone(&dropped)));
            guard.flush();
            assert_eq!(dropped.load(Ordering::SeqCst), 0);
        }

        let deadline = Instant::now() + Duration::from_secs(2);
        while dropped.load(Ordering::SeqCst) < 1 && Instant::now() < deadline {
            registry.flush_reclamation();
            thread::yield_now();
            thread::sleep(Duration::from_micros(50));
        }

        assert_eq!(
            dropped.load(Ordering::SeqCst),
            1,
            "deferred retirement should reclaim after guard drop"
        );
    }

    #[test]
    fn independent_registries_do_not_cross_block_reclamation() {
        let blocking_registry = Arc::new(VersionGuardRegistry::default());
        let reclaiming_registry = Arc::new(VersionGuardRegistry::default());
        let blocker = VersionGuard::pin(blocking_registry);
        let dropped = Arc::new(AtomicUsize::new(0));

        {
            let guard = VersionGuard::pin(Arc::clone(&reclaiming_registry));
            guard.defer_retire(DropCounter(Arc::clone(&dropped)));
            guard.flush();
        }

        let deadline = Instant::now() + Duration::from_secs(2);
        while dropped.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
            reclaiming_registry.flush_reclamation();
            thread::yield_now();
            thread::sleep(Duration::from_micros(50));
        }

        assert_eq!(
            dropped.load(Ordering::SeqCst),
            1,
            "a pinned reader in another registry must not delay this registry's reclamation"
        );
        drop(blocker);
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 2_500,
            .. ProptestConfig::default()
        })]

        #[test]
        fn prop_deferred_retire_respects_pin_lifetime_and_eventually_reclaims(
            deferred_count in 1_u8..33,
        ) {
            let registry = Arc::new(VersionGuardRegistry::default());
            let dropped = Arc::new(AtomicUsize::new(0));
            let expected = usize::from(deferred_count);

            {
                let guard = VersionGuard::pin(Arc::clone(&registry));
                for _ in 0..expected {
                    guard.defer_retire(DropCounter(Arc::clone(&dropped)));
                }
                guard.flush();
                prop_assert_eq!(dropped.load(Ordering::SeqCst), 0);
            }

            let deadline = Instant::now() + Duration::from_secs(2);
            while dropped.load(Ordering::SeqCst) < expected && Instant::now() < deadline {
                registry.flush_reclamation();
                thread::yield_now();
                thread::sleep(Duration::from_micros(50));
            }

            prop_assert_eq!(dropped.load(Ordering::SeqCst), expected);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 1_000,
            .. ProptestConfig::default()
        })]

        #[test]
        fn prop_thread_termination_does_not_lose_deferred_retirements(
            deferred_count in 1_u8..17,
        ) {
            let registry = Arc::new(VersionGuardRegistry::default());
            let dropped = Arc::new(AtomicUsize::new(0));
            let expected = usize::from(deferred_count);

            let worker_registry = Arc::clone(&registry);
            let worker_dropped = Arc::clone(&dropped);
            let worker = thread::spawn(move || {
                let ticket = VersionGuardTicket::register(worker_registry);
                for _ in 0..expected {
                    ticket.defer_retire(DropCounter(Arc::clone(&worker_dropped)));
                }
            });
            worker.join().expect("worker thread must not panic");
            prop_assert_eq!(registry.active_guard_count(), 0);

            let deadline = Instant::now() + Duration::from_secs(2);
            while dropped.load(Ordering::SeqCst) < expected && Instant::now() < deadline {
                registry.flush_reclamation();
                thread::yield_now();
                thread::sleep(Duration::from_micros(50));
            }

            prop_assert_eq!(dropped.load(Ordering::SeqCst), expected);
        }
    }

    // ===================================================================
    // bd-688.4: EBR metrics tests
    // ===================================================================

    #[test]
    fn ebr_metrics_basic_recording() {
        let m = EbrMetrics::new();

        m.record_retirement_deferred();
        m.record_retirement_deferred();
        m.record_flush();
        m.record_guard_pinned(1);
        m.record_guard_unpinned();
        m.record_stale_warnings(2);
        m.record_chain_length_sample(5);
        m.record_chain_length_sample(9);
        m.record_gc_freed(7);
        m.record_gc_blocked();

        let snap = m.snapshot();
        assert_eq!(snap.retirements_deferred_total, 2);
        assert_eq!(snap.flush_calls_total, 1);
        assert_eq!(snap.guards_pinned_total, 1);
        assert_eq!(snap.guards_unpinned_total, 1);
        assert_eq!(snap.stale_reader_warnings_total, 2);
        assert_eq!(snap.active_guards_high_water, 1);
        assert_eq!(snap.max_chain_length_observed, 9);
        assert_eq!(snap.chain_length_samples_total, 2);
        assert_eq!(snap.chain_length_sum_total, 14);
        assert_eq!(snap.gc_freed_count, 7);
        assert_eq!(snap.gc_blocked_count, 1);
        assert!((snap.avg_chain_length() - 7.0).abs() < f64::EPSILON);
    }

    #[test]
    fn ebr_metrics_reset() {
        let m = EbrMetrics::new();
        m.record_retirement_deferred();
        m.record_guard_pinned(5);
        m.record_chain_length_sample(12);
        m.record_gc_freed(3);
        m.record_gc_blocked();
        assert!(m.retirements_deferred_total.load(Ordering::Relaxed) > 0);

        m.reset();
        let snap = m.snapshot();
        assert_eq!(snap.retirements_deferred_total, 0);
        assert_eq!(snap.guards_pinned_total, 0);
        assert_eq!(snap.active_guards_high_water, 0);
        assert_eq!(snap.max_chain_length_observed, 0);
        assert_eq!(snap.chain_length_samples_total, 0);
        assert_eq!(snap.chain_length_sum_total, 0);
        assert_eq!(snap.gc_freed_count, 0);
        assert_eq!(snap.gc_blocked_count, 0);
    }

    #[test]
    fn ebr_metrics_high_water_mark_monotonic() {
        let m = EbrMetrics::new();

        m.record_guard_pinned(3);
        assert_eq!(m.snapshot().active_guards_high_water, 3);

        // Lower value should not reduce high-water mark.
        m.record_guard_pinned(1);
        assert_eq!(m.snapshot().active_guards_high_water, 3);

        // Higher value should update.
        m.record_guard_pinned(7);
        assert_eq!(m.snapshot().active_guards_high_water, 7);
    }

    #[test]
    fn ebr_metrics_display() {
        let m = EbrMetrics::new();
        m.record_retirement_deferred();
        m.record_flush();
        m.record_guard_pinned(1);
        m.record_chain_length_sample(8);
        let display = format!("{}", m.snapshot());
        assert!(display.contains("retired=1"));
        assert!(display.contains("flushed=1"));
        assert!(display.contains("pinned=1"));
        assert!(display.contains("chain_max=8"));
    }

    #[test]
    fn ebr_metrics_snapshot_serializable() {
        let m = EbrMetrics::new();
        m.record_retirement_deferred();
        m.record_guard_pinned(2);
        m.record_chain_length_sample(4);
        let snap = m.snapshot();
        let json = serde_json::to_string(&snap).unwrap();
        assert!(json.contains("\"retirements_deferred_total\":1"));
        assert!(json.contains("\"active_guards_high_water\":2"));
        assert!(json.contains("\"max_chain_length_observed\":4"));
    }

    #[test]
    fn ebr_metrics_guard_lifecycle_records() {
        // Use delta-based assertions with >= to avoid global counter interference
        // from parallel tests that also pin/retire/flush guards.
        let registry = Arc::new(VersionGuardRegistry::default());
        let before = GLOBAL_EBR_METRICS.snapshot();

        {
            let guard = VersionGuard::pin(Arc::clone(&registry));
            let after_pin = GLOBAL_EBR_METRICS.snapshot();
            assert!(
                after_pin.guards_pinned_total - before.guards_pinned_total >= 1,
                "expected at least 1 pin"
            );

            guard.defer_retire(42_u64);
            let after_retire = GLOBAL_EBR_METRICS.snapshot();
            assert!(
                after_retire.retirements_deferred_total - before.retirements_deferred_total >= 1,
                "expected at least 1 retirement"
            );

            guard.flush();
            let after_flush = GLOBAL_EBR_METRICS.snapshot();
            assert!(
                after_flush.flush_calls_total - before.flush_calls_total >= 1,
                "expected at least 1 flush"
            );
        }

        let after_drop = GLOBAL_EBR_METRICS.snapshot();
        assert!(
            after_drop.guards_unpinned_total - before.guards_unpinned_total >= 1,
            "expected at least 1 unpin"
        );
    }

    #[test]
    fn ebr_metrics_ticket_lifecycle_records() {
        let registry = Arc::new(VersionGuardRegistry::default());
        let before = GLOBAL_EBR_METRICS.snapshot();

        {
            let ticket = VersionGuardTicket::register(Arc::clone(&registry));
            let after_reg = GLOBAL_EBR_METRICS.snapshot();
            assert!(
                after_reg.guards_pinned_total > before.guards_pinned_total,
                "ticket registration should record at least one pin event"
            );

            ticket.defer_retire(99_u32);
            let after_retire = GLOBAL_EBR_METRICS.snapshot();
            assert!(
                after_retire.retirements_deferred_total > before.retirements_deferred_total,
                "ticket defer_retire should record at least one retirement"
            );
        }

        let after_drop = GLOBAL_EBR_METRICS.snapshot();
        assert!(
            after_drop.guards_unpinned_total > before.guards_unpinned_total,
            "ticket drop should record at least one unpin event"
        );
    }

    #[test]
    fn ebr_metrics_stale_warning_records() {
        let before = GLOBAL_EBR_METRICS.snapshot();

        let registry = Arc::new(VersionGuardRegistry::new(StaleReaderConfig {
            warn_after: Duration::ZERO,
            warn_every: Duration::ZERO,
            ..StaleReaderConfig::default()
        }));
        let _guard = VersionGuard::pin(Arc::clone(&registry));

        let warned = registry.warn_on_stale_readers(Instant::now());
        assert!(warned > 0);

        let after = GLOBAL_EBR_METRICS.snapshot();
        assert!(
            after.stale_reader_warnings_total > before.stale_reader_warnings_total,
            "stale warnings should have been recorded"
        );
    }

    // ===================================================================
    // D5: EbrRetireQueue tests
    // ===================================================================

    use super::{EbrRetireQueue, VersionIdx};

    #[test]
    fn ebr_retire_queue_retire_and_drain() {
        let queue = EbrRetireQueue::new();
        assert_eq!(queue.pending_count(), 0);
        assert_eq!(queue.total_retired(), 0);
        assert_eq!(queue.total_recycled(), 0);

        // Retire 3 slots at epoch 0.
        let idx1 = VersionIdx::new(0, 1, 0);
        let idx2 = VersionIdx::new(0, 2, 0);
        let idx3 = VersionIdx::new(0, 3, 0);
        queue.retire(idx1, 0);
        queue.retire(idx2, 0);
        queue.retire(idx3, 0);

        assert_eq!(queue.pending_count(), 3);
        assert_eq!(queue.total_retired(), 3);

        // A reader pinned at epoch 0 still needs the retired batch.
        let drained = queue.drain_if_safe(0, Some(0));
        assert!(
            drained.is_empty(),
            "same-epoch reclamation must stay pending"
        );
        assert_eq!(queue.pending_count(), 3);

        // Advance to the next epoch with no active readers: reclaim the whole batch.
        let drained = queue.drain_if_safe(1, None);
        assert_eq!(drained.len(), 3);
        assert_eq!(queue.pending_count(), 0);
        assert_eq!(queue.total_recycled(), 3);

        // Subsequent drain returns empty.
        let drained = queue.drain_if_safe(10, None);
        assert!(drained.is_empty());
    }

    #[test]
    fn ebr_retire_queue_batch_retire() {
        let queue = EbrRetireQueue::new();

        let indices: Vec<VersionIdx> = (0..10).map(|i| VersionIdx::new(0, i, 0)).collect();
        queue.retire_batch(indices.iter().copied(), 5);

        assert_eq!(queue.pending_count(), 10);
        assert_eq!(queue.total_retired(), 10);

        // A newer reader pinned at epoch 7 is past the retire epoch.
        let drained = queue.drain_if_safe(7, Some(7));
        assert_eq!(drained.len(), 10);
        assert_eq!(queue.total_recycled(), 10);
    }

    #[test]
    fn ebr_retire_queue_force_drain() {
        let queue = EbrRetireQueue::new();

        let idx = VersionIdx::new(1, 5, 99);
        queue.retire(idx, 0);
        assert_eq!(queue.pending_count(), 1);

        // Force drain ignores epoch.
        let drained = queue.force_drain();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0], idx);
        assert_eq!(queue.pending_count(), 0);
        assert_eq!(queue.total_recycled(), 1);
    }

    #[test]
    fn ebr_retire_queue_multiple_epochs() {
        let queue = EbrRetireQueue::new();

        // Retire at epoch 0.
        queue.retire(VersionIdx::new(0, 0, 0), 0);
        queue.retire(VersionIdx::new(0, 1, 0), 0);

        // Retire more at epoch 3.
        queue.retire(VersionIdx::new(0, 2, 0), 3);

        assert_eq!(queue.pending_count(), 3);

        // Reader pinned at epoch 0 still needs the epoch-0 batch.
        let drained = queue.drain_if_safe(0, Some(0));
        assert!(
            drained.is_empty(),
            "same-epoch reclamation must stay pending"
        );

        // Reader pinned at epoch 1: epoch-0 batch (retire_epoch 0 < 1) is reclaimable.
        let drained = queue.drain_if_safe(1, Some(1));
        assert_eq!(
            drained.len(),
            2,
            "only the epoch-0 retirements are reclaimable"
        );
        assert_eq!(queue.pending_count(), 1);

        // Once no active readers remain, the remaining batch can also drain.
        let drained = queue.drain_if_safe(4, None);
        assert_eq!(drained.len(), 1, "the later batch drains separately");
    }

    #[test]
    fn ebr_retire_queue_out_of_order_epochs() {
        let queue = EbrRetireQueue::new();

        queue.retire(VersionIdx::new(0, 0, 0), 5);
        queue.retire(VersionIdx::new(0, 1, 0), 3);

        let drained = queue.drain_if_safe(5, Some(5));
        assert_eq!(
            drained,
            vec![VersionIdx::new(0, 1, 0)],
            "reclaimable older batches must drain even when a newer batch was queued first",
        );
        assert_eq!(queue.pending_count(), 1);

        let drained = queue.drain_if_safe(6, Some(6));
        assert_eq!(drained, vec![VersionIdx::new(0, 0, 0)]);
        assert_eq!(queue.pending_count(), 0);
    }

    #[test]
    fn ebr_retire_queue_reclaimable_prefix_tracks_strict_epoch_boundary() {
        let pending = vec![
            super::RetiredBatch {
                retire_epoch: 1,
                indices: vec![VersionIdx::new(0, 0, 0)],
            },
            super::RetiredBatch {
                retire_epoch: 3,
                indices: vec![VersionIdx::new(0, 1, 0)],
            },
            super::RetiredBatch {
                retire_epoch: 3,
                indices: vec![VersionIdx::new(0, 2, 0)],
            },
            super::RetiredBatch {
                retire_epoch: 8,
                indices: vec![VersionIdx::new(0, 3, 0)],
            },
        ];

        assert_eq!(super::reclaim_safe_epoch(9, Some(5)), 5);
        assert_eq!(super::reclaim_safe_epoch(4, None), 4);
        assert_eq!(super::reclaimable_batch_count(&pending, 1), 0);
        assert_eq!(super::reclaimable_batch_count(&pending, 3), 1);
        assert_eq!(super::reclaimable_batch_count(&pending, 4), 3);
        assert_eq!(super::reclaimable_batch_count(&pending, 9), 4);
    }

    #[test]
    fn ebr_retire_queue_empty_drain() {
        let queue = EbrRetireQueue::new();

        // Draining empty queue returns empty vec.
        let drained = queue.drain_if_safe(100, None);
        assert!(drained.is_empty());

        // Force drain on empty also returns empty.
        let drained = queue.force_drain();
        assert!(drained.is_empty());
    }

    // ===================================================================
    // bd-wt4uu: stale-reader horizon clamp + bounded version chain
    // ===================================================================

    use crate::cache_aligned::SharedTxnSlot;
    use crate::core_types::{ReaderPinCommitSeq, raise_gc_horizon_with_reader_clamp};
    use fsqlite_types::CommitSeq;

    /// A reader pinned at `begin_seq=5` MUST prevent the GC horizon from
    /// advancing past `4` even if the writer's view of the slot table shows
    /// no active transaction (the reader holds an EBR pin but its TxnSlot is
    /// freed). Covers the core bd-wt4uu race.
    #[test]
    fn test_gc_horizon_capped_at_stale_reader_pin() {
        // No active slots — pretend the reader's slot was already freed but
        // its EBR pin still carries its snapshot seq.
        let slots: &[SharedTxnSlot] = &[];
        let reader_pins = vec![ReaderPinCommitSeq {
            guard_id: 42,
            pinned_commit_seq: Some(CommitSeq::new(5)),
            pinned_for: Duration::from_secs(45),
        }];

        // Writer commits 100 times → commit_seq=106, proposes horizon=106.
        let old_horizon = CommitSeq::new(0);
        let commit_seq = CommitSeq::new(106);
        let result = raise_gc_horizon_with_reader_clamp(
            slots,
            old_horizon,
            commit_seq,
            &reader_pins,
            /* affected_pages */ 7,
        );

        assert!(
            result.new_horizon <= CommitSeq::new(4),
            "bead_id=bd-wt4uu reader@seq=5 MUST clamp horizon to ≤ 4 \
             (got {:?})",
            result.new_horizon
        );
        assert_eq!(
            result.stale_reader_clamps, 1,
            "bead_id=bd-wt4uu exactly one reader clamp must be recorded"
        );
        assert_eq!(
            result.clamped_by_reader_seq,
            Some(CommitSeq::new(5)),
            "bead_id=bd-wt4uu clamping reader seq must be surfaced"
        );
    }

    /// When a version chain for a page exceeds the configured per-page cap
    /// while a stale reader holds it pinned, the writer MUST force-abort the
    /// reader so the chain can be GC'd — preventing OOM.
    #[test]
    fn test_bounded_chain_force_aborts_stale_reader() {
        let registry = Arc::new(VersionGuardRegistry::new(StaleReaderConfig {
            warn_after: Duration::ZERO,
            warn_every: Duration::from_millis(5),
            max_pending_versions_per_page: 4,
        }));

        // Pin a reader and declare its snapshot seq.
        let reader = VersionGuard::pin(Arc::clone(&registry));
        reader.set_pinned_commit_seq(10);
        assert!(
            !reader.is_force_aborted(),
            "bead_id=bd-wt4uu reader starts un-aborted"
        );
        assert_eq!(
            registry.min_pinned_commit_seq(),
            Some(10),
            "bead_id=bd-wt4uu min_pinned_commit_seq reports reader's seq"
        );

        // Simulate the writer's bounded-chain enforcement: chain length
        // exceeds the per-page cap (4). The writer marks the reader.
        let chain_len = 128_usize;
        let cap = registry.stale_reader_config().max_pending_versions_per_page;
        assert!(chain_len > cap, "precondition: chain must exceed cap");

        let marked = registry.mark_force_abort(reader.guard_id());
        assert!(
            marked,
            "bead_id=bd-wt4uu force-abort mark must succeed on first call"
        );

        // Reader observes the abort on next access.
        assert!(
            reader.is_force_aborted(),
            "bead_id=bd-wt4uu reader MUST observe force-abort on next access"
        );

        // Idempotent: second call returns false.
        assert!(
            !registry.mark_force_abort(reader.guard_id()),
            "bead_id=bd-wt4uu second force-abort mark must be idempotent"
        );

        // Dropping the reader clears its pin — chain is reclaimable.
        drop(reader);
        assert_eq!(
            registry.min_pinned_commit_seq(),
            None,
            "bead_id=bd-wt4uu min_pinned_commit_seq clears after reader drops"
        );
    }

    /// Under normal load (no stale readers, short-lived pins), the horizon
    /// must advance monotonically across many commits — no regression from
    /// the clamp path.
    #[test]
    fn test_no_regression_under_normal_load() {
        let slots: &[SharedTxnSlot] = &[];

        let mut horizon = CommitSeq::new(0);
        let commits = 10_000_u64;
        for i in 1..=commits {
            let commit_seq = CommitSeq::new(i);
            // No readers pinned.
            let result = raise_gc_horizon_with_reader_clamp(
                slots,
                horizon,
                commit_seq,
                &[],
                /* affected_pages */ 1,
            );
            assert!(
                result.new_horizon >= horizon,
                "bead_id=bd-wt4uu horizon must never decrease: \
                 old={horizon:?} new={:?} at commit={i}",
                result.new_horizon
            );
            assert_eq!(
                result.stale_reader_clamps, 0,
                "bead_id=bd-wt4uu no clamp under normal load"
            );
            horizon = result.new_horizon;
        }
        assert_eq!(
            horizon,
            CommitSeq::new(commits),
            "bead_id=bd-wt4uu horizon must equal final commit_seq after \
             {commits} normal commits"
        );
    }

    /// Config default exposes the 4096 per-page cap from bd-wt4uu.
    #[test]
    fn test_stale_reader_config_default_carries_per_page_cap() {
        let cfg = StaleReaderConfig::default();
        assert_eq!(
            cfg.max_pending_versions_per_page, DEFAULT_MAX_PENDING_VERSIONS_PER_PAGE,
            "bead_id=bd-wt4uu default cap must be {DEFAULT_MAX_PENDING_VERSIONS_PER_PAGE}"
        );
        assert_eq!(
            DEFAULT_MAX_PENDING_VERSIONS_PER_PAGE, 4096,
            "bead_id=bd-wt4uu per-page cap default must be 4096"
        );
    }

    /// Clamping must NEVER cause the horizon to regress below `old_horizon`
    /// (monotonicity invariant, §5.6.5).
    #[test]
    fn test_clamp_respects_monotonic_horizon() {
        let slots: &[SharedTxnSlot] = &[];

        // old_horizon is already past the reader's snapshot seq — the reader
        // is effectively too late to clamp; the horizon must stay at
        // old_horizon (never decrease).
        let old_horizon = CommitSeq::new(20);
        let commit_seq = CommitSeq::new(100);
        let reader_pins = vec![ReaderPinCommitSeq {
            guard_id: 1,
            pinned_commit_seq: Some(CommitSeq::new(5)),
            pinned_for: Duration::from_secs(45),
        }];

        let result =
            raise_gc_horizon_with_reader_clamp(slots, old_horizon, commit_seq, &reader_pins, 0);
        assert!(
            result.new_horizon >= old_horizon,
            "bead_id=bd-wt4uu horizon must not decrease below old_horizon \
             even when a reader holds an older seq"
        );
    }
}
