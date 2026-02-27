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

use crossbeam_epoch::{self as epoch, Guard};
use parking_lot::Mutex;
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
    /// Total version objects deferred for retirement via `defer_retire`.
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

    /// Record a deferred retirement.
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

/// Configuration for stale-reader detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StaleReaderConfig {
    /// Reader pins older than this duration are considered stale.
    pub warn_after: Duration,
    /// Minimum interval between repeated warnings for the same guard.
    pub warn_every: Duration,
}

impl Default for StaleReaderConfig {
    fn default() -> Self {
        Self {
            warn_after: Duration::from_secs(30),
            warn_every: Duration::from_secs(5),
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
}

#[derive(Debug, Clone, Copy)]
struct ReaderPinState {
    pinned_at: Instant,
    last_warned_at: Option<Instant>,
}

/// Registry for active epoch pins (`VersionGuard`s).
///
/// The registry is intentionally lock-based and simple for the initial
/// integration slice; cardinality is bounded by active transactions.
#[derive(Debug)]
pub struct VersionGuardRegistry {
    stale_reader: StaleReaderConfig,
    next_guard_id: AtomicU64,
    active: Mutex<HashMap<u64, ReaderPinState>>,
}

impl VersionGuardRegistry {
    /// Construct a registry with the provided stale-reader policy.
    #[must_use]
    pub fn new(stale_reader: StaleReaderConfig) -> Self {
        Self {
            stale_reader,
            next_guard_id: AtomicU64::new(1),
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

    /// Snapshot all stale readers as of `now`.
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
                    })
                } else {
                    None
                }
            })
            .collect()
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
        self.active.lock().insert(
            guard_id,
            ReaderPinState {
                pinned_at,
                last_warned_at: None,
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
    registry: Arc<VersionGuardRegistry>,
    guard_id: u64,
    pinned_at: Instant,
    guard: Guard,
}

impl VersionGuard {
    /// Pin the current thread into the epoch domain.
    #[must_use]
    pub fn pin(registry: Arc<VersionGuardRegistry>) -> Self {
        let pinned_at = Instant::now();
        let guard_id = registry.register_guard(pinned_at);
        let guard = epoch::pin();
        let active_count = registry.active_guard_count() as u64;
        GLOBAL_EBR_METRICS.record_guard_pinned(active_count);
        tracing::trace!(
            target: "fsqlite_mvcc::ebr",
            guard_id,
            active_guards = active_count,
            "epoch guard pinned"
        );
        Self {
            registry,
            guard_id,
            pinned_at,
            guard,
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
/// inside a [`Transaction`] that may be moved between threads (async
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

    /// Pin the current thread's epoch and defer retirement of a value.
    ///
    /// The short-lived epoch pin ensures correctness: the deferred value is
    /// only reclaimed after all concurrently pinned readers have advanced
    /// past the current epoch.
    pub fn defer_retire<T: Send + 'static>(&self, retired: T) {
        GLOBAL_EBR_METRICS.record_retirement_deferred();
        let guard = epoch::pin();
        guard.defer(move || drop(retired));
        guard.flush();
    }

    /// Pin the current thread's epoch and defer an arbitrary closure.
    pub fn defer_retire_with<F, R>(&self, retire: F)
    where
        F: FnOnce() -> R + Send + 'static,
    {
        GLOBAL_EBR_METRICS.record_retirement_deferred();
        let guard = epoch::pin();
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

    use crossbeam_epoch as epoch;
    use proptest::{prelude::*, test_runner::Config as ProptestConfig};

    use super::{
        EbrMetrics, GLOBAL_EBR_METRICS, StaleReaderConfig, VersionGuard, VersionGuardRegistry,
        VersionGuardTicket,
    };

    #[test]
    fn version_guard_registers_and_unregisters() {
        let registry = Arc::new(VersionGuardRegistry::new(StaleReaderConfig {
            warn_after: Duration::from_secs(60),
            warn_every: Duration::from_secs(10),
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
            let flush_guard = epoch::pin();
            flush_guard.flush();
            thread::yield_now();
            thread::sleep(Duration::from_micros(50));
        }

        assert_eq!(
            dropped.load(Ordering::SeqCst),
            1,
            "deferred retirement should reclaim after guard drop"
        );
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
                let flush_guard = epoch::pin();
                flush_guard.flush();
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
                let flush_guard = epoch::pin();
                flush_guard.flush();
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
}
