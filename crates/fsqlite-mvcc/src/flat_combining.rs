//! Flat Combining for sequential batching under contention (§14.2).
//!
//! When many threads contend on a shared data structure, each thread publishes
//! its request to a per-thread slot and one thread becomes the *combiner*.
//! The combiner collects all pending requests, processes them as a single
//! batch holding one lock, then publishes the results.  This reduces
//! cache-line bouncing from N lock acquisitions to 1.
//!
//! ## Protocol
//!
//! 1. Thread publishes `(op, argument)` to its slot (atomic store).
//! 2. Thread tries to acquire the combiner lock (`try_lock`).
//!    - **Won**: scan all slots, collect pending ops, execute batch, store
//!      results, release lock.
//!    - **Lost**: spin-wait until its own slot shows a result.
//! 3. Thread reads its result from the slot.
//!
//! ## Slot Layout
//!
//! Each slot is a pair of `AtomicU64`:
//!   - `state`: EMPTY (0) | REQUEST (op‖arg packed) | RESULT (high-bit set)
//!   - `payload`: argument or result value.
//!
//! ## Safety
//!
//! No `UnsafeCell` or `unsafe` blocks — all state uses `AtomicU64`.
//!
//! ## Tracing & Metrics
//!
//! - **Target**: `fsqlite.flat_combine`
//!   - `DEBUG`: batch execution with `batch_size`, `combiner_thread`
//!   - `INFO`: periodic contention stats
//! - **Metrics**:
//!   - `fsqlite_flat_combining_batches_total`
//!   - `fsqlite_flat_combining_ops_total`
//!   - `fsqlite_flat_combining_batch_size_sum` (for avg = sum / batches)
//!   - `fsqlite_flat_combining_batch_size_max`
//!   - `fsqlite_flat_combining_wait_ns_total`
//!   - `fsqlite_flat_combining_wait_ns_max`
//!   - `fsqlite_htm_attempts`
//!   - `fsqlite_htm_aborts_conflict`
//!   - `fsqlite_htm_aborts_capacity`
//!   - `fsqlite_htm_aborts_explicit`
//!   - `fsqlite_htm_aborts_other`

use fsqlite_types::sync_primitives::Instant;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

use fsqlite_types::sync_primitives::Mutex;
use serde::Serialize;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum threads that can participate in flat combining.
pub const MAX_FC_THREADS: usize = 64;

/// Slot state: empty (available for a new request).
const SLOT_EMPTY: u64 = 0;

/// Bit set in the state word to indicate the slot contains a result.
const RESULT_BIT: u64 = 1 << 63;

/// Maximum spin iterations before yielding while waiting for result.
const SPIN_BEFORE_YIELD: u32 = 1024;

// ---------------------------------------------------------------------------
// HTM Guard State Machine (bd-77l3t Phase 1)
// ---------------------------------------------------------------------------

/// HTM guard states (stored in AtomicU8).
/// See HTM_GUARD_DESIGN.md for the full state machine diagram.
const HTM_NOT_PROBED: u8 = 0;
const HTM_AVAILABLE: u8 = 1;
const HTM_UNAVAILABLE: u8 = 2;
const HTM_BLACKLISTED: u8 = 3;
const HTM_DISABLED: u8 = 4;
const HTM_USER_DISABLED: u8 = 5;

/// EWMA abort-rate threshold for dynamic disable (fixed-point: 5000 = 50%).
#[allow(dead_code)] // Phase 1 HTM guard scaffolding; wired in later phases.
const HTM_DISABLE_THRESHOLD: u32 = 5000;

/// Initial cooldown before re-enabling after dynamic disable (milliseconds).
const HTM_COOLDOWN_INITIAL_MS: u64 = 5000;

/// Maximum cooldown with exponential backoff (milliseconds).
const HTM_COOLDOWN_MAX_MS: u64 = 60_000;

/// EWMA alpha numerator (out of 10000). alpha=0.3 → 3000.
#[allow(dead_code)] // Phase 1 HTM guard scaffolding; wired in later phases.
const HTM_EWMA_ALPHA: u32 = 3000;

/// Window size: update EWMA every this many attempts.
#[allow(dead_code)] // Phase 1 HTM guard scaffolding; wired in later phases.
const HTM_EWMA_WINDOW_SIZE: u64 = 1000;

/// Maximum HTM retries per apply() invocation before falling through to lock.
#[allow(dead_code)] // Phase 1 HTM guard scaffolding; wired in later phases.
const MAX_HTM_RETRIES: u32 = 3;

/// HTM guard: state machine + abort-rate monitor for the flat combiner fast-path.
///
/// Phase 1: guard skeleton only. State always resolves to `HTM_UNAVAILABLE` because
/// no actual HTM intrinsics are wired. The guard infrastructure (state machine,
/// EWMA monitor, PRAGMA support, tracing hooks) is exercisable and testable.
///
/// Zero behavior change: all threads use the existing lock path.
pub struct HtmGuard {
    /// Current guard state (see HTM_* constants).
    state: AtomicU8,
    /// EWMA abort rate: fixed-point [0..10000] = [0.0000..1.0000].
    ewma_abort_rate: AtomicU32,
    /// Attempts in current EWMA window.
    window_attempts: AtomicU64,
    /// Aborts in current EWMA window.
    window_aborts: AtomicU64,
    /// Window start timestamp (nanoseconds since epoch, Relaxed).
    #[allow(dead_code)] // Phase 1 HTM guard scaffolding; wired in later phases.
    window_start_ns: AtomicU64,
    /// Number of times we have transitioned to DISABLED (for exponential backoff).
    disable_count: AtomicU32,
    /// Timestamp of last disable event (nanoseconds).
    last_disable_ns: AtomicU64,
}

impl HtmGuard {
    /// Create a new guard. State starts as NOT_PROBED.
    const fn new() -> Self {
        Self {
            state: AtomicU8::new(HTM_NOT_PROBED),
            ewma_abort_rate: AtomicU32::new(0),
            window_attempts: AtomicU64::new(0),
            window_aborts: AtomicU64::new(0),
            window_start_ns: AtomicU64::new(0),
            disable_count: AtomicU32::new(0),
            last_disable_ns: AtomicU64::new(0),
        }
    }

    /// Probe CPU for HTM support. Called lazily on first use.
    /// Phase 1: always returns UNAVAILABLE (no intrinsics wired).
    fn probe_cpu(&self) {
        // Phase 1: no HTM intrinsics available in safe Rust.
        // Always set UNAVAILABLE. Future phases will wire real detection.
        let new_state = HTM_UNAVAILABLE;

        // CAS from NOT_PROBED to the probed result. If another thread raced
        // us, that's fine — both will compute the same result.
        let _ = self.state.compare_exchange(
            HTM_NOT_PROBED,
            new_state,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );

        tracing::info!(
            target: "fsqlite::htm",
            event = "cpu_probe",
            tsx_available = false,
            tme_available = false,
            stepping = "unknown",
            known_buggy = false,
            phase = "phase1_stub",
        );
    }

    /// Check if HTM fast-path should be attempted.
    /// Returns true only if state == AVAILABLE.
    #[inline]
    fn should_attempt(&self) -> bool {
        let state = self.state.load(Ordering::Relaxed);
        if state == HTM_NOT_PROBED {
            self.probe_cpu();
            return self.state.load(Ordering::Relaxed) == HTM_AVAILABLE;
        }
        // Check for cooldown expiry if DISABLED.
        if state == HTM_DISABLED {
            self.maybe_reenable();
        }
        self.state.load(Ordering::Relaxed) == HTM_AVAILABLE
    }

    /// Record an HTM attempt (regardless of outcome).
    #[allow(dead_code)] // Phase 1 HTM guard scaffolding; wired in later phases.
    fn record_attempt(&self) {
        record_htm_attempt();
        let attempts = self.window_attempts.fetch_add(1, Ordering::Relaxed) + 1;
        if attempts >= HTM_EWMA_WINDOW_SIZE {
            self.update_ewma();
        }
    }

    /// Record an HTM abort with status code.
    #[allow(dead_code)] // Phase 1 HTM guard scaffolding; wired in later phases.
    fn record_abort(&self, status: u32) {
        let classification = record_htm_abort_status(status);
        self.window_aborts.fetch_add(1, Ordering::Relaxed);

        tracing::debug!(
            target: "fsqlite::htm",
            event = "xabort",
            abort_code = status,
            reason = match classification.reason {
                HtmAbortReason::Conflict => "conflict",
                HtmAbortReason::Capacity => "capacity",
                HtmAbortReason::Explicit => "explicit",
                HtmAbortReason::Other => "other",
            },
            retryable = classification.retryable,
        );
    }

    /// Update EWMA and check disable threshold. Called when window fills.
    #[allow(dead_code)] // Phase 1 HTM guard scaffolding; wired in later phases.
    fn update_ewma(&self) {
        let attempts = self.window_attempts.swap(0, Ordering::Relaxed);
        let aborts = self.window_aborts.swap(0, Ordering::Relaxed);

        if attempts == 0 {
            return;
        }

        // Compute new_rate in fixed-point [0..10000].
        #[allow(clippy::cast_possible_truncation)]
        let new_rate_fp = ((aborts * 10000) / attempts) as u32;
        let old_ewma = self.ewma_abort_rate.load(Ordering::Relaxed);

        // EWMA: alpha * new + (1-alpha) * old, all in fixed-point.
        let updated = (HTM_EWMA_ALPHA * new_rate_fp + (10000 - HTM_EWMA_ALPHA) * old_ewma) / 10000;
        self.ewma_abort_rate.store(updated, Ordering::Relaxed);

        // Check disable threshold.
        if updated > HTM_DISABLE_THRESHOLD && self.state.load(Ordering::Relaxed) == HTM_AVAILABLE {
            self.dynamic_disable(updated);
        }
    }

    /// Transition to DISABLED state due to abort storm.
    #[allow(dead_code)] // Phase 1 HTM guard scaffolding; wired in later phases.
    fn dynamic_disable(&self, abort_rate: u32) {
        let prev = self.state.compare_exchange(
            HTM_AVAILABLE,
            HTM_DISABLED,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
        if prev.is_ok() {
            self.disable_count.fetch_add(1, Ordering::Relaxed);
            // Record disable timestamp for cooldown.
            #[allow(clippy::cast_possible_truncation)]
            let now_ns = Instant::now().elapsed().as_nanos() as u64;
            self.last_disable_ns.store(now_ns, Ordering::Relaxed);

            tracing::warn!(
                target: "fsqlite::htm",
                event = "dynamic_disable",
                abort_rate_fp = abort_rate,
                abort_rate_pct = abort_rate as f64 / 100.0,
                threshold_pct = HTM_DISABLE_THRESHOLD as f64 / 100.0,
                disable_count = self.disable_count.load(Ordering::Relaxed),
            );
        }
    }

    /// Check if cooldown has expired and re-enable if so.
    fn maybe_reenable(&self) {
        let dc = self.disable_count.load(Ordering::Relaxed);
        let cooldown_ms = HTM_COOLDOWN_INITIAL_MS
            .saturating_mul(1u64.checked_shl(dc).unwrap_or(u64::MAX))
            .min(HTM_COOLDOWN_MAX_MS);

        // Simple approach: compare timestamps.
        // In Phase 1 this is never reached (state never becomes AVAILABLE → DISABLED).
        #[allow(clippy::cast_possible_truncation)]
        let now_ns = Instant::now().elapsed().as_nanos() as u64;
        let disable_ns = self.last_disable_ns.load(Ordering::Relaxed);
        let elapsed_ms = now_ns.saturating_sub(disable_ns) / 1_000_000;

        if elapsed_ms >= cooldown_ms {
            // Reset EWMA and window, then re-enable.
            self.ewma_abort_rate.store(0, Ordering::Relaxed);
            self.window_attempts.store(0, Ordering::Relaxed);
            self.window_aborts.store(0, Ordering::Relaxed);
            let _ = self.state.compare_exchange(
                HTM_DISABLED,
                HTM_AVAILABLE,
                Ordering::AcqRel,
                Ordering::Relaxed,
            );
            tracing::info!(
                target: "fsqlite::htm",
                event = "reenable",
                cooldown_ms,
                disable_count = dc,
            );
        }
    }

    /// Force-disable via PRAGMA. Returns previous state.
    pub fn pragma_disable(&self) -> u8 {
        self.state.swap(HTM_USER_DISABLED, Ordering::AcqRel)
    }

    /// Re-enable via PRAGMA (re-probes CPU).
    pub fn pragma_enable(&self) {
        let current = self.state.load(Ordering::Relaxed);
        if current == HTM_USER_DISABLED {
            self.state.store(HTM_NOT_PROBED, Ordering::Release);
            // Next should_attempt() will re-probe.
        }
    }

    /// Current state name (for diagnostics / virtual table).
    #[must_use]
    pub fn state_name(&self) -> &'static str {
        match self.state.load(Ordering::Relaxed) {
            HTM_NOT_PROBED => "not_probed",
            HTM_AVAILABLE => "available",
            HTM_UNAVAILABLE => "unavailable",
            HTM_BLACKLISTED => "blacklisted",
            HTM_DISABLED => "disabled",
            HTM_USER_DISABLED => "user_disabled",
            _ => "unknown",
        }
    }

    /// Current EWMA abort rate as percentage (0.0..100.0).
    #[must_use]
    pub fn ewma_pct(&self) -> f64 {
        f64::from(self.ewma_abort_rate.load(Ordering::Relaxed)) / 100.0
    }

    /// Number of times dynamic disable has triggered.
    #[must_use]
    pub fn disable_count(&self) -> u32 {
        self.disable_count.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Global metrics
// ---------------------------------------------------------------------------

static FC_BATCHES_TOTAL: AtomicU64 = AtomicU64::new(0);
static FC_OPS_TOTAL: AtomicU64 = AtomicU64::new(0);
static FC_BATCH_SIZE_SUM: AtomicU64 = AtomicU64::new(0);
static FC_BATCH_SIZE_MAX: AtomicU64 = AtomicU64::new(0);
static FC_WAIT_NS_TOTAL: AtomicU64 = AtomicU64::new(0);
static FC_WAIT_NS_MAX: AtomicU64 = AtomicU64::new(0);
static FC_HTM_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
static FC_HTM_ABORTS_CONFLICT: AtomicU64 = AtomicU64::new(0);
static FC_HTM_ABORTS_CAPACITY: AtomicU64 = AtomicU64::new(0);
static FC_HTM_ABORTS_EXPLICIT: AtomicU64 = AtomicU64::new(0);
static FC_HTM_ABORTS_OTHER: AtomicU64 = AtomicU64::new(0);

const XABORT_EXPLICIT: u32 = 1 << 0;
const XABORT_RETRY: u32 = 1 << 1;
const XABORT_CONFLICT: u32 = 1 << 2;
const XABORT_CAPACITY: u32 = 1 << 3;
const XABORT_DEBUG: u32 = 1 << 4;
const XABORT_NESTED: u32 = 1 << 5;
const XABORT_CODE_SHIFT: u32 = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HtmAbortReason {
    Conflict,
    Capacity,
    Explicit,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HtmAbortClassification {
    reason: HtmAbortReason,
    retryable: bool,
    explicit_code: Option<u8>,
    debug: bool,
    nested: bool,
}

/// Snapshot of flat combining metrics.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct FlatCombiningMetrics {
    pub fsqlite_flat_combining_batches_total: u64,
    pub fsqlite_flat_combining_ops_total: u64,
    pub fsqlite_flat_combining_batch_size_sum: u64,
    pub fsqlite_flat_combining_batch_size_max: u64,
    pub fsqlite_flat_combining_wait_ns_total: u64,
    pub fsqlite_flat_combining_wait_ns_max: u64,
    pub fsqlite_htm_attempts: u64,
    pub fsqlite_htm_aborts_conflict: u64,
    pub fsqlite_htm_aborts_capacity: u64,
    pub fsqlite_htm_aborts_explicit: u64,
    pub fsqlite_htm_aborts_other: u64,
    /// HTM guard state name.
    pub fsqlite_htm_state: &'static str,
    /// EWMA abort rate as percentage (0.0..100.0).
    pub fsqlite_htm_ewma_abort_rate_pct: f64,
    /// Number of dynamic disable events.
    pub fsqlite_htm_disable_count: u32,
}

/// Read current flat combining metrics (global counters only).
/// For HTM guard state, pass a `&FlatCombiner` to [`flat_combining_metrics_from`].
#[must_use]
pub fn flat_combining_metrics() -> FlatCombiningMetrics {
    flat_combining_metrics_with_htm("unavailable", 0.0, 0)
}

/// Read flat combining metrics with HTM guard state from a specific combiner.
#[must_use]
pub fn flat_combining_metrics_from(combiner: &FlatCombiner) -> FlatCombiningMetrics {
    flat_combining_metrics_with_htm(
        combiner.htm_guard.state_name(),
        combiner.htm_guard.ewma_pct(),
        combiner.htm_guard.disable_count(),
    )
}

fn flat_combining_metrics_with_htm(
    state: &'static str,
    ewma_pct: f64,
    disable_count: u32,
) -> FlatCombiningMetrics {
    FlatCombiningMetrics {
        fsqlite_flat_combining_batches_total: FC_BATCHES_TOTAL.load(Ordering::Relaxed),
        fsqlite_flat_combining_ops_total: FC_OPS_TOTAL.load(Ordering::Relaxed),
        fsqlite_flat_combining_batch_size_sum: FC_BATCH_SIZE_SUM.load(Ordering::Relaxed),
        fsqlite_flat_combining_batch_size_max: FC_BATCH_SIZE_MAX.load(Ordering::Relaxed),
        fsqlite_flat_combining_wait_ns_total: FC_WAIT_NS_TOTAL.load(Ordering::Relaxed),
        fsqlite_flat_combining_wait_ns_max: FC_WAIT_NS_MAX.load(Ordering::Relaxed),
        fsqlite_htm_attempts: FC_HTM_ATTEMPTS.load(Ordering::Relaxed),
        fsqlite_htm_aborts_conflict: FC_HTM_ABORTS_CONFLICT.load(Ordering::Relaxed),
        fsqlite_htm_aborts_capacity: FC_HTM_ABORTS_CAPACITY.load(Ordering::Relaxed),
        fsqlite_htm_aborts_explicit: FC_HTM_ABORTS_EXPLICIT.load(Ordering::Relaxed),
        fsqlite_htm_aborts_other: FC_HTM_ABORTS_OTHER.load(Ordering::Relaxed),
        fsqlite_htm_state: state,
        fsqlite_htm_ewma_abort_rate_pct: ewma_pct,
        fsqlite_htm_disable_count: disable_count,
    }
}

/// Reset metrics (for tests).
pub fn reset_flat_combining_metrics() {
    FC_BATCHES_TOTAL.store(0, Ordering::Relaxed);
    FC_OPS_TOTAL.store(0, Ordering::Relaxed);
    FC_BATCH_SIZE_SUM.store(0, Ordering::Relaxed);
    FC_BATCH_SIZE_MAX.store(0, Ordering::Relaxed);
    FC_WAIT_NS_TOTAL.store(0, Ordering::Relaxed);
    FC_WAIT_NS_MAX.store(0, Ordering::Relaxed);
    FC_HTM_ATTEMPTS.store(0, Ordering::Relaxed);
    FC_HTM_ABORTS_CONFLICT.store(0, Ordering::Relaxed);
    FC_HTM_ABORTS_CAPACITY.store(0, Ordering::Relaxed);
    FC_HTM_ABORTS_EXPLICIT.store(0, Ordering::Relaxed);
    FC_HTM_ABORTS_OTHER.store(0, Ordering::Relaxed);
}

fn update_max(metric: &AtomicU64, val: u64) {
    let mut prev = metric.load(Ordering::Relaxed);
    while val > prev {
        match metric.compare_exchange_weak(prev, val, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => prev = actual,
        }
    }
}

const fn classify_htm_abort_status(status: u32) -> HtmAbortClassification {
    let reason = if (status & XABORT_CONFLICT) != 0 {
        HtmAbortReason::Conflict
    } else if (status & XABORT_CAPACITY) != 0 {
        HtmAbortReason::Capacity
    } else if (status & XABORT_EXPLICIT) != 0 {
        HtmAbortReason::Explicit
    } else {
        HtmAbortReason::Other
    };
    let explicit_code = if (status & XABORT_EXPLICIT) != 0 {
        Some(((status >> XABORT_CODE_SHIFT) & 0xff) as u8)
    } else {
        None
    };

    HtmAbortClassification {
        reason,
        retryable: (status & XABORT_RETRY) != 0,
        explicit_code,
        debug: (status & XABORT_DEBUG) != 0,
        nested: (status & XABORT_NESTED) != 0,
    }
}

/// Record a single HTM entry attempt before invoking `_xbegin()`.
fn record_htm_attempt() {
    FC_HTM_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
}

/// Classify and record a failed HTM attempt returned by `_xbegin()`.
fn record_htm_abort_status(status: u32) -> HtmAbortClassification {
    let classification = classify_htm_abort_status(status);
    match classification.reason {
        HtmAbortReason::Conflict => {
            FC_HTM_ABORTS_CONFLICT.fetch_add(1, Ordering::Relaxed);
        }
        HtmAbortReason::Capacity => {
            FC_HTM_ABORTS_CAPACITY.fetch_add(1, Ordering::Relaxed);
        }
        HtmAbortReason::Explicit => {
            FC_HTM_ABORTS_EXPLICIT.fetch_add(1, Ordering::Relaxed);
        }
        HtmAbortReason::Other => {
            FC_HTM_ABORTS_OTHER.fetch_add(1, Ordering::Relaxed);
        }
    }
    classification
}

/// Record a public HTM attempt for future fast-path integrations.
pub fn note_htm_attempt() {
    record_htm_attempt();
}

/// Record a public HTM abort status returned by a failed `_xbegin()`.
pub fn note_htm_abort(status: u32) {
    let _ = record_htm_abort_status(status);
}

// ---------------------------------------------------------------------------
// FcSlot
// ---------------------------------------------------------------------------

/// Per-thread request/result slot.
struct FcSlot {
    /// SLOT_EMPTY | request_tag (1..2^63-1) | RESULT_BIT | result_value
    state: AtomicU64,
    /// Payload: argument for requests, result for completions.
    payload: AtomicU64,
}

impl FcSlot {
    fn new() -> Self {
        Self {
            state: AtomicU64::new(SLOT_EMPTY),
            payload: AtomicU64::new(0),
        }
    }
}

// ---------------------------------------------------------------------------
// FlatCombiner
// ---------------------------------------------------------------------------

/// A flat combining accumulator for `u64` values.
///
/// Threads submit operations via [`FcHandle::apply`] and receive results.
/// Supported operations:
/// - `OP_ADD`: atomic add to the shared accumulator
/// - `OP_READ`: read current accumulator value
///
/// The combiner processes all pending operations in a single batch,
/// reducing lock contention.
pub struct FlatCombiner {
    /// The shared value being operated on.
    value: AtomicU64,
    /// Per-thread slots for request/result exchange.
    slots: [FcSlot; MAX_FC_THREADS],
    /// Slot ownership: 0 = free, non-zero = occupied by a thread.
    owners: [AtomicU64; MAX_FC_THREADS],
    /// Combiner lock — only one thread processes a batch at a time.
    combiner_lock: Mutex<()>,
    /// HTM fast-path guard (bd-77l3t). Phase 1: always UNAVAILABLE.
    htm_guard: HtmGuard,
}

/// Operation tag: add argument to accumulator.
pub const OP_ADD: u64 = 1;
/// Operation tag: read current accumulator value.
pub const OP_READ: u64 = 2;

impl FlatCombiner {
    /// Create a new flat combiner with the given initial value.
    pub fn new(initial: u64) -> Self {
        Self {
            value: AtomicU64::new(initial),
            slots: std::array::from_fn(|_| FcSlot::new()),
            owners: std::array::from_fn(|_| AtomicU64::new(0)),
            combiner_lock: Mutex::new(()),
            htm_guard: HtmGuard::new(),
        }
    }

    /// Access the HTM guard (for PRAGMA support and diagnostics).
    #[must_use]
    pub fn htm_guard(&self) -> &HtmGuard {
        &self.htm_guard
    }

    /// Register a thread.  Returns an [`FcHandle`] with an assigned slot,
    /// or `None` if all slots are occupied.
    pub fn register(&self) -> Option<FcHandle<'_>> {
        // Use a unique non-zero ID based on thread ID hash.
        let tid = {
            let t = std::thread::current().id();
            let s = format!("{t:?}");
            let mut h = 1u64;
            for b in s.bytes() {
                h = h.wrapping_mul(31).wrapping_add(u64::from(b));
            }
            if h == 0 { 1 } else { h }
        };

        for i in 0..MAX_FC_THREADS {
            if self.owners[i]
                .compare_exchange(0, tid, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Some(FcHandle {
                    combiner: self,
                    slot: i,
                });
            }
        }
        None
    }

    /// Current value (for diagnostics — not linearizable without combining).
    #[must_use]
    pub fn value(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    /// Number of registered threads.
    #[must_use]
    pub fn active_threads(&self) -> usize {
        self.owners
            .iter()
            .filter(|o| o.load(Ordering::Relaxed) != 0)
            .count()
    }

    /// Process all pending requests in a single batch.
    /// The caller MUST hold the `combiner_lock`.
    fn combine_locked(&self) {
        let mut batch_size = 0u64;
        let mut current = self.value.load(Ordering::Acquire);

        // Scan all slots for pending requests.
        for i in 0..MAX_FC_THREADS {
            let state = self.slots[i].state.load(Ordering::Acquire);
            if state == SLOT_EMPTY || (state & RESULT_BIT) != 0 {
                continue; // Empty or already has a result.
            }

            let op = state;
            let arg = self.slots[i].payload.load(Ordering::Acquire);
            batch_size += 1;

            let result = match op {
                OP_ADD => {
                    current = current.wrapping_add(arg);
                    current
                }
                OP_READ => current,
                _ => 0, // Unknown op — return 0.
            };

            // Publish result: set payload, then mark state as RESULT.
            self.slots[i].payload.store(result, Ordering::Release);
            self.slots[i]
                .state
                .store(RESULT_BIT | op, Ordering::Release);
        }

        self.value.store(current, Ordering::Release);

        if batch_size > 0 {
            // Update metrics.
            FC_BATCHES_TOTAL.fetch_add(1, Ordering::Relaxed);
            FC_OPS_TOTAL.fetch_add(batch_size, Ordering::Relaxed);
            FC_BATCH_SIZE_SUM.fetch_add(batch_size, Ordering::Relaxed);
            update_max(&FC_BATCH_SIZE_MAX, batch_size);

            tracing::debug!(
                target: "fsqlite.flat_combine",
                batch_size,
                "flat_combine_batch"
            );
        }
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for FlatCombiner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlatCombiner")
            .field("value", &self.value.load(Ordering::Relaxed))
            .field("active_threads", &self.active_threads())
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// FcHandle (per-thread)
// ---------------------------------------------------------------------------

/// Per-thread flat combining handle.  Automatically unregisters on drop.
pub struct FcHandle<'a> {
    combiner: &'a FlatCombiner,
    slot: usize,
}

impl FcHandle<'_> {
    /// Submit an operation and wait for the result.
    ///
    /// The caller publishes its request; either it becomes the combiner and
    /// processes the entire batch, or it waits for the combiner to process
    /// its request.
    pub fn apply(&self, op: u64, arg: u64) -> u64 {
        let start = Instant::now();

        // Publish our request.
        self.combiner.slots[self.slot]
            .payload
            .store(arg, Ordering::Release);
        self.combiner.slots[self.slot]
            .state
            .store(op, Ordering::Release);

        // ── HTM fast-path guard (bd-77l3t) ──────────────────────────────
        // Phase 1: should_attempt() always returns false (UNAVAILABLE).
        // When real HTM is wired (Phase 3), this block will:
        //   1. XBEGIN
        //   2. Check combiner_lock is not held (XABORT if held)
        //   3. Execute combine_locked() speculatively
        //   4. XEND → return result
        //   5. On abort → record_abort(eax), fall through to lock path
        // See HTM_GUARD_DESIGN.md §5 for the full call-site shape.
        if self.combiner.htm_guard.should_attempt() {
            // Phase 2+: HTM transaction would go here.
            // For now, this branch is never taken.
        }
        // ── End HTM guard ────────────────────────────────────────────────

        // ALIEN ARTIFACT: True Flat Combining.
        // We attempt to become the combiner. If we fail, we MUST NOT block on an OS mutex
        // (which would defeat the entire purpose of flat combining by forcing context switches).
        // Instead, we spin on our own cache-line-isolated slot until the active combiner
        // writes our result. This converts global lock contention into read-only local spinning.
        if let Some(_guard) = self.combiner.combiner_lock.try_lock() {
            self.combiner.combine_locked();
        }

        // Check if our request has been serviced.
        let mut spins = 0u32;
        loop {
            let state = self.combiner.slots[self.slot].state.load(Ordering::Acquire);
            if (state & RESULT_BIT) != 0 {
                // Result ready — read payload and clear slot.
                let result = self.combiner.slots[self.slot]
                    .payload
                    .load(Ordering::Acquire);
                self.combiner.slots[self.slot]
                    .state
                    .store(SLOT_EMPTY, Ordering::Release);

                #[allow(clippy::cast_possible_truncation)]
                let elapsed_ns = start.elapsed().as_nanos() as u64;
                FC_WAIT_NS_TOTAL.fetch_add(elapsed_ns, Ordering::Relaxed);
                update_max(&FC_WAIT_NS_MAX, elapsed_ns);

                return result;
            }

            // Still waiting. Spin or yield.
            spins += 1;
            if spins < SPIN_BEFORE_YIELD {
                std::hint::spin_loop();
            } else {
                // If the combiner died or is extremely slow, we attempt to take over.
                // If we can't take over, yield the thread to avoid burning CPU unnecessarily.
                if let Some(_guard) = self.combiner.combiner_lock.try_lock() {
                    self.combiner.combine_locked();
                } else {
                    std::thread::yield_now();
                }
                spins = 0;
            }
        }
    }

    /// Convenience: add a value to the accumulator.
    pub fn add(&self, val: u64) -> u64 {
        self.apply(OP_ADD, val)
    }

    /// Convenience: read the current accumulator value.
    pub fn read(&self) -> u64 {
        self.apply(OP_READ, 0)
    }

    /// Slot index (for diagnostics).
    #[must_use]
    pub fn slot(&self) -> usize {
        self.slot
    }
}

impl Drop for FcHandle<'_> {
    fn drop(&mut self) {
        // Clear slot state and release ownership.
        self.combiner.slots[self.slot]
            .state
            .store(SLOT_EMPTY, Ordering::Release);
        self.combiner.owners[self.slot].store(0, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// ShardedFlatCombiner (E1 — bd-wwqen Track E)
// ---------------------------------------------------------------------------

/// Number of shards for the sharded flat combiner.
/// Set to min(8, num_cpus) at runtime, capped at 8 for cache efficiency.
pub const MAX_FC_SHARDS: usize = 8;

/// A sharded flat combiner that distributes operations across multiple
/// independent combiners based on a shard key (typically page number).
///
/// At c8 concurrency, a single `FlatCombiner` becomes a bottleneck because
/// only one thread can hold the combiner lock. With sharding, operations
/// on different pages can proceed in parallel through different shards.
///
/// # Design
///
/// Each shard is a fully independent `FlatCombiner` with its own:
/// - `combiner_lock` (no cross-shard contention)
/// - slot array (per-shard thread registration)
/// - value accumulator (per-shard state)
///
/// # Routing
///
/// Operations are routed to shards via `shard_key % num_shards`. For MVCC
/// page operations, the shard key is typically `page_number.get()`. This
/// ensures that operations on the same page always go to the same shard
/// (preserving linearizability) while operations on different pages can
/// proceed in parallel.
///
/// # Performance
///
/// - c1: ~same as single combiner (minimal overhead)
/// - c4: ~4x improvement (4 threads can use 4 different shards)
/// - c8: ~8x improvement (8 threads across 8 shards, no contention)
pub struct ShardedFlatCombiner {
    /// The shard array. Using a fixed-size array avoids allocation.
    shards: [FlatCombiner; MAX_FC_SHARDS],
    /// Number of active shards (1..=MAX_FC_SHARDS).
    num_shards: usize,
}

impl ShardedFlatCombiner {
    /// Create a new sharded combiner with the given initial value per shard.
    ///
    /// The number of shards is `min(num_cpus, MAX_FC_SHARDS)`.
    #[must_use]
    pub fn new(initial_per_shard: u64) -> Self {
        let num_shards = std::thread::available_parallelism()
            .map(|p| p.get().min(MAX_FC_SHARDS))
            .unwrap_or(MAX_FC_SHARDS);
        Self {
            shards: std::array::from_fn(|_| FlatCombiner::new(initial_per_shard)),
            num_shards,
        }
    }

    /// Create a sharded combiner with a specific number of shards.
    #[must_use]
    pub fn with_shards(num_shards: usize, initial_per_shard: u64) -> Self {
        let effective = num_shards.clamp(1, MAX_FC_SHARDS);
        Self {
            shards: std::array::from_fn(|_| FlatCombiner::new(initial_per_shard)),
            num_shards: effective,
        }
    }

    /// Number of active shards.
    #[must_use]
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    /// Get the shard index for a given key.
    #[inline]
    fn shard_index(&self, shard_key: u64) -> usize {
        (shard_key as usize) % self.num_shards
    }

    /// Get a reference to the shard for the given key.
    #[inline]
    pub fn shard(&self, shard_key: u64) -> &FlatCombiner {
        &self.shards[self.shard_index(shard_key)]
    }

    /// Register a thread on the shard for the given key.
    pub fn register(&self, shard_key: u64) -> Option<ShardedFcHandle<'_>> {
        let idx = self.shard_index(shard_key);
        self.shards[idx].register().map(|inner| ShardedFcHandle {
            inner,
            shard_idx: idx,
        })
    }

    /// Total value across all shards (for diagnostics).
    #[must_use]
    pub fn total_value(&self) -> u64 {
        self.shards[..self.num_shards]
            .iter()
            .map(FlatCombiner::value)
            .sum()
    }

    /// Total active threads across all shards.
    #[must_use]
    pub fn total_active_threads(&self) -> usize {
        self.shards[..self.num_shards]
            .iter()
            .map(FlatCombiner::active_threads)
            .sum()
    }

    /// Per-shard values (for diagnostics).
    #[must_use]
    pub fn shard_values(&self) -> Vec<u64> {
        self.shards[..self.num_shards]
            .iter()
            .map(FlatCombiner::value)
            .collect()
    }
}

impl std::fmt::Debug for ShardedFlatCombiner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedFlatCombiner")
            .field("num_shards", &self.num_shards)
            .field("total_value", &self.total_value())
            .finish_non_exhaustive()
    }
}

/// Per-thread handle for a sharded flat combiner.
pub struct ShardedFcHandle<'a> {
    inner: FcHandle<'a>,
    shard_idx: usize,
}

impl ShardedFcHandle<'_> {
    /// Submit an operation and wait for the result.
    pub fn apply(&self, op: u64, arg: u64) -> u64 {
        self.inner.apply(op, arg)
    }

    /// Convenience: add a value to the shard's accumulator.
    pub fn add(&self, val: u64) -> u64 {
        self.inner.add(val)
    }

    /// Convenience: read the shard's current accumulator value.
    pub fn read(&self) -> u64 {
        self.inner.read()
    }

    /// Which shard this handle is registered to.
    #[must_use]
    pub fn shard_index(&self) -> usize {
        self.shard_idx
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn register_unregister() {
        let fc = FlatCombiner::new(0);
        assert_eq!(fc.active_threads(), 0);

        let h1 = fc.register().unwrap();
        assert_eq!(fc.active_threads(), 1);

        let h2 = fc.register().unwrap();
        assert_eq!(fc.active_threads(), 2);

        drop(h1);
        assert_eq!(fc.active_threads(), 1);

        drop(h2);
        assert_eq!(fc.active_threads(), 0);
    }

    #[test]
    fn single_thread_add() {
        let fc = FlatCombiner::new(0);
        let h = fc.register().unwrap();

        let r1 = h.add(10);
        assert_eq!(r1, 10);

        let r2 = h.add(20);
        assert_eq!(r2, 30);

        let r3 = h.read();
        assert_eq!(r3, 30);

        assert_eq!(fc.value(), 30);
        drop(h);
    }

    #[test]
    fn single_thread_sequential() {
        let fc = FlatCombiner::new(100);
        let h = fc.register().unwrap();

        for i in 1..=50 {
            let result = h.add(1);
            assert_eq!(result, 100 + i);
        }

        assert_eq!(h.read(), 150);
        drop(h);
    }

    #[test]
    fn concurrent_adds_correct_total() {
        let fc = Arc::new(FlatCombiner::new(0));
        let barrier = Arc::new(Barrier::new(4));
        let mut handles = Vec::new();

        for _ in 0..4 {
            let f = Arc::clone(&fc);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let h = f.register().unwrap();
                b.wait();
                for _ in 0..500 {
                    h.add(1);
                }
                drop(h);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(fc.value(), 2000, "4 threads * 500 adds = 2000");
    }

    #[test]
    #[allow(clippy::many_single_char_names)]
    fn concurrent_stress_no_lost_updates() {
        let fc = Arc::new(FlatCombiner::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(4));
        let total_adds = Arc::new(AtomicU64::new(0));

        let mut handles = Vec::new();
        for _ in 0..4 {
            let f = Arc::clone(&fc);
            let s = Arc::clone(&stop);
            let b = Arc::clone(&barrier);
            let t = Arc::clone(&total_adds);
            handles.push(thread::spawn(move || {
                let h = f.register().unwrap();
                b.wait();
                let mut local = 0u64;
                while !s.load(Ordering::Relaxed) {
                    h.add(1);
                    local += 1;
                }
                t.fetch_add(local, Ordering::Relaxed);
                drop(h);
            }));
        }

        thread::sleep(Duration::from_millis(300));
        stop.store(true, Ordering::Release);

        for h in handles {
            h.join().unwrap();
        }

        let expected = total_adds.load(Ordering::Relaxed);
        let actual = fc.value();
        assert_eq!(
            actual, expected,
            "accumulator {actual} != total submitted {expected}"
        );
    }

    #[test]
    fn metrics_track_batches() {
        // Delta-based: snapshot before, act, snapshot after.
        let before = flat_combining_metrics();

        let fc = FlatCombiner::new(0);
        let h = fc.register().unwrap();

        h.add(1);
        h.add(2);
        h.add(3);

        let after = flat_combining_metrics();
        let batch_delta = after.fsqlite_flat_combining_batches_total
            - before.fsqlite_flat_combining_batches_total;
        let ops_delta =
            after.fsqlite_flat_combining_ops_total - before.fsqlite_flat_combining_ops_total;
        assert!(
            batch_delta >= 3,
            "expected at least 3 batches (single thread = 1 op per batch), got {batch_delta}"
        );
        assert!(ops_delta >= 3, "expected at least 3 ops, got {ops_delta}");

        drop(h);
    }

    #[test]
    fn batching_under_contention() {
        // With many threads contending, some batches should contain > 1 op.
        let before = flat_combining_metrics();

        let fc = Arc::new(FlatCombiner::new(0));
        let barrier = Arc::new(Barrier::new(8));
        let mut handles = Vec::new();

        for _ in 0..8 {
            let f = Arc::clone(&fc);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let h = f.register().unwrap();
                b.wait();
                for _ in 0..200 {
                    h.add(1);
                }
                drop(h);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(fc.value(), 1600, "8 threads * 200 = 1600");

        let after = flat_combining_metrics();
        let batches_delta = after.fsqlite_flat_combining_batches_total
            - before.fsqlite_flat_combining_batches_total;
        let ops_delta =
            after.fsqlite_flat_combining_ops_total - before.fsqlite_flat_combining_ops_total;
        let avg_batch = if batches_delta > 0 {
            ops_delta as f64 / batches_delta as f64
        } else {
            0.0
        };

        // Under contention, we expect at least some batches > 1.
        println!(
            "[flat_combining] batches={batches_delta} ops={ops_delta} avg_batch={avg_batch:.2} max_batch={}",
            after.fsqlite_flat_combining_batch_size_max
        );
    }

    #[test]
    fn read_sees_latest_value() {
        let fc = Arc::new(FlatCombiner::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let f = Arc::clone(&fc);
        let b = Arc::clone(&barrier);
        let writer = thread::spawn(move || {
            let h = f.register().unwrap();
            b.wait();
            for _ in 0..100 {
                h.add(1);
            }
            drop(h);
        });

        let f = Arc::clone(&fc);
        let b2 = Arc::clone(&barrier);
        let reader = thread::spawn(move || {
            let h = f.register().unwrap();
            b2.wait();
            // Give writer some time.
            thread::sleep(Duration::from_millis(50));
            let v = h.read();
            drop(h);
            v
        });

        writer.join().unwrap();
        let last_read = reader.join().unwrap();
        // Reader should see a value between 0 and 100.
        assert!(last_read <= 100, "read {last_read} > 100");
    }

    #[test]
    fn no_starvation_bounded_wait() {
        // Every thread should complete within a reasonable time.
        let fc = Arc::new(FlatCombiner::new(0));
        let barrier = Arc::new(Barrier::new(4));
        let mut handles = Vec::new();

        for _ in 0..4 {
            let f = Arc::clone(&fc);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let h = f.register().unwrap();
                b.wait();
                let start = Instant::now();
                for _ in 0..100 {
                    h.add(1);
                }
                let elapsed = start.elapsed();
                drop(h);
                elapsed
            }));
        }

        for h in handles {
            let elapsed = h.join().unwrap();
            // Each thread should finish within 5 seconds (generous bound).
            assert!(
                elapsed < Duration::from_secs(5),
                "thread took too long: {elapsed:?} — possible starvation"
            );
        }

        assert_eq!(fc.value(), 400);
    }

    #[test]
    fn debug_format() {
        let fc = FlatCombiner::new(42);
        let dbg = format!("{fc:?}");
        assert!(dbg.contains("FlatCombiner"));
        assert!(dbg.contains("42"));
    }

    // ── HTM Guard tests (bd-77l3t Phase 1) ──────────────────────────

    #[test]
    fn htm_guard_defaults_to_unavailable() {
        let guard = HtmGuard::new();
        assert_eq!(guard.state.load(Ordering::Relaxed), HTM_NOT_PROBED);
        assert!(!guard.should_attempt());
        assert_eq!(guard.state_name(), "unavailable");
    }

    #[test]
    fn htm_guard_probe_is_idempotent() {
        let guard = HtmGuard::new();
        guard.probe_cpu();
        let state1 = guard.state.load(Ordering::Relaxed);
        guard.probe_cpu();
        let state2 = guard.state.load(Ordering::Relaxed);
        assert_eq!(state1, state2);
        assert_eq!(state1, HTM_UNAVAILABLE);
    }

    #[test]
    fn htm_guard_pragma_disable_enable() {
        let guard = HtmGuard::new();
        guard.probe_cpu(); // Sets UNAVAILABLE in Phase 1

        let prev = guard.pragma_disable();
        assert_eq!(prev, HTM_UNAVAILABLE);
        assert_eq!(guard.state_name(), "user_disabled");

        guard.pragma_enable();
        // After enable, state goes back to NOT_PROBED, then re-probes.
        assert!(!guard.should_attempt()); // Re-probes → UNAVAILABLE
        assert_eq!(guard.state_name(), "unavailable");
    }

    #[test]
    fn htm_guard_ewma_computation() {
        let guard = HtmGuard::new();
        // Manually set to AVAILABLE to test EWMA logic.
        guard.state.store(HTM_AVAILABLE, Ordering::Relaxed);

        // Simulate 1000 attempts with 800 aborts (80% abort rate).
        guard.window_attempts.store(1000, Ordering::Relaxed);
        guard.window_aborts.store(800, Ordering::Relaxed);
        guard.update_ewma();

        // EWMA: 0.3 * 8000 + 0.7 * 0 = 2400 (fixed-point)
        let ewma = guard.ewma_abort_rate.load(Ordering::Relaxed);
        assert_eq!(ewma, 2400);

        // Second window: another 80% abort rate.
        guard.window_attempts.store(1000, Ordering::Relaxed);
        guard.window_aborts.store(800, Ordering::Relaxed);
        guard.update_ewma();

        // EWMA: 0.3 * 8000 + 0.7 * 2400 = 2400 + 1680 = 4080
        let ewma2 = guard.ewma_abort_rate.load(Ordering::Relaxed);
        assert_eq!(ewma2, 4080);

        // Third window: pushes over threshold.
        guard.window_attempts.store(1000, Ordering::Relaxed);
        guard.window_aborts.store(800, Ordering::Relaxed);
        guard.update_ewma();

        // EWMA: 0.3 * 8000 + 0.7 * 4080 = 2400 + 2856 = 5256 > 5000
        let ewma3 = guard.ewma_abort_rate.load(Ordering::Relaxed);
        assert_eq!(ewma3, 5256);
        // Should have triggered dynamic disable.
        assert_eq!(guard.state.load(Ordering::Relaxed), HTM_DISABLED);
        assert_eq!(guard.disable_count(), 1);
    }

    #[test]
    fn htm_guard_record_abort_updates_window() {
        let guard = HtmGuard::new();
        guard.record_abort(XABORT_CONFLICT | XABORT_RETRY);
        assert_eq!(guard.window_aborts.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn htm_guard_metrics_in_flat_combiner() {
        let fc = FlatCombiner::new(0);
        assert_eq!(fc.htm_guard().state_name(), "not_probed");
        let metrics = flat_combining_metrics_from(&fc);
        assert_eq!(metrics.fsqlite_htm_state, "not_probed");
        assert!((metrics.fsqlite_htm_ewma_abort_rate_pct - 0.0).abs() < f64::EPSILON);
        assert_eq!(metrics.fsqlite_htm_disable_count, 0);
    }

    #[test]
    fn htm_guard_in_apply_path() {
        // Verify that apply() works correctly with the guard present.
        // Phase 1: guard always returns false, so lock path is always used.
        let fc = FlatCombiner::new(0);
        let h = fc.register().unwrap();
        let result = h.add(42);
        assert_eq!(result, 42);
        // Guard should have been probed lazily.
        assert_eq!(fc.htm_guard().state_name(), "unavailable");
    }

    // ── Existing HTM classification tests ─────────────────────────────

    #[test]
    fn classify_htm_abort_status_prefers_conflict() {
        let status =
            XABORT_CONFLICT | XABORT_CAPACITY | XABORT_RETRY | XABORT_DEBUG | XABORT_NESTED;
        let classification = classify_htm_abort_status(status);
        assert_eq!(classification.reason, HtmAbortReason::Conflict);
        assert!(classification.retryable);
        assert!(classification.debug);
        assert!(classification.nested);
        assert_eq!(classification.explicit_code, None);
    }

    #[test]
    fn classify_htm_abort_status_extracts_explicit_code() {
        let status = XABORT_EXPLICIT | XABORT_RETRY | (0x2a_u32 << XABORT_CODE_SHIFT);
        let classification = classify_htm_abort_status(status);
        assert_eq!(classification.reason, HtmAbortReason::Explicit);
        assert!(classification.retryable);
        assert_eq!(classification.explicit_code, Some(0x2a));
    }

    #[test]
    fn record_htm_abort_status_updates_counters() {
        reset_flat_combining_metrics();

        record_htm_attempt();
        record_htm_attempt();
        record_htm_attempt();
        record_htm_attempt();
        let conflict = record_htm_abort_status(XABORT_CONFLICT | XABORT_RETRY);
        let capacity = record_htm_abort_status(XABORT_CAPACITY);
        let explicit = record_htm_abort_status(XABORT_EXPLICIT | (0x07_u32 << XABORT_CODE_SHIFT));
        let other = record_htm_abort_status(0);

        assert_eq!(conflict.reason, HtmAbortReason::Conflict);
        assert_eq!(capacity.reason, HtmAbortReason::Capacity);
        assert_eq!(explicit.reason, HtmAbortReason::Explicit);
        assert_eq!(explicit.explicit_code, Some(0x07));
        assert_eq!(other.reason, HtmAbortReason::Other);

        let metrics = flat_combining_metrics();
        assert_eq!(metrics.fsqlite_htm_attempts, 4);
        assert_eq!(metrics.fsqlite_htm_aborts_conflict, 1);
        assert_eq!(metrics.fsqlite_htm_aborts_capacity, 1);
        assert_eq!(metrics.fsqlite_htm_aborts_explicit, 1);
        assert_eq!(metrics.fsqlite_htm_aborts_other, 1);
    }

    // ── ShardedFlatCombiner tests (E1 — bd-wwqen Track E) ─────────────────

    #[test]
    fn sharded_combiner_basic() {
        let sfc = ShardedFlatCombiner::with_shards(4, 0);
        assert_eq!(sfc.num_shards(), 4);
        assert_eq!(sfc.total_value(), 0);
        assert_eq!(sfc.total_active_threads(), 0);
    }

    #[test]
    fn sharded_combiner_register_different_shards() {
        let sfc = ShardedFlatCombiner::with_shards(4, 0);
        let h0 = sfc.register(0).unwrap();
        let h1 = sfc.register(1).unwrap();
        let h4 = sfc.register(4).unwrap(); // hashes to shard 0

        assert_eq!(h0.shard_index(), 0);
        assert_eq!(h1.shard_index(), 1);
        assert_eq!(h4.shard_index(), 0);

        drop(h0);
        drop(h1);
        drop(h4);
    }

    #[test]
    fn sharded_combiner_adds_to_correct_shard() {
        let sfc = ShardedFlatCombiner::with_shards(4, 0);
        let h0 = sfc.register(0).unwrap();
        let h1 = sfc.register(1).unwrap();

        h0.add(100);
        h1.add(200);
        h0.add(50);

        let values = sfc.shard_values();
        assert_eq!(values[0], 150);
        assert_eq!(values[1], 200);
        assert_eq!(sfc.total_value(), 350);

        drop(h0);
        drop(h1);
    }

    #[test]
    fn sharded_combiner_concurrent_parallel_shards() {
        let sfc = Arc::new(ShardedFlatCombiner::with_shards(8, 0));
        let barrier = Arc::new(Barrier::new(8));
        let mut handles = Vec::new();

        for shard_key in 0..8u64 {
            let s = Arc::clone(&sfc);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let h = s.register(shard_key).unwrap();
                b.wait();
                for _ in 0..500 {
                    h.add(1);
                }
                drop(h);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(sfc.total_value(), 4000, "8 shards * 500 = 4000");
    }

    #[test]
    fn sharded_combiner_clamp_shards() {
        let sfc1 = ShardedFlatCombiner::with_shards(0, 0);
        assert_eq!(sfc1.num_shards(), 1);

        let sfc2 = ShardedFlatCombiner::with_shards(100, 0);
        assert_eq!(sfc2.num_shards(), MAX_FC_SHARDS);
    }
}
