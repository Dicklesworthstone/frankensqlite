//! Remote effects contract primitives (§4.19.1-§4.19.5, `bd-numl`).
//!
//! This module provides:
//! - explicit RemoteCap gating for remote execution paths,
//! - named computations (no closure shipping),
//! - deterministic idempotency key derivation + dedup store,
//! - lease-backed liveness checks with deterministic escalation,
//! - a cancellation-safe remote eviction saga skeleton.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
#[cfg(not(feature = "native"))]
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;
#[cfg(not(feature = "native"))]
use std::time::Instant;
#[cfg(feature = "native")]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "native")]
use asupersync::combinator::bulkhead::{
    Bulkhead as AdmissionBulkhead, BulkheadError as AdmissionBulkheadError,
    BulkheadMetrics as AdmissionBulkheadMetrics, BulkheadPermit as AdmissionPermit, BulkheadPolicy,
};
#[cfg(feature = "native")]
use asupersync::types::Time as AdmissionTime;
use blake3::Hasher;
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::{Cx, cap};
use fsqlite_types::{IdempotencyKey, ObjectId, RemoteCap, Saga};
use tracing::{debug, info, warn};

#[cfg(not(feature = "native"))]
use crate::Bulkhead as AdmissionBulkhead;
#[cfg(feature = "native")]
use crate::available_parallelism_or_one;
#[cfg(not(feature = "native"))]
use crate::{
    BulkheadConfig, BulkheadPermit as AdmissionPermit, OverflowPolicy, available_parallelism_or_one,
};

const BEAD_ID: &str = "bd-numl";
const MAX_BALANCED_REMOTE_IN_FLIGHT: usize = 8;
const REMOTE_EFFECTS_EXECUTOR_NAME: &str = "fsqlite.remote_effects";
// Tiered storage executor infrastructure — reserved for future ECS/Native mode
// integration where cold pages are offloaded to remote object storage.
#[allow(dead_code)]
pub(crate) const TIERED_STORAGE_EXECUTOR_NAME: &str = "fsqlite.tiered_storage";
#[allow(dead_code)]
const DEFAULT_TIERED_STORAGE_QUEUE_DEPTH: usize = 1;
#[allow(dead_code)]
const DEFAULT_TIERED_STORAGE_QUEUE_TIMEOUT: Duration = Duration::from_millis(250);
const ADMISSION_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Domain separator for deterministic remote idempotency keys.
pub const REMOTE_IDEMPOTENCY_DOMAIN: &str = "fsqlite:remote:v1";

/// Named remote computations (§4.19.2).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ComputationName {
    /// `symbol_get_range(object_id, esi_lo, esi_hi, ecs_epoch)`
    SymbolGetRange,
    /// `symbol_put_batch(object_id, symbols[], ecs_epoch)`
    SymbolPutBatch,
    /// `segment_put(segment_id, bytes, ecs_epoch)`
    SegmentPut,
    /// `segment_stat(segment_id, ecs_epoch)`
    SegmentStat,
    /// Explicit extension point; not accepted unless registered.
    Custom(String),
}

impl ComputationName {
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::SymbolGetRange => "symbol_get_range",
            Self::SymbolPutBatch => "symbol_put_batch",
            Self::SegmentPut => "segment_put",
            Self::SegmentStat => "segment_stat",
            Self::Custom(name) => name.as_str(),
        }
    }

    #[must_use]
    fn canonical_tag(&self) -> u8 {
        match self {
            Self::SymbolGetRange => 0x01,
            Self::SymbolPutBatch => 0x02,
            Self::SegmentPut => 0x03,
            Self::SegmentStat => 0x04,
            Self::Custom(_) => 0xFF,
        }
    }

    #[must_use]
    fn canonical_name_bytes(&self) -> Vec<u8> {
        self.as_str().as_bytes().to_vec()
    }
}

/// Serialized remote computation request payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedComputation {
    pub name: ComputationName,
    pub input_bytes: Vec<u8>,
}

impl NamedComputation {
    #[must_use]
    pub const fn new(name: ComputationName, input_bytes: Vec<u8>) -> Self {
        Self { name, input_bytes }
    }

    /// Build canonical bytes used for idempotency and auditing.
    ///
    /// Layout:
    /// `[domain_len:u32][domain][tag:u8][name_len:u32][name][input_len:u32][input]`
    ///
    /// # Errors
    ///
    /// Returns `FrankenError::OutOfRange` if name/input lengths exceed `u32`.
    pub fn canonical_request_bytes(&self) -> Result<Vec<u8>> {
        let domain = REMOTE_IDEMPOTENCY_DOMAIN.as_bytes();
        let name_bytes = self.name.canonical_name_bytes();

        let domain_len = u32::try_from(domain.len()).map_err(|_| FrankenError::OutOfRange {
            what: "remote_domain_len".to_owned(),
            value: domain.len().to_string(),
        })?;
        let name_len = u32::try_from(name_bytes.len()).map_err(|_| FrankenError::OutOfRange {
            what: "computation_name_len".to_owned(),
            value: name_bytes.len().to_string(),
        })?;
        let input_len =
            u32::try_from(self.input_bytes.len()).map_err(|_| FrankenError::OutOfRange {
                what: "computation_input_len".to_owned(),
                value: self.input_bytes.len().to_string(),
            })?;

        let mut out = Vec::with_capacity(
            4 + domain.len() + 1 + 4 + name_bytes.len() + 4 + self.input_bytes.len(),
        );
        out.extend_from_slice(&domain_len.to_le_bytes());
        out.extend_from_slice(domain);
        out.push(self.name.canonical_tag());
        out.extend_from_slice(&name_len.to_le_bytes());
        out.extend_from_slice(&name_bytes);
        out.extend_from_slice(&input_len.to_le_bytes());
        out.extend_from_slice(&self.input_bytes);
        Ok(out)
    }
}

/// Registry of allowed named remote computations.
#[derive(Debug, Clone)]
pub struct ComputationRegistry {
    allowed: HashSet<ComputationName>,
}

impl ComputationRegistry {
    #[must_use]
    pub fn new_empty() -> Self {
        Self {
            allowed: HashSet::new(),
        }
    }

    #[must_use]
    pub fn with_normative_names() -> Self {
        let mut registry = Self::new_empty();
        registry.register(ComputationName::SymbolGetRange);
        registry.register(ComputationName::SymbolPutBatch);
        registry.register(ComputationName::SegmentPut);
        registry.register(ComputationName::SegmentStat);
        registry
    }

    pub fn register(&mut self, name: ComputationName) {
        self.allowed.insert(name);
    }

    #[must_use]
    pub fn is_registered(&self, name: &ComputationName) -> bool {
        self.allowed.contains(name)
    }

    /// Validate computation is registered for dispatch.
    ///
    /// # Errors
    ///
    /// Returns `FrankenError::Unsupported` if the computation is not registered.
    pub fn validate(&self, name: &ComputationName) -> Result<()> {
        if self.is_registered(name) {
            Ok(())
        } else {
            Err(FrankenError::Unsupported)
        }
    }
}

impl Default for ComputationRegistry {
    fn default() -> Self {
        Self::with_normative_names()
    }
}

/// Structured remote-effect log context.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TraceContext {
    pub trace_id: String,
    pub saga_id: Option<Saga>,
    pub idempotency_key: Option<IdempotencyKey>,
    pub attempt: u32,
    pub ecs_epoch: u64,
    pub lab_seed: Option<u64>,
    pub schedule_fingerprint: Option<String>,
}

/// Derive deterministic idempotency key:
/// `Trunc128(BLAKE3("fsqlite:remote:v1" || request_bytes))`.
#[must_use]
pub fn derive_idempotency_key(request_bytes: &[u8]) -> IdempotencyKey {
    let mut hasher = Hasher::new();
    hasher.update(REMOTE_IDEMPOTENCY_DOMAIN.as_bytes());
    hasher.update(request_bytes);
    let digest = hasher.finalize();
    let mut out = [0_u8; 16];
    out.copy_from_slice(&digest.as_bytes()[..16]);
    IdempotencyKey::from_bytes(out)
}

#[must_use]
fn request_digest(request_bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Hasher::new();
    hasher.update(request_bytes);
    *hasher.finalize().as_bytes()
}

/// Deduplication outcome for an idempotent remote request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdempotencyDecision {
    StoredNew(Vec<u8>),
    Replayed(Vec<u8>),
}

#[derive(Debug, Clone)]
struct IdempotencyEntry {
    computation: ComputationName,
    request_digest: [u8; 32],
    outcome: Vec<u8>,
}

/// In-memory idempotency store for remote effects.
#[derive(Debug, Default)]
pub struct IdempotencyStore {
    entries: Mutex<HashMap<IdempotencyKey, IdempotencyEntry>>,
}

impl IdempotencyStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register outcome for `(key, computation, request)` or replay prior value.
    ///
    /// # Errors
    ///
    /// Returns `FrankenError::Internal` if the same idempotency key is reused
    /// with different request bytes or a different computation name.
    pub fn register_or_replay(
        &self,
        key: IdempotencyKey,
        computation: &ComputationName,
        request_bytes: &[u8],
        outcome: &[u8],
    ) -> Result<IdempotencyDecision> {
        let digest = request_digest(request_bytes);
        let mut guard = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        if let Some(existing) = guard.get(&key) {
            if existing.request_digest == digest && existing.computation == *computation {
                return Ok(IdempotencyDecision::Replayed(existing.outcome.clone()));
            }
            return Err(FrankenError::Internal(
                "idempotency conflict: same key used for different remote request".to_owned(),
            ));
        }

        guard.insert(
            key,
            IdempotencyEntry {
                computation: computation.clone(),
                request_digest: digest,
                outcome: outcome.to_vec(),
            },
        );
        drop(guard);
        Ok(IdempotencyDecision::StoredNew(outcome.to_vec()))
    }
}

/// Require a runtime RemoteCap in addition to type-level `HasRemote`.
///
/// # Errors
///
/// Returns `FrankenError::Internal` if `remote_cap` is `None`.
pub fn require_remote_cap<Caps>(_: &Cx<Caps>, remote_cap: Option<RemoteCap>) -> Result<RemoteCap>
where
    Caps: cap::SubsetOf<cap::All> + cap::HasRemote,
{
    remote_cap.ok_or_else(|| {
        FrankenError::Internal("remote capability token missing for remote effect".to_owned())
    })
}

/// Conservative default for `fsqlite.remote_max_in_flight` (balanced profile).
///
/// Formula: `clamp(P / 8, 1, 8)` where `P = available_parallelism`.
#[must_use]
pub const fn conservative_remote_max_in_flight(parallelism: usize) -> usize {
    let base = parallelism / 8;
    if base == 0 {
        1
    } else if base > MAX_BALANCED_REMOTE_IN_FLIGHT {
        MAX_BALANCED_REMOTE_IN_FLIGHT
    } else {
        base
    }
}

/// Snapshotted admission state for remote work.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AdmissionSnapshot {
    pub active_permits: usize,
    pub queue_depth: usize,
    pub total_rejected: u64,
    pub total_cancelled: u64,
}

#[cfg(feature = "native")]
#[must_use]
fn admission_now() -> AdmissionTime {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let millis = u64::try_from(millis).unwrap_or(u64::MAX);
    AdmissionTime::from_millis(millis)
}

/// Executor for remote operations guarded by a global bulkhead.
#[derive(Debug)]
pub struct Executor {
    name: &'static str,
    max_in_flight: usize,
    max_queue: usize,
    queue_timeout: Duration,
    bulkhead: AdmissionBulkhead,
    #[cfg(not(feature = "native"))]
    queued_waiters: AtomicUsize,
    #[cfg(not(feature = "native"))]
    total_rejected: AtomicU64,
    #[cfg(not(feature = "native"))]
    total_cancelled: AtomicU64,
}

impl Executor {
    /// Build executor from `PRAGMA fsqlite.remote_max_in_flight`.
    ///
    /// `0` means "auto" and resolves to the conservative balanced default.
    ///
    /// # Errors
    ///
    /// Returns `FrankenError::OutOfRange` when `remote_max_in_flight` is
    /// non-zero but invalid.
    pub fn from_pragma_remote_max_in_flight(remote_max_in_flight: usize) -> Result<Self> {
        if remote_max_in_flight == 0 {
            Ok(Self::balanced_default())
        } else {
            Self::with_max_in_flight(remote_max_in_flight)
        }
    }

    /// Create with explicit in-flight limit.
    ///
    /// # Errors
    ///
    /// Returns `FrankenError::OutOfRange` if `max_in_flight == 0`.
    pub fn with_max_in_flight(max_in_flight: usize) -> Result<Self> {
        Self::with_limits(
            REMOTE_EFFECTS_EXECUTOR_NAME,
            max_in_flight,
            0,
            Duration::ZERO,
        )
    }

    #[must_use]
    pub fn balanced_default() -> Self {
        let p = available_parallelism_or_one();
        let max_in_flight = conservative_remote_max_in_flight(p);
        Self::with_max_in_flight(max_in_flight)
            .expect("remote balanced max_in_flight is always >= 1")
    }

    /// Low-risk first production rollout slice for `bd-28z4i.6`: keep the
    /// existing conservative concurrency cap, but allow one bounded waiter so
    /// `&Cx` cancellation can unwind tiered-storage remote admission cleanly.
    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn balanced_tiered_storage_default() -> Self {
        let p = available_parallelism_or_one();
        let max_in_flight = conservative_remote_max_in_flight(p);
        Self::with_limits(
            TIERED_STORAGE_EXECUTOR_NAME,
            max_in_flight,
            DEFAULT_TIERED_STORAGE_QUEUE_DEPTH,
            DEFAULT_TIERED_STORAGE_QUEUE_TIMEOUT,
        )
        .expect("tiered storage balanced max_in_flight is always >= 1")
    }

    /// Create a named executor with explicit queue bounds.
    ///
    /// # Errors
    ///
    /// Returns `FrankenError::OutOfRange` when any configured limit does not
    /// fit the underlying admission controller.
    pub(crate) fn with_limits(
        name: &'static str,
        max_in_flight: usize,
        max_queue: usize,
        queue_timeout: Duration,
    ) -> Result<Self> {
        let max_in_flight_u32 =
            u32::try_from(max_in_flight).map_err(|_| FrankenError::OutOfRange {
                what: format!("{name}.max_in_flight"),
                value: max_in_flight.to_string(),
            })?;
        if max_in_flight_u32 == 0 {
            return Err(FrankenError::OutOfRange {
                what: format!("{name}.max_in_flight"),
                value: max_in_flight.to_string(),
            });
        }
        #[cfg(feature = "native")]
        let bulkhead = {
            let max_queue_u32 = u32::try_from(max_queue).map_err(|_| FrankenError::OutOfRange {
                what: format!("{name}.max_queue"),
                value: max_queue.to_string(),
            })?;
            AdmissionBulkhead::new(BulkheadPolicy {
                name: name.to_owned(),
                max_concurrent: max_in_flight_u32,
                max_queue: max_queue_u32,
                queue_timeout,
                weighted: false,
                on_full: None,
            })
        };
        #[cfg(not(feature = "native"))]
        let bulkhead = {
            let config = BulkheadConfig::new(max_in_flight, 0, OverflowPolicy::DropBusy)
                .ok_or_else(|| FrankenError::OutOfRange {
                    what: format!("{name}.max_in_flight"),
                    value: max_in_flight.to_string(),
                })?;
            AdmissionBulkhead::new(config)
        };

        Ok(Self {
            name,
            max_in_flight,
            max_queue,
            queue_timeout,
            bulkhead,
            #[cfg(not(feature = "native"))]
            queued_waiters: AtomicUsize::new(0),
            #[cfg(not(feature = "native"))]
            total_rejected: AtomicU64::new(0),
            #[cfg(not(feature = "native"))]
            total_cancelled: AtomicU64::new(0),
        })
    }

    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    #[must_use]
    pub const fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }

    #[must_use]
    pub const fn max_queue(&self) -> usize {
        self.max_queue
    }

    #[must_use]
    pub fn snapshot(&self) -> AdmissionSnapshot {
        #[cfg(feature = "native")]
        {
            let metrics: AdmissionBulkheadMetrics = self.bulkhead.metrics();
            AdmissionSnapshot {
                #[allow(clippy::cast_possible_truncation)]
                active_permits: metrics.active_permits as usize,
                #[allow(clippy::cast_possible_truncation)]
                queue_depth: metrics.queue_depth as usize,
                total_rejected: metrics.total_rejected,
                total_cancelled: metrics.total_cancelled,
            }
        }
        #[cfg(not(feature = "native"))]
        {
            AdmissionSnapshot {
                active_permits: self.bulkhead.in_flight(),
                queue_depth: self.queued_waiters.load(Ordering::Acquire),
                total_rejected: self.total_rejected.load(Ordering::Acquire),
                total_cancelled: self.total_cancelled.load(Ordering::Acquire),
            }
        }
    }

    #[cfg(not(feature = "native"))]
    fn reserve_fallback_waiter(&self) -> bool {
        loop {
            let current = self.queued_waiters.load(Ordering::Acquire);
            if current >= self.max_queue {
                return false;
            }
            let next = current + 1;
            if self
                .queued_waiters
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn try_acquire_for_testing(&self) -> Option<AdmissionPermit<'_>> {
        #[cfg(feature = "native")]
        {
            self.bulkhead.try_acquire(1)
        }
        #[cfg(not(feature = "native"))]
        {
            self.bulkhead.try_acquire().ok()
        }
    }

    pub(crate) fn run<Caps, T, F>(
        &self,
        cx: &Cx<Caps>,
        effect_name: &str,
        saga: Option<Saga>,
        idempotency_key: Option<IdempotencyKey>,
        ecs_epoch: u64,
        operation: F,
    ) -> Result<T>
    where
        Caps: cap::SubsetOf<cap::All>,
        F: FnOnce() -> Result<T>,
    {
        let _permit = self.acquire(cx, effect_name, saga, idempotency_key, ecs_epoch)?;
        cx.checkpoint().map_err(|_| {
            let snapshot = self.snapshot();
            warn!(
                bead_id = BEAD_ID,
                executor = self.name,
                effect_name,
                saga_id = format_saga(saga),
                idempotency_key = format_key(idempotency_key),
                ecs_epoch,
                admission = "cancelled_post_acquire",
                active_permits = snapshot.active_permits,
                queue_depth = snapshot.queue_depth,
                total_cancelled = snapshot.total_cancelled,
                "remote operation cancelled after admission and before dispatch"
            );
            FrankenError::Busy
        })?;
        match operation() {
            Ok(out) => {
                let snapshot = self.snapshot();
                info!(
                    bead_id = BEAD_ID,
                    executor = self.name,
                    effect_name,
                    saga_id = format_saga(saga),
                    idempotency_key = format_key(idempotency_key),
                    ecs_epoch,
                    active_permits = snapshot.active_permits,
                    queue_depth = snapshot.queue_depth,
                    "remote operation completed under admission control"
                );
                Ok(out)
            }
            Err(err) => {
                let snapshot = self.snapshot();
                warn!(
                    bead_id = BEAD_ID,
                    executor = self.name,
                    effect_name,
                    saga_id = format_saga(saga),
                    idempotency_key = format_key(idempotency_key),
                    ecs_epoch,
                    active_permits = snapshot.active_permits,
                    queue_depth = snapshot.queue_depth,
                    error = %err,
                    "remote operation failed under admission control"
                );
                Err(err)
            }
        }
    }

    fn acquire<Caps>(
        &self,
        cx: &Cx<Caps>,
        effect_name: &str,
        saga: Option<Saga>,
        idempotency_key: Option<IdempotencyKey>,
        ecs_epoch: u64,
    ) -> Result<AdmissionPermit<'_>>
    where
        Caps: cap::SubsetOf<cap::All>,
    {
        cx.checkpoint().map_err(|_| FrankenError::Busy)?;

        #[cfg(feature = "native")]
        {
            if let Some(permit) = self.bulkhead.try_acquire(1) {
                let snapshot = self.snapshot();
                debug!(
                    bead_id = BEAD_ID,
                    executor = self.name,
                    effect_name,
                    saga_id = format_saga(saga),
                    idempotency_key = format_key(idempotency_key),
                    ecs_epoch,
                    admission = "immediate",
                    active_permits = snapshot.active_permits,
                    queue_depth = snapshot.queue_depth,
                    max_in_flight = self.max_in_flight,
                    max_queue = self.max_queue,
                    "remote admission granted immediately"
                );
                return Ok(permit);
            }

            let snapshot = self.snapshot();
            if self.max_queue == 0 {
                warn!(
                    bead_id = BEAD_ID,
                    executor = self.name,
                    effect_name,
                    saga_id = format_saga(saga),
                    idempotency_key = format_key(idempotency_key),
                    ecs_epoch,
                    admission = "rejected",
                    active_permits = snapshot.active_permits,
                    queue_depth = snapshot.queue_depth,
                    total_rejected = snapshot.total_rejected,
                    "remote admission saturated"
                );
                return Err(FrankenError::Busy);
            }

            let queued_at = admission_now();
            let entry_id = match self.bulkhead.enqueue(1, queued_at) {
                Ok(entry_id) => entry_id,
                Err(AdmissionBulkheadError::Full | AdmissionBulkheadError::QueueFull) => {
                    let snapshot = self.snapshot();
                    warn!(
                        bead_id = BEAD_ID,
                        executor = self.name,
                        effect_name,
                        saga_id = format_saga(saga),
                        idempotency_key = format_key(idempotency_key),
                        ecs_epoch,
                        admission = "rejected",
                        active_permits = snapshot.active_permits,
                        queue_depth = snapshot.queue_depth,
                        total_rejected = snapshot.total_rejected,
                        "remote admission queue full"
                    );
                    return Err(FrankenError::Busy);
                }
                Err(
                    AdmissionBulkheadError::QueueTimeout { .. } | AdmissionBulkheadError::Cancelled,
                ) => {
                    return Err(FrankenError::Busy);
                }
                Err(AdmissionBulkheadError::Inner(())) => unreachable!(),
            };

            let snapshot = self.snapshot();
            debug!(
                bead_id = BEAD_ID,
                executor = self.name,
                effect_name,
                saga_id = format_saga(saga),
                idempotency_key = format_key(idempotency_key),
                ecs_epoch,
                admission = "queued",
                entry_id,
                queue_depth = snapshot.queue_depth,
                queue_timeout_ms = self.queue_timeout.as_millis(),
                "remote admission queued"
            );

            loop {
                if cx.checkpoint().is_err() {
                    self.bulkhead.cancel_entry(entry_id, AdmissionTime::ZERO);
                    let snapshot = self.snapshot();
                    warn!(
                        bead_id = BEAD_ID,
                        executor = self.name,
                        effect_name,
                        saga_id = format_saga(saga),
                        idempotency_key = format_key(idempotency_key),
                        ecs_epoch,
                        admission = "cancelled",
                        entry_id,
                        queue_depth = snapshot.queue_depth,
                        total_cancelled = snapshot.total_cancelled,
                        "remote admission cancelled while waiting"
                    );
                    return Err(FrankenError::Busy);
                }

                let now = admission_now();
                match self.bulkhead.check_entry(entry_id, now) {
                    Ok(Some(permit)) => {
                        let waited_ms = now.as_millis().saturating_sub(queued_at.as_millis());
                        let snapshot = self.snapshot();
                        debug!(
                            bead_id = BEAD_ID,
                            executor = self.name,
                            effect_name,
                            saga_id = format_saga(saga),
                            idempotency_key = format_key(idempotency_key),
                            ecs_epoch,
                            admission = "dequeued",
                            entry_id,
                            waited_ms,
                            active_permits = snapshot.active_permits,
                            queue_depth = snapshot.queue_depth,
                            "remote admission granted from queue"
                        );
                        return Ok(permit);
                    }
                    Ok(None) => thread::sleep(ADMISSION_POLL_INTERVAL),
                    Err(AdmissionBulkheadError::QueueTimeout { waited }) => {
                        let snapshot = self.snapshot();
                        warn!(
                            bead_id = BEAD_ID,
                            executor = self.name,
                            effect_name,
                            saga_id = format_saga(saga),
                            idempotency_key = format_key(idempotency_key),
                            ecs_epoch,
                            admission = "timed_out",
                            entry_id,
                            waited_ms = waited.as_millis(),
                            queue_depth = snapshot.queue_depth,
                            total_rejected = snapshot.total_rejected,
                            "remote admission queue timeout"
                        );
                        return Err(FrankenError::Busy);
                    }
                    Err(
                        AdmissionBulkheadError::Cancelled
                        | AdmissionBulkheadError::Full
                        | AdmissionBulkheadError::QueueFull,
                    ) => {
                        return Err(FrankenError::Busy);
                    }
                    Err(AdmissionBulkheadError::Inner(())) => unreachable!(),
                }
            }
        }

        #[cfg(not(feature = "native"))]
        {
            if let Ok(permit) = self.bulkhead.try_acquire() {
                let snapshot = self.snapshot();
                debug!(
                    bead_id = BEAD_ID,
                    executor = self.name,
                    effect_name,
                    saga_id = format_saga(saga),
                    idempotency_key = format_key(idempotency_key),
                    ecs_epoch,
                    admission = "immediate",
                    active_permits = snapshot.active_permits,
                    queue_depth = snapshot.queue_depth,
                    max_in_flight = self.max_in_flight,
                    max_queue = self.max_queue,
                    "remote admission granted via local fallback bulkhead"
                );
                return Ok(permit);
            }

            if self.max_queue == 0 {
                self.total_rejected.fetch_add(1, Ordering::AcqRel);
                let snapshot = self.snapshot();
                warn!(
                    bead_id = BEAD_ID,
                    executor = self.name,
                    effect_name,
                    saga_id = format_saga(saga),
                    idempotency_key = format_key(idempotency_key),
                    ecs_epoch,
                    admission = "rejected",
                    active_permits = snapshot.active_permits,
                    queue_depth = snapshot.queue_depth,
                    total_rejected = snapshot.total_rejected,
                    "remote admission saturated"
                );
                return Err(FrankenError::Busy);
            }

            if !self.reserve_fallback_waiter() {
                self.total_rejected.fetch_add(1, Ordering::AcqRel);
                let snapshot = self.snapshot();
                warn!(
                    bead_id = BEAD_ID,
                    executor = self.name,
                    effect_name,
                    saga_id = format_saga(saga),
                    idempotency_key = format_key(idempotency_key),
                    ecs_epoch,
                    admission = "rejected",
                    active_permits = snapshot.active_permits,
                    queue_depth = snapshot.queue_depth,
                    total_rejected = snapshot.total_rejected,
                    "remote admission queue full"
                );
                return Err(FrankenError::Busy);
            }

            let queued_at = Instant::now();
            let queued = FallbackQueueReservation::new(&self.queued_waiters);
            let snapshot = self.snapshot();
            debug!(
                bead_id = BEAD_ID,
                executor = self.name,
                effect_name,
                saga_id = format_saga(saga),
                idempotency_key = format_key(idempotency_key),
                ecs_epoch,
                admission = "queued",
                queue_depth = snapshot.queue_depth,
                queue_timeout_ms = self.queue_timeout.as_millis(),
                "remote admission queued via local fallback bulkhead"
            );

            loop {
                if cx.checkpoint().is_err() {
                    self.total_cancelled.fetch_add(1, Ordering::AcqRel);
                    queued.release();
                    let snapshot = self.snapshot();
                    warn!(
                        bead_id = BEAD_ID,
                        executor = self.name,
                        effect_name,
                        saga_id = format_saga(saga),
                        idempotency_key = format_key(idempotency_key),
                        ecs_epoch,
                        admission = "cancelled",
                        queue_depth = snapshot.queue_depth,
                        total_cancelled = snapshot.total_cancelled,
                        "remote admission cancelled while waiting"
                    );
                    return Err(FrankenError::Busy);
                }

                match self.bulkhead.try_acquire() {
                    Ok(permit) => {
                        let waited_ms =
                            u64::try_from(queued_at.elapsed().as_millis()).unwrap_or(u64::MAX);
                        queued.release();
                        let snapshot = self.snapshot();
                        debug!(
                            bead_id = BEAD_ID,
                            executor = self.name,
                            effect_name,
                            saga_id = format_saga(saga),
                            idempotency_key = format_key(idempotency_key),
                            ecs_epoch,
                            admission = "dequeued",
                            waited_ms,
                            active_permits = snapshot.active_permits,
                            queue_depth = snapshot.queue_depth,
                            "remote admission granted from local fallback queue"
                        );
                        return Ok(permit);
                    }
                    Err(FrankenError::Busy) => {
                        if queued_at.elapsed() >= self.queue_timeout {
                            self.total_rejected.fetch_add(1, Ordering::AcqRel);
                            let waited_ms =
                                u64::try_from(queued_at.elapsed().as_millis()).unwrap_or(u64::MAX);
                            queued.release();
                            let snapshot = self.snapshot();
                            warn!(
                                bead_id = BEAD_ID,
                                executor = self.name,
                                effect_name,
                                saga_id = format_saga(saga),
                                idempotency_key = format_key(idempotency_key),
                                ecs_epoch,
                                admission = "timed_out",
                                waited_ms,
                                queue_depth = snapshot.queue_depth,
                                total_rejected = snapshot.total_rejected,
                                "remote admission queue timeout"
                            );
                            return Err(FrankenError::Busy);
                        }
                        thread::sleep(ADMISSION_POLL_INTERVAL);
                    }
                    Err(err) => return Err(err),
                }
            }
        }
    }

    /// Execute a named remote computation through the global remote bulkhead.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - `FrankenError::Internal` when `remote_cap` is absent,
    /// - `FrankenError::Unsupported` for unregistered computations,
    /// - `FrankenError::Busy` when the remote bulkhead is saturated,
    /// - or any error from `operation`.
    pub fn execute<Caps, F>(
        &self,
        cx: &Cx<Caps>,
        remote_cap: Option<RemoteCap>,
        registry: &ComputationRegistry,
        computation: &NamedComputation,
        trace: &TraceContext,
        operation: F,
    ) -> Result<Vec<u8>>
    where
        Caps: cap::SubsetOf<cap::All> + cap::HasRemote,
        F: FnOnce() -> Result<Vec<u8>>,
    {
        let _cap = require_remote_cap(cx, remote_cap)?;
        registry.validate(&computation.name)?;
        debug!(
            bead_id = BEAD_ID,
            trace_id = trace.trace_id,
            effect_name = computation.name.as_str(),
            saga_id = format_saga(trace.saga_id),
            idempotency_key = format_key(trace.idempotency_key),
            attempt = trace.attempt,
            ecs_epoch = trace.ecs_epoch,
            lab_seed = ?trace.lab_seed,
            schedule_fingerprint = ?trace.schedule_fingerprint,
            "dispatching named remote computation"
        );
        let out = self.run(
            cx,
            computation.name.as_str(),
            trace.saga_id,
            trace.idempotency_key,
            trace.ecs_epoch,
            operation,
        )?;

        info!(
            bead_id = BEAD_ID,
            trace_id = trace.trace_id,
            effect_name = computation.name.as_str(),
            saga_id = format_saga(trace.saga_id),
            idempotency_key = format_key(trace.idempotency_key),
            attempt = trace.attempt,
            ecs_epoch = trace.ecs_epoch,
            "remote computation completed"
        );

        Ok(out)
    }
}

/// Lease-expiry escalation policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseEscalation {
    Cancel,
    Retry,
    Fail,
}

/// Lease liveness result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseStatus {
    Live,
    Expired { escalation: LeaseEscalation },
}

/// Lease-backed remote handle metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseBackedHandle {
    pub lease_id: u64,
    pub issued_at_millis: u64,
    pub ttl_millis: u64,
    pub escalation: LeaseEscalation,
}

impl LeaseBackedHandle {
    /// Create a lease-backed handle.
    ///
    /// # Errors
    ///
    /// Returns `FrankenError::OutOfRange` if `ttl_millis == 0`.
    pub fn new(
        lease_id: u64,
        issued_at_millis: u64,
        ttl_millis: u64,
        escalation: LeaseEscalation,
    ) -> Result<Self> {
        if ttl_millis == 0 {
            return Err(FrankenError::OutOfRange {
                what: "lease_ttl_millis".to_owned(),
                value: "0".to_owned(),
            });
        }
        Ok(Self {
            lease_id,
            issued_at_millis,
            ttl_millis,
            escalation,
        })
    }

    #[must_use]
    pub fn evaluate(&self, now_millis: u64) -> LeaseStatus {
        let age_millis = now_millis.saturating_sub(self.issued_at_millis);
        if age_millis >= self.ttl_millis {
            LeaseStatus::Expired {
                escalation: self.escalation,
            }
        } else {
            LeaseStatus::Live
        }
    }

    /// Enforce lease validity and map expiry to deterministic escalation errors.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - `FrankenError::Busy` for `Cancel`,
    /// - `FrankenError::BusyRecovery` for `Retry`,
    /// - `FrankenError::LockFailed` for `Fail`.
    pub fn enforce(&self, now_millis: u64, trace: &TraceContext) -> Result<()> {
        match self.evaluate(now_millis) {
            LeaseStatus::Live => Ok(()),
            LeaseStatus::Expired { escalation } => {
                warn!(
                    bead_id = BEAD_ID,
                    trace_id = trace.trace_id,
                    lease_id = self.lease_id,
                    effect_name = "lease_expiry",
                    saga_id = format_saga(trace.saga_id),
                    idempotency_key = format_key(trace.idempotency_key),
                    attempt = trace.attempt,
                    ecs_epoch = trace.ecs_epoch,
                    escalation = ?escalation,
                    "remote lease expired; escalating"
                );
                match escalation {
                    LeaseEscalation::Cancel => Err(FrankenError::Busy),
                    LeaseEscalation::Retry => Err(FrankenError::BusyRecovery),
                    LeaseEscalation::Fail => Err(FrankenError::LockFailed {
                        detail: "remote lease expired".to_owned(),
                    }),
                }
            }
        }
    }
}

/// Local deterministic remote segment store for tests.
#[derive(Debug, Default)]
pub struct InMemoryRemoteStore {
    segments: HashMap<ObjectId, Vec<u8>>,
    uploads: HashMap<IdempotencyKey, UploadRecord>,
    upload_count: HashMap<ObjectId, u64>,
}

#[derive(Debug, Clone)]
struct UploadRecord {
    segment_id: ObjectId,
    payload_digest: [u8; 32],
}

impl InMemoryRemoteStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Idempotent segment upload keyed by idempotency key.
    ///
    /// # Errors
    ///
    /// Returns `FrankenError::Internal` when an existing idempotency key is
    /// reused with a different segment/payload.
    pub fn put_segment(
        &mut self,
        segment_id: ObjectId,
        payload: &[u8],
        key: IdempotencyKey,
    ) -> Result<()> {
        let digest = request_digest(payload);
        if let Some(existing) = self.uploads.get(&key) {
            if existing.segment_id == segment_id && existing.payload_digest == digest {
                // Preserve idempotency while ensuring deterministic replay can
                // reconstruct remote-visible state after compensation cleanup.
                self.segments
                    .entry(segment_id)
                    .or_insert_with(|| payload.to_vec());
                return Ok(());
            }
            return Err(FrankenError::Internal(
                "remote put conflict: idempotency key reused with different payload".to_owned(),
            ));
        }

        self.uploads.insert(
            key,
            UploadRecord {
                segment_id,
                payload_digest: digest,
            },
        );
        self.segments.insert(segment_id, payload.to_vec());
        *self.upload_count.entry(segment_id).or_insert(0) += 1;
        Ok(())
    }

    #[must_use]
    pub fn has_segment(&self, segment_id: ObjectId) -> bool {
        self.segments.contains_key(&segment_id)
    }

    #[must_use]
    pub fn upload_count(&self, segment_id: ObjectId) -> u64 {
        *self.upload_count.get(&segment_id).unwrap_or(&0)
    }

    pub fn remove_segment(&mut self, segment_id: ObjectId) -> bool {
        self.segments.remove(&segment_id).is_some()
    }
}

/// Eviction saga phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPhase {
    Init,
    Uploaded,
    Verified,
    Retired,
    Cancelled,
}

/// Local segment state during eviction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalSegmentState {
    Present,
    Retired,
}

/// Compensation outcome when cancelling an eviction saga.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionCompensation {
    LocalRetained,
    RollbackRequired,
}

/// L2->L3 eviction saga skeleton (`upload -> verify -> retire`).
#[derive(Debug)]
pub struct EvictionSaga {
    saga: Saga,
    segment_id: ObjectId,
    phase: EvictionPhase,
    local_state: LocalSegmentState,
    upload_idempotency_key: IdempotencyKey,
}

impl EvictionSaga {
    #[must_use]
    pub fn new(saga: Saga, segment_id: ObjectId) -> Self {
        Self {
            upload_idempotency_key: derive_step_key(saga.key(), segment_id, b"segment_put"),
            saga,
            segment_id,
            phase: EvictionPhase::Init,
            local_state: LocalSegmentState::Present,
        }
    }

    #[must_use]
    pub const fn phase(&self) -> EvictionPhase {
        self.phase
    }

    #[must_use]
    pub const fn local_state(&self) -> LocalSegmentState {
        self.local_state
    }

    #[must_use]
    pub const fn upload_idempotency_key(&self) -> IdempotencyKey {
        self.upload_idempotency_key
    }

    /// Upload step (`segment_put`).
    ///
    /// # Errors
    ///
    /// Returns `FrankenError::Internal` when called from an invalid phase or if
    /// remote idempotency validation fails.
    pub fn upload(&mut self, remote: &mut InMemoryRemoteStore, bytes: &[u8]) -> Result<()> {
        if !matches!(self.phase, EvictionPhase::Init | EvictionPhase::Cancelled) {
            return Err(FrankenError::Internal(format!(
                "eviction upload invalid in phase {:?}",
                self.phase
            )));
        }
        remote.put_segment(self.segment_id, bytes, self.upload_idempotency_key)?;
        self.phase = EvictionPhase::Uploaded;
        Ok(())
    }

    /// Verify step (`segment_stat`).
    ///
    /// # Errors
    ///
    /// Returns `FrankenError::Internal` when called from an invalid phase or if
    /// the segment is not present remotely.
    pub fn verify_remote(&mut self, remote: &InMemoryRemoteStore) -> Result<()> {
        if self.phase != EvictionPhase::Uploaded {
            return Err(FrankenError::Internal(format!(
                "eviction verify invalid in phase {:?}",
                self.phase
            )));
        }
        if !remote.has_segment(self.segment_id) {
            return Err(FrankenError::Internal(
                "segment verification failed: missing in remote store".to_owned(),
            ));
        }
        self.phase = EvictionPhase::Verified;
        Ok(())
    }

    /// Retire local segment after remote verification.
    ///
    /// # Errors
    ///
    /// Returns `FrankenError::Internal` when called from an invalid phase.
    pub fn retire_local(&mut self) -> Result<()> {
        if self.phase != EvictionPhase::Verified {
            return Err(FrankenError::Internal(format!(
                "eviction retire invalid in phase {:?}",
                self.phase
            )));
        }
        self.local_state = LocalSegmentState::Retired;
        self.phase = EvictionPhase::Retired;
        Ok(())
    }

    /// Cancel saga; before retire we retain local data for safe replay.
    #[must_use]
    pub fn cancel(&mut self) -> EvictionCompensation {
        if self.phase == EvictionPhase::Retired {
            EvictionCompensation::RollbackRequired
        } else {
            self.phase = EvictionPhase::Cancelled;
            self.local_state = LocalSegmentState::Present;
            debug!(
                bead_id = BEAD_ID,
                saga_id = format_key(Some(self.saga.key())),
                "eviction saga cancelled; local segment retained"
            );
            EvictionCompensation::LocalRetained
        }
    }
}

/// Compaction publish saga phase (`write segments -> publish -> update locator`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionPhase {
    Init,
    SegmentsStaged,
    Published,
    LocatorUpdated,
    Cancelled,
}

/// Compensation outcome when cancelling compaction publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionCompensation {
    RemoteCleaned,
    RollbackRequired,
}

/// Compaction publication saga skeleton with deterministic compensation.
#[derive(Debug)]
pub struct CompactionPublishSaga {
    saga: Saga,
    manifest_id: ObjectId,
    staged_segments: Vec<ObjectId>,
    phase: CompactionPhase,
    locator_updated: bool,
}

impl CompactionPublishSaga {
    #[must_use]
    pub fn new(saga: Saga, manifest_id: ObjectId) -> Self {
        Self {
            saga,
            manifest_id,
            staged_segments: Vec::new(),
            phase: CompactionPhase::Init,
            locator_updated: false,
        }
    }

    #[must_use]
    pub const fn phase(&self) -> CompactionPhase {
        self.phase
    }

    #[must_use]
    pub const fn locator_updated(&self) -> bool {
        self.locator_updated
    }

    /// Stage replacement segments for compaction publication.
    ///
    /// # Errors
    ///
    /// Returns `FrankenError::OutOfRange` when `segments` is empty, or
    /// `FrankenError::Internal` when called from an invalid phase.
    pub fn stage_segments(
        &mut self,
        remote: &mut InMemoryRemoteStore,
        segments: &[(ObjectId, Vec<u8>)],
    ) -> Result<()> {
        if !matches!(
            self.phase,
            CompactionPhase::Init | CompactionPhase::Cancelled
        ) {
            return Err(FrankenError::Internal(format!(
                "compaction stage invalid in phase {:?}",
                self.phase
            )));
        }
        if segments.is_empty() {
            return Err(FrankenError::OutOfRange {
                what: "compaction_segments".to_owned(),
                value: "0".to_owned(),
            });
        }

        self.staged_segments.clear();
        for (segment_id, payload) in segments {
            let key = derive_step_key(self.saga.key(), *segment_id, b"compaction_segment_put");
            remote.put_segment(*segment_id, payload, key)?;
            self.staged_segments.push(*segment_id);
        }
        self.phase = CompactionPhase::SegmentsStaged;
        self.locator_updated = false;
        Ok(())
    }

    /// Publish a compaction manifest after segment staging.
    ///
    /// # Errors
    ///
    /// Returns `FrankenError::Internal` when called from an invalid phase.
    pub fn publish_manifest(
        &mut self,
        remote: &mut InMemoryRemoteStore,
        manifest: &[u8],
    ) -> Result<()> {
        if self.phase != CompactionPhase::SegmentsStaged {
            return Err(FrankenError::Internal(format!(
                "compaction publish invalid in phase {:?}",
                self.phase
            )));
        }
        let key = derive_step_key(
            self.saga.key(),
            self.manifest_id,
            b"compaction_manifest_publish",
        );
        remote.put_segment(self.manifest_id, manifest, key)?;
        self.phase = CompactionPhase::Published;
        Ok(())
    }

    /// Update locators/manifests to point at the newly published compaction output.
    ///
    /// # Errors
    ///
    /// Returns `FrankenError::Internal` when called from an invalid phase.
    pub fn update_locator(&mut self) -> Result<()> {
        if self.phase != CompactionPhase::Published {
            return Err(FrankenError::Internal(format!(
                "compaction locator update invalid in phase {:?}",
                self.phase
            )));
        }
        self.locator_updated = true;
        self.phase = CompactionPhase::LocatorUpdated;
        Ok(())
    }

    /// Cancel compaction publication.
    ///
    /// Before locator update, deterministic compensation removes staged remote
    /// objects and leaves local locator state unchanged.
    #[must_use]
    pub fn cancel(&mut self, remote: &mut InMemoryRemoteStore) -> CompactionCompensation {
        match self.phase {
            CompactionPhase::LocatorUpdated => CompactionCompensation::RollbackRequired,
            CompactionPhase::Init | CompactionPhase::Cancelled => {
                self.phase = CompactionPhase::Cancelled;
                self.locator_updated = false;
                CompactionCompensation::RemoteCleaned
            }
            CompactionPhase::SegmentsStaged | CompactionPhase::Published => {
                for segment in &self.staged_segments {
                    let _ = remote.remove_segment(*segment);
                }
                let _ = remote.remove_segment(self.manifest_id);
                self.phase = CompactionPhase::Cancelled;
                self.locator_updated = false;
                debug!(
                    bead_id = BEAD_ID,
                    saga_id = format_key(Some(self.saga.key())),
                    "compaction saga cancelled; remote staged objects cleaned"
                );
                CompactionCompensation::RemoteCleaned
            }
        }
    }
}

#[must_use]
fn derive_step_key(
    base_key: IdempotencyKey,
    object_id: ObjectId,
    step_tag: &[u8],
) -> IdempotencyKey {
    let mut bytes = Vec::with_capacity(16 + 16 + step_tag.len());
    bytes.extend_from_slice(base_key.as_bytes());
    bytes.extend_from_slice(object_id.as_bytes());
    bytes.extend_from_slice(step_tag);
    derive_idempotency_key(&bytes)
}

#[must_use]
fn format_key(key: Option<IdempotencyKey>) -> String {
    key.map_or_else(|| "-".to_owned(), |k| hex16(k.as_bytes()))
}

#[must_use]
fn format_saga(saga: Option<Saga>) -> String {
    saga.map_or_else(|| "-".to_owned(), |s| hex16(s.key().as_bytes()))
}

#[must_use]
fn hex16(bytes: &[u8; 16]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(32);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(not(feature = "native"))]
#[derive(Debug)]
struct FallbackQueueReservation<'a> {
    queued_waiters: &'a AtomicUsize,
    released: bool,
}

#[cfg(not(feature = "native"))]
impl FallbackQueueReservation<'_> {
    fn new(queued_waiters: &AtomicUsize) -> FallbackQueueReservation<'_> {
        FallbackQueueReservation {
            queued_waiters,
            released: false,
        }
    }

    fn release(mut self) {
        if !self.released {
            self.queued_waiters.fetch_sub(1, Ordering::AcqRel);
            self.released = true;
        }
    }
}

#[cfg(not(feature = "native"))]
impl Drop for FallbackQueueReservation<'_> {
    fn drop(&mut self) {
        if !self.released {
            self.queued_waiters.fetch_sub(1, Ordering::AcqRel);
            self.released = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;
    #[cfg(not(feature = "native"))]
    use std::time::Instant;

    use super::*;

    fn cap_token(seed: u8) -> RemoteCap {
        RemoteCap::from_bytes([seed; 16])
    }

    fn segment_id(seed: u8) -> ObjectId {
        ObjectId::from_bytes([seed; 16])
    }

    #[cfg(not(feature = "native"))]
    fn wait_for(condition: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_millis(250);
        while Instant::now() < deadline {
            if condition() {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        assert!(condition(), "timed out waiting for condition");
    }

    #[test]
    fn test_remote_cap_required_for_network_io() {
        let cx = Cx::<cap::All>::new();
        let registry = ComputationRegistry::default();
        let executor = Executor::with_max_in_flight(1).unwrap();
        let computation = NamedComputation::new(ComputationName::SegmentStat, vec![1, 2, 3]);
        let trace = TraceContext::default();

        let err = executor
            .execute(&cx, None, &registry, &computation, &trace, || {
                Ok(vec![0xAA])
            })
            .unwrap_err();

        assert!(matches!(err, FrankenError::Internal(_)));
    }

    #[test]
    fn test_remote_cap_omitted_in_lab_fails_gracefully() {
        let cx = Cx::<cap::All>::new();
        let registry = ComputationRegistry::default();
        let executor = Executor::with_max_in_flight(1).unwrap();
        let computation = NamedComputation::new(ComputationName::SegmentStat, vec![0xAA]);
        let trace = TraceContext {
            trace_id: "lab-no-remote".to_owned(),
            lab_seed: Some(17),
            schedule_fingerprint: Some("sched-A".to_owned()),
            ..TraceContext::default()
        };

        let err = executor
            .execute(&cx, None, &registry, &computation, &trace, || {
                Ok(vec![0xBB])
            })
            .unwrap_err();
        assert!(matches!(err, FrankenError::Internal(_)));
    }

    #[test]
    fn test_named_computation_registry_and_unregistered_rejection() {
        let mut registry = ComputationRegistry::new_empty();
        registry.register(ComputationName::SymbolGetRange);
        registry.register(ComputationName::SymbolPutBatch);
        registry.register(ComputationName::SegmentPut);
        registry.register(ComputationName::SegmentStat);

        assert!(registry.validate(&ComputationName::SegmentPut).is_ok());
        assert!(
            registry
                .validate(&ComputationName::Custom("unregistered".to_owned()))
                .is_err()
        );
    }

    #[test]
    fn test_named_computation_no_closure_shipping_canonical_bytes_deterministic() {
        let computation = NamedComputation::new(
            ComputationName::SymbolGetRange,
            b"obj=01;esi=0..7;epoch=2".to_vec(),
        );
        let bytes_a = computation.canonical_request_bytes().unwrap();
        let bytes_b = computation.canonical_request_bytes().unwrap();
        assert_eq!(bytes_a, bytes_b);
        let domain = REMOTE_IDEMPOTENCY_DOMAIN.as_bytes();
        assert!(bytes_a.windows(domain.len()).any(|window| window == domain));
    }

    #[test]
    fn test_idempotency_key_deterministic() {
        let request = b"segment_put:abc";
        let key_a = derive_idempotency_key(request);
        let key_b = derive_idempotency_key(request);
        assert_eq!(key_a, key_b);
    }

    #[test]
    fn test_idempotency_dedup_same_key_same_input() {
        let store = IdempotencyStore::new();
        let computation = ComputationName::SegmentPut;
        let request = b"segment_put:id=1";
        let key = derive_idempotency_key(request);

        let first = store
            .register_or_replay(key, &computation, request, b"ok:first")
            .unwrap();
        let second = store
            .register_or_replay(key, &computation, request, b"ok:second")
            .unwrap();

        assert!(matches!(first, IdempotencyDecision::StoredNew(_)));
        assert_eq!(second, IdempotencyDecision::Replayed(b"ok:first".to_vec()));
    }

    #[test]
    fn test_idempotency_conflict_same_key_different_input() {
        let store = IdempotencyStore::new();
        let computation = ComputationName::SegmentPut;
        let first_request = b"segment_put:id=1";
        let second_request = b"segment_put:id=2";
        let key = derive_idempotency_key(first_request);

        let _ = store
            .register_or_replay(key, &computation, first_request, b"ok:first")
            .unwrap();

        let err = store
            .register_or_replay(key, &computation, second_request, b"ok:second")
            .unwrap_err();
        assert!(matches!(err, FrankenError::Internal(_)));
    }

    #[test]
    fn test_lease_backed_liveness_expiry() {
        let trace = TraceContext {
            trace_id: "trace-lease".to_owned(),
            attempt: 1,
            ecs_epoch: 7,
            ..TraceContext::default()
        };
        let handle = LeaseBackedHandle::new(42, 1_000, 100, LeaseEscalation::Retry).unwrap();
        let status = handle.evaluate(1_200);
        assert_eq!(
            status,
            LeaseStatus::Expired {
                escalation: LeaseEscalation::Retry
            }
        );
        let err = handle.enforce(1_200, &trace).unwrap_err();
        assert!(matches!(err, FrankenError::BusyRecovery));
    }

    #[test]
    fn test_e2e_remote_effects_saga_eviction_idempotent_restart() {
        let saga_key = derive_idempotency_key(b"saga:evict:segment-9");
        let saga_id = Saga::new(saga_key);
        let target_segment = segment_id(9);
        let payload = b"segment payload".to_vec();

        let mut remote = InMemoryRemoteStore::new();

        let mut first = EvictionSaga::new(saga_id, target_segment);
        first.upload(&mut remote, &payload).unwrap();
        let compensation = first.cancel();
        assert_eq!(compensation, EvictionCompensation::LocalRetained);
        assert_eq!(first.local_state(), LocalSegmentState::Present);

        let mut restart = EvictionSaga::new(saga_id, target_segment);
        restart.upload(&mut remote, &payload).unwrap();
        restart.verify_remote(&remote).unwrap();
        restart.retire_local().unwrap();

        assert_eq!(restart.local_state(), LocalSegmentState::Retired);
        assert_eq!(remote.upload_count(target_segment), 1);
    }

    #[test]
    fn test_remote_bulkhead_concurrency_cap() {
        let executor = Arc::new(Executor::with_max_in_flight(2).unwrap());
        let registry = Arc::new(ComputationRegistry::default());
        let computation = Arc::new(NamedComputation::new(ComputationName::SegmentStat, vec![1]));

        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let busy = Arc::new(AtomicUsize::new(0));

        let start = Arc::new(std::sync::Barrier::new(5));
        let mut workers = Vec::new();
        for _ in 0..5 {
            let exec = Arc::clone(&executor);
            let reg = Arc::clone(&registry);
            let comp = Arc::clone(&computation);
            let active_ctr = Arc::clone(&active);
            let peak_ctr = Arc::clone(&peak);
            let busy_ctr = Arc::clone(&busy);
            let barrier = Arc::clone(&start);
            workers.push(thread::spawn(move || {
                let cx = Cx::<cap::All>::new();
                let trace = TraceContext::default();
                barrier.wait();
                let result = exec.execute(&cx, Some(cap_token(7)), &reg, &comp, &trace, || {
                    let now = active_ctr.fetch_add(1, Ordering::AcqRel) + 1;
                    peak_ctr.fetch_max(now, Ordering::AcqRel);
                    thread::sleep(Duration::from_millis(40));
                    active_ctr.fetch_sub(1, Ordering::AcqRel);
                    Ok(vec![1, 2, 3])
                });
                if matches!(result, Err(FrankenError::Busy)) {
                    busy_ctr.fetch_add(1, Ordering::AcqRel);
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }

        assert!(busy.load(Ordering::Acquire) >= 3);
        assert!(peak.load(Ordering::Acquire) <= 2);
    }

    #[test]
    fn test_remote_bulkhead_zero_means_auto() {
        let expected = conservative_remote_max_in_flight(available_parallelism_or_one());
        let executor = Executor::from_pragma_remote_max_in_flight(0).unwrap();
        assert_eq!(executor.max_in_flight(), expected);
    }

    #[cfg(not(feature = "native"))]
    #[test]
    fn test_remote_bulkhead_fallback_queue_waits_for_capacity() {
        let executor = Arc::new(
            Executor::with_limits("fallback.test", 1, 1, Duration::from_millis(100)).unwrap(),
        );
        let held = executor.try_acquire_for_testing().unwrap();

        let exec = Arc::clone(&executor);
        let waiter = thread::spawn(move || {
            let cx = Cx::<cap::All>::new();
            let permit = exec.acquire(&cx, "segment_stat", None, None, 0).unwrap();
            drop(permit);
        });

        wait_for(|| executor.snapshot().queue_depth == 1);
        assert_eq!(executor.snapshot().active_permits, 1);

        drop(held);
        waiter.join().unwrap();

        let snapshot = executor.snapshot();
        assert_eq!(snapshot.queue_depth, 0);
        assert_eq!(snapshot.total_rejected, 0);
        assert_eq!(snapshot.total_cancelled, 0);
    }

    #[cfg(not(feature = "native"))]
    #[test]
    fn test_remote_bulkhead_fallback_cancellation_releases_queue_slot() {
        let executor = Arc::new(
            Executor::with_limits("fallback.test", 1, 1, Duration::from_millis(100)).unwrap(),
        );
        let held = executor.try_acquire_for_testing().unwrap();
        let cx = Cx::<cap::All>::new();
        let waiter_cx = cx.clone();

        let exec = Arc::clone(&executor);
        let waiter = thread::spawn(move || {
            let err = exec
                .acquire(&waiter_cx, "segment_stat", None, None, 0)
                .unwrap_err();
            assert!(matches!(err, FrankenError::Busy));
        });

        wait_for(|| executor.snapshot().queue_depth == 1);
        cx.cancel();
        waiter.join().unwrap();
        drop(held);

        let snapshot = executor.snapshot();
        assert_eq!(snapshot.queue_depth, 0);
        assert_eq!(snapshot.total_cancelled, 1);
    }

    #[test]
    fn test_compaction_publish_saga_forward() {
        let saga_key = derive_idempotency_key(b"saga:compaction:forward");
        let saga_id = Saga::new(saga_key);
        let manifest_id = segment_id(99);
        let mut remote = InMemoryRemoteStore::new();

        let mut saga = CompactionPublishSaga::new(saga_id, manifest_id);
        let segments = vec![
            (segment_id(11), b"seg-11".to_vec()),
            (segment_id(12), b"seg-12".to_vec()),
        ];

        saga.stage_segments(&mut remote, &segments).unwrap();
        saga.publish_manifest(&mut remote, b"manifest-v2").unwrap();
        saga.update_locator().unwrap();

        assert_eq!(saga.phase(), CompactionPhase::LocatorUpdated);
        assert!(saga.locator_updated());
        assert!(remote.has_segment(segment_id(11)));
        assert!(remote.has_segment(segment_id(12)));
        assert!(remote.has_segment(manifest_id));
    }

    #[test]
    fn test_compaction_publish_saga_compensation_then_restart_idempotent() {
        let saga_key = derive_idempotency_key(b"saga:compaction:restart");
        let saga_id = Saga::new(saga_key);
        let manifest_id = segment_id(101);
        let seg_a = segment_id(21);
        let seg_b = segment_id(22);
        let mut remote = InMemoryRemoteStore::new();
        let segments = vec![(seg_a, b"seg-21".to_vec()), (seg_b, b"seg-22".to_vec())];

        let mut first = CompactionPublishSaga::new(saga_id, manifest_id);
        first.stage_segments(&mut remote, &segments).unwrap();
        first.publish_manifest(&mut remote, b"manifest-v3").unwrap();
        let compensation = first.cancel(&mut remote);
        assert_eq!(compensation, CompactionCompensation::RemoteCleaned);
        assert!(!remote.has_segment(seg_a));
        assert!(!remote.has_segment(seg_b));
        assert!(!remote.has_segment(manifest_id));

        let mut restart = CompactionPublishSaga::new(saga_id, manifest_id);
        restart.stage_segments(&mut remote, &segments).unwrap();
        restart
            .publish_manifest(&mut remote, b"manifest-v3")
            .unwrap();
        restart.update_locator().unwrap();

        assert_eq!(restart.phase(), CompactionPhase::LocatorUpdated);
        assert!(restart.locator_updated());
        assert!(remote.has_segment(seg_a));
        assert!(remote.has_segment(seg_b));
        assert!(remote.has_segment(manifest_id));
        assert_eq!(remote.upload_count(seg_a), 1);
        assert_eq!(remote.upload_count(seg_b), 1);
    }
}
