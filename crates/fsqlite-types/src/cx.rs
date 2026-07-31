//! Capability context (`Cx`) for FrankenSQLite.
//!
//! This is a **capability-passing style** context object that:
//! - threads cancellation checks (`checkpoint`) through long-running operations
//! - carries a [`Budget`] for deadline/priority propagation
//! - encodes available effects (spawn/time/random/io/remote) in the type system
//!   via [`cap::CapSet`], so widening is a **compile-time error**.
//!
//! # Compile-time capability narrowing
//!
//! Narrowing always succeeds:
//! ```
//! use fsqlite_types::cx::{cap, Cx};
//!
//! let cx = Cx::<cap::All>::new();
//! let _compute = cx.restrict::<cap::None>();
//! ```
//!
//! Widening is rejected at compile time:
//! ```compile_fail
//! use fsqlite_types::cx::{cap, Cx};
//!
//! let cx = Cx::<cap::All>::new();
//! let compute = cx.restrict::<cap::None>();
//! let _nope = compute.restrict::<cap::All>();
//! ```
//

use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

#[cfg(feature = "native")]
use asupersync::cx::cap as native_cap;
#[cfg(feature = "native")]
use asupersync::types::Time as NativeTime;
#[cfg(feature = "native")]
use asupersync::types::{CancelKind as NativeCancelKind, CancelReason as NativeCancelReason};
#[cfg(feature = "native")]
use asupersync::{Budget as NativeBudget, Cx as NativeCx};
#[cfg(not(feature = "native"))]
mod native_cx_shim {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum NativeCancelKind {
        User,
        Timeout,
        Deadline,
        PollQuota,
        CostBudget,
        FailFast,
        RaceLost,
        ParentCancelled,
        Shutdown,
        LinkedExit,
        ResourceUnavailable,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct NativeCancelReason {
        pub kind: NativeCancelKind,
    }

    impl NativeCancelReason {
        #[must_use]
        pub const fn timeout() -> Self {
            Self {
                kind: NativeCancelKind::Timeout,
            }
        }

        #[must_use]
        pub fn user(_message: impl Into<String>) -> Self {
            Self {
                kind: NativeCancelKind::User,
            }
        }

        #[must_use]
        pub const fn parent_cancelled() -> Self {
            Self {
                kind: NativeCancelKind::ParentCancelled,
            }
        }

        #[must_use]
        pub const fn resource_unavailable() -> Self {
            Self {
                kind: NativeCancelKind::ResourceUnavailable,
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct NativeCheckpointError;

    #[derive(Debug, Default)]
    struct NativeCxInner {
        cancel_requested: AtomicBool,
        cancel_reason: Mutex<Option<NativeCancelReason>>,
    }

    #[derive(Debug, Clone, Default)]
    pub struct NativeCx {
        inner: Arc<NativeCxInner>,
    }

    impl NativeCx {
        #[must_use]
        pub fn for_testing() -> Self {
            Self::default()
        }

        pub fn set_cancel_requested(&self, requested: bool) {
            self.inner
                .cancel_requested
                .store(requested, Ordering::Release);
            if !requested {
                *self
                    .inner
                    .cancel_reason
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
            }
        }

        pub fn set_cancel_reason(&self, reason: NativeCancelReason) {
            *self
                .inner
                .cancel_reason
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(reason);
            self.inner.cancel_requested.store(true, Ordering::Release);
        }

        #[must_use]
        pub fn is_cancel_requested(&self) -> bool {
            self.inner.cancel_requested.load(Ordering::Acquire)
        }

        #[must_use]
        pub fn cancel_reason(&self) -> Option<NativeCancelReason> {
            self.inner
                .cancel_reason
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }

        pub fn checkpoint(&self) -> std::result::Result<(), NativeCheckpointError> {
            if self.is_cancel_requested() {
                Err(NativeCheckpointError)
            } else {
                Ok(())
            }
        }
    }
}

#[cfg(not(feature = "native"))]
use native_cx_shim::NativeCx;

use crate::eprocess::{EProcessDecision, EProcessOracle, EProcessSnapshot};

/// SQLite error code for `SQLITE_INTERRUPT`.
pub const SQLITE_INTERRUPT: i32 = 9;

/// Maximum nesting depth for masked cancellation sections (INV-MASK-BOUNDED).
///
/// Exceeding this limit panics in lab mode and emits a fatal diagnostic in production.
pub const MAX_MASK_DEPTH: u32 = 64;

// ---------------------------------------------------------------------------
// §4.12 Cancellation State Machine
// ---------------------------------------------------------------------------

/// Observable state of a task's cancellation lifecycle (asupersync oracle model).
///
/// ```text
/// Created → Running → CancelRequested → Cancelling → Finalizing → Completed
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CancelState {
    Created,
    Running,
    CancelRequested,
    Cancelling,
    Finalizing,
    Completed,
}

/// Reason for cancellation, ordered from weakest to strongest.
///
/// INV-CANCEL-IDEMPOTENT: multiple cancel requests are monotone — the strongest
/// reason wins and the reason can never get weaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CancelReason {
    NativeSignal = 0,
    UserInterrupt = 1,
    Timeout = 2,
    RegionClose = 3,
    Abort = 4,
}

/// Exact provenance retained when cancellation originates in Asupersync.
///
/// FrankenSQLite's [`CancelReason`] is intentionally a small behavioral
/// classification. This sidecar preserves the complete native attribution
/// and cause chain without ever reconstructing or writing a lossy reason back
/// into the native context.
#[cfg(feature = "native")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeCancellationProvenance {
    /// The native cancellation flag was set without a native reason.
    SignalWithoutReason,
    /// The strongest fully attributed native reason observed so far.
    Exact(NativeCancelReason),
}

/// Capability set definitions and subset reasoning.
pub mod cap {
    mod sealed {
        pub trait Sealed {}

        pub struct Bit<const V: bool>;

        pub trait Le {}
        impl Le for (Bit<false>, Bit<false>) {}
        impl Le for (Bit<false>, Bit<true>) {}
        impl Le for (Bit<true>, Bit<true>) {}
    }

    /// Type-level capability set: `[SPAWN, TIME, RANDOM, IO, REMOTE]`.
    #[derive(Debug, Clone, Copy, Default)]
    pub struct CapSet<
        const SPAWN: bool,
        const TIME: bool,
        const RANDOM: bool,
        const IO: bool,
        const REMOTE: bool,
    >;

    impl<
        const SPAWN: bool,
        const TIME: bool,
        const RANDOM: bool,
        const IO: bool,
        const REMOTE: bool,
    > sealed::Sealed for CapSet<SPAWN, TIME, RANDOM, IO, REMOTE>
    {
    }

    /// Full capability set.
    pub type All = CapSet<true, true, true, true, true>;
    /// No capabilities.
    pub type None = CapSet<false, false, false, false, false>;

    /// Type-level subset relation.
    ///
    /// Encodes pointwise ordering on capability bits: `false <= false`, `false <= true`,
    /// `true <= true`. The missing impl `(true <= false)` forbids widening.
    pub trait SubsetOf<Super>: sealed::Sealed {}

    impl<
        const S_SPAWN: bool,
        const S_TIME: bool,
        const S_RANDOM: bool,
        const S_IO: bool,
        const S_REMOTE: bool,
        const P_SPAWN: bool,
        const P_TIME: bool,
        const P_RANDOM: bool,
        const P_IO: bool,
        const P_REMOTE: bool,
    > SubsetOf<CapSet<P_SPAWN, P_TIME, P_RANDOM, P_IO, P_REMOTE>>
        for CapSet<S_SPAWN, S_TIME, S_RANDOM, S_IO, S_REMOTE>
    where
        (sealed::Bit<S_SPAWN>, sealed::Bit<P_SPAWN>): sealed::Le,
        (sealed::Bit<S_TIME>, sealed::Bit<P_TIME>): sealed::Le,
        (sealed::Bit<S_RANDOM>, sealed::Bit<P_RANDOM>): sealed::Le,
        (sealed::Bit<S_IO>, sealed::Bit<P_IO>): sealed::Le,
        (sealed::Bit<S_REMOTE>, sealed::Bit<P_REMOTE>): sealed::Le,
    {
    }

    pub trait HasSpawn: sealed::Sealed {}
    impl<const TIME: bool, const RANDOM: bool, const IO: bool, const REMOTE: bool> HasSpawn
        for CapSet<true, TIME, RANDOM, IO, REMOTE>
    {
    }

    pub trait HasTime: sealed::Sealed {}
    impl<const SPAWN: bool, const RANDOM: bool, const IO: bool, const REMOTE: bool> HasTime
        for CapSet<SPAWN, true, RANDOM, IO, REMOTE>
    {
    }

    pub trait HasRandom: sealed::Sealed {}
    impl<const SPAWN: bool, const TIME: bool, const IO: bool, const REMOTE: bool> HasRandom
        for CapSet<SPAWN, TIME, true, IO, REMOTE>
    {
    }

    pub trait HasIo: sealed::Sealed {}
    impl<const SPAWN: bool, const TIME: bool, const RANDOM: bool, const REMOTE: bool> HasIo
        for CapSet<SPAWN, TIME, RANDOM, true, REMOTE>
    {
    }

    pub trait HasRemote: sealed::Sealed {}
    impl<const SPAWN: bool, const TIME: bool, const RANDOM: bool, const IO: bool> HasRemote
        for CapSet<SPAWN, TIME, RANDOM, IO, true>
    {
    }
}

/// Connection-level capabilities: everything enabled.
pub type FullCaps = cap::All;
/// Storage-layer capabilities: time + I/O only.
pub type StorageCaps = cap::CapSet<false, true, false, true, false>;
/// Pure computation capabilities: no I/O, no time, no randomness.
pub type ComputeCaps = cap::None;

/// A budget for cancellation/deadline/priority propagation.
///
/// This is a product lattice with mixed meet/join semantics:
/// - resource constraints tighten by `min` (deadline/poll/cost)
/// - priority propagates by `max`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Budget {
    pub deadline: Option<Duration>,
    pub poll_quota: u32,
    pub cost_quota: Option<u64>,
    pub priority: u8,
}

impl Budget {
    /// No constraints (identity for [`Self::meet`]).
    pub const INFINITE: Self = Self {
        deadline: None,
        poll_quota: u32::MAX,
        cost_quota: None,
        priority: 0,
    };

    /// Minimal budget for cleanup/finalizers.
    pub const MINIMAL: Self = Self {
        deadline: None,
        poll_quota: 100,
        cost_quota: None,
        priority: 0,
    };

    #[must_use]
    pub const fn with_deadline(self, deadline: Duration) -> Self {
        Self {
            deadline: Some(deadline),
            ..self
        }
    }

    #[must_use]
    pub const fn with_priority(self, priority: u8) -> Self {
        Self { priority, ..self }
    }

    #[must_use]
    pub const fn with_poll_quota(self, poll_quota: u32) -> Self {
        Self { poll_quota, ..self }
    }

    #[must_use]
    pub const fn with_cost_quota(self, cost_quota: u64) -> Self {
        Self {
            cost_quota: Some(cost_quota),
            ..self
        }
    }

    /// Meet (tighten) two budgets.
    #[must_use]
    pub fn meet(self, other: Self) -> Self {
        Self {
            deadline: match (self.deadline, other.deadline) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            },
            poll_quota: self.poll_quota.min(other.poll_quota),
            cost_quota: match (self.cost_quota, other.cost_quota) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            },
            priority: self.priority.max(other.priority),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    kind: ErrorKind,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            ErrorKind::Cancelled => write!(f, "operation cancelled"),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    #[must_use]
    pub const fn cancelled() -> Self {
        Self {
            kind: ErrorKind::Cancelled,
        }
    }

    #[must_use]
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    #[must_use]
    pub const fn sqlite_error_code(&self) -> i32 {
        match self.kind {
            ErrorKind::Cancelled => SQLITE_INTERRUPT,
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
#[derive(Debug)]
struct TestInterleavingPause {
    expected_reason: Option<CancelReason>,
    armed: AtomicBool,
    reached: std::sync::Barrier,
    resume: std::sync::Barrier,
}

#[cfg(test)]
impl TestInterleavingPause {
    fn wait_if_armed(&self, reason: Option<CancelReason>) {
        if self.expected_reason == reason && self.armed.swap(false, Ordering::SeqCst) {
            self.reached.wait();
            self.resume.wait();
        }
    }
}

#[cfg(test)]
fn pause_at_test_interleaving(
    slot: &Mutex<Option<Arc<TestInterleavingPause>>>,
    reason: Option<CancelReason>,
) {
    let pause = slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    if let Some(pause) = pause {
        pause.wait_if_armed(reason);
    }
}

#[derive(Debug)]
struct CxInner {
    cancel_requested: AtomicBool,
    cancel_state: Mutex<CancelState>,
    cancel_reason: Mutex<Option<CancelReason>>,
    /// Strongest reason for which native signaling and descendant propagation
    /// have both completed.
    cancel_fully_propagated_reason: Mutex<Option<CancelReason>>,
    #[cfg(test)]
    accepted_cancel_propagations: AtomicU32,
    mask_depth: AtomicU32,
    children: Mutex<Vec<Weak<Self>>>,
    child_registration_epoch: AtomicU32,
    last_checkpoint_msg: Mutex<Option<String>>,
    last_eprocess_decision: Mutex<Option<EProcessDecision>>,
    eprocess_oracle: std::sync::OnceLock<Arc<EProcessOracle>>,
    #[cfg(feature = "native")]
    attached_native_cx: Mutex<Option<NativeCx>>,
    #[cfg(feature = "native")]
    fallback_native_cx: std::sync::OnceLock<NativeCx>,
    /// Cancellation-only native relay for admission/open waiters.
    ///
    /// This remains `cap::None` at the type level and is never exposed as the
    /// attached runtime context, so waking a project-Cx cancellation waiter
    /// cannot mint spawn, timer, I/O, or remote authority.
    #[cfg(feature = "native")]
    native_cancel_relay: Mutex<Option<NativeCx<native_cap::None>>>,
    #[cfg(feature = "native")]
    native_cancel_provenance: Mutex<Option<NativeCancellationProvenance>>,
    #[cfg(test)]
    cancel_publication_pause: Mutex<Option<Arc<TestInterleavingPause>>>,
    #[cfg(test)]
    cancel_after_flag_pause: Mutex<Option<Arc<TestInterleavingPause>>>,
    #[cfg(all(feature = "native", test))]
    native_signal_pause: Mutex<Option<Arc<TestInterleavingPause>>>,
    #[cfg(all(feature = "native", test))]
    native_install_pause: Mutex<Option<Arc<TestInterleavingPause>>>,
    // Strong, one-way cancellation source for an actor-admitted operation.
    // Engine capabilities and native runtime authority still come from this
    // Cx's ordinary root lineage; the source contributes only its atomic
    // cancellation state.
    operation_cancellation_source: Option<Arc<Self>>,
    // Deterministic clock: milliseconds since epoch for tests.
    unix_millis: AtomicU64,
}

impl CxInner {
    fn new_with_operation_cancellation_source(
        operation_cancellation_source: Option<Arc<Self>>,
    ) -> Self {
        Self {
            cancel_requested: AtomicBool::new(false),
            cancel_state: Mutex::new(CancelState::Created),
            cancel_reason: Mutex::new(None),
            cancel_fully_propagated_reason: Mutex::new(None),
            #[cfg(test)]
            accepted_cancel_propagations: AtomicU32::new(0),
            mask_depth: AtomicU32::new(0),
            children: Mutex::new(Vec::new()),
            child_registration_epoch: AtomicU32::new(0),
            last_checkpoint_msg: Mutex::new(None),
            last_eprocess_decision: Mutex::new(None),
            eprocess_oracle: std::sync::OnceLock::new(),
            #[cfg(feature = "native")]
            attached_native_cx: Mutex::new(None),
            #[cfg(feature = "native")]
            fallback_native_cx: std::sync::OnceLock::new(),
            #[cfg(feature = "native")]
            native_cancel_relay: Mutex::new(None),
            #[cfg(feature = "native")]
            native_cancel_provenance: Mutex::new(None),
            #[cfg(test)]
            cancel_publication_pause: Mutex::new(None),
            #[cfg(test)]
            cancel_after_flag_pause: Mutex::new(None),
            #[cfg(all(feature = "native", test))]
            native_signal_pause: Mutex::new(None),
            #[cfg(all(feature = "native", test))]
            native_install_pause: Mutex::new(None),
            operation_cancellation_source,
            unix_millis: AtomicU64::new(0),
        }
    }
}

#[cfg(feature = "native")]
#[must_use]
fn native_reason_to_local(reason: &NativeCancelReason) -> CancelReason {
    match reason.kind {
        NativeCancelKind::User => CancelReason::UserInterrupt,
        NativeCancelKind::Timeout
        | NativeCancelKind::Deadline
        | NativeCancelKind::PollQuota
        | NativeCancelKind::CostBudget => CancelReason::Timeout,
        NativeCancelKind::FailFast
        | NativeCancelKind::RaceLost
        | NativeCancelKind::ParentCancelled
        | NativeCancelKind::LinkedExit => CancelReason::RegionClose,
        NativeCancelKind::ResourceUnavailable | NativeCancelKind::Shutdown => CancelReason::Abort,
    }
}

#[cfg(feature = "native")]
fn merge_native_cancel_provenance(inner: &CxInner, observed: NativeCancellationProvenance) -> bool {
    let mut provenance = inner
        .native_cancel_provenance
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match (&mut *provenance, observed) {
        (None, observed) => {
            *provenance = Some(observed);
            true
        }
        (
            Some(NativeCancellationProvenance::SignalWithoutReason),
            NativeCancellationProvenance::Exact(reason),
        ) => {
            *provenance = Some(NativeCancellationProvenance::Exact(reason));
            true
        }
        (
            Some(NativeCancellationProvenance::Exact(current)),
            NativeCancellationProvenance::Exact(observed),
        ) => current.strengthen(&observed),
        (
            Some(
                NativeCancellationProvenance::SignalWithoutReason
                | NativeCancellationProvenance::Exact(_),
            ),
            NativeCancellationProvenance::SignalWithoutReason,
        ) => false,
    }
}

#[cfg(feature = "native")]
fn propagate_native_cancel_provenance(inner: &CxInner, observed: NativeCancellationProvenance) {
    if !merge_native_cancel_provenance(inner, observed.clone()) {
        return;
    }

    let children = {
        let mut children = inner
            .children
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        children.retain(|child| child.strong_count() > 0);
        children
            .iter()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>()
    };
    let mut pending: Vec<_> = children
        .into_iter()
        .map(|child| (child, observed.clone()))
        .collect();
    while let Some((next, inherited)) = pending.pop() {
        if !merge_native_cancel_provenance(&next, inherited.clone()) {
            continue;
        }
        let children = {
            let mut children = next
                .children
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            children.retain(|child| child.strong_count() > 0);
            children
                .iter()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>()
        };
        pending.extend(children.into_iter().map(|child| (child, inherited.clone())));
    }
}

#[cfg(feature = "native")]
fn signal_native_cancel_target<Caps>(native: &NativeCx<Caps>) {
    // Asupersync 0.3.9's reason-setting APIs overwrite the existing reason
    // rather than strengthening it conditionally. A read-compare-write loop
    // cannot close the race with cancellation initiated by the runtime itself:
    // it can overwrite a stronger native reason after that reason wakes an
    // observer. Keep the exact FrankenSQLite reason in `CxInner` and use the
    // native context only as a cancellation wake signal. This operation does
    // not replace an existing native reason, so native attribution and cause
    // chains remain authoritative on their originating plane.
    if !native.is_cancel_requested() {
        native.set_cancel_requested(true);
    }
}

#[cfg(feature = "native")]
fn retain_native_cancel_provenance<Caps>(inner: &CxInner, native: &NativeCx<Caps>) {
    if let Some(reason) = native.cancel_reason() {
        propagate_native_cancel_provenance(inner, NativeCancellationProvenance::Exact(reason));
    }
}

#[cfg(feature = "native")]
fn signal_native_cx_cancel(inner: &CxInner) {
    #[cfg(test)]
    pause_at_test_interleaving(&inner.native_signal_pause, None);

    let attached_native = inner
        .attached_native_cx
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_ref()
        .cloned();
    if let Some(native) = attached_native {
        retain_native_cancel_provenance(inner, &native);
        signal_native_cancel_target(&native);
    }
    if let Some(native) = inner.fallback_native_cx.get() {
        retain_native_cancel_provenance(inner, native);
        signal_native_cancel_target(native);
    }
    let cancel_relay = inner
        .native_cancel_relay
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_ref()
        .cloned();
    if let Some(relay) = cancel_relay {
        retain_native_cancel_provenance(inner, &relay);
        signal_native_cancel_target(&relay);
    }
}

#[cfg(feature = "native")]
#[must_use]
#[allow(dead_code)]
fn native_budget_from_local(budget: Budget) -> NativeBudget {
    let mut native_budget = NativeBudget::new()
        .with_poll_quota(budget.poll_quota)
        .with_priority(budget.priority);
    if let Some(cost_quota) = budget.cost_quota {
        native_budget = native_budget.with_cost_quota(cost_quota);
    }
    if let Some(deadline) = budget.deadline {
        native_budget = native_budget.with_deadline(local_deadline_to_native_time(deadline));
    }
    native_budget
}

#[cfg(feature = "native")]
#[must_use]
#[allow(dead_code)]
fn wall_clock_now_since_epoch() -> Duration {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
}

#[cfg(feature = "native")]
#[must_use]
#[allow(dead_code)]
fn local_deadline_to_native_time(deadline: Duration) -> NativeTime {
    let absolute_deadline = wall_clock_now_since_epoch()
        .checked_add(deadline)
        .unwrap_or(Duration::MAX);
    let nanos = u64::try_from(absolute_deadline.as_nanos()).unwrap_or(u64::MAX);
    NativeTime::from_nanos(nanos)
}

/// Apply cancellation to one node and return a strong snapshot of its live
/// children.
///
/// Every node lock is released before native cancellation callbacks or child
/// traversal. This keeps arbitrary runtime-handle behavior outside project
/// locks and lets the caller use an iterative worklist.
fn cancel_node_and_snapshot_children(
    inner: &CxInner,
    reason: CancelReason,
) -> Option<(CancelReason, Vec<Arc<CxInner>>)> {
    let effective_reason = {
        let mut current = inner
            .cancel_reason
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match *current {
            Some(existing) if existing >= reason => existing,
            _ => {
                *current = Some(reason);
                #[cfg(test)]
                inner
                    .accepted_cancel_propagations
                    .fetch_add(1, Ordering::Relaxed);
                reason
            }
        }
    };
    if inner
        .cancel_fully_propagated_reason
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .is_some_and(|completed| completed >= effective_reason)
    {
        return None;
    }

    // Publish both the exact reason and the requested lifecycle state before
    // the fast bit. Any observer that acquires `cancel_requested == true` can
    // therefore transition `CancelRequested -> Cancelling`; it can never
    // return a cancellation error while the public lifecycle still says
    // Created or Running.
    {
        let mut state = inner
            .cancel_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(*state, CancelState::Created | CancelState::Running) {
            *state = CancelState::CancelRequested;
        }
    }

    #[cfg(test)]
    pause_at_test_interleaving(&inner.cancel_publication_pause, Some(reason));
    inner.cancel_requested.store(true, Ordering::Release);
    #[cfg(test)]
    pause_at_test_interleaving(&inner.cancel_after_flag_pause, Some(reason));

    let mut children = inner
        .children
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    children.retain(|child| child.strong_count() > 0);
    Some((
        effective_reason,
        children.iter().filter_map(Weak::upgrade).collect(),
    ))
}

fn mark_cancel_fully_propagated(inner: &CxInner, reason: CancelReason) {
    let mut completed = inner
        .cancel_fully_propagated_reason
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if completed.is_none_or(|existing| existing < reason) {
        *completed = Some(reason);
    }
}

fn mark_cancellation_observed(inner: &CxInner) {
    let mut state = inner
        .cancel_state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if *state == CancelState::CancelRequested {
        *state = CancelState::Cancelling;
    }
}

/// Propagate cancellation to a `CxInner` node and all its descendants without
/// consuming native stack per descendant.
fn propagate_cancel(inner: &CxInner, reason: CancelReason) {
    let Some((effective_reason, children)) = cancel_node_and_snapshot_children(inner, reason)
    else {
        return;
    };
    let mut processed = std::collections::HashMap::new();
    processed.insert(std::ptr::from_ref(inner), effective_reason);
    let mut touched_children: std::collections::HashMap<
        *const CxInner,
        (Arc<CxInner>, CancelReason),
    > = std::collections::HashMap::new();
    let mut pending: Vec<_> = children
        .into_iter()
        .map(|child| (child, effective_reason))
        .collect();
    while let Some((next, inherited_reason)) = pending.pop() {
        if let Some((effective_reason, children)) =
            cancel_node_and_snapshot_children(&next, inherited_reason)
        {
            let key = Arc::as_ptr(&next);
            if processed
                .get(&key)
                .is_some_and(|processed_reason| *processed_reason >= effective_reason)
            {
                continue;
            }
            processed.insert(key, effective_reason);
            touched_children
                .entry(key)
                .and_modify(|(_, touched_reason)| {
                    *touched_reason = (*touched_reason).max(effective_reason);
                })
                .or_insert_with(|| (Arc::clone(&next), effective_reason));
            pending.extend(children.into_iter().map(|child| (child, effective_reason)));
        }
    }

    // Runtime wake signals are a second phase. Every reachable project context
    // already carries its exact local reason before a shared native context is
    // signaled, so a child cannot mistake this project-origin wake for an
    // unexplained native cancellation.
    #[cfg(feature = "native")]
    {
        signal_native_cx_cancel(inner);
        for (child, _) in touched_children.values() {
            signal_native_cx_cancel(child);
        }
    }

    // Completion is published only after both descendant traversal and native
    // signaling finish. Concurrent equal/weaker callers may duplicate an
    // in-flight walk, but they cannot return early from a partially propagated
    // cancellation.
    for (child, propagated_reason) in touched_children.into_values() {
        mark_cancel_fully_propagated(&child, propagated_reason);
    }
    mark_cancel_fully_propagated(inner, effective_reason);
}

const CHILD_LINK_PRUNE_INTERVAL: u32 = 64;

/// Register a one-way parent-to-child cancellation link.
///
/// The parent deliberately stores only a `Weak` reference, so a completed
/// operation never keeps its cancellation state alive. Opportunistic pruning
/// every 64 registrations bounds dead slots left by repeatedly dropped
/// admission futures without imposing an O(live-children) scan on every child
/// creation.
fn register_cancellation_child(parent: &CxInner, child: &Arc<CxInner>) {
    let registration = parent
        .child_registration_epoch
        .fetch_add(1, Ordering::Relaxed)
        .wrapping_add(1);
    let mut children = parent
        .children
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if registration % CHILD_LINK_PRUNE_INTERVAL == 0 {
        children.retain(|registered| registered.strong_count() > 0);
    }
    children.push(Arc::downgrade(child));
}

fn inherit_existing_cancellation(parent: &CxInner, child: &CxInner) {
    #[cfg(feature = "native")]
    let native_provenance = {
        parent
            .native_cancel_provenance
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    };
    #[cfg(feature = "native")]
    if let Some(provenance) = native_provenance {
        propagate_native_cancel_provenance(child, provenance);
    }

    let reason = *parent
        .cancel_reason
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(reason) = reason {
        propagate_cancel(child, reason);
    } else if parent.cancel_requested.load(Ordering::Acquire) {
        propagate_cancel(child, CancelReason::UserInterrupt);
    }
}

/// Capability context passed through all effectful operations.
///
/// Carries tracing identifiers (`trace_id`, `decision_id`, `policy_id`) that
/// propagate through all context derivations (clone, restrict, scope, child).
/// A value of `0` means "unset / not assigned".
#[derive(Debug)]
pub struct Cx<Caps: cap::SubsetOf<cap::All> = FullCaps> {
    inner: Arc<CxInner>,
    budget: Budget,
    trace_id: u64,
    decision_id: u64,
    policy_id: u64,
    // fn() -> Caps ensures Send+Sync regardless of Caps marker type.
    _caps: PhantomData<fn() -> Caps>,
}

/// Caller-owned cancellation source for one admitted database operation.
///
/// This type carries no effect capability and no native runtime context. Its
/// only authority is a one-way request to cancel the matching
/// [`OperationCancellationToken`]. Dropping an armed source requests
/// cancellation, which makes dropping an admitted public future
/// cancel-correct without cancelling its parent context or sibling operations.
#[derive(Debug)]
#[must_use = "dropping an armed operation source requests cancellation"]
pub struct OperationCancellationSource {
    inner: Arc<CxInner>,
    armed: bool,
}

impl OperationCancellationSource {
    /// Request cancellation of this operation without affecting its parent or
    /// siblings.
    pub fn cancel(&self) {
        self.cancel_with_reason(CancelReason::UserInterrupt);
    }

    /// Request cancellation with an explicit reason.
    pub fn cancel_with_reason(&self, reason: CancelReason) {
        propagate_cancel(&self.inner, reason);
    }

    /// Forward a project-context cancellation without transferring any of
    /// that context's runtime or effect capabilities into the engine token.
    pub fn cancel_from_cx<Caps: cap::SubsetOf<cap::All>>(&self, cx: &Cx<Caps>) {
        #[cfg(feature = "native")]
        if let Some(provenance) = cx.native_cancel_provenance() {
            propagate_native_cancel_provenance(&self.inner, provenance);
        }
        self.cancel_with_reason(cx.cancel_reason().unwrap_or(CancelReason::UserInterrupt));
    }

    /// Forward cancellation observed on the native context polling the public
    /// future. Only its pure-data reason and provenance are retained; the
    /// task-affine native context itself never crosses the actor boundary.
    #[cfg(feature = "native")]
    pub fn cancel_from_native_cx<Caps>(&self, native: &NativeCx<Caps>) {
        if let Some(reason) = native.cancel_reason() {
            propagate_native_cancel_provenance(
                &self.inner,
                NativeCancellationProvenance::Exact(reason.clone()),
            );
            self.cancel_with_reason(native_reason_to_local(&reason));
        } else {
            propagate_native_cancel_provenance(
                &self.inner,
                NativeCancellationProvenance::SignalWithoutReason,
            );
            self.cancel_with_reason(CancelReason::NativeSignal);
        }
    }

    /// Disarm Drop cancellation after the authoritative terminal result has
    /// been observed.
    pub fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for OperationCancellationSource {
    fn drop(&mut self) {
        if self.armed {
            self.cancel();
        }
    }
}

/// Native-free, capability-free cancellation handoff retained by one engine
/// operation.
///
/// The token cannot cancel its caller. It can only be inspected or linked to a
/// root-derived engine [`Cx`], preserving a strict separation between caller
/// cancellation authority and engine runtime/effect authority. It retains only
/// scalar caller trace/policy metadata plus its pure-data [`Budget`]. Nonzero
/// metadata may override the engine child's corresponding metadata, and the
/// two budgets are met so constraints can only tighten; decision IDs and all
/// runtime-bound state remain engine-owned.
#[derive(Debug, Clone)]
pub struct OperationCancellationToken {
    inner: Arc<CxInner>,
    caller_budget: Budget,
    caller_trace_id: u64,
    caller_policy_id: u64,
}

impl OperationCancellationToken {
    /// Whether cancellation has been requested for this operation.
    #[must_use]
    pub fn is_cancel_requested(&self) -> bool {
        self.inner.cancel_requested.load(Ordering::Acquire)
    }

    /// Strongest cancellation reason observed by this operation.
    #[must_use]
    pub fn cancel_reason(&self) -> Option<CancelReason> {
        *self
            .inner
            .cancel_reason
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Whether engine work has observed this operation's cancellation at a
    /// checkpoint and begun cancellation handling.
    ///
    /// A mere request is intentionally insufficient: callers use this bit to
    /// distinguish an engine `Abort` caused by this operation's cancellation
    /// from an unrelated `Abort` that raced with a later cancellation request.
    #[must_use]
    pub fn cancellation_was_observed(&self) -> bool {
        matches!(
            *self
                .inner
                .cancel_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            CancelState::Cancelling | CancelState::Finalizing | CancelState::Completed
        )
    }

    /// Exact native cancellation provenance observed by this context.
    ///
    /// This is separate from [`Self::cancel_reason`], which reports the
    /// coarse behavioral class used by the engine.
    #[cfg(feature = "native")]
    #[must_use]
    pub fn native_cancel_provenance(&self) -> Option<NativeCancellationProvenance> {
        self.inner
            .native_cancel_provenance
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Check the operation signal without manufacturing an effect-capable
    /// context.
    pub fn checkpoint(&self) -> Result<()> {
        if self.is_cancel_requested() {
            mark_cancellation_observed(&self.inner);
            Err(Error::cancelled())
        } else {
            Ok(())
        }
    }
}

impl<Caps: cap::SubsetOf<cap::All>> Clone for Cx<Caps> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            budget: self.budget,
            trace_id: self.trace_id,
            decision_id: self.decision_id,
            policy_id: self.policy_id,
            _caps: PhantomData,
        }
    }
}

impl Default for Cx<FullCaps> {
    fn default() -> Self {
        Self::new()
    }
}

impl Cx<FullCaps> {
    #[must_use]
    pub fn new() -> Self {
        Self::with_budget(Budget::INFINITE)
    }
}

impl<Caps: cap::SubsetOf<cap::All>> Cx<Caps> {
    #[cfg(all(feature = "native", test))]
    #[must_use]
    #[allow(dead_code)]
    fn effective_native_cx(&self) -> NativeCx {
        let attached_native = self
            .inner
            .attached_native_cx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .cloned();
        if let Some(native) = attached_native {
            return native;
        }

        let native = self
            .inner
            .fallback_native_cx
            .get_or_init(|| {
                NativeCx::for_request_with_budget(native_budget_from_local(self.budget))
            })
            .clone();

        if self.is_cancel_requested() {
            signal_native_cancel_target(&native);
        }
        native
    }

    #[cfg(feature = "native")]
    #[must_use]
    fn native_cx_for_checkpoint(&self) -> Option<NativeCx> {
        let attached_native = self
            .inner
            .attached_native_cx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .cloned();
        attached_native.or_else(|| self.inner.fallback_native_cx.get().cloned())
    }

    #[must_use]
    pub fn with_budget(budget: Budget) -> Self {
        Self::with_budget_and_operation_cancellation_source(budget, None)
    }

    fn with_budget_and_operation_cancellation_source(
        budget: Budget,
        operation_cancellation_source: Option<Arc<CxInner>>,
    ) -> Self {
        Self {
            inner: Arc::new(CxInner::new_with_operation_cancellation_source(
                operation_cancellation_source,
            )),
            budget,
            trace_id: 0,
            decision_id: 0,
            policy_id: 0,
            _caps: PhantomData,
        }
    }

    #[must_use]
    pub fn budget(&self) -> Budget {
        self.budget
    }

    /// Convert this project's effective budget into the native Asupersync
    /// representation for a child scope.
    ///
    /// The project deadline is a relative timeout, so it must be anchored to
    /// the current native context's clock. Using the process wall clock here
    /// would be wrong for runtimes backed by a monotonic or virtual clock.
    /// `NativeCx::scope_with_budget` subsequently meets this value with the
    /// runtime-owned parent budget, so neither plane can loosen the other.
    #[cfg(feature = "native")]
    #[must_use]
    pub fn native_budget_for_child_scope(&self, native_cx: &NativeCx) -> NativeBudget {
        let mut native_budget = NativeBudget::new()
            .with_poll_quota(self.budget.poll_quota)
            .with_priority(self.budget.priority);
        if let Some(cost_quota) = self.budget.cost_quota {
            native_budget = native_budget.with_cost_quota(cost_quota);
        }
        if let Some(timeout) = self.budget.deadline {
            native_budget = native_budget.with_timeout(native_cx.now(), timeout);
        }
        native_cx.budget().meet(native_budget)
    }

    // -----------------------------------------------------------------------
    // Tracing IDs (§4 Cx capability context threading)
    // -----------------------------------------------------------------------

    /// The trace ID for this context (0 = unset).
    #[must_use]
    pub fn trace_id(&self) -> u64 {
        self.trace_id
    }

    /// The decision ID for this context (0 = unset).
    #[must_use]
    pub fn decision_id(&self) -> u64 {
        self.decision_id
    }

    /// The policy ID for this context (0 = unset).
    #[must_use]
    pub fn policy_id(&self) -> u64 {
        self.policy_id
    }

    /// Set all three tracing identifiers at once.
    ///
    /// Typically called once when a connection or request is initialized.
    #[must_use]
    pub fn with_trace_context(mut self, trace_id: u64, decision_id: u64, policy_id: u64) -> Self {
        self.trace_id = trace_id;
        self.decision_id = decision_id;
        self.policy_id = policy_id;
        self
    }

    /// Return a new context with only the `decision_id` changed.
    ///
    /// Used when starting a new operation within the same trace.
    #[must_use]
    pub fn with_decision_id(mut self, decision_id: u64) -> Self {
        self.decision_id = decision_id;
        self
    }

    /// Return a new context with only the `policy_id` changed.
    #[must_use]
    pub fn with_policy_id(mut self, policy_id: u64) -> Self {
        self.policy_id = policy_id;
        self
    }

    /// Returns a view of this context with a tighter effective budget.
    ///
    /// The effective budget is computed as `self.budget.meet(child)`, so the
    /// child cannot loosen its parent's constraints.
    /// Tracing IDs propagate unchanged.
    #[must_use]
    pub fn scope_with_budget(&self, child: Budget) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            budget: self.budget.meet(child),
            trace_id: self.trace_id,
            decision_id: self.decision_id,
            policy_id: self.policy_id,
            _caps: PhantomData,
        }
    }

    /// Returns a cleanup scope that uses [`Budget::MINIMAL`].
    #[must_use]
    pub fn cleanup_scope(&self) -> Self {
        self.scope_with_budget(Budget::MINIMAL)
    }

    /// Re-type this context to a narrower capability set.
    ///
    /// This is zero-cost at runtime and shares cancellation state.
    #[must_use]
    pub fn restrict<NewCaps>(&self) -> Cx<NewCaps>
    where
        NewCaps: cap::SubsetOf<cap::All> + cap::SubsetOf<Caps>,
    {
        self.retype()
    }

    /// Internal re-typing helper without subset enforcement.
    #[must_use]
    fn retype<NewCaps>(&self) -> Cx<NewCaps>
    where
        NewCaps: cap::SubsetOf<cap::All>,
    {
        Cx {
            inner: Arc::clone(&self.inner),
            budget: self.budget,
            trace_id: self.trace_id,
            decision_id: self.decision_id,
            policy_id: self.policy_id,
            _caps: PhantomData,
        }
    }

    // -----------------------------------------------------------------------
    // Cancellation state machine (§4.12)
    // -----------------------------------------------------------------------

    #[must_use]
    pub fn is_cancel_requested(&self) -> bool {
        self.inner.cancel_requested.load(Ordering::Acquire)
    }

    /// Request cancellation with the default reason (`UserInterrupt`).
    ///
    /// Propagates to all child contexts per INV-CANCEL-PROPAGATES.
    pub fn cancel(&self) {
        self.cancel_with_reason(CancelReason::UserInterrupt);
    }

    /// Request cancellation with an explicit reason.
    ///
    /// INV-CANCEL-IDEMPOTENT: the strongest reason wins; weaker reasons are
    /// ignored once a stronger one has been set.
    ///
    /// INV-CANCEL-PROPAGATES: cancellation propagates to all descendants.
    pub fn cancel_with_reason(&self, reason: CancelReason) {
        propagate_cancel(&self.inner, reason);
    }

    /// Current state in the cancellation lifecycle.
    #[must_use]
    pub fn cancel_state(&self) -> CancelState {
        *self
            .inner
            .cancel_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The strongest cancellation reason set so far, if any.
    #[must_use]
    pub fn cancel_reason(&self) -> Option<CancelReason> {
        *self
            .inner
            .cancel_reason
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Exact native cancellation provenance observed by this context.
    ///
    /// This is separate from [`Self::cancel_reason`], which reports the
    /// coarse behavioral class used by the engine.
    #[cfg(feature = "native")]
    #[must_use]
    pub fn native_cancel_provenance(&self) -> Option<NativeCancellationProvenance> {
        self.inner
            .native_cancel_provenance
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Transition from `Created` to `Running`.
    pub fn transition_to_running(&self) {
        let mut state = self
            .inner
            .cancel_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *state == CancelState::Created {
            *state = CancelState::Running;
        }
    }

    /// Transition from `Cancelling` to `Finalizing`.
    pub fn transition_to_finalizing(&self) {
        let mut state = self
            .inner
            .cancel_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *state == CancelState::Cancelling {
            *state = CancelState::Finalizing;
        }
    }

    /// Transition to `Completed` (from `Finalizing` or `Running`).
    pub fn transition_to_completed(&self) {
        let mut state = self
            .inner
            .cancel_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(*state, CancelState::Finalizing | CancelState::Running) {
            *state = CancelState::Completed;
        }
    }

    /// Attach an e-process oracle used by [`Self::checkpoint`].
    pub fn set_eprocess_oracle(&self, oracle: Arc<EProcessOracle>) {
        let _ = self.inner.eprocess_oracle.set(oracle);
    }

    /// Remove the currently attached e-process oracle.
    pub fn clear_eprocess_oracle(&self) {
        // OnceLock cannot be easily cleared. We just leave it as is.
        // It's only called in unused methods anyway.
    }

    /// Attach a native asupersync context used by [`Self::checkpoint`].
    #[cfg(feature = "native")]
    pub fn set_native_cx(&self, native_cx: NativeCx) {
        #[cfg(test)]
        pause_at_test_interleaving(&self.inner.native_install_pause, None);

        let previous = {
            let mut slot = self
                .inner
                .attached_native_cx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            slot.replace(native_cx.clone())
        };
        drop(previous);

        if self.is_cancel_requested() {
            retain_native_cancel_provenance(&self.inner, &native_cx);
            signal_native_cancel_target(&native_cx);
        }
    }

    /// Attach a cancellation-only native relay without adding runtime effect
    /// authority to this project context.
    ///
    /// The relay remains `cap::None` end-to-end. Installation is followed by
    /// a cancellation recheck, closing the race where the project context is
    /// cancelled immediately before the relay becomes visible.
    #[cfg(feature = "native")]
    pub fn set_native_cancel_relay(&self, relay: NativeCx<native_cap::None>) {
        let previous = {
            let mut slot = self
                .inner
                .native_cancel_relay
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            slot.replace(relay.clone())
        };
        drop(previous);

        if self.is_cancel_requested() {
            retain_native_cancel_provenance(&self.inner, &relay);
            signal_native_cancel_target(&relay);
        }
    }

    /// Attach a native context shim in non-native builds.
    #[cfg(not(feature = "native"))]
    pub fn set_native_cx<T>(&self, _native_cx: T) {}

    /// Return the attached native asupersync context, if one exists.
    #[cfg(feature = "native")]
    #[must_use]
    pub fn attached_native_cx(&self) -> Option<NativeCx> {
        self.inner
            .attached_native_cx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Return the attached native context shim, if one exists.
    #[cfg(not(feature = "native"))]
    #[must_use]
    pub fn attached_native_cx(&self) -> Option<NativeCx> {
        None
    }

    /// Remove the currently attached native asupersync context.
    #[cfg(feature = "native")]
    pub fn clear_native_cx(&self) {
        *self
            .inner
            .attached_native_cx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }

    /// Remove the currently attached native context shim.
    #[cfg(not(feature = "native"))]
    pub fn clear_native_cx(&self) {}

    #[must_use]
    fn maybe_cancel_via_eprocess(&self) -> bool {
        let Some(oracle) = self.inner.eprocess_oracle.get() else {
            return false;
        };
        let decision = oracle.decision(self.budget.priority);
        self.record_eprocess_decision(decision.clone());
        tracing::debug!(
            target: "fsqlite::cx",
            event = "eprocess_checkpoint",
            trace_id = self.trace_id,
            decision_id = self.decision_id,
            policy_id = self.policy_id,
            priority = decision.priority,
            evalue = decision.snapshot.evalue,
            threshold = decision.snapshot.rejection_threshold,
            observations = decision.snapshot.observations,
            priority_threshold = decision.snapshot.priority_threshold,
            should_shed = decision.should_shed,
            signal = ?decision.snapshot.last_signal
        );
        if decision.should_shed {
            tracing::info!(
                target: "fsqlite::cx",
                event = "eprocess_shedding_triggered",
                trace_id = self.trace_id,
                decision_id = self.decision_id,
                policy_id = self.policy_id,
                priority = decision.priority,
                evalue = decision.snapshot.evalue,
                threshold = decision.snapshot.rejection_threshold,
                signal = ?decision.snapshot.last_signal
            );
            self.cancel_with_reason(CancelReason::Abort);
            return true;
        }
        false
    }

    #[cfg(feature = "native")]
    fn ingest_native_cancellation(&self, native: &NativeCx) {
        if let Some(reason) = native.cancel_reason() {
            propagate_native_cancel_provenance(
                &self.inner,
                NativeCancellationProvenance::Exact(reason.clone()),
            );
            self.cancel_with_reason(native_reason_to_local(&reason));
        } else if !self.is_cancel_requested() {
            // A reasonless signal can be emitted by native code or can be the
            // echo of this bridge's project-origin wake. The latter always
            // publishes the project bit first, so only an otherwise-live
            // context records an unexplained native signal.
            propagate_native_cancel_provenance(
                &self.inner,
                NativeCancellationProvenance::SignalWithoutReason,
            );
            self.cancel_with_reason(CancelReason::NativeSignal);
        }
    }

    #[cfg(feature = "native")]
    fn capture_native_reason_if_present(&self) {
        let Some(native) = self.native_cx_for_checkpoint() else {
            return;
        };
        if let Some(reason) = native.cancel_reason() {
            propagate_native_cancel_provenance(
                &self.inner,
                NativeCancellationProvenance::Exact(reason.clone()),
            );
            self.cancel_with_reason(native_reason_to_local(&reason));
        }
    }

    #[cfg(feature = "native")]
    #[must_use]
    fn maybe_cancel_via_native_cx(&self, masked: bool) -> bool {
        let Some(native) = self.native_cx_for_checkpoint() else {
            return false;
        };

        if masked {
            if native.is_cancel_requested() {
                self.ingest_native_cancellation(&native);
                return true;
            }
            return false;
        }

        if native.checkpoint().is_err() {
            self.ingest_native_cancellation(&native);
            return true;
        }
        false
    }

    #[must_use]
    fn maybe_cancel_via_operation_source(&self) -> bool {
        let Some(source) = self.inner.operation_cancellation_source.as_ref() else {
            return false;
        };
        if !source.cancel_requested.load(Ordering::Acquire) {
            return false;
        }
        let reason = *source
            .cancel_reason
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        #[cfg(feature = "native")]
        let native_provenance = source
            .native_cancel_provenance
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        #[cfg(feature = "native")]
        if let Some(provenance) = native_provenance {
            propagate_native_cancel_provenance(&self.inner, provenance);
        }
        self.cancel_with_reason(reason.unwrap_or(CancelReason::UserInterrupt));
        true
    }

    // -----------------------------------------------------------------------
    // Checkpoints (§4.12.1)
    // -----------------------------------------------------------------------

    /// Check for cancellation at a yield point.
    ///
    /// Returns `Ok(())` when not cancelled **or when inside a masked section**.
    /// When cancellation is observed, transitions state from `CancelRequested`
    /// to `Cancelling`.
    ///
    /// Hot-path note: the cheap `cancel_requested` atomic load is consulted
    /// first, then `mask_depth`. Only if neither cheap signal proves we're
    /// clear do we consult the e-process oracle and the native asupersync
    /// `Cx::checkpoint()`. Previously `maybe_cancel_via_native_cx` was
    /// evaluated **unconditionally** before the fast-path test — every
    /// checkpoint paid for the nested asupersync cancel machinery even when
    /// the cheap atomic said "not cancelled". That showed up as 5.87%
    /// self-time on the 2026-04-23 post-bench-fix MT 8t capture
    /// (`fsqlite-bench-fix-validation-194151`).
    pub fn checkpoint(&self) -> Result<()> {
        let cancel_requested = self.inner.cancel_requested.load(Ordering::Acquire);
        if !cancel_requested {
            // Cheap path already proved we're not locally cancelled. Only
            // the operation source, oracle, or native cx can still observe a
            // cancel signal.
            if !self.maybe_cancel_via_operation_source() && !self.maybe_cancel_via_eprocess() {
                #[cfg(feature = "native")]
                {
                    let masked = self.inner.mask_depth.load(Ordering::Acquire) > 0;
                    if !self.maybe_cancel_via_native_cx(masked) {
                        return Ok(());
                    }
                }
                #[cfg(not(feature = "native"))]
                {
                    return Ok(());
                }
            }
        } else {
            // Once the project plane is cancelled the normal hot path is no
            // longer relevant. Continue sampling both the durable operation
            // source and an exact native reason so a later, stronger
            // cancellation is not hidden by an earlier local request.
            let _ = self.maybe_cancel_via_operation_source();
            #[cfg(feature = "native")]
            self.capture_native_reason_if_present();
        }

        // Either cancel_requested is set locally, or one of the async plane
        // checks fired. Masked sections defer observation unconditionally.
        let masked = self.inner.mask_depth.load(Ordering::Acquire) > 0;
        if masked {
            return Ok(());
        }

        // Slow path: transition both the engine context and its exact
        // operation source from CancelRequested → Cancelling. The latter is
        // the provenance receipt used at the actor response boundary.
        mark_cancellation_observed(&self.inner);
        if let Some(source) = self.inner.operation_cancellation_source.as_ref() {
            mark_cancellation_observed(source);
        }
        Err(Error::cancelled())
    }

    /// Check for cancellation and record a progress message.
    pub fn checkpoint_with(&self, msg: impl Into<String>) -> Result<()> {
        {
            let mut guard = self
                .inner
                .last_checkpoint_msg
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *guard = Some(msg.into());
        }
        self.checkpoint()
    }

    #[must_use]
    pub fn last_checkpoint_message(&self) -> Option<String> {
        self.inner
            .last_checkpoint_msg
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Most recent e-process decision recorded during [`Self::checkpoint`].
    #[must_use]
    pub fn last_eprocess_decision(&self) -> Option<EProcessDecision> {
        self.inner
            .last_eprocess_decision
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Snapshot portion of the most recent e-process decision.
    #[must_use]
    pub fn last_eprocess_snapshot(&self) -> Option<EProcessSnapshot> {
        self.last_eprocess_decision()
            .map(|decision| decision.snapshot)
    }

    fn record_eprocess_decision(&self, decision: EProcessDecision) {
        *self
            .inner
            .last_eprocess_decision
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(decision);
    }

    // -----------------------------------------------------------------------
    // Masked critical sections (§4.12.2)
    // -----------------------------------------------------------------------

    /// Enter a masked section where `checkpoint()` returns `Ok(())` even if
    /// cancellation is requested.
    ///
    /// Returns a [`MaskGuard`] whose `Drop` restores the mask depth.
    ///
    /// # Panics
    ///
    /// Panics if nesting exceeds [`MAX_MASK_DEPTH`] (INV-MASK-BOUNDED).
    #[must_use]
    pub fn masked(&self) -> MaskGuard<'_> {
        let prev = self.inner.mask_depth.fetch_add(1, Ordering::AcqRel);
        if prev >= MAX_MASK_DEPTH {
            self.inner.mask_depth.fetch_sub(1, Ordering::Release);
            assert!(
                prev < MAX_MASK_DEPTH,
                "MAX_MASK_DEPTH ({MAX_MASK_DEPTH}) exceeded: mask nesting depth would be {}",
                prev + 1
            );
        }
        MaskGuard { inner: &self.inner }
    }

    /// Current mask nesting depth.
    #[must_use]
    pub fn mask_depth(&self) -> u32 {
        self.inner.mask_depth.load(Ordering::Acquire)
    }

    // -----------------------------------------------------------------------
    // Commit sections (§4.12.3)
    // -----------------------------------------------------------------------

    /// Execute a logically atomic commit section.
    ///
    /// The section masks cancellation, enforces a poll quota bound, and
    /// guarantees the `finalizer` runs even on cancellation or panic.
    pub fn commit_section<R>(
        &self,
        poll_quota: u32,
        body: impl FnOnce(&CommitCtx) -> R,
        finalizer: impl FnOnce(),
    ) -> R {
        struct FinGuard<G: FnOnce()>(Option<G>);
        impl<G: FnOnce()> Drop for FinGuard<G> {
            fn drop(&mut self) {
                if let Some(f) = self.0.take() {
                    f();
                }
            }
        }

        let _mask = self.masked();
        let _fin = FinGuard(Some(finalizer));
        let ctx = CommitCtx::new(poll_quota);
        body(&ctx)
    }

    // -----------------------------------------------------------------------
    // Child context management (INV-CANCEL-PROPAGATES)
    // -----------------------------------------------------------------------

    /// Create a child `Cx` that shares the parent's budget but has
    /// independent cancellation state. Cancelling the parent propagates
    /// to this child. Tracing IDs propagate to the child.
    #[must_use]
    pub fn create_child(&self) -> Self {
        self.create_child_impl(true)
    }

    /// Create a same-capability child without copying an attached native
    /// runtime context.
    ///
    /// This is the safe construction primitive for caller-local cancellation
    /// relays that may later cross an OS-thread boundary. Unlike
    /// `create_child().clear_native_cx()`, the task-affine native handle is
    /// never transiently cloned into the new child.
    #[must_use]
    pub fn create_native_free_child(&self) -> Self {
        self.create_child_impl(false)
    }

    /// Create the one-way cancellation source/token pair for a database
    /// operation.
    ///
    /// The token is constructed natively free and without changing the
    /// caller's capability type. Parent cancellation propagates into the
    /// token, while source cancellation never propagates back into the parent.
    pub fn operation_cancellation(
        &self,
    ) -> (OperationCancellationSource, OperationCancellationToken) {
        // Build the handoff directly instead of using the generic child
        // constructor. The latter intentionally copies the parent's
        // e-process oracle, which is caller-side policy state and must not
        // cross the actor boundary inside an otherwise capability-free token.
        let operation = Self::with_budget_and_operation_cancellation_source(self.budget, None);
        register_cancellation_child(&self.inner, &operation.inner);
        inherit_existing_cancellation(&self.inner, &operation.inner);
        if let Some(source) = self.inner.operation_cancellation_source.as_ref() {
            register_cancellation_child(source, &operation.inner);
            inherit_existing_cancellation(source, &operation.inner);
        }
        let inner = operation.inner;
        (
            OperationCancellationSource {
                inner: Arc::clone(&inner),
                armed: true,
            },
            OperationCancellationToken {
                inner,
                caller_budget: self.budget,
                caller_trace_id: self.trace_id,
                caller_policy_id: self.policy_id,
            },
        )
    }

    /// Derive an engine context from this root and link it to an opaque
    /// per-operation cancellation token.
    ///
    /// Runtime handles, budgets, tracing, and effect capabilities come only
    /// from `self`. The token contributes cancellation in one direction and
    /// cannot widen or replace the engine context's capability type.
    #[must_use]
    pub fn create_child_linked_to_operation(&self, operation: &OperationCancellationToken) -> Self {
        // A cloned asupersync NativeCx shares cancellation state with its
        // parent. Operation children therefore never inherit the engine
        // root's attachment: cancelling one command must not contaminate the
        // root or siblings. Dedicated-worker-compatible roots are detached,
        // and actor engine work observes this project Cx directly.
        let mut child =
            self.create_child_impl_with_operation_source(false, Some(Arc::clone(&operation.inner)));
        child.budget = self.budget.meet(operation.caller_budget);
        if operation.caller_trace_id != 0 {
            child.trace_id = operation.caller_trace_id;
        }
        if operation.caller_policy_id != 0 {
            child.policy_id = operation.caller_policy_id;
        }
        if let Some(source) = self.inner.operation_cancellation_source.as_ref()
            && !Arc::ptr_eq(source, &operation.inner)
        {
            register_cancellation_child(source, &child.inner);
            inherit_existing_cancellation(source, &child.inner);
        }
        child
    }

    /// Derive an operation-linked child directly from this root while using a
    /// fresh trace ID only when the caller did not supply one.
    ///
    /// This avoids creating a disposable intermediate parent merely to carry
    /// fallback trace metadata. Parent links are weak, so dropping such an
    /// intermediate would sever root cancellation propagation.
    #[doc(hidden)]
    #[must_use]
    pub fn create_child_linked_to_operation_with_fallback_trace(
        &self,
        operation: &OperationCancellationToken,
        fallback_trace_id: u64,
    ) -> Self {
        let caller_has_trace = operation.caller_trace_id != 0;
        let mut child = self.create_child_linked_to_operation(operation);
        if !caller_has_trace {
            child.trace_id = fallback_trace_id;
        }
        child
    }

    fn create_child_impl(&self, inherit_native: bool) -> Self {
        self.create_child_impl_with_operation_source(
            inherit_native,
            self.inner.operation_cancellation_source.clone(),
        )
    }

    fn create_child_impl_with_operation_source(
        &self,
        _inherit_native: bool,
        operation_cancellation_source: Option<Arc<CxInner>>,
    ) -> Self {
        let mut child = Self::with_budget_and_operation_cancellation_source(
            self.budget,
            operation_cancellation_source,
        );
        child.trace_id = self.trace_id;
        child.decision_id = self.decision_id;
        child.policy_id = self.policy_id;
        if let Some(oracle) = self.inner.eprocess_oracle.get().cloned() {
            child.set_eprocess_oracle(oracle);
        }
        register_cancellation_child(&self.inner, &child.inner);
        inherit_existing_cancellation(&self.inner, &child.inner);
        if let Some(source) = child.inner.operation_cancellation_source.as_ref()
            && !Arc::ptr_eq(source, &self.inner)
        {
            register_cancellation_child(source, &child.inner);
            inherit_existing_cancellation(source, &child.inner);
        }
        #[cfg(feature = "native")]
        if _inherit_native {
            if let Some(native_cx) = self.attached_native_cx() {
                // Publish every project-plane parent/source link and recheck
                // inherited cancellation before exposing the child to a shared
                // native wake signal.
                child.set_native_cx(native_cx);
            }
        }
        child
    }

    /// Set a deterministic unix time for tests.
    pub fn set_unix_millis_for_testing(&self, millis: u64)
    where
        Caps: cap::HasTime,
    {
        self.inner.unix_millis.store(millis, Ordering::Release);
    }

    /// Return current time as a Julian day (via deterministic unix millis).
    #[must_use]
    pub fn current_time_julian_day(&self) -> f64
    where
        Caps: cap::HasTime,
    {
        let millis = self.inner.unix_millis.load(Ordering::Acquire);
        #[allow(clippy::cast_precision_loss)]
        let secs = (millis as f64) / 1000.0;
        // Unix epoch in Julian days: 2440587.5
        2_440_587.5 + (secs / 86_400.0)
    }
}

// ---------------------------------------------------------------------------
// MaskGuard — RAII guard for masked cancellation sections (§4.12.2)
// ---------------------------------------------------------------------------

/// RAII guard that keeps the `Cx` masked while alive.
///
/// Created by [`Cx::masked()`]. On drop, the mask depth is decremented.
#[derive(Debug)]
pub struct MaskGuard<'a> {
    inner: &'a CxInner,
}

impl Drop for MaskGuard<'_> {
    fn drop(&mut self) {
        self.inner.mask_depth.fetch_sub(1, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// CommitCtx — bounded context for commit sections (§4.12.3)
// ---------------------------------------------------------------------------

/// Context passed to commit-section bodies.
///
/// Tracks a poll-quota budget that operations can decrement via [`Self::tick`].
#[derive(Debug)]
pub struct CommitCtx {
    poll_remaining: AtomicU32,
}

impl CommitCtx {
    fn new(poll_quota: u32) -> Self {
        Self {
            poll_remaining: AtomicU32::new(poll_quota),
        }
    }

    /// Remaining poll budget.
    #[must_use]
    pub fn poll_remaining(&self) -> u32 {
        self.poll_remaining.load(Ordering::Acquire)
    }

    /// Consume one unit of poll budget. Returns `true` if budget remains.
    pub fn tick(&self) -> bool {
        let prev = self.poll_remaining.load(Ordering::Acquire);
        if prev == 0 {
            return false;
        }
        self.poll_remaining.fetch_sub(1, Ordering::AcqRel);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eprocess::{EProcessConfig, EProcessSignal};
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Weak};

    fn test_interleaving_pause(
        expected_reason: Option<CancelReason>,
    ) -> Arc<TestInterleavingPause> {
        Arc::new(TestInterleavingPause {
            expected_reason,
            armed: AtomicBool::new(true),
            reached: std::sync::Barrier::new(2),
            resume: std::sync::Barrier::new(2),
        })
    }

    #[test]
    fn test_cx_checkpoint_observes_cancellation() {
        let cx = Cx::new();
        assert!(cx.checkpoint().is_ok());
        cx.cancel();
        let err = cx.checkpoint().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Cancelled);
        assert_eq!(err.sqlite_error_code(), SQLITE_INTERRUPT);
    }

    #[test]
    fn test_cx_capability_narrowing_compiles() {
        let cx = Cx::<FullCaps>::new();
        let _compute = cx.restrict::<ComputeCaps>();
        let _storage = cx.restrict::<StorageCaps>();
    }

    #[test]
    fn test_cx_budget_meet_tightens() {
        let parent = Budget::INFINITE.with_deadline(Duration::from_millis(100));
        let child = Budget::INFINITE.with_deadline(Duration::from_millis(200));
        let effective = parent.meet(child);
        assert_eq!(effective.deadline, Some(Duration::from_millis(100)));
    }

    #[test]
    fn test_cx_budget_priority_join() {
        let parent = Budget::INFINITE.with_priority(2);
        let child = Budget::INFINITE.with_priority(5);
        let effective = parent.meet(child);
        assert_eq!(effective.priority, 5);
    }

    #[test]
    fn test_cx_scope_with_budget_cannot_loosen() {
        let cx =
            Cx::<FullCaps>::with_budget(Budget::INFINITE.with_deadline(Duration::from_millis(50)));
        let child = Budget::INFINITE.with_deadline(Duration::from_millis(100));
        let scoped = cx.scope_with_budget(child);
        assert_eq!(scoped.budget().deadline, Some(Duration::from_millis(50)));
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_native_child_scope_deadline_uses_parent_runtime_clock() {
        let runtime = asupersync::runtime::RuntimeBuilder::current_thread()
            .build()
            .expect("build native-budget test runtime");
        runtime.block_on(async {
            let native_cx =
                NativeCx::current().expect("runtime task must expose its native context");
            let timeout = Duration::from_millis(25);
            let project_cx = Cx::<FullCaps>::with_budget(
                Budget::INFINITE
                    .with_deadline(timeout)
                    .with_poll_quota(41)
                    .with_cost_quota(73)
                    .with_priority(9),
            );

            let before = native_cx.now();
            let native_budget = project_cx.native_budget_for_child_scope(&native_cx);
            let after = native_cx.now();
            let expected_priority = native_cx.budget().priority.max(9);
            let deadline = native_budget
                .deadline
                .expect("relative project deadline must become a native deadline");

            assert!(
                deadline >= before + timeout && deadline <= after + timeout,
                "native deadline must be anchored to the active runtime clock: \
                 before={before:?}, deadline={deadline:?}, after={after:?}"
            );
            assert_eq!(native_budget.poll_quota, 41);
            assert_eq!(native_budget.cost_quota, Some(73));
            assert_eq!(native_budget.priority, expected_priority);
        });
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_native_child_scope_priority_is_monotonic_across_both_planes() {
        let native_high = NativeCx::for_request_with_budget(
            NativeBudget::INFINITE
                .with_poll_quota(500)
                .with_priority(200),
        );
        let project_low =
            Cx::<FullCaps>::with_budget(Budget::INFINITE.with_priority(19).with_poll_quota(400));
        let high_parent_effective = project_low.native_budget_for_child_scope(&native_high);
        assert_eq!(high_parent_effective.priority, 200);
        assert_eq!(high_parent_effective.poll_quota, 400);

        let native_low = NativeCx::for_request_with_budget(
            NativeBudget::INFINITE.with_poll_quota(300).with_priority(7),
        );
        let project_high =
            Cx::<FullCaps>::with_budget(Budget::INFINITE.with_priority(211).with_poll_quota(600));
        let high_project_effective = project_high.native_budget_for_child_scope(&native_low);
        assert_eq!(high_project_effective.priority, 211);
        assert_eq!(high_project_effective.poll_quota, 300);
    }

    #[test]
    fn test_cx_checkpoint_with_message_records_message() {
        let cx = Cx::new();
        assert!(cx.checkpoint_with("vdbe pc=5").is_ok());
        assert_eq!(cx.last_checkpoint_message().as_deref(), Some("vdbe pc=5"));
    }

    #[test]
    fn test_cx_cleanup_uses_minimal_budget() {
        let cx = Cx::<FullCaps>::with_budget(Budget::INFINITE.with_poll_quota(10_000));
        let cleanup = cx.cleanup_scope();
        assert_eq!(cleanup.budget(), Budget::MINIMAL);
    }

    #[test]
    fn test_cx_restrict_storage_to_compute() {
        let cx = Cx::<FullCaps>::new();
        let storage = cx.restrict::<StorageCaps>();
        let _compute = storage.restrict::<ComputeCaps>();
    }

    #[test]
    fn test_cx_restrict_is_zero_cost() {
        // CapSet is a ZST; Cx carries only Arc + Budget + PhantomData.
        // Restrict changes only the phantom marker — same size, same pointer.
        assert_eq!(
            std::mem::size_of::<Cx<FullCaps>>(),
            std::mem::size_of::<Cx<ComputeCaps>>()
        );
    }

    #[test]
    fn test_budget_mixed_lattice() {
        let a = Budget {
            deadline: Some(Duration::from_millis(100)),
            poll_quota: 500,
            cost_quota: Some(1000),
            priority: 2,
        };
        let b = Budget {
            deadline: Some(Duration::from_millis(200)),
            poll_quota: 300,
            cost_quota: Some(2000),
            priority: 5,
        };
        let m = a.meet(b);
        // Resources tighten by min.
        assert_eq!(m.deadline, Some(Duration::from_millis(100)));
        assert_eq!(m.poll_quota, 300);
        assert_eq!(m.cost_quota, Some(1000));
        // Priority propagates by max (join).
        assert_eq!(m.priority, 5);
    }

    #[test]
    fn test_budget_meet_commutative() {
        let a = Budget {
            deadline: Some(Duration::from_millis(50)),
            poll_quota: 400,
            cost_quota: Some(800),
            priority: 3,
        };
        let b = Budget {
            deadline: Some(Duration::from_millis(150)),
            poll_quota: 200,
            cost_quota: None,
            priority: 7,
        };
        assert_eq!(a.meet(b), b.meet(a));
    }

    #[test]
    fn test_budget_meet_associative() {
        let a = Budget::INFINITE
            .with_deadline(Duration::from_millis(50))
            .with_poll_quota(100)
            .with_priority(1);
        let b = Budget::INFINITE
            .with_deadline(Duration::from_millis(150))
            .with_poll_quota(200)
            .with_priority(5);
        let c = Budget::INFINITE
            .with_deadline(Duration::from_millis(75))
            .with_poll_quota(50)
            .with_priority(3);
        assert_eq!(a.meet(b).meet(c), a.meet(b.meet(c)));
    }

    #[test]
    fn test_budget_minimal_is_stricter_than_normal() {
        let normal = Budget::INFINITE.with_poll_quota(10_000);
        let effective = normal.meet(Budget::MINIMAL);
        assert_eq!(effective.poll_quota, Budget::MINIMAL.poll_quota);
    }

    #[test]
    fn test_cx_cancel_shared_across_clones() {
        let cx1 = Cx::<FullCaps>::new();
        let cx2 = cx1.clone();
        assert!(!cx2.is_cancel_requested());
        cx1.cancel();
        assert!(cx2.is_cancel_requested());
        assert!(cx2.checkpoint().is_err());
    }

    #[test]
    fn test_cx_cancel_shared_across_restrict() {
        let cx = Cx::<FullCaps>::new();
        let compute = cx.restrict::<ComputeCaps>();
        cx.cancel();
        assert!(compute.checkpoint().is_err());
    }

    #[test]
    fn test_cx_current_time_julian_day() {
        let cx = Cx::<FullCaps>::new();
        // Unix epoch = Julian day 2440587.5
        cx.set_unix_millis_for_testing(0);
        let jd = cx.current_time_julian_day();
        assert!((jd - 2_440_587.5).abs() < 1e-10);

        // 1 day = 86_400_000 ms
        cx.set_unix_millis_for_testing(86_400_000);
        let jd = cx.current_time_julian_day();
        assert!((jd - 2_440_588.5).abs() < 1e-10);
    }

    #[test]
    fn test_capset_is_zero_sized() {
        assert_eq!(std::mem::size_of::<cap::All>(), 0);
        assert_eq!(std::mem::size_of::<cap::None>(), 0);
        assert_eq!(
            std::mem::size_of::<cap::CapSet<true, false, true, false, true>>(),
            0
        );
    }

    #[test]
    fn test_cx_checkpoint_not_cancelled() {
        let cx = Cx::new();
        assert!(cx.checkpoint().is_ok());
        assert!(cx.checkpoint_with("still going").is_ok());
    }

    #[test]
    fn test_cx_checkpoint_maps_to_sqlite_interrupt() {
        let cx = Cx::new();
        cx.cancel();
        let err = cx.checkpoint().unwrap_err();
        assert_eq!(err.sqlite_error_code(), SQLITE_INTERRUPT);
    }

    #[test]
    fn test_cx_checkpoint_eprocess_sheds_low_priority_context() {
        let cx = Cx::<FullCaps>::with_budget(Budget::INFINITE.with_priority(3));
        let oracle = Arc::new(EProcessOracle::new(
            EProcessConfig {
                p0: 0.1,
                lambda: 5.0,
                alpha: 0.05,
                max_evalue: 1e12,
            },
            1,
        ));
        let signal = EProcessSignal::new(1.0, 1.0, 1.0);
        oracle.observe_signal(signal);
        oracle.observe_signal(signal);
        cx.set_eprocess_oracle(oracle);
        let err = cx.checkpoint().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Cancelled);
        assert_eq!(cx.cancel_reason(), Some(CancelReason::Abort));
        let decision = cx
            .last_eprocess_decision()
            .expect("checkpoint should record an e-process decision");
        assert!(decision.should_shed);
        assert_eq!(decision.snapshot.last_signal, Some(signal));
    }

    #[test]
    fn test_cx_checkpoint_eprocess_respects_priority_threshold() {
        let cx = Cx::<FullCaps>::with_budget(Budget::INFINITE.with_priority(1));
        let oracle = Arc::new(EProcessOracle::new(
            EProcessConfig {
                p0: 0.1,
                lambda: 5.0,
                alpha: 0.05,
                max_evalue: 1e12,
            },
            1,
        ));
        let signal = EProcessSignal::new(1.0, 1.0, 1.0);
        oracle.observe_signal(signal);
        oracle.observe_signal(signal);
        cx.set_eprocess_oracle(oracle);
        assert!(cx.checkpoint().is_ok());
        assert!(!cx.is_cancel_requested());
        let decision = cx
            .last_eprocess_decision()
            .expect("checkpoint should still record non-shedding decisions");
        assert!(!decision.should_shed);
        assert_eq!(decision.priority, 1);
        assert_eq!(decision.snapshot.last_signal, Some(signal));
    }

    #[test]
    fn test_cx_checkpoint_eprocess_preserves_masking_semantics() {
        let cx = Cx::<FullCaps>::with_budget(Budget::INFINITE.with_priority(3));
        let oracle = Arc::new(EProcessOracle::new(
            EProcessConfig {
                p0: 0.1,
                lambda: 5.0,
                alpha: 0.05,
                max_evalue: 1e12,
            },
            1,
        ));
        let signal = EProcessSignal::new(1.0, 1.0, 1.0);
        oracle.observe_signal(signal);
        oracle.observe_signal(signal);
        cx.set_eprocess_oracle(oracle);
        {
            let _mask = cx.masked();
            assert!(cx.checkpoint().is_ok());
            assert!(cx.is_cancel_requested());
            assert_eq!(cx.cancel_state(), CancelState::CancelRequested);
            assert_eq!(
                cx.last_eprocess_snapshot()
                    .expect("checkpoint should record the masked decision")
                    .last_signal,
                Some(signal)
            );
        }
        let err = cx.checkpoint().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Cancelled);
    }

    #[test]
    fn test_create_child_inherits_eprocess_oracle() {
        let parent = Cx::<FullCaps>::with_budget(Budget::INFINITE.with_priority(3));
        let oracle = Arc::new(EProcessOracle::new(
            EProcessConfig {
                p0: 0.1,
                lambda: 5.0,
                alpha: 0.05,
                max_evalue: 1e12,
            },
            1,
        ));
        let signal = EProcessSignal::new(1.0, 1.0, 1.0);
        oracle.observe_signal(signal);
        oracle.observe_signal(signal);
        parent.set_eprocess_oracle(oracle);

        let child = parent.create_child();
        let err = child.checkpoint().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Cancelled);
        assert_eq!(child.cancel_reason(), Some(CancelReason::Abort));
        assert_eq!(
            child
                .last_eprocess_snapshot()
                .expect("child checkpoint should record inherited oracle decision")
                .last_signal,
            Some(signal)
        );
    }

    #[test]
    fn test_create_child_inherits_preexisting_parent_cancellation() {
        let parent = Cx::<FullCaps>::new();
        parent.cancel_with_reason(CancelReason::RegionClose);

        let child = parent.create_child();
        assert_eq!(child.cancel_reason(), Some(CancelReason::RegionClose));
        assert_eq!(child.cancel_state(), CancelState::CancelRequested);

        let err = child.checkpoint().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Cancelled);
    }

    #[test]
    fn test_cancel_reason_precedes_flag_and_linked_child_never_invents_reason() {
        let parent = Cx::<FullCaps>::new();
        let pause = test_interleaving_pause(Some(CancelReason::Timeout));
        *parent
            .inner
            .cancel_publication_pause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&pause));

        let cancelling_parent = parent.clone();
        let cancellation = std::thread::spawn(move || {
            cancelling_parent.cancel_with_reason(CancelReason::Timeout);
        });

        pause.reached.wait();
        assert_eq!(parent.cancel_reason(), Some(CancelReason::Timeout));
        assert_eq!(
            parent.cancel_state(),
            CancelState::CancelRequested,
            "the lifecycle state must precede the fast cancellation bit"
        );
        assert!(
            !parent.is_cancel_requested(),
            "the test must stop between reason/state publication and the Release flag"
        );

        let child = parent.create_child();
        assert_eq!(
            child.cancel_reason(),
            Some(CancelReason::Timeout),
            "a child linked in the publication window must inherit the exact reason"
        );

        pause.resume.wait();
        cancellation
            .join()
            .expect("cancellation publisher should finish");
        assert!(parent.is_cancel_requested());
        assert_eq!(child.cancel_reason(), Some(CancelReason::Timeout));
    }

    #[test]
    fn test_equal_cancel_assists_inflight_publication_before_returning() {
        let cx = Cx::<FullCaps>::new();
        let pause = test_interleaving_pause(Some(CancelReason::Timeout));
        *cx.inner
            .cancel_publication_pause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&pause));

        let first_cx = cx.clone();
        let first = std::thread::spawn(move || {
            first_cx.cancel_with_reason(CancelReason::Timeout);
        });

        pause.reached.wait();
        assert_eq!(cx.cancel_reason(), Some(CancelReason::Timeout));
        assert!(!cx.is_cancel_requested());

        cx.cancel_with_reason(CancelReason::Timeout);

        assert!(
            cx.is_cancel_requested(),
            "an equal caller must finish the in-flight Release publication before returning"
        );
        assert_eq!(
            cx.inner
                .accepted_cancel_propagations
                .load(Ordering::Relaxed),
            1,
            "helping publication must not count as a second accepted reason"
        );

        let child = cx.create_child();
        assert_eq!(child.cancel_reason(), Some(CancelReason::Timeout));
        assert!(child.is_cancel_requested());

        pause.resume.wait();
        first
            .join()
            .expect("the original cancellation publisher should finish");
    }

    #[test]
    fn test_equal_cancel_completes_descendant_propagation_after_flag_publication() {
        let parent = Cx::<FullCaps>::new();
        #[cfg(feature = "native")]
        let native = NativeCx::for_testing();
        #[cfg(feature = "native")]
        parent.set_native_cx(native.clone());
        let child = parent.create_child();
        let pause = test_interleaving_pause(Some(CancelReason::Timeout));
        *parent
            .inner
            .cancel_after_flag_pause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&pause));

        let first_parent = parent.clone();
        let first = std::thread::spawn(move || {
            first_parent.cancel_with_reason(CancelReason::Timeout);
        });

        pause.reached.wait();
        assert!(parent.is_cancel_requested());
        assert!(
            !child.is_cancel_requested(),
            "the test must stop after the parent flag but before descendant propagation"
        );

        parent.cancel_with_reason(CancelReason::Timeout);

        assert!(
            child.is_cancel_requested(),
            "an equal caller must complete the in-flight descendant walk before returning"
        );
        assert_eq!(child.cancel_reason(), Some(CancelReason::Timeout));
        #[cfg(feature = "native")]
        assert!(
            native.checkpoint().is_err(),
            "the helping caller must also finish native wake signaling"
        );

        pause.resume.wait();
        first
            .join()
            .expect("the original cancellation publisher should finish");
    }

    #[test]
    fn test_diamond_reprocesses_shared_descendant_for_stronger_effective_reason() {
        let root = Cx::<FullCaps>::new();
        let strong_branch = Cx::<FullCaps>::new();
        let weak_branch = Cx::<FullCaps>::new();
        let shared = Cx::<FullCaps>::new();
        register_cancellation_child(&root.inner, &strong_branch.inner);
        register_cancellation_child(&root.inner, &weak_branch.inner);
        register_cancellation_child(&strong_branch.inner, &shared.inner);
        register_cancellation_child(&weak_branch.inner, &shared.inner);

        let pause = test_interleaving_pause(Some(CancelReason::Abort));
        *strong_branch
            .inner
            .cancel_after_flag_pause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&pause));
        let cancelling_branch = strong_branch.clone();
        let stronger = std::thread::spawn(move || {
            cancelling_branch.cancel_with_reason(CancelReason::Abort);
        });

        pause.reached.wait();
        root.cancel_with_reason(CancelReason::Timeout);

        assert_eq!(
            shared.cancel_reason(),
            Some(CancelReason::Abort),
            "a node first reached through the weak branch must be revisited when the other branch \
             exposes a stronger effective reason"
        );
        assert_eq!(
            *strong_branch
                .inner
                .cancel_fully_propagated_reason
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            Some(CancelReason::Abort),
            "strong-branch completion is valid only after its shared descendant is upgraded"
        );

        pause.resume.wait();
        stronger
            .join()
            .expect("the original strong publisher should finish");
        assert_eq!(shared.cancel_reason(), Some(CancelReason::Abort));
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_project_wake_never_becomes_reasonless_native_child_provenance() {
        let parent = Cx::<FullCaps>::new();
        let native = NativeCx::for_testing();
        parent.set_native_cx(native.clone());
        let child = parent.create_child();
        let pause = test_interleaving_pause(None);
        *parent
            .inner
            .native_signal_pause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&pause));

        let cancelling_parent = parent.clone();
        let cancellation = std::thread::spawn(move || {
            cancelling_parent.cancel_with_reason(CancelReason::RegionClose);
        });

        pause.reached.wait();
        assert!(
            child.is_cancel_requested(),
            "the entire project lineage must be marked before the first native wake"
        );
        assert!(
            child.checkpoint().is_err(),
            "the child must observe the project cancellation while signaling is paused"
        );
        assert_eq!(
            child.native_cancel_provenance(),
            None,
            "a project-origin wake must not be recorded as unexplained native provenance"
        );

        pause.resume.wait();
        cancellation
            .join()
            .expect("two-phase cancellation propagation should finish");
        assert!(native.checkpoint().is_err());
        assert_eq!(child.native_cancel_provenance(), None);
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_native_wake_reentrant_equal_cancel_is_edge_triggered() {
        struct ReentrantCancel {
            cx: Cx<FullCaps>,
            wake_count: AtomicU32,
        }

        impl std::task::Wake for ReentrantCancel {
            fn wake(self: Arc<Self>) {
                self.wake_count.fetch_add(1, Ordering::Relaxed);
                self.cx.cancel_with_reason(CancelReason::Timeout);
            }
        }

        std::thread::Builder::new()
            .name("cx-reentrant-wake".to_string())
            .stack_size(256 * 1024)
            .spawn(|| {
                use std::future::Future;
                use std::task::{Context, Poll, Waker};

                let cx = Cx::<FullCaps>::new();
                let native = NativeCx::for_testing();
                cx.set_native_cx(native.clone());
                let waiter = asupersync::sync::OnceCell::<()>::new();
                let wake_state = Arc::new(ReentrantCancel {
                    cx: cx.clone(),
                    wake_count: AtomicU32::new(0),
                });
                let waker = Waker::from(Arc::clone(&wake_state));
                let mut task_cx = Context::from_waker(&waker);
                let mut wait = Box::pin(waiter.wait(&native));
                assert_eq!(wait.as_mut().poll(&mut task_cx), Poll::Pending);

                cx.cancel_with_reason(CancelReason::Timeout);

                assert_eq!(
                    wake_state.wake_count.load(Ordering::Relaxed),
                    1,
                    "the native cancellation edge must wake a reentrant waiter exactly once"
                );
                assert_eq!(cx.cancel_reason(), Some(CancelReason::Timeout));
            })
            .expect("spawn small-stack reentrant cancellation test")
            .join()
            .expect("reentrant cancellation must not recurse or overflow");
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_cx_checkpoint_native_cx_cancellation_maps_reason() {
        let cx = Cx::<FullCaps>::new();
        let native = NativeCx::for_testing();
        cx.set_native_cx(native.clone());
        native.set_cancel_reason(NativeCancelReason::timeout());

        let err = cx.checkpoint().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Cancelled);
        assert_eq!(cx.cancel_reason(), Some(CancelReason::Timeout));
        assert_eq!(
            cx.native_cancel_provenance(),
            Some(NativeCancellationProvenance::Exact(
                NativeCancelReason::timeout()
            ))
        );
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_cx_cancel_signals_native_without_fabricating_native_attribution() {
        let cx = Cx::<FullCaps>::new();
        let native = NativeCx::for_testing();
        cx.set_native_cx(native.clone());

        cx.cancel_with_reason(CancelReason::RegionClose);
        assert!(native.checkpoint().is_err());
        assert!(
            native.cancel_reason().is_none(),
            "the project bridge must not manufacture or overwrite native attribution"
        );
        let err = cx
            .checkpoint()
            .expect_err("the project cancellation must remain observable");
        assert_eq!(err.kind(), ErrorKind::Cancelled);
        assert_eq!(cx.cancel_reason(), Some(CancelReason::RegionClose));
        assert_eq!(
            cx.native_cancel_provenance(),
            None,
            "a reasonless echo of the project's own wake is not native provenance"
        );
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_native_install_rechecks_cancellation_after_publication_gap() {
        let cx = Cx::<FullCaps>::new();
        let native = NativeCx::for_testing();
        let pause = test_interleaving_pause(None);
        *cx.inner
            .native_install_pause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&pause));

        let installing_cx = cx.clone();
        let installing_native = native.clone();
        let installation = std::thread::spawn(move || {
            installing_cx.set_native_cx(installing_native);
        });

        pause.reached.wait();
        cx.cancel_with_reason(CancelReason::RegionClose);
        assert!(
            native.checkpoint().is_ok(),
            "the unpublished native context must not be reachable by cancellation"
        );
        pause.resume.wait();
        installation
            .join()
            .expect("native-context installation should finish");

        assert!(
            native.checkpoint().is_err(),
            "post-publication recheck must signal cancellation"
        );
        assert!(
            native.cancel_reason().is_none(),
            "a project-origin signal must not fabricate native attribution"
        );
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_local_cancellation_never_overwrites_native_reason_or_cause() {
        let cx = Cx::<FullCaps>::new();
        let attached = NativeCx::for_testing();
        let expected = NativeCancelReason::shutdown()
            .with_message("runtime shutdown")
            .with_cause(NativeCancelReason::cost_budget());
        attached.set_cancel_reason(expected.clone());
        cx.set_native_cx(attached.clone());

        cx.cancel_with_reason(CancelReason::UserInterrupt);
        cx.cancel_with_reason(CancelReason::Timeout);
        cx.cancel_with_reason(CancelReason::Abort);

        assert_eq!(cx.cancel_reason(), Some(CancelReason::Abort));
        assert_eq!(
            cx.native_cancel_provenance(),
            Some(NativeCancellationProvenance::Exact(expected.clone()))
        );
        assert_eq!(
            attached.cancel_reason(),
            Some(expected),
            "project cancellation must preserve native severity, attribution, and cause"
        );
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_native_install_after_local_cancel_retains_preexisting_exact_reason() {
        let attached_cx = Cx::<FullCaps>::new();
        attached_cx.cancel_with_reason(CancelReason::RegionClose);
        let attached = NativeCx::for_testing();
        let attached_reason =
            NativeCancelReason::shutdown().with_message("preexisting attached shutdown");
        attached.set_cancel_reason(attached_reason.clone());

        attached_cx.set_native_cx(attached.clone());

        assert_eq!(
            attached_cx.native_cancel_provenance(),
            Some(NativeCancellationProvenance::Exact(attached_reason.clone()))
        );
        assert_eq!(attached.cancel_reason(), Some(attached_reason));

        let relay_cx = Cx::<FullCaps>::new();
        relay_cx.cancel_with_reason(CancelReason::Timeout);
        let relay = NativeCx::<native_cap::None>::detached_cancel_context();
        let relay_reason =
            NativeCancelReason::deadline().with_message("preexisting relay deadline");
        relay.set_cancel_reason(relay_reason.clone());

        relay_cx.set_native_cancel_relay(relay.clone());

        assert_eq!(
            relay_cx.native_cancel_provenance(),
            Some(NativeCancellationProvenance::Exact(relay_reason.clone()))
        );
        assert_eq!(relay.cancel_reason(), Some(relay_reason));
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_native_shutdown_maps_to_strongest_local_reason_without_feedback() {
        let cx = Cx::<FullCaps>::new();
        let native = NativeCx::for_testing();
        let expected = NativeCancelReason::shutdown().with_message("runtime shutdown");
        cx.set_native_cx(native.clone());
        native.set_cancel_reason(expected.clone());

        let err = cx.checkpoint().unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Cancelled);
        assert_eq!(cx.cancel_reason(), Some(CancelReason::Abort));
        assert_eq!(
            cx.native_cancel_provenance(),
            Some(NativeCancellationProvenance::Exact(expected.clone()))
        );
        assert_eq!(
            native.cancel_reason(),
            Some(expected),
            "lossy project mapping must never be written back to the native context"
        );
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_reasonless_external_native_signal_is_explicit_not_timeout() {
        let cx = Cx::<FullCaps>::new();
        let native = NativeCx::for_testing();
        cx.set_native_cx(native.clone());
        native.set_cancel_requested(true);

        let err = cx.checkpoint().unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Cancelled);
        assert_eq!(cx.cancel_reason(), Some(CancelReason::NativeSignal));
        assert_eq!(
            cx.native_cancel_provenance(),
            Some(NativeCancellationProvenance::SignalWithoutReason)
        );
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_native_provenance_strengthens_deterministically_across_observations() {
        fn observe_in_order(
            first: NativeCancelReason,
            second: NativeCancelReason,
        ) -> NativeCancellationProvenance {
            let cx = Cx::<FullCaps>::new();
            let native = NativeCx::for_testing();
            cx.set_native_cx(native.clone());

            native.set_cancel_reason(first);
            let _ = cx.checkpoint();
            native.set_cancel_reason(second);
            let _ = cx.checkpoint();

            cx.native_cancel_provenance()
                .expect("native cancellation must retain provenance")
        }

        let user = NativeCancelReason::user("operator request");
        let shutdown = NativeCancelReason::shutdown().with_message("runtime shutdown");
        let forward = observe_in_order(user.clone(), shutdown.clone());
        let reverse = observe_in_order(shutdown.clone(), user);

        assert_eq!(
            forward,
            NativeCancellationProvenance::Exact(shutdown.clone())
        );
        assert_eq!(reverse, NativeCancellationProvenance::Exact(shutdown));
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_cx_checkpoint_native_cx_respects_local_masking() {
        let cx = Cx::<FullCaps>::new();
        let native = NativeCx::for_testing();
        cx.set_native_cx(native.clone());
        native.set_cancel_reason(NativeCancelReason::user("cancel"));

        {
            let _mask = cx.masked();
            assert!(cx.checkpoint().is_ok());
            assert!(cx.is_cancel_requested());
            assert_eq!(cx.cancel_state(), CancelState::CancelRequested);
        }

        let err = cx.checkpoint().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Cancelled);
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_cx_effective_native_cx_uses_fallback_without_marking_explicit_attachment() {
        let cx = Cx::<FullCaps>::with_budget(Budget::INFINITE.with_priority(7));

        assert!(cx.attached_native_cx().is_none());
        let native = cx.effective_native_cx();
        assert!(cx.attached_native_cx().is_none());
        assert!(native.checkpoint().is_ok());
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_cx_checkpoint_without_native_context_does_not_create_fallback() {
        let cx = Cx::<FullCaps>::new();

        assert!(cx.inner.fallback_native_cx.get().is_none());
        assert!(cx.checkpoint().is_ok());
        assert!(cx.inner.fallback_native_cx.get().is_none());
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_cx_set_native_cx_replaces_fallback_context() {
        let cx = Cx::<FullCaps>::new();
        let _ = cx.effective_native_cx();

        let replacement = NativeCx::for_testing();
        cx.set_native_cx(replacement.clone());
        replacement.set_cancel_reason(NativeCancelReason::timeout());

        let err = cx.checkpoint().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Cancelled);
        assert_eq!(cx.cancel_reason(), Some(CancelReason::Timeout));
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_create_child_copies_preexisting_cancellation_into_fallback_native_cx() {
        let parent = Cx::<FullCaps>::new();
        parent.cancel_with_reason(CancelReason::RegionClose);

        let child = parent.create_child();
        let native = child.effective_native_cx();
        assert!(
            native.checkpoint().is_err(),
            "fallback native cx must observe inherited project cancellation"
        );
        assert!(
            native.cancel_reason().is_none(),
            "fallback wake signaling must not fabricate native attribution"
        );
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_create_child_inherits_explicit_native_cx_attachment() {
        let parent = Cx::<FullCaps>::new();
        let native = NativeCx::for_testing();
        parent.set_native_cx(native.clone());

        let child = parent.create_child();
        assert!(child.attached_native_cx().is_some());

        native.set_cancel_reason(NativeCancelReason::timeout());
        let err = child
            .checkpoint()
            .expect_err("child should observe inherited native cancel");
        assert_eq!(err.kind(), ErrorKind::Cancelled);
        assert_eq!(child.cancel_reason(), Some(CancelReason::Timeout));
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_native_free_child_never_copies_task_affine_native_context() {
        let parent = Cx::<FullCaps>::new();
        let native = NativeCx::for_testing();
        parent.set_native_cx(native);

        let child = parent.create_native_free_child();

        assert!(parent.attached_native_cx().is_some());
        assert!(
            child.attached_native_cx().is_none(),
            "native-free construction must not copy and then clear a task-affine handle"
        );
        parent.cancel_with_reason(CancelReason::RegionClose);
        assert!(
            child.checkpoint().is_err(),
            "native-free children must retain project cancellation lineage"
        );
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_native_cancel_relay_stays_capability_free_and_closes_install_race() {
        let before_install = Cx::<FullCaps>::new();
        before_install.cancel_with_reason(CancelReason::Timeout);
        let before_relay = NativeCx::<native_cap::None>::detached_cancel_context();
        before_install.set_native_cancel_relay(before_relay.clone());
        assert!(
            before_relay.checkpoint().is_err(),
            "installation must recheck cancellation that happened before publication"
        );

        let after_install = Cx::<FullCaps>::new();
        let after_relay = NativeCx::<native_cap::None>::detached_cancel_context();
        after_install.set_native_cancel_relay(after_relay.clone());
        assert!(after_relay.checkpoint().is_ok());
        after_install.cancel_with_reason(CancelReason::RegionClose);
        assert!(
            after_relay.checkpoint().is_err(),
            "later project cancellation must wake the capability-free relay"
        );
        assert!(
            after_install.attached_native_cx().is_none(),
            "a cancellation relay must never become attached runtime authority"
        );
    }

    #[test]
    fn test_operation_cancellation_is_one_way_and_source_drop_is_authoritative() {
        let caller = Cx::<FullCaps>::new();
        let engine_root = Cx::<FullCaps>::new();
        let (source_a, token_a) = caller.operation_cancellation();
        let (mut source_b, token_b) = caller.operation_cancellation();
        let engine_a = engine_root.create_child_linked_to_operation(&token_a);
        let engine_b = engine_root.create_child_linked_to_operation(&token_b);

        drop(source_a);

        assert!(token_a.checkpoint().is_err());
        assert!(
            engine_a.checkpoint().is_err(),
            "a linked engine checkpoint must observe source Drop"
        );
        assert!(
            token_b.checkpoint().is_ok() && engine_b.checkpoint().is_ok(),
            "cancelling one operation must not poison a sibling"
        );
        assert!(
            caller.checkpoint().is_ok() && engine_root.checkpoint().is_ok(),
            "operation cancellation must not propagate into either root"
        );

        source_b.disarm();
        drop(source_b);
        assert!(
            token_b.checkpoint().is_ok(),
            "terminal success must disarm the source instead of overwriting it with cancellation"
        );
    }

    #[test]
    fn test_operation_cancellation_request_is_distinct_from_checkpoint_observation() {
        let caller = Cx::<FullCaps>::new();
        let (source, token) = caller.operation_cancellation();

        source.cancel();

        assert!(token.is_cancel_requested());
        assert!(
            !token.cancellation_was_observed(),
            "request publication alone must not claim that engine work observed cancellation"
        );
        assert!(token.checkpoint().is_err());
        assert!(
            token.cancellation_was_observed(),
            "the exact token checkpoint must publish cancellation observation"
        );
    }

    #[test]
    fn test_linked_engine_checkpoint_marks_exact_operation_observed() {
        let caller = Cx::<FullCaps>::new();
        let engine_root = Cx::<FullCaps>::new();
        let (source, token) = caller.operation_cancellation();
        let engine = engine_root.create_child_linked_to_operation(&token);

        source.cancel_with_reason(CancelReason::Timeout);

        assert!(!token.cancellation_was_observed());
        assert!(engine.checkpoint().is_err());
        assert!(
            token.cancellation_was_observed(),
            "an engine checkpoint linked to the token must mark the exact source observed"
        );
        assert!(
            engine_root.checkpoint().is_ok(),
            "operation observation must not contaminate the engine root"
        );
    }

    #[test]
    fn test_explicit_operation_timeout_survives_armed_source_drop() {
        let caller = Cx::<FullCaps>::new();
        let engine_root = Cx::<FullCaps>::new();
        let (source, token) = caller.operation_cancellation();
        let engine = engine_root.create_child_linked_to_operation(&token);

        source.cancel_with_reason(CancelReason::Timeout);
        drop(source);

        assert_eq!(token.cancel_reason(), Some(CancelReason::Timeout));
        assert_eq!(engine.cancel_reason(), Some(CancelReason::Timeout));
        assert!(engine.checkpoint().is_err());
    }

    #[test]
    fn test_operation_token_does_not_retain_caller_eprocess_oracle() {
        let caller = Cx::<FullCaps>::new();
        caller.set_eprocess_oracle(Arc::new(EProcessOracle::new(
            EProcessConfig {
                p0: 0.1,
                lambda: 5.0,
                alpha: 0.05,
                max_evalue: 1e12,
            },
            1,
        )));

        let (_source, token) = caller.operation_cancellation();

        assert!(
            token.inner.eprocess_oracle.get().is_none(),
            "an operation token must retain only cancellation state and scalar metadata"
        );
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_operation_token_inherits_exact_native_cancel_provenance() {
        let caller = Cx::<FullCaps>::new();
        let native = NativeCx::for_testing();
        caller.set_native_cx(native.clone());
        let (_source, token) = caller.operation_cancellation();
        let expected = NativeCancelReason::shutdown()
            .with_message("runtime shutdown")
            .with_cause(NativeCancelReason::deadline());

        native.set_cancel_reason(expected.clone());
        let _ = caller.checkpoint();

        assert_eq!(token.cancel_reason(), Some(CancelReason::Abort));
        assert_eq!(
            token.native_cancel_provenance(),
            Some(NativeCancellationProvenance::Exact(expected))
        );
    }

    #[test]
    fn test_operation_source_forwards_exact_project_cancel_reason() {
        let operation_owner = Cx::<FullCaps>::new();
        let (source, token) = operation_owner.operation_cancellation();
        let caller = Cx::<FullCaps>::new();
        caller.cancel_with_reason(CancelReason::RegionClose);

        source.cancel_from_cx(&caller);

        assert_eq!(token.cancel_reason(), Some(CancelReason::RegionClose));
        assert!(token.checkpoint().is_err());
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_operation_source_forwards_exact_native_reason_and_provenance() {
        let operation_owner = Cx::<FullCaps>::new();
        let (source, token) = operation_owner.operation_cancellation();
        let native = NativeCx::for_testing();
        let expected = NativeCancelReason::shutdown()
            .with_message("dedicated worker shutdown")
            .with_cause(NativeCancelReason::cost_budget());
        native.set_cancel_reason(expected.clone());

        source.cancel_from_native_cx(&native);

        assert_eq!(token.cancel_reason(), Some(CancelReason::Abort));
        assert_eq!(
            token.native_cancel_provenance(),
            Some(NativeCancellationProvenance::Exact(expected))
        );
        assert!(token.checkpoint().is_err());
    }

    #[test]
    fn test_context_and_operation_cancellation_handles_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}

        assert_send_sync::<Cx<FullCaps>>();
        assert_send_sync::<OperationCancellationSource>();
        assert_send_sync::<OperationCancellationToken>();
    }

    #[test]
    fn test_operation_source_drop_cancels_across_threads() {
        let caller = Cx::<FullCaps>::new();
        let (source, token) = caller.operation_cancellation();

        std::thread::spawn(move || drop(source))
            .join()
            .expect("cross-thread source Drop must complete");

        assert_eq!(token.cancel_reason(), Some(CancelReason::UserInterrupt));
        assert!(token.checkpoint().is_err());
    }

    #[test]
    fn test_operation_link_then_recheck_closes_preexisting_cancel_race() {
        let caller = Cx::<FullCaps>::new();
        let engine_root = Cx::<FullCaps>::new();
        let (source, token) = caller.operation_cancellation();
        source.cancel_with_reason(CancelReason::Timeout);

        let engine = engine_root.create_child_linked_to_operation(&token);

        assert_eq!(token.cancel_reason(), Some(CancelReason::Timeout));
        assert!(
            engine.checkpoint().is_err(),
            "a source cancelled before link publication must be observed by the first checkpoint"
        );
        assert_eq!(engine.cancel_reason(), Some(CancelReason::Timeout));
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_surviving_descendant_retains_stronger_operation_reason_and_provenance() {
        let caller = Cx::<FullCaps>::new();
        let native = NativeCx::for_testing();
        caller.set_native_cx(native.clone());
        let (_source, token) = caller.operation_cancellation();
        let engine_root = Cx::<FullCaps>::new();
        let engine = engine_root.create_child_linked_to_operation(&token);
        let descendant = engine.create_child();
        drop(engine);

        descendant.cancel_with_reason(CancelReason::UserInterrupt);
        let expected = NativeCancelReason::shutdown()
            .with_message("operation runtime shutdown")
            .with_cause(NativeCancelReason::deadline());
        native.set_cancel_reason(expected.clone());
        assert!(caller.checkpoint().is_err());

        assert!(descendant.checkpoint().is_err());
        assert_eq!(token.cancel_reason(), Some(CancelReason::Abort));
        assert_eq!(descendant.cancel_reason(), Some(CancelReason::Abort));
        assert_eq!(
            descendant.native_cancel_provenance(),
            Some(NativeCancellationProvenance::Exact(expected))
        );
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_nested_operation_token_survives_dropped_operation_link() {
        let caller = Cx::<FullCaps>::new();
        let (outer_source, outer_token) = caller.operation_cancellation();
        let engine_root = Cx::<FullCaps>::new();
        let intermediate = engine_root.create_child_linked_to_operation(&outer_token);
        let (mut nested_source, nested_token) = intermediate.operation_cancellation();
        drop(intermediate);
        let expected = NativeCancelReason::shutdown()
            .with_message("outer operation shutdown")
            .with_cause(NativeCancelReason::cost_budget());

        propagate_native_cancel_provenance(
            &outer_source.inner,
            NativeCancellationProvenance::Exact(expected.clone()),
        );
        outer_source.cancel_with_reason(CancelReason::Abort);

        assert_eq!(nested_token.cancel_reason(), Some(CancelReason::Abort));
        assert_eq!(
            nested_token.native_cancel_provenance(),
            Some(NativeCancellationProvenance::Exact(expected))
        );
        nested_source.disarm();
    }

    #[test]
    fn test_linked_child_retains_outer_and_supplied_operation_sources() {
        let outer_caller = Cx::<FullCaps>::new();
        let (outer_source, outer_token) = outer_caller.operation_cancellation();
        let engine_root = Cx::<FullCaps>::new();
        let operation_root = engine_root.create_child_linked_to_operation(&outer_token);
        let inner_caller = Cx::<FullCaps>::new();
        let (inner_source, inner_token) = inner_caller.operation_cancellation();
        let child = operation_root.create_child_linked_to_operation(&inner_token);
        drop(operation_root);

        outer_source.cancel_with_reason(CancelReason::Timeout);
        assert_eq!(child.cancel_reason(), Some(CancelReason::Timeout));

        inner_source.cancel_with_reason(CancelReason::Abort);
        assert_eq!(child.cancel_reason(), Some(CancelReason::Abort));
        assert!(engine_root.checkpoint().is_ok());
        assert!(outer_caller.checkpoint().is_ok());
        assert!(inner_caller.checkpoint().is_ok());
    }

    #[test]
    fn test_operation_token_inherits_later_parent_cancel_without_poisoning_root() {
        let caller = Cx::<FullCaps>::new();
        let engine_root = Cx::<FullCaps>::new();
        let (_source, token) = caller.operation_cancellation();
        let engine = engine_root.create_child_linked_to_operation(&token);

        caller.cancel_with_reason(CancelReason::RegionClose);

        assert!(token.checkpoint().is_err());
        assert!(engine.checkpoint().is_err());
        assert!(
            engine_root.checkpoint().is_ok(),
            "caller cancellation must not poison the connection root"
        );
    }

    #[test]
    fn test_operation_metadata_zero_preserves_engine_lineage_and_decision() {
        let caller = Cx::<FullCaps>::new().with_trace_context(0, 0, 0);
        let engine_root = Cx::<FullCaps>::new().with_trace_context(71, 72, 73);
        let (_source, token) = caller.operation_cancellation();

        let engine = engine_root.create_child_linked_to_operation(&token);

        assert_eq!(engine.trace_id(), 71);
        assert_eq!(engine.policy_id(), 73);
        assert_eq!(
            engine.decision_id(),
            72,
            "caller decision metadata must never cross the actor boundary"
        );
    }

    #[test]
    fn test_operation_metadata_nonzero_trace_and_policy_override_engine_lineage() {
        let caller = Cx::<FullCaps>::new().with_trace_context(81, 8_001, 83);
        let engine_root = Cx::<FullCaps>::new().with_trace_context(91, 92, 93);
        let (_source, token) = caller.operation_cancellation();

        let engine = engine_root.create_child_linked_to_operation(&token);

        assert_eq!(engine.trace_id(), 81);
        assert_eq!(engine.policy_id(), 83);
        assert_eq!(
            engine.decision_id(),
            92,
            "fresh engine decision allocation happens after metadata merge; caller decision is ignored"
        );
    }

    #[test]
    fn test_operation_fallback_trace_keeps_direct_engine_root_lineage() {
        let caller = Cx::<FullCaps>::new().with_trace_context(0, 0, 0);
        let engine_root = Cx::<FullCaps>::new().with_trace_context(91, 92, 93);
        let (_source, token) = caller.operation_cancellation();
        let engine = engine_root.create_child_linked_to_operation_with_fallback_trace(&token, 101);

        assert_eq!(engine.trace_id(), 101);
        assert_eq!(engine.decision_id(), 92);
        assert_eq!(engine.policy_id(), 93);

        engine_root.cancel_with_reason(CancelReason::RegionClose);
        assert_eq!(engine.cancel_reason(), Some(CancelReason::RegionClose));
    }

    #[test]
    fn test_operation_caller_trace_wins_over_fallback_trace() {
        let caller = Cx::<FullCaps>::new().with_trace_context(81, 8_001, 83);
        let engine_root = Cx::<FullCaps>::new().with_trace_context(91, 92, 93);
        let (_source, token) = caller.operation_cancellation();
        let engine = engine_root.create_child_linked_to_operation_with_fallback_trace(&token, 101);

        assert_eq!(engine.trace_id(), 81);
        assert_eq!(engine.decision_id(), 92);
        assert_eq!(engine.policy_id(), 83);
    }

    #[test]
    fn test_operation_budget_meets_engine_deadline_quotas_and_priority() {
        let caller_budget = Budget::INFINITE
            .with_deadline(Duration::from_millis(25))
            .with_poll_quota(40)
            .with_cost_quota(400)
            .with_priority(3);
        let engine_budget = Budget::INFINITE
            .with_deadline(Duration::from_millis(10))
            .with_poll_quota(80)
            .with_cost_quota(200)
            .with_priority(7);
        let caller = Cx::<FullCaps>::with_budget(caller_budget);
        let engine_root = Cx::<FullCaps>::with_budget(engine_budget);
        let (_source, token) = caller.operation_cancellation();

        let engine = engine_root.create_child_linked_to_operation(&token);

        assert_eq!(
            engine.budget(),
            Budget {
                deadline: Some(Duration::from_millis(10)),
                poll_quota: 40,
                cost_quota: Some(200),
                priority: 7,
            }
        );
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_operation_cancel_immediately_marks_and_wakes_linked_engine_child() {
        let caller = Cx::<FullCaps>::new();
        let engine_root = Cx::<FullCaps>::new();
        let (source, token) = caller.operation_cancellation();
        let engine = engine_root.create_child_linked_to_operation(&token);
        let native_relay = NativeCx::<native_cap::None>::detached_cancel_context();
        engine.set_native_cancel_relay(native_relay.clone());

        source.cancel_with_reason(CancelReason::Timeout);

        assert!(
            engine.is_cancel_requested(),
            "the source weak link must mark the engine child without a checkpoint poll"
        );
        assert_eq!(engine.cancel_reason(), Some(CancelReason::Timeout));
        assert!(
            native_relay.checkpoint().is_err(),
            "linked cancellation must wake the child's cancellation-only native waiter"
        );
        assert!(
            engine_root.checkpoint().is_ok(),
            "the engine root must remain independent from one operation"
        );
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_linked_operation_never_shares_native_cancel_state_with_root_or_sibling() {
        let caller = Cx::<FullCaps>::new();
        let engine_root = Cx::<FullCaps>::new();
        let root_native = NativeCx::for_testing();
        engine_root.set_native_cx(root_native.clone());
        let (source_a, token_a) = caller.operation_cancellation();
        let (_source_b, token_b) = caller.operation_cancellation();
        let engine_a = engine_root.create_child_linked_to_operation(&token_a);
        let engine_b = engine_root.create_child_linked_to_operation(&token_b);

        assert!(engine_a.attached_native_cx().is_none());
        assert!(engine_b.attached_native_cx().is_none());
        source_a.cancel();

        assert!(engine_a.is_cancel_requested());
        assert!(!engine_b.is_cancel_requested());
        assert!(root_native.checkpoint().is_ok());
        assert!(engine_root.checkpoint().is_ok());
        assert!(engine_b.checkpoint().is_ok());
    }

    fn nested_operation_diamonds(
        depth: usize,
    ) -> (
        Cx<FullCaps>,
        Vec<Cx<FullCaps>>,
        Vec<OperationCancellationToken>,
    ) {
        let root = Cx::<FullCaps>::new();
        let mut current = root.clone();
        let mut engine_nodes = vec![root.clone()];
        let mut operation_nodes = Vec::with_capacity(depth);

        for _ in 0..depth {
            let (mut source, operation) = current.operation_cancellation();
            source.disarm();
            let next = current.create_child_linked_to_operation(&operation);
            operation_nodes.push(operation);
            engine_nodes.push(next.clone());
            current = next;
        }

        (root, engine_nodes, operation_nodes)
    }

    fn assert_accepted_cancel_propagations(
        engine_nodes: &[Cx<FullCaps>],
        operation_nodes: &[OperationCancellationToken],
        expected: u32,
    ) {
        for (depth, node) in engine_nodes.iter().enumerate() {
            assert_eq!(
                node.inner
                    .accepted_cancel_propagations
                    .load(Ordering::Relaxed),
                expected,
                "engine node at depth {depth} accepted an unexpected number of propagations"
            );
        }
        for (depth, operation) in operation_nodes.iter().enumerate() {
            assert_eq!(
                operation
                    .inner
                    .accepted_cancel_propagations
                    .load(Ordering::Relaxed),
                expected,
                "operation node at depth {depth} accepted an unexpected number of propagations"
            );
        }
    }

    #[test]
    fn test_nested_operation_diamonds_equal_and_weaker_cancel_do_not_revisit() {
        let (root, engine_nodes, operation_nodes) = nested_operation_diamonds(12);

        root.cancel_with_reason(CancelReason::RegionClose);
        assert_accepted_cancel_propagations(&engine_nodes, &operation_nodes, 1);

        root.cancel_with_reason(CancelReason::RegionClose);
        root.cancel_with_reason(CancelReason::Timeout);
        assert_accepted_cancel_propagations(&engine_nodes, &operation_nodes, 1);
    }

    #[test]
    fn test_nested_operation_diamonds_stronger_cancel_propagates_once() {
        let (root, engine_nodes, operation_nodes) = nested_operation_diamonds(12);

        root.cancel_with_reason(CancelReason::Timeout);
        root.cancel_with_reason(CancelReason::Abort);
        assert_accepted_cancel_propagations(&engine_nodes, &operation_nodes, 2);
        assert!(
            engine_nodes
                .iter()
                .all(|node| node.cancel_reason() == Some(CancelReason::Abort))
        );
        assert!(
            operation_nodes
                .iter()
                .all(|operation| operation.cancel_reason() == Some(CancelReason::Abort))
        );

        root.cancel_with_reason(CancelReason::Abort);
        root.cancel_with_reason(CancelReason::RegionClose);
        assert_accepted_cancel_propagations(&engine_nodes, &operation_nodes, 2);
    }

    #[test]
    fn test_dead_operation_and_admission_children_remain_bounded() {
        let caller = Cx::<FullCaps>::new();

        // Model a permanently full mailbox: every attempt creates one opaque
        // operation token and one native-free admission child, then the
        // pending future is dropped before capacity ever becomes available.
        for _ in 0..4_096 {
            let (source, token) = caller.operation_cancellation();
            let admission = caller.create_native_free_child();
            drop(admission);
            drop(token);
            drop(source);
        }

        let slots = caller
            .inner
            .children
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        assert!(
            slots <= usize::try_from(CHILD_LINK_PRUNE_INTERVAL).unwrap_or(usize::MAX) * 2,
            "amortized pruning must bound dead child links, found {slots}"
        );
    }

    #[test]
    fn test_deep_cancellation_chain_uses_bounded_native_stack() {
        let worker = std::thread::Builder::new()
            .name("cx-deep-cancel".to_owned())
            .stack_size(256 * 1024)
            .spawn(|| {
                const DEPTH: usize = 10_000;

                // Keep every intermediate child alive so the root really has
                // to traverse the entire parent-to-child chain.
                let mut chain = Vec::with_capacity(DEPTH + 1);
                chain.push(Cx::<FullCaps>::new());
                for _ in 0..DEPTH {
                    let child = chain
                        .last()
                        .expect("the chain always contains its root")
                        .create_child();
                    chain.push(child);
                }

                chain[0].cancel_with_reason(CancelReason::Abort);
                let leaf = chain.last().expect("the chain contains its leaf");
                assert!(leaf.is_cancel_requested());
                assert_eq!(leaf.cancel_reason(), Some(CancelReason::Abort));
                assert!(leaf.checkpoint().is_err());
            })
            .expect("small-stack cancellation worker should spawn");

        worker
            .join()
            .expect("iterative cancellation must not overflow a small stack");
    }

    #[test]
    fn test_budget_infinite_is_identity_for_meet() {
        let budget = Budget {
            deadline: Some(Duration::from_millis(42)),
            poll_quota: 500,
            cost_quota: Some(1000),
            priority: 7,
        };
        assert_eq!(budget.meet(Budget::INFINITE), budget);
        assert_eq!(Budget::INFINITE.meet(budget), budget);
    }

    #[test]
    fn test_budget_none_constraints_propagate() {
        let a = Budget {
            deadline: None,
            poll_quota: u32::MAX,
            cost_quota: None,
            priority: 0,
        };
        let b = Budget {
            deadline: Some(Duration::from_millis(50)),
            poll_quota: 100,
            cost_quota: Some(500),
            priority: 3,
        };
        let m = a.meet(b);
        assert_eq!(m.deadline, Some(Duration::from_millis(50)));
        assert_eq!(m.poll_quota, 100);
        assert_eq!(m.cost_quota, Some(500));
        assert_eq!(m.priority, 3);
    }

    #[test]
    fn test_cx_scope_budget_chains() {
        let cx = Cx::<FullCaps>::with_budget(
            Budget::INFINITE
                .with_deadline(Duration::from_millis(100))
                .with_poll_quota(1000),
        );
        // First scope tightens deadline.
        let s1 = cx.scope_with_budget(Budget::INFINITE.with_deadline(Duration::from_millis(50)));
        assert_eq!(s1.budget().deadline, Some(Duration::from_millis(50)));
        assert_eq!(s1.budget().poll_quota, 1000);

        // Second scope tightens poll_quota further.
        let s2 = s1.scope_with_budget(Budget::INFINITE.with_poll_quota(200));
        assert_eq!(s2.budget().deadline, Some(Duration::from_millis(50)));
        assert_eq!(s2.budget().poll_quota, 200);
    }

    fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                collect_rs_files(&path, out)?;
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
        Ok(())
    }

    fn scan_file_outside_cfg_test_items(src: &str, patterns: &[&str]) -> Vec<(usize, String)> {
        let mut hits = Vec::new();

        let mut brace_depth: i32 = 0;
        let mut pending_cfg_test = false;
        let mut pending_attr_paren_depth: i32 = 0;
        let mut skip_until_depth: Option<i32> = None;

        for (idx, line) in src.lines().enumerate() {
            let trimmed = line.trim_start();
            let paren_delta = i32::try_from(line.matches('(').count()).unwrap_or(i32::MAX)
                - i32::try_from(line.matches(')').count()).unwrap_or(i32::MAX);

            if skip_until_depth.is_none() {
                // Handle single-line `#[cfg(test)]` items that open a block immediately.
                if trimmed.starts_with("#[cfg(test)]") && trimmed.contains('{') {
                    pending_cfg_test = false;
                    pending_attr_paren_depth = 0;
                    skip_until_depth = Some(brace_depth);
                } else if trimmed.contains("fn test_") && trimmed.contains('{') {
                    skip_until_depth = Some(brace_depth);
                } else if trimmed.starts_with("#[cfg(test)]") {
                    pending_cfg_test = true;
                    pending_attr_paren_depth = 0;
                } else if pending_cfg_test {
                    // Allow additional attributes/blank lines before the gated item.
                    if trimmed.starts_with("#[") || pending_attr_paren_depth > 0 {
                        pending_attr_paren_depth =
                            pending_attr_paren_depth.saturating_add(paren_delta);
                    } else if trimmed.is_empty() || trimmed.starts_with("//") {
                        // keep pending
                    } else if trimmed.contains('{') {
                        pending_cfg_test = false;
                        pending_attr_paren_depth = 0;
                        skip_until_depth = Some(brace_depth);
                    } else {
                        pending_cfg_test = false;
                        pending_attr_paren_depth = 0;
                    }
                } else {
                    for &pat in patterns {
                        if line.contains(pat) {
                            hits.push((idx + 1, pat.to_string()));
                        }
                    }
                }
            }

            // Update brace depth (coarse; sufficient for `#[cfg(test)] mod ... {}` blocks).
            let opens = i32::try_from(line.matches('{').count()).unwrap_or(i32::MAX);
            let closes = i32::try_from(line.matches('}').count()).unwrap_or(i32::MAX);
            brace_depth = brace_depth.saturating_add(opens).saturating_sub(closes);

            if let Some(until) = skip_until_depth {
                if brace_depth <= until {
                    skip_until_depth = None;
                }
            }
        }

        hits
    }

    #[test]
    fn test_scan_file_outside_cfg_test_items_skips_cfg_test_functions_and_modules() {
        let src = r"
fn production_path() {
    let _ = Cx::new();
}

#[cfg(test)]
fn test_only_helper() {
    let _ = Cx::new();
}

#[cfg(test)]
mod tests {
    fn nested_test_helper() {
        let _ = Cx::default();
    }
}
";

        let hits = scan_file_outside_cfg_test_items(src, &["Cx::new(", "Cx::default("]);
        assert_eq!(hits, vec![(3, "Cx::new(".to_string())]);
    }

    #[test]
    fn test_no_direct_cx_constructors_in_runtime_production_code() {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let repo_root = manifest_dir
            .parent()
            .and_then(Path::parent)
            .expect("fsqlite-types manifest dir must be crates/<name>");
        let crates_dir = repo_root.join("crates");
        let runtime_crates = [
            "fsqlite-core",
            "fsqlite-vdbe",
            "fsqlite-btree",
            "fsqlite-pager",
            "fsqlite-wal",
            "fsqlite-mvcc",
        ];
        let forbidden = ["Cx::new(", "Cx::default("];

        let mut violations: Vec<String> = Vec::new();
        let mut crate_dirs: Vec<PathBuf> = Vec::new();
        for entry in std::fs::read_dir(&crates_dir).expect("read crates/ dir") {
            let entry = entry.expect("read crates/ entry");
            let path = entry.path();
            if path.is_dir() {
                crate_dirs.push(path);
            }
        }

        for crate_dir in crate_dirs {
            let crate_name = crate_dir
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("<unknown>");
            if !runtime_crates.contains(&crate_name) {
                continue;
            }

            let src_dir = crate_dir.join("src");
            if !src_dir.is_dir() {
                continue;
            }

            let mut files = Vec::new();
            collect_rs_files(&src_dir, &mut files).expect("collect rs files");

            for file in files {
                if file
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.contains("test"))
                {
                    continue;
                }

                let src = std::fs::read_to_string(&file).expect("read file");
                let rel_path = file.strip_prefix(repo_root).unwrap_or(&file);

                for (line, pat) in scan_file_outside_cfg_test_items(&src, &forbidden) {
                    let line_text = src.lines().nth(line - 1).unwrap_or("").trim();
                    let allowed_detached_root_constructor = rel_path
                        == Path::new("crates/fsqlite-core/src/connection.rs")
                        && pat == "Cx::new("
                        && line_text.contains("Cx::new().with_trace_context(");

                    if allowed_detached_root_constructor {
                        continue;
                    }

                    violations.push(format!(
                        "{crate_name}:{path}:{line} uses forbidden `{pat}` outside cfg(test) code: {line_text}",
                        path = rel_path.display()
                    ));
                }
            }
        }

        assert!(
            violations.is_empty(),
            "direct `Cx::new()` / `Cx::default()` production-path violations:\n{}",
            violations.join("\n")
        );
    }

    #[test]
    fn test_ambient_authority_audit_gate() {
        // Scan `crates/*/src/**/*.rs` for ambient-authority usage, excluding
        // `#[cfg(test)]`-gated items.
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let repo_root = manifest_dir
            .parent()
            .and_then(Path::parent)
            .expect("fsqlite-types manifest dir must be crates/<name>");
        let crates_dir = repo_root.join("crates");

        // Always forbidden everywhere (outside cfg(test) modules).
        let always_forbidden = [
            "SystemTime::now(",
            "Instant::now(",
            "thread_rng(",
            "getrandom",
            "std::net::",
            "std::thread::spawn",
            "tokio::spawn",
        ];

        // Forbidden outside VFS boundary (outside cfg(test) modules).
        let non_vfs_forbidden = ["std::fs::"];

        // Crates exempt from ambient-authority scanning:
        // - test infrastructure (harness, cli, e2e)
        // - observability (pure diagnostics, needs Instant::now for timing)
        // - core (needs std::fs for WAL bootstrap/MVCC key, Instant::now for tracing)
        // - vdbe (needs std::fs for sorter temp files, Instant::now for tracing)
        // - mvcc (Instant::now in flat_combining/rcu for latency metrics)
        // - parser (Instant::now for lexer span timing)
        // - planner (Instant::now for access-path selection, SystemTime for contracts)
        // - wal (Instant::now for checkpoint timing)
        // - vfs (Instant::now for VFS operation metrics, std::fs allowed by design)
        // - types (the Cx clock primitive itself: wall_clock_now_since_epoch for
        //   native deadline conversion — the one place real time enters Cx)
        // - func (SQL date/time functions are wall-clock by definition:
        //   datetime('now'), unixepoch(), strftime('now', ...))
        // - fsqlite (migration busy-retry timeout: Instant::now bounds the
        //   SQLITE_BUSY retry loop in apply_one)
        // - btree (B-tree cursor/instrumentation latency metrics)
        // - c-api (FFI boundary, like vfs: the C ABI shim does C-style time/file
        //   ops and carries its own local unsafe_code override)
        // - pager (pager/page-cache latency metrics + shared_file_state_key
        //   canonicalize; the one control-flow time use — eviction shard-probe
        //   start — was replaced with a deterministic round-robin, bd-w4yc9)
        //
        // The gate still guards the extension crates (fts3/fts5/rtree/json/
        // session/icu/misc), ast, error, and wasm, where ambient authority must
        // not appear. bd-w4yc9.
        let exempt_crates = [
            "fsqlite-harness",
            "fsqlite-cli",
            "fsqlite-e2e",
            "fsqlite-observability",
            "fsqlite-core",
            "fsqlite-vdbe",
            "fsqlite-mvcc",
            "fsqlite-parser",
            "fsqlite-planner",
            "fsqlite-wal",
            "fsqlite-vfs",
            "fsqlite-types",
            "fsqlite-func",
            "fsqlite",
            "fsqlite-btree",
            "fsqlite-c-api",
            "fsqlite-pager",
        ];

        let mut violations: Vec<String> = Vec::new();
        let mut crate_dirs: Vec<PathBuf> = Vec::new();
        for entry in std::fs::read_dir(&crates_dir).expect("read crates/ dir") {
            let entry = entry.expect("read crates/ entry");
            let path = entry.path();
            if path.is_dir() {
                crate_dirs.push(path);
            }
        }

        for crate_dir in crate_dirs {
            let crate_name = crate_dir
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("<unknown>");
            if exempt_crates.contains(&crate_name) {
                continue;
            }
            let src_dir = crate_dir.join("src");
            if !src_dir.is_dir() {
                continue;
            }

            let mut files = Vec::new();
            collect_rs_files(&src_dir, &mut files).expect("collect rs files");

            for file in files {
                let src = std::fs::read_to_string(&file).expect("read file");
                for (line, pat) in scan_file_outside_cfg_test_items(&src, &always_forbidden) {
                    violations.push(format!(
                        "{crate_name}:{path}:{line} uses forbidden `{pat}`",
                        path = file.display()
                    ));
                }

                if crate_name != "fsqlite-vfs" {
                    for (line, pat) in scan_file_outside_cfg_test_items(&src, &non_vfs_forbidden) {
                        violations.push(format!(
                            "{crate_name}:{path}:{line} uses forbidden `{pat}` (non-vfs crate)",
                            path = file.display()
                        ));
                    }
                }
            }
        }

        assert!(
            violations.is_empty(),
            "ambient authority violations (outside cfg(test) modules):\n{}",
            violations.join("\n")
        );
    }

    // ===================================================================
    // §4.12 Cancellation Protocol Tests (bd-samf)
    // ===================================================================

    const BEAD_ID: &str = "bd-samf";

    #[test]
    fn test_cancel_state_machine_all_transitions() {
        // Test 1: State machine transitions through all 6 states.
        let cx = Cx::<FullCaps>::new();
        assert_eq!(
            cx.cancel_state(),
            CancelState::Created,
            "bead_id={BEAD_ID} initial_state"
        );

        cx.transition_to_running();
        assert_eq!(
            cx.cancel_state(),
            CancelState::Running,
            "bead_id={BEAD_ID} after_start"
        );

        cx.cancel_with_reason(CancelReason::UserInterrupt);
        assert_eq!(
            cx.cancel_state(),
            CancelState::CancelRequested,
            "bead_id={BEAD_ID} after_cancel"
        );

        // Observing cancellation via checkpoint transitions to Cancelling.
        let err = cx.checkpoint();
        assert!(err.is_err(), "bead_id={BEAD_ID} checkpoint_returns_err");
        assert_eq!(
            cx.cancel_state(),
            CancelState::Cancelling,
            "bead_id={BEAD_ID} after_checkpoint_observation"
        );

        cx.transition_to_finalizing();
        assert_eq!(
            cx.cancel_state(),
            CancelState::Finalizing,
            "bead_id={BEAD_ID} after_finalize_start"
        );

        cx.transition_to_completed();
        assert_eq!(
            cx.cancel_state(),
            CancelState::Completed,
            "bead_id={BEAD_ID} after_complete"
        );
    }

    #[test]
    fn test_cancel_propagates_to_children() {
        // Test 2: Cancel propagates to 3 children within one call.
        let parent = Cx::<FullCaps>::new();
        parent.transition_to_running();

        let child1 = parent.create_child();
        child1.transition_to_running();
        let child2 = parent.create_child();
        child2.transition_to_running();
        let child3 = parent.create_child();
        child3.transition_to_running();

        assert!(!child1.is_cancel_requested());
        assert!(!child2.is_cancel_requested());
        assert!(!child3.is_cancel_requested());

        parent.cancel_with_reason(CancelReason::RegionClose);

        // All children must see cancellation (INV-CANCEL-PROPAGATES).
        assert!(
            child1.is_cancel_requested(),
            "bead_id={BEAD_ID} child1_cancelled"
        );
        assert!(
            child2.is_cancel_requested(),
            "bead_id={BEAD_ID} child2_cancelled"
        );
        assert!(
            child3.is_cancel_requested(),
            "bead_id={BEAD_ID} child3_cancelled"
        );

        // Children must be in CancelRequested state.
        assert_eq!(child1.cancel_state(), CancelState::CancelRequested);
        assert_eq!(child2.cancel_state(), CancelState::CancelRequested);
        assert_eq!(child3.cancel_state(), CancelState::CancelRequested);

        // Reason must propagate.
        assert_eq!(child1.cancel_reason(), Some(CancelReason::RegionClose));
    }

    #[test]
    fn test_dropped_children_are_pruned_from_parent_links() {
        let parent = Cx::<FullCaps>::new();

        let live_child = parent.create_child();
        let dropped_child = parent.create_child();
        drop(dropped_child);

        // Trigger propagation pass, which prunes dead weak child links.
        parent.cancel_with_reason(CancelReason::RegionClose);

        let live_count = {
            let children = parent
                .inner
                .children
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            children.iter().filter_map(Weak::upgrade).count()
        };
        assert_eq!(live_count, 1, "only the live child should remain linked");
        assert!(live_child.is_cancel_requested());
    }

    #[test]
    fn test_cancel_idempotent_strongest_wins() {
        // Test 3: Strongest cancel reason wins, cannot get weaker.
        let cx = Cx::<FullCaps>::new();
        cx.transition_to_running();

        cx.cancel_with_reason(CancelReason::Timeout);
        assert_eq!(
            cx.cancel_reason(),
            Some(CancelReason::Timeout),
            "bead_id={BEAD_ID} first_reason"
        );

        // Stronger reason upgrades.
        cx.cancel_with_reason(CancelReason::Abort);
        assert_eq!(
            cx.cancel_reason(),
            Some(CancelReason::Abort),
            "bead_id={BEAD_ID} upgraded_reason"
        );

        // Weaker reason does NOT downgrade.
        cx.cancel_with_reason(CancelReason::UserInterrupt);
        assert_eq!(
            cx.cancel_reason(),
            Some(CancelReason::Abort),
            "bead_id={BEAD_ID} reason_stays_strongest"
        );
    }

    #[test]
    fn test_losers_drain_on_race() {
        // Test 4: Simulate race combinator — loser with obligation resolves
        // before race returns.
        use std::sync::atomic::AtomicBool;

        let loser_cx = Cx::<FullCaps>::new();
        loser_cx.transition_to_running();

        // Simulate an obligation on the loser.
        let obligation_resolved = Arc::new(AtomicBool::new(false));
        let ob_clone = Arc::clone(&obligation_resolved);

        // Winner finishes → cancel loser.
        loser_cx.cancel_with_reason(CancelReason::RegionClose);

        // Loser observes cancellation at next checkpoint.
        assert!(loser_cx.checkpoint().is_err());
        assert_eq!(loser_cx.cancel_state(), CancelState::Cancelling);

        // Loser drains: resolves obligation.
        ob_clone.store(true, Ordering::Release);
        loser_cx.transition_to_finalizing();
        loser_cx.transition_to_completed();

        assert!(
            obligation_resolved.load(Ordering::Acquire),
            "bead_id={BEAD_ID} loser_obligation_resolved"
        );
        assert_eq!(
            loser_cx.cancel_state(),
            CancelState::Completed,
            "bead_id={BEAD_ID} loser_drained"
        );
    }

    #[test]
    fn test_vdbe_checkpoint_cancel_observed_at_next_opcode() {
        // Test 5: Simulate VDBE opcode loop — cancel after opcode 50,
        // observed at opcode 51.
        let cx = Cx::<FullCaps>::new();
        cx.transition_to_running();

        let mut last_executed = 0u32;
        for opcode in 0..100u32 {
            // Checkpoint at start of each opcode.
            if cx.checkpoint_with(format!("vdbe pc={opcode}")).is_err() {
                last_executed = opcode;
                break;
            }
            // Execute opcode.
            last_executed = opcode;
            // Cancel arrives at end of opcode 50.
            if opcode == 50 {
                cx.cancel_with_reason(CancelReason::UserInterrupt);
            }
        }

        assert_eq!(
            last_executed, 51,
            "bead_id={BEAD_ID} cancel_observed_at_opcode_51"
        );
    }

    #[test]
    fn test_btree_checkpoint_cancel_within_one_node() {
        // Test 6: Simulate B-tree descent — cancel mid-descent, observed
        // within 1 node visit.
        let cx = Cx::<FullCaps>::new();
        cx.transition_to_running();

        let nodes = ["root", "internal_l", "internal_r", "leaf_a", "leaf_b"];
        let cancel_at = 2; // Cancel after visiting internal_r.
        let mut observed_at = None;

        for (i, node) in nodes.iter().enumerate() {
            // Checkpoint at start of each node visit.
            if cx.checkpoint_with(format!("btree node={node}")).is_err() {
                observed_at = Some(i);
                break;
            }
            // Visit node.
            // Cancel arrives after visiting node at index cancel_at.
            if i == cancel_at {
                cx.cancel_with_reason(CancelReason::UserInterrupt);
            }
        }

        assert_eq!(
            observed_at,
            Some(cancel_at + 1),
            "bead_id={BEAD_ID} btree_cancel_within_one_node"
        );
    }

    #[test]
    fn test_masked_section_defers_cancel() {
        // Test 7: Masked section defers cancel — checkpoint returns Ok inside
        // mask, Err after exit.
        let cx = Cx::<FullCaps>::new();
        cx.transition_to_running();

        cx.cancel_with_reason(CancelReason::UserInterrupt);
        assert!(cx.is_cancel_requested());

        // Enter masked section.
        {
            let _guard = cx.masked();
            assert_eq!(cx.mask_depth(), 1);

            // Inside mask, checkpoint succeeds despite cancellation.
            assert!(
                cx.checkpoint().is_ok(),
                "bead_id={BEAD_ID} checkpoint_ok_while_masked"
            );

            // Nested mask.
            {
                let _inner = cx.masked();
                assert_eq!(cx.mask_depth(), 2);
                assert!(cx.checkpoint().is_ok());
            }
            assert_eq!(cx.mask_depth(), 1);
        }
        assert_eq!(cx.mask_depth(), 0);

        // After mask exit, checkpoint observes cancellation.
        assert!(
            cx.checkpoint().is_err(),
            "bead_id={BEAD_ID} checkpoint_err_after_mask_exit"
        );
    }

    #[test]
    #[should_panic(expected = "MAX_MASK_DEPTH")]
    #[allow(clippy::collection_is_never_read)]
    fn test_max_mask_depth_exceeded_panics() {
        // Test 8: MAX_MASK_DEPTH=64 exceeded panics in lab mode.
        let cx = Cx::<FullCaps>::new();
        let mut guards = Vec::new();
        for _ in 0..MAX_MASK_DEPTH {
            guards.push(cx.masked());
        }
        // This 65th mask should panic.
        let _overflow = cx.masked();
    }

    #[test]
    fn test_commit_section_completes_under_cancel() {
        // Test 9: Cancel after op 1 of 3, all 3 complete + finalizers run.
        let cx = Cx::<FullCaps>::new();
        cx.transition_to_running();

        let ops_completed = Arc::new(AtomicU32::new(0));
        let finalizer_ran = Arc::new(AtomicBool::new(false));

        let ops = Arc::clone(&ops_completed);
        let fin = Arc::clone(&finalizer_ran);

        cx.commit_section(
            10,
            |ctx| {
                // Op 1.
                assert!(ctx.tick());
                ops.fetch_add(1, Ordering::Release);

                // Cancel mid-section.
                cx.cancel_with_reason(CancelReason::UserInterrupt);

                // Op 2: still succeeds because commit section is masked.
                assert!(ctx.tick());
                ops.fetch_add(1, Ordering::Release);
                assert!(
                    cx.checkpoint().is_ok(),
                    "bead_id={BEAD_ID} masked_during_commit"
                );

                // Op 3.
                assert!(ctx.tick());
                ops.fetch_add(1, Ordering::Release);
            },
            move || {
                fin.store(true, Ordering::Release);
            },
        );

        assert_eq!(
            ops_completed.load(Ordering::Acquire),
            3,
            "bead_id={BEAD_ID} all_ops_completed"
        );
        assert!(
            finalizer_ran.load(Ordering::Acquire),
            "bead_id={BEAD_ID} finalizer_ran"
        );

        // After commit section, masking is removed — checkpoint should fail.
        assert!(cx.checkpoint().is_err());
    }

    #[test]
    fn test_commit_section_enforces_poll_quota() {
        // Test 10: Commit section poll quota is bounded.
        let cx = Cx::<FullCaps>::new();
        cx.transition_to_running();

        let ticks_succeeded = Arc::new(AtomicU32::new(0));
        let ts = Arc::clone(&ticks_succeeded);

        cx.commit_section(
            3,
            |ctx| {
                assert_eq!(ctx.poll_remaining(), 3);
                for _ in 0..5 {
                    if ctx.tick() {
                        ts.fetch_add(1, Ordering::Release);
                    }
                }
            },
            || {},
        );

        assert_eq!(
            ticks_succeeded.load(Ordering::Acquire),
            3,
            "bead_id={BEAD_ID} poll_quota_enforced"
        );
    }

    #[test]
    fn test_cancel_unaware_hot_loop_detected() {
        // Test 11: Simulate harness detecting a hot loop that never
        // calls checkpoint.
        let cx = Cx::<FullCaps>::new();
        cx.transition_to_running();

        // Harness deadline: if 100 iterations pass without checkpoint,
        // the loop is cancel-unaware.
        let deadline = 100u32;
        let mut iterations_without_checkpoint = 0u32;
        let mut detected_unaware = false;

        cx.cancel_with_reason(CancelReason::UserInterrupt);

        for _i in 0..200u32 {
            iterations_without_checkpoint += 1;
            if iterations_without_checkpoint >= deadline {
                detected_unaware = true;
                break;
            }
            // Bug: no cx.checkpoint() call in the loop body.
        }

        assert!(
            detected_unaware,
            "bead_id={BEAD_ID} cancel_unaware_loop_detected"
        );

        // Contrast: a compliant loop would checkpoint and exit.
        let cx2 = Cx::<FullCaps>::new();
        cx2.transition_to_running();
        cx2.cancel_with_reason(CancelReason::UserInterrupt);
        let mut compliant_iters = 0u32;
        for _ in 0..200u32 {
            if cx2.checkpoint().is_err() {
                break;
            }
            compliant_iters += 1;
        }
        assert_eq!(
            compliant_iters, 0,
            "bead_id={BEAD_ID} compliant_loop_exits_immediately"
        );
    }

    #[test]
    fn test_write_coordinator_commit_section() {
        // Test 12: Simulate WriteCoordinator — cancel mid-publish,
        // proof+marker completes atomically via commit section.
        let cx = Cx::<FullCaps>::new();
        cx.transition_to_running();

        let proof_published = Arc::new(AtomicBool::new(false));
        let marker_published = Arc::new(AtomicBool::new(false));
        let reservation_released = Arc::new(AtomicBool::new(false));

        let proof = Arc::clone(&proof_published);
        let marker = Arc::clone(&marker_published);
        let release = Arc::clone(&reservation_released);

        cx.commit_section(
            10,
            |ctx| {
                // Step 1: FCW validation passed, commit_seq allocated.
                assert!(ctx.tick());

                // Cancel arrives mid-publish.
                cx.cancel_with_reason(CancelReason::RegionClose);

                // Step 2: Publish proof (must complete).
                assert!(ctx.tick());
                proof.store(true, Ordering::Release);
                // Checkpoint inside commit section succeeds (masked).
                assert!(cx.checkpoint().is_ok());

                // Step 3: Publish marker (must complete).
                assert!(ctx.tick());
                marker.store(true, Ordering::Release);
            },
            move || {
                // Finalizer: release reservation.
                release.store(true, Ordering::Release);
            },
        );

        assert!(
            proof_published.load(Ordering::Acquire),
            "bead_id={BEAD_ID} proof_published"
        );
        assert!(
            marker_published.load(Ordering::Acquire),
            "bead_id={BEAD_ID} marker_published"
        );
        assert!(
            reservation_released.load(Ordering::Acquire),
            "bead_id={BEAD_ID} reservation_released"
        );

        // After commit section, cancellation is visible.
        assert!(cx.checkpoint().is_err());
    }

    // ===================================================================
    // Tracing ID propagation tests (bd-2g5.6)
    // ===================================================================

    #[test]
    fn test_trace_ids_default_to_zero() {
        let cx = Cx::<FullCaps>::new();
        assert_eq!(cx.trace_id(), 0);
        assert_eq!(cx.decision_id(), 0);
        assert_eq!(cx.policy_id(), 0);
    }

    #[test]
    fn test_with_trace_context_sets_all_ids() {
        let cx = Cx::<FullCaps>::new().with_trace_context(42, 99, 7);
        assert_eq!(cx.trace_id(), 42);
        assert_eq!(cx.decision_id(), 99);
        assert_eq!(cx.policy_id(), 7);
    }

    #[test]
    fn test_with_decision_id_preserves_other_ids() {
        let cx = Cx::<FullCaps>::new()
            .with_trace_context(10, 20, 30)
            .with_decision_id(55);
        assert_eq!(cx.trace_id(), 10);
        assert_eq!(cx.decision_id(), 55);
        assert_eq!(cx.policy_id(), 30);
    }

    #[test]
    fn test_with_policy_id_preserves_other_ids() {
        let cx = Cx::<FullCaps>::new()
            .with_trace_context(100, 200, 300)
            .with_policy_id(88);
        assert_eq!(cx.trace_id(), 100);
        assert_eq!(cx.decision_id(), 200);
        assert_eq!(cx.policy_id(), 88);
    }

    #[test]
    #[allow(clippy::redundant_clone)]
    fn test_clone_propagates_trace_ids() {
        let cx = Cx::<FullCaps>::new().with_trace_context(1, 2, 3);
        let cloned = cx.clone();
        assert_eq!(cloned.trace_id(), 1);
        assert_eq!(cloned.decision_id(), 2);
        assert_eq!(cloned.policy_id(), 3);
    }

    #[test]
    fn test_restrict_propagates_trace_ids() {
        let cx = Cx::<FullCaps>::new();
        let compute = cx.restrict::<ComputeCaps>();
        assert_eq!(compute.trace_id(), 0);
        assert_eq!(compute.decision_id(), 0);
        assert_eq!(compute.policy_id(), 0);
    }

    #[test]
    fn test_scope_with_budget_propagates_trace_ids() {
        let cx = Cx::<FullCaps>::new().with_trace_context(5, 6, 7);
        let scoped = cx.scope_with_budget(Budget::MINIMAL);
        assert_eq!(scoped.trace_id(), 5);
        assert_eq!(scoped.decision_id(), 6);
        assert_eq!(scoped.policy_id(), 7);
        // Budget should be tightened.
        assert_eq!(scoped.budget().poll_quota, Budget::MINIMAL.poll_quota);
    }

    #[test]
    fn test_cleanup_scope_propagates_trace_ids() {
        let cx = Cx::<FullCaps>::new().with_trace_context(11, 22, 33);
        let cleanup = cx.cleanup_scope();
        assert_eq!(cleanup.trace_id(), 11);
        assert_eq!(cleanup.decision_id(), 22);
        assert_eq!(cleanup.policy_id(), 33);
    }

    #[test]
    fn test_create_child_propagates_trace_ids() {
        let parent = Cx::<FullCaps>::new().with_trace_context(50, 60, 70);
        let child = parent.create_child();
        assert_eq!(child.trace_id(), 50);
        assert_eq!(child.decision_id(), 60);
        assert_eq!(child.policy_id(), 70);
        // Child should have independent cancellation.
        parent.cancel();
        assert!(parent.is_cancel_requested());
        assert!(child.is_cancel_requested()); // Propagated.
    }

    #[test]
    fn test_trace_ids_independent_across_children() {
        let parent = Cx::<FullCaps>::new().with_trace_context(1, 2, 3);
        let child1 = parent.create_child().with_decision_id(100);
        let child2 = parent.create_child().with_decision_id(200);
        // Children share trace_id but have different decision_ids.
        assert_eq!(child1.trace_id(), 1);
        assert_eq!(child2.trace_id(), 1);
        assert_eq!(child1.decision_id(), 100);
        assert_eq!(child2.decision_id(), 200);
        // Parent's decision_id unchanged.
        assert_eq!(parent.decision_id(), 2);
    }

    #[test]
    fn test_with_budget_starts_at_zero_trace_ids() {
        let cx = Cx::<FullCaps>::with_budget(Budget::MINIMAL);
        assert_eq!(cx.trace_id(), 0);
        assert_eq!(cx.decision_id(), 0);
        assert_eq!(cx.policy_id(), 0);
    }
}
