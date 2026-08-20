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

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
#[cfg(feature = "native")]
use std::sync::atomic::AtomicU8;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context as TaskContext, Poll, Waker};
use std::time::Duration;

use crate::sync_primitives::SystemTime;

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
    Timeout = 0,
    UserInterrupt = 1,
    RegionClose = 2,
    Abort = 3,
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
    /// Relative timeout translated into the receiving runtime's clock domain
    /// when work crosses a native async boundary.
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

#[derive(Debug)]
struct LocalCancelWaiter {
    id: u64,
    waker: Waker,
}

#[derive(Debug, Default)]
struct LocalCancelWaiters {
    next_id: u64,
    entries: Vec<LocalCancelWaiter>,
}

impl LocalCancelWaiters {
    fn allocate_id(&mut self) -> u64 {
        loop {
            let id = self.next_id;
            self.next_id = self.next_id.wrapping_add(1);
            if self.entries.iter().all(|waiter| waiter.id != id) {
                return id;
            }
        }
    }

    fn remove(&mut self, id: u64) -> Option<LocalCancelWaiter> {
        self.entries
            .iter()
            .position(|waiter| waiter.id == id)
            .map(|position| self.entries.swap_remove(position))
    }
}

#[derive(Debug)]
struct CxInner {
    cancel_requested: AtomicBool,
    // Strongest ordinary cancellation for this local node. Kept separate
    // from `cancel_reason` so operation-local cancellation is never mirrored
    // into native I/O. NativeCx clones remain upstream-owned overwrite handles;
    // this rank intentionally does not claim cross-sibling arbitration.
    #[cfg(feature = "native")]
    native_cancel_reason: AtomicU8,
    cancel_state: Mutex<CancelState>,
    cancel_reason: Mutex<Option<CancelReason>>,
    mask_depth: AtomicU32,
    cancel_dispatch_gate: Arc<Mutex<()>>,
    local_cancel_waiters: Mutex<LocalCancelWaiters>,
    children: Mutex<Vec<Weak<Self>>>,
    last_checkpoint_msg: Mutex<Option<String>>,
    last_eprocess_decision: Mutex<Option<EProcessDecision>>,
    eprocess_oracle: std::sync::OnceLock<Arc<EProcessOracle>>,
    #[cfg(feature = "native")]
    attached_native_cx: Mutex<Option<NativeCx>>,
    #[cfg(feature = "native")]
    fallback_native_cx: std::sync::OnceLock<NativeCx>,
    // bd-bjm5d: set only on the root Cx of a dedicated engine OS thread
    // that owns its Connection exclusively and is not a shared scheduler
    // worker. Grants VFS backends permission to issue bounded EINTR-safe
    // positional I/O inline instead of hopping to the blocking pool.
    // Deliberately NOT feature-gated: this is an OS-thread-ownership
    // property, not an asupersync property.
    blocking_io_inline_safe: AtomicBool,
    // Deterministic clock override: milliseconds since epoch for tests.
    //
    // The mode bit is separate from the value so every `u64`, including zero
    // and `u64::MAX`, remains a valid fixed timestamp. Writers publish the
    // value before the mode bit with release ordering; readers acquire the
    // mode before loading the value.
    unix_millis: AtomicU64,
    unix_millis_is_fixed: AtomicBool,
}

impl CxInner {
    fn new(cancel_dispatch_gate: Arc<Mutex<()>>) -> Self {
        Self {
            cancel_requested: AtomicBool::new(false),
            #[cfg(feature = "native")]
            native_cancel_reason: AtomicU8::new(0),
            cancel_state: Mutex::new(CancelState::Created),
            cancel_reason: Mutex::new(None),
            mask_depth: AtomicU32::new(0),
            cancel_dispatch_gate,
            local_cancel_waiters: Mutex::new(LocalCancelWaiters::default()),
            children: Mutex::new(Vec::new()),
            last_checkpoint_msg: Mutex::new(None),
            last_eprocess_decision: Mutex::new(None),
            eprocess_oracle: std::sync::OnceLock::new(),
            #[cfg(feature = "native")]
            attached_native_cx: Mutex::new(None),
            #[cfg(feature = "native")]
            fallback_native_cx: std::sync::OnceLock::new(),
            blocking_io_inline_safe: AtomicBool::new(false),
            unix_millis: AtomicU64::new(0),
            unix_millis_is_fixed: AtomicBool::new(false),
        }
    }
}

#[cfg(feature = "native")]
#[must_use]
fn local_reason_to_native(reason: CancelReason) -> NativeCancelReason {
    match reason {
        CancelReason::Timeout => NativeCancelReason::timeout(),
        CancelReason::UserInterrupt => NativeCancelReason::user("sqlite interrupt"),
        CancelReason::RegionClose => NativeCancelReason::parent_cancelled(),
        CancelReason::Abort => NativeCancelReason::resource_unavailable(),
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
        | NativeCancelKind::Shutdown
        | NativeCancelKind::LinkedExit => CancelReason::RegionClose,
        NativeCancelKind::ResourceUnavailable => CancelReason::Abort,
    }
}

#[cfg(feature = "native")]
const fn encode_native_cancel_reason(reason: CancelReason) -> u8 {
    match reason {
        CancelReason::Timeout => 1,
        CancelReason::UserInterrupt => 2,
        CancelReason::RegionClose => 3,
        CancelReason::Abort => 4,
    }
}

#[cfg(feature = "native")]
fn decode_native_cancel_reason(encoded: u8) -> Option<CancelReason> {
    match encoded {
        0 => None,
        1 => Some(CancelReason::Timeout),
        2 => Some(CancelReason::UserInterrupt),
        3 => Some(CancelReason::RegionClose),
        4 => Some(CancelReason::Abort),
        _ => unreachable!("invalid native cancellation reason rank"),
    }
}

#[cfg(feature = "native")]
fn record_native_cancel_reason(inner: &CxInner, reason: CancelReason) {
    inner
        .native_cancel_reason
        .fetch_max(encode_native_cancel_reason(reason), Ordering::AcqRel);
}

#[cfg(feature = "native")]
fn mirrored_native_cancel_reason(inner: &CxInner) -> Option<CancelReason> {
    decode_native_cancel_reason(inner.native_cancel_reason.load(Ordering::Acquire))
}

#[cfg(feature = "native")]
fn sync_one_native_cx_cancel(inner: &CxInner, native: &NativeCx) {
    let mut encoded = inner.native_cancel_reason.load(Ordering::Acquire);
    while let Some(reason) = decode_native_cancel_reason(encoded) {
        // NativeCx drops its own lock before invoking arbitrary wakers. Hold
        // no FrankenSQLite lock across this callback boundary.
        native.set_cancel_reason(local_reason_to_native(reason));
        let latest = inner.native_cancel_reason.load(Ordering::Acquire);
        if latest == encoded {
            break;
        }
        encoded = latest;
    }
}

#[cfg(feature = "native")]
#[must_use]
#[allow(dead_code)]
fn native_budget_from_local_at(budget: Budget, now: NativeTime) -> NativeBudget {
    let mut native_budget = NativeBudget::new()
        .with_poll_quota(budget.poll_quota)
        .with_priority(budget.priority);
    if let Some(cost_quota) = budget.cost_quota {
        native_budget = native_budget.with_cost_quota(cost_quota);
    }
    if let Some(timeout) = budget.deadline {
        native_budget = native_budget.with_timeout(now, timeout);
    }
    native_budget
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativeCancelPropagation {
    LocalAndNative,
    LocalOnly,
}

#[must_use]
fn local_cancellation_matches(inner: &CxInner, respect_mask: bool) -> bool {
    inner.cancel_requested.load(Ordering::Acquire)
        && (!respect_mask || inner.mask_depth.load(Ordering::Acquire) == 0)
}

fn take_local_cancel_waiters(inner: &CxInner) -> Vec<LocalCancelWaiter> {
    let mut waiters = inner
        .local_cancel_waiters
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    std::mem::take(&mut waiters.entries)
}

fn capture_cancel_callback_panic(
    first_panic: &mut Option<Box<dyn std::any::Any + Send>>,
    callback: impl FnOnce(),
) {
    if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback)) {
        if first_panic.is_none() {
            *first_panic = Some(payload);
        } else {
            // A panic payload may itself panic when dropped. The first panic
            // is the one we report; leaking a secondary payload is preferable
            // to aborting before the remaining cancellation observers run.
            std::mem::forget(payload);
        }
    }
}

fn dispatch_local_cancel_waiters(
    waiters: Vec<LocalCancelWaiter>,
    first_panic: &mut Option<Box<dyn std::any::Any + Send>>,
) {
    // A Waker callback or final destructor may run arbitrary executor code.
    // Both therefore happen only after the FrankenSQLite mutex is released.
    // One panicking callback must not prevent the remaining observers from
    // being notified.
    for waiter in waiters {
        capture_cancel_callback_panic(first_panic, || waiter.waker.wake_by_ref());
        capture_cancel_callback_panic(first_panic, || drop(waiter));
    }
}

fn publish_cancel_state(
    inner: &CxInner,
    local_reason: CancelReason,
    native_reason: Option<CancelReason>,
) -> CancelReason {
    #[cfg(not(feature = "native"))]
    let _ = native_reason;

    // Monotone reason update.
    let effective_local_reason = {
        let mut r = inner
            .cancel_reason
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match *r {
            Some(existing) if existing >= local_reason => existing,
            _ => {
                *r = Some(local_reason);
                local_reason
            }
        }
    };

    // State transition: Created/Running → CancelRequested.
    {
        let mut state = inner
            .cancel_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(*state, CancelState::Created | CancelState::Running) {
            *state = CancelState::CancelRequested;
        }
    }

    // Record only cancellation requests that are allowed to cross the native
    // boundary. This reason is deliberately separate from the aggregate local
    // reason: a stronger operation-local cancellation must never be laundered
    // into a shared native context by a later, weaker ordinary cancellation.
    #[cfg(feature = "native")]
    if let Some(reason) = native_reason {
        record_native_cancel_reason(inner, reason);
    }

    // Publish the fast-path flag only after the reason and state are visible.
    // This gives attachment and child-registration rechecks a stable reason to
    // consume once they observe cancellation.
    inner.cancel_requested.store(true, Ordering::Release);

    effective_local_reason
}

fn try_append_live_children(
    inner: &CxInner,
    descendants: &mut Vec<Arc<CxInner>>,
) -> std::result::Result<(), std::collections::TryReserveError> {
    let mut children = inner
        .children
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    children.retain(|child| child.strong_count() > 0);
    descendants.try_reserve(children.len())?;
    for child in children.iter().filter_map(Weak::upgrade) {
        descendants.push(child);
    }
    Ok(())
}

#[must_use]
fn local_cancel_waiter_count(inner: &CxInner) -> usize {
    inner
        .local_cancel_waiters
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .entries
        .len()
}

fn drain_local_cancel_waiters_into(inner: &CxInner, waiters: &mut Vec<LocalCancelWaiter>) {
    let mut registered_waiters = inner
        .local_cancel_waiters
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    debug_assert!(
        waiters.capacity() - waiters.len() >= registered_waiters.entries.len(),
        "cancellation waiter capacity must be reserved before publication"
    );
    waiters.append(&mut registered_waiters.entries);
}

#[cfg(feature = "native")]
struct NativeCancelTarget {
    descendant_index: Option<usize>,
    native_cx: NativeCx,
}

#[cfg(feature = "native")]
fn append_native_cancel_targets(
    inner: &CxInner,
    descendant_index: Option<usize>,
    targets: &mut Vec<NativeCancelTarget>,
) {
    let attached_native = inner
        .attached_native_cx
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_ref()
        .cloned();
    if let Some(native_cx) = attached_native {
        targets.push(NativeCancelTarget {
            descendant_index,
            native_cx,
        });
    }

    if let Some(native_cx) = inner.fallback_native_cx.get().cloned() {
        targets.push(NativeCancelTarget {
            descendant_index,
            native_cx,
        });
    }
}

/// Propagate cancellation to a `CxInner` node and all its descendants.
///
/// The explicit worklist avoids consuming one physical stack frame per
/// descendant. Each node lock is released before its children are visited.
fn propagate_cancel_tree(
    inner: &CxInner,
    reason: CancelReason,
    native_propagation: NativeCancelPropagation,
) {
    let mut descendants = Vec::new();
    let mut waiters = Vec::new();
    #[cfg(feature = "native")]
    let mut native_targets = Vec::new();
    let mut reserve_error = None;
    {
        // Every context in one parent/child family shares this phase gate.
        // Only state publication and waiter extraction happen while it is
        // held; arbitrary native and executor callbacks run after unlock.
        let _dispatch = inner
            .cancel_dispatch_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let native_reason = match native_propagation {
            NativeCancelPropagation::LocalAndNative => Some(reason),
            NativeCancelPropagation::LocalOnly => None,
        };

        if let Err(error) = try_append_live_children(inner, &mut descendants) {
            reserve_error = Some(error);
        }
        let mut cursor = 0;
        while reserve_error.is_none() && cursor < descendants.len() {
            let node = Arc::clone(&descendants[cursor]);
            if let Err(error) = try_append_live_children(&node, &mut descendants) {
                reserve_error = Some(error);
                break;
            }
            cursor += 1;
        }

        if reserve_error.is_none() {
            // Pending registration takes the same family gate. Concurrent
            // unregistration can only make this count smaller, so one
            // aggregate reservation covers every later append.
            let waiter_count = descendants
                .iter()
                .fold(local_cancel_waiter_count(inner), |count, node| {
                    count.saturating_add(local_cancel_waiter_count(node))
                });
            if let Err(error) = waiters.try_reserve(waiter_count) {
                reserve_error = Some(error);
            }
        }

        #[cfg(feature = "native")]
        if reserve_error.is_none() && native_propagation == NativeCancelPropagation::LocalAndNative
        {
            let target_capacity = descendants.len().saturating_mul(2).saturating_add(2);
            if let Err(error) = native_targets.try_reserve(target_capacity) {
                reserve_error = Some(error);
            } else {
                append_native_cancel_targets(inner, None, &mut native_targets);
                for (index, node) in descendants.iter().enumerate() {
                    append_native_cancel_targets(node, Some(index), &mut native_targets);
                }
            }
        }

        if reserve_error.is_none() {
            let propagated_local_reason = publish_cancel_state(inner, reason, native_reason);
            drain_local_cancel_waiters_into(inner, &mut waiters);

            #[cfg(feature = "native")]
            let propagated_native_reason = match native_propagation {
                NativeCancelPropagation::LocalAndNative => mirrored_native_cancel_reason(inner),
                NativeCancelPropagation::LocalOnly => None,
            };
            #[cfg(not(feature = "native"))]
            let propagated_native_reason = None;

            for node in &descendants {
                publish_cancel_state(node, propagated_local_reason, propagated_native_reason);
                drain_local_cancel_waiters_into(node, &mut waiters);
            }
        }
    }

    // Every node visible when this phase began has its cancellation flag
    // published before this batch invokes native or executor callbacks.
    // Reasons are monotone per node; concurrent stronger batches may advance
    // them further after this phase releases the family gate.
    if let Some(error) = reserve_error {
        panic!("failed to reserve cancellation propagation storage: {error}");
    }

    let mut first_panic = None;
    #[cfg(feature = "native")]
    for target in &native_targets {
        let target_inner = target
            .descendant_index
            .map_or(inner, |index| &descendants[index]);
        capture_cancel_callback_panic(&mut first_panic, || {
            sync_one_native_cx_cancel(target_inner, &target.native_cx);
        });
    }
    dispatch_local_cancel_waiters(waiters, &mut first_panic);
    if let Some(payload) = first_panic {
        std::panic::resume_unwind(payload);
    }
}

fn propagate_cancel(inner: &CxInner, reason: CancelReason) {
    propagate_cancel_tree(inner, reason, NativeCancelPropagation::LocalAndNative);
}

fn propagate_local_cancel(inner: &CxInner, reason: CancelReason) {
    propagate_cancel_tree(inner, reason, NativeCancelPropagation::LocalOnly);
}

/// Cancellation-only authority for one derived [`Cx`] subtree.
///
/// This relay deliberately carries only a weak reference to the local
/// FrankenSQLite cancellation state. It cannot expose the target context,
/// runtime effects, I/O authority, budgets, or native asupersync context.
/// Local cancellation reaches the target and its descendants without
/// cancelling an attached native context that may be shared with the worker
/// root. Calling the relay performs synchronous local bookkeeping and subtree
/// traversal; actor integrations must not invoke it from a nonblocking `Drop`.
#[derive(Debug)]
#[must_use = "dropping the relay discards the authority to cancel the derived operation"]
pub struct LocalCancelRelay {
    inner: Weak<CxInner>,
}

impl LocalCancelRelay {
    /// Request cancellation of the derived local context subtree.
    ///
    /// The strongest [`CancelReason`] remains monotone. Returns `false` when
    /// the target context has already been dropped; otherwise returns `true`,
    /// including for an idempotent repeated request.
    #[must_use]
    pub fn cancel_local(&self, reason: CancelReason) -> bool {
        let Some(inner) = self.inner.upgrade() else {
            return false;
        };
        propagate_local_cancel(&inner, reason);
        true
    }
}

/// Scoped notification of local cancellation for one [`Cx`].
///
/// This future watches only FrankenSQLite's local cancellation state. It does
/// not inspect or modify a native runtime context, carry effect capabilities,
/// or create a child context. The constructor determines whether masking
/// defers readiness or whether the raw request itself is sufficient.
///
/// A pending registration is removed on `Drop`, so repeatedly creating and
/// abandoning these futures does not accumulate stale child or waker entries.
#[derive(Debug)]
#[must_use = "futures do nothing unless polled or awaited"]
pub struct LocalCancellation<'a> {
    inner: &'a CxInner,
    waiter_id: Option<u64>,
    respect_mask: bool,
}

impl LocalCancellation<'_> {
    fn unregister(&mut self) {
        let Some(id) = self.waiter_id.take() else {
            return;
        };
        let retired = {
            let mut waiters = self
                .inner
                .local_cancel_waiters
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            waiters.remove(id)
        };
        // Releasing the final Waker reference may invoke executor code.
        drop(retired);
    }
}

impl Future for LocalCancellation<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, task_cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let mut prepared_waker = None;
        let this = self.as_mut().get_mut();
        let inner = this.inner;

        loop {
            if local_cancellation_matches(inner, this.respect_mask) {
                this.unregister();
                drop(prepared_waker);
                return Poll::Ready(());
            }

            // Registration participates in the same family phase gate as
            // tree publication. This lets cancellation pre-reserve one flat
            // waiter buffer for the complete tree before it changes any
            // node. Waker cloning and destruction still happen after both
            // internal locks are released.
            let dispatch = inner
                .cancel_dispatch_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut waiters = inner
                .local_cancel_waiters
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if local_cancellation_matches(inner, this.respect_mask) {
                let retired = this.waiter_id.take().and_then(|id| waiters.remove(id));
                drop(waiters);
                drop(dispatch);
                drop(retired);
                drop(prepared_waker);
                return Poll::Ready(());
            }

            let existing_position = this.waiter_id.and_then(|id| {
                waiters
                    .entries
                    .iter()
                    .position(|registered| registered.id == id)
            });
            if existing_position
                .is_some_and(|position| waiters.entries[position].waker.will_wake(task_cx.waker()))
            {
                drop(waiters);
                drop(dispatch);
                drop(prepared_waker);
                return Poll::Pending;
            }

            if existing_position.is_none()
                && let Err(error) = waiters.entries.try_reserve(1)
            {
                drop(waiters);
                drop(dispatch);
                drop(prepared_waker);
                panic!("failed to reserve local-cancellation waiter storage: {error}");
            }

            let Some(new_waker) = prepared_waker.take() else {
                // RawWaker::clone may execute arbitrary user code, so prepare
                // the replacement only after releasing the internal mutex.
                drop(waiters);
                drop(dispatch);
                prepared_waker = Some(task_cx.waker().clone());
                continue;
            };

            let retired = if let Some(position) = existing_position {
                Some(std::mem::replace(
                    &mut waiters.entries[position].waker,
                    new_waker,
                ))
            } else {
                let id = waiters.allocate_id();
                waiters.entries.push(LocalCancelWaiter {
                    id,
                    waker: new_waker,
                });
                this.waiter_id = Some(id);
                None
            };
            drop(waiters);
            drop(dispatch);
            // Waker destruction is deliberately deferred until after every
            // internal lock is released.
            drop(retired);
            return Poll::Pending;
        }
    }
}

impl Drop for LocalCancellation<'_> {
    fn drop(&mut self) {
        self.unregister();
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

    /// Mint a fresh, fully **detached** root context for MANDATORY post-cancel
    /// cleanup (VACUUM rebind, drop-time external unlock / snapshot-restore).
    ///
    /// This is the SANCTIONED direct constructor for runtime crates. The audit
    /// gate `test_no_direct_cx_constructors_in_runtime_production_code` forbids
    /// a bare `Cx::new()` / `Cx::default()` in runtime crates precisely so this
    /// documented path is the only way to obtain a detached root context — one
    /// with its own `CxInner`, no cancel lineage, and no recorded native-cancel
    /// reason. Unlike a masked `create_child()`, such a context keeps running
    /// even when the operation `Cx` (and its attached native cx) were cancelled
    /// after publication: the db-file write lock's native fast path polls the
    /// attached native cx directly, bypassing `masked()`, so a masked child is
    /// insufficient. Callers re-attach the live task's native cx via
    /// [`Cx::set_native_cx`] and, where trace continuity matters, chain
    /// [`Cx::with_trace_context`]. See `Connection::detached_rebind_cx` and the
    /// pager drop-cleanup path (bd-gzyk1 / GH#348, VACUUM rebind).
    #[must_use]
    pub fn detached_rebind() -> Self {
        Self::new()
    }
}

impl<Caps: cap::SubsetOf<cap::All>> Cx<Caps> {
    #[cfg(all(feature = "native", test))]
    #[must_use]
    #[allow(dead_code)]
    fn effective_native_cx(&self) -> NativeCx {
        let native = {
            let _dispatch = self
                .inner
                .cancel_dispatch_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let attached_native = self
                .inner
                .attached_native_cx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .cloned();
            attached_native.unwrap_or_else(|| {
                self.inner
                    .fallback_native_cx
                    .get_or_init(|| {
                        NativeCx::for_request_with_budget(native_budget_from_local_at(
                            self.budget,
                            asupersync::time::wall_now(),
                        ))
                    })
                    .clone()
            })
        };
        // Recheck after releasing the family publication gate: NativeCx
        // dispatches arbitrary wakers synchronously, so no FrankenSQLite lock
        // may be held while mirroring the reason.
        sync_one_native_cx_cancel(&self.inner, &native);
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
        Self::with_budget_and_cancel_dispatch(budget, Arc::new(Mutex::new(())))
    }

    fn with_budget_and_cancel_dispatch(
        budget: Budget,
        cancel_dispatch_gate: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            inner: Arc::new(CxInner::new(cancel_dispatch_gate)),
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

    /// Wait until local cancellation is observable at an unmasked boundary.
    ///
    /// The returned future borrows this context's existing cancellation node;
    /// it does not allocate a child context or attach a native runtime context.
    /// Dropping it removes any pending waker registration.
    pub fn wait_for_local_cancellation(&self) -> LocalCancellation<'_> {
        LocalCancellation {
            inner: &self.inner,
            waiter_id: None,
            respect_mask: true,
        }
    }

    /// Wait until local cancellation has been requested, even while masked.
    ///
    /// This is a raw wakeup primitive for cancellation-sensitive admission
    /// machinery that separately defines whether masked operation is legal.
    /// It does not transition cancellation state or inspect a native context.
    pub fn wait_for_local_cancel_request(&self) -> LocalCancellation<'_> {
        LocalCancellation {
            inner: &self.inner,
            waiter_id: None,
            respect_mask: false,
        }
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
        let retired = {
            let _dispatch = self
                .inner
                .cancel_dispatch_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut attached = self
                .inner
                .attached_native_cx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            attached.replace(native_cx.clone())
        };
        // Synchronize the exact handle supplied by this call. Re-reading the
        // attachment here would let a racing replacement return with this
        // handle still uncancelled.
        sync_one_native_cx_cancel(&self.inner, &native_cx);
        // Releasing the final old NativeCx reference may retire cancellation
        // Wakers. Do that only after both FrankenSQLite locks are gone and the
        // replacement's synchronization postcondition has been established.
        drop(retired);
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

    /// Compute the native runtime budget for a task spawned on `native_cx`.
    ///
    /// FrankenSQLite deadlines are relative timeouts. They must be translated
    /// using the selected native context's clock so production monotonic time
    /// and deterministic lab time stay in the same domain. Meeting the result
    /// with the native parent's budget preserves every tighter parent bound,
    /// including the maximum scheduling priority.
    #[cfg(feature = "native")]
    #[must_use]
    pub fn native_spawn_budget(&self, native_cx: &NativeCx) -> NativeBudget {
        let local = native_budget_from_local_at(self.budget, native_cx.now_for_observability());
        native_cx.budget().meet(local)
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
        let retired = {
            let _dispatch = self
                .inner
                .cancel_dispatch_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.inner
                .attached_native_cx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
        };
        // NativeCx destruction may invoke executor-owned cleanup.
        drop(retired);
    }

    /// Mark this context as running on a dedicated engine OS thread where
    /// bounded blocking I/O may be issued inline (bd-bjm5d). Irreversible
    /// for the lifetime of this context; shared through [`Cx::clone`] and
    /// propagated to children by [`Cx::create_child`].
    pub fn mark_blocking_io_inline_safe(&self) {
        self.inner
            .blocking_io_inline_safe
            .store(true, Ordering::Release);
    }

    /// Whether bounded, page-sized positional I/O may be issued inline on
    /// the calling thread instead of via the blocking pool (bd-bjm5d).
    #[must_use]
    pub fn blocking_io_inline_safe(&self) -> bool {
        self.inner.blocking_io_inline_safe.load(Ordering::Acquire)
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
    #[must_use]
    fn maybe_cancel_via_native_cx(&self, masked: bool) -> bool {
        let Some(native) = self.native_cx_for_checkpoint() else {
            return false;
        };

        if masked {
            if native.is_cancel_requested() {
                let reason = native
                    .cancel_reason()
                    .as_ref()
                    .map_or(CancelReason::Timeout, native_reason_to_local);
                self.cancel_with_reason(reason);
                return true;
            }
            return false;
        }

        if native.checkpoint().is_err() {
            let reason = native
                .cancel_reason()
                .as_ref()
                .map_or(CancelReason::Timeout, native_reason_to_local);
            self.cancel_with_reason(reason);
            return true;
        }
        false
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
            // the oracle + native cx can still observe a cancel signal.
            if !self.maybe_cancel_via_eprocess() {
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
        }

        // Either cancel_requested is set locally, or one of the async plane
        // checks fired. Masked sections defer observation unconditionally.
        let masked = self.inner.mask_depth.load(Ordering::Acquire) > 0;
        if masked {
            return Ok(());
        }

        // Slow path: transition CancelRequested → Cancelling.
        {
            let mut state = self
                .inner
                .cancel_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if *state == CancelState::CancelRequested {
                *state = CancelState::Cancelling;
            }
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
        self.create_child_with_runtime_affinity(true)
    }

    /// Create a child context that may be moved into a newly spawned task.
    ///
    /// Cancellation lineage, budget, tracing metadata, e-process policy, and
    /// typed capabilities are inherited exactly as for [`Self::create_child`].
    /// Task- and OS-thread-affine state is deliberately excluded: the child
    /// starts without an attached/fallback native context and without inline
    /// blocking-I/O permission. The spawned task must attach its own native
    /// context after it starts.
    #[must_use]
    pub fn create_child_for_spawn(&self) -> Self {
        self.create_child_with_runtime_affinity(false)
    }

    /// Like [`Self::create_child_for_spawn`], but ALSO snapshots the parent's
    /// current mask depth into the child so a spawned sub-operation that is part
    /// of completing a masked section stays mask-protected — its
    /// [`Self::checkpoint`] will not abort while the caller's masked op is in
    /// flight. Use ONLY where the spawned work must finish for the caller's
    /// masked operation to complete (e.g. dirty-page-eviction writeback during a
    /// masked read); the mask is a snapshot, so the child stays masked for its
    /// own (bounded) lifetime rather than tracking the parent's later unwind.
    /// When the parent is unmasked this is identical to
    /// [`Self::create_child_for_spawn`]. bd-twmyh: the plain spawn-child inherits
    /// cancellation but not the mask, so its `checkpoint()` aborts mid-writeback
    /// under a post-VACUUM masked read.
    #[must_use]
    pub fn create_child_for_spawn_preserving_mask(&self) -> Self {
        let child = self.create_child_for_spawn();
        let depth = self.inner.mask_depth.load(Ordering::Acquire);
        if depth > 0 {
            child.inner.mask_depth.store(depth, Ordering::Release);
        }
        child
    }

    fn create_child_with_runtime_affinity(&self, inherit_runtime_affinity: bool) -> Self {
        let mut child = Self::with_budget_and_cancel_dispatch(
            self.budget,
            Arc::clone(&self.inner.cancel_dispatch_gate),
        );
        child.trace_id = self.trace_id;
        child.decision_id = self.decision_id;
        child.policy_id = self.policy_id;
        if self.inner.unix_millis_is_fixed.load(Ordering::Acquire) {
            let unix_millis = self.inner.unix_millis.load(Ordering::Acquire);
            child
                .inner
                .unix_millis
                .store(unix_millis, Ordering::Release);
            child
                .inner
                .unix_millis_is_fixed
                .store(true, Ordering::Release);
        }
        if let Some(oracle) = self.inner.eprocess_oracle.get().cloned() {
            child.set_eprocess_oracle(oracle);
        }
        // bd-bjm5d: inline-safety is a property of the owning OS thread,
        // not of the native runtime handle. Ordinary same-thread children
        // inherit it; spawn-safe children deliberately do not.
        if inherit_runtime_affinity && self.blocking_io_inline_safe() {
            child.mark_blocking_io_inline_safe();
        }

        #[cfg(feature = "native")]
        let native_to_sync = {
            let _dispatch = self
                .inner
                .cancel_dispatch_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let attached_native = inherit_runtime_affinity
                .then(|| {
                    self.inner
                        .attached_native_cx
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .clone()
                })
                .flatten();
            if let Some(native) = attached_native.as_ref() {
                *child
                    .inner
                    .attached_native_cx
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(native.clone());
            }

            let local_reason = self.cancel_reason();
            let native_reason = mirrored_native_cancel_reason(&self.inner);
            if let Some(local_reason) = local_reason.or(native_reason) {
                publish_cancel_state(&child.inner, local_reason, native_reason);
            }

            // The child becomes visible only after its inherited cancellation
            // state is fully initialized.
            let mut children = self
                .inner
                .children
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if children.len() == children.capacity() {
                children.retain(|registered| registered.strong_count() > 0);
            }
            children.push(Arc::downgrade(&child.inner));

            attached_native.filter(|_| native_reason.is_some())
        };

        #[cfg(not(feature = "native"))]
        {
            let _dispatch = self
                .inner
                .cancel_dispatch_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(reason) = self.cancel_reason() {
                publish_cancel_state(&child.inner, reason, None);
            }
            let mut children = self
                .inner
                .children
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if children.len() == children.capacity() {
                children.retain(|registered| registered.strong_count() > 0);
            }
            children.push(Arc::downgrade(&child.inner));
        }

        #[cfg(feature = "native")]
        if let Some(native) = native_to_sync {
            sync_one_native_cx_cancel(&child.inner, &native);
        }

        child
    }

    /// Create a child context plus narrowly scoped local cancellation authority.
    ///
    /// The child inherits the same budget, tracing metadata, effect
    /// capabilities, and native I/O context as [`Self::create_child`]. The
    /// returned relay can only request local cancellation for that child
    /// subtree; it cannot access the context or cancel the inherited native
    /// context.
    pub fn create_child_with_local_cancel_relay(&self) -> (Self, LocalCancelRelay) {
        let child = self.create_child();
        let relay = LocalCancelRelay {
            inner: Arc::downgrade(&child.inner),
        };
        (child, relay)
    }

    /// Set a deterministic unix time for tests.
    pub fn set_unix_millis_for_testing(&self, millis: u64)
    where
        Caps: cap::HasTime,
    {
        self.inner.unix_millis.store(millis, Ordering::Release);
        self.inner
            .unix_millis_is_fixed
            .store(true, Ordering::Release);
    }

    /// Return current Unix time in milliseconds.
    ///
    /// Production contexts use the live system wall clock. Tests can install
    /// an exact fixed value with [`Self::set_unix_millis_for_testing`].
    #[must_use]
    pub fn current_time_unix_millis(&self) -> u64
    where
        Caps: cap::HasTime,
    {
        if self.inner.unix_millis_is_fixed.load(Ordering::Acquire) {
            return self.inner.unix_millis.load(Ordering::Acquire);
        }

        u64::try_from(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX)
    }

    /// Return current time as a Julian day.
    #[must_use]
    pub fn current_time_julian_day(&self) -> f64
    where
        Caps: cap::HasTime,
    {
        let millis = self.current_time_unix_millis();
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
        let previous = self.inner.mask_depth.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "mask depth underflow");
        if previous == 1 {
            let waiters = {
                let _dispatch = self
                    .inner
                    .cancel_dispatch_gate
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if self.inner.mask_depth.load(Ordering::Acquire) == 0
                    && self.inner.cancel_requested.load(Ordering::Acquire)
                {
                    take_local_cancel_waiters(self.inner)
                } else {
                    Vec::new()
                }
            };
            let mut first_panic = None;
            dispatch_local_cancel_waiters(waiters, &mut first_panic);
            if let Some(payload) = first_panic {
                // Never resume arbitrary callback panic from a destructor:
                // this guard may itself be running during another unwind.
                std::mem::forget(payload);
            }
        }
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
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::{Arc, Barrier, Weak};

    #[derive(Debug, Default)]
    struct CountingWake(AtomicUsize);

    impl std::task::Wake for CountingWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    #[derive(Debug)]
    struct DescendantStateProbeWake {
        descendant: Weak<CxInner>,
        wake_count: AtomicUsize,
        saw_descendant_cancelled: AtomicBool,
        dispatch_gate_was_unlocked: AtomicBool,
    }

    impl DescendantStateProbeWake {
        fn observe(&self) {
            let descendant = self
                .descendant
                .upgrade()
                .expect("observed descendant should remain alive");
            self.saw_descendant_cancelled.store(
                descendant.cancel_requested.load(Ordering::Acquire),
                Ordering::Release,
            );
            let dispatch_guard = descendant
                .cancel_dispatch_gate
                .try_lock()
                .expect("cancellation callbacks must run outside the family phase gate");
            self.dispatch_gate_was_unlocked
                .store(true, Ordering::Release);
            drop(dispatch_guard);
            self.wake_count.fetch_add(1, Ordering::AcqRel);
        }
    }

    impl std::task::Wake for DescendantStateProbeWake {
        fn wake(self: Arc<Self>) {
            self.observe();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.observe();
        }
    }

    #[derive(Debug)]
    struct ReentrantFamilyWake {
        cx: Cx<FullCaps>,
        wake_count: AtomicUsize,
        child_inherited_cancellation: AtomicBool,
    }

    impl ReentrantFamilyWake {
        fn exercise(&self) {
            self.cx.cancel_with_reason(CancelReason::Abort);
            let child = self.cx.create_child();
            self.child_inherited_cancellation.store(
                child.cancel_reason() == Some(CancelReason::Abort),
                Ordering::Release,
            );
            let mask = self.cx.masked();
            drop(mask);
            self.wake_count.fetch_add(1, Ordering::AcqRel);
        }
    }

    impl std::task::Wake for ReentrantFamilyWake {
        fn wake(self: Arc<Self>) {
            self.exercise();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.exercise();
        }
    }

    #[derive(Debug, Default)]
    struct PanicWake;

    impl std::task::Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("intentional cancellation-waker panic");
        }

        fn wake_by_ref(self: &Arc<Self>) {
            panic!("intentional cancellation-waker panic");
        }
    }

    #[derive(Debug)]
    struct RegistryProbeWake {
        inner: Weak<CxInner>,
        wake_count: AtomicUsize,
        registry_was_unlocked: AtomicBool,
    }

    impl RegistryProbeWake {
        fn probe_registry(&self) {
            let inner = self
                .inner
                .upgrade()
                .expect("observed context should remain alive");
            let registry_guard = inner
                .local_cancel_waiters
                .try_lock()
                .expect("waker callbacks must run after releasing the waiter registry");
            self.registry_was_unlocked.store(true, Ordering::Release);
            drop(registry_guard);
            self.wake_count.fetch_add(1, Ordering::AcqRel);
        }
    }

    impl std::task::Wake for RegistryProbeWake {
        fn wake(self: Arc<Self>) {
            self.probe_registry();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.probe_registry();
        }
    }

    fn local_cancel_waiter_count<Caps: cap::SubsetOf<cap::All>>(cx: &Cx<Caps>) -> usize {
        cx.inner
            .local_cancel_waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .len()
    }

    #[test]
    fn test_cx_checkpoint_observes_cancellation() {
        let cx = Cx::new();
        assert_eq!(local_cancel_waiter_count(&cx), 0);
        assert!(cx.checkpoint().is_ok());
        cx.cancel();
        assert_eq!(local_cancel_waiter_count(&cx), 0);
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

    #[cfg(feature = "native")]
    #[test]
    fn bd_2jpu6_2_native_budget_translation_uses_supplied_clock_domain() {
        let now = NativeTime::from_nanos(1_000);
        let local = Budget::INFINITE.with_deadline(Duration::from_nanos(250));

        let native = native_budget_from_local_at(local, now);

        assert_eq!(native.deadline, Some(NativeTime::from_nanos(1_250)));
    }

    #[cfg(feature = "native")]
    #[test]
    fn bd_2jpu6_2_native_spawn_budget_meets_parent_bounds_and_priority() {
        let parent = NativeBudget::INFINITE
            .with_poll_quota(80)
            .with_cost_quota(900)
            .with_priority(9);
        let native_cx = NativeCx::for_testing_with_budget(parent);
        let local = Cx::<FullCaps>::with_budget(
            Budget::INFINITE
                .with_poll_quota(60)
                .with_cost_quota(700)
                .with_priority(3),
        );

        let effective = local.native_spawn_budget(&native_cx);

        assert_eq!(effective.poll_quota, 60);
        assert_eq!(effective.cost_quota, Some(700));
        assert_eq!(effective.priority, 9);
    }

    #[test]
    fn test_cx_scope_with_budget_cannot_loosen() {
        let cx =
            Cx::<FullCaps>::with_budget(Budget::INFINITE.with_deadline(Duration::from_millis(50)));
        let child = Budget::INFINITE.with_deadline(Duration::from_millis(100));
        let scoped = cx.scope_with_budget(child);
        assert_eq!(scoped.budget().deadline, Some(Duration::from_millis(50)));
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

    fn system_time_unix_millis() -> u64 {
        u64::try_from(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX)
    }

    #[test]
    fn test_cx_current_time_uses_live_clock_by_default() {
        let cx = Cx::<FullCaps>::new();
        let observed = cx.current_time_unix_millis();
        let expected = system_time_unix_millis();

        assert!(
            observed.abs_diff(expected) <= 60_000,
            "default Cx clock must be live: observed={observed}, expected approximately {expected}"
        );
    }

    #[test]
    fn test_cx_fixed_unix_millis_supports_full_u64_domain() {
        let cx = Cx::<FullCaps>::new();

        cx.set_unix_millis_for_testing(0);
        assert_eq!(cx.current_time_unix_millis(), 0);

        cx.set_unix_millis_for_testing(u64::MAX);
        assert_eq!(cx.current_time_unix_millis(), u64::MAX);
    }

    #[test]
    fn test_cx_current_time_julian_day_uses_fixed_unix_millis() {
        let cx = Cx::<FullCaps>::new();

        // Unix epoch = Julian day 2440587.5.
        cx.set_unix_millis_for_testing(0);
        let jd = cx.current_time_julian_day();
        assert!((jd - 2_440_587.5).abs() < 1e-10);

        // One day = 86_400_000 ms.
        cx.set_unix_millis_for_testing(86_400_000);
        let jd = cx.current_time_julian_day();
        assert!((jd - 2_440_588.5).abs() < 1e-10);
    }

    #[test]
    fn test_cx_children_inherit_fixed_or_live_clock_state() {
        let fixed_parent = Cx::<FullCaps>::new();
        fixed_parent.set_unix_millis_for_testing(0);
        let fixed_child = fixed_parent.create_child();
        let fixed_spawn_child = fixed_parent.create_child_for_spawn();

        assert_eq!(fixed_child.current_time_unix_millis(), 0);
        assert_eq!(fixed_spawn_child.current_time_unix_millis(), 0);

        // Child clocks are snapshots rather than aliases of the parent's
        // override, so a later parent update cannot rewrite an existing child.
        fixed_parent.set_unix_millis_for_testing(86_400_000);
        assert_eq!(fixed_child.current_time_unix_millis(), 0);

        let live_parent = Cx::<FullCaps>::new();
        let live_child = live_parent.create_child();
        let observed = live_child.current_time_unix_millis();
        let expected = system_time_unix_millis();
        assert!(
            observed.abs_diff(expected) <= 60_000,
            "child of live Cx must remain live: observed={observed}, expected approximately {expected}"
        );
    }

    #[test]
    fn test_cx_fixed_clock_updates_publish_complete_values() {
        const FIRST: u64 = 0xAAAA_AAAA_AAAA_AAAA;
        const SECOND: u64 = 0x5555_5555_5555_5555;

        let cx = Cx::<FullCaps>::new();
        cx.set_unix_millis_for_testing(FIRST);
        let writer_cx = cx.clone();
        let writer = std::thread::spawn(move || {
            for _ in 0..1_000 {
                writer_cx.set_unix_millis_for_testing(FIRST);
                writer_cx.set_unix_millis_for_testing(SECOND);
            }
        });

        for _ in 0..1_000 {
            let observed = cx.current_time_unix_millis();
            assert!(matches!(observed, FIRST | SECOND));
        }
        writer.join().expect("clock writer must not panic");
        assert_eq!(cx.current_time_unix_millis(), SECOND);
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
    fn spawn_child_preserves_logical_context_without_thread_affinity() {
        let budget = Budget {
            deadline: Some(Duration::from_secs(7)),
            poll_quota: 123,
            cost_quota: Some(456),
            priority: 3,
        };
        let parent = Cx::<FullCaps>::with_budget(budget).with_trace_context(50, 60, 70);
        let oracle = Arc::new(EProcessOracle::new(
            EProcessConfig {
                p0: 0.1,
                lambda: 5.0,
                alpha: 0.05,
                max_evalue: 1e12,
            },
            1,
        ));
        parent.set_eprocess_oracle(Arc::clone(&oracle));
        parent.mark_blocking_io_inline_safe();

        let child = parent.create_child_for_spawn();
        assert_eq!(child.budget(), budget);
        assert_eq!(child.trace_id(), 50);
        assert_eq!(child.decision_id(), 60);
        assert_eq!(child.policy_id(), 70);
        assert!(
            Arc::ptr_eq(
                child
                    .inner
                    .eprocess_oracle
                    .get()
                    .expect("spawn child should inherit the e-process oracle"),
                &oracle
            ),
            "spawn child must retain the exact logical policy oracle"
        );
        assert!(
            !child.blocking_io_inline_safe(),
            "spawn child must not inherit an OS-thread-only I/O permission"
        );

        parent.cancel_with_reason(CancelReason::RegionClose);
        assert_eq!(child.cancel_reason(), Some(CancelReason::RegionClose));
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
    fn local_cancel_relay_is_subtree_scoped_and_reason_monotone() {
        let root = Cx::<FullCaps>::new();
        let sibling = root.create_child();
        let (operation, relay) = root.create_child_with_local_cancel_relay();
        let existing_descendant = operation.create_child();

        assert!(relay.cancel_local(CancelReason::Timeout));
        assert!(relay.cancel_local(CancelReason::Abort));
        assert!(relay.cancel_local(CancelReason::UserInterrupt));

        assert_eq!(operation.cancel_reason(), Some(CancelReason::Abort));
        assert_eq!(
            existing_descendant.cancel_reason(),
            Some(CancelReason::Abort)
        );
        assert!(operation.checkpoint().is_err());
        assert!(existing_descendant.checkpoint().is_err());

        assert!(root.checkpoint().is_ok());
        assert!(sibling.checkpoint().is_ok());
        assert!(!root.is_cancel_requested());
        assert!(!sibling.is_cancel_requested());

        let late_descendant = operation.create_child();
        assert_eq!(late_descendant.cancel_reason(), Some(CancelReason::Abort));
        assert!(late_descendant.checkpoint().is_err());
        assert!(root.checkpoint().is_ok());
    }

    #[test]
    fn local_cancel_relay_is_weak_and_cross_thread_safe() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<LocalCancelRelay>();

        let root = Cx::<FullCaps>::new();
        let (operation, relay) = root.create_child_with_local_cancel_relay();
        let cancel_thread =
            std::thread::spawn(move || relay.cancel_local(CancelReason::RegionClose));
        assert!(
            cancel_thread
                .join()
                .expect("cancel thread should not panic"),
            "live operation should accept a relayed cancellation"
        );
        assert_eq!(operation.cancel_reason(), Some(CancelReason::RegionClose));

        let (dropped_operation, dropped_relay) = root.create_child_with_local_cancel_relay();
        drop(dropped_operation);
        assert!(
            !dropped_relay.cancel_local(CancelReason::Abort),
            "a weak relay must become inert after its target is dropped"
        );
        assert!(root.checkpoint().is_ok());
    }

    #[test]
    fn local_cancellation_future_wakes_for_local_relay() {
        let root = Cx::<FullCaps>::new();
        let (operation, relay) = root.create_child_with_local_cancel_relay();
        let wake_count = Arc::new(CountingWake::default());
        let waker = Waker::from(Arc::clone(&wake_count));
        let mut task_cx = TaskContext::from_waker(&waker);
        let mut cancellation = std::pin::pin!(operation.wait_for_local_cancellation());

        assert_eq!(
            cancellation.as_mut().poll(&mut task_cx),
            Poll::Pending,
            "uncancelled operation should register one waiter"
        );
        assert_eq!(local_cancel_waiter_count(&operation), 1);

        assert!(relay.cancel_local(CancelReason::RegionClose));
        assert_eq!(
            wake_count.0.load(Ordering::Acquire),
            1,
            "local relay cancellation should wake the registered future"
        );
        assert_eq!(cancellation.as_mut().poll(&mut task_cx), Poll::Ready(()));
        assert_eq!(
            local_cancel_waiter_count(&operation),
            0,
            "ready future must leave no stale registration"
        );
        assert!(root.checkpoint().is_ok());
    }

    #[test]
    fn dropping_local_cancellation_future_unregisters_waiter() {
        let cx = Cx::<FullCaps>::new();
        let wake_count = Arc::new(CountingWake::default());
        let waker = Waker::from(wake_count);
        let mut task_cx = TaskContext::from_waker(&waker);

        {
            let mut cancellation = std::pin::pin!(cx.wait_for_local_cancellation());
            assert_eq!(cancellation.as_mut().poll(&mut task_cx), Poll::Pending);
            assert_eq!(local_cancel_waiter_count(&cx), 1);
        }

        assert_eq!(
            local_cancel_waiter_count(&cx),
            0,
            "dropping a pending future must remove its waker"
        );
    }

    #[test]
    fn already_cancelled_local_future_never_registers_a_waiter() {
        let cx = Cx::<FullCaps>::new();
        cx.cancel();
        assert_eq!(local_cancel_waiter_count(&cx), 0);

        let wake_count = Arc::new(CountingWake::default());
        let waker = Waker::from(wake_count);
        let mut task_cx = TaskContext::from_waker(&waker);
        let mut cancellation = std::pin::pin!(cx.wait_for_local_cancellation());
        assert_eq!(cancellation.as_mut().poll(&mut task_cx), Poll::Ready(()));
        assert_eq!(
            local_cancel_waiter_count(&cx),
            0,
            "an already-ready first poll must not register a waiter"
        );
    }

    #[test]
    fn repoll_replaces_the_registered_waker() {
        let cx = Cx::<FullCaps>::new();
        let first_wake_count = Arc::new(CountingWake::default());
        let first_waker = Waker::from(Arc::clone(&first_wake_count));
        let mut first_task_cx = TaskContext::from_waker(&first_waker);
        let second_wake_count = Arc::new(CountingWake::default());
        let second_waker = Waker::from(Arc::clone(&second_wake_count));
        let mut second_task_cx = TaskContext::from_waker(&second_waker);
        let mut cancellation = std::pin::pin!(cx.wait_for_local_cancellation());

        assert_eq!(
            cancellation.as_mut().poll(&mut first_task_cx),
            Poll::Pending
        );
        assert_eq!(
            cancellation.as_mut().poll(&mut second_task_cx),
            Poll::Pending
        );
        assert_eq!(local_cancel_waiter_count(&cx), 1);

        cx.cancel();
        assert_eq!(
            first_wake_count.0.load(Ordering::Acquire),
            0,
            "a replaced waker must not be invoked"
        );
        assert_eq!(
            second_wake_count.0.load(Ordering::Acquire),
            1,
            "only the most recently registered waker should be invoked"
        );
        assert_eq!(
            cancellation.as_mut().poll(&mut second_task_cx),
            Poll::Ready(())
        );
    }

    #[test]
    fn local_cancellation_waker_runs_outside_the_registry_lock() {
        let cx = Cx::<FullCaps>::new();
        let probe = Arc::new(RegistryProbeWake {
            inner: Arc::downgrade(&cx.inner),
            wake_count: AtomicUsize::new(0),
            registry_was_unlocked: AtomicBool::new(false),
        });
        let waker = Waker::from(Arc::clone(&probe));
        let mut task_cx = TaskContext::from_waker(&waker);
        let mut cancellation = std::pin::pin!(cx.wait_for_local_cancellation());

        assert_eq!(cancellation.as_mut().poll(&mut task_cx), Poll::Pending);
        cx.cancel();
        assert!(
            probe.registry_was_unlocked.load(Ordering::Acquire),
            "wake callback should acquire the registry without reentrant deadlock"
        );
        assert_eq!(probe.wake_count.load(Ordering::Acquire), 1);
        assert_eq!(cancellation.as_mut().poll(&mut task_cx), Poll::Ready(()));
    }

    #[test]
    fn cancellation_publishes_the_complete_subtree_before_waking_observers() {
        let root = Cx::<FullCaps>::new();
        let descendant = root.create_child();
        let probe = Arc::new(DescendantStateProbeWake {
            descendant: Arc::downgrade(&descendant.inner),
            wake_count: AtomicUsize::new(0),
            saw_descendant_cancelled: AtomicBool::new(false),
            dispatch_gate_was_unlocked: AtomicBool::new(false),
        });
        let waker = Waker::from(Arc::clone(&probe));
        let mut task_cx = TaskContext::from_waker(&waker);
        let mut cancellation = std::pin::pin!(root.wait_for_local_cancellation());

        assert_eq!(cancellation.as_mut().poll(&mut task_cx), Poll::Pending);
        root.cancel();

        assert_eq!(probe.wake_count.load(Ordering::Acquire), 1);
        assert!(
            probe.saw_descendant_cancelled.load(Ordering::Acquire),
            "reentrant observers must never see a half-published cancellation tree"
        );
        assert!(
            probe.dispatch_gate_was_unlocked.load(Ordering::Acquire),
            "callbacks must be able to re-enter family cancellation machinery"
        );
        assert_eq!(cancellation.as_mut().poll(&mut task_cx), Poll::Ready(()));
        assert!(descendant.checkpoint().is_err());
    }

    #[test]
    fn panicking_cancellation_waker_does_not_suppress_other_observers() {
        let root = Cx::<FullCaps>::new();
        let descendant = root.create_child();
        let panic_waker = Waker::from(Arc::new(PanicWake));
        let mut panic_task_cx = TaskContext::from_waker(&panic_waker);
        let wake_count = Arc::new(CountingWake::default());
        let counting_waker = Waker::from(Arc::clone(&wake_count));
        let mut counting_task_cx = TaskContext::from_waker(&counting_waker);
        let mut panicking = std::pin::pin!(root.wait_for_local_cancellation());
        let mut counting = std::pin::pin!(root.wait_for_local_cancellation());

        assert_eq!(panicking.as_mut().poll(&mut panic_task_cx), Poll::Pending);
        assert_eq!(counting.as_mut().poll(&mut counting_task_cx), Poll::Pending);
        let cancel_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            root.cancel();
        }));

        assert!(
            cancel_result.is_err(),
            "the first callback panic must be resumed after notification completes"
        );
        assert_eq!(
            wake_count.0.load(Ordering::Acquire),
            1,
            "one panicking observer must not suppress later observers"
        );
        assert!(descendant.checkpoint().is_err());
        assert_eq!(panicking.as_mut().poll(&mut panic_task_cx), Poll::Ready(()));
        assert_eq!(
            counting.as_mut().poll(&mut counting_task_cx),
            Poll::Ready(())
        );
        assert_eq!(local_cancel_waiter_count(&root), 0);
    }

    #[test]
    fn cancellation_waker_can_reenter_family_state_without_deadlock() {
        let root = Cx::<FullCaps>::new();
        let probe = Arc::new(ReentrantFamilyWake {
            cx: root.clone(),
            wake_count: AtomicUsize::new(0),
            child_inherited_cancellation: AtomicBool::new(false),
        });
        let waker = Waker::from(Arc::clone(&probe));
        let mut task_cx = TaskContext::from_waker(&waker);
        let mut cancellation = std::pin::pin!(root.wait_for_local_cancellation());

        assert_eq!(cancellation.as_mut().poll(&mut task_cx), Poll::Pending);
        root.cancel();

        assert_eq!(probe.wake_count.load(Ordering::Acquire), 1);
        assert!(
            probe.child_inherited_cancellation.load(Ordering::Acquire),
            "a child created reentrantly must be initialized before it is linked"
        );
        assert_eq!(root.cancel_reason(), Some(CancelReason::Abort));
        assert_eq!(cancellation.as_mut().poll(&mut task_cx), Poll::Ready(()));
    }

    #[test]
    fn repeated_local_waiter_poll_and_drop_accumulates_nothing() {
        let cx = Cx::<FullCaps>::new();
        let initial_children = cx
            .inner
            .children
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        let wake_count = Arc::new(CountingWake::default());
        let waker = Waker::from(wake_count);
        let mut task_cx = TaskContext::from_waker(&waker);

        for _ in 0..256 {
            let mut cancellation = std::pin::pin!(cx.wait_for_local_cancellation());
            assert_eq!(cancellation.as_mut().poll(&mut task_cx), Poll::Pending);
        }

        assert_eq!(
            local_cancel_waiter_count(&cx),
            0,
            "dropped local wait futures must not accumulate registry entries"
        );
        assert_eq!(
            cx.inner
                .children
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            initial_children,
            "local wait futures must not allocate child contexts"
        );
    }

    #[test]
    fn local_cancellation_future_defers_while_masked_and_wakes_on_unmask() {
        let root = Cx::<FullCaps>::new();
        let (operation, relay) = root.create_child_with_local_cancel_relay();
        let mask = operation.masked();
        let wake_count = Arc::new(CountingWake::default());
        let waker = Waker::from(Arc::clone(&wake_count));
        let mut task_cx = TaskContext::from_waker(&waker);
        let mut cancellation = std::pin::pin!(operation.wait_for_local_cancellation());

        assert_eq!(cancellation.as_mut().poll(&mut task_cx), Poll::Pending);
        assert!(relay.cancel_local(CancelReason::Abort));
        assert_eq!(wake_count.0.load(Ordering::Acquire), 1);

        assert_eq!(
            cancellation.as_mut().poll(&mut task_cx),
            Poll::Pending,
            "masking must defer cancellation observation"
        );
        assert_eq!(
            local_cancel_waiter_count(&operation),
            1,
            "a masked future must remain registered for the unmask boundary"
        );

        drop(mask);
        assert_eq!(
            wake_count.0.load(Ordering::Acquire),
            2,
            "outermost unmask must wake a deferred cancellation observer"
        );
        assert_eq!(cancellation.as_mut().poll(&mut task_cx), Poll::Ready(()));
    }

    #[test]
    fn local_cancel_request_future_is_ready_while_masked() {
        let root = Cx::<FullCaps>::new();
        let (operation, relay) = root.create_child_with_local_cancel_relay();
        let mask = operation.masked();
        let wake_count = Arc::new(CountingWake::default());
        let waker = Waker::from(Arc::clone(&wake_count));
        let mut task_cx = TaskContext::from_waker(&waker);
        let mut request = std::pin::pin!(operation.wait_for_local_cancel_request());

        assert_eq!(request.as_mut().poll(&mut task_cx), Poll::Pending);
        assert!(relay.cancel_local(CancelReason::UserInterrupt));
        assert_eq!(wake_count.0.load(Ordering::Acquire), 1);
        assert_eq!(
            request.as_mut().poll(&mut task_cx),
            Poll::Ready(()),
            "raw request notification must not reinterpret masking policy"
        );
        assert!(
            operation.checkpoint().is_ok(),
            "the context checkpoint itself must continue to defer while masked"
        );
        drop(mask);
        assert!(operation.checkpoint().is_err());
    }

    #[test]
    fn local_cancellation_registration_race_leaves_no_stale_waiter() {
        for _ in 0..64 {
            let root = Cx::<FullCaps>::new();
            let (operation, relay) = root.create_child_with_local_cancel_relay();
            let barrier = Arc::new(Barrier::new(2));
            let cancel_barrier = Arc::clone(&barrier);
            let cancel_thread = std::thread::spawn(move || {
                cancel_barrier.wait();
                relay.cancel_local(CancelReason::UserInterrupt)
            });

            let wake_count = Arc::new(CountingWake::default());
            let waker = Waker::from(Arc::clone(&wake_count));
            let mut task_cx = TaskContext::from_waker(&waker);
            let mut cancellation = std::pin::pin!(operation.wait_for_local_cancellation());
            barrier.wait();
            let first_poll = cancellation.as_mut().poll(&mut task_cx);

            assert!(
                cancel_thread
                    .join()
                    .expect("cancel thread should not panic")
            );
            if first_poll == Poll::Pending {
                assert!(
                    wake_count.0.load(Ordering::Acquire) > 0,
                    "a waiter registered during cancellation must be notified"
                );
            }
            assert_eq!(cancellation.as_mut().poll(&mut task_cx), Poll::Ready(()));
            assert_eq!(
                local_cancel_waiter_count(&operation),
                0,
                "registration/cancellation race must not strand a waker"
            );
        }
    }

    #[test]
    fn local_cancel_relay_handles_deep_subtrees_on_a_small_stack() {
        let root = Cx::<FullCaps>::new();
        let (operation, relay) = root.create_child_with_local_cancel_relay();
        let mut chain = Vec::with_capacity(8_193);
        chain.push(operation);
        for _ in 0..8_192 {
            let child = chain
                .last()
                .expect("chain must contain its root")
                .create_child();
            chain.push(child);
        }

        let cancel_thread = std::thread::Builder::new()
            .name("local-cancel-deep-tree".to_owned())
            .stack_size(256 * 1024)
            .spawn(move || relay.cancel_local(CancelReason::RegionClose))
            .expect("small-stack cancellation thread should spawn");
        assert!(
            cancel_thread
                .join()
                .expect("iterative cancellation traversal must not overflow")
        );
        assert_eq!(
            chain.last().and_then(Cx::cancel_reason),
            Some(CancelReason::RegionClose)
        );
        assert!(root.checkpoint().is_ok());
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
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_cx_cancel_reason_propagates_to_native_cx() {
        let cx = Cx::<FullCaps>::new();
        let native = NativeCx::for_testing();
        cx.set_native_cx(native.clone());

        cx.cancel_with_reason(CancelReason::RegionClose);
        let reason = native
            .cancel_reason()
            .expect("native cancel reason must be set");
        assert_eq!(reason.kind, NativeCancelKind::ParentCancelled);
    }

    #[cfg(feature = "native")]
    #[test]
    fn local_cancel_relay_preserves_shared_native_context_for_late_children() {
        let root = Cx::<FullCaps>::new();
        let native = NativeCx::for_testing();
        root.set_native_cx(native.clone());
        let sibling = root.create_child();
        let (operation, relay) = root.create_child_with_local_cancel_relay();
        let existing_descendant = operation.create_child();

        assert!(relay.cancel_local(CancelReason::Abort));
        assert!(operation.checkpoint().is_err());
        assert!(existing_descendant.checkpoint().is_err());
        assert!(root.checkpoint().is_ok());
        assert!(sibling.checkpoint().is_ok());
        assert!(
            native.checkpoint().is_ok(),
            "local operation cancellation must not poison shared native I/O state"
        );

        let late_descendant = operation.create_child();
        assert!(late_descendant.checkpoint().is_err());
        assert!(
            native.checkpoint().is_ok(),
            "a descendant created after local cancellation must inherit locally"
        );

        root.cancel_with_reason(CancelReason::RegionClose);
        assert!(
            native.is_cancel_requested(),
            "later ordinary root cancellation must still cross the native boundary"
        );
    }

    #[cfg(feature = "native")]
    #[test]
    fn local_cancel_relay_preserves_native_contexts_attached_after_cancellation() {
        let root = Cx::<FullCaps>::new();
        let (operation, relay) = root.create_child_with_local_cancel_relay();
        assert!(relay.cancel_local(CancelReason::Abort));

        let fallback = operation.effective_native_cx();
        assert!(
            fallback.checkpoint().is_ok(),
            "local cancellation must not taint a later fallback native context"
        );

        let replacement = NativeCx::for_testing();
        operation.set_native_cx(replacement.clone());
        assert!(
            replacement.checkpoint().is_ok(),
            "local cancellation must not taint a later explicit native context"
        );
        assert!(operation.checkpoint().is_err());
    }

    #[cfg(feature = "native")]
    #[test]
    fn local_reason_never_leaks_through_later_ordinary_cancellation() {
        let root = Cx::<FullCaps>::new();
        let shared_native = NativeCx::for_testing();
        root.set_native_cx(shared_native.clone());
        let (operation, relay) = root.create_child_with_local_cancel_relay();

        assert!(relay.cancel_local(CancelReason::Abort));
        root.cancel_with_reason(CancelReason::Timeout);

        assert_eq!(operation.cancel_reason(), Some(CancelReason::Abort));
        assert_eq!(
            shared_native
                .cancel_reason()
                .expect("ordinary cancellation must reach shared native")
                .kind,
            NativeCancelKind::Timeout,
            "the stronger local-only Abort must not cross the native boundary"
        );

        let late_descendant = operation.create_child();
        assert_eq!(
            late_descendant.cancel_reason(),
            Some(CancelReason::Abort),
            "late descendants inherit the aggregate local reason"
        );
        assert_eq!(
            shared_native
                .cancel_reason()
                .expect("late-child registration must retain ordinary reason")
                .kind,
            NativeCancelKind::Timeout
        );

        operation.clear_native_cx();
        let fallback = operation.effective_native_cx();
        assert_eq!(
            fallback
                .cancel_reason()
                .expect("late fallback must receive ordinary reason")
                .kind,
            NativeCancelKind::Timeout
        );

        let replacement = NativeCx::for_testing();
        operation.set_native_cx(replacement.clone());
        assert_eq!(
            replacement
                .cancel_reason()
                .expect("late explicit attachment must receive ordinary reason")
                .kind,
            NativeCancelKind::Timeout
        );
    }

    #[cfg(feature = "native")]
    #[test]
    fn weaker_ordinary_reason_cannot_downgrade_native_cancellation() {
        let cx = Cx::<FullCaps>::new();
        let native = NativeCx::for_testing();
        cx.set_native_cx(native.clone());

        cx.cancel_with_reason(CancelReason::Abort);
        cx.cancel_with_reason(CancelReason::Timeout);

        assert_eq!(cx.cancel_reason(), Some(CancelReason::Abort));
        assert_eq!(
            native
                .cancel_reason()
                .expect("native reason must remain present")
                .kind,
            NativeCancelKind::ResourceUnavailable
        );
    }

    #[cfg(feature = "native")]
    #[test]
    fn native_attachment_racing_ordinary_cancellation_never_misses_reason() {
        for _ in 0..128 {
            let cx = Cx::<FullCaps>::new();
            let cancel_cx = cx.clone();
            let attach_cx = cx.clone();
            let native = NativeCx::for_testing();
            let attached_native = native.clone();
            let gate = Arc::new(std::sync::Barrier::new(3));
            let cancel_gate = Arc::clone(&gate);
            let attach_gate = Arc::clone(&gate);

            let cancel_thread = std::thread::spawn(move || {
                cancel_gate.wait();
                cancel_cx.cancel_with_reason(CancelReason::RegionClose);
            });
            let attach_thread = std::thread::spawn(move || {
                attach_gate.wait();
                attach_cx.set_native_cx(attached_native);
            });
            gate.wait();
            cancel_thread.join().expect("cancellation must not panic");
            attach_thread.join().expect("attachment must not panic");

            assert_eq!(
                native
                    .cancel_reason()
                    .expect("racing attachment must observe cancellation")
                    .kind,
                NativeCancelKind::ParentCancelled
            );
        }
    }

    #[cfg(feature = "native")]
    #[test]
    fn racing_native_replacements_each_synchronize_the_exact_supplied_handle() {
        for _ in 0..128 {
            let cx = Cx::<FullCaps>::new();
            cx.cancel_with_reason(CancelReason::RegionClose);

            let native_a = NativeCx::for_testing();
            let native_b = NativeCx::for_testing();
            let setter_a = cx.clone();
            let setter_b = cx.clone();
            let supplied_a = native_a.clone();
            let supplied_b = native_b.clone();
            let gate = Arc::new(std::sync::Barrier::new(3));
            let gate_a = Arc::clone(&gate);
            let gate_b = Arc::clone(&gate);

            let thread_a = std::thread::spawn(move || {
                gate_a.wait();
                setter_a.set_native_cx(supplied_a);
            });
            let thread_b = std::thread::spawn(move || {
                gate_b.wait();
                setter_b.set_native_cx(supplied_b);
            });
            gate.wait();
            thread_a.join().expect("first replacement must not panic");
            thread_b.join().expect("second replacement must not panic");

            for native in [&native_a, &native_b] {
                assert_eq!(
                    native
                        .cancel_reason()
                        .expect("each exact supplied handle must be synchronized")
                        .kind,
                    NativeCancelKind::ParentCancelled
                );
            }
        }
    }

    #[cfg(feature = "native")]
    #[test]
    fn fallback_creation_racing_ordinary_cancellation_never_misses_reason() {
        for _ in 0..128 {
            let cx = Cx::<FullCaps>::new();
            let cancel_cx = cx.clone();
            let fallback_cx = cx.clone();
            let gate = Arc::new(std::sync::Barrier::new(3));
            let cancel_gate = Arc::clone(&gate);
            let fallback_gate = Arc::clone(&gate);

            let cancel_thread = std::thread::spawn(move || {
                cancel_gate.wait();
                cancel_cx.cancel_with_reason(CancelReason::RegionClose);
            });
            let fallback_thread = std::thread::spawn(move || {
                fallback_gate.wait();
                fallback_cx.effective_native_cx()
            });
            gate.wait();
            cancel_thread.join().expect("cancellation must not panic");
            let native = fallback_thread
                .join()
                .expect("fallback creation must not panic");

            assert_eq!(
                native
                    .cancel_reason()
                    .expect("racing fallback must observe cancellation")
                    .kind,
                NativeCancelKind::ParentCancelled
            );
        }
    }

    #[cfg(feature = "native")]
    #[test]
    fn ordinary_cancel_before_native_attachment_is_mirrored_after_registration() {
        let cx = Cx::<FullCaps>::new();
        cx.cancel_with_reason(CancelReason::RegionClose);

        let native = NativeCx::for_testing();
        cx.set_native_cx(native.clone());
        let reason = native
            .cancel_reason()
            .expect("ordinary local cancellation must mirror to a later attachment");
        assert_eq!(reason.kind, NativeCancelKind::ParentCancelled);
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
        let reason = child
            .effective_native_cx()
            .cancel_reason()
            .expect("fallback native cx should mirror inherited cancellation");
        assert_eq!(reason.kind, NativeCancelKind::ParentCancelled);
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
    fn spawn_child_does_not_carry_a_task_affine_native_context() {
        let parent = Cx::<FullCaps>::new();
        parent.set_native_cx(NativeCx::for_testing());
        let child = parent.create_child_for_spawn();

        assert!(
            child.attached_native_cx().is_none(),
            "spawn child must start without the caller task's native context"
        );
        assert!(
            child.inner.fallback_native_cx.get().is_none(),
            "spawn child must not invent a fallback context before task entry"
        );

        let task_native = NativeCx::for_testing();
        child.set_native_cx(task_native);
        assert!(
            child.attached_native_cx().is_some(),
            "the spawned task must be able to attach its own native context"
        );
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

            if let Some(until) = skip_until_depth
                && brace_depth <= until
            {
                skip_until_depth = None;
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
                    // Every detached-root context now routes through the sanctioned
                    // `Cx::detached_rebind()` constructor (fsqlite-types), which the
                    // forbidden scan never matches. Bare `Cx::new()` / `Cx::default()`
                    // is therefore forbidden in runtime crates with ZERO exceptions
                    // (bd-fq2lf: the last connection.rs allowlist entry was removed
                    // once `detached_root_cx` and `detached_rebind_cx` were routed
                    // through the factory).
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
        // - types (capability-context implementation and its test audit)
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
        // - beads-doctor (standalone CLI monitoring tool: std::fs for config /
        //   rolling-log / OS-unit-file I/O, and a single SystemTime::now —
        //   quarantined behind a `Clock` trait — for calendar-age log rotation,
        //   which asupersync's process-monotonic `wall_now` cannot express; the
        //   rotation *decision* is a pure, clock-free function — bd-316l0)
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
            "beads-doctor",
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
    fn dropped_children_are_pruned_before_registry_growth_without_cancellation() {
        let parent = Cx::<FullCaps>::new();
        drop(parent.create_child());
        let initial_capacity = parent
            .inner
            .children
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .capacity();
        assert!(initial_capacity > 0);

        for _ in 0..4_096 {
            drop(parent.create_child());
        }

        let children = parent
            .inner
            .children
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(
            children.capacity(),
            initial_capacity,
            "historical dead children must not grow an uncancelled family registry"
        );
        assert!(
            children.len() <= initial_capacity,
            "only the current bounded batch of dead weak links may remain"
        );
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
    // === bd-bjm5d: blocking_io_inline_safe marker ===

    #[test]
    fn blocking_io_inline_safe_defaults_false() {
        let cx = Cx::new();
        assert!(!cx.blocking_io_inline_safe());
    }

    #[test]
    fn blocking_io_inline_safe_shared_through_clone() {
        let cx = Cx::new();
        let clone = cx.clone();
        cx.mark_blocking_io_inline_safe();
        assert!(clone.blocking_io_inline_safe());
    }

    #[test]
    fn blocking_io_inline_safe_inherited_by_create_child() {
        let cx = Cx::new();
        cx.mark_blocking_io_inline_safe();
        let child = cx.create_child();
        assert!(child.blocking_io_inline_safe());
    }

    #[test]
    fn blocking_io_inline_safe_not_invented_by_child_of_unset_parent() {
        let cx = Cx::new();
        let child = cx.create_child();
        assert!(!child.blocking_io_inline_safe());
        // Marking the child later must not leak back to the parent: the
        // child has its own CxInner.
        child.mark_blocking_io_inline_safe();
        assert!(!cx.blocking_io_inline_safe());
    }
}
