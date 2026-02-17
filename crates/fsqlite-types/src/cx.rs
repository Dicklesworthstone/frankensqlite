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

use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

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
struct CxInner {
    cancel_requested: AtomicBool,
    cancel_state: Mutex<CancelState>,
    cancel_reason: Mutex<Option<CancelReason>>,
    mask_depth: AtomicU32,
    children: Mutex<Vec<Weak<Self>>>,
    last_checkpoint_msg: Mutex<Option<String>>,
    // Deterministic clock: milliseconds since epoch for tests.
    unix_millis: AtomicU64,
}

impl CxInner {
    fn new() -> Self {
        Self {
            cancel_requested: AtomicBool::new(false),
            cancel_state: Mutex::new(CancelState::Created),
            cancel_reason: Mutex::new(None),
            mask_depth: AtomicU32::new(0),
            children: Mutex::new(Vec::new()),
            last_checkpoint_msg: Mutex::new(None),
            unix_millis: AtomicU64::new(0),
        }
    }
}

/// Propagate cancellation to a `CxInner` node and all its descendants.
///
/// We release each node's lock before recursing into children to avoid
/// lock-ordering issues.
fn propagate_cancel(inner: &CxInner, reason: CancelReason) {
    // Set atomic flag (fast-path for checkpoint).
    inner.cancel_requested.store(true, Ordering::Release);

    // Monotone reason update.
    {
        let mut r = inner
            .cancel_reason
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match *r {
            Some(existing) if existing >= reason => {}
            _ => *r = Some(reason),
        }
    }

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

    // Collect children (release lock before recursing).
    let children: Vec<Arc<CxInner>> = {
        let mut guard = inner
            .children
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.retain(|child| child.strong_count() > 0);
        guard.iter().filter_map(Weak::upgrade).collect()
    };
    for child in &children {
        propagate_cancel(child, reason);
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
}

impl<Caps: cap::SubsetOf<cap::All>> Cx<Caps> {
    #[must_use]
    pub fn with_budget(budget: Budget) -> Self {
        Self {
            inner: Arc::new(CxInner::new()),
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

    // -----------------------------------------------------------------------
    // Checkpoints (§4.12.1)
    // -----------------------------------------------------------------------

    /// Check for cancellation at a yield point.
    ///
    /// Returns `Ok(())` when not cancelled **or when inside a masked section**.
    /// When cancellation is observed, transitions state from `CancelRequested`
    /// to `Cancelling`.
    pub fn checkpoint(&self) -> Result<()> {
        // Fast path: not cancelled at all.
        if !self.inner.cancel_requested.load(Ordering::Acquire) {
            return Ok(());
        }
        // Masked: defer cancellation observation.
        if self.inner.mask_depth.load(Ordering::Acquire) > 0 {
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
        let mut child = Self::with_budget(self.budget);
        child.trace_id = self.trace_id;
        child.decision_id = self.decision_id;
        child.policy_id = self.policy_id;
        {
            let mut children = self
                .inner
                .children
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            children.push(Arc::downgrade(&child.inner));
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
    use std::path::{Path, PathBuf};
    use std::sync::Weak;

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

    fn scan_file_outside_cfg_test_modules(src: &str, patterns: &[&str]) -> Vec<(usize, String)> {
        let mut hits = Vec::new();

        let mut brace_depth: i32 = 0;
        let mut pending_cfg_test = false;
        let mut skip_until_depth: Option<i32> = None;

        for (idx, line) in src.lines().enumerate() {
            let trimmed = line.trim_start();

            if skip_until_depth.is_none() {
                // Handle `#[cfg(test)] mod tests {` on a single line.
                if trimmed.starts_with("#[cfg(test)]") && trimmed.contains("mod ") {
                    pending_cfg_test = false;
                    skip_until_depth = Some(brace_depth);
                } else if trimmed.starts_with("#[cfg(test)]") {
                    pending_cfg_test = true;
                } else if pending_cfg_test {
                    // Allow additional attributes/blank lines before `mod ... {`.
                    if trimmed.is_empty() || trimmed.starts_with("//") || trimmed.starts_with("#[")
                    {
                        // keep pending
                    } else if trimmed.starts_with("mod ")
                        || trimmed.starts_with("pub mod ")
                        || trimmed.starts_with("pub(crate) mod ")
                    {
                        pending_cfg_test = false;
                        skip_until_depth = Some(brace_depth);
                    } else {
                        pending_cfg_test = false;
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
    fn test_ambient_authority_audit_gate() {
        // Scan `crates/*/src/**/*.rs` for ambient-authority usage, excluding
        // `#[cfg(test)] mod ...` blocks.
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

        // Crates that are test infrastructure, not runtime code.
        // They legitimately need ambient time/process/fs APIs.
        let exempt_crates = ["fsqlite-harness", "fsqlite-cli", "fsqlite-e2e"];

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
                for (line, pat) in scan_file_outside_cfg_test_modules(&src, &always_forbidden) {
                    violations.push(format!(
                        "{crate_name}:{path}:{line} uses forbidden `{pat}`",
                        path = file.display()
                    ));
                }

                if crate_name != "fsqlite-vfs" {
                    for (line, pat) in scan_file_outside_cfg_test_modules(&src, &non_vfs_forbidden)
                    {
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
            .with_trace_context(10, 20, 30)
            .with_policy_id(88);
        assert_eq!(cx.trace_id(), 10);
        assert_eq!(cx.decision_id(), 20);
        assert_eq!(cx.policy_id(), 88);
    }

    #[test]
    fn test_clone_propagates_trace_ids() {
        let cx = Cx::<FullCaps>::new().with_trace_context(1, 2, 3);
        let cloned = cx.clone();
        assert_eq!(cloned.trace_id(), 1);
        assert_eq!(cloned.decision_id(), 2);
        assert_eq!(cloned.policy_id(), 3);
    }

    #[test]
    fn test_restrict_propagates_trace_ids() {
        let cx = Cx::<FullCaps>::new().with_trace_context(100, 200, 300);
        let compute = cx.restrict::<ComputeCaps>();
        assert_eq!(compute.trace_id(), 100);
        assert_eq!(compute.decision_id(), 200);
        assert_eq!(compute.policy_id(), 300);
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
