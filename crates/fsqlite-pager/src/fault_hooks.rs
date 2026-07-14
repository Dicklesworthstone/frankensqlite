//! Targeted fault hooks for group-commit publish verification.
//!
//! Hooks retain their original process-global behavior unless the caller holds
//! a [`FaultInjectionSession`]. A session serializes fault-injection scenarios,
//! tags every arm with its owning generation, scopes hook consumption to
//! explicitly participating threads, and clears all hook state on drop. This
//! prevents unrelated concurrent pager work and late participants from
//! stealing or leaking a one-shot hook while still supporting multi-threaded
//! scenarios.

use std::cell::Cell;
use std::mem;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, LockResult, Mutex, MutexGuard};

use fsqlite_error::{FrankenError, Result};
use tracing::warn;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultHookArm {
    pub run_id: String,
    pub scenario_id: String,
    pub invariant_family: String,
}

impl FaultHookArm {
    #[must_use]
    pub fn new(
        run_id: impl Into<String>,
        scenario_id: impl Into<String>,
        invariant_family: impl Into<String>,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            scenario_id: scenario_id.into(),
            invariant_family: invariant_family.into(),
        }
    }
}

#[derive(Debug)]
struct OwnedFaultHookArm {
    owner_session_id: u64,
    arm: FaultHookArm,
}

impl OwnedFaultHookArm {
    const fn new(owner_session_id: u64, arm: FaultHookArm) -> Self {
        Self {
            owner_session_id,
            arm,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultInjectionRecord {
    pub trigger_seq: u64,
    pub point: &'static str,
    pub run_id: String,
    pub scenario_id: String,
    pub invariant_family: String,
    pub detail: String,
}

#[derive(Debug, Default)]
struct PagerFaultHookState {
    next_trigger_seq: u64,
    after_flush_before_publish: Option<OwnedFaultHookArm>,
    during_phase_c: Option<OwnedFaultHookArm>,
    drop_condvar_notify: Option<OwnedFaultHookArm>,
    vacuum_after_target_page: Option<OwnedFaultHookArm>,
    vacuum_before_commit_marker: Option<OwnedFaultHookArm>,
    records: Vec<FaultInjectionRecord>,
}

/// Arm a one-shot failure after VACUUM has overwritten one target page while
/// the complete rollback journal is hot.
pub fn arm_vacuum_after_target_page(arm: FaultHookArm) {
    let mut state = PAGER_FAULT_HOOK_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(owner_session_id) = current_fault_hook_session_id() else {
        return;
    };
    state.vacuum_after_target_page = Some(OwnedFaultHookArm::new(owner_session_id, arm));
}

pub(crate) fn maybe_inject_vacuum_after_target_page(page_number: u32) -> Result<()> {
    let mut state = PAGER_FAULT_HOOK_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(arm) = take_owned_hook_for_current_thread(&mut state.vacuum_after_target_page) else {
        return Ok(());
    };
    record_trigger(
        &mut state,
        &arm,
        "vacuum_after_target_page",
        format!("page_number={page_number}"),
    );
    Err(FrankenError::Io(std::io::Error::other(format!(
        "fault_inject:vacuum_after_target_page run_id={} scenario_id={} invariant_family={}",
        arm.run_id, arm.scenario_id, arm.invariant_family
    ))))
}

/// Arm a one-shot failure after the replacement image is fully durable and
/// verified but before the hot journal is invalidated as the commit marker.
pub fn arm_vacuum_before_commit_marker(arm: FaultHookArm) {
    let mut state = PAGER_FAULT_HOOK_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(owner_session_id) = current_fault_hook_session_id() else {
        return;
    };
    state.vacuum_before_commit_marker = Some(OwnedFaultHookArm::new(owner_session_id, arm));
}

pub(crate) fn maybe_inject_vacuum_before_commit_marker() -> Result<()> {
    let mut state = PAGER_FAULT_HOOK_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(arm) = take_owned_hook_for_current_thread(&mut state.vacuum_before_commit_marker)
    else {
        return Ok(());
    };
    record_trigger(
        &mut state,
        &arm,
        "vacuum_before_commit_marker",
        "replacement image durable and verified".to_owned(),
    );
    Err(FrankenError::Io(std::io::Error::other(format!(
        "fault_inject:vacuum_before_commit_marker run_id={} scenario_id={} invariant_family={}",
        arm.run_id, arm.scenario_id, arm.invariant_family
    ))))
}

static PAGER_FAULT_HOOK_STATE: LazyLock<Mutex<PagerFaultHookState>> =
    LazyLock::new(|| Mutex::new(PagerFaultHookState::default()));

static FAULT_INJECTION_SESSION_LOCK: Mutex<()> = Mutex::new(());
static NEXT_FAULT_INJECTION_SESSION_ID: AtomicU64 = AtomicU64::new(1);
static ACTIVE_FAULT_INJECTION_SESSION_ID: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static THREAD_FAULT_INJECTION_SESSION_ID: Cell<u64> = const { Cell::new(0) };
}

/// Shared lock for deterministic fault-injection scenarios.
///
/// Every test or harness that arms process-global hooks should acquire this
/// lock before configuring the scenario. The acquiring thread participates
/// automatically; worker threads must enter with a token obtained from
/// [`FaultInjectionSession::participant`].
#[derive(Debug, Clone, Copy, Default)]
pub struct FaultInjectionSessionLock;

impl FaultInjectionSessionLock {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Acquire exclusive ownership of the process-global pager fault hooks.
    ///
    /// Poisoning is recovered internally because the returned RAII session
    /// resets all hook state before use. `LockResult` is retained so this type
    /// can replace existing `Mutex<()>` guards without bespoke call sites.
    pub fn lock(&self) -> LockResult<FaultInjectionSession> {
        let guard = FAULT_INJECTION_SESSION_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut state = PAGER_FAULT_HOOK_STATE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let session_id = next_fault_injection_session_id();
        let previous_session_id =
            ACTIVE_FAULT_INJECTION_SESSION_ID.swap(session_id, Ordering::AcqRel);
        debug_assert_eq!(
            previous_session_id, 0,
            "fault session lock held while another session is active"
        );
        let previous_thread_session_id =
            THREAD_FAULT_INJECTION_SESSION_ID.with(|active| active.replace(session_id));
        *state = PagerFaultHookState::default();
        drop(state);

        Ok(FaultInjectionSession {
            _guard: guard,
            session_id,
            previous_thread_session_id,
        })
    }
}

/// RAII ownership of all process-global pager fault hooks.
///
/// Hook checks on unrelated threads are no-ops while this value is alive.
/// Dropping it clears all arms and records, including during panic unwinding.
#[must_use = "dropping the session immediately removes fault-hook isolation"]
pub struct FaultInjectionSession {
    _guard: MutexGuard<'static, ()>,
    session_id: u64,
    previous_thread_session_id: u64,
}

impl FaultInjectionSession {
    /// Create a token that lets one worker thread join this fault scenario.
    #[must_use]
    pub const fn participant(&self) -> FaultInjectionParticipant {
        FaultInjectionParticipant {
            session_id: self.session_id,
        }
    }
}

impl std::fmt::Debug for FaultInjectionSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FaultInjectionSession")
            .field("session_id", &self.session_id)
            .finish_non_exhaustive()
    }
}

impl Drop for FaultInjectionSession {
    fn drop(&mut self) {
        // Serialize deactivation with every hook mutation and consumption. Once
        // this lock is released, late participants see an inactive generation
        // and cannot recreate a globally consumable arm.
        let mut state = PAGER_FAULT_HOOK_STATE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let active_session_id = ACTIVE_FAULT_INJECTION_SESSION_ID.swap(0, Ordering::AcqRel);
        debug_assert_eq!(
            active_session_id, self.session_id,
            "dropping a pager fault session that is not active"
        );
        *state = PagerFaultHookState::default();
        drop(state);
        THREAD_FAULT_INJECTION_SESSION_ID.with(|active| {
            active.set(self.previous_thread_session_id);
        });
    }
}

/// Transferable permission for a worker thread to join a fault session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FaultInjectionParticipant {
    session_id: u64,
}

impl FaultInjectionParticipant {
    /// Enter the associated fault session on the current thread.
    ///
    /// The returned guard restores the thread's prior participation on drop.
    #[must_use = "the participation guard must live for the fault-injected operation"]
    pub fn enter(self) -> FaultInjectionParticipantGuard {
        let active_session_id = ACTIVE_FAULT_INJECTION_SESSION_ID.load(Ordering::Acquire);
        assert_eq!(
            active_session_id, self.session_id,
            "cannot enter an inactive pager fault session"
        );
        let previous_session_id =
            THREAD_FAULT_INJECTION_SESSION_ID.with(|active| active.replace(self.session_id));
        FaultInjectionParticipantGuard {
            session_id: self.session_id,
            previous_session_id,
            _not_send: std::marker::PhantomData,
        }
    }
}

/// Thread-bound guard returned by [`FaultInjectionParticipant::enter`].
pub struct FaultInjectionParticipantGuard {
    session_id: u64,
    previous_session_id: u64,
    _not_send: std::marker::PhantomData<Rc<()>>,
}

impl std::fmt::Debug for FaultInjectionParticipantGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FaultInjectionParticipantGuard")
            .field("session_id", &self.session_id)
            .finish_non_exhaustive()
    }
}

impl Drop for FaultInjectionParticipantGuard {
    fn drop(&mut self) {
        THREAD_FAULT_INJECTION_SESSION_ID.with(|active| {
            debug_assert_eq!(
                active.get(),
                self.session_id,
                "dropping a pager fault participant outside its session"
            );
            active.set(self.previous_session_id);
        });
    }
}

fn next_fault_injection_session_id() -> u64 {
    loop {
        let session_id = NEXT_FAULT_INJECTION_SESSION_ID.fetch_add(1, Ordering::Relaxed);
        if session_id != 0 {
            return session_id;
        }
    }
}

fn current_fault_hook_session_id() -> Option<u64> {
    let active_session_id = ACTIVE_FAULT_INJECTION_SESSION_ID.load(Ordering::Acquire);
    let thread_session_id = THREAD_FAULT_INJECTION_SESSION_ID.with(Cell::get);
    match (active_session_id, thread_session_id) {
        (0, 0) => Some(0),
        (active, thread) if active == thread => Some(active),
        _ => None,
    }
}

fn take_owned_hook_for_current_thread(
    hook: &mut Option<OwnedFaultHookArm>,
) -> Option<FaultHookArm> {
    let current_session_id = current_fault_hook_session_id()?;
    if hook.as_ref()?.owner_session_id != current_session_id {
        return None;
    }
    hook.take().map(|owned| owned.arm)
}

pub fn clear() {
    let mut state = PAGER_FAULT_HOOK_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if current_fault_hook_session_id().is_none() {
        return;
    }
    *state = PagerFaultHookState::default();
}

#[must_use]
pub fn take_records() -> Vec<FaultInjectionRecord> {
    let mut state = PAGER_FAULT_HOOK_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if current_fault_hook_session_id().is_none() {
        return Vec::new();
    }
    mem::take(&mut state.records)
}

pub fn arm_after_flush_before_publish(arm: FaultHookArm) {
    let mut state = PAGER_FAULT_HOOK_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(owner_session_id) = current_fault_hook_session_id() else {
        return;
    };
    state.after_flush_before_publish = Some(OwnedFaultHookArm::new(owner_session_id, arm));
}

pub(crate) fn maybe_inject_after_flush_before_publish(
    flush_epoch: u64,
    batch_count: usize,
    frame_count: usize,
) -> Result<()> {
    let mut state = PAGER_FAULT_HOOK_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(arm) = take_owned_hook_for_current_thread(&mut state.after_flush_before_publish)
    else {
        return Ok(());
    };

    let detail =
        format!("flush_epoch={flush_epoch} batch_count={batch_count} frame_count={frame_count}");
    record_trigger(&mut state, &arm, "after_flush_before_publish", detail);
    Err(FrankenError::Io(std::io::Error::other(format!(
        "fault_inject:after_flush_before_publish run_id={} scenario_id={} invariant_family={}",
        arm.run_id, arm.scenario_id, arm.invariant_family
    ))))
}

/// Arm the Phase-C publication fault hook (F4 / H4).
///
/// When armed, `maybe_inject_during_phase_c()` fires once inside
/// `SimpleTransaction::commit()` after commit_seq is updated but before
/// snapshot publish completes. Simulates crash during metadata publication.
pub fn arm_during_phase_c(arm: FaultHookArm) {
    let mut state = PAGER_FAULT_HOOK_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(owner_session_id) = current_fault_hook_session_id() else {
        return;
    };
    state.during_phase_c = Some(OwnedFaultHookArm::new(owner_session_id, arm));
}

/// Check and fire the Phase-C publication fault hook.
pub(crate) fn maybe_inject_during_phase_c(commit_seq: u64, db_size: u32) -> Result<()> {
    let mut state = PAGER_FAULT_HOOK_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(arm) = take_owned_hook_for_current_thread(&mut state.during_phase_c) else {
        return Ok(());
    };

    let detail = format!("commit_seq={commit_seq} db_size={db_size}");
    record_trigger(&mut state, &arm, "during_phase_c", detail);
    Err(FrankenError::Io(std::io::Error::other(format!(
        "fault_inject:during_phase_c run_id={} scenario_id={} invariant_family={}",
        arm.run_id, arm.scenario_id, arm.invariant_family
    ))))
}

/// Arm the dropped-condvar-notify fault hook (F11 / H11).
///
/// When armed, `maybe_inject_drop_condvar_notify()` returns true,
/// signaling the caller to suppress the `Condvar::notify_all()` that normally
/// follows a successful `publish_completed_epoch()` store.  The completed epoch
/// is still published; only the wakeup is dropped so waiters must recover via
/// timeout-based rechecks.
pub fn arm_drop_condvar_notify(arm: FaultHookArm) {
    let mut state = PAGER_FAULT_HOOK_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(owner_session_id) = current_fault_hook_session_id() else {
        return;
    };
    state.drop_condvar_notify = Some(OwnedFaultHookArm::new(owner_session_id, arm));
}

/// Check and fire the dropped-condvar-notify hook.
///
/// Returns `true` if the hook fires (caller should suppress the notify).
pub(crate) fn maybe_inject_drop_condvar_notify(completed_epoch: u64) -> bool {
    let mut state = PAGER_FAULT_HOOK_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(arm) = take_owned_hook_for_current_thread(&mut state.drop_condvar_notify) else {
        return false;
    };

    let detail = format!("completed_epoch={completed_epoch}");
    record_trigger(&mut state, &arm, "drop_condvar_notify", detail);
    true
}

fn record_trigger(
    state: &mut PagerFaultHookState,
    arm: &FaultHookArm,
    point: &'static str,
    detail: String,
) {
    state.next_trigger_seq = state.next_trigger_seq.saturating_add(1);
    let record = FaultInjectionRecord {
        trigger_seq: state.next_trigger_seq,
        point,
        run_id: arm.run_id.clone(),
        scenario_id: arm.scenario_id.clone(),
        invariant_family: arm.invariant_family.clone(),
        detail,
    };
    warn!(
        target: "fsqlite_pager::fault_injection",
        trigger_seq = record.trigger_seq,
        point = record.point,
        run_id = %record.run_id,
        scenario_id = %record.scenario_id,
        invariant_family = %record.invariant_family,
        detail = %record.detail,
        "fault hook fired"
    );
    state.records.push(record);
}

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_GUARD: FaultInjectionSessionLock = FaultInjectionSessionLock::new();

    fn arm(point: &str) -> FaultHookArm {
        FaultHookArm::new(format!("test-{point}"), format!("scenario-{point}"), "unit")
    }

    #[test]
    fn scoped_session_prevents_nonparticipant_thread_from_stealing_hook() {
        let session = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        arm_after_flush_before_publish(arm("owned"));

        let unrelated_result =
            std::thread::spawn(|| maybe_inject_after_flush_before_publish(1, 1, 1))
                .join()
                .expect("unrelated thread should not panic");
        assert!(
            unrelated_result.is_ok(),
            "a nonparticipant thread must not consume the armed hook"
        );
        assert!(
            take_records().is_empty(),
            "a nonparticipant thread must not record a trigger"
        );

        let participant = session.participant();
        let participant_result = std::thread::spawn(move || {
            let _participation = participant.enter();
            maybe_inject_after_flush_before_publish(2, 3, 5)
        })
        .join()
        .expect("participant thread should not panic");
        assert!(
            participant_result.is_err(),
            "a participant thread must consume the armed hook"
        );

        let records = take_records();
        assert_eq!(records.len(), 1, "the hook must fire exactly once");
        assert_eq!(records[0].run_id, "test-owned");
        assert!(records[0].detail.contains("flush_epoch=2"));
        assert!(records[0].detail.contains("batch_count=3"));
        assert!(records[0].detail.contains("frame_count=5"));
    }

    #[test]
    fn session_owned_hook_is_inert_while_owner_generation_is_inactive() {
        let session = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        arm_after_flush_before_publish(arm("owned-generation"));

        let session_id = session.session_id;
        let previous_session_id = ACTIVE_FAULT_INJECTION_SESSION_ID.swap(0, Ordering::AcqRel);
        assert_eq!(previous_session_id, session_id);

        let unrelated_result =
            std::thread::spawn(|| maybe_inject_after_flush_before_publish(1, 1, 1)).join();

        let inactive_session_id =
            ACTIVE_FAULT_INJECTION_SESSION_ID.swap(session_id, Ordering::AcqRel);
        assert_eq!(inactive_session_id, 0);
        let unrelated_result = unrelated_result.expect("unrelated thread should not panic");
        assert!(
            unrelated_result.is_ok(),
            "an inactive session's arm must not become a legacy-global hook"
        );

        assert!(
            maybe_inject_after_flush_before_publish(2, 2, 2).is_err(),
            "the reactivated owner must retain exclusive access to its arm"
        );
    }

    #[test]
    fn inactive_session_context_cannot_arm_a_legacy_global_hook() {
        let session = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let session_id = session.session_id;
        let previous_session_id = ACTIVE_FAULT_INJECTION_SESSION_ID.swap(0, Ordering::AcqRel);
        assert_eq!(previous_session_id, session_id);

        arm_after_flush_before_publish(arm("stale-participant"));

        let inactive_session_id =
            ACTIVE_FAULT_INJECTION_SESSION_ID.swap(session_id, Ordering::AcqRel);
        assert_eq!(inactive_session_id, 0);
        assert!(
            PAGER_FAULT_HOOK_STATE
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .after_flush_before_publish
                .is_none(),
            "a stale enrolled context must not create an owner-zero hook"
        );
    }

    #[test]
    fn entered_participant_cannot_cross_session_generation() {
        let first_session = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let stale_participant = first_session.participant();
        let participant_entered = std::sync::Arc::new(std::sync::Barrier::new(2));
        let next_generation_active = std::sync::Arc::new(std::sync::Barrier::new(2));

        let worker_entered = std::sync::Arc::clone(&participant_entered);
        let worker_next_generation = std::sync::Arc::clone(&next_generation_active);
        let worker = std::thread::spawn(move || {
            let _participation = stale_participant.enter();
            worker_entered.wait();
            worker_next_generation.wait();

            arm_after_flush_before_publish(arm("stale-generation"));
            maybe_inject_after_flush_before_publish(1, 1, 1)
        });

        participant_entered.wait();
        drop(first_session);

        let _second_session = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        arm_after_flush_before_publish(arm("current-generation"));
        next_generation_active.wait();

        let stale_result = worker.join().expect("stale participant must not panic");
        assert!(
            stale_result.is_ok(),
            "a participant from the previous generation must not consume the new arm"
        );
        assert!(
            maybe_inject_after_flush_before_publish(2, 2, 2).is_err(),
            "the current generation must retain its arm"
        );
        let records = take_records();
        assert_eq!(records.len(), 1, "only the current arm may fire");
        assert_eq!(records[0].run_id, "test-current-generation");
    }

    #[test]
    fn session_panic_unwind_clears_arms_and_recovers_lock() {
        let outcome = std::panic::catch_unwind(|| {
            let _session = TEST_GUARD
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            arm_after_flush_before_publish(arm("panic-owned"));
            panic!("intentional fault-session unwind");
        });
        assert!(outcome.is_err(), "the test panic must be observed");

        let recovered_session = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            maybe_inject_after_flush_before_publish(1, 1, 1).is_ok(),
            "an arm from the unwound session must be cleared"
        );
        assert!(
            take_records().is_empty(),
            "an unwound session must not retain evidence records"
        );
        // The panic intentionally poisoned the raw std mutex. Prove that the
        // public session API recovered it, then restore pristine global state
        // before releasing the mutex so no waiter can inherit the poison.
        FAULT_INJECTION_SESSION_LOCK.clear_poison();
        drop(recovered_session);
    }

    #[test]
    fn legacy_global_hook_remains_consumable_across_threads() {
        let _serialization = FAULT_INJECTION_SESSION_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(
            ACTIVE_FAULT_INJECTION_SESSION_ID.load(Ordering::Acquire),
            0,
            "the raw serialization guard must not activate a scoped session"
        );
        clear();
        arm_after_flush_before_publish(arm("legacy-global"));

        let result = std::thread::spawn(|| maybe_inject_after_flush_before_publish(3, 5, 8))
            .join()
            .expect("legacy consumer thread must not panic");
        assert!(
            result.is_err(),
            "owner-zero hooks must preserve their legacy cross-thread behavior"
        );
        let records = take_records();
        assert_eq!(records.len(), 1, "the legacy hook must fire exactly once");
        assert_eq!(records[0].run_id, "test-legacy-global");
        clear();
    }

    #[test]
    fn test_clear_resets_all_armed_hooks_and_records() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_after_flush_before_publish(arm("flush"));
        arm_during_phase_c(arm("phase_c"));
        arm_drop_condvar_notify(arm("condvar"));
        let _ = maybe_inject_after_flush_before_publish(1, 1, 1);
        assert_eq!(take_records().len(), 1);

        clear();
        assert!(
            maybe_inject_after_flush_before_publish(1, 1, 1).is_ok(),
            "cleared flush hook must not fire"
        );
        assert!(
            maybe_inject_during_phase_c(1, 1).is_ok(),
            "cleared phase_c hook must not fire"
        );
        assert!(
            !maybe_inject_drop_condvar_notify(1),
            "cleared condvar hook must not fire"
        );
        assert!(take_records().is_empty(), "clear must reset records");
    }

    #[test]
    fn test_armed_hook_fires_once_then_disarms() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_during_phase_c(arm("phase_c"));
        let err = maybe_inject_during_phase_c(42, 10);
        assert!(err.is_err(), "armed hook should return Err on first call");
        assert!(
            err.unwrap_err()
                .to_string()
                .contains("fault_inject:during_phase_c"),
        );

        assert!(
            maybe_inject_during_phase_c(43, 11).is_ok(),
            "disarmed hook must not fire on second call"
        );
    }

    #[test]
    fn test_drop_condvar_notify_returns_true_once() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_drop_condvar_notify(arm("condvar"));
        assert!(maybe_inject_drop_condvar_notify(1), "first call fires");
        assert!(
            !maybe_inject_drop_condvar_notify(2),
            "second call does not fire"
        );
    }

    #[test]
    fn test_records_capture_trigger_details() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_after_flush_before_publish(FaultHookArm::new("run-1", "scen-1", "inv-1"));
        let _ = maybe_inject_after_flush_before_publish(7, 3, 12);

        arm_during_phase_c(FaultHookArm::new("run-2", "scen-2", "inv-2"));
        let _ = maybe_inject_during_phase_c(99, 50);

        let records = take_records();
        assert_eq!(records.len(), 2);

        assert_eq!(records[0].point, "after_flush_before_publish");
        assert_eq!(records[0].run_id, "run-1");
        assert!(records[0].detail.contains("flush_epoch=7"));
        assert!(records[0].detail.contains("batch_count=3"));
        assert!(records[0].detail.contains("frame_count=12"));

        assert_eq!(records[1].point, "during_phase_c");
        assert_eq!(records[1].run_id, "run-2");
        assert!(records[1].detail.contains("commit_seq=99"));
        assert!(records[1].detail.contains("db_size=50"));

        assert_eq!(records[0].trigger_seq, 1);
        assert_eq!(records[1].trigger_seq, 2);
    }

    #[test]
    fn test_flush_hook_fires_once_then_disarms() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_after_flush_before_publish(arm("flush"));
        let err = maybe_inject_after_flush_before_publish(5, 2, 8);
        assert!(err.is_err());
        assert!(
            err.unwrap_err()
                .to_string()
                .contains("fault_inject:after_flush_before_publish"),
        );

        assert!(
            maybe_inject_after_flush_before_publish(6, 3, 9).is_ok(),
            "disarmed flush hook must not fire on second call"
        );
    }

    #[test]
    fn test_condvar_notify_record_captures_detail() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_drop_condvar_notify(FaultHookArm::new("run-cv", "scen-cv", "inv-cv"));
        assert!(maybe_inject_drop_condvar_notify(77));

        let records = take_records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].point, "drop_condvar_notify");
        assert_eq!(records[0].run_id, "run-cv");
        assert_eq!(records[0].scenario_id, "scen-cv");
        assert_eq!(records[0].invariant_family, "inv-cv");
        assert!(records[0].detail.contains("completed_epoch=77"));
    }

    #[test]
    fn test_all_hooks_fire_independently() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_after_flush_before_publish(arm("flush"));
        arm_during_phase_c(arm("phase_c"));
        arm_drop_condvar_notify(arm("condvar"));

        assert!(maybe_inject_after_flush_before_publish(1, 1, 1).is_err());
        assert!(maybe_inject_during_phase_c(2, 2).is_err());
        assert!(maybe_inject_drop_condvar_notify(3));

        let records = take_records();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].point, "after_flush_before_publish");
        assert_eq!(records[1].point, "during_phase_c");
        assert_eq!(records[2].point, "drop_condvar_notify");
        assert_eq!(records[0].trigger_seq, 1);
        assert_eq!(records[1].trigger_seq, 2);
        assert_eq!(records[2].trigger_seq, 3);
    }

    #[test]
    fn test_unarmed_hooks_are_noop() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        assert!(maybe_inject_after_flush_before_publish(1, 1, 1).is_ok());
        assert!(maybe_inject_during_phase_c(1, 1).is_ok());
        assert!(!maybe_inject_drop_condvar_notify(1));
        assert!(take_records().is_empty());
    }

    #[test]
    fn test_fault_hook_arm_equality() {
        let a = FaultHookArm::new("r1", "s1", "inv1");
        let b = FaultHookArm::new("r1", "s1", "inv1");
        let c = FaultHookArm::new("r2", "s1", "inv1");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_take_records_drains_and_is_empty_after() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_drop_condvar_notify(arm("condvar"));
        let _ = maybe_inject_drop_condvar_notify(1);

        let first = take_records();
        assert_eq!(first.len(), 1);
        let second = take_records();
        assert!(second.is_empty(), "take_records must drain");
    }

    #[test]
    fn test_rearming_overwrites_previous_arm() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_during_phase_c(FaultHookArm::new("first", "s1", "inv1"));
        arm_during_phase_c(FaultHookArm::new("second", "s2", "inv2"));

        let _ = maybe_inject_during_phase_c(1, 1);
        let records = take_records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].run_id, "second", "re-arm must overwrite first");
    }

    #[test]
    fn test_trigger_seq_monotonic_across_cycles() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_after_flush_before_publish(arm("flush"));
        let _ = maybe_inject_after_flush_before_publish(1, 1, 1);
        let r1 = take_records();

        arm_during_phase_c(arm("phase_c"));
        let _ = maybe_inject_during_phase_c(2, 2);
        let r2 = take_records();

        assert!(
            r2[0].trigger_seq > r1[0].trigger_seq,
            "trigger_seq must increase across take_records drains"
        );
    }

    #[test]
    fn test_fault_hook_arm_new_maps_fields_correctly() {
        let a = FaultHookArm::new("my-run", "my-scenario", "my-invariant");
        assert_eq!(a.run_id, "my-run");
        assert_eq!(a.scenario_id, "my-scenario");
        assert_eq!(a.invariant_family, "my-invariant");
    }

    #[test]
    fn test_fault_injection_record_fields_from_condvar_hook() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_drop_condvar_notify(FaultHookArm::new("r", "s", "i"));
        assert!(maybe_inject_drop_condvar_notify(999));

        let records = take_records();
        assert_eq!(records.len(), 1);
        let rec = &records[0];
        assert_eq!(rec.point, "drop_condvar_notify");
        assert_eq!(rec.run_id, "r");
        assert_eq!(rec.scenario_id, "s");
        assert_eq!(rec.invariant_family, "i");
        assert!(rec.detail.contains("completed_epoch=999"));
        assert!(rec.trigger_seq > 0);
    }

    #[test]
    fn test_fault_injection_record_clone_and_eq() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_during_phase_c(FaultHookArm::new("rc", "sc", "ic"));
        let _ = maybe_inject_during_phase_c(42, 7);
        let records = take_records();
        let original = &records[0];
        let cloned = original.clone();
        assert_eq!(*original, cloned);
        assert_eq!(cloned.trigger_seq, original.trigger_seq);
        assert_eq!(cloned.point, "during_phase_c");
        assert_eq!(cloned.detail, original.detail);
    }

    #[test]
    fn test_fault_hook_arm_clone_and_debug() {
        let a = FaultHookArm::new("run-dbg", "scen-dbg", "inv-dbg");
        let b = a.clone();
        assert_eq!(a, b);
        let dbg = format!("{a:?}");
        assert!(dbg.contains("run-dbg"));
        assert!(dbg.contains("scen-dbg"));
        assert!(dbg.contains("inv-dbg"));
    }

    #[test]
    fn test_flush_error_message_includes_all_arm_fields() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_after_flush_before_publish(FaultHookArm::new("R1", "S1", "INV1"));
        let err = maybe_inject_after_flush_before_publish(1, 1, 1).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("run_id=R1"), "error must include run_id");
        assert!(
            msg.contains("scenario_id=S1"),
            "error must include scenario_id"
        );
        assert!(
            msg.contains("invariant_family=INV1"),
            "error must include invariant_family"
        );
    }

    #[test]
    fn test_flush_hook_boundary_zero_counts() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_after_flush_before_publish(arm("zero"));
        let err = maybe_inject_after_flush_before_publish(0, 0, 0);
        assert!(err.is_err());
        let records = take_records();
        assert_eq!(records.len(), 1);
        assert!(records[0].detail.contains("flush_epoch=0"));
        assert!(records[0].detail.contains("batch_count=0"));
        assert!(records[0].detail.contains("frame_count=0"));
    }

    #[test]
    fn fault_hook_arm_clone_eq_debug() {
        let a = FaultHookArm::new("run1", "scen1", "inv1");
        let b = a.clone();
        assert_eq!(a, b);
        let c = FaultHookArm::new("run2", "scen1", "inv1");
        assert_ne!(a, c);
        let dbg = format!("{a:?}");
        assert!(dbg.contains("FaultHookArm"));
        assert!(dbg.contains("run1"));
    }

    #[test]
    fn fault_injection_record_clone_eq_debug() {
        let r = FaultInjectionRecord {
            trigger_seq: 7,
            point: "test_point",
            run_id: "r1".to_owned(),
            scenario_id: "s1".to_owned(),
            invariant_family: "inv".to_owned(),
            detail: "some detail".to_owned(),
        };
        let cloned = r.clone();
        assert_eq!(r, cloned);
        let other = FaultInjectionRecord {
            trigger_seq: 8,
            ..r.clone()
        };
        assert_ne!(r, other);
        let dbg = format!("{r:?}");
        assert!(dbg.contains("FaultInjectionRecord"));
        assert!(dbg.contains("test_point"));
    }

    #[test]
    fn clear_resets_and_take_records_returns_empty() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();
        let records = take_records();
        assert!(records.is_empty());
    }

    #[test]
    fn fault_hook_arm_new_stores_fields() {
        let arm = FaultHookArm::new("rid", "sid", "ifam");
        assert_eq!(arm.run_id, "rid");
        assert_eq!(arm.scenario_id, "sid");
        assert_eq!(arm.invariant_family, "ifam");
    }
}
