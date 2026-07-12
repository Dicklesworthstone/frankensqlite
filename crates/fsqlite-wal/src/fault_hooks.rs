//! Targeted fault hooks for batched WAL append and sync verification.
//!
//! Hooks are process-global unless the caller holds a
//! [`FaultInjectionSession`]. A session serializes fault-injection scenarios,
//! scopes armed hooks to the owning thread, and clears all hook state on drop.
//! This prevents unrelated concurrent work from stealing a one-shot hook while
//! preserving the original process-global behavior for unscoped callers.

use std::mem;
use std::sync::{LazyLock, LockResult, Mutex, MutexGuard};
use std::thread::{self, ThreadId};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::flags::SyncFlags;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultInjectionRecord {
    pub trigger_seq: u64,
    pub point: &'static str,
    pub run_id: String,
    pub scenario_id: String,
    pub invariant_family: String,
    pub detail: String,
}

#[derive(Debug, Clone)]
struct CountdownFaultArm {
    arm: FaultHookArm,
    remaining_invocations: u32,
}

#[derive(Debug, Default)]
struct WalFaultHookState {
    next_trigger_seq: u64,
    after_append: Option<FaultHookArm>,
    sync_failure: Option<FaultHookArm>,
    append_busy: Option<CountdownFaultArm>,
    crash_header_truncate: Option<FaultHookArm>,
    records: Vec<FaultInjectionRecord>,
}

static WAL_FAULT_HOOK_STATE: LazyLock<Mutex<WalFaultHookState>> =
    LazyLock::new(|| Mutex::new(WalFaultHookState::default()));

static FAULT_INJECTION_SESSION_LOCK: Mutex<()> = Mutex::new(());
static FAULT_INJECTION_SESSION_OWNER: LazyLock<Mutex<Option<ThreadId>>> =
    LazyLock::new(|| Mutex::new(None));

/// Shared lock for deterministic fault-injection scenarios.
///
/// Every test or harness that arms process-global hooks should acquire this
/// lock before configuring the scenario. The returned session is deliberately
/// not transferable between threads: the synchronous WAL operation under test
/// must run on the same thread that acquired it.
#[derive(Debug, Clone, Copy, Default)]
pub struct FaultInjectionSessionLock;

impl FaultInjectionSessionLock {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Acquire exclusive ownership of the process-global fault hooks.
    ///
    /// Poisoning is recovered internally because the RAII session resets all
    /// hook state before returning. `LockResult` is retained so this type can
    /// replace existing `Mutex<()>` test guards without bespoke wrappers.
    pub fn lock(&self) -> LockResult<FaultInjectionSession> {
        let guard = FAULT_INJECTION_SESSION_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let owner = thread::current().id();

        {
            let mut active_owner = FAULT_INJECTION_SESSION_OWNER
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            debug_assert!(
                active_owner.is_none(),
                "fault session lock held while another owner is registered"
            );
            *active_owner = Some(owner);
        }

        clear();
        clear_crash_boundary();

        Ok(FaultInjectionSession {
            _guard: guard,
            owner,
        })
    }
}

/// RAII ownership of all process-global WAL fault hooks.
///
/// While this value is alive, hook checks on non-owner threads are no-ops.
/// Dropping it clears both hook families, including during panic unwinding.
#[must_use = "dropping the session immediately removes fault-hook isolation"]
pub struct FaultInjectionSession {
    _guard: MutexGuard<'static, ()>,
    owner: ThreadId,
}

impl std::fmt::Debug for FaultInjectionSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FaultInjectionSession")
            .field("owner", &self.owner)
            .finish_non_exhaustive()
    }
}

impl Drop for FaultInjectionSession {
    fn drop(&mut self) {
        clear();
        clear_crash_boundary();

        let mut active_owner = FAULT_INJECTION_SESSION_OWNER
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *active_owner = None;
    }
}

fn fault_hook_applies_to_current_thread() -> bool {
    let active_owner = FAULT_INJECTION_SESSION_OWNER
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match *active_owner {
        Some(owner) => owner == thread::current().id(),
        None => true,
    }
}

pub fn clear() {
    let mut state = WAL_FAULT_HOOK_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *state = WalFaultHookState::default();
}

#[must_use]
pub fn take_records() -> Vec<FaultInjectionRecord> {
    let mut state = WAL_FAULT_HOOK_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    mem::take(&mut state.records)
}

pub fn arm_after_append(arm: FaultHookArm) {
    let mut state = WAL_FAULT_HOOK_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state.after_append = Some(arm);
}

pub fn arm_sync_failure(arm: FaultHookArm) {
    let mut state = WAL_FAULT_HOOK_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state.sync_failure = Some(arm);
}

pub fn arm_append_busy_countdown(arm: FaultHookArm, fire_on_nth_invocation: u32) {
    let mut state = WAL_FAULT_HOOK_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state.append_busy = Some(CountdownFaultArm {
        arm,
        remaining_invocations: fire_on_nth_invocation.max(1),
    });
}

pub(crate) fn maybe_inject_append_busy(
    frame_count_before: usize,
    submitted_frames: usize,
) -> Result<()> {
    if !fault_hook_applies_to_current_thread() {
        return Ok(());
    }

    let mut state = WAL_FAULT_HOOK_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(mut countdown) = state.append_busy.take() else {
        return Ok(());
    };
    if countdown.remaining_invocations > 1 {
        countdown.remaining_invocations -= 1;
        state.append_busy = Some(countdown);
        return Ok(());
    }

    let detail =
        format!("frame_count_before={frame_count_before} submitted_frames={submitted_frames}");
    record_trigger(
        &mut state,
        &countdown.arm,
        "wal_append_busy_countdown",
        detail,
    );
    Err(FrankenError::Busy)
}

pub(crate) fn maybe_inject_after_append(
    frame_count_before: usize,
    appended_frames: usize,
) -> Result<()> {
    if !fault_hook_applies_to_current_thread() {
        return Ok(());
    }

    let mut state = WAL_FAULT_HOOK_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(arm) = state.after_append.take() else {
        return Ok(());
    };

    let detail =
        format!("frame_count_before={frame_count_before} appended_frames={appended_frames}");
    record_trigger(&mut state, &arm, "wal_after_append", detail);
    Err(fault_error("wal_after_append", &arm))
}

pub(crate) fn maybe_inject_sync_failure(frame_count_before: usize, flags: SyncFlags) -> Result<()> {
    if !fault_hook_applies_to_current_thread() {
        return Ok(());
    }

    let mut state = WAL_FAULT_HOOK_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(arm) = state.sync_failure.take() else {
        return Ok(());
    };

    let detail = format!("frame_count_before={frame_count_before} flags={flags:?}");
    record_trigger(&mut state, &arm, "wal_sync_failure", detail);
    Err(fault_error("wal_sync_failure", &arm))
}

fn record_trigger(
    state: &mut WalFaultHookState,
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
        target: "fsqlite_wal::fault_injection",
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

/// Arm the crash-between-header-and-truncate hook (F9 / H9).
///
/// When armed, `maybe_inject_crash_header_truncate()` fires once after
/// the new WAL header is written but before the file is truncated.
/// This simulates a crash that leaves new-generation salts in the header
/// but old-generation frames still on disk.
pub fn arm_crash_header_truncate(arm: FaultHookArm) {
    let mut state = WAL_FAULT_HOOK_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state.crash_header_truncate = Some(arm);
}

/// Check and fire the crash-between-header-and-truncate hook.
///
/// Called inside `WalFile::reset()` after the new header is written
/// and synced but before the file is truncated.
pub(crate) fn maybe_inject_crash_header_truncate(
    old_frame_count: usize,
    new_checkpoint_seq: u32,
) -> Result<()> {
    if !fault_hook_applies_to_current_thread() {
        return Ok(());
    }

    let mut state = WAL_FAULT_HOOK_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(arm) = state.crash_header_truncate.take() else {
        return Ok(());
    };

    let detail =
        format!("old_frame_count={old_frame_count} new_checkpoint_seq={new_checkpoint_seq}");
    record_trigger(&mut state, &arm, "wal_crash_header_truncate", detail);
    Err(fault_error("wal_crash_header_truncate", &arm))
}

fn fault_error(point: &str, arm: &FaultHookArm) -> FrankenError {
    FrankenError::Io(std::io::Error::other(format!(
        "fault_inject:{point} run_id={} scenario_id={} invariant_family={}",
        arm.run_id, arm.scenario_id, arm.invariant_family
    )))
}

/// The 8 crash-injection phase boundaries for WAL durability testing (bd-hd2he).
///
/// Each boundary represents a point in the commit protocol where a crash
/// (SIGKILL, power loss) could leave the system in a partially committed state.
/// The crash injection harness arms one boundary per test iteration to verify
/// that recovery always produces a consistent state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CrashBoundary {
    /// Before the WAL header is written during reset/creation.
    BeforeWalHeaderWrite,
    /// Before WAL frame bytes are appended to disk.
    BeforeWalFrameAppend,
    /// After WAL frames are appended but before fsync (frames on disk but not durable).
    AfterWalFrameAppendBeforeFsync,
    /// After fsync completes but before CommitIndex/SHM publish.
    AfterFsyncBeforePublish,
    /// Between page-table rebuild steps during recovery.
    BetweenPageTableRebuildSteps,
    /// After publish completes but before checkpoint starts.
    AfterPublishBeforeCheckpoint,
    /// Mid-checkpoint (some pages written back to DB, not all).
    MidCheckpoint,
    /// After checkpoint completes.
    AfterCheckpoint,
}

impl CrashBoundary {
    /// All 8 boundaries in protocol order.
    #[must_use]
    pub const fn all() -> [Self; 8] {
        [
            Self::BeforeWalHeaderWrite,
            Self::BeforeWalFrameAppend,
            Self::AfterWalFrameAppendBeforeFsync,
            Self::AfterFsyncBeforePublish,
            Self::BetweenPageTableRebuildSteps,
            Self::AfterPublishBeforeCheckpoint,
            Self::MidCheckpoint,
            Self::AfterCheckpoint,
        ]
    }

    /// String name for structured logging.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BeforeWalHeaderWrite => "before-wal-header-write",
            Self::BeforeWalFrameAppend => "before-wal-frame-append",
            Self::AfterWalFrameAppendBeforeFsync => "after-wal-frame-append-before-fsync",
            Self::AfterFsyncBeforePublish => "after-fsync-before-publish",
            Self::BetweenPageTableRebuildSteps => "between-page-table-rebuild-steps",
            Self::AfterPublishBeforeCheckpoint => "after-publish-before-checkpoint",
            Self::MidCheckpoint => "mid-checkpoint",
            Self::AfterCheckpoint => "after-checkpoint",
        }
    }
}

impl std::fmt::Display for CrashBoundary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// State for the crash-boundary injection system (bd-hd2he).
#[derive(Debug, Default)]
struct CrashBoundaryState {
    armed: Option<(CrashBoundary, FaultHookArm)>,
    fire_count: u64,
}

static CRASH_BOUNDARY_STATE: LazyLock<Mutex<CrashBoundaryState>> =
    LazyLock::new(|| Mutex::new(CrashBoundaryState::default()));

/// Arm a specific crash boundary. Only one boundary can be armed at a time.
pub fn arm_crash_boundary(boundary: CrashBoundary, arm: FaultHookArm) {
    let mut state = CRASH_BOUNDARY_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state.armed = Some((boundary, arm));
}

/// Disarm any active crash boundary.
pub fn disarm_crash_boundary() {
    let mut state = CRASH_BOUNDARY_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state.armed = None;
}

/// Check whether the given boundary is armed and fire if so.
/// Returns `Err` with a fault injection error if the boundary fires.
pub fn maybe_inject_crash_at(boundary: CrashBoundary, detail: &str) -> Result<()> {
    if !fault_hook_applies_to_current_thread() {
        return Ok(());
    }

    let mut state = CRASH_BOUNDARY_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some((armed_boundary, ref arm)) = state.armed else {
        return Ok(());
    };
    if armed_boundary != boundary {
        return Ok(());
    }
    let arm_clone = arm.clone();
    state.fire_count += 1;
    let fire_count = state.fire_count;
    state.armed = None;

    warn!(
        target: "fsqlite_wal::crash_injection",
        boundary = boundary.as_str(),
        fire_count,
        run_id = %arm_clone.run_id,
        scenario_id = %arm_clone.scenario_id,
        detail,
        "crash boundary fired"
    );

    // Also record in the general fault injection records
    let mut wal_state = WAL_FAULT_HOOK_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    record_trigger(
        &mut wal_state,
        &arm_clone,
        boundary.as_str(),
        detail.to_owned(),
    );

    Err(fault_error(boundary.as_str(), &arm_clone))
}

/// How many times a crash boundary has fired since the last clear.
#[must_use]
pub fn crash_boundary_fire_count() -> u64 {
    CRASH_BOUNDARY_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .fire_count
}

/// Clear the crash boundary state (fire count + armed state).
pub fn clear_crash_boundary() {
    let mut state = CRASH_BOUNDARY_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *state = CrashBoundaryState::default();
}

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_GUARD: FaultInjectionSessionLock = FaultInjectionSessionLock::new();

    fn arm(point: &str) -> FaultHookArm {
        FaultHookArm::new(format!("test-{point}"), format!("scenario-{point}"), "unit")
    }

    #[test]
    fn scoped_session_prevents_other_threads_from_stealing_hooks() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        arm_crash_boundary(CrashBoundary::BeforeWalFrameAppend, arm("owned"));

        let unrelated_result = std::thread::spawn(|| {
            maybe_inject_crash_at(CrashBoundary::BeforeWalFrameAppend, "unrelated-thread")
        })
        .join()
        .expect("unrelated thread should not panic");
        assert!(
            unrelated_result.is_ok(),
            "a non-owner thread must not consume the armed hook"
        );

        let owner_result =
            maybe_inject_crash_at(CrashBoundary::BeforeWalFrameAppend, "owner-thread");
        assert!(
            owner_result.is_err(),
            "the owner thread must still observe its armed hook"
        );
    }

    #[test]
    fn test_clear_resets_all_armed_hooks_and_records() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_after_append(arm("append"));
        arm_sync_failure(arm("sync"));
        arm_append_busy_countdown(arm("busy"), 1);
        arm_crash_header_truncate(arm("crash"));
        let _ = maybe_inject_after_append(0, 1);
        assert_eq!(take_records().len(), 1);

        clear();
        assert!(maybe_inject_after_append(0, 1).is_ok());
        assert!(maybe_inject_sync_failure(0, SyncFlags::NORMAL).is_ok());
        assert!(maybe_inject_append_busy(0, 1).is_ok());
        assert!(maybe_inject_crash_header_truncate(0, 1).is_ok());
        assert!(take_records().is_empty());
    }

    #[test]
    fn test_after_append_fires_once_then_disarms() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_after_append(arm("append"));
        let err = maybe_inject_after_append(5, 3);
        assert!(err.is_err());
        assert!(
            err.unwrap_err()
                .to_string()
                .contains("fault_inject:wal_after_append")
        );

        assert!(maybe_inject_after_append(8, 2).is_ok());
    }

    #[test]
    fn test_sync_failure_fires_once_then_disarms() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_sync_failure(arm("sync"));
        let err = maybe_inject_sync_failure(10, SyncFlags::FULL);
        assert!(err.is_err());
        assert!(
            err.unwrap_err()
                .to_string()
                .contains("fault_inject:wal_sync_failure")
        );

        assert!(maybe_inject_sync_failure(10, SyncFlags::FULL).is_ok());
    }

    #[test]
    fn test_append_busy_countdown_fires_on_nth_invocation() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_append_busy_countdown(arm("busy"), 3);
        assert!(
            maybe_inject_append_busy(0, 1).is_ok(),
            "1st call should not fire"
        );
        assert!(
            maybe_inject_append_busy(1, 1).is_ok(),
            "2nd call should not fire"
        );
        let err = maybe_inject_append_busy(2, 1);
        assert!(err.is_err(), "3rd call should fire");
        assert!(matches!(err.unwrap_err(), FrankenError::Busy));

        assert!(
            maybe_inject_append_busy(3, 1).is_ok(),
            "disarmed after fire"
        );
    }

    #[test]
    fn test_append_busy_countdown_minimum_one() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_append_busy_countdown(arm("busy"), 0);
        let err = maybe_inject_append_busy(0, 1);
        assert!(
            err.is_err(),
            "countdown of 0 should clamp to 1 and fire immediately"
        );
    }

    #[test]
    fn test_crash_header_truncate_fires_once() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_crash_header_truncate(arm("crash"));
        let err = maybe_inject_crash_header_truncate(4, 2);
        assert!(err.is_err());
        assert!(
            err.unwrap_err()
                .to_string()
                .contains("fault_inject:wal_crash_header_truncate"),
        );

        assert!(maybe_inject_crash_header_truncate(4, 3).is_ok());
    }

    #[test]
    fn test_records_capture_trigger_details() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_after_append(FaultHookArm::new("run-a", "scen-a", "inv-a"));
        let _ = maybe_inject_after_append(7, 3);

        arm_sync_failure(FaultHookArm::new("run-b", "scen-b", "inv-b"));
        let _ = maybe_inject_sync_failure(12, SyncFlags::FULL);

        let records = take_records();
        assert_eq!(records.len(), 2);

        assert_eq!(records[0].point, "wal_after_append");
        assert_eq!(records[0].run_id, "run-a");
        assert!(records[0].detail.contains("frame_count_before=7"));
        assert!(records[0].detail.contains("appended_frames=3"));

        assert_eq!(records[1].point, "wal_sync_failure");
        assert_eq!(records[1].run_id, "run-b");
        assert!(records[1].detail.contains("frame_count_before=12"));

        assert_eq!(records[0].trigger_seq, 1);
        assert_eq!(records[1].trigger_seq, 2);
    }

    #[test]
    fn test_take_records_drains_and_is_empty_after() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_after_append(arm("append"));
        let _ = maybe_inject_after_append(0, 1);

        let first = take_records();
        assert_eq!(first.len(), 1);
        let second = take_records();
        assert!(second.is_empty());
    }

    #[test]
    fn test_rearming_overwrites_previous_arm() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_after_append(FaultHookArm::new("first", "s1", "i1"));
        arm_after_append(FaultHookArm::new("second", "s2", "i2"));

        let _ = maybe_inject_after_append(0, 1);
        let records = take_records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].run_id, "second");
    }

    #[test]
    fn test_trigger_seq_monotonic_across_drain_cycles() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_after_append(arm("a"));
        let _ = maybe_inject_after_append(0, 1);
        let r1 = take_records();

        arm_sync_failure(arm("s"));
        let _ = maybe_inject_sync_failure(0, SyncFlags::NORMAL);
        let r2 = take_records();

        assert!(r2[0].trigger_seq > r1[0].trigger_seq);
    }

    #[test]
    fn test_crash_header_truncate_record_detail() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_crash_header_truncate(FaultHookArm::new("r", "s", "i"));
        let _ = maybe_inject_crash_header_truncate(100, 7);

        let records = take_records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].point, "wal_crash_header_truncate");
        assert!(records[0].detail.contains("old_frame_count=100"));
        assert!(records[0].detail.contains("new_checkpoint_seq=7"));
    }

    #[test]
    fn test_all_four_hooks_fire_independently() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_after_append(arm("append"));
        arm_sync_failure(arm("sync"));
        arm_append_busy_countdown(arm("busy"), 1);
        arm_crash_header_truncate(arm("crash"));

        assert!(maybe_inject_after_append(0, 1).is_err());
        assert!(maybe_inject_sync_failure(0, SyncFlags::NORMAL).is_err());
        assert!(maybe_inject_append_busy(0, 1).is_err());
        assert!(maybe_inject_crash_header_truncate(0, 1).is_err());

        let records = take_records();
        assert_eq!(records.len(), 4);
        assert_eq!(records[0].point, "wal_after_append");
        assert_eq!(records[1].point, "wal_sync_failure");
        assert_eq!(records[2].point, "wal_append_busy_countdown");
        assert_eq!(records[3].point, "wal_crash_header_truncate");
    }

    #[test]
    fn test_fault_hook_arm_clone_and_eq() {
        let a = FaultHookArm::new("r1", "s1", "inv");
        let b = a.clone();
        assert_eq!(a, b);
        let c = FaultHookArm::new("r1", "s1", "other");
        assert_ne!(a, c);
    }

    #[test]
    fn test_unarmed_hooks_are_noop_from_fresh_state() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        assert!(maybe_inject_after_append(0, 1).is_ok());
        assert!(maybe_inject_sync_failure(0, SyncFlags::FULL).is_ok());
        assert!(maybe_inject_append_busy(0, 1).is_ok());
        assert!(maybe_inject_crash_header_truncate(0, 1).is_ok());
        assert!(take_records().is_empty());
    }

    #[test]
    fn test_append_busy_countdown_one_fires_immediately() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_append_busy_countdown(arm("busy"), 1);
        let err = maybe_inject_append_busy(0, 5);
        assert!(err.is_err());
        assert!(matches!(err.unwrap_err(), FrankenError::Busy));
        let records = take_records();
        assert_eq!(records.len(), 1);
        assert!(records[0].detail.contains("submitted_frames=5"));
    }

    #[test]
    fn test_fault_injection_record_fields() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_after_append(FaultHookArm::new("run-x", "scen-y", "fam-z"));
        let _ = maybe_inject_after_append(42, 7);
        let records = take_records();
        assert_eq!(records.len(), 1);
        let r = &records[0];
        assert_eq!(r.run_id, "run-x");
        assert_eq!(r.scenario_id, "scen-y");
        assert_eq!(r.invariant_family, "fam-z");
        assert_eq!(r.point, "wal_after_append");
        assert!(r.trigger_seq > 0);
    }

    #[test]
    fn test_fault_injection_record_clone_and_eq() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_sync_failure(FaultHookArm::new("rc", "sc", "ic"));
        let _ = maybe_inject_sync_failure(3, SyncFlags::NORMAL);
        let records = take_records();
        let original = &records[0];
        let cloned = original.clone();
        assert_eq!(*original, cloned);
        assert_eq!(cloned.point, "wal_sync_failure");
        assert_eq!(cloned.detail, original.detail);
    }

    #[test]
    fn test_fault_hook_arm_debug_format() {
        let a = FaultHookArm::new("dbg-run", "dbg-scen", "dbg-inv");
        let dbg = format!("{a:?}");
        assert!(dbg.contains("FaultHookArm"));
        assert!(dbg.contains("dbg-run"));
        assert!(dbg.contains("dbg-scen"));
        assert!(dbg.contains("dbg-inv"));
    }

    #[test]
    fn test_sync_failure_detail_captures_flags() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_sync_failure(arm("sf"));
        let _ = maybe_inject_sync_failure(5, SyncFlags::FULL);
        let records = take_records();
        assert_eq!(records.len(), 1);
        assert!(records[0].detail.contains("frame_count_before=5"));
        assert!(
            records[0].detail.contains("flags="),
            "detail should include flags representation"
        );
    }

    #[test]
    fn test_append_busy_rearming_resets_countdown() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();

        arm_append_busy_countdown(arm("first"), 2);
        assert!(maybe_inject_append_busy(0, 1).is_ok(), "1st of 2");
        arm_append_busy_countdown(arm("second"), 3);
        assert!(maybe_inject_append_busy(1, 1).is_ok(), "1st of 3");
        assert!(maybe_inject_append_busy(2, 1).is_ok(), "2nd of 3");
        let err = maybe_inject_append_busy(3, 1);
        assert!(err.is_err(), "3rd of 3 should fire");
        let records = take_records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].run_id, "test-second");
    }

    #[test]
    fn fault_hook_arm_clone_eq_debug() {
        let a = FaultHookArm::new("r1", "s1", "i1");
        let b = a.clone();
        assert_eq!(a, b);
        let c = FaultHookArm::new("r2", "s1", "i1");
        assert_ne!(a, c);
        let dbg = format!("{a:?}");
        assert!(dbg.contains("FaultHookArm"));
        assert!(dbg.contains("r1"));
    }

    #[test]
    fn fault_injection_record_clone_eq_debug() {
        let r = FaultInjectionRecord {
            trigger_seq: 1,
            point: "pt",
            run_id: "r".into(),
            scenario_id: "s".into(),
            invariant_family: "i".into(),
            detail: "d".into(),
        };
        let cloned = r.clone();
        assert_eq!(r, cloned);
        let other = FaultInjectionRecord {
            trigger_seq: 2,
            ..r.clone()
        };
        assert_ne!(r, other);
        let dbg = format!("{r:?}");
        assert!(dbg.contains("FaultInjectionRecord"));
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
        let a = FaultHookArm::new("rid", "sid", "ifam");
        assert_eq!(a.run_id, "rid");
        assert_eq!(a.scenario_id, "sid");
        assert_eq!(a.invariant_family, "ifam");
    }

    // ---------------------------------------------------------------
    // CrashBoundary tests
    // ---------------------------------------------------------------

    #[test]
    fn crash_boundary_arm_and_fire() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();
        clear_crash_boundary();

        arm_crash_boundary(CrashBoundary::MidCheckpoint, arm("cb"));
        let err = maybe_inject_crash_at(CrashBoundary::MidCheckpoint, "page_idx=3");
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("mid-checkpoint"));
    }

    #[test]
    fn crash_boundary_wrong_boundary_does_not_fire() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();
        clear_crash_boundary();

        arm_crash_boundary(CrashBoundary::AfterCheckpoint, arm("cb"));
        assert!(maybe_inject_crash_at(CrashBoundary::MidCheckpoint, "").is_ok());
    }

    #[test]
    fn crash_boundary_fires_once_then_disarms() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();
        clear_crash_boundary();

        arm_crash_boundary(CrashBoundary::BeforeWalFrameAppend, arm("cb"));
        assert!(maybe_inject_crash_at(CrashBoundary::BeforeWalFrameAppend, "f=1").is_err());
        assert!(maybe_inject_crash_at(CrashBoundary::BeforeWalFrameAppend, "f=2").is_ok());
    }

    #[test]
    fn crash_boundary_fire_count_increments() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();
        clear_crash_boundary();

        assert_eq!(crash_boundary_fire_count(), 0);

        arm_crash_boundary(CrashBoundary::AfterFsyncBeforePublish, arm("cb1"));
        let _ = maybe_inject_crash_at(CrashBoundary::AfterFsyncBeforePublish, "");
        assert_eq!(crash_boundary_fire_count(), 1);

        arm_crash_boundary(CrashBoundary::AfterCheckpoint, arm("cb2"));
        let _ = maybe_inject_crash_at(CrashBoundary::AfterCheckpoint, "");
        assert_eq!(crash_boundary_fire_count(), 2);
    }

    #[test]
    fn crash_boundary_disarm_prevents_fire() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();
        clear_crash_boundary();

        arm_crash_boundary(CrashBoundary::BeforeWalHeaderWrite, arm("cb"));
        disarm_crash_boundary();
        assert!(maybe_inject_crash_at(CrashBoundary::BeforeWalHeaderWrite, "").is_ok());
    }

    #[test]
    fn crash_boundary_clear_resets_fire_count_and_armed() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();
        clear_crash_boundary();

        arm_crash_boundary(CrashBoundary::MidCheckpoint, arm("cb"));
        let _ = maybe_inject_crash_at(CrashBoundary::MidCheckpoint, "");
        assert_eq!(crash_boundary_fire_count(), 1);

        clear_crash_boundary();
        assert_eq!(crash_boundary_fire_count(), 0);
        assert!(maybe_inject_crash_at(CrashBoundary::MidCheckpoint, "").is_ok());
    }

    #[test]
    fn crash_boundary_records_in_wal_fault_records() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();
        clear_crash_boundary();

        arm_crash_boundary(
            CrashBoundary::AfterCheckpoint,
            FaultHookArm::new("run-cb", "scen-cb", "inv-cb"),
        );
        let _ = maybe_inject_crash_at(CrashBoundary::AfterCheckpoint, "pages=42");

        let records = take_records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].point, "after-checkpoint");
        assert_eq!(records[0].run_id, "run-cb");
        assert_eq!(records[0].scenario_id, "scen-cb");
        assert!(records[0].detail.contains("pages=42"));
    }

    #[test]
    fn crash_boundary_unarmed_is_noop() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();
        clear_crash_boundary();

        for boundary in CrashBoundary::all() {
            assert!(maybe_inject_crash_at(boundary, "").is_ok());
        }
        assert!(take_records().is_empty());
    }

    #[test]
    fn crash_boundary_all_returns_8_variants() {
        let all = CrashBoundary::all();
        assert_eq!(all.len(), 8);
        for (i, b) in all.iter().enumerate() {
            for (j, c) in all.iter().enumerate() {
                if i != j {
                    assert_ne!(b, c);
                }
            }
        }
    }

    #[test]
    fn crash_boundary_as_str_and_display() {
        assert_eq!(
            CrashBoundary::BeforeWalHeaderWrite.as_str(),
            "before-wal-header-write"
        );
        assert_eq!(CrashBoundary::MidCheckpoint.to_string(), "mid-checkpoint");
        assert_eq!(
            CrashBoundary::AfterCheckpoint.to_string(),
            "after-checkpoint"
        );
    }

    #[test]
    fn crash_boundary_rearming_overwrites_previous() {
        let _g = TEST_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear();
        clear_crash_boundary();

        arm_crash_boundary(
            CrashBoundary::MidCheckpoint,
            FaultHookArm::new("first", "s1", "i1"),
        );
        arm_crash_boundary(
            CrashBoundary::AfterCheckpoint,
            FaultHookArm::new("second", "s2", "i2"),
        );

        assert!(maybe_inject_crash_at(CrashBoundary::MidCheckpoint, "").is_ok());
        assert!(maybe_inject_crash_at(CrashBoundary::AfterCheckpoint, "").is_err());

        let records = take_records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].run_id, "second");
    }
}
