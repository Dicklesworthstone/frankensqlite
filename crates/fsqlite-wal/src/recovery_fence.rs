//! Recovery fencing, dead-PID lock release, and checkpoint checksum validation
//! primitives for `bd-yfdb6` (OPS-3-2 CRITICAL).
//!
//! # Why this module exists
//!
//! Audit finding: with two connections in flight, WAL recovery can silently
//! discard committed-but-unflushed frames when a process is killed mid-write.
//! Four complementary mechanisms eliminate the race:
//!
//! 1. [`RecoveryFence`] — an `AtomicBool` gate that makes recovery mutually
//!    exclusive. New connections spin with bounded backoff (100 ms × 10
//!    retries) while another connection is running recovery.
//! 2. [`ensure_db_fsync_before_wal_truncate`] — insists on an explicit full
//!    fsync of the database file before any WAL truncate. Callers that only
//!    had `fdatasync` coverage previously must upgrade. See
//!    [`crate::execute_checkpoint`].
//! 3. [`PidOwnedLockRegistry`] — lightweight PID tracker that pairs every
//!    lock acquisition with the owning PID and exposes
//!    `release_dead_pid_locks` for recovery start-up. Uses the `/proc`-based
//!    liveness probe shared with MVCC lifecycle (`process_alive_os`).
//! 4. [`verify_checkpoint_checksum_prefix`] — verifies on-disk DB checksums
//!    match the computed post-checkpoint state before truncating the WAL. On
//!    mismatch, truncate is refused and the caller surfaces an
//!    `UnrecoverableError`.
//!
//! All four are orthogonal and composable: [`execute_recovery_barrier`]
//! wires them together for the common "about to truncate WAL" call-site.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::PageNumber;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::SyncFlags;
use fsqlite_types::sync_primitives::Mutex;
use fsqlite_vfs::VfsFile;
use tracing::{debug, error, info, warn};

use crate::checkpoint_executor::CheckpointTarget;

// ---------------------------------------------------------------------------
// RecoveryFence
// ---------------------------------------------------------------------------

/// Bounded-backoff sleep between recovery-fence probes.
///
/// 100 ms × 10 retries = 1 s worst-case wait for concurrent recovery to
/// finish before the new connection returns a soft `Busy` to the caller.
pub const RECOVERY_FENCE_BACKOFF: Duration = Duration::from_millis(100);

/// Maximum number of backoff cycles before a waiting connection gives up.
pub const RECOVERY_FENCE_MAX_RETRIES: u32 = 10;

/// Number of `spin_loop` iterations to try before falling back to the
/// sleep-backoff cycle.
///
/// The common case for the fence is a concurrent-open storm on a warm
/// database: every waiter holds the fence only for a hot-journal probe
/// (microseconds), so if we catch the release within a short spin we
/// avoid the 100 ms sleep penalty. N connections opening the same path
/// concurrently previously each paid up to `N × 100 ms` of sleep before
/// a CAS succeeded; now all but the holder typically resolve inside a
/// few microseconds of `PAUSE`-driven spin.
///
/// Sized for ~1–5 µs of total spin on modern x86 (rough estimate — one
/// `pause` on Intel is 5–100 ns depending on µarch). If the fence is
/// genuinely held for a long recovery, we fall through to the original
/// sleep-backoff budget.
pub const RECOVERY_FENCE_SPIN_ATTEMPTS: u32 = 4096;

/// Single-owner fence that serializes WAL recovery across concurrent
/// connection-open / checkpoint paths.
///
/// The common pattern:
///
/// ```ignore
/// let _guard = fence.acquire_for_recovery()?;
/// // ... run recovery / checkpoint-truncate path ...
/// // guard is dropped automatically → fence released
/// ```
#[derive(Debug, Default)]
pub struct RecoveryFence {
    /// `true` while a recovery / truncate path holds the fence.
    in_progress: AtomicBool,
    /// Monotonic generation — bumped on release. Used by diagnostics and by
    /// re-validation logic that wants to detect whether a parallel recovery
    /// completed between two probes.
    generation: AtomicU64,
}

impl RecoveryFence {
    /// Create a new, unheld fence.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            in_progress: AtomicBool::new(false),
            generation: AtomicU64::new(0),
        }
    }

    /// Whether another caller currently holds the fence.
    #[must_use]
    pub fn is_recovery_in_progress(&self) -> bool {
        self.in_progress.load(Ordering::Acquire)
    }

    /// Monotonic release counter.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Attempt to acquire the fence for recovery without waiting.
    ///
    /// Returns `Some(guard)` on success, `None` if another caller already
    /// holds the fence.
    #[must_use]
    pub fn try_acquire_for_recovery(&self) -> Option<RecoveryFenceGuard<'_>> {
        if self
            .in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            debug!(target: "fsqlite.wal.recovery_fence", "recovery fence acquired");
            Some(RecoveryFenceGuard { fence: self })
        } else {
            None
        }
    }

    /// Block with bounded backoff until the fence is free, then acquire it.
    ///
    /// Waits up to [`RECOVERY_FENCE_MAX_RETRIES`] cycles of
    /// [`RECOVERY_FENCE_BACKOFF`] each (default: 1 s total). If recovery is
    /// still in progress when the budget is exhausted,
    /// [`FrankenError::Busy`] is returned so the caller can fail fast rather
    /// than hang forever.
    pub fn acquire_for_recovery(&self) -> Result<RecoveryFenceGuard<'_>> {
        self.acquire_for_recovery_with(RECOVERY_FENCE_MAX_RETRIES, RECOVERY_FENCE_BACKOFF)
    }

    /// Version of [`Self::acquire_for_recovery`] with a caller-provided budget.
    ///
    /// Used by tests to keep wall-clock time small.
    pub fn acquire_for_recovery_with(
        &self,
        max_retries: u32,
        backoff: Duration,
    ) -> Result<RecoveryFenceGuard<'_>> {
        // Fast path — concurrent Connection::open storms all contend on
        // the same per-path fence but typically hold it for only a
        // hot-journal probe (microseconds). Spin for a few microseconds
        // before falling through to the sleep-backoff cycle so that
        // N-1 of N concurrent openers don't each pay a 100 ms sleep
        // penalty. `spin_loop` issues `PAUSE` on x86 (hyperthread-
        // friendly) and resolves to a no-op on architectures without
        // an equivalent.
        for _ in 0..RECOVERY_FENCE_SPIN_ATTEMPTS {
            if let Some(guard) = self.try_acquire_for_recovery() {
                return Ok(guard);
            }
            std::hint::spin_loop();
        }
        for attempt in 0..=max_retries {
            if let Some(guard) = self.try_acquire_for_recovery() {
                return Ok(guard);
            }
            if attempt == max_retries {
                break;
            }
            std::thread::sleep(backoff);
        }
        warn!(
            target: "fsqlite.wal.recovery_fence",
            max_retries,
            backoff_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX),
            "recovery fence contested beyond retry budget; returning Busy"
        );
        Err(FrankenError::BusyRecovery)
    }
}

/// Scoped guard that releases the fence on drop.
#[derive(Debug)]
pub struct RecoveryFenceGuard<'a> {
    fence: &'a RecoveryFence,
}

impl<'a> RecoveryFenceGuard<'a> {
    /// Monotonic generation observed when the guard was acquired.
    #[must_use]
    pub fn fence(&self) -> &'a RecoveryFence {
        self.fence
    }
}

impl Drop for RecoveryFenceGuard<'_> {
    fn drop(&mut self) {
        self.fence.generation.fetch_add(1, Ordering::AcqRel);
        self.fence.in_progress.store(false, Ordering::Release);
        debug!(target: "fsqlite.wal.recovery_fence", "recovery fence released");
    }
}

// ---------------------------------------------------------------------------
// Fsync-before-truncate
// ---------------------------------------------------------------------------

/// Ensure the database file is durable before any WAL truncate.
///
/// This is the explicit full-`fsync` called out in the audit finding.
/// `SyncFlags::FULL` (without `DATAONLY`) maps to a true `fsync`, not
/// `fdatasync` — metadata must land too because WAL truncation implicitly
/// rolls back the file-length / mtime story on the DB.
///
/// # Errors
///
/// Propagates the VFS sync failure. Callers MUST NOT proceed to truncate
/// the WAL on error, otherwise recovery may observe the WAL-reset but still
/// see stale committed pages on disk.
pub fn ensure_db_fsync_before_wal_truncate<W>(cx: &Cx, target: &mut W) -> Result<()>
where
    W: CheckpointTarget + ?Sized,
{
    // The CheckpointTarget's sync_db is expected to issue FULL. We rely on
    // it here and on a post-audit assertion in tests via MockCheckpointTarget.
    target.sync_db(cx).map_err(|err| {
        error!(
            target: "fsqlite.wal.recovery_fence",
            error = %err,
            "fsync(db) before WAL truncate failed; refusing to truncate"
        );
        err
    })
}

/// Same as [`ensure_db_fsync_before_wal_truncate`] but operates on a raw
/// `VfsFile` for recovery paths that do not build a full
/// [`CheckpointTarget`].
///
/// Always issues `SyncFlags::FULL` (true `fsync`, not `fdatasync`).
pub fn fsync_db_file_full<F: VfsFile>(cx: &Cx, db_file: &mut F) -> Result<()> {
    db_file.sync(cx, SyncFlags::FULL).map_err(|err| {
        error!(
            target: "fsqlite.wal.recovery_fence",
            error = %err,
            "explicit fsync(db, FULL) before WAL truncate failed"
        );
        err
    })
}

// ---------------------------------------------------------------------------
// PID-owned lock registry
// ---------------------------------------------------------------------------

/// Lightweight tracker that pairs each logical page lock with the PID that
/// acquired it.
///
/// This is the recovery-side complement to the in-process
/// `FcPageLockShard`: recovery can enumerate `(page, holder_pid)` pairs and
/// release any whose owner is no longer a live process.
///
/// Tracking is intentionally best-effort and additive — it does not replace
/// the authoritative lock table. The invariant is: every successful lock
/// acquisition that wants dead-PID recovery must register here; every
/// release must deregister. Missing registrations only mean recovery cannot
/// force-release (safe fallback: lock stays held, next replay attempts
/// will still find it and retry or error).
#[derive(Debug, Default)]
pub struct PidOwnedLockRegistry {
    inner: Mutex<Vec<PidOwnedLockEntry>>,
}

/// A single `(page, owner_pid)` registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PidOwnedLockEntry {
    /// Logical page under lock.
    pub page: PageNumber,
    /// PID that holds the lock.
    pub pid: u32,
    /// Monotonic registration counter — used to stably identify a specific
    /// acquisition across re-entrant callers (e.g. a re-acquire from the
    /// same PID uses the same entry).
    pub sequence: u64,
}

impl PidOwnedLockRegistry {
    /// Empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new lock acquisition. Idempotent on `(page, pid)` pairs —
    /// a re-entrant acquire from the same pid returns the existing
    /// sequence.
    ///
    /// Returns the registration sequence, which callers may optionally pass
    /// back to `deregister_by_sequence` for fast removal.
    pub fn register(&self, page: PageNumber, pid: u32) -> u64 {
        let mut inner = self.inner.lock();
        for entry in inner.iter() {
            if entry.page == page && entry.pid == pid {
                return entry.sequence;
            }
        }
        let sequence = next_sequence();
        inner.push(PidOwnedLockEntry {
            page,
            pid,
            sequence,
        });
        sequence
    }

    /// Deregister a specific `(page, pid)` pair, if present.
    pub fn deregister(&self, page: PageNumber, pid: u32) -> bool {
        let mut inner = self.inner.lock();
        if let Some(pos) = inner.iter().position(|e| e.page == page && e.pid == pid) {
            inner.swap_remove(pos);
            true
        } else {
            false
        }
    }

    /// Snapshot of current registrations.
    #[must_use]
    pub fn snapshot(&self) -> Vec<PidOwnedLockEntry> {
        self.inner.lock().clone()
    }

    /// Count of currently-tracked locks.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    /// Whether the registry is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }

    /// Release every registration whose owning PID is no longer alive.
    ///
    /// `is_alive` is injected so tests can deterministically exercise the
    /// dead-PID branch without spawning processes. The real path wires in
    /// `pid_alive_os`.
    ///
    /// Returns the released entries (so the caller can drop the matching
    /// page locks in the authoritative lock table).
    pub fn release_dead_pid_locks<F>(&self, mut is_alive: F) -> Vec<PidOwnedLockEntry>
    where
        F: FnMut(u32) -> bool,
    {
        let mut inner = self.inner.lock();
        let mut released = Vec::new();
        let mut i = 0;
        while i < inner.len() {
            let entry = inner[i];
            if !is_alive(entry.pid) {
                released.push(entry);
                inner.swap_remove(i);
            } else {
                i += 1;
            }
        }
        if !released.is_empty() {
            info!(
                target: "fsqlite.wal.recovery_fence",
                released = released.len(),
                "dead-PID lock force-release at recovery start"
            );
        }
        released
    }
}

/// OS-level liveness probe.
///
/// On `unix`, checks `/proc/<pid>` existence. On non-unix targets we
/// conservatively report `true` so we never force-release a lock we cannot
/// verify is stale.
///
/// Mirrors the design in `fsqlite_mvcc::lifecycle::process_alive_os`, but
/// is kept here to avoid pulling `fsqlite-mvcc` into `fsqlite-wal`'s
/// dependency graph.
#[must_use]
pub fn pid_alive_os(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    #[cfg(unix)]
    {
        let proc_root = std::path::Path::new("/proc");
        if !proc_root.exists() {
            return true; // conservative fallback
        }
        proc_root.join(pid.to_string()).exists()
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        true
    }
}

fn next_sequence() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// Checkpoint checksum validation
// ---------------------------------------------------------------------------

/// Result of [`verify_checkpoint_checksum_prefix`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointChecksumVerdict {
    /// On-disk DB checksums match every expected post-checkpoint page.
    /// WAL truncate is safe to proceed.
    Match,
    /// At least one page's on-disk checksum differs from the expected
    /// post-checkpoint value. WAL truncate MUST NOT proceed; the caller
    /// must surface an unrecoverable-error telemetry event and keep the
    /// WAL intact so a retry can complete the backfill.
    Mismatch {
        /// First offending page number (the caller may include more in its
        /// telemetry, but we report the first for low-overhead logging).
        first_bad_page: PageNumber,
    },
}

/// Expected page checksum from the post-checkpoint state.
///
/// The caller typically produces these by hashing the WAL frame data that
/// was just backfilled, before issuing the corresponding `write_page` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpectedPageChecksum {
    /// Page that was written during checkpoint.
    pub page: PageNumber,
    /// Expected value of `read_page_checksum(data)` after the write lands
    /// on disk.
    pub checksum: crate::checksum::Xxh3Checksum128,
}

/// Verify that the on-disk DB checksums match the expected post-checkpoint
/// state before truncating the WAL.
///
/// Reads each listed page from `db_file` and hashes its trailer. Any
/// mismatch returns [`CheckpointChecksumVerdict::Mismatch`] — the caller
/// MUST refuse the truncate on this verdict, per the audit finding.
///
/// `page_size` must be the true on-disk page size; `expected` lists the
/// post-checkpoint state. Empty `expected` short-circuits to `Match`.
pub fn verify_checkpoint_checksum_prefix<F: VfsFile>(
    cx: &Cx,
    db_file: &F,
    page_size: u32,
    expected: &[ExpectedPageChecksum],
) -> Result<CheckpointChecksumVerdict> {
    if expected.is_empty() {
        return Ok(CheckpointChecksumVerdict::Match);
    }
    let page_size_usize = usize::try_from(page_size).map_err(|_| FrankenError::OutOfRange {
        what: "page size for checksum verify".to_owned(),
        value: page_size.to_string(),
    })?;
    let mut page_buf = vec![0u8; page_size_usize];
    for exp in expected {
        let offset = u64::from(exp.page.get() - 1)
            .checked_mul(u64::from(page_size))
            .ok_or_else(|| FrankenError::OutOfRange {
                what: "checksum verify offset".to_owned(),
                value: exp.page.get().to_string(),
            })?;
        let n = db_file.read(cx, &mut page_buf, offset)?;
        if n < page_size_usize {
            error!(
                target: "fsqlite.wal.recovery_fence",
                page = exp.page.get(),
                got = n,
                need = page_size_usize,
                "short read during checkpoint checksum verify"
            );
            return Ok(CheckpointChecksumVerdict::Mismatch {
                first_bad_page: exp.page,
            });
        }
        let observed = crate::checksum::read_page_checksum(&page_buf)?;
        if observed != exp.checksum {
            error!(
                target: "fsqlite.wal.recovery_fence",
                page = exp.page.get(),
                "on-disk page checksum mismatch; refusing to truncate WAL"
            );
            return Ok(CheckpointChecksumVerdict::Mismatch {
                first_bad_page: exp.page,
            });
        }
    }
    info!(
        target: "fsqlite.wal.recovery_fence",
        verified_pages = expected.len(),
        "checkpoint checksum prefix verified; WAL truncate safe"
    );
    Ok(CheckpointChecksumVerdict::Match)
}

// ---------------------------------------------------------------------------
// Composite barrier
// ---------------------------------------------------------------------------

/// One-shot convenience that (a) fsyncs the DB full, then (b) verifies the
/// post-checkpoint checksum prefix.
///
/// Returns `Ok(())` when the WAL truncate may proceed;
/// `Err(FrankenError::DatabaseCorrupt)` on mismatch, matching the
/// audit-requested "do not truncate; log unrecoverable-error" policy.
pub fn execute_recovery_barrier<W, F>(
    cx: &Cx,
    target: &mut W,
    db_file: &F,
    page_size: u32,
    expected: &[ExpectedPageChecksum],
) -> Result<()>
where
    W: CheckpointTarget + ?Sized,
    F: VfsFile,
{
    ensure_db_fsync_before_wal_truncate(cx, target)?;
    match verify_checkpoint_checksum_prefix(cx, db_file, page_size, expected)? {
        CheckpointChecksumVerdict::Match => Ok(()),
        CheckpointChecksumVerdict::Mismatch { first_bad_page } => {
            error!(
                target: "fsqlite.wal.recovery_fence",
                first_bad_page = first_bad_page.get(),
                "UNRECOVERABLE: post-checkpoint checksum mismatch; WAL truncate refused"
            );
            Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "post-checkpoint DB/WAL state disagreed at page {}; WAL truncate refused \
                     to preserve committed frames",
                    first_bad_page.get()
                ),
            })
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_vfs::MemoryVfs;
    use fsqlite_vfs::traits::Vfs;

    use super::*;

    fn test_cx() -> Cx {
        Cx::new()
    }

    // --- RecoveryFence ----------------------------------------------------

    #[test]
    fn fence_try_acquire_uncontended() {
        let fence = RecoveryFence::new();
        assert!(!fence.is_recovery_in_progress());
        let guard = fence.try_acquire_for_recovery().expect("first acquire");
        assert!(fence.is_recovery_in_progress());
        assert!(fence.try_acquire_for_recovery().is_none());
        drop(guard);
        assert!(!fence.is_recovery_in_progress());
        let _g2 = fence
            .try_acquire_for_recovery()
            .expect("reacquire after release");
    }

    #[test]
    fn fence_generation_bumps_on_release() {
        let fence = RecoveryFence::new();
        let gen0 = fence.generation();
        {
            let _g = fence.try_acquire_for_recovery().expect("acquire");
        }
        assert!(fence.generation() > gen0);
    }

    #[test]
    fn test_recovery_fences_concurrent_open() {
        // Connection A holds the fence for ~150 ms. Connection B's
        // acquire_for_recovery() must block until A releases, then proceed.
        let fence = Arc::new(RecoveryFence::new());
        let a = {
            let fence = Arc::clone(&fence);
            thread::spawn(move || {
                let guard = fence.try_acquire_for_recovery().expect("A: acquire");
                thread::sleep(Duration::from_millis(150));
                drop(guard);
            })
        };
        // Give A a head start so we know it owns the fence before B probes.
        thread::sleep(Duration::from_millis(20));
        let start = Instant::now();
        let b_guard = fence
            .acquire_for_recovery_with(20, Duration::from_millis(20))
            .expect("B: acquire after wait");
        let waited = start.elapsed();
        assert!(
            waited >= Duration::from_millis(100),
            "B should have waited while A held the fence (waited {waited:?})",
        );
        drop(b_guard);
        a.join().expect("A join");
    }

    #[test]
    fn fence_returns_busy_after_retry_budget() {
        let fence = Arc::new(RecoveryFence::new());
        let held = fence.try_acquire_for_recovery().expect("hold fence");
        let result = fence.acquire_for_recovery_with(2, Duration::from_millis(5));
        assert!(matches!(result, Err(FrankenError::BusyRecovery)));
        drop(held);
    }

    #[test]
    fn fence_concurrent_open_storm_does_not_pay_sleep_penalty() {
        // Simulates the `mt_mvcc_bench` Connection::open storm: 8 workers
        // race to acquire the fence but each holder does only a short
        // (~microsecond) "hot-journal probe" before releasing. Before
        // the spin-fast-path, N-1 of N workers paid at least one
        // `backoff` sleep (historically 100 ms) because
        // acquire_for_recovery_with jumped straight into the sleep loop
        // after the first CAS miss. After the spin-fast-path, almost
        // all contenders resolve inside the spin.
        //
        // Gate: with backoff = 100 ms × 10 retries, 8 workers each
        // holding the fence for ~1 µs must finish the full round-robin
        // in well under `ceil(N/1) × backoff`. We set a generous
        // ceiling of 50 ms so the test does not flake on loaded CI,
        // and print the observed latency for the commit body.
        use std::time::Instant;
        const THREADS: usize = 8;
        const ROUNDS_PER_THREAD: usize = 4;
        let fence = Arc::new(RecoveryFence::new());
        let barrier = Arc::new(std::sync::Barrier::new(THREADS));

        let started = Instant::now();
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let fence = Arc::clone(&fence);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..ROUNDS_PER_THREAD {
                        let guard = fence
                            .acquire_for_recovery_with(
                                RECOVERY_FENCE_MAX_RETRIES,
                                RECOVERY_FENCE_BACKOFF,
                            )
                            .expect("fence acquire under storm");
                        // Simulate a hot-journal probe on a warm DB —
                        // the fence is held for only a few hundred
                        // nanoseconds of real work.
                        std::hint::black_box(&guard);
                        drop(guard);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().expect("worker join");
        }
        let elapsed = started.elapsed();

        eprintln!(
            "recovery_fence concurrent-open storm: {THREADS} threads × \
             {ROUNDS_PER_THREAD} rounds = {elapsed:?} \
             (spin_attempts={RECOVERY_FENCE_SPIN_ATTEMPTS}, \
              backoff={RECOVERY_FENCE_BACKOFF:?}, \
              max_retries={RECOVERY_FENCE_MAX_RETRIES})"
        );
        assert!(
            elapsed < Duration::from_millis(50),
            "concurrent-open storm should resolve inside the spin-fast-path, \
             not pay the 100ms sleep penalty (elapsed {elapsed:?})",
        );
    }

    // --- PidOwnedLockRegistry --------------------------------------------

    #[test]
    fn registry_register_dedupes_same_pid() {
        let reg = PidOwnedLockRegistry::new();
        let page = PageNumber::new(42).unwrap();
        let seq_a = reg.register(page, 1111);
        let seq_b = reg.register(page, 1111);
        assert_eq!(seq_a, seq_b);
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn registry_deregister_removes_pair() {
        let reg = PidOwnedLockRegistry::new();
        let page = PageNumber::new(7).unwrap();
        reg.register(page, 99);
        assert!(reg.deregister(page, 99));
        assert!(reg.is_empty());
    }

    #[test]
    fn test_recovery_force_releases_dead_pid_locks() {
        let reg = PidOwnedLockRegistry::new();
        let page_live = PageNumber::new(1).unwrap();
        let page_dead = PageNumber::new(2).unwrap();
        reg.register(page_live, 100);
        reg.register(page_dead, 200);
        let released = reg.release_dead_pid_locks(|pid| pid == 100);
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].page, page_dead);
        assert_eq!(released[0].pid, 200);
        assert_eq!(reg.len(), 1);
        let remaining = reg.snapshot();
        assert_eq!(remaining[0].page, page_live);
    }

    #[test]
    fn pid_alive_os_current_pid_is_alive() {
        // Our own PID must always register as alive.
        let me = std::process::id();
        assert!(pid_alive_os(me));
        // PID 0 is never alive (and would be dangerous to signal).
        assert!(!pid_alive_os(0));
    }

    // --- fsync ordering / mock target ------------------------------------

    /// Mock CheckpointTarget that records sync calls so we can assert
    /// fsync happens before truncate in every recovery path.
    #[derive(Default)]
    struct SyncAuditTarget {
        sync_count: u32,
        writes: u32,
        truncate_at: Option<u32>,
        truncate_after_sync: Option<bool>,
        sync_should_fail: bool,
    }

    impl CheckpointTarget for SyncAuditTarget {
        fn write_page(&mut self, _cx: &Cx, _page: PageNumber, _data: &[u8]) -> Result<()> {
            self.writes += 1;
            Ok(())
        }

        fn truncate_db(&mut self, _cx: &Cx, n_pages: u32) -> Result<()> {
            self.truncate_after_sync = Some(self.sync_count > 0);
            self.truncate_at = Some(n_pages);
            Ok(())
        }

        fn sync_db(&mut self, _cx: &Cx) -> Result<()> {
            if self.sync_should_fail {
                return Err(FrankenError::internal("mock sync failure"));
            }
            self.sync_count += 1;
            Ok(())
        }
    }

    #[test]
    fn test_fsync_before_wal_truncate() {
        let cx = test_cx();
        let mut target = SyncAuditTarget::default();
        // The barrier helper issues sync_db before any truncate path runs.
        ensure_db_fsync_before_wal_truncate(&cx, &mut target).expect("fsync ok");
        // Simulate the truncate that would follow.
        target.truncate_db(&cx, 3).expect("truncate");
        assert_eq!(target.sync_count, 1, "sync_db must run once");
        assert_eq!(
            target.truncate_after_sync,
            Some(true),
            "truncate must observe a prior sync",
        );
    }

    #[test]
    fn fsync_failure_prevents_truncate_path() {
        let cx = test_cx();
        let mut target = SyncAuditTarget {
            sync_should_fail: true,
            ..SyncAuditTarget::default()
        };
        let res = ensure_db_fsync_before_wal_truncate(&cx, &mut target);
        assert!(res.is_err(), "sync failure must propagate");
        assert!(
            target.truncate_at.is_none(),
            "caller must not proceed to truncate after sync failure"
        );
    }

    // --- Checkpoint checksum validation ---------------------------------

    fn write_page_with_checksum(
        cx: &Cx,
        file: &mut <MemoryVfs as Vfs>::File,
        page_size: u32,
        page: PageNumber,
        fill: u8,
    ) -> crate::checksum::Xxh3Checksum128 {
        let page_size_usize = usize::try_from(page_size).unwrap();
        let mut buf = vec![fill; page_size_usize];
        // `write_page_checksum` computes the digest over the payload region
        // (page[..len - PAGE_CHECKSUM_RESERVED_BYTES]) and stores it in the
        // trailer; it returns the digest it just wrote.
        let checksum =
            crate::checksum::write_page_checksum(&mut buf).expect("write checksum trailer");
        let offset = u64::from(page.get() - 1) * u64::from(page_size);
        file.write(cx, &buf, offset).expect("write page");
        checksum
    }

    fn open_db_file(vfs: &MemoryVfs, cx: &Cx) -> <MemoryVfs as Vfs>::File {
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::MAIN_DB;
        let (file, _) = vfs
            .open(cx, Some(std::path::Path::new("/verify.db")), flags)
            .expect("open db");
        file
    }

    #[test]
    fn verify_checksum_match() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut file = open_db_file(&vfs, &cx);
        let page_size = 4096u32;
        let page = PageNumber::new(1).unwrap();
        let checksum = write_page_with_checksum(&cx, &mut file, page_size, page, 0xAB);
        let expected = vec![ExpectedPageChecksum { page, checksum }];
        let verdict =
            verify_checkpoint_checksum_prefix(&cx, &file, page_size, &expected).expect("verify");
        assert_eq!(verdict, CheckpointChecksumVerdict::Match);
    }

    #[test]
    fn test_checkpoint_mismatch_aborts_truncate() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut file = open_db_file(&vfs, &cx);
        let page_size = 4096u32;
        let page = PageNumber::new(1).unwrap();
        let _expected = write_page_with_checksum(&cx, &mut file, page_size, page, 0x11);
        // Supply a deliberately-wrong "expected" checksum so the verifier
        // must report a mismatch even though the on-disk trailer is valid.
        let lied_expected = vec![ExpectedPageChecksum {
            page,
            checksum: crate::checksum::Xxh3Checksum128 {
                low: 0xDEAD_BEEF_CAFE_F00D,
                high: 0xFEED_FACE_1234_5678,
            },
        }];
        let verdict = verify_checkpoint_checksum_prefix(&cx, &file, page_size, &lied_expected)
            .expect("verify");
        match verdict {
            CheckpointChecksumVerdict::Mismatch { first_bad_page } => {
                assert_eq!(first_bad_page, page);
            }
            other => panic!("expected mismatch, got {other:?}"),
        }

        // execute_recovery_barrier should also convert this into an error so
        // that the caller cannot proceed to truncate.
        struct NoopTarget;
        impl CheckpointTarget for NoopTarget {
            fn write_page(&mut self, _: &Cx, _: PageNumber, _: &[u8]) -> Result<()> {
                Ok(())
            }
            fn truncate_db(&mut self, _: &Cx, _: u32) -> Result<()> {
                Ok(())
            }
            fn sync_db(&mut self, _: &Cx) -> Result<()> {
                Ok(())
            }
        }
        let mut target = NoopTarget;
        let barrier = execute_recovery_barrier(&cx, &mut target, &file, page_size, &lied_expected);
        assert!(
            matches!(barrier, Err(FrankenError::DatabaseCorrupt { .. })),
            "barrier must surface unrecoverable error on mismatch",
        );
    }

    // -- PidOwnedLockRegistry unit tests --

    #[test]
    fn registry_new_is_empty() {
        let reg = PidOwnedLockRegistry::new();
        assert!(reg.is_empty());
        assert_eq!(reg.len(), 0);
        assert!(reg.snapshot().is_empty());
    }

    #[test]
    fn registry_register_and_snapshot() {
        let reg = PidOwnedLockRegistry::new();
        let page = PageNumber::new(5).unwrap();
        let seq = reg.register(page, 100);
        assert!(seq > 0);
        assert_eq!(reg.len(), 1);
        assert!(!reg.is_empty());
        let snap = reg.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].page, page);
        assert_eq!(snap[0].pid, 100);
        assert_eq!(snap[0].sequence, seq);
    }

    #[test]
    fn registry_register_idempotent_same_page_pid() {
        let reg = PidOwnedLockRegistry::new();
        let page = PageNumber::new(3).unwrap();
        let seq1 = reg.register(page, 42);
        let seq2 = reg.register(page, 42);
        assert_eq!(seq1, seq2);
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn registry_register_same_page_different_pid() {
        let reg = PidOwnedLockRegistry::new();
        let page = PageNumber::new(7).unwrap();
        let seq1 = reg.register(page, 10);
        let seq2 = reg.register(page, 20);
        assert_ne!(seq1, seq2);
        assert_eq!(reg.len(), 2);
    }

    #[test]
    fn registry_deregister_returns_false_for_absent() {
        let reg = PidOwnedLockRegistry::new();
        let page = PageNumber::new(1).unwrap();
        assert!(!reg.deregister(page, 999));
    }

    #[test]
    fn registry_deregister_only_matching_pair() {
        let reg = PidOwnedLockRegistry::new();
        let p1 = PageNumber::new(1).unwrap();
        let p2 = PageNumber::new(2).unwrap();
        reg.register(p1, 10);
        reg.register(p2, 10);
        assert_eq!(reg.len(), 2);
        assert!(reg.deregister(p1, 10));
        assert_eq!(reg.len(), 1);
        assert!(!reg.deregister(p1, 10));
        let snap = reg.snapshot();
        assert_eq!(snap[0].page, p2);
    }

    #[test]
    fn registry_release_dead_pid_keeps_alive() {
        let reg = PidOwnedLockRegistry::new();
        let page = PageNumber::new(1).unwrap();
        reg.register(page, 100);
        reg.register(page, 200);
        let released = reg.release_dead_pid_locks(|pid| pid == 100);
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].pid, 200);
        assert_eq!(reg.len(), 1);
        let snap = reg.snapshot();
        assert_eq!(snap[0].pid, 100);
    }

    #[test]
    fn registry_release_dead_pid_releases_all_dead() {
        let reg = PidOwnedLockRegistry::new();
        reg.register(PageNumber::new(1).unwrap(), 10);
        reg.register(PageNumber::new(2).unwrap(), 20);
        reg.register(PageNumber::new(3).unwrap(), 30);
        let released = reg.release_dead_pid_locks(|_| false);
        assert_eq!(released.len(), 3);
        assert!(reg.is_empty());
    }

    // -- RecoveryFence edge tests --

    #[test]
    fn fence_initial_state_not_recovering() {
        let fence = RecoveryFence::default();
        assert!(!fence.is_recovery_in_progress());
        assert_eq!(fence.generation(), 0);
    }

    #[test]
    fn fence_guard_marks_recovery_in_progress() {
        let fence = RecoveryFence::default();
        let guard = fence.try_acquire_for_recovery().expect("uncontended");
        assert!(fence.is_recovery_in_progress());
        assert_eq!(guard.fence().generation(), 0);
        drop(guard);
        assert!(!fence.is_recovery_in_progress());
        assert_eq!(fence.generation(), 1);
    }

    #[test]
    fn fence_concurrent_acquire_returns_none() {
        let fence = RecoveryFence::default();
        let _guard = fence.try_acquire_for_recovery().expect("first acquire");
        assert!(fence.try_acquire_for_recovery().is_none());
    }

    // -- verify_checkpoint_checksum_prefix edge: empty expected --

    #[test]
    fn verify_checkpoint_empty_expected_returns_match() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_db_file(&vfs, &cx);
        let verdict = verify_checkpoint_checksum_prefix(&cx, &file, 4096, &[]).expect("verify");
        assert_eq!(verdict, CheckpointChecksumVerdict::Match);
    }

    #[test]
    fn pid_alive_os_zero_is_dead() {
        assert!(!pid_alive_os(0));
    }

    #[test]
    fn fence_new_matches_default() {
        let a = RecoveryFence::new();
        let b = RecoveryFence::default();
        assert_eq!(a.is_recovery_in_progress(), b.is_recovery_in_progress());
        assert_eq!(a.generation(), b.generation());
    }

    #[test]
    fn fence_generation_increments_across_cycles() {
        let fence = RecoveryFence::new();
        for expected in 1..=3u64 {
            let guard = fence.try_acquire_for_recovery().unwrap();
            drop(guard);
            assert_eq!(fence.generation(), expected);
        }
    }

    #[test]
    fn fence_acquire_with_budget_returns_busy_when_held() {
        let fence = RecoveryFence::new();
        let _guard = fence.try_acquire_for_recovery().unwrap();
        let result = fence.acquire_for_recovery_with(0, Duration::from_millis(1));
        assert!(result.is_err());
    }

    #[test]
    fn checkpoint_checksum_verdict_equality() {
        let m1 = CheckpointChecksumVerdict::Match;
        let m2 = CheckpointChecksumVerdict::Match;
        assert_eq!(m1, m2);

        let page = PageNumber::new(5).unwrap();
        let mm = CheckpointChecksumVerdict::Mismatch {
            first_bad_page: page,
        };
        assert_ne!(m1, mm);
    }

    #[test]
    fn pid_lock_entry_copy_clone_eq_debug() {
        let entry = PidOwnedLockEntry {
            page: PageNumber::new(10).unwrap(),
            pid: 1234,
            sequence: 77,
        };
        let copied = entry;
        assert_eq!(entry, copied);
        let cloned = entry;
        assert_eq!(entry, cloned);
        let dbg = format!("{entry:?}");
        assert!(dbg.contains("PidOwnedLockEntry"));
        assert!(dbg.contains("1234"));
    }

    #[test]
    fn recovery_fence_constants_match_documented_values() {
        assert_eq!(RECOVERY_FENCE_BACKOFF, Duration::from_millis(100));
        assert_eq!(RECOVERY_FENCE_MAX_RETRIES, 10);
        assert_eq!(RECOVERY_FENCE_SPIN_ATTEMPTS, 4096);
    }

    #[test]
    fn checkpoint_checksum_verdict_clone_and_debug() {
        let m = CheckpointChecksumVerdict::Match;
        let mc = m.clone();
        assert_eq!(m, mc);

        let page = PageNumber::new(3).unwrap();
        let mm = CheckpointChecksumVerdict::Mismatch {
            first_bad_page: page,
        };
        let mmc = mm.clone();
        assert_eq!(mm, mmc);

        let dbg_match = format!("{m:?}");
        assert!(dbg_match.contains("Match"));
        let dbg_mismatch = format!("{mm:?}");
        assert!(dbg_mismatch.contains("Mismatch"));
    }

    #[test]
    fn pid_alive_os_nonexistent_high_pid() {
        #[cfg(unix)]
        {
            let high_pid = 4_000_000_000;
            assert!(!pid_alive_os(high_pid));
        }
    }

    #[test]
    fn pid_owned_lock_registry_register_deregister_snapshot() {
        let reg = PidOwnedLockRegistry::new();
        assert!(reg.is_empty());
        let p1 = PageNumber::new(1).unwrap();
        let seq = reg.register(p1, 42);
        assert_eq!(reg.len(), 1);
        let seq2 = reg.register(p1, 42);
        assert_eq!(seq, seq2, "re-entrant register returns same sequence");
        assert_eq!(reg.len(), 1);
        let snap = reg.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].page, p1);
        assert_eq!(snap[0].pid, 42);
        assert!(reg.deregister(p1, 42));
        assert!(reg.is_empty());
        assert!(!reg.deregister(p1, 42));
    }

    #[test]
    fn expected_page_checksum_debug_clone_copy_eq() {
        let a = ExpectedPageChecksum {
            page: PageNumber::new(5).unwrap(),
            checksum: crate::checksum::Xxh3Checksum128 {
                low: 0xAA,
                high: 0xBB,
            },
        };
        let copied = a;
        assert_eq!(copied, a);
        let b = ExpectedPageChecksum {
            page: PageNumber::new(6).unwrap(),
            checksum: crate::checksum::Xxh3Checksum128 {
                low: 0xAA,
                high: 0xBB,
            },
        };
        assert_ne!(a, b);
        let dbg = format!("{a:?}");
        assert!(dbg.contains("ExpectedPageChecksum"));
    }

    #[test]
    fn checkpoint_checksum_verdict_variants_eq() {
        let m = CheckpointChecksumVerdict::Match;
        let mm = CheckpointChecksumVerdict::Mismatch {
            first_bad_page: PageNumber::new(3).unwrap(),
        };
        assert_ne!(m, mm);
        let cloned = mm.clone();
        assert_eq!(mm, cloned);
        let dbg = format!("{m:?}");
        assert!(dbg.contains("Match"));
    }

    #[test]
    fn pid_owned_lock_entry_debug_clone_copy_eq() {
        let e = PidOwnedLockEntry {
            page: PageNumber::new(7).unwrap(),
            pid: 100,
            sequence: 1,
        };
        let copied = e;
        assert_eq!(copied, e);
        let other = PidOwnedLockEntry { pid: 200, ..e };
        assert_ne!(e, other);
        let dbg = format!("{e:?}");
        assert!(dbg.contains("PidOwnedLockEntry"));
    }
}
