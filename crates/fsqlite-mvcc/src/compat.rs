//! Compatibility mode: legacy interop and hybrid SHM protocol (§5.6.6-5.6.7).
//!
//! This module implements:
//! - [`CompatMode`]: Operating posture selection (Hybrid SHM vs File-Lock Only).
//! - [`HybridShmState`]: Dual-SHM maintenance state for coordinator.
//! - `ReadLockProtocol`: WAL reader mark join/claim protocol.
//! - `CoordinatorRecovery`: Crash recovery after coordinator or legacy process death.
//! - `begin_concurrent_check`: Gate for `BEGIN CONCURRENT` under no-SHM fallback.

use std::time::Instant;

use fsqlite_error::{FrankenError, Result};
use fsqlite_wal::wal_index::{WAL_READ_MARK_COUNT, WalCkptInfo, WalIndexHdr};

// ---------------------------------------------------------------------------
// CompatMode — operating posture
// ---------------------------------------------------------------------------

/// Compatibility mode operating posture (§5.6.6).
///
/// Determined at database open time based on whether `foo.db.fsqlite-shm`
/// can be created/opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompatMode {
    /// Hybrid SHM protocol active. FrankenSQLite maintains both
    /// `foo.db-shm` (legacy WAL-index) and `foo.db.fsqlite-shm` (MVCC
    /// coordination). Legacy readers supported; legacy writers excluded via
    /// `WAL_WRITE_LOCK` held for coordinator lifetime.
    HybridShm,

    /// No-SHM fallback. Standard SQLite file locking only. Single-writer,
    /// no multi-writer MVCC, no SSI. `BEGIN CONCURRENT` MUST return error
    /// (§5.6.6.2).
    FileLockOnly,
}

impl CompatMode {
    /// Whether this mode supports multi-writer MVCC.
    #[must_use]
    pub const fn supports_concurrent(&self) -> bool {
        matches!(self, Self::HybridShm)
    }

    /// Whether this mode requires dual SHM maintenance.
    #[must_use]
    pub const fn requires_dual_shm(&self) -> bool {
        matches!(self, Self::HybridShm)
    }
}

// ---------------------------------------------------------------------------
// begin_concurrent_check (§5.6.6.2)
// ---------------------------------------------------------------------------

/// Gate for `BEGIN CONCURRENT` — returns error when fsqlite-shm is unavailable.
///
/// The spec is explicit (§5.6.6.2): callers who issue `BEGIN CONCURRENT`
/// have opted into the multi-writer MVCC/SSI contract. Silently downgrading
/// to Serialized mode would make performance and conflict behavior
/// non-obvious. Therefore, this MUST hard-fail.
///
/// # Errors
///
/// Returns [`FrankenError::ConcurrentUnavailable`] if `mode` is
/// [`CompatMode::FileLockOnly`].
pub fn begin_concurrent_check(mode: CompatMode) -> Result<()> {
    if mode == CompatMode::FileLockOnly {
        return Err(FrankenError::ConcurrentUnavailable);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// HybridShmState — dual-SHM maintenance
// ---------------------------------------------------------------------------

/// State maintained by the coordinator for the hybrid SHM protocol (§5.6.7).
///
/// The coordinator keeps both `foo.db-shm` (standard SQLite WAL-index) and
/// `foo.db.fsqlite-shm` (FrankenSQLite MVCC coordination) in sync.
///
/// On every commit, after `wal.sync()` and before `publish_versions()`,
/// the coordinator must call [`update_legacy_shm`](Self::update_legacy_shm)
/// to update the standard WAL-index hash tables so legacy readers see new
/// frames.
#[derive(Debug)]
pub struct HybridShmState {
    /// Whether `WAL_WRITE_LOCK` is held on the legacy `foo.db-shm`.
    wal_write_lock_held: bool,
    /// Current state of the legacy WAL-index header.
    legacy_hdr: WalIndexHdr,
    /// Current state of the legacy checkpoint info.
    legacy_ckpt: WalCkptInfo,
    /// Instant when the coordinator acquired `WAL_WRITE_LOCK`.
    lock_acquired_at: Option<Instant>,
}

impl HybridShmState {
    /// Create a new hybrid state. The caller must immediately acquire
    /// `WAL_WRITE_LOCK` via the VFS layer and call
    /// [`mark_write_lock_held`](Self::mark_write_lock_held).
    #[must_use]
    pub fn new(initial_hdr: WalIndexHdr, initial_ckpt: WalCkptInfo) -> Self {
        Self {
            wal_write_lock_held: false,
            legacy_hdr: initial_hdr,
            legacy_ckpt: initial_ckpt,
            lock_acquired_at: None,
        }
    }

    /// Record that `WAL_WRITE_LOCK` has been acquired.
    pub fn mark_write_lock_held(&mut self, now: Instant) {
        self.wal_write_lock_held = true;
        self.lock_acquired_at = Some(now);
    }

    /// Whether the coordinator currently holds `WAL_WRITE_LOCK`.
    #[must_use]
    pub fn is_write_lock_held(&self) -> bool {
        self.wal_write_lock_held
    }

    /// Update the legacy WAL-index header after a commit (§5.6.7 Step 2).
    ///
    /// This prepares the header update that the VFS layer will write to
    /// `foo.db-shm` using the dual-copy protocol. The update includes:
    /// - `mxFrame` advanced to cover newly appended frames.
    /// - `aFrameCksum` updated with the new running checksum.
    /// - `aSalt` and `aCksum` reflecting the current WAL state.
    ///
    /// # Errors
    ///
    /// Returns error if `WAL_WRITE_LOCK` is not held.
    pub fn update_legacy_shm(
        &mut self,
        new_mx_frame: u32,
        new_n_page: u32,
        new_frame_cksum: [u32; 2],
        new_salt: [u32; 2],
    ) -> Result<UpdatedLegacyShm> {
        if !self.wal_write_lock_held {
            return Err(FrankenError::Internal(
                "cannot update legacy SHM without WAL_WRITE_LOCK".into(),
            ));
        }

        self.legacy_hdr.mx_frame = new_mx_frame;
        self.legacy_hdr.n_page = new_n_page;
        self.legacy_hdr.a_frame_cksum = new_frame_cksum;
        self.legacy_hdr.a_salt = new_salt;
        self.legacy_hdr.i_change = self.legacy_hdr.i_change.wrapping_add(1);
        // Recompute header checksum over bytes 0..40. The checksum is computed
        // by serializing the header and checksumming the first 40 bytes.
        let hdr_bytes = self.legacy_hdr.to_bytes();
        let (mut s1, mut s2) = (0_u32, 0_u32);
        for chunk in hdr_bytes[..40].chunks(4) {
            let word = u32::from_ne_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
            s1 = s1.wrapping_add(word).wrapping_add(s2);
            s2 = s2.wrapping_add(word).wrapping_add(s1);
        }
        self.legacy_hdr.a_cksum = [s1, s2];

        Ok(UpdatedLegacyShm {
            hdr: self.legacy_hdr,
            ckpt: self.legacy_ckpt,
        })
    }

    /// Update `nBackfill` during checkpoint (§5.6.7 Step 4).
    pub fn update_backfill(&mut self, n_backfill: u32) {
        self.legacy_ckpt.n_backfill = n_backfill;
    }

    /// Record that `WAL_WRITE_LOCK` has been released (coordinator shutdown).
    pub fn mark_write_lock_released(&mut self) {
        self.wal_write_lock_held = false;
        self.lock_acquired_at = None;
    }

    /// Duration since the write lock was acquired.
    #[must_use]
    pub fn lock_held_duration(&self, now: Instant) -> Option<std::time::Duration> {
        self.lock_acquired_at.map(|t| now.duration_since(t))
    }

    /// Current legacy header snapshot.
    #[must_use]
    pub fn legacy_hdr(&self) -> &WalIndexHdr {
        &self.legacy_hdr
    }

    /// Current legacy checkpoint info snapshot.
    #[must_use]
    pub fn legacy_ckpt(&self) -> &WalCkptInfo {
        &self.legacy_ckpt
    }
}

/// Result of [`HybridShmState::update_legacy_shm`] — the header and ckpt info
/// to write to `foo.db-shm` via the VFS layer.
#[derive(Debug, Clone)]
pub struct UpdatedLegacyShm {
    /// The updated WAL-index header (write to both copies using dual-copy
    /// protocol).
    pub hdr: WalIndexHdr,
    /// The updated checkpoint info (write after both header copies).
    pub ckpt: WalCkptInfo,
}

// ---------------------------------------------------------------------------
// ReadLockProtocol — WAL reader mark join/claim (§5.6.7 Step 3)
// ---------------------------------------------------------------------------

/// Outcome of attempting to join or claim a reader mark.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadLockOutcome {
    /// Joined an existing reader mark at slot `i` with SHARED lock.
    Joined { slot: usize },
    /// Claimed a new reader mark at slot `i` (EXCLUSIVE then downgraded to
    /// SHARED).
    Claimed { slot: usize },
    /// All reader mark slots are busy — caller should return SQLITE_BUSY.
    AllSlotsBusy,
}

/// Determine which reader mark slot to use for a given snapshot mark (§5.6.7
/// Step 3).
///
/// This implements the join fast-path / claim slow-path protocol:
///
/// - **Join (preferred):** If `a_read_mark[i] == desired_mark` for some `i`,
///   return `Joined { slot: i }`. The caller must acquire SHARED
///   `WAL_READ_LOCK(i)`.
///
/// - **Claim (fallback):** If no existing mark matches, return
///   `Claimed { slot: i }` for the first `None` or `Some(0)` slot. The
///   caller must acquire EXCLUSIVE `WAL_READ_LOCK(i)`, write the mark, then
///   downgrade to SHARED.
///
/// - **AllSlotsBusy:** All 5 marks are occupied with different values and
///   none can be claimed. Caller returns SQLITE_BUSY.
///
/// NOTE: This function is a pure decision function. Actual lock acquisition
/// happens at the VFS layer.
#[must_use]
pub fn choose_reader_slot(
    a_read_mark: &[u32; WAL_READ_MARK_COUNT],
    desired_mark: u32,
) -> ReadLockOutcome {
    // Join fast path: look for an existing matching mark.
    for (i, &mark) in a_read_mark.iter().enumerate() {
        if mark == desired_mark {
            return ReadLockOutcome::Joined { slot: i };
        }
    }

    // Claim slow path: find a free slot (mark == 0).
    for (i, &mark) in a_read_mark.iter().enumerate() {
        if mark == 0 {
            return ReadLockOutcome::Claimed { slot: i };
        }
    }

    // All slots occupied with different marks.
    ReadLockOutcome::AllSlotsBusy
}

// ---------------------------------------------------------------------------
// CoordinatorRecovery — crash recovery (§5.6.6)
// ---------------------------------------------------------------------------

/// State observed when probing for a stale coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatorProbeResult {
    /// `WAL_WRITE_LOCK` is held by another live process — coordinator is active.
    Active,
    /// `WAL_WRITE_LOCK` is not held — no active coordinator. Safe to recover.
    NoCoordinator,
    /// `WAL_WRITE_LOCK` acquisition timed out — another process may hold it.
    Timeout,
}

/// Recovery actions after detecting no active coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryPlan {
    /// Whether the legacy WAL-index header needs to be reconstructed from
    /// the WAL file.
    pub reconstruct_wal_index: bool,
    /// Whether stale reader marks should be cleared.
    pub clear_stale_read_marks: bool,
    /// Whether `nBackfill` needs to be validated against the database file.
    pub validate_backfill: bool,
}

impl RecoveryPlan {
    /// Plan recovery based on the state of the legacy WAL-index.
    ///
    /// If the header copies don't match (indicating a crash during header
    /// update), we must reconstruct from the WAL file. If they match but
    /// `isInit` is 0, the WAL-index was never fully initialized.
    #[must_use]
    pub fn from_header_state(copies_match: bool, is_init: bool, has_stale_marks: bool) -> Self {
        Self {
            reconstruct_wal_index: !copies_match || !is_init,
            clear_stale_read_marks: has_stale_marks,
            validate_backfill: !copies_match,
        }
    }

    /// Whether any recovery work is needed.
    #[must_use]
    pub fn needs_recovery(&self) -> bool {
        self.reconstruct_wal_index || self.clear_stale_read_marks || self.validate_backfill
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_wal::wal_index::{WAL_INDEX_VERSION, WalCkptInfo, WalIndexHdr};

    const BEAD_3INZ: &str = "bd-3inz";

    /// Helper: build a minimal valid `WalIndexHdr`.
    fn make_hdr(mx_frame: u32, n_page: u32) -> WalIndexHdr {
        WalIndexHdr {
            i_version: WAL_INDEX_VERSION,
            unused: 0,
            i_change: 1,
            is_init: 1,
            big_end_cksum: 0,
            sz_page: 4096,
            mx_frame,
            n_page,
            a_frame_cksum: [0, 0],
            a_salt: [0x1234_5678, 0x9ABC_DEF0],
            a_cksum: [0, 0],
        }
    }

    /// Helper: build a minimal `WalCkptInfo`.
    fn make_ckpt() -> WalCkptInfo {
        WalCkptInfo {
            n_backfill: 0,
            a_read_mark: [0; WAL_READ_MARK_COUNT],
            a_lock: [0; 8],
            n_backfill_attempted: 0,
            not_used0: 0,
        }
    }

    // -----------------------------------------------------------------------
    // test_legacy_reader_sees_committed_data
    // -----------------------------------------------------------------------

    #[test]
    fn test_legacy_reader_sees_committed_data() {
        // bead_id=bd-3inz AC#1: Legacy readers can read committed data while
        // coordinator is active. Simulated: coordinator updates mxFrame after
        // commit; legacy reader's view of mxFrame is correct.
        let hdr = make_hdr(10, 100);
        let ckpt = make_ckpt();
        let mut state = HybridShmState::new(hdr, ckpt);
        state.mark_write_lock_held(Instant::now());

        // Simulate a commit appending frames 11..15.
        let updated = state
            .update_legacy_shm(15, 105, [0xAA, 0xBB], [0x1234_5678, 0x9ABC_DEF0])
            .expect("bead_id=bd-3inz: update should succeed while write lock held");

        assert_eq!(
            updated.hdr.mx_frame, 15,
            "bead_id={BEAD_3INZ} legacy reader should see mxFrame=15 after commit"
        );
        assert_eq!(
            updated.hdr.n_page, 105,
            "bead_id={BEAD_3INZ} legacy reader should see updated nPage"
        );

        // Verify header checksum was recomputed (non-zero after update).
        assert_ne!(
            updated.hdr.a_cksum,
            [0, 0],
            "bead_id={BEAD_3INZ} header checksum must be recomputed"
        );

        // Verify i_change was incremented (schema cookie mirror).
        assert_eq!(
            updated.hdr.i_change, 2,
            "bead_id={BEAD_3INZ} iChange should increment on commit"
        );
    }

    // -----------------------------------------------------------------------
    // test_legacy_writer_blocked
    // -----------------------------------------------------------------------

    #[test]
    fn test_legacy_writer_blocked() {
        // bead_id=bd-3inz AC#2: Legacy writers blocked with SQLITE_BUSY when
        // coordinator holds WAL_WRITE_LOCK. Simulated: update_legacy_shm
        // requires write lock; without it, coordinator rejects the operation.
        let hdr = make_hdr(5, 50);
        let ckpt = make_ckpt();
        let mut state = HybridShmState::new(hdr, ckpt);

        // Write lock NOT held — update must fail.
        let result = state.update_legacy_shm(10, 60, [0, 0], [0, 0]);
        assert!(
            result.is_err(),
            "bead_id={BEAD_3INZ} update without WAL_WRITE_LOCK must be rejected"
        );

        // Acquire write lock — now update succeeds.
        state.mark_write_lock_held(Instant::now());
        let result = state.update_legacy_shm(10, 60, [0, 0], [0, 0]);
        assert!(
            result.is_ok(),
            "bead_id={BEAD_3INZ} update with WAL_WRITE_LOCK must succeed"
        );

        // Release write lock — update must fail again (coordinator shutdown).
        state.mark_write_lock_released();
        assert!(
            !state.is_write_lock_held(),
            "bead_id={BEAD_3INZ} WAL_WRITE_LOCK must be released after coordinator shutdown"
        );
        let result = state.update_legacy_shm(11, 61, [0, 0], [0, 0]);
        assert!(
            result.is_err(),
            "bead_id={BEAD_3INZ} update after release must fail"
        );
    }

    // -----------------------------------------------------------------------
    // test_hybrid_shm_dual_maintenance
    // -----------------------------------------------------------------------

    #[test]
    fn test_hybrid_shm_dual_maintenance() {
        // bead_id=bd-3inz AC#3: Both .db-shm and .fsqlite-shm updated
        // consistently on every commit. Simulated: 5 sequential commits, verify
        // mxFrame advances monotonically and ckpt info is preserved.
        let hdr = make_hdr(0, 10);
        let ckpt = make_ckpt();
        let mut state = HybridShmState::new(hdr, ckpt);
        state.mark_write_lock_held(Instant::now());

        let mut prev_mx_frame = 0;
        for commit_num in 1..=5_u32 {
            let new_mx_frame = commit_num * 3; // 3, 6, 9, 12, 15
            let updated = state
                .update_legacy_shm(new_mx_frame, 10 + commit_num, [commit_num, 0], [0, 0])
                .unwrap_or_else(|_| unreachable!("commit {commit_num} should succeed"));

            assert!(
                updated.hdr.mx_frame > prev_mx_frame,
                "bead_id={BEAD_3INZ} mxFrame must advance monotonically: {} > {}",
                updated.hdr.mx_frame,
                prev_mx_frame
            );

            // Verify both hdr and ckpt are returned for VFS to write to both
            // .db-shm (legacy) and the VFS layer also updates .fsqlite-shm.
            assert_eq!(updated.hdr.mx_frame, new_mx_frame);
            assert_eq!(updated.hdr.a_frame_cksum[0], commit_num);
            prev_mx_frame = updated.hdr.mx_frame;
        }

        // Verify the state accumulates correctly.
        assert_eq!(
            state.legacy_hdr().mx_frame,
            15,
            "bead_id={BEAD_3INZ} final mxFrame should be 15 after 5 commits"
        );

        // Update nBackfill during checkpoint (Step 4).
        state.update_backfill(10);
        assert_eq!(
            state.legacy_ckpt().n_backfill,
            10,
            "bead_id={BEAD_3INZ} nBackfill must be updated during checkpoint"
        );
    }

    // -----------------------------------------------------------------------
    // test_fallback_to_file_locking
    // -----------------------------------------------------------------------

    #[test]
    fn test_fallback_to_file_locking() {
        // bead_id=bd-3inz AC#4 + AC#6: Graceful degradation to file-lock mode;
        // BEGIN CONCURRENT returns explicit error in no-SHM fallback.

        // FileLockOnly mode: does not support concurrent writes.
        let mode = CompatMode::FileLockOnly;
        assert!(
            !mode.supports_concurrent(),
            "bead_id={BEAD_3INZ} FileLockOnly must not support CONCURRENT"
        );
        assert!(
            !mode.requires_dual_shm(),
            "bead_id={BEAD_3INZ} FileLockOnly must not require dual SHM"
        );

        // BEGIN CONCURRENT MUST fail with ConcurrentUnavailable.
        let result = begin_concurrent_check(mode);
        assert!(
            result.is_err(),
            "bead_id={BEAD_3INZ} BEGIN CONCURRENT must fail in FileLockOnly mode"
        );
        let err = result.unwrap_err();
        assert!(
            matches!(err, FrankenError::ConcurrentUnavailable),
            "bead_id={BEAD_3INZ} error must be ConcurrentUnavailable, got: {err}"
        );

        // Verify the error maps to SQLITE_ERROR (not SQLITE_BUSY).
        assert_eq!(
            err.error_code(),
            fsqlite_error::ErrorCode::Error,
            "bead_id={BEAD_3INZ} ConcurrentUnavailable must map to SQLITE_ERROR"
        );

        // Verify there's a user-facing suggestion.
        assert!(
            err.suggestion().is_some(),
            "bead_id={BEAD_3INZ} ConcurrentUnavailable must have a suggestion"
        );

        // HybridShm mode: supports concurrent writes.
        let hybrid_mode = CompatMode::HybridShm;
        assert!(hybrid_mode.supports_concurrent());
        assert!(hybrid_mode.requires_dual_shm());
        assert!(begin_concurrent_check(hybrid_mode).is_ok());
    }

    // -----------------------------------------------------------------------
    // test_coordinator_crash_recovery
    // -----------------------------------------------------------------------

    #[test]
    fn test_coordinator_crash_recovery() {
        // bead_id=bd-3inz AC#5: System recovers correctly after crash.

        // Case 1: Clean state — no recovery needed.
        let plan = RecoveryPlan::from_header_state(
            true,  // copies match
            true,  // is_init
            false, // no stale marks
        );
        assert!(
            !plan.needs_recovery(),
            "bead_id={BEAD_3INZ} clean state should need no recovery"
        );

        // Case 2: Header copies mismatch (crash during header update).
        let plan = RecoveryPlan::from_header_state(
            false, // copies don't match — crash during write
            true,  // was initialized before crash
            false,
        );
        assert!(
            plan.needs_recovery(),
            "bead_id={BEAD_3INZ} mismatched headers should need recovery"
        );
        assert!(
            plan.reconstruct_wal_index,
            "bead_id={BEAD_3INZ} must reconstruct WAL-index after header mismatch"
        );
        assert!(
            plan.validate_backfill,
            "bead_id={BEAD_3INZ} must validate nBackfill after header mismatch"
        );

        // Case 3: WAL-index not initialized (first-time init or crash before init).
        let plan = RecoveryPlan::from_header_state(
            true,  // copies match (both zero)
            false, // not initialized
            false,
        );
        assert!(
            plan.reconstruct_wal_index,
            "bead_id={BEAD_3INZ} uninitialized WAL-index must be reconstructed"
        );

        // Case 4: Stale reader marks (legacy reader crashed while holding mark).
        let plan = RecoveryPlan::from_header_state(
            true, // copies match
            true, // initialized
            true, // stale marks
        );
        assert!(
            plan.clear_stale_read_marks,
            "bead_id={BEAD_3INZ} stale reader marks must be cleared"
        );
        assert!(
            !plan.reconstruct_wal_index,
            "bead_id={BEAD_3INZ} header is fine, no reconstruction needed"
        );

        // Case 5: Full crash scenario — everything needs recovery.
        let plan = RecoveryPlan::from_header_state(false, false, true);
        assert!(plan.needs_recovery());
        assert!(plan.reconstruct_wal_index);
        assert!(plan.clear_stale_read_marks);
        assert!(plan.validate_backfill);
    }

    // -----------------------------------------------------------------------
    // test_read_lock_protocol
    // -----------------------------------------------------------------------

    #[test]
    fn test_read_lock_protocol() {
        // bead_id=bd-3inz: WAL reader mark join/claim protocol (§5.6.7 Step 3).

        // All marks empty — first reader claims slot 0.
        let marks = [0_u32; WAL_READ_MARK_COUNT];
        let outcome = choose_reader_slot(&marks, 42);
        assert_eq!(
            outcome,
            ReadLockOutcome::Claimed { slot: 0 },
            "bead_id={BEAD_3INZ} first reader should claim slot 0 when all empty"
        );

        // Slot 0 has mark 42 — second reader joins.
        let marks = [42, 0, 0, 0, 0];
        let outcome = choose_reader_slot(&marks, 42);
        assert_eq!(
            outcome,
            ReadLockOutcome::Joined { slot: 0 },
            "bead_id={BEAD_3INZ} second reader should join existing mark"
        );

        // Slot 0 has different mark — reader claims next free slot.
        let marks = [10, 0, 0, 0, 0];
        let outcome = choose_reader_slot(&marks, 42);
        assert_eq!(
            outcome,
            ReadLockOutcome::Claimed { slot: 1 },
            "bead_id={BEAD_3INZ} should claim first free slot when no match"
        );

        // All 5 slots occupied with different marks — SQLITE_BUSY.
        let marks = [10, 20, 30, 40, 50];
        let outcome = choose_reader_slot(&marks, 42);
        assert_eq!(
            outcome,
            ReadLockOutcome::AllSlotsBusy,
            "bead_id={BEAD_3INZ} all slots busy when no match and no free slot"
        );

        // Match in the middle slot — join at slot 3.
        let marks = [10, 20, 30, 42, 50];
        let outcome = choose_reader_slot(&marks, 42);
        assert_eq!(
            outcome,
            ReadLockOutcome::Joined { slot: 3 },
            "bead_id={BEAD_3INZ} should join matching mark at any slot position"
        );

        // Verify: 5 reader marks bound distinct snapshots, not total readers.
        // Many readers can share a mark via SHARED WAL_READ_LOCK(i).
        let marks = [42, 42, 42, 42, 42]; // all 5 slots set to same mark
        let outcome = choose_reader_slot(&marks, 42);
        assert_eq!(
            outcome,
            ReadLockOutcome::Joined { slot: 0 },
            "bead_id={BEAD_3INZ} multiple slots with same mark: join first match"
        );

        // Verify: different desired mark with all slots taken.
        let outcome = choose_reader_slot(&marks, 99);
        assert_eq!(
            outcome,
            ReadLockOutcome::AllSlotsBusy,
            "bead_id={BEAD_3INZ} no free slot for different mark value"
        );
    }
}
