//! WAL checkpoint execution engine.
//!
//! Bridges the deterministic checkpoint planner ([`plan_checkpoint`]) with
//! WAL file I/O ([`WalFile`]) to backfill frames into the database.
//!
//! The split is intentional:
//! - `checkpoint.rs` is pure, deterministic planning (no I/O).
//! - This module performs the actual reads from `WalFile` and writes
//!   through [`CheckpointTarget`].
//!
//! [`CheckpointTarget`] mirrors `CheckpointPageWriter` from `fsqlite-pager`
//! but is defined here to avoid a circular crate dependency.  Higher layers
//! (`fsqlite-core`) provide an adapter bridging the two at runtime.

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::PageNumber;
use fsqlite_types::cx::Cx;
use fsqlite_vfs::VfsFile;
use tracing::{debug, info};

use crate::checkpoint::{
    CheckpointMode, CheckpointPlan, CheckpointPostAction, CheckpointProgress, CheckpointState,
    plan_checkpoint,
};
use crate::checksum::WAL_FRAME_HEADER_SIZE;
use crate::recovery_fence::{CheckpointChecksumVerdict, ExpectedPageChecksum};
use crate::wal::WalFile;

// ---------------------------------------------------------------------------
// CheckpointTarget trait
// ---------------------------------------------------------------------------

/// Write-back interface for checkpoint page transfers.
///
/// Implementors push WAL frame content into the main database file.
/// This trait is intentionally **not** sealed so that `fsqlite-core` can
/// provide the concrete adapter at runtime.
pub trait CheckpointTarget {
    /// Write `data` for `page_no` directly to the database file.
    fn write_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()>;

    /// Truncate the database file to exactly `n_pages` pages.
    fn truncate_db(&mut self, cx: &Cx, n_pages: u32) -> Result<()>;

    /// Sync the database file to stable storage.
    fn sync_db(&mut self, cx: &Cx) -> Result<()>;

    /// Read back a page's current on-disk content, if the target supports
    /// it.  Used by the post-checkpoint checksum verification path
    /// (bd-yfdb6) to confirm that the DB file matches the expected state
    /// before the WAL is truncated.
    ///
    /// The default implementation returns `None`, which skips verification.
    /// Concrete `CheckpointTarget`s that back a real VFS file should
    /// override this with the equivalent of `vfs.read(db_fd, buf, offset)`.
    fn read_page_if_supported(
        &self,
        _cx: &Cx,
        _page_no: PageNumber,
        _buf: &mut [u8],
    ) -> Result<Option<usize>> {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Execution result
// ---------------------------------------------------------------------------

/// Summary of a completed checkpoint execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointExecutionResult {
    /// The plan that was executed.
    pub plan: CheckpointPlan,
    /// Number of frames actually backfilled to the database.
    pub frames_backfilled: u32,
    /// Database size in pages reported by the last commit frame, if any.
    pub db_size_pages: Option<u32>,
    /// Whether the WAL was reset after backfill.
    pub wal_was_reset: bool,
}

// ---------------------------------------------------------------------------
// Execution entry point
// ---------------------------------------------------------------------------

/// Execute a WAL checkpoint.
///
/// 1. Computes a [`CheckpointPlan`] from `mode` and `state`.
/// 2. Reads `frames_to_backfill` frames from `wal` starting at
///    `state.backfilled_frames`.
/// 3. Writes each frame's page data through `target`.
/// 4. Syncs the database.
/// 5. Optionally resets / truncates the WAL per the plan's post-action.
///
/// # Errors
///
/// Propagates any I/O error from `WalFile`, `CheckpointTarget`, or VFS.
#[allow(clippy::too_many_lines)]
pub fn execute_checkpoint<F: VfsFile>(
    cx: &Cx,
    wal: &mut WalFile<F>,
    mode: CheckpointMode,
    state: CheckpointState,
    target: &mut impl CheckpointTarget,
) -> Result<CheckpointExecutionResult> {
    let checkpoint_start = fsqlite_types::sync_primitives::Instant::now();
    let plan = plan_checkpoint(mode, state);
    let normalized = state.normalized();

    info!(
        mode = ?plan.mode,
        frames_to_backfill = plan.frames_to_backfill,
        progress = ?plan.progress,
        blocked_by_readers = plan.blocked_by_readers,
        post_action = ?plan.post_action,
        "checkpoint plan computed"
    );

    let mut frames_backfilled: u32 = 0;
    let mut last_db_size: Option<u32> = None;
    // bd-yfdb6: accumulate expected post-checkpoint page checksums so the
    // truncate path can verify the DB state before discarding WAL frames.
    let mut expected_checksums: Vec<ExpectedPageChecksum> = Vec::new();
    if plan.frames_to_backfill > 0 {
        // Backfill frames [backfilled_frames .. backfilled_frames + frames_to_backfill).
        let start = usize::try_from(normalized.backfilled_frames).unwrap_or(usize::MAX);
        let count = usize::try_from(plan.frames_to_backfill).unwrap_or(usize::MAX);
        let end = start.saturating_add(count).min(wal.frame_count());

        let mut latest_frames: std::collections::HashMap<PageNumber, usize> =
            std::collections::HashMap::new();

        // Pass 1: Find the latest frame index for each page in the checkpoint range.
        for frame_idx in start..end {
            let header = wal.read_frame_header(cx, frame_idx)?;

            let page_no =
                PageNumber::new(header.page_number).ok_or_else(|| FrankenError::OutOfRange {
                    what: "checkpoint frame page number".to_owned(),
                    value: header.page_number.to_string(),
                })?;

            latest_frames.insert(page_no, frame_idx);
            frames_backfilled += 1;

            if header.is_commit() && header.db_size > 0 {
                last_db_size = Some(header.db_size);
            }
        }

        // Pass 2: Write deduplicated pages in sorted order to minimize disk seeks.
        let mut sorted_pages: Vec<(PageNumber, usize)> = latest_frames.into_iter().collect();
        sorted_pages.sort_unstable_by_key(|(p, _)| p.get());

        let mut frame_buf = vec![0u8; wal.frame_size()];
        for (fault_page_idx, (page_no, frame_idx)) in sorted_pages.iter().enumerate() {
            #[cfg(not(any(test, feature = "fault-injection")))]
            let _ = fault_page_idx;
            #[cfg(any(test, feature = "fault-injection"))]
            {
                if fault_page_idx > 0 {
                    crate::fault_hooks::maybe_inject_crash_at(
                        crate::fault_hooks::CrashBoundary::MidCheckpoint,
                        &format!("page_idx={fault_page_idx} page_no={}", page_no.get()),
                    )?;
                }
            }

            wal.read_frame_into(cx, *frame_idx, &mut frame_buf)?;
            let page_data = &frame_buf[WAL_FRAME_HEADER_SIZE..];
            target.write_page(cx, *page_no, page_data)?;

            // bd-yfdb6: capture the expected post-checkpoint checksum so the
            // truncate path can verify disk state before discarding WAL
            // frames. Page checksums live in the trailer; if the trailer
            // length is too small (tests with tiny pages), we skip the
            // capture silently — this is additive insurance, not a hard
            // invariant.
            if page_data.len() >= crate::checksum::PAGE_CHECKSUM_RESERVED_BYTES {
                if let Ok(checksum) = crate::checksum::read_page_checksum(page_data) {
                    expected_checksums.push(ExpectedPageChecksum {
                        page: *page_no,
                        checksum,
                    });
                }
            }

            debug!(
                frame_idx = *frame_idx,
                page_number = page_no.get(),
                "checkpoint: page backfilled"
            );
        }

        // Sync database after all frame writes.
        target.sync_db(cx)?;

        // If the checkpoint completed fully, truncate the database to the last
        // committed size.
        if matches!(plan.progress, CheckpointProgress::Complete) {
            if let Some(db_size) = last_db_size {
                target.truncate_db(cx, db_size)?;
                target.sync_db(cx)?;
            }
        }
    }

    // Post-action: reset or truncate WAL. Passes `target` so the
    // truncate path can issue an explicit fsync(db, FULL) before the
    // WAL is truncated, and the expected-checksums prefix so the same
    // path can verify on-disk DB state matches the post-checkpoint
    // state before truncating (both bd-yfdb6).
    let wal_was_reset =
        apply_checkpoint_post_action(cx, wal, plan.post_action, target, &expected_checksums)?;

    let checkpoint_duration_us = crate::metrics::duration_us_saturating(checkpoint_start.elapsed());

    info!(
        frames_backfilled,
        wal_was_reset,
        db_size_pages = ?last_db_size,
        checkpoint_duration_us,
        "checkpoint execution complete"
    );

    crate::metrics::GLOBAL_WAL_METRICS
        .record_checkpoint(u64::from(frames_backfilled), checkpoint_duration_us);

    #[cfg(any(test, feature = "fault-injection"))]
    crate::fault_hooks::maybe_inject_crash_at(
        crate::fault_hooks::CrashBoundary::AfterCheckpoint,
        &format!("frames_backfilled={frames_backfilled} wal_was_reset={wal_was_reset}"),
    )?;

    Ok(CheckpointExecutionResult {
        plan,
        frames_backfilled,
        db_size_pages: last_db_size,
        wal_was_reset,
    })
}

fn apply_checkpoint_post_action<F: VfsFile>(
    cx: &Cx,
    wal: &mut WalFile<F>,
    post_action: CheckpointPostAction,
    target: &mut impl CheckpointTarget,
    expected_checksums: &[ExpectedPageChecksum],
) -> Result<bool> {
    match post_action {
        CheckpointPostAction::ResetWal | CheckpointPostAction::TruncateWal => {
            let new_seq = wal.header().checkpoint_seq.wrapping_add(1);
            // Salt-1 increments, salt-2 randomizes (C SQLite `walRestartHdr`
            // semantics): stale frames from the pre-reset generation must
            // fail salt validation rather than replay (GH #201).
            let new_salts = wal.header().salts.next_generation();
            let truncate = matches!(post_action, CheckpointPostAction::TruncateWal);
            if truncate {
                // bd-yfdb6: enforce fsync(db, FULL) before any WAL
                // truncate. The earlier backfill loop issues `sync_db`
                // already, but we re-issue an explicit full sync here to
                // make the ordering invariant visible at the truncate
                // call-site and defensive against future changes to the
                // backfill path. A failure here MUST prevent the truncate;
                // `?` accomplishes that.
                crate::recovery_fence::ensure_db_fsync_before_wal_truncate(cx, target)?;

                // bd-yfdb6: if the target supports read-back, verify that
                // every page we just checkpointed still matches its
                // expected post-checkpoint checksum on disk. On mismatch,
                // refuse the truncate — the WAL must stay intact so a retry
                // can complete the backfill.
                if !expected_checksums.is_empty() {
                    let verdict = verify_checkpoint_checksums_via_target(
                        cx,
                        target,
                        wal.page_size(),
                        expected_checksums,
                    )?;
                    if let CheckpointChecksumVerdict::Mismatch { first_bad_page } = verdict {
                        tracing::error!(
                            target: "fsqlite.wal.recovery_fence",
                            first_bad_page = first_bad_page.get(),
                            "UNRECOVERABLE: post-checkpoint DB/WAL disagreed; refusing truncate"
                        );
                        return Err(FrankenError::DatabaseCorrupt {
                            detail: format!(
                                "post-checkpoint DB/WAL state disagreed at page {}; WAL truncate refused \
                                 to preserve committed frames (bd-yfdb6)",
                                first_bad_page.get()
                            ),
                        });
                    }
                }
            }
            wal.reset(cx, new_seq, new_salts, truncate)?;
            info!(
                new_checkpoint_seq = new_seq,
                action = ?post_action,
                truncate,
                "WAL reset after checkpoint"
            );
            Ok(true)
        }
        CheckpointPostAction::None => Ok(false),
    }
}

/// Walk each expected page through the target's optional read-back hook
/// and compare on-disk trailers. When the target opts out
/// (`read_page_if_supported` returns `None`), verification is silently
/// skipped — which matches the audit-requested behaviour of "additive
/// insurance, not a hard invariant" for targets without a read path.
fn verify_checkpoint_checksums_via_target(
    cx: &Cx,
    target: &impl CheckpointTarget,
    page_size: usize,
    expected: &[ExpectedPageChecksum],
) -> Result<CheckpointChecksumVerdict> {
    let mut buf = vec![0u8; page_size];
    for exp in expected {
        let maybe_read = target.read_page_if_supported(cx, exp.page, &mut buf)?;
        let Some(n) = maybe_read else {
            // Target does not support read-back; abort further checks.
            return Ok(CheckpointChecksumVerdict::Match);
        };
        if n < page_size {
            return Ok(CheckpointChecksumVerdict::Mismatch {
                first_bad_page: exp.page,
            });
        }
        let observed = crate::checksum::read_page_checksum(&buf)?;
        if observed != exp.checksum {
            return Ok(CheckpointChecksumVerdict::Mismatch {
                first_bad_page: exp.page,
            });
        }
    }
    Ok(CheckpointChecksumVerdict::Match)
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_vfs::MemoryVfs;
    use fsqlite_vfs::traits::Vfs;

    use super::*;
    use crate::checksum::WalSalts;

    const PAGE_SIZE: u32 = 4096;

    fn test_cx() -> Cx {
        Cx::default()
    }

    fn test_salts() -> WalSalts {
        WalSalts {
            salt1: 0xDEAD_BEEF,
            salt2: 0xCAFE_BABE,
        }
    }

    fn sample_page(seed: u8) -> Vec<u8> {
        let page_size = usize::try_from(PAGE_SIZE).expect("page size fits usize");
        let mut page = vec![0u8; page_size];
        for (i, byte) in page.iter_mut().enumerate() {
            let reduced = u8::try_from(i % 251).expect("modulo fits u8");
            *byte = reduced ^ seed;
        }
        page
    }

    fn open_wal_file(vfs: &MemoryVfs, cx: &Cx) -> <MemoryVfs as Vfs>::File {
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let (file, _) = vfs
            .open(cx, Some(std::path::Path::new("test.db-wal")), flags)
            .expect("open WAL file");
        file
    }

    /// Test target that records written pages.
    struct RecordingTarget {
        pages: Vec<(PageNumber, Vec<u8>)>,
        truncate_to: Option<u32>,
        sync_count: u32,
    }

    impl RecordingTarget {
        fn new() -> Self {
            Self {
                pages: Vec::new(),
                truncate_to: None,
                sync_count: 0,
            }
        }
    }

    impl CheckpointTarget for RecordingTarget {
        fn write_page(&mut self, _cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
            self.pages.push((page_no, data.to_vec()));
            Ok(())
        }

        fn truncate_db(&mut self, _cx: &Cx, n_pages: u32) -> Result<()> {
            self.truncate_to = Some(n_pages);
            Ok(())
        }

        fn sync_db(&mut self, _cx: &Cx) -> Result<()> {
            self.sync_count += 1;
            Ok(())
        }
    }

    /// Populate a WAL with N frames, where the last frame is a commit frame.
    fn populate_wal(wal: &mut WalFile<impl VfsFile>, cx: &Cx, n_frames: u32) {
        for i in 0..n_frames {
            let page = sample_page(u8::try_from(i & 0xFF).expect("masked to u8"));
            let db_size = if i == n_frames - 1 { n_frames } else { 0 };
            wal.append_frame(cx, i + 1, &page, db_size)
                .expect("append frame");
        }
    }

    // ── Passive mode tests ──

    #[test]
    fn test_passive_backfills_all_when_no_readers() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        populate_wal(&mut wal, &cx, 5);

        let state = CheckpointState {
            total_frames: 5,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let mut target = RecordingTarget::new();
        let result = execute_checkpoint(&cx, &mut wal, CheckpointMode::Passive, state, &mut target)
            .expect("checkpoint");

        assert_eq!(result.frames_backfilled, 5);
        assert!(result.plan.completes_checkpoint());
        assert!(!result.wal_was_reset);
        assert_eq!(target.pages.len(), 5);
        assert!(target.sync_count >= 1);
    }

    #[test]
    fn test_passive_stops_at_reader_limit() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        populate_wal(&mut wal, &cx, 10);

        let state = CheckpointState {
            total_frames: 10,
            backfilled_frames: 0,
            oldest_reader_frame: Some(6),
        };
        let mut target = RecordingTarget::new();
        let result = execute_checkpoint(&cx, &mut wal, CheckpointMode::Passive, state, &mut target)
            .expect("checkpoint");

        assert_eq!(result.frames_backfilled, 6);
        assert!(!result.plan.completes_checkpoint());
        assert!(!result.wal_was_reset);
        assert_eq!(target.pages.len(), 6);
    }

    #[test]
    fn test_passive_partial_backfill_resumes() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        populate_wal(&mut wal, &cx, 8);

        // First pass: backfill 4 frames (reader at 4).
        let state1 = CheckpointState {
            total_frames: 8,
            backfilled_frames: 0,
            oldest_reader_frame: Some(4),
        };
        let mut target1 = RecordingTarget::new();
        let r1 = execute_checkpoint(&cx, &mut wal, CheckpointMode::Passive, state1, &mut target1)
            .expect("ckpt 1");
        assert_eq!(r1.frames_backfilled, 4);

        // Second pass: reader gone, resume from frame 4.
        let state2 = CheckpointState {
            total_frames: 8,
            backfilled_frames: 4,
            oldest_reader_frame: None,
        };
        let mut target2 = RecordingTarget::new();
        let r2 = execute_checkpoint(&cx, &mut wal, CheckpointMode::Passive, state2, &mut target2)
            .expect("ckpt 2");
        assert_eq!(r2.frames_backfilled, 4);
        assert!(r2.plan.completes_checkpoint());

        // Verify pages from second pass are frames 4..8 (pages 5,6,7,8).
        let page_numbers: Vec<u32> = target2.pages.iter().map(|(pn, _)| pn.get()).collect();
        assert_eq!(page_numbers, vec![5, 6, 7, 8]);
    }

    // ── Full mode tests ──

    #[test]
    fn test_full_marks_blocked_when_reader_pins_tail() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        populate_wal(&mut wal, &cx, 10);

        let state = CheckpointState {
            total_frames: 10,
            backfilled_frames: 0,
            oldest_reader_frame: Some(7),
        };
        let mut target = RecordingTarget::new();
        let result = execute_checkpoint(&cx, &mut wal, CheckpointMode::Full, state, &mut target)
            .expect("checkpoint");

        assert_eq!(result.frames_backfilled, 7);
        assert!(!result.plan.completes_checkpoint());
        assert!(result.plan.blocked_by_readers);
    }

    #[test]
    fn test_full_completes_without_readers() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        populate_wal(&mut wal, &cx, 5);

        let state = CheckpointState {
            total_frames: 5,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let mut target = RecordingTarget::new();
        let result = execute_checkpoint(&cx, &mut wal, CheckpointMode::Full, state, &mut target)
            .expect("checkpoint");

        assert_eq!(result.frames_backfilled, 5);
        assert!(result.plan.completes_checkpoint());
        assert!(!result.plan.blocked_by_readers);
    }

    // ── Restart mode tests ──

    #[test]
    fn test_restart_resets_wal_when_complete() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        populate_wal(&mut wal, &cx, 4);

        let state = CheckpointState {
            total_frames: 4,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let mut target = RecordingTarget::new();
        let result = execute_checkpoint(&cx, &mut wal, CheckpointMode::Restart, state, &mut target)
            .expect("checkpoint");

        assert_eq!(result.frames_backfilled, 4);
        assert!(result.wal_was_reset);
        assert_eq!(wal.frame_count(), 0);
        assert_eq!(wal.header().checkpoint_seq, 1);
    }

    #[test]
    fn test_restart_skips_reset_when_reader_active() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        populate_wal(&mut wal, &cx, 4);

        let state = CheckpointState {
            total_frames: 4,
            backfilled_frames: 0,
            oldest_reader_frame: Some(4),
        };
        let mut target = RecordingTarget::new();
        let result = execute_checkpoint(&cx, &mut wal, CheckpointMode::Restart, state, &mut target)
            .expect("checkpoint");

        // All 4 are backfilled (reader at end doesn't block backfill),
        // but WAL reset is skipped because reader is present.
        assert_eq!(result.frames_backfilled, 4);
        assert!(!result.wal_was_reset);
        assert_eq!(wal.frame_count(), 4);
    }

    #[test]
    fn test_restart_resets_wal_when_already_backfilled() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        populate_wal(&mut wal, &cx, 4);

        let state = CheckpointState {
            total_frames: 4,
            backfilled_frames: 4,
            oldest_reader_frame: None,
        };
        let mut target = RecordingTarget::new();
        let result = execute_checkpoint(&cx, &mut wal, CheckpointMode::Restart, state, &mut target)
            .expect("checkpoint");

        assert_eq!(result.frames_backfilled, 0);
        assert!(result.wal_was_reset);
        assert_eq!(wal.frame_count(), 0);
        assert_eq!(wal.header().checkpoint_seq, 1);
    }

    // ── Truncate mode tests ──

    #[test]
    fn test_truncate_resets_wal_when_complete() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        populate_wal(&mut wal, &cx, 6);

        let state = CheckpointState {
            total_frames: 6,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let mut target = RecordingTarget::new();
        let result =
            execute_checkpoint(&cx, &mut wal, CheckpointMode::Truncate, state, &mut target)
                .expect("checkpoint");

        assert_eq!(result.frames_backfilled, 6);
        assert!(result.wal_was_reset);
        assert_eq!(wal.frame_count(), 0);
        assert_eq!(wal.header().checkpoint_seq, 1);
    }

    #[test]
    fn test_truncate_skips_reset_when_reader_active() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        populate_wal(&mut wal, &cx, 6);

        let state = CheckpointState {
            total_frames: 6,
            backfilled_frames: 0,
            oldest_reader_frame: Some(6),
        };
        let mut target = RecordingTarget::new();
        let result =
            execute_checkpoint(&cx, &mut wal, CheckpointMode::Truncate, state, &mut target)
                .expect("checkpoint");

        assert_eq!(result.frames_backfilled, 6);
        assert!(!result.wal_was_reset);
    }

    #[test]
    fn test_truncate_resets_wal_when_already_backfilled() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        populate_wal(&mut wal, &cx, 6);

        let state = CheckpointState {
            total_frames: 6,
            backfilled_frames: 6,
            oldest_reader_frame: None,
        };
        let mut target = RecordingTarget::new();
        let result =
            execute_checkpoint(&cx, &mut wal, CheckpointMode::Truncate, state, &mut target)
                .expect("checkpoint");

        assert_eq!(result.frames_backfilled, 0);
        assert!(result.wal_was_reset);
        assert_eq!(wal.frame_count(), 0);
        assert_eq!(wal.header().checkpoint_seq, 1);
    }

    // ── Empty / edge case tests ──

    #[test]
    fn test_checkpoint_empty_wal_is_noop() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        let state = CheckpointState {
            total_frames: 0,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let mut target = RecordingTarget::new();
        let result = execute_checkpoint(&cx, &mut wal, CheckpointMode::Passive, state, &mut target)
            .expect("checkpoint");

        assert_eq!(result.frames_backfilled, 0);
        assert!(target.pages.is_empty());
        assert_eq!(target.sync_count, 0);
    }

    #[test]
    fn test_checkpoint_already_fully_backfilled() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        populate_wal(&mut wal, &cx, 3);

        let state = CheckpointState {
            total_frames: 3,
            backfilled_frames: 3,
            oldest_reader_frame: None,
        };
        let mut target = RecordingTarget::new();
        let result = execute_checkpoint(&cx, &mut wal, CheckpointMode::Passive, state, &mut target)
            .expect("checkpoint");

        assert_eq!(result.frames_backfilled, 0);
        assert!(result.plan.completes_checkpoint());
    }

    #[test]
    fn test_checkpoint_writes_correct_page_data() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        // Append 3 frames with distinct page data.
        for i in 0..3u32 {
            let page = sample_page(u8::try_from(i).expect("fits"));
            let db_size = if i == 2 { 3 } else { 0 };
            wal.append_frame(&cx, i + 1, &page, db_size)
                .expect("append");
        }

        let state = CheckpointState {
            total_frames: 3,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let mut target = RecordingTarget::new();
        execute_checkpoint(&cx, &mut wal, CheckpointMode::Passive, state, &mut target)
            .expect("checkpoint");

        // Verify each written page matches the original data.
        for (i, (page_no, data)) in target.pages.iter().enumerate() {
            let expected_page_number = u32::try_from(i + 1).expect("fits");
            assert_eq!(page_no.get(), expected_page_number);
            let expected_data = sample_page(u8::try_from(i).expect("fits"));
            assert_eq!(*data, expected_data);
        }
    }

    #[test]
    fn test_checkpoint_db_truncation_on_complete() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        // Append 3 frames, commit frame reports db_size=3.
        populate_wal(&mut wal, &cx, 3);

        let state = CheckpointState {
            total_frames: 3,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let mut target = RecordingTarget::new();
        let result = execute_checkpoint(&cx, &mut wal, CheckpointMode::Full, state, &mut target)
            .expect("checkpoint");

        assert_eq!(result.db_size_pages, Some(3));
        assert_eq!(target.truncate_to, Some(3));
    }

    #[test]
    fn test_wal_can_accept_new_frames_after_restart() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        populate_wal(&mut wal, &cx, 4);

        let state = CheckpointState {
            total_frames: 4,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let mut target = RecordingTarget::new();
        execute_checkpoint(&cx, &mut wal, CheckpointMode::Restart, state, &mut target)
            .expect("checkpoint");

        assert_eq!(wal.frame_count(), 0);
        assert_eq!(wal.header().checkpoint_seq, 1);

        // Append new frames to the reset WAL.
        wal.append_frame(&cx, 1, &sample_page(0xAA), 0)
            .expect("append after restart");
        wal.append_frame(&cx, 2, &sample_page(0xBB), 2)
            .expect("append commit after restart");
        assert_eq!(wal.frame_count(), 2);
    }

    #[test]
    fn test_checkpoint_deduplicates_same_page_uses_latest_frame() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        let page1_v1 = sample_page(0x01);
        let page1_v2 = sample_page(0x02);
        wal.append_frame(&cx, 1, &page1_v1, 0).expect("append v1");
        wal.append_frame(&cx, 1, &page1_v2, 1).expect("append v2");

        let state = CheckpointState {
            total_frames: 2,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let mut target = RecordingTarget::new();
        let result = execute_checkpoint(&cx, &mut wal, CheckpointMode::Passive, state, &mut target)
            .expect("checkpoint");

        assert_eq!(result.frames_backfilled, 2);
        assert_eq!(
            target.pages.len(),
            1,
            "same page written twice → one deduped write"
        );
        assert_eq!(target.pages[0].0.get(), 1);
        assert_eq!(target.pages[0].1, page1_v2, "must use latest frame's data");
    }

    #[test]
    fn test_recording_target_read_page_default_returns_none() {
        let cx = test_cx();
        let target = RecordingTarget::new();
        let mut buf = vec![0u8; 4096];
        let page = PageNumber::new(1).expect("valid page");
        let result = target
            .read_page_if_supported(&cx, page, &mut buf)
            .expect("no error");
        assert!(result.is_none());
    }

    #[test]
    fn test_consecutive_restarts_bump_salts_and_checkpoint_seq() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        // Salt-2 history across generations: every reset must randomize it
        // (GH #201 — never the old deterministic +1 walk).
        let mut salt2_history = vec![wal.header().salts.salt2];
        for round in 0..3u32 {
            populate_wal(&mut wal, &cx, 2);
            let state = CheckpointState {
                total_frames: 2,
                backfilled_frames: 0,
                oldest_reader_frame: None,
            };
            let mut target = RecordingTarget::new();
            execute_checkpoint(&cx, &mut wal, CheckpointMode::Restart, state, &mut target)
                .expect("checkpoint");
            assert_eq!(wal.header().checkpoint_seq, round + 1);
            salt2_history.push(wal.header().salts.salt2);
        }
        assert_eq!(wal.header().checkpoint_seq, 3);
        let salts = wal.header().salts;
        // Salt-1 increments once per reset (C SQLite walRestartHdr).
        assert_eq!(salts.salt1, test_salts().salt1.wrapping_add(3));
        // Salt-2 is randomized per reset: each generation must differ from
        // its predecessor (a 2^-32 collision would be a red flag, and the
        // old +1 walk always "passed" — assert the walk is gone explicitly).
        for window in salt2_history.windows(2) {
            assert_ne!(window[0], window[1], "salt2 must change on every WAL reset");
            assert_ne!(
                window[1],
                window[0].wrapping_add(1),
                "salt2 must be randomized, not the deterministic +1 walk (GH #201)"
            );
        }
    }

    #[test]
    fn test_checkpoint_execution_result_fields() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        populate_wal(&mut wal, &cx, 3);

        let state = CheckpointState {
            total_frames: 3,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let mut target = RecordingTarget::new();
        let result = execute_checkpoint(&cx, &mut wal, CheckpointMode::Full, state, &mut target)
            .expect("checkpoint");

        assert_eq!(result.frames_backfilled, 3);
        assert_eq!(result.db_size_pages, Some(3));
        assert!(!result.wal_was_reset);
        assert_eq!(result.plan.mode, CheckpointMode::Full);
        assert!(result.plan.completes_checkpoint());
    }

    #[test]
    fn test_checkpoint_execution_result_clone_eq_debug() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        populate_wal(&mut wal, &cx, 2);

        let state = CheckpointState {
            total_frames: 2,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let mut target = RecordingTarget::new();
        let result = execute_checkpoint(&cx, &mut wal, CheckpointMode::Passive, state, &mut target)
            .expect("checkpoint");

        let cloned = result.clone();
        assert_eq!(result, cloned);
        let dbg = format!("{result:?}");
        assert!(dbg.contains("CheckpointExecutionResult"));
        assert!(dbg.contains("frames_backfilled"));
    }

    #[test]
    fn test_passive_syncs_exactly_once() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        populate_wal(&mut wal, &cx, 3);

        let state = CheckpointState {
            total_frames: 3,
            backfilled_frames: 0,
            oldest_reader_frame: Some(2),
        };
        let mut target = RecordingTarget::new();
        execute_checkpoint(&cx, &mut wal, CheckpointMode::Passive, state, &mut target)
            .expect("checkpoint");

        assert_eq!(
            target.sync_count, 1,
            "partial passive should sync exactly once"
        );
    }

    #[test]
    fn test_full_complete_syncs_twice_for_truncate() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        populate_wal(&mut wal, &cx, 3);

        let state = CheckpointState {
            total_frames: 3,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let mut target = RecordingTarget::new();
        execute_checkpoint(&cx, &mut wal, CheckpointMode::Full, state, &mut target)
            .expect("checkpoint");

        assert_eq!(
            target.sync_count, 2,
            "full complete with db_size truncate should sync twice"
        );
    }

    #[test]
    fn test_pages_written_in_ascending_order() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        wal.append_frame(&cx, 5, &sample_page(5), 0)
            .expect("append");
        wal.append_frame(&cx, 2, &sample_page(2), 0)
            .expect("append");
        wal.append_frame(&cx, 8, &sample_page(8), 0)
            .expect("append");
        wal.append_frame(&cx, 1, &sample_page(1), 4)
            .expect("append commit");

        let state = CheckpointState {
            total_frames: 4,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let mut target = RecordingTarget::new();
        execute_checkpoint(&cx, &mut wal, CheckpointMode::Passive, state, &mut target)
            .expect("checkpoint");

        let page_nums: Vec<u32> = target.pages.iter().map(|(pn, _)| pn.get()).collect();
        let mut sorted = page_nums.clone();
        sorted.sort_unstable();
        assert_eq!(
            page_nums, sorted,
            "pages must be written in ascending order"
        );
    }

    #[test]
    fn checkpoint_execution_result_clone_eq_debug() {
        let plan = plan_checkpoint(
            CheckpointMode::Passive,
            CheckpointState {
                total_frames: 4,
                backfilled_frames: 0,
                oldest_reader_frame: None,
            },
        );
        let result = CheckpointExecutionResult {
            plan,
            frames_backfilled: 4,
            db_size_pages: Some(10),
            wal_was_reset: false,
        };
        let cloned = result.clone();
        assert_eq!(cloned, result);
        let dbg = format!("{result:?}");
        assert!(dbg.contains("CheckpointExecutionResult"));
    }

    #[test]
    fn checkpoint_execution_result_ne_on_different_fields() {
        let plan = plan_checkpoint(
            CheckpointMode::Passive,
            CheckpointState {
                total_frames: 1,
                backfilled_frames: 0,
                oldest_reader_frame: None,
            },
        );
        let a = CheckpointExecutionResult {
            plan,
            frames_backfilled: 1,
            db_size_pages: Some(1),
            wal_was_reset: false,
        };
        let b = CheckpointExecutionResult {
            plan,
            frames_backfilled: 2,
            db_size_pages: Some(1),
            wal_was_reset: false,
        };
        assert_ne!(a, b);
    }

    #[test]
    fn checkpoint_target_default_read_page_returns_none() {
        let target = RecordingTarget::new();
        let cx = test_cx();
        let page = PageNumber::new(1).expect("valid");
        let mut buf = [0u8; 4096];
        let result = target
            .read_page_if_supported(&cx, page, &mut buf)
            .expect("ok");
        assert!(result.is_none());
    }

    #[test]
    fn empty_wal_passive_yields_zero_backfilled() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        let state = CheckpointState {
            total_frames: 0,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let mut target = RecordingTarget::new();
        let result = execute_checkpoint(&cx, &mut wal, CheckpointMode::Passive, state, &mut target)
            .expect("checkpoint");
        assert_eq!(result.frames_backfilled, 0);
        assert!(!result.wal_was_reset);
    }

    #[test]
    fn mid_checkpoint_crash_produces_partial_backfill() {
        static LOCK: crate::fault_hooks::FaultInjectionSessionLock =
            crate::fault_hooks::FaultInjectionSessionLock::new();
        let _guard = LOCK.lock().unwrap();

        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        populate_wal(&mut wal, &cx, 5);

        let state = CheckpointState {
            total_frames: 5,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };

        crate::fault_hooks::arm_crash_boundary(
            crate::fault_hooks::CrashBoundary::MidCheckpoint,
            crate::fault_hooks::FaultHookArm::new(
                "mid-ckpt-crash",
                "CHECKPOINT-MID-CRASH",
                "test_mid_checkpoint_crash",
            ),
        );

        let mut target = RecordingTarget::new();
        let err = execute_checkpoint(&cx, &mut wal, CheckpointMode::Passive, state, &mut target)
            .expect_err("should fail at MidCheckpoint boundary after first page");

        crate::fault_hooks::clear_crash_boundary();

        assert!(
            err.to_string().contains("fault_inject"),
            "error identifies the fault hook: {err}"
        );
        assert_eq!(
            target.pages.len(),
            1,
            "only the first page was written before the crash fired on page_idx=1"
        );
    }
}
