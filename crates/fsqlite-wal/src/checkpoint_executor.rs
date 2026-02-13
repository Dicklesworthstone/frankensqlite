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
use crate::checksum::WalSalts;
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
    let checkpoint_start = std::time::Instant::now();
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

    if plan.frames_to_backfill == 0 {
        return Ok(CheckpointExecutionResult {
            plan,
            frames_backfilled: 0,
            db_size_pages: None,
            wal_was_reset: false,
        });
    }

    // Backfill frames [backfilled_frames .. backfilled_frames + frames_to_backfill).
    let start = usize::try_from(normalized.backfilled_frames).unwrap_or(usize::MAX);
    let count = usize::try_from(plan.frames_to_backfill).unwrap_or(usize::MAX);
    let end = start.saturating_add(count).min(wal.frame_count());

    let mut frames_backfilled: u32 = 0;
    let mut last_db_size: Option<u32> = None;

    for frame_idx in start..end {
        let (header, page_data) = wal.read_frame(cx, frame_idx)?;

        let page_no =
            PageNumber::new(header.page_number).ok_or_else(|| FrankenError::OutOfRange {
                what: "checkpoint frame page number".to_owned(),
                value: header.page_number.to_string(),
            })?;

        target.write_page(cx, page_no, &page_data)?;
        frames_backfilled += 1;

        if header.is_commit() && header.db_size > 0 {
            last_db_size = Some(header.db_size);
        }

        debug!(
            frame_idx,
            page_number = header.page_number,
            is_commit = header.is_commit(),
            "checkpoint: frame backfilled"
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

    // Post-action: reset or truncate WAL.
    let wal_was_reset = match plan.post_action {
        CheckpointPostAction::ResetWal | CheckpointPostAction::TruncateWal => {
            let new_seq = wal.header().checkpoint_seq.wrapping_add(1);
            let new_salts = WalSalts {
                salt1: wal.header().salts.salt1.wrapping_add(1),
                salt2: wal.header().salts.salt2.wrapping_add(1),
            };
            wal.reset(cx, new_seq, new_salts)?;
            info!(
                new_checkpoint_seq = new_seq,
                action = ?plan.post_action,
                "WAL reset after checkpoint"
            );
            true
        }
        CheckpointPostAction::None => false,
    };

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

    Ok(CheckpointExecutionResult {
        plan,
        frames_backfilled,
        db_size_pages: last_db_size,
        wal_was_reset,
    })
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
}
