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

use std::future::Future;
use std::pin::Pin;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::PageNumber;
use fsqlite_types::cx::Cx;
use fsqlite_vfs::{VfsFile, VfsWriteCompletion};
use tracing::{debug, info};

use crate::checkpoint::{
    CheckpointMode, CheckpointPlan, CheckpointPostAction, CheckpointProgress, CheckpointState,
    plan_checkpoint,
};
use crate::checksum::{WAL_FRAME_HEADER_SIZE, WalHeader, WalSalts, Xxh3Checksum128};
use crate::recovery_fence::CheckpointChecksumVerdict;
use crate::wal::WalFile;

// ---------------------------------------------------------------------------
// CheckpointTarget trait
// ---------------------------------------------------------------------------

/// Object-safe future returned by [`CheckpointTarget`] operations.
///
/// Keeping the future behind a box allows checkpoint targets to remain usable
/// through `dyn CheckpointTarget` while each I/O step is awaited in sequence.
pub type CheckpointTargetFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Write-back interface for checkpoint page transfers.
///
/// Implementors push WAL frame content into the main database file.
/// This trait is intentionally **not** sealed so that `fsqlite-core` can
/// provide the concrete adapter at runtime.
pub trait CheckpointTarget: Send {
    /// Write `data` for `page_no` directly to the database file.
    fn write_page<'a>(
        &'a mut self,
        cx: &'a Cx,
        page_no: PageNumber,
        data: &'a [u8],
    ) -> CheckpointTargetFuture<'a, ()>;

    /// Truncate the database file to exactly `n_pages` pages.
    fn truncate_db<'a>(&'a mut self, cx: &'a Cx, n_pages: u32) -> CheckpointTargetFuture<'a, ()>;

    /// Sync the database file to stable storage.
    fn sync_db<'a>(&'a mut self, cx: &'a Cx) -> CheckpointTargetFuture<'a, ()>;

    /// Read back a page's current on-disk content, if the target supports
    /// it.  Used by the post-checkpoint checksum verification path
    /// (bd-yfdb6) to confirm that the DB file matches the expected state
    /// before the WAL is truncated.
    ///
    /// The default implementation returns `None`, which skips verification.
    /// Concrete `CheckpointTarget`s that back a real VFS file should
    /// override this with the equivalent of `vfs.read(db_fd, buf, offset)`.
    fn read_page_if_supported<'a>(
        &'a mut self,
        _cx: &'a Cx,
        _page_no: PageNumber,
        _buf: &'a mut [u8],
    ) -> CheckpointTargetFuture<'a, Option<usize>> {
        Box::pin(async { Ok(None) })
    }

    /// Exact page-1 header fields accepted or written by this checkpoint target.
    /// Bytes 0..8 correspond to page offsets 24..32; bytes 8..12 to 92..96.
    /// This receipt must be captured during write/normalization, never from
    /// the verification read. `None` means every WAL page byte is unchanged.
    fn checkpoint_page1_header_patch(&self) -> Option<[u8; 12]> {
        None
    }

    /// GH#399: take the cross-process gate that excludes every WAL reader
    /// pinned to the current generation while a RESTART/TRUNCATE replaces it
    /// (C SQLite holds `WAL_READ_LOCK(1..WAL_NREADER)` exclusively across
    /// `walRestartHdr`). `Ok(false)` means a peer reader still pins the
    /// generation: the post-action is skipped and the WAL is left intact.
    ///
    /// The default is the single-process behaviour (never blocked).
    fn acquire_wal_reset_gate<'a>(&'a mut self, _cx: &'a Cx) -> CheckpointTargetFuture<'a, bool> {
        Box::pin(async { Ok(true) })
    }

    /// Release the gate taken by a successful [`Self::acquire_wal_reset_gate`].
    fn release_wal_reset_gate<'a>(&'a mut self, _cx: &'a Cx) -> CheckpointTargetFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    /// Publish the durable backfill prefix for this exact WAL generation.
    /// Called only after all page writes and database syncs have succeeded.
    fn publish_backfill<'a>(
        &'a mut self,
        _cx: &'a Cx,
        _header: &'a WalHeader,
        _backfilled_frames: u32,
    ) -> CheckpointTargetFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    /// Retain a fixed reset target before its first physical header write.
    /// Called under the reader reset gate after database durability and any
    /// supported checksum verification. A returned token tracks the actual
    /// header-write source, including when this executor future is dropped.
    fn prepare_wal_reset<'a>(
        &'a mut self,
        _cx: &'a Cx,
        _header: &'a WalHeader,
        _new_checkpoint_seq: u32,
        _new_salts: WalSalts,
        _truncate: bool,
    ) -> CheckpointTargetFuture<'a, Option<VfsWriteCompletion>> {
        Box::pin(async { Ok(None) })
    }

    /// Publish the shared reset header before relinquishing reader exclusion.
    fn finish_wal_reset<'a>(&'a mut self, _cx: &'a Cx) -> CheckpointTargetFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    /// Whether a retained reset still requires physical/shared reconciliation.
    /// The owner must keep the reset gate until this obligation is terminal.
    fn wal_reset_pending(&self) -> bool {
        false
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

impl CheckpointExecutionResult {
    /// GH#399: the plan asked for a RESTART/TRUNCATE reset but a peer reader
    /// pinned the current generation, so the executor kept the WAL intact
    /// (the checkpoint effectively ran as FULL).
    #[must_use]
    pub const fn reset_deferred_by_readers(&self) -> bool {
        (self.plan.should_reset_wal() || self.plan.should_truncate_wal()) && !self.wal_was_reset
    }
}

// ---------------------------------------------------------------------------
// Execution entry point
// ---------------------------------------------------------------------------

/// The exact source frame used for one deduplicated database write. Keep its
/// frame index and digest instead of retaining a page-sized allocation; final
/// verification rereads and checks the WAL source before applying any receipt.
struct CheckpointPageExpectation {
    page: PageNumber,
    frame_index: usize,
    source_checksum: Xxh3Checksum128,
}

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
pub async fn execute_checkpoint<F: VfsFile>(
    cx: &Cx,
    wal: &mut WalFile<F>,
    mode: CheckpointMode,
    state: CheckpointState,
    target: &mut impl CheckpointTarget,
) -> Result<CheckpointExecutionResult> {
    let checkpoint_start = fsqlite_types::sync_primitives::Instant::now();

    // bd-km8qs: the plan window, progress, and post-actions all derive from the
    // caller-supplied CheckpointState; only `end` is clamped to the live WAL.
    // Revalidate that the state still describes the current WAL BEFORE copying
    // frames, resetting the WAL, or running post-actions — the production caller
    // sets `total_frames = wal.frame_count()` under the coordination guard, so a
    // mismatch means the WAL grew or shrank between planning and execution (a
    // caller-side locking regression), which would otherwise silently drop the
    // frames beyond the planned window instead of erroring.
    let live_frame_count = u32::try_from(wal.frame_count()).unwrap_or(u32::MAX);
    if state.total_frames != live_frame_count {
        return Err(FrankenError::CheckpointFailed {
            detail: format!(
                "checkpoint state is stale: total_frames={} but the live WAL has \
                 {live_frame_count} frames — a coordination guard was violated between \
                 planning and execution",
                state.total_frames
            ),
        });
    }

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
    // Retain exact written WAL pages for full-byte database readback before
    // discarding this generation. No reserved checksum trailer is required.
    let mut expected_pages: Vec<CheckpointPageExpectation> = Vec::new();
    if plan.frames_to_backfill > 0 {
        // Backfill frames [backfilled_frames .. backfilled_frames + frames_to_backfill).
        let start = usize::try_from(normalized.backfilled_frames).unwrap_or(usize::MAX);
        let count = usize::try_from(plan.frames_to_backfill).unwrap_or(usize::MAX);
        let end = start.saturating_add(count).min(wal.frame_count());

        let mut latest_frames: std::collections::HashMap<PageNumber, usize> =
            std::collections::HashMap::new();

        // Pass 1: Find the latest frame index for each page in the checkpoint range.
        for frame_idx in start..end {
            let header = wal.read_frame_header(cx, frame_idx).await?;

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

            wal.read_frame_into(cx, *frame_idx, &mut frame_buf).await?;
            let page_data = &frame_buf[WAL_FRAME_HEADER_SIZE..];
            target.write_page(cx, *page_no, page_data).await?;

            expected_pages.push(CheckpointPageExpectation {
                page: *page_no,
                frame_index: *frame_idx,
                source_checksum: Xxh3Checksum128::compute(&frame_buf),
            });

            debug!(
                frame_idx = *frame_idx,
                page_number = page_no.get(),
                "checkpoint: page backfilled"
            );
        }

        // Sync database after all frame writes.
        target.sync_db(cx).await?;

        // If the checkpoint completed fully, truncate the database to the last
        // committed size.
        if matches!(plan.progress, CheckpointProgress::Complete)
            && let Some(db_size) = last_db_size
        {
            target.truncate_db(cx, db_size).await?;
            target.sync_db(cx).await?;
            // Earlier commits may have written pages beyond the final size.
            // After durable truncation those pages are intentionally absent;
            // only the surviving database extent can be read back for reset.
            expected_pages.retain(|expected| expected.page.get() <= db_size);
        }
    }

    // Native backfill publication authorizes database-only readers, including
    // in PASSIVE/FULL mode. Detect disagreement before advertising this newly
    // written prefix. The reset path checks again after its later sync.
    require_checkpoint_pages_match(cx, wal, target, &expected_pages).await?;
    let backfilled_prefix = normalized
        .backfilled_frames
        .saturating_add(frames_backfilled);

    // Post-action: reset or truncate WAL. Passes `target` so the
    // truncate path can issue an explicit fsync(db, FULL) before the
    // WAL is truncated, and the expected-page prefix so the same
    // path can verify on-disk DB state matches the post-checkpoint
    // state before truncating (both bd-yfdb6).
    let wal_was_reset = apply_checkpoint_post_action(
        cx,
        wal,
        plan.post_action,
        target,
        &expected_pages,
        backfilled_prefix,
    )
    .await?;

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

async fn apply_checkpoint_post_action<F: VfsFile>(
    cx: &Cx,
    wal: &mut WalFile<F>,
    post_action: CheckpointPostAction,
    target: &mut impl CheckpointTarget,
    expected_pages: &[CheckpointPageExpectation],
    backfilled_prefix: u32,
) -> Result<bool> {
    match post_action {
        CheckpointPostAction::ResetWal | CheckpointPostAction::TruncateWal => {
            // GH#399: the planner only knows the reader horizon sampled
            // before backfill. A reader in another process may hold a slot at
            // exactly the current frame count (so it never limited backfill)
            // or have registered since; either still depends on this WAL
            // generation. Hold the reader slots exclusively across the reset,
            // exactly like C SQLite's `WAL_READ_LOCK(1..)` fence, and defer
            // the reset when they cannot be taken.
            if !target.acquire_wal_reset_gate(cx).await? {
                info!(
                    action = ?post_action,
                    "WAL reset deferred: a peer reader still pins the current WAL generation"
                );
                target
                    .publish_backfill(cx, wal.header(), backfilled_prefix)
                    .await?;
                return Ok(false);
            }
            let reset = replace_wal_generation(
                cx,
                wal,
                post_action,
                target,
                expected_pages,
                backfilled_prefix,
            )
            .await;
            if target.wal_reset_pending() {
                return match reset {
                    Err(error) => Err(error),
                    Ok(()) => Err(FrankenError::CheckpointFailed {
                        detail: "WAL reset returned with an unfinished publication owner"
                            .to_owned(),
                    }),
                };
            }
            let release = target.release_wal_reset_gate(cx).await;
            match (reset, release) {
                (Ok(()), Ok(())) => Ok(true),
                (Err(error), _) | (Ok(()), Err(error)) => Err(error),
            }
        }
        CheckpointPostAction::None => {
            target
                .publish_backfill(cx, wal.header(), backfilled_prefix)
                .await?;
            Ok(false)
        }
    }
}

/// Rewrite the WAL header for a fresh generation (and truncate the file for
/// TRUNCATE) once the database file is durably up to date. Callers must hold
/// the reader reset gate.
async fn replace_wal_generation<F: VfsFile>(
    cx: &Cx,
    wal: &mut WalFile<F>,
    post_action: CheckpointPostAction,
    target: &mut impl CheckpointTarget,
    expected_pages: &[CheckpointPageExpectation],
    backfilled_prefix: u32,
) -> Result<()> {
    match post_action {
        CheckpointPostAction::ResetWal | CheckpointPostAction::TruncateWal => {
            let new_seq = wal.header().checkpoint_seq.wrapping_add(1);
            // Salt-1 increments, salt-2 randomizes (C SQLite `walRestartHdr`
            // semantics): stale frames from the pre-reset generation must
            // fail salt validation rather than replay (GH #201).
            let new_salts = wal.header().salts.next_generation();
            let truncate = matches!(post_action, CheckpointPostAction::TruncateWal);
            // bd-yfdb6 / GH #193: enforce fsync(db, FULL) before ANY reset of
            // the WAL generation. The earlier backfill loop issues `sync_db`
            // already, but we re-issue an explicit full sync here to make the
            // ordering invariant visible at the invalidation call-site and
            // defensive against future changes to the backfill path. Both
            // RESTART (`ResetWal`) and TRUNCATE (`TruncateWal`) rewrite the
            // header with fresh salts, which invalidates every frame of the
            // prior generation; recovery can no longer replay them, so the
            // database file must be durably up to date first. A failure here
            // MUST prevent the reset; `?` accomplishes that.
            target.sync_db(cx).await.map_err(|err| {
                tracing::error!(
                    target: "fsqlite.wal.recovery_fence",
                    error = %err,
                    "fsync(db) before WAL truncate failed; refusing to truncate"
                );
                err
            })?;

            // bd-yfdb6: if the target supports read-back, verify that
            // every surviving page we just checkpointed still matches its
            // expected complete post-checkpoint bytes on disk. On mismatch,
            // refuse the reset — the WAL must stay intact so a retry
            // can complete the backfill.
            require_checkpoint_pages_match(cx, wal, target, expected_pages).await?;
            // No database writes/syncs follow this publication. Publishing
            // before the final sync could advertise a copy that later fails
            // verification while cleanup releases the database-only reader gate.
            target
                .publish_backfill(cx, wal.header(), backfilled_prefix)
                .await?;
            let completion = target
                .prepare_wal_reset(cx, wal.header(), new_seq, new_salts, truncate)
                .await?;
            if let Some(completion) = completion {
                wal.reset_tracked(cx, new_seq, new_salts, truncate, completion)
                    .await?;
            } else {
                wal.reset(cx, new_seq, new_salts, truncate).await?;
            }
            target.finish_wal_reset(cx).await?;
            info!(
                new_checkpoint_seq = new_seq,
                action = ?post_action,
                truncate,
                "WAL reset after checkpoint"
            );
            Ok(())
        }
        CheckpointPostAction::None => Ok(()),
    }
}

async fn require_checkpoint_pages_match<F: VfsFile>(
    cx: &Cx,
    wal: &WalFile<F>,
    target: &mut impl CheckpointTarget,
    expected: &[CheckpointPageExpectation],
) -> Result<()> {
    if expected.is_empty() {
        return Ok(());
    }
    match verify_checkpoint_pages_via_target(cx, wal, target, expected).await? {
        CheckpointChecksumVerdict::Match => Ok(()),
        CheckpointChecksumVerdict::Mismatch { first_bad_page } => {
            tracing::error!(
                target: "fsqlite.wal.recovery_fence",
                first_bad_page = first_bad_page.get(),
                "post-checkpoint DB/WAL disagreed; refusing publication or reset"
            );
            Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "post-checkpoint DB/WAL state disagreed at page {}; publication or \
                     WAL reset refused to preserve committed frames (bd-yfdb6)",
                    first_bad_page.get()
                ),
            })
        }
    }
}

/// Walk each expected page through the target's optional read-back hook
/// and compare all bytes against the unchanged WAL source plus the target's
/// exact page-1 normalization receipt. When the target opts out
/// (`read_page_if_supported` returns `None`), verification is silently
/// skipped — which matches the audit-requested behaviour of "additive
/// insurance, not a hard invariant" for targets without a read path.
async fn verify_checkpoint_pages_via_target<F: VfsFile>(
    cx: &Cx,
    wal: &WalFile<F>,
    target: &mut impl CheckpointTarget,
    expected: &[CheckpointPageExpectation],
) -> Result<CheckpointChecksumVerdict> {
    let page_size = wal.page_size();
    let mut buf = vec![0u8; page_size];
    let mut frame_buf = vec![0; wal.frame_size()];
    let page1_patch = target.checkpoint_page1_header_patch();
    for exp in expected {
        let maybe_read = target
            .read_page_if_supported(cx, exp.page, &mut buf)
            .await?;
        let Some(n) = maybe_read else {
            // Target does not support read-back; abort further checks.
            return Ok(CheckpointChecksumVerdict::Match);
        };
        if n != page_size {
            return Ok(CheckpointChecksumVerdict::Mismatch {
                first_bad_page: exp.page,
            });
        }
        let header = wal
            .read_frame_into(cx, exp.frame_index, &mut frame_buf)
            .await?;
        if header.page_number != exp.page.get()
            || header.salts != wal.header().salts
            || !exp.source_checksum.verify(&frame_buf)
        {
            return Err(FrankenError::WalCorrupt {
                detail: "checkpoint verification source changed after its database write"
                    .to_owned(),
            });
        }
        let page = &mut frame_buf[WAL_FRAME_HEADER_SIZE..];
        if exp.page == PageNumber::ONE
            && let Some(patch) = page1_patch
        {
            if page.len() < 96 {
                return Err(FrankenError::WalCorrupt {
                    detail: "checkpoint page-1 patch exceeds the WAL page".to_owned(),
                });
            }
            page[24..32].copy_from_slice(&patch[..8]);
            page[92..96].copy_from_slice(&patch[8..]);
        }
        if page != buf.as_slice() {
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
    use crate::test_support::FutureResultTestExt as _;

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
        fn write_page<'a>(
            &'a mut self,
            _cx: &'a Cx,
            page_no: PageNumber,
            data: &'a [u8],
        ) -> CheckpointTargetFuture<'a, ()> {
            Box::pin(async move {
                self.pages.push((page_no, data.to_vec()));
                Ok(())
            })
        }

        fn truncate_db<'a>(
            &'a mut self,
            _cx: &'a Cx,
            n_pages: u32,
        ) -> CheckpointTargetFuture<'a, ()> {
            Box::pin(async move {
                self.truncate_to = Some(n_pages);
                Ok(())
            })
        }

        fn sync_db<'a>(&'a mut self, _cx: &'a Cx) -> CheckpointTargetFuture<'a, ()> {
            Box::pin(async move {
                self.sync_count += 1;
                Ok(())
            })
        }
    }

    /// Target with actual VFS bytes, including EOF after truncation. Corruption
    /// is injected once at the first database sync, after backfill writes.
    struct ReadbackTarget {
        file: <MemoryVfs as Vfs>::File,
        written_pages: Vec<PageNumber>,
        read_pages: Vec<PageNumber>,
        truncate_to: Option<u32>,
        corrupt_on_sync: Option<(PageNumber, usize)>,
        corrupt_sync_call: u32,
        fail_sync_call: Option<u32>,
        published_prefixes: Vec<u32>,
        apply_page1_patch_on_sync: Option<[u8; 12]>,
        page1_header_patch: Option<[u8; 12]>,
        sync_count: u32,
        allow_reset: bool,
        gate_acquired: u32,
        gate_released: u32,
    }

    impl ReadbackTarget {
        fn new(vfs: &MemoryVfs, cx: &Cx) -> Self {
            let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE;
            let (file, _) = vfs
                .open(cx, Some(std::path::Path::new("readback.db")), flags)
                .expect("open database");
            Self {
                file,
                written_pages: Vec::new(),
                read_pages: Vec::new(),
                truncate_to: None,
                corrupt_on_sync: None,
                corrupt_sync_call: 1,
                fail_sync_call: None,
                published_prefixes: Vec::new(),
                apply_page1_patch_on_sync: None,
                page1_header_patch: None,
                sync_count: 0,
                allow_reset: true,
                gate_acquired: 0,
                gate_released: 0,
            }
        }
    }

    impl CheckpointTarget for ReadbackTarget {
        fn write_page<'a>(
            &'a mut self,
            cx: &'a Cx,
            page_no: PageNumber,
            data: &'a [u8],
        ) -> CheckpointTargetFuture<'a, ()> {
            Box::pin(async move {
                let offset = u64::from(page_no.get() - 1) * u64::from(PAGE_SIZE);
                self.file.write(cx, data, offset).await?;
                self.written_pages.push(page_no);
                Ok(())
            })
        }

        fn truncate_db<'a>(
            &'a mut self,
            cx: &'a Cx,
            n_pages: u32,
        ) -> CheckpointTargetFuture<'a, ()> {
            Box::pin(async move {
                self.file
                    .truncate(cx, u64::from(n_pages) * u64::from(PAGE_SIZE))?;
                self.truncate_to = Some(n_pages);
                Ok(())
            })
        }

        fn sync_db<'a>(&'a mut self, cx: &'a Cx) -> CheckpointTargetFuture<'a, ()> {
            Box::pin(async move {
                if let Some(patch) = self.apply_page1_patch_on_sync.take() {
                    self.file.write(cx, &patch[..8], 24).await?;
                    self.file.write(cx, &patch[8..], 92).await?;
                    self.page1_header_patch = Some(patch);
                }
                if self.sync_count + 1 == self.corrupt_sync_call
                    && let Some((page, byte)) = self.corrupt_on_sync.take()
                {
                    let offset = u64::from(page.get() - 1) * u64::from(PAGE_SIZE)
                        + u64::try_from(byte).expect("page offset fits u64");
                    let mut value = [0];
                    assert_eq!(self.file.read(cx, &mut value, offset).await?, 1);
                    value[0] ^= 0x80;
                    self.file.write(cx, &value, offset).await?;
                }
                self.file.sync(cx, fsqlite_types::flags::SyncFlags::FULL)?;
                if self.fail_sync_call == Some(self.sync_count + 1) {
                    return Err(FrankenError::Io(std::io::Error::other(
                        "injected database sync failure at final reset fence",
                    )));
                }
                self.sync_count += 1;
                Ok(())
            })
        }

        fn read_page_if_supported<'a>(
            &'a mut self,
            cx: &'a Cx,
            page_no: PageNumber,
            buf: &'a mut [u8],
        ) -> CheckpointTargetFuture<'a, Option<usize>> {
            Box::pin(async move {
                assert!(self.sync_count > 0);
                self.read_pages.push(page_no);
                let offset = u64::from(page_no.get() - 1) * u64::from(PAGE_SIZE);
                self.file.read(cx, buf, offset).await.map(Some)
            })
        }

        fn checkpoint_page1_header_patch(&self) -> Option<[u8; 12]> {
            self.page1_header_patch
        }

        fn publish_backfill<'a>(
            &'a mut self,
            _cx: &'a Cx,
            _header: &'a WalHeader,
            frames: u32,
        ) -> CheckpointTargetFuture<'a, ()> {
            Box::pin(async move {
                self.published_prefixes.push(frames);
                Ok(())
            })
        }

        fn acquire_wal_reset_gate<'a>(
            &'a mut self,
            _cx: &'a Cx,
        ) -> CheckpointTargetFuture<'a, bool> {
            Box::pin(async move {
                if self.allow_reset {
                    self.gate_acquired += 1;
                }
                Ok(self.allow_reset)
            })
        }

        fn release_wal_reset_gate<'a>(&'a mut self, _cx: &'a Cx) -> CheckpointTargetFuture<'a, ()> {
            Box::pin(async move {
                self.gate_released += 1;
                Ok(())
            })
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
    fn test_stale_checkpoint_state_after_wal_growth_errors_bd_km8qs() {
        // bd-km8qs: a CheckpointState planned against an earlier WAL snapshot must
        // be rejected if the WAL grew before execution (a coordination-guard
        // regression), rather than silently checkpointing only the planned window.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        populate_wal(&mut wal, &cx, 3);

        // Plan against the 3-frame snapshot.
        let state = CheckpointState {
            total_frames: 3,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };

        // The WAL grows before execution — the state is now stale.
        let extra = sample_page(0xAB);
        wal.append_frame(&cx, 4, &extra, 0)
            .expect("append extra frame");
        wal.append_frame(&cx, 5, &extra, 5)
            .expect("append extra commit frame");
        assert_eq!(wal.frame_count(), 5);

        let mut target = RecordingTarget::new();
        let err = execute_checkpoint(&cx, &mut wal, CheckpointMode::Passive, state, &mut target)
            .expect_err("a stale checkpoint state must be rejected");
        assert!(matches!(err, FrankenError::CheckpointFailed { .. }));
        assert!(
            target.pages.is_empty(),
            "no frames may be copied before the stale-state guard fires"
        );
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

    /// GH#399: a target whose cross-process reader gate refuses (a peer
    /// process still pins the current WAL generation) keeps the WAL intact
    /// even though the plan asked for a reset, and reports the deferral.
    struct GatedTarget {
        inner: RecordingTarget,
        allow_reset: bool,
        gate_acquired: u32,
        gate_released: u32,
        reset_target: Option<(u32, WalSalts, bool)>,
        reset_completion: Option<VfsWriteCompletion>,
        fail_reset_publication: bool,
    }

    impl CheckpointTarget for GatedTarget {
        fn write_page<'a>(
            &'a mut self,
            cx: &'a Cx,
            page_no: PageNumber,
            data: &'a [u8],
        ) -> CheckpointTargetFuture<'a, ()> {
            self.inner.write_page(cx, page_no, data)
        }

        fn truncate_db<'a>(
            &'a mut self,
            cx: &'a Cx,
            n_pages: u32,
        ) -> CheckpointTargetFuture<'a, ()> {
            self.inner.truncate_db(cx, n_pages)
        }

        fn sync_db<'a>(&'a mut self, cx: &'a Cx) -> CheckpointTargetFuture<'a, ()> {
            self.inner.sync_db(cx)
        }

        fn acquire_wal_reset_gate<'a>(
            &'a mut self,
            _cx: &'a Cx,
        ) -> CheckpointTargetFuture<'a, bool> {
            Box::pin(async move {
                if self.allow_reset {
                    self.gate_acquired += 1;
                }
                Ok(self.allow_reset)
            })
        }

        fn release_wal_reset_gate<'a>(&'a mut self, _cx: &'a Cx) -> CheckpointTargetFuture<'a, ()> {
            Box::pin(async move {
                assert!(
                    self.reset_target.is_none(),
                    "publication precedes reader release"
                );
                self.gate_released += 1;
                Ok(())
            })
        }

        fn prepare_wal_reset<'a>(
            &'a mut self,
            _cx: &'a Cx,
            header: &'a WalHeader,
            new_checkpoint_seq: u32,
            new_salts: WalSalts,
            truncate: bool,
        ) -> CheckpointTargetFuture<'a, Option<VfsWriteCompletion>> {
            Box::pin(async move {
                assert_eq!(self.gate_acquired, 1);
                assert_eq!(self.gate_released, 0);
                assert!(
                    self.inner.sync_count > 0,
                    "database durability precedes reset"
                );
                assert_eq!(new_checkpoint_seq, header.checkpoint_seq.wrapping_add(1));
                assert!(self.reset_target.is_none());
                self.reset_target = Some((new_checkpoint_seq, new_salts, truncate));
                let completion = VfsWriteCompletion::new();
                self.reset_completion = Some(completion.clone());
                Ok(Some(completion))
            })
        }

        fn finish_wal_reset<'a>(&'a mut self, _cx: &'a Cx) -> CheckpointTargetFuture<'a, ()> {
            Box::pin(async move {
                assert_eq!(self.gate_released, 0);
                assert!(self.reset_target.is_some());
                assert_eq!(
                    self.reset_completion.as_ref().unwrap().state(),
                    fsqlite_vfs::VfsWriteCompletionState::Success
                );
                if self.fail_reset_publication {
                    return Err(FrankenError::BusyRecovery);
                }
                self.reset_target = None;
                Ok(())
            })
        }

        fn wal_reset_pending(&self) -> bool {
            self.reset_target.is_some()
        }
    }

    #[test]
    fn test_truncate_defers_reset_when_peer_reader_gate_refuses_gh399() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        populate_wal(&mut wal, &cx, 6);

        // No reader was visible when the plan was sampled, so the plan asks
        // for a truncating reset ...
        let state = CheckpointState {
            total_frames: 6,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let mut target = GatedTarget {
            inner: RecordingTarget::new(),
            allow_reset: false,
            gate_acquired: 0,
            gate_released: 0,
            reset_target: None,
            reset_completion: None,
            fail_reset_publication: false,
        };
        let result =
            execute_checkpoint(&cx, &mut wal, CheckpointMode::Truncate, state, &mut target)
                .expect("checkpoint");

        // ... every frame is still backfilled ...
        assert_eq!(result.frames_backfilled, 6);
        assert!(result.plan.should_truncate_wal());
        assert!(result.plan.completes_checkpoint());
        // ... but the generation a peer reader pins survives untouched.
        assert!(!result.wal_was_reset);
        assert!(result.reset_deferred_by_readers());
        assert_eq!(wal.frame_count(), 6);
        assert_eq!(wal.header().checkpoint_seq, 0);
        assert_eq!(wal.header().salts, test_salts());
        assert_eq!(target.gate_acquired, 0);
        assert_eq!(target.gate_released, 0, "a refused gate is never released");
    }

    #[test]
    fn test_truncate_holds_reader_gate_across_reset_gh399() {
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
        let mut target = GatedTarget {
            inner: RecordingTarget::new(),
            allow_reset: true,
            gate_acquired: 0,
            gate_released: 0,
            reset_target: None,
            reset_completion: None,
            fail_reset_publication: false,
        };
        let result =
            execute_checkpoint(&cx, &mut wal, CheckpointMode::Truncate, state, &mut target)
                .expect("checkpoint");

        assert!(result.wal_was_reset);
        assert!(!result.reset_deferred_by_readers());
        assert_eq!(wal.frame_count(), 0);
        assert_eq!(wal.header().checkpoint_seq, 1);
        assert_eq!(target.gate_acquired, 1);
        assert_eq!(target.gate_released, 1, "the gate is released exactly once");
    }

    #[test]
    fn test_reset_publication_failure_keeps_reader_gate_until_retry() {
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
        let mut target = GatedTarget {
            inner: RecordingTarget::new(),
            allow_reset: true,
            gate_acquired: 0,
            gate_released: 0,
            reset_target: None,
            reset_completion: None,
            fail_reset_publication: true,
        };
        assert!(matches!(
            execute_checkpoint(&cx, &mut wal, CheckpointMode::Truncate, state, &mut target)
                .expect_err("publication failure"),
            FrankenError::BusyRecovery
        ));
        assert_eq!(wal.frame_count(), 0, "physical reset already completed");
        let (sequence, salts, truncate) = target.reset_target.expect("exact retained target");
        assert_eq!(sequence, wal.header().checkpoint_seq);
        assert_eq!(salts, wal.header().salts);
        assert!(truncate);
        assert_eq!(target.gate_acquired, 1);
        assert_eq!(
            target.gate_released, 0,
            "failed publication keeps the reader gate"
        );
        target.fail_reset_publication = false;
        target
            .finish_wal_reset(&cx)
            .expect("retry exact publication");
        target
            .release_wal_reset_gate(&cx)
            .expect("terminal release");
        assert!(!target.wal_reset_pending());
        assert_eq!(target.gate_released, 1);
        assert_eq!(
            wal.header().salts,
            salts,
            "retry never picks another generation"
        );
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

    fn checkpoint_file_bytes(file: &impl VfsFile, cx: &Cx) -> Vec<u8> {
        let size = usize::try_from(file.file_size(cx).expect("file size")).unwrap();
        let mut bytes = vec![0; size];
        assert_eq!(
            file.read(cx, &mut bytes, 0).expect("complete file read"),
            size
        );
        bytes
    }

    #[test]
    fn test_checkpoint_empty_wal_non_truncating_modes_are_noop() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        let header = *wal.header();

        let state = CheckpointState {
            total_frames: 0,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        for mode in [
            CheckpointMode::Passive,
            CheckpointMode::Full,
            CheckpointMode::Restart,
        ] {
            let mut target = RecordingTarget::new();
            let result =
                execute_checkpoint(&cx, &mut wal, mode, state, &mut target).expect("checkpoint");

            assert_eq!(result.frames_backfilled, 0);
            assert!(!result.wal_was_reset);
            assert!(target.pages.is_empty());
            assert_eq!(target.truncate_to, None);
            assert_eq!(target.sync_count, 0);
            assert_eq!(*wal.header(), header);
        }
    }

    #[test]
    fn test_empty_truncate_removes_restart_tail_after_reader_gate_retry() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        populate_wal(&mut wal, &cx, 3);
        let populated_size = wal.file().file_size(&cx).unwrap();
        let mut backfill = ReadbackTarget::new(&vfs, &cx);
        let restart = execute_checkpoint(
            &cx,
            &mut wal,
            CheckpointMode::Restart,
            CheckpointState {
                total_frames: 3,
                backfilled_frames: 0,
                oldest_reader_frame: None,
            },
            &mut backfill,
        )
        .expect("restart after actual database backfill");
        assert!(restart.wal_was_reset);
        assert_eq!(wal.frame_count(), 0);
        assert_eq!(wal.file().file_size(&cx).unwrap(), populated_size);
        let stale_wal = checkpoint_file_bytes(wal.file(), &cx);
        let database = checkpoint_file_bytes(&backfill.file, &cx);
        assert_eq!(database.len(), usize::try_from(3 * PAGE_SIZE).unwrap());
        assert!(stale_wal.len() > crate::checksum::WAL_HEADER_SIZE);

        // A fresh pass has no database mutations. This gate is synthetic;
        // the backing WAL and database byte-preservation checks are actual I/O.
        let mut target = ReadbackTarget::new(&vfs, &cx);
        target.allow_reset = false;
        let state = CheckpointState {
            total_frames: 0,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let refused =
            execute_checkpoint(&cx, &mut wal, CheckpointMode::Truncate, state, &mut target)
                .expect("reader defers empty truncate");
        assert!(refused.reset_deferred_by_readers());
        assert!(!refused.wal_was_reset);
        assert_eq!((target.gate_acquired, target.gate_released), (0, 0));
        assert_eq!(target.sync_count, 0);
        assert_eq!(checkpoint_file_bytes(wal.file(), &cx), stale_wal);
        assert_eq!(checkpoint_file_bytes(&target.file, &cx), database);

        target.allow_reset = true;
        let truncated =
            execute_checkpoint(&cx, &mut wal, CheckpointMode::Truncate, state, &mut target)
                .expect("truncate stale bytes after reader release");
        assert!(truncated.wal_was_reset);
        assert_eq!(truncated.frames_backfilled, 0);
        assert_eq!(truncated.db_size_pages, None);
        assert_eq!((target.gate_acquired, target.gate_released), (1, 1));
        assert_eq!(target.sync_count, 1);
        assert!(target.written_pages.is_empty());
        assert!(
            target.read_pages.is_empty(),
            "no prior-prefix readback claim"
        );
        assert_eq!(target.truncate_to, None);
        assert_eq!(
            wal.file().file_size(&cx).unwrap(),
            u64::try_from(crate::checksum::WAL_HEADER_SIZE).unwrap()
        );
        assert_eq!(checkpoint_file_bytes(&target.file, &cx), database);
    }

    #[test]
    fn test_empty_truncate_retains_exact_reset_when_publication_fails() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        let state = CheckpointState {
            total_frames: 0,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let mut target = GatedTarget {
            inner: RecordingTarget::new(),
            allow_reset: true,
            gate_acquired: 0,
            gate_released: 0,
            reset_target: None,
            reset_completion: None,
            fail_reset_publication: true,
        };
        assert!(matches!(
            execute_checkpoint(&cx, &mut wal, CheckpointMode::Truncate, state, &mut target)
                .expect_err("synthetic publication failure after physical reset"),
            FrankenError::BusyRecovery
        ));
        let reset_header = *wal.header();
        let retained = target.reset_target.expect("retain exact empty reset");
        assert_eq!(
            retained,
            (reset_header.checkpoint_seq, reset_header.salts, true)
        );
        assert_eq!(
            target.reset_completion.as_ref().unwrap().state(),
            fsqlite_vfs::VfsWriteCompletionState::Success
        );
        assert!(target.wal_reset_pending());
        assert_eq!((target.gate_acquired, target.gate_released), (1, 0));
        assert!(target.inner.pages.is_empty());
        assert_eq!(target.inner.truncate_to, None);
        assert_eq!(target.inner.sync_count, 1);
        target.fail_reset_publication = false;
        target
            .finish_wal_reset(&cx)
            .expect("retry same publication");
        target
            .release_wal_reset_gate(&cx)
            .expect("release exact gate");
        assert!(!target.wal_reset_pending());
        assert_eq!(target.gate_released, 1);
        assert_eq!(*wal.header(), reset_header);
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
    fn test_checkpoint_shrink_verifies_only_surviving_pages_before_reset() {
        for mode in [CheckpointMode::Restart, CheckpointMode::Truncate] {
            let cx = test_cx();
            let vfs = MemoryVfs::new();
            let file = open_wal_file(&vfs, &cx);
            let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
            wal.append_frame(&cx, 4, &sample_page(4), 4)
                .expect("older large commit");
            let final_page = sample_page(1);
            wal.append_frame(&cx, 1, &final_page, 2)
                .expect("newer shrinking commit");
            let mut target = ReadbackTarget::new(&vfs, &cx);
            let state = CheckpointState {
                total_frames: 2,
                backfilled_frames: 0,
                oldest_reader_frame: None,
            };
            let result = execute_checkpoint(&cx, &mut wal, mode, state, &mut target)
                .expect("removed pages are outside the durable database extent");
            assert_eq!(result.db_size_pages, Some(2));
            assert_eq!(result.frames_backfilled, 2);
            assert_eq!(target.truncate_to, Some(2));
            assert_eq!(target.written_pages.len(), 2);
            assert_eq!(
                target.read_pages,
                vec![PageNumber::ONE, PageNumber::ONE],
                "verify before backfill publication and again before reset"
            );
            assert_eq!(target.published_prefixes, vec![2]);
            assert_eq!(
                target.file.file_size(&cx).expect("database length"),
                2 * u64::from(PAGE_SIZE)
            );
            let mut observed = vec![0; final_page.len()];
            target
                .file
                .read(&cx, &mut observed, 0)
                .expect("surviving page");
            assert_eq!(observed, final_page);
            assert!(result.wal_was_reset);
            assert_eq!(wal.frame_count(), 0);
            assert_eq!(wal.header().checkpoint_seq, 1);
            assert_eq!((target.gate_acquired, target.gate_released), (1, 1));
        }
    }

    fn assert_checkpoint_readback_corruption_refuses_reset(byte: usize) {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        let page = PageNumber::new(2).expect("valid page");
        let expected = sample_page(2);
        wal.append_frame(&cx, page.get(), &expected, 2)
            .expect("commit surviving page");
        let before = wal.header().to_bytes().expect("old header");
        let mut target = ReadbackTarget::new(&vfs, &cx);
        target.corrupt_on_sync = Some((page, byte));
        let state = CheckpointState {
            total_frames: 1,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let result =
            execute_checkpoint(&cx, &mut wal, CheckpointMode::Truncate, state, &mut target).wait();
        let mut observed = vec![0; expected.len()];
        target
            .file
            .read(&cx, &mut observed, u64::from(PAGE_SIZE))
            .expect("actual corrupted bytes");
        assert_eq!(observed[byte], expected[byte] ^ 0x80);
        let trailer_start = expected.len() - crate::checksum::PAGE_CHECKSUM_RESERVED_BYTES;
        if byte < trailer_start {
            assert_eq!(observed[trailer_start..], expected[trailer_start..]);
        }
        assert!(matches!(
            result.expect_err("readback mismatch must preserve the committed WAL"),
            FrankenError::DatabaseCorrupt { .. }
        ));
        assert_eq!(target.read_pages, vec![page]);
        assert_eq!(wal.frame_count(), 1);
        assert_eq!(wal.header().to_bytes().expect("retained header"), before);
        assert!(target.published_prefixes.is_empty());
        assert_eq!(
            (target.gate_acquired, target.gate_released),
            (0, 0),
            "early mismatch precedes reset-gate acquisition"
        );
    }

    #[test]
    fn test_checkpoint_readback_body_corruption_refuses_reset() {
        // The actual page body changes at sync while its trailer stays equal.
        assert_checkpoint_readback_corruption_refuses_reset(100);
    }

    #[test]
    fn test_checkpoint_readback_in_range_trailer_corruption_refuses_reset() {
        assert_checkpoint_readback_corruption_refuses_reset(
            usize::try_from(PAGE_SIZE).expect("page size fits usize") - 1,
        );
    }

    #[test]
    fn test_checkpoint_readback_rejects_changed_wal_source() {
        for changed_byte in [4, WAL_FRAME_HEADER_SIZE + 100] {
            let cx = test_cx();
            let vfs = MemoryVfs::new();
            let file = open_wal_file(&vfs, &cx);
            let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
            let page = PageNumber::new(2).expect("valid page");
            let data = sample_page(2);
            wal.append_frame(&cx, 2, &data, 2).expect("commit page");
            let mut frame = vec![0; wal.frame_size()];
            wal.read_frame_into(&cx, 0, &mut frame)
                .expect("capture source");
            let expected = [CheckpointPageExpectation {
                page,
                frame_index: 0,
                source_checksum: Xxh3Checksum128::compute(&frame),
            }];
            let mut target = ReadbackTarget::new(&vfs, &cx);
            target
                .write_page(&cx, page, &data)
                .expect("backfill source");
            target.sync_db(&cx).expect("sync database");
            let old_header = wal.header().to_bytes().expect("generation");
            let offset = u64::try_from(crate::checksum::WAL_HEADER_SIZE + changed_byte)
                .expect("frame offset fits u64");
            wal.file()
                .write(&cx, &[frame[changed_byte] ^ 0x80], offset)
                .expect("mutate source header or body after backfill");
            assert!(matches!(
                verify_checkpoint_pages_via_target(&cx, &wal, &mut target, &expected)
                    .expect_err("changed WAL bytes cannot become a new expected page"),
                FrankenError::WalCorrupt { .. }
            ));
            assert_eq!(wal.frame_count(), 1);
            assert_eq!(
                wal.header().to_bytes().expect("retained generation"),
                old_header
            );
        }
    }

    #[test]
    fn test_checkpoint_readback_accepts_exact_page1_patch_receipt() {
        assert_checkpoint_page1_patch_readback(None);
    }

    #[test]
    fn test_checkpoint_readback_refuses_corrupted_page1_stamp() {
        for offset in [24, 28, 92] {
            assert_checkpoint_page1_patch_readback(Some(offset));
        }
    }

    fn assert_checkpoint_page1_patch_readback(corrupt_offset: Option<usize>) {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        let mut page = sample_page(1);
        page[..16].copy_from_slice(b"SQLite format 3\0");
        page[20] = 0; // Stock format: no reserved checksum bytes.
        page[24..28].copy_from_slice(&3_u32.to_be_bytes());
        page[28..32].copy_from_slice(&4_u32.to_be_bytes());
        page[92..96].copy_from_slice(&3_u32.to_be_bytes());
        wal.append_frame(&cx, 1, &page, 2).expect("commit page 1");
        let old_header = wal.header().to_bytes().expect("old generation");
        let mut patch = [0; 12];
        patch[..4].copy_from_slice(&7_u32.to_be_bytes());
        patch[4..8].copy_from_slice(&2_u32.to_be_bytes());
        patch[8..].copy_from_slice(&7_u32.to_be_bytes());
        let mut target = ReadbackTarget::new(&vfs, &cx);
        target.apply_page1_patch_on_sync = Some(patch);
        target.corrupt_on_sync = corrupt_offset.map(|offset| (PageNumber::ONE, offset));
        let state = CheckpointState {
            total_frames: 1,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let result =
            execute_checkpoint(&cx, &mut wal, CheckpointMode::Truncate, state, &mut target).wait();
        assert_eq!(target.checkpoint_page1_header_patch(), Some(patch));
        let expected_reads = if corrupt_offset.is_some() { 1 } else { 2 };
        assert_eq!(target.read_pages, vec![PageNumber::ONE; expected_reads]);
        let mut observed = vec![0; page.len()];
        target
            .file
            .read(&cx, &mut observed, 0)
            .expect("database bytes");
        page[24..32].copy_from_slice(&patch[..8]);
        page[92..96].copy_from_slice(&patch[8..]);
        if let Some(offset) = corrupt_offset {
            assert_eq!(observed[offset], page[offset] ^ 0x80);
            assert!(matches!(result, Err(FrankenError::DatabaseCorrupt { .. })));
            assert_eq!(wal.frame_count(), 1);
            assert_eq!(
                wal.header().to_bytes().expect("retained generation"),
                old_header
            );
            page[offset] ^= 0x80;
        } else {
            assert!(result.expect("exact intended page-1 patch").wal_was_reset);
            assert_eq!(wal.frame_count(), 0);
        }
        assert_eq!(observed, page, "every unmodified byte must also match");
        if corrupt_offset.is_some() {
            assert!(target.published_prefixes.is_empty());
            assert_eq!((target.gate_acquired, target.gate_released), (0, 0));
        } else {
            assert_eq!(target.published_prefixes, vec![1]);
            assert_eq!((target.gate_acquired, target.gate_released), (1, 1));
        }
    }

    #[test]
    fn test_all_checkpoint_modes_refuse_backfill_publication_on_mismatch() {
        for mode in [
            CheckpointMode::Passive,
            CheckpointMode::Full,
            CheckpointMode::Restart,
            CheckpointMode::Truncate,
        ] {
            assert_bad_backfill_is_not_published(mode, false);
        }
    }

    #[test]
    fn test_partial_checkpoint_refuses_backfill_publication_on_mismatch() {
        for mode in [
            CheckpointMode::Passive,
            CheckpointMode::Full,
            CheckpointMode::Restart,
            CheckpointMode::Truncate,
        ] {
            assert_bad_backfill_is_not_published(mode, true);
        }
    }

    fn checkpoint_test_wal_bytes(wal: &WalFile<impl VfsFile>, cx: &Cx) -> Vec<u8> {
        let size = usize::try_from(wal.file().file_size(cx).expect("WAL length"))
            .expect("WAL fits test memory");
        let mut bytes = vec![0; size];
        assert_eq!(wal.file().read(cx, &mut bytes, 0).expect("WAL bytes"), size);
        bytes
    }

    fn assert_bad_backfill_is_not_published(mode: CheckpointMode, partial: bool) {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        let page = PageNumber::new(2).expect("valid page");
        let page_data = sample_page(2);
        wal.append_frame(&cx, 2, &page_data, 2)
            .expect("first commit");
        wal.append_frame(&cx, 3, &sample_page(3), 3)
            .expect("later commit");
        let original = checkpoint_test_wal_bytes(&wal, &cx);
        let state = CheckpointState {
            total_frames: 2,
            backfilled_frames: 0,
            oldest_reader_frame: partial.then_some(1),
        };
        let mut target = ReadbackTarget::new(&vfs, &cx);
        target.corrupt_on_sync = Some((page, 100));
        assert!(matches!(
            execute_checkpoint(&cx, &mut wal, mode, state, &mut target)
                .expect_err("known-bad database prefix must not become reader-visible"),
            FrankenError::DatabaseCorrupt { .. }
        ));
        assert!(
            target.published_prefixes.is_empty(),
            "no publication hook call"
        );
        assert_eq!((target.gate_acquired, target.gate_released), (0, 0));
        assert_eq!(target.read_pages, vec![page]);
        assert_eq!(checkpoint_test_wal_bytes(&wal, &cx), original);
        assert_eq!(wal.frame_count(), 2);
        let mut observed = vec![0; page_data.len()];
        target
            .file
            .read(&cx, &mut observed, u64::from(PAGE_SIZE))
            .expect("actual database mismatch");
        assert_eq!(observed[100], page_data[100] ^ 0x80);
        // The same untouched WAL and same target can redo the failed backfill.
        // The injected corruption was consumed; actual page writes repair it.
        let result = execute_checkpoint(&cx, &mut wal, mode, state, &mut target)
            .expect("retry copies the authoritative WAL before publication");
        let expected_prefix = if partial { 1 } else { 2 };
        assert_eq!(result.frames_backfilled, expected_prefix);
        assert_eq!(target.published_prefixes, vec![expected_prefix]);
        target
            .file
            .read(&cx, &mut observed, u64::from(PAGE_SIZE))
            .expect("repaired database bytes");
        assert_eq!(observed, page_data);
        if partial {
            assert!(!result.wal_was_reset);
            assert_eq!(checkpoint_test_wal_bytes(&wal, &cx), original);
        }
    }

    #[test]
    fn test_reset_gate_refusal_publishes_verified_backfill_once() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        let page = PageNumber::new(2).expect("valid page");
        wal.append_frame(&cx, 2, &sample_page(2), 2)
            .expect("commit page");
        let original = checkpoint_test_wal_bytes(&wal, &cx);
        let mut target = ReadbackTarget::new(&vfs, &cx);
        target.allow_reset = false;
        let state = CheckpointState {
            total_frames: 1,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let result =
            execute_checkpoint(&cx, &mut wal, CheckpointMode::Truncate, state, &mut target)
                .expect("verified backfill survives a refused reset gate");
        assert!(result.reset_deferred_by_readers());
        assert_eq!(target.read_pages, vec![page]);
        assert_eq!(
            target.sync_count, 2,
            "no final reset sync after refused gate"
        );
        assert_eq!(target.published_prefixes, vec![1]);
        assert_eq!((target.gate_acquired, target.gate_released), (0, 0));
        assert_eq!(checkpoint_test_wal_bytes(&wal, &cx), original);
    }

    #[test]
    fn test_reset_rechecks_pages_after_later_sync_corruption() {
        assert_later_reset_fence_preserves_wal(false);
    }

    #[test]
    fn test_reset_preserves_wal_on_later_sync_failure() {
        assert_later_reset_fence_preserves_wal(true);
    }

    fn assert_later_reset_fence_preserves_wal(fail_sync: bool) {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        let page = PageNumber::new(2).expect("valid page");
        wal.append_frame(&cx, 2, &sample_page(2), 2)
            .expect("commit page");
        let original = checkpoint_test_wal_bytes(&wal, &cx);
        let mut target = ReadbackTarget::new(&vfs, &cx);
        if fail_sync {
            target.fail_sync_call = Some(3);
        } else {
            target.corrupt_on_sync = Some((page, 100));
            target.corrupt_sync_call = 3;
        }
        let state = CheckpointState {
            total_frames: 1,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let error = execute_checkpoint(&cx, &mut wal, CheckpointMode::Truncate, state, &mut target)
            .expect_err("later reset durability step must still protect the WAL");
        if fail_sync {
            assert!(error.to_string().contains("injected database sync failure"));
            assert_eq!(target.read_pages, vec![page]);
        } else {
            assert!(matches!(error, FrankenError::DatabaseCorrupt { .. }));
            assert_eq!(target.read_pages, vec![page, page]);
        }
        assert!(
            target.published_prefixes.is_empty(),
            "reset-mode backfill publication waits for the final database check"
        );
        assert_eq!((target.gate_acquired, target.gate_released), (1, 1));
        assert_eq!(checkpoint_test_wal_bytes(&wal, &cx), original);
        assert_eq!(wal.frame_count(), 1);
    }

    #[test]
    fn test_partial_checkpoint_keeps_pages_above_intermediate_commit_size() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        wal.append_frame(&cx, 4, &sample_page(4), 4)
            .expect("older large commit");
        wal.append_frame(&cx, 1, &sample_page(1), 2)
            .expect("intermediate shrink");
        wal.append_frame(&cx, 2, &sample_page(2), 2)
            .expect("final commit");

        let state = CheckpointState {
            total_frames: 3,
            backfilled_frames: 0,
            oldest_reader_frame: Some(2),
        };
        let mut target = ReadbackTarget::new(&vfs, &cx);
        let result = execute_checkpoint(&cx, &mut wal, CheckpointMode::Restart, state, &mut target)
            .expect("partial checkpoint");
        assert_eq!(result.frames_backfilled, 2);
        assert_eq!(result.db_size_pages, Some(2));
        assert!(!result.plan.completes_checkpoint());
        assert!(!result.wal_was_reset);
        assert_eq!(target.truncate_to, None);
        assert_eq!(
            target.file.file_size(&cx).expect("untruncated database"),
            4 * u64::from(PAGE_SIZE)
        );
        assert_eq!(
            target.read_pages,
            vec![PageNumber::ONE, PageNumber::new(4).expect("valid page")],
            "partial backfill must verify its complete written extent"
        );
        assert_eq!(target.published_prefixes, vec![2]);
        assert_eq!(wal.frame_count(), 3);
        assert_eq!(wal.header().salts, test_salts());
        assert_eq!((target.gate_acquired, target.gate_released), (0, 0));
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
        let mut target = RecordingTarget::new();
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
        let mut target = RecordingTarget::new();
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

    // ── GH #193: RESTART must traverse the same full-durability fence as
    //    TRUNCATE before invalidating the prior WAL generation ──

    /// Target whose `sync_db` fails once a configured call count is reached.
    struct FailingSyncTarget {
        inner: RecordingTarget,
        fail_from_call: u32,
    }

    impl CheckpointTarget for FailingSyncTarget {
        fn write_page<'a>(
            &'a mut self,
            cx: &'a Cx,
            page_no: PageNumber,
            data: &'a [u8],
        ) -> CheckpointTargetFuture<'a, ()> {
            Box::pin(async move { self.inner.write_page(cx, page_no, data).await })
        }

        fn truncate_db<'a>(
            &'a mut self,
            cx: &'a Cx,
            n_pages: u32,
        ) -> CheckpointTargetFuture<'a, ()> {
            Box::pin(async move { self.inner.truncate_db(cx, n_pages).await })
        }

        fn sync_db<'a>(&'a mut self, cx: &'a Cx) -> CheckpointTargetFuture<'a, ()> {
            Box::pin(async move {
                self.inner.sync_db(cx).await?;
                if self.inner.sync_count >= self.fail_from_call {
                    return Err(FrankenError::Io(std::io::Error::other(
                        "injected sync_db failure at durability fence",
                    )));
                }
                Ok(())
            })
        }
    }

    fn run_reset_mode_with_failing_fence(mode: CheckpointMode) {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        populate_wal(&mut wal, &cx, 3);
        let generation_before = wal.header().checkpoint_seq;

        let state = CheckpointState {
            total_frames: 3,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        // Backfill issues sync 1 (post-writes) and sync 2 (post-truncate);
        // the explicit pre-reset durability fence is sync 3 — fail there,
        // exactly the schedule from GH #193.
        let mut target = FailingSyncTarget {
            inner: RecordingTarget::new(),
            fail_from_call: 3,
        };

        let err = execute_checkpoint(&cx, &mut wal, mode, state, &mut target)
            .expect_err("failed durability fence must refuse WAL generation reset");
        assert!(
            err.to_string().contains("injected sync_db failure"),
            "error must be the injected fence failure: {err}"
        );
        assert_eq!(
            wal.frame_count(),
            3,
            "WAL frames must survive a failed pre-reset fence"
        );
        assert_eq!(
            wal.header().checkpoint_seq,
            generation_before,
            "checkpoint generation must not advance past a failed fence"
        );
    }

    #[test]
    fn test_restart_refuses_reset_when_durability_fence_fails() {
        run_reset_mode_with_failing_fence(CheckpointMode::Restart);
    }

    #[test]
    fn test_truncate_refuses_reset_when_durability_fence_fails() {
        run_reset_mode_with_failing_fence(CheckpointMode::Truncate);
    }
}
