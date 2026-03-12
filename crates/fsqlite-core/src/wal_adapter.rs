//! Adapters bridging the WAL and pager crates at runtime.
//!
//! These adapters break the circular dependency between `fsqlite-pager` and
//! `fsqlite-wal`:
//!
//! - [`WalBackendAdapter`] wraps `WalFile` to satisfy the pager's
//!   [`WalBackend`] trait (pager -> WAL direction).
//! - [`CheckpointTargetAdapterRef`] wraps `CheckpointPageWriter` to satisfy the
//!   WAL executor's [`CheckpointTarget`] trait (WAL -> pager direction).

use std::collections::HashMap;
use std::sync::Arc;

use fsqlite_error::{FrankenError, Result};
use fsqlite_pager::traits::{
    PreparedWalChecksumTransform, PreparedWalFrameBatch, PreparedWalFrameMeta, WalFrameRef,
};
use fsqlite_pager::{CheckpointMode, CheckpointPageWriter, CheckpointResult, WalBackend};
use fsqlite_types::PageNumber;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::SyncFlags;
use fsqlite_vfs::VfsFile;
use fsqlite_wal::checksum::{WAL_FRAME_HEADER_SIZE, WalChecksumTransform};
use fsqlite_wal::wal::WalAppendFrameRef;
use fsqlite_wal::{
    CheckpointMode as WalCheckpointMode, CheckpointState, CheckpointTarget, WalFile,
    WalGenerationIdentity, execute_checkpoint,
};
use tracing::debug;
#[cfg(not(target_arch = "wasm32"))]
use tracing::warn;

#[cfg(not(target_arch = "wasm32"))]
use crate::wal_fec_adapter::{FecCommitHook, FecCommitResult};

// ---------------------------------------------------------------------------
// WalBackendAdapter: WalFile -> WalBackend
// ---------------------------------------------------------------------------

/// Adapter wrapping [`WalFile`] to implement the pager's [`WalBackend`] trait.
///
/// The pager calls `dyn WalBackend` during WAL-mode commits and page reads.
/// This adapter delegates those calls to the concrete `WalFile<F>` from
/// `fsqlite-wal`.
/// Default steady-state page-index cap.
///
/// Normal runtime operation keeps the published WAL page index authoritative
/// for the full visible generation. Tests can still lower this cap explicitly
/// to exercise the bounded fallback path.
const PAGE_INDEX_MAX_ENTRIES: usize = usize::MAX;

/// How a visible page lookup was resolved for the current WAL generation.
///
/// The steady-state contract is that `Authoritative*` outcomes come from a
/// complete per-generation index. `PartialIndexFallback*` outcomes are an
/// explicit slow-path exception used only when a lowered cap makes the
/// in-memory index incomplete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalPageLookupResolution {
    AuthoritativeHit { frame_index: usize },
    AuthoritativeMiss,
    PartialIndexFallbackHit { frame_index: usize },
    PartialIndexFallbackMiss,
}

impl WalPageLookupResolution {
    #[must_use]
    const fn frame_index(self) -> Option<usize> {
        match self {
            Self::AuthoritativeHit { frame_index }
            | Self::PartialIndexFallbackHit { frame_index } => Some(frame_index),
            Self::AuthoritativeMiss | Self::PartialIndexFallbackMiss => None,
        }
    }

    #[must_use]
    const fn lookup_mode(self) -> &'static str {
        match self {
            Self::AuthoritativeHit { .. } | Self::AuthoritativeMiss => "authoritative_index",
            Self::PartialIndexFallbackHit { .. } | Self::PartialIndexFallbackMiss => {
                "partial_index_fallback"
            }
        }
    }

    #[must_use]
    const fn fallback_reason(self) -> &'static str {
        match self {
            Self::AuthoritativeHit { .. } | Self::AuthoritativeMiss => "none",
            Self::PartialIndexFallbackHit { .. } | Self::PartialIndexFallbackMiss => {
                "partial_index_cap"
            }
        }
    }
}

/// Immutable visibility snapshot published for one WAL generation.
///
/// Readers pin one of these snapshots at transaction start so page lookups stay
/// bound to a stable committed horizon even if later commits advance the active
/// publication plane.
#[derive(Debug, Clone)]
struct WalPublishedSnapshot {
    publication_seq: u64,
    generation: WalGenerationIdentity,
    last_commit_frame: Option<usize>,
    page_index: Arc<HashMap<u32, usize>>,
    index_is_partial: bool,
}

impl WalPublishedSnapshot {
    #[must_use]
    fn empty(publication_seq: u64, generation: WalGenerationIdentity) -> Self {
        Self {
            publication_seq,
            generation,
            last_commit_frame: None,
            page_index: Arc::new(HashMap::new()),
            index_is_partial: false,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct PendingPublicationFrame {
    page_number: u32,
    frame_index: usize,
}

pub struct WalBackendAdapter<F: VfsFile> {
    wal: WalFile<F>,
    /// Guard so commit-time append refresh runs only once per commit batch.
    refresh_before_append: bool,
    /// Active commit-published visibility plane for the current WAL generation.
    published_snapshot: WalPublishedSnapshot,
    /// Monotonic publication sequence assigned to the next published snapshot.
    next_publication_seq: u64,
    /// Transaction-bounded read snapshot pinned at `begin_transaction()`.
    read_snapshot: Option<WalPublishedSnapshot>,
    /// Frames appended after the last published commit horizon.
    pending_publication_frames: Vec<PendingPublicationFrame>,
    /// Optional FEC commit hook for encoding repair symbols on commit.
    #[cfg(not(target_arch = "wasm32"))]
    fec_hook: Option<FecCommitHook>,
    /// Accumulated FEC commit results (for later sidecar persistence).
    #[cfg(not(target_arch = "wasm32"))]
    fec_pending: Vec<FecCommitResult>,
    /// Maximum number of unique pages the index will track. Defaults to a
    /// full authoritative index in steady state. Tests can lower the cap to
    /// exercise the partial-index fallback path explicitly.
    page_index_cap: usize,
}

impl<F: VfsFile> WalBackendAdapter<F> {
    /// Wrap an existing [`WalFile`] in the adapter (FEC disabled).
    #[must_use]
    pub fn new(wal: WalFile<F>) -> Self {
        let generation = wal.generation_identity();
        Self {
            wal,
            refresh_before_append: true,
            published_snapshot: WalPublishedSnapshot::empty(0, generation),
            next_publication_seq: 1,
            read_snapshot: None,
            pending_publication_frames: Vec::new(),
            #[cfg(not(target_arch = "wasm32"))]
            fec_hook: None,
            #[cfg(not(target_arch = "wasm32"))]
            fec_pending: Vec::new(),
            page_index_cap: PAGE_INDEX_MAX_ENTRIES,
        }
    }

    /// Wrap an existing [`WalFile`] with an FEC commit hook.
    #[must_use]
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_fec_hook(wal: WalFile<F>, hook: FecCommitHook) -> Self {
        let generation = wal.generation_identity();
        Self {
            wal,
            refresh_before_append: true,
            published_snapshot: WalPublishedSnapshot::empty(0, generation),
            next_publication_seq: 1,
            read_snapshot: None,
            pending_publication_frames: Vec::new(),
            fec_hook: Some(hook),
            fec_pending: Vec::new(),
            page_index_cap: PAGE_INDEX_MAX_ENTRIES,
        }
    }

    /// Consume the adapter and return the inner [`WalFile`].
    #[must_use]
    pub fn into_inner(self) -> WalFile<F> {
        self.wal
    }

    /// Borrow the inner [`WalFile`].
    #[must_use]
    pub fn inner(&self) -> &WalFile<F> {
        &self.wal
    }

    /// Mutably borrow the inner [`WalFile`].
    ///
    /// Invalidates the publication plane since the caller may mutate WAL state.
    pub fn inner_mut(&mut self) -> &mut WalFile<F> {
        self.invalidate_publication();
        &mut self.wal
    }

    /// Discard published and pinned snapshots after external WAL mutation.
    fn invalidate_publication(&mut self) {
        self.read_snapshot = None;
        self.pending_publication_frames.clear();
        self.published_snapshot = WalPublishedSnapshot::empty(
            self.published_snapshot.publication_seq,
            self.published_snapshot.generation,
        );
    }

    /// Publish an immutable visibility snapshot for the current committed WAL prefix.
    ///
    /// The commit path advances this plane directly, and readers pin a clone of
    /// the published snapshot instead of mutating shared lookup state under an
    /// active transaction.
    fn publish_visible_snapshot(
        &mut self,
        cx: &Cx,
        last_commit_frame: Option<usize>,
        scenario_id: &'static str,
    ) -> Result<()> {
        let generation = self.wal.generation_identity();
        if self.published_snapshot.generation == generation
            && self.published_snapshot.last_commit_frame == last_commit_frame
        {
            return Ok(());
        }

        let previous = self.published_snapshot.clone();
        let mut page_index = if previous.generation == generation {
            previous.page_index.as_ref().clone()
        } else {
            HashMap::new()
        };
        let mut index_is_partial = if previous.generation == generation {
            previous.index_is_partial
        } else {
            false
        };

        let frame_delta_count = match (previous.last_commit_frame, last_commit_frame) {
            (Some(prev), Some(curr)) if curr >= prev => curr.saturating_sub(prev),
            (Some(_) | None, Some(curr)) => curr.saturating_add(1),
            (Some(prev), None) => prev.saturating_add(1),
            (None, None) => 0,
        };

        match last_commit_frame {
            None => {
                page_index.clear();
                index_is_partial = false;
            }
            Some(current_last_commit) => {
                let start = match (
                    previous.generation == generation,
                    previous.last_commit_frame,
                ) {
                    (true, Some(previous_last_commit))
                        if previous_last_commit < current_last_commit =>
                    {
                        previous_last_commit.saturating_add(1)
                    }
                    (true, Some(previous_last_commit))
                        if previous_last_commit == current_last_commit =>
                    {
                        current_last_commit.saturating_add(1)
                    }
                    _ => {
                        page_index.clear();
                        index_is_partial = false;
                        0
                    }
                };
                if start <= current_last_commit {
                    self.build_index_range(
                        cx,
                        &mut page_index,
                        &mut index_is_partial,
                        start,
                        current_last_commit,
                    )?;
                }
            }
        }

        let publication_seq = self.next_publication_seq;
        self.next_publication_seq = self.next_publication_seq.saturating_add(1);
        let latest_frame_entries = page_index.len();
        self.published_snapshot = WalPublishedSnapshot {
            publication_seq,
            generation,
            last_commit_frame,
            page_index: Arc::new(page_index),
            index_is_partial,
        };

        tracing::trace!(
            target: "fsqlite.wal_publication",
            trace_id = cx.trace_id(),
            run_id = "wal-publication",
            scenario_id,
            wal_generation = generation.checkpoint_seq,
            wal_salt1 = generation.salts.salt1,
            wal_salt2 = generation.salts.salt2,
            publication_seq,
            frame_delta_count,
            latest_frame_entries,
            snapshot_age = 0_u64,
            lookup_mode = "published_visibility_map",
            fallback_reason = if index_is_partial {
                "partial_index_cap"
            } else {
                "none"
            },
            "published WAL visibility snapshot"
        );

        Ok(())
    }

    /// Resolve the most recent visible frame for `page_number`.
    ///
    /// The normal contract is `Authoritative*`: the published page index fully
    /// covers the visible WAL generation, so a miss means the page is absent.
    /// `PartialIndexFallback*` is a bounded slow-path used only when the capped
    /// index is known to be incomplete.
    fn resolve_visible_frame(
        &mut self,
        cx: &Cx,
        snapshot: &WalPublishedSnapshot,
        page_number: u32,
    ) -> Result<WalPageLookupResolution> {
        match snapshot.page_index.get(&page_number) {
            Some(&frame_index) => Ok(WalPageLookupResolution::AuthoritativeHit { frame_index }),
            None if !snapshot.index_is_partial => Ok(WalPageLookupResolution::AuthoritativeMiss),
            None => match snapshot.last_commit_frame {
                Some(last_commit_frame) => {
                    match self.scan_backwards_for_page(cx, page_number, last_commit_frame)? {
                        Some(frame_index) => {
                            Ok(WalPageLookupResolution::PartialIndexFallbackHit { frame_index })
                        }
                        None => Ok(WalPageLookupResolution::PartialIndexFallbackMiss),
                    }
                }
                None => Ok(WalPageLookupResolution::AuthoritativeMiss),
            },
        }
    }

    /// Scan frame headers from `start..=end` (inclusive) and populate the page index.
    ///
    /// Since we scan forward, later frames naturally overwrite earlier entries
    /// for the same page number, ensuring "newest frame wins" semantics.
    fn build_index_range(
        &mut self,
        cx: &Cx,
        page_index: &mut HashMap<u32, usize>,
        index_is_partial: &mut bool,
        start: usize,
        end: usize,
    ) -> Result<()> {
        for frame_index in start..=end {
            let header = self.wal.read_frame_header(cx, frame_index)?;
            // Only insert if we haven't hit the capacity cap, or if this page
            // is already tracked (update is free).
            if page_index.len() < self.page_index_cap
                || page_index.contains_key(&header.page_number)
            {
                page_index.insert(header.page_number, frame_index);
            } else {
                // A page was dropped because the index is full -- mark it as
                // partial so that `read_page` knows a HashMap miss cannot be
                // trusted and must fall back to a linear scan.
                *index_is_partial = true;
            }
        }
        Ok(())
    }

    /// Backwards linear scan of committed frames to find a page that was not
    /// captured by the capped page index.
    ///
    /// Scans from `last_commit_frame` down to frame 0 and returns the index
    /// of the first (i.e., most recent) frame containing `page_number`, or
    /// `None` if the page is not in the WAL at all.
    fn scan_backwards_for_page(
        &mut self,
        cx: &Cx,
        page_number: u32,
        last_commit_frame: usize,
    ) -> Result<Option<usize>> {
        for frame_index in (0..=last_commit_frame).rev() {
            let header = self.wal.read_frame_header(cx, frame_index)?;
            if header.page_number == page_number {
                return Ok(Some(frame_index));
            }
        }
        Ok(None)
    }

    /// Take any pending FEC commit results for sidecar persistence.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn take_fec_pending(&mut self) -> Vec<FecCommitResult> {
        std::mem::take(&mut self.fec_pending)
    }

    /// Whether FEC encoding is active.
    #[must_use]
    #[cfg(not(target_arch = "wasm32"))]
    pub fn fec_enabled(&self) -> bool {
        self.fec_hook
            .as_ref()
            .is_some_and(FecCommitHook::is_enabled)
    }

    /// Discard buffered FEC pages (e.g. on transaction rollback).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn fec_discard(&mut self) {
        if let Some(hook) = &mut self.fec_hook {
            hook.discard_buffered();
        }
    }

    /// Override the page index capacity (for testing only).
    #[cfg(test)]
    fn set_page_index_cap(&mut self, cap: usize) {
        self.page_index_cap = cap;
        // Invalidate so the next read rebuilds with the new cap.
        self.invalidate_publication();
    }

    fn publish_latest_committed_snapshot(
        &mut self,
        cx: &Cx,
        scenario_id: &'static str,
    ) -> Result<()> {
        let last_commit_frame = self.wal.last_commit_frame(cx)?;
        self.publish_visible_snapshot(cx, last_commit_frame, scenario_id)
    }

    fn synchronize_publication_before_append(
        &mut self,
        cx: &Cx,
        scenario_id: &'static str,
    ) -> Result<()> {
        self.wal.refresh(cx)?;
        self.pending_publication_frames.clear();
        self.publish_latest_committed_snapshot(cx, scenario_id)
    }

    fn record_appended_frames<I>(&mut self, start_frame_index: usize, frames: I) -> Option<usize>
    where
        I: IntoIterator<Item = (u32, u32)>,
    {
        let mut last_commit_frame = None;
        for (offset, (page_number, db_size_if_commit)) in frames.into_iter().enumerate() {
            let frame_index = start_frame_index.saturating_add(offset);
            self.pending_publication_frames
                .push(PendingPublicationFrame {
                    page_number,
                    frame_index,
                });
            if db_size_if_commit != 0 {
                last_commit_frame = Some(frame_index);
            }
        }
        last_commit_frame
    }

    fn publish_pending_commit_snapshot(
        &mut self,
        cx: &Cx,
        last_commit_frame: usize,
        scenario_id: &'static str,
    ) -> Result<()> {
        let generation = self.wal.generation_identity();
        let previous = self.published_snapshot.clone();
        let can_extend_previous = previous.generation == generation
            && previous
                .last_commit_frame
                .is_none_or(|previous_last_commit| previous_last_commit < last_commit_frame);
        let mut page_index = if can_extend_previous {
            previous.page_index.as_ref().clone()
        } else {
            HashMap::new()
        };
        let mut index_is_partial = if can_extend_previous {
            previous.index_is_partial
        } else {
            false
        };
        let previous_last_commit = if can_extend_previous {
            previous.last_commit_frame
        } else {
            None
        };

        let mut frame_delta_count = 0_usize;
        for frame in &self.pending_publication_frames {
            if previous_last_commit
                .is_some_and(|previous_last_commit| frame.frame_index <= previous_last_commit)
                || frame.frame_index > last_commit_frame
            {
                continue;
            }

            frame_delta_count = frame_delta_count.saturating_add(1);
            if page_index.len() < self.page_index_cap || page_index.contains_key(&frame.page_number)
            {
                page_index.insert(frame.page_number, frame.frame_index);
            } else {
                index_is_partial = true;
            }
        }

        if frame_delta_count == 0 {
            self.pending_publication_frames.clear();
            return self.publish_visible_snapshot(cx, Some(last_commit_frame), scenario_id);
        }

        let publication_seq = self.next_publication_seq;
        self.next_publication_seq = self.next_publication_seq.saturating_add(1);
        let latest_frame_entries = page_index.len();
        self.published_snapshot = WalPublishedSnapshot {
            publication_seq,
            generation,
            last_commit_frame: Some(last_commit_frame),
            page_index: Arc::new(page_index),
            index_is_partial,
        };
        self.pending_publication_frames.clear();

        tracing::trace!(
            target: "fsqlite.wal_publication",
            trace_id = cx.trace_id(),
            run_id = "wal-publication",
            scenario_id,
            wal_generation = generation.checkpoint_seq,
            wal_salt1 = generation.salts.salt1,
            wal_salt2 = generation.salts.salt2,
            publication_seq,
            frame_delta_count,
            latest_frame_entries,
            snapshot_age = 0_u64,
            lookup_mode = "published_visibility_map",
            fallback_reason = if index_is_partial {
                "partial_index_cap"
            } else {
                "none"
            },
            "published WAL visibility snapshot from commit path"
        );

        Ok(())
    }
}

/// Convert pager checkpoint mode to WAL checkpoint mode.
fn to_wal_mode(mode: CheckpointMode) -> WalCheckpointMode {
    match mode {
        CheckpointMode::Passive => WalCheckpointMode::Passive,
        CheckpointMode::Full => WalCheckpointMode::Full,
        CheckpointMode::Restart => WalCheckpointMode::Restart,
        CheckpointMode::Truncate => WalCheckpointMode::Truncate,
    }
}

impl<F: VfsFile> WalBackend for WalBackendAdapter<F> {
    fn begin_transaction(&mut self, cx: &Cx) -> Result<()> {
        // Establish a transaction-bounded snapshot once, instead of doing an
        // expensive refresh for every page read.
        self.wal.refresh(cx)?;
        self.publish_latest_committed_snapshot(cx, "begin_transaction")?;
        self.read_snapshot = Some(self.published_snapshot.clone());
        self.refresh_before_append = true;
        Ok(())
    }

    fn append_frame(
        &mut self,
        cx: &Cx,
        page_number: u32,
        page_data: &[u8],
        db_size_if_commit: u32,
    ) -> Result<()> {
        if self.refresh_before_append {
            // Refresh and synchronize the published base snapshot once before
            // the commit batch starts, then publish local frame deltas directly
            // from the append path.
            self.synchronize_publication_before_append(cx, "append_frame_pre_refresh")?;
        }
        let start_frame_index = self.wal.frame_count();
        self.wal
            .append_frame(cx, page_number, page_data, db_size_if_commit)?;
        self.refresh_before_append = false;
        let last_commit_frame =
            self.record_appended_frames(start_frame_index, [(page_number, db_size_if_commit)]);

        // Feed the frame to the FEC hook.  On commit, it encodes repair
        // symbols and stores them for later sidecar persistence.
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(hook) = &mut self.fec_hook {
            match hook.on_frame(cx, page_number, page_data, db_size_if_commit) {
                Ok(Some(result)) => {
                    debug!(
                        pages = result.page_numbers.len(),
                        k_source = result.k_source,
                        symbols = result.symbols.len(),
                        "FEC commit group encoded"
                    );
                    self.fec_pending.push(result);
                }
                Ok(None) => {}
                Err(e) => {
                    // FEC encoding failure is non-fatal -- log and continue.
                    warn!(error = %e, "FEC encoding failed; commit proceeds without repair symbols");
                }
            }
        }

        if let Some(last_commit_frame) = last_commit_frame {
            self.publish_pending_commit_snapshot(cx, last_commit_frame, "append_frame_commit")?;
        }

        Ok(())
    }

    fn append_frames(&mut self, cx: &Cx, frames: &[WalFrameRef<'_>]) -> Result<()> {
        if frames.is_empty() {
            return Ok(());
        }

        if self.refresh_before_append {
            self.synchronize_publication_before_append(cx, "append_frames_pre_refresh")?;
        }

        let start_frame_index = self.wal.frame_count();
        let mut wal_frames = Vec::with_capacity(frames.len());
        for frame in frames {
            wal_frames.push(WalAppendFrameRef {
                page_number: frame.page_number,
                page_data: frame.page_data,
                db_size_if_commit: frame.db_size_if_commit,
            });
        }
        self.wal.append_frames(cx, &wal_frames)?;
        self.refresh_before_append = false;
        let last_commit_frame = self.record_appended_frames(
            start_frame_index,
            frames
                .iter()
                .map(|frame| (frame.page_number, frame.db_size_if_commit)),
        );

        #[cfg(not(target_arch = "wasm32"))]
        if let Some(hook) = &mut self.fec_hook {
            for frame in frames {
                match hook.on_frame(
                    cx,
                    frame.page_number,
                    frame.page_data,
                    frame.db_size_if_commit,
                ) {
                    Ok(Some(result)) => {
                        debug!(
                            pages = result.page_numbers.len(),
                            k_source = result.k_source,
                            symbols = result.symbols.len(),
                            "FEC commit group encoded"
                        );
                        self.fec_pending.push(result);
                    }
                    Ok(None) => {}
                    Err(e) => {
                        warn!(
                            error = %e,
                            "FEC encoding failed; commit proceeds without repair symbols"
                        );
                    }
                }
            }
        }

        if let Some(last_commit_frame) = last_commit_frame {
            self.publish_pending_commit_snapshot(cx, last_commit_frame, "append_frames_commit")?;
        }

        Ok(())
    }

    fn prepare_append_frames(
        &mut self,
        frames: &[WalFrameRef<'_>],
    ) -> Result<Option<PreparedWalFrameBatch>> {
        if frames.is_empty() {
            return Ok(None);
        }

        let wal_frames: Vec<_> = frames
            .iter()
            .map(|frame| WalAppendFrameRef {
                page_number: frame.page_number,
                page_data: frame.page_data,
                db_size_if_commit: frame.db_size_if_commit,
            })
            .collect();
        let frame_bytes = self.wal.prepare_frame_bytes(&wal_frames)?;
        let checksum_transforms = frame_bytes
            .chunks_exact(self.wal.frame_size())
            .map(|frame| {
                WalChecksumTransform::for_wal_frame(
                    frame,
                    self.wal.page_size(),
                    self.wal.big_endian_checksum(),
                )
            })
            .map(|result| {
                result.map(|transform| PreparedWalChecksumTransform {
                    a11: transform.a11,
                    a12: transform.a12,
                    a21: transform.a21,
                    a22: transform.a22,
                    c1: transform.c1,
                    c2: transform.c2,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let frame_metas = frames
            .iter()
            .map(|frame| PreparedWalFrameMeta {
                page_number: frame.page_number,
                db_size_if_commit: frame.db_size_if_commit,
            })
            .collect();

        Ok(Some(PreparedWalFrameBatch {
            frame_size: self.wal.frame_size(),
            page_data_offset: WAL_FRAME_HEADER_SIZE,
            frame_metas,
            checksum_transforms,
            frame_bytes,
        }))
    }

    fn append_prepared_frames(
        &mut self,
        cx: &Cx,
        prepared: &mut PreparedWalFrameBatch,
    ) -> Result<()> {
        if prepared.frame_count() == 0 {
            return Ok(());
        }

        if self.refresh_before_append {
            self.synchronize_publication_before_append(cx, "append_prepared_pre_refresh")?;
        }

        let start_frame_index = self.wal.frame_count();
        let checksum_transforms = prepared
            .checksum_transforms
            .iter()
            .map(|transform| WalChecksumTransform {
                a11: transform.a11,
                a12: transform.a12,
                a21: transform.a21,
                a22: transform.a22,
                c1: transform.c1,
                c2: transform.c2,
            })
            .collect::<Vec<_>>();
        self.wal.append_prepared_frame_bytes(
            cx,
            &mut prepared.frame_bytes,
            &checksum_transforms,
        )?;
        self.refresh_before_append = false;
        let last_commit_frame = self.record_appended_frames(
            start_frame_index,
            prepared
                .frame_metas
                .iter()
                .map(|frame| (frame.page_number, frame.db_size_if_commit)),
        );

        #[cfg(not(target_arch = "wasm32"))]
        if let Some(hook) = &mut self.fec_hook {
            for (index, frame) in prepared.frame_metas.iter().enumerate() {
                match hook.on_frame(
                    cx,
                    frame.page_number,
                    prepared.page_data(index),
                    frame.db_size_if_commit,
                ) {
                    Ok(Some(result)) => {
                        debug!(
                            pages = result.page_numbers.len(),
                            k_source = result.k_source,
                            symbols = result.symbols.len(),
                            "FEC commit group encoded"
                        );
                        self.fec_pending.push(result);
                    }
                    Ok(None) => {}
                    Err(e) => {
                        warn!(
                            error = %e,
                            "FEC encoding failed; commit proceeds without repair symbols"
                        );
                    }
                }
            }
        }

        if let Some(last_commit_frame) = last_commit_frame {
            self.publish_pending_commit_snapshot(
                cx,
                last_commit_frame,
                "append_prepared_frames_commit",
            )?;
        }

        Ok(())
    }

    fn read_page(&mut self, cx: &Cx, page_number: u32) -> Result<Option<Vec<u8>>> {
        let snapshot = if let Some(snapshot) = self.read_snapshot.clone() {
            snapshot
        } else {
            self.publish_latest_committed_snapshot(cx, "read_page_unpinned")?;
            self.published_snapshot.clone()
        };
        if snapshot.last_commit_frame.is_none() {
            return Ok(None);
        }
        let snapshot_age = self
            .published_snapshot
            .publication_seq
            .saturating_sub(snapshot.publication_seq);

        let resolution = self.resolve_visible_frame(cx, &snapshot, page_number)?;
        let Some(frame_index) = resolution.frame_index() else {
            debug!(
                page_number,
                wal_checkpoint_seq = snapshot.generation.checkpoint_seq,
                wal_salt1 = snapshot.generation.salts.salt1,
                wal_salt2 = snapshot.generation.salts.salt2,
                publication_seq = snapshot.publication_seq,
                snapshot_age,
                lookup_mode = resolution.lookup_mode(),
                fallback_reason = resolution.fallback_reason(),
                "WAL adapter: page absent from current generation"
            );
            return Ok(None);
        };

        // Read the frame data at the resolved position.
        let mut frame_buf = vec![0u8; self.wal.frame_size()];
        let header = self.wal.read_frame_into(cx, frame_index, &mut frame_buf)?;

        // Runtime integrity check: verify the frame actually contains our page.
        // This guards against index corruption or stale entries.
        if header.page_number != page_number {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "WAL page index integrity failure: expected page {page_number} \
                     at frame {frame_index}, found page {}",
                    header.page_number
                ),
            });
        }

        let data = frame_buf[fsqlite_wal::checksum::WAL_FRAME_HEADER_SIZE..].to_vec();
        debug!(
            page_number,
            frame_index,
            wal_checkpoint_seq = snapshot.generation.checkpoint_seq,
            wal_salt1 = snapshot.generation.salts.salt1,
            wal_salt2 = snapshot.generation.salts.salt2,
            publication_seq = snapshot.publication_seq,
            snapshot_age,
            lookup_mode = resolution.lookup_mode(),
            fallback_reason = resolution.fallback_reason(),
            "WAL adapter: resolved page from current WAL generation"
        );
        Ok(Some(data))
    }

    fn committed_txns_since_page(&mut self, cx: &Cx, page_number: u32) -> Result<u64> {
        let snapshot = if let Some(snapshot) = self.read_snapshot.clone() {
            snapshot
        } else {
            self.publish_latest_committed_snapshot(cx, "committed_txns_since_page")?;
            self.published_snapshot.clone()
        };
        let Some(last_commit_frame) = snapshot.last_commit_frame else {
            return Ok(0);
        };

        let resolution = self.resolve_visible_frame(cx, &snapshot, page_number)?;
        let Some(last_page_frame) = resolution.frame_index() else {
            let mut total_commits = 0_u64;
            for frame_index in 0..=last_commit_frame {
                if self.wal.read_frame_header(cx, frame_index)?.is_commit() {
                    total_commits = total_commits.saturating_add(1);
                }
            }
            return Ok(total_commits);
        };

        let mut page_commit_frame = None;
        for frame_index in last_page_frame..=last_commit_frame {
            if self.wal.read_frame_header(cx, frame_index)?.is_commit() {
                page_commit_frame = Some(frame_index);
                break;
            }
        }

        let Some(page_commit_frame) = page_commit_frame else {
            return Ok(0);
        };

        let mut committed_txns_after_page = 0_u64;
        for frame_index in page_commit_frame.saturating_add(1)..=last_commit_frame {
            if self.wal.read_frame_header(cx, frame_index)?.is_commit() {
                committed_txns_after_page = committed_txns_after_page.saturating_add(1);
            }
        }

        Ok(committed_txns_after_page)
    }

    fn sync(&mut self, cx: &Cx) -> Result<()> {
        let result = self.wal.sync(cx, SyncFlags::NORMAL);
        self.refresh_before_append = true;
        result
    }

    fn frame_count(&self) -> usize {
        self.wal.frame_count()
    }

    fn checkpoint(
        &mut self,
        cx: &Cx,
        mode: CheckpointMode,
        writer: &mut dyn CheckpointPageWriter,
        backfilled_frames: u32,
        oldest_reader_frame: Option<u32>,
    ) -> Result<CheckpointResult> {
        // Refresh so planner state reflects the latest on-disk WAL shape.
        self.wal.refresh(cx)?;
        self.refresh_before_append = true;
        let total_frames = u32::try_from(self.wal.frame_count()).unwrap_or(u32::MAX);

        // Build checkpoint state for the planner.
        let state = CheckpointState {
            total_frames,
            backfilled_frames,
            oldest_reader_frame,
        };

        // Wrap the CheckpointPageWriter in a CheckpointTargetAdapter.
        let mut target = CheckpointTargetAdapterRef { writer };

        // Execute the checkpoint.
        let result = execute_checkpoint(cx, &mut self.wal, to_wal_mode(mode), state, &mut target)?;

        // Checkpoint-aware FEC lifecycle: once frames are backfilled to the
        // database file, their FEC symbols are no longer needed.  Clear
        // pending FEC results for the checkpointed range.
        #[cfg(not(target_arch = "wasm32"))]
        if result.frames_backfilled > 0 {
            let drained = self.fec_pending.len();
            self.fec_pending.clear();
            if drained > 0 {
                debug!(
                    drained_groups = drained,
                    frames_backfilled = result.frames_backfilled,
                    "FEC symbols reclaimed after checkpoint"
                );
            }
        }

        // If the WAL was fully reset, also discard any buffered FEC pages
        // and invalidate the page index (salts changed).
        #[cfg(not(target_arch = "wasm32"))]
        if result.wal_was_reset {
            self.fec_discard();
        }
        if result.wal_was_reset {
            self.invalidate_publication();
        }

        self.publish_latest_committed_snapshot(cx, "checkpoint")?;

        Ok(CheckpointResult {
            total_frames,
            frames_backfilled: result.frames_backfilled,
            completed: result.plan.completes_checkpoint(),
            wal_was_reset: result.wal_was_reset,
        })
    }
}

/// Adapter wrapping a `&mut dyn CheckpointPageWriter` to implement `CheckpointTarget`.
///
/// This is used internally by `WalBackendAdapter::checkpoint` to bridge the
/// pager's writer to the WAL executor's target trait.
struct CheckpointTargetAdapterRef<'a> {
    writer: &'a mut dyn CheckpointPageWriter,
}

impl CheckpointTarget for CheckpointTargetAdapterRef<'_> {
    fn write_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
        self.writer.write_page(cx, page_no, data)
    }

    fn truncate_db(&mut self, cx: &Cx, n_pages: u32) -> Result<()> {
        self.writer.truncate(cx, n_pages)
    }

    fn sync_db(&mut self, cx: &Cx) -> Result<()> {
        self.writer.sync(cx)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;

    use fsqlite_pager::MockCheckpointPageWriter;
    use fsqlite_pager::traits::WalFrameRef;
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_vfs::MemoryVfs;
    use fsqlite_vfs::traits::Vfs;
    use fsqlite_wal::checksum::WalSalts;

    use super::*;

    const PAGE_SIZE: u32 = 4096;

    fn init_wal_publication_test_tracing() {
        static TRACING_INIT: OnceLock<()> = OnceLock::new();
        TRACING_INIT.get_or_init(|| {
            if tracing_subscriber::fmt()
                .with_ansi(false)
                .with_max_level(tracing::Level::TRACE)
                .with_test_writer()
                .try_init()
                .is_err()
            {
                // Another test already installed a global subscriber.
            }
        });
    }

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

    fn make_adapter(vfs: &MemoryVfs, cx: &Cx) -> WalBackendAdapter<<MemoryVfs as Vfs>::File> {
        let file = open_wal_file(vfs, cx);
        let wal = WalFile::create(cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");
        WalBackendAdapter::new(wal)
    }

    // -- WalBackendAdapter tests --

    #[test]
    fn test_adapter_append_and_frame_count() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        assert_eq!(adapter.frame_count(), 0);

        let page = sample_page(0x42);
        adapter
            .append_frame(&cx, 1, &page, 0)
            .expect("append frame");
        assert_eq!(adapter.frame_count(), 1);

        adapter
            .append_frame(&cx, 2, &sample_page(0x43), 2)
            .expect("append commit frame");
        assert_eq!(adapter.frame_count(), 2);
    }

    #[test]
    fn test_adapter_read_page_found() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let page1 = sample_page(0x10);
        let page2 = sample_page(0x20);
        adapter.append_frame(&cx, 1, &page1, 0).expect("append");
        adapter
            .append_frame(&cx, 2, &page2, 2)
            .expect("append commit");

        let result = adapter.read_page(&cx, 1).expect("read page 1");
        assert_eq!(result, Some(page1));

        let result = adapter.read_page(&cx, 2).expect("read page 2");
        assert_eq!(result, Some(page2));
    }

    #[test]
    fn test_adapter_read_page_not_found() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        adapter
            .append_frame(&cx, 1, &sample_page(0x10), 1)
            .expect("append");

        let result = adapter.read_page(&cx, 99).expect("read missing page");
        assert_eq!(result, None);
    }

    #[test]
    fn test_adapter_read_page_returns_latest_version() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let old_data = sample_page(0xAA);
        let new_data = sample_page(0xBB);

        // Write page 5 twice -- the adapter should return the latest.
        adapter
            .append_frame(&cx, 5, &old_data, 0)
            .expect("append old");
        adapter
            .append_frame(&cx, 5, &new_data, 1)
            .expect("append new (commit)");

        let result = adapter.read_page(&cx, 5).expect("read page 5");
        assert_eq!(
            result,
            Some(new_data),
            "adapter should return the latest WAL version"
        );
    }

    #[test]
    fn test_adapter_refreshes_cross_handle_visibility_and_append_position() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();

        let file1 = open_wal_file(&vfs, &cx);
        let wal1 = WalFile::create(&cx, file1, PAGE_SIZE, 0, test_salts()).expect("create WAL");
        let mut adapter1 = WalBackendAdapter::new(wal1);

        let file2 = open_wal_file(&vfs, &cx);
        let wal2 = WalFile::open(&cx, file2).expect("open WAL");
        let mut adapter2 = WalBackendAdapter::new(wal2);

        let page1 = sample_page(0x11);
        adapter1
            .append_frame(&cx, 1, &page1, 1)
            .expect("adapter1 append commit");
        adapter1.sync(&cx).expect("adapter1 sync");
        adapter2
            .begin_transaction(&cx)
            .expect("adapter2 begin transaction");
        assert_eq!(
            adapter2.read_page(&cx, 1).expect("adapter2 read page1"),
            Some(page1.clone()),
            "adapter2 should observe adapter1 commit at transaction begin"
        );

        let page2 = sample_page(0x22);
        adapter2
            .append_frame(&cx, 2, &page2, 2)
            .expect("adapter2 append commit");
        adapter2.sync(&cx).expect("adapter2 sync");
        adapter1
            .begin_transaction(&cx)
            .expect("adapter1 begin transaction");
        assert_eq!(
            adapter1.read_page(&cx, 2).expect("adapter1 read page2"),
            Some(page2.clone()),
            "adapter1 should observe adapter2 commit at transaction begin"
        );

        // Ensure the second writer appended to frame 1 (not frame 0 overwrite).
        assert_eq!(
            adapter1.frame_count(),
            2,
            "shared WAL should contain both commit frames"
        );
        assert_eq!(
            adapter2.frame_count(),
            2,
            "shared WAL should contain both commit frames"
        );
    }

    #[test]
    fn test_adapter_batch_append_checksum_chain_matches_single_append() {
        let cx = test_cx();
        let vfs_single = MemoryVfs::new();
        let vfs_batch = MemoryVfs::new();

        let mut adapter_single = make_adapter(&vfs_single, &cx);
        let mut adapter_batch = make_adapter(&vfs_batch, &cx);

        let pages: Vec<Vec<u8>> = (0..4u8).map(sample_page).collect();
        let commit_sizes = [0_u32, 0, 0, 4];

        for (index, page) in pages.iter().enumerate() {
            adapter_single
                .append_frame(
                    &cx,
                    u32::try_from(index + 1).expect("page number fits u32"),
                    page,
                    commit_sizes[index],
                )
                .expect("single append");
        }

        let batch_frames: Vec<_> = pages
            .iter()
            .enumerate()
            .map(|(index, page)| WalFrameRef {
                page_number: u32::try_from(index + 1).expect("page number fits u32"),
                page_data: page,
                db_size_if_commit: commit_sizes[index],
            })
            .collect();
        adapter_batch
            .append_frames(&cx, &batch_frames)
            .expect("batch append");

        assert_eq!(
            adapter_single.frame_count(),
            adapter_batch.frame_count(),
            "batch adapter append must preserve frame count"
        );
        assert_eq!(
            adapter_single.wal.running_checksum(),
            adapter_batch.wal.running_checksum(),
            "batch adapter append must preserve checksum chain"
        );

        for frame_index in 0..pages.len() {
            let (single_header, single_data) = adapter_single
                .wal
                .read_frame(&cx, frame_index)
                .expect("read single frame");
            let (batch_header, batch_data) = adapter_batch
                .wal
                .read_frame(&cx, frame_index)
                .expect("read batch frame");
            assert_eq!(
                single_header, batch_header,
                "frame header {frame_index} must match"
            );
            assert_eq!(
                single_data, batch_data,
                "frame payload {frame_index} must match"
            );
        }
    }

    #[test]
    fn test_adapter_prepared_batch_append_checksum_chain_matches_single_append() {
        let cx = test_cx();
        let vfs_single = MemoryVfs::new();
        let vfs_prepared = MemoryVfs::new();

        let mut adapter_single = make_adapter(&vfs_single, &cx);
        let mut adapter_prepared = make_adapter(&vfs_prepared, &cx);

        let pages: Vec<Vec<u8>> = (0..4u8).map(sample_page).collect();
        let commit_sizes = [0_u32, 0, 0, 4];

        for (index, page) in pages.iter().enumerate() {
            adapter_single
                .append_frame(
                    &cx,
                    u32::try_from(index + 1).expect("page number fits u32"),
                    page,
                    commit_sizes[index],
                )
                .expect("single append");
        }

        let batch_frames: Vec<_> = pages
            .iter()
            .enumerate()
            .map(|(index, page)| WalFrameRef {
                page_number: u32::try_from(index + 1).expect("page number fits u32"),
                page_data: page,
                db_size_if_commit: commit_sizes[index],
            })
            .collect();
        let mut prepared = adapter_prepared
            .prepare_append_frames(&batch_frames)
            .expect("prepare append")
            .expect("prepared batch");
        adapter_prepared
            .append_prepared_frames(&cx, &mut prepared)
            .expect("append prepared");

        assert_eq!(
            adapter_single.frame_count(),
            adapter_prepared.frame_count(),
            "prepared adapter append must preserve frame count"
        );
        assert_eq!(
            adapter_single.wal.running_checksum(),
            adapter_prepared.wal.running_checksum(),
            "prepared adapter append must preserve checksum chain"
        );

        for frame_index in 0..pages.len() {
            let (single_header, single_data) = adapter_single
                .wal
                .read_frame(&cx, frame_index)
                .expect("read single frame");
            let (prepared_header, prepared_data) = adapter_prepared
                .wal
                .read_frame(&cx, frame_index)
                .expect("read prepared frame");
            assert_eq!(
                single_header, prepared_header,
                "frame header {frame_index} must match"
            );
            assert_eq!(
                single_data, prepared_data,
                "frame payload {frame_index} must match"
            );
        }
    }

    #[test]
    fn test_adapter_pins_read_snapshot_until_next_begin() {
        init_wal_publication_test_tracing();
        let cx = test_cx();
        let vfs = MemoryVfs::new();

        let file_writer = open_wal_file(&vfs, &cx);
        let wal_writer =
            WalFile::create(&cx, file_writer, PAGE_SIZE, 0, test_salts()).expect("create WAL");
        let mut writer = WalBackendAdapter::new(wal_writer);

        let file_reader = open_wal_file(&vfs, &cx);
        let wal_reader = WalFile::open(&cx, file_reader).expect("open WAL");
        let mut reader = WalBackendAdapter::new(wal_reader);

        let v1 = sample_page(0x41);
        writer.append_frame(&cx, 3, &v1, 3).expect("append v1");
        writer.sync(&cx).expect("sync v1");

        reader
            .begin_transaction(&cx)
            .expect("begin reader snapshot 1");
        assert_eq!(
            reader.read_page(&cx, 3).expect("reader sees v1"),
            Some(v1.clone())
        );

        let v2 = sample_page(0x42);
        writer.append_frame(&cx, 3, &v2, 3).expect("append v2");
        writer.sync(&cx).expect("sync v2");

        // Same transaction snapshot must stay stable (no mid-transaction drift).
        assert_eq!(
            reader
                .read_page(&cx, 3)
                .expect("reader remains on pinned snapshot"),
            Some(v1.clone())
        );

        // A new transaction snapshot should pick up the latest commit.
        reader
            .begin_transaction(&cx)
            .expect("begin reader snapshot 2");
        assert_eq!(reader.read_page(&cx, 3).expect("reader sees v2"), Some(v2));
    }

    #[test]
    fn test_adapter_read_page_hides_uncommitted_frames() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let committed = sample_page(0x31);
        let uncommitted = sample_page(0x32);

        adapter
            .append_frame(&cx, 7, &committed, 7)
            .expect("append committed frame");
        adapter
            .append_frame(&cx, 7, &uncommitted, 0)
            .expect("append uncommitted frame");

        let result = adapter.read_page(&cx, 7).expect("read committed page");
        assert_eq!(
            result,
            Some(committed),
            "reader must ignore uncommitted tail frames"
        );
    }

    #[test]
    fn test_adapter_read_page_none_when_wal_has_no_commit_frame() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        adapter
            .append_frame(&cx, 3, &sample_page(0x44), 0)
            .expect("append uncommitted frame");

        let result = adapter.read_page(&cx, 3).expect("read page");
        assert_eq!(result, None, "uncommitted WAL frames must stay invisible");
    }

    #[test]
    fn test_adapter_read_page_empty_wal() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let result = adapter.read_page(&cx, 1).expect("read from empty WAL");
        assert_eq!(result, None);
    }

    #[test]
    fn test_adapter_sync() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        adapter
            .append_frame(&cx, 1, &sample_page(0), 1)
            .expect("append");
        adapter.sync(&cx).expect("sync should not fail");
    }

    #[test]
    fn test_adapter_into_inner_round_trip() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        adapter
            .append_frame(&cx, 1, &sample_page(0), 1)
            .expect("append");

        assert_eq!(adapter.inner().frame_count(), 1);

        let wal = adapter.into_inner();
        assert_eq!(wal.frame_count(), 1);
    }

    #[test]
    fn test_adapter_as_dyn_wal_backend() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        // Verify it can be used as a trait object.
        let backend: &mut dyn WalBackend = &mut adapter;
        backend
            .append_frame(&cx, 1, &sample_page(0x77), 1)
            .expect("append via dyn");
        assert_eq!(backend.frame_count(), 1);

        let page = backend.read_page(&cx, 1).expect("read via dyn");
        assert_eq!(page, Some(sample_page(0x77)));
    }

    // -- Page index O(1) lookup tests --

    #[test]
    fn test_page_index_returns_correct_data() {
        // Write several pages, verify O(1) index returns the right data.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let page1 = sample_page(0x01);
        let page2 = sample_page(0x02);
        let page3 = sample_page(0x03);

        adapter.append_frame(&cx, 1, &page1, 0).expect("append");
        adapter.append_frame(&cx, 2, &page2, 0).expect("append");
        adapter
            .append_frame(&cx, 3, &page3, 3)
            .expect("append commit");

        // All three pages should be readable via the index.
        assert_eq!(adapter.read_page(&cx, 1).expect("read"), Some(page1));
        assert_eq!(adapter.read_page(&cx, 2).expect("read"), Some(page2));
        assert_eq!(adapter.read_page(&cx, 3).expect("read"), Some(page3));

        // Non-existent page returns None.
        assert_eq!(adapter.read_page(&cx, 99).expect("read"), None);
    }

    #[test]
    fn test_page_index_returns_latest_version() {
        // Write the same page twice; the index should point to the newer frame.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let old_data = sample_page(0xAA);
        let new_data = sample_page(0xBB);

        adapter
            .append_frame(&cx, 5, &old_data, 0)
            .expect("append old");
        adapter
            .append_frame(&cx, 5, &new_data, 1)
            .expect("append new (commit)");

        assert_eq!(
            adapter.read_page(&cx, 5).expect("read"),
            Some(new_data),
            "page index must return the latest frame for a page"
        );
    }

    #[test]
    fn test_page_index_invalidated_on_wal_reset() {
        // Simulate a WAL reset with new salts. The index must be rebuilt so
        // stale entries from the old generation are not returned.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let old_data = sample_page(0x11);
        adapter
            .append_frame(&cx, 1, &old_data, 1)
            .expect("append commit");

        // Read page 1 to populate the index.
        assert_eq!(adapter.read_page(&cx, 1).expect("read old"), Some(old_data));

        // Reset WAL with new salts (simulates checkpoint reset).
        let new_salts = WalSalts {
            salt1: 0xAAAA_BBBB,
            salt2: 0xCCCC_DDDD,
        };
        adapter
            .inner_mut()
            .reset(&cx, 1, new_salts, false)
            .expect("WAL reset");

        // Write new data for the same page number in the new generation.
        let new_data = sample_page(0x22);
        adapter
            .append_frame(&cx, 1, &new_data, 1)
            .expect("append new generation commit");

        // The index must have been invalidated; we should get the new data.
        let result = adapter.read_page(&cx, 1).expect("read after reset");
        assert_eq!(
            result,
            Some(new_data),
            "after WAL reset, page index must return new-generation data, not stale cached data"
        );

        // A page that existed only in the old generation should be gone.
        let old_only = sample_page(0x33);
        // (We never wrote page 99 in the new generation.)
        assert_eq!(
            adapter.read_page(&cx, 99).expect("read non-existent"),
            None,
            "pages from old WAL generation must not appear after reset"
        );
        // Suppress unused variable warning.
        drop(old_only);
    }

    #[test]
    fn test_page_index_invalidated_on_same_salt_generation_change() {
        init_wal_publication_test_tracing();
        // Generation identity must include checkpoint_seq. Reusing salts across
        // reset must still invalidate the cached page index and avoid ABA bugs.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let reused_salts = adapter.inner().header().salts;
        let old_data = sample_page(0x11);
        adapter
            .append_frame(&cx, 1, &old_data, 1)
            .expect("append commit");
        assert_eq!(adapter.read_page(&cx, 1).expect("read old"), Some(old_data));

        adapter
            .inner_mut()
            .reset(&cx, 1, reused_salts, false)
            .expect("reset with same salts");
        let new_data = sample_page(0x22);
        adapter
            .append_frame(&cx, 2, &new_data, 2)
            .expect("append new generation commit");

        assert_eq!(
            adapter.read_page(&cx, 1).expect("old page should be gone"),
            None,
            "cached index entries from the previous generation must be invalidated"
        );
        assert_eq!(
            adapter.read_page(&cx, 2).expect("read new page"),
            Some(new_data),
            "adapter must resolve pages from the new generation even when salts are reused"
        );
    }

    #[test]
    fn test_page_index_incremental_extend() {
        // Verify that the index extends incrementally when new frames are committed.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let page1 = sample_page(0x10);
        adapter
            .append_frame(&cx, 1, &page1, 1)
            .expect("append commit 1");

        // First read builds the index.
        assert_eq!(
            adapter.read_page(&cx, 1).expect("read"),
            Some(page1.clone())
        );

        // Append more committed frames.
        let page2 = sample_page(0x20);
        let page1_v2 = sample_page(0x30);
        adapter
            .append_frame(&cx, 2, &page2, 0)
            .expect("append page 2");
        adapter
            .append_frame(&cx, 1, &page1_v2, 3)
            .expect("append page 1 v2 (commit)");

        // Reading should trigger incremental extend, not full rebuild.
        assert_eq!(
            adapter.read_page(&cx, 1).expect("read page 1 v2"),
            Some(page1_v2),
            "incremental index extend should pick up the updated page"
        );
        assert_eq!(adapter.read_page(&cx, 2).expect("read page 2"), Some(page2));
    }

    #[test]
    fn test_commit_append_publishes_visibility_snapshot() {
        init_wal_publication_test_tracing();
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let p1 = sample_page(0x41);
        let p2 = sample_page(0x42);
        adapter.append_frame(&cx, 1, &p1, 0).expect("append p1");
        adapter.append_frame(&cx, 2, &p2, 2).expect("append commit");

        assert_eq!(
            adapter.published_snapshot.last_commit_frame,
            Some(1),
            "commit append should publish the visible commit horizon"
        );
        assert_eq!(
            adapter.published_snapshot.page_index.len(),
            2,
            "published snapshot should track both committed pages"
        );
        assert_eq!(
            adapter.published_snapshot.page_index.get(&2),
            Some(&1),
            "published snapshot must map each page to its latest committed frame"
        );
    }

    #[test]
    fn test_prepared_append_publishes_visibility_snapshot() {
        init_wal_publication_test_tracing();
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let p1 = sample_page(0x51);
        let p2 = sample_page(0x52);
        let frames = [
            WalFrameRef {
                page_number: 1,
                page_data: &p1,
                db_size_if_commit: 0,
            },
            WalFrameRef {
                page_number: 2,
                page_data: &p2,
                db_size_if_commit: 2,
            },
        ];
        let mut prepared = adapter
            .prepare_append_frames(&frames)
            .expect("prepare append")
            .expect("prepared batch");
        adapter
            .append_prepared_frames(&cx, &mut prepared)
            .expect("append prepared");

        assert_eq!(
            adapter.published_snapshot.last_commit_frame,
            Some(1),
            "prepared commit append should publish the visible commit horizon"
        );
        assert_eq!(
            adapter.published_snapshot.page_index.len(),
            2,
            "prepared commit append should publish all committed pages"
        );
        assert_eq!(
            adapter.published_snapshot.page_index.get(&2),
            Some(&1),
            "prepared commit append must map each page to its latest committed frame"
        );
    }

    #[test]
    fn test_commit_publication_refreshes_external_prefix_before_local_commit() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();

        let file_writer = open_wal_file(&vfs, &cx);
        let wal_writer =
            WalFile::create(&cx, file_writer, PAGE_SIZE, 0, test_salts()).expect("create WAL");
        let mut writer = WalBackendAdapter::new(wal_writer);

        let file_follower = open_wal_file(&vfs, &cx);
        let wal_follower = WalFile::open(&cx, file_follower).expect("open WAL");
        let mut follower = WalBackendAdapter::new(wal_follower);

        let p1 = sample_page(0x61);
        writer
            .append_frame(&cx, 1, &p1, 1)
            .expect("writer commit 1");
        writer.sync(&cx).expect("sync writer commit 1");

        let p2 = sample_page(0x62);
        writer
            .append_frame(&cx, 2, &p2, 2)
            .expect("writer commit 2");
        writer.sync(&cx).expect("sync writer commit 2");

        let p3 = sample_page(0x63);
        follower
            .append_frame(&cx, 3, &p3, 3)
            .expect("follower local commit");

        assert_eq!(
            follower.published_snapshot.last_commit_frame,
            Some(2),
            "local commit should publish on top of refreshed external WAL state"
        );
        assert_eq!(
            follower.published_snapshot.page_index.get(&1),
            Some(&0),
            "refresh-before-append should preserve earlier committed pages"
        );
        assert_eq!(
            follower.published_snapshot.page_index.get(&2),
            Some(&1),
            "refresh-before-append should publish externally committed pages"
        );
        assert_eq!(
            follower.published_snapshot.page_index.get(&3),
            Some(&2),
            "local commit should extend the published WAL visibility map"
        );
        assert_eq!(follower.read_page(&cx, 1).expect("read p1"), Some(p1));
        assert_eq!(follower.read_page(&cx, 2).expect("read p2"), Some(p2));
        assert_eq!(follower.read_page(&cx, 3).expect("read p3"), Some(p3));
    }

    // -- Partial index fallback tests --

    #[test]
    fn test_partial_index_falls_back_to_linear_scan() {
        init_wal_publication_test_tracing();
        // Verify that when the page index cap is hit, pages that weren't
        // indexed are still found via the backwards linear scan fallback.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        // Set a very small cap so we can trigger the partial-index path
        // with just a handful of frames.
        adapter.set_page_index_cap(2);

        // Write 5 distinct pages.  With a cap of 2, only the first 2 unique
        // pages will be indexed; pages 3-5 will be dropped from the index.
        let p1 = sample_page(0x01);
        let p2 = sample_page(0x02);
        let p3 = sample_page(0x03);
        let p4 = sample_page(0x04);
        let p5 = sample_page(0x05);

        adapter.append_frame(&cx, 1, &p1, 0).expect("append p1");
        adapter.append_frame(&cx, 2, &p2, 0).expect("append p2");
        adapter.append_frame(&cx, 3, &p3, 0).expect("append p3");
        adapter.append_frame(&cx, 4, &p4, 0).expect("append p4");
        adapter
            .append_frame(&cx, 5, &p5, 5)
            .expect("append p5 (commit)");

        // Pages 1 and 2 should be in the index (fast path).
        assert_eq!(
            adapter.read_page(&cx, 1).expect("read p1"),
            Some(p1),
            "indexed page should be found via HashMap"
        );
        assert_eq!(
            adapter.read_page(&cx, 2).expect("read p2"),
            Some(p2),
            "indexed page should be found via HashMap"
        );

        // Pages 3-5 were NOT indexed, but must still be found via the
        // backwards linear scan fallback.
        assert_eq!(
            adapter.read_page(&cx, 3).expect("read p3"),
            Some(p3),
            "non-indexed page must be found via linear scan fallback"
        );
        assert_eq!(
            adapter.read_page(&cx, 4).expect("read p4"),
            Some(p4),
            "non-indexed page must be found via linear scan fallback"
        );
        assert_eq!(
            adapter.read_page(&cx, 5).expect("read p5"),
            Some(p5),
            "non-indexed page must be found via linear scan fallback"
        );

        // A page that was never written should still return None.
        assert_eq!(
            adapter.read_page(&cx, 99).expect("read non-existent"),
            None,
            "non-existent page must return None even with partial index"
        );

        // Verify the index was indeed marked partial.
        assert!(
            adapter.published_snapshot.index_is_partial,
            "index_is_partial should be true when cap is exceeded"
        );
    }

    #[test]
    fn test_partial_index_returns_latest_version_via_fallback() {
        // When the same page appears multiple times and overflows the index,
        // the backwards scan must return the LATEST (highest frame index)
        // version, not the first one it encounters in a forward scan.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        // Cap at 1 so only page 1 fits in the index.
        adapter.set_page_index_cap(1);

        let old_p2 = sample_page(0xAA);
        let new_p2 = sample_page(0xBB);

        // Frame 0: page 1 (indexed)
        adapter
            .append_frame(&cx, 1, &sample_page(0x01), 0)
            .expect("append p1");
        // Frame 1: page 2 old version (NOT indexed -- cap exceeded)
        adapter
            .append_frame(&cx, 2, &old_p2, 0)
            .expect("append p2 old");
        // Frame 2: page 2 new version (NOT indexed -- cap exceeded, and
        // page 2 is not already in the index so it won't be updated)
        adapter
            .append_frame(&cx, 2, &new_p2, 3)
            .expect("append p2 new (commit)");

        // The backwards scan from frame 2 should find the newest version first.
        assert_eq!(
            adapter.read_page(&cx, 2).expect("read p2"),
            Some(new_p2),
            "backwards scan must return the most recent frame for the page"
        );
    }

    #[test]
    fn test_lookup_contract_distinguishes_authoritative_and_fallback_paths() {
        init_wal_publication_test_tracing();
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);
        adapter.set_page_index_cap(1);

        let p1 = sample_page(0x01);
        let p2 = sample_page(0x02);
        adapter.append_frame(&cx, 1, &p1, 0).expect("append p1");
        adapter
            .append_frame(&cx, 2, &p2, 2)
            .expect("append p2 commit");

        let last_commit = adapter
            .inner_mut()
            .last_commit_frame(&cx)
            .expect("last commit")
            .expect("commit exists");
        adapter
            .publish_visible_snapshot(&cx, Some(last_commit), "lookup_contract_test")
            .expect("build published snapshot");
        let snapshot = adapter.published_snapshot.clone();

        assert_eq!(
            adapter
                .resolve_visible_frame(&cx, &snapshot, 1)
                .expect("resolve indexed page"),
            WalPageLookupResolution::AuthoritativeHit { frame_index: 0 }
        );
        assert_eq!(
            adapter
                .resolve_visible_frame(&cx, &snapshot, 2)
                .expect("resolve fallback page"),
            WalPageLookupResolution::PartialIndexFallbackHit { frame_index: 1 }
        );
        assert_eq!(
            adapter
                .resolve_visible_frame(&cx, &snapshot, 99)
                .expect("resolve missing page"),
            WalPageLookupResolution::PartialIndexFallbackMiss
        );
    }

    #[test]
    fn test_lookup_contract_is_authoritative_by_default() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let p1 = sample_page(0x11);
        let p2 = sample_page(0x22);
        adapter.append_frame(&cx, 1, &p1, 0).expect("append p1");
        adapter
            .append_frame(&cx, 2, &p2, 2)
            .expect("append p2 commit");

        let last_commit = adapter
            .inner_mut()
            .last_commit_frame(&cx)
            .expect("last commit")
            .expect("commit exists");
        adapter
            .publish_visible_snapshot(&cx, Some(last_commit), "lookup_contract_default")
            .expect("build published snapshot");
        let snapshot = adapter.published_snapshot.clone();

        assert!(
            !snapshot.index_is_partial,
            "default index should be authoritative"
        );
        assert_eq!(
            adapter
                .resolve_visible_frame(&cx, &snapshot, 1)
                .expect("resolve page 1"),
            WalPageLookupResolution::AuthoritativeHit { frame_index: 0 }
        );
        assert_eq!(
            adapter
                .resolve_visible_frame(&cx, &snapshot, 2)
                .expect("resolve page 2"),
            WalPageLookupResolution::AuthoritativeHit { frame_index: 1 }
        );
        assert_eq!(
            adapter
                .resolve_visible_frame(&cx, &snapshot, 99)
                .expect("resolve missing page"),
            WalPageLookupResolution::AuthoritativeMiss
        );
    }

    #[test]
    fn test_committed_txns_since_page_uses_visible_frame_horizon() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let p1 = sample_page(0x31);
        let p2 = sample_page(0x32);
        let p3 = sample_page(0x33);

        adapter.append_frame(&cx, 1, &p1, 0).expect("append p1");
        adapter.append_frame(&cx, 2, &p2, 2).expect("commit tx1");
        adapter.append_frame(&cx, 3, &p3, 0).expect("append p3");
        adapter.append_frame(&cx, 2, &p2, 3).expect("commit tx2");

        assert_eq!(
            adapter
                .committed_txns_since_page(&cx, 1)
                .expect("count txns since page 1"),
            1
        );
        assert_eq!(
            adapter
                .committed_txns_since_page(&cx, 2)
                .expect("count txns since page 2"),
            0
        );
        assert_eq!(
            adapter
                .committed_txns_since_page(&cx, 99)
                .expect("count txns since missing page"),
            2
        );
    }

    // -- CheckpointTargetAdapterRef tests --

    #[test]
    fn test_checkpoint_adapter_write_page() {
        let cx = test_cx();
        let mut writer = MockCheckpointPageWriter;
        let mut adapter = CheckpointTargetAdapterRef {
            writer: &mut writer,
        };

        let page_no = PageNumber::new(1).expect("valid page number");
        adapter
            .write_page(&cx, page_no, &[0u8; 4096])
            .expect("write_page");
    }

    #[test]
    fn test_checkpoint_adapter_truncate_db() {
        let cx = test_cx();
        let mut writer = MockCheckpointPageWriter;
        let mut adapter = CheckpointTargetAdapterRef {
            writer: &mut writer,
        };

        adapter.truncate_db(&cx, 10).expect("truncate_db");
    }

    #[test]
    fn test_checkpoint_adapter_sync_db() {
        let cx = test_cx();
        let mut writer = MockCheckpointPageWriter;
        let mut adapter = CheckpointTargetAdapterRef {
            writer: &mut writer,
        };

        adapter.sync_db(&cx).expect("sync_db");
    }

    #[test]
    fn test_checkpoint_adapter_as_dyn_target() {
        let cx = test_cx();
        let mut writer = MockCheckpointPageWriter;
        let mut adapter = CheckpointTargetAdapterRef {
            writer: &mut writer,
        };

        // Verify it can be used as a trait object.
        let target: &mut dyn CheckpointTarget = &mut adapter;
        let page_no = PageNumber::new(3).expect("valid page number");
        target
            .write_page(&cx, page_no, &[0u8; 4096])
            .expect("write via dyn");
        target.truncate_db(&cx, 5).expect("truncate via dyn");
        target.sync_db(&cx).expect("sync via dyn");
    }
}
