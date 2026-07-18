//! Adapters bridging the WAL and pager crates at runtime.
//!
//! These adapters break the circular dependency between `fsqlite-pager` and
//! `fsqlite-wal`:
//!
//! - [`WalBackendAdapter`] wraps `WalFile` to satisfy the pager's
//!   [`WalBackend`] trait (pager -> WAL direction).
//! - `CheckpointTargetAdapterRef` wraps `CheckpointPageWriter` to satisfy the
//!   WAL executor's [`CheckpointTarget`] trait (WAL -> pager direction).

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fsqlite_error::{FrankenError, Result};
use fsqlite_pager::traits::{
    PreparedWalChecksumSeed, PreparedWalFinalizationState, PreparedWalFrameBatch,
    PreparedWalFrameMeta, WalFrameRef,
};
use fsqlite_pager::{
    CheckpointMode, CheckpointPageWriter, CheckpointResult, WalBackend, WalPublicationSnapshot,
};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};
use fsqlite_types::{PageNumber, PageSize};
#[cfg(all(feature = "native", any(unix, windows)))]
use fsqlite_vfs::DatabaseNamespaceBinding;
use fsqlite_vfs::{Vfs, VfsFile};
use fsqlite_wal::checksum::{SqliteWalChecksum, WAL_FRAME_HEADER_SIZE, WalChecksumTransform};
use fsqlite_wal::wal::WalAppendFrameRef;
use fsqlite_wal::{
    CheckpointMode as WalCheckpointMode, CheckpointState, CheckpointTarget,
    TransactionConflictPageBaseline, TransactionConflictSnapshot, WAL_HEADER_SIZE, WalFile,
    WalGenerationIdentity, WalHeader, WalSalts, execute_checkpoint, validate_wal_header_checksum,
};
use tracing::debug;
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use tracing::warn;

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
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

fn sqlite_database_header_page_size(page_one: &[u8]) -> Option<u32> {
    if page_one.len() < 18 || !page_one.starts_with(b"SQLite format 3\0") {
        return None;
    }
    let encoded = u16::from_be_bytes([page_one[16], page_one[17]]);
    let decoded = if encoded == 1 {
        65_536
    } else {
        u32::from(encoded)
    };
    PageSize::new(decoded).map(PageSize::get)
}

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
    commit_count: u64,
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
            commit_count: 0,
            page_index: Arc::new(HashMap::new()),
            index_is_partial: false,
        }
    }
}

#[must_use]
fn wal_publication_snapshot_from_published(
    snapshot: &WalPublishedSnapshot,
) -> WalPublicationSnapshot {
    WalPublicationSnapshot {
        publication_seq: snapshot.publication_seq,
        generation: snapshot.generation,
        last_commit_frame: snapshot.last_commit_frame,
        commit_count: snapshot.commit_count,
        latest_frame_entries: snapshot.page_index.len(),
        index_is_partial: snapshot.index_is_partial,
    }
}

#[derive(Debug, Clone, Copy)]
struct PendingPublicationFrame {
    page_number: u32,
    frame_index: usize,
    is_commit: bool,
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
    #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
    fec_hook: Option<FecCommitHook>,
    /// Accumulated FEC commit results (for later sidecar persistence).
    #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
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
            #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
            fec_hook: None,
            #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
            fec_pending: Vec::new(),
            page_index_cap: PAGE_INDEX_MAX_ENTRIES,
        }
    }

    /// Wrap an existing [`WalFile`] with an FEC commit hook.
    #[must_use]
    #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
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

    /// Capture the currently published WAL visibility summary for this handle.
    ///
    /// This is a cheap snapshot of the publication plane the adapter has
    /// already materialized. Call [`Self::refresh_published_snapshot`] first if
    /// the caller needs to bind to the latest on-disk committed prefix.
    #[must_use]
    pub fn published_snapshot(&self) -> WalPublicationSnapshot {
        wal_publication_snapshot_from_published(&self.published_snapshot)
    }

    /// Capture the currently pinned read snapshot, if this handle has one.
    #[must_use]
    pub fn pinned_read_snapshot(&self) -> Option<WalPublicationSnapshot> {
        self.read_snapshot
            .as_ref()
            .map(wal_publication_snapshot_from_published)
    }

    /// Refresh this handle from disk and republish the latest committed WAL
    /// visibility summary without pinning a read transaction.
    pub fn refresh_published_snapshot(&mut self, cx: &Cx) -> Result<WalPublicationSnapshot> {
        self.wal.refresh(cx)?;
        self.publish_latest_committed_snapshot(cx, "refresh_published_snapshot")?;
        Ok(self.published_snapshot())
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

        let previous_generation = self.published_snapshot.generation;
        let previous_last_commit = self.published_snapshot.last_commit_frame;
        let previous_commit_count = if previous_generation == generation {
            self.published_snapshot.commit_count
        } else {
            0
        };
        let mut page_index = if previous_generation == generation {
            std::mem::replace(
                &mut self.published_snapshot.page_index,
                Arc::new(HashMap::new()),
            )
        } else {
            Arc::new(HashMap::new())
        };
        let mut index_is_partial = if previous_generation == generation {
            self.published_snapshot.index_is_partial
        } else {
            false
        };

        let frame_delta_count = match (previous_last_commit, last_commit_frame) {
            (Some(prev), Some(curr)) if curr >= prev => curr.saturating_sub(prev),
            (Some(_) | None, Some(curr)) => curr.saturating_add(1),
            (Some(prev), None) => prev.saturating_add(1),
            (None, None) => 0,
        };

        let scan_result = match last_commit_frame {
            None => {
                Arc::make_mut(&mut page_index).clear();
                index_is_partial = false;
                Ok(0)
            }
            Some(current_last_commit) => {
                let (start, base_commit_count) =
                    match (previous_generation == generation, previous_last_commit) {
                        (true, Some(previous_last_commit))
                            if previous_last_commit < current_last_commit =>
                        {
                            (
                                previous_last_commit.saturating_add(1),
                                previous_commit_count,
                            )
                        }
                        (true, Some(previous_last_commit))
                            if previous_last_commit == current_last_commit =>
                        {
                            (current_last_commit.saturating_add(1), previous_commit_count)
                        }
                        _ => {
                            Arc::make_mut(&mut page_index).clear();
                            index_is_partial = false;
                            (0, 0)
                        }
                    };
                if start <= current_last_commit {
                    self.index_range_and_count_commits(
                        cx,
                        Arc::make_mut(&mut page_index),
                        &mut index_is_partial,
                        start,
                        current_last_commit,
                    )
                    .map(|delta| base_commit_count.saturating_add(delta))
                } else {
                    Ok(base_commit_count)
                }
            }
        };
        let commit_count = match scan_result {
            Ok(commit_count) => commit_count,
            Err(error) => {
                if previous_generation == generation {
                    self.published_snapshot.page_index = page_index;
                }
                return Err(error);
            }
        };

        let publication_seq = self.next_publication_seq;
        self.next_publication_seq = self.next_publication_seq.saturating_add(1);
        let latest_frame_entries = page_index.len();
        self.published_snapshot = WalPublishedSnapshot {
            publication_seq,
            generation,
            last_commit_frame,
            commit_count,
            page_index,
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
        &self,
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

    /// Scan frame headers from `start..=end` (inclusive), populate the page index,
    /// and count commit frames in the same pass.
    ///
    /// Since we scan forward, later frames naturally overwrite earlier entries
    /// for the same page number, ensuring "newest frame wins" semantics.
    fn index_range_and_count_commits(
        &self,
        cx: &Cx,
        page_index: &mut HashMap<u32, usize>,
        index_is_partial: &mut bool,
        start: usize,
        end: usize,
    ) -> Result<u64> {
        if start > end {
            return Ok(0);
        }

        let mut commit_count = 0_u64;
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
            if header.is_commit() {
                commit_count = commit_count.saturating_add(1);
            }
        }
        Ok(commit_count)
    }

    /// Backwards linear scan of committed frames to find a page that was not
    /// captured by the capped page index.
    ///
    /// Scans from `last_commit_frame` down to frame 0 and returns the index
    /// of the first (i.e., most recent) frame containing `page_number`, or
    /// `None` if the page is not in the WAL at all.
    fn scan_backwards_for_page(
        &self,
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
    #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
    pub fn take_fec_pending(&mut self) -> Vec<FecCommitResult> {
        std::mem::take(&mut self.fec_pending)
    }

    /// Whether FEC encoding is active.
    #[must_use]
    #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
    pub fn fec_enabled(&self) -> bool {
        self.fec_hook
            .as_ref()
            .is_some_and(FecCommitHook::is_enabled)
    }

    /// Discard buffered FEC pages (e.g. on transaction rollback).
    #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
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

    #[must_use]
    fn current_prepared_finalization_state(&self) -> PreparedWalFinalizationState {
        let generation = self.wal.generation_identity();
        let seed = self.wal.running_checksum();
        PreparedWalFinalizationState {
            checkpoint_seq: generation.checkpoint_seq,
            salt1: generation.salts.salt1,
            salt2: generation.salts.salt2,
            start_frame_index: self.wal.frame_count(),
            seed: PreparedWalChecksumSeed {
                s1: seed.s1,
                s2: seed.s2,
            },
        }
    }

    #[must_use]
    fn prepared_batch_matches_current_state(&self, prepared: &PreparedWalFrameBatch) -> bool {
        prepared
            .finalized_for
            .is_some_and(|state| state == self.current_prepared_finalization_state())
    }

    fn prepared_batch_matches_disk_state(
        &self,
        cx: &Cx,
        prepared: &PreparedWalFrameBatch,
    ) -> Result<bool> {
        let Some(state) = prepared.finalized_for else {
            return Ok(false);
        };
        let generation = WalGenerationIdentity {
            checkpoint_seq: state.checkpoint_seq,
            salts: fsqlite_wal::checksum::WalSalts {
                salt1: state.salt1,
                salt2: state.salt2,
            },
        };
        self.wal
            .prepared_append_window_still_current(cx, generation, state.start_frame_index)
    }

    fn checksum_transforms_for_prepared(
        prepared: &PreparedWalFrameBatch,
    ) -> Vec<WalChecksumTransform> {
        prepared
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
            .collect()
    }

    fn finalize_prepared_batch_against_current_state(
        &self,
        prepared: &mut PreparedWalFrameBatch,
    ) -> Result<()> {
        let checksum_transforms = Self::checksum_transforms_for_prepared(prepared);
        let final_running_checksum = self
            .wal
            .finalize_prepared_frame_bytes(&mut prepared.frame_bytes, &checksum_transforms)?;
        prepared.finalized_for = Some(self.current_prepared_finalization_state());
        prepared.finalized_running_checksum = Some(PreparedWalChecksumSeed {
            s1: final_running_checksum.s1,
            s2: final_running_checksum.s2,
        });
        Ok(())
    }

    fn finalized_running_checksum(prepared: &PreparedWalFrameBatch) -> Result<SqliteWalChecksum> {
        let Some(checksum) = prepared.finalized_running_checksum else {
            return Err(FrankenError::internal(
                "prepared WAL batch missing finalized running checksum",
            ));
        };
        Ok(SqliteWalChecksum {
            s1: checksum.s1,
            s2: checksum.s2,
        })
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
                    is_commit: db_size_if_commit != 0,
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
        let previous_last_commit = self.published_snapshot.last_commit_frame;
        let can_extend_previous = self.published_snapshot.generation == generation
            && self
                .published_snapshot
                .last_commit_frame
                .is_none_or(|previous_last_commit| previous_last_commit < last_commit_frame);
        let mut page_index = if can_extend_previous {
            std::mem::replace(
                &mut self.published_snapshot.page_index,
                Arc::new(HashMap::new()),
            )
        } else {
            Arc::new(HashMap::new())
        };
        let mut index_is_partial = if can_extend_previous {
            self.published_snapshot.index_is_partial
        } else {
            false
        };
        let previous_last_commit = if can_extend_previous {
            previous_last_commit
        } else {
            None
        };
        let previous_commit_count = if can_extend_previous {
            self.published_snapshot.commit_count
        } else {
            0
        };

        let mut frame_delta_count = 0_usize;
        let mut commit_delta_count = 0_u64;
        for frame in &self.pending_publication_frames {
            if previous_last_commit
                .is_some_and(|previous_last_commit| frame.frame_index <= previous_last_commit)
                || frame.frame_index > last_commit_frame
            {
                continue;
            }

            frame_delta_count = frame_delta_count.saturating_add(1);
            let page_index_map = Arc::make_mut(&mut page_index);
            if page_index_map.len() < self.page_index_cap
                || page_index_map.contains_key(&frame.page_number)
            {
                page_index_map.insert(frame.page_number, frame.frame_index);
            } else {
                index_is_partial = true;
            }
            if frame.is_commit {
                commit_delta_count = commit_delta_count.saturating_add(1);
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
            commit_count: previous_commit_count.saturating_add(commit_delta_count),
            page_index,
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

    fn published_snapshot(&self) -> Option<WalPublicationSnapshot> {
        Some(Self::published_snapshot(self))
    }

    fn pinned_read_snapshot(&self) -> Option<WalPublicationSnapshot> {
        Self::pinned_read_snapshot(self)
    }

    fn refresh_published_snapshot(&mut self, cx: &Cx) -> Result<Option<WalPublicationSnapshot>> {
        Self::refresh_published_snapshot(self, cx).map(Some)
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
        #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
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

        #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
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
        &self,
        frames: &[WalFrameRef<'_>],
    ) -> Result<Option<PreparedWalFrameBatch>> {
        if frames.is_empty() {
            return Ok(None);
        }

        let mut frame_bytes = Vec::new();
        let mut checksum_transforms = Vec::new();
        let last_commit_frame_offset = self.wal.prepare_frame_bytes_with_transforms_into(
            frames.len(),
            frames.iter().map(|frame| WalAppendFrameRef {
                page_number: frame.page_number,
                page_data: frame.page_data,
                db_size_if_commit: frame.db_size_if_commit,
            }),
            &mut frame_bytes,
            &mut checksum_transforms,
        )?;
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
            big_endian_checksum: self.wal.big_endian_checksum(),
            frame_metas,
            checksum_transforms,
            frame_bytes,
            last_commit_frame_offset,
            finalized_for: None,
            finalized_running_checksum: None,
        }))
    }

    fn finalize_prepared_frames(
        &self,
        _cx: &Cx,
        prepared: &mut PreparedWalFrameBatch,
    ) -> Result<()> {
        if prepared.frame_count() == 0 {
            return Ok(());
        }
        // Optimistically finalize against the adapter's current WAL state.
        // The append path still validates against both local and on-disk state
        // and will refresh/reseed if another writer advanced the append window.
        self.finalize_prepared_batch_against_current_state(prepared)
    }

    fn append_prepared_frames(
        &mut self,
        cx: &Cx,
        prepared: &mut PreparedWalFrameBatch,
    ) -> Result<()> {
        if prepared.frame_count() == 0 {
            return Ok(());
        }

        let can_reuse_prelock_finalize = self.refresh_before_append
            && self.prepared_batch_matches_current_state(prepared)
            && self.prepared_batch_matches_disk_state(cx, prepared)?;
        if self.refresh_before_append && !can_reuse_prelock_finalize {
            self.synchronize_publication_before_append(cx, "append_prepared_pre_refresh")?;
        }

        if !self.prepared_batch_matches_current_state(prepared) {
            self.finalize_prepared_batch_against_current_state(prepared)?;
        }

        let start_frame_index = self.wal.frame_count();
        self.wal.append_finalized_prepared_frame_bytes(
            cx,
            &prepared.frame_bytes,
            prepared.frame_count(),
            Self::finalized_running_checksum(prepared)?,
            prepared.last_commit_frame_offset,
        )?;
        self.refresh_before_append = false;
        let last_commit_frame = self.record_appended_frames(
            start_frame_index,
            prepared
                .frame_metas
                .iter()
                .map(|frame| (frame.page_number, frame.db_size_if_commit)),
        );

        #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
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

        // Strip the 24-byte frame header in place rather than
        // allocating a second page-sized Vec. Mirrors the fix in
        // `read_page_pinned` (`d9c410bb`): `frame_buf[HEADER..].to_vec()`
        // allocates a fresh 4 KiB buffer, memcpys the page payload into
        // it, then drops the original 4 KiB+24 B scratch — an alloc/free
        // round-trip on the hot WAL read path. Using `copy_within` +
        // `truncate` reuses the already-populated buffer: one memmove
        // (over the same bytes `to_vec` would have copied) and no new
        // allocation. `read_page` is the `&mut self` fallback path taken
        // when the caller does not hold a pinned snapshot — still hot
        // under mixed OLTP and write-path conflict resolution.
        let header_size = fsqlite_wal::checksum::WAL_FRAME_HEADER_SIZE;
        let page_size = self.wal.page_size();
        frame_buf.copy_within(header_size.., 0);
        frame_buf.truncate(page_size);
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
        Ok(Some(frame_buf))
    }

    // bd-db300.3.8.7: shared-lock read path for pinned snapshots.
    fn read_page_pinned(&self, cx: &Cx, page_number: u32) -> Result<Option<Vec<u8>>> {
        let snapshot = self.read_snapshot.as_ref().ok_or_else(|| {
            FrankenError::internal(
                "read_page_pinned called without a pinned read snapshot; \
                 use read_page(&mut self) or call begin_transaction first",
            )
        })?;
        if snapshot.last_commit_frame.is_none() {
            return Ok(None);
        }

        let resolution = self.resolve_visible_frame(cx, snapshot, page_number)?;
        let Some(frame_index) = resolution.frame_index() else {
            return Ok(None);
        };

        let mut frame_buf = vec![0u8; self.wal.frame_size()];
        let header = self.wal.read_frame_into(cx, frame_index, &mut frame_buf)?;

        if header.page_number != page_number {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "WAL page index integrity failure: expected page {page_number} \
                     at frame {frame_index}, found page {}",
                    header.page_number
                ),
            });
        }

        // Strip the 24-byte frame header in place instead of allocating
        // a fresh page-sized Vec. The pre-existing pattern did
        // `frame_buf[HEADER..].to_vec()` — on a 4 KiB page that
        // allocated a second 4 KiB buffer plus a 4 KiB memcpy and then
        // dropped the original 4 KiB+24 B frame_buf. On an MT pinned-
        // read workload every page served from the WAL paid that per-
        // read alloc/free round-trip; `_int_malloc` and `cfree` already
        // showed up in recent 2-thread profiles. Here we keep the
        // already-populated `frame_buf`, memmove the page bytes over
        // the header, truncate to `page_size`, and return it — one
        // allocation per read instead of two.
        let header_size = fsqlite_wal::checksum::WAL_FRAME_HEADER_SIZE;
        let page_size = self.wal.page_size();
        frame_buf.copy_within(header_size.., 0);
        frame_buf.truncate(page_size);
        Ok(Some(frame_buf))
    }

    fn supports_pinned_reads(&self) -> bool {
        self.read_snapshot.is_some()
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

    fn conflicting_pages_since_snapshot(
        &mut self,
        cx: &Cx,
        snapshot: TransactionConflictSnapshot,
        page_numbers: &[u32],
        _page_baselines: &[TransactionConflictPageBaseline],
    ) -> Result<Vec<u32>> {
        if page_numbers.is_empty() {
            return Ok(Vec::new());
        }

        let mut candidates = page_numbers
            .iter()
            .copied()
            .filter(|page| *page != 0)
            .collect::<Vec<_>>();
        candidates.sort_unstable();
        candidates.dedup();
        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        self.wal.refresh(cx)?;
        self.publish_latest_committed_snapshot(cx, "conflicting_pages_since_snapshot")?;
        let latest = self.published_snapshot();
        if latest.commit_count <= snapshot.commit_count
            && latest.generation == snapshot.generation
            && latest.last_commit_frame <= snapshot.last_commit_frame
        {
            return Ok(Vec::new());
        }

        if latest.generation != snapshot.generation {
            return Ok(candidates);
        }

        let Some(latest_last_commit_frame) = latest.last_commit_frame else {
            return Ok(Vec::new());
        };
        let start_frame = snapshot
            .last_commit_frame
            .map_or(0, |frame| frame.saturating_add(1));
        if start_frame > latest_last_commit_frame {
            return Ok(Vec::new());
        }

        let candidate_set = candidates.iter().copied().collect::<HashSet<_>>();
        let mut conflicts = HashSet::<u32>::new();
        for frame_index in start_frame..=latest_last_commit_frame {
            let header = self.wal.read_frame_header(cx, frame_index)?;
            if candidate_set.contains(&header.page_number) {
                conflicts.insert(header.page_number);
            }
        }

        let mut conflicts = conflicts.into_iter().collect::<Vec<_>>();
        conflicts.sort_unstable();
        Ok(conflicts)
    }

    fn committed_txn_count(&mut self, cx: &Cx) -> Result<u64> {
        let snapshot = if let Some(snapshot) = self.read_snapshot.clone() {
            snapshot
        } else {
            self.publish_latest_committed_snapshot(cx, "committed_txn_count")?;
            self.published_snapshot.clone()
        };
        Ok(snapshot.commit_count)
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
        #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
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
        #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
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
            requested_mode: mode,
            effective_mode: mode,
        })
    }
}

/// WAL backend that can recover when the path-visible `-wal` sidecar is
/// removed or replaced while this process still owns an old file descriptor.
///
/// Real SQLite can checkpoint and unlink/reset `db-wal` when it does not know
/// about a live FrankenSQLite handle. `WalFile::refresh` is intentionally
/// descriptor-local, so it cannot notice that path-level mutation. This wrapper
/// performs a path probe before mutable WAL operations and swaps in a freshly
/// opened/created `WalFile` when the path-visible sidecar no longer matches the
/// open handle.
pub struct PathRefreshingWalBackend<V: Vfs>
where
    V::File: Send + Sync + 'static,
{
    vfs: V,
    db_path: PathBuf,
    wal_path: PathBuf,
    page_size: u32,
    create_missing: bool,
    #[cfg(all(feature = "native", any(unix, windows)))]
    namespace_binding: Option<Arc<DatabaseNamespaceBinding>>,
    inner: WalBackendAdapter<V::File>,
}

impl<V> PathRefreshingWalBackend<V>
where
    V: Vfs + 'static,
    V::File: Send + Sync + 'static,
{
    #[must_use]
    pub fn new(
        vfs: V,
        db_path: impl AsRef<Path>,
        wal_path: impl AsRef<Path>,
        page_size: u32,
        wal: WalFile<V::File>,
        create_missing: bool,
        #[cfg(all(feature = "native", any(unix, windows)))] namespace_binding: Option<
            Arc<DatabaseNamespaceBinding>,
        >,
    ) -> Self {
        Self {
            vfs,
            db_path: db_path.as_ref().to_path_buf(),
            wal_path: wal_path.as_ref().to_path_buf(),
            page_size,
            create_missing,
            #[cfg(all(feature = "native", any(unix, windows)))]
            namespace_binding,
            inner: WalBackendAdapter::new(wal),
        }
    }

    #[must_use]
    pub fn into_inner(self) -> WalBackendAdapter<V::File> {
        self.inner
    }

    fn replace_inner(&mut self, cx: &Cx, wal: WalFile<V::File>) {
        let old = std::mem::replace(&mut self.inner, WalBackendAdapter::new(wal));
        let old_wal = old.into_inner();
        let _ = old_wal.close(cx);
    }

    fn create_replacement_wal(&self, cx: &Cx) -> Result<WalFile<V::File>> {
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let (file, _) = self.vfs.open(cx, Some(&self.wal_path), flags)?;
        // Random salts (GH #201): the replacement WAL must reject frames
        // from the file it replaces.
        WalFile::create(cx, file, self.page_size, 0, WalSalts::generate())
    }

    fn replace_with_created_wal(&mut self, cx: &Cx) -> Result<()> {
        let wal = self.create_replacement_wal(cx)?;
        self.replace_inner(cx, wal);
        Ok(())
    }

    fn open_replacement_wal(&self, cx: &Cx, path_file: V::File) -> Result<WalFile<V::File>> {
        let wal = WalFile::open(cx, path_file)?;
        if u32::try_from(wal.page_size()).ok() != Some(self.page_size) {
            let actual_page_size = wal.page_size();
            let expected_page_size = self.page_size;
            let _ = wal.close(cx);
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "WAL page size {actual_page_size} does not match database page size {expected_page_size} during path refresh"
                ),
            });
        }
        Ok(wal)
    }

    fn path_header_matches_current_handle(&self, cx: &Cx, path_file: &V::File) -> Result<bool> {
        let mut header_buf = [0_u8; WAL_HEADER_SIZE];
        let bytes_read = path_file.read(cx, &mut header_buf, 0)?;
        if bytes_read < WAL_HEADER_SIZE {
            return Ok(false);
        }

        let path_header = WalHeader::from_bytes(&header_buf)?;
        if !validate_wal_header_checksum(&header_buf, path_header.big_endian_checksum())? {
            return Err(FrankenError::WalCorrupt {
                detail: "WAL header checksum mismatch during path refresh".to_owned(),
            });
        }

        let current_header = self.inner.inner().header();
        Ok(path_header.magic == current_header.magic
            && path_header.format_version == current_header.format_version
            && path_header.page_size == current_header.page_size
            && path_header.checkpoint_seq == current_header.checkpoint_seq
            && path_header.salts == current_header.salts)
    }

    /// Revalidate conflict candidates across a WAL-generation transition.
    ///
    /// A stock SQLite reader may checkpoint and replace an otherwise
    /// unchanged WAL while a FrankenSQLite transaction is open. Treating the
    /// generation change itself as a write conflict produces a false
    /// `BusySnapshot`. Conversely, blindly accepting the new generation can
    /// overwrite a real external commit that was checkpointed into the main
    /// database. The only safe admission proof is therefore page-specific:
    /// every candidate must have a transaction-snapshot baseline, and its
    /// latest committed full-page image (new WAL first, main DB otherwise)
    /// must hash identically.
    ///
    /// Any missing/ambiguous baseline, unreadable or short main page, invalid
    /// database header, page-size change, WAL read error, or close failure
    /// fails closed by returning every candidate as conflicting.
    fn conflicts_after_generation_change(
        &mut self,
        cx: &Cx,
        page_numbers: &[u32],
        page_baselines: &[TransactionConflictPageBaseline],
    ) -> Vec<u32> {
        let mut candidates = page_numbers
            .iter()
            .copied()
            .filter(|page| *page != 0)
            .collect::<Vec<_>>();
        candidates.sort_unstable();
        candidates.dedup();
        if candidates.is_empty() {
            return Vec::new();
        }

        let mut baselines = HashMap::<u32, [u8; 32]>::new();
        let mut ambiguous_baselines = HashSet::<u32>::new();
        for baseline in page_baselines {
            if baseline.page_number == 0 {
                continue;
            }
            if let Some(previous) = baselines.insert(baseline.page_number, baseline.page_hash)
                && previous != baseline.page_hash
            {
                ambiguous_baselines.insert(baseline.page_number);
            }
        }

        let main_db_flags = VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB;
        let (mut db_file, _) = match self.vfs.open(cx, Some(&self.db_path), main_db_flags) {
            Ok(opened) => opened,
            Err(_) => return candidates,
        };
        let page_size = match usize::try_from(self.page_size) {
            Ok(page_size) if page_size > 0 => page_size,
            _ => {
                let _ = db_file.close(cx);
                return candidates;
            }
        };

        // Validate the current main-database header before trusting offsets.
        // SQLite encodes a 64 KiB page as the u16 value 1.
        let mut page_one = vec![0_u8; page_size];
        let page_one_read = match db_file.read(cx, &mut page_one, 0) {
            Ok(bytes_read) => bytes_read,
            Err(_) => {
                let _ = db_file.close(cx);
                return candidates;
            }
        };
        let header_page_size =
            (page_one_read == page_size).then(|| sqlite_database_header_page_size(&page_one));
        if header_page_size.flatten() != Some(self.page_size) {
            let _ = db_file.close(cx);
            return candidates;
        }

        let mut conflicts = Vec::new();
        for &page_number in &candidates {
            let Some(expected_hash) = baselines.get(&page_number).copied() else {
                conflicts.push(page_number);
                continue;
            };
            if ambiguous_baselines.contains(&page_number) {
                conflicts.push(page_number);
                continue;
            }

            let current_page = match self.inner.read_page(cx, page_number) {
                Ok(Some(page)) if page.len() == page_size => page,
                Ok(Some(_)) | Err(_) => {
                    conflicts.push(page_number);
                    continue;
                }
                Ok(None) => {
                    let mut page = vec![0_u8; page_size];
                    let page_offset = u64::from(page_number.saturating_sub(1))
                        .saturating_mul(u64::from(self.page_size));
                    match db_file.read(cx, &mut page, page_offset) {
                        Ok(bytes_read) if bytes_read == page_size => page,
                        Ok(_) | Err(_) => {
                            conflicts.push(page_number);
                            continue;
                        }
                    }
                }
            };
            let current_hash = *blake3::hash(&current_page).as_bytes();
            if current_hash != expected_hash {
                conflicts.push(page_number);
            }
        }

        if db_file.close(cx).is_err() {
            return candidates;
        }
        conflicts.sort_unstable();
        conflicts.dedup();
        conflicts
    }

    fn ensure_current_wal_path(&mut self, cx: &Cx) -> Result<()> {
        #[cfg(all(feature = "native", any(unix, windows)))]
        if let Some(binding) = &self.namespace_binding {
            binding.validate_path_identity()?;
        }
        if !self.vfs.access(cx, &self.wal_path, AccessFlags::EXISTS)? {
            if self.create_missing {
                return self.replace_with_created_wal(cx);
            }
            return Ok(());
        }

        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::WAL;
        let (mut path_file, _) = self.vfs.open(cx, Some(&self.wal_path), flags)?;
        let path_size = path_file.file_size(cx)?;
        if path_size < u64::try_from(WAL_HEADER_SIZE).unwrap_or(32) {
            let _ = path_file.close(cx);
            if self.create_missing {
                return self.replace_with_created_wal(cx);
            }
            return Ok(());
        }

        let current_size = self.inner.inner().file().file_size(cx).unwrap_or(u64::MAX);
        let path_matches_current = if path_size == current_size {
            match self.path_header_matches_current_handle(cx, &path_file) {
                Ok(matches) => matches,
                Err(err) => {
                    let _ = path_file.close(cx);
                    return Err(err);
                }
            }
        } else {
            false
        };
        if !path_matches_current {
            let wal = self.open_replacement_wal(cx, path_file)?;
            self.replace_inner(cx, wal);
        } else {
            let _ = path_file.close(cx);
        }
        Ok(())
    }
}

impl<V> WalBackend for PathRefreshingWalBackend<V>
where
    V: Vfs + 'static,
    V::File: Send + Sync + 'static,
{
    fn begin_transaction(&mut self, cx: &Cx) -> Result<()> {
        self.ensure_current_wal_path(cx)?;
        self.inner.begin_transaction(cx)
    }

    fn published_snapshot(&self) -> Option<WalPublicationSnapshot> {
        Some(self.inner.published_snapshot())
    }

    fn pinned_read_snapshot(&self) -> Option<WalPublicationSnapshot> {
        self.inner.pinned_read_snapshot()
    }

    fn refresh_published_snapshot(&mut self, cx: &Cx) -> Result<Option<WalPublicationSnapshot>> {
        self.ensure_current_wal_path(cx)?;
        self.inner.refresh_published_snapshot(cx).map(Some)
    }

    fn append_frame(
        &mut self,
        cx: &Cx,
        page_number: u32,
        page_data: &[u8],
        db_size_if_commit: u32,
    ) -> Result<()> {
        self.ensure_current_wal_path(cx)?;
        self.inner
            .append_frame(cx, page_number, page_data, db_size_if_commit)
    }

    fn append_frames(&mut self, cx: &Cx, frames: &[WalFrameRef<'_>]) -> Result<()> {
        self.ensure_current_wal_path(cx)?;
        self.inner.append_frames(cx, frames)
    }

    fn prepare_append_frames(
        &self,
        frames: &[WalFrameRef<'_>],
    ) -> Result<Option<PreparedWalFrameBatch>> {
        self.inner.prepare_append_frames(frames)
    }

    fn finalize_prepared_frames(
        &self,
        cx: &Cx,
        prepared: &mut PreparedWalFrameBatch,
    ) -> Result<()> {
        self.inner.finalize_prepared_frames(cx, prepared)
    }

    fn append_prepared_frames(
        &mut self,
        cx: &Cx,
        prepared: &mut PreparedWalFrameBatch,
    ) -> Result<()> {
        self.ensure_current_wal_path(cx)?;
        self.inner.append_prepared_frames(cx, prepared)
    }

    fn read_page(&mut self, cx: &Cx, page_number: u32) -> Result<Option<Vec<u8>>> {
        self.ensure_current_wal_path(cx)?;
        self.inner.read_page(cx, page_number)
    }

    fn read_page_pinned(&self, cx: &Cx, page_number: u32) -> Result<Option<Vec<u8>>> {
        self.inner.read_page_pinned(cx, page_number)
    }

    fn supports_pinned_reads(&self) -> bool {
        self.inner.supports_pinned_reads()
    }

    fn committed_txns_since_page(&mut self, cx: &Cx, page_number: u32) -> Result<u64> {
        self.ensure_current_wal_path(cx)?;
        self.inner.committed_txns_since_page(cx, page_number)
    }

    fn conflicting_pages_since_snapshot(
        &mut self,
        cx: &Cx,
        snapshot: TransactionConflictSnapshot,
        page_numbers: &[u32],
        page_baselines: &[TransactionConflictPageBaseline],
    ) -> Result<Vec<u32>> {
        self.ensure_current_wal_path(cx)?;
        let latest = self.inner.refresh_published_snapshot(cx)?;
        if latest.generation != snapshot.generation {
            return Ok(self.conflicts_after_generation_change(cx, page_numbers, page_baselines));
        }
        self.inner
            .conflicting_pages_since_snapshot(cx, snapshot, page_numbers, page_baselines)
    }

    fn committed_txn_count(&mut self, cx: &Cx) -> Result<u64> {
        self.ensure_current_wal_path(cx)?;
        self.inner.committed_txn_count(cx)
    }

    fn sync(&mut self, cx: &Cx) -> Result<()> {
        self.ensure_current_wal_path(cx)?;
        self.inner.sync(cx)
    }

    fn frame_count(&self) -> usize {
        self.inner.frame_count()
    }

    fn checkpoint(
        &mut self,
        cx: &Cx,
        mode: CheckpointMode,
        writer: &mut dyn CheckpointPageWriter,
        backfilled_frames: u32,
        oldest_reader_frame: Option<u32>,
    ) -> Result<CheckpointResult> {
        self.ensure_current_wal_path(cx)?;
        self.inner
            .checkpoint(cx, mode, writer, backfilled_frames, oldest_reader_frame)
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

    fn sqlite_page_one(encoded_page_size: u16) -> Vec<u8> {
        let mut page = sample_page(0x11);
        page[..16].copy_from_slice(b"SQLite format 3\0");
        page[16..18].copy_from_slice(&encoded_page_size.to_be_bytes());
        page
    }

    fn write_main_db_pages(vfs: &MemoryVfs, cx: &Cx, pages: &[Vec<u8>]) {
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::MAIN_DB;
        let (mut file, _) = vfs
            .open(cx, Some(std::path::Path::new("test.db")), flags)
            .expect("open main database");
        file.truncate(cx, 0).expect("truncate main database");
        for (index, page) in pages.iter().enumerate() {
            let offset = u64::try_from(index)
                .expect("page index fits u64")
                .saturating_mul(u64::from(PAGE_SIZE));
            file.write(cx, page, offset).expect("write database page");
        }
        file.close(cx).expect("close main database");
    }

    fn replacement_salts() -> WalSalts {
        WalSalts {
            salt1: 0x1234_5678,
            salt2: 0x9ABC_DEF0,
        }
    }

    fn replace_path_visible_wal(vfs: &MemoryVfs, cx: &Cx) {
        let wal_path = std::path::Path::new("test.db-wal");
        vfs.delete(cx, wal_path, false)
            .expect("remove old path-visible WAL");
        let file = open_wal_file(vfs, cx);
        WalFile::create(cx, file, PAGE_SIZE, 1, replacement_salts())
            .expect("create replacement WAL")
            .close(cx)
            .expect("close replacement WAL");
    }

    fn append_replacement_wal_page(
        vfs: &MemoryVfs,
        cx: &Cx,
        page_number: u32,
        page: &[u8],
        db_size_if_commit: u32,
    ) {
        let file = open_wal_file(vfs, cx);
        let wal = WalFile::open(cx, file).expect("open replacement WAL");
        let mut adapter = WalBackendAdapter::new(wal);
        adapter
            .append_frame(cx, page_number, page, db_size_if_commit)
            .expect("append replacement WAL page");
        adapter.sync(cx).expect("sync replacement WAL page");
        adapter
            .into_inner()
            .close(cx)
            .expect("close replacement WAL");
    }

    fn make_generation_transition_backend(
        vfs: &MemoryVfs,
        cx: &Cx,
    ) -> (
        PathRefreshingWalBackend<MemoryVfs>,
        TransactionConflictSnapshot,
        Vec<u8>,
    ) {
        let page_one = sqlite_page_one(u16::try_from(PAGE_SIZE).expect("page size fits u16"));
        let page_two = sample_page(0x22);
        write_main_db_pages(vfs, cx, &[page_one.clone(), page_two.clone()]);

        let file = open_wal_file(vfs, cx);
        let wal =
            WalFile::create(cx, file, PAGE_SIZE, 0, test_salts()).expect("create original WAL");
        let mut backend = PathRefreshingWalBackend::new(
            vfs.clone(),
            std::path::Path::new("test.db"),
            std::path::Path::new("test.db-wal"),
            PAGE_SIZE,
            wal,
            true,
            #[cfg(all(feature = "native", any(unix, windows)))]
            None,
        );
        backend
            .append_frame(cx, 1, &page_one, 0)
            .expect("append original page 1");
        backend
            .append_frame(cx, 2, &page_two, 2)
            .expect("append original commit");
        backend
            .begin_transaction(cx)
            .expect("pin original WAL generation");
        let pinned = backend.pinned_read_snapshot().expect("pinned WAL snapshot");
        let snapshot = TransactionConflictSnapshot {
            generation: pinned.generation,
            last_commit_frame: pinned.last_commit_frame,
            commit_count: pinned.commit_count,
        };
        replace_path_visible_wal(vfs, cx);
        (backend, snapshot, page_two)
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
    fn test_path_refresh_rejects_replacement_wal_page_size_mismatch() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let wal_path = std::path::Path::new("test.db-wal");

        let file = open_wal_file(&vfs, &cx);
        let wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");
        let mut backend = PathRefreshingWalBackend::new(
            vfs.clone(),
            std::path::Path::new("test.db"),
            wal_path,
            PAGE_SIZE,
            wal,
            true,
            #[cfg(all(feature = "native", any(unix, windows)))]
            None,
        );

        backend
            .append_frame(&cx, 1, &sample_page(0x31), 1)
            .expect("append through live backend");
        backend.sync(&cx).expect("sync live backend");

        vfs.delete(&cx, wal_path, false)
            .expect("remove path-visible WAL");
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let (replacement_file, _) = vfs
            .open(&cx, Some(wal_path), flags)
            .expect("open replacement WAL path");
        let replacement_page_size = PAGE_SIZE
            .checked_mul(2)
            .expect("test replacement page size fits u32");
        let replacement_wal = WalFile::create(
            &cx,
            replacement_file,
            replacement_page_size,
            0,
            test_salts(),
        )
        .expect("create mismatched replacement WAL");
        replacement_wal.close(&cx).expect("close replacement WAL");

        let err = backend
            .begin_transaction(&cx)
            .expect_err("path refresh should reject mismatched WAL page size");
        assert!(
            matches!(
                err,
                FrankenError::WalCorrupt { ref detail }
                    if detail.contains("does not match database page size")
                        && detail.contains("during path refresh")
            ),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_generation_change_allows_identical_full_page_baseline() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let (mut backend, snapshot, page_two) = make_generation_transition_backend(&vfs, &cx);
        let baseline = TransactionConflictPageBaseline {
            page_number: 2,
            page_hash: *blake3::hash(&page_two).as_bytes(),
        };

        let conflicts = backend
            .conflicting_pages_since_snapshot(&cx, snapshot, &[2], &[baseline])
            .expect("validate checkpoint-only generation transition");
        assert!(
            conflicts.is_empty(),
            "byte-identical checkpoint-only reset must not create a false conflict"
        );
    }

    #[test]
    fn test_generation_change_rejects_changed_candidate_page() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let (mut backend, snapshot, page_two) = make_generation_transition_backend(&vfs, &cx);
        let changed_page_two = sample_page(0x33);
        write_main_db_pages(
            &vfs,
            &cx,
            &[
                sqlite_page_one(u16::try_from(PAGE_SIZE).expect("page size fits u16")),
                changed_page_two,
            ],
        );
        let baseline = TransactionConflictPageBaseline {
            page_number: 2,
            page_hash: *blake3::hash(&page_two).as_bytes(),
        };

        let conflicts = backend
            .conflicting_pages_since_snapshot(&cx, snapshot, &[2], &[baseline])
            .expect("validate changed page across generation transition");
        assert_eq!(conflicts, vec![2]);
    }

    #[test]
    fn test_generation_change_rejects_changed_candidate_from_replacement_wal() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let (mut backend, snapshot, page_two) = make_generation_transition_backend(&vfs, &cx);
        append_replacement_wal_page(&vfs, &cx, 2, &sample_page(0x44), 2);
        let baseline = TransactionConflictPageBaseline {
            page_number: 2,
            page_hash: *blake3::hash(&page_two).as_bytes(),
        };

        let conflicts = backend
            .conflicting_pages_since_snapshot(&cx, snapshot, &[2], &[baseline])
            .expect("replacement WAL page must take precedence over identical main page");
        assert_eq!(conflicts, vec![2]);
    }

    #[test]
    fn test_generation_change_rejects_missing_baseline() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let (mut backend, snapshot, _) = make_generation_transition_backend(&vfs, &cx);

        let conflicts = backend
            .conflicting_pages_since_snapshot(&cx, snapshot, &[2], &[])
            .expect("missing baseline must fail closed");
        assert_eq!(conflicts, vec![2]);
    }

    #[test]
    fn test_generation_change_rejects_conflicting_duplicate_baselines() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let (mut backend, snapshot, page_two) = make_generation_transition_backend(&vfs, &cx);
        let baselines = [
            TransactionConflictPageBaseline {
                page_number: 2,
                page_hash: *blake3::hash(&page_two).as_bytes(),
            },
            TransactionConflictPageBaseline {
                page_number: 2,
                page_hash: *blake3::hash(&sample_page(0x55)).as_bytes(),
            },
        ];

        let conflicts = backend
            .conflicting_pages_since_snapshot(&cx, snapshot, &[2], &baselines)
            .expect("conflicting duplicate baselines must fail closed");
        assert_eq!(conflicts, vec![2]);
    }

    #[test]
    fn test_generation_change_rejects_short_candidate_page() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let (mut backend, snapshot, page_two) = make_generation_transition_backend(&vfs, &cx);
        write_main_db_pages(
            &vfs,
            &cx,
            &[sqlite_page_one(
                u16::try_from(PAGE_SIZE).expect("page size fits u16"),
            )],
        );
        let baseline = TransactionConflictPageBaseline {
            page_number: 2,
            page_hash: *blake3::hash(&page_two).as_bytes(),
        };

        let conflicts = backend
            .conflicting_pages_since_snapshot(&cx, snapshot, &[2], &[baseline])
            .expect("short page must fail closed");
        assert_eq!(conflicts, vec![2]);
    }

    #[test]
    fn test_generation_change_rejects_database_page_size_change() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let (mut backend, snapshot, page_two) = make_generation_transition_backend(&vfs, &cx);
        write_main_db_pages(&vfs, &cx, &[sqlite_page_one(8192), page_two.clone()]);
        let baseline = TransactionConflictPageBaseline {
            page_number: 2,
            page_hash: *blake3::hash(&page_two).as_bytes(),
        };

        let conflicts = backend
            .conflicting_pages_since_snapshot(&cx, snapshot, &[2], &[baseline])
            .expect("page-size change must fail closed");
        assert_eq!(conflicts, vec![2]);
    }

    #[test]
    fn test_generation_change_decodes_64k_database_header_sentinel() {
        assert_eq!(
            sqlite_database_header_page_size(&sqlite_page_one(1)),
            Some(65_536)
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
    fn test_adapter_pre_finalize_reused_when_append_window_is_stable() {
        let cx = test_cx();
        let vfs_single = MemoryVfs::new();
        let vfs_prepared = MemoryVfs::new();

        let mut adapter_single = make_adapter(&vfs_single, &cx);
        let mut adapter_prepared = make_adapter(&vfs_prepared, &cx);

        let pages: Vec<Vec<u8>> = (0..3u8).map(sample_page).collect();
        let commit_sizes = [0_u32, 0, 3];

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
            .finalize_prepared_frames(&cx, &mut prepared)
            .expect("pre-finalize prepared batch");
        let finalized_for = prepared.finalized_for.expect("finalization state");
        let finalized_running_checksum = prepared
            .finalized_running_checksum
            .expect("finalized checksum");

        adapter_prepared
            .append_prepared_frames(&cx, &mut prepared)
            .expect("append prepared");

        assert_eq!(
            prepared.finalized_for,
            Some(finalized_for),
            "stable append window should reuse the pre-lock finalization state"
        );
        assert_eq!(
            prepared.finalized_running_checksum,
            Some(finalized_running_checksum),
            "stable append window should reuse the pre-lock finalized checksum"
        );
        assert_eq!(
            adapter_single.wal.running_checksum(),
            adapter_prepared.wal.running_checksum(),
            "stable reuse path must preserve checksum chain"
        );
    }

    #[test]
    fn test_adapter_pre_finalize_reseeds_after_intervening_external_append() {
        let cx = test_cx();
        let baseline_vfs = MemoryVfs::new();
        let shared_vfs = MemoryVfs::new();

        let mut baseline = make_adapter(&baseline_vfs, &cx);
        let mut prepared_writer = make_adapter(&shared_vfs, &cx);
        let intruder_file = open_wal_file(&shared_vfs, &cx);
        let intruder_wal = WalFile::open(&cx, intruder_file).expect("open shared WAL");
        let mut intruder = WalBackendAdapter::new(intruder_wal);

        let pages: Vec<Vec<u8>> = (0..3u8).map(sample_page).collect();
        let commit_sizes = [0_u32, 0, 3];
        let intruder_page = sample_page(0xEE);

        baseline
            .append_frame(&cx, 99, &intruder_page, 1)
            .expect("baseline intruder append");
        for (index, page) in pages.iter().enumerate() {
            baseline
                .append_frame(
                    &cx,
                    u32::try_from(index + 1).expect("page number fits u32"),
                    page,
                    commit_sizes[index],
                )
                .expect("baseline append");
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
        let mut prepared = prepared_writer
            .prepare_append_frames(&batch_frames)
            .expect("prepare append")
            .expect("prepared batch");
        prepared_writer
            .finalize_prepared_frames(&cx, &mut prepared)
            .expect("pre-finalize prepared batch");
        let stale_finalization_state = prepared.finalized_for;

        intruder
            .append_frame(&cx, 99, &intruder_page, 1)
            .expect("intruder append");
        intruder.sync(&cx).expect("intruder sync");

        prepared_writer
            .append_prepared_frames(&cx, &mut prepared)
            .expect("append prepared after external growth");

        assert_ne!(
            prepared.finalized_for, stale_finalization_state,
            "intervening external growth should force prepared batch reseeding"
        );
        assert_eq!(
            baseline.wal.running_checksum(),
            prepared_writer.wal.running_checksum(),
            "reseeding path must preserve checksum chain"
        );
        assert_eq!(
            baseline.frame_count(),
            prepared_writer.frame_count(),
            "reseeding path must preserve frame count"
        );
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
        let pinned_v1 = reader
            .pinned_read_snapshot()
            .expect("reader pins publication snapshot");
        assert_eq!(pinned_v1.last_commit_frame, Some(0));
        assert_eq!(pinned_v1.commit_count, 1);
        assert_eq!(pinned_v1.latest_frame_entries, 1);
        assert!(pinned_v1.lookup_contract_is_authoritative());
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
        assert_eq!(
            reader
                .pinned_read_snapshot()
                .expect("reader keeps the same pinned snapshot"),
            pinned_v1,
            "pinned publication metadata must stay stable until the next begin"
        );

        // A new transaction snapshot should pick up the latest commit.
        reader
            .begin_transaction(&cx)
            .expect("begin reader snapshot 2");
        let pinned_v2 = reader
            .pinned_read_snapshot()
            .expect("reader repins publication snapshot");
        assert!(pinned_v2.publication_seq > pinned_v1.publication_seq);
        assert_eq!(pinned_v2.commit_count, 2);
        assert_eq!(pinned_v2.latest_frame_entries, 1);
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

    #[test]
    fn test_publication_snapshots_are_visible_through_wal_backend_trait() {
        init_wal_publication_test_tracing();
        let cx = test_cx();
        let vfs = MemoryVfs::new();

        let file_writer = open_wal_file(&vfs, &cx);
        let wal_writer =
            WalFile::create(&cx, file_writer, PAGE_SIZE, 0, test_salts()).expect("create WAL");
        let mut writer = WalBackendAdapter::new(wal_writer);

        writer
            .append_frame(&cx, 4, &sample_page(0x84), 4)
            .expect("append committed frame");
        writer.sync(&cx).expect("sync committed frame");

        let file_reader = open_wal_file(&vfs, &cx);
        let wal_reader = WalFile::open(&cx, file_reader).expect("open WAL");
        let mut reader = WalBackendAdapter::new(wal_reader);
        let backend: &mut dyn WalBackend = &mut reader;

        let published_before = backend
            .published_snapshot()
            .expect("trait should expose the adapter publication summary");
        assert_eq!(published_before.last_commit_frame, None);
        assert_eq!(published_before.commit_count, 0);

        let refreshed = backend
            .refresh_published_snapshot(&cx)
            .expect("refresh through trait should succeed")
            .expect("adapter should republish an existing committed prefix");
        assert_eq!(refreshed.last_commit_frame, Some(0));
        assert_eq!(refreshed.commit_count, 1);
        assert_eq!(refreshed.latest_frame_entries, 1);

        backend
            .begin_transaction(&cx)
            .expect("begin_transaction through trait should pin snapshot");
        let pinned = backend
            .pinned_read_snapshot()
            .expect("trait should expose the pinned read snapshot");
        assert_eq!(pinned, refreshed);
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
        let refreshed = adapter
            .refresh_published_snapshot(&cx)
            .expect("refresh published snapshot after same-salt reset");
        assert_eq!(refreshed.generation.checkpoint_seq, 1);
        assert_eq!(refreshed.generation.salts, reused_salts);
        assert_eq!(refreshed.last_commit_frame, Some(0));
        assert_eq!(refreshed.commit_count, 1);
        assert_eq!(refreshed.latest_frame_entries, 1);

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
    fn test_refresh_published_snapshot_materializes_existing_committed_prefix() {
        init_wal_publication_test_tracing();
        let cx = test_cx();
        let vfs = MemoryVfs::new();

        let file_writer = open_wal_file(&vfs, &cx);
        let wal_writer =
            WalFile::create(&cx, file_writer, PAGE_SIZE, 0, test_salts()).expect("create WAL");
        let mut writer = WalBackendAdapter::new(wal_writer);

        let p1 = sample_page(0x71);
        let p2 = sample_page(0x72);
        writer.append_frame(&cx, 1, &p1, 0).expect("append p1");
        writer
            .append_frame(&cx, 2, &p2, 2)
            .expect("append p2 commit");
        writer.sync(&cx).expect("sync writer");

        let file_reader = open_wal_file(&vfs, &cx);
        let wal_reader = WalFile::open(&cx, file_reader).expect("open reader WAL");
        let mut reader = WalBackendAdapter::new(wal_reader);

        let before = reader.published_snapshot();
        assert_eq!(before.last_commit_frame, None);
        assert_eq!(before.commit_count, 0);
        assert_eq!(before.latest_frame_entries, 0);

        let refreshed = reader
            .refresh_published_snapshot(&cx)
            .expect("refresh published snapshot");
        assert_eq!(refreshed.last_commit_frame, Some(1));
        assert_eq!(refreshed.commit_count, 1);
        assert_eq!(refreshed.latest_frame_entries, 2);
        assert!(refreshed.lookup_contract_is_authoritative());
        assert_eq!(reader.read_page(&cx, 1).expect("read p1"), Some(p1));
        assert_eq!(reader.read_page(&cx, 2).expect("read p2"), Some(p2));
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
            adapter.published_snapshot.commit_count, 1,
            "commit append should track the visible WAL commit count"
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
            adapter.published_snapshot.commit_count, 1,
            "prepared commit append should track the visible WAL commit count"
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
            follower.published_snapshot.commit_count, 3,
            "local commit publication should include refreshed external commits"
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

    #[test]
    fn test_truncate_checkpoint_republishes_empty_generation_snapshot() {
        init_wal_publication_test_tracing();
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);
        let mut writer = MockCheckpointPageWriter;

        adapter
            .append_frame(&cx, 1, &sample_page(0x61), 1)
            .expect("append committed frame");
        let before = adapter.published_snapshot();
        assert_eq!(before.last_commit_frame, Some(0));
        assert_eq!(before.commit_count, 1);
        assert_eq!(before.latest_frame_entries, 1);

        let result = adapter
            .checkpoint(&cx, CheckpointMode::Truncate, &mut writer, 0, None)
            .expect("truncate checkpoint");
        assert!(result.completed);
        assert!(result.wal_was_reset);

        let after = adapter.published_snapshot();
        assert_ne!(
            before.generation, after.generation,
            "truncate checkpoint should publish a new WAL generation"
        );
        assert_eq!(after.last_commit_frame, None);
        assert_eq!(after.commit_count, 0);
        assert_eq!(after.latest_frame_entries, 0);
        assert!(after.lookup_contract_is_authoritative());
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
        assert_eq!(
            adapter
                .committed_txn_count(&cx)
                .expect("count visible transactions"),
            2
        );
    }

    #[test]
    fn test_conflicting_pages_since_snapshot_detects_later_wal_writes() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let p1 = sample_page(0x41);
        let p2_before = sample_page(0x42);
        let p2_after = sample_page(0x43);
        let p3 = sample_page(0x44);

        adapter.append_frame(&cx, 1, &p1, 0).expect("append p1");
        adapter
            .append_frame(&cx, 2, &p2_before, 2)
            .expect("commit tx1");
        adapter
            .begin_transaction(&cx)
            .expect("pin transaction snapshot");
        let pinned = adapter
            .pinned_read_snapshot()
            .expect("transaction should expose pinned WAL snapshot");
        let conflict_snapshot = TransactionConflictSnapshot {
            generation: pinned.generation,
            last_commit_frame: pinned.last_commit_frame,
            commit_count: pinned.commit_count,
        };

        adapter
            .append_frame(&cx, 3, &p3, 0)
            .expect("append unrelated later page");
        adapter
            .append_frame(&cx, 2, &p2_after, 3)
            .expect("commit later page 2 update");

        let conflicts = adapter
            .conflicting_pages_since_snapshot(&cx, conflict_snapshot, &[2, 99], &[])
            .expect("conflict check should scan later committed frames");
        assert_eq!(conflicts, vec![2]);

        let unrelated = adapter
            .conflicting_pages_since_snapshot(&cx, conflict_snapshot, &[99], &[])
            .expect("unrelated page should stay conflict-free");
        assert!(unrelated.is_empty());
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
