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
    PreparedWalFrameMeta, WalFrameRef, WalFuture, WalLogicalReadSnapshot,
};
use fsqlite_pager::{
    CheckpointMode, CheckpointPageWriter, CheckpointResult, ParallelWalCommitReconciliation,
    WalBackend, WalPublicationSnapshot,
};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};
use fsqlite_types::{CommitSeq, PageNumber, PageSize};
#[cfg(all(feature = "native", any(unix, windows)))]
use fsqlite_vfs::DatabaseNamespaceBinding;
use fsqlite_vfs::{SyncKind, Vfs, VfsFile, VfsWriteCompletion};
use fsqlite_wal::checkpoint_executor::CheckpointTargetFuture;
use fsqlite_wal::checksum::{SqliteWalChecksum, WAL_FRAME_HEADER_SIZE, WalChecksumTransform};
use fsqlite_wal::wal::WalAppendFrameRef;
use fsqlite_wal::{
    CheckpointMode as WalCheckpointMode, CheckpointState, CheckpointTarget,
    PARALLEL_WAL_DURABLE_CERTIFICATE_MAGIC, PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE,
    ParallelWalCommitCertificate, ParallelWalDurableCertificateRecord,
    ParallelWalFramePayloadDigestBuilder, TransactionConflictPageBaseline,
    TransactionConflictSnapshot, WAL_HEADER_SIZE, WalFile, WalGenerationIdentity, WalHeader,
    WalSalts, execute_checkpoint, validate_wal_header_checksum,
};
use tracing::debug;
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use tracing::warn;

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use crate::wal_fec_adapter::{FecCommitHook, FecCommitResult};

#[cfg(test)]
mod test_support {
    use std::fmt::Debug;
    use std::future::Future;

    std::thread_local! {
        static TEST_RUNTIME: asupersync::runtime::Runtime =
            asupersync::runtime::RuntimeBuilder::current_thread()
                .blocking_threads(1, 2)
                .build()
                .expect("WAL adapter test runtime should build");
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        TEST_RUNTIME.with(|runtime| runtime.block_on(future))
    }

    pub(super) trait FutureResultTestExt<T, E>:
        Future<Output = std::result::Result<T, E>> + Sized
    {
        fn wait(self) -> std::result::Result<T, E> {
            block_on(self)
        }

        fn expect(self, message: &str) -> T
        where
            E: Debug,
        {
            block_on(self).expect(message)
        }

        fn expect_err(self, message: &str) -> E
        where
            T: Debug,
        {
            block_on(self).expect_err(message)
        }
    }

    impl<F, T, E> FutureResultTestExt<T, E> for F where
        F: Future<Output = std::result::Result<T, E>> + Sized
    {
    }
}

#[cfg(test)]
use self::test_support::FutureResultTestExt;

// ---------------------------------------------------------------------------
// WalBackendAdapter: WalFile -> WalBackend
// ---------------------------------------------------------------------------

/// Completes a tracked backend write as an error if it is discarded before
/// ownership reaches the VFS source that performs the physical mutation.
struct WalWriteCompletionPreflight<'a> {
    completion: Option<&'a VfsWriteCompletion>,
}

impl<'a> WalWriteCompletionPreflight<'a> {
    const fn new(completion: Option<&'a VfsWriteCompletion>) -> Self {
        Self { completion }
    }

    fn hand_off(&mut self) {
        self.completion = None;
    }
}

impl Drop for WalWriteCompletionPreflight<'_> {
    fn drop(&mut self) {
        if let Some(completion) = self.completion {
            completion.complete_error();
        }
    }
}

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
    /// Highest commit frame staged by the append path but not yet published.
    ///
    /// Appends only stage this horizon; publication is deferred until
    /// [`WalBackend::sync`] durably persists the frames. Preserved verbatim when
    /// a sync fails so the next successful sync republishes the same batch.
    pending_publication_commit: Option<usize>,
    /// WAL generation observed when the pending frames were staged.
    ///
    /// Publication is refused if the generation moves before the sync lands,
    /// because a checkpoint or restart invalidates the staged frame indices.
    pending_publication_generation: Option<WalGenerationIdentity>,
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
            pending_publication_commit: None,
            pending_publication_generation: None,
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
            pending_publication_commit: None,
            pending_publication_generation: None,
            fec_hook: Some(hook),
            fec_pending: Vec::new(),
            page_index_cap: PAGE_INDEX_MAX_ENTRIES,
        }
    }

    /// Whether staged, unpublished frames remain.
    ///
    /// Staged frames may already be durable — an intermediate sync makes them so
    /// without committing them — but they are not yet part of the published
    /// visibility plane. Callers that would discard, consume, or replace this
    /// adapter must check this first: dropping the staged metadata loses the
    /// batch, and a freshly wrapped adapter would republish those frames
    /// straight from the WAL with no knowledge of their publication state
    /// (GH #187).
    #[must_use]
    pub fn has_pending_publication(&self) -> bool {
        self.pending_publication_commit.is_some() || !self.pending_publication_frames.is_empty()
    }

    /// Consume the adapter and return the inner [`WalFile`].
    ///
    /// Fails closed while staged, unpublished frames remain: consuming the
    /// adapter discards the staged publication metadata, and the `WalFile` can
    /// be rewrapped by an adapter that would then publish those frames without
    /// knowing whether they were ever published or fsynced (GH #187). Drain the
    /// batch with a successful commit sync first.
    /// Returns [`FrankenError::Busy`]: this is a retryable ordering condition,
    /// not database corruption.
    pub fn into_inner(self) -> Result<WalFile<F>> {
        if self.has_pending_publication() {
            return Err(FrankenError::Busy);
        }
        Ok(self.wal)
    }

    /// Borrow the inner [`WalFile`].
    #[must_use]
    pub fn inner(&self) -> &WalFile<F> {
        &self.wal
    }

    /// Mutably borrow the inner [`WalFile`] for explicit external mutation.
    ///
    /// Invalidates the publication plane, since the caller may mutate WAL state
    /// arbitrarily. That invalidation discards any staged batch, so this fails
    /// closed while one exists rather than silently dropping the commit horizon
    /// (GH #187): after the discard, a later publish would see no pending state
    /// and could expose frames that were never fsynced. Drain the batch with a
    /// successful sync first.
    pub fn inner_mut(&mut self) -> Result<&mut WalFile<F>> {
        if self.has_pending_publication() {
            return Err(FrankenError::Busy);
        }
        self.invalidate_publication();
        Ok(&mut self.wal)
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
    pub async fn refresh_published_snapshot(&mut self, cx: &Cx) -> Result<WalPublicationSnapshot> {
        self.wal.refresh(cx).await?;
        self.publish_latest_committed_snapshot(cx, "refresh_published_snapshot")
            .await?;
        Ok(self.published_snapshot())
    }

    /// Discard published and pinned snapshots after external WAL mutation.
    fn invalidate_publication(&mut self) {
        self.read_snapshot = None;
        self.discard_pending_publication();
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
    async fn publish_visible_snapshot(
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
                    .await
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
    async fn resolve_visible_frame(
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
                    match self
                        .scan_backwards_for_page(cx, page_number, last_commit_frame)
                        .await?
                    {
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
    async fn index_range_and_count_commits(
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
            let header = self.wal.read_frame_header(cx, frame_index).await?;
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
    async fn scan_backwards_for_page(
        &self,
        cx: &Cx,
        page_number: u32,
        last_commit_frame: usize,
    ) -> Result<Option<usize>> {
        for frame_index in (0..=last_commit_frame).rev() {
            let header = self.wal.read_frame_header(cx, frame_index).await?;
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

    async fn prepared_batch_matches_disk_state(
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
            .await
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

    async fn publish_latest_committed_snapshot(
        &mut self,
        cx: &Cx,
        scenario_id: &'static str,
    ) -> Result<()> {
        let last_commit_frame = self.wal.last_commit_frame(cx)?;
        // While a local batch is staged, the WAL's own commit horizon includes
        // frames this handle appended but has not yet fsynced. Refresh and
        // unpinned read paths must not expose them, so clamp to the durable
        // prefix. With nothing staged the horizon is used unchanged, preserving
        // publication of commits made durable elsewhere.
        //
        // bd-dw8oe DESIGN INPUT (measured, do not re-attempt naively): this
        // clamp also hides PEERS' appended-but-unfsynced commits from a
        // flusher that refreshes while its own batch is staged — the
        // append-gate guards then validate freelist/page-1 state against a
        // pre-peer snapshot and can republish a consumed freelist head
        // (traced: promote/gate both read the pre-consumption page-1).
        // Relaxing the clamp to "fsynced OR below our own staged batch" did
        // NOT reduce the churn corruption rate (6/8 before and after), so it
        // was withdrawn rather than carried as risk — the guards need a
        // dedicated file-tail conflict horizon, distinct from this
        // reader-visibility plane, chartered under the freelist protocol
        // rework.
        let last_commit_frame = if self.pending_publication_commit.is_some() {
            let durable_frames = self.wal.last_fsynced_frame_count();
            last_commit_frame.filter(|frame| {
                frame
                    .checked_add(1)
                    .is_some_and(|frame_count| frame_count <= durable_frames)
            })
        } else {
            last_commit_frame
        };
        self.publish_visible_snapshot(cx, last_commit_frame, scenario_id)
            .await
    }

    async fn synchronize_publication_before_append(
        &mut self,
        cx: &Cx,
        scenario_id: &'static str,
    ) -> Result<()> {
        // Fail closed. A non-empty staged batch means a durability barrier has
        // not completed: either no sync has run, or one failed. Discarding it
        // here would silently drop the horizon, and republishing straight from
        // the WAL would expose frames that were never fsynced. Any path that
        // sets `refresh_before_append` while a batch is staged — a failed sync
        // followed by `begin_transaction`, or by `checkpoint` — funnels through
        // here, so guarding this single choke point covers all of them.
        if self.has_pending_publication() {
            return Err(FrankenError::Busy);
        }
        self.wal.refresh(cx).await?;
        self.discard_pending_publication();
        self.publish_latest_committed_snapshot(cx, scenario_id)
            .await
    }

    /// Drop every staged frame and the horizon that would have published it.
    fn discard_pending_publication(&mut self) {
        self.pending_publication_frames.clear();
        self.pending_publication_commit = None;
        self.pending_publication_generation = None;
    }

    /// Stage a commit horizon for publication without advancing visibility.
    ///
    /// Appends never publish directly: the frames may sit in the host page cache
    /// with no durable backing, so exposing them to readers would surface a
    /// commit that a crash could still erase. The horizon is retained until
    /// [`WalBackend::sync`] persists the batch.
    fn stage_pending_commit_publication(&mut self, last_commit_frame: usize) -> Result<()> {
        let generation = self.wal.generation_identity();
        // Fail closed rather than silently overwriting: staged frame indices are
        // only meaningful within one generation, so a mixed-generation batch
        // must never be merged into a single publishable horizon.
        if self
            .pending_publication_generation
            .is_some_and(|staged| staged != generation)
        {
            return Err(FrankenError::WalCorrupt {
                detail: "cannot stage a commit horizon across differing WAL generations".to_owned(),
            });
        }
        let staged = self
            .pending_publication_commit
            .map_or(last_commit_frame, |staged| staged.max(last_commit_frame));
        self.pending_publication_commit = Some(staged);
        self.pending_publication_generation = Some(generation);
        Ok(())
    }

    /// Confirm the staged horizon is still publishable against the live WAL.
    ///
    /// Refuses when the generation moved (a checkpoint or restart reindexes the
    /// WAL, invalidating staged frame indices), when the WAL is shorter than the
    /// staged horizon, or when the WAL does not yet report the staged commit.
    /// Callers must leave the pending state untouched on refusal so a later sync
    /// can retry the same batch.
    fn assert_pending_horizon_matches_wal(
        &mut self,
        cx: &Cx,
        last_commit_frame: usize,
    ) -> Result<()> {
        let generation = self.wal.generation_identity();
        if self
            .pending_publication_generation
            .is_some_and(|staged| staged != generation)
        {
            return Err(FrankenError::WalCorrupt {
                detail: "WAL generation changed before the staged commit horizon was published"
                    .to_owned(),
            });
        }

        let frame_count = self.wal.frame_count();
        if last_commit_frame >= frame_count {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "staged commit horizon {last_commit_frame} exceeds WAL frame count {frame_count}"
                ),
            });
        }

        let live_last_commit = self.wal.last_commit_frame(cx)?;
        if live_last_commit.is_none_or(|live| live < last_commit_frame) {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "WAL does not report staged commit horizon {last_commit_frame} as committed"
                ),
            });
        }

        if self
            .pending_publication_frames
            .iter()
            .any(|frame| frame.frame_index >= frame_count)
        {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "a staged publication frame lies beyond WAL frame count {frame_count}"
                ),
            });
        }

        Ok(())
    }

    fn assert_publish_safe(&mut self, cx: &Cx, last_commit_frame: usize) -> Result<()> {
        self.assert_pending_horizon_matches_wal(cx, last_commit_frame)?;

        // Delegate to the WAL's own durability tracker rather than duplicating
        // it: only it knows how far a successful fsync actually reached.
        let publish_frame_count =
            last_commit_frame
                .checked_add(1)
                .ok_or_else(|| FrankenError::WalCorrupt {
                    detail: "staged commit horizon overflows the publishable frame count"
                        .to_owned(),
                })?;
        self.wal.assert_publish_safe(publish_frame_count)?;

        Ok(())
    }

    /// Publish a logically authorized `synchronous=NORMAL` commit without
    /// claiming that an fsync occurred.
    ///
    /// The pager calls this only after the parallel-WAL certificate and both
    /// tracked write completions are terminal. A failed-sync path never reaches
    /// this hook, so its pending horizon remains fail-closed for a later retry.
    fn publish_authorized_deferred_commit(&mut self, cx: &Cx) -> Result<()> {
        let Some(last_commit_frame) = self.pending_publication_commit else {
            return Ok(());
        };
        self.assert_pending_horizon_matches_wal(cx, last_commit_frame)?;
        self.publish_pending_commit_snapshot(cx, last_commit_frame, "authorized_deferred_commit");
        self.pending_publication_commit = None;
        self.pending_publication_generation = None;
        Ok(())
    }

    /// Publish the staged commit horizon after a successful durability barrier.
    ///
    /// Fully synchronous: the staged frames already carry every page/frame pair
    /// the snapshot needs, so no WAL scan — and therefore no async I/O — is
    /// required. On refusal or failure the pending state is preserved verbatim
    /// so the next successful sync retries the identical batch.
    fn publish_pending_after_sync(&mut self, cx: &Cx) -> Result<()> {
        let Some(last_commit_frame) = self.pending_publication_commit else {
            return Ok(());
        };
        self.assert_publish_safe(cx, last_commit_frame)?;
        self.publish_pending_commit_snapshot(cx, last_commit_frame, "sync_publish_commit");
        self.pending_publication_commit = None;
        self.pending_publication_generation = None;
        Ok(())
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

    /// Install a published snapshot from staged frames alone.
    ///
    /// Deliberately synchronous. Every page/frame pair needed for the delta is
    /// already staged by `record_appended_frames`, so this never scans the WAL
    /// and never performs I/O; that keeps it callable from the synchronous
    /// [`WalBackend::sync`] path without a runtime or `block_on`.
    fn publish_pending_commit_snapshot(
        &mut self,
        cx: &Cx,
        last_commit_frame: usize,
        scenario_id: &'static str,
    ) {
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
            // No staged frame advances the horizon, which can only happen when
            // the published plane already covers `last_commit_frame` (an
            // extendable plane always contains the staged commit frame itself).
            // Rebuilding here would need a WAL scan, and this path must stay
            // synchronous, so leave visibility untouched. External refresh paths
            // retain the async rebuild for the cases that genuinely need it.
            if can_extend_previous {
                self.published_snapshot.page_index = page_index;
            }
            self.pending_publication_frames.clear();
            return;
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
    fn begin_transaction<'a>(&'a mut self, cx: &'a Cx) -> WalFuture<'a, ()> {
        Box::pin(async move {
            // Reject at the earliest illegal transition: before `wal.refresh`,
            // before pinning `read_snapshot`, and before re-arming
            // `refresh_before_append`. Beginning a transaction on top of staged,
            // unpublished frames would otherwise leave a half-transition whose
            // only symptom is a later append failure.
            if self.has_pending_publication() {
                return Err(FrankenError::Busy);
            }
            // Establish a transaction-bounded snapshot once, instead of doing an
            // expensive refresh for every page read.
            self.wal.refresh(cx).await?;
            self.publish_latest_committed_snapshot(cx, "begin_transaction")
                .await?;
            self.read_snapshot = Some(self.published_snapshot.clone());
            self.refresh_before_append = true;
            Ok(())
        })
    }

    fn published_snapshot(&self) -> Option<WalPublicationSnapshot> {
        Some(Self::published_snapshot(self))
    }

    fn pinned_read_snapshot(&self) -> Option<WalPublicationSnapshot> {
        Self::pinned_read_snapshot(self)
    }

    fn refresh_published_snapshot<'a>(
        &'a mut self,
        cx: &'a Cx,
    ) -> WalFuture<'a, Option<WalPublicationSnapshot>> {
        Box::pin(async move { Self::refresh_published_snapshot(self, cx).await.map(Some) })
    }

    fn publish_authorized_deferred_commit<'a>(&'a mut self, cx: &'a Cx) -> WalFuture<'a, ()> {
        Box::pin(async move { Self::publish_authorized_deferred_commit(self, cx) })
    }

    fn append_frame<'a>(
        &'a mut self,
        cx: &'a Cx,
        page_number: u32,
        page_data: &'a [u8],
        db_size_if_commit: u32,
    ) -> WalFuture<'a, ()> {
        Box::pin(async move {
            if self.refresh_before_append {
                // Refresh and synchronize the published base snapshot once before
                // the commit batch starts, then publish local frame deltas directly
                // from the append path.
                self.synchronize_publication_before_append(cx, "append_frame_pre_refresh")
                    .await?;
            }
            let start_frame_index = self.wal.frame_count();
            self.wal
                .append_frame(cx, page_number, page_data, db_size_if_commit)
                .await?;
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
                // Stage only: the frames are not durable until `sync`, so
                // publishing here would expose a commit a crash could erase.
                self.stage_pending_commit_publication(last_commit_frame)?;
            }

            Ok(())
        })
    }

    fn append_frames<'a>(
        &'a mut self,
        cx: &'a Cx,
        frames: &'a [WalFrameRef<'a>],
    ) -> WalFuture<'a, ()> {
        Box::pin(async move {
            if frames.is_empty() {
                return Ok(());
            }

            if self.refresh_before_append {
                self.synchronize_publication_before_append(cx, "append_frames_pre_refresh")
                    .await?;
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
            self.wal.append_frames(cx, &wal_frames).await?;
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
                // Stage only: publication is deferred to the durability barrier.
                self.stage_pending_commit_publication(last_commit_frame)?;
            }

            Ok(())
        })
    }

    fn append_frames_tracked<'a>(
        &'a mut self,
        cx: &'a Cx,
        frames: &'a [WalFrameRef<'a>],
        completion: VfsWriteCompletion,
    ) -> WalFuture<'a, ()> {
        Box::pin(async move {
            let mut preflight = WalWriteCompletionPreflight::new(Some(&completion));
            if frames.is_empty() {
                completion.complete_success();
                preflight.hand_off();
                return Ok(());
            }

            if self.refresh_before_append {
                self.synchronize_publication_before_append(cx, "append_frames_pre_refresh")
                    .await?;
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
            preflight.hand_off();
            drop(preflight);
            self.wal
                .append_frames_tracked(cx, &wal_frames, completion)
                .await?;
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
                // Stage only: publication is deferred to the durability barrier.
                self.stage_pending_commit_publication(last_commit_frame)?;
            }

            Ok(())
        })
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

    fn append_prepared_frames<'a>(
        &'a mut self,
        cx: &'a Cx,
        prepared: &'a mut PreparedWalFrameBatch,
    ) -> WalFuture<'a, ()> {
        Box::pin(async move {
            if prepared.frame_count() == 0 {
                return Ok(());
            }

            let can_reuse_prelock_finalize = self.refresh_before_append
                && self.prepared_batch_matches_current_state(prepared)
                && self.prepared_batch_matches_disk_state(cx, prepared).await?;
            if self.refresh_before_append && !can_reuse_prelock_finalize {
                self.synchronize_publication_before_append(cx, "append_prepared_pre_refresh")
                    .await?;
            }

            if !self.prepared_batch_matches_current_state(prepared) {
                self.finalize_prepared_batch_against_current_state(prepared)?;
            }

            let start_frame_index = self.wal.frame_count();
            self.wal
                .append_finalized_prepared_frame_bytes(
                    cx,
                    &prepared.frame_bytes,
                    prepared.frame_count(),
                    Self::finalized_running_checksum(prepared)?,
                    prepared.last_commit_frame_offset,
                )
                .await?;
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
                // Stage only: publication is deferred to the durability barrier.
                self.stage_pending_commit_publication(last_commit_frame)?;
            }

            Ok(())
        })
    }

    fn append_prepared_frames_tracked<'a>(
        &'a mut self,
        cx: &'a Cx,
        prepared: &'a mut PreparedWalFrameBatch,
        completion: VfsWriteCompletion,
    ) -> WalFuture<'a, ()> {
        Box::pin(async move {
            let mut preflight = WalWriteCompletionPreflight::new(Some(&completion));
            if prepared.frame_count() == 0 {
                completion.complete_success();
                preflight.hand_off();
                return Ok(());
            }

            let can_reuse_prelock_finalize = self.refresh_before_append
                && self.prepared_batch_matches_current_state(prepared)
                && self.prepared_batch_matches_disk_state(cx, prepared).await?;
            if self.refresh_before_append && !can_reuse_prelock_finalize {
                self.synchronize_publication_before_append(cx, "append_prepared_pre_refresh")
                    .await?;
            }

            if !self.prepared_batch_matches_current_state(prepared) {
                self.finalize_prepared_batch_against_current_state(prepared)?;
            }

            let start_frame_index = self.wal.frame_count();
            let final_running_checksum = Self::finalized_running_checksum(prepared)?;
            preflight.hand_off();
            drop(preflight);
            self.wal
                .append_finalized_prepared_frame_bytes_tracked(
                    cx,
                    &prepared.frame_bytes,
                    prepared.frame_count(),
                    final_running_checksum,
                    prepared.last_commit_frame_offset,
                    completion,
                )
                .await?;
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
                // Stage only: publication is deferred to the durability barrier.
                self.stage_pending_commit_publication(last_commit_frame)?;
            }

            Ok(())
        })
    }

    fn read_page<'a>(&'a mut self, cx: &'a Cx, page_number: u32) -> WalFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            let snapshot = if let Some(snapshot) = self.read_snapshot.clone() {
                snapshot
            } else {
                self.publish_latest_committed_snapshot(cx, "read_page_unpinned")
                    .await?;
                self.published_snapshot.clone()
            };
            if snapshot.last_commit_frame.is_none() {
                return Ok(None);
            }
            let snapshot_age = self
                .published_snapshot
                .publication_seq
                .saturating_sub(snapshot.publication_seq);

            let resolution = self
                .resolve_visible_frame(cx, &snapshot, page_number)
                .await?;
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
            let header = self
                .wal
                .read_frame_into(cx, frame_index, &mut frame_buf)
                .await?;

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
        })
    }

    // bd-dw8oe: gate-held read from the PHYSICAL appended tail. The published
    // snapshot consulted by `read_page` clamps to the fsynced prefix under
    // deferred sync, so it can lag peers' appended-but-unfsynced commits; the
    // append-gate guards (synthetic page-1 promotion, stale-header byte check,
    // freelist resurrection/erasure walk) need the newest appended frame, not
    // the newest published one. Under the gate the tail is stable, so a
    // backwards header scan from `frame_count() - 1` is exact.
    fn read_page_at_appended_tail<'a>(
        &'a mut self,
        cx: &'a Cx,
        page_number: u32,
    ) -> WalFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            let frame_count = self.wal.frame_count();
            let Some(tail_frame) = frame_count.checked_sub(1) else {
                return Ok(None);
            };
            let Some(frame_index) = self
                .scan_backwards_for_page(cx, page_number, tail_frame)
                .await?
            else {
                return Ok(None);
            };
            let mut frame_buf = vec![0u8; self.wal.frame_size()];
            let header = self
                .wal
                .read_frame_into(cx, frame_index, &mut frame_buf)
                .await?;
            if header.page_number != page_number {
                return Err(FrankenError::WalCorrupt {
                    detail: format!(
                        "WAL appended-tail scan integrity failure: expected page \
                         {page_number} at frame {frame_index}, found page {}",
                        header.page_number
                    ),
                });
            }
            let header_size = fsqlite_wal::checksum::WAL_FRAME_HEADER_SIZE;
            let page_size = self.wal.page_size();
            frame_buf.copy_within(header_size.., 0);
            frame_buf.truncate(page_size);
            Ok(Some(frame_buf))
        })
    }

    // bd-db300.3.8.7: shared-lock read path for pinned snapshots.
    fn read_page_pinned<'a>(
        &'a self,
        cx: &'a Cx,
        page_number: u32,
    ) -> WalFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            let snapshot = self.read_snapshot.as_ref().ok_or_else(|| {
                FrankenError::internal(
                    "read_page_pinned called without a pinned read snapshot; \
                     use read_page(&mut self) or call begin_transaction first",
                )
            })?;
            if snapshot.last_commit_frame.is_none() {
                return Ok(None);
            }

            let resolution = self
                .resolve_visible_frame(cx, snapshot, page_number)
                .await?;
            let Some(frame_index) = resolution.frame_index() else {
                return Ok(None);
            };

            let mut frame_buf = vec![0u8; self.wal.frame_size()];
            let header = self
                .wal
                .read_frame_into(cx, frame_index, &mut frame_buf)
                .await?;

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
        })
    }

    fn supports_pinned_reads(&self) -> bool {
        self.read_snapshot.is_some()
    }

    fn committed_txns_since_page<'a>(
        &'a mut self,
        cx: &'a Cx,
        page_number: u32,
    ) -> WalFuture<'a, u64> {
        Box::pin(async move {
            let snapshot = if let Some(snapshot) = self.read_snapshot.clone() {
                snapshot
            } else {
                self.publish_latest_committed_snapshot(cx, "committed_txns_since_page")
                    .await?;
                self.published_snapshot.clone()
            };
            let Some(last_commit_frame) = snapshot.last_commit_frame else {
                return Ok(0);
            };

            let resolution = self
                .resolve_visible_frame(cx, &snapshot, page_number)
                .await?;
            let Some(last_page_frame) = resolution.frame_index() else {
                let mut total_commits = 0_u64;
                for frame_index in 0..=last_commit_frame {
                    if self
                        .wal
                        .read_frame_header(cx, frame_index)
                        .await?
                        .is_commit()
                    {
                        total_commits = total_commits.saturating_add(1);
                    }
                }
                return Ok(total_commits);
            };

            let mut page_commit_frame = None;
            for frame_index in last_page_frame..=last_commit_frame {
                if self
                    .wal
                    .read_frame_header(cx, frame_index)
                    .await?
                    .is_commit()
                {
                    page_commit_frame = Some(frame_index);
                    break;
                }
            }

            let Some(page_commit_frame) = page_commit_frame else {
                return Ok(0);
            };

            let mut committed_txns_after_page = 0_u64;
            for frame_index in page_commit_frame.saturating_add(1)..=last_commit_frame {
                if self
                    .wal
                    .read_frame_header(cx, frame_index)
                    .await?
                    .is_commit()
                {
                    committed_txns_after_page = committed_txns_after_page.saturating_add(1);
                }
            }

            Ok(committed_txns_after_page)
        })
    }

    fn conflicting_pages_since_snapshot<'a>(
        &'a mut self,
        cx: &'a Cx,
        snapshot: TransactionConflictSnapshot,
        page_numbers: &'a [u32],
        _page_baselines: &'a [TransactionConflictPageBaseline],
    ) -> WalFuture<'a, Vec<u32>> {
        Box::pin(async move {
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

            self.wal.refresh(cx).await?;
            self.publish_latest_committed_snapshot(cx, "conflicting_pages_since_snapshot")
                .await?;
            let latest = self.published_snapshot();

            let mut conflicts = HashSet::<u32>::new();

            // bd-o81ov: cross-connection EOF double-allocation guard.
            //
            // Each committing connection has its own pager allocator, so two
            // connections can both hand out the same fresh EOF page number from
            // a stale committed `db_size`, link that one physical page into
            // different B-tree positions, and both commit — leaving the durable
            // image referencing a single page from multiple parents ("page N
            // referenced multiple times", broken point-seeks, duplicated rows).
            // A candidate page beyond this transaction's allocator base
            // (`snapshot_db_size`) that already exists within the current durable
            // committed size was allocated and committed by a peer first: fail
            // closed so the caller retries against the refreshed size and
            // re-allocates a non-conflicting page. This runs regardless of the
            // horizon short-circuits below, because the aliasing peer commit can
            // predate this transaction's own conflict horizon (its allocator
            // `db_size` lagged the WAL state its snapshot already observed).
            // Existing cross-process first-committer-wins: reject any candidate a
            // peer committed after this transaction's WAL conflict horizon.
            if !(latest.commit_count <= snapshot.commit_count
                && latest.generation == snapshot.generation
                && latest.last_commit_frame <= snapshot.last_commit_frame)
            {
                if latest.generation != snapshot.generation {
                    for &page in &candidates {
                        conflicts.insert(page);
                    }
                } else if let Some(latest_last_commit_frame) = latest.last_commit_frame {
                    let start_frame = snapshot
                        .last_commit_frame
                        .map_or(0, |frame| frame.saturating_add(1));
                    if start_frame <= latest_last_commit_frame {
                        let candidate_set = candidates.iter().copied().collect::<HashSet<_>>();
                        for frame_index in start_frame..=latest_last_commit_frame {
                            let header = self.wal.read_frame_header(cx, frame_index).await?;
                            if candidate_set.contains(&header.page_number) {
                                conflicts.insert(header.page_number);
                            }
                        }
                    }
                }
            }

            // A candidate beyond the allocator's begin-time committed size is a
            // freshly allocated EOF page. If ANY committed frame for that page
            // already exists in this WAL generation, a peer connection
            // allocated and committed the same physical page first — committing
            // ours would link one page into two B-tree positions ("page N
            // referenced multiple times"). The horizon-relative scan above
            // cannot catch this: a rebased/refreshed snapshot can sit PAST the
            // peer's growth frame, and commit-frame `db_size` headers are not
            // monotonic under concurrency (a stale-view pure-update commit
            // regresses them), so a size comparison is unreliable. The
            // generation-wide page index is the authoritative "was this page
            // ever committed" source. A false positive is possible when the
            // allocator base lags the publication plane and the candidate is
            // an ordinary rewrite of a recently committed page — that fails
            // closed as a transient BusySnapshot retry.
            if snapshot.snapshot_db_size > 0
                && latest.generation == snapshot.generation
                && candidates
                    .iter()
                    .any(|page| *page > snapshot.snapshot_db_size)
            {
                let published = self.published_snapshot.clone();
                for &page in &candidates {
                    if page > snapshot.snapshot_db_size
                        && !matches!(
                            self.resolve_visible_frame(cx, &published, page).await?,
                            WalPageLookupResolution::AuthoritativeMiss
                                | WalPageLookupResolution::PartialIndexFallbackMiss
                        )
                    {
                        tracing::debug!(
                            target: "fsqlite.wal.conflict",
                            page,
                            allocation_base_db_size = snapshot.snapshot_db_size,
                            latest_commit_frame = ?latest.last_commit_frame,
                            "fresh EOF allocation aliases a committed page; failing \
                             closed with BusySnapshot (bd-o81ov)"
                        );
                        conflicts.insert(page);
                    }
                }
            }

            let mut conflicts = conflicts.into_iter().collect::<Vec<_>>();
            conflicts.sort_unstable();
            Ok(conflicts)
        })
    }

    fn committed_txn_count<'a>(&'a mut self, cx: &'a Cx) -> WalFuture<'a, u64> {
        Box::pin(async move {
            let snapshot = if let Some(snapshot) = self.read_snapshot.clone() {
                snapshot
            } else {
                self.publish_latest_committed_snapshot(cx, "committed_txn_count")
                    .await?;
                self.published_snapshot.clone()
            };
            Ok(snapshot.commit_count)
        })
    }

    fn sync(&mut self, cx: &Cx) -> Result<()> {
        // Durability first. Only once the frames are on stable storage may the
        // staged commit horizon become visible to readers.
        //
        // `refresh_before_append` is deliberately NOT set on the failure paths.
        // Setting it would let the next append run
        // `synchronize_publication_before_append`, which discards the preserved
        // batch and republishes straight from the WAL — reinstating exactly the
        // publish-before-fsync hazard this guard exists to prevent. Leaving it
        // clear keeps the staged batch intact for a later retry.
        self.wal.sync(cx, SyncFlags::NORMAL)?;
        self.publish_pending_after_sync(cx)?;
        // Re-arm the pre-append resynchronization only when nothing is staged.
        //
        // Syncing mid-transaction makes the appended frames durable but does not
        // commit them: with no commit marker yet, `publish_pending_after_sync`
        // correctly publishes nothing and the frames stay staged for the commit
        // still to come. Re-arming here would send the next append through
        // `synchronize_publication_before_append`, whose fail-closed guard would
        // then reject every further append — including the commit marker — and
        // strand the transaction permanently.
        if !self.has_pending_publication() {
            self.refresh_before_append = true;
        }
        Ok(())
    }

    fn frame_count(&self) -> usize {
        self.wal.frame_count()
    }

    fn checkpoint<'a>(
        &'a mut self,
        cx: &'a Cx,
        mode: CheckpointMode,
        writer: &'a mut dyn CheckpointPageWriter,
        backfilled_frames: u32,
        oldest_reader_frame: Option<u32>,
    ) -> WalFuture<'a, CheckpointResult> {
        Box::pin(async move {
            // Fail closed BEFORE `wal.refresh` or any writer mutation. Checkpoint
            // backfills and may reset the WAL, and its inner paths can call
            // `invalidate_publication`, which discards the staged batch. Running
            // any of that against frames that were never fsynced would both lose
            // the staged horizon and risk backfilling non-durable frames, so the
            // batch must be drained by a successful sync first.
            if self.has_pending_publication() {
                return Err(FrankenError::CheckpointFailed {
                    detail: "staged, unpublished frames remain; a successful commit sync must \
                             drain them before checkpointing"
                        .to_owned(),
                });
            }
            // Refresh so planner state reflects the latest on-disk WAL shape.
            self.wal.refresh(cx).await?;
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
            let result =
                execute_checkpoint(cx, &mut self.wal, to_wal_mode(mode), state, &mut target)
                    .await?;

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

            self.publish_latest_committed_snapshot(cx, "checkpoint")
                .await?;

            Ok(CheckpointResult {
                total_frames,
                frames_backfilled: result.frames_backfilled,
                completed: result.plan.completes_checkpoint(),
                wal_was_reset: result.wal_was_reset,
                requested_mode: mode,
                effective_mode: mode,
            })
        })
    }
}

const MIN_DURABLE_CERTIFICATE_RECORD_SIZE: usize =
    ParallelWalDurableCertificateRecord::MIN_ENCODED_SIZE;
const DURABLE_CERTIFICATE_RECORD_HEADER_SIZE: usize = 14;
const MAX_ORPHAN_CERTIFICATE_LOOKBACK: usize = 64;

fn durable_certificate_declared_len(bytes: &[u8]) -> Option<usize> {
    let length_bytes = bytes.get(10..DURABLE_CERTIFICATE_RECORD_HEADER_SIZE)?;
    usize::try_from(u32::from_le_bytes([
        length_bytes[0],
        length_bytes[1],
        length_bytes[2],
        length_bytes[3],
    ]))
    .ok()
}

fn durable_certificate_declares_len(bytes: &[u8], expected: usize) -> bool {
    durable_certificate_declared_len(bytes).is_some_and(|actual| actual.cmp(&expected).is_eq())
}

fn decode_durable_certificate_record(
    bytes: &[u8],
    location: &str,
) -> Result<ParallelWalDurableCertificateRecord> {
    ParallelWalDurableCertificateRecord::from_bytes(bytes).map_err(|error| {
        FrankenError::WalCorrupt {
            detail: format!("parallel WAL certificate {location} is invalid: {error}"),
        }
    })
}

fn validate_incomplete_certificate_suffix(bytes: &[u8], anchored: bool) -> Result<()> {
    if bytes.is_empty() {
        return Ok(());
    }
    if bytes.len() > PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE {
        return Err(FrankenError::WalCorrupt {
            detail: format!(
                "parallel WAL certificate torn suffix exceeds {} bytes",
                PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE
            ),
        });
    }

    if bytes.len() < PARALLEL_WAL_DURABLE_CERTIFICATE_MAGIC.len() {
        // A failed append can leave fewer bytes than the magic itself. Once a
        // strict record boundary anchors the suffix, those bytes are
        // unambiguously one incomplete append (including legacy one-byte
        // fault injections that predate the magic prefix).
        if anchored || PARALLEL_WAL_DURABLE_CERTIFICATE_MAGIC.starts_with(bytes) {
            return Ok(());
        }
        return Err(FrankenError::WalCorrupt {
            detail: "parallel WAL certificate sidecar starts with non-record garbage".to_owned(),
        });
    }
    if !bytes.starts_with(&PARALLEL_WAL_DURABLE_CERTIFICATE_MAGIC) {
        return Err(FrankenError::WalCorrupt {
            detail: "parallel WAL certificate suffix does not start at a record boundary"
                .to_owned(),
        });
    }
    if bytes.len() < 10 {
        return Ok(());
    }
    let version = u16::from_le_bytes([bytes[8], bytes[9]]);
    if version != fsqlite_wal::PARALLEL_WAL_DURABLE_CERTIFICATE_RECORD_VERSION {
        return Err(FrankenError::WalCorrupt {
            detail: format!(
                "parallel WAL certificate suffix has unsupported record version {version}"
            ),
        });
    }
    if bytes.len() < DURABLE_CERTIFICATE_RECORD_HEADER_SIZE {
        return Ok(());
    }
    let declared_len =
        durable_certificate_declared_len(bytes).ok_or_else(|| FrankenError::WalCorrupt {
            detail: "parallel WAL certificate suffix length exceeds usize".to_owned(),
        })?;
    if !(MIN_DURABLE_CERTIFICATE_RECORD_SIZE..=PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE)
        .contains(&declared_len)
    {
        return Err(FrankenError::WalCorrupt {
            detail: format!(
                "parallel WAL certificate suffix declares invalid record length {declared_len}"
            ),
        });
    }
    if bytes.len() < declared_len {
        return Ok(());
    }

    // A complete envelope is corruption, not a torn suffix. Strict decoding
    // gives a precise CRC/footer/version diagnostic. A valid complete record
    // here would mean more than one suffix record escaped the footer walk,
    // which is equally outside the one-torn-append recovery contract.
    decode_durable_certificate_record(&bytes[..declared_len], "suffix")?;
    Err(FrankenError::WalCorrupt {
        detail:
            "parallel WAL certificate sidecar contains a complete record outside the footer chain"
                .to_owned(),
    })
}

fn combine_sidecar_io_results<const N: usize>(
    context: &str,
    results: [(&str, Result<()>); N],
) -> Result<()> {
    let failures = results
        .into_iter()
        .filter_map(|(stage, result)| result.err().map(|error| (stage, error)))
        .collect::<Vec<_>>();
    if failures.is_empty() {
        return Ok(());
    }
    if failures.len() == 1 {
        return failures
            .into_iter()
            .next()
            .map_or(Ok(()), |(_, error)| Err(error));
    }
    let details = failures
        .iter()
        .map(|(stage, error)| format!("{stage}={error}"))
        .collect::<Vec<_>>()
        .join("; ");
    Err(FrankenError::internal(format!("{context}: {details}")))
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
    /// Cached read-only descriptor for the per-commit FCW page-baseline
    /// verification in [`Self::conflicts_after_generation_change`] (bd-smxhz).
    ///
    /// That verification path is hot under concurrent writers — every commit
    /// whose snapshot generation was overtaken by a peer re-opens the main DB,
    /// reads `page_one` plus baseline pages, and closes, once per commit. On a
    /// contended disk this per-commit open/close storm serializes all writers
    /// through metadata syscalls, collapsing separate-tables write scaling.
    ///
    /// Holding the descriptor across commits is safe: `vfs.open` acquires no
    /// lock (locking is separate fcntl machinery), the descriptor tracks the
    /// inode so in-place checkpoints are seen live, and inode *replacement*
    /// (VACUUM/checkpoint-truncate to a new inode) is caught upstream by
    /// `validate_path_identity` in [`Self::ensure_current_wal_path`] — which
    /// runs before this path and fails the whole operation, so the cached fd is
    /// never read against a replaced file. Any read/header anomaly on the
    /// cached fd closes it and leaves this `None`, forcing a fresh open next
    /// commit (self-healing invalidation).
    cached_verification_db: Option<V::File>,
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
            cached_verification_db: None,
            inner: WalBackendAdapter::new(wal),
        }
    }

    #[must_use]
    pub fn into_inner(self) -> WalBackendAdapter<V::File> {
        self.inner
    }

    /// Swap in a replacement WAL, discarding the previous adapter.
    ///
    /// Fails closed while the outgoing adapter still holds a staged batch: the
    /// replacement would consume away the pending metadata, and the freshly
    /// wrapped adapter would republish those frames from the WAL without knowing
    /// they were never fsynced (GH #187). A successful sync must drain the batch
    /// before a path-visible replacement can proceed.
    fn replace_inner(&mut self, cx: &Cx, wal: WalFile<V::File>) -> Result<()> {
        if self.inner.has_pending_publication() {
            let cleanup_cx = cx.create_child();
            let _cleanup_mask = cleanup_cx.masked();
            let _ = wal.close(&cleanup_cx);
            return Err(FrankenError::Busy);
        }
        let old = std::mem::replace(&mut self.inner, WalBackendAdapter::new(wal));
        let old_wal = old.into_inner()?;
        let _ = old_wal.close(cx);
        Ok(())
    }

    async fn create_replacement_wal(&self, cx: &Cx) -> Result<WalFile<V::File>> {
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let (file, _) = self.vfs.open(cx, Some(&self.wal_path), flags)?;
        // Random salts (GH #201): the replacement WAL must reject frames
        // from the file it replaces.
        let wal = WalFile::create(cx, file, self.page_size, 0, WalSalts::generate()).await?;
        if let Err(error) = self.vfs.sync_parent_directory(cx, &self.wal_path) {
            let cleanup_cx = cx.create_child();
            let _cleanup_mask = cleanup_cx.masked();
            let _ = wal.close(&cleanup_cx);
            return Err(error);
        }
        Ok(wal)
    }

    async fn replace_with_created_wal(&mut self, cx: &Cx) -> Result<()> {
        let wal = self.create_replacement_wal(cx).await?;
        self.replace_inner(cx, wal)
    }

    async fn open_replacement_wal(&self, cx: &Cx, path_file: V::File) -> Result<WalFile<V::File>> {
        let wal = WalFile::open(cx, path_file).await?;
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

    async fn path_header_matches_current_handle(
        &self,
        cx: &Cx,
        path_file: &V::File,
    ) -> Result<bool> {
        let mut header_buf = [0_u8; WAL_HEADER_SIZE];
        let bytes_read = path_file.read(cx, &mut header_buf, 0).await?;
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
    async fn conflicts_after_generation_change(
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

        // bd-smxhz: reuse a cached read-only descriptor across commits instead
        // of opening/closing the main DB per commit. Take it into an owned
        // local so `self.inner.read_page` below can borrow disjointly, and so
        // any early-return anomaly path naturally leaves the cache empty
        // (self-healing invalidation — the fd is closed and not restored, so
        // the next commit re-opens). The success path restores it for reuse.
        let mut db_file = match self.cached_verification_db.take() {
            Some(cached) => cached,
            None => {
                let main_db_flags = VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB;
                match self.vfs.open(cx, Some(&self.db_path), main_db_flags) {
                    Ok((file, _)) => file,
                    Err(_) => return candidates,
                }
            }
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
        let page_one_read = match db_file.read(cx, &mut page_one, 0).await {
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
        // bd-jygg3: committed page count from the just-validated header
        // (bytes 28..32). A no-baseline candidate BEYOND this bound is the
        // transaction's own fresh allocation: it was never read (hence no
        // baseline, by construction) and no committed content exists out
        // there for a benign checkpoint to have replaced — failing it closed
        // aborted every single-connection bulk commit whose fresh index
        // pages landed in the candidate set after a mid-transaction WAL
        // generation change (TEXT PRIMARY KEY repro: BusySnapshot on pages
        // 4-10 with zero peers). In-range no-baseline candidates keep the
        // fail-closed verdict: those genuinely cannot be validated.
        let committed_page_count =
            u32::from_be_bytes([page_one[28], page_one[29], page_one[30], page_one[31]]);

        let mut conflicts = Vec::new();
        for &page_number in &candidates {
            let Some(expected_hash) = baselines.get(&page_number).copied() else {
                if committed_page_count > 0 && page_number > committed_page_count {
                    continue;
                }
                conflicts.push(page_number);
                continue;
            };
            if ambiguous_baselines.contains(&page_number) {
                conflicts.push(page_number);
                continue;
            }

            let current_page = match self.inner.read_page(cx, page_number).await {
                Ok(Some(page)) if page.len() == page_size => page,
                Ok(Some(_)) | Err(_) => {
                    conflicts.push(page_number);
                    continue;
                }
                Ok(None) => {
                    let mut page = vec![0_u8; page_size];
                    let page_offset = u64::from(page_number.saturating_sub(1))
                        .saturating_mul(u64::from(self.page_size));
                    match db_file.read(cx, &mut page, page_offset).await {
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

        // Success: retain the descriptor for the next commit's verification
        // rather than closing it. Every read above is read-only, so deferring
        // the close (to backend teardown / Drop) has no durability impact, and
        // it eliminates the per-commit open/close syscall storm (bd-smxhz).
        self.cached_verification_db = Some(db_file);
        conflicts.sort_unstable();
        conflicts.dedup();
        conflicts
    }

    async fn ensure_current_wal_path(&mut self, cx: &Cx) -> Result<()> {
        #[cfg(all(feature = "native", any(unix, windows)))]
        if let Some(binding) = &self.namespace_binding {
            binding.validate_path_identity()?;
        }
        if !self.vfs.access(cx, &self.wal_path, AccessFlags::EXISTS)? {
            if self.create_missing {
                return self.replace_with_created_wal(cx).await;
            }
            return Ok(());
        }

        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::WAL;
        let (mut path_file, _) = self.vfs.open(cx, Some(&self.wal_path), flags)?;
        let path_size = path_file.file_size(cx)?;
        if path_size < u64::try_from(WAL_HEADER_SIZE).unwrap_or(32) {
            let _ = path_file.close(cx);
            if self.create_missing {
                return self.replace_with_created_wal(cx).await;
            }
            return Ok(());
        }

        let current_size = self.inner.inner().file().file_size(cx).unwrap_or(u64::MAX);
        let path_matches_current = if path_size == current_size {
            match self
                .path_header_matches_current_handle(cx, &path_file)
                .await
            {
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
            let wal = self.open_replacement_wal(cx, path_file).await?;
            self.replace_inner(cx, wal)?;
        } else {
            let _ = path_file.close(cx);
        }
        Ok(())
    }

    fn certificate_sidecar_path(&self) -> PathBuf {
        let mut path = self.wal_path.as_os_str().to_owned();
        path.push("-cert");
        PathBuf::from(path)
    }

    fn certificate_checkpoint_handoff_path(&self) -> PathBuf {
        let mut path = self.wal_path.as_os_str().to_owned();
        path.push("-cert-head");
        PathBuf::from(path)
    }

    async fn read_certificate_sidecar_exact(
        file: &V::File,
        cx: &Cx,
        offset: u64,
        len: usize,
        location: &str,
    ) -> Result<Vec<u8>> {
        let mut bytes = vec![0_u8; len];
        let bytes_read = file.read(cx, &mut bytes, offset).await?;
        if bytes_read != len {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "parallel WAL certificate {location} at offset {offset} was short-read: got {bytes_read} of {len}"
                ),
            });
        }
        Ok(bytes)
    }

    async fn read_certificate_record_ending_at(
        file: &V::File,
        cx: &Cx,
        record_end: u64,
    ) -> Result<(u64, ParallelWalDurableCertificateRecord)> {
        let footer_size =
            u64::try_from(ParallelWalDurableCertificateRecord::LENGTH_FOOTER_SIZE).unwrap_or(4);
        let footer_offset = record_end.checked_sub(footer_size).ok_or_else(|| {
            FrankenError::WalCorrupt {
                detail: format!(
                    "parallel WAL certificate record ending at {record_end} has no length footer"
                ),
            }
        })?;
        let footer = Self::read_certificate_sidecar_exact(
            file,
            cx,
            footer_offset,
            ParallelWalDurableCertificateRecord::LENGTH_FOOTER_SIZE,
            "length footer",
        )
        .await?;
        let record_len = usize::try_from(u32::from_le_bytes([
            footer[0], footer[1], footer[2], footer[3],
        ]))
        .map_err(|_| FrankenError::WalCorrupt {
            detail: "parallel WAL certificate footer length exceeds usize".to_owned(),
        })?;
        if !(MIN_DURABLE_CERTIFICATE_RECORD_SIZE..=PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE)
            .contains(&record_len)
        {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "parallel WAL certificate footer declares invalid record length {record_len}"
                ),
            });
        }
        let record_len_u64 = u64::try_from(record_len).map_err(|_| FrankenError::WalCorrupt {
            detail: "parallel WAL certificate record length exceeds u64".to_owned(),
        })?;
        let record_start =
            record_end
                .checked_sub(record_len_u64)
                .ok_or_else(|| FrankenError::WalCorrupt {
                    detail: format!(
                        "parallel WAL certificate record length {record_len} exceeds end offset {record_end}"
                    ),
                })?;
        let bytes =
            Self::read_certificate_sidecar_exact(file, cx, record_start, record_len, "record")
                .await?;
        let record = decode_durable_certificate_record(&bytes, "record")?;
        Ok((record_start, record))
    }

    /// Return a safe append boundary, repairing exactly one validated torn
    /// suffix in place.
    ///
    /// The caller must hold the database's external writer or maintenance
    /// gate for the whole scan/truncate/append sequence. Keeping this helper
    /// private prevents a scan/reopen race from becoming part of the API.
    async fn prepare_certificate_sidecar_for_append(file: &mut V::File, cx: &Cx) -> Result<u64> {
        let file_size = file.file_size(cx)?;
        if file_size == 0 {
            return Ok(0);
        }

        let footer_size =
            u64::try_from(ParallelWalDurableCertificateRecord::LENGTH_FOOTER_SIZE).unwrap_or(4);
        if file_size >= footer_size {
            let footer = Self::read_certificate_sidecar_exact(
                file,
                cx,
                file_size - footer_size,
                ParallelWalDurableCertificateRecord::LENGTH_FOOTER_SIZE,
                "append-boundary length footer",
            )
            .await?;
            let record_len = usize::try_from(u32::from_le_bytes([
                footer[0], footer[1], footer[2], footer[3],
            ]))
            .unwrap_or(usize::MAX);
            let record_len_u64 = u64::try_from(record_len).unwrap_or(u64::MAX);
            if (MIN_DURABLE_CERTIFICATE_RECORD_SIZE
                ..=PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE)
                .contains(&record_len)
                && record_len_u64 <= file_size
            {
                let record_start = file_size - record_len_u64;
                let bytes = Self::read_certificate_sidecar_exact(
                    file,
                    cx,
                    record_start,
                    record_len,
                    "append-boundary record",
                )
                .await?;
                if bytes.starts_with(&PARALLEL_WAL_DURABLE_CERTIFICATE_MAGIC)
                    || durable_certificate_declares_len(&bytes, record_len)
                {
                    decode_durable_certificate_record(&bytes, "append-boundary record")?;
                    return Ok(file_size);
                }
            }
        }

        // The EOF footer was not a complete valid record. Locate at most one
        // complete anchor plus one maximum-sized suffix, using exact footer
        // boundaries rather than a free-form magic scan.
        let recovery_window_size =
            PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE.saturating_mul(2);
        let recovery_window_size_u64 = u64::try_from(recovery_window_size).unwrap_or(u64::MAX);
        let tail_offset = file_size.saturating_sub(recovery_window_size_u64);
        let tail_len =
            usize::try_from(file_size - tail_offset).map_err(|_| FrankenError::WalCorrupt {
                detail: "parallel WAL certificate append-repair window exceeds usize".to_owned(),
            })?;
        let tail = Self::read_certificate_sidecar_exact(
            file,
            cx,
            tail_offset,
            tail_len,
            "append-repair window",
        )
        .await?;
        let minimum_candidate_end = tail
            .len()
            .saturating_sub(PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE)
            .max(ParallelWalDurableCertificateRecord::LENGTH_FOOTER_SIZE);
        let mut anchor_end = None;
        for candidate_end in (minimum_candidate_end..tail.len()).rev() {
            let footer_start =
                candidate_end - ParallelWalDurableCertificateRecord::LENGTH_FOOTER_SIZE;
            let footer = &tail[footer_start..candidate_end];
            let record_len = usize::try_from(u32::from_le_bytes([
                footer[0], footer[1], footer[2], footer[3],
            ]))
            .unwrap_or(usize::MAX);
            if !(MIN_DURABLE_CERTIFICATE_RECORD_SIZE
                ..=PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE)
                .contains(&record_len)
                || record_len > candidate_end
            {
                continue;
            }
            let record_start = candidate_end - record_len;
            let record_bytes = &tail[record_start..candidate_end];
            if !record_bytes.starts_with(&PARALLEL_WAL_DURABLE_CERTIFICATE_MAGIC)
                || !durable_certificate_declares_len(record_bytes, record_len)
            {
                continue;
            }
            if ParallelWalDurableCertificateRecord::from_bytes(record_bytes).is_ok() {
                anchor_end = Some(candidate_end);
                break;
            }
        }

        let safe_end = if let Some(anchor_end) = anchor_end {
            validate_incomplete_certificate_suffix(&tail[anchor_end..], true)?;
            tail_offset
                .checked_add(u64::try_from(anchor_end).unwrap_or(u64::MAX))
                .ok_or_else(|| FrankenError::WalCorrupt {
                    detail: "parallel WAL certificate append-repair boundary overflow".to_owned(),
                })?
        } else {
            if file_size
                > u64::try_from(PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE)
                    .unwrap_or(u64::MAX)
            {
                return Err(FrankenError::WalCorrupt {
                    detail: format!(
                        "parallel WAL certificate sidecar has no valid append boundary within its bounded {}-byte recovery suffix",
                        PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE
                    ),
                });
            }
            validate_incomplete_certificate_suffix(&tail, false)?;
            0
        };

        if safe_end < file_size {
            file.truncate(cx, safe_end)?;
        }
        Ok(safe_end)
    }

    async fn append_durable_certificate_record(
        &self,
        cx: &Cx,
        certificate: &ParallelWalCommitCertificate,
        wal_frame_start: u64,
        wal_frame_end: u64,
        sync: bool,
    ) -> Result<()> {
        self.append_durable_certificate_record_with_completion(
            cx,
            certificate,
            wal_frame_start,
            wal_frame_end,
            sync,
            None,
        )
        .await
    }

    async fn append_durable_certificate_record_with_completion(
        &self,
        cx: &Cx,
        certificate: &ParallelWalCommitCertificate,
        wal_frame_start: u64,
        wal_frame_end: u64,
        sync: bool,
        completion: Option<&VfsWriteCompletion>,
    ) -> Result<()> {
        let mut preflight = WalWriteCompletionPreflight::new(completion);
        let expected_frame_start = u64::try_from(self.inner.frame_count())
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        if wal_frame_start != expected_frame_start {
            return Err(FrankenError::internal(format!(
                "parallel WAL certificate starts at frame {wal_frame_start}, expected {expected_frame_start}"
            )));
        }
        let record = ParallelWalDurableCertificateRecord::new(
            self.inner.inner().generation_identity(),
            wal_frame_start,
            wal_frame_end,
            certificate.clone(),
        )
        .map_err(|error| {
            FrankenError::internal(format!(
                "could not encode parallel WAL durability certificate: {error}"
            ))
        })?;
        let record_bytes = record.to_bytes();
        if record_bytes.len() > PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "parallel WAL certificate record is {} bytes; maximum is {}",
                    record_bytes.len(),
                    PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE
                ),
            });
        }
        let certificate_path = self.certificate_sidecar_path();
        let existed = self
            .vfs
            .access(cx, &certificate_path, AccessFlags::EXISTS)?;
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let (mut file, _) = self.vfs.open(cx, Some(&certificate_path), flags)?;
        let append_offset = Self::prepare_certificate_sidecar_for_append(&mut file, cx).await?;
        preflight.hand_off();
        drop(preflight);
        let write_result = if let Some(completion) = completion {
            file.write_tracked(cx, &record_bytes, append_offset, completion.clone())
                .await
        } else {
            file.write(cx, &record_bytes, append_offset).await
        };
        if let Err(write_error) = write_result {
            // A VFS may report a failed write after changing a prefix of the
            // destination. Restore the append boundary under a masked child
            // context so a cooperative cancellation cannot leave a torn tail
            // when the failed future itself is allowed to finish.
            let cleanup_cx = cx.create_child();
            let _cleanup_mask = cleanup_cx.masked();
            let cleanup_result = file.truncate(&cleanup_cx, append_offset);
            let close_result = file.close(&cleanup_cx);
            return combine_sidecar_io_results(
                "parallel WAL certificate append cleanup failed",
                [
                    ("write", Err(write_error)),
                    ("truncate", cleanup_result),
                    ("close", close_result),
                ],
            );
        }

        // Match the WAL's configured synchronous policy exactly. Even when
        // `sync` is false, this ordered sidecar write precedes the WAL marker;
        // neither write then claims power-loss-stable persistence.
        let finalization_cx = cx.create_child();
        let _finalization_mask = finalization_cx.masked();
        let sync_result = if sync {
            file.durable_sync(&finalization_cx, SyncKind::FullDurable)
        } else {
            Ok(())
        };
        let directory_sync_result = if sync && !existed && sync_result.is_ok() {
            self.vfs
                .sync_parent_directory(&finalization_cx, &certificate_path)
        } else {
            Ok(())
        };
        let close_result = file.close(&finalization_cx);
        combine_sidecar_io_results(
            "parallel WAL certificate append finalization failed",
            [
                ("file_sync", sync_result),
                ("directory_sync", directory_sync_result),
                ("close", close_result),
            ],
        )
    }

    async fn reconcile_certificate_sidecar_record(
        &self,
        cx: &Cx,
        expected: &ParallelWalDurableCertificateRecord,
        remove_expected_orphan: bool,
        sync: bool,
    ) -> Result<bool> {
        let certificate_path = self.certificate_sidecar_path();
        if !self
            .vfs
            .access(cx, &certificate_path, AccessFlags::EXISTS)?
        {
            return Ok(false);
        }

        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::WAL;
        let (mut file, _) = self.vfs.open(cx, Some(&certificate_path), flags)?;
        let reconciliation_result = async {
            let original_size = file.file_size(cx)?;
            let safe_end = Self::prepare_certificate_sidecar_for_append(&mut file, cx).await?;
            let latest = if safe_end == 0 {
                None
            } else {
                Some(Self::read_certificate_record_ending_at(&file, cx, safe_end).await?)
            };
            let latest_is_expected = latest
                .as_ref()
                .is_some_and(|(_, record)| record == expected);
            let sidecar_changed = if remove_expected_orphan
                && let Some((record_start, _)) = latest.as_ref()
                && latest_is_expected
            {
                file.truncate(cx, *record_start)?;
                true
            } else {
                safe_end != original_size
            };
            if sync && (latest_is_expected || sidecar_changed) {
                file.durable_sync(cx, SyncKind::FullDurable)?;
            }
            if sync && latest_is_expected && !remove_expected_orphan {
                // The original write may have created the sidecar but been
                // dropped before its directory entry was fenced.
                self.vfs.sync_parent_directory(cx, &certificate_path)?;
            }
            Ok(latest_is_expected)
        }
        .await;

        let cleanup_cx = cx.create_child();
        let _cleanup_mask = cleanup_cx.masked();
        let close_result = file.close(&cleanup_cx);
        match (reconciliation_result, close_result) {
            (Ok(latest_is_expected), Ok(())) => Ok(latest_is_expected),
            (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
            (Err(reconciliation_error), Err(close_error)) => Err(FrankenError::internal(format!(
                "parallel WAL certificate reconciliation failed and close also failed: reconciliation={reconciliation_error}; close={close_error}"
            ))),
        }
    }

    async fn persist_checkpoint_certificate_handoff(
        &self,
        cx: &Cx,
        record: &ParallelWalDurableCertificateRecord,
    ) -> Result<()> {
        let handoff_path = self.certificate_checkpoint_handoff_path();
        let existed = self.vfs.access(cx, &handoff_path, AccessFlags::EXISTS)?;
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let (mut file, _) = self.vfs.open(cx, Some(&handoff_path), flags)?;
        let record_bytes = record.to_bytes();
        if record_bytes.len() > PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE {
            let cleanup_cx = cx.create_child();
            let _cleanup_mask = cleanup_cx.masked();
            let close_result = file.close(&cleanup_cx);
            return combine_sidecar_io_results(
                "parallel WAL checkpoint certificate handoff is oversized",
                [
                    (
                        "record_size",
                        Err(FrankenError::WalCorrupt {
                            detail: format!(
                                "parallel WAL checkpoint certificate handoff is {} bytes; maximum is {}",
                                record_bytes.len(),
                                PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE
                            ),
                        }),
                    ),
                    ("close", close_result),
                ],
            );
        }
        // GH #294: when the handoff sidecar already carries exactly this
        // record, rewriting it would only bump its mtime/ctime on every
        // close-time checkpoint. The existing bytes were durably synced by
        // the write that produced them, so the fence already holds.
        if existed {
            let file_size = file.file_size(cx)?;
            if file_size == u64::try_from(record_bytes.len()).unwrap_or(u64::MAX) {
                let mut current = vec![0_u8; record_bytes.len()];
                let unchanged = file
                    .read(cx, &mut current, 0)
                    .await
                    .is_ok_and(|bytes_read| {
                        bytes_read == record_bytes.len() && current == record_bytes
                    });
                if unchanged {
                    let cleanup_cx = cx.create_child();
                    let _cleanup_mask = cleanup_cx.masked();
                    return file.close(&cleanup_cx);
                }
            }
        }
        // This fence is written before the checkpoint is allowed to reset the
        // WAL generation. Once the in-place replacement starts, finish it
        // under a cancellation mask; if any stage fails, the checkpoint
        // returns before reset and the old WAL remains authoritative.
        let mutation_cx = cx.create_child();
        let _mutation_mask = mutation_cx.masked();
        let truncate_result = file.truncate(&mutation_cx, 0);
        let write_result = if truncate_result.is_ok() {
            file.write(&mutation_cx, &record_bytes, 0).await
        } else {
            Ok(())
        };
        if let Err(write_error) = write_result {
            let cleanup_result = file.truncate(&mutation_cx, 0);
            let close_result = file.close(&mutation_cx);
            return combine_sidecar_io_results(
                "parallel WAL checkpoint certificate handoff cleanup failed",
                [
                    ("truncate_before_write", truncate_result),
                    ("write", Err(write_error)),
                    ("truncate_after_write", cleanup_result),
                    ("close", close_result),
                ],
            );
        }
        let sync_result = if truncate_result.is_ok() {
            file.durable_sync(&mutation_cx, SyncKind::FullDurable)
        } else {
            Ok(())
        };
        let directory_sync_result = if !existed && truncate_result.is_ok() && sync_result.is_ok() {
            self.vfs.sync_parent_directory(&mutation_cx, &handoff_path)
        } else {
            Ok(())
        };
        let close_result = file.close(&mutation_cx);
        combine_sidecar_io_results(
            "parallel WAL checkpoint certificate handoff finalization failed",
            [
                ("truncate", truncate_result),
                ("file_sync", sync_result),
                ("directory_sync", directory_sync_result),
                ("close", close_result),
            ],
        )
    }

    async fn checkpoint_certificate_handoff(
        &self,
        cx: &Cx,
    ) -> Result<Option<ParallelWalCommitCertificate>> {
        let handoff_path = self.certificate_checkpoint_handoff_path();
        if !self.vfs.access(cx, &handoff_path, AccessFlags::EXISTS)? {
            return Ok(None);
        }
        let flags = VfsOpenFlags::READONLY | VfsOpenFlags::WAL;
        let (mut file, _) = self.vfs.open(cx, Some(&handoff_path), flags)?;
        let read_result = async {
            let file_size =
                usize::try_from(file.file_size(cx)?).map_err(|_| FrankenError::WalCorrupt {
                    detail: "parallel WAL checkpoint certificate handoff exceeds usize".to_owned(),
                })?;
            if file_size == 0 || file_size > PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE {
                return Err(FrankenError::WalCorrupt {
                    detail: format!(
                        "parallel WAL checkpoint certificate handoff has invalid size {file_size}"
                    ),
                });
            }
            let mut bytes = vec![0_u8; file_size];
            let bytes_read = file.read(cx, &mut bytes, 0).await?;
            if bytes_read != bytes.len() {
                return Err(FrankenError::WalCorrupt {
                    detail: "parallel WAL checkpoint certificate handoff was short-read".to_owned(),
                });
            }
            let record =
                ParallelWalDurableCertificateRecord::from_bytes(&bytes).map_err(|error| {
                    FrankenError::WalCorrupt {
                        detail: format!(
                            "parallel WAL checkpoint certificate handoff is invalid: {error}"
                        ),
                    }
                })?;
            Ok(Some(record.certificate))
        }
        .await;
        let cleanup_cx = cx.create_child();
        let _cleanup_mask = cleanup_cx.masked();
        let close_result = file.close(&cleanup_cx);
        match (read_result, close_result) {
            (Ok(certificate), Ok(())) => Ok(certificate),
            (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
            (Err(read_error), Err(close_error)) => Err(FrankenError::internal(format!(
                "parallel WAL checkpoint handoff read failed and close also failed: read={read_error}; close={close_error}"
            ))),
        }
    }

    async fn wal_frame_payload_digest(
        &self,
        cx: &Cx,
        wal_frame_start: u64,
        wal_frame_end: u64,
    ) -> Result<[u8; 32]> {
        if wal_frame_start == 0 || wal_frame_end < wal_frame_start {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "invalid parallel WAL digest interval {wal_frame_start}..={wal_frame_end}"
                ),
            });
        }

        let mut digest = ParallelWalFramePayloadDigestBuilder::new();
        for frame_number in wal_frame_start..=wal_frame_end {
            let frame_index = usize::try_from(frame_number.saturating_sub(1)).map_err(|_| {
                FrankenError::WalCorrupt {
                    detail: format!(
                        "parallel WAL digest frame number {frame_number} exceeds usize"
                    ),
                }
            })?;
            let (header, page_data) = self.inner.inner().read_frame(cx, frame_index).await?;
            let page_number =
                PageNumber::new(header.page_number).ok_or_else(|| FrankenError::WalCorrupt {
                    detail: format!(
                        "parallel WAL digest frame {frame_number} has invalid page number {}",
                        header.page_number
                    ),
                })?;
            digest.update(page_number, header.db_size, &page_data);
        }
        Ok(digest.finalize())
    }

    async fn latest_authorized_durable_certificate_record(
        &self,
        cx: &Cx,
    ) -> Result<Option<ParallelWalDurableCertificateRecord>> {
        let certificate_path = self.certificate_sidecar_path();
        if !self
            .vfs
            .access(cx, &certificate_path, AccessFlags::EXISTS)?
        {
            return Ok(None);
        }
        let flags = VfsOpenFlags::READONLY | VfsOpenFlags::WAL;
        let (mut file, _) = self.vfs.open(cx, Some(&certificate_path), flags)?;
        let read_result = async {
            let file_size = file.file_size(cx)?;
            if file_size == 0 {
                return Ok(None);
            }

            // Healthy operation is O(1): the final four bytes identify the
            // exact newest record, so only its footer and bytes are read.
            let footer_size =
                u64::try_from(ParallelWalDurableCertificateRecord::LENGTH_FOOTER_SIZE)
                    .unwrap_or(4);
            let mut newest = None;
            if file_size >= footer_size {
                let footer_offset = file_size - footer_size;
                let footer = Self::read_certificate_sidecar_exact(
                    &file,
                    cx,
                    footer_offset,
                    ParallelWalDurableCertificateRecord::LENGTH_FOOTER_SIZE,
                    "newest length footer",
                )
                .await?;
                let record_len = usize::try_from(u32::from_le_bytes([
                    footer[0], footer[1], footer[2], footer[3],
                ]))
                .map_err(|_| FrankenError::WalCorrupt {
                    detail: "parallel WAL certificate newest footer length exceeds usize"
                        .to_owned(),
                })?;
                let record_len_u64 = u64::try_from(record_len).unwrap_or(u64::MAX);
                if (MIN_DURABLE_CERTIFICATE_RECORD_SIZE
                    ..=PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE)
                    .contains(&record_len)
                    && record_len_u64 <= file_size
                {
                    let record_start = file_size - record_len_u64;
                    let bytes = Self::read_certificate_sidecar_exact(
                        &file,
                        cx,
                        record_start,
                        record_len,
                        "newest record",
                    )
                    .await?;
                    // A matching magic or self-declared length makes this a
                    // fully-present envelope candidate. Strict decoding is
                    // mandatory even when its magic/version/CRC/footer is
                    // corrupt; complete corruption is never a torn suffix.
                    if bytes.starts_with(&PARALLEL_WAL_DURABLE_CERTIFICATE_MAGIC)
                        || durable_certificate_declares_len(&bytes, record_len)
                    {
                        let record =
                            decode_durable_certificate_record(&bytes, "newest record")?;
                        newest = Some((record_start, record));
                    }
                }
            }

            if newest.is_none() {
                // Invalid EOF footer means the last append may have torn.
                // Search only footer-derived candidates within one maximum
                // suffix, retaining enough preceding bytes for one maximum
                // anchor record. Magic is only a cheap validation after a
                // candidate footer establishes an exact boundary; it is never
                // used as a free-form scan key.
                let recovery_window_size =
                    PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE.saturating_mul(2);
                let recovery_window_size_u64 =
                    u64::try_from(recovery_window_size).unwrap_or(u64::MAX);
                let tail_offset = file_size.saturating_sub(recovery_window_size_u64);
                let tail_len =
                    usize::try_from(file_size - tail_offset).map_err(|_| {
                        FrankenError::WalCorrupt {
                            detail: "parallel WAL certificate recovery window exceeds usize"
                                .to_owned(),
                        }
                    })?;
                let tail = Self::read_certificate_sidecar_exact(
                    &file,
                    cx,
                    tail_offset,
                    tail_len,
                    "recovery window",
                )
                .await?;
                let minimum_candidate_end = tail
                    .len()
                    .saturating_sub(PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE)
                    .max(ParallelWalDurableCertificateRecord::LENGTH_FOOTER_SIZE);
                let mut anchor = None;
                for candidate_end in (minimum_candidate_end..tail.len()).rev() {
                    let footer_start = candidate_end
                        - ParallelWalDurableCertificateRecord::LENGTH_FOOTER_SIZE;
                    let footer = &tail[footer_start..candidate_end];
                    let record_len = usize::try_from(u32::from_le_bytes([
                        footer[0], footer[1], footer[2], footer[3],
                    ]))
                    .unwrap_or(usize::MAX);
                    if !(MIN_DURABLE_CERTIFICATE_RECORD_SIZE
                        ..=PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE)
                        .contains(&record_len)
                        || record_len > candidate_end
                    {
                        continue;
                    }
                    let record_start = candidate_end - record_len;
                    let record_bytes = &tail[record_start..candidate_end];
                    if !record_bytes.starts_with(&PARALLEL_WAL_DURABLE_CERTIFICATE_MAGIC)
                        || !durable_certificate_declares_len(record_bytes, record_len)
                    {
                        continue;
                    }
                    if let Ok(record) =
                        ParallelWalDurableCertificateRecord::from_bytes(record_bytes)
                    {
                        anchor = Some((record_start, candidate_end, record));
                        break;
                    }
                }

                if let Some((record_start, record_end, record)) = anchor {
                    validate_incomplete_certificate_suffix(&tail[record_end..], true)?;
                    let absolute_start = tail_offset
                        .checked_add(u64::try_from(record_start).unwrap_or(u64::MAX))
                        .ok_or_else(|| FrankenError::WalCorrupt {
                            detail:
                                "parallel WAL certificate recovery anchor offset overflow"
                                    .to_owned(),
                        })?;
                    newest = Some((absolute_start, record));
                } else {
                    if file_size
                        > u64::try_from(PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE)
                            .unwrap_or(u64::MAX)
                    {
                        return Err(FrankenError::WalCorrupt {
                            detail: format!(
                                "parallel WAL certificate sidecar has no valid record within its bounded {}-byte recovery suffix",
                                PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE
                            ),
                        });
                    }
                    validate_incomplete_certificate_suffix(&tail, false)?;
                    return Ok(None);
                }
            }

            let valid_frame_count = u64::try_from(self.inner.frame_count()).unwrap_or(u64::MAX);
            let wal_generation = self.inner.inner().generation_identity();
            let (mut record_start, mut record) = newest.ok_or_else(|| {
                FrankenError::WalCorrupt {
                    detail: "parallel WAL certificate recovery produced no record".to_owned(),
                }
            })?;
            let mut unauthorized_records = 0_usize;
            loop {
                // Append order is generation order. Once the newest tail
                // belongs to a prior reset generation, no earlier sidecar
                // record can authorize the current WAL; checkpoint clock
                // continuation comes from the fixed handoff anchor instead.
                if record.wal_generation != wal_generation {
                    return Ok(None);
                }
                let frame_index =
                    usize::try_from(record.wal_frame_end.saturating_sub(1)).map_err(|_| {
                        FrankenError::WalCorrupt {
                            detail: "parallel WAL certificate commit-marker index exceeds usize"
                                .to_owned(),
                        }
                    })?;
                let commit_marker_frame = if frame_index < self.inner.frame_count()
                    && self
                        .inner
                        .inner()
                        .read_frame_header(cx, frame_index)
                        .await?
                        .is_commit()
                {
                    record.wal_frame_end
                } else {
                    0
                };
                let actual_wal_frame_payload_digest =
                    if commit_marker_frame == record.wal_frame_end {
                        Some(
                            self.wal_frame_payload_digest(
                                cx,
                                record.wal_frame_start,
                                record.wal_frame_end,
                            )
                            .await?,
                        )
                    } else {
                        None
                    };
                if actual_wal_frame_payload_digest.is_some_and(|actual_digest| {
                    record.authorizes_wal_boundary(
                        wal_generation,
                        valid_frame_count,
                        commit_marker_frame,
                        actual_digest,
                    )
                }) {
                    return Ok(Some(record));
                }

                // A current-generation record whose committed boundary lies
                // beyond this reader's frame snapshot is not an orphan:
                // concurrent committers keep appending certificates while the
                // walk runs, so records newer than the snapshot are expected
                // under write load and must not consume the bounded orphan
                // budget (bd-e0ghc: three writers racing a concurrent BEGIN
                // exhausted the 64-record lookback on a healthy database).
                // Genuine orphans — records inside the snapshot that fail the
                // commit-marker or digest checks — still count, so real
                // sidecar corruption trips the bound exactly as before. The
                // walk itself stays terminating either way: record_start is
                // strictly decreasing and stops at zero.
                if record.wal_frame_end > valid_frame_count {
                    tracing::debug!(
                        target: "fsqlite::wal::durability_combiner",
                        future_certificate_epoch = record.certificate.certificate_epoch,
                        future_commit_seq_hi = record.certificate.commit_seq_hi.get(),
                        future_wal_frame_end = record.wal_frame_end,
                        valid_frame_count,
                        "skipped parallel WAL certificate newer than reader frame snapshot"
                    );
                } else {
                    unauthorized_records = unauthorized_records.saturating_add(1);
                    if unauthorized_records > MAX_ORPHAN_CERTIFICATE_LOOKBACK {
                        return Err(FrankenError::WalCorrupt {
                            detail: format!(
                                "parallel WAL certificate sidecar exceeded bounded orphan lookback {MAX_ORPHAN_CERTIFICATE_LOOKBACK}"
                            ),
                        });
                    }
                    tracing::debug!(
                        target: "fsqlite::wal::durability_combiner",
                        orphan_certificate_epoch = record.certificate.certificate_epoch,
                        orphan_commit_seq_hi = record.certificate.commit_seq_hi.get(),
                        orphan_wal_frame_end = record.wal_frame_end,
                        lookback = unauthorized_records,
                        "ignored unauthorized parallel WAL certificate tail"
                    );
                }
                if record_start == 0 {
                    return Ok(None);
                }
                (record_start, record) =
                    Self::read_certificate_record_ending_at(&file, cx, record_start).await?;
            }
        }
        .await;
        let cleanup_cx = cx.create_child();
        let _cleanup_mask = cleanup_cx.masked();
        let close_result = file.close(&cleanup_cx);
        match (read_result, close_result) {
            (Ok(certificate), Ok(())) => Ok(certificate),
            (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
            (Err(read_error), Err(close_error)) => Err(FrankenError::internal(format!(
                "parallel WAL certificate tail read failed and close also failed: read={read_error}; close={close_error}"
            ))),
        }
    }
}

impl<V> WalBackend for PathRefreshingWalBackend<V>
where
    V: Vfs + 'static,
    V::File: Send + Sync + 'static,
{
    fn begin_transaction<'a>(&'a mut self, cx: &'a Cx) -> WalFuture<'a, ()> {
        Box::pin(async move {
            self.ensure_current_wal_path(cx).await?;
            self.inner.begin_transaction(cx).await
        })
    }

    fn published_snapshot(&self) -> Option<WalPublicationSnapshot> {
        Some(self.inner.published_snapshot())
    }

    fn pinned_read_snapshot(&self) -> Option<WalPublicationSnapshot> {
        self.inner.pinned_read_snapshot()
    }

    fn pinned_logical_read_snapshot<'a>(
        &'a self,
        cx: &'a Cx,
    ) -> WalFuture<'a, Option<WalLogicalReadSnapshot>> {
        Box::pin(async move {
            let Some(pinned) = self.inner.pinned_read_snapshot() else {
                return Ok(None);
            };
            let Some(last_commit_frame) = pinned.last_commit_frame else {
                return Ok(None);
            };
            let Some(record) = self
                .latest_authorized_durable_certificate_record(cx)
                .await?
            else {
                return Ok(None);
            };
            if record.wal_generation != pinned.generation {
                return Err(FrankenError::WalCorrupt {
                    detail: "current logical WAL certificate generation differs from pinned reader"
                        .to_owned(),
                });
            }
            let certificate_commit_frame =
                usize::try_from(record.wal_frame_end.checked_sub(1).ok_or_else(|| {
                    FrankenError::WalCorrupt {
                        detail: "current logical WAL certificate ends at frame zero".to_owned(),
                    }
                })?)
                .map_err(|_| FrankenError::WalCorrupt {
                    detail: "current logical WAL certificate frame exceeds usize".to_owned(),
                })?;
            if certificate_commit_frame > last_commit_frame {
                return Err(FrankenError::WalCorrupt {
                    detail: "current logical WAL certificate extends past pinned reader horizon"
                        .to_owned(),
                });
            }

            let first_tail_frame =
                usize::try_from(record.wal_frame_end).map_err(|_| FrankenError::WalCorrupt {
                    detail: "logical WAL tail frame exceeds usize".to_owned(),
                })?;
            let mut tail_commit_count = 0_u64;
            if first_tail_frame <= last_commit_frame {
                for frame_index in first_tail_frame..=last_commit_frame {
                    if self
                        .inner
                        .inner()
                        .read_frame_header(cx, frame_index)
                        .await?
                        .is_commit()
                    {
                        tail_commit_count = tail_commit_count.checked_add(1).ok_or_else(|| {
                            FrankenError::WalCorrupt {
                                detail: "logical WAL tail commit count overflow".to_owned(),
                            }
                        })?;
                    }
                }
            }
            let visible_commit_seq = CommitSeq::new(
                record
                    .certificate
                    .commit_seq_hi
                    .get()
                    .checked_add(tail_commit_count)
                    .ok_or_else(|| FrankenError::WalCorrupt {
                        detail: "logical WAL visible commit sequence overflow".to_owned(),
                    })?,
            );
            Ok(Some(WalLogicalReadSnapshot {
                generation: pinned.generation,
                last_commit_frame: pinned.last_commit_frame,
                visible_commit_seq,
            }))
        })
    }

    fn refresh_published_snapshot<'a>(
        &'a mut self,
        cx: &'a Cx,
    ) -> WalFuture<'a, Option<WalPublicationSnapshot>> {
        Box::pin(async move {
            self.ensure_current_wal_path(cx).await?;
            self.inner.refresh_published_snapshot(cx).await.map(Some)
        })
    }

    fn publish_authorized_deferred_commit<'a>(&'a mut self, cx: &'a Cx) -> WalFuture<'a, ()> {
        Box::pin(async move { self.inner.publish_authorized_deferred_commit(cx) })
    }

    fn append_frame<'a>(
        &'a mut self,
        cx: &'a Cx,
        page_number: u32,
        page_data: &'a [u8],
        db_size_if_commit: u32,
    ) -> WalFuture<'a, ()> {
        Box::pin(async move {
            self.ensure_current_wal_path(cx).await?;
            self.inner
                .append_frame(cx, page_number, page_data, db_size_if_commit)
                .await
        })
    }

    fn append_frames<'a>(
        &'a mut self,
        cx: &'a Cx,
        frames: &'a [WalFrameRef<'a>],
    ) -> WalFuture<'a, ()> {
        Box::pin(async move {
            self.ensure_current_wal_path(cx).await?;
            self.inner.append_frames(cx, frames).await
        })
    }

    fn append_frames_tracked<'a>(
        &'a mut self,
        cx: &'a Cx,
        frames: &'a [WalFrameRef<'a>],
        completion: VfsWriteCompletion,
    ) -> WalFuture<'a, ()> {
        Box::pin(async move {
            let mut preflight = WalWriteCompletionPreflight::new(Some(&completion));
            self.ensure_current_wal_path(cx).await?;
            preflight.hand_off();
            drop(preflight);
            self.inner
                .append_frames_tracked(cx, frames, completion)
                .await
        })
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

    fn append_prepared_frames<'a>(
        &'a mut self,
        cx: &'a Cx,
        prepared: &'a mut PreparedWalFrameBatch,
    ) -> WalFuture<'a, ()> {
        Box::pin(async move {
            self.ensure_current_wal_path(cx).await?;
            self.inner.append_prepared_frames(cx, prepared).await
        })
    }

    fn append_prepared_frames_tracked<'a>(
        &'a mut self,
        cx: &'a Cx,
        prepared: &'a mut PreparedWalFrameBatch,
        completion: VfsWriteCompletion,
    ) -> WalFuture<'a, ()> {
        Box::pin(async move {
            let mut preflight = WalWriteCompletionPreflight::new(Some(&completion));
            self.ensure_current_wal_path(cx).await?;
            preflight.hand_off();
            drop(preflight);
            self.inner
                .append_prepared_frames_tracked(cx, prepared, completion)
                .await
        })
    }

    fn persist_parallel_wal_commit_certificate<'a>(
        &'a mut self,
        cx: &'a Cx,
        certificate: &'a ParallelWalCommitCertificate,
        wal_frame_start: u64,
        wal_frame_end: u64,
        sync: bool,
    ) -> WalFuture<'a, ()> {
        Box::pin(async move {
            self.ensure_current_wal_path(cx).await?;
            self.append_durable_certificate_record(
                cx,
                certificate,
                wal_frame_start,
                wal_frame_end,
                sync,
            )
            .await
        })
    }

    fn persist_parallel_wal_commit_certificate_tracked<'a>(
        &'a mut self,
        cx: &'a Cx,
        certificate: &'a ParallelWalCommitCertificate,
        wal_frame_start: u64,
        wal_frame_end: u64,
        sync: bool,
        completion: VfsWriteCompletion,
    ) -> WalFuture<'a, ()> {
        Box::pin(async move {
            let mut preflight = WalWriteCompletionPreflight::new(Some(&completion));
            self.ensure_current_wal_path(cx).await?;
            preflight.hand_off();
            drop(preflight);
            self.append_durable_certificate_record_with_completion(
                cx,
                certificate,
                wal_frame_start,
                wal_frame_end,
                sync,
                Some(&completion),
            )
            .await
        })
    }

    fn reconcile_parallel_wal_commit<'a>(
        &'a mut self,
        cx: &'a Cx,
        certificate: &'a ParallelWalCommitCertificate,
        wal_frame_start: u64,
        wal_frame_end: u64,
        sync: bool,
    ) -> WalFuture<'a, ParallelWalCommitReconciliation> {
        Box::pin(async move {
            self.ensure_current_wal_path(cx).await?;
            self.inner.wal.refresh(cx).await?;
            let wal_generation = self.inner.wal.generation_identity();
            let expected_record = ParallelWalDurableCertificateRecord::new(
                wal_generation,
                wal_frame_start,
                wal_frame_end,
                certificate.clone(),
            )
            .map_err(|error| {
                FrankenError::internal(format!(
                    "could not reconstruct in-doubt parallel WAL certificate: {error}"
                ))
            })?;

            let valid_frame_count = u64::try_from(self.inner.wal.frame_count()).unwrap_or(u64::MAX);
            let target_commit_present = if valid_frame_count < wal_frame_end {
                false
            } else {
                let target_index =
                    usize::try_from(wal_frame_end.saturating_sub(1)).map_err(|_| {
                        FrankenError::WalCorrupt {
                            detail: "in-doubt WAL commit-marker index exceeds usize".to_owned(),
                        }
                    })?;
                self.inner
                    .wal
                    .read_frame_header(cx, target_index)
                    .await?
                    .is_commit()
            };

            if target_commit_present {
                if valid_frame_count != wal_frame_end {
                    return Err(FrankenError::WalCorrupt {
                        detail: format!(
                            "in-doubt parallel WAL interval ends at frame {wal_frame_end}, but the retained writer gate observed committed frame count {valid_frame_count}"
                        ),
                    });
                }
                let actual_wal_frame_payload_digest = self
                    .wal_frame_payload_digest(cx, wal_frame_start, wal_frame_end)
                    .await?;
                if !expected_record.authorizes_wal_boundary(
                    wal_generation,
                    valid_frame_count,
                    wal_frame_end,
                    actual_wal_frame_payload_digest,
                ) {
                    return Err(FrankenError::WalCorrupt {
                        detail: format!(
                            "in-doubt parallel WAL interval {wal_frame_start}..={wal_frame_end} does not match its content-bound certificate"
                        ),
                    });
                }
                let sidecar_is_exact = self
                    .reconcile_certificate_sidecar_record(cx, &expected_record, false, sync)
                    .await?;
                if !sidecar_is_exact {
                    return Err(FrankenError::WalCorrupt {
                        detail: format!(
                            "parallel WAL commit marker at frame {wal_frame_end} has no exact durable certificate"
                        ),
                    });
                }
                if sync {
                    self.inner.wal.sync(cx, SyncFlags::NORMAL)?;
                    self.vfs.sync_parent_directory(cx, &self.wal_path)?;
                }
                return Ok(ParallelWalCommitReconciliation::Authorized);
            }

            let committed_prefix_before =
                wal_frame_start
                    .checked_sub(1)
                    .ok_or_else(|| FrankenError::WalCorrupt {
                        detail: "parallel WAL recovery interval starts at frame zero".to_owned(),
                    })?;
            if valid_frame_count != committed_prefix_before {
                return Err(FrankenError::WalCorrupt {
                    detail: format!(
                        "in-doubt WAL interval {wal_frame_start}..={wal_frame_end} has unexpected committed prefix {valid_frame_count}"
                    ),
                });
            }
            // Only after the live WAL shape is classified as the exact
            // pre-interval prefix may reconciliation repair torn sidecar bytes
            // or remove the matching orphan certificate. Unexpected WAL state
            // preserves all durable evidence for diagnosis and retry.
            self.reconcile_certificate_sidecar_record(cx, &expected_record, true, sync)
                .await?;
            self.inner.wal.repair_uncommitted_tail(cx)?;
            if sync {
                self.inner.wal.sync(cx, SyncFlags::NORMAL)?;
                self.vfs.sync_parent_directory(cx, &self.wal_path)?;
            }
            Ok(ParallelWalCommitReconciliation::NotCommitted)
        })
    }

    fn latest_authorized_parallel_wal_commit_certificate<'a>(
        &'a mut self,
        cx: &'a Cx,
    ) -> WalFuture<'a, Option<ParallelWalCommitCertificate>> {
        Box::pin(async move {
            self.ensure_current_wal_path(cx).await?;
            if let Some(record) = self
                .latest_authorized_durable_certificate_record(cx)
                .await?
            {
                return Ok(Some(record.certificate));
            }
            self.checkpoint_certificate_handoff(cx).await
        })
    }

    fn read_page<'a>(&'a mut self, cx: &'a Cx, page_number: u32) -> WalFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            self.ensure_current_wal_path(cx).await?;
            self.inner.read_page(cx, page_number).await
        })
    }

    // bd-dw8oe: gate reads must see the physical appended tail through the
    // path-refreshing wrapper too, or the guards silently regress to the
    // clamped published plane via the trait default.
    fn read_page_at_appended_tail<'a>(
        &'a mut self,
        cx: &'a Cx,
        page_number: u32,
    ) -> WalFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            self.ensure_current_wal_path(cx).await?;
            self.inner.read_page_at_appended_tail(cx, page_number).await
        })
    }

    fn read_page_pinned<'a>(
        &'a self,
        cx: &'a Cx,
        page_number: u32,
    ) -> WalFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move { self.inner.read_page_pinned(cx, page_number).await })
    }

    fn supports_pinned_reads(&self) -> bool {
        self.inner.supports_pinned_reads()
    }

    fn committed_txns_since_page<'a>(
        &'a mut self,
        cx: &'a Cx,
        page_number: u32,
    ) -> WalFuture<'a, u64> {
        Box::pin(async move {
            self.ensure_current_wal_path(cx).await?;
            self.inner.committed_txns_since_page(cx, page_number).await
        })
    }

    fn conflicting_pages_since_snapshot<'a>(
        &'a mut self,
        cx: &'a Cx,
        snapshot: TransactionConflictSnapshot,
        page_numbers: &'a [u32],
        page_baselines: &'a [TransactionConflictPageBaseline],
    ) -> WalFuture<'a, Vec<u32>> {
        Box::pin(async move {
            self.ensure_current_wal_path(cx).await?;
            let latest = self.inner.refresh_published_snapshot(cx).await?;
            if latest.generation != snapshot.generation {
                return Ok(self
                    .conflicts_after_generation_change(cx, page_numbers, page_baselines)
                    .await);
            }
            self.inner
                .conflicting_pages_since_snapshot(cx, snapshot, page_numbers, page_baselines)
                .await
        })
    }

    fn committed_txn_count<'a>(&'a mut self, cx: &'a Cx) -> WalFuture<'a, u64> {
        Box::pin(async move {
            self.ensure_current_wal_path(cx).await?;
            self.inner.committed_txn_count(cx).await
        })
    }

    fn sync(&mut self, cx: &Cx) -> Result<()> {
        #[cfg(all(feature = "native", any(unix, windows)))]
        if let Some(binding) = &self.namespace_binding {
            binding.validate_path_identity()?;
        }
        self.inner.sync(cx)
    }

    fn frame_count(&self) -> usize {
        self.inner.frame_count()
    }

    fn checkpoint<'a>(
        &'a mut self,
        cx: &'a Cx,
        mode: CheckpointMode,
        writer: &'a mut dyn CheckpointPageWriter,
        backfilled_frames: u32,
        oldest_reader_frame: Option<u32>,
    ) -> WalFuture<'a, CheckpointResult> {
        Box::pin(async move {
            self.ensure_current_wal_path(cx).await?;
            let checkpoint_handoff = self
                .latest_authorized_durable_certificate_record(cx)
                .await?;
            if let Some(record) = checkpoint_handoff.as_ref() {
                // Fence the certificate clock before the checkpoint is
                // allowed to reset the WAL generation. Replacing the handoff
                // is intentionally non-authoritative while the old WAL and
                // sidecar remain reconstructible: a crash, cancellation, or
                // write failure here aborts the checkpoint without destroying
                // the previous generation's source of truth.
                self.persist_checkpoint_certificate_handoff(cx, record)
                    .await?;
            }
            let result = self
                .inner
                .checkpoint(cx, mode, writer, backfilled_frames, oldest_reader_frame)
                .await?;
            Ok(result)
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
    fn write_page<'a>(
        &'a mut self,
        cx: &'a Cx,
        page_no: PageNumber,
        data: &'a [u8],
    ) -> CheckpointTargetFuture<'a, ()> {
        Box::pin(async move { self.writer.write_page(cx, page_no, data).await })
    }

    fn truncate_db<'a>(&'a mut self, cx: &'a Cx, n_pages: u32) -> CheckpointTargetFuture<'a, ()> {
        Box::pin(async move { self.writer.truncate(cx, n_pages).await })
    }

    fn sync_db<'a>(&'a mut self, cx: &'a Cx) -> CheckpointTargetFuture<'a, ()> {
        Box::pin(async move { self.writer.sync(cx).await })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use fsqlite_pager::MockCheckpointPageWriter;
    use fsqlite_pager::traits::WalFrameRef;
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_vfs::MemoryVfs;
    use fsqlite_vfs::traits::{Vfs, VfsFile};
    use fsqlite_wal::checksum::WalSalts;

    use super::*;

    const PAGE_SIZE: u32 = 4096;
    const CERTIFICATE_PATH: &str = "test.db-wal-cert";
    const CHECKPOINT_HANDOFF_PATH: &str = "test.db-wal-cert-head";

    #[derive(Clone, Copy, Debug)]
    enum CheckpointHandoffWriteFault {
        Error,
        Pending,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum CertificateSyncObservation {
        Ordinary(PathBuf),
        Durable(PathBuf, SyncKind),
    }

    #[derive(Debug, Default)]
    struct CheckpointHandoffFaultState {
        next_write: Option<CheckpointHandoffWriteFault>,
        fail_next_sync: bool,
        /// Fail the next sync on a non-handoff (i.e. WAL) file.
        fail_next_wal_sync: bool,
        sync_observations: Vec<CertificateSyncObservation>,
    }

    #[derive(Clone, Debug)]
    struct CheckpointHandoffFaultVfs {
        inner: MemoryVfs,
        faults: Arc<Mutex<CheckpointHandoffFaultState>>,
    }

    impl CheckpointHandoffFaultVfs {
        fn new() -> Self {
            Self {
                inner: MemoryVfs::new(),
                faults: Arc::new(Mutex::new(CheckpointHandoffFaultState::default())),
            }
        }

        fn fail_next_handoff_write(&self) {
            self.faults
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .next_write = Some(CheckpointHandoffWriteFault::Error);
        }

        fn pend_next_handoff_write(&self) {
            self.faults
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .next_write = Some(CheckpointHandoffWriteFault::Pending);
        }

        fn fail_next_handoff_sync(&self) {
            self.faults
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .fail_next_sync = true;
        }

        /// Arm a one-shot sync failure on the WAL file itself.
        fn fail_next_wal_sync(&self) {
            self.faults
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .fail_next_wal_sync = true;
        }

        fn take_sync_observations(&self) -> Vec<CertificateSyncObservation> {
            std::mem::take(
                &mut self
                    .faults
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .sync_observations,
            )
        }
    }

    #[derive(Debug)]
    struct CheckpointHandoffFaultFile {
        inner: <MemoryVfs as Vfs>::File,
        faults: Arc<Mutex<CheckpointHandoffFaultState>>,
        path: Option<PathBuf>,
        is_checkpoint_handoff: bool,
    }

    impl Vfs for CheckpointHandoffFaultVfs {
        type File = CheckpointHandoffFaultFile;

        fn name(&self) -> &'static str {
            "checkpoint-handoff-fault"
        }

        fn open(
            &self,
            cx: &Cx,
            path: Option<&Path>,
            flags: VfsOpenFlags,
        ) -> Result<(Self::File, VfsOpenFlags)> {
            let is_checkpoint_handoff =
                path.is_some_and(|candidate| candidate == Path::new(CHECKPOINT_HANDOFF_PATH));
            let (inner, actual_flags) = self.inner.open(cx, path, flags)?;
            Ok((
                CheckpointHandoffFaultFile {
                    inner,
                    faults: Arc::clone(&self.faults),
                    path: path.map(Path::to_path_buf),
                    is_checkpoint_handoff,
                },
                actual_flags,
            ))
        }

        fn delete(&self, cx: &Cx, path: &Path, sync_dir: bool) -> Result<()> {
            self.inner.delete(cx, path, sync_dir)
        }

        fn sync_parent_directory(&self, cx: &Cx, path: &Path) -> Result<()> {
            self.inner.sync_parent_directory(cx, path)
        }

        fn access(&self, cx: &Cx, path: &Path, flags: AccessFlags) -> Result<bool> {
            self.inner.access(cx, path, flags)
        }

        fn path_entry_exists(&self, cx: &Cx, path: &Path) -> Result<bool> {
            self.inner.path_entry_exists(cx, path)
        }

        fn full_pathname(&self, cx: &Cx, path: &Path) -> Result<PathBuf> {
            self.inner.full_pathname(cx, path)
        }

        fn randomness(&self, cx: &Cx, buf: &mut [u8]) {
            self.inner.randomness(cx, buf);
        }

        fn current_time(&self, cx: &Cx) -> f64 {
            self.inner.current_time(cx)
        }

        fn is_memory(&self) -> bool {
            true
        }
    }

    impl VfsFile for CheckpointHandoffFaultFile {
        fn close(&mut self, cx: &Cx) -> Result<()> {
            self.inner.close(cx)
        }

        fn file_identity(&self) -> Result<Option<fsqlite_vfs::FileIdentity>> {
            self.inner.file_identity()
        }

        fn read<'a>(
            &'a self,
            cx: &'a Cx,
            buf: &'a mut [u8],
            offset: u64,
        ) -> impl std::future::Future<Output = Result<usize>> + Send + 'a {
            self.inner.read(cx, buf, offset)
        }

        async fn write<'a>(&'a self, cx: &'a Cx, buf: &'a [u8], offset: u64) -> Result<()> {
            let fault = if self.is_checkpoint_handoff {
                self.faults
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .next_write
                    .take()
            } else {
                None
            };
            match fault {
                Some(CheckpointHandoffWriteFault::Error) => Err(FrankenError::Io(
                    std::io::Error::other("injected checkpoint handoff write failure"),
                )),
                Some(CheckpointHandoffWriteFault::Pending) => {
                    std::future::pending::<Result<()>>().await
                }
                None => self.inner.write(cx, buf, offset).await,
            }
        }

        fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
            self.inner.truncate(cx, size)
        }

        fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
            let mut faults = self
                .faults
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(path) = self.path.as_ref().filter(|path| {
                path.as_path() == Path::new(CERTIFICATE_PATH)
                    || path.as_path() == Path::new(CHECKPOINT_HANDOFF_PATH)
            }) {
                faults
                    .sync_observations
                    .push(CertificateSyncObservation::Ordinary(path.clone()));
            }
            let fail = self.is_checkpoint_handoff && std::mem::take(&mut faults.fail_next_sync);
            let fail_wal =
                !self.is_checkpoint_handoff && std::mem::take(&mut faults.fail_next_wal_sync);
            drop(faults);
            if fail {
                Err(FrankenError::Io(std::io::Error::other(
                    "injected checkpoint handoff sync failure",
                )))
            } else if fail_wal {
                Err(FrankenError::Io(std::io::Error::other(
                    "injected WAL sync failure",
                )))
            } else {
                self.inner.sync(cx, flags)
            }
        }

        fn durable_sync(&mut self, cx: &Cx, kind: SyncKind) -> Result<()> {
            let mut faults = self
                .faults
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(path) = self.path.as_ref().filter(|path| {
                path.as_path() == Path::new(CERTIFICATE_PATH)
                    || path.as_path() == Path::new(CHECKPOINT_HANDOFF_PATH)
            }) {
                faults
                    .sync_observations
                    .push(CertificateSyncObservation::Durable(path.clone(), kind));
            }
            let fail = self.is_checkpoint_handoff && std::mem::take(&mut faults.fail_next_sync);
            drop(faults);
            if fail {
                Err(FrankenError::Io(std::io::Error::other(
                    "injected checkpoint handoff durable-sync failure",
                )))
            } else {
                self.inner.durable_sync(cx, kind)
            }
        }

        fn file_size(&self, cx: &Cx) -> Result<u64> {
            self.inner.file_size(cx)
        }

        fn lock(&mut self, cx: &Cx, level: fsqlite_types::LockLevel) -> Result<()> {
            self.inner.lock(cx, level)
        }

        fn unlock(&mut self, cx: &Cx, level: fsqlite_types::LockLevel) -> Result<()> {
            self.inner.unlock(cx, level)
        }

        fn lock_external_shared_snapshot(&mut self, cx: &Cx) -> Result<()> {
            self.inner.lock_external_shared_snapshot(cx)
        }

        fn restore_external_shared_snapshot_attempt(&mut self, cx: &Cx) -> Result<()> {
            self.inner.restore_external_shared_snapshot_attempt(cx)
        }

        fn lock_external_maintenance(&mut self, cx: &Cx, wal_mode: bool) -> Result<()> {
            self.inner.lock_external_maintenance(cx, wal_mode)
        }

        fn restore_external_maintenance_attempt(&mut self, cx: &Cx) -> Result<()> {
            self.inner.restore_external_maintenance_attempt(cx)
        }

        fn check_reserved_lock(&self, cx: &Cx) -> Result<bool> {
            self.inner.check_reserved_lock(cx)
        }

        fn sector_size(&self) -> u32 {
            self.inner.sector_size()
        }

        fn device_characteristics(&self) -> u32 {
            self.inner.device_characteristics()
        }

        fn shm_map(
            &mut self,
            cx: &Cx,
            region: u32,
            size: u32,
            extend: bool,
        ) -> Result<fsqlite_vfs::ShmRegion> {
            self.inner.shm_map(cx, region, size, extend)
        }

        fn shm_lock(&mut self, cx: &Cx, offset: u32, n: u32, flags: u32) -> Result<()> {
            self.inner.shm_lock(cx, offset, n, flags)
        }

        fn shm_barrier(&self) {
            self.inner.shm_barrier();
        }

        fn shm_unmap(&mut self, cx: &Cx, delete: bool) -> Result<()> {
            self.inner.shm_unmap(cx, delete)
        }

        fn set_busy_timeout_ms(&mut self, ms: u64) {
            self.inner.set_busy_timeout_ms(ms);
        }
    }

    /// Deliberate no-op (frankensqlite#299).
    ///
    /// This helper previously installed a process-global `TRACE` subscriber via
    /// `tracing_subscriber::fmt()...with_test_writer().try_init()`. `try_init()`
    /// is process-wide and first-caller-wins, so the first of the 9 callers
    /// changed tracing enablement — and libtest output capture — for every
    /// unrelated test running afterwards in this binary, making a later failure
    /// replay the whole captured global trace stream.
    ///
    /// `fsqlite-core` already fixed the identical pattern in b262b6a6 for its
    /// other helpers; this one was missed. No caller here asserts on emitted
    /// trace events, so the body is simply removed and the call sites are kept
    /// so the diff stays test-only.
    ///
    /// See `wal_publication_tracing_helper_installs_no_global_subscriber`.
    fn init_wal_publication_test_tracing() {}

    /// frankensqlite#299 regression: the WAL publication tracing helper must not
    /// install, or otherwise disturb, a process-global subscriber.
    ///
    /// Only the equality assertion is made, deliberately. Unlike the pager
    /// crate, this test binary contains another global-subscriber installation
    /// site outside this file, so an absolute `!has_been_set()` assertion would
    /// be order-dependent and could fail for reasons unrelated to this helper.
    /// Comparing dispatcher state across the call is untaintable and proves the
    /// exact property under test: that this helper is inert.
    #[test]
    fn wal_publication_tracing_helper_installs_no_global_subscriber() {
        let before = tracing::dispatcher::has_been_set();
        init_wal_publication_test_tracing();

        assert_eq!(
            before,
            tracing::dispatcher::has_been_set(),
            "init_wal_publication_test_tracing must not install or alter a global subscriber"
        );
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

    fn test_frame_payload_digest(
        page_number: u32,
        page_data: &[u8],
        db_size_if_commit: u32,
    ) -> [u8; 32] {
        let mut digest = ParallelWalFramePayloadDigestBuilder::new();
        digest.update(
            PageNumber::new(page_number).expect("test page number must be valid"),
            db_size_if_commit,
            page_data,
        );
        digest.finalize()
    }

    fn sample_certificate(
        certificate_epoch: u64,
        commit_seq: u64,
        lane_record_counts: Vec<u32>,
    ) -> ParallelWalCommitCertificate {
        let lane_count = u16::try_from(lane_record_counts.len()).expect("test lane count fits u16");
        let mut certificate = ParallelWalCommitCertificate {
            format_version: fsqlite_wal::PARALLEL_WAL_COMMIT_CERTIFICATE_VERSION,
            residue: fsqlite_wal::ParallelWalOrderedResidue::CommitCertificateThenPublish,
            certificate_epoch,
            commit_seq_lo: fsqlite_types::CommitSeq::new(commit_seq),
            commit_seq_hi: fsqlite_types::CommitSeq::new(commit_seq),
            durable_segment_epoch: certificate_epoch,
            lane_count,
            lane_record_counts,
            db_size_pages: 1,
            page_set_size: 1,
            wal_frame_payload_digest: [0xA5; 32],
            certificate_crc32c: 0,
            fallback_active: false,
        };
        certificate.certificate_crc32c = certificate.computed_crc32c();
        certificate
    }

    fn make_path_refreshing_backend(
        vfs: &MemoryVfs,
        cx: &Cx,
    ) -> PathRefreshingWalBackend<MemoryVfs> {
        let wal = WalFile::create(cx, open_wal_file(vfs, cx), PAGE_SIZE, 0, test_salts())
            .expect("create WAL");
        PathRefreshingWalBackend::new(
            vfs.clone(),
            std::path::Path::new("test.db"),
            std::path::Path::new("test.db-wal"),
            PAGE_SIZE,
            wal,
            true,
            #[cfg(all(feature = "native", any(unix, windows)))]
            None,
        )
    }

    fn make_authorized_certificate_backend(
        vfs: &MemoryVfs,
        cx: &Cx,
    ) -> (
        PathRefreshingWalBackend<MemoryVfs>,
        ParallelWalCommitCertificate,
    ) {
        let mut backend = make_path_refreshing_backend(vfs, cx);
        let committed_page = sample_page(0x44);
        let mut certificate = sample_certificate(1, 1, vec![1]);
        certificate.wal_frame_payload_digest = test_frame_payload_digest(1, &committed_page, 1);
        certificate.certificate_crc32c = certificate.computed_crc32c();
        backend
            .persist_parallel_wal_commit_certificate(cx, &certificate, 1, 1, true)
            .expect("persist authorized certificate");
        backend
            .append_frame(cx, 1, &committed_page, 1)
            .expect("append matching commit marker");
        backend.sync(cx).expect("sync matching commit marker");
        (backend, certificate)
    }

    struct AuthoritativeWalSnapshot {
        generation: WalGenerationIdentity,
        frame_count: usize,
        wal_bytes: Vec<u8>,
        certificate: ParallelWalCommitCertificate,
        committed_page: Vec<u8>,
    }

    fn make_checkpoint_handoff_fault_backend(
        vfs: &CheckpointHandoffFaultVfs,
        cx: &Cx,
    ) -> (
        PathRefreshingWalBackend<CheckpointHandoffFaultVfs>,
        ParallelWalCommitCertificate,
        Vec<u8>,
    ) {
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let (file, _) = vfs
            .open(cx, Some(Path::new("test.db-wal")), flags)
            .expect("open fault-injected WAL file");
        let wal = WalFile::create(cx, file, PAGE_SIZE, 0, test_salts())
            .expect("create fault-injected WAL");
        let mut backend = PathRefreshingWalBackend::new(
            vfs.clone(),
            Path::new("test.db"),
            Path::new("test.db-wal"),
            PAGE_SIZE,
            wal,
            true,
            #[cfg(all(feature = "native", any(unix, windows)))]
            None,
        );
        let committed_page = sample_page(0x47);
        let mut certificate = sample_certificate(1, 1, vec![1]);
        certificate.wal_frame_payload_digest = test_frame_payload_digest(1, &committed_page, 1);
        certificate.certificate_crc32c = certificate.computed_crc32c();
        backend
            .persist_parallel_wal_commit_certificate(cx, &certificate, 1, 1, true)
            .expect("persist authorized certificate");
        backend
            .append_frame(cx, 1, &committed_page, 1)
            .expect("append matching commit marker");
        backend.sync(cx).expect("sync matching commit marker");
        (backend, certificate, committed_page)
    }

    fn read_fault_injected_wal(vfs: &CheckpointHandoffFaultVfs, cx: &Cx) -> Vec<u8> {
        let flags = VfsOpenFlags::READONLY | VfsOpenFlags::WAL;
        let (mut file, _) = vfs
            .open(cx, Some(Path::new("test.db-wal")), flags)
            .expect("open WAL snapshot");
        let len = usize::try_from(file.file_size(cx).expect("read WAL size"))
            .expect("WAL size fits usize");
        let mut bytes = vec![0_u8; len];
        assert_eq!(
            file.read(cx, &mut bytes, 0).expect("read WAL snapshot"),
            len
        );
        file.close(cx).expect("close WAL snapshot");
        bytes
    }

    fn capture_authoritative_wal(
        backend: &PathRefreshingWalBackend<CheckpointHandoffFaultVfs>,
        vfs: &CheckpointHandoffFaultVfs,
        cx: &Cx,
        certificate: ParallelWalCommitCertificate,
        committed_page: Vec<u8>,
    ) -> AuthoritativeWalSnapshot {
        AuthoritativeWalSnapshot {
            generation: backend.inner.inner().generation_identity(),
            frame_count: backend.inner.frame_count(),
            wal_bytes: read_fault_injected_wal(vfs, cx),
            certificate,
            committed_page,
        }
    }

    fn assert_authoritative_wal_unchanged(
        backend: &mut PathRefreshingWalBackend<CheckpointHandoffFaultVfs>,
        vfs: &CheckpointHandoffFaultVfs,
        cx: &Cx,
        before: &AuthoritativeWalSnapshot,
    ) {
        assert_eq!(
            backend.inner.inner().generation_identity(),
            before.generation,
            "checkpoint handoff failure must not reset the WAL generation"
        );
        assert_eq!(
            backend.inner.frame_count(),
            before.frame_count,
            "checkpoint handoff failure must not change the visible frame count"
        );
        assert_eq!(
            read_fault_injected_wal(vfs, cx),
            before.wal_bytes,
            "checkpoint handoff failure must leave the authoritative WAL byte-for-byte unchanged"
        );
        assert!(
            backend
                .inner
                .inner()
                .read_frame_header(cx, 0)
                .expect("read original commit frame")
                .is_commit(),
            "the original generation's commit marker must remain authoritative"
        );
        assert_eq!(
            backend
                .latest_authorized_parallel_wal_commit_certificate(cx)
                .expect("recover certificate from unchanged WAL generation"),
            Some(before.certificate.clone())
        );
        assert_eq!(
            backend
                .read_page(cx, 1)
                .expect("read committed page from unchanged WAL generation"),
            Some(before.committed_page.clone())
        );
    }

    fn read_certificate_sidecar(vfs: &MemoryVfs, cx: &Cx) -> Vec<u8> {
        let path = std::path::Path::new("test.db-wal-cert");
        let (mut file, _) = vfs
            .open(cx, Some(path), VfsOpenFlags::READONLY | VfsOpenFlags::WAL)
            .expect("open certificate sidecar");
        let len = usize::try_from(file.file_size(cx).expect("read certificate sidecar size"))
            .expect("certificate sidecar size fits usize");
        let mut bytes = vec![0_u8; len];
        assert_eq!(
            file.read(cx, &mut bytes, 0)
                .expect("read certificate sidecar"),
            len
        );
        file.close(cx).expect("close certificate sidecar");
        bytes
    }

    fn replace_certificate_sidecar(vfs: &MemoryVfs, cx: &Cx, bytes: &[u8]) {
        let path = std::path::Path::new("test.db-wal-cert");
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let (mut file, _) = vfs
            .open(cx, Some(path), flags)
            .expect("open mutable certificate sidecar");
        file.truncate(cx, 0)
            .expect("truncate mutable certificate sidecar");
        file.write(cx, bytes, 0)
            .expect("replace certificate sidecar bytes");
        file.close(cx).expect("close mutable certificate sidecar");
    }

    fn assert_wal_corrupt<T: std::fmt::Debug>(result: Result<T>, scenario: &str) {
        assert!(
            matches!(&result, Err(FrankenError::WalCorrupt { .. })),
            "{scenario} must fail closed with WalCorrupt, got {result:?}"
        );
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
            .expect("sync drained the staged frames")
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
        // Durable-certificate contract: staged frames are unpublished until
        // sync; pin the read snapshot AFTER publication so the fixture pins
        // the original generation's committed horizon as intended.
        backend.sync(cx).expect("publish original commit");
        backend
            .begin_transaction(cx)
            .expect("pin original WAL generation");
        let pinned = backend.pinned_read_snapshot().expect("pinned WAL snapshot");
        let snapshot = TransactionConflictSnapshot {
            generation: pinned.generation,
            last_commit_frame: pinned.last_commit_frame,
            commit_count: pinned.commit_count,
            snapshot_db_size: 0,
        };
        replace_path_visible_wal(vfs, cx);
        (backend, snapshot, page_two)
    }

    #[test]
    fn durable_certificate_sidecar_precedes_and_reconstructs_wal_commit() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let committed_page = sample_page(0x44);
        let file = open_wal_file(&vfs, &cx);
        let wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");
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
        let mut certificate = ParallelWalCommitCertificate {
            format_version: fsqlite_wal::PARALLEL_WAL_COMMIT_CERTIFICATE_VERSION,
            residue: fsqlite_wal::ParallelWalOrderedResidue::CommitCertificateThenPublish,
            certificate_epoch: 1,
            commit_seq_lo: fsqlite_types::CommitSeq::new(1),
            commit_seq_hi: fsqlite_types::CommitSeq::new(1),
            durable_segment_epoch: 1,
            lane_count: 1,
            lane_record_counts: vec![1],
            db_size_pages: 1,
            page_set_size: 1,
            wal_frame_payload_digest: test_frame_payload_digest(1, &committed_page, 1),
            certificate_crc32c: 0,
            fallback_active: false,
        };
        certificate.certificate_crc32c = certificate.computed_crc32c();

        backend
            .persist_parallel_wal_commit_certificate(&cx, &certificate, 1, 1, true)
            .expect("persist certificate before WAL commit marker");
        assert_eq!(
            backend.inner.frame_count(),
            0,
            "certificate persistence must not itself expose a WAL commit marker"
        );

        let certificate_path = std::path::Path::new("test.db-wal-cert");
        let (mut certificate_file, _) = vfs
            .open(
                &cx,
                Some(certificate_path),
                VfsOpenFlags::READONLY | VfsOpenFlags::WAL,
            )
            .expect("open certificate sidecar");
        let certificate_len = usize::try_from(
            certificate_file
                .file_size(&cx)
                .expect("certificate sidecar size"),
        )
        .expect("certificate sidecar size fits usize");
        let mut record_bytes = vec![0_u8; certificate_len];
        assert_eq!(
            certificate_file
                .read(&cx, &mut record_bytes, 0)
                .expect("read certificate sidecar"),
            certificate_len
        );
        certificate_file
            .close(&cx)
            .expect("close certificate sidecar");
        let reconstructed = ParallelWalDurableCertificateRecord::from_bytes(&record_bytes)
            .expect("reconstruct durable certificate record");
        assert_eq!(reconstructed.certificate, certificate);
        assert_eq!(reconstructed.wal_frame_start, 1);
        assert_eq!(reconstructed.wal_frame_end, 1);
        assert_eq!(
            reconstructed.wal_generation,
            backend.inner.inner().generation_identity()
        );
        assert!(
            !reconstructed.authorizes_wal_boundary(
                backend.inner.inner().generation_identity(),
                0,
                0,
                test_frame_payload_digest(1, &committed_page, 1),
            ),
            "orphan certificate must not authorize visibility before the matching commit marker"
        );

        backend
            .append_frame(&cx, 1, &committed_page, 1)
            .expect("append matching WAL commit marker");
        backend.sync(&cx).expect("sync WAL commit marker");
        assert!(
            backend
                .inner
                .inner()
                .read_frame_header(&cx, 0)
                .expect("read matching WAL commit frame")
                .is_commit()
        );
        assert!(reconstructed.authorizes_wal_boundary(
            backend.inner.inner().generation_identity(),
            1,
            1,
            test_frame_payload_digest(1, &committed_page, 1),
        ));

        let (mut certificate_file, _) = vfs
            .open(
                &cx,
                Some(certificate_path),
                VfsOpenFlags::READWRITE | VfsOpenFlags::WAL,
            )
            .expect("reopen certificate sidecar");
        let torn_offset = certificate_file
            .file_size(&cx)
            .expect("certificate sidecar size before torn tail");
        certificate_file
            .write(&cx, &[0xA5], torn_offset)
            .expect("append torn footer byte");
        certificate_file
            .close(&cx)
            .expect("close sidecar with torn tail");
        let recovered = backend
            .latest_authorized_parallel_wal_commit_certificate(&cx)
            .wait()
            .expect("torn certificate tail should recover the prior valid record")
            .expect("prior authorized certificate should remain discoverable");
        assert_eq!(recovered, certificate);
    }

    #[test]
    fn content_mismatched_wal_interval_cannot_be_authorized_or_repaired() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let certified_page = sample_page(0x61);
        let actual_page = sample_page(0x62);
        let mut backend = make_path_refreshing_backend(&vfs, &cx);
        let mut certificate = sample_certificate(1, 1, vec![1]);
        certificate.wal_frame_payload_digest = test_frame_payload_digest(1, &certified_page, 1);
        certificate.certificate_crc32c = certificate.computed_crc32c();

        backend
            .persist_parallel_wal_commit_certificate(&cx, &certificate, 1, 1, true)
            .expect("persist content-bound certificate");
        backend
            .append_frame(&cx, 1, &actual_page, 1)
            .expect("append differently valued commit frame");
        backend.sync(&cx).expect("sync mismatched commit frame");

        let sidecar_before = read_certificate_sidecar(&vfs, &cx);
        assert!(
            backend
                .latest_authorized_parallel_wal_commit_certificate(&cx)
                .wait()
                .expect("content mismatch is a non-authorizing record")
                .is_none(),
            "matching generation and commit marker must not authorize different frame bytes"
        );

        assert_wal_corrupt(
            backend
                .reconcile_parallel_wal_commit(&cx, &certificate, 1, 1, true)
                .wait(),
            "in-doubt content-bound reconciliation mismatch",
        );
        assert_eq!(
            read_certificate_sidecar(&vfs, &cx),
            sidecar_before,
            "digest mismatch must be diagnosed before sidecar repair"
        );
        assert_eq!(
            backend.inner.frame_count(),
            1,
            "digest mismatch must preserve the live WAL for diagnosis and retry"
        );
    }

    #[test]
    fn absent_commit_marker_repairs_certificate_and_partial_wal_tail() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut backend = make_path_refreshing_backend(&vfs, &cx);
        let certificate = sample_certificate(1, 1, vec![1]);
        backend
            .persist_parallel_wal_commit_certificate(&cx, &certificate, 1, 1, true)
            .expect("persist orphan certificate");

        let (mut tail_writer, _) = vfs
            .open(
                &cx,
                Some(std::path::Path::new("test.db-wal")),
                VfsOpenFlags::READWRITE | VfsOpenFlags::WAL,
            )
            .expect("open WAL for partial-tail injection");
        let committed_size = tail_writer.file_size(&cx).expect("read committed WAL size");
        tail_writer
            .write(&cx, &[0xA5; 7], committed_size)
            .expect("inject a partial physical frame");
        assert!(
            tail_writer.file_size(&cx).expect("read extended WAL size") > committed_size,
            "fault fixture must extend the physical WAL"
        );
        tail_writer.close(&cx).expect("close partial-tail injector");

        assert_eq!(
            backend
                .reconcile_parallel_wal_commit(&cx, &certificate, 1, 1, true)
                .wait()
                .expect("missing commit marker must be exactly repairable"),
            ParallelWalCommitReconciliation::NotCommitted
        );
        assert!(
            read_certificate_sidecar(&vfs, &cx).is_empty(),
            "matching orphan certificate must be removed after NotCommitted proof"
        );
        let (mut repaired_wal, _) = vfs
            .open(
                &cx,
                Some(std::path::Path::new("test.db-wal")),
                VfsOpenFlags::READONLY | VfsOpenFlags::WAL,
            )
            .expect("open repaired WAL");
        assert_eq!(
            repaired_wal.file_size(&cx).expect("read repaired WAL size"),
            committed_size,
            "NotCommitted reconciliation must truncate the physical partial tail"
        );
        repaired_wal.close(&cx).expect("close repaired WAL");
    }

    #[test]
    fn durable_certificate_recovery_accepts_every_truncated_record_prefix() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let (mut backend, authorized) = make_authorized_certificate_backend(&vfs, &cx);
        let authorized_bytes = read_certificate_sidecar(&vfs, &cx);
        let orphan = sample_certificate(2, 2, vec![1]);
        let orphan_bytes = ParallelWalDurableCertificateRecord::new(
            backend.inner.inner().generation_identity(),
            2,
            2,
            orphan,
        )
        .expect("construct orphan record")
        .to_bytes();

        for prefix_len in 1..orphan_bytes.len() {
            let mut sidecar = authorized_bytes.clone();
            sidecar.extend_from_slice(&orphan_bytes[..prefix_len]);
            replace_certificate_sidecar(&vfs, &cx, &sidecar);
            let recovered_result = backend
                .latest_authorized_parallel_wal_commit_certificate(&cx)
                .wait();
            assert!(
                recovered_result.is_ok(),
                "truncated certificate prefix of {prefix_len} bytes must recover: {recovered_result:?}"
            );
            let recovered = recovered_result
                .expect("truncated certificate recovery was asserted successful")
                .expect("authorized record must remain discoverable");
            assert_eq!(recovered, authorized, "failed at prefix {prefix_len}");
        }
    }

    #[test]
    fn durable_certificate_append_repairs_the_accepted_torn_suffix() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let (mut backend, authorized) = make_authorized_certificate_backend(&vfs, &cx);
        let authorized_bytes = read_certificate_sidecar(&vfs, &cx);
        let orphan = sample_certificate(2, 2, vec![1]);
        let orphan_bytes = ParallelWalDurableCertificateRecord::new(
            backend.inner.inner().generation_identity(),
            2,
            2,
            orphan.clone(),
        )
        .expect("construct orphan record")
        .to_bytes();
        for prefix_len in 1..orphan_bytes.len() {
            let mut torn_sidecar = authorized_bytes.clone();
            torn_sidecar.extend_from_slice(&orphan_bytes[..prefix_len]);
            replace_certificate_sidecar(&vfs, &cx, &torn_sidecar);

            assert_eq!(
                backend
                    .latest_authorized_parallel_wal_commit_certificate(&cx)
                    .wait()
                    .expect("one torn suffix should recover")
                    .expect("authorized predecessor remains visible"),
                authorized,
                "read recovery failed for prefix {prefix_len}"
            );

            backend
                .persist_parallel_wal_commit_certificate(&cx, &orphan, 2, 2, true)
                .expect("next append repairs the torn suffix first");
            let repaired_sidecar = read_certificate_sidecar(&vfs, &cx);
            assert_eq!(
                repaired_sidecar.len(),
                authorized_bytes.len() + orphan_bytes.len(),
                "replacement record did not start at the prior complete boundary for prefix {prefix_len}"
            );
            assert_eq!(
                backend
                    .latest_authorized_parallel_wal_commit_certificate(&cx)
                    .wait()
                    .expect("orphan lookback crosses the repaired boundary")
                    .expect("authorized predecessor remains discoverable"),
                authorized,
                "orphan lookback failed after repairing prefix {prefix_len}"
            );
        }

        let mut corrupt_record = orphan_bytes;
        let envelope_crc_offset =
            corrupt_record.len() - ParallelWalDurableCertificateRecord::LENGTH_FOOTER_SIZE - 4;
        corrupt_record[envelope_crc_offset] ^= 0x80;
        let mut corrupt_sidecar = authorized_bytes;
        corrupt_sidecar.extend_from_slice(&corrupt_record);
        replace_certificate_sidecar(&vfs, &cx, &corrupt_sidecar);
        assert_wal_corrupt(
            backend
                .persist_parallel_wal_commit_certificate(&cx, &orphan, 2, 2, true)
                .wait(),
            "append-time complete record corruption",
        );
    }

    #[test]
    fn durable_certificate_recovery_rejects_complete_corruption_and_garbage() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let (mut backend, _) = make_authorized_certificate_backend(&vfs, &cx);
        let authorized_bytes = read_certificate_sidecar(&vfs, &cx);
        let orphan = sample_certificate(2, 2, vec![1]);
        let orphan_bytes = ParallelWalDurableCertificateRecord::new(
            backend.inner.inner().generation_identity(),
            2,
            2,
            orphan,
        )
        .expect("construct orphan record")
        .to_bytes();

        let mut bad_crc = orphan_bytes.clone();
        let envelope_crc_offset =
            bad_crc.len() - ParallelWalDurableCertificateRecord::LENGTH_FOOTER_SIZE - 4;
        bad_crc[envelope_crc_offset] ^= 0x80;
        let mut sidecar = authorized_bytes.clone();
        sidecar.extend_from_slice(&bad_crc);
        replace_certificate_sidecar(&vfs, &cx, &sidecar);
        assert_wal_corrupt(
            backend
                .latest_authorized_parallel_wal_commit_certificate(&cx)
                .wait(),
            "complete record with bad CRC",
        );

        let mut bad_version = orphan_bytes.clone();
        bad_version[8] ^= 0x01;
        let mut sidecar = authorized_bytes.clone();
        sidecar.extend_from_slice(&bad_version);
        replace_certificate_sidecar(&vfs, &cx, &sidecar);
        assert_wal_corrupt(
            backend
                .latest_authorized_parallel_wal_commit_certificate(&cx)
                .wait(),
            "complete record with bad version",
        );

        let mut bad_magic = orphan_bytes.clone();
        bad_magic[0] ^= 0x01;
        let mut sidecar = authorized_bytes.clone();
        sidecar.extend_from_slice(&bad_magic);
        replace_certificate_sidecar(&vfs, &cx, &sidecar);
        assert_wal_corrupt(
            backend
                .latest_authorized_parallel_wal_commit_certificate(&cx)
                .wait(),
            "complete record with bad magic",
        );

        let mut bad_footer = orphan_bytes;
        let footer_offset =
            bad_footer.len() - ParallelWalDurableCertificateRecord::LENGTH_FOOTER_SIZE;
        bad_footer[footer_offset] ^= 0x80;
        let mut sidecar = authorized_bytes;
        sidecar.extend_from_slice(&bad_footer);
        replace_certificate_sidecar(&vfs, &cx, &sidecar);
        assert_wal_corrupt(
            backend
                .latest_authorized_parallel_wal_commit_certificate(&cx)
                .wait(),
            "complete record with bad footer",
        );

        let garbage_vfs = MemoryVfs::new();
        let mut garbage_backend = make_path_refreshing_backend(&garbage_vfs, &cx);
        replace_certificate_sidecar(&garbage_vfs, &cx, &[0xA5; 128]);
        assert_wal_corrupt(
            garbage_backend
                .latest_authorized_parallel_wal_commit_certificate(&cx)
                .wait(),
            "nonempty garbage sidecar",
        );

        let mut fake_magic = vec![0_u8; MIN_DURABLE_CERTIFICATE_RECORD_SIZE];
        fake_magic[..PARALLEL_WAL_DURABLE_CERTIFICATE_MAGIC.len()]
            .copy_from_slice(&PARALLEL_WAL_DURABLE_CERTIFICATE_MAGIC);
        fake_magic[8..10].copy_from_slice(
            &fsqlite_wal::PARALLEL_WAL_DURABLE_CERTIFICATE_RECORD_VERSION.to_le_bytes(),
        );
        let fake_record_len = u32::try_from(fake_magic.len()).expect("fake record length fits u32");
        fake_magic[10..14].copy_from_slice(&fake_record_len.to_le_bytes());
        let fake_footer_offset =
            fake_magic.len() - ParallelWalDurableCertificateRecord::LENGTH_FOOTER_SIZE;
        fake_magic[fake_footer_offset..].copy_from_slice(&fake_record_len.to_le_bytes());
        replace_certificate_sidecar(&garbage_vfs, &cx, &fake_magic);
        assert_wal_corrupt(
            garbage_backend
                .latest_authorized_parallel_wal_commit_certificate(&cx)
                .wait(),
            "fake magic and length without a valid envelope",
        );
    }

    #[test]
    fn durable_certificate_maximum_size_is_shared_by_writer_and_reader() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut backend = make_path_refreshing_backend(&vfs, &cx);
        let certificate = sample_certificate(1, 1, vec![1; usize::from(u16::MAX)]);
        let record = ParallelWalDurableCertificateRecord::new(
            backend.inner.inner().generation_identity(),
            1,
            1,
            certificate.clone(),
        )
        .expect("construct maximum-size record");
        assert_eq!(
            record.to_bytes().len(),
            PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE
        );
        backend
            .persist_parallel_wal_commit_certificate(&cx, &certificate, 1, 1, true)
            .expect("writer accepts maximum-size record");
        assert!(
            backend
                .latest_authorized_parallel_wal_commit_certificate(&cx)
                .wait()
                .expect("reader accepts maximum-size record")
                .is_none(),
            "record remains unauthorized until its WAL commit marker exists"
        );
    }

    #[test]
    fn durable_certificate_orphan_lookback_allows_exact_boundary_plus_torn_tail() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let (mut backend, authorized) = make_authorized_certificate_backend(&vfs, &cx);
        let mut sidecar = read_certificate_sidecar(&vfs, &cx);
        // bd-e0ghc contract: only IN-SNAPSHOT invalid records consume the
        // bounded orphan budget; records whose frame boundary lies beyond the
        // published frame count are futures under concurrent load and are
        // skipped budget-free. Exercise the budget with in-snapshot orphans
        // (boundary 1,1 — within the published horizon — but content that
        // fails authorization against the real commit marker).
        for orphan_index in 0..MAX_ORPHAN_CERTIFICATE_LOOKBACK {
            let epoch = u64::try_from(orphan_index).expect("orphan index fits u64") + 2;
            let orphan = sample_certificate(epoch, epoch, vec![1]);
            sidecar.extend_from_slice(
                &ParallelWalDurableCertificateRecord::new(
                    backend.inner.inner().generation_identity(),
                    1,
                    1,
                    orphan,
                )
                .expect("construct bounded orphan")
                .to_bytes(),
            );
        }
        sidecar.push(0xA5);
        replace_certificate_sidecar(&vfs, &cx, &sidecar);
        assert_eq!(
            backend
                .latest_authorized_parallel_wal_commit_certificate(&cx)
                .wait()
                .expect("64 orphans plus one torn suffix remain within bound")
                .expect("authorized predecessor is found"),
            authorized
        );

        sidecar.pop();
        let overflow_epoch =
            u64::try_from(MAX_ORPHAN_CERTIFICATE_LOOKBACK).expect("lookback fits u64") + 2;
        let overflow = sample_certificate(overflow_epoch, overflow_epoch, vec![1]);
        sidecar.extend_from_slice(
            &ParallelWalDurableCertificateRecord::new(
                backend.inner.inner().generation_identity(),
                1,
                1,
                overflow,
            )
            .expect("construct overflow orphan")
            .to_bytes(),
        );
        replace_certificate_sidecar(&vfs, &cx, &sidecar);
        assert_wal_corrupt(
            backend
                .latest_authorized_parallel_wal_commit_certificate(&cx)
                .wait(),
            "65 unauthorized records",
        );

        // Contract-positive twin: FUTURE-boundary records (beyond the
        // published frame count) are budget-exempt — 65 of them plus the
        // torn tail must still resolve the authorized predecessor.
        let mut future_sidecar = read_certificate_sidecar(&vfs, &cx);
        future_sidecar.truncate(
            future_sidecar.len()
                - (MAX_ORPHAN_CERTIFICATE_LOOKBACK + 1)
                    * ParallelWalDurableCertificateRecord::new(
                        backend.inner.inner().generation_identity(),
                        1,
                        1,
                        sample_certificate(2, 2, vec![1]),
                    )
                    .expect("sizing record")
                    .to_bytes()
                    .len(),
        );
        for future_index in 0..=MAX_ORPHAN_CERTIFICATE_LOOKBACK {
            let epoch = u64::try_from(future_index).expect("future index fits u64") + 2;
            let future = sample_certificate(epoch, epoch, vec![1]);
            future_sidecar.extend_from_slice(
                &ParallelWalDurableCertificateRecord::new(
                    backend.inner.inner().generation_identity(),
                    2,
                    2,
                    future,
                )
                .expect("construct future record")
                .to_bytes(),
            );
        }
        future_sidecar.push(0xA5);
        replace_certificate_sidecar(&vfs, &cx, &future_sidecar);
        assert_eq!(
            backend
                .latest_authorized_parallel_wal_commit_certificate(&cx)
                .wait()
                .expect("future-boundary records are budget-exempt")
                .expect("authorized predecessor is found beneath futures"),
            authorized
        );
    }

    #[test]
    fn certificate_and_handoff_fences_request_full_durability() {
        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let (mut backend, certificate, _) = make_checkpoint_handoff_fault_backend(&vfs, &cx);

        assert_eq!(
            vfs.take_sync_observations(),
            vec![CertificateSyncObservation::Durable(
                PathBuf::from(CERTIFICATE_PATH),
                SyncKind::FullDurable,
            )],
            "certificate append must use the strongest durability intent"
        );

        assert_eq!(
            backend
                .reconcile_parallel_wal_commit(&cx, &certificate, 1, 1, true)
                .wait()
                .expect("reconcile committed certificate"),
            ParallelWalCommitReconciliation::Authorized
        );
        assert_eq!(
            vfs.take_sync_observations(),
            vec![CertificateSyncObservation::Durable(
                PathBuf::from(CERTIFICATE_PATH),
                SyncKind::FullDurable,
            )],
            "certificate reconciliation must preserve full durability intent"
        );

        let record = backend
            .latest_authorized_durable_certificate_record(&cx)
            .wait()
            .expect("read authorized certificate record")
            .expect("authorized certificate record must exist");
        backend
            .persist_checkpoint_certificate_handoff(&cx, &record)
            .wait()
            .expect("persist checkpoint certificate handoff");
        assert_eq!(
            vfs.take_sync_observations(),
            vec![CertificateSyncObservation::Durable(
                PathBuf::from(CHECKPOINT_HANDOFF_PATH),
                SyncKind::FullDurable,
            )],
            "checkpoint handoff must use the strongest durability intent"
        );
    }

    #[test]
    fn checkpoint_handoff_write_failure_preserves_authoritative_wal_generation() {
        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let (mut backend, certificate, committed_page) =
            make_checkpoint_handoff_fault_backend(&vfs, &cx);
        let before = capture_authoritative_wal(&backend, &vfs, &cx, certificate, committed_page);
        vfs.fail_next_handoff_write();

        let mut checkpoint_writer = MockCheckpointPageWriter;
        let error = backend
            .checkpoint(
                &cx,
                CheckpointMode::Truncate,
                &mut checkpoint_writer,
                0,
                None,
            )
            .expect_err("checkpoint must fail before reset when the handoff write fails");
        assert!(
            error
                .to_string()
                .contains("injected checkpoint handoff write failure"),
            "unexpected handoff write error: {error}"
        );
        assert_authoritative_wal_unchanged(&mut backend, &vfs, &cx, &before);
    }

    #[test]
    fn checkpoint_handoff_durable_sync_failure_preserves_authoritative_wal_generation() {
        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let (mut backend, certificate, committed_page) =
            make_checkpoint_handoff_fault_backend(&vfs, &cx);
        let before = capture_authoritative_wal(&backend, &vfs, &cx, certificate, committed_page);
        vfs.fail_next_handoff_sync();

        let mut checkpoint_writer = MockCheckpointPageWriter;
        let error = backend
            .checkpoint(
                &cx,
                CheckpointMode::Truncate,
                &mut checkpoint_writer,
                0,
                None,
            )
            .expect_err("checkpoint must fail before reset when the handoff sync fails");
        assert!(
            error
                .to_string()
                .contains("injected checkpoint handoff durable-sync failure"),
            "unexpected handoff durable-sync error: {error}"
        );
        assert_authoritative_wal_unchanged(&mut backend, &vfs, &cx, &before);
    }

    #[test]
    fn dropping_pending_checkpoint_handoff_write_preserves_authoritative_wal_generation() {
        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let (mut backend, certificate, committed_page) =
            make_checkpoint_handoff_fault_backend(&vfs, &cx);
        let before = capture_authoritative_wal(&backend, &vfs, &cx, certificate, committed_page);
        vfs.pend_next_handoff_write();

        let mut checkpoint_writer = MockCheckpointPageWriter;
        let reached_pending_handoff = {
            let mut checkpoint = backend.checkpoint(
                &cx,
                CheckpointMode::Truncate,
                &mut checkpoint_writer,
                0,
                None,
            );
            let mut task_cx = std::task::Context::from_waker(std::task::Waker::noop());
            matches!(
                std::future::Future::poll(checkpoint.as_mut(), &mut task_cx),
                std::task::Poll::Pending
            )
        };
        assert!(
            reached_pending_handoff,
            "checkpoint should remain pending inside the injected handoff write"
        );
        assert_authoritative_wal_unchanged(&mut backend, &vfs, &cx, &before);
    }

    #[test]
    fn two_backend_instances_continue_authorized_certificate_clocks() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let wal = WalFile::create(&cx, open_wal_file(&vfs, &cx), PAGE_SIZE, 0, test_salts())
            .expect("create shared WAL");
        let mut first_backend = PathRefreshingWalBackend::new(
            vfs.clone(),
            std::path::Path::new("test.db"),
            std::path::Path::new("test.db-wal"),
            PAGE_SIZE,
            wal,
            true,
            #[cfg(all(feature = "native", any(unix, windows)))]
            None,
        );
        let request =
            |batch_id, wal_frame_payload_digest| fsqlite_wal::ParallelWalDurabilityRequest {
                trace_id: batch_id,
                scenario_id: "two-instance-continuity".to_owned(),
                certificate_epoch: 0,
                durable_segment_epoch: 0,
                batch_size: 1,
                batch_ids: vec![batch_id],
                lane_record_counts: vec![1],
                db_size_pages: 1,
                page_set_size: 1,
                control_mode: fsqlite_wal::ParallelWalOperatingMode::Auto,
                fallback_reason: None,
                checkpoint_active: false,
                wal_frame_payload_digest,
            };

        let first_combiner = fsqlite_wal::ParallelWalDurabilityCombiner::default();
        let first_page = sample_page(0x51);
        let first_receipt = first_combiner
            .certify_and_publish(
                request(1, test_frame_payload_digest(1, &first_page, 1)),
                |certificate| {
                    first_backend
                        .persist_parallel_wal_commit_certificate(&cx, certificate, 1, 1, true)
                        .wait()
                        .and_then(|()| first_backend.append_frame(&cx, 1, &first_page, 1).wait())
                        .and_then(|()| first_backend.sync(&cx))
                        .map_err(|error| error.to_string())
                },
            )
            .expect("first backend publishes certificate");

        let second_wal =
            WalFile::open(&cx, open_wal_file(&vfs, &cx)).expect("second backend opens shared WAL");
        let mut second_backend = PathRefreshingWalBackend::new(
            vfs.clone(),
            std::path::Path::new("test.db"),
            std::path::Path::new("test.db-wal"),
            PAGE_SIZE,
            second_wal,
            true,
            #[cfg(all(feature = "native", any(unix, windows)))]
            None,
        );

        // Simulate a crash after certificate durability but before its WAL
        // commit marker. Bounded tail lookup must step over this well-formed
        // orphan and recover the preceding authorized seed.
        let orphan_combiner = fsqlite_wal::ParallelWalDurabilityCombiner::default();
        orphan_combiner
            .reconcile_authorized_seed(&first_receipt.certificate)
            .expect("seed orphan-producing process");
        let orphan_receipt = orphan_combiner
            .certify_and_publish(
                request(99, test_frame_payload_digest(1, &sample_page(0x52), 1)),
                |_| Ok(()),
            )
            .expect("construct deterministic orphan certificate");
        second_backend
            .persist_parallel_wal_commit_certificate(&cx, &orphan_receipt.certificate, 2, 2, true)
            .expect("persist well-formed orphan certificate tail");
        let authorized_seed = second_backend
            .latest_authorized_parallel_wal_commit_certificate(&cx)
            .expect("second backend performs bounded orphan lookback")
            .expect("preceding first certificate remains authorized");
        assert_eq!(authorized_seed, first_receipt.certificate);

        let second_combiner = fsqlite_wal::ParallelWalDurabilityCombiner::default();
        second_combiner
            .reconcile_authorized_seed(&authorized_seed)
            .expect("seed second process-local combiner");
        let second_page = sample_page(0x52);
        let second_receipt = second_combiner
            .certify_and_publish(
                request(2, test_frame_payload_digest(1, &second_page, 1)),
                |certificate| {
                    second_backend
                        .persist_parallel_wal_commit_certificate(&cx, certificate, 2, 2, true)
                        .wait()
                        .and_then(|()| second_backend.append_frame(&cx, 1, &second_page, 1).wait())
                        .and_then(|()| second_backend.sync(&cx))
                        .map_err(|error| error.to_string())
                },
            )
            .expect("second backend publishes certificate");

        assert_eq!(
            second_receipt.certificate.commit_seq_lo.get(),
            first_receipt.certificate.commit_seq_hi.get() + 1
        );
        assert_eq!(
            second_receipt.certificate.certificate_epoch,
            first_receipt.certificate.certificate_epoch + 1
        );
        assert_eq!(
            second_receipt.certificate, orphan_receipt.certificate,
            "continuation may reuse an orphan identity but must not overlap any authorized certificate"
        );
        let latest = second_backend
            .latest_authorized_parallel_wal_commit_certificate(&cx)
            .expect("read second bounded authorized tail")
            .expect("second certificate is authorized");
        assert_eq!(latest, second_receipt.certificate);

        let generation_before_checkpoint = second_backend.inner.inner().generation_identity();
        let mut checkpoint_writer = MockCheckpointPageWriter;
        let checkpoint = second_backend
            .checkpoint(
                &cx,
                CheckpointMode::Truncate,
                &mut checkpoint_writer,
                0,
                None,
            )
            .expect("truncate checkpoint records certificate clock handoff");
        assert!(checkpoint.wal_was_reset);
        assert_ne!(
            second_backend.inner.inner().generation_identity(),
            generation_before_checkpoint
        );
        let checkpoint_seed = second_backend
            .latest_authorized_parallel_wal_commit_certificate(&cx)
            .expect("read checkpoint certificate clock handoff")
            .expect("reset generation retains the last consumed certificate clock");
        assert_eq!(checkpoint_seed, second_receipt.certificate);
        second_backend
            .begin_transaction(&cx)
            .expect("pin reset-generation reader snapshot");
        let reset_pinned = second_backend
            .pinned_read_snapshot()
            .expect("reset-generation reader snapshot");
        assert_eq!(
            reset_pinned.generation,
            second_backend.inner.inner().generation_identity(),
            "reader snapshot must bind the reset WAL generation"
        );
        assert_eq!(
            reset_pinned.last_commit_frame, None,
            "truncate checkpoint leaves no current-generation commit marker"
        );
        assert_eq!(
            second_backend
                .pinned_logical_read_snapshot(&cx)
                .expect("inspect reset-generation reader horizon"),
            None,
            "an earlier-generation checkpoint handoff is a clock seed, never reader visibility"
        );

        let post_checkpoint_combiner = fsqlite_wal::ParallelWalDurabilityCombiner::default();
        post_checkpoint_combiner
            .reconcile_authorized_seed(&checkpoint_seed)
            .expect("seed fresh post-checkpoint combiner");
        let post_checkpoint_page = sample_page(0x53);
        let post_checkpoint_receipt = post_checkpoint_combiner
            .certify_and_publish(
                request(3, test_frame_payload_digest(1, &post_checkpoint_page, 1)),
                |certificate| {
                    second_backend
                        .persist_parallel_wal_commit_certificate(&cx, certificate, 1, 1, true)
                        .wait()
                        .and_then(|()| {
                            second_backend
                                .append_frame(&cx, 1, &post_checkpoint_page, 1)
                                .wait()
                        })
                        .and_then(|()| second_backend.sync(&cx))
                        .map_err(|error| error.to_string())
                },
            )
            .expect("publish first certificate in reset WAL generation");
        assert_eq!(
            post_checkpoint_receipt.certificate.commit_seq_lo.get(),
            second_receipt.certificate.commit_seq_hi.get() + 1
        );
        assert_eq!(
            post_checkpoint_receipt.certificate.certificate_epoch,
            second_receipt.certificate.certificate_epoch + 1
        );
        assert_eq!(
            second_backend
                .latest_authorized_parallel_wal_commit_certificate(&cx)
                .expect("read post-checkpoint current-generation certificate")
                .expect("post-checkpoint certificate is authorized"),
            post_checkpoint_receipt.certificate
        );
        second_backend
            .begin_transaction(&cx)
            .expect("pin post-checkpoint reader snapshot");
        let pinned = second_backend
            .pinned_read_snapshot()
            .expect("post-checkpoint reader snapshot");
        let logical = second_backend
            .pinned_logical_read_snapshot(&cx)
            .expect("inspect post-checkpoint reader horizon")
            .expect("current-generation certificate exposes a reader horizon");
        assert_eq!(logical.generation, pinned.generation);
        assert_eq!(logical.last_commit_frame, pinned.last_commit_frame);
        assert_eq!(
            logical.visible_commit_seq,
            post_checkpoint_receipt.certificate.commit_seq_hi
        );
    }

    #[test]
    fn pinned_logical_reader_horizon_counts_physical_tail_after_current_certificate() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let (mut backend, certificate) = make_authorized_certificate_backend(&vfs, &cx);

        backend
            .begin_transaction(&cx)
            .expect("pin certificate reader snapshot");
        let initial_pinned = backend
            .pinned_read_snapshot()
            .expect("initial reader snapshot");
        let initial_logical = backend
            .pinned_logical_read_snapshot(&cx)
            .expect("inspect certificate reader horizon")
            .expect("current certificate exposes reader horizon");
        assert_eq!(initial_logical.generation, initial_pinned.generation);
        assert_eq!(
            initial_logical.last_commit_frame,
            initial_pinned.last_commit_frame
        );
        assert_eq!(
            initial_logical.visible_commit_seq, certificate.commit_seq_hi,
            "certificate horizon is exact when no later physical commit exists"
        );

        let tail_page = sample_page(0x45);
        backend
            .append_frame(&cx, 2, &tail_page, 2)
            .expect("append later ordinary commit marker");
        backend
            .sync(&cx)
            .expect("sync later ordinary commit marker");
        backend
            .begin_transaction(&cx)
            .expect("repin reader after ordinary tail commit");
        let pinned = backend
            .pinned_read_snapshot()
            .expect("reader snapshot includes ordinary tail commit");
        let logical = backend
            .pinned_logical_read_snapshot(&cx)
            .expect("inspect reader horizon with ordinary tail")
            .expect("current certificate remains reader-authoritative");
        assert_eq!(logical.generation, pinned.generation);
        assert_eq!(logical.last_commit_frame, pinned.last_commit_frame);
        assert_eq!(
            logical.visible_commit_seq.get(),
            certificate.commit_seq_hi.get() + 1
        );
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

    /// Adapter backed by the fault VFS so WAL `sync` failures can be injected.
    fn make_fault_adapter(
        vfs: &CheckpointHandoffFaultVfs,
        cx: &Cx,
    ) -> WalBackendAdapter<<CheckpointHandoffFaultVfs as Vfs>::File> {
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let (file, _) = vfs
            .open(cx, Some(std::path::Path::new("test.db-wal")), flags)
            .expect("open fault WAL file");
        let wal = WalFile::create(cx, file, PAGE_SIZE, 0, test_salts()).expect("create fault WAL");
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

        // Durable-certificate contract: staged frames are unpublished until
        // sync; the backend read path serves only the published horizon.
        assert_eq!(
            adapter.read_page(&cx, 1).expect("read staged page 1"),
            None,
            "staged frames must stay invisible before publication"
        );
        adapter.sync(&cx).expect("publish staged frames");

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

        // Durable-certificate contract: publication (sync) gates visibility.
        adapter.sync(&cx).expect("publish staged frames");

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

    /// bd-smxhz regression: the per-commit FCW verification descriptor is
    /// cached across generation-change conflict checks to eliminate the
    /// per-commit open/close syscall storm. Prove the cached fd observes
    /// *live* main-database content on reuse rather than serving stale bytes
    /// from the first open — a stale cached read would wrongly clear a commit
    /// that actually conflicts with a checkpointed external write.
    #[test]
    fn test_generation_change_cached_verification_fd_reads_live_main_db_content() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let (mut backend, snapshot, page_two) = make_generation_transition_backend(&vfs, &cx);
        let baseline = TransactionConflictPageBaseline {
            page_number: 2,
            page_hash: *blake3::hash(&page_two).as_bytes(),
        };

        // First check populates the cached verification descriptor: main-db
        // page 2 still matches the baseline, so there is no conflict.
        let first = backend
            .conflicting_pages_since_snapshot(&cx, snapshot, &[2], &[baseline])
            .expect("first (cache-populating) generation-change verification");
        assert!(
            first.is_empty(),
            "identical baseline must not conflict on the first check"
        );

        // Mutate main-db page 2 through an independent handle after the fd was
        // cached. The cached descriptor shares the underlying inode/storage, so
        // reuse must observe this live write and now flag the page.
        write_main_db_pages(
            &vfs,
            &cx,
            &[
                sqlite_page_one(u16::try_from(PAGE_SIZE).expect("page size fits u16")),
                sample_page(0x33),
            ],
        );

        let second = backend
            .conflicting_pages_since_snapshot(&cx, snapshot, &[2], &[baseline])
            .expect("second generation-change verification reuses the cached fd");
        assert_eq!(
            second,
            vec![2],
            "cached verification fd must read the live changed page, not stale cached bytes"
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
        // Publish the committed frame; the tail frame appended after the
        // publication stays staged AND uncommitted.
        adapter.sync(&cx).expect("publish committed frame");
        adapter
            .append_frame(&cx, 7, &uncommitted, 0)
            .expect("append uncommitted frame");

        let result = adapter.read_page(&cx, 7).expect("read committed page");
        assert_eq!(
            result,
            Some(committed),
            "reader must ignore uncommitted (and unpublished) tail frames"
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
    fn test_adapter_into_inner_fails_closed_until_sync() {
        let cx = test_cx();
        let staged_vfs = MemoryVfs::new();
        let mut staged = make_adapter(&staged_vfs, &cx);

        staged
            .append_frame(&cx, 1, &sample_page(0), 1)
            .expect("append");
        assert!(
            matches!(staged.into_inner(), Err(FrankenError::Busy)),
            "an unsynced commit must prevent consuming the adapter"
        );

        let synced_vfs = MemoryVfs::new();
        let mut synced = make_adapter(&synced_vfs, &cx);
        synced
            .append_frame(&cx, 1, &sample_page(0), 1)
            .expect("append");
        synced.sync(&cx).expect("sync staged commit");

        assert_eq!(synced.inner().frame_count(), 1);

        let wal = synced.into_inner().expect("sync drained the staged frames");
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

        // Durable-certificate contract: publication gates dyn reads too.
        backend.sync(&cx).expect("publish via dyn");
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
        adapter.sync(&cx).expect("publish staged frames");

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
        adapter.sync(&cx).expect("publish staged frames");

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
        adapter.sync(&cx).expect("publish staged frames");

        // Read page 1 to populate the index.
        assert_eq!(adapter.read_page(&cx, 1).expect("read old"), Some(old_data));

        // Reset WAL with new salts (simulates checkpoint reset).
        let new_salts = WalSalts {
            salt1: 0xAAAA_BBBB,
            salt2: 0xCCCC_DDDD,
        };
        adapter
            .inner_mut()
            .expect("no staged batch blocks inner access")
            .reset(&cx, 1, new_salts, false)
            .expect("WAL reset");

        // Write new data for the same page number in the new generation.
        let new_data = sample_page(0x22);
        adapter
            .append_frame(&cx, 1, &new_data, 1)
            .expect("append new generation commit");
        adapter.sync(&cx).expect("publish new generation commit");

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
        adapter.sync(&cx).expect("publish staged frames");
        assert_eq!(adapter.read_page(&cx, 1).expect("read old"), Some(old_data));

        adapter
            .inner_mut()
            .expect("no staged batch blocks inner access")
            .reset(&cx, 1, reused_salts, false)
            .expect("reset with same salts");
        let new_data = sample_page(0x22);
        adapter
            .append_frame(&cx, 2, &new_data, 2)
            .expect("append new generation commit");
        adapter.sync(&cx).expect("publish new generation commit");
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
    fn test_page_index_incremental_extend_after_durable_sync() {
        // Verify that the index extends incrementally once each commit crosses
        // the durable-sync publication barrier.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let page1 = sample_page(0x10);
        adapter
            .append_frame(&cx, 1, &page1, 1)
            .expect("append commit 1");
        adapter.sync(&cx).expect("durably publish commit 1");

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
        adapter.sync(&cx).expect("durably publish commit 2");

        // Reading should trigger incremental extend, not full rebuild.
        assert_eq!(
            adapter.read_page(&cx, 1).expect("read page 1 v2"),
            Some(page1_v2),
            "incremental index extend should pick up the updated page"
        );
        assert_eq!(adapter.read_page(&cx, 2).expect("read page 2"), Some(page2));
    }

    /// Frames for a two-page commit batch, the second frame carrying the commit.
    fn commit_batch_pages() -> (Vec<u8>, Vec<u8>) {
        (sample_page(0x71), sample_page(0x72))
    }

    /// Assert no commit horizon has been published yet.
    fn assert_publication_unchanged(adapter: &WalBackendAdapter<impl VfsFile>, context: &str) {
        assert_eq!(
            adapter.published_snapshot.last_commit_frame, None,
            "{context}: publication must not advance before a successful sync"
        );
        assert_eq!(
            adapter.published_snapshot.commit_count, 0,
            "{context}: commit count must not advance before a successful sync"
        );
        assert!(
            adapter.published_snapshot.page_index.is_empty(),
            "{context}: no page may be visible before a successful sync"
        );
    }

    #[test]
    fn test_append_frame_without_sync_leaves_publication_unchanged() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let (p1, p2) = commit_batch_pages();
        adapter.append_frame(&cx, 1, &p1, 0).expect("append p1");
        adapter.append_frame(&cx, 2, &p2, 2).expect("append commit");

        assert_publication_unchanged(&adapter, "append_frame");
        assert_eq!(
            adapter.pending_publication_commit,
            Some(1),
            "append_frame must stage the commit horizon for a later sync"
        );
    }

    #[test]
    fn test_append_frames_without_sync_leaves_publication_unchanged() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let (p1, p2) = commit_batch_pages();
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
        adapter
            .append_frames(&cx, &frames)
            .expect("append frames batch");

        assert_publication_unchanged(&adapter, "append_frames");
        assert_eq!(
            adapter.pending_publication_commit,
            Some(1),
            "append_frames must stage the commit horizon for a later sync"
        );
    }

    #[test]
    fn test_append_frames_tracked_without_sync_leaves_publication_unchanged() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let (p1, p2) = commit_batch_pages();
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
        adapter
            .append_frames_tracked(&cx, &frames, VfsWriteCompletion::new())
            .expect("append tracked frames batch");

        assert_publication_unchanged(&adapter, "append_frames_tracked");
        assert_eq!(
            adapter.pending_publication_commit,
            Some(1),
            "append_frames_tracked must stage the commit horizon for a later sync"
        );
    }

    #[test]
    fn test_append_prepared_frames_without_sync_leaves_publication_unchanged() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let (p1, p2) = commit_batch_pages();
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

        assert_publication_unchanged(&adapter, "append_prepared_frames");
        assert_eq!(
            adapter.pending_publication_commit,
            Some(1),
            "append_prepared_frames must stage the commit horizon for a later sync"
        );
    }

    #[test]
    fn test_append_prepared_frames_tracked_without_sync_leaves_publication_unchanged() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let (p1, p2) = commit_batch_pages();
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
            .append_prepared_frames_tracked(&cx, &mut prepared, VfsWriteCompletion::new())
            .expect("append prepared tracked");

        assert_publication_unchanged(&adapter, "append_prepared_frames_tracked");
        assert_eq!(
            adapter.pending_publication_commit,
            Some(1),
            "append_prepared_frames_tracked must stage the commit horizon for a later sync"
        );
    }

    #[test]
    fn test_successful_sync_publishes_staged_commit_horizon() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let (p1, p2) = commit_batch_pages();
        adapter.append_frame(&cx, 1, &p1, 0).expect("append p1");
        adapter.append_frame(&cx, 2, &p2, 2).expect("append commit");
        assert_publication_unchanged(&adapter, "before sync");

        adapter.sync(&cx).expect("sync must succeed");

        assert_eq!(
            adapter.published_snapshot.last_commit_frame,
            Some(1),
            "a successful sync must publish the staged commit horizon"
        );
        assert_eq!(
            adapter.published_snapshot.commit_count, 1,
            "a successful sync must publish the staged commit count"
        );
        assert_eq!(
            adapter.published_snapshot.page_index.len(),
            2,
            "a successful sync must publish every staged page"
        );
        assert_eq!(
            adapter.pending_publication_commit, None,
            "a published batch must no longer be staged"
        );
        assert!(
            adapter.pending_publication_frames.is_empty(),
            "a published batch must drain its staged frames"
        );
    }

    #[test]
    fn test_failed_sync_advances_no_publication_and_retry_publishes() {
        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let mut adapter = make_fault_adapter(&vfs, &cx);

        let (p1, p2) = commit_batch_pages();
        adapter.append_frame(&cx, 1, &p1, 0).expect("append p1");
        adapter.append_frame(&cx, 2, &p2, 2).expect("append commit");

        vfs.fail_next_wal_sync();
        let failure = adapter
            .sync(&cx)
            .expect_err("injected WAL sync failure must surface");
        assert!(
            failure.to_string().contains("injected WAL sync failure"),
            "sync must report the injected durability failure, got: {failure}"
        );

        assert_publication_unchanged(&adapter, "after failed sync");
        assert_eq!(
            adapter.pending_publication_commit,
            Some(1),
            "a failed sync must preserve the staged horizon for retry"
        );
        assert!(
            !adapter.pending_publication_frames.is_empty(),
            "a failed sync must preserve staged frames for retry"
        );

        // Retry: the same staged batch publishes once durability succeeds.
        adapter.sync(&cx).expect("retry sync must succeed");

        assert_eq!(
            adapter.published_snapshot.last_commit_frame,
            Some(1),
            "retrying sync must publish the preserved commit horizon"
        );
        assert_eq!(
            adapter.published_snapshot.commit_count, 1,
            "retrying sync must publish the preserved commit count"
        );
        assert_eq!(
            adapter.published_snapshot.page_index.len(),
            2,
            "retrying sync must publish every preserved page"
        );
        assert_eq!(
            adapter.pending_publication_commit, None,
            "a retried publication must clear the staged horizon"
        );
    }

    #[test]
    fn test_failed_sync_then_append_cannot_drop_or_publish_pending() {
        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let mut adapter = make_fault_adapter(&vfs, &cx);

        let (p1, p2) = commit_batch_pages();
        adapter.append_frame(&cx, 1, &p1, 0).expect("append p1");
        adapter.append_frame(&cx, 2, &p2, 2).expect("append commit");

        vfs.fail_next_wal_sync();
        adapter
            .sync(&cx)
            .expect_err("injected WAL sync failure must surface");

        let staged_after_failure = adapter.pending_publication_commit;
        let staged_frames_after_failure = adapter.pending_publication_frames.len();
        assert_eq!(
            staged_after_failure,
            Some(1),
            "failed sync must preserve the staged horizon"
        );

        // A further append must not run the pre-append resynchronization, which
        // would discard the preserved batch and republish the unsynced horizon.
        let p3 = sample_page(0x73);
        adapter
            .append_frame(&cx, 3, &p3, 3)
            .expect("append after failed sync");

        assert_publication_unchanged(&adapter, "append after failed sync");
        assert!(
            adapter.pending_publication_frames.len() > staged_frames_after_failure,
            "append after a failed sync must extend, never discard, the staged batch"
        );
        assert_eq!(
            adapter.pending_publication_commit,
            Some(2),
            "append after a failed sync must carry the staged horizon forward"
        );

        // Durability finally succeeds: the whole preserved batch publishes.
        adapter.sync(&cx).expect("sync after failed attempt");
        assert_eq!(
            adapter.published_snapshot.last_commit_frame,
            Some(2),
            "recovered sync must publish the full preserved horizon"
        );
        assert_eq!(
            adapter.pending_publication_commit, None,
            "recovered sync must clear the staged horizon"
        );
    }

    #[test]
    fn test_failed_sync_then_begin_transaction_then_append_fails_closed() {
        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let mut adapter = make_fault_adapter(&vfs, &cx);

        let (p1, p2) = commit_batch_pages();
        adapter.append_frame(&cx, 1, &p1, 0).expect("append p1");
        adapter.append_frame(&cx, 2, &p2, 2).expect("append commit");

        vfs.fail_next_wal_sync();
        adapter
            .sync(&cx)
            .expect_err("injected WAL sync failure must surface");
        assert_eq!(
            adapter.pending_publication_commit,
            Some(1),
            "failed sync must preserve the staged horizon"
        );

        // `begin_transaction` must reject at the earliest illegal transition,
        // before refreshing, pinning a read snapshot, or re-arming the
        // pre-append guard — and it must be a retryable Busy, not corruption.
        let begin_error = adapter
            .begin_transaction(&cx)
            .expect_err("begin_transaction must fail closed while frames are staged");
        assert!(
            matches!(begin_error, FrankenError::Busy),
            "staged-state rejection must be retryable Busy, not corruption: {begin_error:?}"
        );
        assert_publication_unchanged(&adapter, "begin_transaction refused after failed sync");
        assert_eq!(
            adapter.pending_publication_commit,
            Some(1),
            "a refused begin_transaction must not drop the staged horizon"
        );
        assert!(
            adapter.pinned_read_snapshot().is_none(),
            "a refused begin_transaction must not pin a read snapshot"
        );

        // Defense in depth: the pre-append choke guard still refuses for any
        // other path that re-arms `refresh_before_append`.
        adapter.refresh_before_append = true;
        let p3 = sample_page(0x74);
        let append_error = adapter
            .append_frame(&cx, 3, &p3, 3)
            .expect_err("append must fail closed while frames are staged");
        assert!(
            matches!(append_error, FrankenError::Busy),
            "append rejection must be retryable Busy: {append_error:?}"
        );
        assert_publication_unchanged(&adapter, "append refused after failed sync");
        assert_eq!(
            adapter.pending_publication_commit,
            Some(1),
            "a refused append must leave the staged horizon intact"
        );
        assert!(
            !adapter.pending_publication_frames.is_empty(),
            "a refused append must leave the staged frames intact"
        );
        adapter.refresh_before_append = false;

        // The batch is still recoverable: a successful sync publishes it.
        adapter.sync(&cx).expect("sync after failed attempt");
        assert_eq!(
            adapter.published_snapshot.last_commit_frame,
            Some(1),
            "recovered sync must publish the preserved horizon"
        );
    }

    #[test]
    fn test_failed_sync_then_checkpoint_fails_closed_and_preserves_state() {
        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let mut adapter = make_fault_adapter(&vfs, &cx);

        let (p1, p2) = commit_batch_pages();
        adapter.append_frame(&cx, 1, &p1, 0).expect("append p1");
        adapter.append_frame(&cx, 2, &p2, 2).expect("append commit");

        vfs.fail_next_wal_sync();
        adapter
            .sync(&cx)
            .expect_err("injected WAL sync failure must surface");

        let frames_before = adapter.frame_count();
        let staged_before = adapter.pending_publication_commit;
        let staged_frame_count_before = adapter.pending_publication_frames.len();

        // Checkpoint must refuse before touching the WAL: it backfills, may
        // reset, and can invalidate the publication plane, all of which would
        // destroy the staged batch.
        let mut writer = MockCheckpointPageWriter;
        let checkpoint_error = adapter
            .checkpoint(&cx, CheckpointMode::Passive, &mut writer, 0, None)
            .expect_err("checkpoint must fail closed while frames are staged");
        assert!(
            matches!(checkpoint_error, FrankenError::CheckpointFailed { .. }),
            "checkpoint rejection must be CheckpointFailed, not corruption: {checkpoint_error:?}"
        );

        assert_eq!(
            adapter.frame_count(),
            frames_before,
            "a refused checkpoint must not mutate WAL bytes"
        );
        assert_publication_unchanged(&adapter, "checkpoint refused");
        assert_eq!(
            adapter.pending_publication_commit, staged_before,
            "a refused checkpoint must preserve the staged horizon"
        );
        assert_eq!(
            adapter.pending_publication_frames.len(),
            staged_frame_count_before,
            "a refused checkpoint must preserve the staged frames"
        );

        // Retry: durability succeeds and the preserved batch publishes.
        adapter.sync(&cx).expect("retry sync must succeed");
        assert_eq!(
            adapter.published_snapshot.last_commit_frame,
            Some(1),
            "retry sync must publish the preserved horizon"
        );
        assert_eq!(
            adapter.pending_publication_commit, None,
            "a published batch must no longer be staged"
        );
    }

    #[test]
    fn test_midtransaction_sync_preserves_uncommitted_frames_and_allows_continuation() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let (p1, p2) = commit_batch_pages();

        // Append a non-commit frame, then sync. The frame becomes durable but is
        // not committed, so nothing may be published.
        adapter.append_frame(&cx, 1, &p1, 0).expect("append p1");
        assert_eq!(
            adapter.pending_publication_commit, None,
            "a non-commit append stages no commit horizon"
        );
        adapter
            .sync(&cx)
            .expect("mid-transaction sync must succeed");

        assert_publication_unchanged(&adapter, "sync of uncommitted frames");
        assert!(
            !adapter.pending_publication_frames.is_empty(),
            "a mid-transaction sync must preserve durable-but-uncommitted frames"
        );

        // Continuation must remain possible: the commit marker still lands.
        adapter
            .append_frame(&cx, 2, &p2, 2)
            .expect("commit append after mid-transaction sync must be allowed");
        assert_eq!(
            adapter.pending_publication_commit,
            Some(1),
            "the commit append must stage the horizon for the whole batch"
        );
        assert_publication_unchanged(&adapter, "commit staged but not yet synced");

        adapter.sync(&cx).expect("commit sync must succeed");

        assert_eq!(
            adapter.published_snapshot.last_commit_frame,
            Some(1),
            "the commit sync must publish the whole batch"
        );
        assert_eq!(
            adapter.published_snapshot.commit_count, 1,
            "the batch must publish exactly one commit"
        );
        assert_eq!(
            adapter.published_snapshot.page_index.len(),
            2,
            "both pages must be published exactly once"
        );
        assert_eq!(
            adapter.published_snapshot.page_index.get(&1),
            Some(&0),
            "page 1 must map to its frame from before the mid-transaction sync"
        );
        assert_eq!(
            adapter.published_snapshot.page_index.get(&2),
            Some(&1),
            "page 2 must map to the commit frame"
        );
        assert!(
            !adapter.has_pending_publication(),
            "a published batch must leave nothing staged"
        );
    }

    #[test]
    fn test_inner_mut_fails_closed_while_batch_is_staged() {
        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let mut adapter = make_fault_adapter(&vfs, &cx);

        let (p1, p2) = commit_batch_pages();
        adapter.append_frame(&cx, 1, &p1, 0).expect("append p1");
        adapter.append_frame(&cx, 2, &p2, 2).expect("append commit");

        assert!(
            adapter.has_pending_publication(),
            "an appended-but-unsynced batch must report as pending"
        );
        // `expect_err` would require `WalFile: Debug`, which the fault-VFS file
        // type does not implement, so assert on the pattern directly.
        assert!(
            matches!(adapter.inner_mut(), Err(FrankenError::Busy)),
            "inner_mut must fail closed with retryable Busy while frames are staged"
        );
        assert_eq!(
            adapter.pending_publication_commit,
            Some(1),
            "a refused inner_mut must preserve the staged horizon"
        );

        // Once drained, the escape hatch opens again.
        adapter.sync(&cx).expect("sync staged batch");
        assert!(
            !adapter.has_pending_publication(),
            "a published batch must clear the pending flag"
        );
        adapter
            .inner_mut()
            .expect("inner_mut must succeed once the batch is drained");
    }

    #[test]
    fn test_unpinned_refresh_does_not_expose_staged_horizon_before_sync() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let (p1, p2) = commit_batch_pages();
        adapter.append_frame(&cx, 1, &p1, 0).expect("append p1");
        adapter.append_frame(&cx, 2, &p2, 2).expect("append commit");

        // An explicit refresh must not publish frames this handle has staged but
        // not yet made durable.
        adapter
            .refresh_published_snapshot(&cx)
            .expect("refresh published snapshot");
        assert_publication_unchanged(&adapter, "refresh with staged frames");
        assert_eq!(
            adapter.pending_publication_commit,
            Some(1),
            "refresh must leave the staged horizon intact"
        );

        adapter.sync(&cx).expect("sync staged batch");
        assert_eq!(
            adapter.published_snapshot.last_commit_frame,
            Some(1),
            "sync must publish once the staged batch is durable"
        );
    }

    #[test]
    fn test_authorized_deferred_commit_publishes_without_claiming_fsync() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let (p1, p2) = commit_batch_pages();
        adapter.append_frame(&cx, 1, &p1, 0).expect("append p1");
        adapter.append_frame(&cx, 2, &p2, 2).expect("append commit");
        let fsynced_before = adapter.wal.last_fsynced_frame_count();

        adapter
            .publish_authorized_deferred_commit(&cx)
            .expect("parallel-WAL authorization must publish the deferred commit");

        assert_eq!(
            adapter.published_snapshot.last_commit_frame,
            Some(1),
            "the authorized commit marker must become visible"
        );
        assert_eq!(
            adapter.published_snapshot.commit_count, 1,
            "the authorized batch must publish exactly one commit"
        );
        assert!(
            !adapter.has_pending_publication(),
            "authorization must drain the staged publication horizon"
        );
        assert_eq!(
            adapter.wal.last_fsynced_frame_count(),
            fsynced_before,
            "deferred authorization must not claim or force an fsync"
        );
        adapter
            .begin_transaction(&cx)
            .expect("the next transaction must not see a stale Busy");
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
        // Publication is deferred to the durability barrier (#187); the commit
        // horizon only becomes visible once `sync` persists the frames.
        adapter.sync(&cx).expect("sync commit batch");

        assert_eq!(
            adapter.published_snapshot.last_commit_frame,
            Some(1),
            "synced commit should publish the visible commit horizon"
        );
        assert_eq!(
            adapter.published_snapshot.commit_count, 1,
            "synced commit should track the visible WAL commit count"
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
        // Publication is deferred to the durability barrier (#187).
        adapter.sync(&cx).expect("sync prepared commit batch");

        assert_eq!(
            adapter.published_snapshot.last_commit_frame,
            Some(1),
            "synced prepared commit should publish the visible commit horizon"
        );
        assert_eq!(
            adapter.published_snapshot.commit_count, 1,
            "synced prepared commit should track the visible WAL commit count"
        );
        assert_eq!(
            adapter.published_snapshot.page_index.len(),
            2,
            "synced prepared commit should publish all committed pages"
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

        // Durable-certificate contract: the append refreshes the EXTERNAL
        // published prefix into the follower's snapshot, but the follower's
        // own commit stays staged until its sync.
        assert_eq!(
            follower.published_snapshot.last_commit_frame,
            Some(1),
            "refresh-before-append must publish the external prefix only"
        );
        assert_eq!(
            follower.published_snapshot.commit_count, 2,
            "the staged local commit must not count until publication"
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
            None,
            "the staged local page must stay out of the published map"
        );

        follower.sync(&cx).expect("publish follower local commit");
        assert_eq!(
            follower.published_snapshot.last_commit_frame,
            Some(2),
            "publication must extend the map with the local commit"
        );
        assert_eq!(follower.published_snapshot.commit_count, 3);
        assert_eq!(
            follower.published_snapshot.page_index.get(&3),
            Some(&2),
            "published local commit extends the WAL visibility map"
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
        // Publication is deferred to the durability barrier (#187), and
        // checkpoint now fails closed while a batch is staged, so the batch must
        // be drained before checkpointing.
        adapter.sync(&cx).expect("sync committed frame");
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
        adapter.sync(&cx).expect("publish staged frames");

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
        adapter.sync(&cx).expect("publish staged frames");

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
        adapter.sync(&cx).expect("publish staged frames");

        let last_commit = adapter
            .inner_mut()
            .expect("no staged batch blocks inner access")
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
        adapter.sync(&cx).expect("publish staged frames");

        let last_commit = adapter
            .inner_mut()
            .expect("no staged batch blocks inner access")
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
        // Durable-certificate contract: the visible frame horizon advances
        // only at publication; count txns against the published horizon.
        adapter.sync(&cx).expect("publish staged commits");

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
        adapter.sync(&cx).expect("publish staged frames");
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
            snapshot_db_size: 0,
        };

        adapter
            .append_frame(&cx, 3, &p3, 0)
            .expect("append unrelated later page");
        adapter
            .append_frame(&cx, 2, &p2_after, 3)
            .expect("commit later page 2 update");
        // Publication gates conflict visibility exactly like reads: the
        // later commit must be published before it can conflict.
        adapter.sync(&cx).expect("publish later commit");

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
