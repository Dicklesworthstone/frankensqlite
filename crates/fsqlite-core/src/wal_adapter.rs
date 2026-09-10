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
    WalNativeReadBinding, WalNativeReadOutcome, WalNativeReadToken, WalNativeRecoveryReason,
};
use fsqlite_pager::{
    CheckpointMode, CheckpointPageWriter, CheckpointResult, ParallelWalCommitReconciliation,
    WalBackend, WalIndexShmSource, WalPublicationSnapshot,
};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};
use fsqlite_types::{CommitSeq, PageNumber, PageSize};
#[cfg(all(feature = "native", any(unix, windows)))]
use fsqlite_vfs::DatabaseNamespaceBinding;
use fsqlite_vfs::{SyncKind, Vfs, VfsFile, VfsWriteCompletion, VfsWriteCompletionState};
use fsqlite_wal::checkpoint_executor::CheckpointTargetFuture;
use fsqlite_wal::checksum::{SqliteWalChecksum, WAL_FRAME_HEADER_SIZE, WalChecksumTransform};
use fsqlite_wal::wal::WalAppendFrameRef;
use fsqlite_wal::wal_index::{
    SharedWalIndexResetPlan, WalIndexHdr, publish_shared_wal_index_backfill,
    read_shared_wal_index_backfill,
    SharedWalIndexAppendPlan, WalIndexFrameLocation, read_shared_wal_index_header,
    validate_shared_wal_index_wal_binding,
};
use fsqlite_wal::{
    CheckpointMode as WalCheckpointMode, CheckpointState, CheckpointTarget,
    PARALLEL_WAL_DURABLE_CERTIFICATE_MAGIC, PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE,
    ParallelWalCommitCertificate, ParallelWalDurableCertificateRecord,
    ParallelWalFramePayloadDigestBuilder, TransactionConflictPageBaseline,
    TransactionConflictSnapshot, WAL_HEADER_SIZE, WalFile, WalGenerationIdentity, WalHeader,
    WalSalts, durable_certificate_record_version_is_legacy, execute_checkpoint,
    validate_wal_header_checksum,
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

/// Owns one append even when its caller returns an error or drops its future.
/// Candidate metadata stays in `pending_publication_frames`; only successful
/// append completion or exact certificate reconciliation may accept it.
struct PendingWalAppendAttempt {
    generation: WalGenerationIdentity,
    previous_native_publication: Option<SharedWalIndexAppendPlan>,
    start_frame_index: usize,
    previous_running_checksum: SqliteWalChecksum,
    end_frame_count: usize,
    previous_pending_len: usize,
    previous_pending_commit: Option<usize>,
    previous_pending_generation: Option<WalGenerationIdentity>,
    previous_refresh_before_append: bool,
    completion: VfsWriteCompletion,
    authorized: bool,
}

/// Validated updates for one publication scan, bounded by admitted page keys.
/// No published lookup state changes until the entire scan succeeds.
#[derive(Debug, Default)]
struct WalPublicationDelta {
    page_index_updates: HashMap<u32, usize>,
    commit_count: u64,
    index_is_partial: bool,
}

/// One-pass index of the PHYSICAL appended tail (`0..frame_count`), built on
/// demand by [`WalBackend::read_page_at_appended_tail`] and reused while the
/// tail is provably unchanged: same generation identity, same frame count and
/// the same checksum on the last frame. Before this index every gate-held
/// tail read walked the whole WAL backwards one 24-byte header at a time, so
/// the disowned-page reclaim sweep cost O(ledger × frames): a 1.58M-entry
/// ledger against a 48,607-frame WAL never finished a writable open or a
/// checkpoint (cass GH #382).
struct AppendedTailIndex {
    generation: WalGenerationIdentity,
    frame_count: usize,
    tail_checksum: SqliteWalChecksum,
    /// Newest frame index per page within the appended tail.
    latest_frame_by_page: HashMap<u32, usize>,
}

struct NativeCheckpointView<F: VfsFile> {
    source: Arc<WalIndexShmSource<F>>,
    region: fsqlite_vfs::ShmRegion,
    header: WalIndexHdr,
    backfilled_frames: u32,
}

/// The adapter, rather than the executor future, owns every reset obligation.
struct PendingCheckpointReset<F: VfsFile> {
    old_header: WalHeader,
    target_header: WalHeader,
    truncate: bool,
    completion: VfsWriteCompletion,
    physical_complete: bool,
    shared_complete: bool,
    native: Option<SharedWalIndexResetPlan>,
    // Keep the exact attachment alive through async writes and publication.
    _source: Option<Arc<WalIndexShmSource<F>>>,
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
    /// Physical append outcome not yet accepted by its caller or reconciler.
    pending_append_attempt: Option<PendingWalAppendAttempt>,
    pending_checkpoint_reset: Option<PendingCheckpointReset<F>>,
    /// Mapping capability for this pager's exact native main-file attachment.
    wal_index_shm_source: Option<Arc<WalIndexShmSource<F>>>,
    /// Shared publication remains owned after the physical append succeeds.
    native_publication: Option<SharedWalIndexAppendPlan>,
    /// Exact reader token and bounds; its physical owner lives in the pager.
    native_read_binding: Option<WalNativeReadBinding>,
    /// Observation made before staging, retained until gated recovery succeeds.
    native_recovery_requested: Option<WalNativeRecoveryReason>,
    /// Optional FEC commit hook for encoding repair symbols on commit.
    #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
    fec_hook: Option<FecCommitHook>,
    /// Accumulated FEC commit results (for later sidecar persistence).
    #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
    fec_pending: Vec<FecCommitResult>,
    /// Generation actually retired by the last successful checkpoint, captured
    /// after refreshing the WAL rather than from a possibly stale caller view.
    #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
    checkpoint_retired_salts: Option<WalSalts>,
    /// Maximum number of unique pages the index will track. Defaults to a
    /// full authoritative index in steady state. Tests can lower the cap to
    /// exercise the partial-index fallback path explicitly.
    page_index_cap: usize,
    /// GH#402: frames this adapter has already backfilled into the database
    /// file for the tagged WAL generation (`nBackfill` equivalent). A
    /// checkpoint resumes from here instead of re-reading and re-writing the
    /// whole WAL from frame 0 on every pass — the source of the super-linear
    /// per-autocommit cost once the WAL crosses the autocheckpoint target.
    /// The tag is the generation identity (`checkpoint_seq` + salts): any
    /// reset — ours or a peer's — changes it and invalidates the watermark,
    /// so a stale watermark can only ever cause extra re-backfilling of
    /// identical bytes, never a skipped frame.
    checkpoint_backfill_watermark: Option<(WalGenerationIdentity, u32)>,
    /// Lazily built index of the appended tail; see [`AppendedTailIndex`].
    appended_tail_index: Option<AppendedTailIndex>,
    /// How many times the appended-tail index was built from a full forward
    /// pass over every frame — the observable that pins "one full scan per
    /// generation" in tests. A pure append no longer forces a rebuild; see
    /// [`Self::appended_tail_index_folds`].
    appended_tail_index_builds: u64,
    /// How many times the appended-tail index was extended INCREMENTALLY by
    /// folding in only the frames appended since the last lookup (same WAL
    /// generation, tail grew, prior tail frame unchanged). This is O(appended
    /// frames), not O(all frames): the fix for the 16-writer BEGIN/INSERT
    /// starvation convoy (bd-gh382-16writer-begin-starvation) where the tail
    /// moves on every peer commit and a full rebuild per commit — held under
    /// the append gate — starved writers off their retry budget.
    appended_tail_index_folds: u64,
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
            pending_append_attempt: None,
            pending_checkpoint_reset: None,
            wal_index_shm_source: None,
            native_publication: None,
            native_read_binding: None,
            native_recovery_requested: None,
            #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
            fec_hook: None,
            #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
            fec_pending: Vec::new(),
            #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
            checkpoint_retired_salts: None,
            page_index_cap: PAGE_INDEX_MAX_ENTRIES,
            checkpoint_backfill_watermark: None,
            appended_tail_index: None,
            appended_tail_index_builds: 0,
            appended_tail_index_folds: 0,
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
            pending_append_attempt: None,
            pending_checkpoint_reset: None,
            wal_index_shm_source: None,
            native_publication: None,
            native_read_binding: None,
            native_recovery_requested: None,
            fec_hook: Some(hook),
            fec_pending: Vec::new(),
            checkpoint_retired_salts: None,
            page_index_cap: PAGE_INDEX_MAX_ENTRIES,
            checkpoint_backfill_watermark: None,
            appended_tail_index: None,
            appended_tail_index_builds: 0,
            appended_tail_index_folds: 0,
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
        self.pending_append_attempt.is_some()
            || self.pending_checkpoint_reset.is_some()
            || self.native_publication.is_some()
            || self.pending_publication_commit.is_some()
            || !self.pending_publication_frames.is_empty()
    }

    /// Attach publication to the pager's exact main-file SHM capability.
    ///
    /// This performs no mapping, reader admission, recovery, or publication.
    /// Before appending, the caller must establish an initialized shared
    /// header bound to this WAL and retain the exact external WRITE owner
    /// through append, sync/deferred publication, and any reconciliation.
    /// Recovery and reader ownership remain separate prerequisites. Production
    /// constructors select native Unix backends explicitly.
    #[cfg(all(feature = "native", unix))]
    pub fn attach_wal_index_shm_source(
        &mut self,
        source: Arc<WalIndexShmSource<F>>,
    ) -> Result<()> {
        if self.has_pending_publication() || self.wal_index_shm_source.is_some() {
            return Err(FrankenError::BusyRecovery);
        }
        self.wal_index_shm_source = Some(source);
        Ok(())
    }

    /// Consume the adapter and return the inner [`WalFile`].
    ///
    /// Fails closed while staged, unpublished frames remain: consuming the
    /// adapter discards the staged publication metadata, and the `WalFile` can
    /// be rewrapped by an adapter that would then publish those frames without
    /// knowing whether they were ever published or fsynced (GH #187). Drain the
    /// batch with a successful commit sync first.
    /// On refusal, returns the same adapter with its publication metadata and
    /// append completion owner intact. The caller can reconcile or sync it,
    /// then retry extraction without reconstructing lost state.
    pub fn into_inner(self) -> std::result::Result<WalFile<F>, Box<Self>> {
        if self.has_pending_publication() || self.native_read_binding.is_some()
            || self.native_recovery_requested.is_some()
        {
            return Err(Box::new(self));
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
        if self.has_pending_publication() || self.native_read_binding.is_some()
            || self.native_recovery_requested.is_some()
        {
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
        if self.native_read_binding.is_some() || self.native_recovery_requested.is_some() {
            return Err(FrankenError::BusyRecovery);
        }
        self.assert_no_pending_append_attempt()?;
        if self.native_publication.is_some() {
            // A refresh can trim an owned uncommitted suffix or bypass a
            // failed native publication after fsync. Finish that owner first.
            return Err(FrankenError::BusyRecovery);
        }
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
        let (start, base_commit_count, extend_previous) = match (
            previous_generation == generation,
            previous_last_commit,
            last_commit_frame,
        ) {
            (true, Some(previous), Some(current)) if previous < current => (
                previous.saturating_add(1),
                self.published_snapshot.commit_count,
                true,
            ),
            _ => (0, 0, false),
        };

        let frame_delta_count = match (previous_last_commit, last_commit_frame) {
            (Some(prev), Some(curr)) if curr >= prev => curr.saturating_sub(prev),
            (Some(_) | None, Some(curr)) => curr.saturating_add(1),
            (Some(prev), None) => prev.saturating_add(1),
            (None, None) => 0,
        };

        // Header reads can fail or be cancelled after a tracked page has been
        // encountered. Stage only admitted keys; preserve the entire prior
        // snapshot, including its Arc, until every read has succeeded.
        let delta = match last_commit_frame {
            Some(end) => {
                self.scan_publication_delta(cx, extend_previous, start, end)
                    .await?
            }
            None => WalPublicationDelta::default(),
        };
        let commit_count = base_commit_count.saturating_add(delta.commit_count);
        let index_is_partial = delta.index_is_partial;
        // There is no fallible I/O or await after taking the published map.
        // An unpinned map remains uniquely owned, so applying the delta does
        // not clone the full index. Pinned readers retain their prior Arc.
        let page_index = if extend_previous {
            let mut page_index = std::mem::replace(
                &mut self.published_snapshot.page_index,
                Arc::new(HashMap::new()),
            );
            if !delta.page_index_updates.is_empty() {
                Arc::make_mut(&mut page_index).extend(delta.page_index_updates);
            }
            page_index
        } else {
            Arc::new(delta.page_index_updates)
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

    /// Validate `start..=end` without mutating the published page index.
    ///
    /// Admit new keys in scan order, retain updates to already admitted keys
    /// at capacity, and count every commit marker even when its page is dropped.
    /// The delta stores at most one latest frame per admitted key, never one
    /// entry per frame or a clone of the full published map.
    async fn scan_publication_delta(
        &self,
        cx: &Cx,
        extend_previous: bool,
        start: usize,
        end: usize,
    ) -> Result<WalPublicationDelta> {
        let base_index = extend_previous.then_some(self.published_snapshot.page_index.as_ref());
        let mut admitted_page_count = base_index.map_or(0, HashMap::len);
        let mut delta = WalPublicationDelta {
            index_is_partial: extend_previous && self.published_snapshot.index_is_partial,
            ..WalPublicationDelta::default()
        };
        for frame_index in start..=end {
            let header = self.wal.read_frame_header(cx, frame_index).await?;
            let already_admitted = delta.page_index_updates.contains_key(&header.page_number)
                || base_index.is_some_and(|index| index.contains_key(&header.page_number));
            if already_admitted || admitted_page_count < self.page_index_cap {
                if !already_admitted {
                    admitted_page_count += 1;
                }
                delta.page_index_updates.insert(header.page_number, frame_index);
            } else {
                delta.index_is_partial = true;
            }
            if header.is_commit() {
                delta.commit_count = delta.commit_count.saturating_add(1);
            }
        }
        Ok(delta)
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

    /// Newest frame for `page_number` within the appended tail `0..=tail_frame`.
    ///
    /// Same answer as [`Self::scan_backwards_for_page`] over that range (the
    /// newest frame wins), but the tail is indexed once and the index is
    /// reused while the tail is provably unchanged — same generation
    /// identity, same frame count, same checksum on the last frame. A
    /// changed tail costs one fresh pass; a stable tail costs one header
    /// read (the checksum probe) per lookup.
    async fn appended_tail_frame_for_page(
        &mut self,
        cx: &Cx,
        page_number: u32,
        tail_frame: usize,
    ) -> Result<Option<usize>> {
        let frame_count = tail_frame.saturating_add(1);
        let generation = self.wal.generation_identity();
        let tail_checksum = self.wal.read_frame_header(cx, tail_frame).await?.checksum;

        // Classify the cached index against the current physical tail:
        //   Reuse    — identical stable tail (generation, frame count, and the
        //              checksum on the last frame all match): serve as-is.
        //   Fold     — same generation and the tail GREW, with the frame that
        //              was previously the tail still carrying its recorded
        //              checksum (proof the older frames are untouched, so the
        //              WAL only appended). Fold in just the new frames.
        //   Rebuild  — no usable index, a different generation (a WAL
        //              reset/wrap can reuse frame slots), or the append-only
        //              proof failed: one full forward pass.
        //
        // Folding is the fix for bd-gh382-16writer-begin-starvation: under many
        // concurrent writers the tail advances on every peer commit, so keying
        // reuse on the tail checksum alone rebuilt the whole (growing) WAL on
        // every gate-held commit read — an O(frames) pass per commit that
        // convoyed writers off their retry budget. Folding makes a grown tail
        // cost O(appended frames) instead.
        enum TailIndexPlan {
            Reuse,
            Fold { from_frame: usize },
            Rebuild,
        }
        let plan = match self.appended_tail_index.as_ref() {
            Some(index)
                if index.generation == generation
                    && index.frame_count == frame_count
                    && index.tail_checksum == tail_checksum =>
            {
                TailIndexPlan::Reuse
            }
            Some(index) if index.generation == generation && frame_count > index.frame_count => {
                // The tail grew. Confirm append-only: the frame that used to be
                // the tail must still carry the checksum we indexed it with.
                let previous_tail_frame = index.frame_count.saturating_sub(1);
                let previous_tail_checksum =
                    self.wal.read_frame_header(cx, previous_tail_frame).await?.checksum;
                if index.tail_checksum == previous_tail_checksum {
                    TailIndexPlan::Fold {
                        from_frame: index.frame_count,
                    }
                } else {
                    TailIndexPlan::Rebuild
                }
            }
            _ => TailIndexPlan::Rebuild,
        };

        match plan {
            TailIndexPlan::Reuse => {}
            TailIndexPlan::Fold { from_frame } => {
                for frame_index in from_frame..frame_count {
                    let header = self.wal.read_frame_header(cx, frame_index).await?;
                    if let Some(index) = self.appended_tail_index.as_mut() {
                        index.latest_frame_by_page.insert(header.page_number, frame_index);
                    }
                }
                if let Some(index) = self.appended_tail_index.as_mut() {
                    index.frame_count = frame_count;
                    index.tail_checksum = tail_checksum;
                }
                self.appended_tail_index_folds = self.appended_tail_index_folds.saturating_add(1);
            }
            TailIndexPlan::Rebuild => {
                let mut latest_frame_by_page = HashMap::new();
                for frame_index in 0..frame_count {
                    let header = self.wal.read_frame_header(cx, frame_index).await?;
                    latest_frame_by_page.insert(header.page_number, frame_index);
                }
                self.appended_tail_index = Some(AppendedTailIndex {
                    generation,
                    frame_count,
                    tail_checksum,
                    latest_frame_by_page,
                });
                self.appended_tail_index_builds = self.appended_tail_index_builds.saturating_add(1);
            }
        }

        Ok(self
            .appended_tail_index
            .as_ref()
            .and_then(|index| index.latest_frame_by_page.get(&page_number).copied()))
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
        self.assert_no_pending_append_attempt()?;
        if let Some(plan) = &self.native_publication {
            if plan.generation() != self.wal.generation_identity()
                || self.published_snapshot.generation != plan.generation()
            {
                return Err(FrankenError::BusyRecovery);
            }
            // Only the native-before-private commit hooks may advance this
            // owner's visibility, even if its WAL fsync already succeeded.
            return Ok(());
        }
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
            }).or_else(|| {
                // A newer staged marker cannot revoke an earlier publication.
                // Preserve its exact horizon only while it still belongs to
                // this live generation. That publication may have deferred
                // sync authority, so its visibility does not require a local
                // fsync watermark. Never infer a marker from durable_frames.
                let published = &self.published_snapshot;
                if published.generation != self.wal.generation_identity() {
                    return None;
                }
                published.last_commit_frame.filter(|previous| {
                    *previous < self.wal.frame_count()
                        && last_commit_frame.is_some_and(|latest| *previous <= latest)
                })
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
        if self.wal_index_shm_source.is_some() {
            return self.preflight_native_append(cx).await;
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

    /// Refuse mutation or publication while an append still needs reconciliation.
    fn assert_no_pending_append_attempt(&self) -> Result<()> {
        if self.pending_append_attempt.is_some() || self.pending_checkpoint_reset.is_some() {
            return Err(FrankenError::BusyRecovery);
        }
        Ok(())
    }

    fn validate_append_page(&self, page_data: &[u8]) -> Result<()> {
        if page_data.len() != self.wal.page_size() {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "page data size mismatch: expected {}, got {}",
                    self.wal.page_size(), page_data.len()
                ),
            });
        }
        Ok(())
    }

    /// Validate public prepared metadata before it can become a recovery owner.
    fn validate_prepared_frame_metadata(&self, prepared: &PreparedWalFrameBatch) -> Result<()> {
        let expected_bytes = prepared.frame_count().checked_mul(self.wal.frame_size())
            .ok_or(FrankenError::DatabaseFull)?;
        if prepared.frame_size != self.wal.frame_size()
            || prepared.page_data_offset != WAL_FRAME_HEADER_SIZE
            || prepared.big_endian_checksum != self.wal.big_endian_checksum()
            || prepared.checksum_transforms.len() != prepared.frame_count()
            || prepared.frame_bytes.len() != expected_bytes
        {
            return Err(FrankenError::WalCorrupt {
                detail: "prepared WAL batch layout does not match its frame metadata".to_owned(),
            });
        }
        let mut last_commit = None;
        for (index, (meta, bytes)) in prepared.frame_metas.iter()
            .zip(prepared.frame_bytes.chunks_exact(self.wal.frame_size())).enumerate()
        {
            if bytes[..4] != meta.page_number.to_be_bytes()
                || bytes[4..8] != meta.db_size_if_commit.to_be_bytes()
            {
                return Err(FrankenError::WalCorrupt {
                    detail: "prepared WAL frame bytes disagree with publication metadata".to_owned(),
                });
            }
            if meta.db_size_if_commit != 0 {
                last_commit = Some(index);
            }
        }
        if last_commit != prepared.last_commit_frame_offset {
            return Err(FrankenError::WalCorrupt {
                detail: "prepared commit offset does not identify the final commit marker".to_owned(),
            });
        }
        Ok(())
    }

    /// Finalization may refresh salts; check the resulting bytes before arming.
    fn validate_finalized_prepared_generation(&self, prepared: &PreparedWalFrameBatch) -> Result<()> {
        let salts = self.wal.generation_identity().salts;
        let checksum = Self::finalized_running_checksum(prepared)?;
        let tail = prepared.frame_bytes.chunks_exact(self.wal.frame_size()).next_back()
            .ok_or_else(|| FrankenError::internal("nonempty prepared append lost its tail"))?;
        if tail[16..20] != checksum.s1.to_be_bytes() || tail[20..24] != checksum.s2.to_be_bytes() {
            return Err(FrankenError::WalCorrupt {
                detail: "prepared WAL tail checksum differs from finalized metadata".to_owned(),
            });
        }
        if prepared.frame_bytes.chunks_exact(self.wal.frame_size()).any(|bytes| {
            bytes[8..12] != salts.salt1.to_be_bytes() || bytes[12..16] != salts.salt2.to_be_bytes()
        }) {
            return Err(FrankenError::WalCorrupt {
                detail: "prepared WAL frame salts do not match the append generation".to_owned(),
            });
        }
        Ok(())
    }

    /// Stage exact candidate metadata before ownership reaches physical I/O.
    async fn stage_append_attempt<I>(
        &mut self,
        cx: &Cx,
        frames: I,
        completion: VfsWriteCompletion,
    ) -> Result<()>
    where
        I: ExactSizeIterator<Item = (u32, u32)> + Clone,
    {
        self.assert_no_pending_append_attempt()?;
        if completion.state() != VfsWriteCompletionState::Pending {
            return Err(FrankenError::WalCorrupt {
                detail: "WAL append completion token was already terminal".to_owned(),
            });
        }
        frames.len().checked_mul(self.wal.frame_size()).ok_or(FrankenError::DatabaseFull)?;
        let start_frame_index = self.wal.frame_count();
        let end_frame_count = start_frame_index
            .checked_add(frames.len())
            .ok_or(FrankenError::DatabaseFull)?;
        if end_frame_count > usize::try_from(u32::MAX).unwrap_or(usize::MAX) {
            return Err(FrankenError::DatabaseFull);
        }
        let generation = self.wal.generation_identity();
        if self.pending_publication_generation.is_some_and(|old| old != generation) {
            return Err(FrankenError::WalCorrupt {
                detail: "cannot append across an unresolved publication generation".to_owned(),
            });
        }
        let prepared_native_publication = self.prepare_native_publication(cx, frames.clone()).await?;
        self.pending_append_attempt = Some(PendingWalAppendAttempt {
            previous_native_publication: std::mem::replace(
                &mut self.native_publication,
                prepared_native_publication,
            ),
            generation,
            start_frame_index,
            previous_running_checksum: self.wal.running_checksum(),
            end_frame_count,
            previous_pending_len: self.pending_publication_frames.len(),
            previous_pending_commit: self.pending_publication_commit,
            previous_pending_generation: self.pending_publication_generation,
            previous_refresh_before_append: self.refresh_before_append,
            completion,
            authorized: false,
        });
        let last_commit = self.record_appended_frames(start_frame_index, frames);
        self.pending_publication_generation = Some(generation);
        self.refresh_before_append = false;
        if let Some(last_commit) = last_commit {
            self.stage_pending_commit_publication(last_commit)?;
        }
        Ok(())
    }

    /// Record only a complete orphan observed under this exact fresh WRITE.
    /// The old reader pin remains untouched; recovery occurs after unwind.
    async fn classify_unpublished_native_tail(
        &mut self,
        cx: &Cx,
        baseline: fsqlite_wal::wal_index::WalIndexHdr,
        start: u32,
    ) -> Result<()> {
        if baseline.mx_frame >= start { return Ok(()); }
        if self.has_pending_publication() { return Err(FrankenError::BusyRecovery); }
        let (index, committed) = self.wal.last_commit_frame_header()
            .ok_or(FrankenError::BusyRecovery)?;
        let committed_count = u32::try_from(index).ok().and_then(|index| index.checked_add(1))
            .ok_or(FrankenError::DatabaseFull)?;
        if committed_count <= baseline.mx_frame || committed_count > start || !committed.is_commit()
        {
            return Err(FrankenError::BusyRecovery);
        }
        let observed = self.wal.read_frame_header(cx, index).await?;
        if observed != committed || observed.salts != self.wal.header().salts {
            return Err(FrankenError::BusyRecovery);
        }
        let source = self.wal_index_shm_source.as_ref().ok_or(FrankenError::Unsupported)?;
        if !source.owns_external_wal_append_write(cx).await? {
            return Err(FrankenError::BusyRecovery);
        }
        // No fallible work or await separates the ownership query from this
        // retained observation. It is not an append/publication candidate.
        self.native_recovery_requested = Some(WalNativeRecoveryReason::UnpublishedWalTail);
        tracing::debug!(
            target: "fsqlite.wal.recovery",
            shared_frame = baseline.mx_frame,
            committed_frame = committed_count,
            "fresh native append owner observed an unadvertised committed WAL tail"
        );
        Err(FrankenError::BusyRecovery)
    }

    /// Inspect the live native prefix before conflict checks or candidate staging.
    async fn native_append_baseline(&mut self, cx: &Cx) -> Result<fsqlite_wal::wal_index::WalIndexHdr> {
        if self.has_pending_publication() || self.native_recovery_requested.is_some() {
            return Err(FrankenError::BusyRecovery);
        }
        let source = self.wal_index_shm_source.clone().ok_or(FrankenError::Unsupported)?;
        let region = source.map_region(cx, 0, false).await?;
        let baseline = read_shared_wal_index_header(&region)?.ok_or(FrankenError::BusyRecovery)?;
        self.wal.refresh(cx).await?;
        let start = u32::try_from(self.wal.frame_count()).map_err(|_| FrankenError::DatabaseFull)?;
        if baseline.mx_frame > start { return Err(FrankenError::BusyRecovery); }
        let terminal = if baseline.mx_frame == 0 {
            None
        } else {
            let index = usize::try_from(baseline.mx_frame - 1).map_err(|_| FrankenError::DatabaseFull)?;
            Some((baseline.mx_frame, self.wal.read_frame_header(cx, index).await?))
        };
        validate_shared_wal_index_wal_binding(&baseline, self.wal.header(), terminal)?;
        self.classify_unpublished_native_tail(cx, baseline, start).await?;
        Ok(baseline)
    }

    /// Map and validate everything needed by the later synchronous publisher.
    /// Live index entries and both shared header copies remain unchanged.
    async fn prepare_native_publication<I>(
        &mut self,
        cx: &Cx,
        frames: I,
    ) -> Result<Option<SharedWalIndexAppendPlan>>
    where
        I: ExactSizeIterator<Item = (u32, u32)>,
    {
        let Some(source) = self.wal_index_shm_source.clone() else {
            return Ok(None);
        };
        if self.native_recovery_requested.is_some() { return Err(FrankenError::BusyRecovery); }
        if self.native_publication.as_ref().is_some_and(|plan| !plan.can_extend()) {
            return Err(FrankenError::BusyRecovery);
        }
        let region_zero = source.map_region(cx, 0, false).await?;
        let baseline = read_shared_wal_index_header(&region_zero)?
            .ok_or(FrankenError::BusyRecovery)?;
        let generation = self.wal.generation_identity();
        let start = u32::try_from(self.wal.frame_count()).map_err(|_| FrankenError::DatabaseFull)?;
        if baseline.mx_frame > start {
            return Err(FrankenError::BusyRecovery);
        }
        if let Some(previous) = &self.native_publication {
            if previous.baseline() != baseline || previous.generation() != generation {
                return Err(FrankenError::BusyRecovery);
            }
        } else if !self.pending_publication_frames.is_empty() {
            // An unexplained physical suffix is not ours to certify. Shared
            // initialization/recovery belongs to the separate recovery owner.
            return Err(FrankenError::BusyRecovery);
        }
        let terminal = if baseline.mx_frame == 0 {
            None
        } else {
            let index = usize::try_from(baseline.mx_frame - 1)
                .map_err(|_| FrankenError::DatabaseFull)?;
            let header = match self.wal.last_commit_frame_header() {
                Some((cached_index, header)) if cached_index == index => header,
                _ => self.wal.read_frame_header(cx, index).await?,
            };
            Some((baseline.mx_frame, header))
        };
        validate_shared_wal_index_wal_binding(&baseline, self.wal.header(), terminal)?;
        if self.native_publication.is_none() {
            self.classify_unpublished_native_tail(cx, baseline, start).await?;
        }
        let capacity = self.pending_publication_frames.len().checked_add(frames.len())
            .ok_or(FrankenError::DatabaseFull)?;
        let mut entries = Vec::with_capacity(capacity);
        let mut previous = baseline.mx_frame;
        for frame in &self.pending_publication_frames {
            let number = u32::try_from(frame.frame_index)
                .ok().and_then(|index| index.checked_add(1))
                .ok_or(FrankenError::DatabaseFull)?;
            if previous.checked_add(1) != Some(number) {
                return Err(FrankenError::BusyRecovery);
            }
            entries.push((number, frame.page_number, frame.is_commit));
            previous = number;
        }
        if previous != start {
            return Err(FrankenError::BusyRecovery);
        }
        for (page, db_size_if_commit) in frames {
            previous = previous.checked_add(1).ok_or(FrankenError::DatabaseFull)?;
            entries.push((previous, page, db_size_if_commit != 0));
        }
        let mut region_numbers = vec![0];
        for &(frame, _, _) in &entries {
            let number = WalIndexFrameLocation::new(frame)?.region;
            if region_numbers.last() != Some(&number) {
                region_numbers.push(number);
            }
        }
        let mut regions = Vec::with_capacity(region_numbers.len());
        regions.push((0, region_zero));
        for number in region_numbers.into_iter().skip(1) {
            regions.push((number, source.map_region(cx, number, true).await?));
        }
        SharedWalIndexAppendPlan::prepare(baseline, generation, regions, entries).map(Some)
    }

    fn publish_native_pending(&mut self, last_commit_frame: usize) -> Result<()> {
        if self.wal_index_shm_source.is_none() {
            return Ok(());
        }
        let plan = self.native_publication.as_mut().ok_or(FrankenError::BusyRecovery)?;
        let (index, marker) = self.wal.last_commit_frame_header()
            .ok_or(FrankenError::BusyRecovery)?;
        if index != last_commit_frame || plan.generation() != self.wal.generation_identity() {
            return Err(FrankenError::BusyRecovery);
        }
        let mut target = plan.baseline();
        target.mx_frame = u32::try_from(index).ok().and_then(|frame| frame.checked_add(1))
            .ok_or(FrankenError::DatabaseFull)?;
        target.n_page = marker.db_size;
        target.a_frame_cksum = [marker.checksum.s1, marker.checksum.s2];
        target.i_change = plan.publication_change(target.mx_frame)?;
        target.update_checksum()?;
        validate_shared_wal_index_wal_binding(
            &target, self.wal.header(), Some((target.mx_frame, marker)),
        )?;
        plan.publish(target)
    }

    fn finish_native_publication(&mut self) {
        if self.native_publication.as_mut()
            .is_some_and(|plan| !plan.finish_private_publication())
        {
            self.native_publication = None;
        }
    }

    /// Validate the native publication before maintenance reads or DB backfill.
    /// A newer physical commit is not silently checkpointed under an older SHM header.
    async fn native_checkpoint_view(&mut self, cx: &Cx) -> Result<Option<NativeCheckpointView<F>>> {
        if self.has_pending_publication() || self.native_read_binding.is_some()
            || self.native_recovery_requested.is_some()
        {
            return Err(FrankenError::BusyRecovery);
        }
        let Some(source) = self.wal_index_shm_source.clone() else { return Ok(None); };
        let region = source.map_region(cx, 0, false).await?;
        let header = read_shared_wal_index_header(&region)?.ok_or(FrankenError::BusyRecovery)?;
        self.wal.refresh(cx).await?;
        if usize::try_from(header.mx_frame).ok() != Some(self.wal.frame_count()) {
            return Err(FrankenError::BusyRecovery);
        }
        let terminal = if header.mx_frame == 0 { None } else {
            let index = usize::try_from(header.mx_frame - 1).map_err(|_| FrankenError::DatabaseFull)?;
            Some((header.mx_frame, self.wal.read_frame_header(cx, index).await?))
        };
        validate_shared_wal_index_wal_binding(&header, self.wal.header(), terminal)?;
        let backfilled_frames = read_shared_wal_index_backfill(&region, &header)?;
        Ok(Some(NativeCheckpointView { source, region, header, backfilled_frames }))
    }

    /// Commit private reset state only after the physical and shared phases succeeded.
    fn finish_checkpoint_reset(&mut self) -> Result<()> {
        let Some(reset) = &self.pending_checkpoint_reset else { return Ok(()); };
        if !reset.physical_complete || !reset.shared_complete
            || self.wal.header() != &reset.target_header || self.wal.frame_count() != 0
        {
            return Err(FrankenError::BusyRecovery);
        }
        let reset = self.pending_checkpoint_reset.take().expect("validated reset owner");
        #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
        {
            self.checkpoint_retired_salts = Some(reset.old_header.salts);
            self.fec_pending.clear();
            self.fec_discard();
        }
        #[cfg(not(all(not(target_arch = "wasm32"), feature = "native")))]
        let _ = reset;
        self.invalidate_publication();
        self.published_snapshot = WalPublishedSnapshot::empty(self.next_publication_seq, self.wal.generation_identity());
        self.next_publication_seq = self.next_publication_seq.saturating_add(1);
        self.appended_tail_index = None;
        self.checkpoint_backfill_watermark = None;
        self.refresh_before_append = true;
        self.native_recovery_requested = None;
        Ok(())
    }

    /// Construct the exact recovery publication from the fully validated WAL.
    fn native_recovery_header(&self) -> Result<fsqlite_wal::wal_index::WalIndexHdr> {
        use fsqlite_wal::wal_index::{WAL_INDEX_VERSION, WalIndexHdr};

        let wal_header = self.wal.header();
        let mut target = WalIndexHdr {
            i_version: WAL_INDEX_VERSION,
            unused: 0,
            i_change: 0,
            is_init: 1,
            big_end_cksum: u8::from(wal_header.big_endian_checksum()),
            sz_page: if wal_header.page_size == 65_536 {
                1
            } else {
                u16::try_from(wal_header.page_size).map_err(|_| FrankenError::DatabaseFull)?
            },
            mx_frame: u32::try_from(self.wal.frame_count()).map_err(|_| FrankenError::DatabaseFull)?,
            n_page: 0,
            a_frame_cksum: [0, 0],
            a_salt: [wal_header.salts.salt1, wal_header.salts.salt2],
            a_cksum: [0, 0],
        };
        let terminal = if let Some((index, marker)) = self.wal.last_commit_frame_header() {
            let number = u32::try_from(index).ok().and_then(|index| index.checked_add(1))
                .ok_or(FrankenError::DatabaseFull)?;
            target.n_page = marker.db_size;
            target.a_frame_cksum = [marker.checksum.s1, marker.checksum.s2];
            Some((number, marker))
        } else {
            None
        };
        target.update_checksum()?;
        validate_shared_wal_index_wal_binding(&target, wal_header, terminal)?;
        Ok(target)
    }

    /// Rebuild only native index metadata while the caller owns every recovery fence.
    /// A failed or dropped rebuild leaves both advertised headers invalid.
    async fn recover_native_index(&mut self, cx: &Cx) -> Result<()> {
        use fsqlite_wal::wal_index::{
            WAL_SHM_SEGMENT_BYTES, append_native_wal_index_entry, invalidate_shared_wal_index_header,
            publish_shared_wal_index_header, replace_shared_wal_index_region,
            reset_shared_wal_index_recovery_marks,
        };

        if self.has_pending_publication() || self.native_read_binding.is_some() {
            return Err(FrankenError::BusyRecovery);
        }
        let source = self.wal_index_shm_source.clone().ok_or(FrankenError::Unsupported)?;
        let region_zero = source.map_region(cx, 0, true).await?;
        // A peer may have repaired the observed header before we obtained the
        // canonical owner. Preserve its full header and checkpoint progress if
        // it now names the complete committed tail of this WAL generation.
        if let Some(header) = read_shared_wal_index_header(&region_zero)? {
            self.wal.refresh(cx).await?;
            if usize::try_from(header.mx_frame).ok() == Some(self.wal.frame_count()) {
                let terminal = if header.mx_frame == 0 {
                    Some(None)
                } else {
                    let index = usize::try_from(header.mx_frame - 1)
                        .map_err(|_| FrankenError::DatabaseFull)?;
                    match self.wal.read_frame_header(cx, index).await {
                        Ok(marker) => Some(Some((header.mx_frame, marker))),
                        Err(FrankenError::WalCorrupt { .. }) => None,
                        Err(error) => return Err(error),
                    }
                };
                if terminal.is_some_and(|terminal| {
                    validate_shared_wal_index_wal_binding(&header, self.wal.header(), terminal).is_ok()
                }) {
                    self.native_recovery_requested = None;
                    return Ok(());
                }
            }
        }

        // No await or mutation of index entries can precede this invalidation.
        // The exact outer owner may then restore on every error/drop: a later
        // admission observes an unaccepted header and re-enters full recovery.
        invalidate_shared_wal_index_header(&region_zero)?;
        self.invalidate_publication();
        self.appended_tail_index = None;
        self.checkpoint_backfill_watermark = None;
        self.refresh_before_append = true;
        self.wal.rebuild_state_from_file(cx).await?;

        let target = self.native_recovery_header()?;
        let maximum_frame = target.mx_frame;
        let wal_header = *self.wal.header();
        let last_region = if maximum_frame == 0 { 0 } else {
            WalIndexFrameLocation::new(maximum_frame)?.region
        };
        let mut next_frame = 1_u64;
        let mut scratch = vec![0; WAL_SHM_SEGMENT_BYTES];
        for number in 0..=last_region {
            scratch.fill(0);
            while next_frame <= u64::from(maximum_frame) {
                let frame = u32::try_from(next_frame).map_err(|_| FrankenError::DatabaseFull)?;
                if WalIndexFrameLocation::new(frame)?.region != number {
                    break;
                }
                let index = usize::try_from(frame - 1).map_err(|_| FrankenError::DatabaseFull)?;
                let marker = self.wal.read_frame_header(cx, index).await?;
                if marker.page_number == 0 || marker.salts != wal_header.salts {
                    return Err(FrankenError::WalCorrupt {
                        detail: "validated recovery frame changed under the canonical owner".to_owned(),
                    });
                }
                if frame == maximum_frame {
                    validate_shared_wal_index_wal_binding(&target, &wal_header, Some((frame, marker)))?;
                }
                append_native_wal_index_entry(&mut scratch, frame, marker.page_number)?;
                next_frame += 1;
            }
            let region = if number == 0 { region_zero.share() } else {
                source.map_region(cx, number, true).await?
            };
            replace_shared_wal_index_region(&region, number, &scratch)?;
        }
        reset_shared_wal_index_recovery_marks(&region_zero, maximum_frame)?;
        publish_shared_wal_index_header(&region_zero, &target)?;
        self.native_recovery_requested = None;
        Ok(())
    }

    fn finish_successful_append_attempt(&mut self) -> Result<()> {
        let attempt = self.pending_append_attempt.as_ref().ok_or_else(|| {
            FrankenError::internal("successful WAL append lost its retained attempt")
        })?;
        if attempt.completion.state() != VfsWriteCompletionState::Success
            || attempt.generation != self.wal.generation_identity()
            || attempt.end_frame_count != self.wal.frame_count()
        {
            return Err(FrankenError::WalCorrupt {
                detail: "successful WAL append does not match its retained interval".to_owned(),
            });
        }
        self.pending_append_attempt = None;
        Ok(())
    }

    /// Validate the exact one-based recovery interval before refreshing any WAL state.
    fn validate_append_reconciliation(&self, start: u64, end: u64) -> Result<()> {
        if self.pending_checkpoint_reset.is_some() { return Err(FrankenError::BusyRecovery); }
        let Some(attempt) = &self.pending_append_attempt else {
            // A successful append can be followed by a certificate/sync error.
            return Ok(());
        };
        if attempt.completion.state() == VfsWriteCompletionState::Pending {
            return Err(FrankenError::BusyRecovery);
        }
        let expected_start = u64::try_from(attempt.start_frame_index)
            .ok()
            .and_then(|index| index.checked_add(1));
        if attempt.generation != self.wal.generation_identity()
            || expected_start != Some(start)
            || u64::try_from(attempt.end_frame_count).ok() != Some(end)
        {
            return Err(FrankenError::WalCorrupt {
                detail: "WAL reconciliation does not match the retained append generation/interval"
                    .to_owned(),
            });
        }
        Ok(())
    }

    fn authorize_append_reconciliation(&mut self) {
        if let Some(attempt) = &mut self.pending_append_attempt {
            attempt.authorized = true;
        }
    }

    /// Complete private publication only after the certificate proof authorizes it.
    /// The caller retains the external append owner through this entire operation.
    fn publish_reconciled_append(&mut self, cx: &Cx, synced: bool) -> Result<()> {
        if let Some(attempt) = &self.pending_append_attempt {
            if !attempt.authorized {
                return Err(FrankenError::BusyRecovery);
            }
            if attempt.generation != self.wal.generation_identity()
                || self.pending_publication_generation != Some(attempt.generation)
                || self.pending_publication_commit != attempt.end_frame_count.checked_sub(1)
                || self.pending_publication_frames.last().is_none_or(|frame| {
                    !frame.is_commit || frame.frame_index.checked_add(1) != Some(attempt.end_frame_count)
                })
            {
                return Err(FrankenError::WalCorrupt {
                    detail: "authorized WAL append lost its exact final publication marker".to_owned(),
                });
            }
        }
        if let Some(last_commit_frame) = self.pending_publication_commit {
            if synced {
                self.assert_publish_safe(cx, last_commit_frame)?;
            } else {
                self.assert_pending_horizon_matches_wal(cx, last_commit_frame)?;
            }
            self.publish_native_pending(last_commit_frame)?;
            self.publish_pending_commit_snapshot(cx, last_commit_frame, "reconciled_append");
            self.finish_native_publication();
            self.pending_publication_commit = None;
            if self.pending_publication_frames.is_empty() {
                self.pending_publication_generation = None;
            }
        }
        self.pending_append_attempt = None;
        if !self.has_pending_publication() {
            self.refresh_before_append = true;
        }
        Ok(())
    }

    /// Trim only this attempt after exact absence, tail repair, and requested sync.
    fn discard_reconciled_append(&mut self) -> Result<()> {
        let Some(attempt) = &self.pending_append_attempt else {
            return Ok(());
        };
        if attempt.authorized
            || attempt.generation != self.wal.generation_identity()
            || self.wal.frame_count() != attempt.start_frame_index
            || self.pending_publication_frames.len() < attempt.previous_pending_len
        {
            return Err(FrankenError::WalCorrupt {
                detail: "absent WAL append does not match its retained cleanup boundary".to_owned(),
            });
        }
        self.pending_publication_frames.truncate(attempt.previous_pending_len);
        self.pending_publication_commit = attempt.previous_pending_commit;
        self.pending_publication_generation = attempt.previous_pending_generation;
        self.refresh_before_append = attempt.previous_refresh_before_append;
        let attempt = self.pending_append_attempt.take().expect("retained append cleanup owner");
        self.native_publication = attempt.previous_native_publication;
        Ok(())
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
        self.assert_no_pending_append_attempt()?;
        let Some(last_commit_frame) = self.pending_publication_commit else {
            return Ok(());
        };
        self.assert_pending_horizon_matches_wal(cx, last_commit_frame)?;
        self.publish_native_pending(last_commit_frame)?;
        self.publish_pending_commit_snapshot(cx, last_commit_frame, "authorized_deferred_commit");
        self.finish_native_publication();
        self.pending_publication_commit = None;
        if self.pending_publication_frames.is_empty() {
            self.pending_publication_generation = None;
        }
        Ok(())
    }

    /// Publish the staged commit horizon after a successful durability barrier.
    ///
    /// Fully synchronous: the staged frames already carry every page/frame pair
    /// the snapshot needs, so no WAL scan — and therefore no async I/O — is
    /// required. On refusal or failure the pending state is preserved verbatim
    /// so the next successful sync retries the identical batch.
    fn publish_pending_after_sync(&mut self, cx: &Cx) -> Result<()> {
        self.assert_no_pending_append_attempt()?;
        let Some(last_commit_frame) = self.pending_publication_commit else {
            return Ok(());
        };
        self.assert_publish_safe(cx, last_commit_frame)?;
        self.publish_native_pending(last_commit_frame)?;
        self.publish_pending_commit_snapshot(cx, last_commit_frame, "sync_publish_commit");
        self.finish_native_publication();
        self.pending_publication_commit = None;
        if self.pending_publication_frames.is_empty() {
            self.pending_publication_generation = None;
        }
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
            self.pending_publication_frames
                .retain(|frame| frame.frame_index > last_commit_frame);
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
        // A batch may end with uncommitted frames after its last marker.
        // Preserve that suffix for a later commit, including an intermediate
        // sync; clearing it loses page mappings and incorrectly rearms refresh.
        self.pending_publication_frames
            .retain(|frame| frame.frame_index > last_commit_frame);

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
    fn checkpoint_recovery_pending(&self) -> bool {
        self.pending_checkpoint_reset.is_some()
    }

    fn reconcile_checkpoint_reset<'a>(&'a mut self, cx: &'a Cx) -> WalFuture<'a, ()> {
        Box::pin(async move {
            let Some(reset) = self.pending_checkpoint_reset.as_mut() else { return Ok(()); };
            if self.wal.header() != &reset.old_header && self.wal.header() != &reset.target_header {
                return Err(FrankenError::BusyRecovery);
            }
            if !reset.physical_complete {
                // The old write can outlive its dropped caller. Do not issue
                // another header write until that exact source is terminal.
                reset.completion.wait().await;
                let target = reset.target_header;
                let truncate = reset.truncate;
                let completion = VfsWriteCompletion::new();
                reset.completion = completion.clone();
                self.wal.reset_tracked(cx, target.checkpoint_seq, target.salts, truncate, completion).await?;
                let reset = self.pending_checkpoint_reset.as_mut().expect("reset owner survives physical retry");
                if self.wal.header() != &reset.target_header {
                    return Err(FrankenError::BusyRecovery);
                }
                reset.physical_complete = true;
            }
            let reset = self.pending_checkpoint_reset.as_mut().expect("reset owner survives publication retry");
            if !reset.shared_complete {
                if let Some(native) = &mut reset.native { native.publish()?; }
                reset.shared_complete = true;
            }
            self.finish_checkpoint_reset()
        })
    }

    fn native_recovery_required(&self) -> Option<WalNativeRecoveryReason> {
        self.native_recovery_requested
    }

    fn preflight_native_append<'a>(&'a mut self, cx: &'a Cx) -> WalFuture<'a, ()> {
        Box::pin(async move {
            if self.wal_index_shm_source.is_none() {
                return self.refresh_published_snapshot(cx).await.map(|_| ());
            }
            let baseline = self.native_append_baseline(cx).await?;
            let horizon = usize::try_from(baseline.mx_frame).map_err(|_| FrankenError::DatabaseFull)?
                .checked_sub(1);
            // Advance the current shared conflict horizon, while the older
            // read_snapshot and exact native token stay pinned unchanged.
            self.publish_visible_snapshot(cx, horizon, "native_append_preflight").await
        })
    }

    fn native_reader_required(&self) -> bool {
        self.wal_index_shm_source.is_some()
    }

    fn native_read_binding(&self) -> Option<WalNativeReadBinding> {
        self.native_read_binding.clone()
    }

    fn begin_native_read<'a>(
        &'a mut self,
        cx: &'a Cx,
        binding: WalNativeReadBinding,
    ) -> WalFuture<'a, WalNativeReadOutcome> {
        Box::pin(async move {
            if self.has_pending_publication() || self.native_read_binding.is_some() {
                return Err(FrankenError::BusyRecovery);
            }
            if let Some(reason) = self.native_recovery_requested {
                return Ok(WalNativeReadOutcome::RecoveryRequired(reason));
            }
            let source = self.wal_index_shm_source.as_ref().ok_or(FrankenError::Unsupported)?;
            if !source.validates_reader_binding(&binding) || binding.boundary().database_only {
                return Err(FrankenError::BusyRecovery);
            }
            let header = binding.header();
            header.validate()?;
            if header.mx_frame != binding.boundary().maximum_wal_frame {
                return Err(FrankenError::BusyRecovery);
            }
            self.wal.refresh(cx).await?;
            let wal_header = self.wal.header();
            if header.page_size()? != wal_header.page_size
                || header.big_end_cksum != u8::from(wal_header.big_endian_checksum())
                || header.a_salt != [wal_header.salts.salt1, wal_header.salts.salt2]
            {
                return Ok(WalNativeReadOutcome::RecoveryRequired(
                    WalNativeRecoveryReason::WalGenerationMismatch,
                ));
            }
            let frame_count = usize::try_from(header.mx_frame)
                .map_err(|_| FrankenError::BusyRecovery)?;
            if frame_count > self.wal.frame_count() {
                return Ok(WalNativeReadOutcome::RecoveryRequired(
                    WalNativeRecoveryReason::WalTerminalMismatch,
                ));
            }
            let terminal = match frame_count.checked_sub(1) {
                Some(index) => Some((header.mx_frame, self.wal.read_frame_header(cx, index).await?)),
                None => None,
            };
            if validate_shared_wal_index_wal_binding(&header, self.wal.header(), terminal).is_err() {
                return Ok(WalNativeReadOutcome::RecoveryRequired(
                    WalNativeRecoveryReason::WalTerminalMismatch,
                ));
            }
            self.publish_visible_snapshot(cx, frame_count.checked_sub(1), "begin_native_read").await?;
            self.read_snapshot = Some(self.published_snapshot.clone());
            self.native_read_binding = Some(binding);
            self.refresh_before_append = true;
            Ok(WalNativeReadOutcome::Ready)
        })
    }

    fn end_native_read(&mut self, token: &WalNativeReadToken) -> Result<()> {
        if let Some(binding) = &self.native_read_binding {
            if !binding.token().matches(token) {
                return Err(FrankenError::BusyRecovery);
            }
            self.read_snapshot = None;
            self.native_read_binding = None;
        }
        Ok(())
    }

    fn recover_native_read_state<'a>(
        &'a mut self,
        cx: &'a Cx,
        _reason: fsqlite_pager::traits::WalNativeRecoveryReason,
    ) -> WalFuture<'a, ()> {
        Box::pin(async move { self.recover_native_index(cx).await })
    }

    fn begin_transaction<'a>(&'a mut self, cx: &'a Cx) -> WalFuture<'a, ()> {
        Box::pin(async move {
            if self.native_read_binding.is_some() || self.native_recovery_requested.is_some() {
                return Err(FrankenError::BusyRecovery);
            }
            if self.pending_checkpoint_reset.is_some() { return Err(FrankenError::BusyRecovery); }
            // Reject at the earliest illegal transition: before `wal.refresh`,
            // before pinning `read_snapshot`, and before re-arming
            // `refresh_before_append`. Beginning a transaction on top of staged,
            // unpublished frames would otherwise leave a half-transition whose
            // only symptom is a later append failure.
            if self.has_pending_publication() {
                return Err(FrankenError::Busy);
            }
            if self.native_reader_required() {
                self.native_checkpoint_view(cx).await?;
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
            self.assert_no_pending_append_attempt()?;
            let completion = VfsWriteCompletion::new();
            let mut preflight = WalWriteCompletionPreflight::new(Some(&completion));
            self.validate_append_page(page_data)?;
            if self.refresh_before_append {
                // Refresh and synchronize the published base snapshot once before
                // the commit batch starts, then publish local frame deltas directly
                // from the append path.
                self.synchronize_publication_before_append(cx, "append_frame_pre_refresh")
                    .await?;
            }
            self.stage_append_attempt(
                cx,
                std::iter::once((page_number, db_size_if_commit)),
                completion.clone(),
            ).await?;
            let frames = [WalAppendFrameRef { page_number, page_data, db_size_if_commit }];
            preflight.hand_off();
            drop(preflight);
            self.wal.append_frames_tracked(cx, &frames, completion).await?;
            self.finish_successful_append_attempt()?;

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
            self.assert_no_pending_append_attempt()?;
            let completion = VfsWriteCompletion::new();
            let mut preflight = WalWriteCompletionPreflight::new(Some(&completion));

            if self.refresh_before_append {
                self.synchronize_publication_before_append(cx, "append_frames_pre_refresh")
                    .await?;
            }

            let mut wal_frames = Vec::with_capacity(frames.len());
            for frame in frames {
                self.validate_append_page(frame.page_data)?;
                wal_frames.push(WalAppendFrameRef {
                    page_number: frame.page_number,
                    page_data: frame.page_data,
                    db_size_if_commit: frame.db_size_if_commit,
                });
            }
            self.stage_append_attempt(
                cx,
                frames
                    .iter()
                    .map(|frame| (frame.page_number, frame.db_size_if_commit)),
                completion.clone(),
            ).await?;
            preflight.hand_off();
            drop(preflight);
            self.wal.append_frames_tracked(cx, &wal_frames, completion).await?;
            self.finish_successful_append_attempt()?;

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
            self.assert_no_pending_append_attempt()?;

            if self.refresh_before_append {
                self.synchronize_publication_before_append(cx, "append_frames_pre_refresh")
                    .await?;
            }

            let mut wal_frames = Vec::with_capacity(frames.len());
            for frame in frames {
                self.validate_append_page(frame.page_data)?;
                wal_frames.push(WalAppendFrameRef {
                    page_number: frame.page_number,
                    page_data: frame.page_data,
                    db_size_if_commit: frame.db_size_if_commit,
                });
            }
            self.stage_append_attempt(
                cx,
                frames.iter().map(|frame| (frame.page_number, frame.db_size_if_commit)),
                completion.clone(),
            ).await?;
            preflight.hand_off();
            drop(preflight);
            self.wal
                .append_frames_tracked(cx, &wal_frames, completion)
                .await?;
            self.finish_successful_append_attempt()?;

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

            Ok(())
        })
    }

    fn prepare_append_frames(
        &self,
        frames: &[WalFrameRef<'_>],
    ) -> Result<Option<PreparedWalFrameBatch>> {
        if self.pending_checkpoint_reset.is_some() { return Err(FrankenError::BusyRecovery); }
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
        if self.pending_checkpoint_reset.is_some() { return Err(FrankenError::BusyRecovery); }
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
            self.validate_prepared_frame_metadata(prepared)?;
            if prepared.frame_count() == 0 {
                return Ok(());
            }
            self.assert_no_pending_append_attempt()?;
            let completion = VfsWriteCompletion::new();
            let mut preflight = WalWriteCompletionPreflight::new(Some(&completion));

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

            let final_running_checksum = Self::finalized_running_checksum(prepared)?;
            self.validate_finalized_prepared_generation(prepared)?;
            self.stage_append_attempt(
                cx,
                prepared.frame_metas.iter()
                    .map(|frame| (frame.page_number, frame.db_size_if_commit)),
                completion.clone(),
            ).await?;
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
            self.finish_successful_append_attempt()?;

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
            self.validate_prepared_frame_metadata(prepared)?;
            if prepared.frame_count() == 0 {
                completion.complete_success();
                preflight.hand_off();
                return Ok(());
            }
            self.assert_no_pending_append_attempt()?;

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

            let final_running_checksum = Self::finalized_running_checksum(prepared)?;
            self.validate_finalized_prepared_generation(prepared)?;
            self.stage_append_attempt(
                cx,
                prepared.frame_metas.iter()
                    .map(|frame| (frame.page_number, frame.db_size_if_commit)),
                completion.clone(),
            ).await?;
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
            self.finish_successful_append_attempt()?;

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

            Ok(())
        })
    }

    fn read_page<'a>(&'a mut self, cx: &'a Cx, page_number: u32) -> WalFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            if self.pending_checkpoint_reset.is_some() { return Err(FrankenError::BusyRecovery); }
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
    // the newest published one. Under the gate the tail is stable, so one
    // header pass over `0..frame_count()` is exact — and it is done once per
    // stable tail (`AppendedTailIndex`), not once per page: the reclaim sweep
    // asks for every ledger page in turn, and a per-page backwards scan made
    // that O(ledger × frames) (cass GH #382).
    fn read_page_at_appended_tail<'a>(
        &'a mut self,
        cx: &'a Cx,
        page_number: u32,
    ) -> WalFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            self.assert_no_pending_append_attempt()?;
            let frame_count = self.wal.frame_count();
            let Some(tail_frame) = frame_count.checked_sub(1) else {
                return Ok(None);
            };
            let Some(frame_index) = self
                .appended_tail_frame_for_page(cx, page_number, tail_frame)
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
            if self.pending_checkpoint_reset.is_some() { return Err(FrankenError::BusyRecovery); }
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
            if self.pending_checkpoint_reset.is_some() { return Err(FrankenError::BusyRecovery); }
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

            self.assert_no_pending_append_attempt()?;
            if self.native_reader_required() {
                self.preflight_native_append(cx).await?;
            } else {
                self.wal.refresh(cx).await?;
                self.publish_latest_committed_snapshot(cx, "conflicting_pages_since_snapshot")
                    .await?;
            }
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
            if self.pending_checkpoint_reset.is_some() { return Err(FrankenError::BusyRecovery); }
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
        self.assert_no_pending_append_attempt()?;
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

    fn backfilled_frame_count(&self) -> usize {
        // Valid only for the current WAL generation; a reset (ours or a
        // peer's) invalidates the watermark to 0 (GH#402).
        match self.checkpoint_backfill_watermark {
            Some((tagged_generation, frames))
                if tagged_generation == self.wal.generation_identity() =>
            {
                frames as usize
            }
            _ => 0,
        }
    }

    fn validate_empty_wal_for_retirement<'a>(&'a mut self, cx: &'a Cx) -> WalFuture<'a, ()> {
        Box::pin(async move {
            if self.has_pending_publication() || self.native_read_binding.is_some() {
                return Err(FrankenError::Busy);
            }
            if self.native_recovery_requested.is_some() {
                return Err(FrankenError::BusyRecovery);
            }
            // A peer may have retired this exact physical file while this
            // idle adapter still caches the old frames. The caller owns the
            // whole-image maintenance fence; zero bytes need no WAL-header
            // refresh. Path adapters separately prove the exact path identity.
            if self.wal.file().file_size(cx)? == 0 {
                return Ok(());
            }
            // Retrying a mode change after BusyRecovery must not turn an
            // invalid native publication into permission to discard its WAL.
            if self.native_reader_required() {
                self.native_checkpoint_view(cx).await?;
            } else {
                self.wal.refresh(cx).await?;
            }
            if self.wal.frame_count() != 0 {
                return Err(FrankenError::Busy);
            }
            Ok(())
        })
    }

    fn retire_empty_wal<'a>(&'a mut self, cx: &'a Cx) -> WalFuture<'a, ()> {
        Box::pin(async move {
            self.validate_empty_wal_for_retirement(cx).await?;
            self.wal.file_mut().truncate(cx, 0)?;
            self.wal.file_mut().sync(cx, SyncFlags::FULL)?;
            self.invalidate_publication();
            Ok(())
        })
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
            if self.native_read_binding.is_some() {
                return Err(FrankenError::BusyRecovery);
            }
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
            #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
            {
                self.checkpoint_retired_salts = None;
            }
            // Native maintenance must match the shared generation and entire
            // committed prefix before any page write or physical reset.
            let native = self.native_checkpoint_view(cx).await?;
            if native.is_none() { self.wal.refresh(cx).await?; }
            self.refresh_before_append = true;
            let total_frames = u32::try_from(self.wal.frame_count()).unwrap_or(u32::MAX);

            // GH#402: resume from this adapter's durable backfill watermark
            // when it belongs to the CURRENT WAL generation (a reset — ours or
            // a peer's — changes the generation identity and invalidates it).
            // Without this, every autocheckpoint restarted at frame 0 and
            // re-read/re-compared the whole WAL, making each post-commit
            // checkpoint O(total WAL frames) instead of O(new frames).
            let generation = self.wal.generation_identity();
            let tracked_backfilled = match self.checkpoint_backfill_watermark {
                Some((tagged_generation, frames)) if tagged_generation == generation => frames,
                _ => 0,
            };
            let effective_backfilled = native.as_ref().map_or_else(
                || backfilled_frames.max(tracked_backfilled).min(total_frames),
                |view| view.backfilled_frames,
            );

            // Build checkpoint state for the planner.
            let state = CheckpointState {
                total_frames,
                backfilled_frames: effective_backfilled,
                oldest_reader_frame,
            };

            // Wrap the CheckpointPageWriter in a CheckpointTargetAdapter.
            let mut target = CheckpointResetTarget {
                delegate: CheckpointTargetAdapterRef { writer },
                pending: &mut self.pending_checkpoint_reset,
                previous_header: *self.wal.header(),
                native,
            };

            // Execute the checkpoint.
            let result =
                execute_checkpoint(cx, &mut self.wal, to_wal_mode(mode), state, &mut target)
                    .await;
            drop(target);
            let result = result?;
            if result.wal_was_reset { self.finish_checkpoint_reset()?; }

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

            // A completed reset already retired its private/FEC generation
            // through finish_checkpoint_reset, also used by reconciliation.

            // GH#402: advance (or reset) the backfill watermark. Frames
            // [effective_backfilled .. effective_backfilled + frames_backfilled)
            // are now durably represented in the database file for this
            // generation; a reset starts a fresh generation with nothing
            // backfilled.
            self.checkpoint_backfill_watermark = if result.wal_was_reset {
                None
            } else {
                Some((
                    generation,
                    effective_backfilled.saturating_add(result.frames_backfilled),
                ))
            };

            self.publish_latest_committed_snapshot(cx, "checkpoint")
                .await?;

            // GH#399: a RESTART/TRUNCATE whose reset was refused by the
            // cross-process reader gate ran as a FULL checkpoint — every safe
            // frame was backfilled but the generation survives for the peer
            // reader that still pins it. Report that downgrade so callers
            // (PRAGMA wal_checkpoint's `busy` column) can see the request was
            // not honoured yet.
            let effective_mode = if result.reset_deferred_by_readers() {
                CheckpointMode::Full
            } else {
                mode
            };

            // Stock parity (wal.c `sqlite3WalCheckpoint`: `*pnCkpt =
            // nBackfill`): the reported backfill count is CUMULATIVE for the
            // current WAL generation, not this call's new frames. With the
            // GH#402 resume watermark, a checkpoint whose frames were already
            // durably backfilled by an earlier pass copies nothing new — but
            // `PRAGMA wal_checkpoint`'s third column must still report how far
            // the database file has caught up, or every post-watermark
            // checkpoint looks like it checkpointed nothing (the GH#399
            // keepers caught exactly that). The watermark math above keeps
            // using the planner's raw per-call count.
            Ok(CheckpointResult {
                total_frames,
                frames_backfilled: effective_backfilled
                    .saturating_add(result.frames_backfilled)
                    .min(total_frames),
                completed: result.plan.completes_checkpoint(),
                wal_was_reset: result.wal_was_reset,
                requested_mode: mode,
                effective_mode,
            })
        })
    }
}

const MIN_DURABLE_CERTIFICATE_RECORD_SIZE: usize =
    ParallelWalDurableCertificateRecord::MIN_ENCODED_SIZE;
const DURABLE_CERTIFICATE_RECORD_HEADER_SIZE: usize = 14;
/// Smallest byte length a footer may declare for a record that could still be
/// a well-formed LEGACY envelope (GH#372): the fixed header plus the length
/// footer. Envelopes written by older releases are shorter than this build's
/// [`MIN_DURABLE_CERTIFICATE_RECORD_SIZE`], so the legacy gates must admit
/// footer lengths below the current minimum before classifying the bytes.
const MIN_LEGACY_DURABLE_CERTIFICATE_RECORD_SIZE: usize = DURABLE_CERTIFICATE_RECORD_HEADER_SIZE
    + ParallelWalDurableCertificateRecord::LENGTH_FOOTER_SIZE;
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

/// Envelope version declared by a durable-certificate record that begins with
/// the sidecar magic, when the header is long enough to carry one.
fn durable_certificate_declared_version(bytes: &[u8]) -> Option<u16> {
    if !bytes.starts_with(&PARALLEL_WAL_DURABLE_CERTIFICATE_MAGIC) {
        return None;
    }
    bytes
        .get(8..10)
        .map(|version_bytes| u16::from_le_bytes([version_bytes[0], version_bytes[1]]))
}

/// True when `bytes` begins with a well-formed durable-certificate envelope
/// header (correct magic) that declares a LEGACY record version — one an
/// older release wrote and this build recognizes but can no longer decode: v3
/// (the identity-less envelope before bd-85x9y / GH#364) and v2 (the original
/// envelope, which also predates the ordered-frame payload digest).
///
/// Such a record is not corruption: it is a durable proof this build simply
/// cannot honor, so every load gate treats it as an ABSENT certificate
/// (conservative WAL recovery) and the append path discards it, instead of
/// failing the open closed with `WalCorrupt` (GH#372). Any *other* non-current
/// version (a genuine future format, or a corrupted version field) is
/// deliberately NOT matched here — it falls through to strict decoding, which
/// classifies it as corruption exactly as before.
///
/// Accepted trade-off (bd-kt80v): a corrupted CURRENT-version record whose
/// version field happens to read as v2/v3 (a 1–2 bit flip) classifies as
/// legacy and reads as absent rather than `WalCorrupt`. That mirrors stock
/// SQLite's unverifiable-tail-ends-the-log recovery philosophy: the cost is
/// conservative recovery for the newest batch, never accepting corrupt data;
/// and the walk-back monotonicity means a flipped OLD record cannot mask any
/// newer, verifiable record above it.
fn durable_certificate_is_legacy_envelope(bytes: &[u8]) -> bool {
    durable_certificate_declared_version(bytes)
        .is_some_and(durable_certificate_record_version_is_legacy)
}

/// True when `bytes` is a complete legacy envelope whose header agrees with the
/// footer-derived `record_len` — the evidence the recovery scans require before
/// treating footer-addressed bytes as a legacy record rather than garbage.
fn durable_certificate_is_complete_legacy_envelope(bytes: &[u8], record_len: usize) -> bool {
    durable_certificate_is_legacy_envelope(bytes)
        && durable_certificate_declares_len(bytes, record_len)
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
    if durable_certificate_record_version_is_legacy(version) {
        // GH#372: a sidecar written entirely by an older release lands here
        // when its (shorter) records fall below this build's minimum record
        // size, so no footer-derived candidate anchors. That is a legacy proof
        // this build cannot honor — an absent certificate — not a torn or
        // corrupt suffix.
        tracing::debug!(
            target: "fsqlite::wal::durability_combiner",
            legacy_record_version = version,
            "ignored durable certificate sidecar suffix from a legacy record version"
        );
        return Ok(());
    }
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
    /// Cached read-only descriptor for the per-commit durable-certificate read
    /// in [`Self::latest_authorized_durable_certificate_record`] (bd-smxhz).
    ///
    /// That read runs on the pinned-read and checkpoint paths and previously
    /// re-opened/closed the `-wal-cert` sidecar every call — part of the
    /// per-commit file-open storm that serializes writers under disk contention.
    /// Holding the descriptor is safe: the read is `READONLY` (no lock), the
    /// certificate is only ever appended/truncated *in place* within a WAL
    /// generation (never unlinked+recreated), so re-reading `file_size` each
    /// call observes peer appends live; and the sidecar is reset only at a
    /// generation change, where [`Self::replace_inner`] (and `checkpoint`)
    /// invalidate this cache. A stale cross-generation read is additionally
    /// caught by the callers' `record.wal_generation` check (fail-closed). Any
    /// read anomaly drops the descriptor, forcing a fresh open. Interior-mutable
    /// because the read path is `&self` (a `WalBackend` read-snapshot method);
    /// the lock is held only for the sync take/put-back, never across `.await`.
    cached_certificate_read: std::sync::Mutex<Option<V::File>>,
    /// Creation-stable identity of the main database file (page-1 header bytes
    /// 76..92), captured once from the already-held verification descriptor the
    /// first time a WAL operation runs against this adapter (bd-85x9y / GH#364).
    ///
    /// `None` means "not yet captured" (the very first probe has not run, or a
    /// transient read failure left it unset — it is retried on the next call).
    /// `Some([0u8; 16])` means the database is legacy/pre-identity (unstamped).
    /// `Some(id)` with a non-zero `id` positively identifies this physical
    /// database, which lets the certificate load gates reject a stale sidecar
    /// left behind across a database-*file* replacement. It is captured with the
    /// same `cached_verification_db` descriptor used by
    /// [`Self::conflicts_after_generation_change`] rather than a fresh per-call
    /// main-db open, which would release this process's fcntl locks (bd-qduu1).
    db_file_identity: Option<[u8; 16]>,
    /// Retired generations whose sidecar cleanup must retry on a later
    /// checkpoint after contention or I/O failure. This survives inner WAL
    /// replacement; repair workers must separately reject late retired jobs.
    #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
    pending_fec_reclamation: Vec<WalSalts>,
    #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
    fec_producer: Option<fsqlite_wal::wal_fec::WalFecRepairProducer>,
    /// Last admitted durable boundary, including the trusted checksum anchor.
    #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
    fec_admitted: Option<(WalHeader, u32, fsqlite_wal::SqliteWalChecksum)>,
    #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
    fec_inspected_generation: Option<WalHeader>,
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
            cached_certificate_read: std::sync::Mutex::new(None),
            db_file_identity: None,
            #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
            pending_fec_reclamation: Vec::new(),
            #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
            fec_producer: None,
            #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
            fec_admitted: None,
            #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
            fec_inspected_generation: None,
            inner: WalBackendAdapter::new(wal),
        }
    }

    #[must_use]
    pub fn into_inner(self) -> WalBackendAdapter<V::File> {
        self.inner
    }

    /// Attach the inner adapter to an initialized index on the exact main file.
    ///
    /// The caller owns the external WRITE interval and its reconciliation;
    /// see [`WalBackendAdapter::attach_wal_index_shm_source`] for prerequisites.
    #[cfg(all(feature = "native", unix))]
    pub fn attach_wal_index_shm_source(
        &mut self,
        source: Arc<WalIndexShmSource<V::File>>,
    ) -> Result<()> {
        self.inner.attach_wal_index_shm_source(source)
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
    fn pending_fec_range(&mut self, cx: &Cx) -> Result<Option<fsqlite_wal::wal_fec::WalFecCommittedRange>> {
        let Some(producer) = &self.fec_producer else { return Ok(None) };
        let wal = &mut self.inner.wal;
        let end = wal.frame_count();
        // A sync of an unfinished transaction is not a repairable commit. The
        // next commit interval still starts at the last admitted commit marker.
        if end == 0 || wal.last_commit_frame(cx)? != end.checked_sub(1) {
            return Ok(None);
        }
        let header = WalHeader::from_bytes(&wal.header().to_bytes()?)?;
        let end_frame_no = u32::try_from(end).map_err(|_| FrankenError::DatabaseFull)?;
        let (start, previous_checksum) = match self.fec_admitted {
            Some((generation, count, checksum)) if generation == header => (count, checksum),
            _ => (0, header.checksum),
        };
        if start >= end_frame_no {
            return Ok(None);
        }
        Ok(Some(fsqlite_wal::wal_fec::WalFecCommittedRange {
            wal_path: self.wal_path.clone(), header,
            start_frame_no: start + 1, end_frame_no,
            previous_checksum, end_checksum: wal.running_checksum(),
            repair_symbols: producer.repair_symbols(),
        }))
    }

    /// Shared durability boundary for ordinary publication and in-doubt
    /// reconciliation. Recovery sync leaves staged metadata intact until its
    /// exact certificate proof permits the separate publication step.
    fn sync_with_fec(&mut self, cx: &Cx, publish_pending: bool) -> Result<()> {
        if publish_pending {
            self.inner.assert_no_pending_append_attempt()?;
        }
        #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
        let range = self.pending_fec_range(cx)?;
        #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
        let producer = self.fec_producer.clone();
        #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
        let permit = if range.as_ref().is_some_and(|range| range.repair_symbols != 0) {
            producer.as_ref().map(|producer| producer.try_reserve()).transpose()?
        } else {
            None
        };
        let result = if publish_pending {
            self.inner.sync(cx)
        } else {
            self.inner.wal.sync(cx, SyncFlags::NORMAL)
        };
        // Publication can fail after fsync. That does not undo the durable
        // bytes, so submit their descriptor even when publication must retry.
        #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
        if let Some(range) = range
            && self.inner.wal.last_fsynced_frame_count() >= range.end_frame_no as usize
        {
            let boundary = (range.header, range.end_frame_no, range.end_checksum);
            let submitted = permit.is_none_or(|permit| permit.submit(range));
            if submitted {
                self.fec_admitted = Some(boundary);
            }
        }
        result
    }

    /// Swap in a replacement WAL, discarding the previous adapter.
    ///
    /// Fails closed while the outgoing adapter still holds a staged batch: the
    /// replacement would consume away the pending metadata, and the freshly
    /// wrapped adapter would republish those frames from the WAL without knowing
    /// they were never fsynced (GH #187). A successful sync must drain the batch
    /// before a path-visible replacement can proceed.
    fn replace_inner(&mut self, cx: &Cx, wal: WalFile<V::File>) -> Result<()> {
        if self.inner.has_pending_publication() || self.inner.native_read_binding.is_some() {
            let cleanup_cx = cx.create_child();
            let _cleanup_mask = cleanup_cx.masked();
            let _ = wal.close(&cleanup_cx);
            return Err(FrankenError::Busy);
        }
        let mut replacement = WalBackendAdapter::new(wal);
        replacement.wal_index_shm_source.clone_from(&self.inner.wal_index_shm_source);
        replacement.native_recovery_requested = self.inner.native_recovery_requested;
        let old = std::mem::replace(&mut self.inner, replacement);
        // bd-smxhz: the WAL generation changed, so the -wal-cert sidecar is
        // reset for the new generation and the cached certificate descriptor is
        // stale — drop it so the next read re-opens.
        if let Some(mut stale) = self
            .cached_certificate_read
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = stale.close(cx);
        }
        // The pre-replacement guard above proved this adapter has no pending
        // owner; no suspension occurs between that guard and the swap.
        let old_wal = old.wal;
        let _ = old_wal.close(cx);
        Ok(())
    }

    async fn create_replacement_wal(&self, cx: &Cx) -> Result<WalFile<V::File>> {
        if self.inner.has_pending_publication() || self.inner.native_read_binding.is_some() {
            return Err(FrankenError::Busy);
        }
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
        if let (Some(path_identity), Some(current_identity)) = (
            path_file.file_identity()?, self.inner.wal.file().file_identity()?,
        ) && path_identity != current_identity {
            return Ok(false);
        }
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
    /// Capture the main database file's creation-stable identity once
    /// (bd-85x9y / GH#364).
    ///
    /// Reads page-1 header bytes 76..92 through the held `cached_verification_db`
    /// descriptor (opening and retaining it if the cache is cold), never a
    /// fresh per-call main-db open, so it cannot release this process's fcntl
    /// locks (bd-qduu1). The descriptor is read-only and retained for reuse. A
    /// transient failure leaves `db_file_identity` as `None` so the next call
    /// retries; a successful read stores the identity even when it is all-zero
    /// (a legacy/pre-identity database).
    async fn ensure_db_file_identity_captured(&mut self, cx: &Cx) {
        if self.db_file_identity.is_some() {
            return;
        }
        let page_size = match usize::try_from(self.page_size) {
            Ok(size) if size >= 92 => size,
            _ => return,
        };
        let db_file = match self.cached_verification_db.take() {
            Some(cached) => cached,
            None => {
                let main_db_flags = VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB;
                match self.vfs.open(cx, Some(&self.db_path), main_db_flags) {
                    Ok((file, _)) => file,
                    // No readable main-db file yet: leave identity unknown and
                    // retry on a later call. The gates stay lenient meanwhile.
                    Err(_) => return,
                }
            }
        };
        let mut page_one = vec![0_u8; page_size];
        match db_file.read(cx, &mut page_one, 0).await {
            Ok(bytes_read) if bytes_read >= 92 => {
                let mut id = [0_u8; 16];
                id.copy_from_slice(&page_one[76..92]);
                self.db_file_identity = Some(id);
            }
            // Short read / error: leave unknown, retry next call.
            _ => {}
        }
        // Retain the descriptor for reuse (read-only, no lock impact — bd-smxhz).
        self.cached_verification_db = Some(db_file);
    }

    /// True only when this adapter positively knows the database file's
    /// non-zero creation-stable identity and the certificate record was bound to
    /// that same identity (bd-85x9y / GH#364).
    ///
    /// An unknown (`None`) or legacy-zero adapter identity, or an all-zero
    /// record identity, is never a match.
    fn db_file_identity_matches(&self, record: &ParallelWalDurableCertificateRecord) -> bool {
        match self.db_file_identity {
            Some(id) if id != [0u8; 16] => {
                record.db_file_id != [0u8; 16] && record.db_file_id == id
            }
            _ => false,
        }
    }

    /// True only when this adapter positively knows the database file's
    /// non-zero identity AND the certificate record is bound to a *different*
    /// identity — the signature of a stale/foreign sidecar left behind across a
    /// database-*file* replacement (bd-85x9y / GH#364). Such a record must be
    /// treated as absent.
    ///
    /// Returns false — do NOT reject — whenever the identity is indeterminate:
    /// the adapter identity is unknown (capture has not yet succeeded) or
    /// legacy-zero (a database created before identities were stamped). In those
    /// cases identity cannot condemn the record, so the existing WAL
    /// generation / frame-interval / payload-digest checks remain the sole
    /// authority and a valid SAME-FILE certificate is never dropped. This is the
    /// durability guardrail: rejection fires only on a positive cross-identity
    /// conflict, never on uncertainty.
    fn db_file_identity_rejects_record(
        &self,
        record: &ParallelWalDurableCertificateRecord,
    ) -> bool {
        // Reject only when identity is positively known (non-zero) AND the
        // record does not match it. When identity is unknown/legacy the first
        // guard is false, so no record is ever rejected on identity grounds.
        matches!(self.db_file_identity, Some(id) if id != [0u8; 16])
            && !self.db_file_identity_matches(record)
    }

    /// The identity to stamp into a certificate record written for this
    /// database (bd-85x9y / GH#364): the captured non-zero identity, or the
    /// all-zero legacy sentinel when identity is unknown/legacy so a certificate
    /// authored before capture never claims a foreign identity.
    fn db_file_id_for_written_certificate(&self) -> [u8; 16] {
        self.db_file_identity.unwrap_or_default()
    }

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

    /// Read admission never creates or truncates a WAL to satisfy its binding.
    async fn ensure_current_wal_path_for_native_read(
        &mut self, cx: &Cx,
    ) -> Result<WalNativeReadOutcome> {
        if self.inner.has_pending_publication() || self.inner.native_read_binding.is_some() {
            return Err(FrankenError::BusyRecovery);
        }
        #[cfg(all(feature = "native", any(unix, windows)))]
        if let Some(binding) = &self.namespace_binding {
            binding.validate_path_identity()?;
        }
        self.ensure_db_file_identity_captured(cx).await;
        let opened = self.vfs.open(cx, Some(&self.wal_path), VfsOpenFlags::READWRITE | VfsOpenFlags::WAL);
        let opened = match opened {
            Err(_) if !self.create_missing => {
                self.vfs.open(cx, Some(&self.wal_path), VfsOpenFlags::READONLY | VfsOpenFlags::WAL)
            }
            result => result,
        };
        let (mut file, _) = match opened {
            Ok(opened) => opened,
            Err(FrankenError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(WalNativeReadOutcome::RecoveryRequired(WalNativeRecoveryReason::WalGenerationMismatch));
            }
            Err(error) => return Err(error),
        };
        let size = match file.file_size(cx) {
            Ok(size) => size,
            Err(error) => { let _ = file.close(cx); return Err(error); }
        };
        if size < u64::try_from(WAL_HEADER_SIZE).expect("WAL header size fits u64") {
            file.close(cx)?;
            return Ok(WalNativeReadOutcome::RecoveryRequired(WalNativeRecoveryReason::WalGenerationMismatch));
        }
        let same = self.path_header_matches_current_handle(cx, &file).await;
        match same {
            Ok(true) => file.close(cx)?,
            Ok(false) => {
                let wal = self.open_replacement_wal(cx, file).await?;
                self.replace_inner(cx, wal)?;
            }
            Err(error) => { let _ = file.close(cx); return Err(error); }
        }
        Ok(WalNativeReadOutcome::Ready)
    }

    /// Validate an attached native descriptor without creating or replacing it.
    /// Existing-path rebinding belongs to fresh read/recovery admission only.
    async fn validate_current_native_wal_path(&mut self, cx: &Cx) -> Result<()> {
        #[cfg(all(feature = "native", any(unix, windows)))]
        if let Some(binding) = &self.namespace_binding { binding.validate_path_identity()?; }
        self.ensure_db_file_identity_captured(cx).await;
        let (mut file, _) = self.vfs.open(cx, Some(&self.wal_path), VfsOpenFlags::READWRITE | VfsOpenFlags::WAL)?;
        let validation = self.path_header_matches_current_handle(cx, &file).await;
        let cleanup_cx = cx.create_child();
        let _cleanup_mask = cleanup_cx.masked();
        let close = file.close(&cleanup_cx);
        match (validation, close) {
            (Ok(true), Ok(())) => Ok(()),
            (Ok(false), Ok(())) => Err(FrankenError::BusyRecovery),
            (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
            (Err(validation), Err(close)) => Err(FrankenError::internal(format!(
                "native WAL path validation and close failed: validation={validation}; close={close}"
            ))),
        }
    }

    /// Recheck an already-retired native WAL only for whole-image retirement.
    /// This never creates, replaces, or rebinds a WAL descriptor.
    async fn validate_native_wal_retirement_path(&mut self, cx: &Cx) -> Result<()> {
        if self.inner.has_pending_publication() || self.inner.native_read_binding.is_some() {
            return Err(FrankenError::Busy);
        }
        if self.inner.native_recovery_requested.is_some() {
            return Err(FrankenError::BusyRecovery);
        }
        #[cfg(all(feature = "native", any(unix, windows)))]
        if let Some(binding) = &self.namespace_binding {
            binding.validate_path_identity()?;
        }
        let (mut file, _) = self.vfs.open(
            cx,
            Some(&self.wal_path),
            VfsOpenFlags::READWRITE | VfsOpenFlags::WAL,
        )?;
        let validation = async {
            let current = self.inner.wal.file();
            if file.file_size(cx)? == 0 && current.file_size(cx)? == 0 {
                match (file.file_identity()?, current.file_identity()?) {
                    (Some(path_identity), Some(current_identity))
                        if path_identity == current_identity => {}
                    _ => return Err(FrankenError::BusyRecovery),
                }
                // Zero bytes alone do not prove a peer completed its mode
                // transition. Read the persisted rollback format through a
                // retained VFS descriptor under the caller's whole-image
                // fence; never admit unexplained truncation in WAL mode.
                if self.cached_verification_db.is_none() {
                    let (main, _) = self.vfs.open(
                        cx,
                        Some(&self.db_path),
                        VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB,
                    )?;
                    self.cached_verification_db = Some(main);
                }
                let main = self
                    .cached_verification_db
                    .as_ref()
                    .ok_or(FrankenError::BusyRecovery)?;
                #[cfg(all(feature = "native", any(unix, windows)))]
                if let Some(binding) = &self.namespace_binding
                    && main.file_identity()? != Some(binding.identity())
                {
                    return Err(FrankenError::BusyRecovery);
                }
                let mut bytes = [0_u8; fsqlite_types::DATABASE_HEADER_SIZE];
                if main.read(cx, &mut bytes, 0).await? != bytes.len() {
                    return Err(FrankenError::BusyRecovery);
                }
                let header = fsqlite_types::DatabaseHeader::from_bytes(&bytes)
                    .map_err(|_| FrankenError::BusyRecovery)?;
                if header.read_version != 1
                    || header.write_version != 1
                    || header.page_size.get() != self.page_size
                {
                    return Err(FrankenError::BusyRecovery);
                }
                #[cfg(all(feature = "native", any(unix, windows)))]
                if let Some(binding) = &self.namespace_binding {
                    binding.validate_path_identity()?;
                }
                return Ok(());
            }
            if self.path_header_matches_current_handle(cx, &file).await? {
                Ok(())
            } else {
                Err(FrankenError::BusyRecovery)
            }
        }
        .await;
        let cleanup_cx = cx.create_child();
        let _cleanup_mask = cleanup_cx.masked();
        let close = file.close(&cleanup_cx);
        match (validation, close) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(validation), Err(close)) => Err(FrankenError::internal(format!(
                "native WAL retirement path validation and close failed: validation={validation}; close={close}"
            ))),
        }
    }

    async fn ensure_current_wal_path(&mut self, cx: &Cx) -> Result<()> {
        if self.inner.native_reader_required() {
            return self.validate_current_native_wal_path(cx).await;
        }
        #[cfg(all(feature = "native", any(unix, windows)))]
        if let Some(binding) = &self.namespace_binding {
            binding.validate_path_identity()?;
        }
        // bd-85x9y / GH#364: capture the database file's creation-stable identity
        // once, before any certificate read/write, so the load gates can reject a
        // stale foreign certificate. Lazy + retried, so it is harmless if the
        // main-db page 1 is not yet readable on the very first probe.
        self.ensure_db_file_identity_captured(cx).await;
        if !self.vfs.access(cx, &self.wal_path, AccessFlags::EXISTS)? {
            if self.inner.has_pending_publication() {
                return Err(FrankenError::Busy);
            }
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
            if self.inner.has_pending_publication() {
                return Err(FrankenError::Busy);
            }
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
            if self.inner.has_pending_publication() {
                let _ = path_file.close(cx);
                return Err(FrankenError::Busy);
            }
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
    ) -> Result<Option<(u64, ParallelWalDurableCertificateRecord)>> {
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
        let invalid_record_length = || FrankenError::WalCorrupt {
            detail: format!(
                "parallel WAL certificate footer declares invalid record length {record_len}"
            ),
        };
        let current_length = (MIN_DURABLE_CERTIFICATE_RECORD_SIZE
            ..=PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE)
            .contains(&record_len);
        // GH#372: a record written by an older release is shorter than this
        // build's minimum, so a footer length below it is still a candidate
        // legacy envelope — read it and classify the bytes before deciding.
        let legacy_length_only = !current_length
            && (MIN_LEGACY_DURABLE_CERTIFICATE_RECORD_SIZE
                ..=PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE)
                .contains(&record_len);
        if !current_length && !legacy_length_only {
            return Err(invalid_record_length());
        }
        let record_len_u64 = u64::try_from(record_len).map_err(|_| FrankenError::WalCorrupt {
            detail: "parallel WAL certificate record length exceeds u64".to_owned(),
        })?;
        let Some(record_start) = record_end.checked_sub(record_len_u64) else {
            if legacy_length_only {
                return Err(invalid_record_length());
            }
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "parallel WAL certificate record length {record_len} exceeds end offset {record_end}"
                ),
            });
        };
        let bytes =
            Self::read_certificate_sidecar_exact(file, cx, record_start, record_len, "record")
                .await?;
        if durable_certificate_is_complete_legacy_envelope(&bytes, record_len) {
            // A legacy record — and, because the sidecar is append-only and
            // envelope versions only move forward, every record below it too.
            // Nothing older can authorize this WAL: report it as absent.
            tracing::debug!(
                target: "fsqlite::wal::durability_combiner",
                record_start,
                legacy_record_version = durable_certificate_declared_version(&bytes),
                "ignored durable certificate sidecar record from a legacy record version"
            );
            return Ok(None);
        }
        if legacy_length_only {
            return Err(invalid_record_length());
        }
        let record = decode_durable_certificate_record(&bytes, "record")?;
        Ok(Some((record_start, record)))
    }

    /// GH#372: the newest record — and therefore the whole append-only sidecar
    /// — was written by an older release whose envelope this build cannot
    /// honor. Recovery already reads it as absent; before the first append
    /// from this build, drop it so the sidecar never mixes envelope versions.
    ///
    /// Crash-ordering (bd-kt80v): this truncate-to-0 is deliberately NOT
    /// synced before the caller writes the new record at offset 0 and then
    /// `durable_sync`s. Under ordered metadata journaling a crash can replay
    /// the file size only as {old, 0, new-record-len} — never new bytes under
    /// the OLD size — so the recovery walk-back can never observe a fresh
    /// current-version record sitting below a stale legacy tail (which the
    /// legacy-stops-walk-back rule would mask as absent). On a filesystem
    /// without that ordering the worst case is an absent certificate, i.e.
    /// conservative recovery — a durability haircut on the newest batch,
    /// never an integrity fault. This window exists only on the first append
    /// after an upgrade over a legacy sidecar.
    fn discard_legacy_certificate_sidecar(
        file: &mut V::File,
        cx: &Cx,
        file_size: u64,
        newest_record: &[u8],
    ) -> Result<u64> {
        tracing::info!(
            target: "fsqlite::wal::durability_combiner",
            file_size,
            legacy_record_version = durable_certificate_declared_version(newest_record),
            "discarding durable certificate sidecar written by a legacy record version before appending"
        );
        file.truncate(cx, 0)?;
        Ok(0)
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
            // GH#372: admit legacy-length footers too, so a pre-upgrade sidecar
            // is recognized (and discarded) instead of read as corruption.
            if (MIN_LEGACY_DURABLE_CERTIFICATE_RECORD_SIZE
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
                if durable_certificate_is_complete_legacy_envelope(&bytes, record_len) {
                    return Self::discard_legacy_certificate_sidecar(file, cx, file_size, &bytes);
                }
                if record_len >= MIN_DURABLE_CERTIFICATE_RECORD_SIZE
                    && (bytes.starts_with(&PARALLEL_WAL_DURABLE_CERTIFICATE_MAGIC)
                        || durable_certificate_declares_len(&bytes, record_len))
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
        let mut legacy_record_start = None;
        for candidate_end in (minimum_candidate_end..tail.len()).rev() {
            let footer_start =
                candidate_end - ParallelWalDurableCertificateRecord::LENGTH_FOOTER_SIZE;
            let footer = &tail[footer_start..candidate_end];
            let record_len = usize::try_from(u32::from_le_bytes([
                footer[0], footer[1], footer[2], footer[3],
            ]))
            .unwrap_or(usize::MAX);
            if !(MIN_LEGACY_DURABLE_CERTIFICATE_RECORD_SIZE
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
            // GH#372: a legacy envelope at an exact footer boundary means the
            // whole sidecar predates this build (envelope versions only move
            // forward in an append-only file) — discard it below.
            if durable_certificate_is_legacy_envelope(record_bytes) {
                legacy_record_start = Some(record_start);
                break;
            }
            if record_len >= MIN_DURABLE_CERTIFICATE_RECORD_SIZE
                && ParallelWalDurableCertificateRecord::from_bytes(record_bytes).is_ok()
            {
                anchor_end = Some(candidate_end);
                break;
            }
        }
        if let Some(record_start) = legacy_record_start {
            return Self::discard_legacy_certificate_sidecar(
                file,
                cx,
                file_size,
                &tail[record_start..],
            );
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
            // bd-85x9y / GH#364: bind the certificate to this database's
            // creation-stable identity so a reopen after a file replacement can
            // recognize it as foreign.
            self.db_file_id_for_written_certificate(),
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

    async fn reconcile_absent_append(
        &mut self,
        cx: &Cx,
        expected_record: &ParallelWalDurableCertificateRecord,
        valid_frame_count: u64,
        sync: bool,
    ) -> Result<ParallelWalCommitReconciliation> {
        let start = expected_record.wal_frame_start;
        let end = expected_record.wal_frame_end;
        let prefix = start.checked_sub(1).ok_or_else(|| FrankenError::WalCorrupt {
            detail: "parallel WAL recovery interval starts at frame zero".to_owned(),
        })?;
        if valid_frame_count != prefix {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "in-doubt WAL interval {start}..={end} has unexpected committed prefix {valid_frame_count}"
                ),
            });
        }
        if self.inner.pending_append_attempt.as_ref().is_some_and(|attempt| attempt.authorized) {
            return Err(FrankenError::WalCorrupt {
                detail: "previously authorized WAL append disappeared before publication".to_owned(),
            });
        }
        // Exact absence precedes every sidecar/tail mutation. Errors preserve
        // the candidate metadata and owner for the same reconciliation retry.
        self.reconcile_certificate_sidecar_record(cx, expected_record, true, sync).await?;
        self.inner.wal.repair_uncommitted_tail(cx)?;
        if sync {
            self.sync_with_fec(cx, false)?;
            self.vfs.sync_parent_directory(cx, &self.wal_path)?;
        }
        self.inner.discard_reconciled_append()?;
        Ok(ParallelWalCommitReconciliation::NotCommitted)
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
                Self::read_certificate_record_ending_at(&file, cx, safe_end).await?
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
            // bd-85x9y / GH#364 + GH#372: a handoff written by a legacy build
            // (the pre-identity v3 envelope, or the original v2 one) is an
            // absent certificate, not corruption — recovery must not fail
            // closed on it. Any other bad version still decodes strictly below
            // and is caught as corruption.
            if durable_certificate_is_legacy_envelope(&bytes) {
                tracing::debug!(
                    target: "fsqlite::wal::durability_combiner",
                    legacy_record_version = durable_certificate_declared_version(&bytes),
                    "ignored checkpoint handoff certificate from a legacy record version"
                );
                return Ok(None);
            }
            let record =
                ParallelWalDurableCertificateRecord::from_bytes(&bytes).map_err(|error| {
                    FrankenError::WalCorrupt {
                        detail: format!(
                            "parallel WAL checkpoint certificate handoff is invalid: {error}"
                        ),
                    }
                })?;
            // bd-85x9y / GH#364 primary gate: a handoff certificate bound to a
            // DIFFERENT database identity is a stale sidecar left behind across a
            // database-file replacement. Treat it as absent so it cannot
            // re-extend a fresh, smaller database to the replaced file's
            // committed page count. Rejection fires only on a positive
            // cross-identity conflict, never when identity is unknown/legacy, so
            // a valid same-file certificate is preserved (the durability
            // guardrail).
            if self.db_file_identity_rejects_record(&record) {
                tracing::debug!(
                    target: "fsqlite::wal::durability_combiner",
                    "ignored checkpoint handoff certificate bound to a foreign database identity"
                );
                return Ok(None);
            }
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
        read_horizon: Option<WalPublicationSnapshot>,
    ) -> Result<Option<ParallelWalDurableCertificateRecord>> {
        let certificate_path = self.certificate_sidecar_path();
        // bd-smxhz: reuse a held read-only descriptor when the cache is warm;
        // only consult the path (access + open) on a cold cache. Within a WAL
        // generation the sidecar is appended/truncated in place, so a held
        // descriptor observes changes on re-read below; generation resets
        // invalidate the cache via replace_inner / checkpoint.
        // Release the cached_certificate_read lock BEFORE the match: neither arm
        // reads the cache, and the None arm performs blocking VFS access/open, so
        // holding the MutexGuard across the match would needlessly serialize
        // certificate readers across that I/O (clippy::significant_drop_in_scrutinee).
        let cached_reader = self
            .cached_certificate_read
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let mut file = match cached_reader {
            Some(cached) => cached,
            None => {
                if !self
                    .vfs
                    .access(cx, &certificate_path, AccessFlags::EXISTS)?
                {
                    return Ok(None);
                }
                let flags = VfsOpenFlags::READONLY | VfsOpenFlags::WAL;
                let (file, _) = self.vfs.open(cx, Some(&certificate_path), flags)?;
                file
            }
        };
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
                // GH#372: admit legacy-length footers too — an older release's
                // records are shorter than this build's minimum — so the bytes
                // are classified before the length alone rules them out.
                if (MIN_LEGACY_DURABLE_CERTIFICATE_RECORD_SIZE
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
                    // bd-85x9y / GH#364 + GH#372: a well-formed record from a
                    // legacy build (the pre-identity v3 envelope, or the
                    // original v2 one) is an absent certificate, not corruption.
                    // Return None so recovery does not fail closed on a sidecar
                    // left behind by a prior release. Any other bad version
                    // decodes strictly below and is caught as corruption.
                    if durable_certificate_is_complete_legacy_envelope(&bytes, record_len) {
                        tracing::debug!(
                            target: "fsqlite::wal::durability_combiner",
                            legacy_record_version = durable_certificate_declared_version(&bytes),
                            "ignored durable certificate sidecar from a legacy record version"
                        );
                        return Ok(None);
                    }
                    // A matching magic or self-declared length makes this a
                    // fully-present envelope candidate. Strict decoding is
                    // mandatory even when its magic/version/CRC/footer is
                    // corrupt; complete corruption is never a torn suffix.
                    if record_len >= MIN_DURABLE_CERTIFICATE_RECORD_SIZE
                        && (bytes.starts_with(&PARALLEL_WAL_DURABLE_CERTIFICATE_MAGIC)
                            || durable_certificate_declares_len(&bytes, record_len))
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
                    if !(MIN_LEGACY_DURABLE_CERTIFICATE_RECORD_SIZE
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
                    // GH#372: a legacy envelope at an exact footer boundary
                    // means the whole sidecar predates this build — absent.
                    if durable_certificate_is_legacy_envelope(record_bytes) {
                        tracing::debug!(
                            target: "fsqlite::wal::durability_combiner",
                            legacy_record_version =
                                durable_certificate_declared_version(record_bytes),
                            "ignored durable certificate sidecar from a legacy record version"
                        );
                        return Ok(None);
                    }
                    if record_len >= MIN_DURABLE_CERTIFICATE_RECORD_SIZE
                        && let Ok(record) =
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

            let physical_frame_count = u64::try_from(self.inner.frame_count()).unwrap_or(u64::MAX);
            let wal_generation = self.inner.inner().generation_identity();
            if read_horizon.is_some_and(|horizon| horizon.generation != wal_generation) {
                return Ok(None);
            }
            let valid_frame_count = read_horizon.map_or(physical_frame_count, |horizon| {
                horizon.last_commit_frame.map_or(0, |end| {
                    u64::try_from(end).unwrap_or(u64::MAX).saturating_add(1)
                }).min(physical_frame_count)
            });
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
                // bd-85x9y / GH#364 defense-in-depth: a record bound to a
                // DIFFERENT database identity means the whole -wal-cert sidecar
                // belongs to a replaced file. No record in it can authorize this
                // database, so treat the certificate as absent. Fires only on a
                // positive cross-identity conflict (never on unknown/legacy
                // identity), preserving a valid same-file certificate.
                if self.db_file_identity_rejects_record(&record) {
                    tracing::debug!(
                        target: "fsqlite::wal::durability_combiner",
                        "ignored durable certificate sidecar bound to a foreign database identity"
                    );
                    return Ok(None);
                }
                let frame_index =
                    usize::try_from(record.wal_frame_end.saturating_sub(1)).map_err(|_| {
                        FrankenError::WalCorrupt {
                            detail: "parallel WAL certificate commit-marker index exceeds usize"
                                .to_owned(),
                        }
                    })?;
                let commit_marker_frame = if record.wal_frame_end <= valid_frame_count
                    && frame_index < self.inner.frame_count()
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
                // GH#372: `None` means the record below is a legacy envelope
                // from an older release; nothing older can authorize this WAL.
                let Some(previous) =
                    Self::read_certificate_record_ending_at(&file, cx, record_start).await?
                else {
                    return Ok(None);
                };
                (record_start, record) = previous;
            }
        }
        .await;
        match read_result {
            // Success: retain the descriptor for the next call rather than
            // closing it. The read is read-only, so deferring the close (to
            // Drop / generation-change invalidation) has no durability impact
            // and eliminates the per-commit open/close storm (bd-smxhz).
            Ok(certificate) => {
                *self
                    .cached_certificate_read
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(file);
                Ok(certificate)
            }
            // Anomaly: close and drop the descriptor so the next call re-opens
            // (self-healing invalidation).
            Err(error) => {
                let cleanup_cx = cx.create_child();
                let _cleanup_mask = cleanup_cx.masked();
                let _ = file.close(&cleanup_cx);
                Err(error)
            }
        }
    }
}

impl<V> WalBackend for PathRefreshingWalBackend<V>
where
    V: Vfs + 'static,
    V::File: Send + Sync + 'static,
{
    fn checkpoint_recovery_pending(&self) -> bool {
        self.inner.checkpoint_recovery_pending()
    }

    fn reconcile_checkpoint_reset<'a>(&'a mut self, cx: &'a Cx) -> WalFuture<'a, ()> {
        Box::pin(async move {
            if !self.inner.checkpoint_recovery_pending() { return Ok(()); }
            #[cfg(all(feature = "native", any(unix, windows)))]
            if let Some(binding) = &self.namespace_binding { binding.validate_path_identity()?; }
            // The retained WAL header may be torn: compare descriptor identity,
            // never parse/reopen it as a new generation or create its path.
            let (mut current, _) = self.vfs.open(cx, Some(&self.wal_path), VfsOpenFlags::READWRITE | VfsOpenFlags::WAL)?;
            let identity = (|| {
                match (current.file_identity()?, self.inner.wal.file().file_identity()?) {
                    (Some(current), Some(owned)) if current == owned => Ok(()),
                    (Some(_), Some(_)) => Err(FrankenError::BusyRecovery),
                    _ => Err(FrankenError::Unsupported),
                }
            })();
            let cleanup_cx = cx.create_child();
            let _cleanup_mask = cleanup_cx.masked();
            let close = current.close(&cleanup_cx);
            match (identity, close) {
                (Ok(()), Ok(())) => {}
                (Err(error), Ok(())) | (Ok(()), Err(error)) => return Err(error),
                (Err(identity), Err(close)) => return Err(FrankenError::internal(format!(
                    "checkpoint reset path and close failed: identity={identity}; close={close}"
                ))),
            }
            self.inner.reconcile_checkpoint_reset(cx).await?;
            #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
            {
                if let Some(salts) = self.inner.checkpoint_retired_salts
                    && !self.pending_fec_reclamation.contains(&salts)
                {
                    self.pending_fec_reclamation.push(salts);
                }
                self.fec_admitted = None;
                self.fec_inspected_generation = None;
            }
            if let Some(mut stale) = self.cached_certificate_read.get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner).take()
            { let _ = stale.close(&cleanup_cx); }
            Ok(())
        })
    }

    fn native_recovery_required(&self) -> Option<WalNativeRecoveryReason> {
        self.inner.native_recovery_required()
    }

    fn preflight_native_append<'a>(&'a mut self, cx: &'a Cx) -> WalFuture<'a, ()> {
        Box::pin(async move {
            if !self.inner.native_reader_required() {
                return self.refresh_published_snapshot(cx).await.map(|_| ());
            }
            if self.inner.has_pending_publication() || self.inner.native_recovery_requested.is_some() {
                return Err(FrankenError::BusyRecovery);
            }
            self.validate_current_native_wal_path(cx).await?;
            self.inner.preflight_native_append(cx).await
        })
    }

    fn native_reader_required(&self) -> bool {
        self.inner.native_reader_required()
    }

    fn native_read_binding(&self) -> Option<WalNativeReadBinding> {
        self.inner.native_read_binding()
    }

    fn begin_native_read<'a>(
        &'a mut self, cx: &'a Cx, binding: WalNativeReadBinding,
    ) -> WalFuture<'a, WalNativeReadOutcome> {
        Box::pin(async move {
            match self.ensure_current_wal_path_for_native_read(cx).await? {
                WalNativeReadOutcome::Ready => self.inner.begin_native_read(cx, binding).await,
                outcome => Ok(outcome),
            }
        })
    }

    fn end_native_read(&mut self, token: &WalNativeReadToken) -> Result<()> {
        self.inner.end_native_read(token)
    }

    fn recover_native_read_state<'a>(
        &'a mut self,
        cx: &'a Cx,
        reason: fsqlite_pager::traits::WalNativeRecoveryReason,
    ) -> WalFuture<'a, ()> {
        Box::pin(async move {
            if self.inner.has_pending_publication() || self.inner.native_read_binding.is_some() {
                return Err(FrankenError::BusyRecovery);
            }
            match self.ensure_current_wal_path_for_native_read(cx).await? {
                fsqlite_pager::traits::WalNativeReadOutcome::Ready => {}
                fsqlite_pager::traits::WalNativeReadOutcome::RecoveryRequired(_) => {
                    // Creating/resetting the physical WAL requires its own
                    // retained owner; index-only recovery cannot authorize it.
                    return Err(FrankenError::BusyRecovery);
                }
            }
            self.inner.recover_native_read_state(cx, reason).await
        })
    }

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
                .latest_authorized_durable_certificate_record(cx, Some(pinned))
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
            self.inner.assert_no_pending_append_attempt()?;
            self.ensure_current_wal_path(cx).await?;
            self.inner.refresh_published_snapshot(cx).await.map(Some)
        })
    }

    fn validate_empty_wal_for_retirement<'a>(&'a mut self, cx: &'a Cx) -> WalFuture<'a, ()> {
        Box::pin(async move {
            if self.inner.native_reader_required() {
                self.validate_native_wal_retirement_path(cx).await?;
            } else {
                self.ensure_current_wal_path(cx).await?;
            }
            self.inner.validate_empty_wal_for_retirement(cx).await
        })
    }

    fn retire_empty_wal<'a>(&'a mut self, cx: &'a Cx) -> WalFuture<'a, ()> {
        Box::pin(async move {
            self.validate_empty_wal_for_retirement(cx).await?;
            self.inner.retire_empty_wal(cx).await
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
            self.inner.assert_no_pending_append_attempt()?;
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
            self.inner.assert_no_pending_append_attempt()?;
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
            self.inner.assert_no_pending_append_attempt()?;
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
            self.inner.assert_no_pending_append_attempt()?;
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
            self.inner.assert_no_pending_append_attempt()?;
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
            self.inner.assert_no_pending_append_attempt()?;
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
            self.inner.assert_no_pending_append_attempt()?;
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
            self.inner.validate_append_reconciliation(wal_frame_start, wal_frame_end)?;
            self.ensure_current_wal_path(cx).await?;
            self.inner.validate_append_reconciliation(wal_frame_start, wal_frame_end)?;
            if let Some(attempt) = &self.inner.pending_append_attempt {
                self.inner.wal.refresh_preserving_append_prefix(
                    cx, attempt.generation, attempt.start_frame_index, attempt.previous_running_checksum,
                ).await?;
            } else {
                self.inner.wal.refresh(cx).await?;
            }
            self.inner.validate_append_reconciliation(wal_frame_start, wal_frame_end)?;
            let wal_generation = self.inner.wal.generation_identity();
            let expected_record = ParallelWalDurableCertificateRecord::new(
                wal_generation,
                wal_frame_start,
                wal_frame_end,
                // bd-85x9y / GH#364: reconstruct with this database's identity so
                // the in-doubt record matches the sidecar bytes written above.
                self.db_file_id_for_written_certificate(),
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
                self.inner.authorize_append_reconciliation();
                if sync {
                    self.sync_with_fec(cx, false)?;
                    self.vfs.sync_parent_directory(cx, &self.wal_path)?;
                }
                self.inner.publish_reconciled_append(cx, sync)?;
                return Ok(ParallelWalCommitReconciliation::Authorized);
            }

            self.reconcile_absent_append(cx, &expected_record, valid_frame_count, sync).await
        })
    }

    fn latest_authorized_parallel_wal_commit_certificate<'a>(
        &'a mut self,
        cx: &'a Cx,
    ) -> WalFuture<'a, Option<ParallelWalCommitCertificate>> {
        Box::pin(async move {
            self.ensure_current_wal_path(cx).await?;
            if let Some(record) = self
                .latest_authorized_durable_certificate_record(cx, None)
                .await?
            {
                return Ok(Some(record.certificate));
            }
            self.checkpoint_certificate_handoff(cx).await
        })
    }

    fn current_parallel_wal_db_size_floor<'a>(
        &'a mut self,
        cx: &'a Cx,
    ) -> WalFuture<'a, Option<u32>> {
        Box::pin(async move {
            let current = if self.inner.native_reader_required() {
                self.preflight_native_append(cx).await?;
                self.inner.published_snapshot()
            } else {
                self.ensure_current_wal_path(cx).await?;
                self.inner.refresh_published_snapshot(cx).await?
            };
            let Some(current_commit_frame) = current.last_commit_frame else {
                return Ok(None);
            };
            let current_commit_frame_end = u64::try_from(current_commit_frame)
                .ok()
                .and_then(|frame| frame.checked_add(1));
            let current_commit_header = self
                .inner
                .inner()
                .read_frame_header(cx, current_commit_frame)
                .await?;
            if !current_commit_header.is_commit() {
                return Err(FrankenError::WalCorrupt {
                    detail: format!(
                        "published WAL commit horizon {current_commit_frame} is not a commit frame"
                    ),
                });
            }
            let authorized = self
                .latest_authorized_durable_certificate_record(cx, None)
                .await?;
            if let Some(record) = authorized
                && current.generation == record.wal_generation
                && current_commit_frame_end == Some(record.wal_frame_end)
            {
                if record.certificate.db_size_pages != current_commit_header.db_size {
                    return Err(FrankenError::WalCorrupt {
                        detail: format!(
                            "parallel WAL certificate db_size {} disagrees with covered commit marker db_size {}",
                            record.certificate.db_size_pages, current_commit_header.db_size
                        ),
                    });
                }
                return Ok(Some(record.certificate.db_size_pages));
            }
            Ok(Some(current_commit_header.db_size))
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
            let latest = if self.inner.native_reader_required() {
                self.preflight_native_append(cx).await?;
                self.inner.published_snapshot()
            } else {
                self.ensure_current_wal_path(cx).await?;
                self.inner.refresh_published_snapshot(cx).await?
            };
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
        self.sync_with_fec(cx, true)
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
    fn set_wal_fec_producer(
        &mut self,
        cx: &Cx,
        producer: Option<fsqlite_wal::wal_fec::WalFecRepairProducer>,
    ) -> Result<()> {
        self.inner.assert_no_pending_append_attempt()?;
        self.fec_producer = producer;
        if let Some(producer) = &self.fec_producer {
            let header = WalHeader::from_bytes(&self.inner.wal.header().to_bytes()?)?;
            if self.fec_inspected_generation != Some(header)
                && producer.try_reserve()?.submit(fsqlite_wal::wal_fec::WalFecCommittedRange {
                    wal_path: self.wal_path.clone(), header,
                    start_frame_no: 1, end_frame_no: 0,
                    previous_checksum: header.checksum, end_checksum: header.checksum,
                    repair_symbols: 0,
                })
            {
                self.fec_inspected_generation = Some(header);
            }
        }
        // Opening validated the checksum chain, but another live connection's
        // NORMAL-sync commits may still be in the OS cache. Establish a real
        // durability barrier before making that prefix repairable on catch-up.
        if let Some(range) = self.pending_fec_range(cx)?
            && self.inner.wal.last_fsynced_frame_count() >= range.end_frame_no as usize
        {
            let boundary = (range.header, range.end_frame_no, range.end_checksum);
            let submitted = if range.repair_symbols == 0 {
                true
            } else if let Some(producer) = &self.fec_producer {
                let permit = producer.try_reserve()?;
                self.inner.wal.sync(cx, SyncFlags::NORMAL)?;
                permit.submit(range)
            } else {
                false
            };
            if submitted {
                self.fec_admitted = Some(boundary);
            }
        }
        Ok(())
    }

    fn frame_count(&self) -> usize {
        self.inner.frame_count()
    }

    fn backfilled_frame_count(&self) -> usize {
        self.inner.backfilled_frame_count()
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
            if self.inner.has_pending_publication() || self.inner.native_read_binding.is_some()
                || self.inner.native_recovery_requested.is_some()
            { return Err(FrankenError::BusyRecovery); }
            self.ensure_current_wal_path(cx).await?;
            if self.inner.native_reader_required() { self.inner.native_checkpoint_view(cx).await?; }
            let checkpoint_handoff = self
                .latest_authorized_durable_certificate_record(cx, None)
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
            let checkpoint_result = self
                .inner
                .checkpoint(cx, mode, writer, backfilled_frames, oldest_reader_frame)
                .await;
            #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
            if let Some(salts) = self.inner.checkpoint_retired_salts
                && !self.pending_fec_reclamation.contains(&salts)
            {
                self.pending_fec_reclamation.push(salts);
            }
            let result = checkpoint_result?;
            #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
            if result.wal_was_reset {
                self.fec_admitted = None;
                self.fec_inspected_generation = None;
            }
            #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
            if !self.pending_fec_reclamation.is_empty() {
                let sidecar_path = fsqlite_wal::wal_fec_path_for_wal(&self.wal_path);
                // The reset is already durable. Retire only that generation:
                // another writer may have appended repair groups for a newer
                // WAL by the time this best-effort cleanup acquires its guard.
                // Sidecar contention never waits or changes checkpoint success.
                self.pending_fec_reclamation.retain(|salts| {
                    match fsqlite_wal::wal_fec::reclaim_wal_fec_groups(
                        &sidecar_path,
                        *salts,
                        u32::MAX,
                    ) {
                        Ok(reclaimed) => {
                            debug!(reclaimed, sidecar = %sidecar_path.display(),
                                "reclaimed persisted FEC groups after WAL reset");
                            false
                        }
                        Err(err) => {
                            warn!(sidecar = %sidecar_path.display(), error = %err,
                                "WAL reset completed but FEC sidecar reclamation was deferred");
                            true
                        }
                    }
                });
            }
            // bd-smxhz: the checkpoint reset the WAL generation and its
            // -wal-cert sidecar, so the cached certificate descriptor is stale;
            // drop it so the next read re-opens against the fresh generation.
            if let Some(mut stale) = self
                .cached_certificate_read
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
            {
                let cleanup_cx = cx.create_child();
                let _cleanup_mask = cleanup_cx.masked();
                let _ = stale.close(&cleanup_cx);
            }
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
    fn checkpoint_page1_header_patch(&self) -> Option<[u8; 12]> {
        self.writer.checkpoint_page1_header_patch()
    }

    fn read_page_if_supported<'a>(
        &'a mut self, cx: &'a Cx, page_no: PageNumber, buf: &'a mut [u8],
    ) -> CheckpointTargetFuture<'a, Option<usize>> {
        self.writer.read_page_if_supported(cx, page_no, buf)
    }

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

    fn acquire_wal_reset_gate<'a>(&'a mut self, cx: &'a Cx) -> CheckpointTargetFuture<'a, bool> {
        Box::pin(async move { self.writer.acquire_wal_reset_gate(cx).await })
    }

    fn release_wal_reset_gate<'a>(&'a mut self, cx: &'a Cx) -> CheckpointTargetFuture<'a, ()> {
        Box::pin(async move { self.writer.release_wal_reset_gate(cx).await })
    }
}

/// Bridges the writer while retaining reset state in the adapter across Drop.
struct CheckpointResetTarget<'a, F: VfsFile> {
    delegate: CheckpointTargetAdapterRef<'a>,
    pending: &'a mut Option<PendingCheckpointReset<F>>,
    previous_header: WalHeader,
    native: Option<NativeCheckpointView<F>>,
}

impl<F: VfsFile> CheckpointTarget for CheckpointResetTarget<'_, F> {
    fn checkpoint_page1_header_patch(&self) -> Option<[u8; 12]> {
        self.delegate.checkpoint_page1_header_patch()
    }

    fn write_page<'a>(&'a mut self, cx: &'a Cx, page_no: PageNumber, data: &'a [u8]) -> CheckpointTargetFuture<'a, ()> {
        self.delegate.write_page(cx, page_no, data)
    }
    fn truncate_db<'a>(&'a mut self, cx: &'a Cx, pages: u32) -> CheckpointTargetFuture<'a, ()> {
        self.delegate.truncate_db(cx, pages)
    }
    fn sync_db<'a>(&'a mut self, cx: &'a Cx) -> CheckpointTargetFuture<'a, ()> {
        self.delegate.sync_db(cx)
    }
    fn read_page_if_supported<'a>(&'a mut self, cx: &'a Cx, page_no: PageNumber, buf: &'a mut [u8]) -> CheckpointTargetFuture<'a, Option<usize>> {
        self.delegate.read_page_if_supported(cx, page_no, buf)
    }
    fn acquire_wal_reset_gate<'a>(&'a mut self, cx: &'a Cx) -> CheckpointTargetFuture<'a, bool> {
        self.delegate.acquire_wal_reset_gate(cx)
    }
    fn release_wal_reset_gate<'a>(&'a mut self, cx: &'a Cx) -> CheckpointTargetFuture<'a, ()> {
        self.delegate.release_wal_reset_gate(cx)
    }
    fn publish_backfill<'a>(&'a mut self, _cx: &'a Cx, header: &'a WalHeader, frames: u32) -> CheckpointTargetFuture<'a, ()> {
        Box::pin(async move {
            if header != &self.previous_header { return Err(FrankenError::BusyRecovery); }
            if let Some(native) = &self.native {
                publish_shared_wal_index_backfill(&native.region, &native.header, frames)?;
            }
            Ok(())
        })
    }
    fn prepare_wal_reset<'a>(
        &'a mut self, _cx: &'a Cx, header: &'a WalHeader, new_checkpoint_seq: u32,
        new_salts: WalSalts, truncate: bool,
    ) -> CheckpointTargetFuture<'a, Option<VfsWriteCompletion>> {
        Box::pin(async move {
            if self.pending.is_some() || header != &self.previous_header { return Err(FrankenError::BusyRecovery); }
            let target = WalHeader { checkpoint_seq: new_checkpoint_seq, salts: new_salts,
                checksum: SqliteWalChecksum::default(), ..*header };
            let target_header = WalHeader::from_bytes(&target.to_bytes()?)?;
            let (native, source) = if let Some(view) = &self.native {
                let mut target = view.header;
                target.mx_frame = 0;
                target.n_page = 0;
                target.a_frame_cksum = [0, 0];
                target.a_salt = [new_salts.salt1, new_salts.salt2];
                target.update_checksum()?;
                validate_shared_wal_index_wal_binding(&target, &target_header, None)?;
                (Some(SharedWalIndexResetPlan::prepare(view.region.share(), view.header, target)?), Some(Arc::clone(&view.source)))
            } else { (None, None) };
            let completion = VfsWriteCompletion::new();
            *self.pending = Some(PendingCheckpointReset {
                old_header: *header, target_header, truncate, completion: completion.clone(),
                physical_complete: false, shared_complete: false, native, _source: source,
            });
            Ok(Some(completion))
        })
    }
    fn finish_wal_reset<'a>(&'a mut self, _cx: &'a Cx) -> CheckpointTargetFuture<'a, ()> {
        Box::pin(async move {
            let reset = self.pending.as_mut().ok_or(FrankenError::BusyRecovery)?;
            if reset.completion.state() != VfsWriteCompletionState::Success { return Err(FrankenError::BusyRecovery); }
            reset.physical_complete = true;
            if let Some(native) = &mut reset.native { native.publish()?; }
            reset.shared_complete = true;
            Ok(())
        })
    }
    fn wal_reset_pending(&self) -> bool {
        self.pending.as_ref().is_some_and(|reset| !reset.shared_complete)
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
    #[allow(
        clippy::struct_excessive_bools,
        reason = "independent fault controls can be armed together"
    )]
    struct CheckpointHandoffFaultState {
        next_write: Option<CheckpointHandoffWriteFault>,
        fail_next_sync: bool,
        /// Fail the next sync on a non-handoff (i.e. WAL) file.
        fail_next_wal_sync: bool,
        #[cfg(feature = "fault-injection")]
        pause_after_wal_write: bool,
        #[cfg(feature = "fault-injection")]
        next_wal_write_prefix: Option<usize>,
        #[cfg(feature = "fault-injection")]
        pend_next_reset_write: bool,
        #[cfg(feature = "fault-injection")]
        pending_reset_write: Option<(Vec<u8>, u64, VfsWriteCompletion)>,
        #[cfg(feature = "fault-injection")]
        reset_headers: Vec<Vec<u8>>,
        #[cfg(feature = "fault-injection")]
        reset_header_written: bool,
        #[cfg(feature = "fault-injection")]
        reset_sync_failures_remaining: usize,
        #[cfg(feature = "fault-injection")]
        reset_shared_header_after_sync: Option<(fsqlite_vfs::ShmRegion, fsqlite_wal::wal_index::WalIndexHdr)>,
        fail_index_maps: bool,
        wal_header_reads_before_failure: Option<usize>,
        wal_header_reads_completed: usize,
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

        #[cfg(feature = "fault-injection")]
        fn pause_after_next_wal_write(&self) {
            self.faults
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pause_after_wal_write = true;
        }

        #[cfg(feature = "fault-injection")]
        fn fail_next_wal_write_after_prefix(&self, prefix_bytes: usize) {
            self.faults.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                .next_wal_write_prefix = Some(prefix_bytes);
        }

        #[cfg(feature = "fault-injection")]
        fn pend_next_reset_write(&self) {
            self.faults.lock().unwrap().pend_next_reset_write = true;
        }

        #[cfg(feature = "fault-injection")]
        fn fail_next_reset_sync(&self) {
            let mut faults = self.faults.lock().unwrap();
            faults.reset_sync_failures_remaining = 1;
            faults.reset_header_written = false;
        }

        /// The retained synthetic source owns the write after caller Drop.
        /// Only the real MemoryFile write terminalizes its original token.
        #[cfg(feature = "fault-injection")]
        async fn complete_pending_reset_write(&self, cx: &Cx) -> Result<()> {
            let (bytes, offset, completion) = self.faults.lock().unwrap()
                .pending_reset_write.take().expect("source owns a pending reset write");
            let (mut file, _) = self.inner.open(cx, Some(Path::new("test.db-wal")), VfsOpenFlags::READWRITE | VfsOpenFlags::WAL)?;
            let result = file.write_tracked(cx, &bytes, offset, completion).await;
            if result.is_ok() { self.faults.lock().unwrap().reset_header_written = true; }
            let close = file.close(cx);
            result.and(close)
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

        fn fail_wal_frame_header_read_after(&self, successful_reads: usize) {
            let mut faults = self
                .faults
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            faults.wal_header_reads_before_failure = Some(successful_reads);
            faults.wal_header_reads_completed = 0;
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
        fn wal_reader_mark_exclusive_acquire(&mut self, cx: &Cx, reader_slot: u32) -> Result<()> {
            self.inner.wal_reader_mark_exclusive_acquire(cx, reader_slot)
        }

        fn close(&mut self, cx: &Cx) -> Result<()> {
            self.inner.close(cx)
        }

        fn file_identity(&self) -> Result<Option<fsqlite_vfs::FileIdentity>> {
            self.inner.file_identity()
        }

        async fn read<'a>(
            &'a self,
            cx: &'a Cx,
            buf: &'a mut [u8],
            offset: u64,
        ) -> Result<usize> {
            // Full-frame WAL recovery and 32-byte WAL header reads must finish
            // before this opt-in publication header scan fault can fire.
            let observe_header = if self.path.as_deref() == Some(Path::new("test.db-wal"))
                && buf.len() == WAL_FRAME_HEADER_SIZE
                && offset >= u64::try_from(WAL_HEADER_SIZE).expect("WAL header fits u64")
            {
                let mut faults = self
                    .faults
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                match faults.wal_header_reads_before_failure {
                    Some(0) => {
                        faults.wal_header_reads_before_failure = None;
                        return Err(FrankenError::Io(std::io::Error::other(
                            "injected WAL publication header read failure",
                        )));
                    }
                    Some(remaining) => {
                        faults.wal_header_reads_before_failure = Some(remaining - 1);
                        true
                    }
                    None => false,
                }
            } else {
                false
            };
            let bytes_read = self.inner.read(cx, buf, offset).await?;
            if observe_header && bytes_read == WAL_FRAME_HEADER_SIZE {
                self.faults
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .wal_header_reads_completed += 1;
            }
            Ok(bytes_read)
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

        #[cfg(feature = "fault-injection")]
        async fn write_tracked<'a>(
            &'a self,
            cx: &'a Cx,
            buf: &'a [u8],
            offset: u64,
            completion: VfsWriteCompletion,
        ) -> Result<()> {
            let is_reset = self.path.as_deref() == Some(Path::new("test.db-wal"))
                && offset == 0 && buf.len() == WAL_HEADER_SIZE;
            let pend_reset = if is_reset {
                let mut faults = self.faults.lock().unwrap();
                faults.reset_headers.push(buf.to_vec());
                std::mem::take(&mut faults.pend_next_reset_write)
            } else { false };
            if pend_reset {
                self.faults.lock().unwrap().pending_reset_write = Some((buf.to_vec(), offset, completion));
                return std::future::pending::<Result<()>>().await;
            }
            let prefix = if self.path.as_deref() == Some(Path::new("test.db-wal")) {
                self.faults.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                    .next_wal_write_prefix.take()
            } else {
                None
            };
            if let Some(prefix) = prefix {
                assert!(prefix < buf.len(), "fixture must leave an incomplete candidate");
                self.inner.write_tracked(cx, &buf[..prefix], offset, completion.error_mapped_child())
                    .await?;
                return Err(FrankenError::Io(std::io::Error::other("injected partial WAL write")));
            }
            let pause = self.path.as_deref() == Some(Path::new("test.db-wal"))
                && std::mem::take(
                    &mut self
                        .faults
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .pause_after_wal_write,
                );
            if pause {
                // MemoryFile owns the actual write and terminal token. Pause
                // only after that source succeeds, before WalFile observes it.
                self.inner.write_tracked(cx, buf, offset, completion).await?;
                if is_reset { self.faults.lock().unwrap().reset_header_written = true; }
                return std::future::pending::<Result<()>>().await;
            }
            // Preserve this fixture's existing handoff faults and the trait's
            // conservative default completion semantics for every other write.
            let result = self.write(cx, buf, offset).await;
            if result.is_ok() {
                if is_reset { self.faults.lock().unwrap().reset_header_written = true; }
                completion.complete_success();
            } else {
                completion.complete_error();
            }
            result
        }

        fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
            self.inner.truncate(cx, size)
        }

        fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
            let mut faults = self
                .faults
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            #[cfg(feature = "fault-injection")]
            let is_reset_sync = self.path.as_deref() == Some(Path::new("test.db-wal")) && faults.reset_header_written;
            #[cfg(feature = "fault-injection")]
            if is_reset_sync {
                if faults.reset_sync_failures_remaining > 0 {
                    faults.reset_sync_failures_remaining -= 1;
                    return Err(FrankenError::Io(std::io::Error::other("injected exact reset header sync failure")));
                }
                faults.reset_header_written = false;
            }
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
            let result = if fail {
                Err(FrankenError::Io(std::io::Error::other(
                    "injected checkpoint handoff sync failure",
                )))
            } else if fail_wal {
                Err(FrankenError::Io(std::io::Error::other(
                    "injected WAL sync failure",
                )))
            } else {
                self.inner.sync(cx, flags)
            };
            #[cfg(feature = "fault-injection")]
            if result.is_ok() && is_reset_sync {
                let intervention = self.faults.lock().unwrap().reset_shared_header_after_sync.take();
                if let Some((region, header)) = intervention {
                    fsqlite_wal::wal_index::publish_shared_wal_index_header(&region, &header)?;
                }
            }
            result
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

        fn owns_external_wal_append_write(&self, cx: &Cx) -> Result<bool> {
            self.inner.owns_external_wal_append_write(cx)
        }

        fn lock_external_wal_append(&mut self, cx: &Cx) -> Result<()> {
            self.inner.lock_external_wal_append(cx)
        }

        fn restore_external_wal_append_attempt(&mut self, cx: &Cx) -> Result<()> {
            self.inner.restore_external_wal_append_attempt(cx)
        }

        fn lock_external_maintenance(&mut self, cx: &Cx, wal_mode: bool) -> Result<()> {
            self.inner.lock_external_maintenance(cx, wal_mode)
        }

        fn lock_external_wal_recovery(&mut self, cx: &Cx) -> Result<()> {
            self.inner.lock_external_wal_recovery(cx)
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
            if self.faults.lock().unwrap().fail_index_maps {
                return Err(FrankenError::Io(std::io::Error::other("injected native index mapping refusal")));
            }
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

    #[test]
    fn gh346_external_horizon_supersedes_older_certificate_db_size() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut backend = make_path_refreshing_backend(&vfs, &cx);
        let original_page = sample_page(0x46);
        let mut certificate = sample_certificate(1, 1, vec![1]);
        certificate.db_size_pages = 9;
        certificate.wal_frame_payload_digest =
            test_frame_payload_digest(1, &original_page, certificate.db_size_pages);
        certificate.certificate_crc32c = certificate.computed_crc32c();
        backend
            .persist_parallel_wal_commit_certificate(&cx, &certificate, 1, 1, true)
            .expect("persist original authorized certificate");
        backend
            .append_frame(&cx, 1, &original_page, certificate.db_size_pages)
            .expect("append original certified commit");
        backend.sync(&cx).expect("sync original certified commit");

        assert_eq!(
            backend
                .current_parallel_wal_db_size_floor(&cx)
                .expect("read current certified db-size floor"),
            Some(9)
        );

        // Stock SQLite does not write FrankenSQLite's certificate sidecar. A
        // later shrinking commit therefore leaves the older certificate valid
        // as a clock seed, but it no longer covers the current WAL horizon.
        let vacuumed_page = sample_page(0x47);
        backend
            .append_frame(&cx, 1, &vacuumed_page, 5)
            .expect("append later uncertified shrinking commit");
        backend
            .sync(&cx)
            .expect("sync later uncertified shrinking commit");

        assert_eq!(
            backend
                .latest_authorized_parallel_wal_commit_certificate(&cx)
                .expect("older certificate remains a valid clock seed"),
            Some(certificate)
        );
        assert_eq!(
            backend
                .current_parallel_wal_db_size_floor(&cx)
                .expect("classify db-size floor at the latest commit horizon"),
            Some(5),
            "an older certificate must not re-expand a later shrinking commit"
        );

        let grown_page = sample_page(0x48);
        backend
            .append_frame(&cx, 1, &grown_page, 12)
            .expect("append later uncertified growing commit");
        backend
            .sync(&cx)
            .expect("sync later uncertified growing commit");
        assert_eq!(
            backend
                .current_parallel_wal_db_size_floor(&cx)
                .expect("classify growing external db-size floor"),
            Some(12),
            "the current external horizon must still protect later growth"
        );
    }

    #[test]
    fn gh346_exact_certificate_db_size_must_match_commit_marker() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut backend = make_path_refreshing_backend(&vfs, &cx);
        let committed_page = sample_page(0x49);
        let mut certificate = sample_certificate(1, 1, vec![1]);
        certificate.db_size_pages = 9;
        certificate.wal_frame_payload_digest = test_frame_payload_digest(1, &committed_page, 5);
        certificate.certificate_crc32c = certificate.computed_crc32c();
        backend
            .persist_parallel_wal_commit_certificate(&cx, &certificate, 1, 1, true)
            .expect("persist internally inconsistent certificate fixture");
        backend
            .append_frame(&cx, 1, &committed_page, 5)
            .expect("append commit marker covered by fixture digest");
        backend.sync(&cx).expect("sync commit marker");

        let error = backend
            .current_parallel_wal_db_size_floor(&cx)
            .expect_err("certificate and commit-marker size mismatch must fail closed");
        assert!(
            matches!(
                error,
                FrankenError::WalCorrupt { ref detail }
                    if detail.contains("certificate db_size 9")
                        && detail.contains("commit marker db_size 5")
            ),
            "unexpected mismatch error: {error}"
        );
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
        // bd-85x9y / GH#364: leave the db-file identity slot (76..92) all-zero so
        // this fixture is a deterministic legacy/unstamped database — the load
        // gates then stay lenient (identity cannot condemn a certificate).
        page[76..92].fill(0);
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
            .unwrap_or_else(|_| panic!("sync drained the staged frames"))
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
            [0u8; 16],
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
            [0u8; 16],
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
            [0u8; 16],
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
            [0u8; 16],
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
                    [0u8; 16],
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
                [0u8; 16],
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
                        [0u8; 16],
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
                    [0u8; 16],
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

    // -- bd-85x9y / GH#364: db-file identity gate on the checkpoint handoff --

    /// This database's stamped identity in the identity-gate tests.
    const GH364_IDENTITY_A: [u8; 16] = [0xA1; 16];
    /// A foreign (replaced-file) identity in the identity-gate tests.
    const GH364_IDENTITY_B: [u8; 16] = [0xB2; 16];

    /// Write a `test.db` main-database page 1 stamped with `identity` at header
    /// bytes 76..92 so the adapter captures it as this file's identity.
    fn write_identity_stamped_main_db(vfs: &MemoryVfs, cx: &Cx, identity: [u8; 16]) {
        let mut page_one = sqlite_page_one(u16::try_from(PAGE_SIZE).expect("page size fits u16"));
        page_one[76..92].copy_from_slice(&identity);
        write_main_db_pages(vfs, cx, &[page_one]);
    }

    /// Overwrite the checkpoint handoff sidecar (`test.db-wal-cert-head`) with
    /// `bytes`.
    fn write_checkpoint_handoff(vfs: &MemoryVfs, cx: &Cx, bytes: &[u8]) {
        let path = std::path::Path::new("test.db-wal-cert-head");
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let (mut file, _) = vfs
            .open(cx, Some(path), flags)
            .expect("open handoff sidecar");
        file.truncate(cx, 0).expect("truncate handoff sidecar");
        file.write(cx, bytes, 0).expect("write handoff sidecar");
        file.close(cx).expect("close handoff sidecar");
    }

    /// Encode a single handoff certificate record bound to `db_file_id`.
    fn handoff_record_bytes(
        backend: &PathRefreshingWalBackend<MemoryVfs>,
        db_file_id: [u8; 16],
    ) -> Vec<u8> {
        let certificate = sample_certificate(1, 1, vec![1]);
        ParallelWalDurableCertificateRecord::new(
            backend.inner.inner().generation_identity(),
            1,
            1,
            db_file_id,
            certificate,
        )
        .expect("construct handoff record")
        .to_bytes()
    }

    #[test]
    fn gh364_checkpoint_handoff_with_foreign_identity_is_absent() {
        // A stale handoff left behind across a database-file replacement is bound
        // to the REPLACED file's identity. On a fresh (differently stamped)
        // database it must be treated as absent so it cannot re-extend the new,
        // smaller file to the old file's committed page count.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        write_identity_stamped_main_db(&vfs, &cx, GH364_IDENTITY_A);
        let mut backend = make_path_refreshing_backend(&vfs, &cx);
        let stale = handoff_record_bytes(&backend, GH364_IDENTITY_B);
        write_checkpoint_handoff(&vfs, &cx, &stale);

        let recovered = backend
            .latest_authorized_parallel_wal_commit_certificate(&cx)
            .wait()
            .expect("handoff read must not fail closed on a foreign identity");
        assert!(
            recovered.is_none(),
            "a handoff bound to a foreign db-file identity must be absent, got {recovered:?}"
        );
    }

    #[test]
    fn gh364_checkpoint_handoff_with_matching_identity_is_applied() {
        // The database's own handoff (same identity) must still be honored — the
        // db_size floor it carries prevents truncation of committed growth. This
        // is the durability guardrail: a valid same-file certificate is applied.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        write_identity_stamped_main_db(&vfs, &cx, GH364_IDENTITY_A);
        let mut backend = make_path_refreshing_backend(&vfs, &cx);
        let own = handoff_record_bytes(&backend, GH364_IDENTITY_A);
        write_checkpoint_handoff(&vfs, &cx, &own);

        let recovered = backend
            .latest_authorized_parallel_wal_commit_certificate(&cx)
            .wait()
            .expect("same-identity handoff must recover");
        assert_eq!(
            recovered,
            Some(sample_certificate(1, 1, vec![1])),
            "a handoff bound to this database's own identity must be applied"
        );
    }

    #[test]
    fn gh364_checkpoint_handoff_with_legacy_v3_version_is_absent_not_fatal() {
        // A handoff written by a pre-bd-85x9y build carries the identity-less v3
        // envelope version. This build treats it as an absent certificate rather
        // than failing recovery closed with a corruption error.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        write_identity_stamped_main_db(&vfs, &cx, GH364_IDENTITY_A);
        let mut backend = make_path_refreshing_backend(&vfs, &cx);
        let mut record = handoff_record_bytes(&backend, GH364_IDENTITY_A);
        // Roll the envelope version marker back to the identity-less v3 value.
        record[8..10].copy_from_slice(&3u16.to_le_bytes());
        write_checkpoint_handoff(&vfs, &cx, &record);

        let recovered = backend
            .latest_authorized_parallel_wal_commit_certificate(&cx)
            .wait()
            .expect("a legacy v3 handoff must be absent, never a fatal error");
        assert!(
            recovered.is_none(),
            "a legacy v3 (identity-less) handoff must be absent, got {recovered:?}"
        );
    }

    /// GH#372 helper: a synthetic legacy envelope SHORTER than this build's
    /// minimum record size — the shape the original v2 writer produced (no
    /// `db_file_id`, no payload digest). The legacy gates only inspect the
    /// magic, version, and declared length, so the payload is opaque filler.
    fn short_legacy_certificate_record(version: u16) -> Vec<u8> {
        const OPAQUE_PAYLOAD: usize = 28;
        let total_len = DURABLE_CERTIFICATE_RECORD_HEADER_SIZE
            + OPAQUE_PAYLOAD
            + 4
            + ParallelWalDurableCertificateRecord::LENGTH_FOOTER_SIZE;
        let declared = u32::try_from(total_len).expect("legacy record length fits u32");
        let mut bytes = Vec::with_capacity(total_len);
        bytes.extend_from_slice(&PARALLEL_WAL_DURABLE_CERTIFICATE_MAGIC);
        bytes.extend_from_slice(&version.to_le_bytes());
        bytes.extend_from_slice(&declared.to_le_bytes());
        bytes.extend_from_slice(&[0x5A; OPAQUE_PAYLOAD]);
        bytes.extend_from_slice(&[0; 4]);
        bytes.extend_from_slice(&declared.to_le_bytes());
        assert_eq!(bytes.len(), total_len);
        assert!(
            total_len < MIN_DURABLE_CERTIFICATE_RECORD_SIZE,
            "the synthetic legacy record must be shorter than the current minimum"
        );
        bytes
    }

    /// GH#372 helper: the authorized current-version sidecar record with its
    /// envelope version rolled back to `version` — a full-size legacy record.
    fn full_size_legacy_certificate_record(vfs: &MemoryVfs, cx: &Cx, version: u16) -> Vec<u8> {
        let mut record = read_certificate_sidecar(vfs, cx);
        record[8..10].copy_from_slice(&version.to_le_bytes());
        record
    }

    #[test]
    fn gh372_full_size_legacy_sidecar_record_is_absent_not_fatal() {
        // A `-wal-cert` record written by a pre-identity release — v2 (the
        // original envelope) or v3 (identity-less) — is a proof this build
        // cannot honor. Loading reports an ABSENT certificate, never WalCorrupt.
        for legacy_version in [2_u16, 3_u16] {
            let cx = test_cx();
            let vfs = MemoryVfs::new();
            let (mut backend, _) = make_authorized_certificate_backend(&vfs, &cx);
            let record = full_size_legacy_certificate_record(&vfs, &cx, legacy_version);
            replace_certificate_sidecar(&vfs, &cx, &record);

            let recovered = backend
                .latest_authorized_parallel_wal_commit_certificate(&cx)
                .wait()
                .unwrap_or_else(|error| {
                    panic!(
                        "a legacy v{legacy_version} sidecar record must be absent, never fatal: {error}"
                    )
                });
            assert!(
                recovered.is_none(),
                "legacy v{legacy_version} record must read as absent, got {recovered:?}"
            );
        }
    }

    #[test]
    fn gh372_short_legacy_sidecar_record_is_absent_not_fatal() {
        // As reported: the legacy writer's records are SHORTER than this
        // build's minimum record size, so the EOF footer never anchored a
        // current-version candidate and the load fell into the torn-suffix
        // validator, failing closed with "suffix has unsupported record
        // version 2". Three sidecar shapes: one record, two records, and two
        // records followed by a torn third.
        for legacy_version in [2_u16, 3_u16] {
            let record = short_legacy_certificate_record(legacy_version);
            let mut two = record.clone();
            two.extend_from_slice(&record);
            let mut torn = two.clone();
            torn.extend_from_slice(&record[..record.len() / 2]);
            for (label, sidecar) in [("one", record.clone()), ("two", two), ("two+torn", torn)] {
                let cx = test_cx();
                let vfs = MemoryVfs::new();
                let (mut backend, _) = make_authorized_certificate_backend(&vfs, &cx);
                replace_certificate_sidecar(&vfs, &cx, &sidecar);

                let recovered = backend
                    .latest_authorized_parallel_wal_commit_certificate(&cx)
                    .wait()
                    .unwrap_or_else(|error| {
                        panic!(
                            "short legacy v{legacy_version} sidecar ({label}) must be absent, never fatal: {error}"
                        )
                    });
                assert!(
                    recovered.is_none(),
                    "short legacy v{legacy_version} sidecar ({label}) must read as absent, got {recovered:?}"
                );
            }
        }
    }

    #[test]
    fn gh372_append_after_legacy_sidecar_discards_it_and_succeeds() {
        // The first append from this build discards a pre-upgrade sidecar
        // (several legacy records, the last one torn) instead of failing
        // closed at the append boundary, and the new record is recoverable.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let (mut backend, _) = make_authorized_certificate_backend(&vfs, &cx);
        let legacy = full_size_legacy_certificate_record(&vfs, &cx, 2);
        let mut sidecar = Vec::new();
        for _ in 0..3 {
            sidecar.extend_from_slice(&legacy);
        }
        sidecar.extend_from_slice(&legacy[..legacy.len() / 2]);
        replace_certificate_sidecar(&vfs, &cx, &sidecar);
        assert!(
            backend
                .latest_authorized_parallel_wal_commit_certificate(&cx)
                .wait()
                .expect("a legacy sidecar must load as absent")
                .is_none()
        );

        let page = sample_page(0x45);
        let mut certificate = sample_certificate(2, 2, vec![1]);
        certificate.wal_frame_payload_digest = test_frame_payload_digest(2, &page, 2);
        certificate.certificate_crc32c = certificate.computed_crc32c();
        backend
            .persist_parallel_wal_commit_certificate(&cx, &certificate, 2, 2, true)
            .expect("the first append after an upgrade must discard the legacy sidecar, not fail");
        backend
            .append_frame(&cx, 2, &page, 2)
            .expect("append matching commit marker");
        backend.sync(&cx).expect("sync matching commit marker");

        let rewritten = read_certificate_sidecar(&vfs, &cx);
        let decoded = ParallelWalDurableCertificateRecord::from_bytes(&rewritten).expect(
            "the sidecar holds exactly one current-version record once the legacy content is discarded",
        );
        assert_eq!(decoded.certificate, certificate);
        assert_eq!(
            backend
                .latest_authorized_parallel_wal_commit_certificate(&cx)
                .wait()
                .expect("recover the appended certificate"),
            Some(certificate)
        );
    }

    #[test]
    fn gh372_walk_back_into_legacy_record_is_absent_not_fatal() {
        // A current-version record that cannot authorize the WAL (here: it
        // lies beyond the reader's frame snapshot) walks back to the record
        // below it. When that record is a legacy envelope the walk ends as
        // ABSENT instead of failing closed on the undecodable record.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let (mut backend, _) = make_authorized_certificate_backend(&vfs, &cx);
        let mut sidecar = full_size_legacy_certificate_record(&vfs, &cx, 3);
        let beyond_snapshot = ParallelWalDurableCertificateRecord::new(
            backend.inner.inner().generation_identity(),
            2,
            2,
            backend.db_file_id_for_written_certificate(),
            sample_certificate(2, 2, vec![1]),
        )
        .expect("construct record beyond the frame snapshot")
        .to_bytes();
        sidecar.extend_from_slice(&beyond_snapshot);
        replace_certificate_sidecar(&vfs, &cx, &sidecar);

        let recovered = backend
            .latest_authorized_parallel_wal_commit_certificate(&cx)
            .wait()
            .expect("walking back into a legacy record must be absent, never fatal");
        assert!(recovered.is_none(), "expected absent, got {recovered:?}");
    }

    #[test]
    fn gh372_checkpoint_handoff_with_legacy_v2_version_is_absent_not_fatal() {
        // The v2 counterpart of the GH#364 v3 handoff case above.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        write_identity_stamped_main_db(&vfs, &cx, GH364_IDENTITY_A);
        let mut backend = make_path_refreshing_backend(&vfs, &cx);
        let mut record = handoff_record_bytes(&backend, GH364_IDENTITY_A);
        record[8..10].copy_from_slice(&2u16.to_le_bytes());
        write_checkpoint_handoff(&vfs, &cx, &record);

        let recovered = backend
            .latest_authorized_parallel_wal_commit_certificate(&cx)
            .wait()
            .expect("a legacy v2 handoff must be absent, never a fatal error");
        assert!(
            recovered.is_none(),
            "a legacy v2 handoff must be absent, got {recovered:?}"
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
            .latest_authorized_durable_certificate_record(&cx, None)
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

    #[test]
    fn pinned_logical_reader_skips_newer_authorized_certificate() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let (mut backend, old_certificate) = make_authorized_certificate_backend(&vfs, &cx);
        backend.begin_transaction(&cx).expect("pin older certificate horizon");
        let old_pinned = backend.pinned_read_snapshot().unwrap();
        let old_page = backend.read_page_pinned(&cx, 1).expect("capture old page");
        let page = sample_page(0x65);
        let mut newer = sample_certificate(2, 2, vec![1]);
        newer.wal_frame_payload_digest = test_frame_payload_digest(1, &page, 2);
        newer.certificate_crc32c = newer.computed_crc32c();
        backend.persist_parallel_wal_commit_certificate(&cx, &newer, 2, 2, true)
            .expect("persist newer certificate");
        backend.append_frame(&cx, 1, &page, 2).expect("append newer certified commit");
        backend.sync(&cx).expect("publish newer certified commit");
        assert_eq!(backend.pinned_read_snapshot(), Some(old_pinned));
        let logical = backend.pinned_logical_read_snapshot(&cx)
            .expect("walk past newer certificate without widening reader")
            .expect("older certificate is still authoritative for this pin");
        assert_eq!(logical.last_commit_frame, old_pinned.last_commit_frame);
        assert_eq!(logical.visible_commit_seq, old_certificate.commit_seq_hi);
        assert_eq!(backend.read_page_pinned(&cx, 1).expect("old pinned page persists"), old_page);
        backend.begin_transaction(&cx).expect("next transaction may capture newer commit");
        let next = backend.pinned_logical_read_snapshot(&cx).expect("new logical horizon").unwrap();
        assert_eq!(next.visible_commit_seq, newer.commit_seq_hi);
        assert_eq!(next.last_commit_frame, Some(1));
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

    /// Heap-backed transport fixture: exercises the actual adapter and fault
    /// VFS, but supplies an initialized index explicitly. This does not prove
    /// stock SQLite interoperability, filesystem durability, or native locks.
    #[cfg(all(feature = "native", unix))]
    struct SyntheticSharedPublication {
        _pager: fsqlite_pager::SimplePager<CheckpointHandoffFaultVfs>,
        adapter: WalBackendAdapter<<CheckpointHandoffFaultVfs as Vfs>::File>,
        region: fsqlite_vfs::ShmRegion,
        baseline: fsqlite_wal::wal_index::WalIndexHdr,
    }

    #[cfg(all(feature = "native", unix))]
    fn synthetic_shared_publication(vfs: &CheckpointHandoffFaultVfs, cx: &Cx) -> SyntheticSharedPublication {
        use fsqlite_wal::wal_index::{WAL_INDEX_VERSION, publish_shared_wal_index_header};

        let pager = fsqlite_pager::SimplePager::open_with_cx(
            cx, vfs.clone(), Path::new("test.db"), fsqlite_types::PageSize::DEFAULT,
        ).expect("open fixture pager");
        let mut adapter = make_fault_adapter(vfs, cx);
        let source = pager.wal_index_shm_source().expect("exact pager SHM source");
        let region = source.map_region(cx, 0, true).expect("map fixture index");
        let wal_header = adapter.wal.header();
        let mut baseline = fsqlite_wal::wal_index::WalIndexHdr {
            i_version: WAL_INDEX_VERSION, unused: 0, i_change: u32::MAX,
            is_init: 1, big_end_cksum: u8::from(wal_header.big_endian_checksum()),
            sz_page: u16::try_from(PAGE_SIZE).expect("fixture page size fits"),
            mx_frame: 0, n_page: 1, a_frame_cksum: [0, 0],
            a_salt: [wal_header.salts.salt1, wal_header.salts.salt2], a_cksum: [0, 0],
        };
        baseline.update_checksum().expect("fixture header checksum");
        publish_shared_wal_index_header(&region, &baseline).expect("seed fixture index");
        for offset in (96..136).step_by(4) {
            region.atomic_store_u32_ne(offset, 0x5678_1234, std::sync::atomic::Ordering::Release)
                .expect("seed unrelated reader/checkpoint bytes");
        }
        adapter.attach_wal_index_shm_source(source).expect("explicit synthetic transport attachment");
        SyntheticSharedPublication { _pager: pager, adapter, region, baseline }
    }

    #[cfg(all(feature = "native", unix))]
    fn native_recovery_stock_child(path: &Path) {
        let mut child = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", "wal_adapter::tests::test_native_recovery_first_begin_repairs_missing_torn_stale_and_terminal_index", "--nocapture"])
            .env("FSQLITE_NATIVE_RECOVERY_STOCK_CHILD", path)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn().expect("start independent stock reader");
        let start = std::time::Instant::now();
        loop {
            if child.try_wait().expect("poll stock child").is_some() { break; }
            if start.elapsed() > std::time::Duration::from_secs(30) {
                child.kill().expect("stop timed-out stock child");
                child.wait().expect("reap timed-out stock child");
                panic!("stock reader did not finish within 30 seconds");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let output = child.wait_with_output().expect("collect stock child output");
        assert!(output.status.success(), "stock child failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        assert!(String::from_utf8_lossy(&output.stdout).contains("native-recovery-stock-row-sum=30"));
    }

    #[cfg(all(feature = "native", unix))]
    #[test]
    fn test_native_recovery_first_begin_repairs_missing_torn_stale_and_terminal_index() {
        use fsqlite_pager::{JournalMode, MvccPager, SimplePager, TransactionHandle, TransactionMode};
        use fsqlite_vfs::UnixVfs;
        use fsqlite_wal::wal_index::WalIndexHdr;

        if let Some(path) = std::env::var_os("FSQLITE_NATIVE_RECOVERY_STOCK_CHILD") {
            let stock = rusqlite::Connection::open_with_flags(Path::new(&path),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).expect("stock opens recovered database");
            stock.busy_timeout(std::time::Duration::from_secs(1)).unwrap();
            let sum: i64 = stock.query_row("SELECT sum(n) FROM recovery_rows", [], |row| row.get(0))
                .expect("stock resolves recovered page/hash mappings");
            assert_eq!(sum, 30);
            println!("native-recovery-stock-row-sum={sum}");
            return;
        }
        let cx = test_cx();
        let directory = tempfile::tempdir().expect("native recovery directory");
        let seed = directory.path().join("seed.db");
        let stock = rusqlite::Connection::open(&seed).expect("stock creates seed");
        stock.execute_batch("PRAGMA page_size=4096; PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE recovery_rows(n INTEGER); INSERT INTO recovery_rows VALUES(10),(20);")
            .expect("stock commits seed rows while retaining its attachment");
        let main_bytes = std::fs::read(&seed).unwrap();
        let wal_bytes = std::fs::read(seed.with_file_name("seed.db-wal")).unwrap();
        let shared_bytes = std::fs::read(seed.with_file_name("seed.db-shm")).unwrap();
        for kind in ["missing", "torn", "stale", "terminal"] {
            let path = directory.path().join(format!("{kind}.db"));
            let wal_path = directory.path().join(format!("{kind}.db-wal"));
            let shared_path = directory.path().join(format!("{kind}.db-shm"));
            std::fs::write(&path, &main_bytes).unwrap();
            std::fs::write(&wal_path, &wal_bytes).unwrap();
            if kind != "missing" {
                let mut bytes = shared_bytes.clone();
                if kind == "torn" { bytes[0] ^= 1; } else {
                    let mut header = WalIndexHdr::from_bytes(&bytes).unwrap();
                    if kind == "stale" {
                        header.a_salt[0] ^= 1;
                    } else {
                        header.a_frame_cksum[0] ^= 1;
                    }
                    header.update_checksum().unwrap();
                    bytes[..48].copy_from_slice(&header.to_bytes());
                    bytes[48..96].copy_from_slice(&header.to_bytes());
                }
                std::fs::write(&shared_path, &bytes).unwrap();
            }
            let pager = SimplePager::open_with_cx(&cx, UnixVfs::new(), &path, fsqlite_types::PageSize::DEFAULT)
                .expect("open copied native database");
            assert_eq!(pager.journal_mode(), JournalMode::Wal);
            let source = pager.wal_index_shm_source().unwrap();
            let (file, _) = UnixVfs::new().open(&cx, Some(&wal_path), VfsOpenFlags::READWRITE | VfsOpenFlags::WAL)
                .expect("open existing copied WAL without creating it");
            let wal = WalFile::open(&cx, file).expect("validate stock WAL");
            let (last_index, last_marker) = wal.last_commit_frame_header().expect("stock final marker");
            let mut adapter = WalBackendAdapter::new(wal);
            adapter.attach_wal_index_shm_source(Arc::clone(&source)).unwrap();
            assert!(pager.set_wal_backend_owned(adapter).is_ok());
            let mut transaction = pager.begin(&cx, TransactionMode::ReadOnly)
                .expect("first begin performs canonical recovery before page reads");
            transaction.get_page(&cx, PageNumber::ONE).expect("read bound recovered page one");
            let region = source.map_region(&cx, 0, false).expect("recovery supplied the native index");
            let header = read_shared_wal_index_header(&region).unwrap().unwrap();
            assert_eq!(usize::try_from(header.mx_frame).unwrap(), last_index + 1);
            assert_eq!((header.n_page, header.a_frame_cksum),
                (last_marker.db_size, [last_marker.checksum.s1, last_marker.checksum.s2]));
            assert_eq!(std::fs::read(&path).unwrap(), main_bytes, "index recovery never rewrites main bytes");
            assert_eq!(std::fs::read(&wal_path).unwrap(), wal_bytes, "index recovery never rewrites WAL bytes");
            native_recovery_stock_child(&path);
            transaction.rollback(&cx).expect("release recovered native reader");
        }
        drop(stock);
    }

    /// Copy an old stock index beside a later complete stock WAL commit. The
    /// seed connection owns different inodes and never attaches to the copy.
    #[cfg(all(feature = "native", unix))]
    fn native_orphan_tail_database(directory: &Path, trailing_noncommit: bool) -> (PathBuf, Vec<u8>, fsqlite_wal::wal_index::WalIndexHdr) {
        let seed = directory.join("orphan-seed.db");
        let stock = rusqlite::Connection::open(&seed).expect("create stock seed");
        stock.execute_batch("PRAGMA page_size=4096; PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE recovery_rows(n INTEGER); INSERT INTO recovery_rows VALUES(10),(20);")
            .expect("commit old stock prefix");
        let main = std::fs::read(&seed).unwrap();
        let shared = std::fs::read(directory.join("orphan-seed.db-shm")).unwrap();
        let header = fsqlite_wal::wal_index::WalIndexHdr::from_bytes(&shared).unwrap();
        stock.execute_batch("UPDATE recovery_rows SET n=n+10;").expect("commit later page two");
        let mut wal = std::fs::read(directory.join("orphan-seed.db-wal")).unwrap();
        let path = directory.join("orphan.db");
        std::fs::write(&path, main).unwrap();
        std::fs::write(directory.join("orphan.db-wal"), &wal).unwrap();
        std::fs::write(directory.join("orphan.db-shm"), shared).unwrap();
        if trailing_noncommit {
            // Fixture setup has no live target attachment. Supply a complete,
            // checksum-valid noncommit suffix after the orphaned stock commit.
            let cx = test_cx();
            let (file, _) = fsqlite_vfs::UnixVfs::new().open(&cx, Some(&directory.join("orphan.db-wal")), VfsOpenFlags::READWRITE | VfsOpenFlags::WAL).unwrap();
            let mut tail = WalFile::open(&cx, file).expect("validate fixture WAL before extra suffix");
            tail.append_frame(&cx, 2, &sample_page(0xA6), 0).expect("append valid noncommit fixture suffix");
            tail.sync(&cx, SyncFlags::NORMAL).unwrap();
            tail.close(&cx).unwrap();
            wal = std::fs::read(directory.join("orphan.db-wal")).unwrap();
        }
        (path, wal, header)
    }

    #[cfg(all(feature = "native", unix))]
    #[test]
    fn test_native_orphan_tail_refuses_absent_or_unrelated_append_authority() {
        use fsqlite_pager::SimplePager;
        use fsqlite_vfs::UnixVfs;

        let cx = test_cx();
        let directory = tempfile::tempdir().unwrap();
        let (path, wal_bytes, baseline) = native_orphan_tail_database(directory.path(), false);
        let wal_path = directory.path().join("orphan.db-wal");
        let pager = SimplePager::open_with_cx(&cx, UnixVfs::new(), &path, fsqlite_types::PageSize::DEFAULT)
            .expect("open native orphan fixture");
        let source = pager.wal_index_shm_source().unwrap();
        let (file, _) = UnixVfs::new().open(&cx, Some(&wal_path), VfsOpenFlags::READWRITE | VfsOpenFlags::WAL).unwrap();
        let wal = WalFile::open(&cx, file).expect("open validated orphan WAL");
        let mut adapter = WalBackendAdapter::new(wal);
        adapter.attach_wal_index_shm_source(Arc::clone(&source)).unwrap();
        let mut lease = source.acquire_reader(&cx).expect("capture still-valid older shared horizon");
        let binding = lease.binding().unwrap();
        let token = binding.token().clone();
        assert_eq!(adapter.begin_native_read(&cx, binding).expect("older reader remains valid"), WalNativeReadOutcome::Ready);
        let pinned = adapter.pinned_read_snapshot().unwrap();
        let old_page = adapter.read_page_pinned(&cx, 2).expect("read old table page");
        assert_eq!(adapter.native_recovery_required(), None, "a newer physical tail alone does not widen a reader");
        assert!(matches!(adapter.preflight_native_append(&cx).expect_err("no WRITE owner"), FrankenError::BusyRecovery));
        assert_eq!(adapter.native_recovery_required(), None);

        let (mut unrelated, _) = UnixVfs::new().open(&cx, Some(&path), VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB).unwrap();
        unrelated.lock_external_wal_append(&cx).expect("different real handle owns WRITE");
        assert!(unrelated.owns_external_wal_append_write(&cx).unwrap());
        assert!(!source.owns_external_wal_append_write(&cx).expect("query exact source"));
        assert!(matches!(adapter.preflight_native_append(&cx).expect_err("another handle is not this source's append owner"), FrankenError::BusyRecovery));
        assert_eq!(adapter.native_recovery_required(), None);
        assert!(!adapter.has_pending_publication());
        assert_eq!(adapter.pinned_read_snapshot(), Some(pinned));
        assert_eq!(adapter.read_page_pinned(&cx, 2).expect("same reader after refusals"), old_page);
        let region = source.map_region(&cx, 0, false).expect("existing index");
        assert_eq!(read_shared_wal_index_header(&region).unwrap(), Some(baseline));
        assert_eq!(std::fs::read(&wal_path).unwrap(), wal_bytes);
        unrelated.restore_external_wal_append_attempt(&cx).unwrap();
        unrelated.close(&cx).unwrap();
        adapter.end_native_read(&token).unwrap();
        lease.release().expect("release exact old reader");
    }

    #[cfg(all(feature = "native", unix))]
    #[test]
    fn test_native_orphan_tail_requests_recovery_before_conflicts_and_fresh_writer_progress() {
        use fsqlite_pager::{MvccPager, SimplePager, TransactionHandle, TransactionMode};
        use fsqlite_vfs::UnixVfs;

        const CHILD_ENV: &str = "FSQLITE_NATIVE_ORPHAN_TAIL_STOCK_CHILD";
        if let Some(path) = std::env::var_os(CHILD_ENV) {
            let stock = rusqlite::Connection::open_with_flags(Path::new(&path), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .expect("stock opens recovered and newly published index");
            stock.busy_timeout(std::time::Duration::from_secs(1)).unwrap();
            let sum: i64 = stock.query_row("SELECT sum(n) FROM recovery_rows", [], |row| row.get(0)).unwrap();
            assert_eq!(sum, 50);
            println!("native-orphan-tail-stock-row-sum={sum}");
            return;
        }
        let cx = test_cx();
        for trailing_noncommit in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let (path, wal_bytes, baseline) = native_orphan_tail_database(directory.path(), trailing_noncommit);
        let main_bytes = std::fs::read(&path).unwrap();
        let wal_path = directory.path().join("orphan.db-wal");
        let pager = SimplePager::open_with_cx(&cx, UnixVfs::new(), &path, fsqlite_types::PageSize::DEFAULT)
            .expect("open native orphan fixture");
        let source = pager.wal_index_shm_source().unwrap();
        let (file, _) = UnixVfs::new().open(&cx, Some(&wal_path), VfsOpenFlags::READWRITE | VfsOpenFlags::WAL).unwrap();
        let wal = WalFile::open(&cx, file).expect("open validated orphan WAL");
        let complete_frames = u32::try_from(wal.frame_count()).unwrap();
        let (_, last_marker) = wal.last_commit_frame_header().expect("last committed marker excludes noncommit suffix");
        assert!(complete_frames > baseline.mx_frame);
        let physical_frames = (wal_bytes.len() - WAL_HEADER_SIZE) / wal.frame_size();
        assert_eq!(physical_frames, usize::try_from(complete_frames).unwrap() + usize::from(trailing_noncommit));
        let mut backend = PathRefreshingWalBackend::new(UnixVfs::new(), &path, &wal_path, PAGE_SIZE, wal, false, None);
        backend.attach_wal_index_shm_source(Arc::clone(&source)).unwrap();
        assert!(pager.set_wal_backend_owned(backend).is_ok());
        let table_page = PageNumber::new(2).unwrap();
        let mut first = pager.begin(&cx, TransactionMode::Concurrent).expect("old published prefix remains readable");
        let old_page = first.get_page(&cx, table_page).expect("old table page").into_vec();
        first.write_page(&cx, table_page, &old_page).expect("same page touched by the unadvertised commit");
        let error = first.commit(&cx).expect_err("fresh WRITE owner requests recovery before stale-page conflicts");
        assert!(matches!(error, FrankenError::BusyRecovery), "unexpected first append error: {error}");
        let region = source.map_region(&cx, 0, false).expect("same native index");
        assert_eq!(read_shared_wal_index_header(&region).unwrap(), Some(baseline));
        assert_eq!(std::fs::read(&wal_path).unwrap(), wal_bytes, "classification has no physical append side effect");
        assert_eq!(std::fs::read(&path).unwrap(), main_bytes);
        first.rollback(&cx).expect("unwind the entire old reader and append attempt");
        assert!(!source.owns_external_wal_append_write(&cx).expect("append authority retired"));

        let foreign_pager = SimplePager::open_with_cx(&cx, UnixVfs::new(), &path, fsqlite_types::PageSize::DEFAULT)
            .expect("independent old-reader attachment");
        let foreign_source = foreign_pager.wal_index_shm_source().unwrap();
        let mut foreign_reader = foreign_source.acquire_reader(&cx).expect("hold real old reader slot across recovery refusal");
        assert_eq!(foreign_reader.header().unwrap(), baseline);
        assert!(!foreign_reader.boundary().unwrap().database_only);
        assert!(matches!(pager.begin(&cx, TransactionMode::Concurrent).wait(), Err(FrankenError::Busy | FrankenError::BusyRecovery)), "canonical recovery must respect a foreign old reader");
        assert_eq!(read_shared_wal_index_header(&region).unwrap(), Some(baseline));
        assert_eq!(std::fs::read(&wal_path).unwrap(), wal_bytes);
        foreign_reader.release().expect("release foreign reader before canonical retry");
        drop(foreign_source);
        drop(foreign_pager);

        let mut retry = pager.begin(&cx, TransactionMode::Concurrent).expect("retained request performs canonical recovery at fresh admission");
        let recovered = read_shared_wal_index_header(&region).unwrap().unwrap();
        assert_eq!(recovered.mx_frame, complete_frames);
        assert_eq!((recovered.n_page, recovered.a_frame_cksum), (last_marker.db_size, [last_marker.checksum.s1, last_marker.checksum.s2]));
        assert_eq!(std::fs::read(&wal_path).unwrap(), wal_bytes, "index recovery preserves every WAL byte");
        assert_eq!(std::fs::read(&path).unwrap(), main_bytes, "index recovery preserves every main-file byte");
        let current_page = retry.get_page(&cx, table_page).expect("recovered table page").into_vec();
        assert_ne!(current_page, old_page, "fresh admission sees the recovered stock update");
        retry.write_page(&cx, table_page, &current_page).expect("preserve valid stock B-tree bytes in a new native commit");
        retry.commit(&cx).expect("aligned native prefix passes Path conflict and database-size guards");
        let published = read_shared_wal_index_header(&region).unwrap().unwrap();
        assert!(published.mx_frame > complete_frames);
        assert_eq!(published.i_change, recovered.i_change.wrapping_add(1));
        assert!(!source.owns_external_wal_append_write(&cx).expect("successful append authority retired"));

        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "wal_adapter::tests::test_native_orphan_tail_requests_recovery_before_conflicts_and_fresh_writer_progress", "--nocapture"])
            .env(CHILD_ENV, &path).stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped())
            .spawn().expect("independent stock reader of recovered and newly published WAL");
        let started = std::time::Instant::now();
        loop {
            if child.try_wait().unwrap().is_some() { break; }
            if started.elapsed() > std::time::Duration::from_secs(30) {
                child.kill().expect("stop timed-out stock child");
                child.wait().expect("reap timed-out stock child");
                panic!("stock reader did not finish within 30 seconds");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success(), "stock child failed: stdout={} stderr={}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        assert!(String::from_utf8_lossy(&output.stdout).contains("native-orphan-tail-stock-row-sum=50"));
        }
    }

    #[cfg(all(feature = "native", unix))]
    #[test]
    fn test_native_orphan_preflight_never_creates_or_rebinds_invalid_wal_paths() {
        use fsqlite_pager::SimplePager;
        use fsqlite_vfs::UnixVfs;

        let cx = test_cx();
        for kind in ["missing", "short", "replaced"] {
            let directory = tempfile::tempdir().unwrap();
            let (path, wal_bytes, _) = native_orphan_tail_database(directory.path(), false);
            let wal_path = directory.path().join("orphan.db-wal");
            let preserved_path = directory.path().join("preserved-orphan.wal");
            let pager = SimplePager::open_with_cx(&cx, UnixVfs::new(), &path, fsqlite_types::PageSize::DEFAULT)
                .expect("open native fixture");
            let source = pager.wal_index_shm_source().unwrap();
            let (file, _) = UnixVfs::new().open(&cx, Some(&wal_path), VfsOpenFlags::READWRITE | VfsOpenFlags::WAL).unwrap();
            let wal = WalFile::open(&cx, file).expect("open original WAL descriptor");
            let mut backend = PathRefreshingWalBackend::new(UnixVfs::new(), &path, &wal_path, PAGE_SIZE, wal, true, None);
            backend.attach_wal_index_shm_source(source).unwrap();
            std::fs::rename(&wal_path, &preserved_path).expect("preserve the original inode while perturbing its path");
            let replacement = match kind {
                "short" => Some(b"short".to_vec()),
                "replaced" => Some(wal_bytes.clone()),
                _ => None,
            };
            if let Some(bytes) = &replacement { std::fs::write(&wal_path, bytes).unwrap(); }
            // Exercise the direct wrapper, not only the pager's outer gate.
            backend.append_frame(&cx, 2, &sample_page(0x41), 2)
                .expect_err("native path validation refuses creation and same-header inode replacement");
            assert_eq!(backend.native_recovery_required(), None);
            assert!(!backend.inner.has_pending_publication());
            if let Some(bytes) = replacement {
                assert_eq!(std::fs::read(&wal_path).unwrap(), bytes);
            } else {
                assert!(!wal_path.exists(), "native append never creates a missing path");
            }
            assert_eq!(std::fs::read(&preserved_path).unwrap(), wal_bytes);
        }
    }

    #[cfg(all(feature = "native", unix))]
    #[test]
    fn test_native_orphan_preflight_preserves_live_local_publication() {
        // Heap transport proves only the retained candidate state machine.
        // Real exact WRITE ownership is covered by the Unix and pager controls.
        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let mut fixture = synthetic_shared_publication(&vfs, &cx);
        fixture.adapter.append_frame(&cx, 1, &sample_page(0x35), 1).expect("stage local publisher");
        let pending = fixture.adapter.pending_publication_frames.iter()
            .map(|frame| (frame.page_number, frame.frame_index, frame.is_commit)).collect::<Vec<_>>();
        let before = fixture.adapter.published_snapshot();
        assert!(matches!(fixture.adapter.preflight_native_append(&cx).expect_err("live candidate is not abandoned"), FrankenError::BusyRecovery));
        assert_eq!(fixture.adapter.native_recovery_required(), None);
        assert_eq!(fixture.adapter.pending_publication_frames.iter()
            .map(|frame| (frame.page_number, frame.frame_index, frame.is_commit)).collect::<Vec<_>>(), pending);
        assert!(fixture.adapter.native_publication.is_some());
        assert_eq!(fixture.adapter.published_snapshot(), before);
        assert_eq!(read_shared_wal_index_header(&fixture.region).unwrap(), Some(fixture.baseline));
        fixture.adapter.sync(&cx).expect("same retained publisher still completes");
        assert_eq!(fixture.adapter.native_recovery_required(), None);
        assert_eq!(read_shared_wal_index_header(&fixture.region).unwrap().unwrap().mx_frame, 1);
    }

    /// Synthetic transport control: the native gate itself is covered by the
    /// VFS recovery tests; this keeper exercises the actual index data plane.
    #[cfg(all(feature = "native", unix))]
    #[test]
    fn test_shared_recovery_rebuilds_exact_commit_and_preserves_physical_tail() {
        use fsqlite_wal::wal_index::{invalidate_shared_wal_index_header, lookup_native_wal_index_frame};

        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let mut fixture = synthetic_shared_publication(&vfs, &cx);
        let page = sample_page(0x53);
        // Bypass publication to supply a pre-existing physical WAL, as a
        // different process would leave it before this recovery owner exists.
        fixture.adapter.wal.append_frame(&cx, 1, &page, 1).expect("first physical commit");
        fixture.adapter.wal.append_frame(&cx, 2, &page, 2).expect("final physical commit");
        let marker = fixture.adapter.wal.last_commit_frame_header().unwrap().1;
        fixture.adapter.wal.append_frame(&cx, 3, &page, 0).expect("uncommitted physical suffix");
        let size = fixture.adapter.wal.file().file_size(&cx).unwrap();
        let mut before = vec![0; usize::try_from(size).unwrap()];
        fixture.adapter.wal.file().read(&cx, &mut before, 0).expect("capture complete physical WAL");
        let shared_before = fixture.region.lock().to_vec();
        invalidate_shared_wal_index_header(&fixture.region).unwrap();
        fixture.adapter.recover_native_index(&cx).expect("rebuild native index");
        let header = read_shared_wal_index_header(&fixture.region).unwrap().unwrap();
        assert_eq!((header.mx_frame, header.n_page, header.i_change), (2, 2, 0));
        assert_eq!(header.a_frame_cksum, [marker.checksum.s1, marker.checksum.s2]);
        assert_eq!(fixture.adapter.wal.frame_count(), 2);
        assert_eq!(fixture.adapter.wal.last_fsynced_frame_count(), 0, "recovery grants no new fsync authority");
        let shared_after = fixture.region.lock().to_vec();
        assert_eq!(lookup_native_wal_index_frame(&shared_after, 0, 1, header.mx_frame).unwrap(), Some(1));
        assert_eq!(lookup_native_wal_index_frame(&shared_after, 0, 2, header.mx_frame).unwrap(), Some(2));
        assert_eq!(lookup_native_wal_index_frame(&shared_after, 0, 3, header.mx_frame).unwrap(), None);
        assert_eq!(&shared_after[120..128], &shared_before[120..128]);
        assert_eq!(&shared_after[132..136], &shared_before[132..136]);
        assert_eq!(fixture.region.atomic_load_u32_ne(96, std::sync::atomic::Ordering::Acquire).unwrap(), 0);
        assert_eq!(fixture.region.atomic_load_u32_ne(128, std::sync::atomic::Ordering::Acquire).unwrap(), 0);
        assert_eq!(fixture.region.atomic_load_u32_ne(104, std::sync::atomic::Ordering::Acquire).unwrap(), 2);
        let mut after = vec![0; before.len()];
        fixture.adapter.wal.file().read(&cx, &mut after, 0).expect("capture unchanged physical WAL");
        assert_eq!(after, before);
        assert_eq!(fixture.adapter.wal.file().file_size(&cx).unwrap(), size);
        fixture.adapter.recover_native_index(&cx).expect("coherent peer repair is idempotent");
        assert_eq!(fixture.region.lock().to_vec(), shared_after);
    }

    #[cfg(all(feature = "native", unix))]
    #[test]
    fn test_shared_recovery_scan_error_leaves_header_invalid_and_exact_retry() {
        use fsqlite_wal::wal_index::invalidate_shared_wal_index_header;

        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let mut fixture = synthetic_shared_publication(&vfs, &cx);
        let page = sample_page(0x63);
        fixture.adapter.wal.append_frame(&cx, 1, &page, 1).expect("first physical commit");
        fixture.adapter.wal.append_frame(&cx, 2, &page, 2).expect("second physical commit");
        let size = fixture.adapter.wal.file().file_size(&cx).unwrap();
        invalidate_shared_wal_index_header(&fixture.region).unwrap();
        vfs.fail_wal_frame_header_read_after(1);
        assert!(matches!(fixture.adapter.recover_native_index(&cx).wait(), Err(FrankenError::Io(_))));
        assert_eq!(read_shared_wal_index_header(&fixture.region).unwrap(), None);
        assert_eq!(fixture.adapter.published_snapshot.last_commit_frame, None);
        assert_eq!(fixture.adapter.wal.file().file_size(&cx).unwrap(), size);
        assert_eq!(vfs.faults.lock().unwrap().wal_header_reads_completed, 1,
            "fault occurs after full-frame validation, during mapping metadata scan");
        fixture.adapter.recover_native_index(&cx).expect("retry after partial scan failure");
        assert_eq!(read_shared_wal_index_header(&fixture.region).unwrap().unwrap().mx_frame, 2);
        assert!(!fixture.adapter.has_pending_publication());
    }

    #[cfg(all(feature = "native", unix))]
    #[test]
    fn test_shared_recovery_empty_wal_and_retained_publisher_refusal() {
        use fsqlite_wal::wal_index::invalidate_shared_wal_index_header;

        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let mut fixture = synthetic_shared_publication(&vfs, &cx);
        invalidate_shared_wal_index_header(&fixture.region).unwrap();
        fixture.adapter.recover_native_index(&cx).expect("recover empty native generation");
        let header = read_shared_wal_index_header(&fixture.region).unwrap().unwrap();
        assert_eq!((header.mx_frame, header.n_page, header.a_frame_cksum), (0, 0, [0, 0]));
        assert_eq!(header.a_salt, fixture.baseline.a_salt);
        assert_eq!(fixture.region.atomic_load_u32_ne(104, std::sync::atomic::Ordering::Acquire).unwrap(), 0);
        let page = sample_page(0x73);
        fixture.adapter.append_frame(&cx, 1, &page, 1).expect("stage an owned publication");
        let before = fixture.region.lock().to_vec();
        let size = fixture.adapter.wal.file().file_size(&cx).unwrap();
        assert!(matches!(fixture.adapter.recover_native_index(&cx).wait(), Err(FrankenError::BusyRecovery)));
        assert_eq!(fixture.region.lock().to_vec(), before);
        assert_eq!(fixture.adapter.wal.file().file_size(&cx).unwrap(), size);
        assert!(fixture.adapter.has_pending_publication());
        fixture.adapter.sync(&cx).expect("original publication owner remains usable");
        assert_eq!(read_shared_wal_index_header(&fixture.region).unwrap().unwrap().mx_frame, 1);
    }

    #[cfg(all(feature = "native", unix))]
    #[test]
    fn test_shared_publication_transport_failed_sync_two_markers_and_suffix_retry() {
        use fsqlite_wal::wal_index::lookup_native_wal_index_frame;

        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let mut fixture = synthetic_shared_publication(&vfs, &cx);
        let before = fixture.region.lock().to_vec();
        let page = sample_page(0x67);
        let frames = [
            WalFrameRef { page_number: 1, page_data: &page, db_size_if_commit: 1 },
            WalFrameRef { page_number: 2, page_data: &page, db_size_if_commit: 2 },
            WalFrameRef { page_number: 3, page_data: &page, db_size_if_commit: 0 },
        ];
        fixture.adapter.append_frames(&cx, &frames).expect("append two markers and suffix");
        assert_eq!(fixture.region.lock().to_vec(), before, "append only stages shared publication");
        assert!(fixture.adapter.pending_append_attempt.is_none());
        assert!(fixture.adapter.native_publication.is_some());
        vfs.fail_next_wal_sync();
        fixture.adapter.sync(&cx).expect_err("injected physical sync refusal");
        assert_eq!(fixture.region.lock().to_vec(), before);
        assert_eq!(fixture.adapter.published_snapshot.last_commit_frame, None);
        fixture.adapter.refresh_published_snapshot(&cx)
            .expect_err("refresh cannot trim the owned uncommitted suffix");
        assert_eq!(fixture.adapter.wal.frame_count(), 3);
        fixture.adapter.sync(&cx).expect("publish after successful retry");
        let first = read_shared_wal_index_header(&fixture.region).unwrap().unwrap();
        assert_eq!((first.mx_frame, first.i_change, first.n_page), (2, 1, 2));
        let (marker_index, marker) = fixture.adapter.wal.last_commit_frame_header().unwrap();
        assert_eq!(marker_index, 1);
        assert_eq!(first.a_frame_cksum, [marker.checksum.s1, marker.checksum.s2]);
        assert_ne!(marker.checksum, fixture.adapter.wal.running_checksum(), "physical suffix has a later checksum");
        assert_eq!(fixture.adapter.published_snapshot.commit_count, 2);
        assert_eq!(fixture.adapter.pending_publication_frames.len(), 1);
        let published = fixture.region.lock().to_vec();
        assert_eq!(&published[96..136], &before[96..136]);
        assert_eq!(lookup_native_wal_index_frame(&published, 0, 3, 3).unwrap(), None);
        fixture.adapter.sync(&cx).expect("no marker means no second publication");
        assert_eq!(fixture.region.lock().to_vec(), published);
        fixture.adapter.append_frame(&cx, 4, &page, 4).expect("extend retained suffix");
        fixture.adapter.publish_authorized_deferred_commit(&cx).expect("explicit deferred authority");
        let next = read_shared_wal_index_header(&fixture.region).unwrap().unwrap();
        assert_eq!((next.mx_frame, next.i_change), (4, 2));
        let completed = fixture.region.lock().to_vec();
        assert_eq!(&completed[96..136], &before[96..136]);
        assert_eq!(lookup_native_wal_index_frame(&completed, 0, 3, next.mx_frame).unwrap(), Some(3));
        assert_eq!(lookup_native_wal_index_frame(&completed, 0, 4, next.mx_frame).unwrap(), Some(4));
        assert!(!fixture.adapter.has_pending_publication());
        fixture.adapter.sync(&cx).expect("durability after deferred publication");
        assert_eq!(fixture.region.lock().to_vec(), completed);
    }

    #[cfg(all(feature = "native", unix))]
    #[test]
    fn test_shared_publication_transport_refuses_invalid_baseline_before_raw_or_prepared_write() {
        use fsqlite_wal::wal_index::publish_shared_wal_index_header;

        for prepared in [false, true] {
            for missing in [false, true] {
                let cx = test_cx();
                let vfs = CheckpointHandoffFaultVfs::new();
                let mut fixture = synthetic_shared_publication(&vfs, &cx);
                if missing {
                    fixture.region.atomic_store_u32_ne(0, 0, std::sync::atomic::Ordering::Release)
                        .expect("simulate an uninitialized/torn header");
                } else {
                    let mut changed = fixture.baseline;
                    changed.a_salt[0] ^= 1;
                    changed.update_checksum().unwrap();
                    publish_shared_wal_index_header(&fixture.region, &changed).unwrap();
                }
                let before = fixture.region.lock().to_vec();
                let mut wal_before = [0; WAL_HEADER_SIZE];
                fixture.adapter.wal.file().read(&cx, &mut wal_before, 0).expect("capture WAL bytes");
                let page = sample_page(0x78);
                let frames = [WalFrameRef { page_number: 1, page_data: &page, db_size_if_commit: 1 }];
                let completion = VfsWriteCompletion::new();
                if prepared {
                    let mut batch = fixture.adapter.prepare_append_frames(&frames).unwrap().unwrap();
                    fixture.adapter.append_prepared_frames_tracked(&cx, &mut batch, completion.clone())
                        .expect_err("invalid native baseline refuses prepared append");
                } else {
                    fixture.adapter.append_frames_tracked(&cx, &frames, completion.clone())
                        .expect_err("invalid native baseline refuses raw append");
                }
                assert_eq!(completion.state(), VfsWriteCompletionState::Error);
                assert!(!fixture.adapter.has_pending_publication());
                assert_eq!(fixture.adapter.wal.frame_count(), 0);
                assert_eq!(fixture.adapter.wal.file().file_size(&cx).unwrap(), u64::try_from(WAL_HEADER_SIZE).unwrap());
                let mut wal_after = [0; WAL_HEADER_SIZE];
                fixture.adapter.wal.file().read(&cx, &mut wal_after, 0).expect("reread WAL bytes");
                assert_eq!(wal_after, wal_before);
                assert_eq!(fixture.region.lock().to_vec(), before);
                publish_shared_wal_index_header(&fixture.region, &fixture.baseline).unwrap();
                fixture.adapter.append_frames(&cx, &frames).expect("retry from a valid baseline");
                fixture.adapter.sync(&cx).expect("publish retry");
                assert_eq!(read_shared_wal_index_header(&fixture.region).unwrap().unwrap().mx_frame, 1);
            }
        }
    }

    #[cfg(all(feature = "native", unix))]
    #[test]
    fn test_shared_publication_transport_refused_header_keeps_private_horizon() {
        use fsqlite_wal::wal_index::publish_shared_wal_index_header;

        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let mut fixture = synthetic_shared_publication(&vfs, &cx);
        let page = sample_page(0x81);
        fixture.adapter.append_frame(&cx, 1, &page, 1).expect("stage commit");
        let mut changed = fixture.baseline;
        changed.i_change = 27;
        changed.update_checksum().unwrap();
        publish_shared_wal_index_header(&fixture.region, &changed).unwrap();
        let changed_bytes = fixture.region.lock().to_vec();
        fixture.adapter.sync(&cx).expect_err("foreign header prevents native publication");
        assert_eq!(fixture.adapter.wal.last_fsynced_frame_count(), 1);
        assert!(fixture.adapter.has_pending_publication());
        assert_eq!(fixture.adapter.published_snapshot.last_commit_frame, None);
        assert_eq!(fixture.adapter.read_page(&cx, 1).expect("retain prior private horizon"), None);
        assert_eq!(fixture.region.lock().to_vec(), changed_bytes);
        assert_eq!(fixture.adapter.published_snapshot.last_commit_frame, None);
        publish_shared_wal_index_header(&fixture.region, &fixture.baseline).unwrap();
        fixture.adapter.sync(&cx).expect("retry identical retained target");
        assert_eq!(fixture.adapter.read_page(&cx, 1).expect("published page"), Some(page));
        assert_eq!(read_shared_wal_index_header(&fixture.region).unwrap().unwrap().i_change, 0);
        assert!(!fixture.adapter.has_pending_publication());
    }

    #[cfg(all(feature = "native", unix, feature = "fault-injection"))]
    #[test]
    fn test_shared_publication_transport_absent_append_restores_prior_uncommitted_plan() {
        use fsqlite_wal::wal_index::lookup_native_wal_index_frame;

        let _fault_session = fsqlite_wal::fault_hooks::FaultInjectionSessionLock::new().lock().unwrap();
        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let mut fixture = synthetic_shared_publication(&vfs, &cx);
        let page = sample_page(0x89);
        fixture.adapter.append_frame(&cx, 1, &page, 0).expect("stage prior uncommitted frame");
        let before = fixture.region.lock().to_vec();
        let wal_size = fixture.adapter.wal.file().file_size(&cx).unwrap();
        vfs.fail_next_wal_write_after_prefix(0);
        fixture.adapter.append_frame(&cx, 2, &page, 2).expect_err("no-byte append failure");
        assert!(fixture.adapter.pending_append_attempt.is_some());
        assert_eq!(fixture.adapter.wal.file().file_size(&cx).unwrap(), wal_size);
        assert_eq!(fixture.adapter.wal.frame_count(), 1);
        assert_eq!(fixture.region.lock().to_vec(), before);
        // The injected zero-byte outcome supplies the exact absence control.
        // Certificate reconciliation itself is covered by the owning keepers.
        fixture.adapter.discard_reconciled_append().expect("restore the prior publication owner");
        assert!(fixture.adapter.pending_append_attempt.is_none());
        assert_eq!(fixture.adapter.pending_publication_frames.len(), 1);
        assert_eq!(fixture.adapter.native_publication.as_ref().unwrap().baseline(), fixture.baseline);
        fixture.adapter.append_frame(&cx, 2, &page, 2).expect("retry commit after exact absence");
        fixture.adapter.sync(&cx).expect("publish prior frame and retry");
        let bytes = fixture.region.lock().to_vec();
        assert_eq!(lookup_native_wal_index_frame(&bytes, 0, 1, 2).unwrap(), Some(1));
        assert_eq!(lookup_native_wal_index_frame(&bytes, 0, 2, 2).unwrap(), Some(2));
        assert_eq!(read_shared_wal_index_header(&fixture.region).unwrap().unwrap().i_change, 0);
    }

    #[cfg(all(feature = "native", unix, feature = "fault-injection"))]
    #[test]
    fn test_shared_publication_transport_dropped_append_reconciles_native_header_once() {
        use fsqlite_wal::wal_index::publish_shared_wal_index_header;

        let _fault_session = fsqlite_wal::fault_hooks::FaultInjectionSessionLock::new().lock().unwrap();
        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let SyntheticSharedPublication { _pager: pager, adapter, region, baseline } =
            synthetic_shared_publication(&vfs, &cx);
        let wal = match adapter.into_inner() {
            Ok(wal) => wal,
            Err(retained) => panic!(
                "fresh synthetic fixture retained state: publication={}, reader={}, recovery={}",
                retained.has_pending_publication(),
                retained.native_read_binding.is_some(),
                retained.native_recovery_requested.is_some(),
            ),
        };
        let mut backend = PathRefreshingWalBackend::new(
            vfs.clone(), Path::new("test.db"), Path::new("test.db-wal"), PAGE_SIZE, wal, true, None,
        );
        backend.attach_wal_index_shm_source(pager.wal_index_shm_source().unwrap()).unwrap();
        let page = sample_page(0x91);
        let mut certificate = sample_certificate(1, 1, vec![1]);
        certificate.wal_frame_payload_digest = test_frame_payload_digest(1, &page, 1);
        certificate.certificate_crc32c = certificate.computed_crc32c();
        backend.persist_parallel_wal_commit_certificate(&cx, &certificate, 1, 1, true)
            .expect("persist exact authority before append");
        let frames = [WalFrameRef { page_number: 1, page_data: &page, db_size_if_commit: 1 }];
        let completion = VfsWriteCompletion::new();
        vfs.pause_after_next_wal_write();
        {
            let mut future = backend.append_frames_tracked(&cx, &frames, completion.clone());
            let mut task_cx = std::task::Context::from_waker(std::task::Waker::noop());
            assert!(matches!(
                std::future::Future::poll(future.as_mut(), &mut task_cx),
                std::task::Poll::Pending,
            ));
            assert_eq!(completion.state(), VfsWriteCompletionState::Success);
        }
        assert_eq!(backend.inner.wal.frame_count(), 0);
        assert!(backend.inner.pending_append_attempt.is_some());
        assert!(backend.inner.native_publication.is_some());
        assert_eq!(read_shared_wal_index_header(&region).unwrap(), Some(baseline));
        let mut changed = baseline;
        changed.i_change = 19;
        changed.update_checksum().unwrap();
        publish_shared_wal_index_header(&region, &changed).unwrap();
        backend.reconcile_parallel_wal_commit(&cx, &certificate, 1, 1, true)
            .expect_err("native header refusal after exact content proof and fsync");
        assert_eq!(backend.inner.wal.last_fsynced_frame_count(), 1);
        assert!(backend.inner.pending_append_attempt.as_ref().unwrap().authorized);
        assert_eq!(backend.inner.published_snapshot.last_commit_frame, None);
        assert_eq!(read_shared_wal_index_header(&region).unwrap(), Some(changed));
        publish_shared_wal_index_header(&region, &baseline).unwrap();
        assert_eq!(
            backend.reconcile_parallel_wal_commit(&cx, &certificate, 1, 1, true)
                .expect("retry exact native and private publication"),
            ParallelWalCommitReconciliation::Authorized,
        );
        assert!(!backend.inner.has_pending_publication());
        assert_eq!(backend.inner.published_snapshot.commit_count, 1);
        let published = region.lock().to_vec();
        assert_eq!(read_shared_wal_index_header(&region).unwrap().unwrap().i_change, 0);
        backend.sync(&cx).expect("ordinary sync after exact reconciliation");
        assert_eq!(region.lock().to_vec(), published);
        assert_eq!(completion.state(), VfsWriteCompletionState::Success);
    }

    #[cfg(all(feature = "native", unix))]
    #[test]
    fn test_native_read_binding_keeps_captured_prefix_and_exact_token() {
        use fsqlite_wal::wal_index::publish_shared_wal_index_header;
        use std::sync::atomic::Ordering;

        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let mut fixture = synthetic_shared_publication(&vfs, &cx);
        let old_page = sample_page(0x31);
        fixture.adapter.append_frame(&cx, 1, &old_page, 1).expect("old commit");
        fixture.adapter.sync(&cx).expect("publish old commit");
        let old_header = read_shared_wal_index_header(&fixture.region).unwrap().unwrap();
        // Seed a usable reader mark: this heap fixture tests binding metadata,
        // not native lock acquisition or interprocess exclusion.
        fixture.region.atomic_store_u32_ne(96, 0, Ordering::Release).unwrap();
        fixture.region.atomic_store_u32_ne(104, 1, Ordering::Release).unwrap();
        let source = Arc::clone(fixture.adapter.wal_index_shm_source.as_ref().unwrap());
        let mut lease = source.acquire_reader(&cx).expect("capture old shared prefix");
        let mut sibling = source.acquire_reader(&cx).expect("different owner, same prefix");
        let binding = lease.binding().unwrap();
        let token = binding.token().clone();
        let sibling_token = sibling.binding().unwrap().token().clone();
        assert!(!token.matches(&sibling_token));

        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::WAL;
        let (file, _) = vfs.open(&cx, Some(Path::new("test.db-wal")), flags).unwrap();
        let mut peer = WalFile::open(&cx, file).expect("independent physical WAL handle");
        let new_page = sample_page(0x72);
        peer.append_frame(&cx, 1, &new_page, 0).expect("newer uncommitted page");
        peer.append_frame(&cx, 2, &new_page, 2).expect("newer commit");
        peer.sync(&cx, SyncFlags::NORMAL).unwrap();
        let (_, terminal) = peer.last_commit_frame_header().unwrap();
        let mut newer_header = old_header;
        newer_header.mx_frame = 3;
        newer_header.n_page = 2;
        newer_header.i_change = newer_header.i_change.wrapping_add(1);
        newer_header.a_frame_cksum = [terminal.checksum.s1, terminal.checksum.s2];
        newer_header.update_checksum().unwrap();
        publish_shared_wal_index_header(&fixture.region, &newer_header).unwrap();

        assert_eq!(
            fixture.adapter.begin_native_read(&cx, binding).expect("bind old native horizon"),
            WalNativeReadOutcome::Ready,
        );
        assert_eq!(fixture.adapter.wal.frame_count(), 3, "physical refresh saw the newer commit");
        let pinned = fixture.adapter.pinned_read_snapshot().unwrap();
        assert_eq!(pinned.last_commit_frame, Some(0));
        assert_eq!(fixture.adapter.native_read_binding().unwrap().header().n_page, 1);
        assert_eq!(fixture.adapter.read_page_pinned(&cx, 1).expect("old pinned page"), Some(old_page));
        assert_eq!(fixture.adapter.read_page_pinned(&cx, 2).expect("new page stays hidden"), None);
        assert_eq!(fixture.adapter.native_recovery_required(), None);
        fixture.adapter.preflight_native_append(&cx).expect("aligned shared prefix advances only the conflict view");
        assert_eq!(fixture.adapter.published_snapshot().last_commit_frame, Some(2));
        assert_eq!(fixture.adapter.pinned_read_snapshot(), Some(pinned));
        assert!(fixture.adapter.native_read_binding().unwrap().token().matches(&token));
        assert_eq!(fixture.adapter.read_page_pinned(&cx, 2).expect("preflight preserves old read horizon"), None);
        assert_eq!(fixture.adapter.native_recovery_required(), None);
        assert!(matches!(fixture.adapter.end_native_read(&sibling_token), Err(FrankenError::BusyRecovery)));
        assert_eq!(fixture.adapter.pinned_read_snapshot(), Some(pinned));
        assert!(fixture.adapter.native_read_binding().unwrap().token().matches(&token));
        assert!(matches!(fixture.adapter.inner_mut(), Err(FrankenError::Busy)));
        fixture.adapter.refresh_published_snapshot(&cx).expect_err("live pin refuses unbound refresh");
        assert_eq!(fixture.adapter.pinned_read_snapshot(), Some(pinned));
        fixture.adapter.end_native_read(&token).unwrap();
        fixture.adapter.end_native_read(&token).expect("same-token retirement is idempotent");
        assert!(fixture.adapter.pinned_read_snapshot().is_none());
        lease.release().expect("physical release follows exact backend retirement");
        sibling.release().expect("release sibling claim");
        fixture.region.atomic_store_u32_ne(104, 3, Ordering::Release).unwrap();
        let mut next = source.acquire_reader(&cx).expect("capture newer publication");
        let next_binding = next.binding().unwrap();
        let next_token = next_binding.token().clone();
        fixture.adapter.begin_native_read(&cx, next_binding).expect("bind next transaction");
        assert_eq!(fixture.adapter.pinned_read_snapshot().unwrap().last_commit_frame, Some(2));
        assert_eq!(fixture.adapter.read_page_pinned(&cx, 1).expect("new pinned page"), Some(new_page));
        fixture.adapter.end_native_read(&next_token).unwrap();
        next.release().expect("release next claim");
        peer.close(&cx).unwrap();
    }

    #[cfg(all(feature = "native", unix))]
    #[test]
    fn test_native_read_binding_refuses_database_only_lease_without_changing_pin() {
        use std::sync::atomic::Ordering;

        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let mut fixture = synthetic_shared_publication(&vfs, &cx);
        fixture.region.atomic_store_u32_ne(96, 0, Ordering::Release).unwrap();
        let source = Arc::clone(fixture.adapter.wal_index_shm_source.as_ref().unwrap());
        let mut lease = source.acquire_reader(&cx).expect("database-only lease");
        assert!(lease.boundary().unwrap().database_only);
        let before = fixture.adapter.published_snapshot();
        assert!(matches!(
            fixture.adapter.begin_native_read(&cx, lease.binding().unwrap()).expect_err("slot zero cannot bind WAL bytes"),
            FrankenError::BusyRecovery,
        ));
        assert_eq!(fixture.adapter.published_snapshot(), before);
        assert!(fixture.adapter.native_read_binding().is_none());
        assert!(fixture.adapter.pinned_read_snapshot().is_none());
        lease.release().expect("release database-only claim");
    }

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
        let mut retained = match staged.into_inner() {
            Ok(_) => panic!("unsynced extraction must return its adapter"),
            Err(retained) => retained,
        };
        assert_eq!(retained.pending_publication_commit, Some(0));
        assert_eq!(retained.pending_publication_frames.len(), 1);
        retained.sync(&cx).expect("retry on the same returned adapter");
        let retained_wal = (*retained).into_inner()
            .unwrap_or_else(|_| panic!("retry drained the same adapter"));
        assert_eq!(retained_wal.frame_count(), 1);

        let synced_vfs = MemoryVfs::new();
        let mut synced = make_adapter(&synced_vfs, &cx);
        synced
            .append_frame(&cx, 1, &sample_page(0), 1)
            .expect("append");
        synced.sync(&cx).expect("sync staged commit");

        assert_eq!(synced.inner().frame_count(), 1);

        let wal = synced.into_inner()
            .unwrap_or_else(|_| panic!("sync drained the staged frames"));
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
    fn test_publication_scan_io_failure_preserves_exact_prior_snapshot_and_retries() {
        for pin_reader in [false, true] {
            let cx = test_cx();
            let vfs = CheckpointHandoffFaultVfs::new();
            let mut writer = make_fault_adapter(&vfs, &cx);
            let old_page = sample_page(0x41);
            let updated_page = sample_page(0x42);
            let added_page = sample_page(0x43);
            writer
                .append_frame(&cx, 1, &old_page, 1)
                .expect("seed commit");
            writer.sync(&cx).expect("publish seed commit");
            let (file, _) = vfs
                .open(
                    &cx,
                    Some(Path::new("test.db-wal")),
                    VfsOpenFlags::READWRITE | VfsOpenFlags::WAL,
                )
                .expect("open peer reader WAL");
            let mut reader =
                WalBackendAdapter::new(WalFile::open(&cx, file).expect("open WAL"));
            reader
                .refresh_published_snapshot(&cx)
                .expect("publish old prefix");
            if pin_reader {
                reader.begin_transaction(&cx).expect("pin old prefix");
            }
            let before = reader.published_snapshot();
            let before_map = reader.published_snapshot.page_index.as_ref().clone();
            let before_map_address = Arc::as_ptr(&reader.published_snapshot.page_index);
            let before_next_sequence = reader.next_publication_seq;
            assert_eq!(before.last_commit_frame, Some(0));
            assert_eq!(before.commit_count, 1);
            assert_eq!(before_map, HashMap::from([(1, 0)]));

            writer
                .append_frame(&cx, 1, &updated_page, 0)
                .expect("overwrite tracked page");
            writer
                .append_frame(&cx, 2, &added_page, 2)
                .expect("append peer commit");
            writer.sync(&cx).expect("publish peer commit");
            vfs.fail_wal_frame_header_read_after(1);
            let error = reader
                .refresh_published_snapshot(&cx)
                .expect_err("second publication header read must fail");
            assert!(matches!(&error, FrankenError::Io(_)));
            assert!(
                error
                    .to_string()
                    .contains("injected WAL publication header read failure")
            );
            {
                let faults = vfs
                    .faults
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                assert_eq!(faults.wal_header_reads_completed, 1);
                assert_eq!(faults.wal_header_reads_before_failure, None);
            }
            assert_eq!(
                reader.wal.frame_count(),
                3,
                "physical refresh completed before the fault"
            );
            assert_eq!(
                reader.published_snapshot(),
                before,
                "all publication metadata must survive"
            );
            assert_eq!(reader.published_snapshot.page_index.as_ref(), &before_map);
            assert_eq!(
                Arc::as_ptr(&reader.published_snapshot.page_index),
                before_map_address
            );
            assert_eq!(reader.next_publication_seq, before_next_sequence);
            assert_eq!(reader.pinned_read_snapshot(), pin_reader.then_some(before));
            assert_eq!(
                reader
                    .resolve_visible_frame(&cx, &reader.published_snapshot, 1)
                    .expect("old lookup"),
                WalPageLookupResolution::AuthoritativeHit { frame_index: 0 },
                "a failed scan must not point an old horizon at a newer frame"
            );

            let retried = reader
                .refresh_published_snapshot(&cx)
                .expect("retry publication scan");
            assert_eq!(retried.generation, before.generation);
            assert_eq!(retried.publication_seq, before_next_sequence);
            assert_eq!(retried.last_commit_frame, Some(2));
            assert_eq!(retried.commit_count, 2);
            assert_eq!(reader.next_publication_seq, before_next_sequence + 1);
            assert_eq!(
                reader.published_snapshot.page_index.as_ref(),
                &HashMap::from([(1, 1), (2, 2)])
            );
            if pin_reader {
                assert_eq!(
                    reader.read_page(&cx, 1).expect("pinned old page"),
                    Some(old_page)
                );
                assert_eq!(reader.read_page(&cx, 2).expect("pinned absence"), None);
                reader.begin_transaction(&cx).expect("pin refreshed prefix");
            }
            assert_eq!(
                reader.read_page(&cx, 1).expect("updated page after retry"),
                Some(updated_page)
            );
            assert_eq!(
                reader.read_page(&cx, 2).expect("added page after retry"),
                Some(added_page)
            );
        }
    }

    #[test]
    fn test_publication_scan_preserves_capacity_admission_updates_and_commit_count() {
        for cap in [0, 1, 2] {
            let cx = test_cx();
            let vfs = MemoryVfs::new();
            let mut writer = make_adapter(&vfs, &cx);
            writer
                .append_frame(&cx, 1, &sample_page(0x10), 1)
                .expect("seed commit");
            writer.sync(&cx).expect("publish seed commit");
            let wal = WalFile::open(&cx, open_wal_file(&vfs, &cx)).expect("open peer WAL");
            let mut reader = WalBackendAdapter::new(wal);
            reader.set_page_index_cap(cap);
            reader
                .refresh_published_snapshot(&cx)
                .expect("publish seed prefix");

            // Page 2 is the first new key. Page 3 carries a commit marker even
            // when dropped. Repeated page 2 and old page 1 must update at cap.
            for (page_number, byte) in [(2, 0x20_u8), (3, 0x30), (2, 0x21), (1, 0x11)] {
                writer
                    .append_frame(&cx, page_number, &sample_page(byte), 3)
                    .expect("append commit");
            }
            writer.sync(&cx).expect("publish all four new commits");
            let refreshed = reader
                .refresh_published_snapshot(&cx)
                .expect("scan peer commits");
            let mut expected_index = HashMap::new();
            if cap >= 1 {
                expected_index.insert(1, 4);
            }
            if cap >= 2 {
                expected_index.insert(2, 3);
            }
            assert_eq!(reader.published_snapshot.page_index.as_ref(), &expected_index);
            assert!(refreshed.latest_frame_entries <= cap);
            assert_eq!(refreshed.last_commit_frame, Some(4));
            assert_eq!(
                refreshed.commit_count,
                5,
                "dropped pages still carry commit markers"
            );
            assert!(refreshed.index_is_partial);
            for (page_number, byte) in [(1, 0x11_u8), (2, 0x21), (3, 0x30)] {
                assert_eq!(
                    reader
                        .read_page(&cx, page_number)
                        .expect("indexed or fallback lookup"),
                    Some(sample_page(byte))
                );
            }

            // A later scan touching only a tracked key must retain the partial
            // flag established when an earlier scan dropped another page.
            writer
                .append_frame(&cx, 1, &sample_page(0x12), 3)
                .expect("update old key again");
            writer.sync(&cx).expect("publish later update");
            let later = reader
                .refresh_published_snapshot(&cx)
                .expect("scan later update");
            assert_eq!(later.commit_count, 6);
            assert!(later.index_is_partial);
            assert!(later.latest_frame_entries <= cap);
            assert_eq!(
                reader
                    .read_page(&cx, 1)
                    .expect("latest tracked or fallback page"),
                Some(sample_page(0x12))
            );
        }
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

    #[cfg(feature = "fault-injection")]
    mod postwrite_append_faults {
        use fsqlite_vfs::VfsWriteCompletionState;
        use fsqlite_wal::fault_hooks::{
            self, CrashBoundary, FaultHookArm, FaultInjectionSessionLock,
        };

        use super::*;

        #[derive(Clone, Copy, Debug)]
        enum AppendPath {
            Raw,
            RawTracked,
            Prepared,
            PreparedTracked,
        }

        impl AppendPath {
            fn append(
                self,
                adapter: &mut WalBackendAdapter<<MemoryVfs as Vfs>::File>,
                cx: &Cx,
                frames: &[WalFrameRef<'_>],
                completion: VfsWriteCompletion,
            ) -> Result<()> {
                match self {
                    Self::Raw => adapter.append_frames(cx, frames).wait(),
                    Self::RawTracked => adapter
                        .append_frames_tracked(cx, frames, completion)
                        .wait(),
                    Self::Prepared | Self::PreparedTracked => {
                        let mut prepared = adapter
                            .prepare_append_frames(frames)?
                            .expect("nonempty prepared batch");
                        if matches!(self, Self::PreparedTracked) {
                            adapter
                                .append_prepared_frames_tracked(cx, &mut prepared, completion)
                                .wait()
                        } else {
                            adapter.append_prepared_frames(cx, &mut prepared).wait()
                        }
                    }
                }
            }
        }

        fn inject_append_boundary(
            path: AppendPath,
            adapter: &mut WalBackendAdapter<<MemoryVfs as Vfs>::File>,
            cx: &Cx,
            frames: &[WalFrameRef<'_>],
            boundary: CrashBoundary,
        ) {
            let scenario = format!("postwrite-publication-owner-{path:?}-{boundary}");
            fault_hooks::arm_crash_boundary(
                boundary,
                FaultHookArm::new("bd-zywqc.22", &scenario, "publication-ownership"),
            );
            let completion = VfsWriteCompletion::new();
            let error = path
                .append(adapter, cx, frames, completion.clone())
                .expect_err("the injected append error must remain an error");
            assert!(matches!(&error, FrankenError::Io(_)));
            assert!(error.to_string().contains(boundary.as_str()), "{error}");
            let records = fault_hooks::take_records();
            assert_eq!(records.len(), 1, "the intended boundary must fire exactly once");
            assert_eq!(records[0].point, boundary.as_str());
            assert_eq!(records[0].scenario_id, scenario);
            if matches!(path, AppendPath::RawTracked | AppendPath::PreparedTracked) {
                let expected = if boundary == CrashBoundary::BeforeWalFrameAppend {
                    VfsWriteCompletionState::Error
                } else {
                    VfsWriteCompletionState::Success
                };
                assert_eq!(completion.state(), expected);
                // Neither preflight failure nor source success may be relabeled
                // to turn the append API's error into an accepted commit.
                assert!(!completion.complete_success());
                assert!(!completion.complete_error());
            }
        }

        fn assert_prewrite_refusal(path: AppendPath, cx: &Cx, frames: &[WalFrameRef<'_>]) {
            let vfs = MemoryVfs::new();
            let mut adapter = make_adapter(&vfs, cx);
            let initial_checksum = adapter.wal.running_checksum();
            let mut header_bytes = [0; WAL_HEADER_SIZE];
            assert_eq!(
                adapter.wal.file().read(cx, &mut header_bytes, 0).expect("capture WAL"),
                WAL_HEADER_SIZE
            );

            // Causal control: the same append path refuses before its write.
            inject_append_boundary(
                path,
                &mut adapter,
                cx,
                frames,
                CrashBoundary::BeforeWalFrameAppend,
            );
            assert_eq!(adapter.wal.frame_count(), 0);
            assert_eq!(adapter.wal.running_checksum(), initial_checksum);
            assert_eq!(
                adapter.wal.file().file_size(cx).unwrap(),
                u64::try_from(WAL_HEADER_SIZE).unwrap()
            );
            let mut after_refusal = [0; WAL_HEADER_SIZE];
            assert_eq!(
                adapter.wal.file().read(cx, &mut after_refusal, 0).expect("verify no write"),
                WAL_HEADER_SIZE
            );
            assert_eq!(after_refusal, header_bytes);
            assert_publication_unchanged(&adapter, "pre-write refusal");
            // Error alone is not a no-write proof. The control permits a
            // conservative retained owner pending exact interval inspection.
        }

        fn assert_postwrite_owner(path: AppendPath) {
            let _fault_session = FaultInjectionSessionLock::new().lock().unwrap();
            let cx = test_cx();
            let (p1, p1_new) = commit_batch_pages();
            let tail = sample_page(0x73);
            let frames = [
                WalFrameRef {
                    page_number: 1,
                    page_data: &p1,
                    db_size_if_commit: 0,
                },
                WalFrameRef {
                    page_number: 1,
                    page_data: &p1_new,
                    db_size_if_commit: 2,
                },
                WalFrameRef {
                    page_number: 2,
                    page_data: &tail,
                    db_size_if_commit: 0,
                },
            ];
            assert_prewrite_refusal(path, &cx, &frames);
            let vfs = MemoryVfs::new();
            let mut adapter = make_adapter(&vfs, &cx);
            let generation = adapter.wal.generation_identity();

            inject_append_boundary(
                path,
                &mut adapter,
                &cx,
                &frames,
                CrashBoundary::AfterWalFrameAppendBeforeFsync,
            );
            // Physical proof precedes the keeper assertions. The marker is in
            // the middle: neither a latest-page map nor the tail checksum is
            // enough metadata to publish this exact frame prefix and suffix.
            assert_eq!(adapter.wal.frame_count(), 3);
            assert_eq!(adapter.wal.last_commit_frame(&cx).unwrap(), Some(1));
            assert_eq!(adapter.wal.last_fsynced_frame_count(), 0);
            assert_eq!(
                adapter.wal.file().file_size(&cx).unwrap(),
                u64::try_from(WAL_HEADER_SIZE + frames.len() * adapter.wal.frame_size()).unwrap()
            );
            for (index, frame) in frames.iter().enumerate() {
                let (header, page) = adapter
                    .wal
                    .read_frame(&cx, index)
                    .expect("read physical frame");
                assert_eq!(header.page_number, frame.page_number);
                assert_eq!(header.db_size, frame.db_size_if_commit);
                assert_eq!(header.salts, generation.salts);
                assert_eq!(page.as_slice(), frame.page_data);
            }
            let commit = adapter.wal.read_frame_header(&cx, 1).expect("commit header");
            assert_ne!(commit.checksum, adapter.wal.running_checksum());
            assert_publication_unchanged(&adapter, "post-write error before reconciliation");

            let retained = adapter
                .pending_publication_frames
                .iter()
                .map(|frame| (frame.page_number, frame.frame_index, frame.is_commit))
                .collect::<Vec<_>>();
            assert!(
                adapter.has_pending_publication(),
                "{path:?}: frames 0..3 exist after Err without an owner; retained={retained:?}"
            );
            assert_eq!(retained, [(1, 0, false), (1, 1, true), (2, 2, false)]);
            assert_eq!(adapter.pending_publication_commit, Some(1));
            assert_eq!(adapter.pending_publication_generation, Some(generation));
            assert!(!adapter.refresh_before_append);
            let attempt = adapter.pending_append_attempt.as_ref().expect("retained attempt");
            assert_eq!(attempt.generation, generation);
            assert_eq!(attempt.start_frame_index, 0);
            assert_eq!(attempt.end_frame_count, 3);
            assert_eq!(attempt.completion.state(), VfsWriteCompletionState::Success);
            assert!(!attempt.authorized);
            assert_recovery_guards(&mut adapter, &cx, &p1);
            assert!(matches!(adapter.inner_mut(), Err(FrankenError::Busy)));
            assert!(matches!(
                adapter.begin_transaction(&cx).wait(),
                Err(FrankenError::Busy)
            ));
            assert_publication_unchanged(&adapter, "retained owner after guarded mutations");
            // No sync or logical publication is authorized by this API error.
            // The retained recovery owner must resolve the exact interval first.
        }

        fn assert_recovery_guards<F: VfsFile>(
            adapter: &mut WalBackendAdapter<F>,
            cx: &Cx,
            page: &[u8],
        ) {
            let published = adapter.published_snapshot();
            let frame_count = adapter.wal.frame_count();
            let snapshot = TransactionConflictSnapshot {
                generation: published.generation,
                last_commit_frame: published.last_commit_frame,
                commit_count: published.commit_count,
                snapshot_db_size: 0,
            };
            assert!(matches!(adapter.sync(cx), Err(FrankenError::BusyRecovery)));
            assert!(matches!(
                adapter.publish_authorized_deferred_commit(cx),
                Err(FrankenError::BusyRecovery)
            ));
            assert!(matches!(
                adapter.refresh_published_snapshot(cx).wait(),
                Err(FrankenError::BusyRecovery)
            ));
            assert!(matches!(adapter.read_page(cx, 1).wait(), Err(FrankenError::BusyRecovery)));
            assert!(matches!(
                adapter.read_page_at_appended_tail(cx, 1).wait(),
                Err(FrankenError::BusyRecovery)
            ));
            assert!(matches!(
                adapter.conflicting_pages_since_snapshot(cx, snapshot, &[1], &[]).wait(),
                Err(FrankenError::BusyRecovery)
            ));
            assert!(matches!(
                adapter.append_frame(cx, 1, page, 1).wait(),
                Err(FrankenError::BusyRecovery)
            ));
            assert_eq!(adapter.wal.frame_count(), frame_count, "guard precedes WAL refresh");
            assert_eq!(adapter.published_snapshot(), published);
        }

        #[test]
        fn malformed_raw_append_refuses_without_retaining_an_attempt() {
            let cx = test_cx();
            for path in [AppendPath::Raw, AppendPath::RawTracked] {
                let vfs = MemoryVfs::new();
                let mut adapter = make_adapter(&vfs, &cx);
                let page = sample_page(0x70);
                let malformed = [WalFrameRef {
                    page_number: 1, page_data: &page[..page.len() - 1], db_size_if_commit: 1,
                }];
                let completion = VfsWriteCompletion::new();
                assert_wal_corrupt(
                    path.append(&mut adapter, &cx, &malformed, completion.clone()),
                    "raw page length is validated before physical ownership",
                );
                assert!(adapter.pending_append_attempt.is_none());
                assert!(!adapter.has_pending_publication());
                assert_eq!(adapter.wal.frame_count(), 0);
                assert_eq!(adapter.wal.file().file_size(&cx).unwrap(), 32);
                if matches!(path, AppendPath::RawTracked) {
                    assert_eq!(completion.state(), VfsWriteCompletionState::Error);
                }
                assert_wal_corrupt(
                    adapter.append_frame(&cx, 1, &page[..page.len() - 1], 1).wait(),
                    "single page length is validated before physical ownership",
                );
                assert!(adapter.pending_append_attempt.is_none());
                adapter.append_frame(&cx, 1, &page, 1).expect("retry valid input");
                adapter.sync(&cx).expect("publish valid retry");
                assert_eq!(adapter.read_page(&cx, 1).expect("valid page"), Some(page));
            }
        }

        #[test]
        fn malformed_prepared_append_refuses_without_retaining_an_attempt() {
            let cx = test_cx();
            for malformed_case in 0..6 {
                let vfs = MemoryVfs::new();
                let mut adapter = make_adapter(&vfs, &cx);
                let page = sample_page(0x71);
                let frames = [
                    WalFrameRef { page_number: 1, page_data: &page, db_size_if_commit: 1 },
                    WalFrameRef { page_number: 1, page_data: &page, db_size_if_commit: 1 },
                ];
                let mut prepared = adapter.prepare_append_frames(&frames).unwrap().unwrap();
                adapter.finalize_prepared_frames(&cx, &mut prepared).unwrap();
                match malformed_case {
                    0 => { prepared.frame_bytes.pop(); }
                    1 => prepared.frame_metas[0].page_number = 2,
                    2 => prepared.last_commit_frame_offset = None,
                    3 => prepared.last_commit_frame_offset = Some(0),
                    4 => prepared.frame_bytes[8] ^= 1,
                    5 => prepared.finalized_running_checksum.as_mut().unwrap().s1 ^= 1,
                    _ => unreachable!(),
                }
                let completion = VfsWriteCompletion::new();
                assert_wal_corrupt(
                    adapter.append_prepared_frames_tracked(&cx, &mut prepared, completion.clone()).wait(),
                    "prepared layout/markers/salts are validated before physical ownership",
                );
                assert_eq!(completion.state(), VfsWriteCompletionState::Error);
                assert!(adapter.pending_append_attempt.is_none());
                assert!(!adapter.has_pending_publication());
                assert_eq!(adapter.wal.frame_count(), 0);
                assert_eq!(adapter.wal.file().file_size(&cx).unwrap(), 32);
                assert_wal_corrupt(
                    adapter.append_prepared_frames(&cx, &mut prepared).wait(),
                    "untracked prepared path also refuses before ownership",
                );
                assert!(adapter.pending_append_attempt.is_none());
                let mut valid = adapter.prepare_append_frames(&frames).unwrap().unwrap();
                adapter.append_prepared_frames(&cx, &mut valid).expect("valid retry");
                adapter.sync(&cx).expect("publish valid retry");
                assert_eq!(adapter.read_page(&cx, 1).expect("valid page"), Some(page));
            }
        }

        #[test]
        fn postwrite_append_error_single_retains_publication_owner() {
            let _fault_session = FaultInjectionSessionLock::new().lock().unwrap();
            let cx = test_cx();
            let vfs = MemoryVfs::new();
            let mut adapter = make_adapter(&vfs, &cx);
            let page = sample_page(0x74);
            let generation = adapter.wal.generation_identity();
            let boundary = CrashBoundary::AfterWalFrameAppendBeforeFsync;
            fault_hooks::arm_crash_boundary(
                boundary,
                FaultHookArm::new("bd-zywqc.22", "single-postwrite", "publication-ownership"),
            );
            let error = adapter.append_frame(&cx, 1, &page, 1).wait().unwrap_err();
            assert!(matches!(&error, FrankenError::Io(_)));
            assert!(error.to_string().contains(boundary.as_str()));
            let records = fault_hooks::take_records();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].point, boundary.as_str());
            assert_eq!(adapter.wal.frame_count(), 1);
            assert_eq!(adapter.wal.last_fsynced_frame_count(), 0);
            let (header, written_page) = adapter.wal.read_frame(&cx, 0).expect("physical frame");
            assert_eq!(written_page, page);
            assert_eq!(header.db_size, 1);
            let attempt = adapter.pending_append_attempt.as_ref().expect("single owner");
            assert_eq!(attempt.generation, generation);
            assert_eq!((attempt.start_frame_index, attempt.end_frame_count), (0, 1));
            assert_eq!(attempt.completion.state(), VfsWriteCompletionState::Success);
            assert_eq!(adapter.pending_publication_commit, Some(0));
            assert_eq!(adapter.pending_publication_generation, Some(generation));
            assert_eq!(adapter.pending_publication_frames.len(), 1);
            assert_recovery_guards(&mut adapter, &cx, &page);
            assert_publication_unchanged(&adapter, "single post-write error");
        }

        fn drop_after_source_success(
            backend: &mut PathRefreshingWalBackend<CheckpointHandoffFaultVfs>,
            vfs: &CheckpointHandoffFaultVfs,
            cx: &Cx,
            page: &[u8],
        ) -> VfsWriteCompletion {
            let completion = VfsWriteCompletion::new();
            let frames = [WalFrameRef { page_number: 1, page_data: page, db_size_if_commit: 1 }];
            vfs.pause_after_next_wal_write();
            {
                let mut append = backend.append_frames_tracked(cx, &frames, completion.clone());
                let mut task_cx = std::task::Context::from_waker(std::task::Waker::noop());
                assert!(matches!(
                    std::future::Future::poll(append.as_mut(), &mut task_cx),
                    std::task::Poll::Pending
                ));
                assert_eq!(completion.state(), VfsWriteCompletionState::Success);
            }
            assert_eq!(backend.inner.wal.frame_count(), 0, "caller never observed write success");
            assert_eq!(
                backend.inner.wal.file().file_size(cx).unwrap(),
                u64::try_from(WAL_HEADER_SIZE + backend.inner.wal.frame_size()).unwrap()
            );
            let attempt = backend.inner.pending_append_attempt.as_ref().expect("drop owner");
            assert_eq!(attempt.completion.state(), VfsWriteCompletionState::Success);
            assert_eq!((attempt.start_frame_index, attempt.end_frame_count), (0, 1));
            assert_eq!(backend.inner.pending_publication_commit, Some(0));
            assert_eq!(backend.inner.pending_publication_frames.len(), 1);
            assert_recovery_guards(&mut backend.inner, cx, page);
            completion
        }

        #[test]
        fn dropped_append_after_source_success_reconciles_once_after_failed_sync() {
            let _fault_session = FaultInjectionSessionLock::new().lock().unwrap();
            let cx = test_cx();
            let vfs = CheckpointHandoffFaultVfs::new();
            let wal = make_fault_adapter(&vfs, &cx).wal;
            let mut backend = PathRefreshingWalBackend::new(
                vfs.clone(), Path::new("test.db"), Path::new("test.db-wal"), PAGE_SIZE, wal, true,
                #[cfg(all(feature = "native", any(unix, windows)))]
                None,
            );
            let page = sample_page(0x75);
            let mut certificate = sample_certificate(1, 1, vec![1]);
            certificate.wal_frame_payload_digest = test_frame_payload_digest(1, &page, 1);
            certificate.certificate_crc32c = certificate.computed_crc32c();
            backend.persist_parallel_wal_commit_certificate(&cx, &certificate, 1, 1, true)
                .expect("persist exact certificate before append");
            let sidecar = read_certificate_sidecar(&vfs.inner, &cx);
            let completion = drop_after_source_success(&mut backend, &vfs, &cx, &page);
            let retained = match backend.inner.into_inner() {
                Ok(_) => panic!("extraction must return the retained append owner"),
                Err(retained) => retained,
            };
            assert_eq!(retained.pending_publication_commit, Some(0));
            assert_eq!(retained.pending_publication_frames.len(), 1);
            assert_eq!(retained.pending_append_attempt.as_ref().unwrap().completion.state(),
                VfsWriteCompletionState::Success);
            backend.inner = *retained;
            let published = backend.inner.published_snapshot();
            assert_wal_corrupt(
                backend.reconcile_parallel_wal_commit(&cx, &certificate, 1, 2, true).wait(),
                "wrong interval must not refresh or repair retained append",
            );
            assert_eq!(backend.inner.wal.frame_count(), 0);
            assert_eq!(read_certificate_sidecar(&vfs.inner, &cx), sidecar);
            vfs.fail_next_wal_sync();
            let error = backend.reconcile_parallel_wal_commit(&cx, &certificate, 1, 1, true)
                .wait().unwrap_err();
            assert!(error.to_string().contains("injected WAL sync failure"));
            let attempt = backend.inner.pending_append_attempt.as_ref().expect("retry owner");
            assert!(attempt.authorized, "content and certificate proof precedes failed fsync");
            assert_eq!(attempt.completion.state(), VfsWriteCompletionState::Success);
            assert_eq!(backend.inner.wal.frame_count(), 1);
            assert_eq!(backend.inner.published_snapshot(), published);
            assert_eq!(backend.inner.pending_publication_commit, Some(0));
            assert_eq!(read_certificate_sidecar(&vfs.inner, &cx), sidecar);
            assert_recovery_guards(&mut backend.inner, &cx, &page);
            assert_eq!(
                backend.reconcile_parallel_wal_commit(&cx, &certificate, 1, 1, true)
                    .wait().expect("retry exact authorization and publication"),
                ParallelWalCommitReconciliation::Authorized
            );
            assert!(backend.inner.pending_append_attempt.is_none());
            assert!(!backend.inner.has_pending_publication());
            let accepted = backend.inner.published_snapshot();
            assert_eq!(accepted.last_commit_frame, Some(0));
            assert_eq!(accepted.commit_count, 1);
            assert_eq!(accepted.latest_frame_entries, 1);
            assert_eq!(backend.inner.wal.frame_count(), 1);
            assert_eq!(backend.inner.wal.last_fsynced_frame_count(), 1);
            assert_eq!(backend.read_page(&cx, 1).expect("published page"), Some(page));
            backend.sync(&cx).expect("ordinary sync after reconciliation");
            assert_eq!(backend.inner.published_snapshot(), accepted, "publish only once");
            assert_eq!(completion.state(), VfsWriteCompletionState::Success);
            assert!(!completion.complete_error(), "drop cannot rewrite source success");
        }

        #[test]
        fn absent_append_restores_only_its_suffix_and_prior_pending_commit() {
            let _fault_session = FaultInjectionSessionLock::new().lock().unwrap();
            let cx = test_cx();
            let vfs = MemoryVfs::new();
            let mut backend = make_path_refreshing_backend(&vfs, &cx);
            let prior_page = sample_page(0x76);
            backend.append_frame(&cx, 1, &prior_page, 1).expect("prior accepted append");
            let prior_generation = backend.inner.pending_publication_generation;
            let prior_refresh = backend.inner.refresh_before_append;
            let prior_snapshot = backend.inner.published_snapshot();
            assert_eq!(backend.inner.pending_publication_commit, Some(0));
            assert_eq!(backend.inner.pending_publication_frames.len(), 1);
            let page = sample_page(0x77);
            let mut certificate = sample_certificate(2, 2, vec![1]);
            certificate.wal_frame_payload_digest = test_frame_payload_digest(1, &page, 1);
            certificate.certificate_crc32c = certificate.computed_crc32c();
            backend.persist_parallel_wal_commit_certificate(&cx, &certificate, 2, 2, true)
                .expect("persist certificate for next interval");
            let boundary = CrashBoundary::BeforeWalFrameAppend;
            fault_hooks::arm_crash_boundary(
                boundary,
                FaultHookArm::new("bd-zywqc.22", "absent-own-suffix", "publication-ownership"),
            );
            let error = backend.append_frame(&cx, 1, &page, 1).wait().unwrap_err();
            assert!(error.to_string().contains(boundary.as_str()));
            let records = fault_hooks::take_records();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].point, boundary.as_str());
            let attempt = backend.inner.pending_append_attempt.as_ref().expect("absence owner");
            assert_eq!(attempt.completion.state(), VfsWriteCompletionState::Error);
            assert_eq!((attempt.start_frame_index, attempt.end_frame_count), (1, 2));
            assert_eq!(backend.inner.pending_publication_commit, Some(1));
            assert_eq!(backend.inner.pending_publication_frames.len(), 2);
            assert_eq!(backend.inner.wal.frame_count(), 1);
            assert_eq!(
                backend.reconcile_parallel_wal_commit(&cx, &certificate, 2, 2, true)
                    .wait().expect("prove exact absence and repair own suffix"),
                ParallelWalCommitReconciliation::NotCommitted
            );
            assert!(backend.inner.pending_append_attempt.is_none());
            assert_eq!(backend.inner.pending_publication_commit, Some(0));
            assert_eq!(backend.inner.pending_publication_generation, prior_generation);
            assert_eq!(backend.inner.refresh_before_append, prior_refresh);
            let pending = &backend.inner.pending_publication_frames;
            assert_eq!(pending.len(), 1);
            assert_eq!((pending[0].page_number, pending[0].frame_index, pending[0].is_commit), (1, 0, true));
            assert_eq!(backend.inner.published_snapshot(), prior_snapshot);
            assert_eq!(backend.inner.wal.frame_count(), 1);
            assert_eq!(backend.inner.wal.last_fsynced_frame_count(), 1);
            assert!(read_certificate_sidecar(&vfs, &cx).is_empty());
            backend.sync(&cx).expect("prior successful append remains publishable");
            assert_eq!(backend.read_page(&cx, 1).expect("prior page"), Some(prior_page));
            assert_eq!(backend.inner.published_snapshot().commit_count, 1);
        }

        #[test]
        fn partial_append_reconciliation_preserves_prior_uncommitted_suffix() {
            let _fault_session = FaultInjectionSessionLock::new().lock().unwrap();
            let cx = test_cx();
            let vfs = CheckpointHandoffFaultVfs::new();
            let wal = make_fault_adapter(&vfs, &cx).wal;
            let mut backend = PathRefreshingWalBackend::new(
                vfs.clone(), Path::new("test.db"), Path::new("test.db-wal"), PAGE_SIZE, wal, true,
                #[cfg(all(feature = "native", any(unix, windows)))]
                None,
            );
            let prior = sample_page(0x78);
            backend.append_frame(&cx, 1, &prior, 0).expect("prior accepted noncommit");
            let prior_checksum = backend.inner.wal.running_checksum();
            let prior_generation = backend.inner.pending_publication_generation;
            let candidate = sample_page(0x79);
            let frames = [
                WalFrameRef { page_number: 2, page_data: &candidate, db_size_if_commit: 0 },
                WalFrameRef { page_number: 3, page_data: &candidate, db_size_if_commit: 3 },
            ];
            let mut digest = ParallelWalFramePayloadDigestBuilder::new();
            digest.update(PageNumber::new(2).unwrap(), 0, &candidate);
            digest.update(PageNumber::new(3).unwrap(), 3, &candidate);
            let mut certificate = sample_certificate(1, 1, vec![2]);
            certificate.wal_frame_payload_digest = digest.finalize();
            certificate.db_size_pages = 3;
            certificate.page_set_size = 2;
            certificate.certificate_crc32c = certificate.computed_crc32c();
            backend.persist_parallel_wal_commit_certificate(&cx, &certificate, 2, 3, true)
                .expect("persist exact candidate certificate");
            let frame_size = backend.inner.wal.frame_size();
            vfs.fail_next_wal_write_after_prefix(frame_size);
            let completion = VfsWriteCompletion::new();
            let error = backend.append_frames_tracked(&cx, &frames, completion.clone())
                .wait().unwrap_err();
            assert!(error.to_string().contains("injected partial WAL write"));
            assert_eq!(completion.state(), VfsWriteCompletionState::Error);
            assert_eq!(backend.inner.wal.frame_count(), 1);
            assert_eq!(backend.inner.wal.file().file_size(&cx).unwrap(),
                u64::try_from(WAL_HEADER_SIZE + 2 * frame_size).unwrap());
            assert_eq!(backend.inner.pending_publication_frames.len(), 3);
            assert_eq!(backend.inner.pending_publication_commit, Some(2));
            vfs.fail_next_wal_sync();
            let error = backend.reconcile_parallel_wal_commit(&cx, &certificate, 2, 3, true)
                .wait().unwrap_err();
            assert!(error.to_string().contains("injected WAL sync failure"));
            assert!(backend.inner.pending_append_attempt.is_some());
            assert_eq!(backend.inner.pending_publication_frames.len(), 3);
            assert_recovery_guards(&mut backend.inner, &cx, &prior);
            assert_eq!(
                backend.reconcile_parallel_wal_commit(&cx, &certificate, 2, 3, true)
                    .wait().expect("retry exact absence after tail repair and failed sync"),
                ParallelWalCommitReconciliation::NotCommitted
            );
            assert!(backend.inner.pending_append_attempt.is_none());
            assert_eq!(backend.inner.wal.frame_count(), 1);
            assert_eq!(backend.inner.wal.running_checksum(), prior_checksum);
            assert_eq!(backend.inner.pending_publication_generation, prior_generation);
            assert_eq!(backend.inner.pending_publication_commit, None);
            assert_eq!(backend.inner.pending_publication_frames.len(), 1);
            assert!(!backend.inner.refresh_before_append);
            assert_eq!(backend.inner.wal.file().file_size(&cx).unwrap(),
                u64::try_from(WAL_HEADER_SIZE + frame_size).unwrap());
            assert_eq!(backend.inner.wal.read_frame(&cx, 0).expect("prior bytes").1, prior);
            let retry = sample_page(0x7A);
            backend.append_frame(&cx, 2, &retry, 2).expect("complete prior transaction");
            backend.sync(&cx).expect("publish retained prefix and successful retry");
            assert_eq!(backend.read_page(&cx, 1).expect("prior published page"), Some(prior));
            assert_eq!(backend.read_page(&cx, 2).expect("retry published page"), Some(retry));
            assert_eq!(backend.read_page(&cx, 3).expect("aborted marker absent"), None);
            assert_eq!(backend.inner.published_snapshot().commit_count, 1);
        }

        #[test]
        fn postwrite_append_error_raw_retains_publication_owner() {
            assert_postwrite_owner(AppendPath::Raw);
        }

        #[test]
        fn postwrite_append_error_raw_tracked_retains_publication_owner() {
            assert_postwrite_owner(AppendPath::RawTracked);
        }

        #[test]
        fn postwrite_append_error_prepared_retains_publication_owner() {
            assert_postwrite_owner(AppendPath::Prepared);
        }

        #[test]
        fn postwrite_append_error_prepared_tracked_retains_publication_owner() {
            assert_postwrite_owner(AppendPath::PreparedTracked);
        }
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

    #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
    #[test]
    fn wal_fec_failed_sync_and_full_queue_do_not_admit_or_publish() {
        let runtime = asupersync::runtime::RuntimeBuilder::current_thread()
            .blocking_threads(1, 1).build().unwrap();
        let handle = runtime.handle();
        runtime.block_on(async {
            let cx = test_cx();
            let vfs = CheckpointHandoffFaultVfs::new();
            let wal = make_fault_adapter(&vfs, &cx).wal;
            let mut backend = PathRefreshingWalBackend::new(
                vfs.clone(), "test.db", "test.db-wal", PAGE_SIZE, wal, true,
                #[cfg(any(unix, windows))]
                None,
            );
            let (p1, p2) = commit_batch_pages();
            backend.inner.append_frame(&cx, 1, &p1, 0).await.unwrap();
            backend.inner.append_frame(&cx, 2, &p2, 2).await.unwrap();
            let mut pipeline = fsqlite_wal::WalFecRepairPipeline::start(
                &handle, &cx, fsqlite_wal::WalFecRepairPipelineConfig {
                    queue_capacity: 1, per_symbol_delay: std::time::Duration::ZERO,
                },
            ).unwrap();
            let producer = pipeline.producer().unwrap();
            backend.fec_producer = Some(producer.clone());

            vfs.fail_next_wal_sync();
            let error = backend.sync(&cx).expect_err("injected fsync failure");
            assert!(error.to_string().contains("injected WAL sync failure"));
            assert_eq!(pipeline.stats().pending_jobs, 0);
            assert!(backend.fec_admitted.is_none());
            assert_publication_unchanged(&backend.inner, "failed WAL-FEC sync");

            let occupied = producer.try_reserve().unwrap();
            let _ = vfs.take_sync_observations();
            assert!(matches!(backend.sync(&cx), Err(FrankenError::Busy)));
            assert!(vfs.take_sync_observations().is_empty(), "backpressure must precede fsync");
            assert_eq!(pipeline.stats().pending_jobs, 0);
            assert_publication_unchanged(&backend.inner, "full WAL-FEC queue");
            drop(occupied);

            backend.sync(&cx).expect("retry after capacity and durability recover");
            assert_eq!(backend.inner.wal.last_fsynced_frame_count(), 2);
            assert_eq!(backend.fec_admitted.unwrap().1, 2);
            assert_eq!(pipeline.stats().pending_jobs, 1);
            assert_eq!(backend.inner.published_snapshot.last_commit_frame, Some(1));
            // This is a VFS fault test, not OS-sidecar coverage. Cancel before
            // yielding to the worker; the SQL integration suite covers its I/O.
            pipeline.cancel();
            let stats = pipeline.shutdown(&cx).await.unwrap();
            assert_eq!(stats.completed_jobs, 0);
            assert_eq!(stats.canceled_jobs, 1);
        });
    }

    #[test]
    fn test_wal_retirement_failed_terminal_sync_is_retryable() {
        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let mut adapter = make_fault_adapter(&vfs, &cx);
        assert_eq!(adapter.wal.file().file_size(&cx).unwrap(), 32);

        vfs.fail_next_wal_sync();
        let failure = adapter
            .retire_empty_wal(&cx)
            .expect_err("terminal WAL sync failure must surface");
        assert!(failure.to_string().contains("injected WAL sync failure"));
        assert_eq!(adapter.wal.file().file_size(&cx).unwrap(), 0);
        assert_eq!(adapter.wal.frame_count(), 0);
        assert!(!adapter.has_pending_publication());

        // The truncated file has no header to refresh. A retry must finish
        // durability rather than reject that already-empty terminal state.
        adapter
            .validate_empty_wal_for_retirement(&cx)
            .expect("validate after failed terminal sync");
        adapter
            .retire_empty_wal(&cx)
            .expect("retry terminal WAL sync");
        assert_eq!(adapter.wal.file().file_size(&cx).unwrap(), 0);
        assert_publication_unchanged(&adapter, "after retirement retry");
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
    fn test_commit_prefix_sync_preserves_raw_and_prepared_uncommitted_suffix() {
        for prepared in [false, true] {
            let cx = test_cx();
            let vfs = MemoryVfs::new();
            let mut adapter = make_adapter(&vfs, &cx);
            let pages = [sample_page(1), sample_page(2), sample_page(3)];
            let frames = [
                WalFrameRef {
                    page_number: 1,
                    page_data: &pages[0],
                    db_size_if_commit: 0,
                },
                WalFrameRef {
                    page_number: 2,
                    page_data: &pages[1],
                    db_size_if_commit: 2,
                },
                WalFrameRef {
                    page_number: 3,
                    page_data: &pages[2],
                    db_size_if_commit: 0,
                },
            ];
            if prepared {
                let mut batch = adapter
                    .prepare_append_frames(&frames)
                    .expect("prepare append")
                    .expect("prepared batch");
                adapter
                    .append_prepared_frames(&cx, &mut batch)
                    .expect("append prepared prefix and suffix");
            } else {
                adapter.append_frames(&cx, &frames).expect("append raw prefix and suffix");
            }
            let committed = adapter.wal.read_frame_header(&cx, 1).expect("commit header");
            assert_eq!(adapter.wal.last_commit_frame_header(), Some((1, committed)));
            assert_ne!(committed.checksum, adapter.wal.running_checksum());

            adapter.sync(&cx).expect("publish committed prefix");
            assert_eq!(adapter.published_snapshot.last_commit_frame, Some(1));
            assert_eq!(adapter.published_snapshot.commit_count, 1);
            assert_eq!(adapter.pending_publication_commit, None);
            assert_eq!(adapter.pending_publication_frames.len(), 1);
            assert_eq!(adapter.pending_publication_frames[0].frame_index, 2);
            assert_eq!(
                adapter.pending_publication_generation,
                Some(adapter.wal.generation_identity())
            );
            assert!(!adapter.refresh_before_append);
            assert!(adapter.has_pending_publication());
            assert!(matches!(adapter.inner_mut(), Err(FrankenError::Busy)));
            assert_eq!(adapter.read_page(&cx, 3).expect("hide suffix"), None);

            adapter.sync(&cx).expect("intermediate suffix sync");
            assert_eq!(adapter.pending_publication_frames.len(), 1);
            assert_eq!(adapter.wal.last_commit_frame_header(), Some((1, committed)));
            adapter.append_frame(&cx, 4, &sample_page(4), 4).expect("commit suffix");
            adapter.sync(&cx).expect("publish suffix");
            assert_eq!(adapter.published_snapshot.last_commit_frame, Some(3));
            assert_eq!(adapter.published_snapshot.commit_count, 2);
            assert_eq!(adapter.published_snapshot.page_index.get(&3), Some(&2));
            assert_eq!(adapter.read_page(&cx, 3).expect("read suffix"), Some(pages[2].clone()));
            assert!(!adapter.has_pending_publication());
            assert_eq!(adapter.pending_publication_generation, None);
            assert!(adapter.refresh_before_append);
        }
    }

    #[test]
    fn test_deferred_commit_prefix_preserves_uncommitted_suffix() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);
        let pages = [sample_page(1), sample_page(2)];
        let frames = [
            WalFrameRef {
                page_number: 1,
                page_data: &pages[0],
                db_size_if_commit: 1,
            },
            WalFrameRef {
                page_number: 2,
                page_data: &pages[1],
                db_size_if_commit: 0,
            },
        ];
        adapter.append_frames(&cx, &frames).expect("append prefix and suffix");
        adapter.publish_authorized_deferred_commit(&cx).expect("authorize prefix");
        assert_eq!(adapter.wal.last_fsynced_frame_count(), 0);
        assert_eq!(adapter.published_snapshot.last_commit_frame, Some(0));
        assert_eq!(adapter.pending_publication_frames.len(), 1);
        assert_eq!(adapter.pending_publication_frames[0].frame_index, 1);
        assert_eq!(
            adapter.pending_publication_generation,
            Some(adapter.wal.generation_identity())
        );
        assert!(!adapter.refresh_before_append);
        adapter.append_frame(&cx, 3, &sample_page(3), 3).expect("commit suffix");
        adapter.publish_authorized_deferred_commit(&cx).expect("authorize suffix");
        assert_eq!(adapter.wal.last_fsynced_frame_count(), 0);
        assert_eq!(adapter.published_snapshot.page_index.get(&2), Some(&1));
        assert_eq!(adapter.read_page(&cx, 2).expect("read suffix"), Some(pages[1].clone()));
        assert!(!adapter.has_pending_publication());
        assert_eq!(adapter.pending_publication_generation, None);
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
    fn test_unpinned_refresh_preserves_published_prefix_with_new_staged_commit() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);
        let old_page = sample_page(0x51);
        let new_page = sample_page(0xA2);
        adapter.append_frame(&cx, 1, &old_page, 1).expect("append old commit");
        adapter.sync(&cx).expect("publish old commit");
        let published = adapter.published_snapshot();
        assert_eq!(published.last_commit_frame, Some(0));
        assert_eq!(published.commit_count, 1);

        adapter.append_frame(&cx, 2, &new_page, 2).expect("stage new commit");
        let staged_generation = adapter.pending_publication_generation;
        assert_eq!(adapter.pending_publication_frames.len(), 1);
        assert_eq!(adapter.pending_publication_commit, Some(1));
        assert_eq!(adapter.wal.last_fsynced_frame_count(), 1);

        for _ in 0..2 {
            let refreshed = adapter.refresh_published_snapshot(&cx).expect("refresh");
            assert_eq!(refreshed, published, "refresh must preserve the published prefix");
            assert_eq!(adapter.read_page(&cx, 1).expect("old page"), Some(old_page.clone()));
            assert_eq!(adapter.read_page(&cx, 2).expect("staged page"), None);
            assert_eq!(adapter.pending_publication_commit, Some(1));
            assert_eq!(adapter.pending_publication_generation, staged_generation);
            assert_eq!(adapter.pending_publication_frames.len(), 1);
            assert_eq!(adapter.pending_publication_frames[0].frame_index, 1);
            assert!(adapter.has_pending_publication());
        }

        adapter.sync(&cx).expect("publish staged commit");
        assert!(!adapter.has_pending_publication());
        assert_eq!(adapter.published_snapshot().last_commit_frame, Some(1));
        assert_eq!(adapter.published_snapshot().commit_count, 2);
        assert_eq!(adapter.read_page(&cx, 1).expect("retained old page"), Some(old_page));
        assert_eq!(adapter.read_page(&cx, 2).expect("published new page"), Some(new_page));
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
        let published = adapter.published_snapshot();
        let next_page = sample_page(0xC3);
        adapter.append_frame(&cx, 3, &next_page, 3).expect("stage after deferred commit");
        assert_eq!(
            adapter.refresh_published_snapshot(&cx).expect("refresh deferred prefix"),
            published,
            "a prior deferred publication retains authority without an fsync"
        );
        assert_eq!(adapter.read_page(&cx, 1).expect("deferred old page"), Some(p1));
        assert_eq!(adapter.read_page(&cx, 3).expect("new staged page"), None);
        assert!(adapter.has_pending_publication());
        adapter.publish_authorized_deferred_commit(&cx).expect("authorize next commit");
        assert_eq!(adapter.wal.last_fsynced_frame_count(), fsynced_before);
        assert_eq!(adapter.read_page(&cx, 3).expect("next published page"), Some(next_page));
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

    // -- Appended-tail index (cass GH #382) --

    /// The reclaim sweep asks `read_page_at_appended_tail` for every ledger
    /// page in turn. It must answer exactly like the backwards scan it
    /// replaced (newest frame wins, absent pages are `None`) while scanning
    /// the tail once per stable tail. A pure append grows the tail and is
    /// folded in incrementally (see the paired
    /// [`appended_tail_growth_folds_incrementally_not_per_commit`]), never a
    /// full fresh pass.
    #[test]
    fn appended_tail_reads_index_the_tail_once_per_stable_tail() {
        init_wal_publication_test_tracing();
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let p1_old = sample_page(0x11);
        let p2 = sample_page(0x22);
        let p1_new = sample_page(0x33);
        adapter.append_frame(&cx, 1, &p1_old, 0).expect("append p1");
        adapter.append_frame(&cx, 2, &p2, 0).expect("append p2");
        adapter
            .append_frame(&cx, 1, &p1_new, 2)
            .expect("append newer p1 (commit)");
        adapter.sync(&cx).expect("publish staged frames");
        assert_eq!(adapter.appended_tail_index_builds, 0);

        // Newest frame wins, exactly like the backwards scan.
        assert_eq!(
            adapter
                .read_page_at_appended_tail(&cx, 1)
                .expect("tail read p1"),
            Some(p1_new.clone())
        );
        assert_eq!(
            adapter
                .read_page_at_appended_tail(&cx, 2)
                .expect("tail read p2"),
            Some(p2.clone())
        );
        assert_eq!(
            adapter
                .read_page_at_appended_tail(&cx, 3)
                .expect("tail read p3"),
            None
        );
        // Many lookups against an unchanged tail: one scan, not one per page.
        for page in 1..=64_u32 {
            let expected = adapter
                .scan_backwards_for_page(&cx, page, adapter.wal.frame_count() - 1)
                .expect("backwards scan");
            let via_index = adapter
                .appended_tail_frame_for_page(&cx, page, adapter.wal.frame_count() - 1)
                .expect("indexed lookup");
            assert_eq!(via_index, expected, "page {page}: index must equal the scan");
        }
        assert_eq!(
            adapter.appended_tail_index_builds, 1,
            "a stable tail is indexed exactly once"
        );

        // Appending grows the tail: the next lookup FOLDS the new frame into
        // the existing index (O(appended frames)) rather than rescanning the
        // whole WAL, and still sees both the new page and the untouched ones.
        let p3 = sample_page(0x44);
        adapter
            .append_frame(&cx, 3, &p3, 3)
            .expect("append p3 (commit)");
        adapter.sync(&cx).expect("publish p3");
        assert_eq!(
            adapter
                .read_page_at_appended_tail(&cx, 3)
                .expect("tail read p3 after append"),
            Some(p3)
        );
        assert_eq!(
            adapter
                .read_page_at_appended_tail(&cx, 1)
                .expect("tail read p1 after append"),
            Some(p1_new)
        );
        assert_eq!(
            adapter.appended_tail_index_builds, 1,
            "a grown tail is folded, never a second full pass"
        );
        assert_eq!(
            adapter.appended_tail_index_folds, 1,
            "the appended frame is folded in incrementally exactly once"
        );
    }

    /// Regression guard for bd-gh382-16writer-begin-starvation: under many
    /// concurrent writers the physical tail advances on every peer commit, so a
    /// gate-held commit read (`read_durable_page_under_gate`) sees a grown tail
    /// almost every time. Keying index reuse on the tail checksum alone (the
    /// original GH#382 cache) rebuilt the ENTIRE growing WAL on every such read
    /// — an O(frames) forward pass per commit, held under the append gate, that
    /// convoyed writers off their retry budget ("exhausted retry budget at
    /// BEGIN/INSERT after 0 retries"). With incremental folding, N commits that
    /// each grow the tail cost ONE full build plus N cheap folds, not N full
    /// builds — and every answer still matches the backwards scan exactly.
    #[test]
    fn appended_tail_growth_folds_incrementally_not_per_commit() {
        init_wal_publication_test_tracing();
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        // Seed a non-trivial tail so a full rebuild would be visibly expensive.
        // Each frame is a commit that grows the db to `page` pages.
        for page in 1..=200_u32 {
            let image = sample_page(u8::try_from(page % 251).unwrap_or(0));
            adapter
                .append_frame(&cx, page, &image, page)
                .expect("seed append");
        }
        adapter.sync(&cx).expect("publish seed");
        // First lookup builds the index once.
        let _ = adapter
            .read_page_at_appended_tail(&cx, 1)
            .expect("first tail read");
        assert_eq!(adapter.appended_tail_index_builds, 1);
        assert_eq!(adapter.appended_tail_index_folds, 0);

        // Model 40 rounds of peer-commit churn: each round rewrites an existing
        // page (the tail moves, db stays 200 pages) and this connection then
        // reads a couple of pages under the gate — the exact shape that used to
        // rebuild the whole index per commit.
        for round in 0..40_u32 {
            let page = 1_u32 + (round % 200);
            let image = sample_page(u8::try_from((round % 250) + 1).unwrap_or(1));
            adapter
                .append_frame(&cx, page, &image, 200)
                .expect("churn append");
            adapter.sync(&cx).expect("publish churn");

            let tail = adapter.wal.frame_count() - 1;
            for probe in [page, 1_u32, 200_u32] {
                let expected = adapter
                    .scan_backwards_for_page(&cx, probe, tail)
                    .expect("backwards scan");
                let via_index = adapter
                    .appended_tail_frame_for_page(&cx, probe, tail)
                    .expect("indexed lookup");
                assert_eq!(
                    via_index, expected,
                    "round {round} page {probe}: folded index must equal the scan"
                );
            }
        }

        // The whole run rebuilt the tail exactly ONCE; every grown tail after
        // that was folded, not rescanned. Pre-fix this would have been one
        // full O(frames) build per round.
        assert_eq!(
            adapter.appended_tail_index_builds, 1,
            "churn must not force a second full rebuild (was one build per commit)"
        );
        assert!(
            adapter.appended_tail_index_folds >= 40,
            "each grown tail folds incrementally (folds={})",
            adapter.appended_tail_index_folds
        );
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

    #[cfg(all(feature = "native", unix))]
    fn checkpoint_reset_fixture(vfs: &CheckpointHandoffFaultVfs, cx: &Cx) -> SyntheticSharedPublication {
        let mut fixture = synthetic_shared_publication(vfs, cx);
        fixture.adapter.append_frame(cx, 1, &sample_page(0x5A), 1).expect("fixture commit");
        fixture.adapter.sync(cx).expect("publish fixture commit");
        for offset in [96, 128] {
            fixture.region.atomic_store_u32_ne(offset, 0, std::sync::atomic::Ordering::Release).unwrap();
        }
        fixture
    }

    /// Synthetic state-validation control; the public peer-transition keeper
    /// separately proves durable rollback publication and real native fences.
    #[cfg(all(feature = "native", unix))]
    #[test]
    fn test_native_zero_wal_retirement_requires_rollback_header_and_no_recovery_owner() {
        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let SyntheticSharedPublication {
            _pager: pager,
            adapter,
            region,
            ..
        } = checkpoint_reset_fixture(&vfs, &cx);
        let wal = match adapter.into_inner() {
            Ok(wal) => wal,
            Err(retained) => panic!(
                "published fixture retained state: publication={}, reader={}, recovery={}",
                retained.has_pending_publication(),
                retained.native_read_binding.is_some(),
                retained.native_recovery_requested.is_some(),
            ),
        };
        // SimplePager opens the normalized key; MemoryVfs::open itself
        // preserves the supplied path rather than normalizing it again.
        let db_path = pager.db_path();
        let mut backend = PathRefreshingWalBackend::new(
            vfs.clone(),
            db_path,
            "test.db-wal",
            PAGE_SIZE,
            wal,
            true,
            None,
        );
        backend
            .attach_wal_index_shm_source(pager.wal_index_shm_source().unwrap())
            .unwrap();
        let (mut main, _) = vfs
            .open(
                &cx,
                Some(db_path),
                VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB,
            )
            .unwrap();
        assert_eq!(
            main.file_identity().unwrap(),
            pager.file_identity(&cx).wait().unwrap(),
            "the main header belongs to the fixture pager"
        );
        let header = fsqlite_types::DatabaseHeader {
            read_version: 2,
            write_version: 2,
            page_count: 1,
            ..fsqlite_types::DatabaseHeader::default()
        };
        let mut main_image = sample_page(0xA5);
        main_image[..fsqlite_types::DATABASE_HEADER_SIZE]
            .copy_from_slice(&header.to_bytes().unwrap());
        main.write(&cx, &main_image, 0)
            .expect("seed explicit WAL-mode main header");
        main.sync(&cx, SyncFlags::FULL).unwrap();
        let wal_identity = backend.inner.wal.file().file_identity().unwrap();
        assert!(wal_identity.is_some());
        assert_eq!(backend.inner.wal.frame_count(), 1);
        // Deliberate external truncation keeps the same MemoryFile identity
        // and stale private frame count. It does not prove a peer checkpoint.
        backend.inner.wal.file_mut().truncate(&cx, 0).unwrap();
        let shared_before = region.lock().to_vec();
        let published_before = backend.inner.published_snapshot();
        assert!(!backend.inner.has_pending_publication());
        assert!(
            matches!(
                backend.begin_transaction(&cx).wait(),
                Err(FrankenError::BusyRecovery)
            ),
            "ordinary native admission must not recreate the retired WAL"
        );

        for refusal in ["wal_header", "recovery_owner"] {
            if refusal == "recovery_owner" {
                main_image[18..20].copy_from_slice(&[1, 1]);
                main.write(&cx, &main_image, 0)
                    .expect("publish controlled rollback header");
                main.sync(&cx, SyncFlags::FULL).unwrap();
                // Explicit retained-request intervention exercises the guard;
                // canonical recovery itself is covered by separate keepers.
                backend.inner.native_recovery_requested =
                    Some(fsqlite_pager::traits::WalNativeRecoveryReason::WalGenerationMismatch);
            }
            for retire in [false, true] {
                let result = if retire {
                    backend.retire_empty_wal(&cx).wait()
                } else {
                    backend.validate_empty_wal_for_retirement(&cx).wait()
                };
                assert!(
                    matches!(result, Err(FrankenError::BusyRecovery)),
                    "{refusal}: zero length cannot bypass retirement proof"
                );
                let mut observed = vec![0; main_image.len()];
                assert_eq!(
                    main.read(&cx, &mut observed, 0).expect("read retained main"),
                    main_image.len()
                );
                assert_eq!(observed, main_image);
                assert_eq!(region.lock().to_vec(), shared_before);
                assert_eq!(backend.inner.published_snapshot(), published_before);
                assert_eq!(backend.inner.wal.frame_count(), 1);
                assert_eq!(backend.inner.wal.file().file_size(&cx).unwrap(), 0);
                assert_eq!(
                    backend.inner.wal.file().file_identity().unwrap(),
                    wal_identity
                );
                assert_eq!(
                    backend.inner.native_recovery_requested.is_some(),
                    refusal == "recovery_owner"
                );
                assert!(!backend.inner.has_pending_publication());
            }
        }

        // Clear only the deliberately injected request. Retirement still
        // requires the exact path inode and persisted rollback header.
        backend.inner.native_recovery_requested = None;
        assert!(
            matches!(
                backend.begin_transaction(&cx).wait(),
                Err(FrankenError::BusyRecovery)
            ),
            "rollback proof authorizes retirement only, never ordinary native reads"
        );
        backend
            .validate_empty_wal_for_retirement(&cx)
            .expect("same-inode zero WAL with rollback header");
        backend
            .retire_empty_wal(&cx)
            .expect("finish proven zero retirement despite stale cached frames");
        assert_eq!(backend.inner.wal.file().file_size(&cx).unwrap(), 0);
        assert_eq!(backend.inner.wal.file().file_identity().unwrap(), wal_identity);
        assert_eq!(region.lock().to_vec(), shared_before);
        let mut observed = vec![0; main_image.len()];
        assert_eq!(
            main.read(&cx, &mut observed, 0).expect("read retired main"),
            main_image.len()
        );
        assert_eq!(observed, main_image);
        assert!(!backend.inner.has_pending_publication());
        assert!(backend.inner.native_recovery_requested.is_none());
        assert_eq!(
            backend
                .cached_verification_db
                .as_ref()
                .unwrap()
                .file_identity()
                .unwrap(),
            main.file_identity().unwrap(),
            "validation retains the exact main descriptor"
        );
        main.close(&cx).unwrap();
    }

    /// Synthetic transport + real MemoryFile write-source controls. The pager
    /// native test below supplies the separate actual OS-gate/DB durability path.
    #[cfg(all(feature = "fault-injection", feature = "native", unix))]
    #[test]
    fn test_checkpoint_reset_retains_exact_target_through_partial_sync_and_dropped_source() {
        use std::task::{Context, Poll, Waker};

        let cx = test_cx();
        for native in [false, true] {
            for fault in ["partial", "sync", "drop_after_write", "source_pending"] {
                let vfs = CheckpointHandoffFaultVfs::new();
                let mut fixture = checkpoint_reset_fixture(&vfs, &cx);
                if !native { fixture.adapter.wal_index_shm_source = None; }
                fixture.adapter.begin_transaction(&cx).expect("maintenance snapshot before checkpoint");
                let before = fixture.adapter.published_snapshot();
                match fault {
                    "partial" => vfs.fail_next_wal_write_after_prefix(15),
                    "sync" => vfs.fail_next_reset_sync(),
                    "drop_after_write" => vfs.pause_after_next_wal_write(),
                    _ => vfs.pend_next_reset_write(),
                }
                let mut writer = MockCheckpointPageWriter;
                if matches!(fault, "drop_after_write" | "source_pending") {
                    let mut future = fixture.adapter.checkpoint(&cx, CheckpointMode::Truncate, &mut writer, 0, None);
                    assert!(matches!(future.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Pending));
                    drop(future);
                } else {
                    fixture.adapter.checkpoint(&cx, CheckpointMode::Truncate, &mut writer, 0, None)
                        .expect_err("injected physical reset failure");
                }
                assert!(fixture.adapter.checkpoint_recovery_pending());
                let reset = fixture.adapter.pending_checkpoint_reset.as_ref().unwrap();
                let expected = reset.target_header;
                let completion = reset.completion.clone();
                assert!(!reset.physical_complete && !reset.shared_complete);
                let state = match fault {
                    "partial" => VfsWriteCompletionState::Error,
                    "source_pending" => VfsWriteCompletionState::Pending,
                    _ => VfsWriteCompletionState::Success,
                };
                assert_eq!(completion.state(), state);
                assert_eq!(fixture.adapter.published_snapshot(), before, "no private reset publication before physical/shared completion");
                fixture.adapter.read_page(&cx, 1).expect_err("retained reset excludes mutable reads");
                fixture.adapter.read_page_pinned(&cx, 1).expect_err("retained reset excludes old pinned reads");
                fixture.adapter.begin_transaction(&cx).expect_err("retained reset excludes admission");
                fixture.adapter.append_frame(&cx, 1, &sample_page(0x6B), 1).expect_err("retained reset excludes append");
                fixture.adapter.sync(&cx).expect_err("ordinary sync cannot consume reset ownership");
                assert!(fixture.adapter.inner_mut().is_err());
                fixture.adapter.checkpoint(&cx, CheckpointMode::Truncate, &mut writer, 0, None)
                    .expect_err("new checkpoint cannot replace fixed reset salts");
                if fault == "source_pending" {
                    let headers = vfs.faults.lock().unwrap().reset_headers.len();
                    let mut retry = fixture.adapter.reconcile_checkpoint_reset(&cx);
                    assert!(matches!(retry.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Pending));
                    drop(retry);
                    assert_eq!(completion.state(), VfsWriteCompletionState::Pending);
                    assert_eq!(vfs.faults.lock().unwrap().reset_headers.len(), headers, "no second write while old source can execute");
                    vfs.complete_pending_reset_write(&cx).expect("actual source finishes after caller and waiter drop");
                    assert_eq!(completion.state(), VfsWriteCompletionState::Success);
                }
                fixture.adapter.reconcile_checkpoint_reset(&cx).expect("retry exactly the retained reset target");
                assert!(!fixture.adapter.checkpoint_recovery_pending());
                assert!(!fixture.adapter.has_pending_publication());
                assert_eq!(*fixture.adapter.wal.header(), expected);
                assert_eq!(fixture.adapter.wal.frame_count(), 0);
                assert_eq!(fixture.adapter.wal.file().file_size(&cx).unwrap(), u64::try_from(WAL_HEADER_SIZE).unwrap());
                assert_eq!(fixture.adapter.published_snapshot().generation, fixture.adapter.wal.generation_identity());
                assert_eq!(fixture.adapter.published_snapshot().last_commit_frame, None);
                assert!(fixture.adapter.pinned_read_snapshot().is_none());
                if native {
                    let shared = read_shared_wal_index_header(&fixture.region).unwrap().unwrap();
                    assert_eq!((shared.mx_frame, shared.n_page, shared.a_frame_cksum), (0, 0, [0, 0]));
                    assert_eq!(shared.a_salt, [expected.salts.salt1, expected.salts.salt2]);
                    assert_eq!(read_shared_wal_index_backfill(&fixture.region, &shared).unwrap(), 0);
                }
                let headers = vfs.faults.lock().unwrap().reset_headers.clone();
                assert_eq!(headers.len(), 2);
                let expected_bytes = expected.to_bytes().unwrap();
                assert!(headers.iter().all(|bytes| bytes.as_slice() == expected_bytes.as_slice()));
                fixture.adapter.reconcile_checkpoint_reset(&cx).expect("terminal reset retry is idempotent");
                assert_eq!(vfs.faults.lock().unwrap().reset_headers, headers);
                fixture.adapter.append_frame(&cx, 1, &sample_page(0x6C), 1).expect("new generation remains writable");
                fixture.adapter.sync(&cx).expect("publish new generation commit");
            }
        }
    }

    #[cfg(all(feature = "fault-injection", feature = "native", unix))]
    #[test]
    fn test_checkpoint_reset_shared_refusal_retries_publication_without_rewriting_durable_header() {
        use fsqlite_wal::wal_index::publish_shared_wal_index_header;

        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let mut fixture = checkpoint_reset_fixture(&vfs, &cx);
        let baseline = read_shared_wal_index_header(&fixture.region).unwrap().unwrap();
        let mut foreign = baseline;
        foreign.i_change = foreign.i_change.wrapping_add(1);
        foreign.update_checksum().unwrap();
        // Controlled competing-header intervention at the completed physical
        // sync boundary, not a claim of spontaneous OS shared-memory failure.
        vfs.faults.lock().unwrap().reset_shared_header_after_sync = Some((fixture.region.share(), foreign));
        let mut writer = MockCheckpointPageWriter;
        fixture.adapter.checkpoint(&cx, CheckpointMode::Truncate, &mut writer, 0, None)
            .expect_err("shared publication refuses changed baseline after WAL durability");
        let reset = fixture.adapter.pending_checkpoint_reset.as_ref().unwrap();
        assert!(reset.physical_complete && !reset.shared_complete);
        let target = reset.target_header;
        assert_eq!(*fixture.adapter.wal.header(), target);
        assert_eq!(read_shared_wal_index_header(&fixture.region).unwrap(), Some(foreign));
        assert_eq!(vfs.faults.lock().unwrap().reset_headers.len(), 1);
        fixture.adapter.reconcile_checkpoint_reset(&cx).expect_err("foreign header remains a refusal");
        assert!(fixture.adapter.checkpoint_recovery_pending());
        assert_eq!(vfs.faults.lock().unwrap().reset_headers.len(), 1);
        publish_shared_wal_index_header(&fixture.region, &baseline).unwrap();
        fixture.adapter.reconcile_checkpoint_reset(&cx).expect("complete the same retained shared publication");
        assert!(!fixture.adapter.checkpoint_recovery_pending());
        assert_eq!(vfs.faults.lock().unwrap().reset_headers.len(), 1, "durable physical reset is never repeated for shared-only retry");
        assert_eq!(*fixture.adapter.wal.header(), target);
        let shared = read_shared_wal_index_header(&fixture.region).unwrap().unwrap();
        assert_eq!(shared.mx_frame, 0);
        assert_eq!(shared.i_change, baseline.i_change, "reset is not a commit marker");
    }

    #[cfg(all(feature = "native", unix))]
    #[test]
    fn test_native_checkpoint_refuses_unpublished_generation_and_terminal_horizons_before_backfill() {
        use fsqlite_wal::wal_index::publish_shared_wal_index_header;

        let cx = test_cx();
        for kind in ["orphan", "generation", "terminal", "map_error"] {
            let vfs = CheckpointHandoffFaultVfs::new();
            let mut fixture = checkpoint_reset_fixture(&vfs, &cx);
            if kind == "orphan" {
                fixture.adapter.wal.append_frame(&cx, 1, &sample_page(0x7D), 1).expect("fixture unadvertised complete commit");
                fixture.adapter.wal.sync(&cx, SyncFlags::NORMAL).unwrap();
            } else if kind == "map_error" {
                vfs.faults.lock().unwrap().fail_index_maps = true;
            } else {
                let mut header = read_shared_wal_index_header(&fixture.region).unwrap().unwrap();
                if kind == "generation" { header.a_salt[0] ^= 1; } else { header.a_frame_cksum[0] ^= 1; }
                header.update_checksum().unwrap();
                publish_shared_wal_index_header(&fixture.region, &header).unwrap();
            }
            let before = fixture.adapter.published_snapshot();
            let header = *fixture.adapter.wal.header();
            let size = fixture.adapter.wal.file().file_size(&cx).unwrap();
            let shared = fixture.region.lock().to_vec();
            fixture.adapter.begin_transaction(&cx).expect_err("maintenance probe cannot widen an invalid native horizon");
            let mut writer = MockCheckpointPageWriter;
            fixture.adapter.checkpoint(&cx, CheckpointMode::Truncate, &mut writer, 0, None)
                .expect_err("native mismatch refuses before backfill/reset");
            assert!(!fixture.adapter.checkpoint_recovery_pending(), "prewrite validation never arms physical reset");
            assert_eq!(fixture.adapter.published_snapshot(), before);
            assert_eq!(*fixture.adapter.wal.header(), header);
            assert_eq!(fixture.adapter.wal.file().file_size(&cx).unwrap(), size);
            assert_eq!(fixture.region.lock().to_vec(), shared);
        }
    }

    #[cfg(all(feature = "fault-injection", feature = "native", unix))]
    #[test]
    fn test_path_checkpoint_pending_source_excludes_append_reconciliation_and_retains_handoff() {
        use std::task::{Context, Poll, Waker};

        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let (mut backend, certificate, _) = make_checkpoint_handoff_fault_backend(&vfs, &cx);
        vfs.pend_next_reset_write();
        let mut writer = MockCheckpointPageWriter;
        let mut checkpoint = backend.checkpoint(&cx, CheckpointMode::Truncate, &mut writer, 0, None);
        assert!(matches!(checkpoint.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Pending));
        drop(checkpoint);
        assert!(backend.checkpoint_recovery_pending());
        let completion = backend.inner.pending_checkpoint_reset.as_ref().unwrap().completion.clone();
        let target = backend.inner.pending_checkpoint_reset.as_ref().unwrap().target_header;
        assert_eq!(completion.state(), VfsWriteCompletionState::Pending);
        let wal_before = backend.inner.wal.file().file_size(&cx).unwrap();
        let handoff_before = backend.checkpoint_certificate_handoff(&cx).expect("handoff already durable before physical reset");
        assert_eq!(handoff_before, Some(certificate.clone()));
        backend.reconcile_parallel_wal_commit(&cx, &certificate, 1, 1, true)
            .expect_err("append reconciliation cannot race pending reset header source");
        backend.set_wal_fec_producer(&cx, None).expect_err("producer mutation cannot race pending reset source");
        backend.begin_transaction(&cx).expect_err("ordinary admission cannot race pending reset source");
        assert_eq!(completion.state(), VfsWriteCompletionState::Pending);
        assert_eq!(backend.inner.wal.file().file_size(&cx).unwrap(), wal_before);
        assert_eq!(vfs.faults.lock().unwrap().reset_headers.len(), 1);
        vfs.complete_pending_reset_write(&cx).expect("complete actual old source");
        backend.reconcile_checkpoint_reset(&cx).expect("Path retries fixed target with intact certificate handoff");
        assert!(!backend.checkpoint_recovery_pending());
        assert_eq!(*backend.inner.wal.header(), target);
        assert_eq!(backend.checkpoint_certificate_handoff(&cx).expect("read preserved handoff"), handoff_before);
        assert_eq!(backend.latest_authorized_parallel_wal_commit_certificate(&cx).expect("clock survives generation reset"), Some(certificate));
    }

    #[cfg(all(feature = "native", unix))]
    #[test]
    fn test_native_checkpoint_live_reader_refuses_before_certificate_handoff_or_database_mutation() {
        use fsqlite_wal::wal_index::{WAL_INDEX_VERSION, publish_shared_wal_index_header};
        use std::sync::atomic::Ordering;

        let cx = test_cx();
        let vfs = CheckpointHandoffFaultVfs::new();
        let (mut backend, _, _) = make_checkpoint_handoff_fault_backend(&vfs, &cx);
        let pager = fsqlite_pager::SimplePager::open_with_cx(
            &cx, vfs.clone(), Path::new("test.db"), PageSize::DEFAULT,
        ).expect("heap transport pager");
        let source = pager.wal_index_shm_source().unwrap();
        let region = source.map_region(&cx, 0, true).expect("explicit initialized fixture index");
        let wal_header = backend.inner.wal.header();
        let (_, terminal) = backend.inner.wal.last_commit_frame_header().unwrap();
        let mut header = WalIndexHdr {
            i_version: WAL_INDEX_VERSION, unused: 0, i_change: 1, is_init: 1,
            big_end_cksum: u8::from(wal_header.big_endian_checksum()),
            sz_page: u16::try_from(PAGE_SIZE).unwrap(), mx_frame: 1, n_page: 1,
            a_frame_cksum: [terminal.checksum.s1, terminal.checksum.s2],
            a_salt: [wal_header.salts.salt1, wal_header.salts.salt2], a_cksum: [0, 0],
        };
        header.update_checksum().unwrap();
        publish_shared_wal_index_header(&region, &header).unwrap();
        region.atomic_store_u32_ne(104, 1, Ordering::Release).unwrap();
        backend.attach_wal_index_shm_source(Arc::clone(&source)).unwrap();
        let mut lease = source.acquire_reader(&cx).expect("synthetic claim metadata");
        let binding = lease.binding().unwrap();
        let token = binding.token().clone();
        assert_eq!(backend.begin_native_read(&cx, binding).expect("bind exact fixture reader"), WalNativeReadOutcome::Ready);
        let pinned = backend.inner.pinned_read_snapshot();
        let wal_before = read_fault_injected_wal(&vfs, &cx);
        let shared_before = region.lock().to_vec();
        assert_eq!(backend.checkpoint_certificate_handoff(&cx).expect("handoff initially absent"), None);
        let _ = vfs.take_sync_observations();
        let mut writer = MockCheckpointPageWriter;
        backend.checkpoint(&cx, CheckpointMode::Truncate, &mut writer, 0, None)
            .expect_err("Path refuses before certificate persistence");
        backend.inner.checkpoint(&cx, CheckpointMode::Truncate, &mut writer, 0, None)
            .expect_err("base adapter independently refuses before backfill");
        assert!(!backend.checkpoint_recovery_pending());
        assert_eq!(backend.inner.pinned_read_snapshot(), pinned);
        assert!(backend.native_read_binding().unwrap().token().matches(&token));
        assert_eq!(read_fault_injected_wal(&vfs, &cx), wal_before);
        assert_eq!(region.lock().to_vec(), shared_before);
        assert_eq!(backend.checkpoint_certificate_handoff(&cx).expect("handoff remains absent"), None);
        assert!(vfs.take_sync_observations().is_empty());
        backend.end_native_read(&token).unwrap();
        lease.release().expect("release claim after backend retirement");
    }

    #[cfg(all(feature = "native", unix))]
    #[test]
    fn test_native_checkpoint_backfill_reset_reader_gate_and_stock_reopen() {
        use fsqlite_pager::{MvccPager, SimplePager, TransactionHandle, TransactionMode};
        use fsqlite_vfs::UnixVfs;

        let cx = test_cx();
        let directory = tempfile::tempdir().unwrap();
        let seed = directory.path().join("checkpoint-seed.db");
        let stock = rusqlite::Connection::open(&seed).unwrap();
        stock.execute_batch("PRAGMA page_size=4096; PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE recovery_rows(n INTEGER); INSERT INTO recovery_rows VALUES(10),(20);").unwrap();
        let path = directory.path().join("checkpoint.db");
        let wal_path = directory.path().join("checkpoint.db-wal");
        std::fs::write(&path, std::fs::read(&seed).unwrap()).unwrap();
        std::fs::write(&wal_path, std::fs::read(directory.path().join("checkpoint-seed.db-wal")).unwrap()).unwrap();
        std::fs::write(directory.path().join("checkpoint.db-shm"), std::fs::read(directory.path().join("checkpoint-seed.db-shm")).unwrap()).unwrap();
        let pager = SimplePager::open_with_cx(&cx, UnixVfs::new(), &path, PageSize::DEFAULT).expect("native checkpoint pager");
        let source = pager.wal_index_shm_source().unwrap();
        let (file, _) = UnixVfs::new().open(&cx, Some(&wal_path), VfsOpenFlags::READWRITE | VfsOpenFlags::WAL).unwrap();
        let wal = WalFile::open(&cx, file).expect("validate stock WAL");
        let old_generation = wal.generation_identity();
        let mut backend = PathRefreshingWalBackend::new(UnixVfs::new(), &path, &wal_path, PAGE_SIZE, wal, false, None);
        backend.attach_wal_index_shm_source(Arc::clone(&source)).unwrap();
        assert!(pager.set_wal_backend_owned(backend).is_ok());
        let region = source.map_region(&cx, 0, false).expect("existing stock index");
        let before = read_shared_wal_index_header(&region).unwrap().unwrap();
        let foreign_pager = SimplePager::open_with_cx(&cx, UnixVfs::new(), &path, PageSize::DEFAULT).expect("independent native reader attachment");
        let foreign_source = foreign_pager.wal_index_shm_source().unwrap();
        let mut foreign = foreign_source.acquire_reader(&cx).expect("retain nonzero WAL reader before backfill");
        assert!(!foreign.boundary().unwrap().database_only);
        assert_eq!(foreign.header().unwrap(), before);
        let full = pager.checkpoint(&cx, CheckpointMode::Full).expect("durable native backfill with exact reader horizon");
        assert!(!full.wal_was_reset);
        assert_eq!(read_shared_wal_index_header(&region).unwrap(), Some(before));
        assert_eq!(read_shared_wal_index_backfill(&region, &before).unwrap(), before.mx_frame);
        let deferred = pager.checkpoint(&cx, CheckpointMode::Restart).expect("foreign reader defers reset");
        assert!(!deferred.wal_was_reset);
        assert_eq!(deferred.effective_mode, CheckpointMode::Full);
        assert_eq!(read_shared_wal_index_header(&region).unwrap(), Some(before));
        foreign.release().expect("retire actual foreign reader before reset");
        drop(foreign_source);
        drop(foreign_pager);
        let reset = pager.checkpoint(&cx, CheckpointMode::Restart).expect("reset publishes shared header before reader exclusion ends");
        assert!(reset.wal_was_reset);
        let after = read_shared_wal_index_header(&region).unwrap().unwrap();
        assert_eq!((after.mx_frame, after.n_page, after.a_frame_cksum), (0, 0, [0, 0]));
        assert_ne!(after.a_salt, [old_generation.salts.salt1, old_generation.salts.salt2]);
        assert_eq!(after.i_change, before.i_change);
        assert_eq!(read_shared_wal_index_backfill(&region, &after).unwrap(), 0);
        let mut reader = pager.begin(&cx, TransactionMode::ReadOnly).expect("fresh native reader binds zero-frame generation");
        reader.get_page(&cx, PageNumber::new(2).unwrap()).expect("read checkpointed B-tree page from durable main file");
        reader.rollback(&cx).expect("release native zero-frame reader");
        native_recovery_stock_child(&path);
        // Read through the VFS's registered descriptor lifetime. Opening and
        // closing a plain std::fs descriptor here could release this process's
        // classic POSIX claims behind the native reader's ownership ledger.
        let (mut main_observer, _) = UnixVfs::new().open(
            &cx, Some(&path), VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB,
        ).expect("registered main-file observer");
        let mut database_before = vec![0; usize::try_from(main_observer.file_size(&cx).unwrap()).unwrap()];
        assert_eq!(main_observer.read(&cx, &mut database_before, 0).expect("database before empty truncate"), database_before.len());
        let mut database_only = source.acquire_reader(&cx).expect("retain real slot-zero reader through empty truncate");
        assert!(database_only.boundary().unwrap().database_only);
        let truncated = pager.checkpoint(&cx, CheckpointMode::Truncate).expect("truncate already-empty native generation");
        assert!(truncated.wal_was_reset);
        assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), u64::try_from(WAL_HEADER_SIZE).unwrap());
        let mut database_after = vec![0; usize::try_from(main_observer.file_size(&cx).unwrap()).unwrap()];
        assert_eq!(main_observer.read(&cx, &mut database_after, 0).expect("database after empty truncate"), database_after.len());
        assert_eq!(database_after, database_before, "empty truncate never patches or truncates a slot-zero reader's database");
        database_only.release().expect("retire database-only reader after byte preservation");
        main_observer.close(&cx).expect("close registered observer");
        let final_header = read_shared_wal_index_header(&region).unwrap().unwrap();
        assert_eq!(final_header.mx_frame, 0);
        assert_eq!(final_header.i_change, before.i_change);
        native_recovery_stock_child(&path);
        drop(stock);
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
